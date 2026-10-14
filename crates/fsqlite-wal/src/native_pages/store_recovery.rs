//! Retained page-history replay after completed native storage recovery.
//!
//! This owns the sealed storage handoff through every borrowed replay attempt.
//! Neither caller-supplied page maps nor a weaker validation callback can replace
//! the stored capsules, read observations, tombstones or exact transaction tokens.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, TxnToken};
use fsqlite_vfs::VfsFile;

use super::{NativePageLimits, NativePageStore, PageHistory, SharedCodec, corrupt};
use crate::native_commit::durable::{
    NativeCommitAttempt, NativeCommitOutcome, NativeCoordinatorRecovery, NativeObjectCodec,
};
use crate::native_durability::{
    NativeDurabilityLog, NativeDurabilityReceipt, NativeDurabilityRecovery, NativeRecovery,
    RecoveredNativeLog,
};
use crate::native_pages::NativePageSnapshot;

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePageStore<S, M, C> {
    /// Capture an admitted attempt before relinquishing this live store.
    ///
    /// Use the original transaction's `token()` after an errored or dropped
    /// commit returns ownership. This reads retained admission metadata only,
    /// including while storage is indeterminate. The ticket owns no snapshot
    /// pin and does not prevent `try_into_recovery` after handles are retired.
    /// `None` means no retained reservation, not proof of commit or rollback;
    /// completed page commits already delivered their acknowledgement.
    ///
    /// # Errors
    /// Refuses started closes or inconsistent admission metadata. No I/O occurs.
    pub fn commit_attempt(&self, token: TxnToken) -> Result<Option<NativeCommitAttempt>> {
        if self.closed { return Err(FrankenError::BusyRecovery); }
        self.driver.commit_attempt(token)
    }

    /// Transfer a quiescent live store into caller-owned writeback recovery.
    ///
    /// Finish or drop every transaction first, including unread snapshots and
    /// indeterminate handles. This consumes the old admission domain, not the
    /// external namespace/append lease, which the caller must retain throughout.
    /// No open, close, read, write, sync, rollback or retry occurs here.
    ///
    /// Both original streams, their acknowledged prefix and any outstanding
    /// source-owned write completion move into `NativeRecovery`. A pending
    /// write may transfer, but still prevents recovery I/O until it settles.
    /// An indeterminate commit is resolved by actual marker/capsule replay;
    /// discarding its old handle does not assert that the commit was aborted.
    /// The returned Arc retains the exact codec, without copying secret keys.
    /// After `prepare`/`finish`, use `NativePageRecovery::with_shared_codec`
    /// with this store's page size and limits to rebuild usable transactions.
    ///
    /// # Errors
    /// Returns the store while a transaction pin remains, close has begun, or
    /// the coordinator still owns an undelivered acknowledgement. No handles
    /// or queued obligations are discarded on refusal.
    #[allow(clippy::result_large_err)] // Refusal must retain the live storage owner.
    pub fn try_into_recovery(
        self,
    ) -> std::result::Result<(NativeRecovery<S, M>, Arc<C>), Self> {
        if self.closed || self.active.iter().any(|lease| lease.strong_count() != 0) {
            return Err(self);
        }
        let Self {
            mut driver, codec, history, owner, active, next_txn_id,
            page_high_water, recovery_required, closed,
        } = self;
        // Page admission precedes staging. Cancellation after a completed
        // stage can leave the lower log healthy while this page attempt is
        // indeterminate. Propagate that existing obligation, exactly as close
        // does, instead of classifying its reservations as healthy queued work.
        if recovery_required {
            driver.abandon_batch();
        }
        match driver.try_into_recovery() {
            Ok((recovery, _shared_codec)) => Ok((recovery, codec)),
            Err(driver) => Err(Self {
                driver, codec, history, owner, active, next_txn_id,
                page_high_water, recovery_required, closed,
            }),
        }
    }
}

struct PreparedPages {
    history: PageHistory,
    next_txn_id: u64,
    page_high_water: u64,
}

/// Rebuild a usable native page store while retaining the original file handles.
///
/// Storage rewrite/sync must already have completed through `NativeRecovery`.
/// This read-only stage independently replays every bound page capsule and its
/// snapshot observations in commit order. It uses the same page admission and
/// retention accounting as ordinary page-store recovery, with no live readers
/// during replay. Superseded page images are discarded, but tombstone stamps and
/// committed tokens remain authoritative for validation and allocation.
///
/// Keep the external namespace/append lease continuously held. Dropping a
/// `prepare` future discards only that attempt's partial page history; this owner
/// retains the streams and codec for retry or explicit close. This does not
/// switch public Connection storage modes or acknowledge an old transaction.
#[must_use]
pub struct NativePageRecovery<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    replay: NativeCoordinatorRecovery<S, M, SharedCodec<C>>,
    codec: Arc<C>,
    page_size: u32,
    limits: NativePageLimits,
    prepared: Option<PreparedPages>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> fmt::Debug for NativePageRecovery<S, M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativePageRecovery")
            .field("page_size", &self.page_size)
            .field("prepared", &self.prepared.is_some())
            .field("replay", &self.replay)
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePageRecovery<S, M, C> {
    /// Retain a sealed storage handoff without I/O or losing it on bad options.
    /// Page size and limits are checked by `prepare` before its first read.
    pub fn new(
        recovered: RecoveredNativeLog<S, M>,
        codec: C,
        page_size: u32,
        limits: NativePageLimits,
    ) -> Self {
        Self::with_shared_codec(recovered, Arc::new(codec), page_size, limits)
    }

    /// Retain an existing shared codec, including one returned by
    /// `into_recovery`, without requiring codec or secret-key duplication.
    pub fn with_shared_codec(
        recovered: RecoveredNativeLog<S, M>,
        codec: Arc<C>,
        page_size: u32,
        limits: NativePageLimits,
    ) -> Self {
        let replay = NativeCoordinatorRecovery::new(
            recovered,
            SharedCodec(Arc::clone(&codec)),
            limits.max_active_transactions.min(NativePageStore::<S, M, C>::MAX_COMMIT_GROUP),
        );
        Self { replay, codec, page_size, limits, prepared: None }
    }

    /// Verified storage prefix and retained tail accounting, not a new commit.
    #[must_use]
    pub const fn report(&self) -> &NativeDurabilityRecovery {
        self.replay.report()
    }

    /// Resolve an original page commit after full page-history preparation.
    ///
    /// Query before `finish`: installing a new live publisher retires old
    /// tickets, even when its next transaction reuses the same identity.
    /// Both physical recovery and the latest page/semantic replay must have
    /// succeeded. No caller-supplied validator can bypass page observations,
    /// tombstones, duplicate-token checks, authentication, or retention limits.
    /// This reads the stored proof but never writes, retries, sends an old
    /// reply, or changes an abandoned handle's indeterminate state.
    ///
    /// # Errors
    /// Refuses unprepared/closed recovery and foreign/retired tickets. Retains
    /// uncertainty for missing attempts behind torn tails, changed storage,
    /// cancellation, and proof/authentication mismatches.
    pub async fn resolve_commit(
        &self,
        cx: &Cx,
        attempt: &NativeCommitAttempt,
    ) -> Result<NativeCommitOutcome> {
        // A failed page preflight may never enter the generic replay, so its
        // previous prepared state alone cannot authorize a page-level outcome.
        if self.prepared.is_none() { return Err(FrankenError::BusyRecovery); }
        self.replay.resolve_commit(cx, attempt).await
    }

    /// Rebuild page state with mandatory capsule and read-version validation.
    ///
    /// Retries always start from genesis. Errors, cancellation, unwinding and
    /// dropped reads cannot expose a partially reconstructed store. Even after
    /// an earlier successful prepare, a later failed attempt invalidates finish.
    /// No writes, syncs, reopens or transaction-wide locks are introduced here.
    ///
    /// # Errors
    /// Rejects wrong page sizes, invalid/insufficient retention limits, corrupt
    /// proof/capsule bindings, duplicate tokens, stale committed observations,
    /// I/O errors, cancellation and ownership whose close has begun.
    pub async fn prepare(&mut self, cx: &Cx) -> Result<()> {
        self.prepare_inner(cx, None).await.map(|_| ())
    }

    /// Reconstruct an immutable image at an exact native commit sequence.
    ///
    /// Sequence zero selects genesis; future sequences are rejected before
    /// reading storage. Capture occurs after the entire selected capsule, but
    /// replay continues through the LATEST commit. A corrupt or unserializable
    /// suffix therefore rejects even an otherwise readable earlier image.
    /// `finish` always installs the latest store, never this historical image.
    ///
    /// This is a cold full-history replay, not an indexed time-travel query.
    /// At most one selected image and the current replay heads are retained:
    /// each payload separately fits `NativePageLimits`. Decode, token/index
    /// metadata and images retained by callers have their own memory costs.
    /// Plain `prepare` does not allocate a historical image or its index.
    ///
    /// Errors, cancellation and dropped futures invalidate preparation while
    /// retaining the original handles for retry. Previously returned images
    /// remain immutable. No writes, syncs, reopens or commit retries occur here.
    ///
    /// # Errors
    /// Returns the ordinary complete-replay errors or an out-of-range target.
    pub async fn prepare_snapshot(
        &mut self,
        cx: &Cx,
        sequence: CommitSeq,
    ) -> Result<NativePageSnapshot> {
        match self.prepare_inner(cx, Some(sequence)).await? {
            Some(snapshot) => Ok(snapshot),
            None => {
                self.prepared = None;
                Err(corrupt("native historical snapshot was not captured"))
            }
        }
    }

    /// Select the last committed marker at or before this Unix nanosecond time.
    /// Equal timestamps select the greatest sequence; a time before the first
    /// marker selects genesis, and a later time selects the current tip. The
    /// same complete semantic replay as `prepare_snapshot` remains mandatory.
    ///
    /// # Errors
    /// Propagates complete-replay errors, cancellation and retention limits.
    pub async fn prepare_snapshot_at_time(
        &mut self,
        cx: &Cx,
        time_unix_ns: u64,
    ) -> Result<NativePageSnapshot> {
        // The sealed storage scanner already proved nondecreasing timestamps.
        let markers = &self.replay.report().markers;
        let count = markers.partition_point(|marker| marker.commit_time_unix_ns <= time_unix_ns);
        let sequence = count.checked_sub(1)
            .map_or(CommitSeq::ZERO, |index| markers[index].commit_seq);
        self.prepare_snapshot(cx, sequence).await
    }

    /// Advance replica storage and return its exact, intrinsically read-only image.
    ///
    /// Each call validates the complete source page history, including commits
    /// beyond the selected batch, through the ordinary capsule/read-version,
    /// duplicate-token, codec/authentication and resource checks. No supplied
    /// callback or page map can bypass those checks. The image is allocated
    /// before any destination mutation and escapes only with the destination's
    /// successful two-barrier receipt. Existing images stay immutable while
    /// subsequent calls catch up. No writable store or publisher is constructed.
    ///
    /// The destination must be empty or an accepted prefix of this source;
    /// the storage transfer checks both complete marker chains and rejects
    /// aliases, forks and ahead replicas without overwriting them. It reuses
    /// identical staged symbols and verifies destination referents with this
    /// source's codec. The destination therefore uses the same codec profile
    /// and authentication epoch, not a caller-selected weaker verifier.
    /// An already synchronized replica returns `None`, not another receipt.
    ///
    /// `max_commits` is in 1..=128 and bounds marker publication, not total
    /// work: this is cold full-history replay plus the existing object transfer,
    /// which may stage objects for later commits and transitive dependencies.
    /// The selected image and replay heads each obey the page budget; caller-
    /// retained images, codec allocations and storage metadata are additional.
    /// This is explicit local catch-up, not a transport, quorum or SQL selector.
    ///
    /// Keep both namespace/append leases throughout the operation and any
    /// abandoned destination writes. Source tails are preserved, destination
    /// tails refuse admission. A failed or dropped mutating transfer returns no
    /// image and leaves destination recovery/physical-write obligations intact;
    /// actual recovery, not an in-place retry, must resolve them. A torn batch
    /// can contain committed markers even though no complete receipt escaped.
    ///
    /// # Errors
    /// Rejects invalid limits, invalid source history, forks, aliases, capacity
    /// excess, cancellation and VFS errors. No source bytes are changed.
    pub async fn replicate_snapshot_to<DS: VfsFile, DM: VfsFile>(
        &mut self,
        cx: &Cx,
        destination: &mut NativeDurabilityLog<DS, DM>,
        max_commits: usize,
    ) -> Result<Option<(NativeDurabilityReceipt, NativePageSnapshot)>> {
        if !(1..=128).contains(&max_commits) {
            return Err(FrankenError::OutOfRange {
                what: "native page replication commit batch".to_owned(),
                value: max_commits.to_string(),
            });
        }
        let tip = self.report().markers.last()
            .map_or(CommitSeq::ZERO, |marker| marker.commit_seq);
        let base = destination.published_tip();
        if base > tip {
            return Err(corrupt("native page replica is ahead of the source"));
        }
        let count = (tip.get() - base.get())
            .min(u64::try_from(max_commits).map_err(|_| FrankenError::TooBig)?);
        // count <= tip - base, so this addition cannot overflow.
        let target = CommitSeq::new(base.get() + count);
        let snapshot = self.prepare_snapshot(cx, target).await?;
        let receipt = self.replay
            .replicate_prepared_prefix_to(cx, destination, max_commits).await?;
        // The owned source and exclusive destination borrows preserve these
        // tips across the lower transfer. No fallible allocation, I/O, callback
        // or late cancellation check follows acknowledged publication.
        debug_assert_eq!(receipt.is_some(), count != 0);
        debug_assert!(receipt.as_ref().is_none_or(|receipt| receipt.last_seq == target));
        Ok(receipt.map(|receipt| (receipt, snapshot)))
    }

    async fn prepare_inner(
        &mut self,
        cx: &Cx,
        selected: Option<CommitSeq>,
    ) -> Result<Option<NativePageSnapshot>> {
        self.prepared = None;
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        let mut history = PageHistory::new(self.page_size, self.limits)?;
        let tip = self.replay.report().markers.last()
            .map_or(CommitSeq::ZERO, |marker| marker.commit_seq);
        if let Some(sequence) = selected.filter(|sequence| *sequence > tip) {
            return Err(FrankenError::OutOfRange {
                what: "native historical commit sequence".to_owned(),
                value: sequence.get().to_string(),
            });
        }
        let mut snapshot = if selected == Some(CommitSeq::ZERO) {
            Some(capture_snapshot(cx, &history, None)?)
        } else {
            None
        };
        self.replay
            .prepare(cx, |candidate, _| {
                history.replay_candidate(candidate)?;
                if selected == Some(candidate.proof.commit_seq) {
                    snapshot = Some(capture_snapshot(
                        cx, &history, Some(candidate.proof.commit_time_unix_ns),
                    )?);
                }
                Ok(())
            })
            .await?;
        // Full replay, including the suffix AFTER a requested historical image,
        // must succeed before either the image or a current store escapes.
        if history.tip != tip {
            return Err(corrupt("native page replay/driver tip mismatch"));
        }
        if selected.is_some() && snapshot.is_none() {
            return Err(corrupt("native historical snapshot was not captured"));
        }
        let next_txn_id = history.max_txn_id.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
        // Latest tombstones still own their addresses. Never recycle them or
        // enumerate sparse holes, and never use uncommitted allocator metadata.
        let page_high_water = history.pages.last_key_value()
            .map_or(1, |(page, _)| u64::from(page.get()).max(1));
        self.prepared = Some(PreparedPages { history, next_txn_id, page_high_water });
        Ok(snapshot)
    }

    /// Install complete page and coordinator state together, without new I/O.
    /// An unprepared/closed owner is returned intact on refusal. Recovered torn
    /// tails remain admission-blocking even after successful semantic replay.
    ///
    /// # Errors
    /// Returns `self` until the most recent prepare succeeds or after close.
    #[allow(clippy::result_large_err)] // Retaining storage ownership is the error contract.
    pub fn finish(mut self) -> std::result::Result<
        (NativePageStore<S, M, C>, NativeDurabilityRecovery), Self,
    > {
        let Some(pages) = self.prepared.take() else { return Err(self); };
        let Self { replay, codec, page_size, limits, .. } = self;
        match replay.finish() {
            Ok((driver, report)) => Ok((NativePageStore {
                driver,
                codec,
                history: pages.history,
                owner: Arc::new(()),
                active: Vec::new(),
                next_txn_id: pages.next_txn_id,
                page_high_water: AtomicU64::new(pages.page_high_water),
                recovery_required: false,
                closed: false,
            }, report)),
            Err(replay) => Err(Self {
                replay, codec, page_size, limits, prepared: Some(pages),
            }),
        }
    }

    /// Return to explicit storage recovery while preserving both handles and
    /// the shared codec. A new storage preparation is required before handoff.
    #[must_use]
    pub fn into_recovery(self) -> (NativeRecovery<S, M>, Arc<C>) {
        let (recovery, _) = self.replay.into_recovery();
        (recovery, self.codec)
    }

    /// Close only the retained streams. Failed close obligations remain owned.
    ///
    /// # Errors
    /// Propagates the VFS close failure; the owner can retry close but not replay.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        self.prepared = None;
        self.replay.close(cx)
    }
}

// Called only immediately after validated atomic page-capsule application (or
// at empty genesis). Preserve sparse extent while excluding deleted payloads.
fn capture_snapshot(
    cx: &Cx,
    history: &PageHistory,
    time_unix_ns: Option<u64>,
) -> Result<NativePageSnapshot> {
    let mut pages = Vec::new();
    pages.try_reserve_exact(history.pages.len()).map_err(|_| FrankenError::OutOfMemory)?;
    let mut extent = 0;
    let mut bytes = 0_usize;
    for (page, versions) in &history.pages {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        if let Some(version) = versions.last() {
            extent = page.get();
            if let Some(data) = &version.data {
                bytes = bytes.checked_add(data.len()).ok_or(FrankenError::TooBig)?;
                pages.push((*page, Arc::clone(data)));
            }
        }
    }
    if bytes != history.payload_bytes {
        return Err(corrupt("native historical snapshot payload accounting mismatch"));
    }
    Ok(NativePageSnapshot::from_validated_pages(
        history.tip, time_unix_ns, history.page_size, extent, pages, bytes,
    ))
}

#[cfg(test)]
#[path = "store_recovery_live_tests.rs"]
mod live_transfer_tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod replica_snapshot_tests {
    use std::future::{Future, ready};
    use std::path::Path;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{
        CommitMarker, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags,
        TxnEpoch, TxnId, reconstruct_systematic_happy_path,
    };
    use fsqlite_vfs::{MemoryVfs, Vfs};

    use super::*;
    use crate::native_commit::CommitSubmission;
    use crate::native_commit::durable::NativeCommitProof;
    use crate::native_durability::NativeDurabilityLimits;
    use crate::native_pages::{NativePageCapsule, NativePageWrite};

    struct Codec;
    impl NativeObjectCodec for Codec {
        fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(
                ObjectId::derive_from_canonical_bytes(bytes),
                Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
                0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
            )])
        }
        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            let bytes = reconstruct_systematic_happy_path(records)
                .map_err(|error| corrupt(&error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id {
                return Err(corrupt("replica snapshot test object identity mismatch"));
            }
            Ok(bytes)
        }
    }

    type File = <MemoryVfs as Vfs>::File;
    type Log = NativeDurabilityLog<File, File>;
    type Store = NativePageStore<File, File, Codec>;

    fn run(future: impl Future<Output = ()>) {
        let _guard = crate::metrics::GLOBAL_GROUP_COMMIT_METRICS_TEST_LOCK.lock().unwrap();
        RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
    }

    fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
        vfs.open(cx, Some(Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
    }

    fn log(vfs: &MemoryVfs, cx: &Cx, name: &str) -> Log {
        Log::create(cx, file(vfs, cx, &format!("{name}-objects")),
            file(vfs, cx, &format!("{name}-markers")), NativeDurabilityLimits::default()).unwrap()
    }

    fn store(vfs: &MemoryVfs, cx: &Cx, name: &str) -> Store {
        Store::new(log(vfs, cx, name), Codec, 512, NativePageLimits::default()).unwrap()
    }

    async fn put(db: &mut Store, cx: &Cx, value: u8) {
        let mut txn = db.begin(cx).unwrap();
        db.write_page(cx, &mut txn, page(2), Some(&[value; 512])).unwrap();
        db.commit(cx, &mut txn, u64::from(value)).await.unwrap();
    }

    async fn source<C: NativeObjectCodec>(
        db: NativePageStore<File, File, C>, cx: &Cx, limits: NativePageLimits,
    ) -> NativePageRecovery<File, File, C> {
        let (mut physical, codec) = db.try_into_recovery()
            .unwrap_or_else(|_| panic!("quiescent source must transfer"));
        physical.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ())))
            .await.unwrap();
        NativePageRecovery::with_shared_codec(physical.finish().unwrap(), codec, 512, limits)
    }

    async fn disk(vfs: &MemoryVfs, cx: &Cx, name: &str) -> (Vec<u8>, Vec<u8>) {
        async fn bytes(vfs: &MemoryVfs, cx: &Cx, name: &str) -> Vec<u8> {
            let mut handle = file(vfs, cx, name);
            let mut bytes = vec![0; usize::try_from(handle.file_size(cx).unwrap()).unwrap()];
            assert_eq!(handle.read(cx, &mut bytes, 0).await.unwrap(), bytes.len());
            handle.close(cx).unwrap();
            bytes
        }
        (bytes(vfs, cx, &format!("{name}-objects")).await,
         bytes(vfs, cx, &format!("{name}-markers")).await)
    }

    #[test]
    fn replica_snapshots_advance_exact_batches_preserve_old_images_and_reopen() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut db = store(&vfs, &cx, "source");
            let mut first = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut first, page(2), Some(&[1; 512])).unwrap();
            db.write_page(&cx, &mut first, page(9000), Some(&[9; 512])).unwrap();
            db.commit(&cx, &mut first, 10).await.unwrap();
            let mut second = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut second, page(2), Some(&[2; 512])).unwrap();
            db.write_page(&cx, &mut second, page(9000), None).unwrap();
            db.commit(&cx, &mut second, 20).await.unwrap();
            let mut third = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut third, page(3), Some(&[3; 512])).unwrap();
            db.commit(&cx, &mut third, 30).await.unwrap();
            let mut source = source(db, &cx, NativePageLimits::default()).await;
            let before = disk(&vfs, &cx, "source").await;
            let mut replica = log(&vfs, &cx, "replica");
            let (receipt, old) = source.replicate_snapshot_to(&cx, &mut replica, 1)
                .await.unwrap().unwrap();
            assert_eq!(receipt.first_seq, CommitSeq::new(1));
            assert_eq!(receipt.last_seq, old.commit_seq());
            assert_eq!(receipt.commits, 1);
            assert_eq!(old.commit_time_unix_ns(), Some(10));
            assert_eq!(old.read_page(page(2)).unwrap().as_ref(), &[1; 512]);
            assert_eq!(old.page_count(), 2);
            let (receipt, middle) = source.replicate_snapshot_to(&cx, &mut replica, 1)
                .await.unwrap().unwrap();
            assert_eq!(receipt.first_seq, CommitSeq::new(2));
            assert_eq!(middle.commit_seq(), receipt.last_seq);
            assert_eq!(middle.database_size_bound(), 9000);
            assert!(middle.read_page(page(9000)).is_none());
            assert!(middle.read_page(page(3)).is_none(), "never expose the staged future commit");
            let (receipt, latest) = source.replicate_snapshot_to(&cx, &mut replica, 128)
                .await.unwrap().unwrap();
            assert_eq!(receipt.commits, 1);
            assert_eq!(latest.commit_seq(), CommitSeq::new(3));
            assert_eq!(latest.read_page(page(3)).unwrap().as_ref(), &[3; 512]);
            assert_eq!(old.read_page(page(9000)).unwrap().as_ref(), &[9; 512]);
            assert_eq!(old.read_page(page(2)).unwrap().as_ref(), &[1; 512]);
            let replicated = disk(&vfs, &cx, "replica").await;
            assert!(source.replicate_snapshot_to(&cx, &mut replica, 1).await.unwrap().is_none());
            assert_eq!(disk(&vfs, &cx, "replica").await, replicated);
            assert_eq!(disk(&vfs, &cx, "source").await, before);
            assert!(!replica.needs_recovery());
            assert!(replica.outstanding_write().is_none());
            replica.close(&cx).unwrap();
            source.close(&cx).unwrap();
            // Reconstruct from destination bytes alone, not the returned source image.
            let (mut reopened, report) = Store::recover(&cx,
                file(&vfs, &cx, "replica-objects"), file(&vfs, &cx, "replica-markers"),
                Codec, 512, NativeDurabilityLimits::default(), NativePageLimits::default())
                .await.unwrap();
            assert_eq!(report.markers.len(), 3);
            let mut view = reopened.begin(&cx).unwrap();
            assert_eq!(reopened.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[2; 512]);
            assert_eq!(reopened.read_page(&cx, &mut view, page(3)).unwrap().unwrap().as_ref(), &[3; 512]);
            assert!(reopened.read_page(&cx, &mut view, page(9000)).unwrap().is_none());
            reopened.rollback(&mut view).unwrap();
            reopened.close(&cx).unwrap();
            assert_eq!(middle.read_page(page(2)).unwrap().as_ref(), &[2; 512]);
        });
    }

    #[test]
    fn replica_snapshots_reject_invalid_unselected_source_suffix_before_staging() {
        run(async {
            for invalid in 0..4 {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let mut source_log = log(&vfs, &cx, "source");
                let mut previous = None;
                for seq in 1_u64..=3 {
                    let stale = seq == 3 && invalid == 0;
                    let token = if seq == 3 && invalid == 1 { 1 } else { seq };
                    let capsule = NativePageCapsule {
                        page_size: 512, snapshot: CommitSeq::new(seq - 1),
                        reads: vec![(page(2), CommitSeq::new(if stale { 1 } else { seq - 1 }))],
                        writes: vec![NativePageWrite {
                            page: page(2), data: Some(Arc::from(vec![u8::try_from(seq).unwrap(); 512])),
                        }],
                    };
                    let bytes = capsule.to_bytes().unwrap();
                    let records = Codec.encode(&cx, &bytes).unwrap();
                    let proof = NativeCommitProof {
                        commit_seq: CommitSeq::new(seq), commit_time_unix_ns: seq,
                        submission: CommitSubmission {
                            capsule_object_id: records[0].object_id,
                            capsule_digest: *blake3::hash(&bytes).as_bytes(),
                            write_set_pages: vec![page(if seq == 3 && invalid == 2 { 3 } else { 2 })],
                            witness_refs: if seq == 3 && invalid == 3 {
                                vec![ObjectId::from_bytes([0xED; 16])]
                            } else { vec![] },
                            edge_ids: vec![], merge_witness_ids: vec![],
                            txn_token: TxnToken::new(TxnId::new(token).unwrap(), TxnEpoch::new(1)),
                            begin_seq: capsule.snapshot,
                        },
                    };
                    let proofs = Codec.encode(&cx, &proof.to_bytes().unwrap()).unwrap();
                    let marker = CommitMarker::new(proof.commit_seq, seq,
                        records[0].object_id, proofs[0].object_id, previous);
                    previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
                    source_log.append_symbols(&cx, &records).await.unwrap();
                    source_log.append_symbols(&cx, &proofs).await.unwrap();
                    source_log.publish(&cx, &[marker], |id, records| {
                        ready(Codec.decode(&cx, id, &records).map(|_| ()))
                    }).await.unwrap();
                }
                // The storage layer can verify envelopes without proving page semantics.
                let mut physical = source_log.into_recovery();
                physical.prepare(&cx, |id, records| ready(Codec.decode(&cx, id, &records).map(|_| ())))
                    .await.unwrap();
                let mut source = NativePageRecovery::new(physical.finish().unwrap(), Codec,
                    512, NativePageLimits::default());
                let mut replica = log(&vfs, &cx, "replica");
                let before = disk(&vfs, &cx, "replica").await;
                assert!(source.replicate_snapshot_to(&cx, &mut replica, 1).await.is_err());
                assert_eq!(disk(&vfs, &cx, "replica").await, before);
                assert_eq!(replica.published_tip(), CommitSeq::ZERO);
                assert!(!replica.needs_recovery());
                assert!(source.prepared.is_none());
                replica.close(&cx).unwrap();
                source.close(&cx).unwrap();
            }
        });
    }

    #[test]
    fn replica_snapshots_reject_forks_at_equal_tips_and_ahead_destinations() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut original = store(&vfs, &cx, "original");
            put(&mut original, &cx, 1).await;
            put(&mut original, &cx, 2).await;
            let mut original = source(original, &cx, NativePageLimits::default()).await;
            let mut other = store(&vfs, &cx, "other");
            put(&mut other, &cx, 9).await;
            let mut other = source(other, &cx, NativePageLimits::default()).await;
            let mut replica = log(&vfs, &cx, "replica");
            original.replicate_snapshot_to(&cx, &mut replica, 1).await.unwrap().unwrap();
            let before = disk(&vfs, &cx, "replica").await;
            assert!(matches!(other.replicate_snapshot_to(&cx, &mut replica, 1).await,
                Err(FrankenError::WalCorrupt { .. })));
            assert_eq!(disk(&vfs, &cx, "replica").await, before);
            assert!(!replica.needs_recovery());
            let (_, snapshot) = original.replicate_snapshot_to(&cx, &mut replica, 1)
                .await.unwrap().unwrap();
            assert_eq!(snapshot.read_page(page(2)).unwrap().as_ref(), &[2; 512]);
            let before = disk(&vfs, &cx, "replica").await;
            assert!(matches!(other.replicate_snapshot_to(&cx, &mut replica, 1).await,
                Err(FrankenError::WalCorrupt { .. })));
            assert_eq!(disk(&vfs, &cx, "replica").await, before);
            assert!(!replica.needs_recovery());
            replica.close(&cx).unwrap();
            original.close(&cx).unwrap();
            other.close(&cx).unwrap();
        });
    }

    #[test]
    fn replica_snapshots_limits_and_cancellation_leave_destination_reusable() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut db = store(&vfs, &cx, "source");
            put(&mut db, &cx, 1).await;
            let mut source = source(db, &cx, NativePageLimits {
                max_retained_page_bytes: 511, ..NativePageLimits::default()
            }).await;
            let mut replica = log(&vfs, &cx, "replica");
            for count in [0, 129, usize::MAX] {
                assert!(matches!(source.replicate_snapshot_to(&cx, &mut replica, count).await,
                    Err(FrankenError::OutOfRange { .. })));
            }
            assert!(matches!(source.replicate_snapshot_to(&cx, &mut replica, 1).await,
                Err(FrankenError::TooBig)));
            // Correct options through the owned recovery handoff, not mutation of page state.
            let (mut physical, codec) = source.into_recovery();
            physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let mut source = NativePageRecovery::with_shared_codec(physical.finish().unwrap(),
                codec, 512, NativePageLimits::default());
            let cancelled = Cx::new();
            cancelled.cancel();
            assert!(matches!(source.replicate_snapshot_to(&cancelled, &mut replica, 1).await,
                Err(FrankenError::Interrupt)));
            assert_eq!(disk(&vfs, &cx, "replica").await, (vec![], vec![]));
            assert!(!replica.needs_recovery());
            let (_, image) = source.replicate_snapshot_to(&cx, &mut replica, 1)
                .await.unwrap().unwrap();
            assert_eq!(image.read_page(page(2)).unwrap().as_ref(), &[1; 512]);
            source.close(&cx).unwrap();
            assert!(source.replicate_snapshot_to(&cx, &mut replica, 1).await.is_err());
            replica.close(&cx).unwrap();
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn replica_snapshots_use_authenticated_raptorq_and_preserve_the_codec_epoch() {
        use crate::native_commit::durable::codec::RaptorQNativeCodec;

        run(async {
            let cx = Cx::new();
            cx.set_native_cx(asupersync::Cx::current().unwrap());
            let vfs = MemoryVfs::new();
            let mut db = NativePageStore::new(log(&vfs, &cx, "source"),
                RaptorQNativeCodec::new(Some([0x73; 32])), 512, NativePageLimits::default()).unwrap();
            let mut txn = db.begin(&cx).unwrap();
            for n in 2..=12 {
                db.write_page(&cx, &mut txn, page(n), Some(&[u8::try_from(n).unwrap(); 512])).unwrap();
            }
            db.commit(&cx, &mut txn, 100).await.unwrap();
            let mut source = source(db, &cx, NativePageLimits::default()).await;
            let mut replica = log(&vfs, &cx, "replica");
            let (_, image) = source.replicate_snapshot_to(&cx, &mut replica, 128)
                .await.unwrap().unwrap();
            assert_eq!(image.page_count(), 11);
            assert_eq!(image.read_page(page(7)).unwrap().as_ref(), &[7; 512]);
            let copied = disk(&vfs, &cx, "replica").await;
            let (mut physical, correct) = source.into_recovery();
            physical.prepare(&cx, |id, records| ready(correct.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let mut wrong = NativePageRecovery::new(physical.finish().unwrap(),
                RaptorQNativeCodec::new(Some([0x74; 32])), 512, NativePageLimits::default());
            // Even the equal-tip/no-new-marker call must not bypass authentication.
            assert!(wrong.replicate_snapshot_to(&cx, &mut replica, 1).await.is_err());
            assert_eq!(disk(&vfs, &cx, "replica").await, copied);
            wrong.close(&cx).unwrap();
            replica.close(&cx).unwrap();
            let (mut reopened, _) = NativePageStore::recover(&cx,
                file(&vfs, &cx, "replica-objects"), file(&vfs, &cx, "replica-markers"),
                RaptorQNativeCodec::new(Some([0x73; 32])), 512,
                NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
            let mut view = reopened.begin(&cx).unwrap();
            for n in 2..=12 {
                assert_eq!(reopened.read_page(&cx, &mut view, page(n)).unwrap().unwrap().as_ref(),
                    &[u8::try_from(n).unwrap(); 512]);
            }
            reopened.rollback(&mut view).unwrap();
            reopened.close(&cx).unwrap();
        });
    }
}
