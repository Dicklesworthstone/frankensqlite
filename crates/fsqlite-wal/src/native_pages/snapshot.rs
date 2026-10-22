//! Immutable committed images captured live or replayed from native history.
//!
//! These own page bytes, not storage handles, writer authority or snapshot pins
//! in a live publisher. A writable fork requires a separate destination and
//! an ordinary, fully validated durable page commit; the image grants no
//! authority over the source log.

use std::fmt;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, PageNumber, TxnToken};
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};

use crate::native_commit::durable::{
    DurableCommitAcknowledgement, NativeCommitAttempt, NativeObjectCodec,
};

use super::{
    MAX_CAPSULE_PAGES, MAX_PAGE_CAPSULE_BYTES, NativePageCapsule, NativePageStore,
    NativePageTransaction, NativePageTransactionState, NativePageWrite,
};

struct SnapshotPages {
    sequence: CommitSeq,
    commit_time_unix_ns: Option<u64>,
    page_size: u32,
    extent: u32,
    pages: Vec<(PageNumber, Arc<[u8]>)>,
    payload_bytes: usize,
}

/// A read-only native page image at one exact committed sequence.
///
/// Constructed by `NativePageStore::capture_snapshot` from fully published
/// in-memory heads, or by `NativePageRecovery::prepare_snapshot` and
/// `prepare_snapshot_at_time` after complete durable-history validation.
/// Live capture does not reread or reauthenticate the physical log. Clones
/// share the image; later commits, recovery retries and live history reclamation
/// cannot change it. A deleted page is absent, but its address remains in the
/// historical extent. Sparse holes are never expanded into a dense page array.
///
/// This is an in-memory historical image of the native log, not a serialized
/// checkpoint, a SQLite database file, an SSI proof or permission to publish.
/// It does not pin a live transaction or retain file handles or encryption keys.
/// The payload fits the source owner's page budget; independently retained
/// images accumulate caller-owned memory. This is not a process-RSS bound.
#[derive(Clone)]
pub struct NativePageSnapshot {
    inner: Arc<SnapshotPages>,
}

impl fmt::Debug for NativePageSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativePageSnapshot")
            .field("sequence", &self.inner.sequence)
            .field("page_size", &self.inner.page_size)
            .field("extent", &self.inner.extent)
            .field("pages", &self.inner.pages.len())
            .field("payload_bytes", &self.inner.payload_bytes)
            .finish_non_exhaustive()
    }
}

impl NativePageSnapshot {
    // Only the enclosing native-page implementation can create an image. The
    // live store or recovery interpreter supplies ordered, validated heads
    // after atomic publication; callers cannot substitute an arbitrary page map.
    pub(super) fn from_validated_pages(
        sequence: CommitSeq,
        commit_time_unix_ns: Option<u64>,
        page_size: u32,
        extent: u32,
        pages: Vec<(PageNumber, Arc<[u8]>)>,
        payload_bytes: usize,
    ) -> Self {
        Self {
            inner: Arc::new(SnapshotPages {
                sequence,
                commit_time_unix_ns,
                page_size,
                extent,
                pages,
                payload_bytes,
            }),
        }
    }

    /// Exact historical commit. Zero denotes the empty genesis image.
    #[must_use]
    pub fn commit_seq(&self) -> CommitSeq { self.inner.sequence }

    /// Actual selected commit time, not the requested timestamp. Genesis has none.
    #[must_use]
    pub fn commit_time_unix_ns(&self) -> Option<u64> { self.inner.commit_time_unix_ns }

    #[must_use]
    pub fn page_size(&self) -> u32 { self.inner.page_size }

    /// Highest address committed by this point, including historical tombstones.
    /// This is an address bound, not a live-page count or physical file length.
    #[must_use]
    pub fn database_size_bound(&self) -> u32 { self.inner.extent }

    /// Present pages only; deletions and never-allocated holes do not contribute.
    #[must_use]
    pub fn page_count(&self) -> usize { self.inner.pages.len() }

    /// Image payload bytes, excluding the sparse index and caller-held clones.
    #[must_use]
    pub fn retained_page_bytes(&self) -> usize { self.inner.payload_bytes }

    /// Read exact immutable bytes in O(log(present pages)) without storage I/O.
    /// An absent result never falls through to a live publisher's newer pages.
    #[must_use]
    pub fn read_page(&self, page: PageNumber) -> Option<Arc<[u8]>> {
        self.inner.pages.binary_search_by_key(&page, |(number, _)| *number)
            .ok().map(|index| Arc::clone(&self.inner.pages[index].1))
    }

    /// Enumerate present addresses in order, without allocating a hole list.
    pub fn page_numbers(&self) -> impl ExactSizeIterator<Item = PageNumber> + '_ {
        self.inner.pages.iter().map(|(page, _)| *page)
    }

    /// Prepare an atomic writable fork in a separate, genesis page store.
    ///
    /// Copies present pages into one private transaction. A single tombstone
    /// preserves a deleted high address without enumerating sparse holes.
    /// Source commit numbers, conflict stamps and transaction tokens are NOT
    /// imported: this is a new history, whose first writing commit is 1.
    /// The source remains immutable and can outlive or precede its publisher.
    ///
    /// Preparation performs no storage I/O. The returned owner exclusively
    /// borrows the destination until it is dropped, so no peer can change its
    /// accepted head between preparation and commit. It retains the actual
    /// transaction for retry, rollback and indeterminate-attempt recovery.
    /// A successful preparation is not a durability acknowledgement.
    ///
    /// The complete fork must fit ONE canonical page capsule (4 MiB including
    /// metadata, at most 4096 mutations). Larger images fail before BEGIN;
    /// splitting into multiple commits would lose crash atomicity. Destination
    /// page, object-codec and storage budgets additionally apply at commit.
    /// Payload copies and the source image coexist; this is not an RSS bound.
    /// An empty genesis image completes read-only and creates no marker.
    ///
    /// The caller supplies a distinct native log and continuously retains its
    /// namespace/append lease. This does not create files, establish SQL schema
    /// correctness, copy old history, or enable public Connection native mode.
    /// Source metadata exposed by the owner is not persisted as provenance.
    ///
    /// # Errors
    /// Refuses non-genesis destinations or page-size mismatch with Unsupported,
    /// excessive capsule size, unavailable storage, cancellation and allocation
    /// failure. Failed preparation publishes nothing; private allocation high
    /// watermarks may advance, as with ordinary rolled-back native writes.
    pub fn prepare_fork<'a, S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
        &self,
        cx: &Cx,
        destination: &'a mut NativePageStore<S, M, C>,
    ) -> Result<NativeSnapshotInstall<'a, S, M, C>> {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        if destination.needs_recovery() {
            return Err(FrankenError::BusyRecovery);
        }
        if destination.committed_tip() != CommitSeq::ZERO
            || destination.page_size() != self.page_size()
        {
            return Err(FrankenError::Unsupported);
        }
        let high_tombstone = PageNumber::new(self.inner.extent).filter(|last| {
            self.inner.pages.last().is_none_or(|(page, _)| page != last)
        });
        let count = self.inner.pages.len()
            .checked_add(usize::from(high_tombstone.is_some()))
            .ok_or(FrankenError::TooBig)?;
        if count > MAX_CAPSULE_PAGES || self.inner.payload_bytes > MAX_PAGE_CAPSULE_BYTES {
            return Err(FrankenError::TooBig);
        }
        let mut writes = Vec::new();
        writes.try_reserve_exact(count).map_err(|_| FrankenError::OutOfMemory)?;
        for (page, data) in &self.inner.pages {
            cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
            writes.push(NativePageWrite { page: *page, data: Some(Arc::clone(data)) });
        }
        if let Some(page) = high_tombstone {
            writes.push(NativePageWrite { page, data: None });
        }
        // Use the canonical encoder's admission, including every read stamp
        // and mutation header. A payload-only budget would admit oversized
        // forks at the boundary and fail after creating a private transaction.
        if !writes.is_empty() {
            let mut reads = Vec::new();
            reads.try_reserve_exact(count).map_err(|_| FrankenError::OutOfMemory)?;
            reads.extend(writes.iter().map(|write| (write.page, CommitSeq::ZERO)));
            let capsule = NativePageCapsule {
                page_size: self.page_size(), snapshot: CommitSeq::ZERO, reads, writes,
            };
            capsule.encoded_len()?;
            writes = capsule.writes;
        }
        let mut transaction = destination.begin(cx)?;
        for write in &writes {
            destination.write_page(cx, &mut transaction, write.page, write.data.as_deref())?;
        }
        Ok(NativeSnapshotInstall {
            destination, transaction, source_sequence: self.commit_seq(),
            source_time: self.commit_time_unix_ns(), pending_pages: count, acknowledgement: None,
        })
    }
}

/// An unpublished snapshot installation using the existing native page protocol.
///
/// Finish with `commit` or `rollback`. Dropping an active owner discards only
/// its private overlay/pin, with no hidden executor or I/O. An error or dropped
/// commit future after admission leaves the real transaction indeterminate and
/// the destination recovery-blocked. Capture `commit_attempt` before dropping
/// this owner, settle `outstanding_write`, and use original-handle recovery.
/// Dropping the owner is never evidence that an admitted commit was aborted.
#[must_use = "commit or roll back the prepared native snapshot installation"]
pub struct NativeSnapshotInstall<'a, S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    destination: &'a mut NativePageStore<S, M, C>,
    transaction: NativePageTransaction,
    source_sequence: CommitSeq,
    source_time: Option<u64>,
    pending_pages: usize,
    acknowledgement: Option<DurableCommitAcknowledgement>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> fmt::Debug
    for NativeSnapshotInstall<'_, S, M, C>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeSnapshotInstall")
            .field("source_sequence", &self.source_sequence)
            .field("transaction", &self.transaction.token())
            .field("state", &self.transaction.state())
            .field("pending_pages", &self.pending_pages)
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativeSnapshotInstall<'_, S, M, C> {
    /// Source metadata only; the destination has its own commit clock.
    #[must_use]
    pub const fn source_commit_seq(&self) -> CommitSeq { self.source_sequence }

    #[must_use]
    pub const fn source_commit_time_unix_ns(&self) -> Option<u64> { self.source_time }

    /// Original destination transaction identity, including after a failed commit.
    #[must_use]
    pub fn transaction_token(&self) -> TxnToken { self.transaction.token() }

    #[must_use]
    pub fn state(&self) -> NativePageTransactionState { self.transaction.state() }

    /// Planned mutations, including an extent-preserving tombstone if needed.
    #[must_use]
    pub const fn page_change_count(&self) -> usize { self.pending_pages }

    /// Retained only after successful physical publication. Read-only completion
    /// has no acknowledgement; inspect `state` to distinguish it from active work.
    #[must_use]
    pub fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement> {
        self.acknowledgement.as_ref()
    }

    /// Keep this ticket before dropping an indeterminate owner for recovery.
    ///
    /// # Errors
    /// Propagates inconsistent admission metadata; performs no I/O or retry.
    pub fn commit_attempt(&self) -> Result<Option<NativeCommitAttempt>> {
        self.destination.commit_attempt(self.transaction.token())
    }

    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> {
        self.destination.outstanding_write()
    }

    /// Publish through ordinary page conflict validation and both durability
    /// barriers. No fallible work separates the returned acknowledgement from
    /// retaining it on this owner. Source times are not used as destination time.
    ///
    /// # Errors
    /// Propagates cancellation, budgets, conflicts, codec and storage failures.
    /// Pre-admission failures remain retryable; admitted failures require recovery.
    pub async fn commit(&mut self, cx: &Cx, now_unix_ns: u64) -> Result<()> {
        self.acknowledgement = self.destination
            .commit(cx, &mut self.transaction, now_unix_ns).await?;
        Ok(())
    }

    /// Discard an active private overlay without requiring a noncancelled Cx.
    ///
    /// # Errors
    /// Refuses committed or indeterminate installations, as ordinary rollback does.
    pub fn rollback(&mut self) -> Result<()> {
        self.destination.rollback(&mut self.transaction)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
    use crate::native_pages::NativePageLimits;
    use crate::test_support::FutureResultTestExt;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_vfs::{MemoryVfs, Vfs};

    #[derive(Clone, Default)]
    struct TestCodec {
        fail_encode: Arc<AtomicBool>,
    }

    impl NativeObjectCodec for TestCodec {
        fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            if self.fail_encode.load(Ordering::Relaxed) {
                return Err(FrankenError::Unsupported);
            }
            let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(
                ObjectId::derive_from_canonical_bytes(bytes),
                Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
                0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
            )])
        }

        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            let bytes = reconstruct_systematic_happy_path(records)
                .map_err(|error| super::super::corrupt(&error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id {
                return Err(super::super::corrupt("test snapshot object identity mismatch"));
            }
            Ok(bytes)
        }
    }

    type File = <MemoryVfs as Vfs>::File;
    type Store = NativePageStore<File, File, TestCodec>;

    fn page(number: u32) -> PageNumber { PageNumber::new(number).unwrap() }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
        vfs.open(
            cx, Some(std::path::Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
        ).unwrap().0
    }

    fn store(vfs: &MemoryVfs, cx: &Cx, name: &str, size: u32, codec: TestCodec) -> Store {
        let log = NativeDurabilityLog::create(
            cx, file(vfs, cx, &format!("{name}.objects")),
            file(vfs, cx, &format!("{name}.markers")), NativeDurabilityLimits::default(),
        ).unwrap();
        NativePageStore::new(log, codec, size, NativePageLimits::default()).unwrap()
    }

    fn write(db: &mut Store, cx: &Cx, number: u32, data: Option<&[u8]>, time: u64) {
        let mut transaction = db.begin(cx).unwrap();
        db.write_page(cx, &mut transaction, page(number), data).unwrap();
        db.commit(cx, &mut transaction, time).expect("commit fixture page");
    }

    fn image() -> NativePageSnapshot {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut source = store(&vfs, &cx, "source", 512, TestCodec::default());
        write(&mut source, &cx, 1, Some(&[1; 512]), 10);
        let snapshot = source.capture_snapshot(&cx).unwrap();
        source.close(&cx).unwrap();
        snapshot
    }

    #[test]
    fn fork_publishes_one_independent_history_and_recovers_its_sparse_extent() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut source = store(&vfs, &cx, "source", 512, TestCodec::default());
        let mut initial = source.begin(&cx).unwrap();
        for number in [1, 7, 999] {
            source.write_page(&cx, &mut initial, page(number), Some(&[1; 512])).unwrap();
        }
        source.commit(&cx, &mut initial, 10).expect("initial source");
        write(&mut source, &cx, 999, None, 20);
        let snapshot = source.capture_snapshot(&cx).unwrap();
        write(&mut source, &cx, 1, Some(&[3; 512]), 30);
        source.close(&cx).unwrap();

        let mut destination = store(&vfs, &cx, "fork", 512, TestCodec::default());
        let mut old_reader = destination.begin(&cx).unwrap();
        {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            assert_eq!(install.source_commit_seq(), CommitSeq::new(2));
            assert_eq!(install.source_commit_time_unix_ns(), Some(20));
            assert_eq!(install.page_change_count(), 3);
            assert_eq!(install.state(), NativePageTransactionState::Active);
            assert!(install.acknowledgement().is_none());
            install.commit(&cx, 100).expect("atomic snapshot fork");
            assert_eq!(install.state(), NativePageTransactionState::Committed);
            let ack = install.acknowledgement().unwrap();
            assert_eq!(ack.commit_seq, CommitSeq::new(1));
            assert_eq!(ack.txn_token, install.transaction_token());
            assert!(install.rollback().is_err());
        }
        assert!(destination.read_page(&cx, &mut old_reader, page(1)).unwrap().is_none());
        destination.rollback(&mut old_reader).unwrap();
        let fork = destination.capture_snapshot(&cx).unwrap();
        assert_eq!(fork.database_size_bound(), 999);
        assert_eq!(fork.page_numbers().collect::<Vec<_>>(), vec![page(1), page(7)]);
        for number in [1, 7, 999] {
            assert_eq!(fork.read_page(page(number)), snapshot.read_page(page(number)));
        }
        write(&mut destination, &cx, 1, Some(&[9; 512]), 200);
        assert_eq!(snapshot.read_page(page(1)).unwrap().as_ref(), &[1; 512]);
        assert_eq!(fork.read_page(page(1)).unwrap().as_ref(), &[1; 512]);
        destination.close(&cx).unwrap();
        let (mut reopened, _) = Store::recover(
            &cx, file(&vfs, &cx, "fork.objects"), file(&vfs, &cx, "fork.markers"),
            TestCodec::default(), 512, NativeDurabilityLimits::default(), NativePageLimits::default(),
        ).expect("recover actual fork capsules and marker");
        assert_eq!(reopened.committed_tip(), CommitSeq::new(2));
        let mut reader = reopened.begin(&cx).unwrap();
        assert_eq!(reopened.read_page(&cx, &mut reader, page(1)).unwrap().unwrap().as_ref(), &[9; 512]);
        assert_eq!(reopened.read_page(&cx, &mut reader, page(7)).unwrap().unwrap().as_ref(), &[1; 512]);
        assert!(reopened.read_page(&cx, &mut reader, page(999)).unwrap().is_none());
        assert_eq!(reopened.allocate_page(&cx, &mut reader).unwrap(), page(1000));
        reopened.rollback(&mut reader).unwrap();
        reopened.close(&cx).unwrap();
    }

    #[test]
    fn dropped_and_rolled_back_forks_publish_nothing() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let snapshot = image();
        let mut destination = store(&vfs, &cx, "fork", 512, TestCodec::default());
        drop(snapshot.prepare_fork(&cx, &mut destination).unwrap());
        assert_eq!(destination.committed_tip(), CommitSeq::ZERO);
        {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            // An unpolled finalizer has no effect, and does not own the overlay.
            drop(install.commit(&cx, 100));
            assert_eq!(install.state(), NativePageTransactionState::Active);
            install.rollback().unwrap();
            install.rollback().unwrap();
            assert_eq!(install.state(), NativePageTransactionState::RolledBack);
            assert!(install.acknowledgement().is_none());
        }
        assert_eq!(destination.capture_snapshot(&cx).unwrap().page_count(), 0);
        assert_eq!(file(&vfs, &cx, "fork.objects").file_size(&cx).unwrap(), 0);
        assert_eq!(file(&vfs, &cx, "fork.markers").file_size(&cx).unwrap(), 0);
        destination.close(&cx).unwrap();
    }

    #[test]
    fn fork_refuses_non_genesis_and_mismatched_page_size() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let snapshot = image();
        let mut nonempty = store(&vfs, &cx, "occupied", 512, TestCodec::default());
        // No present pages is not genesis: this tombstone is committed history.
        write(&mut nonempty, &cx, 50, None, 1);
        assert!(matches!(snapshot.prepare_fork(&cx, &mut nonempty), Err(FrankenError::Unsupported)));
        assert_eq!(nonempty.committed_tip(), CommitSeq::new(1));
        assert_eq!(nonempty.capture_snapshot(&cx).unwrap().database_size_bound(), 50);
        let mut wrong_size = store(&vfs, &cx, "wrong-size", 1024, TestCodec::default());
        assert!(matches!(snapshot.prepare_fork(&cx, &mut wrong_size), Err(FrankenError::Unsupported)));
        let mut first = wrong_size.begin(&cx).unwrap();
        assert_eq!(first.token().id.get(), 1, "refusal precedes BEGIN");
        wrong_size.rollback(&mut first).unwrap();
        nonempty.close(&cx).unwrap();
        wrong_size.close(&cx).unwrap();
    }

    #[test]
    fn empty_fork_finishes_without_manufacturing_a_commit() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut source = store(&vfs, &cx, "empty", 512, TestCodec::default());
        let snapshot = source.capture_snapshot(&cx).unwrap();
        let mut destination = store(&vfs, &cx, "fork", 512, TestCodec::default());
        {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            assert_eq!(install.page_change_count(), 0);
            install.commit(&cx, 10).expect("read-only empty fork");
            assert_eq!(install.state(), NativePageTransactionState::Committed);
            assert!(install.acknowledgement().is_none());
            assert!(install.commit_attempt().unwrap().is_none());
        }
        assert_eq!(destination.committed_tip(), CommitSeq::ZERO);
        assert_eq!(file(&vfs, &cx, "fork.markers").file_size(&cx).unwrap(), 0);
        source.close(&cx).unwrap();
        destination.close(&cx).unwrap();
    }

    #[test]
    fn all_deleted_fork_preserves_high_water_without_expanding_holes() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut source = store(&vfs, &cx, "deleted", 512, TestCodec::default());
        write(&mut source, &cx, 1_000_000, None, 10);
        let snapshot = source.capture_snapshot(&cx).unwrap();
        assert_eq!(snapshot.page_count(), 0);
        let mut destination = store(&vfs, &cx, "fork", 512, TestCodec::default());
        {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            assert_eq!(install.page_change_count(), 1);
            install.commit(&cx, 20).expect("one high-water tombstone");
            assert!(install.acknowledgement().is_some());
        }
        assert_eq!(destination.retained_page_bytes(), 0);
        assert_eq!(destination.retained_version_count(), 1);
        let mut transaction = destination.begin(&cx).unwrap();
        assert_eq!(destination.allocate_page(&cx, &mut transaction).unwrap(), page(1_000_001));
        destination.rollback(&mut transaction).unwrap();
        source.close(&cx).unwrap();
        destination.close(&cx).unwrap();
    }

    #[test]
    fn fork_capsule_admission_includes_metadata_and_extent_tombstone() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        for (name, size, count, extent) in [
            ("metadata", 65_536_u32, 64_u32, 64_u32),
            ("extent", 512, 4096, 4097),
        ] {
            let data: Arc<[u8]> = Arc::from(vec![1; usize::try_from(size).unwrap()]);
            let snapshot = NativePageSnapshot::from_validated_pages(
                CommitSeq::new(20), Some(200), size, extent,
                (1..=count).map(|number| (page(number), Arc::clone(&data))).collect(),
                usize::try_from(size).unwrap() * usize::try_from(count).unwrap(),
            );
            let mut destination = store(&vfs, &cx, name, size, TestCodec::default());
            assert!(matches!(snapshot.prepare_fork(&cx, &mut destination), Err(FrankenError::TooBig)));
            let mut first = destination.begin(&cx).unwrap();
            assert_eq!(first.token().id.get(), 1, "budget refusal must precede BEGIN");
            destination.rollback(&mut first).unwrap();
            assert_eq!(file(&vfs, &cx, &format!("{name}.objects")).file_size(&cx).unwrap(), 0);
            destination.close(&cx).unwrap();
        }
    }

    #[test]
    fn cancelled_fork_can_rollback_without_a_live_context() {
        let cx = Cx::new();
        let cancelled = Cx::new();
        cancelled.cancel();
        let vfs = MemoryVfs::new();
        let snapshot = image();
        let mut destination = store(&vfs, &cx, "fork", 512, TestCodec::default());
        assert!(matches!(snapshot.prepare_fork(&cancelled, &mut destination), Err(FrankenError::Interrupt)));
        {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            assert_eq!(install.transaction_token().id.get(), 1);
            assert!(matches!(install.commit(&cancelled, 10).wait(), Err(FrankenError::Interrupt)));
            assert_eq!(install.state(), NativePageTransactionState::Active);
            assert!(install.commit_attempt().unwrap().is_none());
            install.rollback().unwrap();
        }
        assert_eq!(destination.committed_tip(), CommitSeq::ZERO);
        destination.close(&cx).unwrap();
    }

    #[test]
    fn pre_admission_codec_failure_retains_the_original_transaction_for_retry() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let snapshot = image();
        let codec = TestCodec::default();
        let flag = Arc::clone(&codec.fail_encode);
        let mut destination = store(&vfs, &cx, "fork", 512, codec);
        {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            let token = install.transaction_token();
            flag.store(true, Ordering::Relaxed);
            assert!(install.commit(&cx, 10).wait().is_err());
            assert_eq!(install.state(), NativePageTransactionState::Active);
            assert_eq!(install.transaction_token(), token);
            assert!(install.acknowledgement().is_none());
            assert!(install.commit_attempt().unwrap().is_none());
            flag.store(false, Ordering::Relaxed);
            install.commit(&cx, 20).expect("retry unchanged private snapshot");
            assert_eq!(install.acknowledgement().unwrap().txn_token, token);
        }
        assert_eq!(destination.committed_tip(), CommitSeq::new(1));
        destination.close(&cx).unwrap();
    }

    #[test]
    fn admitted_fork_failure_retains_attempt_and_requires_original_owner_recovery() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let snapshot = image();
        let mut destination = store(&vfs, &cx, "fork", 512, TestCodec::default());
        let attempt = {
            let mut install = snapshot.prepare_fork(&cx, &mut destination).unwrap();
            // Simulate a changed physical append window before the first staged
            // write. This deliberately violates the normal exclusive append
            // lease and exercises the existing post-admission failure path.
            file(&vfs, &cx, "fork.objects")
                .write(&cx, &[0x7F], 0)
                .expect("move the physical append window");
            assert!(install.commit(&cx, 10).wait().is_err());
            assert_eq!(install.state(), NativePageTransactionState::Indeterminate);
            assert!(install.acknowledgement().is_none());
            assert!(matches!(install.rollback(), Err(FrankenError::BusyRecovery)));
            install.commit_attempt().unwrap().expect("retain admitted identity")
        };
        assert!(destination.needs_recovery());
        assert_eq!(destination.committed_tip(), CommitSeq::ZERO);
        assert!(matches!(destination.begin(&cx), Err(FrankenError::BusyRecovery)));
        // Releasing the install drops its pin, not its source-write or marker
        // obligations. The original store remains transferable for recovery.
        assert!(destination.try_into_recovery().is_ok());
        drop(attempt);
    }
}
