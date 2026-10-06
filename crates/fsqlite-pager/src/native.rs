//! Sealed pager transactions over the native capsule/page-store path.
//!
//! Generic consumers of `TransactionHandle` (including `TransactionPageIo`)
//! can use native durability without substituting a compatibility WAL. This
//! owner deliberately does not implement `MvccPager`: that trait's journal
//! configuration currently describes only rollback journals and SQLite WALs.
//! Public Connection/TransactionKind dispatch is not changed here.
//!
//! Transactions hold private overlays and snapshot pins, not a store lock.
//! Ordinary page operations briefly share the owner's read guard. Publication
//! alone takes its exclusive guard, matching NativePageStore's existing `&mut`
//! boundary. All lock admission is nonblocking: contention returns `Busy`, not
//! a parked executor thread. A commit future may be !Send; its handle is Send.
//! The caller still owns durable namespace creation and the external append
//! lease, which must outlive abandoned source-owned VFS writes and final close.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::sync_primitives::{RwLock, RwLockReadGuard};
use fsqlite_types::{CommitSeq, PageData, PageNumber, PageSize};
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};
use fsqlite_wal::native_commit::durable::{DurableCommitAcknowledgement, NativeObjectCodec};
use fsqlite_wal::native_pages::{
    NativePageSavepoint, NativePageStore, NativePageTransaction, NativePageTransactionState,
};

use crate::traits::{PagerCommitState, TransactionHandle, TransactionMode, sealed};

const MAX_SAVEPOINT_NAME_BYTES: usize = 1024;

struct Shared<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    store: RwLock<NativePageStore<S, M, C>>,
    page_size: PageSize,
    active: AtomicUsize,
    closed: AtomicBool,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> Shared<S, M, C> {
    fn read(&self) -> Result<RwLockReadGuard<'_, NativePageStore<S, M, C>>> {
        let store = self.store.try_read().ok_or(FrankenError::Busy)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(store)
    }
}

/// Shared admission/close owner for native sealed transaction handles.
///
/// Clones share one page store. Beginning multiple transactions does not retain
/// a publication guard; disjoint private writers remain independently active.
/// `Immediate`/`Exclusive` are refused instead of silently providing weaker
/// semantics. No compatibility journal or native SQL selector is synthesized.
pub struct NativePager<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    shared: Arc<Shared<S, M, C>>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> Clone for NativePager<S, M, C> {
    fn clone(&self) -> Self {
        Self { shared: Arc::clone(&self.shared) }
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePager<S, M, C> {
    /// Adopt an already-created or recovered store without altering its pages.
    /// Recovery-blocked stores can be inspected/closed, but cannot admit work.
    ///
    /// # Errors
    /// Rejects an invalid underlying page size.
    pub fn new(store: NativePageStore<S, M, C>) -> Result<Self> {
        let page_size = PageSize::new(store.page_size()).ok_or(FrankenError::Unsupported)?;
        Ok(Self {
            shared: Arc::new(Shared {
                store: RwLock::new(store), page_size,
                active: AtomicUsize::new(0), closed: AtomicBool::new(false),
            }),
        })
    }

    /// Begin a fixed-snapshot transaction. Deferred remains optimistic/concurrent.
    ///
    /// # Errors
    /// Refuses exclusive modes, contention, closed/recovery-blocked storage,
    /// cancellation and the underlying active-session/resource limits.
    pub fn begin(&self, cx: &Cx, mode: TransactionMode) -> Result<NativeTransaction<S, M, C>> {
        checkpoint(cx)?;
        if matches!(mode, TransactionMode::Immediate | TransactionMode::Exclusive) {
            return Err(FrankenError::Unsupported);
        }
        let mut store = self.shared.store.try_write().ok_or(FrankenError::Busy)?;
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(FrankenError::BusyRecovery);
        }
        let transaction = store.begin(cx)?;
        let snapshot = transaction.snapshot();
        self.shared.active.fetch_add(1, Ordering::AcqRel);
        Ok(NativeTransaction {
            shared: Arc::clone(&self.shared), transaction: RefCell::new(transaction),
            mode, snapshot, dirty: Vec::new(), savepoints: Vec::new(),
            was_writer: false, owns_slot: true, acknowledgement: None,
        })
    }

    /// Latest fully published page-store sequence, not a reserved sequence.
    ///
    /// # Errors
    /// Returns Busy during another operation's exclusive publication/close.
    pub fn committed_tip(&self) -> Result<CommitSeq> {
        Ok(self.shared.read()?.committed_tip())
    }

    /// Whether the native owner requires storage recovery before admission.
    ///
    /// # Errors
    /// Returns Busy while an exclusive operation still owns the store.
    pub fn needs_recovery(&self) -> Result<bool> {
        let store = self.shared.store.try_read().ok_or(FrankenError::Busy)?;
        Ok(self.shared.closed.load(Ordering::Acquire) || store.needs_recovery())
    }

    /// Retained source-owned write completion after an abandoned commit.
    ///
    /// # Errors
    /// Returns Busy while the publication future still holds its guard.
    pub fn outstanding_write(&self) -> Result<Option<VfsWriteCompletion>> {
        let store = self.shared.store.try_read().ok_or(FrankenError::Busy)?;
        Ok(store.outstanding_write())
    }

    /// Close only after all active handles finish/drop. Failed physical closes
    /// remain retryable, but no new transaction may enter after close starts.
    /// This does not force rollback, delete files, or release an external lease.
    ///
    /// # Errors
    /// Returns Busy for active work/contention, otherwise the retained VFS error.
    pub fn close(&self, cx: &Cx) -> Result<()> {
        let mut store = self.shared.store.try_write().ok_or(FrankenError::Busy)?;
        if self.shared.active.load(Ordering::Acquire) != 0 {
            return Err(FrankenError::Busy);
        }
        self.shared.closed.store(true, Ordering::Release);
        store.close(cx)
    }
}

struct NamedSavepoint {
    name: String,
    marker: NativePageSavepoint,
    dirty: Vec<PageNumber>,
}

/// A sealed native pager handle. Its snapshot and native commit state remain
/// bound to the same transaction throughout reads, savepoints and publication.
///
/// The dirty-page list mirrors only this private overlay, including tombstones.
/// It is never used instead of NativePageStore's authoritative read validation.
/// Dropping an active handle releases its private overlay/pin. Dropping an
/// indeterminate handle does NOT assert that its marker is absent; the shared
/// owner remains recovery-blocked and retains its tracked-write obligation.
pub struct NativeTransaction<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    shared: Arc<Shared<S, M, C>>,
    transaction: RefCell<NativePageTransaction>,
    mode: TransactionMode,
    snapshot: CommitSeq,
    dirty: Vec<PageNumber>,
    savepoints: Vec<NamedSavepoint>,
    was_writer: bool,
    owns_slot: bool,
    acknowledgement: Option<DurableCommitAcknowledgement>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativeTransaction<S, M, C> {
    #[must_use]
    pub const fn mode(&self) -> TransactionMode { self.mode }

    /// The exact native receipt, absent for read-only completion or failed I/O.
    #[must_use]
    pub fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement> {
        self.acknowledgement.as_ref()
    }

    fn active(&self) -> Result<()> {
        match self.transaction.borrow().state() {
            NativePageTransactionState::Active => Ok(()),
            NativePageTransactionState::Indeterminate => Err(FrankenError::BusyRecovery),
            _ => Err(FrankenError::Abort),
        }
    }

    fn writable(&self) -> Result<()> {
        self.active()?;
        if self.mode == TransactionMode::ReadOnly { return Err(FrankenError::ReadOnly); }
        Ok(())
    }

    fn reserve_dirty(&mut self) -> Result<()> {
        self.dirty.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)
    }

    fn mark_dirty(&mut self, page: PageNumber) {
        if let Err(index) = self.dirty.binary_search(&page) { self.dirty.insert(index, page); }
        self.was_writer = true;
    }

    fn release_slot(&mut self) {
        if self.owns_slot {
            self.owns_slot = false;
            let previous = self.shared.active.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous != 0);
        }
    }

    fn complete(&mut self) {
        self.dirty.clear();
        self.savepoints.clear();
        self.release_slot();
    }

    fn savepoint_index(&self, name: &str) -> Result<usize> {
        self.active()?;
        self.savepoints.iter().rposition(|point| point.name == name)
            .ok_or_else(|| FrankenError::Internal(format!("no savepoint named '{name}'")))
    }

    /// Commit using an explicit wall-clock sample (useful for deterministic
    /// callers). Native sequencing enforces its monotonic timestamp rule.
    /// The native transaction becomes indeterminate before mutating I/O, so
    /// dropping this future cannot reset PagerCommitState to NotCommitted.
    ///
    /// # Errors
    /// Propagates conflicts, limits, cancellation and uncertain storage failures.
    #[allow(clippy::await_holding_lock)] // Only the existing exclusive publication boundary.
    pub async fn commit_at(&mut self, cx: &Cx, now_unix_ns: u64) -> Result<()> {
        self.active()?;
        checkpoint(cx)?;
        let mut store = self.shared.store.try_write().ok_or(FrankenError::Busy)?;
        if self.shared.closed.load(Ordering::Acquire) { return Err(FrankenError::BusyRecovery); }
        let acknowledgement = store.commit(cx, self.transaction.get_mut(), now_unix_ns).await?;
        drop(store);
        self.acknowledgement = acknowledgement;
        self.complete();
        Ok(())
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> Drop for NativeTransaction<S, M, C> {
    fn drop(&mut self) { self.release_slot(); }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> sealed::Sealed for NativeTransaction<S, M, C> {}

impl<S, M, C> TransactionHandle for NativeTransaction<S, M, C>
where
    S: VfsFile + Send + Sync,
    M: VfsFile + Send + Sync,
    C: NativeObjectCodec + Send + Sync,
{
    async fn get_page<'a>(&'a self, cx: &'a Cx, page_no: PageNumber) -> Result<PageData> {
        self.active()?;
        let store = self.shared.read()?;
        let mut transaction = self.transaction.try_borrow_mut().map_err(|_| FrankenError::Abort)?;
        let bytes = store.read_page(cx, &mut transaction, page_no)?.ok_or_else(|| {
            FrankenError::DatabaseCorrupt { detail: format!("native page {} is absent", page_no.get()) }
        })?;
        let mut owned = Vec::new();
        owned.try_reserve_exact(bytes.len()).map_err(|_| FrankenError::OutOfMemory)?;
        owned.extend_from_slice(&bytes);
        Ok(PageData::from_vec(owned))
    }

    async fn write_page<'a>(&'a mut self, cx: &'a Cx, page_no: PageNumber, data: &'a [u8]) -> Result<()> {
        self.writable()?;
        self.reserve_dirty()?;
        let store = self.shared.read()?;
        store.write_page(cx, self.transaction.get_mut(), page_no, Some(data))?;
        drop(store);
        self.mark_dirty(page_no);
        Ok(())
    }

    async fn allocate_page<'a>(&'a mut self, cx: &'a Cx) -> Result<PageNumber> {
        self.writable()?;
        self.reserve_dirty()?;
        let store = self.shared.read()?;
        let page = store.allocate_page(cx, self.transaction.get_mut())?;
        drop(store);
        self.mark_dirty(page);
        Ok(page)
    }

    async fn free_page<'a>(&'a mut self, cx: &'a Cx, page_no: PageNumber) -> Result<()> {
        self.writable()?;
        self.reserve_dirty()?;
        let store = self.shared.read()?;
        let transaction = self.transaction.get_mut();
        if store.read_page(cx, transaction, page_no)?.is_none() {
            return Err(FrankenError::DatabaseCorrupt { detail: "free of absent native page".to_owned() });
        }
        store.write_page(cx, transaction, page_no, None)?;
        drop(store);
        self.mark_dirty(page_no);
        Ok(())
    }

    async fn commit<'a>(&'a mut self, cx: &'a Cx) -> Result<()> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|_| FrankenError::OutOfRange {
                what: "native commit wall clock".to_owned(), value: "before Unix epoch".to_owned(),
            })?.as_nanos();
        let nanos = u64::try_from(nanos).map_err(|_| FrankenError::TooBig)?;
        self.commit_at(cx, nanos).await
    }

    fn pager_commit_state(&self) -> PagerCommitState {
        match self.transaction.borrow().state() {
            NativePageTransactionState::Active | NativePageTransactionState::RolledBack => PagerCommitState::NotCommitted,
            NativePageTransactionState::Indeterminate => PagerCommitState::InDoubt,
            NativePageTransactionState::Committed => PagerCommitState::Committed,
        }
    }

    async fn settle_commit<'a>(&'a mut self, _cx: &'a Cx) -> Result<PagerCommitState> {
        // Reissuing commit here would duplicate an uncertain physical attempt.
        // Recovery must reopen the retained streams after old writes settle.
        let state = self.pager_commit_state();
        if state == PagerCommitState::InDoubt { return Err(FrankenError::BusyRecovery); }
        Ok(state)
    }

    fn is_writer(&self) -> bool { self.was_writer }
    fn has_pending_writes(&self) -> bool { !self.dirty.is_empty() }
    fn published_visible_commit_seq_hint(&self) -> Option<CommitSeq> { Some(self.snapshot) }
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> { Ok(self.dirty.clone()) }
    fn write_set_page_numbers(&self) -> Vec<PageNumber> { self.dirty.clone() }
    fn page_size(&self) -> PageSize { self.shared.page_size }

    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> {
        self.writable()?;
        Ok(false) // The native allocator does not synthesize a page-one write.
    }
    fn free_page_requires_page_one_conflict_tracking(&self, _page: PageNumber) -> Result<bool> {
        self.writable()?;
        Ok(false) // The freed page itself is a versioned tombstone.
    }
    fn write_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        self.writable()?;
        Ok(page == PageNumber::ONE)
    }

    async fn rollback<'a>(&'a mut self, _cx: &'a Cx) -> Result<()> {
        // Private cleanup must remain possible with a cancelled context.
        let store = self.shared.read()?;
        store.rollback(self.transaction.get_mut())?;
        drop(store);
        self.complete();
        Ok(())
    }

    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.active()?;
        if name.len() > MAX_SAVEPOINT_NAME_BYTES { return Err(FrankenError::TooBig); }
        let mut owned_name = String::new();
        owned_name.try_reserve_exact(name.len()).map_err(|_| FrankenError::OutOfMemory)?;
        owned_name.push_str(name);
        let mut dirty = Vec::new();
        dirty.try_reserve_exact(self.dirty.len()).map_err(|_| FrankenError::OutOfMemory)?;
        dirty.extend_from_slice(&self.dirty);
        self.savepoints.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        let store = self.shared.read()?;
        let marker = store.savepoint(cx, self.transaction.get_mut())?;
        self.savepoints.push(NamedSavepoint { name: owned_name, marker, dirty });
        Ok(())
    }

    fn release_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let index = self.savepoint_index(name)?;
        let store = self.shared.read()?;
        store.release_savepoint(self.transaction.get_mut(), &self.savepoints[index].marker)?;
        self.savepoints.truncate(index);
        Ok(())
    }

    fn rollback_to_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let index = self.savepoint_index(name)?;
        let mut dirty = Vec::new();
        dirty.try_reserve_exact(self.savepoints[index].dirty.len()).map_err(|_| FrankenError::OutOfMemory)?;
        dirty.extend_from_slice(&self.savepoints[index].dirty);
        let store = self.shared.read()?;
        store.rollback_to(self.transaction.get_mut(), &self.savepoints[index].marker)?;
        self.dirty = dirty;
        self.savepoints.truncate(index + 1);
        Ok(())
    }
}

fn checkpoint(cx: &Cx) -> Result<()> { cx.checkpoint().map_err(|_| FrankenError::Interrupt) }

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::Path;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
    use fsqlite_wal::native_pages::NativePageLimits;

    use super::*;

    struct TestCodec;
    impl NativeObjectCodec for TestCodec {
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
                .map_err(|error| FrankenError::Internal(error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id { return Err(FrankenError::Abort); }
            Ok(bytes)
        }
    }
    type File = <MemoryVfs as Vfs>::File;
    type Pager = NativePager<File, File, TestCodec>;
    type Transaction = NativeTransaction<File, File, TestCodec>;

    fn run(future: impl Future<Output = ()>) {
        RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
    }
    fn page(number: u32) -> PageNumber { PageNumber::new(number).unwrap() }
    fn file(vfs: &MemoryVfs, cx: &Cx, path: &str) -> File {
        vfs.open(cx, Some(Path::new(path)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
    }
    fn pager(vfs: &MemoryVfs, cx: &Cx) -> Pager {
        let log = NativeDurabilityLog::create(cx, file(vfs, cx, "objects"), file(vfs, cx, "markers"),
            NativeDurabilityLimits::default()).unwrap();
        NativePager::new(NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()).unwrap()
    }

    #[test]
    fn sealed_handles_keep_private_writes_and_fixed_snapshots() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut a = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            let mut b = owner.clone().begin(&cx, TransactionMode::Deferred).unwrap();
            let pa = a.allocate_page(&cx).await.unwrap();
            let pb = b.allocate_page(&cx).await.unwrap();
            assert_ne!(pa, pb);
            a.write_page(&cx, pa, &[1; 512]).await.unwrap();
            b.write_page(&cx, pb, &[2; 512]).await.unwrap();
            assert_eq!(a.get_page(&cx, pa).await.unwrap().as_bytes(), &[1; 512]);
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::ZERO);
            a.commit_at(&cx, 100).await.unwrap();
            b.commit_at(&cx, 101).await.unwrap();
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(2));
            assert_eq!(b.published_visible_commit_seq_hint(), Some(CommitSeq::ZERO));
            assert_eq!(b.acknowledgement().unwrap().commit_seq, CommitSeq::new(2));
            assert!(!a.has_pending_writes());
            let mut old = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let mut writer = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            writer.write_page(&cx, pa, &[3; 512]).await.unwrap();
            writer.commit_at(&cx, 102).await.unwrap();
            assert_eq!(old.get_page(&cx, pa).await.unwrap().as_bytes(), &[1; 512]);
            assert_eq!(old.get_page(&cx, pb).await.unwrap().as_bytes(), &[2; 512]);
            old.commit_at(&cx, 103).await.unwrap();
            assert!(old.acknowledgement().is_none());
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(3));
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn read_only_and_exclusive_mode_contracts_are_not_silently_weakened() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            assert!(matches!(owner.begin(&cx, TransactionMode::Immediate), Err(FrankenError::Unsupported)));
            assert!(matches!(owner.begin(&cx, TransactionMode::Exclusive), Err(FrankenError::Unsupported)));
            let mut reader = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert!(matches!(reader.allocate_page(&cx).await, Err(FrankenError::ReadOnly)));
            assert!(matches!(reader.write_page(&cx, page(9), &[0; 512]).await, Err(FrankenError::ReadOnly)));
            assert!(matches!(reader.free_page(&cx, page(9)).await, Err(FrankenError::ReadOnly)));
            assert!(!reader.is_writer()); assert!(!reader.has_pending_writes());
            reader.commit(&cx).await.unwrap();
            assert_eq!(reader.pager_commit_state(), PagerCommitState::Committed);
            assert_eq!(reader.settle_commit(&cx).await.unwrap(), PagerCommitState::Committed);
            assert!(reader.rollback(&cx).await.is_err());
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::ZERO);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn named_savepoints_restore_exact_dirty_pages_and_keep_read_dependencies() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.savepoint(&cx, "point").unwrap();
            txn.write_page(&cx, page(3), &[3; 512]).await.unwrap();
            txn.savepoint(&cx, "point").unwrap(); // Shadowing resolves the inner marker.
            txn.write_page(&cx, page(2), &[2; 512]).await.unwrap();
            txn.free_page(&cx, page(3)).await.unwrap();
            assert_eq!(txn.pending_commit_pages().unwrap(), vec![page(2), page(3)]);
            txn.rollback_to_savepoint(&cx, "point").unwrap();
            assert_eq!(txn.pending_conflict_pages().unwrap(), vec![page(3)]);
            assert_eq!(txn.get_page(&cx, page(3)).await.unwrap().as_bytes(), &[3; 512]);
            txn.release_savepoint(&cx, "point").unwrap();
            txn.rollback_to_savepoint(&cx, "point").unwrap();
            assert!(!txn.has_pending_writes()); assert!(txn.is_writer());
            let mut peer = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            peer.write_page(&cx, page(2), &[9; 512]).await.unwrap();
            peer.commit_at(&cx, 100).await.unwrap();
            txn.write_page(&cx, page(4), &[4; 512]).await.unwrap();
            assert!(matches!(txn.commit_at(&cx, 101).await, Err(FrankenError::BusySnapshot { .. })));
            assert_eq!(txn.pager_commit_state(), PagerCommitState::NotCommitted);
            assert_eq!(txn.settle_commit(&cx).await.unwrap(), PagerCommitState::NotCommitted);
            txn.rollback(&cx).await.unwrap(); owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn metadata_uses_native_allocation_and_reports_real_page_one_writes() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut txn = owner.begin(&cx, TransactionMode::Deferred).unwrap();
            assert_eq!(txn.page_size().get(), 512);
            assert!(!txn.allocate_page_requires_page_one_conflict_tracking().unwrap());
            let allocated = txn.allocate_page(&cx).await.unwrap();
            assert_eq!(allocated, page(2));
            assert!(!txn.page_one_in_pending_commit_surface().unwrap());
            assert!(!txn.free_page_requires_page_one_conflict_tracking(allocated).unwrap());
            assert!(txn.write_page_requires_page_one_conflict_tracking(PageNumber::ONE).unwrap());
            txn.write_page(&cx, PageNumber::ONE, &[0; 512]).await.unwrap();
            txn.free_page(&cx, allocated).await.unwrap();
            assert!(txn.page_one_in_pending_commit_surface().unwrap());
            assert_eq!(txn.write_set_page_numbers(), vec![PageNumber::ONE, allocated]);
            assert!(txn.get_page(&cx, allocated).await.is_err());
            assert!(txn.free_page(&cx, allocated).await.is_err());
            txn.rollback(&cx).await.unwrap(); owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn cancellation_does_not_prevent_private_savepoint_and_transaction_cleanup() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.savepoint(&cx, "before").unwrap();
            txn.write_page(&cx, page(2), &[2; 512]).await.unwrap();
            cx.cancel();
            assert!(txn.commit_at(&cx, 100).await.is_err());
            assert_eq!(txn.pager_commit_state(), PagerCommitState::NotCommitted);
            txn.rollback_to_savepoint(&cx, "before").unwrap();
            txn.release_savepoint(&cx, "before").unwrap();
            assert!(!txn.has_pending_writes());
            txn.rollback(&cx).await.unwrap();
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::ZERO);
            owner.close(&Cx::new()).unwrap();
        });
    }

    #[test]
    #[allow(clippy::await_holding_lock)] // Exercise nonblocking admission under deliberate contention.
    fn publication_contention_returns_busy_without_consuming_the_transaction() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.write_page(&cx, page(2), &[2; 512]).await.unwrap();
            let exclusive = owner.shared.store.try_write().unwrap();
            assert!(matches!(owner.begin(&cx, TransactionMode::Concurrent), Err(FrankenError::Busy)));
            assert!(matches!(txn.get_page(&cx, page(2)).await, Err(FrankenError::Busy)));
            assert!(matches!(txn.commit_at(&cx, 100).await, Err(FrankenError::Busy)));
            assert_eq!(txn.pager_commit_state(), PagerCommitState::NotCommitted);
            assert_eq!(txn.pending_commit_pages().unwrap(), vec![page(2)]);
            drop(exclusive);
            txn.commit_at(&cx, 100).await.unwrap();
            assert_eq!(txn.pager_commit_state(), PagerCommitState::Committed);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn handles_are_send_and_drop_releases_only_private_state() {
        fn assert_send<T: Send>() {}
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send::<Transaction>(); assert_send_sync::<Pager>();
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.write_page(&cx, page(2), &[2; 512]).await.unwrap();
            assert!(matches!(owner.close(&cx), Err(FrankenError::Busy)));
            drop(txn);
            let mut reader = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert!(reader.get_page(&cx, page(2)).await.is_err());
            reader.rollback(&cx).await.unwrap();
            owner.close(&cx).unwrap();
            assert!(owner.begin(&cx, TransactionMode::Concurrent).is_err());
            owner.close(&cx).unwrap(); // Underlying independent closes are idempotent.
        });
    }
}
