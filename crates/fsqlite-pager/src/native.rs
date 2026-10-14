//! Sealed pager transactions over the native capsule/page-store path.
//!
//! Generic consumers of `TransactionHandle` (including `TransactionPageIo`)
//! can use native durability without substituting a compatibility WAL. This
//! owner deliberately does not implement `MvccPager`: that trait's journal
//! configuration currently describes only rollback journals and SQLite WALs.
//! Native handles can enter `TransactionKind` through the sealed dispatch
//! adapter. Ordinary public Connection admission is still separate.
//!
//! Transactions hold private overlays and snapshot pins, not a store lock.
//! Ordinary page operations briefly share the owner's read guard. Publication
//! alone takes its exclusive guard, matching NativePageStore's existing `&mut`
//! boundary. All lock admission is nonblocking: contention returns `Busy`, not
//! a parked executor thread. A commit future may be !Send; its handle is Send.
//! The caller still owns durable namespace creation and the external append
//! lease, which must outlive abandoned source-owned VFS writes and final close.

pub mod dispatch;

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
use fsqlite_wal::native_durability::NativeRecovery;
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

    /// Publish caller-selected handles from this owner through one shared
    /// two-barrier native commit. Cloned pagers share the same admission domain;
    /// foreign or terminal handles reject the entire group before publication.
    /// Each successful writer retains its own acknowledgement; read-only handles
    /// finish without a marker. Savepoints and snapshot dependencies remain on
    /// the original handles until the page store reports successful publication.
    ///
    /// This is explicit grouping, not an automatic queue or a cross-transaction
    /// crash-atomic promise. After an uncertain write every affected handle keeps
    /// `InDoubt`, its dirty projection and its active-slot obligation. Dropping
    /// the future does not settle the source write or release the external lease.
    /// No lock is held for a transaction's lifetime; this uses the same exclusive
    /// publication boundary as a singleton and returns `Busy` on contention.
    ///
    /// # Errors
    /// Rejects invalid handles, cancellation, contention or page/proof limits.
    /// Propagates conflicts and storage failures without clearing failed handles.
    #[allow(clippy::await_holding_lock)] // One existing publication boundary for the whole group.
    pub async fn commit_batch_at(
        &self,
        cx: &Cx,
        transactions: &mut [&mut NativeTransaction<S, M, C>],
        now_unix_ns: u64,
    ) -> Result<()> {
        checkpoint(cx)?;
        if transactions.len() > NativePageStore::<S, M, C>::MAX_COMMIT_GROUP {
            return Err(FrankenError::TooBig);
        }
        for transaction in transactions.iter() {
            if !Arc::ptr_eq(&self.shared, &transaction.shared) {
                return Err(FrankenError::Abort);
            }
            transaction.active()?;
        }
        let mut store = self.shared.store.try_write().ok_or(FrankenError::Busy)?;
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(FrankenError::BusyRecovery);
        }
        let acknowledgements = {
            let mut pages = Vec::new();
            pages.try_reserve_exact(transactions.len()).map_err(|_| FrankenError::OutOfMemory)?;
            for transaction in transactions.iter_mut() {
                pages.push(transaction.transaction.get_mut());
            }
            store.commit_batch(cx, &mut pages, now_unix_ns).await?
        };
        debug_assert_eq!(acknowledgements.len(), transactions.len());
        // No await, allocation or callback remains before every handle is
        // finalized. Close cannot race active-slot release under this guard.
        for (transaction, acknowledgement) in transactions.iter_mut().zip(acknowledgements) {
            transaction.acknowledgement = acknowledgement;
            transaction.complete();
        }
        drop(store);
        Ok(())
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

    /// Move a uniquely owned, quiescent pager into physical recovery.
    ///
    /// Drop other pager clones, commit workers and every transaction handle
    /// first, including terminal handles and handles erased into TransactionKind.
    /// A drained worker's returned handles still belong to the caller; this
    /// method never cancels its accepted work or discards a completion receipt.
    /// Raw page-store transactions created before pager adoption also block the
    /// transfer until their snapshot pins retire.
    ///
    /// The original streams, acknowledged prefix, outstanding source-write
    /// token and shared codec are retained by the page store's transfer. No
    /// I/O, locking wait, rollback, hidden runtime or automatic retry occurs.
    /// Pending writes still require settlement before recovery may start.
    /// Continuously retain the external namespace/append lease, then drive
    /// NativeRecovery and NativePageRecovery to reconstruct a new pager.
    ///
    /// # Errors
    /// Returns the pager with its storage obligations intact while shared,
    /// while closing, or when the underlying page store refuses transfer.
    #[allow(clippy::result_large_err)] // Preserve ownership on refusal.
    pub fn try_into_recovery(
        self,
    ) -> std::result::Result<(NativeRecovery<S, M>, Arc<C>), Self> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(self);
        }
        // A count-then-take sequence would race another holder. try_unwrap
        // proves exclusive ownership in one operation, including all worker
        // and erased-handle references, without taking a publication lock.
        let shared = match Arc::try_unwrap(self.shared) {
            Ok(shared) => shared,
            Err(shared) => return Err(Self { shared }),
        };
        let Shared { store, page_size, active, closed } = shared;
        match store.into_inner().try_into_recovery() {
            Ok(recovery) => Ok(recovery),
            Err(store) => Err(Self {
                shared: Arc::new(Shared { store: RwLock::new(store), page_size, active, closed }),
            }),
        }
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
    pub async fn commit_at(&mut self, cx: &Cx, now_unix_ns: u64) -> Result<()> {
        let owner = NativePager { shared: Arc::clone(&self.shared) };
        owner.commit_batch_at(cx, &mut [self], now_unix_ns).await
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
        // The quiescent pager can transfer its original streams into recovery;
        // source-owned writes must settle before that recovery performs I/O.
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

    async fn recover_live_pager(cx: &Cx, owner: Pager) -> Pager {
        use fsqlite_wal::native_pages::NativePageRecovery;
        let (mut recovery, codec) = owner.try_into_recovery().ok().expect("quiescent live owner");
        recovery.prepare(cx, |id, records| {
            std::future::ready(codec.decode(cx, id, &records).map(|_| ()))
        }).await.unwrap();
        let mut replay = NativePageRecovery::with_shared_codec(
            recovery.finish().unwrap(), codec, 512, NativePageLimits::default(),
        );
        replay.prepare(cx).await.unwrap();
        let (store, _) = replay.finish().unwrap();
        NativePager::new(store).unwrap()
    }

    #[test]
    fn native_recovery_transfer_refuses_clones_and_erased_terminal_handles() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let peer = owner.clone();
            let owner = owner.try_into_recovery().err().expect("live peer still owns admission");
            assert!(Arc::ptr_eq(&owner.shared, &peer.shared));
            drop(peer);
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.write_page(&cx, page(2), &[7; 512]).await.unwrap();
            let owner = owner.try_into_recovery().err().expect("private transaction retained");
            assert_eq!(txn.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[7; 512]);
            txn.commit_at(&cx, 100).await.unwrap();
            let ack = txn.acknowledgement().unwrap().clone();
            let erased: crate::traits::TransactionKind = txn.into();
            let owner = owner.try_into_recovery().err().expect("terminal erased owner retained");
            assert_eq!(erased.pager_commit_state(), PagerCommitState::Committed);
            match &erased {
                crate::traits::TransactionKind::Native(native) => {
                    assert_eq!(native.acknowledgement(), Some(&ack));
                }
                _ => panic!("native transaction changed storage kind"),
            }
            drop(erased);
            let owner = recover_live_pager(&cx, owner).await;
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(1));
            let mut next = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            assert_eq!(next.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[7; 512]);
            next.write_page(&cx, page(3), &[8; 512]).await.unwrap();
            next.commit_at(&cx, 0).await.unwrap();
            assert_eq!(next.acknowledgement().unwrap().commit_time_unix_ns, 101);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn native_recovery_transfer_does_not_cancel_queued_service_work_or_lose_receipts() {
        use crate::native_service::NativeCommitService;
        run(async {
            let cx = Cx::new(); let native = asupersync::Cx::current().unwrap();
            cx.set_native_cx(native.clone());
            let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let (sender, worker) = NativeCommitService::new(&owner, 2, 2).unwrap();
            let owner = owner.try_into_recovery().err().expect("idle worker retains its pager");
            let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            txn.write_page(&cx, page(2), &[9; 512]).await.unwrap();
            let mut ticket = sender.reserve(&native).await.unwrap().send(txn, 100).unwrap();
            let owner = owner.try_into_recovery().err().expect("accepted work cannot be evacuated");
            sender.request_shutdown();
            worker.run(&cx).await.unwrap();
            let completion = ticket.wait(&native).await.unwrap();
            assert!(completion.result.is_ok());
            assert_eq!(completion.transaction.pager_commit_state(), PagerCommitState::Committed);
            assert_eq!(completion.transaction.acknowledgement().unwrap().commit_seq, CommitSeq::new(1));
            let owner = owner.try_into_recovery().err().expect("receipt handle remains caller owned");
            drop(completion); drop(ticket); drop(sender);
            let owner = recover_live_pager(&cx, owner).await;
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::new(1));
            let mut reader = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert_eq!(reader.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[9; 512]);
            reader.rollback(&cx).await.unwrap(); owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn native_recovery_transfer_honors_raw_pins_created_before_pager_adoption() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let log = NativeDurabilityLog::create(
                &cx, file(&vfs, &cx, "objects"), file(&vfs, &cx, "markers"),
                NativeDurabilityLimits::default(),
            ).unwrap();
            let mut store = NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap();
            let mut pin = store.begin(&cx).unwrap();
            store.write_page(&cx, &mut pin, page(2), Some(&[3; 512])).unwrap();
            let owner = NativePager::new(store).unwrap();
            assert_eq!(Arc::strong_count(&owner.shared), 1);
            let owner = owner.try_into_recovery().err().expect("raw pin survives unique pager check");
            assert_eq!(pin.state(), NativePageTransactionState::Active);
            assert_eq!(owner.committed_tip().unwrap(), CommitSeq::ZERO);
            drop(pin);
            let owner = recover_live_pager(&cx, owner).await;
            let mut reader = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert!(reader.get_page(&cx, page(2)).await.is_err(), "uncommitted raw overlay was not published");
            reader.rollback(&cx).await.unwrap(); owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn native_recovery_transfer_preserves_failed_and_closed_admission_domains() {
        run(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
            let mut first = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            first.write_page(&cx, page(2), &[1; 512]).await.unwrap();
            first.commit_at(&cx, 100).await.unwrap(); drop(first);
            let mut symbols = file(&vfs, &cx, "objects");
            let original_len = symbols.file_size(&cx).unwrap();
            // An unexpected source length produces a real staging failure;
            // do not synthesize an InDoubt enum value or fake completion.
            symbols.write(&cx, b"unexpected", original_len).await.unwrap();
            let mut failed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            failed.write_page(&cx, page(2), &[2; 512]).await.unwrap();
            assert!(failed.commit_at(&cx, 200).await.is_err());
            assert_eq!(failed.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(matches!(failed.settle_commit(&cx).await, Err(FrankenError::BusyRecovery)));
            let owner = owner.try_into_recovery().err().expect("uncertain transaction still owned");
            symbols.truncate(&cx, original_len).unwrap();
            symbols.close(&cx).unwrap();
            assert!(owner.needs_recovery().unwrap(), "restoring bytes alone does not reset admission");
            drop(failed);
            let owner = recover_live_pager(&cx, owner).await;
            let mut reader = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert_eq!(reader.get_page(&cx, page(2)).await.unwrap().as_bytes(), &[1; 512]);
            reader.rollback(&cx).await.unwrap(); drop(reader);
            owner.close(&cx).unwrap();
            let owner = owner.try_into_recovery().err().expect("close cannot become recovery");
            assert!(owner.begin(&cx, TransactionMode::Concurrent).is_err());
            owner.close(&cx).unwrap();
        });
    }
}
