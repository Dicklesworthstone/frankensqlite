//! Read-only historical native images in the sealed transaction dispatcher.
//!
//! These handles have no live publisher, journal, allocator or physical file.
//! Every persistent page read is answered by the selected immutable image;
//! no absent historical page can fall through to a newer live database.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, PageData, PageNumber, PageSize, WitnessKey};
use fsqlite_vfs::VfsFile;
use fsqlite_wal::native_commit::durable::{DurableCommitAcknowledgement, NativeObjectCodec};
use fsqlite_wal::native_pages::{MAX_NATIVE_SAVEPOINTS, NativePageSnapshot};

use super::{ErasedNativeTransaction, NativeFuture, NativeTransactionDispatch};
use crate::native::NativePager;
use crate::traits::{PagerCommitState, TransactionHandle, TransactionKind, TransactionMode, sealed};

/// Read-only transaction factory for an already-validated historical image.
///
/// Cloning shares immutable page bytes. Beginning a reader performs no storage
/// I/O and retains no live publisher or transaction pin. The image may outlive
/// the source files and be read while its original or a reconstructed current pager
/// accepts writes. Only explicit `ReadOnly` admission is accepted; deferred or
/// concurrent modes must not be silently downgraded to historical read-only.
/// This is a pager/bytecode entry point, not public Connection SQL selection.
#[derive(Clone, Debug)]
pub struct NativeSnapshotPager {
    snapshot: NativePageSnapshot,
    page_size: PageSize,
}

impl NativeSnapshotPager {
    /// Adopt a published live image or validated native replay, without new I/O.
    ///
    /// # Errors
    /// Refuses a page size unsupported by the sealed pager.
    pub fn new(snapshot: NativePageSnapshot) -> Result<Self> {
        let page_size = PageSize::new(snapshot.page_size()).ok_or(FrankenError::Unsupported)?;
        Ok(Self { snapshot, page_size })
    }

    #[must_use]
    pub fn snapshot(&self) -> &NativePageSnapshot { &self.snapshot }

    /// Begin an independent read-only handle at this image's fixed commit.
    ///
    /// # Errors
    /// Refuses cancellation or any mode permitting writes.
    pub fn begin(&self, cx: &Cx, mode: TransactionMode) -> Result<NativeSnapshotTransaction> {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        if mode != TransactionMode::ReadOnly { return Err(FrankenError::ReadOnly); }
        Ok(NativeSnapshotTransaction {
            snapshot: self.snapshot.clone(), page_size: self.page_size,
            state: SnapshotState::Active, savepoints: Vec::new(),
        })
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePager<S, M, C> {
    /// Freeze the current committed database for independent read-only queries.
    ///
    /// Existing transactions and commit workers may stay alive. The ordinary
    /// shared read guard prevents publication only during capture, which visits
    /// retained versions and clones the latest page Arcs into a sparse index.
    /// The returned pager has no reference to this live owner, consumes no live
    /// transaction slot, and works after source close or history reclamation.
    /// Its readers can enter `TransactionKind` and the existing B-tree/VDBE path.
    ///
    /// There is no I/O, hidden task, recovery pass, or transaction-lifetime
    /// writer lock. This is not lock-free or a constant-time capture. It reads
    /// admitted in-memory pages, not an independent disk-integrity certificate;
    /// caller-retained images have their own memory cost. Private writes are
    /// excluded and no live transaction's conflict/read set is extended.
    ///
    /// # Errors
    /// Returns `Busy` during exclusive publication/close, `BusyRecovery` for a
    /// closed or indeterminate owner, or cancellation/allocation/history errors.
    pub fn capture_snapshot(&self, cx: &Cx) -> Result<NativeSnapshotPager> {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        let store = self.shared.read()?;
        NativeSnapshotPager::new(store.capture_snapshot(cx)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotState { Active, Committed, RolledBack }

/// Sealed read transaction over immutable historical native pages.
///
/// Conversion to `TransactionKind` preserves this exact image and lifecycle.
/// All mutation paths, including staged-page replacement, fail read-only.
/// `commit` ends a read transaction; it emits no marker or acknowledgement,
/// never updates a live commit clock, and cannot make the image writable.
/// Savepoints track only bounded reader lifecycle names, not mutable pages.
#[derive(Debug)]
pub struct NativeSnapshotTransaction {
    snapshot: NativePageSnapshot,
    page_size: PageSize,
    state: SnapshotState,
    savepoints: Vec<String>,
}

impl NativeSnapshotTransaction {
    fn active(&self) -> Result<()> {
        if self.state == SnapshotState::Active { Ok(()) } else { Err(FrankenError::Abort) }
    }

    fn refuse_write<T>(&self) -> Result<T> {
        self.active()?;
        Err(FrankenError::ReadOnly)
    }

    fn savepoint_index(&self, name: &str) -> Result<usize> {
        self.active()?;
        self.savepoints.iter().rposition(|point| point == name)
            .ok_or_else(|| FrankenError::Internal(format!("no savepoint named '{name}'")))
    }

    #[must_use]
    pub const fn mode(&self) -> TransactionMode { TransactionMode::ReadOnly }

    /// No historical read creates a new native commit receipt.
    #[must_use]
    pub const fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement> { None }

    /// Finish a reader without sampling or modifying a publisher clock.
    ///
    /// # Errors
    /// Refuses cancellation and already-finished handles.
    pub async fn commit_at(&mut self, cx: &Cx, _now_unix_ns: u64) -> Result<()> {
        TransactionHandle::commit(self, cx).await
    }

    #[must_use]
    pub fn snapshot_db_size(&self) -> u32 { self.snapshot.database_size_bound() }

    #[must_use]
    pub fn live_db_size(&self) -> u32 { self.snapshot.database_size_bound() }

    #[must_use]
    pub fn visible_db_size_bound(&self) -> u32 { self.snapshot.database_size_bound() }

    #[must_use]
    pub fn live_reserved_pages(&self) -> Vec<PageNumber> { Vec::new() }
}

impl sealed::Sealed for NativeSnapshotTransaction {}

impl TransactionHandle for NativeSnapshotTransaction {
    async fn get_page<'a>(&'a self, cx: &'a Cx, page: PageNumber) -> Result<PageData> {
        self.active()?;
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        let bytes = self.snapshot.read_page(page).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("page {} is absent at native historical commit {}", page.get(), self.snapshot.commit_seq().get()),
        })?;
        // PageData permits mutation by consumers; give them a private copy,
        // never a mutable alias into the snapshot or another reader's buffer.
        let mut owned = Vec::new();
        owned.try_reserve_exact(bytes.len()).map_err(|_| FrankenError::OutOfMemory)?;
        owned.extend_from_slice(&bytes);
        Ok(PageData::from_vec(owned))
    }

    async fn write_page<'a>(&'a mut self, _cx: &'a Cx, _page: PageNumber, _data: &'a [u8]) -> Result<()> {
        self.refuse_write()
    }
    async fn write_page_data<'a>(&'a mut self, _cx: &'a Cx, _page: PageNumber, _data: PageData) -> Result<()> {
        self.refuse_write()
    }
    async fn restore_staged_page_data<'a>(&'a mut self, _cx: &'a Cx, _page: PageNumber, _data: PageData) -> Result<()> {
        self.refuse_write()
    }
    async fn allocate_page<'a>(&'a mut self, _cx: &'a Cx) -> Result<PageNumber> { self.refuse_write() }
    async fn free_page<'a>(&'a mut self, _cx: &'a Cx, _page: PageNumber) -> Result<()> { self.refuse_write() }
    fn try_take_staged_page_data(&mut self, _page: PageNumber) -> Option<PageData> { None }
    fn try_mutate_staged_page_data(&mut self, _page: PageNumber, _f: &mut dyn FnMut(&mut PageData)) -> bool { false }

    async fn commit<'a>(&'a mut self, cx: &'a Cx) -> Result<()> {
        self.active()?;
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        self.state = SnapshotState::Committed;
        self.savepoints.clear();
        Ok(())
    }
    async fn rollback<'a>(&'a mut self, _cx: &'a Cx) -> Result<()> {
        if self.state == SnapshotState::Committed { return Err(FrankenError::Abort); }
        self.state = SnapshotState::RolledBack;
        self.savepoints.clear();
        Ok(())
    }
    async fn settle_commit<'a>(&'a mut self, _cx: &'a Cx) -> Result<PagerCommitState> {
        Ok(TransactionHandle::pager_commit_state(self))
    }
    fn pager_commit_state(&self) -> PagerCommitState {
        match self.state {
            SnapshotState::Committed => PagerCommitState::Committed,
            SnapshotState::Active | SnapshotState::RolledBack => PagerCommitState::NotCommitted,
        }
    }
    fn is_writer(&self) -> bool { false }
    fn has_pending_writes(&self) -> bool { false }
    fn published_visible_commit_seq_hint(&self) -> Option<CommitSeq> { Some(self.snapshot.commit_seq()) }
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> { Ok(Vec::new()) }
    fn write_set_page_numbers(&self) -> Vec<PageNumber> { Vec::new() }
    fn page_size(&self) -> PageSize { self.page_size }
    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> { self.refuse_write() }
    fn free_page_requires_page_one_conflict_tracking(&self, _page: PageNumber) -> Result<bool> { self.refuse_write() }
    fn write_page_requires_page_one_conflict_tracking(&self, _page: PageNumber) -> Result<bool> { self.refuse_write() }

    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.active()?;
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        if name.len() > 1024 || self.savepoints.len() >= MAX_NATIVE_SAVEPOINTS { return Err(FrankenError::TooBig); }
        let mut owned = String::new();
        owned.try_reserve_exact(name.len()).map_err(|_| FrankenError::OutOfMemory)?;
        owned.push_str(name);
        self.savepoints.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        self.savepoints.push(owned);
        Ok(())
    }
    fn release_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let index = self.savepoint_index(name)?;
        self.savepoints.truncate(index);
        Ok(())
    }
    fn rollback_to_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let index = self.savepoint_index(name)?;
        self.savepoints.truncate(index + 1);
        Ok(())
    }
}

impl From<NativeSnapshotTransaction> for TransactionKind {
    fn from(transaction: NativeSnapshotTransaction) -> Self {
        Self::Native(NativeTransactionDispatch { inner: Box::new(transaction) })
    }
}

// Keep the same private dispatch surface as live native transactions. The
// only additional implementation is this sealed, intrinsically read-only one.
impl ErasedNativeTransaction for NativeSnapshotTransaction {
    fn get_page<'a>(&'a self, cx: &'a Cx, page: PageNumber) -> NativeFuture<'a, PageData> {
        Box::pin(TransactionHandle::get_page(self, cx, page))
    }
    fn write_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: &'a [u8]) -> NativeFuture<'a, ()> {
        Box::pin(TransactionHandle::write_page(self, cx, page, data))
    }
    fn write_page_data<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: PageData) -> NativeFuture<'a, ()> {
        Box::pin(TransactionHandle::write_page_data(self, cx, page, data))
    }
    fn restore_staged_page_data<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: PageData) -> NativeFuture<'a, ()> {
        Box::pin(TransactionHandle::restore_staged_page_data(self, cx, page, data))
    }
    fn allocate_page<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, PageNumber> {
        Box::pin(TransactionHandle::allocate_page(self, cx))
    }
    fn free_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber) -> NativeFuture<'a, ()> {
        Box::pin(TransactionHandle::free_page(self, cx, page))
    }
    fn commit<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, ()> {
        Box::pin(TransactionHandle::commit(self, cx))
    }
    fn commit_at<'a>(&'a mut self, cx: &'a Cx, now_unix_ns: u64) -> NativeFuture<'a, ()> {
        Box::pin(Self::commit_at(self, cx, now_unix_ns))
    }
    fn settle_commit<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, PagerCommitState> {
        Box::pin(TransactionHandle::settle_commit(self, cx))
    }
    fn commit_and_retain<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, bool> {
        Box::pin(TransactionHandle::commit_and_retain(self, cx))
    }
    fn rollback<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, ()> {
        Box::pin(TransactionHandle::rollback(self, cx))
    }
    fn prefetch_page_hint(&self, cx: &Cx, page: PageNumber) {
        TransactionHandle::prefetch_page_hint(self, cx, page);
    }
    fn forget_cached_page(&self, page: PageNumber) {
        TransactionHandle::forget_cached_page(self, page);
    }
    fn try_take_staged_page_data(&mut self, page: PageNumber) -> Option<PageData> {
        TransactionHandle::try_take_staged_page_data(self, page)
    }
    fn try_mutate_staged_page_data(&mut self, page: PageNumber, f: &mut dyn FnMut(&mut PageData)) -> bool {
        TransactionHandle::try_mutate_staged_page_data(self, page, f)
    }
    fn pager_commit_state(&self) -> PagerCommitState { TransactionHandle::pager_commit_state(self) }
    fn is_writer(&self) -> bool { TransactionHandle::is_writer(self) }
    fn has_pending_writes(&self) -> bool { TransactionHandle::has_pending_writes(self) }
    fn published_visible_commit_seq_hint(&self) -> Option<CommitSeq> {
        TransactionHandle::published_visible_commit_seq_hint(self)
    }
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> { TransactionHandle::pending_commit_pages(self) }
    fn pending_conflict_pages(&self) -> Result<Vec<PageNumber>> { TransactionHandle::pending_conflict_pages(self) }
    fn pending_conflict_pages_conservative(&self) -> Vec<PageNumber> {
        TransactionHandle::pending_conflict_pages_conservative(self)
    }
    fn write_set_page_numbers(&self) -> Vec<PageNumber> { TransactionHandle::write_set_page_numbers(self) }
    fn page_one_in_pending_commit_surface(&self) -> Result<bool> {
        TransactionHandle::page_one_in_pending_commit_surface(self)
    }
    fn page_size(&self) -> PageSize { TransactionHandle::page_size(self) }
    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> {
        TransactionHandle::allocate_page_requires_page_one_conflict_tracking(self)
    }
    fn free_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        TransactionHandle::free_page_requires_page_one_conflict_tracking(self, page)
    }
    fn write_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        TransactionHandle::write_page_requires_page_one_conflict_tracking(self, page)
    }
    fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey) {
        TransactionHandle::record_write_witness(self, cx, key);
    }
    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        TransactionHandle::savepoint(self, cx, name)
    }
    fn release_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        TransactionHandle::release_savepoint(self, cx, name)
    }
    fn rollback_to_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        TransactionHandle::rollback_to_savepoint(self, cx, name)
    }
    fn mode(&self) -> TransactionMode { Self::mode(self) }
    fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement> {
        Self::acknowledgement(self)
    }
    fn snapshot_db_size(&self) -> u32 { Self::snapshot_db_size(self) }
    fn live_db_size(&self) -> u32 { Self::live_db_size(self) }
    fn visible_db_size_bound(&self) -> u32 { Self::visible_db_size_bound(self) }
    fn live_reserved_pages(&self) -> Vec<PageNumber> { Self::live_reserved_pages(self) }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod online_capture_tests {
    use std::path::Path;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore};

    use super::*;

    struct Codec;
    impl NativeObjectCodec for Codec {
        fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(bytes),
                Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
                0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START)])
        }
        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            let bytes = reconstruct_systematic_happy_path(records)
                .map_err(|error| FrankenError::Internal(error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id { return Err(FrankenError::Abort); }
            Ok(bytes)
        }
    }

    #[test]
    fn online_capture_refuses_contention_cancellation_and_close_without_invalidating_readers() {
        RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(async {
            let cx = Cx::new(); let vfs = MemoryVfs::new();
            let stream = |name: &str| vfs.open(&cx, Some(Path::new(name)),
                VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0;
            let log = NativeDurabilityLog::create(&cx, stream("objects"), stream("markers"),
                NativeDurabilityLimits::default()).unwrap();
            let owner = NativePager::new(NativePageStore::new(log, Codec, 512,
                NativePageLimits::default()).unwrap()).unwrap();
            let page = PageNumber::new(2).unwrap();
            let mut seed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
            // Both TransactionHandle and ErasedNativeTransaction are in scope here.
            TransactionHandle::write_page(&mut seed, &cx, page, &[7; 512]).await.unwrap();
            seed.commit_at(&cx, 10).await.unwrap();
            let image = owner.capture_snapshot(&cx).unwrap();
            let mut reader = image.begin(&cx, TransactionMode::ReadOnly).unwrap();
            {
                // Exercise the real shared-owner exclusion boundary, not an
                // injected status flag or a count-then-take ownership check.
                let _publication = owner.shared.store.try_write().unwrap();
                assert!(matches!(owner.capture_snapshot(&cx), Err(FrankenError::Busy)));
            }
            let cancelled = Cx::new(); cancelled.cancel();
            assert!(matches!(owner.capture_snapshot(&cancelled), Err(FrankenError::Interrupt)));
            assert!(!owner.needs_recovery().unwrap());
            assert_eq!(owner.capture_snapshot(&cx).unwrap().snapshot().commit_seq(), CommitSeq::new(1));
            assert_eq!(TransactionHandle::pager_commit_state(&seed), PagerCommitState::Committed);
            owner.close(&cx).unwrap();
            assert!(matches!(owner.capture_snapshot(&cx), Err(FrankenError::BusyRecovery)));
            assert_eq!(TransactionHandle::get_page(&reader, &cx, page).await.unwrap().as_bytes(), &[7; 512]);
            reader.commit_at(&cx, u64::MAX).await.unwrap();
            assert!(reader.acknowledgement().is_none());
            assert_eq!(image.snapshot().commit_time_unix_ns(), Some(10));
        });
    }
}
