//! Native transactions in the engine's existing `TransactionKind` dispatcher.
//!
//! Only the generic native handle is erased, not the transaction protocol.
//! Conversion moves that exact handle, retaining its private overlay, snapshot
//! pin, savepoints, commit acknowledgement, and recovery obligations. The
//! erased trait is private and implemented only by `NativeTransaction`; callers
//! cannot inject another validator or a mock that reports invented durability.
//! Compatibility variants remain statically dispatched and pay no boxing cost.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use fsqlite_error::Result;
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, PageData, PageNumber, PageSize, WitnessKey};
use fsqlite_vfs::VfsFile;
use fsqlite_wal::native_commit::durable::{DurableCommitAcknowledgement, NativeObjectCodec};

use super::NativeTransaction;
use crate::traits::{PagerCommitState, TransactionHandle, TransactionKind, TransactionMode, sealed};

// Storage futures deliberately inherit the current-thread engine contract.
// The handle remains Send; adding Send here would change the caller/runtime
// contract and cannot be justified by the type erasure itself.
type NativeFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

trait ErasedNativeTransaction: Send {
    fn get_page<'a>(&'a self, cx: &'a Cx, page: PageNumber) -> NativeFuture<'a, PageData>;
    fn write_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: &'a [u8]) -> NativeFuture<'a, ()>;
    fn write_page_data<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: PageData) -> NativeFuture<'a, ()>;
    fn restore_staged_page_data<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: PageData) -> NativeFuture<'a, ()>;
    fn allocate_page<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, PageNumber>;
    fn free_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber) -> NativeFuture<'a, ()>;
    fn commit<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, ()>;
    fn commit_at<'a>(&'a mut self, cx: &'a Cx, now_unix_ns: u64) -> NativeFuture<'a, ()>;
    fn settle_commit<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, PagerCommitState>;
    fn commit_and_retain<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, bool>;
    fn rollback<'a>(&'a mut self, cx: &'a Cx) -> NativeFuture<'a, ()>;
    fn prefetch_page_hint(&self, cx: &Cx, page: PageNumber);
    fn forget_cached_page(&self, page: PageNumber);
    fn try_take_staged_page_data(&mut self, page: PageNumber) -> Option<PageData>;
    fn try_mutate_staged_page_data(&mut self, page: PageNumber, f: &mut dyn FnMut(&mut PageData)) -> bool;
    fn pager_commit_state(&self) -> PagerCommitState;
    fn is_writer(&self) -> bool;
    fn has_pending_writes(&self) -> bool;
    fn published_visible_commit_seq_hint(&self) -> Option<CommitSeq>;
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>>;
    fn pending_conflict_pages(&self) -> Result<Vec<PageNumber>>;
    fn pending_conflict_pages_conservative(&self) -> Vec<PageNumber>;
    fn write_set_page_numbers(&self) -> Vec<PageNumber>;
    fn page_one_in_pending_commit_surface(&self) -> Result<bool>;
    fn page_size(&self) -> PageSize;
    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool>;
    fn free_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool>;
    fn write_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool>;
    fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey);
    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()>;
    fn release_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()>;
    fn rollback_to_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()>;
    fn mode(&self) -> TransactionMode;
    fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement>;
    fn snapshot_db_size(&self) -> u32;
    fn live_db_size(&self) -> u32;
    fn visible_db_size_bound(&self) -> u32;
    fn live_reserved_pages(&self) -> Vec<PageNumber>;
}

impl<S, M, C> ErasedNativeTransaction for NativeTransaction<S, M, C>
where
    S: VfsFile,
    M: VfsFile,
    C: NativeObjectCodec + Send + Sync,
{
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
        Box::pin(NativeTransaction::commit_at(self, cx, now_unix_ns))
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
    fn mode(&self) -> TransactionMode { NativeTransaction::mode(self) }
    fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement> {
        NativeTransaction::acknowledgement(self)
    }
    fn snapshot_db_size(&self) -> u32 { self.transaction.borrow().snapshot_db_size() }
    fn live_db_size(&self) -> u32 { self.transaction.borrow().live_db_size() }
    fn visible_db_size_bound(&self) -> u32 { self.transaction.borrow().visible_db_size_bound() }
    fn live_reserved_pages(&self) -> Vec<PageNumber> { self.transaction.borrow().live_reserved_pages() }
}

/// The native variant carried by [`TransactionKind`]. It owns the original
/// sealed native handle; conversion performs no I/O, snapshot capture, or copy
/// into a compatibility transaction. It cannot be constructed from arbitrary
/// `TransactionHandle` implementations and offers no mutable downcast escape.
///
/// Futures are boxed only on this native path. Creating an unpolled mutation
/// future does not mutate the transaction, and dropping a polled future retains
/// the original native owner's indeterminate-write behavior.
pub struct NativeTransactionDispatch {
    inner: Box<dyn ErasedNativeTransaction>,
}

impl<S, M, C> From<NativeTransaction<S, M, C>> for TransactionKind
where
    S: VfsFile + 'static,
    M: VfsFile + 'static,
    C: NativeObjectCodec + Send + Sync + 'static,
{
    fn from(transaction: NativeTransaction<S, M, C>) -> Self {
        Self::Native(NativeTransactionDispatch { inner: Box::new(transaction) })
    }
}

impl fmt::Debug for NativeTransactionDispatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeTransactionDispatch")
            .field("snapshot", &self.inner.published_visible_commit_seq_hint())
            .field("commit_state", &self.inner.pager_commit_state())
            .finish_non_exhaustive()
    }
}

impl NativeTransactionDispatch {
    /// Original admission mode; conversion never downgrades read-only handles.
    #[must_use]
    pub fn mode(&self) -> TransactionMode { self.inner.mode() }

    /// Original native receipt, including the transaction identity. Read-only
    /// completion and uncertain writes do not manufacture an acknowledgement.
    #[must_use]
    pub fn acknowledgement(&self) -> Option<&DurableCommitAcknowledgement> {
        self.inner.acknowledgement()
    }

    /// Commit with an explicit clock sample, preserving native ordering rules.
    ///
    /// # Errors
    /// Propagates conflicts, resource limits and indeterminate I/O unchanged.
    pub async fn commit_at(&mut self, cx: &Cx, now_unix_ns: u64) -> Result<()> {
        self.inner.commit_at(cx, now_unix_ns).await
    }

    /// Fixed committed logical address bound at BEGIN, including tombstones.
    #[must_use]
    pub fn snapshot_db_size(&self) -> u32 { self.inner.snapshot_db_size() }

    /// Snapshot plus surviving private writes, or this handle's committed extent.
    #[must_use]
    pub fn live_db_size(&self) -> u32 { self.inner.live_db_size() }

    /// Snapshot plus addresses issued to this handle, including ROLLBACK TO gaps.
    #[must_use]
    pub fn visible_db_size_bound(&self) -> u32 { self.inner.visible_db_size_bound() }

    /// Bounded private reservations, not an enumeration of sparse address holes.
    #[must_use]
    pub fn live_reserved_pages(&self) -> Vec<PageNumber> { self.inner.live_reserved_pages() }
}

impl sealed::Sealed for NativeTransactionDispatch {}

impl TransactionHandle for NativeTransactionDispatch {
    async fn get_page<'a>(&'a self, cx: &'a Cx, page: PageNumber) -> Result<PageData> {
        self.inner.get_page(cx, page).await
    }
    async fn write_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: &'a [u8]) -> Result<()> {
        self.inner.write_page(cx, page, data).await
    }
    async fn write_page_data<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: PageData) -> Result<()> {
        self.inner.write_page_data(cx, page, data).await
    }
    async fn restore_staged_page_data<'a>(&'a mut self, cx: &'a Cx, page: PageNumber, data: PageData) -> Result<()> {
        self.inner.restore_staged_page_data(cx, page, data).await
    }
    async fn allocate_page<'a>(&'a mut self, cx: &'a Cx) -> Result<PageNumber> {
        self.inner.allocate_page(cx).await
    }
    async fn free_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber) -> Result<()> {
        self.inner.free_page(cx, page).await
    }
    async fn commit<'a>(&'a mut self, cx: &'a Cx) -> Result<()> { self.inner.commit(cx).await }
    async fn settle_commit<'a>(&'a mut self, cx: &'a Cx) -> Result<PagerCommitState> {
        self.inner.settle_commit(cx).await
    }
    async fn commit_and_retain<'a>(&'a mut self, cx: &'a Cx) -> Result<bool> {
        self.inner.commit_and_retain(cx).await
    }
    async fn rollback<'a>(&'a mut self, cx: &'a Cx) -> Result<()> { self.inner.rollback(cx).await }
    fn prefetch_page_hint(&self, cx: &Cx, page: PageNumber) { self.inner.prefetch_page_hint(cx, page); }
    fn forget_cached_page(&self, page: PageNumber) { self.inner.forget_cached_page(page); }
    fn try_take_staged_page_data(&mut self, page: PageNumber) -> Option<PageData> {
        self.inner.try_take_staged_page_data(page)
    }
    fn try_mutate_staged_page_data(&mut self, page: PageNumber, f: &mut dyn FnMut(&mut PageData)) -> bool {
        self.inner.try_mutate_staged_page_data(page, f)
    }
    fn pager_commit_state(&self) -> PagerCommitState { self.inner.pager_commit_state() }
    fn is_writer(&self) -> bool { self.inner.is_writer() }
    fn has_pending_writes(&self) -> bool { self.inner.has_pending_writes() }
    fn published_visible_commit_seq_hint(&self) -> Option<CommitSeq> {
        self.inner.published_visible_commit_seq_hint()
    }
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> { self.inner.pending_commit_pages() }
    fn pending_conflict_pages(&self) -> Result<Vec<PageNumber>> { self.inner.pending_conflict_pages() }
    fn pending_conflict_pages_conservative(&self) -> Vec<PageNumber> {
        self.inner.pending_conflict_pages_conservative()
    }
    fn write_set_page_numbers(&self) -> Vec<PageNumber> { self.inner.write_set_page_numbers() }
    fn page_one_in_pending_commit_surface(&self) -> Result<bool> {
        self.inner.page_one_in_pending_commit_surface()
    }
    fn page_size(&self) -> PageSize { self.inner.page_size() }
    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> {
        self.inner.allocate_page_requires_page_one_conflict_tracking()
    }
    fn free_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        self.inner.free_page_requires_page_one_conflict_tracking(page)
    }
    fn write_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        self.inner.write_page_requires_page_one_conflict_tracking(page)
    }
    fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey) { self.inner.record_write_witness(cx, key); }
    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> { self.inner.savepoint(cx, name) }
    fn release_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.inner.release_savepoint(cx, name)
    }
    fn rollback_to_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.inner.rollback_to_savepoint(cx, name)
    }
}
