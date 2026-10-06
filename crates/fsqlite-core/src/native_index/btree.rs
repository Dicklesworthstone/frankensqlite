#![allow(clippy::future_not_send)]
//! Real B-tree cursors over native snapshot page transactions.
//!
//! These are the existing table/index/overflow algorithms, not a second tree
//! implementation. Every traversal reads through `NativePageStore`, retaining
//! its physical page observations. The page store's conservative optimistic
//! validation remains authoritative; granular witness hints cannot weaken it.
//!
//! Mutations are scoped to a private savepoint. Only a successful callback
//! accepts them into the transaction overlay. An error, panic, or dropped
//! operation future restores that overlay without performing storage I/O.
//! Durable publication still requires the owner's explicit page-store commit.
//! The companion [`catalog`] persists schema/root bindings. Public SQL dispatch
//! and cross-process admission are not supplied by this adapter.

pub mod catalog;

use std::cell::{Cell, RefCell};
use std::ops::AsyncFnOnce;
use std::rc::Rc;

use fsqlite_btree::{BtCursor, PageReader, PageWriter};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageNumber, WitnessKey};
use fsqlite_vfs::VfsFile;
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_pages::{NativePageSavepoint, NativePageStore, NativePageTransaction};

struct ScopeState<'a, S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    store: &'a NativePageStore<S, M, C>,
    txn: RefCell<&'a mut NativePageTransaction>,
    point: NativePageSavepoint,
    failed: Cell<bool>,
    active: Cell<bool>,
    accepted: Cell<bool>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> ScopeState<'_, S, M, C> {
    fn access<T>(&self, f: impl FnOnce(&mut NativePageTransaction) -> Result<T>) -> Result<T> {
        if !self.active.get() || self.failed.get() {
            return Err(FrankenError::Abort);
        }
        let result = self.txn.try_borrow_mut()
            .map_err(|_| FrankenError::Abort)
            .and_then(|mut txn| f(&mut txn));
        if result.is_err() {
            // Even if the operation callback catches an I/O/limit error, a
            // partially completed tree mutation cannot be accepted afterward.
            self.failed.set(true);
        }
        result
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> Drop for ScopeState<'_, S, M, C> {
    fn drop(&mut self) {
        if !self.accepted.get() {
            let txn = self.txn.get_mut();
            // Neither cleanup operation observes cancellation. No publication
            // can run while this state holds the transaction's mutable borrow.
            if self.store.rollback_to(txn, &self.point).is_err()
                || self.store.release_savepoint(txn, &self.point).is_err()
            {
                // A violated private-marker invariant must fail closed rather
                // than leave an unknown partial overlay eligible for commit.
                let _ = self.store.rollback(txn);
            }
        }
    }
}

struct MutationScope<'a, S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    state: Rc<ScopeState<'a, S, M, C>>,
}

impl<'a, S: VfsFile, M: VfsFile, C: NativeObjectCodec> MutationScope<'a, S, M, C> {
    fn new(cx: &Cx, store: &'a NativePageStore<S, M, C>, txn: &'a mut NativePageTransaction) -> Result<Self> {
        let point = store.savepoint(cx, txn)?;
        Ok(Self {
            state: Rc::new(ScopeState {
                store, txn: RefCell::new(txn), point,
                failed: Cell::new(false), active: Cell::new(true), accepted: Cell::new(false),
            }),
        })
    }

    fn page_io(&self) -> NativeBtreePageIo<'a, S, M, C> {
        NativeBtreePageIo { state: Rc::clone(&self.state) }
    }

    fn finish<T>(self, result: Result<T>) -> Result<T> {
        self.state.active.set(false);
        let value = result?; // Preserve the callback's original error.
        if self.state.failed.get() || Rc::strong_count(&self.state) != 1 {
            return Err(FrankenError::Abort);
        }
        {
            let mut txn = self.state.txn.try_borrow_mut().map_err(|_| FrankenError::Abort)?;
            self.state.store.release_savepoint(&mut txn, &self.state.point)?;
        }
        self.state.accepted.set(true);
        Ok(value)
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> Drop for MutationScope<'_, S, M, C> {
    fn drop(&mut self) {
        self.state.active.set(false);
    }
}

/// Cursor page I/O exposed only through [`with_native_btree`].
///
/// No public constructor or mutable transaction accessor can bypass the
/// mutation scope. Page reads, writes, allocation and tombstones all use the
/// same owner-bound transaction. RefCell borrows never cross an await point.
pub struct NativeBtreePageIo<'a, S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    state: Rc<ScopeState<'a, S, M, C>>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> PageReader for NativeBtreePageIo<'_, S, M, C> {
    async fn read_page<'a>(&'a self, cx: &'a Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        self.state.access(|txn| {
            let page = self.state.store.read_page(cx, txn, page_no)?
                .ok_or_else(|| malformed("B-tree references an absent native page"))?;
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(page.len()).map_err(|_| FrankenError::OutOfMemory)?;
            bytes.extend_from_slice(&page);
            Ok(bytes)
        })
    }

    fn is_dirty(&self, page_no: PageNumber) -> bool {
        self.state.active.get() && !self.state.failed.get()
            && self.state.txn.try_borrow().is_ok_and(|txn| txn.is_page_dirty(page_no))
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> PageWriter for NativeBtreePageIo<'_, S, M, C> {
    async fn write_page<'a>(&'a mut self, cx: &'a Cx, page_no: PageNumber, data: &'a [u8]) -> Result<()> {
        self.state.access(|txn| self.state.store.write_page(cx, txn, page_no, Some(data)))
    }

    async fn allocate_page<'a>(&'a mut self, cx: &'a Cx) -> Result<PageNumber> {
        self.state.access(|txn| self.state.store.allocate_page(cx, txn))
    }

    async fn free_page<'a>(&'a mut self, cx: &'a Cx, page_no: PageNumber) -> Result<()> {
        self.state.access(|txn| {
            if self.state.store.read_page(cx, txn, page_no)?.is_none() {
                return Err(malformed("B-tree freed an absent native page"));
            }
            self.state.store.write_page(cx, txn, page_no, None)
        })
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {
        // The full-page read/write path records every base observation. This
        // conservative profile does not substitute unvalidated cell hints for
        // those dependencies. The default read-witness hook has the same rule.
    }
}

/// Initialize a fresh table or index root in the transaction's private overlay.
///
/// Allocator pages start above the database-header page, so their B-tree header
/// is at byte zero. A 65,536-byte page encodes its initial content offset as zero.
/// The returned root is not registered in a schema catalog or durably committed.
///
/// # Errors
/// Propagates owner/state, allocation, overlay-size and cancellation failures.
/// Any failure restores the private overlay and keeps allocation numbers spent.
pub fn create_native_btree<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, store: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction, is_table: bool,
) -> Result<PageNumber> {
    let scope = MutationScope::new(cx, store, txn)?;
    let result = scope.state.access(|txn| {
        let page = store.allocate_page(cx, txn)?;
        let size = usize::try_from(store.page_size()).map_err(|_| FrankenError::TooBig)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(|_| FrankenError::OutOfMemory)?;
        bytes.resize(size, 0);
        bytes[0] = if is_table { 0x0D } else { 0x0A };
        let content_offset = if store.page_size() == 65_536 {
            0
        } else {
            u16::try_from(store.page_size()).map_err(|_| FrankenError::TooBig)?
        };
        bytes[5..7].copy_from_slice(&content_offset.to_be_bytes());
        store.write_page(cx, txn, page, Some(&bytes))?;
        Ok(page)
    });
    scope.finish(result)
}

/// Run existing B-tree algorithms in a rollback-on-drop private mutation scope.
///
/// Success accepts the callback's edits into `txn`; it does not commit them.
/// Errors, panics, abandoned futures and swallowed page-I/O errors restore the
/// previous overlay. Snapshot read observations and allocator high water remain
/// monotone across rollback. The cursor is dropped before scope completion, so
/// no cached page image from an abandoned operation can escape into a later one.
/// A read-only callback uses the same path and records the pages it traverses.
///
/// # Errors
/// Rejects missing/wrong-kind roots and unavailable/foreign transactions; returns
/// the callback's error or an error encountered by the page-I/O adapter.
pub async fn with_native_btree<'a, S, M, C, F, T>(
    cx: &Cx,
    store: &'a NativePageStore<S, M, C>,
    txn: &'a mut NativePageTransaction,
    root: PageNumber,
    is_table: bool,
    operation: F,
) -> Result<T>
where
    S: VfsFile,
    M: VfsFile,
    C: NativeObjectCodec,
    F: for<'b> AsyncFnOnce(&'b mut BtCursor<NativeBtreePageIo<'a, S, M, C>>) -> Result<T>,
{
    let scope = MutationScope::new(cx, store, txn)?;
    scope.state.access(|txn| {
        let bytes = store.read_page(cx, txn, root)?
            .ok_or_else(|| malformed("native B-tree root is absent"))?;
        let offset = if root.get() == 1 { 100 } else { 0 };
        let flag = bytes.get(offset).copied().ok_or_else(|| malformed("short native B-tree root"))?;
        if (is_table && !matches!(flag, 0x05 | 0x0D))
            || (!is_table && !matches!(flag, 0x02 | 0x0A))
        {
            return Err(malformed("native B-tree root type mismatch"));
        }
        Ok(())
    })?;
    let mut cursor = BtCursor::new(scope.page_io(), root, store.page_size(), is_table);
    let result = operation(&mut cursor).await;
    drop(cursor);
    scope.finish(result)
}

fn malformed(detail: &str) -> FrankenError {
    FrankenError::DatabaseCorrupt { detail: detail.to_owned() }
}
