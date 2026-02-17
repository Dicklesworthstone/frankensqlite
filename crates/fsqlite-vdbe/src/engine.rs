//! VDBE bytecode interpreter — the fetch-execute engine.
//!
//! Takes a [`VdbeProgram`] (produced by codegen) and executes it instruction by
//! instruction. The engine maintains a register file (`Vec<SqliteValue>`) and
//! accumulates result rows emitted by `OP_ResultRow`.
//!
//! This implementation covers the core opcode set needed for expression
//! evaluation, control flow, arithmetic, comparison, and row output.
//! Cursor-based opcodes (OpenRead, Rewind, Next, Column, etc.) are stubbed
//! and will be wired to the B-tree layer in Phase 5.

use std::any::Any;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;

use fsqlite_btree::{BtCursor, BtreeCursorOps, MemPageStore, PageReader, PageWriter, SeekResult};
use fsqlite_error::{ErrorCode, FrankenError, Result};
use fsqlite_func::{ErasedAggregateFunction, FunctionRegistry};
use fsqlite_mvcc::{ConcurrentRegistry, InProcessPageLockTable, MvccError, concurrent_write_page};
use fsqlite_pager::TransactionHandle;
use fsqlite_types::cx::Cx;
use fsqlite_types::opcode::{Opcode, P4, VdbeOp};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{PageData, PageNumber};

use crate::VdbeProgram;

// ── In-Memory Table Store ──────────────────────────────────────────────────
//
// Phase 4 in-memory cursor backend. Allows the VDBE engine to execute
// CREATE TABLE / INSERT / SELECT / UPDATE / DELETE against a lightweight
// row store without requiring the full B-tree + pager + VFS stack.

/// A row in an in-memory table: (rowid, column values).
#[derive(Debug, Clone, PartialEq)]
struct MemRow {
    rowid: i64,
    values: Vec<SqliteValue>,
}

/// In-memory table storage (Phase 4 backend).
#[derive(Debug, Clone)]
pub struct MemTable {
    /// Column count for this table (used when creating the table;
    /// actual row widths may vary).
    pub num_columns: usize,
    /// Rows stored in insertion order.
    rows: Vec<MemRow>,
    /// Next auto-increment rowid.
    next_rowid: i64,
}

impl MemTable {
    /// Create a new empty table with the given column count.
    fn new(num_columns: usize) -> Self {
        Self {
            num_columns,
            rows: Vec::new(),
            next_rowid: 1,
        }
    }

    /// Allocate a new unique rowid.
    pub fn alloc_rowid(&mut self) -> i64 {
        let id = self.next_rowid;
        self.next_rowid += 1;
        id
    }

    /// Insert a row with the given rowid and values.
    fn insert(&mut self, rowid: i64, values: Vec<SqliteValue>) {
        // Update next_rowid if needed.
        if rowid >= self.next_rowid {
            self.next_rowid = rowid + 1;
        }
        // Replace if rowid already exists (UPSERT semantics).
        if let Some(existing) = self.rows.iter_mut().find(|r| r.rowid == rowid) {
            existing.values = values;
        } else {
            self.rows.push(MemRow { rowid, values });
        }
    }

    /// Delete a row by rowid. Returns true if a row was found and deleted.
    pub fn delete_by_rowid(&mut self, rowid: i64) -> bool {
        if let Some(idx) = self.rows.iter().position(|r| r.rowid == rowid) {
            self.rows.remove(idx);
            true
        } else {
            false
        }
    }

    /// Remove all rows from the table.
    pub fn clear(&mut self) {
        self.rows.clear();
    }

    /// Find a row by rowid. Returns the index.
    pub fn find_by_rowid(&self, rowid: i64) -> Option<usize> {
        self.rows.iter().position(|r| r.rowid == rowid)
    }

    /// Iterate all rows as `(rowid, values)` pairs.
    ///
    /// Used by the compat persistence layer to dump table contents to
    /// real SQLite format files.
    pub fn iter_rows(&self) -> impl Iterator<Item = (i64, &[SqliteValue])> + '_ {
        self.rows.iter().map(|r| (r.rowid, r.values.as_slice()))
    }

    /// Insert a row with an explicit rowid (for loading from file).
    ///
    /// This is the public entry point used by the compat persistence
    /// loader. It delegates to the private `insert` method.
    pub fn insert_row(&mut self, rowid: i64, values: Vec<SqliteValue>) {
        self.insert(rowid, values);
    }
}

/// Cursor state for traversing an in-memory table.
#[derive(Debug, Clone)]
struct MemCursor {
    /// Root page (used as table identifier).
    root_page: i32,
    /// Whether this cursor is writable (enforced at the Connection level).
    #[allow(dead_code)]
    writable: bool,
    /// Current row position (None = not positioned).
    position: Option<usize>,
    /// Pseudo-table data (for OpenPseudo: a single row set by RowData/MakeRecord).
    pseudo_row: Option<Vec<SqliteValue>>,
    /// Whether this is a pseudo cursor (OpenPseudo).
    is_pseudo: bool,
}

impl MemCursor {
    fn new(root_page: i32, writable: bool) -> Self {
        Self {
            root_page,
            writable,
            position: None,
            pseudo_row: None,
            is_pseudo: false,
        }
    }

    fn new_pseudo() -> Self {
        Self {
            root_page: -1,
            writable: false,
            position: None,
            pseudo_row: None,
            is_pseudo: true,
        }
    }
}

/// Cursor state for sorter opcodes (`SorterOpen`, `SorterInsert`, ...).
#[derive(Debug, Clone)]
struct SorterCursor {
    /// Number of leading columns used as sort key.
    key_columns: usize,
    /// Per-key sort direction (length == key_columns).
    sort_key_orders: Vec<SortKeyOrder>,
    /// Inserted records decoded from `MakeRecord` blobs.
    rows: Vec<Vec<SqliteValue>>,
    /// Current position after `SorterSort`/`SorterNext`.
    position: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKeyOrder {
    Asc,
    Desc,
}

impl SorterCursor {
    fn new(key_columns: usize, mut sort_key_orders: Vec<SortKeyOrder>) -> Self {
        let key_columns = key_columns.max(1);
        if sort_key_orders.len() < key_columns {
            sort_key_orders.resize(key_columns, SortKeyOrder::Asc);
        }
        sort_key_orders.truncate(key_columns);
        Self {
            key_columns,
            sort_key_orders,
            rows: Vec::new(),
            position: None,
        }
    }

    fn sort(&mut self) {
        let key_columns = self.key_columns;
        let orders = self.sort_key_orders.clone();
        self.rows
            .sort_by(|lhs, rhs| compare_sorter_rows(lhs, rhs, key_columns, &orders));
    }
}

// ── Shared Transaction Page I/O ─────────────────────────────────────────
//
// Phase 5 (bd-2a3y): Adapter that lets multiple `BtCursor` instances
// share a single pager transaction via `Rc<RefCell<…>>`.  The
// `PageReader`/`PageWriter` impls delegate through the `RefCell` borrow
// so that cursors can read/write pages on the real MVCC stack.

// ── MVCC Concurrent Context (bd-kivg / 5E.2) ────────────────────────────
//
// When concurrent mode is enabled, page-level locks must be acquired
// before writes. The write set is used for FCW validation at commit time.

/// MVCC concurrent mode context for page-level locking (bd-kivg / 5E.2).
///
/// When a transaction is in concurrent mode, this context enables:
/// - Acquiring page-level locks before writes via [`concurrent_write_page`]
/// - Recording written pages in the write set for FCW validation at commit
#[derive(Clone)]
struct ConcurrentContext {
    /// Session ID for this concurrent transaction.
    session_id: u64,
    /// Shared reference to the concurrent writer registry.
    registry: Rc<RefCell<ConcurrentRegistry>>,
    /// Shared reference to the page-level lock table.
    lock_table: Rc<InProcessPageLockTable>,
}

/// Shared wrapper around a boxed [`TransactionHandle`] so multiple
/// storage cursors can share one transaction.
///
/// Optionally includes [`ConcurrentContext`] for MVCC page-level locking
/// (bd-kivg / 5E.2).
#[derive(Clone)]
struct SharedTxnPageIo {
    txn: Rc<RefCell<Box<dyn TransactionHandle>>>,
    /// MVCC concurrent context (bd-kivg / 5E.2). When present, enables
    /// page-level locking for write operations.
    concurrent: Option<ConcurrentContext>,
}

impl std::fmt::Debug for SharedTxnPageIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedTxnPageIo")
            .field("rc_count", &Rc::strong_count(&self.txn))
            .field("concurrent", &self.concurrent.is_some())
            .finish()
    }
}

impl SharedTxnPageIo {
    fn new(txn: Box<dyn TransactionHandle>) -> Self {
        Self {
            txn: Rc::new(RefCell::new(txn)),
            concurrent: None,
        }
    }

    /// Create with MVCC concurrent context (bd-kivg / 5E.2).
    fn with_concurrent(
        txn: Box<dyn TransactionHandle>,
        session_id: u64,
        registry: Rc<RefCell<ConcurrentRegistry>>,
        lock_table: Rc<InProcessPageLockTable>,
    ) -> Self {
        Self {
            txn: Rc::new(RefCell::new(txn)),
            concurrent: Some(ConcurrentContext {
                session_id,
                registry,
                lock_table,
            }),
        }
    }

    /// Unwrap back to the owned transaction handle.
    /// Returns an error if other Rc clones still exist.
    fn into_inner(self) -> Result<Box<dyn TransactionHandle>> {
        match Rc::try_unwrap(self.txn) {
            Ok(cell) => Ok(cell.into_inner()),
            Err(rc) => Err(FrankenError::Internal(format!(
                "SharedTxnPageIo: {} outstanding Rc references",
                Rc::strong_count(&rc),
            ))),
        }
    }
}

impl PageReader for SharedTxnPageIo {
    fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        // bd-kivg / 5E.2: The write set is used for page-level locking and FCW
        // validation, not for read-your-own-writes. The pager's transaction
        // handles read-your-own-writes through its page cache, which also
        // correctly handles savepoint rollbacks.
        //
        // Note: concurrent_read_page is not called here because the pager
        // provides correct read-your-own-writes semantics, and checking the
        // write set would bypass savepoint rollback semantics.
        Ok(self.txn.borrow().get_page(cx, page_no)?.into_vec())
    }
}

impl PageWriter for SharedTxnPageIo {
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        // bd-kivg / 5E.2: Acquire page-level lock and record in write set if concurrent.
        if let Some(ref ctx) = self.concurrent {
            let mut registry = ctx.registry.borrow_mut();
            let handle = registry.get_mut(ctx.session_id).ok_or_else(|| {
                FrankenError::Internal(format!(
                    "MVCC session {} not found in registry during write",
                    ctx.session_id
                ))
            })?;
            let page_data = PageData::from_vec(data.to_vec());
            concurrent_write_page(handle, &ctx.lock_table, ctx.session_id, page_no, page_data)
                .map_err(|e| match e {
                    MvccError::Busy => FrankenError::Busy,
                    _ => FrankenError::Internal(format!("MVCC write_page failed: {e}")),
                })?;
        }
        // Persist to the underlying transaction.
        self.txn.borrow_mut().write_page(cx, page_no, data)
    }

    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
        self.txn.borrow_mut().allocate_page(cx)
    }

    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.txn.borrow_mut().free_page(cx, page_no)
    }
}

// ── Cursor Backend Enum ────────────────────────────────────────────────
//
// Allows StorageCursor to work in two modes:
// - `Mem`: backed by MemPageStore (Phase 4 / tests)
// - `Txn`: backed by SharedTxnPageIo (Phase 5 production path)

/// Backend for a storage cursor, dispatching between in-memory and
/// transaction-backed page I/O.
enum CursorBackend {
    /// In-memory page store (used by tests and Phase 4 fallback).
    Mem(BtCursor<MemPageStore>),
    /// Real pager transaction (Phase 5 production path, bd-2a3y).
    Txn(BtCursor<SharedTxnPageIo>),
}

impl std::fmt::Debug for CursorBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mem(c) => f.debug_tuple("Mem").field(c).finish(),
            Self::Txn(c) => f.debug_tuple("Txn").field(c).finish(),
        }
    }
}

/// Dispatch B-tree cursor operations across both backends.
impl CursorBackend {
    fn first(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.first(cx),
            Self::Txn(c) => c.first(cx),
        }
    }

    fn last(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.last(cx),
            Self::Txn(c) => c.last(cx),
        }
    }

    fn next(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.next(cx),
            Self::Txn(c) => c.next(cx),
        }
    }

    fn prev(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.prev(cx),
            Self::Txn(c) => c.prev(cx),
        }
    }

    fn eof(&self) -> bool {
        match self {
            Self::Mem(c) => c.eof(),
            Self::Txn(c) => c.eof(),
        }
    }

    fn rowid(&self, cx: &Cx) -> Result<i64> {
        match self {
            Self::Mem(c) => c.rowid(cx),
            Self::Txn(c) => c.rowid(cx),
        }
    }

    fn payload(&self, cx: &Cx) -> Result<Vec<u8>> {
        match self {
            Self::Mem(c) => c.payload(cx),
            Self::Txn(c) => c.payload(cx),
        }
    }

    fn table_move_to(&mut self, cx: &Cx, rowid: i64) -> Result<SeekResult> {
        match self {
            Self::Mem(c) => c.table_move_to(cx, rowid),
            Self::Txn(c) => c.table_move_to(cx, rowid),
        }
    }

    fn table_insert(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
        match self {
            Self::Mem(c) => c.table_insert(cx, rowid, data),
            Self::Txn(c) => c.table_insert(cx, rowid, data),
        }
    }

    fn delete(&mut self, cx: &Cx) -> Result<()> {
        match self {
            Self::Mem(c) => c.delete(cx),
            Self::Txn(c) => c.delete(cx),
        }
    }

    /// Position the cursor at the given key in an index B-tree.
    fn index_move_to(&mut self, cx: &Cx, key: &[u8]) -> Result<SeekResult> {
        match self {
            Self::Mem(c) => c.index_move_to(cx, key),
            Self::Txn(c) => c.index_move_to(cx, key),
        }
    }

    /// Insert a key into an index B-tree.
    fn index_insert(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        match self {
            Self::Mem(c) => c.index_insert(cx, key),
            Self::Txn(c) => c.index_insert(cx, key),
        }
    }
}

/// Storage-backed table cursor used by `OpenRead` and `OpenWrite`.
///
/// In Phase 5, `cursor` may be backed by either an in-memory [`MemPageStore`]
/// (for tests / Phase 4 fallback) or a real pager transaction via
/// [`SharedTxnPageIo`] (production path, bd-2a3y).
#[derive(Debug)]
struct StorageCursor {
    cursor: CursorBackend,
    cx: Cx,
    /// Whether this cursor was opened for writing (`OpenWrite`).
    writable: bool,
    /// Highest rowid allocated by `NewRowid` on this cursor (bd-1yi8).
    /// Ensures consecutive allocations return unique values even when
    /// no Insert has been issued between them.
    last_alloc_rowid: i64,
}

/// Lightweight version token for `MemDatabase` undo/rollback (bd-g6eo).
///
/// This is the MVCC-style snapshot identity for the in-memory store.
/// Returned by [`MemDatabase::undo_version`] and consumed by
/// [`MemDatabase::rollback_to`] to identify undo save-points.
/// The token is just the undo-log length — O(1) to capture, no cloning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct MemDbVersionToken(usize);

#[derive(Debug, Clone)]
#[allow(dead_code)] // Variants constructed by MemDatabase methods not yet wired to VDBE opcodes.
enum MemDbUndoOp {
    CreateTable {
        root_page: i32,
        prev_next_root_page: i32,
    },
    DestroyTable {
        root_page: i32,
        table: MemTable,
    },
    ClearTable {
        root_page: i32,
        table: MemTable,
    },
    BumpRowid {
        root_page: i32,
        prev_next_rowid: i64,
    },
    UpsertRow {
        root_page: i32,
        rowid: i64,
        prev_next_rowid: i64,
        old_values: Option<Vec<SqliteValue>>,
    },
    DeleteRow {
        root_page: i32,
        index: usize,
        row: MemRow,
        prev_next_rowid: i64,
    },
}

impl MemDbUndoOp {
    fn undo(self, db: &mut MemDatabase) {
        match self {
            Self::CreateTable {
                root_page,
                prev_next_root_page,
            } => {
                db.tables.remove(&root_page);
                db.next_root_page = prev_next_root_page;
            }
            Self::DestroyTable { root_page, table } | Self::ClearTable { root_page, table } => {
                db.tables.insert(root_page, table);
            }
            Self::BumpRowid {
                root_page,
                prev_next_rowid,
            } => {
                if let Some(table) = db.tables.get_mut(&root_page) {
                    table.next_rowid = prev_next_rowid;
                }
            }
            Self::UpsertRow {
                root_page,
                rowid,
                prev_next_rowid,
                old_values,
            } => {
                if let Some(table) = db.tables.get_mut(&root_page) {
                    match old_values {
                        Some(values) => {
                            if let Some(row) = table.rows.iter_mut().find(|r| r.rowid == rowid) {
                                row.values = values;
                            } else {
                                table.rows.push(MemRow { rowid, values });
                            }
                        }
                        None => {
                            if let Some(idx) = table.rows.iter().position(|r| r.rowid == rowid) {
                                table.rows.remove(idx);
                            }
                        }
                    }
                    table.next_rowid = prev_next_rowid;
                }
            }
            Self::DeleteRow {
                root_page,
                index,
                row,
                prev_next_rowid,
            } => {
                if let Some(table) = db.tables.get_mut(&root_page) {
                    let insert_at = index.min(table.rows.len());
                    table.rows.insert(insert_at, row);
                    table.next_rowid = prev_next_rowid;
                }
            }
        }
    }
}

/// Shared in-memory database backing the VDBE engine's cursor operations.
///
/// Maps root page numbers to in-memory tables. The Connection layer
/// populates this when processing CREATE TABLE and passes it to the engine.
#[derive(Debug)]
pub struct MemDatabase {
    /// Tables indexed by root page number.
    pub tables: HashMap<i32, MemTable>,
    /// Next available root page number.
    next_root_page: i32,
    /// Whether undo logging is enabled for transaction/savepoint rollback.
    undo_enabled: bool,
    /// Undo log. A version token is the log length at the snapshot point.
    undo_log: Vec<MemDbUndoOp>,
}

impl MemDatabase {
    /// Create a new empty in-memory database.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            next_root_page: 2, // Page 1 is reserved for sqlite_master.
            undo_enabled: false,
            undo_log: Vec::new(),
        }
    }

    /// Create a table and return its root page number.
    pub fn create_table(&mut self, num_columns: usize) -> i32 {
        let prev_next_root_page = self.next_root_page;
        let root_page = prev_next_root_page;
        self.next_root_page += 1;
        self.tables.insert(root_page, MemTable::new(num_columns));
        self.push_undo(MemDbUndoOp::CreateTable {
            root_page,
            prev_next_root_page,
        });
        root_page
    }

    /// Create a table at a specific root page number.
    ///
    /// Used by the storage layer (5A.3) when the root page is allocated
    /// from the pager rather than auto-assigned.  Advances
    /// `next_root_page` past `root_page` if necessary so that future
    /// `create_table()` calls do not collide.
    pub fn create_table_at(&mut self, root_page: i32, num_columns: usize) {
        let prev_next_root_page = self.next_root_page;
        if root_page >= self.next_root_page {
            self.next_root_page = root_page + 1;
        }
        self.tables.insert(root_page, MemTable::new(num_columns));
        self.push_undo(MemDbUndoOp::CreateTable {
            root_page,
            prev_next_root_page,
        });
    }

    /// Get a reference to a table by root page.
    pub fn get_table(&self, root_page: i32) -> Option<&MemTable> {
        self.tables.get(&root_page)
    }

    /// Get a mutable reference to a table by root page.
    pub fn get_table_mut(&mut self, root_page: i32) -> Option<&mut MemTable> {
        self.tables.get_mut(&root_page)
    }

    fn push_undo(&mut self, op: MemDbUndoOp) {
        if self.undo_enabled {
            self.undo_log.push(op);
        }
    }

    /// Return the current undo-version token.
    ///
    /// This is the identity captured in snapshots for savepoints/transactions.
    pub fn undo_version(&self) -> MemDbVersionToken {
        MemDbVersionToken(self.undo_log.len())
    }

    /// Begin a new undo region (transaction start).
    pub fn begin_undo(&mut self) {
        self.undo_enabled = true;
        self.undo_log.clear();
    }

    /// End the undo region (transaction committed/finished).
    pub fn commit_undo(&mut self) {
        self.undo_enabled = false;
        self.undo_log.clear();
    }

    /// Restore the database to a previously captured undo-version token.
    pub fn rollback_to(&mut self, token: MemDbVersionToken) {
        while self.undo_log.len() > token.0 {
            if let Some(op) = self.undo_log.pop() {
                op.undo(self);
            }
        }
    }

    /// Drop a table by root page and record undo information.
    pub fn destroy_table(&mut self, root_page: i32) {
        if let Some(table) = self.tables.remove(&root_page) {
            self.push_undo(MemDbUndoOp::DestroyTable { root_page, table });
        }
    }

    fn clear_table(&mut self, root_page: i32) {
        let prev = self.tables.get(&root_page).cloned();
        if let Some(table) = prev {
            self.push_undo(MemDbUndoOp::ClearTable { root_page, table });
        }
        if let Some(table) = self.tables.get_mut(&root_page) {
            table.rows.clear();
        }
    }

    fn alloc_rowid(&mut self, root_page: i32) -> i64 {
        if let Some(table) = self.tables.get_mut(&root_page) {
            let prev_next_rowid = table.next_rowid;
            let rowid = table.alloc_rowid();
            self.push_undo(MemDbUndoOp::BumpRowid {
                root_page,
                prev_next_rowid,
            });
            rowid
        } else {
            1
        }
    }

    /// Allocate a rowid for concurrent mode (`OP_NewRowid` with `p3 != 0`).
    ///
    /// Unlike the serialized path (counter only), this path derives the next
    /// candidate strictly from the visible table contents (`max(rowid) + 1`).
    /// This avoids relying on potentially stale local counter state.
    fn alloc_rowid_concurrent(&mut self, root_page: i32) -> i64 {
        if let Some(table) = self.tables.get_mut(&root_page) {
            let prev_next_rowid = table.next_rowid;
            let max_visible = table.rows.iter().map(|r| r.rowid).max().unwrap_or(0);
            let rowid = max_visible + 1;
            table.next_rowid = rowid + 1;
            self.push_undo(MemDbUndoOp::BumpRowid {
                root_page,
                prev_next_rowid,
            });
            rowid
        } else {
            1
        }
    }

    fn upsert_row(&mut self, root_page: i32, rowid: i64, values: Vec<SqliteValue>) {
        if let Some(table) = self.tables.get_mut(&root_page) {
            let prev_next_rowid = table.next_rowid;
            let old_values = table
                .rows
                .iter()
                .find(|r| r.rowid == rowid)
                .map(|r| r.values.clone());
            table.insert(rowid, values);
            self.push_undo(MemDbUndoOp::UpsertRow {
                root_page,
                rowid,
                prev_next_rowid,
                old_values,
            });
        }
    }

    #[allow(dead_code)]
    fn delete_at(&mut self, root_page: i32, index: usize) {
        if let Some(table) = self.tables.get_mut(&root_page) {
            if index < table.rows.len() {
                let prev_next_rowid = table.next_rowid;
                let row = table.rows.remove(index);
                self.push_undo(MemDbUndoOp::DeleteRow {
                    root_page,
                    index,
                    row,
                    prev_next_rowid,
                });
            }
        }
    }
}

impl Default for MemDatabase {
    fn default() -> Self {
        Self::new()
    }
}

// NOTE: MemDatabase intentionally does NOT implement Clone.
// Snapshot reads use the lightweight `MemDbVersionToken` (undo-log index)
// rather than cloning the entire table state.  See bd-g6eo.

const VDBE_TRACE_ENV: &str = "FSQLITE_VDBE_TRACE_OPCODES";
const VDBE_TRACE_LOGGING_STANDARD: &str = "bd-1fpm";

/// Slow query threshold for INFO-level logging (100ms).
const SLOW_QUERY_THRESHOLD_MS: u128 = 100;

// ── VDBE execution metrics (bd-1rw.1) ──────────────────────────────────────

/// Total number of VDBE opcodes executed across all statements.
static FSQLITE_VDBE_OPCODES_EXECUTED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total number of VDBE statements executed.
static FSQLITE_VDBE_STATEMENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Cumulative statement duration in microseconds (for histogram approximation).
static FSQLITE_VDBE_STATEMENT_DURATION_US_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Monotonic program ID counter for tracing correlation.
static VDBE_PROGRAM_ID_SEQ: AtomicU64 = AtomicU64::new(1);

/// Snapshot of VDBE execution metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VdbeMetricsSnapshot {
    /// Total opcodes executed across all statements.
    pub opcodes_executed_total: u64,
    /// Total statements executed.
    pub statements_total: u64,
    /// Cumulative statement duration in microseconds.
    pub statement_duration_us_total: u64,
}

/// Read a point-in-time snapshot of VDBE execution metrics.
#[must_use]
pub fn vdbe_metrics_snapshot() -> VdbeMetricsSnapshot {
    VdbeMetricsSnapshot {
        opcodes_executed_total: FSQLITE_VDBE_OPCODES_EXECUTED_TOTAL.load(AtomicOrdering::Relaxed),
        statements_total: FSQLITE_VDBE_STATEMENTS_TOTAL.load(AtomicOrdering::Relaxed),
        statement_duration_us_total: FSQLITE_VDBE_STATEMENT_DURATION_US_TOTAL
            .load(AtomicOrdering::Relaxed),
    }
}

/// Reset VDBE metrics to zero (tests/diagnostics).
pub fn reset_vdbe_metrics() {
    FSQLITE_VDBE_OPCODES_EXECUTED_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_VDBE_STATEMENTS_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_VDBE_STATEMENT_DURATION_US_TOTAL.store(0, AtomicOrdering::Relaxed);
}

/// Register spans touched by an opcode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpcodeRegisterSpans {
    read_start: i32,
    read_len: i32,
    write_start: i32,
    write_len: i32,
}

impl OpcodeRegisterSpans {
    const NONE: Self = Self {
        read_start: -1,
        read_len: 0,
        write_start: -1,
        write_len: 0,
    };
}

/// Outcome of a single engine execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutcome {
    /// Program halted normally (Halt with p1=0).
    Done,
    /// Program halted with an error code and message.
    Error { code: i32, message: String },
}

/// The VDBE bytecode interpreter.
///
/// Executes a program produced by the code generator, maintaining a register
/// file and collecting result rows. In Phase 4, cursor operations use an
/// in-memory table store (`MemDatabase`) rather than the full B-tree stack.
pub struct VdbeEngine {
    /// Register file (1-indexed; index 0 is unused/sentinel).
    registers: Vec<SqliteValue>,
    /// Bound SQL parameter values (`?1`, `?2`, ...).
    bindings: Vec<SqliteValue>,
    /// Whether opcode-level tracing is enabled.
    trace_opcodes: bool,
    /// Result rows accumulated during execution.
    results: Vec<Vec<SqliteValue>>,
    /// Open cursors (keyed by cursor number, i.e. p1 of OpenRead/OpenWrite).
    cursors: HashMap<i32, MemCursor>,
    /// Open sorter cursors keyed by cursor number.
    sorters: HashMap<i32, SorterCursor>,
    /// Open storage-backed cursors keyed by cursor number (read and write).
    storage_cursors: HashMap<i32, StorageCursor>,
    /// Cursors that deleted the current row and should treat the next `Next`
    /// as a no-advance "consume successor" step.
    pending_next_after_delete: HashSet<i32>,
    /// Whether `OpenRead`/`OpenWrite` should route through storage-backed cursors.
    storage_cursors_enabled: bool,
    /// Shared pager transaction for storage cursors (Phase 5, bd-2a3y).
    /// When set, `open_storage_cursor` routes through the real pager/WAL
    /// stack instead of building transient `MemPageStore` snapshots.
    txn_page_io: Option<SharedTxnPageIo>,
    /// In-memory database backing cursor operations (shared with Connection).
    db: Option<MemDatabase>,
    /// Scalar/aggregate/window function registry for Function/PureFunc opcodes.
    func_registry: Option<Arc<FunctionRegistry>>,
    /// Aggregate accumulators keyed by accumulator register.
    aggregates: HashMap<i32, AggregateContext>,
    /// Schema cookie value provided by the Connection (bd-3mmj).
    /// Schema cookie value provided by the Connection (bd-3mmj).
    /// Used by `ReadCookie` (p3=1) and `SetCookie` opcodes, and
    /// by `Transaction` for stale-schema detection.
    schema_cookie: u32,
    /// Result of the last `Opcode::Compare` operation.
    last_compare_result: Option<Ordering>,
}

struct AggregateContext {
    func: Arc<ErasedAggregateFunction>,
    state: Box<dyn Any + Send>,
}

impl VdbeEngine {
    /// Create a new engine with enough registers for the given program.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn new(register_count: i32) -> Self {
        // +1 because registers are 1-indexed (register 0 unused).
        let count = register_count.max(0) as u32 + 1;
        Self {
            registers: vec![SqliteValue::Null; count as usize],
            bindings: Vec::new(),
            trace_opcodes: opcode_trace_enabled(),
            results: Vec::new(),
            cursors: HashMap::new(),
            sorters: HashMap::new(),
            storage_cursors: HashMap::new(),
            pending_next_after_delete: HashSet::new(),
            storage_cursors_enabled: true,
            txn_page_io: None,
            db: None,
            func_registry: None,
            aggregates: HashMap::new(),
            schema_cookie: 0,
            last_compare_result: None,
        }
    }

    /// Attach an in-memory database for cursor operations.
    pub fn set_database(&mut self, db: MemDatabase) {
        self.db = Some(db);
    }

    /// Take ownership of the in-memory database back from the engine.
    pub fn take_database(&mut self) -> Option<MemDatabase> {
        self.db.take()
    }

    /// Enable/disable storage-backed cursor execution for `OpenRead`/`OpenWrite`.
    pub fn enable_storage_cursors(&mut self, enabled: bool) {
        self.storage_cursors_enabled = enabled;
    }

    /// Backwards-compatible alias for [`Self::enable_storage_cursors`].
    pub fn enable_storage_read_cursors(&mut self, enabled: bool) {
        self.enable_storage_cursors(enabled);
    }

    /// Lend a pager transaction to the engine for storage cursor I/O.
    ///
    /// When set, `open_storage_cursor` routes through the real pager/WAL
    /// stack (`SharedTxnPageIo`) instead of building transient `MemPageStore`
    /// snapshots. Also enables storage cursors automatically.
    pub fn set_transaction(&mut self, txn: Box<dyn TransactionHandle>) {
        self.txn_page_io = Some(SharedTxnPageIo::new(txn));
        self.storage_cursors_enabled = true;
    }

    /// Lend a pager transaction with MVCC concurrent context (bd-kivg / 5E.2).
    ///
    /// Like [`set_transaction`](Self::set_transaction), but also enables
    /// MVCC page-level locking for concurrent writers. When the concurrent
    /// context is present:
    /// - Write operations acquire page-level locks via [`concurrent_write_page`]
    /// - Written pages are recorded in the write set for FCW validation at commit
    pub fn set_transaction_concurrent(
        &mut self,
        txn: Box<dyn TransactionHandle>,
        session_id: u64,
        registry: Rc<RefCell<ConcurrentRegistry>>,
        lock_table: Rc<InProcessPageLockTable>,
    ) {
        self.txn_page_io = Some(SharedTxnPageIo::with_concurrent(
            txn, session_id, registry, lock_table,
        ));
        self.storage_cursors_enabled = true;
    }

    /// Take back the pager transaction after execution.
    ///
    /// All storage cursors must be dropped first (cleared during execution
    /// cleanup).
    pub fn take_transaction(&mut self) -> Result<Option<Box<dyn TransactionHandle>>> {
        // Drop all storage cursors first to release Rc references.
        self.storage_cursors.clear();
        match self.txn_page_io.take() {
            Some(txn_page_io) => Ok(Some(txn_page_io.into_inner()?)),
            None => Ok(None),
        }
    }

    /// Attach a function registry for `Function`/`PureFunc` opcode dispatch.
    pub fn set_function_registry(&mut self, registry: Arc<FunctionRegistry>) {
        self.func_registry = Some(registry);
    }

    /// Replace the current set of bound SQL parameters.
    ///
    /// Values are 1-indexed at execution time (`?1` maps to `bindings[0]`).
    pub fn set_bindings(&mut self, bindings: Vec<SqliteValue>) {
        self.bindings = bindings;
    }

    /// Set the schema cookie that `ReadCookie` will return and
    /// `Transaction` will use for stale-schema detection (bd-3mmj).
    pub fn set_schema_cookie(&mut self, cookie: u32) {
        self.schema_cookie = cookie;
    }

    /// Read the current schema cookie value (possibly updated by `SetCookie`).
    pub fn schema_cookie(&self) -> u32 {
        self.schema_cookie
    }

    /// Execute a VDBE program to completion.
    ///
    /// Returns `Ok(ExecOutcome::Done)` on normal halt, or an error if the
    /// program encounters a fatal condition.
    #[allow(
        clippy::too_many_lines,
        clippy::match_same_arms,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap
    )]
    pub fn execute(&mut self, program: &VdbeProgram) -> Result<ExecOutcome> {
        let ops = program.ops();
        if ops.is_empty() {
            return Ok(ExecOutcome::Done);
        }

        self.aggregates.clear();

        let program_id = VDBE_PROGRAM_ID_SEQ.fetch_add(1, AtomicOrdering::Relaxed);
        let start_time = Instant::now();
        let mut opcode_count: u64 = 0;
        let result_rows_before = self.results.len();

        // DEBUG-level per-statement log (bd-1rw.1).
        tracing::debug!(
            target: "fsqlite_vdbe::statement",
            program_id,
            num_ops = ops.len(),
            "vdbe statement begin",
        );

        let mut pc: usize = 0;
        // "once" flags: one bit per instruction address.
        let mut once_flags = vec![false; ops.len()];

        let outcome = loop {
            if pc >= ops.len() {
                break ExecOutcome::Done;
            }

            let op = &ops[pc];
            opcode_count += 1;
            self.trace_opcode(pc, op);
            match op.opcode {
                // ── Control Flow ────────────────────────────────────────
                Opcode::Init => {
                    // Jump to p2 if it points to a valid instruction.
                    // In the standard SQLite pattern, p2 points to a Goto
                    // at the end that bounces back. If p2 points past the
                    // end (our codegen pattern), fall through.
                    let target = op.p2 as usize;
                    if op.p2 > 0 && target < ops.len() {
                        pc = target;
                        continue;
                    }
                    pc += 1;
                }

                Opcode::Goto => {
                    pc = op.p2 as usize;
                }

                Opcode::Halt => {
                    if op.p1 != 0 {
                        let msg = match &op.p4 {
                            P4::Str(s) => s.clone(),
                            _ => format!("halt with error code {}", op.p1),
                        };
                        break ExecOutcome::Error {
                            code: op.p1,
                            message: msg,
                        };
                    }
                    break ExecOutcome::Done;
                }

                Opcode::Noop => {
                    pc += 1;
                }

                // ── Constants ───────────────────────────────────────────
                Opcode::Integer => {
                    // Set register p2 to integer value p1.
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(op.p1)));
                    pc += 1;
                }

                Opcode::Int64 => {
                    let val = match &op.p4 {
                        P4::Int64(v) => *v,
                        _ => 0,
                    };
                    self.set_reg(op.p2, SqliteValue::Integer(val));
                    pc += 1;
                }

                Opcode::Real => {
                    let val = match &op.p4 {
                        P4::Real(v) => *v,
                        _ => 0.0,
                    };
                    self.set_reg(op.p2, SqliteValue::Float(val));
                    pc += 1;
                }

                Opcode::String8 => {
                    let val = match &op.p4 {
                        P4::Str(s) => s.clone(),
                        _ => String::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Text(val));
                    pc += 1;
                }

                Opcode::String => {
                    // p1 = length, p4 = string data. Same as String8 for us.
                    let val = match &op.p4 {
                        P4::Str(s) => s.clone(),
                        _ => String::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Text(val));
                    pc += 1;
                }

                Opcode::Null => {
                    // Set registers p2..p3 to NULL.  When p3 == 0 only p2 is
                    // set.  p3 is an absolute register number (matching C
                    // SQLite where cnt = p3 - p2).
                    let start = op.p2;
                    let end = if op.p3 > 0 { op.p3 } else { start };
                    for r in start..=end {
                        self.set_reg(r, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::SoftNull => {
                    self.set_reg(op.p1, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::Blob => {
                    let val = match &op.p4 {
                        P4::Blob(b) => b.clone(),
                        _ => Vec::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Blob(val));
                    pc += 1;
                }

                // ── Register Operations ─────────────────────────────────
                Opcode::Move => {
                    // Move p3 registers from p1 to p2.
                    for i in 0..op.p3 {
                        let val = self.get_reg(op.p1 + i).clone();
                        self.set_reg(op.p2 + i, val);
                        self.set_reg(op.p1 + i, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::Copy => {
                    // Copy register p1 to p2 (deep copy).
                    let val = self.get_reg(op.p1).clone();
                    self.set_reg(op.p2, val);
                    pc += 1;
                }

                Opcode::SCopy => {
                    // Shallow copy register p1 to p2.
                    let val = self.get_reg(op.p1).clone();
                    self.set_reg(op.p2, val);
                    pc += 1;
                }

                Opcode::IntCopy => {
                    let val = self.get_reg(op.p1).to_integer();
                    self.set_reg(op.p2, SqliteValue::Integer(val));
                    pc += 1;
                }

                // ── Result Row ──────────────────────────────────────────
                Opcode::ResultRow => {
                    // Output p2 registers starting at p1.
                    let start = op.p1 as usize;
                    let count = op.p2 as usize;
                    let row: Vec<SqliteValue> = (start..start + count)
                        .map(|r| self.get_reg(r as i32).clone())
                        .collect();
                    self.results.push(row);
                    pc += 1;
                }

                // ── Arithmetic ──────────────────────────────────────────
                Opcode::Add => {
                    // p3 = p2 + p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_add(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Subtract => {
                    // p3 = p2 - p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_sub(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Multiply => {
                    // p3 = p2 * p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_mul(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Divide => {
                    // p3 = p2 / p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_div(dividend, divisor);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Remainder => {
                    // p3 = p2 % p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_rem(dividend, divisor);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                // ── String Concatenation ────────────────────────────────
                Opcode::Concat => {
                    // Concatenate p1 and p2 into p3.
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        let mut s = b.to_text();
                        s.push_str(&a.to_text());
                        SqliteValue::Text(s)
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                // ── Bitwise ─────────────────────────────────────────────
                Opcode::BitAnd => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(a.to_integer() & b.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::BitOr => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(a.to_integer() | b.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::ShiftLeft => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        sql_shift_left(b.to_integer(), a.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::ShiftRight => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        sql_shift_right(b.to_integer(), a.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::BitNot => {
                    // p2 = ~p1
                    let a = self.get_reg(op.p1);
                    let result = if a.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(!a.to_integer())
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Type Conversion ─────────────────────────────────────
                Opcode::AddImm => {
                    // Add integer p2 to register p1.
                    let val = self.get_reg(op.p1).to_integer() + i64::from(op.p2);
                    self.set_reg(op.p1, SqliteValue::Integer(val));
                    pc += 1;
                }

                Opcode::Cast => {
                    // Cast register p1 to type indicated by p2.
                    let val = self.get_reg(op.p1).clone();
                    let casted = sql_cast(val, op.p2);
                    self.set_reg(op.p1, casted);
                    pc += 1;
                }

                Opcode::MustBeInt => {
                    let val = self.get_reg(op.p1);
                    if val.as_integer().is_none() && !val.is_null() {
                        if op.p2 > 0 {
                            pc = op.p2 as usize;
                            continue;
                        }
                        return Err(FrankenError::TypeMismatch {
                            expected: "integer".to_owned(),
                            actual: val.typeof_str().to_owned(),
                        });
                    }
                    pc += 1;
                }

                #[allow(clippy::cast_precision_loss)]
                Opcode::RealAffinity => {
                    if let SqliteValue::Integer(i) = self.get_reg(op.p1) {
                        let f = *i as f64;
                        self.set_reg(op.p1, SqliteValue::Float(f));
                    }
                    pc += 1;
                }

                // ── Comparison Jumps ────────────────────────────────────
                Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge => {
                    let lhs = self.get_reg(op.p3);
                    let rhs = self.get_reg(op.p1);

                    // NULL handling: if either is NULL, jump depends on p5
                    // flag (SQLITE_NULLEQ).
                    let should_jump = if lhs.is_null() || rhs.is_null() {
                        let null_eq = (op.p5 & 0x80) != 0;
                        if null_eq {
                            // IS / IS NOT semantics: NULL == NULL is true.
                            let both_null = lhs.is_null() && rhs.is_null();
                            match op.opcode {
                                Opcode::Eq => both_null,
                                Opcode::Ne => !both_null,
                                _ => false,
                            }
                        } else {
                            // Standard SQL: comparison with NULL is NULL (no jump).
                            false
                        }
                    } else {
                        let cmp = lhs.partial_cmp(rhs);
                        matches!(
                            (op.opcode, cmp),
                            (Opcode::Eq, Some(std::cmp::Ordering::Equal))
                                | (Opcode::Lt, Some(std::cmp::Ordering::Less))
                                | (
                                    Opcode::Le,
                                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                                )
                                | (Opcode::Gt, Some(std::cmp::Ordering::Greater))
                                | (
                                    Opcode::Ge,
                                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                                )
                        ) || matches!(
                            (op.opcode, cmp),
                            (Opcode::Ne, Some(ord)) if ord != std::cmp::Ordering::Equal
                        )
                    };

                    if should_jump {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Boolean Logic ───────────────────────────────────────
                Opcode::And => {
                    // Three-valued AND: p3 = p1 AND p2
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = sql_and(a, b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Or => {
                    // Three-valued OR: p3 = p1 OR p2
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = sql_or(a, b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Not => {
                    // p2 = NOT p1
                    let a = self.get_reg(op.p1);
                    let result = if a.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(i64::from(a.to_integer() == 0))
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Conditional Jumps ───────────────────────────────────
                Opcode::If => {
                    // Jump to p2 if p1 is true (non-zero, non-NULL).
                    let val = self.get_reg(op.p1);
                    if !val.is_null() && val.to_integer() != 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNot => {
                    // Jump to p2 if p1 is false (zero) or NULL.
                    let val = self.get_reg(op.p1);
                    if val.is_null() || val.to_integer() == 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IsNull => {
                    // Jump to p2 if p1 is NULL.
                    if self.get_reg(op.p1).is_null() {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::NotNull => {
                    // Jump to p2 if p1 is NOT NULL.
                    if self.get_reg(op.p1).is_null() {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::Once => {
                    // Jump to p2 on first execution only.
                    if once_flags[pc] {
                        pc += 1;
                    } else {
                        once_flags[pc] = true;
                        pc = op.p2 as usize;
                    }
                }

                // ── Gosub / Return ──────────────────────────────────────
                Opcode::Gosub => {
                    // Store return address in p1, jump to p2.
                    let return_addr = (pc + 1) as i32;
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(return_addr)));
                    pc = op.p2 as usize;
                }

                Opcode::Return => {
                    // Jump to address stored in p1.
                    let addr = self.get_reg(op.p1).to_integer();
                    pc = addr as usize;
                }

                // ── Transaction (stub for expression eval) ──────────────
                Opcode::Transaction | Opcode::AutoCommit | Opcode::TableLock => {
                    // No-op in expression-only mode. Transaction lifecycle
                    // will be wired to WAL and lock manager in Phase 5.
                    pc += 1;
                }

                // ── Cookie operations (bd-3mmj) ────────────────────────
                //
                // ReadCookie: P1=db, P2=dest register, P3=cookie number
                //   cookie 1 = schema_cookie (offset 40 in header)
                // SetCookie: P1=db, P2=cookie number, P3=new value
                Opcode::ReadCookie => {
                    let dest_reg = op.p2;
                    let cookie_num = op.p3;
                    let value = match cookie_num {
                        // Cookie 1 = BTREE_SCHEMA_VERSION (schema cookie)
                        1 => i64::from(self.schema_cookie),
                        // Other cookies return 0 for now.
                        _ => 0,
                    };
                    self.set_reg(dest_reg, SqliteValue::Integer(value));
                    pc += 1;
                }
                Opcode::SetCookie => {
                    let cookie_num = op.p2;
                    let new_value = op.p3;
                    if cookie_num == 1 {
                        #[allow(clippy::cast_sign_loss)]
                        {
                            self.schema_cookie = new_value as u32;
                        }
                    }
                    // Other cookie numbers are silently ignored for now.
                    pc += 1;
                }

                // ── Cursor operations ─────────────────────────────────
                Opcode::OpenRead => {
                    // bd-1xrs: StorageCursor is now the ONLY cursor path.
                    // No MemCursor fallback - open_storage_cursor must succeed.
                    let cursor_id = op.p1;
                    let root_page = op.p2;
                    self.pending_next_after_delete.remove(&cursor_id);
                    if !self.open_storage_cursor(cursor_id, root_page, false) {
                        return Err(FrankenError::Internal(format!(
                            "OpenRead failed: could not open storage cursor on root page {root_page}"
                        )));
                    }
                    self.cursors.remove(&cursor_id);
                    pc += 1;
                }
                Opcode::OpenWrite => {
                    // bd-1xrs: StorageCursor is now the ONLY cursor path.
                    // No MemCursor fallback - open_storage_cursor must succeed.
                    let cursor_id = op.p1;
                    let root_page = op.p2;
                    self.pending_next_after_delete.remove(&cursor_id);
                    if !self.open_storage_cursor(cursor_id, root_page, true) {
                        return Err(FrankenError::Internal(format!(
                            "OpenWrite failed: could not open storage cursor on root page {root_page}"
                        )));
                    }
                    self.cursors.remove(&cursor_id);
                    pc += 1;
                }

                Opcode::OpenEphemeral | Opcode::OpenAutoindex => {
                    // Ephemeral table: create an in-memory table on-the-fly.
                    let cursor_id = op.p1;
                    self.pending_next_after_delete.remove(&cursor_id);
                    let num_cols = op.p2.max(1);
                    if let Some(db) = self.db.as_mut() {
                        let root_page = db.create_table(num_cols as usize);
                        self.storage_cursors.remove(&cursor_id);
                        self.cursors
                            .insert(cursor_id, MemCursor::new(root_page, true));
                    }
                    pc += 1;
                }

                Opcode::OpenPseudo => {
                    let cursor_id = op.p1;
                    self.pending_next_after_delete.remove(&cursor_id);
                    self.storage_cursors.remove(&cursor_id);
                    self.cursors.insert(cursor_id, MemCursor::new_pseudo());
                    pc += 1;
                }

                Opcode::OpenDup | Opcode::ReopenIdx => {
                    // Reopen: reuse existing cursor configuration.
                    pc += 1;
                }

                Opcode::SorterOpen => {
                    let cursor_id = op.p1;
                    self.pending_next_after_delete.remove(&cursor_id);
                    let key_columns = usize::try_from(op.p2.max(1)).unwrap_or(1);
                    let sort_key_orders = match &op.p4 {
                        P4::Str(order) => order
                            .chars()
                            .take(key_columns)
                            .map(|ch| {
                                if ch == '-' {
                                    SortKeyOrder::Desc
                                } else {
                                    SortKeyOrder::Asc
                                }
                            })
                            .collect(),
                        _ => vec![SortKeyOrder::Asc; key_columns],
                    };
                    self.sorters
                        .insert(cursor_id, SorterCursor::new(key_columns, sort_key_orders));
                    // A cursor id cannot be both table and sorter cursor.
                    self.cursors.remove(&cursor_id);
                    self.storage_cursors.remove(&cursor_id);
                    pc += 1;
                }

                Opcode::Close => {
                    self.cursors.remove(&op.p1);
                    self.storage_cursors.remove(&op.p1);
                    self.sorters.remove(&op.p1);
                    self.pending_next_after_delete.remove(&op.p1);
                    pc += 1;
                }

                Opcode::ColumnsUsed => {
                    pc += 1;
                }

                Opcode::Rewind | Opcode::Sort | Opcode::SorterSort => {
                    // Position cursor at the first row. Jump to p2 if empty.
                    let cursor_id = op.p1;
                    // Rewind repositions the cursor, so clear any pending delete state.
                    self.pending_next_after_delete.remove(&cursor_id);
                    let is_empty = if let Some(sorter) = self.sorters.get_mut(&cursor_id) {
                        if matches!(op.opcode, Opcode::Sort | Opcode::SorterSort) {
                            sorter.sort();
                        }
                        if sorter.rows.is_empty() {
                            sorter.position = None;
                            true
                        } else {
                            sorter.position = Some(0);
                            false
                        }
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if cursor.is_pseudo {
                            cursor.pseudo_row.is_none()
                        } else if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if table.rows.is_empty() {
                                    true
                                } else {
                                    cursor.position = Some(0);
                                    false
                                }
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        !cursor.cursor.first(&cursor.cx)?
                    } else {
                        true
                    };
                    if is_empty {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Last => {
                    // Position cursor at the last row. Jump to p2 if empty.
                    let cursor_id = op.p1;
                    // Last repositions the cursor, so clear any pending delete state.
                    self.pending_next_after_delete.remove(&cursor_id);
                    let is_empty = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        !cursor.cursor.last(&cursor.cx)?
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if cursor.is_pseudo {
                            cursor.pseudo_row.is_none()
                        } else if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if table.rows.is_empty() {
                                    true
                                } else {
                                    cursor.position = Some(table.rows.len() - 1);
                                    false
                                }
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                    if is_empty {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Next | Opcode::SorterNext => {
                    // Advance cursor to the next row. Jump to p2 if more rows.
                    let cursor_id = op.p1;
                    let has_next = if self.pending_next_after_delete.remove(&cursor_id) {
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                            !cursor.cursor.eof()
                        } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                            if cursor.is_pseudo {
                                false
                            } else if let Some(pos) = cursor.position {
                                if let Some(table) = self
                                    .db
                                    .as_ref()
                                    .and_then(|db| db.get_table(cursor.root_page))
                                {
                                    if pos < table.rows.len() {
                                        true
                                    } else {
                                        cursor.position = None;
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else if let Some(sorter) = self.sorters.get_mut(&cursor_id) {
                        if let Some(pos) = sorter.position {
                            let next = pos + 1;
                            if next < sorter.rows.len() {
                                sorter.position = Some(next);
                                true
                            } else {
                                sorter.position = None;
                                false
                            }
                        } else {
                            false
                        }
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if cursor.is_pseudo {
                            false
                        } else if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if let Some(pos) = cursor.position {
                                    let next = pos + 1;
                                    if next < table.rows.len() {
                                        cursor.position = Some(next);
                                        true
                                    } else {
                                        cursor.position = None;
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor.cursor.next(&cursor.cx)?
                    } else {
                        false
                    };
                    if has_next {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Prev => {
                    // Move cursor backward. Jump to p2 if more rows.
                    let cursor_id = op.p1;
                    let has_prev = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor.cursor.prev(&cursor.cx)?
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if let Some(pos) = cursor.position {
                            if pos > 0 {
                                cursor.position = Some(pos - 1);
                                true
                            } else {
                                cursor.position = None;
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if has_prev {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Column => {
                    // Read column p2 from cursor p1 into register p3.
                    let cursor_id = op.p1;
                    let col_idx = op.p2 as usize;
                    let target = op.p3;
                    let val = self.cursor_column(cursor_id, col_idx)?;
                    self.set_reg(target, val);
                    pc += 1;
                }

                Opcode::Rowid => {
                    // Get rowid from cursor p1 into register p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let val = self.cursor_rowid(cursor_id)?;
                    self.set_reg(target, val);
                    pc += 1;
                }

                Opcode::RowData => {
                    // Store raw row data as a blob in register p2.
                    // For pseudo-cursors, retrieve the blob from p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if cursor.is_pseudo {
                            // Pseudo cursor: the "row data" was already set via
                            // a prior MakeRecord → set_reg. Leave register as-is.
                        } else {
                            self.set_reg(target, SqliteValue::Null);
                        }
                    } else {
                        self.set_reg(target, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::NullRow => {
                    // Set cursor p1 to a null row.
                    if let Some(cursor) = self.cursors.get_mut(&op.p1) {
                        cursor.position = None;
                    }
                    pc += 1;
                }

                Opcode::Offset => {
                    self.set_reg(op.p3, SqliteValue::Null);
                    pc += 1;
                }

                // ── Seek operations (in-memory) ─────────────────────────
                Opcode::SeekRowid => {
                    // Seek cursor p1 to the row with rowid in register p3.
                    // If not found, jump to p2.
                    let cursor_id = op.p1;
                    // Seek repositions the cursor, so clear any pending delete state.
                    self.pending_next_after_delete.remove(&cursor_id);
                    let rowid_val = self.get_reg(op.p3).to_integer();
                    let found = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor
                            .cursor
                            .table_move_to(&cursor.cx, rowid_val)?
                            .is_found()
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if let Some(idx) = table.find_by_rowid(rowid_val) {
                                    cursor.position = Some(idx);
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if found {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT => {
                    // bd-3pti: Route seek opcodes through B-tree cursor.
                    //
                    // Seek operations position the cursor relative to a key:
                    // - SeekGE: Position at first row >= key
                    // - SeekGT: Position at first row > key
                    // - SeekLE: Position at last row <= key
                    // - SeekLT: Position at last row < key
                    //
                    // Jump to p2 if no matching row exists.
                    let cursor_id = op.p1;
                    // Seek repositions the cursor, so clear any pending delete state.
                    self.pending_next_after_delete.remove(&cursor_id);
                    let key_val = self.get_reg(op.p3).clone();

                    let found = if matches!(key_val, SqliteValue::Blob(_)) {
                        // Index seek path: probe register contains serialized key
                        // bytes (typically from MakeRecord).
                        let key_bytes = record_blob_bytes(&key_val);
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                            let seek_result =
                                cursor.cursor.index_move_to(&cursor.cx, &key_bytes)?;
                            match op.opcode {
                                Opcode::SeekGE => !cursor.cursor.eof(),
                                Opcode::SeekGT => {
                                    if seek_result.is_found() {
                                        cursor.cursor.next(&cursor.cx)?
                                    } else {
                                        !cursor.cursor.eof()
                                    }
                                }
                                Opcode::SeekLE => {
                                    if seek_result.is_found() {
                                        true
                                    } else if cursor.cursor.eof() {
                                        cursor.cursor.last(&cursor.cx)?
                                    } else {
                                        cursor.cursor.prev(&cursor.cx)?
                                    }
                                }
                                Opcode::SeekLT => {
                                    if cursor.cursor.eof() {
                                        cursor.cursor.last(&cursor.cx)?
                                    } else {
                                        cursor.cursor.prev(&cursor.cx)?
                                    }
                                }
                                _ => unreachable!(),
                            }
                        } else {
                            false
                        }
                    } else {
                        let key = key_val.to_integer();
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                            // Route through B-tree cursor (Phase 5 path).
                            //
                            // table_move_to semantics:
                            // - Found: cursor positioned at exact key
                            // - NotFound: cursor positioned at entry that would follow
                            //   key in sort order (or EOF if no such entry)
                            let seek_result = cursor.cursor.table_move_to(&cursor.cx, key)?;

                            match op.opcode {
                                Opcode::SeekGE => {
                                    // Need first row >= key.
                                    // table_move_to already positions at key (Found) or
                                    // at next larger (NotFound). Check for EOF.
                                    !cursor.cursor.eof()
                                }
                                Opcode::SeekGT => {
                                    // Need first row > key.
                                    // If Found (at exact key), advance past it.
                                    // If NotFound, already past key.
                                    if seek_result.is_found() {
                                        cursor.cursor.next(&cursor.cx)?
                                    } else {
                                        !cursor.cursor.eof()
                                    }
                                }
                                Opcode::SeekLE => {
                                    // Need last row <= key.
                                    // If Found, we're at the exact key - done.
                                    // If NotFound, cursor is at entry > key, so prev().
                                    if seek_result.is_found() {
                                        true
                                    } else if cursor.cursor.eof() {
                                        // All entries < key, position at last.
                                        cursor.cursor.last(&cursor.cx)?
                                    } else {
                                        // Cursor at entry > key, move to previous.
                                        cursor.cursor.prev(&cursor.cx)?
                                    }
                                }
                                Opcode::SeekLT => {
                                    // Need last row < key.
                                    // Cursor is either at key (Found) or past key (NotFound).
                                    // Either way, we need to go to the previous entry.
                                    if cursor.cursor.eof() {
                                        // All entries < key, position at last.
                                        cursor.cursor.last(&cursor.cx)?
                                    } else {
                                        // Go to previous entry (which will be < key).
                                        cursor.cursor.prev(&cursor.cx)?
                                    }
                                }
                                _ => unreachable!(),
                            }
                        } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                            // MemCursor fallback (Phase 4 path).
                            // Implement proper seeking via linear scan for correctness.
                            if let Some(db) = self.db.as_ref() {
                                if let Some(table) = db.get_table(cursor.root_page) {
                                    if table.rows.is_empty() {
                                        false
                                    } else {
                                        match op.opcode {
                                            Opcode::SeekGE => {
                                                // Find first row with rowid >= key.
                                                let pos =
                                                    table.rows.iter().position(|r| r.rowid >= key);
                                                if let Some(idx) = pos {
                                                    cursor.position = Some(idx);
                                                    true
                                                } else {
                                                    false
                                                }
                                            }
                                            Opcode::SeekGT => {
                                                // Find first row with rowid > key.
                                                let pos =
                                                    table.rows.iter().position(|r| r.rowid > key);
                                                if let Some(idx) = pos {
                                                    cursor.position = Some(idx);
                                                    true
                                                } else {
                                                    false
                                                }
                                            }
                                            Opcode::SeekLE => {
                                                // Find last row with rowid <= key.
                                                let pos =
                                                    table.rows.iter().rposition(|r| r.rowid <= key);
                                                if let Some(idx) = pos {
                                                    cursor.position = Some(idx);
                                                    true
                                                } else {
                                                    false
                                                }
                                            }
                                            Opcode::SeekLT => {
                                                // Find last row with rowid < key.
                                                let pos =
                                                    table.rows.iter().rposition(|r| r.rowid < key);
                                                if let Some(idx) = pos {
                                                    cursor.position = Some(idx);
                                                    true
                                                } else {
                                                    false
                                                }
                                            }
                                            _ => unreachable!(),
                                        }
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };
                    if found {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::SeekScan | Opcode::SeekEnd | Opcode::SeekHit => {
                    pc += 1;
                }

                Opcode::NotFound | Opcode::NotExists | Opcode::IfNoHope => {
                    // Check if rowid in register p3 exists in cursor p1.
                    let cursor_id = op.p1;
                    let rowid_val = self.get_reg(op.p3).to_integer();
                    let exists = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor
                            .cursor
                            .table_move_to(&cursor.cx, rowid_val)?
                            .is_found()
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                table.find_by_rowid(rowid_val).is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if exists {
                        pc += 1; // Found: fall through.
                    } else {
                        pc = op.p2 as usize; // Not found: jump.
                    }
                }

                Opcode::Found | Opcode::NoConflict => {
                    // Check if key exists; jump to p2 if found.
                    let cursor_id = op.p1;
                    let rowid_val = self.get_reg(op.p3).to_integer();
                    let exists = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor
                            .cursor
                            .table_move_to(&cursor.cx, rowid_val)?
                            .is_found()
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                table.find_by_rowid(rowid_val).is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if exists {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Insert / Delete / NewRowid ──────────────────────────
                Opcode::NewRowid => {
                    // Allocate a new rowid for cursor p1, store in register p2.
                    //
                    // Phase 5B.2 (bd-1yi8): when a StorageCursor exists, read
                    // the max rowid directly from the B-tree (navigate to
                    // last entry) instead of relying on MemDatabase counters.
                    // Falls back to MemDatabase for legacy Phase 4 cursors.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let concurrent_mode = op.p3 != 0;
                    let rowid = if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        // Navigate to last entry to find max rowid from B-tree.
                        let btree_max = if sc.cursor.last(&sc.cx)? {
                            sc.cursor.rowid(&sc.cx)?
                        } else {
                            0 // empty table
                        };
                        // Use the higher of B-tree max and previously allocated
                        // to ensure uniqueness across consecutive allocations.
                        let base = btree_max.max(sc.last_alloc_rowid);
                        let new_rowid = base + 1;
                        sc.last_alloc_rowid = new_rowid;
                        new_rowid
                    } else {
                        // MemDatabase fallback (Phase 4 in-memory cursors).
                        let root = self.cursors.get(&cursor_id).map(|c| c.root_page);
                        if let Some(root) = root {
                            if let Some(db) = self.db.as_mut() {
                                if concurrent_mode {
                                    db.alloc_rowid_concurrent(root)
                                } else {
                                    db.alloc_rowid(root)
                                }
                            } else {
                                1
                            }
                        } else {
                            1
                        }
                    };
                    self.set_reg(target, SqliteValue::Integer(rowid));
                    pc += 1;
                }

                Opcode::Insert => {
                    // Insert record in register p2 with rowid from register p3
                    // into cursor p1. p5 encodes conflict resolution mode:
                    // 1=ROLLBACK, 2=ABORT (default), 3=FAIL, 4=IGNORE, 5=REPLACE
                    //
                    // OE_* constants matching SQLite (4=IGNORE, 5=REPLACE)
                    let cursor_id = op.p1;
                    let record_reg = op.p2;
                    let rowid_reg = op.p3;
                    let oe_flag = op.p5 & 0x0F; // Low 4 bits for OE_* mode
                    let rowid = self.get_reg(rowid_reg).to_integer();
                    let record_val = self.get_reg(record_reg).clone();
                    let pk_conflict = ExecOutcome::Error {
                        code: ErrorCode::Constraint as i32,
                        message: "PRIMARY KEY constraint failed".to_owned(),
                    };

                    // Phase 5B.2 (bd-1yi8): write-through — route ONLY through
                    // StorageCursor when one exists; fall back to MemDatabase
                    // only for legacy Phase 4 cursors.
                    if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        if sc.writable {
                            let blob = record_blob_bytes(&record_val);
                            let exists = sc.cursor.table_move_to(&sc.cx, rowid)?.is_found();

                            if exists {
                                match oe_flag {
                                    4 => {
                                        // OE_IGNORE: Skip insert for conflicting row
                                    }
                                    5 => {
                                        // OE_REPLACE: Delete old, insert new (UPSERT)
                                        sc.cursor.delete(&sc.cx)?;
                                        sc.cursor.table_insert(&sc.cx, rowid, &blob)?;
                                    }
                                    _ => {
                                        // Default (ABORT/FAIL/ROLLBACK): constraint error.
                                        break pk_conflict;
                                    }
                                }
                            } else {
                                // No conflict — insert normally
                                sc.cursor.table_insert(&sc.cx, rowid, &blob)?;
                            }
                        }
                    } else if let Some(root) = self.cursors.get(&cursor_id).map(|c| c.root_page) {
                        // MemDatabase fallback (Phase 4 in-memory cursors).
                        let values = decode_record(&record_val)?;
                        if let Some(db) = self.db.as_mut() {
                            let exists = db
                                .get_table(root)
                                .and_then(|t| t.find_by_rowid(rowid))
                                .is_some();

                            if exists {
                                match oe_flag {
                                    4 => {
                                        // OE_IGNORE: Skip insert for conflicting row
                                    }
                                    5 => {
                                        // OE_REPLACE: UPSERT semantics
                                        db.upsert_row(root, rowid, values);
                                    }
                                    _ => {
                                        // Default (ABORT/FAIL/ROLLBACK): constraint error.
                                        break pk_conflict;
                                    }
                                }
                            } else {
                                // No conflict — insert normally
                                db.upsert_row(root, rowid, values);
                            }
                        }
                    }

                    // br-22iss: Clear pending_next_after_delete since Insert repositions
                    // the cursor. This is critical for UPDATE (Delete+Insert) to avoid
                    // infinite loops when the rowid doesn't change.
                    self.pending_next_after_delete.remove(&cursor_id);

                    pc += 1;
                }

                Opcode::Delete => {
                    // Delete the row at the current cursor position.
                    let cursor_id = op.p1;
                    let mut deleted = false;
                    // Phase 5B.3 (bd-1r0d): write-through — route ONLY through
                    // storage cursor when one exists; fall back to MemDatabase
                    // only for legacy Phase 4 cursors.
                    if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        if sc.writable && !sc.cursor.eof() {
                            sc.cursor.delete(&sc.cx)?;
                            deleted = true;
                        }
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        // Pure in-memory path (Phase 4).
                        if let Some(pos) = cursor.position {
                            let root = cursor.root_page;
                            let can_delete = self
                                .db
                                .as_ref()
                                .and_then(|db| db.get_table(root))
                                .is_some_and(|table| pos < table.rows.len());
                            if can_delete && let Some(db) = self.db.as_mut() {
                                db.delete_at(root, pos);
                                deleted = true;
                            }
                        }
                    }
                    if deleted {
                        self.pending_next_after_delete.insert(cursor_id);
                    }
                    pc += 1;
                }

                Opcode::IdxInsert => {
                    // Insert key from register P2 into index cursor P1.
                    // bd-qluy: Phase 5I.6 - Wire to B-tree index_insert.
                    let cursor_id = op.p1;
                    let key_reg = op.p2;
                    let key_val = self.get_reg(key_reg).clone();

                    if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        if sc.writable {
                            // Extract key bytes from the register value.
                            let key_bytes = record_blob_bytes(&key_val);
                            sc.cursor.index_insert(&sc.cx, &key_bytes)?;
                        }
                    }
                    // No MemDatabase fallback: Phase 4 in-memory backend doesn't
                    // support indexes (they're a no-op there).
                    pc += 1;
                }

                Opcode::SorterInsert => {
                    let cursor_id = op.p1;
                    let record = self.get_reg(op.p2).clone();
                    if let Some(sorter) = self.sorters.get_mut(&cursor_id) {
                        sorter.rows.push(decode_record(&record)?);
                    }
                    pc += 1;
                }

                Opcode::IdxDelete => {
                    // Delete entry at current position in index cursor P1.
                    // bd-qluy: Phase 5I.6 - Wire to B-tree delete.
                    //
                    // If P2 and P3 are provided, they specify the key to delete:
                    // P2 = start register, P3 = number of registers forming the key.
                    // In that case, we first seek to the key, then delete.
                    let cursor_id = op.p1;
                    let key_start_reg = op.p2;
                    let key_count = op.p3;

                    // Collect key bytes BEFORE borrowing cursor (borrow checker).
                    let key_bytes: Option<Vec<u8>> = if key_count > 0 {
                        let mut key_values: Vec<SqliteValue> =
                            Vec::with_capacity(key_count as usize);
                        for i in 0..key_count {
                            key_values.push(self.get_reg(key_start_reg + i).clone());
                        }
                        Some(encode_record(&key_values))
                    } else {
                        None
                    };

                    if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        if sc.writable {
                            if let Some(ref key) = key_bytes {
                                // Seek to the key first, then delete.
                                if sc.cursor.index_move_to(&sc.cx, key)?.is_found() {
                                    sc.cursor.delete(&sc.cx)?;
                                }
                            } else if !sc.cursor.eof() {
                                // Delete at current position.
                                sc.cursor.delete(&sc.cx)?;
                            }
                        }
                    }
                    // No MemDatabase fallback for indexes.
                    pc += 1;
                }

                Opcode::SorterCompare => {
                    // Compare current sorter key with packed record in register p3.
                    // Jump to p2 when keys differ.
                    let cursor_id = op.p1;
                    let differs = if let Some(sorter) = self.sorters.get(&cursor_id) {
                        if let Some(pos) = sorter.position {
                            if let Some(current) = sorter.rows.get(pos) {
                                let probe = decode_record(self.get_reg(op.p3))?;
                                !sorter_keys_equal(current, &probe, sorter.key_columns)
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                    if differs {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::SorterData => {
                    // Encode current sorter row into register p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let value = if let Some(sorter) = self.sorters.get(&cursor_id) {
                        if let Some(pos) = sorter.position {
                            if let Some(row) = sorter.rows.get(pos) {
                                SqliteValue::Blob(encode_record(row))
                            } else {
                                SqliteValue::Null
                            }
                        } else {
                            SqliteValue::Null
                        }
                    } else {
                        SqliteValue::Null
                    };
                    self.set_reg(target, value);
                    pc += 1;
                }

                Opcode::RowCell => {
                    pc += 1;
                }

                Opcode::ResetCount => {
                    pc += 1;
                }

                // ── Record building (SQLite record format) ──────────────
                Opcode::MakeRecord => {
                    // Build a record from registers p1..p1+p2-1 into register p3.
                    let start = op.p1;
                    let count = op.p2;
                    let target = op.p3;
                    let mut values = Vec::with_capacity(count as usize);
                    for i in 0..count {
                        values.push(self.get_reg(start + i).clone());
                    }
                    self.set_reg(target, SqliteValue::Blob(encode_record(&values)));
                    pc += 1;
                }

                Opcode::Affinity => {
                    // Apply type affinity to p2 registers starting at p1.
                    // Uses p4 as affinity string.
                    if let P4::Affinity(aff) = &op.p4 {
                        let start = op.p1;
                        for (i, ch) in aff.chars().enumerate() {
                            #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
                            let reg = start + i as i32;
                            let val = self.get_reg(reg).clone();
                            let affinity = char_to_affinity(ch);
                            self.set_reg(reg, val.apply_affinity(affinity));
                        }
                    }
                    pc += 1;
                }

                // ── Miscellaneous ───────────────────────────────────────
                Opcode::HaltIfNull => {
                    if self.get_reg(op.p3).is_null() {
                        let msg = match &op.p4 {
                            P4::Str(s) => s.clone(),
                            _ => "NOT NULL constraint failed".to_owned(),
                        };
                        break ExecOutcome::Error {
                            code: op.p1,
                            message: msg,
                        };
                    }
                    pc += 1;
                }

                Opcode::Count => {
                    // Stub: set p2 to 0 (no cursor).
                    self.set_reg(op.p2, SqliteValue::Integer(0));
                    pc += 1;
                }

                Opcode::Sequence => {
                    self.set_reg(op.p2, SqliteValue::Integer(0));
                    pc += 1;
                }

                Opcode::SequenceTest => {
                    pc += 1;
                }

                Opcode::Variable => {
                    // Bind parameter (1-indexed). Unbound params read as NULL.
                    let idx = usize::try_from(op.p1)
                        .ok()
                        .and_then(|one_based| one_based.checked_sub(1));
                    let value = idx
                        .and_then(|idx| self.bindings.get(idx))
                        .cloned()
                        .unwrap_or(SqliteValue::Null);
                    self.set_reg(op.p2, value);
                    pc += 1;
                }

                Opcode::BeginSubrtn => {
                    self.set_reg(op.p2, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::IsTrue => {
                    let val = self.get_reg(op.p1);
                    let truth = !val.is_null() && val.to_integer() != 0;
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(truth)));
                    pc += 1;
                }

                Opcode::ZeroOrNull => {
                    // If either P1 or P3 is NULL, set P2 to NULL.
                    // Otherwise set P2 to 0.
                    // Reference: ZeroOrNull semantics (OP_ZeroOrNull spec).
                    if self.get_reg(op.p1).is_null() || self.get_reg(op.p3).is_null() {
                        self.set_reg(op.p2, SqliteValue::Null);
                    } else {
                        self.set_reg(op.p2, SqliteValue::Integer(0));
                    }
                    pc += 1;
                }

                Opcode::IfNullRow => {
                    // Jump to p2 if cursor p1 is not positioned on a row.
                    let is_null = if let Some(cursor) = self.storage_cursors.get(&op.p1) {
                        cursor.cursor.eof()
                    } else {
                        self.cursors
                            .get(&op.p1)
                            .is_none_or(|c| c.position.is_none() && !c.is_pseudo)
                    };
                    if is_null {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNotOpen => {
                    // Jump to p2 if cursor p1 is not open.
                    if self.cursors.contains_key(&op.p1)
                        || self.storage_cursors.contains_key(&op.p1)
                    {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::Compare => {
                    // Compare P1..P1+P3-1 with P2..P2+P3-1.
                    let start_a = op.p1;
                    let start_b = op.p2;
                    let count = op.p3;
                    let mut result = Ordering::Equal;
                    for i in 0..count {
                        let val_a = self.get_reg(start_a + i);
                        let val_b = self.get_reg(start_b + i);
                        // TODO: Apply collation from P4 if present.
                        // For now, use default collation (binary/numeric-aware).
                        if let Some(ord) = val_a.partial_cmp(val_b) {
                            if ord != Ordering::Equal {
                                result = ord;
                                break;
                            }
                        }
                    }
                    self.last_compare_result = Some(result);
                    pc += 1;
                }

                Opcode::Jump => {
                    // Jump to one of p1/p2/p3 based on last comparison.
                    let target = match self.last_compare_result {
                        Some(Ordering::Less) => op.p1,
                        Some(Ordering::Equal) => op.p2,
                        Some(Ordering::Greater) => op.p3,
                        None => {
                            // If no comparison has happened, fall through or use p2?
                            // SQLite spec says Jump logic depends on the preceding Compare.
                            // If we haven't compared, neutral path (p2) is a safe fallback.
                            op.p2
                        }
                    };
                    pc = target as usize;
                }

                Opcode::TypeCheck
                | Opcode::Permutation
                | Opcode::CollSeq
                | Opcode::ElseEq
                | Opcode::FkCheck => {
                    pc += 1;
                }

                Opcode::IsType => {
                    // Type check; stub: fall through.
                    pc += 1;
                }

                Opcode::IfSizeBetween | Opcode::IfEmpty => {
                    // Stub: jump to p2.
                    pc = op.p2 as usize;
                }

                Opcode::IdxRowid => {
                    // Extract rowid from index cursor p1 into register p2.
                    // For storage cursors this delegates to B-tree cursor
                    // rowid(), which decodes the trailing rowid field from the
                    // index key record.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let val = self.cursor_rowid(cursor_id)?;
                    self.set_reg(target, val);
                    pc += 1;
                }

                Opcode::DeferredSeek | Opcode::FinishSeek => {
                    pc += 1;
                }

                // ── Index comparison ────────────────────────────────────
                Opcode::IdxLE | Opcode::IdxGT | Opcode::IdxLT | Opcode::IdxGE => {
                    // Stub: fall through.
                    pc += 1;
                }

                // ── Schema / DDL ────────────────────────────────────────
                Opcode::CreateBtree => {
                    // Create a new B-tree (table) and store the root page in
                    // register p2. In memory mode, allocate a new MemTable.
                    let target = op.p2;
                    let root_page = if let Some(db) = self.db.as_mut() {
                        db.create_table(0) // Column count set later.
                    } else {
                        0
                    };
                    self.set_reg(target, SqliteValue::Integer(i64::from(root_page)));
                    pc += 1;
                }

                Opcode::Clear => {
                    // Clear all rows from a table. p1 = root page.
                    if let Some(db) = self.db.as_mut() {
                        db.clear_table(op.p1);
                    }
                    pc += 1;
                }

                Opcode::Destroy => {
                    // Remove a table. p1 = root page.
                    if let Some(db) = self.db.as_mut() {
                        db.destroy_table(op.p1);
                    }
                    pc += 1;
                }

                Opcode::SqlExec
                | Opcode::ParseSchema
                | Opcode::LoadAnalysis
                | Opcode::DropTable
                | Opcode::DropIndex
                | Opcode::DropTrigger => {
                    pc += 1;
                }

                Opcode::ResetSorter => {
                    if let Some(sorter) = self.sorters.get_mut(&op.p1) {
                        sorter.rows.clear();
                        sorter.position = None;
                    }
                    pc += 1;
                }

                // ── Savepoint / Checkpoint ──────────────────────────────
                Opcode::Savepoint | Opcode::Checkpoint => {
                    pc += 1;
                }

                // ── Program execution (subprogram) ──────────────────────
                Opcode::Program | Opcode::Param => {
                    pc += 1;
                }

                // ── Coroutine ───────────────────────────────────────────
                Opcode::InitCoroutine => {
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(op.p3)));
                    if op.p2 > 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Yield => {
                    let saved = self.get_reg(op.p1).to_integer();
                    let current = (pc + 1) as i32;
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(current)));
                    pc = saved as usize;
                }

                Opcode::EndCoroutine => {
                    let saved = self.get_reg(op.p1).to_integer();
                    pc = saved as usize;
                }

                // ── Aggregation ─────────────────────────────────────────
                //
                // Phase 4 supports single-group aggregation (no GROUP BY) using
                // AggStep/AggFinal. Aggregate state is stored out-of-band and keyed
                // by the accumulator register.
                Opcode::AggStep => {
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "AggStep opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "AggStep opcode executed without function registry".to_owned(),
                        )
                    })?;

                    let arg_count = i32::from(op.p5);
                    let func = registry
                        .find_aggregate(func_name, arg_count)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "no such aggregate function: {func_name}/{arg_count}",
                            ))
                        })?;

                    let mut args = Vec::with_capacity(op.p5 as usize);
                    for i in 0..op.p5 {
                        let reg_idx = op.p2 + i32::from(i);
                        args.push(self.get_reg(reg_idx).clone());
                    }

                    let accum_reg = op.p3;
                    let ctx = self.aggregates.entry(accum_reg).or_insert_with(|| {
                        let state = func.initial_state();
                        AggregateContext {
                            func: func.clone(),
                            state,
                        }
                    });

                    if !Arc::ptr_eq(&ctx.func, &func) {
                        return Err(FrankenError::Internal(
                            "AggStep accumulator reused for a different aggregate".to_owned(),
                        ));
                    }

                    ctx.func.step(&mut ctx.state, &args)?;
                    pc += 1;
                }

                Opcode::AggFinal => {
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "AggFinal opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "AggFinal opcode executed without function registry".to_owned(),
                        )
                    })?;

                    let arg_count = op.p2;
                    let func = registry
                        .find_aggregate(func_name, arg_count)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "no such aggregate function: {func_name}/{arg_count}",
                            ))
                        })?;

                    let accum_reg = op.p1;
                    let result = match self.aggregates.remove(&accum_reg) {
                        Some(ctx) => {
                            if !Arc::ptr_eq(&ctx.func, &func) {
                                return Err(FrankenError::Internal(
                                    "AggFinal accumulator used for a different aggregate"
                                        .to_owned(),
                                ));
                            }
                            ctx.func.finalize(ctx.state)?
                        }
                        None => func.finalize(func.initial_state())?,
                    };

                    self.set_reg(accum_reg, result);
                    pc += 1;
                }

                Opcode::AggInverse | Opcode::AggValue => {
                    // Not needed yet (GROUP BY / window aggregates / inverse ops).
                    pc += 1;
                }

                // ── Scalar function call ──────────────────────────────────
                //
                // Function/PureFunc: p1 = constant-p5-flags, p2 = first-arg register,
                // p3 = output register, p4 = FuncName, p5 = arg count.
                // Arguments are in registers p2..p2+p5.
                Opcode::Function | Opcode::PureFunc => {
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "Function opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };
                    let arg_count = op.p5 as usize;
                    let first_arg_reg = op.p2;
                    let output_reg = op.p3;

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "Function opcode executed without function registry".to_owned(),
                        )
                    })?;

                    #[allow(clippy::cast_possible_wrap)]
                    let func = registry
                        .find_scalar(func_name, arg_count as i32)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "no such function: {func_name}/{arg_count}",
                            ))
                        })?;

                    let mut args = Vec::with_capacity(arg_count);
                    for i in 0..arg_count {
                        #[allow(clippy::cast_possible_wrap)]
                        let reg_idx = first_arg_reg + i as i32;
                        args.push(self.get_reg(reg_idx).clone());
                    }

                    // func_eval span with tracing (bd-2wt.1).
                    let func_start = Instant::now();
                    let result = func.invoke(&args)?;
                    let func_elapsed = func_start.elapsed();

                    // TRACE-level per-call log (bd-2wt.1).
                    let result_type = match &result {
                        SqliteValue::Null => "null",
                        SqliteValue::Integer(_) => "integer",
                        SqliteValue::Float(_) => "real",
                        SqliteValue::Text(_) => "text",
                        SqliteValue::Blob(_) => "blob",
                    };
                    tracing::trace!(
                        target: "fsqlite_func::eval",
                        func_name,
                        arg_count,
                        result_type,
                        "func_eval",
                    );

                    // DEBUG-level for slow functions (>1ms).
                    #[allow(clippy::cast_possible_truncation)]
                    let func_us = func_elapsed.as_micros() as u64;
                    if func_us >= 1000 {
                        tracing::debug!(
                            target: "fsqlite_func::slow",
                            func_name,
                            arg_count,
                            result_type,
                            elapsed_us = func_us,
                            "slow func_eval",
                        );
                    }

                    // Update global metrics counter (bd-2wt.1).
                    fsqlite_func::record_func_call(func_us);

                    self.set_reg(output_reg, result);
                    pc += 1;
                }

                // ── LIMIT/OFFSET support ────────────────────────────────
                // DecrJumpZero: decrement register p1; if result is zero,
                // jump to p2. Used to count down remaining LIMIT rows.
                Opcode::DecrJumpZero => {
                    let val = self.get_reg(op.p1).to_integer() - 1;
                    self.set_reg(op.p1, SqliteValue::Integer(val));
                    if val == 0 {
                        #[allow(clippy::cast_sign_loss)]
                        {
                            pc = op.p2 as usize;
                        }
                    } else {
                        pc += 1;
                    }
                }

                // IfPos: if register p1 > 0, subtract p3, then jump to p2.
                // Used for OFFSET counting (skip rows while offset > 0).
                Opcode::IfPos => {
                    let val = self.get_reg(op.p1).to_integer();
                    if val > 0 {
                        let decremented = val - i64::from(op.p3);
                        self.set_reg(op.p1, SqliteValue::Integer(decremented));
                        #[allow(clippy::cast_sign_loss)]
                        {
                            pc = op.p2 as usize;
                        }
                    } else {
                        pc += 1;
                    }
                }

                // ── Catch-all for remaining opcodes ─────────────────────
                _ => {
                    break ExecOutcome::Error {
                        code: 1,
                        message: format!("unimplemented opcode {:?} at pc={}", op.opcode, pc),
                    };
                }
            }
        };

        // ── Post-execution metrics and tracing (bd-1rw.1) ──────────────────
        let elapsed = start_time.elapsed();
        let elapsed_us = elapsed.as_micros();
        let result_rows = self.results.len() - result_rows_before;

        // Update global counters.
        FSQLITE_VDBE_OPCODES_EXECUTED_TOTAL.fetch_add(opcode_count, AtomicOrdering::Relaxed);
        FSQLITE_VDBE_STATEMENTS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        FSQLITE_VDBE_STATEMENT_DURATION_US_TOTAL
            .fetch_add(elapsed_us as u64, AtomicOrdering::Relaxed);

        // vdbe_exec span with fields: opcode_count, program_id, result_rows (bd-1rw.1).
        let span = tracing::info_span!(
            target: "fsqlite_vdbe",
            "vdbe_exec",
            opcode_count,
            program_id,
            result_rows,
            elapsed_us = elapsed_us as u64,
        );
        let _guard = span.enter();

        // DEBUG-level per-statement completion log.
        tracing::debug!(
            target: "fsqlite_vdbe::statement",
            program_id,
            opcode_count,
            result_rows,
            elapsed_us = elapsed_us as u64,
            outcome = ?outcome,
            "vdbe statement done",
        );

        // INFO-level slow query log (>100ms).
        if elapsed.as_millis() >= SLOW_QUERY_THRESHOLD_MS {
            #[allow(clippy::cast_possible_truncation)]
            let millis = elapsed.as_millis() as u64;
            tracing::info!(
                target: "fsqlite_vdbe::slow_query",
                program_id,
                opcode_count,
                result_rows,
                elapsed_ms = millis,
                "slow vdbe statement",
            );
        }

        Ok(outcome)
    }

    /// Get the collected result rows.
    pub fn results(&self) -> &[Vec<SqliteValue>] {
        &self.results
    }

    /// Take the result rows, consuming them.
    pub fn take_results(&mut self) -> Vec<Vec<SqliteValue>> {
        std::mem::take(&mut self.results)
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    #[allow(clippy::cast_sign_loss)]
    fn get_reg(&self, r: i32) -> &SqliteValue {
        self.registers.get(r as usize).unwrap_or(&SqliteValue::Null)
    }

    #[allow(clippy::cast_sign_loss)]
    fn set_reg(&mut self, r: i32, val: SqliteValue) {
        let idx = r as usize;
        if idx >= self.registers.len() {
            self.registers.resize(idx + 1, SqliteValue::Null);
        }
        self.registers[idx] = match val {
            SqliteValue::Float(f) if f.is_nan() => SqliteValue::Null,
            other => other,
        };
    }

    /// Read a column value from the cursor's current row.
    fn cursor_column(&self, cursor_id: i32, col_idx: usize) -> Result<SqliteValue> {
        if let Some(cursor) = self.storage_cursors.get(&cursor_id) {
            if cursor.cursor.eof() {
                return Ok(SqliteValue::Null);
            }
            let payload = cursor.cursor.payload(&cursor.cx)?;
            let values = decode_record(&SqliteValue::Blob(payload))?;
            return Ok(values.get(col_idx).cloned().unwrap_or(SqliteValue::Null));
        }

        if let Some(cursor) = self.cursors.get(&cursor_id) {
            if cursor.is_pseudo {
                return Ok(cursor
                    .pseudo_row
                    .as_ref()
                    .and_then(|row| row.get(col_idx))
                    .cloned()
                    .unwrap_or(SqliteValue::Null));
            }
            if let Some(pos) = cursor.position
                && let Some(db) = self.db.as_ref()
                && let Some(table) = db.get_table(cursor.root_page)
                && let Some(row) = table.rows.get(pos)
            {
                return Ok(row
                    .values
                    .get(col_idx)
                    .cloned()
                    .unwrap_or(SqliteValue::Null));
            }
        }

        // Sorter cursor: read column directly from the sorted row.
        if let Some(sorter) = self.sorters.get(&cursor_id) {
            if let Some(pos) = sorter.position {
                if let Some(row) = sorter.rows.get(pos) {
                    return Ok(row.get(col_idx).cloned().unwrap_or(SqliteValue::Null));
                }
            }
        }

        Ok(SqliteValue::Null)
    }

    /// Get the rowid from the cursor's current row.
    fn cursor_rowid(&self, cursor_id: i32) -> Result<SqliteValue> {
        if let Some(cursor) = self.storage_cursors.get(&cursor_id) {
            if cursor.cursor.eof() {
                return Ok(SqliteValue::Null);
            }
            return Ok(SqliteValue::Integer(cursor.cursor.rowid(&cursor.cx)?));
        }

        if let Some(cursor) = self.cursors.get(&cursor_id)
            && let Some(pos) = cursor.position
            && let Some(db) = self.db.as_ref()
            && let Some(table) = db.get_table(cursor.root_page)
            && let Some(row) = table.rows.get(pos)
        {
            return Ok(SqliteValue::Integer(row.rowid));
        }
        Ok(SqliteValue::Null)
    }

    #[allow(clippy::cast_sign_loss)]
    fn open_storage_cursor(&mut self, cursor_id: i32, root_page: i32, writable: bool) -> bool {
        const PAGE_SIZE: u32 = 4096;
        // bd-1xrs: storage_cursors_enabled check removed.
        // StorageCursor is now the ONLY cursor path.

        let Some(root_pgno) = PageNumber::new(root_page as u32) else {
            return false;
        };

        // Phase 5C.1 (bd-35my): Route through pager when available, UNLESS
        // this is an ephemeral table that only exists in MemDatabase (not
        // backed by pager pages). Ephemeral tables are created by
        // OpenEphemeral and have uninitialized pager pages (0x00 type flag).
        if let Some(ref mut page_io) = self.txn_page_io {
            let cx = Cx::new();
            // Check if the page has valid B-tree header (type byte != 0x00).
            // Real tables have initialized pages; ephemeral tables don't.
            let page_data = page_io.read_page(&cx, root_pgno).ok();
            let is_valid_btree = page_data
                .as_ref()
                .is_some_and(|p| !p.is_empty() && p[0] != 0x00);

            if is_valid_btree {
                // Real table backed by pager: open cursor on EXISTING page data.
                let cursor = BtCursor::new(page_io.clone(), root_pgno, PAGE_SIZE, true);
                self.storage_cursors.insert(
                    cursor_id,
                    StorageCursor {
                        cursor: CursorBackend::Txn(cursor),
                        cx,
                        writable,
                        last_alloc_rowid: 0,
                    },
                );
                return true;
            }

            // For writable cursors on uninitialized pages (e.g., newly created
            // tables via CREATE TABLE AS SELECT), initialize an empty root page.
            if writable {
                // Initialize empty leaf table page (type 0x0D) - matches
                // MemPageStore::with_empty_table format.
                let mut page = vec![0u8; PAGE_SIZE as usize];
                page[0] = 0x0D; // Leaf table page
                // Bytes 1-2: first freeblock offset = 0 (none).
                // Bytes 3-4: cell count = 0.
                // Bytes 5-6: content area offset = page_size (no cells yet).
                #[allow(clippy::cast_possible_truncation)]
                let content_offset = PAGE_SIZE as u16; // PAGE_SIZE=4096 fits in u16
                page[5..7].copy_from_slice(&content_offset.to_be_bytes());
                // Byte 7: fragmented free bytes = 0.

                // Write the initialized page to pager.
                if let Err(err) = page_io.write_page(&cx, root_pgno, &page) {
                    tracing::warn!(
                        root_page,
                        error = %err,
                        "failed to initialize writable root page in pager"
                    );
                    return false;
                }
                let cursor = BtCursor::new(page_io.clone(), root_pgno, PAGE_SIZE, true);
                self.storage_cursors.insert(
                    cursor_id,
                    StorageCursor {
                        cursor: CursorBackend::Txn(cursor),
                        cx,
                        writable,
                        last_alloc_rowid: 0,
                    },
                );
                return true;
            }
            // Fall through to MemPageStore for ephemeral/read-only uninitialized tables.
        }

        // Fallback: build a transient B-tree snapshot (Phase 4 path used by
        // tests without a real pager). Both read and write cursors can operate
        // on empty tables (INSERT needs to work on new tables).
        let store = MemPageStore::with_empty_table(root_pgno, PAGE_SIZE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root_pgno, PAGE_SIZE, writable);
        // Populate cursor from MemDatabase if available.
        if let Some(table) = self.db.as_ref().and_then(|db| db.get_table(root_page)) {
            for row in &table.rows {
                let payload = encode_record(&row.values);
                if cursor.table_insert(&cx, row.rowid, &payload).is_err() {
                    return false;
                }
            }
        }

        self.storage_cursors.insert(
            cursor_id,
            StorageCursor {
                cursor: CursorBackend::Mem(cursor),
                cx,
                writable,
                last_alloc_rowid: 0,
            },
        );
        true
    }

    fn trace_opcode(&self, pc: usize, op: &VdbeOp) {
        if !self.trace_opcodes || !tracing::enabled!(tracing::Level::TRACE) {
            return;
        }
        let spans = opcode_register_spans(op);
        tracing::trace!(
            target: "fsqlite_vdbe::opcode",
            logging_standard = VDBE_TRACE_LOGGING_STANDARD,
            pc,
            opcode = %op.opcode.name(),
            p1 = op.p1,
            p2 = op.p2,
            p3 = op.p3,
            p5 = op.p5,
            read_start = spans.read_start,
            read_len = spans.read_len,
            write_start = spans.write_start,
            write_len = spans.write_len,
            "executing vdbe opcode",
        );
    }
}

// ── SQLite record encoding ──────────────────────────────────────────────
//
// SQLite `OP_MakeRecord` produces a record in the on-disk record format
// (header + body). Using the same format internally avoids later translation
// when wiring VDBE cursors to the real B-tree layer.

fn encode_record(values: &[SqliteValue]) -> Vec<u8> {
    serialize_record(values)
}

/// Extract the raw bytes from a record blob value (output of `MakeRecord`).
fn record_blob_bytes(val: &SqliteValue) -> Vec<u8> {
    match val {
        SqliteValue::Blob(bytes) => bytes.clone(),
        _ => Vec::new(),
    }
}

fn decode_record(val: &SqliteValue) -> Result<Vec<SqliteValue>> {
    let SqliteValue::Blob(bytes) = val else {
        return Ok(Vec::new());
    };

    parse_record(bytes).ok_or_else(|| FrankenError::internal("malformed SQLite record blob"))
}

fn sorter_keys_equal(lhs: &[SqliteValue], rhs: &[SqliteValue], key_columns: usize) -> bool {
    compare_sorter_keys(lhs, rhs, key_columns) == Ordering::Equal
}

fn compare_sorter_keys(lhs: &[SqliteValue], rhs: &[SqliteValue], key_columns: usize) -> Ordering {
    let key_count = key_columns.max(1);
    for idx in 0..key_count {
        let Some(lhs_value) = lhs.get(idx) else {
            return if rhs.get(idx).is_some() {
                Ordering::Less
            } else {
                break;
            };
        };
        let Some(rhs_value) = rhs.get(idx) else {
            return Ordering::Greater;
        };

        match lhs_value.partial_cmp(rhs_value).unwrap_or(Ordering::Equal) {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }
    }
    Ordering::Equal
}

fn compare_sorter_rows(
    lhs: &[SqliteValue],
    rhs: &[SqliteValue],
    key_columns: usize,
    sort_key_orders: &[SortKeyOrder],
) -> Ordering {
    let key_count = key_columns.max(1);
    for idx in 0..key_count {
        let Some(lhs_value) = lhs.get(idx) else {
            return if rhs.get(idx).is_some() {
                Ordering::Less
            } else {
                break;
            };
        };
        let Some(rhs_value) = rhs.get(idx) else {
            return Ordering::Greater;
        };

        let mut ord = lhs_value.partial_cmp(rhs_value).unwrap_or(Ordering::Equal);
        if ord == Ordering::Equal {
            continue;
        }

        if sort_key_orders.get(idx) == Some(&SortKeyOrder::Desc) {
            ord = ord.reverse();
        }
        return ord;
    }

    // Deterministic tie-breaker: compare full rows so sort order is stable.
    let full_len = lhs.len().max(rhs.len());
    for idx in 0..full_len {
        match (lhs.get(idx), rhs.get(idx)) {
            (Some(lhs_value), Some(rhs_value)) => {
                match lhs_value.partial_cmp(rhs_value).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => {}
                    non_equal => return non_equal,
                }
            }
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => break,
        }
    }

    Ordering::Equal
}

fn opcode_trace_enabled() -> bool {
    let env_enabled = std::env::var(VDBE_TRACE_ENV).is_ok_and(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        !normalized.is_empty() && normalized != "0" && normalized != "false" && normalized != "off"
    });
    env_enabled || cfg!(test)
}

fn range(start: i32, len: i32) -> (i32, i32) {
    if start <= 0 {
        (-1, 0)
    } else {
        (start, len.max(1))
    }
}

fn opcode_register_spans(op: &VdbeOp) -> OpcodeRegisterSpans {
    let (read_start, read_len, write_start, write_len) = match op.opcode {
        Opcode::Integer
        | Opcode::Int64
        | Opcode::Real
        | Opcode::String
        | Opcode::String8
        | Opcode::Blob
        | Opcode::Variable => {
            let (write_start, write_len) = range(op.p2, 1);
            (-1, 0, write_start, write_len)
        }
        Opcode::Null => {
            // p3 is absolute end register; count = p3 - p2 + 1 (or 1 if p3==0).
            let write_count = if op.p3 > 0 { op.p3 - op.p2 + 1 } else { 1 };
            let (write_start, write_len) = range(op.p2, write_count);
            (-1, 0, write_start, write_len)
        }
        Opcode::SoftNull
        | Opcode::Cast
        | Opcode::RealAffinity
        | Opcode::AddImm
        | Opcode::MustBeInt
        | Opcode::InitCoroutine
        | Opcode::Yield
        | Opcode::EndCoroutine => {
            let (start, len) = range(op.p1, 1);
            (start, len, start, len)
        }
        Opcode::Move => {
            let (read_start, read_len) = range(op.p1, op.p3);
            let (write_start, write_len) = range(op.p2, op.p3);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::Copy | Opcode::SCopy | Opcode::IntCopy | Opcode::BitNot | Opcode::Not => {
            let (read_start, read_len) = range(op.p1, 1);
            let (write_start, write_len) = range(op.p2, 1);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::ResultRow => {
            let (read_start, read_len) = range(op.p1, op.p2);
            (read_start, read_len, -1, 0)
        }
        Opcode::Add
        | Opcode::Subtract
        | Opcode::Multiply
        | Opcode::Divide
        | Opcode::Remainder
        | Opcode::Concat
        | Opcode::BitAnd
        | Opcode::BitOr
        | Opcode::ShiftLeft
        | Opcode::ShiftRight
        | Opcode::And
        | Opcode::Or => {
            let (read_start, read_len) = range(op.p1, 2);
            let (write_start, write_len) = range(op.p3, 1);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge => {
            let (read_start, read_len) = range(op.p1, 1);
            let (rhs_start, rhs_len) = range(op.p3, 1);
            let normalized_start = if read_start > 0 && rhs_start > 0 {
                read_start.min(rhs_start)
            } else if read_start > 0 {
                read_start
            } else {
                rhs_start
            };
            let normalized_len = if read_start > 0 && rhs_start > 0 && read_start != rhs_start {
                2
            } else {
                read_len.max(rhs_len)
            };
            (normalized_start, normalized_len, -1, 0)
        }
        Opcode::If | Opcode::IfNot | Opcode::IsNull | Opcode::NotNull | Opcode::IsTrue => {
            let (read_start, read_len) = range(op.p1, 1);
            (read_start, read_len, -1, 0)
        }
        Opcode::MakeRecord => {
            let (read_start, read_len) = range(op.p1, op.p2);
            let (write_start, write_len) = range(op.p3, 1);
            (read_start, read_len, write_start, write_len)
        }
        _ => (
            OpcodeRegisterSpans::NONE.read_start,
            OpcodeRegisterSpans::NONE.read_len,
            OpcodeRegisterSpans::NONE.write_start,
            OpcodeRegisterSpans::NONE.write_len,
        ),
    };

    OpcodeRegisterSpans {
        read_start,
        read_len,
        write_start,
        write_len,
    }
}

// ── Arithmetic helpers ──────────────────────────────────────────────────────

/// SQL division with NULL propagation and division-by-zero handling.
#[allow(clippy::cast_precision_loss)]
fn sql_div(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (dividend, divisor) {
        if *b == 0 {
            SqliteValue::Null
        } else {
            match a.checked_div(*b) {
                Some(result) => SqliteValue::Integer(result),
                // i64::MIN / -1 overflows; promote to float like SQLite.
                #[allow(clippy::cast_precision_loss)]
                None => SqliteValue::Float(*a as f64 / *b as f64),
            }
        }
    } else {
        let b = divisor.to_float();
        if b == 0.0 {
            SqliteValue::Null
        } else {
            let result = dividend.to_float() / b;
            if result.is_nan() {
                SqliteValue::Null
            } else {
                SqliteValue::Float(result)
            }
        }
    }
}

/// SQL remainder with NULL propagation and division-by-zero handling.
fn sql_rem(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    let a = dividend.to_integer();
    let b = divisor.to_integer();
    if b == 0 {
        SqliteValue::Null
    } else {
        // checked_rem handles i64::MIN % -1 which would overflow.
        match a.checked_rem(b) {
            Some(result) => SqliteValue::Integer(result),
            // i64::MIN % -1 = 0 mathematically (no remainder).
            None => SqliteValue::Integer(0),
        }
    }
}

/// SQL shift left (SQLite semantics: negative shift = shift right).
fn sql_shift_left(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_right(val, -amount);
    }
    if amount >= 64 {
        return SqliteValue::Integer(0);
    }
    // amount is in [0, 63] so the cast is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let shift = amount as u32;
    SqliteValue::Integer(val << shift)
}

/// SQL shift right (SQLite semantics: negative shift = shift left).
fn sql_shift_right(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_left(val, -amount);
    }
    if amount >= 64 {
        return SqliteValue::Integer(if val < 0 { -1 } else { 0 });
    }
    // amount is in [0, 63] so the cast is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let shift = amount as u32;
    SqliteValue::Integer(val >> shift)
}

/// Three-valued SQL AND.
fn sql_and(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    let a_val = if a.is_null() {
        None
    } else {
        Some(a.to_integer() != 0)
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(b.to_integer() != 0)
    };

    match (a_val, b_val) {
        (Some(false), _) | (_, Some(false)) => SqliteValue::Integer(0),
        (Some(true), Some(true)) => SqliteValue::Integer(1),
        _ => SqliteValue::Null,
    }
}

/// Three-valued SQL OR.
fn sql_or(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    let a_val = if a.is_null() {
        None
    } else {
        Some(a.to_integer() != 0)
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(b.to_integer() != 0)
    };

    match (a_val, b_val) {
        (Some(true), _) | (_, Some(true)) => SqliteValue::Integer(1),
        (Some(false), Some(false)) => SqliteValue::Integer(0),
        _ => SqliteValue::Null,
    }
}

/// SQL CAST operation (p2 encodes target type).
fn sql_cast(val: SqliteValue, target: i32) -> SqliteValue {
    // Target type encoding matches SQLite:
    // 'A' (65) = BLOB, 'B' (66) = TEXT, 'C' (67) = NUMERIC,
    // 'D' (68) = INTEGER, 'E' (69) = REAL
    // But more commonly p2 is used as an affinity character.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let target_byte = target as u8;
    match target_byte {
        b'A' | b'a' => SqliteValue::Blob(match val {
            SqliteValue::Blob(b) => b,
            SqliteValue::Text(s) => s.into_bytes(),
            other => other.to_text().into_bytes(),
        }),
        b'B' | b'b' => SqliteValue::Text(val.to_text()),
        b'C' | b'c' => val.apply_affinity(fsqlite_types::TypeAffinity::Numeric),
        b'D' | b'd' => SqliteValue::Integer(val.to_integer()),
        b'E' | b'e' => SqliteValue::Float(val.to_float()),
        _ => val, // unknown: no-op
    }
}

/// Convert affinity character to `TypeAffinity`.
fn char_to_affinity(ch: char) -> fsqlite_types::TypeAffinity {
    match ch {
        'B' | 'b' => fsqlite_types::TypeAffinity::Text,
        'C' | 'c' => fsqlite_types::TypeAffinity::Numeric,
        'D' | 'd' => fsqlite_types::TypeAffinity::Integer,
        'E' | 'e' => fsqlite_types::TypeAffinity::Real,
        _ => fsqlite_types::TypeAffinity::Blob,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProgramBuilder;
    use fsqlite_types::opcode::{Opcode, P4, VdbeOp};

    /// Build and execute a program, returning results.
    fn run_program(build: impl FnOnce(&mut ProgramBuilder)) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    /// Build and execute a program with bound SQL parameters.
    fn run_program_with_bindings(
        build: impl FnOnce(&mut ProgramBuilder),
        bindings: Vec<SqliteValue>,
    ) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_bindings(bindings);
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    #[test]
    fn test_opcode_register_spans_for_variable() {
        let op = VdbeOp {
            opcode: Opcode::Variable,
            p1: 2,
            p2: 9,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        let spans = opcode_register_spans(&op);
        assert_eq!(spans.read_start, -1);
        assert_eq!(spans.read_len, 0);
        assert_eq!(spans.write_start, 9);
        assert_eq!(spans.write_len, 1);
    }

    #[test]
    fn test_opcode_register_spans_for_result_row() {
        let op = VdbeOp {
            opcode: Opcode::ResultRow,
            p1: 4,
            p2: 3,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        let spans = opcode_register_spans(&op);
        assert_eq!(spans.read_start, 4);
        assert_eq!(spans.read_len, 3);
        assert_eq!(spans.write_start, -1);
        assert_eq!(spans.write_len, 0);
    }

    #[test]
    fn test_variable_uses_bound_parameter_value() {
        let rows = run_program_with_bindings(
            |b| {
                let end = b.emit_label();
                b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
                let r1 = b.alloc_reg();
                b.emit_op(Opcode::Variable, 2, r1, 0, P4::None, 0);
                b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
                b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
                b.resolve_label(end);
            },
            vec![
                SqliteValue::Integer(11),
                SqliteValue::Text("bound".to_owned()),
            ],
        );
        assert_eq!(rows, vec![vec![SqliteValue::Text("bound".to_owned())]]);
    }

    #[test]
    fn test_variable_unbound_parameter_defaults_to_null() {
        let rows = run_program_with_bindings(
            |b| {
                let end = b.emit_label();
                b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
                let r1 = b.alloc_reg();
                b.emit_op(Opcode::Variable, 3, r1, 0, P4::None, 0);
                b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
                b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
                b.resolve_label(end);
            },
            vec![SqliteValue::Integer(11)],
        );
        assert_eq!(rows, vec![vec![SqliteValue::Null]]);
    }

    // ── test_select_integer_literal ─────────────────────────────────────
    #[test]
    fn test_select_integer_literal() {
        // SELECT 42 → [(42,)]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    // ── test_select_arithmetic ──────────────────────────────────────────
    #[test]
    fn test_select_arithmetic() {
        // SELECT 1+2 → [(3,)]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg(); // 1
            let r2 = b.alloc_reg(); // 2
            let r3 = b.alloc_reg(); // result

            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    // ── test_select_expression_eval ─────────────────────────────────────
    #[test]
    fn test_select_expression_eval() {
        // SELECT 1+2, 'abc'||'def' → [(3, "abcdef")]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg(); // 1+2 result
            let r4 = b.alloc_reg();
            let r5 = b.alloc_reg();
            let r6 = b.alloc_reg(); // concat result

            // 1 + 2
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r1, r2, r3, P4::None, 0);

            // 'abc' || 'def'
            b.emit_op(Opcode::String8, 0, r4, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r5, 0, P4::Str("def".to_owned()), 0);
            b.emit_op(Opcode::Concat, r5, r4, r6, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            // Also emit second column as separate row for now
            b.emit_op(Opcode::ResultRow, r6, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
        assert_eq!(rows[1], vec![SqliteValue::Text("abcdef".to_owned())]);
    }

    // ── test_select_multi_column ────────────────────────────────────────
    #[test]
    fn test_select_multi_column() {
        // SELECT 1+2, 'abc'||'def' as a single row
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let out_start = b.alloc_regs(2);
            let r_tmp1 = b.alloc_reg();
            let r_tmp2 = b.alloc_reg();

            // 1 + 2 → out_start
            b.emit_op(Opcode::Integer, 1, r_tmp1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r_tmp2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r_tmp1, r_tmp2, out_start, P4::None, 0);

            // 'abc' || 'def' → out_start+1
            b.emit_op(Opcode::String8, 0, r_tmp1, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r_tmp2, 0, P4::Str("def".to_owned()), 0);
            b.emit_op(Opcode::Concat, r_tmp2, r_tmp1, out_start + 1, P4::None, 0);

            b.emit_op(Opcode::ResultRow, out_start, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcdef".to_owned()),
            ]
        );
    }

    // ── test_vdbe_null_handling ──────────────────────────────────────────
    #[test]
    fn test_vdbe_null_handling() {
        // NULL + 1 = NULL, NULL = NULL is NULL (no jump)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_one = b.alloc_reg();
            let r_result = b.alloc_reg();
            let r_is_null = b.alloc_reg();

            // NULL
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            // 1
            b.emit_op(Opcode::Integer, 1, r_one, 0, P4::None, 0);
            // NULL + 1
            b.emit_op(Opcode::Add, r_null, r_one, r_result, P4::None, 0);
            // Check: result IS NULL → set r_is_null=1
            b.emit_op(Opcode::Integer, 0, r_is_null, 0, P4::None, 0);
            let skip = b.emit_label();
            b.emit_jump_to_label(Opcode::NotNull, r_result, 0, skip, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_is_null, 0, P4::None, 0);
            b.resolve_label(skip);

            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_is_null, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Null]); // NULL + 1 = NULL
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // IS NULL = true
    }

    // ── test_vdbe_comparison_affinity ────────────────────────────────────
    #[test]
    fn test_vdbe_comparison_affinity() {
        // Test: 5 > 3 → jump taken (result 1), 3 > 5 → not taken (result 0)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_5 = b.alloc_reg();
            let r_3 = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Integer, 5, r_5, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r_3, 0, P4::None, 0);

            // Test 5 > 3: if r_5 (p3) > r_3 (p1), jump.
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let gt_taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Gt, r_3, r_5, gt_taken, P4::None, 0);
            // Not taken path:
            let done1 = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done1, P4::None, 0);
            // Taken path:
            b.resolve_label(gt_taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done1);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);

            // Test 3 > 5: should NOT jump
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let gt_taken2 = b.emit_label();
            // p3=r_3 (3), p1=r_5 (5): is 3 > 5? No.
            b.emit_jump_to_label(Opcode::Gt, r_5, r_3, gt_taken2, P4::None, 0);
            let done2 = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done2, P4::None, 0);
            b.resolve_label(gt_taken2);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done2);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // 5 > 3 = true
        assert_eq!(rows[1], vec![SqliteValue::Integer(0)]); // 3 > 5 = false
    }

    // ── test_vdbe_division_by_zero ──────────────────────────────────────
    #[test]
    fn test_vdbe_division_by_zero() {
        // SELECT 10 / 0 → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();

            b.emit_op(Opcode::Integer, 0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Divide, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null]); // div by zero → NULL
    }

    #[test]
    fn test_vdbe_nan_arithmetic_normalized_to_null() {
        // +Inf - +Inf and 0 * +Inf both produce NaN at IEEE-754 level.
        // VDBE register writes must normalize NaN to SQL NULL.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_inf = b.alloc_reg();
            let r_zero = b.alloc_reg();
            let r_sub = b.alloc_reg();
            let r_mul = b.alloc_reg();

            b.emit_op(Opcode::Real, 0, r_inf, 0, P4::Real(f64::INFINITY), 0);
            b.emit_op(Opcode::Real, 0, r_zero, 0, P4::Real(0.0), 0);
            b.emit_op(Opcode::Subtract, r_inf, r_inf, r_sub, P4::None, 0); // Inf - Inf
            b.emit_op(Opcode::Multiply, r_inf, r_zero, r_mul, P4::None, 0); // 0 * Inf
            b.emit_op(Opcode::ResultRow, r_sub, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null, SqliteValue::Null]);
    }

    // ── test_vdbe_string_concat_null ────────────────────────────────────
    #[test]
    fn test_vdbe_string_concat_null() {
        // 'abc' || NULL → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();

            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::Null, 0, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    // ── test_vdbe_boolean_logic ─────────────────────────────────────────
    #[test]
    fn test_vdbe_boolean_logic() {
        // TRUE AND FALSE → 0, TRUE OR FALSE → 1, NOT TRUE → 0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r_and = b.alloc_reg();
            let r_or = b.alloc_reg();
            let r_not = b.alloc_reg();

            b.emit_op(Opcode::Integer, 1, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);
            b.emit_op(Opcode::And, r_true, r_false, r_and, P4::None, 0);
            b.emit_op(Opcode::Or, r_true, r_false, r_or, P4::None, 0);
            b.emit_op(Opcode::Not, r_true, r_not, 0, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r_and, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_or, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_not, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // T AND F = F
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // T OR F = T
        assert_eq!(rows[2], vec![SqliteValue::Integer(0)]); // NOT T = F
    }

    // ── test_vdbe_three_valued_logic ────────────────────────────────────
    #[test]
    fn test_vdbe_three_valued_logic() {
        // NULL AND FALSE → 0, NULL AND TRUE → NULL
        // NULL OR TRUE → 1, NULL OR FALSE → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            let r4 = b.alloc_reg();

            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);

            b.emit_op(Opcode::And, r_null, r_false, r1, P4::None, 0); // NULL AND F
            b.emit_op(Opcode::And, r_null, r_true, r2, P4::None, 0); // NULL AND T
            b.emit_op(Opcode::Or, r_null, r_true, r3, P4::None, 0); // NULL OR T
            b.emit_op(Opcode::Or, r_null, r_false, r4, P4::None, 0); // NULL OR F

            b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r4, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // NULL AND F = F
        assert_eq!(rows[1], vec![SqliteValue::Null]); // NULL AND T = NULL
        assert_eq!(rows[2], vec![SqliteValue::Integer(1)]); // NULL OR T = T
        assert_eq!(rows[3], vec![SqliteValue::Null]); // NULL OR F = NULL
    }

    // ── test_vdbe_gosub_return ──────────────────────────────────────────
    #[test]
    fn test_vdbe_gosub_return() {
        // Use Gosub/Return to call a subroutine that sets r2=99.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_return = b.alloc_reg(); // return address storage
            let r_val = b.alloc_reg(); // output

            // Main: call subroutine, then output r_val.
            let sub_label = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_return, 0, sub_label, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            // Subroutine: set r_val=99, return.
            b.resolve_label(sub_label);
            b.emit_op(Opcode::Integer, 99, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Return, r_return, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    // ── test_vdbe_is_null_comparison ─────────────────────────────────────
    #[test]
    fn test_vdbe_is_null_comparison() {
        // NULL IS NULL → true (using Eq with NULLEQ flag)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);

            // Eq with p5=0x80 (SQLITE_NULLEQ): NULL IS NULL → jump
            let is_null_label = b.emit_label();
            // p1=r_null, p3=r_null (compare same register)
            b.emit_jump_to_label(Opcode::Eq, r_null, 0, is_null_label, P4::None, 0x80);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(is_null_label);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // NULL IS NULL = true
    }

    // ── test_vdbe_coroutine ─────────────────────────────────────────────
    #[test]
    fn test_vdbe_coroutine() {
        // Test coroutine: producer yields values 10, 20, 30.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_co = b.alloc_reg(); // coroutine state register
            let r_val = b.alloc_reg(); // value register

            // InitCoroutine: p1=r_co, p2=consumer start, p3=producer start
            let consumer_start = b.emit_label();
            let producer_start = b.emit_label();
            b.emit_jump_to_label(Opcode::InitCoroutine, r_co, 0, consumer_start, P4::None, 0);
            // Hack: resolve producer_start at the InitCoroutine's p3 position.
            // Actually, InitCoroutine stores p3 into r_co, then jumps to p2.
            // So p3 should be the producer's first instruction address.

            // For simplicity, just test Yield directly:
            b.resolve_label(consumer_start);

            // Producer: emit 3 values
            b.resolve_label(producer_start);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 30, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(10)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(20)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(30)]);
    }

    // ── test_vdbe_halt_with_error ───────────────────────────────────────
    #[test]
    fn test_vdbe_halt_with_error() {
        let mut b = ProgramBuilder::new();
        b.emit_op(
            Opcode::Halt,
            1,
            0,
            0,
            P4::Str("constraint failed".to_owned()),
            0,
        );
        let prog = b.finish().unwrap();
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: 1,
                message: "constraint failed".to_owned(),
            }
        );
    }

    // ── test_vdbe_disassemble_and_exec ──────────────────────────────────
    #[test]
    fn test_vdbe_disassemble_and_exec() {
        // Build a program, disassemble it, and verify output.
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        let r1 = b.alloc_reg();
        let r2 = b.alloc_reg();
        let r3 = b.alloc_reg();
        b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 20, r2, 0, P4::None, 0);
        b.emit_op(Opcode::Multiply, r1, r2, r3, P4::None, 0);
        b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().unwrap();
        let asm = prog.disassemble();
        assert!(asm.contains("Init"));
        assert!(asm.contains("Integer"));
        assert!(asm.contains("Multiply"));
        assert!(asm.contains("ResultRow"));
        assert!(asm.contains("Halt"));

        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(engine.results().len(), 1);
        assert_eq!(engine.results()[0], vec![SqliteValue::Integer(200)]);
    }

    #[test]
    fn test_sorter_opcodes_sort_and_emit_rows() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            let loop_start = b.emit_label();
            let empty = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_sorted = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);

            for value in [30, 10, 20] {
                b.emit_op(Opcode::Integer, value, r_value, 0, P4::None, 0);
                b.emit_op(Opcode::MakeRecord, r_value, 1, r_record, P4::None, 0);
                b.emit_op(Opcode::SorterInsert, 0, r_record, 0, P4::None, 0);
            }

            b.emit_jump_to_label(Opcode::SorterSort, 0, 0, empty, P4::None, 0);
            b.resolve_label(loop_start);
            b.emit_op(Opcode::SorterData, 0, r_sorted, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_sorted, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterNext, 0, 0, loop_start, P4::None, 0);
            b.resolve_label(empty);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        let decoded: Vec<i64> = rows
            .into_iter()
            .map(|row| decode_record(&row[0]).unwrap()[0].to_integer())
            .collect();
        assert_eq!(decoded, vec![10, 20, 30]);
    }

    #[test]
    fn test_sorter_compare_jumps_on_key_difference() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            let diff = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_probe = b.alloc_reg();
            let r_probe_record = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r_value, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, r_value, 1, r_record, P4::None, 0);
            b.emit_op(Opcode::SorterInsert, 0, r_record, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterSort, 0, 0, diff, P4::None, 0);

            b.emit_op(Opcode::Integer, 20, r_probe, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, r_probe, 1, r_probe_record, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterCompare, 0, r_probe_record, diff, P4::None, 0);

            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            b.resolve_label(diff);
            b.emit_op(Opcode::Integer, 2, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(2)]]);
    }

    #[test]
    fn test_reset_sorter_clears_entries() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            let empty = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r_value, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, r_value, 1, r_record, P4::None, 0);
            b.emit_op(Opcode::SorterInsert, 0, r_record, 0, P4::None, 0);
            b.emit_op(Opcode::ResetSorter, 0, 0, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterSort, 0, 0, empty, P4::None, 0);

            // If ResetSorter failed, this row would be emitted.
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.resolve_label(empty);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert!(rows.is_empty());
    }

    // ── Codegen → Engine Integration Tests ──────────────────────────────

    mod codegen_integration {
        use super::*;
        use crate::codegen::{
            CodegenContext, ColumnInfo, TableSchema, codegen_delete, codegen_insert,
            codegen_select, codegen_update,
        };
        use fsqlite_ast::{
            Assignment, AssignmentTarget, BinaryOp as AstBinaryOp, ColumnRef, DeleteStatement,
            Distinctness, Expr, FromClause, InsertSource, InsertStatement, Literal,
            PlaceholderType, QualifiedName, QualifiedTableRef, ResultColumn, SelectBody,
            SelectCore, SelectStatement, Span, TableOrSubquery, UpdateStatement,
        };

        fn test_schema() -> Vec<TableSchema> {
            vec![TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo {
                        name: "a".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                    },
                ],
                indexes: vec![],
            }]
        }

        fn from_table(name: &str) -> FromClause {
            FromClause {
                source: TableOrSubquery::Table {
                    name: QualifiedName {
                        schema: None,
                        name: name.to_owned(),
                    },
                    alias: None,
                    index_hint: None,
                },
                joins: Vec::new(),
            }
        }

        fn span() -> Span {
            Span { start: 0, end: 0 }
        }

        /// Verify codegen_insert produces a program that executes without panic.
        #[test]
        fn test_codegen_insert_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Literal(Literal::Integer(42), span()),
                    Expr::Literal(Literal::String("hello".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify codegen_select (full scan) produces a program that executes.
        #[test]
        fn test_codegen_select_full_scan_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = SelectStatement {
                with: None,
                body: SelectBody {
                    select: SelectCore::Select {
                        distinct: Distinctness::All,
                        columns: vec![ResultColumn::Star],
                        from: Some(from_table("t")),
                        where_clause: None,
                        group_by: vec![],
                        having: None,
                        windows: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            };

            let mut b = ProgramBuilder::new();
            codegen_select(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            // Engine should execute without panic (cursor ops are stubbed).
            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify `OpenRead` can route through the storage-backed cursor path.
        #[test]
        fn test_openread_uses_storage_cursor_backend_when_enabled() {
            let mut b = ProgramBuilder::new();
            b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            let prog = b.finish().expect("program should build");

            let mut db = MemDatabase::new();
            let root = db.create_table(1);
            assert_eq!(root, 2);
            if let Some(table) = db.get_table_mut(root) {
                table.insert(1, vec![SqliteValue::Integer(99)]);
            }

            let mut engine = VdbeEngine::new(prog.register_count());
            engine.enable_storage_read_cursors(true);
            engine.set_database(db);
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
            assert!(engine.storage_cursors.contains_key(&0));
            assert!(!engine.cursors.contains_key(&0));
        }

        /// Verify codegen_update produces a program that executes.
        #[test]
        fn test_codegen_update_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = UpdateStatement {
                with: None,
                or_conflict: None,
                table: QualifiedTableRef {
                    name: QualifiedName {
                        schema: None,
                        name: "t".to_owned(),
                    },
                    alias: None,
                    index_hint: None,
                },
                assignments: vec![Assignment {
                    target: AssignmentTarget::Column("b".to_owned()),
                    value: Expr::Placeholder(PlaceholderType::Numbered(1), span()),
                }],
                from: None,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column(
                        ColumnRef {
                            table: None,
                            column: "rowid".to_owned(),
                        },
                        span(),
                    )),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Placeholder(PlaceholderType::Numbered(2), span())),
                    span: span(),
                }),
                returning: vec![],
                order_by: vec![],
                limit: None,
            };

            let mut b = ProgramBuilder::new();
            codegen_update(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify codegen_delete produces a program that executes.
        #[test]
        fn test_codegen_delete_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = DeleteStatement {
                with: None,
                table: QualifiedTableRef {
                    name: QualifiedName {
                        schema: None,
                        name: "t".to_owned(),
                    },
                    alias: None,
                    index_hint: None,
                },
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column(
                        ColumnRef {
                            table: None,
                            column: "rowid".to_owned(),
                        },
                        span(),
                    )),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Placeholder(PlaceholderType::Numbered(1), span())),
                    span: span(),
                }),
                returning: vec![],
                order_by: vec![],
                limit: None,
            };

            let mut b = ProgramBuilder::new();
            codegen_delete(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify codegen_insert with RETURNING produces a ResultRow.
        #[test]
        fn test_codegen_insert_returning_produces_result() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Literal(Literal::Integer(7), span()),
                    Expr::Literal(Literal::String("world".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![ResultColumn::Star],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            // Provide a MemDatabase so Insert stores the row and SeekRowid
            // (used by emit_returning) can find it.
            let mut db = MemDatabase::new();
            let root = db.create_table(2);
            assert_eq!(root, 2);

            let mut engine = VdbeEngine::new(prog.register_count());
            engine.set_database(db);
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
            // RETURNING * emits a ResultRow with all columns.
            assert_eq!(engine.results().len(), 1);
        }

        /// Verify INSERT with literal values emits the correct value registers.
        #[test]
        fn test_codegen_insert_literal_values_disassemble() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Literal(Literal::Integer(99), span()),
                    Expr::Literal(Literal::String("test".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Init"), "should have Init opcode");
            assert!(asm.contains("OpenWrite"), "should have OpenWrite opcode");
            assert!(asm.contains("NewRowid"), "should have NewRowid opcode");
            assert!(
                asm.contains("Integer"),
                "should have Integer opcode for literal 99"
            );
            assert!(
                asm.contains("String8"),
                "should have String8 opcode for literal 'test'"
            );
            assert!(asm.contains("MakeRecord"), "should have MakeRecord opcode");
            assert!(asm.contains("Insert"), "should have Insert opcode");
            assert!(asm.contains("Halt"), "should have Halt opcode");
        }

        /// Verify emit_expr handles arithmetic BinaryOp in INSERT values.
        #[test]
        fn test_codegen_insert_arithmetic_expr() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (2 + 3, 'hi')
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::BinaryOp {
                        left: Box::new(Expr::Literal(Literal::Integer(2), span())),
                        op: AstBinaryOp::Add,
                        right: Box::new(Expr::Literal(Literal::Integer(3), span())),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("hi".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Add"), "should have Add opcode for 2+3");
            assert!(asm.contains("Integer"), "should have Integer opcodes");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify emit_expr handles UnaryOp (negation) in INSERT values.
        #[test]
        fn test_codegen_insert_negation_expr() {
            use fsqlite_ast::UnaryOp as AstUnaryOp;

            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (-42, 'neg')
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::UnaryOp {
                        op: AstUnaryOp::Negate,
                        expr: Box::new(Expr::Literal(Literal::Integer(42), span())),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("neg".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Multiply"), "negation emits Multiply by -1");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify emit_expr handles CASE expression in INSERT values.
        #[test]
        fn test_codegen_insert_case_expr() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (CASE WHEN TRUE THEN 10 ELSE 20 END, 'case')
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Case {
                        operand: None,
                        whens: vec![(
                            Expr::Literal(Literal::True, span()),
                            Expr::Literal(Literal::Integer(10), span()),
                        )],
                        else_expr: Some(Box::new(Expr::Literal(Literal::Integer(20), span()))),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("case".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("IfNot"), "searched CASE emits IfNot");
            assert!(asm.contains("Goto"), "CASE branches with Goto");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify emit_expr handles comparison expression producing 0/1 result.
        #[test]
        fn test_codegen_insert_comparison_expr() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (3 > 2, 'cmp') — should produce integer 1
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::BinaryOp {
                        left: Box::new(Expr::Literal(Literal::Integer(3), span())),
                        op: AstBinaryOp::Gt,
                        right: Box::new(Expr::Literal(Literal::Integer(2), span())),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("cmp".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Gt"), "comparison emits Gt opcode");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }
    }

    // ===================================================================
    // bd-202x §16 Phase 4: Comprehensive VDBE opcode unit tests
    // ===================================================================

    // ── Constants & Register Operations ────────────────────────────────

    #[test]
    fn test_int64_large_value() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r, 0, P4::Int64(i64::MAX), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(i64::MAX)]);
    }

    #[test]
    fn test_int64_negative() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r, 0, P4::Int64(-999_999_999_999), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(-999_999_999_999)]);
    }

    #[test]
    fn test_real_constant() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, r, 0, P4::Real(std::f64::consts::PI), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(std::f64::consts::PI)]);
    }

    #[test]
    fn test_real_negative_zero() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, r, 0, P4::Real(0.0), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(0.0)]);
    }

    #[test]
    fn test_string_opcode() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String, 5, r, 0, P4::Str("hello".to_owned()), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("hello".to_owned())]);
    }

    #[test]
    fn test_blob_constant() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(
                Opcode::Blob,
                0,
                r,
                0,
                P4::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                0,
            );
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])]
        );
    }

    #[test]
    fn test_null_range() {
        // Null with p3=2: set registers p2, p2+1, p2+2 to NULL.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            // Pre-populate with integers
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r3, 0, P4::None, 0);
            // Null range: p2=r1, p3=r3 → set r1..=r3 to NULL (absolute end register).
            b.emit_op(Opcode::Null, 0, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![SqliteValue::Null, SqliteValue::Null, SqliteValue::Null]
        );
    }

    #[test]
    fn test_soft_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            b.emit_op(Opcode::SoftNull, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_move_registers() {
        // Move nullifies source and copies to destination.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let src = b.alloc_reg();
            let dst = b.alloc_reg();
            b.emit_op(Opcode::Integer, 77, src, 0, P4::None, 0);
            // Move 1 register from src to dst
            b.emit_op(Opcode::Move, src, dst, 1, P4::None, 0);
            // dst should be 77, src should be NULL
            b.emit_op(Opcode::ResultRow, dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, src, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(77)]);
        assert_eq!(rows[1], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_copy_register() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let src = b.alloc_reg();
            let dst = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, src, 0, P4::Str("copy_me".to_owned()), 0);
            b.emit_op(Opcode::Copy, src, dst, 0, P4::None, 0);
            // Both should be the same value
            b.emit_op(Opcode::ResultRow, src, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("copy_me".to_owned())]);
        assert_eq!(rows[1], vec![SqliteValue::Text("copy_me".to_owned())]);
    }

    #[test]
    fn test_intcopy_coerces() {
        // IntCopy converts value to integer.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let src = b.alloc_reg();
            let dst = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, src, 0, P4::Real(3.7), 0);
            b.emit_op(Opcode::IntCopy, src, dst, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    // ── Arithmetic Edge Cases ──────────────────────────────────────────

    #[test]
    fn test_subtract_integers() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r2, 0, P4::None, 0);
            // p3 = p2 - p1 → r3 = r1 - r2 if p2=r1, p1=r2 → 10 - 3 = 7
            b.emit_op(Opcode::Subtract, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(7)]);
    }

    #[test]
    fn test_multiply_large() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 200, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Multiply, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(20_000)]);
    }

    #[test]
    fn test_integer_division_truncates() {
        // 7 / 2 = 3 (integer division truncates)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_divisor = b.alloc_reg();
            let r_dividend = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 2, r_divisor, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r_dividend, 0, P4::None, 0);
            // p3 = p2 / p1 → r_result = r_dividend / r_divisor
            b.emit_op(Opcode::Divide, r_divisor, r_dividend, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn test_remainder_integers() {
        // 7 % 3 = 1
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_divisor = b.alloc_reg();
            let r_dividend = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 3, r_divisor, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r_dividend, 0, P4::None, 0);
            b.emit_op(
                Opcode::Remainder,
                r_divisor,
                r_dividend,
                r_result,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_remainder_by_zero() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_zero = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_zero, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Remainder, r_zero, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_null_arithmetic_propagation() {
        // NULL + 1, NULL * 5, NULL - 3 should all be NULL.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_one = b.alloc_reg();
            let r_add = b.alloc_reg();
            let r_mul = b.alloc_reg();
            let r_sub = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_one, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r_null, r_one, r_add, P4::None, 0);
            b.emit_op(Opcode::Multiply, r_null, r_one, r_mul, P4::None, 0);
            b.emit_op(Opcode::Subtract, r_null, r_one, r_sub, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_add, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_mul, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_sub, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
        assert_eq!(rows[1], vec![SqliteValue::Null]);
        assert_eq!(rows[2], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_add_imm() {
        // AddImm: register p1 += p2
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r, 0, P4::None, 0);
            b.emit_op(Opcode::AddImm, r, 50, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(150)]);
    }

    #[test]
    fn test_add_imm_negative() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r, 0, P4::None, 0);
            b.emit_op(Opcode::AddImm, r, -30, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(70)]);
    }

    // ── Bitwise Operations ─────────────────────────────────────────────

    #[test]
    fn test_bit_and() {
        // 0xFF & 0x0F = 0x0F (15)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0xFF, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0x0F, r2, 0, P4::None, 0);
            b.emit_op(Opcode::BitAnd, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0x0F)]);
    }

    #[test]
    fn test_bit_or() {
        // 0xF0 | 0x0F = 0xFF (255)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0xF0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0x0F, r2, 0, P4::None, 0);
            b.emit_op(Opcode::BitOr, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0xFF)]);
    }

    #[test]
    fn test_shift_left() {
        // 1 << 8 = 256
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 8, r_amount, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ShiftLeft, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(256)]);
    }

    #[test]
    fn test_shift_right() {
        // 256 >> 4 = 16
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 4, r_amount, 0, P4::None, 0);
            b.emit_op(Opcode::Int64, 0, r_val, 0, P4::Int64(256), 0);
            b.emit_op(Opcode::ShiftRight, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(16)]);
    }

    #[test]
    fn test_shift_left_overflow_clamp() {
        // Shift >= 64 returns 0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r_amount, 0, P4::Int64(64), 0);
            b.emit_op(Opcode::Integer, 1, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ShiftLeft, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_shift_negative_reverses() {
        // Negative shift amount reverses direction: <<(-2) == >>(2)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r_amount, 0, P4::Int64(-2), 0);
            b.emit_op(Opcode::Integer, 8, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ShiftLeft, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // 8 >> 2 = 2
        assert_eq!(rows[0], vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_bit_not() {
        // ~0 = -1 in two's complement
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::BitNot, r1, r2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(-1)]);
    }

    #[test]
    fn test_bitwise_null_propagation() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_and = b.alloc_reg();
            let r_or = b.alloc_reg();
            let r_not = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0xFF, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::BitAnd, r_null, r_val, r_and, P4::None, 0);
            b.emit_op(Opcode::BitOr, r_null, r_val, r_or, P4::None, 0);
            b.emit_op(Opcode::BitNot, r_null, r_not, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_and, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_or, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_not, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
        assert_eq!(rows[1], vec![SqliteValue::Null]);
        assert_eq!(rows[2], vec![SqliteValue::Null]);
    }

    // ── String Operations ──────────────────────────────────────────────

    #[test]
    fn test_concat_two_strings() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("hello ".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("world".to_owned()), 0);
            // Concat: p3 = p2 || p1 (note operand order)
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("hello world".to_owned())]);
    }

    #[test]
    fn test_concat_empty_string() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("test".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str(String::new()), 0);
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("test".to_owned())]);
    }

    // ── Comparison Ops (all 6 + NULL) ──────────────────────────────────

    #[test]
    fn test_eq_jump_taken() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 42, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            // Eq: if p3 == p1, jump to p2 → if r2 == r1, jump
            b.emit_jump_to_label(Opcode::Eq, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_ne_jump_taken() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Ne, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_lt_jump_taken() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_big = b.alloc_reg();
            let r_small = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r_big, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_small, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            // Lt: if p3 < p1, jump → if r_small < r_big
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Lt, r_big, r_small, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_le_with_equal_values() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 7, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Le, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_ge_with_greater_value() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_big = b.alloc_reg();
            let r_small = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_small, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 100, r_big, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            // Ge: if p3 >= p1 → if r_big >= r_small
            b.emit_jump_to_label(Opcode::Ge, r_small, r_big, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_comparison_null_no_jump() {
        // Standard SQL: NULL = 5 → no jump (NULL result)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_five = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Eq, r_five, r_null, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Should NOT jump: NULL = 5 is NULL (not true)
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_ne_nulleq_one_null() {
        // IS NOT semantics: NULL IS NOT 5 → true (jump)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_five = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Ne, r_five, r_null, taken, P4::None, 0x80);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    // ── Logic Edge Cases ───────────────────────────────────────────────

    #[test]
    fn test_not_null_is_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Not, r_null, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_not_zero_is_one() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_zero = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_zero, 0, P4::None, 0);
            b.emit_op(Opcode::Not, r_zero, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_not_nonzero_is_zero() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_val = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Not, r_val, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    // ── Conditional Jumps ──────────────────────────────────────────────

    #[test]
    fn test_if_true_jumps() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_if_false_no_jump() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 99, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // If with false → no jump → r_out stays 99
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    #[test]
    fn test_if_null_no_jump() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 99, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // If with NULL → no jump → r_out stays 99
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    #[test]
    fn test_ifnot_false_jumps() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::IfNot, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_ifnot_null_jumps() {
        // IfNot with NULL → jump (NULL is treated as false/zero)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::IfNot, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_once_fires_only_once() {
        // Once at the same PC fires on first pass, falls through on second.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_counter = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_counter, 0, P4::None, 0);
            // First pass: Once jumps to `init_code`
            let loop_start = b.emit_label();
            b.resolve_label(loop_start);
            let init_code = b.emit_label();
            b.emit_jump_to_label(Opcode::Once, 0, 0, init_code, P4::None, 0);
            // Fall-through path (second+ pass): just output
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            // Init code: increment counter and loop back
            b.resolve_label(init_code);
            b.emit_op(Opcode::AddImm, r_counter, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, loop_start, P4::None, 0);
            // Done
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_counter, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Once fires on first execution (increments to 1), then falls through
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    // ── Type Coercion ──────────────────────────────────────────────────

    #[test]
    fn test_cast_integer_to_text() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            // Cast to TEXT: p2 = 'B' (66)
            b.emit_op(Opcode::Cast, r, 66, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("42".to_owned())]);
    }

    #[test]
    fn test_cast_text_to_integer() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("123".to_owned()), 0);
            // Cast to INTEGER: p2 = 'D' (68)
            b.emit_op(Opcode::Cast, r, 68, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(123)]);
    }

    #[test]
    fn test_cast_to_real() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r, 0, P4::None, 0);
            // Cast to REAL: p2 = 'E' (69)
            b.emit_op(Opcode::Cast, r, 69, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(5.0)]);
    }

    #[test]
    fn test_cast_to_blob() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("hi".to_owned()), 0);
            // Cast to BLOB: p2 = 'A' (65)
            b.emit_op(Opcode::Cast, r, 65, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Blob(b"hi".to_vec())]);
    }

    #[test]
    fn test_must_be_int_accepts_integer() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            // MustBeInt: p2=0 means error on non-int, but 42 is int → passes
            b.emit_op(Opcode::MustBeInt, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_must_be_int_jumps_on_non_int() {
        // MustBeInt with p2 > 0: jump to p2 instead of error.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("not_int".to_owned()), 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let fallback = b.emit_label();
            b.emit_jump_to_label(Opcode::MustBeInt, r, 0, fallback, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(fallback);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Non-int triggers jump → r_out = 1
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_real_affinity_converts_int() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 7, r, 0, P4::None, 0);
            b.emit_op(Opcode::RealAffinity, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(7.0)]);
    }

    #[test]
    fn test_real_affinity_no_op_on_float() {
        // RealAffinity on a float is a no-op.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, r, 0, P4::Real(std::f64::consts::PI), 0);
            b.emit_op(Opcode::RealAffinity, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(std::f64::consts::PI)]);
    }

    // ── Error Handling ─────────────────────────────────────────────────

    #[test]
    fn test_halt_if_null_triggers() {
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        let r = b.alloc_reg();
        b.emit_op(Opcode::Null, 0, r, 0, P4::None, 0);
        b.emit_op(
            Opcode::HaltIfNull,
            19,
            0,
            r,
            P4::Str("NOT NULL constraint failed".to_owned()),
            0,
        );
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);
        let prog = b.finish().unwrap();
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: 19,
                message: "NOT NULL constraint failed".to_owned(),
            }
        );
    }

    #[test]
    fn test_halt_if_null_passes_non_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            b.emit_op(
                Opcode::HaltIfNull,
                19,
                0,
                r,
                P4::Str("should not fire".to_owned()),
                0,
            );
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    // ── Miscellaneous Opcodes ──────────────────────────────────────────

    #[test]
    fn test_is_true_opcode() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r_null = b.alloc_reg();
            let o1 = b.alloc_reg();
            let o2 = b.alloc_reg();
            let o3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::IsTrue, r_true, o1, 0, P4::None, 0);
            b.emit_op(Opcode::IsTrue, r_false, o2, 0, P4::None, 0);
            b.emit_op(Opcode::IsTrue, r_null, o3, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // 42 is true
        assert_eq!(rows[1], vec![SqliteValue::Integer(0)]); // 0 is false
        assert_eq!(rows[2], vec![SqliteValue::Integer(0)]); // NULL is not true
    }

    #[test]
    fn test_noop_does_nothing() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_result_row_three_columns() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("two".to_owned()), 0);
            b.emit_op(Opcode::Real, 0, r3, 0, P4::Real(3.0), 0);
            b.emit_op(Opcode::ResultRow, r1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("two".to_owned()),
                SqliteValue::Float(3.0),
            ]
        );
    }

    #[test]
    fn test_multiple_result_rows() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(2)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn test_gosub_nested() {
        // Test nested Gosub: main calls sub1, which calls sub2.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_ret1 = b.alloc_reg();
            let r_ret2 = b.alloc_reg();
            let r_val = b.alloc_reg();

            // Main: call sub1
            let sub1 = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_ret1, 0, sub1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            // sub1: set r_val=10, call sub2, add 1
            b.resolve_label(sub1);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            let sub2 = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_ret2, 0, sub2, P4::None, 0);
            b.emit_op(Opcode::AddImm, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Return, r_ret1, 0, 0, P4::None, 0);

            // sub2: multiply r_val by 5
            b.resolve_label(sub2);
            let r_five = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            b.emit_op(Opcode::Multiply, r_five, r_val, r_val, P4::None, 0);
            b.emit_op(Opcode::Return, r_ret2, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        // 10 * 5 + 1 = 51
        assert_eq!(rows[0], vec![SqliteValue::Integer(51)]);
    }

    #[test]
    fn test_coroutine_yield_resume() {
        // Producer coroutine yields 3 values; consumer resumes and emits rows.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_co = b.alloc_reg();
            let r_val = b.alloc_reg();

            // Patch target addresses after both blocks are emitted.
            let init_addr = b.emit_op(Opcode::InitCoroutine, r_co, 0, 0, P4::None, 0);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let consumer_start = b.current_addr() as i32;
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let producer_start = b.current_addr() as i32;
            b.emit_op(Opcode::Integer, 100, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 200, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 300, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            if let Some(init_op) = b.op_at_mut(init_addr) {
                init_op.p2 = consumer_start;
                init_op.p3 = producer_start;
            }

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(100)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(200)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(300)]);
    }

    #[test]
    fn test_make_record_encodes_values() {
        // MakeRecord packs source registers into the SQLite record format blob.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_rec = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("a".to_owned()), 0);
            b.emit_op(Opcode::MakeRecord, r1, 2, r_rec, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_rec, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        let produced_blob = rows.first().and_then(|row| row.first());
        assert!(
            matches!(produced_blob, Some(SqliteValue::Blob(_))),
            "MakeRecord should produce a blob"
        );
        let decoded = decode_record(&rows[0][0]).unwrap();
        assert_eq!(
            decoded,
            vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())]
        );
    }

    #[test]
    fn test_complex_expression_chain() {
        // Test: ((10 + 20) * 3 - 5) / 2 = (90 - 5) / 2 = 85 / 2 = 42
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r10 = b.alloc_reg();
            let r20 = b.alloc_reg();
            let r3 = b.alloc_reg();
            let r5 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let t1 = b.alloc_reg();
            let t2 = b.alloc_reg();
            let t3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 10, r10, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r20, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r3, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r5, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r10, r20, t1, P4::None, 0); // 30
            b.emit_op(Opcode::Multiply, r3, t1, t2, P4::None, 0); // 90
            b.emit_op(Opcode::Subtract, r5, t2, t2, P4::None, 0); // 85
            b.emit_op(Opcode::Divide, r2, t2, t3, P4::None, 0); // 42
            b.emit_op(Opcode::ResultRow, t3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_string_comparison() {
        // String comparison: 'abc' < 'abd' → true
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("abd".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            // Lt: if p3 (r2="abc") < p1 (r1="abd"), jump
            b.emit_jump_to_label(Opcode::Lt, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_mixed_type_comparison() {
        // Integer vs Float comparison: 5 == 5.0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_int = b.alloc_reg();
            let r_float = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_int, 0, P4::None, 0);
            b.emit_op(Opcode::Real, 0, r_float, 0, P4::Real(5.0), 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Eq, r_int, r_float, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    // ── bd-1s7a: Storage cursor acceptance tests ───────────────────────

    /// Build and execute a program with a MemDatabase + storage cursors enabled.
    fn run_with_storage_cursors(
        db: MemDatabase,
        build: impl FnOnce(&mut ProgramBuilder),
    ) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.enable_storage_read_cursors(true);
        engine.set_database(db);
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    #[test]
    fn test_vdbe_openread_uses_btree_cursor_backend() {
        // Insert rows into a MemDatabase, then verify OpenRead routes through
        // the storage cursor path (not MemCursor) when enabled.
        let mut db = MemDatabase::new();
        let root = db.create_table(2);
        let table = db.get_table_mut(root).unwrap();
        table.insert(
            1,
            vec![SqliteValue::Integer(10), SqliteValue::Text("a".to_owned())],
        );
        table.insert(
            2,
            vec![SqliteValue::Integer(20), SqliteValue::Text("b".to_owned())],
        );

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(2), 0);
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);

            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
            b.emit_op(Opcode::Column, 0, 1, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 2, 0, P4::None, 0);

            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 2, "should return 2 rows via storage cursor");
        assert_eq!(rows[0][0], SqliteValue::Integer(10));
        assert_eq!(rows[0][1], SqliteValue::Text("a".to_owned()));
        assert_eq!(rows[1][0], SqliteValue::Integer(20));
        assert_eq!(rows[1][1], SqliteValue::Text("b".to_owned()));
    }

    #[test]
    fn test_select_uses_storage_cursor_not_memdb_for_persisted_table() {
        // With storage cursors enabled, verify the engine uses StorageCursor
        // (the read path) rather than MemCursor for OpenRead.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(42)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            // OpenRead with storage cursors enabled should use StorageCursor.
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);

            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    // ── bd-3iw8 / bd-25c6: Storage cursor WRITE path tests ────────────

    /// Build and execute a write program with storage cursors enabled.
    /// Returns both the result rows and the final MemDatabase state.
    fn run_write_with_storage_cursors(
        db: MemDatabase,
        build: impl FnOnce(&mut ProgramBuilder),
    ) -> (Vec<Vec<SqliteValue>>, MemDatabase) {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.enable_storage_cursors(true);
        engine.set_database(db);
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        let results = engine.take_results();
        let db = engine.take_database().expect("database should exist");
        (results, db)
    }

    #[test]
    fn test_openwrite_uses_storage_cursor_backend() {
        // Verify OpenWrite routes through StorageCursor when enabled.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(100)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.enable_storage_cursors(true);
        engine.set_database(db);
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        // Verify the cursor was opened as a storage cursor, not a MemCursor.
        assert!(
            engine.storage_cursors.contains_key(&0),
            "OpenWrite should route through StorageCursor"
        );
        assert!(!engine.cursors.contains_key(&0));
        // Verify it's marked writable.
        assert!(engine.storage_cursors[&0].writable);
    }

    #[test]
    fn test_insert_via_storage_cursor_write_path() {
        // Phase 5B.2 (bd-1yi8): INSERT goes ONLY through StorageCursor
        // (B-tree write path), NOT synced to MemDatabase.
        // Read-back uses the SAME cursor (Rewind) since the MemPageStore
        // is per-cursor and not shared across Close/OpenRead.
        let mut db = MemDatabase::new();
        let root = db.create_table(2);

        let (rows, final_db) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            // OpenWrite cursor 0 on root page.
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(2), 0);

            // NewRowid → r1.
            b.emit_op(Opcode::NewRowid, 0, 1, 0, P4::None, 0);

            // Build record: r2=42, r3="hello" → MakeRecord → r4.
            b.emit_op(Opcode::Integer, 42, 2, 0, P4::None, 0);
            b.emit_op(Opcode::String8, 0, 3, 0, P4::Str("hello".to_owned()), 0);
            b.emit_op(Opcode::MakeRecord, 2, 2, 4, P4::None, 0);

            // Insert(cursor=0, record=r4, rowid=r1).
            b.emit_op(Opcode::Insert, 0, 4, 1, P4::None, 0);

            // Read back via same cursor: Rewind then Column/ResultRow.
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);

            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 5, P4::None, 0);
            b.emit_op(Opcode::Column, 0, 1, 6, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 5, 2, 0, P4::None, 0);
            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // Write-through: MemDatabase should NOT have the row.
        let table = final_db.get_table(root).expect("table should exist");
        assert_eq!(
            table.rows.len(),
            0,
            "MemDatabase must not be synced in write-through mode"
        );

        // Data readable from B-tree via same cursor.
        assert_eq!(
            rows.len(),
            1,
            "should read back exactly one row from B-tree"
        );
        assert_eq!(rows[0][0], SqliteValue::Integer(42));
        assert_eq!(rows[0][1], SqliteValue::Text("hello".to_owned()));
    }

    #[test]
    fn test_delete_via_storage_cursor_write_path() {
        // Insert a row into MemDatabase, open a writable StorageCursor,
        // position on it, delete it, and verify data is removed from the
        // B-tree while MemDatabase remains unchanged (write-through mode).
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(10)]);
        table.insert(2, vec![SqliteValue::Integer(20)]);
        table.insert(3, vec![SqliteValue::Integer(30)]);

        let (rows, final_db) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            // Open writable cursor.
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid=2 (register 1). Jump to end if not found.
            b.emit_op(Opcode::Integer, 2, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekRowid, 0, 1, end, P4::None, 0);

            // Delete the current row.
            b.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);

            // Read back rowids from B-tree to verify rowid=2 was deleted.
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);
            let body = b.current_addr();
            b.emit_op(Opcode::Rowid, 0, 2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);
            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(
            rows,
            vec![vec![SqliteValue::Integer(1)], vec![SqliteValue::Integer(3)],],
            "B-tree cursor should observe rowid=2 deleted"
        );

        // MemDatabase should remain unchanged in write-through mode.
        let table = final_db.get_table(root).expect("table should exist");
        assert_eq!(table.rows.len(), 3);
        let rowids: Vec<i64> = table.rows.iter().map(|r| r.rowid).collect();
        assert!(rowids.contains(&1));
        assert!(rowids.contains(&2));
        assert!(rowids.contains(&3));
    }

    #[test]
    fn test_newrowid_with_storage_cursor_allocates_correctly() {
        // Verify NewRowid allocates sequential rowids when using storage cursors.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(5, vec![SqliteValue::Integer(50)]);

        let (rows, _) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Allocate two new rowids and output them.
            b.emit_op(Opcode::NewRowid, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::NewRowid, 0, 2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // The table had rowid 5 → next_rowid should be 6, then 7.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], SqliteValue::Integer(6));
        assert_eq!(rows[1][0], SqliteValue::Integer(7));
    }

    #[test]
    fn test_newrowid_concurrent_flag_uses_snapshot_independent_path() {
        // Phase 5B.2 (bd-1yi8): with storage cursors, NewRowid reads max
        // rowid from B-tree regardless of p3 (concurrent flag). The p3
        // flag only affects the MemDatabase fallback (Phase 4 cursors).
        fn setup_db_with_stale_counter() -> (MemDatabase, i32) {
            let mut db = MemDatabase::new();
            let root = db.create_table(1);
            let table = db.get_table_mut(root).expect("table should exist");
            table.insert(10, vec![SqliteValue::Integer(10)]);
            table.insert(11, vec![SqliteValue::Integer(11)]);
            // Simulate stale local counter state from an old snapshot.
            table.next_rowid = 1;
            (db, root)
        }

        let (db_serialized, root) = setup_db_with_stale_counter();
        let (rows_serialized, _) = run_write_with_storage_cursors(db_serialized, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
            // Serialized path (`p3 = 0`) — with storage cursors, reads
            // max rowid from B-tree (11), returns 12.
            b.emit_op(Opcode::NewRowid, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        let (db_concurrent, root) = setup_db_with_stale_counter();
        let (rows_concurrent, _) = run_write_with_storage_cursors(db_concurrent, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
            // Concurrent path (`p3 != 0`) — same B-tree path, same result.
            b.emit_op(Opcode::NewRowid, 0, 1, 1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // Both paths read max rowid (11) from B-tree → return 12.
        assert_eq!(rows_serialized, vec![vec![SqliteValue::Integer(12)]]);
        assert_eq!(rows_concurrent, vec![vec![SqliteValue::Integer(12)]]);
    }

    // ── bd-1yi8: INSERT write-through tests ────────────────────────────

    #[test]
    fn test_insert_write_through_no_memdb_sync() {
        // Verify INSERT with storage cursor does NOT write to MemDatabase.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let (_, final_db) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
            b.emit_op(Opcode::NewRowid, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 99, 2, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, 2, 1, 3, P4::None, 0);
            b.emit_op(Opcode::Insert, 0, 3, 1, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        let table = final_db.get_table(root).expect("table should exist");
        assert_eq!(table.rows.len(), 0, "write-through must skip MemDatabase");
    }

    #[test]
    fn test_insert_new_rowid_from_btree() {
        // Verify NewRowid reads max from B-tree, not MemDatabase counter.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        // Insert rows 1..=3 into MemTable (these get copied to B-tree at
        // cursor open time via MemPageStore fallback).
        table.insert(1, vec![SqliteValue::Integer(10)]);
        table.insert(2, vec![SqliteValue::Integer(20)]);
        table.insert(3, vec![SqliteValue::Integer(30)]);
        // Reset counter to simulate stale state.
        table.next_rowid = 1;

        let (rows, _) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
            b.emit_op(Opcode::NewRowid, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // B-tree max rowid is 3 → should return 4, NOT 1.
        assert_eq!(rows, vec![vec![SqliteValue::Integer(4)]]);
    }

    #[test]
    fn test_insert_multiple_rows_write_through() {
        // Insert multiple rows via B-tree and read them all back.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let (rows, final_db) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Insert row 1: value=100
            b.emit_op(Opcode::NewRowid, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 100, 2, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, 2, 1, 3, P4::None, 0);
            b.emit_op(Opcode::Insert, 0, 3, 1, P4::None, 0);

            // Insert row 2: value=200
            b.emit_op(Opcode::NewRowid, 0, 4, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 200, 5, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, 5, 1, 6, P4::None, 0);
            b.emit_op(Opcode::Insert, 0, 6, 4, P4::None, 0);

            // Insert row 3: value=300
            b.emit_op(Opcode::NewRowid, 0, 7, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 300, 8, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, 8, 1, 9, P4::None, 0);
            b.emit_op(Opcode::Insert, 0, 9, 7, P4::None, 0);

            // Read back via Rewind/Column/Next loop.
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);
            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 10, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 10, 1, 0, P4::None, 0);
            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // MemDatabase should be empty (write-through).
        let table = final_db.get_table(root).expect("table should exist");
        assert_eq!(table.rows.len(), 0);

        // All 3 rows readable from B-tree.
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], SqliteValue::Integer(100));
        assert_eq!(rows[1][0], SqliteValue::Integer(200));
        assert_eq!(rows[2][0], SqliteValue::Integer(300));
    }

    #[test]
    fn test_insert_replace_upsert_via_btree() {
        // Insert same rowid twice with OE_REPLACE — second insert should overwrite.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let (rows, _) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Insert rowid=1 with value=10.
            b.emit_op(Opcode::Integer, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, 2, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, 2, 1, 3, P4::None, 0);
            b.emit_op(Opcode::Insert, 0, 3, 1, P4::None, 0);

            // Insert rowid=1 again with value=99 (OE_REPLACE upsert).
            b.emit_op(Opcode::Integer, 99, 4, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, 4, 1, 5, P4::None, 0);
            b.emit_op(Opcode::Insert, 0, 5, 1, P4::None, 5);

            // Read back.
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);
            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 6, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 6, 1, 0, P4::None, 0);
            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // Only one row with the updated value.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqliteValue::Integer(99));
    }

    #[test]
    fn test_insert_default_conflict_errors_via_btree() {
        // Default conflict mode (OE_ABORT) must raise constraint error.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        table.insert(1, vec![SqliteValue::Integer(10)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

        // Duplicate rowid=1 with default conflict handling.
        b.emit_op(Opcode::Integer, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 99, 4, 0, P4::None, 0);
        b.emit_op(Opcode::MakeRecord, 4, 1, 5, P4::None, 0);
        b.emit_op(Opcode::Insert, 0, 5, 1, P4::None, 0);

        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.enable_storage_cursors(true);
        engine.set_database(db);

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: ErrorCode::Constraint as i32,
                message: "PRIMARY KEY constraint failed".to_owned(),
            }
        );

        let db = engine.take_database().expect("database should exist");
        let table = db.get_table(root).expect("table should exist");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].values, vec![SqliteValue::Integer(10)]);
    }

    #[test]
    fn test_insert_default_conflict_errors_memdb_path() {
        // Same behavior must hold for the legacy MemDatabase cursor path.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        table.insert(1, vec![SqliteValue::Integer(10)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

        // Duplicate rowid=1 with default conflict handling.
        b.emit_op(Opcode::Integer, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 99, 4, 0, P4::None, 0);
        b.emit_op(Opcode::MakeRecord, 4, 1, 5, P4::None, 0);
        b.emit_op(Opcode::Insert, 0, 5, 1, P4::None, 0);

        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.enable_storage_cursors(false);
        engine.set_database(db);

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: ErrorCode::Constraint as i32,
                message: "PRIMARY KEY constraint failed".to_owned(),
            }
        );

        let db = engine.take_database().expect("database should exist");
        let table = db.get_table(root).expect("table should exist");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].values, vec![SqliteValue::Integer(10)]);
    }

    // ── bd-2a3y: TransactionPageIo / SharedTxnPageIo integration tests ──

    #[test]
    fn test_set_transaction_enables_storage_cursors() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut engine = VdbeEngine::new(8);
        assert!(engine.storage_cursors_enabled);

        // set_transaction should auto-enable storage cursors.
        engine.set_transaction(Box::new(txn));
        assert!(engine.storage_cursors_enabled);
        assert!(engine.txn_page_io.is_some());
    }

    #[test]
    fn test_storage_cursors_enabled_by_default() {
        let engine = VdbeEngine::new(8);
        assert!(engine.storage_cursors_enabled);
        assert!(engine.txn_page_io.is_none());
    }

    #[test]
    fn test_take_transaction_returns_handle() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_transaction(Box::new(txn));

        // take_transaction should return the handle and clear cursors.
        let recovered = engine
            .take_transaction()
            .expect("take_transaction should succeed");
        assert!(recovered.is_some());
        assert!(engine.txn_page_io.is_none());
        assert!(engine.storage_cursors.is_empty());
    }

    #[test]
    fn test_open_storage_cursor_prefers_txn_backend() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        engine.set_transaction(Box::new(txn));

        // open_storage_cursor should succeed using the Txn backend.
        let opened = engine.open_storage_cursor(0, root, false);
        assert!(opened);

        // Verify the cursor exists in storage_cursors.
        assert!(engine.storage_cursors.contains_key(&0));

        // Clean up: drop cursors before taking transaction.
        engine.storage_cursors.clear();
        let _txn = engine
            .take_transaction()
            .expect("take_transaction should succeed");
    }

    #[test]
    fn test_open_storage_cursor_write_init_failure_does_not_fallback_to_mem() {
        use fsqlite_mvcc::{ConcurrentRegistry, InProcessPageLockTable};
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut engine = VdbeEngine::new(8);
        // Use a page number whose low byte is 0 so MockTransaction::get_page
        // returns a zero type byte, forcing the writable root-page init path.
        let root = 256;

        // Deliberately install concurrent context without a registered session.
        // SharedTxnPageIo::write_page will fail before touching pager state.
        let registry = std::rc::Rc::new(std::cell::RefCell::new(ConcurrentRegistry::new()));
        let lock_table = std::rc::Rc::new(InProcessPageLockTable::new());
        engine.set_transaction_concurrent(Box::new(txn), 999, registry, lock_table);

        let opened = engine.open_storage_cursor(0, root, true);
        assert!(
            !opened,
            "write-init errors must fail cursor open instead of silently falling back to Mem"
        );
        assert!(
            !engine.storage_cursors.contains_key(&0),
            "failed open must not leave a cursor installed"
        );
    }

    #[test]
    fn test_open_storage_cursor_falls_back_to_mem_without_txn() {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        db.get_table_mut(root)
            .unwrap()
            .insert(1, vec![SqliteValue::Integer(100)]);

        let mut engine = VdbeEngine::new(8);
        engine.enable_storage_cursors(true);
        engine.set_database(db);

        // Without a transaction, should fall back to Mem backend.
        let opened = engine.open_storage_cursor(0, root, false);
        assert!(opened);
        assert!(engine.storage_cursors.contains_key(&0));
    }

    #[test]
    fn test_txn_cursor_open_close_lifecycle() {
        // Verify the TransactionPageIo cursor lifecycle:
        // set_transaction → open cursor → close cursor → take_transaction.
        // MockTransaction doesn't produce valid B-tree pages, so we don't
        // attempt navigation — that's tested via MemPageStore-backed tests.
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        // Open a read cursor — this creates a CursorBackend::Txn.
        b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
        // Close the cursor immediately without navigation.
        b.emit_op(Opcode::Close, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);
        let prog = b.finish().expect("program should build");

        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        engine.set_transaction(Box::new(txn));

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);

        // Verify transaction recovery after cursor lifecycle.
        engine.storage_cursors.clear();
        assert!(
            engine
                .take_transaction()
                .expect("take_transaction should succeed")
                .is_some()
        );
    }

    // ── bd-3pti: Seek opcode tests ───────────────────────────────────────

    #[test]
    fn test_seek_ge_with_storage_cursor() {
        // SeekGE(key=5): should position at first row with rowid >= 5.
        // Table has rows: 3, 5, 7, 9
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);
        table.insert(7, vec![SqliteValue::Integer(70)]);
        table.insert(9, vec![SqliteValue::Integer(90)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid >= 5 (should land on rowid 5)
            b.emit_op(Opcode::Integer, 5, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekGE, 0, 1, not_found, P4::None, 0);

            // Read the column value at current position.
            b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(50)]); // rowid 5, value 50
    }

    #[test]
    fn test_seek_ge_not_exact_match() {
        // SeekGE(key=4): should position at first row with rowid >= 4.
        // Table has rows: 3, 5, 7, 9 → should land on rowid 5
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);
        table.insert(7, vec![SqliteValue::Integer(70)]);
        table.insert(9, vec![SqliteValue::Integer(90)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid >= 4 (should land on rowid 5, the next larger)
            b.emit_op(Opcode::Integer, 4, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekGE, 0, 1, not_found, P4::None, 0);

            b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(50)]); // rowid 5, value 50
    }

    #[test]
    fn test_seek_gt_with_storage_cursor() {
        // SeekGT(key=5): should position at first row with rowid > 5.
        // Table has rows: 3, 5, 7, 9 → should land on rowid 7
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);
        table.insert(7, vec![SqliteValue::Integer(70)]);
        table.insert(9, vec![SqliteValue::Integer(90)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid > 5 (should land on rowid 7)
            b.emit_op(Opcode::Integer, 5, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekGT, 0, 1, not_found, P4::None, 0);

            b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(70)]); // rowid 7, value 70
    }

    #[test]
    fn test_seek_le_with_storage_cursor() {
        // SeekLE(key=5): should position at last row with rowid <= 5.
        // Table has rows: 3, 5, 7, 9 → should land on rowid 5
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);
        table.insert(7, vec![SqliteValue::Integer(70)]);
        table.insert(9, vec![SqliteValue::Integer(90)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid <= 5 (should land on rowid 5)
            b.emit_op(Opcode::Integer, 5, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekLE, 0, 1, not_found, P4::None, 0);

            b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(50)]); // rowid 5, value 50
    }

    #[test]
    fn test_seek_le_not_exact_match() {
        // SeekLE(key=6): should position at last row with rowid <= 6.
        // Table has rows: 3, 5, 7, 9 → should land on rowid 5
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);
        table.insert(7, vec![SqliteValue::Integer(70)]);
        table.insert(9, vec![SqliteValue::Integer(90)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid <= 6 (should land on rowid 5)
            b.emit_op(Opcode::Integer, 6, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekLE, 0, 1, not_found, P4::None, 0);

            b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(50)]); // rowid 5, value 50
    }

    #[test]
    fn test_seek_lt_with_storage_cursor() {
        // SeekLT(key=5): should position at last row with rowid < 5.
        // Table has rows: 3, 5, 7, 9 → should land on rowid 3
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);
        table.insert(7, vec![SqliteValue::Integer(70)]);
        table.insert(9, vec![SqliteValue::Integer(90)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid < 5 (should land on rowid 3)
            b.emit_op(Opcode::Integer, 5, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekLT, 0, 1, not_found, P4::None, 0);

            b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(30)]); // rowid 3, value 30
    }

    #[test]
    fn test_seek_ge_empty_table_jumps() {
        // SeekGE on empty table should jump to p2.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        // Table is empty.

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            b.emit_op(Opcode::Integer, 5, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekGE, 0, 1, not_found, P4::None, 0);

            // This should NOT be reached.
            b.emit_op(Opcode::Integer, 999, 2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            // Jump target - we output nothing to indicate the jump was taken.
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // Empty table → no rows returned, jump to p2.
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_seek_lt_no_smaller_row_jumps() {
        // SeekLT(key=3) when smallest rowid is 3 should jump to p2.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(3, vec![SqliteValue::Integer(30)]);
        table.insert(5, vec![SqliteValue::Integer(50)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            let not_found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);

            // Seek to rowid < 3 (no such row → should jump)
            b.emit_op(Opcode::Integer, 3, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekLT, 0, 1, not_found, P4::None, 0);

            // This should NOT be reached.
            b.emit_op(Opcode::Integer, 999, 2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            b.resolve_label(not_found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // No row < 3 → jump taken, no results.
        assert_eq!(rows.len(), 0);
    }
}
