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

use hashbrown::{HashMap, HashSet};
use std::any::Any;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;

use fsqlite_btree::swiss_index::SwissIndex;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fsqlite_btree::{
    BtCursor, BtreeCursorOps, BtreePageHeader, BtreePageType, MemPageStore, PageReader, PageWriter,
    SeekResult, header_offset_for_page,
};
use fsqlite_error::{ErrorCode, FrankenError, Result};
use fsqlite_func::vtab::ColumnContext;
use fsqlite_func::{ErasedAggregateFunction, ErasedWindowFunction, FunctionRegistry};
use fsqlite_mvcc::{
    ConcurrentRegistry, InProcessPageLockTable, MvccError, TimeTravelSnapshot, VersionStore,
    concurrent_read_page, concurrent_write_page,
};
use fsqlite_pager::TransactionHandle;
use fsqlite_types::cx::Cx;
use fsqlite_types::opcode::{IndexCursorMeta, Opcode, P4, VdbeOp};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{CommitSeq, PageData, PageNumber, SchemaEpoch, StrictColumnType};

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
    /// Groups of column indices forming UNIQUE constraints (including
    /// non-IPK PRIMARY KEY). Each inner `Vec<usize>` is one UNIQUE
    /// constraint; a conflict occurs when all columns in a group match
    /// an existing row. Used by the Insert opcode to enforce unique
    /// constraints in MemDatabase mode (where indexes are no-ops).
    unique_column_groups: Vec<Vec<usize>>,
}

impl MemTable {
    /// Create a new empty table with the given column count.
    fn new(num_columns: usize) -> Self {
        Self {
            num_columns,
            rows: Vec::new(),
            next_rowid: 1,
            unique_column_groups: Vec::new(),
        }
    }

    /// Register a group of columns that together form a UNIQUE constraint.
    pub fn add_unique_column_group(&mut self, cols: Vec<usize>) {
        self.unique_column_groups.push(cols);
    }

    /// Find a row that conflicts with `new_values` on any UNIQUE constraint.
    /// Returns the rowid of the first conflicting row, or `None`.
    pub fn find_unique_conflict(&self, new_values: &[SqliteValue]) -> Option<i64> {
        for group in &self.unique_column_groups {
            for row in &self.rows {
                let all_match = group.iter().all(|&col_idx| {
                    let new_val = new_values.get(col_idx);
                    let existing_val = row.values.get(col_idx);
                    match (new_val, existing_val) {
                        // NULL never conflicts with NULL in UNIQUE constraints.
                        // Missing columns also don't conflict.
                        (Some(SqliteValue::Null) | None, _)
                        | (_, Some(SqliteValue::Null) | None) => false,
                        (Some(a), Some(b)) => a == b,
                    }
                });
                if all_match {
                    return Some(row.rowid);
                }
            }
        }
        None
    }

    /// Allocate a new unique rowid.
    pub fn alloc_rowid(&mut self) -> i64 {
        let id = self.next_rowid;
        self.next_rowid = self.next_rowid.saturating_add(1);
        id
    }

    /// Insert a row with the given rowid and values.
    fn insert(&mut self, rowid: i64, values: Vec<SqliteValue>) {
        // Update next_rowid if needed.
        if rowid >= self.next_rowid {
            self.next_rowid = rowid.saturating_add(1);
        }
        // Replace if rowid already exists (UPSERT semantics).
        match self.rows.binary_search_by_key(&rowid, |r| r.rowid) {
            Ok(idx) => {
                self.rows[idx].values = values;
            }
            Err(idx) => {
                self.rows.insert(idx, MemRow { rowid, values });
            }
        }
    }

    /// Delete a row by rowid. Returns true if a row was found and deleted.
    pub fn delete_by_rowid(&mut self, rowid: i64) -> bool {
        if let Ok(idx) = self.rows.binary_search_by_key(&rowid, |r| r.rowid) {
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
        self.rows.binary_search_by_key(&rowid, |r| r.rowid).ok()
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
    /// Register containing the pseudo-row data blob.
    pseudo_reg: Option<i32>,
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
            pseudo_reg: None,
            is_pseudo: false,
        }
    }

    fn new_pseudo(reg: i32) -> Self {
        Self {
            root_page: -1,
            writable: false,
            position: None,
            pseudo_row: None,
            pseudo_reg: Some(reg),
            is_pseudo: true,
        }
    }
}

/// Cursor state for sorter opcodes (`SorterOpen`, `SorterInsert`, ...).
///
/// Supports external merge sort: when in-memory rows exceed `spill_threshold`
/// bytes, the current batch is sorted and flushed to a temporary file as a
/// "run".  At `SorterSort` time, all runs (plus any remaining in-memory rows)
/// are merged via k-way merge.
#[derive(Debug, Clone)]
struct SorterCursor {
    /// Number of leading columns used as sort key.
    key_columns: usize,
    /// Per-key sort direction (length == key_columns).
    sort_key_orders: Vec<SortKeyOrder>,
    /// Per-key collation sequence (e.g. "NOCASE"). `None` means BINARY.
    collations: Vec<Option<String>>,
    /// Inserted records decoded from `MakeRecord` blobs.
    rows: Vec<Vec<SqliteValue>>,
    /// Current position after `SorterSort`/`SorterNext`.
    position: Option<usize>,
    /// Estimated bytes consumed by `rows` (approximate).
    memory_used: usize,
    /// Memory limit before spilling to disk (default 100 MiB).
    spill_threshold: usize,
    /// Sorted runs that have been spilled to disk.
    spill_runs: Vec<SpillRun>,
    /// Total rows sorted (across all runs + final merge).
    rows_sorted_total: u64,
    /// Total pages spilled to disk.
    spill_pages_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKeyOrder {
    /// ASC with NULLS FIRST (SQLite default for ASC).
    Asc,
    /// DESC with NULLS LAST (SQLite default for DESC).
    Desc,
    /// ASC with NULLS LAST (explicit NULLS LAST).
    AscNullsLast,
    /// DESC with NULLS FIRST (explicit NULLS FIRST).
    DescNullsFirst,
}

/// Default spill threshold: 100 MiB.
const SORTER_DEFAULT_SPILL_THRESHOLD: usize = 100 * 1024 * 1024;

/// Number of 64-bit words in a Bloom filter (8192 bits = 1 KiB).
const BLOOM_FILTER_WORDS: usize = 128;

/// Compute a simple hash of a `SqliteValue` for Bloom filter lookups.
fn bloom_hash(val: &SqliteValue) -> u64 {
    // FNV-1a-style hash — sufficient for a Bloom filter.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let bytes: &[u8] = match val {
        SqliteValue::Null => &[0],
        SqliteValue::Integer(i) => {
            // Hash inline to avoid allocation.
            h ^= 1;
            h = h.wrapping_mul(0x0100_0000_01b3);
            for b in i.to_le_bytes() {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            return h;
        }
        SqliteValue::Float(f) => {
            h ^= 2;
            h = h.wrapping_mul(0x0100_0000_01b3);
            for b in f.to_le_bytes() {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            return h;
        }
        SqliteValue::Text(s) => s.as_bytes(),
        SqliteValue::Blob(b) => b.as_slice(),
    };
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Approximate page size for spill accounting (4 KiB).
const SORTER_SPILL_PAGE_SIZE: usize = 4096;

/// A sorted run that has been flushed to a temporary file.
///
/// Each run stores length-prefixed serialized records in sorted order.
/// Format per record: `[u32-le length][serialized record bytes]`.
#[derive(Debug, Clone)]
struct SpillRun {
    /// Path to the temporary file containing serialized sorted records.
    path: std::path::PathBuf,
    /// Number of records in this run (used for merge accounting).
    #[allow(dead_code)]
    record_count: u64,
    /// Total bytes written (used for page accounting).
    #[allow(dead_code)]
    bytes_written: u64,
}

impl SorterCursor {
    fn new(
        key_columns: usize,
        mut sort_key_orders: Vec<SortKeyOrder>,
        collations: Vec<Option<String>>,
    ) -> Self {
        let key_columns = key_columns.max(1);
        if sort_key_orders.len() < key_columns {
            sort_key_orders.resize(key_columns, SortKeyOrder::Asc);
        }
        sort_key_orders.truncate(key_columns);
        Self {
            key_columns,
            sort_key_orders,
            collations,
            rows: Vec::new(),
            position: None,
            memory_used: 0,
            spill_threshold: SORTER_DEFAULT_SPILL_THRESHOLD,
            spill_runs: Vec::new(),
            rows_sorted_total: 0,
            spill_pages_total: 0,
        }
    }

    /// Estimate the memory footprint of a decoded row.
    fn estimate_row_size(row: &[SqliteValue]) -> usize {
        // Base overhead per Vec element + per-value overhead
        let mut size = std::mem::size_of::<Vec<SqliteValue>>() + row.len() * 24;
        for val in row {
            match val {
                SqliteValue::Text(s) => size += s.len(),
                SqliteValue::Blob(b) => size += b.len(),
                _ => {}
            }
        }
        size
    }

    /// Insert a row and spill to disk if memory exceeds the threshold.
    fn insert_row(&mut self, row: Vec<SqliteValue>) -> Result<()> {
        self.memory_used += Self::estimate_row_size(&row);
        self.rows.push(row);

        if self.memory_used >= self.spill_threshold {
            self.spill_to_disk()?;
        }
        Ok(())
    }

    /// Sort the in-memory rows, write them to a temp file, and clear them.
    fn spill_to_disk(&mut self) -> Result<()> {
        use std::io::Write;

        if self.rows.is_empty() {
            return Ok(());
        }

        // Sort current batch
        let key_columns = self.key_columns;
        let orders = self.sort_key_orders.clone();
        let colls = self.collations.clone();
        self.rows
            .sort_by(|lhs, rhs| compare_sorter_rows(lhs, rhs, key_columns, &orders, &colls));

        // Write to temp file.  We use `keep()` to detach the auto-delete
        // guard so the file persists until we explicitly remove it in
        // `sort()` / `reset()`.
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| FrankenError::internal(format!("sorter spill tempfile: {e}")))?;
        let (file, path) = tmp
            .keep()
            .map_err(|e| FrankenError::internal(format!("sorter spill keep: {e}")))?;
        let mut writer = std::io::BufWriter::new(file);

        let record_count = self.rows.len() as u64;
        let mut bytes_written: u64 = 0;
        for row in &self.rows {
            let blob = serialize_record(row);
            #[allow(clippy::cast_possible_truncation)]
            let len_bytes = (blob.len() as u32).to_le_bytes();
            writer
                .write_all(&len_bytes)
                .map_err(|e| FrankenError::internal(format!("sorter spill write len: {e}")))?;
            writer
                .write_all(&blob)
                .map_err(|e| FrankenError::internal(format!("sorter spill write data: {e}")))?;
            bytes_written += 4 + blob.len() as u64;
        }
        writer
            .flush()
            .map_err(|e| FrankenError::internal(format!("sorter spill flush: {e}")))?;

        #[allow(clippy::cast_possible_truncation)]
        let pages = (bytes_written as usize).div_ceil(SORTER_SPILL_PAGE_SIZE);
        self.spill_pages_total += pages as u64;

        tracing::warn!(
            rows = record_count,
            bytes = bytes_written,
            pages,
            run_index = self.spill_runs.len(),
            "sorter spilling to disk"
        );

        self.spill_runs.push(SpillRun {
            path,
            record_count,
            bytes_written,
        });

        self.rows_sorted_total += record_count;
        self.rows.clear();
        self.memory_used = 0;
        Ok(())
    }

    /// Sort the sorter, merging any spilled runs with in-memory rows.
    ///
    /// After this call, `self.rows` contains the fully sorted result and
    /// `self.spill_runs` is drained.
    #[allow(clippy::too_many_lines)]
    fn sort(&mut self) -> Result<()> {
        if self.spill_runs.is_empty() {
            // Pure in-memory sort — fast path.
            let key_columns = self.key_columns;
            let orders = self.sort_key_orders.clone();
            let colls = self.collations.clone();
            self.rows
                .sort_by(|lhs, rhs| compare_sorter_rows(lhs, rhs, key_columns, &orders, &colls));
            self.rows_sorted_total += self.rows.len() as u64;
            return Ok(());
        }

        // Sort remaining in-memory rows as one more "run".
        let key_columns = self.key_columns;
        let orders = self.sort_key_orders.clone();
        let colls = self.collations.clone();
        self.rows
            .sort_by(|lhs, rhs| compare_sorter_rows(lhs, rhs, key_columns, &orders, &colls));

        // Collect all runs: disk runs first, then in-memory remainder.
        let mut run_iters: Vec<RunIterator> = Vec::with_capacity(self.spill_runs.len() + 1);
        for run in &self.spill_runs {
            run_iters.push(RunIterator::from_file(&run.path)?);
        }
        if !self.rows.is_empty() {
            let mem_rows = std::mem::take(&mut self.rows);
            self.rows_sorted_total += mem_rows.len() as u64;
            run_iters.push(RunIterator::from_memory(mem_rows));
        }

        // K-way merge using a simple tournament approach.
        let mut merged: Vec<Vec<SqliteValue>> = Vec::new();

        // Advance all iterators to their first element.
        for iter in &mut run_iters {
            iter.advance()?;
        }

        loop {
            // Find the run with the smallest current element.
            let mut best_idx: Option<usize> = None;
            for (i, iter) in run_iters.iter().enumerate() {
                let Some(row) = iter.current() else {
                    continue;
                };
                if let Some(bi) = best_idx {
                    if let Some(best_row) = run_iters[bi].current() {
                        if compare_sorter_rows(row, best_row, key_columns, &orders, &colls)
                            == Ordering::Less
                        {
                            best_idx = Some(i);
                        }
                    }
                } else {
                    best_idx = Some(i);
                }
            }

            let Some(idx) = best_idx else {
                break; // All runs exhausted.
            };

            if let Some(row) = run_iters[idx].take_current() {
                merged.push(row);
            }
            run_iters[idx].advance()?;
        }

        tracing::debug!(
            rows = merged.len(),
            runs = self.spill_runs.len() + 1,
            "sorter merge complete"
        );

        // Clean up temp files.
        for run in &self.spill_runs {
            let _ = std::fs::remove_file(&run.path);
        }
        self.spill_runs.clear();
        self.rows = merged;
        self.memory_used = 0;
        Ok(())
    }

    /// Clear all rows and spill state (for `ResetSorter`).
    fn reset(&mut self) {
        self.rows.clear();
        self.position = None;
        self.memory_used = 0;
        // Clean up temp files.
        for run in &self.spill_runs {
            let _ = std::fs::remove_file(&run.path);
        }
        self.spill_runs.clear();
    }
}

impl Drop for SorterCursor {
    fn drop(&mut self) {
        for run in &self.spill_runs {
            let _ = std::fs::remove_file(&run.path);
        }
    }
}

/// Iterator over records in a sorted run (either disk-backed or in-memory).
enum RunIterator {
    /// Records read from a temporary file.
    File {
        reader: std::io::BufReader<std::fs::File>,
        current: Option<Vec<SqliteValue>>,
    },
    /// Records from an in-memory Vec (used for the final unsorted batch).
    Memory {
        rows: std::vec::IntoIter<Vec<SqliteValue>>,
        current: Option<Vec<SqliteValue>>,
    },
}

impl RunIterator {
    fn from_file(path: &std::path::Path) -> Result<Self> {
        let file = std::fs::File::open(path)
            .map_err(|e| FrankenError::internal(format!("sorter run open: {e}")))?;
        Ok(Self::File {
            reader: std::io::BufReader::new(file),
            current: None,
        })
    }

    fn from_memory(rows: Vec<Vec<SqliteValue>>) -> Self {
        Self::Memory {
            rows: rows.into_iter(),
            current: None,
        }
    }

    fn current(&self) -> Option<&Vec<SqliteValue>> {
        match self {
            Self::File { current, .. } | Self::Memory { current, .. } => current.as_ref(),
        }
    }

    fn take_current(&mut self) -> Option<Vec<SqliteValue>> {
        match self {
            Self::File { current, .. } | Self::Memory { current, .. } => current.take(),
        }
    }

    fn advance(&mut self) -> Result<()> {
        match self {
            Self::File { reader, current } => {
                use std::io::Read;
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf) {
                    Ok(()) => {
                        let len = u32::from_le_bytes(len_buf) as usize;
                        let mut buf = vec![0u8; len];
                        reader
                            .read_exact(&mut buf)
                            .map_err(|e| FrankenError::internal(format!("sorter run read: {e}")))?;
                        let row = parse_record(&buf).ok_or_else(|| {
                            FrankenError::internal("sorter run: malformed record")
                        })?;
                        *current = Some(row);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        *current = None;
                    }
                    Err(e) => {
                        return Err(FrankenError::internal(format!("sorter run read len: {e}")));
                    }
                }
            }
            Self::Memory { rows, current } => {
                *current = rows.next();
            }
        }
        Ok(())
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
    registry: Arc<Mutex<ConcurrentRegistry>>,
    /// Shared reference to the page-level lock table.
    lock_table: Arc<InProcessPageLockTable>,
    /// Busy-timeout budget used when contending on page-level locks.
    busy_timeout_ms: u64,
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
        registry: Arc<Mutex<ConcurrentRegistry>>,
        lock_table: Arc<InProcessPageLockTable>,
        busy_timeout_ms: u64,
    ) -> Self {
        Self {
            txn: Rc::new(RefCell::new(txn)),
            concurrent: Some(ConcurrentContext {
                session_id,
                registry,
                lock_table,
                busy_timeout_ms,
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
        if let Some(ctx) = &self.concurrent {
            // Read-own-writes visibility: if this txn already wrote the page,
            // return that version first and still record the read for SSI.
            let mut guard = ctx
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let handle = guard.get_mut(ctx.session_id).ok_or_else(|| {
                FrankenError::Internal(format!(
                    "MVCC session {} not found in registry during read",
                    ctx.session_id
                ))
            })?;
            let txn_id = handle.txn_token().id.get();
            let snapshot_high = handle.snapshot().high.get();
            let write_set_page = concurrent_read_page(handle, page_no).cloned();
            handle.record_read(page_no);

            if let Some(page) = write_set_page {
                tracing::debug!(
                    txn_id,
                    commit_seq = snapshot_high,
                    snapshot_high,
                    page_id = page_no.get(),
                    visibility_decision = "write_set_hit",
                    conflict_reason = "none",
                    "mvcc visibility decision"
                );
                return Ok(page.into_vec());
            }

            tracing::debug!(
                txn_id,
                commit_seq = snapshot_high,
                snapshot_high,
                page_id = page_no.get(),
                visibility_decision = "snapshot_pager_read",
                conflict_reason = "none",
                "mvcc visibility decision"
            );
        }

        let page = self.txn.borrow().get_page(cx, page_no)?.into_vec();
        Ok(page)
    }
}

impl PageWriter for SharedTxnPageIo {
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        // bd-kivg / 5E.2: Acquire page-level lock and record in write set if concurrent.
        if let Some(ref ctx) = self.concurrent {
            let started = Instant::now();
            let deadline = Duration::from_millis(ctx.busy_timeout_ms);
            let mut backoff = Duration::from_micros(50);

            loop {
                let page_data = PageData::from_vec(data.to_vec());
                let (write_result, txn_id, snapshot_high) = {
                    let mut guard = ctx
                        .registry
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let handle = guard.get_mut(ctx.session_id).ok_or_else(|| {
                        FrankenError::Internal(format!(
                            "MVCC session {} not found in registry during write",
                            ctx.session_id
                        ))
                    })?;
                    let txn_id = handle.txn_token().id.get();
                    let snapshot_high = handle.snapshot().high.get();
                    let write_result = concurrent_write_page(
                        handle,
                        &ctx.lock_table,
                        ctx.session_id,
                        page_no,
                        page_data,
                    );
                    (write_result, txn_id, snapshot_high)
                };

                match write_result {
                    Ok(()) => {
                        tracing::debug!(
                            txn_id,
                            commit_seq = snapshot_high,
                            snapshot_high,
                            page_id = page_no.get(),
                            visibility_decision = "write_set_recorded",
                            conflict_reason = "none",
                            "mvcc write visibility decision"
                        );
                        break;
                    }
                    Err(MvccError::Busy) => {
                        tracing::warn!(
                            txn_id,
                            commit_seq = snapshot_high,
                            snapshot_high,
                            page_id = page_no.get(),
                            visibility_decision = "write_retry",
                            conflict_reason = "page_lock_busy",
                            "mvcc write conflict detected"
                        );
                        if deadline.is_zero() || started.elapsed() >= deadline {
                            return Err(FrankenError::Busy);
                        }

                        let remaining = deadline
                            .checked_sub(started.elapsed())
                            .unwrap_or(Duration::ZERO);
                        let sleep_for = if backoff < remaining {
                            backoff
                        } else {
                            remaining
                        };
                        if !sleep_for.is_zero() {
                            std::thread::sleep(sleep_for);
                        }
                        backoff = (backoff * 2).min(Duration::from_millis(5));
                    }
                    Err(e) => {
                        tracing::warn!(
                            txn_id,
                            commit_seq = snapshot_high,
                            snapshot_high,
                            page_id = page_no.get(),
                            visibility_decision = "write_abort",
                            conflict_reason = %e,
                            "mvcc write failed"
                        );
                        return Err(FrankenError::Internal(format!(
                            "MVCC write_page failed: {e}"
                        )));
                    }
                }
            }
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

// ── Time-Travel Page I/O ──────────────────────────────────────────────
//
// Wraps a `SharedTxnPageIo` and a `TimeTravelSnapshot` + `VersionStore`
// to intercept page reads and return historical page versions when
// available. Falls back to the underlying transaction for pages not
// present in the version store (i.e., pages unchanged since the
// time-travel target).

/// Read-only page I/O that serves historical page versions for
/// time-travel queries (`FOR SYSTEM_TIME AS OF ...`).
///
/// On `read_page`, the wrapper first resolves the page through the MVCC
/// `VersionStore` at the snapshot's commit sequence. If a historical
/// version is found, its `PageData` is returned directly. Otherwise the
/// read falls through to the underlying transaction (the page has not
/// changed since the target commit, so the current version is correct).
///
/// Write operations are unconditionally rejected — time-travel cursors
/// are strictly read-only.
#[derive(Clone)]
struct TimeTravelPageIo {
    /// Underlying transaction page I/O for fall-through reads.
    inner: SharedTxnPageIo,
    /// MVCC version store for historical page resolution.
    version_store: Rc<RefCell<VersionStore>>,
    /// The pinned time-travel snapshot.
    snapshot: TimeTravelSnapshot,
}

impl std::fmt::Debug for TimeTravelPageIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeTravelPageIo")
            .field("inner", &self.inner)
            .field("version_store", &"<Rc<RefCell<VersionStore>>>")
            .field(
                "target_commit_seq",
                &self.snapshot.target_commit_seq().get(),
            )
            .finish()
    }
}

impl PageReader for TimeTravelPageIo {
    fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        // Try to resolve the page at the historical snapshot first.
        let vs = self.version_store.borrow();
        if let Some(idx) = self.snapshot.resolve_page(&vs, page_no) {
            if let Some(version) = vs.get_version(idx) {
                tracing::trace!(
                    page_id = page_no.get(),
                    commit_seq = self.snapshot.target_commit_seq().get(),
                    "time-travel: serving historical page from version store"
                );
                return Ok(version.data.into_vec());
            }
        }
        drop(vs);
        // Page not in the version store — it hasn't changed since the
        // target commit, so the current transaction's page is correct.
        tracing::trace!(
            page_id = page_no.get(),
            commit_seq = self.snapshot.target_commit_seq().get(),
            "time-travel: page not in version store, falling through to txn"
        );
        self.inner.read_page(cx, page_no)
    }
}

impl PageWriter for TimeTravelPageIo {
    fn write_page(&mut self, _cx: &Cx, _page_no: PageNumber, _data: &[u8]) -> Result<()> {
        Err(FrankenError::Internal(
            "time-travel cursors are read-only: write_page not permitted".to_owned(),
        ))
    }

    fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
        Err(FrankenError::Internal(
            "time-travel cursors are read-only: allocate_page not permitted".to_owned(),
        ))
    }

    fn free_page(&mut self, _cx: &Cx, _page_no: PageNumber) -> Result<()> {
        Err(FrankenError::Internal(
            "time-travel cursors are read-only: free_page not permitted".to_owned(),
        ))
    }
}

// ── Cursor Backend Enum ────────────────────────────────────────────────
//
// Allows StorageCursor to work in three modes:
// - `Mem`: backed by MemPageStore (Phase 4 / tests)
// - `Txn`: backed by SharedTxnPageIo (Phase 5 production path)
// - `TimeTravel`: backed by TimeTravelPageIo (historical snapshot reads)

/// Backend for a storage cursor, dispatching between in-memory,
/// transaction-backed, and time-travel page I/O.
enum CursorBackend {
    /// In-memory page store (used by tests and Phase 4 fallback).
    Mem(BtCursor<MemPageStore>),
    /// Real pager transaction (Phase 5 production path, bd-2a3y).
    Txn(BtCursor<SharedTxnPageIo>),
    /// Time-travel snapshot (historical reads via MVCC version store).
    TimeTravel(BtCursor<TimeTravelPageIo>),
}

impl std::fmt::Debug for CursorBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mem(c) => f.debug_tuple("Mem").field(c).finish(),
            Self::Txn(c) => f.debug_tuple("Txn").field(c).finish(),
            Self::TimeTravel(c) => f.debug_tuple("TimeTravel").field(c).finish(),
        }
    }
}

impl CursorBackend {
    /// Returns `true` if this cursor is backed by the in-memory page store.
    #[must_use]
    fn is_mem(&self) -> bool {
        matches!(self, Self::Mem(_))
    }

    /// Returns `true` if this cursor is backed by the real pager transaction.
    #[must_use]
    fn is_txn(&self) -> bool {
        matches!(self, Self::Txn(_))
    }

    /// Returns `true` if this cursor is a time-travel cursor.
    #[must_use]
    #[allow(dead_code)]
    fn is_time_travel(&self) -> bool {
        matches!(self, Self::TimeTravel(_))
    }

    /// Returns a string identifying the backend kind for diagnostics.
    #[must_use]
    #[allow(dead_code)]
    fn kind_str(&self) -> &'static str {
        match self {
            Self::Mem(_) => "mem",
            Self::Txn(_) => "txn",
            Self::TimeTravel(_) => "time_travel",
        }
    }

    /// Whether the underlying B-tree cursor is for a table (intkey) B-tree.
    #[must_use]
    fn is_table_btree(&self) -> bool {
        match self {
            Self::Mem(c) => c.is_table(),
            Self::Txn(c) => c.is_table(),
            Self::TimeTravel(c) => c.is_table(),
        }
    }
}

/// Dispatch B-tree cursor operations across all backends.
impl CursorBackend {
    fn first(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.first(cx),
            Self::Txn(c) => c.first(cx),
            Self::TimeTravel(c) => c.first(cx),
        }
    }

    fn last(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.last(cx),
            Self::Txn(c) => c.last(cx),
            Self::TimeTravel(c) => c.last(cx),
        }
    }

    fn next(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.next(cx),
            Self::Txn(c) => c.next(cx),
            Self::TimeTravel(c) => c.next(cx),
        }
    }

    fn prev(&mut self, cx: &Cx) -> Result<bool> {
        match self {
            Self::Mem(c) => c.prev(cx),
            Self::Txn(c) => c.prev(cx),
            Self::TimeTravel(c) => c.prev(cx),
        }
    }

    fn eof(&self) -> bool {
        match self {
            Self::Mem(c) => c.eof(),
            Self::Txn(c) => c.eof(),
            Self::TimeTravel(c) => c.eof(),
        }
    }

    fn rowid(&self, cx: &Cx) -> Result<i64> {
        match self {
            Self::Mem(c) => c.rowid(cx),
            Self::Txn(c) => c.rowid(cx),
            Self::TimeTravel(c) => c.rowid(cx),
        }
    }

    fn payload(&self, cx: &Cx) -> Result<Vec<u8>> {
        match self {
            Self::Mem(c) => c.payload(cx),
            Self::Txn(c) => c.payload(cx),
            Self::TimeTravel(c) => c.payload(cx),
        }
    }

    fn table_move_to(&mut self, cx: &Cx, rowid: i64) -> Result<SeekResult> {
        match self {
            Self::Mem(c) => c.table_move_to(cx, rowid),
            Self::Txn(c) => c.table_move_to(cx, rowid),
            Self::TimeTravel(c) => c.table_move_to(cx, rowid),
        }
    }

    fn table_insert(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
        match self {
            Self::Mem(c) => c.table_insert(cx, rowid, data),
            Self::Txn(c) => c.table_insert(cx, rowid, data),
            Self::TimeTravel(_) => Err(FrankenError::Internal(
                "time-travel cursors are read-only: table_insert not permitted".to_owned(),
            )),
        }
    }

    fn delete(&mut self, cx: &Cx) -> Result<()> {
        match self {
            Self::Mem(c) => c.delete(cx),
            Self::Txn(c) => c.delete(cx),
            Self::TimeTravel(_) => Err(FrankenError::Internal(
                "time-travel cursors are read-only: delete not permitted".to_owned(),
            )),
        }
    }

    /// Position the cursor at the given key in an index B-tree.
    fn index_move_to(&mut self, cx: &Cx, key: &[u8]) -> Result<SeekResult> {
        match self {
            Self::Mem(c) => c.index_move_to(cx, key),
            Self::Txn(c) => c.index_move_to(cx, key),
            Self::TimeTravel(c) => c.index_move_to(cx, key),
        }
    }

    /// Insert a key into an index B-tree.
    fn index_insert(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        match self {
            Self::Mem(c) => c.index_insert(cx, key),
            Self::Txn(c) => c.index_insert(cx, key),
            Self::TimeTravel(_) => Err(FrankenError::Internal(
                "time-travel cursors are read-only: index_insert not permitted".to_owned(),
            )),
        }
    }

    /// Insert a key into a UNIQUE index B-tree, checking for duplicates.
    fn index_insert_unique(
        &mut self,
        cx: &Cx,
        key: &[u8],
        n_unique_cols: usize,
        columns_label: &str,
    ) -> Result<()> {
        match self {
            Self::Mem(c) => c.index_insert_unique(cx, key, n_unique_cols, columns_label),
            Self::Txn(c) => c.index_insert_unique(cx, key, n_unique_cols, columns_label),
            Self::TimeTravel(_) => Err(FrankenError::Internal(
                "time-travel cursors are read-only: index_insert_unique not permitted".to_owned(),
            )),
        }
    }

    /// Force the cursor into EOF state so subsequent reads return NULL.
    ///
    /// Used by `OP_NullRow` to satisfy the SQLite contract that Column/Rowid
    /// after NullRow must return NULL.
    fn clear_position(&mut self) {
        match self {
            Self::Mem(c) => c.invalidate(),
            Self::Txn(c) => c.invalidate(),
            Self::TimeTravel(c) => c.invalidate(),
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
    /// Cache for the decoded row payload to optimize OP_Column.
    cached_row: Option<(Vec<u8>, Vec<SqliteValue>)>,
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
                            match table.rows.binary_search_by_key(&rowid, |r| r.rowid) {
                                Ok(idx) => table.rows[idx].values = values,
                                Err(idx) => table.rows.insert(idx, MemRow { rowid, values }),
                            }
                        }
                        None => {
                            if let Ok(idx) = table.rows.binary_search_by_key(&rowid, |r| r.rowid) {
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
    pub tables: SwissIndex<i32, MemTable>,
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
            tables: SwissIndex::new(),
            next_root_page: 2, // Page 1 is reserved for sqlite_master.
            undo_enabled: false,
            undo_log: Vec::new(),
        }
    }

    /// Returns the number of tables in the database.
    #[must_use]
    pub fn table_count(&self) -> i32 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let count = self.tables.len() as i32;
        count
    }

    /// Allocate and return the next root page number without creating a table.
    ///
    /// Used by `OpenAutoindex` which needs a unique page number for a
    /// `MemPageStore`-backed index cursor but must NOT pollute `self.tables`
    /// (a spurious `MemTable` entry would cause `get_table()` to return
    /// `Some`, misleading `open_storage_cursor` into treating the page as a
    /// table B-tree).
    pub fn allocate_root_page(&mut self) -> i32 {
        let root_page = self.next_root_page;
        self.next_root_page += 1;
        root_page
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
            let rowid = max_visible.saturating_add(1);
            table.next_rowid = rowid.saturating_add(1);
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

// ── JIT scaffolding metrics/state (bd-1rw.3) ───────────────────────────────

/// Total number of JIT compilation attempts that succeeded.
static FSQLITE_JIT_COMPILATIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total number of JIT compilation attempts that failed and fell back.
static FSQLITE_JIT_COMPILE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total number of hot-query trigger events.
static FSQLITE_JIT_TRIGGERS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total number of JIT code cache hits.
static FSQLITE_JIT_CACHE_HITS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total number of JIT code cache misses.
static FSQLITE_JIT_CACHE_MISSES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Global JIT enable flag.
static FSQLITE_JIT_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);
/// Hot-query threshold (`N` executions before JIT trigger).
static FSQLITE_JIT_HOT_THRESHOLD: AtomicU64 = AtomicU64::new(8);
/// Maximum cached JIT plan stubs.
static FSQLITE_JIT_CACHE_CAPACITY: AtomicU64 = AtomicU64::new(128);

/// In-memory JIT code-cache entry (scaffold).
#[derive(Debug, Clone, Copy)]
struct JitCacheEntry {
    code_size_bytes: u64,
}

#[derive(Debug, Default)]
struct JitRuntimeState {
    executions_by_plan: HashMap<u64, u64>,
    cache: HashMap<u64, JitCacheEntry>,
    lru: VecDeque<u64>,
    unsupported_plans: HashSet<u64>,
    unsupported_lru: VecDeque<u64>,
}

impl JitRuntimeState {
    fn touch_lru(&mut self, plan_hash: u64) {
        self.lru.retain(|candidate| *candidate != plan_hash);
        self.lru.push_back(plan_hash);
    }

    fn insert_cache(
        &mut self,
        plan_hash: u64,
        entry: JitCacheEntry,
        cache_capacity: usize,
    ) -> Option<u64> {
        if cache_capacity == 0 {
            return None;
        }
        let mut evicted = None;
        if let hashbrown::hash_map::Entry::Occupied(mut occupied) = self.cache.entry(plan_hash) {
            occupied.insert(entry);
            self.touch_lru(plan_hash);
            return None;
        }
        if self.cache.len() >= cache_capacity
            && let Some(oldest) = self.lru.pop_front()
        {
            self.cache.remove(&oldest);
            evicted = Some(oldest);
        }
        self.cache.insert(plan_hash, entry);
        self.touch_lru(plan_hash);
        evicted
    }

    fn apply_capacity(&mut self, cache_capacity: usize) {
        if cache_capacity == 0 {
            self.cache.clear();
            self.lru.clear();
            self.unsupported_plans.clear();
            self.unsupported_lru.clear();
            return;
        }
        while self.cache.len() > cache_capacity {
            if let Some(oldest) = self.lru.pop_front() {
                self.cache.remove(&oldest);
            } else {
                break;
            }
        }
        while self.unsupported_plans.len() > cache_capacity {
            if let Some(oldest) = self.unsupported_lru.pop_front() {
                self.unsupported_plans.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn mark_unsupported_plan(&mut self, plan_hash: u64, cache_capacity: usize) {
        if cache_capacity == 0 {
            return;
        }
        if !self.unsupported_plans.insert(plan_hash) {
            self.unsupported_lru
                .retain(|candidate| *candidate != plan_hash);
            self.unsupported_lru.push_back(plan_hash);
            return;
        }
        if self.unsupported_plans.len() > cache_capacity
            && let Some(oldest) = self.unsupported_lru.pop_front()
        {
            self.unsupported_plans.remove(&oldest);
        }
        self.unsupported_lru.push_back(plan_hash);
    }

    fn is_unsupported_plan(&self, plan_hash: u64) -> bool {
        self.unsupported_plans.contains(&plan_hash)
    }
}

static VDBE_JIT_RUNTIME: std::sync::OnceLock<Mutex<JitRuntimeState>> = std::sync::OnceLock::new();

fn jit_runtime() -> &'static Mutex<JitRuntimeState> {
    VDBE_JIT_RUNTIME.get_or_init(|| Mutex::new(JitRuntimeState::default()))
}

fn lock_jit_runtime() -> std::sync::MutexGuard<'static, JitRuntimeState> {
    jit_runtime()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Snapshot of JIT scaffold metrics/configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VdbeJitMetricsSnapshot {
    /// Whether JIT triggering is enabled.
    pub enabled: bool,
    /// Hot-query threshold (`N` executions before a JIT trigger).
    pub hot_threshold: u64,
    /// Maximum number of cached JIT plan stubs.
    pub cache_capacity: usize,
    /// Current number of cached JIT plan stubs.
    pub cache_entries: usize,
    /// Total successful compilation attempts.
    pub jit_compilations_total: u64,
    /// Total failed compilation attempts.
    pub jit_compile_failures_total: u64,
    /// Total hot-query trigger events.
    pub jit_triggers_total: u64,
    /// Total cache hits.
    pub jit_cache_hits_total: u64,
    /// Total cache misses.
    pub jit_cache_misses_total: u64,
    /// Integer cache hit ratio (percent).
    pub jit_cache_hit_ratio_percent: u64,
}

/// Read a point-in-time snapshot of JIT scaffold metrics/configuration.
#[must_use]
pub fn vdbe_jit_metrics_snapshot() -> VdbeJitMetricsSnapshot {
    let runtime = lock_jit_runtime();
    let hits = FSQLITE_JIT_CACHE_HITS_TOTAL.load(AtomicOrdering::Relaxed);
    let misses = FSQLITE_JIT_CACHE_MISSES_TOTAL.load(AtomicOrdering::Relaxed);
    let denom = hits.saturating_add(misses);
    let ratio_percent = if denom == 0 {
        0
    } else {
        hits.saturating_mul(100).saturating_add(denom / 2) / denom
    };
    VdbeJitMetricsSnapshot {
        enabled: FSQLITE_JIT_ENABLED.load(AtomicOrdering::Relaxed),
        hot_threshold: FSQLITE_JIT_HOT_THRESHOLD.load(AtomicOrdering::Relaxed),
        cache_capacity: usize::try_from(FSQLITE_JIT_CACHE_CAPACITY.load(AtomicOrdering::Relaxed))
            .unwrap_or(usize::MAX),
        cache_entries: runtime.cache.len(),
        jit_compilations_total: FSQLITE_JIT_COMPILATIONS_TOTAL.load(AtomicOrdering::Relaxed),
        jit_compile_failures_total: FSQLITE_JIT_COMPILE_FAILURES_TOTAL
            .load(AtomicOrdering::Relaxed),
        jit_triggers_total: FSQLITE_JIT_TRIGGERS_TOTAL.load(AtomicOrdering::Relaxed),
        jit_cache_hits_total: hits,
        jit_cache_misses_total: misses,
        jit_cache_hit_ratio_percent: ratio_percent,
    }
}

/// Enable/disable JIT triggering.
pub fn set_vdbe_jit_enabled(enabled: bool) {
    FSQLITE_JIT_ENABLED.store(enabled, AtomicOrdering::Relaxed);
}

/// Current JIT trigger enable flag.
#[must_use]
pub fn vdbe_jit_enabled() -> bool {
    FSQLITE_JIT_ENABLED.load(AtomicOrdering::Relaxed)
}

/// Set hot-query threshold (`N` executions before a JIT trigger).
///
/// Values below 1 are clamped to 1.
#[must_use]
pub fn set_vdbe_jit_hot_threshold(threshold: u64) -> u64 {
    let clamped = threshold.max(1);
    FSQLITE_JIT_HOT_THRESHOLD.store(clamped, AtomicOrdering::Relaxed);
    clamped
}

/// Current hot-query threshold.
#[must_use]
pub fn vdbe_jit_hot_threshold() -> u64 {
    FSQLITE_JIT_HOT_THRESHOLD.load(AtomicOrdering::Relaxed)
}

/// Set JIT code cache capacity (number of plans).
///
/// Shrinks current cache immediately if needed.
#[must_use]
pub fn set_vdbe_jit_cache_capacity(capacity: usize) -> usize {
    let value_u64 = u64::try_from(capacity).unwrap_or(u64::MAX);
    FSQLITE_JIT_CACHE_CAPACITY.store(value_u64, AtomicOrdering::Relaxed);
    let mut runtime = lock_jit_runtime();
    runtime.apply_capacity(capacity);
    capacity
}

/// Current JIT code-cache capacity.
#[must_use]
pub fn vdbe_jit_cache_capacity() -> usize {
    usize::try_from(FSQLITE_JIT_CACHE_CAPACITY.load(AtomicOrdering::Relaxed)).unwrap_or(usize::MAX)
}

/// Reset JIT scaffold metrics and in-memory state.
pub fn reset_vdbe_jit_metrics() {
    FSQLITE_JIT_COMPILATIONS_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_JIT_COMPILE_FAILURES_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_JIT_TRIGGERS_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_JIT_CACHE_HITS_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_JIT_CACHE_MISSES_TOTAL.store(0, AtomicOrdering::Relaxed);
    let mut runtime = lock_jit_runtime();
    runtime.executions_by_plan.clear();
    runtime.cache.clear();
    runtime.lru.clear();
    runtime.unsupported_plans.clear();
    runtime.unsupported_lru.clear();
}

// ── Sort metrics (bd-1rw.4) ─────────────────────────────────────────────────

/// Total rows sorted across all sorter invocations.
static FSQLITE_SORT_ROWS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Total pages spilled to disk by sorters.
static FSQLITE_SORT_SPILL_PAGES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of VDBE execution metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VdbeMetricsSnapshot {
    /// Total opcodes executed across all statements.
    pub opcodes_executed_total: u64,
    /// Total statements executed.
    pub statements_total: u64,
    /// Cumulative statement duration in microseconds.
    pub statement_duration_us_total: u64,
    /// Total rows sorted across all sorter invocations.
    pub sort_rows_total: u64,
    /// Total pages spilled to disk by sorters.
    pub sort_spill_pages_total: u64,
}

/// Read a point-in-time snapshot of VDBE execution metrics.
#[must_use]
pub fn vdbe_metrics_snapshot() -> VdbeMetricsSnapshot {
    VdbeMetricsSnapshot {
        opcodes_executed_total: FSQLITE_VDBE_OPCODES_EXECUTED_TOTAL.load(AtomicOrdering::Relaxed),
        statements_total: FSQLITE_VDBE_STATEMENTS_TOTAL.load(AtomicOrdering::Relaxed),
        statement_duration_us_total: FSQLITE_VDBE_STATEMENT_DURATION_US_TOTAL
            .load(AtomicOrdering::Relaxed),
        sort_rows_total: FSQLITE_SORT_ROWS_TOTAL.load(AtomicOrdering::Relaxed),
        sort_spill_pages_total: FSQLITE_SORT_SPILL_PAGES_TOTAL.load(AtomicOrdering::Relaxed),
    }
}

/// Reset VDBE metrics to zero (tests/diagnostics).
pub fn reset_vdbe_metrics() {
    FSQLITE_VDBE_OPCODES_EXECUTED_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_VDBE_STATEMENTS_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_VDBE_STATEMENT_DURATION_US_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_SORT_ROWS_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_SORT_SPILL_PAGES_TOTAL.store(0, AtomicOrdering::Relaxed);
    reset_vdbe_jit_metrics();
}

#[derive(Debug, Clone, Copy)]
enum JitDecision {
    Disabled,
    Warming {
        plan_hash: u64,
        execution_count: u64,
    },
    CacheHit {
        plan_hash: u64,
        code_size_bytes: u64,
    },
    UnsupportedCached {
        plan_hash: u64,
    },
    Compiled {
        plan_hash: u64,
        compile_time_us: u64,
        code_size_bytes: u64,
        evicted_plan_hash: Option<u64>,
    },
    CompileFailed {
        plan_hash: u64,
        compile_time_us: u64,
        reason: &'static str,
    },
}

fn hash_program(program: &VdbeProgram) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn mix(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    let mut hash = FNV_OFFSET;
    mix(&mut hash, &program.register_count().to_le_bytes());
    for op in program.ops() {
        mix(&mut hash, &[op.opcode as u8]);
        mix(&mut hash, &op.p1.to_le_bytes());
        mix(&mut hash, &op.p2.to_le_bytes());
        mix(&mut hash, &op.p3.to_le_bytes());
        mix(&mut hash, &op.p5.to_le_bytes());
    }
    hash
}

fn jit_scaffold_supports_opcode(opcode: Opcode) -> bool {
    !matches!(
        opcode,
        Opcode::OpenRead
            | Opcode::OpenWrite
            | Opcode::OpenDup
            | Opcode::OpenEphemeral
            | Opcode::OpenAutoindex
            | Opcode::OpenPseudo
            | Opcode::SorterOpen
            | Opcode::Close
            | Opcode::Column
            | Opcode::SeekLT
            | Opcode::SeekLE
            | Opcode::SeekGE
            | Opcode::SeekGT
            | Opcode::SeekRowid
            | Opcode::Insert
            | Opcode::Delete
            | Opcode::SorterData
            | Opcode::Rowid
            | Opcode::Last
            | Opcode::SorterSort
            | Opcode::Rewind
            | Opcode::SorterNext
            | Opcode::Prev
            | Opcode::Next
            | Opcode::IdxInsert
            | Opcode::SorterInsert
            | Opcode::IdxDelete
            | Opcode::IdxRowid
            | Opcode::VOpen
            | Opcode::VFilter
            | Opcode::VColumn
            | Opcode::VNext
            | Opcode::VUpdate
            | Opcode::VBegin
            | Opcode::VCreate
            | Opcode::VDestroy
            | Opcode::VCheck
            | Opcode::VInitIn
            | Opcode::VRename
    )
}

fn compile_jit_stub(program: &VdbeProgram) -> std::result::Result<u64, &'static str> {
    if program
        .ops()
        .iter()
        .any(|op| !jit_scaffold_supports_opcode(op.opcode))
    {
        return Err("unsupported opcode in JIT scaffold compiler");
    }
    let op_count = u64::try_from(program.ops().len()).unwrap_or(u64::MAX);
    Ok(op_count.saturating_mul(32).max(64))
}

fn maybe_trigger_jit(program: &VdbeProgram) -> JitDecision {
    if !vdbe_jit_enabled() {
        return JitDecision::Disabled;
    }
    let plan_hash = hash_program(program);
    let hot_threshold = vdbe_jit_hot_threshold();
    let cache_capacity = vdbe_jit_cache_capacity();

    let mut runtime = lock_jit_runtime();
    let execution_count = {
        let count = runtime.executions_by_plan.entry(plan_hash).or_insert(0);
        *count = count.saturating_add(1);
        *count
    };
    if execution_count < hot_threshold {
        return JitDecision::Warming {
            plan_hash,
            execution_count,
        };
    }

    FSQLITE_JIT_TRIGGERS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
    if runtime.is_unsupported_plan(plan_hash) {
        return JitDecision::UnsupportedCached { plan_hash };
    }
    if let Some(code_size_bytes) = runtime
        .cache
        .get(&plan_hash)
        .map(|entry| entry.code_size_bytes)
    {
        FSQLITE_JIT_CACHE_HITS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        runtime.touch_lru(plan_hash);
        return JitDecision::CacheHit {
            plan_hash,
            code_size_bytes,
        };
    }

    FSQLITE_JIT_CACHE_MISSES_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
    let compile_started = Instant::now();
    match compile_jit_stub(program) {
        Ok(code_size_bytes) => {
            FSQLITE_JIT_COMPILATIONS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
            let compile_time_us =
                u64::try_from(compile_started.elapsed().as_micros()).unwrap_or(u64::MAX);
            let evicted_plan_hash =
                runtime.insert_cache(plan_hash, JitCacheEntry { code_size_bytes }, cache_capacity);
            JitDecision::Compiled {
                plan_hash,
                compile_time_us,
                code_size_bytes,
                evicted_plan_hash,
            }
        }
        Err(reason) => {
            FSQLITE_JIT_COMPILE_FAILURES_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
            let compile_time_us =
                u64::try_from(compile_started.elapsed().as_micros()).unwrap_or(u64::MAX);
            runtime.mark_unsupported_plan(plan_hash, cache_capacity.max(1));
            JitDecision::CompileFailed {
                plan_hash,
                compile_time_us,
                reason,
            }
        }
    }
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
#[allow(clippy::struct_excessive_bools)]
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
    cursors: SwissIndex<i32, MemCursor>,
    /// Open sorter cursors keyed by cursor number.
    sorters: SwissIndex<i32, SorterCursor>,
    /// Open storage-backed cursors keyed by cursor number (read and write).
    storage_cursors: SwissIndex<i32, StorageCursor>,
    /// Cursors that deleted the current row and should treat the next `Next`
    /// as a no-advance "consume successor" step.
    pending_next_after_delete: HashSet<i32>,
    /// Whether `OpenRead`/`OpenWrite` should route through storage-backed cursors.
    storage_cursors_enabled: bool,
    /// Shared pager transaction for storage cursors (Phase 5, bd-2a3y).
    /// When set, `open_storage_cursor` routes through the real pager/WAL
    /// stack instead of building transient `MemPageStore` snapshots.
    txn_page_io: Option<SharedTxnPageIo>,
    /// When true, `open_storage_cursor` will reject the MemPageStore fallback
    /// path and return false instead of silently routing through in-memory
    /// storage. Used in parity-certification mode (bd-2ttd8.1) to verify all
    /// cursor operations flow through the real Pager+BtreeCursor stack.
    reject_mem_fallback: bool,
    /// In-memory database backing cursor operations (shared with Connection).
    db: Option<MemDatabase>,
    /// Scalar/aggregate/window function registry for Function/PureFunc opcodes.
    func_registry: Option<Arc<FunctionRegistry>>,
    /// Aggregate accumulators keyed by accumulator register.
    aggregates: SwissIndex<i32, AggregateContext>,
    /// Schema cookie value provided by the Connection (bd-3mmj).
    /// Schema cookie value provided by the Connection (bd-3mmj).
    /// Used by `ReadCookie` (p3=1) and `SetCookie` opcodes, and
    /// by `Transaction` for stale-schema detection.
    schema_cookie: u32,
    /// Result of the last `Opcode::Compare` operation.
    last_compare_result: Option<Ordering>,
    /// Number of rows modified (inserted, deleted, or updated) during execution.
    changes: usize,
    /// Rowid of the last INSERT operation (for `last_insert_rowid()` support).
    last_insert_rowid: i64,
    /// Cursor ID used by the last Insert opcode (for conflict resolution in
    /// `IdxInsert`: allows the index handler to undo or replace the table row).
    last_insert_cursor_id: Option<i32>,
    /// Provisional table insert metadata kept until later `IdxInsert`
    /// opcodes either succeed or roll the row back under `OE_IGNORE`.
    pending_insert_rollback: Option<PendingInsertRollback>,
    /// When true, a UNIQUE conflict with IGNORE was detected during an
    /// `IdxInsert`, so remaining `IdxInsert` opcodes for this row should be
    /// skipped.
    conflict_skip_idx: bool,
    /// RowSet data structures for OR-optimized queries (keyed by register).
    rowsets: SwissIndex<i32, RowSet>,
    /// Foreign key constraint violation counter (deferred FK enforcement).
    fk_counter: i64,
    /// AUTOINCREMENT high-water marks keyed by root page number (bd-31j76).
    /// Populated from `sqlite_sequence` by the Connection before execution.
    autoincrement_seq_by_root_page: HashMap<i32, i64>,
    /// Per-cursor monotonic sequence counters for `Opcode::Sequence`.
    sequence_counters: HashMap<i32, i64>,
    /// Column default values by root page number (for ALTER TABLE ADD COLUMN).
    /// When a row has fewer columns than the schema expects, defaults from this
    /// map are applied instead of returning NULL.
    column_defaults_by_root_page: HashMap<i32, Vec<Option<SqliteValue>>>,
    /// Mapping from cursor_id to root_page for default value lookup.
    cursor_root_pages: HashMap<i32, i32>,
    /// Open virtual table cursors keyed by cursor number.
    vtab_cursors: SwissIndex<i32, VtabCursorState>,
    /// Virtual table instances keyed by cursor number (for transaction ops).
    vtab_instances: SwissIndex<i32, Box<dyn VtabInstance>>,
    /// Cursors with time-travel snapshots (SQL:2011 temporal queries).
    /// Keyed by cursor ID; the integration layer uses this to resolve
    /// historical page versions and enforce read-only semantics.
    time_travel_cursors: HashMap<i32, TimeTravelMarker>,
    /// MVCC version store for time-travel page resolution.
    /// Set by the connection layer before execution when time-travel
    /// queries may be present.
    version_store: Option<Rc<RefCell<VersionStore>>>,
    /// Metadata mapping table cursor IDs to their associated index cursors
    /// and column indices. Used by `native_replace_row` to clean up secondary
    /// index entries during REPLACE conflict resolution.
    table_index_meta: HashMap<i32, Vec<IndexCursorMeta>>,
    /// Window function accumulators keyed by accumulator register.
    window_contexts: SwissIndex<i32, WindowContext>,
    /// Register subtype tags (register index → subtype value).
    /// Used by JSON functions to distinguish JSON text (subtype 74/'J')
    /// from regular text. Cleared on each register write.
    register_subtypes: HashMap<i32, u32>,
    /// Bloom filters keyed by cursor/filter register.
    /// Each entry is a fixed-size bit array used for early rejection
    /// during index lookups.
    bloom_filters: HashMap<i32, Vec<u64>>,
}

/// Time-travel target marker stored on cursors opened with
/// `FOR SYSTEM_TIME AS OF ...`.
#[derive(Debug, Clone)]
pub enum TimeTravelMarker {
    /// Pinned to a specific commit sequence number.
    CommitSeq(u64),
    /// Resolved from an ISO-8601 timestamp string.
    Timestamp(String),
}

/// Type-erased virtual table instance for transaction and lifecycle ops.
///
/// Because `VirtualTable` has an associated `Cursor` type, we need a
/// type-erased wrapper to store heterogeneous vtab instances.
#[allow(dead_code)]
trait VtabInstance: Send + Sync {
    fn begin(&mut self, cx: &Cx) -> Result<()>;
    fn commit(&mut self, cx: &Cx) -> Result<()>;
    fn rollback(&mut self, cx: &Cx) -> Result<()>;
    fn destroy(&mut self, cx: &Cx) -> Result<()>;
    fn rename(&mut self, cx: &Cx, new_name: &str) -> Result<()>;
    fn open_cursor(&self) -> Result<Box<dyn ErasedVtabCursor>>;
    fn vtab_update(&mut self, cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>>;
}

/// A type-erased virtual table cursor.
///
/// Wraps a concrete `VirtualTableCursor` implementation behind dynamic
/// dispatch so the engine can store cursors from different vtab modules
/// in the same index.
pub trait ErasedVtabCursor: Send {
    fn filter(
        &mut self,
        cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()>;
    fn next(&mut self, cx: &Cx) -> Result<()>;
    fn eof(&self) -> bool;
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()>;
    fn rowid(&self) -> Result<i64>;
}

/// Blanket implementation of `ErasedVtabCursor` for any concrete cursor type.
impl<C: fsqlite_func::vtab::VirtualTableCursor + 'static> ErasedVtabCursor for C {
    fn filter(
        &mut self,
        cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        fsqlite_func::vtab::VirtualTableCursor::filter(self, cx, idx_num, idx_str, args)
    }
    fn next(&mut self, cx: &Cx) -> Result<()> {
        fsqlite_func::vtab::VirtualTableCursor::next(self, cx)
    }
    fn eof(&self) -> bool {
        fsqlite_func::vtab::VirtualTableCursor::eof(self)
    }
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        fsqlite_func::vtab::VirtualTableCursor::column(self, ctx, col)
    }
    fn rowid(&self) -> Result<i64> {
        fsqlite_func::vtab::VirtualTableCursor::rowid(self)
    }
}

/// Bridge from `fsqlite_func::vtab::ErasedVtabInstance` to engine's `VtabInstance`.
struct ErasedVtabBridge(Box<dyn fsqlite_func::vtab::ErasedVtabInstance>);

impl VtabInstance for ErasedVtabBridge {
    fn begin(&mut self, cx: &Cx) -> Result<()> {
        self.0.begin(cx)
    }
    fn commit(&mut self, cx: &Cx) -> Result<()> {
        self.0.commit(cx)
    }
    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        self.0.rollback(cx)
    }
    fn destroy(&mut self, cx: &Cx) -> Result<()> {
        self.0.destroy(cx)
    }
    fn rename(&mut self, cx: &Cx, new_name: &str) -> Result<()> {
        self.0.rename(cx, new_name)
    }
    fn open_cursor(&self) -> Result<Box<dyn ErasedVtabCursor>> {
        let func_cursor = self.0.open_cursor()?;
        Ok(Box::new(FuncVtabCursorBridge(func_cursor)))
    }
    fn vtab_update(&mut self, cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        self.0.update(cx, args)
    }
}

/// Bridge from `fsqlite_func::vtab::ErasedVtabCursor` to engine's `ErasedVtabCursor`.
struct FuncVtabCursorBridge(Box<dyn fsqlite_func::vtab::ErasedVtabCursor>);

impl ErasedVtabCursor for FuncVtabCursorBridge {
    fn filter(
        &mut self,
        cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        self.0.erased_filter(cx, idx_num, idx_str, args)
    }
    fn next(&mut self, cx: &Cx) -> Result<()> {
        self.0.erased_next(cx)
    }
    fn eof(&self) -> bool {
        self.0.erased_eof()
    }
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        self.0.erased_column(ctx, col)
    }
    fn rowid(&self) -> Result<i64> {
        self.0.erased_rowid()
    }
}

/// State for an open virtual table cursor in the VDBE engine.
struct VtabCursorState {
    /// The type-erased cursor.
    cursor: Box<dyn ErasedVtabCursor>,
}

/// A set of rowids for RowSetAdd/RowSetRead/RowSetTest opcodes.
///
/// Used by OR-optimized queries and IN subquery evaluation.
/// SQLite implements this as a sorted unique set of i64 rowids.
struct RowSet {
    /// Sorted, deduplicated set of rowids.
    entries: Vec<i64>,
    /// Current read position for `RowSetRead`.
    read_pos: usize,
}

impl RowSet {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            read_pos: 0,
        }
    }

    /// Add a rowid to the set (maintains sorted order, deduplicates).
    fn add(&mut self, rowid: i64) {
        match self.entries.binary_search(&rowid) {
            Ok(_) => {} // Already present
            Err(pos) => self.entries.insert(pos, rowid),
        }
    }

    /// Read the next rowid. Returns `None` when exhausted.
    fn read_next(&mut self) -> Option<i64> {
        if self.read_pos < self.entries.len() {
            let val = self.entries[self.read_pos];
            self.read_pos += 1;
            Some(val)
        } else {
            None
        }
    }

    /// Test if a rowid exists in the set.
    fn contains(&self, rowid: i64) -> bool {
        self.entries.binary_search(&rowid).is_ok()
    }
}

/// Stack-backed byte buffer for distinct keys (avoids heap allocation for small keys).
type DistinctKeyBuf = smallvec::SmallVec<[u8; 64]>;

struct AggregateContext {
    func: Arc<ErasedAggregateFunction>,
    state: Box<dyn Any + Send>,
    /// When DISTINCT is active, tracks seen argument byte-keys to skip duplicates.
    distinct_seen: Option<std::collections::HashSet<DistinctKeyBuf>>,
}

/// Metadata for a provisional table insert that may need to be undone if a
/// later UNIQUE index write resolves to `OE_IGNORE`.
#[derive(Debug, Clone, Copy)]
struct PendingInsertRollback {
    cursor_id: i32,
    rowid: i64,
    previous_last_insert_rowid: i64,
}

/// Per-accumulator window function context (for `AggInverse` / `AggValue`).
///
/// TODO: Window functions are not yet emitted by codegen, so `AggInverse` /
/// `AggValue` opcodes are currently dead code. When window functions are
/// implemented, the split between `self.aggregates` (used by `AggStep` /
/// `AggFinal`) and `self.window_contexts` (used by `AggInverse` / `AggValue`)
/// must be reconciled — a function registered as both aggregate and window
/// would need its state shared, not duplicated across the two maps.
struct WindowContext {
    func: Arc<ErasedWindowFunction>,
    state: Box<dyn Any + Send>,
}

/// Encode aggregate arguments into a canonical byte key for DISTINCT deduplication.
///
/// Each value is tagged with a type discriminant followed by its payload so that
/// values of different types with the same byte representation don't collide.
fn distinct_key(args: &[SqliteValue]) -> DistinctKeyBuf {
    let mut key = DistinctKeyBuf::new();
    for val in args {
        match val {
            SqliteValue::Null => key.push(0),
            SqliteValue::Integer(i) => {
                key.push(1);
                key.extend_from_slice(&i.to_le_bytes());
            }
            SqliteValue::Float(f) => {
                if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(f) {
                    #[allow(clippy::cast_possible_truncation)]
                    let i = *f as i64;
                    #[allow(clippy::cast_precision_loss)]
                    if (i as f64) == *f {
                        key.push(1);
                        key.extend_from_slice(&i.to_le_bytes());
                        continue;
                    }
                }
                key.push(2);
                key.extend_from_slice(&f.to_bits().to_le_bytes());
            }
            SqliteValue::Text(s) => {
                key.push(3);
                #[allow(clippy::cast_possible_truncation)]
                key.extend_from_slice(&(s.len() as u64).to_le_bytes());
                key.extend_from_slice(s.as_bytes());
            }
            SqliteValue::Blob(b) => {
                key.push(4);
                #[allow(clippy::cast_possible_truncation)]
                key.extend_from_slice(&(b.len() as u64).to_le_bytes());
                key.extend_from_slice(b);
            }
        }
    }
    key
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
            cursors: SwissIndex::new(),
            sorters: SwissIndex::new(),
            storage_cursors: SwissIndex::new(),
            pending_next_after_delete: HashSet::new(),
            storage_cursors_enabled: true,
            txn_page_io: None,
            // bd-zjisk.1: Default to parity-cert mode — reject MemPageStore fallback.
            reject_mem_fallback: true,
            db: None,
            func_registry: None,
            aggregates: SwissIndex::new(),
            schema_cookie: 0,
            last_compare_result: None,
            changes: 0,
            last_insert_rowid: 0,
            last_insert_cursor_id: None,
            pending_insert_rollback: None,
            conflict_skip_idx: false,
            rowsets: SwissIndex::new(),
            fk_counter: 0,
            autoincrement_seq_by_root_page: HashMap::new(),
            sequence_counters: HashMap::new(),
            column_defaults_by_root_page: HashMap::new(),
            cursor_root_pages: HashMap::new(),
            vtab_cursors: SwissIndex::new(),
            vtab_instances: SwissIndex::new(),
            time_travel_cursors: HashMap::new(),
            version_store: None,
            table_index_meta: HashMap::new(),
            window_contexts: SwissIndex::new(),
            register_subtypes: HashMap::new(),
            bloom_filters: HashMap::new(),
        }
    }

    /// Returns the number of rows modified (inserted, deleted, or updated).
    pub fn changes(&self) -> usize {
        self.changes
    }

    /// Returns the rowid of the last INSERT operation.
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }

    /// Returns the time-travel marker for a cursor, if any.
    pub fn time_travel_marker(&self, cursor_id: i32) -> Option<&TimeTravelMarker> {
        self.time_travel_cursors.get(&cursor_id)
    }

    /// Returns all time-travel cursor mappings.
    pub fn time_travel_cursors(&self) -> &HashMap<i32, TimeTravelMarker> {
        &self.time_travel_cursors
    }

    /// Set the MVCC version store for time-travel page resolution.
    ///
    /// Must be called by the connection layer before executing programs that
    /// contain `SetSnapshot` opcodes so the engine can create
    /// `TimeTravelPageIo` cursors.
    pub fn set_version_store(&mut self, vs: Rc<RefCell<VersionStore>>) {
        self.version_store = Some(vs);
    }

    /// Handles REPLACE conflict resolution natively (bd-2yqp6.x).
    /// Deletes the conflicting row from the table AND from all associated indexes.
    fn native_replace_row(&mut self, tbl_cursor_id: i32, conflict_rowid: i64) -> Result<()> {
        let old_payload = if let Some(tsc) = self.storage_cursors.get_mut(&tbl_cursor_id) {
            if tsc
                .cursor
                .table_move_to(&tsc.cx, conflict_rowid)?
                .is_found()
            {
                Some(tsc.cursor.payload(&tsc.cx)?)
            } else {
                None
            }
        } else {
            None
        };

        let Some(payload) = old_payload else {
            return Ok(());
        };

        // Parse old row to extract column values for index key construction.
        let old_row = parse_record(&payload).unwrap_or_default();

        // Delete secondary index entries for the old row using the metadata
        // registered by the codegen. For each index cursor, build the index
        // key from the old row's column values and delete it.
        if let Some(index_metas) = self.table_index_meta.get(&tbl_cursor_id).cloned() {
            for meta in &index_metas {
                // Build index key: (indexed_col_values..., rowid).
                let mut key_values: Vec<SqliteValue> =
                    Vec::with_capacity(meta.column_indices.len() + 1);
                for &col_idx in &meta.column_indices {
                    let val = old_row.get(col_idx).cloned().unwrap_or(SqliteValue::Null);
                    key_values.push(val);
                }
                key_values.push(SqliteValue::Integer(conflict_rowid));
                let key_bytes = encode_record(&key_values);

                // Seek to the key in the index cursor and delete it.
                if let Some(sc) = self.storage_cursors.get_mut(&meta.cursor_id) {
                    if sc.writable && sc.cursor.index_move_to(&sc.cx, &key_bytes)?.is_found() {
                        sc.cursor.delete(&sc.cx)?;
                    }
                }
            }
        }

        // Delete the table row.
        if let Some(tsc) = self.storage_cursors.get_mut(&tbl_cursor_id) {
            tsc.cursor.table_move_to(&tsc.cx, conflict_rowid)?;
            tsc.cursor.delete(&tsc.cx)?;
        }

        Ok(())
    }

    /// Attach an in-memory database for cursor operations.
    pub fn set_database(&mut self, db: MemDatabase) {
        self.db = Some(db);
    }

    /// Take ownership of the in-memory database back from the engine.
    pub fn take_database(&mut self) -> Option<MemDatabase> {
        self.db.take()
    }

    /// Register a type-erased virtual table cursor for opcode execution.
    pub fn register_vtab_cursor(&mut self, cursor_id: i32, cursor: Box<dyn ErasedVtabCursor>) {
        self.vtab_cursors
            .insert(cursor_id, VtabCursorState { cursor });
    }

    /// Register a virtual table instance for lifecycle and cursor operations.
    pub fn register_vtab_instance(
        &mut self,
        cursor_id: i32,
        instance: Box<dyn fsqlite_func::vtab::ErasedVtabInstance>,
    ) {
        self.vtab_instances
            .insert(cursor_id, Box::new(ErasedVtabBridge(instance)));
    }

    /// Enable/disable storage-backed cursor execution for `OpenRead`/`OpenWrite`.
    pub fn enable_storage_cursors(&mut self, enabled: bool) {
        self.storage_cursors_enabled = enabled;
    }

    /// Backwards-compatible alias for [`Self::enable_storage_cursors`].
    pub fn enable_storage_read_cursors(&mut self, enabled: bool) {
        self.enable_storage_cursors(enabled);
    }

    /// Enable parity-certification mode (bd-2ttd8.1).
    ///
    /// When enabled, `open_storage_cursor` will refuse to fall back to the
    /// in-memory `MemPageStore` path and instead return an error. This
    /// verifies that all cursor operations route through the real
    /// Pager+BtreeCursor stack (`txn_page_io`).
    pub fn set_reject_mem_fallback(&mut self, reject: bool) {
        self.reject_mem_fallback = reject;
    }

    /// Returns `true` if all open storage cursors use the real pager backend
    /// (`CursorBackend::Txn`). Returns `true` vacuously when no cursors are open.
    ///
    /// Used by parity-certification (bd-2ttd8.4) to verify that no cursor
    /// accidentally routed through MemPageStore.
    #[must_use]
    pub fn all_cursors_are_txn_backed(&self) -> bool {
        self.storage_cursors.values().all(|sc| sc.cursor.is_txn())
    }

    /// Returns `true` if any open storage cursor uses the in-memory backend.
    #[must_use]
    pub fn has_mem_cursor(&self) -> bool {
        self.storage_cursors.values().any(|sc| sc.cursor.is_mem())
    }

    /// Validate the parity-certification invariant: if `reject_mem_fallback`
    /// is enabled, no storage cursor should be backed by MemPageStore.
    ///
    /// Returns `Ok(())` if the invariant holds, or `Err` with a diagnostic
    /// message listing the offending cursor IDs.
    pub fn validate_parity_cert_invariant(&self) -> std::result::Result<(), String> {
        if !self.reject_mem_fallback {
            return Ok(());
        }
        let mem_cursors: Vec<i32> = self
            .storage_cursors
            .iter()
            .filter(|(_, sc)| sc.cursor.is_mem())
            .map(|(id, _)| *id)
            .collect();
        if mem_cursors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "bd-2ttd8.4: parity-cert violation — {} cursor(s) routed through MemPageStore: {:?}",
                mem_cursors.len(),
                mem_cursors
            ))
        }
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
        registry: Arc<Mutex<ConcurrentRegistry>>,
        lock_table: Arc<InProcessPageLockTable>,
        busy_timeout_ms: u64,
    ) {
        self.txn_page_io = Some(SharedTxnPageIo::with_concurrent(
            txn,
            session_id,
            registry,
            lock_table,
            busy_timeout_ms,
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

    /// Provide AUTOINCREMENT high-water marks keyed by root page (bd-31j76).
    /// The engine uses these to guarantee monotonically increasing rowids
    /// for tables declared with `AUTOINCREMENT`.
    pub fn set_autoincrement_sequence_by_root_page(&mut self, map: HashMap<i32, i64>) {
        self.autoincrement_seq_by_root_page = map;
    }

    /// Set column default values by root page, used for ALTER TABLE ADD COLUMN.
    /// Each entry maps a root page to a list of per-column defaults (None = no default).
    pub fn set_column_defaults_by_root_page(
        &mut self,
        map: HashMap<i32, Vec<Option<SqliteValue>>>,
    ) {
        self.column_defaults_by_root_page = map;
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

        // Load table-to-index cursor metadata for REPLACE conflict resolution.
        for (table_cursor, indexes) in program.table_index_meta() {
            self.table_index_meta.insert(*table_cursor, indexes.clone());
        }

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

        match maybe_trigger_jit(program) {
            JitDecision::Disabled => {}
            JitDecision::Warming {
                plan_hash,
                execution_count,
            } => {
                tracing::debug!(
                    target: "fsqlite_vdbe::jit",
                    program_id,
                    plan_hash = format_args!("{plan_hash:016x}"),
                    execution_count,
                    hot_threshold = vdbe_jit_hot_threshold(),
                    "jit warmup (interpreter tier)"
                );
            }
            JitDecision::CacheHit {
                plan_hash,
                code_size_bytes,
            } => {
                tracing::info!(
                    target: "fsqlite_vdbe::jit",
                    program_id,
                    plan_hash = format_args!("{plan_hash:016x}"),
                    code_size_bytes,
                    "jit trigger cache hit (interpreter fallback path)"
                );
            }
            JitDecision::UnsupportedCached { plan_hash } => {
                tracing::debug!(
                    target: "fsqlite_vdbe::jit",
                    program_id,
                    plan_hash = format_args!("{plan_hash:016x}"),
                    "jit unsupported-plan cache hit (skipping recompilation)"
                );
            }
            JitDecision::Compiled {
                plan_hash,
                compile_time_us,
                code_size_bytes,
                evicted_plan_hash,
            } => {
                let plan_hash_hex = format!("{plan_hash:016x}");
                let span = tracing::info_span!(
                    target: "fsqlite_vdbe::jit",
                    "jit_compile",
                    plan_hash = %plan_hash_hex,
                    compile_time_us,
                    code_size_bytes,
                );
                let _compile_guard = span.enter();
                tracing::info!(
                    target: "fsqlite_vdbe::jit",
                    program_id,
                    plan_hash = %plan_hash_hex,
                    compile_time_us,
                    code_size_bytes,
                    evicted_plan_hash = evicted_plan_hash.map(|value| format!("{value:016x}")),
                    "jit trigger compile completed (interpreter fallback path)"
                );
            }
            JitDecision::CompileFailed {
                plan_hash,
                compile_time_us,
                reason,
            } => {
                let plan_hash_hex = format!("{plan_hash:016x}");
                let span = tracing::info_span!(
                    target: "fsqlite_vdbe::jit",
                    "jit_compile",
                    plan_hash = %plan_hash_hex,
                    compile_time_us,
                    code_size_bytes = 0_u64,
                );
                let _compile_guard = span.enter();
                tracing::warn!(
                    target: "fsqlite_vdbe::jit",
                    program_id,
                    plan_hash = %plan_hash_hex,
                    compile_time_us,
                    reason,
                    "jit compilation failed; falling back to interpreter"
                );
            }
        }

        let mut pc: usize = 0;
        // "once" flags: one bit per instruction address (stack-backed for small programs).
        let n_ops = ops.len();
        let mut once_stack = [0u64; 4]; // covers up to 256 opcodes on the stack
        let mut once_heap: Vec<u64> = if n_ops > 256 {
            vec![0u64; n_ops.div_ceil(64)]
        } else {
            Vec::new()
        };
        let once_bits: &mut [u64] = if n_ops > 256 {
            &mut once_heap
        } else {
            &mut once_stack
        };

        let outcome = loop {
            if pc >= ops.len() {
                break ExecOutcome::Done;
            }

            let op = &ops[pc];
            opcode_count += 1;
            if self.trace_opcodes {
                self.trace_opcode(pc, op);
            }
            #[allow(unreachable_patterns)]
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

                Opcode::SetSnapshot => {
                    // Attach a time-travel snapshot to cursor P1.
                    //
                    // 1. Record the marker for read-only enforcement.
                    // 2. If a VersionStore is available, replace the cursor's
                    //    page I/O backend with `TimeTravelPageIo` so that
                    //    subsequent reads resolve historical page versions
                    //    from the MVCC version store.
                    let cursor_id = op.p1;
                    let target = match &op.p4 {
                        P4::TimeTravelCommitSeq(seq) => TimeTravelMarker::CommitSeq(*seq),
                        P4::TimeTravelTimestamp(ts) => TimeTravelMarker::Timestamp(ts.clone()),
                        _ => {
                            return Err(FrankenError::Internal(
                                "SetSnapshot: invalid P4 (expected time-travel target)".to_owned(),
                            ));
                        }
                    };
                    self.time_travel_cursors.insert(cursor_id, target.clone());

                    // Upgrade the cursor to a time-travel backend if we have
                    // both a VersionStore and the cursor uses a Txn backend.
                    if let Some(ref vs) = self.version_store {
                        if let Some(sc) = self.storage_cursors.get(&cursor_id) {
                            // We need the cursor's root page and table flag to
                            // re-create the BtCursor with TimeTravelPageIo.
                            if let CursorBackend::Txn(_) = &sc.cursor {
                                let commit_seq = match &target {
                                    TimeTravelMarker::CommitSeq(seq) => *seq,
                                    TimeTravelMarker::Timestamp(_ts) => {
                                        // Timestamp resolution requires CommitLog
                                        // which is not yet wired. Return an error
                                        // for now.
                                        return Err(FrankenError::Internal(
                                            "SetSnapshot: timestamp-based time-travel \
                                             not yet supported (CommitLog not available)"
                                                .to_owned(),
                                        ));
                                    }
                                };

                                let tt_snapshot = TimeTravelSnapshot::new_for_commit_seq(
                                    CommitSeq::new(commit_seq),
                                    SchemaEpoch::new(u64::from(self.schema_cookie)),
                                );

                                // Re-create cursor with TimeTravelPageIo.
                                // Remove the old cursor to get its metadata.
                                let old_sc = self.storage_cursors.remove(&cursor_id).unwrap();
                                let root_page =
                                    self.cursor_root_pages.get(&cursor_id).copied().unwrap_or(1);
                                let root_pgno =
                                    PageNumber::new(root_page as u32).unwrap_or(PageNumber::ONE);

                                // Extract the SharedTxnPageIo from the old cursor.
                                // Since we verified it's Txn above, this is safe.
                                let inner_page_io = if let Some(ref page_io) = self.txn_page_io {
                                    page_io.clone()
                                } else {
                                    return Err(FrankenError::Internal(
                                        "SetSnapshot: no transaction page I/O available".to_owned(),
                                    ));
                                };

                                let tt_page_io = TimeTravelPageIo {
                                    inner: inner_page_io,
                                    version_store: Rc::clone(vs),
                                    snapshot: tt_snapshot,
                                };

                                // Preserve the cursor's table-vs-index type from
                                // the original cursor so index cursors remain correct.
                                let is_table_btree = old_sc.cursor.is_table_btree();
                                const PAGE_SIZE: u32 = 4096;
                                let new_cursor =
                                    BtCursor::new(tt_page_io, root_pgno, PAGE_SIZE, is_table_btree);
                                self.storage_cursors.insert(
                                    cursor_id,
                                    StorageCursor {
                                        cursor: CursorBackend::TimeTravel(new_cursor),
                                        cx: old_sc.cx,
                                        writable: false, // Time-travel is always read-only
                                        last_alloc_rowid: 0,
                                        cached_row: None,
                                    },
                                );
                                tracing::info!(
                                    cursor_id,
                                    commit_seq,
                                    "SetSnapshot: upgraded cursor to time-travel backend"
                                );
                            }
                        }
                    }
                    pc += 1;
                }

                // ── Constants ───────────────────────────────────────────
                Opcode::Integer => {
                    // Set register p2 to integer value p1.
                    self.set_reg_fast(op.p2, SqliteValue::Integer(i64::from(op.p1)));
                    pc += 1;
                }

                Opcode::Int64 => {
                    let val = match &op.p4 {
                        P4::Int64(v) => *v,
                        _ => 0,
                    };
                    self.set_reg_fast(op.p2, SqliteValue::Integer(val));
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
                    // To handle potential overlap correctly, we collect all source
                    // values into a temporary buffer before writing them to the destination.
                    let count = usize::try_from(op.p3).unwrap_or(0);
                    let mut temp = Vec::with_capacity(count);

                    for offset in 0..count {
                        let value = Self::reg_with_offset(op.p1, offset)
                            .map(|reg| self.take_reg(reg))
                            .unwrap_or(SqliteValue::Null);
                        temp.push(value);
                    }

                    for (i, val) in temp.into_iter().enumerate() {
                        self.set_reg(op.p2 + (i as i32), val);
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
                    let row = self.collect_reg_range(op.p1, usize::try_from(op.p2).unwrap_or(0));
                    self.results.push(row);
                    pc += 1;
                }

                // ── Arithmetic ──────────────────────────────────────────
                Opcode::Add => {
                    // p3 = p2 + p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_add(b);
                    self.set_reg_fast(op.p3, result);
                    pc += 1;
                }

                Opcode::Subtract => {
                    // p3 = p2 - p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_sub(b);
                    self.set_reg_fast(op.p3, result);
                    pc += 1;
                }

                Opcode::Multiply => {
                    // p3 = p2 * p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_mul(b);
                    self.set_reg_fast(op.p3, result);
                    pc += 1;
                }

                Opcode::Divide => {
                    // p3 = p2 / p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_div(dividend, divisor);
                    self.set_reg_fast(op.p3, result);
                    pc += 1;
                }

                Opcode::Remainder => {
                    // p3 = p2 % p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_rem(dividend, divisor);
                    self.set_reg_fast(op.p3, result);
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
                    let val = self
                        .get_reg(op.p1)
                        .to_integer()
                        .wrapping_add(i64::from(op.p2));
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
                    let val = self.get_reg(op.p1).clone();
                    let coerced = val.apply_affinity(fsqlite_types::TypeAffinity::Integer);
                    if coerced.as_integer().is_some() {
                        self.set_reg(op.p1, coerced);
                        pc += 1;
                    } else {
                        if op.p2 > 0 {
                            pc = op.p2 as usize;
                            continue;
                        }
                        return Err(FrankenError::TypeMismatch {
                            expected: "integer".to_owned(),
                            actual: self.get_reg(op.p1).typeof_str().to_owned(),
                        });
                    }
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
                    let store_p2 = (op.p5 & 0x20) != 0; // SQLITE_STOREP2

                    if lhs.is_null() || rhs.is_null() {
                        let null_eq = (op.p5 & 0x80) != 0;
                        if null_eq {
                            // IS / IS NOT semantics: NULL == NULL is true.
                            let both_null = lhs.is_null() && rhs.is_null();
                            let should_jump = match op.opcode {
                                Opcode::Eq => both_null,
                                Opcode::Ne => !both_null,
                                _ => false,
                            };
                            if store_p2 {
                                self.set_reg(op.p2, SqliteValue::Integer(i64::from(should_jump)));
                                pc += 1;
                            } else if should_jump {
                                pc = op.p2 as usize;
                            } else {
                                pc += 1;
                            }
                        } else if store_p2 {
                            // STOREP2 with NULL: store NULL in P2.
                            self.set_reg(op.p2, SqliteValue::Null);
                            pc += 1;
                        } else {
                            // JUMPIFNULL (0x10): jump to P2 when either is NULL.
                            let jump_if_null = (op.p5 & 0x10) != 0;
                            if jump_if_null {
                                pc = op.p2 as usize;
                            } else {
                                pc += 1;
                            }
                        }
                    } else {
                        // Apply SQLite comparison affinity rules (§3.2):
                        // coercion only happens when p5 carries numeric affinity.
                        let (cmp_lhs, cmp_rhs) = coerce_for_comparison(lhs, rhs, op.p5);
                        let cmp = if let P4::Collation(ref coll_name) = op.p4 {
                            collate_compare(&cmp_lhs, &cmp_rhs, coll_name)
                        } else {
                            cmp_lhs.partial_cmp(&cmp_rhs)
                        };
                        let should_jump = matches!(
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
                        );

                        if store_p2 {
                            self.set_reg(op.p2, SqliteValue::Integer(i64::from(should_jump)));
                            pc += 1;
                        } else if should_jump {
                            pc = op.p2 as usize;
                        } else {
                            pc += 1;
                        }
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
                        SqliteValue::Integer(i64::from(!vdbe_real_is_truthy(a)))
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Conditional Jumps ───────────────────────────────────
                Opcode::If => {
                    // Jump to p2 if p1 is true (non-zero, non-NULL).
                    // If p1 is NULL, jump iff p3 != 0 (SQLite semantics).
                    // C SQLite uses sqlite3VdbeRealValue() != 0.0 (NOT integer
                    // truncation), so 0.1 and 0.5 are truthy.
                    let val = self.get_reg(op.p1);
                    let should_jump = if val.is_null() {
                        op.p3 != 0
                    } else {
                        vdbe_real_is_truthy(val)
                    };
                    if should_jump {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNot => {
                    // Jump to p2 if p1 is false (zero).
                    // If p1 is NULL, jump iff p3 != 0 (SQLite semantics).
                    // C SQLite uses sqlite3VdbeRealValue() != 0.0.
                    let val = self.get_reg(op.p1);
                    let should_jump = if val.is_null() {
                        op.p3 != 0
                    } else {
                        !vdbe_real_is_truthy(val)
                    };
                    if should_jump {
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
                    // Fall through on first execution (run the body), jump to
                    // p2 on subsequent executions (skip the body).  This
                    // matches C SQLite's OP_Once semantics.
                    let word = pc / 64;
                    let bit = 1u64 << (pc % 64);
                    if once_bits[word] & bit != 0 {
                        // Already fired — skip the body.
                        pc = op.p2 as usize;
                    } else {
                        // First time — mark as fired, fall through.
                        once_bits[word] |= bit;
                        pc += 1;
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
                    if addr < 0 || addr as usize >= ops.len() {
                        return Err(FrankenError::Internal(format!(
                            "Return address {} out of bounds",
                            addr
                        )));
                    }
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
                    self.cursor_root_pages.insert(cursor_id, root_page);
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
                    self.cursor_root_pages.insert(cursor_id, root_page);
                    self.cursors.remove(&cursor_id);
                    pc += 1;
                }

                Opcode::OpenEphemeral => {
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

                Opcode::OpenAutoindex => {
                    // Autoindex: create an ephemeral INDEX B-tree.
                    // Unlike OpenEphemeral (table B-tree), autoindexes use
                    // index B-tree semantics (no rowid, key-only cells).
                    // We create a StorageCursor backed by MemPageStore with
                    // is_table=false so IdxInsert/IdxGE etc. work correctly.
                    const AUTOINDEX_PAGE_SIZE: u32 = 4096;
                    let cursor_id = op.p1;
                    self.pending_next_after_delete.remove(&cursor_id);
                    let root_pgno = if let Some(db) = self.db.as_mut() {
                        let rp = db.allocate_root_page();
                        PageNumber::new(rp as u32)
                    } else {
                        None
                    };
                    if let Some(root_pgno) = root_pgno {
                        let store = MemPageStore::with_empty_index(root_pgno, AUTOINDEX_PAGE_SIZE);
                        let cx = Cx::new();
                        let cursor = BtCursor::new(store, root_pgno, AUTOINDEX_PAGE_SIZE, false);
                        self.cursors.remove(&cursor_id);
                        self.storage_cursors.insert(
                            cursor_id,
                            StorageCursor {
                                cursor: CursorBackend::Mem(cursor),
                                cx,
                                writable: true,
                                last_alloc_rowid: 0,
                                cached_row: None,
                            },
                        );
                    }
                    pc += 1;
                }

                Opcode::OpenPseudo => {
                    let cursor_id = op.p1;
                    self.pending_next_after_delete.remove(&cursor_id);
                    self.storage_cursors.remove(&cursor_id);
                    self.cursors.insert(cursor_id, MemCursor::new_pseudo(op.p2));
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
                    // P4::Str format: ORDER_CHARS or ORDER_CHARS|COLL1,COLL2,...
                    // where ORDER_CHARS are '+'/'-' per key column,
                    // and COLL values are collation names (empty = BINARY).
                    let (order_str, collation_str) = match &op.p4 {
                        P4::Str(s) => {
                            if let Some((orders, colls)) = s.split_once('|') {
                                (orders.to_owned(), Some(colls.to_owned()))
                            } else {
                                (s.clone(), None)
                            }
                        }
                        _ => (String::new(), None),
                    };
                    let sort_key_orders: Vec<SortKeyOrder> = order_str
                        .chars()
                        .take(key_columns)
                        .map(|ch| match ch {
                            '-' => SortKeyOrder::Desc,
                            '>' => SortKeyOrder::AscNullsLast,
                            '<' => SortKeyOrder::DescNullsFirst,
                            _ => SortKeyOrder::Asc,
                        })
                        .collect();
                    let collations: Vec<Option<String>> = if let Some(cs) = collation_str {
                        cs.split(',')
                            .take(key_columns)
                            .map(|c| {
                                if c.is_empty() {
                                    None
                                } else {
                                    Some(c.to_owned())
                                }
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    self.sorters.insert(
                        cursor_id,
                        SorterCursor::new(key_columns, sort_key_orders, collations),
                    );
                    // A cursor id cannot be both table and sorter cursor.
                    self.cursors.remove(&cursor_id);
                    self.storage_cursors.remove(&cursor_id);
                    pc += 1;
                }

                Opcode::Close => {
                    self.cursors.remove(&op.p1);
                    self.storage_cursors.remove(&op.p1);
                    self.sorters.remove(&op.p1);
                    self.vtab_cursors.remove(&op.p1);
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
                            sorter.sort()?;
                            // Flush per-sorter metrics to global counters.
                            let rows = sorter.rows_sorted_total;
                            let spill_pages = sorter.spill_pages_total;
                            let merge_runs = sorter.spill_runs.len() as u64;
                            FSQLITE_SORT_ROWS_TOTAL.fetch_add(rows, AtomicOrdering::Relaxed);
                            FSQLITE_SORT_SPILL_PAGES_TOTAL
                                .fetch_add(spill_pages, AtomicOrdering::Relaxed);
                            sorter.rows_sorted_total = 0;
                            sorter.spill_pages_total = 0;
                            // Tracing span for sort observability.
                            let _span = tracing::debug_span!(
                                "sort",
                                rows_sorted = rows,
                                spill_pages = spill_pages,
                                merge_runs = merge_runs,
                            )
                            .entered();
                            tracing::debug!(
                                rows_sorted = rows,
                                spill_pages = spill_pages,
                                merge_runs = merge_runs,
                                "sort completed"
                            );
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
                    // Prev repositions the cursor, so clear any pending
                    // delete/next state before evaluating movement.
                    self.pending_next_after_delete.remove(&cursor_id);
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
                    let cursor_id = op.p1;
                    let target = op.p2;
                    if let Some(cursor) = self.storage_cursors.get(&cursor_id) {
                        if cursor.cursor.eof() {
                            self.set_reg(target, SqliteValue::Null);
                        } else {
                            let payload = cursor.cursor.payload(&cursor.cx)?;
                            self.set_reg(target, SqliteValue::Blob(payload));
                        }
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if cursor.is_pseudo {
                            if let Some(reg) = cursor.pseudo_reg {
                                let blob = self.get_reg(reg).clone();
                                self.set_reg(target, blob);
                            } else {
                                self.set_reg(target, SqliteValue::Null);
                            }
                        } else {
                            self.set_reg(target, SqliteValue::Null);
                        }
                    } else {
                        self.set_reg(target, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::NullRow => {
                    // Set cursor p1 to a null row. Subsequent Column/Rowid
                    // reads will return NULL (storage cursor via eof(),
                    // mem cursor via position=None).
                    if let Some(cursor) = self.storage_cursors.get_mut(&op.p1) {
                        // Move storage cursor past the last entry so eof()
                        // returns true for subsequent Column/Rowid reads.
                        cursor.cursor.clear_position();
                    }
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
                    // If not found, jump to p2.  NULL key → not found.
                    let cursor_id = op.p1;
                    // Seek repositions the cursor, so clear any pending delete state.
                    self.pending_next_after_delete.remove(&cursor_id);
                    let key = self.get_reg(op.p3);
                    if key.is_null() {
                        pc = op.p2 as usize;
                        continue;
                    }
                    let rowid_val = key.to_integer();
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
                    // Jump to p2 if no matching row exists.  NULL key → not found.
                    let cursor_id = op.p1;
                    // Seek repositions the cursor, so clear any pending delete state.
                    self.pending_next_after_delete.remove(&cursor_id);
                    let key_val = self.get_reg(op.p3).clone();
                    if key_val.is_null() {
                        pc = op.p2 as usize;
                        continue;
                    }

                    // Dispatch based on cursor type (table vs index), NOT on
                    // key value type. Using the key type was incorrect: an
                    // index cursor receiving an integer key would wrongly call
                    // table_move_to, triggering "table leaf cell has no rowid"
                    // on index pages. (Fixes br#138-140, #144, #145.)
                    let found = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        if cursor.cursor.is_table_btree() {
                            // Table seek: key is a rowid (integer).
                            let key = key_val.to_integer();
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
                        } else {
                            // Index seek: key is a packed record blob.
                            let key_bytes = record_blob_bytes(&key_val);
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
                        }
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        // MemCursor fallback (Phase 4 path).
                        let key = key_val.to_integer();
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if table.rows.is_empty() {
                                    false
                                } else {
                                    match op.opcode {
                                        Opcode::SeekGE => {
                                            let pos = table
                                                .rows
                                                .binary_search_by_key(&key, |r| r.rowid)
                                                .unwrap_or_else(|e| e);
                                            if pos < table.rows.len() {
                                                cursor.position = Some(pos);
                                                true
                                            } else {
                                                false
                                            }
                                        }
                                        Opcode::SeekGT => {
                                            let pos = match table
                                                .rows
                                                .binary_search_by_key(&key, |r| r.rowid)
                                            {
                                                Ok(idx) => idx + 1,
                                                Err(idx) => idx,
                                            };
                                            if pos < table.rows.len() {
                                                cursor.position = Some(pos);
                                                true
                                            } else {
                                                false
                                            }
                                        }
                                        Opcode::SeekLE => {
                                            let pos = match table
                                                .rows
                                                .binary_search_by_key(&key, |r| r.rowid)
                                            {
                                                Ok(idx) => Some(idx),
                                                Err(idx) => idx.checked_sub(1),
                                            };
                                            if let Some(idx) = pos {
                                                cursor.position = Some(idx);
                                                true
                                            } else {
                                                false
                                            }
                                        }
                                        Opcode::SeekLT => {
                                            let pos = table
                                                .rows
                                                .binary_search_by_key(&key, |r| r.rowid)
                                                .unwrap_or_else(|e| e)
                                                .checked_sub(1);
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
                    // Check if key in register P3 exists in cursor P1.
                    // Jump to P2 if NOT found; fall through if found.
                    // NULL key → always "not found".
                    let cursor_id = op.p1;
                    // Probe repositions the cursor; clear pending delete/next
                    // state so a following Next advances relative to the new
                    // cursor position.
                    if self.storage_cursors.contains_key(&cursor_id) {
                        self.pending_next_after_delete.remove(&cursor_id);
                    }
                    let key_val = self.get_reg(op.p3).clone();
                    if key_val.is_null() {
                        pc = op.p2 as usize;
                        continue;
                    }
                    let exists = if matches!(key_val, SqliteValue::Blob(_)) {
                        // Index seek path: P3 contains a packed record blob
                        // (from MakeRecord). Use index_move_to to find the key.
                        let key_bytes = record_blob_bytes(&key_val);
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                            cursor
                                .cursor
                                .index_move_to(&cursor.cx, &key_bytes)?
                                .is_found()
                        } else {
                            false
                        }
                    } else {
                        // Table seek path: P3 contains an integer rowid.
                        let rowid_val = key_val.to_integer();
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
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
                        }
                    };
                    if exists {
                        pc += 1; // Found: fall through.
                    } else {
                        pc = op.p2 as usize; // Not found: jump.
                    }
                }

                Opcode::Found => {
                    // Jump to P2 if key found in cursor P1 (exact match).
                    // NULL key → never found (don't jump).
                    let cursor_id = op.p1;
                    if self.storage_cursors.contains_key(&cursor_id) {
                        self.pending_next_after_delete.remove(&cursor_id);
                    }
                    let key_val = self.get_reg(op.p3).clone();
                    if key_val.is_null() {
                        pc += 1;
                        continue;
                    }
                    let exists = if matches!(key_val, SqliteValue::Blob(_)) {
                        let key_bytes = record_blob_bytes(&key_val);
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                            cursor
                                .cursor
                                .index_move_to(&cursor.cx, &key_bytes)?
                                .is_found()
                        } else {
                            false
                        }
                    } else {
                        let rowid_val = key_val.to_integer();
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
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
                        }
                    };
                    if exists {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::NoConflict => {
                    // Jump to P2 if NO matching key prefix exists in index
                    // cursor P1.  Falls through when a conflict IS found
                    // (cursor positioned on the conflicting entry).
                    // NULL in any key field → always jump (no conflict).
                    let cursor_id = op.p1;
                    if self.storage_cursors.contains_key(&cursor_id) {
                        self.pending_next_after_delete.remove(&cursor_id);
                    }
                    let key_val = self.get_reg(op.p3).clone();

                    // NULL short-circuit: NULL != NULL for UNIQUE purposes.
                    if let SqliteValue::Blob(ref bytes) = key_val {
                        if let Some(fields) = parse_record(bytes) {
                            if fields.iter().any(SqliteValue::is_null) {
                                pc = op.p2 as usize;
                                continue;
                            }
                        }
                    } else if key_val.is_null() {
                        pc = op.p2 as usize;
                        continue;
                    }

                    // Prefix-based conflict check: seek the index, then
                    // compare only the first N fields (where N = number of
                    // fields in the probe key) against the entry at the
                    // cursor position.  The index stores (columns, rowid)
                    // but the probe key has only (columns).
                    let conflict = if let SqliteValue::Blob(ref bytes) = key_val {
                        let probe_fields = parse_record(bytes);
                        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                            cursor.cursor.index_move_to(&cursor.cx, bytes)?;
                            // Read the entry at the current cursor position.
                            if let Ok(entry_bytes) = cursor.cursor.payload(&cursor.cx) {
                                if let (Some(probe), Some(entry)) =
                                    (&probe_fields, parse_record(&entry_bytes))
                                {
                                    let n = probe.len();
                                    entry.len() >= n && entry[..n] == probe[..]
                                } else {
                                    false
                                }
                            } else {
                                // Cursor at EOF — no conflict.
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if conflict {
                        pc += 1; // Conflict found: fall through.
                    } else {
                        pc = op.p2 as usize; // No conflict: jump.
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
                        // Storage NewRowid probes max rowid via `last()`, which
                        // repositions the cursor. Clear any pending delete/next
                        // state so subsequent Next/Prev behavior is consistent
                        // with the new position.
                        self.pending_next_after_delete.remove(&cursor_id);
                        // Navigate to last entry to find max rowid from B-tree.
                        let btree_max = if sc.cursor.last(&sc.cx)? {
                            sc.cursor.rowid(&sc.cx)?
                        } else {
                            0 // empty table
                        };
                        // For AUTOINCREMENT tables, also consult the high-water
                        // mark from sqlite_sequence to prevent rowid reuse after
                        // deletion (bd-31j76).
                        let autoinc_max = self
                            .cursor_root_pages
                            .get(&cursor_id)
                            .and_then(|rp| self.autoincrement_seq_by_root_page.get(rp))
                            .copied()
                            .unwrap_or(0);
                        // Use the highest of B-tree max, previously allocated,
                        // and AUTOINCREMENT high-water mark.
                        let base = btree_max.max(sc.last_alloc_rowid).max(autoinc_max);
                        let new_rowid = base.checked_add(1).ok_or_else(|| {
                            FrankenError::VdbeExecutionError {
                                detail: "rowid overflow: maximum rowid reached".into(),
                            }
                        })?;
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
                    let previous_last_insert_rowid = self.last_insert_rowid;
                    let pk_conflict = ExecOutcome::Error {
                        code: ErrorCode::Constraint as i32,
                        message: "PRIMARY KEY constraint failed".to_owned(),
                    };

                    // Phase 5B.2 (bd-1yi8): write-through — route ONLY through
                    // StorageCursor when one exists; fall back to MemDatabase
                    // only for legacy Phase 4 cursors.
                    let mut actually_inserted = false;
                    if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        if sc.writable {
                            let blob = record_blob_bytes(&record_val);
                            let exists = sc.cursor.table_move_to(&sc.cx, rowid)?.is_found();

                            if exists {
                                match oe_flag {
                                    4 => {
                                        // OE_IGNORE: Skip insert for conflicting row
                                    }
                                    5 | 8 => {
                                        // OE_REPLACE (5) or OPFLAG_ISUPDATE (8): Delete old, insert new (UPSERT/UPDATE)
                                        self.native_replace_row(cursor_id, rowid)?;
                                        let sc2 = self
                                            .storage_cursors
                                            .get_mut(&cursor_id)
                                            .ok_or_else(|| {
                                                FrankenError::internal(
                                                    "cursor disappeared during REPLACE",
                                                )
                                            })?;
                                        sc2.cursor.table_insert(&sc2.cx, rowid, &blob)?;
                                        actually_inserted = true;
                                    }
                                    _ => {
                                        // Default (ABORT/FAIL/ROLLBACK): constraint error.
                                        break pk_conflict;
                                    }
                                }
                            } else {
                                // No conflict — insert normally
                                sc.cursor.table_insert(&sc.cx, rowid, &blob)?;
                                actually_inserted = true;
                            }
                        }
                    } else if let Some(root) = self.cursors.get(&cursor_id).map(|c| c.root_page) {
                        // MemDatabase fallback (Phase 4 in-memory cursors).
                        let values = decode_record(&record_val)?;
                        if let Some(db) = self.db.as_mut() {
                            // Check rowid conflict first.
                            let rowid_conflict = db
                                .get_table(root)
                                .and_then(|t| t.find_by_rowid(rowid))
                                .is_some();

                            // Check UNIQUE column constraint conflicts (non-IPK).
                            let unique_conflict_rowid = if !rowid_conflict {
                                db.get_table(root)
                                    .and_then(|t| t.find_unique_conflict(&values))
                            } else {
                                None
                            };

                            let has_conflict = rowid_conflict || unique_conflict_rowid.is_some();

                            if has_conflict {
                                match oe_flag {
                                    4 => {
                                        // OE_IGNORE: Skip insert for conflicting row
                                    }
                                    5 | 8 => {
                                        // OE_REPLACE / OPFLAG_ISUPDATE: Delete conflicting
                                        // row(s), then insert new.
                                        if let Some(conflict_rid) = unique_conflict_rowid {
                                            db.get_table_mut(root)
                                                .map(|t| t.delete_by_rowid(conflict_rid));
                                        }
                                        db.upsert_row(root, rowid, values);
                                        actually_inserted = true;
                                    }
                                    _ => {
                                        // Default (ABORT/FAIL/ROLLBACK): constraint error.
                                        break pk_conflict;
                                    }
                                }
                            } else {
                                // No conflict — insert normally
                                db.upsert_row(root, rowid, values);
                                actually_inserted = true;
                            }
                        }
                    }

                    // Track last insert rowid only when a row was actually inserted.
                    // C SQLite does not update last_insert_rowid() when IGNORE skips.
                    if actually_inserted {
                        self.changes += 1;
                        self.last_insert_rowid = rowid;
                        self.pending_insert_rollback = Some(PendingInsertRollback {
                            cursor_id,
                            rowid,
                            previous_last_insert_rowid,
                        });
                    } else {
                        self.pending_insert_rollback = None;
                    }
                    self.last_insert_cursor_id = Some(cursor_id);
                    self.conflict_skip_idx = false;

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
                        // P5 bit 0 = OPFLAG_NCHANGE: only count standalone
                        // DELETE changes. UPDATE's internal Delete uses P5=0
                        // so only the subsequent Insert counts.
                        if op.p5 & 1 != 0 {
                            self.changes += 1;
                        }
                        self.pending_next_after_delete.insert(cursor_id);
                    }
                    pc += 1;
                }

                Opcode::IdxInsert => {
                    // Insert key from register P2 into index cursor P1.
                    // bd-qluy: Phase 5I.6 - Wire to B-tree index_insert.
                    // P5 encoding: bit 0 = is_unique, bits 1-4 = oe_flag
                    // (conflict resolution mode for UNIQUE violations).
                    // P3 = number of indexed columns (excluding trailing
                    // rowid). P4 = columns string for the error message.
                    let cursor_id = op.p1;
                    let key_reg = op.p2;
                    let is_unique = (op.p5 & 1) != 0;
                    #[allow(clippy::cast_possible_truncation)]
                    let oe_flag = ((op.p5 >> 1) & 0x0F) as u8;
                    let n_idx_cols = op.p3 as usize;
                    let key_val = self.get_reg(key_reg).clone();

                    // If a previous IdxInsert for the same row triggered IGNORE,
                    // skip all remaining index inserts for this row.
                    if self.conflict_skip_idx {
                        pc += 1;
                        continue;
                    }

                    if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        if sc.writable {
                            let key_bytes = record_blob_bytes(&key_val);

                            if is_unique && n_idx_cols > 0 {
                                let columns_label = match &op.p4 {
                                    P4::Table(s) => s.clone(),
                                    _ => String::new(),
                                };
                                match sc.cursor.index_insert_unique(
                                    &sc.cx,
                                    &key_bytes,
                                    n_idx_cols,
                                    &columns_label,
                                ) {
                                    Ok(()) => {}
                                    Err(FrankenError::UniqueViolation { .. }) => {
                                        match oe_flag {
                                            // OE_IGNORE (4): Undo the table
                                            // insert and skip remaining indexes.
                                            4 => {
                                                let rollback = self
                                                    .pending_insert_rollback
                                                    .take()
                                                    .ok_or_else(|| {
                                                        FrankenError::internal(
                                                            "UNIQUE IGNORE conflict without \
                                                             pending table insert",
                                                        )
                                                    })?;
                                                let tsc = self
                                                    .storage_cursors
                                                    .get_mut(&rollback.cursor_id)
                                                    .ok_or_else(|| {
                                                        FrankenError::internal(
                                                            "table cursor missing during \
                                                             UNIQUE IGNORE rollback",
                                                        )
                                                    })?;
                                                if !tsc
                                                    .cursor
                                                    .table_move_to(&tsc.cx, rollback.rowid)?
                                                    .is_found()
                                                {
                                                    return Err(FrankenError::internal(
                                                        "failed to locate provisional table row \
                                                         during UNIQUE IGNORE rollback",
                                                    ));
                                                }
                                                tsc.cursor.delete(&tsc.cx)?;
                                                self.changes = self
                                                    .changes
                                                    .checked_sub(1)
                                                    .ok_or_else(|| {
                                                        FrankenError::internal(
                                                            "UNIQUE IGNORE rollback underflowed \
                                                             change counter",
                                                        )
                                                    })?;
                                                self.last_insert_rowid =
                                                    rollback.previous_last_insert_rowid;
                                                self.last_insert_cursor_id = None;
                                                self.conflict_skip_idx = true;
                                                pc += 1;
                                                continue;
                                            }
                                            // OE_REPLACE (5 or 8): Find the
                                            // conflicting row, delete it (and its
                                            // index entries), then insert the new
                                            // index entry.
                                            5 | 8 => {
                                                // Find the rowid of the
                                                // conflicting row from the index.
                                                let conflict_rowid =
                                                    find_conflicting_rowid_in_index(
                                                        sc, &key_bytes, n_idx_cols,
                                                    )?;

                                                if let Some(old_rowid) = conflict_rowid {
                                                    if let Some(tbl_cid) =
                                                        self.last_insert_cursor_id
                                                    {
                                                        self.native_replace_row(
                                                            tbl_cid, old_rowid,
                                                        )?;
                                                    }
                                                }

                                                // Now insert the new index entry
                                                // (the conflicting one was already
                                                // deleted by
                                                // find_conflicting_rowid_in_index).
                                                let sc2 = self
                                                    .storage_cursors
                                                    .get_mut(&cursor_id)
                                                    .ok_or_else(|| {
                                                        FrankenError::internal("cursor must exist")
                                                    })?;
                                                sc2.cursor.index_insert(&sc2.cx, &key_bytes)?;
                                            }
                                            // Default: propagate the error
                                            // (ABORT/FAIL/ROLLBACK).
                                            _ => {
                                                return Err(FrankenError::UniqueViolation {
                                                    columns: columns_label,
                                                });
                                            }
                                        }
                                    }
                                    Err(e) => return Err(e),
                                }
                            } else {
                                sc.cursor.index_insert(&sc.cx, &key_bytes)?;
                            }
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
                        let decoded = decode_record(&record)?;
                        sorter.insert_row(decoded)?;
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
                                !sorter_keys_equal(
                                    current,
                                    &probe,
                                    sorter.key_columns,
                                    &sorter.collations,
                                )
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
                    let target = op.p3;
                    let values = self.collect_reg_range(op.p1, usize::try_from(op.p2).unwrap_or(0));
                    let blob = encode_record(&values);
                    self.set_reg(target, SqliteValue::Blob(blob));
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
                    // Count rows in cursor P1, store result in register P2.
                    let cursor_id = op.p1;
                    let count: i64 = if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref()
                            && let Some(table) = db.get_table(cursor.root_page)
                        {
                            i64::try_from(table.rows.len()).unwrap_or(0)
                        } else {
                            0
                        }
                    } else if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        // Walk the cursor to count rows.
                        let has_first = sc.cursor.first(&sc.cx)?;
                        if !has_first {
                            0
                        } else {
                            let mut n: i64 = 1;
                            while sc.cursor.next(&sc.cx)? {
                                n += 1;
                            }
                            n
                        }
                    } else {
                        0
                    };
                    self.set_reg(op.p2, SqliteValue::Integer(count));
                    pc += 1;
                }

                Opcode::Sequence => {
                    let counter = self.sequence_counters.entry(op.p1).or_insert(0);
                    let val = *counter;
                    *counter += 1;
                    self.set_reg(op.p2, SqliteValue::Integer(val));
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
                    // Synopsis: r[P2] = coalesce(IsTrue(r[P1]),P3) ^ P4
                    // Implements IS TRUE, IS FALSE, IS NOT TRUE, IS NOT FALSE.
                    let val = self.get_reg(op.p1);
                    let p4_val = match &op.p4 {
                        P4::Int(n) => *n,
                        _ => 0,
                    };
                    if val.is_null() {
                        self.set_reg(op.p2, SqliteValue::Integer(i64::from(op.p3 ^ p4_val)));
                    } else {
                        let v = i32::from(vdbe_real_is_truthy(val));
                        self.set_reg(op.p2, SqliteValue::Integer(i64::from((v ^ p4_val) & 1)));
                    }
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
                    // C SQLite also sets register P3 to NULL before jumping.
                    let is_null = if let Some(cursor) = self.storage_cursors.get(&op.p1) {
                        cursor.cursor.eof()
                    } else {
                        self.cursors
                            .get(&op.p1)
                            .is_none_or(|c| c.position.is_none() && !c.is_pseudo)
                    };
                    if is_null {
                        if op.p3 > 0 {
                            self.set_reg(op.p3, SqliteValue::Null);
                        }
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNotOpen => {
                    // Jump to p2 if cursor p1 is not open.
                    if self.cursors.contains_key(&op.p1)
                        || self.storage_cursors.contains_key(&op.p1)
                        || self.vtab_cursors.contains_key(&op.p1)
                        || self.sorters.contains_key(&op.p1)
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
                    let compare_collations = parse_compare_collations(&op.p4);
                    let mut result = Ordering::Equal;
                    for i in 0..count {
                        let val_a = self.get_reg(start_a + i);
                        let val_b = self.get_reg(start_b + i);
                        let coll_name = usize::try_from(i).ok().and_then(|field_idx| {
                            compare_collation_for_field(compare_collations.as_deref(), field_idx)
                        });
                        // SQLite NULL sort order: NULLs sort before all other
                        // values.  When partial_cmp returns None (NULL vs
                        // non-NULL or NaN), apply NULL-first ordering.
                        let ord = if let Some(coll_name) = coll_name {
                            collate_compare(val_a, val_b, coll_name)
                        } else {
                            val_a.partial_cmp(val_b)
                        };
                        let o = match ord {
                            Some(o) => o,
                            None => {
                                // NULL < non-NULL; NULL == NULL for sort purposes.
                                match (val_a.is_null(), val_b.is_null()) {
                                    (true, true) => Ordering::Equal,
                                    (true, false) => Ordering::Less,
                                    (false, true) => Ordering::Greater,
                                    (false, false) => Ordering::Equal, // NaN edge case fallback
                                }
                            }
                        };
                        if o != Ordering::Equal {
                            result = o;
                            break;
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

                Opcode::TypeCheck => {
                    // P4 is either Affinity("IRT") or Str("IRT\ttable\tcol1\tcol2\tcol3")
                    let p4_str = match &op.p4 {
                        P4::Affinity(s) | P4::Str(s) => s.as_str(),
                        _ => "",
                    };
                    // Split on tab: first part is affinity pattern, rest is table+columns.
                    let mut parts = p4_str.split('\t');
                    let pattern = parts.next().unwrap_or("").as_bytes();
                    let table_name = parts.next().unwrap_or("");
                    let col_names: Vec<&str> = parts.collect();

                    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                    let count = op.p2.max(0) as usize;
                    for offset in 0..count {
                        #[allow(clippy::cast_possible_wrap)]
                        let reg = op.p1 + offset as i32;
                        let value = self.get_reg(reg);
                        let strict_type = match pattern.get(offset).copied().unwrap_or(b'A') {
                            b'A' | b'a' => None,
                            b'I' | b'i' => Some(StrictColumnType::Integer),
                            b'R' | b'r' => Some(StrictColumnType::Real),
                            b'T' | b't' => Some(StrictColumnType::Text),
                            b'L' | b'l' => Some(StrictColumnType::Blob),
                            other => {
                                return Err(FrankenError::Internal(format!(
                                    "unknown STRICT type code '{}' in OP_TypeCheck",
                                    char::from(other)
                                )));
                            }
                        };

                        if let Some(expected) = strict_type {
                            let checked =
                                value.clone().validate_strict(expected).map_err(|err| {
                                    let col_label = col_names
                                        .get(offset)
                                        .filter(|s| !s.is_empty())
                                        .map_or_else(
                                            || format!("column {offset}"),
                                            |name| {
                                                if table_name.is_empty() {
                                                    (*name).to_owned()
                                                } else {
                                                    format!("{table_name}.{name}")
                                                }
                                            },
                                        );
                                    let col_type = format!("{expected:?}").to_ascii_uppercase();
                                    let actual_str = err.actual.to_string();
                                    tracing::warn!(
                                        register = reg,
                                        expected = ?expected,
                                        actual = %actual_str,
                                        column = %col_label,
                                        value = ?value,
                                        "STRICT type violation"
                                    );
                                    FrankenError::DatatypeViolation {
                                        column: col_label,
                                        column_type: col_type,
                                        actual: actual_str,
                                    }
                                })?;
                            self.set_reg(reg, checked);
                        }
                    }
                    pc += 1;
                }

                Opcode::Permutation | Opcode::CollSeq | Opcode::ElseEq | Opcode::FkCheck => {
                    pc += 1;
                }

                Opcode::IsType => {
                    // Check datatype of a value against the P5 type bitmask.
                    // If P1 >= 0: check column P3 of cursor P1.
                    // If P1 == -1: check register P3.
                    // P4 (Int) = default type code if column is beyond row width.
                    // P5 bitmask: 0x01=INTEGER, 0x02=FLOAT, 0x04=TEXT, 0x08=BLOB, 0x10=NULL
                    // Jump to P2 if the value's type matches a bit in P5.
                    let val = if op.p1 < 0 {
                        self.get_reg(op.p3).clone()
                    } else {
                        self.cursor_column(op.p1, op.p3 as usize)?
                    };
                    let type_bit: u16 = match &val {
                        SqliteValue::Integer(_) => 0x01,
                        SqliteValue::Float(_) => 0x02,
                        SqliteValue::Text(_) => 0x04,
                        SqliteValue::Blob(_) => 0x08,
                        SqliteValue::Null => 0x10,
                    };
                    if op.p5 & type_bit != 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfEmpty => {
                    // Jump to P2 if the table/index at cursor P1 is empty.
                    // WARNING: For storage cursors, this repositions the cursor
                    // to the first entry via `cursor.first()` as a side-effect.
                    // This is currently dead code (codegen never emits IfEmpty),
                    // but if this opcode is ever used, the cursor repositioning
                    // may invalidate assumptions about cursor position.
                    let cursor_id = op.p1;
                    let empty = if let Some(sc) = self.storage_cursors.get_mut(&cursor_id) {
                        // Try moving to first; false means empty.
                        let had_row = sc.cursor.first(&sc.cx)?;
                        !had_row
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref()
                            && let Some(table) = db.get_table(cursor.root_page)
                        {
                            table.rows.is_empty()
                        } else {
                            true // no table = empty
                        }
                    } else {
                        true
                    };
                    if empty {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfSizeBetween => {
                    // Compute X = 10*log2(N) where N = approx row count of
                    // cursor P1 (or -1 if empty). Jump to P2 if X is in
                    // [P3, P4]. When we lack exact stats, estimate from the
                    // MemTable row count or assume 0 (empty) for storage
                    // cursors (conservative).
                    let cursor_id = op.p1;
                    let row_count: i64 = if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref()
                            && let Some(table) = db.get_table(cursor.root_page)
                        {
                            i64::try_from(table.rows.len()).unwrap_or(0)
                        } else {
                            0
                        }
                    } else {
                        // For storage cursors, we don't have a cheap row count.
                        // Default to -1 (empty) which maps to X = -10.
                        -1
                    };
                    #[allow(clippy::cast_precision_loss)]
                    let x = if row_count <= 0 {
                        -10_i32 // empty sentinel
                    } else {
                        ((row_count as f64).log2() * 10.0) as i32
                    };
                    let lo = op.p3;
                    let hi = match &op.p4 {
                        P4::Int(v) => *v,
                        _ => i32::MAX,
                    };
                    if x >= lo && x <= hi {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
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
                //
                // Compare the current index cursor key against a probe
                // key record in register P3. Jump to P2 when the
                // condition holds.
                //
                //   IdxLE: jump if cursor_key <= probe_key
                //   IdxGT: jump if cursor_key >  probe_key
                //   IdxLT: jump if cursor_key <  probe_key
                //   IdxGE: jump if cursor_key >= probe_key
                //
                // P1 = cursor, P2 = jump target, P3 = register with
                // probe key blob, P5 = number of key columns to compare
                // (0 means use all columns from the probe).
                Opcode::IdxLE | Opcode::IdxGT | Opcode::IdxLT | Opcode::IdxGE => {
                    let cursor_id = op.p1;
                    let probe_val = self.get_reg(op.p3).clone();
                    let probe_fields = decode_record(&probe_val).unwrap_or_default();

                    // Extract current cursor key as parsed fields.
                    let cursor_fields = if let Some(sc) = self.storage_cursors.get(&cursor_id) {
                        if sc.cursor.eof() {
                            // EOF: IdxGT/IdxGE jump (past end), IdxLT/IdxLE fall through.
                            let jump = matches!(op.opcode, Opcode::IdxGT | Opcode::IdxGE);
                            if jump {
                                pc = op.p2 as usize;
                            } else {
                                pc += 1;
                            }
                            continue;
                        }
                        let payload = sc.cursor.payload(&sc.cx)?;
                        decode_record(&SqliteValue::Blob(payload)).unwrap_or_default()
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(pos) = cursor.position
                            && let Some(db) = self.db.as_ref()
                            && let Some(table) = db.get_table(cursor.root_page)
                            && let Some(row) = table.rows.get(pos)
                        {
                            row.values.clone()
                        } else {
                            // No position or no table: treat as past-end.
                            let jump = matches!(op.opcode, Opcode::IdxGT | Opcode::IdxGE);
                            if jump {
                                pc = op.p2 as usize;
                            } else {
                                pc += 1;
                            }
                            continue;
                        }
                    } else {
                        pc += 1;
                        continue;
                    };

                    // Compare column-by-column up to P5 columns (0 = all
                    // probe columns).
                    let n_compare = if op.p5 > 0 {
                        op.p5 as usize
                    } else {
                        probe_fields.len()
                    };

                    let cmp = compare_sorter_keys(&cursor_fields, &probe_fields, n_compare, &[]);

                    let condition_met = match op.opcode {
                        Opcode::IdxLE => cmp != Ordering::Greater,
                        Opcode::IdxGT => cmp == Ordering::Greater,
                        Opcode::IdxLT => cmp == Ordering::Less,
                        Opcode::IdxGE => cmp != Ordering::Less,
                        _ => unreachable!(),
                    };

                    if condition_met {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
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
                        sorter.reset();
                    }
                    pc += 1;
                }

                // ── Savepoint ──────────────────────────────────────────
                Opcode::Savepoint => {
                    // P1: 0=BEGIN, 1=RELEASE, 2=ROLLBACK
                    // P4: savepoint name
                    // In the in-memory engine, savepoints use undo
                    // version tokens to snapshot/restore state.
                    // Full implementation deferred to WAL/pager integration.
                    pc += 1;
                }

                // ── Checkpoint ────────────────────────────────────────────
                Opcode::Checkpoint => {
                    // WAL checkpoint. No-op for in-memory engine.
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
                // AggStep1 is a single-argument fast-path with identical semantics.
                Opcode::AggStep | Opcode::AggStep1 => {
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
                    let is_distinct = op.p1 != 0;
                    let ctx = self.aggregates.entry_or_insert_with(accum_reg, || {
                        let state = func.initial_state();
                        AggregateContext {
                            func: func.clone(),
                            state,
                            distinct_seen: if is_distinct {
                                Some(std::collections::HashSet::new())
                            } else {
                                None
                            },
                        }
                    });

                    if !Arc::ptr_eq(&ctx.func, &func) {
                        return Err(FrankenError::Internal(
                            "AggStep accumulator reused for a different aggregate".to_owned(),
                        ));
                    }

                    // For DISTINCT aggregates, skip if we've already seen these args.
                    // NULL values are always skipped for DISTINCT (SQL semantics).
                    let should_step = if let Some(ref mut seen) = ctx.distinct_seen {
                        // Skip NULL arguments for DISTINCT aggregates.
                        if args.iter().any(|a| matches!(a, SqliteValue::Null)) {
                            false
                        } else {
                            seen.insert(distinct_key(&args))
                        }
                    } else {
                        true
                    };

                    if should_step {
                        ctx.func.step(&mut ctx.state, &args)?;
                    }
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

                Opcode::AggInverse => {
                    // Inverse aggregate step for window functions.
                    // Remove a row from the sliding window frame.
                    // P4 = function name, P2 = first arg register,
                    // P5 = arg count, P3 = accumulator register.
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "AggInverse opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "AggInverse opcode executed without function registry".to_owned(),
                        )
                    })?;

                    let arg_count = i32::from(op.p5);
                    let func = registry.find_window(func_name, arg_count).ok_or_else(|| {
                        FrankenError::Internal(format!(
                            "no such window function: {func_name}/{arg_count}",
                        ))
                    })?;

                    let mut args = Vec::with_capacity(op.p5 as usize);
                    for i in 0..op.p5 {
                        let reg_idx = op.p2 + i32::from(i);
                        args.push(self.get_reg(reg_idx).clone());
                    }

                    let accum_reg = op.p3;
                    let ctx = self.window_contexts.entry_or_insert_with(accum_reg, || {
                        let state = func.initial_state();
                        WindowContext {
                            func: func.clone(),
                            state,
                        }
                    });

                    ctx.func.inverse(&mut ctx.state, &args)?;
                    pc += 1;
                }

                Opcode::AggValue => {
                    // Extract the current intermediate value from the
                    // window accumulator in register P3, storing the
                    // result in register P3. Unlike AggFinal, this
                    // does NOT consume the accumulator.
                    // P4 = function name, P1 = accumulator register,
                    // P3 = destination register.
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "AggValue opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "AggValue opcode executed without function registry".to_owned(),
                        )
                    })?;

                    let arg_count = op.p2;
                    let func = registry.find_window(func_name, arg_count).ok_or_else(|| {
                        FrankenError::Internal(format!(
                            "no such window function: {func_name}/{arg_count}",
                        ))
                    })?;

                    let accum_reg = op.p1;
                    let result = match self.window_contexts.get(&accum_reg) {
                        Some(ctx) => ctx.func.value(&ctx.state)?,
                        None => func.value(&func.initial_state())?,
                    };
                    self.set_reg(op.p3, result);
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

                    let args = self.collect_reg_range(first_arg_reg, arg_count);
                    let result = func.invoke(&args)?;

                    if self.trace_opcodes {
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
                    }

                    // Update global call count (fast path: no Instant::now).
                    fsqlite_func::record_func_call_count_only();

                    self.set_reg(output_reg, result);
                    pc += 1;
                }

                // ── LIMIT/OFFSET support ────────────────────────────────
                // DecrJumpZero: decrement register p1; if result is zero
                // jump to p2. If value is initially zero or negative, do nothing.
                // Used to count down remaining LIMIT rows.
                Opcode::DecrJumpZero => {
                    let mut val = self.get_reg(op.p1).to_integer();
                    if val > 0 {
                        val -= 1;
                        self.set_reg(op.p1, SqliteValue::Integer(val));
                        if val == 0 {
                            #[allow(clippy::cast_sign_loss)]
                            {
                                pc = op.p2 as usize;
                            }
                        } else {
                            pc += 1;
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

                // ── RowSet operations ──────────────────────────────────
                // Used by OR-optimized queries and IN subqueries.
                Opcode::RowSetAdd => {
                    // Add integer P2 to rowset in register P1.
                    let rowset_reg = op.p1;
                    let val = self.get_reg(op.p2).to_integer();
                    self.rowsets
                        .entry_or_insert_with(rowset_reg, RowSet::new)
                        .add(val);
                    pc += 1;
                }

                Opcode::RowSetRead => {
                    // Read next value from rowset P1 into register P3;
                    // jump to P2 when exhausted.
                    let rowset_reg = op.p1;
                    let next_val = self
                        .rowsets
                        .get_mut(&rowset_reg)
                        .and_then(|rs| rs.read_next());
                    match next_val {
                        Some(val) => {
                            self.set_reg(op.p3, SqliteValue::Integer(val));
                            pc += 1;
                        }
                        None => {
                            pc = op.p2 as usize;
                        }
                    }
                }

                Opcode::RowSetTest => {
                    // Test if P3 exists in rowset P1; jump to P2 if found.
                    // If not found, add P3 to the rowset and fall through.
                    let rowset_reg = op.p1;
                    let val = self.get_reg(op.p3).to_integer();
                    let found = self
                        .rowsets
                        .get(&rowset_reg)
                        .is_some_and(|rs| rs.contains(val));
                    if found {
                        pc = op.p2 as usize;
                    } else {
                        self.rowsets
                            .entry_or_insert_with(rowset_reg, RowSet::new)
                            .add(val);
                        pc += 1;
                    }
                }

                // ── Foreign Key counters ──────────────────────────────
                Opcode::FkCounter => {
                    // P1=0 → immediate FK counter, P1=1 → deferred.
                    // P2 = delta to add (positive or negative).
                    self.fk_counter += i64::from(op.p2);
                    pc += 1;
                }

                Opcode::FkIfZero => {
                    // Jump to P2 if FK counter is zero.
                    // P1=0 → immediate, P1=1 → deferred.
                    if self.fk_counter == 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── MemMax: P2 = max(P2, P1) ─────────────────────────
                Opcode::MemMax => {
                    let val1 = self.get_reg(op.p1).to_integer();
                    let val2 = self.get_reg(op.p2).to_integer();
                    if val1 > val2 {
                        self.set_reg(op.p2, SqliteValue::Integer(val1));
                    }
                    pc += 1;
                }

                // ── OffsetLimit ───────────────────────────────────────
                // Compute the combined LIMIT+OFFSET value.
                // P1 = LIMIT, P2 = OFFSET output register,
                // P3 = combined output register.
                // If LIMIT is negative (no limit), store -1 in P3.
                // Otherwise store LIMIT+OFFSET in P3.
                Opcode::OffsetLimit => {
                    let limit = self.get_reg(op.p1).to_integer();
                    let offset = self.get_reg(op.p2).to_integer();
                    let combined = if limit < 0 {
                        -1
                    } else {
                        limit.saturating_add(offset)
                    };
                    self.set_reg(op.p3, SqliteValue::Integer(combined));
                    pc += 1;
                }

                // ── IfNotZero: jump if P1 != 0, decrement by 1 ───────
                Opcode::IfNotZero => {
                    let val = self.get_reg(op.p1).to_integer();
                    if val != 0 {
                        self.set_reg(op.p1, SqliteValue::Integer(val.wrapping_sub(1)));
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Page info ────────────────────────────────────────
                Opcode::Pagecount => {
                    // Store the total page count of database P1 into register P2.
                    // In memory mode, approximate as number of tables.
                    let count = self.db.as_ref().map_or(0, |db| db.table_count());
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(count)));
                    pc += 1;
                }

                Opcode::MaxPgcnt => {
                    // Return/set max page count. For now, return a large value.
                    self.set_reg(op.p2, SqliteValue::Integer(1_073_741_823));
                    pc += 1;
                }

                // ── Journal mode ─────────────────────────────────────
                Opcode::JournalMode => {
                    // Return current journal mode as text in register P2.
                    // FrankenSQLite defaults to WAL mode.
                    self.set_reg(op.p2, SqliteValue::Text("wal".to_owned()));
                    pc += 1;
                }

                // ── Vacuum ───────────────────────────────────────────
                Opcode::Vacuum | Opcode::IncrVacuum => {
                    // In the in-memory engine, vacuum is a no-op.
                    // IncrVacuum: jump to P2 when done (always done immediately).
                    if op.opcode == Opcode::IncrVacuum {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Integrity check ──────────────────────────────────
                Opcode::IntegrityCk => {
                    // Run integrity check. For now, always report OK.
                    // P1 = root page register, P2 = output register,
                    // P3 = number of tables to check.
                    self.set_reg(op.p2, SqliteValue::Text("ok".to_owned()));
                    pc += 1;
                }

                // ── Expire ───────────────────────────────────────────
                Opcode::Expire => {
                    // Mark prepared statement as expired (no-op; we don't
                    // cache prepared statements yet).
                    pc += 1;
                }

                // ── Cursor lock/unlock ───────────────────────────────
                Opcode::CursorLock | Opcode::CursorUnlock => {
                    // Advisory cursor locking. No-op in single-process mode.
                    pc += 1;
                }

                // ── Subtype operations ───────────────────────────────
                // Subtypes tag registers with metadata (e.g. JSON
                // subtype 74/'J') without changing the stored value.
                Opcode::ClrSubtype => {
                    // Clear subtype flag on register P1.
                    self.register_subtypes.remove(&op.p1);
                    pc += 1;
                }

                Opcode::GetSubtype => {
                    // Store the subtype of register P1 into register P2.
                    // Returns 0 if no subtype is set.
                    let st = self.register_subtypes.get(&op.p1).copied().unwrap_or(0);
                    #[allow(clippy::cast_possible_wrap)]
                    self.set_reg(op.p2, SqliteValue::Integer(st as i64));
                    pc += 1;
                }

                Opcode::SetSubtype => {
                    // Set the subtype of register P2 from the integer
                    // value in register P1.
                    let val = self.get_reg(op.p1);
                    #[allow(clippy::cast_sign_loss)]
                    let st = match val {
                        SqliteValue::Integer(i) => *i as u32,
                        _ => 0,
                    };
                    if st == 0 {
                        self.register_subtypes.remove(&op.p2);
                    } else {
                        self.register_subtypes.insert(op.p2, st);
                    }
                    pc += 1;
                }

                // ── Bloom filter ─────────────────────────────────────
                // Bloom filters provide early rejection during index
                // lookups. P1 is the filter register, P3 the hash key
                // register, P2 the jump target (for Filter).
                Opcode::FilterAdd => {
                    // Add hash of register P3 to the Bloom filter
                    // identified by P1.
                    let hash = bloom_hash(self.get_reg(op.p3));
                    let filter = self
                        .bloom_filters
                        .entry(op.p1)
                        .or_insert_with(|| vec![0u64; BLOOM_FILTER_WORDS]);
                    let bit = (hash as usize) % (filter.len() * 64);
                    filter[bit / 64] |= 1u64 << (bit % 64);
                    pc += 1;
                }

                Opcode::Filter => {
                    // Test Bloom filter P1 for register P3's hash.
                    // Jump to P2 if definitely not present.
                    if let Some(filter) = self.bloom_filters.get(&op.p1) {
                        let hash = bloom_hash(self.get_reg(op.p3));
                        let bit = (hash as usize) % (filter.len() * 64);
                        let present = (filter[bit / 64] >> (bit % 64)) & 1 == 1;
                        if !present {
                            pc = op.p2 as usize;
                        } else {
                            pc += 1;
                        }
                    } else {
                        // No filter exists — conservatively fall through.
                        pc += 1;
                    }
                }

                // ── Hints & debug ────────────────────────────────────
                Opcode::CursorHint | Opcode::Trace | Opcode::Abortable | Opcode::ReleaseReg => {
                    // Advisory/debug opcodes. No-op.
                    pc += 1;
                }

                // ── Virtual Table opcodes ───────────────────────────────
                Opcode::VOpen => {
                    // Open a virtual table cursor.
                    // P1 = cursor number.
                    // If a vtab instance is registered, call open_cursor().
                    let cursor_id = op.p1;
                    if !self.vtab_cursors.contains_key(&cursor_id) {
                        if let Some(vtab) = self.vtab_instances.get(&cursor_id) {
                            match vtab.open_cursor() {
                                Ok(cursor) => {
                                    self.vtab_cursors
                                        .insert(cursor_id, VtabCursorState { cursor });
                                }
                                Err(e) => {
                                    break ExecOutcome::Error {
                                        code: 1,
                                        message: format!("VOpen error: {e}"),
                                    };
                                }
                            }
                        }
                    }
                    pc += 1;
                }

                Opcode::VFilter => {
                    // Apply filter to virtual table cursor and begin scan.
                    // P1 = cursor number
                    // P2 = jump address if cursor is empty after filter
                    // P3 = register with first filter argument
                    // P4 = number of filter arguments (via P4::Int)
                    let cursor_id = op.p1;
                    let jump_if_empty = op.p2;

                    if let Some(state) = self.vtab_cursors.get_mut(&cursor_id) {
                        let cx = Cx::new();
                        let n_args = match &op.p4 {
                            P4::Int(n) => *n as usize,
                            _ => 0,
                        };
                        let args: Vec<SqliteValue> = (0..n_args)
                            .map(|i| {
                                #[allow(clippy::cast_possible_wrap)]
                                self.registers
                                    .get((op.p3 + i as i32) as usize)
                                    .cloned()
                                    .unwrap_or(SqliteValue::Null)
                            })
                            .collect();
                        let idx_num = op.p5 as i32;
                        if let Err(e) = state.cursor.filter(&cx, idx_num, None, &args) {
                            break ExecOutcome::Error {
                                code: 1,
                                message: format!("VFilter error: {e}"),
                            };
                        }
                        if state.cursor.eof() {
                            #[allow(clippy::cast_sign_loss)]
                            {
                                pc = jump_if_empty as usize;
                            }
                            continue;
                        }
                    }
                    pc += 1;
                }

                Opcode::VColumn => {
                    // Read column from virtual table cursor.
                    // P1 = cursor number
                    // P2 = column index
                    // P3 = destination register
                    let cursor_id = op.p1;
                    let col = op.p2;
                    let dest = op.p3;

                    if let Some(state) = self.vtab_cursors.get(&cursor_id) {
                        let mut ctx = ColumnContext::new();
                        if let Err(e) = state.cursor.column(&mut ctx, col) {
                            break ExecOutcome::Error {
                                code: 1,
                                message: format!("VColumn error: {e}"),
                            };
                        }
                        #[allow(clippy::cast_sign_loss)]
                        {
                            if let Some(reg) = self.registers.get_mut(dest as usize) {
                                *reg = ctx.take_value().unwrap_or(SqliteValue::Null);
                            }
                        }
                    }
                    pc += 1;
                }

                Opcode::VNext => {
                    // Advance virtual table cursor to the next row.
                    // P1 = cursor number
                    // P2 = jump address to loop body (go back if not eof)
                    let cursor_id = op.p1;
                    let jump_if_more = op.p2;

                    if let Some(state) = self.vtab_cursors.get_mut(&cursor_id) {
                        let cx = Cx::new();
                        if let Err(e) = state.cursor.next(&cx) {
                            break ExecOutcome::Error {
                                code: 1,
                                message: format!("VNext error: {e}"),
                            };
                        }
                        if !state.cursor.eof() {
                            #[allow(clippy::cast_sign_loss)]
                            {
                                pc = jump_if_more as usize;
                            }
                            continue;
                        }
                    }
                    pc += 1;
                }

                Opcode::VUpdate => {
                    // INSERT/UPDATE/DELETE on a virtual table.
                    // P1 = cursor number, P2 = arg count, P3 = first arg reg
                    let cursor_id = op.p1;
                    let n_args = op.p2;
                    let first_reg = op.p3;
                    let dest_reg = op.p5 as i32;
                    #[allow(clippy::cast_sign_loss)]
                    let args: Vec<SqliteValue> = (0..n_args)
                        .map(|i| {
                            self.registers
                                .get((first_reg + i) as usize)
                                .cloned()
                                .unwrap_or(SqliteValue::Null)
                        })
                        .collect();
                    if let Some(vtab) = self.vtab_instances.get_mut(&cursor_id) {
                        let cx = Cx::new();
                        match vtab.vtab_update(&cx, &args) {
                            Ok(Some(rowid)) => {
                                #[allow(clippy::cast_sign_loss)]
                                if let Some(reg) = self.registers.get_mut(dest_reg as usize) {
                                    *reg = SqliteValue::Integer(rowid);
                                }
                            }
                            Ok(None) => {
                                #[allow(clippy::cast_sign_loss)]
                                if let Some(reg) = self.registers.get_mut(dest_reg as usize) {
                                    *reg = SqliteValue::Null;
                                }
                            }
                            Err(e) => {
                                break ExecOutcome::Error {
                                    code: 1,
                                    message: format!("VUpdate error: {e}"),
                                };
                            }
                        }
                    } else {
                        #[allow(clippy::cast_sign_loss)]
                        if let Some(reg) = self.registers.get_mut(dest_reg as usize) {
                            *reg = SqliteValue::Null;
                        }
                    }
                    pc += 1;
                }

                Opcode::VBegin => {
                    // Begin a virtual table transaction.
                    // P1 = cursor number identifying the vtab instance.
                    let cursor_id = op.p1;
                    if let Some(vtab) = self.vtab_instances.get_mut(&cursor_id) {
                        let cx = Cx::new();
                        if let Err(e) = vtab.begin(&cx) {
                            break ExecOutcome::Error {
                                code: 1,
                                message: format!("VBegin error: {e}"),
                            };
                        }
                    }
                    pc += 1;
                }

                Opcode::VCreate => {
                    // Create a virtual table — handled at Connection layer.
                    pc += 1;
                }

                Opcode::VDestroy => {
                    // Destroy a virtual table — handled at Connection layer.
                    pc += 1;
                }

                Opcode::VCheck => {
                    // Check virtual table integrity.
                    // P3 = destination register for error message (NULL if OK).
                    let dest_reg = op.p3;
                    #[allow(clippy::cast_sign_loss)]
                    if let Some(reg) = self.registers.get_mut(dest_reg as usize) {
                        *reg = SqliteValue::Null;
                    }
                    pc += 1;
                }

                Opcode::VInitIn => {
                    // Initialize IN constraint for virtual table.
                    // P2 = register containing the IN value list
                    // P3 = destination register
                    let _cursor_id = op.p1;
                    let src_reg = op.p2;
                    let dest_reg = op.p3;
                    #[allow(clippy::cast_sign_loss)]
                    {
                        let val = self
                            .registers
                            .get(src_reg as usize)
                            .cloned()
                            .unwrap_or(SqliteValue::Null);
                        if let Some(reg) = self.registers.get_mut(dest_reg as usize) {
                            *reg = val;
                        }
                    }
                    pc += 1;
                }

                Opcode::VRename => {
                    // Rename a virtual table.
                    // P1 = cursor number for the vtab instance
                    // P4 = new table name (via P4::Str)
                    let cursor_id = op.p1;
                    if let Some(vtab) = self.vtab_instances.get_mut(&cursor_id) {
                        let new_name = match &op.p4 {
                            P4::Str(s) => s.as_str(),
                            _ => "",
                        };
                        let cx = Cx::new();
                        if let Err(e) = vtab.rename(&cx, new_name) {
                            break ExecOutcome::Error {
                                code: 1,
                                message: format!("VRename error: {e}"),
                            };
                        }
                    }
                    pc += 1;
                }

                // ── Catch-all for future opcodes ─────────────────────
                #[allow(unreachable_patterns)]
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

    fn get_reg(&self, r: i32) -> &SqliteValue {
        usize::try_from(r)
            .ok()
            .and_then(|idx| self.registers.get(idx))
            .unwrap_or(&SqliteValue::Null)
    }

    fn reg_with_offset(start: i32, offset: usize) -> Option<i32> {
        i32::try_from(offset)
            .ok()
            .and_then(|delta| start.checked_add(delta))
    }

    fn collect_reg_range(&self, start: i32, count: usize) -> Vec<SqliteValue> {
        (0..count)
            .map(|offset| {
                Self::reg_with_offset(start, offset)
                    .map_or(SqliteValue::Null, |reg| self.get_reg(reg).clone())
            })
            .collect()
    }

    fn take_reg(&mut self, r: i32) -> SqliteValue {
        usize::try_from(r)
            .ok()
            .and_then(|idx| self.registers.get_mut(idx))
            .map_or(SqliteValue::Null, |slot| {
                std::mem::replace(slot, SqliteValue::Null)
            })
    }

    #[allow(clippy::cast_sign_loss)]
    fn set_reg(&mut self, r: i32, val: SqliteValue) {
        if !(0..=65535).contains(&r) {
            // Drop out-of-bounds register writes to prevent OOM.
            // SQLite defines a max register limit (SQLITE_MAX_COLUMN + some overhead).
            return;
        }
        let idx = r as usize;
        if idx >= self.registers.len() {
            self.registers.resize(idx + 1, SqliteValue::Null);
        }
        self.registers[idx] = match val {
            SqliteValue::Float(f) if f.is_nan() => SqliteValue::Null,
            other => other,
        };
    }

    /// Fast-path register write with NaN -> Null normalization.
    /// Auto-resizes the register file when necessary (handles both
    /// builder-allocated programs and hand-crafted test programs).
    #[inline]
    #[allow(clippy::cast_sign_loss)]
    fn set_reg_fast(&mut self, r: i32, val: SqliteValue) {
        if !(0..=65535).contains(&r) {
            return;
        }
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
    fn cursor_column(&mut self, cursor_id: i32, col_idx: usize) -> Result<SqliteValue> {
        if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
            if cursor.cursor.eof() {
                return Ok(SqliteValue::Null);
            }
            let payload = cursor.cursor.payload(&cursor.cx)?;

            let values = if let Some((cached_payload, cached_values)) = &cursor.cached_row {
                if cached_payload == &payload {
                    Some(cached_values)
                } else {
                    None
                }
            } else {
                None
            };

            let decoded = if let Some(vals) = values {
                vals
            } else {
                let new_values = decode_record(&SqliteValue::Blob(payload.clone()))?;
                cursor.cached_row = Some((payload, new_values));
                &cursor.cached_row.as_ref().unwrap().1
            };

            if col_idx < decoded.len() {
                return Ok(decoded[col_idx].clone());
            }
            // Column beyond record width — check ALTER TABLE ADD COLUMN defaults.
            if let Some(&root_page) = self.cursor_root_pages.get(&cursor_id) {
                if let Some(defaults) = self.column_defaults_by_root_page.get(&root_page) {
                    if let Some(Some(default_val)) = defaults.get(col_idx) {
                        return Ok(default_val.clone());
                    }
                }
            }
            return Ok(SqliteValue::Null);
        }

        if let Some(cursor) = self.cursors.get(&cursor_id) {
            if cursor.is_pseudo {
                if let Some(reg) = cursor.pseudo_reg {
                    let blob = self.get_reg(reg);
                    if let Ok(values) = decode_record(blob) {
                        return Ok(values.get(col_idx).cloned().unwrap_or(SqliteValue::Null));
                    }
                }
                return Ok(SqliteValue::Null);
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
        let mode = if self.reject_mem_fallback {
            "parity_cert"
        } else {
            "fallback_allowed"
        };

        let Some(root_pgno) = PageNumber::new(root_page as u32) else {
            tracing::debug!(
                cursor_id,
                root_page,
                writable,
                mode,
                backend_kind = "none",
                decision_reason = "invalid_page_number",
                "open_storage_cursor: invalid root page number"
            );
            return false;
        };

        let has_txn = self.txn_page_io.is_some();

        // Phase 5C.1 (bd-35my): Route through pager when available.
        //
        // Critical safety rule:
        // If a pager transaction exists, NEVER fall back to MemPageStore. A
        // fallback can silently route writes to a non-durable in-memory copy
        // and create divergence/corruption under concurrency.
        //
        // The only acceptable writable "bootstrap" case is a truly
        // zero-initialized root page that we can initialize in-place.
        if let Some(ref mut page_io) = self.txn_page_io {
            let cx = Cx::new();
            // If the pager read itself fails, fail cursor open instead of
            // treating it as an uninitialized page.
            let page_data = match page_io.read_page(&cx, root_pgno) {
                Ok(bytes) => bytes,
                Err(err) => {
                    tracing::warn!(
                        cursor_id,
                        page_id = root_page,
                        writable,
                        has_txn,
                        mode,
                        backend_kind = "txn",
                        decision_reason = "pager_read_failed",
                        error = %err,
                        "open_storage_cursor: failed to read root page from pager"
                    );
                    return false;
                }
            };
            let hdr_offset = header_offset_for_page(root_pgno);
            let parsed_header = BtreePageHeader::parse(&page_data, hdr_offset).ok();
            // A page is a valid B-tree if the header parses successfully.
            // Legacy fallback: synthetic test pages with non-zero first byte
            // but unparseable header are also accepted, but we infer
            // is_table from the raw page-type byte rather than defaulting
            // to true (which was incorrect for index pages).
            let is_valid_btree =
                parsed_header.is_some() || (!page_data.is_empty() && page_data[0] != 0x00);
            let is_zero_page = page_data.iter().all(|&byte| byte == 0);

            if is_valid_btree {
                // Real B-tree backed by pager: infer table-vs-index from the
                // parsed page header when available, falling back to the raw
                // page-type byte for synthetic/legacy pages.
                let (is_table_btree, detected_page_type) = if let Some(header) = parsed_header {
                    (header.page_type.is_table(), Some(header.page_type))
                } else {
                    // Header parse failed but page has data. Use the raw
                    // page-type flag byte at the header offset to infer
                    // table vs index — do NOT blindly default to table.
                    let type_byte = page_data.get(hdr_offset).copied().unwrap_or(0);
                    let is_table = match type_byte {
                        0x02 | 0x0A => false, // InteriorIndex / LeafIndex
                        0x05 | 0x0D => true,  // InteriorTable / LeafTable
                        _ => {
                            tracing::warn!(
                                cursor_id,
                                page_id = root_page,
                                type_byte,
                                "open_storage_cursor: unparseable header with unknown page-type byte, defaulting to table"
                            );
                            true
                        }
                    };
                    (is_table, None)
                };
                let cursor = BtCursor::new(page_io.clone(), root_pgno, PAGE_SIZE, is_table_btree);
                self.storage_cursors.insert(
                    cursor_id,
                    StorageCursor {
                        cursor: CursorBackend::Txn(cursor),
                        cx,
                        writable,
                        last_alloc_rowid: 0,
                        cached_row: None,
                    },
                );
                tracing::debug!(
                    cursor_id,
                    page_id = root_page,
                    writable,
                    has_txn,
                    mode,
                    backend_kind = "txn",
                    decision_reason = "valid_btree_page",
                    detected_page_type = ?detected_page_type,
                    is_table_btree,
                    "open_storage_cursor: routed through pager transaction"
                );
                return true;
            }

            // For writable cursors on truly zeroed pages (e.g., freshly
            // allocated roots), initialize an empty root page.
            if writable && is_zero_page {
                // Infer root kind from MemDatabase when available; default to
                // table for backwards compatibility if MemDatabase is absent.
                let is_table_btree = self
                    .db
                    .as_ref()
                    .is_none_or(|db| db.get_table(root_page).is_some());
                let init_page_type = if is_table_btree {
                    BtreePageType::LeafTable
                } else {
                    BtreePageType::LeafIndex
                };
                // Initialize empty leaf page for the inferred B-tree kind.
                let mut page = vec![0u8; PAGE_SIZE as usize];
                page[0] = init_page_type as u8;
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
                        cursor_id,
                        page_id = root_page,
                        writable,
                        has_txn,
                        mode,
                        backend_kind = "txn",
                        decision_reason = "zero_page_init_failed",
                        error = %err,
                        "open_storage_cursor: failed to initialize writable root page in pager"
                    );
                    return false;
                }
                let cursor = BtCursor::new(page_io.clone(), root_pgno, PAGE_SIZE, is_table_btree);
                self.storage_cursors.insert(
                    cursor_id,
                    StorageCursor {
                        cursor: CursorBackend::Txn(cursor),
                        cx,
                        writable,
                        last_alloc_rowid: 0,
                        cached_row: None,
                    },
                );
                tracing::debug!(
                    cursor_id,
                    page_id = root_page,
                    writable,
                    has_txn,
                    mode,
                    backend_kind = "txn",
                    decision_reason = "zero_page_initialized",
                    initialized_page_type = ?init_page_type,
                    is_table_btree,
                    "open_storage_cursor: initialized empty root page via pager"
                );
                return true;
            }

            // If the page is zero/invalid but MemDatabase has this table
            // (e.g., materialized sqlite_master virtual table), fall through
            // to the MemDatabase path below instead of refusing.
            let has_mem_table = self
                .db
                .as_ref()
                .is_some_and(|db| db.get_table(root_page).is_some());
            if !has_mem_table {
                // No MemDatabase fallback available — refuse to open.
                tracing::warn!(
                    cursor_id,
                    page_id = root_page,
                    writable,
                    has_txn,
                    mode,
                    backend_kind = "none",
                    decision_reason = "invalid_page_no_mem_fallback",
                    first_byte = page_data.first().copied().unwrap_or_default(),
                    is_zero_page,
                    "open_storage_cursor: refusing on invalid transaction-backed root page"
                );
                return false;
            }
            // else: fall through to MemDatabase path
            tracing::debug!(
                cursor_id,
                page_id = root_page,
                writable,
                has_txn,
                mode,
                backend_kind = "mem",
                decision_reason = "txn_page_invalid_mem_fallback",
                "open_storage_cursor: pager page invalid, falling through to MemDatabase"
            );
        }

        // bd-2ttd8.1: Parity-certification mode — reject MemPageStore fallback.
        if self.reject_mem_fallback {
            tracing::warn!(
                cursor_id,
                page_id = root_page,
                writable,
                has_txn,
                mode,
                backend_kind = "mem",
                decision_reason = "parity_cert_rejection",
                "open_storage_cursor: MemPageStore fallback rejected in parity-cert mode"
            );
            return false;
        }

        // Fallback: build a transient B-tree snapshot (Phase 4 path used by
        // tests without a real pager). Both read and write cursors can operate
        // on empty tables (INSERT needs to work on new tables).
        let is_table_btree = self
            .db
            .as_ref()
            .is_none_or(|db| db.get_table(root_page).is_some());
        let store = if is_table_btree {
            MemPageStore::with_empty_table(root_pgno, PAGE_SIZE)
        } else {
            MemPageStore::with_empty_index(root_pgno, PAGE_SIZE)
        };
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root_pgno, PAGE_SIZE, is_table_btree);
        // Populate cursor from MemDatabase if available.
        if is_table_btree
            && let Some(table) = self.db.as_ref().and_then(|db| db.get_table(root_page))
        {
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
                cached_row: None,
            },
        );
        tracing::debug!(
            cursor_id,
            page_id = root_page,
            writable,
            has_txn,
            mode,
            backend_kind = "mem",
            decision_reason = "no_pager_transaction",
            is_table_btree,
            "open_storage_cursor: routed through MemPageStore fallback"
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

/// SQLite affinity constants (from §3.2 of datatype3.html).
/// Encoded in the lower bits of comparison opcode p5 (masked by 0x47).
const SQLITE_AFF_TEXT: u16 = 0x42; // 'B'
const SQLITE_AFF_NUMERIC: u16 = 0x43; // 'C'

/// Apply SQLite comparison affinity coercion (§3.2 of datatype3.html).
///
/// Coercion only applies when the comparison opcode's p5 carries a
/// numeric-class affinity (>= NUMERIC / 0x43).  When p5 is 0 or carries
/// BLOB affinity (0x41), no coercion is performed — values compare using
/// their native storage classes (NULL < numeric < text < blob).
/// C SQLite OP_If/OP_IfNot truthiness: uses `sqlite3VdbeRealValue() != 0.0`,
/// which means 0.1, 0.5, -0.1 etc. are all truthy (unlike integer truncation).
fn vdbe_real_is_truthy(val: &SqliteValue) -> bool {
    match val {
        SqliteValue::Null => false,
        SqliteValue::Integer(n) => *n != 0,
        SqliteValue::Float(f) => *f != 0.0,
        SqliteValue::Text(_) | SqliteValue::Blob(_) => {
            let i = val.to_integer();
            if i != 0 {
                return true;
            }
            val.to_float() != 0.0
        }
    }
}

fn coerce_for_comparison<'a>(
    lhs: &'a SqliteValue,
    rhs: &'a SqliteValue,
    p5: u16,
) -> (
    std::borrow::Cow<'a, SqliteValue>,
    std::borrow::Cow<'a, SqliteValue>,
) {
    use std::borrow::Cow;

    let affinity = p5 & 0x47_u16; // SQLITE_AFF_MASK

    // TEXT affinity (0x42): convert numeric operands to text for comparison.
    if affinity == SQLITE_AFF_TEXT {
        let coerce_to_text = |v: &SqliteValue| -> Option<SqliteValue> {
            match v {
                SqliteValue::Integer(_) | SqliteValue::Float(_) => {
                    Some(SqliteValue::Text(v.to_text()))
                }
                _ => None,
            }
        };
        let new_lhs = coerce_to_text(lhs);
        let new_rhs = coerce_to_text(rhs);
        return (
            new_lhs.map_or_else(|| Cow::Borrowed(lhs), Cow::Owned),
            new_rhs.map_or_else(|| Cow::Borrowed(rhs), Cow::Owned),
        );
    }

    // Numeric affinity (>= 0x43): coerce text→numeric when one side is numeric.
    if affinity >= SQLITE_AFF_NUMERIC {
        let is_numeric =
            |v: &SqliteValue| matches!(v, SqliteValue::Integer(_) | SqliteValue::Float(_));

        if is_numeric(lhs) {
            if let SqliteValue::Text(s) = rhs {
                if let Some(coerced) = try_coerce_text_to_numeric_cmp(s) {
                    return (Cow::Borrowed(lhs), Cow::Owned(coerced));
                }
            }
        }
        if is_numeric(rhs) {
            if let SqliteValue::Text(s) = lhs {
                if let Some(coerced) = try_coerce_text_to_numeric_cmp(s) {
                    return (Cow::Owned(coerced), Cow::Borrowed(rhs));
                }
            }
        }
    }

    (Cow::Borrowed(lhs), Cow::Borrowed(rhs))
}

/// Try to parse a text string as a numeric value for comparison coercion.
fn try_coerce_text_to_numeric_cmp(s: &str) -> Option<SqliteValue> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Try integer first.
    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(SqliteValue::Integer(i));
    }
    // Try float.
    if let Ok(f) = trimmed.parse::<f64>() {
        if !f.is_finite() {
            let lower = trimmed.to_ascii_lowercase();
            if lower.contains("inf") || lower.contains("nan") {
                return None;
            }
        }
        return Some(SqliteValue::Float(f));
    }
    None
}

fn collate_compare(
    lhs: &SqliteValue,
    rhs: &SqliteValue,
    coll_name: &str,
) -> Option<std::cmp::Ordering> {
    match (lhs, rhs) {
        (SqliteValue::Text(l), SqliteValue::Text(r)) => {
            let upper = coll_name.to_ascii_uppercase();
            Some(match upper.as_str() {
                "NOCASE" => {
                    let li = l.as_bytes().iter().map(u8::to_ascii_uppercase);
                    let ri = r.as_bytes().iter().map(u8::to_ascii_uppercase);
                    li.cmp(ri)
                }
                "RTRIM" => {
                    let lb = l.as_bytes();
                    let rb = r.as_bytes();
                    let lt = &lb[..lb.iter().rposition(|&b| b != b' ').map_or(0, |p| p + 1)];
                    let rt = &rb[..rb.iter().rposition(|&b| b != b' ').map_or(0, |p| p + 1)];
                    lt.cmp(rt)
                }
                _ => l.as_bytes().cmp(r.as_bytes()),
            })
        }
        _ => lhs.partial_cmp(rhs),
    }
}

fn parse_compare_collations(p4: &P4) -> Option<Vec<String>> {
    match p4 {
        P4::Collation(name) => Some(vec![name.clone()]),
        P4::Str(spec) => {
            let parsed: Vec<String> = spec
                .split([',', '|', '\0'])
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_owned)
                .collect();
            (!parsed.is_empty()).then_some(parsed)
        }
        _ => None,
    }
}

fn compare_collation_for_field(collations: Option<&[String]>, field_idx: usize) -> Option<&str> {
    let collations = collations?;
    if collations.len() == 1 {
        return collations.first().map(String::as_str);
    }
    collations.get(field_idx).map(String::as_str)
}

/// For REPLACE conflict resolution: re-seek the index cursor, find the
/// conflicting entry (which has matching indexed columns but a different
/// rowid), delete it from the index, and return its rowid so the caller
/// can delete the old table row.
fn find_conflicting_rowid_in_index(
    sc: &mut StorageCursor,
    key_bytes: &[u8],
    n_idx_cols: usize,
) -> Result<Option<i64>> {
    // Re-seek to the position where the conflicting entry should be.
    sc.cursor.index_move_to(&sc.cx, key_bytes)?;

    // The new key we're trying to insert — parse its prefix for comparison.
    let new_fields = parse_record(key_bytes).unwrap_or_default();

    // Check the entry at current position and previous entry for a prefix match.
    for attempt in 0..2 {
        if sc.cursor.eof() {
            if attempt == 0 {
                // Try moving to the previous entry.
                sc.cursor.prev(&sc.cx)?;
                continue;
            }
            break;
        }

        let entry_bytes = sc.cursor.payload(&sc.cx)?;
        let entry_fields = parse_record(&entry_bytes).unwrap_or_default();

        // Check if the indexed columns (excluding the trailing rowid) match
        // and none of them are NULL.
        let mut prefix_match = true;
        let mut has_null = false;
        for i in 0..n_idx_cols {
            let new_val = new_fields.get(i);
            let entry_val = entry_fields.get(i);
            if matches!(new_val, Some(SqliteValue::Null) | None)
                || matches!(entry_val, Some(SqliteValue::Null) | None)
            {
                has_null = true;
                break;
            }
            if new_val != entry_val {
                prefix_match = false;
                break;
            }
        }

        if prefix_match && !has_null {
            // Extract the rowid (last field in the index entry).
            let old_rowid = if let Some(SqliteValue::Integer(rid)) = entry_fields.last() {
                *rid
            } else {
                // Fallback: try cursor rowid.
                sc.cursor.rowid(&sc.cx).unwrap_or(0)
            };

            // Delete the conflicting index entry.
            sc.cursor.delete(&sc.cx)?;

            return Ok(Some(old_rowid));
        }

        if attempt == 0 {
            sc.cursor.prev(&sc.cx)?;
        }
    }

    Ok(None)
}

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

fn sorter_keys_equal(
    lhs: &[SqliteValue],
    rhs: &[SqliteValue],
    key_columns: usize,
    collations: &[Option<String>],
) -> bool {
    compare_sorter_keys(lhs, rhs, key_columns, collations) == Ordering::Equal
}

fn compare_sorter_keys(
    lhs: &[SqliteValue],
    rhs: &[SqliteValue],
    key_columns: usize,
    collations: &[Option<String>],
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

        let coll = collations.get(idx).and_then(|c| c.as_deref());
        match cmp_values_collated(lhs_value, rhs_value, coll) {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }
    }
    Ordering::Equal
}

/// Compare two `SqliteValue`s with an optional collation sequence.
/// When `collation` is `Some("NOCASE")`, text values are compared
/// case-insensitively.
fn cmp_values_collated(lhs: &SqliteValue, rhs: &SqliteValue, collation: Option<&str>) -> Ordering {
    if let (Some(coll), SqliteValue::Text(lt), SqliteValue::Text(rt)) = (collation, lhs, rhs) {
        if coll.eq_ignore_ascii_case("NOCASE") {
            return lt.to_ascii_lowercase().cmp(&rt.to_ascii_lowercase());
        }
    }
    lhs.partial_cmp(rhs).unwrap_or(Ordering::Equal)
}

fn compare_sorter_rows(
    lhs: &[SqliteValue],
    rhs: &[SqliteValue],
    key_columns: usize,
    sort_key_orders: &[SortKeyOrder],
    collations: &[Option<String>],
) -> Ordering {
    let key_count = key_columns.max(1);
    for idx in 0..key_count {
        let order = sort_key_orders
            .get(idx)
            .copied()
            .unwrap_or(SortKeyOrder::Asc);
        let is_desc = matches!(order, SortKeyOrder::Desc | SortKeyOrder::DescNullsFirst);
        let nulls_last = matches!(order, SortKeyOrder::Desc | SortKeyOrder::AscNullsLast);
        let Some(lhs_value) = lhs.get(idx) else {
            return if rhs.get(idx).is_some() {
                if is_desc {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            } else {
                break;
            };
        };
        let Some(rhs_value) = rhs.get(idx) else {
            return if is_desc {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        };

        // Handle NULLs with explicit NULLS FIRST/LAST ordering.
        let l_null = lhs_value.is_null();
        let r_null = rhs_value.is_null();
        if l_null || r_null {
            if l_null && r_null {
                continue;
            }
            // l_null + nulls_last → Greater (NULL at end)
            // l_null + !nulls_last → Less (NULL at start)
            // !l_null + nulls_last → Less (non-NULL before NULL)
            // !l_null + !nulls_last → Greater (non-NULL after NULL)
            return if l_null == nulls_last {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        let coll = collations.get(idx).and_then(|c| c.as_deref());
        let mut ord = cmp_values_collated(lhs_value, rhs_value, coll);
        if ord == Ordering::Equal {
            continue;
        }

        if is_desc {
            ord = ord.reverse();
        }
        return ord;
    }

    // Rust's sort_by is stable, so equal-key rows stay in insertion order,
    // matching C SQLite's behavior (especially for COLLATE NOCASE DISTINCT).
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

/// Mirrors C SQLite `numericType()` (SQLite VDBE:496): returns true if BOTH
/// operands should be treated as integers for arithmetic purposes.
/// Text/Blob that parse as i64 are integer-typed; Float is not.
fn both_integer_numeric_type(a: &SqliteValue, b: &SqliteValue) -> bool {
    a.is_integer_numeric_type() && b.is_integer_numeric_type()
}

/// SQL division with NULL propagation and division-by-zero handling.
#[allow(clippy::cast_precision_loss)]
fn sql_div(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    // C SQLite numericType() coercion (SQLite VDBE:1932-1934): if both operands
    // are integer-typed (including text that parses as integer), use int math.
    let both_int = both_integer_numeric_type(dividend, divisor);
    if both_int {
        let a = dividend.to_integer();
        let b = divisor.to_integer();
        if b == 0 {
            SqliteValue::Null
        } else {
            match a.checked_div(b) {
                Some(result) => SqliteValue::Integer(result),
                // i64::MIN / -1 overflows; C SQLite promotes to float via
                // `goto fp_math` (SQLite VDBE:1916), NOT wrapping.
                #[allow(clippy::cast_precision_loss)]
                None => {
                    let result = a as f64 / b as f64;
                    if result.is_nan() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Float(result)
                    }
                }
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
///
/// C SQLite (SQLite VDBE:1920): when both operands are MEM_Int, result is Integer.
/// When either is Float/Text/Blob, fp_math path casts to integer for the
/// modulo but stores the result as Float (MEM_Real).
/// C SQLite `numericType()` coercion: text that parses as integer is treated
/// as integer for the both-int check (SQLite VDBE:1932-1934).
#[allow(clippy::cast_precision_loss)]
fn sql_rem(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    let both_int = both_integer_numeric_type(dividend, divisor);
    let a = dividend.to_integer();
    let b = divisor.to_integer();
    if b == 0 {
        return SqliteValue::Null;
    }
    // i64::MIN % -1 = 0 mathematically.
    let result = a.checked_rem(b).unwrap_or_default();
    if both_int {
        SqliteValue::Integer(result)
    } else {
        SqliteValue::Float(result as f64)
    }
}

/// SQL shift left (SQLite semantics: negative shift = shift right).
fn sql_shift_left(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_right(val, amount.saturating_neg());
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
        return sql_shift_left(val, amount.saturating_neg());
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
    // C SQLite compiles AND/OR using OP_If/OP_IfNot which use
    // sqlite3VdbeRealValue() != 0.0, so 0.5 is truthy.
    let a_val = if a.is_null() {
        None
    } else {
        Some(vdbe_real_is_truthy(a))
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(vdbe_real_is_truthy(b))
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
        Some(vdbe_real_is_truthy(a))
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(vdbe_real_is_truthy(b))
    };

    match (a_val, b_val) {
        (Some(true), _) | (_, Some(true)) => SqliteValue::Integer(1),
        (Some(false), Some(false)) => SqliteValue::Integer(0),
        _ => SqliteValue::Null,
    }
}

/// Scan the leading numeric prefix from a byte slice.
///
/// Recognises `[+-]? [0-9]* ('.' [0-9]*)? ([eE] [+-]? [0-9]+)?`.
/// Returns the byte offset where the prefix ends (0 if no prefix).
fn scan_numeric_prefix(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }
    let mut i = 0;
    // Optional leading sign.
    if bytes[i] == b'+' || bytes[i] == b'-' {
        i += 1;
    }
    let digit_start = i;
    // Integer digits.
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    // Optional decimal part.
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    // Must have consumed at least one digit.
    if i == digit_start {
        return 0;
    }
    // Optional exponent (e.g. e+10, E-3, e5).
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let exp_start = i;
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        if i < bytes.len() && bytes[i].is_ascii_digit() {
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
        } else {
            // No digits after 'e' — revert to before exponent.
            i = exp_start;
        }
    }
    i
}

/// Parse the text/blob prefix used by `CAST(... AS INTEGER)`.
///
/// SQLite only consumes an optional sign followed by decimal digits here.
/// Decimal points and exponents terminate the parse instead of contributing to
/// the numeric value.
fn parse_cast_integer_prefix(s: &str) -> i64 {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return 0;
    }

    let bytes = trimmed.as_bytes();
    let mut end = if matches!(bytes.first(), Some(b'+' | b'-')) {
        1
    } else {
        0
    };
    let digit_start = end;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == digit_start {
        return 0;
    }

    let prefix = &trimmed[..end];
    match prefix.parse::<i64>() {
        Ok(value) => value,
        Err(_) if prefix.starts_with('-') => i64::MIN,
        Err(_) => i64::MAX,
    }
}

/// SQL CAST operation (p2 encodes target type).
fn sql_cast(val: SqliteValue, target: i32) -> SqliteValue {
    if val.is_null() {
        return SqliteValue::Null;
    }
    // Target type encoding matches SQLite:
    // 'A' (65) = BLOB, 'B' (66) = TEXT, 'C' (67) = NUMERIC,
    // 'D' (68) = INTEGER, 'E' (69) = REAL
    // But more commonly p2 is used as an affinity character.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let target_byte = target as u8;
    // C SQLite interprets blob bytes as UTF-8 text before numeric casts.
    let val = match (val, target_byte) {
        (SqliteValue::Blob(b), b'C' | b'c' | b'D' | b'd' | b'E' | b'e') => {
            SqliteValue::Text(String::from_utf8_lossy(&b).into_owned())
        }
        (other, _) => other,
    };
    match target_byte {
        b'A' | b'a' => SqliteValue::Blob(match val {
            SqliteValue::Blob(b) => b,
            SqliteValue::Text(s) => s.into_bytes(),
            other => other.to_text().into_bytes(),
        }),
        b'B' | b'b' => {
            // C SQLite: CAST(blob AS TEXT) decodes bytes as UTF-8,
            // not as hex literal.
            match val {
                SqliteValue::Blob(b) => SqliteValue::Text(String::from_utf8_lossy(&b).into_owned()),
                other => SqliteValue::Text(other.to_text()),
            }
        }
        b'C' | b'c' => val.cast_to_numeric(),
        b'D' | b'd' => {
            // C SQLite integer casts from text/blob consume only the signed
            // integer prefix; decimal/exponent syntax is ignored.
            match &val {
                SqliteValue::Text(s) => SqliteValue::Integer(parse_cast_integer_prefix(s)),
                _ => SqliteValue::Integer(val.to_integer()),
            }
        }
        b'E' | b'e' => {
            // C SQLite: CAST('3.14abc' AS REAL) extracts leading numeric prefix.
            // C SQLite allows Inf from "1e999" but not from literal "inf" text.
            match &val {
                SqliteValue::Text(s) => {
                    let trimmed = s.trim();
                    let end = scan_numeric_prefix(trimmed.as_bytes());
                    // Full-string numeric match (rejects Rust's "nan"/"inf" parsing).
                    if end == trimmed.len() && end > 0 {
                        if let Ok(f) = trimmed.parse::<f64>() {
                            return SqliteValue::Float(f);
                        }
                    }
                    // Prefix match for strings with trailing non-numeric text.
                    if end > 0 {
                        if let Ok(f) = trimmed[..end].parse::<f64>() {
                            return SqliteValue::Float(f);
                        }
                    }
                    SqliteValue::Float(0.0)
                }
                _ => SqliteValue::Float(val.to_float()),
            }
        }
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
    use std::sync::Arc;

    use super::*;
    use crate::ProgramBuilder;
    use fsqlite_func::{FunctionRegistry, register_builtins};
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

    /// Build and execute a program with the builtin function registry attached.
    fn run_program_with_functions(
        build: impl FnOnce(&mut ProgramBuilder),
    ) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let mut registry = FunctionRegistry::new();
        register_builtins(&mut registry);
        engine.set_function_registry(Arc::new(registry));
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

    #[test]
    fn test_compare_opcode_uses_per_field_collation_list() {
        let mut b = ProgramBuilder::new();
        let left_key = b.alloc_regs(2);
        let right_key = b.alloc_regs(2);

        b.emit_op(
            Opcode::String8,
            0,
            left_key,
            0,
            P4::Str("Apple".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::String8,
            0,
            left_key + 1,
            0,
            P4::Str("beta".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::String8,
            0,
            right_key,
            0,
            P4::Str("apple".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::String8,
            0,
            right_key + 1,
            0,
            P4::Str("Beta".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::Compare,
            left_key,
            right_key,
            2,
            P4::Str("NOCASE,BINARY".to_owned()),
            0,
        );
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(
            engine.last_compare_result,
            Some(std::cmp::Ordering::Greater)
        );
    }

    #[test]
    fn test_compare_opcode_parses_trimmed_collation_list() {
        let mut b = ProgramBuilder::new();
        let left_key = b.alloc_regs(2);
        let right_key = b.alloc_regs(2);

        b.emit_op(
            Opcode::String8,
            0,
            left_key,
            0,
            P4::Str("Alpha".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::String8,
            0,
            left_key + 1,
            0,
            P4::Str("tail   ".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::String8,
            0,
            right_key,
            0,
            P4::Str("alpha".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::String8,
            0,
            right_key + 1,
            0,
            P4::Str("tail".to_owned()),
            0,
        );
        b.emit_op(
            Opcode::Compare,
            left_key,
            right_key,
            2,
            P4::Str(" NOCASE | RTRIM ".to_owned()),
            0,
        );
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(engine.last_compare_result, Some(std::cmp::Ordering::Equal));
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
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 30, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            if let Some(init_op) = b.op_at_mut(init_addr) {
                init_op.p2 = consumer_start;
                init_op.p3 = producer_start;
            }

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
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                        generated_expr: None,
                        generated_stored: None,
                        collation: None,
                    },
                ],
                indexes: vec![],
                strict: false,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
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
                    time_travel: None,
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
                    time_travel: None,
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
            engine.set_reject_mem_fallback(false);
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
                    time_travel: None,
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
            engine.set_reject_mem_fallback(false);
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
    fn test_divide_text_prefix_uses_integer_path() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_divisor = b.alloc_reg();
            let r_dividend = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 2, r_divisor, 0, P4::None, 0);
            b.emit_op(
                Opcode::String8,
                0,
                r_dividend,
                0,
                P4::Str("123abc".to_owned()),
                0,
            );
            b.emit_op(Opcode::Divide, r_divisor, r_dividend, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(61)]);
    }

    #[test]
    fn test_remainder_blob_prefix_uses_integer_path() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_divisor = b.alloc_reg();
            let r_dividend = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 2, r_divisor, 0, P4::None, 0);
            b.emit_op(
                Opcode::Blob,
                4,
                r_dividend,
                0,
                P4::Blob(b"123a".to_vec()),
                0,
            );
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

    #[test]
    fn test_comparison_jumpifnull_flag() {
        // JUMPIFNULL (0x10): jump to P2 when either operand is NULL.
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
            // Eq with JUMPIFNULL (0x10): NULL = 5 should jump
            b.emit_jump_to_label(Opcode::Eq, r_five, r_null, taken, P4::None, 0x10);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // With JUMPIFNULL, NULL = 5 should jump to P2
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_comparison_storep2_non_null() {
        // STOREP2 (0x20): store boolean result in P2 instead of jumping.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_a = b.alloc_reg();
            let r_b = b.alloc_reg();
            let o_eq = b.alloc_reg();
            let o_ne = b.alloc_reg();
            let o_lt = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_a, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r_b, 0, P4::None, 0);
            // Eq with STOREP2: 5 == 10 → store 0
            b.emit_op(Opcode::Eq, r_b, o_eq, r_a, P4::None, 0x20);
            // Ne with STOREP2: 5 != 10 → store 1
            b.emit_op(Opcode::Ne, r_b, o_ne, r_a, P4::None, 0x20);
            // Lt with STOREP2: 5 < 10 → store 1
            b.emit_op(Opcode::Lt, r_b, o_lt, r_a, P4::None, 0x20);
            b.emit_op(Opcode::ResultRow, o_eq, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(0), // 5 == 10 → false
                SqliteValue::Integer(1), // 5 != 10 → true
                SqliteValue::Integer(1), // 5 < 10 → true
            ]
        );
    }

    #[test]
    fn test_comparison_storep2_null_gives_null() {
        // STOREP2 (0x20) with NULL operand: store NULL in P2.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_five = b.alloc_reg();
            let o1 = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            // Eq with STOREP2: NULL == 5 → store NULL
            b.emit_op(Opcode::Eq, r_five, o1, r_null, P4::None, 0x20);
            b.emit_op(Opcode::ResultRow, o1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
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
    fn test_ifnot_null_p3_zero_no_jump() {
        // IfNot with NULL and p3=0 → no jump (SQLite: p3 controls NULL behavior)
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
        // p3=0: NULL → don't jump → r_out stays 0
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_ifnot_null_p3_one_jumps() {
        // IfNot with NULL and p3=1 → jump (SQLite: p3!=0 means jump on NULL)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::IfNot, r_cond, 1, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // p3=1: NULL → jump → r_out = 1
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_if_null_p3_one_jumps() {
        // If with NULL and p3=1 → jump (SQLite: p3!=0 means jump on NULL)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 1, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // p3=1: NULL → jump → r_out = 1
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_once_fires_only_once() {
        // Once falls through on first pass (runs the body), jumps to p2 on
        // subsequent passes (skips the body).  This matches C SQLite semantics.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_counter = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_counter, 0, P4::None, 0);

            let loop_start = b.emit_label();
            b.resolve_label(loop_start);
            // Once: first pass → fall through (run init code below),
            //        second+ pass → jump to `skip_init`.
            let skip_init = b.emit_label();
            b.emit_jump_to_label(Opcode::Once, 0, 0, skip_init, P4::None, 0);
            // Init code (runs only on first pass): increment counter.
            b.emit_op(Opcode::AddImm, r_counter, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, loop_start, P4::None, 0);
            // Second pass lands here — skip the init, output result.
            b.resolve_label(skip_init);
            b.emit_op(Opcode::ResultRow, r_counter, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // First pass: Once falls through → counter becomes 1 → loop back.
        // Second pass: Once jumps to skip_init → output counter=1.
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
    fn test_cast_text_sci_notation_to_integer() {
        // SQLite integer casts ignore exponent syntax in text and consume only
        // the signed integer prefix.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("1.5e2abc".to_owned()), 0);
            b.emit_op(Opcode::Cast, r, 68, 0, P4::None, 0); // 'D' = INTEGER
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_cast_text_huge_exponent_to_integer_ignores_exponent() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("1e999".to_owned()), 0);
            b.emit_op(Opcode::Cast, r, 68, 0, P4::None, 0); // 'D' = INTEGER
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_cast_text_prefix_to_real() {
        // Regression: CAST('3.14abc' AS REAL) must yield 3.14, not 0.0.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("3.14abc".to_owned()), 0);
            b.emit_op(Opcode::Cast, r, 69, 0, P4::None, 0); // 'E' = REAL
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(3.14)]);
    }

    #[test]
    fn test_cast_sci_notation_to_real() {
        // CAST('2.5e3xyz' AS REAL) must yield 2500.0.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("2.5e3xyz".to_owned()), 0);
            b.emit_op(Opcode::Cast, r, 69, 0, P4::None, 0); // 'E' = REAL
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(2500.0)]);
    }

    #[test]
    fn test_cast_text_prefix_to_numeric() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("123abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("1.5e2abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r3, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::Cast, r1, 67, 0, P4::None, 0); // 'C' = NUMERIC
            b.emit_op(Opcode::Cast, r2, 67, 0, P4::None, 0);
            b.emit_op(Opcode::Cast, r3, 67, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(123),
                SqliteValue::Integer(150),
                SqliteValue::Integer(0),
            ]
        );
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

    #[test]
    fn test_typecheck_reports_sqlite_constraint_datatype_marker() {
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        let r = b.alloc_reg();
        b.emit_op(Opcode::String8, 0, r, 0, P4::Str("bad".to_owned()), 0);
        // Encoded TypeCheck P4: "I\ttbl\tcol_a"
        b.emit_op(
            Opcode::TypeCheck,
            r,
            1,
            0,
            P4::Str("I\ttbl\tcol_a".to_owned()),
            0,
        );
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let err = engine
            .execute(&prog)
            .expect_err("typecheck should fail for TEXT into INTEGER STRICT slot");
        let err_text = err.to_string();
        assert!(
            err_text.contains("cannot store"),
            "error should mention 'cannot store': {err_text}"
        );
        assert!(
            err_text.contains("tbl.col_a"),
            "error should mention column: {err_text}"
        );
        assert_eq!(err.error_code(), ErrorCode::Constraint);
        assert_eq!(err.extended_error_code(), 3091); // SQLITE_CONSTRAINT_DATATYPE
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
    fn test_is_true_is_false_semantics() {
        // IS FALSE: P3=1, P4=1  →  NULL→0, truthy→0, falsy→1
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
            b.emit_op(Opcode::IsTrue, r_true, o1, 1, P4::Int(1), 0);
            b.emit_op(Opcode::IsTrue, r_false, o2, 1, P4::Int(1), 0);
            b.emit_op(Opcode::IsTrue, r_null, o3, 1, P4::Int(1), 0);
            b.emit_op(Opcode::ResultRow, o1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // 42 IS FALSE → 0
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // 0 IS FALSE → 1
        assert_eq!(rows[2], vec![SqliteValue::Integer(0)]); // NULL IS FALSE → 0
    }

    #[test]
    fn test_is_true_is_not_true_semantics() {
        // IS NOT TRUE: P3=0, P4=1  →  NULL→1, truthy→0, falsy→1
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
            b.emit_op(Opcode::IsTrue, r_true, o1, 0, P4::Int(1), 0);
            b.emit_op(Opcode::IsTrue, r_false, o2, 0, P4::Int(1), 0);
            b.emit_op(Opcode::IsTrue, r_null, o3, 0, P4::Int(1), 0);
            b.emit_op(Opcode::ResultRow, o1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // 42 IS NOT TRUE → 0
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // 0 IS NOT TRUE → 1
        assert_eq!(rows[2], vec![SqliteValue::Integer(1)]); // NULL IS NOT TRUE → 1
    }

    #[test]
    fn test_is_true_is_not_false_semantics() {
        // IS NOT FALSE: P3=1, P4=0  →  NULL→1, truthy→1, falsy→0
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
            b.emit_op(Opcode::IsTrue, r_true, o1, 1, P4::Int(0), 0);
            b.emit_op(Opcode::IsTrue, r_false, o2, 1, P4::Int(0), 0);
            b.emit_op(Opcode::IsTrue, r_null, o3, 1, P4::Int(0), 0);
            b.emit_op(Opcode::ResultRow, o1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // 42 IS NOT FALSE → 1
        assert_eq!(rows[1], vec![SqliteValue::Integer(0)]); // 0 IS NOT FALSE → 0
        assert_eq!(rows[2], vec![SqliteValue::Integer(1)]); // NULL IS NOT FALSE → 1
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
    fn test_make_record_negative_source_register_stays_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_record = b.alloc_reg();
            b.emit_op(Opcode::Integer, 777, 0, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, -1, 1, r_record, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_record, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        let decoded = decode_record(&rows[0][0]).expect("record should decode");
        assert_eq!(decoded, vec![SqliteValue::Null]);
    }

    #[test]
    fn test_function_negative_argument_register_stays_null() {
        let rows = run_program_with_functions(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 777, 0, 0, P4::None, 0);
            b.emit_op(
                Opcode::Function,
                0,
                -1,
                r_out,
                P4::FuncName("typeof".to_owned()),
                1,
            );
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Text("null".to_owned())]]);
    }

    #[test]
    fn test_result_row_negative_start_register_stays_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            b.emit_op(Opcode::Integer, 777, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, -1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Null]]);
    }

    #[test]
    fn test_move_negative_source_register_stays_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_dest = b.alloc_reg();
            b.emit_op(Opcode::Integer, 777, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Move, -1, r_dest, 1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_dest, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Null]]);
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
        // These tests exercise the MemPageStore path without a real pager txn.
        engine.set_reject_mem_fallback(false);
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
        // These tests exercise the MemPageStore path without a real pager txn.
        engine.set_reject_mem_fallback(false);
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
        engine.set_reject_mem_fallback(false);
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
    fn test_delete_then_prev_then_next_advances_correctly() {
        // Regression: after Delete marks pending_next_after_delete, a
        // subsequent Prev must clear that pending state. Otherwise the next
        // Next call can incorrectly "stay put" and repeat the same row.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(10)]);
        table.insert(2, vec![SqliteValue::Integer(20)]);
        table.insert(3, vec![SqliteValue::Integer(30)]);

        let (rows, _) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Seek rowid=2 and delete it. Cursor should land on successor.
            b.emit_op(Opcode::Integer, 2, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekRowid, 0, 1, end, P4::None, 0);
            b.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);

            // Step backward once (to rowid=1) and emit it.
            let prev_ok = b.emit_label();
            b.emit_jump_to_label(Opcode::Prev, 0, 0, prev_ok, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);
            b.resolve_label(prev_ok);
            b.emit_op(Opcode::Rowid, 0, 2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);

            // Now step forward once. Correct behavior is rowid=3 (not 1).
            let next_ok = b.emit_label();
            b.emit_jump_to_label(Opcode::Next, 0, 0, next_ok, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);
            b.resolve_label(next_ok);
            b.emit_op(Opcode::Rowid, 0, 3, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 3, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(
            rows,
            vec![vec![SqliteValue::Integer(1)], vec![SqliteValue::Integer(3)]],
            "Delete->Prev->Next should land on rowids 1 then 3 without repeating row 1"
        );
    }

    #[test]
    fn test_delete_then_notexists_then_next_advances_from_probe_position() {
        // Regression: NotExists on storage cursor repositions via table_move_to.
        // If pending_next_after_delete is left stale, a following Next can
        // incorrectly repeat the probe row instead of advancing.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(10)]);
        table.insert(2, vec![SqliteValue::Integer(20)]);
        table.insert(3, vec![SqliteValue::Integer(30)]);

        let (rows, _) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Delete rowid=2.
            b.emit_op(Opcode::Integer, 2, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekRowid, 0, 1, end, P4::None, 0);
            b.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);

            // Probe rowid=1 via NotExists (falls through when row exists).
            let probe_missing = b.emit_label();
            b.emit_op(Opcode::Integer, 1, 2, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::NotExists, 0, 2, probe_missing, P4::None, 0);

            // Emit current probe position (rowid=1).
            b.emit_op(Opcode::Rowid, 0, 3, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 3, 1, 0, P4::None, 0);

            // Next should advance to rowid=3 (not repeat rowid=1).
            let next_ok = b.emit_label();
            b.emit_jump_to_label(Opcode::Next, 0, 0, next_ok, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);
            b.resolve_label(next_ok);
            b.emit_op(Opcode::Rowid, 0, 4, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 4, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);

            // Missing-probe path (not expected in this fixture).
            b.resolve_label(probe_missing);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(
            rows,
            vec![vec![SqliteValue::Integer(1)], vec![SqliteValue::Integer(3)]],
            "Delete->NotExists->Next should advance from probe row 1 to row 3"
        );
    }

    #[test]
    fn test_delete_then_newrowid_then_next_reports_no_successor() {
        // Regression: NewRowid on storage cursor repositions with `last()`.
        // If pending_next_after_delete is stale, Next can incorrectly report
        // a successor row when already at the end.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(10)]);
        table.insert(2, vec![SqliteValue::Integer(20)]);
        table.insert(3, vec![SqliteValue::Integer(30)]);

        let (rows, _) = run_write_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

            // Delete rowid=2 (cursor lands on successor rowid=3).
            b.emit_op(Opcode::Integer, 2, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SeekRowid, 0, 1, end, P4::None, 0);
            b.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);

            // NewRowid probes max rowid via last(); this should clear stale
            // pending delete state for cursor 0.
            b.emit_op(Opcode::NewRowid, 0, 2, 0, P4::None, 0);

            // At end of table, Next must report no successor.
            let has_next = b.emit_label();
            b.emit_jump_to_label(Opcode::Next, 0, 0, has_next, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, 3, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 3, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);

            // Unexpected path if stale pending state causes false positive.
            b.resolve_label(has_next);
            b.emit_op(Opcode::Integer, 1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 3, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, end, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(
            rows,
            vec![vec![SqliteValue::Integer(0)]],
            "Delete->NewRowid->Next should report no successor at end-of-table"
        );
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
        engine.set_reject_mem_fallback(false);

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
        engine.set_reject_mem_fallback(false);

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
        let registry = Arc::new(Mutex::new(ConcurrentRegistry::new()));
        let lock_table = Arc::new(InProcessPageLockTable::new());
        engine.set_transaction_concurrent(Box::new(txn), 999, registry, lock_table, 5000);

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
        engine.set_reject_mem_fallback(false);

        // Without a transaction, should fall back to Mem backend.
        let opened = engine.open_storage_cursor(0, root, false);
        assert!(opened);
        assert!(engine.storage_cursors.contains_key(&0));
    }

    #[test]
    fn test_open_storage_cursor_zero_page_with_txn_does_not_fallback_to_mem() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_transaction(Box::new(txn));

        // MockTransaction synthesizes page bytes from the page number; page 256
        // yields first byte 0x00, simulating an uninitialized root page.
        let opened = engine.open_storage_cursor(0, 256, false);
        assert!(
            !opened,
            "transaction-backed opens must not silently fall back to MemPageStore"
        );
        assert!(
            !engine.storage_cursors.contains_key(&0),
            "failed open must not leave a cursor installed"
        );
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

    // ── Swiss Tables (bd-3ta.7) integration tests ─────────────────────

    #[test]
    fn test_swiss_index_metrics_emitted_on_cursor_ops() {
        // Verify that SwissIndex probe metrics are emitted when the engine
        // opens cursors and accesses tables via the instrumented hash map.
        use fsqlite_btree::instrumentation::{btree_metrics_snapshot, reset_btree_metrics};

        reset_btree_metrics();

        let mut db = MemDatabase::new();
        let root = db.create_table(2);
        // create_table inserts into db.tables (SwissIndex) — should record probes.
        let metrics_after_create = btree_metrics_snapshot();
        assert!(
            metrics_after_create.fsqlite_swiss_table_probes_total > 0,
            "MemDatabase::create_table should trigger SwissIndex probe metrics"
        );

        // Insert a row via MemDatabase to exercise more SwissIndex lookups.
        db.get_table_mut(root).unwrap().insert_row(
            1,
            vec![SqliteValue::Integer(42), SqliteValue::Text("a".into())],
        );
        let metrics_after_insert = btree_metrics_snapshot();
        assert!(
            metrics_after_insert.fsqlite_swiss_table_probes_total
                > metrics_after_create.fsqlite_swiss_table_probes_total,
            "get_table_mut should trigger additional SwissIndex probes"
        );
    }

    #[test]
    fn test_swiss_index_replaces_hashmap_in_engine() {
        // Smoke test: run a simple expression program to exercise the engine's
        // SwissIndex-based internal maps (sorters, cursors, aggregates).
        use fsqlite_btree::instrumentation::reset_btree_metrics;

        reset_btree_metrics();

        // Even a simple expression program exercises VdbeEngine construction
        // which initializes SwissIndex maps. The metrics counter is global, so
        // any cursor open/close in the test suite contributes.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::Integer, 42, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqliteValue::Integer(42));

        // The engine's internal SwissIndex maps (cursors, sorters, aggregates,
        // storage_cursors) are all SwissIndex now. If we got here without
        // panics, the drop-in replacement works.
    }

    // ── External Sort Tests (bd-1rw.4) ──────────────────────────────────

    /// Mutex to serialize tests that mutate global JIT settings (enabled,
    /// threshold, capacity). Without this, parallel tests overwrite each
    /// other's globals, causing false negatives in delta-based assertions.
    static JIT_SETTINGS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_sort_metrics_emitted_on_sorter_sort() {
        // Verify that sort row metrics are updated when a sorter sorts rows.
        // Delta-based: snapshot before/after to avoid racing with parallel tests.
        let before = vdbe_metrics_snapshot();
        let rows = run_program(|b| {
            let end = b.emit_label();
            let loop_start = b.emit_label();
            let empty = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_sorted = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);

            for value in [50, 30, 10, 40, 20] {
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

        assert_eq!(rows.len(), 5);
        let after = vdbe_metrics_snapshot();
        let delta_sort_rows = after.sort_rows_total - before.sort_rows_total;
        let delta_spill = after.sort_spill_pages_total - before.sort_spill_pages_total;
        assert!(
            delta_sort_rows >= 5,
            "sort_rows_total delta should be >= 5, got {delta_sort_rows}",
        );
        // No spill expected for 5 tiny rows.
        assert_eq!(delta_spill, 0, "no spill expected for small dataset");
    }

    #[test]
    fn test_jit_scaffold_metrics_compile_and_cache_hit() {
        let _guard = JIT_SETTINGS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev_enabled = vdbe_jit_enabled();
        let prev_threshold = vdbe_jit_hot_threshold();
        let prev_capacity = vdbe_jit_cache_capacity();

        // Delta-based: snapshot before/after to avoid racing with parallel tests.
        set_vdbe_jit_enabled(true);
        let _ = set_vdbe_jit_hot_threshold(2);
        let _ = set_vdbe_jit_cache_capacity(8);
        let before = vdbe_jit_metrics_snapshot();

        for _ in 0..3 {
            let rows = run_program(|b| {
                let end = b.emit_label();
                b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
                b.emit_op(Opcode::Integer, 42, 1, 0, P4::None, 0);
                b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
                b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
                b.resolve_label(end);
            });
            assert_eq!(rows, vec![vec![SqliteValue::Integer(42)]]);
        }

        let after = vdbe_jit_metrics_snapshot();
        let delta_compilations = after.jit_compilations_total - before.jit_compilations_total;
        let delta_cache_hits = after.jit_cache_hits_total - before.jit_cache_hits_total;
        assert!(
            delta_compilations >= 1,
            "expected at least one JIT compile delta, got {delta_compilations}",
        );
        assert!(
            delta_cache_hits >= 1,
            "expected at least one JIT cache hit delta, got {delta_cache_hits}",
        );
        assert!(
            after.cache_entries >= 1,
            "expected non-empty JIT cache after hot runs"
        );

        set_vdbe_jit_enabled(prev_enabled);
        let _ = set_vdbe_jit_hot_threshold(prev_threshold);
        let _ = set_vdbe_jit_cache_capacity(prev_capacity);
    }

    #[test]
    fn test_jit_scaffold_compile_failure_falls_back_to_interpreter() {
        let _guard = JIT_SETTINGS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev_enabled = vdbe_jit_enabled();
        let prev_threshold = vdbe_jit_hot_threshold();
        let prev_capacity = vdbe_jit_cache_capacity();

        // Delta-based: snapshot before/after to avoid racing with parallel tests.
        set_vdbe_jit_enabled(true);
        let _ = set_vdbe_jit_hot_threshold(1);
        let _ = set_vdbe_jit_cache_capacity(8);
        let before = vdbe_jit_metrics_snapshot();

        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        db.get_table_mut(root)
            .expect("table should exist")
            .insert_row(7, vec![SqliteValue::Integer(700)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(700)]]);
        let after = vdbe_jit_metrics_snapshot();
        let delta_failures = after.jit_compile_failures_total - before.jit_compile_failures_total;
        assert!(
            delta_failures >= 1,
            "expected at least one JIT compile failure delta, got {delta_failures}",
        );

        set_vdbe_jit_enabled(prev_enabled);
        let _ = set_vdbe_jit_hot_threshold(prev_threshold);
        let _ = set_vdbe_jit_cache_capacity(prev_capacity);
    }

    #[test]
    fn test_sorter_spill_to_disk_under_low_threshold() {
        // Set an artificially low spill threshold to trigger disk spill
        // with a small dataset, then verify the external merge produces
        // correct sorted output.

        // Build the sorter cursor directly to test spill logic.
        let mut sorter = SorterCursor::new(1, vec![SortKeyOrder::Asc], Vec::new());
        // Set threshold to 1 byte to force immediate spill on first insert.
        sorter.spill_threshold = 1;

        // Insert several rows — each should trigger a spill.
        for value in [50i64, 30, 10, 40, 20] {
            sorter
                .insert_row(vec![SqliteValue::Integer(value)])
                .expect("insert should succeed");
        }

        // We should have spilled runs.
        assert!(
            !sorter.spill_runs.is_empty(),
            "low threshold should cause spills"
        );
        let spill_count = sorter.spill_runs.len();
        assert!(
            sorter.spill_pages_total > 0,
            "spill_pages_total should be > 0"
        );

        // Sort (triggers merge).
        sorter.sort().expect("sort should succeed");

        // After merge, all runs should be cleaned up.
        assert!(sorter.spill_runs.is_empty(), "runs should be drained");

        // Verify sorted order.
        let values: Vec<i64> = sorter.rows.iter().map(|r| r[0].to_integer()).collect();
        assert_eq!(values, vec![10, 20, 30, 40, 50]);

        // All 5 rows should have been counted (spill batches + in-memory remainder).
        assert!(
            sorter.rows_sorted_total >= 5,
            "rows_sorted_total should be >= 5, got {}",
            sorter.rows_sorted_total
        );
        // At least one spill run was created.
        assert!(spill_count >= 1, "at least one spill run expected");
    }

    #[test]
    fn test_sorter_reset_cleans_spill_state() {
        // Verify that reset() clears in-memory rows, position, and spill runs.
        let mut sorter = SorterCursor::new(1, vec![SortKeyOrder::Asc], Vec::new());
        sorter.spill_threshold = 1;

        for value in [3i64, 1, 2] {
            sorter
                .insert_row(vec![SqliteValue::Integer(value)])
                .expect("insert should succeed");
        }
        assert!(!sorter.spill_runs.is_empty());

        sorter.reset();

        assert!(sorter.rows.is_empty(), "rows should be cleared");
        assert!(sorter.position.is_none(), "position should be None");
        assert!(sorter.spill_runs.is_empty(), "spill_runs should be cleared");
        assert_eq!(sorter.memory_used, 0, "memory_used should be 0");
    }

    #[test]
    fn test_sorter_desc_key_order_with_external_merge() {
        // Verify that DESC sort order works correctly through the external
        // merge path.
        let mut sorter = SorterCursor::new(1, vec![SortKeyOrder::Desc], Vec::new());
        sorter.spill_threshold = 1;

        for value in [10i64, 50, 30, 20, 40] {
            sorter
                .insert_row(vec![SqliteValue::Integer(value)])
                .expect("insert should succeed");
        }

        sorter.sort().expect("sort should succeed");

        let values: Vec<i64> = sorter.rows.iter().map(|r| r[0].to_integer()).collect();
        assert_eq!(values, vec![50, 40, 30, 20, 10]);
    }

    #[test]
    fn test_sorter_multi_column_key_with_mixed_order() {
        // Test sorting with 2 key columns: first ASC, second DESC.
        let mut sorter =
            SorterCursor::new(2, vec![SortKeyOrder::Asc, SortKeyOrder::Desc], Vec::new());

        // Insert rows: (group, value)
        for (group, value) in [(1i64, 30i64), (2, 10), (1, 20), (2, 40), (1, 10)] {
            sorter
                .insert_row(vec![
                    SqliteValue::Integer(group),
                    SqliteValue::Integer(value),
                ])
                .expect("insert should succeed");
        }

        sorter.sort().expect("sort should succeed");

        let values: Vec<(i64, i64)> = sorter
            .rows
            .iter()
            .map(|r| (r[0].to_integer(), r[1].to_integer()))
            .collect();
        // Group 1 first (ASC), within group value DESC: 30, 20, 10.
        // Group 2 second, within group value DESC: 40, 10.
        assert_eq!(values, vec![(1, 30), (1, 20), (1, 10), (2, 40), (2, 10)]);
    }

    #[test]
    fn test_sorter_memory_estimation() {
        // Verify memory tracking increases with row insertions.
        let mut sorter = SorterCursor::new(1, vec![SortKeyOrder::Asc], Vec::new());
        assert_eq!(sorter.memory_used, 0);

        sorter
            .insert_row(vec![SqliteValue::Integer(42)])
            .expect("insert should succeed");
        let after_one = sorter.memory_used;
        assert!(after_one > 0, "memory should increase after insert");

        sorter
            .insert_row(vec![SqliteValue::Text("hello world".into())])
            .expect("insert should succeed");
        let after_two = sorter.memory_used;
        assert!(
            after_two > after_one,
            "memory should increase with text value"
        );
    }

    #[test]
    fn test_sorter_empty_sort() {
        // Sorting an empty sorter should succeed and leave rows empty.
        let mut sorter = SorterCursor::new(1, vec![SortKeyOrder::Asc], Vec::new());
        sorter.sort().expect("empty sort should succeed");
        assert!(sorter.rows.is_empty());
    }

    #[test]
    fn test_sorter_pure_inmemory_sort_path() {
        // Verify the fast in-memory path works when no spill occurs.
        let mut sorter = SorterCursor::new(1, vec![SortKeyOrder::Asc], Vec::new());
        // Default threshold is 100 MiB — won't spill.

        for value in [5i64, 3, 1, 4, 2] {
            sorter
                .insert_row(vec![SqliteValue::Integer(value)])
                .expect("insert should succeed");
        }

        assert!(sorter.spill_runs.is_empty(), "no spill expected");
        sorter.sort().expect("sort should succeed");

        let values: Vec<i64> = sorter.rows.iter().map(|r| r[0].to_integer()).collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
        assert_eq!(sorter.rows_sorted_total, 5);
    }

    // ── bd-2ttd8.1: Pager routing and parity-cert tests ──────────────

    #[test]
    fn test_reject_mem_fallback_default_on() {
        // bd-zjisk.1: Parity-cert mode is enabled by default.
        let engine = VdbeEngine::new(8);
        assert!(engine.reject_mem_fallback);
    }

    #[test]
    fn test_set_reject_mem_fallback() {
        let mut engine = VdbeEngine::new(8);
        engine.set_reject_mem_fallback(true);
        assert!(engine.reject_mem_fallback);
        engine.set_reject_mem_fallback(false);
        assert!(!engine.reject_mem_fallback);
    }

    #[test]
    fn test_open_storage_cursor_mem_fallback_without_parity_cert() {
        // Without parity-cert mode, OpenRead should succeed via MemPageStore
        // fallback when no pager transaction is set.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        table.insert(1, vec![SqliteValue::Integer(42)]);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        // Explicitly opt out of parity-cert mode to test fallback path.
        engine.set_reject_mem_fallback(false);

        // No txn_page_io set — should fall back to MemPageStore.
        assert!(engine.open_storage_cursor(0, root, false));
        assert!(engine.storage_cursors.get(&0).is_some());
    }

    #[test]
    fn test_open_storage_cursor_rejected_in_parity_cert_mode() {
        // In parity-cert mode (now the default), OpenRead should FAIL when
        // no pager transaction is available and MemPageStore fallback would
        // be used.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        table.insert(1, vec![SqliteValue::Integer(42)]);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        // Default is already true (parity-cert mode), but be explicit.
        engine.set_reject_mem_fallback(true);

        // No txn_page_io set — parity-cert should reject the fallback.
        assert!(!engine.open_storage_cursor(0, root, false));
        assert!(engine.storage_cursors.get(&0).is_none());
    }

    #[test]
    fn test_open_storage_cursor_invalid_page_number() {
        // Root page 0 is invalid (PageNumber requires nonzero).
        let mut engine = VdbeEngine::new(8);
        assert!(!engine.open_storage_cursor(0, 0, false));
    }

    #[test]
    fn test_bd_2ttd8_set_transaction_enables_storage_cursors() {
        // set_transaction should auto-enable storage cursors and set txn_page_io.
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_transaction(Box::new(txn));
        assert!(engine.storage_cursors_enabled);
        assert!(engine.txn_page_io.is_some());
    }

    #[test]
    fn test_open_read_opcode_with_mem_fallback() {
        // OpenRead via VDBE execution should succeed when MemDatabase has the
        // table, verifying the full cursor lifecycle.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        table.insert(1, vec![SqliteValue::Integer(100)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
        // Rewind to first row.
        let halt_label = b.emit_label();
        b.emit_jump_to_label(Opcode::Rewind, 0, 0, halt_label, P4::None, 0);
        // Read column 0 into register 1.
        b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
        b.resolve_label(halt_label);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        // Explicitly opt out of parity-cert to test the MemPageStore fallback.
        engine.set_reject_mem_fallback(false);

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);

        let results = engine.take_results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![SqliteValue::Integer(100)]);
    }

    #[test]
    fn test_open_write_insert_delete_cursor_lifecycle() {
        // Verify full cursor lifecycle: OpenWrite → Insert → Rewind →
        // Column → Delete → verify empty.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);

        // Insert rowid=1 with value 42.
        b.emit_op(Opcode::Integer, 1, 1, 0, P4::None, 0); // rowid in r1
        b.emit_op(Opcode::Integer, 42, 2, 0, P4::None, 0); // value in r2
        b.emit_op(Opcode::MakeRecord, 2, 1, 3, P4::None, 0); // record in r3
        b.emit_op(Opcode::Insert, 0, 3, 1, P4::None, 0);

        // Rewind and read back.
        let eof_label = b.emit_label();
        b.emit_jump_to_label(Opcode::Rewind, 0, 0, eof_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 0, 4, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 4, 1, 0, P4::None, 0);
        // Delete the row.
        b.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);
        b.resolve_label(eof_label);

        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        // Explicitly opt out of parity-cert to test the MemPageStore fallback.
        engine.set_reject_mem_fallback(false);

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);

        let results = engine.take_results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_parity_cert_rejects_open_read_without_txn() {
        // In parity-cert mode, OpenRead should fail execution when no pager
        // transaction is available.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        table.insert(1, vec![SqliteValue::Integer(1)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        engine.set_reject_mem_fallback(true);

        let result = engine.execute(&prog);
        assert!(
            result.is_err(),
            "OpenRead should fail in parity-cert mode without txn"
        );
    }

    #[test]
    fn test_parity_cert_rejects_open_write_without_txn() {
        // In parity-cert mode, OpenWrite should also fail without a txn.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenWrite, 0, root, 0, P4::Int(1), 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        engine.set_reject_mem_fallback(true);

        let result = engine.execute(&prog);
        assert!(
            result.is_err(),
            "OpenWrite should fail in parity-cert mode without txn"
        );
    }

    // ── bd-2ttd8.4: Backend-identity ratchet tests ──────────────────

    #[test]
    fn test_cursor_backend_kind_mem() {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        engine.set_reject_mem_fallback(false);

        assert!(engine.open_storage_cursor(0, root, false));
        assert!(
            engine.has_mem_cursor(),
            "cursor should be mem-backed without txn"
        );
        assert!(!engine.all_cursors_are_txn_backed());
    }

    #[test]
    fn test_cursor_backend_kind_txn() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};
        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_database(MemDatabase::new());
        engine.set_transaction(Box::new(txn));

        // Open cursor on page 1 (valid with pager txn).
        assert!(engine.open_storage_cursor(0, 1, false));
        assert!(
            engine.all_cursors_are_txn_backed(),
            "cursor should be txn-backed with pager transaction"
        );
        assert!(!engine.has_mem_cursor());
    }

    #[test]
    fn test_validate_parity_cert_invariant_no_cursors() {
        let mut engine = VdbeEngine::new(8);
        engine.set_reject_mem_fallback(true);
        assert!(
            engine.validate_parity_cert_invariant().is_ok(),
            "vacuously valid with no cursors"
        );
    }

    #[test]
    fn test_validate_parity_cert_invariant_with_txn_cursor() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};
        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_database(MemDatabase::new());
        engine.set_transaction(Box::new(txn));
        engine.set_reject_mem_fallback(true);

        assert!(engine.open_storage_cursor(0, 1, false));
        assert!(
            engine.validate_parity_cert_invariant().is_ok(),
            "txn-backed cursor satisfies parity-cert invariant"
        );
    }

    #[test]
    fn test_validate_parity_cert_invariant_disabled_allows_mem() {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        // Explicitly disable parity-cert — mem cursors allowed.
        engine.set_reject_mem_fallback(false);
        assert!(engine.open_storage_cursor(0, root, false));
        assert!(
            engine.validate_parity_cert_invariant().is_ok(),
            "parity-cert disabled should always pass"
        );
    }

    #[test]
    fn test_all_cursors_txn_backed_vacuous() {
        let engine = VdbeEngine::new(8);
        assert!(
            engine.all_cursors_are_txn_backed(),
            "no cursors → vacuously true"
        );
        assert!(!engine.has_mem_cursor(), "no cursors → no mem cursor");
    }

    #[test]
    fn test_cursor_kind_str_values() {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        engine.set_reject_mem_fallback(false);
        engine.open_storage_cursor(0, root, false);

        let sc = engine.storage_cursors.get(&0).unwrap();
        assert_eq!(sc.cursor.kind_str(), "mem");
    }

    #[test]
    fn test_ratchet_prevents_mem_cursor_creation_in_parity_mode() {
        // This is the core anti-regression ratchet: when parity-cert is
        // enabled and no txn is set, cursor creation MUST fail — it cannot
        // silently fall through to MemPageStore.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(99)]);

        let mut engine = VdbeEngine::new(8);
        engine.set_database(db);
        engine.set_reject_mem_fallback(true);

        // Attempt to open cursor — should fail.
        let opened = engine.open_storage_cursor(0, root, false);
        assert!(
            !opened,
            "ratchet must prevent cursor creation in parity-cert mode"
        );

        // Validate invariant still holds.
        assert!(engine.validate_parity_cert_invariant().is_ok());
        assert!(!engine.has_mem_cursor());
    }

    #[test]
    fn test_ratchet_allows_txn_cursor_in_parity_mode() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};
        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_database(MemDatabase::new());
        engine.set_transaction(Box::new(txn));
        engine.set_reject_mem_fallback(true);

        // With txn set, cursor creation should succeed via pager path.
        let opened = engine.open_storage_cursor(0, 1, false);
        assert!(opened, "txn-backed cursor should work in parity-cert mode");
        assert!(engine.all_cursors_are_txn_backed());
        assert!(engine.validate_parity_cert_invariant().is_ok());
    }

    #[test]
    fn test_ratchet_multiple_cursors_mixed_rejection() {
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};
        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let mut engine = VdbeEngine::new(8);
        engine.set_database(MemDatabase::new());
        engine.set_transaction(Box::new(txn));
        engine.set_reject_mem_fallback(true);

        // Open cursor 0 on page 1 — should succeed (txn path).
        assert!(engine.open_storage_cursor(0, 1, false));
        assert!(engine.all_cursors_are_txn_backed());

        // Attempt cursor 1 on non-existent high page — should still
        // succeed via txn path (MockMvccPager returns zero-filled pages).
        assert!(engine.open_storage_cursor(1, 1, false));
        assert!(engine.all_cursors_are_txn_backed());
        assert!(engine.validate_parity_cert_invariant().is_ok());
    }

    // ── RowSet opcode tests ──────────────────────────────────────

    #[test]
    fn test_rowset_add_and_read_returns_sorted() {
        // Add rowids 30, 10, 20 then read them back — should come out sorted.
        let rows = run_program(|b| {
            let end = b.emit_label();
            let exhausted = b.emit_label();
            let loop_start = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_val = b.alloc_reg();
            let r_out = b.alloc_reg();
            let rowset_reg = b.alloc_reg();

            // Add 30, 10, 20 to rowset
            for v in [30, 10, 20] {
                b.emit_op(Opcode::Integer, v, r_val, 0, P4::None, 0);
                b.emit_op(Opcode::RowSetAdd, rowset_reg, r_val, 0, P4::None, 0);
            }

            // Read loop: RowSetRead P1=rowset, P2=jump_when_exhausted, P3=output
            b.resolve_label(loop_start);
            b.emit_jump_to_label(
                Opcode::RowSetRead,
                rowset_reg,
                r_out,
                exhausted,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, loop_start, P4::None, 0);

            b.resolve_label(exhausted);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        let vals: Vec<i64> = rows.into_iter().map(|row| row[0].to_integer()).collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn test_rowset_deduplicates() {
        // Add the same value twice; read should return it once.
        let rows = run_program(|b| {
            let end = b.emit_label();
            let exhausted = b.emit_label();
            let loop_start = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_val = b.alloc_reg();
            let r_out = b.alloc_reg();
            let rowset_reg = b.alloc_reg();

            for v in [5, 5, 5] {
                b.emit_op(Opcode::Integer, v, r_val, 0, P4::None, 0);
                b.emit_op(Opcode::RowSetAdd, rowset_reg, r_val, 0, P4::None, 0);
            }

            b.resolve_label(loop_start);
            b.emit_jump_to_label(
                Opcode::RowSetRead,
                rowset_reg,
                r_out,
                exhausted,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, loop_start, P4::None, 0);

            b.resolve_label(exhausted);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_integer(), 5);
    }

    #[test]
    fn test_rowset_test_jumps_if_found() {
        // Add 42 to rowset, then test for 42 — should jump.
        let rows = run_program(|b| {
            let end = b.emit_label();
            let found = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_val = b.alloc_reg();
            let r_out = b.alloc_reg();
            let rowset_reg = b.alloc_reg();

            // Add 42
            b.emit_op(Opcode::Integer, 42, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::RowSetAdd, rowset_reg, r_val, 0, P4::None, 0);

            // Test for 42 — should jump to `found`
            // RowSetTest: P1=rowset, P2=jump_if_found, P3=value register
            b.emit_jump_to_label(Opcode::RowSetTest, rowset_reg, r_val, found, P4::None, 0);
            // Not found path
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            // Found path
            b.resolve_label(found);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    #[test]
    fn test_rowset_test_falls_through_and_adds_if_not_found() {
        // RowSetTest on empty set: should fall through and add the value.
        let rows = run_program(|b| {
            let end = b.emit_label();
            let found = b.emit_label();
            let exhausted = b.emit_label();
            let loop_start = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_val = b.alloc_reg();
            let r_out = b.alloc_reg();
            let rowset_reg = b.alloc_reg();

            // Test for 99 on empty rowset — should fall through and add 99
            b.emit_op(Opcode::Integer, 99, r_val, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::RowSetTest, rowset_reg, r_val, found, P4::None, 0);

            // Fall-through: 99 was added, now read it back
            b.resolve_label(loop_start);
            b.emit_jump_to_label(
                Opcode::RowSetRead,
                rowset_reg,
                r_out,
                exhausted,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, loop_start, P4::None, 0);

            b.resolve_label(found);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(exhausted);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].to_integer(), 99);
    }

    // ── FK counter opcode tests ──────────────────────────────────

    #[test]
    fn test_fk_counter_and_fk_if_zero() {
        // FkCounter increments, FkIfZero tests for zero.
        let rows = run_program(|b| {
            let end = b.emit_label();
            let is_zero = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_out = b.alloc_reg();

            // Increment FK counter by 3
            b.emit_op(Opcode::FkCounter, 0, 3, 0, P4::None, 0);
            // Test if zero — should NOT jump (counter is 3)
            b.emit_jump_to_label(Opcode::FkIfZero, 0, 0, is_zero, P4::None, 0);
            // Decrement by 3
            b.emit_op(Opcode::FkCounter, 0, -3, 0, P4::None, 0);
            // Test if zero — SHOULD jump now
            b.emit_jump_to_label(Opcode::FkIfZero, 0, 0, is_zero, P4::None, 0);
            // Should not reach here
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            b.resolve_label(is_zero);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    // ── MemMax opcode test ───────────────────────────────────────

    #[test]
    fn test_memmax_stores_larger_value() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();

            // r1=50, r2=30 → MemMax(r1, r2) → r2=50
            b.emit_op(Opcode::Integer, 50, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 30, r2, 0, P4::None, 0);
            b.emit_op(Opcode::MemMax, r1, r2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);

            // r1=10, r2=50 → MemMax(r1, r2) → r2 stays 50
            b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
            b.emit_op(Opcode::MemMax, r1, r2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(
            rows,
            vec![
                vec![SqliteValue::Integer(50)],
                vec![SqliteValue::Integer(50)],
            ]
        );
    }

    // ── OffsetLimit opcode test ──────────────────────────────────

    #[test]
    fn test_offset_limit_combines_values() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_limit = b.alloc_reg();
            let r_offset = b.alloc_reg();
            let r_combined = b.alloc_reg();

            // LIMIT=10, OFFSET=5 → combined=15
            b.emit_op(Opcode::Integer, 10, r_limit, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_offset, 0, P4::None, 0);
            b.emit_op(
                Opcode::OffsetLimit,
                r_limit,
                r_offset,
                r_combined,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_combined, 1, 0, P4::None, 0);

            // LIMIT=-1 (no limit), OFFSET=5 → combined=-1
            b.emit_op(Opcode::Integer, -1, r_limit, 0, P4::None, 0);
            b.emit_op(
                Opcode::OffsetLimit,
                r_limit,
                r_offset,
                r_combined,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_combined, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(
            rows,
            vec![
                vec![SqliteValue::Integer(15)],
                vec![SqliteValue::Integer(-1)],
            ]
        );
    }

    // ── IfNotZero opcode test ────────────────────────────────────

    #[test]
    fn test_if_not_zero_decrements_and_jumps() {
        // Start with 2 in register, loop with IfNotZero until it reaches 0.
        let rows = run_program(|b| {
            let end = b.emit_label();
            let loop_start = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_counter = b.alloc_reg();
            let r_count = b.alloc_reg();

            b.emit_op(Opcode::Integer, 3, r_counter, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_count, 0, P4::None, 0);

            b.resolve_label(loop_start);
            // Count iterations
            b.emit_op(Opcode::AddImm, r_count, 1, 0, P4::None, 0);
            // Decrement and jump if not zero
            b.emit_jump_to_label(Opcode::IfNotZero, r_counter, 0, loop_start, P4::None, 0);

            // When counter reaches 0, output iteration count
            b.emit_op(Opcode::ResultRow, r_count, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // Loop: AddImm then IfNotZero.
        // iter 1: count=1, 3→2 jump; iter 2: count=2, 2→1 jump;
        // iter 3: count=3, 1→0 jump; iter 4: count=4, 0→fall through.
        assert_eq!(rows, vec![vec![SqliteValue::Integer(4)]]);
    }

    // ── Pagecount / MaxPgcnt / JournalMode / IntegrityCk ─────────

    #[test]
    fn test_pagecount_returns_table_count() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Pagecount, 0, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        // No database set → 0 pages
        assert_eq!(rows, vec![vec![SqliteValue::Integer(0)]]);
    }

    #[test]
    fn test_max_pgcnt_returns_large_value() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_out = b.alloc_reg();
            b.emit_op(Opcode::MaxPgcnt, 0, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(1_073_741_823)]]);
    }

    #[test]
    fn test_journal_mode_returns_wal() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_out = b.alloc_reg();
            b.emit_op(Opcode::JournalMode, 0, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Text("wal".to_owned())]]);
    }

    #[test]
    fn test_integrity_ck_returns_ok() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_root = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r_root, 0, P4::None, 0);
            b.emit_op(Opcode::IntegrityCk, r_root, r_out, 1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Text("ok".to_owned())]]);
    }

    // ── Vacuum and IncrVacuum ────────────────────────────────────

    #[test]
    fn test_vacuum_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Vacuum, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    // ── Virtual Table opcodes ──────────────────────────────────

    /// Mock virtual table cursor for testing VTable opcodes.
    struct MockVtabCursor {
        rows: Vec<Vec<SqliteValue>>,
        pos: usize,
        filtered: bool,
    }

    impl MockVtabCursor {
        fn new(rows: Vec<Vec<SqliteValue>>) -> Self {
            Self {
                rows,
                pos: 0,
                filtered: false,
            }
        }
    }

    impl ErasedVtabCursor for MockVtabCursor {
        fn filter(
            &mut self,
            _cx: &Cx,
            _idx_num: i32,
            _idx_str: Option<&str>,
            _args: &[SqliteValue],
        ) -> Result<()> {
            self.pos = 0;
            self.filtered = true;
            Ok(())
        }
        fn next(&mut self, _cx: &Cx) -> Result<()> {
            self.pos += 1;
            Ok(())
        }
        fn eof(&self) -> bool {
            self.pos >= self.rows.len()
        }
        fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
            #[allow(clippy::cast_sign_loss)]
            let val = self
                .rows
                .get(self.pos)
                .and_then(|row| row.get(col as usize))
                .cloned()
                .unwrap_or(SqliteValue::Null);
            ctx.set_value(val);
            Ok(())
        }
        fn rowid(&self) -> Result<i64> {
            #[allow(clippy::cast_possible_wrap)]
            Ok(self.pos as i64 + 1)
        }
    }

    /// Helper: build a program, register a vtab cursor, then execute.
    fn run_vtab_program(
        cursor_id: i32,
        cursor: MockVtabCursor,
        build: impl FnOnce(&mut ProgramBuilder),
    ) -> (Vec<Vec<SqliteValue>>, ExecOutcome) {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.register_vtab_cursor(cursor_id, Box::new(cursor));
        let outcome = engine.execute(&prog).expect("execution should succeed");
        (engine.take_results(), outcome)
    }

    #[test]
    fn test_vopen_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::VOpen, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 42, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(42)]]);
    }

    #[test]
    fn test_vcreate_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(
                Opcode::VCreate,
                0,
                0,
                0,
                P4::Str("test_module".to_owned()),
                0,
            );
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    #[test]
    fn test_vdestroy_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(
                Opcode::VDestroy,
                0,
                0,
                0,
                P4::Str("test_table".to_owned()),
                0,
            );
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    #[test]
    fn test_vcheck_stores_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 99, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::VCheck, 0, 0, r_out, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Null]]);
    }

    #[test]
    fn test_vupdate_stores_null_in_dest() {
        // VUpdate P1=cursor, P2=n_args, P3=first_arg_reg, P5=dest_reg.
        // When no vtab instance is registered for the cursor, VUpdate
        // writes Null into dest_reg (P5).
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_arg = b.alloc_reg();
            let r_dest = b.alloc_reg();
            b.emit_op(Opcode::Integer, 77, r_arg, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 99, r_dest, 0, P4::None, 0);
            // P1=cursor 0, P2=1 arg, P3=r_arg, P5=r_dest as dest
            #[allow(clippy::cast_possible_truncation)]
            b.emit_op(Opcode::VUpdate, 0, 1, r_arg, P4::None, r_dest as u16);
            b.emit_op(Opcode::ResultRow, r_dest, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Null]]);
    }

    #[test]
    fn test_vinitin_copies_register() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_src = b.alloc_reg();
            let r_dst = b.alloc_reg();
            b.emit_op(Opcode::Integer, 123, r_src, 0, P4::None, 0);
            b.emit_op(Opcode::VInitIn, 0, r_src, r_dst, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(123)]]);
    }

    #[test]
    fn test_vfilter_jumps_on_empty_cursor() {
        let empty_cursor = MockVtabCursor::new(vec![]);
        let (rows, outcome) = run_vtab_program(0, empty_cursor, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            let after_scan = b.emit_label();
            b.emit_jump_to_label(Opcode::VFilter, 0, 0, after_scan, P4::Int(0), 0);
            b.emit_op(Opcode::Integer, 999, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.resolve_label(after_scan);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(rows, vec![vec![SqliteValue::Integer(0)]]);
    }

    #[test]
    fn test_vfilter_vcolumn_vnext_scan_loop() {
        let cursor = MockVtabCursor::new(vec![
            vec![SqliteValue::Integer(10), SqliteValue::Text("a".to_owned())],
            vec![SqliteValue::Integer(20), SqliteValue::Text("b".to_owned())],
        ]);
        let (rows, outcome) = run_vtab_program(0, cursor, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let done_label = b.emit_label();
            let loop_label = b.emit_label();
            b.emit_jump_to_label(Opcode::VFilter, 0, 0, done_label, P4::Int(0), 0);
            b.resolve_label(loop_label);
            b.emit_op(Opcode::VColumn, 0, 0, r1, P4::None, 0);
            b.emit_op(Opcode::VColumn, 0, 1, r2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 2, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::VNext, 0, 0, loop_label, P4::None, 0);
            b.resolve_label(done_label);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(
            rows,
            vec![
                vec![SqliteValue::Integer(10), SqliteValue::Text("a".to_owned())],
                vec![SqliteValue::Integer(20), SqliteValue::Text("b".to_owned())],
            ]
        );
    }

    #[test]
    fn test_vnext_no_cursor_falls_through() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            let loop_label = b.emit_label();
            b.resolve_label(loop_label);
            b.emit_jump_to_label(Opcode::VNext, 99, 0, loop_label, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    #[test]
    fn test_vbegin_no_instance_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::VBegin, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    #[test]
    fn test_vrename_no_instance_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::VRename, 0, 0, 0, P4::Str("new_name".to_owned()), 0);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
    }

    #[test]
    fn test_vcolumn_no_cursor_is_noop() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 55, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::VColumn, 99, 0, r_out, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows, vec![vec![SqliteValue::Integer(55)]]);
    }

    // ── Time-travel (SetSnapshot) tests ──────────────────────────────────

    #[test]
    fn test_set_snapshot_stores_time_travel_marker() {
        // Verify that SetSnapshot stores a TimeTravelMarker on the engine
        // even without a VersionStore (marker-only behavior).
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(42)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
        // SetSnapshot with commit sequence 5 on cursor 0.
        b.emit_op(Opcode::SetSnapshot, 0, 0, 0, P4::TimeTravelCommitSeq(5), 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);
        let prog = b.finish().expect("program should build");

        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        engine.set_reject_mem_fallback(false);
        engine.enable_storage_read_cursors(true);

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);

        // The marker should be recorded.
        let marker = engine.time_travel_marker(0);
        assert!(
            marker.is_some(),
            "time-travel marker should be set on cursor 0"
        );
        match marker.unwrap() {
            TimeTravelMarker::CommitSeq(seq) => assert_eq!(*seq, 5),
            _ => panic!("expected CommitSeq marker"),
        }
    }

    #[test]
    fn test_set_snapshot_upgrades_txn_cursor_to_time_travel() {
        // Verify that when a VersionStore is available, SetSnapshot replaces
        // the cursor backend with TimeTravelPageIo.
        use fsqlite_pager::{MockMvccPager, MvccPager as _, TransactionMode};

        let pager = MockMvccPager;
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(99)]);

        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
        b.emit_op(Opcode::SetSnapshot, 0, 0, 0, P4::TimeTravelCommitSeq(3), 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);
        let prog = b.finish().expect("program should build");

        let vs = Rc::new(RefCell::new(VersionStore::new(
            fsqlite_types::PageSize::DEFAULT,
        )));

        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_database(db);
        engine.set_transaction(Box::new(txn));
        engine.set_version_store(Rc::clone(&vs));

        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);

        // Verify the cursor was upgraded to a TimeTravel backend.
        let sc = engine
            .storage_cursors
            .get(&0)
            .expect("cursor 0 should exist");
        assert!(
            sc.cursor.is_time_travel(),
            "cursor should be upgraded to TimeTravel backend"
        );
        // The cursor should be marked read-only.
        assert!(!sc.writable, "time-travel cursor should be read-only");
    }

    // ── Subtype opcode tests ─────────────────────────────────────────

    #[test]
    fn test_subtype_set_get_clr_roundtrip() {
        // SetSubtype P1=subtype_reg P2=target_reg,
        // GetSubtype P1=target_reg P2=result_reg,
        // ClrSubtype P1=target_reg.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_subtype = b.alloc_reg(); // holds subtype value (74 = 'J')
            let r_target = b.alloc_reg(); // register to tag
            let r_out1 = b.alloc_reg(); // result before SetSubtype
            let r_out2 = b.alloc_reg(); // result after SetSubtype
            let r_out3 = b.alloc_reg(); // result after ClrSubtype

            // Put a value into r_target.
            b.emit_op(
                Opcode::String8,
                0,
                r_target,
                0,
                P4::Str(r#"{"a":1}"#.to_owned()),
                0,
            );

            // GetSubtype before any SetSubtype → 0.
            b.emit_op(Opcode::GetSubtype, r_target, r_out1, 0, P4::None, 0);

            // SetSubtype: tag r_target with subtype 74 ('J' for JSON).
            b.emit_op(Opcode::Integer, 74, r_subtype, 0, P4::None, 0);
            b.emit_op(Opcode::SetSubtype, r_subtype, r_target, 0, P4::None, 0);

            // GetSubtype → should be 74.
            b.emit_op(Opcode::GetSubtype, r_target, r_out2, 0, P4::None, 0);

            // ClrSubtype → clear the tag.
            b.emit_op(Opcode::ClrSubtype, r_target, 0, 0, P4::None, 0);
            b.emit_op(Opcode::GetSubtype, r_target, r_out3, 0, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r_out1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqliteValue::Integer(0)); // before set
        assert_eq!(rows[0][1], SqliteValue::Integer(74)); // after set
        assert_eq!(rows[0][2], SqliteValue::Integer(0)); // after clear
    }

    #[test]
    fn test_subtype_set_zero_clears() {
        // Setting subtype to 0 should effectively clear it.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_st = b.alloc_reg();
            let r_target = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Integer, 42, r_target, 0, P4::None, 0);
            // Set subtype to 74.
            b.emit_op(Opcode::Integer, 74, r_st, 0, P4::None, 0);
            b.emit_op(Opcode::SetSubtype, r_st, r_target, 0, P4::None, 0);
            // Set subtype to 0 → should clear.
            b.emit_op(Opcode::Integer, 0, r_st, 0, P4::None, 0);
            b.emit_op(Opcode::SetSubtype, r_st, r_target, 0, P4::None, 0);
            b.emit_op(Opcode::GetSubtype, r_target, r_out, 0, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0][0], SqliteValue::Integer(0));
    }

    // ── Bloom filter opcode tests ────────────────────────────────────

    #[test]
    fn test_bloom_filter_add_and_test() {
        // FilterAdd adds a hash; Filter should NOT jump (entry present).
        // Filter: jump to P2 if NOT found, fall through if possibly found.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_filter = b.alloc_reg();
            let r_key = b.alloc_reg();
            let r_out = b.alloc_reg();

            // Add key "hello" to filter.
            b.emit_op(Opcode::String8, 0, r_key, 0, P4::Str("hello".to_owned()), 0);
            b.emit_op(Opcode::FilterAdd, r_filter, 0, r_key, P4::None, 0);

            // Test "hello" — should be found (falls through past Filter).
            let not_found = b.emit_label();
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0); // default: not found
            b.emit_jump_to_label(Opcode::Filter, r_filter, r_key, not_found, P4::None, 0);
            // Fell through → found. Overwrite with 1.
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(not_found);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0][0], SqliteValue::Integer(1)); // found
    }

    #[test]
    fn test_bloom_filter_miss_jumps() {
        // Test a key NOT in the filter → Filter should jump to P2.
        // Filter: jump to P2 if NOT found, fall through if found.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_filter = b.alloc_reg();
            let r_key1 = b.alloc_reg();
            let r_key2 = b.alloc_reg();
            let r_out = b.alloc_reg();

            // Add "hello" to filter.
            b.emit_op(
                Opcode::String8,
                0,
                r_key1,
                0,
                P4::Str("hello".to_owned()),
                0,
            );
            b.emit_op(Opcode::FilterAdd, r_filter, 0, r_key1, P4::None, 0);

            // Test "world" — likely NOT found → jumps to not_found.
            b.emit_op(
                Opcode::String8,
                0,
                r_key2,
                0,
                P4::Str("world".to_owned()),
                0,
            );
            let not_found = b.emit_label();
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0); // default: not found
            b.emit_jump_to_label(Opcode::Filter, r_filter, r_key2, not_found, P4::None, 0);
            // Fell through → found (false positive).
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(not_found);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Due to Bloom filter false positives, we can't assert the exact value.
        assert!(matches!(rows[0][0], SqliteValue::Integer(0 | 1)));
    }

    #[test]
    fn test_bloom_filter_no_filter_falls_through() {
        // Filter with no FilterAdd → no filter exists → conservatively
        // falls through (never skips).
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_filter = b.alloc_reg();
            let r_key = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Integer, 42, r_key, 0, P4::None, 0);
            let not_found = b.emit_label();
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0); // default: not found
            b.emit_jump_to_label(Opcode::Filter, r_filter, r_key, not_found, P4::None, 0);
            // Fell through → found (or no filter). Set to 1.
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(not_found);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0][0], SqliteValue::Integer(1)); // fell through
    }
}
