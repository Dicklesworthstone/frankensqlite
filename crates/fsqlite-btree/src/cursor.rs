//! B-tree cursor implementation (§11, bd-2kvo).
//!
//! A cursor maintains a position within a single B-tree (either table or
//! index) and supports seek, navigation, and payload access. The cursor
//! uses a page stack of depth up to [`BTREE_MAX_DEPTH`] (20) to track
//! its path from root to leaf.
//!
//! # Architecture
//!
//! ```text
//!               ┌─────────┐
//!   stack[0]    │ Root     │  cell_idx = 1
//!               └────┬────┘
//!               ┌────▼────┐
//!   stack[1]    │ Interior│  cell_idx = 0
//!               └────┬────┘
//!               ┌────▼────┐
//!   stack[2]    │ Leaf    │  cell_idx = 3  ← current position
//!               └─────────┘
//! ```

use crate::balance;
use crate::cell::{self, BtreePageHeader, BtreePageType, CellRef};
use crate::instrumentation::{self, BtreeOpRuntimeStats, BtreeOpType};
use crate::overflow;
use crate::traits::{BtreeCursorOps, SeekResult, sealed};
use fsqlite_error::{FrankenError, Result};
use fsqlite_func::collation::CollationRegistry;
use fsqlite_pager::TransactionHandle;
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::BTREE_MAX_DEPTH;
use fsqlite_types::record::{
    RecordProfileScope, enter_record_profile_scope, parse_record, parse_record_prefix,
    parse_record_projected_column_offsets,
};
use fsqlite_types::serial_type::{
    SerialTypeClass, classify_serial_type, read_varint, serial_type_len, write_varint,
};
use fsqlite_types::{PageData, PageNumber, SqliteValue, WitnessKey};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cell::RefCell;
#[cfg(target_arch = "x86_64")]
use std::intrinsics::prefetch_read_data;
use std::sync::{Arc, Mutex, OnceLock};

/// OPT-1: process-wide shared default `CollationRegistry`.
///
/// `CollationRegistry::new()` pre-populates BINARY / NOCASE / RTRIM and
/// allocates a `HashMap<String, Arc<dyn CollationFunction>>`. Doing that
/// inside `BtCursor::new` — fired for every INSERT/UPDATE/DELETE — showed
/// up in the INSERT flamegraph at ~1.3% self-time (0.9% `HashMap::insert`
/// and 0.39% `Arc::drop_slow`). Callers that need custom collations still
/// override via `set_index_collation_context`; the overwhelming majority
/// of cursor constructions (table cursors, all the direct-simple DML
/// paths) never mutate the registry at all, so they can safely share this
/// immutable default Arc.
fn default_collation_registry() -> &'static Arc<Mutex<CollationRegistry>> {
    static DEFAULT: OnceLock<Arc<Mutex<CollationRegistry>>> = OnceLock::new();
    DEFAULT.get_or_init(|| Arc::new(Mutex::new(CollationRegistry::new())))
}
use tracing::{Level, debug, trace, warn};

/// Environment variable controlling the default per-cursor read-witness
/// retention cap. Parsed once at first cursor construction. `0` (the
/// default) means "unbounded" — preserves historical behavior; any other
/// `usize` is a hard upper bound on `BtCursor::witness_keys().len()`.
///
/// Workloads that never inspect the per-cursor witness vec (analytical
/// COUNT(*), full-text-index rebuild, downstream consumers like cass that
/// only need SSI evidence at the pager level) can set this to bound
/// per-cursor RSS during long B-tree descents.
const READ_WITNESS_CAP_ENV: &str = "FSQLITE_READ_WITNESS_CAP";

/// Cached env-var-driven default cap. Read once; ignored thereafter so a
/// child process can't change cap policy mid-run.
fn default_read_witness_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var(READ_WITNESS_CAP_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(0)
    })
}

/// OPT-3: throttled cancellation check for cursor ops.
///
/// The full `Cx::checkpoint()` path walks e-process oracle consultation +
/// asupersync native-cancellation-shim checkpoint (which calls
/// `clock_gettime` on every invocation). On the INSERT profile those two
/// paths together consumed ~4.8% self-time. The cursor hot path fires
/// `observe_cursor_cancellation` once per traversal step — thousands of
/// times per INSERT on deep B-trees — so the full check dominates.
///
/// We throttle the expensive check to once per `THROTTLE_INTERVAL`
/// invocations; between those we only pay the cheap
/// `cancel_requested: AtomicBool` acquire-load. If a cancellation is
/// actually requested the atomic load sees it immediately (so latency
/// is bounded by the time between the requester's release-store and the
/// next cursor operation, not by the throttle interval).
///
/// The throttle counter is thread-local so multiple concurrent cursors
/// on the same thread share it (they're all doing the "same" work
/// anyway — the interval bound is per-thread of progress, not per
/// cursor).
#[inline]
fn observe_cursor_cancellation(cx: &Cx) -> Result<()> {
    // Cheap path every call: atomic bool load. Must be Acquire so we
    // synchronize with the requester's Release store of the cancel flag.
    if cx.is_cancel_requested() {
        // Cancellation observed on the cheap fast path — drop through
        // to full checkpoint so the CancelRequested→Cancelling state
        // machine transition actually fires.
        return cx.checkpoint().map_err(|_| FrankenError::Abort);
    }

    // Throttled expensive path: e-process oracle + native-cx
    // checkpoint. `COUNTER` is per-thread so we don't need a
    // multi-threaded counter; each cursor op runs on one thread.
    const THROTTLE_INTERVAL: u32 = 64;
    thread_local! {
        static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }
    let do_full = COUNTER.with(|c| {
        let next = c.get().wrapping_add(1);
        c.set(next);
        next % THROTTLE_INTERVAL == 0
    });
    if do_full {
        return cx.checkpoint().map_err(|_| FrankenError::Abort);
    }
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn prefetch_l1_read(ptr: *const u8) {
    if ptr.is_null() {
        return;
    }

    // Locality=3 is the strongest temporal-locality hint, which matches the
    // `_MM_HINT_T0` intent of pulling the line toward the L1 data cache.
    prefetch_read_data::<u8, 3>(ptr);
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn prefetch_l1_read(_ptr: *const u8) {}

const TABLE_LEAF_INTERPOLATION_MAX_PROBES: usize = 3;

/// Size-dispatched sort for the `(ptr, size, i)` triples built during DELETE
/// cell-reshuffle in [`BtCursor::remove_cell_from_leaf`], sorted so the largest
/// `ptr` (first tuple element) comes first (descending primary key).
///
/// Profile context (OPT-7): `core::slice::sort::unstable::quicksort` +
/// `small_sort_general` were at ~2.2% self-time inside `remove_cell_from_leaf`.
/// Post OPT-A2/A3/C1 flamegraph shows the quicksort+smallsort combination is
/// still ~3.5% on the UPDATE/DELETE workload — defragmentation of full 4 KiB
/// pages hits N = 40..80 and spills into `sort_unstable_by_key`.
///
/// Microbenchmark on this x86_64 host (release mode, sort-only per-iteration,
/// data shaped like real cell offsets; see `bench_remove_cell_from_leaf_sort`):
/// at N <= 16 the specialized insertion sort runs roughly equal to or slightly
/// faster than std's `sort_unstable_by_key`. Crossover into std's pdqsort
/// small-sort path happens around N=16-20; beyond that the generic sort
/// historically won, but its closure + Reverse-wrapper path incurs overhead
/// per comparison that dwarfs the single integer subtract used here.
///
/// Dispatch strategy:
/// - N <= 20: inline insertion sort with already-sorted early-continue,
///   covering the majority of DELETE samples the flamegraph highlights.
/// - N >  20: first check whether the page is already laid out in monotone
///   cell-offset order. Descending can stream directly; ascending only needs
///   `reverse()`. Both are common after repeated compact-defrag cycles, and
///   both avoid the generic pdqsort closure/comparison path.
/// - Otherwise: `sort_unstable_by` with a direct `b.0.cmp(&a.0)` reverse
///   comparison. Avoids the `Reverse<usize>` newtype allocation that
///   `sort_unstable_by_key(|k| Reverse(k.0))` materializes per comparison.
#[inline(always)]
fn sort_cells_desc_by_ptr(v: &mut [(usize, usize, usize)]) {
    const INSERTION_SORT_THRESHOLD: usize = 20;
    if v.len() <= INSERTION_SORT_THRESHOLD {
        for i in 1..v.len() {
            // Fast path: already in descending order vs predecessor.
            // Saves the single-element copy that the shift loop would otherwise do.
            if v[i - 1].0 >= v[i].0 {
                continue;
            }
            let cur = v[i];
            let mut j = i;
            while j > 0 && v[j - 1].0 < cur.0 {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = cur;
        }
    } else {
        let mut seen_ascending_pair = false;
        let mut seen_descending_pair = false;
        for pair in v.windows(2) {
            if pair[0].0 < pair[1].0 {
                seen_ascending_pair = true;
            } else if pair[0].0 > pair[1].0 {
                seen_descending_pair = true;
            }

            if seen_ascending_pair && seen_descending_pair {
                break;
            }
        }
        if !seen_ascending_pair {
            return;
        }
        if !seen_descending_pair {
            v.reverse();
            return;
        }

        // sort_unstable_by avoids the Reverse<usize> wrapper allocation that
        // sort_unstable_by_key materialises on every comparison.
        #[allow(clippy::unnecessary_sort_by)]
        {
            v.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        }
    }
}

#[inline]
fn cell_ptrs_are_descending(ptrs: &[u16]) -> bool {
    ptrs.windows(2).all(|pair| pair[0] > pair[1])
}

// ---------------------------------------------------------------------------
// Page reader trait (for testability)
// ---------------------------------------------------------------------------

/// Trait for reading raw page data by page number.
///
/// This allows the cursor to be tested with an in-memory page store.
/// The real implementation wraps a `TransactionHandle`.
pub trait PageReader {
    /// Read a page by number, returning the raw bytes.
    fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>>;

    /// Read a page by number, returning owned page data.
    ///
    /// Implementations can override this to forward shared page buffers
    /// without forcing an intermediate `Vec<u8>` clone.
    fn read_page_data(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        Ok(PageData::from_vec(self.read_page(cx, page_no)?))
    }

    /// Read a page for B-tree cursor traversal.
    ///
    /// Transaction-backed implementations may override this to avoid recording
    /// a coarse SIREAD page witness while the cursor emits finer cell/range
    /// witnesses for the logical B-tree operation.
    fn read_btree_page_data(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        self.read_page_data(cx, page_no)
    }

    /// Hint that a page is likely to be needed soon.
    ///
    /// Default implementation is a no-op so platforms without a safe prefetch
    /// primitive degrade gracefully.
    fn prefetch_page_hint(&self, _cx: &Cx, _page_no: PageNumber) {}

    /// Record a granular read witness for fine-grained SSI validation.
    fn record_read_witness(&self, _cx: &Cx, _key: WitnessKey) {}

    /// Returns `true` if the page has been modified in the current transaction.
    fn is_dirty(&self, _page_no: PageNumber) -> bool {
        false
    }
}

/// Trait for writing pages (needed for insert/delete).
pub trait PageWriter: PageReader {
    /// Write raw data to a page.
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()>;

    /// Write owned page data to a page.
    ///
    /// Implementations can override this to adopt owned page buffers without
    /// routing through a borrowed slice first.
    fn write_page_data(&mut self, cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
        self.write_page(cx, page_no, data.as_bytes())
    }

    /// Temporarily take ownership of an unpublished staged page image.
    ///
    /// Implementations that sit directly on top of a transaction write-set can
    /// override this to let the cursor mutate the authoritative staged page in
    /// place instead of cloning a second page image for retained append hints.
    fn try_take_staged_page_data(&mut self, _page_no: PageNumber) -> Option<PageData> {
        None
    }

    /// Mutate an unpublished staged page already owned by the transaction.
    fn try_mutate_staged_page_data(
        &mut self,
        _page_no: PageNumber,
        _f: &mut dyn FnMut(&mut PageData),
    ) -> bool {
        false
    }

    /// Restore a page image previously taken with `try_take_staged_page_data`.
    fn restore_staged_page_data(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        data: PageData,
    ) -> Result<()> {
        self.write_page_data(cx, page_no, data)
    }

    /// Allocate a new page.
    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber>;
    /// Free a page.
    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()>;
    /// Record a granular write witness for fine-grained SSI.
    fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey);
}

// ---------------------------------------------------------------------------
// TransactionHandle adapter (pager -> btree)
// ---------------------------------------------------------------------------

/// Adapter implementing [`PageReader`] and [`PageWriter`] by forwarding to a
/// [`TransactionHandle`].
///
/// This is the glue layer that lets `BtCursor` operate directly on top of the
/// MVCC pager transaction surface without any intermediate page store.
#[derive(Debug)]
pub struct TransactionPageIo<'a, T: TransactionHandle + ?Sized> {
    txn: &'a mut T,
}

/// Cold corruption-error builder for `BtCursor::read_cell_pointer_inline`.
///
/// Outlined as a free `#[cold]` function so a single non-generic copy is
/// shared across every `BtCursor<P>` monomorphization, and so the heavy
/// `format!` machinery cannot block inlining of the hot 2-byte cell-pointer
/// read in the cursor descent loop.
#[cold]
#[inline(never)]
fn cell_pointer_oob(
    cell_idx: u16,
    page_no: PageNumber,
    ptr_offset: usize,
    page_len: usize,
) -> FrankenError {
    FrankenError::DatabaseCorrupt {
        detail: format!(
            "cell pointer {cell_idx} extends past page {} (ptr_offset={ptr_offset} len={page_len})",
            page_no.get(),
        ),
    }
}

impl<'a, T: TransactionHandle + ?Sized> TransactionPageIo<'a, T> {
    /// Wrap a pager transaction handle for use as a B-tree page I/O backend.
    #[must_use]
    pub fn new(txn: &'a mut T) -> Self {
        Self { txn }
    }

    /// Access the underlying transaction handle immutably.
    pub fn txn(&self) -> &T {
        &*self.txn
    }

    /// Access the underlying transaction handle mutably.
    pub fn txn_mut(&mut self) -> &mut T {
        self.txn
    }
}

impl<T: TransactionHandle + ?Sized> PageReader for TransactionPageIo<'_, T> {
    fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        Ok(self.txn.get_page(cx, page_no)?.into_vec())
    }

    fn read_page_data(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        self.txn.get_page(cx, page_no)
    }

    fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
        self.txn.prefetch_page_hint(cx, page_no);
    }
}

impl<T: TransactionHandle + ?Sized> PageWriter for TransactionPageIo<'_, T> {
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.txn.write_page(cx, page_no, data)
    }

    fn write_page_data(&mut self, cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
        self.txn.write_page_data(cx, page_no, data)
    }

    fn try_take_staged_page_data(&mut self, page_no: PageNumber) -> Option<PageData> {
        <T as TransactionHandle>::try_take_staged_page_data(self.txn, page_no)
    }

    fn try_mutate_staged_page_data(
        &mut self,
        page_no: PageNumber,
        f: &mut dyn FnMut(&mut PageData),
    ) -> bool {
        <T as TransactionHandle>::try_mutate_staged_page_data(self.txn, page_no, f)
    }

    fn restore_staged_page_data(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        data: PageData,
    ) -> Result<()> {
        <T as TransactionHandle>::restore_staged_page_data(self.txn, cx, page_no, data)
    }

    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
        self.txn.allocate_page(cx)
    }

    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.txn.free_page(cx, page_no)
    }

    fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey) {
        self.txn.record_write_witness(cx, key);
    }
}

// ---------------------------------------------------------------------------
// In-memory page store (for VDBE storage cursors)
// ---------------------------------------------------------------------------

/// Simple in-memory page store implementing [`PageReader`] and [`PageWriter`].
///
/// Used by the VDBE storage cursor path to build transient B-trees from
/// in-memory table data without requiring the full pager/VFS stack.
#[derive(Debug, Clone)]
pub struct MemPageStore {
    pages: std::collections::HashMap<u32, Vec<u8>, foldhash::fast::FixedState>,
    page_size: u32,
    page_slots: Vec<u8>,
}

impl MemPageStore {
    /// Create a new empty page store with the given page size.
    #[must_use]
    pub fn new(page_size: u32) -> Self {
        Self {
            pages: std::collections::HashMap::with_hasher(foldhash::fast::FixedState::default()),
            page_size,
            page_slots: Vec::new(),
        }
    }

    #[inline]
    fn page_slot_index(page_no: PageNumber) -> Option<usize> {
        page_no
            .get()
            .checked_sub(1)
            .and_then(|slot| usize::try_from(slot).ok())
    }

    fn set_page_slot_present(&mut self, page_no: PageNumber, present: bool) {
        let Some(slot_idx) = Self::page_slot_index(page_no) else {
            return;
        };
        if slot_idx >= self.page_slots.len() {
            self.page_slots.resize(slot_idx + 1, 0);
        }
        self.page_slots[slot_idx] = u8::from(present);
    }

    /// Initialize an empty leaf-table root page at the given page number.
    ///
    /// Call this once before constructing a [`BtCursor`] that will insert
    /// rows into the store. Avoid page 1 for transient stores since the
    /// B-tree code applies a 100-byte header offset to page 1.
    #[allow(clippy::cast_possible_truncation)]
    pub fn init_leaf_table_root(&mut self, pgno: PageNumber) {
        let mut page = vec![0u8; self.page_size as usize];
        page[0] = 0x0D;
        page[3..5].copy_from_slice(&0u16.to_be_bytes());
        let content_off = self.page_size as u16;
        page[5..7].copy_from_slice(&content_off.to_be_bytes());
        self.pages.insert(pgno.get(), page);
        self.set_page_slot_present(pgno, true);
    }

    /// Create a page store pre-initialized with an empty leaf table B-tree
    /// at the given root page number.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn with_empty_table(root_page: PageNumber, page_size: u32) -> Self {
        let mut store = Self::new(page_size);
        let mut page = vec![0u8; page_size as usize];
        // Initialize as empty leaf table page (type 0x0D).
        page[0] = 0x0D;
        // Bytes 1-2: first freeblock offset = 0 (none).
        // Bytes 3-4: cell count = 0.
        // Bytes 5-6: content area offset = page_size (no cells yet).
        let content_offset = page_size as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        // Byte 7: fragmented free bytes = 0.
        store.pages.insert(root_page.get(), page);
        store.set_page_slot_present(root_page, true);
        store
    }

    pub fn with_empty_index(root_page: PageNumber, page_size: u32) -> Self {
        let mut store = Self::new(page_size);
        let mut page = vec![0u8; page_size as usize];
        // Initialize as empty leaf index page (type 0x0A).
        page[0] = 0x0A;
        // Bytes 1-2: first freeblock offset = 0 (none).
        // Bytes 3-4: cell count = 0.
        // Bytes 5-6: content area offset = page_size (no cells yet).
        let content_offset = page_size as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        // Byte 7: fragmented free bytes = 0.
        store.pages.insert(root_page.get(), page);
        store.set_page_slot_present(root_page, true);
        store
    }
}

impl PageReader for MemPageStore {
    fn read_page(&self, _cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        self.pages
            .get(&page_no.get())
            .cloned()
            .ok_or_else(|| FrankenError::internal("page not found"))
    }

    fn prefetch_page_hint(&self, _cx: &Cx, page_no: PageNumber) {
        let Some(slot_idx) = Self::page_slot_index(page_no) else {
            return;
        };

        if let Some(slot_present) = self.page_slots.get(slot_idx) {
            prefetch_l1_read(std::ptr::from_ref(slot_present).cast::<u8>());
        }

        let Some(page) = self.pages.get(&page_no.get()) else {
            return;
        };
        prefetch_l1_read(page.as_ptr());
    }
}

impl PageWriter for MemPageStore {
    fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        let page_size = self.page_size as usize;
        if data.len() > page_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "test page store refused oversized page write: {} > {}",
                    data.len(),
                    page_size
                ),
            });
        }
        let mut page = vec![0_u8; page_size];
        let copy_len = data.len().min(page_size);
        page[..copy_len].copy_from_slice(&data[..copy_len]);
        self.pages.insert(page_no.get(), page);
        self.set_page_slot_present(page_no, true);
        Ok(())
    }

    fn write_page_data(&mut self, _cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
        let page_size = self.page_size as usize;
        if data.len() > page_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "test page store refused oversized page write: {} > {}",
                    data.len(),
                    page_size
                ),
            });
        }
        let mut page = vec![0_u8; page_size];
        let copy_len = data.len().min(page_size);
        page[..copy_len].copy_from_slice(&data.as_bytes()[..copy_len]);
        self.pages.insert(page_no.get(), page);
        self.set_page_slot_present(page_no, true);
        Ok(())
    }

    fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
        let next = self
            .pages
            .keys()
            .copied()
            .max()
            .unwrap_or(1)
            .checked_add(1)
            .ok_or(FrankenError::DatabaseFull)?;
        let pgno = PageNumber::new(next).ok_or(FrankenError::DatabaseFull)?;
        self.pages.insert(next, vec![0u8; self.page_size as usize]);
        self.set_page_slot_present(pgno, true);
        Ok(pgno)
    }

    fn free_page(&mut self, _cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.pages.remove(&page_no.get());
        self.set_page_slot_present(page_no, false);
        Ok(())
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
}

// ---------------------------------------------------------------------------
// Cell-pointer Vec pool
// ---------------------------------------------------------------------------
//
// Every fresh `load_page` allocated a `Vec<u16>` to hold the cell pointer
// offsets for the page, and every pop/drop of that `StackEntry` freed it.
// On the MT 8t mt-mvcc-bench profile `read_cell_pointers_into` was 1.55 %
// self-time and the allocator helpers (`_int_malloc` 2.18 %, `finish_grow`
// 0.62 %, `cfree` 0.71 %) compounded across hot operations that load many
// pages per step (`table_seek_for_insert` 0.94 %, `move_to_rightmost_leaf`
// 0.69 %). Pooling the `Vec<u16>` buffers across calls keeps the same
// allocations alive for the life of the thread and lets `read_cell_pointers_into`
// reuse an existing capacity-backed buffer on every load after the first few.
//
// The pool is thread-local because cursors are single-thread by construction
// (each `BtCursor` is driven by one caller at a time). A thread-local pool
// also amortises across concurrent cursors on the same thread — they share
// the same warmed-up free list.

thread_local! {
    static CELL_POINTERS_POOL: RefCell<Vec<Vec<u16>>> = const { RefCell::new(Vec::new()) };
}

/// Maximum number of pooled `Vec<u16>` entries kept per thread.
///
/// Cursor stacks are bounded by [`BTREE_MAX_DEPTH`] (20) and individual
/// cursor operations rarely hold more than a few live stack entries at
/// once, so this cap keeps the free list small enough to fit in the
/// thread's working set while still covering the typical working set
/// of concurrent cursors on the same thread.
const CELL_POINTERS_POOL_MAX_ENTRIES: usize = 32;

/// Don't pool buffers whose capacity grew past this many u16 slots.
///
/// A 64 KiB page with 4-byte cells fits ~16 Ki cells, so capping at 8 Ki
/// keeps normal-workload buffers pooled while releasing pathological ones
/// back to the allocator.
const CELL_POINTERS_POOL_MAX_CAPACITY: usize = 8192;

fn take_pooled_cell_pointers() -> Vec<u16> {
    CELL_POINTERS_POOL
        .with(|pool| pool.borrow_mut().pop())
        .unwrap_or_default()
}

fn recycle_cell_pointers(mut buf: Vec<u16>) {
    if buf.capacity() == 0 || buf.capacity() > CELL_POINTERS_POOL_MAX_CAPACITY {
        return;
    }
    CELL_POINTERS_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < CELL_POINTERS_POOL_MAX_ENTRIES {
            buf.clear();
            pool.push(buf);
        }
    });
}

// ---------------------------------------------------------------------------
// Cursor stack entry
// ---------------------------------------------------------------------------

/// A single entry in the cursor's page stack.
#[derive(Debug)]
struct StackEntry {
    /// Page number of this page (retained for debugging and future use in
    /// mutation operations).
    #[allow(dead_code)]
    page_no: PageNumber,
    /// Cached raw page data.
    page_data: PageData,
    /// Parsed page header.
    header: BtreePageHeader,
    /// Cell pointer offsets (cached from the cell pointer array).
    /// bd-perf (V1.1): Vec instead of Box<[u16]> — eliminates the
    /// Box::from(Vec) reallocation+copy on every page load.
    /// The Vec allocation is pooled thread-locally (see `CELL_POINTERS_POOL`),
    /// so drops return the buffer for reuse and clones pull a fresh buffer
    /// from the pool before copying.
    cell_pointers: Vec<u16>,
    /// Page-image mutation signature for validating cached cell-slot parses.
    mutation_counter: u64,
    /// Current cell index. For interior pages, this indicates which child
    /// was descended into. For leaf pages, this is the current position.
    /// A value equal to `cell_count` means "past the right-most child" on
    /// interior pages, or "past the last cell" on leaf pages.
    cell_idx: u16,
}

impl Clone for StackEntry {
    fn clone(&self) -> Self {
        let mut cell_pointers = take_pooled_cell_pointers();
        cell_pointers.extend_from_slice(&self.cell_pointers);
        Self {
            page_no: self.page_no,
            page_data: self.page_data.clone(),
            header: self.header,
            cell_pointers,
            mutation_counter: self.mutation_counter,
            cell_idx: self.cell_idx,
        }
    }
}

impl Drop for StackEntry {
    fn drop(&mut self) {
        recycle_cell_pointers(std::mem::take(&mut self.cell_pointers));
    }
}

// ---------------------------------------------------------------------------
// BtCursor
// ---------------------------------------------------------------------------

const CURSOR_STACK_INLINE_DEPTH: usize = 4;
const TABLE_SEEK_CACHE_SLOTS: usize = 4;
const CELL_SLOT_CACHE_ENTRIES: usize = 64;

type CursorStack = SmallVec<[StackEntry; CURSOR_STACK_INLINE_DEPTH]>;

// bd-9e3xf.2: inline slot capacity widened 8 → 16. The bd-9e3xf audit
// showed MISS_SLOT at 71 % of all misses — 54 739 / 76 865 across the
// workload suite — and nearly all of those come from index-btree binary
// search on a just-invalidated leaf. A search on an ~N-cell leaf adds
// log2(N)+1 slots to the fresh entry as each probe misses; with the old
// inline capacity of 8, any leaf with more than ~256 cells spills to the
// heap on the 9th probe, paying a `malloc` + copy per search. Widening to
// 16 covers leaves up to ~65 k cells inline while only adding 8 slots ×
// 56 bytes ≈ 450 bytes per entry (capped at 64 entries per cursor).
type CachedCellSlots = SmallVec<[(u16, CachedCellSlot); 16]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedCellSlot {
    left_child: Option<PageNumber>,
    rowid: Option<i64>,
    payload_size: u32,
    local_size: u32,
    payload_offset: usize,
    overflow_page: Option<PageNumber>,
}

impl CachedCellSlot {
    fn from_cell_ref(cell: &CellRef) -> Self {
        Self {
            left_child: cell.left_child,
            rowid: cell.rowid,
            payload_size: cell.payload_size,
            local_size: cell.local_size,
            payload_offset: cell.payload_offset,
            overflow_page: cell.overflow_page,
        }
    }

    fn into_cell_ref(self) -> CellRef {
        CellRef {
            left_child: self.left_child,
            rowid: self.rowid,
            payload_size: self.payload_size,
            local_size: self.local_size,
            payload_offset: self.payload_offset,
            overflow_page: self.overflow_page,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CellSlotCacheEntry {
    page_no: PageNumber,
    mutation_counter: u64,
    slots: CachedCellSlots,
}

#[derive(Debug, Default)]
struct CellSlotCache {
    entries: Vec<CellSlotCacheEntry>,
}

/// bd-yafor.2 + bd-o92pn.2: cell-slot cache hit/miss accounting, classified.
///
/// Incremented by `CellSlotCache::get` under `Relaxed` ordering so the cost is
/// a single uncontended `lock xadd` per lookup — small enough to leave live in
/// release builds while giving us ground-truth hit-rate data from any
/// workload. Misses are split into three kinds so an audit can tell whether a
/// high overall miss-rate comes from cold starts (no entry for this page in
/// the cache yet), writer invalidation (entry exists for this `page_no` but a
/// previous write bumped the `mutation_counter` so the key no longer
/// matches), or in-entry slot misses (the entry matched but this specific
/// `cell_idx` has not been parsed yet). The counters are inert unless read
/// by `cell_slot_cache_counter_snapshot`.
static CELL_SLOT_CACHE_HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static CELL_SLOT_CACHE_MISS_COLD: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static CELL_SLOT_CACHE_MISS_INVALIDATED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static CELL_SLOT_CACHE_MISS_SLOT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Snapshot of cell-slot cache counters for audit/tests.
#[derive(Debug, Clone, Copy)]
pub struct CellSlotCacheCounters {
    pub hits: u64,
    /// No existing entry in the cache covers this `page_no` (cold start or
    /// LRU eviction).
    pub miss_cold: u64,
    /// Entry exists for this `page_no` but the recorded `mutation_counter`
    /// differs from the current one — i.e. a writer changed the page image
    /// since the entry was recorded.
    pub miss_invalidated: u64,
    /// The `(page_no, mutation_counter)` entry matched but this specific
    /// `cell_idx` had not been parsed into the slot list yet.
    pub miss_slot: u64,
}

impl CellSlotCacheCounters {
    #[must_use]
    pub fn misses(self) -> u64 {
        self.miss_cold
            .saturating_add(self.miss_invalidated)
            .saturating_add(self.miss_slot)
    }

    #[must_use]
    pub fn total(self) -> u64 {
        self.hits.saturating_add(self.misses())
    }

    #[must_use]
    pub fn hit_rate(self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            {
                self.hits as f64 / total as f64
            }
        }
    }
}

pub fn cell_slot_cache_counter_snapshot() -> CellSlotCacheCounters {
    use std::sync::atomic::Ordering::Relaxed;
    CellSlotCacheCounters {
        hits: CELL_SLOT_CACHE_HITS.load(Relaxed),
        miss_cold: CELL_SLOT_CACHE_MISS_COLD.load(Relaxed),
        miss_invalidated: CELL_SLOT_CACHE_MISS_INVALIDATED.load(Relaxed),
        miss_slot: CELL_SLOT_CACHE_MISS_SLOT.load(Relaxed),
    }
}

pub fn reset_cell_slot_cache_counters() {
    use std::sync::atomic::Ordering::Relaxed;
    CELL_SLOT_CACHE_HITS.store(0, Relaxed);
    CELL_SLOT_CACHE_MISS_COLD.store(0, Relaxed);
    CELL_SLOT_CACHE_MISS_INVALIDATED.store(0, Relaxed);
    CELL_SLOT_CACHE_MISS_SLOT.store(0, Relaxed);
}

impl CellSlotCache {
    fn clear(&mut self) {
        self.entries.clear();
    }

    fn get(
        &mut self,
        page_no: PageNumber,
        mutation_counter: u64,
        cell_idx: u16,
    ) -> Option<CachedCellSlot> {
        use std::sync::atomic::Ordering::Relaxed;
        // Binary search probes the same page repeatedly, so the common hit is
        // the already-promoted front entry. Avoid scanning the whole entry LRU
        // just to rediscover the MRU page.
        let front_page_match = if let Some(front) = self.entries.first() {
            if front.page_no == page_no && front.mutation_counter == mutation_counter {
                let slot = front
                    .slots
                    .iter()
                    .find_map(|(idx, slot)| (*idx == cell_idx).then_some(*slot));
                if crate::instrumentation::copy_profile_enabled() {
                    if slot.is_some() {
                        CELL_SLOT_CACHE_HITS.fetch_add(1, Relaxed);
                    } else {
                        CELL_SLOT_CACHE_MISS_SLOT.fetch_add(1, Relaxed);
                    }
                }
                return slot;
            }
            front.page_no == page_no
        } else {
            false
        };
        self.get_slow(page_no, mutation_counter, cell_idx, front_page_match)
    }

    #[cold]
    fn get_slow(
        &mut self,
        page_no: PageNumber,
        mutation_counter: u64,
        cell_idx: u16,
        front_page_match: bool,
    ) -> Option<CachedCellSlot> {
        use std::sync::atomic::Ordering::Relaxed;
        // Scan the rest of the LRU. `front_page_match` carries over the
        // observation from `get`: if the front entry already matched the
        // page_no (but had a stale mutation_counter) we know at least one
        // writer-invalidated image exists, even if this deeper scan finds
        // nothing.
        let mut saw_page = front_page_match;
        let mut matching_idx = None;
        for (idx, entry) in self.entries.iter().enumerate().skip(1) {
            if entry.page_no == page_no {
                saw_page = true;
                if entry.mutation_counter == mutation_counter {
                    matching_idx = Some(idx);
                    break;
                }
            }
        }
        let Some(entry_idx) = matching_idx else {
            if crate::instrumentation::copy_profile_enabled() {
                if saw_page {
                    CELL_SLOT_CACHE_MISS_INVALIDATED.fetch_add(1, Relaxed);
                } else {
                    CELL_SLOT_CACHE_MISS_COLD.fetch_add(1, Relaxed);
                }
            }
            return None;
        };
        let slot = self.entries[entry_idx]
            .slots
            .iter()
            .find_map(|(idx, slot)| (*idx == cell_idx).then_some(*slot));
        match slot {
            Some(found) => {
                if crate::instrumentation::copy_profile_enabled() {
                    CELL_SLOT_CACHE_HITS.fetch_add(1, Relaxed);
                }
                if entry_idx != 0 {
                    let entry = self.entries.remove(entry_idx);
                    self.entries.insert(0, entry);
                }
                Some(found)
            }
            None => {
                if crate::instrumentation::copy_profile_enabled() {
                    CELL_SLOT_CACHE_MISS_SLOT.fetch_add(1, Relaxed);
                }
                None
            }
        }
    }

    #[inline]
    fn insert(
        &mut self,
        page_no: PageNumber,
        mutation_counter: u64,
        cell_idx: u16,
        slot: CachedCellSlot,
    ) {
        // Binary-search on a single page repeatedly hits the same MRU entry —
        // previously we `remove(0)` + `insert(0, entry)` on every slot insert,
        // paying O(CELL_SLOT_CACHE_ENTRIES) memmoves just to shuffle the front
        // entry back to the front. Mutate in place when the target is already
        // MRU to keep this path allocation-free and bounded by the SmallVec
        // slot scan.
        if let Some(front) = self.entries.first_mut() {
            if front.page_no == page_no && front.mutation_counter == mutation_counter {
                if let Some((_, existing)) =
                    front.slots.iter_mut().find(|(idx, _)| *idx == cell_idx)
                {
                    *existing = slot;
                } else {
                    front.slots.push((cell_idx, slot));
                }
                return;
            }
        }
        self.insert_slow(page_no, mutation_counter, cell_idx, slot);
    }

    #[cold]
    fn insert_slow(
        &mut self,
        page_no: PageNumber,
        mutation_counter: u64,
        cell_idx: u16,
        slot: CachedCellSlot,
    ) {
        let mut entry = if let Some(existing_idx) = self.entries.iter().position(|entry| {
            entry.page_no == page_no && entry.mutation_counter == mutation_counter
        }) {
            self.entries.remove(existing_idx)
        } else {
            CellSlotCacheEntry {
                page_no,
                mutation_counter,
                slots: CachedCellSlots::new(),
            }
        };

        if let Some((_, existing)) = entry.slots.iter_mut().find(|(idx, _)| *idx == cell_idx) {
            *existing = slot;
        } else {
            entry.slots.push((cell_idx, slot));
        }

        self.entries.insert(0, entry);
        self.entries.truncate(CELL_SLOT_CACHE_ENTRIES);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TableSeekCacheEntry {
    rowid: i64,
    page_no: PageNumber,
    cell_idx: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorPositionStamp {
    page_no: u32,
    cell_idx: u16,
    row_image_epoch: u64,
}

impl CursorPositionStamp {
    #[must_use]
    pub const fn page_no(self) -> u32 {
        self.page_no
    }

    #[must_use]
    pub const fn cell_idx(self) -> u16 {
        self.cell_idx
    }

    #[must_use]
    pub const fn row_image_epoch(self) -> u64 {
        self.row_image_epoch
    }

    #[must_use]
    pub const fn same_logical_position(self, other: Self) -> bool {
        self.page_no == other.page_no && self.cell_idx == other.cell_idx
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RightmostLeafCacheEntry {
    page_no: PageNumber,
    rowid: i64,
    tree_depth: usize,
    parent_page: Option<PageNumber>,
    page_data: PageData,
    header: BtreePageHeader,
    cell_pointers: Vec<u16>,
}

impl Drop for RightmostLeafCacheEntry {
    fn drop(&mut self) {
        recycle_cell_pointers(std::mem::take(&mut self.cell_pointers));
    }
}

/// Opaque retained append hint for monotonic table inserts.
///
/// Short-lived callers can carry this between cursor instances to keep the
/// rightmost append path hot without re-reading and reparsing the hinted leaf
/// page on every row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAppendHint {
    leaf_page: PageNumber,
    last_rowid: i64,
    tree_depth: usize,
    parent_page: Option<PageNumber>,
    page_data: Option<PageData>,
    header: BtreePageHeader,
}

impl TableAppendHint {
    #[must_use]
    pub const fn leaf_page(&self) -> PageNumber {
        self.leaf_page
    }

    #[must_use]
    pub const fn last_rowid(&self) -> i64 {
        self.last_rowid
    }

    #[must_use]
    pub const fn tree_depth(&self) -> usize {
        self.tree_depth
    }

    #[must_use]
    pub const fn parent_page(&self) -> Option<PageNumber> {
        self.parent_page
    }

    #[must_use]
    pub fn without_page_data(mut self) -> Self {
        self.page_data = None;
        self
    }

    /// In-place variant of [`Self::without_page_data`] for hot-path
    /// callers that already have a `&mut TableAppendHint` and want to
    /// drop the cached page data without paying the two by-value
    /// struct moves (take out, modify, put back) that the consuming
    /// variant forces.
    ///
    /// On an INSERT hotpath this matters: `TableAppendHint` carries a
    /// `BtreePageHeader` (Copy, ~32 B) plus the 48-byte `Option<PageData>`
    /// plus the other fields, so each by-value round-trip is ~100 B of
    /// memcpy per row. At 650 k wps on a 1-thread workload that is
    /// ~150 MB/s of pointless motion; attributable to the
    /// `store_prepared_direct_insert_append_hint` 3.98% self-time bar
    /// observed in the 2026-04-24 1-thread profile
    /// (`tests/artifacts/perf/1t-gap-20260424T2050Z/`).
    pub fn clear_page_data(&mut self) {
        self.page_data = None;
    }

    #[must_use]
    pub const fn retains_page_data(&self) -> bool {
        self.page_data.is_some()
    }
}

impl From<&RightmostLeafCacheEntry> for TableAppendHint {
    fn from(value: &RightmostLeafCacheEntry) -> Self {
        Self {
            leaf_page: value.page_no,
            last_rowid: value.rowid,
            tree_depth: value.tree_depth,
            parent_page: value.parent_page,
            page_data: Some(value.page_data.clone()),
            header: value.header,
        }
    }
}

/// Opaque same-leaf payload patch run for direct table UPDATEs.
///
/// This owns one decoded leaf image so a higher layer can apply several
/// same-size, no-overflow payload patches before publishing the page once.
/// It deliberately exposes only rowid-targeted fixed-width REAL mutation until
/// the broader DML leaf-run operator has correctness and benchmark proof.
#[derive(Debug, Clone)]
pub struct TableLeafPayloadPatchRun {
    entry: StackEntry,
    dirty: bool,
}

impl TableLeafPayloadPatchRun {
    #[must_use]
    pub const fn leaf_page(&self) -> PageNumber {
        self.entry.page_no
    }

    #[must_use]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn table_leaf_rowid_at(entry: &StackEntry, cell_idx: u16) -> Result<i64> {
        let idx = usize::from(cell_idx);
        let offset =
            entry
                .cell_pointers
                .get(idx)
                .copied()
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell index {} out of bounds ({})",
                        cell_idx,
                        entry.cell_pointers.len()
                    ),
                })?;
        let offset = usize::from(offset);
        let cell_data = entry.page_data.as_bytes().get(offset..).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf cell pointer {} points past page end (len {})",
                    offset,
                    entry.page_data.as_bytes().len()
                ),
            }
        })?;
        let Some((_, payload_varint_len)) = read_varint(cell_data) else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "table leaf cell has invalid payload size varint".to_owned(),
            });
        };
        let Some((rowid, _)) = read_varint(&cell_data[payload_varint_len..]) else {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "table leaf cell has invalid rowid varint".to_owned(),
            });
        };
        #[allow(clippy::cast_possible_wrap)]
        Ok(rowid as i64)
    }

    fn search_table_leaf(&self, cx: &Cx, target: i64) -> Result<Option<u16>> {
        let entry = &self.entry;
        let mut lo = 0u16;
        let mut hi = entry.header.cell_count;
        while lo < hi {
            observe_cursor_cancellation(cx)?;
            let mid = lo + (hi - lo) / 2;
            let rowid = Self::table_leaf_rowid_at(entry, mid)?;
            match rowid.cmp(&target) {
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    /// Patch a fixed-width REAL column inside a local table payload.
    ///
    /// Returns `Ok(false)` when the row is not present on this retained leaf or
    /// the cell shape is not the narrow same-size/no-overflow REAL case.
    pub fn patch_fixed_width_real(
        &mut self,
        cx: &Cx,
        rowid: i64,
        column_index: usize,
        column_count: usize,
        next_value: f64,
        usable_size: u32,
    ) -> Result<bool> {
        if next_value.is_nan() {
            return Ok(false);
        }
        let Some(cell_idx) = self.search_table_leaf(cx, rowid)? else {
            return Ok(false);
        };
        let cell_offset = usize::from(self.entry.cell_pointers[usize::from(cell_idx)]);
        let cell = CellRef::parse(
            self.entry.page_data.as_bytes(),
            cell_offset,
            self.entry.header.page_type,
            usable_size,
        )?;
        if cell.rowid != Some(rowid)
            || cell.overflow_page.is_some()
            || cell.payload_size != cell.local_size
        {
            return Ok(false);
        }
        let payload_len =
            usize::try_from(cell.local_size).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch local payload size exceeds usize".to_owned(),
            })?;
        let payload_end = cell
            .payload_offset
            .checked_add(payload_len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch payload offset overflow".to_owned(),
            })?;
        let usable_size_usize = usize::try_from(usable_size).unwrap_or(usize::MAX);
        if payload_end > self.entry.page_data.as_bytes().len() || payload_end > usable_size_usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch payload extends past usable page".to_owned(),
            });
        }
        let payload = &self.entry.page_data.as_bytes()[cell.payload_offset..payload_end];
        let projected = parse_record_projected_column_offsets(payload, column_index, None)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch failed to parse row payload".to_owned(),
            })?;
        if projected.column_count != column_count {
            return Ok(false);
        }
        let Some(column_offset) = projected.primary else {
            return Ok(false);
        };
        if column_offset.serial_type != 7 || column_offset.value_len != 8 {
            return Ok(false);
        }
        let start_in_payload = usize::try_from(column_offset.body_offset).map_err(|_| {
            FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch column offset exceeds usize".to_owned(),
            }
        })?;
        let start = cell
            .payload_offset
            .checked_add(start_in_payload)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch absolute offset overflow".to_owned(),
            })?;
        let end = start
            .checked_add(8)
            .filter(|end| *end <= payload_end)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "fixed-width REAL patch column extends past payload".to_owned(),
            })?;
        self.entry.page_data.as_bytes_mut()[start..end]
            .copy_from_slice(&next_value.to_bits().to_be_bytes());
        self.entry.mutation_counter = self.entry.page_data.image_token();
        self.dirty = true;
        Ok(true)
    }

    fn into_page(self) -> (PageNumber, PageData) {
        (self.entry.page_no, self.entry.page_data.clone())
    }
}

/// Reason a same-leaf delete run declined an additional rowid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLeafDeleteRunMissReason {
    /// The rowid is outside the retained leaf image.
    RowidNotInLeaf,
    /// The rowid was already accepted into this pending run.
    AlreadyDeleted,
    /// Accepting the delete could leave a non-root leaf empty.
    NonRootWouldEmptyLeaf,
    /// Accepting the delete could require parent separator repair.
    NonRootLastCell,
    /// The page no longer has the compact shape this fast path can rewrite.
    NonCompactCellArea,
    /// The cell did not match the expected on-page, non-overflow rowid shape.
    CellShapeOrOverflow,
}

/// Result of attempting to add a rowid to a same-leaf delete run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLeafDeleteRunDelete {
    /// The rowid was accepted into the pending run.
    Deleted,
    /// The run declined the rowid and reports why.
    Miss(TableLeafDeleteRunMissReason),
}

/// Opaque same-leaf delete run for direct table DELETEs.
///
/// The run owns one table-leaf image and accepts only deletes that cannot
/// require parent separator repair, overflow-chain cleanup, or structural
/// rebalance. Unsupported rowids return a miss reason so callers can flush and
/// fall back to the ordinary cursor delete path.
#[derive(Debug, Clone)]
pub struct TableLeafDeleteRun {
    entry: StackEntry,
    tree_depth: usize,
    dirty: bool,
    compact_cell_area: bool,
    profile_delete_leaf_run: bool,
    deleted_cell_indices: SmallVec<[u16; 16]>,
}

const COMPACT_DELETE_SINGLE_PASS_MIN: usize = 6;

impl TableLeafDeleteRun {
    #[must_use]
    pub const fn leaf_page(&self) -> PageNumber {
        self.entry.page_no
    }

    pub fn max_rowid(&self) -> Result<i64> {
        let max_cell_idx = self
            .entry
            .header
            .cell_count
            .checked_sub(1)
            .ok_or_else(|| FrankenError::internal("empty table leaf delete run"))?;
        TableLeafPayloadPatchRun::table_leaf_rowid_at(&self.entry, max_cell_idx)
    }

    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.dirty || !self.deleted_cell_indices.is_empty()
    }

    pub fn delete_rowid(&mut self, cx: &Cx, rowid: i64, usable_size: u32) -> Result<bool> {
        Ok(matches!(
            self.delete_rowid_with_reason(cx, rowid, usable_size)?,
            TableLeafDeleteRunDelete::Deleted
        ))
    }

    pub fn delete_rowid_with_reason(
        &mut self,
        cx: &Cx,
        rowid: i64,
        usable_size: u32,
    ) -> Result<TableLeafDeleteRunDelete> {
        let cell_idx = if self.profile_delete_leaf_run {
            let search_start = Some(std::time::Instant::now());
            let search_result = self.search_table_leaf(cx, rowid);
            instrumentation::record_delete_leaf_run_search(search_start);
            search_result?
        } else {
            self.search_table_leaf(cx, rowid)?
        };
        let Some(cell_idx) = cell_idx else {
            return Ok(TableLeafDeleteRunDelete::Miss(
                TableLeafDeleteRunMissReason::RowidNotInLeaf,
            ));
        };
        let already_deleted = if self.profile_delete_leaf_run {
            let duplicate_check_start = Some(std::time::Instant::now());
            let already_deleted = self.deleted_cell_indices.contains(&cell_idx);
            instrumentation::record_delete_leaf_run_duplicate_check(duplicate_check_start);
            already_deleted
        } else {
            self.deleted_cell_indices.contains(&cell_idx)
        };
        if already_deleted {
            return Ok(TableLeafDeleteRunDelete::Miss(
                TableLeafDeleteRunMissReason::AlreadyDeleted,
            ));
        }
        if self.tree_depth > 1 {
            if self.live_cell_count() <= 1 {
                return Ok(TableLeafDeleteRunDelete::Miss(
                    TableLeafDeleteRunMissReason::NonRootWouldEmptyLeaf,
                ));
            }
            if cell_idx == self.entry.header.cell_count.saturating_sub(1) {
                return Ok(TableLeafDeleteRunDelete::Miss(
                    TableLeafDeleteRunMissReason::NonRootLastCell,
                ));
            }
        }
        let has_compact_cell_area = if self.profile_delete_leaf_run {
            let compact_check_start = Some(std::time::Instant::now());
            let has_compact_cell_area = self.compact_cell_area;
            instrumentation::record_delete_leaf_run_compact_check(compact_check_start);
            has_compact_cell_area
        } else {
            self.compact_cell_area
        };
        if !has_compact_cell_area {
            return Ok(TableLeafDeleteRunDelete::Miss(
                TableLeafDeleteRunMissReason::NonCompactCellArea,
            ));
        }
        let cell_offset = usize::from(self.entry.cell_pointers[usize::from(cell_idx)]);
        let cell = if self.profile_delete_leaf_run {
            let cell_parse_start = Some(std::time::Instant::now());
            let cell = CellRef::parse(
                self.entry.page_data.as_bytes(),
                cell_offset,
                self.entry.header.page_type,
                usable_size,
            );
            instrumentation::record_delete_leaf_run_cell_parse(cell_parse_start);
            cell?
        } else {
            CellRef::parse(
                self.entry.page_data.as_bytes(),
                cell_offset,
                self.entry.header.page_type,
                usable_size,
            )?
        };
        if cell.rowid != Some(rowid) || cell.overflow_page.is_some() {
            return Ok(TableLeafDeleteRunDelete::Miss(
                TableLeafDeleteRunMissReason::CellShapeOrOverflow,
            ));
        }

        self.deleted_cell_indices.push(cell_idx);
        self.dirty = true;
        self.entry.cell_idx = if cell_idx >= self.entry.header.cell_count.saturating_sub(1) {
            self.entry.header.cell_count.saturating_sub(1)
        } else {
            cell_idx
        };
        Ok(TableLeafDeleteRunDelete::Deleted)
    }

    fn search_table_leaf(&self, cx: &Cx, target: i64) -> Result<Option<u16>> {
        let mut lo = 0u16;
        let mut hi = self.entry.header.cell_count;
        while lo < hi {
            observe_cursor_cancellation(cx)?;
            let mid = lo + (hi - lo) / 2;
            let rowid = TableLeafPayloadPatchRun::table_leaf_rowid_at(&self.entry, mid)?;
            match rowid.cmp(&target) {
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    fn live_cell_count(&self) -> u16 {
        self.entry
            .header
            .cell_count
            .saturating_sub(u16::try_from(self.deleted_cell_indices.len()).unwrap_or(u16::MAX))
    }

    fn has_compact_cell_area(&self, usable_size: u32) -> bool {
        Self::has_compact_cell_area_for_entry(&self.entry, usable_size)
    }

    fn has_compact_cell_area_for_entry(entry: &StackEntry, usable_size: u32) -> bool {
        entry.header.first_freeblock == 0
            && entry.header.fragmented_free_bytes == 0
            && entry
                .cell_pointers
                .iter()
                .copied()
                .min()
                .is_some_and(|min_ptr| {
                    usize::from(min_ptr) == entry.header.content_offset(usable_size)
                })
    }

    fn materialize_deletions_incremental_descending(
        &mut self,
        usable_size: u32,
        header_offset: usize,
    ) -> Result<()> {
        let leaf_page_no = self.entry.page_no;
        let mut header = self.entry.header;
        let mut ptrs = self.entry.cell_pointers.clone();

        for &cell_idx in self.deleted_cell_indices.iter().rev() {
            let delete_idx = usize::from(cell_idx);
            let ptr_array_end =
                header_offset + usize::from(header.page_type.header_size()) + (ptrs.len() - 1) * 2;
            let deleted_ptr = usize::from(ptrs[delete_idx]);
            let deleted_upper = if delete_idx == 0 {
                usable_size as usize
            } else {
                usize::from(ptrs[delete_idx - 1])
            };
            if deleted_ptr >= deleted_upper {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "compact table leaf cell offsets are not monotone".to_owned(),
                });
            }
            let deleted_size = deleted_upper - deleted_ptr;
            let old_content_offset = header.content_offset(usable_size);
            if old_content_offset > deleted_ptr {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "compact table leaf content offset exceeds deleted cell".to_owned(),
                });
            }
            let new_content_offset =
                old_content_offset
                    .checked_add(deleted_size)
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "table leaf cell size overflow during delete defragmentation"
                            .to_owned(),
                    })?;
            if new_content_offset > usable_size as usize || new_content_offset < ptr_array_end {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "table leaf cell content overlaps pointer array during delete defragmentation"
                        .to_owned(),
                });
            }
            let page_bytes = self.entry.page_data.as_bytes_mut();
            if old_content_offset < deleted_ptr {
                page_bytes.copy_within(old_content_offset..deleted_ptr, new_content_offset);
            }
            for ptr_slot in ptrs.iter_mut().skip(delete_idx + 1) {
                let adjusted = usize::from(*ptr_slot) + deleted_size;
                *ptr_slot = u16::try_from(adjusted).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell offset {} exceeds u16 range on page {}",
                        adjusted,
                        leaf_page_no.get()
                    ),
                })?;
            }
            ptrs.remove(delete_idx);
            header.cell_content_offset =
                u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell content offset {} exceeds u32 range on page {}",
                        new_content_offset,
                        leaf_page_no.get()
                    ),
                })?;
            header.first_freeblock = 0;
            header.fragmented_free_bytes = 0;
            header.cell_count =
                u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf page {} cell count exceeds u16 range during delete",
                        leaf_page_no.get()
                    ),
                })?;
        }

        let page_bytes = self.entry.page_data.as_bytes_mut();
        header.write(page_bytes, header_offset);
        cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        self.entry.header = header;
        self.entry.cell_pointers = ptrs;
        self.entry.cell_idx = if self.entry.header.cell_count == 0 {
            0
        } else {
            self.entry.header.cell_count - 1
        };
        self.entry.mutation_counter = self.entry.page_data.image_token();
        self.deleted_cell_indices.clear();
        Ok(())
    }

    fn materialize_deletions_compact_descending_single_pass(
        &mut self,
        usable_size: u32,
        header_offset: usize,
        original_len: usize,
    ) -> Result<()> {
        let leaf_page_no = self.entry.page_no;
        let mut header = self.entry.header;
        let live_len = original_len - self.deleted_cell_indices.len();
        let ptr_array_end =
            header_offset + usize::from(header.page_type.header_size()) + live_len * 2;
        let original_ptrs = &self.entry.cell_pointers;
        let mut ptrs = Vec::with_capacity(live_len);
        let mut deleted_idx = 0usize;
        let mut new_content_offset = usable_size as usize;

        {
            let page_bytes = self.entry.page_data.as_bytes_mut();
            let mut live_start = 0usize;
            while live_start < original_len {
                while deleted_idx < self.deleted_cell_indices.len()
                    && usize::from(self.deleted_cell_indices[deleted_idx]) == live_start
                {
                    deleted_idx += 1;
                    live_start += 1;
                }
                if live_start >= original_len {
                    break;
                }

                let mut live_end = live_start + 1;
                while live_end < original_len
                    && (deleted_idx >= self.deleted_cell_indices.len()
                        || usize::from(self.deleted_cell_indices[deleted_idx]) != live_end)
                {
                    live_end += 1;
                }

                let upper = if live_start == 0 {
                    usable_size as usize
                } else {
                    usize::from(original_ptrs[live_start - 1])
                };
                let lower = usize::from(original_ptrs[live_end - 1]);
                if lower >= upper {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "compact table leaf cell offsets are not monotone".to_owned(),
                    });
                }
                let range_size = upper - lower;
                new_content_offset =
                    new_content_offset.checked_sub(range_size).ok_or_else(|| {
                        FrankenError::DatabaseCorrupt {
                            detail: "table leaf cell size overflow during delete defragmentation"
                                .to_owned(),
                        }
                    })?;
                if new_content_offset < ptr_array_end {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "table leaf cell content overlaps pointer array during delete defragmentation"
                            .to_owned(),
                    });
                }
                if new_content_offset != lower {
                    page_bytes.copy_within(lower..upper, new_content_offset);
                }

                for &original_ptr in original_ptrs.iter().take(live_end).skip(live_start) {
                    let adjusted_ptr = new_content_offset + (usize::from(original_ptr) - lower);
                    ptrs.push(u16::try_from(adjusted_ptr).map_err(|_| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "table leaf cell offset {} exceeds u16 range on page {}",
                                adjusted_ptr,
                                leaf_page_no.get()
                            ),
                        }
                    })?);
                }

                live_start = live_end;
            }
        }

        header.cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf cell content offset {} exceeds u32 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        header.first_freeblock = 0;
        header.fragmented_free_bytes = 0;
        header.cell_count =
            u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf page {} cell count exceeds u16 range during delete",
                    leaf_page_no.get()
                ),
            })?;

        let page_bytes = self.entry.page_data.as_bytes_mut();
        header.write(page_bytes, header_offset);
        cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        self.entry.header = header;
        self.entry.cell_pointers = ptrs;
        self.entry.cell_idx = if self.entry.header.cell_count == 0 {
            0
        } else {
            self.entry.header.cell_count - 1
        };
        self.entry.mutation_counter = self.entry.page_data.image_token();
        self.deleted_cell_indices.clear();
        Ok(())
    }

    fn materialize_deletions(&mut self, usable_size: u32) -> Result<()> {
        if self.deleted_cell_indices.is_empty() {
            return Ok(());
        }
        let leaf_page_no = self.entry.page_no;
        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let original_len = self.entry.cell_pointers.len();
        self.deleted_cell_indices.sort_unstable();
        self.deleted_cell_indices.dedup();
        for &cell_idx in &self.deleted_cell_indices {
            if usize::from(cell_idx) >= original_len {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "delete_idx {} out of bounds for page {} with {} cells",
                        cell_idx, leaf_page_no, original_len
                    ),
                });
            }
        }
        let mut header = self.entry.header;
        if !self.has_compact_cell_area(usable_size) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "pending table leaf delete run lost compact page shape".to_owned(),
            });
        }
        if cell_ptrs_are_descending(&self.entry.cell_pointers) {
            if self.deleted_cell_indices.len() >= COMPACT_DELETE_SINGLE_PASS_MIN {
                return self.materialize_deletions_compact_descending_single_pass(
                    usable_size,
                    header_offset,
                    original_len,
                );
            }
            return self.materialize_deletions_incremental_descending(usable_size, header_offset);
        }
        let mut ptrs = Vec::with_capacity(original_len - self.deleted_cell_indices.len());
        let mut cells_to_move = Vec::with_capacity(ptrs.capacity());
        let mut deleted_idx = 0usize;
        for (original_idx, &off) in self.entry.cell_pointers.iter().enumerate() {
            if deleted_idx < self.deleted_cell_indices.len()
                && usize::from(self.deleted_cell_indices[deleted_idx]) == original_idx
            {
                deleted_idx += 1;
                continue;
            }
            let post_delete_idx = ptrs.len();
            let ptr = usize::from(off);
            let size = cell::cell_on_page_size_fast(
                self.entry.page_data.as_bytes(),
                ptr,
                header.page_type,
                usable_size,
            )?;
            cells_to_move.push((ptr, size, post_delete_idx));
            ptrs.push(off);
        }
        sort_cells_desc_by_ptr(&mut cells_to_move);

        let ptr_array_end =
            header_offset + usize::from(header.page_type.header_size()) + ptrs.len() * 2;
        let mut new_content_offset = usable_size as usize;
        {
            let page_bytes = self.entry.page_data.as_bytes_mut();
            for &(ptr, size, post_delete_idx) in &cells_to_move {
                new_content_offset = new_content_offset.checked_sub(size).ok_or_else(|| {
                    FrankenError::DatabaseCorrupt {
                        detail: "table leaf cell size overflow during delete defragmentation"
                            .to_owned(),
                    }
                })?;
                if new_content_offset < ptr_array_end {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "table leaf cell content overlaps pointer array during delete defragmentation"
                            .to_owned(),
                    });
                }
                if new_content_offset != ptr {
                    page_bytes.copy_within(ptr..ptr + size, new_content_offset);
                }
                ptrs[post_delete_idx] = u16::try_from(new_content_offset).map_err(|_| {
                    FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table leaf cell offset {} exceeds u16 range on page {}",
                            new_content_offset,
                            leaf_page_no.get()
                        ),
                    }
                })?;
            }
        }

        header.cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf cell content offset {} exceeds u32 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        header.first_freeblock = 0;
        header.fragmented_free_bytes = 0;
        header.cell_count =
            u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf page {} cell count exceeds u16 range during delete",
                    leaf_page_no.get()
                ),
            })?;

        let page_bytes = self.entry.page_data.as_bytes_mut();
        header.write(page_bytes, header_offset);
        cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        self.entry.header = header;
        self.entry.cell_pointers = ptrs;
        self.entry.cell_idx = if self.entry.header.cell_count == 0 {
            0
        } else {
            self.entry.header.cell_count - 1
        };
        self.entry.mutation_counter = self.entry.page_data.image_token();
        self.deleted_cell_indices.clear();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BulkTableChild {
    page_no: PageNumber,
    max_rowid: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BulkTableGroup {
    start: usize,
    end: usize,
}

#[derive(Debug)]
enum MutationPathTarget {
    TableInsert(i64),
    ExactTableRowid(i64),
    ExactIndexKey(Vec<u8>),
}

/// A B-tree cursor that navigates through B-tree pages using a page stack.
///
/// Generic over the page I/O backend for testability.
#[derive(Debug)]
pub struct BtCursor<P> {
    /// Page I/O backend.
    pager: P,
    /// Root page number of the B-tree.
    root_page: PageNumber,
    /// Usable page size (page_size - reserved_bytes).
    usable_size: u32,
    /// Full page size on disk (usable_size + reserved_bytes).
    ///
    /// Pages written to disk must always be this size. Defaults to
    /// `usable_size` (i.e. reserved_bytes == 0) when not explicitly set.
    page_size: u32,
    /// Whether this is a table (intkey) or index (blobkey) B-tree.
    is_table: bool,
    /// Per-key descending flags for index cursors.
    ///
    /// The implicit trailing rowid suffix always sorts ascending, so this
    /// vector covers only the logical key terms before the rowid.
    index_desc_flags: Vec<bool>,
    /// Per-key collation names for index cursors.
    ///
    /// Missing or `None` entries imply BINARY collation.
    index_collations: Vec<Option<String>>,
    /// Shared collation registry used by text-key comparisons.
    collation_registry: Arc<Mutex<CollationRegistry>>,
    /// Page stack from root to current leaf.
    stack: CursorStack,
    /// Whether the cursor is at EOF (past the last entry).
    at_eof: bool,
    /// Read witnesses collected for SSI evidence.
    read_witnesses: Vec<WitnessKey>,
    /// Optional cap on `read_witnesses.len()`. `0` (the default) preserves
    /// the historical unbounded behavior. When non-zero, witnesses above the
    /// cap are silently dropped from the per-cursor vec; the canonical SSI
    /// evidence recorded into `pager.record_read_witness` is unaffected.
    ///
    /// This exists because long B-tree descents (full-table COUNT(*), lexical
    /// rebuild walks) accumulate one `WitnessKey` per page touched, and on
    /// multi-GB DBs the per-cursor vec balloons into multi-GB RSS allocations
    /// for queries whose result sets are tiny — see
    /// frankensqlite#92 / coding_agent_session_search#252.
    ///
    /// Initialized from the `FSQLITE_READ_WITNESS_CAP` env var at cursor
    /// construction. `set_read_witness_cap` overrides it for programmatic use.
    read_witness_cap: usize,
    /// One-shot flag: log a single `warn!` when the cap first throttles a
    /// witness push, so operators see the policy hit without spamming logs
    /// for every subsequent page.
    read_witness_cap_warned: bool,
    /// Last page number passed to `record_range_page_witness`. Used to dedup
    /// consecutive identical range-page witnesses independently of the
    /// `read_witnesses` vec — without this, enabling `read_witness_cap`
    /// silently breaks dedup (the vec stops growing, so
    /// `self.read_witnesses.last()` becomes stale and adjacent duplicates
    /// leak through to the pager, *increasing* pager-level memory pressure
    /// under the very flag intended to reduce per-cursor memory).
    last_range_witness_page: Option<PageNumber>,
    /// Active per-operation observability stats while a `btree_op` span is open.
    active_op_stats: Option<BtreeOpRuntimeStats>,
    /// Reusable buffer for cell encoding — avoids per-insert heap allocation.
    ///
    /// Taken out of the struct via `std::mem::take` during encoding (to
    /// satisfy the borrow checker), then put back after insert completes.
    /// The Vec capacity is preserved across the take/put cycle so repeated
    /// inserts reuse the same allocation.
    cell_buf: Vec<u8>,
    /// OPT-A4: reusable cell-pointer array for the DELETE defrag path.
    ///
    /// `remove_cell_from_leaf` re-reads the cell-pointer array after the
    /// stack entry has been reshaped by cell removal. Using
    /// `cell::read_cell_pointers_into` with this caller-owned buffer reuses
    /// its allocation across repeated deletes instead of allocating a fresh
    /// `Vec<u16>` every time. Take/put via `std::mem::take` so the same
    /// capacity survives across the borrow boundary.
    defrag_ptrs_scratch: Vec<u16>,
    /// OPT-A4: reusable `(ptr, size, i)` triple array for defragmentation.
    ///
    /// Same take/put pattern as `defrag_ptrs_scratch`: sized to the number
    /// of cells on the page being defragged, cleared before each use, and
    /// repopulated from the live cell pointer array. Eliminates the per-
    /// `remove_cell_from_leaf` allocation (N * 24 bytes for N cells).
    defrag_cells_scratch: Vec<(usize, usize, usize)>,
    /// PERF-W5-1: LRU cache of parsed cell slots keyed by page and page-image
    /// mutation signature.
    ///
    /// Non-append INSERTs repeatedly binary-search the same leaf page and used
    /// to pay `CellRef::parse` for the same slot across probes. Cache entries
    /// are tied to the loaded page image's mutation signature, so page splits,
    /// merges, defragmentation, and freeblock rewrites naturally miss after the
    /// page bytes change.
    ///
    /// The slots are intentionally sparse: a miss for cell `i` parses only
    /// cell `i`. The previous whole-page fill strategy parsed every cell on
    /// the page on a single miss, which made point probes pay for unrelated
    /// cells and left `CellRef::parse` as a top MT 8t hotspot.
    cell_slot_cache: RefCell<CellSlotCache>,
    /// Last rowid successfully inserted via `table_insert`.
    ///
    /// Set on successful leaf insert or balance-for-insert.  Used by the VDBE
    /// engine to implement `sqlite3_last_insert_rowid()` on a per-cursor basis.
    pub last_insert_rowid: Option<i64>,
    /// bd-udl9m: Cached rightmost leaf page plus its maximum rowid.
    ///
    /// Sequential inserts can try this page directly and skip a full
    /// root-to-leaf descent plus leaf binary search. The cache is updated
    /// after successful right-edge inserts and cleared conservatively when a
    /// mutating operation could have invalidated the tree's right edge.
    rightmost_leaf_cache: Option<RightmostLeafCacheEntry>,
    /// Best-known depth for this tree, even when a hot append helper leaves
    /// the cursor stack empty on purpose.
    last_known_depth: Option<usize>,
    /// Four-entry LRU of table-seek leaf anchors.
    ///
    /// Each slot remembers the rowid probe plus the leaf page/cell position
    /// where that seek landed. Later seeks probe cached leaves before they
    /// fall back to a full root-to-leaf descent.
    seek_cache: [Option<TableSeekCacheEntry>; TABLE_SEEK_CACHE_SLOTS],
    /// Monotonic epoch for the current row image visible through this cursor.
    ///
    /// `(page_no, cell_idx)` already identifies logical movement. This epoch
    /// advances when a successful write keeps the cursor on the same slot but
    /// rewrites the underlying page image.
    row_image_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum FirstIndexKeyIntegerLocalRunSegment {
    Matched(i64),
    Mismatch { matched: i64, current_value: i64 },
    NeedsFallback { matched: i64 },
}

impl<P> BtCursor<P> {
    /// Force the cursor into EOF state (not positioned on any row).
    ///
    /// Used by `OP_NullRow` to ensure subsequent `Column`/`Rowid` reads
    /// return NULL without having to navigate the B-tree.
    pub fn invalidate(&mut self) {
        self.at_eof = true;
        self.stack.clear();
        self.rightmost_leaf_cache = None;
        self.last_known_depth = None;
        self.seek_cache.fill(None);
        self.cell_slot_cache.get_mut().clear();
        self.bump_row_image_epoch();
    }

    /// Whether this cursor is for a table (intkey) B-tree.
    #[must_use]
    pub fn is_table(&self) -> bool {
        self.is_table
    }

    /// The root page number of the B-tree this cursor operates on.
    #[must_use]
    pub fn root_page(&self) -> PageNumber {
        self.root_page
    }

    /// Lightweight identity for the cursor's current logical position.
    ///
    /// Used by the VDBE to cache decoded row state while the cursor remains
    /// on the same leaf cell.
    #[must_use]
    pub fn position_stamp(&self) -> Option<CursorPositionStamp> {
        if self.at_eof {
            return None;
        }

        self.stack.last().map(|entry| CursorPositionStamp {
            page_no: entry.page_no.get(),
            cell_idx: entry.cell_idx,
            row_image_epoch: self.row_image_epoch,
        })
    }

    /// The usable page size for this cursor's B-tree.
    #[must_use]
    pub fn usable_size(&self) -> u32 {
        self.usable_size
    }

    /// The full on-disk page size (usable_size + reserved_bytes).
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Set the full on-disk page size when reserved_bytes > 0.
    ///
    /// By default `page_size == usable_size` (no reserved bytes). Call this
    /// after construction when the database header specifies a non-zero
    /// reserved-byte count so that newly built pages are allocated at the
    /// correct full page size.
    pub fn set_page_size(&mut self, page_size: u32) {
        debug_assert!(
            page_size >= self.usable_size,
            "page_size ({page_size}) must be >= usable_size ({})",
            self.usable_size
        );
        self.page_size = page_size;
        self.cell_slot_cache.get_mut().clear();
    }

    fn clear_seek_cache(&mut self) {
        self.seek_cache.fill(None);
    }

    fn clear_rightmost_leaf_cache(&mut self) {
        self.rightmost_leaf_cache = None;
    }

    fn bump_row_image_epoch(&mut self) {
        self.row_image_epoch = self.row_image_epoch.wrapping_add(1);
    }

    #[inline]
    fn page_mutation_counter(page_data: &PageData) -> u64 {
        page_data.image_token()
    }

    fn is_on_rightmost_insert_edge(&self) -> bool {
        let Some((leaf, ancestors)) = self.stack.split_last() else {
            return false;
        };
        if let Some(depth) = self.last_known_depth
            && self.stack.len() < depth
        {
            return false;
        }
        if leaf.page_no != self.root_page && ancestors.is_empty() {
            return false;
        }
        if !leaf.header.page_type.is_leaf() {
            return false;
        }
        let leaf_on_right_edge = if leaf.header.cell_count == 0 {
            self.at_eof
        } else if self.at_eof {
            leaf.cell_idx == leaf.header.cell_count
        } else {
            leaf.cell_idx.saturating_add(1) == leaf.header.cell_count
        };
        leaf_on_right_edge
            && ancestors
                .iter()
                .all(|entry| entry.cell_idx == entry.header.cell_count)
    }

    fn remember_table_seek(&mut self, rowid: i64, page_no: PageNumber, cell_idx: u16) {
        let entry = TableSeekCacheEntry {
            rowid,
            page_no,
            cell_idx,
        };

        let mut refreshed = [None; TABLE_SEEK_CACHE_SLOTS];
        refreshed[0] = Some(entry);

        let mut next_slot = 1usize;
        for existing in self.seek_cache.into_iter().flatten() {
            if existing.page_no == page_no {
                continue;
            }
            if next_slot >= TABLE_SEEK_CACHE_SLOTS {
                break;
            }
            refreshed[next_slot] = Some(existing);
            next_slot += 1;
        }

        self.seek_cache = refreshed;
    }
}

impl<P: PageReader> BtCursor<P> {
    fn first_index_key_integer_from_local_payload(local: &[u8]) -> Result<Option<i64>> {
        if local.is_empty() {
            return Ok(None);
        }

        let (header_size_u64, hdr_varint_len) = match read_varint(local) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };
        let header_size = match usize::try_from(header_size_u64) {
            Ok(size) => size,
            Err(_) => return Ok(None),
        };
        if header_size < hdr_varint_len || header_size > local.len() {
            return Ok(None);
        }

        let (serial_type, _) = match read_varint(&local[hdr_varint_len..header_size]) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };
        let value_len = match serial_type_len(serial_type).and_then(|len| usize::try_from(len).ok())
        {
            Some(len) => len,
            None => return Ok(None),
        };
        let body_offset = header_size;
        let col_end = match body_offset.checked_add(value_len) {
            Some(end) => end,
            None => return Ok(None),
        };
        if col_end > local.len() {
            return Ok(None);
        }

        Ok(Some(match classify_serial_type(serial_type) {
            SerialTypeClass::Zero => 0,
            SerialTypeClass::One => 1,
            SerialTypeClass::Integer => decode_big_endian_signed_fast(&local[body_offset..col_end]),
            _ => return Ok(None),
        }))
    }

    fn first_index_key_integer_local_value_from_cell_offset(
        &self,
        page: &[u8],
        page_type: cell::BtreePageType,
        cell_offset: usize,
    ) -> Result<Option<i64>> {
        if page_type.is_table() {
            return Ok(None);
        }

        let mut pos = cell_offset;
        if page_type.is_interior() {
            if pos + 4 > page.len() {
                return Ok(None);
            }
            pos += 4;
        }

        let (payload_size_raw, payload_varint_len) = match page.get(pos..) {
            Some(rest) => match read_varint(rest) {
                Some(parsed) => parsed,
                None => return Ok(None),
            },
            None => return Ok(None),
        };
        let payload_size = match u32::try_from(payload_size_raw) {
            Ok(size) => size,
            Err(_) => return Ok(None),
        };
        pos = match pos.checked_add(payload_varint_len) {
            Some(next) => next,
            None => return Ok(None),
        };

        let local_size =
            cell::local_payload_size(payload_size, self.usable_size, page_type) as usize;
        let local_end = match pos.checked_add(local_size) {
            Some(end) => end,
            None => return Ok(None),
        };
        if local_end > page.len() || local_end > self.usable_size as usize {
            return Ok(None);
        }

        Self::first_index_key_integer_from_local_payload(&page[pos..local_end])
    }

    fn index_cell_first_key_integer_local_value_at(
        &self,
        entry: &StackEntry,
        cell_idx: u16,
    ) -> Result<Option<i64>> {
        let idx_usize = cell_idx as usize;
        if idx_usize >= entry.cell_pointers.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell index {} out of bounds ({})",
                    cell_idx,
                    entry.cell_pointers.len()
                ),
            });
        }

        let cell_offset = entry.cell_pointers[idx_usize] as usize;
        self.first_index_key_integer_local_value_from_cell_offset(
            entry.page_data.as_bytes(),
            entry.header.page_type,
            cell_offset,
        )
    }

    fn current_first_index_key_integer_local_value(&self) -> Result<Option<i64>> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
        self.index_cell_first_key_integer_local_value_at(top, top.cell_idx)
    }

    /// Probe the current index cell's first key column as an integer using
    /// only the local payload bytes already resident on the leaf page.
    ///
    /// Returns `Ok(Some(...))` when the first column is fully available from
    /// the local payload and is integer-class (`0`, `1`, or integer serial
    /// types). Returns `Ok(None)` when the caller must fall back to the slower
    /// prefix-buffer path (for example because the first field spans overflow
    /// or is non-integer-class).
    pub fn try_probe_current_first_index_key_integer_local(
        &self,
        probe_value: i64,
    ) -> Result<Option<(bool, i64)>> {
        Ok(self
            .current_first_index_key_integer_local_value()?
            .map(|current_value| (current_value == probe_value, current_value)))
    }

    /// Count a matched segment of integer first-key entries while the cursor
    /// stays on a leaf page and the first column is fully available from local
    /// payload bytes. The cursor is left on the first unconsumed row
    /// (mismatch/fallback) or advanced once to the next logical row/eof after
    /// the matched local segment.
    pub fn count_equal_first_index_key_run_integer_local_segment(
        &mut self,
        cx: &Cx,
        probe_value: i64,
    ) -> Result<FirstIndexKeyIntegerLocalRunSegment> {
        enum LocalRunScanOutcome {
            MatchedAll(i64),
            MatchedCurrent(i64),
            Mismatch {
                matched: i64,
                cell_idx: Option<u16>,
                current_value: i64,
            },
            NeedsFallback {
                matched: i64,
                cell_idx: Option<u16>,
            },
        }

        let mut matched_total = 0_i64;
        loop {
            if self.at_eof || self.stack.is_empty() {
                return Ok(FirstIndexKeyIntegerLocalRunSegment::Matched(matched_total));
            }

            let scan_outcome = {
                let top = self
                    .stack
                    .last()
                    .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
                let page_type = top.header.page_type;
                if page_type.is_leaf() && !page_type.is_table() {
                    let start_idx = top.cell_idx;
                    let cell_count = top.header.cell_count;
                    let page = top.page_data.as_bytes();
                    let cell_pointers = &top.cell_pointers;
                    let mut matched = 0_i64;
                    let mut outcome = None;

                    for idx in start_idx..cell_count {
                        let cell_offset =
                            cell_pointers.get(idx as usize).copied().ok_or_else(|| {
                                FrankenError::DatabaseCorrupt {
                                    detail: format!(
                                        "cell index {} out of bounds ({})",
                                        idx,
                                        cell_pointers.len()
                                    ),
                                }
                            })? as usize;

                        match self.first_index_key_integer_local_value_from_cell_offset(
                            page,
                            page_type,
                            cell_offset,
                        )? {
                            Some(value) if value == probe_value => {
                                matched = matched.wrapping_add(1);
                            }
                            Some(current_value) => {
                                outcome = Some(LocalRunScanOutcome::Mismatch {
                                    matched,
                                    cell_idx: Some(idx),
                                    current_value,
                                });
                                break;
                            }
                            None => {
                                outcome = Some(LocalRunScanOutcome::NeedsFallback {
                                    matched,
                                    cell_idx: Some(idx),
                                });
                                break;
                            }
                        }
                    }

                    outcome.unwrap_or(LocalRunScanOutcome::MatchedAll(matched))
                } else if page_type.is_table() {
                    LocalRunScanOutcome::NeedsFallback {
                        matched: 0,
                        cell_idx: Some(top.cell_idx),
                    }
                } else {
                    match self.current_first_index_key_integer_local_value()? {
                        Some(current_value) if current_value == probe_value => {
                            LocalRunScanOutcome::MatchedCurrent(1)
                        }
                        Some(current_value) => LocalRunScanOutcome::Mismatch {
                            matched: 0,
                            cell_idx: None,
                            current_value,
                        },
                        None => LocalRunScanOutcome::NeedsFallback {
                            matched: 0,
                            cell_idx: None,
                        },
                    }
                }
            };

            match scan_outcome {
                LocalRunScanOutcome::MatchedAll(matched)
                | LocalRunScanOutcome::MatchedCurrent(matched) => {
                    matched_total = matched_total.wrapping_add(matched);
                    if !self.advance_next(cx)? {
                        return Ok(FirstIndexKeyIntegerLocalRunSegment::Matched(matched_total));
                    }
                }
                LocalRunScanOutcome::Mismatch {
                    matched,
                    cell_idx,
                    current_value,
                } => {
                    if let Some(cell_idx) = cell_idx {
                        self.stack
                            .last_mut()
                            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?
                            .cell_idx = cell_idx;
                    }
                    return Ok(FirstIndexKeyIntegerLocalRunSegment::Mismatch {
                        matched: matched_total.wrapping_add(matched),
                        current_value,
                    });
                }
                LocalRunScanOutcome::NeedsFallback { matched, cell_idx } => {
                    if let Some(cell_idx) = cell_idx {
                        self.stack
                            .last_mut()
                            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?
                            .cell_idx = cell_idx;
                    }
                    return Ok(FirstIndexKeyIntegerLocalRunSegment::NeedsFallback {
                        matched: matched_total.wrapping_add(matched),
                    });
                }
            }
        }
    }

    /// Create a new cursor positioned before the first entry (at EOF).
    #[must_use]
    pub fn new(pager: P, root_page: PageNumber, usable_size: u32, is_table: bool) -> Self {
        Self::new_with_index_desc(pager, root_page, usable_size, is_table, Vec::new())
    }

    /// Create a new cursor with explicit descending metadata for index keys.
    #[must_use]
    pub fn new_with_index_desc(
        pager: P,
        root_page: PageNumber,
        usable_size: u32,
        is_table: bool,
        index_desc_flags: Vec<bool>,
    ) -> Self {
        Self {
            pager,
            root_page,
            usable_size,
            page_size: usable_size,
            is_table,
            index_desc_flags,
            index_collations: Vec::new(),
            // OPT-1: the default registry (BINARY/NOCASE/RTRIM) never changes
            // and is never mutated except via `set_index_collation_context`.
            // Per-cursor construction of a fresh `Arc<Mutex<CollationRegistry>>`
            // was 0.9% + 0.39% (registry insert + Arc drop) on the INSERT
            // hot path — a new cursor fires for every INSERT/UPDATE/DELETE.
            // Share the default Arc across all cursors via a OnceLock; callers
            // that need custom collations still override via
            // `set_index_collation_context`.
            collation_registry: Arc::clone(default_collation_registry()),
            // Most cursor lifetimes stay within root/interior/leaf depth, and
            // the prepared direct-insert append lane can complete without ever
            // pushing a stack entry. Keep shallow descents inline so both paths
            // avoid heap growth in the common case.
            stack: CursorStack::new(),
            at_eof: true,
            read_witnesses: Vec::new(),
            read_witness_cap: default_read_witness_cap(),
            read_witness_cap_warned: false,
            last_range_witness_page: None,
            active_op_stats: None,
            cell_buf: Vec::new(),
            defrag_ptrs_scratch: Vec::new(),
            defrag_cells_scratch: Vec::new(),
            cell_slot_cache: RefCell::new(CellSlotCache::default()),
            last_insert_rowid: None,
            rightmost_leaf_cache: None,
            last_known_depth: None,
            seek_cache: [None; TABLE_SEEK_CACHE_SLOTS],
            row_image_epoch: 0,
        }
    }

    /// Attach per-index collation metadata and a shared registry.
    pub fn set_index_collation_context(
        &mut self,
        index_collations: Vec<Option<String>>,
        collation_registry: Arc<Mutex<CollationRegistry>>,
    ) {
        self.index_collations = index_collations;
        self.collation_registry = collation_registry;
    }

    /// Returns the descending-key metadata for index cursors.
    #[must_use]
    pub fn index_desc_flags(&self) -> &[bool] {
        &self.index_desc_flags
    }

    /// Returns the per-key collation metadata for index cursors.
    #[must_use]
    pub fn index_collations(&self) -> &[Option<String>] {
        &self.index_collations
    }

    /// Returns the shared collation registry used for index-key comparisons.
    #[must_use]
    pub fn collation_registry(&self) -> Arc<Mutex<CollationRegistry>> {
        Arc::clone(&self.collation_registry)
    }

    /// Returns the read witness keys captured by the cursor.
    #[must_use]
    pub fn witness_keys(&self) -> &[WitnessKey] {
        &self.read_witnesses
    }

    /// Clears captured read witness keys.
    pub fn clear_witness_keys(&mut self) {
        self.read_witnesses.clear();
        // Reset the one-shot warn flag so the next descent can re-warn if it
        // hits the cap again — operators who clear witnesses between queries
        // expect cap policy to surface per-query, not just once per cursor.
        self.read_witness_cap_warned = false;
        // Drop the dedup memory too: callers that clear witnesses between
        // queries expect each descent to start fresh; otherwise the first
        // page of the new descent could be silently deduped against the last
        // page of the previous one.
        self.last_range_witness_page = None;
    }

    /// Set the per-cursor read-witness retention cap.
    ///
    /// `0` disables the cap (unbounded retention, the historical default).
    /// Any other value is a hard upper bound on `witness_keys().len()`;
    /// witnesses above the cap are dropped from the per-cursor vec, but
    /// the canonical SSI evidence in `pager.record_read_witness` is still
    /// recorded for every read.
    pub fn set_read_witness_cap(&mut self, cap: usize) {
        self.read_witness_cap = cap;
        self.read_witness_cap_warned = false;
        // Drop dedup state so a cap toggle never silently dedups against an
        // unrelated past descent.
        self.last_range_witness_page = None;
    }

    /// Returns the current read-witness retention cap (0 = unbounded).
    #[must_use]
    pub fn read_witness_cap(&self) -> usize {
        self.read_witness_cap
    }

    /// True if the per-cursor witness vec is at or above the configured cap.
    /// Always false when the cap is disabled (`read_witness_cap == 0`).
    #[inline]
    fn read_witness_at_cap(&self) -> bool {
        self.read_witness_cap != 0 && self.read_witnesses.len() >= self.read_witness_cap
    }

    /// Return the current leaf page when positioned on a row.
    ///
    /// Returns `None` when the cursor is at EOF or not yet positioned.
    #[must_use]
    pub fn current_page(&self) -> Option<PageNumber> {
        if self.at_eof {
            return None;
        }
        self.stack.last().map(|entry| entry.page_no)
    }

    /// Return the cursor's current root-to-leaf page path, excluding the root.
    ///
    /// Callers that already know the table root can append this slice to their
    /// own root entry to cover the whole cursor path without double-counting
    /// root-only trees.
    #[must_use]
    pub fn current_page_path(&self) -> Vec<PageNumber> {
        self.stack
            .iter()
            .filter_map(|entry| {
                if entry.page_no == self.root_page {
                    None
                } else {
                    Some(entry.page_no)
                }
            })
            .collect()
    }

    /// Advance a table cursor to `rowid`, reusing local leaf state when possible.
    ///
    /// This is intended for monotonic rowid probe streams such as the VDBE's
    /// `RowSetRead` loops for batch UPDATE/DELETE. The cursor first probes the
    /// current leaf, then the immediate next leaf, before falling back to the
    /// normal root-to-leaf seek path.
    pub fn advance_to(&mut self, cx: &Cx, rowid: i64) -> Result<SeekResult> {
        self.with_btree_op(cx, BtreeOpType::Seek, |cursor| {
            cursor.table_advance_to(cx, rowid)
        })
    }

    /// Issue an explicit best-effort prefetch hint for `page_no`.
    ///
    /// This is a non-blocking hint only; callers must not rely on it for
    /// correctness.
    pub fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
        self.issue_prefetch_hint(cx, page_no);
    }

    #[inline]
    fn cell_tag_from_rowid(rowid: i64) -> u64 {
        u64::from_ne_bytes(rowid.to_ne_bytes())
    }

    fn cell_tag_from_index_key(key: &[u8]) -> u64 {
        // Deterministic FNV-1a hash keeps tags stable across runs.
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for &byte in key {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3_u64);
        }
        hash
    }

    fn record_point_witness(&mut self, cx: &Cx, key: WitnessKey) {
        if let WitnessKey::Page(page) = key {
            warn!(
                root_page = self.root_page.get(),
                page = page.get(),
                "policy violation: point operation emitted page-level witness"
            );
        }
        // A point witness breaks the range-witness dedup chain — preserves
        // the historical (pre-cap) semantics where dedup keyed off the
        // vec-tail and any non-Page push reset the chain.
        self.last_range_witness_page = None;
        // Canonical SSI evidence still goes to the pager regardless of cap —
        // that's the source of truth for transaction isolation. The cap only
        // bounds the per-cursor copy returned by `witness_keys()`.
        self.pager.record_read_witness(cx, key.clone());
        if self.read_witness_at_cap() {
            self.maybe_warn_witness_cap_hit();
            return;
        }
        self.read_witnesses.push(key);
    }

    fn record_range_page_witness(&mut self, cx: &Cx, page_no: PageNumber) {
        // Dedup against `last_range_witness_page` rather than
        // `read_witnesses.last()`. The vec-based dedup breaks the moment
        // `read_witness_cap` throttles a push: the vec stops growing, so
        // `.last()` returns a stale entry and adjacent duplicate range
        // witnesses leak through to the pager. That would *increase* pager
        // memory pressure under the very flag intended to reduce per-cursor
        // memory — the opposite of what the operator asked for.
        if self.last_range_witness_page == Some(page_no) {
            return;
        }
        self.last_range_witness_page = Some(page_no);
        let key = WitnessKey::Page(page_no);
        // Same split as `record_point_witness`: pager always sees the read,
        // the per-cursor vec respects the cap.
        self.pager.record_read_witness(cx, key.clone());
        if self.read_witness_at_cap() {
            self.maybe_warn_witness_cap_hit();
            return;
        }
        self.read_witnesses.push(key);
    }

    /// Log a single rate-limited `warn!` the first time the per-cursor
    /// witness cap throttles a push. Subsequent throttles within the same
    /// "session" (until `clear_witness_keys` or `set_read_witness_cap` resets
    /// the flag) are silent — the policy decision was already surfaced.
    fn maybe_warn_witness_cap_hit(&mut self) {
        if self.read_witness_cap_warned {
            return;
        }
        self.read_witness_cap_warned = true;
        warn!(
            root_page = self.root_page.get(),
            cap = self.read_witness_cap,
            "read-witness cap reached on cursor — dropping further per-cursor witnesses (pager-level SSI evidence unaffected)"
        );
    }

    fn issue_prefetch_hint(&self, cx: &Cx, page_no: PageNumber) {
        self.pager.prefetch_page_hint(cx, page_no);
        debug!(
            page_number = page_no.get(),
            source = "btree_descent",
            "issued best-effort btree prefetch hint"
        );
    }

    fn note_page_visit(&mut self, page_no: PageNumber) {
        if let Some(stats) = self.active_op_stats.as_mut() {
            stats.record_page_visit();
            trace!(page_number = page_no.get(), "btree page visit");
        }
    }

    fn note_split_event(&mut self) {
        instrumentation::record_split_event();
        if let Some(stats) = self.active_op_stats.as_mut() {
            stats.record_split();
        }
    }

    fn note_merge_event(&mut self) {
        if let Some(stats) = self.active_op_stats.as_mut() {
            stats.record_merge();
        }
    }

    fn measure_tree_depth(&mut self, cx: &Cx) -> Result<usize> {
        let saved_stats = self.active_op_stats.take();
        let mut depth = 0usize;
        let mut current_page = self.root_page;

        let result = loop {
            if depth >= BTREE_MAX_DEPTH as usize {
                break Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let entry = match self.load_page(cx, current_page) {
                Ok(e) => e,
                Err(err) => break Err(err),
            };
            depth = depth.saturating_add(1);
            if entry.header.page_type.is_leaf() {
                break Ok(depth);
            }

            current_page = if entry.header.cell_count == 0 {
                match entry
                    .header
                    .right_child
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "interior page has no right child".to_owned(),
                    }) {
                    Ok(p) => p,
                    Err(err) => break Err(err),
                }
            } else {
                match self.parse_cell_at(&entry, 0) {
                    Ok(cell) => match cell
                        .left_child
                        .ok_or_else(|| FrankenError::DatabaseCorrupt {
                            detail: "interior cell has no left child".to_owned(),
                        }) {
                        Ok(p) => p,
                        Err(err) => break Err(err),
                    },
                    Err(err) => break Err(err),
                }
            };
        };

        self.active_op_stats = saved_stats;
        result
    }

    /// Count all rows in this table B-tree without decoding cell payloads.
    ///
    /// Walks every leaf page summing `cell_count` values. This avoids key
    /// parsing, overflow chain following, and register materialization — it
    /// only reads page headers. Still O(pages) but with a much smaller
    /// constant factor than the `first()/while next()` row-by-row scan.
    ///
    /// bd-wwqen.1 (B1.1): direct COUNT(*) fast path.
    pub fn count_all_rows(&mut self, cx: &Cx) -> Result<i64> {
        // Save and restore cursor state so this doesn't disturb
        // any in-progress iteration.
        let saved_eof = self.at_eof;
        self.stack.clear();
        self.at_eof = true;

        // bd-wwqen.1: iterative B-tree walk — no recursion overhead.
        let result = self.count_all_rows_iterative(cx);

        // Restore cursor state.
        self.stack.clear();
        self.at_eof = saved_eof;

        result
    }

    /// bd-wwqen.1: Iterative B-tree count matching SQLite's sqlite3BtreeCount
    /// pattern. Walks every page exactly once without recursion, extracting
    /// child page numbers directly from raw cell bytes (4-byte BE at cell
    /// offset) to avoid full parse_cell_at overhead.
    fn count_all_rows_iterative(&mut self, cx: &Cx) -> Result<i64> {
        // Pending child pages for a depth-first walk. Interior pages are read
        // once: all child page numbers are extracted from their cell-pointer
        // array up front instead of rereading the parent for each child.
        let mut pending_pages: SmallVec<[PageNumber; 16]> = SmallVec::new();
        pending_pages.push(self.root_page);
        let mut total: i64 = 0;

        while let Some(current_page) = pending_pages.pop() {
            observe_cursor_cancellation(cx)?;
            self.note_page_visit(current_page);
            self.record_range_page_witness(cx, current_page);

            let page_data = self.pager.read_btree_page_data(cx, current_page)?;
            let page_bytes = page_data.as_bytes();
            let header = cell::parse_page_header(page_bytes, current_page)?;

            if header.page_type.is_leaf() {
                total = total.saturating_add(i64::from(header.cell_count));
            } else {
                let cell_count = header.cell_count;
                let hdr_size = header.page_type.header_size() as usize;
                let right_child =
                    header
                        .right_child
                        .ok_or_else(|| FrankenError::DatabaseCorrupt {
                            detail: "interior page has no right child in count_all_rows".to_owned(),
                        })?;

                // Index interior separator cells are logical entries in this
                // implementation, and cursor next/prev traversal already visits
                // them directly. COUNT on an index root must include them too.
                if !self.is_table {
                    total = total.saturating_add(i64::from(cell_count));
                }

                pending_pages.push(right_child);
                for cell_idx in (0..cell_count).rev() {
                    let cell_ptr = Self::read_cell_pointer_inline(
                        page_bytes,
                        current_page,
                        hdr_size,
                        cell_idx,
                    )?;
                    let child = Self::read_child_at_offset(page_bytes, cell_ptr as usize)?;
                    pending_pages.push(child);
                }
            }
        }

        Ok(total)
    }

    /// bd-wwqen.1: Read a 4-byte BE child page number directly from raw page
    /// bytes at the given cell offset. Used by count_all_rows_iterative to
    /// avoid needing the allocated cell_pointers Vec.
    fn read_child_at_offset(page: &[u8], cell_offset: usize) -> Result<PageNumber> {
        if cell_offset + 4 > page.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "interior cell extends past page in count_all_rows".to_owned(),
            });
        }
        let pgno = u32::from_be_bytes([
            page[cell_offset],
            page[cell_offset + 1],
            page[cell_offset + 2],
            page[cell_offset + 3],
        ]);
        PageNumber::new(pgno).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "interior cell has zero left-child pointer in count_all_rows".to_owned(),
        })
    }

    /// bd-wwqen.1: Read the cell pointer at index `cell_idx` directly from
    /// raw page bytes without allocating a Vec<u16> copy. The cell pointer
    /// array starts right after the page header, so staying on the page image
    /// keeps the descent hot path on the node's contiguous prefix.
    //
    // Hot path: invoked once per cell during binary-search descent and per
    // visited child during full-tree walks. The body is small (one offset
    // calc + one 2-byte BE load) but the in-body `format!` previously kept
    // the function out of the inliner, so the dyn-IO monomorphization
    // showed up as a separate ~0.37% MT8 self-time symbol on
    // `tests/artifacts/perf/profiling-mt-mvcc-20260424T161631Z/perf_mt8_flat.txt`.
    // Outlining the cold corruption-error builder lets `#[inline]` apply,
    // and `page.get(..).try_into::<&[u8; 2]>()` collapses the manual
    // `ptr_offset + 2 > page.len()` guard plus the two `page[i]` indices
    // into a single bounds check (same array-conversion bounds-elide
    // pattern that took `BtreePageHeader::parse` from 10.7 ns to 3.7 ns
    // in commit 1f266968).
    #[inline]
    fn read_cell_pointer_inline(
        page: &[u8],
        page_no: PageNumber,
        header_size: usize,
        cell_idx: u16,
    ) -> Result<u16> {
        let header_offset = cell::header_offset_for_page(page_no);
        let ptr_offset = header_offset + header_size + (cell_idx as usize) * 2;
        let bytes: &[u8; 2] = page
            .get(ptr_offset..ptr_offset + 2)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| cell_pointer_oob(cell_idx, page_no, ptr_offset, page.len()))?;
        Ok(u16::from_be_bytes(*bytes))
    }

    #[inline]
    fn read_stack_entry_cell_pointer_inline(entry: &StackEntry, cell_idx: u16) -> Result<u16> {
        if cell_idx >= entry.header.cell_count {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell index {} out of bounds ({})",
                    cell_idx, entry.header.cell_count
                ),
            });
        }

        let header_size = usize::from(entry.header.page_type.header_size());
        Self::read_cell_pointer_inline(
            entry.page_data.as_bytes(),
            entry.page_no,
            header_size,
            cell_idx,
        )
    }

    #[inline]
    fn read_interior_child_inline(entry: &StackEntry, cell_idx: u16) -> Result<PageNumber> {
        if cell_idx >= entry.header.cell_count {
            return entry
                .header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior page has no right child".to_owned(),
                });
        }

        let cell_offset = usize::from(Self::read_stack_entry_cell_pointer_inline(entry, cell_idx)?);
        Self::read_child_at_offset(entry.page_data.as_bytes(), cell_offset)
    }

    /// Decode the routing rowid stored in an interior table cell.
    ///
    /// An interior table cell is `[left-child: u32][rowid: varint]`. Keeping
    /// this decoder shared by descent and insertion-position validation makes
    /// it impossible for those two routing decisions to drift apart.
    fn table_interior_rowid_at(entry: &StackEntry, cell_idx: u16) -> Result<i64> {
        if !(entry.header.page_type.is_interior() && entry.header.page_type.is_table()) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "expected interior table page while reading routing rowid, got {:?}",
                    entry.header.page_type
                ),
            });
        }

        let cell_offset = usize::from(Self::read_stack_entry_cell_pointer_inline(entry, cell_idx)?);
        let cell_data = entry
            .page_data
            .as_bytes()
            .get(cell_offset..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior table cell pointer {} points past page end (len {})",
                    cell_offset,
                    entry.page_data.as_bytes().len()
                ),
            })?;
        let rowid_bytes = cell_data
            .get(4..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "interior table cell is shorter than its child pointer".to_owned(),
            })?;
        let (rowid, _) = read_varint(rowid_bytes).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "interior table cell has invalid rowid varint".to_owned(),
        })?;
        #[allow(clippy::cast_possible_wrap)]
        Ok(rowid as i64)
    }

    /// Check that the cursor's current table-leaf position is a valid slot
    /// for inserting `rowid`.
    ///
    /// Leaf-local ordering is necessary but not sufficient: after a delete
    /// drains a subtree, the cursor can legitimately be positioned on the
    /// logical successor in the next subtree. Inserting the deleted rowid at
    /// that successor keeps the leaf sorted while placing it on the wrong side
    /// of an ancestor separator. Validate both local neighbours and the full
    /// root-to-leaf routing interval, including every parent/child link.
    fn current_position_admits_table_insert(&self, rowid: i64) -> Result<bool> {
        let Some((leaf, ancestors)) = self.stack.split_last() else {
            return Ok(false);
        };
        if !(leaf.header.page_type.is_leaf() && leaf.header.page_type.is_table()) {
            return Ok(false);
        }

        if leaf.header.cell_count == 0 {
            return Ok(leaf.page_no == self.root_page && ancestors.is_empty() && self.at_eof);
        }

        if self.at_eof {
            let last_rowid = Self::table_leaf_rowid_at(leaf, leaf.header.cell_count - 1)?;
            if rowid <= last_rowid {
                return Ok(false);
            }
        } else {
            if leaf.cell_idx >= leaf.header.cell_count {
                return Ok(false);
            }
            let successor = Self::table_leaf_rowid_at(leaf, leaf.cell_idx)?;
            if rowid >= successor {
                return Ok(false);
            }
            if leaf.cell_idx > 0 {
                let predecessor = Self::table_leaf_rowid_at(leaf, leaf.cell_idx - 1)?;
                if rowid <= predecessor {
                    return Ok(false);
                }
            }
        }

        let mut routed_child = leaf.page_no;
        for parent in ancestors.iter().rev() {
            if !(parent.header.page_type.is_interior() && parent.header.page_type.is_table()) {
                return Ok(false);
            }

            let child_idx = parent.cell_idx;
            if Self::read_interior_child_inline(parent, child_idx)? != routed_child {
                return Ok(false);
            }
            if child_idx > 0 && Self::table_interior_rowid_at(parent, child_idx - 1)? >= rowid {
                return Ok(false);
            }
            if child_idx < parent.header.cell_count
                && Self::table_interior_rowid_at(parent, child_idx)? < rowid
            {
                return Ok(false);
            }
            routed_child = parent.page_no;
        }

        Ok(routed_child == self.root_page)
    }

    fn record_depth_gauge(&mut self, cx: &Cx) -> Result<()> {
        let depth = if self.stack.is_empty() {
            match self.last_known_depth {
                Some(depth) => depth,
                None => self.measure_tree_depth(cx)?,
            }
        } else {
            self.stack.len()
        };
        self.last_known_depth = Some(depth);
        instrumentation::set_depth_gauge(depth);
        Ok(())
    }

    fn current_tree_depth_hint(&self) -> Option<usize> {
        if self.stack.is_empty() {
            self.last_known_depth
        } else if self.stack.len() == 1 {
            self.stack.last().and_then(|entry| {
                self.rightmost_leaf_cache
                    .as_ref()
                    .filter(|cached| cached.page_no == entry.page_no)
                    .map(|cached| cached.tree_depth)
                    .or(Some(1))
            })
        } else {
            Some(self.stack.len())
        }
    }

    #[inline]
    fn with_btree_op<T, F>(&mut self, cx: &Cx, op_type: BtreeOpType, work: F) -> Result<T>
    where
        F: FnOnce(&mut Self) -> Result<T>,
    {
        instrumentation::record_operation(op_type);

        // Keep the common path shaped like SQLite's narrow step/execute flow:
        // one branch, direct work call, then a tiny postlude. The traced/stats
        // machinery stays in a cold helper so it does not bloat the hot body.
        if tracing::enabled!(target: "fsqlite.btree", Level::DEBUG) {
            return self.with_btree_op_tracing(cx, op_type, work);
        }

        let result = work(self);
        self.finish_btree_op_postlude(cx, op_type);
        result
    }

    #[inline]
    fn finish_btree_op_postlude(&mut self, cx: &Cx, op_type: BtreeOpType) {
        if let Err(error) = self.record_depth_gauge(cx) {
            debug!(
                op_type = op_type.as_str(),
                error = %error,
                "failed to refresh btree depth gauge"
            );
        }

        if !matches!(op_type, BtreeOpType::Seek) {
            self.clear_seek_cache();
        }
    }

    #[cold]
    #[inline(never)]
    fn with_btree_op_tracing<T, F>(&mut self, cx: &Cx, op_type: BtreeOpType, work: F) -> Result<T>
    where
        F: FnOnce(&mut Self) -> Result<T>,
    {
        let span = tracing::span!(
            Level::DEBUG,
            "btree_op",
            op_type = op_type.as_str(),
            pages_visited = tracing::field::Empty,
            splits = tracing::field::Empty,
            merges = tracing::field::Empty
        );
        let _entered = span.enter();
        debug!(op_type = op_type.as_str(), "starting btree operation");

        self.active_op_stats = Some(BtreeOpRuntimeStats::default());
        let result = work(self);
        self.finish_btree_op_postlude(cx, op_type);

        let stats = self.active_op_stats.take().unwrap_or_default();
        span.record("pages_visited", stats.pages_visited);
        span.record("splits", stats.splits);
        span.record("merges", stats.merges);

        if let Err(error) = &result {
            debug!(
                op_type = op_type.as_str(),
                pages_visited = stats.pages_visited,
                splits = stats.splits,
                merges = stats.merges,
                error = %error,
                "btree operation failed"
            );
        } else {
            debug!(
                op_type = op_type.as_str(),
                pages_visited = stats.pages_visited,
                splits = stats.splits,
                merges = stats.merges,
                "btree operation completed"
            );
        }

        result
    }

    /// Load a page into a stack entry.
    fn load_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<StackEntry> {
        observe_cursor_cancellation(cx)?;

        // Alien Optimization: Hot-path Stack Elision.
        // We can only elide the load if the page hasn't been modified
        // in the current transaction (is not dirty).
        if let Some(existing) = self.stack.last() {
            if existing.page_no == page_no && !self.pager.is_dirty(page_no) {
                // In MVCC, unmodified pages for a given snapshot are immutable.
                let mut cached = existing.clone();
                cached.cell_idx = 0;
                self.note_page_visit(page_no);
                return Ok(cached);
            }
        }

        self.note_page_visit(page_no);
        let page_data = self.pager.read_btree_page_data(cx, page_no)?;
        let header_offset = cell::header_offset_for_page(page_no);
        let header = cell::parse_page_header(page_data.as_bytes(), page_no)?;
        let mut cell_pointers = take_pooled_cell_pointers();
        cell::read_cell_pointers_into(
            page_data.as_bytes(),
            &header,
            header_offset,
            &mut cell_pointers,
        )?;
        let mutation_counter = Self::page_mutation_counter(&page_data);

        Ok(StackEntry {
            page_no,
            page_data,
            header,
            cell_pointers,
            mutation_counter,
            cell_idx: 0,
        })
    }

    /// Reload a page from the pager, bypassing the stack-entry cache.
    ///
    /// This is required immediately after in-place writes because some test
    /// pagers do not surface dirty-state through `is_dirty()`.
    fn reload_page_fresh(&mut self, cx: &Cx, page_no: PageNumber) -> Result<StackEntry> {
        observe_cursor_cancellation(cx)?;
        self.note_page_visit(page_no);
        instrumentation::record_page_header_rebuild();
        let page_data = self.pager.read_btree_page_data(cx, page_no)?;
        let header_offset = cell::header_offset_for_page(page_no);
        let header = cell::parse_page_header(page_data.as_bytes(), page_no)?;
        let mut cell_pointers = take_pooled_cell_pointers();
        cell::read_cell_pointers_into(
            page_data.as_bytes(),
            &header,
            header_offset,
            &mut cell_pointers,
        )?;
        let mutation_counter = Self::page_mutation_counter(&page_data);
        Ok(StackEntry {
            page_no,
            page_data,
            header,
            cell_pointers,
            mutation_counter,
            cell_idx: 0,
        })
    }

    fn parse_cell_slot_at(&self, entry: &StackEntry, idx: u16) -> Result<CachedCellSlot> {
        let idx_usize = usize::from(idx);
        let offset = if idx_usize < entry.cell_pointers.len() {
            usize::from(entry.cell_pointers[idx_usize])
        } else {
            usize::from(Self::read_stack_entry_cell_pointer_inline(entry, idx)?)
        };
        let cell = CellRef::parse(
            entry.page_data.as_bytes(),
            offset,
            entry.header.page_type,
            self.usable_size,
        )?;
        Ok(CachedCellSlot::from_cell_ref(&cell))
    }

    fn parse_cell_ref_at(&self, entry: &StackEntry, idx: u16) -> Result<CellRef> {
        let idx_usize = usize::from(idx);
        let offset = if idx_usize < entry.cell_pointers.len() {
            usize::from(entry.cell_pointers[idx_usize])
        } else {
            usize::from(Self::read_stack_entry_cell_pointer_inline(entry, idx)?)
        };
        CellRef::parse(
            entry.page_data.as_bytes(),
            offset,
            entry.header.page_type,
            self.usable_size,
        )
    }

    /// Parse a cell at the given index on the top-of-stack page.
    fn parse_cell_at(&self, entry: &StackEntry, idx: u16) -> Result<CellRef> {
        if idx >= entry.header.cell_count {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell index {} out of bounds ({})",
                    idx, entry.header.cell_count
                ),
            });
        }

        if let Some(slot) =
            self.cell_slot_cache
                .borrow_mut()
                .get(entry.page_no, entry.mutation_counter, idx)
        {
            return Ok(slot.into_cell_ref());
        }

        let slot = self.parse_cell_slot_at(entry, idx)?;
        self.cell_slot_cache
            .borrow_mut()
            .insert(entry.page_no, entry.mutation_counter, idx, slot);
        Ok(slot.into_cell_ref())
    }

    /// Parse a cell without updating the cursor-local cell-slot cache.
    ///
    /// This is for single-consumer sequential scans where the caller needs
    /// rowid and payload exactly once before advancing. Binary-search and
    /// repeated-probe paths should keep using [`Self::parse_cell_at`].
    fn parse_cell_at_uncached(&self, entry: &StackEntry, idx: u16) -> Result<CellRef> {
        if idx >= entry.header.cell_count {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell index {} out of bounds ({})",
                    idx, entry.header.cell_count
                ),
            });
        }

        self.parse_cell_ref_at(entry, idx)
    }

    /// Move the cursor to the first entry in the subtree rooted at `page_no`.
    ///
    /// Fully iterative (frankensqlite#96): descent walks down the leftmost path
    /// in the outer loop, and an **empty leaf** is handled inline by an ascend-
    /// and-recover scan rather than by calling `advance_next_impl`. When the
    /// recovery finds an interior page with a further child, a table B-tree
    /// re-descends into that sibling's leftmost path by continuing the outer
    /// loop (the stack is maintained in place, so no resume snapshot is needed),
    /// while an index B-tree returns its current separator key — the smallest
    /// remaining entry. This removes the former `move_to_leftmost_leaf` <->
    /// `advance_next_impl` mutual recursion entirely; forward progress is the
    /// same explicit invariant `advance_next_impl` already enforces (every
    /// iteration returns, advances a cell index, or pops the stack, bounded by
    /// `max_iterations`).
    ///
    /// **History:** the previous recursion shape hid two frankensqlite#95-class
    /// forward-progress defects (infinite loops on empty subtrees / replayed
    /// subtrees after an interior pop), since fixed. The reverse-iteration and
    /// round-trip property tests (`prop_btree_reverse_iter_matches_btreemap_rev`,
    /// `prop_btree_forward_and_reverse_round_trip`,
    /// `prop_index_btree_reverse_iter_matches_sorted_rev`) guard this path.
    fn move_to_leftmost_leaf(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        record_leaf_witness: bool,
    ) -> Result<bool> {
        // Bound on the empty-leaf recovery scan below; mirrors the ceiling in
        // `advance_next_impl`. Generous so legitimate cases (descend -> empty
        // leaf -> ascend -> descend into a sibling, possibly across several
        // empty subtrees) have ample headroom.
        let max_iterations = (BTREE_MAX_DEPTH as usize).saturating_mul(8).max(128);
        let mut current_page = page_no;
        loop {
            observe_cursor_cancellation(cx)?;
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let mut entry = self.load_page(cx, current_page)?;
            entry.cell_idx = 0;

            if entry.header.page_type.is_leaf() {
                let leaf_page_no = entry.page_no;
                if entry.header.cell_count == 0 {
                    // Empty leaf. Previously this delegated to
                    // `advance_next_impl`, which created a move_to_leftmost_leaf
                    // <-> advance_next_impl mutual recursion (frankensqlite#96).
                    // Instead, recover inline: record the witness, then ascend
                    // until we find an interior page with another child to
                    // visit. For a table B-tree we re-descend into that sibling
                    // by continuing this loop (`current_page = child`); for an
                    // index B-tree the parent separator key is itself the next
                    // logical entry, so we stop there. This keeps the cursor
                    // stack maintained in place, so no resume snapshot is
                    // needed and there is no call back into advance_next_impl.
                    self.stack.push(entry);
                    if record_leaf_witness {
                        self.record_range_page_witness(cx, leaf_page_no);
                    }
                    self.at_eof = false;
                    self.stack.pop();

                    let mut recovered = false;
                    for _ in 0..max_iterations {
                        observe_cursor_cancellation(cx)?;
                        let Some(top) = self.stack.last() else {
                            self.at_eof = true;
                            return Ok(false);
                        };
                        let parent_cell_idx = top.cell_idx;
                        let parent_cell_count = top.header.cell_count;

                        if parent_cell_idx < parent_cell_count {
                            // For index B-trees the separator at the current
                            // position is the smallest remaining entry; return
                            // it in place (no descent).
                            if !self.is_table {
                                return Ok(true);
                            }
                            // Table B-tree: advance to the next child and
                            // re-descend its leftmost path via the outer loop.
                            let next_child_idx = parent_cell_idx + 1;
                            let child = {
                                let parent = self
                                    .stack
                                    .last()
                                    .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
                                Self::child_page_at(parent, next_child_idx)?
                            };
                            if let Some(parent) = self.stack.last_mut() {
                                parent.cell_idx = next_child_idx;
                            }
                            self.issue_prefetch_hint(cx, child);
                            current_page = child;
                            recovered = true;
                            break;
                        }
                        // Visited every child including right_child; ascend.
                        self.stack.pop();
                    }
                    if recovered {
                        continue;
                    }

                    debug_assert!(
                        false,
                        "BtCursor::move_to_leftmost_leaf (empty-leaf recovery) \
                         exceeded {} iterations without forward progress \
                         (frankensqlite#96); root_page = {}, stack_depth = {}",
                        max_iterations,
                        self.root_page.get(),
                        self.stack.len(),
                    );
                    self.at_eof = true;
                    return Ok(false);
                }
                self.stack.push(entry);
                self.at_eof = false;
                if record_leaf_witness {
                    self.record_range_page_witness(cx, leaf_page_no);
                }
                return Ok(true);
            }

            // Interior page: follow the leftmost child (cell 0's left child).
            if entry.header.cell_count == 0 {
                // Interior page with no cells — follow right_child.
                let right =
                    entry
                        .header
                        .right_child
                        .ok_or_else(|| FrankenError::DatabaseCorrupt {
                            detail: "interior page has no right child".to_owned(),
                        })?;
                entry.cell_idx = 0;
                self.stack.push(entry);
                self.issue_prefetch_hint(cx, right);
                current_page = right;
            } else {
                let child = Self::read_interior_child_inline(&entry, 0)?;
                entry.cell_idx = 0;
                self.stack.push(entry);
                self.issue_prefetch_hint(cx, child);
                current_page = child;
            }
        }
    }

    /// Move the cursor to the last entry in the subtree rooted at `page_no`.
    ///
    /// Fully iterative (frankensqlite#96), the mirror image of
    /// `move_to_leftmost_leaf`: descent walks down the rightmost path in the
    /// outer loop, and an **empty leaf** is handled inline by an ascend-and-
    /// recover scan rather than by calling `advance_prev`. When recovery finds
    /// an interior page with an earlier child, a table B-tree re-descends into
    /// that sibling's rightmost path by continuing the outer loop, while an
    /// index B-tree repositions on the separator just before the current cursor
    /// — the largest remaining entry. This removes the former
    /// `move_to_rightmost_leaf` <-> `advance_prev` mutual recursion entirely.
    ///
    /// Note the deliberate asymmetry with the leftmost variant: an exhausted
    /// stack here means "before the first row", so it returns `Ok(false)`
    /// *without* setting `at_eof` (which denotes after-last) — matching
    /// `advance_prev`'s before-first handling.
    ///
    /// **History:** the previous recursion shape hid two frankensqlite#95-class
    /// backward-progress defects (infinite loops / replayed subtrees after an
    /// interior pop), since fixed. The reverse-iteration and round-trip property
    /// tests guard this path.
    fn move_to_rightmost_leaf(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        record_leaf_witness: bool,
    ) -> Result<bool> {
        // Bound on the empty-leaf recovery scan below; mirrors `advance_prev`.
        let max_iterations = (BTREE_MAX_DEPTH as usize).saturating_mul(8).max(128);
        let mut current_page = page_no;
        loop {
            observe_cursor_cancellation(cx)?;
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let mut entry = self.load_page(cx, current_page)?;

            if entry.header.page_type.is_leaf() {
                let leaf_page_no = entry.page_no;
                if entry.header.cell_count == 0 {
                    // Empty leaf. Previously this delegated to `advance_prev`,
                    // creating a move_to_rightmost_leaf <-> advance_prev mutual
                    // recursion (frankensqlite#96). Recover inline (mirror image
                    // of move_to_leftmost_leaf): record the witness, then ascend
                    // until we find an interior page with an earlier child. For
                    // a table B-tree we re-descend into that sibling's rightmost
                    // path via this loop; for an index B-tree the separator key
                    // just before the current position is the previous logical
                    // entry, so we stop there. No call back into advance_prev.
                    entry.cell_idx = 0;
                    self.stack.push(entry);
                    if record_leaf_witness {
                        self.record_range_page_witness(cx, leaf_page_no);
                    }
                    self.at_eof = false;
                    self.stack.pop();

                    let mut recovered = false;
                    for _ in 0..max_iterations {
                        observe_cursor_cancellation(cx)?;
                        // An exhausted stack here means "before the first row".
                        // Match advance_prev: return false WITHOUT setting
                        // at_eof (at_eof denotes after-last, not before-first).
                        let Some(top) = self.stack.last() else {
                            return Ok(false);
                        };
                        let parent_cell_idx = top.cell_idx;

                        if parent_cell_idx > 0 {
                            // For index B-trees the separator just before the
                            // current position is the largest remaining entry;
                            // reposition there in place (no descent).
                            if !self.is_table {
                                if let Some(parent) = self.stack.last_mut() {
                                    parent.cell_idx = parent_cell_idx - 1;
                                }
                                self.at_eof = false;
                                return Ok(true);
                            }
                            // Table B-tree: step to the previous child and
                            // re-descend its rightmost path via the outer loop.
                            let prev_child_idx = parent_cell_idx - 1;
                            let child = {
                                let parent = self
                                    .stack
                                    .last()
                                    .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
                                Self::child_page_at(parent, prev_child_idx)?
                            };
                            if let Some(parent) = self.stack.last_mut() {
                                parent.cell_idx = prev_child_idx;
                            }
                            self.issue_prefetch_hint(cx, child);
                            current_page = child;
                            recovered = true;
                            break;
                        }
                        // Came from the leftmost child; ascend further.
                        self.stack.pop();
                    }
                    if recovered {
                        continue;
                    }

                    debug_assert!(
                        false,
                        "BtCursor::move_to_rightmost_leaf (empty-leaf recovery) \
                         exceeded {} iterations without reverse progress \
                         (frankensqlite#96); root_page = {}, stack_depth = {}",
                        max_iterations,
                        self.root_page.get(),
                        self.stack.len(),
                    );
                    self.at_eof = true;
                    return Ok(false);
                }
                entry.cell_idx = entry.header.cell_count - 1;
                self.stack.push(entry);
                self.at_eof = false;
                if record_leaf_witness {
                    self.record_range_page_witness(cx, leaf_page_no);
                }
                return Ok(true);
            }

            // Interior page: follow the right-most child.
            let right = entry
                .header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior page has no right child".to_owned(),
                })?;
            entry.cell_idx = entry.header.cell_count;
            self.stack.push(entry);
            self.issue_prefetch_hint(cx, right);
            current_page = right;
        }
    }

    /// Get the child page for an interior page at the given cell index.
    ///
    /// If `cell_idx == cell_count`, returns the right child. Otherwise,
    /// returns the left child of the cell at `cell_idx`.
    ///
    /// bd-4i4vh.3: this used to route through `parse_cell_at`, paying for a
    /// full `CellRef::parse` (varint decodes for payload size / rowid, local
    /// and overflow bounds checks) on every interior-page descent probe just
    /// to extract the 4-byte left-child pointer. Interior cells start with
    /// that pointer as their first field, so `read_interior_child_inline`
    /// reads it directly from the page image — no varint work, no cell-slot
    /// cache traffic — and covers the `>= cell_count` case with `right_child`
    /// already.
    #[inline]
    fn child_page_at(entry: &StackEntry, cell_idx: u16) -> Result<PageNumber> {
        Self::read_interior_child_inline(entry, cell_idx)
    }

    /// Recompute which child slot in an ancestor page points at `child_page_no`.
    ///
    /// Upward split propagation cannot trust the stale cursor-stack `cell_idx`
    /// after a lower-level balance rewrites the tree shape underneath it.
    fn find_child_slot_by_page_no(
        &mut self,
        cx: &Cx,
        parent_page_no: PageNumber,
        child_page_no: PageNumber,
    ) -> Result<u16> {
        let entry = self.reload_page_fresh(cx, parent_page_no)?;
        if !entry.header.page_type.is_interior() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cannot locate child slot in non-interior page {}",
                    parent_page_no
                ),
            });
        }

        for child_idx in 0..=entry.header.cell_count {
            if Self::child_page_at(&entry, child_idx)? == child_page_no {
                return Ok(child_idx);
            }
        }

        Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "ancestor page {} has no child pointer to page {}",
                parent_page_no, child_page_no
            ),
        })
    }

    /// Seek to a rowid in a table B-tree. Returns the seek result.
    fn table_seek(&mut self, cx: &Cx, target_rowid: i64) -> Result<SeekResult> {
        let res = self.table_seek_for_insert(cx, target_rowid)?;
        if !res.is_found() && self.at_eof {
            // We fell off the right edge of the leaf.
            // Determine if there is a successor up the tree.
            let mut has_successor = false;
            for parent in self.stack.iter().rev().skip(1) {
                if parent.cell_idx < parent.header.cell_count {
                    has_successor = true;
                    break;
                }
            }

            if has_successor {
                // There is a successor. Reset eof and use advance_next to reach it.
                self.at_eof = false;
                let advanced = self.advance_next(cx)?;
                if advanced {
                    let successor = self
                        .stack
                        .last()
                        .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
                    let successor_rowid = Self::table_leaf_rowid_at(successor, successor.cell_idx)?;
                    if successor_rowid == target_rowid {
                        let successor_page = self
                            .current_page()
                            .map_or_else(|| "unknown".to_owned(), |page_no| page_no.to_string());
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "table rowid seek on root {} missed scan-visible rowid {} on successor page {}",
                                self.root_page, target_rowid, successor_page
                            ),
                        });
                    }
                } else {
                    self.at_eof = true;
                }
            }
        }
        Ok(res)
    }

    /// Advance a table cursor to `target_rowid`, reusing nearby leaf pages first.
    fn table_advance_to(&mut self, cx: &Cx, target_rowid: i64) -> Result<SeekResult> {
        observe_cursor_cancellation(cx)?;

        let Some(entry) = self.load_current_table_leaf(cx)? else {
            return self.table_seek(cx, target_rowid);
        };

        let Some((min_rowid, max_rowid)) = Self::table_leaf_rowid_bounds(&entry)? else {
            return self.table_seek(cx, target_rowid);
        };

        if target_rowid >= min_rowid && target_rowid <= max_rowid {
            return self.position_on_loaded_table_leaf(cx, entry, target_rowid);
        }

        if target_rowid > max_rowid
            && self.advance_to_next_table_leaf(cx)?
            && let Some(next_entry) = self.load_current_table_leaf(cx)?
            && let Some((next_min_rowid, next_max_rowid)) =
                Self::table_leaf_rowid_bounds(&next_entry)?
            && target_rowid >= next_min_rowid
            && target_rowid <= next_max_rowid
        {
            return self.position_on_loaded_table_leaf(cx, next_entry, target_rowid);
        }

        self.table_seek(cx, target_rowid)
    }

    /// Internal seek used by INSERT that anchors the cursor on the leaf where
    /// the target belongs, even if it falls off the right edge.
    fn table_seek_for_insert(&mut self, cx: &Cx, target_rowid: i64) -> Result<SeekResult> {
        observe_cursor_cancellation(cx)?;

        if let Some(result) = self.try_table_seek_cache(cx, target_rowid)? {
            return Ok(result);
        }

        self.stack.clear();
        let mut current_page = self.root_page;

        loop {
            observe_cursor_cancellation(cx)?;
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let entry = self.load_page(cx, current_page)?;

            // Guard: detect is_table vs actual page-type mismatch early.
            // If the cursor was opened with is_table=true but the page is
            // actually an index page, binary_search_table_* will fail with
            // "cell has no rowid". Catch this here with a clearer error.
            if !entry.header.page_type.is_table() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table_seek called on index page (type {:?}, page {}, root {}): \
                         cursor is_table flag likely incorrect",
                        entry.header.page_type, current_page, self.root_page
                    ),
                });
            }

            if entry.header.page_type.is_leaf() {
                // Table leaf pages are integer-keyed by rowid, so use a
                // bounded interpolation probe before falling back to binary.
                let result = Self::search_integer_key_table_leaf(cx, &entry, target_rowid)?;
                match result {
                    BinarySearchResult::Found(idx) => {
                        let mut entry = entry;
                        entry.cell_idx = idx;
                        self.stack.push(entry);
                        self.at_eof = false;
                        self.remember_table_seek(target_rowid, current_page, idx);
                        self.record_point_witness(
                            cx,
                            WitnessKey::Cell {
                                btree_root: self.root_page,
                                leaf_page: current_page,
                                tag: Self::cell_tag_from_rowid(target_rowid),
                            },
                        );
                        return Ok(SeekResult::Found);
                    }
                    BinarySearchResult::NotFound(idx) => {
                        let mut entry = entry;
                        if idx >= entry.header.cell_count {
                            // Target is strictly greater than the last key on this leaf.
                            // Keep the path anchored to this right-most leaf and mark EOF,
                            // so callers (notably INSERT) still have a valid stack context.
                            let landed_cell_idx = entry.header.cell_count.saturating_sub(1);
                            entry.cell_idx = landed_cell_idx;
                            self.stack.push(entry);
                            self.at_eof = true;
                            self.remember_table_seek(target_rowid, current_page, landed_cell_idx);
                        } else {
                            entry.cell_idx = idx;
                            self.stack.push(entry);
                            self.at_eof = false;
                            self.remember_table_seek(target_rowid, current_page, idx);
                        }
                        self.record_point_witness(
                            cx,
                            WitnessKey::Cell {
                                btree_root: self.root_page,
                                leaf_page: current_page,
                                tag: Self::cell_tag_from_rowid(target_rowid),
                            },
                        );
                        return Ok(SeekResult::NotFound);
                    }
                }
            }

            // Interior table page: binary search to find which child to descend.
            let child_idx = Self::binary_search_table_interior(cx, &entry, target_rowid)?;
            let child = Self::read_interior_child_inline(&entry, child_idx)?;
            let mut entry = entry;
            entry.cell_idx = child_idx;
            self.stack.push(entry);
            self.issue_prefetch_hint(cx, child);
            current_page = child;
        }
    }

    fn try_table_seek_cache(&mut self, cx: &Cx, target_rowid: i64) -> Result<Option<SeekResult>> {
        observe_cursor_cancellation(cx)?;

        for slot_idx in 0..TABLE_SEEK_CACHE_SLOTS {
            let Some(cached) = self.seek_cache[slot_idx] else {
                continue;
            };

            // If the cached landing point was the first cell on this leaf,
            // any smaller rowid must belong to an earlier leaf.
            if target_rowid < cached.rowid && cached.cell_idx == 0 {
                continue;
            }

            // NOTE (bd-kwei8): this fast path deliberately rebuilds the stack
            // as JUST the landing leaf — no root-to-leaf path. That is fine
            // for reads, but every structural mutation entry point must
            // rebuild a full path first, because the balance machinery
            // interprets `stack.len() == 1` as "the leaf IS the root"
            // (`balance_for_delete` skips rebalancing; `balance_for_insert`
            // pushes the true root down). See the rootless-stack guards in
            // `delete` and `table_insert`.
            let reuse_current = self
                .stack
                .last()
                .is_some_and(|entry| entry.page_no == cached.page_no)
                && !self.pager.is_dirty(cached.page_no);
            let mut loaded_entry = if reuse_current {
                // Match `load_page`'s cancellation and visit accounting while
                // borrowing the resident immutable page for the search.
                observe_cursor_cancellation(cx)?;
                self.note_page_visit(cached.page_no);
                None
            } else {
                Some(self.load_page(cx, cached.page_no)?)
            };

            let (result, cell_count) = {
                let entry = loaded_entry
                    .as_ref()
                    .or_else(|| self.stack.last())
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "cached table seek lost resident page {}",
                            cached.page_no.get()
                        ),
                    })?;
                if !(entry.header.page_type.is_leaf() && entry.header.page_type.is_table()) {
                    continue;
                }
                (
                    Self::search_integer_key_table_leaf(cx, entry, target_rowid)?,
                    entry.header.cell_count,
                )
            };
            let landing = match result {
                BinarySearchResult::Found(idx) => Some((idx, SeekResult::Found)),
                BinarySearchResult::NotFound(idx) if idx < cell_count && idx > 0 => {
                    Some((idx, SeekResult::NotFound))
                }
                BinarySearchResult::NotFound(_) => None,
            };
            let Some((idx, seek_result)) = landing else {
                continue;
            };

            let mut entry = loaded_entry
                .take()
                .or_else(|| self.stack.pop())
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "cached table seek lost landing page {}",
                        cached.page_no.get()
                    ),
                })?;
            self.stack.clear();
            entry.cell_idx = idx;
            self.stack.push(entry);
            self.at_eof = false;
            self.remember_table_seek(target_rowid, cached.page_no, idx);
            self.record_point_witness(
                cx,
                WitnessKey::Cell {
                    btree_root: self.root_page,
                    leaf_page: cached.page_no,
                    tag: Self::cell_tag_from_rowid(target_rowid),
                },
            );
            return Ok(Some(seek_result));
        }

        Ok(None)
    }

    fn load_current_table_leaf(&mut self, cx: &Cx) -> Result<Option<StackEntry>> {
        let Some(current) = self.stack.last() else {
            return Ok(None);
        };
        if self.at_eof
            || !current.header.page_type.is_leaf()
            || !current.header.page_type.is_table()
        {
            return Ok(None);
        }
        let current_page = current.page_no;
        let entry = self.load_page(cx, current_page)?;
        if entry.header.cell_count == 0 {
            return Ok(None);
        }
        Ok(Some(entry))
    }

    fn table_leaf_rowid_bounds(entry: &StackEntry) -> Result<Option<(i64, i64)>> {
        if entry.header.cell_count == 0 {
            return Ok(None);
        }
        let first_rowid = Self::table_leaf_rowid_at(entry, 0)?;
        let last_rowid = Self::table_leaf_rowid_at(entry, entry.header.cell_count - 1)?;
        Ok(Some((first_rowid, last_rowid)))
    }

    fn position_on_loaded_table_leaf(
        &mut self,
        cx: &Cx,
        entry: StackEntry,
        target_rowid: i64,
    ) -> Result<SeekResult> {
        let page_no = entry.page_no;
        let search = Self::search_integer_key_table_leaf(cx, &entry, target_rowid)?;
        let mut entry = entry;
        let seek_result = match search {
            BinarySearchResult::Found(idx) => {
                entry.cell_idx = idx;
                SeekResult::Found
            }
            BinarySearchResult::NotFound(idx) => {
                entry.cell_idx = idx.min(entry.header.cell_count.saturating_sub(1));
                SeekResult::NotFound
            }
        };

        *self
            .stack
            .last_mut()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))? = entry;
        self.at_eof = false;
        let positioned_cell_idx = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?
            .cell_idx;
        self.remember_table_seek(target_rowid, page_no, positioned_cell_idx);
        self.record_point_witness(
            cx,
            WitnessKey::Cell {
                btree_root: self.root_page,
                leaf_page: page_no,
                tag: Self::cell_tag_from_rowid(target_rowid),
            },
        );
        Ok(seek_result)
    }

    fn advance_to_next_table_leaf(&mut self, cx: &Cx) -> Result<bool> {
        let Some(top) = self.stack.last().cloned() else {
            return Ok(false);
        };
        if !top.header.page_type.is_leaf() || top.header.cell_count == 0 {
            return Ok(false);
        }

        let current_page = top.page_no;
        self.stack
            .last_mut()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?
            .cell_idx = top.header.cell_count - 1;
        self.at_eof = false;

        let advanced = self.advance_next_impl(cx, false)?;
        Ok(advanced
            && !self.at_eof
            && self
                .current_page()
                .is_some_and(|next_page| next_page != current_page))
    }

    fn table_leaf_rowid_at(entry: &StackEntry, cell_idx: u16) -> Result<i64> {
        let idx = usize::from(cell_idx);
        if idx >= entry.cell_pointers.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf cell index {} out of bounds ({})",
                    cell_idx,
                    entry.cell_pointers.len()
                ),
            });
        }

        let offset = usize::from(entry.cell_pointers[idx]);
        let cell_data = entry.page_data.as_bytes().get(offset..).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf cell pointer {} points past page end (len {})",
                    offset,
                    entry.page_data.as_bytes().len()
                ),
            }
        })?;
        if let Some((_, payload_varint_len)) = read_varint(cell_data) {
            if let Some((rowid, _)) = read_varint(&cell_data[payload_varint_len..]) {
                #[allow(clippy::cast_possible_wrap)]
                let rowid_val = rowid as i64;
                Ok(rowid_val)
            } else {
                Err(FrankenError::DatabaseCorrupt {
                    detail: "table leaf cell has invalid rowid varint".to_owned(),
                })
            }
        } else {
            Err(FrankenError::DatabaseCorrupt {
                detail: "table leaf cell has invalid payload size varint".to_owned(),
            })
        }
    }

    /// Search an integer-keyed table leaf page for a rowid.
    ///
    /// This uses up to three interpolation probes on the current key range,
    /// then falls back to the pure binary search helper over the remaining
    /// window. Index/blob-key pages continue to use binary search only.
    fn search_integer_key_table_leaf(
        cx: &Cx,
        entry: &StackEntry,
        target: i64,
    ) -> Result<BinarySearchResult> {
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(BinarySearchResult::NotFound(0));
        }

        let first_rowid = Self::table_leaf_rowid_at(entry, 0)?;
        match target.cmp(&first_rowid) {
            std::cmp::Ordering::Less => return Ok(BinarySearchResult::NotFound(0)),
            std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(0)),
            std::cmp::Ordering::Greater => {}
        }

        let last_idx = count - 1;
        let last_rowid = Self::table_leaf_rowid_at(entry, last_idx)?;
        match target.cmp(&last_rowid) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(last_idx)),
            std::cmp::Ordering::Greater => return Ok(BinarySearchResult::NotFound(count)),
        }

        // Common append-built table leaves contain dense rowid runs. The first
        // and last keys prove density for a sorted unique rowid page, so compute
        // the exact slot and verify it before paying the interpolation divide.
        if first_rowid.checked_add(i64::from(count) - 1) == Some(last_rowid) {
            let offset = u16::try_from(target - first_rowid)
                .map_err(|_| FrankenError::internal("dense table leaf seek offset overflow"))?;
            if Self::table_leaf_rowid_at(entry, offset)? == target {
                return Ok(BinarySearchResult::Found(offset));
            }
        }

        let mut lo = 0u16;
        let mut hi = count;
        let mut lo_rowid = first_rowid;
        let mut hi_rowid = last_rowid;

        for _ in 0..TABLE_LEAF_INTERPOLATION_MAX_PROBES {
            observe_cursor_cancellation(cx)?;

            let window_len = hi - lo;
            if window_len == 0 {
                return Ok(BinarySearchResult::NotFound(lo));
            }

            let denominator = i128::from(hi_rowid) - i128::from(lo_rowid);
            if denominator <= 0 {
                break;
            }

            // Estimate the probe slot from the rowid's relative position in
            // the current search window, then clamp to a valid cell index.
            let estimate = ((i128::from(target) - i128::from(lo_rowid)) * i128::from(window_len))
                / denominator;
            let probe_offset_i128 = estimate.clamp(0, i128::from(window_len) - 1);
            let probe_offset = u16::try_from(probe_offset_i128)
                .map_err(|_| FrankenError::internal("table leaf interpolation offset overflow"))?;
            let probe_idx = lo
                .checked_add(probe_offset)
                .ok_or_else(|| FrankenError::internal("table leaf interpolation index overflow"))?;
            let probe_rowid = Self::table_leaf_rowid_at(entry, probe_idx)?;

            match probe_rowid.cmp(&target) {
                std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(probe_idx)),
                std::cmp::Ordering::Less => {
                    lo = probe_idx.saturating_add(1);
                    if lo >= hi {
                        return Ok(BinarySearchResult::NotFound(lo));
                    }
                    lo_rowid = Self::table_leaf_rowid_at(entry, lo)?;
                }
                std::cmp::Ordering::Greater => {
                    hi = probe_idx;
                    if lo >= hi {
                        return Ok(BinarySearchResult::NotFound(lo));
                    }
                    hi_rowid = Self::table_leaf_rowid_at(entry, hi - 1)?;
                }
            }
        }

        Self::binary_search_table_leaf_range(cx, entry, target, lo, hi)
    }

    /// Binary search a leaf table page for a rowid.
    #[cfg(test)]
    fn binary_search_table_leaf(
        cx: &Cx,
        entry: &StackEntry,
        target: i64,
    ) -> Result<BinarySearchResult> {
        Self::binary_search_table_leaf_range(cx, entry, target, 0, entry.header.cell_count)
    }

    fn binary_search_table_leaf_range(
        cx: &Cx,
        entry: &StackEntry,
        target: i64,
        mut lo: u16,
        mut hi: u16,
    ) -> Result<BinarySearchResult> {
        while lo < hi {
            observe_cursor_cancellation(cx)?;
            let mid = lo + (hi - lo) / 2;
            let rowid = Self::table_leaf_rowid_at(entry, mid)?;

            match rowid.cmp(&target) {
                std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(BinarySearchResult::NotFound(lo))
    }

    /// Binary search an interior table page to find which child to descend.
    ///
    /// Returns the child index (0..=cell_count). If the target is greater
    /// than all keys, returns cell_count (meaning follow right_child).
    fn binary_search_table_interior(cx: &Cx, entry: &StackEntry, target: i64) -> Result<u16> {
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(0); // Follow right_child.
        }

        // Interior table cells are sorted by rowid. We want the child
        // whose subtree may contain `target`.
        //
        // Cell[i] has left_child and rowid. The left_child subtree contains
        // rowids < cell[i].rowid. If target <= cell[i].rowid, descend into
        // left_child of cell[i]. If target > last cell's rowid, follow right_child.
        let mut lo = 0u16;
        let mut hi = count;
        while lo < hi {
            observe_cursor_cancellation(cx)?;
            let mid = lo + (hi - lo) / 2;
            let rowid = Self::table_interior_rowid_at(entry, mid)?;

            if target <= rowid {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Ok(lo)
    }

    /// Seek to a key in an index B-tree. Returns the seek result.
    fn index_seek(&mut self, cx: &Cx, target_key: &[u8]) -> Result<SeekResult> {
        self.index_seek_with_bias(cx, target_key, IndexSeekBias::LowerBound)
    }

    /// Seek to the first key after the complete equal-prefix run.
    fn index_seek_upper_bound(&mut self, cx: &Cx, target_key: &[u8]) -> Result<SeekResult> {
        self.index_seek_with_bias(cx, target_key, IndexSeekBias::UpperBound)
    }

    fn index_seek_with_bias(
        &mut self,
        cx: &Cx,
        target_key: &[u8],
        bias: IndexSeekBias,
    ) -> Result<SeekResult> {
        let res = self.index_seek_for_insert_with_bias(cx, target_key, bias)?;
        if !res.is_found() && self.at_eof {
            // We fell off the right edge of the leaf.
            // Determine if there is a successor up the tree.
            let mut has_successor = false;
            for parent in self.stack.iter().rev().skip(1) {
                if parent.cell_idx < parent.header.cell_count {
                    has_successor = true;
                    break;
                }
            }

            if has_successor {
                // There is a successor. Reset eof and use advance_next to reach it.
                self.at_eof = false;
                let advanced = self.advance_next(cx)?;
                if !advanced {
                    self.at_eof = true;
                }
            }
        }
        Ok(res)
    }

    /// Internal seek used by INSERT that anchors the cursor on the leaf where
    /// the target belongs, even if it falls off the right edge.
    fn index_seek_for_insert(&mut self, cx: &Cx, target_key: &[u8]) -> Result<SeekResult> {
        self.index_seek_for_insert_with_bias(cx, target_key, IndexSeekBias::LowerBound)
    }

    fn index_seek_for_insert_with_bias(
        &mut self,
        cx: &Cx,
        target_key: &[u8],
        bias: IndexSeekBias,
    ) -> Result<SeekResult> {
        observe_cursor_cancellation(cx)?;
        self.stack.clear();
        let mut current_page = self.root_page;

        loop {
            observe_cursor_cancellation(cx)?;
            if self.stack.len() >= BTREE_MAX_DEPTH as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("B-tree depth exceeds maximum of {}", BTREE_MAX_DEPTH),
                });
            }

            let entry = self.load_page(cx, current_page)?;

            // Guard: detect is_table vs actual page-type mismatch early.
            // If the cursor was opened with is_table=false but the page is
            // actually a table page, catch this with a clear diagnostic.
            if entry.header.page_type.is_table() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "index_seek called on table page (type {:?}, page {}, root {}): \
                         cursor is_table flag likely incorrect",
                        entry.header.page_type, current_page, self.root_page
                    ),
                });
            }

            if entry.header.page_type.is_leaf() {
                let result =
                    self.binary_search_index_leaf_with_bias(cx, &entry, target_key, bias)?;
                match result {
                    BinarySearchResult::Found(idx) => {
                        let mut entry = entry;
                        entry.cell_idx = idx;
                        self.stack.push(entry);
                        self.at_eof = false;
                        self.record_point_witness(
                            cx,
                            WitnessKey::Cell {
                                btree_root: self.root_page,
                                leaf_page: current_page,
                                tag: Self::cell_tag_from_index_key(target_key),
                            },
                        );
                        return Ok(SeekResult::Found);
                    }
                    BinarySearchResult::NotFound(idx) => {
                        let mut entry = entry;
                        if idx >= entry.header.cell_count {
                            // Target is strictly greater than the last key on this leaf.
                            // Keep the path anchored to this right-most leaf and mark EOF,
                            // so callers (notably INSERT) still have a valid stack context.
                            entry.cell_idx = entry.header.cell_count.saturating_sub(1);
                            self.stack.push(entry);
                            self.at_eof = true;
                        } else {
                            entry.cell_idx = idx;
                            self.stack.push(entry);
                            self.at_eof = false;
                        }
                        self.record_point_witness(
                            cx,
                            WitnessKey::Cell {
                                btree_root: self.root_page,
                                leaf_page: current_page,
                                tag: Self::cell_tag_from_index_key(target_key),
                            },
                        );
                        return Ok(SeekResult::NotFound);
                    }
                }
            }

            // Interior index page: binary search to find which child to descend.
            let search_result =
                self.binary_search_index_interior_with_bias(cx, &entry, target_key, bias)?;
            match search_result {
                BinarySearchResult::Found(idx) => {
                    let mut entry = entry;
                    entry.cell_idx = idx;
                    self.stack.push(entry);
                    self.at_eof = false;
                    self.record_point_witness(
                        cx,
                        WitnessKey::Cell {
                            btree_root: self.root_page,
                            leaf_page: current_page,
                            tag: Self::cell_tag_from_index_key(target_key),
                        },
                    );
                    return Ok(SeekResult::Found);
                }
                BinarySearchResult::NotFound(idx) => {
                    let child = Self::read_interior_child_inline(&entry, idx)?;
                    let mut entry = entry;
                    entry.cell_idx = idx;
                    self.stack.push(entry);
                    self.issue_prefetch_hint(cx, child);
                    current_page = child;
                }
            }
        }
    }

    /// Read the full payload for a cell into the provided buffer.
    fn read_cell_payload_into(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        cell: &CellRef,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let local = cell.local_payload(entry.page_data.as_bytes());

        if let Some(first_overflow) = cell.overflow_page {
            overflow::read_overflow_chain_into(
                local,
                first_overflow,
                cell.payload_size,
                self.usable_size,
                &mut |pgno| {
                    self.pager
                        .read_btree_page_data(cx, pgno)
                        .map(PageData::into_vec)
                },
                out,
            )
        } else {
            out.clear();
            instrumentation::record_local_payload_copy(local.len());
            out.extend_from_slice(local);
            Ok(())
        }
    }

    /// Read only a prefix of a cell payload into the provided buffer.
    fn read_cell_payload_prefix_into(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        cell: &CellRef,
        max_prefix_bytes: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let local = cell.local_payload(entry.page_data.as_bytes());
        let target_size = usize::try_from(cell.payload_size)
            .unwrap_or(usize::MAX)
            .min(max_prefix_bytes);
        if target_size == 0 {
            out.clear();
            return Ok(());
        }

        if let Some(first_overflow) = cell.overflow_page {
            overflow::read_overflow_chain_prefix_into(
                local,
                first_overflow,
                cell.payload_size,
                self.usable_size,
                target_size,
                &mut |pgno| {
                    self.pager
                        .read_btree_page_data(cx, pgno)
                        .map(PageData::into_vec)
                },
                out,
            )
        } else {
            out.clear();
            let local_copy_len = local.len().min(target_size);
            instrumentation::record_local_payload_copy(local_copy_len);
            out.extend_from_slice(&local[..local_copy_len]);
            Ok(())
        }
    }

    /// Read the full payload for a cell (resolving overflow if needed).
    fn read_cell_payload<'a>(
        &self,
        cx: &Cx,
        entry: &'a StackEntry,
        cell: &CellRef,
    ) -> Result<Cow<'a, [u8]>> {
        if cell.overflow_page.is_none() {
            return Ok(Cow::Borrowed(
                cell.local_payload(entry.page_data.as_bytes()),
            ));
        }

        instrumentation::record_owned_payload_materialization(
            usize::try_from(cell.payload_size).unwrap_or(usize::MAX),
        );
        let mut payload = Vec::new();
        self.read_cell_payload_into(cx, entry, cell, &mut payload)?;
        Ok(Cow::Owned(payload))
    }

    fn binary_search_index_leaf_with_bias(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        target: &[u8],
        bias: IndexSeekBias,
    ) -> Result<BinarySearchResult> {
        let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::BtreeCursor);
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(BinarySearchResult::NotFound(0));
        }

        let mut lo = 0u16;
        let mut hi = count;
        let parsed_target = parse_record(target);

        while lo < hi {
            observe_cursor_cancellation(cx)?;
            let mid = lo + (hi - lo) / 2;
            let cell = self.parse_cell_at(entry, mid)?;
            let key = self.read_cell_payload(cx, entry, &cell)?;
            let ord = self.compare_index_key_bytes_with_bias(
                key.as_ref(),
                target,
                parsed_target.as_deref(),
                bias,
            );

            match ord {
                std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(BinarySearchResult::NotFound(lo))
    }

    /// Binary search an interior index page to find which child to descend.
    #[cfg(test)]
    fn binary_search_index_interior(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        target: &[u8],
    ) -> Result<BinarySearchResult> {
        self.binary_search_index_interior_with_bias(cx, entry, target, IndexSeekBias::LowerBound)
    }

    fn binary_search_index_interior_with_bias(
        &self,
        cx: &Cx,
        entry: &StackEntry,
        target: &[u8],
        bias: IndexSeekBias,
    ) -> Result<BinarySearchResult> {
        let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::BtreeCursor);
        let count = entry.header.cell_count;
        if count == 0 {
            return Ok(BinarySearchResult::NotFound(0));
        }

        let mut lo = 0u16;
        let mut hi = count;
        let parsed_target = parse_record(target);

        while lo < hi {
            observe_cursor_cancellation(cx)?;
            let mid = lo + (hi - lo) / 2;
            let cell = self.parse_cell_at(entry, mid)?;
            let key = self.read_cell_payload(cx, entry, &cell)?;
            let ord = self.compare_index_key_bytes_with_bias(
                key.as_ref(),
                target,
                parsed_target.as_deref(),
                bias,
            );

            // Note: target vs key comparison direction
            match ord {
                std::cmp::Ordering::Equal => return Ok(BinarySearchResult::Found(mid)),
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Less => lo = mid + 1,
            }
        }
        Ok(BinarySearchResult::NotFound(lo))
    }

    #[cfg(test)]
    fn compare_index_key_bytes(
        &self,
        lhs_bytes: &[u8],
        rhs_bytes: &[u8],
        parsed_rhs: Option<&[fsqlite_types::SqliteValue]>,
    ) -> std::cmp::Ordering {
        self.compare_index_key_bytes_with_bias(
            lhs_bytes,
            rhs_bytes,
            parsed_rhs,
            IndexSeekBias::LowerBound,
        )
    }

    fn compare_index_key_bytes_with_bias(
        &self,
        lhs_bytes: &[u8],
        rhs_bytes: &[u8],
        parsed_rhs: Option<&[fsqlite_types::SqliteValue]>,
        bias: IndexSeekBias,
    ) -> std::cmp::Ordering {
        let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::BtreeCursor);
        match (parse_record(lhs_bytes), parsed_rhs) {
            (Some(lhs_vals), Some(rhs_vals)) => self
                .compare_index_key_values(&lhs_vals, rhs_vals, bias)
                .unwrap_or_else(|| bias.adjust_byte_order(lhs_bytes.cmp(rhs_bytes))),
            _ => bias.adjust_byte_order(lhs_bytes.cmp(rhs_bytes)),
        }
    }

    fn compare_index_key_values(
        &self,
        lhs: &[fsqlite_types::SqliteValue],
        rhs: &[fsqlite_types::SqliteValue],
        bias: IndexSeekBias,
    ) -> Option<std::cmp::Ordering> {
        let registry_guard = self
            .collation_registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let shared_len = lhs.len().min(rhs.len());
        for idx in 0..shared_len {
            let coll_name = self
                .index_collations
                .get(idx)
                .and_then(|coll| coll.as_deref());
            let mut ord =
                Self::cmp_index_values_collated(&lhs[idx], &rhs[idx], coll_name, &registry_guard)?;
            if self.index_desc_flags.get(idx).copied().unwrap_or(false) {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return Some(ord);
            }
        }
        if bias == IndexSeekBias::UpperBound && rhs.len() <= lhs.len() {
            Some(std::cmp::Ordering::Less)
        } else {
            Some(lhs.len().cmp(&rhs.len()))
        }
    }

    fn cmp_index_values_collated(
        lhs: &SqliteValue,
        rhs: &SqliteValue,
        coll_name: Option<&str>,
        registry: &CollationRegistry,
    ) -> Option<std::cmp::Ordering> {
        if let (Some(coll_name), SqliteValue::Text(left), SqliteValue::Text(right)) =
            (coll_name, lhs, rhs)
        {
            return Some(Self::compare_text_with_collation(
                left.as_bytes(),
                right.as_bytes(),
                coll_name,
                registry,
            ));
        }
        lhs.partial_cmp(rhs)
    }

    fn compare_text_with_collation(
        left: &[u8],
        right: &[u8],
        coll_name: &str,
        registry: &CollationRegistry,
    ) -> std::cmp::Ordering {
        registry
            .find(coll_name)
            .map(|collation| collation.compare(left, right))
            .unwrap_or_else(|| left.cmp(right))
    }

    /// Advance to the next entry. Returns false if at EOF.
    ///
    /// Forward-progress fix (frankensqlite#95): previously, when an inner
    /// `move_to_leftmost_leaf` returned `false` (subtree was exhausted, e.g.
    /// because of an empty leaf bubbling up to EOF), the recovery branches
    /// restored a `resume_stack`, cleared `at_eof`, and recursively
    /// re-entered `advance_next_impl`. That recursion lacked a hard
    /// forward-progress guarantee: on multi-level table B-trees the cursor
    /// could effectively re-descend the same subtree indefinitely, with
    /// `rchar` climbing while `read_bytes` stayed at zero (page cache
    /// serving the same pages over and over).
    ///
    /// The fix is two-fold:
    ///   1. Replace the mutual recursion with an iterative inner loop in
    ///      both the leaf-exhausted recovery (table-only) and the interior
    ///      branch (index-only). The loop maintains the invariant that
    ///      every iteration either returns a row, pops a stack frame, or
    ///      strictly advances `cell_idx` on the current stack top.
    ///   2. Add a `debug_assert!`-gated iteration ceiling
    ///      (`BTREE_MAX_DEPTH * 8`). Any cursor that fails to make forward
    ///      progress is caught loudly in tests; in release builds we
    ///      degrade safely by setting `at_eof = true` and returning false.
    ///
    /// The empty-stack re-seek from `root_page` is preserved at the top of
    /// the function for legitimate "before-first" recovery (after `prev()`
    /// falls off the start, `next()` re-positions at row 1), but it is
    /// never re-entered from within the loop body.
    fn advance_next_impl(&mut self, cx: &Cx, record_leaf_witness: bool) -> Result<bool> {
        observe_cursor_cancellation(cx)?;

        if self.at_eof {
            return Ok(false);
        }
        if self.stack.is_empty() {
            // SQLite-style before-first recovery: `prev()` past the first
            // row leaves the stack empty with `at_eof = false`. The next
            // `next()` call must re-position at row 1.
            return self.move_to_leftmost_leaf(cx, self.root_page, record_leaf_witness);
        }

        let depth = self.stack.len();
        let (is_leaf, cell_idx, cell_count) = {
            let top = &self.stack[depth - 1];
            (
                top.header.page_type.is_leaf(),
                top.cell_idx,
                top.header.cell_count,
            )
        };

        // Forward-progress safety net: bound the number of recovery loop
        // iterations. The bound is generous so the legitimate cases
        // (descend → leaf → ascend, possibly across multiple empty
        // subtrees) have plenty of headroom.
        let max_iterations = (BTREE_MAX_DEPTH as usize).saturating_mul(8).max(128);

        // On a leaf page: try to advance to the next cell.
        if is_leaf {
            let next_idx = cell_idx + 1;
            if next_idx < cell_count {
                self.stack[depth - 1].cell_idx = next_idx;
                return Ok(true);
            }

            // Past the last cell on this leaf. Pop up until we find an
            // interior page with more children to visit. This is an
            // iterative recovery — the previous implementation recursed
            // back into `advance_next_impl` when `move_to_leftmost_leaf`
            // returned `false`, which lacked a forward-progress invariant
            // and could re-enter the empty-stack re-seek from `root_page`.
            self.stack.pop();
            for _ in 0..max_iterations {
                observe_cursor_cancellation(cx)?;
                if self.stack.is_empty() {
                    self.at_eof = true;
                    return Ok(false);
                }
                let depth = self.stack.len();
                let parent_cell_idx = self.stack[depth - 1].cell_idx;
                let parent_cell_count = self.stack[depth - 1].header.cell_count;

                if parent_cell_idx < parent_cell_count {
                    // For index B-trees, the separator key in the parent
                    // is the next logical entry. Return it; the next call
                    // will enter the interior branch below and descend
                    // into the right subtree.
                    if !self.is_table {
                        return Ok(true);
                    }

                    let next_child_idx = parent_cell_idx + 1;
                    let child = {
                        let parent = &self.stack[depth - 1];
                        Self::child_page_at(parent, next_child_idx)?
                    };

                    self.stack[depth - 1].cell_idx = next_child_idx;
                    // Snapshot the parent stack so we can restore it if
                    // `move_to_leftmost_leaf` exhausts to EOF inside an
                    // empty subtree (the recursion may pop everything,
                    // including this parent frame).
                    let resume_stack = self.stack.clone();
                    self.issue_prefetch_hint(cx, child);
                    let found = self.move_to_leftmost_leaf(cx, child, record_leaf_witness)?;
                    if found {
                        return Ok(true);
                    }
                    // Subtree at `child` was empty/exhausted. Restore the
                    // stack to "parent advanced past this child" so the
                    // loop's next iteration makes strict forward progress
                    // (either advancing to the next sibling or popping
                    // this interior page once cell_idx == cell_count).
                    self.at_eof = false;
                    self.stack = resume_stack;
                    continue;
                }
                // cell_idx == cell_count means we already visited
                // right_child. Pop and continue upward.
                self.stack.pop();
            }

            // Forward-progress invariant violation: the recovery loop
            // exceeded `max_iterations` without returning a row or finding
            // EOF. This is the signature of frankensqlite#95.
            debug_assert!(
                false,
                "BtCursor::advance_next_impl (leaf-recovery) exceeded {} iterations \
                 without forward progress (frankensqlite#95); root_page = {}, \
                 stack_depth = {}, at_eof = {}",
                max_iterations,
                self.root_page.get(),
                self.stack.len(),
                self.at_eof,
            );
            self.at_eof = true;
            return Ok(false);
        }

        // On an interior page (only happens for index B-trees).
        // The current position is the separator cell itself.
        // The next logical entry is the leftmost descendant of the right
        // subtree. Iterative loop replaces the recursive recovery that
        // could spin on empty subtrees.
        let mut cell_idx = cell_idx;
        let mut cell_count = cell_count;
        for _ in 0..max_iterations {
            observe_cursor_cancellation(cx)?;
            if cell_idx >= cell_count {
                self.stack.pop();
                if self.stack.is_empty() {
                    self.at_eof = true;
                    return Ok(false);
                }
                let depth = self.stack.len();
                let top = &self.stack[depth - 1];
                if top.header.page_type.is_leaf() {
                    // Defensive: an interior-page ascent should land on
                    // another interior page, but if state is anomalous,
                    // recurse via `advance_next_impl` to re-enter the
                    // leaf-exhausted recovery. This is bounded by the
                    // tree depth, not by data size, so it cannot spin.
                    self.at_eof = false;
                    return self.advance_next_impl(cx, record_leaf_witness);
                }
                cell_idx = top.cell_idx;
                cell_count = top.header.cell_count;
                continue;
            }
            let next_child_idx = cell_idx + 1;
            let depth = self.stack.len();
            let child = {
                let top = &self.stack[depth - 1];
                Self::child_page_at(top, next_child_idx)?
            };
            self.stack
                .last_mut()
                .ok_or_else(|| FrankenError::internal("cursor stack empty"))?
                .cell_idx = next_child_idx;
            let resume_stack = self.stack.clone();
            self.issue_prefetch_hint(cx, child);
            let found = self.move_to_leftmost_leaf(cx, child, false)?;
            if found {
                return Ok(true);
            }
            self.at_eof = false;
            self.stack = resume_stack;
            cell_idx = next_child_idx;
            cell_count = self.stack[self.stack.len() - 1].header.cell_count;
        }

        debug_assert!(
            false,
            "BtCursor::advance_next_impl (interior) exceeded {} iterations without \
             forward progress (frankensqlite#95); root_page = {}, stack_depth = {}, \
             at_eof = {}",
            max_iterations,
            self.root_page.get(),
            self.stack.len(),
            self.at_eof,
        );
        self.at_eof = true;
        Ok(false)
    }

    fn advance_next(&mut self, cx: &Cx) -> Result<bool> {
        self.advance_next_impl(cx, true)
    }

    /// Move to the previous entry. Returns false if at the beginning.
    ///
    /// Forward-progress fix (frankensqlite#95, symmetric to `advance_next_impl`):
    /// the previous implementation recursed back into `advance_prev` whenever
    /// `move_to_rightmost_leaf` returned `false` (subtree exhausted via an
    /// empty leaf bubbling up). Unlike `advance_next_impl` the recursion did
    /// not even snapshot a `resume_stack`, so the cursor's stack was left in
    /// whatever state the recursive `move_to_rightmost_leaf` → `advance_prev`
    /// chain produced. While the canonical exhaustion path appears to
    /// terminate (each inner descent strictly decrements `parent.cell_idx`),
    /// the recursive pattern is exactly the one the forward-scan fix removed,
    /// and any future change that leaves the cursor in `(stack_empty,
    /// at_eof=false)` would trigger the rightmost re-seek from `root_page`
    /// and replay rows already returned. Replace the recursion with an
    /// iterative loop bounded by `BTREE_MAX_DEPTH * 8` that maintains the
    /// invariant that every iteration either returns a row, pops a stack
    /// frame, or strictly decrements `parent.cell_idx` on the current stack
    /// top.
    fn advance_prev(&mut self, cx: &Cx) -> Result<bool> {
        observe_cursor_cancellation(cx)?;

        if self.at_eof {
            // Recover from an after-last state (e.g., next() from last row).
            if self.stack.is_empty() {
                return self.move_to_rightmost_leaf(cx, self.root_page, true);
            }

            // Recover from a seek-past-end EOF sentinel while preserving leaf context.
            let (is_leaf, cell_count) = {
                let top = self
                    .stack
                    .last()
                    .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
                (top.header.page_type.is_leaf(), top.header.cell_count)
            };
            if is_leaf && cell_count > 0 {
                self.stack
                    .last_mut()
                    .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?
                    .cell_idx = cell_count - 1;
                self.at_eof = false;
                return Ok(true);
            }

            // Fallback to canonical rightmost positioning for any odd interior/empty state.
            return self.move_to_rightmost_leaf(cx, self.root_page, true);
        }

        if self.stack.is_empty() {
            return Ok(false);
        }

        let depth = self.stack.len();
        let (is_leaf, cell_idx) = {
            let top = &self.stack[depth - 1];
            (top.header.page_type.is_leaf(), top.cell_idx)
        };

        // Forward-progress safety net: bound the number of recovery loop
        // iterations. Mirrors `advance_next_impl`'s ceiling.
        let max_iterations = (BTREE_MAX_DEPTH as usize).saturating_mul(8).max(128);

        if is_leaf {
            if cell_idx > 0 {
                self.stack[depth - 1].cell_idx -= 1;
                self.at_eof = false;
                return Ok(true);
            }

            // Before the first cell on this leaf. Pop up until we find an
            // interior page with an earlier sibling to descend into. This
            // is an iterative recovery — the previous implementation
            // recursed back into `advance_prev` when `move_to_rightmost_leaf`
            // returned `false`, which lacked a forward-progress invariant.
            self.stack.pop();
            for _ in 0..max_iterations {
                observe_cursor_cancellation(cx)?;
                if self.stack.is_empty() {
                    // At the very beginning of the tree.
                    return Ok(false);
                }
                let depth = self.stack.len();
                let parent_cell_idx = self.stack[depth - 1].cell_idx;

                if parent_cell_idx > 0 {
                    // In index B-trees, moving backward from a child should
                    // land on the separator key immediately to its left.
                    // Return without descending; the caller's next prev()
                    // call will enter the interior branch below.
                    if !self.is_table {
                        self.stack[depth - 1].cell_idx -= 1;
                        self.at_eof = false;
                        return Ok(true);
                    }

                    let prev_child_idx = parent_cell_idx - 1;
                    let child = {
                        let parent = &self.stack[depth - 1];
                        Self::child_page_at(parent, prev_child_idx)?
                    };

                    self.stack[depth - 1].cell_idx = prev_child_idx;
                    // Snapshot the parent stack so we can restore it if
                    // `move_to_rightmost_leaf` exhausts to EOF inside an
                    // empty subtree (the recursion may pop everything,
                    // including this parent frame).
                    let resume_stack = self.stack.clone();
                    self.issue_prefetch_hint(cx, child);
                    let found = self.move_to_rightmost_leaf(cx, child, true)?;
                    if found {
                        return Ok(true);
                    }
                    // Subtree at `child` was empty/exhausted. Restore the
                    // stack to "parent advanced past this child" so the
                    // loop's next iteration makes strict reverse progress
                    // (either descending into the next earlier sibling or
                    // popping this interior page once cell_idx == 0).
                    self.at_eof = false;
                    self.stack = resume_stack;
                    continue;
                }
                // cell_idx == 0 means we came from the leftmost child.
                self.stack.pop();
            }

            // Reverse-progress invariant violation: the recovery loop
            // exceeded `max_iterations` without returning a row or finding
            // the start of the tree. This is the signature of
            // frankensqlite#95 in the reverse direction.
            debug_assert!(
                false,
                "BtCursor::advance_prev (leaf-recovery) exceeded {} iterations \
                 without reverse progress (frankensqlite#95); root_page = {}, \
                 stack_depth = {}, at_eof = {}",
                max_iterations,
                self.root_page.get(),
                self.stack.len(),
                self.at_eof,
            );
            self.at_eof = true;
            return Ok(false);
        }

        // On an interior page (only happens for index B-trees).
        // The current position is the separator cell itself.
        // The previous logical entry is the rightmost descendant of the
        // left subtree.
        //
        // Reverse-order semantics for an index B-tree interior with cells
        // [c0, c1, c2] and children [l0, l1, l2, l3] reading right-to-left:
        //   rightmost(l3), c2, rightmost(l2), c1, rightmost(l1), c0,
        //   rightmost(l0), <ascend to ancestor>.
        // When on separator `cell_idx`, prev = rightmost(l[cell_idx]); if
        // that subtree is empty AND cell_idx > 0, prev = separator at
        // cell_idx-1 itself (a logical entry to return); if cell_idx == 0
        // we must ascend to the parent.
        //
        // Forward-progress fix (frankensqlite#95, reverse direction):
        // replaced the original `return self.advance_prev(cx)` recursive
        // recovery (which relied on `move_to_rightmost_leaf`'s internal
        // recursion to bubble exhaustion up through the entire ancestor
        // chain in a single call) with a single explicit step. After a
        // failed descent into the left subtree, the prev row is either
        // the separator at cell_idx-1 (in-place repositioning) or a
        // bounded-depth ascent via one recursive `advance_prev` call.
        let child = {
            let top = &self.stack[depth - 1];
            Self::child_page_at(top, cell_idx)?
        };
        // Snapshot the parent stack so we can restore it if the rightmost
        // descent into this child subtree finds nothing.
        let resume_stack = self.stack.clone();
        self.issue_prefetch_hint(cx, child);
        let found = self.move_to_rightmost_leaf(cx, child, false)?;
        if found {
            return Ok(true);
        }
        // Subtree empty/exhausted. Restore parent stack.
        self.at_eof = false;
        self.stack = resume_stack;
        if cell_idx > 0 {
            // The separator at cell_idx-1 IS the prev logical entry.
            // Reposition the cursor on that separator and return.
            self.stack[depth - 1].cell_idx = cell_idx - 1;
            return Ok(true);
        }
        // cell_idx == 0: no earlier separator on this page. Pop and
        // ascend, looking for the next-earlier separator in an ancestor.
        //
        // frankensqlite#95 secondary defect: the previous implementation
        // recursed into `self.advance_prev(cx)` here. That recursion
        // re-entered the interior branch at the parent with the parent's
        // `cell_idx` still pointing at the *slot we just popped from*,
        // causing `child_page_at(parent, cell_idx)` to re-descend into
        // the subtree we just exhausted via `move_to_rightmost_leaf`
        // (which would happily return its rightmost leaf — a row that
        // had already been returned earlier in the reverse scan,
        // producing an infinite/looping replay). Caught by
        // `test_advance_prev_does_not_replay_after_interior_pop_recurse_frankensqlite_95`.
        //
        // The fix mirrors the iterative leaf-recovery loop: each iteration
        // either returns a separator (strict reverse progress), pops a
        // frame (strict stack shrink), or terminates with `Ok(false)`.
        self.stack.pop();
        for _ in 0..max_iterations {
            observe_cursor_cancellation(cx)?;
            if self.stack.is_empty() {
                return Ok(false);
            }
            let depth = self.stack.len();
            let parent_cell_idx = self.stack[depth - 1].cell_idx;
            if parent_cell_idx > 0 {
                // Reverse step into the previous separator on this
                // ancestor. The interior branch is only entered for
                // index B-trees; tables never reach this code path.
                self.stack[depth - 1].cell_idx -= 1;
                self.at_eof = false;
                return Ok(true);
            }
            // parent_cell_idx == 0: we already came from this ancestor's
            // leftmost child. Pop and keep ascending.
            self.stack.pop();
        }

        // Reverse-progress invariant violation: the interior-pop ascent
        // exceeded `max_iterations` without finding an earlier separator
        // or the start of the tree.
        debug_assert!(
            false,
            "BtCursor::advance_prev (interior-pop ascent) exceeded {} iterations \
             without reverse progress (frankensqlite#95); root_page = {}, \
             stack_depth = {}, at_eof = {}",
            max_iterations,
            self.root_page.get(),
            self.stack.len(),
            self.at_eof,
        );
        self.at_eof = true;
        Ok(false)
    }
}

#[allow(clippy::cast_possible_wrap)]
fn decode_big_endian_signed_fast(bytes: &[u8]) -> i64 {
    match bytes.len() {
        0 => 0,
        1 => bytes[0] as i8 as i64,
        2 => {
            let mut buf = [0_u8; 2];
            buf.copy_from_slice(bytes);
            i16::from_be_bytes(buf) as i64
        }
        3 => {
            let mut buf = [if bytes[0] & 0x80 != 0 { 0xFF } else { 0 }; 4];
            buf[1..4].copy_from_slice(bytes);
            i32::from_be_bytes(buf) as i64
        }
        4 => {
            let mut buf = [0_u8; 4];
            buf.copy_from_slice(bytes);
            i32::from_be_bytes(buf) as i64
        }
        6 => {
            let mut buf = [if bytes[0] & 0x80 != 0 { 0xFF } else { 0 }; 8];
            buf[2..8].copy_from_slice(bytes);
            i64::from_be_bytes(buf)
        }
        8 => {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(bytes);
            i64::from_be_bytes(buf)
        }
        _ => {
            let negative = bytes.first().is_some_and(|&b| b & 0x80 != 0);
            let mut value: u64 = if negative { u64::MAX } else { 0 };
            for &b in bytes {
                value = (value << 8) | u64::from(b);
            }
            value as i64
        }
    }
}

/// Result of a search within a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinarySearchResult {
    /// Exact match found at this cell index.
    Found(u16),
    /// No match; the target would be inserted at this position.
    NotFound(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexSeekBias {
    LowerBound,
    UpperBound,
}

impl IndexSeekBias {
    fn adjust_byte_order(self, ordering: std::cmp::Ordering) -> std::cmp::Ordering {
        if self == Self::UpperBound && ordering == std::cmp::Ordering::Equal {
            std::cmp::Ordering::Less
        } else {
            ordering
        }
    }
}

impl<P: PageWriter> BtCursor<P> {
    /// Recursively free all pages in the B-tree rooted at `page_no`,
    /// including `page_no` itself, any overflow chains attached to cells,
    /// and all child subtrees. Used by DROP TABLE / DROP INDEX to return
    /// every page of the dropped object to the pager freelist.
    pub fn free_subtree_pages(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
        let page_data = self.pager.read_btree_page_data(cx, page_no)?;
        let header = cell::parse_page_header(page_data.as_bytes(), page_no)?;
        let header_offset = cell::header_offset_for_page(page_no);
        let ptrs = cell::read_cell_pointers(page_data.as_bytes(), &header, header_offset)?;

        // Free overflow chains on ALL cells (both interior and leaf).
        // Interior cells of index B-trees carry key payloads that can overflow;
        // table B-tree interior cells only carry rowids (no overflow possible)
        // but checking is harmless since overflow_page will be None.
        for ptr in &ptrs {
            let cell = CellRef::parse(
                page_data.as_bytes(),
                usize::from(*ptr),
                header.page_type,
                self.usable_size,
            )?;
            if let Some(first_overflow) = cell.overflow_page {
                self.free_overflow_chain(cx, first_overflow)?;
            }
        }

        // For interior pages, recursively free child subtrees.
        if header.page_type.is_interior() {
            for ptr in &ptrs {
                let cell = CellRef::parse(
                    page_data.as_bytes(),
                    usize::from(*ptr),
                    header.page_type,
                    self.usable_size,
                )?;
                let left_child = cell
                    .left_child
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior page {} cell is missing left child",
                            page_no.get()
                        ),
                    })?;
                self.free_subtree_pages(cx, left_child)?;
            }

            let right_child = header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("interior page {} is missing right child", page_no.get()),
                })?;
            self.free_subtree_pages(cx, right_child)?;
        }

        self.pager.free_page(cx, page_no)
    }

    fn seed_empty_root_leaf_cursor(&mut self, cx: &Cx) -> Result<bool> {
        self.collapse_empty_table_root_if_needed(cx)?;

        let mut root_entry = self.load_page(cx, self.root_page)?;
        if !root_entry.header.page_type.is_leaf() || root_entry.header.cell_count != 0 {
            return Ok(false);
        }

        root_entry.cell_idx = 0;
        self.stack.clear();
        self.stack.push(root_entry);
        self.at_eof = true;
        Ok(true)
    }

    fn collapse_empty_table_root_if_needed(&mut self, cx: &Cx) -> Result<()> {
        if !self.is_table {
            return Ok(());
        }

        let root_entry = self.reload_page_fresh(cx, self.root_page)?;
        if root_entry.header.page_type.is_leaf() || self.count_all_rows_iterative(cx)? != 0 {
            return Ok(());
        }

        for &ptr in &root_entry.cell_pointers {
            let cell = CellRef::parse(
                root_entry.page_data.as_bytes(),
                usize::from(ptr),
                root_entry.header.page_type,
                self.usable_size,
            )?;
            let left_child = cell
                .left_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "empty interior root {} has divider without left child",
                        self.root_page.get()
                    ),
                })?;
            self.free_subtree_pages(cx, left_child)?;
        }
        let right_child =
            root_entry
                .header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "empty interior root {} is missing right child",
                        self.root_page.get()
                    ),
                })?;
        self.free_subtree_pages(cx, right_child)?;

        let header_offset = cell::header_offset_for_page(self.root_page);
        let mut page = vec![0u8; self.page_size as usize];
        if header_offset > 0 {
            page[..header_offset]
                .copy_from_slice(&root_entry.page_data.as_bytes()[..header_offset]);
        }

        let header = BtreePageHeader {
            page_type: cell::BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_offset: self.usable_size,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        header.write(&mut page, header_offset);
        self.pager
            .write_page_data(cx, self.root_page, PageData::from_vec(page))?;
        self.stack.clear();
        self.at_eof = true;
        Ok(())
    }

    fn varint_len(value: u64) -> usize {
        let mut buf = [0u8; 9];
        write_varint(&mut buf, value)
    }

    fn table_leaf_cell_len(rowid: i64, payload: &[u8]) -> Result<usize> {
        let payload_len = u64::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(Self::varint_len(payload_len)
            + Self::varint_len(u64::from_ne_bytes(rowid.to_ne_bytes()))
            + payload.len())
    }

    fn table_interior_cell_len(rowid: i64) -> usize {
        4 + Self::varint_len(u64::from_ne_bytes(rowid.to_ne_bytes()))
    }

    fn bulk_page_can_append_cell(
        header_offset: usize,
        page_type: BtreePageType,
        cell_count: usize,
        content_offset: usize,
        cell_len: usize,
    ) -> bool {
        let Some(new_content_offset) = content_offset.checked_sub(cell_len) else {
            return false;
        };
        let ptr_array_end = header_offset
            + usize::from(page_type.header_size())
            + (cell_count + 1) * usize::from(fsqlite_types::limits::CELL_POINTER_SIZE);
        ptr_array_end <= new_content_offset
    }

    fn bulk_table_leaf_groups<R: AsRef<[u8]>>(
        &self,
        records: &[(i64, R)],
        header_offset: usize,
    ) -> Result<Option<Vec<BulkTableGroup>>> {
        let mut groups = Vec::new();
        let mut group_start = 0usize;
        let mut group_cell_count = 0usize;
        let mut content_offset = self.usable_size as usize;

        for (idx, (rowid, payload)) in records.iter().enumerate() {
            let payload = payload.as_ref();
            let payload_size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
            if cell::has_overflow(payload_size, self.usable_size, BtreePageType::LeafTable) {
                return Ok(None);
            }

            let cell_len = Self::table_leaf_cell_len(*rowid, payload)?;
            if group_cell_count > 0
                && !Self::bulk_page_can_append_cell(
                    header_offset,
                    BtreePageType::LeafTable,
                    group_cell_count,
                    content_offset,
                    cell_len,
                )
            {
                groups.push(BulkTableGroup {
                    start: group_start,
                    end: idx,
                });
                group_start = idx;
                group_cell_count = 0;
                content_offset = self.usable_size as usize;
            }

            if !Self::bulk_page_can_append_cell(
                header_offset,
                BtreePageType::LeafTable,
                group_cell_count,
                content_offset,
                cell_len,
            ) {
                return Ok(None);
            }
            content_offset -= cell_len;
            group_cell_count += 1;
        }

        if group_start < records.len() {
            groups.push(BulkTableGroup {
                start: group_start,
                end: records.len(),
            });
        }
        Ok(Some(groups))
    }

    fn bulk_table_interior_groups(
        &self,
        children: &[BulkTableChild],
        header_offset: usize,
    ) -> Option<Vec<BulkTableGroup>> {
        if children.is_empty() {
            return Some(Vec::new());
        }

        let mut groups = Vec::new();
        let mut group_start = 0usize;
        let mut cell_count = 0usize;
        let mut content_offset = self.usable_size as usize;

        for child_idx in 1..children.len() {
            let divider_len = Self::table_interior_cell_len(children[child_idx - 1].max_rowid);
            if cell_count > 0
                && !Self::bulk_page_can_append_cell(
                    header_offset,
                    BtreePageType::InteriorTable,
                    cell_count,
                    content_offset,
                    divider_len,
                )
            {
                groups.push(BulkTableGroup {
                    start: group_start,
                    end: child_idx,
                });
                group_start = child_idx;
                cell_count = 0;
                content_offset = self.usable_size as usize;
                continue;
            }

            if !Self::bulk_page_can_append_cell(
                header_offset,
                BtreePageType::InteriorTable,
                cell_count,
                content_offset,
                divider_len,
            ) {
                return None;
            }
            content_offset -= divider_len;
            cell_count += 1;
        }

        groups.push(BulkTableGroup {
            start: group_start,
            end: children.len(),
        });
        if groups.len() > 1
            && let Some(last) = groups.last()
            && last.end == last.start + 1
        {
            let prev_idx = groups.len() - 2;
            if groups[prev_idx].end <= groups[prev_idx].start + 2 {
                return None;
            }
            groups[prev_idx].end -= 1;
            let last_idx = groups.len() - 1;
            groups[last_idx].start -= 1;
        }
        Some(groups)
    }

    fn build_bulk_table_leaf_page<R: AsRef<[u8]>>(
        &self,
        page_no: PageNumber,
        prefix: Option<&[u8]>,
        records: &[(i64, R)],
    ) -> Result<PageData> {
        let header_offset = cell::header_offset_for_page(page_no);
        let mut page = vec![0u8; self.page_size as usize];
        if let Some(prefix) = prefix {
            page[..header_offset].copy_from_slice(prefix);
        }

        let mut content_offset = self.usable_size as usize;
        let mut cell_offsets: SmallVec<[u16; 256]> = SmallVec::with_capacity(records.len());
        let mut payload_varint = [0u8; 9];
        let mut rowid_varint = [0u8; 9];

        for (rowid, payload) in records {
            let payload = payload.as_ref();
            let payload_len = write_varint(
                &mut payload_varint,
                u64::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?,
            );
            let rowid_len =
                write_varint(&mut rowid_varint, u64::from_ne_bytes(rowid.to_ne_bytes()));
            let cell_len = payload_len + rowid_len + payload.len();
            content_offset = content_offset
                .checked_sub(cell_len)
                .ok_or(FrankenError::TooBig)?;
            let cell_offset =
                u16::try_from(content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "bulk leaf cell offset {} exceeds u16 range on page {}",
                        content_offset,
                        page_no.get()
                    ),
                })?;

            let mut write_offset = content_offset;
            page[write_offset..write_offset + payload_len]
                .copy_from_slice(&payload_varint[..payload_len]);
            write_offset += payload_len;
            page[write_offset..write_offset + rowid_len]
                .copy_from_slice(&rowid_varint[..rowid_len]);
            write_offset += rowid_len;
            page[write_offset..write_offset + payload.len()].copy_from_slice(payload);
            cell_offsets.push(cell_offset);
        }

        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: u16::try_from(records.len()).map_err(|_| FrankenError::TooBig)?,
            cell_content_offset: u32::try_from(content_offset).map_err(|_| FrankenError::TooBig)?,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        header.write(&mut page, header_offset);
        cell::write_cell_pointers(&mut page, header_offset, &header, &cell_offsets);
        Ok(PageData::from_vec(page))
    }

    fn build_bulk_table_interior_page(
        &self,
        page_no: PageNumber,
        prefix: Option<&[u8]>,
        children: &[BulkTableChild],
    ) -> Result<PageData> {
        let header_offset = cell::header_offset_for_page(page_no);
        let mut page = vec![0u8; self.page_size as usize];
        if let Some(prefix) = prefix {
            page[..header_offset].copy_from_slice(prefix);
        }

        let right_child = children.last().map(|child| child.page_no).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: format!(
                    "bulk interior page {} cannot be built without children",
                    page_no.get()
                ),
            }
        })?;
        let mut content_offset = self.usable_size as usize;
        let mut cell_offsets: SmallVec<[u16; 256]> =
            SmallVec::with_capacity(children.len().saturating_sub(1));
        let mut rowid_varint = [0u8; 9];

        for child in &children[..children.len() - 1] {
            let rowid_len = write_varint(
                &mut rowid_varint,
                u64::from_ne_bytes(child.max_rowid.to_ne_bytes()),
            );
            let cell_len = 4 + rowid_len;
            content_offset = content_offset
                .checked_sub(cell_len)
                .ok_or(FrankenError::TooBig)?;
            let cell_offset =
                u16::try_from(content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "bulk interior cell offset {} exceeds u16 range on page {}",
                        content_offset,
                        page_no.get()
                    ),
                })?;
            page[content_offset..content_offset + 4]
                .copy_from_slice(&child.page_no.get().to_be_bytes());
            page[content_offset + 4..content_offset + cell_len]
                .copy_from_slice(&rowid_varint[..rowid_len]);
            cell_offsets.push(cell_offset);
        }

        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: u16::try_from(children.len().saturating_sub(1))
                .map_err(|_| FrankenError::TooBig)?,
            cell_content_offset: u32::try_from(content_offset).map_err(|_| FrankenError::TooBig)?,
            fragmented_free_bytes: 0,
            right_child: Some(right_child),
        };
        header.write(&mut page, header_offset);
        cell::write_cell_pointers(&mut page, header_offset, &header, &cell_offsets);
        Ok(PageData::from_vec(page))
    }

    /// Bulk-build an empty table B-tree from strictly increasing rowid records.
    ///
    /// This is a narrow Bε-tree-style batching primitive for monotonic append
    /// runs: all rows are laid out into leaf pages and parent divider pages
    /// once, avoiding one cursor descent / quick-balance cycle per row. Shapes
    /// that need overflow pages, duplicate rowids, non-table trees, or a
    /// non-empty root return `Ok(false)` so callers can replay through the
    /// normal insertion path.
    pub fn table_bulk_load_empty_root_sorted_records<R: AsRef<[u8]>>(
        &mut self,
        cx: &Cx,
        records: &[(i64, R)],
    ) -> Result<bool> {
        if !self.is_table || records.is_empty() {
            return Ok(false);
        }
        if records.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Ok(false);
        }

        let root_data = self.pager.read_btree_page_data(cx, self.root_page)?;
        let root_header = cell::parse_page_header(root_data.as_bytes(), self.root_page)?;
        if root_header.page_type != BtreePageType::LeafTable || root_header.cell_count != 0 {
            return Ok(false);
        }

        let root_header_offset = cell::header_offset_for_page(self.root_page);
        let root_grouping_start = instrumentation::profile_start();
        let Some(root_leaf_groups) = self.bulk_table_leaf_groups(records, root_header_offset)?
        else {
            instrumentation::record_bulk_table_grouping(root_grouping_start);
            return Ok(false);
        };
        instrumentation::record_bulk_table_grouping(root_grouping_start);
        let root_prefix = root_data.as_bytes().get(..root_header_offset);
        if root_leaf_groups.len() == 1 {
            let leaf_build_start = instrumentation::profile_start();
            let root_page =
                self.build_bulk_table_leaf_page(self.root_page, root_prefix, records)?;
            instrumentation::record_bulk_table_leaf_page_build(leaf_build_start);
            let leaf_write_start = instrumentation::profile_start();
            self.pager.write_page_data(cx, self.root_page, root_page)?;
            instrumentation::record_bulk_table_leaf_page_write(leaf_write_start);
            self.stack.clear();
            self.at_eof = true;
            self.last_insert_rowid = records.last().map(|record| record.0);
            self.clear_rightmost_leaf_cache();
            self.clear_seek_cache();
            self.cell_slot_cache.get_mut().clear();
            self.last_known_depth = Some(1);
            self.bump_row_image_epoch();
            return Ok(true);
        }

        let leaf_grouping_start = instrumentation::profile_start();
        let Some(leaf_groups) = self.bulk_table_leaf_groups(records, 0)? else {
            instrumentation::record_bulk_table_grouping(leaf_grouping_start);
            return Ok(false);
        };
        instrumentation::record_bulk_table_grouping(leaf_grouping_start);
        let mut current_level = Vec::with_capacity(leaf_groups.len());
        for group in leaf_groups {
            let page_no = self.pager.allocate_page(cx)?;
            let leaf_build_start = instrumentation::profile_start();
            let page =
                self.build_bulk_table_leaf_page(page_no, None, &records[group.start..group.end])?;
            instrumentation::record_bulk_table_leaf_page_build(leaf_build_start);
            let leaf_write_start = instrumentation::profile_start();
            self.pager.write_page_data(cx, page_no, page)?;
            instrumentation::record_bulk_table_leaf_page_write(leaf_write_start);
            current_level.push(BulkTableChild {
                page_no,
                max_rowid: records[group.end - 1].0,
            });
        }

        let mut depth = 2usize;
        loop {
            let root_grouping_start = instrumentation::profile_start();
            let root_groups = self
                .bulk_table_interior_groups(&current_level, root_header_offset)
                .ok_or(FrankenError::TooBig)?;
            instrumentation::record_bulk_table_grouping(root_grouping_start);
            if root_groups.len() == 1 {
                let interior_build_start = instrumentation::profile_start();
                let root_page = self.build_bulk_table_interior_page(
                    self.root_page,
                    root_prefix,
                    &current_level,
                )?;
                instrumentation::record_bulk_table_interior_page_build(interior_build_start);
                let interior_write_start = instrumentation::profile_start();
                self.pager.write_page_data(cx, self.root_page, root_page)?;
                instrumentation::record_bulk_table_interior_page_write(interior_write_start);
                self.stack.clear();
                self.at_eof = true;
                self.last_insert_rowid = records.last().map(|record| record.0);
                self.clear_rightmost_leaf_cache();
                self.clear_seek_cache();
                self.cell_slot_cache.get_mut().clear();
                self.last_known_depth = Some(depth);
                self.bump_row_image_epoch();
                return Ok(true);
            }

            let interior_grouping_start = instrumentation::profile_start();
            let interior_groups = self
                .bulk_table_interior_groups(&current_level, 0)
                .ok_or(FrankenError::TooBig)?;
            instrumentation::record_bulk_table_grouping(interior_grouping_start);
            let mut next_level = Vec::with_capacity(interior_groups.len());
            for group in interior_groups {
                let page_no = self.pager.allocate_page(cx)?;
                let group_children = &current_level[group.start..group.end];
                let interior_build_start = instrumentation::profile_start();
                let page = self.build_bulk_table_interior_page(page_no, None, group_children)?;
                instrumentation::record_bulk_table_interior_page_build(interior_build_start);
                let interior_write_start = instrumentation::profile_start();
                self.pager.write_page_data(cx, page_no, page)?;
                instrumentation::record_bulk_table_interior_page_write(interior_write_start);
                next_level.push(BulkTableChild {
                    page_no,
                    max_rowid: group_children.last().ok_or(FrankenError::TooBig)?.max_rowid,
                });
            }
            current_level = next_level;
            depth += 1;
        }
    }

    /// Return whether one new right-edge table record matches the narrow
    /// depth-2 bulk-append shape.
    pub fn table_can_bulk_append_depth2_right_edge_record(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload: &[u8],
    ) -> Result<bool> {
        if !self.is_table {
            return Ok(false);
        }
        let payload_size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        if cell::has_overflow(payload_size, self.usable_size, BtreePageType::LeafTable) {
            return Ok(false);
        }

        let root_data = self.pager.read_btree_page_data(cx, self.root_page)?;
        let root_header = cell::parse_page_header(root_data.as_bytes(), self.root_page)?;
        if root_header.page_type != BtreePageType::InteriorTable || root_header.cell_count == 0 {
            return Ok(false);
        }
        if root_header.first_freeblock != 0 || root_header.fragmented_free_bytes != 0 {
            return Ok(false);
        }
        let Some(old_right_child) = root_header.right_child else {
            return Ok(false);
        };

        let old_right_data = self.pager.read_btree_page_data(cx, old_right_child)?;
        let old_right_header = cell::parse_page_header(old_right_data.as_bytes(), old_right_child)?;
        if old_right_header.page_type != BtreePageType::LeafTable
            || old_right_header.cell_count == 0
        {
            return Ok(false);
        }
        let old_right_header_offset = cell::header_offset_for_page(old_right_child);
        let old_right_ptrs = cell::read_cell_pointers(
            old_right_data.as_bytes(),
            &old_right_header,
            old_right_header_offset,
        )?;
        let Some(last_ptr) = old_right_ptrs.last().copied() else {
            return Ok(false);
        };
        let Some(old_max_rowid) =
            cell::read_table_leaf_rowid_at_offset(old_right_data.as_bytes(), usize::from(last_ptr))
        else {
            return Ok(false);
        };
        if rowid <= old_max_rowid {
            return Ok(false);
        }

        let root_header_offset = cell::header_offset_for_page(self.root_page);
        let root_content_offset =
            usize::try_from(root_header.cell_content_offset).map_err(|_| FrankenError::TooBig)?;
        if root_content_offset == 0 {
            return Ok(false);
        }
        Ok(Self::bulk_page_can_append_cell(
            root_header_offset,
            BtreePageType::InteriorTable,
            usize::from(root_header.cell_count),
            root_content_offset,
            Self::table_interior_cell_len(old_max_rowid),
        ))
    }

    /// Bulk-append sorted table records to the right edge of a depth-2 table.
    ///
    /// This deliberately handles only the common benchmark-shaped case where
    /// the root is an interior-table page whose right child is the current
    /// rightmost leaf and the root has enough free space for the new child
    /// separators. Deeper trees, root splits, overflow payloads, and unordered
    /// rows return `Ok(false)` so callers can fall back to normal append logic.
    pub fn table_bulk_append_depth2_right_edge_sorted_records<R: AsRef<[u8]>>(
        &mut self,
        cx: &Cx,
        records: &[(i64, R)],
    ) -> Result<bool> {
        if !self.is_table || records.is_empty() {
            return Ok(false);
        }
        if records.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Ok(false);
        }

        let root_data = self.pager.read_btree_page_data(cx, self.root_page)?;
        let root_header = cell::parse_page_header(root_data.as_bytes(), self.root_page)?;
        if root_header.page_type != BtreePageType::InteriorTable || root_header.cell_count == 0 {
            return Ok(false);
        }
        if root_header.first_freeblock != 0 || root_header.fragmented_free_bytes != 0 {
            return Ok(false);
        }
        let Some(old_right_child) = root_header.right_child else {
            return Ok(false);
        };

        let old_right_data = self.pager.read_btree_page_data(cx, old_right_child)?;
        let old_right_header = cell::parse_page_header(old_right_data.as_bytes(), old_right_child)?;
        if old_right_header.page_type != BtreePageType::LeafTable
            || old_right_header.cell_count == 0
        {
            return Ok(false);
        }
        let old_right_header_offset = cell::header_offset_for_page(old_right_child);
        let old_right_ptrs = cell::read_cell_pointers(
            old_right_data.as_bytes(),
            &old_right_header,
            old_right_header_offset,
        )?;
        let Some(last_ptr) = old_right_ptrs.last().copied() else {
            return Ok(false);
        };
        let Some(old_max_rowid) =
            cell::read_table_leaf_rowid_at_offset(old_right_data.as_bytes(), usize::from(last_ptr))
        else {
            return Ok(false);
        };
        if records[0].0 <= old_max_rowid {
            return Ok(false);
        }

        let leaf_grouping_start = instrumentation::profile_start();
        let Some(leaf_groups) = self.bulk_table_leaf_groups(records, 0)? else {
            instrumentation::record_bulk_table_grouping(leaf_grouping_start);
            return Ok(false);
        };
        instrumentation::record_bulk_table_grouping(leaf_grouping_start);

        let root_header_offset = cell::header_offset_for_page(self.root_page);
        let initial_root_cell_count = usize::from(root_header.cell_count);
        let mut root_content_offset =
            usize::try_from(root_header.cell_content_offset).map_err(|_| FrankenError::TooBig)?;
        if root_content_offset == 0 {
            return Ok(false);
        }
        let mut divider_keys = Vec::with_capacity(leaf_groups.len());
        divider_keys.push(old_max_rowid);
        divider_keys.extend(
            leaf_groups
                .iter()
                .take(leaf_groups.len().saturating_sub(1))
                .map(|group| records[group.end - 1].0),
        );
        for (root_cell_count, divider_key) in (initial_root_cell_count..).zip(divider_keys.iter()) {
            let divider_len = Self::table_interior_cell_len(*divider_key);
            if !Self::bulk_page_can_append_cell(
                root_header_offset,
                BtreePageType::InteriorTable,
                root_cell_count,
                root_content_offset,
                divider_len,
            ) {
                return Ok(false);
            }
            root_content_offset -= divider_len;
        }

        let mut new_children = Vec::with_capacity(leaf_groups.len());
        for group in &leaf_groups {
            let page_no = self.pager.allocate_page(cx)?;
            let leaf_build_start = instrumentation::profile_start();
            let page =
                self.build_bulk_table_leaf_page(page_no, None, &records[group.start..group.end])?;
            instrumentation::record_bulk_table_leaf_page_build(leaf_build_start);
            let leaf_write_start = instrumentation::profile_start();
            self.pager.write_page_data(cx, page_no, page)?;
            instrumentation::record_bulk_table_leaf_page_write(leaf_write_start);
            new_children.push(BulkTableChild {
                page_no,
                max_rowid: records[group.end - 1].0,
            });
        }

        let interior_build_start = instrumentation::profile_start();
        let mut root_page = root_data.into_vec();
        let mut root_ptrs = cell::read_cell_pointers(&root_page, &root_header, root_header_offset)?;
        let mut write_offset =
            usize::try_from(root_header.cell_content_offset).map_err(|_| FrankenError::TooBig)?;
        let mut rowid_varint = [0u8; 9];
        let mut append_cell = |child_page: PageNumber, divider_key: i64| -> Result<()> {
            let rowid_len = write_varint(
                &mut rowid_varint,
                u64::from_ne_bytes(divider_key.to_ne_bytes()),
            );
            let cell_len = 4 + rowid_len;
            write_offset = write_offset
                .checked_sub(cell_len)
                .ok_or(FrankenError::TooBig)?;
            let cell_offset =
                u16::try_from(write_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "bulk append root cell offset {} exceeds u16 range on page {}",
                        write_offset,
                        self.root_page.get()
                    ),
                })?;
            root_page[write_offset..write_offset + 4]
                .copy_from_slice(&child_page.get().to_be_bytes());
            root_page[write_offset + 4..write_offset + cell_len]
                .copy_from_slice(&rowid_varint[..rowid_len]);
            root_ptrs.push(cell_offset);
            Ok(())
        };

        append_cell(old_right_child, old_max_rowid)?;
        for child in new_children
            .iter()
            .take(new_children.len().saturating_sub(1))
        {
            append_cell(child.page_no, child.max_rowid)?;
        }
        let new_right_child = new_children
            .last()
            .map(|child| child.page_no)
            .ok_or(FrankenError::TooBig)?;
        let new_header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: root_header.first_freeblock,
            cell_count: u16::try_from(root_ptrs.len()).map_err(|_| FrankenError::TooBig)?,
            cell_content_offset: u32::try_from(write_offset).map_err(|_| FrankenError::TooBig)?,
            fragmented_free_bytes: root_header.fragmented_free_bytes,
            right_child: Some(new_right_child),
        };
        new_header.write(&mut root_page, root_header_offset);
        cell::write_cell_pointers(&mut root_page, root_header_offset, &new_header, &root_ptrs);
        instrumentation::record_bulk_table_interior_page_build(interior_build_start);
        let interior_write_start = instrumentation::profile_start();
        self.pager
            .write_page_data(cx, self.root_page, PageData::from_vec(root_page))?;
        instrumentation::record_bulk_table_interior_page_write(interior_write_start);

        self.stack.clear();
        self.at_eof = true;
        self.last_insert_rowid = records.last().map(|record| record.0);
        self.clear_rightmost_leaf_cache();
        self.clear_seek_cache();
        self.cell_slot_cache.get_mut().clear();
        self.last_known_depth = Some(2);
        self.bump_row_image_epoch();
        Ok(true)
    }

    fn refresh_rightmost_leaf_cache_after_insert(&mut self, cx: &Cx, rowid: i64) -> Result<()> {
        if let Some(cached) = self
            .rightmost_leaf_cache
            .as_ref()
            .filter(|cached| cached.rowid == rowid)
        {
            let cursor_on_cached_leaf = !self.at_eof
                && self.stack.last().is_some_and(|entry| {
                    entry.page_no == cached.page_no
                        && entry.header.page_type == cell::BtreePageType::LeafTable
                });
            if cursor_on_cached_leaf {
                return Ok(());
            }

            if let Some(cell_idx) = cached.header.cell_count.checked_sub(1) {
                let page_no = cached.page_no;
                let page_data = cached.page_data.clone();
                let header = cached.header;
                let cell_pointers = cached.cell_pointers.clone();
                let tree_depth = cached.tree_depth;
                let mutation_counter = Self::page_mutation_counter(&page_data);
                self.stack.clear();
                self.stack.push(StackEntry {
                    page_no,
                    page_data,
                    header,
                    cell_pointers,
                    mutation_counter,
                    cell_idx,
                });
                self.at_eof = false;
                self.last_known_depth = Some(tree_depth);
                return Ok(());
            }
        }

        if !self.at_eof
            && self
                .stack
                .last()
                .is_some_and(|entry| entry.header.page_type == cell::BtreePageType::LeafTable)
            && self.rowid(cx)? == rowid
        {
            let parent_page = self.current_parent_page_hint_from_stack();
            if let Some(cache_entry) = self.stack.last().map(|entry| RightmostLeafCacheEntry {
                page_no: entry.page_no,
                rowid,
                tree_depth: self.current_tree_depth_hint().unwrap_or(1),
                parent_page,
                page_data: entry.page_data.clone(),
                header: entry.header,
                cell_pointers: entry.cell_pointers.clone(),
            }) {
                self.rightmost_leaf_cache = Some(cache_entry);
                return Ok(());
            }
        }

        if self.last(cx)? {
            let cache_entry = self
                .stack
                .last()
                .map(|entry| RightmostLeafCacheEntry {
                    page_no: entry.page_no,
                    rowid,
                    tree_depth: self.current_tree_depth_hint().unwrap_or(1),
                    parent_page: self.current_parent_page_hint_from_stack(),
                    page_data: entry.page_data.clone(),
                    header: entry.header,
                    cell_pointers: entry.cell_pointers.clone(),
                })
                .ok_or_else(|| FrankenError::internal("cursor lost rightmost leaf after insert"))?;
            self.rightmost_leaf_cache = Some(cache_entry);
        } else {
            self.clear_rightmost_leaf_cache();
        }

        Ok(())
    }

    fn current_parent_page_hint_from_stack(&self) -> Option<PageNumber> {
        if self.stack.len() >= 2 {
            return Some(self.stack[self.stack.len() - 2].page_no);
        }

        self.stack.last().and_then(|entry| {
            self.rightmost_leaf_cache
                .as_ref()
                .filter(|cached| cached.page_no == entry.page_no)
                .and_then(|cached| cached.parent_page)
        })
    }

    fn write_overflow_chain_for_insert(
        &mut self,
        cx: &Cx,
        overflow_data: &[u8],
    ) -> Result<PageNumber> {
        if overflow_data.is_empty() {
            return Err(FrankenError::internal(
                "overflow writer called with empty payload",
            ));
        }
        if self.usable_size <= 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "invalid usable page size {} for overflow chain",
                    self.usable_size
                ),
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let bytes_per_page = self.usable_size.saturating_sub(4) as usize;
        if bytes_per_page == 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "usable page size too small for overflow pages".to_owned(),
            });
        }
        #[allow(clippy::cast_possible_truncation)]
        let page_size = self.page_size as usize;
        let num_pages = overflow_data.len().div_ceil(bytes_per_page);
        if num_pages > overflow::MAX_OVERFLOW_CHAIN {
            return Err(FrankenError::TooBig);
        }

        let mut pages = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            match self.pager.allocate_page(cx) {
                Ok(pgno) => pages.push(pgno),
                Err(err) => {
                    for leaked in pages {
                        let _ = self.pager.free_page(cx, leaked);
                    }
                    return Err(err);
                }
            }
        }

        let mut page_buf = vec![0u8; page_size];
        for (idx, &pgno) in pages.iter().enumerate() {
            let data_start = idx * bytes_per_page;
            let data_end = ((idx + 1) * bytes_per_page).min(overflow_data.len());
            let chunk = &overflow_data[data_start..data_end];

            let next = if idx + 1 < pages.len() {
                pages[idx + 1].get()
            } else {
                0
            };

            page_buf[0..4].copy_from_slice(&next.to_be_bytes());
            page_buf[4..4 + chunk.len()].copy_from_slice(chunk);
            if chunk.len() < bytes_per_page {
                // Ensure tail is zeroed if the chunk didn't fill the space.
                page_buf[4 + chunk.len()..].fill(0);
            }
            let owned_page = std::mem::replace(&mut page_buf, vec![0_u8; page_size]);
            if let Err(err) = self
                .pager
                .write_page_data(cx, pgno, PageData::from_vec(owned_page))
            {
                // Best-effort cleanup: any overflow pages allocated for this
                // cell must be released if chain materialization fails midway.
                for leaked in pages.iter().copied() {
                    let _ = self.pager.free_page(cx, leaked);
                }
                return Err(err);
            }
        }

        Ok(pages[0])
    }

    fn free_overflow_chain(&mut self, cx: &Cx, first: PageNumber) -> Result<()> {
        let _mask = cx.masked();
        let mut current = Some(first);
        let mut visited = 0usize;

        while let Some(pgno) = current {
            // Once overflow cleanup starts, finish the chain so the statement
            // cannot strand partially-freed pages behind an interrupt.
            visited += 1;
            if visited > overflow::MAX_OVERFLOW_CHAIN {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "overflow chain exceeds {} pages while freeing",
                        overflow::MAX_OVERFLOW_CHAIN
                    ),
                });
            }

            let page = self.pager.read_btree_page_data(cx, pgno)?;
            let page_bytes = page.as_bytes();
            if page_bytes.len() < 4 {
                warn!(
                    page = pgno.get(),
                    page_len = page_bytes.len(),
                    "overflow chain corruption detected while freeing"
                );
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("overflow page {} too small while freeing", pgno.get()),
                });
            }

            let next =
                u32::from_be_bytes([page_bytes[0], page_bytes[1], page_bytes[2], page_bytes[3]]);
            current = PageNumber::new(next);
            self.pager.free_page(cx, pgno)?;
        }

        Ok(())
    }

    /// Encode a table leaf cell into the provided buffer, returning the
    /// overflow head page (if any).
    ///
    /// The buffer is cleared and reused across calls so repeated inserts
    /// reuse the same heap allocation.
    #[inline]
    fn encode_table_leaf_cell_into(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<Option<PageNumber>> {
        out.clear();
        let payload_size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        let payload_size_u64 = u64::from(payload_size);
        let local_size = cell::local_payload_size(
            payload_size,
            self.usable_size,
            cell::BtreePageType::LeafTable,
        ) as usize;
        let local_size = local_size.min(payload.len());

        let mut varint = [0u8; 9];
        let p_len = write_varint(&mut varint, payload_size_u64);
        out.extend_from_slice(&varint[..p_len]);

        let rowid_bits = u64::from_ne_bytes(rowid.to_ne_bytes());
        let r_len = write_varint(&mut varint, rowid_bits);
        out.extend_from_slice(&varint[..r_len]);
        out.extend_from_slice(&payload[..local_size]);

        if local_size < payload.len() {
            let first_overflow =
                self.write_overflow_chain_for_insert(cx, &payload[local_size..])?;
            out.extend_from_slice(&first_overflow.get().to_be_bytes());
            instrumentation::record_table_leaf_cell_assembly(out.len());
            Ok(Some(first_overflow))
        } else {
            instrumentation::record_table_leaf_cell_assembly(out.len());
            Ok(None)
        }
    }

    /// Backwards-compatible wrapper — allocates a fresh Vec per call.
    #[allow(dead_code)]
    fn encode_table_leaf_cell(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload: &[u8],
    ) -> Result<(Vec<u8>, Option<PageNumber>)> {
        let mut out = Vec::with_capacity(24 + payload.len().min(self.usable_size as usize) + 4);
        let overflow = self.encode_table_leaf_cell_into(cx, rowid, payload, &mut out)?;
        Ok((out, overflow))
    }

    /// Encode an index leaf cell into the provided buffer, returning the
    /// overflow head page (if any).
    fn encode_index_leaf_cell_into(
        &mut self,
        cx: &Cx,
        key: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<Option<PageNumber>> {
        out.clear();
        let payload_size = u32::try_from(key.len()).map_err(|_| FrankenError::TooBig)?;
        let payload_size_u64 = u64::from(payload_size);
        let local_size = cell::local_payload_size(
            payload_size,
            self.usable_size,
            cell::BtreePageType::LeafIndex,
        ) as usize;
        let local_size = local_size.min(key.len());

        let mut varint = [0u8; 9];
        let p_len = write_varint(&mut varint, payload_size_u64);
        out.extend_from_slice(&varint[..p_len]);
        out.extend_from_slice(&key[..local_size]);

        if local_size < key.len() {
            let first_overflow = self.write_overflow_chain_for_insert(cx, &key[local_size..])?;
            out.extend_from_slice(&first_overflow.get().to_be_bytes());
            instrumentation::record_index_leaf_cell_assembly(out.len());
            Ok(Some(first_overflow))
        } else {
            instrumentation::record_index_leaf_cell_assembly(out.len());
            Ok(None)
        }
    }

    /// Backwards-compatible wrapper — allocates a fresh Vec per call.
    #[allow(dead_code)]
    fn encode_index_leaf_cell(
        &mut self,
        cx: &Cx,
        key: &[u8],
    ) -> Result<(Vec<u8>, Option<PageNumber>)> {
        let mut out = Vec::with_capacity(16 + key.len().min(self.usable_size as usize) + 4);
        let overflow = self.encode_index_leaf_cell_into(cx, key, &mut out)?;
        Ok((out, overflow))
    }

    #[allow(dead_code)] // Retained for future compaction-before-balance optimization
    fn current_table_leaf_needs_compaction(&self) -> bool {
        self.stack.last().is_some_and(|entry| {
            entry.header.page_type == cell::BtreePageType::LeafTable
                && (entry.header.first_freeblock != 0 || entry.header.fragmented_free_bytes != 0)
        })
    }

    #[allow(dead_code)] // Retained for future compaction-before-balance optimization
    fn compact_current_table_leaf(&mut self, cx: &Cx) -> Result<bool> {
        if !self.current_table_leaf_needs_compaction() {
            return Ok(false);
        }

        let saved_entry = self
            .stack
            .last()
            .cloned()
            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
        let saved_eof = self.at_eof;
        let page_no = saved_entry.page_no;
        let header_offset = cell::header_offset_for_page(page_no);

        let mut compacted = vec![0u8; self.page_size as usize];
        if header_offset > 0 {
            compacted[..header_offset]
                .copy_from_slice(&saved_entry.page_data.as_bytes()[..header_offset]);
        }

        let mut cell_bytes = Vec::with_capacity(saved_entry.cell_pointers.len());
        for &off in &saved_entry.cell_pointers {
            let ptr = usize::from(off);
            // OPT-A3: size-only fast path for compaction — we only need
            // each cell's byte extent, not its logical structure.
            let size = cell::cell_on_page_size_fast(
                saved_entry.page_data.as_bytes(),
                ptr,
                saved_entry.header.page_type,
                self.usable_size,
            )?;
            cell_bytes.push(saved_entry.page_data.as_bytes()[ptr..ptr + size].to_vec());
        }

        let mut new_ptrs = Vec::with_capacity(cell_bytes.len());
        let mut new_content_offset = self.usable_size as usize;
        for bytes in cell_bytes.iter().rev() {
            new_content_offset = new_content_offset.checked_sub(bytes.len()).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: format!("table leaf compaction underflow on page {}", page_no.get()),
                }
            })?;
            compacted[new_content_offset..new_content_offset + bytes.len()].copy_from_slice(bytes);
            new_ptrs.push(u16::try_from(new_content_offset).map_err(|_| {
                FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf compaction offset {} exceeds u16 range on page {}",
                        new_content_offset,
                        page_no.get()
                    ),
                }
            })?);
        }
        new_ptrs.reverse();

        let mut header = saved_entry.header;
        header.first_freeblock = 0;
        header.fragmented_free_bytes = 0;
        header.cell_count =
            u16::try_from(new_ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf compaction cell count exceeds u16 range on page {}",
                    page_no.get()
                ),
            })?;
        header.cell_content_offset = if new_ptrs.is_empty() {
            self.usable_size
        } else {
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "table leaf compaction content offset {} exceeds u32 range on page {}",
                    new_content_offset,
                    page_no.get()
                ),
            })?
        };
        header.write(&mut compacted, header_offset);
        cell::write_cell_pointers(&mut compacted, header_offset, &header, &new_ptrs);

        self.pager
            .write_page_data(cx, page_no, PageData::from_vec(compacted))?;

        let mut refreshed = self.reload_page_fresh(cx, page_no)?;
        if refreshed.header.cell_count == 0 {
            refreshed.cell_idx = 0;
        } else {
            refreshed.cell_idx = saved_entry
                .cell_idx
                .min(refreshed.header.cell_count.saturating_sub(1));
        }
        if let Some(top) = self.stack.last_mut() {
            *top = refreshed;
        }
        self.at_eof = saved_eof;
        Ok(true)
    }

    fn try_allocate_table_leaf_freeblock(
        entry: &mut StackEntry,
        cell_len: usize,
        usable_size: u32,
    ) -> Result<Option<u16>> {
        if entry.header.page_type != cell::BtreePageType::LeafTable
            || entry.header.first_freeblock == 0
        {
            return Ok(None);
        }

        let usable_limit =
            usize::try_from(usable_size).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "usable size {} exceeds usize range on page {}",
                    usable_size,
                    entry.page_no.get()
                ),
            })?;
        let page_bytes = entry.page_data.as_bytes_mut();
        let mut previous_freeblock = None;
        let mut current_freeblock = entry.header.first_freeblock;

        while current_freeblock != 0 {
            let freeblock_offset = usize::from(current_freeblock);
            if freeblock_offset + 4 > usable_limit {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock header at offset {} extends past usable space on page {}",
                        freeblock_offset,
                        entry.page_no.get()
                    ),
                });
            }

            let next_freeblock = u16::from_be_bytes([
                page_bytes[freeblock_offset],
                page_bytes[freeblock_offset + 1],
            ]);
            let freeblock_size = usize::from(u16::from_be_bytes([
                page_bytes[freeblock_offset + 2],
                page_bytes[freeblock_offset + 3],
            ]));

            if freeblock_size < 4 {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock size {} is too small on page {}",
                        freeblock_size,
                        entry.page_no.get()
                    ),
                });
            }
            if freeblock_offset + freeblock_size > usable_limit {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock [{}..{}) exceeds usable space on page {}",
                        freeblock_offset,
                        freeblock_offset + freeblock_size,
                        entry.page_no.get()
                    ),
                });
            }

            if freeblock_size >= cell_len {
                let leftover = freeblock_size - cell_len;
                if leftover >= 4 {
                    let leftover_offset = freeblock_offset + cell_len;
                    let leftover_offset_u16 = u16::try_from(leftover_offset).map_err(|_| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "leftover freeblock offset {} exceeds u16 range on page {}",
                                leftover_offset,
                                entry.page_no.get()
                            ),
                        }
                    })?;
                    let leftover_size_u16 =
                        u16::try_from(leftover).map_err(|_| FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "leftover freeblock size {} exceeds u16 range on page {}",
                                leftover,
                                entry.page_no.get()
                            ),
                        })?;

                    page_bytes[leftover_offset..leftover_offset + 2]
                        .copy_from_slice(&next_freeblock.to_be_bytes());
                    page_bytes[leftover_offset + 2..leftover_offset + 4]
                        .copy_from_slice(&leftover_size_u16.to_be_bytes());
                    if leftover > 4 {
                        page_bytes[leftover_offset + 4..freeblock_offset + freeblock_size].fill(0);
                    }

                    if let Some(previous_offset) = previous_freeblock {
                        page_bytes[previous_offset..previous_offset + 2]
                            .copy_from_slice(&leftover_offset_u16.to_be_bytes());
                    } else {
                        entry.header.first_freeblock = leftover_offset_u16;
                    }
                } else {
                    if let Some(previous_offset) = previous_freeblock {
                        page_bytes[previous_offset..previous_offset + 2]
                            .copy_from_slice(&next_freeblock.to_be_bytes());
                    } else {
                        entry.header.first_freeblock = next_freeblock;
                    }

                    entry.header.fragmented_free_bytes = entry
                        .header
                        .fragmented_free_bytes
                        .saturating_add(u8::try_from(leftover).unwrap_or(u8::MAX));
                }

                return Ok(Some(current_freeblock));
            }

            previous_freeblock = Some(freeblock_offset);
            current_freeblock = next_freeblock;
        }

        Ok(None)
    }

    /// Try to insert a cell directly onto the leaf page at the top of the
    /// cursor stack. Returns `Ok(true)` if the cell was inserted, or
    /// `Ok(false)` if the page is full and balance is needed.
    fn try_insert_on_leaf(&mut self, cx: &Cx, insert_idx: u16, cell_data: &[u8]) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        let (leaf_page_no, staged_page, insert_at) = {
            let entry = self
                .stack
                .last_mut()
                .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
            if !entry.header.page_type.is_leaf() {
                return Err(FrankenError::internal(
                    "try_insert_on_leaf requires a leaf stack entry",
                ));
            }

            let leaf_page_no = entry.page_no;
            let header_offset = cell::header_offset_for_page(leaf_page_no);
            let content_offset = entry.header.content_offset(self.usable_size);
            let insert_at = usize::from(insert_idx).min(entry.cell_pointers.len());
            let ptr_array_end = header_offset
                + usize::from(entry.header.page_type.header_size())
                + (entry.cell_pointers.len() + 1) * 2;
            let new_cell_offset = if ptr_array_end <= content_offset {
                if let Some(reused_offset) = Self::try_allocate_table_leaf_freeblock(
                    entry,
                    cell_data.len(),
                    self.usable_size,
                )? {
                    reused_offset
                } else if let Some(new_content_offset) = content_offset.checked_sub(cell_data.len())
                    && ptr_array_end <= new_content_offset
                {
                    entry.header.cell_content_offset =
                        u32::try_from(new_content_offset).map_err(|_| {
                            FrankenError::DatabaseCorrupt {
                                detail: format!(
                                    "new leaf content offset {} exceeds u32 range on page {}",
                                    new_content_offset,
                                    leaf_page_no.get()
                                ),
                            }
                        })?;
                    u16::try_from(new_content_offset).map_err(|_| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "new leaf cell offset {} exceeds u16 range on page {}",
                                new_content_offset,
                                leaf_page_no.get()
                            ),
                        }
                    })?
                } else {
                    debug!(
                        page_number = leaf_page_no.get(),
                        requested_insert_idx = insert_idx,
                        reason = "content_underflow",
                        "leaf insert requires balance or compaction"
                    );
                    return Ok(false);
                }
            } else {
                debug!(
                    page_number = leaf_page_no.get(),
                    requested_insert_idx = insert_idx,
                    reason = "pointer_array_overlap",
                    "leaf insert requires balance or compaction"
                );
                return Ok(false);
            };
            if insert_at == entry.cell_pointers.len() {
                entry.cell_pointers.push(new_cell_offset);
            } else {
                entry.cell_pointers.insert(insert_at, new_cell_offset);
            }
            entry.header.cell_count = u16::try_from(entry.cell_pointers.len()).map_err(|_| {
                FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "leaf page {} cell count exceeds u16 range during insert",
                        leaf_page_no.get()
                    ),
                }
            })?;

            {
                let page_bytes = entry.page_data.as_bytes_mut();
                let new_cell_offset_usize = usize::from(new_cell_offset);
                page_bytes[new_cell_offset_usize..new_cell_offset_usize + cell_data.len()]
                    .copy_from_slice(cell_data);
                entry.header.write(page_bytes, header_offset);
                cell::write_cell_pointers(
                    page_bytes,
                    header_offset,
                    &entry.header,
                    &entry.cell_pointers,
                );
            }
            entry.mutation_counter = Self::page_mutation_counter(&entry.page_data);
            #[allow(clippy::cast_possible_truncation)]
            {
                entry.cell_idx = insert_at as u16;
            }

            debug!(
                page_number = leaf_page_no.get(),
                insert_at,
                cell_count = entry.header.cell_count,
                "reused current leaf state after no-split insert"
            );
            instrumentation::record_no_split_reuse_hit();
            (leaf_page_no, entry.page_data.clone(), insert_at)
        };

        self.pager.write_page_data(cx, leaf_page_no, staged_page)?;
        self.at_eof = false;
        trace!(
            page_number = leaf_page_no.get(),
            insert_at, "published retained no-split leaf insert without reload"
        );
        Ok(true)
    }

    /// Try to append a cell onto the current leaf page already loaded at the
    /// top of the cursor stack.
    ///
    /// This is used by the rightmost-leaf hint path, where the leaf was just
    /// loaded and validated in the same function. That lets us reuse the
    /// cached page/header directly and avoid a second pager read plus a full
    /// cell-pointer-array decode/reload cycle on the hot append case.
    fn try_append_on_current_leaf(&mut self, cx: &Cx, cell_data: &[u8]) -> Result<bool> {
        let mut entry = self
            .stack
            .pop()
            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
        let leaf_page_no = entry.page_no;
        let append_result = self.try_append_leaf_page_in_place(
            cx,
            leaf_page_no,
            &mut entry.page_data,
            &mut entry.header,
            cell_data,
        );
        let (insert_idx, new_cell_offset) = match append_result {
            Ok(Some(result)) => result,
            Ok(None) => {
                self.stack.push(entry);
                return Ok(false);
            }
            Err(error) => {
                self.at_eof = false;
                self.stack.push(entry);
                return Err(error);
            }
        };

        entry.cell_pointers.push(new_cell_offset);
        entry.cell_idx = insert_idx;

        self.stack.push(entry);
        self.at_eof = false;
        Ok(true)
    }

    fn try_append_payload_on_current_leaf_with_writer<W>(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload_len: usize,
        writer: W,
    ) -> Result<bool>
    where
        W: FnOnce(&mut [u8]) -> Result<()>,
    {
        let mut entry = self
            .stack
            .pop()
            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
        let leaf_page_no = entry.page_no;
        let append_result = self.try_append_table_leaf_payload_in_place_no_overflow_with_writer(
            cx,
            leaf_page_no,
            &mut entry.page_data,
            &mut entry.header,
            rowid,
            payload_len,
            writer,
        );
        let (insert_idx, new_cell_offset) = match append_result {
            Ok(Some(result)) => result,
            Ok(None) => {
                self.stack.push(entry);
                return Ok(false);
            }
            Err(error) => {
                self.at_eof = false;
                self.stack.push(entry);
                return Err(error);
            }
        };

        entry.cell_pointers.push(new_cell_offset);
        entry.cell_idx = insert_idx;
        entry.mutation_counter = Self::page_mutation_counter(&entry.page_data);
        self.stack.push(entry);
        self.at_eof = false;
        self.last_insert_rowid = Some(rowid);
        Ok(true)
    }

    fn try_append_leaf_page_in_place(
        &mut self,
        cx: &Cx,
        leaf_page_no: PageNumber,
        page_data: &mut PageData,
        header: &mut BtreePageHeader,
        cell_data: &[u8],
    ) -> Result<Option<(u16, u16)>> {
        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let insert_idx = header.cell_count;
        let content_offset = header.content_offset(self.usable_size);
        let Some(new_content_offset) = content_offset.checked_sub(cell_data.len()) else {
            return Ok(None);
        };

        let ptr_array_end = header_offset
            + usize::from(header.page_type.header_size())
            + (usize::from(insert_idx) + 1) * 2;
        if ptr_array_end > new_content_offset {
            return Ok(None);
        }

        let new_cell_offset =
            u16::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "new leaf cell offset {} exceeds u16 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        let new_cell_count =
            insert_idx
                .checked_add(1)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "leaf page {} cell count overflow while appending",
                        leaf_page_no.get()
                    ),
                })?;
        let ptr_offset = header_offset
            + usize::from(header.page_type.header_size())
            + usize::from(insert_idx) * 2;

        header.cell_count = new_cell_count;
        header.cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "new leaf content offset {} exceeds u32 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        let mutate_start = instrumentation::profile_start();
        {
            let page_bytes = page_data.as_bytes_mut();
            page_bytes[new_content_offset..new_content_offset + cell_data.len()]
                .copy_from_slice(cell_data);
            page_bytes[ptr_offset..ptr_offset + 2].copy_from_slice(&new_cell_offset.to_be_bytes());
            header.write(page_bytes, header_offset);
        }
        instrumentation::record_fast_table_leaf_full_cell_append_mutate(mutate_start);

        let staged_page = page_data.clone();
        let stage_start = instrumentation::profile_start();
        self.pager.write_page_data(cx, leaf_page_no, staged_page)?;
        instrumentation::record_fast_table_leaf_full_cell_append_stage(stage_start);
        Ok(Some((insert_idx, new_cell_offset)))
    }

    fn try_append_table_leaf_payload_in_place_no_overflow_mutate_only(
        usable_size: u32,
        leaf_page_no: PageNumber,
        page_data: &mut PageData,
        header: &mut BtreePageHeader,
        rowid: i64,
        payload: &[u8],
    ) -> Result<Option<(u16, u16)>> {
        let payload_size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        let local_size =
            cell::local_payload_size(payload_size, usable_size, cell::BtreePageType::LeafTable)
                as usize;
        let local_size = local_size.min(payload.len());
        if local_size < payload.len() {
            return Ok(None);
        }

        let mut payload_varint = [0u8; 9];
        let payload_len = write_varint(&mut payload_varint, u64::from(payload_size));
        let mut rowid_varint = [0u8; 9];
        let rowid_len = write_varint(&mut rowid_varint, u64::from_ne_bytes(rowid.to_ne_bytes()));
        let cell_len = payload_len + rowid_len + local_size;

        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let insert_idx = header.cell_count;
        let content_offset = header.content_offset(usable_size);
        let Some(new_content_offset) = content_offset.checked_sub(cell_len) else {
            return Ok(None);
        };

        let ptr_array_end = header_offset
            + usize::from(header.page_type.header_size())
            + (usize::from(insert_idx) + 1) * 2;
        if ptr_array_end > new_content_offset {
            return Ok(None);
        }

        let new_cell_offset =
            u16::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "new leaf cell offset {} exceeds u16 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        let new_cell_count =
            insert_idx
                .checked_add(1)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "leaf page {} cell count overflow while appending",
                        leaf_page_no.get()
                    ),
                })?;
        let ptr_offset = header_offset
            + usize::from(header.page_type.header_size())
            + usize::from(insert_idx) * 2;

        header.cell_count = new_cell_count;
        header.cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "new leaf content offset {} exceeds u32 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        let mutate_start = instrumentation::profile_start();
        {
            let page_bytes = page_data.as_bytes_mut();
            let mut write_offset = new_content_offset;
            page_bytes[write_offset..write_offset + payload_len]
                .copy_from_slice(&payload_varint[..payload_len]);
            write_offset += payload_len;
            page_bytes[write_offset..write_offset + rowid_len]
                .copy_from_slice(&rowid_varint[..rowid_len]);
            write_offset += rowid_len;
            page_bytes[write_offset..write_offset + local_size]
                .copy_from_slice(&payload[..local_size]);
            page_bytes[ptr_offset..ptr_offset + 2].copy_from_slice(&new_cell_offset.to_be_bytes());
            header.write(page_bytes, header_offset);
        }
        instrumentation::record_fast_table_leaf_payload_append_mutate(mutate_start);
        Ok(Some((insert_idx, new_cell_offset)))
    }

    fn try_append_table_leaf_payload_in_place_no_overflow(
        &mut self,
        cx: &Cx,
        leaf_page_no: PageNumber,
        page_data: &mut PageData,
        header: &mut BtreePageHeader,
        rowid: i64,
        payload: &[u8],
    ) -> Result<Option<(u16, u16)>> {
        let Some((insert_idx, new_cell_offset)) =
            Self::try_append_table_leaf_payload_in_place_no_overflow_mutate_only(
                self.usable_size,
                leaf_page_no,
                page_data,
                header,
                rowid,
                payload,
            )?
        else {
            return Ok(None);
        };
        let staged_page = page_data.clone();
        let stage_start = instrumentation::profile_start();
        self.pager.write_page_data(cx, leaf_page_no, staged_page)?;
        instrumentation::record_fast_table_leaf_payload_append_stage(stage_start);
        Ok(Some((insert_idx, new_cell_offset)))
    }

    /// Writer-callback variant of
    /// [`Self::try_append_table_leaf_payload_in_place_no_overflow_mutate_only`].
    ///
    /// Instead of copying from an existing `&[u8]` payload, the caller passes
    /// `payload_len` (exact number of payload bytes to reserve in the cell)
    /// plus a one-shot `writer` closure which receives a `&mut [u8]` of that
    /// exact length carved inside the page. This lets the caller serialize
    /// record bytes directly into the page buffer, eliminating the
    /// intermediate `record_scratch: Vec<u8>` allocation and its memcpy into
    /// the page on the INSERT hot path.
    ///
    /// Returns `Ok(None)` when the local payload would need overflow pages
    /// (caller must fall back to the generic cell-encode path) OR when the
    /// remaining free space on the page is too small. Returns `Ok(Some((idx,
    /// offset)))` on a successful append. Propagates any error from `writer`
    /// or from size conversions.
    ///
    /// # Error contract
    ///
    /// The writer MUST fully initialize exactly `payload_len` bytes of the
    /// slice it receives (or return `Err`). On `Err`, no cell pointer or header
    /// update is published; bytes in the unreferenced free-space slot may have
    /// been touched, but the page still describes the pre-call cell set.
    fn try_append_table_leaf_payload_in_place_no_overflow_mutate_only_with_writer<W>(
        usable_size: u32,
        leaf_page_no: PageNumber,
        page_data: &mut PageData,
        header: &mut BtreePageHeader,
        rowid: i64,
        payload_len: usize,
        writer: W,
    ) -> Result<Option<(u16, u16)>>
    where
        W: FnOnce(&mut [u8]) -> Result<()>,
    {
        let payload_size = u32::try_from(payload_len).map_err(|_| FrankenError::TooBig)?;
        let local_size =
            cell::local_payload_size(payload_size, usable_size, cell::BtreePageType::LeafTable)
                as usize;
        let local_size = local_size.min(payload_len);
        if local_size < payload_len {
            // Overflow required: callers with a writer-based payload don't
            // support overflow today — fall back to the generic path.
            return Ok(None);
        }

        let mut payload_varint = [0u8; 9];
        let payload_varint_len = write_varint(&mut payload_varint, u64::from(payload_size));
        let mut rowid_varint = [0u8; 9];
        let rowid_len = write_varint(&mut rowid_varint, u64::from_ne_bytes(rowid.to_ne_bytes()));
        let cell_len = payload_varint_len + rowid_len + local_size;

        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let insert_idx = header.cell_count;
        let content_offset = header.content_offset(usable_size);
        let Some(new_content_offset) = content_offset.checked_sub(cell_len) else {
            return Ok(None);
        };

        let ptr_array_end = header_offset
            + usize::from(header.page_type.header_size())
            + (usize::from(insert_idx) + 1) * 2;
        if ptr_array_end > new_content_offset {
            return Ok(None);
        }

        let new_cell_offset =
            u16::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "new leaf cell offset {} exceeds u16 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        let new_cell_count =
            insert_idx
                .checked_add(1)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "leaf page {} cell count overflow while appending",
                        leaf_page_no.get()
                    ),
                })?;
        let ptr_offset = header_offset
            + usize::from(header.page_type.header_size())
            + usize::from(insert_idx) * 2;

        let new_cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "new leaf content offset {} exceeds u32 range on page {}",
                    new_content_offset,
                    leaf_page_no.get()
                ),
            })?;
        let mutate_start = instrumentation::profile_start();
        {
            let page_bytes = page_data.as_bytes_mut();
            let mut write_offset = new_content_offset;
            page_bytes[write_offset..write_offset + payload_varint_len]
                .copy_from_slice(&payload_varint[..payload_varint_len]);
            write_offset += payload_varint_len;
            page_bytes[write_offset..write_offset + rowid_len]
                .copy_from_slice(&rowid_varint[..rowid_len]);
            write_offset += rowid_len;
            let payload_slice = &mut page_bytes[write_offset..write_offset + local_size];
            writer(payload_slice)?;
            header.cell_count = new_cell_count;
            header.cell_content_offset = new_cell_content_offset;
            page_bytes[ptr_offset..ptr_offset + 2].copy_from_slice(&new_cell_offset.to_be_bytes());
            header.write(page_bytes, header_offset);
        }
        instrumentation::record_fast_table_leaf_payload_append_mutate(mutate_start);
        Ok(Some((insert_idx, new_cell_offset)))
    }

    /// Writer-callback analogue of
    /// [`Self::try_append_table_leaf_payload_in_place_no_overflow`].
    ///
    /// See the `_mutate_only_with_writer` helper for the payload-writing
    /// contract; this wrapper additionally stages the mutated page image
    /// through the pager.
    #[allow(clippy::too_many_arguments)]
    fn try_append_table_leaf_payload_in_place_no_overflow_with_writer<W>(
        &mut self,
        cx: &Cx,
        leaf_page_no: PageNumber,
        page_data: &mut PageData,
        header: &mut BtreePageHeader,
        rowid: i64,
        payload_len: usize,
        writer: W,
    ) -> Result<Option<(u16, u16)>>
    where
        W: FnOnce(&mut [u8]) -> Result<()>,
    {
        let Some((insert_idx, new_cell_offset)) =
            Self::try_append_table_leaf_payload_in_place_no_overflow_mutate_only_with_writer(
                self.usable_size,
                leaf_page_no,
                page_data,
                header,
                rowid,
                payload_len,
                writer,
            )?
        else {
            return Ok(None);
        };
        let staged_page = page_data.clone();
        let stage_start = instrumentation::profile_start();
        self.pager.write_page_data(cx, leaf_page_no, staged_page)?;
        instrumentation::record_fast_table_leaf_payload_append_stage(stage_start);
        Ok(Some((insert_idx, new_cell_offset)))
    }

    /// Balance the tree after an insert when the leaf page is full.
    ///
    /// `cell_data` is the cell that didn't fit. `insert_idx` is where within
    /// the leaf's cell array it should be placed.
    fn balance_for_insert(&mut self, cx: &Cx, cell_data: &[u8], insert_idx: u16) -> Result<()> {
        let depth = self.stack.len();
        if depth == 0 {
            return Err(FrankenError::internal("cursor stack empty during balance"));
        }
        let path_root = self.stack[0].page_no;
        if path_root != self.root_page {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "insert balance cursor path starts at page {} instead of root {} (depth {})",
                    path_root.get(),
                    self.root_page.get(),
                    depth
                ),
            });
        }

        // A balance can split, merge, or free any leaf remembered by the
        // table-seek cache. Invalidate those topology-dependent anchors before
        // the first structural write so an error or a post-balance re-seek can
        // never reload a page that this operation already retired.
        self.clear_seek_cache();

        if depth == 1 {
            // Leaf is the root — push root down first.
            balance::balance_deeper(
                cx,
                &mut self.pager,
                self.root_page,
                self.usable_size,
                self.page_size,
            )?;
            self.note_split_event();
            // Root is now an interior page with 1 child at index 0.
            let outcome = balance::balance_nonroot(
                cx,
                &mut self.pager,
                self.root_page,
                0,
                &[cell_data.to_vec()],
                insert_idx as usize,
                self.usable_size,
                self.page_size,
                true,
            )?;
            if matches!(outcome, balance::BalanceResult::Split { .. }) {
                return Err(FrankenError::internal(
                    "root balance unexpectedly returned split requiring parent update",
                ));
            }
        } else {
            let parent_page_no = self.stack[depth - 2].page_no;
            let child_idx = self.stack[depth - 2].cell_idx as usize;
            let parent_is_root = parent_page_no == self.root_page;

            // Attempt balance_quick optimization for sequential inserts.
            // This avoids full 3-sibling rewrites when just appending to the right.
            let leaf_entry = &self.stack[depth - 1];
            let parent_entry = &self.stack[depth - 2];

            if leaf_entry.header.page_type == cell::BtreePageType::LeafTable
                && insert_idx == leaf_entry.header.cell_count
                && child_idx == parent_entry.header.cell_count as usize
            {
                if let Some((_, n)) = read_varint(cell_data) {
                    if let Some((rowid, _)) = read_varint(&cell_data[n..]) {
                        #[allow(clippy::cast_possible_wrap)]
                        let rowid = rowid as i64;
                        let divider_rowid = self
                            .rightmost_leaf_cache
                            .as_ref()
                            .filter(|cached| cached.page_no == leaf_entry.page_no)
                            .map(|cached| cached.rowid)
                            .unwrap_or_else(|| {
                                Self::table_leaf_rowid_at(
                                    leaf_entry,
                                    leaf_entry.header.cell_count.saturating_sub(1),
                                )
                                .unwrap_or_else(|_| rowid.saturating_sub(1))
                            });
                        let quick_balance_start = instrumentation::profile_start();
                        match balance::balance_quick_known_divider_rowid(
                            cx,
                            &mut self.pager,
                            parent_page_no,
                            leaf_entry.page_no,
                            cell_data,
                            divider_rowid,
                            self.usable_size,
                            self.page_size,
                        ) {
                            Ok(Some(result)) => {
                                instrumentation::record_quick_balance_attempt(
                                    quick_balance_start,
                                    true,
                                );
                                self.note_split_event();
                                self.stack.clear();
                                self.at_eof = true;
                                self.last_known_depth = Some(depth);
                                self.rightmost_leaf_cache = Some(RightmostLeafCacheEntry {
                                    page_no: result.new_pgno,
                                    rowid,
                                    tree_depth: depth,
                                    parent_page: Some(parent_page_no),
                                    page_data: result.new_page_data,
                                    header: result.new_header,
                                    cell_pointers: vec![result.new_cell_ptr],
                                });
                                return Ok(());
                            }
                            Ok(None) => {
                                instrumentation::record_quick_balance_attempt(
                                    quick_balance_start,
                                    false,
                                );
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            }

            let mut outcome = if leaf_entry.header.page_type == cell::BtreePageType::LeafTable {
                let local_split_start = instrumentation::profile_start();
                match balance::balance_table_leaf_local_split(
                    cx,
                    &mut self.pager,
                    parent_page_no,
                    child_idx,
                    leaf_entry.page_no,
                    cell_data,
                    insert_idx as usize,
                    self.usable_size,
                    self.page_size,
                    parent_is_root,
                )? {
                    Some(outcome) => {
                        instrumentation::record_local_split_attempt(local_split_start, true);
                        self.note_split_event();
                        outcome
                    }
                    None => {
                        instrumentation::record_local_split_attempt(local_split_start, false);
                        let nonroot_start = instrumentation::profile_start();
                        let outcome = balance::balance_nonroot(
                            cx,
                            &mut self.pager,
                            parent_page_no,
                            child_idx,
                            &[cell_data.to_vec()],
                            insert_idx as usize,
                            self.usable_size,
                            self.page_size,
                            parent_is_root,
                        )?;
                        instrumentation::record_nonroot_balance(nonroot_start);
                        outcome
                    }
                }
            } else {
                let nonroot_start = instrumentation::profile_start();
                let outcome = balance::balance_nonroot(
                    cx,
                    &mut self.pager,
                    parent_page_no,
                    child_idx,
                    &[cell_data.to_vec()],
                    insert_idx as usize,
                    self.usable_size,
                    self.page_size,
                    parent_is_root,
                )?;
                instrumentation::record_nonroot_balance(nonroot_start);
                outcome
            };

            // If balancing split the parent page, propagate the split up the
            // cursor stack by updating each ancestor in turn.
            let mut parent_level = depth - 2; // stack index of the split page
            let mut split_page_no = parent_page_no;
            while let balance::BalanceResult::Split {
                new_pgnos,
                new_dividers,
            } = outcome
            {
                // Once we start rewriting parent links, finish propagating the
                // split so we do not return with a half-rebalanced tree.
                self.note_split_event();
                if parent_level == 0 {
                    return Err(FrankenError::internal(
                        "balance split bubbled above root (unexpected)",
                    ));
                }

                let ancestor_page_no = self.stack[parent_level - 1].page_no;
                let ancestor_child_idx = usize::from(self.find_child_slot_by_page_no(
                    cx,
                    ancestor_page_no,
                    split_page_no,
                )?);
                let ancestor_is_root = ancestor_page_no == self.root_page;

                outcome = balance::apply_child_replacement(
                    cx,
                    &mut self.pager,
                    ancestor_page_no,
                    self.usable_size,
                    self.page_size,
                    ancestor_child_idx,
                    1, // Replacing a single child page with its split siblings.
                    &new_pgnos,
                    &new_dividers,
                    ancestor_is_root,
                )?;

                split_page_no = ancestor_page_no;
                parent_level -= 1;
            }
        }

        // Tree structure changed — invalidate the cursor stack.
        self.stack.clear();
        self.at_eof = true;
        Ok(())
    }

    /// Balance the tree after deleting from a non-root leaf.
    ///
    /// For a root leaf page (single-level tree), no balancing is required.
    ///
    /// This propagates the rebalance upward through the tree.  When
    /// merging siblings at one level reduces the parent to zero cells,
    /// the parent itself becomes a candidate for merging with *its*
    /// siblings at the next level up.  At the root level,
    /// `apply_child_replacement` triggers `balance_shallower` which
    /// copies the sole remaining child into the root page, reducing the
    /// tree depth by one (the inverse of `balance_deeper`).
    fn balance_for_delete(&mut self, cx: &Cx) -> Result<()> {
        let depth = self.stack.len();
        if depth == 0 {
            return Err(FrankenError::internal(
                "cursor stack empty during delete balance",
            ));
        }
        let path_root = self.stack[0].page_no;
        if path_root != self.root_page {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "delete balance cursor path starts at page {} instead of root {} (depth {})",
                    path_root.get(),
                    self.root_page.get(),
                    depth
                ),
            });
        }
        if depth == 1 {
            return Ok(());
        }

        // The positioning seek immediately before DELETE may have populated
        // the cache with the leaf that is about to be merged and freed. Clear
        // it before balancing: DELETE's successor re-seek happens after this
        // method returns and must descend through the new tree, not reload the
        // retired leaf (GH-123 / bd-h1kvh).
        self.clear_seek_cache();

        // K2 (bd-yywuv): this deferred rebalance fixup runs only when a leaf
        // empties, not per DELETE. Count it so tests can prove the deferral
        // (invocations stay far below the DELETE count).
        instrumentation::record_balance_for_delete();

        // Start at the leaf's parent and propagate upward as needed.
        let mut level = depth - 2;

        loop {
            // Deletion has already mutated the tree by the time rebalance
            // begins, so we intentionally finish this fixup even if the
            // caller's context becomes cancelled mid-flight.
            let parent_page_no = self.stack[level].page_no;
            let child_idx = usize::from(self.stack[level].cell_idx);
            let parent_is_root = parent_page_no == self.root_page;

            self.note_merge_event();
            let mut outcome = balance::balance_nonroot(
                cx,
                &mut self.pager,
                parent_page_no,
                child_idx,
                &[],
                0,
                self.usable_size,
                self.page_size,
                parent_is_root,
            )?;

            // If balancing split the parent page, propagate the split up the
            // cursor stack by updating each ancestor in turn.
            let mut split_level = level;
            let mut split_page_no = parent_page_no;
            while let balance::BalanceResult::Split {
                new_pgnos,
                new_dividers,
            } = outcome
            {
                // Keep split propagation atomic with respect to interrupts for
                // the same reason as balance_for_insert above.
                self.note_split_event();
                if split_level == 0 {
                    return Err(FrankenError::internal(
                        "balance split bubbled above root (unexpected)",
                    ));
                }

                let ancestor_page_no = self.stack[split_level - 1].page_no;
                let ancestor_child_idx = usize::from(self.find_child_slot_by_page_no(
                    cx,
                    ancestor_page_no,
                    split_page_no,
                )?);
                let ancestor_is_root = ancestor_page_no == self.root_page;

                outcome = balance::apply_child_replacement(
                    cx,
                    &mut self.pager,
                    ancestor_page_no,
                    self.usable_size,
                    self.page_size,
                    ancestor_child_idx,
                    1, // Replacing a single child page with its split siblings.
                    &new_pgnos,
                    &new_dividers,
                    ancestor_is_root,
                )?;

                split_page_no = ancestor_page_no;
                split_level -= 1;
            }

            // If we just balanced at the root level, we are done.
            // balance_shallower (called from apply_child_replacement)
            // already handled the 0-cell root case.
            if parent_is_root || level == 0 {
                break;
            }

            // Check whether the parent now has zero cells — if so, it
            // needs to be merged with its siblings at the next level up.
            let parent_data = self.pager.read_btree_page_data(cx, parent_page_no)?;
            let parent_header = cell::parse_page_header(parent_data.as_bytes(), parent_page_no)?;

            if parent_header.cell_count == 0 && parent_header.page_type.is_interior() {
                level -= 1;
            } else {
                break;
            }
        }

        // Tree shape may change after balancing.
        self.stack.clear();
        self.at_eof = true;
        Ok(())
    }

    fn replace_interior_cell(&mut self, cx: &Cx, new_payload: &[u8]) -> Result<bool> {
        if self.stack.is_empty() || self.at_eof {
            return Err(FrankenError::internal(
                "cursor at EOF during interior replace",
            ));
        }
        let top =
            self.stack.last().cloned().ok_or_else(|| {
                FrankenError::internal("cursor stack empty during interior replace")
            })?;
        let page_no = top.page_no;
        let cell_idx = top.cell_idx;

        let cell_ref = self.parse_cell_at(&top, cell_idx)?;
        let left_child = cell_ref
            .left_child
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "interior cell missing left child pointer".to_owned(),
            })?;

        // Encode the new cell.
        let mut new_cell = Vec::new();
        new_cell.extend_from_slice(&left_child.get().to_be_bytes());

        let payload_size = u32::try_from(new_payload.len()).map_err(|_| FrankenError::TooBig)?;
        let mut varint = [0u8; 9];
        let p_len = write_varint(&mut varint, u64::from(payload_size));
        new_cell.extend_from_slice(&varint[..p_len]);

        let local_size =
            cell::local_payload_size(payload_size, self.usable_size, top.header.page_type) as usize;
        let local_size = local_size.min(new_payload.len());

        new_cell.extend_from_slice(&new_payload[..local_size]);
        let new_overflow_head = if local_size < new_payload.len() {
            let first_overflow =
                self.write_overflow_chain_for_insert(cx, &new_payload[local_size..])?;
            new_cell.extend_from_slice(&first_overflow.get().to_be_bytes());
            Some(first_overflow)
        } else {
            None
        };
        instrumentation::record_interior_cell_rebuild(new_cell.len());

        // Remove old cell from page and try to insert new cell.
        let mut page_data = self.pager.read_btree_page_data(cx, page_no)?;
        let header_offset = cell::header_offset_for_page(page_no);
        let mut header = cell::parse_page_header(page_data.as_bytes(), page_no)?;
        let mut ptrs = cell::read_cell_pointers(page_data.as_bytes(), &header, header_offset)?;
        let cell_idx_usize = usize::from(cell_idx);
        if cell_idx_usize >= ptrs.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior replace index {} out of bounds for page {} with {} cells",
                    cell_idx,
                    page_no,
                    ptrs.len()
                ),
            });
        }

        let old_overflow = cell_ref.overflow_page;
        ptrs.remove(cell_idx_usize);

        // Defragment.
        let mut new_content_offset = self.usable_size as usize;
        let ptr_array_end =
            header_offset + usize::from(header.page_type.header_size()) + ptrs.len() * 2;

        let mut cells_to_move = Vec::with_capacity(ptrs.len());
        for (i, &off) in ptrs.iter().enumerate() {
            let ptr = off as usize;
            // OPT-A3: compute on-page size directly from the cell's
            // varints — we're defragmenting, so we only need the byte
            // extent of the cell, not a full CellRef. This avoids the
            // overflow-page PageNumber::new validation on every cell.
            let size = cell::cell_on_page_size_fast(
                page_data.as_bytes(),
                ptr,
                header.page_type,
                self.usable_size,
            )?;
            cells_to_move.push((ptr, size, i));
        }

        // Sort by ptr descending so we can shift right safely without overwriting unread data.
        // OPT-7 follow-up: reuse the size-dispatched helper used by
        // `remove_cell_from_leaf`; the typical N here is also small (cells
        // per interior page), so insertion sort wins on the fast path and
        // falls through to std above the crossover.
        sort_cells_desc_by_ptr(&mut cells_to_move);

        for (ptr, size, i) in cells_to_move {
            new_content_offset = new_content_offset.checked_sub(size).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "cell size overflow during interior defragmentation".to_owned(),
                }
            })?;
            if new_content_offset < ptr_array_end {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "cell content overlaps pointer array during defragmentation".to_owned(),
                });
            }
            if new_content_offset != ptr {
                page_data
                    .as_bytes_mut()
                    .copy_within(ptr..ptr + size, new_content_offset);
            }
            ptrs[i] = new_content_offset as u16;
        }

        // Check if new cell fits.
        let ptr_array_end_with_new = ptr_array_end + 2;
        let fits = ptr_array_end_with_new
            .checked_add(new_cell.len())
            .is_some_and(|needed| new_content_offset >= needed);
        if fits {
            new_content_offset -= new_cell.len();
            let new_end = new_content_offset + new_cell.len();
            ptrs.insert(
                cell_idx_usize,
                u16::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: "new interior cell offset exceeds u16 range".to_owned(),
                })?,
            );

            header.cell_count =
                u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: "interior cell count exceeds u16 range".to_owned(),
                })?;
            header.cell_content_offset =
                u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: "interior content offset exceeds u32 range".to_owned(),
                })?;
            {
                let page_bytes = page_data.as_bytes_mut();
                page_bytes[new_content_offset..new_end].copy_from_slice(&new_cell);
                header.write(page_bytes, header_offset);
                cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
            }

            self.pager.write_page_data(cx, page_no, page_data)?;
            let mut refreshed = self.reload_page_fresh(cx, page_no)?;
            refreshed.cell_idx = cell_idx;
            if let Some(top) = self.stack.last_mut() {
                *top = refreshed;
            }
            self.at_eof = false;
            if let Some(first) = old_overflow {
                self.free_overflow_chain(cx, first)?;
            }
            return Ok(false);
        }

        // It doesn't fit! We must delete the old cell and balance.
        header.cell_count =
            u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "interior cell count exceeds u16 range".to_owned(),
            })?;
        header.cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "interior content offset exceeds u32 range".to_owned(),
            })?;
        {
            let page_bytes = page_data.as_bytes_mut();
            header.write(page_bytes, header_offset);
            cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        }

        self.pager.write_page_data(cx, page_no, page_data)?;

        // Now we must insert `new_cell` at `cell_idx`, which will trigger a structural rebalance.
        let balance_result = self.balance_for_insert(cx, &new_cell, cell_idx);
        if balance_result.is_err() {
            if let Some(first) = new_overflow_head {
                let _ = self.free_overflow_chain(cx, first);
            }
        }
        balance_result?;

        // The old separator remains the authoritative owner of its overflow
        // chain until the structural replacement succeeds. Retiring that
        // chain before `balance_for_insert` commits would let an error return
        // with pages freed while the old cell is still the rollback source.
        if let Some(first) = old_overflow {
            self.free_overflow_chain(cx, first)?;
        }

        Ok(true)
    }

    /// Insert a freeblock into the page's freeblock chain in ascending offset
    /// order, coalescing with adjacent freeblocks as required by the SQLite
    /// on-disk format.
    ///
    /// C sqlite3's `btreeComputeFreeSpace()` enforces:
    ///  - Freeblocks are in strictly ascending offset order.
    ///  - Consecutive freeblocks must be separated by more than 3 bytes;
    ///    adjacent freeblocks must be merged.
    ///  - The first freeblock must be at an offset >= `cell_content_offset`.
    ///
    /// This function handles all three constraints.
    #[allow(dead_code)] // Kept for future freeblock-chain experiments; delete path uses defrag for safety.
    fn insert_freeblock_sorted_coalesced(
        page_bytes: &mut [u8],
        header: &mut cell::BtreePageHeader,
        new_offset: usize,
        new_size: usize,
        usable_limit: usize,
        page_no: PageNumber,
    ) -> Result<()> {
        debug_assert!(new_size >= 4);

        // Walk the freeblock chain to find (prev_fb, next_fb) such that
        // prev_fb.offset < new_offset < next_fb.offset.
        // prev_fb == None means the new block goes before the first freeblock.
        let mut prev_fb: Option<usize> = None; // offset of previous freeblock
        let mut prev_fb_size: usize = 0;
        let mut cur_fb = usize::from(header.first_freeblock);

        while cur_fb != 0 && cur_fb < new_offset {
            if cur_fb + 4 > usable_limit {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock at offset {} extends past usable space on page {}",
                        cur_fb,
                        page_no.get()
                    ),
                });
            }
            prev_fb_size = usize::from(u16::from_be_bytes([
                page_bytes[cur_fb + 2],
                page_bytes[cur_fb + 3],
            ]));
            prev_fb = Some(cur_fb);
            cur_fb = usize::from(u16::from_be_bytes([
                page_bytes[cur_fb],
                page_bytes[cur_fb + 1],
            ]));
        }

        // cur_fb is either 0 (end of chain) or the next freeblock after new_offset.
        // Read next_fb's size if it exists.
        let next_fb = cur_fb;
        let next_fb_size = if next_fb != 0 && next_fb + 4 <= usable_limit {
            usize::from(u16::from_be_bytes([
                page_bytes[next_fb + 2],
                page_bytes[next_fb + 3],
            ]))
        } else {
            0
        };
        // next_fb's successor in the chain (for linking after merge).
        let next_fb_next = if next_fb != 0 && next_fb + 2 <= usable_limit {
            u16::from_be_bytes([page_bytes[next_fb], page_bytes[next_fb + 1]])
        } else {
            0u16
        };

        // Determine adjacency for coalescing.
        let merge_prev = prev_fb.is_some_and(|p| p + prev_fb_size == new_offset);
        let merge_next = next_fb != 0 && new_offset + new_size == next_fb;

        if merge_prev && merge_next {
            // The new block bridges prev and next — merge all three.
            let p = prev_fb.expect("merge_prev implies prev_fb.is_some()");
            let merged_size = prev_fb_size + new_size + next_fb_size;
            let merged_size_u16 =
                u16::try_from(merged_size).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "merged freeblock size {} exceeds u16 range on page {}",
                        merged_size,
                        page_no.get()
                    ),
                })?;
            // prev now spans all three regions; link to next_fb's successor.
            page_bytes[p..p + 2].copy_from_slice(&next_fb_next.to_be_bytes());
            page_bytes[p + 2..p + 4].copy_from_slice(&merged_size_u16.to_be_bytes());
            // Zero the interior (optional, for cleanliness).
            if merged_size > 4 {
                page_bytes[p + 4..p + merged_size].fill(0);
            }
        } else if merge_prev {
            // Extend prev to absorb the new block.
            let p = prev_fb.expect("merge_prev implies prev_fb.is_some()");
            let merged_size = prev_fb_size + new_size;
            let merged_size_u16 =
                u16::try_from(merged_size).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "merged freeblock size {} exceeds u16 range on page {}",
                        merged_size,
                        page_no.get()
                    ),
                })?;
            page_bytes[p + 2..p + 4].copy_from_slice(&merged_size_u16.to_be_bytes());
            // Zero newly absorbed region (optional).
            if p + prev_fb_size + new_size > p + 4 {
                let zero_start = (p + 4).max(p + prev_fb_size);
                page_bytes[zero_start..p + merged_size].fill(0);
            }
        } else if merge_next {
            // Extend the new block to absorb next_fb.
            let merged_size = new_size + next_fb_size;
            let merged_size_u16 =
                u16::try_from(merged_size).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "merged freeblock size {} exceeds u16 range on page {}",
                        merged_size,
                        page_no.get()
                    ),
                })?;
            let new_offset_u16 =
                u16::try_from(new_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock offset {} exceeds u16 range on page {}",
                        new_offset,
                        page_no.get()
                    ),
                })?;
            // The merged block starts at new_offset, links to next_fb's successor.
            page_bytes[new_offset..new_offset + 2].copy_from_slice(&next_fb_next.to_be_bytes());
            page_bytes[new_offset + 2..new_offset + 4]
                .copy_from_slice(&merged_size_u16.to_be_bytes());
            if merged_size > 4 {
                page_bytes[new_offset + 4..new_offset + merged_size].fill(0);
            }
            // Update the pointer from prev (or header) to point to new_offset.
            if let Some(p) = prev_fb {
                page_bytes[p..p + 2].copy_from_slice(&new_offset_u16.to_be_bytes());
            } else {
                header.first_freeblock = new_offset_u16;
            }
        } else {
            // No coalescing needed — insert as a standalone freeblock.
            let new_offset_u16 =
                u16::try_from(new_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock offset {} exceeds u16 range on page {}",
                        new_offset,
                        page_no.get()
                    ),
                })?;
            let freeblock_size_u16 =
                u16::try_from(new_size).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "freeblock size {} exceeds u16 range on page {}",
                        new_size,
                        page_no.get()
                    ),
                })?;
            let next_fb_u16 =
                u16::try_from(next_fb).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "next freeblock offset {} exceeds u16 range on page {}",
                        next_fb,
                        page_no.get()
                    ),
                })?;
            page_bytes[new_offset..new_offset + 2].copy_from_slice(&next_fb_u16.to_be_bytes());
            page_bytes[new_offset + 2..new_offset + 4]
                .copy_from_slice(&freeblock_size_u16.to_be_bytes());
            if new_size > 4 {
                page_bytes[new_offset + 4..new_offset + new_size].fill(0);
            }
            // Update the pointer from prev (or header) to point to new_offset.
            if let Some(p) = prev_fb {
                page_bytes[p..p + 2].copy_from_slice(&new_offset_u16.to_be_bytes());
            } else {
                header.first_freeblock = new_offset_u16;
            }
        }

        Ok(())
    }

    fn remove_table_cell_from_leaf_deferred(&mut self, cx: &Cx) -> Result<(PageNumber, u16)> {
        let depth = self.stack.len();
        if depth == 0 || self.at_eof {
            return Err(FrankenError::internal("cursor at EOF during remove"));
        }

        let (overflow_head, delete_idx, leaf_page_no) = {
            let top = &self.stack[depth - 1];
            if top.header.page_type != cell::BtreePageType::LeafTable {
                return Err(FrankenError::internal(
                    "remove_table_cell_from_leaf_deferred requires a table leaf page",
                ));
            }

            let delete_idx = usize::from(top.cell_idx);
            if delete_idx >= top.cell_pointers.len() {
                return Err(FrankenError::internal("cursor position out of bounds"));
            }
            let cell_ref = self.parse_cell_at(top, top.cell_idx)?;
            (cell_ref.overflow_page, delete_idx, top.page_no)
        };

        let top = &self.stack[depth - 1];
        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let mut page_data = top.page_data.clone();
        let mut header = top.header;
        let mut ptrs = take_pooled_cell_pointers();
        ptrs.extend_from_slice(&top.cell_pointers);
        let original_len = ptrs.len();
        if delete_idx >= original_len {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "delete_idx {} out of bounds for page {} with {} cells",
                    delete_idx,
                    leaf_page_no,
                    ptrs.len()
                ),
            });
        }
        let compact_cell_area = header.first_freeblock == 0
            && header.fragmented_free_bytes == 0
            && ptrs.iter().copied().min().is_some_and(|min_ptr| {
                usize::from(min_ptr) == header.content_offset(self.usable_size)
            });

        // Keep table-leaf deletes conservative: compact the remaining cells
        // immediately instead of maintaining an incremental freeblock chain.
        // Small mixed INSERT/UPDATE/DELETE churn can otherwise leave 1-3 byte
        // gaps between freeblocks, which SQLite's btreeComputeFreeSpace()
        // reports as "free space corruption". Index leaves already use this
        // defragment-on-delete strategy; table leaves should prefer the same
        // SQLite-compatible shape over a fragile micro-optimization.
        let ptr_array_end =
            header_offset + usize::from(header.page_type.header_size()) + (ptrs.len() - 1) * 2;

        if compact_cell_area && cell_ptrs_are_descending(&ptrs) {
            let deleted_ptr = usize::from(ptrs[delete_idx]);
            let deleted_upper = if delete_idx == 0 {
                self.usable_size as usize
            } else {
                usize::from(ptrs[delete_idx - 1])
            };
            if deleted_ptr >= deleted_upper {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "compact table leaf cell offsets are not monotone".to_owned(),
                });
            }
            let deleted_size = deleted_upper - deleted_ptr;
            let old_content_offset = header.content_offset(self.usable_size);
            if old_content_offset > deleted_ptr {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "compact table leaf content offset exceeds deleted cell".to_owned(),
                });
            }
            let new_content_offset =
                old_content_offset
                    .checked_add(deleted_size)
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "table leaf cell size overflow during delete defragmentation"
                            .to_owned(),
                    })?;
            if new_content_offset > self.usable_size as usize || new_content_offset < ptr_array_end
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "table leaf cell content overlaps pointer array during delete defragmentation"
                        .to_owned(),
                });
            }

            let page_bytes = page_data.as_bytes_mut();
            if old_content_offset < deleted_ptr {
                page_bytes.copy_within(old_content_offset..deleted_ptr, new_content_offset);
            }
            for ptr_slot in ptrs.iter_mut().skip(delete_idx + 1) {
                let adjusted = usize::from(*ptr_slot) + deleted_size;
                *ptr_slot = u16::try_from(adjusted).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell offset {} exceeds u16 range on page {}",
                        adjusted,
                        leaf_page_no.get()
                    ),
                })?;
            }
            ptrs.remove(delete_idx);
            header.first_freeblock = 0;
            header.fragmented_free_bytes = 0;
            header.cell_content_offset =
                u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell content offset {} exceeds u32 range on page {}",
                        new_content_offset,
                        leaf_page_no.get()
                    ),
                })?;

            header.cell_count =
                u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf page {} cell count exceeds u16 range during delete",
                        leaf_page_no.get()
                    ),
                })?;

            header.write(page_bytes, header_offset);
            cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        } else if compact_cell_area && self.usable_size <= 4096 {
            let deleted_ptr = usize::from(ptrs[delete_idx]);

            // Compact table leaves have a contiguous physical cell-content
            // interval. The deleted cell's byte extent is bounded by the next
            // higher physical cell pointer (or the usable page end), independent
            // of logical/key pointer order. Updates often disturb pointer order,
            // so use this linear physical-neighbor scan instead of sorting all
            // cell pointers just to recover the same interval.
            let mut seen_offsets = [0_u64; 64];
            let mut deleted_upper = self.usable_size as usize;
            for (idx, &off) in ptrs.iter().enumerate() {
                let ptr = usize::from(off);
                if ptr >= self.usable_size as usize {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "compact table leaf cell offset exceeds usable page size"
                            .to_owned(),
                    });
                }
                let seen_word = &mut seen_offsets[ptr / 64];
                let seen_bit = 1_u64 << (ptr % 64);
                if (*seen_word & seen_bit) != 0 {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail:
                            "compact table leaf cell offsets are not monotone: duplicate offset"
                                .to_owned(),
                    });
                }
                *seen_word |= seen_bit;

                if idx != delete_idx && ptr > deleted_ptr && ptr < deleted_upper {
                    deleted_upper = ptr;
                }
            }

            if deleted_ptr >= deleted_upper {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "compact table leaf cell offsets are not monotone".to_owned(),
                });
            }
            let deleted_size = deleted_upper - deleted_ptr;
            let old_content_offset = header.content_offset(self.usable_size);
            if old_content_offset > deleted_ptr {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "compact table leaf content offset exceeds deleted cell".to_owned(),
                });
            }
            let new_content_offset =
                old_content_offset
                    .checked_add(deleted_size)
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "table leaf cell size overflow during delete defragmentation"
                            .to_owned(),
                    })?;
            if new_content_offset > self.usable_size as usize || new_content_offset < ptr_array_end
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "table leaf cell content overlaps pointer array during delete defragmentation"
                        .to_owned(),
                });
            }
            for ptr_slot in &mut ptrs {
                let ptr = usize::from(*ptr_slot);
                if ptr < deleted_ptr {
                    let adjusted = ptr.checked_add(deleted_size).ok_or_else(|| {
                        FrankenError::DatabaseCorrupt {
                            detail: "table leaf cell offset overflow during delete".to_owned(),
                        }
                    })?;
                    *ptr_slot =
                        u16::try_from(adjusted).map_err(|_| FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "table leaf cell offset {} exceeds u16 range on page {}",
                                adjusted,
                                leaf_page_no.get()
                            ),
                        })?;
                }
            }

            let page_bytes = page_data.as_bytes_mut();
            if old_content_offset < deleted_ptr {
                page_bytes.copy_within(old_content_offset..deleted_ptr, new_content_offset);
            }
            ptrs.remove(delete_idx);
            header.first_freeblock = 0;
            header.fragmented_free_bytes = 0;
            header.cell_content_offset =
                u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell content offset {} exceeds u32 range on page {}",
                        new_content_offset,
                        leaf_page_no.get()
                    ),
                })?;

            header.cell_count =
                u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf page {} cell count exceeds u16 range during delete",
                        leaf_page_no.get()
                    ),
                })?;

            header.write(page_bytes, header_offset);
            cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        } else {
            let mut cells_to_move = std::mem::take(&mut self.defrag_cells_scratch);
            cells_to_move.clear();
            cells_to_move.reserve(ptrs.len());
            if compact_cell_area {
                for (original_idx, &off) in ptrs.iter().enumerate() {
                    let post_delete_idx = match original_idx.cmp(&delete_idx) {
                        std::cmp::Ordering::Less => original_idx,
                        std::cmp::Ordering::Equal => usize::MAX,
                        std::cmp::Ordering::Greater => original_idx - 1,
                    };
                    cells_to_move.push((usize::from(off), 0, post_delete_idx));
                }
            } else {
                ptrs.remove(delete_idx);
                for (i, &off) in ptrs.iter().enumerate() {
                    let ptr = usize::from(off);
                    // OPT-A3: size-only fast path, same as replace_interior_cell defrag.
                    let size = cell::cell_on_page_size_fast(
                        page_data.as_bytes(),
                        ptr,
                        header.page_type,
                        self.usable_size,
                    )?;
                    cells_to_move.push((ptr, size, i));
                }
            }
            // OPT-7 follow-up: size-dispatched sort for typical small N here too.
            sort_cells_desc_by_ptr(&mut cells_to_move);
            if compact_cell_area {
                let mut next_boundary = self.usable_size as usize;
                for cell in &mut cells_to_move {
                    if cell.0 >= next_boundary {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: "compact table leaf cell offsets are not monotone".to_owned(),
                        });
                    }
                    cell.1 = next_boundary - cell.0;
                    next_boundary = cell.0;
                }
                if next_boundary != header.content_offset(self.usable_size) {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "compact table leaf content offset does not match cell extent"
                            .to_owned(),
                    });
                }
                ptrs.remove(delete_idx);
            }

            {
                let page_bytes = page_data.as_bytes_mut();
                let mut new_content_offset = self.usable_size as usize;
                for &(ptr, size, i) in &cells_to_move {
                    if i == usize::MAX {
                        continue;
                    }
                    new_content_offset = new_content_offset.checked_sub(size).ok_or_else(|| {
                        FrankenError::DatabaseCorrupt {
                            detail: "table leaf cell size overflow during delete defragmentation"
                                .to_owned(),
                        }
                    })?;
                    if new_content_offset < ptr_array_end {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: "table leaf cell content overlaps pointer array during delete defragmentation"
                                .to_owned(),
                        });
                    }
                    if new_content_offset != ptr {
                        page_bytes.copy_within(ptr..ptr + size, new_content_offset);
                    }
                    ptrs[i] = u16::try_from(new_content_offset).map_err(|_| {
                        FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "table leaf cell offset {} exceeds u16 range on page {}",
                                new_content_offset,
                                leaf_page_no.get()
                            ),
                        }
                    })?;
                }

                header.first_freeblock = 0;
                header.fragmented_free_bytes = 0;
                header.cell_content_offset = u32::try_from(new_content_offset).map_err(|_| {
                    FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table leaf cell content offset {} exceeds u32 range on page {}",
                            new_content_offset,
                            leaf_page_no.get()
                        ),
                    }
                })?;

                header.cell_count =
                    u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table leaf page {} cell count exceeds u16 range during delete",
                            leaf_page_no.get()
                        ),
                    })?;

                header.write(page_bytes, header_offset);
                cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
            }
            self.defrag_cells_scratch = cells_to_move;
        }

        let refreshed_page_data = page_data.clone();
        let write_result = self.pager.write_page_data(cx, leaf_page_no, page_data);
        if let Err(error) = write_result {
            recycle_cell_pointers(ptrs);
            return Err(error);
        }

        let new_count = header.cell_count;
        let mutation_counter = Self::page_mutation_counter(&refreshed_page_data);
        let mut refreshed = StackEntry {
            page_no: leaf_page_no,
            page_data: refreshed_page_data,
            header,
            cell_pointers: ptrs,
            mutation_counter,
            cell_idx: 0,
        };
        if new_count == 0 {
            refreshed.cell_idx = 0;
            self.at_eof = true;
            self.stack[depth - 1] = refreshed;
        } else if delete_idx >= usize::from(new_count) {
            refreshed.cell_idx = new_count - 1;
            self.at_eof = false;
            self.stack[depth - 1] = refreshed;
            let eof_insert_stack = self.stack.clone();
            if !self.advance_next(cx)? {
                self.stack = eof_insert_stack;
                self.at_eof = true;
            }
        } else {
            refreshed.cell_idx = delete_idx as u16;
            self.at_eof = false;
            self.stack[depth - 1] = refreshed;
        }

        if let Some(first) = overflow_head {
            self.free_overflow_chain(cx, first)?;
        }

        Ok((leaf_page_no, new_count))
    }

    fn remove_cell_from_leaf(&mut self, cx: &Cx) -> Result<(PageNumber, u16)> {
        let depth = self.stack.len();
        if depth == 0 || self.at_eof {
            return Err(FrankenError::internal("cursor at EOF during remove"));
        }

        let (overflow_head, delete_idx, leaf_page_no) = {
            let top = &self.stack[depth - 1];
            if !top.header.page_type.is_leaf() {
                return Err(FrankenError::internal(
                    "remove_cell_from_leaf called on interior page",
                ));
            }

            let delete_idx = usize::from(top.cell_idx);
            if delete_idx >= top.cell_pointers.len() {
                return Err(FrankenError::internal("cursor position out of bounds"));
            }

            let cell_ref = self.parse_cell_at(top, top.cell_idx)?;
            (cell_ref.overflow_page, delete_idx, top.page_no)
        };

        // Identify overflow chain to free, but DO NOT free it yet.
        // We must remove the pointer from the leaf page first. If we freed
        // the chain first and then failed to update the leaf, the leaf would
        // contain a dangling pointer to a freed (and potentially reused) page,
        // causing corruption.
        //
        // If we update the leaf first and then fail to free the chain, we leak
        // pages but preserve database integrity. Leaks are recoverable (VACUUM);
        // corruption is not.
        let mut page_data = self.pager.read_btree_page_data(cx, leaf_page_no)?;
        let header_offset = cell::header_offset_for_page(leaf_page_no);
        let mut header = cell::parse_page_header(page_data.as_bytes(), leaf_page_no)?;
        // OPT-A4: reuse the cursor-owned defrag scratch buffers so repeated
        // DELETEs don't pay per-call `Vec::with_capacity` + allocator round-
        // trips. `read_cell_pointers_into` clears + reserves its caller's
        // buffer before filling, so passing in our retained `defrag_ptrs_
        // scratch` preserves the max capacity seen across all prior deletes.
        let mut ptrs = std::mem::take(&mut self.defrag_ptrs_scratch);
        let ptrs_read =
            cell::read_cell_pointers_into(page_data.as_bytes(), &header, header_offset, &mut ptrs);
        let cell_operation_error = ptrs_read.err();
        if let Some(err) = cell_operation_error {
            self.defrag_ptrs_scratch = ptrs;
            return Err(err);
        }
        if delete_idx >= ptrs.len() {
            let err = FrankenError::DatabaseCorrupt {
                detail: format!(
                    "delete_idx {} out of bounds for page {} with {} cells",
                    delete_idx,
                    leaf_page_no,
                    ptrs.len()
                ),
            };
            self.defrag_ptrs_scratch = ptrs;
            return Err(err);
        }
        ptrs.remove(delete_idx);

        // Defragment the page to reclaim the space used by the deleted cell.
        // This avoids maintaining a complex freeblock list and keeps fragmented_free_bytes at 0.
        let ptr_array_end =
            header_offset + usize::from(header.page_type.header_size()) + ptrs.len() * 2;

        // OPT-A4: reuse the cursor-owned cells-to-move scratch; clear keeps
        // the backing allocation.
        let mut cells_to_move = std::mem::take(&mut self.defrag_cells_scratch);
        cells_to_move.clear();
        cells_to_move.reserve(ptrs.len());
        for (i, &off) in ptrs.iter().enumerate() {
            let ptr = off as usize;
            // OPT-A3: defragmentation only needs the byte extent of each
            // cell, so compute size directly from its varints instead of
            // building a full CellRef.
            let size = match cell::cell_on_page_size_fast(
                page_data.as_bytes(),
                ptr,
                header.page_type,
                self.usable_size,
            ) {
                Ok(size) => size,
                Err(err) => {
                    self.defrag_ptrs_scratch = ptrs;
                    self.defrag_cells_scratch = cells_to_move;
                    return Err(err);
                }
            };
            cells_to_move.push((ptr, size, i));
        }

        // Sort by ptr descending so we can shift right safely without overwriting unread data.
        // N is typically 1..~80 (capped by cells-per-page); `sort_cells_desc_by_ptr`
        // uses a specialized insertion sort with an already-sorted fast path for small N
        // and falls through to `sort_unstable_by` above the empirical crossover.
        sort_cells_desc_by_ptr(&mut cells_to_move);

        {
            let mut new_content_offset = self.usable_size as usize;
            let mut defrag_err: Option<FrankenError> = None;
            let page_bytes = page_data.as_bytes_mut();
            for &(ptr, size, i) in &cells_to_move {
                match new_content_offset.checked_sub(size) {
                    Some(candidate) if candidate >= ptr_array_end => {
                        new_content_offset = candidate;
                        if new_content_offset != ptr {
                            page_bytes.copy_within(ptr..ptr + size, new_content_offset);
                        }
                        ptrs[i] = new_content_offset as u16;
                    }
                    Some(_) => {
                        defrag_err = Some(FrankenError::DatabaseCorrupt {
                            detail: "cell content overlaps pointer array during defragmentation"
                                .to_owned(),
                        });
                        break;
                    }
                    None => {
                        defrag_err = Some(FrankenError::DatabaseCorrupt {
                            detail: "cell size overflow during defragmentation".to_owned(),
                        });
                        break;
                    }
                }
            }
            if let Some(err) = defrag_err {
                self.defrag_ptrs_scratch = ptrs;
                self.defrag_cells_scratch = cells_to_move;
                return Err(err);
            }

            // Fill the now-unused space with zeros for cleanliness (optional, but good for reproducibility/debugging).
            if new_content_offset > ptr_array_end {
                page_bytes[ptr_array_end..new_content_offset].fill(0);
            }

            #[allow(clippy::cast_possible_truncation)]
            {
                header.cell_count = ptrs.len() as u16;
                header.cell_content_offset = new_content_offset as u32;
            }
            header.fragmented_free_bytes = 0;
            header.first_freeblock = 0;

            header.write(page_bytes, header_offset);
            cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        }
        let write_result = self.pager.write_page_data(cx, leaf_page_no, page_data);
        self.defrag_ptrs_scratch = ptrs;
        self.defrag_cells_scratch = cells_to_move;
        write_result?;

        // Refresh the stack entry.
        let mut refreshed = self.reload_page_fresh(cx, leaf_page_no)?;
        let new_count = refreshed.header.cell_count;
        if new_count == 0 {
            refreshed.cell_idx = 0;
            self.at_eof = true;
            self.stack[depth - 1] = refreshed;
        } else if delete_idx >= usize::from(new_count) {
            refreshed.cell_idx = new_count - 1;
            self.at_eof = false;
            self.stack[depth - 1] = refreshed;
            self.advance_next(cx)?;
        } else {
            #[allow(clippy::cast_possible_truncation)]
            {
                refreshed.cell_idx = delete_idx as u16;
            }
            self.at_eof = false;
            self.stack[depth - 1] = refreshed;
        }

        // Now it is safe to free the overflow chain.
        if let Some(first) = overflow_head {
            self.free_overflow_chain(cx, first)?;
        }

        Ok((leaf_page_no, new_count))
    }

    fn separator_ancestor_level_for_deleted_leaf_max(&self) -> Option<usize> {
        if self.stack.len() < 2 {
            return None;
        }

        for level in (0..self.stack.len().saturating_sub(1)).rev() {
            let entry = &self.stack[level];
            if usize::from(entry.cell_idx) < usize::from(entry.header.cell_count) {
                return Some(level);
            }
        }

        None
    }

    fn separator_repair_for_deleted_leaf_max(
        &self,
        leaf_entry: &StackEntry,
    ) -> Result<Option<(PageNumber, u16, i64)>> {
        if !self.is_table {
            return Ok(None);
        }
        if usize::from(leaf_entry.cell_idx) + 1 != usize::from(leaf_entry.header.cell_count) {
            return Ok(None);
        }
        if leaf_entry.header.cell_count <= 1 {
            return Ok(None);
        }

        let Some(level) = self.separator_ancestor_level_for_deleted_leaf_max() else {
            return Ok(None);
        };
        let separator = self
            .stack
            .get(level)
            .ok_or_else(|| FrankenError::internal("separator level out of bounds"))?;
        let predecessor_idx = leaf_entry
            .cell_idx
            .checked_sub(1)
            .ok_or_else(|| FrankenError::internal("last leaf cell has no predecessor"))?;
        // bd-9e3xf.4: predecessor lookup only needs the leaf-table rowid, so
        // skip the cell-slot cache round-trip and the overflow/local-size
        // bookkeeping that `CellRef::parse` computes for full cell access.
        // Same "caller only needs one field" pattern as commits 4b061dcc
        // (child_page_at) and 385591f8 (replace_separator left-child).
        let predecessor_offset = usize::from(Self::read_stack_entry_cell_pointer_inline(
            leaf_entry,
            predecessor_idx,
        )?);
        let new_rowid =
            CellRef::parse_leaf_table_rowid(leaf_entry.page_data.as_bytes(), predecessor_offset)?;

        Ok(Some((separator.page_no, separator.cell_idx, new_rowid)))
    }

    fn replace_table_interior_separator_rowid(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        separator_idx: u16,
        new_rowid: i64,
    ) -> Result<()> {
        let entry = self.reload_page_fresh(cx, page_no)?;
        if entry.header.page_type != cell::BtreePageType::InteriorTable {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "expected interior table page at page {}, found {:?}",
                    page_no, entry.header.page_type
                ),
            });
        }

        let separator_idx_usize = usize::from(separator_idx);
        if separator_idx_usize >= usize::from(entry.header.cell_count) {
            return Err(FrankenError::internal(format!(
                "separator index {} out of bounds for page {} with {} cells",
                separator_idx_usize, entry.page_no, entry.header.cell_count
            )));
        }

        // bd-k7zd7.3: extract the left-child pointer directly from the cell's
        // first 4 bytes instead of routing through `parse_cell_at` →
        // `CellRef::parse`. The separator cell is on an interior-table page
        // (verified above) and the caller only needs `left_child`; decoding
        // the rowid varint, computing local payload size, and checking
        // overflow bounds are all dead work on this path. Same principle as
        // commit 4b061dcc (`child_page_at` rewired to
        // `read_interior_child_inline`).
        let left_child = Self::read_interior_child_inline(&entry, separator_idx)?;

        let mut new_cell = [0_u8; 13];
        new_cell[0..4].copy_from_slice(&left_child.get().to_be_bytes());
        #[allow(clippy::cast_sign_loss)]
        let varint_len = write_varint(&mut new_cell[4..], new_rowid as u64);
        let new_cell = &new_cell[..4 + varint_len];

        let page_no = entry.page_no;
        let header_offset = cell::header_offset_for_page(page_no);
        let mut page_data = self.pager.read_btree_page_data(cx, page_no)?;
        let mut header = cell::parse_page_header(page_data.as_bytes(), page_no)?;
        let mut ptrs = cell::read_cell_pointers(page_data.as_bytes(), &header, header_offset)?;
        if separator_idx_usize >= ptrs.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "separator index {} out of bounds for page {} with {} cells",
                    separator_idx_usize,
                    page_no,
                    ptrs.len()
                ),
            });
        }
        ptrs.remove(separator_idx_usize);

        let ptr_array_end =
            header_offset + usize::from(header.page_type.header_size()) + ptrs.len() * 2;
        let mut cells_to_move = Vec::with_capacity(ptrs.len());
        for (i, &off) in ptrs.iter().enumerate() {
            let ptr = usize::from(off);
            // OPT-A3: size-only fast path for the separator-repair defrag.
            let size = cell::cell_on_page_size_fast(
                page_data.as_bytes(),
                ptr,
                header.page_type,
                self.usable_size,
            )?;
            cells_to_move.push((ptr, size, i));
        }
        // OPT-7 follow-up: same size-dispatched sort as above.
        sort_cells_desc_by_ptr(&mut cells_to_move);

        let mut new_content_offset = self.usable_size as usize;
        for (ptr, size, i) in cells_to_move {
            new_content_offset = new_content_offset.checked_sub(size).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "cell size overflow during separator repair".to_owned(),
                }
            })?;
            if new_content_offset < ptr_array_end {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "separator repair would overlap the pointer array".to_owned(),
                });
            }
            if new_content_offset != ptr {
                page_data
                    .as_bytes_mut()
                    .copy_within(ptr..ptr + size, new_content_offset);
            }
            ptrs[i] =
                u16::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: "separator repair cell offset exceeds u16 range".to_owned(),
                })?;
        }

        let ptr_array_end_with_new = ptr_array_end + 2;
        let fits = ptr_array_end_with_new
            .checked_add(new_cell.len())
            .is_some_and(|needed| new_content_offset >= needed);
        if !fits {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "separator repair for page {} could not fit the updated divider",
                    page_no
                ),
            });
        }

        new_content_offset -= new_cell.len();
        ptrs.insert(
            separator_idx_usize,
            u16::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "separator repair new offset exceeds u16 range".to_owned(),
            })?,
        );
        header.cell_count =
            u16::try_from(ptrs.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "separator repair cell count exceeds u16 range".to_owned(),
            })?;
        header.cell_content_offset =
            u32::try_from(new_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "separator repair content offset exceeds u32 range".to_owned(),
            })?;
        header.fragmented_free_bytes = 0;
        header.first_freeblock = 0;

        {
            let page_bytes = page_data.as_bytes_mut();
            page_bytes[new_content_offset..new_content_offset + new_cell.len()]
                .copy_from_slice(new_cell);
            if new_content_offset > ptr_array_end_with_new {
                page_bytes[ptr_array_end_with_new..new_content_offset].fill(0);
            }
            header.write(page_bytes, header_offset);
            cell::write_cell_pointers(page_bytes, header_offset, &header, &ptrs);
        }

        self.pager.write_page_data(cx, page_no, page_data)?;

        let refreshed = self.reload_page_fresh(cx, page_no)?;
        for stack_entry in &mut self.stack {
            if stack_entry.page_no == page_no {
                let mut updated = refreshed.clone();
                updated.cell_idx = stack_entry.cell_idx;
                *stack_entry = updated;
            }
        }
        Ok(())
    }

    fn table_insert_from_current_position(
        &mut self,
        cx: &Cx,
        rowid: i64,
        data: &[u8],
    ) -> Result<()> {
        if self.stack.is_empty() && !self.seed_empty_root_leaf_cursor(cx)? {
            return Err(FrankenError::internal("cursor stack is empty"));
        }

        // A read fast path may leave only the landing leaf on the stack. The
        // shared helper restores the root-to-leaf path before any mutation can
        // reach balancing, where depth one means the page is the actual root.
        if let Some(reseek) =
            self.ensure_full_mutation_path(cx, MutationPathTarget::TableInsert(rowid))?
        {
            if reseek.is_found() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "rowid {rowid} appeared during rootless-stack rebuild before table insert"
                    ),
                });
            }
        }

        let insert_idx = {
            let top = self
                .stack
                .last()
                .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
            if self.at_eof {
                top.header.cell_count
            } else {
                top.cell_idx
            }
        };

        // Take cell_buf for reuse so repeated inserts preserve allocation capacity.
        let mut cell_data = std::mem::take(&mut self.cell_buf);
        let overflow_head = match self.encode_table_leaf_cell_into(cx, rowid, data, &mut cell_data)
        {
            Ok(head) => head,
            Err(error) => {
                self.cell_buf = cell_data;
                return Err(error);
            }
        };
        if self.at_eof {
            match self.try_append_on_current_leaf(cx, &cell_data) {
                Ok(true) => {
                    self.cell_buf = cell_data;
                    self.last_insert_rowid = Some(rowid);
                    return Ok(());
                }
                Ok(false) => {}
                Err(error) => {
                    self.cell_buf = cell_data;
                    if let Some(first) = overflow_head {
                        let _ = self.free_overflow_chain(cx, first);
                    }
                    return Err(error);
                }
            }
        }
        match self.try_insert_on_leaf(cx, insert_idx, &cell_data) {
            Ok(true) => {
                self.cell_buf = cell_data;
                self.last_insert_rowid = Some(rowid);
                Ok(())
            }
            Ok(false) => {
                instrumentation::record_conservative_reload_fallback();
                let balance_result = self.balance_for_insert(cx, &cell_data, insert_idx);
                self.cell_buf = cell_data;
                if balance_result.is_ok() {
                    self.last_insert_rowid = Some(rowid);
                } else if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                balance_result
            }
            Err(error) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                Err(error)
            }
        }
    }

    fn index_insert_from_current_position(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        let insert_idx = {
            let top = self
                .stack
                .last()
                .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
            if !top.header.page_type.is_leaf() {
                return Err(FrankenError::internal(
                    "index insert from current position requires leaf cursor state",
                ));
            }
            if self.at_eof {
                top.header.cell_count
            } else {
                top.cell_idx
            }
        };

        let mut cell_data = std::mem::take(&mut self.cell_buf);
        let overflow_head = match self.encode_index_leaf_cell_into(cx, key, &mut cell_data) {
            Ok(head) => head,
            Err(error) => {
                self.cell_buf = cell_data;
                return Err(error);
            }
        };

        match self.try_insert_on_leaf(cx, insert_idx, &cell_data) {
            Ok(true) => {
                self.cell_buf = cell_data;
                Ok(())
            }
            Ok(false) => {
                instrumentation::record_conservative_reload_fallback();
                let balance_result = self.balance_for_insert(cx, &cell_data, insert_idx);
                self.cell_buf = cell_data;
                if balance_result.is_err() {
                    if let Some(first) = overflow_head {
                        let _ = self.free_overflow_chain(cx, first);
                    }
                }
                balance_result
            }
            Err(error) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                Err(error)
            }
        }
    }

    /// Ensure structural mutation sees a complete path beginning at this
    /// cursor's declared root page.
    ///
    /// Table seek-cache hits deliberately retain only the landing leaf for
    /// read performance. Such a stack is safe for reads but not for balancing:
    /// `stack.len() == 1` is reserved for a tree whose root is itself a leaf.
    /// Re-seek from the root while mutation is still reversible, then let the
    /// insert/delete caller validate whether the target was expected to exist.
    fn ensure_full_mutation_path(
        &mut self,
        cx: &Cx,
        target: MutationPathTarget,
    ) -> Result<Option<SeekResult>> {
        let first_page = self
            .stack
            .first()
            .ok_or_else(|| FrankenError::internal("cursor stack empty before mutation"))?
            .page_no;
        if first_page == self.root_page {
            return Ok(None);
        }

        self.clear_seek_cache();
        let result = match target {
            MutationPathTarget::TableInsert(rowid) => self.table_seek_for_insert(cx, rowid)?,
            MutationPathTarget::ExactTableRowid(rowid) => self.table_seek(cx, rowid)?,
            MutationPathTarget::ExactIndexKey(key) => self.index_seek(cx, &key)?,
        };

        let rebuilt_first = self
            .stack
            .first()
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "mutation re-seek from root {} produced an empty cursor path",
                    self.root_page.get()
                ),
            })?
            .page_no;
        if rebuilt_first != self.root_page {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "mutation cursor path starts at page {} instead of root {} after re-seek",
                    rebuilt_first.get(),
                    self.root_page.get()
                ),
            });
        }

        Ok(Some(result))
    }

    fn try_append_on_cached_rightmost_leaf(
        &mut self,
        cx: &Cx,
        rowid: i64,
        data: &[u8],
    ) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        let Some(mut cached) = self.rightmost_leaf_cache.take() else {
            return Ok(false);
        };
        if rowid <= cached.rowid {
            return Ok(false);
        }

        if let Some((insert_idx, new_cell_offset)) = self
            .try_append_table_leaf_payload_in_place_no_overflow(
                cx,
                cached.page_no,
                &mut cached.page_data,
                &mut cached.header,
                rowid,
                data,
            )?
        {
            self.last_insert_rowid = Some(rowid);
            self.last_known_depth = Some(cached.tree_depth);
            cached.rowid = rowid;
            cached.cell_pointers.push(new_cell_offset);
            let stack_page_data = cached.page_data.clone();
            let stack_cell_pointers = cached.cell_pointers.clone();
            let mutation_counter = Self::page_mutation_counter(&stack_page_data);
            self.stack.clear();
            self.stack.push(StackEntry {
                page_no: cached.page_no,
                page_data: stack_page_data,
                header: cached.header,
                cell_pointers: stack_cell_pointers,
                mutation_counter,
                cell_idx: insert_idx,
            });
            self.at_eof = false;
            self.rightmost_leaf_cache = Some(cached);
            return Ok(true);
        }

        let mut cell_data = std::mem::take(&mut self.cell_buf);
        let overflow_head = match self.encode_table_leaf_cell_into(cx, rowid, data, &mut cell_data)
        {
            Ok(overflow_head) => overflow_head,
            Err(error) => {
                self.cell_buf = cell_data;
                self.rightmost_leaf_cache = Some(cached);
                return Err(error);
            }
        };
        let append_result = self.try_append_leaf_page_in_place(
            cx,
            cached.page_no,
            &mut cached.page_data,
            &mut cached.header,
            &cell_data,
        );
        match append_result {
            Ok(Some((insert_idx, new_cell_offset))) => {
                self.cell_buf = cell_data;
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(cached.tree_depth);
                cached.rowid = rowid;
                cached.cell_pointers.push(new_cell_offset);
                let stack_page_data = cached.page_data.clone();
                let stack_cell_pointers = cached.cell_pointers.clone();
                let mutation_counter = Self::page_mutation_counter(&stack_page_data);
                self.stack.clear();
                self.stack.push(StackEntry {
                    page_no: cached.page_no,
                    page_data: stack_page_data,
                    header: cached.header,
                    cell_pointers: stack_cell_pointers,
                    mutation_counter,
                    cell_idx: insert_idx,
                });
                self.at_eof = false;
                self.rightmost_leaf_cache = Some(cached);
                Ok(true)
            }
            Ok(None) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    self.free_overflow_chain(cx, first)?;
                }
                self.stack.clear();
                self.at_eof = true;
                self.clear_rightmost_leaf_cache();
                Ok(false)
            }
            Err(error) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                self.stack.clear();
                self.at_eof = true;
                self.clear_rightmost_leaf_cache();
                Err(error)
            }
        }
    }

    fn try_table_append_on_hinted_leaf(
        &mut self,
        cx: &Cx,
        hinted_leaf_page: PageNumber,
        rowid: i64,
        data: &[u8],
    ) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        let hinted_tree_depth = self.last_known_depth.unwrap_or(1);
        self.stack.clear();
        self.at_eof = true;

        let mut entry = match self.load_page(cx, hinted_leaf_page) {
            Ok(entry) => entry,
            Err(_) => {
                self.clear_rightmost_leaf_cache();
                return Ok(false);
            }
        };
        if entry.header.page_type != cell::BtreePageType::LeafTable || entry.header.cell_count == 0
        {
            self.stack.clear();
            self.at_eof = true;
            self.clear_rightmost_leaf_cache();
            return Ok(false);
        }

        entry.cell_idx = entry.header.cell_count - 1;
        self.stack.push(entry);
        self.at_eof = false;
        self.record_range_page_witness(cx, hinted_leaf_page);

        // bd-wwqen.3: use cached rowid if available for this leaf page,
        // avoiding a cell parse just to read the last rowid.
        let last_rowid = if let Some(cached) = self.rightmost_leaf_cache.as_ref() {
            if cached.page_no == hinted_leaf_page {
                cached.rowid
            } else {
                self.rowid(cx)?
            }
        } else {
            self.rowid(cx)?
        };
        if rowid <= last_rowid {
            self.stack.clear();
            self.at_eof = true;
            self.clear_rightmost_leaf_cache();
            return Ok(false);
        }

        self.at_eof = true;
        let mut cell_data = std::mem::take(&mut self.cell_buf);
        let overflow_head = match self.encode_table_leaf_cell_into(cx, rowid, data, &mut cell_data)
        {
            Ok(head) => head,
            Err(error) => {
                self.cell_buf = cell_data;
                return Err(error);
            }
        };

        match self.try_append_on_current_leaf(cx, &cell_data) {
            Ok(true) => {
                self.cell_buf = cell_data;
                self.last_insert_rowid = Some(rowid);
                let parent_page = self.current_parent_page_hint_from_stack();
                if let Some(cache_entry) = self.stack.last().map(|entry| RightmostLeafCacheEntry {
                    page_no: entry.page_no,
                    rowid,
                    tree_depth: hinted_tree_depth,
                    parent_page,
                    page_data: entry.page_data.clone(),
                    header: entry.header,
                    cell_pointers: entry.cell_pointers.clone(),
                }) {
                    self.rightmost_leaf_cache = Some(cache_entry);
                } else {
                    self.clear_rightmost_leaf_cache();
                }
                Ok(true)
            }
            Ok(false) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    self.free_overflow_chain(cx, first)?;
                }
                self.stack.clear();
                self.at_eof = true;
                self.clear_rightmost_leaf_cache();
                Ok(false)
            }
            Err(error) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                self.stack.clear();
                self.at_eof = true;
                self.clear_rightmost_leaf_cache();
                Err(error)
            }
        }
    }

    fn try_table_append_on_hinted_leaf_with_known_last_rowid(
        &mut self,
        cx: &Cx,
        hinted_leaf_page: PageNumber,
        hinted_last_rowid: i64,
        rowid: i64,
        data: &[u8],
        // `false` is the retained-hint handoff path used by prepared direct
        // INSERT: the caller only needs the updated `TableAppendHint`, so
        // rebuilding a cursor stack and rightmost-cache pointer vector would
        // decode the whole cell-pointer array for state that is immediately
        // dropped.
        refresh_cursor_cache: bool,
    ) -> Result<Option<TableAppendHint>> {
        observe_cursor_cancellation(cx)?;
        let hinted_tree_depth = self.last_known_depth.unwrap_or(1);
        self.stack.clear();
        self.at_eof = true;

        let mut page_data = match self.pager.read_btree_page_data(cx, hinted_leaf_page) {
            Ok(page_data) => page_data,
            Err(_) => {
                self.clear_rightmost_leaf_cache();
                return Ok(None);
            }
        };
        let mut header = match cell::parse_page_header(page_data.as_bytes(), hinted_leaf_page) {
            Ok(header) => header,
            Err(_) => {
                self.clear_rightmost_leaf_cache();
                return Ok(None);
            }
        };
        if header.page_type != cell::BtreePageType::LeafTable || header.cell_count == 0 {
            self.clear_rightmost_leaf_cache();
            return Ok(None);
        }

        self.record_range_page_witness(cx, hinted_leaf_page);
        if rowid <= hinted_last_rowid {
            self.clear_rightmost_leaf_cache();
            return Ok(None);
        }
        let header_offset = cell::header_offset_for_page(hinted_leaf_page);
        let last_cell_idx = header.cell_count - 1;
        let last_ptr = match Self::read_cell_pointer_inline(
            page_data.as_bytes(),
            hinted_leaf_page,
            usize::from(header.page_type.header_size()),
            last_cell_idx,
        ) {
            Ok(ptr) => ptr,
            Err(_) => {
                self.clear_rightmost_leaf_cache();
                return Ok(None);
            }
        };
        // OPT-A3: skip full CellRef::parse — we only need the last cell's
        // rowid to confirm the hint is still valid. Read just the two
        // leading varints (payload_size, rowid) which avoids parsing the
        // local payload bounds / overflow pointer validation on every
        // append.
        let Some(actual_last_rowid) =
            cell::read_table_leaf_rowid_at_offset(page_data.as_bytes(), usize::from(last_ptr))
        else {
            self.clear_rightmost_leaf_cache();
            return Ok(None);
        };
        if actual_last_rowid != hinted_last_rowid {
            self.clear_rightmost_leaf_cache();
            return Ok(None);
        }

        // Snapshot the pre-append header so the cached pointer vector can be
        // rebuilt from the old pointer array plus the returned new-cell
        // offset. The append helpers already proved the extra pointer slot is
        // valid and wrote it to the page; rereading it from the page would just
        // decode the byte pair we already have.
        let pre_append_header = header;
        if let Some((insert_idx, new_cell_offset)) = self
            .try_append_table_leaf_payload_in_place_no_overflow(
                cx,
                hinted_leaf_page,
                &mut page_data,
                &mut header,
                rowid,
                data,
            )?
        {
            self.last_insert_rowid = Some(rowid);
            self.last_known_depth = Some(hinted_tree_depth);
            let parent_page = if refresh_cursor_cache {
                let mut cell_pointers = cell::read_cell_pointers(
                    page_data.as_bytes(),
                    &pre_append_header,
                    header_offset,
                )?;
                cell_pointers.push(new_cell_offset);
                let mutation_counter = Self::page_mutation_counter(&page_data);
                let stack_cell_pointers = cell_pointers.clone();
                self.stack.clear();
                self.stack.push(StackEntry {
                    page_no: hinted_leaf_page,
                    page_data: page_data.clone(),
                    header,
                    cell_pointers: stack_cell_pointers,
                    mutation_counter,
                    cell_idx: insert_idx,
                });
                self.at_eof = false;
                let parent_page = self.current_parent_page_hint_from_stack();
                self.rightmost_leaf_cache = Some(RightmostLeafCacheEntry {
                    page_no: hinted_leaf_page,
                    rowid,
                    tree_depth: hinted_tree_depth,
                    parent_page,
                    page_data: page_data.clone(),
                    header,
                    cell_pointers,
                });
                parent_page
            } else {
                self.stack.clear();
                self.at_eof = true;
                self.clear_rightmost_leaf_cache();
                None
            };
            return Ok(Some(TableAppendHint {
                leaf_page: hinted_leaf_page,
                last_rowid: rowid,
                tree_depth: hinted_tree_depth,
                parent_page,
                page_data: Some(page_data),
                header,
            }));
        }

        let mut cell_data = std::mem::take(&mut self.cell_buf);
        let overflow_head = match self.encode_table_leaf_cell_into(cx, rowid, data, &mut cell_data)
        {
            Ok(head) => head,
            Err(error) => {
                self.cell_buf = cell_data;
                return Err(error);
            }
        };
        let append_result = self.try_append_leaf_page_in_place(
            cx,
            hinted_leaf_page,
            &mut page_data,
            &mut header,
            &cell_data,
        );
        match append_result {
            Ok(Some((insert_idx, new_cell_offset))) => {
                self.cell_buf = cell_data;
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(hinted_tree_depth);
                let parent_page = if refresh_cursor_cache {
                    let mut cell_pointers = cell::read_cell_pointers(
                        page_data.as_bytes(),
                        &pre_append_header,
                        header_offset,
                    )?;
                    cell_pointers.push(new_cell_offset);
                    let mutation_counter = Self::page_mutation_counter(&page_data);
                    self.stack.clear();
                    self.stack.push(StackEntry {
                        page_no: hinted_leaf_page,
                        page_data: page_data.clone(),
                        header,
                        cell_pointers: cell_pointers.clone(),
                        mutation_counter,
                        cell_idx: insert_idx,
                    });
                    self.at_eof = false;
                    let parent_page = self.current_parent_page_hint_from_stack();
                    self.rightmost_leaf_cache = Some(RightmostLeafCacheEntry {
                        page_no: hinted_leaf_page,
                        rowid,
                        tree_depth: hinted_tree_depth,
                        parent_page,
                        page_data: page_data.clone(),
                        header,
                        cell_pointers,
                    });
                    parent_page
                } else {
                    self.stack.clear();
                    self.at_eof = true;
                    self.clear_rightmost_leaf_cache();
                    None
                };
                Ok(Some(TableAppendHint {
                    leaf_page: hinted_leaf_page,
                    last_rowid: rowid,
                    tree_depth: hinted_tree_depth,
                    parent_page,
                    page_data: Some(page_data),
                    header,
                }))
            }
            Ok(None) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    self.free_overflow_chain(cx, first)?;
                }
                self.clear_rightmost_leaf_cache();
                Ok(None)
            }
            Err(error) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                self.clear_rightmost_leaf_cache();
                Err(error)
            }
        }
    }

    fn try_append_on_external_rightmost_leaf_hint(
        &mut self,
        cx: &Cx,
        hint: &mut TableAppendHint,
        rowid: i64,
        data: &[u8],
    ) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        self.last_known_depth = Some(hint.tree_depth);
        self.stack.clear();
        self.at_eof = true;

        if rowid <= hint.last_rowid
            || hint.header.page_type != cell::BtreePageType::LeafTable
            || hint.header.cell_count == 0
        {
            return Ok(false);
        }

        self.record_range_page_witness(cx, hint.leaf_page);
        let usable_size = self.usable_size;
        let mut mutate_payload_result: Result<Option<(u16, u16)>> = Ok(None);
        let mut mutate_payload_only = |staged_page: &mut PageData| {
            mutate_payload_result =
                Self::try_append_table_leaf_payload_in_place_no_overflow_mutate_only(
                    usable_size,
                    hint.leaf_page,
                    staged_page,
                    &mut hint.header,
                    rowid,
                    data,
                );
        };
        if self
            .pager
            .try_mutate_staged_page_data(hint.leaf_page, &mut mutate_payload_only)
            && mutate_payload_result?.is_some()
        {
            self.last_insert_rowid = Some(rowid);
            self.last_known_depth = Some(hint.tree_depth);
            hint.last_rowid = rowid;
            hint.clear_page_data();
            self.clear_rightmost_leaf_cache();
            return Ok(true);
        }

        if let Some(mut page_data) = hint.page_data.take() {
            let result = self.try_append_on_external_rightmost_leaf_hint_page_data(
                cx,
                hint,
                rowid,
                data,
                &mut page_data,
            );
            if hint.page_data.is_none() {
                hint.page_data = Some(page_data);
            }
            return result;
        }

        if let Some(mut page_data) = self.pager.try_take_staged_page_data(hint.leaf_page) {
            if self
                .try_append_table_leaf_payload_in_place_no_overflow(
                    cx,
                    hint.leaf_page,
                    &mut page_data,
                    &mut hint.header,
                    rowid,
                    data,
                )?
                .is_some()
            {
                self.pager
                    .restore_staged_page_data(cx, hint.leaf_page, page_data)?;
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(hint.tree_depth);
                hint.last_rowid = rowid;
                self.clear_rightmost_leaf_cache();
                return Ok(true);
            }

            let mut cell_data = std::mem::take(&mut self.cell_buf);
            let overflow_head =
                match self.encode_table_leaf_cell_into(cx, rowid, data, &mut cell_data) {
                    Ok(head) => head,
                    Err(error) => {
                        self.cell_buf = cell_data;
                        self.pager
                            .restore_staged_page_data(cx, hint.leaf_page, page_data)?;
                        return Err(error);
                    }
                };
            let append_result = self.try_append_leaf_page_in_place(
                cx,
                hint.leaf_page,
                &mut page_data,
                &mut hint.header,
                &cell_data,
            );
            if let Err(error) = self
                .pager
                .restore_staged_page_data(cx, hint.leaf_page, page_data)
            {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                return Err(error);
            }
            match append_result {
                Ok(Some(_)) => {
                    self.cell_buf = cell_data;
                    self.last_insert_rowid = Some(rowid);
                    self.last_known_depth = Some(hint.tree_depth);
                    hint.last_rowid = rowid;
                    self.clear_rightmost_leaf_cache();
                    return Ok(true);
                }
                Ok(None) => {
                    let quick_balance_result = self
                        .try_quick_balance_on_external_rightmost_leaf_hint(
                            cx, hint, rowid, &cell_data,
                        );
                    self.cell_buf = cell_data;
                    match quick_balance_result {
                        Ok(true) => return Ok(true),
                        Ok(false) => {
                            if let Some(first) = overflow_head {
                                self.free_overflow_chain(cx, first)?;
                            }
                            return Ok(false);
                        }
                        Err(error) => {
                            if let Some(first) = overflow_head {
                                let _ = self.free_overflow_chain(cx, first);
                            }
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    self.cell_buf = cell_data;
                    if let Some(first) = overflow_head {
                        let _ = self.free_overflow_chain(cx, first);
                    }
                    return Err(error);
                }
            }
        }

        Ok(false)
    }

    fn try_append_on_external_rightmost_leaf_hint_with_writer<W>(
        &mut self,
        cx: &Cx,
        hint: &mut TableAppendHint,
        rowid: i64,
        payload_len: usize,
        writer: W,
    ) -> Result<bool>
    where
        W: FnOnce(&mut [u8]) -> Result<()>,
    {
        observe_cursor_cancellation(cx)?;
        self.last_known_depth = Some(hint.tree_depth);
        self.stack.clear();
        self.at_eof = true;

        if rowid <= hint.last_rowid
            || hint.header.page_type != cell::BtreePageType::LeafTable
            || hint.header.cell_count == 0
        {
            return Ok(false);
        }

        self.record_range_page_witness(cx, hint.leaf_page);
        let mut writer_slot = Some(writer);
        let usable_size = self.usable_size;
        let mut mutate_payload_result: Result<Option<(u16, u16)>> = Ok(None);
        let mut mutate_payload_only = |staged_page: &mut PageData| {
            let Some(writer) = writer_slot.take() else {
                mutate_payload_result = Err(FrankenError::internal(
                    "retained writer append attempted to consume writer twice",
                ));
                return;
            };
            mutate_payload_result =
                Self::try_append_table_leaf_payload_in_place_no_overflow_mutate_only_with_writer(
                    usable_size,
                    hint.leaf_page,
                    staged_page,
                    &mut hint.header,
                    rowid,
                    payload_len,
                    writer,
                );
        };
        if self
            .pager
            .try_mutate_staged_page_data(hint.leaf_page, &mut mutate_payload_only)
        {
            if mutate_payload_result?.is_some() {
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(hint.tree_depth);
                hint.last_rowid = rowid;
                hint.clear_page_data();
                self.clear_rightmost_leaf_cache();
                return Ok(true);
            }
            return Ok(false);
        }

        let Some(writer) = writer_slot.take() else {
            return Err(FrankenError::internal(
                "retained writer append lost writer without staged mutation",
            ));
        };
        let Some(mut page_data) = hint.page_data.take() else {
            return Ok(false);
        };

        let append_result =
            Self::try_append_table_leaf_payload_in_place_no_overflow_mutate_only_with_writer(
                self.usable_size,
                hint.leaf_page,
                &mut page_data,
                &mut hint.header,
                rowid,
                payload_len,
                writer,
            );
        hint.page_data = Some(page_data);

        match append_result {
            Ok(Some(_)) => {
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(hint.tree_depth);
                hint.last_rowid = rowid;
                self.clear_rightmost_leaf_cache();
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn try_append_on_external_rightmost_leaf_hint_page_data(
        &mut self,
        cx: &Cx,
        hint: &mut TableAppendHint,
        rowid: i64,
        data: &[u8],
        page_data: &mut PageData,
    ) -> Result<bool> {
        if self
            .try_append_table_leaf_payload_in_place_no_overflow(
                cx,
                hint.leaf_page,
                page_data,
                &mut hint.header,
                rowid,
                data,
            )?
            .is_some()
        {
            self.last_insert_rowid = Some(rowid);
            self.last_known_depth = Some(hint.tree_depth);
            hint.last_rowid = rowid;
            // The caller-owned retained hint is now the authoritative leaf image.
            // Clear the internal cache rather than keep a stale pre-append copy
            // that could overwrite this row on a later plain `table_insert()`.
            self.clear_rightmost_leaf_cache();
            return Ok(true);
        }
        let mut cell_data = std::mem::take(&mut self.cell_buf);
        let overflow_head = match self.encode_table_leaf_cell_into(cx, rowid, data, &mut cell_data)
        {
            Ok(head) => head,
            Err(error) => {
                self.cell_buf = cell_data;
                return Err(error);
            }
        };
        let append_result = self.try_append_leaf_page_in_place(
            cx,
            hint.leaf_page,
            page_data,
            &mut hint.header,
            &cell_data,
        );
        match append_result {
            Ok(Some(_)) => {
                self.cell_buf = cell_data;
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(hint.tree_depth);
                hint.last_rowid = rowid;
                // See the no-overflow fast path above: without a full in-cursor
                // refresh, the retained hint is newer than any cached leaf image.
                self.clear_rightmost_leaf_cache();
                Ok(true)
            }
            Ok(None) => {
                let fallback_result = (|| {
                    if self.try_quick_balance_on_external_rightmost_leaf_hint(
                        cx, hint, rowid, &cell_data,
                    )? {
                        return Ok(true);
                    }
                    let has_last = self.last(cx)?;
                    if has_last {
                        let last_rowid = self.rowid(cx)?;
                        if rowid <= last_rowid {
                            return Ok(false);
                        }
                    }
                    self.at_eof = true;
                    match self.try_append_on_current_leaf(cx, &cell_data) {
                        Ok(true) => {
                            self.last_insert_rowid = Some(rowid);
                        }
                        Ok(false) => {
                            instrumentation::record_conservative_reload_fallback();
                            let insert_idx = self
                                .stack
                                .last()
                                .ok_or_else(|| {
                                    FrankenError::internal(
                                        "cursor lost rightmost leaf during cached-hint fallback",
                                    )
                                })?
                                .header
                                .cell_count;
                            self.balance_for_insert(cx, &cell_data, insert_idx)?;
                            self.last_insert_rowid = Some(rowid);
                        }
                        Err(error) => return Err(error),
                    }
                    self.refresh_rightmost_leaf_cache_after_insert(cx, rowid)?;
                    *hint = self.table_cached_rightmost_leaf_hint().ok_or_else(|| {
                        FrankenError::internal(
                            "cached rightmost-leaf fallback did not refresh retained hint state",
                        )
                    })?;
                    Ok(true)
                })();
                self.cell_buf = cell_data;
                match fallback_result {
                    Ok(inserted) => {
                        if !inserted && let Some(first) = overflow_head {
                            self.free_overflow_chain(cx, first)?;
                        }
                        Ok(inserted)
                    }
                    Err(error) => {
                        if let Some(first) = overflow_head {
                            let _ = self.free_overflow_chain(cx, first);
                        }
                        Err(error)
                    }
                }
            }
            Err(error) => {
                self.cell_buf = cell_data;
                if let Some(first) = overflow_head {
                    let _ = self.free_overflow_chain(cx, first);
                }
                Err(error)
            }
        }
    }

    fn try_quick_balance_on_external_rightmost_leaf_hint(
        &mut self,
        cx: &Cx,
        hint: &mut TableAppendHint,
        rowid: i64,
        cell_data: &[u8],
    ) -> Result<bool> {
        let Some(parent_page) = hint.parent_page else {
            return Ok(false);
        };

        let quick_balance_start = instrumentation::profile_start();
        match balance::balance_quick_known_divider_rowid(
            cx,
            &mut self.pager,
            parent_page,
            hint.leaf_page,
            cell_data,
            hint.last_rowid,
            self.usable_size,
            self.page_size,
        ) {
            Ok(Some(result)) => {
                instrumentation::record_quick_balance_attempt(quick_balance_start, true);
                self.note_split_event();
                self.stack.clear();
                self.at_eof = true;
                self.last_insert_rowid = Some(rowid);
                self.last_known_depth = Some(hint.tree_depth);
                hint.leaf_page = result.new_pgno;
                hint.last_rowid = rowid;
                hint.parent_page = Some(parent_page);
                hint.page_data = Some(result.new_page_data.clone());
                hint.header = result.new_header;
                self.rightmost_leaf_cache = Some(RightmostLeafCacheEntry {
                    page_no: result.new_pgno,
                    rowid,
                    tree_depth: hint.tree_depth,
                    parent_page: Some(parent_page),
                    page_data: result.new_page_data,
                    header: result.new_header,
                    cell_pointers: vec![result.new_cell_ptr],
                });
                Ok(true)
            }
            Ok(None) => {
                instrumentation::record_quick_balance_attempt(quick_balance_start, false);
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Fast insert path for callers that already positioned the cursor with
    /// `table_move_to(rowid)` and observed `SeekResult::NotFound`.
    ///
    /// This reuses the current successor/EOF position instead of performing a
    /// second full B-tree seek before the insert. The VDBE `Insert` path uses
    /// this as its `USESEEKRESULT`-style successor-position reuse primitive.
    #[doc(hidden)]
    pub fn table_insert_prechecked_absent(
        &mut self,
        cx: &Cx,
        rowid: i64,
        data: &[u8],
    ) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if !cursor.current_position_admits_table_insert(rowid)? {
                cursor.clear_seek_cache();
                let seek = cursor.table_seek_for_insert(cx, rowid)?;
                if seek.is_found() {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "rowid {rowid} appeared while revalidating a prechecked-absent insert"
                        ),
                    });
                }
            }
            cursor.table_insert_from_current_position(cx, rowid, data)
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Refresh the persistent rightmost-leaf hint after a caller reused an
    /// already-proven EOF insertion position via `table_insert_prechecked_absent`.
    ///
    /// This keeps repeated append callers on the zero-seek path without forcing
    /// them to re-descend from the root just to reseed the hint.
    #[doc(hidden)]
    pub fn table_refresh_rightmost_leaf_cache_after_insert(
        &mut self,
        cx: &Cx,
        rowid: i64,
    ) -> Result<()> {
        self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            cursor.refresh_rightmost_leaf_cache_after_insert(cx, rowid)
        })
    }

    /// Fast append path for callers that already positioned the cursor on the
    /// rightmost row (for example via `last()` or a previous successful append).
    ///
    /// This reuses the current right-edge cursor stack instead of descending
    /// from the root again.
    #[doc(hidden)]
    pub fn table_append_after_last_position(
        &mut self,
        cx: &Cx,
        rowid: i64,
        data: &[u8],
    ) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if !cursor.is_on_rightmost_insert_edge() {
                let has_last = cursor.last(cx)?;
                if has_last {
                    let last_rowid = cursor.rowid(cx)?;
                    if rowid <= last_rowid {
                        cursor.clear_rightmost_leaf_cache();
                        let seek = cursor.table_seek_for_insert(cx, rowid)?;
                        if seek.is_found() {
                            return Err(FrankenError::PrimaryKeyViolation);
                        }
                        return cursor.table_insert_from_current_position(cx, rowid, data);
                    }
                }
            }
            cursor.at_eof = true;
            cursor.table_insert_from_current_position(cx, rowid, data)?;
            cursor.refresh_rightmost_leaf_cache_after_insert(cx, rowid)
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Writer-callback variant of [`Self::table_append_after_last_position`].
    ///
    /// Returns `Ok(true)` when the payload was written directly into the current
    /// rightmost leaf. Returns `Ok(false)` without inserting when the caller must
    /// fall back to the byte-slice path, for example after a page fills, when the
    /// rowid is no longer an append, or when the payload would need overflow
    /// pages.
    #[doc(hidden)]
    pub fn table_append_after_last_position_with_writer<W>(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload_len: usize,
        writer: W,
    ) -> Result<bool>
    where
        W: FnOnce(&mut [u8]) -> Result<()>,
    {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if !cursor.is_on_rightmost_insert_edge() {
                let has_last = cursor.last(cx)?;
                if has_last {
                    let last_rowid = cursor.rowid(cx)?;
                    if rowid <= last_rowid {
                        cursor.clear_rightmost_leaf_cache();
                        return Ok(false);
                    }
                }
            }
            cursor.at_eof = true;
            if cursor.stack.is_empty() && !cursor.seed_empty_root_leaf_cursor(cx)? {
                return Ok(false);
            }
            if !cursor.try_append_payload_on_current_leaf_with_writer(
                cx,
                rowid,
                payload_len,
                writer,
            )? {
                return Ok(false);
            }
            cursor.refresh_rightmost_leaf_cache_after_insert(cx, rowid)?;
            Ok(true)
        });
        if matches!(result, Ok(true)) {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Fast insert path for callers that already positioned the cursor with
    /// `index_move_to(key)` and observed `SeekResult::NotFound`.
    ///
    /// This reuses the current successor/EOF position instead of performing a
    /// second full B-tree seek before the insert.
    #[doc(hidden)]
    pub fn index_insert_prechecked_absent(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            cursor.index_insert_from_current_position(cx, key)
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Fast insert path for callers that expect a monotonically increasing
    /// rowid stream and want to try the rightmost-leaf append path before
    /// falling back to a full seek.
    #[doc(hidden)]
    pub fn table_insert_rightmost_hint(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            let has_last = cursor.last(cx)?;
            if has_last {
                let last_rowid = cursor.rowid(cx)?;
                if rowid <= last_rowid {
                    cursor.clear_rightmost_leaf_cache();
                    let seek = cursor.table_seek_for_insert(cx, rowid)?;
                    if seek.is_found() {
                        return Err(FrankenError::PrimaryKeyViolation);
                    }
                    return cursor.table_insert_from_current_position(cx, rowid, data);
                }
                cursor.at_eof = true;
            }
            cursor.table_insert_from_current_position(cx, rowid, data)?;
            cursor.refresh_rightmost_leaf_cache_after_insert(cx, rowid)
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Fast append path for repeated monotonic inserts when the caller has a
    /// previously successful rightmost leaf hint from the same table.
    ///
    /// The hint is conservative: if the leaf is stale, full, or no longer
    /// ordered before `rowid`, this falls back to the normal rightmost-hint
    /// insert path.
    #[doc(hidden)]
    pub fn table_insert_rightmost_leaf_hint(
        &mut self,
        cx: &Cx,
        hinted_leaf_page: PageNumber,
        rowid: i64,
        data: &[u8],
    ) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if cursor.try_table_append_on_hinted_leaf(cx, hinted_leaf_page, rowid, data)? {
                return Ok(());
            }

            let has_last = cursor.last(cx)?;
            if has_last {
                let last_rowid = cursor.rowid(cx)?;
                if rowid <= last_rowid {
                    cursor.clear_rightmost_leaf_cache();
                    let seek = cursor.table_seek_for_insert(cx, rowid)?;
                    if seek.is_found() {
                        return Err(FrankenError::PrimaryKeyViolation);
                    }
                    return cursor.table_insert_from_current_position(cx, rowid, data);
                }
                cursor.at_eof = true;
            }
            cursor.table_insert_from_current_position(cx, rowid, data)?;
            cursor.refresh_rightmost_leaf_cache_after_insert(cx, rowid)
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Specialized append fast path for external callers that already know the
    /// last successful rowid associated with `hinted_leaf_page`.
    ///
    /// Unlike [`Self::table_insert_rightmost_leaf_hint`], this does not decode
    /// the leaf cell pointer array or parse the final row just to rediscover
    /// the same right-edge rowid. It returns `Ok(Some(page))` when the hinted
    /// leaf accepted the append directly and `Ok(None)` when the caller should
    /// fall back to the normal insert path.
    #[doc(hidden)]
    pub fn table_try_append_rightmost_leaf_hint_known_last_rowid(
        &mut self,
        cx: &Cx,
        hinted_leaf_page: PageNumber,
        hinted_last_rowid: i64,
        rowid: i64,
        data: &[u8],
    ) -> Result<Option<PageNumber>> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if cursor.rightmost_leaf_cache.as_ref().is_some_and(|cached| {
                cached.page_no == hinted_leaf_page && cached.rowid == hinted_last_rowid
            }) && cursor.try_append_on_cached_rightmost_leaf(cx, rowid, data)?
            {
                return Ok(Some(hinted_leaf_page));
            }
            let hint = cursor.try_table_append_on_hinted_leaf_with_known_last_rowid(
                cx,
                hinted_leaf_page,
                hinted_last_rowid,
                rowid,
                data,
                true,
            )?;
            Ok(hint.map(|value| value.leaf_page()))
        });
        if result.as_ref().is_ok_and(|page| page.is_some()) {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Same as [`Self::table_try_append_rightmost_leaf_hint_known_last_rowid`]
    /// but returns the refreshed retained leaf image on success.
    #[doc(hidden)]
    pub fn table_try_append_rightmost_leaf_hint_known_last_rowid_with_state(
        &mut self,
        cx: &Cx,
        hinted_leaf_page: PageNumber,
        hinted_last_rowid: i64,
        rowid: i64,
        data: &[u8],
    ) -> Result<Option<TableAppendHint>> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if cursor.rightmost_leaf_cache.as_ref().is_some_and(|cached| {
                cached.page_no == hinted_leaf_page && cached.rowid == hinted_last_rowid
            }) && cursor.try_append_on_cached_rightmost_leaf(cx, rowid, data)?
            {
                return Ok(cursor
                    .rightmost_leaf_cache
                    .as_ref()
                    .map(TableAppendHint::from));
            }
            cursor.try_table_append_on_hinted_leaf_with_known_last_rowid(
                cx,
                hinted_leaf_page,
                hinted_last_rowid,
                rowid,
                data,
                false,
            )
        });
        if result.as_ref().is_ok_and(|hint| hint.is_some()) {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Reuse a retained rightmost-leaf image captured by a prior append.
    #[doc(hidden)]
    pub fn table_try_append_cached_rightmost_leaf_hint(
        &mut self,
        cx: &Cx,
        hint: &mut TableAppendHint,
        rowid: i64,
        data: &[u8],
    ) -> Result<bool> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            cursor.try_append_on_external_rightmost_leaf_hint(cx, hint, rowid, data)
        });
        if matches!(result, Ok(true)) {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Writer-callback variant of
    /// [`Self::table_try_append_cached_rightmost_leaf_hint`].
    ///
    /// This only succeeds while the retained hint still owns a hot page image.
    /// It deliberately does not fall through to staged-page or quick-balance
    /// paths, because the one-shot writer cannot be replayed after a failed
    /// fit check. Callers should fall back to the byte-slice API when this
    /// returns `Ok(false)`.
    #[doc(hidden)]
    pub fn table_try_append_cached_rightmost_leaf_hint_with_writer<W>(
        &mut self,
        cx: &Cx,
        hint: &mut TableAppendHint,
        rowid: i64,
        payload_len: usize,
        writer: W,
    ) -> Result<bool>
    where
        W: FnOnce(&mut [u8]) -> Result<()>,
    {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            cursor.try_append_on_external_rightmost_leaf_hint_with_writer(
                cx,
                hint,
                rowid,
                payload_len,
                writer,
            )
        });
        if matches!(result, Ok(true)) {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Snapshot the cursor's current retained rightmost-leaf cache.
    #[doc(hidden)]
    #[must_use]
    pub fn table_cached_rightmost_leaf_hint(&self) -> Option<TableAppendHint> {
        self.rightmost_leaf_cache
            .as_ref()
            .map(TableAppendHint::from)
    }

    /// Overwrite the payload bytes for the currently positioned leaf-table row
    /// when the new record has the same local, non-overflow size.
    ///
    /// This is a deliberately narrow primitive for direct UPDATE fast paths that
    /// have already proven the rowid is unchanged and no secondary index or
    /// trigger maintenance is required.  It preserves the cell header, rowid
    /// varint, pointer array, freeblock chain, and cursor position; callers fall
    /// back to delete+insert whenever the record size changes or overflow is
    /// involved.
    #[doc(hidden)]
    pub fn table_overwrite_current_payload_same_size_no_overflow(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload: &[u8],
    ) -> Result<bool> {
        let result = self.with_btree_op(cx, BtreeOpType::Delete, |cursor| {
            cursor.overwrite_current_table_payload_same_size_no_overflow(cx, rowid, payload)
        });
        if matches!(result, Ok(true)) {
            self.bump_row_image_epoch();
        }
        result
    }

    /// Capture the currently positioned table leaf for batched payload patches.
    ///
    /// The returned run is private to the caller until it is flushed back
    /// through [`Self::flush_table_leaf_payload_patch_run`]. Unsupported cursor
    /// positions return `None` so callers can stay on the ordinary per-row path.
    #[doc(hidden)]
    pub fn table_leaf_payload_patch_run_current(
        &mut self,
        rowid: i64,
    ) -> Result<Option<TableLeafPayloadPatchRun>> {
        if self.at_eof {
            return Ok(None);
        }
        let Some(entry) = self.stack.last() else {
            return Ok(None);
        };
        if entry.header.page_type != BtreePageType::LeafTable || entry.header.cell_count == 0 {
            return Ok(None);
        }
        if TableLeafPayloadPatchRun::table_leaf_rowid_at(entry, entry.cell_idx)? != rowid {
            return Ok(None);
        }
        Ok(Some(TableLeafPayloadPatchRun {
            entry: entry.clone(),
            dirty: false,
        }))
    }

    /// Capture the currently positioned table leaf for batched same-leaf deletes.
    ///
    /// This only admits cells whose removal cannot force parent separator repair
    /// or an immediate non-root leaf rebalance. Callers flush the run and fall
    /// back to the ordinary cursor path when this returns `None`.
    #[doc(hidden)]
    pub fn table_leaf_delete_run_current(
        &mut self,
        rowid: i64,
    ) -> Result<Option<TableLeafDeleteRun>> {
        if self.at_eof {
            return Ok(None);
        }
        let Some(entry) = self.stack.last() else {
            return Ok(None);
        };
        if entry.header.page_type != BtreePageType::LeafTable || entry.header.cell_count == 0 {
            return Ok(None);
        }
        if TableLeafPayloadPatchRun::table_leaf_rowid_at(entry, entry.cell_idx)? != rowid {
            return Ok(None);
        }
        let tree_depth = self.stack.len();
        if tree_depth > 1
            && (entry.header.cell_count <= 1
                || entry.cell_idx == entry.header.cell_count.saturating_sub(1))
        {
            return Ok(None);
        }
        Ok(Some(TableLeafDeleteRun {
            entry: entry.clone(),
            tree_depth,
            dirty: false,
            compact_cell_area: TableLeafDeleteRun::has_compact_cell_area_for_entry(
                entry,
                self.usable_size,
            ),
            profile_delete_leaf_run: instrumentation::copy_profile_enabled(),
            deleted_cell_indices: SmallVec::new(),
        }))
    }

    /// Publish a same-leaf payload patch run as one page write.
    #[doc(hidden)]
    pub fn flush_table_leaf_payload_patch_run(
        &mut self,
        cx: &Cx,
        run: TableLeafPayloadPatchRun,
    ) -> Result<()> {
        observe_cursor_cancellation(cx)?;
        let (leaf_page, page_data) = run.into_page();
        self.pager.write_page_data(cx, leaf_page, page_data)?;
        self.stack.clear();
        self.at_eof = true;
        self.clear_rightmost_leaf_cache();
        self.clear_seek_cache();
        self.cell_slot_cache.get_mut().clear();
        self.bump_row_image_epoch();
        Ok(())
    }

    /// Publish a same-leaf delete run as one page write.
    #[doc(hidden)]
    pub fn flush_table_leaf_delete_run(
        &mut self,
        cx: &Cx,
        mut run: TableLeafDeleteRun,
    ) -> Result<()> {
        self.flush_table_leaf_delete_run_in_place(cx, &mut run)
    }

    /// Publish a same-leaf delete run without consuming it.
    #[doc(hidden)]
    pub fn flush_table_leaf_delete_run_in_place(
        &mut self,
        cx: &Cx,
        run: &mut TableLeafDeleteRun,
    ) -> Result<()> {
        observe_cursor_cancellation(cx)?;
        if run.is_dirty() {
            let materialize_start = instrumentation::profile_start();
            let leaf_page = run.leaf_page();
            run.materialize_deletions(self.usable_size)?;
            instrumentation::record_delete_leaf_run_materialize(materialize_start);
            let write_start = instrumentation::profile_start();
            self.pager
                .write_page_data(cx, leaf_page, run.entry.page_data.clone())?;
            instrumentation::record_delete_leaf_run_write(write_start);
            run.dirty = false;
        }
        self.stack.clear();
        self.at_eof = true;
        self.clear_rightmost_leaf_cache();
        self.clear_seek_cache();
        self.cell_slot_cache.get_mut().clear();
        self.bump_row_image_epoch();
        Ok(())
    }

    /// Swap in or out an external reusable cell-assembly buffer.
    ///
    /// Fresh cursor instances otherwise start with an empty `cell_buf`, which
    /// forces repeated monotonic insert loops to rebuild allocator state for
    /// every encoded table leaf cell. Swapping a connection-owned scratch
    /// buffer through the cursor preserves that capacity across executes.
    #[doc(hidden)]
    pub fn swap_cell_scratch(&mut self, scratch: &mut Vec<u8>) {
        std::mem::swap(&mut self.cell_buf, scratch);
    }

    fn overwrite_current_table_payload_same_size_no_overflow(
        &mut self,
        cx: &Cx,
        rowid: i64,
        payload: &[u8],
    ) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        self.clear_rightmost_leaf_cache();

        let Some(top) = self.stack.last() else {
            return Ok(false);
        };
        if self.at_eof || top.header.page_type != BtreePageType::LeafTable {
            return Ok(false);
        }

        let leaf_page_no = top.page_no;
        let cell_idx = top.cell_idx;
        let cell = self.parse_cell_at(top, cell_idx)?;
        if cell.rowid != Some(rowid)
            || cell.overflow_page.is_some()
            || usize::try_from(cell.payload_size).ok() != Some(payload.len())
            || usize::try_from(cell.local_size).ok() != Some(payload.len())
        {
            return Ok(false);
        }

        let usable_size = usize::try_from(self.usable_size).unwrap_or(usize::MAX);
        let end = cell
            .payload_offset
            .checked_add(payload.len())
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "same-size payload overwrite offset overflow".to_owned(),
            })?;

        let staged_page = {
            let entry = self
                .stack
                .last_mut()
                .ok_or_else(|| FrankenError::internal("cursor stack empty during overwrite"))?;
            if end > entry.page_data.as_bytes().len() || end > usable_size {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "same-size payload overwrite extends past usable page".to_owned(),
                });
            }
            entry.page_data.as_bytes_mut()[cell.payload_offset..end].copy_from_slice(payload);
            entry.mutation_counter = Self::page_mutation_counter(&entry.page_data);
            entry.page_data.clone()
        };
        self.pager.write_page_data(cx, leaf_page_no, staged_page)?;
        self.at_eof = false;
        Ok(true)
    }

    fn local_leaf_table_cell<'a>(
        &'a self,
        entry: &'a StackEntry,
        idx: u16,
    ) -> Result<Option<(i64, &'a [u8])>> {
        if entry.header.page_type != BtreePageType::LeafTable {
            return Ok(None);
        }
        if idx >= entry.header.cell_count {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell index {} out of bounds ({})",
                    idx, entry.header.cell_count
                ),
            });
        }

        let page = entry.page_data.as_bytes();
        let idx_usize = usize::from(idx);
        let cell_offset = if idx_usize < entry.cell_pointers.len() {
            usize::from(entry.cell_pointers[idx_usize])
        } else {
            usize::from(Self::read_stack_entry_cell_pointer_inline(entry, idx)?)
        };
        let payload_size_slice =
            page.get(cell_offset..)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("cell offset {cell_offset} extends past page"),
                })?;
        let (payload_size_raw, ps_len) =
            read_varint(payload_size_slice).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in cell (payload size)".to_owned(),
            })?;
        let payload_size =
            u32::try_from(payload_size_raw).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "cell payload size exceeds 32-bit range".to_owned(),
            })?;
        if payload_size > self.usable_size.saturating_sub(35) {
            return Ok(None);
        }

        let rowid_start =
            cell_offset
                .checked_add(ps_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell offset overflow after payload size".to_owned(),
                })?;
        let rowid_slice = page
            .get(rowid_start..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell offset overflow after payload size".to_owned(),
            })?;
        let (rowid_raw, rowid_len) =
            read_varint(rowid_slice).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in table cell (rowid)".to_owned(),
            })?;
        let payload_offset =
            rowid_start
                .checked_add(rowid_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell payload offset overflow".to_owned(),
                })?;
        let payload_len =
            usize::try_from(payload_size).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "cell payload size exceeds usize range".to_owned(),
            })?;
        let local_end = payload_offset.checked_add(payload_len).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: "cell payload offset overflow".to_owned(),
            }
        })?;
        let usable_size = usize::try_from(self.usable_size).unwrap_or(usize::MAX);
        if local_end > page.len() || local_end > usable_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "cell extends past usable page size (payload bytes)".to_owned(),
            });
        }

        #[allow(clippy::cast_possible_wrap)]
        let rowid = rowid_raw as i64;
        Ok(Some((rowid, &page[payload_offset..local_end])))
    }

    fn local_leaf_table_rowid_and_payload<'a>(
        &'a self,
        entry: &'a StackEntry,
        idx: u16,
    ) -> Result<Option<(i64, Cow<'a, [u8]>)>> {
        Ok(self
            .local_leaf_table_cell(entry, idx)?
            .map(|(rowid, payload)| (rowid, Cow::Borrowed(payload))))
    }

    fn local_leaf_table_payload_into(
        &self,
        entry: &StackEntry,
        idx: u16,
        out: &mut Vec<u8>,
    ) -> Result<bool> {
        let Some((_, payload)) = self.local_leaf_table_cell(entry, idx)? else {
            return Ok(false);
        };
        out.clear();
        instrumentation::record_local_payload_copy(payload.len());
        out.extend_from_slice(payload);
        Ok(true)
    }

    fn local_leaf_table_payload_prefix_into(
        &self,
        entry: &StackEntry,
        idx: u16,
        max_prefix_bytes: usize,
        out: &mut Vec<u8>,
    ) -> Result<bool> {
        let Some((_, payload)) = self.local_leaf_table_rowid_and_payload(entry, idx)? else {
            return Ok(false);
        };
        out.clear();
        let prefix_len = payload.len().min(max_prefix_bytes);
        if prefix_len > 0 {
            instrumentation::record_local_payload_copy(prefix_len);
            out.extend_from_slice(&payload[..prefix_len]);
        }
        Ok(true)
    }
}

#[allow(clippy::missing_errors_doc)]
impl<P: PageWriter> BtCursor<P> {
    /// Insert a UNIQUE index key and report whether the key was proven to be
    /// strictly after the pre-existing rightmost key.
    ///
    /// The report is intentionally conservative. It is used by VDBE callers to
    /// seed a same-statement monotonic append hint; correctness never depends
    /// on receiving `true`.
    #[doc(hidden)]
    pub fn index_insert_unique_with_rightmost_report(
        &mut self,
        cx: &Cx,
        key: &[u8],
        n_unique_cols: usize,
        columns_label: &str,
    ) -> Result<bool> {
        let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::BtreeCursor);
        // Parse only the indexed column prefix. The trailing rowid suffix is
        // irrelevant to UNIQUE checks and can dominate hot ingest probes.
        let new_fields = match parse_record_prefix(key, n_unique_cols) {
            Some(f) => f,
            None => {
                self.index_insert(cx, key)?;
                return Ok(false);
            }
        };
        if new_fields.len() < n_unique_cols {
            self.index_insert(cx, key)?;
            return Ok(false);
        }
        // Check that all indexed columns are non-NULL — if any is NULL,
        // SQLite allows the insert regardless of uniqueness.
        let any_null = new_fields
            .iter()
            .take(n_unique_cols)
            .any(|v| matches!(v, fsqlite_types::SqliteValue::Null));
        if any_null {
            self.index_insert(cx, key)?;
            return Ok(false);
        }

        let new_prefix = &new_fields[..n_unique_cols];
        let mut inserted_after_existing_rightmost = false;

        // Use index_seek to position cursor, then scan adjacent entries for
        // prefix matches. We check the current entry and the predecessor.
        // Because the full key includes the rowid suffix, two records with the
        // same indexed columns but different rowids sort adjacently.
        self.with_btree_op(cx, BtreeOpType::Seek, |cursor| {
            let _seek = cursor.index_seek(cx, key)?;
            let restore_eof = cursor.at_eof;
            inserted_after_existing_rightmost = restore_eof;

            let mut to_check = Vec::with_capacity(2);

            if !cursor.at_eof {
                to_check.push(cursor.payload(cx)?);
            }

            if cursor.prev(cx)? {
                to_check.push(cursor.payload(cx)?);
            }

            for existing_key in to_check {
                if let Some(existing_fields) = parse_record_prefix(&existing_key, n_unique_cols) {
                    if existing_fields.len() >= n_unique_cols && new_prefix == existing_fields {
                        return Err(FrankenError::UniqueViolation {
                            columns: columns_label.to_owned(),
                        });
                    }
                }
            }

            if restore_eof {
                cursor.at_eof = true;
            } else {
                cursor.next(cx)?;
            }
            Ok(())
        })?;

        // The uniqueness probe already positioned the cursor at the insertion
        // successor/EOF. Reuse that position only when it is still a leaf; a
        // probe can restore to an interior separator in deeper index trees, and
        // inserting from that state was the historical 10k undercount failure.
        if self
            .stack
            .last()
            .is_some_and(|top| top.header.page_type.is_leaf())
        {
            self.index_insert_prechecked_absent(cx, key)?;
        } else {
            self.index_insert(cx, key)?;
        }
        Ok(inserted_after_existing_rightmost)
    }

    /// Append an index key using the current right-edge cursor position.
    ///
    /// The caller must already have proved both uniqueness and monotonicity.
    /// This function only verifies that the cursor is still positioned on the
    /// rightmost insert edge; if not, it returns `Ok(false)` and leaves the
    /// canonical insert path to the caller.
    #[doc(hidden)]
    pub fn index_append_after_current_rightmost_position(
        &mut self,
        cx: &Cx,
        key: &[u8],
    ) -> Result<bool> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if !cursor.is_on_rightmost_insert_edge() {
                return Ok(false);
            }
            cursor.at_eof = true;
            cursor.index_insert_from_current_position(cx, key)?;
            Ok(true)
        });
        if matches!(result, Ok(true)) {
            self.bump_row_image_epoch();
        }
        result
    }
}

// ---------------------------------------------------------------------------
// BtreeCursorOps implementation
// ---------------------------------------------------------------------------

impl<P: PageWriter> sealed::Sealed for BtCursor<P> {}

#[allow(clippy::missing_errors_doc)]
impl<P: PageWriter> BtreeCursorOps for BtCursor<P> {
    fn index_move_to(&mut self, cx: &Cx, key: &[u8]) -> Result<SeekResult> {
        self.with_btree_op(cx, BtreeOpType::Seek, |cursor| cursor.index_seek(cx, key))
    }

    fn index_move_to_upper_bound(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        self.with_btree_op(cx, BtreeOpType::Seek, |cursor| {
            cursor.index_seek_upper_bound(cx, key)?;
            Ok(())
        })
    }

    fn table_move_to(&mut self, cx: &Cx, rowid: i64) -> Result<SeekResult> {
        self.with_btree_op(cx, BtreeOpType::Seek, |cursor| cursor.table_seek(cx, rowid))
    }

    fn first(&mut self, cx: &Cx) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        self.stack.clear();
        self.at_eof = true;
        self.move_to_leftmost_leaf(cx, self.root_page, true)
    }

    fn last(&mut self, cx: &Cx) -> Result<bool> {
        observe_cursor_cancellation(cx)?;
        self.stack.clear();
        self.at_eof = true;
        self.move_to_rightmost_leaf(cx, self.root_page, true)
    }

    fn next(&mut self, cx: &Cx) -> Result<bool> {
        self.advance_next(cx)
    }

    fn prev(&mut self, cx: &Cx) -> Result<bool> {
        self.advance_prev(cx)
    }

    fn index_insert(&mut self, cx: &Cx, key: &[u8]) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            let seek = cursor.index_seek_for_insert(cx, key)?;
            let (is_leaf, cell_idx) = {
                let top = cursor
                    .stack
                    .last()
                    .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
                (top.header.page_type.is_leaf(), top.cell_idx)
            };

            let mut insert_idx;

            if !is_leaf {
                if seek.is_found() {
                    // Matched exactly on an interior page. Descend to the right child's leftmost leaf.
                    let right_child = {
                        let top = cursor
                            .stack
                            .last()
                            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
                        Self::child_page_at(top, cell_idx + 1)?
                    };
                    cursor.move_to_leftmost_leaf(cx, right_child, false)?;

                    insert_idx = 0; // The new key goes at the very beginning of the right subtree.
                } else {
                    // Successor on an interior page. The key belongs in the LEFT child's rightmost leaf.
                    let left_child = {
                        let top = cursor
                            .stack
                            .last()
                            .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
                        Self::child_page_at(top, cell_idx)?
                    };
                    cursor.move_to_rightmost_leaf(cx, left_child, false)?;
                    let top = cursor
                        .stack
                        .last()
                        .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
                    insert_idx = top.header.cell_count; // Append at the end of the left child.
                }
            } else {
                let top = cursor
                    .stack
                    .last()
                    .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?;
                insert_idx = if cursor.at_eof {
                    top.header.cell_count
                } else {
                    top.cell_idx
                };
                if seek.is_found() {
                    // Duplicate key on a leaf; place after the existing one.
                    insert_idx = insert_idx.saturating_add(1);
                }
            }

            // Take cell_buf for reuse — same pattern as table_insert.
            let mut cell_data = std::mem::take(&mut cursor.cell_buf);
            let overflow_head = match cursor.encode_index_leaf_cell_into(cx, key, &mut cell_data) {
                Ok(head) => head,
                Err(error) => {
                    cursor.cell_buf = cell_data;
                    return Err(error);
                }
            };

            match cursor.try_insert_on_leaf(cx, insert_idx, &cell_data) {
                Ok(true) => {
                    cursor.cell_buf = cell_data;
                    Ok(())
                }
                Ok(false) => {
                    // Page full — balance and redistribute.
                    instrumentation::record_conservative_reload_fallback();
                    let balance_result = cursor.balance_for_insert(cx, &cell_data, insert_idx);
                    cursor.cell_buf = cell_data;
                    if balance_result.is_err() {
                        if let Some(first) = overflow_head {
                            let _ = cursor.free_overflow_chain(cx, first);
                        }
                    }
                    balance_result
                }
                Err(error) => {
                    cursor.cell_buf = cell_data;
                    if let Some(first) = overflow_head {
                        let _ = cursor.free_overflow_chain(cx, first);
                    }
                    Err(error)
                }
            }
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    fn index_insert_unique(
        &mut self,
        cx: &Cx,
        key: &[u8],
        n_unique_cols: usize,
        columns_label: &str,
    ) -> Result<()> {
        self.index_insert_unique_with_rightmost_report(cx, key, n_unique_cols, columns_label)
            .map(|_| ())
    }

    fn table_insert(&mut self, cx: &Cx, rowid: i64, data: &[u8]) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Insert, |cursor| {
            if let Some((cached_page_no, cached_rowid)) = cursor
                .rightmost_leaf_cache
                .as_ref()
                .map(|cached| (cached.page_no, cached.rowid))
            {
                if rowid > cached_rowid {
                    if cursor.try_append_on_cached_rightmost_leaf(cx, rowid, data)? {
                        return Ok(());
                    }
                    if cursor
                        .try_table_append_on_hinted_leaf_with_known_last_rowid(
                            cx,
                            cached_page_no,
                            cached_rowid,
                            rowid,
                            data,
                            true,
                        )?
                        .is_some()
                    {
                        return Ok(());
                    }
                } else {
                    // A midstream insert can rebalance the right edge, so drop
                    // the append hint before taking the general path.
                    cursor.clear_rightmost_leaf_cache();
                }
            }

            let seek = cursor.table_seek_for_insert(cx, rowid)?;
            if seek.is_found() {
                return Err(FrankenError::PrimaryKeyViolation);
            }
            let rightmost_insert = cursor.at_eof;
            cursor.table_insert_from_current_position(cx, rowid, data)?;
            if rightmost_insert {
                cursor.refresh_rightmost_leaf_cache_after_insert(cx, rowid)?;
            }
            Ok(())
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    fn delete(&mut self, cx: &Cx) -> Result<()> {
        let result = self.with_btree_op(cx, BtreeOpType::Delete, |cursor| {
            cursor.clear_rightmost_leaf_cache();
            // Delete may rebalance and then re-seek internally to restore
            // cursor position. Any cache entries from the caller's prior seek
            // can point at a leaf that this delete is about to rewrite or free.
            cursor.clear_seek_cache();

            if cursor.at_eof || cursor.stack.is_empty() {
                return Err(FrankenError::internal("cursor at EOF"));
            }

            // The caller's positioning seek may have gone through the table
            // seek-cache fast path (`try_table_seek_cache`), which rebuilds
            // the stack as just the landing leaf with no path from the root.
            // Deleting from such a rootless stack used to silently disable
            // `balance_for_delete` (depth one was treated as "the leaf IS the
            // root"), the pre-delete anchor capture, and
            // separator repair — so a leaf drained to zero cells stays
            // referenced by its parent interior page, a shape stock SQLite
            // rejects as "database disk image is malformed" (bd-kwei8; the
            // INSERT OR REPLACE conflict path hits this because the failed
            // insert's seek primes the cache and `native_replace_row`'s
            // follow-up `table_move_to` then lands on the fast path).
            // Rebuild the full root-to-leaf path before mutating anything;
            // the seek cache was cleared above, so this re-seek descends
            // from the root.
            if cursor
                .stack
                .first()
                .is_some_and(|entry| entry.page_no != cursor.root_page)
            {
                let path_target = if cursor.is_table {
                    MutationPathTarget::ExactTableRowid(cursor.rowid(cx)?)
                } else {
                    MutationPathTarget::ExactIndexKey(cursor.payload(cx)?)
                };
                if let Some(reseek) = cursor.ensure_full_mutation_path(cx, path_target)?
                    && !reseek.is_found()
                {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "delete re-seek lost the current entry while rebuilding the mutation path"
                            .to_owned(),
                    });
                }
            }

            let top = cursor
                .stack
                .last()
                .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?
                .clone();
            let separator_repair = cursor.separator_repair_for_deleted_leaf_max(&top)?;

            if !top.header.page_type.is_leaf() {
                // Interior node deletion (index B-trees):
                // 1) identify successor payload, 2) replace interior key,
                // 3) remove duplicate successor from leaf.
                let original_key = cursor.payload(cx)?;

                // Advance to the successor in the right subtree.
                let advanced = cursor.advance_next(cx)?;
                if !advanced || cursor.at_eof {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "no successor for interior node".to_owned(),
                    });
                }
                let successor_key = cursor.payload(cx)?;

                // Re-seek the original key to perform in-place interior replacement.
                let seek_res = cursor.index_seek(cx, &original_key)?;
                if !seek_res.is_found() {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "original key disappeared during interior delete".to_owned(),
                    });
                }

                let top_after = cursor
                    .stack
                    .last()
                    .ok_or_else(|| FrankenError::internal("cursor stack is empty"))?
                    .clone();
                if top_after.header.page_type.is_leaf() {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "interior delete re-seek landed on leaf".to_owned(),
                    });
                }

                // Replace the interior key first so failures do not lose both keys.
                let rebalanced = cursor.replace_interior_cell(cx, &successor_key)?;

                if rebalanced {
                    // The replacement triggered a rebalance on the interior node, which
                    // invalidated the cursor stack. We must re-seek to the successor key
                    // to find the duplicate leaf entry.
                    let seek_res = cursor.index_seek(cx, &successor_key)?;
                    if !seek_res.is_found() {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: "duplicate successor missing after interior rebalance"
                                .to_owned(),
                        });
                    }

                    let top_after_seek = cursor
                        .stack
                        .last()
                        .ok_or_else(|| {
                            FrankenError::internal("cursor stack empty after reseek in delete")
                        })?
                        .clone();
                    if !top_after_seek.header.page_type.is_leaf() {
                        // The seek found the interior node separator we just inserted.
                        // We must advance to the next logical entry to reach the duplicate in the leaf.
                        let successor_found = cursor.advance_next(cx)?;
                        if !successor_found || cursor.at_eof {
                            return Err(FrankenError::DatabaseCorrupt {
                                detail: "duplicate leaf successor missing after interior rebalance"
                                    .to_owned(),
                            });
                        }
                    }
                } else {
                    // The duplicate successor still exists as the next logical entry
                    // in the right subtree. Walk there from the interior replacement
                    // site instead of re-seeking, which would land back on the new
                    // interior separator.
                    let successor_found = cursor.advance_next(cx)?;
                    if !successor_found || cursor.at_eof {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: "duplicate successor missing after interior replacement"
                                .to_owned(),
                        });
                    }
                    let duplicate_successor = cursor.payload(cx)?;
                    if duplicate_successor != successor_key {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: "interior delete advanced to wrong successor duplicate"
                                .to_owned(),
                        });
                    }
                }

                // Remove the duplicate leaf successor.
                let (_leaf_pgno, new_count) = cursor.remove_cell_from_leaf(cx)?;
                if new_count == 0 {
                    cursor.balance_for_delete(cx)?;
                }

                // Delete contract: position cursor at the next logical entry.
                let _ = cursor.index_seek(cx, &successor_key)?;
                return Ok(());
            }

            // If the leaf is about to become empty (and it's not the root),
            // balancing will occur, which clears the cursor stack (invalidating position).
            // We must save the current key/rowid to re-establish position (at the successor)
            // after balancing.
            let depth = cursor.stack.len();
            let needs_anchor = depth > 1 && top.header.cell_count == 1;

            #[allow(clippy::items_after_statements)]
            enum Anchor {
                Rowid(i64),
                Key(Vec<u8>),
            }
            let anchor = if needs_anchor {
                if cursor.is_table {
                    Some(Anchor::Rowid(cursor.rowid(cx)?))
                } else {
                    Some(Anchor::Key(cursor.payload(cx)?))
                }
            } else {
                None
            };

            if cursor.is_table {
                let (_leaf_page_no, new_count) = cursor.remove_table_cell_from_leaf_deferred(cx)?;
                if new_count == 0 {
                    cursor.balance_for_delete(cx)?;

                    // If we balanced, the stack was cleared. Re-seek to the
                    // deleted rowid anchor; the seek lands on the successor
                    // (or EOF), which is the DELETE cursor contract.
                    if let Some(anc) = anchor {
                        match anc {
                            Anchor::Rowid(r) => {
                                cursor.table_move_to(cx, r)?;
                            }
                            Anchor::Key(k) => {
                                cursor.index_move_to(cx, &k)?;
                            }
                        }
                    }
                } else if let Some((page_no, separator_idx, new_max_rowid)) = separator_repair {
                    cursor.replace_table_interior_separator_rowid(
                        cx,
                        page_no,
                        separator_idx,
                        new_max_rowid,
                    )?;
                }
            } else {
                // Remove the cell from the leaf. This handles overflow chain
                // cleanup and refreshes the stack entry.
                let (_leaf_page_no, new_count) = cursor.remove_cell_from_leaf(cx)?;

                // Trigger structural rebalance only when a non-root leaf drains.
                if new_count == 0 {
                    cursor.balance_for_delete(cx)?;

                    // If we balanced, the stack was cleared. Re-seek to the anchor.
                    // Since the anchor key was just deleted, the seek will land on
                    // the *next* entry (or EOF), which is exactly what we want.
                    if let Some(anc) = anchor {
                        match anc {
                            Anchor::Rowid(r) => {
                                cursor.table_move_to(cx, r)?;
                            }
                            Anchor::Key(k) => {
                                cursor.index_move_to(cx, &k)?;
                            }
                        }
                    }
                }
            }

            Ok(())
        });
        if result.is_ok() {
            self.bump_row_image_epoch();
        }
        result
    }

    fn payload(&self, cx: &Cx) -> Result<Vec<u8>> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
        if let Some((_, payload)) = self.local_leaf_table_cell(top, top.cell_idx)? {
            instrumentation::record_owned_payload_materialization(payload.len());
            return Ok(payload.to_vec());
        }
        let cell = self.parse_cell_at(top, top.cell_idx)?;
        match self.read_cell_payload(cx, top, &cell)? {
            Cow::Borrowed(bytes) => {
                instrumentation::record_owned_payload_materialization(bytes.len());
                Ok(bytes.to_vec())
            }
            Cow::Owned(bytes) => Ok(bytes),
        }
    }

    fn payload_into(&self, cx: &Cx, buf: &mut Vec<u8>) -> Result<()> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
        if self.local_leaf_table_payload_into(top, top.cell_idx, buf)? {
            return Ok(());
        }
        let cell = self.parse_cell_at(top, top.cell_idx)?;

        self.read_cell_payload_into(cx, top, &cell, buf)
    }

    fn rowid_and_payload_into(&self, cx: &Cx, buf: &mut Vec<u8>) -> Result<i64> {
        let (rowid, payload) = self.rowid_and_payload_cow(cx)?;
        buf.clear();
        match payload {
            Cow::Borrowed(bytes) => {
                instrumentation::record_local_payload_copy(bytes.len());
                buf.extend_from_slice(bytes);
            }
            Cow::Owned(bytes) => {
                buf.extend_from_slice(&bytes);
            }
        }
        Ok(rowid)
    }

    fn rowid_and_payload_cow<'a>(&'a self, cx: &Cx) -> Result<(i64, Cow<'a, [u8]>)> {
        let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::BtreeCursor);
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
        if let Some(row) = self.local_leaf_table_rowid_and_payload(top, top.cell_idx)? {
            return Ok(row);
        }
        let cell = self.parse_cell_at_uncached(top, top.cell_idx)?;
        let payload = self.read_cell_payload(cx, top, &cell)?;
        if let Some(rowid) = cell.rowid {
            return Ok((rowid, payload));
        }

        // Index cursor fallback: match rowid() semantics, but reuse the
        // payload bytes already materialized above instead of reading them a
        // second time.
        let key_values =
            parse_record(payload.as_ref()).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "malformed index key record while extracting rowid".to_owned(),
            })?;
        let rowid = key_values
            .last()
            .and_then(fsqlite_types::SqliteValue::as_integer)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "index key record missing trailing integer rowid".to_owned(),
            })?;
        Ok((rowid, payload))
    }

    fn payload_prefix_into(
        &self,
        cx: &Cx,
        max_prefix_bytes: usize,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
        if self.local_leaf_table_payload_prefix_into(top, top.cell_idx, max_prefix_bytes, buf)? {
            return Ok(());
        }
        let cell = self.parse_cell_at(top, top.cell_idx)?;

        self.read_cell_payload_prefix_into(cx, top, &cell, max_prefix_bytes, buf)
    }

    fn rowid(&self, cx: &Cx) -> Result<i64> {
        let _record_profile_scope = enter_record_profile_scope(RecordProfileScope::BtreeCursor);
        if self.at_eof || self.stack.is_empty() {
            return Err(FrankenError::internal("cursor at EOF"));
        }
        let top = self
            .stack
            .last()
            .ok_or_else(|| FrankenError::internal("cursor stack empty"))?;
        if top.header.page_type.is_leaf() && top.header.page_type.is_table() {
            return Self::table_leaf_rowid_at(top, top.cell_idx);
        }
        let cell = self.parse_cell_at(top, top.cell_idx)?;
        if let Some(rowid) = cell.rowid {
            return Ok(rowid);
        }

        // Index cursor: rowid is stored as the trailing field in the
        // serialized key record.
        let key = self.read_cell_payload(cx, top, &cell)?;
        let key_values =
            parse_record(key.as_ref()).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "malformed index key record while extracting rowid".to_owned(),
            })?;
        key_values
            .last()
            .and_then(fsqlite_types::SqliteValue::as_integer)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "index key record missing trailing integer rowid".to_owned(),
            })
    }

    fn eof(&self) -> bool {
        self.at_eof
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use crate::instrumentation::{
        btree_leaf_reuse_snapshot, btree_metrics_snapshot, set_btree_metrics_enabled,
    };
    use fsqlite_pager::{MemoryMockMvccPager, MockMvccPager, MvccPager as _, TransactionMode};
    use fsqlite_types::SqliteValue;
    use fsqlite_types::record::serialize_record;
    use fsqlite_types::serial_type::write_varint;
    use proptest::strategy::Strategy as _;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::rc::Rc;
    use std::sync::{LazyLock, Mutex};
    use std::time::{Duration, Instant};

    // MemPageStore is now defined at module scope (pub) and imported via
    // `use super::*;`.  Tests use `MemPageStore::new(USABLE)` instead of
    // the former `MemPageStore::new(USABLE)`.
    static LEAF_REUSE_CURSOR_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct PrefetchProbeSnapshot {
        hinted_pages: Vec<PageNumber>,
        read_pages: Vec<PageNumber>,
        prefetch_issued_count: usize,
        prefetch_hit_count: usize,
        slot_prefetch_count: usize,
        page_prefetch_count: usize,
        invalid_slot_pages: Vec<PageNumber>,
        missing_page_pages: Vec<PageNumber>,
    }

    #[derive(Debug, Default)]
    struct PrefetchProbeState {
        hinted_pages: Vec<PageNumber>,
        read_pages: Vec<PageNumber>,
        pending_hints: BTreeMap<u32, usize>,
        prefetch_issued_count: usize,
        prefetch_hit_count: usize,
        slot_prefetch_count: usize,
        page_prefetch_count: usize,
        invalid_slot_pages: Vec<PageNumber>,
        missing_page_pages: Vec<PageNumber>,
    }

    #[derive(Debug, Clone)]
    struct PrefetchProbeStore {
        inner: MemPageStore,
        probe: Rc<RefCell<PrefetchProbeState>>,
    }

    impl PrefetchProbeStore {
        fn new(inner: MemPageStore) -> Self {
            Self {
                inner,
                probe: Rc::new(RefCell::new(PrefetchProbeState::default())),
            }
        }

        fn hinted_pages(&self) -> Vec<PageNumber> {
            self.probe.borrow().hinted_pages.clone()
        }

        fn clear_probe(&self) {
            *self.probe.borrow_mut() = PrefetchProbeState::default();
        }

        fn snapshot(&self) -> PrefetchProbeSnapshot {
            let probe = self.probe.borrow();
            PrefetchProbeSnapshot {
                hinted_pages: probe.hinted_pages.clone(),
                read_pages: probe.read_pages.clone(),
                prefetch_issued_count: probe.prefetch_issued_count,
                prefetch_hit_count: probe.prefetch_hit_count,
                slot_prefetch_count: probe.slot_prefetch_count,
                page_prefetch_count: probe.page_prefetch_count,
                invalid_slot_pages: probe.invalid_slot_pages.clone(),
                missing_page_pages: probe.missing_page_pages.clone(),
            }
        }
    }

    impl PageReader for PrefetchProbeStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            let mut probe = self.probe.borrow_mut();
            probe.read_pages.push(page_no);
            let mut counted_prefetch_hit = false;
            let remove_pending = if let Some(pending) = probe.pending_hints.get_mut(&page_no.get())
                && *pending > 0
            {
                *pending -= 1;
                counted_prefetch_hit = true;
                *pending == 0
            } else {
                false
            };
            if counted_prefetch_hit {
                probe.prefetch_hit_count = probe.prefetch_hit_count.saturating_add(1);
            }
            if remove_pending {
                probe.pending_hints.remove(&page_no.get());
            }
            self.inner.read_page(cx, page_no)
        }

        fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
            let mut probe = self.probe.borrow_mut();
            probe.hinted_pages.push(page_no);
            probe.prefetch_issued_count = probe.prefetch_issued_count.saturating_add(1);

            let Some(slot_idx) = MemPageStore::page_slot_index(page_no) else {
                probe.invalid_slot_pages.push(page_no);
                return;
            };

            if self.inner.page_slots.get(slot_idx).is_some() {
                probe.slot_prefetch_count = probe.slot_prefetch_count.saturating_add(1);
            } else {
                probe.invalid_slot_pages.push(page_no);
                return;
            }

            if self.inner.pages.contains_key(&page_no.get()) {
                probe.page_prefetch_count = probe.page_prefetch_count.saturating_add(1);
                *probe.pending_hints.entry(page_no.get()).or_default() += 1;
            } else {
                probe.missing_page_pages.push(page_no);
            }

            drop(probe);
            self.inner.prefetch_page_hint(cx, page_no);
        }
    }

    impl PageWriter for PrefetchProbeStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.inner.write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Default)]
    struct WitnessProbeState {
        coarse_page_data_reads: usize,
        btree_page_data_reads: usize,
        read_witnesses: Vec<WitnessKey>,
    }

    #[derive(Debug, Clone)]
    struct WitnessProbeStore {
        inner: MemPageStore,
        state: Rc<RefCell<WitnessProbeState>>,
    }

    impl WitnessProbeStore {
        fn new(inner: MemPageStore) -> Self {
            Self {
                inner,
                state: Rc::new(RefCell::new(WitnessProbeState::default())),
            }
        }

        fn state(&self) -> Rc<RefCell<WitnessProbeState>> {
            Rc::clone(&self.state)
        }
    }

    impl PageReader for WitnessProbeStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.inner.read_page(cx, page_no)
        }

        fn read_page_data(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
            self.state.borrow_mut().coarse_page_data_reads += 1;
            self.inner.read_page_data(cx, page_no)
        }

        fn read_btree_page_data(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
            self.state.borrow_mut().btree_page_data_reads += 1;
            self.inner.read_page_data(cx, page_no)
        }

        fn record_read_witness(&self, _cx: &Cx, key: WitnessKey) {
            self.state.borrow_mut().read_witnesses.push(key);
        }
    }

    impl PageWriter for WitnessProbeStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.inner.write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Clone)]
    struct SeekProbeStore {
        inner: MemPageStore,
        read_pages: Rc<RefCell<Vec<PageNumber>>>,
    }

    impl SeekProbeStore {
        fn new(inner: MemPageStore) -> Self {
            Self {
                inner,
                read_pages: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn clear_reads(&self) {
            self.read_pages.borrow_mut().clear();
        }

        fn read_pages(&self) -> Vec<PageNumber> {
            self.read_pages.borrow().clone()
        }
    }

    impl PageReader for SeekProbeStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.read_pages.borrow_mut().push(page_no);
            self.inner.read_page(cx, page_no)
        }

        fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
            self.inner.prefetch_page_hint(cx, page_no);
        }
    }

    impl PageWriter for SeekProbeStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.inner.write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Clone)]
    struct StagedMutationStore {
        inner: MemPageStore,
    }

    impl StagedMutationStore {
        fn new(inner: MemPageStore) -> Self {
            Self { inner }
        }
    }

    impl PageReader for StagedMutationStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.inner.read_page(cx, page_no)
        }
    }

    impl PageWriter for StagedMutationStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.inner.write_page(cx, page_no, data)
        }

        fn write_page_data(&mut self, cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
            self.inner.write_page_data(cx, page_no, data)
        }

        fn try_take_staged_page_data(&mut self, page_no: PageNumber) -> Option<PageData> {
            self.inner
                .pages
                .remove(&page_no.get())
                .map(PageData::from_vec)
        }

        fn try_mutate_staged_page_data(
            &mut self,
            page_no: PageNumber,
            f: &mut dyn FnMut(&mut PageData),
        ) -> bool {
            let Some(page) = self.inner.pages.get(&page_no.get()).cloned() else {
                return false;
            };
            let mut data = PageData::from_vec(page);
            f(&mut data);
            self.inner.pages.insert(page_no.get(), data.into_vec());
            self.inner.set_page_slot_present(page_no, true);
            true
        }

        fn restore_staged_page_data(
            &mut self,
            cx: &Cx,
            page_no: PageNumber,
            data: PageData,
        ) -> Result<()> {
            self.inner.write_page_data(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug)]
    struct FailingOverflowStore {
        inner: Rc<RefCell<MemPageStore>>,
        fail_on_write: usize,
        write_count: usize,
    }

    impl FailingOverflowStore {
        fn new(inner: Rc<RefCell<MemPageStore>>, fail_on_write: usize) -> Self {
            Self {
                inner,
                fail_on_write,
                write_count: 0,
            }
        }
    }

    impl PageReader for FailingOverflowStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.inner.borrow().read_page(cx, page_no)
        }
    }

    impl PageWriter for FailingOverflowStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.write_count = self.write_count.saturating_add(1);
            if self.write_count == self.fail_on_write {
                return Err(FrankenError::internal("injected write failure"));
            }
            self.inner.borrow_mut().write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.borrow_mut().allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.borrow_mut().free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Clone)]
    struct CancelAfterReadStore {
        inner: MemPageStore,
        cancelled: Rc<RefCell<bool>>,
    }

    impl CancelAfterReadStore {
        fn new(inner: MemPageStore) -> Self {
            Self {
                inner,
                cancelled: Rc::new(RefCell::new(false)),
            }
        }
    }

    impl PageReader for CancelAfterReadStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            let page = self.inner.read_page(cx, page_no)?;
            let mut cancelled = self.cancelled.borrow_mut();
            if !*cancelled {
                cx.cancel();
                *cancelled = true;
            }
            Ok(page)
        }

        fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
            self.inner.prefetch_page_hint(cx, page_no);
        }
    }

    impl PageWriter for CancelAfterReadStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.inner.write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey) {
            self.inner.record_write_witness(cx, key);
        }
    }

    #[derive(Debug)]
    struct CancelAfterFirstOverflowFreeStore {
        inner: Rc<RefCell<MemPageStore>>,
        cancelled: Rc<RefCell<bool>>,
    }

    impl CancelAfterFirstOverflowFreeStore {
        fn new(inner: Rc<RefCell<MemPageStore>>) -> Self {
            Self {
                inner,
                cancelled: Rc::new(RefCell::new(false)),
            }
        }
    }

    impl PageReader for CancelAfterFirstOverflowFreeStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            self.inner.borrow().read_page(cx, page_no)
        }
    }

    impl PageWriter for CancelAfterFirstOverflowFreeStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.inner.borrow_mut().write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.borrow_mut().allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            self.inner.borrow_mut().free_page(cx, page_no)?;

            let mut cancelled = self.cancelled.borrow_mut();
            if !*cancelled {
                cx.cancel();
                *cancelled = true;
            }
            Ok(())
        }

        fn record_write_witness(&mut self, cx: &Cx, key: WitnessKey) {
            self.inner.borrow_mut().record_write_witness(cx, key);
        }
    }

    const USABLE: u32 = 4096;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TableSubtreeBounds {
        min_rowid: i64,
        max_rowid: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct IndexSubtreeBounds {
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        entry_count: usize,
    }

    fn collect_reachable_pages(
        store: &MemPageStore,
        page_no: PageNumber,
        usable_size: u32,
        out: &mut BTreeSet<u32>,
    ) {
        if !out.insert(page_no.get()) {
            return;
        }

        let page = store
            .pages
            .get(&page_no.get())
            .expect("reachable page should exist in store");
        let header_offset = cell::header_offset_for_page(page_no);
        let header = BtreePageHeader::parse(page, header_offset).expect("page header should parse");

        if !header.page_type.is_interior() {
            return;
        }

        let ptrs =
            cell::read_cell_pointers(page, &header, header_offset).expect("cell pointers parse");
        for ptr in ptrs {
            let cell = CellRef::parse(page, usize::from(ptr), header.page_type, usable_size)
                .expect("interior cell should parse");
            let child = cell
                .left_child
                .expect("interior cell should reference child");
            collect_reachable_pages(store, child, usable_size, out);
        }
        let right_child = header
            .right_child
            .expect("interior page should reference right child");
        collect_reachable_pages(store, right_child, usable_size, out);
    }

    fn validate_table_tree_invariants<P: PageReader>(
        pager: &P,
        root: PageNumber,
        usable_size: u32,
    ) -> Result<Option<TableSubtreeBounds>> {
        let cx = Cx::new();
        let mut visited = BTreeSet::new();
        validate_table_subtree_invariants(pager, &cx, root, usable_size, true, &mut visited)
    }

    fn validate_table_subtree_invariants<P: PageReader>(
        pager: &P,
        cx: &Cx,
        page_no: PageNumber,
        usable_size: u32,
        _is_root: bool,
        visited: &mut BTreeSet<u32>,
    ) -> Result<Option<TableSubtreeBounds>> {
        if !visited.insert(page_no.get()) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("table b-tree contains a cycle at page {}", page_no.get()),
            });
        }

        let page = pager.read_page(cx, page_no)?;
        let header_offset = cell::header_offset_for_page(page_no);
        let header = BtreePageHeader::parse(&page, header_offset)?;
        if !header.page_type.is_table() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "expected table b-tree page at {}, found {:?}",
                    page_no.get(),
                    header.page_type
                ),
            });
        }

        let ptrs = cell::read_cell_pointers(&page, &header, header_offset)?;
        if header.page_type == cell::BtreePageType::LeafTable {
            if ptrs.is_empty() {
                return Ok(None);
            }

            let mut min_rowid = None;
            let mut max_rowid = None;
            let mut prev_rowid = None;
            for ptr in ptrs {
                let cell = CellRef::parse(&page, usize::from(ptr), header.page_type, usable_size)?;
                let rowid = cell.rowid.ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "table leaf cell on page {} is missing a rowid",
                        page_no.get()
                    ),
                })?;
                if let Some(prev) = prev_rowid
                    && rowid <= prev
                {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "table leaf page {} rowids are out of order: {} after {}",
                            page_no.get(),
                            rowid,
                            prev
                        ),
                    });
                }
                min_rowid.get_or_insert(rowid);
                max_rowid = Some(rowid);
                prev_rowid = Some(rowid);
            }

            return Ok(Some(TableSubtreeBounds {
                min_rowid: min_rowid.expect("non-empty leaf must have a minimum rowid"),
                max_rowid: max_rowid.expect("non-empty leaf must have a maximum rowid"),
            }));
        }

        if ptrs.is_empty() {
            let right_child = header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "interior table page {} is missing a right child",
                        page_no.get()
                    ),
                })?;
            return validate_table_subtree_invariants(
                pager,
                cx,
                right_child,
                usable_size,
                false,
                visited,
            );
        }

        let mut overall_min = None;
        let mut prev_separator = None;
        for ptr in ptrs {
            let cell = CellRef::parse(&page, usize::from(ptr), header.page_type, usable_size)?;
            let left_child = cell
                .left_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "interior table page {} divider is missing a left child",
                        page_no.get()
                    ),
                })?;
            let separator = cell.rowid.ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior table page {} divider is missing a rowid",
                    page_no.get()
                ),
            })?;
            let child_bounds = validate_table_subtree_invariants(
                pager,
                cx,
                left_child,
                usable_size,
                false,
                visited,
            )?;

            if let Some(prev) = prev_separator {
                if separator <= prev {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior table page {} separators are out of order: {} after {}",
                            page_no.get(),
                            separator,
                            prev
                        ),
                    });
                }
                if child_bounds.is_some_and(|bounds| bounds.min_rowid <= prev) {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior table page {} child {} overlaps prior separator {}",
                            page_no.get(),
                            left_child.get(),
                            prev
                        ),
                    });
                }
            }

            if let Some(child_bounds) = child_bounds {
                if child_bounds.max_rowid > separator {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior table page {} child {} max {} exceeds separator {}",
                            page_no.get(),
                            left_child.get(),
                            child_bounds.max_rowid,
                            separator
                        ),
                    });
                }
                overall_min.get_or_insert(child_bounds.min_rowid);
            }
            prev_separator = Some(separator);
        }

        let right_child = header
            .right_child
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior table page {} is missing a right child",
                    page_no.get()
                ),
            })?;
        let right_bounds =
            validate_table_subtree_invariants(pager, cx, right_child, usable_size, false, visited)?;

        if let Some(prev) = prev_separator
            && right_bounds.is_some_and(|bounds| bounds.min_rowid <= prev)
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior table page {} right child {} overlaps separator {}",
                    page_no.get(),
                    right_child.get(),
                    prev
                ),
            });
        }

        if let Some(right_bounds) = right_bounds {
            return Ok(Some(TableSubtreeBounds {
                min_rowid: overall_min.unwrap_or(right_bounds.min_rowid),
                max_rowid: right_bounds.max_rowid,
            }));
        }

        if let Some(min_rowid) = overall_min {
            return Ok(Some(TableSubtreeBounds {
                min_rowid,
                max_rowid: prev_separator
                    .expect("interior table page with cells must have a separator"),
            }));
        }

        Ok(None)
    }

    fn compare_index_test_keys<P: PageReader>(
        cursor: &BtCursor<P>,
        lhs: &[u8],
        rhs: &[u8],
    ) -> std::cmp::Ordering {
        let parsed_rhs = parse_record(rhs);
        cursor.compare_index_key_bytes(lhs, rhs, parsed_rhs.as_deref())
    }

    fn validate_index_tree_invariants<P: PageReader>(
        cursor: &mut BtCursor<P>,
        root: PageNumber,
    ) -> Result<Option<IndexSubtreeBounds>> {
        let cx = Cx::new();
        let mut visited = BTreeSet::new();
        validate_index_subtree_invariants(cursor, &cx, root, true, &mut visited)
    }

    fn validate_index_subtree_invariants<P: PageReader>(
        cursor: &mut BtCursor<P>,
        cx: &Cx,
        page_no: PageNumber,
        is_root: bool,
        visited: &mut BTreeSet<u32>,
    ) -> Result<Option<IndexSubtreeBounds>> {
        if !visited.insert(page_no.get()) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("index b-tree contains a cycle at page {}", page_no.get()),
            });
        }

        let entry = cursor.load_page(cx, page_no)?;
        if !entry.header.page_type.is_index() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "expected index b-tree page at {}, found {:?}",
                    page_no.get(),
                    entry.header.page_type
                ),
            });
        }

        if entry.header.page_type == cell::BtreePageType::LeafIndex {
            if entry.cell_pointers.is_empty() {
                if is_root {
                    return Ok(None);
                }
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("non-root index leaf page {} is empty", page_no.get()),
                });
            }

            let mut min_key = None;
            let mut max_key = None;
            let mut prev_key = None::<Vec<u8>>;
            for idx in 0..entry.header.cell_count {
                let cell = cursor.parse_cell_at(&entry, idx)?;
                let key = cursor.read_cell_payload(cx, &entry, &cell)?.into_owned();
                if let Some(prev) = &prev_key
                    && compare_index_test_keys(cursor, prev, &key) != std::cmp::Ordering::Less
                {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "index leaf page {} keys are out of order or duplicated",
                            page_no.get()
                        ),
                    });
                }
                if min_key.is_none() {
                    min_key = Some(key.clone());
                }
                max_key = Some(key.clone());
                prev_key = Some(key);
            }

            return Ok(Some(IndexSubtreeBounds {
                min_key: min_key.expect("non-empty index leaf must have a minimum key"),
                max_key: max_key.expect("non-empty index leaf must have a maximum key"),
                entry_count: usize::from(entry.header.cell_count),
            }));
        }

        if entry.header.cell_count == 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior index page {} has no separator cells",
                    page_no.get()
                ),
            });
        }

        let mut overall_min = None::<Vec<u8>>;
        let mut prev_separator = None::<Vec<u8>>;
        let mut entry_count = 0usize;

        for idx in 0..entry.header.cell_count {
            let left_child = BtCursor::<P>::child_page_at(&entry, idx)?;
            let left_bounds =
                validate_index_subtree_invariants(cursor, cx, left_child, false, visited)?
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior index page {} points to an empty child subtree {}",
                            page_no.get(),
                            left_child.get()
                        ),
                    })?;

            let cell = cursor.parse_cell_at(&entry, idx)?;
            let separator_key = cursor.read_cell_payload(cx, &entry, &cell)?.into_owned();

            if compare_index_test_keys(cursor, &left_bounds.max_key, &separator_key)
                != std::cmp::Ordering::Less
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "interior index page {} separator does not sort after child {} max key",
                        page_no.get(),
                        left_child.get()
                    ),
                });
            }
            if let Some(prev) = &prev_separator {
                if compare_index_test_keys(cursor, prev, &separator_key) != std::cmp::Ordering::Less
                {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior index page {} separators are out of order or duplicated",
                            page_no.get()
                        ),
                    });
                }
                if compare_index_test_keys(cursor, prev, &left_bounds.min_key)
                    != std::cmp::Ordering::Less
                {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: format!(
                            "interior index page {} child {} overlaps the prior separator range",
                            page_no.get(),
                            left_child.get()
                        ),
                    });
                }
            }

            if overall_min.is_none() {
                overall_min = Some(left_bounds.min_key.clone());
            }
            prev_separator = Some(separator_key);
            entry_count = entry_count
                .checked_add(left_bounds.entry_count + 1)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "index subtree entry count overflow".to_owned(),
                })?;
        }

        let right_child =
            entry
                .header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "interior index page {} is missing a right child",
                        page_no.get()
                    ),
                })?;
        let right_bounds =
            validate_index_subtree_invariants(cursor, cx, right_child, false, visited)?
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "interior index page {} points to an empty right subtree {}",
                        page_no.get(),
                        right_child.get()
                    ),
                })?;

        if let Some(prev) = &prev_separator
            && compare_index_test_keys(cursor, prev, &right_bounds.min_key)
                != std::cmp::Ordering::Less
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "interior index page {} right child {} overlaps the last separator range",
                    page_no.get(),
                    right_child.get()
                ),
            });
        }

        entry_count = entry_count
            .checked_add(right_bounds.entry_count)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "index subtree entry count overflow".to_owned(),
            })?;

        Ok(Some(IndexSubtreeBounds {
            min_key: overall_min.expect("interior index page with cells must have a minimum key"),
            max_key: right_bounds.max_key,
            entry_count,
        }))
    }

    fn scan_all_index_keys<P: PageWriter>(
        cursor: &mut BtCursor<P>,
        cx: &Cx,
    ) -> Result<Vec<Vec<u8>>> {
        let mut scanned = Vec::new();
        if cursor.first(cx)? {
            loop {
                scanned.push(cursor.payload(cx)?);
                if !cursor.next(cx)? {
                    break;
                }
            }
        }
        Ok(scanned)
    }

    fn synthetic_index_key(id: i64) -> Vec<u8> {
        serialize_record(&[
            SqliteValue::Integer(id.rem_euclid(17)),
            SqliteValue::Integer(id),
        ])
    }

    #[test]
    fn test_btree_observability_operation_totals() {
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let before = btree_metrics_snapshot();

        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        cursor.table_insert(&cx, 7, b"payload").unwrap();
        assert!(cursor.table_move_to(&cx, 7).unwrap().is_found());
        cursor.delete(&cx).unwrap();

        let after = btree_metrics_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            after.fsqlite_btree_operations_total.seek
                >= before.fsqlite_btree_operations_total.seek.saturating_add(1)
        );
        assert!(
            after.fsqlite_btree_operations_total.insert
                >= before
                    .fsqlite_btree_operations_total
                    .insert
                    .saturating_add(1)
        );
        assert!(
            after.fsqlite_btree_operations_total.delete
                >= before
                    .fsqlite_btree_operations_total
                    .delete
                    .saturating_add(1)
        );
        assert!(after.fsqlite_btree_depth >= 1);
    }

    #[test]
    fn test_rowid_and_payload_into_matches_separate_accessors() -> Result<()> {
        let cx = Cx::new();

        let table_root = pn(2);
        let table_store = MemPageStore::with_empty_table(table_root, USABLE);
        let mut table_cursor = BtCursor::new(table_store, table_root, USABLE, true);
        table_cursor.table_insert(&cx, 42, b"table-payload")?;
        assert!(table_cursor.table_move_to(&cx, 42)?.is_found());

        let mut payload = Vec::new();
        let rowid = table_cursor.rowid_and_payload_into(&cx, &mut payload)?;
        assert_eq!(rowid, table_cursor.rowid(&cx)?);
        assert_eq!(payload, table_cursor.payload(&cx)?);

        let index_root = pn(3);
        let index_store = MemPageStore::with_empty_index(index_root, USABLE);
        let mut index_cursor = BtCursor::new(index_store, index_root, USABLE, false);
        let key = serialize_record(&[SqliteValue::Text("k".into()), SqliteValue::Integer(99)]);
        index_cursor.index_insert(&cx, &key)?;
        assert!(index_cursor.index_move_to(&cx, &key)?.is_found());

        payload.clear();
        let rowid = index_cursor.rowid_and_payload_into(&cx, &mut payload)?;
        assert_eq!(rowid, index_cursor.rowid(&cx)?);
        assert_eq!(payload, index_cursor.payload(&cx)?);
        Ok(())
    }

    #[test]
    fn test_btree_observability_split_counter_and_depth_gauge() {
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let before = btree_metrics_snapshot();

        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        // Use large payloads to force page splits quickly.  With 9KB
        // payloads on a 4096-byte page, each insert uses overflow pages
        // and the root splits after just a few rows.
        let payload = vec![0xAB; 9_000];
        let mut inserts_done: i64 = 0;
        for rowid in 1_i64..=500_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            inserts_done = rowid;

            let root_page = cursor.pager.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }

            assert!(
                rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        let snapshot = btree_metrics_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            snapshot.fsqlite_btree_page_splits_total > before.fsqlite_btree_page_splits_total,
            "expected at least one split when loading large rows"
        );
        // The insert counter must have increased by at least the number
        // of rows we actually inserted (the loop may break early when
        // the root splits).
        assert!(
            snapshot.fsqlite_btree_operations_total.insert
                >= before
                    .fsqlite_btree_operations_total
                    .insert
                    .saturating_add(inserts_done as u64),
            "insert counter should reflect at least {inserts_done} inserts"
        );
    }

    #[test]
    fn test_table_insert_prechecked_absent_reuses_successor_position() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        cursor.table_insert(&cx, 10, b"ten").unwrap();
        cursor.table_insert(&cx, 30, b"thirty").unwrap();

        let seek = cursor.table_move_to(&cx, 20).unwrap();
        assert_eq!(seek, SeekResult::NotFound);
        assert!(!cursor.eof(), "seek should land on successor rowid 30");

        cursor
            .table_insert_prechecked_absent(&cx, 20, b"twenty")
            .unwrap();

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert_eq!(cursor.payload(&cx).unwrap(), b"ten");

        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 20);
        assert_eq!(cursor.payload(&cx).unwrap(), b"twenty");

        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 30);
        assert_eq!(cursor.payload(&cx).unwrap(), b"thirty");
    }

    #[test]
    fn test_position_stamp_changes_when_same_leaf_slot_is_rewritten() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        cursor.table_insert(&cx, 1, b"one").unwrap();
        cursor.table_insert(&cx, 2, b"two").unwrap();

        assert!(cursor.first(&cx).unwrap());
        let before = cursor
            .position_stamp()
            .expect("cursor should be positioned");

        cursor.delete(&cx).unwrap();
        let after = cursor
            .position_stamp()
            .expect("delete should land on successor");

        assert_eq!(before.page_no(), after.page_no());
        assert_eq!(before.cell_idx(), after.cell_idx());
        assert_ne!(
            before, after,
            "same-slot successor after delete must advance the row-image epoch"
        );
    }

    #[test]
    fn test_table_insert_prechecked_absent_reuses_eof_position() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three"), (4, b"four")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let seek = cursor.table_move_to(&cx, 99).unwrap();
        assert_eq!(seek, SeekResult::NotFound);
        assert!(
            cursor.eof(),
            "seek past end should preserve EOF insertion context"
        );

        cursor
            .table_insert_prechecked_absent(&cx, 99, b"tail")
            .unwrap();

        assert!(cursor.table_move_to(&cx, 99).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"tail");
    }

    #[test]
    fn test_table_insert_prechecked_absent_reuses_eof_after_tail_delete() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        cursor.table_insert(&cx, 1, b"one").unwrap();
        cursor.table_insert(&cx, 2, b"two").unwrap();
        cursor.table_insert(&cx, 3, b"three").unwrap();

        assert!(cursor.table_move_to(&cx, 3).unwrap().is_found());
        cursor.delete(&cx).unwrap();
        assert!(
            cursor.eof(),
            "tail delete should leave an EOF insertion context"
        );

        cursor
            .table_insert_prechecked_absent(&cx, 3, b"THREE-updated")
            .unwrap();

        assert!(cursor.table_move_to(&cx, 3).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"THREE-updated");
    }

    #[test]
    fn test_table_insert_prechecked_absent_deep_tree_rightmost_10k() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let row_count = 10_000_i64;

        for rowid in 0..row_count {
            let payload = format!("row-{rowid}");
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert_eq!(
                seek,
                SeekResult::NotFound,
                "monotonic prechecked insert should not find existing rowid {rowid}"
            );
            cursor
                .table_insert_prechecked_absent(&cx, rowid, payload.as_bytes())
                .unwrap();
        }

        let counted = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            counted, row_count,
            "deep/rightmost prechecked-absent table with {row_count} rows must count exactly"
        );

        let seek = cursor.table_move_to(&cx, row_count - 1).unwrap();
        assert_eq!(seek, SeekResult::Found);
        assert_eq!(
            cursor.payload(&cx).unwrap(),
            format!("row-{}", row_count - 1).as_bytes()
        );
    }

    /// A delete of a singleton leaf may rebalance the tree and position the
    /// cursor on a successor leaf in a different ancestor subtree. Reusing
    /// that position to restore the deleted row must not put the row on the
    /// wrong side of an interior-table separator.
    #[test]
    fn test_delete_reinsert_keeps_deep_table_rowids_seekable() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        const ROW_COUNT: i64 = 700;
        let payload = vec![0x6D; 3_500];
        for rowid in 1..=ROW_COUNT {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        assert!(
            cursor.measure_tree_depth(&cx).unwrap() >= 3,
            "the regression needs an ancestor separator above the leaf parent"
        );

        for rowid in 1..=ROW_COUNT {
            assert_eq!(
                cursor.table_move_to(&cx, rowid).unwrap(),
                SeekResult::Found,
                "rowid {rowid} must exist before replacement"
            );
            cursor.delete(&cx).unwrap();
            cursor
                .table_insert_prechecked_absent(&cx, rowid, &payload)
                .unwrap();
        }

        for rowid in 1..=ROW_COUNT {
            assert_eq!(
                cursor.table_move_to(&cx, rowid).unwrap(),
                SeekResult::Found,
                "rowid {rowid} must remain reachable through table routing"
            );
        }
        assert_eq!(cursor.count_all_rows(&cx).unwrap(), ROW_COUNT);
    }

    /// The revalidation guard must fail loud — not corrupt — when a caller
    /// violates the "prechecked absent" contract itself: reusing a stale
    /// NotFound position for a rowid that is actually present, where the
    /// stale position ALSO fails admissibility. The fallback re-seek finds
    /// the row and reports `DatabaseCorrupt` instead of inserting a
    /// duplicate or splicing the tree out of order.
    #[test]
    fn test_prechecked_absent_present_row_at_inadmissible_position_fails_loud() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let payload = vec![0x3Cu8; 3600];
        for rowid in 1..=200_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        // Position the cursor far from rowid 7's true slot (EOF context from
        // seeking past the end), then violate the contract: rowid 7 exists.
        assert_eq!(
            cursor.table_move_to(&cx, 10_000).unwrap(),
            SeekResult::NotFound
        );
        assert!(cursor.eof());
        let err = cursor
            .table_insert_prechecked_absent(&cx, 7, &payload)
            .expect_err("present rowid at an inadmissible reused position must fail loud");
        assert!(
            matches!(err, FrankenError::DatabaseCorrupt { .. }),
            "expected DatabaseCorrupt, got {err:?}"
        );

        // The tree must be unharmed: exactly one rowid 7, full count intact.
        assert_eq!(cursor.table_move_to(&cx, 7).unwrap(), SeekResult::Found);
        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 200);
    }

    /// Helper: build a leaf table page with sorted (rowid, payload) entries.
    fn build_leaf_table(entries: &[(i64, &[u8])]) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 8usize; // leaf

        // Build cells from the end of the page.
        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::with_capacity(entries.len());

        for &(rowid, payload) in entries {
            // Cell: [payload_size varint] [rowid varint] [payload]
            let mut cell = Vec::with_capacity(payload.len() + 18);
            let mut vbuf = [0u8; 9];
            let n = write_varint(&mut vbuf, payload.len() as u64);
            cell.extend_from_slice(&vbuf[..n]);
            #[allow(clippy::cast_sign_loss)]
            let n = write_varint(&mut vbuf, rowid as u64);
            cell.extend_from_slice(&vbuf[..n]);
            cell.extend_from_slice(payload);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        // Write header.
        page[0] = 0x0D; // LeafTable
        page[1..3].copy_from_slice(&0u16.to_be_bytes()); // no freeblock
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = entries.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0; // fragmented bytes

        // Write cell pointer array.
        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    fn stack_cell_cache_matches_fresh_parse<P: PageWriter>(
        cursor: &BtCursor<P>,
    ) -> std::result::Result<(), String> {
        for entry in &cursor.stack {
            for idx in 0..entry.header.cell_count {
                let cached = cursor.parse_cell_at(entry, idx).map_err(|err| {
                    format!(
                        "cached parse failed for page {} cell {idx}: {err}",
                        entry.page_no
                    )
                })?;
                let cell_offset = usize::from(
                    BtCursor::<P>::read_stack_entry_cell_pointer_inline(entry, idx).map_err(
                        |err| {
                            format!(
                                "fresh pointer read failed for page {} cell {idx}: {err}",
                                entry.page_no
                            )
                        },
                    )?,
                );
                let fresh = CellRef::parse(
                    entry.page_data.as_bytes(),
                    cell_offset,
                    entry.header.page_type,
                    cursor.usable_size,
                )
                .map_err(|err| {
                    format!(
                        "fresh parse failed for page {} cell {idx} at offset {cell_offset}: {err}",
                        entry.page_no
                    )
                })?;
                if CachedCellSlot::from_cell_ref(&cached) != CachedCellSlot::from_cell_ref(&fresh) {
                    return Err(format!(
                        "cached cell slot diverged from fresh parse for page {} cell {idx}",
                        entry.page_no
                    ));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_cell_slot_cache_invalidation_tracks_page_image_mutation() {
        let cx = Cx::new();
        let root = pn(2);
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(root.get(), build_leaf_table(&[(1, b"one"), (2, b"two")]));
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let first_entry = cursor.load_page(&cx, root).unwrap();
        let first_cell = cursor.parse_cell_at(&first_entry, 0).unwrap();
        assert_eq!(first_cell.rowid, Some(1));
        let first_counter = first_entry.mutation_counter;
        assert_eq!(cursor.cell_slot_cache.borrow().entries.len(), 1);
        assert_eq!(cursor.cell_slot_cache.borrow().entries[0].slots.len(), 1);

        let second_cell = cursor.parse_cell_at(&first_entry, 1).unwrap();
        assert_eq!(second_cell.rowid, Some(2));
        assert_eq!(cursor.cell_slot_cache.borrow().entries.len(), 1);
        assert_eq!(cursor.cell_slot_cache.borrow().entries[0].slots.len(), 2);

        cursor
            .pager
            .write_page(
                &cx,
                root,
                &build_leaf_table(&[(10, b"ten"), (20, b"twenty")]),
            )
            .unwrap();
        let mutated_entry = cursor.reload_page_fresh(&cx, root).unwrap();
        assert_ne!(mutated_entry.mutation_counter, first_counter);
        let mutated_cell = cursor.parse_cell_at(&mutated_entry, 0).unwrap();
        assert_eq!(mutated_cell.rowid, Some(10));
        assert_eq!(cursor.cell_slot_cache.borrow().entries.len(), 2);
    }

    #[test]
    fn test_table_leaf_rowid_at_rejects_out_of_range_cell_pointer() {
        let cx = Cx::new();
        let root = pn(2);
        let mut page = build_leaf_table(&[(1, b"one"), (2, b"two")]);
        page[8..10].copy_from_slice(&u16::MAX.to_be_bytes());

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(root.get(), page);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let entry = cursor.load_page(&cx, root).expect("page header loads");

        let err = BtCursor::<MemPageStore>::table_leaf_rowid_at(&entry, 0)
            .expect_err("out-of-range cell pointer must be corruption, not panic");
        assert!(
            matches!(&err, FrankenError::DatabaseCorrupt { detail } if detail.contains("points past page end")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_table_interior_search_rejects_out_of_range_cell_pointer() {
        let cx = Cx::new();
        let root = pn(2);
        let mut page = build_interior_table(&[(pn(3), 10)], pn(4));
        page[12..14].copy_from_slice(&u16::MAX.to_be_bytes());

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(root.get(), page);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let entry = cursor.load_page(&cx, root).expect("page header loads");

        let err = BtCursor::<MemPageStore>::binary_search_table_interior(&cx, &entry, 5)
            .expect_err("out-of-range interior pointer must be corruption, not panic");
        assert!(
            matches!(&err, FrankenError::DatabaseCorrupt { detail } if detail.contains("points past page end")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_transaction_page_io_reads_bytes_from_transaction_handle() {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let mut txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .expect("mock transaction begin should succeed");
        let page_no = PageNumber::new(42).expect("page number must be non-zero");

        let io = TransactionPageIo::new(&mut txn);
        let bytes = io
            .read_page(&cx, page_no)
            .expect("read_page should forward to transaction handle");

        assert_eq!(
            bytes.get(..4),
            Some(&page_no.get().to_le_bytes()[..]),
            "TransactionHandle::get_page stamps page number in first 4 bytes"
        );
    }

    #[test]
    fn test_transaction_page_io_writes_owned_page_data_via_transaction_handle() {
        let cx = Cx::new();
        let pager = MemoryMockMvccPager;
        let mut txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .expect("mock transaction begin should succeed");
        let page_no = PageNumber::new(2).expect("page number must be non-zero");
        let expected = vec![0xAB; 32];

        let mut io = TransactionPageIo::new(&mut txn);
        io.write_page_data(&cx, page_no, PageData::from_vec(expected.clone()))
            .expect("write_page_data should forward");

        let bytes = io
            .read_page(&cx, page_no)
            .expect("read_page should return the owned bytes");
        assert_eq!(
            bytes.len(),
            fsqlite_types::PageSize::default().as_usize(),
            "owned-page writes should preserve the page-size invariant"
        );
        assert_eq!(&bytes[..expected.len()], expected.as_slice());
        assert!(
            bytes[expected.len()..].iter().all(|byte| *byte == 0),
            "owned-page writes should zero-fill any unwritten tail bytes"
        );
    }

    #[test]
    fn test_mem_page_store_write_page_short_buffer_is_zero_filled_to_page_size() {
        let cx = Cx::new();
        let page_size = 128_u32;
        let mut store = MemPageStore::new(page_size);
        let page_no = PageNumber::new(2).expect("page number must be non-zero");
        let expected = vec![0xCD; 32];

        store
            .write_page(&cx, page_no, &expected)
            .expect("write_page should normalize short buffers");

        let bytes = store
            .read_page(&cx, page_no)
            .expect("read_page should return normalized page bytes");
        assert_eq!(
            bytes.len(),
            page_size as usize,
            "raw write_page should preserve the page-size invariant"
        );
        assert_eq!(&bytes[..expected.len()], expected.as_slice());
        assert!(
            bytes[expected.len()..].iter().all(|byte| *byte == 0),
            "raw write_page should zero-fill any unwritten tail bytes"
        );
    }

    #[test]
    fn test_transaction_page_io_allocates_pages_via_transaction_handle() {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let mut txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .expect("mock transaction begin should succeed");

        let mut io = TransactionPageIo::new(&mut txn);
        let first = io.allocate_page(&cx).expect("allocate_page should forward");
        let second = io.allocate_page(&cx).expect("allocate_page should forward");

        assert_eq!(first.get(), 2, "mock allocator starts at page 2");
        assert_eq!(second.get(), 3, "mock allocator increments page numbers");
    }

    #[test]
    fn test_transaction_page_io_writes_and_frees_via_transaction_handle() {
        let cx = Cx::new();
        let pager = MockMvccPager;
        let mut txn = pager
            .begin(&cx, TransactionMode::Deferred)
            .expect("mock transaction begin should succeed");
        let page_no = PageNumber::new(2).expect("page number must be non-zero");

        let mut io = TransactionPageIo::new(&mut txn);
        io.write_page(&cx, page_no, &[0_u8; 32])
            .expect("write_page should forward");
        io.free_page(&cx, page_no)
            .expect("free_page should forward");
    }

    /// Helper: build an interior table page.
    ///
    /// `children` is a list of `(left_child, rowid)` pairs plus a final right_child.
    fn build_interior_table(children: &[(PageNumber, i64)], right_child: PageNumber) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 12usize; // interior

        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::with_capacity(children.len());

        for &(left_child, rowid) in children {
            // Interior table cell: [left_child: u32 BE] [rowid: varint]
            let mut cell = Vec::with_capacity(13);
            cell.extend_from_slice(&left_child.get().to_be_bytes());
            let mut vbuf = [0u8; 9];
            #[allow(clippy::cast_sign_loss)]
            let n = write_varint(&mut vbuf, rowid as u64);
            cell.extend_from_slice(&vbuf[..n]);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        // Write header.
        page[0] = 0x05; // InteriorTable
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = children.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0;
        page[8..12].copy_from_slice(&right_child.get().to_be_bytes());

        // Write cell pointer array.
        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    /// Helper: build an interior index page.
    ///
    /// `children` is a list of `(left_child, key)` pairs plus a final right_child.
    fn build_interior_index(children: &[(PageNumber, &[u8])], right_child: PageNumber) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 12usize; // interior

        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::with_capacity(children.len());

        for &(left_child, key) in children {
            // Interior index cell: [left_child: u32 BE] [payload_size varint] [payload]
            let mut cell = Vec::with_capacity(4 + 9 + key.len());
            cell.extend_from_slice(&left_child.get().to_be_bytes());
            let mut vbuf = [0u8; 9];
            let n = write_varint(&mut vbuf, key.len() as u64);
            cell.extend_from_slice(&vbuf[..n]);
            cell.extend_from_slice(key);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        page[0] = 0x02; // InteriorIndex
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = children.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0;
        page[8..12].copy_from_slice(&right_child.get().to_be_bytes());

        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    /// Helper: build a leaf index page with sorted key payloads.
    fn build_leaf_index(entries: &[&[u8]]) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let header_size = 8usize; // leaf

        let mut cell_end = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::with_capacity(entries.len());

        for &key in entries {
            let mut cell = Vec::with_capacity(9 + key.len());
            let mut vbuf = [0u8; 9];
            let n = write_varint(&mut vbuf, key.len() as u64);
            cell.extend_from_slice(&vbuf[..n]);
            cell.extend_from_slice(key);

            cell_end -= cell.len();
            page[cell_end..cell_end + cell.len()].copy_from_slice(&cell);
            cell_offsets.push(cell_end as u16);
        }

        page[0] = 0x0A; // LeafIndex
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let cell_count = entries.len() as u16;
        page[3..5].copy_from_slice(&cell_count.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let content_offset = cell_end as u16;
        page[5..7].copy_from_slice(&content_offset.to_be_bytes());
        page[7] = 0;

        for (i, &off) in cell_offsets.iter().enumerate() {
            let ptr_offset = header_size + i * 2;
            page[ptr_offset..ptr_offset + 2].copy_from_slice(&off.to_be_bytes());
        }

        page
    }

    fn pn(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn lcg_next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        *state
    }

    fn deterministic_shuffle(values: &mut [i64], seed: u64) {
        if values.len() <= 1 {
            return;
        }
        let mut state = seed;
        for i in (1..values.len()).rev() {
            let j = (lcg_next(&mut state) as usize) % (i + 1);
            values.swap(i, j);
        }
    }

    fn payload_for_rowid(rowid: i64) -> Vec<u8> {
        let rowid_usize = usize::try_from(rowid).expect("rowid must be positive in this test");
        let payload_len = if rowid % 257 == 0 {
            1_600 // force overflow-chain path for some keys
        } else {
            32 + (rowid_usize % 180)
        };

        let mut payload = Vec::with_capacity(payload_len);
        for i in 0..payload_len {
            let byte = (rowid_usize.wrapping_mul(31).wrapping_add(i * 17) & 0xFF) as u8;
            payload.push(byte);
        }
        payload
    }

    fn build_prefetch_descent_probe_store() -> PrefetchProbeStore {
        let cx = Cx::new();
        let mut store = MemPageStore::new(USABLE);
        store
            .write_page(&cx, pn(2), &build_interior_table(&[(pn(3), 15)], pn(4)))
            .unwrap();
        store
            .write_page(
                &cx,
                pn(3),
                &build_interior_table(&[(pn(5), 3), (pn(6), 8)], pn(7)),
            )
            .unwrap();
        store
            .write_page(&cx, pn(4), &build_interior_table(&[(pn(8), 25)], pn(9)))
            .unwrap();
        store
            .write_page(
                &cx,
                pn(5),
                &build_leaf_table(&[(1, b"a"), (2, b"b"), (3, b"c")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(6),
                &build_leaf_table(&[(4, b"d"), (5, b"e"), (8, b"f")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(7),
                &build_leaf_table(&[(9, b"g"), (10, b"h"), (15, b"i")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(8),
                &build_leaf_table(&[(16, b"j"), (20, b"k"), (24, b"l")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(9),
                &build_leaf_table(&[(26, b"m"), (30, b"n"), (40, b"o")]),
            )
            .unwrap();
        PrefetchProbeStore::new(store)
    }

    fn assert_prefetch_snapshot_addresses_are_valid(
        store: &PrefetchProbeStore,
        snapshot: &PrefetchProbeSnapshot,
        root: PageNumber,
    ) {
        let mut reachable = BTreeSet::new();
        collect_reachable_pages(&store.inner, root, USABLE, &mut reachable);

        assert!(
            snapshot.invalid_slot_pages.is_empty(),
            "prefetch should never touch an out-of-range slot address: {:?}",
            snapshot.invalid_slot_pages
        );
        assert!(
            snapshot.missing_page_pages.is_empty(),
            "prefetch should never dereference a missing page: {:?}",
            snapshot.missing_page_pages
        );
        assert!(
            snapshot
                .hinted_pages
                .iter()
                .all(|page_no| reachable.contains(&page_no.get())),
            "descent should only hint reachable child pages, got {:?}",
            snapshot.hinted_pages
        );
        assert_eq!(
            snapshot.page_prefetch_count, snapshot.prefetch_issued_count,
            "every descent hint in this seeded tree should resolve to a real page"
        );
    }

    #[test]
    fn test_cursor_first_last_single_leaf() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"alice"), (5, b"bob"), (10, b"charlie")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert_eq!(cursor.payload(&cx).unwrap(), b"alice");

        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert_eq!(cursor.payload(&cx).unwrap(), b"charlie");
    }

    #[test]
    fn test_cursor_first_observes_cancelled_context_before_descent() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_table(&[(1, b"alice"), (5, b"bob")]));

        let cx = Cx::new();
        cx.cancel();

        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let err = cursor
            .first(&cx)
            .expect_err("cancelled context should abort before leaf descent");

        assert!(matches!(err, FrankenError::Abort));
    }

    #[test]
    fn test_table_seek_observes_cancellation_during_leaf_binary_search() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"alpha"), (5, b"bravo"), (9, b"charlie")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(CancelAfterReadStore::new(store), pn(2), USABLE, true);
        let err = cursor
            .table_move_to(&cx, 5)
            .expect_err("binary search should observe cancellation after page load");

        assert!(matches!(err, FrankenError::Abort));
    }

    #[test]
    fn test_index_seek_observes_cancellation_during_leaf_binary_search() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_index(&[b"alpha", b"bravo", b"charlie"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(CancelAfterReadStore::new(store), pn(2), USABLE, false);
        let err = cursor
            .index_move_to(&cx, b"bravo")
            .expect_err("index binary search should observe cancellation after page load");

        assert!(matches!(err, FrankenError::Abort));
    }

    #[test]
    fn test_cursor_seek_exact() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten"), (15, b"fifteen")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert_eq!(cursor.payload(&cx).unwrap(), b"five");
    }

    #[test]
    fn test_cursor_seek_not_found() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Seek for 3 — should land on 5 (next in sort order).
        let result = cursor.table_move_to(&cx, 3).unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);

        // Seek for 20 — past the end.
        let result = cursor.table_move_to(&cx, 20).unwrap();
        assert!(!result.is_found());
        assert!(cursor.eof());
    }

    #[test]
    fn test_table_seek_cache_uses_four_slot_lru() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_table(&[(pn(3), 20), (pn(4), 40), (pn(5), 60), (pn(6), 80)], pn(7)),
        );
        store
            .pages
            .insert(3, build_leaf_table(&[(10, b"a"), (20, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(30, b"c"), (40, b"d")]));
        store
            .pages
            .insert(5, build_leaf_table(&[(50, b"e"), (60, b"f")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(70, b"g"), (80, b"h")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(90, b"i"), (100, b"j")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), pn(2), USABLE, true);

        for rowid in [10_i64, 30, 50, 70] {
            assert!(cursor.table_move_to(&cx, rowid).unwrap().is_found());
        }

        let cache_pages: Vec<PageNumber> = cursor
            .seek_cache
            .iter()
            .flatten()
            .map(|entry| entry.page_no)
            .collect();
        assert_eq!(cache_pages, vec![pn(6), pn(5), pn(4), pn(3)]);

        cursor.pager.clear_reads();
        let result = cursor.table_move_to(&cx, 15).unwrap();
        assert!(!result.is_found());
        assert_eq!(cursor.pager.read_pages(), vec![pn(3)]);
        let cache_pages: Vec<PageNumber> = cursor
            .seek_cache
            .iter()
            .flatten()
            .map(|entry| entry.page_no)
            .collect();
        assert_eq!(cache_pages, vec![pn(3), pn(6), pn(5), pn(4)]);

        cursor.pager.clear_reads();
        assert!(cursor.table_move_to(&cx, 90).unwrap().is_found());
        assert_eq!(
            cursor.pager.read_pages(),
            vec![pn(6), pn(5), pn(4), pn(2), pn(7)]
        );
        let cache_pages: Vec<PageNumber> = cursor
            .seek_cache
            .iter()
            .flatten()
            .map(|entry| entry.page_no)
            .collect();
        assert_eq!(cache_pages, vec![pn(7), pn(3), pn(6), pn(5)]);

        cursor.pager.clear_reads();
        let result = cursor.table_move_to(&cx, 35).unwrap();
        assert!(!result.is_found());
        assert_eq!(cursor.pager.read_pages(), vec![pn(3), pn(2), pn(4)]);
        let cache_pages: Vec<PageNumber> = cursor
            .seek_cache
            .iter()
            .flatten()
            .map(|entry| entry.page_no)
            .collect();
        assert_eq!(cache_pages, vec![pn(4), pn(7), pn(3), pn(6)]);

        cursor.pager.clear_reads();
        let result = cursor.table_move_to(&cx, 12).unwrap();
        assert!(!result.is_found());
        assert_eq!(cursor.pager.read_pages(), vec![pn(3)]);
    }

    #[test]
    fn test_table_seek_cache_hot_set_avoids_root_descent() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_table(&[(pn(3), 20), (pn(4), 40), (pn(5), 60), (pn(6), 80)], pn(7)),
        );
        store
            .pages
            .insert(3, build_leaf_table(&[(10, b"a"), (20, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(30, b"c"), (40, b"d")]));
        store
            .pages
            .insert(5, build_leaf_table(&[(50, b"e"), (60, b"f")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(70, b"g"), (80, b"h")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(90, b"i"), (100, b"j")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), pn(2), USABLE, true);

        for rowid in [10_i64, 30, 50, 70] {
            assert!(cursor.table_move_to(&cx, rowid).unwrap().is_found());
        }

        for (target, expected_leaf) in [(15_i64, pn(3)), (35, pn(4)), (55, pn(5)), (75, pn(6))] {
            cursor.pager.clear_reads();
            let result = cursor.table_move_to(&cx, target).unwrap();
            assert!(!result.is_found());
            let read_pages = cursor.pager.read_pages();
            assert_eq!(
                read_pages.last().copied(),
                Some(expected_leaf),
                "cached seek should still land on the expected leaf for target {target}"
            );
            assert!(
                read_pages.iter().all(|page_no| *page_no != pn(2)),
                "four-page hot set should satisfy target {target} without revisiting the root: {read_pages:?}"
            );
        }
    }

    #[test]
    fn test_table_leaf_interpolation_search_matches_binary_on_sparse_rowids() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[
                (-4_000, b"a"),
                (-17, b"b"),
                (0, b"c"),
                (275, b"d"),
                (50_000, b"e"),
            ]),
        );

        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let entry = cursor.load_page(&cx, pn(2)).unwrap();

        for target in [
            -9_999, -4_000, -18, -17, -16, 1, 274, 275, 276, 50_000, 99_999,
        ] {
            let interpolation =
                BtCursor::<MemPageStore>::search_integer_key_table_leaf(&cx, &entry, target)
                    .unwrap();
            let binary =
                BtCursor::<MemPageStore>::binary_search_table_leaf(&cx, &entry, target).unwrap();
            assert_eq!(
                interpolation, binary,
                "interpolation search must match binary search for target {target}"
            );
        }
    }

    #[test]
    fn test_table_leaf_interpolation_search_matches_binary_on_sequential_rowids() {
        let cx = Cx::new();
        let rowids: Vec<i64> = (1_i64..=128).collect();
        let payloads: Vec<Vec<u8>> = rowids
            .iter()
            .map(|rowid| rowid.to_le_bytes().to_vec())
            .collect();
        let entries: Vec<(i64, &[u8])> = rowids
            .iter()
            .zip(payloads.iter())
            .map(|(rowid, payload)| (*rowid, payload.as_slice()))
            .collect();
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&entries));

        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let entry = cursor.load_page(&cx, pn(2)).unwrap();

        for target in [0_i64, 1, 2, 31, 64, 65, 127, 128, 129] {
            let interpolation =
                BtCursor::<MemPageStore>::search_integer_key_table_leaf(&cx, &entry, target)
                    .unwrap();
            let binary =
                BtCursor::<MemPageStore>::binary_search_table_leaf(&cx, &entry, target).unwrap();
            assert_eq!(
                interpolation, binary,
                "interpolation search must match binary search for sequential target {target}"
            );
        }
    }

    #[test]
    fn test_index_seek_text_keys_falls_back_to_binary_search_successor_positioning() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        for (text, rowid) in [("alpha", 1_i64), ("charlie", 3_i64), ("echo", 5_i64)] {
            let key =
                serialize_record(&[SqliteValue::Text(text.into()), SqliteValue::Integer(rowid)]);
            cursor.index_insert(&cx, &key).unwrap();
        }

        let between_probe = serialize_record(&[
            SqliteValue::Text("bravo".into()),
            SqliteValue::Integer(i64::MIN),
        ]);
        let between_seek = cursor.index_move_to(&cx, &between_probe).unwrap();
        assert!(
            !between_seek.is_found(),
            "missing text probe should fall back to successor positioning on index pages"
        );
        assert!(
            !cursor.eof(),
            "a text probe between existing keys should land on the successor entry"
        );
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        let successor_fields = parse_record(&cursor.payload(&cx).unwrap()).unwrap();
        assert_eq!(
            successor_fields,
            vec![SqliteValue::Text("charlie".into()), SqliteValue::Integer(3),],
            "text-key seek should preserve binary-search successor semantics on index pages"
        );

        let tail_probe = serialize_record(&[
            SqliteValue::Text("zulu".into()),
            SqliteValue::Integer(i64::MIN),
        ]);
        let tail_seek = cursor.index_move_to(&cx, &tail_probe).unwrap();
        assert!(
            !tail_seek.is_found(),
            "tail text probe should not report a false-positive exact hit"
        );
        assert!(
            cursor.eof(),
            "text probe beyond the last key should still fall off the right edge"
        );
    }

    #[test]
    fn test_cursor_seek_observes_cancellation_during_leaf_binary_search() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten"), (15, b"fifteen")]),
        );

        let cancelled_cx = Cx::new();
        let mut cursor = BtCursor::new(CancelAfterReadStore::new(store), pn(2), USABLE, true);
        let err = cursor
            .table_move_to(&cancelled_cx, 10)
            .expect_err("cancellation should interrupt the in-node search loop");
        assert!(matches!(err, FrankenError::Abort));

        let recovery_cx = Cx::new();
        assert!(cursor.table_move_to(&recovery_cx, 10).unwrap().is_found());
        assert_eq!(cursor.rowid(&recovery_cx).unwrap(), 10);
    }

    #[test]
    fn test_cursor_first_observes_cancellation_during_descent_and_recovers() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"ten"), (15, b"fifteen")]));

        let cancelled_cx = Cx::new();
        let mut cursor = BtCursor::new(CancelAfterReadStore::new(store), pn(2), USABLE, true);
        let err = cursor
            .first(&cancelled_cx)
            .expect_err("cancellation should interrupt multi-page descent");
        assert!(matches!(err, FrankenError::Abort));

        let recovery_cx = Cx::new();
        assert!(cursor.first(&recovery_cx).unwrap());
        assert_eq!(cursor.rowid(&recovery_cx).unwrap(), 1);
    }

    #[test]
    fn test_cursor_table_insert_single_leaf() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (3, b"three"), (5, b"five"), (7, b"seven")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        cursor.table_insert(&cx, 2, b"two").unwrap();

        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"two");

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 7);
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_cursor_table_insert_duplicate_rowid() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[(7, b"seven")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let err = cursor.table_insert(&cx, 7, b"dupe").unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));
    }

    #[test]
    fn test_balance_choke_points_reject_nonroot_single_entry_paths() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (10, b"ten")]));
        store.pages.insert(4, build_leaf_table(&[(20, b"twenty")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let root_before = cursor.pager.pages[&2].clone();
        let leaf_before = cursor.pager.pages[&3].clone();

        // Model a read fast path that retained only its landing leaf. Neither
        // balance entry point may reinterpret this non-root page as the root.
        let mut rootless_leaf = cursor.load_page(&cx, pn(3)).unwrap();
        rootless_leaf.cell_idx = 0;
        cursor.stack.push(rootless_leaf);

        let insert_error = cursor
            .balance_for_insert(&cx, b"unused", 0)
            .expect_err("insert balance must fail closed on a rootless path");
        assert!(
            matches!(
                &insert_error,
                FrankenError::DatabaseCorrupt { detail }
                    if detail.contains("insert balance cursor path starts at page 3 instead of root 2")
            ),
            "unexpected insert-balance error: {insert_error:?}"
        );

        let delete_error = cursor
            .balance_for_delete(&cx)
            .expect_err("delete balance must fail closed on a rootless path");
        assert!(
            matches!(
                &delete_error,
                FrankenError::DatabaseCorrupt { detail }
                    if detail.contains("delete balance cursor path starts at page 3 instead of root 2")
            ),
            "unexpected delete-balance error: {delete_error:?}"
        );

        assert_eq!(&cursor.pager.pages[&2], &root_before);
        assert_eq!(&cursor.pager.pages[&3], &leaf_before);
    }

    #[test]
    fn test_ensure_full_mutation_path_rebuilds_from_declared_root() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (10, b"ten")]));
        store.pages.insert(4, build_leaf_table(&[(20, b"twenty")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let mut rootless_leaf = cursor.load_page(&cx, pn(3)).unwrap();
        rootless_leaf.cell_idx = 0;
        cursor.stack.push(rootless_leaf);
        cursor.at_eof = false;

        let result = cursor
            .ensure_full_mutation_path(&cx, MutationPathTarget::ExactTableRowid(1))
            .expect("rootless read position should be recoverable")
            .expect("rootless position should trigger a re-seek");
        assert!(result.is_found());
        assert_eq!(cursor.stack.len(), 2);
        assert_eq!(cursor.stack[0].page_no, pn(2));
        assert_eq!(cursor.stack[1].page_no, pn(3));
    }

    #[test]
    fn test_cursor_index_insert_single_leaf() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_index(&[b"apple", b"carrot", b"pear"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, false);
        cursor.index_insert(&cx, b"banana").unwrap();

        assert!(cursor.index_move_to(&cx, b"banana").unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"banana");
    }

    #[test]
    fn test_cursor_index_insert_before_cached_leaf_slot_preserves_order() {
        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, 512);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root, 512, false);

        let high_key = synthetic_index_key(791);
        let low_key = synthetic_index_key(357);

        cursor.index_insert(&cx, &high_key).unwrap();
        cursor.index_insert(&cx, &low_key).unwrap();

        validate_index_tree_invariants(&mut cursor, root).unwrap();
        assert_eq!(
            scan_all_index_keys(&mut cursor, &cx).unwrap(),
            vec![low_key, high_key]
        );
    }

    #[test]
    fn test_index_insert_prechecked_absent_reuses_successor_position() {
        let store = MemPageStore::with_empty_index(pn(2), USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        let low_key = serialize_record(&[SqliteValue::Integer(10), SqliteValue::Integer(1)]);
        let mid_key = serialize_record(&[SqliteValue::Integer(20), SqliteValue::Integer(2)]);
        let high_key = serialize_record(&[SqliteValue::Integer(30), SqliteValue::Integer(3)]);

        cursor.index_insert(&cx, &low_key).unwrap();
        cursor.index_insert(&cx, &high_key).unwrap();

        let seek = cursor.index_move_to(&cx, &mid_key).unwrap();
        assert_eq!(seek, SeekResult::NotFound);
        assert!(!cursor.eof(), "seek should land on successor key");

        cursor
            .index_insert_prechecked_absent(&cx, &mid_key)
            .unwrap();

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(10), SqliteValue::Integer(1)]
        );
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(20), SqliteValue::Integer(2)]
        );
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(30), SqliteValue::Integer(3)]
        );
    }

    #[test]
    fn test_index_insert_unique_no_conflict_inserts_between_adjacent_prefixes() {
        let store = MemPageStore::with_empty_index(pn(2), USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        let low_key = serialize_record(&[SqliteValue::Integer(10), SqliteValue::Integer(1)]);
        let mid_key = serialize_record(&[SqliteValue::Integer(20), SqliteValue::Integer(2)]);
        let high_key = serialize_record(&[SqliteValue::Integer(30), SqliteValue::Integer(3)]);

        cursor.index_insert(&cx, &low_key).unwrap();
        cursor.index_insert(&cx, &high_key).unwrap();
        cursor.index_insert_unique(&cx, &mid_key, 1, "t.x").unwrap();

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(10), SqliteValue::Integer(1)]
        );
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(20), SqliteValue::Integer(2)]
        );
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(30), SqliteValue::Integer(3)]
        );
    }

    #[test]
    fn test_index_insert_unique_non_leaf_restore_state_falls_back_to_full_insert() {
        let low_key = serialize_record(&[
            SqliteValue::Text("alpha@example.com".into()),
            SqliteValue::Integer(1),
        ]);
        let separator_key = serialize_record(&[
            SqliteValue::Text("mango@example.com".into()),
            SqliteValue::Integer(2),
        ]);
        let high_key = serialize_record(&[
            SqliteValue::Text("zebra@example.com".into()),
            SqliteValue::Integer(3),
        ]);
        let probe_key = serialize_record(&[
            SqliteValue::Text("hotel@example.com".into()),
            SqliteValue::Integer(4),
        ]);

        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_index(&[(pn(3), &separator_key)], pn(4)));
        store.pages.insert(3, build_leaf_index(&[&low_key]));
        store.pages.insert(4, build_leaf_index(&[&high_key]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        let restores_to_non_leaf = cursor
            .with_btree_op(&cx, BtreeOpType::Seek, |cursor| {
                let _seek = cursor.index_seek(&cx, &probe_key)?;
                let restore_eof = cursor.at_eof;

                if !cursor.at_eof {
                    let _payload = cursor.payload(&cx)?;
                }

                if cursor.prev(&cx)? {
                    let _payload = cursor.payload(&cx)?;
                }

                if restore_eof {
                    cursor.at_eof = true;
                } else {
                    cursor.next(&cx)?;
                }

                Ok(cursor
                    .stack
                    .last()
                    .is_some_and(|top| !top.header.page_type.is_leaf()))
            })
            .unwrap();

        assert!(
            restores_to_non_leaf,
            "test requires the uniqueness restore state to sit on an interior separator"
        );

        cursor
            .index_insert_unique(&cx, &probe_key, 1, "bench.email")
            .unwrap();

        assert!(cursor.index_move_to(&cx, &probe_key).unwrap().is_found());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![
                SqliteValue::Text("hotel@example.com".into()),
                SqliteValue::Integer(4),
            ]
        );
    }

    #[test]
    fn test_index_insert_monotonic_unique_email_keys_10k_counts_and_reaches_last_key() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, false);
        let row_count = 10_000_i64;

        for rowid in 1..=row_count {
            let key = serialize_record(&[
                SqliteValue::Text(format!("user_{:05}@test.com", rowid - 1).into()),
                SqliteValue::Integer(rowid),
            ]);
            cursor.index_insert(&cx, &key).unwrap();
        }

        let depth = measure_tree_depth(&cursor.pager, root, USABLE);
        assert!(
            depth >= 2,
            "test requires at least one interior index level, got depth {depth}"
        );

        let last_key = serialize_record(&[
            SqliteValue::Text(format!("user_{:05}@test.com", row_count - 1).into()),
            SqliteValue::Integer(row_count),
        ]);
        let last_found = cursor.index_move_to(&cx, &last_key).unwrap().is_found();
        let last_payload = last_found.then(|| parse_record(&cursor.payload(&cx).unwrap()).unwrap());
        let count = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            count, row_count,
            "plain monotonic index_insert should preserve all {row_count} index entries; last_found={last_found} last_payload={last_payload:?}"
        );

        assert!(
            last_found,
            "plain monotonic index_insert should keep the last key reachable"
        );
        assert_eq!(
            last_payload.unwrap(),
            vec![
                SqliteValue::Text(format!("user_{:05}@test.com", row_count - 1).into()),
                SqliteValue::Integer(row_count),
            ]
        );
    }

    #[test]
    fn test_index_insert_unique_monotonic_unique_email_keys_10k_counts_and_reaches_last_key() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, false);
        let row_count = 10_000_i64;

        for rowid in 1..=row_count {
            let key = serialize_record(&[
                SqliteValue::Text(format!("user_{:05}@test.com", rowid - 1).into()),
                SqliteValue::Integer(rowid),
            ]);
            cursor
                .index_insert_unique(&cx, &key, 1, "bench.email")
                .unwrap();
        }

        let depth = measure_tree_depth(&cursor.pager, root, USABLE);
        assert!(
            depth >= 2,
            "test requires at least one interior index level, got depth {depth}"
        );

        let last_key = serialize_record(&[
            SqliteValue::Text(format!("user_{:05}@test.com", row_count - 1).into()),
            SqliteValue::Integer(row_count),
        ]);
        let last_found = cursor.index_move_to(&cx, &last_key).unwrap().is_found();
        let last_payload = last_found.then(|| parse_record(&cursor.payload(&cx).unwrap()).unwrap());
        let count = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            count, row_count,
            "monotonic unique index_insert_unique should preserve all {row_count} index entries; last_found={last_found} last_payload={last_payload:?}"
        );

        assert!(
            last_found,
            "monotonic unique index_insert_unique should keep the last key reachable"
        );
        assert_eq!(
            last_payload.unwrap(),
            vec![
                SqliteValue::Text(format!("user_{:05}@test.com", row_count - 1).into()),
                SqliteValue::Integer(row_count),
            ]
        );
    }

    #[test]
    fn test_index_insert_unique_deep_tree_monotonic_10k_counts_all_rows() {
        let store = MemPageStore::with_empty_index(pn(2), USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        let row_count = 10_000_i64;

        for rowid in 0..row_count {
            let key = serialize_record(&[
                SqliteValue::Text(format!("user_{rowid}@test.com").into()),
                SqliteValue::Integer(rowid),
            ]);
            cursor
                .index_insert_unique(&cx, &key, 1, "bench.email")
                .unwrap();
        }

        let counted = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            counted, row_count,
            "deep monotonic unique index should contain every inserted entry"
        );

        let last_key = serialize_record(&[
            SqliteValue::Text(format!("user_{}@test.com", row_count - 1).into()),
            SqliteValue::Integer(row_count - 1),
        ]);
        let seek = cursor.index_move_to(&cx, &last_key).unwrap();
        assert_eq!(seek, SeekResult::Found);
        assert_eq!(cursor.payload(&cx).unwrap(), last_key);
    }

    #[test]
    fn test_index_unique_rightmost_append_helper_preserves_count_and_conflicts() {
        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root, USABLE, false);
        let key = |idx: i64, rowid: i64| {
            serialize_record(&[
                SqliteValue::Integer(7),
                SqliteValue::Integer(idx),
                SqliteValue::Integer(rowid),
            ])
        };

        let first = key(0, 100);
        assert!(
            cursor
                .index_insert_unique_with_rightmost_report(&cx, &first, 2, "messages.conv_idx")
                .unwrap(),
            "empty-tree insert should report a rightmost insertion"
        );

        let second = key(1, 101);
        assert!(
            cursor
                .index_append_after_current_rightmost_position(&cx, &second)
                .unwrap(),
            "cursor should still be on the right edge after the reported insert"
        );

        cursor.first(&cx).unwrap();
        let third = key(2, 102);
        assert!(
            !cursor
                .index_append_after_current_rightmost_position(&cx, &third)
                .unwrap(),
            "append helper must reject a cursor no longer positioned at the right edge"
        );
        assert!(
            cursor
                .index_insert_unique_with_rightmost_report(&cx, &third, 2, "messages.conv_idx")
                .unwrap(),
            "canonical fallback should reseed the rightmost hint"
        );

        let duplicate = key(1, 201);
        let err = cursor
            .index_insert_unique_with_rightmost_report(&cx, &duplicate, 2, "messages.conv_idx")
            .unwrap_err();
        assert!(matches!(err, FrankenError::UniqueViolation { .. }));

        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 3);
        assert_eq!(
            scan_all_index_keys(&mut cursor, &cx).unwrap(),
            vec![first, second, third]
        );
    }

    #[test]
    fn test_index_insert_deep_tree_monotonic_10k_counts_all_rows() {
        let store = MemPageStore::with_empty_index(pn(2), USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        let row_count = 10_000_i64;

        for rowid in 0..row_count {
            let key = serialize_record(&[
                SqliteValue::Text(format!("user_{rowid}@test.com").into()),
                SqliteValue::Integer(rowid),
            ]);
            cursor.index_insert(&cx, &key).unwrap();
        }

        let counted = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            counted, row_count,
            "deep monotonic plain index should contain every inserted entry"
        );

        let last_key = serialize_record(&[
            SqliteValue::Text(format!("user_{}@test.com", row_count - 1).into()),
            SqliteValue::Integer(row_count - 1),
        ]);
        let seek = cursor.index_move_to(&cx, &last_key).unwrap();
        assert_eq!(seek, SeekResult::Found);
        assert_eq!(cursor.payload(&cx).unwrap(), last_key);
    }

    #[test]
    fn test_cursor_index_next_visits_interior_separator_cells() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_index(&[(pn(3), b"b"), (pn(4), b"d")], pn(5)),
        );
        store.pages.insert(3, build_leaf_index(&[b"a"]));
        store.pages.insert(4, build_leaf_index(&[b"c"]));
        store.pages.insert(5, build_leaf_index(&[b"e", b"f"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, false);
        assert!(cursor.first(&cx).unwrap());
        let mut scanned = vec![cursor.payload(&cx).unwrap()];
        while cursor.next(&cx).unwrap() {
            scanned.push(cursor.payload(&cx).unwrap());
        }
        assert_eq!(
            scanned,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec(),
            ]
        );
    }

    #[test]
    fn test_cursor_index_prev_visits_interior_separator_cells() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_index(&[(pn(3), b"b"), (pn(4), b"d")], pn(5)),
        );
        store.pages.insert(3, build_leaf_index(&[b"a"]));
        store.pages.insert(4, build_leaf_index(&[b"c"]));
        store.pages.insert(5, build_leaf_index(&[b"e", b"f"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        assert!(cursor.last(&cx).unwrap());
        let mut scanned = vec![cursor.payload(&cx).unwrap()];
        while cursor.prev(&cx).unwrap() {
            scanned.push(cursor.payload(&cx).unwrap());
        }
        assert_eq!(
            scanned,
            vec![
                b"f".to_vec(),
                b"e".to_vec(),
                b"d".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ]
        );
    }

    #[test]
    fn test_count_all_rows_on_interior_index_includes_separator_cells() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_index(&[(pn(3), b"b"), (pn(4), b"d")], pn(5)),
        );
        store.pages.insert(3, build_leaf_index(&[b"a"]));
        store.pages.insert(4, build_leaf_index(&[b"c"]));
        store.pages.insert(5, build_leaf_index(&[b"e", b"f"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 6);
    }

    #[test]
    fn test_cursor_index_rowid_extracted_from_trailing_record_field() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let key = serialize_record(&[SqliteValue::Text("beacon".into()), SqliteValue::Integer(73)]);

        cursor.index_insert(&cx, &key).unwrap();
        assert!(cursor.index_move_to(&cx, &key).unwrap().is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 73);
    }

    #[test]
    fn test_cursor_index_rowid_with_overflow_key_payload() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let key = serialize_record(&[
            SqliteValue::Blob(vec![0xAB; 2_500].into()),
            SqliteValue::Integer(901),
        ]);

        cursor.index_insert(&cx, &key).unwrap();
        assert!(cursor.index_move_to(&cx, &key).unwrap().is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 901);
    }

    #[test]
    fn test_cursor_index_seek_duplicate_run_walks_all_matching_entries() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        for rowid in [1_i64, 2, 5] {
            let key = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(rowid)]);
            cursor.index_insert(&cx, &key).unwrap();
        }
        let other_key = serialize_record(&[SqliteValue::Integer(99), SqliteValue::Integer(9)]);

        cursor.index_insert(&cx, &other_key).unwrap();

        let probe = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(i64::MIN)]);
        let seek = cursor.index_move_to(&cx, &probe).unwrap();
        assert!(
            !seek.is_found(),
            "probe uses a synthetic minimum rowid and should anchor via successor positioning"
        );
        assert!(
            !cursor.eof(),
            "duplicate-run probe should land on the first matching entry"
        );

        let mut seen_rowids = Vec::new();
        loop {
            let payload = cursor.payload(&cx).unwrap();
            let fields = parse_record(&payload).unwrap();
            if fields.first() != Some(&SqliteValue::Integer(42)) {
                break;
            }
            seen_rowids.push(cursor.rowid(&cx).unwrap());
            if !cursor.next(&cx).unwrap() {
                break;
            }
        }

        assert_eq!(seen_rowids, vec![1, 2, 5]);
    }

    #[test]
    fn test_cursor_index_prefix_upper_bound_skips_duplicate_run_during_descent() {
        let key_42_1 = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(1)]);
        let key_42_2 = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(2)]);
        let separator = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(3)]);
        let successor = serialize_record(&[SqliteValue::Integer(99), SqliteValue::Integer(9)]);

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_index(&[(pn(3), separator.as_slice())], pn(4)),
        );
        store.pages.insert(
            3,
            build_leaf_index(&[key_42_1.as_slice(), key_42_2.as_slice()]),
        );
        store
            .pages
            .insert(4, build_leaf_index(&[successor.as_slice()]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), pn(2), USABLE, false);
        let prefix = serialize_record(&[SqliteValue::Integer(42)]);

        cursor.index_move_to_upper_bound(&cx, &prefix).unwrap();
        assert_eq!(
            cursor.pager.read_pages(),
            vec![pn(2), pn(4)],
            "upper-bound descent must bypass the equal-prefix subtree"
        );
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(99), SqliteValue::Integer(9)]
        );

        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Integer(42), SqliteValue::Integer(3)]
        );
    }

    #[test]
    fn test_cursor_index_prefix_upper_bound_honors_nocase_collation() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        cursor.set_index_collation_context(
            vec![Some("NOCASE".to_owned())],
            Arc::new(Mutex::new(CollationRegistry::new())),
        );
        for (text, rowid) in [("alpha", 1_i64), ("ALPHA", 2), ("beta", 3)] {
            let key =
                serialize_record(&[SqliteValue::Text(text.into()), SqliteValue::Integer(rowid)]);
            cursor.index_insert(&cx, &key).unwrap();
        }

        let prefix = serialize_record(&[SqliteValue::Text("aLpHa".into())]);
        cursor.index_move_to_upper_bound(&cx, &prefix).unwrap();
        assert_eq!(
            parse_record(&cursor.payload(&cx).unwrap()).unwrap(),
            vec![SqliteValue::Text("beta".into()), SqliteValue::Integer(3)]
        );
    }

    #[test]
    fn test_cursor_index_seek_honors_nocase_collation_on_full_record_key() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new_with_index_desc(store, pn(2), USABLE, false, Vec::new());
        cursor.set_index_collation_context(
            vec![Some("NOCASE".to_owned())],
            Arc::new(Mutex::new(CollationRegistry::new())),
        );

        let alpha = serialize_record(&[SqliteValue::Text("alpha".into()), SqliteValue::Integer(1)]);
        let beta = serialize_record(&[SqliteValue::Text("beta".into()), SqliteValue::Integer(2)]);
        cursor.index_insert(&cx, &alpha).unwrap();
        cursor.index_insert(&cx, &beta).unwrap();

        let probe = serialize_record(&[SqliteValue::Text("ALPHA".into()), SqliteValue::Integer(1)]);
        assert!(
            cursor.index_move_to(&cx, &probe).unwrap().is_found(),
            "full-record index seek should honor NOCASE collation on the indexed text term"
        );
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
    }

    #[test]
    fn test_cursor_index_iteration_honors_nocase_collation_order() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new_with_index_desc(store, pn(2), USABLE, false, Vec::new());
        cursor.set_index_collation_context(
            vec![Some("NOCASE".to_owned())],
            Arc::new(Mutex::new(CollationRegistry::new())),
        );

        for key in [
            serialize_record(&[SqliteValue::Text("alpha".into()), SqliteValue::Integer(1)]),
            serialize_record(&[SqliteValue::Text("ALPHA".into()), SqliteValue::Integer(2)]),
            serialize_record(&[SqliteValue::Text("beta".into()), SqliteValue::Integer(3)]),
        ] {
            cursor.index_insert(&cx, &key).unwrap();
        }

        let mut seen = Vec::new();
        if cursor.first(&cx).unwrap() {
            loop {
                let fields = parse_record(&cursor.payload(&cx).unwrap()).unwrap();
                seen.push((fields[0].clone(), cursor.rowid(&cx).unwrap()));
                if !cursor.next(&cx).unwrap() {
                    break;
                }
            }
        }

        assert_eq!(
            seen,
            vec![
                (SqliteValue::Text("alpha".into()), 1),
                (SqliteValue::Text("ALPHA".into()), 2),
                (SqliteValue::Text("beta".into()), 3),
            ],
            "index scan order should follow NOCASE collation with rowid as the tie-breaker"
        );
    }

    #[test]
    fn test_cursor_index_rowid_rejects_record_without_trailing_integer() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        let key = serialize_record(&[SqliteValue::Text("missing-rowid".into())]);

        cursor.index_insert(&cx, &key).unwrap();
        assert!(cursor.index_move_to(&cx, &key).unwrap().is_found());

        let err = cursor.rowid(&cx).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
    }

    #[test]
    fn test_cursor_table_insert_with_overflow_payload() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));
        let payload: Vec<u8> = (0u8..=255).cycle().take(5000).collect();

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        cursor.table_insert(&cx, 42, &payload).unwrap();

        assert!(cursor.table_move_to(&cx, 42).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), payload);
    }

    #[test]
    fn test_cursor_table_seek_past_end_then_insert() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three"), (4, b"four")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let seek = cursor.table_move_to(&cx, 99).unwrap();
        assert!(!seek.is_found());
        assert!(cursor.eof(), "seek past end should set eof");

        cursor.table_insert(&cx, 99, b"tail").unwrap();
        assert!(cursor.table_move_to(&cx, 99).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"tail");
    }

    #[test]
    fn test_cursor_index_seek_past_end_then_insert() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[b"alpha", b"mid"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        let key = b"zz-top";

        let seek = cursor.index_move_to(&cx, key).unwrap();
        assert!(!seek.is_found());
        assert!(cursor.eof(), "seek past end should set eof");

        cursor.index_insert(&cx, key).unwrap();
        assert!(cursor.index_move_to(&cx, key).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), key);
    }

    #[test]
    fn test_cursor_prev_from_seek_past_end_lands_on_last_entry() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three"), (4, b"four")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let seek = cursor.table_move_to(&cx, 99).unwrap();
        assert!(!seek.is_found());
        assert!(cursor.eof());

        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 4);
    }

    #[test]
    fn test_cursor_next_after_prev_from_first_recovers() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three"), (4, b"four")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(!cursor.prev(&cx).unwrap());

        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
    }

    #[test]
    fn test_cursor_prev_after_next_from_last_recovers() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three"), (4, b"four")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 4);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(!cursor.prev(&cx).unwrap());
    }

    #[test]
    fn test_cursor_table_insert_overflow_write_failure_frees_allocations() {
        let cx = Cx::new();
        let root_page = pn(2);
        let mut base = MemPageStore::new(USABLE);
        base.init_leaf_table_root(root_page);
        let shared = Rc::new(RefCell::new(base));

        // Force a mid-chain overflow write failure.
        let failing = FailingOverflowStore::new(Rc::clone(&shared), 2);
        let mut cursor = BtCursor::new(failing, root_page, USABLE, true);
        let payload = vec![0xCC; 9_000];

        let err = cursor.table_insert(&cx, 1, &payload).unwrap_err();
        assert!(matches!(err, FrankenError::Internal(_)));
        assert_eq!(
            shared.borrow().pages.len(),
            1,
            "only the root page should remain after failed overflow write"
        );
    }

    #[test]
    fn test_cursor_delete_masks_overflow_cleanup_cancellation() {
        let root_page = pn(2);
        let mut base = MemPageStore::new(USABLE);
        base.init_leaf_table_root(root_page);
        let shared = Rc::new(RefCell::new(base));

        let store = CancelAfterFirstOverflowFreeStore::new(Rc::clone(&shared));
        let mut cursor = BtCursor::new(store, root_page, USABLE, true);

        let insert_cx = Cx::new();
        let payload = vec![0xAB; 9_000];
        cursor.table_insert(&insert_cx, 7, &payload).unwrap();

        let pages_before: BTreeSet<u32> = shared.borrow().pages.keys().copied().collect();
        assert!(
            pages_before.len() > 2,
            "test requires a multi-page overflow chain, found pages {pages_before:?}"
        );

        let delete_cx = Cx::new();
        assert!(cursor.table_move_to(&delete_cx, 7).unwrap().is_found());
        cursor
            .delete(&delete_cx)
            .expect("overflow cleanup must finish even if cancellation arrives mid-chain");
        assert!(
            delete_cx.checkpoint().is_err(),
            "test store should request cancellation during overflow cleanup"
        );

        let recovery_cx = Cx::new();
        assert!(!cursor.first(&recovery_cx).unwrap());

        let remaining_pages: BTreeSet<u32> = shared.borrow().pages.keys().copied().collect();
        assert_eq!(
            remaining_pages,
            BTreeSet::from([root_page.get()]),
            "masked cleanup must reclaim every overflow page before returning"
        );
    }

    #[test]
    fn test_cursor_table_insert_triggers_root_split() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut rowid = 1i64;
        let split_rowid = loop {
            let payload = vec![b'Z'; 220];
            cursor.table_insert(&cx, rowid, &payload).unwrap();

            let root_page = cursor.pager.inner.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break rowid;
            }

            rowid += 1;
            assert!(
                rowid < 1000,
                "table root did not split under sustained inserts"
            );
        };

        let root_page = cursor.pager.inner.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert_eq!(root_header.page_type, cell::BtreePageType::InteriorTable);

        assert!(cursor.table_move_to(&cx, split_rowid).unwrap().is_found());
    }

    #[test]
    fn test_cursor_index_insert_triggers_root_split() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_index(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        let mut idx = 0usize;
        let split_key = loop {
            let key = format!("key-{idx:05}");
            cursor.index_insert(&cx, key.as_bytes()).unwrap();

            let root_page = cursor.pager.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorIndex {
                break key.into_bytes();
            }

            idx += 1;
            assert!(
                idx < 2000,
                "index root did not split under sustained inserts"
            );
        };

        let root_page = cursor.pager.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert_eq!(root_header.page_type, cell::BtreePageType::InteriorIndex);

        assert!(cursor.index_move_to(&cx, &split_key).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), split_key);
    }

    #[test]
    fn test_cursor_index_delete_removes_interior_separator_key() {
        const INDEX_USABLE: u32 = 512;

        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, INDEX_USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root, INDEX_USABLE, false);

        let mut key_idx = 0usize;
        let separator_key = loop {
            let key = format!("key-{key_idx:04}").into_bytes();
            cursor.index_insert(&cx, &key).unwrap();
            key_idx += 1;

            let root_page = cursor.pager.pages.get(&root.get()).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorIndex {
                let root_entry = cursor.load_page(&cx, root).unwrap();
                let divider = cursor.parse_cell_at(&root_entry, 0).unwrap();
                break cursor
                    .read_cell_payload(&cx, &root_entry, &divider)
                    .unwrap()
                    .into_owned();
            }
        };

        let mut expected = Vec::new();
        if cursor.first(&cx).unwrap() {
            loop {
                expected.push(cursor.payload(&cx).unwrap());
                if !cursor.next(&cx).unwrap() {
                    break;
                }
            }
        }
        expected.retain(|key| key.as_slice() != separator_key.as_slice());

        let seek = cursor.index_move_to(&cx, &separator_key).unwrap();
        assert!(
            seek.is_found(),
            "separator key should be seekable before delete"
        );
        assert!(
            !cursor
                .stack
                .last()
                .expect("separator seek should leave a cursor frame")
                .header
                .page_type
                .is_leaf(),
            "separator key must resolve to an interior frame to exercise interior delete"
        );

        cursor.delete(&cx).unwrap();

        let seek_after = cursor.index_move_to(&cx, &separator_key).unwrap();
        assert!(
            !seek_after.is_found(),
            "deleted separator key must not remain reachable"
        );

        let mut scanned = Vec::new();
        if cursor.first(&cx).unwrap() {
            loop {
                scanned.push(cursor.payload(&cx).unwrap());
                if !cursor.next(&cx).unwrap() {
                    break;
                }
            }
        }
        assert_eq!(
            scanned, expected,
            "interior delete must remove the separator without leaving a stale logical entry"
        );

        let bounds = validate_index_tree_invariants(&mut cursor, root)
            .expect("index invariants should hold after deleting an interior separator");
        assert_eq!(
            bounds
                .expect("non-empty index tree should report bounds")
                .entry_count,
            expected.len(),
            "index invariant harness should count the same logical entries as the scan"
        );
    }

    #[test]
    fn test_replace_interior_overflow_is_not_freed_before_balance_succeeds() {
        const INDEX_USABLE: u32 = 512;

        let root = pn(2);
        let cx = Cx::new();
        let store = MemPageStore::with_empty_index(root, INDEX_USABLE);
        let mut builder = BtCursor::new(store, root, INDEX_USABLE, false);

        let (replacement_len, replacement_local_len) = (1_usize..=4_096)
            .map(|payload_len| {
                let payload_size = u32::try_from(payload_len).unwrap();
                let local_len = usize::try_from(cell::local_payload_size(
                    payload_size,
                    INDEX_USABLE,
                    BtreePageType::InteriorIndex,
                ))
                .unwrap();
                let mut varint = [0_u8; 9];
                let varint_len = write_varint(&mut varint, u64::from(payload_size));
                let overflow_pointer_len = if local_len < payload_len { 4 } else { 0 };
                let on_page_len = 4 + varint_len + local_len + overflow_pointer_len;
                (payload_len, local_len, on_page_len)
            })
            .max_by_key(|&(_, _, on_page_len)| on_page_len)
            .map(|(payload_len, local_len, _)| (payload_len, local_len))
            .unwrap();

        // Build a deterministic, validly encoded root with nine minimal-local
        // overflow cells. Nine 49-byte cells leave only 41 free bytes; after
        // removing one separator there are still only 90 bytes available, less
        // than the maximally sized replacement above. This guarantees the
        // balance path without relying on organically produced tree density.
        let mut encoded_cells = Vec::new();
        let mut target = None;
        for key_idx in 0_u32..9 {
            let mut key = vec![0x40_u8; 200];
            key[..4].copy_from_slice(&key_idx.to_be_bytes());
            let local_len = usize::try_from(cell::local_payload_size(
                u32::try_from(key.len()).unwrap(),
                INDEX_USABLE,
                BtreePageType::InteriorIndex,
            ))
            .unwrap();
            let overflow_head = builder
                .write_overflow_chain_for_insert(&cx, &key[local_len..])
                .unwrap();

            let mut encoded = Vec::new();
            encoded.extend_from_slice(&pn(10_000 + key_idx).get().to_be_bytes());
            let mut varint = [0_u8; 9];
            let varint_len = write_varint(&mut varint, u64::try_from(key.len()).unwrap());
            encoded.extend_from_slice(&varint[..varint_len]);
            encoded.extend_from_slice(&key[..local_len]);
            encoded.extend_from_slice(&overflow_head.get().to_be_bytes());
            if key_idx == 4 {
                target = Some((key.clone(), overflow_head));
            }
            encoded_cells.push(encoded);
        }

        let mut root_bytes = vec![0_u8; usize::try_from(INDEX_USABLE).unwrap()];
        let mut content_offset = root_bytes.len();
        let mut cell_pointers = Vec::with_capacity(encoded_cells.len());
        for encoded in &encoded_cells {
            content_offset = content_offset.checked_sub(encoded.len()).unwrap();
            root_bytes[content_offset..content_offset + encoded.len()].copy_from_slice(encoded);
            cell_pointers.push(u16::try_from(content_offset).unwrap());
        }
        let root_header = BtreePageHeader {
            page_type: BtreePageType::InteriorIndex,
            first_freeblock: 0,
            cell_count: u16::try_from(cell_pointers.len()).unwrap(),
            cell_content_offset: u32::try_from(content_offset).unwrap(),
            fragmented_free_bytes: 0,
            right_child: Some(pn(20_000)),
        };
        root_header.write(&mut root_bytes, 0);
        cell::write_cell_pointers(&mut root_bytes, 0, &root_header, &cell_pointers);
        builder.pager.pages.insert(root.get(), root_bytes);

        let (old_key, old_overflow_head) = target.unwrap();
        let base_store = builder.pager;
        assert!(base_store.pages.contains_key(&old_overflow_head.get()));
        let shared = Rc::new(RefCell::new(base_store));

        let overflow_payload_len = replacement_len - replacement_local_len;
        let new_overflow_pages =
            overflow_payload_len.div_ceil(usize::try_from(INDEX_USABLE).unwrap() - 4);
        // Replacement writes: N new-overflow pages, the old-cell-free page,
        // then the first structural balance write. Fail that balance write.
        let fail_on_write = new_overflow_pages + 2;
        let failing = FailingOverflowStore::new(Rc::clone(&shared), fail_on_write);
        let mut cursor = BtCursor::new(failing, root, INDEX_USABLE, false);
        assert!(cursor.index_move_to(&cx, &old_key).unwrap().is_found());
        assert!(
            !cursor.stack.last().unwrap().header.page_type.is_leaf(),
            "fixture key must resolve to an interior separator"
        );

        let replacement = vec![0xA5; replacement_len];
        let error = cursor
            .replace_interior_cell(&cx, &replacement)
            .expect_err("the injected first balance write must fail");
        assert!(
            matches!(error, FrankenError::Internal(ref message) if message == "injected write failure"),
            "expected the injected balance failure, got {error:?}"
        );
        assert!(
            shared.borrow().pages.contains_key(&old_overflow_head.get()),
            "the old separator must retain ownership of its overflow chain until balance succeeds"
        );
    }

    #[test]
    fn test_cursor_index_delete_updates_nonroot_interior_sequence_in_depth3_tree() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_index(&[(pn(3), b"m")], pn(4)));
        store.pages.insert(
            3,
            build_interior_index(&[(pn(5), b"d"), (pn(6), b"h")], pn(7)),
        );
        store
            .pages
            .insert(4, build_interior_index(&[(pn(8), b"s")], pn(9)));
        store.pages.insert(5, build_leaf_index(&[b"a", b"b"]));
        store.pages.insert(6, build_leaf_index(&[b"e", b"f"]));
        store.pages.insert(7, build_leaf_index(&[b"i", b"j"]));
        store.pages.insert(8, build_leaf_index(&[b"n", b"q"]));
        store.pages.insert(9, build_leaf_index(&[b"t", b"z"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        let scanned_before = scan_all_index_keys(&mut cursor, &cx).unwrap();
        assert_eq!(
            scanned_before,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec(),
                b"h".to_vec(),
                b"i".to_vec(),
                b"j".to_vec(),
                b"m".to_vec(),
                b"n".to_vec(),
                b"q".to_vec(),
                b"s".to_vec(),
                b"t".to_vec(),
                b"z".to_vec(),
            ]
        );
        validate_index_tree_invariants(&mut cursor, pn(2))
            .expect("hand-built depth-3 index tree should satisfy structural invariants");

        let seek = cursor.index_move_to(&cx, b"h").unwrap();
        assert!(
            seek.is_found(),
            "target separator should exist before delete"
        );
        assert!(
            !cursor
                .stack
                .last()
                .expect("seek should leave a cursor frame")
                .header
                .page_type
                .is_leaf(),
            "target key must resolve to the non-root interior separator"
        );

        cursor.delete(&cx).unwrap();

        let scanned_after = scan_all_index_keys(&mut cursor, &cx).unwrap();
        assert_eq!(
            scanned_after,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec(),
                b"i".to_vec(),
                b"j".to_vec(),
                b"m".to_vec(),
                b"n".to_vec(),
                b"q".to_vec(),
                b"s".to_vec(),
                b"t".to_vec(),
                b"z".to_vec(),
            ],
            "non-root interior delete should preserve a strictly ordered logical sequence"
        );
        assert!(!cursor.index_move_to(&cx, b"h").unwrap().is_found());

        let bounds = validate_index_tree_invariants(&mut cursor, pn(2))
            .expect("index invariants should hold after non-root interior delete");
        assert_eq!(
            bounds
                .expect("non-empty index tree should report bounds")
                .entry_count,
            scanned_after.len(),
        );
    }

    #[test]
    fn test_cursor_index_delete_then_reinsert_same_key_preserves_exact_count() {
        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root, USABLE, false);

        let provenance_key = serialize_record(&[
            SqliteValue::Text("local".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("dup-session".into()),
            SqliteValue::Integer(1),
        ]);
        let source_id_key =
            serialize_record(&[SqliteValue::Text("local".into()), SqliteValue::Integer(1)]);

        for key in [&provenance_key, &source_id_key] {
            cursor.index_insert(&cx, key).unwrap();
            assert_eq!(
                cursor.count_all_rows(&cx).unwrap(),
                1,
                "freshly inserted key should count exactly once"
            );

            assert!(cursor.index_move_to(&cx, key).unwrap().is_found());
            cursor.delete(&cx).unwrap();
            assert_eq!(
                cursor.count_all_rows(&cx).unwrap(),
                0,
                "deleted key should be removed completely"
            );

            cursor.index_insert(&cx, key).unwrap();
            assert_eq!(
                cursor.count_all_rows(&cx).unwrap(),
                1,
                "reinserting the same key must not leave a duplicate logical entry"
            );

            let mut scanned = Vec::new();
            if cursor.first(&cx).unwrap() {
                loop {
                    scanned.push(cursor.payload(&cx).unwrap());
                    if !cursor.next(&cx).unwrap() {
                        break;
                    }
                }
            }
            assert_eq!(scanned, vec![key.clone()]);

            assert!(cursor.index_move_to(&cx, key).unwrap().is_found());
            cursor.delete(&cx).unwrap();
            assert_eq!(cursor.count_all_rows(&cx).unwrap(), 0);
        }
    }

    #[test]
    fn test_cursor_repeated_root_overflow_does_not_leave_orphan_pages() {
        const SMALL_USABLE: u32 = 512;

        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root, SMALL_USABLE, true);

        for rowid in 1_i64..=200_i64 {
            cursor.table_insert(&cx, rowid, &vec![b'R'; 180]).unwrap();
        }

        let root_page = cursor.pager.pages.get(&root.get()).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert!(
            root_header.page_type.is_interior(),
            "test requires an interior root after sustained inserts"
        );

        let all_pages: BTreeSet<u32> = cursor.pager.pages.keys().copied().collect();
        assert!(
            all_pages.len() > 6,
            "test requires enough pages to exercise repeated root overflow"
        );

        let mut reachable = BTreeSet::new();
        collect_reachable_pages(&cursor.pager, root, SMALL_USABLE, &mut reachable);
        assert_eq!(
            reachable, all_pages,
            "repeated root overflow must not leave detached child generations behind"
        );
    }

    #[test]
    fn test_cursor_table_insert_after_root_split() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut rowid = 1i64;
        loop {
            let payload = vec![b'Z'; 220];
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            let root_page = cursor.pager.inner.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }
            rowid += 1;
            assert!(
                rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        // Insert after split to exercise multi-level insert path.
        rowid += 1;
        cursor.table_insert(&cx, rowid, b"after-split").unwrap();

        assert!(cursor.table_move_to(&cx, rowid).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"after-split");
    }

    #[test]
    fn test_cursor_delete_single_leaf() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        cursor.delete(&cx).unwrap();

        let result = cursor.table_move_to(&cx, 2).unwrap();
        assert!(!result.is_found());
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_delete_rejects_duplicate_compact_leaf_pointer() {
        let mut page = build_leaf_table(&[(1, b"one"), (2, b"two"), (3, b"three")]);
        let duplicate_ptr = u16::from_be_bytes([page[10], page[11]]);
        page[12..14].copy_from_slice(&duplicate_ptr.to_be_bytes());
        page[5..7].copy_from_slice(&duplicate_ptr.to_be_bytes());

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, page);

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        assert!(cursor.first(&cx).unwrap());

        let err = cursor
            .delete(&cx)
            .expect_err("duplicate compact cell pointers must be rejected as corruption");
        assert!(
            matches!(err, FrankenError::DatabaseCorrupt { ref detail } if detail.contains("not monotone")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_cursor_delete_after_root_split_preserves_right_subtree_rows() {
        // After commit 5eed5a0a, table-leaf DELETE uses the defragment-on-delete
        // strategy (instead of maintaining a freeblock chain) for SQLite
        // `btreeComputeFreeSpace()` compatibility. Deleting every rowid in the
        // leftmost leaf of a split root may therefore trigger an eager merge
        // + balance_shallower that collapses the tree back to a single leaf.
        // The correctness invariant this test protects is that the
        // surviving right-subtree rows remain reachable and the cursor
        // positions cleanly on the successor.
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut max_rowid = 0i64;
        loop {
            let payload = vec![b'Q'; 220];
            cursor.table_insert(&cx, max_rowid, &payload).unwrap();
            let root_page = cursor.pager.inner.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }
            max_rowid += 1;
            assert!(
                max_rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        let root_page = cursor.pager.inner.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        let root_ptrs = cell::read_cell_pointers(root_page, &root_header, 0).unwrap();
        let first_divider_cell = CellRef::parse(
            root_page,
            usize::from(root_ptrs[0]),
            root_header.page_type,
            USABLE,
        )
        .unwrap();
        let leftmost_max_rowid = first_divider_cell.rowid.unwrap();
        assert!(leftmost_max_rowid >= 1);
        assert!(leftmost_max_rowid < max_rowid);

        for rowid in 0..=leftmost_max_rowid {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), leftmost_max_rowid + 1);
    }

    #[test]
    fn test_balance_for_delete_defers_until_leaf_empties() {
        // bd-yywuv (K2 deferred delete): prove balance_for_delete (the rebalance
        // fixup) fires ONLY when a leaf empties, not per DELETE. Deleting every
        // cell of the leftmost leaf EXCEPT the last must trigger zero rebalances;
        // the final delete that empties the leaf triggers the rebalance.
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));
        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Insert until the table root splits into a multi-level tree, so the
        // leftmost leaf is a non-root leaf that balance_for_delete can rebalance.
        let mut max_rowid = 0i64;
        loop {
            let payload = vec![b'Q'; 220];
            cursor.table_insert(&cx, max_rowid, &payload).unwrap();
            let root_page = cursor.pager.inner.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }
            max_rowid += 1;
            assert!(
                max_rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        // Max rowid contained in the leftmost leaf subtree.
        let root_page = cursor.pager.inner.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        let root_ptrs = cell::read_cell_pointers(root_page, &root_header, 0).unwrap();
        let first_divider_cell = CellRef::parse(
            root_page,
            usize::from(root_ptrs[0]),
            root_header.page_type,
            USABLE,
        )
        .unwrap();
        let leftmost_max_rowid = first_divider_cell.rowid.unwrap();
        assert!(
            leftmost_max_rowid >= 1,
            "leftmost leaf must hold >= 2 cells for this proof"
        );

        // Phase A: delete every cell of the leftmost leaf EXCEPT the last. The
        // leaf stays non-empty, so the deferred-delete strategy performs no
        // rebalance (balance_for_delete is gated on new_count == 0). The balance
        // counter lives on the leaf-reuse snapshot; the DELETE op total on the
        // metrics snapshot.
        let before_balance = btree_leaf_reuse_snapshot();
        let before_ops = btree_metrics_snapshot();
        for rowid in 0..leftmost_max_rowid {
            assert!(cursor.table_move_to(&cx, rowid).unwrap().is_found());
            cursor.delete(&cx).unwrap();
        }
        let after_partial_balance = btree_leaf_reuse_snapshot();
        let after_partial_ops = btree_metrics_snapshot();
        assert_eq!(
            after_partial_balance.balance_for_delete_calls
                - before_balance.balance_for_delete_calls,
            0,
            "deleting non-emptying cells must defer rebalancing (zero balance_for_delete)"
        );
        assert!(
            after_partial_ops.fsqlite_btree_operations_total.delete
                - before_ops.fsqlite_btree_operations_total.delete
                >= u64::try_from(leftmost_max_rowid).unwrap(),
            "Phase A must have performed real DELETEs"
        );

        // Phase B: delete the final cell of the leftmost leaf -> it empties ->
        // the deferred rebalance runs exactly now.
        assert!(
            cursor
                .table_move_to(&cx, leftmost_max_rowid)
                .unwrap()
                .is_found()
        );
        cursor.delete(&cx).unwrap();
        let after_empty_balance = btree_leaf_reuse_snapshot();
        assert!(
            after_empty_balance.balance_for_delete_calls
                - after_partial_balance.balance_for_delete_calls
                >= 1,
            "emptying a leaf must trigger the deferred rebalance exactly then"
        );

        set_btree_metrics_enabled(false);

        // Behavior preserved: the surviving right-subtree rows remain reachable.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), leftmost_max_rowid + 1);
    }

    #[test]
    fn test_cursor_delete_of_empty_leftmost_leaf_keeps_right_subtree_reachable() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut max_rowid = 0i64;
        loop {
            let payload = vec![b'P'; 220];
            cursor.table_insert(&cx, max_rowid, &payload).unwrap();
            let root_page = cursor.pager.inner.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }
            max_rowid += 1;
            assert!(
                max_rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        let root_page = cursor.pager.inner.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        let root_ptrs = cell::read_cell_pointers(root_page, &root_header, 0).unwrap();
        let first_divider_cell = CellRef::parse(
            root_page,
            usize::from(root_ptrs[0]),
            root_header.page_type,
            USABLE,
        )
        .unwrap();
        let leftmost_max_rowid = first_divider_cell.rowid.unwrap();
        assert!(leftmost_max_rowid >= 1);
        assert!(leftmost_max_rowid < max_rowid);

        for rowid in 0..=leftmost_max_rowid {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        // DELETE is now eager (commit 5eed5a0a) — the tree may have collapsed
        // back to a single leaf if the empty left sibling merged with the
        // right. The correctness invariant: surviving rowids remain reachable.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), leftmost_max_rowid + 1);
    }

    #[test]
    fn test_cursor_delete_updates_nonroot_table_separator_after_leaf_max_delete() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut max_rowid = 0_i64;
        for rowid in 1..=2_000_i64 {
            let payload = vec![b'S'; 1_400];
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            max_rowid = rowid;

            if measure_tree_depth(&cursor.pager, pn(2), USABLE) >= 3 {
                break;
            }
        }

        assert!(
            measure_tree_depth(&cursor.pager, pn(2), USABLE) >= 3,
            "failed to build a depth-3 table tree (reached rowid {max_rowid})"
        );

        let root_entry = cursor.load_page(&cx, pn(2)).unwrap();
        assert_eq!(
            root_entry.header.page_type,
            cell::BtreePageType::InteriorTable
        );
        let root_separator_before = cursor.parse_cell_at(&root_entry, 0).unwrap().rowid.unwrap();

        let left_subtree_page = BtCursor::<MemPageStore>::child_page_at(&root_entry, 0).unwrap();
        let left_subtree_before = cursor.load_page(&cx, left_subtree_page).unwrap();
        assert_eq!(
            left_subtree_before.header.page_type,
            cell::BtreePageType::InteriorTable
        );

        let target_rowid = cursor
            .parse_cell_at(&left_subtree_before, 0)
            .unwrap()
            .rowid
            .unwrap();
        assert!(
            target_rowid > 1,
            "target rowid must leave the leaf non-empty after delete"
        );

        let seek = cursor.table_move_to(&cx, target_rowid).unwrap();
        assert!(seek.is_found(), "target rowid should exist before delete");
        cursor.delete(&cx).unwrap();

        let root_after = cursor.load_page(&cx, pn(2)).unwrap();
        let root_separator_after = cursor.parse_cell_at(&root_after, 0).unwrap().rowid.unwrap();
        assert_eq!(
            root_separator_after, root_separator_before,
            "deleting a non-root subtree maximum must not perturb the enclosing subtree maximum"
        );

        let left_subtree_after = cursor.load_page(&cx, left_subtree_page).unwrap();
        let repaired_separator = cursor
            .parse_cell_at(&left_subtree_after, 0)
            .unwrap()
            .rowid
            .unwrap();
        assert_eq!(
            repaired_separator,
            target_rowid - 1,
            "non-root interior separator must shrink to the deleted leaf's new maximum"
        );

        let seek_after = cursor.table_move_to(&cx, target_rowid).unwrap();
        assert!(
            !seek_after.is_found(),
            "deleted rowid must not remain reachable after separator repair"
        );
        assert!(
            cursor
                .table_move_to(&cx, target_rowid - 1)
                .unwrap()
                .is_found()
        );
    }

    #[test]
    fn test_cursor_delete_updates_ancestor_table_separator_for_rightmost_descendant_max() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 15)], pn(4)));
        store
            .pages
            .insert(3, build_interior_table(&[(pn(5), 3), (pn(6), 8)], pn(7)));
        store
            .pages
            .insert(4, build_leaf_table(&[(20, b"L20"), (25, b"L25")]));
        store
            .pages
            .insert(5, build_leaf_table(&[(1, b"L1"), (3, b"L3")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(5, b"L5"), (8, b"L8")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(10, b"L10"), (15, b"L15")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let root_before = cursor.load_page(&cx, pn(2)).unwrap();
        let root_separator_before = cursor
            .parse_cell_at(&root_before, 0)
            .unwrap()
            .rowid
            .unwrap();
        assert_eq!(root_separator_before, 15);

        let seek = cursor.table_move_to(&cx, 15).unwrap();
        assert!(seek.is_found(), "target rowid should exist before delete");
        cursor.delete(&cx).unwrap();

        let root_after = cursor.load_page(&cx, pn(2)).unwrap();
        let root_separator_after = cursor.parse_cell_at(&root_after, 0).unwrap().rowid.unwrap();
        assert_eq!(
            root_separator_after, 10,
            "ancestor separator must shrink when the subtree's rightmost descendant maximum is deleted"
        );

        validate_table_tree_invariants(&cursor.pager, pn(2), USABLE)
            .expect("table invariants should still hold after ancestor separator repair");
        assert!(!cursor.table_move_to(&cx, 15).unwrap().is_found());
        assert!(cursor.table_move_to(&cx, 10).unwrap().is_found());
    }

    #[test]
    fn test_table_delete_all_from_left_leaf_preserves_right_subtree() {
        // DELETE is eager post-5eed5a0a; after deleting every rowid from the
        // left leaf, the tree may merge + collapse. The correctness check:
        // right-subtree rows are still reachable and the cursor lands on the
        // first surviving rowid.
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"L1"), (5, b"L5"), (10, b"L10")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(20, b"L20"), (25, b"L25")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        for rowid in [1_i64, 5, 10] {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        assert!(
            cursor.first(&cx).unwrap(),
            "remaining right subtree should still be reachable"
        );
        assert_eq!(cursor.rowid(&cx).unwrap(), 20);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 25);
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_empty_root_collapse_reclaims_detached_child_subtree_pages() {
        const SMALL_USABLE: u32 = 512;

        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root, SMALL_USABLE, true);

        for rowid in 1_i64..=200_i64 {
            cursor.table_insert(&cx, rowid, &vec![b'R'; 180]).unwrap();
        }

        let root_page = cursor.pager.pages.get(&root.get()).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert!(
            root_header.page_type.is_interior(),
            "test requires an interior root before delete-all cleanup"
        );

        for rowid in 1_i64..=200_i64 {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "row {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        assert!(
            !cursor.first(&cx).unwrap(),
            "delete-all cleanup should leave an empty tree"
        );

        let root_page = cursor.pager.pages.get(&root.get()).unwrap();
        let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
        assert!(
            root_header.page_type == cell::BtreePageType::LeafTable,
            "empty root cleanup should rewrite the root as a leaf"
        );

        let all_pages: BTreeSet<u32> = cursor.pager.pages.keys().copied().collect();
        let mut reachable = BTreeSet::new();
        collect_reachable_pages(&cursor.pager, root, SMALL_USABLE, &mut reachable);

        assert_eq!(
            all_pages,
            BTreeSet::from([root.get()]),
            "empty root cleanup should reclaim detached child pages"
        );
        assert_eq!(
            reachable, all_pages,
            "no unreachable pages should remain after collapsing the empty root"
        );
    }

    #[test]
    fn test_table_delete_leaves_compact_leaf_that_insert_fills_back_in() {
        // DELETE now defragments the leaf immediately (commit 5eed5a0a) for
        // SQLite `btreeComputeFreeSpace()` compatibility, so the old
        // freeblock-chain dead-space invariant was intentionally dropped. The
        // correctness check after a middle-rowid delete is that the leaf is
        // already compact (no freeblocks, no fragmented bytes) AND a
        // subsequent insert at the deleted rowid continues to work.
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        for rowid in 1_i64..=3 {
            cursor
                .table_insert(&cx, rowid, payload_for_rowid(rowid).as_slice())
                .unwrap();
        }

        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        cursor.delete(&cx).unwrap();

        let page_before = cursor.pager.read_page(&cx, pn(2)).unwrap();
        let header_before = BtreePageHeader::parse(&page_before, 0).unwrap();
        assert_eq!(
            header_before.first_freeblock, 0,
            "eager defrag on DELETE should leave no freeblock chain"
        );
        assert_eq!(
            header_before.fragmented_free_bytes, 0,
            "eager defrag on DELETE should leave no fragmented bytes"
        );

        // Re-insert at rowid 2 and confirm the leaf still reaches it. This
        // guards against a hypothetical regression where defrag corrupts
        // surrounding cells' offsets.
        let reclaimed_payload = vec![0x5A; payload_for_rowid(2).len()];
        cursor
            .table_insert(&cx, 2, reclaimed_payload.as_slice())
            .unwrap();

        let page_after = cursor.pager.read_page(&cx, pn(2)).unwrap();
        let header_after = BtreePageHeader::parse(&page_after, 0).unwrap();
        assert_eq!(header_after.first_freeblock, 0);
        assert_eq!(header_after.fragmented_free_bytes, 0);
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
    }

    #[test]
    fn test_table_delete_leaves_no_work_for_explicit_compaction() {
        // Post-5eed5a0a: DELETE compacts immediately, so
        // `compact_current_table_leaf` has nothing left to do and returns
        // `Ok(false)`. Keep the test as a lightweight regression guard that
        // (a) the leaf is already compact after DELETE, and (b) the cursor
        // still points at the correct successor rowid.
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        for rowid in 1_i64..=3 {
            cursor
                .table_insert(&cx, rowid, payload_for_rowid(rowid).as_slice())
                .unwrap();
        }

        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        cursor.delete(&cx).unwrap();
        assert!(
            !cursor.compact_current_table_leaf(&cx).unwrap(),
            "eager delete leaves the leaf already compact; explicit compaction should be a no-op"
        );

        let page = cursor.pager.read_page(&cx, pn(2)).unwrap();
        let header = BtreePageHeader::parse(&page, 0).unwrap();
        assert_eq!(header.first_freeblock, 0);
        assert_eq!(header.fragmented_free_bytes, 0);
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
    }

    #[test]
    fn test_cursor_delete_all_after_root_split() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut max_rowid = 0i64;
        loop {
            let payload = vec![b'Q'; 220];
            cursor.table_insert(&cx, max_rowid, &payload).unwrap();
            let root_page = cursor.pager.inner.pages.get(&2).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            if root_header.page_type == cell::BtreePageType::InteriorTable {
                break;
            }
            max_rowid += 1;
            assert!(
                max_rowid < 1000,
                "table root did not split under sustained inserts"
            );
        }

        for rowid in 0..=max_rowid {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        assert!(
            !cursor.first(&cx).unwrap(),
            "tree should be empty after total delete"
        );
        assert!(cursor.eof());

        // The root page should have collapsed to a leaf (depth 1).
        let root_data = cursor.pager.read_page(&cx, pn(2)).unwrap();
        let root_header = BtreePageHeader::parse(&root_data, 0).unwrap();
        assert!(
            root_header.page_type.is_leaf(),
            "root should collapse to leaf after all rows deleted, got {:?}",
            root_header.page_type
        );
        assert_eq!(root_header.cell_count, 0);
    }

    #[test]
    fn test_e2e_bd_2kvo() {
        const TOTAL_ROWS: i64 = 2_000;
        const DELETE_ROWS: usize = 1_000;

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let mut expected = BTreeMap::<i64, Vec<u8>>::new();

        for rowid in 1..=TOTAL_ROWS {
            let payload = payload_for_rowid(rowid);
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            expected.insert(rowid, payload);
        }

        for (rowid, payload) in &expected {
            let seek = cursor.table_move_to(&cx, *rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} not found");
            let got = cursor.payload(&cx).unwrap();
            assert_eq!(
                got.len(),
                payload.len(),
                "payload length mismatch at rowid {rowid}"
            );
            assert_eq!(&got[..], &payload[..], "payload mismatch at rowid {rowid}");
        }

        let mut deletion_order: Vec<i64> = expected.keys().copied().collect();
        deterministic_shuffle(&mut deletion_order, 0x0BAD_5EED);

        for rowid in deletion_order.into_iter().take(DELETE_ROWS) {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
            expected.remove(&rowid);
        }

        if expected.is_empty() {
            assert!(!cursor.first(&cx).unwrap());
            assert!(cursor.eof());
            return;
        }

        let mut expected_iter = expected.iter();
        assert!(cursor.first(&cx).unwrap());
        loop {
            let rowid = cursor.rowid(&cx).unwrap();
            let payload = cursor.payload(&cx).unwrap();

            let (expected_rowid, expected_payload) =
                expected_iter.next().expect("cursor yielded extra row");
            assert_eq!(rowid, *expected_rowid);
            assert_eq!(payload.len(), expected_payload.len());
            assert_eq!(&payload[..], expected_payload);

            if !cursor.next(&cx).unwrap() {
                break;
            }
        }

        assert!(
            expected_iter.next().is_none(),
            "cursor missed one or more rows during forward scan"
        );
    }

    #[test]
    fn test_e2e_btree_prefetch_latency() {
        const TOTAL_ROWS: i64 = 1_500;

        let mut seed_store = MemPageStore::new(USABLE);
        seed_store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut seed_cursor = BtCursor::new(seed_store, pn(2), USABLE, true);
        for rowid in 1..=TOTAL_ROWS {
            let payload = payload_for_rowid(rowid);
            seed_cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        let baseline_store = seed_cursor.pager.clone();
        let prefetch_store = PrefetchProbeStore::new(seed_cursor.pager);

        let mut workload: Vec<i64> = (1..=TOTAL_ROWS).collect();
        deterministic_shuffle(&mut workload, 0x0FEE_D123);

        let mut baseline_cursor = BtCursor::new(baseline_store, pn(2), USABLE, true);
        let baseline_started = Instant::now();
        let mut baseline_total_bytes = 0usize;
        for rowid in &workload {
            let result = baseline_cursor.table_move_to(&cx, *rowid).unwrap();
            assert!(result.is_found(), "baseline lookup miss for rowid={rowid}");
            baseline_total_bytes += baseline_cursor.payload(&cx).unwrap().len();
        }
        let baseline_elapsed = baseline_started.elapsed();

        let mut hinted_cursor = BtCursor::new(prefetch_store, pn(2), USABLE, true);
        let hinted_started = Instant::now();
        let mut hinted_total_bytes = 0usize;
        for rowid in &workload {
            let result = hinted_cursor.table_move_to(&cx, *rowid).unwrap();
            assert!(result.is_found(), "hinted lookup miss for rowid={rowid}");
            hinted_total_bytes += hinted_cursor.payload(&cx).unwrap().len();
        }
        let hinted_elapsed = hinted_started.elapsed();

        assert_eq!(baseline_total_bytes, hinted_total_bytes);
        assert!(
            !hinted_cursor.pager.hinted_pages().is_empty(),
            "prefetch implementation must remain fully safe"
        );

        let allowed_regression = baseline_elapsed.saturating_mul(50) + Duration::from_millis(250);
        assert!(
            hinted_elapsed <= allowed_regression,
            "prefetch workload regressed too much: baseline={baseline_elapsed:?}, hinted={hinted_elapsed:?}"
        );
    }

    #[test]
    fn test_table_seek_prefetches_interior_children_along_descent_path() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(USABLE);
        store
            .write_page(&cx, pn(2), &build_interior_table(&[(pn(3), 15)], pn(4)))
            .unwrap();
        store
            .write_page(
                &cx,
                pn(3),
                &build_interior_table(&[(pn(5), 3), (pn(6), 8)], pn(7)),
            )
            .unwrap();
        store
            .write_page(&cx, pn(4), &build_interior_table(&[(pn(8), 25)], pn(9)))
            .unwrap();
        store
            .write_page(
                &cx,
                pn(5),
                &build_leaf_table(&[(1, b"a"), (2, b"b"), (3, b"c")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(6),
                &build_leaf_table(&[(4, b"d"), (5, b"e"), (8, b"f")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(7),
                &build_leaf_table(&[(9, b"g"), (10, b"h"), (15, b"i")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(8),
                &build_leaf_table(&[(16, b"j"), (20, b"k"), (24, b"l")]),
            )
            .unwrap();
        store
            .write_page(
                &cx,
                pn(9),
                &build_leaf_table(&[(26, b"m"), (30, b"n"), (40, b"o")]),
            )
            .unwrap();

        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let result = cursor.table_move_to(&cx, 20).unwrap();

        assert_eq!(result, SeekResult::Found);
        assert_eq!(cursor.pager.hinted_pages(), vec![pn(4), pn(8)]);
    }

    #[test]
    fn test_prefetch_valid_address() {
        let cx = Cx::new();
        let probe_store = build_prefetch_descent_probe_store();
        probe_store.clear_probe();

        for rowid in [2_i64, 5, 10, 20, 24, 26, 35, 41] {
            let mut cursor = BtCursor::new(probe_store.clone(), pn(2), USABLE, true);
            cursor
                .table_seek_for_insert(&cx, rowid)
                .expect("descent path should remain valid");
        }

        let snapshot = probe_store.snapshot();
        assert!(
            snapshot.prefetch_issued_count > 0,
            "test setup must exercise at least one interior prefetch"
        );
        assert!(
            !snapshot.read_pages.is_empty(),
            "descent path should read at least one child page after prefetch"
        );
        assert_prefetch_snapshot_addresses_are_valid(&probe_store, &snapshot, pn(2));
        assert_eq!(
            snapshot.prefetch_hit_count, snapshot.prefetch_issued_count,
            "insert descent should immediately consume each hinted child page"
        );
    }

    #[test]
    fn test_prefetch_sequential_insert() {
        let cx = Cx::new();
        let probe_store = build_prefetch_descent_probe_store();
        probe_store.clear_probe();

        for rowid in 26_i64..=40_i64 {
            let mut cursor = BtCursor::new(probe_store.clone(), pn(2), USABLE, true);
            cursor
                .table_seek_for_insert(&cx, rowid)
                .expect("right-edge insert descent should succeed");
        }

        let snapshot = probe_store.snapshot();
        assert_prefetch_snapshot_addresses_are_valid(&probe_store, &snapshot, pn(2));
        assert_eq!(
            snapshot.prefetch_hit_count, snapshot.prefetch_issued_count,
            "right-edge insert descent should consume every hinted page"
        );
        assert!(
            snapshot
                .hinted_pages
                .iter()
                .all(|page_no| matches!(page_no.get(), 4 | 9)),
            "sequential insert descent should stay on the rightmost child chain, got {:?}",
            snapshot.hinted_pages
        );
        assert!(
            snapshot.hinted_pages.contains(&pn(4)) && snapshot.hinted_pages.contains(&pn(9)),
            "sequential insert descent should prefetch both rightmost interior levels"
        );
    }

    #[test]
    fn test_prefetch_random_insert() {
        let cx = Cx::new();
        let probe_store = build_prefetch_descent_probe_store();
        probe_store.clear_probe();

        let mut targets: Vec<i64> = (1_i64..=40_i64).collect();
        deterministic_shuffle(&mut targets, 0xE2A4_0001_u64);

        for rowid in targets.into_iter().take(24) {
            let mut cursor = BtCursor::new(probe_store.clone(), pn(2), USABLE, true);
            cursor
                .table_seek_for_insert(&cx, rowid)
                .expect("random insert descent should succeed");
        }

        let snapshot = probe_store.snapshot();
        assert_prefetch_snapshot_addresses_are_valid(&probe_store, &snapshot, pn(2));
        assert_eq!(
            snapshot.prefetch_hit_count, snapshot.prefetch_issued_count,
            "random insert descent should still consume every hinted child page"
        );
        assert!(
            snapshot.hinted_pages.contains(&pn(3))
                && snapshot.hinted_pages.contains(&pn(4))
                && snapshot
                    .hinted_pages
                    .iter()
                    .any(|page_no| matches!(page_no.get(), 6 | 8)),
            "random insert descent should cover both left/mid and right child paths, got {:?}",
            snapshot.hinted_pages
        );
    }

    #[test]
    fn test_prefetch_no_crash_on_evicted() {
        let cx = Cx::new();
        let mut inner = MemPageStore::new(USABLE);
        inner
            .write_page(&cx, pn(2), &build_leaf_table(&[(1, b"alive")]))
            .unwrap();
        inner.free_page(&cx, pn(2)).unwrap();

        let probe_store = PrefetchProbeStore::new(inner);
        probe_store.prefetch_page_hint(&cx, pn(2));

        let snapshot = probe_store.snapshot();
        assert_eq!(snapshot.prefetch_issued_count, 1);
        assert_eq!(snapshot.slot_prefetch_count, 1);
        assert_eq!(snapshot.page_prefetch_count, 0);
        assert_eq!(snapshot.prefetch_hit_count, 0);
        assert_eq!(snapshot.missing_page_pages, vec![pn(2)]);
    }

    #[test]
    fn test_cache_line_alignment() {
        const CACHE_LINE_BYTES: usize = 64;

        for page_type in [
            cell::BtreePageType::LeafTable,
            cell::BtreePageType::InteriorTable,
            cell::BtreePageType::LeafIndex,
            cell::BtreePageType::InteriorIndex,
        ] {
            let header_bytes = usize::from(page_type.header_size());
            assert!(
                header_bytes <= CACHE_LINE_BYTES,
                "page header should fit in one cache line for prefetch locality: type={page_type:?} header_bytes={header_bytes}"
            );
            assert!(
                cell::header_offset_for_page(pn(2)) + header_bytes <= CACHE_LINE_BYTES,
                "prefetching page.as_ptr() should warm the entire header cache line: type={page_type:?} header_bytes={header_bytes}"
            );
        }
    }

    #[test]
    fn test_table_interior_descent_uses_inline_page_pointer_reads() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_table(&[(pn(3), 20), (pn(4), 40), (pn(5), 60)], pn(6)),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let mut entry = cursor.load_page(&cx, pn(2)).unwrap();
        entry.cell_pointers.clear();

        let child_idx =
            BtCursor::<MemPageStore>::binary_search_table_interior(&cx, &entry, 35).unwrap();
        assert_eq!(child_idx, 1);
        assert_eq!(
            BtCursor::<MemPageStore>::read_interior_child_inline(&entry, child_idx).unwrap(),
            pn(4)
        );
    }

    #[test]
    fn test_index_interior_descent_uses_inline_page_pointer_reads() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_interior_index(&[(pn(3), b"b"), (pn(4), b"d")], pn(5)),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);
        let mut entry = cursor.load_page(&cx, pn(2)).unwrap();
        entry.cell_pointers.clear();

        let search = cursor
            .binary_search_index_interior(&cx, &entry, b"c")
            .unwrap();
        assert_eq!(search, BinarySearchResult::NotFound(1));
        assert_eq!(
            BtCursor::<MemPageStore>::read_interior_child_inline(&entry, 1).unwrap(),
            pn(4)
        );
        assert_eq!(
            BtCursor::<MemPageStore>::read_interior_child_inline(&entry, 2).unwrap(),
            pn(5)
        );
    }

    #[test]
    fn test_btree_insert_delete_5k() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let mut remaining = BTreeSet::new();

        // Insert 10,000 rows so that deleting 5,000 leaves 5,000 survivors.
        for i in 1..=10_000_i64 {
            let payload = payload_for_rowid(i);
            cursor.table_insert(&cx, i, &payload).unwrap();
            remaining.insert(i);
        }

        let mut deletion_order: Vec<i64> = remaining.iter().copied().collect();
        deterministic_shuffle(&mut deletion_order, 0x00D1_5EA5);

        for rowid in deletion_order.into_iter().take(5_000) {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
            remaining.remove(&rowid);
        }

        assert_eq!(remaining.len(), 5_000);
        assert!(cursor.first(&cx).unwrap());

        let mut expected_iter = remaining.iter();
        loop {
            let rowid = cursor.rowid(&cx).unwrap();
            let expected = expected_iter.next().expect("cursor yielded extra row");
            assert_eq!(&rowid, expected);

            if !cursor.next(&cx).unwrap() {
                break;
            }
        }

        assert!(
            expected_iter.next().is_none(),
            "cursor missed one or more rows after delete workload"
        );
    }

    #[test]
    fn test_btree_insert_delete_sorted_order() {
        test_btree_insert_delete_5k();
    }

    #[test]
    fn test_table_insert_rightmost_hint_appends_and_falls_back_for_midstream_key() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();
        cursor
            .table_insert_rightmost_hint(&cx, 3, payload_for_rowid(3).as_slice())
            .unwrap();
        cursor
            .table_insert_rightmost_hint(&cx, 2, payload_for_rowid(2).as_slice())
            .unwrap();

        for rowid in 4..=256_i64 {
            let payload = payload_for_rowid(rowid);
            cursor
                .table_insert_rightmost_hint(&cx, rowid, payload.as_slice())
                .unwrap();
        }

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=256_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 256 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_insert_rightmost_leaf_hint_reuses_leaf_and_falls_back_after_split() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();

        let mut hinted_leaf = cursor
            .current_page()
            .expect("first append should leave the cursor on a concrete leaf");
        for rowid in 2..=256_i64 {
            let payload = payload_for_rowid(rowid);
            cursor
                .table_insert_rightmost_leaf_hint(&cx, hinted_leaf, rowid, payload.as_slice())
                .unwrap();
            if let Some(current_leaf) = cursor.current_page() {
                hinted_leaf = current_leaf;
            }
        }

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=256_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 256 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    /// OPT-A3: verify the lightweight last-rowid read on the append hint path
    /// correctly rejects a mismatched hint and the cursor falls back cleanly.
    ///
    /// The stored `hinted_last_rowid` is deliberately wrong (off by one) so
    /// the fast-path check must fail and return `Ok(None)`, after which the
    /// caller can retry with the real `table_insert_rightmost_hint` and
    /// produce identical on-disk state to a fresh insert.
    fn test_table_try_append_rightmost_leaf_hint_known_last_rowid_rejects_mismatched_hint() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        // Seed the table with two rows so there IS a "last rowid" to mismatch.
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();
        cursor
            .table_insert_rightmost_hint(&cx, 2, payload_for_rowid(2).as_slice())
            .unwrap();
        let hinted_leaf = cursor
            .current_page()
            .expect("append should leave cursor on a concrete leaf");

        // Now pass a DELIBERATELY WRONG hinted_last_rowid (1 instead of 2).
        // The OPT-A3 fast path reads the last cell's rowid from the page
        // directly and must notice the mismatch, returning Ok(None).
        let wrong_hinted_last_rowid = 1_i64;
        let result = cursor
            .table_try_append_rightmost_leaf_hint_known_last_rowid(
                &cx,
                hinted_leaf,
                wrong_hinted_last_rowid,
                3,
                payload_for_rowid(3).as_slice(),
            )
            .expect("mismatch path must not error");
        assert!(
            result.is_none(),
            "mismatched hinted_last_rowid should force fallback, got {result:?}"
        );

        // Cache should have been cleared on the mismatch.
        assert!(
            cursor.rightmost_leaf_cache.is_none(),
            "mismatched hint should clear the rightmost-leaf cache"
        );

        // The caller can now recover via the generic rightmost-hint path and
        // observe the same end-state as if the fast path had never been tried.
        cursor
            .table_insert_rightmost_hint(&cx, 3, payload_for_rowid(3).as_slice())
            .unwrap();

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=3_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 3 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_try_append_rightmost_leaf_hint_known_last_rowid_reuses_leaf_and_falls_back() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();

        let mut hinted_leaf = cursor
            .current_page()
            .expect("first append should leave the cursor on a concrete leaf");
        let mut hinted_last_rowid = 1_i64;
        for rowid in 2..=256_i64 {
            let payload = payload_for_rowid(rowid);
            if let Some(leaf_page) = cursor
                .table_try_append_rightmost_leaf_hint_known_last_rowid(
                    &cx,
                    hinted_leaf,
                    hinted_last_rowid,
                    rowid,
                    payload.as_slice(),
                )
                .unwrap()
            {
                hinted_leaf = leaf_page;
                hinted_last_rowid = rowid;
                assert_eq!(cursor.current_page(), Some(leaf_page));
                let cached = cursor
                    .rightmost_leaf_cache
                    .as_ref()
                    .expect("successful helper append should refresh the rightmost-leaf cache");
                assert_eq!(cached.page_no, leaf_page);
                assert_eq!(cached.rowid, rowid);
                continue;
            }

            cursor
                .table_insert_rightmost_hint(&cx, rowid, payload.as_slice())
                .unwrap();
            hinted_leaf = cursor
                .current_page()
                .expect("fallback append should leave the cursor on a concrete leaf");
            hinted_last_rowid = rowid;
        }

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=256_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 256 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_try_append_rightmost_leaf_hint_known_last_rowid_with_state_refreshes_hint() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();

        let hinted_leaf = cursor
            .current_page()
            .expect("first append should leave the cursor on a concrete leaf");
        cursor.clear_rightmost_leaf_cache();
        cursor.stack.clear();
        cursor.at_eof = true;

        let cached_leaf = cursor
            .table_try_append_rightmost_leaf_hint_known_last_rowid_with_state(
                &cx,
                hinted_leaf,
                1,
                2,
                payload_for_rowid(2).as_slice(),
            )
            .unwrap()
            .expect("known-last-rowid helper should append on a fresh cursor");

        assert_eq!(cached_leaf.leaf_page(), hinted_leaf);
        assert_eq!(cached_leaf.last_rowid(), 2);
        assert!(
            cached_leaf.retains_page_data(),
            "with_state helper should return the refreshed retained leaf image"
        );
        assert_eq!(
            cursor.current_page(),
            None,
            "with_state helper should not rebuild cursor stack state just to return a retained hint"
        );
        assert!(
            cursor.rightmost_leaf_cache.is_none(),
            "with_state helper skips internal cache hydration; the returned hint is authoritative"
        );
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_reuses_retained_leaf_image() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), pn(2), USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("first append should seed a retained rightmost-leaf image");
        for rowid in 2..=256_i64 {
            let payload = payload_for_rowid(rowid);
            let previous_leaf = hint.leaf_page();
            cursor.pager.clear_reads();
            if cursor
                .table_try_append_cached_rightmost_leaf_hint(
                    &cx,
                    &mut hint,
                    rowid,
                    payload.as_slice(),
                )
                .unwrap()
            {
                assert_eq!(hint.last_rowid(), rowid);
                let read_pages = cursor.pager.read_pages();
                if hint.leaf_page() == previous_leaf {
                    assert!(
                        read_pages.is_empty(),
                        "cached retained-leaf append should not re-read the tree when it stays on the same leaf: {read_pages:?}"
                    );
                }
                continue;
            }

            cursor
                .table_insert_rightmost_hint(&cx, rowid, payload.as_slice())
                .unwrap();
            hint = cursor
                .table_cached_rightmost_leaf_hint()
                .expect("fallback append should refresh the retained rightmost-leaf image");
            assert_eq!(hint.last_rowid(), rowid);
        }

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=256_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 256 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_with_writer_mutates_staged_leaf() {
        let cx = Cx::new();
        let root = pn(2);
        let store = StagedMutationStore::new(MemPageStore::with_empty_table(root, USABLE));
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        cursor.table_insert_rightmost_hint(&cx, 1, b"seed").unwrap();

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed append should capture a retained rightmost-leaf image");
        let payload = b"direct-retained-page-payload";
        let appended = cursor
            .table_try_append_cached_rightmost_leaf_hint_with_writer(
                &cx,
                &mut hint,
                2,
                payload.len(),
                |dst| {
                    dst.copy_from_slice(payload);
                    Ok(())
                },
            )
            .unwrap();

        assert!(
            appended,
            "writer should append directly into the retained rightmost leaf image"
        );
        assert_eq!(hint.last_rowid(), 2);
        assert!(
            !hint.retains_page_data(),
            "staged-page writer append should drop the stale retained page image"
        );
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), payload);
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_with_writer_updates_retained_page_image() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor.table_insert_rightmost_hint(&cx, 1, b"seed").unwrap();

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed append should capture a retained rightmost-leaf image");
        let payload = b"direct-retained-page-payload";
        let appended = cursor
            .table_try_append_cached_rightmost_leaf_hint_with_writer(
                &cx,
                &mut hint,
                2,
                payload.len(),
                |dst| {
                    dst.copy_from_slice(payload);
                    Ok(())
                },
            )
            .unwrap();

        assert!(
            appended,
            "writer should append directly into the retained rightmost leaf image"
        );
        assert_eq!(hint.last_rowid(), 2);
        let retained_page = hint
            .page_data
            .as_ref()
            .expect("retained writer append should preserve a hot page image")
            .as_bytes()
            .to_vec();

        let mut retained_store = MemPageStore::new(USABLE);
        retained_store
            .pages
            .insert(hint.leaf_page().get(), retained_page);
        let mut retained_cursor = BtCursor::new(retained_store, pn(2), USABLE, true);
        assert!(retained_cursor.table_move_to(&cx, 2).unwrap().is_found());
        assert_eq!(retained_cursor.payload(&cx).unwrap(), payload);
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_with_writer_error_does_not_publish_cell() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), pn(2), USABLE, true);
        cursor.table_insert_rightmost_hint(&cx, 1, b"seed").unwrap();

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed append should capture a retained rightmost-leaf image");
        let error = cursor
            .table_try_append_cached_rightmost_leaf_hint_with_writer(&cx, &mut hint, 2, 8, |dst| {
                dst.copy_from_slice(b"partial!");
                Err(FrankenError::internal("forced retained writer failure"))
            })
            .unwrap_err();

        assert!(error.to_string().contains("forced retained writer failure"));
        assert_eq!(hint.last_rowid(), 1);
        assert!(
            hint.retains_page_data(),
            "failed writer should preserve the retained pre-append page image for fallback"
        );
        assert!(
            !cursor.table_move_to(&cx, 2).unwrap().is_found(),
            "failed retained writer append must not publish a new row"
        );

        assert!(
            cursor
                .table_try_append_cached_rightmost_leaf_hint(&cx, &mut hint, 2, b"fallback")
                .unwrap(),
            "byte-slice fallback should still be able to use the retained hint"
        );
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"fallback");
    }

    #[test]
    fn test_cached_rightmost_hint_drops_page_data_after_staged_mutation() {
        const SMALL_USABLE: u32 = 256;

        let cx = Cx::new();
        let root = pn(2);
        let payload = vec![b'S'; 120];
        let store = StagedMutationStore::new(MemPageStore::with_empty_table(root, SMALL_USABLE));
        let mut cursor = BtCursor::new(store, root, SMALL_USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, &payload)
            .expect("seed insert should succeed");

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed insert should capture a retained rightmost-leaf image");
        assert!(
            hint.retains_page_data(),
            "seed hint should start with a retained page image"
        );

        assert!(
            cursor
                .table_try_append_cached_rightmost_leaf_hint(&cx, &mut hint, 2, &payload)
                .expect("staged-page mutation append should succeed"),
            "second row should append through the staged-page mutation path"
        );
        assert_eq!(hint.last_rowid(), 2);
        assert!(
            !hint.retains_page_data(),
            "a staged-page mutation must drop the stale retained page image"
        );

        if !cursor
            .table_try_append_cached_rightmost_leaf_hint(&cx, &mut hint, 3, &payload)
            .expect("fallback probe after staged-page mutation should not corrupt state")
        {
            cursor
                .table_insert_rightmost_hint(&cx, 3, &payload)
                .expect("caller fallback should preserve rows after stale hint bytes are dropped");
        }

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=3_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 3 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_cached_rightmost_hint_overflow_encode_error_retains_cell_buffer() {
        const SMALL_USABLE: u32 = 256;
        let inner = Rc::new(RefCell::new(MemPageStore::with_empty_table(
            pn(2),
            SMALL_USABLE,
        )));
        let cx = Cx::new();
        let mut cursor = BtCursor::new(
            FailingOverflowStore::new(inner, usize::MAX),
            pn(2),
            SMALL_USABLE,
            true,
        );
        cursor
            .table_insert_rightmost_hint(&cx, 1, b"seed")
            .expect("seed insert should succeed");
        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed insert should capture a retained rightmost-leaf hint");

        cursor.cell_buf = Vec::with_capacity(4096);
        let expected_capacity = cursor.cell_buf.capacity();
        cursor.pager.fail_on_write = cursor.pager.write_count.saturating_add(1);

        let overflow_payload = vec![b'X'; 512];
        let error = cursor
            .table_try_append_cached_rightmost_leaf_hint(&cx, &mut hint, 2, &overflow_payload)
            .expect_err("injected overflow write failure should abort append");

        assert!(
            matches!(&error, FrankenError::Internal(msg) if msg == "injected write failure"),
            "expected injected write failure, got {error:?}"
        );
        assert!(
            cursor.cell_buf.capacity() >= expected_capacity,
            "retained-hint overflow encode failure should return the reusable cell buffer"
        );
    }

    #[test]
    fn test_table_overwrite_current_payload_same_size_no_overflow_preserves_cell_shape() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"aaaaaaaa"), (2, b"bbbbbbbb"), (3, b"cccccccc")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());

        assert!(
            cursor
                .table_overwrite_current_payload_same_size_no_overflow(&cx, 2, b"BBBBBBBB")
                .unwrap()
        );
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert_eq!(cursor.payload(&cx).unwrap(), b"BBBBBBBB");

        assert!(
            !cursor
                .table_overwrite_current_payload_same_size_no_overflow(&cx, 2, b"too-long!")
                .unwrap(),
            "size-changing updates must fall back to delete+insert"
        );
        assert_eq!(cursor.payload(&cx).unwrap(), b"BBBBBBBB");

        assert!(cursor.table_move_to(&cx, 1).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"aaaaaaaa");
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.payload(&cx).unwrap(), b"cccccccc");
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_drops_stale_internal_cache() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, payload_for_rowid(1).as_slice())
            .unwrap();

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed insert should capture a retained rightmost-leaf hint");
        assert!(
            cursor
                .table_try_append_cached_rightmost_leaf_hint(
                    &cx,
                    &mut hint,
                    2,
                    payload_for_rowid(2).as_slice(),
                )
                .unwrap(),
            "retained-hint helper should append directly on the cached leaf"
        );
        assert!(
            cursor.rightmost_leaf_cache.is_none(),
            "retained-hint appends must not leave a stale internal leaf image behind"
        );

        cursor
            .table_insert(&cx, 3, payload_for_rowid(3).as_slice())
            .unwrap();

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=3_i64 {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < 3 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_handles_split_fallback() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        const SMALL_USABLE: u32 = 256;

        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let payload = vec![b'S'; 120];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, SMALL_USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, &payload)
            .expect("seed insert should succeed");

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed insert should capture a retained rightmost-leaf hint");
        let mut observed_split = false;
        let mut inserted_through = 1_i64;

        for rowid in 2..=32_i64 {
            let previous_leaf = hint.leaf_page();
            cursor.stack.clear();
            cursor.at_eof = true;
            cursor.clear_rightmost_leaf_cache();

            assert!(
                cursor
                    .table_try_append_cached_rightmost_leaf_hint(&cx, &mut hint, rowid, &payload)
                    .expect("cached rightmost-leaf helper should handle split fallback"),
                "helper should not need a second append API when the hinted leaf fills"
            );
            inserted_through = rowid;

            if hint.leaf_page() != previous_leaf {
                observed_split = true;
                break;
            }
        }

        assert!(
            observed_split,
            "test setup should force at least one cached-hint leaf split"
        );

        assert!(cursor.first(&cx).unwrap());
        for expected_rowid in 1..=inserted_through {
            assert_eq!(cursor.rowid(&cx).unwrap(), expected_rowid);
            if expected_rowid < inserted_through {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_table_try_append_cached_rightmost_leaf_hint_split_reads_only_parent_after_root_split() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        const SMALL_USABLE: u32 = 256;

        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let payload = vec![b'P'; 120];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, SMALL_USABLE, true);
        cursor
            .table_insert_rightmost_hint(&cx, 1, &payload)
            .expect("seed insert should succeed");

        let mut hint = cursor
            .table_cached_rightmost_leaf_hint()
            .expect("seed insert should capture a retained rightmost-leaf hint");
        let mut observed_post_root_split = false;

        for rowid in 2..=128_i64 {
            let previous_leaf = hint.leaf_page();
            let parent_before = hint.parent_page();
            cursor.stack.clear();
            cursor.at_eof = true;
            cursor.clear_rightmost_leaf_cache();
            cursor.pager.clear_reads();

            assert!(
                cursor
                    .table_try_append_cached_rightmost_leaf_hint(&cx, &mut hint, rowid, &payload)
                    .expect("cached rightmost-leaf helper should handle split fallback"),
                "helper should not need a second append API when the hinted leaf fills"
            );

            if let Some(parent_before) = parent_before.filter(|_| hint.leaf_page() != previous_leaf)
            {
                observed_post_root_split = true;
                assert_eq!(
                    cursor.pager.read_pages(),
                    vec![parent_before],
                    "cached-hint split should read only the parent page once it has a parent hint"
                );
                break;
            }
        }

        assert!(
            observed_post_root_split,
            "test setup should reach a cached-hint split after the first root split"
        );
    }

    #[test]
    fn test_table_insert_reuses_rightmost_leaf_cache_for_sequential_appends() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'A'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1..=128_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        let root_entry = cursor.reload_page_fresh(&cx, root).unwrap();
        assert!(
            root_entry.header.page_type.is_interior(),
            "test requires an interior root so the uncached path would revisit it"
        );

        let cached_before = cursor
            .rightmost_leaf_cache
            .clone()
            .expect("sequential inserts should seed the rightmost-leaf cache");

        let mut cell_data = Vec::new();
        cursor
            .encode_table_leaf_cell_into(&cx, 129, &payload, &mut cell_data)
            .unwrap();
        let rightmost_leaf = cursor.load_page(&cx, cached_before.page_no).unwrap();
        let header_offset = cell::header_offset_for_page(cached_before.page_no);
        let content_offset = rightmost_leaf.header.content_offset(cursor.usable_size);
        let new_content_offset = content_offset
            .checked_sub(cell_data.len())
            .expect("test requires free space on the cached rightmost leaf");
        let ptr_array_end = header_offset
            + usize::from(rightmost_leaf.header.page_type.header_size())
            + (usize::from(rightmost_leaf.header.cell_count) + 1) * 2;
        assert!(
            ptr_array_end <= new_content_offset,
            "test setup must leave room for one more append on the cached rightmost leaf"
        );

        cursor.pager.clear_reads();
        cursor.table_insert(&cx, 129, &payload).unwrap();
        assert!(
            cursor.pager.read_pages().is_empty(),
            "cached rightmost-leaf append should not re-read the leaf page"
        );

        let cached_after = cursor
            .rightmost_leaf_cache
            .clone()
            .expect("successful append should refresh the rightmost-leaf cache");
        assert_eq!(cached_after.page_no, cached_before.page_no);
        assert_eq!(cached_after.rowid, 129);
    }

    #[test]
    fn test_table_insert_reuses_rightmost_leaf_cache_for_multiple_sequential_appends() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'M'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1..=128_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        let root_entry = cursor.reload_page_fresh(&cx, root).unwrap();
        assert!(
            root_entry.header.page_type.is_interior(),
            "test requires an interior root so the uncached path would revisit it"
        );

        // The rightmost-leaf cache has variable headroom after 128 sequential
        // inserts (depends on split cadence); continue inserting until we
        // land right after a split so at least 3 appends fit before the next
        // split. This keeps the test's core claim — "sequential appends land
        // in the cached leaf with zero page reads" — independent of the
        // specific split policy and page-packing invariants.
        let mut next_rowid = 129_i64;
        while next_rowid < 512 {
            let mut probe_cell = Vec::new();
            cursor
                .encode_table_leaf_cell_into(&cx, next_rowid, &payload, &mut probe_cell)
                .unwrap();
            let cached = cursor
                .rightmost_leaf_cache
                .clone()
                .expect("sequential inserts should seed the rightmost-leaf cache");
            let header_offset = cell::header_offset_for_page(cached.page_no);
            let content_offset = cached.header.content_offset(cursor.usable_size);
            let header_size = usize::from(cached.header.page_type.header_size());
            let cells_needed_for_assert = 3_usize;
            let has_room = content_offset
                .checked_sub(probe_cell.len() * cells_needed_for_assert)
                .is_some_and(|after_three| {
                    let ptr_array_after = header_offset
                        + header_size
                        + (usize::from(cached.header.cell_count) + cells_needed_for_assert) * 2;
                    ptr_array_after <= after_three
                });
            if has_room {
                break;
            }
            cursor.table_insert(&cx, next_rowid, &payload).unwrap();
            next_rowid += 1;
        }

        let mut appended = 0_u32;
        let start_rowid = next_rowid;
        for rowid in start_rowid..start_rowid + 128 {
            let cached = cursor
                .rightmost_leaf_cache
                .clone()
                .expect("sequential inserts should seed the rightmost-leaf cache");
            let mut cell_data = Vec::new();
            cursor
                .encode_table_leaf_cell_into(&cx, rowid, &payload, &mut cell_data)
                .unwrap();
            let header_offset = cell::header_offset_for_page(cached.page_no);
            let content_offset = cached.header.content_offset(cursor.usable_size);
            let Some(new_content_offset) = content_offset.checked_sub(cell_data.len()) else {
                break;
            };
            let ptr_array_end = header_offset
                + usize::from(cached.header.page_type.header_size())
                + (usize::from(cached.header.cell_count) + 1) * 2;
            if ptr_array_end > new_content_offset {
                break;
            }

            cursor.pager.clear_reads();
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            assert!(
                cursor.pager.read_pages().is_empty(),
                "cached sequential append should avoid page reloads before the next split (rowid {rowid})"
            );
            appended += 1;
        }

        assert!(
            appended >= 3,
            "test setup should admit several zero-read cached appends before the next split, got {appended}"
        );
        let cached_after = cursor
            .rightmost_leaf_cache
            .as_ref()
            .expect("successful append should preserve the rightmost-leaf cache");
        assert_eq!(cached_after.rowid, start_rowid - 1 + i64::from(appended));
    }

    #[test]
    fn test_table_insert_sequential_fast_path_records_append_metrics_without_reloads() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        crate::instrumentation::reset_btree_copy_profile();
        crate::instrumentation::set_btree_copy_profile_enabled(true);

        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'G'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1..=128_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        let root_entry = cursor.reload_page_fresh(&cx, root).unwrap();
        assert!(
            root_entry.header.page_type.is_interior(),
            "test requires an interior root so the fast path would otherwise revisit it"
        );

        // See `test_table_insert_reuses_rightmost_leaf_cache_for_multiple_sequential_appends`
        // for the rationale; keep inserting until the rightmost leaf has room
        // for at least 3 more cells so this test's metric assertion is not
        // hostage to split-cadence implementation detail.
        let mut next_rowid = 129_i64;
        while next_rowid < 512 {
            let mut probe_cell = Vec::new();
            cursor
                .encode_table_leaf_cell_into(&cx, next_rowid, &payload, &mut probe_cell)
                .unwrap();
            let cached = cursor
                .rightmost_leaf_cache
                .clone()
                .expect("sequential inserts should seed the rightmost-leaf cache");
            let header_offset = cell::header_offset_for_page(cached.page_no);
            let content_offset = cached.header.content_offset(cursor.usable_size);
            let header_size = usize::from(cached.header.page_type.header_size());
            let cells_needed_for_assert = 3_usize;
            let has_room = content_offset
                .checked_sub(probe_cell.len() * cells_needed_for_assert)
                .is_some_and(|after_three| {
                    let ptr_array_after = header_offset
                        + header_size
                        + (usize::from(cached.header.cell_count) + cells_needed_for_assert) * 2;
                    ptr_array_after <= after_three
                });
            if has_room {
                break;
            }
            cursor.table_insert(&cx, next_rowid, &payload).unwrap();
            next_rowid += 1;
        }

        let before = crate::instrumentation::btree_leaf_reuse_snapshot();
        let mut appended = 0_u64;
        let start_rowid = next_rowid;
        for rowid in start_rowid..start_rowid + 128 {
            let cached = cursor
                .rightmost_leaf_cache
                .clone()
                .expect("sequential inserts should seed the rightmost-leaf cache");
            let mut cell_data = Vec::new();
            cursor
                .encode_table_leaf_cell_into(&cx, rowid, &payload, &mut cell_data)
                .unwrap();
            let header_offset = cell::header_offset_for_page(cached.page_no);
            let content_offset = cached.header.content_offset(cursor.usable_size);
            let Some(new_content_offset) = content_offset.checked_sub(cell_data.len()) else {
                break;
            };
            let ptr_array_end = header_offset
                + usize::from(cached.header.page_type.header_size())
                + (usize::from(cached.header.cell_count) + 1) * 2;
            if ptr_array_end > new_content_offset {
                break;
            }

            cursor.pager.clear_reads();
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            assert!(
                cursor.pager.read_pages().is_empty(),
                "fast sequential append should not re-read the rightmost leaf before the next split (rowid {rowid})"
            );
            appended = appended.saturating_add(1);
        }

        let after = crate::instrumentation::btree_leaf_reuse_snapshot();
        crate::instrumentation::set_btree_copy_profile_enabled(false);

        assert!(
            appended >= 3,
            "test setup should permit multiple zero-read sequential appends"
        );
        let before_fast_appends = before
            .fast_table_leaf_payload_appends
            .saturating_add(before.fast_table_leaf_full_cell_appends);
        let after_fast_appends = after
            .fast_table_leaf_payload_appends
            .saturating_add(after.fast_table_leaf_full_cell_appends);
        assert!(
            after_fast_appends >= before_fast_appends.saturating_add(appended),
            "sequential fast-path appends should be reflected in the append metrics: before={before:?} after={after:?} appended={appended}"
        );
    }

    #[test]
    fn test_table_append_after_last_position_avoids_reseek() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'P'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1..=128_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        assert!(cursor.last(&cx).unwrap());
        cursor.pager.clear_reads();
        cursor
            .table_append_after_last_position(&cx, 129, &payload)
            .unwrap();

        assert!(
            cursor.pager.read_pages().is_empty(),
            "append-after-last should reuse the current right-edge cursor state"
        );
        assert_eq!(cursor.rowid(&cx).unwrap(), 129);
    }

    #[test]
    fn test_table_append_after_last_position_with_writer_writes_payload_directly() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = b"direct-page-payload";
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        cursor.table_insert(&cx, 1, b"seed").unwrap();
        assert!(cursor.last(&cx).unwrap());
        cursor.pager.clear_reads();
        let appended = cursor
            .table_append_after_last_position_with_writer(&cx, 2, payload.len(), |dst| {
                dst.copy_from_slice(payload);
                Ok(())
            })
            .unwrap();

        assert!(
            appended,
            "direct writer should append into the rightmost leaf"
        );
        assert!(
            cursor.pager.read_pages().is_empty(),
            "writer append should reuse the current right-edge cursor state"
        );
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert_eq!(cursor.payload(&cx).unwrap(), payload);
    }

    #[test]
    fn test_table_append_after_last_position_with_writer_error_does_not_publish_cell() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        cursor.table_insert(&cx, 1, b"seed").unwrap();
        assert!(cursor.last(&cx).unwrap());
        let error = cursor
            .table_append_after_last_position_with_writer(&cx, 2, 8, |dst| {
                dst.copy_from_slice(b"partial!");
                Err(FrankenError::internal("forced direct writer failure"))
            })
            .unwrap_err();

        assert!(error.to_string().contains("forced direct writer failure"));
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert_eq!(cursor.payload(&cx).unwrap(), b"seed");
        assert!(
            !cursor.table_move_to(&cx, 2).unwrap().is_found(),
            "failed writer append must not publish a new row"
        );
        assert!(cursor.table_move_to(&cx, 1).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"seed");
    }

    #[test]
    fn test_table_append_after_last_position_reseeks_when_stack_is_empty() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'Q'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1..=128_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        cursor.stack.clear();
        cursor.at_eof = true;
        cursor
            .table_append_after_last_position(&cx, 129, &payload)
            .unwrap();

        assert_eq!(cursor.rowid(&cx).unwrap(), 129);
    }

    #[test]
    fn test_table_append_after_last_position_repeated_after_existing_rows_crosses_split() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'R'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1_i64..=8_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }
        assert!(cursor.last(&cx).unwrap());

        for rowid in 9_i64..=72_i64 {
            cursor
                .table_append_after_last_position(&cx, rowid, &payload)
                .unwrap();
        }

        for rowid in 1_i64..=72_i64 {
            let seek = cursor
                .table_move_to(&cx, rowid)
                .expect("rowid seek should succeed after repeated append");
            assert!(seek.is_found(), "rowid {rowid} should remain reachable");
            assert_eq!(cursor.payload(&cx).unwrap(), payload.as_slice());
        }
    }

    #[test]
    fn test_table_insert_refreshes_rightmost_leaf_cache_after_split() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'S'; 220];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        let mut previous_cached_page = None;
        let split_insert_rowid = loop {
            let next_rowid = cursor.last_insert_rowid.unwrap_or(0) + 1;
            cursor.table_insert(&cx, next_rowid, &payload).unwrap();
            let cached = cursor
                .rightmost_leaf_cache
                .as_ref()
                .expect("sequential append should maintain a rightmost-leaf cache");

            if let Some(previous_page) = previous_cached_page
                && cached.page_no != previous_page
            {
                break next_rowid;
            }

            previous_cached_page = Some(cached.page_no);
            assert!(
                next_rowid < 512,
                "expected a right-edge split that refreshes the cached leaf page"
            );
        };

        let _cached_after_split = cursor
            .rightmost_leaf_cache
            .clone()
            .expect("split should refresh the rightmost-leaf cache");
        cursor.pager.clear_reads();
        cursor
            .table_insert(&cx, split_insert_rowid + 1, &payload)
            .unwrap();
        assert!(
            cursor.pager.read_pages().is_empty(),
            "post-split append should reuse the refreshed cached leaf state"
        );
    }

    #[test]
    fn test_refresh_rightmost_leaf_cache_after_insert_skips_last_when_cache_is_authoritative() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let payload = vec![b'R'; 180];
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in 1..=8_i64 {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        let cached = cursor
            .rightmost_leaf_cache
            .clone()
            .expect("sequential inserts should seed the rightmost-leaf cache");
        cursor.stack.clear();
        cursor.at_eof = true;
        cursor.last_known_depth = Some(cached.tree_depth);
        cursor.rightmost_leaf_cache = Some(cached.clone());

        cursor.pager.clear_reads();
        cursor
            .refresh_rightmost_leaf_cache_after_insert(&cx, cached.rowid)
            .unwrap();

        assert!(
            cursor.pager.read_pages().is_empty(),
            "refresh should trust an already-authoritative rightmost-leaf cache instead of calling last()"
        );
        let refreshed = cursor
            .rightmost_leaf_cache
            .as_ref()
            .expect("refresh should preserve the rightmost-leaf cache");
        assert_eq!(refreshed.page_no, cached.page_no);
        assert_eq!(refreshed.rowid, cached.rowid);
    }

    #[test]
    fn test_table_insert_from_current_position_reuses_leaf_state_without_reload() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in [10_i64, 30, 50] {
            let payload = payload_for_rowid(rowid);
            cursor
                .table_insert(&cx, rowid, payload.as_slice())
                .expect("seed insert should succeed");
        }

        let insert_rowid = 40_i64;
        assert!(
            !cursor
                .table_move_to(&cx, insert_rowid)
                .expect("seek to insertion point should succeed")
                .is_found(),
            "test rowid should target a missing insertion point"
        );

        let before = crate::instrumentation::btree_leaf_reuse_snapshot();
        cursor.pager.clear_reads();
        let payload = payload_for_rowid(insert_rowid);
        cursor
            .table_insert_from_current_position(&cx, insert_rowid, payload.as_slice())
            .expect("no-split insert should succeed");

        let snapshot = crate::instrumentation::btree_leaf_reuse_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            cursor.pager.read_pages().is_empty(),
            "no-split insert should not re-read the current leaf"
        );
        assert!(
            snapshot.no_split_reuse_hits >= before.no_split_reuse_hits.saturating_add(1),
            "the no-split reuse counter should advance for an in-place leaf insert"
        );

        let mut rowids = Vec::new();
        assert!(cursor.first(&cx).expect("scan should start"));
        loop {
            rowids.push(cursor.rowid(&cx).expect("rowid should decode"));
            if !cursor.next(&cx).expect("scan should advance") {
                break;
            }
        }
        assert_eq!(rowids, vec![10, 30, 40, 50]);
    }

    #[test]
    fn test_table_insert_from_current_position_overflow_encode_error_retains_cell_buffer() {
        const SMALL_USABLE: u32 = 256;
        let root = pn(2);
        let inner = Rc::new(RefCell::new(MemPageStore::with_empty_table(
            root,
            SMALL_USABLE,
        )));
        let cx = Cx::new();
        let mut cursor = BtCursor::new(
            FailingOverflowStore::new(inner, usize::MAX),
            root,
            SMALL_USABLE,
            true,
        );
        assert!(
            !cursor
                .table_move_to(&cx, 1)
                .expect("seek to insertion point should succeed")
                .is_found(),
            "test rowid should be absent before insertion"
        );

        cursor.cell_buf = Vec::with_capacity(4096);
        let expected_capacity = cursor.cell_buf.capacity();
        cursor.pager.fail_on_write = cursor.pager.write_count.saturating_add(1);

        let overflow_payload = vec![b'X'; 512];
        let error = cursor
            .table_insert_from_current_position(&cx, 1, &overflow_payload)
            .expect_err("injected overflow write failure should abort insert");

        assert!(
            matches!(&error, FrankenError::Internal(msg) if msg == "injected write failure"),
            "expected injected write failure, got {error:?}"
        );
        assert!(
            cursor.cell_buf.capacity() >= expected_capacity,
            "table current-position encode failure should return the reusable cell buffer"
        );
    }

    #[test]
    fn test_index_insert_from_current_position_reuses_leaf_state_without_reload() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_index(root, USABLE);
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, false);

        for id in [10_i64, 30, 50] {
            let key = synthetic_index_key(id);
            cursor
                .index_insert(&cx, &key)
                .expect("seed index insert should succeed");
        }

        let inserted_id = 40_i64;
        let inserted_key = synthetic_index_key(inserted_id);
        assert!(
            !cursor
                .index_move_to(&cx, &inserted_key)
                .expect("seek to insertion point should succeed")
                .is_found(),
            "inserted key should be missing before the test insert"
        );

        let before = crate::instrumentation::btree_leaf_reuse_snapshot();
        cursor.pager.clear_reads();
        cursor
            .index_insert_from_current_position(&cx, &inserted_key)
            .expect("no-split index insert should succeed");

        let snapshot = crate::instrumentation::btree_leaf_reuse_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            cursor.pager.read_pages().is_empty(),
            "no-split index insert should not re-read the current leaf"
        );
        assert!(
            snapshot.no_split_reuse_hits >= before.no_split_reuse_hits.saturating_add(1),
            "the no-split reuse counter should advance for an in-place index insert"
        );

        let scanned = scan_all_index_keys(&mut cursor, &cx).expect("scan should succeed");
        let mut expected = vec![
            synthetic_index_key(10),
            synthetic_index_key(30),
            synthetic_index_key(40),
            synthetic_index_key(50),
        ];
        expected.sort_by(|lhs, rhs| compare_index_test_keys(&cursor, lhs, rhs));
        assert_eq!(scanned, expected);
    }

    #[test]
    fn test_index_insert_from_current_position_overflow_encode_error_retains_cell_buffer() {
        const SMALL_USABLE: u32 = 256;
        let root = pn(2);
        let inner = Rc::new(RefCell::new(MemPageStore::with_empty_index(
            root,
            SMALL_USABLE,
        )));
        let cx = Cx::new();
        let mut cursor = BtCursor::new(
            FailingOverflowStore::new(inner, usize::MAX),
            root,
            SMALL_USABLE,
            false,
        );
        assert!(
            !cursor
                .index_move_to(&cx, b"probe")
                .expect("seek to insertion point should succeed")
                .is_found(),
            "test key should be absent before insertion"
        );

        cursor.cell_buf = Vec::with_capacity(4096);
        let expected_capacity = cursor.cell_buf.capacity();
        cursor.pager.fail_on_write = cursor.pager.write_count.saturating_add(1);

        let overflow_key = vec![b'K'; 512];
        let error = cursor
            .index_insert_from_current_position(&cx, &overflow_key)
            .expect_err(
                "injected overflow write failure should abort current-position index insert",
            );

        assert!(
            matches!(&error, FrankenError::Internal(msg) if msg == "injected write failure"),
            "expected injected write failure, got {error:?}"
        );
        assert!(
            cursor.cell_buf.capacity() >= expected_capacity,
            "index current-position encode failure should return the reusable cell buffer"
        );
    }

    #[test]
    fn test_index_insert_overflow_encode_error_retains_cell_buffer() {
        const SMALL_USABLE: u32 = 256;
        let root = pn(2);
        let inner = Rc::new(RefCell::new(MemPageStore::with_empty_index(
            root,
            SMALL_USABLE,
        )));
        let cx = Cx::new();
        let mut cursor = BtCursor::new(
            FailingOverflowStore::new(inner, usize::MAX),
            root,
            SMALL_USABLE,
            false,
        );

        cursor.cell_buf = Vec::with_capacity(4096);
        let expected_capacity = cursor.cell_buf.capacity();
        cursor.pager.fail_on_write = cursor.pager.write_count.saturating_add(1);

        let overflow_key = vec![b'K'; 512];
        let error = cursor
            .index_insert(&cx, &overflow_key)
            .expect_err("injected overflow write failure should abort generic index insert");

        assert!(
            matches!(&error, FrankenError::Internal(msg) if msg == "injected write failure"),
            "expected injected write failure, got {error:?}"
        );
        assert!(
            cursor.cell_buf.capacity() >= expected_capacity,
            "generic index encode failure should return the reusable cell buffer"
        );
    }

    #[test]
    fn test_table_insert_from_current_position_after_delete_reuses_leaf_state() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, USABLE, true);

        for rowid in [10_i64, 20, 40] {
            let payload = payload_for_rowid(rowid);
            cursor
                .table_insert(&cx, rowid, payload.as_slice())
                .expect("seed insert should succeed");
        }

        assert!(
            cursor
                .table_move_to(&cx, 20)
                .expect("seek before delete should succeed")
                .is_found(),
            "seed row must exist before delete"
        );
        cursor.delete(&cx).expect("delete should succeed");

        let insert_rowid = 30_i64;
        assert!(
            !cursor
                .table_move_to(&cx, insert_rowid)
                .expect("seek to insertion point should succeed")
                .is_found(),
            "deleted-gap rowid should be missing before reinsertion"
        );

        let before = crate::instrumentation::btree_leaf_reuse_snapshot();
        cursor.pager.clear_reads();
        let payload = payload_for_rowid(insert_rowid);
        cursor
            .table_insert_from_current_position(&cx, insert_rowid, payload.as_slice())
            .expect("insert after delete should reuse the retained leaf state");

        let snapshot = crate::instrumentation::btree_leaf_reuse_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            cursor.pager.read_pages().is_empty(),
            "insert-after-delete should not force a leaf reload on the retained leaf"
        );
        assert!(
            snapshot.no_split_reuse_hits >= before.no_split_reuse_hits.saturating_add(1),
            "insert-after-delete should still count as an in-place leaf reuse"
        );

        let mut rowids = Vec::new();
        assert!(cursor.first(&cx).expect("scan should start"));
        loop {
            rowids.push(cursor.rowid(&cx).expect("rowid should decode"));
            if !cursor.next(&cx).expect("scan should advance") {
                break;
            }
        }
        assert_eq!(rowids, vec![10, 30, 40]);
    }

    #[test]
    fn test_table_insert_from_current_position_records_fallback_when_balance_needed() {
        let _guard = LEAF_REUSE_CURSOR_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _shared_guard = crate::instrumentation::LEAF_REUSE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _gate_guard = crate::instrumentation::BTREE_METRICS_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_btree_metrics_enabled(true);
        const SMALL_USABLE: u32 = 256;

        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let mut cursor = BtCursor::new(SeekProbeStore::new(store), root, SMALL_USABLE, true);
        let payload = vec![b'F'; 120];

        cursor
            .table_insert(&cx, 10, &payload)
            .expect("first insert should succeed");
        cursor
            .table_insert(&cx, 30, &payload)
            .expect("second insert should succeed");

        assert!(
            !cursor
                .table_move_to(&cx, 20)
                .expect("seek to insertion point should succeed")
                .is_found(),
            "middle rowid should be absent before insert"
        );

        let mut cell_data = Vec::new();
        cursor
            .encode_table_leaf_cell_into(&cx, 20, &payload, &mut cell_data)
            .expect("cell encoding should succeed");
        let top = cursor
            .stack
            .last()
            .expect("seek should leave the leaf on stack");
        let content_offset = top.header.content_offset(cursor.usable_size);
        let would_fit = content_offset
            .checked_sub(cell_data.len())
            .is_some_and(|new_offset| {
                let ptr_array_end = cell::header_offset_for_page(top.page_no)
                    + usize::from(top.header.page_type.header_size())
                    + (top.cell_pointers.len() + 1) * 2;
                ptr_array_end <= new_offset
            });
        assert!(
            !would_fit,
            "test setup must force the balance/reload fallback path"
        );

        let before = crate::instrumentation::btree_leaf_reuse_snapshot();
        cursor.pager.clear_reads();
        cursor
            .table_insert_from_current_position(&cx, 20, &payload)
            .expect("fallback insert should still succeed via balance");

        let snapshot = crate::instrumentation::btree_leaf_reuse_snapshot();
        set_btree_metrics_enabled(false);
        assert!(
            snapshot.conservative_reload_fallbacks
                >= before.conservative_reload_fallbacks.saturating_add(1),
            "the fallback counter should advance when the insert must rebalance"
        );
        assert!(
            validate_table_tree_invariants(&cursor.pager, root, SMALL_USABLE).is_ok(),
            "balance fallback must preserve table invariants"
        );
    }

    #[test]
    fn test_btree_insert_10k_random_keys() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let mut insertion_order: Vec<i64> = (1_i64..=10_000_i64).collect();
        deterministic_shuffle(&mut insertion_order, 0x000D_EADB);

        for rowid in insertion_order {
            let payload = payload_for_rowid(rowid);
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        for rowid in 1_i64..=10_000_i64 {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "missing rowid {rowid} after insert");
            assert_eq!(cursor.rowid(&cx).unwrap(), rowid);
            assert_eq!(cursor.payload(&cx).unwrap(), payload_for_rowid(rowid));
        }
    }

    #[test]
    fn test_rowid_and_payload_cow_reads_local_leaf_table_payload() {
        let payload = b"small local payload";
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[(7, payload)]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        assert!(cursor.table_move_to(&cx, 7).unwrap().is_found());

        let (rowid, got) = cursor.rowid_and_payload_cow(&cx).unwrap();
        assert_eq!(rowid, 7);
        assert_eq!(got.as_ref(), payload);
        assert!(
            matches!(got, Cow::Borrowed(_)),
            "local leaf-table payloads should stay borrowed"
        );
        assert!(cursor.cell_slot_cache.borrow().entries.is_empty());
        assert_eq!(cursor.payload(&cx).unwrap(), payload);
        assert!(
            cursor.cell_slot_cache.borrow().entries.is_empty(),
            "payload() should read local leaf-table cells without populating the cell-slot cache"
        );

        let mut prefix = Vec::new();
        cursor
            .payload_prefix_into(&cx, 5, &mut prefix)
            .expect("local leaf prefix read should succeed");
        assert_eq!(prefix, b"small");
    }

    #[test]
    fn test_rowid_reads_table_leaf_without_cell_slot_parse() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_table(&[(7, b"seven"), (9, b"nine")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        assert!(cursor.table_move_to(&cx, 9).unwrap().is_found());
        assert!(cursor.cell_slot_cache.borrow().entries.is_empty());

        assert_eq!(cursor.rowid(&cx).unwrap(), 9);
        assert!(cursor.cell_slot_cache.borrow().entries.is_empty());
    }

    #[test]
    fn test_btree_depth_4_cursor_traversal() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 100)], pn(7)));
        store
            .pages
            .insert(3, build_interior_table(&[(pn(4), 50)], pn(8)));
        store
            .pages
            .insert(4, build_interior_table(&[(pn(5), 25)], pn(6)));
        store
            .pages
            .insert(5, build_leaf_table(&[(10, b"ten"), (20, b"twenty")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(30, b"thirty"), (40, b"forty")]));
        store
            .pages
            .insert(8, build_leaf_table(&[(60, b"sixty"), (80, b"eighty")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(120, b"one20"), (140, b"one40")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let depth = measure_tree_depth(&cursor.pager, pn(2), USABLE);
        assert_eq!(depth, 4, "expected a manually seeded depth-4 tree");

        let expected_rowids = [10_i64, 20, 30, 40, 60, 80, 120, 140];
        for rowid in expected_rowids {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "missing rowid {rowid} in depth-4 tree");
        }

        assert!(cursor.first(&cx).unwrap());
        let mut scanned = vec![cursor.rowid(&cx).unwrap()];
        while cursor.next(&cx).unwrap() {
            scanned.push(cursor.rowid(&cx).unwrap());
        }
        assert_eq!(scanned, expected_rowids);
    }

    #[test]
    fn test_table_advance_to_reuses_local_and_sibling_leaf_before_full_seek() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 15)], pn(4)));

        store
            .pages
            .insert(3, build_interior_table(&[(pn(5), 3), (pn(6), 8)], pn(7)));
        store
            .pages
            .insert(4, build_interior_table(&[(pn(8), 25)], pn(9)));

        store
            .pages
            .insert(5, build_leaf_table(&[(1, b"L1"), (3, b"L3")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(5, b"L5"), (8, b"L8")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(10, b"L10"), (15, b"L15")]));
        store
            .pages
            .insert(8, build_leaf_table(&[(20, b"L20"), (25, b"L25")]));
        store
            .pages
            .insert(9, build_leaf_table(&[(30, b"L30"), (40, b"L40")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(cursor.table_move_to(&cx, 10).unwrap().is_found());
        let same_leaf = cursor.current_page().unwrap();
        let same_leaf_seek = cursor.advance_to(&cx, 12).unwrap();
        assert!(!same_leaf_seek.is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
        assert_eq!(cursor.current_page().unwrap(), same_leaf);

        assert!(cursor.table_move_to(&cx, 8).unwrap().is_found());
        let left_leaf = cursor.current_page().unwrap();
        let sibling_seek = cursor.advance_to(&cx, 10).unwrap();
        assert!(sibling_seek.is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert_ne!(cursor.current_page().unwrap(), left_leaf);

        assert!(cursor.table_move_to(&cx, 8).unwrap().is_found());
        let fallback_seek = cursor.advance_to(&cx, 30).unwrap();
        assert!(fallback_seek.is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 30);
        assert_eq!(cursor.current_page().unwrap(), pn(9));
        assert!(
            cursor
                .witness_keys()
                .iter()
                .all(|key| matches!(key, WitnessKey::Cell { .. })),
            "advance_to must remain a point probe and avoid page witnesses"
        );
    }

    #[test]
    fn test_point_read_uses_cell_witness() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let result = cursor.table_move_to(&cx, 5).unwrap();
        assert!(result.is_found());
        assert_eq!(cursor.witness_keys().len(), 1);
        assert!(matches!(cursor.witness_keys()[0], WitnessKey::Cell { .. }));
    }

    #[test]
    fn test_btree_page_reads_publish_precise_witness_without_coarse_page_read() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let probe = WitnessProbeStore::new(store);
        let state = probe.state();
        let mut cursor = BtCursor::new(probe, pn(2), USABLE, true);

        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());

        let state = state.borrow();
        assert_eq!(
            state.coarse_page_data_reads, 0,
            "B-tree traversal must not register coarse page reads before the cursor emits its logical witness"
        );
        assert!(
            state.btree_page_data_reads > 0,
            "cursor traversal should still read B-tree pages through the dedicated path"
        );
        assert_eq!(state.read_witnesses.as_slice(), cursor.witness_keys());
        assert!(
            state
                .read_witnesses
                .iter()
                .all(|key| matches!(key, WitnessKey::Cell { .. })),
            "point lookup should publish only cell witnesses"
        );
    }

    #[test]
    fn test_read_witness_cap_throttles_per_cursor_vec_but_preserves_pager_evidence() {
        // Regression test for frankensqlite#92 / coding_agent_session_search#252:
        // a multi-page range scan must not balloon the per-cursor witness vec
        // when an operator has explicitly opted into a cap, but the canonical
        // pager-level SSI evidence must still see every read.
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let probe = WitnessProbeStore::new(store);
        let state = probe.state();
        let mut cursor = BtCursor::new(probe, pn(2), USABLE, true);
        // Cap at 1: COUNT visits 3 B-tree pages → expect cap to throttle after 1.
        cursor.set_read_witness_cap(1);
        assert_eq!(cursor.read_witness_cap(), 1);

        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 4);

        // Per-cursor witness vec respects the cap: at most 1 entry.
        assert!(
            cursor.witness_keys().len() <= 1,
            "read_witness_cap=1 should throttle per-cursor vec; got {} entries",
            cursor.witness_keys().len()
        );

        // Pager-level SSI evidence is unaffected — every B-tree page read still
        // got a witness recorded into the pager.
        let state = state.borrow();
        assert!(
            [pn(2), pn(3), pn(4)]
                .into_iter()
                .all(|page| state.read_witnesses.contains(&WitnessKey::Page(page))),
            "pager-level evidence must record every page read regardless of per-cursor cap"
        );
    }

    #[test]
    fn test_read_witness_cap_zero_is_unbounded_default() {
        // Default cap = 0 means "unbounded": the historical behavior must be
        // preserved for callers that rely on full per-cursor witness lists.
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        assert_eq!(
            cursor.read_witness_cap(),
            0,
            "default cap should be 0 (unbounded) when env var is unset"
        );
        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 4);
        assert!(
            cursor.witness_keys().len() >= 3,
            "unbounded default must keep all page witnesses; got {}",
            cursor.witness_keys().len()
        );
    }

    #[test]
    fn test_clear_witness_keys_resets_warn_flag_and_keeps_cap_active() {
        // `clear_witness_keys` must reset the one-shot warn flag so the next
        // descent that hits the cap can re-emit the policy-hit warning, and it
        // must NOT silently drop the cap (operators that clear witnesses
        // between queries still want the throttle to apply).
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        cursor.set_read_witness_cap(1);

        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 4);
        let first_len = cursor.witness_keys().len();
        assert!(first_len <= 1);
        // Cap fired at least once during the descent → warn flag is now set.
        assert!(
            cursor.read_witness_cap_warned,
            "warn flag should be set after cap throttles"
        );

        cursor.clear_witness_keys();
        assert!(cursor.witness_keys().is_empty());
        assert!(
            !cursor.read_witness_cap_warned,
            "clear_witness_keys must reset the one-shot warn flag"
        );
        // Cap is still active after clear.
        assert_eq!(cursor.read_witness_cap(), 1);
    }

    #[test]
    fn test_read_witness_cap_does_not_break_range_witness_dedup_to_pager() {
        // Regression test: enabling `read_witness_cap` must not silently break
        // the adjacent-duplicate dedup in `record_range_page_witness`.
        //
        // Pre-fix, dedup keyed off `read_witnesses.last()` — once the cap
        // throttled the per-cursor vec, `.last()` returned a stale entry, so
        // calling `record_range_page_witness(N)` twice in a row leaked TWO
        // pager-level witness pushes (vs ONE without the cap). That would
        // *increase* pager memory pressure under the very flag intended to
        // reduce per-cursor memory.
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let probe = WitnessProbeStore::new(store);
        let state = probe.state();
        let mut cursor = BtCursor::new(probe, pn(2), USABLE, true);
        cursor.set_read_witness_cap(1);

        // Drive the bug: call `record_range_page_witness` for the same page
        // many times in a row. Pre-fix this would push N entries into the
        // pager; with the dedup fix, only 1 push lands.
        let target = pn(7);
        for _ in 0..10 {
            cursor.record_range_page_witness(&cx, target);
        }

        let state = state.borrow();
        let target_pushes = state
            .read_witnesses
            .iter()
            .filter(|key| matches!(key, WitnessKey::Page(p) if *p == target))
            .count();
        assert_eq!(
            target_pushes, 1,
            "with cap active, adjacent identical range witnesses must still dedup at the pager; \
             got {target_pushes} pager pushes (pre-fix bug would show 10)"
        );
    }

    #[test]
    fn test_count_all_rows_publishes_page_witnesses_without_coarse_page_read() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let probe = WitnessProbeStore::new(store);
        let state = probe.state();
        let mut cursor = BtCursor::new(probe, pn(2), USABLE, true);

        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 4);

        let state = state.borrow();
        assert_eq!(
            state.coarse_page_data_reads, 0,
            "COUNT traversal must not fall back to coarse raw page reads"
        );
        assert_eq!(state.btree_page_data_reads, 3);
        assert!(
            [pn(2), pn(3), pn(4)]
                .into_iter()
                .all(|page| state.read_witnesses.contains(&WitnessKey::Page(page))),
            "COUNT traversal must publish page witnesses for every counted B-tree page"
        );
        assert!(
            state
                .read_witnesses
                .iter()
                .all(|key| matches!(key, WitnessKey::Page(_))),
            "COUNT traversal should publish page witnesses rather than point-cell witnesses"
        );
    }

    #[test]
    fn test_descent_pages_not_witnessed() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (2, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let result = cursor.table_move_to(&cx, 10).unwrap();
        assert!(result.is_found());
        assert!(
            cursor
                .witness_keys()
                .iter()
                .all(|key| matches!(key, WitnessKey::Cell { .. }))
        );
    }

    #[test]
    fn test_negative_read_uses_cell_witness() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"one"), (5, b"five"), (10, b"ten")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let result = cursor.table_move_to(&cx, 7).unwrap();
        assert!(!result.is_found());
        assert_eq!(cursor.witness_keys().len(), 1);
        assert!(matches!(cursor.witness_keys()[0], WitnessKey::Cell { .. }));
    }

    #[test]
    fn test_range_scan_uses_page_witness() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());
        assert!(
            cursor
                .witness_keys()
                .iter()
                .any(|key| matches!(key, WitnessKey::Page(_)))
        );
    }

    #[test]
    fn test_range_page_witness_dedups_consecutive_leaf() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_table(&[(1, b"a"), (5, b"b"), (10, b"c")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());
        assert!(cursor.next(&cx).unwrap());

        let page_witnesses = cursor
            .witness_keys()
            .iter()
            .filter(|key| matches!(key, WitnessKey::Page(page) if *page == pn(2)))
            .count();
        assert_eq!(
            page_witnesses, 1,
            "one page witness is enough for repeated range steps on the same leaf"
        );
    }

    #[test]
    fn test_page_only_witnesses_collapse_merge() {
        use std::collections::HashSet;

        let root = pn(2);
        let same_leaf = pn(4);

        let txn1_cell = HashSet::from([WitnessKey::Cell {
            btree_root: root,
            leaf_page: same_leaf,
            tag: BtCursor::<MemPageStore>::cell_tag_from_rowid(10),
        }]);
        let txn2_cell = HashSet::from([WitnessKey::Cell {
            btree_root: root,
            leaf_page: same_leaf,
            tag: BtCursor::<MemPageStore>::cell_tag_from_rowid(11),
        }]);
        assert!(
            txn1_cell.is_disjoint(&txn2_cell),
            "cell witnesses preserve independent point operations"
        );

        let txn1_page = HashSet::from([WitnessKey::Page(same_leaf)]);
        let txn2_page = HashSet::from([WitnessKey::Page(same_leaf)]);
        assert!(
            !txn1_page.is_disjoint(&txn2_page),
            "page-only witnesses over-approximate and force conflicts"
        );
    }

    #[test]
    fn test_e2e_point_ops_use_cell_witnesses() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 50)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(10, b"a"), (50, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(60, b"c"), (90, b"d")]));

        let cx = Cx::new();
        let mut point_cursor = BtCursor::new(store.clone(), pn(2), USABLE, true);
        let mut range_cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Point workload: one hit and one miss.
        assert!(point_cursor.table_move_to(&cx, 60).unwrap().is_found());
        assert!(!point_cursor.table_move_to(&cx, 61).unwrap().is_found());
        assert!(
            point_cursor
                .witness_keys()
                .iter()
                .all(|key| matches!(key, WitnessKey::Cell { .. })),
            "point operations must not emit page-level witnesses"
        );

        // Range workload: traversal witnesses leaves at page granularity.
        assert!(range_cursor.first(&cx).unwrap());
        assert!(range_cursor.next(&cx).unwrap());
        assert!(range_cursor.next(&cx).unwrap());
        assert!(
            range_cursor
                .witness_keys()
                .iter()
                .any(|key| matches!(key, WitnessKey::Page(_))),
            "range operations may emit page-level witnesses"
        );
    }

    #[test]
    fn test_cursor_next_prev() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(
            2,
            build_leaf_table(&[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Forward.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 4);
        assert!(!cursor.next(&cx).unwrap());
        assert!(cursor.eof());

        // Backward.
        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 4);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 3);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(!cursor.prev(&cx).unwrap());
    }

    #[test]
    fn test_cursor_empty_tree() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(!cursor.first(&cx).unwrap());
        assert!(cursor.eof());
    }

    // -- Two-level tree tests --

    #[test]
    fn test_cursor_two_level_tree_seek() {
        // Build a two-level tree:
        //   Interior page 2: children=[left=3, rowid=5], right=4
        //   Leaf page 3: (1, "a"), (5, "b")     — rowids <= 5
        //   Leaf page 4: (10, "c"), (15, "d")    — rowids > 5
        //
        // In SQLite intkey trees, the interior cell key is the max rowid
        // in the left subtree.
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Seek exact matches.
        assert!(cursor.table_move_to(&cx, 1).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"a");

        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"b");

        assert!(cursor.table_move_to(&cx, 10).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"c");

        assert!(cursor.table_move_to(&cx, 15).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"d");
    }

    #[test]
    fn test_table_seek_fails_closed_when_successor_contains_missed_rowid() {
        // Mirrors frankensqlite#73: a stale interior separator routes the
        // equality seek into the left child, while a forward scan reaches the
        // same rowid in the successor leaf. This must not silently return
        // NotFound, because UPDATE/DELETE callers would no-op a scan-visible row.
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (4, b"d")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(5, b"e"), (10, b"j")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut scanned = Vec::new();
        assert!(cursor.first(&cx).unwrap());
        loop {
            scanned.push(cursor.rowid(&cx).unwrap());
            if !cursor.next(&cx).unwrap() {
                break;
            }
        }
        assert_eq!(scanned, vec![1, 4, 5, 10]);

        let err = cursor
            .table_move_to(&cx, 5)
            .expect_err("stale separator must fail closed");
        assert!(
            matches!(
                err,
                FrankenError::DatabaseCorrupt { ref detail }
                    if detail.contains("missed scan-visible rowid 5")
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_cursor_two_level_tree_traverse() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (5, b"b")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"c"), (15, b"d")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Forward traversal: 1, 5, 10, 15.
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
        assert!(!cursor.next(&cx).unwrap());
        assert!(cursor.eof());

        // Backward traversal: 15, 10, 5, 1.
        assert!(cursor.last(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(cursor.prev(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(!cursor.prev(&cx).unwrap());
    }

    // -- Three-level tree test --

    #[test]
    fn test_cursor_three_level_tree() {
        // Build a three-level tree. Interior cell keys are the max rowid
        // in their left subtree.
        //
        //   Root (page 2): interior, children=[(3, 15)], right=4
        //     → left subtree (page 3) has rowids <= 15
        //     → right subtree (page 4) has rowids > 15
        //
        //   Page 3: interior, children=[(5, 3), (6, 8)], right=7
        //     → page 5 has rowids <= 3
        //     → page 6 has rowids in (3, 8]
        //     → page 7 has rowids in (8, 15]
        //
        //   Page 4: interior, children=[(8, 25)], right=9
        //     → page 8 has rowids in (15, 25]
        //     → page 9 has rowids > 25
        //
        //   Leaf pages:
        //     5: (1, 3)  6: (5, 8)  7: (10, 15)  8: (20, 25)  9: (30, 40)
        let mut store = MemPageStore::new(USABLE);

        // Root.
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 15)], pn(4)));

        // Interior pages.
        store
            .pages
            .insert(3, build_interior_table(&[(pn(5), 3), (pn(6), 8)], pn(7)));
        store
            .pages
            .insert(4, build_interior_table(&[(pn(8), 25)], pn(9)));

        // Leaves.
        store
            .pages
            .insert(5, build_leaf_table(&[(1, b"L1"), (3, b"L3")]));
        store
            .pages
            .insert(6, build_leaf_table(&[(5, b"L5"), (8, b"L8")]));
        store
            .pages
            .insert(7, build_leaf_table(&[(10, b"L10"), (15, b"L15")]));
        store
            .pages
            .insert(8, build_leaf_table(&[(20, b"L20"), (25, b"L25")]));
        store
            .pages
            .insert(9, build_leaf_table(&[(30, b"L30"), (40, b"L40")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Full forward scan.
        let mut rowids = Vec::new();
        assert!(cursor.first(&cx).unwrap());
        loop {
            rowids.push(cursor.rowid(&cx).unwrap());
            if !cursor.next(&cx).unwrap() {
                break;
            }
        }
        assert_eq!(rowids, vec![1, 3, 5, 8, 10, 15, 20, 25, 30, 40]);

        // Full backward scan.
        let mut rowids_rev = Vec::new();
        assert!(cursor.last(&cx).unwrap());
        loop {
            rowids_rev.push(cursor.rowid(&cx).unwrap());
            if !cursor.prev(&cx).unwrap() {
                break;
            }
        }
        assert_eq!(rowids_rev, vec![40, 30, 25, 20, 15, 10, 8, 5, 3, 1]);

        // Seek tests.
        assert!(cursor.table_move_to(&cx, 8).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"L8");

        assert!(cursor.table_move_to(&cx, 25).unwrap().is_found());
        assert_eq!(cursor.payload(&cx).unwrap(), b"L25");

        // Seek not found: 12 → should land on 15.
        let r = cursor.table_move_to(&cx, 12).unwrap();
        assert!(!r.is_found());
        assert_eq!(cursor.rowid(&cx).unwrap(), 15);
    }

    #[test]
    fn test_cursor_seek_then_next() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"ten"), (20, b"twenty")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Seek to 5, then next should give 10.
        assert!(cursor.table_move_to(&cx, 5).unwrap().is_found());
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 10);
    }

    #[test]
    fn test_cursor_next_skips_empty_table_child_subtree_without_restarting_root() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5), (pn(4), 10)], pn(5)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store.pages.insert(4, build_leaf_table(&[]));
        store.pages.insert(
            5,
            build_leaf_table(&[(20, b"twenty"), (25, b"twenty-five")]),
        );

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(
            cursor.rowid(&cx).unwrap(),
            20,
            "next() should skip the empty middle child subtree and continue forward"
        );
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 25);
        assert!(!cursor.next(&cx).unwrap());
    }

    #[test]
    fn test_cursor_next_handles_empty_rightmost_table_child_subtree() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 5)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store.pages.insert(4, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);
        assert!(cursor.next(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 5);
        assert!(
            !cursor.next(&cx).unwrap(),
            "advancing past an empty rightmost child subtree should cleanly reach EOF"
        );
    }

    #[test]
    fn test_cursor_eof_at_payload() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[(1, b"x")]));

        let cx = Cx::new();
        let cursor = BtCursor::new(store, pn(2), USABLE, true);

        // Before first/last, cursor is at EOF.
        assert!(cursor.eof());
        assert!(cursor.payload(&cx).is_err());
        assert!(cursor.rowid(&cx).is_err());
    }

    /// Regression test for bd-14lx: sustained deletes that drain all leaf
    /// pages under one interior subtree must collapse the tree from depth 3
    /// back to depth 2 (or even depth 1).
    ///
    /// The test inserts enough data to force a depth-3 tree (root →
    /// interior → leaf), then deletes every row.  After all deletes the
    /// tree must have collapsed — either to a single leaf root page, or
    /// at least from depth 3 to a shallower structure that passes
    /// first()/next() enumeration correctly.
    #[test]
    fn test_depth3_collapse_after_sustained_deletes() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Use large payloads (~1400 bytes) so that fewer cells per leaf
        // forces deeper trees sooner.
        let mut max_rowid = 0i64;
        let mut reached_depth_3 = false;

        for rowid in 1..=2000_i64 {
            let payload = vec![b'D'; 1400];
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            max_rowid = rowid;

            // Check tree depth by descending from root.
            let depth = measure_tree_depth(&cursor.pager, pn(2), USABLE);
            if depth >= 3 {
                reached_depth_3 = true;
                break;
            }
        }

        assert!(
            reached_depth_3,
            "failed to build depth-3 tree (reached rowid {max_rowid})"
        );

        // Delete every row.
        for rowid in 1..=max_rowid {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        // After deleting everything, the tree must be empty.
        assert!(
            !cursor.first(&cx).unwrap(),
            "tree should be empty after total delete"
        );
        assert!(cursor.eof());

        // The root page should have collapsed to a leaf (depth 1).
        let root_data = cursor.pager.read_page(&cx, pn(2)).unwrap();
        let root_header = BtreePageHeader::parse(&root_data, 0).unwrap();
        assert!(
            root_header.page_type.is_leaf(),
            "root should collapse to leaf after all rows deleted, got {:?}",
            root_header.page_type
        );
        assert_eq!(root_header.cell_count, 0);
    }

    /// Variant of the depth-3 collapse test that deletes only *some* rows,
    /// enough to drain one interior subtree, and verifies the remaining
    /// rows are still correctly enumerable.
    #[test]
    fn test_depth3_partial_delete_collapse() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let mut max_rowid = 0i64;
        for rowid in 1..=2000_i64 {
            let payload = vec![b'P'; 1400];
            cursor.table_insert(&cx, rowid, &payload).unwrap();
            max_rowid = rowid;

            let depth = measure_tree_depth(&cursor.pager, pn(2), USABLE);
            if depth >= 3 {
                break;
            }
        }

        // Delete the first half of rows.
        let half = max_rowid / 2;
        for rowid in 1..=half {
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} should exist before delete");
            cursor.delete(&cx).unwrap();
        }

        // Verify the remaining rows are intact.
        let mut seen = 0usize;
        if cursor.first(&cx).unwrap() {
            let mut prev = i64::MIN;
            loop {
                let rowid = cursor.rowid(&cx).unwrap();
                assert!(rowid > prev, "out-of-order rowid {rowid} after {prev}");
                assert!(rowid > half, "deleted rowid {rowid} still present");
                prev = rowid;
                seen += 1;
                if !cursor.next(&cx).unwrap() {
                    break;
                }
            }
        }
        assert_eq!(
            seen,
            usize::try_from(max_rowid - half).unwrap(),
            "wrong number of surviving rows"
        );
    }

    /// Measure tree depth by descending from the root following the
    /// leftmost child at each interior level.
    fn measure_tree_depth<P: PageReader>(pager: &P, root: PageNumber, _usable: u32) -> usize {
        let cx = Cx::new();
        let mut pgno = root;
        let mut depth = 1;
        loop {
            let data = pager.read_page(&cx, pgno).unwrap();
            let offset = cell::header_offset_for_page(pgno);
            let header = cell::BtreePageHeader::parse(&data, offset).unwrap();
            if header.page_type.is_leaf() {
                return depth;
            }
            // Descend into the leftmost child.
            let ptrs = cell::read_cell_pointers(&data, &header, offset).unwrap();
            if ptrs.is_empty() {
                // Interior page with 0 cells — use right_child.
                pgno = header.right_child.unwrap();
            } else {
                // First cell's left-child pointer (first 4 bytes of cell).
                let cell_offset = ptrs[0] as usize;
                let raw = u32::from_be_bytes([
                    data[cell_offset],
                    data[cell_offset + 1],
                    data[cell_offset + 2],
                    data[cell_offset + 3],
                ]);
                let left = PageNumber::new(raw).unwrap();
                pgno = left;
            }
            depth += 1;
        }
    }

    /// Phase 3 acceptance: large overflow payloads are stored and retrieved
    /// correctly across multiple overflow pages.
    #[test]
    fn test_btree_multiple_overflow_pages() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Insert 10 rows with payloads between 5000 and 10000 bytes,
        // each requiring multiple overflow pages (page usable = 4096).
        let payloads: Vec<Vec<u8>> = (0..10)
            .map(|i| vec![b'A' + (i as u8 % 26); 5000 + i * 500])
            .collect();

        for (i, payload) in payloads.iter().enumerate() {
            let rowid = i64::try_from(i + 1).unwrap();
            cursor.table_insert(&cx, rowid, payload).unwrap();
        }

        // Verify every row round-trips exactly.
        for (i, expected) in payloads.iter().enumerate() {
            let rowid = i64::try_from(i + 1).unwrap();
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "rowid {rowid} not found");
            let got = cursor.payload(&cx).unwrap();
            assert_eq!(
                got.len(),
                expected.len(),
                "payload length mismatch at rowid {rowid}"
            );
            assert_eq!(&got[..], &expected[..], "payload mismatch at rowid {rowid}");
        }
    }

    #[test]
    fn test_btree_overflow_page_chain_100kb() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        let payload = vec![0xCD_u8; 100 * 1024];

        cursor.table_insert(&cx, 1, &payload).unwrap();
        let seek = cursor.table_move_to(&cx, 1).unwrap();
        assert!(seek.is_found(), "expected rowid 1 to be present");

        let roundtrip = cursor.payload(&cx).unwrap();
        assert_eq!(roundtrip.len(), payload.len());
        assert_eq!(roundtrip, payload);
    }

    /// Phase 3 acceptance: page count must grow as rows are inserted
    /// (proving page splits occur), and sorted order is maintained.
    #[test]
    fn test_btree_page_count_grows_with_inserts() {
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        // Insert 500 rows with ~200 byte payloads to force multiple splits.
        for i in 1..=500_i64 {
            let payload = format!("row-{i:05}-payload-data-{}", "X".repeat(180));
            cursor.table_insert(&cx, i, payload.as_bytes()).unwrap();
        }

        // The tree must have split into multiple levels.
        let depth = measure_tree_depth(&cursor.pager, pn(2), USABLE);
        assert!(
            depth > 1,
            "expected tree depth > 1 after 500 inserts, got {depth}"
        );

        // Full forward scan must yield 500 rows in sorted order.
        assert!(cursor.first(&cx).unwrap());
        let mut count = 1u32;
        let mut prev = cursor.rowid(&cx).unwrap();
        while cursor.next(&cx).unwrap() {
            let current = cursor.rowid(&cx).unwrap();
            assert!(current > prev, "sort violation: {current} followed {prev}");
            prev = current;
            count += 1;
        }
        assert_eq!(count, 500, "expected 500 rows, saw {count}");
    }

    proptest::proptest! {
        /// Property: after arbitrary insert/delete sequences the B-tree
        /// always maintains sorted rowid order when scanned.
        #[test]
        fn prop_btree_order_invariant(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    (1..=5000_i64, proptest::collection::vec(proptest::num::u8::ANY, 10..200))
                        .prop_map(|(r, p)| (true, r, p)),
                    (1..=5000_i64,).prop_map(|(r,)| (false, r, Vec::new())),
                ],
                1..200
            )
        ) {
            let mut store = MemPageStore::new(USABLE);
            store.pages.insert(2, build_leaf_table(&[]));

            let cx = Cx::new();
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            let mut live: BTreeSet<i64> = BTreeSet::new();

            for (is_insert, rowid, payload) in &ops {
                if *is_insert && !live.contains(rowid) {
                    cursor.table_insert(&cx, *rowid, payload).unwrap();
                    live.insert(*rowid);
                } else if !*is_insert && live.contains(rowid) {
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        live.remove(rowid);
                    }
                }
            }

            // Verify sorted order and correct count.
            let mut scanned = Vec::new();
            if cursor.first(&cx).unwrap() {
                loop {
                    scanned.push(cursor.rowid(&cx).unwrap());
                    if !cursor.next(&cx).unwrap() {
                        break;
                    }
                }
            }

            // Rowids must be strictly ascending.
            for window in scanned.windows(2) {
                proptest::prop_assert!(
                    window[0] < window[1],
                    "sort violation: {} >= {}",
                    window[0],
                    window[1]
                );
            }
            proptest::prop_assert_eq!(scanned.len(), live.len());
        }

        /// bd-2sm1: B-tree order matches BTreeMap reference after random ops.
        /// Unlike prop_btree_order_invariant, this allows duplicate inserts
        /// (which produce PrimaryKeyViolation) and verifies exact rowid-set
        /// equality with a reference BTreeMap.
        #[test]
        fn prop_btree_vs_btreemap_reference(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=2000_i64, proptest::collection::vec(proptest::num::u8::ANY, 10..100))
                        .prop_map(|(r, p)| (true, r, p)),
                    1 => (1..=2000_i64,).prop_map(|(r,)| (false, r, Vec::new())),
                ],
                1..500
            )
        ) {
            let mut store = MemPageStore::new(USABLE);
            store.pages.insert(2, build_leaf_table(&[]));

            let cx = Cx::new();
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            let mut reference: std::collections::BTreeMap<i64, Vec<u8>> =
                std::collections::BTreeMap::new();

            for (is_insert, rowid, payload) in &ops {
                if *is_insert {
                    if reference.contains_key(rowid) {
                        // Duplicate: inserting an existing rowid must fail.
                        let result = cursor.table_insert(&cx, *rowid, payload);
                        proptest::prop_assert!(
                            matches!(result, Err(FrankenError::PrimaryKeyViolation)),
                            "duplicate rowid {} should produce PrimaryKeyViolation, got {:?}",
                            rowid,
                            result,
                        );
                    } else {
                        cursor.table_insert(&cx, *rowid, payload).unwrap();
                        reference.insert(*rowid, payload.clone());
                    }
                } else if reference.contains_key(rowid) {
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(rowid);
                    }
                }
            }

            // Scan and compare with reference.
            let mut scanned_rowids = Vec::new();
            if cursor.first(&cx).unwrap() {
                loop {
                    scanned_rowids.push(cursor.rowid(&cx).unwrap());
                    if !cursor.next(&cx).unwrap() {
                        break;
                    }
                }
            }

            let ref_rowids: Vec<i64> = reference.keys().copied().collect();
            proptest::prop_assert_eq!(
                &scanned_rowids,
                &ref_rowids,
                "bead_id=bd-2sm1 case=btree_vs_btreemap rowids mismatch"
            );
        }

        #[test]
        fn prop_table_btree_structural_invariants_hold_after_random_mutations(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=2_000_i64, proptest::collection::vec(proptest::num::u8::ANY, 10..100))
                        .prop_map(|(r, p)| (true, r, p)),
                    1 => (1..=2_000_i64,).prop_map(|(r,)| (false, r, Vec::new())),
                ],
                1..400
            )
        ) {
            let mut store = MemPageStore::new(USABLE);
            store.pages.insert(2, build_leaf_table(&[]));

            let cx = Cx::new();
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            let mut reference = BTreeMap::<i64, Vec<u8>>::new();

            for (step, (is_insert, rowid, payload)) in ops.iter().enumerate() {
                if *is_insert {
                    if reference.contains_key(rowid) {
                        let result = cursor.table_insert(&cx, *rowid, payload);
                        proptest::prop_assert!(
                            matches!(result, Err(FrankenError::PrimaryKeyViolation)),
                            "duplicate rowid {} at step {} should produce PrimaryKeyViolation, got {:?}",
                            rowid,
                            step,
                            result,
                        );
                    } else {
                        cursor.table_insert(&cx, *rowid, payload).unwrap();
                        reference.insert(*rowid, payload.clone());
                    }
                } else if reference.contains_key(rowid) {
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(rowid);
                    }
                }

                let bounds = validate_table_tree_invariants(&cursor.pager, pn(2), USABLE)
                    .map_err(|err| {
                        proptest::test_runner::TestCaseError::fail(format!(
                            "table structural invariant failed after step {} ({:?}, {}, payload_len={}): {}",
                            step,
                            if *is_insert { "insert" } else { "delete" },
                            rowid,
                            payload.len(),
                            err
                        ))
                    })?;

                let expected_bounds = match (
                    reference.keys().next().copied(),
                    reference.keys().next_back().copied(),
                ) {
                    (Some(min_rowid), Some(max_rowid)) => Some(TableSubtreeBounds {
                        min_rowid,
                        max_rowid,
                    }),
                    (None, None) => None,
                    _ => unreachable!("BTreeMap first/last should agree on emptiness"),
                };
                proptest::prop_assert_eq!(
                    bounds,
                    expected_bounds,
                    "table subtree bounds diverged from reference after step {}",
                    step
                );
            }
        }

        #[test]
        fn prop_index_btree_structural_invariants_hold_after_random_mutations(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=2_000_i64,).prop_map(|(id,)| (true, id)),
                    1 => (1..=2_000_i64,).prop_map(|(id,)| (false, id)),
                ],
                1..220
            )
        ) {
            const INDEX_USABLE: u32 = 512;

            let root = pn(2);
            let store = MemPageStore::with_empty_index(root, INDEX_USABLE);
            let cx = Cx::new();
            let mut cursor = BtCursor::new(store, root, INDEX_USABLE, false);
            let mut reference = BTreeMap::<i64, Vec<u8>>::new();

            for (step, (is_insert, id)) in ops.iter().enumerate() {
                let key = synthetic_index_key(*id);

                if *is_insert {
                    if !reference.contains_key(id) {
                        cursor.index_insert(&cx, &key).unwrap();
                        reference.insert(*id, key);
                    }
                } else if reference.contains_key(id) {
                    let seek = cursor.index_move_to(&cx, &key).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(id);
                    }
                }

                let bounds = validate_index_tree_invariants(&mut cursor, root).map_err(|err| {
                    proptest::test_runner::TestCaseError::fail(format!(
                        "index structural invariant failed after step {} ({:?}, {}): {}",
                        step,
                        if *is_insert { "insert" } else { "delete" },
                        id,
                        err
                    ))
                })?;

                let mut expected_keys: Vec<Vec<u8>> = reference.values().cloned().collect();
                expected_keys.sort_by(|lhs, rhs| compare_index_test_keys(&cursor, lhs, rhs));

                let scanned = scan_all_index_keys(&mut cursor, &cx).map_err(|err| {
                    proptest::test_runner::TestCaseError::fail(format!(
                        "index scan failed after step {} ({:?}, {}): {}",
                        step,
                        if *is_insert { "insert" } else { "delete" },
                        id,
                        err
                    ))
                })?;

                let expected_bounds = match (expected_keys.first(), expected_keys.last()) {
                    (Some(min_key), Some(max_key)) => Some(IndexSubtreeBounds {
                        min_key: min_key.clone(),
                        max_key: max_key.clone(),
                        entry_count: expected_keys.len(),
                    }),
                    (None, None) => None,
                    _ => unreachable!("expected key bounds should agree on emptiness"),
                };

                proptest::prop_assert_eq!(
                    scanned,
                    expected_keys,
                    "index logical sequence diverged from the reference after step {}",
                    step
                );
                proptest::prop_assert_eq!(
                    bounds,
                    expected_bounds,
                    "index subtree bounds diverged from the reference after step {}",
                    step
                );
            }
        }

        #[test]
        fn prop_table_seek_cache_matches_forced_full_descent(
            workload in proptest::collection::vec(-64_i64..=320_i64, 1..200)
        ) {
            let cx = Cx::new();
            let root = pn(2);
            let store = MemPageStore::with_empty_table(root, USABLE);
            let mut seed_cursor = BtCursor::new(store, root, USABLE, true);

            for rowid in 1_i64..=256_i64 {
                let payload = vec![b'Q'; 160 + usize::try_from(rowid % 17).unwrap()];
                seed_cursor.table_insert(&cx, rowid, &payload).unwrap();
            }

            let mut cached_cursor = BtCursor::new(seed_cursor.pager.clone(), root, USABLE, true);
            let mut baseline_cursor = BtCursor::new(seed_cursor.pager.clone(), root, USABLE, true);

            for target in workload {
                baseline_cursor.clear_seek_cache();

                let baseline = baseline_cursor.table_move_to(&cx, target).unwrap();
                let cached = cached_cursor.table_move_to(&cx, target).unwrap();

                proptest::prop_assert_eq!(
                    cached.is_found(),
                    baseline.is_found(),
                    "seek hit mismatch for rowid {}",
                    target
                );
                proptest::prop_assert_eq!(
                    cached_cursor.eof(),
                    baseline_cursor.eof(),
                    "EOF mismatch for rowid {}",
                    target
                );

                if !cached_cursor.eof() {
                    let cached_rowid = cached_cursor.rowid(&cx).unwrap();
                    let baseline_rowid = baseline_cursor.rowid(&cx).unwrap();
                    proptest::prop_assert_eq!(
                        cached_rowid,
                        baseline_rowid,
                        "landing rowid mismatch for target {}",
                        target
                    );

                    let cached_payload = cached_cursor.payload(&cx).unwrap();
                    let baseline_payload = baseline_cursor.payload(&cx).unwrap();
                    proptest::prop_assert_eq!(
                        cached_payload,
                        baseline_payload,
                        "landing payload mismatch for target {}",
                        target
                    );
                }
            }
        }

        #[test]
        fn prop_cell_slot_cache_matches_fresh_parse_after_random_mutations(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=300_i64, proptest::collection::vec(proptest::num::u8::ANY, 0..64))
                        .prop_map(|(rowid, payload)| (true, rowid, payload)),
                    1 => (1..=300_i64,).prop_map(|(rowid,)| (false, rowid, Vec::new())),
                ],
                1..120
            )
        ) {
            let cx = Cx::new();
            let root = pn(2);
            let store = MemPageStore::with_empty_table(root, USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), root, USABLE, true);
            let mut reference = BTreeMap::<i64, Vec<u8>>::new();

            for (is_insert, rowid, payload) in ops {
                if is_insert {
                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        reference.entry(rowid)
                    {
                        cursor.table_insert(&cx, rowid, &payload).unwrap();
                        entry.insert(payload);
                    }
                } else if reference.contains_key(&rowid) {
                    let seek = cursor.table_move_to(&cx, rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(&rowid);
                    }
                }

                if let Some(target) = reference.keys().next().copied() {
                    cursor.table_move_to(&cx, target).unwrap();
                } else {
                    cursor.first(&cx).unwrap();
                }

                stack_cell_cache_matches_fresh_parse(&cursor)
                    .map_err(proptest::test_runner::TestCaseError::fail)?;
            }
        }

        #[test]
        fn prop_table_leaf_interpolation_matches_binary_search(
            rowids in proptest::collection::btree_set(-10_000_i64..=10_000_i64, 0..128),
            target in -12_000_i64..=12_000_i64,
        ) {
            let cx = Cx::new();
            let mut store = MemPageStore::new(USABLE);
            let payloads: Vec<Vec<u8>> = rowids
                .iter()
                .map(|rowid| rowid.to_le_bytes().to_vec())
                .collect();
            let entries: Vec<(i64, &[u8])> = rowids
                .iter()
                .zip(payloads.iter())
                .map(|(rowid, payload)| (*rowid, payload.as_slice()))
                .collect();
            store.pages.insert(2, build_leaf_table(&entries));

            let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
            let entry = cursor.load_page(&cx, pn(2)).unwrap();

            let interpolation =
                BtCursor::<MemPageStore>::search_integer_key_table_leaf(&cx, &entry, target)
                    .unwrap();
            let binary =
                BtCursor::<MemPageStore>::binary_search_table_leaf(&cx, &entry, target).unwrap();

            proptest::prop_assert_eq!(
                interpolation,
                binary,
                "interpolation search must match binary search for target {} and rowids {:?}",
                target,
                rowids
            );
        }

        /// Reverse-direction companion to `prop_btree_vs_btreemap_reference`.
        ///
        /// The existing forward-only property test (`prop_btree_vs_btreemap_reference`)
        /// walked the B-tree with `first()` + `next()`.  Both frankensqlite#95-class
        /// defects (the asymmetric `advance_prev` original fix, and the secondary
        /// interior pop-and-recurse defect) lived exclusively in the reverse-scan
        /// code path and would have been caught automatically by this test.
        ///
        /// Strategy: build the same B-tree as `prop_btree_vs_btreemap_reference`,
        /// then walk it backward with `last()` + `prev()` and compare against
        /// `reference.keys().rev().cloned().collect::<Vec<_>>()`.
        #[test]
        fn prop_btree_reverse_iter_matches_btreemap_rev(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=2000_i64, proptest::collection::vec(proptest::num::u8::ANY, 10..100))
                        .prop_map(|(r, p)| (true, r, p)),
                    1 => (1..=2000_i64,).prop_map(|(r,)| (false, r, Vec::new())),
                ],
                1..500
            )
        ) {
            let mut store = MemPageStore::new(USABLE);
            store.pages.insert(2, build_leaf_table(&[]));

            let cx = Cx::new();
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            let mut reference: std::collections::BTreeMap<i64, Vec<u8>> =
                std::collections::BTreeMap::new();

            for (is_insert, rowid, payload) in &ops {
                if *is_insert {
                    if reference.contains_key(rowid) {
                        // Duplicate: must produce PrimaryKeyViolation.
                        let result = cursor.table_insert(&cx, *rowid, payload);
                        proptest::prop_assert!(
                            matches!(result, Err(FrankenError::PrimaryKeyViolation)),
                            "duplicate rowid {} should produce PrimaryKeyViolation, got {:?}",
                            rowid,
                            result,
                        );
                    } else {
                        cursor.table_insert(&cx, *rowid, payload).unwrap();
                        reference.insert(*rowid, payload.clone());
                    }
                } else if reference.contains_key(rowid) {
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(rowid);
                    }
                }
            }

            // Scan in REVERSE using last() + prev() and compare against the
            // reversed reference sequence.
            let mut scanned_rev = Vec::new();
            if cursor.last(&cx).unwrap() {
                loop {
                    scanned_rev.push(cursor.rowid(&cx).unwrap());
                    if !cursor.prev(&cx).unwrap() {
                        break;
                    }
                }
            }

            let expected_rev: Vec<i64> = reference.keys().rev().copied().collect();
            proptest::prop_assert_eq!(
                &scanned_rev,
                &expected_rev,
                "reverse scan (last+prev) rowids must match BTreeMap::keys().rev()"
            );
        }

        /// Round-trip property: forward scan followed by reverse scan must
        /// produce mirror-image sequences for the same B-tree state.
        ///
        /// This is a stronger invariant than either direction test alone: it
        /// asserts that `forward.rev() == reverse` without requiring any
        /// external reference, catching asymmetries that cancel out when
        /// compared against a BTreeMap separately.
        #[test]
        fn prop_btree_forward_and_reverse_round_trip(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=2000_i64, proptest::collection::vec(proptest::num::u8::ANY, 10..100))
                        .prop_map(|(r, p)| (true, r, p)),
                    1 => (1..=2000_i64,).prop_map(|(r,)| (false, r, Vec::new())),
                ],
                1..500
            )
        ) {
            let mut store = MemPageStore::new(USABLE);
            store.pages.insert(2, build_leaf_table(&[]));

            let cx = Cx::new();
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            let mut reference: std::collections::BTreeMap<i64, Vec<u8>> =
                std::collections::BTreeMap::new();

            for (is_insert, rowid, payload) in &ops {
                if *is_insert {
                    if !reference.contains_key(rowid) {
                        cursor.table_insert(&cx, *rowid, payload).unwrap();
                        reference.insert(*rowid, payload.clone());
                    } else {
                        // Tolerate PrimaryKeyViolation on duplicates; keep reference consistent.
                        let _ = cursor.table_insert(&cx, *rowid, payload);
                    }
                } else if reference.contains_key(rowid) {
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(rowid);
                    }
                }
            }

            // Forward pass.
            let mut forward = Vec::new();
            if cursor.first(&cx).unwrap() {
                loop {
                    forward.push(cursor.rowid(&cx).unwrap());
                    if !cursor.next(&cx).unwrap() {
                        break;
                    }
                }
            }

            // Reverse pass on the same tree.
            let mut reverse = Vec::new();
            if cursor.last(&cx).unwrap() {
                loop {
                    reverse.push(cursor.rowid(&cx).unwrap());
                    if !cursor.prev(&cx).unwrap() {
                        break;
                    }
                }
            }

            // The reverse scan must be exactly the mirror of the forward scan.
            let forward_rev: Vec<i64> = forward.iter().rev().copied().collect();
            proptest::prop_assert_eq!(
                &reverse,
                &forward_rev,
                "reverse scan must be the exact mirror of forward scan \
                 (forward.rev() != reverse)"
            );
        }

        /// Index B-tree traversal property test: walks an index B-tree in
        /// REVERSE using `last()` + `prev()` after random insertions/deletions
        /// and asserts the reverse-scanned key sequence matches the
        /// `reference.values().sorted().rev()` sequence.
        ///
        /// The existing `prop_index_btree_structural_invariants_hold_after_random_mutations`
        /// only validates structural invariants and forward scan (via
        /// `scan_all_index_keys`); this test exercises the reverse-traversal code
        /// path for index B-trees, which was not covered by any prior property test.
        #[test]
        fn prop_index_btree_reverse_iter_matches_sorted_rev(
            ops in proptest::collection::vec(
                proptest::prop_oneof![
                    3 => (1..=2_000_i64,).prop_map(|(id,)| (true, id)),
                    1 => (1..=2_000_i64,).prop_map(|(id,)| (false, id)),
                ],
                1..220
            )
        ) {
            const INDEX_USABLE: u32 = 512;

            let root = pn(2);
            let store = MemPageStore::with_empty_index(root, INDEX_USABLE);
            let cx = Cx::new();
            let mut cursor = BtCursor::new(store, root, INDEX_USABLE, false);
            let mut reference = BTreeMap::<i64, Vec<u8>>::new();

            for (is_insert, id) in &ops {
                let key = synthetic_index_key(*id);
                if *is_insert {
                    if !reference.contains_key(id) {
                        cursor.index_insert(&cx, &key).unwrap();
                        reference.insert(*id, key);
                    }
                } else if reference.contains_key(id) {
                    let seek = cursor.index_move_to(&cx, &key).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        reference.remove(id);
                    }
                }
            }

            // Build expected forward key sequence (same sort order as index).
            let mut expected_keys: Vec<Vec<u8>> = reference.values().cloned().collect();
            expected_keys.sort_by(|lhs, rhs| compare_index_test_keys(&cursor, lhs, rhs));

            // Reverse scan: last() + prev().
            let mut scanned_rev: Vec<Vec<u8>> = Vec::new();
            if cursor.last(&cx).unwrap() {
                loop {
                    scanned_rev.push(cursor.payload(&cx).unwrap());
                    if !cursor.prev(&cx).unwrap() {
                        break;
                    }
                }
            }

            // Expected reverse = forward list reversed.
            let expected_rev: Vec<Vec<u8>> = expected_keys.iter().rev().cloned().collect();
            proptest::prop_assert_eq!(
                &scanned_rev,
                &expected_rev,
                "index reverse scan (last+prev) must match sorted keys reversed"
            );
        }
    }

    #[test]
    fn test_real_cursor_revives_from_eof() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, build_leaf_table(&[]));

        // Insert a few records into a leaf
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        cursor.table_insert(&cx, 1, b"one").unwrap();
        cursor.table_insert(&cx, 2, b"two").unwrap();

        cursor.first(&cx).unwrap();
        cursor.next(&cx).unwrap(); // at 2
        assert!(!cursor.next(&cx).unwrap()); // now at EOF

        assert!(cursor.eof());

        // REVIVE FROM EOF
        let revived = cursor.prev(&cx).unwrap();
        assert!(revived, "Real cursor should revive from EOF");
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid(&cx).unwrap(), 2);
    }

    #[test]
    fn test_table_leaf_delete_run_defragments_multiple_root_leaf_cells() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let payloads: Vec<(i64, Vec<u8>)> = (1_i64..=20_i64)
            .map(|rowid| {
                let payload = format!(
                    "payload-{rowid:02}-{}",
                    "x".repeat(usize::try_from(rowid % 5 + 1).unwrap())
                )
                .into_bytes();
                (rowid, payload)
            })
            .collect();

        for (rowid, payload) in &payloads {
            cursor.table_insert(&cx, *rowid, payload).unwrap();
        }
        assert!(cursor.table_move_to(&cx, 3).unwrap().is_found());
        let mut run = cursor
            .table_leaf_delete_run_current(3)
            .unwrap()
            .expect("positioned root leaf should admit a delete run");
        let deleted_rowids = [3_i64, 7, 11];
        for rowid in deleted_rowids {
            assert!(
                run.delete_rowid(&cx, rowid, USABLE).unwrap(),
                "delete run should accept root-leaf rowid {rowid}"
            );
        }
        cursor.flush_table_leaf_delete_run(&cx, run).unwrap();

        for deleted in deleted_rowids {
            assert!(
                !cursor.table_move_to(&cx, deleted).unwrap().is_found(),
                "deleted rowid {deleted} should be absent"
            );
        }
        for (rowid, payload) in payloads
            .iter()
            .filter(|(rowid, _)| !deleted_rowids.contains(rowid))
        {
            assert!(
                cursor.table_move_to(&cx, *rowid).unwrap().is_found(),
                "surviving rowid {rowid} should remain reachable"
            );
            assert_eq!(cursor.payload(&cx).unwrap().as_slice(), payload.as_slice());
        }

        let entry = cursor.load_page(&cx, root).unwrap();
        assert_eq!(entry.header.cell_count, 17);
        assert_eq!(entry.header.first_freeblock, 0);
        assert_eq!(entry.header.fragmented_free_bytes, 0);
        let min_ptr = entry.cell_pointers.iter().copied().min().unwrap();
        assert_eq!(
            usize::from(min_ptr),
            entry.header.content_offset(USABLE),
            "delete-run flush should leave one compact cell area"
        );
    }

    #[test]
    fn test_table_leaf_delete_run_handles_out_of_order_duplicate_checks() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let payloads: Vec<(i64, Vec<u8>)> = (1_i64..=20_i64)
            .map(|rowid| (rowid, format!("payload-{rowid:02}").into_bytes()))
            .collect();

        for (rowid, payload) in &payloads {
            cursor.table_insert(&cx, *rowid, payload).unwrap();
        }
        assert!(cursor.table_move_to(&cx, 9).unwrap().is_found());
        let mut run = cursor
            .table_leaf_delete_run_current(9)
            .unwrap()
            .expect("positioned root leaf should admit a delete run");

        for rowid in [9_i64, 3, 11] {
            assert_eq!(
                run.delete_rowid_with_reason(&cx, rowid, USABLE).unwrap(),
                TableLeafDeleteRunDelete::Deleted,
                "delete run should accept out-of-order root-leaf rowid {rowid}"
            );
        }
        assert_eq!(
            run.delete_rowid_with_reason(&cx, 9, USABLE).unwrap(),
            TableLeafDeleteRunDelete::Miss(TableLeafDeleteRunMissReason::AlreadyDeleted),
            "out-of-order delete run must still detect duplicate rowids"
        );
        cursor.flush_table_leaf_delete_run(&cx, run).unwrap();

        for (rowid, payload) in &payloads {
            let found = cursor.table_move_to(&cx, *rowid).unwrap().is_found();
            if [3_i64, 9, 11].contains(rowid) {
                assert!(!found, "deleted rowid {rowid} should be absent");
            } else {
                assert!(found, "surviving rowid {rowid} should remain reachable");
                assert_eq!(cursor.payload(&cx).unwrap().as_slice(), payload.as_slice());
            }
        }
    }

    #[test]
    fn test_table_leaf_delete_run_defragments_large_root_leaf_delete_set() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let payloads: Vec<(i64, Vec<u8>)> = (1_i64..=40_i64)
            .map(|rowid| (rowid, format!("payload-{rowid:02}").into_bytes()))
            .collect();

        for (rowid, payload) in &payloads {
            cursor.table_insert(&cx, *rowid, payload).unwrap();
        }
        assert!(cursor.table_move_to(&cx, 2).unwrap().is_found());
        let mut run = cursor
            .table_leaf_delete_run_current(2)
            .unwrap()
            .expect("positioned root leaf should admit a delete run");
        let deleted_rowids = [2_i64, 4, 6, 8, 10, 12, 14, 16, 18];
        assert!(
            deleted_rowids.len() >= COMPACT_DELETE_SINGLE_PASS_MIN,
            "test must exercise the one-pass delete-run materializer"
        );
        for rowid in deleted_rowids {
            assert!(
                run.delete_rowid(&cx, rowid, USABLE).unwrap(),
                "delete run should accept root-leaf rowid {rowid}"
            );
        }
        cursor.flush_table_leaf_delete_run(&cx, run).unwrap();

        for (rowid, payload) in &payloads {
            let found = cursor.table_move_to(&cx, *rowid).unwrap().is_found();
            if deleted_rowids.contains(rowid) {
                assert!(!found, "deleted rowid {rowid} should be absent");
            } else {
                assert!(found, "surviving rowid {rowid} should remain reachable");
                assert_eq!(cursor.payload(&cx).unwrap().as_slice(), payload.as_slice());
            }
        }
    }

    #[test]
    fn test_table_leaf_delete_run_rejects_nonroot_leaf_max_cell() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10)], pn(4)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"L1"), (5, b"L5"), (10, b"L10")]));
        store
            .pages
            .insert(4, build_leaf_table(&[(20, b"L20"), (25, b"L25")]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
        assert!(cursor.table_move_to(&cx, 10).unwrap().is_found());
        assert!(
            cursor.table_leaf_delete_run_current(10).unwrap().is_none(),
            "non-root leaf maximum deletion must use the ordinary path so the parent separator is repaired"
        );
    }

    #[test]
    fn test_table_leaf_delete_run_honors_cancelled_context_before_search() {
        let cx = Cx::new();
        let root = pn(2);
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        for rowid in 1_i64..=8 {
            cursor.table_insert(&cx, rowid, b"payload").unwrap();
        }

        assert!(cursor.table_move_to(&cx, 3).unwrap().is_found());
        let mut run = cursor
            .table_leaf_delete_run_current(3)
            .unwrap()
            .expect("positioned root leaf should admit a delete run");

        let cancelled_cx = Cx::new();
        cancelled_cx.transition_to_running();
        cancelled_cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        let err = run.delete_rowid(&cancelled_cx, 3, USABLE).unwrap_err();
        assert!(matches!(err, FrankenError::Abort));
        assert!(
            !run.is_dirty(),
            "cancelled delete-run search must not stage a deletion"
        );
    }

    #[test]
    fn test_table_move_to_honors_cancelled_context() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_table(&[(1, b"one"), (2, b"two")]));

        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let cx = Cx::new();
        cx.transition_to_running();
        cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        let err = cursor.table_move_to(&cx, 2).unwrap_err();
        assert!(matches!(err, FrankenError::Abort));
        assert!(
            cursor.stack.is_empty(),
            "cancelled seek should not mutate stack"
        );
    }

    #[test]
    fn test_next_honors_cancelled_context() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_leaf_table(&[(1, b"one"), (2, b"two")]));

        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        assert!(cursor.first(&cx).unwrap());
        assert_eq!(cursor.rowid(&cx).unwrap(), 1);

        cx.transition_to_running();
        cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        let err = cursor.next(&cx).unwrap_err();
        assert!(matches!(err, FrankenError::Abort));
        assert_eq!(
            cursor.rowid(&Cx::new()).unwrap(),
            1,
            "cancelled iteration should preserve the prior cursor position"
        );
    }

    /// bd-wwqen.1: count_all_rows must return the correct count for empty,
    /// root-only (single-leaf), and multi-leaf (interior-node) trees.
    #[test]
    fn test_count_all_rows_empty_root_only_and_multi_leaf() {
        const USABLE: u32 = 4096;
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();

        // ── Empty tree: zero rows ──
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        assert_eq!(
            cursor.count_all_rows(&cx).unwrap(),
            0,
            "bd-wwqen.1: empty table must return count 0"
        );

        // ── Root-only tree: small number of rows in a single leaf ──
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        for i in 1..=5_i64 {
            cursor
                .table_insert(&cx, i, format!("row-{i}").as_bytes())
                .expect("insert should succeed");
        }
        assert_eq!(
            cursor.count_all_rows(&cx).unwrap(),
            5,
            "bd-wwqen.1: root-only table with 5 rows must return count 5"
        );

        // ── Multi-leaf tree: enough rows to force page splits ──
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let n = 500;
        let payload = vec![b'X'; 200]; // ~200 bytes per row → ~20 rows/page → ~25 pages
        for i in 1..=n {
            cursor
                .table_insert(&cx, i, &payload)
                .expect("insert should succeed");
        }
        let count = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            count, n,
            "bd-wwqen.1: multi-leaf table with {n} rows must return count {n}, got {count}"
        );

        // ── count_all_rows preserves cursor usability ──
        // After count, cursor should still be usable for a seek.
        assert!(
            cursor
                .table_move_to(&cx, 1)
                .expect("seek after count should succeed")
                .is_found(),
            "bd-wwqen.1: cursor must remain usable after count_all_rows"
        );
    }

    #[test]
    fn test_count_all_rows_deep_tree_rightmost_10k() {
        const USABLE: u32 = 4096;
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let row_count = 10_000_i64;
        let payload = vec![b'R'; 200];

        for rowid in 1..=row_count {
            cursor
                .table_insert(&cx, rowid, &payload)
                .expect("insert should succeed");
        }

        let depth = measure_tree_depth(&cursor.pager, root, USABLE);
        assert!(
            depth >= 3,
            "test requires a deeper interior tree, got depth {depth}"
        );

        let count = cursor.count_all_rows(&cx).unwrap();
        assert_eq!(
            count, row_count,
            "deep/rightmost table with {row_count} rows must count exactly"
        );

        assert!(
            cursor
                .table_move_to(&cx, row_count)
                .expect("seek after deep count should succeed")
                .is_found(),
            "rightmost row must remain reachable after count_all_rows"
        );
    }

    #[test]
    fn test_table_bulk_load_empty_root_sorted_records_builds_reachable_tree() {
        const USABLE: u32 = 4096;
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let records: Vec<(i64, Vec<u8>)> = (1_i64..=10_000_i64)
            .map(|rowid| {
                let byte = u8::try_from(rowid % 251).unwrap();
                (
                    rowid,
                    vec![byte; 220 + usize::try_from(rowid % 31).unwrap()],
                )
            })
            .collect();

        assert!(
            cursor
                .table_bulk_load_empty_root_sorted_records(&cx, &records)
                .unwrap(),
            "bulk load should accept sorted no-overflow records for an empty table"
        );
        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 10_000);
        assert!(
            measure_tree_depth(&cursor.pager, root, USABLE) >= 3,
            "test data should force at least one interior page split below the root"
        );

        assert!(cursor.first(&cx).unwrap());
        for (expected_rowid, expected_payload) in &records {
            assert_eq!(cursor.rowid(&cx).unwrap(), *expected_rowid);
            assert_eq!(cursor.payload(&cx).unwrap(), *expected_payload);
            if *expected_rowid < 10_000 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());

        assert!(
            !cursor
                .table_bulk_load_empty_root_sorted_records(&cx, &records)
                .unwrap(),
            "bulk load must fall back once the root is no longer an empty leaf"
        );
    }

    #[test]
    fn test_table_bulk_append_depth2_right_edge_sorted_records_extends_tree() {
        const USABLE: u32 = 4096;
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let initial: Vec<(i64, Vec<u8>)> = (1_i64..=1_000_i64)
            .map(|rowid| (rowid, vec![u8::try_from(rowid % 251).unwrap(); 220]))
            .collect();
        let appended: Vec<(i64, Vec<u8>)> = (1_001_i64..=2_000_i64)
            .map(|rowid| (rowid, vec![u8::try_from(rowid % 251).unwrap(); 220]))
            .collect();

        assert!(
            cursor
                .table_bulk_load_empty_root_sorted_records(&cx, &initial)
                .unwrap(),
            "initial rows should bulk-load into an empty table"
        );
        assert_eq!(measure_tree_depth(&cursor.pager, root, USABLE), 2);
        assert!(
            cursor
                .table_can_bulk_append_depth2_right_edge_record(
                    &cx,
                    appended[0].0,
                    appended[0].1.as_slice()
                )
                .unwrap(),
            "right-edge admission probe should accept the next sorted row"
        );
        assert!(
            cursor
                .table_bulk_append_depth2_right_edge_sorted_records(&cx, &appended)
                .unwrap(),
            "depth-2 right-edge append should accept sorted rows above the old max"
        );
        assert_eq!(cursor.count_all_rows(&cx).unwrap(), 2_000);

        assert!(cursor.first(&cx).unwrap());
        for (expected_rowid, expected_payload) in initial.iter().chain(appended.iter()) {
            assert_eq!(cursor.rowid(&cx).unwrap(), *expected_rowid);
            assert_eq!(cursor.payload(&cx).unwrap(), *expected_payload);
            if *expected_rowid < 2_000 {
                assert!(cursor.next(&cx).unwrap());
            }
        }
        assert!(!cursor.next(&cx).unwrap());
        assert!(
            !cursor
                .table_can_bulk_append_depth2_right_edge_record(
                    &cx,
                    initial[0].0,
                    initial[0].1.as_slice()
                )
                .unwrap(),
            "right-edge admission probe must reject records below the old max"
        );
        assert!(
            !cursor
                .table_bulk_append_depth2_right_edge_sorted_records(&cx, &initial)
                .unwrap(),
            "append primitive must reject rows that do not sort after the old max"
        );
    }

    #[test]
    fn test_find_child_slot_by_page_no_matches_actual_root_children() {
        const USABLE: u32 = 4096;
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);
        let payload = vec![b'S'; 200];

        for rowid in 1..=10_000_i64 {
            cursor
                .table_insert(&cx, rowid, &payload)
                .expect("insert should succeed");
        }

        let root_entry = cursor
            .reload_page_fresh(&cx, root)
            .expect("reload root after inserts");
        assert!(
            root_entry.header.page_type.is_interior(),
            "expected interior root after enough inserts"
        );

        for child_idx in 0..=root_entry.header.cell_count {
            let child_page = BtCursor::<MemPageStore>::child_page_at(&root_entry, child_idx)
                .expect("read child pointer");
            let found = cursor
                .find_child_slot_by_page_no(&cx, root, child_page)
                .expect("find child slot");
            assert_eq!(
                found, child_idx,
                "slot lookup must round-trip for child {} on root page {}",
                child_page, root
            );
        }
    }

    // ---- OPT-7: sort_cells_desc_by_ptr correctness + micro-benchmark ----

    /// Correctness: the specialized insertion sort must match
    /// `sort_unstable_by_key(|k| Reverse(k.0))` for primary-key order across
    /// a variety of inputs (equal-primary ties are allowed to reorder).
    #[test]
    fn remove_cell_from_leaf_specialized_sort_matches_std() {
        use std::cmp::Reverse;

        // Empty.
        let mut v: Vec<(usize, usize, usize)> = vec![];
        sort_cells_desc_by_ptr(&mut v);
        assert!(v.is_empty());

        // Single element.
        let mut v: Vec<(usize, usize, usize)> = vec![(100, 10, 0)];
        sort_cells_desc_by_ptr(&mut v);
        assert_eq!(v, vec![(100, 10, 0)]);

        // Already sorted descending (common fast path).
        let mut v: Vec<(usize, usize, usize)> =
            vec![(400, 1, 0), (300, 2, 1), (200, 3, 2), (100, 4, 3)];
        let mut expected = v.clone();
        expected.sort_unstable_by_key(|k| Reverse(k.0));
        sort_cells_desc_by_ptr(&mut v);
        assert_eq!(v, expected);

        // Reverse-sorted (ascending) — worst case for insertion sort.
        let mut v: Vec<(usize, usize, usize)> =
            vec![(100, 4, 3), (200, 3, 2), (300, 2, 1), (400, 1, 0)];
        let mut expected = v.clone();
        expected.sort_unstable_by_key(|k| Reverse(k.0));
        sort_cells_desc_by_ptr(&mut v);
        assert_eq!(v, expected);

        // Mixed shape, with a duplicate primary key.
        let mut v: Vec<(usize, usize, usize)> = vec![
            (250, 5, 0),
            (100, 5, 1),
            (400, 5, 2),
            (150, 5, 3),
            (400, 5, 4), // duplicate primary
            (50, 5, 5),
            (300, 5, 6),
        ];
        let mut expected = v.clone();
        expected.sort_unstable_by_key(|k| Reverse(k.0));
        sort_cells_desc_by_ptr(&mut v);
        // std's `sort_unstable_by_key` is unstable, so only primary-key order
        // must match; tie-breaking between the two 400s may differ.
        let keys_got: Vec<usize> = v.iter().map(|t| t.0).collect();
        let keys_exp: Vec<usize> = expected.iter().map(|t| t.0).collect();
        assert_eq!(keys_got, keys_exp);

        // Larger realistic page: ~80 cells with typical cell-offset distribution.
        let mut v: Vec<(usize, usize, usize)> = (0..80)
            .map(|i| (4096 - (i * 50 + 17) % 3900, (i * 7) % 200 + 8, i))
            .collect();
        let mut expected = v.clone();
        expected.sort_unstable_by_key(|k| Reverse(k.0));
        sort_cells_desc_by_ptr(&mut v);
        let keys_got: Vec<usize> = v.iter().map(|t| t.0).collect();
        let keys_exp: Vec<usize> = expected.iter().map(|t| t.0).collect();
        assert_eq!(keys_got, keys_exp);

        // Large monotone pages should not need pdqsort: descending is already
        // ready for defrag, and ascending only needs a linear reverse.
        let mut descending: Vec<(usize, usize, usize)> =
            (0..80).map(|i| (4096 - i * 37, 16 + i % 11, i)).collect();
        let mut expected = descending.clone();
        expected.sort_unstable_by_key(|k| Reverse(k.0));
        sort_cells_desc_by_ptr(&mut descending);
        assert_eq!(descending, expected);

        let mut ascending: Vec<(usize, usize, usize)> =
            (0..80).map(|i| (512 + i * 37, 16 + i % 11, i)).collect();
        let mut expected = ascending.clone();
        expected.sort_unstable_by_key(|k| Reverse(k.0));
        sort_cells_desc_by_ptr(&mut ascending);
        assert_eq!(ascending, expected);
    }

    /// Micro-benchmark: size-dispatched sort vs unconditional std `sort_unstable_by_key`.
    /// Ignored by default (run with `--release --ignored --nocapture`).
    #[test]
    #[ignore = "micro-benchmark; run with `--release --ignored --nocapture`"]
    fn bench_remove_cell_from_leaf_sort() {
        use std::cmp::Reverse;

        // Shapes that match the DELETE hot path (typical N is 1-8, max ~60-80).
        let shapes: &[(&str, usize)] = &[
            ("N=1", 1),
            ("N=2", 2),
            ("N=4", 4),
            ("N=8", 8),
            ("N=12", 12),
            ("N=16", 16),
            ("N=32", 32),
            ("N=60", 60),
            ("N=80", 80),
        ];

        const ITERS: usize = 1_000_000;

        for (label, n) in shapes {
            // Descending-ish but not perfectly sorted input (cells mostly
            // inserted in order, some gaps after prior deletes).
            let base: Vec<(usize, usize, usize)> = (0..*n)
                .map(|i| (4096 - (i * 47 + 11) % 3800, (i * 13) % 200 + 8, i))
                .collect();

            // Warm-up so both variants see steady-state caches/branches.
            for _ in 0..1024 {
                let mut v = base.clone();
                v.sort_unstable_by_key(|k| Reverse(k.0));
                let mut v = base.clone();
                sort_cells_desc_by_ptr(&mut v);
            }

            let t0 = Instant::now();
            for _ in 0..ITERS {
                let mut v = base.clone();
                v.sort_unstable_by_key(|k| Reverse(k.0));
                std::hint::black_box(&v);
            }
            let std_ns = t0.elapsed().as_nanos() as u64 / ITERS as u64;

            let t0 = Instant::now();
            for _ in 0..ITERS {
                let mut v = base.clone();
                sort_cells_desc_by_ptr(&mut v);
                std::hint::black_box(&v);
            }
            let dispatched_ns = t0.elapsed().as_nanos() as u64 / ITERS as u64;

            println!(
                "[OPT-7 bench] {label}: std={std_ns}ns  dispatched={dispatched_ns}ns  speedup={:.2}x",
                std_ns as f64 / dispatched_ns.max(1) as f64
            );
        }
    }

    /// Micro-benchmark for bd-4i4vh: hot-front-entry CellSlotCache insert.
    ///
    /// Ignored by default (run with `--release --ignored --nocapture`).
    /// Simulates the binary-search-on-a-single-page hot path where every
    /// `parse_cell_at` miss calls `insert()` on the same MRU entry.
    #[test]
    #[ignore = "micro-benchmark; run with `--release --ignored --nocapture`"]
    fn bench_cell_slot_cache_insert_front_entry() {
        const PREFILL: u32 = 32;
        const WARMUP: u32 = 200_000;
        const ITERS: u32 = 5_000_000;

        fn sample_slot(payload_size: u32) -> CachedCellSlot {
            CachedCellSlot {
                left_child: None,
                rowid: Some(1),
                payload_size,
                local_size: payload_size,
                payload_offset: 128,
                overflow_page: None,
            }
        }

        let hot_page = pn(999);
        let hot_counter = 0x1234_5678_u64;
        let slot = sample_slot(32);

        let prime_cache = |cache: &mut CellSlotCache| {
            for i in 0..PREFILL {
                let page = pn(i + 2);
                cache.insert_slow(page, u64::from(i.wrapping_add(0xAB00)), 0, sample_slot(16));
            }
            // Promote hot entry to the front so subsequent inserts exercise
            // the front-entry path.
            cache.insert_slow(hot_page, hot_counter, 0, slot);
            assert_eq!(cache.entries.first().unwrap().page_no, hot_page);
            assert_eq!(cache.entries.first().unwrap().mutation_counter, hot_counter);
        };

        // Fast path — production code.
        let mut cache_fast = CellSlotCache::default();
        prime_cache(&mut cache_fast);
        for i in 0..WARMUP {
            cache_fast.insert(hot_page, hot_counter, (i % 10) as u16, slot);
            std::hint::black_box(&cache_fast);
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            cache_fast.insert(hot_page, hot_counter, (i % 10) as u16, slot);
            std::hint::black_box(&cache_fast);
        }
        let fast_ns = t0.elapsed().as_nanos() as u64 / u64::from(ITERS);

        // Slow path — legacy remove(0) + insert(0) + truncate.
        let mut cache_slow = CellSlotCache::default();
        prime_cache(&mut cache_slow);
        for i in 0..WARMUP {
            cache_slow.insert_slow(hot_page, hot_counter, (i % 10) as u16, slot);
            std::hint::black_box(&cache_slow);
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            cache_slow.insert_slow(hot_page, hot_counter, (i % 10) as u16, slot);
            std::hint::black_box(&cache_slow);
        }
        let slow_ns = t0.elapsed().as_nanos() as u64 / u64::from(ITERS);

        println!(
            "[cell_slot_cache insert front-entry] fast={fast_ns}ns  slow={slow_ns}ns  \
             speedup={:.2}x  (prefill={PREFILL}, iters={ITERS})",
            slow_ns as f64 / fast_ns.max(1) as f64
        );
    }

    /// Micro-benchmark for the hot-front-entry CellSlotCache lookup path.
    ///
    /// Ignored by default (run with `--release --ignored --nocapture`).
    /// Simulates repeated `parse_cell_at` probes on the same leaf page after
    /// the page entry has already been promoted to MRU.
    #[test]
    #[ignore = "micro-benchmark; run with `--release --ignored --nocapture`"]
    fn bench_cell_slot_cache_get_front_entry() {
        const PREFILL: u32 = 32;
        const WARMUP: u32 = 200_000;
        const ITERS: u32 = 10_000_000;

        fn sample_slot(payload_size: u32) -> CachedCellSlot {
            CachedCellSlot {
                left_child: None,
                rowid: Some(1),
                payload_size,
                local_size: payload_size,
                payload_offset: 128,
                overflow_page: None,
            }
        }

        fn legacy_get_slow(
            cache: &mut CellSlotCache,
            page_no: PageNumber,
            mutation_counter: u64,
            cell_idx: u16,
        ) -> Option<CachedCellSlot> {
            let entry_idx = cache.entries.iter().position(|entry| {
                entry.page_no == page_no && entry.mutation_counter == mutation_counter
            })?;
            let slot = cache.entries[entry_idx]
                .slots
                .iter()
                .find_map(|(idx, slot)| (*idx == cell_idx).then_some(*slot))?;
            if entry_idx != 0 {
                let entry = cache.entries.remove(entry_idx);
                cache.entries.insert(0, entry);
            }
            Some(slot)
        }

        let hot_page = pn(999);
        let hot_counter = 0x1234_5678_u64;
        let slot = sample_slot(32);

        let prime_cache = |cache: &mut CellSlotCache| {
            for i in 0..PREFILL {
                let page = pn(i + 2);
                cache.insert_slow(page, u64::from(i.wrapping_add(0xAB00)), 0, sample_slot(16));
            }
            cache.insert_slow(hot_page, hot_counter, 0, slot);
            for idx in 1..10 {
                cache.insert(hot_page, hot_counter, idx, sample_slot(32 + u32::from(idx)));
            }
            assert_eq!(cache.entries.first().unwrap().page_no, hot_page);
            assert_eq!(cache.entries.first().unwrap().mutation_counter, hot_counter);
        };

        let mut cache_fast = CellSlotCache::default();
        prime_cache(&mut cache_fast);
        for i in 0..WARMUP {
            std::hint::black_box(cache_fast.get(hot_page, hot_counter, (i % 10) as u16));
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            std::hint::black_box(cache_fast.get(hot_page, hot_counter, (i % 10) as u16));
        }
        let fast_ns = t0.elapsed().as_nanos() as u64 / u64::from(ITERS);

        let mut cache_slow = CellSlotCache::default();
        prime_cache(&mut cache_slow);
        for i in 0..WARMUP {
            std::hint::black_box(legacy_get_slow(
                &mut cache_slow,
                hot_page,
                hot_counter,
                (i % 10) as u16,
            ));
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            std::hint::black_box(legacy_get_slow(
                &mut cache_slow,
                hot_page,
                hot_counter,
                (i % 10) as u16,
            ));
        }
        let slow_ns = t0.elapsed().as_nanos() as u64 / u64::from(ITERS);

        println!(
            "[cell_slot_cache get front-entry] fast={fast_ns}ns  slow={slow_ns}ns  \
             speedup={:.2}x  (prefill={PREFILL}, iters={ITERS})",
            slow_ns as f64 / fast_ns.max(1) as f64
        );
    }

    #[test]
    fn page_mutation_counter_uses_page_image_token() {
        let mut page_data = PageData::from_vec(vec![0; USABLE as usize]);
        let first = BtCursor::<MemPageStore>::page_mutation_counter(&page_data);
        let snapshot = page_data.clone();

        assert_eq!(
            BtCursor::<MemPageStore>::page_mutation_counter(&snapshot),
            first
        );

        page_data.as_bytes_mut()[0] = 1;
        assert_ne!(
            BtCursor::<MemPageStore>::page_mutation_counter(&page_data),
            first
        );
    }

    /// Micro-benchmark for bd-4i4vh.3: `child_page_at` direct-read vs the old
    /// `parse_cell_at + cell.left_child` path on interior table pages.
    ///
    /// Ignored by default (run with `--release --ignored --nocapture`).
    /// Simulates root-to-leaf descent where every interior-page probe only
    /// needs the left-child pointer; the legacy path paid for two varint
    /// decodes + cell-slot cache traffic + a full `CellRef` struct build just
    /// to throw away everything except `left_child`.
    #[test]
    #[ignore = "micro-benchmark; run with `--release --ignored --nocapture`"]
    fn bench_child_page_at_interior_table() {
        const CHILDREN: usize = 128;
        const WARMUP: u32 = 200_000;
        const ITERS: u32 = 5_000_000;

        // Build a realistic interior-table page: 128 (left_child, rowid)
        // divider cells + a right_child pointer.
        let children: Vec<(PageNumber, i64)> = (0..CHILDREN)
            .map(|i| (pn((i as u32) + 10), (i as i64) * 100 + 1))
            .collect();
        let right = pn((CHILDREN as u32) + 200);
        let page_bytes = build_interior_table(&children, right);

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, page_bytes);

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let entry = cursor.load_page(&cx, pn(2)).unwrap();

        // Warm-up the cell-slot cache so the legacy path hits its cache.
        for idx in 0..entry.header.cell_count {
            let _ = cursor.parse_cell_at(&entry, idx).unwrap();
        }

        // Fast path — production code.
        for i in 0..WARMUP {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(BtCursor::<MemPageStore>::child_page_at(&entry, idx).unwrap());
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(BtCursor::<MemPageStore>::child_page_at(&entry, idx).unwrap());
        }
        let fast_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0 / f64::from(ITERS);

        // Legacy path — parse_cell_at + cell.left_child.ok_or(...)
        let legacy_child_page = |cursor: &BtCursor<MemPageStore>, idx: u16| -> Result<PageNumber> {
            if idx >= entry.header.cell_count {
                entry
                    .header
                    .right_child
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "interior page has no right child".to_owned(),
                    })
            } else {
                let cell = cursor.parse_cell_at(&entry, idx)?;
                cell.left_child
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "interior cell has no left child".to_owned(),
                    })
            }
        };
        for i in 0..WARMUP {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(legacy_child_page(&cursor, idx).unwrap());
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(legacy_child_page(&cursor, idx).unwrap());
        }
        let slow_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0 / f64::from(ITERS);

        println!(
            "[child_page_at interior-table] direct={fast_ns:.3}ns  parse_cell_at={slow_ns:.3}ns  \
             speedup={:.2}x  (children={CHILDREN}, iters={ITERS})",
            slow_ns / fast_ns.max(f64::EPSILON)
        );
    }

    /// bd-yafor.2 cell-slot cache hit-rate audit on the write-append hot path.
    ///
    /// Ignored by default. Drives three realistic workloads and reports the
    /// cache hit-rate observed by the production `CellSlotCache::get` path:
    ///
    /// 1. Monotonic append — table_insert with strictly-increasing rowids.
    /// 2. Random inserts — table_insert with a pseudo-random rowid sequence.
    /// 3. Read-heavy — monotonic insert then repeated `table_move_to` seeks.
    ///
    /// Use with `--release --ignored --nocapture`.
    #[test]
    #[ignore = "audit benchmark; run with `--release --ignored --nocapture`"]
    fn audit_cell_slot_cache_hit_rate_write_vs_read() {
        // bd-9e3xf.5: CellSlotCache hit/miss counters are now gated by the
        // copy-profile flag (shared with other btree instrumentation) so the
        // 1-thread write hot path doesn't pay a `lock xadd` per `get`. The
        // audit needs the gate enabled to observe any counts.
        crate::instrumentation::set_btree_copy_profile_enabled(true);
        struct GateGuard;
        impl Drop for GateGuard {
            fn drop(&mut self) {
                crate::instrumentation::set_btree_copy_profile_enabled(false);
            }
        }
        let _gate_guard = GateGuard;

        const ROWS: i64 = 10_000;
        const PAYLOAD: &[u8] = b"cc_3_audit_payload_0123456789abcdef";

        fn run_and_report(label: &str, counters: crate::cursor::CellSlotCacheCounters) {
            let total = counters.total();
            let misses = counters.misses();
            println!(
                "[cell_slot_cache audit] {label}: hits={}  miss_cold={}  miss_invalidated={}  \
                 miss_slot={}  misses={}  total={total}  hit_rate={:.4}",
                counters.hits,
                counters.miss_cold,
                counters.miss_invalidated,
                counters.miss_slot,
                misses,
                counters.hit_rate()
            );
        }

        // --- Scenario 1: Monotonic append ---
        {
            let cx = Cx::new();
            let store = MemPageStore::with_empty_table(pn(2), USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            reset_cell_slot_cache_counters();
            for rowid in 1..=ROWS {
                cursor.table_insert(&cx, rowid, PAYLOAD).unwrap();
            }
            run_and_report("monotonic_append", cell_slot_cache_counter_snapshot());
        }

        // --- Scenario 2: Random middle-of-tree inserts ---
        {
            let cx = Cx::new();
            let store = MemPageStore::with_empty_table(pn(2), USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            reset_cell_slot_cache_counters();
            let mut state = 0x1234_5678_9abc_def0_u64;
            let mut inserted = std::collections::HashSet::new();
            let mut count = 0;
            while count < ROWS {
                let raw = lcg_next(&mut state);
                #[allow(clippy::cast_possible_wrap)]
                let rowid = ((raw % 100_000) + 1) as i64;
                if inserted.insert(rowid) {
                    cursor.table_insert(&cx, rowid, PAYLOAD).unwrap();
                    count += 1;
                }
            }
            run_and_report("random_insert", cell_slot_cache_counter_snapshot());
        }

        // --- Scenario 3: Read-heavy after append ---
        {
            let cx = Cx::new();
            let store = MemPageStore::with_empty_table(pn(2), USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            for rowid in 1..=ROWS {
                cursor.table_insert(&cx, rowid, PAYLOAD).unwrap();
            }
            reset_cell_slot_cache_counters();
            let mut state = 0xdeadbeef_cafebabe_u64;
            for _ in 0..(ROWS * 4) {
                let raw = lcg_next(&mut state);
                #[allow(clippy::cast_possible_wrap)]
                let target = (raw % (ROWS as u64)) as i64 + 1;
                let _ = cursor.table_move_to(&cx, target).unwrap();
            }
            run_and_report(
                "read_heavy_after_append",
                cell_slot_cache_counter_snapshot(),
            );
        }

        // --- Scenario 4: Binary search probes within a single append ---
        // (sanity check — within a single insert the binary search walks mid,
        // mid/2, mid/4... so the same cell slots ARE re-probed; cache should
        // hit inside this single search if the entry is fresh)
        {
            let cx = Cx::new();
            let store = MemPageStore::with_empty_table(pn(2), USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            // Pre-populate with a full leaf + some interior breadth so binary
            // search actually does multi-level probing.
            for rowid in 1..=500_i64 {
                cursor.table_insert(&cx, rowid, PAYLOAD).unwrap();
            }
            reset_cell_slot_cache_counters();
            for target in (1..=500_i64).rev() {
                let _ = cursor.table_move_to(&cx, target).unwrap();
            }
            run_and_report("reverse_seek_preloaded", cell_slot_cache_counter_snapshot());
        }

        // --- Scenario 5: Delete-heavy (exercises predecessor_idx / separator_idx) ---
        // Each delete that empties a leaf rebalances and may call parse_cell_at
        // for predecessor + separator lookup on an interior page.
        {
            let cx = Cx::new();
            let store = MemPageStore::with_empty_table(pn(2), USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);
            for rowid in 1..=ROWS {
                cursor.table_insert(&cx, rowid, PAYLOAD).unwrap();
            }
            reset_cell_slot_cache_counters();
            for rowid in 1..=ROWS {
                if cursor.table_move_to(&cx, rowid).unwrap().is_found() {
                    cursor.delete(&cx).unwrap();
                }
            }
            run_and_report(
                "delete_all_after_append",
                cell_slot_cache_counter_snapshot(),
            );
        }

        // --- Scenario 6: Index btree inserts (exercises binary_search_index_leaf) ---
        {
            let cx = Cx::new();
            let store = MemPageStore::with_empty_index(pn(2), USABLE);
            let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, false);
            reset_cell_slot_cache_counters();
            let mut state = 0x0102_0304_0506_0708_u64;
            for _ in 0..ROWS {
                let raw = lcg_next(&mut state);
                let key = serialize_record(&[
                    #[allow(clippy::cast_possible_wrap)]
                    SqliteValue::Integer((raw % 100_000) as i64),
                    #[allow(clippy::cast_possible_wrap)]
                    SqliteValue::Integer(raw as i64),
                ]);
                let _ = cursor.index_insert(&cx, &key);
            }
            run_and_report("index_random_insert", cell_slot_cache_counter_snapshot());
        }
    }

    /// bd-k7zd7.3 microbench: `replace_table_interior_separator_rowid`'s
    /// separator-left-child extraction, comparing the direct
    /// `read_interior_child_inline` path against the old
    /// `parse_cell_at + cell.left_child.ok_or(..)` path.
    ///
    /// Ignored by default. Same pattern as `bench_child_page_at_interior_table`
    /// but targets the DELETE-rebalance separator lookup (≈1 call per leaf
    /// collapse; 99 % cache miss per the bd-k7zd7 audit).
    #[test]
    #[ignore = "micro-benchmark; run with `--release --ignored --nocapture`"]
    fn bench_replace_separator_inline_vs_parse_cell_at() {
        const CHILDREN: usize = 128;
        const WARMUP: u32 = 200_000;
        const ITERS: u32 = 5_000_000;

        let children: Vec<(PageNumber, i64)> = (0..CHILDREN)
            .map(|i| (pn((i as u32) + 10), (i as i64) * 100 + 1))
            .collect();
        let right = pn((CHILDREN as u32) + 200);
        let page_bytes = build_interior_table(&children, right);

        let mut store = MemPageStore::new(USABLE);
        store.pages.insert(2, page_bytes);

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, true);
        let entry = cursor.load_page(&cx, pn(2)).unwrap();

        // Warm cell-slot cache so the legacy path hits it.
        for idx in 0..entry.header.cell_count {
            let _ = cursor.parse_cell_at(&entry, idx).unwrap();
        }

        // Fast path — production code after bd-k7zd7.3.
        for i in 0..WARMUP {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(
                BtCursor::<MemPageStore>::read_interior_child_inline(&entry, idx).unwrap(),
            );
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(
                BtCursor::<MemPageStore>::read_interior_child_inline(&entry, idx).unwrap(),
            );
        }
        let fast_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0 / f64::from(ITERS);

        // Legacy path — parse_cell_at + separator_cell.left_child.ok_or(...).
        let legacy = |cursor: &BtCursor<MemPageStore>, idx: u16| -> Result<PageNumber> {
            let separator_cell = cursor.parse_cell_at(&entry, idx)?;
            separator_cell
                .left_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior table separator missing left child".to_owned(),
                })
        };
        for i in 0..WARMUP {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(legacy(&cursor, idx).unwrap());
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            let idx = (i as u16) % entry.header.cell_count;
            std::hint::black_box(legacy(&cursor, idx).unwrap());
        }
        let slow_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0 / f64::from(ITERS);

        println!(
            "[replace_separator_inline] direct={fast_ns:.3}ns  parse_cell_at={slow_ns:.3}ns  \
             speedup={:.2}x  (children={CHILDREN}, iters={ITERS})",
            slow_ns / fast_ns.max(f64::EPSILON)
        );
    }

    /// bd-9e3xf.5 microbench: CellSlotCache::get hot-front-entry path with
    /// the copy-profile gate OFF (production default, 1t write path) vs ON
    /// (observability mode, as used by the audit benchmark).
    ///
    /// The counters added in bd-9e3xf fire six `AtomicU64::fetch_add(1,
    /// Relaxed)` callsites across `get`/`get_slow`. On the 1-thread write
    /// hot path those counters are pure overhead. Gating them behind the
    /// same profile flag that already gates `record_*_copy` etc. restores
    /// the get-fast-path cost to where it was pre-instrumentation.
    #[test]
    #[ignore = "micro-benchmark; run with `--release --ignored --nocapture`"]
    fn bench_cell_slot_cache_get_gate_on_vs_off() {
        const PREFILL: u32 = 32;
        const WARMUP: u32 = 200_000;
        const ITERS: u32 = 10_000_000;

        fn sample_slot(payload_size: u32) -> CachedCellSlot {
            CachedCellSlot {
                left_child: None,
                rowid: Some(1),
                payload_size,
                local_size: payload_size,
                payload_offset: 128,
                overflow_page: None,
            }
        }

        let hot_page = pn(999);
        let hot_counter = 0x1234_5678_u64;
        let slot = sample_slot(32);

        let prime_cache = |cache: &mut CellSlotCache| {
            for i in 0..PREFILL {
                let page = pn(i + 2);
                cache.insert_slow(page, u64::from(i.wrapping_add(0xAB00)), 0, sample_slot(16));
            }
            for warmup in 0..10_u16 {
                cache.insert_slow(hot_page, hot_counter, warmup, slot);
            }
            assert_eq!(cache.entries.first().unwrap().page_no, hot_page);
        };

        // --- Gate OFF: production default (1t write path) ---
        crate::instrumentation::set_btree_copy_profile_enabled(false);
        let mut cache_off = CellSlotCache::default();
        prime_cache(&mut cache_off);
        for i in 0..WARMUP {
            std::hint::black_box(cache_off.get(hot_page, hot_counter, (i % 10) as u16));
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            std::hint::black_box(cache_off.get(hot_page, hot_counter, (i % 10) as u16));
        }
        let off_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0 / f64::from(ITERS);

        // --- Gate ON: observability / audit mode ---
        crate::instrumentation::set_btree_copy_profile_enabled(true);
        let mut cache_on = CellSlotCache::default();
        prime_cache(&mut cache_on);
        for i in 0..WARMUP {
            std::hint::black_box(cache_on.get(hot_page, hot_counter, (i % 10) as u16));
        }
        let t0 = Instant::now();
        for i in 0..ITERS {
            std::hint::black_box(cache_on.get(hot_page, hot_counter, (i % 10) as u16));
        }
        let on_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0 / f64::from(ITERS);
        crate::instrumentation::set_btree_copy_profile_enabled(false);

        println!(
            "[cell_slot_cache get gate] profile_off={off_ns:.3}ns  profile_on={on_ns:.3}ns  \
             counter_cost={:.3}ns  (prefill={PREFILL}, iters={ITERS})",
            on_ns - off_ns
        );
    }

    #[test]
    fn cell_pointers_pool_recycles_buffer_capacity() {
        // Drain the thread-local pool so this test sees a known starting state.
        CELL_POINTERS_POOL.with(|pool| pool.borrow_mut().clear());

        let mut warm = Vec::with_capacity(64);
        warm.extend_from_slice(&[1, 2, 3, 4]);
        let warm_ptr = warm.as_ptr();
        recycle_cell_pointers(warm);

        let recycled = take_pooled_cell_pointers();
        assert_eq!(recycled.len(), 0, "pooled buffer must be returned cleared");
        assert!(
            recycled.capacity() >= 64,
            "pooled buffer must retain its capacity (got {})",
            recycled.capacity()
        );
        assert!(
            std::ptr::eq(recycled.as_ptr(), warm_ptr),
            "pool should hand back the same heap allocation"
        );

        // Empty / oversized buffers must not poison the pool.
        recycle_cell_pointers(Vec::<u16>::new());
        let mut huge = Vec::<u16>::with_capacity(CELL_POINTERS_POOL_MAX_CAPACITY + 1);
        huge.push(0);
        recycle_cell_pointers(huge);
        recycle_cell_pointers(recycled);

        let pool_len = CELL_POINTERS_POOL.with(|pool| pool.borrow().len());
        assert_eq!(
            pool_len, 1,
            "only the in-range buffer should remain pooled (got {pool_len})"
        );

        // Cleanup so we don't leak a buffer into other tests on this thread.
        CELL_POINTERS_POOL.with(|pool| pool.borrow_mut().clear());
    }

    /// Adversarial regression test for frankensqlite#95: forward scan
    /// across a deep multi-level table B-tree that contains a manually
    /// seeded EMPTY subtree must terminate without re-reading pages
    /// indefinitely.
    ///
    /// The reporter's diagnosis was that when the recursive recovery in
    /// `advance_next_impl` hit `move_to_leftmost_leaf` returning false
    /// (e.g. for an empty subtree), the cursor restored `resume_stack`,
    /// cleared `at_eof`, and re-entered `advance_next_impl`. With the
    /// previous implementation this could fail to advance the parent
    /// pointer past the exhausted subtree, leading to a hang. This test
    /// constructs an explicit tree with an empty middle subtree and
    /// asserts that the forward scan terminates and visits every leaf
    /// row exactly once.
    #[test]
    fn test_advance_next_terminates_on_multi_level_with_empty_subtree_frankensqlite_95() {
        // Hand-crafted depth-3 tree:
        //   root (interior, page 2): [(page 3, sep=10), (page 4, sep=20)] right_child=page 5
        //   page 3 (leaf):  [1, 5]
        //   page 4 (interior, no cells): right_child = page 6 (EMPTY leaf)
        //   page 5 (leaf):  [30, 40]
        //   page 6 (leaf):  []   <-- the adversarial empty subtree
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10), (pn(4), 20)], pn(5)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store.pages.insert(4, build_interior_table(&[], pn(6)));
        store
            .pages
            .insert(5, build_leaf_table(&[(30, b"thirty"), (40, b"forty")]));
        store.pages.insert(6, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let started = Instant::now();
        let budget = Duration::from_secs(10);
        let mut observed: Vec<i64> = Vec::new();

        assert!(cursor.first(&cx).unwrap());
        observed.push(cursor.rowid(&cx).unwrap());
        let mut iterations: usize = 0;
        loop {
            iterations += 1;
            assert!(
                iterations <= 32,
                "advance_next called more than 32 times for 4-row tree — \
                 forward-progress invariant violated (frankensqlite#95)"
            );
            assert!(
                started.elapsed() < budget,
                "forward scan did not terminate (frankensqlite#95 hang)"
            );
            if !cursor.next(&cx).unwrap() {
                break;
            }
            observed.push(cursor.rowid(&cx).unwrap());
        }

        assert_eq!(
            observed,
            vec![1_i64, 5, 30, 40],
            "forward scan must visit every leaf row exactly once, in order, \
             even when the tree contains an empty subtree (frankensqlite#95)"
        );
    }

    /// Regression test for frankensqlite#95: forward scan of a multi-level
    /// table B-tree must terminate in O(n) and visit every row exactly once.
    ///
    /// Background:
    /// `BtCursor::advance_next_impl` and `move_to_leftmost_leaf` are mutually
    /// recursive. When `move_to_leftmost_leaf` returns `false` for a subtree
    /// (signalling EOF found *inside* the recursion), the table-leaf-exhausted
    /// branch at cursor.rs:4350-4352 (and the interior-page branch at
    /// cursor.rs:4390-4392) used to restore the resume_stack, clear
    /// `at_eof`, and recurse into `advance_next_impl` from the same parent
    /// position. With a multi-level B-tree this could fail to advance the
    /// parent and re-descend the same subtree forever — the symptom was a
    /// `cass index --full` that pinned one core at 100%, with `/proc/<pid>/io`
    /// showing `rchar` plateau and `read_bytes = 0` (re-reading cached pages).
    ///
    /// This test builds a deep multi-level table B-tree (root interior page
    /// pointing at interior pages pointing at leaves — depth >= 3), then
    /// performs a full forward scan via `first()`/`next()` and asserts:
    ///   1. The scan terminates in well under the wall-clock budget.
    ///   2. Every rowid is visited exactly once, in ascending order.
    ///   3. No infinite loop in `advance_next_impl`.
    #[test]
    fn test_advance_next_terminates_on_multi_level_table_btree_frankensqlite_95() {
        let cx = Cx::new();
        let root = pn(2);
        // Small usable size forces page splits at lower row counts, building
        // a depth >= 3 table B-tree (root interior -> interior -> leaf), which
        // is the multi-level regime the cass `messages` table reaches at
        // ~6_000 rows and which the report identifies as the trigger for
        // frankensqlite#95.
        const SMALL_USABLE: u32 = 512;
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let mut cursor = BtCursor::new(store, root, SMALL_USABLE, true);

        const ROWS: i64 = 6_000;
        const PAYLOAD_LEN: usize = 200;
        let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xFF) as u8).collect();

        for rowid in 1..=ROWS {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        // Confirm we actually have a multi-level tree (root must be interior),
        // and that the depth is at least 3 (root -> interior -> leaf).
        let mut saw_deeper_interior = false;
        {
            let root_page = cursor.pager.pages.get(&root.get()).unwrap();
            let root_header = BtreePageHeader::parse(root_page, 0).unwrap();
            assert_eq!(
                root_header.page_type,
                cell::BtreePageType::InteriorTable,
                "test must build a multi-level B-tree to exercise frankensqlite#95"
            );

            // Walk root's children directly from the raw page image
            // (avoid touching the cursor's stack before the scan starts).
            let header_offset = cell::header_offset_for_page(root);
            let mut cell_pointers: Vec<u16> = Vec::new();
            cell::read_cell_pointers_into(
                root_page.as_slice(),
                &root_header,
                header_offset,
                &mut cell_pointers,
            )
            .unwrap();
            let total_children = root_header.cell_count + 1;
            for child_idx in 0..total_children {
                let child = if child_idx < root_header.cell_count {
                    let cell_offset = usize::from(cell_pointers[usize::from(child_idx)]);
                    let bytes = &root_page.as_slice()[cell_offset..cell_offset + 4];
                    PageNumber::new(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                        .unwrap()
                } else {
                    root_header.right_child.unwrap()
                };
                let cp = cursor.pager.pages.get(&child.get()).unwrap();
                let ch = BtreePageHeader::parse(cp, 0).unwrap();
                if matches!(ch.page_type, cell::BtreePageType::InteriorTable) {
                    saw_deeper_interior = true;
                    break;
                }
            }
        }
        assert!(
            saw_deeper_interior,
            "test must build a depth >= 3 B-tree to exercise frankensqlite#95"
        );

        // Bounded wall-clock budget. A correct O(n) scan of 6_000 rows is
        // far under 10s even on a slow CI host; an infinite re-read loop
        // will blow through this immediately.
        let budget = Duration::from_secs(20);
        let started = Instant::now();
        // Bound the number of `next()` calls too, as a belt-and-suspenders
        // safety net in case `next()` returns true forever without making
        // progress.
        let max_iterations = (ROWS as usize) * 4;
        let mut observed: Vec<i64> = Vec::with_capacity(ROWS as usize);

        assert!(cursor.first(&cx).unwrap(), "tree must be non-empty");
        observed.push(cursor.rowid(&cx).unwrap());

        let mut iterations = 0usize;
        loop {
            iterations += 1;
            assert!(
                iterations <= max_iterations,
                "advance_next called more than {} times for {} rows — \
                 forward-progress invariant violated (frankensqlite#95)",
                max_iterations,
                ROWS
            );
            assert!(
                started.elapsed() < budget,
                "forward scan did not terminate within {:?} \
                 (frankensqlite#95 multi-level B-tree hang)",
                budget
            );

            if !cursor.next(&cx).unwrap() {
                break;
            }
            observed.push(cursor.rowid(&cx).unwrap());
        }

        // The scan must visit every rowid exactly once, in ascending order.
        assert_eq!(
            observed.len(),
            ROWS as usize,
            "forward scan visited {} rows, expected {}",
            observed.len(),
            ROWS
        );
        for (i, rowid) in observed.iter().enumerate() {
            assert_eq!(
                *rowid,
                (i as i64) + 1,
                "rowid mismatch at scan position {i} (frankensqlite#95)"
            );
        }
    }

    /// Symmetric reverse-direction regression for frankensqlite#95:
    /// `BtCursor::prev` across a multi-level table B-tree with a manually
    /// seeded EMPTY subtree must terminate and visit every leaf row exactly
    /// once in descending order. The pre-fix `advance_prev` used a
    /// recursive recovery (`return self.advance_prev(cx);`) without
    /// snapshotting a `resume_stack`, so any future change that leaves
    /// the cursor in `(stack_empty, at_eof=false)` after a failed inner
    /// `move_to_rightmost_leaf` would re-trigger the rightmost re-seek
    /// from `root_page` and replay rows. This test pins reverse
    /// forward-progress as an invariant of the iterative rewrite.
    #[test]
    fn test_advance_prev_terminates_on_multi_level_with_empty_subtree_frankensqlite_95() {
        // Mirror of the forward test: depth-3 tree with an empty middle
        // subtree under page 4 (right-child = page 6, leaf with no cells).
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_table(&[(pn(3), 10), (pn(4), 20)], pn(5)));
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"one"), (5, b"five")]));
        store.pages.insert(4, build_interior_table(&[], pn(6)));
        store
            .pages
            .insert(5, build_leaf_table(&[(30, b"thirty"), (40, b"forty")]));
        store.pages.insert(6, build_leaf_table(&[]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(PrefetchProbeStore::new(store), pn(2), USABLE, true);

        let started = Instant::now();
        let budget = Duration::from_secs(10);
        let mut observed: Vec<i64> = Vec::new();

        assert!(cursor.last(&cx).unwrap());
        observed.push(cursor.rowid(&cx).unwrap());
        let mut iterations: usize = 0;
        loop {
            iterations += 1;
            assert!(
                iterations <= 32,
                "advance_prev called more than 32 times for 4-row tree — \
                 reverse forward-progress invariant violated (frankensqlite#95)"
            );
            assert!(
                started.elapsed() < budget,
                "reverse scan did not terminate (frankensqlite#95 hang)"
            );
            if !cursor.prev(&cx).unwrap() {
                break;
            }
            observed.push(cursor.rowid(&cx).unwrap());
        }

        assert_eq!(
            observed,
            vec![40_i64, 30, 5, 1],
            "reverse scan must visit every leaf row exactly once, in \
             descending order, even when the tree contains an empty subtree \
             (frankensqlite#95)"
        );
    }

    /// Symmetric reverse-direction regression for frankensqlite#95:
    /// reverse scan of a multi-level table B-tree must terminate in O(n)
    /// and visit every row exactly once. Mirrors the forward 6_000-row
    /// test.
    #[test]
    fn test_advance_prev_terminates_on_multi_level_table_btree_frankensqlite_95() {
        let cx = Cx::new();
        let root = pn(2);
        const SMALL_USABLE: u32 = 512;
        let store = MemPageStore::with_empty_table(root, SMALL_USABLE);
        let mut cursor = BtCursor::new(store, root, SMALL_USABLE, true);

        const ROWS: i64 = 6_000;
        const PAYLOAD_LEN: usize = 200;
        let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xFF) as u8).collect();

        for rowid in 1..=ROWS {
            cursor.table_insert(&cx, rowid, &payload).unwrap();
        }

        let budget = Duration::from_secs(20);
        let started = Instant::now();
        let max_iterations = (ROWS as usize) * 4;
        let mut observed: Vec<i64> = Vec::with_capacity(ROWS as usize);

        assert!(cursor.last(&cx).unwrap(), "tree must be non-empty");
        observed.push(cursor.rowid(&cx).unwrap());

        let mut iterations = 0usize;
        loop {
            iterations += 1;
            assert!(
                iterations <= max_iterations,
                "advance_prev called more than {} times for {} rows — \
                 reverse forward-progress invariant violated (frankensqlite#95)",
                max_iterations,
                ROWS
            );
            assert!(
                started.elapsed() < budget,
                "reverse scan did not terminate within {:?} \
                 (frankensqlite#95 multi-level B-tree hang)",
                budget
            );

            if !cursor.prev(&cx).unwrap() {
                break;
            }
            observed.push(cursor.rowid(&cx).unwrap());
        }

        assert_eq!(
            observed.len(),
            ROWS as usize,
            "reverse scan visited {} rows, expected {}",
            observed.len(),
            ROWS
        );
        // Reverse scan should produce rowids in descending order: ROWS, ROWS-1, ..., 1.
        for (i, rowid) in observed.iter().enumerate() {
            assert_eq!(
                *rowid,
                ROWS - (i as i64),
                "rowid mismatch at reverse scan position {i} (frankensqlite#95)"
            );
        }
    }

    /// Adversarial regression test for `BtCursor::advance_prev` interior-page
    /// pop-and-recurse path (frankensqlite#95 secondary defect).
    ///
    /// Setup is a 3-level index B-tree whose leftmost path descends through
    /// a chain of interior pages whose `cell_idx == 0` slot points to a
    /// chain that eventually bottoms out in an empty leaf:
    ///
    /// ```text
    ///   pn2 (root, interior):  cells = [(pn3, "c")],  right_child = pn4
    ///   pn3 (interior):        cells = [(pn5, "b")],  right_child = pn6
    ///   pn5 (interior):        cells = [],            right_child = pn7
    ///   pn7 (empty leaf)
    ///   pn6 (leaf):            ["bb"]
    ///   pn4 (leaf):            ["d"]
    /// ```
    ///
    /// Expected reverse-scan visit order, in keyspace, is:
    ///   `["d", "c", "bb", "b"]`
    ///
    /// The bug exercised by this test:
    /// when the cursor is sitting on separator `pn3.c_0 = "b"` (after a
    /// prior leaf-recovery decremented pn3.cell_idx from 1 to 0) and the
    /// caller invokes `prev()`, the interior branch of `advance_prev`
    /// descends `child_page_at(pn3, 0) = pn5`, which exhausts via
    /// `move_to_rightmost_leaf(pn5) -> advance_prev (leaf-recovery)` and
    /// pops the entire stack including pn2. Restoring the snapshot puts
    /// pn3.cell_idx back to 0 with pn2.cell_idx still 0. With `cell_idx == 0`
    /// the interior branch pops pn3 and recursively calls `advance_prev`,
    /// which lands on pn2 at cell_idx=0 (still on separator "c" from the
    /// outer caller's perspective). The recursive call enters the interior
    /// branch at pn2.cell_idx=0 and descends `child_page_at(pn2, 0) = pn3`
    /// again — re-walking the rightmost path of pn3 and returning "bb"
    /// (which was already returned earlier in the reverse scan).
    ///
    /// Correct semantics: after "b", reverse scan should land on EOF
    /// (return `false`), because nothing in the tree has a key < "b".
    #[test]
    fn test_advance_prev_does_not_replay_after_interior_pop_recurse_frankensqlite_95() {
        let mut store = MemPageStore::new(USABLE);
        store
            .pages
            .insert(2, build_interior_index(&[(pn(3), b"c")], pn(4)));
        store
            .pages
            .insert(3, build_interior_index(&[(pn(5), b"b")], pn(6)));
        store.pages.insert(5, build_interior_index(&[], pn(7)));
        store.pages.insert(7, build_leaf_index(&[]));
        store.pages.insert(6, build_leaf_index(&[b"bb"]));
        store.pages.insert(4, build_leaf_index(&[b"d"]));

        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, pn(2), USABLE, false);

        let started = Instant::now();
        let budget = Duration::from_secs(5);
        let mut observed: Vec<Vec<u8>> = Vec::new();

        assert!(cursor.last(&cx).unwrap());
        observed.push(cursor.payload(&cx).unwrap());

        let mut iterations: usize = 0;
        loop {
            iterations += 1;
            assert!(
                iterations <= 32,
                "advance_prev called more than 32 times for a 4-entry index — \
                 reverse forward-progress invariant violated (frankensqlite#95)"
            );
            assert!(
                started.elapsed() < budget,
                "reverse scan did not terminate (frankensqlite#95 hang)"
            );
            if !cursor.prev(&cx).unwrap() {
                break;
            }
            observed.push(cursor.payload(&cx).unwrap());
        }

        // The bug, if present, would either (a) hang (caught by the
        // iteration cap above), or (b) replay "bb" after "b" and continue
        // into a second pass — producing more than 4 visits or an
        // out-of-order sequence.
        assert_eq!(
            observed,
            vec![b"d".to_vec(), b"c".to_vec(), b"bb".to_vec(), b"b".to_vec(),],
            "reverse scan over a multi-level index B-tree with an empty \
             leftmost-path subtree must visit each entry exactly once in \
             descending order — interior-pop-and-recurse must not re-descend \
             the same subtree (frankensqlite#95)"
        );
    }
}
