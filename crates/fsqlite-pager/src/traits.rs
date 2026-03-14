//! Storage trait hierarchy for MVCC pager and checkpoint operations.
//!
//! This module defines the sealed, internal-only traits that encode
//! MVCC safety invariants. Only the defining crate can implement these
//! traits.
//!
//! # Sealed Trait Discipline (§9)
//!
//! Internal traits use `mod sealed { pub trait Sealed {} }` so that
//! downstream crates cannot provide alternate implementations.
//!
//! - **Sealed:** [`MvccPager`], [`TransactionHandle`], [`CheckpointPageWriter`]
//! - **Open (user-implementable):** `Vfs`, `VfsFile` (in `fsqlite-vfs`)

use std::collections::HashMap;

use fsqlite_error::Result;
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageData, PageNumber};

// ---------------------------------------------------------------------------
// Sealed trait discipline
// ---------------------------------------------------------------------------

/// Sealed trait module — prevents external crates from implementing
/// internal traits that encode MVCC safety invariants.
pub(crate) mod sealed {
    /// Marker trait restricting implementation to this crate.
    pub trait Sealed {}
}

// ---------------------------------------------------------------------------
// Journal mode
// ---------------------------------------------------------------------------

/// The journal mode for database persistence (PRAGMA journal_mode).
///
/// Determines how changes are committed — either through a rollback journal
/// (the default) or through a write-ahead log (WAL mode). WAL mode enables
/// concurrent readers alongside a single writer without blocking.
///
/// Only `Delete` and `Wal` are currently supported; the remaining SQLite
/// journal modes (`Truncate`, `Persist`, `Memory`, `Off`) may be added in
/// future phases.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// Rollback journal — the journal file is deleted after each commit.
    /// This is the default mode.
    #[default]
    Delete,
    /// Write-ahead log — frames are appended to a WAL file; checkpoints
    /// transfer committed pages back to the database. Concurrent readers
    /// see consistent snapshots without blocking the writer.
    Wal,
}

// ---------------------------------------------------------------------------
// WAL backend trait (open, for `fsqlite-core` adapter)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Checkpoint mode (mirrors fsqlite-wal::CheckpointMode without adding a dep)
// ---------------------------------------------------------------------------

/// Checkpoint mode for WAL checkpointing.
///
/// This mirrors `fsqlite_wal::CheckpointMode` but is defined here to avoid
/// a circular dependency between `fsqlite-pager` and `fsqlite-wal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointMode {
    /// PASSIVE: Checkpoint as many frames as possible without blocking.
    /// Does not wait for readers or acquire a write lock.
    #[default]
    Passive,
    /// FULL: Checkpoint all frames, waiting for readers if necessary.
    /// Does not reset the WAL.
    Full,
    /// RESTART: Like FULL, but also resets the WAL after completion.
    Restart,
    /// TRUNCATE: Like RESTART, but also truncates the WAL file to zero.
    Truncate,
}

/// Result of a checkpoint operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointResult {
    /// Number of frames in the WAL before the checkpoint.
    pub total_frames: u32,
    /// Number of frames actually transferred to the database.
    pub frames_backfilled: u32,
    /// Whether the checkpoint completed (all frames transferred).
    pub completed: bool,
    /// Whether the WAL was reset after the checkpoint.
    pub wal_was_reset: bool,
}

/// Backend interface for WAL operations consumed by the pager.
///
/// This trait breaks the `pager ↔ wal` circular dependency: it is defined
/// here in `fsqlite-pager` but implemented by an adapter in `fsqlite-core`
/// that wraps `WalFile` from `fsqlite-wal`.
///
/// The pager calls into this trait during WAL-mode commits and page lookups
/// instead of writing a rollback journal.
pub trait WalBackend: Send {
    /// Prepare WAL state for a newly-started transaction.
    ///
    /// Implementations may refresh internal snapshot metadata so reads during
    /// this transaction see a coherent view without per-page refresh costs.
    fn begin_transaction(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Append a single frame to the WAL.
    ///
    /// `page_number` is the 1-based database page.
    /// `page_data` must be exactly `page_size` bytes.
    /// `db_size_if_commit` is the database size in pages for commit frames,
    /// or 0 for non-commit frames.
    fn append_frame(
        &mut self,
        cx: &Cx,
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> Result<()>;

    /// Append a batch of frames to the WAL.
    ///
    /// The default path preserves existing behavior by delegating to
    /// [`Self::append_frame`] one frame at a time.
    fn append_frames(&mut self, cx: &Cx, frames: &[WalFrameRef<'_>]) -> Result<()> {
        for frame in frames {
            self.append_frame(
                cx,
                frame.page_number,
                frame.page_data,
                frame.db_size_if_commit,
            )?;
        }
        Ok(())
    }

    /// Prepare a batch of frames for a later append.
    ///
    /// Implementations may use this to move pure serialization and copy work
    /// ahead of the serialized append window. Returning `None` keeps the
    /// existing `append_frames` path.
    fn prepare_append_frames(
        &mut self,
        _frames: &[WalFrameRef<'_>],
    ) -> Result<Option<PreparedWalFrameBatch>> {
        Ok(None)
    }

    /// Append a previously prepared frame batch.
    ///
    /// The default path rebuilds borrowed frame refs and delegates back to
    /// [`Self::append_frames`]. Backends that can preserve more pre-serialized
    /// state should override this.
    fn append_prepared_frames(
        &mut self,
        cx: &Cx,
        prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        let frame_refs = prepared.frame_refs();
        self.append_frames(cx, &frame_refs)
    }

    /// Look up the latest version of a page in the current visible WAL snapshot.
    ///
    /// Implementations should prefer an authoritative per-generation lookup
    /// structure for the steady-state path. Any slower fallback path should be
    /// explicit and reserved for exceptional cases such as a deliberately
    /// partial index or recovery-oriented handling.
    fn read_page(&mut self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>>;

    /// Count committed transactions that occur after the latest committed
    /// frame for `page_number` in the current visible WAL snapshot.
    ///
    /// This lets the pager derive an exact visible commit sequence even when a
    /// WAL commit does not need to rewrite page 1. Implementations may return
    /// 0 when they cannot provide a more precise answer.
    fn committed_txns_since_page(&mut self, _cx: &Cx, _page_number: u32) -> Result<u64> {
        Ok(0)
    }

    /// Sync the WAL file to stable storage.
    fn sync(&mut self, cx: &Cx) -> Result<()>;

    /// Number of valid frames currently in the WAL.
    fn frame_count(&self) -> usize;

    /// Run a checkpoint to transfer frames from the WAL to the database.
    ///
    /// Takes a `CheckpointPageWriter` that handles the actual page writes
    /// to the database file. The writer is typically provided by the pager.
    ///
    /// # Arguments
    ///
    /// * `cx` - Cancellation/deadline context
    /// * `mode` - Checkpoint mode (Passive, Full, Restart, Truncate)
    /// * `writer` - Writer to transfer pages to the database file
    /// * `backfilled_frames` - Number of frames already backfilled (for resume)
    /// * `oldest_reader_frame` - Frame index of oldest active reader (None if no readers)
    ///
    /// # Returns
    ///
    /// A `CheckpointResult` describing what was accomplished.
    fn checkpoint(
        &mut self,
        cx: &Cx,
        mode: CheckpointMode,
        writer: &mut dyn CheckpointPageWriter,
        backfilled_frames: u32,
        oldest_reader_frame: Option<u32>,
    ) -> Result<CheckpointResult>;
}

/// Borrowed frame descriptor used for WAL batch appends.
#[derive(Debug, Clone, Copy)]
pub struct WalFrameRef<'a> {
    /// Database page number this frame writes.
    pub page_number: u32,
    /// Page data for the frame. Must be exactly `page_size` bytes.
    pub page_data: &'a [u8],
    /// Database size in pages for commit frames, or 0 for non-commit frames.
    pub db_size_if_commit: u32,
}

/// Metadata describing one frame within a prepared WAL batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedWalFrameMeta {
    /// Database page number this frame writes.
    pub page_number: u32,
    /// Database size in pages for commit frames, or 0 for non-commit frames.
    pub db_size_if_commit: u32,
}

/// Affine checksum transform for one prepared WAL frame.
///
/// The SQLite WAL rolling checksum is linear in the incoming `(s1, s2)` seed.
/// Preparing a frame can therefore precompute the transform coefficients and
/// defer only the final seed rebind to publish time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedWalChecksumTransform {
    /// Contribution of the incoming `s1` to the outgoing `s1`.
    pub a11: u32,
    /// Contribution of the incoming `s2` to the outgoing `s1`.
    pub a12: u32,
    /// Contribution of the incoming `s1` to the outgoing `s2`.
    pub a21: u32,
    /// Contribution of the incoming `s2` to the outgoing `s2`.
    pub a22: u32,
    /// Constant bias added to the outgoing `s1`.
    pub c1: u32,
    /// Constant bias added to the outgoing `s2`.
    pub c2: u32,
}

/// Owned WAL batch representation that can be prepared before append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedWalFrameBatch {
    /// Byte width of each serialized frame record.
    pub frame_size: usize,
    /// Offset of the page payload inside each serialized frame record.
    pub page_data_offset: usize,
    /// Per-frame metadata in order.
    pub frame_metas: Vec<PreparedWalFrameMeta>,
    /// Per-frame checksum transforms in order.
    pub checksum_transforms: Vec<PreparedWalChecksumTransform>,
    /// Serialized frame bytes in order.
    pub frame_bytes: Vec<u8>,
}

impl PreparedWalFrameBatch {
    /// Number of frames carried by this batch.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frame_metas.len()
    }

    /// Borrow this batch as pager-facing frame refs.
    #[must_use]
    pub fn frame_refs(&self) -> Vec<WalFrameRef<'_>> {
        self.frame_metas
            .iter()
            .enumerate()
            .map(|(index, meta)| {
                let frame_start = index * self.frame_size;
                let page_start = frame_start + self.page_data_offset;
                let page_end = frame_start + self.frame_size;
                WalFrameRef {
                    page_number: meta.page_number,
                    page_data: &self.frame_bytes[page_start..page_end],
                    db_size_if_commit: meta.db_size_if_commit,
                }
            })
            .collect()
    }

    /// Borrow the page payload for a prepared frame.
    #[must_use]
    pub fn page_data(&self, index: usize) -> &[u8] {
        let frame_start = index * self.frame_size;
        let page_start = frame_start + self.page_data_offset;
        let page_end = frame_start + self.frame_size;
        &self.frame_bytes[page_start..page_end]
    }
}

// ---------------------------------------------------------------------------
// Transaction mode
// ---------------------------------------------------------------------------

/// How a transaction should be opened.
///
/// Matches SQLite's `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]` semantics
/// adapted for MVCC.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Deferred: starts as read-only, upgrades to writer on first write.
    /// This is the default mode.
    #[default]
    Deferred,
    /// Immediate: acquires write intent at `BEGIN` time. Corresponds to
    /// `BEGIN IMMEDIATE` in SQLite. Under MVCC this takes a reservation
    /// on the serialized writer token.
    Immediate,
    /// Exclusive: like Immediate but also prevents new readers from
    /// starting. Used for schema changes and `VACUUM`.
    Exclusive,
    /// Concurrent: `BEGIN CONCURRENT` mode.
    ///
    /// This is the MVCC concurrent-writer entry point from the SQL layer.
    /// Pager implementations may initially map it to deferred semantics,
    /// but must preserve the mode so upper layers can engage concurrent
    /// conflict detection/commit paths.
    Concurrent,
    /// Read-only: the transaction will never write. The pager can skip
    /// SSI bookkeeping and use a lightweight snapshot.
    ReadOnly,
}

// ---------------------------------------------------------------------------
// MvccPager — primary storage interface
// ---------------------------------------------------------------------------

/// The MVCC-aware page-level storage interface.
///
/// This is the primary interface consumed by the B-tree layer and VDBE.
/// It supports multiple concurrent transactions from different threads,
/// with internal locking (version store `RwLock`, lock table `Mutex`).
///
/// The pager outlives all transactions it creates (via `Arc`).
///
/// # Cx Everywhere
///
/// Every method that touches I/O, acquires locks, or could block accepts
/// `&Cx` for cancellation and deadline propagation (§9 cross-cutting rule).
///
/// # Sealed
///
/// This trait is sealed — only this crate can implement it.
pub trait MvccPager: sealed::Sealed + Send + Sync {
    /// The transaction handle type produced by this pager.
    type Txn: TransactionHandle;

    /// Begin a new transaction.
    ///
    /// Returns a [`TransactionHandle`] that provides page-level access
    /// within the transaction's snapshot. The handle is `Send` so it
    /// can be moved to another thread if needed.
    fn begin(&self, cx: &Cx, mode: TransactionMode) -> Result<Self::Txn>;

    /// Return the current journal mode.
    fn journal_mode(&self) -> JournalMode;

    /// Switch the journal mode.
    ///
    /// Switching from `Delete` to `Wal` requires providing a [`WalBackend`]
    /// via [`set_wal_backend`](Self::set_wal_backend) first; otherwise the
    /// call returns `FrankenError::Unsupported`.
    ///
    /// Returns the mode that is actually in effect after the call.
    fn set_journal_mode(&self, cx: &Cx, mode: JournalMode) -> Result<JournalMode>;

    /// Install a WAL backend for WAL-mode operation.
    ///
    /// The backend is consumed and stored internally. It must be set before
    /// calling `set_journal_mode(Wal)`.
    fn set_wal_backend(&self, backend: Box<dyn WalBackend>) -> Result<()>;
}

// ---------------------------------------------------------------------------
// TransactionHandle
// ---------------------------------------------------------------------------

/// A handle to an active MVCC transaction.
///
/// Provides page-level read/write access scoped to the transaction's
/// snapshot. Dropping a handle without calling [`commit`](Self::commit)
/// implicitly rolls back.
///
/// # Page resolution chain
///
/// `get_page` resolves through: write-set → version chain → disk.
/// SSI `WitnessKey` tracking records which pages were read.
///
/// # Sealed
///
/// This trait is sealed — only this crate can implement it.
pub trait TransactionHandle: sealed::Sealed + Send {
    /// Read a page, resolving through the MVCC version chain.
    ///
    /// Resolution order: local write-set → version chain → on-disk.
    /// Records the read in SSI witness tracking for conflict detection
    /// at commit time.
    fn get_page(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData>;

    /// Write a page within this transaction.
    ///
    /// Acquires a page-level lock and records the write for SSI
    /// validation at commit time.
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()>;

    /// Write owned page data within this transaction.
    ///
    /// The default implementation borrows the page bytes, but implementations
    /// can override this to adopt owned buffers without another copy.
    fn write_page_data(&mut self, cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
        self.write_page(cx, page_no, data.as_bytes())
    }

    /// Allocate a new page and return its page number.
    ///
    /// Searches the freelist first, then extends the database file.
    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber>;

    /// Free a page, returning it to the freelist.
    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()>;

    /// Commit this transaction.
    ///
    /// Performs SSI validation, First-Committer-Wins check, merge ladder,
    /// WAL append, and version publish. Returns `SQLITE_BUSY_SNAPSHOT`
    /// (via `FrankenError::Busy`) on serialization failure.
    fn commit(&mut self, cx: &Cx) -> Result<()>;

    /// Whether this transaction has been upgraded to a writer.
    ///
    /// Read-only and deferred transactions that never dirtied a page must
    /// return `false` so upper layers do not synthesize commit sequences for
    /// no-op commits.
    fn is_writer(&self) -> bool;

    /// Whether this transaction still has net page changes to publish.
    ///
    /// This can become `false` again after `ROLLBACK TO` discards all pending
    /// writes, even if the transaction had previously upgraded to writer mode.
    fn has_pending_writes(&self) -> bool;

    /// Return the full set of pages this transaction would mutate if it
    /// committed right now, including commit-time metadata synthesis such as
    /// freelist trunk rewrites.
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> {
        Ok(Vec::new())
    }

    /// Roll back this transaction, discarding the write-set.
    ///
    /// Rollback is infallible in the MVCC model (we simply discard the
    /// local write-set and release page locks), but returns `Result` for
    /// consistency with the trait surface.
    fn rollback(&mut self, cx: &Cx) -> Result<()>;

    /// Record a granular write witness for fine-grained SSI bookkeeping.
    ///
    /// Simple pager-backed transactions may ignore this, but concurrent MVCC
    /// implementations can override it to feed witness-plane validation.
    fn record_write_witness(&mut self, _cx: &Cx, _key: fsqlite_types::WitnessKey) {}

    /// Create a named savepoint, snapshotting the current write-set.
    ///
    /// Corresponds to SQL `SAVEPOINT name`. The snapshot captures the
    /// write-set and freed-pages state at this point so that
    /// [`rollback_to_savepoint`](Self::rollback_to_savepoint) can restore it.
    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()>;

    /// Release (collapse) a named savepoint without rolling back.
    ///
    /// Corresponds to SQL `RELEASE name`. All changes since the savepoint
    /// are kept, and the savepoint is removed from the stack. Savepoints
    /// created after the named one are also released.
    fn release_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()>;

    /// Roll back to a named savepoint, restoring the snapshotted state.
    ///
    /// Corresponds to SQL `ROLLBACK TO name`. The write-set and freed-pages
    /// are restored to their state at the time the savepoint was created.
    /// The savepoint itself is retained (it can be rolled back to again).
    /// Savepoints created after the named one are discarded.
    fn rollback_to_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// CheckpointPageWriter
// ---------------------------------------------------------------------------

/// A write-back interface used during WAL checkpointing.
///
/// This trait breaks the `pager ↔ wal` circular dependency: it is
/// defined here in `fsqlite-pager` but passed to `fsqlite-wal` at
/// runtime from `fsqlite-core`.
///
/// # Sealed
///
/// This trait is sealed — only this crate can implement it.
pub trait CheckpointPageWriter: sealed::Sealed + Send {
    /// Write a page directly to the database file (bypassing the cache).
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()>;

    /// Truncate the database file to `n_pages` pages.
    fn truncate(&mut self, cx: &Cx, n_pages: u32) -> Result<()>;

    /// Sync the database file to stable storage.
    fn sync(&mut self, cx: &Cx) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Exported test mocks (cross-crate)
// ---------------------------------------------------------------------------

/// Test/mock pager implementation exported for cross-crate tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockMvccPager;

impl sealed::Sealed for MockMvccPager {}

impl MvccPager for MockMvccPager {
    type Txn = MockTransaction;

    fn begin(&self, _cx: &Cx, _mode: TransactionMode) -> Result<Self::Txn> {
        Ok(MockTransaction {
            committed: false,
            next_page: 2,
            savepoint_names: Vec::new(),
        })
    }

    fn journal_mode(&self) -> JournalMode {
        JournalMode::Delete
    }

    fn set_journal_mode(&self, _cx: &Cx, mode: JournalMode) -> Result<JournalMode> {
        Ok(mode)
    }

    fn set_wal_backend(&self, _backend: Box<dyn WalBackend>) -> Result<()> {
        Ok(())
    }
}

/// Test/mock transaction handle exported for cross-crate tests.
#[derive(Debug, Clone)]
pub struct MockTransaction {
    committed: bool,
    next_page: u32,
    savepoint_names: Vec<String>,
}

impl sealed::Sealed for MockTransaction {}

impl TransactionHandle for MockTransaction {
    fn get_page(&self, _cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        let size = fsqlite_types::PageSize::default();
        let mut data = PageData::zeroed(size);
        // Stamp the page number in the first 4 bytes for test verification.
        data.as_bytes_mut()[..4].copy_from_slice(&page_no.get().to_le_bytes());
        Ok(data)
    }

    fn write_page(&mut self, _cx: &Cx, _page_no: PageNumber, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
        let page = PageNumber::new(self.next_page)
            .expect("mock allocator must always produce non-zero page numbers");
        self.next_page += 1;
        Ok(page)
    }

    fn free_page(&mut self, _cx: &Cx, _page_no: PageNumber) -> Result<()> {
        Ok(())
    }

    fn commit(&mut self, _cx: &Cx) -> Result<()> {
        self.committed = true;
        Ok(())
    }

    fn is_writer(&self) -> bool {
        false
    }

    fn has_pending_writes(&self) -> bool {
        false
    }

    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> {
        Ok(Vec::new())
    }

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: fsqlite_types::WitnessKey) {}

    fn savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        self.savepoint_names.push(name.to_owned());
        Ok(())
    }

    fn release_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        if let Some(pos) = self.savepoint_names.iter().rposition(|n| n == name) {
            self.savepoint_names.truncate(pos);
            Ok(())
        } else {
            Err(fsqlite_error::FrankenError::internal(format!(
                "no savepoint named '{name}'"
            )))
        }
    }

    fn rollback_to_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        if let Some(pos) = self.savepoint_names.iter().rposition(|n| n == name) {
            self.savepoint_names.truncate(pos + 1);
            Ok(())
        } else {
            Err(fsqlite_error::FrankenError::internal(format!(
                "no savepoint named '{name}'"
            )))
        }
    }
}

/// In-memory pager mock exported for cross-crate tests that need zero-filled
/// pages and durable writes within a transaction.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryMockMvccPager;

impl sealed::Sealed for MemoryMockMvccPager {}

impl MvccPager for MemoryMockMvccPager {
    type Txn = MemoryMockTransaction;

    fn begin(&self, _cx: &Cx, _mode: TransactionMode) -> Result<Self::Txn> {
        Ok(MemoryMockTransaction {
            committed: false,
            next_page: 2,
            pages: HashMap::new(),
            savepoints: Vec::new(),
        })
    }

    fn journal_mode(&self) -> JournalMode {
        JournalMode::Delete
    }

    fn set_journal_mode(&self, _cx: &Cx, mode: JournalMode) -> Result<JournalMode> {
        Ok(mode)
    }

    fn set_wal_backend(&self, _backend: Box<dyn WalBackend>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MemoryMockSavepoint {
    name: String,
    next_page: u32,
    pages: HashMap<PageNumber, PageData>,
}

/// In-memory transaction mock that returns zero-filled pages until written and
/// preserves writes for subsequent reads.
#[derive(Debug, Clone)]
pub struct MemoryMockTransaction {
    committed: bool,
    next_page: u32,
    pages: HashMap<PageNumber, PageData>,
    savepoints: Vec<MemoryMockSavepoint>,
}

impl sealed::Sealed for MemoryMockTransaction {}

impl TransactionHandle for MemoryMockTransaction {
    fn get_page(&self, _cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        Ok(self
            .pages
            .get(&page_no)
            .cloned()
            .unwrap_or_else(|| PageData::zeroed(fsqlite_types::PageSize::default())))
    }

    fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.committed = false;
        let page_size = fsqlite_types::PageSize::default().as_usize();
        let mut page = vec![0_u8; page_size];
        let copy_len = data.len().min(page_size);
        page[..copy_len].copy_from_slice(&data[..copy_len]);
        self.pages.insert(page_no, PageData::from_vec(page));
        Ok(())
    }

    fn write_page_data(&mut self, _cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
        self.committed = false;
        self.pages.insert(page_no, data);
        Ok(())
    }

    fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
        self.committed = false;
        let page = PageNumber::new(self.next_page)
            .expect("mock allocator must always produce non-zero page numbers");
        self.next_page += 1;
        self.pages
            .entry(page)
            .or_insert_with(|| PageData::zeroed(fsqlite_types::PageSize::default()));
        Ok(page)
    }

    fn free_page(&mut self, _cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.committed = false;
        self.pages.remove(&page_no);
        Ok(())
    }

    fn commit(&mut self, _cx: &Cx) -> Result<()> {
        self.committed = true;
        Ok(())
    }

    fn is_writer(&self) -> bool {
        !self.pages.is_empty()
    }

    fn has_pending_writes(&self) -> bool {
        !self.committed && !self.pages.is_empty()
    }

    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> {
        let mut pages = self.pages.keys().copied().collect::<Vec<_>>();
        pages.sort_unstable();
        Ok(pages)
    }

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        self.committed = false;
        self.next_page = 2;
        self.pages.clear();
        self.savepoints.clear();
        Ok(())
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: fsqlite_types::WitnessKey) {}

    fn savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        self.savepoints.push(MemoryMockSavepoint {
            name: name.to_owned(),
            next_page: self.next_page,
            pages: self.pages.clone(),
        });
        Ok(())
    }

    fn release_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        if let Some(pos) = self.savepoints.iter().rposition(|sp| sp.name == name) {
            self.savepoints.truncate(pos);
            Ok(())
        } else {
            Err(fsqlite_error::FrankenError::internal(format!(
                "no savepoint named '{name}'"
            )))
        }
    }

    fn rollback_to_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        if let Some(pos) = self.savepoints.iter().rposition(|sp| sp.name == name) {
            let snapshot = self.savepoints[pos].clone();
            self.next_page = snapshot.next_page;
            self.pages = snapshot.pages;
            self.savepoints.truncate(pos + 1);
            Ok(())
        } else {
            Err(fsqlite_error::FrankenError::internal(format!(
                "no savepoint named '{name}'"
            )))
        }
    }
}

/// Test/mock checkpoint writer exported for cross-crate tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockCheckpointPageWriter;

impl sealed::Sealed for MockCheckpointPageWriter {}

impl CheckpointPageWriter for MockCheckpointPageWriter {
    fn write_page(&mut self, _cx: &Cx, _page_no: PageNumber, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    fn truncate(&mut self, _cx: &Cx, _n_pages: u32) -> Result<()> {
        Ok(())
    }

    fn sync(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Unit tests --

    #[test]
    fn test_pager_trait_is_sealed_mock_impl() {
        // This compiles because MockPager is in the same crate.
        // External crates cannot impl Sealed, so they cannot impl MvccPager.
        let pager = MockMvccPager;
        let cx = Cx::new();
        let _txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();
    }

    #[test]
    fn test_mvccpager_begin_commit_rollback_signatures() {
        let pager = MockMvccPager;
        let cx = Cx::new();

        // Begin takes &Cx and returns Result.
        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();

        // All blocking/I/O methods take &Cx and return Result.
        let page_no = PageNumber::new(1).unwrap();
        let data = txn.get_page(&cx, page_no).unwrap();
        assert_eq!(
            u32::from_le_bytes(data.as_bytes()[..4].try_into().unwrap()),
            1
        );

        txn.write_page(&cx, page_no, &[0u8; 4096]).unwrap();
        let new_page = txn.allocate_page(&cx).unwrap();
        assert_eq!(new_page.get(), 2);
        txn.free_page(&cx, new_page).unwrap();

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_transaction_rollback_is_infallible() {
        let pager = MockMvccPager;
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();
        // Rollback should succeed without error.
        txn.rollback(&cx).unwrap();
    }

    #[test]
    fn test_checkpoint_page_writer_signatures() {
        let mut writer = MockCheckpointPageWriter;
        let cx = Cx::new();
        let page1 = PageNumber::new(1).unwrap();

        writer.write_page(&cx, page1, &[0u8; 4096]).unwrap();
        writer.truncate(&cx, 10).unwrap();
        writer.sync(&cx).unwrap();
    }

    #[test]
    fn test_transaction_mode_default_is_deferred() {
        assert_eq!(TransactionMode::default(), TransactionMode::Deferred);
    }

    #[test]
    fn test_open_traits_are_extensible() {
        // Vfs and VfsFile are open traits — external crates CAN implement them.
        // This test is in fsqlite-vfs, but we verify the concept:
        // sealed traits CANNOT be implemented externally.
        // Open traits CAN be implemented externally.
        //
        // Since we can't directly test "external crate fails to compile"
        // in a unit test, we verify that our mock impls compile and work.
        let pager = MockMvccPager;
        let _: &dyn MvccPager<Txn = MockTransaction> = &pager;
    }

    #[test]
    fn test_memory_mock_transaction_persists_writes() {
        let pager = MemoryMockMvccPager;
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_no = PageNumber::new(256).unwrap();

        let mut bytes = vec![0_u8; fsqlite_types::PageSize::default().as_usize()];
        bytes[0] = 0x0A;
        txn.write_page(&cx, page_no, &bytes).unwrap();

        let page = txn.get_page(&cx, page_no).unwrap();
        assert_eq!(page.as_bytes()[0], 0x0A);
        assert!(txn.has_pending_writes());
        assert!(txn.is_writer());
    }

    #[test]
    fn test_memory_mock_transaction_commit_clears_pending_writes() {
        let pager = MemoryMockMvccPager;
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_no = PageNumber::new(2).unwrap();

        txn.write_page(&cx, page_no, &[1_u8; 4096]).unwrap();
        assert!(txn.has_pending_writes());

        txn.commit(&cx).unwrap();
        assert!(
            !txn.has_pending_writes(),
            "committed mock transactions must not report pending writes"
        );
    }

    #[test]
    fn test_memory_mock_transaction_rollback_resets_allocator() {
        let pager = MemoryMockMvccPager;
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        assert_eq!(txn.allocate_page(&cx).unwrap().get(), 2);
        assert_eq!(txn.allocate_page(&cx).unwrap().get(), 3);

        txn.rollback(&cx).unwrap();

        assert_eq!(
            txn.allocate_page(&cx).unwrap().get(),
            2,
            "rollback should restore the mock allocator to its initial state"
        );
    }
}
