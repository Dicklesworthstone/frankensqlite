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

use crate::pager::SimpleTransaction;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageData, PageNumber, PageSize};
#[cfg(all(feature = "native", target_os = "linux"))]
use fsqlite_vfs::IoUringVfs;
use fsqlite_vfs::MemoryVfs;
#[cfg(all(feature = "native", unix))]
use fsqlite_vfs::UnixVfs;
#[cfg(all(feature = "native", target_os = "windows"))]
use fsqlite_vfs::WindowsVfs;
use fsqlite_wal::{
    TransactionConflictPageBaseline, TransactionConflictSnapshot, WalGenerationIdentity,
    checksum::WalChecksumTransform,
};

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
    /// The mode the caller originally requested.
    pub requested_mode: CheckpointMode,
    /// The mode actually executed (may differ from `requested_mode` if the
    /// pager conservatively downgraded due to safety constraints).
    pub effective_mode: CheckpointMode,
}

/// Public summary of the commit-published WAL visibility plane.
///
/// This lets callers bind to generation-stamped WAL metadata without reaching
/// into backend-specific page-index storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalPublicationSnapshot {
    /// Monotonic publication sequence for this backend handle.
    pub publication_seq: u64,
    /// WAL generation visible through this publication.
    pub generation: WalGenerationIdentity,
    /// Latest visible commit frame for this generation, if any.
    pub last_commit_frame: Option<usize>,
    /// Number of committed transactions visible through this publication.
    pub commit_count: u64,
    /// Number of latest-frame entries published in the visibility map.
    pub latest_frame_entries: usize,
    /// Whether the page index is partial and may fall back to bounded scans.
    pub index_is_partial: bool,
}

impl WalPublicationSnapshot {
    #[must_use]
    pub const fn lookup_contract_is_authoritative(self) -> bool {
        !self.index_is_partial
    }
}

/// Backend interface for WAL operations consumed by the pager.
///
/// This trait breaks the `pager ↔ wal` circular dependency: it is defined
/// here in `fsqlite-pager` but implemented by an adapter in `fsqlite-core`
/// that wraps `WalFile` from `fsqlite-wal`.
///
/// The pager calls into this trait during WAL-mode commits and page lookups
/// instead of writing a rollback journal.
pub trait WalBackend: Send + Sync {
    /// Prepare WAL state for a newly-started transaction.
    ///
    /// Implementations may refresh internal snapshot metadata so reads during
    /// this transaction see a coherent view without per-page refresh costs.
    fn begin_transaction(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Capture the currently published WAL visibility summary for this handle.
    ///
    /// Backends that do not maintain a commit-published visibility plane may
    /// return `None`.
    #[must_use]
    fn published_snapshot(&self) -> Option<WalPublicationSnapshot> {
        None
    }

    /// Capture the currently pinned read snapshot for this handle, if any.
    ///
    /// Backends that do not pin generation-stamped read snapshots may return
    /// `None`.
    #[must_use]
    fn pinned_read_snapshot(&self) -> Option<WalPublicationSnapshot> {
        None
    }

    /// Refresh the published WAL visibility summary without pinning a new
    /// read transaction.
    ///
    /// The default implementation reports the current published snapshot
    /// unchanged.
    fn refresh_published_snapshot(&mut self, _cx: &Cx) -> Result<Option<WalPublicationSnapshot>> {
        Ok(self.published_snapshot())
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
        &self,
        _frames: &[WalFrameRef<'_>],
    ) -> Result<Option<PreparedWalFrameBatch>> {
        Ok(None)
    }

    /// Optionally finalize a prepared batch before the serialized append.
    ///
    /// Backends can use this hook to move seed-dependent checksum stamping or
    /// similar pure compute out of the exclusive publish window. Callers must
    /// still tolerate the backend redoing that work later if the live append
    /// state changed before the actual write.
    fn finalize_prepared_frames(
        &self,
        _cx: &Cx,
        _prepared: &mut PreparedWalFrameBatch,
    ) -> Result<()> {
        Ok(())
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

    /// Read a page from the WAL using a previously pinned read snapshot.
    ///
    /// This method takes `&self` instead of `&mut self`, enabling callers to
    /// hold only a shared (read) lock on the WAL backend when the transaction
    /// has already pinned its snapshot via `begin_transaction`.
    ///
    /// The default implementation falls back to `read_page(&mut self)` which
    /// requires exclusive access. Implementors that can serve reads from an
    /// immutable pinned snapshot should override this to avoid contention with
    /// the append path.
    ///
    /// # bd-db300.3.8.7: write-lock-scope narrowing
    fn read_page_pinned(&self, _cx: &Cx, _page_number: u32) -> Result<Option<Vec<u8>>> {
        // Default: signal that the implementation doesn't support pinned reads.
        // Callers must fall back to read_page(&mut self) via write lock.
        Err(FrankenError::internal(
            "read_page_pinned not supported by this WalBackend; use read_page",
        ))
    }

    /// Whether this backend supports `read_page_pinned` (shared-lock reads).
    ///
    /// Callers check this before choosing the read vs write lock path.
    fn supports_pinned_reads(&self) -> bool {
        false
    }

    /// Count committed transactions that occur after the latest committed
    /// frame for `page_number` in the current visible WAL snapshot.
    ///
    /// This lets the pager derive an exact visible commit sequence even when a
    /// WAL commit does not need to rewrite page 1. Implementations may return
    /// 0 when they cannot provide a more precise answer.
    fn committed_txns_since_page(&mut self, _cx: &Cx, _page_number: u32) -> Result<u64> {
        Ok(0)
    }

    /// Return conflict pages that were committed after `snapshot`.
    ///
    /// This is the cross-process half of first-committer-wins. The
    /// connection-local MVCC registry protects writers in one process, but a
    /// WAL flusher can also receive batches from transactions whose stale page
    /// images race with commits made by another process. Implementations that
    /// can inspect the WAL frame stream should reject those stale batches
    /// before append.
    fn conflicting_pages_since_snapshot(
        &mut self,
        _cx: &Cx,
        _snapshot: TransactionConflictSnapshot,
        _page_numbers: &[u32],
        _page_baselines: &[TransactionConflictPageBaseline],
    ) -> Result<Vec<u32>> {
        Ok(Vec::new())
    }

    /// Count committed transactions visible in the current WAL snapshot.
    ///
    /// This lets the pager derive a connection-local visible commit sequence
    /// from the durable database header change-counter plus the currently
    /// visible WAL commit horizon, without depending on whether page 1 was
    /// rewritten in recent WAL commits.
    fn committed_txn_count(&mut self, _cx: &Cx) -> Result<u64> {
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
/// Alias the canonical WAL transform type so prepared batches can flow through
/// finalize/append paths without a per-frame transform copy.
pub type PreparedWalChecksumTransform = WalChecksumTransform;

/// Rolling-checksum seed/result captured for a prepared WAL batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PreparedWalChecksumSeed {
    /// First checksum word.
    pub s1: u32,
    /// Second checksum word.
    pub s2: u32,
}

/// Live WAL state that a prepared batch was finalized against.
///
/// This lets the append path cheaply decide whether a pre-lock finalize pass
/// is still valid once the serialized publish window opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PreparedWalFinalizationState {
    /// WAL checkpoint sequence for the generation being appended to.
    pub checkpoint_seq: u32,
    /// WAL salt1 for the generation being appended to.
    pub salt1: u32,
    /// WAL salt2 for the generation being appended to.
    pub salt2: u32,
    /// Frame index where this batch expects to start appending.
    pub start_frame_index: usize,
    /// Rolling checksum seed seen before finalizing this batch.
    pub seed: PreparedWalChecksumSeed,
}

/// Owned WAL batch representation that can be prepared before append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedWalFrameBatch {
    /// Byte width of each serialized frame record.
    pub frame_size: usize,
    /// Offset of the page payload inside each serialized frame record.
    pub page_data_offset: usize,
    /// Whether checksum words use big-endian encoding for transform derivation.
    pub big_endian_checksum: bool,
    /// Per-frame metadata in order.
    pub frame_metas: Vec<PreparedWalFrameMeta>,
    /// Per-frame checksum transforms in order.
    pub checksum_transforms: Vec<PreparedWalChecksumTransform>,
    /// Serialized frame bytes in order.
    pub frame_bytes: Vec<u8>,
    /// Offset of the last commit frame inside this batch, if any.
    pub last_commit_frame_offset: Option<usize>,
    /// WAL state that `frame_bytes` were last finalized against.
    pub finalized_for: Option<PreparedWalFinalizationState>,
    /// Final running checksum after the last finalize pass.
    pub finalized_running_checksum: Option<PreparedWalChecksumSeed>,
}

impl PreparedWalFrameBatch {
    /// Number of frames carried by this batch.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frame_metas.len()
    }

    /// Page size carried by each prepared frame.
    #[must_use]
    pub fn page_size(&self) -> usize {
        self.frame_size.saturating_sub(self.page_data_offset)
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

    /// Borrow the full serialized frame record at `index`.
    #[must_use]
    pub fn frame_slice(&self, index: usize) -> &[u8] {
        let frame_start = index * self.frame_size;
        let frame_end = frame_start + self.frame_size;
        &self.frame_bytes[frame_start..frame_end]
    }

    /// Update the commit-marker db-size for one frame and clear stale finalize state.
    pub fn set_db_size_if_commit(&mut self, index: usize, db_size_if_commit: u32) {
        self.frame_metas[index].db_size_if_commit = db_size_if_commit;
        let frame_start = index * self.frame_size;
        let db_size_offset = frame_start + 4;
        self.frame_bytes[db_size_offset..db_size_offset + 4]
            .copy_from_slice(&db_size_if_commit.to_be_bytes());
        self.finalized_for = None;
        self.finalized_running_checksum = None;
    }

    /// Recompute checksum transforms after header-level metadata changes.
    pub fn recompute_checksum_transforms(&mut self) -> Result<()> {
        let page_size = self.page_size();
        self.checksum_transforms = (0..self.frame_count())
            .map(|index| {
                WalChecksumTransform::for_wal_frame(
                    self.frame_slice(index),
                    page_size,
                    self.big_endian_checksum,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.finalized_for = None;
        self.finalized_running_checksum = None;
        Ok(())
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

    /// Whether this pager was opened read-only.
    fn is_readonly(&self) -> bool;

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

    /// Hint that `page_no` is likely to be read soon.
    ///
    /// Implementations should keep this best-effort and non-blocking. It is
    /// purely a latency-hiding hint and must not affect correctness.
    fn prefetch_page_hint(&self, _cx: &Cx, _page_no: PageNumber) {}

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

    /// Temporarily take ownership of an unpublished staged page image.
    ///
    /// This exists for hot B-tree append paths that want to mutate the
    /// transaction's authoritative staged page without cloning a separate
    /// compatibility copy first. Implementations may return `None` when the
    /// staged page is unavailable or has already been published for read reuse.
    fn try_take_staged_page_data(&mut self, _page_no: PageNumber) -> Option<PageData> {
        None
    }

    /// Mutate an unpublished staged page image in place.
    ///
    /// This is the cheapest hot-path option for repeated right-edge writes:
    /// the transaction already owns the authoritative staged page, so callers
    /// can patch it without removing and re-inserting the page in the write-set.
    fn try_mutate_staged_page_data(
        &mut self,
        _page_no: PageNumber,
        _f: &mut dyn FnMut(&mut PageData),
    ) -> bool {
        false
    }

    /// Restore a page image previously taken with `try_take_staged_page_data`.
    ///
    /// The default implementation routes through `write_page_data`, which is
    /// correct but may copy. Implementations can override this to restore the
    /// staged page without extra allocation.
    fn restore_staged_page_data(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        data: PageData,
    ) -> Result<()> {
        self.write_page_data(cx, page_no, data)
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

    /// Commit dirty pages and reset for immediate reuse without destroying
    /// the transaction handle.
    ///
    /// This is a performance optimization for `:memory:` autocommit: instead
    /// of commit + destroy + begin, we commit the write set and clear it for
    /// the next statement while keeping the transaction alive.  The pager's
    /// `writer_active` and `active_transactions` state remain set, avoiding
    /// a full begin/commit ceremony on the next statement.
    ///
    /// Returns `Ok(true)` if the transaction was retained and can be reused.
    /// Returns `Ok(false)` if retention is not supported (falls back to
    /// regular commit semantics — the caller should treat the transaction
    /// as finished).
    ///
    /// Default implementation falls back to regular `commit`.
    fn commit_and_retain(&mut self, cx: &Cx) -> Result<bool> {
        self.commit(cx)?;
        Ok(false)
    }

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

    /// Visible commit sequence bound to this transaction's current snapshot.
    ///
    /// Pager-backed transactions can expose this so upper layers reuse the
    /// transaction's own visibility boundary instead of re-binding against the
    /// global published plane mid-transaction.
    fn published_visible_commit_seq_hint(&self) -> Option<fsqlite_types::CommitSeq> {
        None
    }

    /// Return the full set of pages this transaction would mutate if it
    /// committed right now, including commit-time metadata synthesis such as
    /// freelist trunk rewrites.
    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> {
        Ok(Vec::new())
    }

    /// Return the subset of pending commit pages that must participate in
    /// MVCC conflict tracking for concurrent commit planning.
    ///
    /// Pager-backed implementations may exclude commit-time-only synthetic
    /// metadata pages here when those bytes are reconciled under a serialized
    /// commit critical section and therefore do not represent true
    /// user-visible overlap.
    fn pending_conflict_pages(&self) -> Result<Vec<PageNumber>> {
        self.pending_commit_pages()
    }

    /// Lock-free conservative conflict estimate for commit planning
    /// (bd-3qeu9.4).
    ///
    /// Implementations whose commits can mutate pages outside their explicit
    /// write set (for example, freed pages or freelist metadata) must override
    /// this method with a correctness-preserving superset. A shared metadata
    /// page may be used as the conflict token when enumerating every synthesized
    /// metadata page would require the pager-inner lock. The default is suitable
    /// only for implementations whose entire mutation surface is represented by
    /// `write_set_page_numbers()`.
    ///
    /// This avoids a redundant pager-inner lock acquisition on the commit hot
    /// path. The precise set remains available via `pending_conflict_pages()`
    /// when callers need exact commit-time page synthesis.
    fn pending_conflict_pages_conservative(&self) -> Vec<PageNumber> {
        self.write_set_page_numbers()
    }

    /// Sorted page numbers in the current write set, without locking.
    /// Default returns empty; pager-backed implementations override.
    fn write_set_page_numbers(&self) -> Vec<PageNumber> {
        Vec::new()
    }

    /// Whether page 1 is currently part of this transaction's pending commit
    /// surface, including commit-time allocator/header synthesis.
    fn page_one_in_pending_commit_surface(&self) -> Result<bool> {
        Ok(self.pending_commit_pages()?.contains(&PageNumber::ONE))
    }

    /// Returns the transaction's effective database page size.
    ///
    /// Real pager-backed transactions override this so upper layers can
    /// normalize owned page buffers before staging them in MVCC state.
    fn page_size(&self) -> PageSize {
        PageSize::default()
    }

    /// Whether calling [`allocate_page`](Self::allocate_page) right now must
    /// add page 1 to the MVCC conflict surface before the underlying allocator
    /// state changes.
    ///
    /// Real pager-backed transactions override this with exact allocator
    /// semantics so upper layers can avoid false page-1 conflicts on net-zero
    /// allocator churn or commit-time-only metadata updates. The default
    /// remains conservative.
    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> {
        Ok(true)
    }

    /// Whether calling [`free_page`](Self::free_page) for `page_no` right now
    /// must add page 1 to the MVCC conflict surface before the underlying
    /// allocator state changes.
    ///
    /// Real pager-backed transactions override this with exact allocator
    /// semantics so upper layers can avoid false page-1 conflicts on net-zero
    /// allocator churn or commit-time-only metadata updates. The default
    /// remains conservative.
    fn free_page_requires_page_one_conflict_tracking(&self, _page_no: PageNumber) -> Result<bool> {
        Ok(true)
    }

    /// Whether calling [`write_page`](Self::write_page) or
    /// [`write_page_data`](Self::write_page_data) for `page_no` right now must
    /// add page 1 to the MVCC conflict surface before the underlying page
    /// state changes.
    ///
    /// Real pager-backed transactions override this with exact growth
    /// semantics so upper layers can defer page-1 tracking until a newly
    /// allocated high page actually becomes part of the pending commit
    /// surface. The default remains conservative.
    fn write_page_requires_page_one_conflict_tracking(&self, _page_no: PageNumber) -> Result<bool> {
        Ok(true)
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

    fn is_readonly(&self) -> bool {
        false
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

    fn is_readonly(&self) -> bool {
        false
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
        let page_size = fsqlite_types::PageSize::default().as_usize();
        let mut page = vec![0_u8; page_size];
        let copy_len = data.len().min(page_size);
        page[..copy_len].copy_from_slice(&data.as_bytes()[..copy_len]);
        self.pages.insert(page_no, PageData::from_vec(page));
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

/// Stack-allocated transaction wrapper used by upper layers to avoid boxing
/// pager transactions behind `dyn TransactionHandle`.
pub enum TransactionKind {
    /// In-memory pager transaction (`:memory:` databases).
    Memory(SimpleTransaction<MemoryVfs>),
    /// Linux io_uring pager transaction.
    #[cfg(all(feature = "native", target_os = "linux"))]
    IoUring(SimpleTransaction<IoUringVfs>),
    /// Unix filesystem pager transaction.
    #[cfg(all(feature = "native", unix))]
    Unix(SimpleTransaction<UnixVfs>),
    /// Windows filesystem pager transaction.
    #[cfg(all(feature = "native", target_os = "windows"))]
    Windows(SimpleTransaction<WindowsVfs>),
    /// Generic mock transaction used by cross-crate tests.
    Mock(MockTransaction),
    /// In-memory mock transaction used by cross-crate tests.
    MemoryMock(MemoryMockTransaction),
    /// bd-perf: Sentinel used by SharedTxnPageIo::drain() when the real
    /// transaction is extracted while retaining cursor Rc references.
    /// Any page read/write through this variant panics — it should only
    /// exist transiently between drain and the next refill.
    Drained,
}

impl std::fmt::Debug for TransactionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(_) => f.write_str("TransactionKind::Memory"),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(_) => f.write_str("TransactionKind::IoUring"),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(_) => f.write_str("TransactionKind::Unix"),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(_) => f.write_str("TransactionKind::Windows"),
            Self::Mock(_) => f.write_str("TransactionKind::Mock"),
            Self::MemoryMock(_) => f.write_str("TransactionKind::MemoryMock"),
            Self::Drained => f.write_str("TransactionKind::Drained"),
        }
    }
}

impl TransactionKind {
    /// The pager's live free-page set for this transaction (see
    /// [`SimpleTransaction::live_freelist_pages`]). Used by `PRAGMA
    /// integrity_check` (GH#113) to validate page ownership against the
    /// authoritative in-transaction freelist rather than the deferred,
    /// commit-time on-disk trunk. Mock and drained variants have no freelist
    /// projection and return an empty set.
    #[must_use]
    pub fn live_freelist_pages(&self) -> Vec<PageNumber> {
        match self {
            Self::Memory(txn) => txn.live_freelist_pages(),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => txn.live_freelist_pages(),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => txn.live_freelist_pages(),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => txn.live_freelist_pages(),
            Self::Mock(_) | Self::MemoryMock(_) | Self::Drained => Vec::new(),
        }
    }

    /// The in-transaction database size in pages (see
    /// [`SimpleTransaction::live_db_size`]). Used as the page-extent bound by
    /// `PRAGMA integrity_check` (GH#113) so the walk does not flag pages
    /// allocated this transaction as past the end of the database. Mock and
    /// drained variants return 0 (the caller falls back to the published size).
    #[must_use]
    pub fn live_db_size(&self) -> u32 {
        match self {
            Self::Memory(txn) => txn.live_db_size(),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => txn.live_db_size(),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => txn.live_db_size(),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => txn.live_db_size(),
            Self::Mock(_) | Self::MemoryMock(_) | Self::Drained => 0,
        }
    }

    fn with_handle<R>(&self, f: impl FnOnce(&dyn TransactionHandle) -> R) -> R {
        match self {
            Self::Memory(txn) => f(txn),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => f(txn),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => f(txn),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => f(txn),
            Self::Mock(txn) => f(txn),
            Self::MemoryMock(txn) => f(txn),
            Self::Drained => panic!(
                "BUG: TransactionKind::Drained accessed — a retained cursor tried to \
                 read/write pages while the transaction was extracted. This sentinel \
                 only exists between engine.drain_transaction() and the next \
                 engine.refill_transaction(). If you see this, a cursor was accessed \
                 outside the VDBE execution window."
            ),
        }
    }

    fn with_handle_mut<R>(&mut self, f: impl FnOnce(&mut dyn TransactionHandle) -> R) -> R {
        match self {
            Self::Memory(txn) => f(txn),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => f(txn),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => f(txn),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => f(txn),
            Self::Mock(txn) => f(txn),
            Self::MemoryMock(txn) => f(txn),
            Self::Drained => panic!(
                "BUG: TransactionKind::Drained accessed — a retained cursor tried to \
                 read/write pages while the transaction was extracted. This sentinel \
                 only exists between engine.drain_transaction() and the next \
                 engine.refill_transaction(). If you see this, a cursor was accessed \
                 outside the VDBE execution window."
            ),
        }
    }
}

impl From<SimpleTransaction<MemoryVfs>> for TransactionKind {
    fn from(txn: SimpleTransaction<MemoryVfs>) -> Self {
        Self::Memory(txn)
    }
}

#[cfg(all(feature = "native", target_os = "linux"))]
impl From<SimpleTransaction<IoUringVfs>> for TransactionKind {
    fn from(txn: SimpleTransaction<IoUringVfs>) -> Self {
        Self::IoUring(txn)
    }
}

#[cfg(all(feature = "native", unix))]
impl From<SimpleTransaction<UnixVfs>> for TransactionKind {
    fn from(txn: SimpleTransaction<UnixVfs>) -> Self {
        Self::Unix(txn)
    }
}

#[cfg(all(feature = "native", target_os = "windows"))]
impl From<SimpleTransaction<WindowsVfs>> for TransactionKind {
    fn from(txn: SimpleTransaction<WindowsVfs>) -> Self {
        Self::Windows(txn)
    }
}

impl From<MockTransaction> for TransactionKind {
    fn from(txn: MockTransaction) -> Self {
        Self::Mock(txn)
    }
}

impl From<MemoryMockTransaction> for TransactionKind {
    fn from(txn: MemoryMockTransaction) -> Self {
        Self::MemoryMock(txn)
    }
}

impl sealed::Sealed for TransactionKind {}

impl TransactionHandle for TransactionKind {
    // These TransactionKind dispatch sites show up in self-time profiles.
    // Routing them through `with_handle` / `with_handle_mut` coerces the
    // concrete `&SimpleTransaction<V>` into `&dyn TransactionHandle` inside the
    // closure, so every call pays a vtable lookup. Inlining the match here lets
    // LLVM see the concrete type and dispatch statically; the rest of
    // `with_handle`'s callers are cold or shape-uniform enough to keep sharing
    // the smaller helper.
    fn get_page(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        match self {
            Self::Memory(txn) => txn.get_page(cx, page_no),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => txn.get_page(cx, page_no),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => txn.get_page(cx, page_no),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => txn.get_page(cx, page_no),
            Self::Mock(txn) => txn.get_page(cx, page_no),
            Self::MemoryMock(txn) => txn.get_page(cx, page_no),
            Self::Drained => panic!(
                "BUG: TransactionKind::Drained accessed in get_page — a retained \
                 cursor tried to read pages while the transaction was extracted."
            ),
        }
    }

    fn prefetch_page_hint(&self, cx: &Cx, page_no: PageNumber) {
        self.with_handle(|txn| txn.prefetch_page_hint(cx, page_no));
    }

    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.with_handle_mut(|txn| txn.write_page(cx, page_no, data))
    }

    fn write_page_data(&mut self, cx: &Cx, page_no: PageNumber, data: PageData) -> Result<()> {
        match self {
            Self::Memory(txn) => txn.write_page_data(cx, page_no, data),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => txn.write_page_data(cx, page_no, data),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => txn.write_page_data(cx, page_no, data),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => txn.write_page_data(cx, page_no, data),
            Self::Mock(txn) => txn.write_page_data(cx, page_no, data),
            Self::MemoryMock(txn) => txn.write_page_data(cx, page_no, data),
            Self::Drained => panic!(
                "BUG: TransactionKind::Drained accessed in write_page_data — a \
                 retained cursor tried to write pages while the transaction was \
                 extracted."
            ),
        }
    }

    fn try_mutate_staged_page_data(
        &mut self,
        page_no: PageNumber,
        f: &mut dyn FnMut(&mut PageData),
    ) -> bool {
        self.with_handle_mut(|txn| txn.try_mutate_staged_page_data(page_no, f))
    }

    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
        self.with_handle_mut(|txn| txn.allocate_page(cx))
    }

    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
        match self {
            Self::Memory(txn) => txn.free_page(cx, page_no),
            #[cfg(all(feature = "native", target_os = "linux"))]
            Self::IoUring(txn) => txn.free_page(cx, page_no),
            #[cfg(all(feature = "native", unix))]
            Self::Unix(txn) => txn.free_page(cx, page_no),
            #[cfg(all(feature = "native", target_os = "windows"))]
            Self::Windows(txn) => txn.free_page(cx, page_no),
            Self::Mock(txn) => txn.free_page(cx, page_no),
            Self::MemoryMock(txn) => txn.free_page(cx, page_no),
            Self::Drained => panic!(
                "BUG: TransactionKind::Drained accessed in free_page — a retained \
                 cursor tried to free pages while the transaction was extracted."
            ),
        }
    }

    fn commit(&mut self, cx: &Cx) -> Result<()> {
        self.with_handle_mut(|txn| txn.commit(cx))
    }

    fn commit_and_retain(&mut self, cx: &Cx) -> Result<bool> {
        self.with_handle_mut(|txn| txn.commit_and_retain(cx))
    }

    fn is_writer(&self) -> bool {
        self.with_handle(|txn| txn.is_writer())
    }

    fn has_pending_writes(&self) -> bool {
        self.with_handle(|txn| txn.has_pending_writes())
    }

    fn published_visible_commit_seq_hint(&self) -> Option<fsqlite_types::CommitSeq> {
        self.with_handle(|txn| txn.published_visible_commit_seq_hint())
    }

    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> {
        self.with_handle(|txn| txn.pending_commit_pages())
    }

    fn pending_conflict_pages(&self) -> Result<Vec<PageNumber>> {
        self.with_handle(|txn| txn.pending_conflict_pages())
    }

    fn pending_conflict_pages_conservative(&self) -> Vec<PageNumber> {
        self.with_handle(|txn| txn.pending_conflict_pages_conservative())
    }

    fn write_set_page_numbers(&self) -> Vec<PageNumber> {
        self.with_handle(|txn| txn.write_set_page_numbers())
    }

    fn page_one_in_pending_commit_surface(&self) -> Result<bool> {
        self.with_handle(|txn| txn.page_one_in_pending_commit_surface())
    }

    fn page_size(&self) -> PageSize {
        self.with_handle(|txn| txn.page_size())
    }

    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> {
        self.with_handle(|txn| txn.allocate_page_requires_page_one_conflict_tracking())
    }

    fn free_page_requires_page_one_conflict_tracking(&self, page_no: PageNumber) -> Result<bool> {
        self.with_handle(|txn| txn.free_page_requires_page_one_conflict_tracking(page_no))
    }

    fn write_page_requires_page_one_conflict_tracking(&self, page_no: PageNumber) -> Result<bool> {
        self.with_handle(|txn| txn.write_page_requires_page_one_conflict_tracking(page_no))
    }

    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        self.with_handle_mut(|txn| txn.rollback(cx))
    }

    fn record_write_witness(&mut self, cx: &Cx, key: fsqlite_types::WitnessKey) {
        self.with_handle_mut(|txn| txn.record_write_witness(cx, key));
    }

    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.with_handle_mut(|txn| txn.savepoint(cx, name))
    }

    fn release_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.with_handle_mut(|txn| txn.release_savepoint(cx, name))
    }

    fn rollback_to_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.with_handle_mut(|txn| txn.rollback_to_savepoint(cx, name))
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

    const fn test_wal_generation_identity() -> WalGenerationIdentity {
        WalGenerationIdentity {
            checkpoint_seq: 0,
            salts: fsqlite_wal::checksum::WalSalts { salt1: 0, salt2: 0 },
        }
    }

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

    #[test]
    fn test_checkpoint_mode_default_is_passive() {
        assert_eq!(CheckpointMode::default(), CheckpointMode::Passive);
    }

    #[test]
    fn test_journal_mode_default_is_delete() {
        assert_eq!(JournalMode::default(), JournalMode::Delete);
    }

    #[test]
    fn test_wal_publication_snapshot_authoritative_when_index_full() {
        let snap = WalPublicationSnapshot {
            publication_seq: 1,
            generation: test_wal_generation_identity(),
            last_commit_frame: Some(10),
            commit_count: 5,
            latest_frame_entries: 10,
            index_is_partial: false,
        };
        assert!(
            snap.lookup_contract_is_authoritative(),
            "full index must be authoritative"
        );
    }

    #[test]
    fn test_wal_publication_snapshot_not_authoritative_when_partial() {
        let snap = WalPublicationSnapshot {
            publication_seq: 1,
            generation: test_wal_generation_identity(),
            last_commit_frame: None,
            commit_count: 0,
            latest_frame_entries: 0,
            index_is_partial: true,
        };
        assert!(
            !snap.lookup_contract_is_authoritative(),
            "partial index must not be authoritative"
        );
    }

    #[test]
    fn test_prepared_wal_frame_batch_frame_count_and_page_size() {
        let batch = PreparedWalFrameBatch {
            frame_size: 4120,
            page_data_offset: 24,
            big_endian_checksum: false,
            frame_metas: vec![
                PreparedWalFrameMeta {
                    page_number: 1,
                    db_size_if_commit: 0,
                },
                PreparedWalFrameMeta {
                    page_number: 2,
                    db_size_if_commit: 10,
                },
            ],
            checksum_transforms: Vec::new(),
            frame_bytes: vec![0u8; 4120 * 2],
            last_commit_frame_offset: Some(4120),
            finalized_for: None,
            finalized_running_checksum: None,
        };
        assert_eq!(batch.frame_count(), 2);
        assert_eq!(batch.page_size(), 4096);
    }

    #[test]
    fn test_prepared_wal_frame_batch_set_db_size_clears_finalized() {
        let mut batch = PreparedWalFrameBatch {
            frame_size: 32,
            page_data_offset: 8,
            big_endian_checksum: false,
            frame_metas: vec![PreparedWalFrameMeta {
                page_number: 1,
                db_size_if_commit: 0,
            }],
            checksum_transforms: Vec::new(),
            frame_bytes: vec![0u8; 32],
            last_commit_frame_offset: None,
            finalized_for: Some(PreparedWalFinalizationState {
                checkpoint_seq: 1,
                salt1: 0xAA,
                salt2: 0xBB,
                start_frame_index: 0,
                seed: PreparedWalChecksumSeed::default(),
            }),
            finalized_running_checksum: Some(PreparedWalChecksumSeed { s1: 1, s2: 2 }),
        };

        batch.set_db_size_if_commit(0, 42);

        assert_eq!(batch.frame_metas[0].db_size_if_commit, 42);
        assert!(
            batch.finalized_for.is_none(),
            "set_db_size_if_commit must invalidate finalized_for"
        );
        assert!(
            batch.finalized_running_checksum.is_none(),
            "set_db_size_if_commit must invalidate finalized_running_checksum"
        );
        let db_bytes = &batch.frame_bytes[4..8];
        assert_eq!(u32::from_be_bytes(db_bytes.try_into().unwrap()), 42);
    }

    #[test]
    fn test_mock_release_savepoint_unknown_name_returns_error() {
        let pager = MockMvccPager;
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let result = txn.release_savepoint(&cx, "nonexistent");
        assert!(result.is_err(), "releasing unknown savepoint must fail");
    }

    #[test]
    fn test_memory_mock_savepoint_rollback_restores_pages() {
        let pager = MemoryMockMvccPager;
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let p1 = PageNumber::new(1).unwrap();
        let page_size = fsqlite_types::PageSize::default().as_usize();
        let mut data_a = vec![0u8; page_size];
        data_a[0] = 0xAA;
        txn.write_page(&cx, p1, &data_a).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();

        let mut data_b = vec![0u8; page_size];
        data_b[0] = 0xBB;
        txn.write_page(&cx, p1, &data_b).unwrap();
        assert_eq!(txn.get_page(&cx, p1).unwrap().as_bytes()[0], 0xBB);

        txn.rollback_to_savepoint(&cx, "sp1").unwrap();
        assert_eq!(
            txn.get_page(&cx, p1).unwrap().as_bytes()[0],
            0xAA,
            "rollback_to_savepoint must restore page state"
        );
    }

    #[test]
    fn test_transaction_mode_default_trait_contract_is_deferred() {
        assert_eq!(TransactionMode::default(), TransactionMode::Deferred);
    }

    #[test]
    fn test_checkpoint_result_fields() {
        let result = CheckpointResult {
            total_frames: 100,
            frames_backfilled: 80,
            completed: false,
            wal_was_reset: false,
            requested_mode: CheckpointMode::Full,
            effective_mode: CheckpointMode::Passive,
        };
        assert_eq!(result.total_frames, 100);
        assert_eq!(result.frames_backfilled, 80);
        assert!(!result.completed);
        assert_ne!(result.requested_mode, result.effective_mode);
    }

    #[test]
    fn test_journal_mode_debug_clone_copy_eq() {
        let a = JournalMode::Wal;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(JournalMode::Delete, JournalMode::Wal);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("Wal"));
    }

    #[test]
    fn test_checkpoint_result_clone_debug() {
        let result = CheckpointResult {
            total_frames: 50,
            frames_backfilled: 50,
            completed: true,
            wal_was_reset: true,
            requested_mode: CheckpointMode::Truncate,
            effective_mode: CheckpointMode::Truncate,
        };
        let cloned = result.clone();
        assert_eq!(result, cloned);
        let dbg = format!("{result:?}");
        assert!(dbg.contains("CheckpointResult"));
        assert!(dbg.contains("Truncate"));
        assert!(dbg.contains("wal_was_reset"));
    }

    #[test]
    fn test_wal_publication_snapshot_clone_copy_debug() {
        let snap = WalPublicationSnapshot {
            publication_seq: 42,
            generation: test_wal_generation_identity(),
            last_commit_frame: Some(100),
            commit_count: 7,
            latest_frame_entries: 50,
            index_is_partial: false,
        };
        let copied = snap;
        assert_eq!(copied, snap);
        let dbg = format!("{snap:?}");
        assert!(dbg.contains("WalPublicationSnapshot"));
        assert!(dbg.contains("publication_seq"));
        assert!(dbg.contains("42"));
    }

    #[test]
    fn test_checkpoint_mode_all_variants_debug() {
        for (mode, expected) in [
            (CheckpointMode::Passive, "Passive"),
            (CheckpointMode::Full, "Full"),
            (CheckpointMode::Restart, "Restart"),
            (CheckpointMode::Truncate, "Truncate"),
        ] {
            let dbg = format!("{mode:?}");
            assert!(dbg.contains(expected), "expected {expected} in {dbg}");
            let copy = mode;
            assert_eq!(mode, copy);
        }
    }

    #[test]
    fn test_prepared_wal_frame_batch_page_data_and_frame_slice() {
        let frame_size = 32;
        let page_data_offset = 8;
        let mut frame_bytes = vec![0u8; frame_size * 2];
        frame_bytes[8] = 0xAA;
        frame_bytes[frame_size + 8] = 0xBB;

        let batch = PreparedWalFrameBatch {
            frame_size,
            page_data_offset,
            big_endian_checksum: false,
            frame_metas: vec![
                PreparedWalFrameMeta {
                    page_number: 1,
                    db_size_if_commit: 0,
                },
                PreparedWalFrameMeta {
                    page_number: 2,
                    db_size_if_commit: 5,
                },
            ],
            checksum_transforms: Vec::new(),
            frame_bytes,
            last_commit_frame_offset: None,
            finalized_for: None,
            finalized_running_checksum: None,
        };

        assert_eq!(batch.page_data(0)[0], 0xAA);
        assert_eq!(batch.page_data(1)[0], 0xBB);
        assert_eq!(batch.frame_slice(0).len(), frame_size);
        assert_eq!(batch.frame_slice(1).len(), frame_size);

        let refs = batch.frame_refs();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].page_number, 1);
        assert_eq!(refs[1].db_size_if_commit, 5);
        assert_eq!(refs[0].page_data[0], 0xAA);
        assert_eq!(refs[1].page_data[0], 0xBB);
    }

    #[test]
    fn prepared_wal_frame_meta_debug_clone_copy_eq() {
        let a = PreparedWalFrameMeta {
            page_number: 5,
            db_size_if_commit: 0,
        };
        let b = PreparedWalFrameMeta {
            page_number: 5,
            db_size_if_commit: 10,
        };
        let copied = a;
        assert_eq!(copied, a);
        assert_ne!(a, b);
        let dbg = format!("{a:?}");
        assert!(dbg.contains("PreparedWalFrameMeta"));
        assert!(dbg.contains("5"));
    }

    #[test]
    fn prepared_wal_checksum_seed_default_and_eq() {
        let def = PreparedWalChecksumSeed::default();
        assert_eq!(def.s1, 0);
        assert_eq!(def.s2, 0);
        let other = PreparedWalChecksumSeed { s1: 1, s2: 2 };
        assert_ne!(def, other);
        let copied = other;
        assert_eq!(copied, other);
        let dbg = format!("{def:?}");
        assert!(dbg.contains("PreparedWalChecksumSeed"));
    }

    #[test]
    fn prepared_wal_finalization_state_default_and_eq() {
        let def = PreparedWalFinalizationState::default();
        assert_eq!(def.checkpoint_seq, 0);
        assert_eq!(def.salt1, 0);
        assert_eq!(def.salt2, 0);
        assert_eq!(def.start_frame_index, 0);
        assert_eq!(def.seed, PreparedWalChecksumSeed::default());
        let other = PreparedWalFinalizationState {
            checkpoint_seq: 1,
            salt1: 0xAA,
            salt2: 0xBB,
            start_frame_index: 42,
            seed: PreparedWalChecksumSeed { s1: 10, s2: 20 },
        };
        assert_ne!(def, other);
        let copied = other;
        assert_eq!(copied, other);
        let dbg = format!("{other:?}");
        assert!(dbg.contains("PreparedWalFinalizationState"));
    }

    #[test]
    fn transaction_mode_all_variants_debug_copy_eq() {
        let variants = [
            (TransactionMode::Deferred, "Deferred"),
            (TransactionMode::Immediate, "Immediate"),
            (TransactionMode::Exclusive, "Exclusive"),
            (TransactionMode::Concurrent, "Concurrent"),
            (TransactionMode::ReadOnly, "ReadOnly"),
        ];
        for (mode, expected) in &variants {
            let dbg = format!("{mode:?}");
            assert!(dbg.contains(expected), "expected {expected} in {dbg}");
            let copied = *mode;
            assert_eq!(copied, *mode);
        }
        assert_ne!(TransactionMode::Deferred, TransactionMode::Concurrent);
    }

    #[test]
    fn wal_frame_ref_debug_clone_copy() {
        let data = [0xABu8; 16];
        let frame = WalFrameRef {
            page_number: 3,
            page_data: &data,
            db_size_if_commit: 0,
        };
        let copied = frame;
        assert_eq!(copied.page_number, 3);
        assert_eq!(copied.page_data.len(), 16);
        assert_eq!(copied.db_size_if_commit, 0);
        let dbg = format!("{frame:?}");
        assert!(dbg.contains("WalFrameRef"));
    }

    #[test]
    fn mock_checkpoint_page_writer_default_and_trait_methods() {
        let mut writer = MockCheckpointPageWriter;
        let cx = Cx::new();
        let page = PageNumber::new(1).unwrap();
        writer.write_page(&cx, page, &[0u8; 4096]).unwrap();
        writer.truncate(&cx, 10).unwrap();
        writer.sync(&cx).unwrap();
        let dbg = format!("{writer:?}");
        assert!(dbg.contains("MockCheckpointPageWriter"));
    }

    #[test]
    fn transaction_kind_drained_debug() {
        let kind = TransactionKind::Drained;
        let dbg = format!("{kind:?}");
        assert!(dbg.contains("Drained"));
    }

    #[test]
    fn wal_publication_snapshot_authoritative_boundary() {
        let base = WalPublicationSnapshot {
            publication_seq: 1,
            generation: test_wal_generation_identity(),
            last_commit_frame: Some(10),
            commit_count: 5,
            latest_frame_entries: 10,
            index_is_partial: false,
        };
        assert!(base.lookup_contract_is_authoritative());
        let partial = WalPublicationSnapshot {
            index_is_partial: true,
            ..base
        };
        assert!(!partial.lookup_contract_is_authoritative());
    }
}
