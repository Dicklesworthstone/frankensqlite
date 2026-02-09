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

/// Backend interface for WAL operations consumed by the pager.
///
/// This trait breaks the `pager ↔ wal` circular dependency: it is defined
/// here in `fsqlite-pager` but implemented by an adapter in `fsqlite-core`
/// that wraps `WalFile` from `fsqlite-wal`.
///
/// The pager calls into this trait during WAL-mode commits and page lookups
/// instead of writing a rollback journal.
pub trait WalBackend: Send {
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

    /// Look up the latest version of a page in the WAL.
    ///
    /// Scans backwards from the most recent frame. Returns `None` if the
    /// page has no WAL entry (caller should fall through to disk).
    fn read_page(&mut self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>>;

    /// Sync the WAL file to stable storage.
    fn sync(&mut self, cx: &Cx) -> Result<()>;

    /// Number of valid frames currently in the WAL.
    fn frame_count(&self) -> usize;
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

    /// Roll back this transaction, discarding the write-set.
    ///
    /// Rollback is infallible in the MVCC model (we simply discard the
    /// local write-set and release page locks), but returns `Result` for
    /// consistency with the trait surface.
    fn rollback(&mut self, cx: &Cx) -> Result<()>;

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

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

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
}
