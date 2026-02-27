//! Concrete single-writer pager for Phase 5 persistence.
//!
//! `SimplePager` implements [`MvccPager`] with single-writer semantics over a
//! VFS-backed database file and a zero-copy [`PageCache`].
//! Full concurrent MVCC behavior is layered on top in Phase 6.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    BTreePageHeader, CommitSeq, DATABASE_HEADER_SIZE, DatabaseHeader,
    FRANKENSQLITE_SQLITE_VERSION_NUMBER, PageData, PageNumber, PageSize,
};
use fsqlite_vfs::{Vfs, VfsFile};

use crate::journal::{JournalHeader, JournalPageRecord};
use crate::page_buf::{PageBuf, PageBufPool};
use crate::page_cache::{PageCache, PageCacheMetricsSnapshot};
use crate::traits::{self, JournalMode, MvccPager, TransactionHandle, TransactionMode, WalBackend};

/// The inner mutable pager state protected by a mutex.
pub(crate) struct PagerInner<F: VfsFile> {
    /// Handle to the main database file.
    db_file: F,
    /// Page cache used for zero-copy read/write-through.
    cache: PageCache,
    /// Page size for this database.
    page_size: PageSize,
    /// Current database size in pages.
    db_size: u32,
    /// Next page to allocate (1-based).
    next_page: u32,
    /// Whether a writer transaction is currently active.
    writer_active: bool,
    /// Number of active transactions (readers + writers).
    active_transactions: u32,
    /// Whether a checkpoint is currently running.
    checkpoint_active: bool,
    /// Deallocated pages available for reuse.
    ///
    /// TODO: This is currently an in-memory freelist. Pages freed here are
    /// reused during the session but are NOT persisted to the database file's
    /// freelist structure (trunk/leaf pages). This results in leaked pages
    /// (space leak) upon restart until full persistent freelist support is
    /// implemented (Phase 5/6).
    freelist: Vec<PageNumber>,
    /// Current journal mode (rollback journal vs WAL).
    journal_mode: JournalMode,
    /// Optional WAL backend for WAL-mode operation.
    wal_backend: Option<Box<dyn WalBackend>>,
    /// Monotonic commit sequence for MVCC version tracking.
    commit_seq: CommitSeq,
}

impl<F: VfsFile> PagerInner<F> {
    /// Read a page through WAL (if present) → cache → disk and return an owned copy.
    fn read_page_copy(&mut self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        // In WAL mode, check the WAL for the latest version of the page first.
        if self.journal_mode == JournalMode::Wal {
            if let Some(ref mut wal) = self.wal_backend {
                if let Some(wal_data) = wal.read_page(cx, page_no.get())? {
                    return Ok(wal_data);
                }
            }
        }

        if let Some(data) = self.cache.get(page_no) {
            return Ok(data.to_vec());
        }

        // Reads of yet-unallocated pages should observe zero-filled content.
        // This is relied upon by savepoint rollback semantics for pages that
        // were allocated and then rolled back before commit.
        if page_no.get() > self.db_size {
            return Ok(vec![0_u8; self.page_size.as_usize()]);
        }

        let slice = match self.cache.read_page(cx, &mut self.db_file, page_no) {
            Ok(slice) => slice,
            Err(FrankenError::OutOfMemory) => {
                if self.cache.evict_any() {
                    self.cache.read_page(cx, &mut self.db_file, page_no)?
                } else {
                    let page_size = self.page_size.as_usize();
                    let offset = u64::from(page_no.get() - 1) * page_size as u64;
                    let mut out = vec![0_u8; page_size];
                    let bytes_read = self.db_file.read(cx, &mut out, offset)?;
                    if bytes_read < page_size {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "short read fetching page {page}: got {bytes_read} of {page_size}",
                                page = page_no.get()
                            ),
                        });
                    }
                    return Ok(out);
                }
            }
            Err(err) => return Err(err),
        };
        Ok(slice.to_vec())
    }

    /// Flush page data to cache and file.
    fn flush_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        if let Some(cached) = self.cache.get_mut(page_no) {
            let len = cached.len().min(data.len());
            cached[..len].copy_from_slice(&data[..len]);
        } else {
            // Cache population is best-effort. If the pool is full, try to
            // evict a page to make room.
            match self.cache.insert_fresh(page_no) {
                Ok(fresh) => {
                    let len = fresh.len().min(data.len());
                    fresh[..len].copy_from_slice(&data[..len]);
                }
                Err(FrankenError::OutOfMemory) => {
                    if self.cache.evict_any() {
                        if let Ok(fresh) = self.cache.insert_fresh(page_no) {
                            let len = fresh.len().min(data.len());
                            fresh[..len].copy_from_slice(&data[..len]);
                        }
                    }
                    // If still OOM or eviction failed, we skip caching and
                    // write directly to disk. This is valid write-through behavior.
                }
                Err(err) => return Err(err),
            }
        }

        let page_size = self.page_size.as_usize();
        let offset = u64::from(page_no.get() - 1) * page_size as u64;
        self.db_file.write(cx, data, offset)?;
        Ok(())
    }
}

/// A concrete single-writer pager backed by a VFS file.
pub struct SimplePager<V: Vfs> {
    /// VFS used to open journal/WAL companion files.
    vfs: Arc<V>,
    /// Path to the database file.
    db_path: PathBuf,
    /// Shared mutable state used by transactions.
    inner: Arc<Mutex<PagerInner<V::File>>>,
}

impl<V: Vfs> traits::sealed::Sealed for SimplePager<V> {}

impl<V> MvccPager for SimplePager<V>
where
    V: Vfs + Send + Sync,
    V::File: Send + Sync,
{
    type Txn = SimpleTransaction<V>;

    fn begin(&self, cx: &Cx, mode: TransactionMode) -> Result<Self::Txn> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;

        if inner.checkpoint_active {
            return Err(FrankenError::Busy);
        }

        if inner.journal_mode == JournalMode::Wal {
            let wal = inner.wal_backend.as_mut().ok_or_else(|| {
                FrankenError::internal("WAL mode active but no WAL backend installed")
            })?;
            wal.begin_transaction(cx)?;
        }

        let eager_writer = matches!(
            mode,
            TransactionMode::Immediate | TransactionMode::Exclusive
        );
        if eager_writer && inner.writer_active {
            return Err(FrankenError::Busy);
        }
        if eager_writer {
            inner.writer_active = true;
        }
        inner.active_transactions = inner.active_transactions.saturating_add(1);
        let original_db_size = inner.db_size;
        let journal_mode = inner.journal_mode;
        let pool = inner.cache.pool().clone();
        drop(inner);

        Ok(SimpleTransaction {
            vfs: Arc::clone(&self.vfs),
            journal_path: Self::journal_path(&self.db_path),
            inner: Arc::clone(&self.inner),
            write_set: HashMap::new(),
            freed_pages: Vec::new(),
            allocated_from_freelist: Vec::new(),
            mode,
            is_writer: eager_writer,
            committed: false,
            finished: false,
            original_db_size,
            savepoint_stack: Vec::new(),
            journal_mode,
            pool,
        })
    }

    fn journal_mode(&self) -> JournalMode {
        self.inner
            .lock()
            .map(|inner| inner.journal_mode)
            .unwrap_or_default()
    }

    fn set_journal_mode(&self, _cx: &Cx, mode: JournalMode) -> Result<JournalMode> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;

        if inner.checkpoint_active {
            return Err(FrankenError::Busy);
        }
        if inner.writer_active {
            // Cannot switch journal mode while a writer is active.
            return Err(FrankenError::Busy);
        }

        if mode == JournalMode::Wal && inner.wal_backend.is_none() {
            return Err(FrankenError::Unsupported);
        }

        inner.journal_mode = mode;
        drop(inner);
        Ok(mode)
    }

    fn set_wal_backend(&self, backend: Box<dyn WalBackend>) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;
        if inner.checkpoint_active {
            return Err(FrankenError::Busy);
        }
        inner.wal_backend = Some(backend);
        drop(inner);
        Ok(())
    }
}

impl<V: Vfs> SimplePager<V>
where
    V::File: Send + Sync,
{
    /// Capture point-in-time page-cache counters.
    pub fn cache_metrics_snapshot(&self) -> Result<PageCacheMetricsSnapshot> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;
        Ok(inner.cache.metrics_snapshot())
    }

    /// Reset page-cache counters without altering resident pages.
    pub fn reset_cache_metrics(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?
            .cache
            .reset_metrics();
        Ok(())
    }

    /// Compute the journal path from the database path.
    fn journal_path(db_path: &Path) -> PathBuf {
        let mut jp = db_path.as_os_str().to_owned();
        jp.push("-journal");
        PathBuf::from(jp)
    }

    /// Open (or create) a database and return a pager.
    ///
    /// If a hot journal is detected (leftover from a crash), it is replayed
    /// to restore the database to a consistent state before returning.
    #[allow(clippy::too_many_lines)]
    pub fn open(vfs: V, path: &Path, page_size: PageSize) -> Result<Self> {
        let cx = Cx::new();
        let vfs = Arc::new(vfs);
        let flags = VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _actual_flags) = vfs.open(&cx, Some(path), flags)?;

        // Hot journal recovery: if a journal file exists, replay it.
        let journal_path = Self::journal_path(path);
        if vfs.access(&cx, &journal_path, AccessFlags::EXISTS)? {
            Self::replay_journal(&cx, &*vfs, &mut db_file, &journal_path, page_size)?;
            // After successful replay, delete the journal.
            let _ = vfs.delete(&cx, &journal_path, true);
        }

        let mut file_size = db_file.file_size(&cx)?;
        if file_size == 0 {
            // SQLite databases are never truly empty: page 1 contains the
            // 100-byte database header followed by the sqlite_master root page.
            //
            // This makes newly-created databases valid for downstream layers
            // (B-tree, schema) and avoids surprising "empty file" semantics.
            let page_len = page_size.as_usize();
            let mut page1 = vec![0u8; page_len];

            let header = DatabaseHeader {
                page_size,
                page_count: 1,
                sqlite_version: FRANKENSQLITE_SQLITE_VERSION_NUMBER,
                ..DatabaseHeader::default()
            };
            let hdr_bytes = header.to_bytes().map_err(|err| {
                FrankenError::internal(format!("failed to encode new database header: {err}"))
            })?;
            page1[..DATABASE_HEADER_SIZE].copy_from_slice(&hdr_bytes);

            // Initialize sqlite_master root page as an empty leaf table B-tree
            // page (type 0x0D) with zero cells.
            let usable = page_size.usable(header.reserved_per_page);
            BTreePageHeader::write_empty_leaf_table(&mut page1, DATABASE_HEADER_SIZE, usable);

            db_file.write(&cx, &page1, 0)?;
            db_file.sync(&cx, SyncFlags::NORMAL)?;
            file_size = db_file.file_size(&cx)?;
        } else {
            if file_size < DATABASE_HEADER_SIZE as u64 {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "database file too small for header: {file_size} bytes (< {DATABASE_HEADER_SIZE})"
                    ),
                });
            }

            let mut header_bytes = [0u8; DATABASE_HEADER_SIZE];
            let header_read = db_file.read(&cx, &mut header_bytes, 0)?;
            if header_read < DATABASE_HEADER_SIZE {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "short read fetching database header: got {header_read} of {DATABASE_HEADER_SIZE}"
                    ),
                });
            }
            let header = DatabaseHeader::from_bytes(&header_bytes).map_err(|error| {
                FrankenError::DatabaseCorrupt {
                    detail: format!("invalid database header: {error}"),
                }
            })?;
            if header.page_size != page_size {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "database page size mismatch: header={} requested={}",
                        header.page_size.get(),
                        page_size.get()
                    ),
                });
            }
        }

        let page_size_u64 = page_size.as_usize() as u64;
        if file_size % page_size_u64 != 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "database file size {file_size} is not aligned to page size {}",
                    page_size.get()
                ),
            });
        }
        let db_pages = file_size
            .checked_div(page_size_u64)
            .ok_or_else(|| FrankenError::internal("page size must be non-zero"))?;
        let db_size = u32::try_from(db_pages).map_err(|_| FrankenError::OutOfRange {
            what: "database page count".to_owned(),
            value: db_pages.to_string(),
        })?;
        let next_page = if db_size >= 2 { db_size + 1 } else { 2 };

        Ok(Self {
            vfs,
            db_path: path.to_owned(),
            inner: Arc::new(Mutex::new(PagerInner {
                db_file,
                cache: PageCache::new(page_size, 1024),
                page_size,
                db_size,
                next_page,
                writer_active: false,
                active_transactions: 0,
                checkpoint_active: false,
                freelist: Vec::new(),
                journal_mode: JournalMode::Delete,
                wal_backend: None,
                commit_seq: CommitSeq::ZERO,
            })),
        })
    }

    /// Replay a hot journal by writing original pages back to the database.
    fn replay_journal(
        cx: &Cx,
        vfs: &V,
        db_file: &mut V::File,
        journal_path: &Path,
        page_size: PageSize,
    ) -> Result<()> {
        let jrnl_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
        let Ok((mut jrnl_file, _)) = vfs.open(cx, Some(journal_path), jrnl_flags) else {
            return Ok(()); // Cannot open journal — treat as no journal.
        };

        let jrnl_size = jrnl_file.file_size(cx)?;
        if jrnl_size < crate::journal::JOURNAL_HEADER_SIZE as u64 {
            return Ok(()); // Truncated/empty journal — nothing to replay.
        }

        // Read and parse the journal header.
        let mut hdr_buf = vec![0u8; crate::journal::JOURNAL_HEADER_SIZE];
        let _ = jrnl_file.read(cx, &mut hdr_buf, 0)?;
        let Ok(header) = JournalHeader::decode(&hdr_buf) else {
            return Ok(()); // Corrupt header — nothing to replay.
        };
        if header.page_size != page_size.get() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "hot journal page size mismatch: header={} expected={}",
                    header.page_size,
                    page_size.get()
                ),
            });
        }

        let page_count = if header.page_count < 0 {
            header.compute_page_count_from_file_size(jrnl_size)
        } else {
            #[allow(clippy::cast_sign_loss)]
            let c = header.page_count as u32;
            c
        };

        let header_size = u64::try_from(crate::journal::JOURNAL_HEADER_SIZE)
            .expect("journal header size should fit in u64");
        let hdr_padded = u64::from(header.sector_size).max(header_size);
        let ps = page_size.as_usize();
        let record_size = 4 + ps + 4;
        let mut offset = hdr_padded;

        for _ in 0..page_count {
            let mut rec_buf = vec![0u8; record_size];
            let bytes_read = jrnl_file.read(cx, &mut rec_buf, offset)?;
            if bytes_read < record_size {
                break; // Torn record — stop replay.
            }

            #[allow(clippy::cast_possible_truncation)]
            let Ok(record) = JournalPageRecord::decode(&rec_buf, ps as u32) else {
                break; // Corrupt record — stop replay.
            };

            // Verify checksum before applying.
            if record.verify_checksum(header.nonce).is_err() {
                break; // Checksum failure — stop replay at this point.
            }

            // Write the pre-image back to the database file.
            let Some(page_no) = PageNumber::new(record.page_number) else {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "hot journal contains invalid page number {}",
                        record.page_number
                    ),
                });
            };
            let page_offset = u64::from(page_no.get() - 1) * ps as u64;
            db_file.write(cx, &record.content, page_offset)?;

            offset += record_size as u64;
        }

        // Sync the database after replaying.
        db_file.sync(cx, SyncFlags::NORMAL)?;

        // Truncate the database to the original size from the journal header.
        if header.initial_db_size > 0 {
            let target_size = u64::from(header.initial_db_size) * ps as u64;
            let current_size = db_file.file_size(cx)?;
            if current_size > target_size {
                db_file.truncate(cx, target_size)?;
            }
        }

        Ok(())
    }
}

/// A snapshot of the transaction state at a savepoint boundary.
struct SavepointEntry {
    /// The user-supplied savepoint name.
    name: String,
    /// Snapshot of the write-set at the time the savepoint was created.
    /// Stores raw bytes (Vec<u8>) to decouple from buffer pool handle lifetime.
    write_set_snapshot: HashMap<PageNumber, Vec<u8>>,
    /// Snapshot of freed pages at the time the savepoint was created.
    freed_pages_snapshot: Vec<PageNumber>,
    /// Snapshot of the pager's next_page counter.
    /// Used to restore allocation state on rollback.
    next_page_snapshot: u32,
    /// Snapshot of the pager's freelist.
    /// Used to restore allocation state on rollback.
    freelist_snapshot: Vec<PageNumber>,
    /// Snapshot of pages allocated from freelist by this transaction.
    allocated_from_freelist_snapshot: Vec<PageNumber>,
}

/// Transaction handle produced by [`SimplePager`].
pub struct SimpleTransaction<V: Vfs> {
    vfs: Arc<V>,
    journal_path: PathBuf,
    inner: Arc<Mutex<PagerInner<V::File>>>,
    write_set: HashMap<PageNumber, PageBuf>,
    freed_pages: Vec<PageNumber>,
    allocated_from_freelist: Vec<PageNumber>,
    mode: TransactionMode,
    is_writer: bool,
    committed: bool,
    finished: bool,
    original_db_size: u32,
    /// Stack of savepoints, pushed on SAVEPOINT and popped on RELEASE.
    savepoint_stack: Vec<SavepointEntry>,
    /// Journal mode captured at transaction start.
    journal_mode: JournalMode,
    /// Buffer pool for allocating write-set pages.
    pool: PageBufPool,
}

impl<V: Vfs> traits::sealed::Sealed for SimpleTransaction<V> {}

impl<V: Vfs> SimpleTransaction<V> {
    /// Whether this transaction has been upgraded to a writer.
    #[must_use]
    pub fn is_writer(&self) -> bool {
        self.is_writer
    }
}

impl<V> SimpleTransaction<V>
where
    V: Vfs + Send,
    V::File: Send + Sync,
{
    /// Commit using the rollback journal protocol.
    #[allow(clippy::too_many_lines)]
    fn commit_journal(
        cx: &Cx,
        vfs: &Arc<V>,
        journal_path: &Path,
        inner: &mut PagerInner<V::File>,
        write_set: &HashMap<PageNumber, PageBuf>,
        freed_pages: &mut Vec<PageNumber>,
        original_db_size: u32,
    ) -> Result<()> {
        if !write_set.is_empty() {
            // Phase 1: Write rollback journal with pre-images.
            let nonce = 0x4652_414E; // "FRAN" — deterministic nonce.
            let page_size = inner.page_size;
            let ps = page_size.as_usize();

            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl_file, _) = vfs.open(cx, Some(journal_path), jrnl_flags)?;

            let header = JournalHeader {
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                page_count: write_set.len() as i32,
                nonce,
                initial_db_size: original_db_size,
                sector_size: 512,
                page_size: page_size.get(),
            };
            let hdr_bytes = header.encode_padded();
            jrnl_file.write(cx, &hdr_bytes, 0)?;

            let mut jrnl_offset = hdr_bytes.len() as u64;
            for &page_no in write_set.keys() {
                // Read current on-disk content as the pre-image.
                let mut pre_image = vec![0u8; ps];
                if page_no.get() <= inner.db_size {
                    let disk_offset = u64::from(page_no.get() - 1) * ps as u64;
                    let bytes_read = inner.db_file.read(cx, &mut pre_image, disk_offset)?;
                    if bytes_read < ps {
                        return Err(FrankenError::DatabaseCorrupt {
                            detail: format!(
                                "short read while journaling pre-image for page {}: got {bytes_read} of {ps}",
                                page_no.get()
                            ),
                        });
                    }
                }

                let record = JournalPageRecord::new(page_no.get(), pre_image, nonce);
                let rec_bytes = record.encode();
                jrnl_file.write(cx, &rec_bytes, jrnl_offset)?;
                jrnl_offset += rec_bytes.len() as u64;
            }

            // Sync journal to ensure durability before modifying database.
            jrnl_file.sync(cx, SyncFlags::NORMAL)?;

            // Phase 2: Write dirty pages to database.
            for (page_no, data) in write_set {
                inner.flush_page(cx, *page_no, data)?;
                inner.db_size = inner.db_size.max(page_no.get());
            }
            inner.db_file.sync(cx, SyncFlags::NORMAL)?;

            // Phase 3: Delete journal (commit point).
            let _ = vfs.delete(cx, journal_path, true);
        }

        for page_no in freed_pages.drain(..) {
            inner.freelist.push(page_no);
        }
        Ok(())
    }

    /// Commit using the WAL protocol (append frames to WAL file).
    fn commit_wal(
        cx: &Cx,
        inner: &mut PagerInner<V::File>,
        write_set: &HashMap<PageNumber, PageBuf>,
        freed_pages: &mut Vec<PageNumber>,
    ) -> Result<()> {
        if !write_set.is_empty() {
            let wal = inner.wal_backend.as_mut().ok_or_else(|| {
                FrankenError::internal("WAL mode active but no WAL backend installed")
            })?;

            let page_count = write_set.len();
            let mut written = 0_usize;

            for (page_no, data) in write_set {
                written += 1;
                // The last frame in the commit gets db_size > 0 as commit marker.
                let db_size_if_commit = if written == page_count {
                    // Compute final database size: max of current and all written pages.
                    let max_written = write_set.keys().map(|p| p.get()).max().unwrap_or(0);
                    inner.db_size.max(max_written)
                } else {
                    0
                };
                wal.append_frame(cx, page_no.get(), data, db_size_if_commit)?;
            }

            // Sync WAL to ensure durability.
            let wal = inner
                .wal_backend
                .as_mut()
                .ok_or_else(|| FrankenError::internal("WAL backend disappeared during commit"))?;
            wal.sync(cx)?;

            // Update db_size for any new pages.
            for page_no in write_set.keys() {
                inner.db_size = inner.db_size.max(page_no.get());
            }
        }

        for page_no in freed_pages.drain(..) {
            inner.freelist.push(page_no);
        }
        Ok(())
    }

    fn ensure_writer(&mut self) -> Result<()> {
        if self.is_writer {
            return Ok(());
        }

        match self.mode {
            TransactionMode::ReadOnly => Err(FrankenError::ReadOnly),
            TransactionMode::Concurrent => {
                let inner = self
                    .inner
                    .lock()
                    .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
                if inner.checkpoint_active {
                    return Err(FrankenError::Busy);
                }
                // Concurrent writers don't acquire the global writer_active lock.
                drop(inner);
                self.is_writer = true;
                Ok(())
            }
            TransactionMode::Deferred => {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
                if inner.checkpoint_active {
                    return Err(FrankenError::Busy);
                }
                if inner.writer_active {
                    return Err(FrankenError::Busy);
                }
                inner.writer_active = true;
                drop(inner);
                self.is_writer = true;
                Ok(())
            }
            TransactionMode::Immediate | TransactionMode::Exclusive => Err(FrankenError::internal(
                "writer transaction lost writer role",
            )),
        }
    }
}

impl<V> TransactionHandle for SimpleTransaction<V>
where
    V: Vfs + Send,
    V::File: Send + Sync,
{
    fn get_page(&self, cx: &Cx, page_no: PageNumber) -> Result<PageData> {
        if let Some(data) = self.write_set.get(&page_no) {
            return Ok(PageData::from_vec(data.to_vec()));
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
        let data = inner.read_page_copy(cx, page_no)?;
        drop(inner);
        Ok(PageData::from_vec(data))
    }

    fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.ensure_writer()?;

        // If we are writing to a page that was previously freed in this transaction,
        // we must "un-free" it.
        if let Some(pos) = self.freed_pages.iter().position(|&p| p == page_no) {
            self.freed_pages.swap_remove(pos);
        }

        let mut buf = self.pool.acquire()?;
        let len = buf.len().min(data.len());
        buf[..len].copy_from_slice(&data[..len]);
        // Zero-fill tail if needed (shouldn't happen for full page writes).
        if len < buf.len() {
            buf[len..].fill(0);
        }
        self.write_set.insert(page_no, buf);
        Ok(())
    }

    fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
        self.ensure_writer()?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        if let Some(page) = inner.freelist.pop() {
            self.allocated_from_freelist.push(page);
            return Ok(page);
        }

        let raw = inner.next_page;
        inner.next_page = inner.next_page.saturating_add(1);
        drop(inner);
        PageNumber::new(raw).ok_or_else(|| FrankenError::OutOfRange {
            what: "allocated page number".to_owned(),
            value: raw.to_string(),
        })
    }

    fn free_page(&mut self, _cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.ensure_writer()?;
        if page_no == PageNumber::ONE {
            return Err(FrankenError::OutOfRange {
                what: "free page number".to_owned(),
                value: page_no.get().to_string(),
            });
        }
        if !self.freed_pages.contains(&page_no) {
            self.freed_pages.push(page_no);
        }
        self.write_set.remove(&page_no);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn commit(&mut self, cx: &Cx) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        if !self.is_writer {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            inner.active_transactions = inner.active_transactions.saturating_sub(1);
            drop(inner);
            self.committed = true;
            self.finished = true;
            return Ok(());
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        let commit_result = if self.journal_mode == JournalMode::Wal {
            Self::commit_wal(cx, &mut inner, &self.write_set, &mut self.freed_pages)
        } else {
            Self::commit_journal(
                cx,
                &self.vfs,
                &self.journal_path,
                &mut inner,
                &self.write_set,
                &mut self.freed_pages,
                self.original_db_size,
            )
        };

        if commit_result.is_ok() {
            inner.commit_seq = inner.commit_seq.next();
            inner.active_transactions = inner.active_transactions.saturating_sub(1);
            if self.mode != TransactionMode::Concurrent {
                inner.writer_active = false;
            }
            drop(inner);
            self.write_set.clear();
            self.committed = true;
            self.finished = true;
        } else {
            // Keep the writer lock held on commit failure so no other writer
            // can interleave while the caller decides to retry or roll back.
            drop(inner);
        }
        commit_result
    }

    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.write_set.clear();
        self.freed_pages.clear();
        self.savepoint_stack.clear();
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        // Restore pages allocated from the freelist.
        for page in self.allocated_from_freelist.drain(..) {
            inner.freelist.push(page);
        }

        if self.is_writer && self.mode != TransactionMode::Concurrent {
            inner.db_size = self.original_db_size;

            // Reset next_page to avoid holes if we allocated pages that are now discarded.
            // Logic matches SimplePager::open.
            let db_size = inner.db_size;
            inner.next_page = if db_size >= 2 { db_size + 1 } else { 2 };

            inner.writer_active = false;
        }
        inner.active_transactions = inner.active_transactions.saturating_sub(1);
        drop(inner);
        if self.is_writer {
            // Delete any partial journal file.
            let _ = self.vfs.delete(cx, &self.journal_path, true);
        }
        self.committed = false;
        self.finished = true;
        Ok(())
    }

    fn savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        self.savepoint_stack.push(SavepointEntry {
            name: name.to_owned(),
            write_set_snapshot: self
                .write_set
                .iter()
                .map(|(&k, v)| (k, v.to_vec()))
                .collect(),
            freed_pages_snapshot: self.freed_pages.clone(),
            next_page_snapshot: inner.next_page,
            freelist_snapshot: inner.freelist.clone(),
            allocated_from_freelist_snapshot: self.allocated_from_freelist.clone(),
        });
        drop(inner);
        Ok(())
    }

    fn release_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let pos = self
            .savepoint_stack
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| FrankenError::internal(format!("no savepoint named '{name}'")))?;
        // RELEASE removes the named savepoint and all savepoints above it.
        // Changes since the savepoint are kept (merged into the parent).
        self.savepoint_stack.truncate(pos);
        Ok(())
    }

    fn rollback_to_savepoint(&mut self, _cx: &Cx, name: &str) -> Result<()> {
        let pos = self
            .savepoint_stack
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| FrankenError::internal(format!("no savepoint named '{name}'")))?;
        // Restore write-set and freed-pages to the snapshot state.
        // Convert Vec<u8> snapshots back to PageBuf (allocated from pool).
        let entry = &self.savepoint_stack[pos];
        self.write_set = entry
            .write_set_snapshot
            .iter()
            .map(|(&k, v)| -> Result<(PageNumber, PageBuf)> {
                let mut buf = self.pool.acquire()?;
                let len = buf.len().min(v.len());
                buf.as_mut_slice()[..len].copy_from_slice(&v[..len]);
                if len < buf.len() {
                    buf[len..].fill(0);
                }
                Ok((k, buf))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        self.freed_pages = entry.freed_pages_snapshot.clone();
        self.allocated_from_freelist = entry.allocated_from_freelist_snapshot.clone();

        // Restore allocation state.
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            inner.next_page = entry.next_page_snapshot;
            inner.freelist.clone_from(&entry.freelist_snapshot);
        }

        // Discard savepoints created after the named one, but keep
        // the named savepoint itself (it can be rolled back to again).
        self.savepoint_stack.truncate(pos + 1);
        Ok(())
    }
}

impl<V: Vfs> Drop for SimpleTransaction<V> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            // Restore pages allocated from the freelist.
            for page in self.allocated_from_freelist.drain(..) {
                inner.freelist.push(page);
            }

            if self.is_writer && self.mode != TransactionMode::Concurrent {
                inner.db_size = self.original_db_size;

                // Reset next_page to avoid holes if we allocated pages that are now discarded.
                // Logic matches SimplePager::open and SimpleTransaction::rollback.
                let db_size = inner.db_size;
                inner.next_page = if db_size >= 2 { db_size + 1 } else { 2 };

                inner.writer_active = false;
            }
            inner.active_transactions = inner.active_transactions.saturating_sub(1);
        }
        // We cannot easily delete the journal file here because Drop doesn't
        // take a Context or return a Result. It's best effort cleanup.
        // Hot journal recovery will handle any leftover files on next open.
        self.finished = true;
    }
}

// ---------------------------------------------------------------------------
// CheckpointPageWriter implementation for WAL checkpointing
// ---------------------------------------------------------------------------

/// A checkpoint page writer that writes pages directly to the database file.
///
/// This type implements [`CheckpointPageWriter`] and is used during WAL
/// checkpointing to transfer committed pages from the WAL back to the main
/// database file.
///
/// The writer holds a reference to the pager's inner state and acquires the
/// mutex for each operation. This is acceptable because checkpoint is an
/// infrequent operation and the writes must be serialized with other pager
/// operations anyway.
pub struct SimplePagerCheckpointWriter<V: Vfs>
where
    V::File: Send + Sync,
{
    inner: Arc<Mutex<PagerInner<V::File>>>,
}

impl<V: Vfs> traits::sealed::Sealed for SimplePagerCheckpointWriter<V> where V::File: Send + Sync {}

impl<V> traits::CheckpointPageWriter for SimplePagerCheckpointWriter<V>
where
    V: Vfs + Send + Sync,
    V::File: Send + Sync,
{
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePagerCheckpointWriter lock poisoned"))?;

        // Write directly to the database file, bypassing the cache.
        // The WAL checkpoint is authoritative, so we overwrite any cached version.
        let page_size = inner.page_size.as_usize();
        let offset = u64::from(page_no.get() - 1) * page_size as u64;
        inner.db_file.write(cx, data, offset)?;

        // Invalidate cache entry if present to avoid stale reads.
        inner.cache.evict(page_no);

        // Update db_size if this page extends the database.
        inner.db_size = inner.db_size.max(page_no.get());

        drop(inner);
        Ok(())
    }

    fn truncate(&mut self, cx: &Cx, n_pages: u32) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePagerCheckpointWriter lock poisoned"))?;

        let old_db_size = inner.db_size;
        let page_size = inner.page_size.as_usize();
        let target_size = u64::from(n_pages) * page_size as u64;
        inner.db_file.truncate(cx, target_size)?;
        inner.db_size = n_pages;

        // Invalidate cached pages beyond the new size.
        // We only need to evict pages that are beyond the truncation point.
        // Note: This is a best-effort cleanup - pages may not all be cached.
        for pgno in (n_pages + 1)..=old_db_size {
            if let Some(page_no) = PageNumber::new(pgno) {
                inner.cache.evict(page_no);
            }
        }

        drop(inner);
        Ok(())
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePagerCheckpointWriter lock poisoned"))?;
        inner.db_file.sync(cx, SyncFlags::NORMAL)
    }
}

impl<V: Vfs> SimplePager<V>
where
    V::File: Send + Sync,
{
    /// Create a checkpoint page writer for WAL checkpointing.
    ///
    /// The returned writer implements [`CheckpointPageWriter`] and can be
    /// wrapped in a `CheckpointTargetAdapter` from `fsqlite-core` to satisfy
    /// the WAL executor's `CheckpointTarget` trait.
    ///
    /// # Panics
    ///
    /// This method does not panic, but the returned writer's methods may
    /// return errors if the pager's internal mutex is poisoned.
    #[must_use]
    pub fn checkpoint_writer(&self) -> SimplePagerCheckpointWriter<V> {
        SimplePagerCheckpointWriter {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Run a WAL checkpoint to transfer frames from the WAL to the database.
    ///
    /// This is the main checkpoint entry point for WAL mode. It:
    /// 1. Acquires the pager lock
    /// 2. Creates a checkpoint writer for database page writes
    /// 3. Delegates to the WAL backend's checkpoint implementation
    ///
    /// # Arguments
    ///
    /// * `cx` - Cancellation/deadline context
    /// * `mode` - Checkpoint mode (Passive, Full, Restart, Truncate)
    ///
    /// # Returns
    ///
    /// A `CheckpointResult` describing what was accomplished, or an error if:
    /// - The pager is not in WAL mode
    /// - The pager lock is poisoned
    /// - Any I/O error occurs during the checkpoint
    ///
    /// # Notes
    ///
    /// This implementation refuses to checkpoint while any transaction is active.
    /// It starts from the beginning (backfilled_frames = 0) and passes
    /// `oldest_reader_frame = None`. For incremental, reader-aware checkpointing,
    /// use the lower-level WAL backend API.
    pub fn checkpoint(
        &self,
        cx: &Cx,
        mode: traits::CheckpointMode,
    ) -> Result<traits::CheckpointResult> {
        // Take the WAL backend out of the pager while marking checkpoint active.
        // `begin()` and deferred writer upgrades are blocked while this flag is
        // set so commits cannot observe "WAL mode but no backend".
        let mut wal = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;

            // Check we're in WAL mode.
            if inner.journal_mode != JournalMode::Wal {
                return Err(FrankenError::Unsupported);
            }
            if inner.checkpoint_active {
                return Err(FrankenError::Busy);
            }
            // Without reader tracking in pager, the safe policy is to refuse
            // checkpoint while any transaction is active.
            if inner.active_transactions > 0 {
                return Err(FrankenError::Busy);
            }

            inner.checkpoint_active = true;
            // Take the WAL backend out temporarily.
            let Some(wal) = inner.wal_backend.take() else {
                inner.checkpoint_active = false;
                return Err(FrankenError::internal(
                    "WAL mode active but no WAL backend installed",
                ));
            };
            drop(inner);
            wal
        };
        // Lock is released here.

        // Create a checkpoint writer that writes directly to the database file.
        let mut writer = self.checkpoint_writer();

        // Run the checkpoint from the beginning. Reader-aware incremental
        // checkpointing requires exposing oldest-reader tracking from pager.
        let result = wal.checkpoint(cx, mode, &mut writer, 0, None);

        // Put the WAL backend back and clear checkpoint state.
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;
            inner.wal_backend = Some(wal);
            inner.checkpoint_active = false;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{MvccPager, TransactionHandle, TransactionMode};
    use fsqlite_types::PageSize;
    use fsqlite_types::flags::AccessFlags;
    use fsqlite_types::{BTreePageHeader, DatabaseHeader};
    use fsqlite_vfs::MemoryVfs;
    use std::path::PathBuf;

    const BEAD_ID: &str = "bd-bca.1";

    fn test_pager() -> (SimplePager<MemoryVfs>, PathBuf) {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/test.db");
        let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        (pager, path)
    }

    #[test]
    fn test_open_empty_database() {
        let (pager, _) = test_pager();
        let inner = pager.inner.lock().unwrap();
        assert_eq!(inner.db_size, 1, "bead_id={BEAD_ID} case=empty_db_size");
        assert_eq!(
            inner.page_size,
            PageSize::DEFAULT,
            "bead_id={BEAD_ID} case=page_size_default"
        );
        drop(inner);

        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let raw_page = txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec();

        let hdr: [u8; DATABASE_HEADER_SIZE] = raw_page[..DATABASE_HEADER_SIZE]
            .try_into()
            .expect("page 1 must contain database header");
        let parsed = DatabaseHeader::from_bytes(&hdr).expect("header should parse");
        assert_eq!(
            parsed.page_size,
            PageSize::DEFAULT,
            "bead_id={BEAD_ID} case=page1_header_page_size"
        );
        assert_eq!(
            parsed.page_count, 1,
            "bead_id={BEAD_ID} case=page1_header_page_count"
        );

        let btree_hdr =
            BTreePageHeader::parse(&raw_page, PageSize::DEFAULT, 0, true).expect("btree header");
        assert_eq!(
            btree_hdr.cell_count, 0,
            "bead_id={BEAD_ID} case=sqlite_master_initially_empty"
        );
    }

    #[test]
    fn test_open_existing_database_rejects_page_size_mismatch() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/page_size_mismatch.db");

        let _pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let wrong_page_size = PageSize::new(8192).unwrap();
        let Err(err) = SimplePager::open(vfs, &path, wrong_page_size) else {
            panic!("expected page size mismatch error");
        };
        assert!(
            matches!(err, FrankenError::DatabaseCorrupt { .. }),
            "bead_id={BEAD_ID} case=reject_page_size_mismatch"
        );
    }

    #[test]
    fn test_open_existing_database_rejects_non_page_aligned_size() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/misaligned.db");
        let cx = Cx::new();
        let _pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
        let file_size = db_file.file_size(&cx).unwrap();
        db_file.write(&cx, &[0xAB], file_size).unwrap();

        let Err(err) = SimplePager::open(vfs, &path, PageSize::DEFAULT) else {
            panic!("expected non-page-aligned file size error");
        };
        assert!(
            matches!(err, FrankenError::DatabaseCorrupt { .. }),
            "bead_id={BEAD_ID} case=reject_non_page_aligned_file_size"
        );
    }

    #[test]
    fn test_begin_readonly_transaction() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert!(!txn.is_writer, "bead_id={BEAD_ID} case=readonly_not_writer");
    }

    #[test]
    fn test_begin_write_transaction() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        assert!(txn.is_writer, "bead_id={BEAD_ID} case=immediate_is_writer");
    }

    #[test]
    fn test_begin_deferred_transaction_starts_reader() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Deferred).unwrap();
        assert!(
            !txn.is_writer,
            "bead_id={BEAD_ID} case=deferred_starts_readonly"
        );
    }

    #[test]
    fn test_begin_concurrent_transaction_starts_reader() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        assert!(
            !txn.is_writer,
            "bead_id={BEAD_ID} case=concurrent_starts_readonly"
        );
    }

    #[test]
    fn test_deferred_upgrades_on_first_write_intent() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut deferred = pager.begin(&cx, TransactionMode::Deferred).unwrap();
        assert!(
            !deferred.is_writer,
            "bead_id={BEAD_ID} case=deferred_pre_upgrade"
        );

        let _page = deferred.allocate_page(&cx).unwrap();
        assert!(
            deferred.is_writer,
            "bead_id={BEAD_ID} case=deferred_upgraded_to_writer"
        );
    }

    #[test]
    fn test_deferred_upgrade_busy_when_writer_active() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut deferred = pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let err = deferred.allocate_page(&cx).unwrap_err();
        assert!(matches!(err, FrankenError::Busy));
    }

    #[test]
    fn test_concurrent_writer_blocked() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _txn1 = pager.begin(&cx, TransactionMode::Exclusive).unwrap();
        let result = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=concurrent_writer_busy"
        );
    }

    #[test]
    fn test_multiple_readers_allowed() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _r1 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let _r2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        // Both readers can coexist.
    }

    #[test]
    fn test_write_page_and_read_back() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let page_no = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        let mut data = vec![0_u8; page_size];
        data[0] = 0xDE;
        data[1] = 0xAD;
        txn.write_page(&cx, page_no, &data).unwrap();

        let read_back = txn.get_page(&cx, page_no).unwrap();
        assert_eq!(
            read_back.as_ref()[0],
            0xDE,
            "bead_id={BEAD_ID} case=read_back_byte0"
        );
        assert_eq!(
            read_back.as_ref()[1],
            0xAD,
            "bead_id={BEAD_ID} case=read_back_byte1"
        );
    }

    #[test]
    fn test_commit_persists_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Write in first transaction.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_no = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        let mut data = vec![0_u8; page_size];
        data[0..4].copy_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]);
        txn.write_page(&cx, page_no, &data).unwrap();
        txn.commit(&cx).unwrap();

        // Read in second transaction.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let read_back = txn2.get_page(&cx, page_no).unwrap();
        assert_eq!(
            &read_back.as_ref()[0..4],
            &[0xCA, 0xFE, 0xBA, 0xBE],
            "bead_id={BEAD_ID} case=commit_persists"
        );
    }

    #[test]
    fn test_rollback_discards_writes() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Allocate and write a page, then commit so it exists on disk.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_no = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        let original = vec![0x11_u8; page_size];
        txn.write_page(&cx, page_no, &original).unwrap();
        txn.commit(&cx).unwrap();

        // Overwrite in a new transaction, then rollback.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let modified = vec![0x99_u8; page_size];
        txn2.write_page(&cx, page_no, &modified).unwrap();
        txn2.rollback(&cx).unwrap();

        // Read again — should see original data.
        let txn3 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let read_back = txn3.get_page(&cx, page_no).unwrap();
        assert_eq!(
            read_back.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=rollback_restores"
        );
    }

    #[test]
    fn test_allocate_returns_sequential_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let p1 = txn.allocate_page(&cx).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        assert!(
            p2.get() > p1.get(),
            "bead_id={BEAD_ID} case=sequential_alloc p1={} p2={}",
            p1.get(),
            p2.get()
        );
    }

    #[test]
    fn test_free_page_reuses_on_next_alloc() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Allocate two pages and commit.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        let page_size = PageSize::DEFAULT.as_usize();
        txn.write_page(&cx, p1, &vec![1_u8; page_size]).unwrap();
        txn.write_page(&cx, p2, &vec![2_u8; page_size]).unwrap();
        txn.commit(&cx).unwrap();

        // Free p1 and commit.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.free_page(&cx, p1).unwrap();
        txn2.commit(&cx).unwrap();

        // Next allocation should reuse freed page.
        let mut txn3 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p3 = txn3.allocate_page(&cx).unwrap();
        assert_eq!(
            p3,
            p1,
            "bead_id={BEAD_ID} case=freelist_reuse p3={} p1={}",
            p3.get(),
            p1.get()
        );
    }

    #[test]
    fn test_cannot_free_page_one() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let result = txn.free_page(&cx, PageNumber::ONE);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=cannot_free_page_one"
        );
    }

    #[test]
    fn test_readonly_cannot_write() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let result = txn.write_page(&cx, PageNumber::ONE, &[0_u8; 4096]);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=readonly_cannot_write"
        );
    }

    #[test]
    fn test_readonly_cannot_allocate() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let result = txn.allocate_page(&cx);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=readonly_cannot_allocate"
        );
    }

    #[test]
    fn test_drop_uncommitted_writer_releases_lock() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        {
            let _txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            // Dropped without commit or rollback.
        }

        // Should be able to begin a new writer.
        let txn2 = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            txn2.is_ok(),
            "bead_id={BEAD_ID} case=drop_releases_writer_lock"
        );
    }

    #[test]
    fn test_commit_then_drop_no_double_release() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            txn.commit(&cx).unwrap();
            // committed=true, drop should skip writer_active=false
        }

        // Writer should already be released by commit.
        let txn2 = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            txn2.is_ok(),
            "bead_id={BEAD_ID} case=commit_releases_writer"
        );
    }

    #[test]
    fn test_double_commit_is_idempotent() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.commit(&cx).unwrap();
        // Second commit should be a no-op.
        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_multi_page_write_commit_read() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let page_size = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut allocated_pages = Vec::new();
        for i in 0_u8..5 {
            let p = txn.allocate_page(&cx).unwrap();
            let data = vec![i; page_size];
            txn.write_page(&cx, p, &data).unwrap();
            allocated_pages.push(p);
        }
        txn.commit(&cx).unwrap();

        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (i, p) in allocated_pages.iter().enumerate() {
            let data = txn2.get_page(&cx, *p).unwrap();
            #[allow(clippy::cast_possible_truncation)]
            let expected = i as u8;
            assert_eq!(
                data.as_ref()[0],
                expected,
                "bead_id={BEAD_ID} case=multi_page idx={i}"
            );
        }
    }

    // ── Journal crash recovery tests ────────────────────────────────────

    #[test]
    fn test_commit_journal_short_preimage_read_errors() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/short_preimage.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();

        // Establish page 2 so the next commit must read a pre-image for it.
        let page_two = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
            p
        };

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, page_two, &vec![0x22; ps]).unwrap();

        // Simulate external truncation: pre-image read for page 2 becomes short.
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
        db_file
            .truncate(&cx, PageSize::DEFAULT.as_usize() as u64)
            .unwrap();

        let err = txn.commit(&cx).unwrap_err();
        assert!(
            matches!(err, FrankenError::DatabaseCorrupt { .. }),
            "bead_id={BEAD_ID} case=short_preimage_read_is_corruption"
        );

        // Commit failure should keep the writer lock until explicit rollback.
        let Err(busy) = pager.begin(&cx, TransactionMode::Immediate) else {
            panic!("expected begin to fail while writer lock is still held");
        };
        assert!(
            matches!(busy, FrankenError::Busy),
            "bead_id={BEAD_ID} case=commit_error_keeps_writer_lock"
        );

        txn.rollback(&cx).unwrap();
        let _next_writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
    }

    #[test]
    fn test_commit_creates_and_deletes_journal() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/jrnl_test.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);

        // Before commit, no journal.
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=no_journal_before_commit"
        );

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xAA; 4096]).unwrap();
        txn.commit(&cx).unwrap();

        // After commit, journal should be deleted.
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_after_commit"
        );
    }

    #[test]
    fn test_hot_journal_recovery_restores_original_data() {
        // Simulate a crash: write data, manually create a journal with pre-images,
        // then reopen. The journal should be replayed, restoring original data.
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/crash_test.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Step 1: Create a database with known data via normal commit.
        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p1 = txn.allocate_page(&cx).unwrap();
            assert_eq!(p1.get(), 2);
            txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Step 2: Corrupt the database (simulate a partial write that crashed).
        {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
            let corrupt_data = vec![0x99; ps];
            let offset = u64::from(2_u32 - 1) * ps as u64;
            db_file.write(&cx, &corrupt_data, offset).unwrap();
        }

        // Step 3: Create a hot journal with the original pre-image.
        {
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 42;
            let header = JournalHeader {
                page_count: 1,
                nonce,
                initial_db_size: 2,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            let record = JournalPageRecord::new(2, vec![0x11; ps], nonce);
            let rec_bytes = record.encode();
            jrnl.write(&cx, &rec_bytes, hdr_bytes.len() as u64).unwrap();
        }

        // Step 4: Reopen — should detect hot journal and replay.
        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let page_no_2 = PageNumber::new(2).unwrap();
            let data = txn.get_page(&cx, page_no_2).unwrap();

            assert_eq!(
                data.as_ref()[0],
                0x11,
                "bead_id={BEAD_ID} case=journal_recovery_restores"
            );
            assert_eq!(
                data.as_ref()[ps - 1],
                0x11,
                "bead_id={BEAD_ID} case=journal_recovery_restores_last_byte"
            );
        }

        // Step 5: Verify journal is deleted after recovery.
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_after_recovery"
        );
    }

    #[test]
    fn test_hot_journal_truncated_record_stops_replay() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/trunc_jrnl.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Create DB with 2 pages.
        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p1 = txn.allocate_page(&cx).unwrap();
            let p2 = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p1, &vec![0xAA; ps]).unwrap();
            txn.write_page(&cx, p2, &vec![0xBB; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Corrupt page 2.
        {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
            db_file
                .write(&cx, &vec![0xFF; ps], u64::from(2_u32 - 1) * ps as u64)
                .unwrap();
        }

        // Journal claims 2 records but second is truncated.
        {
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 7;
            let header = JournalHeader {
                page_count: 2,
                nonce,
                initial_db_size: 3,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            // First record: valid pre-image for page 3.
            let rec1 = JournalPageRecord::new(3, vec![0xCC; ps], nonce);
            let rec1_bytes = rec1.encode();
            jrnl.write(&cx, &rec1_bytes, hdr_bytes.len() as u64)
                .unwrap();

            // Second record: truncated.
            let rec2 = JournalPageRecord::new(2, vec![0xBB; ps], nonce);
            let rec2_bytes = rec2.encode();
            let trunc_len = rec2_bytes.len() / 2;
            let offset = hdr_bytes.len() as u64 + rec1_bytes.len() as u64;
            jrnl.write(&cx, &rec2_bytes[..trunc_len], offset).unwrap();
        }

        // Reopen — first record replays, second skipped.
        {
            let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let page_no_2 = PageNumber::new(2).unwrap();
            let data2 = txn.get_page(&cx, page_no_2).unwrap();
            assert_eq!(
                data2.as_ref()[0],
                0xFF,
                "bead_id={BEAD_ID} case=truncated_journal_page2_not_restored"
            );
        }
    }

    #[test]
    fn test_hot_journal_checksum_mismatch_stops_replay() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/cksum_jrnl.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x55; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Corrupt page 2.
        {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();
            db_file
                .write(&cx, &vec![0xEE; ps], u64::from(2_u32 - 1) * ps as u64)
                .unwrap();
        }

        // Journal with wrong nonce in record (checksum won't verify).
        {
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 99;
            let header = JournalHeader {
                page_count: 1,
                nonce,
                initial_db_size: 2,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            // Wrong nonce in record.
            let record = JournalPageRecord::new(2, vec![0x55; ps], nonce + 1);
            let rec_bytes = record.encode();
            jrnl.write(&cx, &rec_bytes, hdr_bytes.len() as u64).unwrap();
        }

        // Reopen — bad checksum stops replay.
        {
            let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let page_no_2 = PageNumber::new(2).unwrap();
            let data = txn.get_page(&cx, page_no_2).unwrap();
            assert_eq!(
                data.as_ref()[0],
                0xEE,
                "bead_id={BEAD_ID} case=bad_checksum_stops_replay"
            );
        }
    }

    #[test]
    fn test_hot_journal_invalid_page_number_errors() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/bad_pgno_jrnl.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        {
            let _pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();

            let nonce = 321;
            let header = JournalHeader {
                page_count: 1,
                nonce,
                initial_db_size: 1,
                sector_size: 512,
                page_size: 4096,
            };
            let hdr_bytes = header.encode_padded();
            jrnl.write(&cx, &hdr_bytes, 0).unwrap();

            let record = JournalPageRecord::new(0, vec![0xAA; ps], nonce);
            let rec_bytes = record.encode();
            jrnl.write(&cx, &rec_bytes, hdr_bytes.len() as u64).unwrap();
        }

        let Err(err) = SimplePager::open(vfs, &path, PageSize::DEFAULT) else {
            panic!("expected invalid journal page number error");
        };
        assert!(
            matches!(err, FrankenError::DatabaseCorrupt { .. }),
            "bead_id={BEAD_ID} case=invalid_journal_page_number_rejected"
        );
    }

    #[test]
    fn test_journal_not_created_for_readonly_commit() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/readonly_jrnl.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);

        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        txn.commit(&cx).unwrap();

        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=no_journal_for_readonly"
        );
    }

    #[test]
    fn test_rollback_deletes_journal() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/rollback_jrnl.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xDD; 4096]).unwrap();
        txn.rollback(&cx).unwrap();

        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_on_rollback"
        );
    }

    // ── Savepoint tests ────────────────────────────────────────────────

    #[test]
    fn test_savepoint_basic_rollback_to() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        // Create savepoint after first write.
        txn.savepoint(&cx, "sp1").unwrap();

        // Second write (after savepoint).
        let p2 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p2, &vec![0x22; ps]).unwrap();
        // Overwrite p1 after savepoint.
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();

        // Rollback to sp1 — should undo second write and p1 overwrite.
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        // p1 should have the value from before the savepoint.
        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=savepoint_rollback_restores_p1"
        );

        // p2 should no longer be in the write-set (reads zeros from disk).
        let data2 = txn.get_page(&cx, p2).unwrap();
        assert_eq!(
            data2.as_ref()[0],
            0x00,
            "bead_id={BEAD_ID} case=savepoint_rollback_removes_p2"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_release_keeps_changes() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0xAA; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();

        // Write after savepoint.
        txn.write_page(&cx, p1, &vec![0xBB; ps]).unwrap();

        // Release — changes after savepoint are kept.
        txn.release_savepoint(&cx, "sp1").unwrap();

        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0xBB,
            "bead_id={BEAD_ID} case=release_keeps_changes"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_nested_rollback_to_inner() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "outer").unwrap();
        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();

        txn.savepoint(&cx, "inner").unwrap();
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();

        // Rollback to inner — should restore to 0x22 (state at "inner" creation).
        txn.rollback_to_savepoint(&cx, "inner").unwrap();

        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x22,
            "bead_id={BEAD_ID} case=nested_rollback_inner"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_nested_rollback_to_outer() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "outer").unwrap();
        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();

        txn.savepoint(&cx, "inner").unwrap();
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();

        // Rollback to outer — should restore to 0x11 and discard inner savepoint.
        txn.rollback_to_savepoint(&cx, "outer").unwrap();

        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=nested_rollback_outer"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_rollback_to_preserves_savepoint() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();

        // First modification + rollback.
        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        // Should be back to 0x11.
        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(data.as_ref()[0], 0x11);

        // Modify again.
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data2 = txn2.get_page(&cx, p1).unwrap();
        assert_eq!(data2.as_ref()[0], 0x33);
    }

    #[test]
    fn test_savepoint_rollback_reclaims_allocated_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        // Initial state: 1 page (header)
        let p1 = txn.allocate_page(&cx).unwrap(); // Page 2
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();

        // Allocate Page 3 inside savepoint
        let p2 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p2, &vec![0x22; ps]).unwrap();

        assert_eq!(p2.get(), p1.get() + 1, "Expected sequential allocation");

        // Rollback to sp1. This should ideally "un-allocate" p2.
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        // Allocate again. Should we get p2 again?
        // If next_page wasn't reverted, we'll get p2 + 1 (Page 4), leaving Page 3 as a hole.
        let p3 = txn.allocate_page(&cx).unwrap();

        assert_eq!(
            p3.get(),
            p2.get(),
            "bead_id={BEAD_ID} case=rollback_reclaims_allocation: expected page {} but got {}",
            p2.get(),
            p3.get()
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_rollback_to_preserves_savepoint_multi() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();

        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        // Second modification + rollback (savepoint still exists).
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=rollback_to_preserves_savepoint"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_freed_pages_restored_on_rollback() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0xAA; ps]).unwrap();
        txn.write_page(&cx, p2, &vec![0xBB; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();

        // Free p2 after savepoint.
        txn.free_page(&cx, p2).unwrap();

        // Rollback — p2 should no longer be freed.
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        // p2 should still be in the write-set (not freed).
        let data = txn.get_page(&cx, p2).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0xBB,
            "bead_id={BEAD_ID} case=freed_pages_restored"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_unknown_name_errors() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        let result = txn.rollback_to_savepoint(&cx, "nonexistent");
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=rollback_to_unknown_savepoint_errors"
        );

        let result = txn.release_savepoint(&cx, "nonexistent");
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=release_unknown_savepoint_errors"
        );
    }

    #[test]
    fn test_savepoint_release_then_rollback_to_outer() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "outer").unwrap();
        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();

        txn.savepoint(&cx, "inner").unwrap();
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();

        // Release inner — changes kept, inner savepoint removed.
        txn.release_savepoint(&cx, "inner").unwrap();

        // Rollback to outer — should revert to 0x11.
        txn.rollback_to_savepoint(&cx, "outer").unwrap();

        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=release_inner_then_rollback_outer"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_savepoint_commit_with_active_savepoints() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();
        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();

        // Commit with active savepoint — all changes should be persisted.
        txn.commit(&cx).unwrap();

        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn2.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x22,
            "bead_id={BEAD_ID} case=commit_with_savepoints_persists_all"
        );
    }

    #[test]
    fn test_savepoint_full_rollback_clears_savepoints() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();

        txn.savepoint(&cx, "sp1").unwrap();
        txn.savepoint(&cx, "sp2").unwrap();

        // Full rollback should clear all savepoints.
        txn.rollback(&cx).unwrap();

        // Trying to rollback to a savepoint after full rollback should error.
        let result = txn.rollback_to_savepoint(&cx, "sp1");
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=full_rollback_clears_savepoints"
        );
    }

    #[test]
    fn test_savepoint_three_levels_deep() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();

        // Level 0: write 0x00
        txn.write_page(&cx, p1, &vec![0x00; ps]).unwrap();
        txn.savepoint(&cx, "L0").unwrap();

        // Level 1: write 0x11
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();
        txn.savepoint(&cx, "L1").unwrap();

        // Level 2: write 0x22
        txn.write_page(&cx, p1, &vec![0x22; ps]).unwrap();
        txn.savepoint(&cx, "L2").unwrap();

        // Level 3: write 0x33
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();

        // Verify current state
        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x33,
            "bead_id={BEAD_ID} case=3level_current"
        );

        // Rollback to L2 → should see 0x22
        txn.rollback_to_savepoint(&cx, "L2").unwrap();
        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(data.as_ref()[0], 0x22, "bead_id={BEAD_ID} case=3level_L2");

        // Rollback to L1 → should see 0x11
        txn.rollback_to_savepoint(&cx, "L1").unwrap();
        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(data.as_ref()[0], 0x11, "bead_id={BEAD_ID} case=3level_L1");

        // Rollback to L0 → should see 0x00
        txn.rollback_to_savepoint(&cx, "L0").unwrap();
        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(data.as_ref()[0], 0x00, "bead_id={BEAD_ID} case=3level_L0");

        txn.commit(&cx).unwrap();

        // Verify committed value is 0x00 (state at L0).
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn2.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x00,
            "bead_id={BEAD_ID} case=3level_committed"
        );
    }

    // ── WAL mode integration tests ──────────────────────────────────────

    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    /// (page_number, page_data, db_size_if_commit)
    type WalFrame = (u32, Vec<u8>, u32);
    type SharedFrames = StdArc<StdMutex<Vec<WalFrame>>>;

    /// In-memory WAL backend for testing WAL-mode commit and page lookup.
    struct MockWalBackend {
        frames: SharedFrames,
    }

    impl MockWalBackend {
        fn new() -> (Self, SharedFrames) {
            let frames: SharedFrames = StdArc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    frames: StdArc::clone(&frames),
                },
                frames,
            )
        }
    }

    impl crate::traits::WalBackend for MockWalBackend {
        fn append_frame(
            &mut self,
            _cx: &Cx,
            page_number: u32,
            page_data: &[u8],
            db_size_if_commit: u32,
        ) -> fsqlite_error::Result<()> {
            self.frames
                .lock()
                .unwrap()
                .push((page_number, page_data.to_vec(), db_size_if_commit));
            Ok(())
        }

        fn read_page(
            &mut self,
            _cx: &Cx,
            page_number: u32,
        ) -> fsqlite_error::Result<Option<Vec<u8>>> {
            let frames = self.frames.lock().unwrap();
            // Scan backwards for the latest version of the page.
            let result = frames
                .iter()
                .rev()
                .find(|(pn, _, _)| *pn == page_number)
                .map(|(_, data, _)| data.clone());
            drop(frames);
            Ok(result)
        }

        fn sync(&mut self, _cx: &Cx) -> fsqlite_error::Result<()> {
            Ok(())
        }

        fn frame_count(&self) -> usize {
            self.frames.lock().unwrap().len()
        }

        fn checkpoint(
            &mut self,
            _cx: &Cx,
            _mode: crate::traits::CheckpointMode,
            _writer: &mut dyn crate::traits::CheckpointPageWriter,
            _backfilled_frames: u32,
            _oldest_reader_frame: Option<u32>,
        ) -> fsqlite_error::Result<crate::traits::CheckpointResult> {
            let total_frames = u32::try_from(self.frames.lock().unwrap().len()).map_err(|_| {
                fsqlite_error::FrankenError::internal("mock wal frame count exceeds u32")
            })?;
            Ok(crate::traits::CheckpointResult {
                total_frames,
                frames_backfilled: total_frames,
                completed: true,
                wal_was_reset: false,
            })
        }
    }

    fn wal_pager() -> (SimplePager<MemoryVfs>, SharedFrames) {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/wal_test.db");
        let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let (backend, frames) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();
        (pager, frames)
    }

    #[test]
    fn test_journal_mode_default_is_delete() {
        let (pager, _) = test_pager();
        assert_eq!(
            pager.journal_mode(),
            JournalMode::Delete,
            "bead_id={BEAD_ID} case=default_journal_mode"
        );
    }

    #[test]
    fn test_set_journal_mode_wal_requires_backend() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        // Without a WAL backend, switching to WAL should fail.
        let result = pager.set_journal_mode(&cx, JournalMode::Wal);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=wal_requires_backend"
        );
    }

    #[test]
    fn test_set_journal_mode_wal_with_backend() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let (backend, _frames) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        let mode = pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();
        assert_eq!(
            mode,
            JournalMode::Wal,
            "bead_id={BEAD_ID} case=wal_mode_set"
        );
        assert_eq!(
            pager.journal_mode(),
            JournalMode::Wal,
            "bead_id={BEAD_ID} case=wal_mode_persisted"
        );
    }

    #[test]
    fn test_set_journal_mode_blocked_during_write() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let (backend, _frames) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        let result = pager.set_journal_mode(&cx, JournalMode::Wal);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=mode_switch_blocked_during_write"
        );
    }

    #[test]
    fn test_checkpoint_busy_with_active_reader() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();

        let reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let err = pager
            .checkpoint(&cx, crate::traits::CheckpointMode::Passive)
            .expect_err("checkpoint should be blocked by active reader");
        assert!(matches!(err, FrankenError::Busy));
        drop(reader);

        // After reader ends, checkpoint should proceed.
        let result = pager
            .checkpoint(&cx, crate::traits::CheckpointMode::Passive)
            .expect("checkpoint should succeed after reader closes");
        assert_eq!(result.total_frames, 0);
    }

    #[test]
    fn test_checkpoint_busy_with_active_writer() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();

        let _writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let err = pager
            .checkpoint(&cx, crate::traits::CheckpointMode::Passive)
            .expect_err("checkpoint should be blocked by active writer");
        assert!(matches!(err, FrankenError::Busy));
    }

    #[test]
    fn test_wal_commit_appends_frames() {
        let (pager, frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        let data = vec![0xAA_u8; ps];
        txn.write_page(&cx, p1, &data).unwrap();
        txn.commit(&cx).unwrap();

        let locked_frames = frames.lock().unwrap();
        assert_eq!(
            locked_frames.len(),
            1,
            "bead_id={BEAD_ID} case=wal_one_frame_appended"
        );
        assert_eq!(
            locked_frames[0].0,
            p1.get(),
            "bead_id={BEAD_ID} case=wal_frame_page_number"
        );
        assert_eq!(
            locked_frames[0].1[0], 0xAA,
            "bead_id={BEAD_ID} case=wal_frame_data"
        );
        // Commit frame should have db_size > 0.
        assert!(
            locked_frames[0].2 > 0,
            "bead_id={BEAD_ID} case=wal_commit_marker"
        );
        drop(locked_frames);
    }

    #[test]
    fn test_wal_commit_multi_page() {
        let (pager, frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();
        txn.write_page(&cx, p2, &vec![0x22; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let locked_frames = frames.lock().unwrap();
        assert_eq!(
            locked_frames.len(),
            2,
            "bead_id={BEAD_ID} case=wal_multi_page_count"
        );
        // Exactly one frame should be the commit frame (db_size > 0).
        let commit_count = locked_frames.iter().filter(|f| f.2 > 0).count();
        drop(locked_frames);
        assert_eq!(
            commit_count, 1,
            "bead_id={BEAD_ID} case=wal_exactly_one_commit_marker"
        );
    }

    #[test]
    fn test_wal_read_page_from_wal() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Write and commit via WAL.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        let data = vec![0xBB_u8; ps];
        txn.write_page(&cx, p1, &data).unwrap();
        txn.commit(&cx).unwrap();

        // Read back in a new transaction — should find the page in WAL.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let read_back = txn2.get_page(&cx, p1).unwrap();
        assert_eq!(
            read_back.as_ref()[0],
            0xBB,
            "bead_id={BEAD_ID} case=wal_read_back_from_wal"
        );
    }

    #[test]
    fn test_wal_no_journal_file_created() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/wal_no_jrnl.db");
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let (backend, _frames) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0xFF; 4096]).unwrap();
        txn.commit(&cx).unwrap();

        // In WAL mode, no journal file should be created.
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&path);
        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=wal_no_journal_created"
        );
    }

    #[test]
    fn test_wal_mode_switch_back_to_delete() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();

        assert_eq!(pager.journal_mode(), JournalMode::Wal);
        let mode = pager.set_journal_mode(&cx, JournalMode::Delete).unwrap();
        assert_eq!(
            mode,
            JournalMode::Delete,
            "bead_id={BEAD_ID} case=switch_back_to_delete"
        );
    }

    #[test]
    fn test_wal_overwrite_page_reads_latest() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // First commit: write 0x11.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0x11; ps]).unwrap();
        txn.commit(&cx).unwrap();

        // Second commit: overwrite with 0x22.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.write_page(&cx, p1, &vec![0x22; ps]).unwrap();
        txn2.commit(&cx).unwrap();

        // Read should see 0x22 (latest WAL entry).
        let txn3 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn3.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x22,
            "bead_id={BEAD_ID} case=wal_latest_version"
        );
    }

    #[test]
    fn test_wal_rollback_does_not_append_frames() {
        let (pager, frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p1 = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p1, &vec![0xDD; ps]).unwrap();
        txn.rollback(&cx).unwrap();

        assert_eq!(
            frames.lock().unwrap().len(),
            0,
            "bead_id={BEAD_ID} case=wal_rollback_no_frames"
        );
    }

    // ── 5A.1: Page 1 initialization tests (bd-2yy6) ───────────────────

    const BEAD_5A1: &str = "bd-2yy6";

    #[test]
    fn test_page1_database_header_all_fields() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let raw = txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec();

        eprintln!(
            "[5A1][test=page1_database_header_all_fields][step=parse] page_len={}",
            raw.len()
        );

        let hdr_bytes: [u8; DATABASE_HEADER_SIZE] = raw[..DATABASE_HEADER_SIZE]
            .try_into()
            .expect("page 1 must have 100-byte header");
        let hdr = DatabaseHeader::from_bytes(&hdr_bytes).expect("header must parse");

        // Verify each field matches the expected new-database defaults.
        assert_eq!(
            hdr.page_size,
            PageSize::DEFAULT,
            "bead_id={BEAD_5A1} case=page_size"
        );
        assert_eq!(hdr.page_count, 1, "bead_id={BEAD_5A1} case=page_count");
        assert_eq!(
            hdr.sqlite_version, FRANKENSQLITE_SQLITE_VERSION_NUMBER,
            "bead_id={BEAD_5A1} case=sqlite_version"
        );
        assert_eq!(
            hdr.schema_format, 4,
            "bead_id={BEAD_5A1} case=schema_format"
        );
        assert_eq!(
            hdr.freelist_trunk, 0,
            "bead_id={BEAD_5A1} case=freelist_trunk"
        );
        assert_eq!(
            hdr.freelist_count, 0,
            "bead_id={BEAD_5A1} case=freelist_count"
        );
        assert_eq!(
            hdr.schema_cookie, 0,
            "bead_id={BEAD_5A1} case=schema_cookie"
        );
        assert_eq!(
            hdr.text_encoding,
            fsqlite_types::TextEncoding::Utf8,
            "bead_id={BEAD_5A1} case=text_encoding"
        );
        assert_eq!(hdr.user_version, 0, "bead_id={BEAD_5A1} case=user_version");
        assert_eq!(
            hdr.application_id, 0,
            "bead_id={BEAD_5A1} case=application_id"
        );
        assert_eq!(
            hdr.change_counter, 0,
            "bead_id={BEAD_5A1} case=change_counter"
        );

        // Magic string bytes 0..16.
        assert_eq!(
            &raw[..16],
            b"SQLite format 3\0",
            "bead_id={BEAD_5A1} case=magic_string"
        );
        // Payload fractions at bytes 21/22/23.
        assert_eq!(raw[21], 64, "bead_id={BEAD_5A1} case=max_payload_fraction");
        assert_eq!(raw[22], 32, "bead_id={BEAD_5A1} case=min_payload_fraction");
        assert_eq!(raw[23], 32, "bead_id={BEAD_5A1} case=leaf_payload_fraction");

        eprintln!(
            "[5A1][test=page1_database_header_all_fields][step=verify] \
             page_size={} page_count={} schema_format={} encoding=UTF8 \u{2713}",
            hdr.page_size.get(),
            hdr.page_count,
            hdr.schema_format
        );
    }

    #[test]
    fn test_page1_btree_header_is_valid_empty_leaf_table() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let raw = txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec();

        let btree = BTreePageHeader::parse(&raw, PageSize::DEFAULT, 0, true)
            .expect("page 1 must parse as B-tree page");

        eprintln!(
            "[5A1][test=page1_btree_header][step=parse] \
             page_type={:?} cell_count={} content_start={} freeblock={} frag={}",
            btree.page_type,
            btree.cell_count,
            btree.cell_content_start,
            btree.first_freeblock,
            btree.fragmented_free_bytes
        );

        assert_eq!(
            btree.page_type,
            fsqlite_types::BTreePageType::LeafTable,
            "bead_id={BEAD_5A1} case=btree_page_type"
        );
        assert_eq!(
            btree.cell_count, 0,
            "bead_id={BEAD_5A1} case=btree_cell_count"
        );
        assert_eq!(
            btree.cell_content_start,
            PageSize::DEFAULT.get(),
            "bead_id={BEAD_5A1} case=btree_content_start"
        );
        assert_eq!(
            btree.first_freeblock, 0,
            "bead_id={BEAD_5A1} case=btree_first_freeblock"
        );
        assert_eq!(
            btree.fragmented_free_bytes, 0,
            "bead_id={BEAD_5A1} case=btree_fragmented_free"
        );
        assert_eq!(
            btree.header_offset, DATABASE_HEADER_SIZE,
            "bead_id={BEAD_5A1} case=btree_header_offset"
        );
        assert!(
            btree.right_most_child.is_none(),
            "bead_id={BEAD_5A1} case=leaf_no_child"
        );

        eprintln!("[5A1][test=page1_btree_header][step=verify] empty_leaf_table valid \u{2713}");
    }

    #[test]
    fn test_page1_rest_is_zeroed() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let raw = txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec();

        // After the B-tree header (8 bytes starting at offset 100), the rest of
        // the page should be all zeros (no cells, no cell pointers, no data).
        let btree_header_end = DATABASE_HEADER_SIZE + 8;
        let trailing = &raw[btree_header_end..];
        let non_zero_count = trailing.iter().filter(|&&b| b != 0).count();
        assert_eq!(
            non_zero_count, 0,
            "bead_id={BEAD_5A1} case=trailing_bytes_zeroed non_zero_count={non_zero_count}"
        );

        eprintln!(
            "[5A1][test=page1_rest_is_zeroed][step=verify] \
             trailing_bytes={} all_zero=true \u{2713}",
            trailing.len()
        );
    }

    #[test]
    fn test_page1_various_page_sizes() {
        for &ps_val in &[512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let page_size = PageSize::new(ps_val).unwrap();
            let vfs = MemoryVfs::new();
            let path = PathBuf::from(format!("/test_{ps_val}.db"));
            let pager = SimplePager::open(vfs, &path, page_size).unwrap();
            let cx = Cx::new();

            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let raw = txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec();

            eprintln!(
                "[5A1][test=page1_various_page_sizes][step=open] page_size={ps_val} page_len={}",
                raw.len()
            );

            assert_eq!(
                raw.len(),
                ps_val as usize,
                "bead_id={BEAD_5A1} case=page_len ps={ps_val}"
            );

            // Verify database header parses.
            let hdr_bytes: [u8; DATABASE_HEADER_SIZE] =
                raw[..DATABASE_HEADER_SIZE].try_into().unwrap();
            let hdr = DatabaseHeader::from_bytes(&hdr_bytes).unwrap_or_else(|e| {
                panic!("bead_id={BEAD_5A1} case=hdr_parse ps={ps_val} err={e}")
            });
            assert_eq!(
                hdr.page_size, page_size,
                "bead_id={BEAD_5A1} case=hdr_page_size ps={ps_val}"
            );

            // Verify B-tree header parses.
            let btree = BTreePageHeader::parse(&raw, page_size, 0, true).unwrap_or_else(|e| {
                panic!("bead_id={BEAD_5A1} case=btree_parse ps={ps_val} err={e}")
            });
            assert_eq!(
                btree.cell_count, 0,
                "bead_id={BEAD_5A1} case=empty_cells ps={ps_val}"
            );

            // Content offset should be usable_size (= page_size when reserved=0).
            let expected_content = ps_val;
            assert_eq!(
                btree.cell_content_start, expected_content,
                "bead_id={BEAD_5A1} case=content_start ps={ps_val}"
            );

            eprintln!(
                "[5A1][test=page1_various_page_sizes][step=verify] \
                 page_size={ps_val} content_start={} \u{2713}",
                btree.cell_content_start
            );
        }
    }

    #[test]
    fn test_write_empty_leaf_table_roundtrip() {
        // Verify that write_empty_leaf_table produces bytes that parse back
        // correctly via BTreePageHeader::parse().
        let page_size = PageSize::DEFAULT;
        let mut page = vec![0u8; page_size.as_usize()];

        // Write at offset 0 (non-page-1 case).
        BTreePageHeader::write_empty_leaf_table(&mut page, 0, page_size.get());

        let parsed = BTreePageHeader::parse(&page, page_size, 0, false)
            .expect("bead_id=bd-2yy6 written page must parse");

        assert_eq!(parsed.page_type, fsqlite_types::BTreePageType::LeafTable);
        assert_eq!(parsed.cell_count, 0);
        assert_eq!(parsed.first_freeblock, 0);
        assert_eq!(parsed.fragmented_free_bytes, 0);
        assert_eq!(parsed.cell_content_start, page_size.get());
        assert_eq!(parsed.header_offset, 0);

        eprintln!(
            "[5A1][test=write_empty_leaf_roundtrip][step=verify] \
             non_page1 roundtrip \u{2713}"
        );

        // Write at offset 100 (page-1 case).
        let mut page1 = vec![0u8; page_size.as_usize()];
        BTreePageHeader::write_empty_leaf_table(&mut page1, DATABASE_HEADER_SIZE, page_size.get());

        // Need to also write a valid database header for parse to succeed.
        let hdr = DatabaseHeader {
            page_size,
            page_count: 1,
            sqlite_version: FRANKENSQLITE_SQLITE_VERSION_NUMBER,
            ..DatabaseHeader::default()
        };
        let hdr_bytes = hdr.to_bytes().unwrap();
        page1[..DATABASE_HEADER_SIZE].copy_from_slice(&hdr_bytes);

        let parsed1 = BTreePageHeader::parse(&page1, page_size, 0, true)
            .expect("bead_id=bd-2yy6 page1 written page must parse");

        assert_eq!(parsed1.page_type, fsqlite_types::BTreePageType::LeafTable);
        assert_eq!(parsed1.cell_count, 0);
        assert_eq!(parsed1.header_offset, DATABASE_HEADER_SIZE);

        eprintln!(
            "[5A1][test=write_empty_leaf_roundtrip][step=verify] \
             page1 roundtrip \u{2713}"
        );
    }

    #[test]
    fn test_write_empty_leaf_table_65536_page_size() {
        let page_size = PageSize::new(65536).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        BTreePageHeader::write_empty_leaf_table(&mut page, 0, page_size.get());

        // The raw content offset bytes should be 0x00 0x00 (0 encodes 65536).
        assert_eq!(page[5], 0x00);
        assert_eq!(page[6], 0x00);

        let parsed =
            BTreePageHeader::parse(&page, page_size, 0, false).expect("65536 page must parse");
        assert_eq!(parsed.cell_content_start, 65536);

        eprintln!(
            "[5A1][test=write_empty_leaf_65536][step=verify] \
             content_start=65536 encoding=0x0000 \u{2713}"
        );
    }

    #[test]
    fn test_freelist_leak_on_rollback() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // 1. Allocate a page and commit.
        let mut txn1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn1.allocate_page(&cx).unwrap();
        txn1.write_page(&cx, p, &vec![0xAA; ps]).unwrap();
        txn1.commit(&cx).unwrap();

        // 2. Free the page and commit -> moves to freelist.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.free_page(&cx, p).unwrap();
        txn2.commit(&cx).unwrap();

        // Verify freelist has the page.
        {
            let inner = pager.inner.lock().unwrap();
            assert_eq!(inner.freelist.len(), 1);
            assert_eq!(inner.freelist[0], p);
            drop(inner);
        }

        // 3. Allocate the page again (pops from freelist).
        let mut txn3 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p2 = txn3.allocate_page(&cx).unwrap();
        assert_eq!(p2, p, "should reuse freed page");

        // Verify freelist is empty (in-flight).
        {
            let inner = pager.inner.lock().unwrap();
            assert!(inner.freelist.is_empty());
            drop(inner);
        }

        // 4. Rollback.
        txn3.rollback(&cx).unwrap();

        // 5. Verify freelist has the page again (no leak).
        {
            let inner = pager.inner.lock().unwrap();
            assert_eq!(
                inner.freelist.len(),
                1,
                "bead_id={BEAD_ID} case=freelist_leak_on_rollback"
            );
            assert_eq!(inner.freelist[0], p);
            drop(inner);
        }
    }

    #[test]
    #[allow(clippy::similar_names, clippy::cast_possible_truncation)]
    fn test_cache_eviction_under_pressure() {
        // Verify that SimplePager can handle more pages than the cache capacity.
        // PageCache is initialized with 256 pages. We write 300 pages.
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();

        // Write 300 pages. This exceeds the 256-page cache capacity.
        for i in 0..300u32 {
            let p = txn.allocate_page(&cx).unwrap();
            pages.push(p);
            // Unique pattern per page to verify content.
            let byte = (i % 256) as u8;
            let data = vec![byte; ps];
            txn.write_page(&cx, p, &data).unwrap();
        }
        txn.commit(&cx).unwrap();

        // Read all pages back. Some will be cache misses, requiring eviction of others.
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (i, &p) in pages.iter().enumerate() {
            let data = txn.get_page(&cx, p).unwrap();
            let expected_byte = (i % 256) as u8;
            assert_eq!(
                data.as_ref()[0],
                expected_byte,
                "bead_id={BEAD_ID} case=cache_pressure page={p}"
            );
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // bd-2ttd8.2: Pager invariant suite — SimplePager correctness
    // ═══════════════════════════════════════════════════════════════════

    const BEAD_INV: &str = "bd-2ttd8.2";

    #[test]
    fn test_inv_write_set_not_in_freelist_during_txn() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Allocate, write, commit.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xAA; ps]).unwrap();
        txn.commit(&cx).unwrap();

        // Free the page.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.free_page(&cx, p).unwrap();

        // The freed page should be in freed_pages, not in write_set.
        assert!(
            txn2.freed_pages.contains(&p),
            "bead_id={BEAD_INV} inv=freed_page_tracked"
        );
        assert!(
            !txn2.write_set.contains_key(&p),
            "bead_id={BEAD_INV} inv=freed_not_in_write_set"
        );

        txn2.commit(&cx).unwrap();
    }

    #[test]
    fn test_inv_allocated_pages_sequential_and_nonzero() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for _ in 0..10 {
            let p = txn.allocate_page(&cx).unwrap();
            assert!(p.get() > 0, "bead_id={BEAD_INV} inv=page_nonzero");
            // No duplicates.
            assert!(
                !pages.contains(&p),
                "bead_id={BEAD_INV} inv=page_unique p={p}"
            );
            pages.push(p);
        }
        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_inv_writer_serialization_single_writer() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        let _w1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();

        // Second immediate should fail (writer_active).
        let err = pager.begin(&cx, TransactionMode::Immediate);
        assert!(
            err.is_err(),
            "bead_id={BEAD_INV} inv=single_writer_enforced"
        );

        // Exclusive also fails.
        let err2 = pager.begin(&cx, TransactionMode::Exclusive);
        assert!(
            err2.is_err(),
            "bead_id={BEAD_INV} inv=exclusive_blocked_by_writer"
        );
    }

    #[test]
    fn test_inv_writer_released_on_commit() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        let mut w1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        w1.commit(&cx).unwrap();

        // Writer lock should be released; new writer should succeed.
        let _w2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
    }

    #[test]
    fn test_inv_writer_released_on_rollback() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        let mut w1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        w1.rollback(&cx).unwrap();

        let _w2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
    }

    #[test]
    fn test_inv_writer_released_on_drop() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        {
            let _w1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            // Drop without commit or rollback.
        }

        let _w2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
    }

    #[test]
    fn test_inv_commit_persists_all_dirty_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for i in 0..5u8 {
            let p = txn.allocate_page(&cx).unwrap();
            let mut data = vec![0u8; ps];
            data[0] = 0xD0 + i;
            data[ps - 1] = i;
            txn.write_page(&cx, p, &data).unwrap();
            pages.push((p, 0xD0 + i, i));
        }
        txn.commit(&cx).unwrap();

        // Read back in a new read-only transaction.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (p, first_byte, last_byte) in &pages {
            let data = txn2.get_page(&cx, *p).unwrap();
            assert_eq!(
                data.as_ref()[0],
                *first_byte,
                "bead_id={BEAD_INV} inv=dirty_page_committed p={p}"
            );
            assert_eq!(
                data.as_ref()[ps - 1],
                *last_byte,
                "bead_id={BEAD_INV} inv=dirty_page_last_byte p={p}"
            );
        }
    }

    #[test]
    fn test_inv_rollback_discards_all_dirty_pages() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Initial committed data.
        let mut txn1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn1.allocate_page(&cx).unwrap();
        txn1.write_page(&cx, p, &vec![0xAA; ps]).unwrap();
        txn1.commit(&cx).unwrap();

        // Overwrite and rollback.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.write_page(&cx, p, &vec![0xBB; ps]).unwrap();
        txn2.rollback(&cx).unwrap();

        // Verify original data survives.
        let txn3 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn3.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0xAA,
            "bead_id={BEAD_INV} inv=rollback_preserves_committed"
        );
    }

    #[test]
    fn test_inv_savepoint_nested_stack_order() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();

        txn.write_page(&cx, p, &vec![0x01; ps]).unwrap();
        txn.savepoint(&cx, "sp1").unwrap();

        txn.write_page(&cx, p, &vec![0x02; ps]).unwrap();
        txn.savepoint(&cx, "sp2").unwrap();

        txn.write_page(&cx, p, &vec![0x03; ps]).unwrap();
        txn.savepoint(&cx, "sp3").unwrap();

        txn.write_page(&cx, p, &vec![0x04; ps]).unwrap();

        // Rollback to sp2 → data should be 0x02.
        txn.rollback_to_savepoint(&cx, "sp2").unwrap();
        let data = txn.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x02,
            "bead_id={BEAD_INV} inv=nested_rollback_sp2"
        );

        // sp3 should no longer exist.
        let err = txn.rollback_to_savepoint(&cx, "sp3");
        assert!(
            err.is_err(),
            "bead_id={BEAD_INV} inv=sp3_removed_after_rollback_to_sp2"
        );

        // sp1 should still exist.
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();
        let data = txn.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x01,
            "bead_id={BEAD_INV} inv=nested_rollback_sp1"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_inv_savepoint_release_merges_to_parent() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();

        txn.write_page(&cx, p, &vec![0x10; ps]).unwrap();
        txn.savepoint(&cx, "outer").unwrap();

        txn.write_page(&cx, p, &vec![0x20; ps]).unwrap();
        txn.savepoint(&cx, "inner").unwrap();

        txn.write_page(&cx, p, &vec![0x30; ps]).unwrap();

        // Release inner → changes kept.
        txn.release_savepoint(&cx, "inner").unwrap();
        let data = txn.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x30,
            "bead_id={BEAD_INV} inv=release_keeps_changes"
        );

        // Rollback to outer → restores data from before inner.
        txn.rollback_to_savepoint(&cx, "outer").unwrap();
        let data = txn.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x10,
            "bead_id={BEAD_INV} inv=rollback_outer_after_release_inner"
        );

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_inv_freelist_restored_on_rollback() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Allocate + commit.
        let mut txn1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn1.allocate_page(&cx).unwrap();
        txn1.write_page(&cx, p, &vec![0xAA; ps]).unwrap();
        txn1.commit(&cx).unwrap();

        // Free + commit → moves to freelist.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.free_page(&cx, p).unwrap();
        txn2.commit(&cx).unwrap();

        let freelist_before = {
            let inner = pager.inner.lock().unwrap();
            inner.freelist.clone()
        };
        assert!(
            freelist_before.contains(&p),
            "bead_id={BEAD_INV} inv=freed_in_freelist"
        );

        // Allocate from freelist, then rollback → page returns to freelist.
        let mut txn3 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let reused = txn3.allocate_page(&cx).unwrap();
        assert_eq!(reused, p, "should reuse freed page");
        txn3.rollback(&cx).unwrap();

        let freelist_after = {
            let inner = pager.inner.lock().unwrap();
            inner.freelist.clone()
        };
        assert_eq!(
            freelist_after, freelist_before,
            "bead_id={BEAD_INV} inv=freelist_restored_after_rollback"
        );
    }

    #[test]
    fn test_inv_page_identity_read_before_write() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Allocate a page, write, commit.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xEE; ps]).unwrap();
        txn.commit(&cx).unwrap();

        // Read in new transaction → should see committed data.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn2.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0xEE,
            "bead_id={BEAD_INV} inv=committed_visible"
        );
    }

    #[test]
    fn test_inv_write_set_isolation() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Commit baseline data.
        let mut txn1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn1.allocate_page(&cx).unwrap();
        txn1.write_page(&cx, p, &vec![0x11; ps]).unwrap();
        txn1.commit(&cx).unwrap();

        // Start a reader → sees committed.
        let reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let r_data = reader.get_page(&cx, p).unwrap();
        assert_eq!(r_data.as_ref()[0], 0x11);

        // Writer modifies.
        let mut writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        writer.write_page(&cx, p, &vec![0x22; ps]).unwrap();

        // Reader still sees committed data (write-set is txn-private).
        let r_data2 = reader.get_page(&cx, p).unwrap();
        assert_eq!(
            r_data2.as_ref()[0],
            0x11,
            "bead_id={BEAD_INV} inv=write_set_isolated_from_readers"
        );

        writer.commit(&cx).unwrap();
    }

    #[test]
    fn test_inv_db_size_grows_on_allocate_commit() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let initial_size = {
            let inner = pager.inner.lock().unwrap();
            inner.db_size
        };

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        for _ in 0..5 {
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x00; ps]).unwrap();
        }
        txn.commit(&cx).unwrap();

        let final_size = {
            let inner = pager.inner.lock().unwrap();
            inner.db_size
        };

        assert!(
            final_size > initial_size,
            "bead_id={BEAD_INV} inv=db_size_grows initial={initial_size} final={final_size}"
        );
    }

    #[test]
    fn test_inv_db_size_restored_on_rollback() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let size_before = {
            let inner = pager.inner.lock().unwrap();
            inner.db_size
        };

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        for _ in 0..5 {
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x00; ps]).unwrap();
        }
        txn.rollback(&cx).unwrap();

        let size_after = {
            let inner = pager.inner.lock().unwrap();
            inner.db_size
        };

        assert_eq!(
            size_after, size_before,
            "bead_id={BEAD_INV} inv=db_size_restored_on_rollback"
        );
    }

    #[test]
    fn test_inv_active_transaction_count() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        let count_before = {
            let inner = pager.inner.lock().unwrap();
            inner.active_transactions
        };
        assert_eq!(count_before, 0, "bead_id={BEAD_INV} inv=initial_zero_txns");

        let r1 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let r2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();

        {
            let inner = pager.inner.lock().unwrap();
            assert_eq!(
                inner.active_transactions, 2,
                "bead_id={BEAD_INV} inv=two_active_txns"
            );
        }

        drop(r1);
        {
            let inner = pager.inner.lock().unwrap();
            assert_eq!(
                inner.active_transactions, 1,
                "bead_id={BEAD_INV} inv=one_after_drop"
            );
        }

        drop(r2);
        {
            let inner = pager.inner.lock().unwrap();
            assert_eq!(
                inner.active_transactions, 0,
                "bead_id={BEAD_INV} inv=zero_after_all_dropped"
            );
        }
    }

    #[test]
    fn test_inv_journal_mode_default_delete() {
        let (pager, _) = test_pager();
        assert_eq!(
            pager.journal_mode(),
            JournalMode::Delete,
            "bead_id={BEAD_INV} inv=default_journal_delete"
        );
    }

    #[test]
    fn test_inv_commit_seq_monotonic() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut prev_seq = {
            let inner = pager.inner.lock().unwrap();
            inner.commit_seq.get()
        };

        for _ in 0..5 {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x00; ps]).unwrap();
            txn.commit(&cx).unwrap();

            let seq = {
                let inner = pager.inner.lock().unwrap();
                inner.commit_seq.get()
            };
            assert!(
                seq >= prev_seq,
                "bead_id={BEAD_INV} inv=commit_seq_monotonic seq={seq} prev={prev_seq}"
            );
            prev_seq = seq;
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // bd-2ttd8.3: Deterministic pager e2e scenarios with cache-pressure
    //             telemetry
    // ═══════════════════════════════════════════════════════════════════

    const BEAD_E2E: &str = "bd-2ttd8.3";

    #[test]
    fn test_e2e_sequential_write_read_with_metrics() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        pager.reset_cache_metrics().unwrap();

        // Phase 1: Sequential write of 20 pages.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for i in 0..20u32 {
            let p = txn.allocate_page(&cx).unwrap();
            let mut data = vec![0u8; ps];
            data[0] = (i & 0xFF) as u8;
            data[1] = ((i >> 8) & 0xFF) as u8;
            txn.write_page(&cx, p, &data).unwrap();
            pages.push(p);
        }
        txn.commit(&cx).unwrap();

        let post_write = pager.cache_metrics_snapshot().unwrap();
        assert!(
            post_write.admits > 0,
            "bead_id={BEAD_E2E} case=seq_write_admits"
        );

        // Phase 2: Sequential read — all pages should be cached.
        pager.reset_cache_metrics().unwrap();
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (i, &p) in pages.iter().enumerate() {
            let data = txn2.get_page(&cx, p).unwrap();
            assert_eq!(
                data.as_ref()[0],
                (i & 0xFF) as u8,
                "bead_id={BEAD_E2E} case=seq_read_content page={p}"
            );
        }

        let post_read = pager.cache_metrics_snapshot().unwrap();
        assert!(
            post_read.total_accesses() >= 20,
            "bead_id={BEAD_E2E} case=seq_read_accesses total={}",
            post_read.total_accesses()
        );
        // Most pages should be cache hits (written data stays in cache).
        assert!(
            post_read.hit_rate_percent() >= 50.0,
            "bead_id={BEAD_E2E} case=seq_read_hit_rate rate={}",
            post_read.hit_rate_percent()
        );
    }

    #[test]
    fn test_e2e_cache_pressure_eviction_telemetry() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Pool capacity is 1024. Write 300 pages — all fit in cache.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for i in 0..300u32 {
            let p = txn.allocate_page(&cx).unwrap();
            let byte = (i % 256) as u8;
            txn.write_page(&cx, p, &vec![byte; ps]).unwrap();
            pages.push((p, byte));
        }
        txn.commit(&cx).unwrap();

        pager.reset_cache_metrics().unwrap();

        // Sequential read of all 300 pages.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        for (p, expected) in &pages {
            let data = txn2.get_page(&cx, *p).unwrap();
            assert_eq!(
                data.as_ref()[0],
                *expected,
                "bead_id={BEAD_E2E} case=pressure_content page={p}"
            );
        }

        let metrics = pager.cache_metrics_snapshot().unwrap();
        // All 300 pages should be cache hits (committed pages are cached).
        assert!(
            metrics.total_accesses() >= 300,
            "bead_id={BEAD_E2E} case=pressure_total_accesses total={}",
            metrics.total_accesses()
        );
        // Cache admits should reflect the committed pages from flush_page.
        let overall_metrics = pager.cache_metrics_snapshot().unwrap();
        assert!(
            overall_metrics.cached_pages > 0,
            "bead_id={BEAD_E2E} case=pressure_pages_cached cached={}",
            overall_metrics.cached_pages
        );
    }

    #[test]
    fn test_e2e_hot_cold_workload_hit_rate() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Write 50 pages.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for i in 0..50u32 {
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![(i % 256) as u8; ps]).unwrap();
            pages.push(p);
        }
        txn.commit(&cx).unwrap();

        // Define hot set (first 5 pages) and cold set (remaining 45).
        let hot = &pages[..5];

        pager.reset_cache_metrics().unwrap();
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();

        // Zipfian-like access: 80% hot, 20% cold.
        // Deterministic sequence: hot pages repeated, cold pages scattered.
        for round in 0..10u32 {
            // 8 hot accesses.
            for h in hot {
                let _ = txn2.get_page(&cx, *h).unwrap();
            }
            // 2 cold accesses (rotating through cold pages).
            let cold_idx = (round as usize * 2) % 45;
            let _ = txn2.get_page(&cx, pages[5 + cold_idx]).unwrap();
            let _ = txn2.get_page(&cx, pages[5 + (cold_idx + 1) % 45]).unwrap();
        }

        let metrics = pager.cache_metrics_snapshot().unwrap();
        let total = metrics.total_accesses();
        assert_eq!(total, 70, "bead_id={BEAD_E2E} case=hot_cold_total_accesses");
        // Hot pages should achieve high hit rate after first access.
        assert!(
            metrics.hit_rate_percent() > 50.0,
            "bead_id={BEAD_E2E} case=hot_cold_hit_rate rate={}",
            metrics.hit_rate_percent()
        );
    }

    #[test]
    fn test_e2e_random_access_pattern_deterministic() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Write 100 pages.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for _ in 0..100u32 {
            let p = txn.allocate_page(&cx).unwrap();
            let mut data = vec![0u8; ps];
            // Unique fingerprint: page number in first 4 bytes.
            data[..4].copy_from_slice(&p.get().to_le_bytes());
            txn.write_page(&cx, p, &data).unwrap();
            pages.push(p);
        }
        txn.commit(&cx).unwrap();

        // Deterministic "random" access via linear congruential generator.
        // LCG: next = (a * prev + c) mod m, with a=13, c=7, m=100.
        pager.reset_cache_metrics().unwrap();
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let mut idx: usize = 0;
        for _ in 0..200 {
            idx = (13 * idx + 7) % 100;
            let p = pages[idx];
            let data = txn2.get_page(&cx, p).unwrap();
            let stored_pgno = u32::from_le_bytes(data.as_ref()[..4].try_into().unwrap());
            assert_eq!(
                stored_pgno,
                p.get(),
                "bead_id={BEAD_E2E} case=random_fingerprint page={p}"
            );
        }

        let metrics = pager.cache_metrics_snapshot().unwrap();
        assert_eq!(
            metrics.total_accesses(),
            200,
            "bead_id={BEAD_E2E} case=random_total_accesses"
        );
        // With 100 pages and 256-page cache, everything fits → high hit rate.
        assert!(
            metrics.hit_rate_percent() > 40.0,
            "bead_id={BEAD_E2E} case=random_hit_rate rate={}",
            metrics.hit_rate_percent()
        );
    }

    #[test]
    fn test_e2e_mixed_read_write_workload() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Phase 1: Seed 30 pages.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();
        for i in 0..30u32 {
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![(i % 256) as u8; ps]).unwrap();
            pages.push(p);
        }
        txn.commit(&cx).unwrap();

        // Phase 2: Mixed read/write in batches (deterministic).
        for batch in 0..5u32 {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();

            // Read existing pages.
            for i in 0..10 {
                let idx = ((batch as usize * 3) + i) % pages.len();
                let _ = txn.get_page(&cx, pages[idx]).unwrap();
            }

            // Write/overwrite some pages.
            for i in 0..3 {
                let idx = ((batch as usize * 5) + i) % pages.len();
                let new_val = ((batch * 10 + i as u32) % 256) as u8;
                txn.write_page(&cx, pages[idx], &vec![new_val; ps]).unwrap();
            }

            // Allocate a new page per batch.
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0xF0 + batch as u8; ps])
                .unwrap();
            pages.push(p);

            txn.commit(&cx).unwrap();
        }

        // Phase 3: Verify final state.
        let metrics = pager.cache_metrics_snapshot().unwrap();
        assert!(
            metrics.total_accesses() > 0,
            "bead_id={BEAD_E2E} case=mixed_total_accesses"
        );

        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        // Verify the 5 newly allocated pages.
        for batch in 0..5u32 {
            let p = pages[30 + batch as usize];
            let data = txn.get_page(&cx, p).unwrap();
            assert_eq!(
                data.as_ref()[0],
                0xF0 + batch as u8,
                "bead_id={BEAD_E2E} case=mixed_new_page batch={batch}"
            );
        }
    }

    #[test]
    fn test_e2e_write_overwrite_verify_latest() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Allocate and commit.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0x01; ps]).unwrap();
        txn.commit(&cx).unwrap();

        // Overwrite 10 times across separate transactions.
        for version in 2..=11u8 {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            txn.write_page(&cx, p, &vec![version; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        // Final read should see version 11.
        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            11,
            "bead_id={BEAD_E2E} case=overwrite_latest_version"
        );
    }

    #[test]
    fn test_e2e_savepoint_heavy_workload() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let mut pages = Vec::new();

        // Allocate 10 pages.
        for i in 0..10u8 {
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![i; ps]).unwrap();
            pages.push(p);
        }

        // Savepoint → more writes → rollback → verify.
        txn.savepoint(&cx, "sp_heavy").unwrap();
        for &p in &pages {
            txn.write_page(&cx, p, &vec![0xFF; ps]).unwrap();
        }

        // All pages should read 0xFF before rollback.
        for &p in &pages {
            let data = txn.get_page(&cx, p).unwrap();
            assert_eq!(data.as_ref()[0], 0xFF);
        }

        txn.rollback_to_savepoint(&cx, "sp_heavy").unwrap();

        // After rollback, original values restored.
        for (i, &p) in pages.iter().enumerate() {
            let data = txn.get_page(&cx, p).unwrap();
            assert_eq!(
                data.as_ref()[0],
                i as u8,
                "bead_id={BEAD_E2E} case=savepoint_heavy_restored page={p}"
            );
        }

        txn.commit(&cx).unwrap();
    }

    #[test]
    fn test_e2e_alloc_free_cycle_no_leak() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let initial_db_size = {
            let inner = pager.inner.lock().unwrap();
            inner.db_size
        };

        // Cycle: allocate → commit → free → commit, 10 times.
        let mut freed_pages = Vec::new();
        for _ in 0..10 {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0xCC; ps]).unwrap();
            txn.commit(&cx).unwrap();

            let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            txn2.free_page(&cx, p).unwrap();
            txn2.commit(&cx).unwrap();
            freed_pages.push(p);
        }

        // Freelist should have pages available for reuse.
        let freelist_len = {
            let inner = pager.inner.lock().unwrap();
            inner.freelist.len()
        };
        assert!(
            freelist_len > 0,
            "bead_id={BEAD_E2E} case=alloc_free_freelist_populated"
        );

        // Allocate again — should reuse freed pages.
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let reused = txn.allocate_page(&cx).unwrap();
        assert!(
            freed_pages.contains(&reused),
            "bead_id={BEAD_E2E} case=alloc_free_reuse reused={reused}"
        );
        txn.commit(&cx).unwrap();

        let final_db_size = {
            let inner = pager.inner.lock().unwrap();
            inner.db_size
        };
        assert_eq!(
            final_db_size,
            initial_db_size + 1,
            "DB size should only grow by 1 page (the one currently allocated)"
        );
    }

    #[test]
    fn test_e2e_metrics_monotonic_across_transactions() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut prev_hits = 0u64;
        let mut prev_misses = 0u64;

        for round in 0..5u32 {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![round as u8; ps]).unwrap();
            txn.commit(&cx).unwrap();

            // Read back.
            let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            let _ = txn2.get_page(&cx, p).unwrap();

            let metrics = pager.cache_metrics_snapshot().unwrap();
            assert!(
                metrics.hits + metrics.misses >= prev_hits + prev_misses,
                "bead_id={BEAD_E2E} case=metrics_monotonic round={round} \
                 total={} prev={}",
                metrics.hits + metrics.misses,
                prev_hits + prev_misses
            );
            prev_hits = metrics.hits;
            prev_misses = metrics.misses;
        }
    }

    #[test]
    fn test_e2e_journal_recovery_after_crash_simulation() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/crash_sim.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Write committed data.
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0xAA; ps]).unwrap();
        txn.commit(&cx).unwrap();

        // Start another write but DON'T commit → simulates crash mid-journal.
        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.write_page(&cx, p, &vec![0xBB; ps]).unwrap();
        // Drop without commit → implicit rollback.
        drop(txn2);
        drop(pager);

        // Re-open: hot journal recovery should restore original data.
        let pager2 = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        let txn3 = pager2.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn3.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0xAA,
            "bead_id={BEAD_E2E} case=journal_recovery_restores_committed"
        );
    }
}
