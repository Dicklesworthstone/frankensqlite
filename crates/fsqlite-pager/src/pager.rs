//! Concrete single-writer pager for Phase 5 persistence.
//!
//! `SimplePager` implements [`MvccPager`] with single-writer semantics over a
//! VFS-backed database file and a zero-copy [`PageCache`].
//! Full concurrent MVCC behavior is layered on top in Phase 6.

use std::collections::{HashMap, HashSet};
use std::hint::spin_loop;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering as AtomicOrdering,
};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    BTreePageHeader, CommitSeq, DATABASE_HEADER_SIZE, DatabaseHeader,
    FRANKENSQLITE_SQLITE_VERSION_NUMBER, LockLevel, PageData, PageNumber, PageSize,
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
    freelist: Vec<PageNumber>,
    /// Current journal mode (rollback journal vs WAL).
    journal_mode: JournalMode,
    /// Whether this pager has a locally failed rollback-journal commit that
    /// must be repaired before the handle can be reused.
    rollback_journal_recovery_pending: bool,
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

    /// Read a page from the latest committed database state without consulting
    /// the local cache.
    ///
    /// This is used to refresh connection-local pager metadata after another
    /// connection has committed. The local cache may still reflect an older
    /// generation, so committed-state refresh must bypass it.
    fn read_committed_page_copy(&mut self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
        if self.journal_mode == JournalMode::Wal
            && let Some(ref mut wal) = self.wal_backend
            && let Some(wal_data) = wal.read_page(cx, page_no.get())?
        {
            return Ok(wal_data);
        }

        let page_size = self.page_size.as_usize();
        let offset = u64::from(page_no.get().saturating_sub(1)) * page_size as u64;
        let file_size = self.db_file.file_size(cx)?;
        if offset >= file_size {
            return Ok(vec![0_u8; page_size]);
        }

        let mut out = vec![0_u8; page_size];
        let bytes_read = self.db_file.read(cx, &mut out, offset)?;
        if bytes_read < page_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "short read fetching committed page {page}: got {bytes_read} of {page_size}",
                    page = page_no.get()
                ),
            });
        }
        Ok(out)
    }

    /// Refresh connection-local pager metadata from the latest committed state.
    ///
    /// Returns `true` when WAL snapshot setup was already performed as part of
    /// the refresh and does not need to be repeated for the new transaction.
    fn refresh_committed_state(&mut self, cx: &Cx) -> Result<bool> {
        let wal_post_page1_commits = if self.journal_mode == JournalMode::Wal {
            let wal = self.wal_backend.as_mut().ok_or_else(|| {
                FrankenError::internal("WAL mode active but no WAL backend installed")
            })?;
            wal.begin_transaction(cx)?;
            wal.committed_txns_since_page(cx, PageNumber::ONE.get())?
        } else {
            0
        };
        let wal_snapshot_initialized = self.journal_mode == JournalMode::Wal;

        let page1 = self.read_committed_page_copy(cx, PageNumber::ONE)?;
        if page1.len() < DATABASE_HEADER_SIZE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "committed page 1 too small for database header: got {}, need {}",
                    page1.len(),
                    DATABASE_HEADER_SIZE
                ),
            });
        }

        let mut header_bytes = [0_u8; DATABASE_HEADER_SIZE];
        header_bytes.copy_from_slice(&page1[..DATABASE_HEADER_SIZE]);
        let header = DatabaseHeader::from_bytes(&header_bytes).map_err(|error| {
            FrankenError::DatabaseCorrupt {
                detail: format!("invalid database header during pager refresh: {error}"),
            }
        })?;

        let db_size = if header.is_page_count_stale() {
            let file_size = self.db_file.file_size(cx)?;
            header
                .page_count_from_file_size(file_size)
                .unwrap_or(header.page_count)
        } else {
            header.page_count
        }
        .max(1);
        let freelist = load_freelist_from_committed_state(
            cx,
            self,
            db_size,
            header.freelist_trunk,
            header.freelist_count,
        )?;

        self.db_size = db_size;
        self.next_page = if db_size >= 2 {
            db_size.saturating_add(1)
        } else {
            2
        };
        self.freelist = freelist;

        let new_commit_seq =
            CommitSeq::new(u64::from(header.change_counter).saturating_add(wal_post_page1_commits));
        // Only clear the cache if the database was modified by another
        // connection. In WAL mode this uses the latest visible page-1
        // change-counter plus any committed transactions that occurred after
        // the most recent committed page-1 frame.
        if new_commit_seq != self.commit_seq {
            self.cache.clear();
        }
        self.commit_seq = new_commit_seq;

        Ok(wal_snapshot_initialized)
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

fn normalize_freelist(pages: &[PageNumber], db_size: u32) -> Vec<PageNumber> {
    let mut normalized: Vec<PageNumber> = pages
        .iter()
        .copied()
        .filter(|p| {
            let raw = p.get();
            raw > 1 && raw <= db_size
        })
        .collect();
    normalized.sort_unstable_by_key(|p| p.get());
    normalized.dedup_by_key(|p| p.get());
    normalized
}

fn load_freelist_from_disk<F: VfsFile>(
    cx: &Cx,
    db_file: &mut F,
    page_size: PageSize,
    db_size: u32,
    first_trunk: u32,
    freelist_count: u32,
) -> Result<Vec<PageNumber>> {
    if first_trunk == 0 || freelist_count == 0 {
        return Ok(Vec::new());
    }

    let ps = page_size.as_usize();
    let mut visited: HashSet<u32> = HashSet::new();
    let mut out: Vec<PageNumber> = Vec::with_capacity(freelist_count as usize);
    let mut trunk = first_trunk;

    while trunk != 0 && out.len() < freelist_count as usize {
        if trunk > db_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("freelist trunk page {trunk} exceeds db_size {db_size}"),
            });
        }
        if !visited.insert(trunk) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("freelist loop detected at trunk page {trunk}"),
            });
        }

        let trunk_page = PageNumber::new(trunk).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("invalid freelist trunk page number {trunk}"),
        })?;
        out.push(trunk_page);

        let mut buf = vec![0u8; ps];
        let offset = u64::from(trunk.saturating_sub(1)) * ps as u64;
        let bytes_read = db_file.read(cx, &mut buf, offset)?;
        if bytes_read < ps {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "short read loading freelist trunk page {trunk}: got {bytes_read} of {ps}"
                ),
            });
        }

        let next_trunk = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let leaf_count = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let max_leaf_entries = (ps / 4).saturating_sub(2);
        if leaf_count > max_leaf_entries {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "freelist trunk {trunk} leaf_count {leaf_count} exceeds max {max_leaf_entries}"
                ),
            });
        }

        for idx in 0..leaf_count {
            if out.len() >= freelist_count as usize {
                break;
            }
            let base = 8 + idx * 4;
            let leaf = u32::from_be_bytes([buf[base], buf[base + 1], buf[base + 2], buf[base + 3]]);
            if leaf == 0 {
                continue;
            }
            if leaf > db_size {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("freelist leaf page {leaf} exceeds db_size {db_size}"),
                });
            }
            let leaf_page = PageNumber::new(leaf).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("invalid freelist leaf page number {leaf}"),
            })?;
            out.push(leaf_page);
        }

        trunk = next_trunk;
    }

    out.truncate(freelist_count as usize);
    Ok(normalize_freelist(&out, db_size))
}

fn load_freelist_from_committed_state<F: VfsFile>(
    cx: &Cx,
    inner: &mut PagerInner<F>,
    db_size: u32,
    first_trunk: u32,
    freelist_count: u32,
) -> Result<Vec<PageNumber>> {
    if first_trunk == 0 || freelist_count == 0 {
        return Ok(Vec::new());
    }

    let ps = inner.page_size.as_usize();
    let mut visited: HashSet<u32> = HashSet::new();
    let mut out: Vec<PageNumber> = Vec::with_capacity(freelist_count as usize);
    let mut trunk = first_trunk;

    while trunk != 0 && out.len() < freelist_count as usize {
        if trunk > db_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("freelist trunk page {trunk} exceeds db_size {db_size}"),
            });
        }
        if !visited.insert(trunk) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("freelist loop detected at trunk page {trunk}"),
            });
        }

        let trunk_page = PageNumber::new(trunk).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("invalid freelist trunk page number {trunk}"),
        })?;
        out.push(trunk_page);

        let buf = inner.read_committed_page_copy(cx, trunk_page)?;
        if buf.len() < ps {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "short read loading committed freelist trunk page {trunk}: got {} of {ps}",
                    buf.len()
                ),
            });
        }

        let next_trunk = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let leaf_count = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let max_leaf_entries = (ps / 4).saturating_sub(2);
        if leaf_count > max_leaf_entries {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "freelist trunk {trunk} leaf_count {leaf_count} exceeds max {max_leaf_entries}"
                ),
            });
        }

        for idx in 0..leaf_count {
            if out.len() >= freelist_count as usize {
                break;
            }
            let base = 8 + idx * 4;
            let leaf = u32::from_be_bytes([buf[base], buf[base + 1], buf[base + 2], buf[base + 3]]);
            if leaf == 0 {
                continue;
            }
            if leaf > db_size {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("freelist leaf page {leaf} exceeds db_size {db_size}"),
                });
            }
            let leaf_page = PageNumber::new(leaf).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("invalid freelist leaf page number {leaf}"),
            })?;
            out.push(leaf_page);
        }

        trunk = next_trunk;
    }

    out.truncate(freelist_count as usize);
    Ok(normalize_freelist(&out, db_size))
}

fn serialize_freelist_to_write_set<F: VfsFile>(
    cx: &Cx,
    inner: &mut PagerInner<F>,
    write_set: &mut HashMap<PageNumber, PageBuf>,
    write_pages_sorted: &mut Vec<PageNumber>,
) -> Result<()> {
    if inner.db_size == 0 {
        inner.freelist.clear();
        return Ok(());
    }

    // Use next_page as the upper bound to preserve pages allocated from EOF by
    // rolled-back concurrent transactions.  Those pages have numbers > db_size
    // (never flushed to disk) but < next_page (validly allocated space).
    let upper_bound = inner.next_page.saturating_sub(1).max(inner.db_size);
    let freelist = normalize_freelist(&inner.freelist, upper_bound);
    inner.freelist.clone_from(&freelist);

    let ps = inner.page_size.as_usize();
    let total_free = freelist.len() as u32;

    let (first_trunk, trunk_pages) = if freelist.is_empty() {
        (0u32, Vec::<u32>::new())
    } else {
        let max_leaf_entries = (ps / 4).saturating_sub(2).max(1);
        let trunk_count = freelist.len().div_ceil(max_leaf_entries + 1);
        let trunks: Vec<u32> = freelist.iter().take(trunk_count).map(|p| p.get()).collect();
        (trunks[0], trunks)
    };

    if !trunk_pages.is_empty() {
        let mut leaf_index = trunk_pages.len();
        let max_leaf_entries = (ps / 4).saturating_sub(2).max(1);

        for (idx, trunk_pg) in trunk_pages.iter().enumerate() {
            let next = trunk_pages.get(idx + 1).copied().unwrap_or(0);
            let remaining = freelist.len().saturating_sub(leaf_index);
            let take = remaining.min(max_leaf_entries);

            let mut buf = inner.cache.pool().acquire()?;
            // Zero the entire page to avoid leaking stale data from the
            // pool in the unused tail of the trunk page.
            buf.fill(0);
            buf[0..4].copy_from_slice(&next.to_be_bytes());
            buf[4..8].copy_from_slice(&(take as u32).to_be_bytes());

            for i in 0..take {
                let leaf = freelist[leaf_index + i].get();
                let base = 8 + i * 4;
                buf[base..base + 4].copy_from_slice(&leaf.to_be_bytes());
            }
            leaf_index += take;

            if let Some(pg) = PageNumber::new(*trunk_pg) {
                if write_set.insert(pg, buf).is_none() {
                    insert_page_sorted(write_pages_sorted, pg);
                }
            }
        }
    }

    let mut page1 = ensure_page_one_in_write_set(cx, inner, write_set)?;

    page1[32..36].copy_from_slice(&first_trunk.to_be_bytes());
    page1[36..40].copy_from_slice(&total_free.to_be_bytes());
    if write_set.insert(PageNumber::ONE, page1).is_none() {
        insert_page_sorted(write_pages_sorted, PageNumber::ONE);
    }

    Ok(())
}

fn ensure_page_one_in_write_set<F: VfsFile>(
    cx: &Cx,
    inner: &mut PagerInner<F>,
    write_set: &mut HashMap<PageNumber, PageBuf>,
) -> Result<PageBuf> {
    if let Some(buf) = write_set.remove(&PageNumber::ONE) {
        return Ok(buf);
    }

    let page1_vec = inner.read_page_copy(cx, PageNumber::ONE)?;
    let mut buf = inner.cache.pool().acquire()?;
    buf.copy_from_slice(&page1_vec);
    Ok(buf)
}

fn insert_page_sorted(pages: &mut Vec<PageNumber>, page_no: PageNumber) {
    match pages.binary_search_by_key(&page_no.get(), |page| page.get()) {
        Ok(_) => {}
        Err(idx) => pages.insert(idx, page_no),
    }
}

fn remove_page_sorted(pages: &mut Vec<PageNumber>, page_no: PageNumber) {
    if let Ok(idx) = pages.binary_search_by_key(&page_no.get(), |page| page.get()) {
        pages.remove(idx);
    }
}

struct WalCommitBatch<'a> {
    new_db_size: u32,
    frames: Vec<traits::WalFrameRef<'a>>,
}

fn collect_wal_commit_batch<'a>(
    current_db_size: u32,
    write_set: &'a HashMap<PageNumber, PageBuf>,
    write_pages_sorted: &[PageNumber],
) -> Result<Option<WalCommitBatch<'a>>> {
    if write_pages_sorted.is_empty() {
        return Ok(None);
    }

    let max_written = write_pages_sorted.last().map_or(0, |page| page.get());
    let new_db_size = current_db_size.max(max_written);
    let frame_count = write_pages_sorted.len();
    let mut frames = Vec::with_capacity(frame_count);

    for (idx, page_no) in write_pages_sorted.iter().enumerate() {
        let page_data = write_set.get(page_no).ok_or_else(|| {
            FrankenError::internal(format!(
                "WAL commit batch missing page {} from write_set",
                page_no.get()
            ))
        })?;
        let db_size_if_commit = if idx + 1 == frame_count {
            new_db_size
        } else {
            0
        };

        frames.push(traits::WalFrameRef {
            page_number: page_no.get(),
            page_data,
            db_size_if_commit,
        });
    }

    Ok(Some(WalCommitBatch {
        new_db_size,
        frames,
    }))
}

const SNAPSHOT_PUBLICATION_MODE: &str = "seqlock_published_pages";
const PUBLISHED_SNAPSHOT_SPIN_LIMIT: u32 = 64;
const PUBLISHED_SNAPSHOT_SLEEP_AFTER: u32 = 512;
const PUBLISHED_SNAPSHOT_SLEEP: Duration = Duration::from_micros(50);
const PUBLISHED_READ_FAST_RETRY_LIMIT: usize = 64;
const PUBLISHED_COUNTER_STRIPE_COUNT: usize = 64;

static NEXT_PUBLISHED_COUNTER_STRIPE: AtomicUsize = AtomicUsize::new(0);

std::thread_local! {
    static PUBLISHED_COUNTER_STRIPE_INDEX: usize =
        NEXT_PUBLISHED_COUNTER_STRIPE.fetch_add(1, AtomicOrdering::Relaxed)
            % PUBLISHED_COUNTER_STRIPE_COUNT;
}

#[derive(Debug)]
#[repr(align(64))]
struct CacheAlignedAtomicU64(AtomicU64);

impl CacheAlignedAtomicU64 {
    const fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    fn fetch_add(&self, value: u64, ordering: AtomicOrdering) {
        self.0.fetch_add(value, ordering);
    }

    fn load(&self, ordering: AtomicOrdering) -> u64 {
        self.0.load(ordering)
    }
}

#[derive(Debug)]
struct StripedCounter64 {
    stripes: [CacheAlignedAtomicU64; PUBLISHED_COUNTER_STRIPE_COUNT],
}

impl StripedCounter64 {
    fn new() -> Self {
        Self {
            stripes: std::array::from_fn(|_| CacheAlignedAtomicU64::new(0)),
        }
    }

    fn increment(&self) {
        PUBLISHED_COUNTER_STRIPE_INDEX.with(|stripe| {
            self.stripes[*stripe].fetch_add(1, AtomicOrdering::Relaxed);
        });
    }

    fn load(&self) -> u64 {
        self.stripes.iter().fold(0_u64, |sum, stripe| {
            sum.saturating_add(stripe.load(AtomicOrdering::Acquire))
        })
    }
}

#[inline]
fn publication_retry_pause(retry: u32) {
    if retry < PUBLISHED_SNAPSHOT_SPIN_LIMIT {
        spin_loop();
        return;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        if retry < PUBLISHED_SNAPSHOT_SLEEP_AFTER {
            std::thread::yield_now();
        } else {
            std::thread::sleep(PUBLISHED_SNAPSHOT_SLEEP);
        }
    }
}

/// Point-in-time view of the pager metadata publication plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PagerPublishedSnapshot {
    /// Even-numbered generation identifying the current published snapshot.
    pub snapshot_gen: u64,
    /// Commit sequence visible through the publication plane.
    pub visible_commit_seq: CommitSeq,
    /// Published database size in pages.
    pub db_size: u32,
    /// Published journal mode.
    pub journal_mode: JournalMode,
    /// Published freelist length.
    pub freelist_count: usize,
    /// Whether a checkpoint is currently active.
    pub checkpoint_active: bool,
    /// Number of committed pages currently served through the publication plane.
    pub page_set_size: usize,
}

#[derive(Debug, Clone, Copy)]
struct PublishedPagerUpdate {
    visible_commit_seq: CommitSeq,
    db_size: u32,
    journal_mode: JournalMode,
    freelist_count: usize,
    checkpoint_active: bool,
}

#[derive(Debug)]
struct PublishedPagerState {
    publish_lock: Mutex<()>,
    sequence: AtomicU64,
    visible_commit_seq: AtomicU64,
    db_size: AtomicU32,
    journal_mode: AtomicU8,
    freelist_count: AtomicUsize,
    checkpoint_active: AtomicBool,
    page_set_size: AtomicUsize,
    read_retry_count: StripedCounter64,
    published_page_hits: StripedCounter64,
    pages: RwLock<HashMap<PageNumber, PageData, foldhash::fast::FixedState>>,
}

impl PublishedPagerState {
    fn new(
        db_size: u32,
        visible_commit_seq: CommitSeq,
        journal_mode: JournalMode,
        freelist_count: usize,
    ) -> Self {
        Self {
            publish_lock: Mutex::new(()),
            sequence: AtomicU64::new(2),
            visible_commit_seq: AtomicU64::new(visible_commit_seq.get()),
            db_size: AtomicU32::new(db_size),
            journal_mode: AtomicU8::new(encode_journal_mode(journal_mode)),
            freelist_count: AtomicUsize::new(freelist_count),
            checkpoint_active: AtomicBool::new(false),
            page_set_size: AtomicUsize::new(0),
            read_retry_count: StripedCounter64::new(),
            published_page_hits: StripedCounter64::new(),
            pages: RwLock::new(HashMap::with_hasher(foldhash::fast::FixedState::default())),
        }
    }

    fn snapshot(&self) -> PagerPublishedSnapshot {
        let mut retry = 0_u32;
        loop {
            let snapshot_gen = self.sequence.load(AtomicOrdering::Acquire);
            if snapshot_gen % 2 == 1 {
                self.record_retry();
                publication_retry_pause(retry);
                retry = retry.saturating_add(1);
                continue;
            }

            let visible_commit_seq =
                CommitSeq::new(self.visible_commit_seq.load(AtomicOrdering::Acquire));
            let db_size = self.db_size.load(AtomicOrdering::Acquire);
            let journal_mode = decode_journal_mode(self.journal_mode.load(AtomicOrdering::Acquire));
            let freelist_count = self.freelist_count.load(AtomicOrdering::Acquire);
            let checkpoint_active = self.checkpoint_active.load(AtomicOrdering::Acquire);
            let page_set_size = self.page_set_size.load(AtomicOrdering::Acquire);

            if self.sequence.load(AtomicOrdering::Acquire) == snapshot_gen {
                return PagerPublishedSnapshot {
                    snapshot_gen,
                    visible_commit_seq,
                    db_size,
                    journal_mode,
                    freelist_count,
                    checkpoint_active,
                    page_set_size,
                };
            }

            self.record_retry();
            publication_retry_pause(retry);
            retry = retry.saturating_add(1);
        }
    }

    fn try_get_page(&self, page_no: PageNumber) -> Option<PageData> {
        self.pages
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&page_no)
            .cloned()
    }

    fn publish<F>(&self, cx: &Cx, update: PublishedPagerUpdate, mutate_pages: F)
    where
        F: FnOnce(&mut HashMap<PageNumber, PageData, foldhash::fast::FixedState>),
    {
        let _publish_guard = self
            .publish_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.publish_locked(cx, update, mutate_pages);
    }

    fn publish_observed_page(
        &self,
        cx: &Cx,
        update: PublishedPagerUpdate,
        page_no: PageNumber,
        page: PageData,
    ) -> bool {
        let _publish_guard = self
            .publish_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_visible_commit_seq =
            CommitSeq::new(self.visible_commit_seq.load(AtomicOrdering::Acquire));
        if current_visible_commit_seq > update.visible_commit_seq {
            tracing::trace!(
                target: "fsqlite.snapshot_publication",
                trace_id = cx.trace_id(),
                run_id = "pager-publication",
                scenario_id = "stale_read_publish_skip",
                page_no = page_no.get(),
                observed_commit_seq = update.visible_commit_seq.get(),
                visible_commit_seq = current_visible_commit_seq.get(),
                publication_mode = SNAPSHOT_PUBLICATION_MODE,
                "skipping stale published page observation"
            );
            return false;
        }

        self.publish_locked(cx, update, |pages| {
            pages.insert(page_no, page);
        });
        true
    }

    fn publish_locked<F>(&self, cx: &Cx, update: PublishedPagerUpdate, mutate_pages: F)
    where
        F: FnOnce(&mut HashMap<PageNumber, PageData, foldhash::fast::FixedState>),
    {
        let start = Instant::now();
        self.sequence.fetch_add(1, AtomicOrdering::AcqRel);
        let page_set_size = {
            let mut pages = self
                .pages
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            mutate_pages(&mut pages);
            pages.len()
        };
        self.visible_commit_seq
            .store(update.visible_commit_seq.get(), AtomicOrdering::Release);
        self.db_size.store(update.db_size, AtomicOrdering::Release);
        self.journal_mode.store(
            encode_journal_mode(update.journal_mode),
            AtomicOrdering::Release,
        );
        self.freelist_count
            .store(update.freelist_count, AtomicOrdering::Release);
        self.checkpoint_active
            .store(update.checkpoint_active, AtomicOrdering::Release);
        self.page_set_size
            .store(page_set_size, AtomicOrdering::Release);
        let snapshot_gen = self
            .sequence
            .fetch_add(1, AtomicOrdering::AcqRel)
            .saturating_add(1);
        tracing::trace!(
            target: "fsqlite.snapshot_publication",
            trace_id = cx.trace_id(),
            run_id = "pager-publication",
            scenario_id = "metadata_publish",
            snapshot_gen,
            visible_commit_seq = update.visible_commit_seq.get(),
            publication_mode = SNAPSHOT_PUBLICATION_MODE,
            freelist_count = update.freelist_count,
            checkpoint_active = update.checkpoint_active,
            read_retry_count = self.read_retry_count(),
            page_set_size,
            elapsed_ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            "published pager snapshot"
        );
    }

    fn note_published_hit(&self) {
        self.published_page_hits.increment();
    }

    fn record_retry(&self) {
        self.read_retry_count.increment();
    }

    fn read_retry_count(&self) -> u64 {
        self.read_retry_count.load()
    }

    fn published_page_hits(&self) -> u64 {
        self.published_page_hits.load()
    }
}

const fn encode_journal_mode(mode: JournalMode) -> u8 {
    match mode {
        JournalMode::Delete => 0,
        JournalMode::Wal => 1,
    }
}

const fn decode_journal_mode(raw: u8) -> JournalMode {
    match raw {
        1 => JournalMode::Wal,
        _ => JournalMode::Delete,
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
    /// Published metadata/page plane for lock-light steady-state reads.
    published: Arc<PublishedPagerState>,
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

        let active_transactions_before_begin = inner.active_transactions;
        let had_recovery_pending = inner.rollback_journal_recovery_pending;
        let commit_seq_before_refresh = inner.commit_seq;

        // Acquire a SHARED lock on the database file for cross-process
        // reader/writer exclusion. The file handle lock is shared across all
        // local transactions, so only the first active transaction should
        // acquire it.
        if active_transactions_before_begin == 0 {
            inner.db_file.lock(cx, LockLevel::Shared)?;
        }

        let wal_snapshot_initialized = if active_transactions_before_begin == 0 {
            if inner.rollback_journal_recovery_pending {
                let journal_path = Self::journal_path(&self.db_path);
                let page_size = inner.page_size;
                match Self::recover_rollback_journal_if_present(
                    cx,
                    &*self.vfs,
                    &mut inner.db_file,
                    &journal_path,
                    page_size,
                ) {
                    Ok(false) => {
                        inner.db_file.unlock(cx, LockLevel::None)?;
                        return Err(FrankenError::internal(
                            "rollback journal missing while local recovery was pending",
                        ));
                    }
                    Ok(true) => {}
                    Err(err) => {
                        let _ = inner.db_file.unlock(cx, LockLevel::None);
                        return Err(err);
                    }
                }
                // Failed rollback-journal commits can leave uncommitted bytes in
                // the cache even after the on-disk pre-images are restored.
                inner.cache.clear();
                inner.rollback_journal_recovery_pending = false;
            }
            match inner.refresh_committed_state(cx) {
                Ok(v) => v,
                Err(err) => {
                    let _ = inner.db_file.unlock(cx, LockLevel::None);
                    return Err(err);
                }
            }
        } else {
            false
        };

        if active_transactions_before_begin == 0 {
            let clear_published_pages =
                had_recovery_pending || inner.commit_seq != commit_seq_before_refresh;
            self.published.publish(
                cx,
                PublishedPagerUpdate {
                    visible_commit_seq: inner.commit_seq,
                    db_size: inner.db_size,
                    journal_mode: inner.journal_mode,
                    freelist_count: inner.freelist.len(),
                    checkpoint_active: inner.checkpoint_active,
                },
                |pages| {
                    if clear_published_pages {
                        pages.clear();
                    }
                },
            );
        }

        let eager_writer = matches!(
            mode,
            TransactionMode::Immediate | TransactionMode::Exclusive
        );
        if eager_writer && inner.writer_active {
            if active_transactions_before_begin == 0 {
                inner.db_file.unlock(cx, LockLevel::None)?;
            }
            return Err(FrankenError::Busy);
        }

        // For write transactions, escalate to RESERVED to signal write intent
        // to other processes. This is a non-blocking advisory lock that
        // prevents multiple processes from writing simultaneously.
        if eager_writer {
            if let Err(err) = inner.db_file.lock(cx, LockLevel::Reserved) {
                let preserve_level = retained_lock_level_after_txn_exit(
                    active_transactions_before_begin,
                    inner.writer_active,
                );
                inner.db_file.unlock(cx, preserve_level)?;
                return Err(err);
            }
            inner.writer_active = true;
        }

        if inner.journal_mode == JournalMode::Wal && !wal_snapshot_initialized {
            let wal_begin_result = {
                let wal = inner.wal_backend.as_mut().ok_or_else(|| {
                    FrankenError::internal("WAL mode active but no WAL backend installed")
                });
                match wal {
                    Ok(w) => w.begin_transaction(cx),
                    Err(err) => {
                        if eager_writer {
                            inner.writer_active = false;
                        }
                        let preserve_level = retained_lock_level_after_txn_exit(
                            active_transactions_before_begin,
                            inner.writer_active,
                        );
                        let _ = inner.db_file.unlock(cx, preserve_level);
                        return Err(err);
                    }
                }
            };
            if let Err(err) = wal_begin_result {
                if eager_writer {
                    inner.writer_active = false;
                }
                let preserve_level = retained_lock_level_after_txn_exit(
                    active_transactions_before_begin,
                    inner.writer_active,
                );
                inner.db_file.unlock(cx, preserve_level)?;
                return Err(err);
            }
        }

        inner.active_transactions = inner.active_transactions.saturating_add(1);
        let original_db_size = inner.db_size;
        let journal_mode = inner.journal_mode;
        let pool = inner.cache.pool().clone();
        let published_visible_commit_seq = self.published.snapshot().visible_commit_seq;
        drop(inner);

        Ok(SimpleTransaction {
            vfs: Arc::clone(&self.vfs),
            journal_path: Self::journal_path(&self.db_path),
            inner: Arc::clone(&self.inner),
            published: Arc::clone(&self.published),
            published_visible_commit_seq,
            write_set: HashMap::new(),
            write_pages_sorted: Vec::new(),
            freed_pages: Vec::new(),
            allocated_from_freelist: Vec::new(),
            allocated_from_eof: Vec::new(),
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
        self.published.snapshot().journal_mode
    }

    fn set_journal_mode(&self, cx: &Cx, mode: JournalMode) -> Result<JournalMode> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;

        if inner.journal_mode == mode {
            return Ok(mode);
        }

        if inner.checkpoint_active {
            return Err(FrankenError::Busy);
        }
        if inner.active_transactions > 0 {
            // Cannot switch journal mode while any transaction is active.
            return Err(FrankenError::Busy);
        }

        if mode == JournalMode::Wal && inner.wal_backend.is_none() {
            return Err(FrankenError::Unsupported);
        }

        // Update the file format version in the database header (bytes 18-19).
        // WAL mode uses version 2; all rollback journal modes use version 1.
        // Without this, standard SQLite tools cannot detect WAL mode from the
        // on-disk header and will fail to look for the WAL file.
        let version_byte: u8 = if mode == JournalMode::Wal { 2 } else { 1 };
        if inner.db_size > 0 {
            let page_size = inner.page_size.as_usize();
            let mut page1 = vec![0u8; page_size];
            let bytes_read = inner.db_file.read(cx, &mut page1, 0)?;
            if bytes_read >= DATABASE_HEADER_SIZE {
                page1[18] = version_byte;
                page1[19] = version_byte;
                inner.db_file.write(cx, &page1, 0)?;
                inner.cache.evict(PageNumber::ONE);
            }
        }

        inner.journal_mode = mode;
        self.published.publish(
            cx,
            PublishedPagerUpdate {
                visible_commit_seq: inner.commit_seq,
                db_size: inner.db_size,
                journal_mode: inner.journal_mode,
                freelist_count: inner.freelist.len(),
                checkpoint_active: inner.checkpoint_active,
            },
            |pages| {
                pages.remove(&PageNumber::ONE);
            },
        );
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
    /// Clone the pager's VFS handle for companion-file operations.
    pub fn vfs_handle(&self) -> Arc<V> {
        Arc::clone(&self.vfs)
    }

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

    /// Capture the current published pager metadata snapshot.
    #[must_use]
    pub fn published_snapshot(&self) -> PagerPublishedSnapshot {
        self.published.snapshot()
    }

    /// Refresh the publication plane from the latest committed pager state.
    ///
    /// This is used by upper layers that need a coherent published visibility
    /// snapshot before starting a new transaction or deciding whether a
    /// connection-local execution image is stale.
    pub fn refresh_published_snapshot(&self, cx: &Cx) -> Result<PagerPublishedSnapshot> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePager lock poisoned"))?;

        if inner.active_transactions > 0 || inner.checkpoint_active {
            return Ok(self.published.snapshot());
        }

        inner.db_file.lock(cx, LockLevel::Shared)?;

        let had_recovery_pending = inner.rollback_journal_recovery_pending;
        let commit_seq_before_refresh = inner.commit_seq;

        if inner.rollback_journal_recovery_pending {
            let journal_path = Self::journal_path(&self.db_path);
            let page_size = inner.page_size;
            match Self::recover_rollback_journal_if_present(
                cx,
                &*self.vfs,
                &mut inner.db_file,
                &journal_path,
                page_size,
            ) {
                Ok(false) => {
                    inner.db_file.unlock(cx, LockLevel::None)?;
                    return Err(FrankenError::internal(
                        "rollback journal missing while local recovery was pending",
                    ));
                }
                Ok(true) => {
                    inner.cache.clear();
                    inner.rollback_journal_recovery_pending = false;
                }
                Err(err) => {
                    let _ = inner.db_file.unlock(cx, LockLevel::None);
                    return Err(err);
                }
            }
        }

        if let Err(err) = inner.refresh_committed_state(cx) {
            let _ = inner.db_file.unlock(cx, LockLevel::None);
            return Err(err);
        }

        let clear_published_pages =
            had_recovery_pending || inner.commit_seq != commit_seq_before_refresh;
        self.published.publish(
            cx,
            PublishedPagerUpdate {
                visible_commit_seq: inner.commit_seq,
                db_size: inner.db_size,
                journal_mode: inner.journal_mode,
                freelist_count: inner.freelist.len(),
                checkpoint_active: inner.checkpoint_active,
            },
            |pages| {
                if clear_published_pages {
                    pages.clear();
                }
            },
        );
        inner.db_file.unlock(cx, LockLevel::None)?;

        Ok(self.published.snapshot())
    }

    /// Number of snapshot retries steady-state readers have taken.
    #[must_use]
    pub fn published_read_retry_count(&self) -> u64 {
        self.published.read_retry_count()
    }

    /// Number of page reads satisfied directly from the publication plane.
    #[must_use]
    pub fn published_page_hits(&self) -> u64 {
        self.published.published_page_hits()
    }

    /// Number of frames currently in the WAL for this pager.
    pub fn wal_frame_count(&self) -> usize {
        let Ok(inner) = self.inner.lock() else {
            return 0;
        };
        inner
            .wal_backend
            .as_ref()
            .map_or(0, |wal| wal.frame_count())
    }

    /// Compute the journal path from the database path.
    fn journal_path(db_path: &Path) -> PathBuf {
        let mut jp = db_path.as_os_str().to_owned();
        jp.push("-journal");
        PathBuf::from(jp)
    }

    fn recover_rollback_journal_if_present(
        cx: &Cx,
        vfs: &V,
        db_file: &mut V::File,
        journal_path: &Path,
        page_size: PageSize,
    ) -> Result<bool> {
        if !vfs.access(cx, journal_path, AccessFlags::EXISTS)? {
            return Ok(false);
        }

        Self::replay_journal(cx, vfs, db_file, journal_path, page_size)?;
        let _ = vfs.delete(cx, journal_path, true);
        Ok(true)
    }

    /// Open (or create) a database and return a pager using a caller-owned
    /// capability context.
    ///
    /// If a hot journal is detected (leftover from a crash), it is replayed
    /// to restore the database to a consistent state before returning.
    #[allow(clippy::too_many_lines)]
    pub fn open_with_cx(cx: &Cx, vfs: V, path: &Path, page_size: PageSize) -> Result<Self> {
        let vfs = Arc::new(vfs);
        let flags = VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
        let (mut db_file, _actual_flags) = vfs.open(cx, Some(path), flags)?;

        let journal_path = Self::journal_path(path);
        // Hot journal recovery: if a journal file exists, replay it.
        let _ = Self::recover_rollback_journal_if_present(
            cx,
            &*vfs,
            &mut db_file,
            &journal_path,
            page_size,
        )?;

        let mut file_size = db_file.file_size(cx)?;
        let header = if file_size == 0 {
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

            db_file.write(cx, &page1, 0)?;
            db_file.sync(cx, SyncFlags::NORMAL)?;
            file_size = db_file.file_size(cx)?;
            header
        } else {
            if file_size < DATABASE_HEADER_SIZE as u64 {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!(
                        "database file too small for header: {file_size} bytes (< {DATABASE_HEADER_SIZE})"
                    ),
                });
            }

            let mut header_bytes = [0u8; DATABASE_HEADER_SIZE];
            let header_read = db_file.read(cx, &mut header_bytes, 0)?;
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
            header
        };

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
        let next_page = if db_size >= 2 {
            db_size.saturating_add(1)
        } else {
            2
        };
        let freelist = load_freelist_from_disk(
            cx,
            &mut db_file,
            page_size,
            db_size,
            header.freelist_trunk,
            header.freelist_count,
        )?;

        let initial_commit_seq = CommitSeq::new(u64::from(header.change_counter));
        let freelist_count = freelist.len();
        Ok(Self {
            vfs,
            db_path: path.to_owned(),
            inner: Arc::new(Mutex::new(PagerInner {
                db_file,
                // Generous cache size (65536 pages = 256MB) to support large
                // in-memory transactions (like 1M inserts) without OutOfMemory.
                cache: PageCache::new(page_size),
                page_size,
                db_size,
                next_page,
                writer_active: false,
                active_transactions: 0,
                checkpoint_active: false,
                freelist,
                journal_mode: JournalMode::Delete,
                rollback_journal_recovery_pending: false,
                wal_backend: None,
                commit_seq: initial_commit_seq,
            })),
            published: Arc::new(PublishedPagerState::new(
                db_size,
                initial_commit_seq,
                JournalMode::Delete,
                freelist_count,
            )),
        })
    }

    /// Open (or create) a database and return a pager.
    ///
    /// This convenience helper preserves the historical detached-context
    /// behavior for tests and legacy callers. Production paths should prefer
    /// [`Self::open_with_cx`] so trace/budget lineage is preserved.
    #[allow(clippy::too_many_lines)]
    pub fn open(vfs: V, path: &Path, page_size: PageSize) -> Result<Self> {
        let cx = Cx::new();
        Self::open_with_cx(&cx, vfs, path, page_size)
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
                // Sync after truncation to ensure durability of the new file size.
                db_file.sync(cx, SyncFlags::NORMAL)?;
            }
        }

        // Recovery is complete. Invalidate the journal before best-effort
        // deletion so a later delete failure does not keep replaying the same
        // rollback journal on every open.
        jrnl_file.truncate(cx, 0)?;
        jrnl_file.sync(cx, SyncFlags::NORMAL)?;

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
    /// Sorted unique page ids in the write-set snapshot.
    write_pages_sorted_snapshot: Vec<PageNumber>,
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
    /// Snapshot of pages allocated from EOF by this transaction.
    allocated_from_eof_snapshot: Vec<PageNumber>,
}

/// Transaction handle produced by [`SimplePager`].
pub struct SimpleTransaction<V: Vfs> {
    vfs: Arc<V>,
    journal_path: PathBuf,
    inner: Arc<Mutex<PagerInner<V::File>>>,
    published: Arc<PublishedPagerState>,
    published_visible_commit_seq: CommitSeq,
    write_set: HashMap<PageNumber, PageBuf>,
    write_pages_sorted: Vec<PageNumber>,
    freed_pages: Vec<PageNumber>,
    allocated_from_freelist: Vec<PageNumber>,
    allocated_from_eof: Vec<PageNumber>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalPageOneWritePlan {
    max_written: u32,
    page_one_dirty: bool,
    freelist_metadata_dirty: bool,
    db_growth: bool,
}

impl WalPageOneWritePlan {
    #[must_use]
    fn requires_page_one_rewrite(self) -> bool {
        self.page_one_dirty || self.freelist_metadata_dirty || self.db_growth
    }

    #[must_use]
    fn requires_page_count_advance(self) -> bool {
        self.db_growth
    }
}

impl<V: Vfs> SimpleTransaction<V> {
    /// Whether this transaction has been upgraded to a writer.
    #[must_use]
    pub fn is_writer(&self) -> bool {
        self.is_writer
    }

    #[must_use]
    fn freelist_metadata_dirty(&self) -> bool {
        !self.freed_pages.is_empty() || !self.allocated_from_freelist.is_empty()
    }

    #[must_use]
    fn classify_wal_page_one_write(
        &self,
        current_db_size: u32,
        freelist_dirty: bool,
    ) -> WalPageOneWritePlan {
        let max_written = self.write_pages_sorted.last().map_or(0, |page| page.get());
        WalPageOneWritePlan {
            max_written,
            page_one_dirty: self.write_set.contains_key(&PageNumber::ONE),
            freelist_metadata_dirty: freelist_dirty,
            db_growth: max_written > current_db_size,
        }
    }
}

impl<V> SimpleTransaction<V>
where
    V: Vfs + Send,
    V::File: Send + Sync,
{
    fn invalidate_journal_after_commit(cx: &Cx, journal_file: &mut V::File) -> Result<()> {
        journal_file.truncate(cx, 0)?;
        journal_file.sync(cx, SyncFlags::NORMAL)?;
        Ok(())
    }

    /// Commit using the rollback journal protocol.
    #[allow(clippy::too_many_lines)]
    fn commit_journal(
        cx: &Cx,
        vfs: &Arc<V>,
        journal_path: &Path,
        inner: &mut PagerInner<V::File>,
        write_set: &HashMap<PageNumber, PageBuf>,
        original_db_size: u32,
    ) -> Result<()> {
        if !write_set.is_empty() {
            // Escalate to EXCLUSIVE before writing to the database file.
            // This prevents concurrent processes from reading partially
            // written pages during the commit.
            inner.db_file.lock(cx, LockLevel::Exclusive)?;

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
            inner.rollback_journal_recovery_pending = true;

            // Phase 2: Write dirty pages to database.
            let saved_db_size = inner.db_size;
            for (page_no, data) in write_set {
                if let Err(e) = inner.flush_page(cx, *page_no, data) {
                    inner.db_size = saved_db_size;
                    return Err(e);
                }
                inner.db_size = inner.db_size.max(page_no.get());
            }

            inner.db_file.sync(cx, SyncFlags::NORMAL)?;

            // Phase 3: Make the journal non-hot before best-effort deletion.
            //
            // If directory-entry deletion fails after the database sync, a
            // leftover valid journal must not roll back the committed pages on
            // the next open.
            let cleanup_result = match Self::invalidate_journal_after_commit(cx, &mut jrnl_file) {
                Ok(()) => {
                    let _ = vfs.delete(cx, journal_path, true);
                    Ok(())
                }
                Err(invalidate_err) => {
                    if let Err(delete_err) = vfs.delete(cx, journal_path, true) {
                        Err(FrankenError::internal(format!(
                            "committed database but failed to invalidate or delete rollback journal: invalidate={invalidate_err}; delete={delete_err}"
                        )))
                    } else {
                        Ok(())
                    }
                }
            };
            inner.rollback_journal_recovery_pending = false;
            cleanup_result?;
        }

        Ok(())
    }
    /// Commit using the WAL protocol (append frames to WAL file).
    fn commit_wal(
        cx: &Cx,
        inner: &mut PagerInner<V::File>,
        write_set: &HashMap<PageNumber, PageBuf>,
        write_pages_sorted: &[PageNumber],
    ) -> Result<()> {
        if let Some(batch) = collect_wal_commit_batch(inner.db_size, write_set, write_pages_sorted)?
        {
            let mut prepared_batch = {
                let wal = inner.wal_backend.as_mut().ok_or_else(|| {
                    FrankenError::internal("WAL mode active but no WAL backend installed")
                })?;
                wal.prepare_append_frames(&batch.frames)?
            };

            // Escalate to EXCLUSIVE before writing WAL frames.
            // This prevents concurrent processes from appending to the WAL
            // simultaneously, which would cause corruption.
            inner.db_file.lock(cx, LockLevel::Exclusive)?;

            let wal = inner.wal_backend.as_mut().ok_or_else(|| {
                FrankenError::internal("WAL mode active but no WAL backend installed")
            })?;
            if let Some(prepared_batch) = prepared_batch.as_mut() {
                wal.append_prepared_frames(cx, prepared_batch)?;
            } else {
                wal.append_frames(cx, &batch.frames)?;
            }

            // Sync WAL to ensure durability.
            let wal = inner
                .wal_backend
                .as_mut()
                .ok_or_else(|| FrankenError::internal("WAL backend disappeared during commit"))?;
            wal.sync(cx)?;

            // Update db_size for any new pages.
            inner.db_size = batch.new_db_size;
        }

        Ok(())
    }

    fn ensure_writer(&mut self, cx: &Cx) -> Result<()> {
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
                // Escalate to RESERVED lock for cross-process writer exclusion.
                inner.db_file.lock(cx, LockLevel::Reserved)?;
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

const fn retained_lock_level_after_txn_exit(
    remaining_active_transactions: u32,
    writer_active: bool,
) -> LockLevel {
    if remaining_active_transactions == 0 {
        LockLevel::None
    } else if writer_active {
        LockLevel::Reserved
    } else {
        LockLevel::Shared
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

        let read_start = Instant::now();
        let mut published_retry_count = 0_usize;
        while self.published.snapshot().visible_commit_seq == self.published_visible_commit_seq {
            let snapshot = self.published.snapshot();
            if page_no.get() > snapshot.db_size {
                let confirm = self.published.snapshot();
                if confirm.snapshot_gen == snapshot.snapshot_gen {
                    tracing::trace!(
                        target: "fsqlite.snapshot_publication",
                        trace_id = cx.trace_id(),
                        run_id = "pager-publication",
                        scenario_id = "zero_fill_read",
                        snapshot_gen = snapshot.snapshot_gen,
                        visible_commit_seq = snapshot.visible_commit_seq.get(),
                        publication_mode = SNAPSHOT_PUBLICATION_MODE,
                        read_retry_count = self.published.read_retry_count(),
                        page_set_size = snapshot.page_set_size,
                        elapsed_ns =
                            u64::try_from(read_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                        "resolved zero-filled page from published metadata"
                    );
                    return Ok(PageData::from_vec(vec![0_u8; self.pool.page_size()]));
                }
                self.published.record_retry();
                if published_retry_count >= PUBLISHED_READ_FAST_RETRY_LIMIT {
                    break;
                }
                publication_retry_pause(u32::try_from(published_retry_count).unwrap_or(u32::MAX));
                published_retry_count = published_retry_count.saturating_add(1);
                continue;
            }

            if let Some(page) = self.published.try_get_page(page_no) {
                let confirm = self.published.snapshot();
                if confirm.snapshot_gen == snapshot.snapshot_gen {
                    self.published.note_published_hit();
                    tracing::trace!(
                        target: "fsqlite.snapshot_publication",
                        trace_id = cx.trace_id(),
                        run_id = "pager-publication",
                        scenario_id = "published_read_hit",
                        snapshot_gen = snapshot.snapshot_gen,
                        visible_commit_seq = snapshot.visible_commit_seq.get(),
                        publication_mode = SNAPSHOT_PUBLICATION_MODE,
                        read_retry_count = self.published.read_retry_count(),
                        page_set_size = snapshot.page_set_size,
                        elapsed_ns =
                            u64::try_from(read_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                        "served page from published snapshot"
                    );
                    return Ok(page);
                }
                self.published.record_retry();
                if published_retry_count >= PUBLISHED_READ_FAST_RETRY_LIMIT {
                    break;
                }
                publication_retry_pause(u32::try_from(published_retry_count).unwrap_or(u32::MAX));
                published_retry_count = published_retry_count.saturating_add(1);
                continue;
            }

            break;
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
        let data = inner.read_page_copy(cx, page_no)?;
        let page = PageData::from_vec(data);
        let publish_update = PublishedPagerUpdate {
            visible_commit_seq: inner.commit_seq,
            db_size: inner.db_size,
            journal_mode: inner.journal_mode,
            freelist_count: inner.freelist.len(),
            checkpoint_active: inner.checkpoint_active,
        };
        let publish_page =
            page_no.get() <= inner.db_size && inner.commit_seq == self.published_visible_commit_seq;
        drop(inner);
        if publish_page {
            self.published
                .publish_observed_page(cx, publish_update, page_no, page.clone());
        }
        Ok(page)
    }

    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.ensure_writer(cx)?;

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
        if self.write_set.insert(page_no, buf).is_none() {
            insert_page_sorted(&mut self.write_pages_sorted, page_no);
        }
        Ok(())
    }

    fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
        self.ensure_writer(cx)?;

        // First, try to reuse a page freed by this transaction.
        if let Some(page) = self.freed_pages.pop() {
            return Ok(page);
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        if let Some(page) = inner.freelist.pop() {
            self.allocated_from_freelist.push(page);
            return Ok(page);
        }

        let mut raw = inner.next_page;
        let pending_byte_page = (0x4000_0000 / inner.page_size.get()) + 1;
        if raw == pending_byte_page {
            raw = raw.saturating_add(1);
        }
        inner.next_page = raw.saturating_add(1);
        drop(inner);
        let page = PageNumber::new(raw).ok_or_else(|| FrankenError::OutOfRange {
            what: "allocated page number".to_owned(),
            value: raw.to_string(),
        })?;
        self.allocated_from_eof.push(page);
        Ok(page)
    }

    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
        self.ensure_writer(cx)?;
        if page_no == PageNumber::ONE {
            return Err(FrankenError::OutOfRange {
                what: "free page number".to_owned(),
                value: page_no.get().to_string(),
            });
        }
        if !self.freed_pages.contains(&page_no) {
            self.freed_pages.push(page_no);
        }
        if self.write_set.remove(&page_no).is_some() {
            remove_page_sorted(&mut self.write_pages_sorted, page_no);
        }
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
            let preserve_level =
                retained_lock_level_after_txn_exit(inner.active_transactions, inner.writer_active);
            let _ = inner.db_file.unlock(cx, preserve_level);
            drop(inner);
            self.committed = true;
            self.finished = true;
            return Ok(());
        }
        if !self.has_pending_writes() {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            inner.active_transactions = inner.active_transactions.saturating_sub(1);
            if self.mode != TransactionMode::Concurrent {
                inner.writer_active = false;
            }
            let preserve_level =
                retained_lock_level_after_txn_exit(inner.active_transactions, inner.writer_active);
            let _ = inner.db_file.unlock(cx, preserve_level);
            drop(inner);
            self.committed = true;
            self.finished = true;
            return Ok(());
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        let freelist_dirty = self.freelist_metadata_dirty();
        for page_no in self.freed_pages.drain(..) {
            inner.freelist.push(page_no);
        }
        if freelist_dirty {
            serialize_freelist_to_write_set(
                cx,
                &mut inner,
                &mut self.write_set,
                &mut self.write_pages_sorted,
            )?;
        }

        // In rollback-journal mode page 1 is still the durable commit beacon.
        // In WAL mode classify page-1 work by semantic trigger so later beads
        // can remove or defer each class independently.
        let wal_page1_plan = self.classify_wal_page_one_write(inner.db_size, freelist_dirty);
        let must_write_page1 = if self.journal_mode == JournalMode::Wal {
            wal_page1_plan.requires_page_one_rewrite()
        } else {
            true
        };
        if must_write_page1 {
            let mut page1 = ensure_page_one_in_write_set(cx, &mut inner, &mut self.write_set)?;
            if page1.len() >= DATABASE_HEADER_SIZE {
                let mut page_count_bytes = [0_u8; 4];
                page_count_bytes.copy_from_slice(&page1[28..32]);
                let existing_page_count = u32::from_be_bytes(page_count_bytes);
                let new_change_counter = inner.commit_seq.get().wrapping_add(1) as u32;

                // Offset 24..28: change counter (big-endian u32)
                page1[24..28].copy_from_slice(&new_change_counter.to_be_bytes());
                if self.journal_mode != JournalMode::Wal
                    || wal_page1_plan.requires_page_count_advance()
                {
                    let new_db_size = inner
                        .db_size
                        .max(wal_page1_plan.max_written)
                        .max(existing_page_count);
                    // Offset 28..32: page count (big-endian u32)
                    page1[28..32].copy_from_slice(&new_db_size.to_be_bytes());
                }
                // Offset 92..96: version-valid-for
                page1[92..96].copy_from_slice(&new_change_counter.to_be_bytes());
            }
            if self.write_set.insert(PageNumber::ONE, page1).is_none() {
                insert_page_sorted(&mut self.write_pages_sorted, PageNumber::ONE);
            }
        }

        let commit_result = if self.journal_mode == JournalMode::Wal {
            Self::commit_wal(cx, &mut inner, &self.write_set, &self.write_pages_sorted)
        } else {
            Self::commit_journal(
                cx,
                &self.vfs,
                &self.journal_path,
                &mut inner,
                &self.write_set,
                self.original_db_size,
            )
        };

        if commit_result.is_ok() {
            inner.commit_seq = inner.commit_seq.next();
            inner.active_transactions = inner.active_transactions.saturating_sub(1);
            if self.mode != TransactionMode::Concurrent {
                inner.writer_active = false;
            }
            self.published.publish(
                cx,
                PublishedPagerUpdate {
                    visible_commit_seq: inner.commit_seq,
                    db_size: inner.db_size,
                    journal_mode: inner.journal_mode,
                    freelist_count: inner.freelist.len(),
                    checkpoint_active: inner.checkpoint_active,
                },
                |pages| {
                    for (&page_no, buf) in &self.write_set {
                        pages.insert(page_no, PageData::from_vec(buf.to_vec()));
                    }
                    pages.retain(|page_no, _| page_no.get() <= inner.db_size);
                },
            );

            // Move newly written pages from the write_set directly into the read cache.
            // This ensures subsequent readers see cache hits for newly committed data.
            for (page_no, buf) in self.write_set.drain() {
                inner.cache.insert_buffer(page_no, buf);
            }
            self.write_pages_sorted.clear();

            let preserve_level =
                retained_lock_level_after_txn_exit(inner.active_transactions, inner.writer_active);
            let _ = inner.db_file.unlock(cx, preserve_level);

            drop(inner);
            self.committed = true;
            self.finished = true;
        } else {
            // Keep the writer lock held on commit failure so no other writer
            // can interleave while the caller decides to retry or roll back.
            drop(inner);
        }
        commit_result
    }

    fn is_writer(&self) -> bool {
        self.is_writer
    }

    fn has_pending_writes(&self) -> bool {
        !self.write_set.is_empty() || !self.freed_pages.is_empty()
    }

    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.write_set.clear();
        self.write_pages_sorted.clear();
        self.freed_pages.clear();
        self.savepoint_stack.clear();
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;

        let restored_from_journal = if self.is_writer
            && self.journal_mode != JournalMode::Wal
            && inner.rollback_journal_recovery_pending
        {
            let page_size = inner.page_size;
            if !SimplePager::<V>::recover_rollback_journal_if_present(
                cx,
                &*self.vfs,
                &mut inner.db_file,
                &self.journal_path,
                page_size,
            )? {
                return Err(FrankenError::internal(
                    "rollback journal missing while failed commit recovery was pending",
                ));
            }
            true
        } else {
            false
        };

        if restored_from_journal {
            inner.cache.clear();
            inner.refresh_committed_state(cx)?;
            inner.rollback_journal_recovery_pending = false;
            self.allocated_from_freelist.clear();
            self.allocated_from_eof.clear();
            if self.mode != TransactionMode::Concurrent {
                inner.writer_active = false;
            }
        } else {
            // Restore pages allocated from the freelist.
            for page in self.allocated_from_freelist.drain(..) {
                inner.freelist.push(page);
            }

            if self.is_writer && self.mode != TransactionMode::Concurrent {
                inner.db_size = self.original_db_size;

                // Reset next_page to avoid holes if we allocated pages that are now discarded.
                // Logic matches SimplePager::open.
                let db_size = inner.db_size;
                inner.next_page = if db_size >= 2 {
                    db_size.saturating_add(1)
                } else {
                    2
                };

                inner.writer_active = false;
            } else if self.is_writer && self.mode == TransactionMode::Concurrent {
                // For concurrent transactions, only return pages to the freelist
                // if they are within the current committed db_size. Pages beyond
                // db_size were allocated from EOF and should be "dropped" by
                // just not being reused, as they were never part of a committed
                // state and cannot be safely referenced in a trunk page if
                // db_size is not also extended.
                let current_db_size = inner.db_size;
                for page in self.allocated_from_eof.drain(..) {
                    if page.get() <= current_db_size {
                        inner.freelist.push(page);
                    }
                }
            }
        }
        inner.active_transactions = inner.active_transactions.saturating_sub(1);
        let preserve_level =
            retained_lock_level_after_txn_exit(inner.active_transactions, inner.writer_active);
        let _ = inner.db_file.unlock(cx, preserve_level);
        drop(inner);
        if self.is_writer {
            // Delete any partial journal file.
            let _ = self.vfs.delete(cx, &self.journal_path, true);
        }
        self.committed = false;
        self.finished = true;
        Ok(())
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: fsqlite_types::WitnessKey) {}

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
            write_pages_sorted_snapshot: self.write_pages_sorted.clone(),
            freed_pages_snapshot: self.freed_pages.clone(),
            next_page_snapshot: inner.next_page,
            freelist_snapshot: inner.freelist.clone(),
            allocated_from_freelist_snapshot: self.allocated_from_freelist.clone(),
            allocated_from_eof_snapshot: self.allocated_from_eof.clone(),
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

        let entry = &self.savepoint_stack[pos];

        // Restore write-set FIRST to ensure we don't leave the transaction in an
        // inconsistent state if PageBuf allocation fails (OOM).
        let new_write_set = entry
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

        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| FrankenError::internal("SimpleTransaction lock poisoned"))?;
            if self.mode != TransactionMode::Concurrent {
                inner.next_page = entry.next_page_snapshot;
                inner.freelist.clone_from(&entry.freelist_snapshot);
            } else {
                for page in self
                    .allocated_from_eof
                    .drain(entry.allocated_from_eof_snapshot.len()..)
                {
                    inner.freelist.push(page);
                }
                for page in self
                    .allocated_from_freelist
                    .drain(entry.allocated_from_freelist_snapshot.len()..)
                {
                    inner.freelist.push(page);
                }
            }
        }
        self.allocated_from_freelist = entry.allocated_from_freelist_snapshot.clone();
        self.allocated_from_eof = entry.allocated_from_eof_snapshot.clone();
        self.freed_pages = entry.freed_pages_snapshot.clone();
        self.write_set = new_write_set;
        self.write_pages_sorted = entry.write_pages_sorted_snapshot.clone();

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
                inner.next_page = if db_size >= 2 {
                    db_size.saturating_add(1)
                } else {
                    2
                };

                inner.writer_active = false;
            } else if self.is_writer && self.mode == TransactionMode::Concurrent {
                for page in self.allocated_from_eof.drain(..) {
                    inner.freelist.push(page);
                }
            }
            inner.active_transactions = inner.active_transactions.saturating_sub(1);
            let preserve_level =
                retained_lock_level_after_txn_exit(inner.active_transactions, inner.writer_active);
            // Best-effort downgrade/release. Drop has no caller-owned context,
            // so cleanup uses a detached Cx only for this final unlock.
            let cx = Cx::new();
            let _ = inner.db_file.unlock(&cx, preserve_level);
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
    published: Arc<PublishedPagerState>,
}

impl<V: Vfs> traits::sealed::Sealed for SimplePagerCheckpointWriter<V> where V::File: Send + Sync {}

impl<V> SimplePagerCheckpointWriter<V>
where
    V: Vfs + Send + Sync,
    V::File: Send + Sync,
{
    /// Patch page 1 header fields that must remain globally consistent.
    ///
    /// This ensures external SQLite readers see:
    /// - a valid change counter (24..28),
    /// - the true on-disk page count (28..32),
    /// - matching version-valid-for (92..96).
    fn patch_page1_header(inner: &mut PagerInner<V::File>, cx: &Cx) -> Result<()> {
        // SQLite databases always keep page 1 when non-empty.
        if inner.db_size == 0 {
            return Ok(());
        }

        let page_size = inner.page_size.as_usize();
        let mut page1 = vec![0u8; page_size];
        let bytes_read = inner.db_file.read(cx, &mut page1, 0)?;
        if bytes_read < DATABASE_HEADER_SIZE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "short read while patching page 1 header: got {bytes_read} bytes, need at least {DATABASE_HEADER_SIZE}",
                ),
            });
        }

        let new_page_count = inner.db_size;

        page1[28..32].copy_from_slice(&new_page_count.to_be_bytes());
        inner.db_file.write(cx, &page1, 0)?;
        inner.cache.evict(PageNumber::ONE);
        Ok(())
    }
}

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

        // Update db_size if this page extends the database.
        inner.db_size = inner.db_size.max(page_no.get());

        // Write directly to the database file, bypassing the cache.
        // The WAL checkpoint is authoritative, so we overwrite any cached version.
        let page_size = inner.page_size.as_usize();
        let offset = u64::from(page_no.get() - 1) * page_size as u64;

        // For page 1, repair header fields after writing the frame bytes.
        // A final repair also occurs in sync() so page_count remains correct
        // even when page 1 was checkpointed before higher-numbered pages.
        inner.db_file.write(cx, data, offset)?;
        if page_no == PageNumber::ONE && data.len() >= DATABASE_HEADER_SIZE {
            Self::patch_page1_header(&mut inner, cx)?;
        }

        // Invalidate cache entry if present to avoid stale reads.
        inner.cache.evict(page_no);
        if page_no == PageNumber::ONE {
            self.published.publish(
                cx,
                PublishedPagerUpdate {
                    visible_commit_seq: inner.commit_seq,
                    db_size: inner.db_size,
                    journal_mode: inner.journal_mode,
                    freelist_count: inner.freelist.len(),
                    checkpoint_active: inner.checkpoint_active,
                },
                |pages| {
                    pages.remove(&PageNumber::ONE);
                },
            );
        }

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
        for pgno in (n_pages.saturating_add(1))..=old_db_size {
            if let Some(page_no) = PageNumber::new(pgno) {
                inner.cache.evict(page_no);
            }
        }
        self.published.publish(
            cx,
            PublishedPagerUpdate {
                visible_commit_seq: inner.commit_seq,
                db_size: inner.db_size,
                journal_mode: inner.journal_mode,
                freelist_count: inner.freelist.len(),
                checkpoint_active: inner.checkpoint_active,
            },
            |pages| {
                pages.retain(|page_no, _| page_no.get() <= n_pages);
                pages.remove(&PageNumber::ONE);
            },
        );

        drop(inner);
        Ok(())
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| FrankenError::internal("SimplePagerCheckpointWriter lock poisoned"))?;
        // Ensure header page_count reflects the final db_size after all
        // checkpoint writes/truncation, even if page 1 was checkpointed early.
        Self::patch_page1_header(&mut inner, cx)?;
        self.published.publish(
            cx,
            PublishedPagerUpdate {
                visible_commit_seq: inner.commit_seq,
                db_size: inner.db_size,
                journal_mode: inner.journal_mode,
                freelist_count: inner.freelist.len(),
                checkpoint_active: inner.checkpoint_active,
            },
            |pages| {
                pages.remove(&PageNumber::ONE);
            },
        );
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
            published: Arc::clone(&self.published),
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
        let wal = {
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
            self.published.publish(
                cx,
                PublishedPagerUpdate {
                    visible_commit_seq: inner.commit_seq,
                    db_size: inner.db_size,
                    journal_mode: inner.journal_mode,
                    freelist_count: inner.freelist.len(),
                    checkpoint_active: inner.checkpoint_active,
                },
                |_| {},
            );
            wal
        };
        // Lock is released here.

        struct CheckpointGuard<'a, F: VfsFile> {
            inner: &'a std::sync::Mutex<PagerInner<F>>,
            published: &'a PublishedPagerState,
            wal: Option<Box<dyn WalBackend>>,
        }

        impl<F: VfsFile> Drop for CheckpointGuard<'_, F> {
            fn drop(&mut self) {
                if let Ok(mut inner) = self.inner.lock() {
                    if let Some(wal) = self.wal.take() {
                        inner.wal_backend = Some(wal);
                    }
                    inner.checkpoint_active = false;
                    self.published.publish(
                        &Cx::new(),
                        PublishedPagerUpdate {
                            visible_commit_seq: inner.commit_seq,
                            db_size: inner.db_size,
                            journal_mode: inner.journal_mode,
                            freelist_count: inner.freelist.len(),
                            checkpoint_active: inner.checkpoint_active,
                        },
                        |_| {},
                    );
                }
            }
        }

        let mut guard = CheckpointGuard {
            inner: &self.inner,
            published: self.published.as_ref(),
            wal: Some(wal),
        };

        // Create a checkpoint writer that writes directly to the database file.
        let mut writer = self.checkpoint_writer();

        // Run the checkpoint from the beginning. Reader-aware incremental
        // checkpointing requires exposing oldest-reader tracking from pager.
        guard
            .wal
            .as_mut()
            .expect("wal was just inserted")
            .checkpoint(cx, mode, &mut writer, 0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{MvccPager, TransactionHandle, TransactionMode};
    use fsqlite_types::PageSize;
    use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
    use fsqlite_types::{BTreePageHeader, DatabaseHeader};
    use fsqlite_vfs::{MemoryFile, MemoryVfs, Vfs, VfsFile};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, OnceLock};

    const BEAD_ID: &str = "bd-bca.1";

    fn init_publication_test_tracing() {
        static TRACING_INIT: OnceLock<()> = OnceLock::new();
        TRACING_INIT.get_or_init(|| {
            if tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::Level::TRACE)
                .with_test_writer()
                .try_init()
                .is_err()
            {
                // Another test already installed a global subscriber.
            }
        });
    }

    fn test_pager() -> (SimplePager<MemoryVfs>, PathBuf) {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/test.db");
        let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        (pager, path)
    }

    #[derive(Clone, Default)]
    struct JournalDeleteFailVfs {
        inner: MemoryVfs,
    }

    impl JournalDeleteFailVfs {
        fn new() -> Self {
            Self {
                inner: MemoryVfs::new(),
            }
        }
    }

    impl Vfs for JournalDeleteFailVfs {
        type File = MemoryFile;

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn open(
            &self,
            cx: &Cx,
            path: Option<&std::path::Path>,
            flags: VfsOpenFlags,
        ) -> Result<(Self::File, VfsOpenFlags)> {
            self.inner.open(cx, path, flags)
        }

        fn delete(&self, cx: &Cx, path: &std::path::Path, sync_dir: bool) -> Result<()> {
            if path.to_string_lossy().ends_with("-journal") {
                return Err(FrankenError::internal(
                    "simulated journal delete failure".to_owned(),
                ));
            }
            self.inner.delete(cx, path, sync_dir)
        }

        fn access(&self, cx: &Cx, path: &std::path::Path, flags: AccessFlags) -> Result<bool> {
            self.inner.access(cx, path, flags)
        }

        fn full_pathname(&self, cx: &Cx, path: &std::path::Path) -> Result<PathBuf> {
            self.inner.full_pathname(cx, path)
        }
    }

    #[derive(Clone)]
    struct ObservedLockVfs {
        inner: MemoryVfs,
        observed_lock_level: Arc<Mutex<LockLevel>>,
    }

    impl ObservedLockVfs {
        fn new() -> Self {
            Self {
                inner: MemoryVfs::new(),
                observed_lock_level: Arc::new(Mutex::new(LockLevel::None)),
            }
        }

        fn observed_lock_level(&self) -> Arc<Mutex<LockLevel>> {
            Arc::clone(&self.observed_lock_level)
        }
    }

    struct ObservedLockFile {
        inner: MemoryFile,
        observed_lock_level: Arc<Mutex<LockLevel>>,
    }

    impl Vfs for ObservedLockVfs {
        type File = ObservedLockFile;

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn open(
            &self,
            cx: &Cx,
            path: Option<&std::path::Path>,
            flags: VfsOpenFlags,
        ) -> Result<(Self::File, VfsOpenFlags)> {
            let (inner, actual_flags) = self.inner.open(cx, path, flags)?;
            Ok((
                ObservedLockFile {
                    inner,
                    observed_lock_level: self.observed_lock_level(),
                },
                actual_flags,
            ))
        }

        fn delete(&self, cx: &Cx, path: &std::path::Path, sync_dir: bool) -> Result<()> {
            self.inner.delete(cx, path, sync_dir)
        }

        fn access(&self, cx: &Cx, path: &std::path::Path, flags: AccessFlags) -> Result<bool> {
            self.inner.access(cx, path, flags)
        }

        fn full_pathname(&self, cx: &Cx, path: &std::path::Path) -> Result<PathBuf> {
            self.inner.full_pathname(cx, path)
        }
    }

    impl VfsFile for ObservedLockFile {
        fn close(&mut self, cx: &Cx) -> Result<()> {
            let result = self.inner.close(cx);
            if result.is_ok() {
                *self.observed_lock_level.lock().unwrap() = LockLevel::None;
            }
            result
        }

        fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.inner.read(cx, buf, offset)
        }

        fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
            self.inner.write(cx, buf, offset)
        }

        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
            self.inner.truncate(cx, size)
        }

        fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
            self.inner.sync(cx, flags)
        }

        fn file_size(&self, cx: &Cx) -> Result<u64> {
            self.inner.file_size(cx)
        }

        fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
            self.inner.lock(cx, level)?;
            *self.observed_lock_level.lock().unwrap() = level;
            Ok(())
        }

        fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
            self.inner.unlock(cx, level)?;
            *self.observed_lock_level.lock().unwrap() = level;
            Ok(())
        }

        fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
            self.inner.check_reserved_lock(cx)
        }

        fn sector_size(&self) -> u32 {
            self.inner.sector_size()
        }

        fn device_characteristics(&self) -> u32 {
            self.inner.device_characteristics()
        }

        fn shm_map(
            &mut self,
            cx: &Cx,
            region: u32,
            size: u32,
            extend: bool,
        ) -> Result<fsqlite_vfs::ShmRegion> {
            self.inner.shm_map(cx, region, size, extend)
        }

        fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
            self.inner.shm_lock(cx, offset, n, flags)
        }

        fn shm_barrier(&self) {
            self.inner.shm_barrier();
        }

        fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
            self.inner.shm_unmap(cx, delete)
        }
    }

    fn observed_lock_pager() -> (SimplePager<ObservedLockVfs>, Arc<Mutex<LockLevel>>) {
        let vfs = ObservedLockVfs::new();
        let observed_lock_level = vfs.observed_lock_level();
        let path = PathBuf::from("/observed-lock.db");
        let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        (pager, observed_lock_level)
    }

    #[derive(Debug, Default)]
    struct ExclusiveLockMetrics {
        owner: Option<u64>,
        acquired_at: Option<Instant>,
        acquisition_count: usize,
        hold_samples_ns: Vec<u64>,
        wait_samples_ns: Vec<u64>,
    }

    #[derive(Clone)]
    struct BlockingObservedLockVfs {
        inner: MemoryVfs,
        observed_lock_level: Arc<Mutex<LockLevel>>,
        next_handle_id: StdArc<AtomicU64>,
        exclusive_metrics: StdArc<(StdMutex<ExclusiveLockMetrics>, StdCondvar)>,
    }

    impl BlockingObservedLockVfs {
        fn new() -> Self {
            Self {
                inner: MemoryVfs::new(),
                observed_lock_level: Arc::new(Mutex::new(LockLevel::None)),
                next_handle_id: StdArc::new(AtomicU64::new(1)),
                exclusive_metrics: StdArc::new((
                    StdMutex::new(ExclusiveLockMetrics::default()),
                    StdCondvar::new(),
                )),
            }
        }

        fn observed_lock_level(&self) -> Arc<Mutex<LockLevel>> {
            Arc::clone(&self.observed_lock_level)
        }

        fn wait_for_exclusive_acquisitions(&self, target: usize) {
            let (metrics_lock, metrics_ready) = &*self.exclusive_metrics;
            let mut metrics = metrics_lock.lock().unwrap();
            while metrics.acquisition_count < target {
                metrics = metrics_ready.wait(metrics).unwrap();
            }
        }

        fn exclusive_hold_samples_ns(&self) -> Vec<u64> {
            let (metrics_lock, _) = &*self.exclusive_metrics;
            metrics_lock.lock().unwrap().hold_samples_ns.clone()
        }

        fn exclusive_wait_samples_ns(&self) -> Vec<u64> {
            let (metrics_lock, _) = &*self.exclusive_metrics;
            metrics_lock.lock().unwrap().wait_samples_ns.clone()
        }

        fn clear_exclusive_metrics(&self) {
            let (metrics_lock, _) = &*self.exclusive_metrics;
            let mut metrics = metrics_lock.lock().unwrap();
            *metrics = ExclusiveLockMetrics::default();
        }
    }

    struct BlockingObservedLockFile {
        inner: MemoryFile,
        observed_lock_level: Arc<Mutex<LockLevel>>,
        handle_id: u64,
        lock_level: LockLevel,
        exclusive_metrics: StdArc<(StdMutex<ExclusiveLockMetrics>, StdCondvar)>,
    }

    impl BlockingObservedLockFile {
        fn release_exclusive_hold(&self) {
            let (metrics_lock, metrics_ready) = &*self.exclusive_metrics;
            let mut metrics = metrics_lock.lock().unwrap();
            if metrics.owner == Some(self.handle_id) {
                if let Some(acquired_at) = metrics.acquired_at.take() {
                    metrics
                        .hold_samples_ns
                        .push(u64::try_from(acquired_at.elapsed().as_nanos()).unwrap_or(u64::MAX));
                }
                metrics.owner = None;
                metrics_ready.notify_all();
            }
        }
    }

    impl Vfs for BlockingObservedLockVfs {
        type File = BlockingObservedLockFile;

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn open(
            &self,
            cx: &Cx,
            path: Option<&std::path::Path>,
            flags: VfsOpenFlags,
        ) -> Result<(Self::File, VfsOpenFlags)> {
            let (inner, actual_flags) = self.inner.open(cx, path, flags)?;
            Ok((
                BlockingObservedLockFile {
                    inner,
                    observed_lock_level: self.observed_lock_level(),
                    handle_id: self.next_handle_id.fetch_add(1, AtomicOrdering::Relaxed),
                    lock_level: LockLevel::None,
                    exclusive_metrics: StdArc::clone(&self.exclusive_metrics),
                },
                actual_flags,
            ))
        }

        fn delete(&self, cx: &Cx, path: &std::path::Path, sync_dir: bool) -> Result<()> {
            self.inner.delete(cx, path, sync_dir)
        }

        fn access(&self, cx: &Cx, path: &std::path::Path, flags: AccessFlags) -> Result<bool> {
            self.inner.access(cx, path, flags)
        }

        fn full_pathname(&self, cx: &Cx, path: &std::path::Path) -> Result<PathBuf> {
            self.inner.full_pathname(cx, path)
        }
    }

    impl VfsFile for BlockingObservedLockFile {
        fn close(&mut self, cx: &Cx) -> Result<()> {
            self.release_exclusive_hold();
            let result = self.inner.close(cx);
            if result.is_ok() {
                self.lock_level = LockLevel::None;
                *self.observed_lock_level.lock().unwrap() = LockLevel::None;
            }
            result
        }

        fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.inner.read(cx, buf, offset)
        }

        fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
            self.inner.write(cx, buf, offset)
        }

        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
            self.inner.truncate(cx, size)
        }

        fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
            self.inner.sync(cx, flags)
        }

        fn file_size(&self, cx: &Cx) -> Result<u64> {
            self.inner.file_size(cx)
        }

        fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
            if self.lock_level < LockLevel::Exclusive && level >= LockLevel::Exclusive {
                let wait_started = Instant::now();
                let (metrics_lock, metrics_ready) = &*self.exclusive_metrics;
                let mut metrics = metrics_lock.lock().unwrap();
                while metrics.owner.is_some() && metrics.owner != Some(self.handle_id) {
                    metrics = metrics_ready.wait(metrics).unwrap();
                }
                metrics
                    .wait_samples_ns
                    .push(u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                metrics.owner = Some(self.handle_id);
                metrics.acquired_at = Some(Instant::now());
                metrics.acquisition_count = metrics.acquisition_count.saturating_add(1);
                metrics_ready.notify_all();
            }

            self.inner.lock(cx, level)?;
            if self.lock_level < level {
                self.lock_level = level;
            }
            *self.observed_lock_level.lock().unwrap() = self.lock_level;
            Ok(())
        }

        fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
            if self.lock_level >= LockLevel::Exclusive && level < LockLevel::Exclusive {
                self.release_exclusive_hold();
            }

            self.inner.unlock(cx, level)?;
            if self.lock_level > level {
                self.lock_level = level;
            }
            *self.observed_lock_level.lock().unwrap() = self.lock_level;
            Ok(())
        }

        fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
            self.inner.check_reserved_lock(cx)
        }

        fn sector_size(&self) -> u32 {
            self.inner.sector_size()
        }

        fn device_characteristics(&self) -> u32 {
            self.inner.device_characteristics()
        }

        fn shm_map(
            &mut self,
            cx: &Cx,
            region: u32,
            size: u32,
            extend: bool,
        ) -> Result<fsqlite_vfs::ShmRegion> {
            self.inner.shm_map(cx, region, size, extend)
        }

        fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
            self.inner.shm_lock(cx, offset, n, flags)
        }

        fn shm_barrier(&self) {
            self.inner.shm_barrier();
        }

        fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
            self.inner.shm_unmap(cx, delete)
        }
    }

    #[derive(Debug)]
    struct DbWriteFailState {
        target_path: PathBuf,
        armed: bool,
        remaining_successful_db_writes: usize,
    }

    #[derive(Clone)]
    struct DbWriteFailOnceVfs {
        inner: MemoryVfs,
        state: Arc<Mutex<DbWriteFailState>>,
    }

    impl DbWriteFailOnceVfs {
        fn new(target_path: PathBuf) -> Self {
            Self {
                inner: MemoryVfs::new(),
                state: Arc::new(Mutex::new(DbWriteFailState {
                    target_path,
                    armed: false,
                    remaining_successful_db_writes: 0,
                })),
            }
        }

        fn arm_after_db_writes(&self, successful_db_writes_before_failure: usize) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.armed = true;
            state.remaining_successful_db_writes = successful_db_writes_before_failure;
        }
    }

    #[derive(Debug)]
    struct DbWriteFailOnceFile {
        inner: MemoryFile,
        state: Arc<Mutex<DbWriteFailState>>,
        is_target_db: bool,
    }

    impl Vfs for DbWriteFailOnceVfs {
        type File = DbWriteFailOnceFile;

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn open(
            &self,
            cx: &Cx,
            path: Option<&std::path::Path>,
            flags: VfsOpenFlags,
        ) -> Result<(Self::File, VfsOpenFlags)> {
            let (inner, actual_flags) = self.inner.open(cx, path, flags)?;
            let is_target_db = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                path == Some(state.target_path.as_path()) && flags.contains(VfsOpenFlags::MAIN_DB)
            };
            Ok((
                DbWriteFailOnceFile {
                    inner,
                    state: Arc::clone(&self.state),
                    is_target_db,
                },
                actual_flags,
            ))
        }

        fn delete(&self, cx: &Cx, path: &std::path::Path, sync_dir: bool) -> Result<()> {
            self.inner.delete(cx, path, sync_dir)
        }

        fn access(&self, cx: &Cx, path: &std::path::Path, flags: AccessFlags) -> Result<bool> {
            self.inner.access(cx, path, flags)
        }

        fn full_pathname(&self, cx: &Cx, path: &std::path::Path) -> Result<PathBuf> {
            self.inner.full_pathname(cx, path)
        }
    }

    impl VfsFile for DbWriteFailOnceFile {
        fn close(&mut self, cx: &Cx) -> Result<()> {
            self.inner.close(cx)
        }

        fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.inner.read(cx, buf, offset)
        }

        fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
            if self.is_target_db {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.armed {
                    if state.remaining_successful_db_writes == 0 {
                        state.armed = false;
                        return Err(FrankenError::Io(std::io::Error::other(
                            "simulated main-db write failure",
                        )));
                    }
                    state.remaining_successful_db_writes -= 1;
                }
            }
            self.inner.write(cx, buf, offset)
        }

        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
            self.inner.truncate(cx, size)
        }

        fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
            self.inner.sync(cx, flags)
        }

        fn file_size(&self, cx: &Cx) -> Result<u64> {
            self.inner.file_size(cx)
        }

        fn lock(&mut self, cx: &Cx, level: fsqlite_types::LockLevel) -> Result<()> {
            self.inner.lock(cx, level)
        }

        fn unlock(&mut self, cx: &Cx, level: fsqlite_types::LockLevel) -> Result<()> {
            self.inner.unlock(cx, level)
        }

        fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
            self.inner.check_reserved_lock(cx)
        }

        fn sector_size(&self) -> u32 {
            self.inner.sector_size()
        }

        fn device_characteristics(&self) -> u32 {
            self.inner.device_characteristics()
        }

        fn shm_map(
            &mut self,
            cx: &Cx,
            region: u32,
            size: u32,
            extend: bool,
        ) -> Result<fsqlite_vfs::ShmRegion> {
            self.inner.shm_map(cx, region, size, extend)
        }

        fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
            self.inner.shm_lock(cx, offset, n, flags)
        }

        fn shm_barrier(&self) {
            self.inner.shm_barrier();
        }

        fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
            self.inner.shm_unmap(cx, delete)
        }
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
    fn test_begin_refreshes_external_page_growth_before_allocation() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/pager_refresh_external_growth.db");
        let pager1 = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let pager2 = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut writer1 = pager1.begin(&cx, TransactionMode::Immediate).unwrap();
        let page2 = writer1.allocate_page(&cx).unwrap();
        assert_eq!(page2.get(), 2, "first writer should allocate page 2");
        writer1.write_page(&cx, page2, &vec![0xAB; ps]).unwrap();
        writer1.commit(&cx).unwrap();

        let mut writer2 = pager2.begin(&cx, TransactionMode::Immediate).unwrap();
        let page3 = writer2.allocate_page(&cx).unwrap();
        assert_eq!(
            page3.get(),
            3,
            "bead_id={BEAD_ID} case=refresh_external_growth_reissues_next_page"
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
        let ps = PageSize::DEFAULT.as_usize();

        // 1. Allocate a page and commit.
        let mut txn1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn1.allocate_page(&cx).unwrap();
        txn1.write_page(&cx, p, &vec![0_u8; ps]).unwrap();
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
        assert_eq!(
            p2,
            p,
            "bead_id={BEAD_ID} case=freelist_reuse p3={} p1={}",
            p2.get(),
            p.get()
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
    fn test_reader_exit_preserves_shared_lock_for_other_reader() {
        let (pager, observed_lock_level) = observed_lock_pager();
        let cx = Cx::new();

        let mut reader1 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let reader2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();

        assert_eq!(*observed_lock_level.lock().unwrap(), LockLevel::Shared);

        reader1.commit(&cx).unwrap();

        assert_eq!(
            *observed_lock_level.lock().unwrap(),
            LockLevel::Shared,
            "bead_id={BEAD_ID} case=reader_commit_keeps_shared_for_other_reader"
        );

        drop(reader2);

        assert_eq!(
            *observed_lock_level.lock().unwrap(),
            LockLevel::None,
            "bead_id={BEAD_ID} case=last_reader_releases_shared"
        );
    }

    #[test]
    fn test_reader_exit_preserves_reserved_lock_for_active_writer() {
        let (pager, observed_lock_level) = observed_lock_pager();
        let cx = Cx::new();

        let mut writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();

        assert_eq!(*observed_lock_level.lock().unwrap(), LockLevel::Reserved);

        drop(reader);

        assert_eq!(
            *observed_lock_level.lock().unwrap(),
            LockLevel::Reserved,
            "bead_id={BEAD_ID} case=reader_drop_keeps_reserved_for_writer"
        );

        writer.commit(&cx).unwrap();

        assert_eq!(
            *observed_lock_level.lock().unwrap(),
            LockLevel::None,
            "bead_id={BEAD_ID} case=writer_commit_releases_last_lock"
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
        for (i, &p) in allocated_pages.iter().enumerate() {
            let data = txn2.get_page(&cx, p).unwrap();
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

        // Commit failure should keep the writer lock held on commit failure so no other writer
        // can interleave while the caller decides to retry or roll back.
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
    fn test_rollback_recovers_after_partial_commit_failure() {
        let path = PathBuf::from("/rollback_after_failed_commit.db");
        let journal_path = SimplePager::<DbWriteFailOnceVfs>::journal_path(&path);
        let vfs = DbWriteFailOnceVfs::new(path.clone());
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let original_two = vec![0x11; ps];
        let original_three = vec![0x44; ps];
        let (page_two, page_three) = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            let page_three = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &original_two).unwrap();
            txn.write_page(&cx, page_three, &original_three).unwrap();
            txn.commit(&cx).unwrap();
            (page_two, page_three)
        };

        vfs.arm_after_db_writes(1);

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, page_two, &vec![0x22; ps]).unwrap();
        txn.write_page(&cx, page_three, &vec![0x55; ps]).unwrap();

        let err = txn.commit(&cx).unwrap_err();
        assert!(
            matches!(err, FrankenError::Io(_)),
            "bead_id={BEAD_ID} case=partial_commit_surfaces_io_error"
        );

        txn.rollback(&cx).unwrap();

        let reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            reader.get_page(&cx, page_two).unwrap().into_vec(),
            original_two
        );
        assert_eq!(
            reader.get_page(&cx, page_three).unwrap().into_vec(),
            original_three
        );

        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=rollback_removes_failed_commit_journal"
        );

        let reopened = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let reopened_reader = reopened.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            reopened_reader.get_page(&cx, page_two).unwrap().into_vec(),
            vec![0x11; ps]
        );
        assert_eq!(
            reopened_reader
                .get_page(&cx, page_three)
                .unwrap()
                .into_vec(),
            vec![0x44; ps]
        );
    }

    #[test]
    fn test_begin_recovers_abandoned_failed_commit() {
        let path = PathBuf::from("/begin_recovers_failed_commit.db");
        let journal_path = SimplePager::<DbWriteFailOnceVfs>::journal_path(&path);
        let vfs = DbWriteFailOnceVfs::new(path.clone());
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let original_two = vec![0x61; ps];
        let original_three = vec![0x73; ps];
        let (page_two, page_three) = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            let page_three = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &original_two).unwrap();
            txn.write_page(&cx, page_three, &original_three).unwrap();
            txn.commit(&cx).unwrap();
            (page_two, page_three)
        };

        vfs.arm_after_db_writes(1);

        {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            txn.write_page(&cx, page_two, &vec![0x62; ps]).unwrap();
            txn.write_page(&cx, page_three, &vec![0x74; ps]).unwrap();
            let err = txn.commit(&cx).unwrap_err();
            assert!(
                matches!(err, FrankenError::Io(_)),
                "bead_id={BEAD_ID} case=abandoned_failed_commit_surfaces_io_error"
            );
        }

        let reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            reader.get_page(&cx, page_two).unwrap().into_vec(),
            original_two
        );
        assert_eq!(
            reader.get_page(&cx, page_three).unwrap().into_vec(),
            original_three
        );

        assert!(
            !vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=next_begin_cleans_abandoned_failed_commit_journal"
        );

        let reopened = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let reopened_reader = reopened.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            reopened_reader.get_page(&cx, page_two).unwrap().into_vec(),
            vec![0x61; ps]
        );
        assert_eq!(
            reopened_reader
                .get_page(&cx, page_three)
                .unwrap()
                .into_vec(),
            vec![0x73; ps]
        );
    }

    #[test]
    fn test_commit_survives_journal_delete_failure() {
        let vfs = JournalDeleteFailVfs::new();
        let path = PathBuf::from("/journal_delete_failure_commit.db");
        let journal_path = SimplePager::<JournalDeleteFailVfs>::journal_path(&path);
        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let page_two = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0xAB; ps]).unwrap();
            txn.commit(&cx).unwrap();
            page_two
        };

        let reopened = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let reader = reopened.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            reader.get_page(&cx, page_two).unwrap().into_vec(),
            vec![0xAB; ps]
        );

        assert!(
            vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=delete_failure_leaves_journal_inode"
        );

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
        let (journal_file, _) = vfs.open(&cx, Some(&journal_path), flags).unwrap();
        assert_eq!(
            journal_file.file_size(&cx).unwrap(),
            0,
            "bead_id={BEAD_ID} case=delete_failure_still_invalidates_journal"
        );
    }

    #[test]
    fn test_hot_journal_recovery_survives_delete_failure() {
        let vfs = JournalDeleteFailVfs::new();
        let path = PathBuf::from("/journal_delete_failure_recovery.db");
        let journal_path = SimplePager::<JournalDeleteFailVfs>::journal_path(&path);
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        {
            let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();

            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
            let (mut db_file, _) = vfs.open(&cx, Some(&path), flags).unwrap();

            let header = JournalHeader {
                page_count: 1,
                nonce: 0x4652_414E,
                initial_db_size: 2,
                sector_size: 512,
                page_size: PageSize::DEFAULT.get(),
            };
            let hdr_bytes = header.encode_padded();
            let jrnl_flags =
                VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
            let (mut jrnl_file, _) = vfs.open(&cx, Some(&journal_path), jrnl_flags).unwrap();
            jrnl_file.write(&cx, &hdr_bytes, 0).unwrap();
            let record = JournalPageRecord::new(2, vec![0x11; ps], header.nonce);
            jrnl_file
                .write(&cx, &record.encode(), hdr_bytes.len() as u64)
                .unwrap();
            jrnl_file.sync(&cx, SyncFlags::NORMAL).unwrap();

            let page_offset = PageSize::DEFAULT.as_usize() as u64;
            db_file.write(&cx, &vec![0x22; ps], page_offset).unwrap();
            db_file.sync(&cx, SyncFlags::NORMAL).unwrap();
        }

        let reopened = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let reader = reopened.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let page_two = PageNumber::new(2).unwrap();
        assert_eq!(
            reader.get_page(&cx, page_two).unwrap().into_vec(),
            vec![0x11; ps]
        );

        assert!(
            vfs.access(&cx, &journal_path, AccessFlags::EXISTS).unwrap(),
            "bead_id={BEAD_ID} case=recovery_delete_failure_leaves_journal_inode"
        );

        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_JOURNAL;
        let (journal_file, _) = vfs.open(&cx, Some(&journal_path), flags).unwrap();
        assert_eq!(
            journal_file.file_size(&cx).unwrap(),
            0,
            "bead_id={BEAD_ID} case=recovery_delete_failure_still_invalidates_journal"
        );
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
            let p = txn.allocate_page(&cx).unwrap();
            assert_eq!(p.get(), 2);
            txn.write_page(&cx, p, &vec![0x11; ps]).unwrap();
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
            txn.write_page(&cx, p1, &vec![0xAB; ps]).unwrap();
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
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let journal_path = SimplePager::<MemoryVfs>::journal_path(&pager.db_path);

        let mut txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        txn.commit(&cx).unwrap();

        assert!(
            !pager
                .vfs
                .access(&cx, &journal_path, AccessFlags::EXISTS)
                .unwrap(),
            "bead_id={BEAD_ID} case=journal_deleted_for_readonly"
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
        assert_eq!(
            data.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=rollback_to_preserves_savepoint"
        );

        txn.commit(&cx).unwrap();
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

        // Modify again.
        txn.write_page(&cx, p1, &vec![0x33; ps]).unwrap();
        txn.rollback_to_savepoint(&cx, "sp1").unwrap();

        let data = txn.get_page(&cx, p1).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0x11,
            "bead_id={BEAD_ID} case=rollback_to_preserves_savepoint_multi"
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

    use fsqlite_wal::checksum::{WAL_FRAME_HEADER_SIZE, WalChecksumTransform};
    use fsqlite_wal::wal::WalAppendFrameRef;
    use fsqlite_wal::{WalFile, WalSalts};
    use serde_json::json;
    use std::sync::{Arc as StdArc, Condvar as StdCondvar, Mutex as StdMutex};
    use std::time::Instant;

    /// (page_number, page_data, db_size_if_commit)
    type WalFrame = (u32, Vec<u8>, u32);
    type SharedFrames = StdArc<StdMutex<Vec<WalFrame>>>;
    type SharedCounter = StdArc<StdMutex<usize>>;
    type SharedLockLevels = StdArc<StdMutex<Vec<LockLevel>>>;
    const TRACK_C_BATCH_BENCH_BEAD_ID: &str = "bd-db300.3.1.4";
    const TRACK_C_PUBLISH_WINDOW_INVENTORY_BEAD_ID: &str = "bd-db300.3.2.1";
    const TRACK_C_PUBLISH_WINDOW_BENCH_BEAD_ID: &str = "bd-db300.3.2.3";
    const TRACK_C_BATCH_BENCH_WARMUP_ITERS: usize = 5;
    const TRACK_C_BATCH_BENCH_MEASURE_ITERS: usize = 25;
    const TRACK_C_BATCH_BENCH_CASES: [(&str, usize); 3] = [
        ("page1_plus_1_new_page", 2),
        ("page1_plus_7_new_pages", 8),
        ("page1_plus_31_new_pages", 32),
    ];
    const TRACK_C_PUBLISH_WINDOW_BENCH_WARMUP_ITERS: usize = 5;
    const TRACK_C_PUBLISH_WINDOW_BENCH_MEASURE_ITERS: usize = 20;
    const TRACK_C_PUBLISH_WINDOW_BENCH_CASES: [(&str, usize); 3] = [
        ("interior_only_1_dirty_page", 1),
        ("interior_only_7_dirty_pages", 7),
        ("interior_only_31_dirty_pages", 31),
    ];
    const TRACK_C_METADATA_BENCH_BEAD_ID: &str = "bd-db300.3.3.3";
    const TRACK_C_METADATA_BENCH_WARMUP_ITERS: usize = 5;
    const TRACK_C_METADATA_BENCH_MEASURE_ITERS: usize = 25;
    const TRACK_C_METADATA_BENCH_CASES: [(&str, usize); 3] = [
        ("interior_only_1_dirty_page", 1),
        ("interior_only_7_dirty_pages", 7),
        ("interior_only_31_dirty_pages", 31),
    ];

    /// In-memory WAL backend for testing WAL-mode commit and page lookup.
    struct MockWalBackend {
        frames: SharedFrames,
        begin_calls: SharedCounter,
        batch_calls: SharedCounter,
    }

    impl MockWalBackend {
        fn new() -> (Self, SharedFrames, SharedCounter, SharedCounter) {
            let frames: SharedFrames = StdArc::new(StdMutex::new(Vec::new()));
            let begin_calls: SharedCounter = StdArc::new(StdMutex::new(0));
            let batch_calls: SharedCounter = StdArc::new(StdMutex::new(0));
            (
                Self {
                    frames: StdArc::clone(&frames),
                    begin_calls: StdArc::clone(&begin_calls),
                    batch_calls: StdArc::clone(&batch_calls),
                },
                frames,
                begin_calls,
                batch_calls,
            )
        }

        fn with_shared_frames(frames: SharedFrames) -> (Self, SharedCounter, SharedCounter) {
            let begin_calls: SharedCounter = StdArc::new(StdMutex::new(0));
            let batch_calls: SharedCounter = StdArc::new(StdMutex::new(0));
            (
                Self {
                    frames,
                    begin_calls: StdArc::clone(&begin_calls),
                    batch_calls: StdArc::clone(&batch_calls),
                },
                begin_calls,
                batch_calls,
            )
        }
    }

    struct PreparedBatchObservedWalBackend {
        frames: SharedFrames,
        append_frames_calls: SharedCounter,
        append_prepared_calls: SharedCounter,
        prepare_lock_levels: SharedLockLevels,
        append_lock_levels: SharedLockLevels,
        observed_lock_level: Arc<Mutex<LockLevel>>,
    }

    impl PreparedBatchObservedWalBackend {
        fn new(
            observed_lock_level: Arc<Mutex<LockLevel>>,
        ) -> (
            Self,
            SharedFrames,
            SharedCounter,
            SharedCounter,
            SharedLockLevels,
            SharedLockLevels,
        ) {
            let frames: SharedFrames = StdArc::new(StdMutex::new(Vec::new()));
            let append_frames_calls: SharedCounter = StdArc::new(StdMutex::new(0));
            let append_prepared_calls: SharedCounter = StdArc::new(StdMutex::new(0));
            let prepare_lock_levels: SharedLockLevels = StdArc::new(StdMutex::new(Vec::new()));
            let append_lock_levels: SharedLockLevels = StdArc::new(StdMutex::new(Vec::new()));

            (
                Self {
                    frames: StdArc::clone(&frames),
                    append_frames_calls: StdArc::clone(&append_frames_calls),
                    append_prepared_calls: StdArc::clone(&append_prepared_calls),
                    prepare_lock_levels: StdArc::clone(&prepare_lock_levels),
                    append_lock_levels: StdArc::clone(&append_lock_levels),
                    observed_lock_level,
                },
                frames,
                append_frames_calls,
                append_prepared_calls,
                prepare_lock_levels,
                append_lock_levels,
            )
        }
    }

    #[derive(Clone, Copy)]
    enum TrackCBatchMode {
        SingleFrame,
        Batched,
    }

    impl TrackCBatchMode {
        const fn as_str(self) -> &'static str {
            match self {
                Self::SingleFrame => "single_frame",
                Self::Batched => "batch_append",
            }
        }
    }

    #[derive(Clone, Copy)]
    enum TrackCPublishWindowMode {
        InlinePrepareBaseline,
        PreparedCandidate,
    }

    impl TrackCPublishWindowMode {
        const fn as_str(self) -> &'static str {
            match self {
                Self::InlinePrepareBaseline => "inline_prepare_baseline",
                Self::PreparedCandidate => "prepared_candidate",
            }
        }
    }

    #[derive(Clone, Copy)]
    enum TrackCMetadataMode {
        ForcedPageOneBaseline,
        SemanticCleanupCandidate,
    }

    impl TrackCMetadataMode {
        const fn as_str(self) -> &'static str {
            match self {
                Self::ForcedPageOneBaseline => "forced_page_one_baseline",
                Self::SemanticCleanupCandidate => "semantic_cleanup_candidate",
            }
        }
    }

    struct TrackCBenchmarkWalBackend {
        wal: WalFile<MemoryFile>,
        mode: TrackCBatchMode,
    }

    struct TrackCPublishWindowBenchWalBackend {
        wal: WalFile<BlockingObservedLockFile>,
        mode: TrackCPublishWindowMode,
    }

    impl TrackCBenchmarkWalBackend {
        fn new(vfs: &MemoryVfs, cx: &Cx, path: &std::path::Path, mode: TrackCBatchMode) -> Self {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
            let (file, _) = vfs.open(cx, Some(path), flags).unwrap();
            let wal = WalFile::create(
                cx,
                file,
                PageSize::DEFAULT.get(),
                0,
                WalSalts {
                    salt1: 0xDB30_0314,
                    salt2: 0xC1C1_C1C1,
                },
            )
            .unwrap();
            Self { wal, mode }
        }
    }

    impl TrackCPublishWindowBenchWalBackend {
        fn new(
            vfs: &BlockingObservedLockVfs,
            cx: &Cx,
            path: &std::path::Path,
            mode: TrackCPublishWindowMode,
        ) -> Self {
            let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
            let (file, _) = vfs.open(cx, Some(path), flags).unwrap();
            let wal = WalFile::create(
                cx,
                file,
                PageSize::DEFAULT.get(),
                0,
                WalSalts {
                    salt1: 0xDB30_0323,
                    salt2: 0xC2C3_C2C3,
                },
            )
            .unwrap();
            Self { wal, mode }
        }
    }

    impl crate::traits::WalBackend for TrackCBenchmarkWalBackend {
        fn append_frame(
            &mut self,
            cx: &Cx,
            page_number: u32,
            page_data: &[u8],
            db_size_if_commit: u32,
        ) -> fsqlite_error::Result<()> {
            self.wal
                .append_frame(cx, page_number, page_data, db_size_if_commit)
        }

        fn append_frames(
            &mut self,
            cx: &Cx,
            frames: &[crate::traits::WalFrameRef<'_>],
        ) -> fsqlite_error::Result<()> {
            match self.mode {
                TrackCBatchMode::SingleFrame => {
                    for frame in frames {
                        self.wal.append_frame(
                            cx,
                            frame.page_number,
                            frame.page_data,
                            frame.db_size_if_commit,
                        )?;
                    }
                    Ok(())
                }
                TrackCBatchMode::Batched => {
                    let wal_frames: Vec<_> = frames
                        .iter()
                        .map(|frame| WalAppendFrameRef {
                            page_number: frame.page_number,
                            page_data: frame.page_data,
                            db_size_if_commit: frame.db_size_if_commit,
                        })
                        .collect();
                    self.wal.append_frames(cx, &wal_frames)
                }
            }
        }

        fn read_page(
            &mut self,
            _cx: &Cx,
            _page_number: u32,
        ) -> fsqlite_error::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn sync(&mut self, cx: &Cx) -> fsqlite_error::Result<()> {
            self.wal.sync(cx, SyncFlags::NORMAL)
        }

        fn frame_count(&self) -> usize {
            self.wal.frame_count()
        }

        fn checkpoint(
            &mut self,
            _cx: &Cx,
            _mode: crate::traits::CheckpointMode,
            _writer: &mut dyn crate::traits::CheckpointPageWriter,
            _backfilled_frames: u32,
            _oldest_reader_frame: Option<u32>,
        ) -> fsqlite_error::Result<crate::traits::CheckpointResult> {
            let total_frames = u32::try_from(self.wal.frame_count()).unwrap_or(u32::MAX);
            Ok(crate::traits::CheckpointResult {
                total_frames,
                frames_backfilled: 0,
                completed: false,
                wal_was_reset: false,
            })
        }
    }

    impl crate::traits::WalBackend for TrackCPublishWindowBenchWalBackend {
        fn append_frame(
            &mut self,
            cx: &Cx,
            page_number: u32,
            page_data: &[u8],
            db_size_if_commit: u32,
        ) -> fsqlite_error::Result<()> {
            self.wal
                .append_frame(cx, page_number, page_data, db_size_if_commit)
        }

        fn append_frames(
            &mut self,
            cx: &Cx,
            frames: &[crate::traits::WalFrameRef<'_>],
        ) -> fsqlite_error::Result<()> {
            let wal_frames: Vec<_> = frames
                .iter()
                .map(|frame| WalAppendFrameRef {
                    page_number: frame.page_number,
                    page_data: frame.page_data,
                    db_size_if_commit: frame.db_size_if_commit,
                })
                .collect();
            self.wal.append_frames(cx, &wal_frames)
        }

        fn prepare_append_frames(
            &mut self,
            frames: &[crate::traits::WalFrameRef<'_>],
        ) -> fsqlite_error::Result<Option<crate::traits::PreparedWalFrameBatch>> {
            if matches!(self.mode, TrackCPublishWindowMode::InlinePrepareBaseline) {
                return Ok(None);
            }

            if frames.is_empty() {
                return Ok(None);
            }

            let wal_frames: Vec<_> = frames
                .iter()
                .map(|frame| WalAppendFrameRef {
                    page_number: frame.page_number,
                    page_data: frame.page_data,
                    db_size_if_commit: frame.db_size_if_commit,
                })
                .collect();
            let frame_size = WAL_FRAME_HEADER_SIZE + wal_frames[0].page_data.len();
            let frame_metas = wal_frames
                .iter()
                .map(|frame| crate::traits::PreparedWalFrameMeta {
                    page_number: frame.page_number,
                    db_size_if_commit: frame.db_size_if_commit,
                })
                .collect();
            let frame_bytes = self.wal.prepare_frame_bytes(&wal_frames)?;
            let checksum_transforms = wal_frames
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let frame_start = index
                        .checked_mul(frame_size)
                        .expect("frame start fits usize");
                    let frame_end = frame_start
                        .checked_add(frame_size)
                        .expect("frame end fits usize");
                    let transform = WalChecksumTransform::for_wal_frame(
                        &frame_bytes[frame_start..frame_end],
                        self.wal.page_size(),
                        self.wal.big_endian_checksum(),
                    )?;
                    Ok(crate::traits::PreparedWalChecksumTransform {
                        a11: transform.a11,
                        a12: transform.a12,
                        a21: transform.a21,
                        a22: transform.a22,
                        c1: transform.c1,
                        c2: transform.c2,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Some(crate::traits::PreparedWalFrameBatch {
                frame_size,
                page_data_offset: WAL_FRAME_HEADER_SIZE,
                frame_metas,
                checksum_transforms,
                frame_bytes,
            }))
        }

        fn append_prepared_frames(
            &mut self,
            cx: &Cx,
            prepared: &mut crate::traits::PreparedWalFrameBatch,
        ) -> fsqlite_error::Result<()> {
            let checksum_transforms: Vec<_> = prepared
                .checksum_transforms
                .iter()
                .map(|transform| WalChecksumTransform {
                    a11: transform.a11,
                    a12: transform.a12,
                    a21: transform.a21,
                    a22: transform.a22,
                    c1: transform.c1,
                    c2: transform.c2,
                })
                .collect();
            self.wal.append_prepared_frame_bytes(
                cx,
                &mut prepared.frame_bytes,
                &checksum_transforms,
            )
        }

        fn read_page(
            &mut self,
            _cx: &Cx,
            _page_number: u32,
        ) -> fsqlite_error::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn sync(&mut self, cx: &Cx) -> fsqlite_error::Result<()> {
            self.wal.sync(cx, SyncFlags::NORMAL)
        }

        fn frame_count(&self) -> usize {
            self.wal.frame_count()
        }

        fn checkpoint(
            &mut self,
            _cx: &Cx,
            _mode: crate::traits::CheckpointMode,
            _writer: &mut dyn crate::traits::CheckpointPageWriter,
            _backfilled_frames: u32,
            _oldest_reader_frame: Option<u32>,
        ) -> fsqlite_error::Result<crate::traits::CheckpointResult> {
            let total_frames = u32::try_from(self.wal.frame_count()).unwrap_or(u32::MAX);
            Ok(crate::traits::CheckpointResult {
                total_frames,
                frames_backfilled: 0,
                completed: false,
                wal_was_reset: false,
            })
        }
    }

    fn track_c_prepared_commit(
        mode: TrackCBatchMode,
        dirty_pages: usize,
    ) -> (Cx, SimpleTransaction<MemoryVfs>) {
        assert!(dirty_pages >= 2);

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let db_path = PathBuf::from(format!(
            "/track_c_batch_commit_{}_{}.db",
            mode.as_str(),
            dirty_pages
        ));
        let wal_path = PathBuf::from(format!(
            "/track_c_batch_commit_{}_{}.db-wal",
            mode.as_str(),
            dirty_pages
        ));
        let pager = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        let backend = TrackCBenchmarkWalBackend::new(&vfs, &cx, &wal_path, mode);
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_bytes = PageSize::DEFAULT.as_usize();
        txn.write_page(&cx, PageNumber::ONE, &vec![0xA1; page_bytes])
            .unwrap();
        for page_idx in 1..dirty_pages {
            let page_no = txn.allocate_page(&cx).unwrap();
            let fill = u8::try_from((page_idx % 251) + 1).unwrap();
            txn.write_page(&cx, page_no, &vec![fill; page_bytes])
                .unwrap();
        }
        (cx, txn)
    }

    fn track_c_measure_commit_ns(mode: TrackCBatchMode, dirty_pages: usize) -> Vec<u64> {
        let total_iters = TRACK_C_BATCH_BENCH_WARMUP_ITERS + TRACK_C_BATCH_BENCH_MEASURE_ITERS;
        let mut samples = Vec::with_capacity(TRACK_C_BATCH_BENCH_MEASURE_ITERS);

        for iter_idx in 0..total_iters {
            let (cx, mut txn) = track_c_prepared_commit(mode, dirty_pages);
            let started = Instant::now();
            txn.commit(&cx).unwrap();
            if iter_idx >= TRACK_C_BATCH_BENCH_WARMUP_ITERS {
                let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                samples.push(elapsed_ns);
            }
        }

        samples
    }

    fn track_c_metadata_seed_existing_pages(
        pager: &SimplePager<MemoryVfs>,
        cx: &Cx,
        interior_dirty_pages: usize,
    ) {
        assert!(interior_dirty_pages > 0);
        let mut txn = pager.begin(cx, TransactionMode::Immediate).unwrap();
        let page_bytes = PageSize::DEFAULT.as_usize();
        for page_idx in 0..interior_dirty_pages {
            let page_no = txn.allocate_page(cx).unwrap();
            let fill = u8::try_from((page_idx % 251) + 1).unwrap();
            txn.write_page(cx, page_no, &vec![fill; page_bytes])
                .unwrap();
        }
        txn.commit(cx).unwrap();
    }

    fn track_c_metadata_apply_workload(
        txn: &mut SimpleTransaction<MemoryVfs>,
        cx: &Cx,
        interior_dirty_pages: usize,
        mode: TrackCMetadataMode,
    ) {
        let page_bytes = PageSize::DEFAULT.as_usize();
        for page_idx in 0..interior_dirty_pages {
            let page_no = PageNumber::new(u32::try_from(page_idx + 2).unwrap()).unwrap();
            let fill = u8::try_from(((page_idx + 17) % 251) + 1).unwrap();
            txn.write_page(cx, page_no, &vec![fill; page_bytes])
                .unwrap();
        }
        if matches!(mode, TrackCMetadataMode::ForcedPageOneBaseline) {
            let page_one = txn.get_page(cx, PageNumber::ONE).unwrap().into_vec();
            txn.write_page(cx, PageNumber::ONE, &page_one).unwrap();
        }
    }

    fn track_c_metadata_prepared_commit(
        mode: TrackCMetadataMode,
        interior_dirty_pages: usize,
    ) -> (Cx, SimpleTransaction<MemoryVfs>) {
        assert!(interior_dirty_pages > 0);

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let db_path = PathBuf::from(format!(
            "/track_c_metadata_cleanup_{}_{}.db",
            mode.as_str(),
            interior_dirty_pages
        ));
        let wal_path = PathBuf::from(format!(
            "/track_c_metadata_cleanup_{}_{}.db-wal",
            mode.as_str(),
            interior_dirty_pages
        ));
        let pager = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        track_c_metadata_seed_existing_pages(&pager, &cx, interior_dirty_pages);
        let backend =
            TrackCBenchmarkWalBackend::new(&vfs, &cx, &wal_path, TrackCBatchMode::Batched);
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        track_c_metadata_apply_workload(&mut txn, &cx, interior_dirty_pages, mode);
        (cx, txn)
    }

    fn track_c_metadata_measure_commit_ns(
        mode: TrackCMetadataMode,
        interior_dirty_pages: usize,
    ) -> Vec<u64> {
        let total_iters =
            TRACK_C_METADATA_BENCH_WARMUP_ITERS + TRACK_C_METADATA_BENCH_MEASURE_ITERS;
        let mut samples = Vec::with_capacity(TRACK_C_METADATA_BENCH_MEASURE_ITERS);

        for iter_idx in 0..total_iters {
            let (cx, mut txn) = track_c_metadata_prepared_commit(mode, interior_dirty_pages);
            let started = Instant::now();
            txn.commit(&cx).unwrap();
            if iter_idx >= TRACK_C_METADATA_BENCH_WARMUP_ITERS {
                let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                samples.push(elapsed_ns);
            }
        }

        samples
    }

    fn track_c_metadata_capture_frame_pages(
        mode: TrackCMetadataMode,
        interior_dirty_pages: usize,
    ) -> Vec<u32> {
        assert!(interior_dirty_pages > 0);

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let db_path = PathBuf::from(format!(
            "/track_c_metadata_capture_{}_{}.db",
            mode.as_str(),
            interior_dirty_pages
        ));
        let pager = SimplePager::open(vfs, &db_path, PageSize::DEFAULT).unwrap();
        track_c_metadata_seed_existing_pages(&pager, &cx, interior_dirty_pages);

        let (backend, frames, _begin_calls, _batch_calls) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();
        frames.lock().unwrap().clear();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        track_c_metadata_apply_workload(&mut txn, &cx, interior_dirty_pages, mode);
        txn.commit(&cx).unwrap();

        frames
            .lock()
            .unwrap()
            .iter()
            .map(|(page_number, _, _)| *page_number)
            .collect()
    }

    fn track_c_seed_existing_pages_blocking(
        pager: &SimplePager<BlockingObservedLockVfs>,
        cx: &Cx,
        page_count: usize,
    ) {
        assert!(page_count > 0);
        let mut txn = pager.begin(cx, TransactionMode::Immediate).unwrap();
        let page_bytes = PageSize::DEFAULT.as_usize();
        for page_idx in 0..page_count {
            let page_no = txn.allocate_page(cx).unwrap();
            let fill = u8::try_from((page_idx % 251) + 1).unwrap();
            txn.write_page(cx, page_no, &vec![fill; page_bytes])
                .unwrap();
        }
        txn.commit(cx).unwrap();
    }

    fn track_c_write_existing_page_range(
        txn: &mut SimpleTransaction<BlockingObservedLockVfs>,
        cx: &Cx,
        start_page_no: u32,
        page_count: usize,
        fill_offset: usize,
    ) {
        let page_bytes = PageSize::DEFAULT.as_usize();
        for page_idx in 0..page_count {
            let page_no =
                PageNumber::new(start_page_no + u32::try_from(page_idx).unwrap()).unwrap();
            let fill = u8::try_from(((page_idx + fill_offset) % 251) + 1).unwrap();
            txn.write_page(cx, page_no, &vec![fill; page_bytes])
                .unwrap();
        }
    }

    fn track_c_publish_window_prepared_commit(
        mode: TrackCPublishWindowMode,
        dirty_pages: usize,
    ) -> (
        Cx,
        SimpleTransaction<BlockingObservedLockVfs>,
        BlockingObservedLockVfs,
    ) {
        assert!(dirty_pages > 0);

        let cx = Cx::new();
        let vfs = BlockingObservedLockVfs::new();
        let db_path = PathBuf::from(format!(
            "/track_c_publish_window_hold_{}_{}.db",
            mode.as_str(),
            dirty_pages
        ));
        let wal_path = PathBuf::from(format!(
            "/track_c_publish_window_hold_{}_{}.db-wal",
            mode.as_str(),
            dirty_pages
        ));

        let seed_pager = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        track_c_seed_existing_pages_blocking(&seed_pager, &cx, dirty_pages);
        drop(seed_pager);

        let pager = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        let backend = TrackCPublishWindowBenchWalBackend::new(&vfs, &cx, &wal_path, mode);
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();
        vfs.clear_exclusive_metrics();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        track_c_write_existing_page_range(&mut txn, &cx, 2, dirty_pages, 37);
        (cx, txn, vfs)
    }

    fn track_c_measure_publish_window_hold_ns(
        mode: TrackCPublishWindowMode,
        dirty_pages: usize,
    ) -> Vec<u64> {
        let total_iters =
            TRACK_C_PUBLISH_WINDOW_BENCH_WARMUP_ITERS + TRACK_C_PUBLISH_WINDOW_BENCH_MEASURE_ITERS;
        let mut samples = Vec::with_capacity(TRACK_C_PUBLISH_WINDOW_BENCH_MEASURE_ITERS);

        for iter_idx in 0..total_iters {
            let (cx, mut txn, vfs) = track_c_publish_window_prepared_commit(mode, dirty_pages);
            txn.commit(&cx).unwrap();
            let hold_ns = *vfs.exclusive_hold_samples_ns().last().unwrap_or(&0);
            if iter_idx >= TRACK_C_PUBLISH_WINDOW_BENCH_WARMUP_ITERS {
                samples.push(hold_ns);
            }
        }

        samples
    }

    fn track_c_open_contending_pagers(
        mode: TrackCPublishWindowMode,
        dirty_pages: usize,
    ) -> (
        BlockingObservedLockVfs,
        SimplePager<BlockingObservedLockVfs>,
        SimplePager<BlockingObservedLockVfs>,
    ) {
        assert!(dirty_pages > 0);

        let vfs = BlockingObservedLockVfs::new();
        let db_path = PathBuf::from(format!(
            "/track_c_publish_window_contention_{}_{}.db",
            mode.as_str(),
            dirty_pages
        ));
        let wal_path = PathBuf::from(format!(
            "/track_c_publish_window_contention_{}_{}.db-wal",
            mode.as_str(),
            dirty_pages
        ));
        let seed_cx = Cx::new();
        let seed_pager = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        track_c_seed_existing_pages_blocking(&seed_pager, &seed_cx, dirty_pages.saturating_mul(2));
        drop(seed_pager);

        let pager_a = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        let pager_b = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).unwrap();
        let cx_a = Cx::new();
        let cx_b = Cx::new();
        pager_a
            .set_wal_backend(Box::new(TrackCPublishWindowBenchWalBackend::new(
                &vfs, &cx_a, &wal_path, mode,
            )))
            .unwrap();
        pager_b
            .set_wal_backend(Box::new(TrackCPublishWindowBenchWalBackend::new(
                &vfs, &cx_b, &wal_path, mode,
            )))
            .unwrap();
        pager_a.set_journal_mode(&cx_a, JournalMode::Wal).unwrap();
        pager_b.set_journal_mode(&cx_b, JournalMode::Wal).unwrap();
        vfs.clear_exclusive_metrics();
        (vfs, pager_a, pager_b)
    }

    fn track_c_measure_competing_writer_stall_ns(
        mode: TrackCPublishWindowMode,
        dirty_pages: usize,
    ) -> Vec<u64> {
        let total_iters =
            TRACK_C_PUBLISH_WINDOW_BENCH_WARMUP_ITERS + TRACK_C_PUBLISH_WINDOW_BENCH_MEASURE_ITERS;
        let mut samples = Vec::with_capacity(TRACK_C_PUBLISH_WINDOW_BENCH_MEASURE_ITERS);

        for iter_idx in 0..total_iters {
            let (vfs, pager_a, pager_b) = track_c_open_contending_pagers(mode, dirty_pages);
            let writer_a = std::thread::spawn(move || {
                let cx = Cx::new();
                let mut txn = pager_a.begin(&cx, TransactionMode::Immediate).unwrap();
                track_c_write_existing_page_range(&mut txn, &cx, 2, dirty_pages, 53);
                txn.commit(&cx).unwrap();
            });

            vfs.wait_for_exclusive_acquisitions(1);

            let cx_b = Cx::new();
            let mut txn_b = pager_b.begin(&cx_b, TransactionMode::Immediate).unwrap();
            let contender_start_page = u32::try_from(dirty_pages).unwrap() + 2;
            track_c_write_existing_page_range(
                &mut txn_b,
                &cx_b,
                contender_start_page,
                dirty_pages,
                97,
            );
            txn_b.commit(&cx_b).unwrap();
            writer_a.join().unwrap();

            let stall_ns = vfs
                .exclusive_wait_samples_ns()
                .into_iter()
                .max()
                .unwrap_or(0);
            if iter_idx >= TRACK_C_PUBLISH_WINDOW_BENCH_WARMUP_ITERS {
                samples.push(stall_ns);
            }
        }

        samples
    }

    fn track_c_percentile_ns(sorted_samples: &[u64], percentile: usize) -> u64 {
        let last_idx = sorted_samples.len().saturating_sub(1);
        let rank = last_idx.saturating_mul(percentile).div_ceil(100);
        sorted_samples[rank]
    }

    fn track_c_sample_summary(samples: &[u64]) -> serde_json::Value {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let total_ns: u128 = sorted.iter().map(|value| u128::from(*value)).sum();
        let mean_ns = (total_ns as f64) / (sorted.len() as f64);

        json!({
            "sample_count": sorted.len(),
            "min_ns": sorted.first().copied().unwrap_or(0),
            "median_ns": track_c_percentile_ns(&sorted, 50),
            "p95_ns": track_c_percentile_ns(&sorted, 95),
            "max_ns": sorted.last().copied().unwrap_or(0),
            "mean_ns": mean_ns,
            "raw_samples_ns": sorted,
        })
    }

    impl crate::traits::WalBackend for MockWalBackend {
        fn begin_transaction(&mut self, _cx: &Cx) -> fsqlite_error::Result<()> {
            let mut begin_calls = self.begin_calls.lock().unwrap();
            *begin_calls += 1;
            Ok(())
        }

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

        fn append_frames(
            &mut self,
            _cx: &Cx,
            frames: &[crate::traits::WalFrameRef<'_>],
        ) -> fsqlite_error::Result<()> {
            let mut batch_calls = self.batch_calls.lock().unwrap();
            *batch_calls += 1;
            drop(batch_calls);

            let mut written = self.frames.lock().unwrap();
            for frame in frames {
                written.push((
                    frame.page_number,
                    frame.page_data.to_vec(),
                    frame.db_size_if_commit,
                ));
            }
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

        fn committed_txns_since_page(
            &mut self,
            _cx: &Cx,
            page_number: u32,
        ) -> fsqlite_error::Result<u64> {
            let frames = self.frames.lock().unwrap();
            let last_page_frame = frames.iter().rposition(|(pn, _, _)| *pn == page_number);
            let Some(last_page_frame) = last_page_frame else {
                return Ok(frames
                    .iter()
                    .filter(|(_, _, db_size_if_commit)| *db_size_if_commit > 0)
                    .count() as u64);
            };

            let mut page_commit_seen = false;
            let mut committed_txns_after_page = 0_u64;
            for (frame_index, (_, _, db_size_if_commit)) in frames.iter().enumerate() {
                if *db_size_if_commit == 0 {
                    continue;
                }
                if !page_commit_seen && frame_index >= last_page_frame {
                    page_commit_seen = true;
                    continue;
                }
                if page_commit_seen {
                    committed_txns_after_page = committed_txns_after_page.saturating_add(1);
                }
            }
            Ok(committed_txns_after_page)
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

    impl crate::traits::WalBackend for PreparedBatchObservedWalBackend {
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

        fn append_frames(
            &mut self,
            _cx: &Cx,
            frames: &[crate::traits::WalFrameRef<'_>],
        ) -> fsqlite_error::Result<()> {
            *self.append_frames_calls.lock().unwrap() += 1;

            let mut written = self.frames.lock().unwrap();
            for frame in frames {
                written.push((
                    frame.page_number,
                    frame.page_data.to_vec(),
                    frame.db_size_if_commit,
                ));
            }
            Ok(())
        }

        fn prepare_append_frames(
            &mut self,
            frames: &[crate::traits::WalFrameRef<'_>],
        ) -> fsqlite_error::Result<Option<crate::traits::PreparedWalFrameBatch>> {
            self.prepare_lock_levels
                .lock()
                .unwrap()
                .push(*self.observed_lock_level.lock().unwrap());

            if frames.is_empty() {
                return Ok(None);
            }

            let frame_size =
                fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE + frames[0].page_data.len();
            let mut frame_bytes = Vec::with_capacity(frame_size * frames.len());
            let mut frame_metas = Vec::with_capacity(frames.len());
            let mut checksum_transforms = Vec::with_capacity(frames.len());
            for frame in frames {
                frame_metas.push(crate::traits::PreparedWalFrameMeta {
                    page_number: frame.page_number,
                    db_size_if_commit: frame.db_size_if_commit,
                });
                checksum_transforms.push(crate::traits::PreparedWalChecksumTransform {
                    a11: 0,
                    a12: 0,
                    a21: 0,
                    a22: 0,
                    c1: 0,
                    c2: 0,
                });
                frame_bytes
                    .extend_from_slice(&[0_u8; fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE]);
                frame_bytes.extend_from_slice(frame.page_data);
            }

            Ok(Some(crate::traits::PreparedWalFrameBatch {
                frame_size,
                page_data_offset: fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE,
                frame_metas,
                checksum_transforms,
                frame_bytes,
            }))
        }

        fn append_prepared_frames(
            &mut self,
            _cx: &Cx,
            prepared: &mut crate::traits::PreparedWalFrameBatch,
        ) -> fsqlite_error::Result<()> {
            self.append_lock_levels
                .lock()
                .unwrap()
                .push(*self.observed_lock_level.lock().unwrap());
            *self.append_prepared_calls.lock().unwrap() += 1;

            let mut written = self.frames.lock().unwrap();
            for frame in prepared.frame_refs() {
                written.push((
                    frame.page_number,
                    frame.page_data.to_vec(),
                    frame.db_size_if_commit,
                ));
            }
            Ok(())
        }

        fn read_page(
            &mut self,
            _cx: &Cx,
            page_number: u32,
        ) -> fsqlite_error::Result<Option<Vec<u8>>> {
            let frames = self.frames.lock().unwrap();
            Ok(frames
                .iter()
                .rev()
                .find(|(pn, _, _)| *pn == page_number)
                .map(|(_, data, _)| data.clone()))
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
                fsqlite_error::FrankenError::internal("observed wal frame count exceeds u32")
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
        let (pager, frames, _, _) = wal_pager_with_tracking();
        (pager, frames)
    }

    fn wal_pager_with_tracking() -> (
        SimplePager<MemoryVfs>,
        SharedFrames,
        SharedCounter,
        SharedCounter,
    ) {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/wal_test.db");
        let pager = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();
        let (backend, frames, begin_calls, batch_calls) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();
        (pager, frames, begin_calls, batch_calls)
    }

    fn wal_pager_pair_with_shared_backend()
    -> (SimplePager<MemoryVfs>, SimplePager<MemoryVfs>, SharedFrames) {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/wal_shared_refresh.db");
        let pager1 = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let pager2 = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        let cx = Cx::new();

        let frames: SharedFrames = StdArc::new(StdMutex::new(Vec::new()));
        let (backend1, _, _) = MockWalBackend::with_shared_frames(StdArc::clone(&frames));
        let (backend2, _, _) = MockWalBackend::with_shared_frames(StdArc::clone(&frames));
        pager1.set_wal_backend(Box::new(backend1)).unwrap();
        pager2.set_wal_backend(Box::new(backend2)).unwrap();
        pager1.set_journal_mode(&cx, JournalMode::Wal).unwrap();
        pager2.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        (pager1, pager2, frames)
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
        let (backend, _frames, _, _) = MockWalBackend::new();
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
    fn test_set_journal_mode_updates_header_version_bytes() {
        let (pager, _) = test_pager();
        let cx = Cx::new();

        // Verify default state: bytes 18-19 should be 1 (rollback journal).
        {
            let mut inner = pager.inner.lock().unwrap();
            let mut page1 = vec![0u8; inner.page_size.as_usize()];
            let n = inner.db_file.read(&cx, &mut page1, 0).unwrap();
            assert!(n >= DATABASE_HEADER_SIZE);
            assert_eq!(page1[18], 1, "bead_id={BEAD_ID} case=default_write_version");
            assert_eq!(page1[19], 1, "bead_id={BEAD_ID} case=default_read_version");
        }

        // Switch to WAL mode: bytes 18-19 should become 2.
        let (backend, _frames, _, _) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        {
            let mut inner = pager.inner.lock().unwrap();
            let mut page1 = vec![0u8; inner.page_size.as_usize()];
            let n = inner.db_file.read(&cx, &mut page1, 0).unwrap();
            assert!(n >= DATABASE_HEADER_SIZE);
            assert_eq!(page1[18], 2, "bead_id={BEAD_ID} case=wal_write_version");
            assert_eq!(page1[19], 2, "bead_id={BEAD_ID} case=wal_read_version");
        }

        // Switch back to DELETE mode: bytes 18-19 should revert to 1.
        pager.set_journal_mode(&cx, JournalMode::Delete).unwrap();

        {
            let mut inner = pager.inner.lock().unwrap();
            let mut page1 = vec![0u8; inner.page_size.as_usize()];
            let n = inner.db_file.read(&cx, &mut page1, 0).unwrap();
            assert!(n >= DATABASE_HEADER_SIZE);
            assert_eq!(page1[18], 1, "bead_id={BEAD_ID} case=delete_write_version");
            assert_eq!(page1[19], 1, "bead_id={BEAD_ID} case=delete_read_version");
        }
    }

    #[test]
    fn test_set_journal_mode_blocked_during_write() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let _writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let (backend, _frames, _, _) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        let result = pager.set_journal_mode(&cx, JournalMode::Wal);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=mode_switch_blocked_during_write"
        );
    }

    #[test]
    fn test_set_journal_mode_same_mode_succeeds_during_reader() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let _reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();

        let mode = pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        assert_eq!(mode, JournalMode::Wal);
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
    fn test_checkpoint_writer_sync_repairs_page1_header_after_late_growth() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        // Snapshot current page 1 as a realistic checkpoint payload.
        let page1_data = {
            let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
            txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec()
        };

        let mut writer = pager.checkpoint_writer();

        // Simulate checkpoint replay order where page 1 arrives first, then a
        // higher page extends the DB. Without final header repair this can
        // leave header page_count stale.
        crate::traits::CheckpointPageWriter::write_page(
            &mut writer,
            &cx,
            PageNumber::ONE,
            &page1_data,
        )
        .unwrap();
        let page_three = PageNumber::new(3).unwrap();
        crate::traits::CheckpointPageWriter::write_page(
            &mut writer,
            &cx,
            page_three,
            &vec![0xAB; ps],
        )
        .unwrap();
        crate::traits::CheckpointPageWriter::sync(&mut writer, &cx).unwrap();

        let txn = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let raw_page1 = txn.get_page(&cx, PageNumber::ONE).unwrap().into_vec();
        let header: [u8; DATABASE_HEADER_SIZE] = raw_page1[..DATABASE_HEADER_SIZE]
            .try_into()
            .expect("page 1 header must be present");
        let parsed = DatabaseHeader::from_bytes(&header).expect("header must parse");
        assert_eq!(
            parsed.page_count,
            page_three.get(),
            "bead_id={BEAD_ID} case=checkpoint_sync_repairs_page_count"
        );
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
            2,
            "bead_id={BEAD_ID} case=wal_two_frames_appended_including_header"
        );
        let p1_frame = locked_frames.iter().find(|f| f.0 == p1.get()).unwrap();
        assert_eq!(p1_frame.1[0], 0xAA, "bead_id={BEAD_ID} case=wal_frame_data");
        // Commit frame should have db_size > 0.
        let commit_count = locked_frames.iter().filter(|f| f.2 > 0).count();
        assert_eq!(commit_count, 1, "bead_id={BEAD_ID} case=wal_commit_marker");
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
            3,
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
    fn test_wal_commit_uses_single_batch_append() {
        let (pager, frames, _begin_calls, batch_calls) = wal_pager_with_tracking();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let new_page = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, PageNumber::ONE, &vec![0x11; ps])
            .unwrap();
        txn.write_page(&cx, new_page, &vec![0x22; ps]).unwrap();
        txn.commit(&cx).unwrap();

        assert_eq!(
            *batch_calls.lock().unwrap(),
            1,
            "bead_id={BEAD_ID} case=wal_batch_append_single_call"
        );

        let frames = frames.lock().unwrap();
        assert_eq!(
            frames.len(),
            2,
            "bead_id={BEAD_ID} case=wal_batch_append_frame_count"
        );
        assert_eq!(
            frames[0].0,
            PageNumber::ONE.get(),
            "bead_id={BEAD_ID} case=wal_batch_append_sorted_page1"
        );
        assert_eq!(
            frames[1].0,
            new_page.get(),
            "bead_id={BEAD_ID} case=wal_batch_append_sorted_new_page"
        );
        assert_eq!(
            frames[0].2, 0,
            "bead_id={BEAD_ID} case=wal_batch_append_non_commit_first"
        );
        assert_eq!(
            frames[1].2,
            new_page.get(),
            "bead_id={BEAD_ID} case=wal_batch_append_commit_marker_last"
        );
    }

    #[test]
    fn test_wal_preparation_happens_before_exclusive_publish_lock() {
        let (pager, observed_lock_level) = observed_lock_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let (
            backend,
            frames,
            append_frames_calls,
            append_prepared_calls,
            prepare_lock_levels,
            append_lock_levels,
        ) = PreparedBatchObservedWalBackend::new(Arc::clone(&observed_lock_level));
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let new_page = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, PageNumber::ONE, &vec![0x11; ps])
            .unwrap();
        txn.write_page(&cx, new_page, &vec![0x22; ps]).unwrap();
        txn.commit(&cx).unwrap();

        assert_eq!(
            *append_frames_calls.lock().unwrap(),
            0,
            "bead_id=bd-db300.3.2 case=prepared_path_skips_fallback_append"
        );
        assert_eq!(
            *append_prepared_calls.lock().unwrap(),
            1,
            "bead_id=bd-db300.3.2 case=prepared_path_uses_prepared_append"
        );
        assert_eq!(
            prepare_lock_levels.lock().unwrap().as_slice(),
            &[LockLevel::Reserved],
            "bead_id=bd-db300.3.2 case=prepare_runs_before_exclusive_publish"
        );
        assert_eq!(
            append_lock_levels.lock().unwrap().as_slice(),
            &[LockLevel::Exclusive],
            "bead_id=bd-db300.3.2 case=prepared_append_runs_inside_exclusive_publish"
        );

        let frames = frames.lock().unwrap();
        assert_eq!(
            frames.len(),
            2,
            "bead_id=bd-db300.3.2 case=prepared_path_preserves_frame_count"
        );
        assert_eq!(
            frames[0].0,
            PageNumber::ONE.get(),
            "bead_id=bd-db300.3.2 case=prepared_path_preserves_sorted_page_one"
        );
        assert_eq!(
            frames[1].0,
            new_page.get(),
            "bead_id=bd-db300.3.2 case=prepared_path_preserves_sorted_commit_frame"
        );
    }

    #[test]
    fn test_publish_window_measurement_captures_exclusive_hold_sample() {
        let (cx, mut txn, vfs) =
            track_c_publish_window_prepared_commit(TrackCPublishWindowMode::PreparedCandidate, 7);
        txn.commit(&cx).unwrap();

        let hold_samples = vfs.exclusive_hold_samples_ns();
        assert_eq!(
            hold_samples.len(),
            1,
            "bead_id={TRACK_C_PUBLISH_WINDOW_BENCH_BEAD_ID} case=hold_sample_count"
        );
        assert!(
            hold_samples[0] > 0,
            "bead_id={TRACK_C_PUBLISH_WINDOW_BENCH_BEAD_ID} case=hold_sample_positive"
        );
    }

    #[test]
    fn test_publish_window_contention_measurement_captures_competing_writer_wait() {
        let (vfs, pager_a, pager_b) =
            track_c_open_contending_pagers(TrackCPublishWindowMode::PreparedCandidate, 7);
        let writer_a = std::thread::spawn(move || {
            let cx = Cx::new();
            let mut txn = pager_a.begin(&cx, TransactionMode::Immediate).unwrap();
            track_c_write_existing_page_range(&mut txn, &cx, 2, 7, 53);
            txn.commit(&cx).unwrap();
        });

        vfs.wait_for_exclusive_acquisitions(1);

        let cx_b = Cx::new();
        let mut txn_b = pager_b.begin(&cx_b, TransactionMode::Immediate).unwrap();
        track_c_write_existing_page_range(&mut txn_b, &cx_b, 9, 7, 97);
        txn_b.commit(&cx_b).unwrap();
        writer_a.join().unwrap();

        let wait_samples = vfs.exclusive_wait_samples_ns();
        assert_eq!(
            wait_samples.len(),
            2,
            "bead_id={TRACK_C_PUBLISH_WINDOW_BENCH_BEAD_ID} case=wait_sample_count"
        );
        assert!(
            wait_samples.iter().copied().max().unwrap_or(0) > 0,
            "bead_id={TRACK_C_PUBLISH_WINDOW_BENCH_BEAD_ID} case=contending_writer_wait_positive"
        );
    }

    #[test]
    fn test_collect_wal_commit_batch_keeps_sorted_order_and_single_commit_boundary() {
        let page_three = PageNumber::new(3).unwrap();
        let mut write_set = HashMap::new();

        let mut page1 = PageBuf::new(PageSize::DEFAULT);
        page1.fill(0x11);
        write_set.insert(PageNumber::ONE, page1);

        let mut page3 = PageBuf::new(PageSize::DEFAULT);
        page3.fill(0x33);
        write_set.insert(page_three, page3);

        let write_pages_sorted = vec![PageNumber::ONE, page_three];
        let batch = collect_wal_commit_batch(2, &write_set, &write_pages_sorted)
            .unwrap()
            .expect("non-empty write set should yield a WAL batch");

        assert_eq!(
            batch.new_db_size,
            page_three.get(),
            "bead_id={BEAD_ID} case=wal_batch_helper_new_db_size"
        );
        assert_eq!(
            batch.frames.len(),
            2,
            "bead_id={BEAD_ID} case=wal_batch_helper_frame_count"
        );
        assert_eq!(
            batch.frames[0].page_number,
            PageNumber::ONE.get(),
            "bead_id={BEAD_ID} case=wal_batch_helper_sorted_first"
        );
        assert_eq!(
            batch.frames[0].db_size_if_commit, 0,
            "bead_id={BEAD_ID} case=wal_batch_helper_non_commit_prefix"
        );
        assert_eq!(
            batch.frames[1].page_number,
            page_three.get(),
            "bead_id={BEAD_ID} case=wal_batch_helper_sorted_last"
        );
        assert_eq!(
            batch.frames[1].db_size_if_commit,
            page_three.get(),
            "bead_id={BEAD_ID} case=wal_batch_helper_commit_boundary_last"
        );
    }

    #[test]
    fn test_collect_wal_commit_batch_preserves_existing_db_size_for_interior_updates() {
        let page_two = PageNumber::new(2).unwrap();
        let mut write_set = HashMap::new();

        let mut page2 = PageBuf::new(PageSize::DEFAULT);
        page2.fill(0x22);
        write_set.insert(page_two, page2);

        let batch = collect_wal_commit_batch(9, &write_set, &[page_two])
            .unwrap()
            .expect("single dirty page should yield a WAL batch");

        assert_eq!(
            batch.new_db_size, 9,
            "bead_id={BEAD_ID} case=wal_batch_helper_preserve_db_size"
        );
        assert_eq!(
            batch.frames[0].db_size_if_commit, 9,
            "bead_id={BEAD_ID} case=wal_batch_helper_commit_marker_uses_existing_db_size"
        );
    }

    #[test]
    fn test_collect_wal_commit_batch_returns_none_for_empty_write_set() {
        let write_set = HashMap::new();
        let batch = collect_wal_commit_batch(4, &write_set, &[]).unwrap();
        assert!(
            batch.is_none(),
            "bead_id={BEAD_ID} case=wal_batch_helper_empty_batch"
        );
    }

    #[test]
    fn test_collect_wal_commit_batch_errors_when_sorted_page_missing_from_write_set() {
        let missing_page = PageNumber::new(4).unwrap();
        let write_set = HashMap::new();
        let err = match collect_wal_commit_batch(1, &write_set, &[missing_page]) {
            Ok(_) => panic!(
                "bead_id={BEAD_ID} case=wal_batch_helper_missing_page expected helper to fail"
            ),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("missing page 4"),
            "bead_id={BEAD_ID} case=wal_batch_helper_missing_page error={message}"
        );
    }

    #[test]
    #[ignore = "benchmark evidence only"]
    fn wal_publish_window_shrink_benchmark_report() {
        let cases: Vec<_> = TRACK_C_PUBLISH_WINDOW_BENCH_CASES
            .iter()
            .map(|(scenario_id, dirty_pages)| {
                let baseline_hold_samples = track_c_measure_publish_window_hold_ns(
                    TrackCPublishWindowMode::InlinePrepareBaseline,
                    *dirty_pages,
                );
                let candidate_hold_samples = track_c_measure_publish_window_hold_ns(
                    TrackCPublishWindowMode::PreparedCandidate,
                    *dirty_pages,
                );
                let baseline_stall_samples = track_c_measure_competing_writer_stall_ns(
                    TrackCPublishWindowMode::InlinePrepareBaseline,
                    *dirty_pages,
                );
                let candidate_stall_samples = track_c_measure_competing_writer_stall_ns(
                    TrackCPublishWindowMode::PreparedCandidate,
                    *dirty_pages,
                );

                let baseline_hold_summary = track_c_sample_summary(&baseline_hold_samples);
                let candidate_hold_summary = track_c_sample_summary(&candidate_hold_samples);
                let baseline_stall_summary = track_c_sample_summary(&baseline_stall_samples);
                let candidate_stall_summary = track_c_sample_summary(&candidate_stall_samples);

                let baseline_hold_median =
                    baseline_hold_summary["median_ns"].as_u64().unwrap_or(0);
                let candidate_hold_median =
                    candidate_hold_summary["median_ns"].as_u64().unwrap_or(0);
                let baseline_stall_median =
                    baseline_stall_summary["median_ns"].as_u64().unwrap_or(0);
                let candidate_stall_median =
                    candidate_stall_summary["median_ns"].as_u64().unwrap_or(0);

                json!({
                    "scenario_id": scenario_id,
                    "dirty_pages": dirty_pages,
                    "exclusive_window_hold_baseline": baseline_hold_summary,
                    "exclusive_window_hold_candidate": candidate_hold_summary,
                    "contending_writer_stall_baseline": baseline_stall_summary,
                    "contending_writer_stall_candidate": candidate_stall_summary,
                    "hold_reduction_ratio_median": if baseline_hold_median == 0 {
                        0.0
                    } else {
                        1.0 - (candidate_hold_median as f64 / baseline_hold_median as f64)
                    },
                    "stall_reduction_ratio_median": if baseline_stall_median == 0 {
                        0.0
                    } else {
                        1.0 - (candidate_stall_median as f64 / baseline_stall_median as f64)
                    },
                    "faster_variant_by_hold_median": if candidate_hold_median <= baseline_hold_median {
                        "prepared_candidate"
                    } else {
                        "inline_prepare_baseline"
                    },
                    "faster_variant_by_stall_median": if candidate_stall_median <= baseline_stall_median {
                        "prepared_candidate"
                    } else {
                        "inline_prepare_baseline"
                    },
                })
            })
            .collect();

        let report = json!({
            "schema_version": "fsqlite.track_c.publish_window_benchmark.v1",
            "bead_id": TRACK_C_PUBLISH_WINDOW_BENCH_BEAD_ID,
            "parent_bead_id": "bd-db300.3.2",
            "measured_operation": "pager_commit_wal_publish_window",
            "warmup_iterations": TRACK_C_PUBLISH_WINDOW_BENCH_WARMUP_ITERS,
            "measurement_iterations": TRACK_C_PUBLISH_WINDOW_BENCH_MEASURE_ITERS,
            "vfs": "blocking_memory_vfs",
            "baseline_variant": "inline_prepare_under_exclusive_lock",
            "candidate_variant": "prepared_batch_before_exclusive_lock",
            "cases": cases,
        });

        println!("BEGIN_BD_DB300_3_2_3_REPORT");
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        println!("END_BD_DB300_3_2_3_REPORT");
    }

    #[test]
    #[ignore = "benchmark evidence only"]
    fn wal_commit_batch_benchmark_report() {
        let cases: Vec<_> = TRACK_C_BATCH_BENCH_CASES
            .iter()
            .map(|(scenario_id, dirty_pages)| {
                let single_samples =
                    track_c_measure_commit_ns(TrackCBatchMode::SingleFrame, *dirty_pages);
                let batch_samples =
                    track_c_measure_commit_ns(TrackCBatchMode::Batched, *dirty_pages);

                let single_summary = track_c_sample_summary(&single_samples);
                let batch_summary = track_c_sample_summary(&batch_samples);
                let single_median = single_summary["median_ns"].as_u64().unwrap_or(0);
                let batch_median = batch_summary["median_ns"].as_u64().unwrap_or(0);
                let single_mean = single_summary["mean_ns"].as_f64().unwrap_or(0.0);
                let batch_mean = batch_summary["mean_ns"].as_f64().unwrap_or(0.0);

                json!({
                    "scenario_id": scenario_id,
                    "dirty_pages": dirty_pages,
                    "single_frame": single_summary,
                    "batch_append": batch_summary,
                    "speedup_vs_single_median": if batch_median == 0 {
                        0.0
                    } else {
                        (single_median as f64) / (batch_median as f64)
                    },
                    "speedup_vs_single_mean": if batch_mean == 0.0 {
                        0.0
                    } else {
                        single_mean / batch_mean
                    },
                    "faster_variant_by_median": if batch_median <= single_median {
                        "batch_append"
                    } else {
                        "single_frame"
                    },
                })
            })
            .collect();

        let report = json!({
            "schema_version": "fsqlite.track_c.batch_commit_benchmark.v1",
            "bead_id": TRACK_C_BATCH_BENCH_BEAD_ID,
            "parent_bead_id": "bd-db300.3.1",
            "measured_operation": "pager_commit_wal_path",
            "warmup_iterations": TRACK_C_BATCH_BENCH_WARMUP_ITERS,
            "measurement_iterations": TRACK_C_BATCH_BENCH_MEASURE_ITERS,
            "vfs": "memory",
            "sync_mode": "normal_noop_memory_vfs",
            "baseline_variant": "single_frame_append_loop",
            "candidate_variant": "transaction_wide_batch_append",
            "cases": cases,
        });

        println!("BEGIN_BD_DB300_3_1_4_REPORT");
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        println!("END_BD_DB300_3_1_4_REPORT");
    }

    #[test]
    #[ignore = "inventory evidence only"]
    fn wal_publish_window_inventory_report() {
        let outside_window = vec![
            json!({
                "component": "collect_wal_commit_batch",
                "location": "crates/fsqlite-pager/src/pager.rs::commit_wal",
                "classification": "already_outside_publish_window",
                "move_candidate": "not_applicable",
                "rationale": "frame ordering, commit-marker boundary, and new_db_size derivation happen before EXCLUSIVE lock acquisition",
            }),
            json!({
                "component": "wal_adapter_frame_ref_copy",
                "location": "crates/fsqlite-core/src/wal_adapter.rs::prepare_append_frames",
                "classification": "pure_copy",
                "move_candidate": "completed",
                "rationale": "borrowed WalFrameRef values are copied into owned batch metadata before the exclusive publish window starts",
            }),
            json!({
                "component": "wal_batch_buffer_allocation",
                "location": "crates/fsqlite-wal/src/wal.rs::prepare_frame_bytes",
                "classification": "allocation",
                "move_candidate": "completed",
                "rationale": "contiguous frame buffer allocation now happens in the prepare phase before EXCLUSIVE lock acquisition",
            }),
            json!({
                "component": "wal_header_and_payload_copy",
                "location": "crates/fsqlite-wal/src/wal.rs::prepare_frame_bytes",
                "classification": "pure_copy",
                "move_candidate": "completed",
                "rationale": "page-number/db-size stamping, salt staging, and payload copies are fully serialized before publish time",
            }),
            json!({
                "component": "wal_checksum_transform_precompute",
                "location": "crates/fsqlite-core/src/wal_adapter.rs::prepare_append_frames",
                "classification": "pure_compute",
                "move_candidate": "completed",
                "rationale": "per-frame checksum transforms are now derived outside the lock so publish only rebinds the live seed",
            }),
        ];

        let inside_window = vec![
            json!({
                "component": "lock_exclusive",
                "location": "crates/fsqlite-pager/src/pager.rs::commit_wal",
                "classification": "serialized_boundary",
                "move_candidate": "no",
                "rationale": "cross-process WAL append exclusion is the start of the current publish window",
            }),
            json!({
                "component": "wal_refresh_before_append",
                "location": "crates/fsqlite-core/src/wal_adapter.rs::append_prepared_frames",
                "classification": "state_refresh_read_only",
                "move_candidate": "conditional",
                "rationale": "refresh chooses append offset/checksum seed but is still read/refresh work rather than durable publication",
            }),
            json!({
                "component": "wal_checksum_seed_rebind",
                "location": "crates/fsqlite-core/src/wal_adapter.rs::append_prepared_frames",
                "classification": "publish_seed_binding",
                "move_candidate": "no",
                "rationale": "publish still has to bind the authoritative post-refresh seed and rewrite checksum header words before the single durable write",
            }),
            json!({
                "component": "wal_file_write",
                "location": "crates/fsqlite-wal/src/wal.rs::append_prepared_frame_bytes",
                "classification": "durable_state_transition",
                "move_candidate": "no",
                "rationale": "single contiguous file write is the core serialized append that must observe the authoritative WAL end",
            }),
            json!({
                "component": "wal_state_advance_after_write",
                "location": "crates/fsqlite-wal/src/wal.rs::append_prepared_frame_bytes",
                "classification": "durable_state_transition",
                "move_candidate": "no",
                "rationale": "frame_count/running_checksum advancement must match the durable append that just occurred",
            }),
            json!({
                "component": "fec_hook_on_frame",
                "location": "crates/fsqlite-core/src/wal_adapter.rs::append_prepared_frames",
                "classification": "post_append_compute",
                "move_candidate": "yes",
                "rationale": "FEC hook work remains explicitly non-fatal and still runs after append while the publish window is open",
            }),
            json!({
                "component": "wal_sync",
                "location": "crates/fsqlite-pager/src/pager.rs::commit_wal",
                "classification": "durability_barrier",
                "move_candidate": "no",
                "rationale": "sync is the durability barrier for the commit and therefore part of the required serialized state transition",
            }),
            json!({
                "component": "inner_db_size_update",
                "location": "crates/fsqlite-pager/src/pager.rs::commit_wal",
                "classification": "pager_state_publish",
                "move_candidate": "no",
                "rationale": "pager-visible db_size must only advance after the WAL append and sync succeed",
            }),
        ];

        let definitely_movable = inside_window
            .iter()
            .filter(|entry| entry["move_candidate"] == "yes")
            .count();
        let conditionally_movable = inside_window
            .iter()
            .filter(|entry| entry["move_candidate"] == "conditional")
            .count();
        let required_serialized = inside_window
            .iter()
            .filter(|entry| entry["move_candidate"] == "no")
            .count();

        let report = json!({
            "schema_version": "fsqlite.track_c.publish_window_inventory.v1",
            "bead_id": TRACK_C_PUBLISH_WINDOW_INVENTORY_BEAD_ID,
            "parent_bead_id": "bd-db300.3.2",
            "measured_operation": "pager_commit_wal_path",
            "measurement_anchor_bead_id": TRACK_C_BATCH_BENCH_BEAD_ID,
            "measurement_anchor_report": "fsqlite.track_c.batch_commit_benchmark.v1",
            "current_window": {
                "entry_function": "crates/fsqlite-pager/src/pager.rs::commit_wal",
                "window_begins_at": "inner.db_file.lock(cx, LockLevel::Exclusive)?",
                "window_ends_after": "inner.db_size = batch.new_db_size",
            },
            "outside_window": outside_window,
            "inside_window": inside_window,
            "summary": {
                "outside_window_steps": outside_window.len(),
                "inside_window_steps": inside_window.len(),
                "definitely_movable_inside_window_steps": definitely_movable,
                "conditionally_movable_inside_window_steps": conditionally_movable,
                "required_serialized_inside_window_steps": required_serialized,
            },
        });

        println!("BEGIN_BD_DB300_3_2_1_REPORT");
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        println!("END_BD_DB300_3_2_1_REPORT");

        assert_eq!(
            report["measured_operation"], "pager_commit_wal_path",
            "bead_id={TRACK_C_PUBLISH_WINDOW_INVENTORY_BEAD_ID} case=measured_operation_anchor"
        );
        assert_eq!(
            report["summary"]["definitely_movable_inside_window_steps"], 5,
            "bead_id={TRACK_C_PUBLISH_WINDOW_INVENTORY_BEAD_ID} case=movable_step_count"
        );
        assert_eq!(
            report["summary"]["conditionally_movable_inside_window_steps"], 1,
            "bead_id={TRACK_C_PUBLISH_WINDOW_INVENTORY_BEAD_ID} case=conditional_step_count"
        );
        assert_eq!(
            report["summary"]["required_serialized_inside_window_steps"], 5,
            "bead_id={TRACK_C_PUBLISH_WINDOW_INVENTORY_BEAD_ID} case=required_step_count"
        );
    }

    #[test]
    #[ignore = "benchmark evidence only"]
    fn wal_metadata_cleanup_benchmark_report() {
        let cases: Vec<_> = TRACK_C_METADATA_BENCH_CASES
            .iter()
            .map(|(scenario_id, interior_dirty_pages)| {
                let baseline_samples = track_c_metadata_measure_commit_ns(
                    TrackCMetadataMode::ForcedPageOneBaseline,
                    *interior_dirty_pages,
                );
                let candidate_samples = track_c_metadata_measure_commit_ns(
                    TrackCMetadataMode::SemanticCleanupCandidate,
                    *interior_dirty_pages,
                );
                let baseline_summary = track_c_sample_summary(&baseline_samples);
                let candidate_summary = track_c_sample_summary(&candidate_samples);
                let baseline_median = baseline_summary["median_ns"].as_u64().unwrap_or(0);
                let candidate_median = candidate_summary["median_ns"].as_u64().unwrap_or(0);
                let baseline_mean = baseline_summary["mean_ns"].as_f64().unwrap_or(0.0);
                let candidate_mean = candidate_summary["mean_ns"].as_f64().unwrap_or(0.0);
                let baseline_frame_pages = track_c_metadata_capture_frame_pages(
                    TrackCMetadataMode::ForcedPageOneBaseline,
                    *interior_dirty_pages,
                );
                let candidate_frame_pages = track_c_metadata_capture_frame_pages(
                    TrackCMetadataMode::SemanticCleanupCandidate,
                    *interior_dirty_pages,
                );
                let baseline_page_one_frames = baseline_frame_pages
                    .iter()
                    .filter(|page_number| **page_number == PageNumber::ONE.get())
                    .count();
                let candidate_page_one_frames = candidate_frame_pages
                    .iter()
                    .filter(|page_number| **page_number == PageNumber::ONE.get())
                    .count();

                json!({
                    "scenario_id": scenario_id,
                    "interior_dirty_pages": interior_dirty_pages,
                    "forced_page_one_baseline": baseline_summary,
                    "semantic_cleanup_candidate": candidate_summary,
                    "baseline_frame_pages": baseline_frame_pages,
                    "candidate_frame_pages": candidate_frame_pages,
                    "baseline_total_frames_per_commit": baseline_frame_pages.len(),
                    "candidate_total_frames_per_commit": candidate_frame_pages.len(),
                    "baseline_page_one_frames_per_commit": baseline_page_one_frames,
                    "candidate_page_one_frames_per_commit": candidate_page_one_frames,
                    "frame_count_reduction_per_commit": baseline_frame_pages.len().saturating_sub(candidate_frame_pages.len()),
                    "page_one_exposure_reduction_per_commit": baseline_page_one_frames.saturating_sub(candidate_page_one_frames),
                    "speedup_vs_baseline_median": if candidate_median == 0 {
                        0.0
                    } else {
                        (baseline_median as f64) / (candidate_median as f64)
                    },
                    "speedup_vs_baseline_mean": if candidate_mean == 0.0 {
                        0.0
                    } else {
                        baseline_mean / candidate_mean
                    },
                    "faster_variant_by_median": if candidate_median <= baseline_median {
                        "semantic_cleanup_candidate"
                    } else {
                        "forced_page_one_baseline"
                    },
                })
            })
            .collect();

        let report = json!({
            "schema_version": "fsqlite.track_c.metadata_cleanup_benchmark.v1",
            "bead_id": TRACK_C_METADATA_BENCH_BEAD_ID,
            "parent_bead_id": "bd-db300.3.3",
            "measured_operation": "wal_commit_interior_only_workload",
            "warmup_iterations": TRACK_C_METADATA_BENCH_WARMUP_ITERS,
            "measurement_iterations": TRACK_C_METADATA_BENCH_MEASURE_ITERS,
            "vfs": "memory",
            "sync_mode": "normal_noop_memory_vfs",
            "baseline_variant": "forced_page_one_rewrite_every_commit",
            "candidate_variant": "semantic_trigger_page_one_cleanup",
            "cases": cases,
        });

        println!("BEGIN_BD_DB300_3_3_3_REPORT");
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        println!("END_BD_DB300_3_3_3_REPORT");
    }

    #[test]
    fn test_wal_commit_preserves_sorted_unique_frame_order() {
        let (pager, frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p2 = txn.allocate_page(&cx).unwrap();
        let p3 = txn.allocate_page(&cx).unwrap();
        let p4 = txn.allocate_page(&cx).unwrap();

        txn.write_page(&cx, p4, &vec![0x44; ps]).unwrap();
        txn.write_page(&cx, p2, &vec![0x22; ps]).unwrap();
        txn.write_page(&cx, p3, &vec![0x33; ps]).unwrap();
        txn.write_page(&cx, p4, &vec![0x55; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let frame_pages: Vec<u32> = frames.lock().unwrap().iter().map(|frame| frame.0).collect();
        assert_eq!(
            frame_pages,
            vec![PageNumber::ONE.get(), p2.get(), p3.get(), p4.get()],
            "bead_id={BEAD_ID} case=wal_frames_sorted_and_unique"
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
        let (backend, _frames, _, _) = MockWalBackend::new();
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
    fn test_wal_begin_not_called_for_rejected_eager_writer() {
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let (backend, _frames, begin_calls, _) = MockWalBackend::new();
        pager.set_wal_backend(Box::new(backend)).unwrap();
        pager.set_journal_mode(&cx, JournalMode::Wal).unwrap();

        let _writer = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        assert_eq!(
            *begin_calls.lock().unwrap(),
            1,
            "bead_id={BEAD_ID} case=first_writer_initializes_wal_snapshot"
        );

        let err = pager
            .begin(&cx, TransactionMode::Immediate)
            .err()
            .expect("second eager writer should be rejected");
        assert!(matches!(err, FrankenError::Busy));
        assert_eq!(
            *begin_calls.lock().unwrap(),
            1,
            "bead_id={BEAD_ID} case=rejected_writer_must_not_mutate_wal_state"
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
    fn test_wal_interior_update_skips_page1_when_metadata_unchanged() {
        let (pager, frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let page_two = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
            page_two
        };

        let frames_before = frames.lock().unwrap().len();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, page_two, &vec![0x22; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let frames = frames.lock().unwrap();
        let appended = &frames[frames_before..];
        assert_eq!(
            appended.len(),
            1,
            "bead_id={BEAD_ID} case=wal_headerless_interior_commit_frame_count"
        );
        assert_eq!(
            appended[0].0,
            page_two.get(),
            "bead_id={BEAD_ID} case=wal_headerless_interior_commit_page"
        );
        assert_eq!(
            appended[0].2,
            page_two.get(),
            "bead_id={BEAD_ID} case=wal_headerless_interior_commit_commit_marker_uses_existing_db_size"
        );
    }

    #[test]
    fn test_wal_page_one_write_plan_for_interior_update_has_no_trigger() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let page_two = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
            page_two
        };

        let current_db_size = pager.published_snapshot().db_size;
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, page_two, &vec![0x22; ps]).unwrap();

        let plan = txn.classify_wal_page_one_write(current_db_size, txn.freelist_metadata_dirty());
        assert_eq!(
            plan,
            WalPageOneWritePlan {
                max_written: page_two.get(),
                page_one_dirty: false,
                freelist_metadata_dirty: false,
                db_growth: false,
            },
            "bead_id={BEAD_ID} case=wal_page1_plan_interior_update"
        );
        assert!(
            !plan.requires_page_one_rewrite(),
            "bead_id={BEAD_ID} case=wal_page1_plan_interior_update_no_rewrite"
        );
        assert!(
            !plan.requires_page_count_advance(),
            "bead_id={BEAD_ID} case=wal_page1_plan_interior_update_no_growth"
        );
    }

    #[test]
    fn test_wal_page_one_write_plan_marks_page_one_dirty_trigger() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
        }

        let current_db_size = pager.published_snapshot().db_size;
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, PageNumber::ONE, &vec![0x77; ps])
            .unwrap();

        let plan = txn.classify_wal_page_one_write(current_db_size, txn.freelist_metadata_dirty());
        assert_eq!(
            plan,
            WalPageOneWritePlan {
                max_written: PageNumber::ONE.get(),
                page_one_dirty: true,
                freelist_metadata_dirty: false,
                db_growth: false,
            },
            "bead_id={BEAD_ID} case=wal_page1_plan_page1_dirty"
        );
        assert!(
            plan.requires_page_one_rewrite(),
            "bead_id={BEAD_ID} case=wal_page1_plan_page1_dirty_requires_rewrite"
        );
        assert!(
            !plan.requires_page_count_advance(),
            "bead_id={BEAD_ID} case=wal_page1_plan_page1_dirty_no_growth"
        );
    }

    #[test]
    fn test_wal_page_one_write_plan_marks_freelist_trigger() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let page_two = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
            page_two
        };

        let current_db_size = pager.published_snapshot().db_size;
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.free_page(&cx, page_two).unwrap();

        let plan = txn.classify_wal_page_one_write(current_db_size, txn.freelist_metadata_dirty());
        assert_eq!(
            plan,
            WalPageOneWritePlan {
                max_written: 0,
                page_one_dirty: false,
                freelist_metadata_dirty: true,
                db_growth: false,
            },
            "bead_id={BEAD_ID} case=wal_page1_plan_freelist_dirty"
        );
        assert!(
            plan.requires_page_one_rewrite(),
            "bead_id={BEAD_ID} case=wal_page1_plan_freelist_dirty_requires_rewrite"
        );
        assert!(
            !plan.requires_page_count_advance(),
            "bead_id={BEAD_ID} case=wal_page1_plan_freelist_dirty_no_growth"
        );
    }

    #[test]
    fn test_wal_page_one_write_plan_marks_database_growth_trigger() {
        let (pager, _frames) = wal_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let current_db_size = pager.published_snapshot().db_size;
        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let page_two = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();

        let plan = txn.classify_wal_page_one_write(current_db_size, txn.freelist_metadata_dirty());
        assert_eq!(
            plan,
            WalPageOneWritePlan {
                max_written: page_two.get(),
                page_one_dirty: false,
                freelist_metadata_dirty: false,
                db_growth: true,
            },
            "bead_id={BEAD_ID} case=wal_page1_plan_db_growth"
        );
        assert!(
            plan.requires_page_one_rewrite(),
            "bead_id={BEAD_ID} case=wal_page1_plan_db_growth_requires_rewrite"
        );
        assert!(
            plan.requires_page_count_advance(),
            "bead_id={BEAD_ID} case=wal_page1_plan_db_growth_advances_count"
        );
    }

    #[test]
    fn test_wal_external_refresh_tracks_headerless_interior_commit() {
        let (pager1, pager2, _frames) = wal_pager_pair_with_shared_backend();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let page_two = {
            let mut txn = pager1.begin(&cx, TransactionMode::Immediate).unwrap();
            let page_two = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, page_two, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
            page_two
        };

        let seq_before = pager1.published_snapshot().visible_commit_seq;

        let mut txn = pager1.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, page_two, &vec![0x22; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let latest_seq = pager1.published_snapshot().visible_commit_seq;
        assert!(
            latest_seq > seq_before,
            "bead_id={BEAD_ID} case=wal_headerless_interior_commit_advances_local_seq"
        );

        let reader = pager2.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let refreshed = pager2.published_snapshot();
        assert_eq!(
            refreshed.visible_commit_seq, latest_seq,
            "bead_id={BEAD_ID} case=wal_headerless_interior_commit_refreshes_visible_seq"
        );
        assert_eq!(
            reader.get_page(&cx, page_two).unwrap().as_ref()[0],
            0x22,
            "bead_id={BEAD_ID} case=wal_headerless_interior_commit_refreshes_latest_page"
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

        let btree_hdr =
            BTreePageHeader::parse(&raw, PageSize::DEFAULT, 0, true).expect("btree header");
        assert_eq!(
            btree_hdr.page_type,
            fsqlite_types::BTreePageType::LeafTable,
            "bead_id={BEAD_5A1} case=btree_page_type"
        );
        assert_eq!(
            btree_hdr.cell_count, 0,
            "bead_id={BEAD_5A1} case=btree_cell_count"
        );
        assert_eq!(
            btree_hdr.cell_content_start,
            PageSize::DEFAULT.get(),
            "bead_id={BEAD_5A1} case=btree_content_start"
        );
        assert_eq!(
            btree_hdr.first_freeblock, 0,
            "bead_id={BEAD_5A1} case=btree_first_freeblock"
        );
        assert_eq!(
            btree_hdr.fragmented_free_bytes, 0,
            "bead_id={BEAD_5A1} case=btree_fragmented_free"
        );
        assert_eq!(
            btree_hdr.header_offset, DATABASE_HEADER_SIZE,
            "bead_id={BEAD_5A1} case=btree_header_offset"
        );
        assert!(
            btree_hdr.right_most_child.is_none(),
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
        assert_eq!(
            p2,
            p,
            "bead_id={BEAD_ID} case=freelist_reuse p3={} p1={}",
            p2.get(),
            p.get()
        );
        txn3.write_page(&cx, p2, &vec![0xBB; ps]).unwrap();
        txn3.commit(&cx).unwrap();

        // Verify freelist is empty (in-flight).
        {
            let inner = pager.inner.lock().unwrap();
            assert!(inner.freelist.is_empty());
            drop(inner);
        }

        // 4. Rollback.
        let mut txn4 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn4.free_page(&cx, p2).unwrap();
        txn4.rollback(&cx).unwrap();

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
    fn test_freelist_persisted_and_reloaded_on_reopen() {
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/freelist_persist.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let pager = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let mut txn1 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn1.allocate_page(&cx).unwrap();
        txn1.write_page(&cx, p, &vec![0xAB; ps]).unwrap();
        txn1.commit(&cx).unwrap();

        let mut txn2 = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        txn2.free_page(&cx, p).unwrap();
        txn2.commit(&cx).unwrap();

        let txn_ro = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let raw = txn_ro.get_page(&cx, PageNumber::ONE).unwrap().into_vec();
        let hdr_bytes: [u8; DATABASE_HEADER_SIZE] = raw[..DATABASE_HEADER_SIZE].try_into().unwrap();
        let hdr = DatabaseHeader::from_bytes(&hdr_bytes).unwrap();
        assert_eq!(hdr.freelist_count, 1);
        assert_eq!(hdr.freelist_trunk, p.get());

        let reopened = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();
        {
            let inner = reopened.inner.lock().unwrap();
            assert_eq!(inner.freelist.len(), 1);
            assert_eq!(inner.freelist[0], p);
        }

        let mut txn3 = reopened.begin(&cx, TransactionMode::Immediate).unwrap();
        let reused = txn3.allocate_page(&cx).unwrap();
        assert_eq!(
            reused, p,
            "reopened pager should reuse persisted freelist page"
        );
        txn3.commit(&cx).unwrap();
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
        txn.write_page(&cx, p, &vec![0xAA; ps]).unwrap();
        txn.commit(&cx).unwrap();

        // Read in new transaction → should see committed data.
        let txn2 = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = txn2.get_page(&cx, p).unwrap();
        assert_eq!(
            data.as_ref()[0],
            0xAA,
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
        println!(
            "DEBUG_METRICS: hits={} misses={} admits={} evictions={} cached={}",
            post_read.hits,
            post_read.misses,
            post_read.admits,
            post_read.evictions,
            post_read.cached_pages
        );

        assert!(
            post_read.total_accesses() >= 20,
            "bead_id={BEAD_E2E} case=seq_read_accesses total={}",
            post_read.total_accesses()
        );
        // Most pages should be cache hits (written data stays in cache).
        assert!(
            post_read.hit_rate_percent() > 40.0,
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
        let total = metrics.total_accesses();
        assert_eq!(
            total, 300,
            "bead_id={BEAD_E2E} case=pressure_total_accesses"
        );
        // All 300 pages should be cache hits (committed pages are cached).
        assert!(
            metrics.hit_rate_percent() > 40.0,
            "bead_id={BEAD_E2E} case=pressure_hit_rate rate={}",
            metrics.hit_rate_percent()
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
        for idx in 0..200usize {
            // 8 hot accesses.
            for h in hot {
                let _ = txn2.get_page(&cx, *h).unwrap();
            }
            // 2 cold accesses (rotating through cold pages).
            let cold_idx = (idx * 2) % 45;
            let _ = txn2.get_page(&cx, pages[5 + cold_idx]).unwrap();
            let _ = txn2.get_page(&cx, pages[5 + (cold_idx + 1) % 45]).unwrap();
        }
        drop(txn2);

        let metrics = pager.cache_metrics_snapshot().unwrap();
        let total = metrics.total_accesses();
        assert_eq!(
            total, 200,
            "bead_id={BEAD_E2E} case=hot_cold_total_accesses"
        );
        // Hot pages should achieve high hit rate after first access.
        assert!(
            metrics.hit_rate_percent() > 40.0,
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
        assert!(
            metrics.total_accesses() > 0,
            "bead_id={BEAD_E2E} case=random_total_accesses"
        );

        let total = metrics.total_accesses();
        assert_eq!(total, 200, "bead_id={BEAD_E2E} case=random_total_accesses");
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
        for _ in 0..30u32 {
            let p = txn.allocate_page(&cx).unwrap();
            let mut data = vec![0u8; ps];
            // Unique fingerprint: page number in first 4 bytes.
            data[..4].copy_from_slice(&p.get().to_le_bytes());
            txn.write_page(&cx, p, &data).unwrap();
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

    #[test]
    fn test_published_snapshot_monotonic_after_commit() {
        init_publication_test_tracing();
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();
        let before = pager.published_snapshot();

        let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
        let p = txn.allocate_page(&cx).unwrap();
        txn.write_page(&cx, p, &vec![0x5A; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let after = pager.published_snapshot();
        assert!(
            after.snapshot_gen > before.snapshot_gen,
            "bead_id={BEAD_ID} case=publication_snapshot_gen_monotonic"
        );
        assert!(
            after.visible_commit_seq > before.visible_commit_seq,
            "bead_id={BEAD_ID} case=publication_commit_seq_monotonic"
        );
        assert_eq!(
            after.db_size,
            p.get(),
            "bead_id={BEAD_ID} case=publication_db_size_updates"
        );
        assert_eq!(
            after.freelist_count, 0,
            "bead_id={BEAD_ID} case=publication_freelist_count_updates"
        );
        assert!(
            !after.checkpoint_active,
            "bead_id={BEAD_ID} case=publication_checkpoint_inactive_after_commit"
        );
    }

    #[test]
    fn test_published_read_hit_does_not_touch_cache_metrics() {
        init_publication_test_tracing();
        let (pager, _) = test_pager();
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let p = {
            let mut txn = pager.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0xAB; ps]).unwrap();
            txn.commit(&cx).unwrap();
            p
        };

        let cache_before = pager.cache_metrics_snapshot().unwrap();
        let published_hits_before = pager.published_page_hits();

        let reader = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let data = reader.get_page(&cx, p).unwrap();
        assert_eq!(data.as_ref()[0], 0xAB);

        let cache_after = pager.cache_metrics_snapshot().unwrap();
        assert_eq!(
            cache_after, cache_before,
            "bead_id={BEAD_ID} case=publication_hit_skips_cache_metrics"
        );
        assert_eq!(
            pager.published_page_hits(),
            published_hits_before + 1,
            "bead_id={BEAD_ID} case=publication_hit_counter"
        );
    }

    #[test]
    fn test_observed_page_publication_populates_snapshot_plane() {
        init_publication_test_tracing();
        let published = PublishedPagerState::new(4, CommitSeq::new(7), JournalMode::Wal, 0);
        let cx = Cx::new();
        let page_no = PageNumber::new(2).unwrap();
        let page = PageData::from_vec(vec![0xAB; PageSize::DEFAULT.as_usize()]);

        let published_page = published.publish_observed_page(
            &cx,
            PublishedPagerUpdate {
                visible_commit_seq: CommitSeq::new(7),
                db_size: 4,
                journal_mode: JournalMode::Wal,
                freelist_count: 0,
                checkpoint_active: false,
            },
            page_no,
            page.clone(),
        );

        assert!(
            published_page,
            "bead_id={BEAD_ID} case=observed_page_publication_applies"
        );
        assert_eq!(
            published.try_get_page(page_no),
            Some(page),
            "bead_id={BEAD_ID} case=observed_page_visible"
        );
        assert_eq!(
            published.snapshot().visible_commit_seq,
            CommitSeq::new(7),
            "bead_id={BEAD_ID} case=observed_page_keeps_commit_seq"
        );
    }

    #[test]
    fn test_observed_page_publication_skips_stale_commit_regression() {
        init_publication_test_tracing();
        let published = PublishedPagerState::new(4, CommitSeq::new(7), JournalMode::Wal, 0);
        let cx = Cx::new();
        let page_no = PageNumber::new(2).unwrap();
        let current_page = PageData::from_vec(vec![0xCC; PageSize::DEFAULT.as_usize()]);

        published.publish(
            &cx,
            PublishedPagerUpdate {
                visible_commit_seq: CommitSeq::new(8),
                db_size: 4,
                journal_mode: JournalMode::Wal,
                freelist_count: 0,
                checkpoint_active: false,
            },
            |pages| {
                pages.insert(page_no, current_page.clone());
            },
        );

        let stale_publish_applied = published.publish_observed_page(
            &cx,
            PublishedPagerUpdate {
                visible_commit_seq: CommitSeq::new(7),
                db_size: 4,
                journal_mode: JournalMode::Wal,
                freelist_count: 0,
                checkpoint_active: false,
            },
            page_no,
            PageData::from_vec(vec![0x11; PageSize::DEFAULT.as_usize()]),
        );

        assert!(
            !stale_publish_applied,
            "bead_id={BEAD_ID} case=stale_observed_page_publication_skipped"
        );
        assert_eq!(
            published.snapshot().visible_commit_seq,
            CommitSeq::new(8),
            "bead_id={BEAD_ID} case=stale_publication_preserves_commit_seq"
        );
        assert_eq!(
            published.try_get_page(page_no),
            Some(current_page),
            "bead_id={BEAD_ID} case=stale_publication_preserves_page"
        );
    }

    #[test]
    fn test_external_refresh_clears_stale_published_pages() {
        init_publication_test_tracing();
        let vfs = MemoryVfs::new();
        let path = PathBuf::from("/published_refresh.db");
        let cx = Cx::new();
        let ps = PageSize::DEFAULT.as_usize();

        let pager1 = SimplePager::open(vfs.clone(), &path, PageSize::DEFAULT).unwrap();
        let pager2 = SimplePager::open(vfs, &path, PageSize::DEFAULT).unwrap();

        let p = {
            let mut txn = pager1.begin(&cx, TransactionMode::Immediate).unwrap();
            let p = txn.allocate_page(&cx).unwrap();
            txn.write_page(&cx, p, &vec![0x11; ps]).unwrap();
            txn.commit(&cx).unwrap();
            p
        };

        let reader = pager1.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(reader.get_page(&cx, p).unwrap().as_ref()[0], 0x11);
        drop(reader);

        let published_before = pager1.published_snapshot();
        assert!(
            published_before.page_set_size > 0,
            "bead_id={BEAD_ID} case=publication_plane_populated_before_refresh"
        );

        let mut txn = pager2.begin(&cx, TransactionMode::Immediate).unwrap();
        txn.write_page(&cx, p, &vec![0x22; ps]).unwrap();
        txn.commit(&cx).unwrap();

        let refreshed_reader = pager1.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let published_after_refresh = pager1.published_snapshot();
        assert!(
            published_after_refresh.snapshot_gen > published_before.snapshot_gen,
            "bead_id={BEAD_ID} case=publication_gen_advances_on_refresh"
        );
        assert_eq!(
            published_after_refresh.visible_commit_seq,
            pager2.published_snapshot().visible_commit_seq,
            "bead_id={BEAD_ID} case=publication_visible_seq_tracks_external_commit"
        );
        assert_eq!(
            published_after_refresh.page_set_size, 0,
            "bead_id={BEAD_ID} case=publication_clears_stale_pages_on_refresh"
        );
        assert_eq!(
            refreshed_reader.get_page(&cx, p).unwrap().as_ref()[0],
            0x22,
            "bead_id={BEAD_ID} case=publication_refresh_reads_latest_committed_page"
        );
    }

    #[test]
    fn test_published_snapshot_retries_during_inflight_publication() {
        init_publication_test_tracing();
        let published = Arc::new(PublishedPagerState::new(
            1,
            CommitSeq::new(1),
            JournalMode::Delete,
            0,
        ));
        published.sequence.store(3, AtomicOrdering::Release);

        let reader_plane = Arc::clone(&published);
        let handle = std::thread::spawn(move || reader_plane.snapshot());

        for _ in 0..10_000 {
            if published.read_retry_count() > 0 {
                break;
            }
            std::thread::yield_now();
        }
        assert!(
            published.read_retry_count() > 0,
            "bead_id={BEAD_ID} case=publication_retry_counter_increments"
        );

        published
            .visible_commit_seq
            .store(2, AtomicOrdering::Release);
        published.db_size.store(2, AtomicOrdering::Release);
        published.journal_mode.store(
            encode_journal_mode(JournalMode::Wal),
            AtomicOrdering::Release,
        );
        published.freelist_count.store(1, AtomicOrdering::Release);
        published
            .checkpoint_active
            .store(true, AtomicOrdering::Release);
        published.page_set_size.store(0, AtomicOrdering::Release);
        published.sequence.store(4, AtomicOrdering::Release);

        let snapshot = handle.join().unwrap();
        assert_eq!(
            snapshot.snapshot_gen, 4,
            "bead_id={BEAD_ID} case=publication_retry_returns_new_snapshot"
        );
        assert_eq!(
            snapshot.visible_commit_seq,
            CommitSeq::new(2),
            "bead_id={BEAD_ID} case=publication_retry_visible_commit_seq"
        );
        assert_eq!(
            snapshot.freelist_count, 1,
            "bead_id={BEAD_ID} case=publication_retry_freelist_count"
        );
        assert!(
            snapshot.checkpoint_active,
            "bead_id={BEAD_ID} case=publication_retry_checkpoint_flag"
        );
    }

    fn run_parallel_counter_benchmark<F>(
        thread_count: usize,
        increments_per_thread: usize,
        increment: F,
    ) -> u64
    where
        F: Fn() + Send + Sync + 'static,
    {
        let start_barrier = Arc::new(std::sync::Barrier::new(thread_count + 1));
        let increment = Arc::new(increment);
        let handles: Vec<_> = (0..thread_count)
            .map(|_| {
                let start_barrier = Arc::clone(&start_barrier);
                let increment = Arc::clone(&increment);
                std::thread::spawn(move || {
                    start_barrier.wait();
                    for _ in 0..increments_per_thread {
                        increment();
                    }
                })
            })
            .collect();

        start_barrier.wait();
        let started = Instant::now();
        for handle in handles {
            handle.join().expect("counter worker should finish");
        }
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    #[test]
    fn test_published_counter_striping_tracks_parallel_increments() {
        let counter = Arc::new(StripedCounter64::new());
        let thread_count = 8;
        let increments_per_thread = 5_000;
        let expected_total =
            u64::try_from(thread_count * increments_per_thread).unwrap_or(u64::MAX);

        let counter_for_bench = Arc::clone(&counter);
        let elapsed_ns =
            run_parallel_counter_benchmark(thread_count, increments_per_thread, move || {
                counter_for_bench.increment();
            });

        assert!(
            elapsed_ns > 0,
            "bead_id={BEAD_ID} case=publication_counter_parallel_elapsed"
        );
        assert_eq!(
            counter.load(),
            expected_total,
            "bead_id={BEAD_ID} case=publication_counter_parallel_total"
        );
    }

    #[test]
    #[ignore = "manual perf evidence for bd-db300.2.3.2"]
    fn bench_bd_db300_2_3_2_publication_counter_striping() {
        #[derive(Debug)]
        struct BaselineCounter(AtomicU64);

        impl BaselineCounter {
            fn increment(&self) {
                self.0.fetch_add(1, AtomicOrdering::Relaxed);
            }

            fn load(&self) -> u64 {
                self.0.load(AtomicOrdering::Acquire)
            }
        }

        let thread_count = std::thread::available_parallelism()
            .map_or(4, |parallelism| parallelism.get().clamp(2, 16));
        let increments_per_thread = 200_000;
        let expected_total =
            u64::try_from(thread_count * increments_per_thread).unwrap_or(u64::MAX);

        let baseline = Arc::new(BaselineCounter(AtomicU64::new(0)));
        let baseline_counter = Arc::clone(&baseline);
        let baseline_ns =
            run_parallel_counter_benchmark(thread_count, increments_per_thread, move || {
                baseline_counter.increment();
            });
        assert_eq!(
            baseline.load(),
            expected_total,
            "bead_id={BEAD_ID} case=publication_counter_baseline_total"
        );

        let striped = Arc::new(StripedCounter64::new());
        let striped_counter = Arc::clone(&striped);
        let striped_ns =
            run_parallel_counter_benchmark(thread_count, increments_per_thread, move || {
                striped_counter.increment();
            });
        assert_eq!(
            striped.load(),
            expected_total,
            "bead_id={BEAD_ID} case=publication_counter_striped_total"
        );

        let speedup_milli = if striped_ns == 0 {
            0_u64
        } else {
            u64::try_from((u128::from(baseline_ns)).saturating_mul(1_000) / u128::from(striped_ns))
                .unwrap_or(u64::MAX)
        };

        println!("BEGIN_BD_DB300_2_3_2_REPORT");
        println!(
            "{{\"threads\":{thread_count},\"increments_per_thread\":{increments_per_thread},\"baseline_ns\":{baseline_ns},\"striped_ns\":{striped_ns},\"speedup_milli\":{speedup_milli}}}"
        );
        println!("END_BD_DB300_2_3_2_REPORT");
    }
}
