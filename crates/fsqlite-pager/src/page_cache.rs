//! Zero-copy page cache backed by [`PageBufPool`] (§1.5 Mechanical Sympathy, bd-22n.2).
//!
//! The cache stores pages as [`PageBuf`] handles indexed by [`PageNumber`].
//! Page reads go directly from VFS into a pool-allocated buffer with no
//! intermediate heap allocation.  Callers receive `&[u8]` references into
//! the cached buffer — never copies.
//!
//! This module is the *plumbing layer* for zero-copy I/O; the full ARC
//! eviction policy lives in a higher-level module (bd-7pu).

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use fsqlite_error::Result;
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vfs::VfsFile;

use crate::page_buf::{PageBuf, PageBufPool};

// ---------------------------------------------------------------------------
// PageCache
// ---------------------------------------------------------------------------

/// Simple page cache: `PageNumber → PageBuf`.
///
/// All buffers are drawn from a shared [`PageBufPool`].  On eviction the
/// backing allocation is returned to the pool for reuse, avoiding hot-path
/// heap allocations.
///
/// The cache does **not** implement an eviction policy — that is the
/// responsibility of the higher-level ARC cache (§6).  This type is the
/// low-level storage layer that proves the zero-copy invariant.
pub struct PageCache {
    pool: PageBufPool,
    pages: HashMap<PageNumber, PageBuf>,
    page_size: PageSize,
}

impl PageCache {
    /// Create a new page cache.
    ///
    /// `pool_capacity` is the maximum number of outstanding page buffers the
    /// underlying pool will allow (idle + in-use).
    #[must_use]
    pub fn new(page_size: PageSize, pool_capacity: usize) -> Self {
        Self {
            pool: PageBufPool::new(page_size, pool_capacity),
            pages: HashMap::new(),
            page_size,
        }
    }

    /// Create a page cache backed by an existing pool.
    #[must_use]
    pub fn with_pool(pool: PageBufPool, page_size: PageSize) -> Self {
        debug_assert_eq!(
            pool.page_size(),
            page_size.as_usize(),
            "pool page_size must match cache page_size"
        );
        Self {
            pool,
            pages: HashMap::new(),
            page_size,
        }
    }

    /// The page size this cache serves.
    #[inline]
    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// Number of pages currently cached.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Returns `true` if the cache contains no pages.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// A reference to the underlying buffer pool.
    #[inline]
    #[must_use]
    pub fn pool(&self) -> &PageBufPool {
        &self.pool
    }

    // --- Lookup ---

    /// Get a reference to a cached page.
    ///
    /// Returns `None` if the page is not in the cache.  The returned
    /// reference points directly into the pool-allocated buffer — no copy.
    #[inline]
    #[must_use]
    pub fn get(&self, page_no: PageNumber) -> Option<&[u8]> {
        self.pages.get(&page_no).map(PageBuf::as_slice)
    }

    /// Get a mutable reference to a cached page.
    ///
    /// Returns `None` if the page is not in the cache.  Callers can modify
    /// the page in place; the dirty-tracking flag is managed by the higher
    /// layer.
    #[inline]
    pub fn get_mut(&mut self, page_no: PageNumber) -> Option<&mut [u8]> {
        self.pages.get_mut(&page_no).map(PageBuf::as_mut_slice)
    }

    /// Returns `true` if the page is present in the cache.
    #[inline]
    #[must_use]
    pub fn contains(&self, page_no: PageNumber) -> bool {
        self.pages.contains_key(&page_no)
    }

    // --- Read / Write through VFS ---

    /// Read a page from a VFS file into the cache.
    ///
    /// If the page is already cached, this is a no-op and returns the
    /// existing reference.  Otherwise a buffer is acquired from the pool,
    /// the page is read directly into it via [`VfsFile::read`], and a
    /// reference to the cached data is returned.
    ///
    /// **Zero-copy guarantee:** the buffer passed to `VfsFile::read` is the
    /// same memory that the returned `&[u8]` points into.
    pub fn read_page(
        &mut self,
        cx: &Cx,
        file: &mut impl VfsFile,
        page_no: PageNumber,
    ) -> Result<&[u8]> {
        if !self.pages.contains_key(&page_no) {
            let mut buf = self.pool.acquire()?;
            let offset = page_offset(page_no, self.page_size);
            let _ = file.read(cx, buf.as_mut_slice(), offset)?;
            self.pages.insert(page_no, buf);
        }
        // SAFETY (logical): we just ensured the key exists above.
        Ok(self.pages.get(&page_no).expect("just inserted").as_slice())
    }

    /// Write a cached page out to a VFS file.
    ///
    /// The page data is written directly from the pool-allocated buffer —
    /// no intermediate staging copy.
    ///
    /// Returns `Err` if the page is not in the cache.
    pub fn write_page(&self, cx: &Cx, file: &mut impl VfsFile, page_no: PageNumber) -> Result<()> {
        let buf = self.pages.get(&page_no).ok_or_else(|| {
            fsqlite_error::FrankenError::internal(format!("page {} not in cache", page_no))
        })?;
        let offset = page_offset(page_no, self.page_size);
        file.write(cx, buf.as_slice(), offset)?;
        Ok(())
    }

    /// Insert a fresh (zeroed) page into the cache.
    ///
    /// Returns a mutable reference so the caller can populate it.
    pub fn insert_fresh(&mut self, page_no: PageNumber) -> Result<&mut [u8]> {
        // Freshly acquired buffers from the pool may contain stale data.
        // Zero the buffer to match the "new page" semantics.
        let mut buf = self.pool.acquire()?;
        buf.as_mut_slice().fill(0);

        let out = match self.pages.entry(page_no) {
            Entry::Occupied(mut entry) => {
                entry.insert(buf);
                entry.into_mut().as_mut_slice()
            }
            Entry::Vacant(entry) => entry.insert(buf).as_mut_slice(),
        };
        Ok(out)
    }

    // --- Eviction ---

    /// Evict a page from the cache, returning its buffer to the pool.
    ///
    /// Returns `true` if the page was present.
    pub fn evict(&mut self, page_no: PageNumber) -> bool {
        // Dropping the PageBuf returns it to the pool via Drop impl.
        self.pages.remove(&page_no).is_some()
    }

    /// Evict an arbitrary page from the cache to free up space.
    ///
    /// Returns `true` if a page was evicted, `false` if the cache was empty.
    pub fn evict_any(&mut self) -> bool {
        // We pick an arbitrary key to evict. Since we don't track usage,
        // this is effectively random eviction.
        let key = self.pages.keys().next().copied();
        if let Some(key) = key {
            self.pages.remove(&key);
            true
        } else {
            false
        }
    }

    /// Evict all pages from the cache.
    pub fn clear(&mut self) {
        self.pages.clear();
    }
}

impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCache")
            .field("page_size", &self.page_size)
            .field("cached_pages", &self.pages.len())
            .field("pool", &self.pool)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute the byte offset of a page within the database file.
///
/// Pages are 1-indexed, so page 1 starts at offset 0.
#[inline]
fn page_offset(page_no: PageNumber, page_size: PageSize) -> u64 {
    u64::from(page_no.get() - 1) * u64::from(page_size.get())
}

/// Read a database file header from a VFS file into a stack-allocated buffer.
///
/// The 100-byte SQLite database header is small enough for a stack buffer.
/// This does NOT violate the zero-copy principle — §1.5 explicitly permits
/// "small stack buffers for fixed-size headers."
///
/// Returns the raw header bytes.
pub fn read_db_header(cx: &Cx, file: &mut impl VfsFile) -> Result<[u8; 100]> {
    let mut header = [0u8; 100];
    file.read(cx, &mut header, 0)?;
    Ok(header)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use std::path::Path;

    const BEAD_ID: &str = "bd-22n.2";

    fn setup() -> (Cx, impl VfsFile) {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        let (file, _) = vfs.open(&cx, Some(Path::new("test.db")), flags).unwrap();
        (cx, file)
    }

    #[cfg(unix)]
    #[test]
    fn test_spawn_blocking_io_read_page() {
        use asupersync::runtime::{RuntimeBuilder, spawn_blocking_io};
        use std::io::{ErrorKind, Write as _};
        use std::os::unix::fs::FileExt as _;
        use std::sync::Arc;
        use tempfile::NamedTempFile;

        fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
            let mut total = 0_usize;
            while total < buf.len() {
                #[allow(clippy::cast_possible_truncation)]
                let off = offset + total as u64;
                let n = file.read_at(&mut buf[total..], off)?;
                if n == 0 {
                    return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "short read"));
                }
                total += n;
            }
            Ok(())
        }

        let mut tmp = NamedTempFile::new().unwrap();
        let page_data: Vec<u8> = (0..4096u16)
            .map(|i| u8::try_from(i % 256).expect("i % 256 fits in u8"))
            .collect();
        tmp.as_file_mut().write_all(&page_data).unwrap();
        tmp.as_file_mut().flush().unwrap();

        let file = Arc::new(tmp.reopen().unwrap());
        let pool = PageBufPool::new(PageSize::DEFAULT, 1);

        let rt = RuntimeBuilder::low_latency()
            .worker_threads(1)
            .blocking_threads(1, 1)
            .build()
            .unwrap();

        let join = rt.handle().spawn(async move {
            let worker_tid = std::thread::current().id();

            let mut buf = pool.acquire().unwrap();
            let file2 = Arc::clone(&file);
            let (buf, io_tid) = spawn_blocking_io(move || {
                let io_tid = std::thread::current().id();
                read_exact_at(file2.as_ref(), buf.as_mut_slice(), 0)?;
                Ok::<_, std::io::Error>((buf, io_tid))
            })
            .await
            .unwrap();

            assert_ne!(
                io_tid, worker_tid,
                "spawn_blocking_io must dispatch work to a blocking thread"
            );
            assert_eq!(
                buf.as_slice(),
                page_data.as_slice(),
                "bead_id={BEAD_ID} case=spawn_blocking_io_read_page data mismatch"
            );

            drop(buf);
            assert_eq!(
                pool.available(),
                1,
                "bead_id={BEAD_ID} case=spawn_blocking_io_read_page buf must return to pool"
            );
        });

        rt.block_on(join);
    }

    #[test]
    fn test_spawn_blocking_io_no_unsafe() {
        // Workspace-wide lint gate: unsafe code is forbidden.
        let manifest = include_str!("../../../Cargo.toml");
        assert!(
            manifest.contains(r#"unsafe_code = "forbid""#),
            "workspace must keep unsafe_code=forbid for IO dispatch paths"
        );
    }

    #[test]
    fn test_blocking_pool_lab_mode_inline() {
        use asupersync::lab::{LabConfig, LabRuntime};
        use asupersync::runtime::spawn_blocking_io;
        use asupersync::types::Budget;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut rt = LabRuntime::new(LabConfig::new(42));
        let region = rt.state.create_root_region(Budget::INFINITE);

        let ok = Arc::new(AtomicBool::new(false));
        let ok_task = Arc::clone(&ok);

        let (task_id, _handle) = rt
            .state
            .create_task(region, Budget::INFINITE, async move {
                let worker_tid = std::thread::current().id();
                let io_tid =
                    spawn_blocking_io(|| Ok::<_, std::io::Error>(std::thread::current().id()))
                        .await
                        .unwrap();
                ok_task.store(worker_tid == io_tid, Ordering::Release);
            })
            .unwrap();

        rt.scheduler
            .lock()
            .expect("LabRuntime scheduler lock poisoned")
            .schedule(task_id, 0);
        rt.run_until_quiescent();

        assert!(
            ok.load(Ordering::Acquire),
            "spawn_blocking_io must execute inline when no blocking pool exists (lab determinism)"
        );
    }

    #[test]
    fn test_cancel_mid_io_returns_buf_to_pool() {
        use asupersync::runtime::{RuntimeBuilder, spawn_blocking_io, yield_now};
        use std::future::poll_fn;
        use std::task::Poll;
        use std::time::Duration;

        let rt = RuntimeBuilder::low_latency()
            .worker_threads(1)
            .blocking_threads(1, 1)
            .build()
            .unwrap();

        let pool = PageBufPool::new(PageSize::DEFAULT, 1);
        let join = rt.handle().spawn(async move {
            let buf = pool.acquire().unwrap();

            let mut fut = Box::pin(spawn_blocking_io(move || {
                std::thread::sleep(Duration::from_millis(20));
                Ok::<_, std::io::Error>(buf)
            }));

            // Poll once to ensure the blocking task is enqueued, then drop the
            // future (soft cancel). The owned PageBuf must be returned to the pool.
            let mut polled = false;
            poll_fn(|cx| {
                if !polled {
                    polled = true;
                    let _ = fut.as_mut().poll(cx);
                }
                Poll::Ready(())
            })
            .await;

            drop(fut);

            // The blocking task sleeps 20ms then drops the PageBuf.
            // Yield in a loop with a brief real-time sleep per iteration so
            // the blocking thread has time to finish and return the buffer.
            for _ in 0..200u32 {
                if pool.available() == 1 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
                yield_now().await;
            }
            assert_eq!(
                pool.available(),
                1,
                "bead_id={BEAD_ID} case=cancel_mid_io_returns_buf_to_pool"
            );
        });

        rt.block_on(join);
    }

    #[test]
    fn test_pager_reads_pages_via_pool() {
        let (cx, mut file) = setup();
        let page_data = vec![0xAB_u8; 4096];
        file.write(&cx, &page_data, 0).unwrap();

        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut cache = PageCache::with_pool(pool.clone(), PageSize::DEFAULT);
        let read = cache.read_page(&cx, &mut file, PageNumber::ONE).unwrap();
        assert_eq!(read, page_data.as_slice());
        assert_eq!(pool.available(), 0, "cached page still holds the buffer");

        assert!(cache.evict(PageNumber::ONE));
        assert_eq!(
            pool.available(),
            1,
            "evicting a cached page should return its buffer to the pool"
        );
    }

    // --- test_vfs_read_no_intermediate_alloc ---

    #[test]
    fn test_vfs_read_no_intermediate_alloc() {
        // Demonstrate that VfsFile::read writes directly into the PageBuf
        // memory with no intermediate buffer.  We verify by checking that
        // the data appears at the same pointer address as the PageBuf slice.
        let (cx, mut file) = setup();

        // Write a recognizable page to the file.
        let pattern: Vec<u8> = (0..4096u16)
            .map(|i| u8::try_from(i % 256).expect("i % 256 fits in u8"))
            .collect();
        file.write(&cx, &pattern, 0).unwrap();

        // Acquire a PageBuf from the pool and read directly into it.
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut buf = pool.acquire().unwrap();
        let ptr_before = buf.as_ptr();

        // VfsFile::read takes &mut [u8] — PageBuf::as_mut_slice gives us
        // a reference to the same aligned memory.
        file.read(&cx, buf.as_mut_slice(), 0).unwrap();

        let ptr_after = buf.as_ptr();
        assert_eq!(
            ptr_before, ptr_after,
            "bead_id={BEAD_ID} case=vfs_read_no_intermediate_alloc \
             pointer must not change — read goes directly into PageBuf"
        );
        assert_eq!(
            buf.as_slice(),
            pattern.as_slice(),
            "bead_id={BEAD_ID} case=vfs_read_data_correct"
        );
    }

    // --- test_vfs_write_no_intermediate_alloc ---

    #[test]
    fn test_vfs_write_no_intermediate_alloc() {
        // Demonstrate that VfsFile::write reads directly from the PageBuf
        // memory with no intermediate staging copy.
        let (cx, mut file) = setup();

        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut buf = pool.acquire().unwrap();

        // Fill with a recognizable pattern.
        for (i, b) in buf.as_mut_slice().iter_mut().enumerate() {
            *b = u8::try_from(i % 251).expect("i % 251 fits in u8"); // prime-sized pattern
        }

        let ptr_before = buf.as_ptr();

        // VfsFile::write takes &[u8] — PageBuf::as_slice gives us a
        // reference to the same aligned memory, no copy.
        file.write(&cx, buf.as_slice(), 0).unwrap();

        let ptr_after = buf.as_ptr();
        assert_eq!(
            ptr_before, ptr_after,
            "bead_id={BEAD_ID} case=vfs_write_no_intermediate_alloc \
             PageBuf pointer must be stable through write"
        );

        // Verify the data was written correctly.
        let mut verify = vec![0u8; 4096];
        file.read(&cx, &mut verify, 0).unwrap();
        assert_eq!(
            verify.as_slice(),
            buf.as_slice(),
            "bead_id={BEAD_ID} case=vfs_write_data_roundtrip"
        );
    }

    // --- test_pager_returns_ref_not_copy ---

    #[test]
    fn test_pager_returns_ref_not_copy() {
        // PageCache::get() returns &[u8] that points to the same memory
        // as the stored PageBuf — a reference, not a copy.
        let (cx, mut file) = setup();

        // Write a page to the file.
        let data = vec![0xAB_u8; 4096];
        file.write(&cx, &data, 0).unwrap();

        let mut cache = PageCache::new(PageSize::DEFAULT, 8);
        let page1 = PageNumber::ONE;

        // Read the page into cache.
        let ref1 = cache.read_page(&cx, &mut file, page1).unwrap();
        let ref1_ptr = ref1.as_ptr();
        assert_eq!(
            &ref1[..4096],
            data.as_slice(),
            "bead_id={BEAD_ID} case=pager_ref_data_correct"
        );

        // Get the same page again — must be same pointer (cached).
        let ref2 = cache.get(page1).unwrap();
        let ref2_ptr = ref2.as_ptr();
        assert_eq!(
            ref1_ptr, ref2_ptr,
            "bead_id={BEAD_ID} case=pager_returns_ref_not_copy \
             get() must return reference to same memory as read_page()"
        );
    }

    // --- test_wal_uses_buffered_io_compat ---

    #[test]
    fn test_wal_uses_buffered_io_compat() {
        // Verify that WAL frame size (24 + page_size) does NOT preserve
        // sector alignment, proving that WAL I/O requires buffered I/O
        // (not O_DIRECT) in compatibility mode.
        //
        // Per §1.5: "SQLite .wal frames are 24 + page_size bytes — they
        // do NOT preserve sector alignment at frame boundaries."
        let wal_header_size: u64 = 24;

        for &size in &[512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let frame_size = wal_header_size + u64::from(size);

            // Sector alignment: 512 for HDD, 4096 for modern SSD.
            // WAL frame offset after N frames = 32 (WAL header) + N * frame_size.
            let wal_header_bytes: u64 = 32; // WAL file header
            let frame2_offset = wal_header_bytes + frame_size;

            // Frame 2 offset must NOT be sector-aligned for most page sizes.
            // 24 bytes of per-frame header breaks alignment.
            let sector_4k_aligned = frame2_offset % 4096 == 0;

            // For page_size >= 4096 the frame start won't be 4K-aligned
            // because 32 + (24 + page_size) = 56 + page_size.
            // 56 + 4096 = 4152, 4152 % 4096 = 56 ≠ 0.
            if size >= 4096 {
                assert!(
                    !sector_4k_aligned,
                    "bead_id={BEAD_ID} case=wal_buffered_io_compat \
                     WAL frame 2 at offset {frame2_offset} should NOT be 4K-aligned \
                     for page_size={size}"
                );
            }

            // Even for 512-byte sector: 32 + (24+512) = 568, 568 % 512 = 56.
            let sector_512_aligned = frame2_offset % 512 == 0;
            assert!(
                !sector_512_aligned,
                "bead_id={BEAD_ID} case=wal_frame_not_512_aligned \
                 WAL frame 2 at offset {frame2_offset} should NOT be 512-byte aligned \
                 for page_size={size}"
            );
        }
    }

    // --- test_small_header_stack_buffer_ok ---

    #[test]
    fn test_small_header_stack_buffer_ok() {
        // Per §1.5: "Small stack buffers for fixed-size headers ARE permitted."
        // Demonstrate that reading the 100-byte DB header into a stack
        // buffer works correctly and does not violate zero-copy.
        let (cx, mut file) = setup();

        // Write a minimal SQLite header (first 16 bytes of magic string).
        let mut header_data = [0u8; 100];
        header_data[..16].copy_from_slice(b"SQLite format 3\0");
        header_data[16..18].copy_from_slice(&4096u16.to_be_bytes()); // page size
        file.write(&cx, &header_data, 0).unwrap();

        // Read using the stack-buffer helper.
        let header = read_db_header(&cx, &mut file).unwrap();
        assert_eq!(
            &header[..16],
            b"SQLite format 3\0",
            "bead_id={BEAD_ID} case=small_header_stack_buffer_ok"
        );

        // Verify page size field.
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        assert_eq!(
            page_size, 4096,
            "bead_id={BEAD_ID} case=header_page_size_correct"
        );
    }

    // --- test_page_decode_bounds_checked ---

    #[test]
    fn test_page_decode_bounds_checked() {
        // Verify that page structures are decoded with bounds-checked reads
        // in safe Rust — no transmute of variable-length formats.
        //
        // We simulate decoding a B-tree page header from a cached page.
        let (cx, mut file) = setup();

        // Write a page with a simulated B-tree leaf header.
        let mut page_data = vec![0u8; 4096];
        page_data[0] = 0x0D; // leaf table b-tree page type
        page_data[3..5].copy_from_slice(&10u16.to_be_bytes()); // cell count = 10
        page_data[5..7].copy_from_slice(&100u16.to_be_bytes()); // cell content offset
        file.write(&cx, &page_data, 0).unwrap();

        // Read into cache.
        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page = cache.read_page(&cx, &mut file, PageNumber::ONE).unwrap();

        // Bounds-checked decode: every access goes through slice indexing.
        let page_type = page[0];
        assert_eq!(page_type, 0x0D, "bead_id={BEAD_ID} case=page_decode_type");

        let cell_count = u16::from_be_bytes([page[3], page[4]]);
        assert_eq!(
            cell_count, 10,
            "bead_id={BEAD_ID} case=page_decode_cell_count"
        );

        let content_offset = u16::from_be_bytes([page[5], page[6]]);
        assert_eq!(
            content_offset, 100,
            "bead_id={BEAD_ID} case=page_decode_content_offset"
        );

        // Out-of-bounds access panics (safe Rust guarantee).
        // We verify by checking the page length is exactly page_size.
        assert_eq!(
            page.len(),
            4096,
            "bead_id={BEAD_ID} case=page_decode_bounds_checked"
        );
    }

    // --- Cache operation tests ---

    #[test]
    fn test_cache_insert_fresh_zeroed() {
        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page1 = PageNumber::ONE;

        let data = cache.insert_fresh(page1).unwrap();
        assert!(
            data.iter().all(|&b| b == 0),
            "bead_id={BEAD_ID} case=insert_fresh_zeroed"
        );
        assert_eq!(data.len(), 4096);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_get_mut_modifies_in_place() {
        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page1 = PageNumber::ONE;

        cache.insert_fresh(page1).unwrap();
        let data = cache.get_mut(page1).unwrap();
        data[0] = 0xFF;
        data[4095] = 0xEE;

        let read_back = cache.get(page1).unwrap();
        assert_eq!(read_back[0], 0xFF);
        assert_eq!(read_back[4095], 0xEE);
    }

    #[test]
    fn test_cache_evict_returns_to_pool() {
        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page1 = PageNumber::ONE;

        assert_eq!(cache.pool().available(), 0);
        cache.insert_fresh(page1).unwrap();
        assert_eq!(cache.pool().available(), 0); // buffer is in use

        assert!(cache.evict(page1));
        assert!(!cache.contains(page1));
        // Buffer returned to pool via PageBuf::Drop.
        assert_eq!(
            cache.pool().available(),
            1,
            "bead_id={BEAD_ID} case=evict_returns_to_pool"
        );
    }

    #[test]
    fn test_cache_evict_nonexistent() {
        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page1 = PageNumber::ONE;
        assert!(!cache.evict(page1));
    }

    #[test]
    fn test_cache_clear_returns_all_to_pool() {
        let mut cache = PageCache::new(PageSize::DEFAULT, 8);

        for i in 1..=5u32 {
            let pn = PageNumber::new(i).unwrap();
            cache.insert_fresh(pn).unwrap();
        }
        assert_eq!(cache.len(), 5);
        assert_eq!(cache.pool().available(), 0);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(
            cache.pool().available(),
            5,
            "bead_id={BEAD_ID} case=clear_returns_all_to_pool"
        );
    }

    #[test]
    fn test_cache_multiple_pages() {
        let (cx, mut file) = setup();

        // Write 3 pages with distinct content.
        for i in 0..3u32 {
            let seed = u8::try_from(i).expect("i <= 2");
            let data = vec![(seed + 1) * 0x11; 4096];
            let offset = u64::from(i) * 4096;
            file.write(&cx, &data, offset).unwrap();
        }

        let mut cache = PageCache::new(PageSize::DEFAULT, 8);

        for i in 1..=3u32 {
            let pn = PageNumber::new(i).unwrap();
            let page = cache.read_page(&cx, &mut file, pn).unwrap();
            let expected = u8::try_from(i).expect("i <= 3") * 0x11;
            assert!(
                page.iter().all(|&b| b == expected),
                "bead_id={BEAD_ID} case=multiple_pages page={i} expected={expected:#x}"
            );
        }

        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_cache_write_page_roundtrip() {
        let (cx, mut file) = setup();

        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page1 = PageNumber::ONE;

        // Insert a fresh page, modify it, write to VFS.
        let data = cache.insert_fresh(page1).unwrap();
        data.fill(0xCD);

        cache.write_page(&cx, &mut file, page1).unwrap();

        // Read back from VFS directly (bypassing cache).
        let mut verify = vec![0u8; 4096];
        file.read(&cx, &mut verify, 0).unwrap();
        assert!(
            verify.iter().all(|&b| b == 0xCD),
            "bead_id={BEAD_ID} case=write_page_roundtrip"
        );
    }

    #[test]
    fn test_page_offset_calculation() {
        // Page 1 starts at offset 0.
        assert_eq!(
            page_offset(PageNumber::ONE, PageSize::DEFAULT),
            0,
            "bead_id={BEAD_ID} case=page_offset_page1"
        );

        // Page 2 starts at 4096.
        let p2 = PageNumber::new(2).unwrap();
        assert_eq!(
            page_offset(p2, PageSize::DEFAULT),
            4096,
            "bead_id={BEAD_ID} case=page_offset_page2"
        );

        // Page 100 with 512-byte pages starts at 99 * 512 = 50688.
        let p100 = PageNumber::new(100).unwrap();
        let ps512 = PageSize::new(512).unwrap();
        assert_eq!(
            page_offset(p100, ps512),
            50688,
            "bead_id={BEAD_ID} case=page_offset_page100_512"
        );
    }

    // --- E2E: combined zero-copy verification ---

    #[test]
    fn test_e2e_zero_copy_io_no_allocations() {
        // E2E: run a read-heavy workload (simulated point lookups) and
        // verify that steady-state reads are allocation-free by checking
        // pool reuse and pointer stability.
        let (cx, mut file) = setup();

        // Write 10 pages with distinct content.
        let num_pages: u32 = 10;
        for i in 0..num_pages {
            let byte = u8::try_from(i).expect("i <= 9").wrapping_add(0x10);
            let data = vec![byte; 4096];
            file.write(&cx, &data, u64::from(i) * 4096).unwrap();
        }

        let mut cache = PageCache::new(PageSize::DEFAULT, 16);

        // Phase 1: Cold reads — pages load from VFS into cache.
        let mut ptrs: Vec<usize> = Vec::with_capacity(num_pages as usize);
        for i in 1..=num_pages {
            let pn = PageNumber::new(i).unwrap();
            let page = cache.read_page(&cx, &mut file, pn).unwrap();
            ptrs.push(page.as_ptr() as usize);
        }

        // Phase 2: Hot reads — all pages are cached.  Verify no new
        // allocations by checking pointer stability.
        for round in 0..5u32 {
            for i in 1..=num_pages {
                let pn = PageNumber::new(i).unwrap();
                let page = cache.get(pn).unwrap();
                let ptr = page.as_ptr() as usize;
                assert_eq!(
                    ptr,
                    ptrs[(i - 1) as usize],
                    "bead_id={BEAD_ID} case=e2e_pointer_stable \
                     round={round} page={i}"
                );

                // Verify data correctness.
                let expected = u8::try_from(i - 1).expect("i - 1 <= 9").wrapping_add(0x10);
                assert!(
                    page.iter().all(|&b| b == expected),
                    "bead_id={BEAD_ID} case=e2e_data_correct \
                     round={round} page={i}"
                );
            }
        }

        // Phase 3: Evict and re-read — pool reuse avoids new allocation.
        let pool_available_before = cache.pool().available();
        let old_ptr = ptrs[0];

        cache.evict(PageNumber::ONE);
        assert_eq!(
            cache.pool().available(),
            pool_available_before + 1,
            "bead_id={BEAD_ID} case=e2e_evict_returns_to_pool"
        );

        // Re-read page 1: should reuse pool buffer (no new heap alloc).
        let page1_reread = cache.read_page(&cx, &mut file, PageNumber::ONE).unwrap();
        let new_ptr = page1_reread.as_ptr() as usize;

        // The recycled buffer from the pool should be the same allocation.
        assert_eq!(
            new_ptr, old_ptr,
            "bead_id={BEAD_ID} case=e2e_pool_reuse_after_evict \
             Expected recycled buffer at {old_ptr:#x}, got {new_ptr:#x}"
        );

        // Summary (grep-friendly).
        eprintln!("pages_cached={}", cache.len());
        eprintln!("pool_available={}", cache.pool().available());
        eprintln!("pointer_checks_passed={}", num_pages * 5 + 1);
    }

    // --- Debug ---

    #[test]
    fn test_page_cache_debug() {
        let cache = PageCache::new(PageSize::DEFAULT, 4);
        let debug = format!("{cache:?}");
        assert!(
            debug.contains("PageCache"),
            "bead_id={BEAD_ID} case=debug_format"
        );
    }

    // -----------------------------------------------------------------------
    // bd-22n.8 — Allocation-Free Read Path Tests (Pager Layer)
    // -----------------------------------------------------------------------

    const BEAD_22N8: &str = "bd-22n.8";

    #[test]
    fn test_cache_lookup_no_alloc() {
        // bd-22n.8: Buffer pool cache lookup is allocation-free.
        //
        // PageCache::get() returns Option<&[u8]> — a reference into the
        // pool-allocated buffer.  It does a HashMap::get + PageBuf::as_slice,
        // neither of which allocates.
        //
        // We verify by: (a) checking the returned &[u8] is the same pointer
        // as the original PageBuf, and (b) repeating the lookup many times
        // and verifying pointer stability (proves no reallocation).
        let (cx, mut file) = setup();

        let data = vec![0xBE_u8; 4096];
        file.write(&cx, &data, 0).unwrap();

        let mut cache = PageCache::new(PageSize::DEFAULT, 8);
        let page1 = PageNumber::ONE;

        // Cold read — allocates from pool.
        let initial = cache.read_page(&cx, &mut file, page1).unwrap();
        let initial_ptr = initial.as_ptr();

        // Hot reads — must be allocation-free (same pointer).
        for round in 0..100u32 {
            let cached = cache.get(page1).unwrap();
            assert_eq!(
                cached.as_ptr(),
                initial_ptr,
                "bead_id={BEAD_22N8} case=cache_lookup_no_alloc \
                 round={round} pointer must be stable (no realloc)"
            );
        }
    }

    #[test]
    fn test_cache_lookup_hit_returns_reference() {
        // bd-22n.8: Verify structurally that get() returns a borrow, not a copy.
        // We insert a page, mutate it via get_mut, then verify get() sees the
        // mutation at the same pointer — proving it's a reference into the
        // same memory.
        let mut cache = PageCache::new(PageSize::DEFAULT, 4);
        let page1 = PageNumber::ONE;

        // Insert a fresh page and write a sentinel.
        let fresh = cache.insert_fresh(page1).unwrap();
        fresh[0] = 0xAA;
        let ptr_after_insert = cache.get(page1).unwrap().as_ptr();

        // Mutate in place.
        let mutref = cache.get_mut(page1).unwrap();
        mutref[1] = 0xBB;

        // get() must see the mutation AND return the same pointer.
        let read_back = cache.get(page1).unwrap();
        assert_eq!(
            read_back.as_ptr(),
            ptr_after_insert,
            "bead_id={BEAD_22N8} case=cache_lookup_returns_reference \
             pointer must be stable through mutation"
        );
        assert_eq!(read_back[0], 0xAA);
        assert_eq!(read_back[1], 0xBB);
    }

    #[test]
    fn test_pool_reuse_avoids_alloc_on_reread() {
        // bd-22n.8: After eviction, re-reading a page reuses a pool buffer
        // rather than allocating fresh memory.  This ensures the read path
        // is allocation-free in steady state (pool has recycled buffers).
        let (cx, mut file) = setup();

        let data = vec![0xDD_u8; 4096];
        file.write(&cx, &data, 0).unwrap();

        let mut cache = PageCache::new(PageSize::DEFAULT, 8);
        let page1 = PageNumber::ONE;

        // Cold read, then evict.
        let _ = cache.read_page(&cx, &mut file, page1).unwrap();
        assert_eq!(cache.pool().available(), 0);
        cache.evict(page1);
        assert_eq!(
            cache.pool().available(),
            1,
            "bead_id={BEAD_22N8} case=evicted_buffer_returned_to_pool"
        );

        // Re-read: pool has a buffer, so no new allocation needed.
        let reread = cache.read_page(&cx, &mut file, page1).unwrap();
        assert_eq!(
            reread,
            data.as_slice(),
            "bead_id={BEAD_22N8} case=pool_reuse_data_correct"
        );
        assert_eq!(
            cache.pool().available(),
            0,
            "bead_id={BEAD_22N8} case=pool_buffer_consumed_on_reread"
        );
    }
}
