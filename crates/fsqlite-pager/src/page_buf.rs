//! Page-aligned buffer allocation and pooling (§1.5 Mechanical Sympathy, bd-22n.1).
//!
//! All page I/O buffers are aligned to `page_size` boundaries, enabling
//! `O_DIRECT` where physically compatible and avoiding partial-page kernel
//! copies.  The alignment guarantee is achieved by over-allocating a `Vec<u8>`
//! and using a sub-slice starting at the first aligned offset — no `unsafe`
//! code is required in this crate.
//!
//! # Key types
//!
//! - [`PageBuf`]: owned, page-sized, page-aligned buffer (`Send + 'static`).
//!   When dropped, the backing allocation is returned to the originating pool.
//! - [`PageBufPool`]: bounded pool keyed by `page_size`.  Avoids repeated heap
//!   allocation on the hot path by reusing returned buffers.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use fsqlite_types::sync_primitives::Mutex;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageSize;

const PAGE_BUF_POOL_FREE_LIST_INITIAL_CAPACITY: usize = 64;
const GLOBAL_PAGE_BUF_RECYCLE_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// PageBuf
// ---------------------------------------------------------------------------

/// Owned, page-sized, page-aligned buffer handle.
///
/// `Send + 'static` — suitable for cross-task transfer.  When dropped, the
/// underlying allocation is returned to the originating pool (if any), making
/// the type cancellation-safe per §4.10.
pub struct PageBuf {
    /// Backing storage.  `None` only transiently during `Drop`.
    backing: Option<Vec<u8>>,
    /// Byte offset into `backing` where the aligned region begins.
    offset: usize,
    /// Page size (= length of the aligned region).
    page_size: usize,
    /// Pool to return the buffer to on drop (`None` for standalone buffers).
    pool: Option<Arc<PageBufPoolInner>>,
}

// Compile-time assertion: PageBuf must be Send + 'static.
const _: () = {
    const fn assert_send_static<T: Send + 'static>() {}
    assert_send_static::<PageBuf>();
};

impl PageBuf {
    /// Create a standalone page-aligned buffer (not pool-backed).
    ///
    /// The buffer is zero-filled.  On drop the allocation is freed normally.
    #[must_use]
    pub fn new(page_size: PageSize) -> Self {
        let size = page_size.as_usize();
        let (backing, offset) = allocate_aligned(size);
        Self {
            backing: Some(backing),
            offset,
            page_size: size,
            pool: None,
        }
    }

    /// The page size (in bytes) of this buffer.
    #[inline]
    #[must_use]
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Get the aligned region as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let backing = self.backing.as_ref().expect("PageBuf backing consumed");
        &backing[self.offset..self.offset + self.page_size]
    }

    /// Get the aligned region as a mutable byte slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let backing = self.backing.as_mut().expect("PageBuf backing consumed");
        &mut backing[self.offset..self.offset + self.page_size]
    }

    /// Returns `true` if this buffer is backed by a pool.
    #[inline]
    #[must_use]
    pub fn is_pooled(&self) -> bool {
        self.pool.is_some()
    }

    /// Whether dropping this buffer will return its allocation to `pool`.
    #[inline]
    #[must_use]
    pub(crate) fn returns_to_pool(&self, pool: &PageBufPool) -> bool {
        self.pool
            .as_ref()
            .is_some_and(|origin| Arc::ptr_eq(origin, &pool.inner))
    }

    /// Raw pointer to the start of the aligned region (useful for alignment
    /// verification in tests).
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }
}

impl Deref for PageBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for PageBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Drop for PageBuf {
    fn drop(&mut self) {
        if let Some(backing) = self.backing.take() {
            if let Some(ref pool) = self.pool {
                pool.return_buf(backing, self.offset);
            }
            // Otherwise the backing Vec drops and frees normally.
        }
    }
}

impl Clone for PageBuf {
    /// Clone produces a standalone (non-pooled) copy of the buffer contents.
    fn clone(&self) -> Self {
        let src = self.as_slice();
        let (mut backing, offset) = allocate_aligned(self.page_size);
        backing[offset..offset + self.page_size].copy_from_slice(src);
        Self {
            backing: Some(backing),
            offset,
            page_size: self.page_size,
            pool: None,
        }
    }
}

impl fmt::Debug for PageBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageBuf")
            .field("page_size", &self.page_size)
            .field("aligned_ptr", &format_args!("{:?}", self.as_ptr()))
            .field("pooled", &self.is_pooled())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Aligned allocation helper
// ---------------------------------------------------------------------------

/// Allocate a zero-filled `Vec<u8>` whose sub-region at the returned offset
/// is aligned to `page_size`.
///
/// # Invariant
///
/// `(vec.as_ptr() as usize + offset) % page_size == 0`
fn allocate_aligned(page_size: usize) -> (Vec<u8>, usize) {
    debug_assert!(page_size.is_power_of_two(), "page_size must be power of 2");
    debug_assert!(page_size >= 512, "page_size must be >= 512");

    // Over-allocate by up to page_size − 1 bytes to guarantee alignment.
    let total = page_size
        .checked_add(page_size - 1)
        .expect("page_size overflow");
    let backing = vec![0u8; total];
    let ptr = backing.as_ptr() as usize;
    let misalignment = ptr & (page_size - 1); // fast modulo for power-of-2
    let offset = if misalignment == 0 {
        0
    } else {
        page_size - misalignment
    };

    debug_assert_eq!((ptr + offset) & (page_size - 1), 0);
    debug_assert!(offset + page_size <= backing.len());

    (backing, offset)
}

// ---------------------------------------------------------------------------
// PageBufPool
// ---------------------------------------------------------------------------

struct PageBufPoolInner {
    page_size: usize,
    free: Mutex<Vec<(Vec<u8>, usize)>>,
    max_buffers: usize,
    total_buffers: AtomicUsize,
    acquire_hits: AtomicUsize,
    acquire_misses: AtomicUsize,
}

struct GlobalPageBuf {
    page_size: usize,
    backing: Vec<u8>,
    offset: usize,
}

static GLOBAL_PAGE_BUF_RECYCLE: OnceLock<Mutex<Vec<GlobalPageBuf>>> = OnceLock::new();

fn global_page_buf_recycle() -> &'static Mutex<Vec<GlobalPageBuf>> {
    GLOBAL_PAGE_BUF_RECYCLE
        .get_or_init(|| Mutex::new(Vec::with_capacity(GLOBAL_PAGE_BUF_RECYCLE_CAPACITY)))
}

fn take_global_page_buf(page_size: usize) -> Option<(Vec<u8>, usize)> {
    let mut recycle = global_page_buf_recycle().lock();
    let idx = recycle
        .iter()
        .rposition(|candidate| candidate.page_size == page_size)?;
    let GlobalPageBuf {
        mut backing,
        offset,
        ..
    } = recycle.swap_remove(idx);
    backing[offset..offset + page_size].fill(0);
    Some((backing, offset))
}

fn recycle_global_page_buf(page_size: usize, backing: Vec<u8>, offset: usize) {
    let mut recycle = global_page_buf_recycle().lock();
    if recycle.len() < GLOBAL_PAGE_BUF_RECYCLE_CAPACITY {
        recycle.push(GlobalPageBuf {
            page_size,
            backing,
            offset,
        });
    }
}

impl PageBufPoolInner {
    /// Return a backing allocation to the free list (if not at capacity).
    fn return_buf(&self, backing: Vec<u8>, offset: usize) {
        let mut free = self.free.lock();
        free.push((backing, offset));
        drop(free);
    }
}

impl Drop for PageBufPoolInner {
    fn drop(&mut self) {
        let mut free = self.free.lock();
        while let Some((backing, offset)) = free.pop() {
            recycle_global_page_buf(self.page_size, backing, offset);
        }
    }
}

/// Bounded pool of page-aligned buffers keyed by `page_size`.
///
/// Avoids repeated heap allocation on the hot path by reusing returned
/// buffers.  When the pool is exhausted, [`acquire`](Self::acquire) allocates
/// a fresh buffer.  When the pool is full, returned buffers are freed normally.
///
/// Thread-safe and cheaply cloneable (backed by `Arc`).
#[derive(Clone)]
pub struct PageBufPool {
    inner: Arc<PageBufPoolInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageBufPoolMetricsSnapshot {
    pub page_buffer_pool_hits: u64,
    pub page_buffer_pool_misses: u64,
}

static FSQLITE_PAGE_BUFFER_POOL_HITS_TOTAL: AtomicUsize = AtomicUsize::new(0);
static FSQLITE_PAGE_BUFFER_POOL_MISSES_TOTAL: AtomicUsize = AtomicUsize::new(0);

#[must_use]
pub fn page_buffer_pool_metrics_snapshot() -> PageBufPoolMetricsSnapshot {
    PageBufPoolMetricsSnapshot {
        page_buffer_pool_hits: u64::try_from(
            FSQLITE_PAGE_BUFFER_POOL_HITS_TOTAL.load(Ordering::Relaxed),
        )
        .unwrap_or(u64::MAX),
        page_buffer_pool_misses: u64::try_from(
            FSQLITE_PAGE_BUFFER_POOL_MISSES_TOTAL.load(Ordering::Relaxed),
        )
        .unwrap_or(u64::MAX),
    }
}

pub fn reset_page_buffer_pool_metrics() {
    FSQLITE_PAGE_BUFFER_POOL_HITS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_PAGE_BUFFER_POOL_MISSES_TOTAL.store(0, Ordering::Relaxed);
}

impl PageBufPool {
    /// Create a new pool for the given `page_size`.
    ///
    /// `max_buffers` is the maximum number of outstanding buffers the pool will
    /// allow to exist (idle + in-use). Once the bound is reached, further
    /// acquisitions fail with [`FrankenError::OutOfMemory`].
    #[must_use]
    pub fn new(page_size: PageSize, max_buffers: usize) -> Self {
        let free_list_capacity = max_buffers.min(PAGE_BUF_POOL_FREE_LIST_INITIAL_CAPACITY);
        Self {
            inner: Arc::new(PageBufPoolInner {
                page_size: page_size.as_usize(),
                free: Mutex::new(Vec::with_capacity(free_list_capacity)),
                max_buffers,
                total_buffers: AtomicUsize::new(0),
                acquire_hits: AtomicUsize::new(0),
                acquire_misses: AtomicUsize::new(0),
            }),
        }
    }

    /// Acquire a page-aligned buffer from the pool.
    ///
    /// Returns a recycled buffer if one is available, or allocates a new one
    /// if the pool has not yet reached its `max_buffers` bound.
    /// Freshly allocated buffers are zero-filled; recycled buffers retain
    /// their previous contents (callers should overwrite via I/O).
    pub fn acquire(&self) -> Result<PageBuf> {
        let page_size = self.inner.page_size;

        let recycled = {
            let mut free = self.inner.free.lock();
            free.pop()
        };

        if let Some((backing, offset)) = recycled {
            self.inner.acquire_hits.fetch_add(1, Ordering::Relaxed);
            FSQLITE_PAGE_BUFFER_POOL_HITS_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Ok(PageBuf {
                backing: Some(backing),
                offset,
                page_size,
                pool: Some(Arc::clone(&self.inner)),
            });
        }

        loop {
            let current = self.inner.total_buffers.load(Ordering::Acquire);
            if current >= self.inner.max_buffers {
                return Err(FrankenError::OutOfMemory);
            }
            if self
                .inner
                .total_buffers
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        self.inner.acquire_misses.fetch_add(1, Ordering::Relaxed);
        FSQLITE_PAGE_BUFFER_POOL_MISSES_TOTAL.fetch_add(1, Ordering::Relaxed);
        let (backing, offset) =
            take_global_page_buf(page_size).unwrap_or_else(|| allocate_aligned(page_size));
        Ok(PageBuf {
            backing: Some(backing),
            offset,
            page_size,
            pool: Some(Arc::clone(&self.inner)),
        })
    }

    /// The page size (in bytes) this pool serves.
    #[inline]
    #[must_use]
    pub fn page_size(&self) -> usize {
        self.inner.page_size
    }

    /// Number of idle buffers currently available in the pool.
    #[must_use]
    pub fn available(&self) -> usize {
        self.inner.free.lock().len()
    }

    /// Maximum number of outstanding buffers the pool will allow.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.max_buffers
    }

    /// Number of buffers currently allocated by this pool, including both
    /// idle and checked-out buffers.
    #[inline]
    #[must_use]
    pub fn total_buffers(&self) -> usize {
        self.inner.total_buffers.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> PageBufPoolMetricsSnapshot {
        PageBufPoolMetricsSnapshot {
            page_buffer_pool_hits: u64::try_from(self.inner.acquire_hits.load(Ordering::Relaxed))
                .unwrap_or(u64::MAX),
            page_buffer_pool_misses: u64::try_from(
                self.inner.acquire_misses.load(Ordering::Relaxed),
            )
            .unwrap_or(u64::MAX),
        }
    }
}

impl fmt::Debug for PageBufPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageBufPool")
            .field("page_size", &self.inner.page_size)
            .field("capacity", &self.inner.max_buffers)
            .field("available", &self.available())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const BEAD_ID: &str = "bd-22n.1";

    // --- Alignment tests ---

    #[test]
    fn test_page_buf_4096_aligned() {
        let buf = PageBuf::new(PageSize::DEFAULT);
        let ptr = buf.as_ptr() as usize;
        assert_eq!(
            ptr % 4096,
            0,
            "bead_id={BEAD_ID} case=page_buf_4096_aligned ptr={ptr:#x}"
        );
        assert_eq!(buf.page_size(), 4096);
        assert_eq!(buf.len(), 4096);
    }

    #[test]
    fn test_page_buf_multiple_sizes() {
        for &size in &[512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let ps = PageSize::new(size).expect("valid page size");
            let buf = PageBuf::new(ps);
            let ptr = buf.as_ptr() as usize;
            assert_eq!(
                ptr % (size as usize),
                0,
                "bead_id={BEAD_ID} case=page_buf_multiple_sizes size={size} ptr={ptr:#x}"
            );
            assert_eq!(buf.len(), size as usize);
        }
    }

    #[test]
    fn test_page_buf_alignment_stress() {
        // Keep all buffers alive simultaneously to stress-test alignment
        // across different heap layouts.
        let mut bufs = Vec::with_capacity(64);
        for i in 0..64u32 {
            let buf = PageBuf::new(PageSize::DEFAULT);
            let ptr = buf.as_ptr() as usize;
            assert_eq!(
                ptr % 4096,
                0,
                "bead_id={BEAD_ID} case=alignment_stress iteration={i}"
            );
            bufs.push(buf);
        }
        drop(bufs);
    }

    // --- Zero-fill and read/write ---

    #[test]
    fn test_page_buf_is_zero_filled() {
        let buf = PageBuf::new(PageSize::DEFAULT);
        assert!(
            buf.iter().all(|&b| b == 0),
            "bead_id={BEAD_ID} case=zero_filled"
        );
    }

    #[test]
    fn test_page_buf_read_write() {
        let mut buf = PageBuf::new(PageSize::DEFAULT);
        buf[0] = 0xDE;
        buf[1] = 0xAD;
        buf[4095] = 0xFF;
        assert_eq!(buf[0], 0xDE);
        assert_eq!(buf[1], 0xAD);
        assert_eq!(buf[4095], 0xFF);
    }

    // --- Pool tests ---

    #[test]
    fn test_page_buf_owned_send_static() {
        fn assert_send_static<T: Send + 'static>() {}
        assert_send_static::<PageBuf>();
    }

    #[test]
    fn test_page_buf_page_aligned() {
        let buf = PageBuf::new(PageSize::DEFAULT);
        let ptr = buf.as_ptr() as usize;
        assert_eq!(ptr % PageSize::DEFAULT.as_usize(), 0);
    }

    #[test]
    fn test_page_buf_pool_reuse() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        assert_eq!(pool.available(), 0);

        // Acquire and drop — should return to pool.
        let buf = pool.acquire().unwrap();
        let ptr1 = buf.as_ptr() as usize;
        drop(buf);
        assert_eq!(pool.available(), 1);

        // Acquire again — should reuse the same allocation.
        let buf2 = pool.acquire().unwrap();
        let ptr2 = buf2.as_ptr() as usize;
        assert_eq!(
            ptr1, ptr2,
            "bead_id={BEAD_ID} case=pool_reuse should reuse same allocation"
        );
        assert_eq!(pool.available(), 0);
        assert_eq!(
            pool.metrics_snapshot(),
            PageBufPoolMetricsSnapshot {
                page_buffer_pool_hits: 1,
                page_buffer_pool_misses: 1,
            },
            "bead_id={BEAD_ID} case=pool_reuse_metrics should count one fresh allocation and one pooled reuse"
        );
    }

    #[test]
    fn test_page_buf_drop_returns_to_pool() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        assert_eq!(pool.available(), 0);
        {
            let _buf = pool.acquire().unwrap();
            assert_eq!(pool.available(), 0);
        }
        assert_eq!(pool.available(), 1, "dropped buffers must be recycled");
    }

    #[test]
    fn test_page_buf_pool_capacity_limit() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 2);

        let b1 = pool.acquire().unwrap();
        let b2 = pool.acquire().unwrap();
        assert!(
            pool.acquire().is_err(),
            "pool must enforce max_buffers bound"
        );

        drop(b1);
        drop(b2);
        assert_eq!(pool.available(), 2, "pool should retain returned buffers");
    }

    #[test]
    fn test_page_buf_pool_free_list_capacity_is_lazy() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 262_144);
        let capacity = pool.inner.free.lock().capacity();
        assert_eq!(
            capacity, PAGE_BUF_POOL_FREE_LIST_INITIAL_CAPACITY,
            "pool construction must not preallocate one idle-list slot per possible buffer"
        );
    }

    #[test]
    fn test_page_buf_pool_bounded() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 2);

        let _b1 = pool.acquire().unwrap();
        let _b2 = pool.acquire().unwrap();
        let err = pool.acquire().unwrap_err();
        assert!(
            matches!(err, FrankenError::OutOfMemory),
            "pool must fail when capacity is exhausted: {err}"
        );
    }

    #[test]
    fn test_page_buf_pool_acquired_is_aligned() {
        for &size in &[512u32, 1024, 4096, 16384, 65536] {
            let ps = PageSize::new(size).expect("valid page size");
            let pool = PageBufPool::new(ps, 4);
            let buf = pool.acquire().unwrap();
            let ptr = buf.as_ptr() as usize;
            assert_eq!(
                ptr % (size as usize),
                0,
                "bead_id={BEAD_ID} case=pool_multiple_sizes size={size}"
            );
            assert_eq!(buf.page_size(), size as usize);
        }
    }

    #[test]
    fn test_page_buf_pool_keyed_by_page_size() {
        let pool_4k = PageBufPool::new(PageSize::DEFAULT, 4);
        let pool_8k = PageBufPool::new(PageSize::new(8192).unwrap(), 4);

        {
            let _buf_4k = pool_4k.acquire().unwrap();
            let _buf_8k = pool_8k.acquire().unwrap();
            assert_eq!(pool_4k.page_size(), 4096);
            assert_eq!(pool_8k.page_size(), 8192);
        }

        assert_eq!(pool_4k.available(), 1);
        assert_eq!(pool_8k.available(), 1);
    }

    #[test]
    fn test_page_buf_pool_cross_pool_recycle_returns_zeroed_buffer() {
        let ps = PageSize::new(32768).unwrap();
        {
            let pool = PageBufPool::new(ps, 1);
            let mut buf = pool.acquire().unwrap();
            buf.as_mut_slice()[0] = 0xAA;
            buf.as_mut_slice()[ps.as_usize() - 1] = 0x55;
        }

        let pool = PageBufPool::new(ps, 1);
        let buf = pool.acquire().unwrap();
        assert!(
            buf.as_slice().iter().all(|byte| *byte == 0),
            "cross-pool recycled buffers must preserve first-acquire zero-fill semantics"
        );
    }

    #[test]
    fn test_page_buf_pool_recycled_alignment() {
        // Acquire, drop, re-acquire — recycled buffer must still be aligned.
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let buf = pool.acquire().unwrap();
        drop(buf);

        let buf2 = pool.acquire().unwrap();
        let ptr = buf2.as_ptr() as usize;
        assert_eq!(ptr % 4096, 0, "bead_id={BEAD_ID} case=recycled_alignment");
    }

    // --- Standalone vs pooled ---

    #[test]
    fn test_page_buf_standalone_not_pooled() {
        let buf = PageBuf::new(PageSize::DEFAULT);
        assert!(!buf.is_pooled());
    }

    #[test]
    fn test_page_buf_pooled() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let buf = pool.acquire().unwrap();
        assert!(buf.is_pooled());
    }

    // --- Deref gives &[u8] reference, not copy ---

    #[test]
    fn test_page_buf_ref_not_copy() {
        let buf = PageBuf::new(PageSize::DEFAULT);
        // Deref gives &[u8] — a reference to the backing store, not a copy.
        let slice: &[u8] = &buf;
        assert_eq!(slice.len(), 4096);
        // The slice pointer must point into the same allocation.
        let slice_ptr = slice.as_ptr() as usize;
        let buf_ptr = buf.as_ptr() as usize;
        assert_eq!(
            slice_ptr, buf_ptr,
            "bead_id={BEAD_ID} case=ref_not_copy Deref must return reference to same memory"
        );
    }

    // --- Debug ---

    #[test]
    fn test_page_buf_debug() {
        let buf = PageBuf::new(PageSize::DEFAULT);
        let debug = format!("{buf:?}");
        assert!(debug.contains("PageBuf"));
        assert!(debug.contains("4096"));
    }

    #[test]
    fn test_page_buf_pool_debug() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 8);
        let debug = format!("{pool:?}");
        assert!(debug.contains("PageBufPool"));
        assert!(debug.contains("4096"));
    }

    // --- Clone (pool) ---

    #[test]
    fn test_page_buf_pool_clone_shares_state() {
        let pool1 = PageBufPool::new(PageSize::DEFAULT, 4);
        let pool2 = pool1.clone();

        let buf = pool1.acquire().unwrap();
        drop(buf);

        // Both clones see the returned buffer.
        assert_eq!(pool1.available(), 1);
        assert_eq!(pool2.available(), 1);
    }

    // --- No-unsafe workspace assertion ---

    #[test]
    fn test_page_buf_no_unsafe_in_workspace() {
        // The workspace enforces `unsafe_code = "forbid"` in [workspace.lints.rust].
        // If unsafe were present in any workspace crate, compilation would fail
        // before this test runs.  The aligned allocation uses only safe Vec<u8>
        // over-allocation — no external alignment crate is needed.
        //
        // This test verifies the workspace Cargo.toml lint setting by parsing it.
        let manifest = include_str!("../../../Cargo.toml");
        assert!(
            manifest.contains(r#"unsafe_code = "forbid""#),
            "bead_id={BEAD_ID} case=no_unsafe_in_workspace \
             Workspace must have unsafe_code = forbid"
        );
    }

    #[test]
    fn test_page_buf_clone_is_standalone_copy() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        let mut buf = pool.acquire().unwrap();
        buf[0] = 0xAB;
        buf[4095] = 0xCD;
        assert!(buf.is_pooled());

        let cloned = buf.clone();
        assert!(!cloned.is_pooled(), "clone must be standalone");
        assert_eq!(cloned[0], 0xAB);
        assert_eq!(cloned[4095], 0xCD);
        assert_eq!(cloned.page_size(), buf.page_size());
        let ptr = cloned.as_ptr() as usize;
        assert_eq!(ptr % 4096, 0, "clone must remain aligned");
    }

    #[test]
    fn test_page_buf_pool_capacity_accessor() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 16);
        assert_eq!(pool.capacity(), 16);
        let pool2 = PageBufPool::new(PageSize::new(512).unwrap(), 1);
        assert_eq!(pool2.capacity(), 1);
    }

    #[test]
    fn test_page_buf_pool_metrics_snapshot_equality() {
        let a = PageBufPoolMetricsSnapshot {
            page_buffer_pool_hits: 5,
            page_buffer_pool_misses: 3,
        };
        let b = PageBufPoolMetricsSnapshot {
            page_buffer_pool_hits: 5,
            page_buffer_pool_misses: 3,
        };
        let c = PageBufPoolMetricsSnapshot {
            page_buffer_pool_hits: 5,
            page_buffer_pool_misses: 4,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_page_buf_as_mut_slice_write_through() {
        let mut buf = PageBuf::new(PageSize::DEFAULT);
        let slice = buf.as_mut_slice();
        slice[0] = 0xFE;
        slice[2047] = 0xED;
        assert_eq!(buf.as_slice()[0], 0xFE);
        assert_eq!(buf.as_slice()[2047], 0xED);
    }

    #[test]
    fn test_metrics_snapshot_debug_clone_copy() {
        let snap = PageBufPoolMetricsSnapshot {
            page_buffer_pool_hits: 10,
            page_buffer_pool_misses: 3,
        };
        let dbg = format!("{snap:?}");
        assert!(dbg.contains("PageBufPoolMetricsSnapshot"));
        let copied = snap;
        assert_eq!(copied, snap);
        assert_eq!(snap.page_buffer_pool_hits, 10);
    }

    #[test]
    fn test_pool_available_tracks_idle_buffers() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 4);
        assert_eq!(pool.available(), 0);
        let buf = pool.acquire().unwrap();
        assert_eq!(pool.available(), 0);
        drop(buf);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn test_pool_per_instance_metrics_snapshot() {
        let pool = PageBufPool::new(PageSize::DEFAULT, 8);
        let snap0 = pool.metrics_snapshot();
        assert_eq!(snap0.page_buffer_pool_hits, 0);
        assert_eq!(snap0.page_buffer_pool_misses, 0);
        let _b1 = pool.acquire().unwrap();
        let snap1 = pool.metrics_snapshot();
        assert_eq!(snap1.page_buffer_pool_misses, 1);
    }

    #[test]
    fn test_reset_global_pool_metrics() {
        reset_page_buffer_pool_metrics();
        let before = page_buffer_pool_metrics_snapshot();
        assert_eq!(before.page_buffer_pool_hits, 0);
        assert_eq!(before.page_buffer_pool_misses, 0);
    }
}
