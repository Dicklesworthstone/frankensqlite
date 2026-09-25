#![allow(clippy::future_not_send)]
//! Overflow page chain management (§11, bd-2kvo).
//!
//! When a cell's payload exceeds the local maximum for its page type,
//! the excess bytes are stored in a linked list of overflow pages.
//! Each overflow page stores up to `(usable_size - 4)` bytes of payload,
//! with the first 4 bytes being the page number of the next overflow
//! page (0 for the last page in the chain).
//!
//! ```text
//! ┌───────────────────────────────────┐
//! │ Next overflow pgno (4 bytes, BE)  │
//! ├───────────────────────────────────┤
//! │ Overflow data (usable_size - 4)   │
//! └───────────────────────────────────┘
//! ```

use crate::cursor::{PageReader, PageWriter};
use crate::instrumentation;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::MAX_ALLOCATION_SIZE;
use fsqlite_types::{PageData, PageNumber};
use smallvec::SmallVec;
use std::collections::HashSet;

/// Maximum number of overflow pages in a chain (safety bound to prevent
/// infinite loops on corrupt databases).
pub const MAX_OVERFLOW_CHAIN: usize = 1_000_000;

/// Inline capacity of [`VisitedPages`]; chains up to this many pages are
/// tracked without touching the heap.
const VISITED_INLINE_PAGES: usize = 8;

/// Pages already visited on one overflow chain.
///
/// Every overflow read runs this check, and nearly every chain is a few pages
/// long, so membership is a linear scan over an inline array; only a chain
/// longer than [`VISITED_INLINE_PAGES`] moves into a `HashSet`. This keeps the
/// per-read cost free of heap allocation and hashing without weakening cycle
/// detection.
#[derive(Debug, Default)]
struct VisitedPages {
    inline: SmallVec<[PageNumber; VISITED_INLINE_PAGES]>,
    spilled: HashSet<PageNumber>,
}

impl VisitedPages {
    fn len(&self) -> usize {
        self.inline.len() + self.spilled.len()
    }

    fn contains(&self, page_no: PageNumber) -> bool {
        self.inline.contains(&page_no) || self.spilled.contains(&page_no)
    }

    /// Record `page_no`; `Ok(false)` if it was already visited.
    fn insert(&mut self, page_no: PageNumber) -> Result<bool> {
        if self.contains(page_no) {
            return Ok(false);
        }
        if self.spilled.is_empty() && self.inline.len() < VISITED_INLINE_PAGES {
            self.inline.push(page_no);
            return Ok(true);
        }
        self.spilled
            .try_reserve(self.inline.len() + 1)
            .map_err(|_| FrankenError::OutOfMemory)?;
        self.spilled.extend(self.inline.drain(..));
        self.spilled.insert(page_no);
        Ok(true)
    }
}

/// Validation shared by the callback and async readers. A payload length is
/// not a cycle detector: a corrupt chain can repeat bytes until that length
/// is satisfied. Track visited pages and validate the links against the full
/// payload, even when the caller only requests a prefix.
#[derive(Debug)]
struct OverflowReadState {
    visited: VisitedPages,
    usable_size: usize,
    remaining: usize,
}

impl OverflowReadState {
    fn new(usable_size: u32, remaining: usize) -> Self {
        Self {
            visited: VisitedPages::default(),
            usable_size: usable_size as usize,
            remaining,
        }
    }

    fn visit(&mut self, page_no: PageNumber) -> Result<()> {
        if page_no.get() == 1 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "database header page used as an overflow page".to_owned(),
            });
        }
        if self.visited.len() >= MAX_OVERFLOW_CHAIN {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("overflow chain exceeds maximum length of {MAX_OVERFLOW_CHAIN}"),
            });
        }
        if !self.visited.insert(page_no)? {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("cycle in overflow chain at page {}", page_no.get()),
            });
        }
        Ok(())
    }

    fn inspect(&mut self, page: &[u8]) -> Result<(u32, usize)> {
        // Every overflow page has the database's usable extent, including the
        // final page. Concatenating short pages silently shifts payload bytes.
        if page.len() < self.usable_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow page too small: expected at least {} bytes, got {}",
                    self.usable_size,
                    page.len()
                ),
            });
        }
        let next = u32::from_be_bytes([page[0], page[1], page[2], page[3]]);
        if next == 1 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "overflow chain points to the database header page".to_owned(),
            });
        }
        if next != 0 {
            // Validate every observed link, even if a prefix read stops on
            // this page. Only zero is a terminator; an unrepresentable page
            // number must not bypass validation when no next read is needed.
            let next_page = PageNumber::new(next).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("invalid next overflow page number {next}"),
            })?;
            if self.visited.contains(next_page) {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: format!("cycle in overflow chain at page {next}"),
                });
            }
        }

        let available = self.remaining.min(self.usable_size - 4);
        let remaining = self.remaining - available;
        if remaining > 0 && next == 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "unexpected end of overflow chain".to_owned(),
            });
        }
        if remaining == 0 && next != 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "overflow chain continues beyond the declared payload".to_owned(),
            });
        }
        self.remaining = remaining;
        Ok((next, available))
    }
}

/// Read a complete payload that spans local data and an overflow chain.
///
/// `local_data` is the portion of the payload stored on the B-tree page.
/// `first_overflow` is the page number of the first overflow page.
/// `total_payload_size` is the total payload size in bytes.
/// `usable_size` is the usable page size.
/// `read_page` is a callback that reads a raw page by page number.
///
/// Returns the complete reassembled payload.
pub fn read_overflow_chain<F, P>(
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    read_page: &mut F,
) -> Result<Vec<u8>>
where
    F: FnMut(PageNumber) -> Result<P>,
    P: AsRef<[u8]>,
{
    instrumentation::record_owned_payload_materialization(
        usize::try_from(total_payload_size).unwrap_or(usize::MAX),
    );
    let mut payload = Vec::new();
    read_overflow_chain_into(
        local_data,
        first_overflow,
        total_payload_size,
        usable_size,
        read_page,
        &mut payload,
    )?;
    Ok(payload)
}

/// Read a complete payload into an existing buffer.
pub fn read_overflow_chain_into<F, P>(
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    read_page: &mut F,
    out: &mut Vec<u8>,
) -> Result<()>
where
    F: FnMut(PageNumber) -> Result<P>,
    P: AsRef<[u8]>,
{
    read_overflow_chain_prefix_into(
        local_data,
        first_overflow,
        total_payload_size,
        usable_size,
        usize::try_from(total_payload_size).unwrap_or(usize::MAX),
        read_page,
        out,
    )?;

    #[allow(clippy::cast_possible_truncation)]
    let total_size = total_payload_size as usize;
    if out.len() != total_size {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "read overflow chain size mismatch: expected {}, got {}",
                total_size,
                out.len()
            ),
        });
    }
    Ok(())
}

/// Read only a prefix of a payload that spans local data and an overflow chain.
pub fn read_overflow_chain_prefix_into<F, P>(
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    max_prefix_bytes: usize,
    read_page: &mut F,
    out: &mut Vec<u8>,
) -> Result<()>
where
    F: FnMut(PageNumber) -> Result<P>,
    P: AsRef<[u8]>,
{
    out.clear();
    if total_payload_size > MAX_ALLOCATION_SIZE {
        return Err(FrankenError::TooBig);
    }
    if usable_size <= 4 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "invalid usable page size {} for overflow chain",
                usable_size
            ),
        });
    }

    #[allow(clippy::cast_possible_truncation)]
    let total_size = total_payload_size as usize;
    let target_size = total_size.min(max_prefix_bytes);
    if target_size == 0 {
        return Ok(());
    }
    let local_copy_len = local_data.len().min(target_size);

    out.try_reserve(target_size)
        .map_err(|_| FrankenError::OutOfMemory)?;
    out.extend_from_slice(&local_data[..local_copy_len]);

    let mut current_page = first_overflow;
    let mut bytes_remaining = target_size.saturating_sub(local_copy_len);
    let mut state = OverflowReadState::new(usable_size, total_size - local_copy_len);

    while bytes_remaining > 0 {
        state.visit(current_page)?;
        let page_data = read_page(current_page)?;
        let page_bytes = page_data.as_ref();
        let (next_raw, available) = state.inspect(page_bytes)?;
        let to_read = bytes_remaining.min(available);

        out.extend_from_slice(&page_bytes[4..4 + to_read]);
        bytes_remaining -= to_read;

        if bytes_remaining > 0 {
            current_page =
                PageNumber::new(next_raw).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "unexpected end of overflow chain".to_owned(),
                })?;
        }
    }

    instrumentation::record_overflow_chain_reassembly(
        local_copy_len,
        target_size.saturating_sub(local_copy_len),
        state.visited.len(),
    );

    Ok(())
}

/// Read a complete overflow-backed payload through an async page backend.
pub(crate) async fn read_overflow_chain_async<R: PageReader>(
    cx: &Cx,
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    reader: &R,
) -> Result<Vec<u8>> {
    instrumentation::record_owned_payload_materialization(
        usize::try_from(total_payload_size).unwrap_or(usize::MAX),
    );
    let mut payload = Vec::new();
    read_overflow_chain_into_async(
        cx,
        local_data,
        first_overflow,
        total_payload_size,
        usable_size,
        reader,
        &mut payload,
    )
    .await?;
    Ok(payload)
}

/// Read a complete overflow-backed payload into a reusable buffer.
pub(crate) async fn read_overflow_chain_into_async<R: PageReader>(
    cx: &Cx,
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    reader: &R,
    out: &mut Vec<u8>,
) -> Result<()> {
    read_overflow_chain_prefix_into_async(
        cx,
        local_data,
        first_overflow,
        total_payload_size,
        usable_size,
        usize::try_from(total_payload_size).unwrap_or(usize::MAX),
        reader,
        out,
    )
    .await?;

    let total_size = usize::try_from(total_payload_size).unwrap_or(usize::MAX);
    if out.len() != total_size {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "read overflow chain size mismatch: expected {}, got {}",
                total_size,
                out.len()
            ),
        });
    }
    Ok(())
}

/// Read a prefix of an overflow-backed payload through an async page backend.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_overflow_chain_prefix_into_async<R: PageReader>(
    cx: &Cx,
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    max_prefix_bytes: usize,
    reader: &R,
    out: &mut Vec<u8>,
) -> Result<()> {
    out.clear();
    if total_payload_size > MAX_ALLOCATION_SIZE {
        return Err(FrankenError::TooBig);
    }
    if usable_size <= 4 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "invalid usable page size {} for overflow chain",
                usable_size
            ),
        });
    }

    let total_size = usize::try_from(total_payload_size).unwrap_or(usize::MAX);
    let target_size = total_size.min(max_prefix_bytes);
    if target_size == 0 {
        return Ok(());
    }
    let local_copy_len = local_data.len().min(target_size);
    out.try_reserve(target_size)
        .map_err(|_| FrankenError::OutOfMemory)?;
    out.extend_from_slice(&local_data[..local_copy_len]);

    let mut current_page = first_overflow;
    let mut bytes_remaining = target_size.saturating_sub(local_copy_len);
    let mut state = OverflowReadState::new(usable_size, total_size - local_copy_len);

    while bytes_remaining > 0 {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        state.visit(current_page)?;
        let page_data = reader.read_page_data(cx, current_page).await?;
        let page_bytes = page_data.as_bytes();
        let (next_raw, available) = state.inspect(page_bytes)?;
        let to_read = bytes_remaining.min(available);
        out.extend_from_slice(&page_bytes[4..4 + to_read]);
        bytes_remaining -= to_read;

        if bytes_remaining > 0 {
            current_page =
                PageNumber::new(next_raw).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "unexpected end of overflow chain".to_owned(),
                })?;
        }
    }

    instrumentation::record_overflow_chain_reassembly(
        local_copy_len,
        target_size.saturating_sub(local_copy_len),
        state.visited.len(),
    );
    Ok(())
}

async fn free_allocated_pages_best_effort<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    pages: &[PageNumber],
) {
    let cleanup_cx = cx.create_child();
    let _cleanup_mask = cleanup_cx.masked();
    for &page_no in pages {
        let _ = writer.free_page(&cleanup_cx, page_no).await;
    }
}

/// Reserve all bookkeeping and the serialization buffer before allocating
/// database pages. A backend must not hand out the header or the same page
/// twice; otherwise writing the chain would overwrite unrelated data or
/// create a cycle. Only validated, distinct reservations enter `pages`, so
/// error cleanup never frees the header or frees a reservation twice.
#[derive(Debug)]
struct OverflowWriteState {
    pages: Vec<PageNumber>,
    seen: HashSet<PageNumber>,
    page_buf: PageData,
    bytes_per_page: usize,
    num_pages: usize,
}

impl OverflowWriteState {
    fn new(overflow_len: usize, usable_size: u32, full_page_size: u32) -> Result<Self> {
        if overflow_len == 0 {
            return Err(FrankenError::internal(
                "write_overflow_chain called with empty data",
            ));
        }
        if overflow_len > MAX_ALLOCATION_SIZE as usize {
            return Err(FrankenError::TooBig);
        }
        if usable_size <= 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("invalid usable page size {usable_size} for overflow chain"),
            });
        }
        if full_page_size < usable_size {
            return Err(FrankenError::internal(format!(
                "full_page_size ({full_page_size}) < usable_size ({usable_size})"
            )));
        }
        let bytes_per_page = (usable_size - 4) as usize;
        let num_pages = overflow_len.div_ceil(bytes_per_page);
        if num_pages > MAX_OVERFLOW_CHAIN {
            return Err(FrankenError::TooBig);
        }

        let mut pages = Vec::new();
        pages
            .try_reserve_exact(num_pages)
            .map_err(|_| FrankenError::OutOfMemory)?;
        let mut seen = HashSet::new();
        seen.try_reserve(num_pages)
            .map_err(|_| FrankenError::OutOfMemory)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(full_page_size as usize)
            .map_err(|_| FrankenError::OutOfMemory)?;
        bytes.resize(full_page_size as usize, 0);
        Ok(Self {
            pages,
            seen,
            page_buf: PageData::from_vec(bytes),
            bytes_per_page,
            num_pages,
        })
    }

    fn push_page(&mut self, page_no: PageNumber) -> Result<()> {
        if page_no.get() == 1 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "overflow allocator returned the database header page".to_owned(),
            });
        }
        if !self.seen.insert(page_no) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow allocator returned duplicate page {}",
                    page_no.get()
                ),
            });
        }
        self.pages.push(page_no);
        Ok(())
    }

    fn prepare_page(&mut self, index: usize, overflow_data: &[u8]) {
        let start = index * self.bytes_per_page;
        let end = start + self.bytes_per_page.min(overflow_data.len() - start);
        let chunk = &overflow_data[start..end];
        let next = self.pages.get(index + 1).map_or(0, |page| page.get());
        let bytes = self.page_buf.as_bytes_mut();
        bytes.fill(0);
        bytes[..4].copy_from_slice(&next.to_be_bytes());
        bytes[4..4 + chunk.len()].copy_from_slice(chunk);
    }
}

/// Write an overflow chain through an async page backend.
///
/// When driven to an error, including cooperative cancellation between page
/// allocations, release every known reservation under a masked cleanup
/// context. Cleanup is best-effort; the owning transaction must roll back on
/// error (and when a caller abandons this future without driving it to completion).
pub(crate) async fn write_overflow_chain_async<W: PageWriter>(
    cx: &Cx,
    overflow_data: &[u8],
    usable_size: u32,
    full_page_size: u32,
    writer: &mut W,
) -> Result<PageNumber> {
    cx.checkpoint().map_err(|_| FrankenError::Abort)?;
    let mut state = OverflowWriteState::new(overflow_data.len(), usable_size, full_page_size)?;
    // One error exit owns compensation for both phases. In particular, a
    // checkpoint failure during allocation must not bypass cleanup of the
    // pages successfully allocated in earlier iterations.
    let result: Result<PageNumber> = async {
        for _ in 0..state.num_pages {
            cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            let page_no = writer.allocate_page(cx).await?;
            state.push_page(page_no)?;
        }
        for index in 0..state.pages.len() {
            cx.checkpoint().map_err(|_| FrankenError::Abort)?;
            let page_no = state.pages[index];
            state.prepare_page(index, overflow_data);
            writer
                .write_page_data(cx, page_no, state.page_buf.clone())
                .await?;
        }
        Ok(state.pages[0])
    }
    .await;
    if result.is_err() {
        free_allocated_pages_best_effort(cx, writer, &state.pages).await;
    }
    result
}

/// Write a payload to an overflow chain, allocating pages as needed.
///
/// `overflow_data` is the portion of the payload that doesn't fit locally.
/// `usable_size` is the usable page size (page_size - reserved_bytes).
/// `full_page_size` is the on-disk page size. Overflow page buffers are
/// allocated at this size so stock SQLite sees correctly-sized pages.
/// `allocate_page` allocates a new page and returns its number.
/// `write_page` writes data to a given page number.
///
/// Returns the page number of the first overflow page.
/// The callbacks must belong to a transaction that rolls back on error: this
/// interface has no deallocation callback with which to compensate failures.
pub fn write_overflow_chain<A, W>(
    overflow_data: &[u8],
    usable_size: u32,
    full_page_size: u32,
    allocate_page: &mut A,
    write_page: &mut W,
) -> Result<PageNumber>
where
    A: FnMut() -> Result<PageNumber>,
    W: FnMut(PageNumber, &[u8]) -> Result<()>,
{
    let mut state = OverflowWriteState::new(overflow_data.len(), usable_size, full_page_size)?;
    for _ in 0..state.num_pages {
        state.push_page(allocate_page()?)?;
    }
    for index in 0..state.pages.len() {
        let page_no = state.pages[index];
        state.prepare_page(index, overflow_data);
        write_page(page_no, state.page_buf.as_bytes())?;
    }
    Ok(state.pages[0])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::WitnessKey;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::future::Future;

    fn run_async<F: Future>(future: F) -> F::Output {
        RuntimeBuilder::current_thread()
            .build()
            .expect("build overflow test runtime")
            .block_on(future)
    }

    #[derive(Debug)]
    struct TestPageStore {
        pages: HashMap<u32, Vec<u8>>,
        next_page: u32,
        reads: Cell<usize>,
    }

    impl TestPageStore {
        fn from_pages(pages: HashMap<u32, Vec<u8>>) -> Self {
            let next_page = pages
                .keys()
                .copied()
                .max()
                .unwrap_or(0)
                .saturating_add(1)
                .max(1);
            Self {
                pages,
                next_page,
                reads: Cell::new(0),
            }
        }

        fn allocating_from(next_page: u32) -> Self {
            Self {
                pages: HashMap::new(),
                next_page,
                reads: Cell::new(0),
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    impl PageReader for TestPageStore {
        fn read_page<'a>(
            &'a self,
            _cx: &'a Cx,
            page_no: PageNumber,
        ) -> impl Future<Output = Result<Vec<u8>>> + 'a {
            async move {
                self.reads.set(self.reads.get() + 1);
                self.pages
                    .get(&page_no.get())
                    .cloned()
                    .ok_or_else(|| FrankenError::internal("page not found"))
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    impl PageWriter for TestPageStore {
        fn write_page<'a>(
            &'a mut self,
            _cx: &'a Cx,
            page_no: PageNumber,
            data: &'a [u8],
        ) -> impl Future<Output = Result<()>> + 'a {
            async move {
                self.pages.insert(page_no.get(), data.to_vec());
                Ok(())
            }
        }

        fn allocate_page<'a>(
            &'a mut self,
            _cx: &'a Cx,
        ) -> impl Future<Output = Result<PageNumber>> + 'a {
            async move {
                let page_no = PageNumber::new(self.next_page).ok_or(FrankenError::DatabaseFull)?;
                self.next_page = self
                    .next_page
                    .checked_add(1)
                    .ok_or(FrankenError::DatabaseFull)?;
                self.pages.entry(page_no.get()).or_default();
                Ok(page_no)
            }
        }

        fn free_page<'a>(
            &'a mut self,
            _cx: &'a Cx,
            page_no: PageNumber,
        ) -> impl Future<Output = Result<()>> + 'a {
            async move {
                self.pages.remove(&page_no.get());
                Ok(())
            }
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Clone, Copy)]
    enum WriteFault {
        None,
        CancelAfterAllocation(usize),
        FailAllocation(usize),
        FailWrite(usize),
        DuplicateAllocation(usize),
        HeaderAllocation(usize),
    }

    #[derive(Debug)]
    struct FaultPageStore {
        store: TestPageStore,
        fault: WriteFault,
        allocations: usize,
        writes: usize,
        freed: Vec<PageNumber>,
    }

    impl FaultPageStore {
        fn new(fault: WriteFault) -> Self {
            Self {
                store: TestPageStore::allocating_from(5),
                fault,
                allocations: 0,
                writes: 0,
                freed: Vec::new(),
            }
        }
    }

    impl PageReader for FaultPageStore {
        fn read_page<'a>(
            &'a self,
            cx: &'a Cx,
            page_no: PageNumber,
        ) -> impl Future<Output = Result<Vec<u8>>> + 'a {
            self.store.read_page(cx, page_no)
        }
    }

    #[allow(clippy::manual_async_fn)]
    impl PageWriter for FaultPageStore {
        fn allocate_page<'a>(
            &'a mut self,
            cx: &'a Cx,
        ) -> impl Future<Output = Result<PageNumber>> + 'a {
            async move {
                let call = self.allocations;
                self.allocations += 1;
                match self.fault {
                    WriteFault::FailAllocation(index) if index == call => {
                        return Err(FrankenError::Busy);
                    }
                    WriteFault::DuplicateAllocation(index) if index == call => {
                        return Ok(PageNumber::new(5).unwrap());
                    }
                    WriteFault::HeaderAllocation(index) if index == call => {
                        return Ok(PageNumber::new(1).unwrap());
                    }
                    _ => {}
                }
                let page = self.store.allocate_page(cx).await?;
                if matches!(self.fault, WriteFault::CancelAfterAllocation(n) if n == call + 1) {
                    cx.cancel();
                }
                Ok(page)
            }
        }

        fn write_page<'a>(
            &'a mut self,
            cx: &'a Cx,
            page_no: PageNumber,
            data: &'a [u8],
        ) -> impl Future<Output = Result<()>> + 'a {
            async move {
                let call = self.writes;
                self.writes += 1;
                // Inject after mutation as well as after earlier successful
                // writes: an error does not imply the backend wrote no bytes.
                self.store.write_page(cx, page_no, data).await?;
                if matches!(self.fault, WriteFault::FailWrite(index) if index == call) {
                    return Err(FrankenError::Busy);
                }
                Ok(())
            }
        }

        fn free_page<'a>(
            &'a mut self,
            cx: &'a Cx,
            page_no: PageNumber,
        ) -> impl Future<Output = Result<()>> + 'a {
            async move {
                assert!(
                    cx.checkpoint().is_ok(),
                    "cleanup cancellation must be masked"
                );
                assert!(
                    !self.freed.contains(&page_no),
                    "double free during compensation"
                );
                assert_ne!(page_no.get(), 1, "must never free the database header");
                self.freed.push(page_no);
                self.store.free_page(cx, page_no).await
            }
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[test]
    fn overflow_write_cancellation_releases_each_allocated_prefix() {
        run_async(async {
            for allocated in 0..=4 {
                let cx = Cx::new();
                let mut store = FaultPageStore::new(WriteFault::CancelAfterAllocation(allocated));
                if allocated == 0 {
                    cx.cancel();
                }
                let result = write_overflow_chain_async(&cx, &[0x42; 50], 20, 32, &mut store).await;
                assert!(matches!(result, Err(FrankenError::Abort)));
                assert_eq!(store.allocations, allocated);
                assert_eq!(store.writes, 0);
                assert_eq!(store.freed.len(), allocated);
                assert!(store.store.pages.is_empty());
                assert!(
                    cx.checkpoint().is_err(),
                    "cleanup must not uncancel its parent"
                );
            }
        });
    }

    #[test]
    fn overflow_write_allocation_failures_release_only_owned_pages() {
        run_async(async {
            for index in 0..4 {
                let mut store = FaultPageStore::new(WriteFault::FailAllocation(index));
                let result =
                    write_overflow_chain_async(&Cx::new(), &[0x42; 50], 20, 32, &mut store).await;
                assert!(matches!(result, Err(FrankenError::Busy)));
                assert_eq!(store.allocations, index + 1);
                assert_eq!(store.writes, 0);
                assert_eq!(store.freed.len(), index);
                assert!(store.store.pages.is_empty());
            }
        });
    }

    #[test]
    fn overflow_write_failures_release_complete_reservation() {
        run_async(async {
            for index in 0..4 {
                let mut store = FaultPageStore::new(WriteFault::FailWrite(index));
                let result =
                    write_overflow_chain_async(&Cx::new(), &[0x42; 50], 20, 32, &mut store).await;
                assert!(matches!(result, Err(FrankenError::Busy)));
                assert_eq!(store.allocations, 4);
                assert_eq!(store.writes, index + 1);
                assert_eq!(store.freed.len(), 4);
                assert!(store.store.pages.is_empty());
            }
        });
    }

    #[test]
    fn overflow_write_rejects_aliases_before_any_page_write() {
        run_async(async {
            for index in 0..4 {
                let mut faults = vec![WriteFault::HeaderAllocation(index)];
                if index > 0 {
                    faults.push(WriteFault::DuplicateAllocation(index));
                }
                for fault in faults {
                    let mut store = FaultPageStore::new(fault);
                    let result =
                        write_overflow_chain_async(&Cx::new(), &[0x42; 50], 20, 32, &mut store)
                            .await;
                    assert!(matches!(result, Err(FrankenError::DatabaseCorrupt { .. })));
                    assert_eq!(store.allocations, index + 1);
                    assert_eq!(store.writes, 0);
                    assert_eq!(store.freed.len(), index);
                    assert!(store.store.pages.is_empty());
                }
            }
        });
    }

    #[test]
    fn overflow_write_preflight_rejects_invalid_sizes_without_backend_calls() {
        run_async(async {
            for (usable, full) in [(0, 16), (4, 16), (16, 8)] {
                let mut store = FaultPageStore::new(WriteFault::None);
                assert!(
                    write_overflow_chain_async(&Cx::new(), &[0x42; 50], usable, full, &mut store,)
                        .await
                        .is_err()
                );
                assert_eq!(store.allocations, 0);
                assert_eq!(store.writes, 0);
                assert!(store.freed.is_empty());
            }
            assert!(matches!(
                OverflowWriteState::new(MAX_OVERFLOW_CHAIN + 1, 5, 5),
                Err(FrankenError::TooBig)
            ));
            assert!(matches!(
                OverflowWriteState::new(MAX_ALLOCATION_SIZE as usize + 1, 4096, 4096),
                Err(FrankenError::TooBig)
            ));
        });
    }

    #[test]
    fn overflow_callback_writer_validates_reservations_and_roundtrips() {
        for candidates in [vec![1], vec![5, 1], vec![5, 5]] {
            let mut candidates = candidates.into_iter();
            let mut allocate = || Ok(PageNumber::new(candidates.next().unwrap()).unwrap());
            let mut writes = 0;
            let mut write = |_page: PageNumber, _bytes: &[u8]| {
                writes += 1;
                Ok(())
            };
            assert!(matches!(
                write_overflow_chain(&[0x42; 50], 20, 32, &mut allocate, &mut write),
                Err(FrankenError::DatabaseCorrupt { .. })
            ));
            assert_eq!(writes, 0);
        }

        let data: Vec<u8> = (0..25).collect();
        let mut next = 5;
        let mut allocate = || {
            let page = PageNumber::new(next).unwrap();
            next += 1;
            Ok(page)
        };
        let mut pages = HashMap::new();
        let mut write = |page: PageNumber, bytes: &[u8]| {
            pages.insert(page.get(), bytes.to_vec());
            Ok(())
        };
        let first = write_overflow_chain(&data, 16, 32, &mut allocate, &mut write).unwrap();
        assert_eq!(pages.len(), 3);
        assert!(
            pages
                .values()
                .all(|page| page[16..].iter().all(|&b| b == 0))
        );
        let mut read = |page: PageNumber| pages.get(&page.get()).cloned().ok_or(FrankenError::Busy);
        assert_eq!(
            read_overflow_chain(&[], first, 25, 16, &mut read).unwrap(),
            data
        );
    }

    fn linked_page(next: u32) -> Vec<u8> {
        let mut page = vec![b'x'; 16];
        page[..4].copy_from_slice(&next.to_be_bytes());
        page
    }

    async fn assert_invalid_overflow(
        pages: HashMap<u32, Vec<u8>>,
        first: u32,
        total: u32,
        prefix: usize,
        message: &str,
        expected_reads: usize,
    ) {
        let store = TestPageStore::from_pages(pages);
        let first = PageNumber::new(first).unwrap();
        let mut out = vec![0xEE; 32];
        let mut read_page = |page: PageNumber| {
            store.reads.set(store.reads.get() + 1);
            store
                .pages
                .get(&page.get())
                .cloned()
                .ok_or(FrankenError::Busy)
        };
        let error = read_overflow_chain_prefix_into(
            &[],
            first,
            total,
            16,
            prefix,
            &mut read_page,
            &mut out,
        )
        .unwrap_err();
        assert!(matches!(&error, FrankenError::DatabaseCorrupt { .. }));
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(store.reads.get(), expected_reads);

        store.reads.set(0);
        let error = read_overflow_chain_prefix_into_async(
            &Cx::new(),
            &[],
            first,
            total,
            16,
            prefix,
            &store,
            &mut out,
        )
        .await
        .unwrap_err();
        assert!(matches!(&error, FrankenError::DatabaseCorrupt { .. }));
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(store.reads.get(), expected_reads);
    }

    #[test]
    fn overflow_readers_reject_cycles_before_repeating_payload() {
        run_async(async {
            for prefix in [1, 12, 13, 100] {
                assert_invalid_overflow(
                    HashMap::from([(5, linked_page(5))]),
                    5,
                    100,
                    prefix,
                    "cycle in overflow chain",
                    1,
                )
                .await;
            }
            for (links, prefix, reads) in [
                (vec![(5, 6), (6, 5)], 24, 2),
                (vec![(5, 6), (6, 7), (7, 6)], 36, 3),
            ] {
                let pages = links
                    .into_iter()
                    .map(|(page, next)| (page, linked_page(next)))
                    .collect();
                assert_invalid_overflow(pages, 5, 100, prefix, "cycle in overflow chain", reads)
                    .await;
            }
        });
    }

    #[test]
    fn overflow_readers_reject_cycles_after_the_visited_set_spills() {
        run_async(async {
            // Pages 5..=16 chain in order, then 16 links back to 10: the
            // repeat is seen only after the inline visited array spilled.
            let mut pages: HashMap<u32, Vec<u8>> =
                (5..16).map(|page| (page, linked_page(page + 1))).collect();
            pages.insert(16, linked_page(10));
            assert_invalid_overflow(pages, 5, 1000, 1000, "cycle in overflow chain", 12).await;
        });
    }

    #[test]
    fn visited_pages_detects_repeats_across_the_inline_spill() {
        let page = |n: u32| PageNumber::new(n).expect("nonzero page");
        let mut visited = VisitedPages::default();
        for n in 2..(2 + VISITED_INLINE_PAGES as u32 + 4) {
            assert!(
                visited.insert(page(n)).expect("insert"),
                "first visit of {n}"
            );
        }
        assert_eq!(visited.len(), VISITED_INLINE_PAGES + 4);
        for n in 2..(2 + VISITED_INLINE_PAGES as u32 + 4) {
            assert!(visited.contains(page(n)));
            assert!(!visited.insert(page(n)).expect("insert"), "repeat of {n}");
        }
        assert!(!visited.contains(page(1000)));
    }

    #[test]
    fn overflow_readers_reject_short_pages_even_when_prefix_fits() {
        run_async(async {
            for size in [0, 1, 4, 5, 15] {
                let mut page = linked_page(0);
                page.truncate(size);
                assert_invalid_overflow(
                    HashMap::from([(5, page)]),
                    5,
                    1,
                    1,
                    "overflow page too small",
                    1,
                )
                .await;
            }
        });
    }

    #[test]
    fn overflow_readers_validate_observed_termination_against_full_payload() {
        run_async(async {
            for prefix in [1, 12, 13, 100] {
                // A short prefix does not justify accepting an observed link
                // that already proves the complete payload is truncated.
                assert_invalid_overflow(
                    HashMap::from([(5, linked_page(0))]),
                    5,
                    13,
                    prefix,
                    "unexpected end of overflow chain",
                    1,
                )
                .await;
                assert_invalid_overflow(
                    HashMap::from([(5, linked_page(6))]),
                    5,
                    12,
                    prefix,
                    "continues beyond the declared payload",
                    1,
                )
                .await;
            }
        });
    }

    #[test]
    fn overflow_readers_never_read_database_header_as_payload() {
        run_async(async {
            assert_invalid_overflow(HashMap::new(), 1, 12, 12, "header page", 0).await;
            assert_invalid_overflow(
                HashMap::from([(5, linked_page(1))]),
                5,
                13,
                13,
                "header page",
                1,
            )
            .await;
        });
    }

    #[test]
    fn overflow_readers_reject_invalid_next_page_even_when_prefix_fits() {
        run_async(async {
            for prefix in [1, 12, 13, 100] {
                assert_invalid_overflow(
                    HashMap::from([(5, linked_page(u32::MAX))]),
                    5,
                    13,
                    prefix,
                    "invalid next overflow page number",
                    1,
                )
                .await;
            }
            // Exercise the same defect after a valid first link, including
            // prefixes that consume only part of the second page's payload.
            for prefix in [13, 24, 25, 100] {
                assert_invalid_overflow(
                    HashMap::from([(5, linked_page(6)), (6, linked_page(u32::MAX))]),
                    5,
                    25,
                    prefix,
                    "invalid next overflow page number",
                    2,
                )
                .await;
            }
        });
    }

    #[test]
    fn overflow_readers_accept_largest_valid_next_page() {
        run_async(async {
            let last = u32::MAX - 1;
            let store = TestPageStore::from_pages(HashMap::from([
                (5, linked_page(last)),
                (last, linked_page(0)),
            ]));
            let first = PageNumber::new(5).unwrap();
            let cx = Cx::new();
            let mut out = Vec::new();
            for prefix in [1_usize, 12, 13, 100] {
                let count = prefix.min(13);
                let expected_reads = count.div_ceil(12);
                store.reads.set(0);
                let mut read_page = |page: PageNumber| {
                    store.reads.set(store.reads.get() + 1);
                    store
                        .pages
                        .get(&page.get())
                        .cloned()
                        .ok_or(FrankenError::Busy)
                };
                read_overflow_chain_prefix_into(
                    &[],
                    first,
                    13,
                    16,
                    prefix,
                    &mut read_page,
                    &mut out,
                )
                .unwrap();
                assert_eq!(out, vec![b'x'; count]);
                assert_eq!(store.reads.get(), expected_reads);

                store.reads.set(0);
                read_overflow_chain_prefix_into_async(
                    &cx,
                    &[],
                    first,
                    13,
                    16,
                    prefix,
                    &store,
                    &mut out,
                )
                .await
                .unwrap();
                assert_eq!(out, vec![b'x'; count]);
                assert_eq!(store.reads.get(), expected_reads);
            }
        });
    }

    #[test]
    fn overflow_prefixes_keep_lazy_reads_and_exclude_reserved_bytes() {
        run_async(async {
            let mut first_page = linked_page(6);
            first_page.extend_from_slice(&[0xEE; 8]);
            let mut last_page = linked_page(0);
            last_page[4] = b'y';
            last_page.extend_from_slice(&[0xEE; 8]);
            let store = TestPageStore::from_pages(HashMap::from([(5, first_page), (6, last_page)]));
            let mut expected = b"Lxxxxxxxxxxxxy".to_vec();
            assert_eq!(expected.len(), 14);
            let first = PageNumber::new(5).unwrap();
            let cx = Cx::new();
            let mut out = Vec::new();
            for prefix in [0, 1, 2, 13, 14, usize::MAX] {
                let count = prefix.min(expected.len());
                let reads = count.saturating_sub(1).div_ceil(12);
                store.reads.set(0);
                let mut read_page = |page: PageNumber| {
                    store.reads.set(store.reads.get() + 1);
                    store
                        .pages
                        .get(&page.get())
                        .cloned()
                        .ok_or(FrankenError::Busy)
                };
                read_overflow_chain_prefix_into(
                    b"L",
                    first,
                    14,
                    16,
                    prefix,
                    &mut read_page,
                    &mut out,
                )
                .unwrap();
                assert_eq!(out, expected[..count]);
                assert_eq!(store.reads.get(), reads);
                store.reads.set(0);
                read_overflow_chain_prefix_into_async(
                    &cx, b"L", first, 14, 16, prefix, &store, &mut out,
                )
                .await
                .unwrap();
                assert_eq!(out, expected[..count]);
                assert_eq!(store.reads.get(), reads);
            }
            // Full-read wrappers use the same validation, not a separate path.
            assert_eq!(
                read_overflow_chain_async(&cx, b"L", first, 14, 16, &store)
                    .await
                    .unwrap(),
                expected
            );
            expected.clear();
            let mut read_page = |page: PageNumber| {
                store
                    .pages
                    .get(&page.get())
                    .cloned()
                    .ok_or(FrankenError::Busy)
            };
            read_overflow_chain_into(b"L", first, 14, 16, &mut read_page, &mut expected).unwrap();
            assert_eq!(expected.as_slice(), b"Lxxxxxxxxxxxxy");
        });
    }

    #[test]
    fn overflow_readers_propagate_backend_errors() {
        run_async(async {
            let store = TestPageStore::from_pages(HashMap::from([(5, linked_page(6))]));
            let first = PageNumber::new(5).unwrap();
            let mut read_page = |page: PageNumber| {
                store
                    .pages
                    .get(&page.get())
                    .cloned()
                    .ok_or(FrankenError::Busy)
            };
            assert!(matches!(
                read_overflow_chain(&[], first, 13, 16, &mut read_page),
                Err(FrankenError::Busy)
            ));
            // The async fixture reports a missing page as Internal, which must
            // likewise propagate rather than return a shortened payload.
            let error = read_overflow_chain_async(&Cx::new(), &[], first, 13, 16, &store)
                .await
                .unwrap_err();
            assert!(matches!(error, FrankenError::Internal(_)));
        });
    }

    #[test]
    fn test_read_overflow_single_page() {
        run_async(async {
            let usable = 4096u32;
            let local_data = b"local";
            let overflow_data = b"overflow";
            let total_size = (local_data.len() + overflow_data.len()) as u32;

            // Build a single overflow page.
            let mut overflow_page = vec![0u8; usable as usize];
            overflow_page[0..4].copy_from_slice(&0u32.to_be_bytes()); // No next page.
            overflow_page[4..4 + overflow_data.len()].copy_from_slice(overflow_data);

            let first_overflow = PageNumber::new(5).unwrap();
            let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
            pages.insert(5, overflow_page);
            let store = TestPageStore::from_pages(pages);
            let cx = Cx::new();

            let result = read_overflow_chain_async(
                &cx,
                local_data,
                first_overflow,
                total_size,
                usable,
                &store,
            )
            .await
            .unwrap();

            assert_eq!(&result[..5], b"local");
            assert_eq!(&result[5..], b"overflow");
        });
    }

    #[test]
    fn test_read_overflow_multi_page() {
        run_async(async {
            let usable = 20u32; // Small page for testing: 16 bytes of data per overflow page.
            let local_data = b"L";
            let overflow_bytes: Vec<u8> = (0..40).collect(); // 40 bytes of overflow → 3 pages.
            let total_size = (1 + 40) as u32;

            let bytes_per_page = (usable - 4) as usize; // 16
            let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();

            // Page 10: first 16 bytes, next = 11
            let mut p10 = vec![0u8; usable as usize];
            p10[0..4].copy_from_slice(&11u32.to_be_bytes());
            p10[4..4 + bytes_per_page].copy_from_slice(&overflow_bytes[0..16]);
            pages.insert(10, p10);

            // Page 11: next 16 bytes, next = 12
            let mut p11 = vec![0u8; usable as usize];
            p11[0..4].copy_from_slice(&12u32.to_be_bytes());
            p11[4..4 + bytes_per_page].copy_from_slice(&overflow_bytes[16..32]);
            pages.insert(11, p11);

            // Page 12: last 8 bytes, next = 0
            let mut p12 = vec![0u8; usable as usize];
            p12[0..4].copy_from_slice(&0u32.to_be_bytes());
            p12[4..4 + 8].copy_from_slice(&overflow_bytes[32..40]);
            pages.insert(12, p12);
            let store = TestPageStore::from_pages(pages);
            let cx = Cx::new();

            let result = read_overflow_chain_async(
                &cx,
                local_data,
                PageNumber::new(10).unwrap(),
                total_size,
                usable,
                &store,
            )
            .await
            .unwrap();

            assert_eq!(result.len(), 41);
            assert_eq!(result[0], b'L');
            assert_eq!(&result[1..], &overflow_bytes[..]);
        });
    }

    #[test]
    fn test_write_overflow_chain_single_page() {
        run_async(async {
            let usable = 4096u32;
            let overflow_data = b"hello overflow world";
            let mut store = TestPageStore::allocating_from(10);
            let cx = Cx::new();

            let first = write_overflow_chain_async(&cx, overflow_data, usable, usable, &mut store)
                .await
                .unwrap();

            assert_eq!(first.get(), 10);
            assert_eq!(store.pages.len(), 1);

            // Verify the page content.
            let page = &store.pages[&10];
            assert_eq!(u32::from_be_bytes([page[0], page[1], page[2], page[3]]), 0); // No next.
            assert_eq!(&page[4..4 + overflow_data.len()], overflow_data);
        });
    }

    #[test]
    fn test_write_read_overflow_roundtrip() {
        run_async(async {
            let usable = 20u32; // Small pages for testing.
            let overflow_data: Vec<u8> = (0..50).collect();
            let mut store = TestPageStore::allocating_from(100);
            let cx = Cx::new();

            let first = write_overflow_chain_async(&cx, &overflow_data, usable, usable, &mut store)
                .await
                .unwrap();

            // Read it back.
            let local_data = b"prefix";
            let total_size = (local_data.len() + overflow_data.len()) as u32;
            let result =
                read_overflow_chain_async(&cx, local_data, first, total_size, usable, &store)
                    .await
                    .unwrap();

            assert_eq!(&result[..6], b"prefix");
            assert_eq!(&result[6..], &overflow_data[..]);
        });
    }

    #[test]
    fn test_write_overflow_chain_multi_page_structure() {
        run_async(async {
            // usable=20 -> 16 data bytes per page. 50 bytes needs ceil(50/16) = 4
            // pages. The roundtrip test checks reassembled data; this pins the chain
            // structure the writer builds (page count, next pointers, split, tail).
            let usable = 20u32;
            let data: Vec<u8> = (0u8..50).collect();
            let mut store = TestPageStore::allocating_from(100);
            let cx = Cx::new();

            let first = write_overflow_chain_async(&cx, &data, usable, usable, &mut store)
                .await
                .unwrap();

            // Exactly 4 pages allocated (100..=103); the head is the first page.
            assert_eq!(first.get(), 100);
            assert_eq!(store.pages.len(), 4);

            let next_of = |p: u32| {
                u32::from_be_bytes([
                    store.pages[&p][0],
                    store.pages[&p][1],
                    store.pages[&p][2],
                    store.pages[&p][3],
                ])
            };
            // Chain linkage: 100 -> 101 -> 102 -> 103 -> 0.
            assert_eq!(next_of(100), 101);
            assert_eq!(next_of(101), 102);
            assert_eq!(next_of(102), 103);
            assert_eq!(next_of(103), 0, "the last page terminates the chain");

            // Data is split 16/16/16/2 and reassembles to the original.
            let mut reassembled = Vec::new();
            for (p, len) in [(100u32, 16usize), (101, 16), (102, 16), (103, 2)] {
                reassembled.extend_from_slice(&store.pages[&p][4..4 + len]);
            }
            assert_eq!(reassembled, data);

            // The last page's unused tail (after the 4-byte header + 2 data bytes) is
            // zero-padded out to the full page size.
            let last = &store.pages[&103];
            assert_eq!(last.len(), 20);
            assert!(
                last[6..].iter().all(|&b| b == 0),
                "the tail must be zero-padded"
            );
        });
    }

    #[test]
    fn test_overflow_chain_premature_end() {
        run_async(async {
            // Use small pages so one overflow page can't satisfy the full payload.
            let usable = 20u32; // 16 bytes of data per overflow page.
            let local_data = b"L";
            // Claim 50 bytes total = 1 local + 49 overflow.
            // One overflow page holds 16 bytes. Chain ends after 1 page → only 17 bytes.
            let total_size = 50u32;

            let mut overflow_page = vec![0u8; usable as usize];
            overflow_page[0..4].copy_from_slice(&0u32.to_be_bytes()); // No next page.
            for i in 0..16 {
                overflow_page[4 + i] = i as u8;
            }

            let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
            pages.insert(5, overflow_page);
            let store = TestPageStore::from_pages(pages);
            let cx = Cx::new();

            let result = read_overflow_chain_async(
                &cx,
                local_data,
                PageNumber::new(5).unwrap(),
                total_size,
                usable,
                &store,
            )
            .await;
            // Chain ends (next = 0) but we only have 1 + 16 = 17 bytes, need 50.
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("unexpected end of overflow chain")
            );
        });
    }

    #[test]
    fn test_write_overflow_empty_data_errors() {
        run_async(async {
            let cx = Cx::new();
            let mut store = TestPageStore::allocating_from(1);
            let result = write_overflow_chain_async(&cx, &[], 4096, 4096, &mut store).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_read_overflow_too_big() {
        run_async(async {
            let usable = 4096u32;
            let local_data = b"local";
            // MAX_ALLOCATION_SIZE + 1
            let total_size = MAX_ALLOCATION_SIZE.saturating_add(1);
            let store = TestPageStore::allocating_from(1);
            let cx = Cx::new();

            let result = read_overflow_chain_async(
                &cx,
                local_data,
                PageNumber::new(5).unwrap(),
                total_size,
                usable,
                &store,
            )
            .await;

            assert!(matches!(result, Err(FrankenError::TooBig)));
        });
    }

    #[test]
    fn test_read_overflow_chain_prefix_reads_only_needed_pages() {
        run_async(async {
            // usable=16 -> 12 payload bytes per overflow page. Local holds 5 bytes,
            // then a 3-page chain (10 -> 11 -> 12) holds 36 more = 41 total. Payload
            // bytes are the sequence 0..41 so positions are self-checking.
            let usable = 16u32;
            let local: Vec<u8> = (0u8..5).collect();
            let mk_page = |next: u32, data: &[u8]| {
                let mut p = next.to_be_bytes().to_vec();
                p.extend_from_slice(data);
                p
            };
            let mut store: HashMap<u32, Vec<u8>> = HashMap::new();
            store.insert(10, mk_page(11, &(5u8..17).collect::<Vec<u8>>()));
            store.insert(11, mk_page(12, &(17u8..29).collect::<Vec<u8>>()));
            store.insert(12, mk_page(0, &(29u8..41).collect::<Vec<u8>>()));
            let store = TestPageStore::from_pages(store);
            let cx = Cx::new();
            let pg10 = PageNumber::new(10).unwrap();
            let full: Vec<u8> = (0u8..41).collect();

            // Full read reassembles the whole payload, visiting all 3 overflow pages.
            store.reads.set(0);
            let got = read_overflow_chain_async(&cx, &local, pg10, 41, usable, &store)
                .await
                .unwrap();
            assert_eq!(got, full);
            assert_eq!(store.reads.get(), 3, "full read visits the entire chain");

            // A prefix ending inside the first overflow page reads ONLY that page.
            store.reads.set(0);
            let mut out = Vec::new();
            read_overflow_chain_prefix_into_async(
                &cx, &local, pg10, 41, usable, 10, &store, &mut out,
            )
            .await
            .unwrap();
            assert_eq!(out, (0u8..10).collect::<Vec<u8>>());
            assert_eq!(
                store.reads.get(),
                1,
                "prefix should stop after the first overflow page"
            );

            // A prefix wholly within the local data reads no overflow pages at all.
            store.reads.set(0);
            out.clear();
            read_overflow_chain_prefix_into_async(
                &cx, &local, pg10, 41, usable, 3, &store, &mut out,
            )
            .await
            .unwrap();
            assert_eq!(out, vec![0u8, 1, 2]);
            assert_eq!(
                store.reads.get(),
                0,
                "local-only prefix touches no overflow pages"
            );

            // A zero-length prefix yields an empty result and reads nothing.
            store.reads.set(0);
            out.clear();
            read_overflow_chain_prefix_into_async(
                &cx, &local, pg10, 41, usable, 0, &store, &mut out,
            )
            .await
            .unwrap();
            assert!(out.is_empty());
            assert_eq!(store.reads.get(), 0);
        });
    }

    #[test]
    fn test_read_overflow_chain_prefix_boundary_cases() {
        run_async(async {
            // Same geometry as the "reads only needed pages" test: usable=16 -> 12
            // payload bytes per overflow page, 5 local bytes, then chain 10->11->12
            // holding 36 more = 41 total, payload = the byte sequence 0..41. This
            // pins the two off-by-one seams the sibling test does not exercise:
            // a prefix ending exactly at the local boundary, and a prefix ending
            // exactly at an overflow-page boundary (the `if bytes_remaining > 0`
            // guard that must NOT read the following page), plus the clamp that
            // caps max_prefix_bytes at the total payload size.
            let usable = 16u32;
            let local: Vec<u8> = (0u8..5).collect();
            let mk_page = |next: u32, data: &[u8]| {
                let mut p = next.to_be_bytes().to_vec();
                p.extend_from_slice(data);
                p
            };
            let mut store: HashMap<u32, Vec<u8>> = HashMap::new();
            store.insert(10, mk_page(11, &(5u8..17).collect::<Vec<u8>>()));
            store.insert(11, mk_page(12, &(17u8..29).collect::<Vec<u8>>()));
            store.insert(12, mk_page(0, &(29u8..41).collect::<Vec<u8>>()));
            let store = TestPageStore::from_pages(store);
            let cx = Cx::new();
            let pg10 = PageNumber::new(10).unwrap();

            // Prefix exactly equal to the local byte count: satisfied entirely from
            // local data, so no overflow page is touched.
            store.reads.set(0);
            let mut out = Vec::new();
            read_overflow_chain_prefix_into_async(
                &cx, &local, pg10, 41, usable, 5, &store, &mut out,
            )
            .await
            .unwrap();
            assert_eq!(out, (0u8..5).collect::<Vec<u8>>());
            assert_eq!(
                store.reads.get(),
                0,
                "prefix at the local boundary reads no overflow pages"
            );

            // Prefix ending EXACTLY at the end of the first overflow page (5 local +
            // 12 = 17): the chain pointer to page 11 must NOT be followed, so only
            // page 10 is read even though a valid next page exists.
            store.reads.set(0);
            out.clear();
            read_overflow_chain_prefix_into_async(
                &cx, &local, pg10, 41, usable, 17, &store, &mut out,
            )
            .await
            .unwrap();
            assert_eq!(out, (0u8..17).collect::<Vec<u8>>());
            assert_eq!(
                store.reads.get(),
                1,
                "prefix ending at a page boundary must not read the following page"
            );

            // max_prefix_bytes larger than the total payload clamps to the total,
            // reassembling the whole chain (all three overflow pages).
            store.reads.set(0);
            out.clear();
            read_overflow_chain_prefix_into_async(
                &cx, &local, pg10, 41, usable, 1000, &store, &mut out,
            )
            .await
            .unwrap();
            assert_eq!(out, (0u8..41).collect::<Vec<u8>>());
            assert_eq!(
                store.reads.get(),
                3,
                "an over-large prefix clamps to the full payload"
            );
        });
    }
}
