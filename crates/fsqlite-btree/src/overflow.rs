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

/// Maximum number of overflow pages in a chain (safety bound to prevent
/// infinite loops on corrupt databases).
pub const MAX_OVERFLOW_CHAIN: usize = 1_000_000;

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

    out.reserve(target_size);
    out.extend_from_slice(&local_data[..local_copy_len]);

    let mut current_page = first_overflow;
    let mut bytes_remaining = target_size.saturating_sub(local_copy_len);
    let bytes_per_overflow = usable_size.saturating_sub(4) as usize;
    let mut chain_length = 0;

    while bytes_remaining > 0 {
        chain_length += 1;
        if chain_length > MAX_OVERFLOW_CHAIN {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow chain exceeds maximum length of {}",
                    MAX_OVERFLOW_CHAIN
                ),
            });
        }

        let page_data = read_page(current_page)?;
        let page_bytes = page_data.as_ref();
        if page_bytes.len() <= 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "overflow page too small or empty".to_owned(),
            });
        }

        let next_raw =
            u32::from_be_bytes([page_bytes[0], page_bytes[1], page_bytes[2], page_bytes[3]]);

        let available = page_bytes.len().saturating_sub(4).min(bytes_per_overflow);
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
        chain_length,
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
    out.reserve(target_size);
    out.extend_from_slice(&local_data[..local_copy_len]);

    let mut current_page = first_overflow;
    let mut bytes_remaining = target_size.saturating_sub(local_copy_len);
    let bytes_per_overflow = usable_size.saturating_sub(4) as usize;
    let mut chain_length = 0;

    while bytes_remaining > 0 {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        chain_length += 1;
        if chain_length > MAX_OVERFLOW_CHAIN {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow chain exceeds maximum length of {}",
                    MAX_OVERFLOW_CHAIN
                ),
            });
        }

        let page_data = reader.read_page_data(cx, current_page).await?;
        let page_bytes = page_data.as_bytes();
        if page_bytes.len() <= 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "overflow page too small or empty".to_owned(),
            });
        }

        let next_raw =
            u32::from_be_bytes([page_bytes[0], page_bytes[1], page_bytes[2], page_bytes[3]]);
        let available = page_bytes.len().saturating_sub(4).min(bytes_per_overflow);
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
        chain_length,
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

/// Write an overflow chain through an async page backend.
pub(crate) async fn write_overflow_chain_async<W: PageWriter>(
    cx: &Cx,
    overflow_data: &[u8],
    usable_size: u32,
    full_page_size: u32,
    writer: &mut W,
) -> Result<PageNumber> {
    if overflow_data.is_empty() {
        return Err(FrankenError::internal(
            "write_overflow_chain_async called with empty data",
        ));
    }
    if usable_size <= 4 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "invalid usable page size {} for overflow chain",
                usable_size
            ),
        });
    }

    let bytes_per_page = usable_size.saturating_sub(4) as usize;
    if bytes_per_page == 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "usable page size too small for overflow data".to_owned(),
        });
    }
    if full_page_size < usable_size {
        return Err(FrankenError::internal(format!(
            "full_page_size ({full_page_size}) < usable_size ({usable_size})"
        )));
    }
    let page_size = full_page_size as usize;
    let num_pages = overflow_data.len().div_ceil(bytes_per_page);
    if num_pages > MAX_OVERFLOW_CHAIN {
        return Err(FrankenError::TooBig);
    }

    let mut pages = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        match writer.allocate_page(cx).await {
            Ok(page_no) => pages.push(page_no),
            Err(error) => {
                free_allocated_pages_best_effort(cx, writer, &pages).await;
                return Err(error);
            }
        }
    }

    let mut page_buf = PageData::from_vec(vec![0; page_size]);
    for (index, &page_no) in pages.iter().enumerate() {
        if let Err(error) = cx.checkpoint().map_err(|_| FrankenError::Abort) {
            free_allocated_pages_best_effort(cx, writer, &pages).await;
            return Err(error);
        }
        let data_start = index * bytes_per_page;
        let data_end = ((index + 1) * bytes_per_page).min(overflow_data.len());
        let chunk = &overflow_data[data_start..data_end];
        let next_page = pages.get(index + 1).map_or(0, |next| next.get());

        let page_bytes = page_buf.as_bytes_mut();
        page_bytes.fill(0);
        page_bytes[0..4].copy_from_slice(&next_page.to_be_bytes());
        page_bytes[4..4 + chunk.len()].copy_from_slice(chunk);
        if let Err(error) = writer.write_page_data(cx, page_no, page_buf.clone()).await {
            free_allocated_pages_best_effort(cx, writer, &pages).await;
            return Err(error);
        }
    }

    pages
        .first()
        .copied()
        .ok_or_else(|| FrankenError::internal("overflow allocation produced no pages"))
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
    if overflow_data.is_empty() {
        return Err(FrankenError::internal(
            "write_overflow_chain called with empty data",
        ));
    }
    if usable_size <= 4 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "invalid usable page size {} for overflow chain",
                usable_size
            ),
        });
    }

    let bytes_per_page = usable_size.saturating_sub(4) as usize;
    if bytes_per_page == 0 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "usable page size too small for overflow data".to_owned(),
        });
    }
    if full_page_size < usable_size {
        return Err(FrankenError::internal(format!(
            "full_page_size ({full_page_size}) < usable_size ({usable_size})"
        )));
    }
    let page_size = full_page_size as usize;

    // Calculate number of overflow pages needed.
    let num_pages = overflow_data.len().div_ceil(bytes_per_page);
    if num_pages > MAX_OVERFLOW_CHAIN {
        return Err(FrankenError::TooBig);
    }

    // Allocate all pages first so we know the chain.
    let mut pages = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        pages.push(allocate_page()?);
    }

    let mut page_buf = vec![0u8; page_size];
    // Write each page with its next pointer and data chunk.
    for (i, &pgno) in pages.iter().enumerate() {
        let data_start = i * bytes_per_page;
        let data_end = ((i + 1) * bytes_per_page).min(overflow_data.len());
        let chunk = &overflow_data[data_start..data_end];

        let next_pgno: u32 = if i + 1 < pages.len() {
            pages[i + 1].get()
        } else {
            0 // End of chain.
        };

        page_buf[0..4].copy_from_slice(&next_pgno.to_be_bytes());
        page_buf[4..4 + chunk.len()].copy_from_slice(chunk);
        if chunk.len() < bytes_per_page {
            // Ensure tail is zeroed if the chunk didn't fill the space.
            page_buf[4 + chunk.len()..].fill(0);
        }

        write_page(pgno, &page_buf)?;
    }

    Ok(pages[0])
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
