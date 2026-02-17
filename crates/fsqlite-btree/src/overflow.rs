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

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::limits::MAX_ALLOCATION_SIZE;
use tracing::warn;

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
pub fn read_overflow_chain<F>(
    local_data: &[u8],
    first_overflow: PageNumber,
    total_payload_size: u32,
    usable_size: u32,
    read_page: &mut F,
) -> Result<Vec<u8>>
where
    F: FnMut(PageNumber) -> Result<Vec<u8>>,
{
    if total_payload_size > MAX_ALLOCATION_SIZE {
        return Err(FrankenError::TooBig);
    }
    let total = total_payload_size as usize;
    let mut payload = Vec::with_capacity(total);
    payload.extend_from_slice(local_data);

    let bytes_per_overflow = (usable_size - 4) as usize;
    let mut next_pgno = Some(first_overflow);
    let mut previous_pgno: Option<PageNumber> = None;
    let mut pages_read = 0usize;

    while payload.len() < total {
        let Some(pgno) = next_pgno else {
            warn!(
                expected_bytes = total,
                got_bytes = payload.len(),
                last_page = previous_pgno.map(PageNumber::get),
                "overflow chain corruption detected: chain ended prematurely"
            );
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow chain ended prematurely: got {} of {} bytes",
                    payload.len(),
                    total
                ),
            });
        };

        pages_read += 1;
        if pages_read > MAX_OVERFLOW_CHAIN {
            warn!(
                page = pgno.get(),
                max_pages = MAX_OVERFLOW_CHAIN,
                "overflow chain corruption detected: chain length exceeds safety bound"
            );
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow chain exceeds {} pages (possible cycle)",
                    MAX_OVERFLOW_CHAIN
                ),
            });
        }

        let page_data = match read_page(pgno) {
            Ok(page_data) => page_data,
            Err(error) => {
                warn!(
                    page = pgno.get(),
                    error = %error,
                    "overflow chain corruption detected: failed to read overflow page"
                );
                return Err(error);
            }
        };
        if page_data.len() < 4 {
            warn!(
                page = pgno.get(),
                page_len = page_data.len(),
                "overflow chain corruption detected: page too small"
            );
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflow page {} is too small: {} bytes",
                    pgno,
                    page_data.len()
                ),
            });
        }

        // First 4 bytes: next overflow page number (0 = end of chain).
        let next_raw = u32::from_be_bytes([page_data[0], page_data[1], page_data[2], page_data[3]]);
        next_pgno = PageNumber::new(next_raw);
        previous_pgno = Some(pgno);

        // Remaining bytes: overflow payload data.
        let remaining_needed = total - payload.len();
        let available = page_data.len().saturating_sub(4).min(bytes_per_overflow);
        let to_copy = remaining_needed.min(available);
        payload.extend_from_slice(&page_data[4..4 + to_copy]);
    }

    Ok(payload)
}

/// Write a payload to an overflow chain, allocating pages as needed.
///
/// `overflow_data` is the portion of the payload that doesn't fit locally.
/// `usable_size` is the usable page size.
/// `allocate_page` allocates a new page and returns its number.
/// `write_page` writes data to a given page number.
///
/// Returns the page number of the first overflow page.
pub fn write_overflow_chain<A, W>(
    overflow_data: &[u8],
    usable_size: u32,
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

    let bytes_per_page = (usable_size - 4) as usize;
    let page_size = usable_size as usize;

    // Calculate number of overflow pages needed.
    let num_pages = overflow_data.len().div_ceil(bytes_per_page);

    // Allocate all pages first so we know the chain.
    let mut pages = Vec::with_capacity(num_pages);
    for _ in 0..num_pages {
        pages.push(allocate_page()?);
    }

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

        let mut page_buf = vec![0u8; page_size];
        page_buf[0..4].copy_from_slice(&next_pgno.to_be_bytes());
        page_buf[4..4 + chunk.len()].copy_from_slice(chunk);

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
    use std::collections::HashMap;

    #[test]
    fn test_read_overflow_single_page() {
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

        let result = read_overflow_chain(
            local_data,
            first_overflow,
            total_size,
            usable,
            &mut |pgno| {
                pages
                    .get(&pgno.get())
                    .cloned()
                    .ok_or_else(|| FrankenError::internal("page not found"))
            },
        )
        .unwrap();

        assert_eq!(&result[..5], b"local");
        assert_eq!(&result[5..], b"overflow");
    }

    #[test]
    fn test_read_overflow_multi_page() {
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

        let result = read_overflow_chain(
            local_data,
            PageNumber::new(10).unwrap(),
            total_size,
            usable,
            &mut |pgno| {
                pages
                    .get(&pgno.get())
                    .cloned()
                    .ok_or_else(|| FrankenError::internal("page not found"))
            },
        )
        .unwrap();

        assert_eq!(result.len(), 41);
        assert_eq!(result[0], b'L');
        assert_eq!(&result[1..], &overflow_bytes[..]);
    }

    #[test]
    fn test_write_overflow_chain_single_page() {
        let usable = 4096u32;
        let overflow_data = b"hello overflow world";
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut next_page = 10u32;

        let first = write_overflow_chain(
            overflow_data,
            usable,
            &mut || {
                let pgno = PageNumber::new(next_page).unwrap();
                next_page += 1;
                Ok(pgno)
            },
            &mut |pgno, data| {
                pages.insert(pgno.get(), data.to_vec());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(first.get(), 10);
        assert_eq!(pages.len(), 1);

        // Verify the page content.
        let page = &pages[&10];
        assert_eq!(u32::from_be_bytes([page[0], page[1], page[2], page[3]]), 0); // No next.
        assert_eq!(&page[4..4 + overflow_data.len()], overflow_data);
    }

    #[test]
    fn test_write_read_overflow_roundtrip() {
        let usable = 20u32; // Small pages for testing.
        let overflow_data: Vec<u8> = (0..50).collect();
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut next_page = 100u32;

        let first = write_overflow_chain(
            &overflow_data,
            usable,
            &mut || {
                let pgno = PageNumber::new(next_page).unwrap();
                next_page += 1;
                Ok(pgno)
            },
            &mut |pgno, data| {
                pages.insert(pgno.get(), data.to_vec());
                Ok(())
            },
        )
        .unwrap();

        // Read it back.
        let local_data = b"prefix";
        let total_size = (local_data.len() + overflow_data.len()) as u32;
        let result = read_overflow_chain(local_data, first, total_size, usable, &mut |pgno| {
            pages
                .get(&pgno.get())
                .cloned()
                .ok_or_else(|| FrankenError::internal("page not found"))
        })
        .unwrap();

        assert_eq!(&result[..6], b"prefix");
        assert_eq!(&result[6..], &overflow_data[..]);
    }

    #[test]
    fn test_overflow_chain_premature_end() {
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

        let result = read_overflow_chain(
            local_data,
            PageNumber::new(5).unwrap(),
            total_size,
            usable,
            &mut |pgno| {
                pages
                    .get(&pgno.get())
                    .cloned()
                    .ok_or_else(|| FrankenError::internal("page not found"))
            },
        );
        // Chain ends (next = 0) but we only have 1 + 16 = 17 bytes, need 50.
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("prematurely"));
    }

    #[test]
    fn test_write_overflow_empty_data_errors() {
        let result = write_overflow_chain(
            &[],
            4096,
            &mut || Ok(PageNumber::new(1).unwrap()),
            &mut |_, _| Ok(()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_read_overflow_too_big() {
        let usable = 4096u32;
        let local_data = b"local";
        // MAX_ALLOCATION_SIZE + 1
        let total_size = MAX_ALLOCATION_SIZE.saturating_add(1);

        let result = read_overflow_chain(
            local_data,
            PageNumber::new(5).unwrap(),
            total_size,
            usable,
            &mut |_| Ok(vec![]),
        );

        assert!(matches!(result, Err(FrankenError::TooBig)));
    }
}
