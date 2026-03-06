//! B-tree page and cell parsing (§11, bd-2kvo).
//!
//! This module handles:
//!
//! - [`BtreePageType`]: The four page types (interior/leaf table/index).
//! - [`BtreePageHeader`]: Parsing the page header from raw bytes.
//! - [`CellRef`]: A parsed reference to a single cell on a page.
//! - Local payload calculation and overflow detection.
//!
//! # Page Layout (from SQLite file format)
//!
//! ```text
//! ┌──────────────────────────┐
//! │ Page header (8 or 12 B)  │  (12 for interior, 8 for leaf)
//! ├──────────────────────────┤
//! │ Cell pointer array       │  (2 bytes per cell, ascending offsets)
//! ├──────────────────────────┤
//! │ Unallocated space        │
//! ├──────────────────────────┤
//! │ Cell content area        │  (grows downward from end of page)
//! ├──────────────────────────┤
//! │ Reserved region          │  (0 bytes by default)
//! └──────────────────────────┘
//! ```

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::limits::{
    BTREE_INTERIOR_HEADER_SIZE, BTREE_LEAF_HEADER_SIZE, CELL_POINTER_SIZE, DB_HEADER_SIZE,
};
use fsqlite_types::serial_type::read_varint;
use tracing::debug;

// ---------------------------------------------------------------------------
// Page type
// ---------------------------------------------------------------------------

/// The four B-tree page types, identified by the flag byte at offset 0
/// of the page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BtreePageType {
    /// Interior index page (0x02): index keys + child page pointers.
    InteriorIndex = 0x02,
    /// Interior table page (0x05): rowid keys + child page pointers.
    InteriorTable = 0x05,
    /// Leaf index page (0x0A): index keys only.
    LeafIndex = 0x0A,
    /// Leaf table page (0x0D): rowid keys + record payloads.
    LeafTable = 0x0D,
}

impl BtreePageType {
    /// Parse a page type from the flag byte.
    pub const fn from_flag(flag: u8) -> Option<Self> {
        match flag {
            0x02 => Some(Self::InteriorIndex),
            0x05 => Some(Self::InteriorTable),
            0x0A => Some(Self::LeafIndex),
            0x0D => Some(Self::LeafTable),
            _ => None,
        }
    }

    /// Whether this is an interior (non-leaf) page.
    #[must_use]
    pub const fn is_interior(self) -> bool {
        matches!(self, Self::InteriorIndex | Self::InteriorTable)
    }

    /// Whether this is a leaf page.
    #[must_use]
    pub const fn is_leaf(self) -> bool {
        !self.is_interior()
    }

    /// Whether this is a table (intkey) page.
    #[must_use]
    pub const fn is_table(self) -> bool {
        matches!(self, Self::InteriorTable | Self::LeafTable)
    }

    /// Whether this is an index (blobkey) page.
    #[must_use]
    pub const fn is_index(self) -> bool {
        !self.is_table()
    }

    /// Size of the page header for this type.
    #[must_use]
    pub const fn header_size(self) -> u8 {
        if self.is_interior() {
            BTREE_INTERIOR_HEADER_SIZE
        } else {
            BTREE_LEAF_HEADER_SIZE
        }
    }
}

// ---------------------------------------------------------------------------
// Page header
// ---------------------------------------------------------------------------

/// Parsed B-tree page header.
///
/// All multi-byte integers in the header are big-endian per the SQLite
/// file format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BtreePageHeader {
    /// Page type (interior/leaf, table/index).
    pub page_type: BtreePageType,
    /// Byte offset of the first freeblock on the page (0 = no freeblocks).
    pub first_freeblock: u16,
    /// Number of cells on the page.
    pub cell_count: u16,
    /// Byte offset of the first byte of the cell content area.
    /// A value of 0 means 65536.
    /// The raw value from the header. Use `content_offset()` for the usable value.
    pub cell_content_offset: u32,
    /// Number of fragmented free bytes in the cell content area.
    pub fragmented_free_bytes: u8,
    /// For interior pages: the right-most child page number.
    /// For leaf pages: `None`.
    pub right_child: Option<PageNumber>,
}

impl BtreePageHeader {
    /// Return the resolved content offset, clamping the 65536 sentinel to `usable_size`.
    #[must_use]
    pub fn content_offset(&self, usable_size: u32) -> usize {
        if self.cell_count == 0 || self.cell_content_offset >= 65536 {
            usable_size as usize
        } else {
            self.cell_content_offset as usize
        }
    }

    /// Parse a B-tree page header from raw page bytes.
    ///
    /// `header_offset` is typically 0, except for page 1 where the database
    /// file header occupies the first 100 bytes (`header_offset = 100`).
    pub fn parse(page: &[u8], header_offset: usize) -> Result<Self> {
        let remaining = page.len().saturating_sub(header_offset);
        if remaining < BTREE_LEAF_HEADER_SIZE as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page too small for B-tree header: {} bytes at offset {}",
                    remaining, header_offset
                ),
            });
        }

        let h = &page[header_offset..];

        let page_type =
            BtreePageType::from_flag(h[0]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("invalid B-tree page type flag: {:#04x}", h[0]),
            })?;

        let first_freeblock = u16::from_be_bytes([h[1], h[2]]);
        let cell_count = u16::from_be_bytes([h[3], h[4]]);
        let raw_content_offset = u16::from_be_bytes([h[5], h[6]]);
        let cell_content_offset = if raw_content_offset == 0 {
            65536
        } else {
            u32::from(raw_content_offset)
        };
        let fragmented_free_bytes = h[7];

        let right_child = if page_type.is_interior() {
            if remaining < BTREE_INTERIOR_HEADER_SIZE as usize {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "page too small for interior B-tree header".to_owned(),
                });
            }
            let pgno = u32::from_be_bytes([h[8], h[9], h[10], h[11]]);
            Some(
                PageNumber::new(pgno).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior page has zero right-child pointer".to_owned(),
                })?,
            )
        } else {
            None
        };

        Ok(Self {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_offset,
            fragmented_free_bytes,
            right_child,
        })
    }

    /// Write this header into a page buffer.
    ///
    /// `header_offset` is typically 0 (or 100 for page 1).
    pub fn write(&self, page: &mut [u8], header_offset: usize) {
        let h = &mut page[header_offset..];
        h[0] = self.page_type as u8;
        h[1..3].copy_from_slice(&self.first_freeblock.to_be_bytes());
        h[3..5].copy_from_slice(&self.cell_count.to_be_bytes());
        let content_offset_u16 = if self.cell_content_offset >= 65536 {
            0u16
        } else {
            #[allow(clippy::cast_possible_truncation)]
            {
                self.cell_content_offset as u16
            }
        };
        h[5..7].copy_from_slice(&content_offset_u16.to_be_bytes());
        h[7] = self.fragmented_free_bytes;

        if let Some(right_child) = self.right_child {
            h[8..12].copy_from_slice(&right_child.get().to_be_bytes());
        }
    }
}

/// Parse a B-tree page header using the canonical header offset for `page_no`
/// and attach page-number context to corruption errors.
pub fn parse_page_header(page: &[u8], page_no: PageNumber) -> Result<BtreePageHeader> {
    let header_offset = header_offset_for_page(page_no);
    BtreePageHeader::parse(page, header_offset).map_err(|error| FrankenError::DatabaseCorrupt {
        detail: format!(
            "failed to parse B-tree page {} at header offset {}: {}",
            page_no.get(),
            header_offset,
            error
        ),
    })
}

// ---------------------------------------------------------------------------
// Cell pointer array helpers
// ---------------------------------------------------------------------------

/// Read the cell pointer array from a page.
///
/// Returns a vector of byte offsets into the page where each cell starts.
/// `header_offset` is 0 for most pages, 100 for page 1.
pub fn read_cell_pointers(
    page: &[u8],
    header: &BtreePageHeader,
    header_offset: usize,
) -> Result<Vec<u16>> {
    let ptr_array_start = header_offset + header.page_type.header_size() as usize;
    let count = header.cell_count as usize;
    let ptr_array_end = ptr_array_start + count * CELL_POINTER_SIZE as usize;

    if ptr_array_end > page.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "cell pointer array extends past page: {} pointers at offset {}",
                count, ptr_array_start
            ),
        });
    }

    if ptr_array_end > header.cell_content_offset as usize {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "cell pointer array overlaps with cell content area: ptr_end={} > content={}",
                ptr_array_end, header.cell_content_offset
            ),
        });
    }

    let mut pointers = Vec::with_capacity(count);
    for i in 0..count {
        let off = ptr_array_start + i * CELL_POINTER_SIZE as usize;
        let ptr = u16::from_be_bytes([page[off], page[off + 1]]);
        pointers.push(ptr);
    }
    Ok(pointers)
}

/// Write the cell pointer array into a page.
pub fn write_cell_pointers(
    page: &mut [u8],
    header_offset: usize,
    header: &BtreePageHeader,
    pointers: &[u16],
) {
    let ptr_array_start = header_offset + header.page_type.header_size() as usize;
    for (i, &ptr) in pointers.iter().enumerate() {
        let off = ptr_array_start + i * CELL_POINTER_SIZE as usize;
        page[off..off + 2].copy_from_slice(&ptr.to_be_bytes());
    }
}

// ---------------------------------------------------------------------------
// Local payload calculation
// ---------------------------------------------------------------------------

/// Compute the maximum local payload for a cell on a page of the given type.
///
/// - Table leaf pages: `U - 35`
/// - All other page types: `((U - 12) * 64 / 255) - 23`
///
/// Where `U` is the usable page size.
#[must_use]
pub const fn max_local_payload(usable_size: u32, page_type: BtreePageType) -> u32 {
    if page_type.is_table() && page_type.is_leaf() {
        usable_size - 35
    } else {
        (usable_size - 12) * 64 / 255 - 23
    }
}

/// Compute the minimum local payload when overflow occurs.
///
/// Formula: `((U - 12) * 32 / 255) - 23`
///
/// This is the same for all page types.
#[must_use]
pub const fn min_local_payload(usable_size: u32) -> u32 {
    (usable_size - 12) * 32 / 255 - 23
}

/// Compute the actual local payload size for a cell.
///
/// If the total payload fits on the page (`payload_size <= max_local`),
/// all bytes are local. Otherwise, the local portion is computed as:
///
/// ```text
/// local = M + ((P - M) % (U - 4))
/// if local > X: local = M
/// ```
///
/// Where `P` = payload size, `U` = usable size, `X` = max local, `M` = min local.
#[must_use]
pub const fn local_payload_size(
    payload_size: u32,
    usable_size: u32,
    page_type: BtreePageType,
) -> u32 {
    let max_local = max_local_payload(usable_size, page_type);
    if payload_size <= max_local {
        return payload_size;
    }
    let min_local = min_local_payload(usable_size);
    let surplus = usable_size - 4;
    let local = min_local + (payload_size - min_local) % surplus;
    if local > max_local { min_local } else { local }
}

/// Whether a cell with the given payload size will overflow.
#[must_use]
pub const fn has_overflow(payload_size: u32, usable_size: u32, page_type: BtreePageType) -> bool {
    payload_size > max_local_payload(usable_size, page_type)
}

// ---------------------------------------------------------------------------
// Parsed cell references
// ---------------------------------------------------------------------------

/// A parsed reference to a cell on a B-tree page.
///
/// This is a lightweight struct that references the page data. For table
/// cells it extracts the rowid; for index cells it just references the
/// key bytes.
#[derive(Debug, Clone)]
pub struct CellRef {
    /// For interior pages: the left child page number.
    pub left_child: Option<PageNumber>,
    /// For table pages: the integer rowid key.
    /// For index pages: `None` (key is in payload).
    pub rowid: Option<i64>,
    /// Total payload size in bytes (local + overflow).
    pub payload_size: u32,
    /// Number of bytes of payload stored locally on this page.
    pub local_size: u32,
    /// Byte offset within the page where the local payload data starts.
    pub payload_offset: usize,
    /// If the cell overflows, the page number of the first overflow page.
    pub overflow_page: Option<PageNumber>,
}

impl CellRef {
    /// Parse a cell from the given page at the specified byte offset.
    ///
    /// `usable_size` is the usable page size (page_size - reserved_bytes).
    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    pub fn parse(
        page: &[u8],
        cell_offset: usize,
        page_type: BtreePageType,
        usable_size: u32,
    ) -> Result<Self> {
        let mut pos = cell_offset;

        // Interior pages start with a 4-byte left child pointer.
        let left_child = if page_type.is_interior() {
            if pos + 4 > page.len() {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "cell extends past page (left child)".to_owned(),
                });
            }
            let pgno = u32::from_be_bytes([page[pos], page[pos + 1], page[pos + 2], page[pos + 3]]);
            pos += 4;
            Some(
                PageNumber::new(pgno).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell has zero left-child pointer".to_owned(),
                })?,
            )
        } else {
            None
        };

        // Interior table cells: just left_child + rowid varint (no payload).
        if page_type == BtreePageType::InteriorTable {
            let (rowid_raw, rowid_len) =
                read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "truncated varint in interior table cell (rowid)".to_owned(),
                })?;
            #[allow(clippy::cast_possible_wrap)]
            let rowid = rowid_raw as i64;
            debug!(
                cell_type = ?page_type,
                payload_len = 0_u32,
                overflow = false,
                "decoded btree cell boundary"
            );
            return Ok(Self {
                left_child,
                rowid: Some(rowid),
                payload_size: 0,
                local_size: 0,
                payload_offset: pos + rowid_len,
                overflow_page: None,
            });
        }

        // All other cell types: payload_size varint.
        let (payload_size_raw, ps_len) =
            read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in cell (payload size)".to_owned(),
            })?;
        let payload_size =
            u32::try_from(payload_size_raw).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "cell payload size exceeds 32-bit range".to_owned(),
            })?;
        pos += ps_len;

        // Table cells (leaf table): rowid varint after payload size.
        let rowid = if page_type.is_table() {
            let (rowid_raw, rowid_len) =
                read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "truncated varint in table cell (rowid)".to_owned(),
                })?;
            pos += rowid_len;
            #[allow(clippy::cast_possible_wrap)]
            let r = rowid_raw as i64;
            Some(r)
        } else {
            None
        };

        let payload_offset = pos;
        let local_size = local_payload_size(payload_size, usable_size, page_type);
        let local_end = payload_offset
            .checked_add(local_size as usize)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell payload offset overflow".to_owned(),
            })?;
        if local_end > page.len() || local_end > usable_size as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "cell extends past usable page size (payload bytes)".to_owned(),
            });
        }

        // Check for overflow page pointer.
        let overflow_page = if local_size < payload_size {
            let overflow_ptr_offset = local_end;
            if overflow_ptr_offset + 4 > page.len()
                || overflow_ptr_offset + 4 > usable_size as usize
            {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "cell extends past usable page size (overflow pointer)".to_owned(),
                });
            }
            let pgno = u32::from_be_bytes([
                page[overflow_ptr_offset],
                page[overflow_ptr_offset + 1],
                page[overflow_ptr_offset + 2],
                page[overflow_ptr_offset + 3],
            ]);
            Some(
                PageNumber::new(pgno).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell has zero overflow page pointer".to_owned(),
                })?,
            )
        } else {
            None
        };

        debug!(
            cell_type = ?page_type,
            payload_len = payload_size,
            overflow = overflow_page.is_some(),
            "decoded btree cell boundary"
        );

        Ok(Self {
            left_child,
            rowid,
            payload_size,
            local_size,
            payload_offset,
            overflow_page,
        })
    }

    /// Get the local payload bytes from the page.
    pub fn local_payload<'a>(&self, page: &'a [u8]) -> &'a [u8] {
        &page[self.payload_offset..self.payload_offset + self.local_size as usize]
    }

    /// Size of the **payload portion** of this cell on the page: local payload
    /// bytes plus the 4-byte overflow pointer (if any).
    ///
    /// **Does NOT include** the cell header (left-child pointer, payload-size
    /// varint, rowid varint).  For the full on-page cell size use
    /// [`crate::payload::cell_on_page_size`], which requires `cell_start`.
    #[must_use]
    pub fn payload_on_page_size(&self) -> usize {
        let mut size = self.local_size as usize;
        if self.overflow_page.is_some() {
            size += 4;
        }
        size
    }
}

// ---------------------------------------------------------------------------
// Utility: compute header offset for a page
// ---------------------------------------------------------------------------

/// Returns the header offset for a given page number.
///
/// Page 1 has the 100-byte database file header before the B-tree header.
/// All other pages start at offset 0.
#[must_use]
pub const fn header_offset_for_page(page_no: PageNumber) -> usize {
    if page_no.get() == 1 {
        DB_HEADER_SIZE as usize
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    // -- Page type tests --

    #[test]
    fn test_page_type_from_flag() {
        assert_eq!(
            BtreePageType::from_flag(0x02),
            Some(BtreePageType::InteriorIndex)
        );
        assert_eq!(
            BtreePageType::from_flag(0x05),
            Some(BtreePageType::InteriorTable)
        );
        assert_eq!(
            BtreePageType::from_flag(0x0A),
            Some(BtreePageType::LeafIndex)
        );
        assert_eq!(
            BtreePageType::from_flag(0x0D),
            Some(BtreePageType::LeafTable)
        );
        assert_eq!(BtreePageType::from_flag(0x00), None);
        assert_eq!(BtreePageType::from_flag(0xFF), None);
    }

    #[test]
    fn test_page_type_predicates() {
        assert!(BtreePageType::InteriorTable.is_interior());
        assert!(BtreePageType::InteriorIndex.is_interior());
        assert!(!BtreePageType::LeafTable.is_interior());
        assert!(!BtreePageType::LeafIndex.is_interior());

        assert!(BtreePageType::LeafTable.is_leaf());
        assert!(BtreePageType::LeafIndex.is_leaf());

        assert!(BtreePageType::InteriorTable.is_table());
        assert!(BtreePageType::LeafTable.is_table());
        assert!(!BtreePageType::InteriorIndex.is_table());

        assert!(BtreePageType::InteriorIndex.is_index());
        assert!(BtreePageType::LeafIndex.is_index());
    }

    #[test]
    fn test_page_type_header_size() {
        assert_eq!(BtreePageType::LeafTable.header_size(), 8);
        assert_eq!(BtreePageType::LeafIndex.header_size(), 8);
        assert_eq!(BtreePageType::InteriorTable.header_size(), 12);
        assert_eq!(BtreePageType::InteriorIndex.header_size(), 12);
    }

    // -- Page header parse/write round-trip --

    #[test]
    fn test_page_header_leaf_table_roundtrip() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 5,
            cell_content_offset: 3800,
            fragmented_free_bytes: 2,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);

        let parsed = BtreePageHeader::parse(&page, 0).unwrap();
        assert_eq!(parsed, header);
    }

    #[test]
    fn test_page_header_interior_table_roundtrip() {
        let right_child = PageNumber::new(42).unwrap();
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 100,
            cell_count: 10,
            cell_content_offset: 2048,
            fragmented_free_bytes: 0,
            right_child: Some(right_child),
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);

        let parsed = BtreePageHeader::parse(&page, 0).unwrap();
        assert_eq!(parsed, header);
        assert_eq!(parsed.right_child.unwrap().get(), 42);
    }

    #[test]
    fn test_page_header_page_one_offset() {
        // Page 1 has the 100-byte database file header first.
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 3,
            cell_content_offset: 3900,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 100);

        let parsed = BtreePageHeader::parse(&page, 100).unwrap();
        assert_eq!(parsed, header);
    }

    #[test]
    fn test_page_header_content_offset_zero_means_65536() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_offset: 65536,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 65536];
        header.write(&mut page, 0);
        // Verify the raw bytes show 0x0000 for content offset.
        assert_eq!(page[5], 0);
        assert_eq!(page[6], 0);

        let parsed = BtreePageHeader::parse(&page, 0).unwrap();
        assert_eq!(parsed.cell_content_offset, 65536);
    }

    #[test]
    fn test_page_header_invalid_type() {
        let mut page = vec![0u8; 4096];
        page[0] = 0xFF; // Invalid type.
        let err = BtreePageHeader::parse(&page, 0).unwrap_err();
        assert!(err.to_string().contains("invalid B-tree page type"));
    }

    #[test]
    fn test_page_header_truncated() {
        let page = vec![0u8; 4]; // Too short.
        let err = BtreePageHeader::parse(&page, 0).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    // -- Cell pointer array tests --

    #[test]
    fn test_read_write_cell_pointers() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 3,
            cell_content_offset: 3800,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);

        let ptrs = [3900u16, 3950, 4000];
        write_cell_pointers(&mut page, 0, &header, &ptrs);

        let read_ptrs = read_cell_pointers(&page, &header, 0).unwrap();
        assert_eq!(read_ptrs, vec![3900, 3950, 4000]);
    }

    // -- Local payload calculation tests --

    #[test]
    fn test_max_local_payload_leaf_table() {
        // U = 4096, max local = 4096 - 35 = 4061
        assert_eq!(max_local_payload(4096, BtreePageType::LeafTable), 4061);
    }

    #[test]
    fn test_max_local_payload_other_types() {
        // U = 4096, max local = (4096 - 12) * 64 / 255 - 23 = 1002
        let expected = (4096 - 12) * 64 / 255 - 23;
        assert_eq!(
            max_local_payload(4096, BtreePageType::InteriorIndex),
            expected
        );
        assert_eq!(max_local_payload(4096, BtreePageType::LeafIndex), expected);
        assert_eq!(
            max_local_payload(4096, BtreePageType::InteriorTable),
            expected
        );
    }

    #[test]
    fn test_min_local_payload() {
        // U = 4096, min local = (4096 - 12) * 32 / 255 - 23 = 489
        let expected = (4096 - 12) * 32 / 255 - 23;
        assert_eq!(min_local_payload(4096), expected);
    }

    #[test]
    fn test_local_payload_fits_entirely() {
        // Small payload fits entirely on page.
        assert_eq!(local_payload_size(100, 4096, BtreePageType::LeafTable), 100);
    }

    #[test]
    fn test_local_payload_overflow() {
        // Large payload requires overflow.
        let usable = 4096u32;
        let payload = 5000u32;
        let local = local_payload_size(payload, usable, BtreePageType::LeafTable);
        let max_local = max_local_payload(usable, BtreePageType::LeafTable);
        let min_local = min_local_payload(usable);
        assert!(local >= min_local);
        assert!(local <= max_local);
        assert!(local < payload);
    }

    #[test]
    fn test_has_overflow() {
        assert!(!has_overflow(100, 4096, BtreePageType::LeafTable));
        assert!(has_overflow(5000, 4096, BtreePageType::LeafTable));
        assert!(!has_overflow(1000, 4096, BtreePageType::LeafIndex));
        assert!(has_overflow(1500, 4096, BtreePageType::LeafIndex));
    }

    // -- Cell parsing tests --

    #[test]
    fn test_parse_leaf_table_cell_no_overflow() {
        // Build a leaf table cell: [payload_size varint] [rowid varint] [payload]
        let mut page = vec![0u8; 4096];
        let cell_offset = 3900;

        // payload_size = 10, rowid = 42
        let mut pos = cell_offset;
        // payload_size varint (10 fits in 1 byte)
        page[pos] = 10;
        pos += 1;
        // rowid varint (42 fits in 1 byte)
        page[pos] = 42;
        pos += 1;
        // payload data
        for i in 0..10 {
            page[pos + i] = (i + 1) as u8;
        }

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::LeafTable, 4096).unwrap();
        assert_eq!(cell.rowid, Some(42));
        assert_eq!(cell.payload_size, 10);
        assert_eq!(cell.local_size, 10);
        assert!(cell.overflow_page.is_none());
        assert!(cell.left_child.is_none());

        let payload = cell.local_payload(&page);
        assert_eq!(payload, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_parse_leaf_table_cell_rejects_truncated_local_payload() {
        // Payload claims 10 bytes but only 2 header bytes fit on page.
        let mut page = vec![0u8; 64];
        let cell_offset = 60;
        page[cell_offset] = 10; // payload_size
        page[cell_offset + 1] = 1; // rowid

        let err = CellRef::parse(&page, cell_offset, BtreePageType::LeafTable, 64).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
        assert!(err.to_string().contains("payload bytes"));
    }

    #[test]
    fn test_parse_interior_table_cell() {
        // Interior table cell: [left_child: u32 BE] [rowid: varint]
        let mut page = vec![0u8; 4096];
        let cell_offset = 2000;
        let mut pos = cell_offset;

        // left_child = page 7
        page[pos..pos + 4].copy_from_slice(&7u32.to_be_bytes());
        pos += 4;
        // rowid = 100
        page[pos] = 100;

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::InteriorTable, 4096).unwrap();
        assert_eq!(cell.left_child.unwrap().get(), 7);
        assert_eq!(cell.rowid, Some(100));
        assert_eq!(cell.payload_size, 0);
    }

    #[test]
    fn test_parse_leaf_index_cell_no_overflow() {
        // Leaf index cell: [payload_size varint] [payload]
        let mut page = vec![0u8; 4096];
        let cell_offset = 3500;

        // payload_size = 5
        page[cell_offset] = 5;
        // payload data
        for i in 0..5 {
            page[cell_offset + 1 + i] = (i + 10) as u8;
        }

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::LeafIndex, 4096).unwrap();
        assert!(cell.left_child.is_none());
        assert!(cell.rowid.is_none());
        assert_eq!(cell.payload_size, 5);
        assert_eq!(cell.local_size, 5);
        assert!(cell.overflow_page.is_none());

        let payload = cell.local_payload(&page);
        assert_eq!(payload, &[10, 11, 12, 13, 14]);
    }

    #[test]
    fn test_parse_interior_index_cell() {
        // Interior index cell: [left_child: u32 BE] [payload_size varint] [payload]
        let mut page = vec![0u8; 4096];
        let cell_offset = 2500;
        let mut pos = cell_offset;

        // left_child = page 15
        page[pos..pos + 4].copy_from_slice(&15u32.to_be_bytes());
        pos += 4;
        // payload_size = 8
        page[pos] = 8;
        pos += 1;
        // payload data
        for i in 0..8 {
            page[pos + i] = (i + 20) as u8;
        }

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::InteriorIndex, 4096).unwrap();
        assert_eq!(cell.left_child.unwrap().get(), 15);
        assert!(cell.rowid.is_none());
        assert_eq!(cell.payload_size, 8);
        assert_eq!(cell.local_size, 8);
        assert!(cell.overflow_page.is_none());
    }

    #[test]
    fn test_parse_leaf_table_cell_with_overflow() {
        // Build a cell that overflows for a 4096-byte page.
        // max_local for leaf table = 4096 - 35 = 4061.
        // We need payload_size > 4061.
        let mut page = vec![0u8; 4096];
        let cell_offset = 0; // Place at start for simplicity.

        let payload_size: u32 = 5000;
        let usable_size: u32 = 4096;

        // payload_size varint (5000 in 2 bytes: 0x80|39=0xA7, 0x08=8 → 39*128+8=5000)
        // Actually let's compute: 5000 = 0x1388
        // Varint: byte0 = 0x80 | (5000 >> 7 & 0x7F) = 0x80 | 39 = 0xA7
        // byte1 = 5000 & 0x7F = 0x08
        // Wait, 39*128 = 4992, 4992+8 = 5000. Yes.
        let mut varint_buf = [0u8; 9];
        let ps_len =
            fsqlite_types::serial_type::write_varint(&mut varint_buf, u64::from(payload_size));
        page[cell_offset..cell_offset + ps_len].copy_from_slice(&varint_buf[..ps_len]);

        // rowid = 1
        let rowid_offset = cell_offset + ps_len;
        page[rowid_offset] = 1;
        let rowid_len = 1;

        let payload_offset = rowid_offset + rowid_len;
        let local = local_payload_size(payload_size, usable_size, BtreePageType::LeafTable);

        // Fill local payload with pattern.
        for i in 0..local as usize {
            if payload_offset + i < page.len() {
                page[payload_offset + i] = (i & 0xFF) as u8;
            }
        }

        // Write overflow page pointer after local payload.
        let overflow_ptr_offset = payload_offset + local as usize;
        if overflow_ptr_offset + 4 <= page.len() {
            page[overflow_ptr_offset..overflow_ptr_offset + 4]
                .copy_from_slice(&99u32.to_be_bytes());
        }

        let cell =
            CellRef::parse(&page, cell_offset, BtreePageType::LeafTable, usable_size).unwrap();
        assert_eq!(cell.rowid, Some(1));
        assert_eq!(cell.payload_size, payload_size);
        assert_eq!(cell.local_size, local);
        assert!(cell.local_size < cell.payload_size);
        assert_eq!(cell.overflow_page.unwrap().get(), 99);
    }

    #[test]
    fn test_header_offset_for_page() {
        assert_eq!(header_offset_for_page(PageNumber::ONE), 100);
        assert_eq!(header_offset_for_page(PageNumber::new(2).unwrap()), 0);
        assert_eq!(header_offset_for_page(PageNumber::new(100).unwrap()), 0);
    }

    // -- Various page sizes --

    #[test]
    fn test_local_payload_various_page_sizes() {
        for &page_size in &[512u32, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let max_tbl = max_local_payload(page_size, BtreePageType::LeafTable);
            let max_idx = max_local_payload(page_size, BtreePageType::LeafIndex);
            let min = min_local_payload(page_size);

            // max_local should always be > min_local.
            assert!(
                max_tbl > min,
                "page_size={page_size}: max_tbl={max_tbl} <= min={min}"
            );
            assert!(
                max_idx > min,
                "page_size={page_size}: max_idx={max_idx} <= min={min}"
            );

            // Table leaf max_local should be larger than index max_local.
            assert!(
                max_tbl > max_idx,
                "page_size={page_size}: max_tbl={max_tbl} <= max_idx={max_idx}"
            );
        }
    }
}
