//! Shared SQLite B-tree page/cell layout parser and verified EOF page-reference
//! rewriter (bd-2kvo, bd-reality-check-1022-llk6g.5.3).
//!
//! This module is the single source of truth for SQLite B-tree page header
//! parsing, cell-pointer array validation, local-payload overflow formulas,
//! cell decoding, and batch EOF page-number renumbering. Both `fsqlite-btree`
//! and `fsqlite-pager` consume these exact routines so pointer rewriting at
//! the WAL append gate can never drift from B-tree cell layout rules.

use std::collections::{HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};

use crate::PageNumber;
use crate::limits::{
    BTREE_INTERIOR_HEADER_SIZE, BTREE_LEAF_HEADER_SIZE, CELL_POINTER_SIZE, DB_HEADER_SIZE,
};
use crate::serial_type::read_varint;

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
    #[inline]
    pub fn parse(page: &[u8], header_offset: usize) -> Result<Self> {
        const LEAF: usize = BTREE_LEAF_HEADER_SIZE as usize;
        const INTERIOR_TAIL: usize = BTREE_INTERIOR_HEADER_SIZE as usize - LEAF;

        let leaf_end = header_offset.saturating_add(LEAF);
        let h: &[u8; LEAF] = page
            .get(header_offset..leaf_end)
            .and_then(|slice| slice.try_into().ok())
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page too small for B-tree header: {} bytes at offset {}",
                    page.len().saturating_sub(header_offset),
                    header_offset
                ),
            })?;

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
            let interior_end = leaf_end + INTERIOR_TAIL;
            let tail: &[u8; INTERIOR_TAIL] = page
                .get(leaf_end..interior_end)
                .and_then(|slice| slice.try_into().ok())
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "page too small for interior B-tree header".to_owned(),
                })?;
            let pgno = u32::from_be_bytes(*tail);
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
#[inline]
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
    let mut buf = Vec::new();
    read_cell_pointers_into(page, header, header_offset, &mut buf)?;
    Ok(buf)
}

/// Read the cell pointer array into a caller-owned buffer.
#[allow(clippy::incompatible_msrv)] // `<[u8]>::as_chunks` (1.88+) under nightly toolchain.
pub fn read_cell_pointers_into(
    page: &[u8],
    header: &BtreePageHeader,
    header_offset: usize,
    buf: &mut Vec<u16>,
) -> Result<()> {
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

    let cell_content_offset =
        usize::try_from(header.cell_content_offset).map_err(|_| FrankenError::DatabaseCorrupt {
            detail: format!(
                "cell content offset {} exceeds platform address range",
                header.cell_content_offset
            ),
        })?;
    if count > 0 && cell_content_offset >= page.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "non-empty page cell content offset {cell_content_offset} starts outside page of {} bytes",
                page.len()
            ),
        });
    }

    if ptr_array_end > cell_content_offset {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "cell pointer array overlaps with cell content area: ptr_end={} > content={}",
                ptr_array_end, cell_content_offset
            ),
        });
    }

    buf.clear();
    buf.reserve(count);

    let lo = u16::try_from(ptr_array_end).unwrap_or(u16::MAX);
    let span = u16::try_from(page.len() - ptr_array_end).unwrap_or(u16::MAX);
    let mut out_of_range = false;
    let ptr_bytes = &page[ptr_array_start..ptr_array_end];
    let (chunks, _) = ptr_bytes.as_chunks::<2>();
    buf.extend(chunks.iter().map(|c| {
        let ptr = u16::from_be_bytes(*c);
        out_of_range |= ptr.wrapping_sub(lo) >= span;
        ptr
    }));
    if out_of_range {
        return Err(cell_pointer_out_of_range(buf, ptr_array_end, page.len()));
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn cell_pointer_out_of_range(
    pointers: &[u16],
    ptr_array_end: usize,
    usable_len: usize,
) -> FrankenError {
    let min_ptr = pointers.iter().copied().min().unwrap_or(0);
    let max_ptr = pointers.iter().copied().max().unwrap_or(0);
    FrankenError::DatabaseCorrupt {
        detail: format!(
            "cell pointer outside the cell content area: pointers span [{min_ptr}, {max_ptr}], \
             valid range is [{ptr_array_end}, {usable_len})"
        ),
    }
}

/// The usable prefix of a page image: the bytes a b-tree page may use, with
/// the reserved trailer (`page_size - usable_size` bytes) cut off.
#[must_use]
#[inline]
pub fn usable_prefix(page: &[u8], usable_size: u32) -> &[u8] {
    let usable = usize::try_from(usable_size).unwrap_or(usize::MAX);
    &page[..usable.min(page.len())]
}

/// Write the cell pointer array into a page.
#[allow(clippy::incompatible_msrv)] // `<[u8]>::as_chunks_mut` (1.88+) under nightly toolchain.
pub fn write_cell_pointers(
    page: &mut [u8],
    header_offset: usize,
    header: &BtreePageHeader,
    pointers: &[u16],
) {
    let ptr_array_start = header_offset + header.page_type.header_size() as usize;
    let ptr_array_end = ptr_array_start + pointers.len() * CELL_POINTER_SIZE as usize;
    let dst = &mut page[ptr_array_start..ptr_array_end];
    let (chunks, _) = dst.as_chunks_mut::<2>();
    for (chunk, &ptr) in chunks.iter_mut().zip(pointers) {
        *chunk = ptr.to_be_bytes();
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
        usable_size.saturating_sub(35)
    } else {
        (usable_size.saturating_sub(12) * 64 / 255).saturating_sub(23)
    }
}

/// Compute the minimum local payload when overflow occurs.
///
/// Formula: `((U - 12) * 32 / 255) - 23`
#[must_use]
pub const fn min_local_payload(usable_size: u32) -> u32 {
    (usable_size.saturating_sub(12) * 32 / 255).saturating_sub(23)
}

/// Compute the actual local payload size for a cell.
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
    let surplus = usable_size.saturating_sub(4);
    if surplus == 0 {
        return min_local;
    }
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
    /// `usable_size` is the usable page size (`page_size - reserved_bytes`).
    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)]
    pub fn parse(
        page: &[u8],
        cell_offset: usize,
        page_type: BtreePageType,
        usable_size: u32,
    ) -> Result<Self> {
        let page = &page[..page.len().min(usable_size as usize)];
        if cell_offset > page.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell offset {cell_offset} extends past page length {}",
                    page.len()
                ),
            });
        }
        if page_type == BtreePageType::LeafTable {
            return Self::parse_leaf_table(page, cell_offset, usable_size);
        }

        let mut pos = cell_offset;

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

        if page_type == BtreePageType::InteriorTable {
            let (rowid_raw, rowid_len) =
                read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "truncated varint in interior table cell (rowid)".to_owned(),
                })?;
            #[allow(clippy::cast_possible_wrap)]
            let rowid = rowid_raw as i64;
            return Ok(Self {
                left_child,
                rowid: Some(rowid),
                payload_size: 0,
                local_size: 0,
                payload_offset: pos + rowid_len,
                overflow_page: None,
            });
        }

        let (payload_size_raw, ps_len) =
            read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in cell (payload size)".to_owned(),
            })?;
        let payload_size =
            u32::try_from(payload_size_raw).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "cell payload size exceeds 32-bit range".to_owned(),
            })?;
        pos += ps_len;

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

        Ok(Self {
            left_child,
            rowid,
            payload_size,
            local_size,
            payload_offset,
            overflow_page,
        })
    }

    #[inline]
    fn parse_leaf_table(page: &[u8], cell_offset: usize, usable_size: u32) -> Result<Self> {
        let (payload_size_raw, ps_len) =
            read_varint(&page[cell_offset..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in cell (payload size)".to_owned(),
            })?;
        let payload_size =
            u32::try_from(payload_size_raw).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "cell payload size exceeds 32-bit range".to_owned(),
            })?;
        let rowid_start =
            cell_offset
                .checked_add(ps_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell offset overflow after payload size".to_owned(),
                })?;
        let (rowid_raw, rowid_len) =
            read_varint(&page[rowid_start..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in table cell (rowid)".to_owned(),
            })?;
        let payload_offset =
            rowid_start
                .checked_add(rowid_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell payload offset overflow".to_owned(),
                })?;

        let max_local = usable_size.saturating_sub(35);
        let local_size = if payload_size <= max_local {
            payload_size
        } else {
            local_payload_size(payload_size, usable_size, BtreePageType::LeafTable)
        };
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

        #[allow(clippy::cast_possible_wrap)]
        let rowid = rowid_raw as i64;
        Ok(Self {
            left_child: None,
            rowid: Some(rowid),
            payload_size,
            local_size,
            payload_offset,
            overflow_page,
        })
    }

    /// Left-child fast path for interior cells.
    #[inline]
    pub fn read_interior_left_child(page: &[u8], cell_offset: usize) -> Result<PageNumber> {
        if cell_offset.saturating_add(4) > page.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "interior cell extends past page (left child)".to_owned(),
            });
        }
        let pgno = u32::from_be_bytes([
            page[cell_offset],
            page[cell_offset + 1],
            page[cell_offset + 2],
            page[cell_offset + 3],
        ]);
        PageNumber::new(pgno).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "interior cell has zero left-child pointer".to_owned(),
        })
    }

    /// Rowid-only fast path for leaf-table cells.
    #[inline]
    pub fn parse_leaf_table_rowid(page: &[u8], cell_offset: usize) -> Result<i64> {
        let cell = page
            .get(cell_offset..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell offset {cell_offset} extends past page length {}",
                    page.len()
                ),
            })?;
        let (_, ps_len) = read_varint(cell).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "truncated varint in table cell (payload size)".to_owned(),
        })?;
        let rowid_start =
            cell_offset
                .checked_add(ps_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell offset overflow after payload size".to_owned(),
                })?;
        let rowid_slice = page
            .get(rowid_start..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell offset overflow after payload size".to_owned(),
            })?;
        let (rowid_raw, _) =
            read_varint(rowid_slice).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in table cell (rowid)".to_owned(),
            })?;
        #[allow(clippy::cast_possible_wrap)]
        Ok(rowid_raw as i64)
    }

    /// Get the local payload bytes from the page.
    pub fn local_payload<'a>(&self, page: &'a [u8]) -> &'a [u8] {
        &page[self.payload_offset..self.payload_offset + self.local_size as usize]
    }

    /// Size of the **payload portion** of this cell on the page: local payload
    /// bytes plus the 4-byte overflow pointer (if any).
    #[must_use]
    pub fn payload_on_page_size(&self) -> usize {
        let mut size = self.local_size as usize;
        if self.overflow_page.is_some() {
            size += 4;
        }
        size
    }
}

/// Returns the header offset for a given page number (100 for page 1, 0 otherwise).
#[must_use]
#[inline]
pub const fn header_offset_for_page(page_no: PageNumber) -> usize {
    if page_no.get() == 1 {
        DB_HEADER_SIZE as usize
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Verified EOF page-reference rewriter (bd-reality-check-1022-llk6g.5.3)
// ---------------------------------------------------------------------------

/// Reason a batch is ineligible for append-gate EOF page-number renumbering.
///
/// Every variant is strictly fail-closed: the caller leaves the batch untouched
/// and falls back to the conservative `BusySnapshot` refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EofRenumberRejection {
    /// Batch writes page 1 (database header, freelist metadata, or `sqlite_master`).
    ContainsPageOne,
    /// `usable_size` is invalid or larger than a frame's byte length.
    InvalidUsableSize { usable_size: u32, frame_len: usize },
    /// Remap table is empty, non-injective, maps a page `<= snapshot_db_size`,
    /// targets page 0 or 1, or omits a fresh EOF page present in the batch.
    InvalidRemap(&'static str),
    /// Batch contains duplicate frame page numbers.
    DuplicateBatchPage(u32),
    /// A page in the batch is neither a valid B-tree page nor a reachable
    /// overflow page (for example a freelist trunk/leaf or pointer-map page).
    UnclassifiedPage(u32),
    /// A page in the batch was classified as both a B-tree page and an overflow page.
    AmbiguousPageKind(u32),
    /// A B-tree interior child pointer targets an overflow page in the batch.
    InteriorChildIsOverflow { parent: u32, child: u32 },
    /// A B-tree cell or overflow page references an EOF page (`> snapshot_db_size`)
    /// that is missing from the batch.
    MissingReferencedEofPage { source: u32, target: u32 },
    /// An overflow chain inside the batch is malformed (short page, cycle,
    /// shared overflow page, or length/terminator mismatch).
    MalformedOverflowChain { page: u32, detail: &'static str },
    /// A remapped EOF page does not have exact in-degree 1 inside the batch
    /// (0 = unreferenced orphan / new schema root; >1 = duplicate reference).
    EofPageInvalidInDegree { page: u32, in_degree: usize },
}

/// Summary of an applied batch EOF page-reference rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EofRenumberStats {
    pub btree_pages_checked: usize,
    pub overflow_pages_checked: usize,
    pub pointers_rewritten: usize,
}

#[derive(Debug, Clone)]
struct ParsedBtreePointers {
    /// `(byte_offset_in_page, target_page_no)` for every interior child pointer
    /// (`right_child` at `[8..12]` and each cell's `left_child`).
    interior_children: Vec<(usize, u32)>,
    /// `(byte_offset_in_page, first_overflow_pgno, overflow_byte_len)` for
    /// every spilling cell on the page.
    cell_overflows: Vec<(usize, u32, usize)>,
}

fn parse_btree_pointers(page: &[u8], usable_size: u32) -> Result<ParsedBtreePointers> {
    let usable = usable_prefix(page, usable_size);
    if usable.len() < usable_size as usize {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "page shorter than usable_size".to_owned(),
        });
    }
    let header = BtreePageHeader::parse(usable, 0)?;
    let cell_ptrs = read_cell_pointers(usable, &header, 0)?;

    let mut interior_children = Vec::new();
    let mut cell_overflows = Vec::new();

    if let Some(right_child) = header.right_child {
        interior_children.push((8, right_child.get()));
    }

    for ptr in cell_ptrs {
        let cell_offset = usize::from(ptr);
        let cell = CellRef::parse(usable, cell_offset, header.page_type, usable_size)?;
        if let Some(left_child) = cell.left_child {
            interior_children.push((cell_offset, left_child.get()));
        }
        if let Some(overflow_page) = cell.overflow_page {
            let overflow_ptr_offset = cell
                .payload_offset
                .checked_add(cell.local_size as usize)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "overflow pointer offset overflow".to_owned(),
                })?;
            let overflow_bytes = cell
                .payload_size
                .checked_sub(cell.local_size)
                .filter(|&rem| rem > 0)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "spilling cell has zero overflow bytes".to_owned(),
                })?;
            cell_overflows.push((
                overflow_ptr_offset,
                overflow_page.get(),
                overflow_bytes as usize,
            ));
        }
    }

    Ok(ParsedBtreePointers {
        interior_children,
        cell_overflows,
    })
}

/// Validate a batch's frames as a closed B-tree + overflow subgraph and rewrite
/// every page number and internal pointer according to `remap`.
///
/// - Every `(page_no, bytes)` entry in `frames` represents one page written by
///   a single transaction whose begin snapshot had `snapshot_db_size` pages.
/// - `remap` must map EVERY page in `frames` with `page_no > snapshot_db_size`
///   injectively to a fresh page number (`>= 2`, disjoint from all other mapped
///   targets and all unmapped pages in `frames`).
/// - Fails closed with `Err(EofRenumberRejection)` BEFORE mutating any byte in
///   `frames` if any invariant is violated.
/// - On `Ok(stats)`, every frame's `page_no` and every interior-child,
///   cell-overflow, and overflow-next 4-byte big-endian pointer referencing a
///   key in `remap` has been atomically updated in place, and every other byte
///   (including keys, payloads, headers, freeblocks, and reserved trailers) is
///   untouched.
#[allow(clippy::too_many_lines)]
pub fn rewrite_batch_eof_page_references<S: std::hash::BuildHasher>(
    frames: &mut [(u32, Vec<u8>)],
    snapshot_db_size: u32,
    usable_size: u32,
    remap: &HashMap<u32, u32, S>,
) -> std::result::Result<EofRenumberStats, EofRenumberRejection> {
    if usable_size < 480 {
        return Err(EofRenumberRejection::InvalidUsableSize {
            usable_size,
            frame_len: frames.first().map_or(0, |(_, data)| data.len()),
        });
    }
    if remap.is_empty() {
        return Err(EofRenumberRejection::InvalidRemap("remap is empty"));
    }

    // Validate batch uniqueness, page-1 exclusion, frame lengths, and remap shape.
    let mut index_by_page: HashMap<u32, usize> = HashMap::with_capacity(frames.len());
    for (idx, (page_no, data)) in frames.iter().enumerate() {
        if *page_no == 1 {
            return Err(EofRenumberRejection::ContainsPageOne);
        }
        if *page_no == 0 {
            return Err(EofRenumberRejection::UnclassifiedPage(0));
        }
        if data.len() < usable_size as usize {
            return Err(EofRenumberRejection::InvalidUsableSize {
                usable_size,
                frame_len: data.len(),
            });
        }
        if index_by_page.insert(*page_no, idx).is_some() {
            return Err(EofRenumberRejection::DuplicateBatchPage(*page_no));
        }
        if *page_no > snapshot_db_size && !remap.contains_key(page_no) {
            return Err(EofRenumberRejection::InvalidRemap(
                "fresh EOF page in batch is absent from remap",
            ));
        }
    }

    let mut target_pages: HashSet<u32> = HashSet::with_capacity(remap.len());
    for (&old_pgno, &new_pgno) in remap {
        if old_pgno <= snapshot_db_size {
            return Err(EofRenumberRejection::InvalidRemap(
                "remap attempts to move a pre-snapshot page",
            ));
        }
        if !index_by_page.contains_key(&old_pgno) {
            return Err(EofRenumberRejection::InvalidRemap(
                "remap contains an EOF page not present in batch",
            ));
        }
        if new_pgno < 2 {
            return Err(EofRenumberRejection::InvalidRemap(
                "remap target must be >= 2",
            ));
        }
        if !target_pages.insert(new_pgno) {
            return Err(EofRenumberRejection::InvalidRemap(
                "remap targets are not injective",
            ));
        }
        if index_by_page.contains_key(&new_pgno) && !remap.contains_key(&new_pgno) {
            return Err(EofRenumberRejection::InvalidRemap(
                "remap target collides with an unmapped batch page",
            ));
        }
    }

    // Pass 1: identify every valid B-tree page in the batch using the canonical
    // `BtreePageHeader` + `read_cell_pointers` + `CellRef::parse` single source.
    let mut btree_pages: HashMap<u32, ParsedBtreePointers> = HashMap::new();
    for (page_no, data) in frames.iter() {
        if let Ok(parsed) = parse_btree_pointers(data, usable_size) {
            btree_pages.insert(*page_no, parsed);
        }
    }

    // Pass 2: trace every overflow chain starting from all spilling B-tree cells,
    // validating exact SQLite overflow length/termination and disjointness.
    let overflow_chunk_cap = (usable_size as usize).saturating_sub(4);
    let mut overflow_next_by_page: HashMap<u32, u32> = HashMap::new();
    let mut eof_in_degree: HashMap<u32, usize> =
        remap.keys().copied().map(|pgno| (pgno, 0)).collect();

    for (&btree_pgno, parsed) in &btree_pages {
        for &(_, child_pgno) in &parsed.interior_children {
            if let Some(deg) = eof_in_degree.get_mut(&child_pgno) {
                *deg += 1;
            } else if child_pgno > snapshot_db_size {
                return Err(EofRenumberRejection::MissingReferencedEofPage {
                    source: btree_pgno,
                    target: child_pgno,
                });
            }
        }

        for &(_, first_overflow, overflow_byte_len) in &parsed.cell_overflows {
            let mut cur = first_overflow;
            let mut rem = overflow_byte_len;
            let mut prev_source = btree_pgno;
            while rem > 0 {
                if let Some(&frame_idx) = index_by_page.get(&cur) {
                    if btree_pages.contains_key(&cur) {
                        return Err(EofRenumberRejection::AmbiguousPageKind(cur));
                    }
                    if overflow_next_by_page.contains_key(&cur) {
                        return Err(EofRenumberRejection::MalformedOverflowChain {
                            page: cur,
                            detail: "overflow page visited twice (cycle or shared page)",
                        });
                    }
                    if let Some(deg) = eof_in_degree.get_mut(&cur) {
                        *deg += 1;
                    }
                    let page_bytes = &frames[frame_idx].1;
                    let next = u32::from_be_bytes([
                        page_bytes[0],
                        page_bytes[1],
                        page_bytes[2],
                        page_bytes[3],
                    ]);
                    if next == 1 {
                        return Err(EofRenumberRejection::MalformedOverflowChain {
                            page: cur,
                            detail: "overflow next pointer targets page 1",
                        });
                    }
                    let chunk = rem.min(overflow_chunk_cap);
                    rem -= chunk;
                    if rem > 0 && next == 0 {
                        return Err(EofRenumberRejection::MalformedOverflowChain {
                            page: cur,
                            detail: "overflow chain terminates before declared payload ends",
                        });
                    }
                    if rem == 0 && next != 0 {
                        return Err(EofRenumberRejection::MalformedOverflowChain {
                            page: cur,
                            detail: "overflow chain continues after declared payload ends",
                        });
                    }
                    overflow_next_by_page.insert(cur, next);
                    prev_source = cur;
                    cur = next;
                } else if cur <= snapshot_db_size {
                    // Pre-existing unmodified overflow chain on a pre-snapshot page;
                    // neither this page nor any unmodified successor can belong to `remap`.
                    break;
                } else {
                    return Err(EofRenumberRejection::MissingReferencedEofPage {
                        source: prev_source,
                        target: cur,
                    });
                }
            }
        }
    }

    // Pass 3: completeness & disjointness verification (no mutation yet).
    // 1. Every interior child that resides in the batch must be a B-tree page.
    for (&btree_pgno, parsed) in &btree_pages {
        for &(_, child_pgno) in &parsed.interior_children {
            if overflow_next_by_page.contains_key(&child_pgno) {
                return Err(EofRenumberRejection::InteriorChildIsOverflow {
                    parent: btree_pgno,
                    child: child_pgno,
                });
            }
            if index_by_page.contains_key(&child_pgno) && !btree_pages.contains_key(&child_pgno) {
                return Err(EofRenumberRejection::UnclassifiedPage(child_pgno));
            }
        }
    }

    // 2. Every frame in the batch must be classified as either B-tree or overflow.
    for (page_no, _) in frames.iter() {
        let is_btree = btree_pages.contains_key(page_no);
        let is_overflow = overflow_next_by_page.contains_key(page_no);
        match (is_btree, is_overflow) {
            (true, false) | (false, true) => {}
            (true, true) => return Err(EofRenumberRejection::AmbiguousPageKind(*page_no)),
            (false, false) => return Err(EofRenumberRejection::UnclassifiedPage(*page_no)),
        }
    }

    // 3. Every remapped EOF page must have exact in-degree 1 inside the batch.
    for (&eof_pgno, &in_degree) in &eof_in_degree {
        if in_degree != 1 {
            return Err(EofRenumberRejection::EofPageInvalidInDegree {
                page: eof_pgno,
                in_degree,
            });
        }
    }

    // Pass 4 (infallible): apply all pointer rewrites and frame page-number renames.
    let mut pointers_rewritten = 0_usize;
    for (page_no, data) in frames.iter_mut() {
        let old_pgno = *page_no;
        if let Some(parsed) = btree_pages.get(&old_pgno) {
            for &(offset, target) in &parsed.interior_children {
                if let Some(&mapped) = remap.get(&target) {
                    data[offset..offset + 4].copy_from_slice(&mapped.to_be_bytes());
                    pointers_rewritten += 1;
                }
            }
            for &(offset, target, _) in &parsed.cell_overflows {
                if let Some(&mapped) = remap.get(&target) {
                    data[offset..offset + 4].copy_from_slice(&mapped.to_be_bytes());
                    pointers_rewritten += 1;
                }
            }
        } else if let Some(&next) = overflow_next_by_page.get(&old_pgno)
            && next != 0
            && let Some(&mapped) = remap.get(&next)
        {
            data[0..4].copy_from_slice(&mapped.to_be_bytes());
            pointers_rewritten += 1;
        }
        if let Some(&mapped) = remap.get(&old_pgno) {
            *page_no = mapped;
        }
    }

    Ok(EofRenumberStats {
        btree_pages_checked: btree_pages.len(),
        overflow_pages_checked: overflow_next_by_page.len(),
        pointers_rewritten,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serial_type::write_varint;
    use proptest::prelude::*;

    fn encode_cell_for_page(
        page_type: BtreePageType,
        left_child: Option<u32>,
        rowid: i64,
        payload_size: u32,
        local_payload_bytes: &[u8],
        overflow_page: Option<u32>,
    ) -> Vec<u8> {
        let mut cell = Vec::new();
        if page_type.is_interior() {
            cell.extend_from_slice(&left_child.expect("interior left_child").to_be_bytes());
        }
        if page_type == BtreePageType::InteriorTable {
            let mut vbuf = [0_u8; 9];
            #[allow(clippy::cast_sign_loss)]
            let n = write_varint(&mut vbuf, rowid as u64);
            cell.extend_from_slice(&vbuf[..n]);
            return cell;
        }
        let mut vbuf = [0_u8; 9];
        let n = write_varint(&mut vbuf, u64::from(payload_size));
        cell.extend_from_slice(&vbuf[..n]);
        if page_type.is_table() {
            #[allow(clippy::cast_sign_loss)]
            let rn = write_varint(&mut vbuf, rowid as u64);
            cell.extend_from_slice(&vbuf[..rn]);
        }
        cell.extend_from_slice(local_payload_bytes);
        if let Some(ovf) = overflow_page {
            cell.extend_from_slice(&ovf.to_be_bytes());
        }
        cell
    }

    fn build_btree_page(
        page_size: usize,
        reserved: usize,
        page_type: BtreePageType,
        right_child: Option<u32>,
        cells: &[Vec<u8>],
    ) -> Vec<u8> {
        let usable = page_size - reserved;
        let mut page = vec![0_u8; page_size];
        // Fill reserved trailer with non-zero sentinel bytes so tests prove
        // rewriting never touches reserved bytes.
        for (i, b) in page[usable..].iter_mut().enumerate() {
            *b = 0xA0_u8.wrapping_add(u8::try_from(i % 16).unwrap_or(0));
        }

        let mut cursor = usable;
        let mut ptrs = Vec::with_capacity(cells.len());
        for cell in cells {
            cursor = cursor.checked_sub(cell.len()).expect("cell fits in page");
            page[cursor..cursor + cell.len()].copy_from_slice(cell);
            ptrs.push(u16::try_from(cursor).expect("offset fits u16"));
        }

        let header = BtreePageHeader {
            page_type,
            first_freeblock: 0,
            cell_count: u16::try_from(cells.len()).expect("cell count fits u16"),
            cell_content_offset: u32::try_from(cursor).expect("content offset fits u32"),
            fragmented_free_bytes: 0,
            right_child: right_child.map(|p| PageNumber::new(p).expect("nonzero right_child")),
        };
        header.write(&mut page, 0);
        write_cell_pointers(&mut page, 0, &header, &ptrs);
        page
    }

    fn build_overflow_chain(
        page_size: usize,
        reserved: usize,
        overflow_pages: &[u32],
        payload_tail: &[u8],
    ) -> Vec<(u32, Vec<u8>)> {
        let usable = page_size - reserved;
        let chunk_cap = usable - 4;
        assert_eq!(overflow_pages.len(), payload_tail.len().div_ceil(chunk_cap));
        let mut out = Vec::with_capacity(overflow_pages.len());
        for (idx, &pgno) in overflow_pages.iter().enumerate() {
            let mut page = vec![0_u8; page_size];
            let next = overflow_pages.get(idx + 1).copied().unwrap_or(0);
            page[0..4].copy_from_slice(&next.to_be_bytes());
            let start = idx * chunk_cap;
            let end = (start + chunk_cap).min(payload_tail.len());
            page[4..4 + (end - start)].copy_from_slice(&payload_tail[start..end]);
            for (i, b) in page[usable..].iter_mut().enumerate() {
                *b = 0xC0_u8.wrapping_add(u8::try_from(i % 16).unwrap_or(0));
            }
            out.push((pgno, page));
        }
        out
    }

    /// Parse all logical pointer references out of a B-tree page (and verify
    /// every cell parses cleanly) so proptest can check
    /// `parse(rewrite(page, map)) == map(parse(page))`.
    fn extract_btree_logical_refs(
        page: &[u8],
        usable_size: u32,
    ) -> (
        Option<u32>,
        Vec<(Option<u32>, Option<i64>, u32, Vec<u8>, Option<u32>)>,
    ) {
        let usable = usable_prefix(page, usable_size);
        let header = BtreePageHeader::parse(usable, 0).expect("valid btree header");
        let ptrs = read_cell_pointers(usable, &header, 0).expect("valid cell ptrs");
        let mut cells = Vec::with_capacity(ptrs.len());
        for ptr in ptrs {
            let cell = CellRef::parse(usable, usize::from(ptr), header.page_type, usable_size)
                .expect("valid cell");
            cells.push((
                cell.left_child.map(PageNumber::get),
                cell.rowid,
                cell.payload_size,
                cell.local_payload(usable).to_vec(),
                cell.overflow_page.map(PageNumber::get),
            ));
        }
        (header.right_child.map(PageNumber::get), cells)
    }

    fn apply_map(map: &HashMap<u32, u32>, pgno: u32) -> u32 {
        map.get(&pgno).copied().unwrap_or(pgno)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Proptest across ALL FOUR B-tree page types (`0x02`, `0x05`, `0x0A`,
        /// `0x0D`), multiple page sizes & reserved byte counts, and both local
        /// and multi-page spilling overflow chains:
        /// `parse(rewrite(batch, map)) == map(parse(batch))`, while every
        /// payload byte and reserved trailer remains bit-for-bit identical.
        #[test]
        fn prop_rewrite_all_four_page_types_and_overflow_chains(
            (page_size, reserved) in prop_oneof![
                Just((512_usize, 0_usize)),
                Just((512_usize, 16_usize)),
                Just((512_usize, 32_usize)),
                Just((1024_usize, 0_usize)),
                Just((1024_usize, 16_usize)),
                Just((1024_usize, 48_usize)),
                Just((4096_usize, 0_usize)),
                Just((4096_usize, 16_usize)),
                Just((4096_usize, 48_usize)),
            ],
            page_type_idx in 0_u8..4,
            cell_count in 1_usize..=4,
            spill_mask in any::<u8>(),
            chain_len in 1_usize..=3,
            shift in 10_u32..500,
        ) {
            let page_type = match page_type_idx {
                0 => BtreePageType::InteriorIndex,
                1 => BtreePageType::InteriorTable,
                2 => BtreePageType::LeafIndex,
                _ => BtreePageType::LeafTable,
            };
            let usable_size = u32::try_from(page_size - reserved).expect("usable u32");
            let snapshot_db_size = 20_u32;
            let root_pgno = 5_u32; // pre-snapshot root (not moved)
            let mut next_eof = snapshot_db_size + 1;
            let mut extra_frames: Vec<(u32, Vec<u8>)> = Vec::new();
            let mut cells: Vec<Vec<u8>> = Vec::new();
            let mut remap: HashMap<u32, u32> = HashMap::new();

            let right_child = if page_type.is_interior() {
                let rc = next_eof;
                next_eof += 1;
                // Child leaf page of matching family.
                let child_type = if page_type.is_table() {
                    BtreePageType::LeafTable
                } else {
                    BtreePageType::LeafIndex
                };
                let child_page = build_btree_page(page_size, reserved, child_type, None, &[]);
                extra_frames.push((rc, child_page));
                Some(rc)
            } else {
                None
            };

            let max_local = max_local_payload(usable_size, page_type) as usize;
            let chunk_cap = (usable_size as usize) - 4;
            for i in 0..cell_count {
                let left_child = if page_type.is_interior() {
                    // Alternate between a pre-snapshot child (<= snapshot_db_size)
                    // and a fresh EOF child (> snapshot_db_size).
                    if i.is_multiple_of(2) {
                        Some(10 + u32::try_from(i).unwrap_or(0))
                    } else {
                        let lc = next_eof;
                        next_eof += 1;
                        let child_type = if page_type.is_table() {
                            BtreePageType::LeafTable
                        } else {
                            BtreePageType::LeafIndex
                        };
                        let child_page =
                            build_btree_page(page_size, reserved, child_type, None, &[]);
                        extra_frames.push((lc, child_page));
                        Some(lc)
                    }
                } else {
                    None
                };

                let rowid = i64::try_from(i + 1).unwrap_or(1) * 100;
                if page_type == BtreePageType::InteriorTable {
                    cells.push(encode_cell_for_page(page_type, left_child, rowid, 0, &[], None));
                    continue;
                }

                let should_spill = ((spill_mask >> (i % 8)) & 1) == 1;
                if should_spill {
                    let payload_size =
                        u32::try_from(max_local + 1 + (chain_len - 1) * chunk_cap + 17)
                            .expect("payload fits u32");
                    let local_size =
                        local_payload_size(payload_size, usable_size, page_type) as usize;
                    let overflow_bytes = (payload_size as usize) - local_size;
                    let needed_ovf_pages = overflow_bytes.div_ceil(chunk_cap);
                    let mut ovf_pgnos = Vec::with_capacity(needed_ovf_pages);
                    for _ in 0..needed_ovf_pages {
                        ovf_pgnos.push(next_eof);
                        next_eof += 1;
                    }
                    let local_data: Vec<u8> = (0..local_size)
                        .map(|k| u8::try_from((k + i * 13) % 251).unwrap_or(0))
                        .collect();
                    let overflow_tail: Vec<u8> = (0..overflow_bytes)
                        .map(|k| u8::try_from((k * 7 + i * 29) % 251).unwrap_or(0))
                        .collect();
                    extra_frames.extend(build_overflow_chain(
                        page_size,
                        reserved,
                        &ovf_pgnos,
                        &overflow_tail,
                    ));
                    cells.push(encode_cell_for_page(
                        page_type,
                        left_child,
                        rowid,
                        payload_size,
                        &local_data,
                        Some(ovf_pgnos[0]),
                    ));
                } else {
                    let local_size = 16.min(max_local);
                    let local_data: Vec<u8> = (0..local_size)
                        .map(|k| u8::try_from((k + i * 5) % 251).unwrap_or(0))
                        .collect();
                    cells.push(encode_cell_for_page(
                        page_type,
                        left_child,
                        rowid,
                        u32::try_from(local_size).expect("local u32"),
                        &local_data,
                        None,
                    ));
                }
            }

            // Skip cases where no EOF pages were created at all (e.g. LeafTable
            // with spill_mask == 0); force at least one EOF page if needed.
            if next_eof == snapshot_db_size + 1 {
                return Ok(());
            }

            let root_page = build_btree_page(page_size, reserved, page_type, right_child, &cells);
            let mut frames = Vec::with_capacity(1 + extra_frames.len());
            frames.push((root_pgno, root_page));
            frames.extend(extra_frames);

            for &(pgno, _) in &frames {
                if pgno > snapshot_db_size {
                    remap.insert(pgno, pgno + shift);
                }
            }

            let before_root = extract_btree_logical_refs(&frames[0].1, usable_size);
            let before_overflow_payloads: HashMap<u32, (u32, Vec<u8>, Vec<u8>)> = frames[1..]
                .iter()
                .filter_map(|(pgno, data)| {
                    if BtreePageHeader::parse(usable_prefix(data, usable_size), 0).is_ok() {
                        None
                    } else {
                        let next = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                        Some((
                            *pgno,
                            (next, data[4..usable_size as usize].to_vec(), data[usable_size as usize..].to_vec()),
                        ))
                    }
                })
                .collect();
            let before_root_trailer = frames[0].1[usable_size as usize..].to_vec();

            let stats = rewrite_batch_eof_page_references(
                &mut frames,
                snapshot_db_size,
                usable_size,
                &remap,
            )
            .expect("valid batch rewrite must succeed");
            prop_assert!(stats.pointers_rewritten >= remap.len());

            // Verify root page parses to exact `map(parse(before))` and preserves trailer.
            prop_assert_eq!(frames[0].0, root_pgno);
            prop_assert_eq!(&frames[0].1[usable_size as usize..], before_root_trailer.as_slice());
            let after_root = extract_btree_logical_refs(&frames[0].1, usable_size);
            prop_assert_eq!(after_root.0, before_root.0.map(|p| apply_map(&remap, p)));
            prop_assert_eq!(after_root.1.len(), before_root.1.len());
            for (before_cell, after_cell) in before_root.1.iter().zip(&after_root.1) {
                prop_assert_eq!(after_cell.0, before_cell.0.map(|p| apply_map(&remap, p)));
                prop_assert_eq!(after_cell.1, before_cell.1);
                prop_assert_eq!(after_cell.2, before_cell.2);
                prop_assert_eq!(&after_cell.3, &before_cell.3);
                prop_assert_eq!(after_cell.4, before_cell.4.map(|p| apply_map(&remap, p)));
            }

            // Verify every overflow page was renamed, its next pointer mapped, and
            // its payload + reserved trailer preserved bit-for-bit.
            for (old_pgno, (old_next, old_payload, old_trailer)) in before_overflow_payloads {
                let new_pgno = apply_map(&remap, old_pgno);
                let (_, new_bytes) = frames
                    .iter()
                    .find(|(pgno, _)| *pgno == new_pgno)
                    .expect("renamed overflow page present");
                let new_next = u32::from_be_bytes([new_bytes[0], new_bytes[1], new_bytes[2], new_bytes[3]]);
                let expected_next = if old_next == 0 {
                    0
                } else {
                    apply_map(&remap, old_next)
                };
                prop_assert_eq!(new_next, expected_next);
                prop_assert_eq!(&new_bytes[4..usable_size as usize], old_payload.as_slice());
                prop_assert_eq!(&new_bytes[usable_size as usize..], old_trailer.as_slice());
            }
        }
    }

    #[test]
    fn reject_page_one_and_unreferenced_and_corrupt_overflow_without_mutating() {
        let page_size = 4096;
        let usable = 4096_u32;

        // 1. Page 1 present -> ContainsPageOne
        let p1 = build_btree_page(page_size, 0, BtreePageType::LeafTable, None, &[]);
        let p11 = build_btree_page(page_size, 0, BtreePageType::LeafTable, None, &[]);
        let mut frames = vec![(1, p1), (11, p11)];
        let snap = frames.clone();
        let remap = HashMap::from([(11, 21)]);
        assert_eq!(
            rewrite_batch_eof_page_references(&mut frames, 10, usable, &remap),
            Err(EofRenumberRejection::ContainsPageOne)
        );
        assert_eq!(frames, snap, "rejected rewrite must not mutate frames");

        // 2. Unreferenced EOF page (in_degree == 0) -> EofPageInvalidInDegree
        let root = build_btree_page(page_size, 0, BtreePageType::LeafTable, None, &[]);
        let orphan = build_btree_page(page_size, 0, BtreePageType::LeafTable, None, &[]);
        let mut frames = vec![(3, root), (11, orphan)];
        let snap = frames.clone();
        assert_eq!(
            rewrite_batch_eof_page_references(&mut frames, 10, usable, &remap),
            Err(EofRenumberRejection::EofPageInvalidInDegree {
                page: 11,
                in_degree: 0,
            })
        );
        assert_eq!(frames, snap);

        // 3. Unclassified non-B-tree page (e.g. zeroed freelist/unknown page) -> UnclassifiedPage
        let root = build_btree_page(page_size, 0, BtreePageType::LeafTable, None, &[]);
        let mut frames = vec![(3, root), (11, vec![0_u8; page_size])];
        let snap = frames.clone();
        assert_eq!(
            rewrite_batch_eof_page_references(&mut frames, 10, usable, &remap),
            Err(EofRenumberRejection::UnclassifiedPage(11))
        );
        assert_eq!(frames, snap);

        // 4. Truncated overflow chain (next == 0 before payload ends) -> MalformedOverflowChain
        let payload_size = 9000_u32;
        let local_size =
            local_payload_size(payload_size, usable, BtreePageType::LeafTable) as usize;
        let cell = encode_cell_for_page(
            BtreePageType::LeafTable,
            None,
            1,
            payload_size,
            &vec![0x55; local_size],
            Some(11),
        );
        let root = build_btree_page(page_size, 0, BtreePageType::LeafTable, None, &[cell]);
        let bad_ovf = vec![0_u8; page_size]; // next = 0, but needs 2 overflow pages!
        let mut frames = vec![(3, root), (11, bad_ovf)];
        let snap = frames.clone();
        assert!(matches!(
            rewrite_batch_eof_page_references(&mut frames, 10, usable, &remap),
            Err(EofRenumberRejection::MalformedOverflowChain { page: 11, .. })
        ));
        assert_eq!(frames, snap);
    }
}
