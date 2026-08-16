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
    //
    // Hot path: this runs once per page on every cursor `load_page`. The
    // body is inlined so cross-crate callers (the `parse_page_header`
    // wrapper, plus direct uses in `cursor.rs` / `balance.rs`) can fold
    // the constant `header_offset` away. Bytes are pulled out through a
    // single `page.get(..).try_into()::<&[u8; 8]>` so every subsequent
    // index becomes statically in-bounds — replacing the eight separate
    // `h[i]` accesses (each previously bounds-checked because LLVM could
    // not see the saturating-sub guard) with one branchless load.
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
//
// Inlined so the constant 0 / 100 `header_offset` produced by
// `header_offset_for_page` folds into the inner `parse` body for every
// non-page-1 caller.
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
///
/// This allocates a fresh `Vec<u16>` on every call. Hot callers that parse
/// the same page repeatedly (or that already own a `Vec<u16>` buffer) should
/// prefer [`read_cell_pointers_into`], which reuses caller-provided storage.
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
///
/// Clears `buf` and fills it with the cell pointer offsets for the given
/// page header. The buffer's existing allocation is reused when possible
/// (when `buf.capacity() >= cell_count`), eliminating an allocation on the
/// cursor hot path.
///
/// This is the low-level primitive; [`read_cell_pointers`] is a thin
/// convenience wrapper that allocates a fresh `Vec`.
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

    // Slice once, then use `as_chunks::<2>` to hand LLVM `&[u8; 2]` per
    // element instead of the unsized `&[u8]` from `chunks_exact`. The
    // sized array drops the per-iteration bounds checks on `c[0]` /
    // `c[1]` that `chunks_exact(2).map(|c| [c[0], c[1]])` was still
    // paying — same array-conversion bounds-elide pattern that took
    // `BtreePageHeader::parse` from 10.7 ns to 3.7 ns (commit 1f266968).
    let ptr_bytes = &page[ptr_array_start..ptr_array_end];
    let (chunks, _) = ptr_bytes.as_chunks::<2>();
    buf.extend(chunks.iter().map(|c| u16::from_be_bytes(*c)));
    Ok(())
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
    // Hoist the bounds check out of the loop with a single mut slice, then
    // hand LLVM `&mut [u8; 2]` chunks via `as_chunks_mut::<2>`. The
    // index-arithmetic `page[off..off + 2]` form forced a per-iteration
    // bounds check that wouldn't fold even though `chunks` is exact.
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
///
/// This is the same for all page types.
#[must_use]
pub const fn min_local_payload(usable_size: u32) -> u32 {
    (usable_size.saturating_sub(12) * 32 / 255).saturating_sub(23)
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

    /// bd-ah597.3: left-child fast path for interior cells.
    ///
    /// Every interior cell (Table or Index) starts with a 4-byte big-endian
    /// child-page pointer. Callers that only need the child page — descent
    /// navigation, balance-time child enumeration, separator-replace — can
    /// read those 4 bytes directly without paying for the full
    /// `CellRef::parse` (varint decodes, local-size math, overflow checks).
    /// Identical byte-level shape to the private `read_child_at_offset`
    /// helper in `BtCursor`; this is the public sibling so `balance.rs` and
    /// future crates can use it.
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

    /// bd-9e3xf.4: rowid-only fast path for leaf-table cells.
    ///
    /// Callers like `BtCursor::predecessor_idx` on the DELETE rebalance path
    /// invoke `CellRef::parse` only to read `cell.rowid`, throwing away the
    /// `local_size` / `overflow_page` plumbing. A leaf-table cell is
    /// `[payload_size varint][rowid varint][payload bytes…]`, so the rowid
    /// is fully determined by two varint reads — no `local_payload_size`
    /// math, no overflow-pointer materialisation.
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
#[inline]
pub const fn header_offset_for_page(page_no: PageNumber) -> usize {
    if page_no.get() == 1 {
        DB_HEADER_SIZE as usize
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Lightweight cell helpers (OPT-A3)
// ---------------------------------------------------------------------------

/// Read a table-leaf cell's rowid without constructing a full [`CellRef`].
///
/// Table-leaf cells begin with two varints: `payload_size` then `rowid`.
/// This helper reads just those two varints and returns the rowid, skipping
/// all the overflow-chain / local-size / bounds-check work that
/// [`CellRef::parse`] performs.
///
/// Caller MUST have already verified that `page[cell_offset..]` is a
/// table-leaf cell (page type flag 0x0D) and that `cell_offset` is within
/// `page.len()`. This is intended for the INSERT append fast path after the
/// page header has been checked.
///
/// Returns `None` when the varints are truncated.
#[must_use]
pub fn read_table_leaf_rowid_at_offset(page: &[u8], cell_offset: usize) -> Option<i64> {
    if cell_offset >= page.len() {
        return None;
    }
    let cell = &page[cell_offset..];
    let (_, payload_varint_len) = read_varint(cell)?;
    if payload_varint_len >= cell.len() {
        return None;
    }
    let (rowid_raw, _) = read_varint(&cell[payload_varint_len..])?;
    #[allow(clippy::cast_possible_wrap)]
    Some(rowid_raw as i64)
}

/// Minimum on-page allocation for any cell, per the SQLite file format
/// (per SQLite's `cellSizePtr`: "the minimum size of any cell is 4 bytes").
///
/// A leaf-index cell whose key record encodes in fewer bytes (e.g. a
/// single-column WITHOUT ROWID PRIMARY KEY holding 0/1 via serial types 8/9
/// is 3 bytes) still occupies 4 bytes of the cell-content area; the trailing
/// bytes are padding inside the cell's allocation. Stock `sqlite3` integrity
/// checks count coverage at this floor, so tighter packing reads as page
/// corruption ("Multiple uses for byte N"). bd-bfnlm.
pub const MIN_CELL_ALLOCATION: usize = 4;

/// Compute the on-page size of a cell without constructing a full [`CellRef`].
///
/// The on-page size is the total number of bytes the cell occupies in the
/// cell-content area, which equals:
///
///   `max((payload_offset - cell_start) + local_size + (4 if overflow else 0),
///        MIN_CELL_ALLOCATION)`
///
/// The [`MIN_CELL_ALLOCATION`] floor mirrors SQLite's `cellSizePtr` so that
/// freeing and defragmentation account undersized leaf-index cells at their
/// true allocated span. The floor is clamped at the usable-area boundary so a
/// legacy tight-packed page (written before the floor existed) never yields a
/// span past the page.
///
/// Unlike [`CellRef::parse`] + [`crate::payload::cell_on_page_size`], this
/// helper reads only the varints it needs and avoids the overflow-page
/// `PageNumber::new` validation (we already know the cell layout is on-page
/// because the page header's `content_offset` covers it; if the payload
/// ever overflows we just add 4 for the trailing pointer without decoding it).
///
/// Used by defragmentation loops in `replace_interior_cell`,
/// `remove_cell_from_leaf`, and the separator-repair / table-leaf-delete
/// paths, which only need the cell's on-page size and don't care about the
/// logical payload contents.
///
/// Returns `Err(DatabaseCorrupt)` when the cell header varints are
/// truncated or extend past the page.
pub fn cell_on_page_size_fast(
    page: &[u8],
    cell_offset: usize,
    page_type: BtreePageType,
    usable_size: u32,
) -> Result<usize> {
    if cell_offset >= page.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "cell offset past end of page".to_owned(),
        });
    }
    let mut pos = cell_offset;

    // Interior pages: 4-byte left child pointer prefix.
    if page_type.is_interior() {
        if pos + 4 > page.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "cell extends past page (left child)".to_owned(),
            });
        }
        pos += 4;
    }

    // Interior-table cells: left_child (4) + rowid varint. Nothing else.
    if page_type == BtreePageType::InteriorTable {
        let (_rowid, rowid_len) =
            read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in interior table cell (rowid)".to_owned(),
            })?;
        return Ok(pos + rowid_len - cell_offset);
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

    // Table-leaf cells: rowid varint after payload size.
    if page_type.is_table() {
        let (_rowid, rowid_len) =
            read_varint(&page[pos..]).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "truncated varint in table cell (rowid)".to_owned(),
            })?;
        pos += rowid_len;
    }

    let local_size = local_payload_size(payload_size, usable_size, page_type) as usize;
    let local_end = pos
        .checked_add(local_size)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "cell payload offset overflow".to_owned(),
        })?;
    if local_end > page.len() || local_end > usable_size as usize {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "cell extends past usable page size (payload bytes)".to_owned(),
        });
    }

    let total = if (local_size as u32) < payload_size {
        // Cell has an overflow pointer (4 bytes trailing the local payload).
        // We don't need to validate its contents here — the defragmentation
        // copy is a byte-for-byte move, not a logical dereference.
        if local_end + 4 > page.len() || local_end + 4 > usable_size as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "cell extends past usable page size (overflow pointer)".to_owned(),
            });
        }
        local_end + 4 - cell_offset
    } else {
        local_end - cell_offset
    };

    // SQLite format floor (bd-bfnlm): every cell occupies at least
    // MIN_CELL_ALLOCATION bytes of the content area. Clamp at the usable
    // boundary so legacy tight-packed pages never report a span past the page.
    Ok(total
        .max(MIN_CELL_ALLOCATION)
        .min(usable_size as usize - cell_offset))
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
    fn test_content_offset_resolves_sentinel_and_empty_page() {
        // content_offset() is the resolver used on every page load. It clamps
        // both the empty-page case and the "cell_content_offset == 0 means
        // 65536" sentinel to usable_size, and otherwise returns the stored
        // offset verbatim. test_page_header_content_offset_zero_means_65536 only
        // checks the parse/write round-trip of the sentinel value; the resolver
        // method itself was never invoked by a test.
        let usable = 4096u32;

        // Branch 1: an empty page (cell_count == 0) resolves to usable_size even
        // when the stored offset is a normal in-range value.
        let empty = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_offset: 1234,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        assert_eq!(empty.content_offset(usable), usable as usize);

        // Branch 2: a non-empty page carrying the 65536 sentinel (on-disk 0)
        // also resolves to usable_size.
        let sentinel = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 5,
            cell_content_offset: 65536,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        assert_eq!(sentinel.content_offset(usable), usable as usize);

        // Branch 3: a non-empty page with a real in-range offset returns it
        // unchanged.
        let normal = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 5,
            cell_content_offset: 3800,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        assert_eq!(normal.content_offset(usable), 3800);
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

    #[test]
    fn test_read_cell_pointers_rejects_non_empty_page_with_65536_content_offset() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 1,
            cell_content_offset: 65536,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);

        let err = read_cell_pointers(&page, &header, 0).unwrap_err();
        assert!(err.to_string().contains("starts outside page"));
    }

    #[test]
    fn test_read_cell_pointers_rejects_non_empty_page_with_end_content_offset() {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 1,
            cell_content_offset: 4096,
            fragmented_free_bytes: 0,
            right_child: None,
        };

        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);

        let err = read_cell_pointers(&page, &header, 0).unwrap_err();
        assert!(err.to_string().contains("starts outside page"));
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

    #[test]
    fn test_payload_overflow_threshold_and_clamp_across_page_sizes() {
        // The SQLite payload thresholds must hold for every supported usable size,
        // not just 4096. Hand-anchored conformance values (independently computed
        // from the file-format formulas: table-leaf X = U-35; other X =
        // (U-12)*64/255 - 23; M = (U-12)*32/255 - 23):
        assert_eq!(max_local_payload(512, BtreePageType::LeafTable), 477);
        assert_eq!(max_local_payload(1024, BtreePageType::LeafTable), 989);
        assert_eq!(max_local_payload(65536, BtreePageType::LeafTable), 65_501);
        assert_eq!(max_local_payload(512, BtreePageType::LeafIndex), 102);
        assert_eq!(min_local_payload(512), 39);

        for &usable in &[512u32, 1024, 4096, 65536] {
            for &(page_type, label) in &[
                (BtreePageType::LeafTable, "table-leaf"),
                (BtreePageType::LeafIndex, "leaf-index"),
            ] {
                let max_local = max_local_payload(usable, page_type);
                let min_local = min_local_payload(usable);
                assert!(min_local < max_local, "U={usable} {label}: expect M < X");

                // Exact overflow boundary: a payload of exactly X stays fully
                // local; X+1 is the first size that overflows.
                assert!(
                    !has_overflow(max_local, usable, page_type),
                    "U={usable} {label}"
                );
                assert!(
                    has_overflow(max_local + 1, usable, page_type),
                    "U={usable} {label}"
                );
                assert_eq!(
                    local_payload_size(max_local, usable, page_type),
                    max_local,
                    "U={usable} {label}: payload == X must be entirely local"
                );

                // Overflowing payloads keep a local portion clamped to [M, X] and
                // strictly smaller than the total payload.
                for &payload in &[max_local + 1, max_local + 100, usable, usable * 4] {
                    let local = local_payload_size(payload, usable, page_type);
                    assert!(
                        (min_local..=max_local).contains(&local),
                        "U={usable} {label} P={payload}: local {local} not in [{min_local},{max_local}]"
                    );
                    assert!(
                        local < payload,
                        "U={usable} {label} P={payload}: overflow must spill"
                    );
                }
            }
        }
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
    fn test_read_interior_left_child_parses_be_and_rejects_corruption() {
        // Interior cells begin with a 4-byte big-endian left-child page number.
        let mut page = vec![0u8; 16];
        page[0..4].copy_from_slice(&42u32.to_be_bytes());
        assert_eq!(
            CellRef::read_interior_left_child(&page, 0).unwrap(),
            PageNumber::new(42).unwrap()
        );

        // Big-endian byte order, read at a non-zero offset.
        page[8..12].copy_from_slice(&256u32.to_be_bytes()); // 0x0000_0100
        assert_eq!(
            CellRef::read_interior_left_child(&page, 8).unwrap(),
            PageNumber::new(256).unwrap()
        );

        // A zero left-child pointer is corruption (page 0 is not a valid pgno).
        assert!(matches!(
            CellRef::read_interior_left_child(&[0u8; 4], 0),
            Err(FrankenError::DatabaseCorrupt { .. })
        ));

        // Reading past the end of the page is rejected (cell_offset + 4 > len).
        assert!(matches!(
            CellRef::read_interior_left_child(&[0u8, 0, 0], 0),
            Err(FrankenError::DatabaseCorrupt { .. })
        ));
        assert!(
            matches!(
                CellRef::read_interior_left_child(&page, 13), // 13 + 4 = 17 > 16
                Err(FrankenError::DatabaseCorrupt { .. })
            ),
            "offset too close to the end must be rejected"
        );
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

    // -- OPT-A3 lightweight cell helpers --

    #[test]
    fn test_read_table_leaf_rowid_at_offset_single_byte() {
        let mut page = vec![0u8; 4096];
        let cell_offset = 3900;
        page[cell_offset] = 10; // payload_size varint (1 byte)
        page[cell_offset + 1] = 42; // rowid varint (1 byte)
        let rowid = read_table_leaf_rowid_at_offset(&page, cell_offset).unwrap();
        assert_eq!(rowid, 42);
    }

    #[test]
    fn test_read_table_leaf_rowid_at_offset_multi_byte_varints() {
        let mut page = vec![0u8; 4096];
        let cell_offset = 100;
        let mut pos = cell_offset;
        // payload_size = 1000 (2-byte varint)
        let mut buf = [0u8; 9];
        let ps_len = fsqlite_types::serial_type::write_varint(&mut buf, 1000);
        page[pos..pos + ps_len].copy_from_slice(&buf[..ps_len]);
        pos += ps_len;
        // rowid = 0xDEAD_BEEF (5-byte varint)
        let rid_len = fsqlite_types::serial_type::write_varint(&mut buf, 0xDEAD_BEEF);
        page[pos..pos + rid_len].copy_from_slice(&buf[..rid_len]);

        let rowid = read_table_leaf_rowid_at_offset(&page, cell_offset).unwrap();
        #[allow(clippy::cast_possible_wrap)]
        let expected = 0xDEAD_BEEFu64 as i64;
        assert_eq!(rowid, expected);
    }

    #[test]
    fn test_read_table_leaf_rowid_at_offset_out_of_range() {
        let page = vec![0u8; 16];
        assert!(read_table_leaf_rowid_at_offset(&page, 17).is_none());
    }

    #[test]
    fn test_parse_leaf_table_rowid_reports_out_of_range_offset() {
        let page = vec![0u8; 16];
        let err = CellRef::parse_leaf_table_rowid(&page, 17).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
    }

    #[test]
    fn test_parse_leaf_table_rowid_decodes_rowid_after_payload_size_varint() {
        use fsqlite_types::serial_type::write_varint;

        // Single-byte payload_size + single-byte rowid.
        let mut page = vec![0u8; 64];
        page[10] = 5; // payload_size varint (value < 128 -> 1 byte)
        page[11] = 42; // rowid varint
        assert_eq!(CellRef::parse_leaf_table_rowid(&page, 10).unwrap(), 42);

        // Multi-byte payload_size: parse must skip the whole varint to find the rowid.
        let mut page = vec![0u8; 64];
        let off = 4;
        let n = write_varint(&mut page[off..], 200u64); // payload_size = 200 (2-byte varint)
        let _ = write_varint(&mut page[off + n..], 7u64); // rowid = 7
        assert_eq!(CellRef::parse_leaf_table_rowid(&page, off).unwrap(), 7);

        // Multi-byte rowid is decoded fully.
        let mut page = vec![0u8; 64];
        let n = write_varint(&mut page[0..], 5u64); // payload_size = 5
        let _ = write_varint(&mut page[n..], 300u64); // rowid = 300 (2-byte varint)
        assert_eq!(CellRef::parse_leaf_table_rowid(&page, 0).unwrap(), 300);

        // A large rowid round-trips through the varint decode.
        let mut page = vec![0u8; 64];
        let n = write_varint(&mut page[0..], 1u64);
        let _ = write_varint(&mut page[n..], 1_000_000_000u64);
        assert_eq!(
            CellRef::parse_leaf_table_rowid(&page, 0).unwrap(),
            1_000_000_000
        );
    }

    #[test]
    fn test_cellref_parse_reports_out_of_range_offset() {
        let page = vec![0u8; 16];
        let err = CellRef::parse(&page, 17, BtreePageType::LeafTable, 4096).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
    }

    #[test]
    fn test_cell_on_page_size_fast_matches_cellref_leaf_table_no_overflow() {
        // Build a leaf-table cell, compare sizes.
        let mut page = vec![0u8; 4096];
        let cell_offset = 3900;
        let mut pos = cell_offset;
        page[pos] = 10; // payload_size
        pos += 1;
        page[pos] = 42; // rowid
        pos += 1;
        for i in 0..10 {
            page[pos + i] = (i + 1) as u8;
        }

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::LeafTable, 4096).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, cell_offset);
        let fast =
            cell_on_page_size_fast(&page, cell_offset, BtreePageType::LeafTable, 4096).unwrap();
        assert_eq!(fast, expected);
        assert_eq!(fast, 1 + 1 + 10);
    }

    #[test]
    fn test_cell_on_page_size_fast_interior_table_cell() {
        use fsqlite_types::serial_type::write_varint;
        // Interior-table cell layout: a 4-byte left-child page pointer followed
        // by a rowid varint and NO payload. The existing fast-size tests only
        // cover leaf-table cells; this exercises the InteriorTable early-return
        // path, cross-checked against the CellRef-based size like its siblings.
        let usable: u32 = 4096;
        let mut page = vec![0u8; 4096];
        let cell_offset = 100;
        // 4-byte left child pointer.
        page[cell_offset..cell_offset + 4].copy_from_slice(&7u32.to_be_bytes());
        // rowid varint (300 -> 2 bytes).
        let rowid_len = write_varint(&mut page[cell_offset + 4..], 300u64);

        let cell =
            CellRef::parse(&page, cell_offset, BtreePageType::InteriorTable, usable).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, cell_offset);
        let fast = cell_on_page_size_fast(&page, cell_offset, BtreePageType::InteriorTable, usable)
            .unwrap();
        assert_eq!(fast, expected);
        assert_eq!(
            fast,
            4 + rowid_len,
            "interior-table cell size = 4-byte child pointer + rowid varint"
        );
    }

    #[test]
    fn test_cell_on_page_size_fast_index_cells_have_no_rowid() {
        use fsqlite_types::serial_type::write_varint;
        // Index cells (leaf or interior) carry a payload_size varint + payload
        // but NO rowid, unlike table cells. Interior-index cells additionally
        // have the 4-byte left-child prefix. The existing fast-size tests cover
        // only leaf-table; pin the two index paths, cross-checked against the
        // CellRef-based size. payload_size 10 is far below the index max-local
        // threshold (~1002 at usable=4096), so the whole payload stays local.
        let usable: u32 = 4096;

        // Leaf-index cell: payload_size varint + payload, no child pointer.
        let mut page = vec![0u8; 4096];
        let off = 50;
        let ps_len = write_varint(&mut page[off..], 10u64); // payload_size = 10
        let cell = CellRef::parse(&page, off, BtreePageType::LeafIndex, usable).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, off);
        let fast = cell_on_page_size_fast(&page, off, BtreePageType::LeafIndex, usable).unwrap();
        assert_eq!(fast, expected);
        assert_eq!(
            fast,
            ps_len + 10,
            "leaf-index size = payload_size varint + payload"
        );

        // Interior-index cell: 4-byte left child + payload_size varint + payload.
        let mut page = vec![0u8; 4096];
        let off = 50;
        page[off..off + 4].copy_from_slice(&9u32.to_be_bytes());
        let ps_len = write_varint(&mut page[off + 4..], 10u64);
        let cell = CellRef::parse(&page, off, BtreePageType::InteriorIndex, usable).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, off);
        let fast =
            cell_on_page_size_fast(&page, off, BtreePageType::InteriorIndex, usable).unwrap();
        assert_eq!(fast, expected);
        assert_eq!(
            fast,
            4 + ps_len + 10,
            "interior-index size = child pointer + payload_size varint + payload"
        );
    }

    #[test]
    fn test_cell_on_page_size_fast_matches_cellref_leaf_table_with_overflow() {
        let mut page = vec![0u8; 4096];
        let cell_offset = 0;
        let payload_size: u32 = 5000;
        let usable_size: u32 = 4096;

        let mut buf = [0u8; 9];
        let ps_len = fsqlite_types::serial_type::write_varint(&mut buf, u64::from(payload_size));
        page[cell_offset..cell_offset + ps_len].copy_from_slice(&buf[..ps_len]);
        let rowid_offset = cell_offset + ps_len;
        page[rowid_offset] = 1;
        let rowid_len = 1;
        let payload_offset = rowid_offset + rowid_len;
        let local = local_payload_size(payload_size, usable_size, BtreePageType::LeafTable);

        for i in 0..local as usize {
            if payload_offset + i < page.len() {
                page[payload_offset + i] = (i & 0xFF) as u8;
            }
        }
        let overflow_ptr_offset = payload_offset + local as usize;
        if overflow_ptr_offset + 4 <= page.len() {
            page[overflow_ptr_offset..overflow_ptr_offset + 4]
                .copy_from_slice(&99u32.to_be_bytes());
        }

        let cell =
            CellRef::parse(&page, cell_offset, BtreePageType::LeafTable, usable_size).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, cell_offset);
        let fast =
            cell_on_page_size_fast(&page, cell_offset, BtreePageType::LeafTable, usable_size)
                .unwrap();
        assert_eq!(fast, expected);
    }

    #[test]
    fn test_cell_on_page_size_fast_matches_cellref_interior_table() {
        let mut page = vec![0u8; 4096];
        let cell_offset = 2000;
        page[cell_offset..cell_offset + 4].copy_from_slice(&7u32.to_be_bytes());
        page[cell_offset + 4] = 100; // rowid

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::InteriorTable, 4096).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, cell_offset);
        let fast =
            cell_on_page_size_fast(&page, cell_offset, BtreePageType::InteriorTable, 4096).unwrap();
        assert_eq!(fast, expected);
        assert_eq!(fast, 4 + 1);
    }

    #[test]
    fn test_cell_on_page_size_fast_matches_cellref_interior_index() {
        let mut page = vec![0u8; 4096];
        let cell_offset = 2500;
        page[cell_offset..cell_offset + 4].copy_from_slice(&15u32.to_be_bytes());
        page[cell_offset + 4] = 8; // payload_size
        for i in 0..8 {
            page[cell_offset + 5 + i] = (i + 20) as u8;
        }

        let cell = CellRef::parse(&page, cell_offset, BtreePageType::InteriorIndex, 4096).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, cell_offset);
        let fast =
            cell_on_page_size_fast(&page, cell_offset, BtreePageType::InteriorIndex, 4096).unwrap();
        assert_eq!(fast, expected);
        assert_eq!(fast, 4 + 1 + 8);
    }

    #[test]
    fn test_cell_on_page_size_fast_matches_cellref_leaf_index() {
        let mut page = vec![0u8; 4096];
        let cell_offset = 3500;
        page[cell_offset] = 5;
        for i in 0..5 {
            page[cell_offset + 1 + i] = (i + 10) as u8;
        }
        let cell = CellRef::parse(&page, cell_offset, BtreePageType::LeafIndex, 4096).unwrap();
        let expected = crate::payload::cell_on_page_size(&cell, cell_offset);
        let fast =
            cell_on_page_size_fast(&page, cell_offset, BtreePageType::LeafIndex, 4096).unwrap();
        assert_eq!(fast, expected);
        assert_eq!(fast, 1 + 5);
    }

    #[test]
    fn test_cell_on_page_size_fast_floors_to_min_cell_allocation() {
        // bd-bfnlm: a leaf-index cell encoding in 3 bytes (payload_size=2,
        // e.g. a single-column WITHOUT ROWID key using serial type 9) is
        // accounted at the 4-byte SQLite allocation floor.
        let mut page = vec![0u8; 4096];
        let off = 3500;
        page[off] = 2; // payload_size = 2
        page[off + 1] = 0x02; // record header length
        page[off + 2] = 0x09; // serial type 9 (integer 1, zero body bytes)
        let fast = cell_on_page_size_fast(&page, off, BtreePageType::LeafIndex, 4096).unwrap();
        assert_eq!(fast, MIN_CELL_ALLOCATION);

        // Legacy tight-packed page: the same cell sitting 3 bytes before the
        // usable boundary clamps at the boundary instead of reporting a span
        // past the page.
        let mut page = vec![0u8; 4096];
        let off = 4093;
        page[off] = 2;
        page[off + 1] = 0x02;
        page[off + 2] = 0x09;
        let fast = cell_on_page_size_fast(&page, off, BtreePageType::LeafIndex, 4096).unwrap();
        assert_eq!(fast, 3);
    }

    #[test]
    fn test_cell_on_page_size_fast_rejects_out_of_bounds() {
        let page = vec![0u8; 16];
        let err = cell_on_page_size_fast(&page, 17, BtreePageType::LeafTable, 4096).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
    }

    #[test]
    fn test_cell_on_page_size_fast_rejects_truncated_interior_child() {
        let page = vec![0u8; 3]; // 3 bytes, can't hold 4-byte left_child.
        let err = cell_on_page_size_fast(&page, 0, BtreePageType::InteriorTable, 4096).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
    }

    #[test]
    fn test_cell_on_page_size_fast_rejects_payload_past_page() {
        let mut page = vec![0u8; 64];
        let cell_offset = 60;
        page[cell_offset] = 10;
        page[cell_offset + 1] = 1;
        // Leaf-table cell claims 10 bytes but only 2 bytes remain.
        let err =
            cell_on_page_size_fast(&page, cell_offset, BtreePageType::LeafTable, 64).unwrap_err();
        assert!(matches!(err, FrankenError::DatabaseCorrupt { .. }));
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

    // ------------------------------------------------------------------
    // Microbench (#[ignore]) — parse hot path
    // ------------------------------------------------------------------
    //
    // Compares the optimized `BtreePageHeader::parse` against a reference
    // implementation that mirrors the pre-optimisation per-byte indexing
    // shape (`let h = &page[off..]; h[0]; h[1]; ...`). Both arms run in
    // the same release build so allocator / cache state is shared; the
    // delta isolates the bounds-check elision + `#[inline]`d header-offset
    // folding.

    #[inline(never)]
    fn parse_header_naive(page: &[u8], header_offset: usize) -> Result<BtreePageHeader> {
        let remaining = page.len().saturating_sub(header_offset);
        if remaining < BTREE_LEAF_HEADER_SIZE as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page too small for B-tree header: {remaining} bytes at offset {header_offset}",
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
        Ok(BtreePageHeader {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_offset,
            fragmented_free_bytes,
            right_child,
        })
    }

    fn build_leaf_page() -> Vec<u8> {
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: 17,
            cell_content_offset: 3800,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);
        page
    }

    fn build_interior_page() -> Vec<u8> {
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 9,
            cell_content_offset: 2048,
            fragmented_free_bytes: 0,
            right_child: PageNumber::new(7),
        };
        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);
        page
    }

    #[test]
    #[ignore = "microbench; run with --profile release-perf -- --ignored --nocapture bench_parse_page_header"]
    fn bench_parse_page_header() {
        use std::hint::black_box;
        use std::time::Instant;

        let leaf = build_leaf_page();
        let interior = build_interior_page();
        let pages: [&[u8]; 4] = [&leaf, &interior, &leaf, &interior];

        let iters: u64 = 4_000_000;
        let trials = 5;

        for trial in 0..trials {
            // Optimized parse (current `BtreePageHeader::parse`).
            let t = Instant::now();
            for i in 0..iters {
                let page = pages[(i as usize) & 3];
                let header = BtreePageHeader::parse(black_box(page), black_box(0)).unwrap();
                black_box(header);
            }
            let opt = t.elapsed();

            // Naive baseline (per-byte indexing, no `#[inline]`).
            let t = Instant::now();
            for i in 0..iters {
                let page = pages[(i as usize) & 3];
                let header = parse_header_naive(black_box(page), black_box(0)).unwrap();
                black_box(header);
            }
            let naive = t.elapsed();

            #[allow(clippy::cast_precision_loss)]
            let opt_ns = opt.as_nanos() as f64 / iters as f64;
            #[allow(clippy::cast_precision_loss)]
            let naive_ns = naive.as_nanos() as f64 / iters as f64;
            let delta_pct = (opt_ns - naive_ns) / naive_ns * 100.0;

            println!(
                "[trial {trial}] parse: optimized={opt_ns:.2} ns naive={naive_ns:.2} ns \
                 delta={delta_pct:+.1}% (mixed leaf/interior, n={iters})",
            );
        }
    }

    // ------------------------------------------------------------------
    // Microbench (#[ignore]) — cell pointer array read/write
    // ------------------------------------------------------------------
    //
    // Compares the optimized `read_cell_pointers_into` /
    // `write_cell_pointers` against `#[inline(never)]` reference
    // implementations that mirror the pre-change shapes. Both arms run
    // in the same release build for a same-cache comparison.

    #[inline(never)]
    #[allow(clippy::incompatible_msrv)]
    fn read_cell_pointers_into_naive(
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
                detail: "cell pointer array extends past page".to_owned(),
            });
        }
        if ptr_array_end > header.cell_content_offset as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "cell pointer array overlaps with cell content area".to_owned(),
            });
        }
        buf.clear();
        buf.reserve(count);
        let ptr_bytes = &page[ptr_array_start..ptr_array_end];
        let (chunks, remainder) = ptr_bytes.as_chunks::<2>();
        debug_assert!(remainder.is_empty());
        buf.extend(chunks.iter().map(|chunk| u16::from_be_bytes(*chunk)));
        Ok(())
    }

    #[inline(never)]
    fn write_cell_pointers_naive(
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

    #[test]
    #[ignore = "microbench; run with --profile release-perf -- --ignored --nocapture bench_cell_pointers"]
    fn bench_cell_pointers() {
        use std::hint::black_box;
        use std::time::Instant;

        // Realistic leaf page with 64 cells (typical mid-sized leaf).
        let cell_count: usize = 64;
        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            #[allow(clippy::cast_possible_truncation)]
            cell_count: cell_count as u16,
            cell_content_offset: 3000,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);
        let pointers: Vec<u16> = (0..cell_count).map(|i| 3000u16 + (i as u16) * 16).collect();
        write_cell_pointers(&mut page, 0, &header, &pointers);

        let iters: u64 = 200_000;
        let trials = 5;
        let mut buf: Vec<u16> = Vec::with_capacity(cell_count);

        for trial in 0..trials {
            // Read — optimized.
            let t = Instant::now();
            for _ in 0..iters {
                read_cell_pointers_into(black_box(&page), black_box(&header), 0, &mut buf).unwrap();
                black_box(&buf);
            }
            let read_opt = t.elapsed();

            // Read — naive.
            let t = Instant::now();
            for _ in 0..iters {
                read_cell_pointers_into_naive(black_box(&page), black_box(&header), 0, &mut buf)
                    .unwrap();
                black_box(&buf);
            }
            let read_naive = t.elapsed();

            // Write — optimized.
            let mut wpage = vec![0u8; 4096];
            header.write(&mut wpage, 0);
            let t = Instant::now();
            for _ in 0..iters {
                write_cell_pointers(
                    black_box(&mut wpage),
                    0,
                    black_box(&header),
                    black_box(&pointers),
                );
            }
            let write_opt = t.elapsed();

            // Write — naive.
            let mut wpage = vec![0u8; 4096];
            header.write(&mut wpage, 0);
            let t = Instant::now();
            for _ in 0..iters {
                write_cell_pointers_naive(
                    black_box(&mut wpage),
                    0,
                    black_box(&header),
                    black_box(&pointers),
                );
            }
            let write_naive = t.elapsed();

            #[allow(clippy::cast_precision_loss)]
            let total = (iters as f64) * (cell_count as f64);
            #[allow(clippy::cast_precision_loss)]
            let read_opt_ns = read_opt.as_nanos() as f64 / total;
            #[allow(clippy::cast_precision_loss)]
            let read_naive_ns = read_naive.as_nanos() as f64 / total;
            let read_delta = (read_opt_ns - read_naive_ns) / read_naive_ns * 100.0;
            #[allow(clippy::cast_precision_loss)]
            let write_opt_ns = write_opt.as_nanos() as f64 / total;
            #[allow(clippy::cast_precision_loss)]
            let write_naive_ns = write_naive.as_nanos() as f64 / total;
            let write_delta = (write_opt_ns - write_naive_ns) / write_naive_ns * 100.0;

            println!(
                "[trial {trial}] read:  opt={read_opt_ns:.3} ns/ptr  naive={read_naive_ns:.3} ns/ptr  delta={read_delta:+.1}% \
                 | write: opt={write_opt_ns:.3} ns/ptr  naive={write_naive_ns:.3} ns/ptr  delta={write_delta:+.1}% \
                 ({cell_count} ptrs/page, n={iters})",
            );
        }
    }

    // ------------------------------------------------------------------
    // Microbench (#[ignore]) — single cell-pointer read pattern
    // ------------------------------------------------------------------
    //
    // Mirrors the shape of `BtCursor::read_cell_pointer_inline`. The
    // cursor function showed up as a separate ~0.37% MT8 self-time
    // symbol on the dyn-IO monomorphization in
    // `tests/artifacts/perf/profiling-mt-mvcc-20260424T161631Z/perf_mt8_flat.txt`
    // because the in-body `format!` blocked inlining and LLVM was
    // re-checking each `page[ptr_offset + i]` index after a separate
    // `ptr_offset + 2 > page.len()` guard. The optimised arm here uses
    // a single `page.get(..).try_into::<&[u8; 2]>()` (one bounds
    // check) and outlines the corruption-error builder via
    // `#[cold] #[inline(never)]`; the naive arm reproduces the
    // pre-change shape.

    #[cold]
    #[inline(never)]
    fn cell_pointer_oob_bench(
        cell_idx: u16,
        page_no: PageNumber,
        ptr_offset: usize,
        page_len: usize,
    ) -> FrankenError {
        FrankenError::DatabaseCorrupt {
            detail: format!(
                "cell pointer {cell_idx} extends past page {} (ptr_offset={ptr_offset} len={page_len})",
                page_no.get(),
            ),
        }
    }

    #[inline]
    fn read_cell_pointer_inline_optimized(
        page: &[u8],
        page_no: PageNumber,
        header_size: usize,
        cell_idx: u16,
    ) -> Result<u16> {
        let header_offset = header_offset_for_page(page_no);
        let ptr_offset = header_offset + header_size + (cell_idx as usize) * 2;
        let bytes: &[u8; 2] = page
            .get(ptr_offset..ptr_offset + 2)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| cell_pointer_oob_bench(cell_idx, page_no, ptr_offset, page.len()))?;
        Ok(u16::from_be_bytes(*bytes))
    }

    #[inline(never)]
    fn read_cell_pointer_inline_naive(
        page: &[u8],
        page_no: PageNumber,
        header_size: usize,
        cell_idx: u16,
    ) -> Result<u16> {
        let header_offset = header_offset_for_page(page_no);
        let ptr_offset = header_offset + header_size + (cell_idx as usize) * 2;
        if ptr_offset + 2 > page.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell pointer {cell_idx} extends past page {} (ptr_offset={ptr_offset} len={})",
                    page_no.get(),
                    page.len()
                ),
            });
        }
        Ok(u16::from_be_bytes([page[ptr_offset], page[ptr_offset + 1]]))
    }

    #[test]
    #[ignore = "microbench; run with --profile release-perf -- --ignored --nocapture bench_read_cell_pointer_inline"]
    fn bench_read_cell_pointer_inline() {
        use std::hint::black_box;
        use std::time::Instant;

        // Realistic interior-page descent target: 32 cell pointers, header
        // size 12 (interior page header), page_no 7 (non-page-1 path so
        // header_offset_for_page folds to 0 just like the production
        // descent loop).
        let cell_count: usize = 32;
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            #[allow(clippy::cast_possible_truncation)]
            cell_count: cell_count as u16,
            cell_content_offset: 3000,
            fragmented_free_bytes: 0,
            right_child: PageNumber::new(99),
        };
        let mut page = vec![0u8; 4096];
        header.write(&mut page, 0);
        // Populate the cell-pointer array with deterministic offsets so
        // the read result is predictable but non-trivial.
        let header_size = usize::from(header.page_type.header_size());
        for i in 0..cell_count {
            let off = header_size + i * 2;
            #[allow(clippy::cast_possible_truncation)]
            let payload = (3000 + i as u16 * 10).to_be_bytes();
            page[off] = payload[0];
            page[off + 1] = payload[1];
        }

        let page_no = PageNumber::new(7).unwrap();
        let iters: u64 = 4_000_000;
        let trials = 5;

        for trial in 0..trials {
            // Optimized — array-conversion + cold-outlined error.
            let t = Instant::now();
            let mut sum_opt = 0u64;
            for i in 0..iters {
                #[allow(clippy::cast_possible_truncation)]
                let idx = (i as usize % cell_count) as u16;
                let v = read_cell_pointer_inline_optimized(
                    black_box(&page),
                    black_box(page_no),
                    black_box(header_size),
                    black_box(idx),
                )
                .unwrap();
                sum_opt = sum_opt.wrapping_add(u64::from(v));
            }
            let opt = t.elapsed();
            black_box(sum_opt);

            // Naive — guard + per-index access + in-body format!.
            let t = Instant::now();
            let mut sum_naive = 0u64;
            for i in 0..iters {
                #[allow(clippy::cast_possible_truncation)]
                let idx = (i as usize % cell_count) as u16;
                let v = read_cell_pointer_inline_naive(
                    black_box(&page),
                    black_box(page_no),
                    black_box(header_size),
                    black_box(idx),
                )
                .unwrap();
                sum_naive = sum_naive.wrapping_add(u64::from(v));
            }
            let naive = t.elapsed();
            black_box(sum_naive);

            assert_eq!(sum_opt, sum_naive);

            #[allow(clippy::cast_precision_loss)]
            let opt_ns = opt.as_nanos() as f64 / iters as f64;
            #[allow(clippy::cast_precision_loss)]
            let naive_ns = naive.as_nanos() as f64 / iters as f64;
            let delta_pct = (opt_ns - naive_ns) / naive_ns * 100.0;

            println!(
                "[trial {trial}] read_cell_pointer_inline: optimized={opt_ns:.3} ns naive={naive_ns:.3} ns \
                 delta={delta_pct:+.1}% (n={iters}, cells={cell_count})",
            );
        }
    }
}
