//! B-tree page balancing algorithms (§11, bd-2kvo).
//!
//! When a page overflows (cell insertion causes it to exceed capacity) or
//! underflows (cell deletion leaves it below threshold), the B-tree must
//! be rebalanced. This module provides the three balance algorithms from
//! SQLite:
//!
//! - [`balance_deeper`]: Root page split — increases tree depth by 1.
//! - `balance_nonroot`: 3-way sibling rebalancing.
//! - [`balance_quick`]: Fast-path leaf append (rightmost cell on rightmost child).
//!
//! The central entry point dispatches to the appropriate algorithm based on
//! the cursor position and page state.

use crate::cell::{
    BtreePageHeader, BtreePageType, CellRef, header_offset_for_page, read_cell_pointers,
    write_cell_pointers,
};
use crate::cursor::PageWriter;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::{BTREE_LEAF_HEADER_SIZE, CELL_POINTER_SIZE};
use fsqlite_types::serial_type::write_varint;

/// Maximum number of sibling pages involved in a single balance_nonroot.
/// SQLite uses NN=1 (1 neighbor each side) → NB = 2*NN+1 = 3.
const NB: usize = 3;

/// Maximum cells that can be gathered across NB pages plus dividers.
/// Conservative upper bound: max page size (65536) / min cell size (~4 bytes) * NB (3).
/// We use 65536 as a safe static limit.
const MAX_GATHERED_CELLS: usize = 65_536;

/// Result of a balancing operation that may require updating higher levels.
#[derive(Debug)]
pub enum BalanceResult {
    /// Balancing completed; no further parent updates are required.
    Done,
    /// The updated interior page overflowed and was split into multiple pages.
    ///
    /// The caller must insert `new_dividers` into the parent-of-this-page to
    /// reference the additional pages in `new_pgnos[1..]`. `new_pgnos[0]` is
    /// always the original page number (rewritten in-place).
    Split {
        new_pgnos: Vec<PageNumber>,
        new_dividers: Vec<(PageNumber, Vec<u8>)>,
    },
}

type SplitPagesAndDividers = (Vec<PageNumber>, Vec<(PageNumber, Vec<u8>)>);

// ---------------------------------------------------------------------------
// Gathered cell descriptor
// ---------------------------------------------------------------------------

/// A descriptor for a cell gathered during balance_nonroot.
///
/// Cells are collected from all sibling pages and divider cells from the
/// parent into a flat array, then redistributed across new pages.
#[derive(Debug, Clone)]
struct GatheredCell {
    /// The raw cell bytes (complete cell as stored on page).
    data: Vec<u8>,
    /// Size of this cell including left-child pointer, varints, local
    /// payload, and overflow pointer.
    #[allow(dead_code)]
    size: u16,
}

// ---------------------------------------------------------------------------
// balance_deeper: root page split
// ---------------------------------------------------------------------------

/// Split the root page by creating a new child and pushing the root's
/// contents down one level.
///
/// After this call:
/// - The root page becomes an interior page with zero cells.
/// - The new child page holds all the former root's cells.
/// - The root's right_child pointer points to the new child.
/// - The caller should then call `balance_nonroot` on the child
///   to redistribute cells properly.
///
/// Returns the page number of the new child.
#[allow(clippy::too_many_lines)]
pub fn balance_deeper<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    root_page_no: PageNumber,
    usable_size: u32,
) -> Result<PageNumber> {
    let root_data = writer.read_page(cx, root_page_no)?;
    let root_offset = header_offset_for_page(root_page_no);
    let root_header = BtreePageHeader::parse(&root_data, root_offset)?;

    // Extract cells from the root page safely to avoid offset shifting bugs.
    let cell_ptrs = read_cell_pointers(&root_data, &root_header, root_offset)?;
    let mut root_cells: Vec<GatheredCell> = Vec::with_capacity(cell_ptrs.len());
    for &ptr in &cell_ptrs {
        let cell_offset = usize::from(ptr);
        let cell_ref = CellRef::parse(&root_data, cell_offset, root_header.page_type, usable_size)?;
        let cell_end = cell_offset + cell_on_page_size_from_ref(&cell_ref, cell_offset);
        let data = root_data[cell_offset..cell_end].to_vec();
        let size = u16::try_from(data.len()).map_err(|_| {
            FrankenError::Internal("cell too large during balance_deeper".to_owned())
        })?;
        root_cells.push(GatheredCell { data, size });
    }

    // Allocate a new child page.
    let child_pgno = writer.allocate_page(cx)?;
    let child_offset = header_offset_for_page(child_pgno);

    // Build the child page using the extracted cells.
    let child_data = build_page(
        &root_cells,
        root_header.page_type,
        child_offset,
        usable_size,
        root_header.right_child,
    )?;

    writer.write_page(cx, child_pgno, &child_data)?;

    // Clear the root page and make it an interior page pointing to the child.
    let mut new_root = vec![0u8; usable_size as usize];
    let new_root_type = if root_header.page_type.is_table() {
        BtreePageType::InteriorTable
    } else {
        BtreePageType::InteriorIndex
    };

    let new_root_header = BtreePageHeader {
        page_type: new_root_type,
        first_freeblock: 0,
        cell_count: 0,
        cell_content_offset: usable_size,
        fragmented_free_bytes: 0,
        right_child: Some(child_pgno),
    };
    new_root_header.write(&mut new_root, root_offset);

    // Preserve the database header on page 1.
    if root_offset > 0 {
        new_root[..root_offset].copy_from_slice(&root_data[..root_offset]);
    }

    writer.write_page(cx, root_page_no, &new_root)?;

    Ok(child_pgno)
}

// ---------------------------------------------------------------------------
// balance_quick: fast-path leaf append
// ---------------------------------------------------------------------------

/// Fast-path balance for appending a single cell to the rightmost leaf.
///
/// Preconditions (caller must verify):
/// - The leaf page is an intkey leaf (table B-tree).
/// - The overflow cell is the rightmost cell (appended at end).
/// - The parent's rightmost child points to this leaf.
///
/// This allocates a new sibling page, moves the overflow cell to it,
/// creates a divider in the parent, and updates the parent's right-child.
///
/// `parent_page_no` is the parent of the leaf.
/// `leaf_page_no` is the overfull leaf.
/// `overflow_cell` is the raw cell bytes that need to move.
/// `overflow_rowid` is the rowid of the overflow cell.
///
/// Returns `Ok(Some(new_page))` if successful, or `Ok(None)` if the
/// parent page is full and cannot accept the divider cell.
pub fn balance_quick<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    parent_page_no: PageNumber,
    leaf_page_no: PageNumber,
    overflow_cell: &[u8],
    overflow_rowid: i64,
    usable_size: u32,
) -> Result<Option<PageNumber>> {
    // Read parent page to check for space.
    let parent_data = writer.read_page(cx, parent_page_no)?;
    let parent_offset = header_offset_for_page(parent_page_no);
    let parent_header = BtreePageHeader::parse(&parent_data, parent_offset)?;

    // Calculate required space for the divider cell.
    // Divider: [4-byte child ptr] [rowid varint]
    // We don't know the exact divider rowid yet (depends on leaf content),
    // but we can upper-bound it or estimate it.
    // Actually, we can read the leaf to get the exact divider rowid,
    // OR we can just use a conservative estimate (max varint size = 9).
    // Let's read the leaf first, as we need it anyway for the divider.
    // But `balance_quick` is an optimization, avoiding the leaf read if parent is full is better?
    // No, we need to know if parent is full.
    // Let's assume max divider size (4 + 9 = 13) + 2 byte pointer = 15 bytes.
    // If parent has < 15 bytes free, abort.

    let parent_used = parent_offset
        + usize::from(parent_header.page_type.header_size())
        + (usize::from(parent_header.cell_count) * 2);

    let free_space = (parent_header.cell_content_offset as usize).saturating_sub(parent_used);

    // We need space for:
    // 1. The new cell pointer (2 bytes)
    // 2. The divider cell (4 bytes + varint). Max varint is 9.
    // Total max requirement: 2 + 4 + 9 = 15 bytes.
    if free_space < 15 {
        return Ok(None);
    }

    // Allocate new sibling page.
    let new_pgno = writer.allocate_page(cx)?;
    let mut new_page = vec![0u8; usable_size as usize];
    let new_offset = header_offset_for_page(new_pgno);

    // Initialize as leaf table page with one cell.
    let cell_size = overflow_cell.len();
    let Some(content_start) = (usable_size as usize).checked_sub(cell_size) else {
        return Ok(None);
    };
    if content_start < new_offset + BTREE_LEAF_HEADER_SIZE as usize + 2 {
        return Ok(None); // Cell too large, falls back to standard balance.
    }

    let new_header = BtreePageHeader {
        page_type: BtreePageType::LeafTable,
        first_freeblock: 0,
        cell_count: 1,
        #[allow(clippy::cast_possible_truncation)]
        cell_content_offset: content_start as u32,
        fragmented_free_bytes: 0,
        right_child: None,
    };
    new_header.write(&mut new_page, new_offset);

    // Write cell pointer.
    #[allow(clippy::cast_possible_truncation)]
    let cell_ptr = content_start as u16;
    let ptr_offset = new_offset + BTREE_LEAF_HEADER_SIZE as usize;
    new_page[ptr_offset..ptr_offset + 2].copy_from_slice(&cell_ptr.to_be_bytes());

    // Write cell content.
    new_page[content_start..content_start + cell_size].copy_from_slice(overflow_cell);

    writer.write_page(cx, new_pgno, &new_page)?;

    // Read the existing leaf to find the divider key (its rightmost rowid).
    let leaf_data = writer.read_page(cx, leaf_page_no)?;
    let leaf_offset = header_offset_for_page(leaf_page_no);
    let leaf_header = BtreePageHeader::parse(&leaf_data, leaf_offset)?;
    let leaf_ptrs = read_cell_pointers(&leaf_data, &leaf_header, leaf_offset)?;

    let divider_rowid = if leaf_header.cell_count > 0 {
        let last_ptr = leaf_ptrs[leaf_header.cell_count as usize - 1] as usize;
        let last_cell =
            CellRef::parse(&leaf_data, last_ptr, BtreePageType::LeafTable, usable_size)?;
        last_cell
            .rowid
            .ok_or_else(|| FrankenError::internal("leaf table cell missing rowid"))?
    } else {
        // Edge case: leaf is somehow empty before the overflow.
        // Use the overflow rowid minus 1 (the divider separates left from right).
        overflow_rowid.saturating_sub(1)
    };

    // Build a divider cell for the parent: [4-byte child ptr] [rowid varint].
    let mut divider = [0u8; 13]; // 4 + up to 9 varint bytes
    divider[0..4].copy_from_slice(&leaf_page_no.get().to_be_bytes());
    #[allow(clippy::cast_sign_loss)]
    let rowid_u64 = divider_rowid as u64;
    let varint_size = write_varint(&mut divider[4..], rowid_u64);
    let divider_size = 4 + varint_size;

    // Insert divider into parent and update right_child.
    // We already checked for space, so this should succeed unless concurrent modification
    // (which shouldn't happen with exclusive latching).
    insert_cell_into_page(
        cx,
        writer,
        parent_page_no,
        usable_size,
        &divider[..divider_size],
    )?;

    // Update parent's right_child to point to new sibling.
    let mut parent_data = writer.read_page(cx, parent_page_no)?;
    let parent_offset = header_offset_for_page(parent_page_no);
    // Right-child is at header_offset + 8.
    parent_data[parent_offset + 8..parent_offset + 12]
        .copy_from_slice(&new_pgno.get().to_be_bytes());
    writer.write_page(cx, parent_page_no, &parent_data)?;

    Ok(Some(new_pgno))
}

// ---------------------------------------------------------------------------
// balance_nonroot: 3-way sibling rebalancing
// ---------------------------------------------------------------------------

/// Rebalance a non-root page by redistributing cells across its siblings.
///
/// `parent_page_no` is the parent page containing the child pointers.
/// `child_idx` is the index in the parent's children that needs rebalancing
/// (0 = leftmost child, cell_count = rightmost/right_child).
/// `overflow_cells` are cells that didn't fit on the page (if any).
///
/// This gathers all cells from up to 3 sibling pages plus divider cells
/// from the parent, computes a new distribution, and writes the result.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) fn balance_nonroot<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    parent_page_no: PageNumber,
    child_idx: usize,
    overflow_cells: &[Vec<u8>],
    overflow_insert_idx: usize,
    usable_size: u32,
    parent_is_root: bool,
) -> Result<BalanceResult> {
    let parent_data = writer.read_page(cx, parent_page_no)?;
    let parent_offset = header_offset_for_page(parent_page_no);
    let parent_header = BtreePageHeader::parse(&parent_data, parent_offset)?;
    let parent_ptrs = read_cell_pointers(&parent_data, &parent_header, parent_offset)?;

    let total_children = parent_header.cell_count as usize + 1;

    // Determine the range of siblings to balance.
    // We want at most NB (3) siblings centered on child_idx.
    let (first_child, sibling_count) = compute_sibling_range(child_idx, total_children);

    // Collect sibling page numbers and divider cells from parent.
    let mut sibling_pgnos: Vec<PageNumber> = Vec::with_capacity(sibling_count);
    let mut divider_cells: Vec<Vec<u8>> = Vec::with_capacity(sibling_count.saturating_sub(1));

    for i in 0..sibling_count {
        let abs_idx = first_child + i;
        let pgno = child_page_number(
            &parent_data,
            &parent_header,
            &parent_ptrs,
            parent_offset,
            abs_idx,
            usable_size,
        )?;
        sibling_pgnos.push(pgno);

        // Collect divider cell between sibling[i] and sibling[i+1].
        if i < sibling_count - 1 {
            let div_idx = first_child + i; // Parent cell index for this divider.
            let div_offset = parent_ptrs[div_idx] as usize;
            let div_cell = CellRef::parse(
                &parent_data,
                div_offset,
                parent_header.page_type,
                usable_size,
            )?;
            // Extract the raw divider cell bytes.
            let div_end = div_offset + cell_on_page_size_from_ref(&div_cell, div_offset);
            divider_cells.push(parent_data[div_offset..div_end].to_vec());
        }
    }

    // Read all sibling pages and gather cells.
    let mut all_cells: Vec<GatheredCell> = Vec::new();
    let mut sibling_types: Vec<BtreePageType> = Vec::new();
    let mut old_right_children: Vec<Option<PageNumber>> = Vec::new();

    for (sib_idx, &pgno) in sibling_pgnos.iter().enumerate() {
        let page_data = writer.read_page(cx, pgno)?;
        let page_offset = header_offset_for_page(pgno);
        let page_header = BtreePageHeader::parse(&page_data, page_offset)?;
        let ptrs = read_cell_pointers(&page_data, &page_header, page_offset)?;

        sibling_types.push(page_header.page_type);
        old_right_children.push(page_header.right_child);

        // Gather cells from this sibling.
        let relative_sib = child_idx.saturating_sub(first_child);
        for (cell_idx, &ptr) in ptrs.iter().enumerate() {
            let cell_ref =
                CellRef::parse(&page_data, ptr as usize, page_header.page_type, usable_size)?;
            let cell_end = ptr as usize + cell_on_page_size_from_ref(&cell_ref, ptr as usize);
            let raw = page_data[ptr as usize..cell_end].to_vec();

            // Insert overflow cells at the correct position.
            if sib_idx == relative_sib && cell_idx == overflow_insert_idx {
                for ov in overflow_cells {
                    all_cells.push(GatheredCell {
                        size: u16::try_from(ov.len()).unwrap_or(u16::MAX),
                        data: ov.clone(),
                    });
                }
            }

            all_cells.push(GatheredCell {
                size: u16::try_from(raw.len()).unwrap_or(u16::MAX),
                data: raw,
            });
        }

        // Handle overflow cells after the last cell.
        if sib_idx == relative_sib && overflow_insert_idx >= ptrs.len() {
            for ov in overflow_cells {
                all_cells.push(GatheredCell {
                    size: u16::try_from(ov.len()).unwrap_or(u16::MAX),
                    data: ov.clone(),
                });
            }
        }

        // Add divider cell (for non-leaf pages and index leaves, the divider becomes a regular cell).
        if sib_idx < sibling_count - 1 {
            let page_type = page_header.page_type;
            if page_type.is_interior() {
                // For interior pages: the divider's left-child pointer is replaced
                // by the right-child of the current sibling.
                let mut div = divider_cells[sib_idx].clone();
                if let Some(rc) = page_header.right_child {
                    if div.len() >= 4 {
                        div[0..4].copy_from_slice(&rc.get().to_be_bytes());
                    }
                }
                all_cells.push(GatheredCell {
                    size: u16::try_from(div.len()).unwrap_or(u16::MAX),
                    data: div,
                });
            } else if page_type == BtreePageType::LeafIndex {
                // For index leaf pages, the parent divider is an InteriorIndex cell.
                // It contains a unique key that must not be lost. We strip the 4-byte
                // left-child pointer to turn it into a LeafIndex cell.
                let div = &divider_cells[sib_idx];
                if div.len() >= 4 {
                    let leaf_cell_data = div[4..].to_vec();
                    all_cells.push(GatheredCell {
                        size: u16::try_from(leaf_cell_data.len()).unwrap_or(u16::MAX),
                        data: leaf_cell_data,
                    });
                } else {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "index divider cell too small".to_owned(),
                    });
                }
            }
            // For LeafTable: divider is not added to cells (it's extracted from keys).
        }

        if all_cells.len() > MAX_GATHERED_CELLS {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "balance: gathered {} cells exceeds maximum {}",
                    all_cells.len(),
                    MAX_GATHERED_CELLS
                ),
            });
        }
    }

    if all_cells.is_empty() {
        return Ok(BalanceResult::Done);
    }

    // Determine the page type for new pages.
    let page_type = sibling_types[0];
    let is_leaf = page_type.is_leaf();
    let hdr_size = page_type.header_size() as usize;

    // Compute cell distribution across pages.
    let distribution = compute_distribution(&all_cells, usable_size, hdr_size, page_type)?;

    // Allocate/reuse pages.
    let new_page_count = distribution.len();
    let mut new_pgnos: Vec<PageNumber> = Vec::with_capacity(new_page_count);
    for i in 0..new_page_count {
        if i < sibling_pgnos.len() {
            new_pgnos.push(sibling_pgnos[i]);
        } else {
            new_pgnos.push(writer.allocate_page(cx)?);
        }
    }

    // Free any excess old sibling pages.
    if new_page_count < sibling_pgnos.len() {
        for &pgno in &sibling_pgnos[new_page_count..] {
            writer.free_page(cx, pgno)?;
        }
    }

    // Populate new pages and collect divider info for parent.
    let mut new_dividers: Vec<(PageNumber, Vec<u8>)> = Vec::new();
    let mut cell_cursor = 0usize;

    for (page_idx, &cell_count) in distribution.iter().enumerate() {
        let pgno = new_pgnos[page_idx];
        let page_offset = header_offset_for_page(pgno);

        let cells_for_page = &all_cells[cell_cursor..cell_cursor + cell_count];

        // Determine right-child for interior pages.
        let right_child = if !is_leaf && page_idx < new_page_count - 1 {
            // The divider cell is the cell after this page's cells.
            // Its left-child pointer becomes this page's right_child.
            let divider_cell_idx = cell_cursor + cell_count;
            if divider_cell_idx < all_cells.len() && all_cells[divider_cell_idx].data.len() >= 4 {
                let pgno_bytes = &all_cells[divider_cell_idx].data[0..4];
                let raw = u32::from_be_bytes([
                    pgno_bytes[0],
                    pgno_bytes[1],
                    pgno_bytes[2],
                    pgno_bytes[3],
                ]);
                PageNumber::new(raw)
            } else {
                None
            }
        } else if !is_leaf {
            // Last interior page: use the last old right_child.
            *old_right_children.last().unwrap_or(&None)
        } else {
            None
        };

        let page_data = build_page(
            cells_for_page,
            page_type,
            page_offset,
            usable_size,
            right_child,
        )?;

        writer.write_page(cx, pgno, &page_data)?;

        // Extract divider for parent (between this page and the next).
        if page_idx < new_page_count - 1 {
            let divider_data = if is_leaf && page_type.is_table() {
                // Table leaf: divider is [4-byte child ptr][rowid varint].
                // The divider key is the rightmost rowid on this page.
                let last_cell = &cells_for_page[cell_count - 1];
                let last_ref = CellRef::parse(&last_cell.data, 0, page_type, usable_size)?;
                let rowid = last_ref.rowid.ok_or_else(|| {
                    FrankenError::internal("table cell missing rowid for divider")
                })?;
                #[allow(clippy::cast_sign_loss)]
                let rowid_u64 = rowid as u64;
                let mut div = [0u8; 13];
                div[0..4].copy_from_slice(&pgno.get().to_be_bytes());
                let vlen = write_varint(&mut div[4..], rowid_u64);
                div[..4 + vlen].to_vec()
            } else if is_leaf {
                // Index leaf: the divider is the promoted cell. It is consumed from
                // all_cells. We prefix it with the 4-byte child pointer to turn it
                // into an InteriorIndex cell.
                let div_cell = &all_cells[cell_cursor + cell_count];
                let mut div = Vec::with_capacity(4 + div_cell.data.len());
                div.extend_from_slice(&pgno.get().to_be_bytes());
                div.extend_from_slice(&div_cell.data);
                div
            } else {
                // Interior page: the divider cell is consumed from the
                // gathered cells. Skip it and use it as the parent divider.
                let div_cell = &all_cells[cell_cursor + cell_count];
                let mut div = div_cell.data.clone();
                // Replace the left-child pointer with this page's pgno.
                if div.len() >= 4 {
                    div[0..4].copy_from_slice(&pgno.get().to_be_bytes());
                }
                div
            };

            new_dividers.push((pgno, divider_data));
        }

        cell_cursor += cell_count;
        // For interior pages and index leaves, skip the divider cell between pages.
        if page_type != BtreePageType::LeafTable && page_idx < new_page_count - 1 {
            cell_cursor += 1;
        }
    }

    // Update parent: remove old dividers, insert new ones, update child pointers.
    apply_child_replacement(
        cx,
        writer,
        parent_page_no,
        usable_size,
        first_child,
        sibling_count,
        &new_pgnos,
        &new_dividers,
        parent_is_root,
    )
}

// ---------------------------------------------------------------------------
// Helper: compute sibling range
// ---------------------------------------------------------------------------

/// Determine which siblings to balance (centered on child_idx).
///
/// Returns `(first_child_index, count)`.
fn compute_sibling_range(child_idx: usize, total_children: usize) -> (usize, usize) {
    if total_children <= NB {
        // Few enough children to balance all of them.
        return (0, total_children);
    }

    // Center the window on child_idx.
    let half = NB / 2; // 1 for NB=3
    let first = if child_idx <= half {
        0
    } else if child_idx + half >= total_children {
        total_children - NB
    } else {
        child_idx - half
    };

    (first, NB)
}

// ---------------------------------------------------------------------------
// Helper: get child page number from parent
// ---------------------------------------------------------------------------

/// Get the page number of the i-th child of a parent page.
///
/// Children 0..cell_count-1 are left-children of the corresponding cells.
/// Child cell_count is the right-child from the page header.
fn child_page_number(
    parent_data: &[u8],
    parent_header: &BtreePageHeader,
    parent_ptrs: &[u16],
    _parent_offset: usize,
    child_idx: usize,
    usable_size: u32,
) -> Result<PageNumber> {
    let cell_count = parent_header.cell_count as usize;

    match child_idx.cmp(&cell_count) {
        std::cmp::Ordering::Less => {
            let ptr = parent_ptrs[child_idx] as usize;
            let cell = CellRef::parse(parent_data, ptr, parent_header.page_type, usable_size)?;
            cell.left_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: format!("interior cell {} has no left child", child_idx),
                })
        }
        std::cmp::Ordering::Equal => {
            parent_header
                .right_child
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "interior page has no right child".to_owned(),
                })
        }
        std::cmp::Ordering::Greater => Err(FrankenError::internal("child_idx out of range")),
    }
}

// ---------------------------------------------------------------------------
// Helper: cell on-page size from parsed ref
// ---------------------------------------------------------------------------

/// Compute the on-page size of a cell given its parsed reference.
fn cell_on_page_size_from_ref(cell: &CellRef, cell_start: usize) -> usize {
    let mut size = cell.payload_offset - cell_start + cell.local_size as usize;
    if cell.overflow_page.is_some() {
        size += 4;
    }
    size
}

// ---------------------------------------------------------------------------
// Helper: compute cell distribution
// ---------------------------------------------------------------------------

/// Compute how many cells go on each output page.
///
/// Returns a vector of cell counts per page.
fn compute_distribution(
    cells: &[GatheredCell],
    usable_size: u32,
    hdr_size: usize,
    page_type: BtreePageType,
) -> Result<Vec<usize>> {
    if page_type == BtreePageType::LeafTable {
        return compute_leaf_distribution(cells, usable_size, hdr_size);
    }
    compute_interior_distribution(cells, usable_size, hdr_size)
}

#[allow(clippy::too_many_lines)]
fn compute_leaf_distribution(
    cells: &[GatheredCell],
    usable_size: u32,
    hdr_size: usize,
) -> Result<Vec<usize>> {
    let usable_space = usable_size as usize;
    let total_cells = cells.len();

    if total_cells == 0 {
        return Ok(vec![0]);
    }

    // Calculate total space needed.
    let total_size: usize = cells
        .iter()
        .map(cell_cost)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| FrankenError::internal("balance: cell size overflow while sizing page"))?
        .into_iter()
        .sum();

    // Estimate number of pages needed.
    let space_per_page = usable_space - hdr_size;
    let est_pages = total_size.div_ceil(space_per_page).max(1);
    let page_count = est_pages.min(NB + 2); // Safety cap.

    let mut distribution: Vec<usize> = vec![0; page_count];
    let mut page_sizes: Vec<usize> = vec![hdr_size; page_count];

    // Greedy first-fit: assign cells to pages left-to-right.
    let mut current_page = 0;
    for (i, cell) in cells.iter().enumerate() {
        let cell_cost = cell_cost(cell)
            .ok_or_else(|| FrankenError::internal(format!("balance: cell {} size overflow", i)))?;
        if hdr_size + cell_cost > usable_space {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "balance: cell {} requires {} bytes but page usable space is {}",
                    i,
                    hdr_size + cell_cost,
                    usable_space
                ),
            });
        }

        // Check if cell fits on current page.
        if page_sizes[current_page] + cell_cost > usable_space && distribution[current_page] > 0 {
            // Move to next page.
            current_page += 1;
            if current_page >= page_count {
                // Need more pages than estimated.
                distribution.push(0);
                page_sizes.push(hdr_size);
            }
        }
        if page_sizes[current_page] + cell_cost > usable_space {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "balance: cell {} cannot fit on page {} (usage {} + cost {} > {})",
                    i, current_page, page_sizes[current_page], cell_cost, usable_space
                ),
            });
        }

        distribution[current_page] += 1;
        page_sizes[current_page] += cell_cost;
        let _ = i;
    }

    // Trim trailing empty pages.
    while distribution.len() > 1 && *distribution.last().unwrap_or(&1) == 0 {
        distribution.pop();
        page_sizes.pop();
    }

    // Rebalance: try to equalize page usage.
    // Move cells from overfull pages to the right.
    let page_count = distribution.len();
    if page_count > 1 {
        for _ in 0..3 {
            // Few iterations suffice.
            let mut changed = false;
            for i in 0..page_count - 1 {
                let mut left_start: usize = distribution[..i].iter().sum();
                let mut left_count = distribution[i];
                let mut right_start = left_start + left_count;
                let mut right_count = distribution[i + 1];

                // Try moving the last cell from left to right.
                if left_count > 1 {
                    let last_cell = &cells[right_start - 1];
                    let cell_cost = last_cell.data.len() + CELL_POINTER_SIZE as usize;

                    let left_usage = page_sizes[i] - cell_cost;
                    let right_usage = page_sizes[i + 1] + cell_cost;

                    // Move if it makes pages more balanced.
                    let old_diff = page_sizes[i].abs_diff(page_sizes[i + 1]);
                    let new_diff = left_usage.abs_diff(right_usage);

                    if new_diff < old_diff && right_usage <= usable_space {
                        distribution[i] -= 1;
                        distribution[i + 1] += 1;
                        page_sizes[i] = left_usage;
                        page_sizes[i + 1] = right_usage;
                        changed = true;
                        left_count = distribution[i];
                        right_count = distribution[i + 1];
                        left_start = distribution[..i].iter().sum();
                        right_start = left_start + left_count;
                    }
                }

                // Try moving the first cell from right to left.
                if right_count > 1 {
                    let first_cell = &cells[right_start];
                    let cell_cost = first_cell.data.len() + CELL_POINTER_SIZE as usize;

                    let left_usage = page_sizes[i] + cell_cost;
                    let right_usage = page_sizes[i + 1] - cell_cost;

                    let old_diff = page_sizes[i].abs_diff(page_sizes[i + 1]);
                    let new_diff = left_usage.abs_diff(right_usage);

                    if new_diff < old_diff && left_usage <= usable_space {
                        distribution[i] += 1;
                        distribution[i + 1] -= 1;
                        page_sizes[i] = left_usage;
                        page_sizes[i + 1] = right_usage;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
    }

    validate_distribution(cells, &distribution, hdr_size, usable_space, true)?;

    Ok(distribution)
}

fn compute_interior_distribution(
    cells: &[GatheredCell],
    usable_size: u32,
    hdr_size: usize,
) -> Result<Vec<usize>> {
    let usable_space = usable_size as usize;
    let total_cells = cells.len();
    if total_cells == 0 {
        return Ok(vec![0]);
    }

    let mut distribution: Vec<usize> = Vec::new();
    let mut cursor = 0usize;

    while cursor < total_cells {
        let mut used = hdr_size;
        let mut count = 0usize;

        while cursor + count < total_cells {
            let idx = cursor + count;
            let cell_cost = cell_cost(&cells[idx]).ok_or_else(|| {
                FrankenError::internal(format!("balance: interior cell {} size overflow", idx))
            })?;
            let Some(next_used) = used.checked_add(cell_cost) else {
                return Err(FrankenError::internal(format!(
                    "balance: interior usage overflow on page {}",
                    distribution.len()
                )));
            };
            if next_used > usable_space {
                break;
            }
            used = next_used;
            count += 1;

            let remaining = total_cells - (cursor + count);
            if remaining == 0 {
                break;
            }

            if remaining == 1 {
                // A non-final page needs at least one divider plus one child cell.
                // Keep packing this page rather than leaving an orphan divider.
            }
        }

        if cursor + count < total_cells && total_cells - (cursor + count) == 1 {
            // A single trailing interior cell would be consumed as a divider with no
            // payload cell left for the right page. Backtrack one cell so we leave
            // two cells: divider + right-page payload.
            if count <= 1 {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "balance: interior distribution cannot leave a solitary divider"
                        .to_owned(),
                });
            }
            count -= 1;
        }

        if count == 0 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "balance: interior cell {} cannot fit on page (usable {})",
                    cursor, usable_space
                ),
            });
        }

        distribution.push(count);
        cursor += count;

        if cursor < total_cells {
            // One gathered interior cell is promoted back to the parent divider
            // between each pair of output pages.
            cursor += 1;
            if cursor >= total_cells {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "balance: interior distribution consumed trailing divider".to_owned(),
                });
            }
        }
    }

    let logical_cells: usize = distribution.iter().sum();
    let divider_count = distribution.len().saturating_sub(1);
    if logical_cells + divider_count != total_cells {
        return Err(FrankenError::internal(format!(
            "balance: interior distribution accounting mismatch (cells={} dividers={} total={})",
            logical_cells, divider_count, total_cells
        )));
    }

    validate_distribution(cells, &distribution, hdr_size, usable_space, false)?;

    Ok(distribution)
}

fn validate_distribution(
    cells: &[GatheredCell],
    distribution: &[usize],
    hdr_size: usize,
    usable_space: usize,
    is_leaf: bool,
) -> Result<()> {
    let total_cells = cells.len();
    let mut cursor = 0usize;
    for (i, &count) in distribution.iter().enumerate() {
        let Some(slice_end) = cursor.checked_add(count) else {
            return Err(FrankenError::internal(format!(
                "balance: distribution overflow at page {}",
                i
            )));
        };
        if slice_end > total_cells {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "balance: distribution page {} exceeded gathered cells (end={} total={})",
                    i, slice_end, total_cells
                ),
            });
        }

        let payload_bytes = cells[cursor..slice_end]
            .iter()
            .map(|c| c.data.len())
            .sum::<usize>();
        let required = hdr_size + count * CELL_POINTER_SIZE as usize + payload_bytes;
        if required > usable_space {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "balance: distribution page {} requires {} bytes (usable {})",
                    i, required, usable_space
                ),
            });
        }
        if count == 0 && total_cells > 0 {
            return Err(FrankenError::internal(format!(
                "balance: page {} in distribution has zero cells",
                i
            )));
        }

        cursor = slice_end;
        if !is_leaf && i < distribution.len().saturating_sub(1) {
            if cursor >= total_cells {
                return Err(FrankenError::DatabaseCorrupt {
                    detail: "balance: missing divider cell between interior pages".to_owned(),
                });
            }
            cursor += 1;
        }
    }

    if cursor != total_cells {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "balance: distribution consumed {} cells but gathered {}",
                cursor, total_cells
            ),
        });
    }

    Ok(())
}

fn cell_cost(cell: &GatheredCell) -> Option<usize> {
    cell.data.len().checked_add(CELL_POINTER_SIZE as usize)
}

// ---------------------------------------------------------------------------
// Helper: build a B-tree page from cells
// ---------------------------------------------------------------------------

/// Build a complete B-tree page from a list of cells.
///
/// Returns the raw page data.
fn build_page(
    cells: &[GatheredCell],
    page_type: BtreePageType,
    header_offset: usize,
    usable_size: u32,
    right_child: Option<PageNumber>,
) -> Result<Vec<u8>> {
    let page_size = usable_size as usize;
    let mut page = vec![0u8; page_size];

    // Place cells from the end of the page, growing downward.
    let mut content_offset = page_size;
    let mut cell_pointers: Vec<u16> = Vec::with_capacity(cells.len());

    for cell in cells {
        let cell_len = cell.data.len();
        let Some(next_offset) = content_offset.checked_sub(cell_len) else {
            return Err(FrankenError::internal(format!(
                "build_page overflow: page_type={page_type:?} header_offset={header_offset} \
                 page_size={page_size} content_offset={content_offset} cell_len={cell_len} \
                 cells={}",
                cells.len()
            )));
        };
        content_offset = next_offset;
        page[content_offset..content_offset + cell_len].copy_from_slice(&cell.data);
        #[allow(clippy::cast_possible_truncation)]
        cell_pointers.push(content_offset as u16);
    }

    let pointer_array_start = header_offset + page_type.header_size() as usize;
    let pointer_array_end = pointer_array_start + cells.len() * CELL_POINTER_SIZE as usize;
    if pointer_array_end > content_offset {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "build_page layout overlap: page_type={page_type:?} header_offset={header_offset} \
                 pointer_end={pointer_array_end} content_offset={content_offset} cells={} usable={page_size}",
                cells.len()
            ),
        });
    }

    // Write header.
    #[allow(clippy::cast_possible_truncation)]
    let header = BtreePageHeader {
        page_type,
        first_freeblock: 0,
        cell_count: cells.len() as u16,
        cell_content_offset: content_offset as u32,
        fragmented_free_bytes: 0,
        right_child,
    };
    header.write(&mut page, header_offset);

    // Write cell pointer array.
    write_cell_pointers(&mut page, header_offset, &header, &cell_pointers);

    Ok(page)
}

// ---------------------------------------------------------------------------
// Helper: insert a cell into a page
// ---------------------------------------------------------------------------

/// Insert a raw cell into an interior page at the end of the cell array.
///
/// This is used for inserting divider cells into the parent page.
fn insert_cell_into_page<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    page_no: PageNumber,
    _usable_size: u32,
    cell_data: &[u8],
) -> Result<()> {
    let mut page_data = writer.read_page(cx, page_no)?;
    let offset = header_offset_for_page(page_no);
    let mut header = BtreePageHeader::parse(&page_data, offset)?;
    let mut ptrs = read_cell_pointers(&page_data, &header, offset)?;

    let cell_len = cell_data.len();
    let new_content_offset = header.cell_content_offset as usize - cell_len;

    // Check there's room for the cell + pointer.
    let ptr_array_end = offset
        + header.page_type.header_size() as usize
        + (header.cell_count as usize + 1) * CELL_POINTER_SIZE as usize;

    if ptr_array_end > new_content_offset {
        return Err(FrankenError::internal(
            "insufficient space for cell insertion (parent page overflow)",
        ));
    }

    // Write cell content.
    page_data[new_content_offset..new_content_offset + cell_len].copy_from_slice(cell_data);

    // Add cell pointer.
    #[allow(clippy::cast_possible_truncation)]
    ptrs.push(new_content_offset as u16);

    // Update header.
    header.cell_count += 1;
    #[allow(clippy::cast_possible_truncation)]
    {
        header.cell_content_offset = new_content_offset as u32;
    }
    header.write(&mut page_data, offset);
    write_cell_pointers(&mut page_data, offset, &header, &ptrs);

    writer.write_page(cx, page_no, &page_data)
}

// ---------------------------------------------------------------------------
// Helper: update parent after balance
// ---------------------------------------------------------------------------

/// Update the parent page after a balance_nonroot operation.
///
/// Removes old divider cells and inserts new ones.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) fn apply_child_replacement<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    parent_page_no: PageNumber,
    usable_size: u32,
    first_child: usize,
    old_sibling_count: usize,
    new_pgnos: &[PageNumber],
    new_dividers: &[(PageNumber, Vec<u8>)],
    parent_is_root: bool,
) -> Result<BalanceResult> {
    let page_data = writer.read_page(cx, parent_page_no)?;
    let offset = header_offset_for_page(parent_page_no);
    let header = BtreePageHeader::parse(&page_data, offset)?;
    let ptrs = read_cell_pointers(&page_data, &header, offset)?;
    let total_children = header.cell_count as usize + 1;
    let touches_rightmost = first_child + old_sibling_count == total_children;

    // Number of old divider cells to remove.
    let old_divider_count = old_sibling_count.saturating_sub(1);

    // Collect cells to keep: everything except the old dividers.
    let mut kept_cells: Vec<GatheredCell> = Vec::new();

    for (i, &ptr) in ptrs.iter().enumerate() {
        if i >= first_child && i < first_child + old_divider_count {
            continue; // Skip old divider.
        }
        let cell_ref = CellRef::parse(&page_data, ptr as usize, header.page_type, usable_size)?;
        let cell_end = ptr as usize + cell_on_page_size_from_ref(&cell_ref, ptr as usize);
        let raw = page_data[ptr as usize..cell_end].to_vec();
        kept_cells.push(GatheredCell {
            size: u16::try_from(raw.len()).unwrap_or(u16::MAX),
            data: raw,
        });
    }

    // Insert new divider cells at the correct position.
    let insert_pos = first_child;
    let mut final_cells: Vec<GatheredCell> = Vec::new();
    let mut inserted = false;
    let mut kept_idx = 0;

    for i in 0..kept_cells.len() + new_dividers.len() {
        if !inserted && kept_idx == insert_pos {
            // Insert all new dividers here.
            for (_, div_data) in new_dividers {
                final_cells.push(GatheredCell {
                    size: u16::try_from(div_data.len()).unwrap_or(u16::MAX),
                    data: div_data.clone(),
                });
            }
            inserted = true;
        }
        if kept_idx < kept_cells.len() {
            final_cells.push(kept_cells[kept_idx].clone());
            kept_idx += 1;
        }
        let _ = i;
    }

    if !inserted {
        // Dividers go at the end.
        for (_, div_data) in new_dividers {
            final_cells.push(GatheredCell {
                size: u16::try_from(div_data.len()).unwrap_or(u16::MAX),
                data: div_data.clone(),
            });
        }
    }

    // Update right_child to point to the last new sibling.
    let right_child = if touches_rightmost {
        new_pgnos.last().copied().or(header.right_child)
    } else {
        header.right_child
    };
    if header.page_type.is_interior() && right_child.is_none() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("interior page {} missing right child", parent_page_no),
        });
    }

    // If we're not touching the rightmost child, the divider cell that follows
    // the balanced range must have its left-child pointer updated to the
    // last new sibling page.
    if !touches_rightmost && header.page_type.is_interior() && !new_pgnos.is_empty() {
        let patch_idx = first_child + new_dividers.len();
        if patch_idx >= final_cells.len() {
            return Err(FrankenError::internal(format!(
                "parent {} missing post-range divider cell at {} (final_cells={})",
                parent_page_no,
                patch_idx,
                final_cells.len()
            )));
        }
        let last_pgno = *new_pgnos.last().unwrap();
        if final_cells[patch_idx].data.len() < 4 {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "parent {} divider cell at {} too small to patch child pointer",
                    parent_page_no, patch_idx
                ),
            });
        }
        final_cells[patch_idx].data[0..4].copy_from_slice(&last_pgno.get().to_be_bytes());
    }

    if !page_fits(&final_cells, header.page_type, offset, usable_size) {
        if !parent_is_root {
            let (new_pgnos, new_dividers) = split_overflowing_nonroot_interior_page(
                cx,
                writer,
                parent_page_no,
                usable_size,
                offset,
                header.page_type,
                &page_data[..offset],
                &final_cells,
                right_child,
            )?;
            return Ok(BalanceResult::Split {
                new_pgnos,
                new_dividers,
            });
        }

        split_overflowing_root(
            cx,
            writer,
            parent_page_no,
            usable_size,
            offset,
            header.page_type,
            &page_data[..offset],
            &final_cells,
            right_child,
        )?;
        return Ok(BalanceResult::Done);
    }

    // Rebuild the parent page.
    let new_page = build_page(
        &final_cells,
        header.page_type,
        offset,
        usable_size,
        right_child,
    )?;

    // Preserve database header on page 1.
    let mut final_page = new_page;
    if offset > 0 {
        final_page[..offset].copy_from_slice(&page_data[..offset]);
    }

    writer.write_page(cx, parent_page_no, &final_page)?;

    // Balance-shallower: when the root page has zero cells after merging
    // children, copy the single right-child's content into the root and
    // free the child page, reducing tree depth by one.  This is the
    // inverse of balance_deeper and corresponds to SQLite's
    // "balance-shallower" sub-algorithm in the canonical upstream implementation.
    if parent_is_root && final_cells.is_empty() {
        if let Some(child_pgno) = right_child {
            balance_shallower(cx, writer, parent_page_no, child_pgno, usable_size)?;
        }
    }

    Ok(BalanceResult::Done)
}

// ---------------------------------------------------------------------------
// balance_shallower: root collapse (inverse of balance_deeper)
// ---------------------------------------------------------------------------

/// Collapse the root by copying the single right-child's content into
/// the root page and freeing the child.
///
/// Preconditions:
/// - `root_page_no` is an interior page with zero cells.
/// - `child_pgno` is its sole right-child.
///
/// After this call the root inherits whatever page type and content the
/// child had (which may itself be interior or leaf).  If the child is
/// also an interior page with zero cells, the caller is responsible for
/// repeating the collapse (handled by the cursor's upward propagation
/// loop).
fn balance_shallower<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    root_page_no: PageNumber,
    child_pgno: PageNumber,
    usable_size: u32,
) -> Result<()> {
    let child_data = writer.read_page(cx, child_pgno)?;
    let root_offset = header_offset_for_page(root_page_no);
    let child_offset = header_offset_for_page(child_pgno);
    let child_header = BtreePageHeader::parse(&child_data, child_offset)?;

    // Rebuild child cells using the root's header offset. This handles page-1
    // (100-byte header) safely and avoids raw offset shifting pitfalls.
    let child_ptrs = read_cell_pointers(&child_data, &child_header, child_offset)?;
    let mut child_cells: Vec<GatheredCell> = Vec::with_capacity(child_ptrs.len());
    for &ptr in &child_ptrs {
        let cell_offset = usize::from(ptr);
        let cell_ref = CellRef::parse(
            &child_data,
            cell_offset,
            child_header.page_type,
            usable_size,
        )?;
        let cell_end = cell_offset + cell_on_page_size_from_ref(&cell_ref, cell_offset);
        let data = child_data[cell_offset..cell_end].to_vec();
        let size = u16::try_from(data.len()).map_err(|_| {
            FrankenError::Internal("cell too large during balance_shallower".to_owned())
        })?;
        child_cells.push(GatheredCell { data, size });
    }

    // If the child cannot fit on the root due to header-offset reduction
    // (typically page 1), keep the existing shallow form instead of panicking.
    if !page_fits(
        &child_cells,
        child_header.page_type,
        root_offset,
        usable_size,
    ) {
        return Ok(());
    }

    let mut new_root = build_page(
        &child_cells,
        child_header.page_type,
        root_offset,
        usable_size,
        child_header.right_child,
    )?;

    // Preserve the database file header on page 1.
    if root_offset > 0 {
        let original_root = writer.read_page(cx, root_page_no)?;
        new_root[..root_offset].copy_from_slice(&original_root[..root_offset]);
    }

    writer.write_page(cx, root_page_no, &new_root)?;
    writer.free_page(cx, child_pgno)?;

    Ok(())
}

fn page_fits(
    cells: &[GatheredCell],
    page_type: BtreePageType,
    header_offset: usize,
    usable: u32,
) -> bool {
    let Some(total) = page_required_bytes(cells, page_type, header_offset) else {
        return false;
    };
    total <= usable as usize
}

fn page_required_bytes(
    cells: &[GatheredCell],
    page_type: BtreePageType,
    header_offset: usize,
) -> Option<usize> {
    let ptr_bytes = cells.len().checked_mul(CELL_POINTER_SIZE as usize)?;
    let payload_bytes = cells
        .iter()
        .try_fold(0usize, |acc, c| acc.checked_add(c.data.len()))?;
    header_offset
        .checked_add(page_type.header_size() as usize)?
        .checked_add(ptr_bytes)?
        .checked_add(payload_bytes)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn split_overflowing_root<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    root_page_no: PageNumber,
    usable_size: u32,
    root_offset: usize,
    page_type: BtreePageType,
    root_prefix: &[u8],
    final_cells: &[GatheredCell],
    right_child: Option<PageNumber>,
) -> Result<()> {
    if !page_type.is_interior() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "cannot split overflowing non-interior root page {}",
                root_page_no
            ),
        });
    }
    if final_cells.len() < 2 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "overflowing root {} has too few cells to split",
                root_page_no
            ),
        });
    }
    let root_right_child = right_child.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!("overflowing root {} missing right child", root_page_no),
    })?;
    let mut chosen_ranges: Option<Vec<(usize, usize)>> = None;
    let mut chosen_dividers: Option<Vec<Vec<u8>>> = None;
    let mut chosen_right_children: Option<Vec<PageNumber>> = None;

    for child_count in 3usize..=8usize {
        if final_cells.len() < child_count.saturating_mul(2).saturating_sub(1) {
            break;
        }

        let divider_indices: Vec<usize> = (1..child_count)
            .map(|k| k.saturating_mul(final_cells.len()) / child_count)
            .collect();

        let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(child_count);
        let mut dividers: Vec<Vec<u8>> = Vec::with_capacity(child_count.saturating_sub(1));
        let mut right_children: Vec<PageNumber> = Vec::with_capacity(child_count);

        let mut start = 0usize;
        let mut valid = true;

        for &divider_idx in &divider_indices {
            if divider_idx <= start || divider_idx >= final_cells.len() {
                valid = false;
                break;
            }

            let segment = &final_cells[start..divider_idx];
            if segment.is_empty() {
                valid = false;
                break;
            }

            let divider = &final_cells[divider_idx].data;
            if divider.len() < 4 {
                valid = false;
                break;
            }

            let raw = u32::from_be_bytes([divider[0], divider[1], divider[2], divider[3]]);
            let Some(rc) = PageNumber::new(raw) else {
                valid = false;
                break;
            };

            ranges.push((start, divider_idx));
            dividers.push(divider.clone());
            right_children.push(rc);

            start = divider_idx + 1;
        }

        if !valid || start >= final_cells.len() {
            continue;
        }
        ranges.push((start, final_cells.len()));
        if ranges.last().is_some_and(|(s, e)| s == e) {
            continue;
        }
        right_children.push(root_right_child);

        if ranges.len() != child_count
            || dividers.len() != child_count.saturating_sub(1)
            || right_children.len() != child_count
        {
            continue;
        }

        chosen_ranges = Some(ranges);
        chosen_dividers = Some(dividers);
        chosen_right_children = Some(right_children);
        break;
    }

    let ranges = chosen_ranges.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!(
            "unable to choose split points for overflowing root {} with {} cells",
            root_page_no,
            final_cells.len()
        ),
    })?;
    let mut dividers = chosen_dividers.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!("missing divider set for overflowing root {}", root_page_no),
    })?;
    let right_children = chosen_right_children.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!(
            "missing right-child set for overflowing root {}",
            root_page_no
        ),
    })?;

    let child_count = ranges.len();
    let mut child_pgnos: Vec<PageNumber> = Vec::with_capacity(child_count);
    for _ in 0..child_count {
        child_pgnos.push(writer.allocate_page(cx)?);
    }

    for (i, (start, end)) in ranges.iter().copied().enumerate() {
        let child_offset = header_offset_for_page(child_pgnos[i]);
        let child_cells = &final_cells[start..end];
        if !page_fits(child_cells, page_type, child_offset, usable_size) {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "overflowing root {} split child {} does not fit",
                    root_page_no, i
                ),
            });
        }
    }

    for (i, divider) in dividers.iter_mut().enumerate() {
        divider[0..4].copy_from_slice(&child_pgnos[i].get().to_be_bytes());
    }
    let root_cells: Vec<GatheredCell> = dividers
        .into_iter()
        .map(|data| GatheredCell {
            size: u16::try_from(data.len()).unwrap_or(u16::MAX),
            data,
        })
        .collect();
    if !page_fits(&root_cells, page_type, root_offset, usable_size) {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "overflowing root {} cannot fit {} promoted dividers",
                root_page_no,
                root_cells.len()
            ),
        });
    }

    for (i, (start, end)) in ranges.iter().copied().enumerate() {
        let child_cells = &final_cells[start..end];
        let child_offset = header_offset_for_page(child_pgnos[i]);
        let page = build_page(
            child_cells,
            page_type,
            child_offset,
            usable_size,
            Some(right_children[i]),
        )?;
        writer.write_page(cx, child_pgnos[i], &page)?;
    }

    let root_right = child_pgnos
        .last()
        .copied()
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!(
                "overflowing root {} split produced no children",
                root_page_no
            ),
        })?;
    let mut new_root = build_page(
        &root_cells,
        page_type,
        root_offset,
        usable_size,
        Some(root_right),
    )?;
    if root_offset > 0 {
        new_root[..root_offset].copy_from_slice(root_prefix);
    }
    writer.write_page(cx, root_page_no, &new_root)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn split_overflowing_nonroot_interior_page<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    page_no: PageNumber,
    usable_size: u32,
    page_offset: usize,
    page_type: BtreePageType,
    page_prefix: &[u8],
    final_cells: &[GatheredCell],
    right_child: Option<PageNumber>,
) -> Result<SplitPagesAndDividers> {
    if !page_type.is_interior() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!("cannot split overflowing non-interior page {}", page_no),
        });
    }
    let page_right_child = right_child.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!("overflowing interior page {} missing right child", page_no),
    })?;
    if final_cells.len() < 3 {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "overflowing interior page {} has too few cells ({}) to split",
                page_no,
                final_cells.len()
            ),
        });
    }

    let mut chosen_ranges: Option<Vec<(usize, usize)>> = None;
    let mut chosen_dividers: Option<Vec<Vec<u8>>> = None;
    let mut chosen_right_children: Option<Vec<PageNumber>> = None;

    for child_count in 2usize..=8usize {
        if final_cells.len() < child_count.saturating_mul(2).saturating_sub(1) {
            break;
        }

        let divider_indices: Vec<usize> = (1..child_count)
            .map(|k| k.saturating_mul(final_cells.len()) / child_count)
            .collect();

        // Validate indices are strictly increasing and within bounds.
        if divider_indices
            .windows(2)
            .any(|w| w[0] == 0 || w[1] <= w[0] || w[1] >= final_cells.len())
        {
            continue;
        }

        let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(child_count);
        let mut dividers: Vec<Vec<u8>> = Vec::with_capacity(child_count.saturating_sub(1));
        let mut right_children: Vec<PageNumber> = Vec::with_capacity(child_count);

        let mut start = 0usize;
        let mut valid = true;

        for &divider_idx in &divider_indices {
            if divider_idx <= start || divider_idx >= final_cells.len() {
                valid = false;
                break;
            }
            let segment = &final_cells[start..divider_idx];
            if segment.is_empty() {
                valid = false;
                break;
            }

            let divider = &final_cells[divider_idx].data;
            if divider.len() < 4 {
                valid = false;
                break;
            }
            let raw = u32::from_be_bytes([divider[0], divider[1], divider[2], divider[3]]);
            let Some(rc) = PageNumber::new(raw) else {
                valid = false;
                break;
            };

            ranges.push((start, divider_idx));
            dividers.push(divider.clone());
            right_children.push(rc);
            start = divider_idx + 1;
        }

        if !valid || start >= final_cells.len() {
            continue;
        }
        ranges.push((start, final_cells.len()));
        if ranges.last().is_some_and(|(s, e)| s == e) {
            continue;
        }
        right_children.push(page_right_child);

        if ranges.len() != child_count
            || dividers.len() != child_count.saturating_sub(1)
            || right_children.len() != child_count
        {
            continue;
        }

        // Ensure each child page fits (first child may be page 1 offset=100).
        let mut fits = true;
        for (i, (s, e)) in ranges.iter().copied().enumerate() {
            let off = if i == 0 { page_offset } else { 0 };
            if !page_fits(&final_cells[s..e], page_type, off, usable_size) {
                fits = false;
                break;
            }
        }
        if !fits {
            continue;
        }

        chosen_ranges = Some(ranges);
        chosen_dividers = Some(dividers);
        chosen_right_children = Some(right_children);
        break;
    }

    let ranges = chosen_ranges.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!(
            "unable to choose split points for overflowing interior page {} with {} cells",
            page_no,
            final_cells.len()
        ),
    })?;
    let mut dividers = chosen_dividers.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!(
            "missing divider set for overflowing interior page {}",
            page_no
        ),
    })?;
    let right_children = chosen_right_children.ok_or_else(|| FrankenError::DatabaseCorrupt {
        detail: format!(
            "missing right-child set for overflowing interior page {}",
            page_no
        ),
    })?;

    // Allocate pages: reuse the current page as the leftmost child.
    let child_count = ranges.len();
    let mut child_pgnos: Vec<PageNumber> = Vec::with_capacity(child_count);
    child_pgnos.push(page_no);
    for _ in 1..child_count {
        child_pgnos.push(writer.allocate_page(cx)?);
    }

    // Patch promoted divider cells to point to their left child pages.
    for (i, divider) in dividers.iter_mut().enumerate() {
        divider[0..4].copy_from_slice(&child_pgnos[i].get().to_be_bytes());
    }
    let promoted: Vec<(PageNumber, Vec<u8>)> = dividers
        .into_iter()
        .enumerate()
        .map(|(i, data)| (child_pgnos[i], data))
        .collect();

    // Write child pages.
    for (i, (start, end)) in ranges.iter().copied().enumerate() {
        let child_pgno = child_pgnos[i];
        let child_off = header_offset_for_page(child_pgno);
        let page = build_page(
            &final_cells[start..end],
            page_type,
            child_off,
            usable_size,
            Some(right_children[i]),
        )?;
        let mut final_page = page;
        if i == 0 && child_off > 0 {
            final_page[..child_off].copy_from_slice(page_prefix);
        }
        writer.write_page(cx, child_pgno, &final_page)?;
    }

    Ok((child_pgnos, promoted))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::cast_possible_truncation, clippy::similar_names)]
mod tests {
    use super::*;
    use fsqlite_types::serial_type::write_varint;
    use std::collections::HashMap;

    const USABLE: u32 = 4096;

    /// A simple in-memory page store implementing PageReader + PageWriter.
    #[derive(Debug, Clone, Default)]
    struct MemPageStore {
        pages: HashMap<u32, Vec<u8>>,
        next_page: u32,
    }

    impl MemPageStore {
        fn new(start_page: u32) -> Self {
            Self {
                pages: HashMap::new(),
                next_page: start_page,
            }
        }
    }

    impl crate::cursor::PageReader for MemPageStore {
        fn read_page(&self, _cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.pages
                .get(&page_no.get())
                .cloned()
                .ok_or_else(|| FrankenError::internal(format!("page {} not found", page_no)))
        }
    }

    impl PageWriter for MemPageStore {
        fn write_page(&mut self, _cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.pages.insert(page_no.get(), data.to_vec());
            Ok(())
        }
        fn allocate_page(&mut self, _cx: &Cx) -> Result<PageNumber> {
            let pgno = self.next_page;
            self.next_page += 1;
            PageNumber::new(pgno).ok_or(FrankenError::DatabaseFull)
        }
        fn free_page(&mut self, _cx: &Cx, _page_no: PageNumber) -> Result<()> {
            Ok(())
        }
    }

    fn pn(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    /// Build a leaf table page with sorted (rowid, payload) entries.
    #[allow(clippy::cast_sign_loss)]
    fn build_leaf_table(entries: &[(i64, &[u8])]) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let mut content_offset = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::new();

        for &(rowid, payload) in entries {
            let mut cell_buf = [0u8; 256];
            let mut pos = 0;
            // payload_size varint
            pos += write_varint(&mut cell_buf[pos..], payload.len() as u64);
            // rowid varint
            pos += write_varint(&mut cell_buf[pos..], rowid as u64);
            // payload
            cell_buf[pos..pos + payload.len()].copy_from_slice(payload);
            pos += payload.len();

            content_offset -= pos;
            page[content_offset..content_offset + pos].copy_from_slice(&cell_buf[..pos]);
            cell_offsets.push(content_offset as u16);
        }

        let header = BtreePageHeader {
            page_type: BtreePageType::LeafTable,
            first_freeblock: 0,
            cell_count: entries.len() as u16,
            cell_content_offset: content_offset as u32,
            fragmented_free_bytes: 0,
            right_child: None,
        };
        header.write(&mut page, 0);
        write_cell_pointers(&mut page, 0, &header, &cell_offsets);

        page
    }

    /// Build an interior table page with divider cells + right_child.
    #[allow(clippy::cast_sign_loss)]
    fn build_interior_table(cells: &[(PageNumber, i64)], right_child: PageNumber) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let mut content_offset = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::new();

        for &(left_child, rowid) in cells {
            let mut cell_buf = [0u8; 64];
            // left child pointer (4 bytes)
            cell_buf[0..4].copy_from_slice(&left_child.get().to_be_bytes());
            // rowid varint
            let vlen = write_varint(&mut cell_buf[4..], rowid as u64);
            let cell_size = 4 + vlen;

            content_offset -= cell_size;
            page[content_offset..content_offset + cell_size]
                .copy_from_slice(&cell_buf[..cell_size]);
            cell_offsets.push(content_offset as u16);
        }

        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: cells.len() as u16,
            cell_content_offset: content_offset as u32,
            fragmented_free_bytes: 0,
            right_child: Some(right_child),
        };
        header.write(&mut page, 0);
        write_cell_pointers(&mut page, 0, &header, &cell_offsets);

        page
    }

    // -- balance_deeper tests --

    #[test]
    fn test_balance_deeper_basic() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(10);

        // Create a leaf table root page (page 2) with some cells.
        let root = build_leaf_table(&[(1, b"aaa"), (2, b"bbb"), (3, b"ccc")]);
        store.pages.insert(2, root);

        let child_pgno = balance_deeper(&cx, &mut store, pn(2), USABLE).unwrap();

        // Root should now be an interior page with 0 cells.
        let root_data = store.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_data, 0).unwrap();
        assert_eq!(root_header.page_type, BtreePageType::InteriorTable);
        assert_eq!(root_header.cell_count, 0);
        assert_eq!(root_header.right_child, Some(child_pgno));

        // Child should have all 3 cells.
        let child_data = store.pages.get(&child_pgno.get()).unwrap();
        let child_header = BtreePageHeader::parse(child_data, 0).unwrap();
        assert_eq!(child_header.page_type, BtreePageType::LeafTable);
        assert_eq!(child_header.cell_count, 3);

        // Verify cells are intact.
        let child_ptrs = read_cell_pointers(child_data, &child_header, 0).unwrap();
        for &ptr in &child_ptrs {
            let cell =
                CellRef::parse(child_data, ptr as usize, BtreePageType::LeafTable, USABLE).unwrap();
            assert!(cell.rowid.is_some());
        }
    }

    #[test]
    fn test_balance_deeper_preserves_cell_order() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(10);

        let entries: Vec<(i64, &[u8])> = (1..=10).map(|i| (i, b"data" as &[u8])).collect();
        let root = build_leaf_table(&entries);
        store.pages.insert(3, root);

        let child_pgno = balance_deeper(&cx, &mut store, pn(3), USABLE).unwrap();

        let child_data = store.pages.get(&child_pgno.get()).unwrap();
        let child_header = BtreePageHeader::parse(child_data, 0).unwrap();
        let child_ptrs = read_cell_pointers(child_data, &child_header, 0).unwrap();

        // Verify rowid ordering.
        let mut prev_rowid = 0i64;
        for &ptr in &child_ptrs {
            let cell =
                CellRef::parse(child_data, ptr as usize, BtreePageType::LeafTable, USABLE).unwrap();
            let rowid = cell.rowid.unwrap();
            assert!(rowid > prev_rowid, "rowids should be ascending");
            prev_rowid = rowid;
        }
    }

    #[test]
    fn test_balance_deeper_interior_page() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Create an interior root page (page 5) with 2 divider cells.
        let root = build_interior_table(&[(pn(6), 10), (pn(7), 20)], pn(8));
        store.pages.insert(5, root);

        let child_pgno = balance_deeper(&cx, &mut store, pn(5), USABLE).unwrap();

        // Root should be interior with 0 cells.
        let root_data = store.pages.get(&5).unwrap();
        let root_header = BtreePageHeader::parse(root_data, 0).unwrap();
        assert_eq!(root_header.page_type, BtreePageType::InteriorTable);
        assert_eq!(root_header.cell_count, 0);
        assert_eq!(root_header.right_child, Some(child_pgno));

        // Child should have 2 cells (same as original root).
        let child_data = store.pages.get(&child_pgno.get()).unwrap();
        let child_header = BtreePageHeader::parse(child_data, 0).unwrap();
        assert_eq!(child_header.page_type, BtreePageType::InteriorTable);
        assert_eq!(child_header.cell_count, 2);
        assert_eq!(child_header.right_child, Some(pn(8)));
    }

    // -- balance_quick tests --

    #[test]
    fn test_balance_quick_basic() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Set up parent (page 2) with one cell pointing to leaf (page 3).
        // Right-child of parent is page 3 (the leaf).
        let parent = build_interior_table(&[(pn(4), 5)], pn(3));
        store.pages.insert(2, parent);

        // Set up leaf (page 3) with some entries.
        let leaf = build_leaf_table(&[(10, b"ten"), (20, b"twenty")]);
        store.pages.insert(3, leaf);

        // Build an overflow cell for rowid 30.
        let mut overflow_cell = [0u8; 64];
        let mut pos = 0;
        pos += write_varint(&mut overflow_cell[pos..], 5); // payload size
        pos += write_varint(&mut overflow_cell[pos..], 30); // rowid
        overflow_cell[pos..pos + 5].copy_from_slice(b"hello");
        pos += 5;

        let new_pgno = balance_quick(
            &cx,
            &mut store,
            pn(2),
            pn(3),
            &overflow_cell[..pos],
            30,
            USABLE,
        )
        .unwrap()
        .expect("balance_quick should succeed");

        // New sibling should have the overflow cell.
        let new_data = store.pages.get(&new_pgno.get()).unwrap();
        let new_header = BtreePageHeader::parse(new_data, 0).unwrap();
        assert_eq!(new_header.cell_count, 1);
        assert_eq!(new_header.page_type, BtreePageType::LeafTable);

        // Verify the cell on the new page.
        let new_ptrs = read_cell_pointers(new_data, &new_header, 0).unwrap();
        let new_cell = CellRef::parse(
            new_data,
            new_ptrs[0] as usize,
            BtreePageType::LeafTable,
            USABLE,
        )
        .unwrap();
        assert_eq!(new_cell.rowid, Some(30));

        // Parent should now have 2 cells and right_child = new_pgno.
        let parent_data = store.pages.get(&2).unwrap();
        let parent_header = BtreePageHeader::parse(parent_data, 0).unwrap();
        assert_eq!(parent_header.cell_count, 2);
        assert_eq!(parent_header.right_child, Some(new_pgno));
    }

    #[test]
    fn test_balance_quick_parent_full_returns_none() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Create a parent page that is almost full.
        // We'll fill it with cells so that only < 15 bytes remain.
        // Header size = 12.
        // Set cell_content_offset = 20. Free space = 20 - 12 = 8 bytes.
        // That's < 15, so balance_quick should fail.
        let mut full_parent = vec![0u8; USABLE as usize];
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_offset: 20, // artificially low
            fragmented_free_bytes: 0,
            right_child: Some(pn(3)),
        };
        header.write(&mut full_parent, 0);
        store.pages.insert(2, full_parent);

        store.pages.insert(3, build_leaf_table(&[(10, b"ten")]));

        let result = balance_quick(&cx, &mut store, pn(2), pn(3), b"overflow", 30, USABLE).unwrap();

        assert!(
            result.is_none(),
            "balance_quick should return None when parent is full"
        );
    }

    // -- compute_distribution tests --

    #[test]
    fn test_distribution_single_page() {
        let cells: Vec<GatheredCell> = (0..5)
            .map(|_| GatheredCell {
                data: vec![0; 20],
                size: 20,
            })
            .collect();

        let dist = compute_distribution(&cells, USABLE, 8, BtreePageType::LeafTable).unwrap();
        assert_eq!(dist, vec![5]);
    }

    #[test]
    fn test_distribution_multiple_pages() {
        // Each cell is 2000 bytes. With usable=4096 and header=8,
        // available space = 4088. Each cell costs 2000 + 2 = 2002 bytes.
        // So 2 cells per page.
        let cells: Vec<GatheredCell> = (0..6)
            .map(|_| GatheredCell {
                data: vec![0; 2000],
                size: 2000,
            })
            .collect();

        let dist = compute_distribution(&cells, USABLE, 8, BtreePageType::LeafTable).unwrap();
        assert!(dist.len() >= 3, "should need at least 3 pages");
        assert_eq!(dist.iter().sum::<usize>(), 6);
        // All pages should have cells.
        assert!(dist.iter().all(|&c| c > 0));
    }

    #[test]
    fn test_distribution_empty() {
        let dist = compute_distribution(&[], USABLE, 8, BtreePageType::LeafTable).unwrap();
        assert_eq!(dist, vec![0]);
    }

    #[test]
    fn test_distribution_interior_accounts_for_parent_dividers() {
        let cells: Vec<GatheredCell> = (0..15)
            .map(|_| GatheredCell {
                data: vec![0; 300],
                size: 300,
            })
            .collect();

        let dist = compute_distribution(&cells, USABLE, 12, BtreePageType::InteriorTable).unwrap();
        assert!(
            dist.len() > 1,
            "interior distribution should split across pages"
        );
        assert!(dist.iter().all(|&c| c > 0));
        assert_eq!(
            dist.iter().sum::<usize>() + dist.len() - 1,
            cells.len(),
            "interior distribution must account for promoted divider cells"
        );
    }

    #[test]
    fn test_distribution_interior_avoids_trailing_orphan_divider() {
        let cells = vec![
            GatheredCell {
                data: vec![0; 1_500],
                size: 1_500,
            },
            GatheredCell {
                data: vec![0; 1_500],
                size: 1_500,
            },
            GatheredCell {
                data: vec![0; 3_000],
                size: 3_000,
            },
        ];

        let dist = compute_distribution(&cells, USABLE, 12, BtreePageType::InteriorIndex).unwrap();
        assert_eq!(dist, vec![1, 1]);
        assert_eq!(
            dist.iter().sum::<usize>() + dist.len() - 1,
            cells.len(),
            "interior distribution must account for promoted divider cells"
        );
    }

    // -- compute_sibling_range tests --

    #[test]
    fn test_sibling_range_small_tree() {
        assert_eq!(compute_sibling_range(0, 2), (0, 2));
        assert_eq!(compute_sibling_range(1, 2), (0, 2));
    }

    #[test]
    fn test_sibling_range_centered() {
        // 5 children, child 2 is center.
        assert_eq!(compute_sibling_range(2, 5), (1, 3));
        assert_eq!(compute_sibling_range(0, 5), (0, 3));
        assert_eq!(compute_sibling_range(4, 5), (2, 3));
    }

    #[test]
    fn test_sibling_range_three_children() {
        assert_eq!(compute_sibling_range(0, 3), (0, 3));
        assert_eq!(compute_sibling_range(1, 3), (0, 3));
        assert_eq!(compute_sibling_range(2, 3), (0, 3));
    }

    // -- build_page tests --

    #[test]
    fn test_build_page_roundtrip() {
        let cells: Vec<GatheredCell> = vec![
            GatheredCell {
                data: vec![5, 1, b'a', b'b', b'c', b'd', b'e'],
                size: 7,
            },
            GatheredCell {
                data: vec![3, 2, b'x', b'y', b'z'],
                size: 5,
            },
        ];

        let page = build_page(&cells, BtreePageType::LeafTable, 0, USABLE, None)
            .expect("build_page should succeed");

        let header = BtreePageHeader::parse(&page, 0).unwrap();
        assert_eq!(header.cell_count, 2);
        assert_eq!(header.page_type, BtreePageType::LeafTable);

        let ptrs = read_cell_pointers(&page, &header, 0).unwrap();
        assert_eq!(ptrs.len(), 2);

        // Verify cell data is intact.
        for (i, ptr) in ptrs.iter().enumerate() {
            let offset = *ptr as usize;
            let expected = &cells[i].data;
            assert_eq!(&page[offset..offset + expected.len()], expected.as_slice());
        }
    }

    #[test]
    fn test_build_page_rejects_header_overlap() {
        let cells: Vec<GatheredCell> = vec![
            GatheredCell {
                data: vec![0xAA; 2_000],
                size: 2_000,
            },
            GatheredCell {
                data: vec![0xBB; 2_000],
                size: 2_000,
            },
        ];

        let err = build_page(&cells, BtreePageType::LeafTable, 100, USABLE, None)
            .expect_err("page-1 style header offset should reject overlap");
        assert!(err.to_string().contains("layout overlap"));
    }

    // -- balance_nonroot tests --

    #[test]
    fn test_balance_nonroot_two_siblings_merge() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Parent (page 2): 1 divider cell pointing to left=page 3, right=page 4.
        let parent = build_interior_table(&[(pn(3), 5)], pn(4));
        store.pages.insert(2, parent);

        // Left child (page 3) has 3 entries.
        store
            .pages
            .insert(3, build_leaf_table(&[(1, b"a"), (3, b"c"), (5, b"e")]));

        // Right child (page 4) has 3 entries.
        store
            .pages
            .insert(4, build_leaf_table(&[(10, b"j"), (15, b"o"), (20, b"t")]));

        // Balance around child 0 (left child), no overflow.
        let outcome = balance_nonroot(&cx, &mut store, pn(2), 0, &[], 0, USABLE, true).unwrap();
        assert!(matches!(outcome, BalanceResult::Done));

        // With tiny cells on 4096-byte pages, all 6 cells fit on one page.
        // balance_shallower collapses the root — it now directly holds
        // all cells as a leaf page.
        let root_data = store.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_data, 0).unwrap();
        assert!(
            root_header.page_type.is_leaf(),
            "root should collapse to leaf after small-cell merge"
        );
        assert_eq!(root_header.cell_count, 6, "total cells should be preserved");
    }

    #[test]
    fn test_balance_nonroot_with_overflow() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Parent (page 2) with left=page 3, right=page 4.
        let parent = build_interior_table(&[(pn(3), 50)], pn(4));
        store.pages.insert(2, parent);

        // Left child (page 3): 2 entries.
        store
            .pages
            .insert(3, build_leaf_table(&[(10, b"ten"), (50, b"fifty")]));

        // Right child (page 4): 2 entries.
        store
            .pages
            .insert(4, build_leaf_table(&[(60, b"sixty"), (70, b"seventy")]));

        // Create overflow cell for rowid=55, to be inserted on left child at position 2.
        let mut ov_cell = [0u8; 64];
        let mut pos = 0;
        pos += write_varint(&mut ov_cell[pos..], 10); // payload size
        pos += write_varint(&mut ov_cell[pos..], 55); // rowid
        ov_cell[pos..pos + 10].copy_from_slice(b"fiftyfive!");
        pos += 10;

        let overflow_cells = vec![ov_cell[..pos].to_vec()];

        let outcome =
            balance_nonroot(&cx, &mut store, pn(2), 0, &overflow_cells, 2, USABLE, true).unwrap();
        assert!(matches!(outcome, BalanceResult::Done));

        // With small cells plus the overflow, all 5 cells fit on one page.
        // balance_shallower collapses the root to a leaf.
        let root_data = store.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_data, 0).unwrap();
        assert!(
            root_header.page_type.is_leaf(),
            "root should collapse to leaf after small-cell merge with overflow"
        );
        // Original: 4 cells + 1 overflow = 5 total.
        assert_eq!(
            root_header.cell_count, 5,
            "all cells including overflow should be preserved"
        );
    }

    #[test]
    fn test_balance_shallower_page1_skips_when_child_cannot_fit() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(10);

        // Build a child leaf page whose payload fits offset=0 pages but not
        // page-1 offset=100 pages.
        let child_data = {
            let mut selected: Option<Vec<u8>> = None;
            'search: for payload_len in [96usize, 112, 128, 144, 160] {
                for entry_count in 16usize..64usize {
                    let payload = vec![b'x'; payload_len];
                    let mut entries: Vec<(i64, &[u8])> = Vec::with_capacity(entry_count);
                    for rowid in 1..=entry_count {
                        let rowid_i64 = i64::try_from(rowid).expect("rowid fits in i64");
                        entries.push((rowid_i64, payload.as_slice()));
                    }
                    let candidate = build_leaf_table(&entries);
                    let header = BtreePageHeader::parse(&candidate, 0).expect("parse child header");
                    let ptrs =
                        read_cell_pointers(&candidate, &header, 0).expect("read child pointers");
                    let mut cells: Vec<GatheredCell> = Vec::with_capacity(ptrs.len());
                    for ptr in ptrs {
                        let cell_offset = usize::from(ptr);
                        let cell_ref =
                            CellRef::parse(&candidate, cell_offset, header.page_type, USABLE)
                                .expect("cell ref");
                        let cell_end =
                            cell_offset + cell_on_page_size_from_ref(&cell_ref, cell_offset);
                        let data = candidate[cell_offset..cell_end].to_vec();
                        let size = u16::try_from(data.len()).expect("cell size");
                        cells.push(GatheredCell { data, size });
                    }
                    if page_fits(&cells, header.page_type, 0, USABLE)
                        && !page_fits(&cells, header.page_type, 100, USABLE)
                    {
                        selected = Some(candidate);
                        break 'search;
                    }
                }
            }
            selected.expect("find child page that only fits non-page1 offset")
        };

        let db_header = vec![0xAB; 100];
        let mut root_page = vec![0u8; USABLE as usize];
        root_page[..100].copy_from_slice(&db_header);
        let root_header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 0,
            cell_content_offset: USABLE,
            fragmented_free_bytes: 0,
            right_child: Some(pn(2)),
        };
        root_header.write(&mut root_page, 100);

        store.pages.insert(1, root_page.clone());
        store.pages.insert(2, child_data);

        balance_shallower(&cx, &mut store, pn(1), pn(2), USABLE).expect("balance shallower");

        // Root remains unchanged interior page with right-child pointer.
        let updated_root = store.pages.get(&1).expect("root page exists");
        assert_eq!(&updated_root[..100], db_header.as_slice());
        let updated_header = BtreePageHeader::parse(updated_root, 100).expect("root header");
        assert_eq!(updated_header.page_type, BtreePageType::InteriorTable);
        assert_eq!(updated_header.cell_count, 0);
        assert_eq!(updated_header.right_child, Some(pn(2)));
    }

    // -- insert_cell_into_page test --

    #[test]
    fn test_insert_cell_into_page() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Start with an interior page with 1 cell.
        let page = build_interior_table(&[(pn(3), 10)], pn(4));
        store.pages.insert(2, page);

        // Build a new divider cell: [child_ptr=5] [rowid=20].
        let mut cell_buf = [0u8; 13];
        cell_buf[0..4].copy_from_slice(&5u32.to_be_bytes());
        let vlen = write_varint(&mut cell_buf[4..], 20);
        let cell_size = 4 + vlen;

        insert_cell_into_page(&cx, &mut store, pn(2), USABLE, &cell_buf[..cell_size]).unwrap();

        let page_data = store.pages.get(&2).unwrap();
        let header = BtreePageHeader::parse(page_data, 0).unwrap();
        assert_eq!(header.cell_count, 2);
    }
}
