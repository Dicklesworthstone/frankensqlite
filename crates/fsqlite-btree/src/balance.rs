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
    BtreePageHeader, BtreePageType, CellRef, header_offset_for_page, parse_page_header,
    read_cell_pointers, write_cell_pointers,
};
use crate::cursor::PageWriter;
use crate::instrumentation;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::{BTREE_LEAF_HEADER_SIZE, CELL_POINTER_SIZE};
use fsqlite_types::serial_type::write_varint;
use fsqlite_types::{PageData, PageNumber};
use tracing::trace;

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

#[derive(Debug, Clone)]
struct PreparedLeafTableLocalSplit {
    original_leaf_page: PageData,
    new_sibling_pgno: PageNumber,
    new_pgnos: Vec<PageNumber>,
    new_dividers: Vec<(PageNumber, Vec<u8>)>,
    pending_page_writes: Vec<(PageNumber, Vec<u8>, Option<PageData>)>,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub(crate) struct QuickBalanceResult {
    pub(crate) new_pgno: PageNumber,
    pub(crate) new_page_data: PageData,
    pub(crate) new_header: BtreePageHeader,
    pub(crate) new_cell_ptr: u16,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafTableSplitHeat {
    LeftEdge,
    Interior,
    RightEdge,
}

impl LeafTableSplitHeat {
    const fn target_left_basis_points(self) -> usize {
        match self {
            // Leave extra headroom on the left page when inserts keep landing
            // near the beginning of the leaf.
            Self::LeftEdge => 4_500,
            // Keep a mild structural-conflict bias even when the hot side is
            // ambiguous, but avoid overcommitting slack to one half.
            Self::Interior => 5_500,
            // Right-edge growth is the common sequential-rowid case. Bias more
            // payload onto the left page so the new right sibling stays roomy.
            Self::RightEdge => 6_500,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::LeftEdge => "left_edge",
            Self::Interior => "interior",
            Self::RightEdge => "right_edge",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeafTableSplitPolicy {
    heat: LeafTableSplitHeat,
    target_left_basis_points: usize,
    topology_advice: instrumentation::ConflictTopologySplitAdvice,
}

fn no_page_topology_advice(
    baseline_target_left_basis_points: usize,
) -> instrumentation::ConflictTopologySplitAdvice {
    let policy_mode = instrumentation::conflict_topology_policy_mode();
    instrumentation::ConflictTopologySplitAdvice {
        policy_id: "btree.conflict_topology_split.v1",
        mitigation_policy_id: "btree.hot_page_deflection.v1",
        policy_mode,
        hot_page_id: 0,
        baseline_target_left_basis_points,
        advised_target_left_basis_points: baseline_target_left_basis_points,
        effective_target_left_basis_points: baseline_target_left_basis_points,
        conflict_heat: 0,
        writer_overlap_estimate: 0,
        topology_hot: false,
        applied: false,
        deflection_status: if policy_mode == instrumentation::ConflictTopologyPolicyMode::Baseline {
            instrumentation::HotPageDeflectionStatus::OperatorOverride
        } else {
            instrumentation::HotPageDeflectionStatus::Inactive
        },
        deflection_credits_before: 0,
        deflection_credits_after: 0,
        budget_ns: 0,
        budget_pages: 2,
        publication_generation: 0,
        heat_before: 0,
        heat_after: 0,
        trigger_reason: if policy_mode == instrumentation::ConflictTopologyPolicyMode::Baseline {
            "operator_override_baseline"
        } else {
            "no_hot_page_signal"
        },
        migration_outcome: if policy_mode == instrumentation::ConflictTopologyPolicyMode::Baseline {
            "operator_override_baseline"
        } else {
            "not_triggered"
        },
        rollback_reason: if policy_mode == instrumentation::ConflictTopologyPolicyMode::Baseline {
            "operator_override_baseline"
        } else {
            "no_hotspot"
        },
        predicted_overlap_delta: 0,
        operator_override_active: policy_mode
            == instrumentation::ConflictTopologyPolicyMode::Baseline,
    }
}

fn leaf_table_split_policy_for_page(
    page_no: Option<PageNumber>,
    cell_count: usize,
    overflow_insert_idx: usize,
) -> LeafTableSplitPolicy {
    if cell_count < 2 {
        let baseline_target = LeafTableSplitHeat::Interior.target_left_basis_points();
        let topology_advice = page_no.map_or_else(
            || no_page_topology_advice(baseline_target),
            |page| {
                instrumentation::conflict_topology_split_advice(
                    page,
                    LeafTableSplitHeat::Interior.as_str(),
                    baseline_target,
                )
            },
        );
        return LeafTableSplitPolicy {
            heat: LeafTableSplitHeat::Interior,
            target_left_basis_points: topology_advice.effective_target_left_basis_points,
            topology_advice,
        };
    }

    let insert_idx = overflow_insert_idx.min(cell_count - 1);
    let edge_window = cell_count.div_ceil(4);
    let heat = if insert_idx < edge_window {
        LeafTableSplitHeat::LeftEdge
    } else if insert_idx >= cell_count.saturating_sub(edge_window) {
        LeafTableSplitHeat::RightEdge
    } else {
        LeafTableSplitHeat::Interior
    };
    let baseline_target = heat.target_left_basis_points();
    let topology_advice = page_no.map_or_else(
        || no_page_topology_advice(baseline_target),
        |page| {
            instrumentation::conflict_topology_split_advice(page, heat.as_str(), baseline_target)
        },
    );

    // Adaptive fill-factor control: bias the topology-aware target further
    // toward the contended side in proportion to accumulated conflict heat,
    // within explicit clamps. A no-op (returns the topology target unchanged)
    // unless an operator has enabled it.
    let target_left_basis_points = instrumentation::adaptive_fill_factor_target(
        heat.as_str(),
        topology_advice.effective_target_left_basis_points,
        topology_advice.conflict_heat,
    );

    LeafTableSplitPolicy {
        heat,
        target_left_basis_points,
        topology_advice,
    }
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
    page_size: u32,
) -> Result<PageNumber> {
    let root_data = writer.read_page_data(cx, root_page_no)?;
    let root_offset = header_offset_for_page(root_page_no);
    let root_header = parse_page_header(root_data.as_bytes(), root_page_no)?;

    // Extract cells from the root page safely to avoid offset shifting bugs.
    let cell_ptrs = read_cell_pointers(root_data.as_bytes(), &root_header, root_offset)?;
    let mut root_cells: Vec<GatheredCell> = Vec::with_capacity(cell_ptrs.len());
    for &ptr in &cell_ptrs {
        let cell_offset = usize::from(ptr);
        let cell_ref = CellRef::parse(
            root_data.as_bytes(),
            cell_offset,
            root_header.page_type,
            usable_size,
        )?;
        let cell_end = cell_offset + cell_on_page_size_from_ref(&cell_ref, cell_offset);
        let data = root_data.as_bytes()[cell_offset..cell_end].to_vec();
        let size = u16::try_from(data.len()).map_err(|_| FrankenError::DatabaseCorrupt {
            detail: "cell too large during balance_deeper".to_owned(),
        })?;
        root_cells.push(GatheredCell { data, size });
    }

    // Allocate a new child page.
    let child_pgno = writer.allocate_page(cx)?;
    let child_offset = header_offset_for_page(child_pgno);

    // Build the child page using the extracted cells.
    let child_data = match build_page(
        &root_cells,
        root_header.page_type,
        child_offset,
        usable_size,
        page_size,
        root_header.right_child,
    ) {
        Ok(page) => page,
        Err(err) => {
            let _ = writer.free_page(cx, child_pgno);
            return Err(err);
        }
    };

    if let Err(err) = writer.write_page_data(cx, child_pgno, PageData::from_vec(child_data)) {
        let _ = writer.free_page(cx, child_pgno);
        return Err(err);
    }

    // Clear the root page and make it an interior page pointing to the child.
    let mut new_root = vec![0u8; page_size as usize];
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
        new_root[..root_offset].copy_from_slice(&root_data.as_bytes()[..root_offset]);
    }

    if let Err(err) = writer.write_page_data(cx, root_page_no, PageData::from_vec(new_root)) {
        let _ = writer.free_page(cx, child_pgno);
        return Err(err);
    }

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
#[expect(
    clippy::too_many_arguments,
    reason = "quick-balance operates on explicit B-tree state rather than an aggregate config"
)]
pub fn balance_quick<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    parent_page_no: PageNumber,
    leaf_page_no: PageNumber,
    overflow_cell: &[u8],
    overflow_rowid: i64,
    usable_size: u32,
    page_size: u32,
) -> Result<Option<PageNumber>> {
    let divider_rowid =
        quick_balance_divider_rowid(cx, writer, leaf_page_no, overflow_rowid, usable_size)?;
    Ok(balance_quick_known_divider_rowid(
        cx,
        writer,
        parent_page_no,
        leaf_page_no,
        overflow_cell,
        divider_rowid,
        usable_size,
        page_size,
    )?
    .map(|result| result.new_pgno))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn balance_quick_known_divider_rowid<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    parent_page_no: PageNumber,
    leaf_page_no: PageNumber,
    overflow_cell: &[u8],
    divider_rowid: i64,
    usable_size: u32,
    page_size: u32,
) -> Result<Option<QuickBalanceResult>> {
    // Read parent page once, then perform the divider append and right-child
    // update in-memory. This hot path is only used when the split lands on the
    // rightmost child, so the parent update is always an append, not a general
    // insertion.
    let mut parent_data = writer.read_page_data(cx, parent_page_no)?;
    let parent_offset = header_offset_for_page(parent_page_no);
    let parent_header = parse_page_header(parent_data.as_bytes(), parent_page_no)?;

    // Build the exact divider up front so the quick-balance gate uses the
    // space the parent will actually consume, not the worst-case varint size.
    let mut divider = [0u8; 13]; // 4-byte child pointer + up to 9-byte varint.
    divider[0..4].copy_from_slice(&leaf_page_no.get().to_be_bytes());
    #[allow(clippy::cast_sign_loss)]
    let rowid_u64 = divider_rowid as u64;
    let varint_size = write_varint(&mut divider[4..], rowid_u64);
    let divider_size = 4 + varint_size;
    let required_parent_space = usize::from(CELL_POINTER_SIZE) + divider_size;

    let parent_used = parent_offset
        + usize::from(parent_header.page_type.header_size())
        + (usize::from(parent_header.cell_count) * usize::from(CELL_POINTER_SIZE));

    let free_space = parent_header
        .content_offset(usable_size)
        .saturating_sub(parent_used);

    if free_space < required_parent_space {
        return Ok(None);
    }

    // Allocate new sibling page.
    let new_pgno = writer.allocate_page(cx)?;
    let mut new_page = vec![0u8; page_size as usize];
    let new_offset = header_offset_for_page(new_pgno);

    // Initialize as leaf table page with one cell.
    let cell_size = overflow_cell.len();
    let Some(content_start) = (usable_size as usize).checked_sub(cell_size) else {
        writer.free_page(cx, new_pgno)?;
        return Ok(None);
    };
    if content_start < new_offset + BTREE_LEAF_HEADER_SIZE as usize + 2 {
        writer.free_page(cx, new_pgno)?;
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

    let new_page_data = PageData::from_vec(new_page);
    if let Err(err) = writer.write_page_data(cx, new_pgno, new_page_data.clone()) {
        let _ = writer.free_page(cx, new_pgno);
        return Err(err);
    }

    let new_parent_content_offset = parent_header
        .content_offset(usable_size)
        .checked_sub(divider_size)
        .ok_or_else(|| FrankenError::internal("divider cell too large for parent content area"))?;
    let parent_ptr_write_offset = parent_offset
        + usize::from(parent_header.page_type.header_size())
        + (usize::from(parent_header.cell_count) * usize::from(CELL_POINTER_SIZE));
    if parent_ptr_write_offset + usize::from(CELL_POINTER_SIZE) > new_parent_content_offset {
        let _ = writer.free_page(cx, new_pgno);
        return Ok(None);
    }

    {
        let page_bytes = parent_data.as_bytes_mut();
        page_bytes[new_parent_content_offset..new_parent_content_offset + divider_size]
            .copy_from_slice(&divider[..divider_size]);
        #[allow(clippy::cast_possible_truncation)]
        let divider_ptr = new_parent_content_offset as u16;
        page_bytes
            [parent_ptr_write_offset..parent_ptr_write_offset + usize::from(CELL_POINTER_SIZE)]
            .copy_from_slice(&divider_ptr.to_be_bytes());

        let mut updated_parent_header = parent_header;
        updated_parent_header.cell_count += 1;
        #[allow(clippy::cast_possible_truncation)]
        {
            updated_parent_header.cell_content_offset = new_parent_content_offset as u32;
        }
        updated_parent_header.right_child = Some(new_pgno);
        updated_parent_header.write(page_bytes, parent_offset);
    }

    if let Err(err) = writer.write_page_data(cx, parent_page_no, parent_data) {
        let _ = writer.free_page(cx, new_pgno);
        return Err(err);
    }

    Ok(Some(QuickBalanceResult {
        new_pgno,
        new_page_data,
        new_header,
        new_cell_ptr: cell_ptr,
    }))
}

fn quick_balance_divider_rowid<W: PageWriter>(
    cx: &Cx,
    writer: &W,
    leaf_page_no: PageNumber,
    overflow_rowid: i64,
    usable_size: u32,
) -> Result<i64> {
    let leaf_data = writer.read_page_data(cx, leaf_page_no)?;
    let leaf_offset = header_offset_for_page(leaf_page_no);
    let leaf_header = parse_page_header(leaf_data.as_bytes(), leaf_page_no)?;
    if leaf_header.cell_count == 0 {
        return Ok(overflow_rowid.saturating_sub(1));
    }
    let leaf_ptrs = read_cell_pointers(leaf_data.as_bytes(), &leaf_header, leaf_offset)?;
    let last_ptr = leaf_ptrs[leaf_header.cell_count as usize - 1] as usize;
    // bd-ah597.2: same "caller only needs the rowid" pattern as commit
    // b35f091c (predecessor_idx). The divider computed by quick-balance is
    // just the last LeafTable cell's rowid; decoding local_size /
    // overflow_page / payload_offset via the full `CellRef::parse` is dead
    // work on this hot split path.
    let _ = usable_size;
    CellRef::parse_leaf_table_rowid(leaf_data.as_bytes(), last_ptr)
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
    page_size: u32,
    parent_is_root: bool,
) -> Result<BalanceResult> {
    let parent_data = writer.read_page_data(cx, parent_page_no)?;
    let parent_offset = header_offset_for_page(parent_page_no);
    let parent_header = parse_page_header(parent_data.as_bytes(), parent_page_no)?;
    let parent_ptrs = read_cell_pointers(parent_data.as_bytes(), &parent_header, parent_offset)?;

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
            parent_data.as_bytes(),
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
                parent_data.as_bytes(),
                div_offset,
                parent_header.page_type,
                usable_size,
            )?;
            // Extract the raw divider cell bytes.
            let div_end = div_offset + cell_on_page_size_from_ref(&div_cell, div_offset);
            divider_cells.push(parent_data.as_bytes()[div_offset..div_end].to_vec());
        }
    }

    // Read all sibling pages and gather cells.
    let mut all_cells: Vec<GatheredCell> = Vec::with_capacity(sibling_count * 32);
    let mut sibling_types: Vec<BtreePageType> = Vec::with_capacity(sibling_count);
    let mut old_right_children: Vec<Option<PageNumber>> = Vec::with_capacity(sibling_count);
    let mut original_sibling_pages: Vec<(PageNumber, PageData)> = Vec::with_capacity(sibling_count);

    for (sib_idx, &pgno) in sibling_pgnos.iter().enumerate() {
        let page_data = writer.read_page_data(cx, pgno)?;
        original_sibling_pages.push((pgno, page_data.clone()));
        let page_offset = header_offset_for_page(pgno);
        let page_header = parse_page_header(page_data.as_bytes(), pgno)?;
        let ptrs = read_cell_pointers(page_data.as_bytes(), &page_header, page_offset)?;

        sibling_types.push(page_header.page_type);
        old_right_children.push(page_header.right_child);

        // Gather cells from this sibling.
        let relative_sib = child_idx.saturating_sub(first_child);
        for (cell_idx, &ptr) in ptrs.iter().enumerate() {
            let cell_ref = CellRef::parse(
                page_data.as_bytes(),
                ptr as usize,
                page_header.page_type,
                usable_size,
            )?;
            let cell_end = ptr as usize + cell_on_page_size_from_ref(&cell_ref, ptr as usize);
            let raw = page_data.as_bytes()[ptr as usize..cell_end].to_vec();

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
                let rc = page_header
                    .right_child
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "interior page missing right_child during balance".to_owned(),
                    })?;
                if div.len() >= 4 {
                    div[0..4].copy_from_slice(&rc.get().to_be_bytes());
                } else {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "interior divider cell too small".to_owned(),
                    });
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

    // We cannot return early if all_cells is empty, because we still need to
    // call apply_child_replacement so that balance_shallower can reduce the
    // tree depth (e.g. collapsing an empty interior root back into an empty leaf).
    let page_type = sibling_types[0];
    let is_leaf = page_type.is_leaf();
    let hdr_size = page_type.header_size() as usize;

    // Compute cell distribution across pages.
    let distribution = compute_distribution(&all_cells, usable_size, hdr_size, page_type)?;

    // Allocate/reuse pages.
    let new_page_count = distribution.len();
    let mut new_pgnos: Vec<PageNumber> = Vec::with_capacity(new_page_count);
    let mut newly_allocated_pgnos: Vec<PageNumber> =
        Vec::with_capacity(new_page_count.saturating_sub(sibling_pgnos.len()));
    for i in 0..new_page_count {
        if i < sibling_pgnos.len() {
            new_pgnos.push(sibling_pgnos[i]);
        } else {
            match writer.allocate_page(cx) {
                Ok(pgno) => {
                    newly_allocated_pgnos.push(pgno);
                    new_pgnos.push(pgno);
                }
                Err(err) => {
                    free_pages_best_effort(cx, writer, &newly_allocated_pgnos);
                    return Err(err);
                }
            }
        }
    }
    let pages_to_free_after_success: Vec<PageNumber> =
        sibling_pgnos.iter().skip(new_page_count).copied().collect();
    let original_parent_page = writer.read_page_data(cx, parent_page_no)?;

    // Inline rollback helper — avoids closure capturing `writer` which would
    // conflict with the mutable borrows needed inside the loop.
    macro_rules! do_rollback {
        ($err:expr) => {{
            let _ = writer.write_page_data(cx, parent_page_no, original_parent_page.clone());
            restore_pages_best_effort(cx, writer, &original_sibling_pages);
            free_pages_best_effort(cx, writer, &newly_allocated_pgnos);
            $err
        }};
    }

    // Populate new pages and collect divider info for parent.
    let mut new_dividers: Vec<(PageNumber, Vec<u8>)> =
        Vec::with_capacity(new_page_count.saturating_sub(1));
    let mut pending_page_writes: Vec<(PageNumber, Vec<u8>, Option<PageData>)> =
        Vec::with_capacity(new_page_count);
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

        let page_data = match build_page(
            cells_for_page,
            page_type,
            page_offset,
            usable_size,
            page_size,
            right_child,
        ) {
            Ok(page) => page,
            Err(err) => return Err(do_rollback!(err)),
        };

        pending_page_writes.push((
            pgno,
            page_data,
            if page_idx < original_sibling_pages.len() {
                Some(original_sibling_pages[page_idx].1.clone())
            } else {
                None
            },
        ));

        // Extract divider for parent (between this page and the next).
        if page_idx < new_page_count - 1 {
            let divider_data = if is_leaf && page_type.is_table() {
                // Table leaf: divider is [4-byte child ptr][rowid varint].
                // The divider key is the rightmost rowid on this page.
                let last_cell = &cells_for_page[cell_count - 1];
                let last_ref = match CellRef::parse(&last_cell.data, 0, page_type, usable_size) {
                    Ok(cell) => cell,
                    Err(err) => return Err(do_rollback!(err)),
                };
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

    pending_page_writes.sort_by_key(|(pgno, _, _)| pgno.get());
    for (pgno, page_data, original_page) in pending_page_writes {
        if let Err(err) = write_page_if_changed(
            cx,
            writer,
            pgno,
            page_data,
            original_page.as_ref().map(PageData::as_bytes),
        ) {
            return Err(do_rollback!(err));
        }
    }

    // Update parent: remove old dividers, insert new ones, update child pointers.
    let outcome = match apply_child_replacement(
        cx,
        writer,
        parent_page_no,
        usable_size,
        page_size,
        first_child,
        sibling_count,
        &new_pgnos,
        &new_dividers,
        parent_is_root,
    ) {
        Ok(outcome) => outcome,
        Err(err) => return Err(do_rollback!(err)),
    };

    for pgno in pages_to_free_after_success {
        writer.free_page(cx, pgno)?;
    }

    Ok(outcome)
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
            // bd-ah597.3: same "caller only needs left_child" pattern as
            // commits 4b061dcc (child_page_at), 385591f8 (replace_separator).
            // Use the public `CellRef::read_interior_left_child` helper
            // instead of a full `CellRef::parse` — the cell's first 4 bytes
            // are the child pointer; decoding the rowid varint, local-size
            // math, and overflow bounds are dead work here. `usable_size`
            // stays in the signature because the shared-sibling helper
            // `read_cell_pointers` and the `compute_sibling_range` plumbing
            // both expect it.
            let _ = usable_size;
            let ptr = parent_ptrs[child_idx] as usize;
            CellRef::read_interior_left_child(parent_data, ptr)
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

fn restore_pages_best_effort<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    pages: &[(PageNumber, PageData)],
) {
    for (pgno, data) in pages {
        restore_page_best_effort(cx, writer, *pgno, data);
    }
}

fn restore_page_best_effort<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    page_no: PageNumber,
    original_page: &PageData,
) {
    if let Ok(current_page) = writer.read_page_data(cx, page_no) {
        if current_page.as_bytes() == original_page.as_bytes() {
            return;
        }
    }
    let _ = writer.write_page_data(cx, page_no, original_page.clone());
}

fn write_page_if_changed<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    page_no: PageNumber,
    page_data: Vec<u8>,
    original_page: Option<&[u8]>,
) -> Result<()> {
    if let Some(original_page) = original_page {
        if original_page == page_data.as_slice() {
            return Ok(());
        }
    }
    writer.write_page_data(cx, page_no, PageData::from_vec(page_data))
}

fn parent_has_room_for_table_leaf_split<R: crate::cursor::PageReader>(
    cx: &Cx,
    reader: &R,
    parent_page_no: PageNumber,
    usable_size: u32,
) -> Result<bool> {
    let parent_page = reader.read_page_data(cx, parent_page_no)?;
    let parent_offset = header_offset_for_page(parent_page_no);
    let parent_header = parse_page_header(parent_page.as_bytes(), parent_page_no)?;
    let parent_used = parent_offset
        + usize::from(parent_header.page_type.header_size())
        + (usize::from(parent_header.cell_count) * usize::from(CELL_POINTER_SIZE));
    let free_space = parent_header
        .content_offset(usable_size)
        .saturating_sub(parent_used);
    Ok(free_space >= 15)
}

/// Choose the split index for leaf table cell redistribution.
///
/// The split point balances two competing goals:
///
/// 1. **Space efficiency:** Minimize wasted space across both pages.
/// 2. **Concurrency friendliness:** Leave slack in both halves to reduce
///    the frequency of future splits, which in turn reduces parent-page
///    modifications — the primary source of MVCC structural conflicts.
///
/// The target split is biased toward the side likely to stay hot after
/// the split. Right-edge growth keeps more slack in the new sibling;
/// left-edge growth mirrors that for the original page; interior growth
/// stays near-balanced with only a mild structural-conflict bias.
///
/// Inspired by B-link tree designs (Lehman & Yao, 1981) which prioritize
/// reducing structural modifications for concurrent access, and by the
/// observation in `docs/planning/STATE_OF_THE_CODEBASE_AND_NEXT_STEPS.md` that the
/// best shared-page conflict is the one that never happens.
fn choose_leaf_table_split_index(
    cells: &[GatheredCell],
    left_header_offset: usize,
    right_header_offset: usize,
    usable_size: u32,
    policy: LeafTableSplitPolicy,
) -> Option<usize> {
    if cells.len() < 2 {
        return None;
    }

    let header_size = BtreePageType::LeafTable.header_size() as usize;
    let left_base = left_header_offset.checked_add(header_size)?;
    let right_base = right_header_offset.checked_add(header_size)?;
    let usable = usable_size as usize;
    let mut cell_costs = Vec::with_capacity(cells.len());
    let mut total_cost = 0usize;
    for cell in cells {
        let cost = cell_cost(cell)?;
        total_cost = total_cost.checked_add(cost)?;
        cell_costs.push(cost);
    }

    // Target fill: bias free space toward the side most likely to stay hot.
    // Right-edge growth keeps the new sibling roomy; left-edge growth mirrors
    // that for the original page. Interior inserts stay near-balanced with a
    // slight left bias to reduce split churn without overcommitting either side.
    let target_left = left_base + ((total_cost * policy.target_left_basis_points) / 10_000);

    let mut left_total = left_base;
    let mut right_total = right_base.checked_add(total_cost)?;
    let mut best_split: Option<(usize, usize)> = None;

    for (idx, cost) in cell_costs
        .iter()
        .copied()
        .enumerate()
        .take(cells.len().saturating_sub(1))
    {
        left_total = left_total.checked_add(cost)?;
        right_total = right_total.checked_sub(cost)?;
        if left_total > usable || right_total > usable {
            continue;
        }

        // Distance from the biased target — smaller is better.
        // On ties, prefer the side consistent with the predicted hot page:
        // later for right/interior heat, earlier for left heat.
        let key = left_total.abs_diff(target_left);

        match best_split {
            Some((_, best_key)) if key > best_key => {}
            Some((best_idx, best_key)) if key == best_key => {
                let candidate_idx = idx + 1;
                let prefer_candidate = match policy.heat {
                    LeafTableSplitHeat::LeftEdge => candidate_idx < best_idx,
                    LeafTableSplitHeat::Interior | LeafTableSplitHeat::RightEdge => {
                        candidate_idx > best_idx
                    }
                };
                if prefer_candidate {
                    best_split = Some((candidate_idx, key));
                }
            }
            _ => best_split = Some((idx + 1, key)),
        }
    }

    best_split.map(|(split_idx, _)| split_idx)
}

fn table_leaf_divider_bytes(
    left_page_no: PageNumber,
    rightmost_left_cell: &GatheredCell,
    usable_size: u32,
) -> Result<Vec<u8>> {
    let cell_ref = CellRef::parse(
        &rightmost_left_cell.data,
        0,
        BtreePageType::LeafTable,
        usable_size,
    )?;
    let rowid = cell_ref
        .rowid
        .ok_or_else(|| FrankenError::internal("table leaf split cell missing rowid"))?;
    #[allow(clippy::cast_sign_loss)]
    let rowid_u64 = rowid as u64;
    let mut divider = [0u8; 13];
    divider[0..4].copy_from_slice(&left_page_no.get().to_be_bytes());
    let divider_len = write_varint(&mut divider[4..], rowid_u64);
    Ok(divider[..4 + divider_len].to_vec())
}

fn rollback_prepared_leaf_table_local_split_best_effort<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    prepared: &PreparedLeafTableLocalSplit,
) {
    restore_page_best_effort(
        cx,
        writer,
        prepared.new_pgnos[0],
        &prepared.original_leaf_page,
    );
    let _ = writer.free_page(cx, prepared.new_sibling_pgno);
}

fn prepare_leaf_table_local_split<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    leaf_page_no: PageNumber,
    overflow_cell: &[u8],
    overflow_insert_idx: usize,
    usable_size: u32,
    page_size: u32,
) -> Result<Option<PreparedLeafTableLocalSplit>> {
    let leaf_page = writer.read_page_data(cx, leaf_page_no)?;
    let leaf_offset = header_offset_for_page(leaf_page_no);
    let leaf_header = parse_page_header(leaf_page.as_bytes(), leaf_page_no)?;
    if leaf_header.page_type != BtreePageType::LeafTable {
        return Ok(None);
    }

    let ptrs = read_cell_pointers(leaf_page.as_bytes(), &leaf_header, leaf_offset)?;
    let insert_idx = overflow_insert_idx.min(ptrs.len());
    let mut all_cells = Vec::with_capacity(ptrs.len().saturating_add(1));
    let mut overflow_inserted = false;

    for (cell_idx, &ptr) in ptrs.iter().enumerate() {
        if cell_idx == insert_idx {
            all_cells.push(GatheredCell {
                size: u16::try_from(overflow_cell.len()).unwrap_or(u16::MAX),
                data: overflow_cell.to_vec(),
            });
            overflow_inserted = true;
        }

        let ptr = usize::from(ptr);
        let cell_ref = CellRef::parse(
            leaf_page.as_bytes(),
            ptr,
            BtreePageType::LeafTable,
            usable_size,
        )?;
        let cell_end = ptr + cell_on_page_size_from_ref(&cell_ref, ptr);
        let raw = leaf_page.as_bytes()[ptr..cell_end].to_vec();
        all_cells.push(GatheredCell {
            size: u16::try_from(raw.len()).unwrap_or(u16::MAX),
            data: raw,
        });
    }

    if !overflow_inserted {
        all_cells.push(GatheredCell {
            size: u16::try_from(overflow_cell.len()).unwrap_or(u16::MAX),
            data: overflow_cell.to_vec(),
        });
    }

    let split_policy =
        leaf_table_split_policy_for_page(Some(leaf_page_no), all_cells.len(), insert_idx);
    let Some(split_idx) =
        choose_leaf_table_split_index(&all_cells, leaf_offset, 0, usable_size, split_policy)
    else {
        return Ok(None);
    };

    trace!(
        target: "fsqlite.btree",
        leaf_page = leaf_page_no.get(),
        overflow_insert_idx = insert_idx,
        total_cells = all_cells.len(),
        predicted_hot_side = split_policy.heat.as_str(),
        hot_page_id = split_policy.topology_advice.hot_page_id,
        policy_id = split_policy.topology_advice.policy_id,
        mitigation_policy_id = split_policy.topology_advice.mitigation_policy_id,
        policy_mode = split_policy.topology_advice.policy_mode.as_str(),
        placement_policy = split_policy.topology_advice.placement_policy(),
        split_reason = split_policy.topology_advice.split_reason(),
        trigger_reason = split_policy.topology_advice.trigger_reason,
        fill_factor = split_policy.target_left_basis_points as u32,
        adaptive_fill_factor_active = instrumentation::adaptive_fill_factor_enabled(),
        adaptive_policy_id = instrumentation::adaptive_fill_factor_policy_id(),
        page_role = "table_leaf",
        predicted_overlap_delta = split_policy.topology_advice.predicted_overlap_delta,
        observed_overlap_delta = 0_i64,
        abort_rate = 0_u64,
        latency_p95_ns = 0_u64,
        operator_override_active = split_policy.topology_advice.operator_override_active,
        conflict_heat = split_policy.topology_advice.conflict_heat,
        heat_before = split_policy.topology_advice.heat_before,
        heat_after = split_policy.topology_advice.heat_after,
        writer_overlap_estimate = split_policy.topology_advice.writer_overlap_estimate,
        deflection_status = split_policy.topology_advice.deflection_status.as_str(),
        deflection_active = split_policy.topology_advice.deflection_active(),
        deflection_applied = split_policy.topology_advice.deflection_applied(),
        deflection_credits_before = split_policy.topology_advice.deflection_credits_before,
        deflection_credits_after = split_policy.topology_advice.deflection_credits_after,
        budget_ns = split_policy.topology_advice.budget_ns,
        budget_pages = split_policy.topology_advice.budget_pages,
        publication_generation = split_policy.topology_advice.publication_generation,
        migration_outcome = split_policy.topology_advice.migration_outcome,
        rollback_reason = split_policy.topology_advice.rollback_reason,
        first_failure_diag = "none",
        target_left_basis_points = split_policy.target_left_basis_points as u32,
        split_idx,
        "contention-aware leaf table split policy"
    );

    let new_sibling_pgno = writer.allocate_page(cx)?;
    let rollback_allocation = |writer: &mut W| {
        let _ = writer.free_page(cx, new_sibling_pgno);
    };

    let left_page = match build_page(
        &all_cells[..split_idx],
        BtreePageType::LeafTable,
        leaf_offset,
        usable_size,
        page_size,
        None,
    ) {
        Ok(page) => page,
        Err(err) => {
            rollback_allocation(writer);
            return Err(err);
        }
    };
    let right_page = match build_page(
        &all_cells[split_idx..],
        BtreePageType::LeafTable,
        header_offset_for_page(new_sibling_pgno),
        usable_size,
        page_size,
        None,
    ) {
        Ok(page) => page,
        Err(err) => {
            rollback_allocation(writer);
            return Err(err);
        }
    };

    let original_leaf_page = leaf_page.clone();
    let mut pending_page_writes = vec![
        (leaf_page_no, left_page, Some(leaf_page.clone())),
        (new_sibling_pgno, right_page, None),
    ];
    pending_page_writes.sort_by_key(|(pgno, _, _)| pgno.get());

    let divider =
        match table_leaf_divider_bytes(leaf_page_no, &all_cells[split_idx - 1], usable_size) {
            Ok(divider) => divider,
            Err(err) => {
                restore_page_best_effort(cx, writer, leaf_page_no, &original_leaf_page);
                let _ = writer.free_page(cx, new_sibling_pgno);
                return Err(err);
            }
        };

    Ok(Some(PreparedLeafTableLocalSplit {
        original_leaf_page,
        new_sibling_pgno,
        new_pgnos: vec![leaf_page_no, new_sibling_pgno],
        new_dividers: vec![(leaf_page_no, divider)],
        pending_page_writes,
    }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn balance_table_leaf_local_split<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    parent_page_no: PageNumber,
    child_idx: usize,
    leaf_page_no: PageNumber,
    overflow_cell: &[u8],
    overflow_insert_idx: usize,
    usable_size: u32,
    page_size: u32,
    parent_is_root: bool,
) -> Result<Option<BalanceResult>> {
    if !parent_has_room_for_table_leaf_split(cx, writer, parent_page_no, usable_size)? {
        return Ok(None);
    }

    let Some(prepared) = prepare_leaf_table_local_split(
        cx,
        writer,
        leaf_page_no,
        overflow_cell,
        overflow_insert_idx,
        usable_size,
        page_size,
    )?
    else {
        return Ok(None);
    };

    for (page_no, page_data, original_page) in prepared.pending_page_writes.iter() {
        if let Err(err) = write_page_if_changed(
            cx,
            writer,
            *page_no,
            page_data.clone(),
            original_page.as_ref().map(PageData::as_bytes),
        ) {
            rollback_prepared_leaf_table_local_split_best_effort(cx, writer, &prepared);
            return Err(err);
        }
    }

    match apply_child_replacement(
        cx,
        writer,
        parent_page_no,
        usable_size,
        page_size,
        child_idx,
        1,
        &prepared.new_pgnos,
        &prepared.new_dividers,
        parent_is_root,
    ) {
        Ok(outcome) => Ok(Some(outcome)),
        Err(err) => {
            rollback_prepared_leaf_table_local_split_best_effort(cx, writer, &prepared);
            Err(err)
        }
    }
}

fn free_pages_best_effort<W: PageWriter>(cx: &Cx, writer: &mut W, pages: &[PageNumber]) {
    for &pgno in pages {
        let _ = writer.free_page(cx, pgno);
    }
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

    let mut distribution: Vec<usize> = Vec::with_capacity(cells.len().div_ceil(64).max(1));
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
/// `page_size` is the full on-disk page size (usable_size + reserved_bytes).
/// The buffer is allocated at `page_size` so that stock SQLite sees
/// correctly-sized pages, but cell content grows downward from
/// `usable_size` as required by the file format.
///
/// Returns the raw page data.
fn build_page(
    cells: &[GatheredCell],
    page_type: BtreePageType,
    header_offset: usize,
    usable_size: u32,
    page_size: u32,
    right_child: Option<PageNumber>,
) -> Result<Vec<u8>> {
    if page_size < usable_size {
        return Err(FrankenError::internal(format!(
            "build_page: page_size ({page_size}) < usable_size ({usable_size})"
        )));
    }
    let full_page_size = page_size as usize;
    let usable = usable_size as usize;
    let mut page = vec![0u8; full_page_size];

    // Place cells from the end of the *usable* area, growing downward.
    // The reserved region (page[usable..full_page_size]) stays zeroed.
    let mut content_offset = usable;
    let mut cell_pointers: Vec<u16> = Vec::with_capacity(cells.len());

    for cell in cells {
        let cell_len = cell.data.len();
        let Some(next_offset) = content_offset.checked_sub(cell_len) else {
            return Err(FrankenError::internal(format!(
                "build_page overflow: page_type={page_type:?} header_offset={header_offset} \
                 page_size={full_page_size} usable_size={usable} content_offset={content_offset} cell_len={cell_len} \
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
                 pointer_end={pointer_array_end} content_offset={content_offset} cells={} usable={usable}",
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
#[cfg(test)]
fn insert_cell_into_page<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    page_no: PageNumber,
    _usable_size: u32,
    cell_data: &[u8],
) -> Result<()> {
    let mut page_data = writer.read_page_data(cx, page_no)?;
    let offset = header_offset_for_page(page_no);
    let mut header = parse_page_header(page_data.as_bytes(), page_no)?;
    let mut ptrs = read_cell_pointers(page_data.as_bytes(), &header, offset)?;

    let cell_len = cell_data.len();
    let new_content_offset = header
        .content_offset(_usable_size)
        .checked_sub(cell_len)
        .ok_or_else(|| FrankenError::internal("cell too large for page content area"))?;

    // Check there's room for the cell + pointer.
    let ptr_array_end = offset
        + header.page_type.header_size() as usize
        + (header.cell_count as usize + 1) * CELL_POINTER_SIZE as usize;

    if ptr_array_end > new_content_offset {
        return Err(FrankenError::internal(
            "insufficient space for cell insertion (parent page overflow)",
        ));
    }

    // Add cell pointer.
    #[allow(clippy::cast_possible_truncation)]
    ptrs.push(new_content_offset as u16);

    // Update header.
    header.cell_count += 1;
    #[allow(clippy::cast_possible_truncation)]
    {
        header.cell_content_offset = new_content_offset as u32;
    }
    {
        let page_bytes = page_data.as_bytes_mut();
        page_bytes[new_content_offset..new_content_offset + cell_len].copy_from_slice(cell_data);
        header.write(page_bytes, offset);
        write_cell_pointers(page_bytes, offset, &header, &ptrs);
    }

    writer.write_page_data(cx, page_no, page_data)
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
    page_size: u32,
    first_child: usize,
    old_sibling_count: usize,
    new_pgnos: &[PageNumber],
    new_dividers: &[(PageNumber, Vec<u8>)],
    parent_is_root: bool,
) -> Result<BalanceResult> {
    let page_data = writer.read_page_data(cx, parent_page_no)?;
    let offset = header_offset_for_page(parent_page_no);
    let header = parse_page_header(page_data.as_bytes(), parent_page_no)?;
    let ptrs = read_cell_pointers(page_data.as_bytes(), &header, offset)?;
    let total_children = header.cell_count as usize + 1;
    let touches_rightmost = first_child + old_sibling_count == total_children;

    // Number of old divider cells to remove.
    let old_divider_count = old_sibling_count.saturating_sub(1);

    // Collect cells to keep: everything except the old dividers.
    let mut kept_cells: Vec<GatheredCell> =
        Vec::with_capacity(ptrs.len().saturating_sub(old_divider_count));

    for (i, &ptr) in ptrs.iter().enumerate() {
        if i >= first_child && i < first_child + old_divider_count {
            continue; // Skip old divider.
        }
        let cell_ref = CellRef::parse(
            page_data.as_bytes(),
            ptr as usize,
            header.page_type,
            usable_size,
        )?;
        let cell_end = ptr as usize + cell_on_page_size_from_ref(&cell_ref, ptr as usize);
        let raw = page_data.as_bytes()[ptr as usize..cell_end].to_vec();
        kept_cells.push(GatheredCell {
            size: u16::try_from(raw.len()).unwrap_or(u16::MAX),
            data: raw,
        });
    }

    // Insert new divider cells at the correct position.
    let insert_pos = first_child;
    let mut final_cells: Vec<GatheredCell> =
        Vec::with_capacity(kept_cells.len() + new_dividers.len());

    for cell in &kept_cells[..insert_pos] {
        final_cells.push(cell.clone());
    }

    for (_, div_data) in new_dividers {
        final_cells.push(GatheredCell {
            size: u16::try_from(div_data.len()).unwrap_or(u16::MAX),
            data: div_data.clone(),
        });
    }

    for cell in &kept_cells[insert_pos..] {
        final_cells.push(cell.clone());
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
        let last_pgno = *new_pgnos.last().ok_or_else(|| {
            FrankenError::internal(format!(
                "apply_child_replacement: new_pgnos empty despite guard for page {}",
                parent_page_no
            ))
        })?;
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
                page_size,
                offset,
                header.page_type,
                page_data.as_bytes(),
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
            page_size,
            offset,
            header.page_type,
            &page_data.as_bytes()[..offset],
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
        page_size,
        right_child,
    )?;

    // Preserve database header on page 1.
    let mut final_page = new_page;
    if offset > 0 {
        final_page[..offset].copy_from_slice(&page_data.as_bytes()[..offset]);
    }

    write_page_if_changed(
        cx,
        writer,
        parent_page_no,
        final_page,
        Some(page_data.as_bytes()),
    )?;

    // Balance-shallower: when the root page has zero cells after merging
    // children, copy the single right-child's content into the root and
    // free the child page, reducing tree depth by one.  This is the
    // inverse of balance_deeper and corresponds to SQLite's
    // "balance-shallower" sub-algorithm in the canonical upstream implementation.
    if parent_is_root && final_cells.is_empty() {
        if let Some(child_pgno) = right_child {
            balance_shallower(
                cx,
                writer,
                parent_page_no,
                child_pgno,
                usable_size,
                page_size,
            )?;
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
    page_size: u32,
) -> Result<()> {
    let child_data = writer.read_page_data(cx, child_pgno)?;
    let root_offset = header_offset_for_page(root_page_no);
    let child_offset = header_offset_for_page(child_pgno);
    let child_header = parse_page_header(child_data.as_bytes(), child_pgno)?;

    // Rebuild child cells using the root's header offset. This handles page-1
    // (100-byte header) safely and avoids raw offset shifting pitfalls.
    let child_ptrs = read_cell_pointers(child_data.as_bytes(), &child_header, child_offset)?;
    let mut child_cells: Vec<GatheredCell> = Vec::with_capacity(child_ptrs.len());
    for &ptr in &child_ptrs {
        let cell_offset = usize::from(ptr);
        let cell_ref = CellRef::parse(
            child_data.as_bytes(),
            cell_offset,
            child_header.page_type,
            usable_size,
        )?;
        let cell_end = cell_offset + cell_on_page_size_from_ref(&cell_ref, cell_offset);
        let data = child_data.as_bytes()[cell_offset..cell_end].to_vec();
        let size = u16::try_from(data.len()).map_err(|_| FrankenError::DatabaseCorrupt {
            detail: "cell too large during balance_shallower".to_owned(),
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
        page_size,
        child_header.right_child,
    )?;

    // Preserve the database file header on page 1.
    if root_offset > 0 {
        let original_root = writer.read_page_data(cx, root_page_no)?;
        new_root[..root_offset].copy_from_slice(&original_root.as_bytes()[..root_offset]);
    }

    writer.write_page_data(cx, root_page_no, PageData::from_vec(new_root))?;
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
    page_size: u32,
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

    for child_count in 2usize..=8usize {
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
        match writer.allocate_page(cx) {
            Ok(pgno) => child_pgnos.push(pgno),
            Err(err) => {
                free_pages_best_effort(cx, writer, &child_pgnos);
                return Err(err);
            }
        }
    }

    for (i, (start, end)) in ranges.iter().copied().enumerate() {
        let child_offset = header_offset_for_page(child_pgnos[i]);
        let child_cells = &final_cells[start..end];
        if !page_fits(child_cells, page_type, child_offset, usable_size) {
            free_pages_best_effort(cx, writer, &child_pgnos);
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
        free_pages_best_effort(cx, writer, &child_pgnos);
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
        let page = match build_page(
            child_cells,
            page_type,
            child_offset,
            usable_size,
            page_size,
            Some(right_children[i]),
        ) {
            Ok(page) => page,
            Err(err) => {
                free_pages_best_effort(cx, writer, &child_pgnos);
                return Err(err);
            }
        };
        if let Err(err) = writer.write_page_data(cx, child_pgnos[i], PageData::from_vec(page)) {
            free_pages_best_effort(cx, writer, &child_pgnos);
            return Err(err);
        }
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
        page_size,
        Some(root_right),
    )?;
    if root_offset > 0 {
        new_root[..root_offset].copy_from_slice(root_prefix);
    }
    match writer.write_page_data(cx, root_page_no, PageData::from_vec(new_root)) {
        Ok(()) => Ok(()),
        Err(err) => {
            free_pages_best_effort(cx, writer, &child_pgnos);
            Err(err)
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn split_overflowing_nonroot_interior_page<W: PageWriter>(
    cx: &Cx,
    writer: &mut W,
    page_no: PageNumber,
    usable_size: u32,
    page_size: u32,
    page_offset: usize,
    page_type: BtreePageType,
    original_page: &[u8],
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
        match writer.allocate_page(cx) {
            Ok(pgno) => child_pgnos.push(pgno),
            Err(err) => {
                free_pages_best_effort(cx, writer, &child_pgnos[1..]);
                return Err(err);
            }
        }
    }

    macro_rules! do_rollback2 {
        ($err:expr) => {{
            let _ = writer.write_page(cx, page_no, original_page);
            free_pages_best_effort(cx, writer, &child_pgnos[1..]);
            $err
        }};
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

    let mut pending_child_writes: Vec<(PageNumber, Vec<u8>, bool)> =
        Vec::with_capacity(child_count);
    for (i, (start, end)) in ranges.iter().copied().enumerate() {
        let child_pgno = child_pgnos[i];
        let child_off = header_offset_for_page(child_pgno);
        let page = match build_page(
            &final_cells[start..end],
            page_type,
            child_off,
            usable_size,
            page_size,
            Some(right_children[i]),
        ) {
            Ok(page) => page,
            Err(err) => return Err(do_rollback2!(err)),
        };
        let mut final_page = page;
        if i == 0 && child_off > 0 {
            final_page[..child_off].copy_from_slice(&original_page[..child_off]);
        }
        pending_child_writes.push((child_pgno, final_page, i == 0));
    }

    pending_child_writes.sort_by_key(|(pgno, _, _)| pgno.get());
    for (child_pgno, final_page, is_original_page) in pending_child_writes {
        if let Err(err) = write_page_if_changed(
            cx,
            writer,
            child_pgno,
            final_page,
            if is_original_page {
                Some(original_page)
            } else {
                None
            },
        ) {
            return Err(do_rollback2!(err));
        }
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
    use fsqlite_types::WitnessKey;
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
        fn free_page(&mut self, _cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.pages.remove(&page_no.get());
            Ok(())
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Clone)]
    struct FailingMemPageStore {
        inner: MemPageStore,
        fail_on_write: usize,
        write_calls: usize,
    }

    impl FailingMemPageStore {
        fn new(inner: MemPageStore, fail_on_write: usize) -> Self {
            Self {
                inner,
                fail_on_write,
                write_calls: 0,
            }
        }
    }

    impl crate::cursor::PageReader for FailingMemPageStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.inner.read_page(cx, page_no)
        }
    }

    impl PageWriter for FailingMemPageStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            self.write_calls = self.write_calls.saturating_add(1);
            if self.write_calls == self.fail_on_write {
                return Err(FrankenError::internal(format!(
                    "injected write failure on page {}",
                    page_no.get()
                )));
            }
            self.inner.write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }

        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    #[derive(Debug, Clone)]
    struct RecordingMemPageStore {
        inner: MemPageStore,
        writes_by_page: HashMap<u32, usize>,
    }

    impl RecordingMemPageStore {
        fn new(inner: MemPageStore) -> Self {
            Self {
                inner,
                writes_by_page: HashMap::new(),
            }
        }

        fn write_count(&self, page_no: PageNumber) -> usize {
            self.writes_by_page
                .get(&page_no.get())
                .copied()
                .unwrap_or(0)
        }
    }

    impl crate::cursor::PageReader for RecordingMemPageStore {
        fn read_page(&self, cx: &Cx, page_no: PageNumber) -> Result<Vec<u8>> {
            self.inner.read_page(cx, page_no)
        }
    }

    impl PageWriter for RecordingMemPageStore {
        fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
            *self.writes_by_page.entry(page_no.get()).or_default() += 1;
            self.inner.write_page(cx, page_no, data)
        }

        fn allocate_page(&mut self, cx: &Cx) -> Result<PageNumber> {
            self.inner.allocate_page(cx)
        }

        fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> Result<()> {
            self.inner.free_page(cx, page_no)
        }
        fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
    }

    fn pn(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    /// Build a leaf table page with sorted (rowid, payload) entries.
    #[allow(clippy::cast_sign_loss)]
    fn build_leaf_table(entries: &[(i64, &[u8])]) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let mut content_offset = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::with_capacity(entries.len());

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

    #[allow(clippy::cast_sign_loss)]
    fn build_leaf_table_cell(rowid: i64, payload: &[u8]) -> Vec<u8> {
        let mut cell_buf = vec![0u8; payload.len() + 18];
        let mut pos = 0usize;
        pos += write_varint(&mut cell_buf[pos..], payload.len() as u64);
        pos += write_varint(&mut cell_buf[pos..], rowid as u64);
        cell_buf[pos..pos + payload.len()].copy_from_slice(payload);
        pos += payload.len();
        cell_buf.truncate(pos);
        cell_buf
    }

    fn fixed_cost_cells(count: usize, payload_len: usize) -> Vec<GatheredCell> {
        (0..count)
            .map(|_| GatheredCell {
                data: vec![0x5A; payload_len],
                size: u16::try_from(payload_len).unwrap_or(u16::MAX),
            })
            .collect()
    }

    /// Build an interior table page with divider cells + right_child.
    #[allow(clippy::cast_sign_loss)]
    fn build_interior_table(cells: &[(PageNumber, i64)], right_child: PageNumber) -> Vec<u8> {
        let mut page = vec![0u8; USABLE as usize];
        let mut content_offset = USABLE as usize;
        let mut cell_offsets: Vec<u16> = Vec::with_capacity(cells.len());

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

    #[allow(clippy::cast_sign_loss)]
    fn build_interior_table_cell(left_child: PageNumber, rowid: i64) -> Vec<u8> {
        let mut cell_buf = [0u8; 64];
        cell_buf[0..4].copy_from_slice(&left_child.get().to_be_bytes());
        let vlen = write_varint(&mut cell_buf[4..], rowid as u64);
        cell_buf[..4 + vlen].to_vec()
    }

    // -- balance_deeper tests --

    #[test]
    fn test_balance_deeper_basic() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(10);

        // Create a leaf table root page (page 2) with some cells.
        let root = build_leaf_table(&[(1, b"aaa"), (2, b"bbb"), (3, b"ccc")]);
        store.pages.insert(2, root);

        let child_pgno = balance_deeper(&cx, &mut store, pn(2), USABLE, USABLE).unwrap();

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

        let child_pgno = balance_deeper(&cx, &mut store, pn(3), USABLE, USABLE).unwrap();

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

        let child_pgno = balance_deeper(&cx, &mut store, pn(5), USABLE, USABLE).unwrap();

        // Root should be interior with 0 cells.
        let root_data = store.pages.get(&5).unwrap();
        let root_header = BtreePageHeader::parse(root_data, 0).unwrap();
        assert_eq!(root_header.page_type, BtreePageType::InteriorTable);
        assert_eq!(root_header.cell_count, 0);
        assert_eq!(root_header.right_child, Some(child_pgno));
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
        // With one existing pointer, cell_content_offset = 20 leaves 6 bytes
        // of pointer/content gap. Even the smallest divider needs 7 bytes, so
        // balance_quick should fail.
        let mut full_parent = vec![0u8; USABLE as usize];
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 1, // must be > 0 for content_offset to use cell_content_offset
            cell_content_offset: 20, // artificially low
            fragmented_free_bytes: 0,
            right_child: Some(pn(3)),
        };
        header.write(&mut full_parent, 0);
        store.pages.insert(2, full_parent);

        store.pages.insert(3, build_leaf_table(&[(10, b"ten")]));

        let result = balance_quick(
            &cx,
            &mut store,
            pn(2),
            pn(3),
            b"overflow",
            30,
            USABLE,
            USABLE,
        )
        .unwrap();

        assert!(
            result.is_none(),
            "balance_quick should return None when parent is full"
        );
    }

    #[test]
    fn test_balance_quick_uses_exact_divider_space() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // One existing pointer plus cell_content_offset = 22 leaves 8 bytes
        // for the new pointer and divider. The old worst-case gate required
        // 15 bytes, but rowid 10 has a one-byte varint, so the actual divider
        // update needs only 7 bytes.
        let mut parent = vec![0u8; USABLE as usize];
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 1,
            cell_content_offset: 22,
            fragmented_free_bytes: 0,
            right_child: Some(pn(3)),
        };
        header.write(&mut parent, 0);
        store.pages.insert(2, parent);
        store.pages.insert(3, build_leaf_table(&[(10, b"ten")]));

        let mut overflow_cell = [0u8; 64];
        let mut pos = 0;
        pos += write_varint(&mut overflow_cell[pos..], 5);
        pos += write_varint(&mut overflow_cell[pos..], 30);
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
            USABLE,
        )
        .expect("quick balance should not fail")
        .expect("exact divider space should allow quick balance");

        let parent_data = store.pages.get(&2).unwrap();
        let parent_header = BtreePageHeader::parse(parent_data, 0).unwrap();
        assert_eq!(parent_header.cell_count, 2);
        assert_eq!(parent_header.cell_content_offset, 17);
        assert_eq!(parent_header.right_child, Some(new_pgno));
    }

    #[test]
    fn test_balance_quick_parent_write_failure_frees_new_page_and_preserves_parent() {
        let cx = Cx::new();
        let mut store = FailingMemPageStore::new(MemPageStore::new(20), 2);

        let original_parent = build_interior_table(&[(pn(4), 5)], pn(3));
        store.inner.pages.insert(2, original_parent.clone());
        store
            .inner
            .pages
            .insert(3, build_leaf_table(&[(10, b"ten")]));

        let mut overflow_cell = [0u8; 64];
        let mut pos = 0;
        pos += write_varint(&mut overflow_cell[pos..], 5);
        pos += write_varint(&mut overflow_cell[pos..], 30);
        overflow_cell[pos..pos + 5].copy_from_slice(b"hello");
        pos += 5;

        let err = balance_quick(
            &cx,
            &mut store,
            pn(2),
            pn(3),
            &overflow_cell[..pos],
            30,
            USABLE,
            USABLE,
        )
        .expect_err("injected parent insert failure should propagate");
        assert!(err.to_string().contains("injected write failure"));
        assert_eq!(store.inner.pages.get(&2), Some(&original_parent));
        assert!(
            !store.inner.pages.contains_key(&20),
            "failed quick balance must free the allocated sibling page"
        );
    }

    #[test]
    fn test_balance_quick_writes_parent_once() {
        let cx = Cx::new();
        let mut store = RecordingMemPageStore::new(MemPageStore::new(20));

        let original_parent = build_interior_table(&[(pn(4), 5)], pn(3));
        store.inner.pages.insert(2, original_parent);
        store
            .inner
            .pages
            .insert(3, build_leaf_table(&[(10, b"ten"), (20, b"twenty")]));

        let mut overflow_cell = [0u8; 64];
        let mut pos = 0;
        pos += write_varint(&mut overflow_cell[pos..], 5);
        pos += write_varint(&mut overflow_cell[pos..], 30);
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
            USABLE,
        )
        .expect("quick balance should succeed")
        .expect("quick balance should allocate a sibling");

        assert_eq!(
            store.write_count(pn(2)),
            1,
            "quick balance should write the parent once"
        );
        assert_eq!(
            store.write_count(new_pgno),
            1,
            "quick balance should write the new sibling once"
        );
    }

    #[test]
    fn test_leaf_table_split_policy_tracks_insert_heat() {
        let left = leaf_table_split_policy_for_page(None, 12, 0);
        let interior = leaf_table_split_policy_for_page(None, 12, 5);
        let right = leaf_table_split_policy_for_page(None, 12, 11);

        assert_eq!(left.heat, LeafTableSplitHeat::LeftEdge);
        assert_eq!(left.target_left_basis_points, 4_500);
        assert_eq!(interior.heat, LeafTableSplitHeat::Interior);
        assert_eq!(interior.target_left_basis_points, 5_500);
        assert_eq!(right.heat, LeafTableSplitHeat::RightEdge);
        assert_eq!(right.target_left_basis_points, 6_500);
    }

    #[test]
    fn test_leaf_table_split_heat_classifies_edge_window_boundaries() {
        // page_no=None keeps this purely deterministic (no topology/global state):
        // heat is decided solely by `edge_window = cell_count.div_ceil(4)`.
        let heat =
            |cells: usize, idx: usize| leaf_table_split_policy_for_page(None, cells, idx).heat;

        // cell_count = 8 -> edge_window = 2. Left:[0,1] Interior:[2..=5] Right:[6,7].
        assert_eq!(heat(8, 0), LeafTableSplitHeat::LeftEdge);
        assert_eq!(heat(8, 1), LeafTableSplitHeat::LeftEdge);
        assert_eq!(
            heat(8, 2),
            LeafTableSplitHeat::Interior,
            "left/interior boundary"
        );
        assert_eq!(heat(8, 5), LeafTableSplitHeat::Interior);
        assert_eq!(
            heat(8, 6),
            LeafTableSplitHeat::RightEdge,
            "interior/right boundary"
        );
        assert_eq!(heat(8, 7), LeafTableSplitHeat::RightEdge);

        // cell_count = 4 -> edge_window = 1. Left:[0] Interior:[1,2] Right:[3].
        assert_eq!(heat(4, 0), LeafTableSplitHeat::LeftEdge);
        assert_eq!(heat(4, 1), LeafTableSplitHeat::Interior);
        assert_eq!(heat(4, 3), LeafTableSplitHeat::RightEdge);

        // cell_count = 2 -> edge_window = 1, no interior band: idx 0 left, idx 1 right.
        assert_eq!(heat(2, 0), LeafTableSplitHeat::LeftEdge);
        assert_eq!(heat(2, 1), LeafTableSplitHeat::RightEdge);

        // Degenerate pages (< 2 cells) take the early branch and are always Interior.
        assert_eq!(heat(1, 0), LeafTableSplitHeat::Interior);
        assert_eq!(heat(0, 0), LeafTableSplitHeat::Interior);

        // An out-of-range insert position is clamped to the last cell (right edge).
        assert_eq!(
            heat(8, 99),
            LeafTableSplitHeat::RightEdge,
            "clamped to cell_count-1"
        );

        // With page_no=None the effective target is the heat class's baseline fill factor.
        assert_eq!(
            leaf_table_split_policy_for_page(None, 8, 0).target_left_basis_points,
            4_500
        );
        assert_eq!(
            leaf_table_split_policy_for_page(None, 8, 3).target_left_basis_points,
            5_500
        );
        assert_eq!(
            leaf_table_split_policy_for_page(None, 8, 7).target_left_basis_points,
            6_500
        );
    }

    #[test]
    fn test_choose_leaf_table_split_index_biases_space_toward_predicted_hot_side() {
        let cells = fixed_cost_cells(12, 180);
        let left = choose_leaf_table_split_index(
            &cells,
            0,
            0,
            USABLE,
            leaf_table_split_policy_for_page(None, cells.len(), 0),
        )
        .expect("left-edge split");
        let interior = choose_leaf_table_split_index(
            &cells,
            0,
            0,
            USABLE,
            leaf_table_split_policy_for_page(None, cells.len(), cells.len() / 2),
        )
        .expect("interior split");
        let right = choose_leaf_table_split_index(
            &cells,
            0,
            0,
            USABLE,
            leaf_table_split_policy_for_page(None, cells.len(), cells.len() - 1),
        )
        .expect("right-edge split");

        assert!(
            left < interior,
            "left-hot inserts should keep more slack on the left page"
        );
        assert!(
            interior < right,
            "right-hot inserts should keep more slack on the new right page"
        );
    }

    #[test]
    fn test_choose_leaf_table_split_index_keeps_both_pages_nonempty() {
        // The split index must leave both pages non-empty (1 <= split <= n-1)
        // for every heat policy and cell count — a split at 0 or n would pile
        // every cell onto one side (a correctness bug / non-terminating
        // balance). Fewer than 2 cells cannot split. The existing bias test
        // checks split *ordering* only, not this bound or the < 2 edge.
        for n in [2usize, 3, 8, 12, 50] {
            let cells = fixed_cost_cells(n, 64);
            for hot_idx in [0, n / 2, n - 1] {
                let policy = leaf_table_split_policy_for_page(None, n, hot_idx);
                let split = choose_leaf_table_split_index(&cells, 0, 0, USABLE, policy)
                    .expect("a split must exist for n >= 2 with small cells");
                assert!(
                    (1..n).contains(&split),
                    "split {split} must keep both pages non-empty (n={n}, hot_idx={hot_idx})"
                );
            }
        }

        // Fewer than 2 cells: nothing to split.
        assert_eq!(
            choose_leaf_table_split_index(
                &fixed_cost_cells(1, 64),
                0,
                0,
                USABLE,
                leaf_table_split_policy_for_page(None, 1, 0)
            ),
            None
        );
        assert_eq!(
            choose_leaf_table_split_index(
                &fixed_cost_cells(0, 64),
                0,
                0,
                USABLE,
                leaf_table_split_policy_for_page(None, 0, 0)
            ),
            None
        );

        // Equal-cost cells with interior heat (55% target) land the split just
        // right of center, never at an extreme. The per-cell cost cancels in the
        // fraction, so 20 cells -> ~11 regardless of cell size.
        let cells = fixed_cost_cells(20, 64);
        let interior = leaf_table_split_policy_for_page(None, 20, 10);
        let split = choose_leaf_table_split_index(&cells, 0, 0, USABLE, interior).expect("split");
        assert!(
            (10..=12).contains(&split),
            "interior 55% split should sit near 11, got {split}"
        );
    }

    #[test]
    fn test_conflict_heat_adjusts_leaf_table_split_target_for_hot_page() {
        let _guard = crate::instrumentation::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hot_page = pn(44);
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::record_conflict_topology_heat(hot_page, 3, 4);

        let baseline = leaf_table_split_policy_for_page(None, 12, 11);
        let heated = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        let cells = fixed_cost_cells(12, 180);
        let baseline_split =
            choose_leaf_table_split_index(&cells, 0, 0, USABLE, baseline).expect("baseline split");
        let heated_split =
            choose_leaf_table_split_index(&cells, 0, 0, USABLE, heated).expect("heated split");

        assert!(heated.topology_advice.applied);
        assert_eq!(heated.target_left_basis_points, 8_000);
        assert!(
            heated_split > baseline_split,
            "right-edge hot pages should leave more slack on the new right sibling"
        );

        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
    }

    #[test]
    fn test_conflict_topology_policy_mode_consistency_advisory_does_not_enact() {
        // bd-1dp9.6.7.13.2 AC#3 (mode consistency): the conflict-topology split
        // policy must compute the SAME advice across advisory and enforced modes
        // and differ ONLY in whether it is enacted; baseline mode must ignore the
        // heat entirely; and selecting baseline (the kill switch) must revert to
        // the byte-identical baseline split target. This is the policy-stability
        // / advisory-vs-enforced-consistency guarantee, independent of the
        // bench-gated p95/p99 throughput question.
        let _guard = crate::instrumentation::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hot_page = pn(46);

        // Cold baseline target for this (cell_count, insert_idx) shape (no page,
        // so no topology heat is consulted).
        let baseline_target =
            leaf_table_split_policy_for_page(None, 12, 11).target_left_basis_points;

        // ENFORCED: heat shifts the *enacted* target off baseline.
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::record_conflict_topology_heat(hot_page, 3, 4);
        let enforced = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        assert!(
            enforced.topology_advice.applied,
            "enforced mode must enact the topology advice for a hot page"
        );
        assert_ne!(
            enforced.target_left_basis_points, baseline_target,
            "enforced mode with heat must shift the enacted split target off baseline"
        );
        assert_eq!(
            enforced.topology_advice.advised_target_left_basis_points,
            enforced.target_left_basis_points,
            "enforced: the advised target is the one actually enacted"
        );

        // ADVISORY: identical heat. The advice is COMPUTED (advised target matches
        // what enforced enacts) but NOT applied (effective target stays baseline).
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Advisory,
        );
        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::record_conflict_topology_heat(hot_page, 3, 4);
        let advisory = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        assert!(
            !advisory.topology_advice.applied,
            "advisory mode must NOT enact the advice"
        );
        assert_eq!(
            advisory.target_left_basis_points, baseline_target,
            "advisory mode must leave the enacted split target at baseline"
        );
        assert_eq!(
            advisory.topology_advice.advised_target_left_basis_points,
            enforced.target_left_basis_points,
            "advisory and enforced must compute the SAME advice; only enactment differs"
        );

        // BASELINE (kill switch): identical heat is ignored entirely.
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Baseline,
        );
        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::record_conflict_topology_heat(hot_page, 3, 4);
        let baseline_mode = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        assert!(
            !baseline_mode.topology_advice.applied,
            "baseline mode (kill switch) must not enact the advice"
        );
        assert_eq!(
            baseline_mode.target_left_basis_points, baseline_target,
            "baseline mode (kill switch) must revert to the byte-identical baseline target"
        );

        // Restore the enforced default for sibling tests (mirrors existing tests).
        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
    }

    #[test]
    fn test_pathological_conflict_heat_deflects_leaf_table_split_once_bounded() {
        let _guard = crate::instrumentation::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hot_page = pn(45);
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
        crate::instrumentation::reset_conflict_topology_policy_state();
        for _ in 0..64 {
            crate::instrumentation::record_conflict_topology_heat(hot_page, 1, 4);
        }

        let first = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        let second = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        let exhausted = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);

        assert!(first.topology_advice.deflection_applied());
        assert_eq!(first.target_left_basis_points, 9_000);
        assert_eq!(first.topology_advice.deflection_credits_after, 1);
        assert!(second.topology_advice.deflection_applied());
        assert_eq!(second.target_left_basis_points, 9_000);
        assert_eq!(second.topology_advice.deflection_credits_after, 0);
        assert!(!exhausted.topology_advice.deflection_applied());
        assert_eq!(exhausted.target_left_basis_points, 8_000);
        assert_eq!(
            exhausted.topology_advice.rollback_reason,
            "budget_exhausted"
        );

        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
    }

    #[test]
    fn test_conflict_topology_split_never_produces_invalid_or_overflowing_split() {
        // bd-1dp9.6.7.13.2 AC#3 (B-tree invariant safety): the conflict-topology
        // fill-factor bias can only PREFER a capacity-valid split; it can never
        // force an invalid one. choose_leaf_table_split_index guards every
        // candidate split against `usable` (skips any where either half would
        // overflow), so across the full heat spectrum — baseline, advisory, and
        // enforced driven into the pathological deflection band — the chosen
        // split index must stay in [1, n-1] (both halves non-empty) and always
        // represent a both-halves-fit split. This proves the contention-aware
        // allocator can never corrupt a page split, no matter how aggressive the
        // heat-driven bias.
        let _guard = crate::instrumentation::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Configs that genuinely require a split (total cell cost > usable) yet
        // still fit within two pages.
        let configs = [
            (12usize, 380usize),
            (20, 250),
            (8, 600),
            (16, 360),
            (5, 1100),
        ];
        let hot_page = pn(60);
        for (count, payload) in configs {
            let cells = fixed_cost_cells(count, payload);
            for mode in [
                crate::instrumentation::ConflictTopologyPolicyMode::Baseline,
                crate::instrumentation::ConflictTopologyPolicyMode::Advisory,
                crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
            ] {
                crate::instrumentation::set_conflict_topology_policy_mode(mode);
                crate::instrumentation::reset_conflict_topology_policy_state();
                // Drive the page hot, including into the pathological deflection
                // band (the most aggressive fill-factor bias).
                for _ in 0..64 {
                    crate::instrumentation::record_conflict_topology_heat(hot_page, 1, 4);
                }
                // Cover left/interior/right insert positions (distinct heat classes).
                for insert_idx in [0usize, count / 2, count - 1] {
                    let policy =
                        leaf_table_split_policy_for_page(Some(hot_page), count, insert_idx);
                    let split = choose_leaf_table_split_index(&cells, 0, 0, USABLE, policy);
                    let idx = split.unwrap_or_else(|| {
                        panic!(
                            "topology split must exist for count={count} payload={payload} \
                             mode={mode:?} insert_idx={insert_idx}"
                        )
                    });
                    // Any Some(idx) from choose_leaf_table_split_index is, by the
                    // function's capacity guard, a both-halves-fit split; assert
                    // the index is also a non-empty split point.
                    assert!(
                        idx > 0 && idx < count,
                        "split index {idx} out of [1,{}] (count={count} payload={payload} \
                         mode={mode:?} insert_idx={insert_idx})",
                        count - 1
                    );
                }
            }
        }

        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
    }

    #[test]
    fn test_adaptive_fill_factor_biases_hot_leaf_split_further_when_enabled() {
        let _guard = crate::instrumentation::CONFLICT_TOPOLOGY_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let hot_page = pn(47);
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
        crate::instrumentation::reset_conflict_topology_policy_state();
        // Heat well inside the topology-hot band but below the deflection
        // threshold, so the topology target is the flat 8_000 right-edge cap.
        crate::instrumentation::record_conflict_topology_heat(hot_page, 33, 4);

        // Disabled (default): topology-only target, byte-identical to pre-adaptive.
        crate::instrumentation::set_adaptive_fill_factor_enabled(false);
        let topology_only = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        assert_eq!(topology_only.target_left_basis_points, 8_000);

        // Enabled: the adaptive ramp adds a bounded extra shift on top (750 bps at heat 33).
        crate::instrumentation::set_adaptive_fill_factor_enabled(true);
        let adaptive = leaf_table_split_policy_for_page(Some(hot_page), 12, 11);
        assert_eq!(adaptive.target_left_basis_points, 8_750);

        // A higher fill target keeps more payload on the left page, so the split
        // index lands at least as far right as the topology-only decision.
        let cells = fixed_cost_cells(12, 180);
        let topology_split =
            choose_leaf_table_split_index(&cells, 0, 0, USABLE, topology_only).expect("split");
        let adaptive_split =
            choose_leaf_table_split_index(&cells, 0, 0, USABLE, adaptive).expect("split");
        assert!(
            adaptive_split >= topology_split,
            "adaptive fill-factor should bias the split further toward the hot right edge"
        );

        crate::instrumentation::set_adaptive_fill_factor_enabled(false);
        crate::instrumentation::reset_conflict_topology_policy_state();
        crate::instrumentation::set_conflict_topology_policy_mode(
            crate::instrumentation::ConflictTopologyPolicyMode::Enforced,
        );
    }

    #[test]
    fn test_apply_child_replacement_noop_skips_parent_rewrite() {
        let cx = Cx::new();
        let mut store = RecordingMemPageStore::new(MemPageStore::new(20));

        let parent = build_interior_table(&[(pn(3), 40), (pn(4), 80)], pn(5));
        store.inner.pages.insert(2, parent.clone());

        let outcome = apply_child_replacement(
            &cx,
            &mut store,
            pn(2),
            USABLE,
            USABLE,
            1,
            1,
            &[pn(4)],
            &[],
            false,
        )
        .expect("no-op replacement should succeed");

        assert!(matches!(outcome, BalanceResult::Done));
        assert_eq!(
            store.write_count(pn(2)),
            0,
            "identical parent image should not be rewritten"
        );
        assert_eq!(store.inner.pages.get(&2), Some(&parent));
    }

    #[test]
    fn test_balance_table_leaf_local_split_only_touches_target_leaf_parent_and_new_sibling() {
        let cx = Cx::new();
        let mut base = MemPageStore::new(20);

        let payload = vec![b'm'; 240];
        let left_entries: Vec<(i64, &[u8])> = (10_i64..=12)
            .map(|rowid| (rowid, b"left" as &[u8]))
            .collect();
        let middle_entries: Vec<(i64, &[u8])> = (100_i64..=115)
            .map(|rowid| (rowid, payload.as_slice()))
            .collect();
        let right_entries: Vec<(i64, &[u8])> = (200_i64..=202)
            .map(|rowid| (rowid, b"right" as &[u8]))
            .collect();

        let parent = build_interior_table(&[(pn(3), 40), (pn(4), 150)], pn(5));
        let left_leaf = build_leaf_table(&left_entries);
        let middle_leaf = build_leaf_table(&middle_entries);
        let right_leaf = build_leaf_table(&right_entries);
        let original_middle_leaf = middle_leaf.clone();

        base.pages.insert(2, parent);
        base.pages.insert(3, left_leaf.clone());
        base.pages.insert(4, middle_leaf);
        base.pages.insert(5, right_leaf.clone());

        let mut store = RecordingMemPageStore::new(base);
        let overflow_cell = build_leaf_table_cell(50, payload.as_slice());

        let outcome = balance_table_leaf_local_split(
            &cx,
            &mut store,
            pn(2),
            1,
            pn(4),
            &overflow_cell,
            0,
            USABLE,
            USABLE,
            true,
        )
        .expect("local split should succeed")
        .expect("leaf table split should take the local fast path");

        assert!(matches!(outcome, BalanceResult::Done));
        assert_eq!(
            store.write_count(pn(2)),
            1,
            "parent should be updated exactly once"
        );
        assert_eq!(
            store.write_count(pn(4)),
            1,
            "target leaf should be rewritten exactly once"
        );
        assert_eq!(
            store.write_count(pn(20)),
            1,
            "local split should allocate and write exactly one new sibling"
        );
        assert_eq!(
            store.write_count(pn(3)),
            0,
            "left neighbor must remain untouched"
        );
        assert_eq!(
            store.write_count(pn(5)),
            0,
            "right neighbor must remain untouched"
        );
        assert_eq!(store.inner.pages.get(&3), Some(&left_leaf));
        assert_eq!(store.inner.pages.get(&5), Some(&right_leaf));
        assert_ne!(
            store.inner.pages.get(&4),
            Some(&original_middle_leaf),
            "target leaf should actually change when the split fires"
        );
    }

    #[test]
    fn test_balance_table_leaf_local_split_bails_when_parent_is_full() {
        let cx = Cx::new();
        let mut base = MemPageStore::new(20);

        let mut full_parent = vec![0u8; USABLE as usize];
        let header = BtreePageHeader {
            page_type: BtreePageType::InteriorTable,
            first_freeblock: 0,
            cell_count: 1,
            cell_content_offset: 20,
            fragmented_free_bytes: 0,
            right_child: Some(pn(5)),
        };
        header.write(&mut full_parent, 0);

        let payload = vec![b'm'; 240];
        let leaf_entries: Vec<(i64, &[u8])> = (100_i64..=115)
            .map(|rowid| (rowid, payload.as_slice()))
            .collect();
        let leaf = build_leaf_table(&leaf_entries);
        let original_leaf = leaf.clone();

        base.pages.insert(2, full_parent.clone());
        base.pages.insert(5, leaf);

        let mut store = RecordingMemPageStore::new(base);
        let overflow_cell = build_leaf_table_cell(50, payload.as_slice());

        let outcome = balance_table_leaf_local_split(
            &cx,
            &mut store,
            pn(2),
            1,
            pn(5),
            &overflow_cell,
            0,
            USABLE,
            USABLE,
            true,
        )
        .expect("parent-space gate should not error");

        assert!(
            outcome.is_none(),
            "local split should decline when parent lacks room for the divider"
        );
        assert_eq!(store.write_count(pn(2)), 0);
        assert_eq!(store.write_count(pn(5)), 0);
        assert_eq!(store.inner.pages.get(&2), Some(&full_parent));
        assert_eq!(store.inner.pages.get(&5), Some(&original_leaf));
        assert!(
            !store.inner.pages.contains_key(&20),
            "declined local split must not allocate a sibling page"
        );
    }

    // -- page_required_bytes / page_fits tests --

    #[test]
    fn test_page_required_bytes_and_page_fits_formula() {
        // page_required_bytes = header_offset + page header + 2 bytes per cell
        // pointer + the sum of cell payload lengths; page_fits compares that
        // total (<=) against the usable page size. These two helpers gate every
        // split/rebalance "does this set of cells fit" decision, so pin the
        // arithmetic, the leaf-vs-interior header difference, and the boundary.
        let cells: Vec<GatheredCell> = (0..3)
            .map(|_| GatheredCell {
                data: vec![0u8; 10],
                size: 10,
            })
            .collect();
        let payload = 30usize; // 3 cells * 10 payload bytes
        let ptrs = 3 * usize::from(CELL_POINTER_SIZE); // 3 * 2 = 6
        let leaf_hdr = usize::from(BtreePageType::LeafTable.header_size());
        let interior_hdr = usize::from(BtreePageType::InteriorTable.header_size());

        // Leaf table page, no page-1 header offset.
        let leaf = page_required_bytes(&cells, BtreePageType::LeafTable, 0).unwrap();
        assert_eq!(leaf, leaf_hdr + ptrs + payload);

        // Interior table page uses a larger page header; the cells are identical,
        // so the only difference from the leaf total is the header size.
        let interior = page_required_bytes(&cells, BtreePageType::InteriorTable, 0).unwrap();
        assert_eq!(interior, interior_hdr + ptrs + payload);
        assert_eq!(interior - leaf, interior_hdr - leaf_hdr);

        // The 100-byte page-1 database-header offset is added on top.
        let with_offset = page_required_bytes(&cells, BtreePageType::LeafTable, 100).unwrap();
        assert_eq!(with_offset, leaf + 100);

        // An empty cell list reduces to just header_offset + the page header.
        assert_eq!(
            page_required_bytes(&[], BtreePageType::LeafTable, 0).unwrap(),
            leaf_hdr
        );

        // page_fits is the `<=` comparison against usable: an exact fit passes,
        // one byte short fails.
        let exact = u32::try_from(leaf).unwrap();
        assert!(page_fits(&cells, BtreePageType::LeafTable, 0, exact));
        assert!(!page_fits(&cells, BtreePageType::LeafTable, 0, exact - 1));
    }

    // -- table_leaf_divider_bytes tests --

    #[test]
    fn test_table_leaf_divider_bytes_format() {
        // An interior-table divider cell is a 4-byte big-endian left-child page
        // number followed by the varint-encoded rowid of the rightmost cell on
        // the left page. This is what lets the parent route searches after a
        // table-leaf split, so pin the exact byte layout and that the trailing
        // varint round-trips the rowid with nothing after it -- across the 1-,
        // 2-, and 9-byte varint lengths.
        let left = PageNumber::new(7).unwrap();
        let divider_for = |rowid: i64| -> Vec<u8> {
            let data = build_leaf_table_cell(rowid, b"payload");
            let cell = GatheredCell {
                size: u16::try_from(data.len()).unwrap(),
                data,
            };
            table_leaf_divider_bytes(left, &cell, USABLE).unwrap()
        };

        for (rowid, varint_len) in [(5i64, 1usize), (300, 2), (i64::MAX, 9)] {
            let divider = divider_for(rowid);
            // First 4 bytes: the left child page number, big-endian.
            assert_eq!(&divider[0..4], &left.get().to_be_bytes());
            // Remaining bytes: the rowid as a varint, with nothing trailing.
            let (got, n) = fsqlite_types::serial_type::read_varint(&divider[4..]).unwrap();
            assert_eq!(
                got,
                u64::try_from(rowid).unwrap(),
                "divider must encode the cell rowid"
            );
            assert_eq!(n, varint_len, "varint byte length for rowid {rowid}");
            assert_eq!(
                divider.len(),
                4 + varint_len,
                "divider has no trailing bytes"
            );
        }
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

        let page = build_page(&cells, BtreePageType::LeafTable, 0, USABLE, USABLE, None)
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

        let err = build_page(&cells, BtreePageType::LeafTable, 100, USABLE, USABLE, None)
            .expect_err("page-1 style header offset should reject overlap");
        assert!(err.to_string().contains("layout overlap"));
    }

    // -- balance_nonroot tests --

    #[test]
    fn test_balance_nonroot_two_siblings_merge() {
        let cx = Cx::new();
        let mut store = MemPageStore::new(20);

        // Parent (page 2): 1 divider cell pointing to left=page 3, right=page 4.
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

        // Balance around child 0 (left child), no overflow.
        let outcome =
            balance_nonroot(&cx, &mut store, pn(2), 0, &[], 0, USABLE, USABLE, true).unwrap();
        assert!(matches!(outcome, BalanceResult::Done));

        // All four leaf-table cells fit on one page, so balance_shallower
        // collapses the root to a leaf. The parent divider key is not an
        // extra table row; it is only a separator copied from the left child.
        let root_data = store.pages.get(&2).unwrap();
        let root_header = BtreePageHeader::parse(root_data, 0).unwrap();
        assert!(
            root_header.page_type.is_leaf(),
            "root should collapse to leaf after small-cell merge with overflow"
        );
        // Original: 4 leaf rows total.
        assert_eq!(
            root_header.cell_count, 4,
            "all leaf-table rows should be preserved after merge"
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

        balance_shallower(&cx, &mut store, pn(1), pn(2), USABLE, USABLE)
            .expect("balance shallower");

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

    #[test]
    fn test_balance_nonroot_restores_siblings_when_rewrite_fails() {
        let cx = Cx::new();
        let mut base = MemPageStore::new(20);

        let parent = build_interior_table(&[(pn(3), 50)], pn(4));
        base.pages.insert(2, parent);
        base.pages
            .insert(3, build_leaf_table(&[(10, b"ten"), (50, b"fifty")]));
        base.pages
            .insert(4, build_leaf_table(&[(60, b"sixty"), (70, b"seventy")]));

        let original_left = base.pages.get(&3).cloned().unwrap();
        let original_right = base.pages.get(&4).cloned().unwrap();

        let mut store = FailingMemPageStore::new(base, 1);
        let result = balance_nonroot(&cx, &mut store, pn(2), 0, &[], 0, USABLE, USABLE, true);
        assert!(result.is_err(), "injected write failure should surface");
        assert_eq!(store.inner.pages.get(&3), Some(&original_left));
        assert_eq!(store.inner.pages.get(&4), Some(&original_right));
    }

    #[test]
    fn test_balance_nonroot_restores_parent_when_root_collapse_fails() {
        let cx = Cx::new();
        let mut base = MemPageStore::new(20);

        let parent = build_interior_table(&[(pn(3), 50)], pn(4));
        base.pages.insert(2, parent.clone());
        base.pages
            .insert(3, build_leaf_table(&[(10, b"ten"), (50, b"fifty")]));
        base.pages
            .insert(4, build_leaf_table(&[(60, b"sixty"), (70, b"seventy")]));

        let original_left = base.pages.get(&3).cloned().unwrap();
        let original_right = base.pages.get(&4).cloned().unwrap();

        let mut store = FailingMemPageStore::new(base, 3);
        let result = balance_nonroot(&cx, &mut store, pn(2), 0, &[], 0, USABLE, USABLE, true);
        assert!(
            result.is_err(),
            "injected root-collapse failure should surface"
        );
        assert_eq!(store.inner.pages.get(&2), Some(&parent));
        assert_eq!(store.inner.pages.get(&3), Some(&original_left));
        assert_eq!(store.inner.pages.get(&4), Some(&original_right));
    }

    #[test]
    fn test_split_overflowing_nonroot_interior_restores_original_page_on_failure() {
        let cx = Cx::new();
        let mut base = MemPageStore::new(20);
        let original_page = build_interior_table(&[(pn(10), 50), (pn(20), 100)], pn(30));
        base.pages.insert(2, original_page.clone());

        let final_cells: Vec<GatheredCell> = (0_u32..1_500)
            .map(|i| {
                let left_child = pn(1_000 + i);
                let rowid = i64::from(i + 1);
                let data = build_interior_table_cell(left_child, rowid);
                GatheredCell {
                    size: u16::try_from(data.len()).unwrap_or(u16::MAX),
                    data,
                }
            })
            .collect();

        let mut store = FailingMemPageStore::new(base, 3);
        let result = split_overflowing_nonroot_interior_page(
            &cx,
            &mut store,
            pn(2),
            USABLE,
            USABLE,
            0,
            BtreePageType::InteriorTable,
            &original_page,
            &final_cells,
            Some(pn(9_999)),
        );
        assert!(result.is_err(), "injected write failure should surface");
        assert_eq!(
            store.inner.pages.len(),
            1,
            "new siblings should be cleaned up"
        );
        assert_eq!(store.inner.pages.get(&2), Some(&original_page));
    }
}
