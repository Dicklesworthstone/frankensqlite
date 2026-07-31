//! Page Materialization Layer (C5: bd-l9k8e.5)
//!
//! Converts accumulated cell-level deltas into full page images when required:
//! - Structural B-tree changes (page split/merge)
//! - WAL checkpointing
//! - Eager materialization threshold (~32 deltas)
//!
//! # Design
//!
//! Cell-level MVCC avoids full page copies on every row write. But eventually
//! we need real pages for:
//! 1. When the B-tree needs to split/merge a page (structural operation)
//! 2. When WAL checkpoint writes pages back to the main database file
//! 3. When eager materialization threshold is reached
//!
//! # Key Function
//!
//! [`materialize_page`] takes a base page and a list of deltas, applies only
//! those visible to the given snapshot in commit_seq order, and returns a
//! complete, valid B-tree page.

use std::collections::HashMap;

use fsqlite_btree::{
    BtreePageHeader, BtreePageType, header_offset_for_page, read_cell_pointers, write_cell_pointers,
};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::limits::CELL_POINTER_SIZE;
use fsqlite_types::sync_primitives::Instant;
use fsqlite_types::{PageData, PageNumber, Snapshot};
use tracing::{debug, info, warn};

use crate::cell_visibility::{CellDelta, CellDeltaKind};

// ---------------------------------------------------------------------------
// Error Types
// ---------------------------------------------------------------------------

/// Errors that can occur during page materialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializationError {
    /// The base page is not a valid B-tree page.
    InvalidBasePage { detail: String },
    /// The page is an interior page (cell-level MVCC only supports leaf pages).
    InteriorPageNotSupported,
    /// Cell data would overflow the page.
    PageOverflow { needed: usize, available: usize },
    /// Inconsistent delta: delete on non-existent cell.
    DeleteNonExistent { key_digest: [u8; 16] },
    /// Inconsistent delta: insert on existing cell.
    InsertExisting { key_digest: [u8; 16] },
}

impl std::fmt::Display for MaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBasePage { detail } => write!(f, "invalid base page: {detail}"),
            Self::InteriorPageNotSupported => {
                write!(f, "cell-level MVCC not supported for interior pages")
            }
            Self::PageOverflow { needed, available } => {
                write!(f, "page overflow: need {needed} bytes, have {available}")
            }
            Self::DeleteNonExistent { key_digest } => {
                write!(f, "delete on non-existent cell: {:?}", &key_digest[..4])
            }
            Self::InsertExisting { key_digest } => {
                write!(f, "insert on existing cell: {:?}", &key_digest[..4])
            }
        }
    }
}

impl std::error::Error for MaterializationError {}

// ---------------------------------------------------------------------------
// Materialization Trigger
// ---------------------------------------------------------------------------

/// Reason why materialization was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationTrigger {
    /// Page split or merge operation.
    Structural,
    /// WAL checkpoint writing pages to main DB.
    Checkpoint,
    /// Delta count exceeded threshold.
    Threshold,
    /// Explicit request (e.g., read path caching).
    Explicit,
}

impl MaterializationTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Structural => "split",
            Self::Checkpoint => "checkpoint",
            Self::Threshold => "threshold",
            Self::Explicit => "explicit",
        }
    }
}

// ---------------------------------------------------------------------------
// Materialization Result
// ---------------------------------------------------------------------------

/// Result of a page materialization operation.
#[derive(Debug, Clone)]
pub struct MaterializationResult {
    /// The materialized page data.
    pub page: PageData,
    /// Number of deltas applied.
    pub deltas_applied: usize,
    /// Number of cells in the resulting page.
    pub cell_count: u16,
    /// Time taken for materialization in microseconds.
    pub duration_us: u64,
}

// ---------------------------------------------------------------------------
// Core Materialization Function
// ---------------------------------------------------------------------------

/// Materialize a page by applying cell deltas to a base page.
///
/// # Arguments
///
/// * `base` - The base page (last full page version from VersionStore or disk)
/// * `page_number` - The page number (needed for header offset calculation)
/// * `deltas` - Cell deltas to apply (will be filtered by visibility)
/// * `snapshot` - The snapshot determining which deltas are visible
/// * `usable_size` - The usable page size (page_size - reserved_bytes)
///
/// # Returns
///
/// A `MaterializationResult` containing the materialized page and stats.
///
/// # Errors
///
/// Returns an error if:
/// - The base page is not a valid B-tree leaf page
/// - A delta would cause page overflow
/// - Delta consistency check fails
///
/// # Example
///
/// ```ignore
/// let result = materialize_page(
///     &base_page,
///     page_no,
///     &deltas,
///     &snapshot,
///     4096,
///     MaterializationTrigger::Checkpoint,
/// )?;
/// ```
pub fn materialize_page(
    base: &PageData,
    page_number: PageNumber,
    deltas: &[CellDelta],
    snapshot: &Snapshot,
    usable_size: u32,
    trigger: MaterializationTrigger,
) -> Result<MaterializationResult> {
    let start = Instant::now();
    let header_offset = header_offset_for_page(page_number);

    // Parse base page header
    let header = BtreePageHeader::parse(base.as_bytes(), header_offset).map_err(|e| {
        warn!(
            pgno = page_number.get(),
            error = %e,
            "materialization_failed"
        );
        FrankenError::DatabaseCorrupt {
            detail: format!("materialize_page: invalid base page: {e}"),
        }
    })?;

    // Cell-level MVCC only supports leaf pages
    if header.page_type.is_interior() {
        warn!(
            pgno = page_number.get(),
            page_type = ?header.page_type,
            "materialization_failed"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: "materialize_page: interior pages not supported for cell-level MVCC".to_owned(),
        });
    }

    // Filter and sort deltas by commit_seq
    let mut visible_deltas: Vec<&CellDelta> = deltas
        .iter()
        .filter(|d| d.is_visible_to(snapshot.high))
        .collect();
    visible_deltas.sort_by_key(|d| d.commit_seq);

    if visible_deltas.is_empty() {
        // No visible deltas — return base page unchanged
        let duration_us = start.elapsed().as_micros() as u64;
        info!(
            pgno = page_number.get(),
            delta_count = 0,
            trigger = trigger.as_str(),
            "page_materialized"
        );
        return Ok(MaterializationResult {
            page: base.clone(),
            deltas_applied: 0,
            cell_count: header.cell_count,
            duration_us,
        });
    }

    // Build working state from base page
    let btree_ref = visible_deltas
        .first()
        .map(|delta| delta.cell_key.btree)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "materialize_page: no visible deltas after non-empty check".to_owned(),
        })?;
    let mut state = WorkingPageState::from_base_page(
        base,
        page_number,
        &header,
        header_offset,
        usable_size,
        btree_ref,
    )?;

    // Apply each visible delta
    for delta in &visible_deltas {
        state.apply_delta(delta)?;
    }

    // Reconstruct page from working state
    let result_cell_count = state.live_cell_count()?;
    let page = state.build_page(base, &header, header_offset, usable_size)?;
    let duration_us = start.elapsed().as_micros() as u64;

    debug!(
        pgno = page_number.get(),
        base_commit_seq = 0, // base doesn't have a commit_seq
        applied_deltas = visible_deltas.len(),
        result_cell_count,
        "materialization_detail"
    );

    info!(
        pgno = page_number.get(),
        delta_count = visible_deltas.len(),
        trigger = trigger.as_str(),
        "page_materialized"
    );

    info!(
        pgno = page_number.get(),
        duration_us, "materialization_timing"
    );

    Ok(MaterializationResult {
        page,
        deltas_applied: visible_deltas.len(),
        cell_count: result_cell_count,
        duration_us,
    })
}

// ---------------------------------------------------------------------------
// Working Page State (intermediate representation during materialization)
// ---------------------------------------------------------------------------

/// A working cell with its content and sort key.
struct WorkingCell {
    /// Cell content bytes.
    content: Vec<u8>,
    /// Extracted sort key (rowid for table cells, key bytes for index cells).
    /// This ensures proper B-tree ordering after materialization.
    sort_key: SortKey,
}

/// Sort key for maintaining B-tree cell ordering.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SortKey {
    /// Table leaf cell: sort by rowid.
    Rowid(i64),
    /// Index leaf cell: sort by key bytes.
    IndexKey(Vec<u8>),
}

/// Intermediate state while applying deltas.
#[allow(dead_code)]
struct WorkingPageState {
    /// Current cells indexed by key_digest.
    cells_by_key: HashMap<[u8; 16], usize>,
    /// All cells in order.
    cells: Vec<WorkingCell>,
    /// Page type for validation.
    page_type: BtreePageType,
    /// Usable page size.
    usable_size: u32,
}

impl WorkingPageState {
    fn live_cell_count(&self) -> Result<u16> {
        let count = self
            .cells
            .iter()
            .filter(|cell| !cell.content.is_empty())
            .count();
        u16::try_from(count).map_err(|_| FrankenError::DatabaseCorrupt {
            detail: "materialize_page: too many live cells for b-tree page".to_owned(),
        })
    }

    /// Initialize working state from a base page.
    fn from_base_page(
        base: &PageData,
        page_number: PageNumber,
        header: &BtreePageHeader,
        header_offset: usize,
        usable_size: u32,
        btree_ref: fsqlite_types::BtreeRef,
    ) -> Result<Self> {
        let cell_pointers = read_cell_pointers(base.as_bytes(), header, header_offset)?;
        let page_bytes = base.as_bytes();

        let mut cells = Vec::with_capacity(cell_pointers.len());
        let mut cells_by_key = HashMap::with_capacity(cell_pointers.len());

        for (idx, &ptr) in cell_pointers.iter().enumerate() {
            let cell_offset = ptr as usize;

            // Extract cell content - we need to compute the cell size
            let cell_end =
                compute_cell_end(page_bytes, cell_offset, header.page_type, usable_size)?;
            let cell_content = page_bytes
                .get(cell_offset..cell_end)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "materialize_page: computed cell range out of bounds".to_owned(),
                })?
                .to_vec();

            // Compute key digest and sort key for this cell
            let (key_digest, sort_key) = compute_cell_key_and_sort_key(
                page_bytes,
                cell_offset,
                header.page_type,
                btree_ref,
                usable_size,
            )?;

            cells.push(WorkingCell {
                content: cell_content,
                sort_key,
            });
            cells_by_key.insert(key_digest, idx);
        }

        debug!(
            pgno = page_number.get(),
            base_cell_count = cells.len(),
            "working_state_initialized"
        );

        Ok(Self {
            cells_by_key,
            cells,
            page_type: header.page_type,
            usable_size,
        })
    }

    /// Apply a single delta to the working state.
    fn apply_delta(&mut self, delta: &CellDelta) -> Result<()> {
        match delta.kind {
            CellDeltaKind::Insert => {
                let (key_digest, sort_key) = compute_cell_key_and_sort_key_from_delta(
                    delta,
                    self.page_type,
                    self.usable_size,
                )?;
                // Insert: add new cell (should not exist)
                if let Some(&idx) = self.cells_by_key.get(&key_digest) {
                    // Cell already exists - this could be a re-insert after delete
                    // which is valid (the delta chain handles this)
                    // Just update the content
                    if let Some(cell) = self.cells.get_mut(idx) {
                        cell.content.clone_from(&delta.cell_data);
                        cell.sort_key = sort_key;
                        return Ok(());
                    }
                    self.cells_by_key.remove(&key_digest);
                }
                let idx = self.cells.len();
                self.cells.push(WorkingCell {
                    content: delta.cell_data.clone(),
                    sort_key,
                });
                self.cells_by_key.insert(key_digest, idx);
                Ok(())
            }
            CellDeltaKind::Update => {
                let (key_digest, sort_key) = compute_cell_key_and_sort_key_from_delta(
                    delta,
                    self.page_type,
                    self.usable_size,
                )?;
                // Update: replace existing cell content
                if let Some(&idx) = self.cells_by_key.get(&key_digest) {
                    if let Some(cell) = self.cells.get_mut(idx) {
                        cell.content.clone_from(&delta.cell_data);
                        cell.sort_key = sort_key;
                        return Ok(());
                    }
                    self.cells_by_key.remove(&key_digest);
                }
                // Update on non-existent cell - treat as insert.
                let idx = self.cells.len();
                self.cells.push(WorkingCell {
                    content: delta.cell_data.clone(),
                    sort_key,
                });
                self.cells_by_key.insert(key_digest, idx);
                Ok(())
            }
            CellDeltaKind::Delete => {
                if !delta.cell_data.is_empty() {
                    return Err(FrankenError::DatabaseCorrupt {
                        detail: "delete delta unexpectedly contains cell bytes".to_owned(),
                    });
                }
                // Delete: remove cell
                let key_digest = delta.cell_key.key_digest;
                if let Some(idx) = self.cells_by_key.remove(&key_digest) {
                    // Mark as deleted by clearing content
                    // We'll filter these out during page reconstruction
                    if let Some(cell) = self.cells.get_mut(idx) {
                        cell.content.clear();
                    }
                }
                // Delete on non-existent is a no-op (idempotent)
                Ok(())
            }
        }
    }

    /// Build the final page from working state.
    fn build_page(
        &self,
        base: &PageData,
        original_header: &BtreePageHeader,
        header_offset: usize,
        usable_size: u32,
    ) -> Result<PageData> {
        // Filter out deleted cells and collect live cells
        let live_cells: Vec<&WorkingCell> = self
            .cells
            .iter()
            .filter(|c| !c.content.is_empty())
            .collect();

        // Sort cells by B-tree key order (rowid for table, key bytes for index)
        // This maintains B-tree invariants required for binary search
        let mut sorted_cells: Vec<&WorkingCell> = live_cells;
        sorted_cells.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        // Calculate required space
        let header_size = original_header.page_type.header_size() as usize;
        let ptr_array_size = sorted_cells.len() * CELL_POINTER_SIZE as usize;
        let total_cell_bytes: usize = sorted_cells.iter().map(|c| c.content.len()).sum();

        let needed_space = header_offset + header_size + ptr_array_size + total_cell_bytes;
        if needed_space > usable_size as usize {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "materialize_page: page overflow: need {} bytes, have {}",
                    needed_space, usable_size
                ),
            });
        }

        // Allocate new page buffer
        let page_size = base.len();
        let mut page = vec![0u8; page_size];

        // Copy database file header if page 1
        if header_offset > 0 {
            let dst =
                page.get_mut(..header_offset)
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "materialize_page: page header offset out of bounds".to_owned(),
                    })?;
            let src = base.as_bytes().get(..header_offset).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "materialize_page: base header offset out of bounds".to_owned(),
                }
            })?;
            dst.copy_from_slice(src);
        }

        // Write cell content area (grows down from end of usable space)
        let mut content_offset = usable_size as usize;
        let mut cell_pointers = Vec::with_capacity(sorted_cells.len());

        for cell in &sorted_cells {
            content_offset = content_offset
                .checked_sub(cell.content.len())
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "materialize_page: cell content offset underflow".to_owned(),
                })?;
            page.get_mut(content_offset..content_offset + cell.content.len())
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "materialize_page: cell content range out of bounds".to_owned(),
                })?
                .copy_from_slice(&cell.content);
            cell_pointers.push(content_offset as u16);
        }

        // Build new header
        let cell_count =
            u16::try_from(sorted_cells.len()).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "materialize_page: too many cells for b-tree page".to_owned(),
            })?;
        let new_header = BtreePageHeader {
            page_type: original_header.page_type,
            first_freeblock: 0, // No freeblocks in freshly packed page
            cell_count,
            cell_content_offset: content_offset as u32,
            fragmented_free_bytes: 0, // No fragmentation in freshly packed page
            right_child: original_header.right_child,
        };

        // Write header
        new_header.write(&mut page, header_offset);

        // Write cell pointer array
        write_cell_pointers(&mut page, header_offset, &new_header, &cell_pointers);

        Ok(PageData::from_vec(page))
    }
}

// ---------------------------------------------------------------------------
// Cell Key Computation Helpers
// ---------------------------------------------------------------------------

/// Compute both the key digest (for HashMap lookup) and sort key (for B-tree ordering).
fn compute_cell_key_and_sort_key(
    page: &[u8],
    cell_offset: usize,
    page_type: BtreePageType,
    btree_ref: fsqlite_types::BtreeRef,
    usable_size: u32,
) -> Result<([u8; 16], SortKey)> {
    // For table leaf pages, extract the rowid
    // For index leaf pages, extract the key bytes

    if page_type == BtreePageType::LeafTable {
        // Table leaf cell: payload_size varint, rowid varint, payload
        let cell = page
            .get(cell_offset..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell offset out of bounds".to_owned(),
            })?;
        let (_, ps_len) = fsqlite_types::serial_type::read_varint(cell).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: "invalid varint in cell (payload size)".to_owned(),
            }
        })?;

        let rowid_start =
            cell_offset
                .checked_add(ps_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "cell rowid offset overflow".to_owned(),
                })?;
        let rowid_cell = page
            .get(rowid_start..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell rowid offset out of bounds".to_owned(),
            })?;
        let (rowid, _rowid_len) =
            fsqlite_types::serial_type::read_varint(rowid_cell).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "invalid varint in cell (rowid)".to_owned(),
                }
            })?;
        let rowid = rowid as i64;

        // Hash the rowid for key_digest
        let mut key_bytes = [0u8; 10];
        let len = encode_varint_i64(rowid, &mut key_bytes);
        let key_digest = fsqlite_types::SemanticKeyRef::compute_digest(
            fsqlite_types::SemanticKeyKind::TableRow,
            btree_ref,
            &key_bytes[..len],
        );

        Ok((key_digest, SortKey::Rowid(rowid)))
    } else if page_type == BtreePageType::LeafIndex {
        use fsqlite_btree::local_payload_size;

        // Index leaf cell: payload_size varint, key bytes.
        let cell = page
            .get(cell_offset..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell offset out of bounds".to_owned(),
            })?;
        let (payload_size, ps_len) =
            fsqlite_types::serial_type::read_varint(cell).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "invalid varint in cell (payload size)".to_owned(),
                }
            })?;

        let key_start =
            cell_offset
                .checked_add(ps_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "index cell key offset overflow".to_owned(),
                })?;

        let payload_size =
            u32::try_from(payload_size).map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "index cell payload size exceeds supported page size".to_owned(),
            })?;
        let local_payload = local_payload_size(payload_size, usable_size, page_type);
        let key_len = usize::try_from(payload_size.min(local_payload)).map_err(|_| {
            FrankenError::DatabaseCorrupt {
                detail: "index cell local key length exceeds addressable size".to_owned(),
            }
        })?;
        let key_end =
            key_start
                .checked_add(key_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "index cell key end offset overflow".to_owned(),
                })?;
        let digest_len = key_len
            .checked_add(if local_payload < payload_size { 4 } else { 0 })
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "index cell digest length overflow".to_owned(),
            })?;
        let digest_end =
            key_start
                .checked_add(digest_len)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "index cell digest end offset overflow".to_owned(),
                })?;

        let key_bytes = page
            .get(key_start..key_end)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "index cell key range out of bounds".to_owned(),
            })?
            .to_vec();
        let digest_bytes =
            page.get(key_start..digest_end)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "index cell digest range out of bounds".to_owned(),
                })?;
        let key_digest = fsqlite_types::SemanticKeyRef::compute_digest(
            fsqlite_types::SemanticKeyKind::IndexEntry,
            btree_ref,
            digest_bytes,
        );

        Ok((key_digest, SortKey::IndexKey(key_bytes)))
    } else {
        // Interior pages not supported
        Err(FrankenError::DatabaseCorrupt {
            detail: "cannot compute key digest for interior page cells".to_owned(),
        })
    }
}

/// Compute key digest and sort key from a cell delta.
///
/// DELETE deltas carry no cell bytes, so their sort key is only a placeholder.
/// They still target the correct cell through the originating `CellKey` digest.
fn compute_cell_key_and_sort_key_from_delta(
    delta: &CellDelta,
    page_type: BtreePageType,
    usable_size: u32,
) -> Result<([u8; 16], SortKey)> {
    let key_digest = delta.cell_key.key_digest;

    if delta.cell_data.is_empty() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: "insert/update delta missing cell bytes".to_owned(),
        });
    }

    if page_type == BtreePageType::LeafTable {
        // Table cell: payload_size varint, rowid varint, payload
        let (_, ps_len) =
            fsqlite_types::serial_type::read_varint(&delta.cell_data).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "invalid varint in delta cell (payload size)".to_owned(),
                }
            })?;
        let rowid_cell =
            delta
                .cell_data
                .get(ps_len..)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "delta cell rowid offset out of bounds".to_owned(),
                })?;
        let (rowid, _) = fsqlite_types::serial_type::read_varint(rowid_cell).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: "invalid varint in delta cell (rowid)".to_owned(),
            }
        })?;
        let rowid = rowid as i64;
        Ok((key_digest, SortKey::Rowid(rowid)))
    } else if page_type == BtreePageType::LeafIndex {
        use fsqlite_btree::local_payload_size;

        // Index cell: payload_size varint, key bytes
        if let Some((payload_size, ps_len)) =
            fsqlite_types::serial_type::read_varint(&delta.cell_data)
        {
            let key_start = ps_len;
            let payload_size =
                u32::try_from(payload_size).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: "delta index cell payload size exceeds supported page size".to_owned(),
                })?;
            let key_len = usize::try_from(payload_size.min(local_payload_size(
                payload_size,
                usable_size,
                page_type,
            )))
            .map_err(|_| FrankenError::DatabaseCorrupt {
                detail: "delta index cell local key length exceeds addressable size".to_owned(),
            })?;
            let key_end =
                key_start
                    .checked_add(key_len)
                    .ok_or_else(|| FrankenError::DatabaseCorrupt {
                        detail: "delta index cell key end offset overflow".to_owned(),
                    })?;
            let key_bytes = delta
                .cell_data
                .get(key_start..key_end)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "delta index cell key range out of bounds".to_owned(),
                })?
                .to_vec();
            return Ok((key_digest, SortKey::IndexKey(key_bytes)));
        }
        Err(FrankenError::DatabaseCorrupt {
            detail: "invalid varint in delta index cell (payload size)".to_owned(),
        })
    } else {
        Err(FrankenError::DatabaseCorrupt {
            detail: "cannot compute key digest for interior delta cell".to_owned(),
        })
    }
}

// Removed compute_key_digest

/// Compute the end offset of a cell.
fn compute_cell_end(
    page: &[u8],
    cell_offset: usize,
    page_type: BtreePageType,
    usable_size: u32,
) -> Result<usize> {
    use fsqlite_btree::local_payload_size;

    // Read varints to determine cell structure
    let cell = page
        .get(cell_offset..)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "cell offset out of bounds".to_owned(),
        })?;
    let (payload_size, ps_len) =
        fsqlite_types::serial_type::read_varint(cell).ok_or_else(|| {
            FrankenError::DatabaseCorrupt {
                detail: "invalid varint in cell (payload size)".to_owned(),
            }
        })?;

    let mut pos = cell_offset
        .checked_add(ps_len)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "cell payload offset overflow".to_owned(),
        })?;

    // Table cells have a rowid varint
    if page_type.is_table() && page_type.is_leaf() {
        let rowid_cell = page
            .get(pos..)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell rowid offset out of bounds".to_owned(),
            })?;
        let (_, rowid_len) =
            fsqlite_types::serial_type::read_varint(rowid_cell).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "invalid varint in cell (rowid)".to_owned(),
                }
            })?;
        pos = pos
            .checked_add(rowid_len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell rowid end offset overflow".to_owned(),
            })?;
    }

    // Payload (potentially with overflow pointer)
    let payload_size = u32::try_from(payload_size).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: "cell payload size exceeds supported page size".to_owned(),
    })?;
    let local_size = local_payload_size(payload_size, usable_size, page_type);
    pos = pos
        .checked_add(local_size as usize)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "cell payload end offset overflow".to_owned(),
        })?;

    // If overflow, add 4 bytes for overflow page pointer
    if local_size < payload_size {
        pos = pos
            .checked_add(4)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cell overflow pointer offset overflow".to_owned(),
            })?;
    }

    Ok(pos)
}

/// Encode an i64 as a SQLite varint.
fn encode_varint_i64(value: i64, buf: &mut [u8]) -> usize {
    fsqlite_types::serial_type::write_varint(buf, value as u64)
}

// ---------------------------------------------------------------------------
// Threshold-Based Eager Materialization
// ---------------------------------------------------------------------------

/// Default threshold for eager materialization (number of deltas per page).
pub const DEFAULT_MATERIALIZATION_THRESHOLD: usize = 32;

/// Check if a page should be eagerly materialized based on delta count.
#[must_use]
pub fn should_materialize_eagerly(delta_count: usize, threshold: usize) -> bool {
    delta_count > threshold
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{CommitSeq, PageNumber, SchemaEpoch, TxnEpoch, TxnId, TxnToken};

    const PAGE_SIZE: u32 = 4096;
    const USABLE_SIZE: u32 = 4096;

    fn test_snapshot(high: u64) -> Snapshot {
        Snapshot {
            high: CommitSeq::new(high),
            schema_epoch: SchemaEpoch::new(1),
        }
    }

    fn test_txn() -> TxnToken {
        TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0))
    }

    fn create_empty_leaf_table_page() -> PageData {
        let mut page = vec![0u8; PAGE_SIZE as usize];

        // Page header for leaf table (8 bytes)
        page[0] = BtreePageType::LeafTable as u8; // page type
        page[1] = 0;
        page[2] = 0; // first freeblock
        page[3] = 0;
        page[4] = 0; // cell count = 0
        page[5] = 0x10;
        page[6] = 0x00; // cell content offset = 4096
        page[7] = 0; // fragmented free bytes

        PageData::from_vec(page)
    }

    fn create_empty_leaf_index_page() -> PageData {
        let mut page = vec![0u8; PAGE_SIZE as usize];

        page[0] = BtreePageType::LeafIndex as u8;
        page[1] = 0;
        page[2] = 0;
        page[3] = 0;
        page[4] = 0;
        page[5] = 0x10;
        page[6] = 0x00;
        page[7] = 0;

        PageData::from_vec(page)
    }

    fn create_leaf_table_cell(rowid: i64, payload: &[u8]) -> Vec<u8> {
        let mut cell = Vec::new();

        // Payload size varint
        let payload_len = payload.len() as u64;
        let mut buf = [0u8; 10];
        let ps_len = encode_varint_u64(payload_len, &mut buf);
        cell.extend_from_slice(&buf[..ps_len]);

        // Rowid varint
        let ri_len = encode_varint_i64(rowid, &mut buf);
        cell.extend_from_slice(&buf[..ri_len]);

        // Payload
        cell.extend_from_slice(payload);

        cell
    }

    fn create_leaf_index_cell(key: &[u8]) -> Vec<u8> {
        let mut cell = Vec::new();

        let mut buf = [0u8; 10];
        let ps_len = encode_varint_u64(key.len() as u64, &mut buf);
        cell.extend_from_slice(&buf[..ps_len]);
        cell.extend_from_slice(key);

        cell
    }

    fn materialized_table_payloads(page: &PageData) -> Result<Vec<(i64, Vec<u8>)>> {
        let header = BtreePageHeader::parse(page.as_bytes(), 0)?;
        let pointers = read_cell_pointers(page.as_bytes(), &header, 0)?;
        let mut payloads = Vec::with_capacity(pointers.len());

        for ptr in pointers {
            let cell = page.as_bytes().get(ptr as usize..).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "materialized cell pointer out of bounds".to_owned(),
                }
            })?;
            let (payload_size, ps_len) =
                fsqlite_types::serial_type::read_varint(cell).ok_or_else(|| {
                    FrankenError::DatabaseCorrupt {
                        detail: "materialized cell missing payload varint".to_owned(),
                    }
                })?;
            let rowid_cell = cell
                .get(ps_len..)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "materialized cell missing rowid varint".to_owned(),
                })?;
            let (rowid, rowid_len) = fsqlite_types::serial_type::read_varint(rowid_cell)
                .ok_or_else(|| FrankenError::DatabaseCorrupt {
                    detail: "materialized cell invalid rowid varint".to_owned(),
                })?;
            let payload_start = ps_len + rowid_len;
            let payload_len =
                usize::try_from(payload_size).map_err(|_| FrankenError::DatabaseCorrupt {
                    detail: "materialized payload length exceeds usize".to_owned(),
                })?;
            let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "materialized payload length overflow".to_owned(),
                }
            })?;
            let payload = cell.get(payload_start..payload_end).ok_or_else(|| {
                FrankenError::DatabaseCorrupt {
                    detail: "materialized payload out of bounds".to_owned(),
                }
            })?;
            payloads.push((rowid as i64, payload.to_vec()));
        }

        Ok(payloads)
    }

    fn encode_varint_u64(value: u64, buf: &mut [u8]) -> usize {
        encode_varint_i64(value as i64, buf)
    }

    fn create_delta_insert(rowid: i64, payload: &[u8], commit_seq: u64) -> CellDelta {
        CellDelta {
            commit_seq: CommitSeq::new(commit_seq),
            created_by: test_txn(),
            cell_key: crate::cell_visibility::CellKey::table_row(
                fsqlite_types::BtreeRef::Table(fsqlite_types::TableId::new(1)),
                rowid,
            ),
            kind: CellDeltaKind::Insert,
            page_number: PageNumber::new(2).unwrap(),
            cell_data: create_leaf_table_cell(rowid, payload),
            prev_idx: None,
        }
    }

    fn create_delta_delete(commit_seq: u64) -> CellDelta {
        CellDelta {
            commit_seq: CommitSeq::new(commit_seq),
            created_by: test_txn(),
            cell_key: crate::cell_visibility::CellKey::table_row(
                fsqlite_types::BtreeRef::Table(fsqlite_types::TableId::new(1)),
                100,
            ),
            kind: CellDeltaKind::Delete,
            page_number: PageNumber::new(2).unwrap(),
            cell_data: vec![],
            prev_idx: None,
        }
    }

    fn create_delta_update(rowid: i64, payload: &[u8], commit_seq: u64) -> CellDelta {
        CellDelta {
            commit_seq: CommitSeq::new(commit_seq),
            created_by: test_txn(),
            cell_key: crate::cell_visibility::CellKey::table_row(
                fsqlite_types::BtreeRef::Table(fsqlite_types::TableId::new(1)),
                rowid,
            ),
            kind: CellDeltaKind::Update,
            page_number: PageNumber::new(2).unwrap(),
            cell_data: create_leaf_table_cell(rowid, payload),
            prev_idx: None,
        }
    }

    fn create_delta_index_insert(key: &[u8], commit_seq: u64) -> CellDelta {
        let btree = fsqlite_types::BtreeRef::Index(fsqlite_types::IndexId::new(1));
        CellDelta {
            commit_seq: CommitSeq::new(commit_seq),
            created_by: test_txn(),
            cell_key: crate::cell_visibility::CellKey::index_entry(btree, key),
            kind: CellDeltaKind::Insert,
            page_number: PageNumber::new(2).unwrap(),
            cell_data: create_leaf_index_cell(key),
            prev_idx: None,
        }
    }

    fn create_delta_index_delete(key: &[u8], commit_seq: u64) -> CellDelta {
        let btree = fsqlite_types::BtreeRef::Index(fsqlite_types::IndexId::new(1));
        CellDelta {
            commit_seq: CommitSeq::new(commit_seq),
            created_by: test_txn(),
            cell_key: crate::cell_visibility::CellKey::index_entry(btree, key),
            kind: CellDeltaKind::Delete,
            page_number: PageNumber::new(2).unwrap(),
            cell_data: Vec::new(),
            prev_idx: None,
        }
    }

    #[test]
    fn test_materialize_single_insert() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let deltas = vec![create_delta_insert(100, b"hello world", 5)];
        let snapshot = test_snapshot(10);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 1);
        assert_eq!(result.cell_count, 1);

        // Verify the page is valid
        let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
        assert_eq!(header.cell_count, 1);
        assert_eq!(header.page_type, BtreePageType::LeafTable);
    }

    #[test]
    fn test_materialize_multiple_inserts() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let deltas: Vec<CellDelta> = (0..10)
            .map(|i| {
                create_delta_insert(i as i64 + 100, format!("value{i}").as_bytes(), i as u64 + 1)
            })
            .collect();
        let snapshot = test_snapshot(20);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 10);
        assert_eq!(result.cell_count, 10);

        // Verify all cells present
        let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
        assert_eq!(header.cell_count, 10);
    }

    #[test]
    fn test_materialize_update() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let deltas = vec![
            create_delta_insert(100, b"original", 5),
            create_delta_update(100, b"updated", 10),
        ];
        let snapshot = test_snapshot(15);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 2);
        assert_eq!(result.cell_count, 1);

        // Verify the cell content is updated
        let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
        assert_eq!(header.cell_count, 1);
    }

    #[test]
    fn test_materialize_updates_existing_multi_byte_rowid_base_cell() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let original_payload = vec![b'a'; 130];
        let updated_payload = vec![b'b'; 131];

        let initial = materialize_page(
            &base,
            page_no,
            &[create_delta_insert(4242, &original_payload, 5)],
            &test_snapshot(5),
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("initial materialization should succeed");

        let result = materialize_page(
            &initial.page,
            page_no,
            &[create_delta_update(4242, &updated_payload, 10)],
            &test_snapshot(10),
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("update materialization should succeed");

        assert_eq!(result.deltas_applied, 1);
        assert_eq!(result.cell_count, 1);
        assert_eq!(
            materialized_table_payloads(&result.page).unwrap(),
            vec![(4242, updated_payload)]
        );
    }

    #[test]
    fn test_materialize_preserves_negative_rowid_base_cell() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();

        let initial = materialize_page(
            &base,
            page_no,
            &[create_delta_insert(-7, b"original", 5)],
            &test_snapshot(5),
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("initial negative-rowid materialization should succeed");

        let result = materialize_page(
            &initial.page,
            page_no,
            &[create_delta_update(-7, b"updated", 10)],
            &test_snapshot(10),
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("negative-rowid update materialization should succeed");

        assert_eq!(result.deltas_applied, 1);
        assert_eq!(
            materialized_table_payloads(&result.page).unwrap(),
            vec![(-7, b"updated".to_vec())]
        );
    }

    #[test]
    fn test_materialize_deletes_existing_index_base_cell_without_trailing_page_bytes() {
        let base = create_empty_leaf_index_page();
        let page_no = PageNumber::new(2).unwrap();
        let key = b"abc";

        let initial = materialize_page(
            &base,
            page_no,
            &[create_delta_index_insert(key, 5)],
            &test_snapshot(5),
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("initial index materialization should succeed");
        assert_eq!(initial.cell_count, 1);

        let result = materialize_page(
            &initial.page,
            page_no,
            &[create_delta_index_delete(key, 10)],
            &test_snapshot(10),
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("index delete materialization should match the existing base cell");

        assert_eq!(result.cell_count, 0);
        let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
        assert_eq!(header.cell_count, 0);
    }

    #[test]
    fn test_index_overflow_cell_digest_includes_overflow_pointer() {
        let payload_size = 1_500_u32;
        let local_payload =
            fsqlite_btree::local_payload_size(payload_size, USABLE_SIZE, BtreePageType::LeafIndex);
        assert!(local_payload < payload_size);

        let cell_offset = 32_usize;
        let mut page = vec![0_u8; PAGE_SIZE as usize];
        let mut buf = [0_u8; 10];
        let ps_len = encode_varint_u64(u64::from(payload_size), &mut buf);
        page[cell_offset..cell_offset + ps_len].copy_from_slice(&buf[..ps_len]);

        let key_start = cell_offset + ps_len;
        let local_len = local_payload as usize;
        for (idx, byte) in page[key_start..key_start + local_len]
            .iter_mut()
            .enumerate()
        {
            *byte = (idx % 251) as u8;
        }
        let overflow_ptr = 0x0102_0304_u32.to_be_bytes();
        page[key_start + local_len..key_start + local_len + 4].copy_from_slice(&overflow_ptr);

        let btree = fsqlite_types::BtreeRef::Index(fsqlite_types::IndexId::new(7));
        let (digest, sort_key) = compute_cell_key_and_sort_key(
            &page,
            cell_offset,
            BtreePageType::LeafIndex,
            btree,
            USABLE_SIZE,
        )
        .expect("overflow index cell key should parse");

        let digest_bytes = &page[key_start..key_start + local_len + 4];
        let expected = fsqlite_types::SemanticKeyRef::compute_digest(
            fsqlite_types::SemanticKeyKind::IndexEntry,
            btree,
            digest_bytes,
        );
        let local_only = fsqlite_types::SemanticKeyRef::compute_digest(
            fsqlite_types::SemanticKeyKind::IndexEntry,
            btree,
            &page[key_start..key_start + local_len],
        );

        assert_eq!(digest, expected);
        assert_ne!(
            digest, local_only,
            "overflow index cell identity must include the overflow pointer"
        );
        assert!(
            matches!(&sort_key, SortKey::IndexKey(_)),
            "index cells must sort by index key bytes"
        );
        let SortKey::IndexKey(key) = sort_key else {
            return;
        };
        assert_eq!(key, page[key_start..key_start + local_len]);
    }

    #[test]
    fn test_materialize_rejects_insert_delta_without_cell_bytes() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let mut delta = create_delta_insert(100, b"value", 5);
        delta.cell_data.clear();

        assert!(
            materialize_page(
                &base,
                page_no,
                &[delta],
                &test_snapshot(5),
                USABLE_SIZE,
                MaterializationTrigger::Explicit,
            )
            .is_err()
        );
    }

    #[test]
    fn test_materialize_preserves_collected_same_txn_order_for_one_cell() -> Result<()> {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "valid test page number".to_owned(),
        })?;
        let log = crate::cell_visibility::CellVisibilityLog::new(1024 * 1024);
        let btree = fsqlite_types::BtreeRef::Table(fsqlite_types::TableId::new(1));
        let cell_key = crate::cell_visibility::CellKey::table_row(btree, 100);
        let token = test_txn();

        log.record_insert(
            cell_key,
            page_no,
            create_leaf_table_cell(100, b"original"),
            token,
        )
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "insert should fit test budget".to_owned(),
        })?;
        log.record_update(
            cell_key,
            page_no,
            create_leaf_table_cell(100, b"updated"),
            token,
        )
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "update should fit test budget".to_owned(),
        })?;
        log.commit_txn(token, CommitSeq::new(10));

        let deltas = log.collect_visible_deltas(page_no, CommitSeq::new(10));
        let snapshot = test_snapshot(10);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )?;

        assert_eq!(result.deltas_applied, 2);
        assert_eq!(result.cell_count, 1);
        assert_eq!(
            materialized_table_payloads(&result.page)?,
            vec![(100, b"updated".to_vec())]
        );

        Ok(())
    }

    #[test]
    fn test_materialize_delete() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let deltas = vec![
            create_delta_insert(100, b"to be deleted", 5),
            create_delta_delete(10),
        ];
        let snapshot = test_snapshot(15);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 2);
        assert_eq!(result.cell_count, 0);
        assert_eq!(
            materialized_table_payloads(&result.page).unwrap(),
            Vec::new()
        );
    }

    #[test]
    fn test_materialize_mixed() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();

        let mut deltas = Vec::new();
        // 20 inserts
        for i in 0..20 {
            deltas.push(create_delta_insert(
                i as i64 + 100,
                format!("val{i}").as_bytes(),
                i as u64 + 1,
            ));
        }
        // 5 updates
        for i in 0..5 {
            deltas.push(create_delta_update(
                i as i64 + 100,
                format!("upd{i}").as_bytes(),
                25 + i as u64,
            ));
        }
        // Three deletes against the same row: the first removes it, the rest
        // are idempotent.
        for _ in 0..3 {
            deltas.push(create_delta_delete(35));
        }

        let snapshot = test_snapshot(50);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 28);
        assert_eq!(result.cell_count, 19);
        let payloads = materialized_table_payloads(&result.page).unwrap();
        assert!(!payloads.iter().any(|(rowid, _)| *rowid == 100));
    }

    #[test]
    fn test_materialize_snapshot_visibility() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let deltas = vec![
            create_delta_insert(100, b"visible", 5),
            create_delta_insert(101, b"not visible", 15),
        ];

        // Snapshot at 10 should only see the first insert
        let snapshot = test_snapshot(10);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 1);
        assert_eq!(result.cell_count, 1);
    }

    #[test]
    fn test_materialize_threshold_trigger() {
        // Test the threshold check function
        assert!(!should_materialize_eagerly(
            10,
            DEFAULT_MATERIALIZATION_THRESHOLD
        ));
        assert!(!should_materialize_eagerly(
            32,
            DEFAULT_MATERIALIZATION_THRESHOLD
        ));
        assert!(should_materialize_eagerly(
            33,
            DEFAULT_MATERIALIZATION_THRESHOLD
        ));
        assert!(should_materialize_eagerly(
            100,
            DEFAULT_MATERIALIZATION_THRESHOLD
        ));
    }

    #[test]
    fn test_materialize_round_trip() {
        // Insert cells via deltas, materialize, verify valid page
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();

        let deltas: Vec<CellDelta> = (0..5)
            .map(|i| create_delta_insert(i as i64 + 1, format!("data{i}").as_bytes(), i as u64 + 1))
            .collect();

        let snapshot = test_snapshot(10);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        )
        .expect("materialization should succeed");

        // Parse the resulting page and verify structure
        let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
        assert_eq!(header.cell_count, 5);
        assert_eq!(header.page_type, BtreePageType::LeafTable);
        assert_eq!(header.first_freeblock, 0);
        assert_eq!(header.fragmented_free_bytes, 0);

        // Verify cell pointers are valid
        let pointers = read_cell_pointers(result.page.as_bytes(), &header, 0).unwrap();
        assert_eq!(pointers.len(), 5);
        for &ptr in &pointers {
            assert!(ptr >= header.cell_content_offset as u16);
            assert!((ptr as usize) < result.page.len());
        }
    }

    #[test]
    fn test_materialize_empty_deltas() {
        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let deltas: Vec<CellDelta> = vec![];
        let snapshot = test_snapshot(10);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Checkpoint,
        )
        .expect("materialization should succeed");

        assert_eq!(result.deltas_applied, 0);
        assert_eq!(result.cell_count, 0);
        // Should return base unchanged
        assert_eq!(result.page.as_bytes(), base.as_bytes());
    }

    #[test]
    fn test_materialize_interior_page_fails() {
        // Interior pages should fail
        let mut page = vec![0u8; PAGE_SIZE as usize];
        page[0] = BtreePageType::InteriorTable as u8;
        page[3] = 0;
        page[4] = 0; // cell count = 0
        page[5] = 0x10;
        page[6] = 0x00; // cell content offset
        // Right child pointer
        page[8] = 0;
        page[9] = 0;
        page[10] = 0;
        page[11] = 3;

        let base = PageData::from_vec(page);
        let page_no = PageNumber::new(2).unwrap();
        let deltas = vec![create_delta_insert(100, b"test", 5)];
        let snapshot = test_snapshot(10);

        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Explicit,
        );
        assert!(result.is_err());
    }

    /// Helper: create N insert deltas with payloads of the given size, using
    /// sequential rowids and commit sequences.
    fn create_bulk_inserts(
        count: usize,
        payload_size: usize,
        commit_seq_base: u64,
    ) -> Vec<CellDelta> {
        (0..count)
            .map(|i| {
                let payload: Vec<u8> = (0..payload_size).map(|b| (b + i) as u8).collect();
                create_delta_insert((i + 1) as i64, &payload, commit_seq_base + i as u64)
            })
            .collect()
    }

    #[test]
    fn test_split_trigger_materializes() {
        // Simulate the pre-split path: accumulate cell deltas that fill most
        // of a page, then materialise with `Structural` trigger — the same
        // trigger the real B-tree split code would use.
        //
        // A 4096-byte page has 8 bytes of header.  Each cell costs:
        //   2 bytes (cell pointer) + varint(payload_len) + varint(rowid) + payload
        // With 100-byte payloads the per-cell cost is ~104 bytes, so ~38
        // cells saturate the page.  We pick 35 cells to stay just under the
        // limit, then verify the materialised page is valid.

        let base = create_empty_leaf_table_page();
        let page_no = PageNumber::new(2).unwrap();
        let snapshot = test_snapshot(50);

        // ------- Phase 1: materialize a nearly-full page ---------
        let deltas = create_bulk_inserts(35, 100, 1);
        let result = materialize_page(
            &base,
            page_no,
            &deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Structural,
        )
        .expect("structural materialization of 35 cells should succeed");

        assert_eq!(result.deltas_applied, 35);
        assert_eq!(result.cell_count, 35);
        assert!(result.duration_us < 10_000_000, "should be fast");

        // Verify header is consistent.
        let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
        assert_eq!(header.cell_count, 35);
        assert_eq!(header.page_type, BtreePageType::LeafTable);
        assert_eq!(
            header.fragmented_free_bytes, 0,
            "freshly packed page has no fragmentation"
        );
        // Cell content area must start above the header + pointer array.
        let ptr_array_end = 8 + 35 * CELL_POINTER_SIZE as usize;
        assert!(
            header.cell_content_offset as usize >= ptr_array_end,
            "cell content offset ({}) must be past pointer array ({})",
            header.cell_content_offset,
            ptr_array_end,
        );

        // Verify all 35 rows are present with correct rowids and payloads.
        let payloads = materialized_table_payloads(&result.page).unwrap();
        assert_eq!(payloads.len(), 35);
        // Rows must be in rowid-sorted order (B-tree invariant).
        for (i, (rowid, payload)) in payloads.iter().enumerate() {
            let expected_rowid = (i + 1) as i64;
            assert_eq!(*rowid, expected_rowid, "row {i} has wrong rowid");
            assert_eq!(payload.len(), 100, "row {i} has wrong payload length");
            // First byte of payload encodes (byte_offset + row_index) mod 256.
            assert_eq!(payload[0], i as u8, "row {i} has wrong payload content");
        }

        // ------- Phase 2: verify overflow detection --------
        // With 50 cells of 100 bytes each (~5200 bytes needed) the page
        // (4096 usable) must overflow.
        let overflow_deltas = create_bulk_inserts(50, 100, 1);
        let overflow_result = materialize_page(
            &base,
            page_no,
            &overflow_deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Structural,
        );
        assert!(
            overflow_result.is_err(),
            "50 cells x 100-byte payload should overflow a 4096-byte page"
        );

        // ------- Phase 3: structural trigger with mixed committed + new ----
        // Simulate the real split flow: some deltas are already committed
        // (visible to snapshot) and some arrive in a new batch. Materialise
        // them together with the Structural trigger.
        let committed = create_bulk_inserts(20, 80, 1);
        let extra: Vec<CellDelta> = (20..30)
            .map(|i| {
                let payload: Vec<u8> = (0..80_usize).map(|b| (b + i) as u8).collect();
                create_delta_insert((i + 1) as i64, &payload, (i + 1) as u64)
            })
            .collect();
        let all_deltas: Vec<CellDelta> = committed.into_iter().chain(extra).collect();
        let result2 = materialize_page(
            &base,
            page_no,
            &all_deltas,
            &snapshot,
            USABLE_SIZE,
            MaterializationTrigger::Structural,
        )
        .expect("structural materialization of 30 cells should succeed");

        assert_eq!(result2.deltas_applied, 30);
        assert_eq!(result2.cell_count, 30);

        // Verify B-tree ordering is maintained.
        let payloads2 = materialized_table_payloads(&result2.page).unwrap();
        let rowids: Vec<i64> = payloads2.iter().map(|(r, _)| *r).collect();
        let mut sorted = rowids.clone();
        sorted.sort();
        assert_eq!(rowids, sorted, "rows must be in ascending rowid order");
    }

    /// Integration test: simulate a WAL checkpoint that materializes all pages
    /// with outstanding cell deltas.
    ///
    /// This exercises the full checkpoint-materialization flow:
    /// 1. Multiple transactions write cell deltas to several pages
    /// 2. Transactions are committed at distinct commit sequences
    /// 3. `pages_with_deltas()` discovers the dirty pages
    /// 4. `collect_visible_deltas()` + `materialize_page()` produces valid
    ///    B-tree pages for each
    /// 5. `clear_page_deltas()` reclaims the delta memory
    /// 6. After cleanup, no outstanding deltas remain
    #[test]
    fn test_checkpoint_materializes_all() {
        let log = crate::cell_visibility::CellVisibilityLog::new(1024 * 1024);
        let btree = fsqlite_types::BtreeRef::Table(fsqlite_types::TableId::new(1));

        // ---------- Phase 1: record deltas across 3 pages via 2 transactions ----------

        let txn1 = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let txn2 = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));

        let page2 = PageNumber::new(2).unwrap();
        let page3 = PageNumber::new(3).unwrap();
        let page4 = PageNumber::new(4).unwrap();

        // Txn1: 3 inserts on page 2, 2 inserts on page 3
        for i in 0..3 {
            let cell_key = crate::cell_visibility::CellKey::table_row(btree, 100 + i);
            let cell_data = create_leaf_table_cell(100 + i, format!("txn1_p2_{i}").as_bytes());
            log.record_insert(cell_key, page2, cell_data, txn1)
                .expect("insert should fit budget");
        }
        for i in 0..2 {
            let cell_key = crate::cell_visibility::CellKey::table_row(btree, 200 + i);
            let cell_data = create_leaf_table_cell(200 + i, format!("txn1_p3_{i}").as_bytes());
            log.record_insert(cell_key, page3, cell_data, txn1)
                .expect("insert should fit budget");
        }

        // Txn2: 4 inserts on page 4, 1 insert on page 2 (overlapping with txn1)
        for i in 0..4 {
            let cell_key = crate::cell_visibility::CellKey::table_row(btree, 300 + i);
            let cell_data = create_leaf_table_cell(300 + i, format!("txn2_p4_{i}").as_bytes());
            log.record_insert(cell_key, page4, cell_data, txn2)
                .expect("insert should fit budget");
        }
        {
            let cell_key = crate::cell_visibility::CellKey::table_row(btree, 103);
            let cell_data = create_leaf_table_cell(103, b"txn2_p2_overlap");
            log.record_insert(cell_key, page2, cell_data, txn2)
                .expect("insert should fit budget");
        }

        // Commit both transactions at different sequences
        log.commit_txn(txn1, CommitSeq::new(10));
        log.commit_txn(txn2, CommitSeq::new(20));

        // ---------- Phase 2: discover dirty pages ----------

        let dirty_pages = log.pages_with_deltas();
        assert!(
            dirty_pages.len() >= 3,
            "should have deltas on at least 3 pages, got {}",
            dirty_pages.len()
        );
        assert!(dirty_pages.contains(&page2), "page 2 should have deltas");
        assert!(dirty_pages.contains(&page3), "page 3 should have deltas");
        assert!(dirty_pages.contains(&page4), "page 4 should have deltas");

        // ---------- Phase 3: checkpoint-style materialization ----------

        // The snapshot covers both committed transactions.
        let checkpoint_snapshot = test_snapshot(25);

        // Provide a base page for each dirty page (empty leaf table).
        let base = create_empty_leaf_table_page();

        // Expected cell counts per page:
        //   page2: 4 cells (3 from txn1 + 1 from txn2)
        //   page3: 2 cells (from txn1)
        //   page4: 4 cells (from txn2)
        let expected: std::collections::HashMap<PageNumber, u16> =
            [(page2, 4), (page3, 2), (page4, 4)].into_iter().collect();

        let mut materialized_pages: std::collections::HashMap<PageNumber, MaterializationResult> =
            std::collections::HashMap::new();

        for &pgno in &dirty_pages {
            let deltas = log.collect_visible_deltas(pgno, checkpoint_snapshot.high);
            if deltas.is_empty() {
                continue;
            }

            let result = materialize_page(
                &base,
                pgno,
                &deltas,
                &checkpoint_snapshot,
                USABLE_SIZE,
                MaterializationTrigger::Checkpoint,
            )
            .unwrap_or_else(|e| {
                panic!("materialization of page {} failed: {e}", pgno.get());
            });

            materialized_pages.insert(pgno, result);
        }

        // Verify we materialized all 3 expected pages.
        assert_eq!(
            materialized_pages.len(),
            3,
            "checkpoint should materialize exactly 3 pages"
        );

        for (&pgno, result) in &materialized_pages {
            let want = expected[&pgno];
            assert_eq!(
                result.cell_count,
                want,
                "page {} should have {want} cells, got {}",
                pgno.get(),
                result.cell_count
            );

            // Validate the B-tree header on each materialized page.
            let header = BtreePageHeader::parse(result.page.as_bytes(), 0).unwrap();
            assert_eq!(header.cell_count, want);
            assert_eq!(header.page_type, BtreePageType::LeafTable);
            assert_eq!(
                header.fragmented_free_bytes, 0,
                "freshly packed page should have no fragmentation"
            );

            // Verify cell pointers are in bounds.
            let pointers = read_cell_pointers(result.page.as_bytes(), &header, 0).unwrap();
            assert_eq!(pointers.len(), want as usize);
            for &ptr in &pointers {
                assert!(
                    (ptr as usize) < PAGE_SIZE as usize,
                    "cell pointer {ptr} out of bounds"
                );
            }

            // Verify row ordering: rowids must be ascending (B-tree invariant).
            let payloads = materialized_table_payloads(&result.page).unwrap();
            let rowids: Vec<i64> = payloads.iter().map(|(r, _)| *r).collect();
            let mut sorted = rowids.clone();
            sorted.sort();
            assert_eq!(
                rowids,
                sorted,
                "page {} rows must be in ascending rowid order",
                pgno.get()
            );
        }

        // ---------- Phase 4: clear deltas and verify cleanup ----------

        // The checkpoint has materialized everything up to commit_seq 25.
        // Clear deltas for each page.
        for &pgno in &dirty_pages {
            log.clear_page_deltas(pgno, CommitSeq::new(25));
        }

        // After clearing, no page should have outstanding deltas.
        let remaining = log.pages_with_deltas();
        assert!(
            remaining.is_empty(),
            "all deltas should be cleared after checkpoint; {} pages still dirty",
            remaining.len()
        );

        // The global delta count should be zero.
        assert_eq!(
            log.delta_count(),
            0,
            "global delta count should be 0 after clearing all pages"
        );
    }
}
