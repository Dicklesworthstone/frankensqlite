//! §5.10.3-5.10.5 Physical Merge: Structured Page Patches & Safety Ladder.
//!
//! Implements the parse→merge→repack pipeline for B-tree pages and the
//! commit-time merge policy ladder (§5.10.4).
//!
//! **Key invariant (normative):** Physical merge MUST NOT be implemented as
//! "apply two byte patches to the same base page."  Instead, it operates on
//! parsed semantic objects keyed by stable cell digests, not raw byte offsets.

use std::collections::{BTreeMap, HashMap};

use tracing::{debug, info};

use fsqlite_types::{
    BTreePageHeader, BTreePageType, BtreeRef, CommitSeq, IntentOp, MergePageKind, PageNumber,
    PageSize, SchemaEpoch, SemanticKeyKind, SemanticKeyRef, Snapshot, TableId,
};

use crate::deterministic_rebase::{
    BaseRowReader, RebaseError, RebaseSchemaLookup, check_rebase_eligibility, deterministic_rebase,
};
use crate::lifecycle::WriteMergePolicy;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors produced by the physical merge pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeError {
    /// Raw XOR merge attempted on a `SQLite` structured page.
    RawXorForbiddenForStructuredPage,
    /// Schema epoch mismatch — merge across DDL/VACUUM boundary forbidden.
    SchemaEpochMismatch { expected: u64, actual: u64 },
    /// Page buffer too small or invalid for the claimed page size.
    InvalidPageBuffer,
    /// Page type is not a recognized B-tree type.
    UnrecognizedPageType { raw: u8 },
    /// Cell overlap detected — no safe merge exists.
    CellOverlap { cell_key_digest: [u8; 16] },
    /// Header mutations from both patches — non-commutative conflict.
    HeaderConflict,
    /// Free-space ops from both patches — conservative reject.
    FreeSpaceConflict,
    /// Deterministic rebase failed (wraps inner error).
    RebaseFailed(RebaseError),
    /// Cell content area would overflow the page after merge.
    PageOverflow {
        required_bytes: usize,
        available_bytes: usize,
    },
    /// Page parse error from fsqlite-types.
    PageParseError(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RawXorForbiddenForStructuredPage => {
                f.write_str("raw XOR merge forbidden for `SQLite` structured pages")
            }
            Self::SchemaEpochMismatch { expected, actual } => {
                write!(
                    f,
                    "schema epoch mismatch: expected {expected}, got {actual}"
                )
            }
            Self::InvalidPageBuffer => f.write_str("invalid page buffer"),
            Self::UnrecognizedPageType { raw } => {
                write!(f, "unrecognized page type byte: 0x{raw:02x}")
            }
            Self::CellOverlap { cell_key_digest } => {
                write!(
                    f,
                    "cell overlap on digest {:02x}{:02x}{:02x}{:02x}...",
                    cell_key_digest[0], cell_key_digest[1], cell_key_digest[2], cell_key_digest[3]
                )
            }
            Self::HeaderConflict => {
                f.write_str("non-commutative header mutations from both patches")
            }
            Self::FreeSpaceConflict => {
                f.write_str("free-space ops from both patches — conservative reject")
            }
            Self::RebaseFailed(inner) => write!(f, "deterministic rebase failed: {inner}"),
            Self::PageOverflow {
                required_bytes,
                available_bytes,
            } => {
                write!(
                    f,
                    "page overflow: need {required_bytes} bytes, only {available_bytes} available"
                )
            }
            Self::PageParseError(msg) => write!(f, "page parse error: {msg}"),
        }
    }
}

impl std::error::Error for MergeError {}

impl From<RebaseError> for MergeError {
    fn from(e: RebaseError) -> Self {
        Self::RebaseFailed(e)
    }
}

// ---------------------------------------------------------------------------
// StructuredPagePatch types (§5.10.3 normative)
// ---------------------------------------------------------------------------

/// A single header mutation for a B-tree page.
///
/// Header ops are non-commutative: if both patches include header mutations
/// that cannot be serialized without ambiguity, the merge MUST reject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderOp {
    /// Set cell count to a new value.
    SetCellCount(u16),
    /// Set the right-most child pointer (interior pages only).
    SetRightMostChild(PageNumber),
}

/// Operation kind for a single cell within a B-tree page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellOpKind {
    /// Insert a new cell with the given raw bytes.
    Insert { cell_bytes: Vec<u8> },
    /// Delete the cell.
    Delete,
    /// Replace the cell with new raw bytes.
    Replace { new_cell_bytes: Vec<u8> },
}

/// A semantic cell operation keyed by a stable digest, not a physical offset.
///
/// `cell_key_digest` MUST be derived from the same domain-separated semantic
/// key digest as [`SemanticKeyRef::key_digest`] (§5.10.7 normative).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellOp {
    /// Stable BLAKE3-based digest of the semantic key (rowid or index key).
    pub cell_key_digest: [u8; 16],
    /// The operation to apply to this cell.
    pub kind: CellOpKind,
}

/// Free-space layout operation (derived during repack; SHOULD be empty for
/// SAFE B-tree leaf merges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreeSpaceOp {
    /// Defragment the page (reclaim fragmented bytes).
    Defragment,
}

/// Byte-range XOR patch for opaque pages only (§5.10.3 normative).
///
/// FORBIDDEN for `SQLite` structured pages under all SAFE builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeXorPatch {
    /// Byte offset within the page.
    pub offset: u32,
    /// XOR delta bytes.
    pub data: Vec<u8>,
}

/// Structured page patch (§5.10.3 normative representation).
///
/// Merge operations are keyed by stable identifiers (cell digests), not
/// physical byte offsets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StructuredPagePatch {
    /// Header mutations (derived during repack; SHOULD be empty for SAFE B-tree leaf).
    pub header_ops: Vec<HeaderOp>,
    /// Cell-level operations, mergeable when disjoint by `cell_key_digest`.
    pub cell_ops: Vec<CellOp>,
    /// Free-space layout ops (derived during repack; SHOULD be empty for SAFE B-tree leaf).
    pub free_ops: Vec<FreeSpaceOp>,
    /// Raw XOR ranges — FORBIDDEN for `SQLite` structured pages.
    pub raw_xor_ranges: Vec<RangeXorPatch>,
}

impl StructuredPagePatch {
    /// Whether this patch is empty (no operations).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.header_ops.is_empty()
            && self.cell_ops.is_empty()
            && self.free_ops.is_empty()
            && self.raw_xor_ranges.is_empty()
    }

    /// Validate that raw XOR ranges are empty for structured pages.
    ///
    /// # Errors
    ///
    /// Returns `MergeError::RawXorForbiddenForStructuredPage` if this patch
    /// contains raw XOR ranges and the page kind is `SQLite`-structured.
    pub fn validate_no_raw_xor_for_structured(
        &self,
        page_kind: MergePageKind,
    ) -> Result<(), MergeError> {
        if page_kind.is_sqlite_structured() && !self.raw_xor_ranges.is_empty() {
            return Err(MergeError::RawXorForbiddenForStructuredPage);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parsed page representation
// ---------------------------------------------------------------------------

/// A cell extracted from a B-tree page, with its semantic key digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCell {
    /// The semantic key digest (same domain as `SemanticKeyRef.key_digest`).
    pub cell_key_digest: [u8; 16],
    /// The raw cell bytes (on-page representation, excluding cell pointer).
    pub cell_bytes: Vec<u8>,
    /// For table B-trees: the rowid.  For index B-trees: `None`.
    pub rowid: Option<i64>,
}

/// A B-tree page parsed into its semantic components.
#[derive(Debug, Clone)]
pub struct ParsedPage {
    /// The page type.
    pub page_type: BTreePageType,
    /// Header offset (0 normally, 100 for page 1).
    pub header_offset: usize,
    /// The parsed header.
    pub header: BTreePageHeader,
    /// Cells in cell-pointer order, keyed by digest.
    pub cells: Vec<ParsedCell>,
    /// Page size.
    pub page_size: PageSize,
    /// Reserved bytes per page.
    pub reserved_per_page: u8,
    /// Table ID used for digest computation (caller-supplied context).
    pub table_id: TableId,
}

/// Extracted cell data: (`raw_bytes`, `optional_rowid`, `key_digest`).
type CellExtract = (Vec<u8>, Option<i64>, [u8; 16]);

// ---------------------------------------------------------------------------
// Parse phase
// ---------------------------------------------------------------------------

/// Parse a B-tree page into its semantic cell array.
///
/// Each cell gets a `cell_key_digest` derived from the same domain-separated
/// BLAKE3 computation as `SemanticKeyRef::compute_digest`, ensuring alignment
/// with the intent log's key references.
///
/// # Errors
///
/// Returns [`MergeError::PageParseError`] or [`MergeError::InvalidPageBuffer`]
/// if the page buffer is invalid or truncated.
pub fn parse_btree_page(
    page: &[u8],
    page_size: PageSize,
    reserved_per_page: u8,
    is_page1: bool,
    table_id: TableId,
) -> Result<ParsedPage, MergeError> {
    let header = BTreePageHeader::parse(page, page_size, reserved_per_page, is_page1)
        .map_err(|e| MergeError::PageParseError(format!("{e}")))?;

    let header_size = header.header_size();
    let header_offset = header.header_offset;
    let ptr_array_start = header_offset + header_size;
    let cell_count = usize::from(header.cell_count);

    let mut cells = Vec::with_capacity(cell_count);
    let usable = page_size.usable(reserved_per_page) as usize;

    for i in 0..cell_count {
        let ptr_offset = ptr_array_start + i * 2;
        if ptr_offset + 2 > page.len() {
            return Err(MergeError::InvalidPageBuffer);
        }
        let cell_offset = usize::from(u16::from_be_bytes([page[ptr_offset], page[ptr_offset + 1]]));

        if cell_offset >= usable || cell_offset < ptr_array_start + cell_count * 2 {
            return Err(MergeError::PageParseError(format!(
                "cell pointer {i} out of bounds: {cell_offset}"
            )));
        }

        let (cell_bytes, rowid, cell_key_digest) =
            extract_cell_with_digest(page, cell_offset, usable, header.page_type, table_id)?;

        cells.push(ParsedCell {
            cell_key_digest,
            cell_bytes,
            rowid,
        });
    }

    Ok(ParsedPage {
        page_type: header.page_type,
        header_offset,
        header,
        cells,
        page_size,
        reserved_per_page,
        table_id,
    })
}

/// Extract a single cell's raw bytes, rowid (if table B-tree), and key digest.
#[allow(clippy::cast_possible_truncation)]
fn extract_cell_with_digest(
    page: &[u8],
    cell_offset: usize,
    usable: usize,
    page_type: BTreePageType,
    table_id: TableId,
) -> Result<CellExtract, MergeError> {
    let remaining = &page[cell_offset..usable.min(page.len())];

    match page_type {
        BTreePageType::LeafTable => parse_leaf_table_cell(remaining, table_id, usable as u32),
        BTreePageType::LeafIndex => parse_leaf_index_cell(remaining, table_id, usable as u32),
        BTreePageType::InteriorTable => parse_interior_table_cell(remaining, table_id),
        BTreePageType::InteriorIndex => {
            parse_interior_index_cell(remaining, table_id, usable as u32)
        }
    }
}

/// Parse a leaf table cell: `[varint payload_size][varint rowid][payload...][overflow?]`
fn parse_leaf_table_cell(
    data: &[u8],
    table_id: TableId,
    usable: u32,
) -> Result<CellExtract, MergeError> {
    let (payload_size, n1) =
        fsqlite_types::serial_type::read_varint(data).ok_or(MergeError::InvalidPageBuffer)?;
    let (rowid_u64, n2) = fsqlite_types::serial_type::read_varint(&data[n1..])
        .ok_or(MergeError::InvalidPageBuffer)?;

    #[allow(clippy::cast_possible_wrap)]
    let rowid = rowid_u64 as i64;

    let header_len = n1 + n2;
    let local_payload = compute_local_payload_size(payload_size, usable, true);
    let has_overflow = payload_size > u64::from(local_payload);

    let total_cell_size = header_len + local_payload as usize + if has_overflow { 4 } else { 0 };
    let cell_end = total_cell_size.min(data.len());

    let cell_bytes = data[..cell_end].to_vec();

    // Compute digest using rowid as canonical key bytes (LE i64)
    let digest = SemanticKeyRef::compute_digest(
        SemanticKeyKind::TableRow,
        BtreeRef::Table(table_id),
        &rowid.to_le_bytes(),
    );

    Ok((cell_bytes, Some(rowid), digest))
}

/// Parse a leaf index cell: `[varint payload_size][payload...][overflow?]`
fn parse_leaf_index_cell(
    data: &[u8],
    table_id: TableId,
    usable: u32,
) -> Result<CellExtract, MergeError> {
    let (payload_size, n1) =
        fsqlite_types::serial_type::read_varint(data).ok_or(MergeError::InvalidPageBuffer)?;

    let local_payload = compute_local_payload_size(payload_size, usable, false);
    let has_overflow = payload_size > u64::from(local_payload);
    let total_cell_size = n1 + local_payload as usize + if has_overflow { 4 } else { 0 };
    let cell_end = total_cell_size.min(data.len());

    let cell_bytes = data[..cell_end].to_vec();

    // For index pages, use the payload as canonical key bytes
    let payload_start = n1;
    let payload_end = (n1 + local_payload as usize).min(data.len());
    let key_bytes = &data[payload_start..payload_end];

    let digest = SemanticKeyRef::compute_digest(
        SemanticKeyKind::IndexEntry,
        BtreeRef::Table(table_id),
        key_bytes,
    );

    Ok((cell_bytes, None, digest))
}

/// Parse an interior table cell: `[4-byte left_child][varint rowid]`
fn parse_interior_table_cell(data: &[u8], table_id: TableId) -> Result<CellExtract, MergeError> {
    if data.len() < 4 {
        return Err(MergeError::InvalidPageBuffer);
    }
    let (rowid_u64, n) =
        fsqlite_types::serial_type::read_varint(&data[4..]).ok_or(MergeError::InvalidPageBuffer)?;

    #[allow(clippy::cast_possible_wrap)]
    let rowid = rowid_u64 as i64;

    let cell_end = (4 + n).min(data.len());
    let cell_bytes = data[..cell_end].to_vec();

    let digest = SemanticKeyRef::compute_digest(
        SemanticKeyKind::TableRow,
        BtreeRef::Table(table_id),
        &rowid.to_le_bytes(),
    );

    Ok((cell_bytes, Some(rowid), digest))
}

/// Parse an interior index cell: `[4-byte left_child][varint payload_size][payload...][overflow?]`
fn parse_interior_index_cell(
    data: &[u8],
    table_id: TableId,
    usable: u32,
) -> Result<CellExtract, MergeError> {
    if data.len() < 4 {
        return Err(MergeError::InvalidPageBuffer);
    }
    let (payload_size, n1) =
        fsqlite_types::serial_type::read_varint(&data[4..]).ok_or(MergeError::InvalidPageBuffer)?;

    let local_payload = compute_local_payload_size(payload_size, usable, false);
    let has_overflow = payload_size > u64::from(local_payload);
    let total = 4 + n1 + local_payload as usize + if has_overflow { 4 } else { 0 };
    let cell_end = total.min(data.len());

    let cell_bytes = data[..cell_end].to_vec();

    let payload_start = 4 + n1;
    let payload_end = (payload_start + local_payload as usize).min(data.len());
    let key_bytes = &data[payload_start..payload_end];

    let digest = SemanticKeyRef::compute_digest(
        SemanticKeyKind::IndexEntry,
        BtreeRef::Table(table_id),
        key_bytes,
    );

    Ok((cell_bytes, None, digest))
}

/// Compute local payload size (simplified version for merge pipeline).
///
/// This follows the `SQLite` formula: if total payload fits in `max_local`,
/// all bytes are local; otherwise `min_local` bytes plus remainder go to overflow.
#[must_use]
fn compute_local_payload_size(payload_size: u64, usable: u32, is_table_leaf: bool) -> u32 {
    let payload = u32::try_from(payload_size).unwrap_or(u32::MAX);
    let max_local = if is_table_leaf {
        usable.saturating_sub(35)
    } else {
        ((usable.saturating_sub(12)) * 64 / 255).saturating_sub(23)
    };
    if payload <= max_local {
        return payload;
    }
    let min_local = ((usable.saturating_sub(12)) * 32 / 255).saturating_sub(23);
    let surplus = (payload.saturating_sub(min_local)) % (usable.saturating_sub(4));
    if surplus <= max_local.saturating_sub(min_local) {
        min_local + surplus
    } else {
        min_local
    }
}

// ---------------------------------------------------------------------------
// Diff phase: generate StructuredPagePatch from two parsed pages
// ---------------------------------------------------------------------------

/// Compute a `StructuredPagePatch` representing the delta from `base` to `modified`.
///
/// Both pages must have the same page type and table context.
///
/// # Errors
///
/// Returns [`MergeError::PageParseError`] if the page types don't match.
pub fn diff_parsed_pages(
    base: &ParsedPage,
    modified: &ParsedPage,
) -> Result<StructuredPagePatch, MergeError> {
    if base.page_type != modified.page_type {
        return Err(MergeError::PageParseError(
            "page type mismatch between base and modified".into(),
        ));
    }

    let mut cell_ops = Vec::new();

    // Build index of base cells by digest
    let base_cells: HashMap<[u8; 16], &ParsedCell> =
        base.cells.iter().map(|c| (c.cell_key_digest, c)).collect();
    let modified_cells: HashMap<[u8; 16], &ParsedCell> = modified
        .cells
        .iter()
        .map(|c| (c.cell_key_digest, c))
        .collect();

    // Detect inserts and replacements
    for mc in &modified.cells {
        if let Some(bc) = base_cells.get(&mc.cell_key_digest) {
            if bc.cell_bytes != mc.cell_bytes {
                cell_ops.push(CellOp {
                    cell_key_digest: mc.cell_key_digest,
                    kind: CellOpKind::Replace {
                        new_cell_bytes: mc.cell_bytes.clone(),
                    },
                });
            }
        } else {
            cell_ops.push(CellOp {
                cell_key_digest: mc.cell_key_digest,
                kind: CellOpKind::Insert {
                    cell_bytes: mc.cell_bytes.clone(),
                },
            });
        }
    }

    // Detect deletes
    for bc in &base.cells {
        if !modified_cells.contains_key(&bc.cell_key_digest) {
            cell_ops.push(CellOp {
                cell_key_digest: bc.cell_key_digest,
                kind: CellOpKind::Delete,
            });
        }
    }

    Ok(StructuredPagePatch {
        header_ops: Vec::new(),
        cell_ops,
        free_ops: Vec::new(),
        raw_xor_ranges: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Merge phase (§5.10.3 normative)
// ---------------------------------------------------------------------------

/// Merge two `StructuredPagePatch`es that are disjoint by `cell_key_digest`.
///
/// # Safety constraints (normative)
///
/// - `header_ops`: non-commutative; if both patches have header mutations → reject.
/// - `free_ops`: conservative; if either patch non-empty → reject.
/// - `raw_xor_ranges`: MUST be empty for structured pages.
/// - `cell_ops`: mergeable when disjoint by `cell_key_digest`.
///
/// # Errors
///
/// Returns [`MergeError::CellOverlap`] if both patches modify the same cell,
/// [`MergeError::HeaderConflict`] if both have header mutations, or
/// [`MergeError::RawXorForbiddenForStructuredPage`] if raw XOR is used on
/// structured pages.
pub fn merge_structured_patches(
    patch_a: &StructuredPagePatch,
    patch_b: &StructuredPagePatch,
    page_kind: MergePageKind,
) -> Result<StructuredPagePatch, MergeError> {
    // Validate no raw XOR on structured pages
    if page_kind.is_sqlite_structured()
        && (!patch_a.raw_xor_ranges.is_empty() || !patch_b.raw_xor_ranges.is_empty())
    {
        return Err(MergeError::RawXorForbiddenForStructuredPage);
    }

    // Header ops: non-commutative — reject if both have mutations
    if !patch_a.header_ops.is_empty() && !patch_b.header_ops.is_empty() {
        return Err(MergeError::HeaderConflict);
    }

    // Free-space ops: conservative — reject if either non-empty
    if !patch_a.free_ops.is_empty() || !patch_b.free_ops.is_empty() {
        return Err(MergeError::FreeSpaceConflict);
    }

    // Check cell disjointness
    let a_digests: std::collections::HashSet<[u8; 16]> = patch_a
        .cell_ops
        .iter()
        .map(|op| op.cell_key_digest)
        .collect();

    for op_b in &patch_b.cell_ops {
        if a_digests.contains(&op_b.cell_key_digest) {
            return Err(MergeError::CellOverlap {
                cell_key_digest: op_b.cell_key_digest,
            });
        }
    }

    // Compose: union of cell ops, take header ops from whichever has them
    let mut merged = StructuredPagePatch {
        header_ops: if patch_a.header_ops.is_empty() {
            patch_b.header_ops.clone()
        } else {
            patch_a.header_ops.clone()
        },
        cell_ops: Vec::with_capacity(patch_a.cell_ops.len() + patch_b.cell_ops.len()),
        free_ops: Vec::new(),
        raw_xor_ranges: Vec::new(),
    };

    merged.cell_ops.extend_from_slice(&patch_a.cell_ops);
    merged.cell_ops.extend_from_slice(&patch_b.cell_ops);

    // Sort by digest for deterministic order (canonical merge order)
    merged.cell_ops.sort_by_key(|op| op.cell_key_digest);

    Ok(merged)
}

// ---------------------------------------------------------------------------
// Apply phase: apply patch to parsed page
// ---------------------------------------------------------------------------

/// Apply a `StructuredPagePatch` to a `ParsedPage`, producing a new cell list.
///
/// # Errors
///
/// Currently infallible but returns `Result` for future extensibility.
pub fn apply_patch(
    base: &ParsedPage,
    patch: &StructuredPagePatch,
) -> Result<Vec<ParsedCell>, MergeError> {
    // Build mutable cell map keyed by digest
    let mut cell_map: BTreeMap<[u8; 16], ParsedCell> = base
        .cells
        .iter()
        .map(|c| (c.cell_key_digest, c.clone()))
        .collect();

    for op in &patch.cell_ops {
        match &op.kind {
            CellOpKind::Insert { cell_bytes } => {
                // Extract rowid from cell bytes for table pages
                let rowid = if base.page_type.is_table() {
                    extract_rowid_from_cell(cell_bytes, base.page_type)
                } else {
                    None
                };
                cell_map.insert(
                    op.cell_key_digest,
                    ParsedCell {
                        cell_key_digest: op.cell_key_digest,
                        cell_bytes: cell_bytes.clone(),
                        rowid,
                    },
                );
            }
            CellOpKind::Delete => {
                cell_map.remove(&op.cell_key_digest);
            }
            CellOpKind::Replace { new_cell_bytes } => {
                if let Some(cell) = cell_map.get_mut(&op.cell_key_digest) {
                    cell.cell_bytes.clone_from(new_cell_bytes);
                    if base.page_type.is_table() {
                        cell.rowid = extract_rowid_from_cell(new_cell_bytes, base.page_type);
                    }
                }
                // If cell not found, Replace is a no-op (defensive)
            }
        }
    }

    // Return cells sorted by rowid (table) or key bytes (index) for canonical order
    let usable = base.page_size.usable(base.reserved_per_page);
    let mut cells: Vec<ParsedCell> = cell_map.into_values().collect();
    cells.sort_by(|a, b| {
        if let (Some(ra), Some(rb)) = (a.rowid, b.rowid) {
            ra.cmp(&rb)
        } else {
            let key_a = extract_index_key_from_cell(&a.cell_bytes, base.page_type, usable);
            let key_b = extract_index_key_from_cell(&b.cell_bytes, base.page_type, usable);
            key_a.cmp(key_b)
        }
    });

    Ok(cells)
}

/// Extract the index key bytes (payload) from an index cell.
fn extract_index_key_from_cell(cell_bytes: &[u8], page_type: BTreePageType, usable: u32) -> &[u8] {
    match page_type {
        BTreePageType::LeafIndex => {
            if let Some((payload_size, n1)) = fsqlite_types::serial_type::read_varint(cell_bytes) {
                let local_payload = compute_local_payload_size(payload_size, usable, false);
                let payload_start = n1;
                let payload_end = (n1 + local_payload as usize).min(cell_bytes.len());
                return &cell_bytes[payload_start..payload_end];
            }
        }
        BTreePageType::InteriorIndex => {
            if cell_bytes.len() >= 4 {
                if let Some((payload_size, n1)) =
                    fsqlite_types::serial_type::read_varint(&cell_bytes[4..])
                {
                    let local_payload = compute_local_payload_size(payload_size, usable, false);
                    let payload_start = 4 + n1;
                    let payload_end =
                        (payload_start + local_payload as usize).min(cell_bytes.len());
                    return &cell_bytes[payload_start..payload_end];
                }
            }
        }
        _ => {}
    }
    &[]
}

/// Extract rowid from raw cell bytes.
fn extract_rowid_from_cell(cell_bytes: &[u8], page_type: BTreePageType) -> Option<i64> {
    match page_type {
        BTreePageType::LeafTable => {
            let (_, n1) = fsqlite_types::serial_type::read_varint(cell_bytes)?;
            let (rowid_u64, _) = fsqlite_types::serial_type::read_varint(&cell_bytes[n1..])?;
            #[allow(clippy::cast_possible_wrap)]
            Some(rowid_u64 as i64)
        }
        BTreePageType::InteriorTable => {
            if cell_bytes.len() < 4 {
                return None;
            }
            let (rowid_u64, _) = fsqlite_types::serial_type::read_varint(&cell_bytes[4..])?;
            #[allow(clippy::cast_possible_wrap)]
            Some(rowid_u64 as i64)
        }
        BTreePageType::LeafIndex | BTreePageType::InteriorIndex => None,
    }
}

// ---------------------------------------------------------------------------
// Repack phase (§5.10.3 normative: canonical repacker)
// ---------------------------------------------------------------------------

/// Repack a cell list into canonical B-tree page bytes.
///
/// The repacker MUST be canonical: `repack(parse(bytes))` yields a stable
/// layout across processes and replays for equivalent semantic content.
///
/// Layout strategy: cells are packed from the end of the page toward the
/// cell pointer array, in sorted order. No fragmented free bytes.
///
/// # Errors
///
/// Returns [`MergeError::PageOverflow`] if cells don't fit in the page.
pub fn repack_btree_page(
    cells: &[ParsedCell],
    page_type: BTreePageType,
    page_size: PageSize,
    reserved_per_page: u8,
    is_page1: bool,
    right_most_child: Option<PageNumber>,
) -> Result<Vec<u8>, MergeError> {
    let size = page_size.as_usize();
    let usable = page_size.usable(reserved_per_page) as usize;
    let header_offset = if is_page1 { 100 } else { 0 };
    let header_size: usize = if page_type.is_leaf() { 8 } else { 12 };
    let ptr_array_start = header_offset + header_size;
    let cell_count = cells.len();
    let ptr_array_end = ptr_array_start + cell_count * 2;

    // Calculate total cell content size
    let total_cell_bytes: usize = cells.iter().map(|c| c.cell_bytes.len()).sum();

    // Cell content area starts from `usable` and grows downward
    let cell_content_start = usable.saturating_sub(total_cell_bytes);
    if cell_content_start < ptr_array_end {
        return Err(MergeError::PageOverflow {
            required_bytes: total_cell_bytes + ptr_array_end,
            available_bytes: usable,
        });
    }

    let mut page = vec![0u8; size];

    // Write page type byte
    page[header_offset] = page_type as u8;

    // No freeblocks
    page[header_offset + 1] = 0;
    page[header_offset + 2] = 0;

    // Cell count
    let cc = u16::try_from(cell_count).map_err(|_| MergeError::PageOverflow {
        required_bytes: cell_count,
        available_bytes: 65535,
    })?;
    let cc_bytes = cc.to_be_bytes();
    page[header_offset + 3] = cc_bytes[0];
    page[header_offset + 4] = cc_bytes[1];

    // Cell content start
    #[allow(clippy::cast_possible_truncation)]
    let ccs_raw =
        if cell_content_start == 65536 || (cell_count == 0 && cell_content_start == usable) {
            0u16
        } else {
            u16::try_from(cell_content_start).unwrap_or(0)
        };
    let ccs_be = ccs_raw.to_be_bytes();
    page[header_offset + 5] = ccs_be[0];
    page[header_offset + 6] = ccs_be[1];

    // Fragmented free bytes = 0 (canonical layout)
    page[header_offset + 7] = 0;

    // Right-most child for interior pages
    if let Some(child) = right_most_child {
        if page_type.is_interior() {
            let child_bytes = child.get().to_be_bytes();
            page[header_offset + 8] = child_bytes[0];
            page[header_offset + 9] = child_bytes[1];
            page[header_offset + 10] = child_bytes[2];
            page[header_offset + 11] = child_bytes[3];
        }
    }

    // Write cells from end of page, and cell pointers
    let mut write_offset = usable;
    for (i, cell) in cells.iter().enumerate() {
        let cell_len = cell.cell_bytes.len();
        write_offset -= cell_len;

        // Write cell bytes
        page[write_offset..write_offset + cell_len].copy_from_slice(&cell.cell_bytes);

        // Write cell pointer
        #[allow(clippy::cast_possible_truncation)]
        let ptr = write_offset as u16;
        let ptr_bytes = ptr.to_be_bytes();
        let ptr_offset = ptr_array_start + i * 2;
        page[ptr_offset] = ptr_bytes[0];
        page[ptr_offset + 1] = ptr_bytes[1];
    }

    Ok(page)
}

// ---------------------------------------------------------------------------
// §5.10.4 Safety Ladder
// ---------------------------------------------------------------------------

/// Result of the commit-time merge ladder evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeLadderResult {
    /// Level 1: Base unchanged since snapshot — no merge needed.
    NoConflict,
    /// Level 2: Deterministic rebase succeeded.
    RebaseSucceeded {
        /// The rebased intent ops.
        rebased_ops: Vec<fsqlite_types::IntentOpKind>,
    },
    /// Level 3: Cell-disjoint structured page patch merge succeeded.
    StructuredMergeSucceeded {
        /// The merged page bytes.
        merged_page: Vec<u8>,
    },
    /// Level 4: Cell overlap — `SQLITE_BUSY_SNAPSHOT`.
    AbortBusySnapshot,
}

/// Execute the commit-time merge ladder (§5.10.4) for a single page.
///
/// The ladder is strict: we only take merges we can justify.
///
/// 1. Base unchanged since snapshot → OK (no merge needed).
/// 2. Schema epoch check → abort `SQLITE_SCHEMA` if mismatch.
/// 3. Deterministic rebase replay (preferred).
/// 4. Structured page patch merge (if ops are disjoint by semantic key).
/// 5. Abort/retry (`SQLITE_BUSY_SNAPSHOT`).
///
/// # Errors
///
/// Returns [`MergeError::SchemaEpochMismatch`] if the schema epoch changed,
/// or propagates errors from the parse/merge/repack pipeline.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn evaluate_merge_ladder(
    policy: WriteMergePolicy,
    base_page: &[u8],
    committed_page: &[u8],
    txn_page: &[u8],
    page_size: PageSize,
    reserved_per_page: u8,
    is_page1: bool,
    page_kind: MergePageKind,
    table_id: TableId,
    snapshot_schema_epoch: u64,
    current_schema_epoch: u64,
    intent_log: Option<&[IntentOp]>,
    base_row_reader: Option<&dyn BaseRowReader>,
    schema_lookup: Option<&dyn RebaseSchemaLookup>,
) -> Result<MergeLadderResult, MergeError> {
    // Level 1: No conflict — base unchanged
    if base_page == committed_page {
        info!(
            ladder_step = "level1",
            result = "no_conflict",
            "merge_ladder: base unchanged since snapshot"
        );
        return Ok(MergeLadderResult::NoConflict);
    }

    // Policy OFF → always abort
    if policy == WriteMergePolicy::Off {
        info!(
            ladder_step = "off",
            result = "abort",
            reason = "policy OFF",
            "merge_ladder: policy OFF — skipping all merge attempts"
        );
        return Ok(MergeLadderResult::AbortBusySnapshot);
    }

    // Schema epoch check (required before any merge attempt)
    if snapshot_schema_epoch != current_schema_epoch {
        info!(
            ladder_step = "schema_check",
            result = "abort",
            reason = "schema epoch mismatch",
            expected = snapshot_schema_epoch,
            actual = current_schema_epoch,
            "merge_ladder: schema epoch mismatch"
        );
        return Err(MergeError::SchemaEpochMismatch {
            expected: snapshot_schema_epoch,
            actual: current_schema_epoch,
        });
    }

    // Level 2: Deterministic rebase (preferred)
    if let (Some(log), Some(reader), Some(schema)) = (intent_log, base_row_reader, schema_lookup) {
        if !log.is_empty() {
            let eligibility = check_rebase_eligibility(log);
            if matches!(
                eligibility,
                crate::deterministic_rebase::RebaseEligibility::Eligible
            ) {
                let snapshot =
                    Snapshot::new(CommitSeq::new(0), SchemaEpoch::new(snapshot_schema_epoch));
                let no_unique = crate::index_regen::NoOpUniqueChecker;
                if let Ok(result) = deterministic_rebase(log, snapshot, reader, schema, &no_unique)
                {
                    info!(
                        ladder_step = "level2",
                        result = "merge",
                        reason = "deterministic rebase succeeded",
                        rebased_op_count = result.rebased_ops.len(),
                        "merge_ladder: deterministic rebase succeeded"
                    );
                    return Ok(MergeLadderResult::RebaseSucceeded {
                        rebased_ops: result.rebased_ops,
                    });
                }
                // Rebase failed — fall through to Level 3
            }
        }
    }

    // Level 3: Structured page patch merge (cell-disjoint)
    if page_kind.is_sqlite_structured() {
        let base_parsed =
            parse_btree_page(base_page, page_size, reserved_per_page, is_page1, table_id)?;
        let committed_parsed = parse_btree_page(
            committed_page,
            page_size,
            reserved_per_page,
            is_page1,
            table_id,
        )?;
        let txn_parsed =
            parse_btree_page(txn_page, page_size, reserved_per_page, is_page1, table_id)?;

        let patch_committed = diff_parsed_pages(&base_parsed, &committed_parsed)?;
        let patch_txn = diff_parsed_pages(&base_parsed, &txn_parsed)?;

        debug!(
            committed_cell_ops = patch_committed.cell_ops.len(),
            txn_cell_ops = patch_txn.cell_ops.len(),
            "merge_ladder: attempting cell-level structured merge"
        );

        match merge_structured_patches(&patch_committed, &patch_txn, page_kind) {
            Ok(merged_patch) => {
                debug!(
                    merged_cell_ops = merged_patch.cell_ops.len(),
                    "merge_ladder: patch merge succeeded, applying"
                );
                let merged_cells = apply_patch(&base_parsed, &merged_patch)?;
                let merged_page = repack_btree_page(
                    &merged_cells,
                    base_parsed.page_type,
                    page_size,
                    reserved_per_page,
                    is_page1,
                    base_parsed.header.right_most_child,
                )?;
                info!(
                    ladder_step = "level3",
                    result = "merge",
                    reason = "cell-disjoint physical merge",
                    cell_count = merged_cells.len(),
                    "merge_ladder: structured merge succeeded"
                );
                return Ok(MergeLadderResult::StructuredMergeSucceeded { merged_page });
            }
            Err(MergeError::CellOverlap { cell_key_digest }) => {
                info!(
                    ladder_step = "level4",
                    result = "abort",
                    reason = "cell overlap",
                    digest = ?cell_key_digest,
                    "merge_ladder: cell overlap — SQLITE_BUSY_SNAPSHOT"
                );
                return Ok(MergeLadderResult::AbortBusySnapshot);
            }
            Err(e) => return Err(e),
        }
    }

    // Level 4: No safe merge found
    info!(
        ladder_step = "level4",
        result = "abort",
        reason = "no safe merge found for unstructured page",
        "merge_ladder: abort — no safe merge available"
    );
    Ok(MergeLadderResult::AbortBusySnapshot)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{
        BtreeRef, IntentFootprint, IntentOp, IntentOpKind, RowId, SemanticKeyKind, SemanticKeyRef,
        StructuralEffects, TableId,
    };

    /// Helper: build a minimal leaf table page with given rowid→payload pairs.
    fn build_leaf_table_page(rows: &[(i64, &[u8])], page_size: PageSize) -> Vec<u8> {
        let size = page_size.as_usize();
        let mut page = vec![0u8; size];
        let header_offset = 0;
        let usable = size;

        // Page type: LeafTable (0x0D)
        page[header_offset] = 13;

        let cell_count = rows.len();
        let cc = u16::try_from(cell_count).expect("cell count fits u16");
        let cc_bytes = cc.to_be_bytes();
        page[header_offset + 3] = cc_bytes[0];
        page[header_offset + 4] = cc_bytes[1];

        // Fragmented free = 0
        page[header_offset + 7] = 0;

        // Build cells from end of page
        let ptr_array_start = header_offset + 8; // leaf header = 8 bytes
        let mut write_offset = usable;

        // Sort rows by rowid for canonical ordering
        let mut sorted_rows: Vec<(i64, &[u8])> = rows.to_vec();
        sorted_rows.sort_by_key(|(k, _)| *k);

        for (i, (rowid, payload)) in sorted_rows.iter().enumerate() {
            // Encode cell: [varint payload_size][varint rowid][payload]
            let mut cell = Vec::with_capacity(20 + payload.len());
            let mut tmp = [0u8; 9];

            // payload_size varint
            let n1 = encode_varint(&mut tmp, payload.len() as u64);
            cell.extend_from_slice(&tmp[..n1]);

            // rowid varint
            #[allow(clippy::cast_sign_loss)]
            let n2 = encode_varint(&mut tmp, *rowid as u64);
            cell.extend_from_slice(&tmp[..n2]);

            // payload
            cell.extend_from_slice(payload);

            write_offset -= cell.len();
            page[write_offset..write_offset + cell.len()].copy_from_slice(&cell);

            // Write cell pointer
            #[allow(clippy::cast_possible_truncation)]
            let ptr = write_offset as u16;
            let ptr_bytes = ptr.to_be_bytes();
            let po = ptr_array_start + i * 2;
            page[po] = ptr_bytes[0];
            page[po + 1] = ptr_bytes[1];
        }

        // cell content start
        #[allow(clippy::cast_possible_truncation)]
        let content_start = write_offset as u16;
        let cs_be = content_start.to_be_bytes();
        page[header_offset + 5] = cs_be[0];
        page[header_offset + 6] = cs_be[1];

        page
    }

    /// Simple varint encoder for tests.
    #[allow(clippy::cast_possible_truncation)]
    fn encode_varint(buf: &mut [u8; 9], val: u64) -> usize {
        if val <= 0x7F {
            buf[0] = val as u8;
            return 1;
        }

        let mut v = val;
        let mut stack = [0u8; 9];

        // 9-byte varint: byte 9 stores full 8 bits
        let n = if v > 0x7FFF_FFFF_FFFF_FFFF {
            stack[0] = v as u8;
            v >>= 8;
            1
        } else {
            0
        };

        // Remaining bytes store 7 bits each
        let mut temp = [0u8; 8];
        let mut tn = 0;
        loop {
            temp[tn] = (v & 0x7F) as u8;
            v >>= 7;
            tn += 1;
            if v == 0 {
                break;
            }
        }

        // Write in reverse (big-endian-like)
        let total = n + tn;
        let mut wi = 0;
        for i in (0..tn).rev() {
            let byte = if wi < total - 1 {
                temp[i] | 0x80
            } else {
                temp[i]
            };
            buf[wi] = byte;
            wi += 1;
        }
        // Append the full-byte suffix if present
        if n > 0 {
            buf[wi] = stack[0];
            wi += 1;
        }
        wi
    }

    fn table_id_1() -> TableId {
        TableId::new(1)
    }

    fn default_page_size() -> PageSize {
        PageSize::new(4096).expect("4096 is valid")
    }

    // -----------------------------------------------------------------------
    // Test 1: Structured page merge parse→merge→repack round-trip
    // -----------------------------------------------------------------------
    #[test]
    fn test_structured_page_merge_parse_merge_repack() {
        let ps = default_page_size();
        let tid = table_id_1();

        // Base page with rows 1, 2
        let base = build_leaf_table_page(&[(1, b"hello"), (2, b"world")], ps);

        // T1: adds row 3
        let t1 = build_leaf_table_page(&[(1, b"hello"), (2, b"world"), (3, b"foo")], ps);

        // T2: adds row 4
        let t2 = build_leaf_table_page(&[(1, b"hello"), (2, b"world"), (4, b"bar")], ps);

        let base_parsed = parse_btree_page(&base, ps, 0, false, tid).unwrap();
        let t1_parsed = parse_btree_page(&t1, ps, 0, false, tid).unwrap();
        let t2_parsed = parse_btree_page(&t2, ps, 0, false, tid).unwrap();

        assert_eq!(base_parsed.cells.len(), 2);
        assert_eq!(t1_parsed.cells.len(), 3);
        assert_eq!(t2_parsed.cells.len(), 3);

        // Diff
        let patch_t1 = diff_parsed_pages(&base_parsed, &t1_parsed).unwrap();
        let patch_t2 = diff_parsed_pages(&base_parsed, &t2_parsed).unwrap();

        assert_eq!(patch_t1.cell_ops.len(), 1); // insert row 3
        assert_eq!(patch_t2.cell_ops.len(), 1); // insert row 4

        // Merge
        let merged =
            merge_structured_patches(&patch_t1, &patch_t2, MergePageKind::BtreeLeafTable).unwrap();

        assert_eq!(merged.cell_ops.len(), 2);
        assert!(merged.raw_xor_ranges.is_empty());

        // Apply and repack
        let merged_cells = apply_patch(&base_parsed, &merged).unwrap();
        assert_eq!(merged_cells.len(), 4);

        let repacked =
            repack_btree_page(&merged_cells, BTreePageType::LeafTable, ps, 0, false, None).unwrap();

        // Verify round-trip: reparse and check
        let re_parsed = parse_btree_page(&repacked, ps, 0, false, tid).unwrap();
        assert_eq!(re_parsed.cells.len(), 4);

        // Verify idempotence: repack(parse(repacked)) == repacked
        let re_repacked = repack_btree_page(
            &re_parsed.cells,
            BTreePageType::LeafTable,
            ps,
            0,
            false,
            None,
        )
        .unwrap();
        assert_eq!(
            repacked, re_repacked,
            "canonical repacker must be idempotent"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: Raw XOR forbidden for structured pages
    // -----------------------------------------------------------------------
    #[test]
    fn test_raw_xor_forbidden_for_structured_pages() {
        let patch_with_xor = StructuredPagePatch {
            header_ops: Vec::new(),
            cell_ops: Vec::new(),
            free_ops: Vec::new(),
            raw_xor_ranges: vec![RangeXorPatch {
                offset: 100,
                data: vec![0xFF; 10],
            }],
        };

        let empty = StructuredPagePatch::default();

        // Structured pages MUST reject raw XOR
        for kind in [
            MergePageKind::BtreeLeafTable,
            MergePageKind::BtreeLeafIndex,
            MergePageKind::BtreeInteriorTable,
            MergePageKind::BtreeInteriorIndex,
            MergePageKind::Overflow,
            MergePageKind::Freelist,
            MergePageKind::PointerMap,
        ] {
            let result = merge_structured_patches(&patch_with_xor, &empty, kind);
            assert_eq!(
                result.unwrap_err(),
                MergeError::RawXorForbiddenForStructuredPage,
                "raw XOR must be forbidden for {kind:?}"
            );
        }

        // Opaque pages may use raw XOR
        let result = merge_structured_patches(&patch_with_xor, &empty, MergePageKind::Opaque);
        assert!(result.is_ok(), "opaque pages should allow raw XOR");

        // Validate via StructuredPagePatch method too
        assert!(
            patch_with_xor
                .validate_no_raw_xor_for_structured(MergePageKind::BtreeLeafTable)
                .is_err()
        );
        assert!(
            patch_with_xor
                .validate_no_raw_xor_for_structured(MergePageKind::Opaque)
                .is_ok()
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: Merge ladder level 1 — no conflict, direct commit
    // -----------------------------------------------------------------------
    #[test]
    fn test_merge_ladder_level1_no_conflict_direct_commit() {
        let ps = default_page_size();
        let base = build_leaf_table_page(&[(1, b"hello")], ps);
        let txn = build_leaf_table_page(&[(1, b"hello"), (2, b"world")], ps);

        // Base unchanged (committed == base) → Level 1: no merge needed
        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base, // base_page
            &base, // committed_page (unchanged)
            &txn,  // txn_page
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            table_id_1(),
            1, // snapshot epoch
            1, // current epoch (same)
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(result, MergeLadderResult::NoConflict);
    }

    // -----------------------------------------------------------------------
    // Test 4: Merge ladder level 2 — deterministic rebase
    // -----------------------------------------------------------------------
    #[test]
    #[allow(clippy::items_after_statements)]
    fn test_merge_ladder_level2_deterministic_rebase() {
        let ps = default_page_size();
        let tid = table_id_1();

        let base = build_leaf_table_page(&[(1, b"aaa")], ps);
        let committed = build_leaf_table_page(&[(1, b"aaa"), (5, b"ccc")], ps);
        let txn = build_leaf_table_page(&[(1, b"aaa"), (10, b"ddd")], ps);

        // Intent log with disjoint rowid inserts, no blocking reads
        let intent_log = vec![IntentOp {
            schema_epoch: 1,
            footprint: IntentFootprint {
                reads: Vec::new(),
                writes: vec![SemanticKeyRef::new(
                    BtreeRef::Table(tid),
                    SemanticKeyKind::TableRow,
                    &10_i64.to_le_bytes(),
                )],
                structural: StructuralEffects::NONE,
            },
            op: IntentOpKind::Insert {
                table: tid,
                key: RowId::new(10),
                record: b"ddd".to_vec(),
            },
        }];

        // No base row reader needed for Insert ops (they don't replay expressions)
        // But the rebase engine needs it — we provide a simple one
        struct SimpleReader;
        impl BaseRowReader for SimpleReader {
            fn read_base_row(&self, _table: TableId, _key: RowId) -> Option<Vec<u8>> {
                None // Row 10 doesn't exist in committed base
            }
        }

        struct NoSchema;
        impl RebaseSchemaLookup for NoSchema {
            fn table_constraints(
                &self,
                _table: TableId,
            ) -> Option<crate::deterministic_rebase::TableConstraints> {
                None
            }
            fn table_indexes(&self, _table: TableId) -> Vec<crate::index_regen::IndexDef> {
                Vec::new()
            }
        }

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &committed,
            &txn,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1,
            1,
            Some(&intent_log),
            Some(&SimpleReader),
            Some(&NoSchema),
        )
        .unwrap();

        // Insert ops with no blocking reads pass eligibility but rebase of
        // Insert ops may fail (target row not found is expected for inserts
        // in the current rebase engine). The ladder will fall through to
        // Level 3 (structured patch merge) which should succeed since the
        // rows are on disjoint rowids.
        match result {
            MergeLadderResult::RebaseSucceeded { .. }
            | MergeLadderResult::StructuredMergeSucceeded { .. } => {
                // Either path is valid — rebase may succeed or fall through to merge
            }
            other => panic!("expected rebase or structured merge, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: Merge ladder level 3 — cell-disjoint physical merge
    // -----------------------------------------------------------------------
    #[test]
    fn test_merge_ladder_level3_cell_disjoint_physical_merge() {
        let ps = default_page_size();
        let tid = table_id_1();

        let base = build_leaf_table_page(&[(1, b"orig")], ps);
        // T1 committed: added row 2
        let committed = build_leaf_table_page(&[(1, b"orig"), (2, b"new_a")], ps);
        // T2: added row 3
        let txn = build_leaf_table_page(&[(1, b"orig"), (3, b"new_b")], ps);

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &committed,
            &txn,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1,
            1,
            None, // No intent log — skip rebase, go to structured merge
            None,
            None,
        )
        .unwrap();

        match result {
            MergeLadderResult::StructuredMergeSucceeded { merged_page } => {
                let parsed = parse_btree_page(&merged_page, ps, 0, false, tid).unwrap();
                assert_eq!(parsed.cells.len(), 3, "merged page should have 3 cells");
                // Verify all rowids present
                let rowids: Vec<i64> = parsed.cells.iter().filter_map(|c| c.rowid).collect();
                assert!(rowids.contains(&1));
                assert!(rowids.contains(&2));
                assert!(rowids.contains(&3));
            }
            other => panic!("expected StructuredMergeSucceeded, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: Merge ladder level 4 — cell overlap → `SQLITE_BUSY_SNAPSHOT`
    // -----------------------------------------------------------------------
    #[test]
    fn test_merge_ladder_level4_cell_overlap_abort() {
        let ps = default_page_size();
        let tid = table_id_1();

        let base = build_leaf_table_page(&[(1, b"orig")], ps);
        // T1: modified row 1
        let committed = build_leaf_table_page(&[(1, b"from_t1")], ps);
        // T2: also modified row 1
        let txn = build_leaf_table_page(&[(1, b"from_t2")], ps);

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &committed,
            &txn,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1,
            1,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            result,
            MergeLadderResult::AbortBusySnapshot,
            "overlapping cell modifications must abort"
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: Cell key digest alignment with SemanticKeyRef
    // -----------------------------------------------------------------------
    #[test]
    fn test_cell_key_digest_alignment_with_semantic_key_ref() {
        let ps = default_page_size();
        let tid = table_id_1();
        let rowid: i64 = 42;

        let page = build_leaf_table_page(&[(rowid, b"test_payload")], ps);
        let parsed = parse_btree_page(&page, ps, 0, false, tid).unwrap();

        assert_eq!(parsed.cells.len(), 1);

        // Compute the expected digest using SemanticKeyRef::compute_digest
        let expected_digest = SemanticKeyRef::compute_digest(
            SemanticKeyKind::TableRow,
            BtreeRef::Table(tid),
            &rowid.to_le_bytes(),
        );

        assert_eq!(
            parsed.cells[0].cell_key_digest, expected_digest,
            "parsed cell digest must match SemanticKeyRef::compute_digest"
        );

        // Also verify via SemanticKeyRef::new
        let skr = SemanticKeyRef::new(
            BtreeRef::Table(tid),
            SemanticKeyKind::TableRow,
            &rowid.to_le_bytes(),
        );
        assert_eq!(
            parsed.cells[0].cell_key_digest, skr.key_digest,
            "parsed cell digest must match SemanticKeyRef.key_digest"
        );
    }

    // -----------------------------------------------------------------------
    // Test 8: Merged state equivalent to serial execution (proptest)
    // -----------------------------------------------------------------------
    #[test]
    fn test_merged_state_equivalent_to_serial_execution() {
        // Verify that physical merge produces output equivalent to some serial
        // execution of the participating transactions.
        //
        // For disjoint-rowid inserts: serial(T1, T2) and serial(T2, T1) both
        // produce the same result, which merge must match.
        let ps = default_page_size();
        let tid = table_id_1();

        let base = build_leaf_table_page(&[(1, b"base")], ps);

        // T1: insert row 10
        let t1 = build_leaf_table_page(&[(1, b"base"), (10, b"t1_data")], ps);
        // T2: insert row 20
        let t2 = build_leaf_table_page(&[(1, b"base"), (20, b"t2_data")], ps);

        // Serial execution: apply T1, then T2
        let serial_result =
            build_leaf_table_page(&[(1, b"base"), (10, b"t1_data"), (20, b"t2_data")], ps);

        // Physical merge
        let base_parsed = parse_btree_page(&base, ps, 0, false, tid).unwrap();
        let t1_parsed = parse_btree_page(&t1, ps, 0, false, tid).unwrap();
        let t2_parsed = parse_btree_page(&t2, ps, 0, false, tid).unwrap();

        let patch_t1 = diff_parsed_pages(&base_parsed, &t1_parsed).unwrap();
        let patch_t2 = diff_parsed_pages(&base_parsed, &t2_parsed).unwrap();

        let merged_patch =
            merge_structured_patches(&patch_t1, &patch_t2, MergePageKind::BtreeLeafTable).unwrap();

        let merged_cells = apply_patch(&base_parsed, &merged_patch).unwrap();
        let merged_page =
            repack_btree_page(&merged_cells, BTreePageType::LeafTable, ps, 0, false, None).unwrap();

        // Parse both and compare semantic content (rowids + payloads)
        let merged_parsed = parse_btree_page(&merged_page, ps, 0, false, tid).unwrap();
        let serial_parsed = parse_btree_page(&serial_result, ps, 0, false, tid).unwrap();

        assert_eq!(merged_parsed.cells.len(), serial_parsed.cells.len());

        for (mc, sc) in merged_parsed.cells.iter().zip(serial_parsed.cells.iter()) {
            assert_eq!(mc.rowid, sc.rowid, "rowids must match serial execution");
            assert_eq!(
                mc.cell_key_digest, sc.cell_key_digest,
                "digests must match serial execution"
            );
            assert_eq!(
                mc.cell_bytes, sc.cell_bytes,
                "cell bytes must match serial execution"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Additional edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_patch_merge() {
        let a = StructuredPagePatch::default();
        let b = StructuredPagePatch::default();
        let result = merge_structured_patches(&a, &b, MergePageKind::BtreeLeafTable).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_schema_epoch_mismatch_aborts_ladder() {
        let ps = default_page_size();
        let base = build_leaf_table_page(&[(1, b"a")], ps);
        let committed = build_leaf_table_page(&[(1, b"a"), (2, b"b")], ps);
        let txn = build_leaf_table_page(&[(1, b"a"), (3, b"c")], ps);

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &committed,
            &txn,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            table_id_1(),
            1, // snapshot epoch
            2, // current epoch (different!)
            None,
            None,
            None,
        );

        match result {
            Err(MergeError::SchemaEpochMismatch {
                expected: 1,
                actual: 2,
            }) => {}
            other => panic!("expected SchemaEpochMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_policy_off_always_aborts() {
        let ps = default_page_size();
        let base = build_leaf_table_page(&[(1, b"a")], ps);
        let committed = build_leaf_table_page(&[(1, b"a"), (2, b"b")], ps);
        let txn = build_leaf_table_page(&[(1, b"a"), (3, b"c")], ps);

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Off,
            &base,
            &committed,
            &txn,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            table_id_1(),
            1,
            1,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(result, MergeLadderResult::AbortBusySnapshot);
    }

    #[test]
    fn test_delete_merge_disjoint() {
        let ps = default_page_size();
        let tid = table_id_1();

        let base = build_leaf_table_page(&[(1, b"a"), (2, b"b"), (3, b"c")], ps);
        // T1: delete row 1
        let t1 = build_leaf_table_page(&[(2, b"b"), (3, b"c")], ps);
        // T2: delete row 3
        let t2 = build_leaf_table_page(&[(1, b"a"), (2, b"b")], ps);

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &t1,
            &t2,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1,
            1,
            None,
            None,
            None,
        )
        .unwrap();

        match result {
            MergeLadderResult::StructuredMergeSucceeded { merged_page } => {
                let parsed = parse_btree_page(&merged_page, ps, 0, false, tid).unwrap();
                assert_eq!(parsed.cells.len(), 1);
                assert_eq!(parsed.cells[0].rowid, Some(2));
            }
            other => panic!("expected StructuredMergeSucceeded, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // E2E Test: Merge ladder with concurrent writers (bd-3dv4)
    // -----------------------------------------------------------------------

    /// Simulate two concurrent writers inserting disjoint rowids on the same
    /// leaf page, then verify the merge ladder produces the correct outcome.
    /// Also covers the conflicting case (same rowid → abort).
    ///
    /// This exercises the full pipeline:
    ///   base snapshot → writer T1 commits → writer T2 attempts commit
    ///   → merge ladder evaluates → result matches serial schedule.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_merge_ladder_concurrent_writers() {
        let ps = default_page_size();
        let tid = table_id_1();

        // ---- CASE 1: Commuting writes (disjoint rowids) ----
        //
        // Base state: {row 1 = "init"}
        // Writer T1 (commits first): inserts row 5 → committed = {1, 5}
        // Writer T2 (attempts commit): inserts row 10 → tentative = {1, 10}
        //
        // Serial schedule equivalent: T1 then T2 → {1, 5, 10}
        let base = build_leaf_table_page(&[(1, b"init")], ps);
        let t1_committed = build_leaf_table_page(&[(1, b"init"), (5, b"t1val")], ps);
        let t2_tentative = build_leaf_table_page(&[(1, b"init"), (10, b"t2val")], ps);

        let result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &t1_committed,
            &t2_tentative,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1, // snapshot epoch
            1, // current epoch (same)
            None,
            None,
            None,
        )
        .expect("disjoint merge should succeed");

        match result {
            MergeLadderResult::StructuredMergeSucceeded { ref merged_page } => {
                let parsed = parse_btree_page(merged_page, ps, 0, false, tid).unwrap();
                assert_eq!(
                    parsed.cells.len(),
                    3,
                    "bead_id=bd-3dv4 case=e2e_commuting expected 3 rows after merge"
                );
                let rowids: Vec<i64> = parsed.cells.iter().filter_map(|c| c.rowid).collect();
                assert!(
                    rowids.contains(&1),
                    "bead_id=bd-3dv4 case=e2e_commuting missing row 1"
                );
                assert!(
                    rowids.contains(&5),
                    "bead_id=bd-3dv4 case=e2e_commuting missing row 5"
                );
                assert!(
                    rowids.contains(&10),
                    "bead_id=bd-3dv4 case=e2e_commuting missing row 10"
                );

                // Verify result matches serial schedule (T1 then T2).
                let serial =
                    build_leaf_table_page(&[(1, b"init"), (5, b"t1val"), (10, b"t2val")], ps);
                let serial_parsed = parse_btree_page(&serial, ps, 0, false, tid).unwrap();

                // Same number of cells
                assert_eq!(parsed.cells.len(), serial_parsed.cells.len());

                // Same rowids
                let serial_rowids: Vec<i64> =
                    serial_parsed.cells.iter().filter_map(|c| c.rowid).collect();
                assert_eq!(rowids, serial_rowids);

                // Same cell data (key digests and bytes)
                for (merged_cell, serial_cell) in
                    parsed.cells.iter().zip(serial_parsed.cells.iter())
                {
                    assert_eq!(
                        merged_cell.cell_key_digest, serial_cell.cell_key_digest,
                        "bead_id=bd-3dv4 case=e2e_commuting digest mismatch"
                    );
                    assert_eq!(
                        merged_cell.cell_bytes, serial_cell.cell_bytes,
                        "bead_id=bd-3dv4 case=e2e_commuting cell bytes mismatch for rowid {:?}",
                        merged_cell.rowid
                    );
                }
            }
            other => panic!(
                "bead_id=bd-3dv4 case=e2e_commuting expected StructuredMergeSucceeded, got {other:?}"
            ),
        }

        // ---- CASE 2: Non-commuting writes (same rowid → abort) ----
        //
        // Base state: {row 1 = "init"}
        // Writer T1 (commits first): modifies row 1 → {row 1 = "t1mod"}
        // Writer T2 (attempts commit): modifies row 1 → {row 1 = "t2mod"}
        //
        // Same cell modified by both → SQLITE_BUSY_SNAPSHOT
        let t1_conflict = build_leaf_table_page(&[(1, b"t1mod")], ps);
        let t2_conflict = build_leaf_table_page(&[(1, b"t2mod")], ps);

        let conflict_result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base,
            &t1_conflict,
            &t2_conflict,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1,
            1,
            None,
            None,
            None,
        )
        .expect("conflict should return AbortBusySnapshot, not error");

        assert_eq!(
            conflict_result,
            MergeLadderResult::AbortBusySnapshot,
            "bead_id=bd-3dv4 case=e2e_conflict expected abort for same-rowid conflict"
        );

        // ---- CASE 3: Multiple disjoint writers on same page ----
        //
        // Base state: {row 1 = "base"}
        // T1 inserts rows 2 and 3 (keeps row 1 unchanged)
        // T2 inserts rows 100 and 200 (keeps row 1 unchanged)
        // Expected: all 5 rows present after merge
        let base_multi = build_leaf_table_page(&[(1, b"base")], ps);
        let t1_multi = build_leaf_table_page(&[(1, b"base"), (2, b"t1a"), (3, b"t1b")], ps);
        let t2_multi = build_leaf_table_page(&[(1, b"base"), (100, b"t2a"), (200, b"t2b")], ps);

        let multi_result = evaluate_merge_ladder(
            WriteMergePolicy::Safe,
            &base_multi,
            &t1_multi,
            &t2_multi,
            ps,
            0,
            false,
            MergePageKind::BtreeLeafTable,
            tid,
            1,
            1,
            None,
            None,
            None,
        )
        .expect("multi-insert disjoint merge should succeed");

        match multi_result {
            MergeLadderResult::StructuredMergeSucceeded { ref merged_page } => {
                let parsed = parse_btree_page(merged_page, ps, 0, false, tid).unwrap();
                assert_eq!(
                    parsed.cells.len(),
                    5,
                    "bead_id=bd-3dv4 case=e2e_multi expected 5 rows"
                );
                let rowids: Vec<i64> = parsed.cells.iter().filter_map(|c| c.rowid).collect();
                for expected in &[1, 2, 3, 100, 200] {
                    assert!(
                        rowids.contains(expected),
                        "bead_id=bd-3dv4 case=e2e_multi missing row {expected}"
                    );
                }

                // Compare against serial schedule
                let serial = build_leaf_table_page(
                    &[
                        (1, b"base"),
                        (2, b"t1a"),
                        (3, b"t1b"),
                        (100, b"t2a"),
                        (200, b"t2b"),
                    ],
                    ps,
                );
                let serial_parsed = parse_btree_page(&serial, ps, 0, false, tid).unwrap();
                assert_eq!(parsed.cells.len(), serial_parsed.cells.len());
                for (mc, sc) in parsed.cells.iter().zip(serial_parsed.cells.iter()) {
                    assert_eq!(mc.cell_key_digest, sc.cell_key_digest);
                    assert_eq!(mc.cell_bytes, sc.cell_bytes);
                }
            }
            other => panic!(
                "bead_id=bd-3dv4 case=e2e_multi expected StructuredMergeSucceeded, got {other:?}"
            ),
        }
    }
}
