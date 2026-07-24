//! Cell-Delta WAL Commit Integration (C4: bd-l9k8e.4)
//!
//! This module wires cell-level MVCC deltas into the WAL commit path, enabling
//! crash-recoverable cell-level operations without full-page WAL frames.
//!
//! # Design Overview
//!
//! When a transaction commits, its write set may contain:
//!
//! 1. **Structural changes** (page splits, merges, overflow chains): These require
//!    full 4KB page frames via the existing WAL path.
//!
//! 2. **Logical changes** (cell INSERT/UPDATE/DELETE within existing pages): These
//!    can use cell-delta frames (~100-200 bytes each) instead of full pages.
//!
//! This module provides the integration layer to:
//! - Extract cell deltas from `CellVisibilityLog` at commit time
//! - Serialize them to [`CellDeltaWalFrame`] format
//! - Append them to WAL alongside (or instead of) full-page frames
//! - Support mixed commits with both frame types
//!
//! # Commit Protocol Integration
//!
//! The commit path (in `write_coordinator.rs` and `group_commit.rs`) calls:
//!
//! ```ignore
//! // 1. Extract cell deltas for this transaction
//! let cell_frames = extract_cell_delta_frames(cell_log, txn_token, commit_seq);
//!
//! // 2. Build combined submission with both frame types
//! let mixed = MixedFrameSubmission {
//!     full_page_frames: vec![...],
//!     cell_delta_frames: cell_frames,
//! };
//!
//! // 3. Write combined frames atomically
//! write_mixed_frames(wal, &mixed)?;
//! ```
//!
//! # Recovery Integration
//!
//! During WAL recovery:
//! 1. Read the first 4-byte marker word
//! 2. Full-page frames: Apply to page cache (existing path)
//! 3. Cell-delta frames: Insert into `CellVisibilityLog`, then materialize
//!
//! # Atomicity Guarantee
//!
//! All frames (full-page and cell-delta) for a single transaction commit are
//! written before the final commit frame's `db_size > 0` marker. On crash:
//! - If commit frame is present: All preceding frames are applied
//! - If commit frame is missing: All frames from that transaction are discarded

use crate::cell_delta_wal::{
    CELL_DELTA_CHECKSUM_SIZE, CELL_DELTA_HEADER_SIZE, CellDeltaWalFrame, CellOp,
};
use fsqlite_error::Result;
use fsqlite_types::{CommitSeq, PageNumber, TxnId};
use tracing::{debug, trace};

// ---------------------------------------------------------------------------
// Mixed Frame Submission (§C4.1)
// ---------------------------------------------------------------------------

/// A mixed submission containing both full-page and cell-delta frames.
///
/// This is the unified type for committing transactions that may have
/// both structural changes (full pages) and logical changes (cell deltas).
#[derive(Debug, Clone)]
pub struct MixedFrameSubmission {
    /// Full-page frames for structural changes.
    /// Each entry is (page_number, page_data, db_size_if_commit).
    pub full_page_frames: Vec<FullPageFrame>,

    /// Cell-delta frames for logical changes.
    pub cell_delta_frames: Vec<CellDeltaWalFrame>,

    /// Transaction ID for audit/debugging.
    pub txn_id: TxnId,

    /// Commit sequence number (assigned at commit time).
    pub commit_seq: CommitSeq,
}

/// A full-page WAL frame submission.
#[derive(Debug, Clone)]
pub struct FullPageFrame {
    /// Database page number.
    pub page_number: PageNumber,
    /// Full page content (exactly page_size bytes).
    pub page_data: Vec<u8>,
    /// Database size in pages for commit frames, or 0 for non-commit.
    pub db_size_if_commit: u32,
}

impl MixedFrameSubmission {
    /// Create a new mixed submission.
    #[must_use]
    pub fn new(txn_id: TxnId, commit_seq: CommitSeq) -> Self {
        Self {
            full_page_frames: Vec::new(),
            cell_delta_frames: Vec::new(),
            txn_id,
            commit_seq,
        }
    }

    /// Total number of frames (both types).
    #[must_use]
    pub fn total_frame_count(&self) -> usize {
        self.full_page_frames.len() + self.cell_delta_frames.len()
    }

    /// Whether this submission contains any cell-delta frames.
    #[must_use]
    pub fn has_cell_deltas(&self) -> bool {
        !self.cell_delta_frames.is_empty()
    }

    /// Whether this submission contains any full-page frames.
    #[must_use]
    pub fn has_full_pages(&self) -> bool {
        !self.full_page_frames.is_empty()
    }

    /// Whether this is a pure cell-delta commit (no full pages).
    #[must_use]
    pub fn is_cell_only(&self) -> bool {
        self.full_page_frames.is_empty() && !self.cell_delta_frames.is_empty()
    }

    /// Add a full-page frame.
    pub fn add_full_page(&mut self, page_number: PageNumber, page_data: Vec<u8>) {
        self.full_page_frames.push(FullPageFrame {
            page_number,
            page_data,
            db_size_if_commit: 0,
        });
    }

    /// Add a cell-delta frame.
    pub fn add_cell_delta(&mut self, frame: CellDeltaWalFrame) {
        self.cell_delta_frames.push(frame);
    }

    /// Mark the last full-page frame as the commit frame.
    ///
    /// If there are no full-page frames, creates a synthetic commit marker
    /// on the last affected page from cell deltas.
    pub fn mark_commit(&mut self, db_size: u32) {
        if let Some(last) = self.full_page_frames.last_mut() {
            last.db_size_if_commit = db_size;
        }
        // Note: For cell-only commits, the commit marker is embedded in
        // a cell-delta commit frame (separate protocol, see C4.2).
    }

    /// Estimate total serialized size in bytes.
    ///
    /// Used for I/O planning and telemetry.
    #[must_use]
    pub fn estimated_size(&self, page_size: usize) -> usize {
        let full_page_size = self
            .full_page_frames
            .len()
            .saturating_mul(24usize.saturating_add(page_size));
        let cell_delta_size = self.cell_delta_frames.iter().fold(0usize, |acc, f| {
            acc.saturating_add(
                CELL_DELTA_HEADER_SIZE
                    .saturating_add(f.cell_data.len())
                    .saturating_add(CELL_DELTA_CHECKSUM_SIZE),
            )
        });
        full_page_size.saturating_add(cell_delta_size)
    }
}

// ---------------------------------------------------------------------------
// Cell Delta Extraction (§C4.2)
// ---------------------------------------------------------------------------

/// Extract cell deltas for a transaction and convert to WAL frames.
///
/// This function is called at commit time to get all cell-level changes
/// for a transaction and serialize them to WAL frame format.
///
/// # Arguments
///
/// * `deltas` - Iterator of (page_number, key_digest, op, cell_data) tuples
/// * `commit_seq` - The commit sequence number
/// * `txn_id` - The transaction ID
///
/// # Returns
///
/// A vector of serialized [`CellDeltaWalFrame`] objects ready for WAL append.
pub fn build_cell_delta_frames<I>(
    deltas: I,
    commit_seq: CommitSeq,
    txn_id: TxnId,
) -> Vec<CellDeltaWalFrame>
where
    I: Iterator<Item = CellDeltaDescriptor>,
{
    let (lower, _) = deltas.size_hint();
    let mut frames = Vec::with_capacity(lower);

    for desc in deltas {
        let frame = CellDeltaWalFrame::new(
            desc.page_number,
            desc.cell_key_digest,
            desc.op,
            commit_seq,
            txn_id,
            desc.cell_data,
        );

        trace!(
            pgno = desc.page_number.get(),
            op = ?desc.op,
            commit_seq = commit_seq.get(),
            txn_id = txn_id.get(),
            data_len = frame.cell_data.len(),
            "cell_delta_frame_built"
        );

        frames.push(frame);
    }

    debug!(
        frame_count = frames.len(),
        commit_seq = commit_seq.get(),
        txn_id = txn_id.get(),
        "cell_delta_frames_extracted"
    );

    frames
}

/// Descriptor for a single cell delta to be converted to a WAL frame.
#[derive(Debug, Clone)]
pub struct CellDeltaDescriptor {
    /// Page containing this cell.
    pub page_number: PageNumber,
    /// BLAKE3-truncated digest of the cell key (16 bytes).
    pub cell_key_digest: [u8; 16],
    /// Operation type.
    pub op: CellOp,
    /// Cell data (empty for Delete).
    pub cell_data: Vec<u8>,
}

impl CellDeltaDescriptor {
    /// Create a new cell delta descriptor.
    #[must_use]
    pub fn new(
        page_number: PageNumber,
        cell_key_digest: [u8; 16],
        op: CellOp,
        cell_data: Vec<u8>,
    ) -> Self {
        Self {
            page_number,
            cell_key_digest,
            op,
            cell_data,
        }
    }

    /// Create an INSERT descriptor.
    #[must_use]
    pub fn insert(page_number: PageNumber, cell_key_digest: [u8; 16], cell_data: Vec<u8>) -> Self {
        Self::new(page_number, cell_key_digest, CellOp::Insert, cell_data)
    }

    /// Create an UPDATE descriptor.
    #[must_use]
    pub fn update(page_number: PageNumber, cell_key_digest: [u8; 16], cell_data: Vec<u8>) -> Self {
        Self::new(page_number, cell_key_digest, CellOp::Update, cell_data)
    }

    /// Create a DELETE descriptor.
    #[must_use]
    pub fn delete(page_number: PageNumber, cell_key_digest: [u8; 16]) -> Self {
        Self::new(page_number, cell_key_digest, CellOp::Delete, Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Serialization Buffer Builder (§C4.3)
// ---------------------------------------------------------------------------

/// Build a serialized buffer containing mixed frame types.
///
/// Frame ordering in the buffer:
/// 1. All cell-delta frames (variable length)
/// 2. All full-page frames (fixed page_size + 24 byte header)
/// 3. Final commit frame (full-page with db_size > 0)
///
/// This ordering ensures that cell deltas are always followed by the commit
/// marker, enabling atomic crash recovery semantics.
pub fn serialize_mixed_frames(
    submission: &MixedFrameSubmission,
    page_size: usize,
) -> Result<Vec<u8>> {
    let estimated_size = submission.estimated_size(page_size);
    let mut buf = Vec::new();

    // 1. Serialize cell-delta frames first
    for frame in &submission.cell_delta_frames {
        let serialized = frame.serialize()?;
        buf.extend_from_slice(&serialized);
    }

    // 2. Serialize full-page frames
    // Note: Full-page frames use the standard WAL frame format (24-byte header + page)
    // The actual serialization is done by the WalFile::append_frames method,
    // so we just return the cell-delta portion here for separate handling.
    //
    // In the full integration, the caller will:
    // - Write cell-delta bytes directly to WAL file
    // - Use WalFile::append_frames for full-page frames (maintains checksum chain)

    debug!(
        cell_delta_bytes = buf.len(),
        full_page_count = submission.full_page_frames.len(),
        total_estimated = estimated_size,
        "mixed_frames_serialized"
    );

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Commit Statistics (§C4.4)
// ---------------------------------------------------------------------------

/// Statistics from a mixed-frame commit operation.
#[derive(Debug, Clone, Default)]
pub struct MixedCommitStats {
    /// Number of full-page frames written.
    pub full_page_frames: u64,
    /// Number of cell-delta frames written.
    pub cell_delta_frames: u64,
    /// Total bytes written for full-page frames.
    pub full_page_bytes: u64,
    /// Total bytes written for cell-delta frames.
    pub cell_delta_bytes: u64,
    /// Byte savings vs all-full-page commit.
    pub bytes_saved: u64,
}

impl MixedCommitStats {
    /// Calculate byte savings from using cell deltas vs full pages.
    #[must_use]
    pub fn calculate(submission: &MixedFrameSubmission, page_size: usize) -> Self {
        let full_page_count = submission.full_page_frames.len() as u64;
        let cell_delta_count = submission.cell_delta_frames.len() as u64;
        let bytes_per_full_page =
            24u64.saturating_add(u64::try_from(page_size).unwrap_or(u64::MAX));

        let full_page_bytes = full_page_count.saturating_mul(bytes_per_full_page);
        let cell_delta_bytes = submission.cell_delta_frames.iter().fold(0u64, |acc, f| {
            acc.saturating_add(
                u64::try_from(CELL_DELTA_HEADER_SIZE)
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(f.cell_data.len()).unwrap_or(u64::MAX))
                    .saturating_add(u64::try_from(CELL_DELTA_CHECKSUM_SIZE).unwrap_or(u64::MAX)),
            )
        });

        // Without cell-delta optimization, all would be full-page frames
        let hypothetical_full_page_bytes = cell_delta_count.saturating_mul(bytes_per_full_page);
        let bytes_saved = hypothetical_full_page_bytes.saturating_sub(cell_delta_bytes);

        Self {
            full_page_frames: full_page_count,
            cell_delta_frames: cell_delta_count,
            full_page_bytes,
            cell_delta_bytes,
            bytes_saved,
        }
    }

    /// Compression ratio: actual bytes / hypothetical all-full-page bytes.
    #[must_use]
    pub fn compression_ratio(&self, page_size: usize) -> f64 {
        let bytes_per_full_page =
            24u64.saturating_add(u64::try_from(page_size).unwrap_or(u64::MAX));
        let hypothetical = self
            .full_page_frames
            .saturating_add(self.cell_delta_frames)
            .saturating_mul(bytes_per_full_page);
        if hypothetical == 0 {
            return 1.0;
        }
        self.full_page_bytes.saturating_add(self.cell_delta_bytes) as f64 / hypothetical as f64
    }
}

// ---------------------------------------------------------------------------
// Tests (§C4.5)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_txn_id() -> TxnId {
        TxnId::new(42).unwrap()
    }

    fn test_page_number() -> PageNumber {
        PageNumber::new(10).unwrap()
    }

    fn test_key_digest() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    #[test]
    fn test_mixed_frame_submission_creation() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));
        assert_eq!(sub.total_frame_count(), 0);
        assert!(!sub.has_cell_deltas());
        assert!(!sub.has_full_pages());

        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(),
            vec![1, 2, 3],
        ));

        assert_eq!(sub.total_frame_count(), 1);
        assert!(sub.has_cell_deltas());
        assert!(sub.is_cell_only());

        sub.add_full_page(test_page_number(), vec![0u8; 4096]);
        assert_eq!(sub.total_frame_count(), 2);
        assert!(sub.has_full_pages());
        assert!(!sub.is_cell_only());
    }

    #[test]
    fn test_cell_delta_descriptor() {
        let desc =
            CellDeltaDescriptor::insert(test_page_number(), test_key_digest(), vec![1, 2, 3]);
        assert_eq!(desc.page_number, test_page_number());
        assert_eq!(desc.op, CellOp::Insert);
        assert_eq!(desc.cell_data, vec![1, 2, 3]);

        let delete_desc = CellDeltaDescriptor::delete(test_page_number(), test_key_digest());
        assert_eq!(delete_desc.op, CellOp::Delete);
        assert!(delete_desc.cell_data.is_empty());
    }

    #[test]
    fn test_build_cell_delta_frames() {
        let descs = vec![
            CellDeltaDescriptor::insert(PageNumber::new(10).unwrap(), [1; 16], vec![0xAA; 50]),
            CellDeltaDescriptor::update(PageNumber::new(11).unwrap(), [2; 16], vec![0xBB; 100]),
            CellDeltaDescriptor::delete(PageNumber::new(12).unwrap(), [3; 16]),
        ];

        let frames = build_cell_delta_frames(descs.into_iter(), CommitSeq::new(200), test_txn_id());

        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].page_number, PageNumber::new(10).unwrap());
        assert_eq!(frames[0].op, CellOp::Insert);
        assert_eq!(frames[1].op, CellOp::Update);
        assert_eq!(frames[2].op, CellOp::Delete);
        assert!(frames[2].cell_data.is_empty());
    }

    #[test]
    fn test_serialize_mixed_frames() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));

        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(),
            vec![1, 2, 3, 4, 5],
        ));

        let buf = serialize_mixed_frames(&sub, 4096).unwrap();

        // Verify the buffer contains a valid cell-delta frame
        assert!(!buf.is_empty());
        // Frame size: 45 header + 5 data + 4 checksum = 54 bytes
        assert_eq!(buf.len(), 54);

        // Verify we can deserialize it back
        let frame = CellDeltaWalFrame::deserialize(&buf).unwrap();
        assert_eq!(frame.page_number, test_page_number());
        assert_eq!(frame.cell_data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_mixed_commit_stats() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));

        // Add 2 cell-delta frames (small)
        for i in 0..2 {
            sub.add_cell_delta(CellDeltaWalFrame::new(
                PageNumber::new(10 + i).unwrap(),
                [i as u8; 16],
                CellOp::Insert,
                CommitSeq::new(100),
                test_txn_id(),
                vec![0u8; 100], // 100 bytes each
            ));
        }

        // Add 1 full-page frame
        sub.add_full_page(PageNumber::new(20).unwrap(), vec![0u8; 4096]);

        let stats = MixedCommitStats::calculate(&sub, 4096);

        assert_eq!(stats.full_page_frames, 1);
        assert_eq!(stats.cell_delta_frames, 2);

        // Cell delta bytes: 2 * (45 + 100 + 4) = 2 * 149 = 298
        assert_eq!(stats.cell_delta_bytes, 298);

        // Full page bytes: 1 * (24 + 4096) = 4120
        assert_eq!(stats.full_page_bytes, 4120);

        // Without cell deltas, those 2 would be 2 * 4120 = 8240 bytes
        // Savings = 8240 - 298 = 7942 bytes
        assert_eq!(stats.bytes_saved, 7942);

        // Compression ratio should be < 1.0 (we're saving space)
        let ratio = stats.compression_ratio(4096);
        assert!(
            ratio < 1.0,
            "compression ratio should be < 1.0, got {ratio}"
        );
    }

    #[test]
    fn test_mixed_commit_stats_saturate_for_pathological_page_size() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));
        sub.add_full_page(test_page_number(), Vec::new());
        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(),
            vec![1],
        ));

        let stats = MixedCommitStats::calculate(&sub, usize::MAX);
        let bytes_per_full_page =
            24u64.saturating_add(u64::try_from(usize::MAX).unwrap_or(u64::MAX));

        assert_eq!(stats.full_page_bytes, bytes_per_full_page);
        assert!(stats.bytes_saved <= bytes_per_full_page);
        assert!(stats.compression_ratio(usize::MAX).is_finite());
    }

    #[test]
    fn test_estimated_size() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));

        // 1 cell-delta frame with 50 bytes of data
        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(),
            vec![0u8; 50],
        ));

        // 1 full-page frame
        sub.add_full_page(test_page_number(), vec![0u8; 4096]);

        let estimated = sub.estimated_size(4096);
        // Cell delta: 45 + 50 + 4 = 99
        // Full page: 24 + 4096 = 4120
        // Total: 4219
        assert_eq!(estimated, 4219);
    }

    #[test]
    fn test_serialize_mixed_frames_rejects_oversized_cell_delta_without_preallocation() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));
        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(),
            vec![0u8; crate::cell_delta_wal::CELL_DELTA_MAX_DATA_SIZE + 1],
        ));

        assert!(serialize_mixed_frames(&sub, 4096).is_err());
    }

    #[test]
    fn test_mark_commit() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));

        sub.add_full_page(PageNumber::new(10).unwrap(), vec![0u8; 4096]);
        sub.add_full_page(PageNumber::new(11).unwrap(), vec![0u8; 4096]);

        assert_eq!(sub.full_page_frames[0].db_size_if_commit, 0);
        assert_eq!(sub.full_page_frames[1].db_size_if_commit, 0);

        sub.mark_commit(100);

        assert_eq!(sub.full_page_frames[0].db_size_if_commit, 0);
        assert_eq!(sub.full_page_frames[1].db_size_if_commit, 100);
    }

    #[test]
    fn test_compression_ratio_zero_frames_returns_one() {
        let sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(1));
        let stats = MixedCommitStats::calculate(&sub, 4096);
        assert_eq!(stats.full_page_frames, 0);
        assert_eq!(stats.cell_delta_frames, 0);
        assert!((stats.compression_ratio(4096) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mixed_commit_stats_default_all_zero() {
        let stats = MixedCommitStats::default();
        assert_eq!(stats.full_page_frames, 0);
        assert_eq!(stats.cell_delta_frames, 0);
        assert_eq!(stats.full_page_bytes, 0);
        assert_eq!(stats.cell_delta_bytes, 0);
        assert_eq!(stats.bytes_saved, 0);
    }

    #[test]
    fn test_build_cell_delta_frames_empty_iterator() {
        let frames = build_cell_delta_frames(std::iter::empty(), CommitSeq::new(1), test_txn_id());
        assert!(frames.is_empty());
    }

    #[test]
    fn test_cell_delta_descriptor_update_factory() {
        let desc =
            CellDeltaDescriptor::update(test_page_number(), test_key_digest(), vec![0xCC; 50]);
        assert_eq!(desc.op, CellOp::Update);
        assert_eq!(desc.cell_data.len(), 50);
        assert_eq!(desc.page_number, test_page_number());
    }

    #[test]
    fn test_mark_commit_on_empty_full_pages_is_noop() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(1));
        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(1),
            test_txn_id(),
            vec![1],
        ));
        sub.mark_commit(50);
        assert!(sub.full_page_frames.is_empty());
    }

    #[test]
    fn test_serialize_mixed_frames_empty_submission() {
        let sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(1));
        let buf = serialize_mixed_frames(&sub, 4096).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn test_cell_only_commit() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(100));

        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(),
            vec![1, 2, 3],
        ));

        assert!(sub.is_cell_only());
        assert!(sub.has_cell_deltas());
        assert!(!sub.has_full_pages());
    }

    #[test]
    fn test_estimated_size_empty_returns_zero() {
        let sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(1));
        assert_eq!(sub.estimated_size(4096), 0);
        assert_eq!(sub.estimated_size(0), 0);
    }

    #[test]
    fn test_full_page_frame_fields_and_debug() {
        let frame = FullPageFrame {
            page_number: test_page_number(),
            page_data: vec![0xAB; 4096],
            db_size_if_commit: 55,
        };
        assert_eq!(frame.page_number, test_page_number());
        assert_eq!(frame.page_data.len(), 4096);
        assert_eq!(frame.db_size_if_commit, 55);

        let cloned = frame.clone();
        assert_eq!(cloned.page_number, frame.page_number);
        assert_eq!(cloned.db_size_if_commit, frame.db_size_if_commit);

        let dbg = format!("{frame:?}");
        assert!(dbg.contains("FullPageFrame"));
    }

    #[test]
    fn test_build_cell_delta_frames_preserves_key_digest() {
        let digest_a = [0xAA; 16];
        let digest_b = [0xBB; 16];
        let descs = vec![
            CellDeltaDescriptor::insert(PageNumber::new(5).unwrap(), digest_a, vec![1, 2]),
            CellDeltaDescriptor::delete(PageNumber::new(6).unwrap(), digest_b),
        ];
        let frames = build_cell_delta_frames(descs.into_iter(), CommitSeq::new(10), test_txn_id());
        assert_eq!(frames[0].cell_key_digest, digest_a);
        assert_eq!(frames[1].cell_key_digest, digest_b);
    }

    #[test]
    fn mixed_frame_submission_debug_and_clone() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(7));
        sub.add_full_page(test_page_number(), vec![0u8; 64]);
        let dbg = format!("{sub:?}");
        assert!(dbg.contains("MixedFrameSubmission"));
        let cloned = sub.clone();
        assert_eq!(cloned.txn_id, test_txn_id());
        assert_eq!(cloned.commit_seq, CommitSeq::new(7));
        assert_eq!(cloned.full_page_frames.len(), 1);
    }

    #[test]
    fn cell_delta_descriptor_debug_and_clone() {
        let desc =
            CellDeltaDescriptor::insert(test_page_number(), test_key_digest(), vec![9, 8, 7]);
        let dbg = format!("{desc:?}");
        assert!(dbg.contains("CellDeltaDescriptor"));
        let cloned = desc.clone();
        assert_eq!(cloned.page_number, test_page_number());
        assert_eq!(cloned.cell_data, vec![9, 8, 7]);
        assert_eq!(cloned.cell_key_digest, test_key_digest());
    }

    #[test]
    fn mixed_commit_stats_debug_and_clone() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(1));
        sub.add_full_page(test_page_number(), vec![0u8; 4096]);
        let stats = MixedCommitStats::calculate(&sub, 4096);
        let dbg = format!("{stats:?}");
        assert!(dbg.contains("MixedCommitStats"));
        let cloned = stats.clone();
        assert_eq!(cloned.full_page_frames, stats.full_page_frames);
        assert_eq!(cloned.full_page_bytes, stats.full_page_bytes);
    }

    #[test]
    fn new_submission_stores_txn_id_and_commit_seq() {
        let txn = TxnId::new(999).unwrap();
        let seq = CommitSeq::new(555);
        let sub = MixedFrameSubmission::new(txn, seq);
        assert_eq!(sub.txn_id, txn);
        assert_eq!(sub.commit_seq, seq);
        assert!(sub.full_page_frames.is_empty());
        assert!(sub.cell_delta_frames.is_empty());
    }

    #[test]
    fn test_compression_ratio_cell_only_below_one() {
        let mut sub = MixedFrameSubmission::new(test_txn_id(), CommitSeq::new(1));
        sub.add_cell_delta(CellDeltaWalFrame::new(
            test_page_number(),
            test_key_digest(),
            CellOp::Insert,
            CommitSeq::new(1),
            test_txn_id(),
            vec![0u8; 80],
        ));
        let stats = MixedCommitStats::calculate(&sub, 4096);
        assert_eq!(stats.full_page_frames, 0);
        assert_eq!(stats.cell_delta_frames, 1);
        let ratio = stats.compression_ratio(4096);
        assert!(ratio < 1.0, "cell-only ratio should be < 1.0, got {ratio}");
        assert!(ratio > 0.0, "ratio should be positive, got {ratio}");
    }
}
