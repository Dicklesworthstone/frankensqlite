//! Cell-Delta WAL Frame Format (C4-WAL: bd-l9k8e.10)
//!
//! This module implements the WAL record format for cell-level deltas, enabling
//! crash-recoverable cell-level MVCC without writing full 4KB page images.
//!
//! # Design Overview
//!
//! Cell-delta frames are distinguished from full-page frames by a dedicated
//! marker word in the first 4 bytes. Older experimental encodings also packed
//! the page number into the lower 31 bits of that word. Current recovery
//! intentionally rejects those legacy frames so high-bit page numbers remain
//! unambiguously valid full-page frames; only the marker helper below decodes
//! the old word shape for diagnostics.
//!
//! ## Frame Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ frame_marker = 0x00000000  (4 bytes, BE) — cell-delta indicator │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ actual_page_number          (4 bytes, BE)                      │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ cell_key_digest             (16 bytes)                         │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ op                          (1 byte: 1=Insert, 2=Update, 3=Del)│
//! ├─────────────────────────────────────────────────────────────────┤
//! │ commit_seq                  (8 bytes, BE)                      │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ txn_id                      (8 bytes, BE)                      │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ cell_data_len               (4 bytes, BE; 0 for Delete)        │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ cell_data                   (cell_data_len bytes)              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ checksum                    (4 bytes, CRC32C)                  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Fixed header: 45 bytes. Total: 45 + cell_data_len + 4 (checksum) = 49 + cell_data_len.
//!
//! ## Comparison with Full-Page Frames
//!
//! | Frame Type | Typical Size | Use Case |
//! |------------|--------------|----------|
//! | Full-page  | 4096+ bytes  | Structural changes (splits, merges) |
//! | Cell-delta | ~100-200 bytes | Logical row ops (INSERT, UPDATE, DELETE) |
//!
//! A typical 100-byte INSERT: 149 bytes vs 4096 bytes = **27x smaller**.
//!
//! # Recovery
//!
//! During crash recovery:
//! 1. Read WAL frames sequentially
//! 2. Check the frame envelope for the exact cell-delta marker and checksum
//! 3. Full-page frames: apply directly to page cache
//! 4. Cell-delta frames: reconstruct into CellVisibilityLog
//! 5. Materialize affected pages from CellVisibilityLog

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{CommitSeq, PageNumber, TxnId};
use tracing::debug;

/// Marker word that indicates a cell-delta frame.
///
/// This must stay outside the valid page-number domain because mixed WAL
/// recovery uses the first word to distinguish cell-delta records from ordinary
/// full-page frames. Page number 0 is invalid in SQLite, so it is a safe
/// in-band type word.
pub const CELL_DELTA_FRAME_MARKER: u32 = 0;

/// Legacy experimental frames used the high bit of the page-number word as a
/// marker. Keep the helper below able to decode that shape, but never use it as
/// the current discriminator because high-bit page numbers are valid.
const LEGACY_CELL_DELTA_FRAME_MARKER: u32 = 0x8000_0000;

/// Fixed header size for cell-delta frames (excluding variable cell_data).
pub const CELL_DELTA_HEADER_SIZE: usize = 45;

/// Checksum size (CRC32C).
pub const CELL_DELTA_CHECKSUM_SIZE: usize = 4;

/// Minimum frame size (header + checksum, no cell data).
pub const CELL_DELTA_MIN_FRAME_SIZE: usize = CELL_DELTA_HEADER_SIZE + CELL_DELTA_CHECKSUM_SIZE;

/// Maximum cell data size (same as max page size minus overhead).
pub const CELL_DELTA_MAX_DATA_SIZE: usize = 65536;

// ---------------------------------------------------------------------------
// CellOp — Operation type (§C4-WAL.1)
// ---------------------------------------------------------------------------

/// Cell operation type encoded in WAL frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CellOp {
    /// Insert a new cell.
    Insert = 1,
    /// Update an existing cell.
    Update = 2,
    /// Delete a cell.
    Delete = 3,
}

impl CellOp {
    /// Decode from byte, returning None for invalid values.
    #[must_use]
    pub const fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Insert),
            2 => Some(Self::Update),
            3 => Some(Self::Delete),
            _ => None,
        }
    }

    /// Encode as byte.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

// ---------------------------------------------------------------------------
// CellDeltaWalFrame — WAL frame for cell-level deltas (§C4-WAL.2)
// ---------------------------------------------------------------------------

/// A cell-delta WAL frame for crash recovery.
///
/// This is a lightweight alternative to full-page WAL frames for logical
/// row operations (INSERT, UPDATE, DELETE) that don't change page structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellDeltaWalFrame {
    /// Database page number containing this cell.
    pub page_number: PageNumber,
    /// Cell key digest (BLAKE3-based, 16 bytes) for stable identity.
    pub cell_key_digest: [u8; 16],
    /// Operation type.
    pub op: CellOp,
    /// Commit sequence number (0 = uncommitted).
    pub commit_seq: CommitSeq,
    /// Transaction ID that created this delta.
    pub txn_id: TxnId,
    /// Cell content (empty for Delete operations).
    pub cell_data: Vec<u8>,
}

impl CellDeltaWalFrame {
    /// Create a new cell-delta frame.
    #[must_use]
    pub fn new(
        page_number: PageNumber,
        cell_key_digest: [u8; 16],
        op: CellOp,
        commit_seq: CommitSeq,
        txn_id: TxnId,
        cell_data: Vec<u8>,
    ) -> Self {
        Self {
            page_number,
            cell_key_digest,
            op,
            commit_seq,
            txn_id,
            cell_data,
        }
    }

    /// Compute the serialized size of this frame.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        CELL_DELTA_HEADER_SIZE + self.cell_data.len() + CELL_DELTA_CHECKSUM_SIZE
    }

    /// Serialize this frame to bytes.
    ///
    /// Returns a `Vec` containing the complete frame with checksum.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        if self.cell_data.len() > CELL_DELTA_MAX_DATA_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta payload too large: {} bytes exceeds max {CELL_DELTA_MAX_DATA_SIZE}",
                    self.cell_data.len()
                ),
            });
        }
        if self.op == CellOp::Delete && !self.cell_data.is_empty() {
            return Err(FrankenError::WalCorrupt {
                detail: "delete cell-delta frame cannot carry cell data".to_owned(),
            });
        }

        let mut buf = Vec::with_capacity(self.serialized_size());

        // Frame marker + actual page number (4 bytes each).
        // Keep the first word as a pure type tag so page numbers can use the
        // full non-zero u32 range accepted by PageNumber.
        buf.extend_from_slice(&CELL_DELTA_FRAME_MARKER.to_be_bytes());
        buf.extend_from_slice(&self.page_number.get().to_be_bytes());

        // Cell key digest (16 bytes)
        buf.extend_from_slice(&self.cell_key_digest);

        // Op (1 byte)
        buf.push(self.op.as_byte());

        // Commit seq (8 bytes)
        buf.extend_from_slice(&self.commit_seq.get().to_be_bytes());

        // Txn ID (8 bytes)
        buf.extend_from_slice(&self.txn_id.get().to_be_bytes());

        // Cell data length (4 bytes). The payload-size guard above keeps this
        // conversion infallible without silent narrowing.
        let data_len =
            u32::try_from(self.cell_data.len()).map_err(|_| FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta payload length {} does not fit in u32",
                    self.cell_data.len()
                ),
            })?;
        buf.extend_from_slice(&data_len.to_be_bytes());

        // Cell data (variable)
        buf.extend_from_slice(&self.cell_data);

        // CRC32C checksum (4 bytes) over everything before checksum
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        debug!(
            frame_type = "cell_delta",
            pgno = self.page_number.get(),
            cell_key = ?&self.cell_key_digest[..4],
            op = ?self.op,
            commit_seq = self.commit_seq.get(),
            data_len = self.cell_data.len(),
            "wal_frame_written"
        );

        Ok(buf)
    }

    /// Deserialize a cell-delta frame from bytes.
    ///
    /// Returns an error if the frame is too short, has an invalid marker,
    /// or fails checksum verification.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < CELL_DELTA_MIN_FRAME_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta frame too short: {} bytes, need at least {}",
                    data.len(),
                    CELL_DELTA_MIN_FRAME_SIZE
                ),
            });
        }

        // Verify marker. New frames use an invalid page-number word as a pure
        // type tag, so full-page frames cannot collide with cell-delta frames.
        let marker_and_pgno = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if marker_and_pgno != CELL_DELTA_FRAME_MARKER {
            return Err(FrankenError::WalCorrupt {
                detail: format!("cell-delta frame has invalid marker word: {marker_and_pgno:#x}"),
            });
        }

        // Parse fixed header fields
        let actual_pgno = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let page_number = PageNumber::new(actual_pgno).ok_or_else(|| FrankenError::WalCorrupt {
            detail: "cell-delta frame has invalid page number 0".to_owned(),
        })?;
        let mut cell_key_digest = [0u8; 16];
        cell_key_digest.copy_from_slice(&data[8..24]);

        let op = CellOp::from_byte(data[24]).ok_or_else(|| FrankenError::WalCorrupt {
            detail: format!("cell-delta frame has invalid op byte: {}", data[24]),
        })?;

        let commit_seq = CommitSeq::new(u64::from_be_bytes([
            data[25], data[26], data[27], data[28], data[29], data[30], data[31], data[32],
        ]));

        let txn_id_raw = u64::from_be_bytes([
            data[33], data[34], data[35], data[36], data[37], data[38], data[39], data[40],
        ]);
        let txn_id = TxnId::new(txn_id_raw).ok_or_else(|| FrankenError::WalCorrupt {
            detail: format!("cell-delta frame has invalid txn_id: {}", txn_id_raw),
        })?;

        let cell_data_len = u32::from_be_bytes([data[41], data[42], data[43], data[44]]) as usize;
        if cell_data_len > CELL_DELTA_MAX_DATA_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta frame data too large: {} bytes, max {}",
                    cell_data_len, CELL_DELTA_MAX_DATA_SIZE
                ),
            });
        }
        if op == CellOp::Delete && cell_data_len != 0 {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta delete frame has non-empty payload: {} bytes",
                    cell_data_len
                ),
            });
        }

        // Validate total length
        let expected_len = CELL_DELTA_HEADER_SIZE
            .checked_add(cell_data_len)
            .and_then(|len| len.checked_add(CELL_DELTA_CHECKSUM_SIZE))
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "cell-delta frame length overflow".to_owned(),
            })?;
        if data.len() < expected_len {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta frame truncated: {} bytes, need {} (data_len={})",
                    data.len(),
                    expected_len,
                    cell_data_len
                ),
            });
        }
        if data.len() > expected_len {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta frame has {} trailing bytes after checksum",
                    data.len() - expected_len
                ),
            });
        }

        // Extract cell data
        let cell_data =
            data[CELL_DELTA_HEADER_SIZE..CELL_DELTA_HEADER_SIZE + cell_data_len].to_vec();

        // Verify checksum
        let checksum_offset = CELL_DELTA_HEADER_SIZE + cell_data_len;
        let stored_checksum = u32::from_be_bytes([
            data[checksum_offset],
            data[checksum_offset + 1],
            data[checksum_offset + 2],
            data[checksum_offset + 3],
        ]);
        let computed_checksum = crc32c::crc32c(&data[..checksum_offset]);

        if stored_checksum != computed_checksum {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta frame checksum mismatch: stored {:08x}, computed {:08x}",
                    stored_checksum, computed_checksum
                ),
            });
        }

        Ok(Self {
            page_number,
            cell_key_digest,
            op,
            commit_seq,
            txn_id,
            cell_data,
        })
    }
}

// ---------------------------------------------------------------------------
// Frame Type Detection (§C4-WAL.3)
// ---------------------------------------------------------------------------

/// Check if a WAL frame is a structurally valid cell-delta frame.
///
/// Full-page WAL frames also start with a page number. The marker word is page
/// 0, which is invalid, and the remaining envelope is still verified before a
/// frame is accepted.
#[must_use]
pub fn is_cell_delta_frame(frame_data: &[u8]) -> bool {
    if frame_data.len() < CELL_DELTA_MIN_FRAME_SIZE {
        return false;
    }
    let marker_and_pgno =
        u32::from_be_bytes([frame_data[0], frame_data[1], frame_data[2], frame_data[3]]);
    if marker_and_pgno != CELL_DELTA_FRAME_MARKER {
        return false;
    }
    let actual_pgno =
        u32::from_be_bytes([frame_data[4], frame_data[5], frame_data[6], frame_data[7]]);
    if PageNumber::new(actual_pgno).is_none() {
        return false;
    }
    let Some(op) = CellOp::from_byte(frame_data[24]) else {
        return false;
    };
    let txn_id_raw = u64::from_be_bytes([
        frame_data[33],
        frame_data[34],
        frame_data[35],
        frame_data[36],
        frame_data[37],
        frame_data[38],
        frame_data[39],
        frame_data[40],
    ]);
    if TxnId::new(txn_id_raw).is_none() {
        return false;
    }
    let cell_data_len = u32::from_be_bytes([
        frame_data[41],
        frame_data[42],
        frame_data[43],
        frame_data[44],
    ]) as usize;
    if cell_data_len > CELL_DELTA_MAX_DATA_SIZE || (op == CellOp::Delete && cell_data_len != 0) {
        return false;
    }
    let Some(expected_len) = CELL_DELTA_HEADER_SIZE
        .checked_add(cell_data_len)
        .and_then(|len| len.checked_add(CELL_DELTA_CHECKSUM_SIZE))
    else {
        return false;
    };
    if frame_data.len() != expected_len {
        return false;
    }

    let checksum_offset = CELL_DELTA_HEADER_SIZE + cell_data_len;
    let stored_checksum = u32::from_be_bytes([
        frame_data[checksum_offset],
        frame_data[checksum_offset + 1],
        frame_data[checksum_offset + 2],
        frame_data[checksum_offset + 3],
    ]);
    stored_checksum == crc32c::crc32c(&frame_data[..checksum_offset])
}

/// Extract the legacy embedded page number from a cell-delta frame marker.
///
/// New frames use an invalid page-number word as a pure type marker and store
/// the real page number in the second word, so this returns `None` for the
/// current encoding.
#[must_use]
pub fn extract_page_number_from_marker(marker_and_pgno: u32) -> Option<PageNumber> {
    if marker_and_pgno & LEGACY_CELL_DELTA_FRAME_MARKER == 0 {
        return None; // Not a cell-delta frame
    }
    let embedded_page = marker_and_pgno & !LEGACY_CELL_DELTA_FRAME_MARKER;
    if embedded_page == 0 {
        return None;
    }
    PageNumber::new(embedded_page)
}

// ---------------------------------------------------------------------------
// Recovery Helpers (§C4-WAL.4)
// ---------------------------------------------------------------------------

/// Summary statistics from WAL recovery.
#[derive(Debug, Clone, Default)]
pub struct WalRecoverySummary {
    /// Number of full-page frames processed.
    pub full_page_frames: u64,
    /// Number of cell-delta frames processed.
    pub cell_delta_frames: u64,
    /// Number of cell-delta frames skipped (uncommitted).
    pub cell_delta_uncommitted: u64,
    /// Number of checksum errors encountered.
    pub checksum_errors: u64,
    /// Total bytes of cell data recovered.
    pub cell_data_bytes: u64,
}

impl WalRecoverySummary {
    /// Log summary statistics.
    pub fn log_summary(&self) {
        tracing::info!(
            full_page_frames = self.full_page_frames,
            cell_delta_frames = self.cell_delta_frames,
            cell_delta_uncommitted = self.cell_delta_uncommitted,
            checksum_errors = self.checksum_errors,
            cell_data_bytes = self.cell_data_bytes,
            "wal_recovery_summary"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests (§C4-WAL.5)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_page_number() -> PageNumber {
        PageNumber::new(42).unwrap()
    }

    fn test_cell_key_digest() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    fn test_txn_id(id: u64) -> TxnId {
        TxnId::new(id).unwrap()
    }

    fn high_bit_page_number() -> PageNumber {
        PageNumber::new(0x8000_0042).unwrap()
    }

    #[test]
    fn test_cell_delta_frame_round_trip() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3, 4, 5],
        );

        let serialized = frame.serialize().unwrap();
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();

        assert_eq!(frame, deserialized);
    }

    #[test]
    fn test_cell_delta_frame_checksum() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Update,
            CommitSeq::new(200),
            test_txn_id(99),
            vec![10, 20, 30],
        );

        let mut serialized = frame.serialize().unwrap();

        // Corrupt one byte in the middle
        serialized[20] ^= 0xFF;

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[test]
    fn test_cell_delta_frame_variable_length() {
        // Test empty cell data (Delete)
        let frame_empty = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Delete,
            CommitSeq::new(50),
            test_txn_id(1),
            vec![],
        );
        let serialized = frame_empty.serialize().unwrap();
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
        assert_eq!(frame_empty, deserialized);
        assert!(deserialized.cell_data.is_empty());

        // Test 100 bytes
        let frame_100 = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(51),
            test_txn_id(2),
            vec![0xAB; 100],
        );
        let serialized = frame_100.serialize().unwrap();
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
        assert_eq!(frame_100, deserialized);
        assert_eq!(deserialized.cell_data.len(), 100);

        // Test 4000 bytes
        let frame_4000 = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Update,
            CommitSeq::new(52),
            test_txn_id(3),
            vec![0xCD; 4000],
        );
        let serialized = frame_4000.serialize().unwrap();
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
        assert_eq!(frame_4000, deserialized);
        assert_eq!(deserialized.cell_data.len(), 4000);
    }

    #[test]
    fn test_cell_delta_frame_marker_word() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );

        let serialized = frame.serialize().unwrap();

        // Verify exact cell-delta marker.
        assert!(is_cell_delta_frame(&serialized));

        // Verify first 4 bytes are the pure marker word.
        let marker_and_pgno =
            u32::from_be_bytes([serialized[0], serialized[1], serialized[2], serialized[3]]);
        assert_eq!(marker_and_pgno, CELL_DELTA_FRAME_MARKER);

        // New frames use a pure marker word; there is no embedded page number.
        assert_eq!(extract_page_number_from_marker(marker_and_pgno), None);

        // Legacy experimental markers may still embed the page number in the
        // lower 31 bits.
        let legacy_marker = LEGACY_CELL_DELTA_FRAME_MARKER | test_page_number().get();
        assert_eq!(
            extract_page_number_from_marker(legacy_marker),
            Some(test_page_number())
        );
    }

    #[test]
    fn test_cell_op_encoding() {
        assert_eq!(CellOp::from_byte(1), Some(CellOp::Insert));
        assert_eq!(CellOp::from_byte(2), Some(CellOp::Update));
        assert_eq!(CellOp::from_byte(3), Some(CellOp::Delete));
        assert_eq!(CellOp::from_byte(0), None);
        assert_eq!(CellOp::from_byte(4), None);

        assert_eq!(CellOp::Insert.as_byte(), 1);
        assert_eq!(CellOp::Update.as_byte(), 2);
        assert_eq!(CellOp::Delete.as_byte(), 3);
    }

    #[test]
    fn test_is_cell_delta_frame_detection() {
        // Cell-delta frame
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let serialized = frame.serialize().unwrap();
        assert!(is_cell_delta_frame(&serialized));

        let mut invalid_txn_id = serialized.clone();
        invalid_txn_id[33..41].copy_from_slice(&0u64.to_be_bytes());
        let checksum_offset = invalid_txn_id.len() - CELL_DELTA_CHECKSUM_SIZE;
        let checksum = crc32c::crc32c(&invalid_txn_id[..checksum_offset]);
        invalid_txn_id[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
        assert!(
            !is_cell_delta_frame(&invalid_txn_id),
            "frame detector should reject envelopes with invalid transaction ids"
        );
        assert!(CellDeltaWalFrame::deserialize(&invalid_txn_id).is_err());

        // Too short to be a structurally valid cell-delta frame.
        let fake_page_frame = [0x00, 0x00, 0x00, 0x2A];
        assert!(!is_cell_delta_frame(&fake_page_frame));

        // High-bit page numbers are valid full-page frame page numbers. The
        // discriminator must not classify them as cell-delta frames just
        // because they share the legacy marker bit.
        let high_bit_full_page_frame = 0x8000_0042_u32.to_be_bytes();
        assert!(!is_cell_delta_frame(&high_bit_full_page_frame));

        let mut exact_marker_full_page_frame = vec![0u8; 24 + 4096];
        exact_marker_full_page_frame[..4].copy_from_slice(&CELL_DELTA_FRAME_MARKER.to_be_bytes());
        assert!(
            !is_cell_delta_frame(&exact_marker_full_page_frame),
            "page-zero marker must not classify a malformed envelope as cell-delta"
        );

        // Too short
        assert!(!is_cell_delta_frame(&[0x80]));
        assert!(!is_cell_delta_frame(&[]));
    }

    #[test]
    fn test_serialized_size() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![0xAB; 50],
        );

        // Header (45) + data (50) + checksum (4) = 99
        assert_eq!(frame.serialized_size(), 99);
        assert_eq!(frame.serialize().unwrap().len(), 99);
    }

    #[test]
    fn test_deserialize_truncated() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3, 4, 5],
        );

        let serialized = frame.serialize().unwrap();

        // Truncate to various lengths
        assert!(CellDeltaWalFrame::deserialize(&serialized[..10]).is_err());
        assert!(
            CellDeltaWalFrame::deserialize(&serialized[..CELL_DELTA_MIN_FRAME_SIZE - 1]).is_err()
        );

        // Truncate cell data
        let truncated = &serialized[..serialized.len() - 3];
        let result = CellDeltaWalFrame::deserialize(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_rejects_trailing_bytes() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );

        let mut serialized = frame.serialize().unwrap();
        serialized.extend_from_slice(b"junk");

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("trailing bytes"),
            "decoder should reject bytes not covered by the frame checksum"
        );
    }

    #[test]
    fn test_deserialize_rejects_oversized_cell_data_len() {
        let mut serialized = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            Vec::new(),
        )
        .serialize()
        .unwrap();
        let too_large = u32::try_from(CELL_DELTA_MAX_DATA_SIZE + 1)
            .expect("test max cell delta size should fit u32");
        serialized[41..45].copy_from_slice(&too_large.to_be_bytes());

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("data too large"),
            "decoder should reject impossible allocation sizes before checksum work"
        );
    }

    #[test]
    fn test_deserialize_rejects_delete_payload() {
        let mut serialized = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Update,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        )
        .serialize()
        .unwrap();
        serialized[24] = CellOp::Delete.as_byte();

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("delete frame has non-empty payload"),
            "decoder should reject delete frames with unreachable payload bytes"
        );
    }

    #[test]
    fn test_serialize_rejects_oversized_cell_data() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![0; CELL_DELTA_MAX_DATA_SIZE + 1],
        );

        let err = frame
            .serialize()
            .expect_err("serializer should reject payloads larger than the frame limit");
        assert!(
            err.to_string().contains("payload too large"),
            "unexpected serializer error: {err}"
        );
    }

    #[test]
    fn test_serialize_rejects_delete_payload() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Delete,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1],
        );

        let err = frame
            .serialize()
            .expect_err("serializer should reject DELETE frames with a payload");
        assert!(
            err.to_string().contains("cannot carry cell data"),
            "unexpected serializer error: {err}"
        );
    }

    #[test]
    fn test_recovery_summary() {
        let summary = WalRecoverySummary {
            full_page_frames: 100,
            cell_delta_frames: 500,
            cell_delta_uncommitted: 10,
            cell_data_bytes: 50_000,
            ..Default::default()
        };

        // Just verify it compiles and logs without panic
        summary.log_summary();
    }

    #[test]
    fn test_all_ops_round_trip() {
        for op in [CellOp::Insert, CellOp::Update, CellOp::Delete] {
            let cell_data = if op == CellOp::Delete {
                vec![]
            } else {
                vec![1, 2, 3, 4, 5]
            };

            let frame = CellDeltaWalFrame::new(
                test_page_number(),
                test_cell_key_digest(),
                op,
                CommitSeq::new(100),
                test_txn_id(42),
                cell_data,
            );

            let serialized = frame.serialize().unwrap();
            let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
            assert_eq!(frame, deserialized);
        }
    }

    #[test]
    fn test_high_bit_page_number_round_trips() {
        let frame = CellDeltaWalFrame::new(
            high_bit_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );

        let serialized = frame.serialize().unwrap();
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.page_number, high_bit_page_number());
    }

    #[test]
    fn test_extract_page_number_legacy_marker_with_embedded_zero() {
        assert_eq!(
            extract_page_number_from_marker(LEGACY_CELL_DELTA_FRAME_MARKER),
            None
        );
    }

    #[test]
    fn test_extract_page_number_pure_zero_marker() {
        assert_eq!(extract_page_number_from_marker(0), None);
    }

    #[test]
    fn test_extract_page_number_non_legacy_nonzero() {
        assert_eq!(extract_page_number_from_marker(42), None);
        assert_eq!(extract_page_number_from_marker(0x7FFF_FFFF), None);
    }

    #[test]
    fn test_deserialize_rejects_page_number_zero() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let mut serialized = frame.serialize().unwrap();
        serialized[4..8].copy_from_slice(&0u32.to_be_bytes());
        let checksum_offset = serialized.len() - CELL_DELTA_CHECKSUM_SIZE;
        let checksum = crc32c::crc32c(&serialized[..checksum_offset]);
        serialized[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid page number 0")
        );
    }

    #[test]
    fn test_deserialize_rejects_invalid_op_byte() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let mut serialized = frame.serialize().unwrap();
        serialized[24] = 0;
        let checksum_offset = serialized.len() - CELL_DELTA_CHECKSUM_SIZE;
        let checksum = crc32c::crc32c(&serialized[..checksum_offset]);
        serialized[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid op byte"));
    }

    #[test]
    fn test_deserialize_rejects_txn_id_zero() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let mut serialized = frame.serialize().unwrap();
        serialized[33..41].copy_from_slice(&0u64.to_be_bytes());
        let checksum_offset = serialized.len() - CELL_DELTA_CHECKSUM_SIZE;
        let checksum = crc32c::crc32c(&serialized[..checksum_offset]);
        serialized[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid txn_id"));
    }

    #[test]
    fn test_is_cell_delta_frame_rejects_bad_op_in_valid_envelope() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let mut serialized = frame.serialize().unwrap();
        serialized[24] = 4;
        let checksum_offset = serialized.len() - CELL_DELTA_CHECKSUM_SIZE;
        let checksum = crc32c::crc32c(&serialized[..checksum_offset]);
        serialized[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        assert!(!is_cell_delta_frame(&serialized));
    }

    #[test]
    fn test_is_cell_delta_frame_rejects_page_zero_in_envelope() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let mut serialized = frame.serialize().unwrap();
        serialized[4..8].copy_from_slice(&0u32.to_be_bytes());
        let checksum_offset = serialized.len() - CELL_DELTA_CHECKSUM_SIZE;
        let checksum = crc32c::crc32c(&serialized[..checksum_offset]);
        serialized[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());

        assert!(!is_cell_delta_frame(&serialized));
    }

    #[test]
    fn test_constant_values() {
        assert_eq!(CELL_DELTA_FRAME_MARKER, 0);
        assert_eq!(CELL_DELTA_HEADER_SIZE, 45);
        assert_eq!(CELL_DELTA_CHECKSUM_SIZE, 4);
        assert_eq!(CELL_DELTA_MIN_FRAME_SIZE, 49);
        assert_eq!(CELL_DELTA_MAX_DATA_SIZE, 65536);
    }

    #[test]
    fn test_recovery_summary_default_all_zero() {
        let summary = WalRecoverySummary::default();
        assert_eq!(summary.full_page_frames, 0);
        assert_eq!(summary.cell_delta_frames, 0);
        assert_eq!(summary.cell_delta_uncommitted, 0);
        assert_eq!(summary.checksum_errors, 0);
        assert_eq!(summary.cell_data_bytes, 0);
    }

    #[test]
    fn test_uncommitted_frame_round_trip() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(0),
            test_txn_id(7),
            vec![0xDE, 0xAD],
        );

        let serialized = frame.serialize().unwrap();
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.commit_seq.get(), 0);
        assert_eq!(frame, deserialized);
    }

    #[test]
    fn test_deserialize_rejects_legacy_marker_word() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );

        let mut serialized = frame.serialize().unwrap();
        let legacy_marker = LEGACY_CELL_DELTA_FRAME_MARKER | test_page_number().get();
        serialized[..4].copy_from_slice(&legacy_marker.to_be_bytes());

        let result = CellDeltaWalFrame::deserialize(&serialized);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid marker word")
        );
    }

    #[test]
    fn test_serialized_size_empty_delete_frame() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Delete,
            CommitSeq::new(1),
            test_txn_id(1),
            vec![],
        );
        assert_eq!(frame.serialized_size(), CELL_DELTA_MIN_FRAME_SIZE);
        assert_eq!(frame.serialize().unwrap().len(), CELL_DELTA_MIN_FRAME_SIZE);
    }

    #[test]
    fn test_cell_delta_wal_frame_clone_preserves_equality() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Update,
            CommitSeq::new(77),
            test_txn_id(33),
            vec![0xAA; 200],
        );
        let cloned = frame.clone();
        assert_eq!(frame, cloned);
        assert_eq!(cloned.cell_data.len(), 200);

        let dbg = format!("{frame:?}");
        assert!(dbg.contains("CellDeltaWalFrame"));
    }

    #[test]
    fn test_is_cell_delta_frame_rejects_corrupted_checksum() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(100),
            test_txn_id(42),
            vec![1, 2, 3],
        );
        let mut serialized = frame.serialize().unwrap();
        let last = serialized.len() - 1;
        serialized[last] ^= 0xFF;
        assert!(!is_cell_delta_frame(&serialized));
    }

    #[test]
    fn test_serialize_exact_max_data_size_succeeds() {
        let frame = CellDeltaWalFrame::new(
            test_page_number(),
            test_cell_key_digest(),
            CellOp::Insert,
            CommitSeq::new(1),
            test_txn_id(1),
            vec![0u8; CELL_DELTA_MAX_DATA_SIZE],
        );
        let serialized = frame.serialize().unwrap();
        let expected = CELL_DELTA_HEADER_SIZE + CELL_DELTA_MAX_DATA_SIZE + CELL_DELTA_CHECKSUM_SIZE;
        assert_eq!(serialized.len(), expected);
        let deserialized = CellDeltaWalFrame::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.cell_data.len(), CELL_DELTA_MAX_DATA_SIZE);
    }

    #[test]
    fn cell_op_debug_clone_copy_eq() {
        let ops = [CellOp::Insert, CellOp::Update, CellOp::Delete];
        for op in &ops {
            let copied = *op;
            assert_eq!(copied, *op);
        }
        assert_ne!(CellOp::Insert, CellOp::Delete);
        let dbg = format!("{:?}", CellOp::Update);
        assert!(dbg.contains("Update"));
    }

    #[test]
    fn wal_recovery_summary_debug_and_clone() {
        let s = WalRecoverySummary {
            full_page_frames: 10,
            cell_delta_frames: 5,
            checksum_errors: 1,
            ..WalRecoverySummary::default()
        };
        let dbg = format!("{s:?}");
        assert!(dbg.contains("WalRecoverySummary"));
        let cloned = s.clone();
        assert_eq!(cloned.full_page_frames, 10);
        assert_eq!(cloned.cell_delta_frames, 5);
        assert_eq!(cloned.checksum_errors, 1);
    }

    #[test]
    fn cell_op_from_byte_rejects_zero_and_max() {
        assert!(CellOp::from_byte(0).is_none());
        assert!(CellOp::from_byte(4).is_none());
        assert!(CellOp::from_byte(u8::MAX).is_none());
        assert_eq!(CellOp::from_byte(1), Some(CellOp::Insert));
        assert_eq!(CellOp::from_byte(3), Some(CellOp::Delete));
    }

    #[test]
    fn wal_recovery_summary_log_does_not_panic() {
        let s = WalRecoverySummary {
            full_page_frames: 100,
            cell_data_bytes: 4096,
            ..WalRecoverySummary::default()
        };
        s.log_summary();
    }
}
