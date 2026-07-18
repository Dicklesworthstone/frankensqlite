//! Cell-Delta WAL Frame Format (C4-WAL: bd-l9k8e.10)
//!
//! This module defines the WAL record format for cell-level MVCC deltas, enabling
//! crash recovery of committed cell-level changes without writing full 4KB page images.
//!
//! # Design Rationale
//!
//! Cell-level deltas live in memory ([`crate::cell_visibility::CellVisibilityLog`]).
//! When a transaction commits, we need durability without the cost of full page images.
//! Cell-delta WAL frames are ~28-30x smaller than full-page frames for typical rows.
//!
//! # Frame Format
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!   0       4   Frame marker word (0 = cell-delta, invalid as a page number)
//!   4       4   Page number (big-endian)
//!   8      16   Cell key digest (BLAKE3 truncated)
//!  24       1   Cell operation (1=Insert, 2=Update, 3=Delete)
//!  25       8   Commit sequence (big-endian)
//!  33       8   Transaction ID (big-endian)
//!  41       4   Cell data length (big-endian, 0 for Delete)
//!  45       N   Cell data bytes
//!  45+N     4   CRC32C checksum of bytes 0..(45+N)
//! ```
//!
//! Total overhead: 49 bytes fixed + cell_data
//! Typical 100-byte INSERT: ~149 bytes vs 4096 bytes full-page = ~27x smaller
//!
//! # Frame Type Discrimination
//!
//! Regular full-page frames start with page_number (4 bytes, always >= 1 for
//! valid pages). Cell-delta frames start with marker word 0, which is outside
//! the valid page-number domain. The remaining envelope is still validated
//! before accepting the frame as cell-delta data.
//!
//! # Recovery
//!
//! During WAL recovery:
//! 1. Read frame marker word
//! 2. Full-page frames: apply directly to page cache (existing path)
//! 3. Cell-delta frames: insert into [`crate::CellVisibilityLog`], then materialize affected pages
//!
//! # Checkpoint Integration
//!
//! At checkpoint:
//! 1. Materialize all pages with outstanding cell deltas
//! 2. Write full page images to main DB file
//! 3. Truncate WAL (clears both frame types)
//! 4. Clear [`crate::CellVisibilityLog`] for checkpointed pages

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{CommitSeq, PageNumber, TxnId};
use tracing::{debug, trace, warn};

use crate::cell_visibility::{CellDeltaKind, CellKey};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Marker word for cell-delta frames.
///
/// Page number 0 is invalid in SQLite, so this marker remains disjoint from
/// ordinary full-page WAL frames while still allowing high-bit page numbers.
pub const CELL_DELTA_FRAME_MARKER: u32 = 0;

/// Fixed header size before variable-length cell data.
pub const CELL_DELTA_HEADER_SIZE: usize = 45;

/// CRC32C checksum size.
pub const CELL_DELTA_CHECKSUM_SIZE: usize = 4;

/// Minimum frame size (header + checksum, no data).
pub const CELL_DELTA_MIN_FRAME_SIZE: usize = CELL_DELTA_HEADER_SIZE + CELL_DELTA_CHECKSUM_SIZE;

/// Maximum cell data length.
pub const CELL_DELTA_MAX_DATA_LEN: u32 = 65_536;

// ---------------------------------------------------------------------------
// CellDeltaOp — Wire format for cell operation kind
// ---------------------------------------------------------------------------

/// Cell operation encoded as a single byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CellDeltaOp {
    Insert = 1,
    Update = 2,
    Delete = 3,
}

impl CellDeltaOp {
    /// Convert from wire byte.
    #[must_use]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Insert),
            2 => Some(Self::Update),
            3 => Some(Self::Delete),
            _ => None,
        }
    }

    /// Convert from [`CellDeltaKind`].
    #[must_use]
    pub fn from_kind(kind: &CellDeltaKind) -> Self {
        match kind {
            CellDeltaKind::Insert => Self::Insert,
            CellDeltaKind::Update => Self::Update,
            CellDeltaKind::Delete => Self::Delete,
        }
    }

    /// Convert to [`CellDeltaKind`].
    #[must_use]
    pub fn to_kind(self) -> CellDeltaKind {
        match self {
            Self::Insert => CellDeltaKind::Insert,
            Self::Update => CellDeltaKind::Update,
            Self::Delete => CellDeltaKind::Delete,
        }
    }
}

// ---------------------------------------------------------------------------
// CellDeltaWalFrame — The cell-delta WAL record
// ---------------------------------------------------------------------------

/// A cell-delta WAL frame for crash recovery.
///
/// This is the lightweight alternative to full-page WAL frames for logical
/// row operations (INSERT/UPDATE/DELETE that don't trigger structural changes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellDeltaWalFrame {
    /// Page number containing this cell.
    pub page_number: PageNumber,
    /// BLAKE3-truncated digest of the cell key.
    pub key_digest: [u8; 16],
    /// What operation was performed.
    pub op: CellDeltaOp,
    /// Commit sequence when this delta became visible.
    pub commit_seq: CommitSeq,
    /// Transaction that created this delta.
    pub txn_id: TxnId,
    /// Cell data bytes (empty for Delete).
    pub cell_data: Vec<u8>,
}

impl CellDeltaWalFrame {
    /// Create a new cell-delta WAL frame.
    #[must_use]
    pub fn new(
        page_number: PageNumber,
        cell_key: &CellKey,
        op: CellDeltaOp,
        commit_seq: CommitSeq,
        txn_id: TxnId,
        cell_data: Vec<u8>,
    ) -> Self {
        Self {
            page_number,
            key_digest: cell_key.key_digest,
            op,
            commit_seq,
            txn_id,
            cell_data,
        }
    }

    /// Total serialized size of this frame.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        CELL_DELTA_HEADER_SIZE + self.cell_data.len() + CELL_DELTA_CHECKSUM_SIZE
    }

    /// Serialize this frame to bytes.
    ///
    /// Returns the complete frame including CRC32C checksum.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        if self.cell_data.len() > CELL_DELTA_MAX_DATA_LEN as usize {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta payload too large: {} bytes exceeds max {CELL_DELTA_MAX_DATA_LEN}",
                    self.cell_data.len()
                ),
            });
        }
        if matches!(self.op, CellDeltaOp::Delete) && !self.cell_data.is_empty() {
            return Err(FrankenError::WalCorrupt {
                detail: "delete cell-delta frame cannot carry cell data".to_owned(),
            });
        }

        let total_size = self.serialized_size();
        let mut buf = Vec::with_capacity(total_size);

        // Marker word + page number (4 bytes each, big-endian).
        buf.extend_from_slice(&CELL_DELTA_FRAME_MARKER.to_be_bytes());
        buf.extend_from_slice(&self.page_number.get().to_be_bytes());

        // Key digest (16 bytes)
        buf.extend_from_slice(&self.key_digest);

        // Operation (1 byte)
        buf.push(self.op as u8);

        // Commit sequence (8 bytes, big-endian)
        buf.extend_from_slice(&self.commit_seq.get().to_be_bytes());

        // Transaction ID (8 bytes, big-endian)
        buf.extend_from_slice(&self.txn_id.get().to_be_bytes());

        // Cell data length (4 bytes, big-endian)
        let data_len =
            u32::try_from(self.cell_data.len()).map_err(|_| FrankenError::WalCorrupt {
                detail: format!(
                    "cell-delta payload length {} does not fit in u32",
                    self.cell_data.len()
                ),
            })?;
        buf.extend_from_slice(&data_len.to_be_bytes());

        // Cell data
        buf.extend_from_slice(&self.cell_data);

        // CRC32C checksum of everything before the checksum
        let checksum = crc32c_checksum(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        trace!(
            pgno = self.page_number.get(),
            op = ?self.op,
            commit_seq = self.commit_seq.get(),
            data_len = self.cell_data.len(),
            frame_size = buf.len(),
            "cell_delta_wal_frame_serialized"
        );

        Ok(buf)
    }

    /// Deserialize a cell-delta frame from bytes.
    ///
    /// Returns `None` if:
    /// - Frame is too short
    /// - Frame marker word doesn't match
    /// - CRC32C checksum fails
    /// - Cell data length exceeds maximum
    #[must_use]
    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < CELL_DELTA_MIN_FRAME_SIZE {
            warn!(
                buf_len = buf.len(),
                min_size = CELL_DELTA_MIN_FRAME_SIZE,
                "cell_delta_wal_frame_too_short"
            );
            return None;
        }

        // Check frame marker. Page number 0 is invalid for ordinary full-page
        // frames, so this remains disjoint from the page-number domain.
        let marker = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if marker != CELL_DELTA_FRAME_MARKER {
            return None; // Not a cell-delta frame
        }

        // Read header fields
        let page_number = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let page_number = PageNumber::new(page_number)?;

        let mut key_digest = [0u8; 16];
        key_digest.copy_from_slice(&buf[8..24]);

        let op = CellDeltaOp::from_byte(buf[24])?;

        let commit_seq = u64::from_be_bytes([
            buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31], buf[32],
        ]);
        let commit_seq = CommitSeq::new(commit_seq);

        let txn_id = u64::from_be_bytes([
            buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39], buf[40],
        ]);
        let txn_id = TxnId::new(txn_id)?;

        let data_len = u32::from_be_bytes([buf[41], buf[42], buf[43], buf[44]]);

        // Validate data length
        if data_len > CELL_DELTA_MAX_DATA_LEN {
            warn!(
                data_len,
                max = CELL_DELTA_MAX_DATA_LEN,
                "cell_delta_wal_frame_data_too_large"
            );
            return None;
        }
        if matches!(op, CellDeltaOp::Delete) && data_len != 0 {
            warn!(data_len, "cell_delta_wal_frame_delete_payload");
            return None;
        }

        let expected_total_size =
            CELL_DELTA_HEADER_SIZE + data_len as usize + CELL_DELTA_CHECKSUM_SIZE;
        if buf.len() < expected_total_size {
            warn!(
                buf_len = buf.len(),
                expected_size = expected_total_size,
                "cell_delta_wal_frame_truncated"
            );
            return None;
        }
        if buf.len() > expected_total_size {
            warn!(
                buf_len = buf.len(),
                expected_size = expected_total_size,
                trailing_bytes = buf.len() - expected_total_size,
                "cell_delta_wal_frame_trailing_bytes"
            );
            return None;
        }

        // Extract cell data
        let data_start = CELL_DELTA_HEADER_SIZE;
        let data_end = data_start + data_len as usize;
        let cell_data = buf[data_start..data_end].to_vec();

        // Verify checksum
        let checksum_start = data_end;
        let stored_checksum = u32::from_be_bytes([
            buf[checksum_start],
            buf[checksum_start + 1],
            buf[checksum_start + 2],
            buf[checksum_start + 3],
        ]);
        let computed_checksum = crc32c_checksum(&buf[..checksum_start]);

        if stored_checksum != computed_checksum {
            warn!(
                stored = stored_checksum,
                computed = computed_checksum,
                "cell_delta_wal_frame_checksum_mismatch"
            );
            return None;
        }

        trace!(
            pgno = page_number.get(),
            op = ?op,
            commit_seq = commit_seq.get(),
            data_len,
            "cell_delta_wal_frame_deserialized"
        );

        Some(Self {
            page_number,
            key_digest,
            op,
            commit_seq,
            txn_id,
            cell_data,
        })
    }

    /// Check if a buffer is a structurally valid cell-delta frame.
    #[inline]
    #[must_use]
    pub fn is_cell_delta_frame(buf: &[u8]) -> bool {
        if buf.len() < CELL_DELTA_MIN_FRAME_SIZE {
            return false;
        }
        let marker = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if marker != CELL_DELTA_FRAME_MARKER {
            return false;
        }
        if PageNumber::new(u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]])).is_none() {
            return false;
        }
        let Some(op) = CellDeltaOp::from_byte(buf[24]) else {
            return false;
        };
        let txn_id = u64::from_be_bytes([
            buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39], buf[40],
        ]);
        if TxnId::new(txn_id).is_none() {
            return false;
        }
        let data_len = u32::from_be_bytes([buf[41], buf[42], buf[43], buf[44]]);
        if data_len > CELL_DELTA_MAX_DATA_LEN
            || (matches!(op, CellDeltaOp::Delete) && data_len != 0)
        {
            return false;
        }
        let Some(expected_total_size) = CELL_DELTA_HEADER_SIZE
            .checked_add(data_len as usize)
            .and_then(|len| len.checked_add(CELL_DELTA_CHECKSUM_SIZE))
        else {
            return false;
        };
        if buf.len() != expected_total_size {
            return false;
        }
        let checksum_start = CELL_DELTA_HEADER_SIZE + data_len as usize;
        let stored_checksum = u32::from_be_bytes([
            buf[checksum_start],
            buf[checksum_start + 1],
            buf[checksum_start + 2],
            buf[checksum_start + 3],
        ]);
        stored_checksum == crc32c_checksum(&buf[..checksum_start])
    }
}

// ---------------------------------------------------------------------------
// CRC32C Checksum
// ---------------------------------------------------------------------------

/// Compute CRC32C checksum of data using the crc32c crate.
#[inline]
fn crc32c_checksum(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

// ---------------------------------------------------------------------------
// Batch Serialization
// ---------------------------------------------------------------------------

/// Serialize multiple cell-delta frames into a single buffer.
///
/// This is used when a transaction commits multiple cell changes atomically.
/// Each frame is written sequentially with its own checksum.
pub fn serialize_cell_delta_batch(frames: &[CellDeltaWalFrame]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    for frame in frames {
        buf.extend_from_slice(&frame.serialize()?);
    }

    debug!(
        frame_count = frames.len(),
        total_bytes = buf.len(),
        "cell_delta_batch_serialized"
    );

    Ok(buf)
}

/// Deserialize cell-delta frames from a buffer.
///
/// Reads frames sequentially until the buffer is exhausted or a non-cell-delta
/// frame is encountered.
#[must_use]
pub fn deserialize_cell_delta_batch(buf: &[u8]) -> Vec<CellDeltaWalFrame> {
    let mut frames = Vec::new();
    let mut offset = 0;

    while offset < buf.len() {
        let remaining = &buf[offset..];

        // Need at least header to read data length
        if remaining.len() < CELL_DELTA_HEADER_SIZE {
            break;
        }
        let marker = u32::from_be_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]);
        if marker != CELL_DELTA_FRAME_MARKER {
            break;
        }
        if PageNumber::new(u32::from_be_bytes([
            remaining[4],
            remaining[5],
            remaining[6],
            remaining[7],
        ]))
        .is_none()
        {
            break;
        }
        let Some(op) = CellDeltaOp::from_byte(remaining[24]) else {
            break;
        };
        let txn_id = u64::from_be_bytes([
            remaining[33],
            remaining[34],
            remaining[35],
            remaining[36],
            remaining[37],
            remaining[38],
            remaining[39],
            remaining[40],
        ]);
        if TxnId::new(txn_id).is_none() {
            break;
        }

        // Read data length to determine frame size
        let data_len =
            u32::from_be_bytes([remaining[41], remaining[42], remaining[43], remaining[44]]);
        if data_len > CELL_DELTA_MAX_DATA_LEN
            || (matches!(op, CellDeltaOp::Delete) && data_len != 0)
        {
            break;
        }

        let Some(frame_size) = CELL_DELTA_HEADER_SIZE
            .checked_add(data_len as usize)
            .and_then(|len| len.checked_add(CELL_DELTA_CHECKSUM_SIZE))
        else {
            break;
        };

        if remaining.len() < frame_size {
            break;
        }

        // Try to deserialize
        if let Some(frame) = CellDeltaWalFrame::deserialize(&remaining[..frame_size]) {
            frames.push(frame);
            offset += frame_size;
        } else {
            break;
        }
    }

    debug!(
        frame_count = frames.len(),
        bytes_consumed = offset,
        "cell_delta_batch_deserialized"
    );

    frames
}

// ---------------------------------------------------------------------------
// Recovery Summary
// ---------------------------------------------------------------------------

/// Summary statistics from WAL recovery.
#[derive(Debug, Clone, Default)]
pub struct CellDeltaRecoverySummary {
    /// Number of cell-delta frames recovered.
    pub cell_delta_frames: u64,
    /// Number of full-page frames recovered.
    pub full_page_frames: u64,
    /// Total bytes in cell-delta frames.
    pub cell_delta_bytes: u64,
    /// Number of unique pages with cell deltas.
    pub pages_with_cell_deltas: u64,
    /// Number of cell deltas inserted into the visibility log.
    pub deltas_inserted: u64,
}

impl CellDeltaRecoverySummary {
    /// Log the recovery summary.
    pub fn log_summary(&self) {
        tracing::info!(
            cell_delta_frames = self.cell_delta_frames,
            full_page_frames = self.full_page_frames,
            cell_delta_bytes = self.cell_delta_bytes,
            pages_with_cell_deltas = self.pages_with_cell_deltas,
            deltas_inserted = self.deltas_inserted,
            "wal_recovery_summary"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::cell_visibility::{CellResolve, CellVisibilityLog};
    use fsqlite_types::{BtreeRef, SemanticKeyKind, TableId, TxnEpoch, TxnToken};
    use proptest::{
        collection::vec as prop_vec,
        prelude::*,
        test_runner::{Config as ProptestConfig, TestCaseError},
    };

    const MVCC_DURABILITY_PROPTEST_CASES: u32 = 64;

    fn make_cell_key() -> CellKey {
        CellKey {
            btree: BtreeRef::Table(TableId::new(1)),
            kind: SemanticKeyKind::TableRow,
            key_digest: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        }
    }

    fn serialize(frame: &CellDeltaWalFrame) -> Vec<u8> {
        frame.serialize().expect("test cell-delta frame is valid")
    }

    fn durable_cell_key_from_digest(key_digest: [u8; 16]) -> CellKey {
        CellKey {
            btree: BtreeRef::Table(TableId::new(1)),
            kind: SemanticKeyKind::TableRow,
            key_digest,
        }
    }

    fn replay_committed_frame(
        log: &CellVisibilityLog,
        frame: &CellDeltaWalFrame,
    ) -> std::result::Result<(), String> {
        let key = durable_cell_key_from_digest(frame.key_digest);
        let epoch = u32::try_from(frame.txn_id.get())
            .map_err(|err| format!("txn epoch conversion failed: {err}"))?;
        let txn = TxnToken::new(frame.txn_id, TxnEpoch::new(epoch));
        let delta_idx = match frame.op {
            CellDeltaOp::Insert => {
                log.record_insert(key, frame.page_number, frame.cell_data.clone(), txn)
            }
            CellDeltaOp::Update => {
                log.record_update(key, frame.page_number, frame.cell_data.clone(), txn)
            }
            CellDeltaOp::Delete => log.record_delete(key, frame.page_number, txn),
        }
        .ok_or_else(|| {
            format!(
                "replay exceeded visibility-log budget for page {}",
                frame.page_number.get()
            )
        })?;
        log.commit_delta(delta_idx, frame.commit_seq);
        Ok(())
    }

    fn resolve_committed_cells(
        log: &CellVisibilityLog,
        cells: &BTreeMap<(u32, [u8; 16]), (PageNumber, CellKey)>,
        snapshot: CommitSeq,
    ) -> BTreeMap<(u32, [u8; 16]), CellResolve> {
        cells
            .iter()
            .map(|(identity, (page, key))| (*identity, log.resolve_state(*page, key, snapshot)))
            .collect()
    }

    // Metamorphic relation MR4: durability under crash/replay.
    // Fault sensitivity 5, independence 5, execution cost 2, score 12.5.
    // Transform a committed MVCC cell-delta history by serializing it to WAL
    // bytes, dropping the live visibility log, and replaying into a fresh log.
    // The visible committed state must be identical after recovery.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(MVCC_DURABILITY_PROPTEST_CASES))]

        #[test]
        fn metamorphic_durability_replays_committed_cell_deltas_after_crash(
            raw_writes in prop_vec(
                (1_u16..=64, 0_u8..=2, prop_vec(any::<u8>(), 0..=32)),
                1..=32,
            ),
        ) {
            let live_log = CellVisibilityLog::new(1 << 20);
            let mut frames = Vec::with_capacity(raw_writes.len());
            let mut cells = BTreeMap::new();

            for (idx, (rowid, op_selector, raw_payload)) in raw_writes.iter().enumerate() {
                let page = PageNumber::new(1 + u32::from(*rowid % 16))
                    .ok_or_else(|| TestCaseError::fail("generated page number was invalid"))?;
                let key = CellKey::table_row(
                    BtreeRef::Table(TableId::new(1)),
                    i64::from(*rowid),
                );
                let op = match op_selector {
                    0 => CellDeltaOp::Insert,
                    1 => CellDeltaOp::Update,
                    _ => CellDeltaOp::Delete,
                };
                let cell_data = if matches!(op, CellDeltaOp::Delete) {
                    Vec::new()
                } else {
                    raw_payload.clone()
                };
                let commit_seq = CommitSeq::new(idx as u64 + 1);
                let txn_id = TxnId::new(idx as u64 + 1)
                    .ok_or_else(|| TestCaseError::fail("generated txn id was invalid"))?;
                let txn_epoch = u32::try_from(idx + 1)
                    .map_err(|err| TestCaseError::fail(format!("txn epoch conversion failed: {err}")))?;
                let txn = TxnToken::new(txn_id, TxnEpoch::new(txn_epoch));

                let delta_idx = match op {
                    CellDeltaOp::Insert => {
                        live_log.record_insert(key, page, cell_data.clone(), txn)
                    }
                    CellDeltaOp::Update => {
                        live_log.record_update(key, page, cell_data.clone(), txn)
                    }
                    CellDeltaOp::Delete => live_log.record_delete(key, page, txn),
                }
                .ok_or_else(|| {
                    TestCaseError::fail(format!(
                        "live visibility log budget exceeded for rowid {rowid}"
                    ))
                })?;
                live_log.commit_delta(delta_idx, commit_seq);

                frames.push(CellDeltaWalFrame::new(
                    page,
                    &key,
                    op,
                    commit_seq,
                    txn_id,
                    cell_data,
                ));
                cells.insert((page.get(), key.key_digest), (page, key));
            }

            let snapshot = CommitSeq::new(raw_writes.len() as u64 + 1);
            let expected = resolve_committed_cells(&live_log, &cells, snapshot);
            let wal_bytes = serialize_cell_delta_batch(&frames)
                .map_err(|err| TestCaseError::fail(format!("serialize WAL batch failed: {err}")))?;

            let recovered_frames = deserialize_cell_delta_batch(&wal_bytes);
            prop_assert_eq!(&recovered_frames, &frames);

            let recovered_log = CellVisibilityLog::new(1 << 20);
            for frame in &recovered_frames {
                replay_committed_frame(&recovered_log, frame)
                    .map_err(TestCaseError::fail)?;
            }
            let recovered = resolve_committed_cells(&recovered_log, &cells, snapshot);

            prop_assert_eq!(recovered, expected);
        }
    }

    // -----------------------------------------------------------------------
    // Serialization tests (from C4-WAL bead)
    // -----------------------------------------------------------------------

    #[test]
    fn test_cell_delta_frame_round_trip() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(12345),
            txn_id: TxnId::new(67890).unwrap(),
            cell_data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };

        let serialized = serialize(&frame);
        let deserialized = CellDeltaWalFrame::deserialize(&serialized);

        assert_eq!(deserialized, Some(frame));
    }

    #[test]
    fn test_cell_delta_frame_matches_wal_crate_wire_format() {
        let key_digest = [1, 3, 3, 7, 8, 13, 21, 34, 55, 89, 144, 233, 5, 8, 13, 21];
        let mvcc_frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest,
            op: CellDeltaOp::Update,
            commit_seq: CommitSeq::new(12345),
            txn_id: TxnId::new(67890).unwrap(),
            cell_data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };
        let wal_frame = fsqlite_wal::CellDeltaWalFrame::new(
            mvcc_frame.page_number,
            key_digest,
            fsqlite_wal::CellOp::Update,
            mvcc_frame.commit_seq,
            mvcc_frame.txn_id,
            mvcc_frame.cell_data.clone(),
        );

        assert_eq!(
            serialize(&mvcc_frame),
            wal_frame.serialize().expect("WAL frame should serialize")
        );
    }

    #[test]
    fn test_cell_delta_frame_checksum() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            op: CellDeltaOp::Update,
            commit_seq: CommitSeq::new(100),
            txn_id: TxnId::new(200).unwrap(),
            cell_data: vec![1, 2, 3, 4, 5],
        };

        let mut serialized = serialize(&frame);

        // Corrupt one byte in the middle
        let corrupt_idx = serialized.len() / 2;
        serialized[corrupt_idx] ^= 0xFF;

        // Should fail to deserialize
        assert!(CellDeltaWalFrame::deserialize(&serialized).is_none());
    }

    #[test]
    fn test_cell_delta_frame_variable_length() {
        // Empty cell data (Delete)
        let frame_empty = CellDeltaWalFrame {
            page_number: PageNumber::new(1).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Delete,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![],
        };
        let ser = serialize(&frame_empty);
        assert_eq!(CellDeltaWalFrame::deserialize(&ser), Some(frame_empty));

        // 100 bytes cell data
        let frame_100 = CellDeltaWalFrame {
            page_number: PageNumber::new(2).unwrap(),
            key_digest: [1; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(2),
            txn_id: TxnId::new(2).unwrap(),
            cell_data: vec![0xAB; 100],
        };
        let ser = serialize(&frame_100);
        assert_eq!(CellDeltaWalFrame::deserialize(&ser), Some(frame_100));

        // 4000 bytes cell data
        let frame_4000 = CellDeltaWalFrame {
            page_number: PageNumber::new(3).unwrap(),
            key_digest: [2; 16],
            op: CellDeltaOp::Update,
            commit_seq: CommitSeq::new(3),
            txn_id: TxnId::new(3).unwrap(),
            cell_data: vec![0xCD; 4000],
        };
        let ser = serialize(&frame_4000);
        assert_eq!(CellDeltaWalFrame::deserialize(&ser), Some(frame_4000));
    }

    #[test]
    fn test_cell_delta_frame_marker_word() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![1, 2, 3],
        };

        let serialized = serialize(&frame);

        let marker =
            u32::from_be_bytes([serialized[0], serialized[1], serialized[2], serialized[3]]);
        let page_number =
            u32::from_be_bytes([serialized[4], serialized[5], serialized[6], serialized[7]]);

        assert_eq!(marker, CELL_DELTA_FRAME_MARKER);
        assert_eq!(page_number, 42);

        // is_cell_delta_frame should return true
        assert!(CellDeltaWalFrame::is_cell_delta_frame(&serialized));

        // Regular page frame (starts with page number) should return false
        let fake_page_frame = [0x00, 0x00, 0x00, 0x01]; // page 1
        assert!(!CellDeltaWalFrame::is_cell_delta_frame(&fake_page_frame));

        // A full-page frame whose first word happens to match the marker is
        // still not a valid cell-delta envelope.
        let fake_marker_page_frame = [
            0x00, 0x00, 0x00, 0x00, // marker
            0x00, 0x00, 0x00, 0x01, // page 1
        ];
        assert!(!CellDeltaWalFrame::is_cell_delta_frame(
            &fake_marker_page_frame
        ));
    }

    #[test]
    fn test_cell_delta_op_conversion() {
        assert_eq!(CellDeltaOp::from_byte(1), Some(CellDeltaOp::Insert));
        assert_eq!(CellDeltaOp::from_byte(2), Some(CellDeltaOp::Update));
        assert_eq!(CellDeltaOp::from_byte(3), Some(CellDeltaOp::Delete));
        assert_eq!(CellDeltaOp::from_byte(0), None);
        assert_eq!(CellDeltaOp::from_byte(4), None);
        assert_eq!(CellDeltaOp::from_byte(255), None);
    }

    #[test]
    fn test_batch_serialization() {
        let frames = vec![
            CellDeltaWalFrame {
                page_number: PageNumber::new(1).unwrap(),
                key_digest: [1; 16],
                op: CellDeltaOp::Insert,
                commit_seq: CommitSeq::new(10),
                txn_id: TxnId::new(100).unwrap(),
                cell_data: vec![1, 2, 3],
            },
            CellDeltaWalFrame {
                page_number: PageNumber::new(2).unwrap(),
                key_digest: [2; 16],
                op: CellDeltaOp::Update,
                commit_seq: CommitSeq::new(20),
                txn_id: TxnId::new(200).unwrap(),
                cell_data: vec![4, 5, 6, 7],
            },
            CellDeltaWalFrame {
                page_number: PageNumber::new(3).unwrap(),
                key_digest: [3; 16],
                op: CellDeltaOp::Delete,
                commit_seq: CommitSeq::new(30),
                txn_id: TxnId::new(300).unwrap(),
                cell_data: vec![],
            },
        ];

        let serialized =
            serialize_cell_delta_batch(&frames).expect("test cell-delta batch is valid");
        let deserialized = deserialize_cell_delta_batch(&serialized);

        assert_eq!(deserialized, frames);
    }

    #[test]
    fn test_serialized_size() {
        // Empty data: 45 header + 0 data + 4 checksum = 49
        let frame_empty = CellDeltaWalFrame {
            page_number: PageNumber::new(1).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Delete,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![],
        };
        assert_eq!(frame_empty.serialized_size(), 49);
        assert_eq!(serialize(&frame_empty).len(), 49);

        // 100 bytes data: 45 + 100 + 4 = 149
        let frame_100 = CellDeltaWalFrame {
            page_number: PageNumber::new(1).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![0; 100],
        };
        assert_eq!(frame_100.serialized_size(), 149);
        assert_eq!(serialize(&frame_100).len(), 149);
    }

    #[test]
    fn test_truncated_frame_rejected() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![1, 2, 3, 4, 5],
        };

        let serialized = serialize(&frame);

        // Truncate at various points
        for truncate_at in [0, 10, 20, 40, serialized.len() - 1] {
            let truncated = &serialized[..truncate_at];
            assert!(
                CellDeltaWalFrame::deserialize(truncated).is_none(),
                "Should reject frame truncated at {truncate_at}"
            );
        }
    }

    #[test]
    fn test_trailing_bytes_rejected() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![1, 2, 3],
        };

        let mut serialized = serialize(&frame);
        serialized.extend_from_slice(b"junk");

        assert!(
            CellDeltaWalFrame::deserialize(&serialized).is_none(),
            "frame decoder must reject bytes not covered by the frame checksum"
        );
    }

    #[test]
    fn test_delete_payload_rejected() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Delete,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![1, 2, 3],
        };

        assert!(frame.serialize().is_err());
    }

    #[test]
    fn test_serialize_oversized_payload_rejected() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![0; CELL_DELTA_MAX_DATA_LEN as usize + 1],
        };

        assert!(frame.serialize().is_err());
    }

    #[test]
    fn test_serialize_batch_rejects_oversized_payload() {
        let frame = CellDeltaWalFrame {
            page_number: PageNumber::new(42).unwrap(),
            key_digest: [0; 16],
            op: CellDeltaOp::Insert,
            commit_seq: CommitSeq::new(1),
            txn_id: TxnId::new(1).unwrap(),
            cell_data: vec![0; CELL_DELTA_MAX_DATA_LEN as usize + 1],
        };

        assert!(serialize_cell_delta_batch(&[frame]).is_err());
    }

    #[test]
    fn test_invalid_page_number_rejected() {
        // page_number 0 is invalid
        let mut buf = CELL_DELTA_FRAME_MARKER.to_be_bytes().to_vec();
        buf.extend_from_slice(&0u32.to_be_bytes()); // page 0
        buf.extend_from_slice(&[0u8; 16]); // key_digest
        buf.push(1); // op
        buf.extend_from_slice(&1u64.to_be_bytes()); // commit_seq
        buf.extend_from_slice(&1u64.to_be_bytes()); // txn_id
        buf.extend_from_slice(&0u32.to_be_bytes()); // data_len
        let checksum = crc32c_checksum(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        assert!(CellDeltaWalFrame::deserialize(&buf).is_none());
    }

    #[test]
    fn test_invalid_txn_id_rejected() {
        // txn_id 0 is invalid
        let mut buf = CELL_DELTA_FRAME_MARKER.to_be_bytes().to_vec();
        buf.extend_from_slice(&1u32.to_be_bytes()); // page 1
        buf.extend_from_slice(&[0u8; 16]); // key_digest
        buf.push(1); // op
        buf.extend_from_slice(&1u64.to_be_bytes()); // commit_seq
        buf.extend_from_slice(&0u64.to_be_bytes()); // txn_id 0 (invalid)
        buf.extend_from_slice(&0u32.to_be_bytes()); // data_len
        let checksum = crc32c_checksum(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        assert!(CellDeltaWalFrame::deserialize(&buf).is_none());
    }

    #[test]
    fn test_invalid_op_rejected() {
        let mut buf = CELL_DELTA_FRAME_MARKER.to_be_bytes().to_vec();
        buf.extend_from_slice(&1u32.to_be_bytes()); // page 1
        buf.extend_from_slice(&[0u8; 16]); // key_digest
        buf.push(99); // invalid op
        buf.extend_from_slice(&1u64.to_be_bytes()); // commit_seq
        buf.extend_from_slice(&1u64.to_be_bytes()); // txn_id
        buf.extend_from_slice(&0u32.to_be_bytes()); // data_len
        let checksum = crc32c_checksum(&buf);
        buf.extend_from_slice(&checksum.to_be_bytes());

        assert!(CellDeltaWalFrame::deserialize(&buf).is_none());
    }

    #[test]
    fn test_from_cell_key() {
        let cell_key = make_cell_key();
        let frame = CellDeltaWalFrame::new(
            PageNumber::new(42).unwrap(),
            &cell_key,
            CellDeltaOp::Insert,
            CommitSeq::new(100),
            TxnId::new(200).unwrap(),
            vec![1, 2, 3],
        );

        assert_eq!(frame.key_digest, cell_key.key_digest);
    }
}
