//! §5.9.0 Coordinator IPC Transport: Unix Socket Protocol + Wire Schemas (bd-1m07).
//!
//! Length-delimited framing with big-endian header, little-endian payload.
//! Two-phase reserve/submit discipline with permit lifecycle.
//! TxnToken-keyed idempotency cache and peer UID authentication.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_types::ObjectId;
use fsqlite_types::encoding::{
    append_u16_be, append_u32_be, append_u32_le, append_u64_be, append_u64_le, read_u16_be,
    read_u32_be, read_u32_le, read_u64_be, read_u64_le,
};
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Constants (§5.9.0 normative)
// ---------------------------------------------------------------------------

/// Minimum `len_be` value: version(2) + kind(2) + request_id(8) = 12.
pub const FRAME_MIN_LEN_BE: u32 = 12;

/// Maximum `len_be` value: 4 MiB.
pub const FRAME_MAX_LEN_BE: u32 = 4 * 1024 * 1024;

/// Wire protocol version (must be 1).
pub const PROTOCOL_VERSION: u16 = 1;

/// Default maximum outstanding permits per coordinator.
pub const MAX_OUTSTANDING_PERMITS: usize = 16;

/// Maximum wire `write_set_summary` length in bytes.
pub const WIRE_WRITE_SET_MAX_BYTES: usize = 1024 * 1024;

/// Maximum total witness + edge array element count per commit.
pub const WIRE_WITNESS_EDGE_MAX: usize = 65_536;

/// Frame header size on wire: `len_be`(4) + `version_be`(2) + `kind_be`(2) + `request_id`(8).
const FRAME_HEADER_WIRE_BYTES: usize = 16;

/// Wire `TxnToken` size: `txn_id`(8) + `txn_epoch`(4) + `pad`(4) = 16.
const WIRE_TXN_TOKEN_BYTES: usize = 16;

// ---------------------------------------------------------------------------
// MessageKind (§5.9.0 wire kinds V1)
// ---------------------------------------------------------------------------

/// Wire message kind discriminant (V1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageKind {
    /// Client → coordinator: request commit pipeline slot.
    Reserve,
    /// Client → coordinator: submit native-mode commit.
    SubmitNativePublish,
    /// Client → coordinator: submit WAL-mode commit.
    SubmitWalCommit,
    /// Client → coordinator: reserve contiguous rowid block.
    RowidReserve,
    /// Coordinator → client: response to any request.
    Response,
    /// Keepalive ping.
    Ping,
    /// Keepalive pong.
    Pong,
}

impl MessageKind {
    /// Wire discriminant value.
    #[must_use]
    pub const fn to_u16(self) -> u16 {
        match self {
            Self::Reserve => 1,
            Self::SubmitNativePublish => 2,
            Self::SubmitWalCommit => 3,
            Self::RowidReserve => 4,
            Self::Response => 5,
            Self::Ping => 6,
            Self::Pong => 7,
        }
    }

    /// Parse wire discriminant; `None` for unknown kinds.
    #[must_use]
    pub const fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::Reserve),
            2 => Some(Self::SubmitNativePublish),
            3 => Some(Self::SubmitWalCommit),
            4 => Some(Self::RowidReserve),
            5 => Some(Self::Response),
            6 => Some(Self::Ping),
            7 => Some(Self::Pong),
            _ => None,
        }
    }
}

impl fmt::Display for MessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Reserve => "RESERVE",
            Self::SubmitNativePublish => "SUBMIT_NATIVE_PUBLISH",
            Self::SubmitWalCommit => "SUBMIT_WAL_COMMIT",
            Self::RowidReserve => "ROWID_RESERVE",
            Self::Response => "RESPONSE",
            Self::Ping => "PING",
            Self::Pong => "PONG",
        };
        f.write_str(label)
    }
}

// ---------------------------------------------------------------------------
// FrameError
// ---------------------------------------------------------------------------

/// Errors from frame decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Input buffer shorter than frame header.
    TooShort,
    /// `len_be` below minimum (12).
    LenTooSmall(u32),
    /// `len_be` exceeds 4 MiB cap.
    LenTooLarge(u32),
    /// Unsupported protocol version.
    UnknownVersion(u16),
    /// Unrecognised message kind.
    UnknownKind(u16),
    /// Buffer does not contain full payload indicated by `len_be`.
    PayloadTruncated { expected: u32, actual: usize },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort => f.write_str("frame buffer too short for header"),
            Self::LenTooSmall(v) => write!(f, "len_be {v} below minimum {FRAME_MIN_LEN_BE}"),
            Self::LenTooLarge(v) => write!(f, "len_be {v} exceeds cap {FRAME_MAX_LEN_BE}"),
            Self::UnknownVersion(v) => write!(f, "unknown protocol version {v}"),
            Self::UnknownKind(v) => write!(f, "unknown message kind {v}"),
            Self::PayloadTruncated { expected, actual } => {
                write!(
                    f,
                    "payload truncated: expected {expected} bytes, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for FrameError {}

// ---------------------------------------------------------------------------
// Frame (§5.9.0 framing)
// ---------------------------------------------------------------------------

/// A decoded wire frame.
///
/// On-wire layout (big-endian header):
/// ```text
/// [len_be:u32][version_be:u16][kind_be:u16][request_id:u64_be][payload...]
/// ```
/// `len_be` = 12 + payload.len().
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: MessageKind,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Encode this frame to wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let payload_len = self.payload.len();
        let max_payload_len = (FRAME_MAX_LEN_BE - FRAME_MIN_LEN_BE) as usize;
        assert!(
            payload_len <= max_payload_len,
            "frame payload length {payload_len} exceeds max {max_payload_len}"
        );
        let len_be = FRAME_MIN_LEN_BE + u32::try_from(payload_len).expect("payload_len fits u32");
        let mut buf = Vec::with_capacity(FRAME_HEADER_WIRE_BYTES + payload_len);
        append_u32_be(&mut buf, len_be);
        append_u16_be(&mut buf, PROTOCOL_VERSION);
        append_u16_be(&mut buf, self.kind.to_u16());
        append_u64_be(&mut buf, self.request_id);
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode a frame from wire bytes.
    ///
    /// # Errors
    /// Returns [`FrameError`] if the buffer is malformed.
    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < FRAME_HEADER_WIRE_BYTES {
            return Err(FrameError::TooShort);
        }
        let len_be = read_u32_be(&buf[0..4]).ok_or(FrameError::TooShort)?;
        if len_be < FRAME_MIN_LEN_BE {
            return Err(FrameError::LenTooSmall(len_be));
        }
        if len_be > FRAME_MAX_LEN_BE {
            return Err(FrameError::LenTooLarge(len_be));
        }
        let version = read_u16_be(&buf[4..6]).ok_or(FrameError::TooShort)?;
        if version != PROTOCOL_VERSION {
            return Err(FrameError::UnknownVersion(version));
        }
        let kind_raw = read_u16_be(&buf[6..8]).ok_or(FrameError::TooShort)?;
        let kind = MessageKind::from_u16(kind_raw).ok_or(FrameError::UnknownKind(kind_raw))?;
        let request_id = read_u64_be(&buf[8..16]).ok_or(FrameError::TooShort)?;

        let payload_len = (len_be - FRAME_MIN_LEN_BE) as usize;
        let remaining = &buf[FRAME_HEADER_WIRE_BYTES..];
        if remaining.len() < payload_len {
            return Err(FrameError::PayloadTruncated {
                expected: len_be - FRAME_MIN_LEN_BE,
                actual: remaining.len(),
            });
        }
        let payload = remaining[..payload_len].to_vec();
        Ok(Self {
            kind,
            request_id,
            payload,
        })
    }
}

// ---------------------------------------------------------------------------
// Wire TxnToken (§5.9.0 common atom)
// ---------------------------------------------------------------------------

/// On-wire TxnToken: `txn_id:u64_le`, `txn_epoch:u32_le`, `pad:u32_le=0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WireTxnToken {
    pub txn_id: u64,
    pub txn_epoch: u32,
}

impl WireTxnToken {
    /// Encode to a 16-byte little-endian representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; WIRE_TXN_TOKEN_BYTES] {
        let mut buf = [0u8; WIRE_TXN_TOKEN_BYTES];
        buf[..8].copy_from_slice(&self.txn_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.txn_epoch.to_le_bytes());
        // pad bytes 12..16 remain zero
        buf
    }

    /// Decode from a 16-byte slice.
    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let txn_id = read_u64_le(src.get(..8)?)?;
        let txn_epoch = read_u32_le(src.get(8..12)?)?;
        Some(Self { txn_id, txn_epoch })
    }

    /// Idempotency key for cache lookup.
    #[must_use]
    pub const fn idempotency_key(self) -> (u64, u32) {
        (self.txn_id, self.txn_epoch)
    }
}

// ---------------------------------------------------------------------------
// RESERVE payload + response (§5.9.0)
// ---------------------------------------------------------------------------

/// RESERVE request payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservePayload {
    /// Purpose discriminant: 0 = commit, 1 = rowid.
    pub purpose: u8,
    pub txn: WireTxnToken,
}

impl ReservePayload {
    /// Wire size: purpose(1) + pad(7) + txn(16) = 24 (§5.9.0.1).
    const WIRE_BYTES: usize = 24;

    /// Encode to payload bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::WIRE_BYTES);
        buf.push(self.purpose);
        buf.extend_from_slice(&[0u8; 7]); // pad to 8-byte alignment (§5.9.0.1)
        buf.extend_from_slice(&self.txn.to_bytes());
        buf
    }

    /// Decode from payload bytes.
    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        if src.len() < Self::WIRE_BYTES {
            return None;
        }
        let purpose = src[0];
        let txn = WireTxnToken::from_bytes(&src[8..])?;
        Some(Self { purpose, txn })
    }
}

/// RESERVE response variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveResponse {
    Ok { permit_id: u64 },
    Busy { retry_after_ms: u32 },
    Err { code: u32 },
}

impl ReserveResponse {
    /// Tag values for the tagged union.
    const TAG_OK: u8 = 0;
    const TAG_BUSY: u8 = 1;
    const TAG_ERR: u8 = 2;

    /// Encode to payload bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        match self {
            Self::Ok { permit_id } => {
                buf.push(Self::TAG_OK);
                buf.extend_from_slice(&[0u8; 7]); // pad to 8-byte alignment
                append_u64_le(&mut buf, *permit_id);
            }
            Self::Busy { retry_after_ms } => {
                buf.push(Self::TAG_BUSY);
                buf.extend_from_slice(&[0u8; 7]); // pad to 8-byte alignment (§5.9.0.1)
                append_u32_le(&mut buf, *retry_after_ms);
                append_u32_le(&mut buf, 0); // pad1
            }
            Self::Err { code } => {
                buf.push(Self::TAG_ERR);
                buf.extend_from_slice(&[0u8; 7]); // pad to 8-byte alignment (§5.9.0.1)
                append_u32_le(&mut buf, *code);
            }
        }
        buf
    }

    /// Decode from payload bytes.
    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let tag = *src.first()?;
        match tag {
            Self::TAG_OK => {
                let permit_id = read_u64_le(src.get(8..16)?)?;
                Some(Self::Ok { permit_id })
            }
            Self::TAG_BUSY => {
                let retry = read_u32_le(src.get(8..12)?)?;
                Some(Self::Busy {
                    retry_after_ms: retry,
                })
            }
            Self::TAG_ERR => {
                let code = read_u32_le(src.get(8..12)?)?;
                Some(Self::Err { code })
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// ROWID_RESERVE payload + response
// ---------------------------------------------------------------------------

/// ROWID_RESERVE request payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidReservePayload {
    pub txn: WireTxnToken,
    pub schema_epoch: u64,
    pub table_id: u32,
    pub count: u32,
}

impl RowidReservePayload {
    /// txn(16) + schema_epoch(8) + table_id(4) + count(4) = 32 (§5.9.0.1).
    const WIRE_BYTES: usize = 32;

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::WIRE_BYTES);
        buf.extend_from_slice(&self.txn.to_bytes());
        append_u64_le(&mut buf, self.schema_epoch);
        append_u32_le(&mut buf, self.table_id);
        append_u32_le(&mut buf, self.count);
        buf
    }

    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        if src.len() < Self::WIRE_BYTES {
            return None;
        }
        let txn = WireTxnToken::from_bytes(src)?;
        let schema_epoch = read_u64_le(&src[16..24])?;
        let table_id = read_u32_le(&src[24..28])?;
        let count = read_u32_le(&src[28..32])?;
        Some(Self {
            txn,
            schema_epoch,
            table_id,
            count,
        })
    }
}

/// ROWID_RESERVE response variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowidReserveResponse {
    Ok { start_rowid: u64, count: u32 },
    Err { code: u32 },
}

impl RowidReserveResponse {
    const TAG_OK: u8 = 0;
    const TAG_ERR: u8 = 1;

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        match self {
            Self::Ok { start_rowid, count } => {
                buf.push(Self::TAG_OK);
                buf.extend_from_slice(&[0u8; 7]); // pad to 8-byte alignment
                append_u64_le(&mut buf, *start_rowid);
                append_u32_le(&mut buf, *count);
            }
            Self::Err { code } => {
                buf.push(Self::TAG_ERR);
                buf.extend_from_slice(&[0u8; 3]); // pad
                append_u32_le(&mut buf, *code);
            }
        }
        buf
    }

    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let tag = *src.first()?;
        match tag {
            Self::TAG_OK => {
                let start_rowid = read_u64_le(src.get(8..16)?)?;
                let count = read_u32_le(src.get(16..20)?)?;
                Some(Self::Ok { start_rowid, count })
            }
            Self::TAG_ERR => {
                let code = read_u32_le(src.get(4..8)?)?;
                Some(Self::Err { code })
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// SUBMIT_NATIVE_PUBLISH payload (§5.9.0)
// ---------------------------------------------------------------------------

/// Spill page descriptor for WAL commit wire message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillPageEntry {
    pub pgno: u32,
    pub offset: u64,
    pub len: u32,
    pub xxh3_64: u64,
}

impl SpillPageEntry {
    /// pgno(4) + pad0(4) + offset(8) + len(4) + pad1(4) + xxh3_64(8) = 32 (§5.9.0.1).
    const WIRE_BYTES: usize = 32;

    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::WIRE_BYTES] {
        let mut buf = [0u8; Self::WIRE_BYTES];
        buf[..4].copy_from_slice(&self.pgno.to_le_bytes());
        // buf[4..8] = pad0 (zero)
        buf[8..16].copy_from_slice(&self.offset.to_le_bytes());
        buf[16..20].copy_from_slice(&self.len.to_le_bytes());
        // buf[20..24] = pad1 (zero)
        buf[24..32].copy_from_slice(&self.xxh3_64.to_le_bytes());
        buf
    }

    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        if src.len() < Self::WIRE_BYTES {
            return None;
        }
        Some(Self {
            pgno: read_u32_le(&src[..4])?,
            // src[4..8] = pad0 (skipped)
            offset: read_u64_le(&src[8..16])?,
            len: read_u32_le(&src[16..20])?,
            // src[20..24] = pad1 (skipped)
            xxh3_64: read_u64_le(&src[24..32])?,
        })
    }
}

/// SUBMIT_NATIVE_PUBLISH payload (abbreviated for framing layer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitNativePayload {
    pub permit_id: u64,
    pub txn: WireTxnToken,
    pub begin_seq: u64,
    pub capsule_object_id: ObjectId,
    pub capsule_digest_32: [u8; 32],
    /// Sorted ascending, no duplicates (page numbers as u32).
    pub write_set_summary: Vec<u32>,
    /// Sorted lexicographically, no duplicates.
    pub read_witness_refs: Vec<ObjectId>,
    /// Sorted lexicographically, no duplicates.
    pub write_witness_refs: Vec<ObjectId>,
    /// Sorted lexicographically, no duplicates.
    pub edge_refs: Vec<ObjectId>,
    /// Sorted lexicographically, no duplicates.
    pub merge_refs: Vec<ObjectId>,
    pub abort_policy: u8,
}

impl SubmitNativePayload {
    /// Encode to payload bytes.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn to_bytes(&self) -> Vec<u8> {
        assert!(
            validate_write_set_summary(&self.write_set_summary),
            "write_set_summary exceeds wire cap"
        );
        assert!(
            is_canonical_pages(&self.write_set_summary),
            "write_set_summary must be sorted ascending with no duplicates"
        );
        assert!(
            validate_witness_edge_counts(
                self.read_witness_refs.len(),
                self.write_witness_refs.len(),
                self.edge_refs.len(),
                self.merge_refs.len()
            ),
            "witness/edge counts exceed wire cap"
        );
        assert!(
            is_canonical_object_ids(&self.read_witness_refs),
            "read_witness_refs must be sorted with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.write_witness_refs),
            "write_witness_refs must be sorted with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.edge_refs),
            "edge_refs must be sorted with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.merge_refs),
            "merge_refs must be sorted with no duplicates"
        );

        let mut buf = Vec::with_capacity(256);
        // permit_id
        append_u64_le(&mut buf, self.permit_id);
        // txn
        buf.extend_from_slice(&self.txn.to_bytes());
        // begin_seq
        append_u64_le(&mut buf, self.begin_seq);
        // capsule_object_id (16 bytes)
        buf.extend_from_slice(self.capsule_object_id.as_bytes());
        // capsule_digest_32 (32 bytes)
        buf.extend_from_slice(&self.capsule_digest_32);
        // write_set_summary: len(u32) + pages
        let ws_count = u32::try_from(self.write_set_summary.len())
            .expect("write_set_summary length must fit u32");
        append_u32_le(&mut buf, ws_count);
        for &pgno in &self.write_set_summary {
            append_u32_le(&mut buf, pgno);
        }
        // witness arrays: count(u32) + ObjectId[16] each
        encode_object_id_array(&mut buf, &self.read_witness_refs);
        encode_object_id_array(&mut buf, &self.write_witness_refs);
        encode_object_id_array(&mut buf, &self.edge_refs);
        encode_object_id_array(&mut buf, &self.merge_refs);
        // abort_policy
        buf.push(self.abort_policy);
        buf
    }

    /// Decode from payload bytes.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        let permit_id = read_u64_le(src.get(pos..pos + 8)?)?;
        pos += 8;
        let txn = WireTxnToken::from_bytes(src.get(pos..)?)?;
        pos += WIRE_TXN_TOKEN_BYTES;
        let begin_seq = read_u64_le(src.get(pos..pos + 8)?)?;
        pos += 8;
        let capsule_object_id = ObjectId::from_bytes(src.get(pos..pos + 16)?.try_into().ok()?);
        pos += 16;
        let capsule_digest_32: [u8; 32] = src.get(pos..pos + 32)?.try_into().ok()?;
        pos += 32;
        // write_set_summary
        let ws_count = read_u32_le(src.get(pos..pos + 4)?)? as usize;
        pos += 4;
        if ws_count > (WIRE_WRITE_SET_MAX_BYTES / 4) {
            return None;
        }
        let ws_bytes = ws_count.checked_mul(4)?;
        if src.len() < pos.checked_add(ws_bytes)? {
            return None;
        }
        let mut write_set_summary = Vec::with_capacity(ws_count);
        for _ in 0..ws_count {
            write_set_summary.push(read_u32_le(src.get(pos..pos + 4)?)?);
            pos += 4;
        }
        // witness arrays
        let mut remaining = WIRE_WITNESS_EDGE_MAX;
        let (read_witness_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&read_witness_refs) {
            return None;
        }
        remaining = remaining.saturating_sub(read_witness_refs.len());

        let (write_witness_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&write_witness_refs) {
            return None;
        }
        remaining = remaining.saturating_sub(write_witness_refs.len());

        let (edge_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&edge_refs) {
            return None;
        }
        remaining = remaining.saturating_sub(edge_refs.len());

        let (merge_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&merge_refs) {
            return None;
        }

        if !is_canonical_pages(&write_set_summary) {
            return None;
        }
        if !validate_write_set_summary(&write_set_summary) {
            return None;
        }
        if !validate_witness_edge_counts(
            read_witness_refs.len(),
            write_witness_refs.len(),
            edge_refs.len(),
            merge_refs.len(),
        ) {
            return None;
        }
        let abort_policy = *src.get(pos)?;

        Some(Self {
            permit_id,
            txn,
            begin_seq,
            capsule_object_id,
            capsule_digest_32,
            write_set_summary,
            read_witness_refs,
            write_witness_refs,
            edge_refs,
            merge_refs,
            abort_policy,
        })
    }
}

// ---------------------------------------------------------------------------
// SUBMIT_WAL_COMMIT payload (§5.9.0)
// ---------------------------------------------------------------------------

/// SUBMIT_WAL_COMMIT payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitWalPayload {
    pub permit_id: u64,
    pub txn: WireTxnToken,
    pub mode: u8,
    pub snapshot_high: u64,
    pub schema_epoch: u64,
    pub has_in_rw: bool,
    pub has_out_rw: bool,
    pub wal_fec_r: u8,
    /// Sorted ascending by pgno, no duplicates.
    pub spill_pages: Vec<SpillPageEntry>,
    pub read_witness_refs: Vec<ObjectId>,
    pub write_witness_refs: Vec<ObjectId>,
    pub edge_refs: Vec<ObjectId>,
    pub merge_refs: Vec<ObjectId>,
}

impl SubmitWalPayload {
    /// Encode to payload bytes.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn to_bytes(&self) -> Vec<u8> {
        assert!(
            validate_witness_edge_counts(
                self.read_witness_refs.len(),
                self.write_witness_refs.len(),
                self.edge_refs.len(),
                self.merge_refs.len()
            ),
            "witness/edge counts exceed wire cap"
        );
        assert!(
            is_canonical_spill_pages(&self.spill_pages),
            "spill_pages must be sorted by pgno with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.read_witness_refs),
            "read_witness_refs must be sorted with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.write_witness_refs),
            "write_witness_refs must be sorted with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.edge_refs),
            "edge_refs must be sorted with no duplicates"
        );
        assert!(
            is_canonical_object_ids(&self.merge_refs),
            "merge_refs must be sorted with no duplicates"
        );

        let mut buf = Vec::with_capacity(256);
        append_u64_le(&mut buf, self.permit_id);
        buf.extend_from_slice(&self.txn.to_bytes());
        buf.push(self.mode);
        buf.extend_from_slice(&[0u8; 7]); // pad0 to 8-byte alignment (§5.9.0.1)
        append_u64_le(&mut buf, self.snapshot_high);
        append_u64_le(&mut buf, self.schema_epoch);
        buf.push(u8::from(self.has_in_rw));
        buf.push(u8::from(self.has_out_rw));
        buf.push(self.wal_fec_r);
        buf.extend_from_slice(&[0u8; 5]); // pad1 to 8-byte alignment (§5.9.0.1)
        // spill_pages
        let sp_count =
            u32::try_from(self.spill_pages.len()).expect("spill_pages length must fit u32");
        append_u32_le(&mut buf, sp_count);
        for sp in &self.spill_pages {
            buf.extend_from_slice(&sp.to_bytes());
        }
        encode_object_id_array(&mut buf, &self.read_witness_refs);
        encode_object_id_array(&mut buf, &self.write_witness_refs);
        encode_object_id_array(&mut buf, &self.edge_refs);
        encode_object_id_array(&mut buf, &self.merge_refs);
        buf
    }

    /// Decode from payload bytes.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        let permit_id = read_u64_le(src.get(pos..pos + 8)?)?;
        pos += 8;
        let txn = WireTxnToken::from_bytes(src.get(pos..)?)?;
        pos += WIRE_TXN_TOKEN_BYTES;
        let mode = *src.get(pos)?;
        pos += 8; // mode(1) + pad0(7)
        let snapshot_high = read_u64_le(src.get(pos..pos + 8)?)?;
        pos += 8;
        let schema_epoch = read_u64_le(src.get(pos..pos + 8)?)?;
        pos += 8;
        let has_in_rw = *src.get(pos)? != 0;
        pos += 1;
        let has_out_rw = *src.get(pos)? != 0;
        pos += 1;
        let wal_fec_r = *src.get(pos)?;
        pos += 6; // fec_r(1) + pad1(5)
        // spill_pages
        let sp_count = read_u32_le(src.get(pos..pos + 4)?)? as usize;
        pos += 4;
        // Lower bound: we must have enough bytes for `sp_count` entries plus the four
        // trailing ObjectId arrays (each at least a u32 count = 4 bytes).
        let min_tail = 4usize * 4;
        let available = src.len().checked_sub(pos)?;
        if available < min_tail {
            return None;
        }
        let max_spill = available
            .checked_sub(min_tail)?
            .checked_div(SpillPageEntry::WIRE_BYTES)?;
        if sp_count > max_spill {
            return None;
        }
        let mut spill_pages = Vec::with_capacity(sp_count);
        for _ in 0..sp_count {
            spill_pages.push(SpillPageEntry::from_bytes(src.get(pos..)?)?);
            pos += SpillPageEntry::WIRE_BYTES;
        }
        if !is_canonical_spill_pages(&spill_pages) {
            return None;
        }
        let mut remaining = WIRE_WITNESS_EDGE_MAX;
        let (read_witness_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&read_witness_refs) {
            return None;
        }
        remaining = remaining.saturating_sub(read_witness_refs.len());

        let (write_witness_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&write_witness_refs) {
            return None;
        }
        remaining = remaining.saturating_sub(write_witness_refs.len());

        let (edge_refs, new_pos) = decode_object_id_array(src, pos, remaining)?;
        pos = new_pos;
        if !is_canonical_object_ids(&edge_refs) {
            return None;
        }
        remaining = remaining.saturating_sub(edge_refs.len());

        let (merge_refs, _) = decode_object_id_array(src, pos, remaining)?;
        if !is_canonical_object_ids(&merge_refs) {
            return None;
        }
        if !validate_witness_edge_counts(
            read_witness_refs.len(),
            write_witness_refs.len(),
            edge_refs.len(),
            merge_refs.len(),
        ) {
            return None;
        }

        Some(Self {
            permit_id,
            txn,
            mode,
            snapshot_high,
            schema_epoch,
            has_in_rw,
            has_out_rw,
            wal_fec_r,
            spill_pages,
            read_witness_refs,
            write_witness_refs,
            edge_refs,
            merge_refs,
        })
    }
}

// ---------------------------------------------------------------------------
// Response payloads (§5.9.0)
// ---------------------------------------------------------------------------

/// Commit response for native-mode publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePublishResponse {
    Ok { commit_seq: u64 },
    Conflict { pages: Vec<u32>, reason: u8 },
    Aborted { code: u32 },
    Err { code: u32 },
}

impl NativePublishResponse {
    const TAG_OK: u8 = 0;
    const TAG_CONFLICT: u8 = 1;
    const TAG_ABORTED: u8 = 2;
    const TAG_ERR: u8 = 3;

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        match self {
            Self::Ok { commit_seq } => {
                buf.push(Self::TAG_OK);
                buf.extend_from_slice(&[0u8; 7]);
                append_u64_le(&mut buf, *commit_seq);
            }
            Self::Conflict { pages, reason } => {
                buf.push(Self::TAG_CONFLICT);
                buf.push(*reason);
                buf.extend_from_slice(&[0u8; 2]); // pad
                let count = u32::try_from(pages.len()).expect("conflict pages length must fit u32");
                append_u32_le(&mut buf, count);
                for &p in pages {
                    append_u32_le(&mut buf, p);
                }
            }
            Self::Aborted { code } => {
                buf.push(Self::TAG_ABORTED);
                buf.extend_from_slice(&[0u8; 3]);
                append_u32_le(&mut buf, *code);
            }
            Self::Err { code } => {
                buf.push(Self::TAG_ERR);
                buf.extend_from_slice(&[0u8; 3]);
                append_u32_le(&mut buf, *code);
            }
        }
        buf
    }

    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let tag = *src.first()?;
        match tag {
            Self::TAG_OK => {
                let commit_seq = read_u64_le(src.get(8..16)?)?;
                Some(Self::Ok { commit_seq })
            }
            Self::TAG_CONFLICT => {
                let reason = *src.get(1)?;
                let count = read_u32_le(src.get(4..8)?)? as usize;
                let max_pages = src.len().saturating_sub(8) / 4;
                if count > max_pages {
                    return None;
                }
                let mut pages = Vec::with_capacity(count);
                for i in 0..count {
                    let off = 8 + i * 4;
                    pages.push(read_u32_le(src.get(off..off + 4)?)?);
                }
                Some(Self::Conflict { pages, reason })
            }
            Self::TAG_ABORTED => {
                let code = read_u32_le(src.get(4..8)?)?;
                Some(Self::Aborted { code })
            }
            Self::TAG_ERR => {
                let code = read_u32_le(src.get(4..8)?)?;
                Some(Self::Err { code })
            }
            _ => None,
        }
    }
}

/// Commit response for WAL-mode commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalCommitResponse {
    Ok { commit_seq: u64 },
    Conflict { pages: Vec<u32>, reason: u8 },
    IoError { code: u32 },
    Err { code: u32 },
}

impl WalCommitResponse {
    const TAG_OK: u8 = 0;
    const TAG_CONFLICT: u8 = 1;
    const TAG_IO_ERROR: u8 = 2;
    const TAG_ERR: u8 = 3;

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        match self {
            Self::Ok { commit_seq } => {
                buf.push(Self::TAG_OK);
                buf.extend_from_slice(&[0u8; 7]);
                append_u64_le(&mut buf, *commit_seq);
            }
            Self::Conflict { pages, reason } => {
                buf.push(Self::TAG_CONFLICT);
                buf.push(*reason);
                buf.extend_from_slice(&[0u8; 2]);
                let count = u32::try_from(pages.len()).expect("conflict pages length must fit u32");
                append_u32_le(&mut buf, count);
                for &p in pages {
                    append_u32_le(&mut buf, p);
                }
            }
            Self::IoError { code } => {
                buf.push(Self::TAG_IO_ERROR);
                buf.extend_from_slice(&[0u8; 3]);
                append_u32_le(&mut buf, *code);
            }
            Self::Err { code } => {
                buf.push(Self::TAG_ERR);
                buf.extend_from_slice(&[0u8; 3]);
                append_u32_le(&mut buf, *code);
            }
        }
        buf
    }

    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        let tag = *src.first()?;
        match tag {
            Self::TAG_OK => {
                let commit_seq = read_u64_le(src.get(8..16)?)?;
                Some(Self::Ok { commit_seq })
            }
            Self::TAG_CONFLICT => {
                let reason = *src.get(1)?;
                let count = read_u32_le(src.get(4..8)?)? as usize;
                let max_pages = src.len().saturating_sub(8) / 4;
                if count > max_pages {
                    return None;
                }
                let mut pages = Vec::with_capacity(count);
                for i in 0..count {
                    let off = 8 + i * 4;
                    pages.push(read_u32_le(src.get(off..off + 4)?)?);
                }
                Some(Self::Conflict { pages, reason })
            }
            Self::TAG_IO_ERROR => {
                let code = read_u32_le(src.get(4..8)?)?;
                Some(Self::IoError { code })
            }
            Self::TAG_ERR => {
                let code = read_u32_le(src.get(4..8)?)?;
                Some(Self::Err { code })
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// ObjectId array encode/decode helpers
// ---------------------------------------------------------------------------

fn encode_object_id_array(buf: &mut Vec<u8>, ids: &[ObjectId]) {
    let count = u32::try_from(ids.len()).expect("ObjectId array length must fit u32");
    append_u32_le(buf, count);
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
    }
}

fn decode_object_id_array(
    src: &[u8],
    pos: usize,
    count_cap: usize,
) -> Option<(Vec<ObjectId>, usize)> {
    let count = read_u32_le(src.get(pos..pos + 4)?)? as usize;
    if count > count_cap {
        return None;
    }
    let bytes_needed = count.checked_mul(16)?.checked_add(4)?;
    let end = pos.checked_add(bytes_needed)?;
    if end > src.len() {
        return None;
    }
    let mut cur = pos + 4;
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let bytes: [u8; 16] = src.get(cur..cur + 16)?.try_into().ok()?;
        ids.push(ObjectId::from_bytes(bytes));
        cur += 16;
    }
    Some((ids, cur))
}

// ---------------------------------------------------------------------------
// Canonical ordering validation (§5.9.0 normative)
// ---------------------------------------------------------------------------

/// Validate that `pages` is sorted ascending with no duplicates.
#[must_use]
pub fn is_canonical_pages(pages: &[u32]) -> bool {
    pages.windows(2).all(|w| w[0] < w[1])
}

/// Validate that `spill_pages` is sorted ascending by pgno with no duplicates.
#[must_use]
pub fn is_canonical_spill_pages(spill_pages: &[SpillPageEntry]) -> bool {
    spill_pages.windows(2).all(|w| w[0].pgno < w[1].pgno)
}

/// Validate that `ids` is sorted lexicographically with no duplicates.
#[must_use]
pub fn is_canonical_object_ids(ids: &[ObjectId]) -> bool {
    ids.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes())
}

/// Validate that a `write_set_summary` meets wire size caps.
#[must_use]
pub fn validate_write_set_summary(pages: &[u32]) -> bool {
    let byte_len = pages.len().saturating_mul(4);
    byte_len <= WIRE_WRITE_SET_MAX_BYTES && byte_len % 4 == 0
}

/// Validate a raw write_set_summary byte length (§5.9.0.1).
///
/// Returns `true` if `byte_len` is a multiple of 4 and does not exceed 1 MiB.
#[must_use]
pub fn validate_write_set_summary_raw_len(byte_len: usize) -> bool {
    byte_len % 4 == 0 && byte_len <= WIRE_WRITE_SET_MAX_BYTES
}

/// Validate total witness + edge counts do not exceed wire cap.
#[must_use]
pub fn validate_witness_edge_counts(
    read_w: usize,
    write_w: usize,
    edges: usize,
    merges: usize,
) -> bool {
    read_w
        .saturating_add(write_w)
        .saturating_add(edges)
        .saturating_add(merges)
        <= WIRE_WITNESS_EDGE_MAX
}

// ---------------------------------------------------------------------------
// PermitManager (§5.9.0 reserve/submit discipline)
// ---------------------------------------------------------------------------

/// Error from permit operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermitError {
    /// All permit slots are in use.
    Busy,
    /// Permit ID was not found (expired or never issued).
    NotFound(u64),
    /// Permit was already consumed by a prior SUBMIT.
    AlreadyConsumed(u64),
}

impl fmt::Display for PermitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => f.write_str("max outstanding permits reached"),
            Self::NotFound(id) => write!(f, "permit {id} not found"),
            Self::AlreadyConsumed(id) => write!(f, "permit {id} already consumed"),
        }
    }
}

impl std::error::Error for PermitError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermitState {
    Reserved,
    Consumed,
}

/// Manages the two-phase reserve/submit permit lifecycle.
///
/// Permits are connection-scoped, single-use capabilities. The coordinator
/// enforces a maximum of [`MAX_OUTSTANDING_PERMITS`] active permits.
pub struct PermitManager {
    max_permits: usize,
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, PermitState>>,
}

impl PermitManager {
    /// Create a new permit manager with the given maximum.
    #[must_use]
    pub fn new(max_permits: usize) -> Self {
        Self {
            max_permits,
            next_id: AtomicU64::new(1),
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Reserve a new permit. Returns `Err(Busy)` if at capacity.
    pub fn reserve(&self) -> Result<u64, PermitError> {
        let mut active = self.active.lock();
        let outstanding = active
            .values()
            .filter(|s| **s == PermitState::Reserved)
            .count();
        if outstanding >= self.max_permits {
            drop(active);
            return Err(PermitError::Busy);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        active.insert(id, PermitState::Reserved);
        drop(active);
        Ok(id)
    }

    /// Consume a permit (SUBMIT). Returns `Err` if not found or already consumed.
    pub fn consume(&self, permit_id: u64) -> Result<(), PermitError> {
        let mut active = self.active.lock();
        let res = match active.get(&permit_id) {
            None => Err(PermitError::NotFound(permit_id)),
            Some(PermitState::Consumed) => Err(PermitError::AlreadyConsumed(permit_id)),
            Some(PermitState::Reserved) => {
                active.insert(permit_id, PermitState::Consumed);
                Ok(())
            }
        };
        drop(active);
        res
    }

    /// Release a permit (connection drop without SUBMIT).
    pub fn release(&self, permit_id: u64) {
        let mut active = self.active.lock();
        active.remove(&permit_id);
    }

    /// Number of currently outstanding (reserved, not yet consumed) permits.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        let active = self.active.lock();
        active
            .values()
            .filter(|s| **s == PermitState::Reserved)
            .count()
    }

    /// Garbage-collect consumed permits.
    pub fn gc_consumed(&self) {
        let mut active = self.active.lock();
        active.retain(|_, s| *s == PermitState::Reserved);
    }
}

// ---------------------------------------------------------------------------
// IdempotencyCache (§5.9.0)
// ---------------------------------------------------------------------------

/// Caches terminal responses keyed by `(txn_id, txn_epoch)`.
///
/// If a SUBMIT arrives with a `TxnToken` that has already produced a terminal
/// response, the cached response is returned without re-executing.
pub struct IdempotencyCache {
    inner: Mutex<HashMap<(u64, u32), Vec<u8>>>,
}

impl IdempotencyCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Look up a cached terminal response.
    #[must_use]
    pub fn get(&self, txn_id: u64, txn_epoch: u32) -> Option<Vec<u8>> {
        let cache = self.inner.lock();
        cache.get(&(txn_id, txn_epoch)).cloned()
    }

    /// Store a terminal response for future idempotent lookups.
    pub fn insert(&self, txn_id: u64, txn_epoch: u32, response: Vec<u8>) {
        let mut cache = self.inner.lock();
        cache.insert((txn_id, txn_epoch), response);
    }

    /// Number of cached entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

impl Default for IdempotencyCache {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Peer authentication (§5.9.0)
// ---------------------------------------------------------------------------

/// Error from peer authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerAuthError {
    /// Could not retrieve peer credentials.
    NoCreds,
    /// Peer UID does not match expected UID.
    UidMismatch { expected: u32, actual: u32 },
}

impl fmt::Display for PeerAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCreds => f.write_str("could not retrieve peer credentials"),
            Self::UidMismatch { expected, actual } => {
                write!(f, "UID mismatch: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for PeerAuthError {}

/// Authenticate a Unix domain socket peer by UID.
///
/// Uses the nightly `UnixStream::peer_cred()` API (gated by the crate-level
/// `peer_credentials_unix_socket` feature) to retrieve the peer UID and compare
/// it to the expected UID.
#[cfg(target_family = "unix")]
pub fn authenticate_peer(
    stream: &std::os::unix::net::UnixStream,
    expected_uid: u32,
) -> Result<(), PeerAuthError> {
    let cred = stream.peer_cred().map_err(|_| PeerAuthError::NoCreds)?;
    let actual_uid = cred.uid;
    if actual_uid != expected_uid {
        return Err(PeerAuthError::UidMismatch {
            expected: expected_uid,
            actual: actual_uid,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// File descriptor passing helpers (§5.9.0 bulk payload transfer)
// ---------------------------------------------------------------------------

/// A received file descriptor that is closed on drop.
///
/// We intentionally keep this as a raw fd and close it via `nix` in `Drop` to
/// avoid `unsafe` `FromRawFd` conversions.
#[cfg(target_family = "unix")]
#[derive(Debug)]
pub struct ReceivedFd(std::os::unix::io::RawFd);

#[cfg(target_family = "unix")]
impl ReceivedFd {
    #[must_use]
    pub fn raw_fd(&self) -> std::os::unix::io::RawFd {
        self.0
    }
}

#[cfg(target_family = "unix")]
impl Drop for ReceivedFd {
    fn drop(&mut self) {
        // Best-effort close; ignore errors since we can't recover meaningfully here.
        let _ = nix::unistd::close(self.0);
    }
}

/// Send raw bytes plus a file descriptor over a Unix stream.
#[cfg(target_family = "unix")]
pub fn send_with_fd(
    stream: &std::os::unix::net::UnixStream,
    data: &[u8],
    fd: std::os::unix::io::RawFd,
) -> std::io::Result<usize> {
    use std::io::IoSlice;
    use std::io::Write as _;
    use std::os::unix::net::SocketAncillary;

    if data.is_empty() {
        return Err(std::io::Error::other(
            "cannot send an fd with an empty data payload",
        ));
    }

    let mut ancillary_buf = [0u8; 128];
    let mut ancillary = SocketAncillary::new(&mut ancillary_buf);
    if !ancillary.add_fds(&[fd]) {
        return Err(std::io::Error::other("ancillary buffer too small for fd"));
    }

    // The ancillary control message (SCM_RIGHTS) is delivered with the first
    // byte(s) received for this send call; subsequent writes should not repeat
    // it. We therefore send once with ancillary, then (if needed) write the
    // remaining bytes without ancillary.
    let mut sent = stream.send_vectored_with_ancillary(&[IoSlice::new(data)], &mut ancillary)?;
    if sent == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "send_vectored_with_ancillary wrote 0 bytes",
        ));
    }
    if sent < data.len() {
        (&*stream).write_all(&data[sent..])?;
        sent = data.len();
    }
    Ok(sent)
}

/// Receive raw bytes plus a file descriptor from a Unix stream.
///
/// Returns `(bytes_read, Option<ReceivedFd>)`.
#[cfg(target_family = "unix")]
pub fn recv_with_fd(
    stream: &std::os::unix::net::UnixStream,
    buf: &mut [u8],
) -> std::io::Result<(usize, Option<ReceivedFd>)> {
    use std::io::IoSliceMut;
    use std::os::unix::net::{AncillaryData, SocketAncillary};

    let mut ancillary_buf = [0u8; 128];
    let mut ancillary = SocketAncillary::new(&mut ancillary_buf);

    let mut iov = [IoSliceMut::new(buf)];
    let n = stream.recv_vectored_with_ancillary(&mut iov, &mut ancillary)?;

    if ancillary.truncated() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ancillary data truncated",
        ));
    }

    let mut fds = Vec::<std::os::unix::io::RawFd>::new();
    for msg in ancillary.messages() {
        let msg = msg.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ancillary message decode error: {e:?}"),
            )
        })?;
        if let AncillaryData::ScmRights(scm_rights) = msg {
            fds.extend(scm_rights);
        }
    }

    match fds.len() {
        0 => Ok((n, None)),
        1 => Ok((n, Some(ReceivedFd(fds[0])))),
        _ => {
            // Close all received fds to avoid leaking them.
            for fd in fds {
                let _ = nix::unistd::close(fd);
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "received more than one fd",
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_family = "unix")]
    fn recv_frame_with_optional_fd(
        stream: &std::os::unix::net::UnixStream,
    ) -> std::io::Result<(Frame, Option<ReceivedFd>)> {
        use std::io::ErrorKind;

        let mut header = [0u8; FRAME_HEADER_WIRE_BYTES];
        let mut filled = 0usize;
        let mut fd: Option<ReceivedFd> = None;

        while filled < FRAME_HEADER_WIRE_BYTES {
            let (n, maybe_fd) = recv_with_fd(stream, &mut header[filled..])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "eof while reading frame header",
                ));
            }
            filled += n;
            if let Some(new_fd) = maybe_fd {
                if fd.is_some() {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "received multiple fds for a single frame",
                    ));
                }
                fd = Some(new_fd);
            }
        }

        let len_be = read_u32_be(&header[..4])
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, "missing frame length"))?;
        if len_be < FRAME_MIN_LEN_BE {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("len_be {len_be} below minimum {FRAME_MIN_LEN_BE}"),
            ));
        }
        if len_be > FRAME_MAX_LEN_BE {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("len_be {len_be} exceeds cap {FRAME_MAX_LEN_BE}"),
            ));
        }

        // Total wire length includes the 4-byte len field.
        let total_len = 4usize + len_be as usize;
        let mut wire = vec![0u8; total_len];
        wire[..FRAME_HEADER_WIRE_BYTES].copy_from_slice(&header);

        let mut pos = FRAME_HEADER_WIRE_BYTES;
        while pos < total_len {
            let (n, maybe_fd) = recv_with_fd(stream, &mut wire[pos..])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "eof while reading frame payload",
                ));
            }
            pos += n;
            if let Some(new_fd) = maybe_fd {
                if fd.is_some() {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "received multiple fds for a single frame",
                    ));
                }
                fd = Some(new_fd);
            }
        }

        let frame = Frame::decode(&wire)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok((frame, fd))
    }

    // -- bd-1m07 test 1: Frame round-trip encode/decode for all 7 kinds --
    #[test]
    fn test_frame_round_trip() {
        for kind in [
            MessageKind::Reserve,
            MessageKind::SubmitNativePublish,
            MessageKind::SubmitWalCommit,
            MessageKind::RowidReserve,
            MessageKind::Response,
            MessageKind::Ping,
            MessageKind::Pong,
        ] {
            let original = Frame {
                kind,
                request_id: 0xDEAD_BEEF_CAFE_BABE,
                payload: vec![1, 2, 3, 4, 5],
            };
            let wire = original.encode();
            let decoded = Frame::decode(&wire).expect("decode must succeed");
            assert_eq!(decoded.kind, original.kind, "kind mismatch for {kind}");
            assert_eq!(
                decoded.request_id, original.request_id,
                "request_id mismatch for {kind}"
            );
            assert_eq!(
                decoded.payload, original.payload,
                "payload mismatch for {kind}"
            );
        }

        // Empty payload frame (Ping/Pong typical use).
        let ping = Frame {
            kind: MessageKind::Ping,
            request_id: 42,
            payload: vec![],
        };
        let wire = ping.encode();
        assert_eq!(wire.len(), FRAME_HEADER_WIRE_BYTES); // 16 bytes, no payload
        let decoded = Frame::decode(&wire).expect("decode empty payload");
        assert_eq!(decoded.kind, MessageKind::Ping);
        assert!(decoded.payload.is_empty());

        // Reserve payload round-trip through Frame.
        let reserve = ReservePayload {
            purpose: 0,
            txn: WireTxnToken {
                txn_id: 100,
                txn_epoch: 3,
            },
        };
        let frame = Frame {
            kind: MessageKind::Reserve,
            request_id: 7,
            payload: reserve.to_bytes(),
        };
        let wire = frame.encode();
        let decoded = Frame::decode(&wire).expect("decode reserve frame");
        let parsed = ReservePayload::from_bytes(&decoded.payload).expect("parse reserve payload");
        assert_eq!(parsed, reserve);
    }

    // -- bd-1m07 test 2: Frame validation rejects malformed input --
    #[test]
    fn test_frame_validation() {
        // Too short (< 16 bytes).
        assert_eq!(Frame::decode(&[0u8; 4]), Err(FrameError::TooShort));

        // len_be too small (< 12).
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&5_u32.to_be_bytes()); // len_be = 5
        assert_eq!(Frame::decode(&buf), Err(FrameError::LenTooSmall(5)));

        // len_be too large (> 4 MiB).
        buf[..4].copy_from_slice(&(5_000_000_u32).to_be_bytes());
        assert_eq!(Frame::decode(&buf), Err(FrameError::LenTooLarge(5_000_000)));

        // Unknown version.
        let bad_version = Frame {
            kind: MessageKind::Ping,
            request_id: 0,
            payload: vec![],
        };
        let mut wire = bad_version.encode();
        wire[4..6].copy_from_slice(&99_u16.to_be_bytes()); // corrupt version
        assert_eq!(Frame::decode(&wire), Err(FrameError::UnknownVersion(99)));

        // Unknown kind.
        let mut wire = bad_version.encode();
        wire[6..8].copy_from_slice(&255_u16.to_be_bytes()); // corrupt kind
        assert_eq!(Frame::decode(&wire), Err(FrameError::UnknownKind(255)));

        // Payload truncated: len_be says 20 (= 12 + 8 payload) but only 4 payload bytes.
        let mut wire = vec![0u8; 20]; // 16 header + 4 payload
        wire[..4].copy_from_slice(&20_u32.to_be_bytes()); // len_be = 20, needs 8 payload bytes
        wire[4..6].copy_from_slice(&1_u16.to_be_bytes()); // version 1
        wire[6..8].copy_from_slice(&6_u16.to_be_bytes()); // kind = Ping
        assert_eq!(
            Frame::decode(&wire),
            Err(FrameError::PayloadTruncated {
                expected: 8,
                actual: 4
            })
        );
    }

    // -- bd-1m07 test 3: Reserve/submit discipline lifecycle --
    #[test]
    fn test_reserve_submit_discipline() {
        let pm = PermitManager::new(MAX_OUTSTANDING_PERMITS);

        // Reserve a permit.
        let p1 = pm.reserve().expect("first reserve");
        assert_eq!(pm.outstanding(), 1);

        // Consume it (SUBMIT).
        pm.consume(p1).expect("consume p1");
        assert_eq!(pm.outstanding(), 0);

        // Reserve another, then release without consuming (connection drop).
        let p2 = pm.reserve().expect("second reserve");
        assert_eq!(pm.outstanding(), 1);
        pm.release(p2);
        assert_eq!(pm.outstanding(), 0);

        // Full lifecycle: reserve → consume → gc.
        let p3 = pm.reserve().expect("third reserve");
        pm.consume(p3).expect("consume p3");
        pm.gc_consumed();
        // After GC, no entries remain.
        assert_eq!(pm.outstanding(), 0);
    }

    // -- bd-1m07 test 4: Permit is single-use --
    #[test]
    fn test_permit_single_use() {
        let pm = PermitManager::new(MAX_OUTSTANDING_PERMITS);
        let p = pm.reserve().expect("reserve");
        pm.consume(p).expect("first consume");

        // Second consume must fail.
        assert_eq!(pm.consume(p), Err(PermitError::AlreadyConsumed(p)));

        // Unknown permit must fail.
        assert_eq!(pm.consume(999), Err(PermitError::NotFound(999)));
    }

    // -- bd-1m07 test 5: Idempotency cache returns same response --
    #[test]
    fn test_idempotency() {
        let cache = IdempotencyCache::new();
        let txn = WireTxnToken {
            txn_id: 42,
            txn_epoch: 1,
        };

        // First lookup: miss.
        assert!(cache.get(txn.txn_id, txn.txn_epoch).is_none());

        // Insert terminal response.
        let response = ReserveResponse::Ok { permit_id: 77 }.to_bytes();
        cache.insert(txn.txn_id, txn.txn_epoch, response.clone());

        // Second lookup: hit with identical bytes.
        let cached = cache.get(txn.txn_id, txn.txn_epoch).expect("cache hit");
        assert_eq!(cached, response);

        // Different txn_epoch: miss.
        assert!(cache.get(txn.txn_id, txn.txn_epoch + 1).is_none());

        // Insert a second response for a different token.
        let resp2 = ReserveResponse::Busy {
            retry_after_ms: 100,
        }
        .to_bytes();
        cache.insert(99, 2, resp2.clone());
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(99, 2).expect("second hit"), resp2);
    }

    // -- bd-1m07 test 6: Peer auth rejects wrong UID --
    #[cfg(target_family = "unix")]
    #[test]
    fn test_peer_auth_rejects_wrong_uid() {
        use std::os::unix::net::UnixStream;

        let (a, _b) = UnixStream::pair().expect("socketpair");

        let actual_uid = a.peer_cred().expect("peer_cred").uid;
        authenticate_peer(&a, actual_uid).expect("peer auth ok");

        let wrong_uid = actual_uid ^ 1;
        assert_eq!(
            authenticate_peer(&a, wrong_uid),
            Err(PeerAuthError::UidMismatch {
                expected: wrong_uid,
                actual: actual_uid,
            })
        );
    }

    // -- bd-1m07 test 7: SCM_RIGHTS fd passing --
    #[cfg(target_family = "unix")]
    #[test]
    fn test_scm_rights_fd_passing() {
        use std::io::Write;
        use std::io::pipe;
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;

        let (sender, receiver) = UnixStream::pair().expect("socketpair");

        let (pipe_r, mut pipe_w) = pipe().expect("pipe");

        // Send bytes with a real fd attached.
        let data = b"fd-marker";
        let sent = send_with_fd(&sender, data, pipe_r.as_raw_fd()).expect("send_with_fd");
        assert_eq!(sent, data.len());
        drop(pipe_r);

        // Receive bytes + fd.
        let mut buf = [0u8; 64];
        let (n, maybe_fd) = recv_with_fd(&receiver, &mut buf).expect("recv_with_fd");
        assert_eq!(&buf[..n], data);
        let recv_fd = maybe_fd.expect("fd must be attached");

        // Prove the received fd is usable by reading from it after writing into the original pipe.
        let payload = b"pipe-data";
        pipe_w.write_all(payload).expect("write into pipe");

        let mut out = [0u8; 64];
        let nr = nix::unistd::read(recv_fd.raw_fd(), &mut out).expect("read from received fd");
        assert_eq!(&out[..nr], payload);
    }

    // -- bd-1m07 test 8: Canonical ordering validation --
    #[test]
    fn test_canonical_ordering() {
        // Pages: sorted ascending, no dupes.
        assert!(is_canonical_pages(&[]));
        assert!(is_canonical_pages(&[1]));
        assert!(is_canonical_pages(&[1, 2, 3]));
        assert!(!is_canonical_pages(&[2, 1])); // not sorted
        assert!(!is_canonical_pages(&[1, 1])); // duplicate

        // ObjectIds: sorted lexicographically, no dupes.
        let a = ObjectId::from_bytes([0u8; 16]);
        let b = ObjectId::from_bytes([1u8; 16]);
        let c = ObjectId::from_bytes([2u8; 16]);
        assert!(is_canonical_object_ids(&[]));
        assert!(is_canonical_object_ids(&[a]));
        assert!(is_canonical_object_ids(&[a, b, c]));
        assert!(!is_canonical_object_ids(&[b, a])); // not sorted
        assert!(!is_canonical_object_ids(&[a, a])); // duplicate

        // Mixed first byte: [0,0,...] < [1,0,...] lexicographically.
        let mut mixed_low = [0u8; 16];
        mixed_low[0] = 0;
        mixed_low[1] = 255;
        let mut mixed_high = [0u8; 16];
        mixed_high[0] = 1;
        mixed_high[1] = 0;
        assert!(is_canonical_object_ids(&[
            ObjectId::from_bytes(mixed_low),
            ObjectId::from_bytes(mixed_high),
        ]));

        // Write set summary caps.
        assert!(validate_write_set_summary(&[1, 2, 3])); // 12 bytes, fine
        assert!(validate_write_set_summary(&[])); // 0 bytes, fine

        // Witness + edge counts.
        assert!(validate_witness_edge_counts(10_000, 10_000, 10_000, 10_000));
        assert!(!validate_witness_edge_counts(30_000, 30_000, 5_000, 537));
    }

    // -- bd-1m07 test 9: Backpressure — 17th concurrent reserve returns BUSY --
    #[test]
    fn test_backpressure_busy() {
        let pm = PermitManager::new(MAX_OUTSTANDING_PERMITS);
        let mut permits = Vec::with_capacity(MAX_OUTSTANDING_PERMITS);

        // Fill all 16 slots.
        for i in 0..MAX_OUTSTANDING_PERMITS {
            permits.push(
                pm.reserve()
                    .unwrap_or_else(|_| unreachable!("reserve #{i}")),
            );
        }
        assert_eq!(pm.outstanding(), MAX_OUTSTANDING_PERMITS);

        // 17th must be rejected.
        assert_eq!(pm.reserve(), Err(PermitError::Busy));

        // Release one → can reserve again.
        pm.release(permits[0]);
        assert_eq!(pm.outstanding(), MAX_OUTSTANDING_PERMITS - 1);
        let p17 = pm.reserve().expect("reserve after release");
        assert_eq!(pm.outstanding(), MAX_OUTSTANDING_PERMITS);

        // Consume the newly reserved permit to verify it works.
        pm.consume(p17).expect("consume p17");
    }

    // -- bd-1m07 E2E: Full protocol exercise over UnixStream --
    #[cfg(target_family = "unix")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_bd_1m07() {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::{Arc, Barrier};

        let pm = Arc::new(PermitManager::new(MAX_OUTSTANDING_PERMITS));
        let cache = Arc::new(IdempotencyCache::new());
        let barrier = Arc::new(Barrier::new(2));

        let (client_sock, server_sock) = UnixStream::pair().expect("socketpair");

        let expected_uid = server_sock.peer_cred().expect("peer_cred").uid;
        authenticate_peer(&server_sock, expected_uid).expect("E2E peer auth");

        let pm_server = Arc::clone(&pm);
        let cache_server = Arc::clone(&cache);
        let barrier_server = Arc::clone(&barrier);

        // Server thread: read frames, process reserve/submit.
        let server = std::thread::spawn(move || {
            barrier_server.wait();

            // --- Round 1: RESERVE ---
            let (frame, maybe_fd) =
                recv_frame_with_optional_fd(&server_sock).expect("server recv reserve");
            assert!(maybe_fd.is_none(), "reserve must not carry an fd");
            assert_eq!(frame.kind, MessageKind::Reserve);

            let _payload =
                ReservePayload::from_bytes(&frame.payload).expect("parse reserve payload");
            let permit_id = pm_server.reserve().expect("server reserve");

            // Send response.
            let resp = ReserveResponse::Ok { permit_id };
            let resp_frame = Frame {
                kind: MessageKind::Response,
                request_id: frame.request_id,
                payload: resp.to_bytes(),
            };
            (&server_sock)
                .write_all(&resp_frame.encode())
                .expect("server write reserve response");

            // --- Round 2: SUBMIT_WAL_COMMIT ---
            let (frame, maybe_fd) =
                recv_frame_with_optional_fd(&server_sock).expect("server recv submit");
            let _spill_fd = maybe_fd.expect("submit must carry spill fd");
            assert_eq!(frame.kind, MessageKind::SubmitWalCommit);

            let wal_payload =
                SubmitWalPayload::from_bytes(&frame.payload).expect("parse wal payload");
            assert_eq!(wal_payload.permit_id, permit_id);

            // Check idempotency cache.
            let key = wal_payload.txn.idempotency_key();
            if let Some(cached) = cache_server.get(key.0, key.1) {
                // Return cached response.
                let resp_frame = Frame {
                    kind: MessageKind::Response,
                    request_id: frame.request_id,
                    payload: cached,
                };
                (&server_sock)
                    .write_all(&resp_frame.encode())
                    .expect("server write cached");
            } else {
                pm_server.consume(permit_id).expect("consume permit");
                let commit_resp = WalCommitResponse::Ok { commit_seq: 42 };
                let resp_bytes = commit_resp.to_bytes();
                cache_server.insert(key.0, key.1, resp_bytes.clone());
                let resp_frame = Frame {
                    kind: MessageKind::Response,
                    request_id: frame.request_id,
                    payload: resp_bytes,
                };
                (&server_sock)
                    .write_all(&resp_frame.encode())
                    .expect("server write commit response");
            }

            // --- Round 3: Duplicate SUBMIT (idempotency) ---
            let (frame, maybe_fd) =
                recv_frame_with_optional_fd(&server_sock).expect("server recv dup");
            let _spill_fd = maybe_fd.expect("dup submit must carry spill fd");
            let wal_payload =
                SubmitWalPayload::from_bytes(&frame.payload).expect("parse dup payload");
            let key = wal_payload.txn.idempotency_key();
            let cached = cache_server
                .get(key.0, key.1)
                .expect("idempotency cache must hit");
            let resp_frame = Frame {
                kind: MessageKind::Response,
                request_id: frame.request_id,
                payload: cached,
            };
            (&server_sock)
                .write_all(&resp_frame.encode())
                .expect("server write dup response");

            // --- Round 4: PING ---
            let (frame, maybe_fd) =
                recv_frame_with_optional_fd(&server_sock).expect("server recv ping");
            assert!(maybe_fd.is_none(), "ping must not carry an fd");
            assert_eq!(frame.kind, MessageKind::Ping);
            let pong = Frame {
                kind: MessageKind::Pong,
                request_id: frame.request_id,
                payload: vec![],
            };
            (&server_sock)
                .write_all(&pong.encode())
                .expect("server write pong");
        });

        // Client side.
        barrier.wait();

        // Round 1: RESERVE.
        let txn = WireTxnToken {
            txn_id: 1,
            txn_epoch: 1,
        };
        let reserve = Frame {
            kind: MessageKind::Reserve,
            request_id: 1,
            payload: ReservePayload { purpose: 0, txn }.to_bytes(),
        };
        (&client_sock)
            .write_all(&reserve.encode())
            .expect("client write reserve");

        let (resp, resp_fd) =
            recv_frame_with_optional_fd(&client_sock).expect("client read reserve resp");
        assert!(resp_fd.is_none(), "reserve response must not carry an fd");
        assert_eq!(resp.kind, MessageKind::Response);
        let reserve_resp =
            ReserveResponse::from_bytes(&resp.payload).expect("parse reserve response");
        let permit_id = match reserve_resp {
            ReserveResponse::Ok { permit_id } => permit_id,
            other => unreachable!("expected Ok, got {other:?}"),
        };

        // Build a WAL commit payload.
        let wal = SubmitWalPayload {
            permit_id,
            txn,
            mode: 0,
            snapshot_high: 10,
            schema_epoch: 1,
            has_in_rw: false,
            has_out_rw: false,
            wal_fec_r: 0,
            spill_pages: vec![SpillPageEntry {
                pgno: 1,
                offset: 0,
                len: 4096,
                xxh3_64: 0xABCD,
            }],
            read_witness_refs: vec![],
            write_witness_refs: vec![],
            edge_refs: vec![],
            merge_refs: vec![],
        };

        let (_spill_r, spill_w) = std::io::pipe().expect("spill pipe");

        // Round 2: SUBMIT_WAL_COMMIT (with spill fd attached).
        let submit = Frame {
            kind: MessageKind::SubmitWalCommit,
            request_id: 2,
            payload: wal.to_bytes(),
        };
        send_with_fd(&client_sock, &submit.encode(), spill_w.as_raw_fd())
            .expect("client send submit");

        let (resp, resp_fd) =
            recv_frame_with_optional_fd(&client_sock).expect("client read commit resp");
        assert!(resp_fd.is_none(), "commit response must not carry an fd");
        let commit_resp =
            WalCommitResponse::from_bytes(&resp.payload).expect("parse commit response");
        assert_eq!(commit_resp, WalCommitResponse::Ok { commit_seq: 42 });

        // Round 3: Duplicate SUBMIT (test idempotency from client side).
        let dup_submit = Frame {
            kind: MessageKind::SubmitWalCommit,
            request_id: 3,
            payload: wal.to_bytes(),
        };
        send_with_fd(&client_sock, &dup_submit.encode(), spill_w.as_raw_fd())
            .expect("client send dup submit");

        let (resp, resp_fd) =
            recv_frame_with_optional_fd(&client_sock).expect("client read dup resp");
        assert!(resp_fd.is_none(), "dup response must not carry an fd");
        let dup_resp =
            WalCommitResponse::from_bytes(&resp.payload).expect("parse dup commit response");
        assert_eq!(
            dup_resp,
            WalCommitResponse::Ok { commit_seq: 42 },
            "idempotent response must match"
        );

        // Round 4: PING/PONG.
        let ping = Frame {
            kind: MessageKind::Ping,
            request_id: 4,
            payload: vec![],
        };
        (&client_sock)
            .write_all(&ping.encode())
            .expect("client write ping");

        let (resp, resp_fd) = recv_frame_with_optional_fd(&client_sock).expect("client read pong");
        assert!(resp_fd.is_none(), "pong must not carry an fd");
        assert_eq!(resp.kind, MessageKind::Pong);
        assert_eq!(resp.request_id, 4);

        server.join().expect("server thread");
    }

    // ===================================================================
    // bd-3ipx — Wire Payload Schemas (§5.9.0.1)
    // ===================================================================

    #[test]
    fn test_reserve_v1_roundtrip() {
        let original = ReservePayload {
            purpose: 0, // NativePublish
            txn: WireTxnToken {
                txn_id: 0xDEAD_BEEF_0000_0001,
                txn_epoch: 7,
            },
        };
        let bytes = original.to_bytes();
        assert_eq!(bytes.len(), 24, "ReserveV1 must be exactly 24 bytes");

        // Verify pad bytes are zero.
        assert_eq!(&bytes[1..8], &[0u8; 7], "pad0 must be zero");

        let decoded = ReservePayload::from_bytes(&bytes).expect("decode must succeed");
        assert_eq!(decoded, original, "round-trip mismatch");

        // Also test purpose=1 (WalCommit).
        let wal_reserve = ReservePayload {
            purpose: 1,
            txn: WireTxnToken {
                txn_id: 42,
                txn_epoch: 0,
            },
        };
        let bytes2 = wal_reserve.to_bytes();
        assert_eq!(bytes2.len(), 24);
        assert_eq!(bytes2[0], 1, "purpose must be 1");
        let decoded2 = ReservePayload::from_bytes(&bytes2).expect("decode purpose=1");
        assert_eq!(decoded2, wal_reserve);
    }

    #[test]
    fn test_reserve_resp_tagged_union_variants() {
        // Ok variant: tag(1) + pad(7) + permit_id(8) = 16.
        let ok = ReserveResponse::Ok { permit_id: 0xCAFE };
        let ok_bytes = ok.to_bytes();
        assert_eq!(ok_bytes[0], 0, "Ok tag = 0");
        assert_eq!(&ok_bytes[1..8], &[0u8; 7], "pad0 must be zero");
        let ok_rt = ReserveResponse::from_bytes(&ok_bytes).expect("decode Ok");
        assert_eq!(ok_rt, ok);

        // Busy variant: tag(1) + pad(7) + retry_after_ms(4) + pad1(4) = 16.
        let busy = ReserveResponse::Busy {
            retry_after_ms: 500,
        };
        let busy_bytes = busy.to_bytes();
        assert_eq!(busy_bytes[0], 1, "Busy tag = 1");
        assert_eq!(&busy_bytes[1..8], &[0u8; 7], "pad0 must be zero");
        let busy_rt = ReserveResponse::from_bytes(&busy_bytes).expect("decode Busy");
        assert_eq!(busy_rt, busy);

        // Err variant: tag(1) + pad(7) + code(4) = 12.
        let err = ReserveResponse::Err { code: 0x07 };
        let err_bytes = err.to_bytes();
        assert_eq!(err_bytes[0], 2, "Err tag = 2");
        assert_eq!(&err_bytes[1..8], &[0u8; 7], "pad0 must be zero");
        let err_rt = ReserveResponse::from_bytes(&err_bytes).expect("decode Err");
        assert_eq!(err_rt, err);

        // Verify unknown tag is rejected.
        let mut bad = ok_bytes;
        bad[0] = 99;
        assert!(
            ReserveResponse::from_bytes(&bad).is_none(),
            "unknown tag must be rejected"
        );
    }

    #[test]
    fn test_write_set_summary_canonical_encoding() {
        // Input in arbitrary order.
        let mut pages = vec![5_u32, 1, 100, 3];
        pages.sort_unstable();
        assert!(is_canonical_pages(&pages), "sorted pages must be canonical");
        assert_eq!(pages, [1, 3, 5, 100]);

        // Encode as raw bytes.
        let mut bytes = Vec::new();
        for &p in &pages {
            append_u32_le(&mut bytes, p);
        }
        assert_eq!(bytes.len(), 16, "4 pages × 4 bytes = 16");
        assert!(
            validate_write_set_summary_raw_len(bytes.len()),
            "16 bytes must be valid"
        );

        // Decode back.
        let mut decoded = Vec::new();
        for i in 0..4 {
            let off = i * 4;
            decoded.push(read_u32_le(&bytes[off..off + 4]).unwrap());
        }
        assert_eq!(decoded, pages, "round-trip mismatch");
    }

    #[test]
    fn test_write_set_summary_len_not_multiple_of_4_rejected() {
        assert!(
            !validate_write_set_summary_raw_len(7),
            "7 bytes is not a multiple of 4 — must be rejected"
        );
        assert!(
            !validate_write_set_summary_raw_len(1),
            "1 byte is not a multiple of 4"
        );
        assert!(
            !validate_write_set_summary_raw_len(5),
            "5 bytes is not a multiple of 4"
        );
        // Valid multiples of 4.
        assert!(validate_write_set_summary_raw_len(0), "0 is valid");
        assert!(validate_write_set_summary_raw_len(4), "4 is valid");
        assert!(validate_write_set_summary_raw_len(8), "8 is valid");
    }

    #[test]
    fn test_native_publish_conflict_response_page_list() {
        let conflict = NativePublishResponse::Conflict {
            pages: vec![10, 42, 99],
            reason: 1,
        };
        let bytes = conflict.to_bytes();
        assert_eq!(bytes[0], 1, "Conflict tag = 1");

        let decoded = NativePublishResponse::from_bytes(&bytes).expect("decode Conflict response");
        assert_eq!(decoded, conflict, "round-trip mismatch");

        // Verify page list is intact.
        if let NativePublishResponse::Conflict { pages, reason } = &decoded {
            assert_eq!(pages.len(), 3);
            assert_eq!(pages[0], 10);
            assert_eq!(pages[1], 42);
            assert_eq!(pages[2], 99);
            assert_eq!(*reason, 1);
        } else {
            unreachable!("must be Conflict variant");
        }
    }

    #[test]
    fn test_wire_size_cap_write_set_summary_exceeds_1mib() {
        // 262,144 pages = exactly 1 MiB of u32_le → accepted.
        let max_pages: Vec<u32> = (0..262_144).collect();
        assert!(
            validate_write_set_summary(&max_pages),
            "262,144 pages (1 MiB) must be accepted"
        );

        // 262,145 pages = just over 1 MiB → rejected.
        let over_pages: Vec<u32> = (0..262_145).collect();
        assert!(
            !validate_write_set_summary(&over_pages),
            "262,145 pages (> 1 MiB) must be rejected"
        );

        // Raw byte-level check.
        assert!(
            validate_write_set_summary_raw_len(WIRE_WRITE_SET_MAX_BYTES),
            "exactly 1 MiB must pass"
        );
        assert!(
            !validate_write_set_summary_raw_len(WIRE_WRITE_SET_MAX_BYTES + 4),
            "1 MiB + 4 must fail"
        );
    }

    #[test]
    fn test_wire_size_cap_total_witness_count_exceeds_65536() {
        // Exactly at limit: 65,536 total → accepted.
        assert!(
            validate_witness_edge_counts(16_384, 16_384, 16_384, 16_384),
            "65,536 total must be accepted"
        );

        // Over limit: 65,537 total → rejected.
        assert!(
            !validate_witness_edge_counts(16_384, 16_384, 16_384, 16_385),
            "65,537 total must be rejected"
        );

        // Edge case: all in one array.
        assert!(validate_witness_edge_counts(65_536, 0, 0, 0));
        assert!(!validate_witness_edge_counts(65_537, 0, 0, 0));
    }

    #[test]
    fn test_wal_commit_spill_page_encoding() {
        let sp1 = SpillPageEntry {
            pgno: 1,
            offset: 4096,
            len: 4096,
            xxh3_64: 0x1234_5678_ABCD_EF01,
        };
        let sp2 = SpillPageEntry {
            pgno: 5,
            offset: 8192,
            len: 4096,
            xxh3_64: 0xFEDC_BA98_7654_3210,
        };
        let sp3 = SpillPageEntry {
            pgno: 10,
            offset: 12288,
            len: 4096,
            xxh3_64: 0,
        };

        // Each SpillPageEntry is exactly 32 bytes.
        assert_eq!(
            sp1.to_bytes().len(),
            32,
            "SpillPageV1 must be exactly 32 bytes"
        );

        // Round-trip each.
        let sp1_rt = SpillPageEntry::from_bytes(&sp1.to_bytes()).unwrap();
        assert_eq!(sp1_rt, sp1);
        let sp2_rt = SpillPageEntry::from_bytes(&sp2.to_bytes()).unwrap();
        assert_eq!(sp2_rt, sp2);
        let sp3_rt = SpillPageEntry::from_bytes(&sp3.to_bytes()).unwrap();
        assert_eq!(sp3_rt, sp3);

        // Round-trip via SubmitWalPayload with 3 spill pages.
        let wal = SubmitWalPayload {
            permit_id: 42,
            txn: WireTxnToken {
                txn_id: 100,
                txn_epoch: 1,
            },
            mode: 1, // Concurrent
            snapshot_high: 999,
            schema_epoch: 5,
            has_in_rw: true,
            has_out_rw: false,
            wal_fec_r: 3,
            spill_pages: vec![sp1, sp2, sp3],
            read_witness_refs: vec![],
            write_witness_refs: vec![],
            edge_refs: vec![],
            merge_refs: vec![],
        };
        let bytes = wal.to_bytes();
        let decoded = SubmitWalPayload::from_bytes(&bytes).expect("decode WAL payload");
        assert_eq!(decoded.spill_pages.len(), 3);
        assert_eq!(decoded.spill_pages[0], sp1);
        assert_eq!(decoded.spill_pages[1], sp2);
        assert_eq!(decoded.spill_pages[2], sp3);
        assert_eq!(decoded.schema_epoch, 5);
        assert!(decoded.has_in_rw);
        assert!(!decoded.has_out_rw);
    }

    #[test]
    fn test_rowid_reserve_roundtrip() {
        let original = RowidReservePayload {
            txn: WireTxnToken {
                txn_id: 0xAAAA_BBBB_CCCC_DDDD,
                txn_epoch: 42,
            },
            schema_epoch: 7,
            table_id: 100,
            count: 64,
        };
        let bytes = original.to_bytes();
        assert_eq!(bytes.len(), 32, "RowIdReserveV1 must be exactly 32 bytes");

        let decoded = RowidReservePayload::from_bytes(&bytes).expect("decode must succeed");
        assert_eq!(decoded, original, "round-trip mismatch");

        // Verify field positions.
        assert_eq!(
            read_u64_le(&bytes[0..8]).unwrap(),
            original.txn.txn_id,
            "txn_id at offset 0"
        );
        assert_eq!(
            read_u32_le(&bytes[8..12]).unwrap(),
            original.txn.txn_epoch,
            "txn_epoch at offset 8"
        );
        assert_eq!(
            read_u64_le(&bytes[16..24]).unwrap(),
            original.schema_epoch,
            "schema_epoch at offset 16"
        );
        assert_eq!(
            read_u32_le(&bytes[24..28]).unwrap(),
            original.table_id,
            "table_id at offset 24"
        );
        assert_eq!(
            read_u32_le(&bytes[28..32]).unwrap(),
            original.count,
            "count at offset 28"
        );
    }
}
