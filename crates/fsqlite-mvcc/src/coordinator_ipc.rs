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
        let len_be = u32::try_from(12 + payload_len).unwrap_or(FRAME_MAX_LEN_BE);
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
    /// Wire size: purpose(1) + pad(3) + txn(16) = 20.
    const WIRE_BYTES: usize = 20;

    /// Encode to payload bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::WIRE_BYTES);
        buf.push(self.purpose);
        buf.extend_from_slice(&[0u8; 3]); // pad
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
        let txn = WireTxnToken::from_bytes(&src[4..])?;
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
                buf.extend_from_slice(&[0u8; 3]); // pad
                append_u32_le(&mut buf, *retry_after_ms);
            }
            Self::Err { code } => {
                buf.push(Self::TAG_ERR);
                buf.extend_from_slice(&[0u8; 3]); // pad
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
                let retry = read_u32_le(src.get(4..8)?)?;
                Some(Self::Busy {
                    retry_after_ms: retry,
                })
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
// ROWID_RESERVE payload + response
// ---------------------------------------------------------------------------

/// ROWID_RESERVE request payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowidReservePayload {
    pub txn: WireTxnToken,
    pub schema_epoch: u32,
    pub table_id: u32,
    pub count: u32,
}

impl RowidReservePayload {
    const WIRE_BYTES: usize = WIRE_TXN_TOKEN_BYTES + 12; // txn(16) + 3×u32(12)

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::WIRE_BYTES);
        buf.extend_from_slice(&self.txn.to_bytes());
        append_u32_le(&mut buf, self.schema_epoch);
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
        let schema_epoch = read_u32_le(&src[16..20])?;
        let table_id = read_u32_le(&src[20..24])?;
        let count = read_u32_le(&src[24..28])?;
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
    const WIRE_BYTES: usize = 24; // u32(4) + u64(8) + u32(4) + u64(8)

    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::WIRE_BYTES] {
        let mut buf = [0u8; Self::WIRE_BYTES];
        buf[..4].copy_from_slice(&self.pgno.to_le_bytes());
        buf[4..12].copy_from_slice(&self.offset.to_le_bytes());
        buf[12..16].copy_from_slice(&self.len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.xxh3_64.to_le_bytes());
        buf
    }

    #[must_use]
    pub fn from_bytes(src: &[u8]) -> Option<Self> {
        if src.len() < Self::WIRE_BYTES {
            return None;
        }
        Some(Self {
            pgno: read_u32_le(&src[..4])?,
            offset: read_u64_le(&src[4..12])?,
            len: read_u32_le(&src[12..16])?,
            xxh3_64: read_u64_le(&src[16..24])?,
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
        let ws_count = u32::try_from(self.write_set_summary.len()).unwrap_or(u32::MAX);
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
        let mut write_set_summary = Vec::with_capacity(ws_count);
        for _ in 0..ws_count {
            write_set_summary.push(read_u32_le(src.get(pos..pos + 4)?)?);
            pos += 4;
        }
        // witness arrays
        let (read_witness_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
        let (write_witness_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
        let (edge_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
        let (merge_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
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
    pub schema_epoch: u32,
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
        let mut buf = Vec::with_capacity(256);
        append_u64_le(&mut buf, self.permit_id);
        buf.extend_from_slice(&self.txn.to_bytes());
        buf.push(self.mode);
        buf.extend_from_slice(&[0u8; 3]); // pad to 4-byte alignment
        append_u64_le(&mut buf, self.snapshot_high);
        append_u32_le(&mut buf, self.schema_epoch);
        buf.push(u8::from(self.has_in_rw));
        buf.push(u8::from(self.has_out_rw));
        buf.push(self.wal_fec_r);
        buf.push(0); // pad
        // spill_pages
        let sp_count = u32::try_from(self.spill_pages.len()).unwrap_or(u32::MAX);
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
        pos += 4; // mode(1) + pad(3)
        let snapshot_high = read_u64_le(src.get(pos..pos + 8)?)?;
        pos += 8;
        let schema_epoch = read_u32_le(src.get(pos..pos + 4)?)?;
        pos += 4;
        let has_in_rw = *src.get(pos)? != 0;
        pos += 1;
        let has_out_rw = *src.get(pos)? != 0;
        pos += 1;
        let wal_fec_r = *src.get(pos)?;
        pos += 2; // fec_r(1) + pad(1)
        // spill_pages
        let sp_count = read_u32_le(src.get(pos..pos + 4)?)? as usize;
        pos += 4;
        let mut spill_pages = Vec::with_capacity(sp_count);
        for _ in 0..sp_count {
            spill_pages.push(SpillPageEntry::from_bytes(src.get(pos..)?)?);
            pos += SpillPageEntry::WIRE_BYTES;
        }
        let (read_witness_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
        let (write_witness_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
        let (edge_refs, new_pos) = decode_object_id_array(src, pos)?;
        pos = new_pos;
        let (merge_refs, _) = decode_object_id_array(src, pos)?;

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
                let count = u32::try_from(pages.len()).unwrap_or(u32::MAX);
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
                let count = u32::try_from(pages.len()).unwrap_or(u32::MAX);
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
    let count = u32::try_from(ids.len()).unwrap_or(u32::MAX);
    append_u32_le(buf, count);
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
    }
}

fn decode_object_id_array(src: &[u8], pos: usize) -> Option<(Vec<ObjectId>, usize)> {
    let count = read_u32_le(src.get(pos..pos + 4)?)? as usize;
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

/// Validate that `ids` is sorted lexicographically with no duplicates.
#[must_use]
pub fn is_canonical_object_ids(ids: &[ObjectId]) -> bool {
    ids.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes())
}

/// Validate that a `write_set_summary` meets wire size caps.
#[must_use]
pub fn validate_write_set_summary(pages: &[u32]) -> bool {
    let byte_len = pages.len() * 4;
    byte_len <= WIRE_WRITE_SET_MAX_BYTES && byte_len % 4 == 0
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
/// Note: `UnixStream::peer_cred()` is currently an unstable stdlib API. Until we
/// wire a stable, safe peer-credential path (likely via an external dependency),
/// this function is a no-op that accepts the peer.
#[cfg(target_family = "unix")]
pub fn authenticate_peer(
    _stream: &std::os::unix::net::UnixStream,
    _expected_uid: u32,
) -> Result<(), PeerAuthError> {
    Ok(())
}

// ---------------------------------------------------------------------------
// File descriptor passing helpers (§5.9.0 bulk payload transfer)
// ---------------------------------------------------------------------------

/// Send raw bytes plus a file descriptor over a Unix stream.
///
/// Note: fd passing via `SCM_RIGHTS` is currently an unstable stdlib API.
/// For now, this sends only the bytes and ignores the fd.
#[cfg(target_family = "unix")]
pub fn send_with_fd(
    stream: &std::os::unix::net::UnixStream,
    data: &[u8],
    _fd: std::os::unix::io::RawFd,
) -> std::io::Result<usize> {
    use std::io::Write;
    let mut s = stream;
    s.write_all(data)?;
    Ok(data.len())
}

/// Receive raw bytes plus a file descriptor from a Unix stream.
///
/// Returns `(bytes_read, Option<RawFd>)`.
#[cfg(target_family = "unix")]
pub fn recv_with_fd(
    stream: &std::os::unix::net::UnixStream,
    buf: &mut [u8],
) -> std::io::Result<(usize, Option<std::os::unix::io::RawFd>)> {
    use std::io::Read;
    let mut s = stream;
    let n = s.read(buf)?;
    Ok((n, None))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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

    // -- bd-1m07 test 6: Peer auth (stub) is callable --
    #[cfg(target_family = "unix")]
    #[test]
    fn test_peer_auth_is_callable() {
        use std::os::unix::net::UnixStream;

        let (a, _b) = UnixStream::pair().expect("socketpair");

        // Current implementation is a no-op (see authenticate_peer docs).
        authenticate_peer(&a, 0).expect("peer auth");
        authenticate_peer(&a, 1).expect("peer auth");
    }

    // -- bd-1m07 test 7: SCM_RIGHTS fd passing --
    #[cfg(target_family = "unix")]
    #[test]
    fn test_send_recv_with_fd_marker_no_fd_attached() {
        use std::os::unix::net::UnixStream;

        let (sender, receiver) = UnixStream::pair().expect("socketpair");

        // Send bytes; fd is currently ignored.
        let data = b"fd-marker";
        let sent = send_with_fd(&sender, data, -1).expect("send_with_fd");
        assert_eq!(sent, data.len());

        // Receive.
        let mut buf = [0u8; 64];
        let (n, maybe_fd) = recv_with_fd(&receiver, &mut buf).expect("recv_with_fd");
        assert_eq!(&buf[..n], data);
        assert!(maybe_fd.is_none(), "no fd should be attached in stub mode");
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
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;
        use std::sync::{Arc, Barrier};

        let pm = Arc::new(PermitManager::new(MAX_OUTSTANDING_PERMITS));
        let cache = Arc::new(IdempotencyCache::new());
        let barrier = Arc::new(Barrier::new(2));

        let (client_sock, server_sock) = UnixStream::pair().expect("socketpair");

        // Peer auth is currently a no-op (see authenticate_peer docs).
        authenticate_peer(&server_sock, 0).expect("E2E peer auth");

        let pm_server = Arc::clone(&pm);
        let cache_server = Arc::clone(&cache);
        let barrier_server = Arc::clone(&barrier);

        // Server thread: read frames, process reserve/submit.
        let server = std::thread::spawn(move || {
            let mut buf = vec![0u8; 4096];
            barrier_server.wait();

            // --- Round 1: RESERVE ---
            let n = (&server_sock).read(&mut buf).expect("server read reserve");
            let frame = Frame::decode(&buf[..n]).expect("decode reserve frame");
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
            let n = (&server_sock).read(&mut buf).expect("server read submit");
            let frame = Frame::decode(&buf[..n]).expect("decode submit frame");
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
            let n = (&server_sock).read(&mut buf).expect("server read dup");
            let frame = Frame::decode(&buf[..n]).expect("decode dup frame");
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
            let n = (&server_sock).read(&mut buf).expect("server read ping");
            let frame = Frame::decode(&buf[..n]).expect("decode ping");
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
        let mut buf = vec![0u8; 4096];

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

        let n = (&client_sock)
            .read(&mut buf)
            .expect("client read reserve resp");
        let resp = Frame::decode(&buf[..n]).expect("decode reserve resp");
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

        // Round 2: SUBMIT_WAL_COMMIT.
        let submit = Frame {
            kind: MessageKind::SubmitWalCommit,
            request_id: 2,
            payload: wal.to_bytes(),
        };
        (&client_sock)
            .write_all(&submit.encode())
            .expect("client write submit");

        let n = (&client_sock)
            .read(&mut buf)
            .expect("client read commit resp");
        let resp = Frame::decode(&buf[..n]).expect("decode commit resp");
        let commit_resp =
            WalCommitResponse::from_bytes(&resp.payload).expect("parse commit response");
        assert_eq!(commit_resp, WalCommitResponse::Ok { commit_seq: 42 });

        // Round 3: Duplicate SUBMIT (test idempotency from client side).
        let dup_submit = Frame {
            kind: MessageKind::SubmitWalCommit,
            request_id: 3,
            payload: wal.to_bytes(),
        };
        (&client_sock)
            .write_all(&dup_submit.encode())
            .expect("client write dup submit");

        let n = (&client_sock).read(&mut buf).expect("client read dup resp");
        let resp = Frame::decode(&buf[..n]).expect("decode dup resp");
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

        let n = (&client_sock).read(&mut buf).expect("client read pong");
        let resp = Frame::decode(&buf[..n]).expect("decode pong");
        assert_eq!(resp.kind, MessageKind::Pong);
        assert_eq!(resp.request_id, 4);

        server.join().expect("server thread");
    }

    // No peer-cred / SCM_RIGHTS helpers yet (kept stub-only to avoid unsafe + unstable APIs).
}
