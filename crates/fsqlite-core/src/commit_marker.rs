//! Commit Marker Stream Format (§3.5.4 + §3.5.4.1, bd-1hi.23).
//!
//! The marker stream under `ecs/markers/` is the total order of commits in
//! Native mode.  It is the authoritative, tamper-evident, seekable commit log.
//!
//! On-disk encoding: all fixed-width integers are **little-endian** (§3.5.1).
//! Sizes are byte-exact — never derived from `mem::size_of::<T>()`.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes for a marker segment header: "FSMK".
pub const MARKER_SEGMENT_MAGIC: [u8; 4] = *b"FSMK";

/// Current format version.
pub const MARKER_FORMAT_VERSION: u32 = 1;

/// Byte size of [`MarkerSegmentHeader`] on disk.
pub const MARKER_SEGMENT_HEADER_BYTES: usize = 36;

/// Byte size of [`CommitMarkerRecord`] on disk.
pub const COMMIT_MARKER_RECORD_BYTES: usize = 88;

/// Default number of markers per segment (fixed rotation policy).
pub const MARKERS_PER_SEGMENT: u64 = 1_000_000;

/// Domain separation tag for marker_id computation.
const MARKER_ID_DOMAIN: &[u8] = b"fsqlite:marker:v1";

/// Size of a marker_id or object_id in bytes.
const ID_SIZE: usize = 16;

/// Byte length of the record prefix used for marker_id hashing.
/// commit_seq(8) + commit_time_unix_ns(8) + capsule_object_id(16) +
/// proof_object_id(16) + prev_marker_id(16) = 64.
const RECORD_PREFIX_BYTES: usize = 64;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from marker stream operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerError {
    /// Header buffer too short (need [`MARKER_SEGMENT_HEADER_BYTES`]).
    HeaderTooShort,
    /// Bad magic bytes in segment header.
    BadMagic,
    /// Header xxh3 checksum mismatch.
    HeaderChecksumMismatch { expected: u64, actual: u64 },
    /// Record buffer too short (need [`COMMIT_MARKER_RECORD_BYTES`]).
    RecordTooShort,
    /// Record xxh3 checksum mismatch.
    RecordChecksumMismatch { expected: u64, actual: u64 },
    /// Record version mismatch.
    UnsupportedVersion { version: u32 },
    /// Record size in header doesn't match expected.
    RecordSizeMismatch { expected: u32, actual: u32 },
    /// commit_seq doesn't match expected slot position.
    CommitSeqMismatch { expected: u64, actual: u64 },
    /// Segment data has incomplete (torn) tail.
    TornTail {
        complete_records: u64,
        trailing_bytes: usize,
    },
    /// marker_id does not match recomputed value (forged or corrupt record).
    MarkerIdMismatch { commit_seq: u64 },
    /// The record does not link to the preceding accepted marker.
    HashChainMismatch { commit_seq: u64 },
    /// Commit time moved backwards, invalidating ordered time lookup.
    TimestampRegression {
        commit_seq: u64,
        previous: u64,
        actual: u64,
    },
    /// A sequence or range endpoint cannot be represented without wrapping.
    SequenceOverflow,
    /// Recovery could not reserve space for another accepted marker.
    OutOfMemory,
}

impl std::fmt::Display for MarkerError {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeaderTooShort => f.write_str("marker segment header too short"),
            Self::BadMagic => f.write_str("bad magic in marker segment header"),
            Self::HeaderChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "marker header xxh3 mismatch: expected {expected:#018X}, got {actual:#018X}"
                )
            }
            Self::RecordTooShort => f.write_str("commit marker record too short"),
            Self::RecordChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "marker record xxh3 mismatch: expected {expected:#018X}, got {actual:#018X}"
                )
            }
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported marker format version: {version}")
            }
            Self::RecordSizeMismatch { expected, actual } => {
                write!(
                    f,
                    "marker record size mismatch: expected {expected}, got {actual}"
                )
            }
            Self::CommitSeqMismatch { expected, actual } => {
                write!(f, "commit_seq mismatch: expected {expected}, got {actual}")
            }
            Self::TornTail {
                complete_records,
                trailing_bytes,
            } => {
                write!(
                    f,
                    "torn tail: {complete_records} complete records, {trailing_bytes} trailing bytes"
                )
            }
            Self::MarkerIdMismatch { commit_seq } => {
                write!(
                    f,
                    "marker_id verification failed for commit_seq {commit_seq} (forged or corrupt)"
                )
            }
            Self::HashChainMismatch { commit_seq } => {
                write!(f, "marker hash chain mismatch at commit_seq {commit_seq}")
            }
            Self::TimestampRegression {
                commit_seq,
                previous,
                actual,
            } => write!(
                f,
                "marker timestamp regression at commit_seq {commit_seq}: {actual} < {previous}"
            ),
            Self::SequenceOverflow => f.write_str("marker commit sequence overflow"),
            Self::OutOfMemory => f.write_str("unable to reserve marker recovery memory"),
        }
    }
}

impl std::error::Error for MarkerError {}

// ---------------------------------------------------------------------------
// MarkerSegmentHeader (36 bytes)
// ---------------------------------------------------------------------------

/// On-disk segment header for the commit marker stream.
///
/// Layout (36 bytes, all LE):
/// ```text
///   magic           : [u8; 4]   — "FSMK"
///   version         : u32       — 1
///   segment_id      : u64       — monotonic identifier
///   start_commit_seq: u64       — first commit_seq in this segment
///   record_size     : u32       — bytes per record (88 in V1)
///   header_xxh3     : u64       — xxhash3 of all preceding fields
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerSegmentHeader {
    /// Monotonic segment identifier (matches filename).
    pub segment_id: u64,
    /// First `commit_seq` stored in this segment.
    pub start_commit_seq: u64,
}

impl MarkerSegmentHeader {
    /// Create a new header for the given segment.
    #[must_use]
    pub const fn new(segment_id: u64, start_commit_seq: u64) -> Self {
        Self {
            segment_id,
            start_commit_seq,
        }
    }

    /// Encode to exactly [`MARKER_SEGMENT_HEADER_BYTES`] bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; MARKER_SEGMENT_HEADER_BYTES] {
        let mut buf = [0u8; MARKER_SEGMENT_HEADER_BYTES];
        // magic (4)
        buf[0..4].copy_from_slice(&MARKER_SEGMENT_MAGIC);
        // version (4)
        buf[4..8].copy_from_slice(&MARKER_FORMAT_VERSION.to_le_bytes());
        // segment_id (8)
        buf[8..16].copy_from_slice(&self.segment_id.to_le_bytes());
        // start_commit_seq (8)
        buf[16..24].copy_from_slice(&self.start_commit_seq.to_le_bytes());
        // record_size (4)
        #[allow(clippy::cast_possible_truncation)]
        let record_size = COMMIT_MARKER_RECORD_BYTES as u32;
        buf[24..28].copy_from_slice(&record_size.to_le_bytes());
        // header_xxh3 (8) — hash of bytes [0..28]
        let hash = xxhash_rust::xxh3::xxh3_64(&buf[..28]);
        buf[28..36].copy_from_slice(&hash.to_le_bytes());
        buf
    }

    /// Decode from a byte slice. Validates magic, version, record_size, and checksum.
    pub fn decode(data: &[u8]) -> Result<Self, MarkerError> {
        if data.len() < MARKER_SEGMENT_HEADER_BYTES {
            return Err(MarkerError::HeaderTooShort);
        }

        // magic
        if data[0..4] != MARKER_SEGMENT_MAGIC {
            return Err(MarkerError::BadMagic);
        }

        // version
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != MARKER_FORMAT_VERSION {
            return Err(MarkerError::UnsupportedVersion { version });
        }

        // segment_id
        let segment_id = u64::from_le_bytes(data[8..16].try_into().expect("8 bytes"));

        // start_commit_seq
        let start_commit_seq = u64::from_le_bytes(data[16..24].try_into().expect("8 bytes"));

        // record_size
        let record_size = u32::from_le_bytes([data[24], data[25], data[26], data[27]]);
        #[allow(clippy::cast_possible_truncation)]
        let expected_record_size = COMMIT_MARKER_RECORD_BYTES as u32;
        if record_size != expected_record_size {
            return Err(MarkerError::RecordSizeMismatch {
                expected: expected_record_size,
                actual: record_size,
            });
        }

        // header_xxh3
        let stored_hash = u64::from_le_bytes(data[28..36].try_into().expect("8 bytes"));
        let computed_hash = xxhash_rust::xxh3::xxh3_64(&data[..28]);
        if stored_hash != computed_hash {
            return Err(MarkerError::HeaderChecksumMismatch {
                expected: computed_hash,
                actual: stored_hash,
            });
        }

        Ok(Self {
            segment_id,
            start_commit_seq,
        })
    }
}

// ---------------------------------------------------------------------------
// CommitMarkerRecord (88 bytes)
// ---------------------------------------------------------------------------

/// On-disk commit marker record.
///
/// Layout (88 bytes, all LE):
/// ```text
///   commit_seq          : u64      (8)
///   commit_time_unix_ns : u64      (8)
///   capsule_object_id   : [u8;16]  (16)
///   proof_object_id     : [u8;16]  (16)
///   prev_marker_id      : [u8;16]  (16) — 0 for genesis
///   marker_id           : [u8;16]  (16)
///   record_xxh3         : u64      (8)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMarkerRecord {
    /// Monotonic commit sequence number (gap-free within a segment).
    pub commit_seq: u64,
    /// Commit timestamp in nanoseconds since Unix epoch.
    pub commit_time_unix_ns: u64,
    /// ObjectId of the commit capsule.
    pub capsule_object_id: [u8; ID_SIZE],
    /// ObjectId of the proof object.
    pub proof_object_id: [u8; ID_SIZE],
    /// marker_id of the previous commit (all zeros for genesis).
    pub prev_marker_id: [u8; ID_SIZE],
    /// This record's marker_id: `Trunc128(BLAKE3("fsqlite:marker:v1" || prefix_bytes))`.
    pub marker_id: [u8; ID_SIZE],
}

impl CommitMarkerRecord {
    /// Create a new record, computing `marker_id` from the other fields.
    #[must_use]
    pub fn new(
        commit_seq: u64,
        commit_time_unix_ns: u64,
        capsule_object_id: [u8; ID_SIZE],
        proof_object_id: [u8; ID_SIZE],
        prev_marker_id: [u8; ID_SIZE],
    ) -> Self {
        let marker_id = compute_marker_id(
            commit_seq,
            commit_time_unix_ns,
            &capsule_object_id,
            &proof_object_id,
            &prev_marker_id,
        );
        Self {
            commit_seq,
            commit_time_unix_ns,
            capsule_object_id,
            proof_object_id,
            prev_marker_id,
            marker_id,
        }
    }

    /// Encode to exactly [`COMMIT_MARKER_RECORD_BYTES`] bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; COMMIT_MARKER_RECORD_BYTES] {
        let mut buf = [0u8; COMMIT_MARKER_RECORD_BYTES];
        buf[0..8].copy_from_slice(&self.commit_seq.to_le_bytes());
        buf[8..16].copy_from_slice(&self.commit_time_unix_ns.to_le_bytes());
        buf[16..32].copy_from_slice(&self.capsule_object_id);
        buf[32..48].copy_from_slice(&self.proof_object_id);
        buf[48..64].copy_from_slice(&self.prev_marker_id);
        buf[64..80].copy_from_slice(&self.marker_id);
        // record_xxh3: hash of bytes [0..80]
        let hash = xxhash_rust::xxh3::xxh3_64(&buf[..80]);
        buf[80..88].copy_from_slice(&hash.to_le_bytes());
        buf
    }

    /// Decode from a byte slice. Validates xxh3 checksum.
    pub fn decode(data: &[u8]) -> Result<Self, MarkerError> {
        if data.len() < COMMIT_MARKER_RECORD_BYTES {
            return Err(MarkerError::RecordTooShort);
        }

        let commit_seq = u64::from_le_bytes(data[0..8].try_into().expect("8 bytes"));
        let commit_time_unix_ns = u64::from_le_bytes(data[8..16].try_into().expect("8 bytes"));

        let mut capsule_object_id = [0u8; ID_SIZE];
        capsule_object_id.copy_from_slice(&data[16..32]);

        let mut proof_object_id = [0u8; ID_SIZE];
        proof_object_id.copy_from_slice(&data[32..48]);

        let mut prev_marker_id = [0u8; ID_SIZE];
        prev_marker_id.copy_from_slice(&data[48..64]);

        let mut marker_id = [0u8; ID_SIZE];
        marker_id.copy_from_slice(&data[64..80]);

        let stored_hash = u64::from_le_bytes(data[80..88].try_into().expect("8 bytes"));
        let computed_hash = xxhash_rust::xxh3::xxh3_64(&data[..80]);
        if stored_hash != computed_hash {
            return Err(MarkerError::RecordChecksumMismatch {
                expected: computed_hash,
                actual: stored_hash,
            });
        }

        Ok(Self {
            commit_seq,
            commit_time_unix_ns,
            capsule_object_id,
            proof_object_id,
            prev_marker_id,
            marker_id,
        })
    }

    /// Verify that `marker_id` matches the recomputed value.
    #[must_use]
    pub fn verify_marker_id(&self) -> bool {
        let expected = compute_marker_id(
            self.commit_seq,
            self.commit_time_unix_ns,
            &self.capsule_object_id,
            &self.proof_object_id,
            &self.prev_marker_id,
        );
        self.marker_id == expected
    }
}

// ---------------------------------------------------------------------------
// marker_id computation
// ---------------------------------------------------------------------------

/// Compute marker_id: `Trunc128(BLAKE3("fsqlite:marker:v1" || prefix_bytes))`.
///
/// `prefix_bytes` is the LE encoding of
/// `(commit_seq, commit_time_unix_ns, capsule_object_id, proof_object_id, prev_marker_id)`.
#[must_use]
pub fn compute_marker_id(
    commit_seq: u64,
    commit_time_unix_ns: u64,
    capsule_object_id: &[u8; ID_SIZE],
    proof_object_id: &[u8; ID_SIZE],
    prev_marker_id: &[u8; ID_SIZE],
) -> [u8; ID_SIZE] {
    let mut prefix = [0u8; RECORD_PREFIX_BYTES];
    prefix[0..8].copy_from_slice(&commit_seq.to_le_bytes());
    prefix[8..16].copy_from_slice(&commit_time_unix_ns.to_le_bytes());
    prefix[16..32].copy_from_slice(capsule_object_id);
    prefix[32..48].copy_from_slice(proof_object_id);
    prefix[48..64].copy_from_slice(prev_marker_id);

    let mut hasher = blake3::Hasher::new();
    hasher.update(MARKER_ID_DOMAIN);
    hasher.update(&prefix);
    let hash = hasher.finalize();

    let mut id = [0u8; ID_SIZE];
    id.copy_from_slice(&hash.as_bytes()[..ID_SIZE]);
    id
}

// ---------------------------------------------------------------------------
// O(1) seek helpers
// ---------------------------------------------------------------------------

/// Compute which segment a `commit_seq` falls in (fixed rotation policy).
#[must_use]
pub const fn segment_id_for_commit_seq(commit_seq: u64) -> u64 {
    commit_seq / MARKERS_PER_SEGMENT
}

/// Compute the `start_commit_seq` for a given segment_id.
#[must_use]
pub const fn start_commit_seq_for_segment(segment_id: u64) -> u64 {
    segment_id * MARKERS_PER_SEGMENT
}

/// Compute the byte offset of a record within a segment file.
///
/// `offset = MARKER_SEGMENT_HEADER_BYTES + (commit_seq - start_commit_seq) * COMMIT_MARKER_RECORD_BYTES`
#[must_use]
pub const fn record_offset(commit_seq: u64, start_commit_seq: u64) -> u64 {
    let slot = commit_seq - start_commit_seq;
    MARKER_SEGMENT_HEADER_BYTES as u64 + slot * COMMIT_MARKER_RECORD_BYTES as u64
}

/// Compute the next `commit_seq` from segment file length (crash-safe allocation).
///
/// `next_commit_seq = start_commit_seq + floor((file_len - header) / record_size)`
#[must_use]
pub const fn next_commit_seq_from_file_len(start_commit_seq: u64, file_len: u64) -> u64 {
    if file_len < MARKER_SEGMENT_HEADER_BYTES as u64 {
        return start_commit_seq;
    }
    let data_bytes = file_len - MARKER_SEGMENT_HEADER_BYTES as u64;
    let n_records = data_bytes / COMMIT_MARKER_RECORD_BYTES as u64;
    start_commit_seq + n_records
}

// ---------------------------------------------------------------------------
// Torn tail handling
// ---------------------------------------------------------------------------

/// Scan a segment's record region and return the count of valid records
/// from the start. Stops at the first checksum, content-hash, sequence,
/// hash-chain, or timestamp violation.
///
/// `data` must be the record region only (header already stripped).
/// The first record is unanchored: this does not establish its provenance,
/// expected sequence, or continuity with a preceding segment.
#[must_use]
pub fn valid_record_prefix_count(data: &[u8]) -> u64 {
    let mut count = 0u64;
    let mut previous: Option<CommitMarkerRecord> = None;
    for record_bytes in data.chunks_exact(COMMIT_MARKER_RECORD_BYTES) {
        let Ok(record) = CommitMarkerRecord::decode(record_bytes) else {
            break;
        };
        if !record.verify_marker_id()
            || previous
                .as_ref()
                .is_some_and(|prev| validate_successor(prev, &record).is_err())
        {
            break;
        }
        count += 1;
        previous = Some(record);
    }
    count
}

/// Analyze a full segment buffer for torn tail conditions.
///
/// Returns `Ok(n_records)` if all complete records are valid,
/// or `Err(TornTail { .. })` if there are trailing partial bytes.
pub fn check_segment_integrity(segment_data: &[u8]) -> Result<u64, MarkerError> {
    if segment_data.len() < MARKER_SEGMENT_HEADER_BYTES {
        return Err(MarkerError::HeaderTooShort);
    }

    // Header must be valid before we reason about record layout.
    let _header = MarkerSegmentHeader::decode(&segment_data[..MARKER_SEGMENT_HEADER_BYTES])?;

    let record_region = &segment_data[MARKER_SEGMENT_HEADER_BYTES..];
    let complete_records = record_region.len() / COMMIT_MARKER_RECORD_BYTES;
    let trailing = record_region.len() % COMMIT_MARKER_RECORD_BYTES;

    // Verify all complete records up to the first decode failure while also
    // enforcing density (`commit_seq = start_commit_seq + slot`).
    let recovered = recover_valid_prefix(segment_data)?;
    let valid = u64::try_from(recovered.len()).expect("record count always fits in u64");

    #[allow(clippy::cast_possible_truncation)]
    let complete_u64 = complete_records as u64;

    if trailing > 0 || valid < complete_u64 {
        let valid_usize = recovered.len();
        return Err(MarkerError::TornTail {
            complete_records: valid,
            trailing_bytes: if valid < complete_u64 {
                // Corruption mid-stream: remaining bytes from corrupt record onward.
                record_region
                    .len()
                    .saturating_sub(valid_usize.saturating_mul(COMMIT_MARKER_RECORD_BYTES))
            } else {
                trailing
            },
        });
    }

    Ok(valid)
}

/// Recover the valid, density- and chain-checked prefix of one segment.
///
/// This helper is intentionally tolerant of torn tails: it stops at the first
/// undecodable record and returns the valid prefix. Complete records with
/// invalid identity, sequence, linkage, or time ordering fail closed instead
/// of being treated as an incomplete append. The first record's predecessor
/// remains unanchored; callers combining segments must validate that boundary.
pub fn recover_valid_prefix(segment_data: &[u8]) -> Result<Vec<CommitMarkerRecord>, MarkerError> {
    if segment_data.len() < MARKER_SEGMENT_HEADER_BYTES {
        return Err(MarkerError::HeaderTooShort);
    }

    let header = MarkerSegmentHeader::decode(&segment_data[..MARKER_SEGMENT_HEADER_BYTES])?;
    let record_region = &segment_data[MARKER_SEGMENT_HEADER_BYTES..];

    let mut records: Vec<CommitMarkerRecord> = Vec::new();

    for record_bytes in record_region.chunks_exact(COMMIT_MARKER_RECORD_BYTES) {
        let Ok(record) = CommitMarkerRecord::decode(record_bytes) else {
            break;
        };

        let slot = u64::try_from(records.len()).map_err(|_| MarkerError::SequenceOverflow)?;
        let expected = header
            .start_commit_seq
            .checked_add(slot)
            .ok_or(MarkerError::SequenceOverflow)?;
        if record.commit_seq != expected {
            return Err(MarkerError::CommitSeqMismatch {
                expected,
                actual: record.commit_seq,
            });
        }

        // These unkeyed hashes establish internal consistency, not sender
        // authentication. A substituted, rehashed record must also extend the
        // accepted chain; a trusted external anchor is still needed to detect
        // replacement of an entire internally consistent history.
        if !record.verify_marker_id() {
            return Err(MarkerError::MarkerIdMismatch {
                commit_seq: record.commit_seq,
            });
        }

        if let Some(previous) = records.last() {
            validate_successor(previous, &record)?;
        }
        records
            .try_reserve(1)
            .map_err(|_| MarkerError::OutOfMemory)?;
        records.push(record);
    }

    Ok(records)
}

/// Validate adjacency after both records' individual hashes were checked.
fn validate_successor(
    previous: &CommitMarkerRecord,
    record: &CommitMarkerRecord,
) -> Result<(), MarkerError> {
    let expected = previous
        .commit_seq
        .checked_add(1)
        .ok_or(MarkerError::SequenceOverflow)?;
    if record.commit_seq != expected {
        return Err(MarkerError::CommitSeqMismatch {
            expected,
            actual: record.commit_seq,
        });
    }
    if record.prev_marker_id != previous.marker_id {
        return Err(MarkerError::HashChainMismatch {
            commit_seq: record.commit_seq,
        });
    }
    if record.commit_time_unix_ns < previous.commit_time_unix_ns {
        return Err(MarkerError::TimestampRegression {
            commit_seq: record.commit_seq,
            previous: previous.commit_time_unix_ns,
            actual: record.commit_time_unix_ns,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Binary search by time
// ---------------------------------------------------------------------------

/// Binary search for the commit_seq whose `commit_time_unix_ns` is the greatest
/// value <= `target_ns`.  Returns `None` if all records are after `target_ns`.
///
/// `read_record` is called with a commit_seq and must return the decoded record.
/// The caller must provide an already validated, monotonic marker stream.
/// A missing, mismatched, or invalid record encountered during the search,
/// or an overflowing sequence range, returns `None`, never a partial best hit.
pub fn binary_search_by_time<F>(
    start_commit_seq: u64,
    record_count: u64,
    target_ns: u64,
    mut read_record: F,
) -> Option<u64>
where
    F: FnMut(u64) -> Option<CommitMarkerRecord>,
{
    if record_count == 0 {
        return None;
    }
    start_commit_seq.checked_add(record_count - 1)?;

    let mut lo = 0u64;
    let mut hi = record_count;
    let mut best: Option<u64> = None;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let seq = start_commit_seq + mid;
        let record = read_record(seq)?;
        if record.commit_seq != seq || !record.verify_marker_id() {
            return None;
        }

        if record.commit_time_unix_ns <= target_ns {
            best = Some(seq);
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    best
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ===================================================================
    // bd-1hi.23 — Commit Marker Stream Format (§3.5.4)
    // ===================================================================

    fn make_test_record(seq: u64, prev: [u8; ID_SIZE]) -> CommitMarkerRecord {
        let capsule = [(seq & 0xFF) as u8; ID_SIZE];
        let proof = [((seq >> 8) & 0xFF) as u8; ID_SIZE];
        let time_ns = 1_700_000_000_000_000_000u64 + seq * 1_000_000;
        CommitMarkerRecord::new(seq, time_ns, capsule, proof, prev)
    }

    fn encode_test_segment(start: u64, records: &[CommitMarkerRecord]) -> Vec<u8> {
        let mut bytes = Vec::from(MarkerSegmentHeader::new(0, start).encode());
        for record in records {
            bytes.extend_from_slice(&record.encode());
        }
        bytes
    }

    #[test]
    fn recovery_rejects_rehashed_broken_link() {
        let first = make_test_record(10, [0; ID_SIZE]);
        let second = make_test_record(11, [0xAB; ID_SIZE]);
        assert!(second.verify_marker_id());
        assert!(CommitMarkerRecord::decode(&second.encode()).is_ok());
        let bytes = encode_test_segment(10, &[first, second]);
        assert_eq!(
            recover_valid_prefix(&bytes),
            Err(MarkerError::HashChainMismatch { commit_seq: 11 })
        );
        assert_eq!(
            check_segment_integrity(&bytes),
            Err(MarkerError::HashChainMismatch { commit_seq: 11 })
        );
        assert_eq!(
            valid_record_prefix_count(&bytes[MARKER_SEGMENT_HEADER_BYTES..]),
            1
        );
    }

    #[test]
    fn recovery_rejects_rehashed_timestamp_regression() {
        let first = CommitMarkerRecord::new(10, 100, [1; ID_SIZE], [2; ID_SIZE], [0; ID_SIZE]);
        let second = CommitMarkerRecord::new(11, 99, [3; ID_SIZE], [4; ID_SIZE], first.marker_id);
        let bytes = encode_test_segment(10, &[first, second]);
        assert_eq!(
            recover_valid_prefix(&bytes),
            Err(MarkerError::TimestampRegression {
                commit_seq: 11,
                previous: 100,
                actual: 99,
            })
        );
        assert_eq!(
            valid_record_prefix_count(&bytes[MARKER_SEGMENT_HEADER_BYTES..]),
            1
        );
    }

    #[test]
    fn recovery_accepts_equal_times_and_nonzero_unanchored_start() {
        let first = CommitMarkerRecord::new(10, 100, [1; ID_SIZE], [2; ID_SIZE], [9; ID_SIZE]);
        let second = CommitMarkerRecord::new(11, 100, [3; ID_SIZE], [4; ID_SIZE], first.marker_id);
        let records = vec![first, second];
        let bytes = encode_test_segment(10, &records);
        assert_eq!(recover_valid_prefix(&bytes).unwrap(), records);
        assert_eq!(check_segment_integrity(&bytes), Ok(2));
        assert_eq!(
            valid_record_prefix_count(&bytes[MARKER_SEGMENT_HEADER_BYTES..]),
            2
        );
    }

    #[test]
    fn recovery_refuses_sequence_wraparound() {
        let first =
            CommitMarkerRecord::new(u64::MAX, 100, [1; ID_SIZE], [2; ID_SIZE], [0; ID_SIZE]);
        let mut bytes = encode_test_segment(u64::MAX, std::slice::from_ref(&first));
        assert_eq!(recover_valid_prefix(&bytes).unwrap(), vec![first.clone()]);
        let wrapped = CommitMarkerRecord::new(0, 101, [3; ID_SIZE], [4; ID_SIZE], first.marker_id);
        bytes.extend_from_slice(&wrapped.encode());
        assert_eq!(
            recover_valid_prefix(&bytes),
            Err(MarkerError::SequenceOverflow)
        );
        assert_eq!(
            valid_record_prefix_count(&bytes[MARKER_SEGMENT_HEADER_BYTES..]),
            1
        );
    }

    #[test]
    fn prefix_counter_rejects_gap_duplicate_and_reordering() {
        let first = make_test_record(10, [0; ID_SIZE]);
        for next_seq in [9, 10, 12] {
            let second = make_test_record(next_seq, first.marker_id);
            let bytes = encode_test_segment(10, &[first.clone(), second]);
            assert_eq!(
                valid_record_prefix_count(&bytes[MARKER_SEGMENT_HEADER_BYTES..]),
                1
            );
            assert!(matches!(
                recover_valid_prefix(&bytes),
                Err(MarkerError::CommitSeqMismatch { .. })
            ));
        }
    }

    #[test]
    fn recovery_preserves_every_torn_tail_prefix() {
        let mut records = Vec::new();
        let mut prev = [0; ID_SIZE];
        for seq in 10..14 {
            let record = make_test_record(seq, prev);
            prev = record.marker_id;
            records.push(record);
        }
        let bytes = encode_test_segment(10, &records);
        for end in MARKER_SEGMENT_HEADER_BYTES..=bytes.len() {
            let complete = (end - MARKER_SEGMENT_HEADER_BYTES) / COMMIT_MARKER_RECORD_BYTES;
            assert_eq!(
                recover_valid_prefix(&bytes[..end]).unwrap(),
                records[..complete]
            );
            assert_eq!(
                valid_record_prefix_count(&bytes[MARKER_SEGMENT_HEADER_BYTES..end]),
                u64::try_from(complete).unwrap()
            );
        }
    }

    #[test]
    fn time_search_does_not_return_best_hit_after_read_failure() {
        let mut visited = Vec::new();
        let hit = binary_search_by_time(0, 8, u64::MAX, |seq| {
            visited.push(seq);
            (seq != 6).then(|| make_test_record(seq, [0; ID_SIZE]))
        });
        assert_eq!(visited, vec![4, 6]);
        assert_eq!(
            hit, None,
            "commit 4 is not a proven answer after read 6 failed"
        );
    }

    #[test]
    fn time_search_rejects_wrong_slot_and_bad_content_hash() {
        for wrong_slot in [false, true] {
            let hit = binary_search_by_time(0, 8, u64::MAX, |seq| {
                let mut record = make_test_record(seq, [0; ID_SIZE]);
                if seq == 6 {
                    if wrong_slot {
                        record = make_test_record(5, [0; ID_SIZE]);
                    } else {
                        record.capsule_object_id[0] ^= 1;
                    }
                }
                Some(record)
            });
            assert_eq!(hit, None);
        }
    }

    #[test]
    fn time_search_checks_range_before_reading() {
        assert_eq!(
            binary_search_by_time(u64::MAX, 2, 0, |_| panic!("invalid range read")),
            None
        );
        assert_eq!(
            binary_search_by_time(u64::MAX, 0, 0, |_| panic!("empty range read")),
            None
        );
        let record =
            CommitMarkerRecord::new(u64::MAX, 100, [1; ID_SIZE], [2; ID_SIZE], [0; ID_SIZE]);
        assert_eq!(
            binary_search_by_time(u64::MAX, 1, 100, |_| Some(record.clone())),
            Some(u64::MAX)
        );
    }

    #[test]
    fn test_marker_segment_header_encode_decode() {
        let header = MarkerSegmentHeader::new(42, 42_000_000);
        let encoded = header.encode();
        assert_eq!(
            encoded.len(),
            MARKER_SEGMENT_HEADER_BYTES,
            "header must be exactly {MARKER_SEGMENT_HEADER_BYTES} bytes"
        );

        let decoded = MarkerSegmentHeader::decode(&encoded).expect("decode must succeed");
        assert_eq!(decoded, header);

        // Verify magic bytes.
        assert_eq!(&encoded[0..4], b"FSMK");
    }

    #[test]
    fn test_commit_marker_record_encode_decode() {
        let record = make_test_record(7, [0u8; ID_SIZE]);
        let encoded = record.encode();
        assert_eq!(
            encoded.len(),
            COMMIT_MARKER_RECORD_BYTES,
            "record must be exactly {COMMIT_MARKER_RECORD_BYTES} bytes"
        );

        let decoded = CommitMarkerRecord::decode(&encoded).expect("decode must succeed");
        assert_eq!(decoded, record);
    }

    #[test]
    fn test_marker_id_computation() {
        let seq = 100u64;
        let time_ns = 1_700_000_000_000_000_000u64;
        let capsule = [0xAAu8; ID_SIZE];
        let proof = [0xBBu8; ID_SIZE];
        let prev = [0u8; ID_SIZE];

        let marker_id = compute_marker_id(seq, time_ns, &capsule, &proof, &prev);

        // Manually compute the expected value.
        let mut prefix = [0u8; RECORD_PREFIX_BYTES];
        prefix[0..8].copy_from_slice(&seq.to_le_bytes());
        prefix[8..16].copy_from_slice(&time_ns.to_le_bytes());
        prefix[16..32].copy_from_slice(&capsule);
        prefix[32..48].copy_from_slice(&proof);
        prefix[48..64].copy_from_slice(&prev);

        let mut hasher = blake3::Hasher::new();
        hasher.update(MARKER_ID_DOMAIN);
        hasher.update(&prefix);
        let hash = hasher.finalize();
        let mut expected = [0u8; ID_SIZE];
        expected.copy_from_slice(&hash.as_bytes()[..ID_SIZE]);

        assert_eq!(
            marker_id, expected,
            "marker_id must be Trunc128(BLAKE3(domain || prefix))"
        );
    }

    #[test]
    fn test_density_invariant() {
        let start_seq = 1000u64;
        let mut prev = [0u8; ID_SIZE];
        let mut records = Vec::new();

        for i in 0..5u64 {
            let record = make_test_record(start_seq + i, prev);
            prev = record.marker_id;
            records.push(record);
        }

        for (i, record) in records.iter().enumerate() {
            assert_eq!(
                record.commit_seq,
                start_seq + i as u64,
                "record at slot {i} must have commit_seq = start + {i}"
            );
        }
    }

    #[test]
    fn test_o1_seek_by_commit_seq() {
        let start_seq = 0u64;
        let header = MarkerSegmentHeader::new(0, start_seq);
        let mut segment = Vec::from(header.encode());

        let mut prev = [0u8; ID_SIZE];
        let mut records = Vec::new();
        for i in 0..1000u64 {
            let record = make_test_record(start_seq + i, prev);
            prev = record.marker_id;
            segment.extend_from_slice(&record.encode());
            records.push(record);
        }

        // Seek to commit_seq=500 via offset formula.
        let target_seq = 500u64;
        #[allow(clippy::cast_possible_truncation)]
        let offset = record_offset(target_seq, start_seq) as usize;
        let record_bytes = &segment[offset..offset + COMMIT_MARKER_RECORD_BYTES];
        let record = CommitMarkerRecord::decode(record_bytes).expect("decode at offset");
        assert_eq!(record.commit_seq, target_seq);
        assert_eq!(record, records[500]);
    }

    #[test]
    fn test_commit_seq_allocation_from_file_length() {
        let start_seq = 5000u64;
        // 10 records: file_len = 36 + 10 * 88 = 916
        let file_len = MARKER_SEGMENT_HEADER_BYTES as u64 + 10 * COMMIT_MARKER_RECORD_BYTES as u64;
        assert_eq!(file_len, 916);

        let next = next_commit_seq_from_file_len(start_seq, file_len);
        assert_eq!(next, start_seq + 10);
    }

    #[test]
    fn test_torn_tail_handling() {
        let start_seq = 0u64;
        let header = MarkerSegmentHeader::new(0, start_seq);
        let mut segment = Vec::from(header.encode());

        let mut prev = [0u8; ID_SIZE];
        for i in 0..5u64 {
            let record = make_test_record(start_seq + i, prev);
            prev = record.marker_id;
            segment.extend_from_slice(&record.encode());
        }

        // Append 44 partial bytes (half a record).
        segment.extend_from_slice(&[0xDE; 44]);

        let result = check_segment_integrity(&segment);
        match result {
            Err(MarkerError::TornTail {
                complete_records,
                trailing_bytes,
            }) => {
                assert_eq!(complete_records, 5);
                assert_eq!(trailing_bytes, 44);
            }
            other => unreachable!("expected TornTail, got {other:?}"),
        }
    }

    #[test]
    fn test_torn_tail_corrupt_last_record() {
        let start_seq = 0u64;
        let header = MarkerSegmentHeader::new(0, start_seq);
        let mut segment = Vec::from(header.encode());

        let mut prev = [0u8; ID_SIZE];
        for i in 0..5u64 {
            let record = make_test_record(start_seq + i, prev);
            prev = record.marker_id;
            segment.extend_from_slice(&record.encode());
        }

        // Corrupt record 4's xxh3 (last 8 bytes of record 4).
        let record_4_end = MARKER_SEGMENT_HEADER_BYTES + 5 * COMMIT_MARKER_RECORD_BYTES;
        let xxh3_start = record_4_end - 8;
        segment[xxh3_start] ^= 0xFF;

        let result = check_segment_integrity(&segment);
        match result {
            Err(MarkerError::TornTail {
                complete_records, ..
            }) => {
                assert_eq!(complete_records, 4, "valid prefix is records 0-3");
            }
            other => unreachable!("expected TornTail, got {other:?}"),
        }
    }

    #[test]
    fn test_commit_seq_mismatch_detected() {
        let start_seq = 100u64;
        let header = MarkerSegmentHeader::new(0, start_seq);
        let mut segment = Vec::from(header.encode());

        let first = make_test_record(start_seq, [0u8; ID_SIZE]);
        let second = make_test_record(start_seq + 2, first.marker_id);
        segment.extend_from_slice(&first.encode());
        segment.extend_from_slice(&second.encode());

        let result = check_segment_integrity(&segment);
        match result {
            Err(MarkerError::CommitSeqMismatch { expected, actual }) => {
                assert_eq!(expected, start_seq + 1);
                assert_eq!(actual, start_seq + 2);
            }
            other => unreachable!("expected CommitSeqMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_binary_search_by_time() {
        let start_seq = 0u64;
        let base_ns = 1_000_000_000_000_000_000u64;

        let records: Vec<CommitMarkerRecord> = (0..100u64)
            .scan([0u8; ID_SIZE], |prev, i| {
                let capsule = [(i & 0xFF) as u8; ID_SIZE];
                let proof = [((i >> 8) & 0xFF) as u8; ID_SIZE];
                let time_ns = base_ns + i * 1_000_000;
                let record = CommitMarkerRecord::new(i, time_ns, capsule, proof, *prev);
                *prev = record.marker_id;
                Some(record)
            })
            .collect();

        // Search for time at commit_seq=50.
        let target_ns = base_ns + 50 * 1_000_000;
        #[allow(clippy::cast_possible_truncation)]
        let result = binary_search_by_time(start_seq, 100, target_ns, |seq| {
            records.get(seq as usize).cloned()
        });
        assert_eq!(result, Some(50));

        // Search before all records.
        #[allow(clippy::cast_possible_truncation)]
        let result = binary_search_by_time(start_seq, 100, base_ns - 1, |seq| {
            records.get(seq as usize).cloned()
        });
        assert_eq!(result, None);

        // Search after all records.
        #[allow(clippy::cast_possible_truncation)]
        let result = binary_search_by_time(start_seq, 100, u64::MAX, |seq| {
            records.get(seq as usize).cloned()
        });
        assert_eq!(result, Some(99));
    }

    #[test]
    fn test_fork_detection() {
        let base_ns = 1_700_000_000_000_000_000u64;
        let mut prev = [0u8; ID_SIZE];

        // Build a shared prefix of 10 commits.
        let mut shared = Vec::new();
        for i in 0..10u64 {
            let capsule = [0xAAu8; ID_SIZE];
            let proof = [0xBBu8; ID_SIZE];
            let record = CommitMarkerRecord::new(i, base_ns + i * 1_000_000, capsule, proof, prev);
            prev = record.marker_id;
            shared.push(record);
        }

        // Fork A: continues from shared[9].
        let mut fork_a = shared.clone();
        let mut prev_a = shared[9].marker_id;
        for i in 10..15u64 {
            let capsule = [0x11u8; ID_SIZE];
            let proof = [0x22u8; ID_SIZE];
            let record =
                CommitMarkerRecord::new(i, base_ns + i * 1_000_000, capsule, proof, prev_a);
            prev_a = record.marker_id;
            fork_a.push(record);
        }

        // Fork B: different content from commit 10.
        let mut fork_b = shared.clone();
        let mut prev_b = shared[9].marker_id;
        for i in 10..13u64 {
            let capsule = [0x33u8; ID_SIZE];
            let proof = [0x44u8; ID_SIZE];
            let record =
                CommitMarkerRecord::new(i, base_ns + i * 1_000_000, capsule, proof, prev_b);
            prev_b = record.marker_id;
            fork_b.push(record);
        }

        // Binary search for divergence point.
        let min_len = fork_a.len().min(fork_b.len());
        let mut lo = 0usize;
        let mut hi = min_len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if fork_a[mid].marker_id == fork_b[mid].marker_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Greatest common prefix is at index lo - 1.
        assert_eq!(lo, 10, "divergence starts at commit_seq 10");
        assert_eq!(
            fork_a[9].marker_id, fork_b[9].marker_id,
            "last matching marker_id is at seq 9"
        );
        assert_ne!(
            fork_a[10].marker_id, fork_b[10].marker_id,
            "first divergence at seq 10"
        );
    }

    #[test]
    fn test_hash_chain_integrity() {
        let mut prev = [0u8; ID_SIZE];
        let mut records = Vec::new();

        for i in 0..10u64 {
            let record = make_test_record(i, prev);
            prev = record.marker_id;
            records.push(record);
        }

        // Verify chain links.
        for i in 1..records.len() {
            assert_eq!(
                records[i].prev_marker_id,
                records[i - 1].marker_id,
                "record {i} prev_marker_id must link to record {}'s marker_id",
                i - 1
            );
        }
        assert_eq!(records[0].prev_marker_id, [0u8; ID_SIZE], "genesis is zero");

        // Verify all marker_ids.
        for record in &records {
            assert!(
                record.verify_marker_id(),
                "marker_id must be verifiable for commit_seq {}",
                record.commit_seq
            );
        }

        // Tamper with one record and verify detection.
        let mut tampered = records[5].clone();
        tampered.capsule_object_id[0] ^= 0xFF;
        assert!(
            !tampered.verify_marker_id(),
            "tampered record must fail marker_id verification"
        );
    }

    #[test]
    fn test_marker_no_mem_size_of() {
        // Verify on-disk sizes are constants, not derived from mem::size_of.
        assert_eq!(MARKER_SEGMENT_HEADER_BYTES, 36);
        assert_eq!(COMMIT_MARKER_RECORD_BYTES, 88);

        // Verify the actual struct sizes may differ from on-disk sizes
        // (padding), confirming we don't use mem::size_of for offsets.
        let header = MarkerSegmentHeader::new(0, 0);
        let encoded_header = header.encode();
        assert_eq!(
            encoded_header.len(),
            36,
            "header on-disk size is a constant"
        );

        let record = make_test_record(0, [0u8; ID_SIZE]);
        let encoded_record = record.encode();
        assert_eq!(
            encoded_record.len(),
            88,
            "record on-disk size is a constant"
        );
    }

    #[test]
    fn test_header_bad_magic_rejected() {
        let header = MarkerSegmentHeader::new(1, 0);
        let mut encoded = header.encode();
        encoded[0] = b'X';

        let result = MarkerSegmentHeader::decode(&encoded);
        assert_eq!(result.unwrap_err(), MarkerError::BadMagic);
    }

    #[test]
    fn test_header_checksum_tamper_detected() {
        let header = MarkerSegmentHeader::new(1, 0);
        let mut encoded = header.encode();
        // Tamper with segment_id.
        encoded[8] ^= 0x01;

        let result = MarkerSegmentHeader::decode(&encoded);
        assert!(matches!(
            result.unwrap_err(),
            MarkerError::HeaderChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_record_checksum_tamper_detected() {
        let record = make_test_record(42, [0u8; ID_SIZE]);
        let mut encoded = record.encode();
        // Tamper with commit_time_unix_ns.
        encoded[10] ^= 0x01;

        let result = CommitMarkerRecord::decode(&encoded);
        assert!(matches!(
            result.unwrap_err(),
            MarkerError::RecordChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_segment_id_for_commit_seq() {
        assert_eq!(segment_id_for_commit_seq(0), 0);
        assert_eq!(segment_id_for_commit_seq(999_999), 0);
        assert_eq!(segment_id_for_commit_seq(1_000_000), 1);
        assert_eq!(segment_id_for_commit_seq(2_500_000), 2);
    }

    #[test]
    fn test_start_commit_seq_for_segment() {
        assert_eq!(start_commit_seq_for_segment(0), 0);
        assert_eq!(start_commit_seq_for_segment(1), 1_000_000);
        assert_eq!(start_commit_seq_for_segment(5), 5_000_000);
    }

    #[test]
    fn test_record_offset_formula() {
        let offset = record_offset(500, 0);
        assert_eq!(
            offset,
            MARKER_SEGMENT_HEADER_BYTES as u64 + 500 * COMMIT_MARKER_RECORD_BYTES as u64
        );

        let offset2 = record_offset(1_000_050, 1_000_000);
        assert_eq!(
            offset2,
            MARKER_SEGMENT_HEADER_BYTES as u64 + 50 * COMMIT_MARKER_RECORD_BYTES as u64
        );
    }

    #[test]
    fn test_error_display() {
        let err = MarkerError::BadMagic;
        assert_eq!(err.to_string(), "bad magic in marker segment header");

        let err = MarkerError::TornTail {
            complete_records: 5,
            trailing_bytes: 44,
        };
        assert!(err.to_string().contains("torn tail"));
        assert!(err.to_string().contains('5'));
    }

    #[test]
    fn test_marker_id_deterministic() {
        let capsule = [0xAA; ID_SIZE];
        let proof = [0xBB; ID_SIZE];
        let prev = [0u8; ID_SIZE];

        let id1 = compute_marker_id(1, 100, &capsule, &proof, &prev);
        let id2 = compute_marker_id(1, 100, &capsule, &proof, &prev);
        assert_eq!(id1, id2, "marker_id must be deterministic");

        // Different input → different output.
        let id3 = compute_marker_id(2, 100, &capsule, &proof, &prev);
        assert_ne!(id1, id3);
    }

    #[test]
    fn prop_marker_id_unique() {
        let mut rng_state = 0xDEAD_BEEF_CAFE_BABE_u64;
        let mut observed_ids = HashSet::new();

        for i in 0..2048u64 {
            // Deterministic pseudo-random generation to avoid flaky tests.
            rng_state = rng_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let mut capsule = [0u8; ID_SIZE];
            let mut proof = [0u8; ID_SIZE];
            let mut prev = [0u8; ID_SIZE];

            capsule[..8].copy_from_slice(&rng_state.to_le_bytes());
            proof[..8].copy_from_slice(&rng_state.rotate_left(13).to_le_bytes());
            prev[..8].copy_from_slice(&rng_state.rotate_right(7).to_le_bytes());
            capsule[8..16].copy_from_slice(&i.to_le_bytes());
            proof[8..16].copy_from_slice(&(i ^ rng_state).to_le_bytes());
            prev[8..16].copy_from_slice(&(i.wrapping_mul(17)).to_le_bytes());

            let marker_id =
                compute_marker_id(i, 1_700_000_000_000_000_000 + i, &capsule, &proof, &prev);
            assert!(
                observed_ids.insert(marker_id),
                "marker_id collision at sample {i}: {marker_id:02X?}"
            );
        }
    }

    #[test]
    fn prop_density_invariant_holds() {
        for count in 1..=256u64 {
            let start_seq = 10_000 + count * 31;
            let header = MarkerSegmentHeader::new(segment_id_for_commit_seq(start_seq), start_seq);
            let mut segment = Vec::from(header.encode());
            let mut prev = [0u8; ID_SIZE];

            for i in 0..count {
                let record = make_test_record(start_seq + i, prev);
                prev = record.marker_id;
                segment.extend_from_slice(&record.encode());
            }

            let integrity = check_segment_integrity(&segment).expect("segment should be dense");
            assert_eq!(integrity, count);
        }
    }

    #[test]
    fn test_e2e_marker_stream_recovery() {
        let start_seq = 20_000u64;
        let header = MarkerSegmentHeader::new(segment_id_for_commit_seq(start_seq), start_seq);
        let mut segment = Vec::from(header.encode());
        let mut expected = Vec::new();
        let mut prev = [0u8; ID_SIZE];

        for i in 0..1000u64 {
            let record = make_test_record(start_seq + i, prev);
            prev = record.marker_id;
            segment.extend_from_slice(&record.encode());
            expected.push(record);
        }

        // Simulate crash: truncate in the middle of the final record.
        segment.truncate(segment.len() - (COMMIT_MARKER_RECORD_BYTES / 2));

        let recovered = recover_valid_prefix(&segment).expect("recovery should succeed");
        assert_eq!(recovered.len(), expected.len() - 1);
        assert_eq!(recovered, expected[..expected.len() - 1]);

        let integrity = check_segment_integrity(&segment);
        match integrity {
            Err(MarkerError::TornTail {
                complete_records,
                trailing_bytes,
            }) => {
                assert_eq!(complete_records, 999);
                assert_eq!(trailing_bytes, COMMIT_MARKER_RECORD_BYTES / 2);
            }
            other => unreachable!("expected torn-tail integrity result, got {other:?}"),
        }
    }

    #[test]
    fn test_e2e_time_travel_query() {
        let start_seq = 5_000u64;
        let count = 256u64;
        let base_ns = 1_900_000_000_000_000_000u64;
        let mut prev = [0u8; ID_SIZE];
        let mut records = Vec::new();

        for i in 0..count {
            let capsule = [(i & 0xFF) as u8; ID_SIZE];
            let proof = [((i >> 8) & 0xFF) as u8; ID_SIZE];
            let record = CommitMarkerRecord::new(
                start_seq + i,
                base_ns + i * 2_000_000,
                capsule,
                proof,
                prev,
            );
            prev = record.marker_id;
            records.push(record);
        }

        let lookup = |seq: u64| -> Option<CommitMarkerRecord> {
            if seq < start_seq {
                return None;
            }
            let idx = usize::try_from(seq - start_seq).ok()?;
            records.get(idx).cloned()
        };

        // Before the first marker.
        assert_eq!(
            binary_search_by_time(start_seq, count, base_ns - 1, lookup),
            None
        );
        // Exact hit.
        assert_eq!(
            binary_search_by_time(start_seq, count, base_ns + 40 * 2_000_000, lookup),
            Some(start_seq + 40)
        );
        // Between two commits should select the lower commit_seq.
        assert_eq!(
            binary_search_by_time(
                start_seq,
                count,
                base_ns + 40 * 2_000_000 + 1_000_000,
                lookup
            ),
            Some(start_seq + 40)
        );
        // After the final marker.
        assert_eq!(
            binary_search_by_time(start_seq, count, u64::MAX, lookup),
            Some(start_seq + count - 1)
        );
    }
}
