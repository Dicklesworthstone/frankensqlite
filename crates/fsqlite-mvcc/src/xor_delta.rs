//! XOR-delta compression for MVCC version chains (§3.4.4, `bd-1hi.16`).
//!
//! This module keeps delta compression and durability concerns separate:
//! - Compression: sparse XOR deltas between adjacent page versions.
//! - Durability: delta blobs can be wrapped as ECS objects (`PatchKind::SparseXor`)
//!   and then treated like any other object by higher layers.

use std::fmt;

use fsqlite_types::ecs::PatchKind;
use fsqlite_types::{MergePageKind, ObjectId, PayloadHash};

/// Sparse-delta magic bytes (`"XD"`).
pub const DELTA_MAGIC: [u8; 2] = *b"XD";
/// Sparse-delta wire version.
pub const DELTA_VERSION: u8 = 1;
/// Sparse-delta header size in bytes.
pub const DELTA_HEADER_BYTES: usize = 8;
/// Run header size (`offset:u16`, `len:u16`) in bytes.
pub const DELTA_RUN_HEADER_BYTES: usize = 4;
/// Fixed overhead used by the threshold estimator.
pub const DELTA_FIXED_OVERHEAD_BYTES: usize = 16;
/// Additional sparse encoding overhead percentage (5%).
pub const DELTA_SPARSE_OVERHEAD_PCT: usize = 5;
/// Default threshold for `PRAGMA fsqlite.delta_threshold_pct`.
pub const DEFAULT_DELTA_THRESHOLD_PCT: u8 = 25;

const MAX_PAGE_BYTES_FOR_U16_OFFSETS: usize = (u16::MAX as usize) + 1;
const OBJECT_ID_HEADER_SPARSE_XOR: &[u8] = b"fsqlite:patch:sparse_xor:v1";

/// Errors raised by XOR-delta encoding/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaError {
    /// Base and target page lengths differ.
    LengthMismatch { base_len: usize, target_len: usize },
    /// Page size is unsupported for u16 run offsets.
    UnsupportedPageSize { page_len: usize },
    /// Threshold percentage is invalid.
    InvalidThresholdPct { threshold_pct: u8 },
    /// Delta header is too short.
    TruncatedHeader { actual_len: usize },
    /// Delta magic bytes do not match.
    InvalidMagic { actual: [u8; 2] },
    /// Delta version is unsupported.
    UnsupportedVersion { version: u8 },
    /// Run header is truncated.
    TruncatedRunHeader { at: usize, remaining: usize },
    /// Run data is truncated.
    TruncatedRunData {
        at: usize,
        expected_len: usize,
        remaining: usize,
    },
    /// Run offset+length exceeds page bounds.
    RunOutOfBounds {
        offset: usize,
        len: usize,
        page_len: usize,
    },
    /// Run with zero length is invalid.
    ZeroLengthRun { offset: usize },
    /// Header nonzero count disagrees with run payload.
    NonzeroCountMismatch {
        header_nonzero_count: u32,
        actual_nonzero_count: u32,
    },
    /// Nonzero count overflowed u32.
    NonzeroCountOverflow { nonzero_count: usize },
}

impl fmt::Display for DeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthMismatch {
                base_len,
                target_len,
            } => {
                write!(
                    f,
                    "base/target page length mismatch: base={base_len} target={target_len}"
                )
            }
            Self::UnsupportedPageSize { page_len } => write!(
                f,
                "unsupported page size for sparse-u16 offsets: {page_len} (max {MAX_PAGE_BYTES_FOR_U16_OFFSETS})"
            ),
            Self::InvalidThresholdPct { threshold_pct } => write!(
                f,
                "invalid delta threshold percentage: {threshold_pct} (must be 1..=99)"
            ),
            Self::TruncatedHeader { actual_len } => {
                write!(
                    f,
                    "truncated delta header: expected >= {DELTA_HEADER_BYTES}, got {actual_len}"
                )
            }
            Self::InvalidMagic { actual } => {
                write!(f, "invalid delta magic: {:?} (expected \"XD\")", actual)
            }
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported delta version: {version}")
            }
            Self::TruncatedRunHeader { at, remaining } => write!(
                f,
                "truncated run header at byte {at}: remaining={remaining}, need={DELTA_RUN_HEADER_BYTES}"
            ),
            Self::TruncatedRunData {
                at,
                expected_len,
                remaining,
            } => write!(
                f,
                "truncated run data at byte {at}: remaining={remaining}, expected={expected_len}"
            ),
            Self::RunOutOfBounds {
                offset,
                len,
                page_len,
            } => write!(
                f,
                "run out of bounds: offset={offset} len={len} page_len={page_len}"
            ),
            Self::ZeroLengthRun { offset } => {
                write!(f, "zero-length run at offset={offset}")
            }
            Self::NonzeroCountMismatch {
                header_nonzero_count,
                actual_nonzero_count,
            } => write!(
                f,
                "nonzero count mismatch: header={header_nonzero_count} actual={actual_nonzero_count}"
            ),
            Self::NonzeroCountOverflow { nonzero_count } => {
                write!(f, "nonzero count overflow: {nonzero_count}")
            }
        }
    }
}

impl std::error::Error for DeltaError {}

/// Configuration backing `PRAGMA fsqlite.delta_threshold_pct`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaThresholdConfig {
    threshold_pct: u8,
}

impl DeltaThresholdConfig {
    /// Build a new threshold config.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaError::InvalidThresholdPct`] when `threshold_pct` is not
    /// in the inclusive range `1..=99`.
    pub fn new(threshold_pct: u8) -> Result<Self, DeltaError> {
        validate_threshold_pct(threshold_pct)?;
        Ok(Self { threshold_pct })
    }

    /// Current threshold percentage.
    #[must_use]
    pub const fn threshold_pct(self) -> u8 {
        self.threshold_pct
    }

    /// Apply a `PRAGMA fsqlite.delta_threshold_pct = <n>` style update.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaError::InvalidThresholdPct`] when `threshold_pct` is not
    /// in the inclusive range `1..=99`.
    pub fn set_from_pragma(&mut self, threshold_pct: u8) -> Result<(), DeltaError> {
        validate_threshold_pct(threshold_pct)?;
        self.threshold_pct = threshold_pct;
        Ok(())
    }

    /// Maximum accepted encoded-delta bytes for `page_size`.
    ///
    /// # Errors
    ///
    /// Returns [`DeltaError::InvalidThresholdPct`] when the config is invalid.
    pub fn max_delta_bytes(self, page_size: usize) -> Result<usize, DeltaError> {
        max_delta_bytes(page_size, self.threshold_pct)
    }

    /// Decide if a page diff should be stored as sparse XOR delta.
    ///
    /// # Errors
    ///
    /// Returns a [`DeltaError`] for invalid page lengths or invalid threshold
    /// configuration.
    pub fn use_delta(self, base: &[u8], target: &[u8]) -> Result<bool, DeltaError> {
        use_delta(base, target, self.threshold_pct)
    }
}

impl Default for DeltaThresholdConfig {
    fn default() -> Self {
        Self {
            threshold_pct: DEFAULT_DELTA_THRESHOLD_PCT,
        }
    }
}

/// Stored representation for a page version payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaEncoding {
    /// Store the full page image (no compression).
    FullImage(Vec<u8>),
    /// Store sparse XOR delta bytes.
    SparseXor(Vec<u8>),
}

impl DeltaEncoding {
    /// Size in bytes of the stored payload.
    #[must_use]
    pub fn storage_len(&self) -> usize {
        match self {
            Self::FullImage(bytes) | Self::SparseXor(bytes) => bytes.len(),
        }
    }
}

/// Sparse XOR delta as an ECS object payload (`PatchKind::SparseXor`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseXorDeltaObject {
    /// Content-addressed object id of this delta payload.
    pub object_id: ObjectId,
    /// Patch kind for index integration.
    pub patch_kind: PatchKind,
    /// Canonical sparse-XOR payload bytes.
    pub bytes: Vec<u8>,
}

impl SparseXorDeltaObject {
    /// Construct a deterministic object wrapper from raw sparse-XOR bytes.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        let payload_hash = PayloadHash::blake3(&bytes);
        let object_id = ObjectId::derive(OBJECT_ID_HEADER_SPARSE_XOR, payload_hash);
        Self {
            object_id,
            patch_kind: PatchKind::SparseXor,
            bytes,
        }
    }
}

/// Estimate encoded sparse-delta size for `nonzero_bytes`.
#[must_use]
pub fn estimate_sparse_delta_size(nonzero_bytes: usize) -> usize {
    let sparse_overhead = nonzero_bytes.div_ceil(100 / DELTA_SPARSE_OVERHEAD_PCT);
    DELTA_FIXED_OVERHEAD_BYTES
        .saturating_add(nonzero_bytes)
        .saturating_add(sparse_overhead)
}

/// Count nonzero bytes in `base XOR target`.
///
/// # Errors
///
/// Returns [`DeltaError::LengthMismatch`] when `base.len() != target.len()`, and
/// [`DeltaError::UnsupportedPageSize`] when the page length exceeds the format limit.
pub fn count_nonzero_xor(base: &[u8], target: &[u8]) -> Result<usize, DeltaError> {
    validate_pages(base, target)?;
    let mut nonzero = 0usize;
    for (&lhs, &rhs) in base.iter().zip(target) {
        if lhs ^ rhs != 0 {
            nonzero = nonzero.saturating_add(1);
        }
    }
    Ok(nonzero)
}

/// Maximum accepted encoded-delta bytes for a page and threshold.
///
/// `threshold_pct=25` means "require at least 25% savings", so max delta bytes
/// are `page_size * 75 / 100`.
///
/// # Errors
///
/// Returns [`DeltaError::InvalidThresholdPct`] when `threshold_pct` is not in
/// the inclusive range `1..=99`.
pub fn max_delta_bytes(page_size: usize, threshold_pct: u8) -> Result<usize, DeltaError> {
    validate_threshold_pct(threshold_pct)?;
    let keep_pct = usize::from(100_u8.saturating_sub(threshold_pct));
    Ok(page_size.saturating_mul(keep_pct) / 100)
}

/// Decide whether to use sparse XOR delta for this page transition.
///
/// # Errors
///
/// Returns a [`DeltaError`] for invalid page lengths or threshold configuration.
pub fn use_delta(base: &[u8], target: &[u8], threshold_pct: u8) -> Result<bool, DeltaError> {
    validate_pages(base, target)?;
    let nonzero = count_nonzero_xor(base, target)?;
    let estimated_delta_size = estimate_sparse_delta_size(nonzero);
    let threshold_bytes = max_delta_bytes(base.len(), threshold_pct)?;
    Ok(estimated_delta_size < threshold_bytes)
}

/// Encode either full-image or sparse-XOR representation using `config`.
///
/// # Errors
///
/// Returns a [`DeltaError`] for invalid page lengths or malformed thresholds.
pub fn encode_page_delta(
    base: &[u8],
    target: &[u8],
    config: DeltaThresholdConfig,
) -> Result<DeltaEncoding, DeltaError> {
    if config.use_delta(base, target)? {
        Ok(DeltaEncoding::SparseXor(encode_sparse_xor_delta(
            base, target,
        )?))
    } else {
        Ok(DeltaEncoding::FullImage(target.to_vec()))
    }
}

/// Encode a sparse XOR delta in canonical wire format.
///
/// Header (8 bytes):
/// - magic\[2\] = "XD"
/// - version\[1\] = 1
/// - flags\[1\] = 0
/// - nonzero\_count\[4\] little-endian
///
/// Runs:
/// - offset\[u16 little-endian\]
/// - len\[u16 little-endian\]
/// - data\[len\]
///
/// # Errors
///
/// Returns a [`DeltaError`] for invalid page sizes or length mismatches.
pub fn encode_sparse_xor_delta(base: &[u8], target: &[u8]) -> Result<Vec<u8>, DeltaError> {
    validate_pages(base, target)?;
    let nonzero = count_nonzero_xor(base, target)?;
    let nonzero_u32 = u32::try_from(nonzero).map_err(|_| DeltaError::NonzeroCountOverflow {
        nonzero_count: nonzero,
    })?;

    let mut encoded = Vec::with_capacity(DELTA_HEADER_BYTES + nonzero.saturating_add(64));
    encoded.extend_from_slice(&DELTA_MAGIC);
    encoded.push(DELTA_VERSION);
    encoded.push(0); // flags
    encoded.extend_from_slice(&nonzero_u32.to_le_bytes());

    let mut cursor = 0usize;
    while cursor < base.len() {
        if base[cursor] ^ target[cursor] == 0 {
            cursor = cursor.saturating_add(1);
            continue;
        }

        let run_start = cursor;
        let mut run_len = 0usize;
        while cursor < base.len()
            && (base[cursor] ^ target[cursor] != 0)
            && run_len < usize::from(u16::MAX)
        {
            cursor = cursor.saturating_add(1);
            run_len = run_len.saturating_add(1);
        }

        let run_offset_u16 = u16::try_from(run_start).map_err(|_| DeltaError::RunOutOfBounds {
            offset: run_start,
            len: run_len,
            page_len: base.len(),
        })?;
        let run_len_u16 = u16::try_from(run_len).map_err(|_| DeltaError::RunOutOfBounds {
            offset: run_start,
            len: run_len,
            page_len: base.len(),
        })?;

        encoded.extend_from_slice(&run_offset_u16.to_le_bytes());
        encoded.extend_from_slice(&run_len_u16.to_le_bytes());
        for (&lhs, &rhs) in base[run_start..run_start + run_len]
            .iter()
            .zip(&target[run_start..run_start + run_len])
        {
            encoded.push(lhs ^ rhs);
        }
    }

    Ok(encoded)
}

/// Decode sparse XOR delta bytes and apply them to `base`.
///
/// # Errors
///
/// Returns a [`DeltaError`] when the delta payload is malformed or out of bounds.
pub fn decode_sparse_xor_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, DeltaError> {
    validate_page_len(base.len())?;
    if delta.len() < DELTA_HEADER_BYTES {
        return Err(DeltaError::TruncatedHeader {
            actual_len: delta.len(),
        });
    }

    let magic = [delta[0], delta[1]];
    if magic != DELTA_MAGIC {
        return Err(DeltaError::InvalidMagic { actual: magic });
    }
    if delta[2] != DELTA_VERSION {
        return Err(DeltaError::UnsupportedVersion { version: delta[2] });
    }

    let mut nonzero_buf = [0u8; 4];
    nonzero_buf.copy_from_slice(&delta[4..8]);
    let expected_nonzero = u32::from_le_bytes(nonzero_buf);

    let mut out = base.to_vec();
    let mut cursor = DELTA_HEADER_BYTES;
    let mut actual_nonzero = 0u32;

    while cursor < delta.len() {
        let remaining = delta.len().saturating_sub(cursor);
        if remaining < DELTA_RUN_HEADER_BYTES {
            return Err(DeltaError::TruncatedRunHeader {
                at: cursor,
                remaining,
            });
        }

        let mut off_buf = [0u8; 2];
        off_buf.copy_from_slice(&delta[cursor..cursor + 2]);
        let offset = usize::from(u16::from_le_bytes(off_buf));

        let mut len_buf = [0u8; 2];
        len_buf.copy_from_slice(&delta[cursor + 2..cursor + 4]);
        let len = usize::from(u16::from_le_bytes(len_buf));

        cursor = cursor.saturating_add(DELTA_RUN_HEADER_BYTES);
        if len == 0 {
            return Err(DeltaError::ZeroLengthRun { offset });
        }

        let remaining_data = delta.len().saturating_sub(cursor);
        if remaining_data < len {
            return Err(DeltaError::TruncatedRunData {
                at: cursor,
                expected_len: len,
                remaining: remaining_data,
            });
        }

        let end = offset.checked_add(len).ok_or(DeltaError::RunOutOfBounds {
            offset,
            len,
            page_len: out.len(),
        })?;
        if end > out.len() {
            return Err(DeltaError::RunOutOfBounds {
                offset,
                len,
                page_len: out.len(),
            });
        }

        for (dst, &delta_byte) in out[offset..end]
            .iter_mut()
            .zip(&delta[cursor..cursor + len])
        {
            *dst ^= delta_byte;
            if delta_byte != 0 {
                actual_nonzero = actual_nonzero.saturating_add(1);
            }
        }
        cursor = cursor.saturating_add(len);
    }

    if actual_nonzero != expected_nonzero {
        return Err(DeltaError::NonzeroCountMismatch {
            header_nonzero_count: expected_nonzero,
            actual_nonzero_count: actual_nonzero,
        });
    }

    Ok(out)
}

/// Reconstruct all versions from newest full image plus deltas.
///
/// Input deltas must be ordered newest-to-oldest:
/// `delta(Vn-1, Vn), delta(Vn-2, Vn-1), ...`.
///
/// Returns versions ordered newest-to-oldest.
///
/// # Errors
///
/// Returns a [`DeltaError`] when any delta is malformed or incompatible.
pub fn reconstruct_chain_from_newest<T: AsRef<[u8]>>(
    newest_full: &[u8],
    deltas_newest_to_oldest: &[T],
) -> Result<Vec<Vec<u8>>, DeltaError> {
    validate_page_len(newest_full.len())?;

    let mut current = newest_full.to_vec();
    let mut versions = Vec::with_capacity(deltas_newest_to_oldest.len().saturating_add(1));
    versions.push(current.clone());

    for delta in deltas_newest_to_oldest {
        current = decode_sparse_xor_delta(&current, delta.as_ref())?;
        versions.push(current.clone());
    }

    Ok(versions)
}

// ---------------------------------------------------------------------------
// §3.4.5 GF(256) patch algebra + merge safety policy
// ---------------------------------------------------------------------------

/// Required-forbidden page kinds for raw XOR merge.
pub const RAW_XOR_FORBIDDEN_PAGE_KINDS: [MergePageKind; 7] = [
    MergePageKind::BtreeInteriorTable,
    MergePageKind::BtreeLeafTable,
    MergePageKind::BtreeInteriorIndex,
    MergePageKind::BtreeLeafIndex,
    MergePageKind::Overflow,
    MergePageKind::Freelist,
    MergePageKind::PointerMap,
];

/// Write-merge policy (`PRAGMA fsqlite.write_merge`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteMergePolicy {
    /// Conflicts abort/retry (first-committer wins).
    Off,
    /// Allow only semantically justified merges (intent replay / structured patch).
    Safe,
    /// Debug-only unsafe experiments for opaque pages.
    LabUnsafe,
}

/// Merge path requested for a page conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeMethod {
    /// Raw byte-level XOR merge.
    RawXor,
    /// Deterministic intent replay.
    IntentReplay,
    /// Structured patch merge keyed by stable identifiers.
    StructuredPatch,
}

/// High-level outcome for a page conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConflictOutcome {
    /// No blocking conflict; both commits proceed.
    BothCommit,
    /// Conflict abort/retry.
    SecondAbortRetry,
    /// Conflict resolved via intent replay in SAFE mode.
    SecondCommitsViaIntentReplay,
}

/// Merge-safety policy and patch-composition errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeSafetyError {
    /// Patch vectors must have equal length.
    LengthMismatch { base_len: usize, delta_len: usize },
    /// Overlapping support is forbidden for disjoint-delta composition.
    OverlappingSupport { offset: usize },
    /// Raw XOR merge is forbidden for this page kind/policy combination.
    RawXorForbidden {
        page_kind: MergePageKind,
        policy: WriteMergePolicy,
    },
    /// LAB_UNSAFE is rejected in release builds.
    LabUnsafeRejectedInRelease,
}

impl fmt::Display for MergeSafetyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthMismatch {
                base_len,
                delta_len,
            } => {
                write!(
                    f,
                    "patch length mismatch: base={base_len} delta={delta_len}"
                )
            }
            Self::OverlappingSupport { offset } => {
                write!(f, "overlapping patch support at offset={offset}")
            }
            Self::RawXorForbidden { page_kind, policy } => {
                write!(
                    f,
                    "raw XOR merge forbidden for page_kind={page_kind:?} policy={policy:?}"
                )
            }
            Self::LabUnsafeRejectedInRelease => {
                write!(f, "LAB_UNSAFE is rejected in release builds")
            }
        }
    }
}

impl std::error::Error for MergeSafetyError {}

/// GF(256) addition: XOR. In GF(2^8), addition and subtraction are both XOR.
#[inline]
#[must_use]
pub const fn gf256_add_byte(lhs: u8, rhs: u8) -> u8 {
    lhs ^ rhs
}

/// Return true when a page kind is structurally unsafe for raw XOR merge.
#[must_use]
pub const fn is_raw_xor_forbidden_page_kind(page_kind: MergePageKind) -> bool {
    !matches!(page_kind, MergePageKind::Opaque)
}

/// Resolve policy gating (`LAB_UNSAFE` is debug-only).
pub fn resolve_write_merge_policy(
    policy: WriteMergePolicy,
) -> Result<WriteMergePolicy, MergeSafetyError> {
    if policy == WriteMergePolicy::LabUnsafe && !cfg!(debug_assertions) {
        return Err(MergeSafetyError::LabUnsafeRejectedInRelease);
    }
    Ok(policy)
}

/// GF(256) patch addition for equal-length vectors.
pub fn gf256_patch_add(lhs: &[u8], rhs: &[u8]) -> Result<Vec<u8>, MergeSafetyError> {
    if lhs.len() != rhs.len() {
        return Err(MergeSafetyError::LengthMismatch {
            base_len: lhs.len(),
            delta_len: rhs.len(),
        });
    }
    Ok(lhs
        .iter()
        .zip(rhs.iter())
        .map(|(&left, &right)| gf256_add_byte(left, right))
        .collect())
}

/// Apply a GF(256) patch (`base + patch`) where `+` is XOR.
pub fn gf256_apply_patch(base: &[u8], patch: &[u8]) -> Result<Vec<u8>, MergeSafetyError> {
    gf256_patch_add(base, patch)
}

/// Build a dense XOR delta (`target XOR base`) for GF(256)-style patch algebra.
pub fn dense_xor_delta(base: &[u8], target: &[u8]) -> Result<Vec<u8>, MergeSafetyError> {
    if base.len() != target.len() {
        return Err(MergeSafetyError::LengthMismatch {
            base_len: base.len(),
            delta_len: target.len(),
        });
    }
    Ok(base
        .iter()
        .zip(target.iter())
        .map(|(lhs, rhs)| lhs ^ rhs)
        .collect())
}

/// Return true when two dense deltas have disjoint support.
pub fn deltas_disjoint(lhs: &[u8], rhs: &[u8]) -> Result<bool, MergeSafetyError> {
    if lhs.len() != rhs.len() {
        return Err(MergeSafetyError::LengthMismatch {
            base_len: lhs.len(),
            delta_len: rhs.len(),
        });
    }
    Ok(lhs
        .iter()
        .zip(rhs.iter())
        .all(|(&left, &right)| left == 0 || right == 0))
}

/// Compose two disjoint dense XOR deltas over `base` (§3.4.5 lemma).
///
/// Returns `base XOR delta_a XOR delta_b`, rejecting any overlapping support.
pub fn compose_disjoint_deltas(
    base: &[u8],
    delta_a: &[u8],
    delta_b: &[u8],
) -> Result<Vec<u8>, MergeSafetyError> {
    if base.len() != delta_a.len() {
        return Err(MergeSafetyError::LengthMismatch {
            base_len: base.len(),
            delta_len: delta_a.len(),
        });
    }
    if base.len() != delta_b.len() {
        return Err(MergeSafetyError::LengthMismatch {
            base_len: base.len(),
            delta_len: delta_b.len(),
        });
    }

    let mut out = base.to_vec();
    for (idx, ((dst, &left), &right)) in out
        .iter_mut()
        .zip(delta_a.iter())
        .zip(delta_b.iter())
        .enumerate()
    {
        if left != 0 && right != 0 {
            return Err(MergeSafetyError::OverlappingSupport { offset: idx });
        }
        *dst ^= left;
        *dst ^= right;
    }
    Ok(out)
}

/// Enforce raw XOR merge policy for `page_kind` and `policy`.
pub fn enforce_raw_xor_merge_policy(
    page_kind: MergePageKind,
    policy: WriteMergePolicy,
) -> Result<(), MergeSafetyError> {
    let resolved = resolve_write_merge_policy(policy)?;
    if is_raw_xor_forbidden_page_kind(page_kind)
        || resolved == WriteMergePolicy::Off
        || resolved == WriteMergePolicy::Safe
    {
        return Err(MergeSafetyError::RawXorForbidden {
            page_kind,
            policy: resolved,
        });
    }
    Ok(())
}

/// Attempt a raw XOR merge with policy and page-kind safety checks.
pub fn attempt_raw_xor_merge(
    base: &[u8],
    delta_a: &[u8],
    delta_b: &[u8],
    page_kind: MergePageKind,
    policy: WriteMergePolicy,
) -> Result<Vec<u8>, MergeSafetyError> {
    enforce_raw_xor_merge_policy(page_kind, policy)?;
    compose_disjoint_deltas(base, delta_a, delta_b)
}

/// Decide conflict handling for concurrent page writes.
pub fn decide_conflict_outcome(
    tx1_pages: &[u32],
    tx2_pages: &[u32],
    policy: WriteMergePolicy,
    page_kind: MergePageKind,
    requested_method: MergeMethod,
) -> Result<ConflictOutcome, MergeSafetyError> {
    let overlap = tx1_pages.iter().any(|page| tx2_pages.contains(page));
    if !overlap {
        return Ok(ConflictOutcome::BothCommit);
    }

    let resolved = resolve_write_merge_policy(policy)?;
    match resolved {
        WriteMergePolicy::Off => Ok(ConflictOutcome::SecondAbortRetry),
        WriteMergePolicy::Safe => match requested_method {
            MergeMethod::IntentReplay | MergeMethod::StructuredPatch => {
                Ok(ConflictOutcome::SecondCommitsViaIntentReplay)
            }
            MergeMethod::RawXor => Err(MergeSafetyError::RawXorForbidden {
                page_kind,
                policy: resolved,
            }),
        },
        WriteMergePolicy::LabUnsafe => match requested_method {
            MergeMethod::RawXor => {
                enforce_raw_xor_merge_policy(page_kind, resolved)?;
                Ok(ConflictOutcome::BothCommit)
            }
            MergeMethod::IntentReplay | MergeMethod::StructuredPatch => {
                Ok(ConflictOutcome::SecondCommitsViaIntentReplay)
            }
        },
    }
}

fn validate_pages(base: &[u8], target: &[u8]) -> Result<(), DeltaError> {
    if base.len() != target.len() {
        return Err(DeltaError::LengthMismatch {
            base_len: base.len(),
            target_len: target.len(),
        });
    }
    validate_page_len(base.len())
}

fn validate_page_len(page_len: usize) -> Result<(), DeltaError> {
    if page_len == 0 || page_len > MAX_PAGE_BYTES_FOR_U16_OFFSETS {
        Err(DeltaError::UnsupportedPageSize { page_len })
    } else {
        Ok(())
    }
}

fn validate_threshold_pct(threshold_pct: u8) -> Result<(), DeltaError> {
    if (1..=99).contains(&threshold_pct) {
        Ok(())
    } else {
        Err(DeltaError::InvalidThresholdPct { threshold_pct })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{gf256_inverse_byte, gf256_mul_byte};
    use proptest::prelude::*;

    fn page_pair_strategy(max_len: usize) -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
        (1usize..=max_len).prop_flat_map(|len| {
            (
                prop::collection::vec(any::<u8>(), len),
                prop::collection::vec(any::<u8>(), len),
            )
        })
    }

    fn mutate_run(page: &mut [u8], start: usize, len: usize, seed: u8) {
        for (index, byte) in page[start..start + len].iter_mut().enumerate() {
            let step = u8::try_from(index & 0xFF).expect("masked index fits u8");
            *byte = seed.wrapping_add(step).wrapping_add(1);
        }
    }

    #[test]
    fn test_xor_delta_encode_small_change() {
        let base = vec![0u8; 4096];
        let mut target = base.clone();
        mutate_run(&mut target, 100, 50, 0x22);

        let encoded = encode_sparse_xor_delta(&base, &target).unwrap();
        assert_eq!(encoded.len(), 62, "8-byte header + 4-byte run + 50 payload");
    }

    #[test]
    fn test_xor_delta_encode_large_change() {
        let base = vec![0u8; 4096];
        let mut target = base.clone();
        mutate_run(&mut target, 0, 3000, 0x33);

        let config = DeltaThresholdConfig::default();
        assert!(
            !config.use_delta(&base, &target).unwrap(),
            "3000-byte change must be rejected by default 25% threshold"
        );
        let encoded = encode_page_delta(&base, &target, config).unwrap();
        assert!(matches!(encoded, DeltaEncoding::FullImage(_)));
    }

    #[test]
    fn test_xor_delta_decode_exact() {
        let base = vec![0xAAu8; 4096];
        let mut target = base.clone();
        mutate_run(&mut target, 64, 17, 0x11);
        mutate_run(&mut target, 511, 33, 0x55);
        mutate_run(&mut target, 2048, 12, 0x88);

        let encoded = encode_sparse_xor_delta(&base, &target).unwrap();
        let decoded = decode_sparse_xor_delta(&base, &encoded).unwrap();
        assert_eq!(decoded, target);
    }

    #[test]
    fn test_xor_delta_chain_reconstruction() {
        let v1 = vec![0u8; 4096];
        let mut v2 = v1.clone();
        mutate_run(&mut v2, 100, 40, 0x20);
        let mut v3 = v2.clone();
        mutate_run(&mut v3, 1800, 24, 0x60);

        let delta_v2_v3 = encode_sparse_xor_delta(&v2, &v3).unwrap();
        let delta_v1_v2 = encode_sparse_xor_delta(&v1, &v2).unwrap();

        let reconstructed =
            reconstruct_chain_from_newest(&v3, &[delta_v2_v3, delta_v1_v2]).unwrap();
        assert_eq!(reconstructed.len(), 3);
        assert_eq!(reconstructed[0], v3);
        assert_eq!(reconstructed[1], v2);
        assert_eq!(reconstructed[2], v1);
    }

    #[test]
    fn test_sparse_encoding_format() {
        let base = vec![0u8; 4096];
        let mut target = base.clone();
        target[100..103].copy_from_slice(&[1, 2, 3]);
        target[1000..1002].copy_from_slice(&[4, 5]);
        target[2000] = 6;

        let encoded = encode_sparse_xor_delta(&base, &target).unwrap();

        let mut expected = vec![b'X', b'D', 1, 0, 6, 0, 0, 0];
        expected.extend_from_slice(&100u16.to_le_bytes());
        expected.extend_from_slice(&3u16.to_le_bytes());
        expected.extend_from_slice(&[1, 2, 3]);
        expected.extend_from_slice(&1000u16.to_le_bytes());
        expected.extend_from_slice(&2u16.to_le_bytes());
        expected.extend_from_slice(&[4, 5]);
        expected.extend_from_slice(&2000u16.to_le_bytes());
        expected.extend_from_slice(&1u16.to_le_bytes());
        expected.extend_from_slice(&[6]);

        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_use_delta_threshold() {
        let base = vec![0u8; 4096];
        let mut small = base.clone();
        mutate_run(&mut small, 200, 100, 0x10);
        let mut large = base.clone();
        mutate_run(&mut large, 0, 3500, 0x40);

        assert!(use_delta(&base, &small, DEFAULT_DELTA_THRESHOLD_PCT).unwrap());
        assert!(!use_delta(&base, &large, DEFAULT_DELTA_THRESHOLD_PCT).unwrap());
    }

    #[test]
    fn test_pragma_delta_threshold_pct() {
        let mut config = DeltaThresholdConfig::default();
        assert_eq!(config.threshold_pct(), 25);

        config.set_from_pragma(50).unwrap();
        assert_eq!(config.threshold_pct(), 50);
        assert_eq!(config.max_delta_bytes(4096).unwrap(), 2048);

        let base = vec![0u8; 4096];
        let mut change_1800 = base.clone();
        mutate_run(&mut change_1800, 0, 1800, 0x12);
        let mut change_2000 = base.clone();
        mutate_run(&mut change_2000, 0, 2000, 0x34);

        assert!(config.use_delta(&base, &change_1800).unwrap());
        assert!(!config.use_delta(&base, &change_2000).unwrap());
    }

    #[test]
    fn test_delta_chain_depth_bounded() {
        let mut versions = Vec::with_capacity(10);
        let mut current = vec![0u8; 4096];
        versions.push(current.clone());

        for step in 1..10usize {
            let pos = step * 19;
            current[pos] =
                current[pos].wrapping_add(u8::try_from(step).unwrap_or(0).wrapping_add(1));
            versions.push(current.clone());
        }

        let newest = versions.last().expect("non-empty").clone();
        let mut deltas = Vec::with_capacity(versions.len() - 1);
        for idx in (1..versions.len()).rev() {
            let older = &versions[idx - 1];
            let newer = &versions[idx];
            deltas.push(encode_sparse_xor_delta(older, newer).unwrap());
        }

        let reconstructed = reconstruct_chain_from_newest(&newest, &deltas).unwrap();
        assert_eq!(reconstructed.len(), 10);

        for (actual, expected) in reconstructed.iter().zip(versions.iter().rev()) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_delta_as_ecs_object() {
        let base = vec![0u8; 4096];
        let mut target = base.clone();
        mutate_run(&mut target, 256, 31, 0x66);
        let delta = encode_sparse_xor_delta(&base, &target).unwrap();

        let object_a = SparseXorDeltaObject::new(delta.clone());
        let object_b = SparseXorDeltaObject::new(delta.clone());
        assert_eq!(object_a.object_id, object_b.object_id);
        assert_eq!(object_a.patch_kind, PatchKind::SparseXor);

        let mut modified_delta = delta;
        modified_delta.push(0xAB);
        let object_c = SparseXorDeltaObject::new(modified_delta);
        assert_ne!(object_a.object_id, object_c.object_id);
    }

    #[test]
    fn test_delta_vacuum_page_full_copy() {
        let base = vec![0xAAu8; 4096];
        let target = vec![0x55u8; 4096];
        let encoded = encode_page_delta(&base, &target, DeltaThresholdConfig::default()).unwrap();
        assert!(matches!(encoded, DeltaEncoding::FullImage(_)));
    }

    #[test]
    fn test_worked_example_byte_values() {
        let v1 = vec![0u8; 4096];
        let mut v2 = v1.clone();

        let v2_runs = [100, 450, 820, 1190, 1560, 1930, 2300, 2670, 3040, 3410];
        for (index, start) in v2_runs.iter().copied().enumerate() {
            let seed = u8::try_from(index).expect("small index");
            mutate_run(&mut v2, start, 30, seed.wrapping_mul(7).wrapping_add(3));
        }

        let mut v3 = v2.clone();
        let v3_runs = [120, 760, 1500, 2400, 3300];
        for (index, start) in v3_runs.iter().copied().enumerate() {
            let seed = u8::try_from(index).expect("small index").wrapping_add(91);
            mutate_run(&mut v3, start, 12, seed);
        }

        let delta_v2_v3 = encode_sparse_xor_delta(&v2, &v3).unwrap();
        let delta_v1_v2 = encode_sparse_xor_delta(&v1, &v2).unwrap();

        assert_eq!(delta_v2_v3.len(), 88, "expected ~88 bytes per spec example");
        assert_eq!(
            delta_v1_v2.len(),
            348,
            "expected ~348 bytes per spec example"
        );
    }

    proptest! {
        #[test]
        fn prop_xor_delta_roundtrip((base, target) in page_pair_strategy(512)) {
            let delta = encode_sparse_xor_delta(&base, &target)?;
            let decoded = decode_sparse_xor_delta(&base, &delta)?;
            prop_assert_eq!(decoded, target);
        }

        #[test]
        fn prop_chain_reconstruction_correct(
            versions in prop::collection::vec(prop::collection::vec(any::<u8>(), 128), 2..=6)
        ) {
            let newest = versions.last().expect("len>=2").clone();
            let mut deltas = Vec::with_capacity(versions.len() - 1);
            for idx in (1..versions.len()).rev() {
                deltas.push(encode_sparse_xor_delta(&versions[idx - 1], &versions[idx])?);
            }

            let reconstructed = reconstruct_chain_from_newest(&newest, &deltas)?;
            prop_assert_eq!(reconstructed.len(), versions.len());
            for (actual, expected) in reconstructed.iter().zip(versions.iter().rev()) {
                prop_assert_eq!(actual, expected);
            }
        }

        #[test]
        fn prop_delta_size_bounded((base, target) in page_pair_strategy(512)) {
            let encoding = encode_page_delta(&base, &target, DeltaThresholdConfig::default())?;
            prop_assert!(encoding.storage_len() <= base.len() + DELTA_FIXED_OVERHEAD_BYTES);
        }
    }

    #[test]
    fn test_e2e_oltp_compression_ratio() {
        const PAGE_SIZE: usize = 4096;
        const STEPS: usize = 100;
        const CHANGE_LEN: usize = 40;

        let mut versions = Vec::with_capacity(STEPS + 1);
        let mut current = vec![0u8; PAGE_SIZE];
        versions.push(current.clone());

        for step in 0..STEPS {
            let start = (step * 37) % (PAGE_SIZE - CHANGE_LEN);
            for byte in &mut current[start..start + CHANGE_LEN] {
                *byte = byte.wrapping_add(1);
            }
            versions.push(current.clone());
        }

        let full_copy_bytes = versions.len() * PAGE_SIZE;
        let mut delta_bytes = PAGE_SIZE; // newest full image
        for idx in (1..versions.len()).rev() {
            let delta = encode_sparse_xor_delta(&versions[idx - 1], &versions[idx]).unwrap();
            delta_bytes = delta_bytes.saturating_add(delta.len());
        }

        let ratio_permille = full_copy_bytes.saturating_mul(1000) / delta_bytes;
        assert!(
            ratio_permille > 5000,
            "expected >5x compression; ratio_permille={ratio_permille} full={full_copy_bytes} delta={delta_bytes}"
        );
    }

    #[test]
    fn test_e2e_version_chain_memory() {
        const PAGE_SIZE: usize = 1024;
        const STEPS: usize = 1000;
        const CHANGE_LEN: usize = 16;

        let mut versions = Vec::with_capacity(STEPS + 1);
        let mut current = vec![0u8; PAGE_SIZE];
        versions.push(current.clone());

        for step in 0..STEPS {
            let start = (step * 13) % (PAGE_SIZE - CHANGE_LEN);
            for byte in &mut current[start..start + CHANGE_LEN] {
                *byte = byte.wrapping_add(3);
            }
            versions.push(current.clone());
        }

        let full_copy_bytes = versions.len() * PAGE_SIZE;
        let mut delta_bytes = PAGE_SIZE; // newest full
        for idx in (1..versions.len()).rev() {
            let delta = encode_sparse_xor_delta(&versions[idx - 1], &versions[idx]).unwrap();
            delta_bytes = delta_bytes.saturating_add(delta.len());
        }

        assert!(
            delta_bytes.saturating_mul(2) < full_copy_bytes,
            "delta memory should be <50% baseline: delta={delta_bytes} full={full_copy_bytes}"
        );
    }

    #[test]
    fn test_xor_merge_forbidden_btree_interior() {
        let base = vec![0u8; 16];
        let a = vec![0u8; 16];
        let b = vec![0u8; 16];
        let err = attempt_raw_xor_merge(
            &base,
            &a,
            &b,
            MergePageKind::BtreeInteriorTable,
            WriteMergePolicy::Safe,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_xor_merge_forbidden_btree_leaf() {
        let base = vec![0u8; 16];
        let a = vec![0u8; 16];
        let b = vec![0u8; 16];
        let err = attempt_raw_xor_merge(
            &base,
            &a,
            &b,
            MergePageKind::BtreeLeafTable,
            WriteMergePolicy::Safe,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_xor_merge_forbidden_overflow() {
        let base = vec![0u8; 16];
        let a = vec![0u8; 16];
        let b = vec![0u8; 16];
        let err = attempt_raw_xor_merge(
            &base,
            &a,
            &b,
            MergePageKind::Overflow,
            WriteMergePolicy::Safe,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_xor_merge_forbidden_freelist() {
        let base = vec![0u8; 16];
        let a = vec![0u8; 16];
        let b = vec![0u8; 16];
        let err = attempt_raw_xor_merge(
            &base,
            &a,
            &b,
            MergePageKind::Freelist,
            WriteMergePolicy::Safe,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_xor_merge_forbidden_pointer_map() {
        let base = vec![0u8; 16];
        let a = vec![0u8; 16];
        let b = vec![0u8; 16];
        let err = attempt_raw_xor_merge(
            &base,
            &a,
            &b,
            MergePageKind::PointerMap,
            WriteMergePolicy::Safe,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_disjoint_delta_lemma_correct() {
        let base = vec![10u8, 20, 30, 40, 50, 60];
        let delta_a = vec![1u8, 0, 2, 0, 3, 0];
        let delta_b = vec![0u8, 4, 0, 5, 0, 6];

        let merged = compose_disjoint_deltas(&base, &delta_a, &delta_b).unwrap();
        let expected = base
            .iter()
            .zip(delta_a.iter())
            .zip(delta_b.iter())
            .map(|((&orig, &a), &b)| orig ^ a ^ b)
            .collect::<Vec<_>>();
        assert_eq!(merged, expected);
    }

    #[test]
    fn test_counterexample_lost_update() {
        // Mock "pointer to active payload" at byte 0.
        let mut base = vec![0u8; 64];
        base[0] = 20; // pointer -> payload at offset 20
        base[20] = b'A';

        // T1: defragment/move payload to offset 40 and update pointer.
        let mut t1 = base.clone();
        t1[0] = 40;
        t1[40] = base[20];

        // T2: update payload at old logical location (offset 20).
        let mut t2 = base.clone();
        t2[20] = b'B';

        let d1 = dense_xor_delta(&base, &t1).unwrap();
        let d2 = dense_xor_delta(&base, &t2).unwrap();
        assert!(deltas_disjoint(&d1, &d2).unwrap());

        // Raw vector merge is valid algebraically.
        let merged = compose_disjoint_deltas(&base, &d1, &d2).unwrap();
        let ptr = usize::from(merged[0]);
        assert!(ptr < merged.len(), "structurally valid pointer");
        // Lost update: pointer now resolves to old payload at new location.
        assert_eq!(merged[ptr], b'A');
        assert_eq!(merged[20], b'B', "updated byte became unreachable garbage");
    }

    #[test]
    fn test_pragma_write_merge_off() {
        let outcome = decide_conflict_outcome(
            &[42],
            &[42],
            WriteMergePolicy::Off,
            MergePageKind::BtreeLeafTable,
            MergeMethod::IntentReplay,
        )
        .unwrap();
        assert_eq!(outcome, ConflictOutcome::SecondAbortRetry);
    }

    #[test]
    fn test_pragma_write_merge_safe() {
        let outcome = decide_conflict_outcome(
            &[7],
            &[7],
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            MergeMethod::IntentReplay,
        )
        .unwrap();
        assert_eq!(outcome, ConflictOutcome::SecondCommitsViaIntentReplay);

        let raw_err = decide_conflict_outcome(
            &[7],
            &[7],
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            MergeMethod::RawXor,
        )
        .unwrap_err();
        assert!(matches!(raw_err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_pragma_write_merge_lab_unsafe_rejected_in_release() {
        let result = resolve_write_merge_policy(WriteMergePolicy::LabUnsafe);
        if cfg!(debug_assertions) {
            assert_eq!(result.unwrap(), WriteMergePolicy::LabUnsafe);
        } else {
            assert!(matches!(
                result.unwrap_err(),
                MergeSafetyError::LabUnsafeRejectedInRelease
            ));
        }
    }

    #[test]
    fn test_lab_unsafe_still_forbids_btree_xor() {
        if resolve_write_merge_policy(WriteMergePolicy::LabUnsafe).is_err() {
            return;
        }

        let base = vec![0u8; 8];
        let delta_a = vec![0u8; 8];
        let delta_b = vec![0u8; 8];
        let err = attempt_raw_xor_merge(
            &base,
            &delta_a,
            &delta_b,
            MergePageKind::BtreeLeafTable,
            WriteMergePolicy::LabUnsafe,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_gf256_delta_as_encoding_not_correctness() {
        let base = vec![0u8; 4096];
        let mut target = base.clone();
        mutate_run(&mut target, 100, 64, 0x5b);

        // Encoding correctness.
        let encoded = encode_sparse_xor_delta(&base, &target).unwrap();
        let decoded = decode_sparse_xor_delta(&base, &encoded).unwrap();
        assert_eq!(decoded, target);

        // Merge correctness still requires policy checks.
        let err =
            enforce_raw_xor_merge_policy(MergePageKind::BtreeLeafTable, WriteMergePolicy::Safe)
                .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_gf256_patch_commutative() {
        let base = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let patch_a = vec![0u8, 0, 7, 0, 0, 9, 0, 0];
        let patch_b = vec![5u8, 0, 0, 11, 0, 0, 13, 0];

        let ab = compose_disjoint_deltas(&base, &patch_a, &patch_b).unwrap();
        let ba = compose_disjoint_deltas(&base, &patch_b, &patch_a).unwrap();
        assert_eq!(ab, ba, "disjoint GF(256) patch composition must commute");
    }

    #[test]
    fn test_gf256_patch_non_commutative_overlap() {
        let base = vec![0u8; 8];
        let patch_a = vec![0u8, 1, 0, 0, 0, 0, 0, 0];
        let patch_b = vec![0u8, 2, 0, 0, 0, 0, 0, 0];
        let err = compose_disjoint_deltas(&base, &patch_a, &patch_b).unwrap_err();
        assert!(matches!(err, MergeSafetyError::OverlappingSupport { .. }));
    }

    #[test]
    fn test_merge_safety_disjoint_ops() {
        let outcome = decide_conflict_outcome(
            &[1],
            &[1],
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            MergeMethod::IntentReplay,
        )
        .unwrap();
        assert_eq!(outcome, ConflictOutcome::SecondCommitsViaIntentReplay);
    }

    #[test]
    fn test_merge_safety_overlapping_ops() {
        let err = decide_conflict_outcome(
            &[1],
            &[1],
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            MergeMethod::RawXor,
        )
        .unwrap_err();
        assert!(matches!(err, MergeSafetyError::RawXorForbidden { .. }));
    }

    #[test]
    fn test_gf256_inverse() {
        for value in 1u16..=255 {
            let byte = u8::try_from(value).expect("1..=255 fits u8");
            let inv = gf256_inverse_byte(byte).expect("non-zero must have inverse");
            assert_eq!(gf256_mul_byte(byte, inv), 1);
        }
        assert!(gf256_inverse_byte(0).is_none());
    }

    proptest! {
        #[test]
        fn prop_disjoint_delta_composition(
            base in prop::collection::vec(any::<u8>(), 128),
            left_values in prop::collection::vec(any::<u8>(), 128),
            right_values in prop::collection::vec(any::<u8>(), 128),
        ) {
            let mut delta_a = vec![0u8; 128];
            let mut delta_b = vec![0u8; 128];
            for index in 0..128usize {
                if index % 2 == 0 {
                    delta_a[index] = left_values[index];
                } else {
                    delta_b[index] = right_values[index];
                }
            }

            let merged = compose_disjoint_deltas(&base, &delta_a, &delta_b)?;
            let expected = base
                .iter()
                .zip(delta_a.iter())
                .zip(delta_b.iter())
                .map(|((&orig, &a), &b)| orig ^ a ^ b)
                .collect::<Vec<_>>();
            prop_assert_eq!(merged, expected);
        }

        #[test]
        fn prop_merge_safety_compile_time(index in 0usize..RAW_XOR_FORBIDDEN_PAGE_KINDS.len()) {
            let kind = RAW_XOR_FORBIDDEN_PAGE_KINDS[index];
            prop_assert!(is_raw_xor_forbidden_page_kind(kind));
        }
    }

    #[test]
    fn test_e2e_concurrent_insert_different_pages() {
        let outcome = decide_conflict_outcome(
            &[1],
            &[2],
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            MergeMethod::IntentReplay,
        )
        .unwrap();
        assert_eq!(outcome, ConflictOutcome::BothCommit);
    }

    #[test]
    fn test_e2e_concurrent_insert_same_page_conflict() {
        let outcome = decide_conflict_outcome(
            &[9],
            &[9],
            WriteMergePolicy::Off,
            MergePageKind::BtreeLeafTable,
            MergeMethod::IntentReplay,
        )
        .unwrap();
        assert_eq!(outcome, ConflictOutcome::SecondAbortRetry);
    }

    #[test]
    fn test_e2e_concurrent_insert_same_page_intent_replay() {
        let outcome = decide_conflict_outcome(
            &[9],
            &[9],
            WriteMergePolicy::Safe,
            MergePageKind::BtreeLeafTable,
            MergeMethod::IntentReplay,
        )
        .unwrap();
        assert_eq!(outcome, ConflictOutcome::SecondCommitsViaIntentReplay);
    }

    #[test]
    fn test_merge_safety_no_xor() {
        test_counterexample_lost_update();
    }

    #[test]
    fn test_version_chain_compression() {
        test_e2e_oltp_compression_ratio();
    }
}
