//! ECS (Erasure-Coded Stream) substrate types.
//!
//! This module defines foundational identity primitives for Native mode:
//! - [`ObjectId`] / [`PayloadHash`]: content-addressed identity (§3.5.1)
//! - [`SymbolRecord`] / [`SymbolRecordFlags`]: physical storage envelope (§3.5.2)
//!
//! Spec: COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md §3.5.1–§3.5.2.

use std::fmt;

use crate::encoding::{append_u32_le, append_u64_le, read_u32_le, read_u64_le, write_u64_le};
use crate::glossary::{OTI_WIRE_SIZE, Oti};

/// Domain separation prefix for ECS ObjectIds (spec: `"fsqlite:ecs:v1"`).
const ECS_OBJECT_ID_DOMAIN_SEPARATOR: &[u8] = b"fsqlite:ecs:v1";

/// Canonical 32-byte hash of an ECS object's payload.
///
/// The spec refers to this as `payload_hash` in:
/// `ObjectId = Trunc128(BLAKE3("fsqlite:ecs:v1" || canonical_object_header || payload_hash))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct PayloadHash([u8; 32]);

impl PayloadHash {
    /// Construct from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the hash bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hash a payload using BLAKE3-256.
    #[must_use]
    pub fn blake3(payload: &[u8]) -> Self {
        let hash = blake3::hash(payload);
        Self(*hash.as_bytes())
    }
}

/// 16-byte truncated content-addressed identity for an ECS object.
///
/// Spec:
/// `ObjectId = Trunc128(BLAKE3("fsqlite:ecs:v1" || canonical_object_header || payload_hash))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ObjectId([u8; 16]);

impl ObjectId {
    /// ObjectId length in bytes.
    pub const LEN: usize = 16;

    /// Domain separation prefix from the spec.
    pub const DOMAIN_SEPARATOR: &'static [u8] = ECS_OBJECT_ID_DOMAIN_SEPARATOR;

    /// Construct from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Derive an ObjectId from already-canonicalized bytes.
    ///
    /// `canonical_bytes` must be a deterministic, versioned wire-format blob
    /// (spec: "not serde vibes") representing the object's header plus its
    /// `payload_hash`.
    #[must_use]
    pub fn derive_from_canonical_bytes(canonical_bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(Self::DOMAIN_SEPARATOR);
        hasher.update(canonical_bytes);
        let digest = hasher.finalize();

        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&digest.as_bytes()[..Self::LEN]);
        Self(out)
    }

    /// Derive an ObjectId from canonical header bytes and a payload hash.
    #[must_use]
    pub fn derive(canonical_object_header: &[u8], payload_hash: PayloadHash) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(Self::DOMAIN_SEPARATOR);
        hasher.update(canonical_object_header);
        hasher.update(payload_hash.as_bytes());
        let digest = hasher.finalize();

        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&digest.as_bytes()[..Self::LEN]);
        Self(out)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for ObjectId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 16]> for ObjectId {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

// ---------------------------------------------------------------------------
// §3.5.2 SymbolRecord Envelope and Auth Tags
// ---------------------------------------------------------------------------

/// Magic bytes identifying a SymbolRecord: `"FSEC"` (0x46 0x53 0x45 0x43).
pub const SYMBOL_RECORD_MAGIC: [u8; 4] = [0x46, 0x53, 0x45, 0x43];

/// Current envelope version.
pub const SYMBOL_RECORD_VERSION: u8 = 1;

/// Domain separation prefix for symbol auth tags.
const SYMBOL_AUTH_DOMAIN: &[u8] = b"fsqlite:symbol-auth:v1";

bitflags::bitflags! {
    /// Flags for a [`SymbolRecord`].
    ///
    /// Additional local flags MAY be defined but MUST be treated as advisory
    /// optimization hints. Correctness never depends on them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SymbolRecordFlags: u8 {
        /// This record is the first source symbol (esi = 0) and the writer
        /// attempted to place the entire systematic run contiguously.
        const SYSTEMATIC_RUN_START = 0x01;
    }
}

/// Validation error when deserializing or checking a [`SymbolRecord`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolRecordError {
    /// Input too short to contain a complete record.
    TooShort { expected_min: usize, actual: usize },
    /// Magic bytes do not match `"FSEC"`.
    BadMagic([u8; 4]),
    /// Envelope version is unsupported.
    UnsupportedVersion(u8),
    /// `symbol_size != OTI.T` — key invariant violated.
    SymbolSizeMismatch { symbol_size: u32, oti_t: u32 },
    /// `symbol_size` is not representable as a `usize` on this platform.
    SymbolSizeTooLarge { symbol_size: u32 },
    /// Size computation overflowed.
    SizeOverflow,
    /// `frame_xxh3` integrity check failed.
    IntegrityFailure { expected: u64, computed: u64 },
    /// Auth tag verification failed.
    AuthTagFailure,
}

impl fmt::Display for SymbolRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort {
                expected_min,
                actual,
            } => {
                write!(
                    f,
                    "symbol record too short: need {expected_min}, got {actual}"
                )
            }
            Self::BadMagic(m) => write!(f, "bad magic: {m:02x?}"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported version: {v}"),
            Self::SymbolSizeMismatch { symbol_size, oti_t } => {
                write!(f, "symbol_size ({symbol_size}) != OTI.T ({oti_t})")
            }
            Self::SymbolSizeTooLarge { symbol_size } => {
                write!(f, "symbol_size too large for platform: {symbol_size}")
            }
            Self::SizeOverflow => write!(f, "symbol record size overflow"),
            Self::IntegrityFailure { expected, computed } => {
                write!(
                    f,
                    "frame_xxh3 mismatch: stored {expected:#018x}, computed {computed:#018x}"
                )
            }
            Self::AuthTagFailure => write!(f, "auth tag verification failed"),
        }
    }
}

impl std::error::Error for SymbolRecordError {}

/// Fixed header size before `symbol_data`:
/// magic(4) + version(1) + object_id(16) + OTI(22) + esi(4) + symbol_size(4) = 51.
const HEADER_BEFORE_DATA: usize = 4 + 1 + 16 + OTI_WIRE_SIZE + 4 + 4;

/// Fixed trailer size after `symbol_data`:
/// flags(1) + frame_xxh3(8) + auth_tag(16) = 25.
const TRAILER_AFTER_DATA: usize = 1 + 8 + 16;

/// The atomic unit of physical storage for ECS objects (§3.5.2).
///
/// A `SymbolRecord` is self-describing: a decoder collecting K' symbols with
/// the same `ObjectId` can reconstruct the original object without any
/// external metadata.
///
/// Wire layout (all integers little-endian):
/// ```text
/// magic[4] | version[1] | object_id[16] | OTI[22] | esi[4] | symbol_size[4]
/// | symbol_data[T] | flags[1] | frame_xxh3[8] | auth_tag[16]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolRecord {
    /// Content-addressed identity of the parent ECS object.
    pub object_id: ObjectId,
    /// RaptorQ Object Transmission Information.
    pub oti: Oti,
    /// Encoding Symbol Identifier — which symbol this is.
    pub esi: u32,
    /// The actual RaptorQ encoding symbol payload.
    pub symbol_data: Vec<u8>,
    /// Advisory flags.
    pub flags: SymbolRecordFlags,
    /// xxhash3 of all preceding fields for fast integrity checking.
    pub frame_xxh3: u64,
    /// Optional BLAKE3-keyed auth tag for authenticated transport.
    /// All-zero when `symbol_auth = off`.
    pub auth_tag: [u8; 16],
}

impl SymbolRecord {
    /// Compute the `frame_xxh3` digest over the header + symbol_data + flags.
    ///
    /// This covers everything from `magic` through `flags`, i.e. all fields
    /// preceding `frame_xxh3` in the wire layout.
    #[must_use]
    fn compute_frame_xxh3(pre_hash_bytes: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(pre_hash_bytes)
    }

    /// Build the byte region that `frame_xxh3` covers (magic..flags inclusive).
    fn pre_hash_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            HEADER_BEFORE_DATA + self.symbol_data.len() + 1, /* flags */
        );
        buf.extend_from_slice(&SYMBOL_RECORD_MAGIC);
        buf.push(SYMBOL_RECORD_VERSION);
        buf.extend_from_slice(self.object_id.as_bytes());
        buf.extend_from_slice(&self.oti.to_bytes());
        append_u32_le(&mut buf, self.esi);
        let expected_len = usize::try_from(self.oti.t).expect("OTI.t fits in usize");
        debug_assert_eq!(
            self.symbol_data.len(),
            expected_len,
            "symbol_data length must equal OTI.t"
        );
        append_u32_le(&mut buf, self.oti.t);
        buf.extend_from_slice(&self.symbol_data);
        buf.push(self.flags.bits());
        buf
    }

    /// Create a new `SymbolRecord`, computing `frame_xxh3` automatically.
    ///
    /// `auth_tag` is set to all-zero (symbol_auth off). Use
    /// [`Self::with_auth_tag`] to set an authenticated tag.
    #[must_use]
    pub fn new(
        object_id: ObjectId,
        oti: Oti,
        esi: u32,
        symbol_data: Vec<u8>,
        flags: SymbolRecordFlags,
    ) -> Self {
        let expected_len = usize::try_from(oti.t).expect("OTI.t fits in usize");
        assert_eq!(
            symbol_data.len(),
            expected_len,
            "SymbolRecord::new: symbol_data.len ({}) must equal oti.t ({})",
            symbol_data.len(),
            oti.t
        );

        let mut rec = Self {
            object_id,
            oti,
            esi,
            symbol_data,
            flags,
            frame_xxh3: 0,
            auth_tag: [0u8; 16],
        };
        let pre_hash = rec.pre_hash_bytes();
        rec.frame_xxh3 = Self::compute_frame_xxh3(&pre_hash);
        rec
    }

    /// Set the auth tag using a BLAKE3-keyed MAC.
    ///
    /// `epoch_key` is the 32-byte key derived from `SymbolSegmentHeader.epoch_id`
    /// per §4.18.2.
    ///
    /// ```text
    /// auth_tag = Trunc128(BLAKE3_KEYED(epoch_key,
    ///     "fsqlite:symbol-auth:v1" || bytes(magic..frame_xxh3)))
    /// ```
    #[must_use]
    pub fn with_auth_tag(mut self, epoch_key: &[u8; 32]) -> Self {
        self.auth_tag = Self::compute_auth_tag(epoch_key, &self.pre_hash_bytes(), self.frame_xxh3);
        self
    }

    /// Compute the 16-byte auth tag.
    fn compute_auth_tag(epoch_key: &[u8; 32], pre_hash: &[u8], frame_xxh3: u64) -> [u8; 16] {
        let mut keyed_hasher = blake3::Hasher::new_keyed(epoch_key);
        keyed_hasher.update(SYMBOL_AUTH_DOMAIN);
        keyed_hasher.update(pre_hash);
        let mut frame_hash_bytes = [0u8; 8];
        write_u64_le(&mut frame_hash_bytes, frame_xxh3).expect("fixed u64 field");
        keyed_hasher.update(&frame_hash_bytes);
        let digest = keyed_hasher.finalize();
        let mut tag = [0u8; 16];
        tag.copy_from_slice(&digest.as_bytes()[..16]);
        tag
    }

    /// Serialize to canonical wire bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let expected_len = usize::try_from(self.oti.t).expect("OTI.t fits in usize");
        debug_assert_eq!(
            self.symbol_data.len(),
            expected_len,
            "symbol_data length must equal OTI.t"
        );

        let total = HEADER_BEFORE_DATA + self.symbol_data.len() + TRAILER_AFTER_DATA;
        let mut buf = Vec::with_capacity(total);

        // Header
        buf.extend_from_slice(&SYMBOL_RECORD_MAGIC);
        buf.push(SYMBOL_RECORD_VERSION);
        buf.extend_from_slice(self.object_id.as_bytes());
        buf.extend_from_slice(&self.oti.to_bytes());
        append_u32_le(&mut buf, self.esi);
        append_u32_le(&mut buf, self.oti.t);

        // Payload
        buf.extend_from_slice(&self.symbol_data);

        // Trailer
        buf.push(self.flags.bits());
        append_u64_le(&mut buf, self.frame_xxh3);
        buf.extend_from_slice(&self.auth_tag);

        debug_assert_eq!(buf.len(), total);
        buf
    }

    /// Deserialize from canonical wire bytes, validating all invariants.
    ///
    /// # Errors
    ///
    /// Returns [`SymbolRecordError`] if the data is malformed, the magic is
    /// wrong, the version is unsupported, `symbol_size != OTI.T`, or the
    /// `frame_xxh3` integrity check fails.
    pub fn from_bytes(data: &[u8]) -> Result<Self, SymbolRecordError> {
        // Need at least the fixed header to read symbol_size.
        if data.len() < HEADER_BEFORE_DATA {
            return Err(SymbolRecordError::TooShort {
                expected_min: HEADER_BEFORE_DATA,
                actual: data.len(),
            });
        }

        // Magic
        let magic: [u8; 4] = data[0..4].try_into().expect("4 bytes");
        if magic != SYMBOL_RECORD_MAGIC {
            return Err(SymbolRecordError::BadMagic(magic));
        }

        // Version
        let version = data[4];
        if version != SYMBOL_RECORD_VERSION {
            return Err(SymbolRecordError::UnsupportedVersion(version));
        }

        // ObjectId
        let object_id = ObjectId::from_bytes(data[5..21].try_into().expect("16 bytes"));

        // OTI
        let oti =
            Oti::from_bytes(&data[21..43]).expect("already checked length >= HEADER_BEFORE_DATA");

        // ESI + symbol_size
        let esi = read_u32_le(&data[43..47]).expect("fixed u32 field");
        let symbol_size = read_u32_le(&data[47..51]).expect("fixed u32 field");

        // Key invariant: symbol_size == OTI.T
        if symbol_size != oti.t {
            return Err(SymbolRecordError::SymbolSizeMismatch {
                symbol_size,
                oti_t: oti.t,
            });
        }

        let symbol_size_usize = usize::try_from(symbol_size)
            .map_err(|_| SymbolRecordError::SymbolSizeTooLarge { symbol_size })?;
        let total_size = HEADER_BEFORE_DATA
            .checked_add(symbol_size_usize)
            .and_then(|v| v.checked_add(TRAILER_AFTER_DATA))
            .ok_or(SymbolRecordError::SizeOverflow)?;
        if data.len() < total_size {
            return Err(SymbolRecordError::TooShort {
                expected_min: total_size,
                actual: data.len(),
            });
        }

        // Symbol data
        let data_start = HEADER_BEFORE_DATA;
        let data_end = data_start
            .checked_add(symbol_size_usize)
            .ok_or(SymbolRecordError::SizeOverflow)?;
        let symbol_data = data[data_start..data_end].to_vec();

        // Trailer
        let flags = SymbolRecordFlags::from_bits_truncate(data[data_end]);
        let frame_xxh3 = read_u64_le(&data[data_end + 1..data_end + 9]).expect("fixed u64 field");
        let auth_tag: [u8; 16] = data[data_end + 9..data_end + 25]
            .try_into()
            .expect("16 bytes");

        // Verify frame_xxh3 integrity
        let pre_hash_end = data_end + 1; // magic..flags inclusive
        let computed = Self::compute_frame_xxh3(&data[..pre_hash_end]);
        if computed != frame_xxh3 {
            return Err(SymbolRecordError::IntegrityFailure {
                expected: frame_xxh3,
                computed,
            });
        }

        Ok(Self {
            object_id,
            oti,
            esi,
            symbol_data,
            flags,
            frame_xxh3,
            auth_tag,
        })
    }

    /// Verify `frame_xxh3` integrity without full deserialization.
    #[must_use]
    pub fn verify_integrity(&self) -> bool {
        let pre_hash = self.pre_hash_bytes();
        Self::compute_frame_xxh3(&pre_hash) == self.frame_xxh3
    }

    /// Verify the auth tag using the given epoch key.
    ///
    /// Returns `true` if the auth tag matches, or if the tag is all-zero
    /// (symbol_auth off — tag is ignored per spec).
    #[must_use]
    pub fn verify_auth(&self, epoch_key: &[u8; 32]) -> bool {
        if self.auth_tag == [0u8; 16] {
            return true; // auth off: tag ignored
        }
        let expected = Self::compute_auth_tag(epoch_key, &self.pre_hash_bytes(), self.frame_xxh3);
        self.auth_tag == expected
    }

    /// Total serialized size of this record in bytes.
    #[must_use]
    pub fn wire_size(&self) -> usize {
        HEADER_BEFORE_DATA + self.symbol_data.len() + TRAILER_AFTER_DATA
    }
}

// ---------------------------------------------------------------------------
// §1.5 Systematic symbol layout + fast-path reconstruction helpers
// ---------------------------------------------------------------------------

/// Error when validating or reconstructing systematic symbol runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystematicLayoutError {
    /// No symbol records were provided.
    EmptySymbolSet,
    /// OTI uses `t = 0`, which is invalid.
    ZeroSymbolSize,
    /// Source-symbol count cannot be represented on this platform.
    SourceSymbolCountTooLarge { source_symbols: u64 },
    /// Source-symbol count exceeds the `u32` ESI namespace.
    SourceSymbolCountExceedsEsiRange { source_symbols: u64 },
    /// Transfer length cannot be represented as `usize`.
    TransferLengthTooLarge { transfer_length: u64 },
    /// Reconstructed buffer size overflow.
    ReconstructedSizeOverflow {
        source_symbols: usize,
        symbol_size: usize,
    },
    /// Record object id does not match the run's object id.
    InconsistentObjectId {
        expected: ObjectId,
        found: ObjectId,
        esi: u32,
    },
    /// Record OTI does not match the run's OTI.
    InconsistentOti { expected: Oti, found: Oti, esi: u32 },
    /// Record payload length does not match `OTI.t`.
    InvalidSymbolPayloadSize {
        esi: u32,
        expected: usize,
        found: usize,
    },
    /// ESI 0 must carry [`SymbolRecordFlags::SYSTEMATIC_RUN_START`].
    MissingSystematicStartFlag,
    /// Missing required source symbol.
    MissingSystematicSymbol { expected_esi: u32 },
    /// Duplicate source symbol with the same ESI.
    DuplicateSystematicSymbol { esi: u32 },
    /// Source symbols are not laid out as `ESI 0..K-1` contiguously.
    NonContiguousSystematicSymbol { expected_esi: u32, found_esi: u32 },
    /// A source symbol appears after the systematic run.
    RepairInterleaved { index: usize, esi: u32 },
    /// Symbol integrity check failed.
    CorruptSymbol { esi: u32 },
}

impl fmt::Display for SystematicLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySymbolSet => f.write_str("no symbol records provided"),
            Self::ZeroSymbolSize => f.write_str("OTI.t is zero"),
            Self::SourceSymbolCountTooLarge { source_symbols } => {
                write!(
                    f,
                    "source symbol count too large for platform: {source_symbols}"
                )
            }
            Self::SourceSymbolCountExceedsEsiRange { source_symbols } => {
                write!(
                    f,
                    "source symbol count exceeds u32 ESI range: {source_symbols}"
                )
            }
            Self::TransferLengthTooLarge { transfer_length } => {
                write!(
                    f,
                    "transfer length too large for platform: {transfer_length}"
                )
            }
            Self::ReconstructedSizeOverflow {
                source_symbols,
                symbol_size,
            } => {
                write!(
                    f,
                    "reconstructed size overflow: {source_symbols} * {symbol_size}"
                )
            }
            Self::InconsistentObjectId {
                expected,
                found,
                esi,
            } => {
                write!(
                    f,
                    "object_id mismatch at esi={esi}: expected {expected}, found {found}"
                )
            }
            Self::InconsistentOti {
                expected,
                found,
                esi,
            } => {
                write!(
                    f,
                    "OTI mismatch at esi={esi}: expected {expected:?}, found {found:?}"
                )
            }
            Self::InvalidSymbolPayloadSize {
                esi,
                expected,
                found,
            } => {
                write!(
                    f,
                    "symbol payload size mismatch at esi={esi}: expected {expected}, found {found}"
                )
            }
            Self::MissingSystematicStartFlag => {
                f.write_str("missing SYSTEMATIC_RUN_START flag on ESI 0")
            }
            Self::MissingSystematicSymbol { expected_esi } => {
                write!(f, "missing systematic symbol esi={expected_esi}")
            }
            Self::DuplicateSystematicSymbol { esi } => {
                write!(f, "duplicate systematic symbol esi={esi}")
            }
            Self::NonContiguousSystematicSymbol {
                expected_esi,
                found_esi,
            } => {
                write!(
                    f,
                    "non-contiguous systematic run: expected esi={expected_esi}, found esi={found_esi}"
                )
            }
            Self::RepairInterleaved { index, esi } => {
                write!(
                    f,
                    "repair/source interleave at index={index}: encountered source esi={esi} after systematic run"
                )
            }
            Self::CorruptSymbol { esi } => write!(f, "integrity check failed for esi={esi}"),
        }
    }
}

impl std::error::Error for SystematicLayoutError {}

fn source_symbol_count_u64(oti: Oti) -> Result<u64, SystematicLayoutError> {
    if oti.t == 0 {
        return Err(SystematicLayoutError::ZeroSymbolSize);
    }
    if oti.f == 0 {
        return Ok(0);
    }
    Ok(oti.f.div_ceil(u64::from(oti.t)))
}

fn validate_record_shape(
    record: &SymbolRecord,
    object_id: ObjectId,
    oti: Oti,
    symbol_size: usize,
) -> Result<(), SystematicLayoutError> {
    if record.object_id != object_id {
        return Err(SystematicLayoutError::InconsistentObjectId {
            expected: object_id,
            found: record.object_id,
            esi: record.esi,
        });
    }
    if record.oti != oti {
        return Err(SystematicLayoutError::InconsistentOti {
            expected: oti,
            found: record.oti,
            esi: record.esi,
        });
    }
    if record.symbol_data.len() != symbol_size {
        return Err(SystematicLayoutError::InvalidSymbolPayloadSize {
            esi: record.esi,
            expected: symbol_size,
            found: record.symbol_data.len(),
        });
    }
    if !record.verify_integrity() {
        return Err(SystematicLayoutError::CorruptSymbol { esi: record.esi });
    }
    Ok(())
}

/// Compute source-symbol count `K = ceil(F / T)` for the given [`Oti`].
pub fn source_symbol_count(oti: Oti) -> Result<usize, SystematicLayoutError> {
    let source_symbols = source_symbol_count_u64(oti)?;
    usize::try_from(source_symbols)
        .map_err(|_| SystematicLayoutError::SourceSymbolCountTooLarge { source_symbols })
}

/// Writer helper: normalize records into `ESI 0..K-1` contiguous layout.
///
/// Guarantees:
/// - Source symbols are first, in ascending ESI order (`0..K-1`).
/// - Repair symbols (`ESI >= K`) follow the systematic run.
/// - Only ESI 0 has [`SymbolRecordFlags::SYSTEMATIC_RUN_START`].
pub fn layout_systematic_run(
    records: Vec<SymbolRecord>,
) -> Result<Vec<SymbolRecord>, SystematicLayoutError> {
    let first = records
        .first()
        .ok_or(SystematicLayoutError::EmptySymbolSet)?
        .clone();
    let source_symbols = source_symbol_count(first.oti)?;
    let source_symbols_u64 = source_symbol_count_u64(first.oti)?;
    let source_symbols_u32 = u32::try_from(source_symbols_u64).map_err(|_| {
        SystematicLayoutError::SourceSymbolCountExceedsEsiRange {
            source_symbols: source_symbols_u64,
        }
    })?;
    let symbol_size =
        usize::try_from(first.oti.t).map_err(|_| SystematicLayoutError::ZeroSymbolSize)?;

    let mut systematic = vec![None; source_symbols];
    let mut repairs = Vec::new();

    for mut record in records {
        validate_record_shape(&record, first.object_id, first.oti, symbol_size)?;
        record.flags.remove(SymbolRecordFlags::SYSTEMATIC_RUN_START);
        if record.esi < source_symbols_u32 {
            let idx = usize::try_from(record.esi).expect("ESI < K fits usize");
            if systematic[idx].is_some() {
                return Err(SystematicLayoutError::DuplicateSystematicSymbol { esi: record.esi });
            }
            systematic[idx] = Some(record);
        } else {
            repairs.push(record);
        }
    }

    let mut ordered = Vec::with_capacity(systematic.len().saturating_add(repairs.len()));
    for (idx, maybe_record) in systematic.into_iter().enumerate() {
        let mut record =
            maybe_record.ok_or_else(|| SystematicLayoutError::MissingSystematicSymbol {
                expected_esi: u32::try_from(idx).expect("idx fits u32"),
            })?;
        if idx == 0 {
            record.flags.insert(SymbolRecordFlags::SYSTEMATIC_RUN_START);
        }
        ordered.push(record);
    }

    repairs.sort_by_key(|record| record.esi);
    ordered.extend(repairs);

    Ok(ordered)
}

/// Validate whether records already satisfy systematic contiguous run layout.
///
/// Returns the required source-symbol count `K` on success.
pub fn validate_systematic_run(records: &[SymbolRecord]) -> Result<usize, SystematicLayoutError> {
    let first = records
        .first()
        .ok_or(SystematicLayoutError::EmptySymbolSet)?;
    let source_symbols = source_symbol_count(first.oti)?;
    let source_symbols_u64 = source_symbol_count_u64(first.oti)?;
    let source_symbols_u32 = u32::try_from(source_symbols_u64).map_err(|_| {
        SystematicLayoutError::SourceSymbolCountExceedsEsiRange {
            source_symbols: source_symbols_u64,
        }
    })?;
    let symbol_size =
        usize::try_from(first.oti.t).map_err(|_| SystematicLayoutError::ZeroSymbolSize)?;

    if source_symbols == 0 {
        return Ok(0);
    }

    for expected_idx in 0..source_symbols {
        let record = records.get(expected_idx).ok_or_else(|| {
            SystematicLayoutError::MissingSystematicSymbol {
                expected_esi: u32::try_from(expected_idx).expect("idx fits u32"),
            }
        })?;
        validate_record_shape(record, first.object_id, first.oti, symbol_size)?;

        let expected_esi = u32::try_from(expected_idx).expect("idx fits u32");
        if record.esi != expected_esi {
            return Err(SystematicLayoutError::NonContiguousSystematicSymbol {
                expected_esi,
                found_esi: record.esi,
            });
        }

        if expected_idx == 0 {
            if !record
                .flags
                .contains(SymbolRecordFlags::SYSTEMATIC_RUN_START)
            {
                return Err(SystematicLayoutError::MissingSystematicStartFlag);
            }
        } else if record
            .flags
            .contains(SymbolRecordFlags::SYSTEMATIC_RUN_START)
        {
            return Err(SystematicLayoutError::MissingSystematicStartFlag);
        }
    }

    for (index, record) in records.iter().enumerate().skip(source_symbols) {
        validate_record_shape(record, first.object_id, first.oti, symbol_size)?;
        if record.esi < source_symbols_u32 {
            return Err(SystematicLayoutError::RepairInterleaved {
                index,
                esi: record.esi,
            });
        }
    }

    Ok(source_symbols)
}

/// Reconstruct payload bytes directly from contiguous systematic symbols.
///
/// This is the GF(256)-free happy path.
pub fn reconstruct_systematic_happy_path(
    records: &[SymbolRecord],
) -> Result<Vec<u8>, SystematicLayoutError> {
    let source_symbols = validate_systematic_run(records)?;
    if source_symbols == 0 {
        return Ok(Vec::new());
    }

    let first = &records[0];
    let symbol_size =
        usize::try_from(first.oti.t).map_err(|_| SystematicLayoutError::ZeroSymbolSize)?;
    let transfer_length = usize::try_from(first.oti.f).map_err(|_| {
        SystematicLayoutError::TransferLengthTooLarge {
            transfer_length: first.oti.f,
        }
    })?;
    let total_len = source_symbols.checked_mul(symbol_size).ok_or(
        SystematicLayoutError::ReconstructedSizeOverflow {
            source_symbols,
            symbol_size,
        },
    )?;

    let mut out = Vec::with_capacity(total_len);
    for record in records.iter().take(source_symbols) {
        out.extend_from_slice(&record.symbol_data);
    }
    out.truncate(transfer_length);
    Ok(out)
}

/// Read-path classification for ECS object recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolReadPath {
    /// Recovered by concatenating contiguous systematic symbols.
    SystematicFastPath,
    /// Happy path was unavailable; decoder fallback was invoked.
    FullDecodeFallback { reason: SystematicLayoutError },
}

/// Recover object bytes using happy-path first, with decoder fallback.
pub fn recover_object_with_fallback<F>(
    records: &[SymbolRecord],
    mut fallback_decode: F,
) -> Result<(Vec<u8>, SymbolReadPath), SystematicLayoutError>
where
    F: FnMut(&[SymbolRecord]) -> Result<Vec<u8>, SystematicLayoutError>,
{
    match reconstruct_systematic_happy_path(records) {
        Ok(bytes) => Ok((bytes, SymbolReadPath::SystematicFastPath)),
        Err(reason) => {
            let decoded = fallback_decode(records)?;
            Ok((decoded, SymbolReadPath::FullDecodeFallback { reason }))
        }
    }
}

// ---------------------------------------------------------------------------
// §3.6.1-3.6.3 Native Index Types
// ---------------------------------------------------------------------------

/// How a page version is stored in an ECS patch object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PatchKind {
    /// Full page image — the patch object contains the entire page.
    FullImage = 0,
    /// Intent log — a sequence of semantic operations to replay.
    IntentLog = 1,
    /// Sparse XOR — byte-range XOR delta against a base image.
    SparseXor = 2,
}

impl PatchKind {
    /// Deserialize from wire byte.
    #[must_use]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::FullImage),
            1 => Some(Self::IntentLog),
            2 => Some(Self::SparseXor),
            _ => None,
        }
    }
}

/// Stable, content-addressed pointer from a page index to a patch object (§3.6.2).
///
/// The atom of lookup in Native mode. References content-addressed ECS
/// objects, not physical offsets, so the pointer is replicable across nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionPointer {
    /// Commit sequence at which this version was created.
    pub commit_seq: u64,
    /// ECS object containing the patch/intent.
    pub patch_object: ObjectId,
    /// How the page bytes are represented.
    pub patch_kind: PatchKind,
    /// Optional base image hint for fast materialization of deltas.
    pub base_hint: Option<ObjectId>,
}

/// Wire size of [`VersionPointer`]: commit_seq(8) + object_id(16) + patch_kind(1)
/// + has_base(1) + optional base_hint(16) = 26 or 42 bytes.
const VERSION_POINTER_MIN_WIRE: usize = 8 + 16 + 1 + 1;

impl VersionPointer {
    /// Serialize to canonical little-endian bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let has_base: u8 = u8::from(self.base_hint.is_some());
        let cap = VERSION_POINTER_MIN_WIRE + if has_base == 1 { 16 } else { 0 };
        let mut buf = Vec::with_capacity(cap);
        append_u64_le(&mut buf, self.commit_seq);
        buf.extend_from_slice(self.patch_object.as_bytes());
        buf.push(self.patch_kind as u8);
        buf.push(has_base);
        if let Some(base) = self.base_hint {
            buf.extend_from_slice(base.as_bytes());
        }
        buf
    }

    /// Deserialize from canonical little-endian bytes.
    #[must_use]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < VERSION_POINTER_MIN_WIRE {
            return None;
        }
        let commit_seq = read_u64_le(&data[0..8])?;
        let patch_object = ObjectId::from_bytes(data[8..24].try_into().ok()?);
        let patch_kind = PatchKind::from_byte(data[24])?;
        let has_base = data[25];
        let base_hint = if has_base != 0 {
            if data.len() < VERSION_POINTER_MIN_WIRE + 16 {
                return None;
            }
            Some(ObjectId::from_bytes(data[26..42].try_into().ok()?))
        } else {
            None
        };
        Some(Self {
            commit_seq,
            patch_object,
            patch_kind,
            base_hint,
        })
    }
}

/// Offset into a symbol log file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SymbolLogOffset(pub u64);

impl SymbolLogOffset {
    #[must_use]
    pub const fn new(offset: u64) -> Self {
        Self(offset)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Minimal bloom filter for fast "not present" checks in index segments.
///
/// Uses double hashing (xxh3 + BLAKE3 truncated) to probe `k` bit positions
/// in a bitvec of `m` bits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: u32,
    num_hashes: u8,
}

impl BloomFilter {
    /// Create a new bloom filter sized for `expected_items` with the given
    /// false positive rate.
    ///
    /// Uses the classic formula: m = -n*ln(p) / (ln2)^2, k = (m/n)*ln2.
    #[must_use]
    pub fn new(expected_items: u32, false_positive_rate: f64) -> Self {
        let n = f64::from(expected_items).max(1.0);
        let p = false_positive_rate.clamp(1e-10, 0.5);

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let m = ((-n * p.ln()) / (core::f64::consts::LN_2.powi(2))).ceil() as u32;
        let m = m.max(64); // minimum 64 bits

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let k = ((f64::from(m) / n) * core::f64::consts::LN_2).ceil() as u8;
        let k = k.clamp(1, 16);

        let words = usize::try_from(m.div_ceil(64)).expect("BloomFilter word count fits usize");
        Self {
            bits: vec![0u64; words],
            num_bits: m,
            num_hashes: k,
        }
    }

    /// Insert a page number into the filter.
    pub fn insert(&mut self, page: crate::PageNumber) {
        let raw = page.get();
        let (h1, h2) = Self::double_hash(raw);
        for i in 0..u32::from(self.num_hashes) {
            let pos = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            let word = (pos / 64) as usize;
            let bit = pos % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    /// Check if a page number might be present.
    ///
    /// Returns `false` if definitely not present (zero false negatives).
    /// Returns `true` if possibly present (may be false positive).
    #[must_use]
    pub fn maybe_contains(&self, page: crate::PageNumber) -> bool {
        let raw = page.get();
        let (h1, h2) = Self::double_hash(raw);
        for i in 0..u32::from(self.num_hashes) {
            let pos = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            let word = (pos / 64) as usize;
            let bit = pos % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    fn double_hash(page_raw: u32) -> (u32, u32) {
        let mut bytes = [0u8; 4];
        crate::encoding::write_u32_le(&mut bytes, page_raw).expect("fixed u32 field");
        let h1 = xxhash_rust::xxh3::xxh3_64(&bytes);
        let mut h2 = {
            let digest = blake3::hash(&bytes);
            let b = digest.as_bytes();
            read_u32_le(&b[..4]).expect("blake3 digest prefix is 4 bytes")
        };
        // A zero step size degenerates double hashing into a single probe.
        if h2 == 0 {
            h2 = 1;
        }
        #[allow(clippy::cast_possible_truncation)]
        let h1_trunc = h1 as u32;
        (h1_trunc, h2)
    }
}

/// Maps `PageNumber -> VersionPointer` for a specific commit range (§3.6.3).
///
/// Includes a bloom filter for fast "not present" checks. All index segments
/// are ECS objects (content-addressed, repairable via RaptorQ).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageVersionIndexSegment {
    /// Inclusive start of the commit range covered.
    pub start_seq: u64,
    /// Inclusive end of the commit range covered.
    pub end_seq: u64,
    /// Sorted entries mapping page numbers to version pointers.
    ///
    /// Sorted by `(page_number, commit_seq)` ascending. Multiple entries per
    /// page are allowed (a page may be updated multiple times within the
    /// segment's commit range).
    pub entries: Vec<(crate::PageNumber, VersionPointer)>,
    /// Bloom filter for fast "not present" checks.
    pub bloom: BloomFilter,
}

impl PageVersionIndexSegment {
    /// Create a new segment from entries. Sorts entries by `(page, commit_seq)` and
    /// builds the bloom filter automatically.
    #[must_use]
    pub fn new(
        start_seq: u64,
        end_seq: u64,
        mut entries: Vec<(crate::PageNumber, VersionPointer)>,
    ) -> Self {
        entries.sort_by_key(|(pgno, vp)| (pgno.get(), vp.commit_seq));

        #[allow(clippy::cast_possible_truncation)]
        let count = entries.len() as u32;
        let mut bloom = BloomFilter::new(count.max(1), 0.01);
        for &(pgno, _) in &entries {
            bloom.insert(pgno);
        }

        Self {
            start_seq,
            end_seq,
            entries,
            bloom,
        }
    }

    /// Look up the newest version pointer for `page` with
    /// `commit_seq <= snapshot_high`.
    ///
    /// Returns `None` if the page has no entry in this segment or if
    /// no version is visible under the given snapshot.
    #[must_use]
    pub fn lookup(&self, page: crate::PageNumber, snapshot_high: u64) -> Option<&VersionPointer> {
        if !self.bloom.maybe_contains(page) {
            return None;
        }

        let page_raw = page.get();
        let start = self
            .entries
            .partition_point(|(pgno, _)| pgno.get() < page_raw);
        let end = self
            .entries
            .partition_point(|(pgno, _)| pgno.get() <= page_raw);
        let slice = self.entries.get(start..end)?;
        if slice.is_empty() {
            return None;
        }

        // Find the newest commit_seq <= snapshot_high.
        let idx = slice.partition_point(|(_, vp)| vp.commit_seq <= snapshot_high);
        if idx == 0 {
            None
        } else {
            Some(&slice[idx - 1].1)
        }
    }
}

/// Maps `ObjectId -> Vec<SymbolLogOffset>` — accelerator for finding
/// symbols on disk (§3.6.3).
///
/// Rebuildable by scanning symbol logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLocatorSegment {
    /// Sorted entries mapping object IDs to their symbol log offsets.
    pub entries: Vec<(ObjectId, Vec<SymbolLogOffset>)>,
}

impl ObjectLocatorSegment {
    /// Create from unsorted entries. Sorts by `ObjectId` bytes for
    /// deterministic encoding.
    #[must_use]
    pub fn new(mut entries: Vec<(ObjectId, Vec<SymbolLogOffset>)>) -> Self {
        entries.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        Self { entries }
    }

    /// Look up symbol log offsets for a given `ObjectId`.
    #[must_use]
    pub fn lookup(&self, id: &ObjectId) -> Option<&[SymbolLogOffset]> {
        self.entries
            .binary_search_by(|(oid, _)| oid.as_bytes().cmp(id.as_bytes()))
            .ok()
            .map(|idx| self.entries[idx].1.as_slice())
    }

    /// Rebuild from a set of `(ObjectId, SymbolLogOffset)` pairs, typically
    /// obtained by scanning symbol log files.
    #[must_use]
    pub fn rebuild_from_scan(pairs: impl IntoIterator<Item = (ObjectId, SymbolLogOffset)>) -> Self {
        let mut map: std::collections::BTreeMap<[u8; 16], Vec<SymbolLogOffset>> =
            std::collections::BTreeMap::new();
        for (oid, offset) in pairs {
            map.entry(*oid.as_bytes()).or_default().push(offset);
        }
        let entries: Vec<_> = map
            .into_iter()
            .map(|(bytes, mut offsets)| {
                offsets.sort();
                (ObjectId::from_bytes(bytes), offsets)
            })
            .collect();
        Self { entries }
    }
}

/// Maps commit_seq ranges to `IndexSegment` object IDs (§3.6.3).
///
/// Used for bootstrapping: given a commit_seq, find which index segment
/// covers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSegment {
    /// Sorted, non-overlapping entries: (start_seq, end_seq, segment ObjectId).
    pub entries: Vec<(u64, u64, ObjectId)>,
}

impl ManifestSegment {
    /// Create from entries. Sorts by `start_seq` for binary search.
    #[must_use]
    pub fn new(mut entries: Vec<(u64, u64, ObjectId)>) -> Self {
        entries.sort_by_key(|&(start, _, _)| start);
        Self { entries }
    }

    /// Find the index segment covering the given `commit_seq`.
    #[must_use]
    pub fn lookup(&self, commit_seq: u64) -> Option<&ObjectId> {
        // Binary search for the last entry with start_seq <= commit_seq
        let idx = self
            .entries
            .partition_point(|&(start, _, _)| start <= commit_seq);
        if idx == 0 {
            return None;
        }
        let (start, end, ref oid) = self.entries[idx - 1];
        if commit_seq >= start && commit_seq <= end {
            Some(oid)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{read_u32_le, read_u64_le};

    #[test]
    fn test_object_id_blake3_derivation() {
        let header = b"hdr:v1\x00";
        let payload = b"hello world";
        let payload_hash = PayloadHash::blake3(payload);

        let derived = ObjectId::derive(header, payload_hash);

        let mut canonical = Vec::new();
        canonical.extend_from_slice(header);
        canonical.extend_from_slice(payload_hash.as_bytes());
        let derived2 = ObjectId::derive_from_canonical_bytes(&canonical);

        assert_eq!(derived, derived2);

        let mut hasher = blake3::Hasher::new();
        hasher.update(ObjectId::DOMAIN_SEPARATOR);
        hasher.update(&canonical);
        let digest = hasher.finalize();
        let mut expected = [0u8; 16];
        expected.copy_from_slice(&digest.as_bytes()[..16]);

        assert_eq!(derived.as_bytes(), &expected);
    }

    #[test]
    fn test_object_id_collision_resistance() {
        let header = b"hdr:v1\x00";
        let payload_a = b"payload-a";
        let payload_b = b"payload-b";
        let id_a = ObjectId::derive(header, PayloadHash::blake3(payload_a));
        let id_b = ObjectId::derive(header, PayloadHash::blake3(payload_b));
        assert_ne!(id_a, id_b);
    }

    #[test]
    fn test_object_id_deterministic() {
        let header = b"hdr:v1\x00";
        let payload = b"payload";
        let hash = PayloadHash::blake3(payload);
        let id1 = ObjectId::derive(header, hash);
        let id2 = ObjectId::derive(header, hash);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_object_id_display_hex() {
        let id = ObjectId::from_bytes([0u8; 16]);
        let s = id.to_string();
        assert_eq!(s.len(), 32);
        assert!(s.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f')));

        // A stable known-value check (16 zero bytes => 32 zero hex chars).
        assert_eq!(s, "00000000000000000000000000000000");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn test_oti(symbol_size: u32) -> Oti {
        Oti {
            f: 16384,
            al: 4,
            t: symbol_size,
            z: 1,
            n: 1,
        }
    }

    fn test_record(symbol_size: u32) -> SymbolRecord {
        let data = vec![0xAB; symbol_size as usize];
        let oid = ObjectId::from_bytes([1u8; 16]);
        SymbolRecord::new(
            oid,
            test_oti(symbol_size),
            0,
            data,
            SymbolRecordFlags::empty(),
        )
    }

    fn make_symbol_run(
        object_id: ObjectId,
        source_symbols: u32,
        symbol_size: u32,
        repair_symbols: u32,
    ) -> (Vec<SymbolRecord>, Vec<u8>, Oti) {
        let symbol_size_usize = usize::try_from(symbol_size).expect("symbol_size fits usize");
        let transfer_length = u64::from(source_symbols).saturating_mul(u64::from(symbol_size));
        let oti = Oti {
            f: transfer_length,
            al: 4,
            t: symbol_size,
            z: 1,
            n: 1,
        };
        let mut records = Vec::new();
        let mut expected = Vec::new();

        for esi in 0..source_symbols {
            let mut payload = Vec::with_capacity(symbol_size_usize);
            for idx in 0..symbol_size_usize {
                let idx_low = u8::try_from(idx & 0xFF).expect("masked to u8");
                let esi_low = u8::try_from(esi & 0xFF).expect("masked to u8");
                payload.push(esi_low ^ idx_low.wrapping_mul(3));
            }
            expected.extend_from_slice(&payload);
            let flags = if esi == 0 {
                SymbolRecordFlags::SYSTEMATIC_RUN_START
            } else {
                SymbolRecordFlags::empty()
            };
            records.push(SymbolRecord::new(object_id, oti, esi, payload, flags));
        }

        for repair in 0..repair_symbols {
            let esi = source_symbols.saturating_add(repair);
            let mut payload = vec![0u8; symbol_size_usize];
            let esi_low = u8::try_from(esi & 0xFF).expect("masked to u8");
            for (idx, byte) in payload.iter_mut().enumerate() {
                let idx_low = u8::try_from(idx & 0xFF).expect("masked to u8");
                *byte = esi_low.wrapping_mul(13) ^ idx_low;
            }
            records.push(SymbolRecord::new(
                object_id,
                oti,
                esi,
                payload,
                SymbolRecordFlags::empty(),
            ));
        }

        (records, expected, oti)
    }

    // -----------------------------------------------------------------------
    // §3.5.2 SymbolRecord tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_symbol_record_serialize_deserialize() {
        let rec = test_record(4096);
        let bytes = rec.to_bytes();
        let rec2 = SymbolRecord::from_bytes(&bytes).expect("roundtrip");
        assert_eq!(rec, rec2);
    }

    #[test]
    fn test_symbol_record_magic_validation() {
        let rec = test_record(64);
        let mut bytes = rec.to_bytes();
        bytes[0] = 0xFF;
        let err = SymbolRecord::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, SymbolRecordError::BadMagic(_)));
    }

    #[test]
    fn test_symbol_record_frame_xxh3_integrity() {
        let rec = test_record(128);
        let mut bytes = rec.to_bytes();
        // Flip one bit in symbol_data
        bytes[HEADER_BEFORE_DATA] ^= 0x01;
        let err = SymbolRecord::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, SymbolRecordError::IntegrityFailure { .. }));
    }

    #[test]
    fn test_symbol_record_invariant_symbol_size_eq_oti_t() {
        let oid = ObjectId::from_bytes([2u8; 16]);
        let oti = test_oti(100);
        // Manually build wire bytes with symbol_size=200 but OTI.T=100
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SYMBOL_RECORD_MAGIC);
        bytes.push(SYMBOL_RECORD_VERSION);
        bytes.extend_from_slice(oid.as_bytes());
        bytes.extend_from_slice(&oti.to_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // esi
        bytes.extend_from_slice(&200u32.to_le_bytes()); // symbol_size != oti.t
        bytes.extend_from_slice(&[0u8; 200]);
        bytes.push(0); // flags
        let hash = xxhash_rust::xxh3::xxh3_64(&bytes);
        bytes.extend_from_slice(&hash.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 16]);

        let err = SymbolRecord::from_bytes(&bytes).unwrap_err();
        assert!(matches!(
            err,
            SymbolRecordError::SymbolSizeMismatch {
                symbol_size: 200,
                oti_t: 100
            }
        ));
    }

    #[test]
    fn test_symbol_record_auth_tag_verification() {
        let epoch_key = [0x42u8; 32];
        let rec = test_record(64).with_auth_tag(&epoch_key);
        assert_ne!(rec.auth_tag, [0u8; 16]);
        assert!(rec.verify_auth(&epoch_key));

        // Tamper: change one data byte, recompute frame_xxh3 but NOT auth_tag
        let mut tampered = rec;
        tampered.symbol_data[0] ^= 0x01;
        let pre_hash = tampered.pre_hash_bytes();
        tampered.frame_xxh3 = xxhash_rust::xxh3::xxh3_64(&pre_hash);
        assert!(!tampered.verify_auth(&epoch_key));
    }

    #[test]
    fn test_symbol_record_auth_tag_ignored_when_off() {
        let rec = test_record(64);
        assert_eq!(rec.auth_tag, [0u8; 16]);
        let any_key = [0xFFu8; 32];
        assert!(rec.verify_auth(&any_key));
    }

    #[test]
    fn test_symbol_record_systematic_flag() {
        let oid = ObjectId::from_bytes([3u8; 16]);
        let rec = SymbolRecord::new(
            oid,
            test_oti(64),
            0,
            vec![0u8; 64],
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        );
        assert!(rec.flags.contains(SymbolRecordFlags::SYSTEMATIC_RUN_START));
        assert_eq!(rec.esi, 0);

        let bytes = rec.to_bytes();
        let rec2 = SymbolRecord::from_bytes(&bytes).unwrap();
        assert!(rec2.flags.contains(SymbolRecordFlags::SYSTEMATIC_RUN_START));
    }

    #[test]
    fn test_oti_field_widths() {
        let oti = Oti {
            f: 1_000_000,
            al: 4,
            t: 65536,
            z: 10,
            n: 1,
        };
        let bytes = oti.to_bytes();
        let oti2 = Oti::from_bytes(&bytes).unwrap();
        assert_eq!(oti, oti2);
        assert_eq!(oti2.t, 65536);
    }

    #[test]
    fn test_systematic_fast_path_happy() {
        let oid = ObjectId::from_bytes([4u8; 16]);
        let oti = Oti {
            f: 256,
            al: 4,
            t: 64,
            z: 1,
            n: 1,
        };

        let records: Vec<_> = (0u32..4)
            .map(|i| {
                let flags = if i == 0 {
                    SymbolRecordFlags::SYSTEMATIC_RUN_START
                } else {
                    SymbolRecordFlags::empty()
                };
                let fill = u8::try_from(i).expect("i < 4");
                SymbolRecord::new(oid, oti, i, vec![fill; 64], flags)
            })
            .collect();

        assert!(
            records[0]
                .flags
                .contains(SymbolRecordFlags::SYSTEMATIC_RUN_START)
        );
        for rec in &records[1..] {
            assert!(!rec.flags.contains(SymbolRecordFlags::SYSTEMATIC_RUN_START));
        }

        // Reconstruct via systematic fast path
        let mut reconstructed = Vec::new();
        for rec in &records {
            assert!(rec.verify_integrity());
            reconstructed.extend_from_slice(&rec.symbol_data);
        }
        let f = usize::try_from(oti.f).expect("OTI transfer length fits in usize");
        reconstructed.truncate(f);
        assert_eq!(reconstructed.len(), 256);

        for (i, chunk) in reconstructed.chunks(64).enumerate() {
            let expected = u8::try_from(i).expect("i < 4");
            assert!(chunk.iter().all(|&b| b == expected));
        }
    }

    #[test]
    fn test_systematic_fast_path_fallback() {
        let oid = ObjectId::from_bytes([5u8; 16]);
        let oti = Oti {
            f: 256,
            al: 4,
            t: 64,
            z: 1,
            n: 1,
        };

        let rec2 = SymbolRecord::new(oid, oti, 2, vec![2u8; 64], SymbolRecordFlags::empty());
        let mut bytes = rec2.to_bytes();
        bytes[HEADER_BEFORE_DATA] ^= 0xFF; // corrupt data

        let result = SymbolRecord::from_bytes(&bytes);
        assert!(matches!(
            result.unwrap_err(),
            SymbolRecordError::IntegrityFailure { .. }
        ));
    }

    #[test]
    fn test_systematic_symbols_contiguous() {
        let oid = ObjectId::from_bytes([0x44; 16]);
        let (mut records, expected, oti) = make_symbol_run(oid, 100, 64, 8);
        let repair = records
            .pop()
            .expect("repair symbol exists for interleaving simulation");
        records.insert(9, repair);
        records.swap(3, 21);

        let ordered = layout_systematic_run(records).expect("layout must normalize");
        let source_symbols = validate_systematic_run(&ordered).expect("must be contiguous");
        assert_eq!(source_symbols, 100);

        for (idx, record) in ordered.iter().take(100).enumerate() {
            let expected_esi = u32::try_from(idx).expect("idx fits u32");
            assert_eq!(record.esi, expected_esi);
        }
        assert!(
            ordered.iter().skip(100).all(|record| record.esi >= 100_u32),
            "repair symbols must follow source run"
        );
        assert!(
            ordered[0]
                .flags
                .contains(SymbolRecordFlags::SYSTEMATIC_RUN_START)
        );
        assert!(ordered[1..].iter().all(|record| {
            !record
                .flags
                .contains(SymbolRecordFlags::SYSTEMATIC_RUN_START)
        }));

        let recovered =
            reconstruct_systematic_happy_path(&ordered).expect("happy-path reconstruction");
        assert_eq!(
            recovered.len(),
            usize::try_from(oti.f).expect("transfer length fits usize")
        );
        assert_eq!(recovered, expected);
    }

    #[test]
    fn test_happy_path_read_no_gf256() {
        let oid = ObjectId::from_bytes([0x55; 16]);
        let (records, expected, _) = make_symbol_run(oid, 50, 64, 5);
        let decode_invocations = std::cell::Cell::new(0_u32);

        let (decoded, path) = recover_object_with_fallback(&records, |_| {
            decode_invocations.set(decode_invocations.get().saturating_add(1));
            Err(SystematicLayoutError::EmptySymbolSet)
        })
        .expect("happy-path should succeed");

        assert!(matches!(path, SymbolReadPath::SystematicFastPath));
        assert_eq!(
            decode_invocations.get(),
            0,
            "fallback decode must not run on systematic happy path"
        );
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_fallback_on_missing_symbol() {
        let oid = ObjectId::from_bytes([0x66; 16]);
        let (mut records, expected, _) = make_symbol_run(oid, 50, 64, 5);
        records.retain(|record| record.esi != 5);

        let decode_invocations = std::cell::Cell::new(0_u32);
        let fallback_expected = expected.clone();
        let (decoded, path) = recover_object_with_fallback(&records, |_| {
            decode_invocations.set(decode_invocations.get().saturating_add(1));
            Ok(fallback_expected.clone())
        })
        .expect("fallback decode should recover object");

        assert_eq!(decode_invocations.get(), 1);
        assert_eq!(decoded, expected);
        match path {
            SymbolReadPath::FullDecodeFallback { reason } => {
                assert!(matches!(
                    reason,
                    SystematicLayoutError::NonContiguousSystematicSymbol {
                        expected_esi: 5,
                        ..
                    } | SystematicLayoutError::MissingSystematicSymbol { expected_esi: 5 }
                ));
            }
            SymbolReadPath::SystematicFastPath => {
                panic!("fallback path expected when a systematic symbol is missing");
            }
        }
    }

    #[test]
    fn test_fallback_on_corruption() {
        let oid = ObjectId::from_bytes([0x77; 16]);
        let (mut records, expected, _) = make_symbol_run(oid, 50, 64, 5);
        let corrupt_idx = records
            .iter()
            .position(|record| record.esi == 3)
            .expect("ESI 3 present");
        records[corrupt_idx].symbol_data[0] ^= 0xAA;

        let decode_invocations = std::cell::Cell::new(0_u32);
        let fallback_expected = expected.clone();
        let (decoded, path) = recover_object_with_fallback(&records, |_| {
            decode_invocations.set(decode_invocations.get().saturating_add(1));
            Ok(fallback_expected.clone())
        })
        .expect("fallback decode should recover corrupted symbol run");

        assert_eq!(decode_invocations.get(), 1);
        assert_eq!(decoded, expected);
        match path {
            SymbolReadPath::FullDecodeFallback { reason } => {
                assert!(matches!(
                    reason,
                    SystematicLayoutError::CorruptSymbol { esi: 3 }
                ));
            }
            SymbolReadPath::SystematicFastPath => {
                panic!("corrupted systematic symbol must trigger fallback");
            }
        }
    }

    #[test]
    fn test_benchmark_happy_vs_full() {
        fn emulate_full_decode(records: &[SymbolRecord]) -> Vec<u8> {
            let first = records.first().expect("records non-empty");
            let source_symbols = source_symbol_count(first.oti).expect("valid K");
            let source_symbols_u32 = u32::try_from(source_symbols).expect("K fits u32");
            let symbol_size = usize::try_from(first.oti.t).expect("symbol size fits usize");
            let mut scratch = vec![0_u8; symbol_size];
            let mut out = Vec::with_capacity(source_symbols.saturating_mul(symbol_size));

            for record in records {
                if record.esi < source_symbols_u32 {
                    out.extend_from_slice(&record.symbol_data);
                }
                let coeff = u8::try_from((record.esi % 251) + 1).expect("coeff in 1..=251");
                for _ in 0..24 {
                    for (dst, src) in scratch.iter_mut().zip(record.symbol_data.iter()) {
                        *dst ^= crate::gf256_mul_byte(coeff, *src);
                    }
                }
            }

            let transfer_len = usize::try_from(first.oti.f).expect("transfer length fits usize");
            out.truncate(transfer_len);
            out
        }

        let oid = ObjectId::from_bytes([0x88; 16]);
        let (records, expected, _) = make_symbol_run(oid, 100, 4096, 6);
        let rounds = 6_u32;

        let fast_start = std::time::Instant::now();
        let mut fast_guard = 0_u8;
        for _ in 0..rounds {
            let decoded = reconstruct_systematic_happy_path(&records).expect("happy-path decode");
            fast_guard ^= decoded[0];
            assert_eq!(decoded, expected);
        }
        let fast_elapsed = fast_start.elapsed();

        let full_start = std::time::Instant::now();
        let mut full_guard = 0_u8;
        for _ in 0..rounds {
            let decoded = emulate_full_decode(&records);
            full_guard ^= decoded[0];
            assert_eq!(decoded, expected);
        }
        let full_elapsed = full_start.elapsed();

        assert_ne!(
            fast_guard,
            full_guard.wrapping_add(1),
            "keep optimizer honest"
        );

        let fast_ns = fast_elapsed.as_nanos().max(1);
        let full_ns = full_elapsed.as_nanos().max(1);
        let speedup = full_ns as f64 / fast_ns as f64;
        assert!(
            speedup >= 10.0,
            "expected happy-path to be >=10x faster, got {speedup:.2}x (happy={fast_elapsed:?}, full={full_elapsed:?})"
        );
    }

    #[test]
    fn test_symbol_record_version_validation() {
        let rec = test_record(64);
        let mut bytes = rec.to_bytes();
        bytes[4] = 99;
        let err = SymbolRecord::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, SymbolRecordError::UnsupportedVersion(99)));
    }

    #[test]
    fn test_symbol_record_too_short() {
        let err = SymbolRecord::from_bytes(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, SymbolRecordError::TooShort { .. }));
    }

    #[test]
    fn test_symbol_record_wire_size() {
        let rec = test_record(4096);
        assert_eq!(
            rec.wire_size(),
            HEADER_BEFORE_DATA + 4096 + TRAILER_AFTER_DATA
        );
        assert_eq!(rec.wire_size(), rec.to_bytes().len());
    }

    #[test]
    fn test_symbol_record_verify_integrity() {
        let rec = test_record(128);
        assert!(rec.verify_integrity());

        let mut bad = rec;
        bad.symbol_data[0] ^= 0x01;
        assert!(!bad.verify_integrity());
    }

    #[test]
    fn test_oti_roundtrip() {
        let oti = Oti {
            f: u64::MAX,
            al: u16::MAX,
            t: u32::MAX,
            z: u32::MAX,
            n: u32::MAX,
        };
        let bytes = oti.to_bytes();
        assert_eq!(bytes.len(), OTI_WIRE_SIZE);
        let oti2 = Oti::from_bytes(&bytes).unwrap();
        assert_eq!(oti, oti2);
    }

    #[test]
    fn test_oti_from_bytes_too_short() {
        assert!(Oti::from_bytes(&[0u8; 10]).is_none());
    }

    // -----------------------------------------------------------------------
    // §3.6 Native Index Types tests
    // -----------------------------------------------------------------------

    fn make_oid(seed: u8) -> ObjectId {
        ObjectId::from_bytes([seed; 16])
    }

    fn make_page(n: u32) -> crate::PageNumber {
        crate::PageNumber::new(n).expect("non-zero")
    }

    fn make_vp(seq: u64, seed: u8, kind: PatchKind) -> VersionPointer {
        VersionPointer {
            commit_seq: seq,
            patch_object: make_oid(seed),
            patch_kind: kind,
            base_hint: None,
        }
    }

    #[test]
    fn test_version_pointer_serialization_roundtrip() {
        for kind in [
            PatchKind::FullImage,
            PatchKind::IntentLog,
            PatchKind::SparseXor,
        ] {
            let vp = VersionPointer {
                commit_seq: 42,
                patch_object: make_oid(0xAA),
                patch_kind: kind,
                base_hint: None,
            };
            let bytes = vp.to_bytes();
            let vp2 = VersionPointer::from_bytes(&bytes).unwrap();
            assert_eq!(vp, vp2);

            let vp_with_base = VersionPointer {
                base_hint: Some(make_oid(0xBB)),
                ..vp
            };
            let bytes2 = vp_with_base.to_bytes();
            let vp3 = VersionPointer::from_bytes(&bytes2).unwrap();
            assert_eq!(vp_with_base, vp3);
        }
    }

    #[test]
    fn test_page_version_index_segment_lookup() {
        let entries: Vec<_> = (1..=50u32)
            .map(|i| {
                let pgno = make_page(i);
                let seed = u8::try_from(i).expect("i <= 50");
                let vp = make_vp(u64::from(i) + 10, seed, PatchKind::FullImage);
                (pgno, vp)
            })
            .collect();

        let seg = PageVersionIndexSegment::new(10, 60, entries);

        let result = seg.lookup(make_page(25), 60);
        assert!(result.is_some());
        assert_eq!(result.unwrap().commit_seq, 35);

        assert!(seg.lookup(make_page(25), 30).is_none());
        assert!(seg.lookup(make_page(99), 60).is_none());
    }

    #[test]
    fn test_page_version_index_segment_lookup_picks_latest_leq_snapshot() {
        let page = make_page(7);
        let vp10 = make_vp(10, 0x10, PatchKind::FullImage);
        let vp15 = make_vp(15, 0x20, PatchKind::IntentLog);
        let vp20 = make_vp(20, 0x30, PatchKind::SparseXor);
        let seg =
            PageVersionIndexSegment::new(10, 20, vec![(page, vp10), (page, vp15), (page, vp20)]);

        assert!(seg.lookup(page, 9).is_none());
        assert_eq!(seg.lookup(page, 10), Some(&vp10));
        assert_eq!(seg.lookup(page, 14), Some(&vp10));
        assert_eq!(seg.lookup(page, 15), Some(&vp15));
        assert_eq!(seg.lookup(page, 19), Some(&vp15));
        assert_eq!(seg.lookup(page, 20), Some(&vp20));
    }

    #[test]
    fn test_page_version_index_segment_bloom_filter() {
        let entries: Vec<_> = (1..=100u32)
            .map(|i| {
                let seed = u8::try_from(i).expect("i <= 100");
                (
                    make_page(i),
                    make_vp(u64::from(i), seed, PatchKind::FullImage),
                )
            })
            .collect();
        let seg = PageVersionIndexSegment::new(1, 100, entries);

        // Zero false negatives
        for i in 1..=100u32 {
            assert!(
                seg.bloom.maybe_contains(make_page(i)),
                "bloom must not have false negatives for page {i}"
            );
        }

        // False positive rate check
        let mut false_positives = 0u32;
        for i in 101..=1100u32 {
            if seg.bloom.maybe_contains(make_page(i)) {
                false_positives += 1;
            }
        }
        let fp_rate = f64::from(false_positives) / 1000.0;
        assert!(fp_rate < 0.05, "bloom FP rate {fp_rate:.3} exceeds 5%");
    }

    #[test]
    fn test_object_locator_segment_rebuild() {
        let pairs = vec![
            (make_oid(1), vec![SymbolLogOffset(0), SymbolLogOffset(100)]),
            (make_oid(2), vec![SymbolLogOffset(200)]),
            (
                make_oid(3),
                vec![SymbolLogOffset(300), SymbolLogOffset(400)],
            ),
        ];
        let seg = ObjectLocatorSegment::new(pairs);

        let scan_pairs = vec![
            (make_oid(1), SymbolLogOffset(100)),
            (make_oid(3), SymbolLogOffset(300)),
            (make_oid(1), SymbolLogOffset(0)),
            (make_oid(2), SymbolLogOffset(200)),
            (make_oid(3), SymbolLogOffset(400)),
        ];
        let rebuilt = ObjectLocatorSegment::rebuild_from_scan(scan_pairs);

        assert_eq!(seg.lookup(&make_oid(1)), rebuilt.lookup(&make_oid(1)));
        assert_eq!(seg.lookup(&make_oid(2)), rebuilt.lookup(&make_oid(2)));
        assert_eq!(seg.lookup(&make_oid(3)), rebuilt.lookup(&make_oid(3)));
        assert!(seg.lookup(&make_oid(99)).is_none());
    }

    #[test]
    fn test_manifest_segment_bootstrap() {
        let seg = ManifestSegment::new(vec![
            (1, 100, make_oid(0x10)),
            (101, 200, make_oid(0x20)),
            (201, 300, make_oid(0x30)),
        ]);

        assert_eq!(seg.lookup(50), Some(&make_oid(0x10)));
        assert_eq!(seg.lookup(100), Some(&make_oid(0x10)));
        assert_eq!(seg.lookup(101), Some(&make_oid(0x20)));
        assert_eq!(seg.lookup(250), Some(&make_oid(0x30)));
        assert_eq!(seg.lookup(300), Some(&make_oid(0x30)));
        assert!(seg.lookup(0).is_none());
        assert!(seg.lookup(301).is_none());
    }

    #[test]
    fn test_version_pointer_references_content_addressed() {
        let vp = make_vp(42, 0xCC, PatchKind::FullImage);
        assert_eq!(vp.patch_object.as_bytes().len(), ObjectId::LEN);
    }

    #[test]
    fn test_patch_kind_from_byte() {
        assert_eq!(PatchKind::from_byte(0), Some(PatchKind::FullImage));
        assert_eq!(PatchKind::from_byte(1), Some(PatchKind::IntentLog));
        assert_eq!(PatchKind::from_byte(2), Some(PatchKind::SparseXor));
        assert!(PatchKind::from_byte(3).is_none());
        assert!(PatchKind::from_byte(255).is_none());
    }

    #[test]
    fn test_version_pointer_too_short() {
        assert!(VersionPointer::from_bytes(&[0u8; 10]).is_none());
        let vp = make_vp(1, 1, PatchKind::FullImage);
        let bytes = vp.to_bytes();
        assert_eq!(bytes.len(), VERSION_POINTER_MIN_WIRE);
        assert!(VersionPointer::from_bytes(&bytes).is_some());
    }

    #[test]
    fn test_native_ecs_structures_little_endian() {
        let rec = test_record(64);
        let bytes = rec.to_bytes();
        assert_eq!(read_u32_le(&bytes[43..47]), Some(rec.esi));
        assert_eq!(read_u32_le(&bytes[47..51]), Some(rec.oti.t));
        let frame_offset = HEADER_BEFORE_DATA + rec.symbol_data.len() + 1;
        assert_eq!(
            read_u64_le(&bytes[frame_offset..frame_offset + 8]),
            Some(rec.frame_xxh3)
        );

        let vp = make_vp(0x0102_0304_0506_0708, 0xAA, PatchKind::SparseXor);
        let vp_bytes = vp.to_bytes();
        assert_eq!(
            read_u64_le(&vp_bytes[0..8]),
            Some(0x0102_0304_0506_0708),
            "version pointer commit_seq must remain little-endian"
        );
    }

    #[test]
    fn test_canonical_encoding_unique() {
        let rec = test_record(48);
        let encoded_a = rec.to_bytes();
        let encoded_b = rec.to_bytes();
        assert_eq!(
            encoded_a, encoded_b,
            "same symbol record must encode identically"
        );

        let different = make_vp(2, 0x11, PatchKind::FullImage);
        let different_encoded = different.to_bytes();
        assert_ne!(
            encoded_a, different_encoded,
            "different structures must not share canonical byte encodings"
        );
    }

    #[test]
    fn test_roundtrip_encode_decode() {
        let oti = test_oti(512);
        let oti_bytes = oti.to_bytes();
        let oti_decoded = Oti::from_bytes(&oti_bytes).expect("OTI roundtrip must succeed");
        assert_eq!(oti, oti_decoded);

        let rec = test_record(128);
        let rec_bytes = rec.to_bytes();
        let rec_decoded =
            SymbolRecord::from_bytes(&rec_bytes).expect("symbol record roundtrip must succeed");
        assert_eq!(rec, rec_decoded);

        let vp = make_vp(99, 0x55, PatchKind::IntentLog);
        let vp_bytes = vp.to_bytes();
        let vp_decoded =
            VersionPointer::from_bytes(&vp_bytes).expect("version pointer roundtrip must succeed");
        assert_eq!(vp, vp_decoded);
    }

    #[test]
    fn test_no_adhoc_byte_shuffling() {
        let source = include_str!("ecs.rs");
        let production = source.split("\n#[cfg(test)]").next().unwrap_or(source);
        assert!(
            !production.contains("to_le_bytes("),
            "production ECS serialization should use canonical helpers"
        );
        assert!(
            !production.contains("from_le_bytes("),
            "production ECS decoding should use canonical helpers"
        );
        assert!(
            production.contains("append_u32_le")
                && production.contains("append_u64_le")
                && production.contains("read_u32_le")
                && production.contains("read_u64_le"),
            "expected canonical helper usage markers missing"
        );
    }

    #[test]
    fn test_symbol_log_offset_ordering() {
        let a = SymbolLogOffset::new(10);
        let b = SymbolLogOffset::new(20);
        assert!(a < b);
        assert_eq!(a.get(), 10);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_oti() -> impl Strategy<Value = Oti> {
        (
            any::<u64>(),
            any::<u16>(),
            1..=65536u32,
            1..=100u32,
            1..=100u32,
        )
            .prop_map(|(f, al, t, z, n)| Oti { f, al, t, z, n })
    }

    proptest! {
        #[test]
        fn prop_symbol_record_roundtrip(
            oti in arb_oti(),
            esi in any::<u32>(),
            data_byte in any::<u8>(),
        ) {
            let oid = ObjectId::from_bytes([7u8; 16]);
            let data = vec![data_byte; oti.t as usize];
            let rec = SymbolRecord::new(oid, oti, esi, data, SymbolRecordFlags::empty());
            let bytes = rec.to_bytes();
            let rec2 = SymbolRecord::from_bytes(&bytes).unwrap();
            prop_assert_eq!(rec, rec2);
        }

        #[test]
        fn test_write_produces_contiguous_layout(
            source_symbols in 1u16..=500u16,
            symbol_size in prop::sample::select(vec![64u32, 128u32, 256u32, 512u32]),
            seed in any::<u8>(),
        ) {
            let source_symbols_u32 = u32::from(source_symbols);
            let symbol_size_usize = usize::try_from(symbol_size).expect("symbol size fits usize");
            let transfer_length = u64::from(source_symbols_u32).saturating_mul(u64::from(symbol_size));
            let oti = Oti {
                f: transfer_length,
                al: 4,
                t: symbol_size,
                z: 1,
                n: 1,
            };
            let oid = ObjectId::from_bytes([seed; 16]);

            let mut records = Vec::new();
            for esi in 0..source_symbols_u32 {
                let mut payload = vec![0u8; symbol_size_usize];
                let esi_low = u8::try_from(esi & 0xFF).expect("masked to u8");
                for (idx, byte) in payload.iter_mut().enumerate() {
                    let idx_low = u8::try_from(idx & 0xFF).expect("masked to u8");
                    *byte = idx_low ^ esi_low.wrapping_mul(5);
                }
                let flags = if esi == 0 {
                    SymbolRecordFlags::SYSTEMATIC_RUN_START
                } else {
                    SymbolRecordFlags::empty()
                };
                records.push(SymbolRecord::new(oid, oti, esi, payload, flags));
            }

            for extra in 0..3_u32 {
                let esi = source_symbols_u32.saturating_add(extra);
                records.push(SymbolRecord::new(
                    oid,
                    oti,
                    esi,
                    vec![0xEE; symbol_size_usize],
                    SymbolRecordFlags::empty(),
                ));
            }

            if records.len() > 1 {
                let rotate_by = usize::from(seed) % records.len();
                records.rotate_left(rotate_by);
            }

            let contiguous = layout_systematic_run(records).expect("writer layout normalization");
            let k = validate_systematic_run(&contiguous).expect("must validate after layout");
            prop_assert_eq!(k, usize::from(source_symbols));
            for (idx, record) in contiguous.iter().take(usize::from(source_symbols)).enumerate() {
                let expected_esi = u32::try_from(idx).expect("idx fits u32");
                prop_assert_eq!(record.esi, expected_esi);
            }
            prop_assert!(
                contiguous
                    .iter()
                    .skip(usize::from(source_symbols))
                    .all(|record| record.esi >= source_symbols_u32)
            );
        }
    }
}
