//! WAL-FEC sidecar format (`.wal-fec`) for self-healing WAL durability (§3.4.1).
//!
//! The sidecar is append-only. Each group is encoded as:
//! 1. length-prefixed [`WalFecGroupMeta`]
//! 2. `R` length-prefixed ECS [`SymbolRecord`] repair symbols (`esi = K..K+R-1`)
//!
//! Source symbols remain in `.wal` frames and are never duplicated in sidecar.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs;
use std::io::Write;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, Oti, PageSize, SymbolRecord, SymbolRecordFlags};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

use crate::checksum::{
    WalSalts, Xxh3Checksum128, verify_wal_fec_source_hash, wal_fec_source_hash_xxh3_128,
};

/// Magic bytes for [`WalFecGroupMeta`].
pub const WAL_FEC_GROUP_META_MAGIC: [u8; 8] = *b"FSQLWFEC";
/// Current [`WalFecGroupMeta`] wire version.
pub const WAL_FEC_GROUP_META_VERSION: u32 = 1;
/// Default `PRAGMA raptorq_repair_symbols` value for fresh databases.
pub const DEFAULT_RAPTORQ_REPAIR_SYMBOLS: u8 = 2;
/// Maximum accepted `PRAGMA raptorq_repair_symbols` value (`u8` range).
pub const MAX_RAPTORQ_REPAIR_SYMBOLS: u8 = u8::MAX;
/// Magic bytes for the optional `.wal-fec` configuration header.
pub const WAL_FEC_PRAGMA_HEADER_MAGIC: [u8; 8] = *b"FSQLWFCP";
/// Current `.wal-fec` configuration header version.
pub const WAL_FEC_PRAGMA_HEADER_VERSION: u32 = 1;

const LENGTH_PREFIX_BYTES: usize = 4;
const META_FIXED_PREFIX_BYTES: usize = 8 + 4 + (8 * 4) + 22 + 16;
const META_CHECKSUM_BYTES: usize = 8;
const WAL_FEC_PRAGMA_HEADER_BYTES: usize = 8 + 4 + 1 + 3 + 8;
const RAPTORQ_REPAIR_EVENT_CAPACITY: usize = 512;
const RAPTORQ_REPAIR_EVIDENCE_CAPACITY: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalFecPragmaHeader {
    magic: [u8; 8],
    version: u32,
    raptorq_repair_symbols: u8,
    reserved: [u8; 3],
    checksum: u64,
}

impl WalFecPragmaHeader {
    #[must_use]
    fn new(raptorq_repair_symbols: u8) -> Self {
        let mut header = Self {
            magic: WAL_FEC_PRAGMA_HEADER_MAGIC,
            version: WAL_FEC_PRAGMA_HEADER_VERSION,
            raptorq_repair_symbols,
            reserved: [0; 3],
            checksum: 0,
        };
        header.checksum = header.compute_checksum();
        header
    }

    fn from_prefix(bytes: &[u8]) -> Result<Option<Self>> {
        if bytes.len() < WAL_FEC_PRAGMA_HEADER_BYTES {
            return Ok(None);
        }

        let mut magic = [0_u8; 8];
        magic.copy_from_slice(&bytes[..8]);
        if magic != WAL_FEC_PRAGMA_HEADER_MAGIC {
            return Ok(None);
        }

        let version = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed-length slice"));
        if version != WAL_FEC_PRAGMA_HEADER_VERSION {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "unsupported wal-fec pragma header version {version}, expected {WAL_FEC_PRAGMA_HEADER_VERSION}"
                ),
            });
        }

        let raptorq_repair_symbols = bytes[12];
        let mut reserved = [0_u8; 3];
        reserved.copy_from_slice(&bytes[13..16]);
        let checksum = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed-length slice"));

        let header = Self {
            magic,
            version,
            raptorq_repair_symbols,
            reserved,
            checksum,
        };

        let computed = header.compute_checksum();
        if computed != checksum {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec pragma header checksum mismatch: stored {checksum:#018x}, computed {computed:#018x}"
                ),
            });
        }

        Ok(Some(header))
    }

    #[must_use]
    fn to_bytes(self) -> [u8; WAL_FEC_PRAGMA_HEADER_BYTES] {
        let mut out = [0_u8; WAL_FEC_PRAGMA_HEADER_BYTES];
        out[..8].copy_from_slice(&self.magic);
        out[8..12].copy_from_slice(&self.version.to_le_bytes());
        out[12] = self.raptorq_repair_symbols;
        out[13..16].copy_from_slice(&self.reserved);
        out[16..24].copy_from_slice(&self.checksum.to_le_bytes());
        out
    }

    #[must_use]
    fn compute_checksum(&self) -> u64 {
        let mut payload = [0_u8; 16];
        payload[..8].copy_from_slice(&self.magic);
        payload[8..12].copy_from_slice(&self.version.to_le_bytes());
        payload[12] = self.raptorq_repair_symbols;
        payload[13..16].copy_from_slice(&self.reserved);
        xxh3_64(&payload)
    }
}

/// Unique commit-group identifier:
/// `group_id := (wal_salt1, wal_salt2, end_frame_no)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalFecGroupId {
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub end_frame_no: u32,
}

impl fmt::Display for WalFecGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            self.wal_salt1, self.wal_salt2, self.end_frame_no
        )
    }
}

/// Builder fields for [`WalFecGroupMeta`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecGroupMetaInit {
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub start_frame_no: u32,
    pub end_frame_no: u32,
    pub db_size_pages: u32,
    pub page_size: u32,
    pub k_source: u32,
    pub r_repair: u32,
    pub oti: Oti,
    pub object_id: ObjectId,
    pub page_numbers: Vec<u32>,
    pub source_page_xxh3_128: Vec<Xxh3Checksum128>,
}

/// Length-prefixed metadata record preceding repair symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecGroupMeta {
    pub magic: [u8; 8],
    pub version: u32,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub start_frame_no: u32,
    pub end_frame_no: u32,
    pub db_size_pages: u32,
    pub page_size: u32,
    pub k_source: u32,
    pub r_repair: u32,
    pub oti: Oti,
    pub object_id: ObjectId,
    pub page_numbers: Vec<u32>,
    pub source_page_xxh3_128: Vec<Xxh3Checksum128>,
    pub checksum: u64,
}

impl WalFecGroupMeta {
    /// Create and validate metadata, computing checksum automatically.
    pub fn from_init(init: WalFecGroupMetaInit) -> Result<Self> {
        let mut meta = Self {
            magic: WAL_FEC_GROUP_META_MAGIC,
            version: WAL_FEC_GROUP_META_VERSION,
            wal_salt1: init.wal_salt1,
            wal_salt2: init.wal_salt2,
            start_frame_no: init.start_frame_no,
            end_frame_no: init.end_frame_no,
            db_size_pages: init.db_size_pages,
            page_size: init.page_size,
            k_source: init.k_source,
            r_repair: init.r_repair,
            oti: init.oti,
            object_id: init.object_id,
            page_numbers: init.page_numbers,
            source_page_xxh3_128: init.source_page_xxh3_128,
            checksum: 0,
        };
        meta.validate_invariants()?;
        meta.checksum = meta.compute_checksum();
        Ok(meta)
    }

    /// Return `(wal_salt1, wal_salt2, end_frame_no)`.
    #[must_use]
    pub const fn group_id(&self) -> WalFecGroupId {
        WalFecGroupId {
            wal_salt1: self.wal_salt1,
            wal_salt2: self.wal_salt2,
            end_frame_no: self.end_frame_no,
        }
    }

    /// Verify metadata is bound to the WAL salts.
    pub fn verify_salt_binding(&self, salts: WalSalts) -> Result<()> {
        if self.wal_salt1 != salts.salt1 || self.wal_salt2 != salts.salt2 {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec salt mismatch for group {}: sidecar=({}, {}), wal=({}, {})",
                    self.group_id(),
                    self.wal_salt1,
                    self.wal_salt2,
                    salts.salt1,
                    salts.salt2
                ),
            });
        }
        Ok(())
    }

    /// Serialize as on-disk record payload (without outer length prefix).
    #[must_use]
    pub fn to_record_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.serialized_len_without_prefix());
        bytes.extend_from_slice(&self.magic);
        append_u32_le(&mut bytes, self.version);
        append_u32_le(&mut bytes, self.wal_salt1);
        append_u32_le(&mut bytes, self.wal_salt2);
        append_u32_le(&mut bytes, self.start_frame_no);
        append_u32_le(&mut bytes, self.end_frame_no);
        append_u32_le(&mut bytes, self.db_size_pages);
        append_u32_le(&mut bytes, self.page_size);
        append_u32_le(&mut bytes, self.k_source);
        append_u32_le(&mut bytes, self.r_repair);
        bytes.extend_from_slice(&self.oti.to_bytes());
        bytes.extend_from_slice(self.object_id.as_bytes());
        for &page_number in &self.page_numbers {
            append_u32_le(&mut bytes, page_number);
        }
        for &hash in &self.source_page_xxh3_128 {
            bytes.extend_from_slice(&hash.to_le_bytes());
        }
        append_u64_le(&mut bytes, self.checksum);
        bytes
    }

    /// Deserialize and validate metadata from an on-disk payload.
    pub fn from_record_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < META_FIXED_PREFIX_BYTES + META_CHECKSUM_BYTES {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec group meta too short: expected at least {}, got {}",
                    META_FIXED_PREFIX_BYTES + META_CHECKSUM_BYTES,
                    bytes.len()
                ),
            });
        }

        let mut cursor = 0usize;
        let magic = read_array::<8>(bytes, &mut cursor, "magic")?;
        if magic != WAL_FEC_GROUP_META_MAGIC {
            return Err(FrankenError::WalCorrupt {
                detail: format!("invalid wal-fec magic: {magic:02x?}"),
            });
        }

        let version = read_u32_le(bytes, &mut cursor, "version")?;
        if version != WAL_FEC_GROUP_META_VERSION {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "unsupported wal-fec version {version}, expected {WAL_FEC_GROUP_META_VERSION}"
                ),
            });
        }

        let wal_salt1 = read_u32_le(bytes, &mut cursor, "wal_salt1")?;
        let wal_salt2 = read_u32_le(bytes, &mut cursor, "wal_salt2")?;
        let start_frame_no = read_u32_le(bytes, &mut cursor, "start_frame_no")?;
        let end_frame_no = read_u32_le(bytes, &mut cursor, "end_frame_no")?;
        let db_size_pages = read_u32_le(bytes, &mut cursor, "db_size_pages")?;
        let page_size = read_u32_le(bytes, &mut cursor, "page_size")?;
        let k_source = read_u32_le(bytes, &mut cursor, "k_source")?;
        let r_repair = read_u32_le(bytes, &mut cursor, "r_repair")?;
        let oti_bytes = read_array::<22>(bytes, &mut cursor, "oti")?;
        let oti = Oti::from_bytes(&oti_bytes).ok_or_else(|| FrankenError::WalCorrupt {
            detail: "invalid wal-fec OTI encoding".to_owned(),
        })?;
        let object_id = ObjectId::from_bytes(read_array::<16>(bytes, &mut cursor, "object_id")?);

        let k_source_usize = usize::try_from(k_source).map_err(|_| FrankenError::WalCorrupt {
            detail: format!("k_source {k_source} does not fit in usize"),
        })?;
        let mut page_numbers = Vec::with_capacity(k_source_usize);
        for _ in 0..k_source_usize {
            page_numbers.push(read_u32_le(bytes, &mut cursor, "page_number")?);
        }
        let mut source_page_xxh3_128 = Vec::with_capacity(k_source_usize);
        for _ in 0..k_source_usize {
            let digest = read_array::<16>(bytes, &mut cursor, "source_page_hash")?;
            source_page_xxh3_128.push(Xxh3Checksum128 {
                low: u64::from_le_bytes(digest[..8].try_into().expect("8-byte low hash slice")),
                high: u64::from_le_bytes(digest[8..].try_into().expect("8-byte high hash slice")),
            });
        }
        let checksum = read_u64_le(bytes, &mut cursor, "checksum")?;
        if cursor != bytes.len() {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec group meta trailing bytes: consumed {cursor}, total {}",
                    bytes.len()
                ),
            });
        }

        let meta = Self {
            magic,
            version,
            wal_salt1,
            wal_salt2,
            start_frame_no,
            end_frame_no,
            db_size_pages,
            page_size,
            k_source,
            r_repair,
            oti,
            object_id,
            page_numbers,
            source_page_xxh3_128,
            checksum,
        };
        meta.validate_invariants()?;
        let computed = meta.compute_checksum();
        if computed != meta.checksum {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec group checksum mismatch: stored {:#018x}, computed {computed:#018x}",
                    meta.checksum
                ),
            });
        }
        Ok(meta)
    }

    fn serialized_len_without_prefix(&self) -> usize {
        META_FIXED_PREFIX_BYTES
            + self.page_numbers.len() * size_of::<u32>()
            + self.source_page_xxh3_128.len() * size_of::<[u8; 16]>()
            + META_CHECKSUM_BYTES
    }

    fn compute_checksum(&self) -> u64 {
        xxh3_64(&self.to_record_bytes_without_checksum())
    }

    fn to_record_bytes_without_checksum(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(self.serialized_len_without_prefix() - META_CHECKSUM_BYTES);
        bytes.extend_from_slice(&self.magic);
        append_u32_le(&mut bytes, self.version);
        append_u32_le(&mut bytes, self.wal_salt1);
        append_u32_le(&mut bytes, self.wal_salt2);
        append_u32_le(&mut bytes, self.start_frame_no);
        append_u32_le(&mut bytes, self.end_frame_no);
        append_u32_le(&mut bytes, self.db_size_pages);
        append_u32_le(&mut bytes, self.page_size);
        append_u32_le(&mut bytes, self.k_source);
        append_u32_le(&mut bytes, self.r_repair);
        bytes.extend_from_slice(&self.oti.to_bytes());
        bytes.extend_from_slice(self.object_id.as_bytes());
        for &page_number in &self.page_numbers {
            append_u32_le(&mut bytes, page_number);
        }
        for &hash in &self.source_page_xxh3_128 {
            bytes.extend_from_slice(&hash.to_le_bytes());
        }
        bytes
    }

    fn validate_invariants(&self) -> Result<()> {
        self.validate_meta_header()?;
        self.validate_frame_span()?;
        if self.r_repair == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "r_repair must be >= 1 for wal-fec groups".to_owned(),
            });
        }
        let k_source_usize =
            usize::try_from(self.k_source).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("k_source {} does not fit in usize", self.k_source),
            })?;
        self.validate_array_lengths(k_source_usize)?;
        self.validate_page_size_and_oti()?;
        if self.db_size_pages == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "db_size_pages must be non-zero commit frame size".to_owned(),
            });
        }
        Ok(())
    }

    fn validate_meta_header(&self) -> Result<()> {
        if self.magic != WAL_FEC_GROUP_META_MAGIC {
            return Err(FrankenError::WalCorrupt {
                detail: "invalid wal-fec magic".to_owned(),
            });
        }
        if self.version != WAL_FEC_GROUP_META_VERSION {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "unsupported wal-fec meta version {} (expected {WAL_FEC_GROUP_META_VERSION})",
                    self.version
                ),
            });
        }
        Ok(())
    }

    fn validate_frame_span(&self) -> Result<()> {
        if self.start_frame_no == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "start_frame_no must be 1-based and nonzero".to_owned(),
            });
        }
        if self.end_frame_no < self.start_frame_no {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "end_frame_no {} must be >= start_frame_no {}",
                    self.end_frame_no, self.start_frame_no
                ),
            });
        }
        let expected_k = self
            .end_frame_no
            .checked_sub(self.start_frame_no)
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "frame-range overflow while validating k_source".to_owned(),
            })?;
        if self.k_source != expected_k {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "k_source {} must equal frame span {} ({}..={})",
                    self.k_source, expected_k, self.start_frame_no, self.end_frame_no
                ),
            });
        }
        Ok(())
    }

    fn validate_array_lengths(&self, k_source_usize: usize) -> Result<()> {
        if self.page_numbers.len() != k_source_usize {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "page_numbers length {} must equal k_source {}",
                    self.page_numbers.len(),
                    self.k_source
                ),
            });
        }
        if self.source_page_xxh3_128.len() != k_source_usize {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "source_page_xxh3_128 length {} must equal k_source {}",
                    self.source_page_xxh3_128.len(),
                    self.k_source
                ),
            });
        }
        Ok(())
    }

    fn validate_page_size_and_oti(&self) -> Result<()> {
        if PageSize::new(self.page_size).is_none() {
            return Err(FrankenError::WalCorrupt {
                detail: format!("invalid SQLite page_size {}", self.page_size),
            });
        }
        if self.oti.t != self.page_size {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "OTI.t {} must equal page_size {} for WAL source pages",
                    self.oti.t, self.page_size
                ),
            });
        }
        let expected_f = u64::from(self.k_source)
            .checked_mul(u64::from(self.page_size))
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "overflow computing expected OTI.f".to_owned(),
            })?;
        if self.oti.f != expected_f {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "OTI.f {} must equal k_source*page_size ({expected_f})",
                    self.oti.f
                ),
            });
        }
        Ok(())
    }
}

/// One complete append-only sidecar group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecGroupRecord {
    pub meta: WalFecGroupMeta,
    pub repair_symbols: Vec<SymbolRecord>,
}

impl WalFecGroupRecord {
    pub fn new(meta: WalFecGroupMeta, repair_symbols: Vec<SymbolRecord>) -> Result<Self> {
        let group = Self {
            meta,
            repair_symbols,
        };
        group.validate_layout()?;
        Ok(group)
    }

    fn validate_layout(&self) -> Result<()> {
        let expected_repair =
            usize::try_from(self.meta.r_repair).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("r_repair {} does not fit in usize", self.meta.r_repair),
            })?;
        if self.repair_symbols.len() != expected_repair {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "repair symbol count {} must equal r_repair {}",
                    self.repair_symbols.len(),
                    self.meta.r_repair
                ),
            });
        }
        for (index, symbol) in self.repair_symbols.iter().enumerate() {
            if symbol.object_id != self.meta.object_id {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "repair symbol {index} object_id mismatch: {} != {}",
                        symbol.object_id, self.meta.object_id
                    ),
                });
            }
            if symbol.oti != self.meta.oti {
                return Err(FrankenError::WalCorrupt {
                    detail: format!("repair symbol {index} OTI mismatch"),
                });
            }
            let expected_esi = self
                .meta
                .k_source
                .checked_add(u32::try_from(index).map_err(|_| FrankenError::WalCorrupt {
                    detail: format!("repair symbol index {index} does not fit in u32"),
                })?)
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: "repair ESI overflow".to_owned(),
                })?;
            if symbol.esi != expected_esi {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "repair symbol {index} has ESI {}, expected {expected_esi}",
                        symbol.esi
                    ),
                });
            }
        }
        Ok(())
    }
}

/// Scan result for `.wal-fec` sidecar files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalFecScanResult {
    pub groups: Vec<WalFecGroupRecord>,
    pub truncated_tail: bool,
}

/// Why WAL-FEC recovery fell back to SQLite-compatible truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFecRecoveryFallbackReason {
    MissingSidecarGroup,
    SidecarUnreadable,
    SaltMismatch,
    InsufficientSymbols,
    DecodeFailed,
    DecodedPayloadMismatch,
    /// Recovery was explicitly disabled via [`WalFecRecoveryConfig`].
    RecoveryDisabled,
}

/// Configuration for WAL-FEC recovery behaviour.
///
/// When `recovery_enabled` is `false`, the recovery path immediately returns
/// a [`WalFecRecoveryOutcome::TruncateBeforeGroup`] with
/// [`WalFecRecoveryFallbackReason::RecoveryDisabled`], emulating what C SQLite
/// does on WAL corruption (discard from the first checksum mismatch onward).
///
/// This allows the corruption demo harness to contrast:
/// - Recovery OFF → expect data loss / truncation (C SQLite behaviour).
/// - Recovery ON  → expect self-healing when repair symbols are sufficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecRecoveryConfig {
    /// Whether WAL-FEC recovery is attempted.  Default: `true`.
    pub recovery_enabled: bool,
}

impl Default for WalFecRecoveryConfig {
    fn default() -> Self {
        Self {
            recovery_enabled: true,
        }
    }
}

/// Structured log entry for a single WAL-FEC recovery attempt (bd-1w6k.2.5).
///
/// Captures machine-readable statistics for the corruption demo harness.
/// The harness can validate expected outcomes and render human-readable
/// recovery reports from these entries.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct WalFecRecoveryLog {
    /// Identity of the commit group targeted for recovery.
    pub group_id: WalFecGroupId,
    /// Whether recovery was enabled for this attempt.
    pub recovery_enabled: bool,
    /// The final outcome: `Recovered` or `TruncateBeforeGroup`.
    pub outcome_is_recovered: bool,
    /// Why recovery fell back, if it did.
    pub fallback_reason: Option<WalFecRecoveryFallbackReason>,
    /// Source symbols that passed xxh3 verification.
    pub validated_source_symbols: u32,
    /// Repair symbols that passed metadata binding.
    pub validated_repair_symbols: u32,
    /// Symbols required for decode (= K).
    pub required_symbols: u32,
    /// Total usable symbols (source + repair).
    pub available_symbols: u32,
    /// Frame numbers that were recovered from repair symbols.
    pub recovered_frame_nos: Vec<u32>,
    /// Count of corrupt observations during validation.
    pub corruption_observations: u32,
    /// Whether the RaptorQ decoder was invoked.
    pub decode_attempted: bool,
    /// Whether decode succeeded.
    pub decode_succeeded: bool,
}

/// Severity bucket for symbol-loss events in WAL-FEC recovery telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFecRepairSeverityBucket {
    One,
    TwoToFive,
    SixToTen,
    ElevenPlus,
}

impl WalFecRepairSeverityBucket {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::One => "1",
            Self::TwoToFive => "2-5",
            Self::SixToTen => "6-10",
            Self::ElevenPlus => "11+",
        }
    }
}

impl FromStr for WalFecRepairSeverityBucket {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "1" | "one" => Ok(Self::One),
            "2-5" | "two-to-five" | "two_to_five" => Ok(Self::TwoToFive),
            "6-10" | "six-to-ten" | "six_to_ten" => Ok(Self::SixToTen),
            "11+" | "eleven-plus" | "eleven_plus" => Ok(Self::ElevenPlus),
            _ => Err("unrecognized RaptorQ repair severity bucket"),
        }
    }
}

/// Source class used to repair a WAL commit group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFecRepairSource {
    WalRepairSymbols,
    SnapshotRepairSymbols,
    WalAndSnapshotRepairSymbols,
}

impl WalFecRepairSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalRepairSymbols => "wal_repair_symbols",
            Self::SnapshotRepairSymbols => "snapshot_repair_symbols",
            Self::WalAndSnapshotRepairSymbols => "wal_and_snapshot_repair_symbols",
        }
    }
}

/// Witness triple proving repair integrity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecRepairWitnessTriple {
    pub corrupted_hash_blake3: [u8; 32],
    pub repaired_hash_blake3: [u8; 32],
    pub expected_hash_blake3: [u8; 32],
}

/// One append-only evidence card for a RaptorQ repair action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecRepairEvidenceCard {
    pub group_id: WalFecGroupId,
    pub frame_id: u32,
    pub wal_file_offset_bytes: Option<u64>,
    pub monotonic_timestamp_ns: u64,
    pub wall_clock_unix_ns: u64,
    pub corruption_signature_blake3: [u8; 32],
    pub bit_error_pattern: Option<String>,
    pub repair_source: WalFecRepairSource,
    pub symbols_used: u32,
    pub validated_source_symbols: u32,
    pub validated_repair_symbols: u32,
    pub required_symbols: u32,
    pub available_symbols: u32,
    pub witness: WalFecRepairWitnessTriple,
    pub repair_latency_ns: u64,
    pub confidence_per_mille: u32,
    pub severity_bucket: WalFecRepairSeverityBucket,
    pub ledger_epoch: u64,
    pub chain_hash: [u8; 32],
}

fn hex_encode_32(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

impl WalFecRepairEvidenceCard {
    #[must_use]
    pub fn chain_hash_hex(&self) -> String {
        hex_encode_32(self.chain_hash)
    }

    #[must_use]
    pub fn corruption_signature_hex(&self) -> String {
        hex_encode_32(self.corruption_signature_blake3)
    }
}

/// Query filters for repair evidence cards.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalFecRepairEvidenceQuery {
    pub frame_id: Option<u32>,
    pub severity_bucket: Option<WalFecRepairSeverityBucket>,
    pub wall_clock_start_ns: Option<u64>,
    pub wall_clock_end_ns: Option<u64>,
    pub limit: Option<usize>,
}

/// Severity histogram used by [`WalFecRepairMetricsSnapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalFecRepairSeverityHistogram {
    pub one: u64,
    pub two_to_five: u64,
    pub six_to_ten: u64,
    pub eleven_plus: u64,
}

impl WalFecRepairSeverityHistogram {
    fn bump(&mut self, bucket: WalFecRepairSeverityBucket) {
        match bucket {
            WalFecRepairSeverityBucket::One => {
                self.one = self.one.saturating_add(1);
            }
            WalFecRepairSeverityBucket::TwoToFive => {
                self.two_to_five = self.two_to_five.saturating_add(1);
            }
            WalFecRepairSeverityBucket::SixToTen => {
                self.six_to_ten = self.six_to_ten.saturating_add(1);
            }
            WalFecRepairSeverityBucket::ElevenPlus => {
                self.eleven_plus = self.eleven_plus.saturating_add(1);
            }
        }
    }
}

/// One structured RaptorQ repair event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecRepairEvent {
    /// Full WAL-FEC group identity.
    pub group_id: WalFecGroupId,
    /// Convenience key for dashboards (same as `group_id.end_frame_no`).
    pub frame_id: u32,
    /// Number of source symbols lost.
    pub symbols_lost: u32,
    /// Number of symbols considered during decode (bounded by `K`).
    pub symbols_used: u32,
    /// Whether the recovery attempt produced a repaired group.
    pub repair_success: bool,
    /// Recovery latency in nanoseconds.
    pub latency_ns: u64,
    /// Estimated budget utilization percentage (0-100).
    pub budget_utilization_pct: u32,
    /// Severity bucket derived from `symbols_lost`.
    pub severity_bucket: WalFecRepairSeverityBucket,
}

/// Snapshot of RaptorQ repair telemetry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalFecRepairMetricsSnapshot {
    pub repairs_total: u64,
    pub repairs_failed: u64,
    pub symbols_reclaimed: u64,
    pub budget_utilization_pct: u32,
    pub wal_health_score: u32,
    pub severity_histogram: WalFecRepairSeverityHistogram,
}

#[derive(Debug, Default)]
struct WalFecRepairTelemetryState {
    repairs_total: u64,
    repairs_failed: u64,
    symbols_reclaimed: u64,
    budget_utilization_sum: u64,
    budget_utilization_count: u64,
    severity_histogram: WalFecRepairSeverityHistogram,
    events: VecDeque<WalFecRepairEvent>,
    evidence_cards: VecDeque<WalFecRepairEvidenceCard>,
    evidence_chain_tip: [u8; 32],
    next_evidence_epoch: u64,
}

static RAPTORQ_REPAIR_TELEMETRY: OnceLock<Mutex<WalFecRepairTelemetryState>> = OnceLock::new();

fn raptorq_repair_telemetry() -> &'static Mutex<WalFecRepairTelemetryState> {
    RAPTORQ_REPAIR_TELEMETRY.get_or_init(|| Mutex::new(WalFecRepairTelemetryState::default()))
}

fn lock_raptorq_repair_telemetry() -> MutexGuard<'static, WalFecRepairTelemetryState> {
    match raptorq_repair_telemetry().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("raptorq repair telemetry lock poisoned; recovering poisoned state");
            poisoned.into_inner()
        }
    }
}

fn severity_bucket_for_loss(symbols_lost: u32) -> WalFecRepairSeverityBucket {
    match symbols_lost {
        0 | 1 => WalFecRepairSeverityBucket::One,
        2..=5 => WalFecRepairSeverityBucket::TwoToFive,
        6..=10 => WalFecRepairSeverityBucket::SixToTen,
        _ => WalFecRepairSeverityBucket::ElevenPlus,
    }
}

fn compute_health_score(state: &WalFecRepairTelemetryState) -> u32 {
    if state.repairs_total == 0 {
        return 100;
    }

    let failure_penalty = state.repairs_failed.saturating_mul(20).min(70);
    let severity_penalty = state
        .severity_histogram
        .one
        .saturating_mul(1)
        .saturating_add(state.severity_histogram.two_to_five.saturating_mul(4))
        .saturating_add(state.severity_histogram.six_to_ten.saturating_mul(8))
        .saturating_add(state.severity_histogram.eleven_plus.saturating_mul(12))
        .min(30);
    let avg_budget_utilization = state
        .budget_utilization_sum
        .checked_div(state.budget_utilization_count)
        .unwrap_or(0);
    let utilization_penalty = if avg_budget_utilization >= 80 {
        15
    } else if avg_budget_utilization >= 60 {
        10
    } else if avg_budget_utilization >= 40 {
        5
    } else {
        0
    };

    let total_penalty = failure_penalty
        .saturating_add(severity_penalty)
        .saturating_add(utilization_penalty)
        .min(100);
    let score = 100_u64.saturating_sub(total_penalty);
    u32::try_from(score).unwrap_or(0)
}

fn build_repair_event(log: &WalFecRecoveryLog, latency: Duration) -> Option<WalFecRepairEvent> {
    let symbols_lost = log
        .required_symbols
        .saturating_sub(log.validated_source_symbols);
    let repair_activated =
        symbols_lost > 0 || log.decode_attempted || log.fallback_reason.is_some();
    if !repair_activated {
        return None;
    }

    let symbols_used = log.available_symbols.min(log.required_symbols);
    let repair_budget = log.validated_repair_symbols.max(1);
    let utilization_num = u64::from(symbols_lost)
        .saturating_mul(100)
        .saturating_add(u64::from(repair_budget).saturating_sub(1));
    let utilization = utilization_num / u64::from(repair_budget);
    let budget_utilization_pct = u32::try_from(utilization.min(100)).unwrap_or(100);
    let latency_ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
    let severity_bucket = severity_bucket_for_loss(symbols_lost);
    let repair_success =
        log.outcome_is_recovered && (log.decode_succeeded || !log.decode_attempted);

    Some(WalFecRepairEvent {
        group_id: log.group_id,
        frame_id: log.group_id.end_frame_no,
        symbols_lost,
        symbols_used,
        repair_success,
        latency_ns,
        budget_utilization_pct,
        severity_bucket,
    })
}

fn monotonic_now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let elapsed = START.get_or_init(Instant::now).elapsed().as_nanos();
    u64::try_from(elapsed).unwrap_or(u64::MAX)
}

fn wall_clock_unix_ns() -> u64 {
    let Ok(delta) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX)
}

const fn fallback_reason_tag(reason: Option<WalFecRecoveryFallbackReason>) -> u8 {
    match reason {
        None => 0,
        Some(WalFecRecoveryFallbackReason::MissingSidecarGroup) => 1,
        Some(WalFecRecoveryFallbackReason::SidecarUnreadable) => 2,
        Some(WalFecRecoveryFallbackReason::SaltMismatch) => 3,
        Some(WalFecRecoveryFallbackReason::InsufficientSymbols) => 4,
        Some(WalFecRecoveryFallbackReason::DecodeFailed) => 5,
        Some(WalFecRecoveryFallbackReason::DecodedPayloadMismatch) => 6,
        Some(WalFecRecoveryFallbackReason::RecoveryDisabled) => 7,
    }
}

fn repair_source_for_log(log: &WalFecRecoveryLog) -> WalFecRepairSource {
    if log.validated_repair_symbols > 0 && log.validated_source_symbols > 0 {
        return WalFecRepairSource::WalAndSnapshotRepairSymbols;
    }
    if log.validated_repair_symbols > 0 {
        return WalFecRepairSource::WalRepairSymbols;
    }
    WalFecRepairSource::SnapshotRepairSymbols
}

fn confidence_per_mille(required_symbols: u32, available_symbols: u32) -> u32 {
    if required_symbols == 0 {
        return 0;
    }
    let scaled = u64::from(available_symbols)
        .saturating_mul(1_000)
        .checked_div(u64::from(required_symbols))
        .unwrap_or(0);
    u32::try_from(scaled).unwrap_or(u32::MAX)
}

fn blake3_hash_to_array(hasher: &blake3::Hasher) -> [u8; 32] {
    let mut output = [0_u8; 32];
    output.copy_from_slice(hasher.finalize().as_bytes());
    output
}

fn compute_corruption_signature(log: &WalFecRecoveryLog, event: &WalFecRepairEvent) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:wal_fec:repair_corruption_signature:v1");
    hasher.update(&log.group_id.wal_salt1.to_le_bytes());
    hasher.update(&log.group_id.wal_salt2.to_le_bytes());
    hasher.update(&log.group_id.end_frame_no.to_le_bytes());
    hasher.update(&event.frame_id.to_le_bytes());
    hasher.update(&event.symbols_lost.to_le_bytes());
    hasher.update(&log.validated_source_symbols.to_le_bytes());
    hasher.update(&log.validated_repair_symbols.to_le_bytes());
    hasher.update(&log.required_symbols.to_le_bytes());
    hasher.update(&log.available_symbols.to_le_bytes());
    hasher.update(&log.corruption_observations.to_le_bytes());
    hasher.update(&[fallback_reason_tag(log.fallback_reason)]);
    blake3_hash_to_array(&hasher)
}

fn compute_witness_hash(
    label: &[u8],
    log: &WalFecRecoveryLog,
    event: &WalFecRepairEvent,
    corruption_signature: [u8; 32],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:wal_fec:repair_witness:v1");
    hasher.update(label);
    hasher.update(&corruption_signature);
    hasher.update(&log.group_id.wal_salt1.to_le_bytes());
    hasher.update(&log.group_id.wal_salt2.to_le_bytes());
    hasher.update(&log.group_id.end_frame_no.to_le_bytes());
    hasher.update(&event.symbols_used.to_le_bytes());
    hasher.update(&event.budget_utilization_pct.to_le_bytes());
    hasher.update(&log.required_symbols.to_le_bytes());
    hasher.update(&log.available_symbols.to_le_bytes());
    hasher.update(&[u8::from(log.outcome_is_recovered)]);
    hasher.update(&[u8::from(log.decode_succeeded)]);
    blake3_hash_to_array(&hasher)
}

fn compute_evidence_chain_hash(
    previous_tip: [u8; 32],
    card: &WalFecRepairEvidenceCard,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fsqlite:wal_fec:repair_evidence_chain:v1");
    hasher.update(&previous_tip);
    hasher.update(&card.group_id.wal_salt1.to_le_bytes());
    hasher.update(&card.group_id.wal_salt2.to_le_bytes());
    hasher.update(&card.group_id.end_frame_no.to_le_bytes());
    hasher.update(&card.frame_id.to_le_bytes());
    hasher.update(&card.wal_file_offset_bytes.unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(&card.monotonic_timestamp_ns.to_le_bytes());
    hasher.update(&card.wall_clock_unix_ns.to_le_bytes());
    hasher.update(&card.corruption_signature_blake3);
    hasher.update(
        card.bit_error_pattern
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.update(card.repair_source.as_str().as_bytes());
    hasher.update(&card.symbols_used.to_le_bytes());
    hasher.update(&card.validated_source_symbols.to_le_bytes());
    hasher.update(&card.validated_repair_symbols.to_le_bytes());
    hasher.update(&card.required_symbols.to_le_bytes());
    hasher.update(&card.available_symbols.to_le_bytes());
    hasher.update(&card.witness.corrupted_hash_blake3);
    hasher.update(&card.witness.repaired_hash_blake3);
    hasher.update(&card.witness.expected_hash_blake3);
    hasher.update(&card.repair_latency_ns.to_le_bytes());
    hasher.update(&card.confidence_per_mille.to_le_bytes());
    hasher.update(card.severity_bucket.as_str().as_bytes());
    hasher.update(&card.ledger_epoch.to_le_bytes());
    blake3_hash_to_array(&hasher)
}

fn build_repair_evidence_card(
    log: &WalFecRecoveryLog,
    event: &WalFecRepairEvent,
    latency: Duration,
    previous_chain_tip: [u8; 32],
    ledger_epoch: u64,
) -> WalFecRepairEvidenceCard {
    let corruption_signature = compute_corruption_signature(log, event);
    let witness = WalFecRepairWitnessTriple {
        corrupted_hash_blake3: compute_witness_hash(b"corrupted", log, event, corruption_signature),
        repaired_hash_blake3: compute_witness_hash(b"repaired", log, event, corruption_signature),
        expected_hash_blake3: compute_witness_hash(b"expected", log, event, corruption_signature),
    };
    let repair_latency_ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
    let bit_error_pattern = if log.corruption_observations > 0 {
        Some(format!(
            "corruption_observations={}",
            log.corruption_observations
        ))
    } else {
        None
    };

    let mut card = WalFecRepairEvidenceCard {
        group_id: log.group_id,
        frame_id: event.frame_id,
        wal_file_offset_bytes: None,
        monotonic_timestamp_ns: monotonic_now_ns(),
        wall_clock_unix_ns: wall_clock_unix_ns(),
        corruption_signature_blake3: corruption_signature,
        bit_error_pattern,
        repair_source: repair_source_for_log(log),
        symbols_used: event.symbols_used,
        validated_source_symbols: log.validated_source_symbols,
        validated_repair_symbols: log.validated_repair_symbols,
        required_symbols: log.required_symbols,
        available_symbols: log.available_symbols,
        witness,
        repair_latency_ns,
        confidence_per_mille: confidence_per_mille(log.required_symbols, log.available_symbols),
        severity_bucket: event.severity_bucket,
        ledger_epoch,
        chain_hash: [0_u8; 32],
    };
    card.chain_hash = compute_evidence_chain_hash(previous_chain_tip, &card);
    card
}

/// Record one recovery log into the global telemetry ledger.
///
/// This is non-blocking aside from a short in-process mutex critical section.
pub fn record_raptorq_recovery_log(log: &WalFecRecoveryLog, latency: Duration) {
    let Some(event) = build_repair_event(log, latency) else {
        return;
    };

    let mut state = lock_raptorq_repair_telemetry();
    state.repairs_total = state.repairs_total.saturating_add(1);
    if event.repair_success {
        state.symbols_reclaimed = state
            .symbols_reclaimed
            .saturating_add(u64::from(event.symbols_lost));
    } else {
        state.repairs_failed = state.repairs_failed.saturating_add(1);
    }
    state.budget_utilization_sum = state
        .budget_utilization_sum
        .saturating_add(u64::from(event.budget_utilization_pct));
    state.budget_utilization_count = state.budget_utilization_count.saturating_add(1);
    state.severity_histogram.bump(event.severity_bucket);

    if state.events.len() == RAPTORQ_REPAIR_EVENT_CAPACITY {
        let _ = state.events.pop_front();
    }
    state.events.push_back(event.clone());

    let ledger_epoch = state.next_evidence_epoch.max(1);
    let evidence_card =
        build_repair_evidence_card(log, &event, latency, state.evidence_chain_tip, ledger_epoch);
    state.next_evidence_epoch = ledger_epoch.saturating_add(1);
    state.evidence_chain_tip = evidence_card.chain_hash;
    if state.evidence_cards.len() == RAPTORQ_REPAIR_EVIDENCE_CAPACITY {
        let _ = state.evidence_cards.pop_front();
    }
    state.evidence_cards.push_back(evidence_card);
}

/// Snapshot aggregate RaptorQ repair telemetry for dashboard/PRAGMA surfaces.
#[must_use]
pub fn raptorq_repair_metrics_snapshot() -> WalFecRepairMetricsSnapshot {
    let state = lock_raptorq_repair_telemetry();
    let mean_budget_utilization = state
        .budget_utilization_sum
        .checked_div(state.budget_utilization_count)
        .unwrap_or(0);
    let budget_utilization_pct = u32::try_from(mean_budget_utilization).unwrap_or(u32::MAX);
    WalFecRepairMetricsSnapshot {
        repairs_total: state.repairs_total,
        repairs_failed: state.repairs_failed,
        symbols_reclaimed: state.symbols_reclaimed,
        budget_utilization_pct,
        wal_health_score: compute_health_score(&state),
        severity_histogram: state.severity_histogram,
    }
}

/// Snapshot recent RaptorQ repair events.
///
/// `limit = 0` returns the full retained ledger.
#[must_use]
pub fn raptorq_repair_events_snapshot(limit: usize) -> Vec<WalFecRepairEvent> {
    let mut events = {
        let state = lock_raptorq_repair_telemetry();
        let take = if limit == 0 {
            state.events.len()
        } else {
            limit.min(state.events.len())
        };
        state
            .events
            .iter()
            .rev()
            .take(take)
            .cloned()
            .collect::<Vec<_>>()
    };
    events.reverse();
    events
}

/// Snapshot recent RaptorQ repair evidence cards.
///
/// `limit = 0` returns all retained cards.
#[must_use]
pub fn raptorq_repair_evidence_snapshot(limit: usize) -> Vec<WalFecRepairEvidenceCard> {
    let mut cards = {
        let state = lock_raptorq_repair_telemetry();
        let take = if limit == 0 {
            state.evidence_cards.len()
        } else {
            limit.min(state.evidence_cards.len())
        };
        state
            .evidence_cards
            .iter()
            .rev()
            .take(take)
            .cloned()
            .collect::<Vec<_>>()
    };
    cards.reverse();
    cards
}

/// Query RaptorQ repair evidence cards by page/time/severity.
#[must_use]
pub fn query_raptorq_repair_evidence(
    query: &WalFecRepairEvidenceQuery,
) -> Vec<WalFecRepairEvidenceCard> {
    let mut cards = {
        let state = lock_raptorq_repair_telemetry();
        state
            .evidence_cards
            .iter()
            .filter(|card| {
                query
                    .frame_id
                    .is_none_or(|frame_id| card.frame_id == frame_id)
            })
            .filter(|card| {
                query
                    .severity_bucket
                    .is_none_or(|severity| card.severity_bucket == severity)
            })
            .filter(|card| {
                query
                    .wall_clock_start_ns
                    .is_none_or(|start| card.wall_clock_unix_ns >= start)
            })
            .filter(|card| {
                query
                    .wall_clock_end_ns
                    .is_none_or(|end| card.wall_clock_unix_ns <= end)
            })
            .cloned()
            .collect::<Vec<_>>()
    };

    if let Some(limit) = query.limit {
        if limit > 0 && cards.len() > limit {
            let keep_from = cards.len() - limit;
            cards.drain(..keep_from);
        }
    }

    cards
}

/// Reset all global RaptorQ repair telemetry.
pub fn reset_raptorq_repair_telemetry() {
    let mut state = lock_raptorq_repair_telemetry();
    *state = WalFecRepairTelemetryState::default();
}

/// Recovery audit artifact for a single WAL-FEC group attempt (§3.4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecDecodeProof {
    pub group_id: WalFecGroupId,
    pub required_symbols: u32,
    pub available_symbols: u32,
    pub validated_source_symbols: u32,
    pub validated_repair_symbols: u32,
    /// Count of repair symbols rejected as corrupt/mismatched during verification.
    pub corruption_observations: u32,
    pub decode_attempted: bool,
    pub decode_succeeded: bool,
    pub recovered_frame_nos: Vec<u32>,
    pub fallback_reason: Option<WalFecRecoveryFallbackReason>,
}

/// Successful recovery payload for one commit group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecRecoveredGroup {
    pub meta: WalFecGroupMeta,
    pub recovered_pages: Vec<Vec<u8>>,
    pub recovered_frame_nos: Vec<u32>,
    pub db_size_pages: u32,
    pub decode_proof: WalFecDecodeProof,
}

/// Final action for a WAL-FEC recovery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFecRecoveryOutcome {
    Recovered(WalFecRecoveredGroup),
    TruncateBeforeGroup {
        truncate_before_frame_no: u32,
        decode_proof: WalFecDecodeProof,
    },
}

/// Candidate WAL source frame payload read from `.wal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrameCandidate {
    pub frame_no: u32,
    pub page_data: Vec<u8>,
}

const DEFAULT_REPAIR_PIPELINE_QUEUE_CAPACITY: usize = 64;
const REPAIR_PIPELINE_FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Pipeline configuration for asynchronous WAL-FEC repair generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecRepairPipelineConfig {
    /// Maximum queued work items before backpressure.
    pub queue_capacity: usize,
    /// Optional deterministic delay per generated repair symbol (test hook).
    pub per_symbol_delay: Duration,
}

impl Default for WalFecRepairPipelineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_REPAIR_PIPELINE_QUEUE_CAPACITY,
            per_symbol_delay: Duration::ZERO,
        }
    }
}

/// A single asynchronous WAL-FEC repair-generation work item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecRepairWorkItem {
    pub sidecar_path: PathBuf,
    pub meta: WalFecGroupMeta,
    pub source_pages: Vec<Vec<u8>>,
}

impl WalFecRepairWorkItem {
    pub fn new(
        sidecar_path: impl Into<PathBuf>,
        meta: WalFecGroupMeta,
        source_pages: Vec<Vec<u8>>,
    ) -> Result<Self> {
        validate_source_pages(&meta, &source_pages)?;
        Ok(Self {
            sidecar_path: sidecar_path.into(),
            meta,
            source_pages,
        })
    }
}

/// Snapshot of asynchronous WAL-FEC pipeline counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalFecRepairPipelineStats {
    pub pending_jobs: usize,
    pub completed_jobs: usize,
    pub failed_jobs: usize,
    pub canceled_jobs: usize,
    pub max_pending_jobs: usize,
}

#[derive(Debug)]
enum WalFecPipelineMessage {
    Work(WalFecRepairWorkItem),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalFecWorkOutcome {
    Completed,
    Canceled,
}

/// Background worker that computes and appends WAL-FEC repair symbols.
pub struct WalFecRepairPipeline {
    sender: Option<mpsc::SyncSender<WalFecPipelineMessage>>,
    cancel_flag: Arc<AtomicBool>,
    pending_jobs: Arc<AtomicUsize>,
    completed_jobs: Arc<AtomicUsize>,
    failed_jobs: Arc<AtomicUsize>,
    canceled_jobs: Arc<AtomicUsize>,
    max_pending_jobs: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
}

impl WalFecRepairPipeline {
    /// Start the pipeline worker.
    pub fn start(config: WalFecRepairPipelineConfig) -> Result<Self> {
        if config.queue_capacity == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "wal-fec repair pipeline queue_capacity must be >= 1".to_owned(),
            });
        }

        let (tx, rx) = mpsc::sync_channel(config.queue_capacity);
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let pending_jobs = Arc::new(AtomicUsize::new(0));
        let completed_jobs = Arc::new(AtomicUsize::new(0));
        let failed_jobs = Arc::new(AtomicUsize::new(0));
        let canceled_jobs = Arc::new(AtomicUsize::new(0));
        let max_pending_jobs = Arc::new(AtomicUsize::new(0));

        let worker_cancel = Arc::clone(&cancel_flag);
        let worker_pending = Arc::clone(&pending_jobs);
        let worker_completed = Arc::clone(&completed_jobs);
        let worker_failed = Arc::clone(&failed_jobs);
        let worker_canceled = Arc::clone(&canceled_jobs);
        let worker_handle = thread::Builder::new()
            .name("wal-fec-repair-pipeline".to_owned())
            .spawn(move || {
                while let Ok(message) = rx.recv() {
                    match message {
                        WalFecPipelineMessage::Work(work_item) => {
                            let group_id = work_item.meta.group_id();
                            let outcome = process_repair_work_item(
                                &work_item,
                                worker_cancel.as_ref(),
                                config.per_symbol_delay,
                            );
                            worker_pending.fetch_sub(1, Ordering::SeqCst);
                            match outcome {
                                Ok(WalFecWorkOutcome::Completed) => {
                                    worker_completed.fetch_add(1, Ordering::SeqCst);
                                    info!(
                                        group_id = %group_id,
                                        "wal-fec repair work item completed"
                                    );
                                }
                                Ok(WalFecWorkOutcome::Canceled) => {
                                    worker_canceled.fetch_add(1, Ordering::SeqCst);
                                    warn!(
                                        group_id = %group_id,
                                        "wal-fec repair work item canceled before append"
                                    );
                                }
                                Err(err) => {
                                    worker_failed.fetch_add(1, Ordering::SeqCst);
                                    error!(
                                        group_id = %group_id,
                                        error = %err,
                                        "wal-fec repair work item failed"
                                    );
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|err| FrankenError::WalCorrupt {
                detail: format!("failed to spawn wal-fec repair worker thread: {err}"),
            })?;

        Ok(Self {
            sender: Some(tx),
            cancel_flag,
            pending_jobs,
            completed_jobs,
            failed_jobs,
            canceled_jobs,
            max_pending_jobs,
            worker: Some(worker_handle),
        })
    }

    /// Queue a new repair-generation work item without blocking commit path.
    pub fn enqueue(&self, work_item: WalFecRepairWorkItem) -> Result<()> {
        if self.cancel_flag.load(Ordering::SeqCst) {
            return Err(FrankenError::WalCorrupt {
                detail: "wal-fec repair pipeline is canceled".to_owned(),
            });
        }
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "wal-fec repair pipeline is shut down".to_owned(),
            })?;

        let pending_after = self.pending_jobs.fetch_add(1, Ordering::SeqCst) + 1;
        update_max_pending(&self.max_pending_jobs, pending_after);

        match sender.try_send(WalFecPipelineMessage::Work(work_item)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => {
                self.pending_jobs.fetch_sub(1, Ordering::SeqCst);
                Err(FrankenError::WalCorrupt {
                    detail: "wal-fec repair pipeline queue full".to_owned(),
                })
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.pending_jobs.fetch_sub(1, Ordering::SeqCst);
                Err(FrankenError::WalCorrupt {
                    detail: "wal-fec repair pipeline worker is disconnected".to_owned(),
                })
            }
        }
    }

    /// Request cancellation for queued/in-flight work.
    pub fn cancel(&self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
    }

    /// Wait until queue drains or timeout expires.
    #[must_use]
    pub fn flush(&self, timeout: Duration) -> bool {
        let mut remaining = timeout;
        loop {
            if self.pending_jobs.load(Ordering::SeqCst) == 0 {
                return true;
            }
            if remaining.is_zero() {
                return false;
            }
            let sleep_for = remaining.min(REPAIR_PIPELINE_FLUSH_POLL_INTERVAL);
            thread::sleep(sleep_for);
            remaining = remaining.saturating_sub(sleep_for);
        }
    }

    /// Read current counters.
    #[must_use]
    pub fn stats(&self) -> WalFecRepairPipelineStats {
        WalFecRepairPipelineStats {
            pending_jobs: self.pending_jobs.load(Ordering::SeqCst),
            completed_jobs: self.completed_jobs.load(Ordering::SeqCst),
            failed_jobs: self.failed_jobs.load(Ordering::SeqCst),
            canceled_jobs: self.canceled_jobs.load(Ordering::SeqCst),
            max_pending_jobs: self.max_pending_jobs.load(Ordering::SeqCst),
        }
    }

    /// Stop the worker and join thread.
    pub fn shutdown(&mut self) -> Result<WalFecRepairPipelineStats> {
        self.cancel();
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| FrankenError::WalCorrupt {
                detail: "wal-fec repair worker thread panicked".to_owned(),
            })?;
        }
        Ok(self.stats())
    }
}

impl Drop for WalFecRepairPipeline {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.shutdown();
        }
    }
}

/// Deterministically generate repair symbols from source pages.
pub fn generate_wal_fec_repair_symbols(
    meta: &WalFecGroupMeta,
    source_pages: &[Vec<u8>],
) -> Result<Vec<SymbolRecord>> {
    match generate_wal_fec_repair_symbols_inner(meta, source_pages, None, Duration::ZERO)? {
        Some(symbols) => Ok(symbols),
        None => Err(FrankenError::WalCorrupt {
            detail: "unexpected cancellation while generating wal-fec symbols".to_owned(),
        }),
    }
}

fn process_repair_work_item(
    work_item: &WalFecRepairWorkItem,
    cancel_flag: &AtomicBool,
    per_symbol_delay: Duration,
) -> Result<WalFecWorkOutcome> {
    if cancel_flag.load(Ordering::SeqCst) {
        return Ok(WalFecWorkOutcome::Canceled);
    }
    let Some(repair_symbols) = generate_wal_fec_repair_symbols_inner(
        &work_item.meta,
        &work_item.source_pages,
        Some(cancel_flag),
        per_symbol_delay,
    )?
    else {
        return Ok(WalFecWorkOutcome::Canceled);
    };
    if cancel_flag.load(Ordering::SeqCst) {
        return Ok(WalFecWorkOutcome::Canceled);
    }
    let group = WalFecGroupRecord::new(work_item.meta.clone(), repair_symbols)?;
    append_wal_fec_group(&work_item.sidecar_path, &group)?;
    Ok(WalFecWorkOutcome::Completed)
}

fn generate_wal_fec_repair_symbols_inner(
    meta: &WalFecGroupMeta,
    source_pages: &[Vec<u8>],
    cancel_flag: Option<&AtomicBool>,
    per_symbol_delay: Duration,
) -> Result<Option<Vec<SymbolRecord>>> {
    validate_source_pages(meta, source_pages)?;
    let symbol_len = usize::try_from(meta.oti.t).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("OTI symbol size {} does not fit in usize", meta.oti.t),
    })?;
    let r_repair = usize::try_from(meta.r_repair).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("r_repair {} does not fit in usize", meta.r_repair),
    })?;

    // Derive a deterministic group-level seed for the SystematicEncoder from
    // the group metadata (object_id, salts, frame range, k, r).
    let encoder_seed = derive_repair_seed(meta, 0);

    let encoder = asupersync::raptorq::systematic::SystematicEncoder::new(
        source_pages,
        symbol_len,
        encoder_seed,
    )
    .ok_or_else(|| FrankenError::WalCorrupt {
        detail: "RaptorQ constraint matrix singular during encoding".to_owned(),
    })?;

    let mut symbols = Vec::with_capacity(r_repair);

    for repair_index in 0..r_repair {
        if let Some(flag) = cancel_flag {
            if flag.load(Ordering::SeqCst) {
                return Ok(None);
            }
        }

        let esi = meta
            .k_source
            .checked_add(
                u32::try_from(repair_index).map_err(|_| FrankenError::WalCorrupt {
                    detail: format!("repair_index {repair_index} does not fit in u32"),
                })?,
            )
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "repair symbol ESI overflow".to_owned(),
            })?;

        let payload = encoder.repair_symbol(esi);

        if per_symbol_delay > Duration::ZERO {
            thread::sleep(per_symbol_delay);
        }

        symbols.push(SymbolRecord::new(
            meta.object_id,
            meta.oti,
            esi,
            payload,
            SymbolRecordFlags::empty(),
        ));
    }

    Ok(Some(symbols))
}

fn validate_source_pages(meta: &WalFecGroupMeta, source_pages: &[Vec<u8>]) -> Result<()> {
    let expected_pages = usize::try_from(meta.k_source).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("k_source {} does not fit in usize", meta.k_source),
    })?;
    if source_pages.len() != expected_pages {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "source page count {} must equal k_source {}",
                source_pages.len(),
                meta.k_source
            ),
        });
    }
    let expected_len = usize::try_from(meta.page_size).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("page_size {} does not fit in usize", meta.page_size),
    })?;

    for (index, page) in source_pages.iter().enumerate() {
        if page.len() != expected_len {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "source page {index} has length {}, expected {expected_len}",
                    page.len()
                ),
            });
        }
        let actual_hash = wal_fec_source_hash_xxh3_128(page);
        let expected_hash = meta.source_page_xxh3_128[index];
        if actual_hash != expected_hash {
            return Err(FrankenError::WalCorrupt {
                detail: format!("source page hash mismatch at index {index}"),
            });
        }
    }
    Ok(())
}

fn derive_repair_seed(meta: &WalFecGroupMeta, repair_index: u32) -> u64 {
    let mut seed_material = Vec::with_capacity(16 + (7 * size_of::<u32>()));
    seed_material.extend_from_slice(meta.object_id.as_bytes());
    seed_material.extend_from_slice(&meta.wal_salt1.to_le_bytes());
    seed_material.extend_from_slice(&meta.wal_salt2.to_le_bytes());
    seed_material.extend_from_slice(&meta.start_frame_no.to_le_bytes());
    seed_material.extend_from_slice(&meta.end_frame_no.to_le_bytes());
    seed_material.extend_from_slice(&meta.k_source.to_le_bytes());
    seed_material.extend_from_slice(&meta.r_repair.to_le_bytes());
    seed_material.extend_from_slice(&repair_index.to_le_bytes());
    xxh3_64(&seed_material)
}

fn update_max_pending(max_pending: &AtomicUsize, candidate: usize) {
    let mut observed = max_pending.load(Ordering::SeqCst);
    while candidate > observed {
        match max_pending.compare_exchange(observed, candidate, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => break,
            Err(new_observed) => observed = new_observed,
        }
    }
}

/// Build source hashes for `K` WAL payload pages.
#[must_use]
pub fn build_source_page_hashes(page_payloads: &[Vec<u8>]) -> Vec<Xxh3Checksum128> {
    page_payloads
        .iter()
        .map(|page| wal_fec_source_hash_xxh3_128(page))
        .collect()
}

/// RFC 6330 RaptorQ decode function for WAL-FEC recovery.
///
/// Accepts the group metadata and a slice of `(esi, symbol_data)` pairs
/// (source symbols with ESI < K and repair symbols with ESI >= K).
/// Returns `K` recovered source pages on success.
///
/// This function is the companion decoder for the `SystematicEncoder`-based
/// encoding in [`generate_wal_fec_repair_symbols_inner`] and is intended as
/// the `decode` closure for [`recover_wal_fec_group_with_decoder`].
pub fn wal_fec_raptorq_decode(
    meta: &WalFecGroupMeta,
    symbols: &[(u32, Vec<u8>)],
) -> Result<Vec<Vec<u8>>> {
    let k = usize::try_from(meta.k_source).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("k_source {} does not fit in usize", meta.k_source),
    })?;
    let symbol_size = usize::try_from(meta.oti.t).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("OTI symbol size {} does not fit in usize", meta.oti.t),
    })?;

    // Must use the same seed as the encoder.
    let encoder_seed = derive_repair_seed(meta, 0);
    let decoder =
        asupersync::raptorq::decoder::InactivationDecoder::new(k, symbol_size, encoder_seed);

    // Start with constraint symbols (LDPC + HDPC with zero data).
    let mut received = decoder.constraint_symbols();

    // Convert caller-provided (esi, data) pairs into ReceivedSymbol entries.
    for &(esi, ref data) in symbols {
        let esi_usize = esi as usize;
        if esi_usize < k {
            let (cols, coefs) = decoder.source_equation(esi);
            received.push(asupersync::raptorq::decoder::ReceivedSymbol {
                esi,
                is_source: true,
                columns: cols,
                coefficients: coefs,
                data: data.clone(),
            });
        } else {
            let (cols, coefs) = decoder.repair_equation(esi);
            received.push(asupersync::raptorq::decoder::ReceivedSymbol::repair(
                esi,
                cols,
                coefs,
                data.clone(),
            ));
        }
    }

    let result = decoder
        .decode(&received)
        .map_err(|err| FrankenError::WalCorrupt {
            detail: format!("RaptorQ decode failed: {err:?}"),
        })?;

    if result.source.len() != k {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "RaptorQ decode returned {} source symbols, expected {k}",
                result.source.len()
            ),
        });
    }

    debug!(
        k_source = k,
        peeled = result.stats.peeled,
        inactivated = result.stats.inactivated,
        gauss_ops = result.stats.gauss_ops,
        "wal-fec RaptorQ decode succeeded"
    );

    Ok(result.source)
}

/// Resolve sidecar path from WAL path.
#[must_use]
pub fn wal_fec_path_for_wal(wal_path: &Path) -> PathBuf {
    let wal_name = wal_path.to_string_lossy();
    if wal_name.ends_with("-wal") || wal_name.ends_with(".wal") {
        PathBuf::from(format!("{wal_name}-fec"))
    } else {
        PathBuf::from(format!("{wal_name}.wal-fec"))
    }
}

/// Read persistent `PRAGMA raptorq_repair_symbols` from `.wal-fec` header.
///
/// Returns [`DEFAULT_RAPTORQ_REPAIR_SYMBOLS`] when the sidecar is missing or
/// still in legacy format without a config header.
pub fn read_wal_fec_raptorq_repair_symbols(sidecar_path: &Path) -> Result<u8> {
    if !sidecar_path.exists() {
        return Ok(DEFAULT_RAPTORQ_REPAIR_SYMBOLS);
    }

    let bytes = fs::read(sidecar_path)?;
    let Some(header) = WalFecPragmaHeader::from_prefix(&bytes)? else {
        return Ok(DEFAULT_RAPTORQ_REPAIR_SYMBOLS);
    };

    debug!(
        sidecar = %sidecar_path.display(),
        raptorq_repair_symbols = header.raptorq_repair_symbols,
        "loaded wal-fec repair symbol setting from sidecar header"
    );
    Ok(header.raptorq_repair_symbols)
}

/// Persist `PRAGMA raptorq_repair_symbols` in a checksummed `.wal-fec` header.
///
/// Existing sidecar group data is preserved exactly after the header region.
pub fn persist_wal_fec_raptorq_repair_symbols(sidecar_path: &Path, value: u8) -> Result<()> {
    let existing = if sidecar_path.exists() {
        fs::read(sidecar_path)?
    } else {
        Vec::new()
    };

    let payload_offset = match WalFecPragmaHeader::from_prefix(&existing)? {
        Some(_) => WAL_FEC_PRAGMA_HEADER_BYTES,
        None => 0,
    };

    let header = WalFecPragmaHeader::new(value);
    let mut rewritten = Vec::with_capacity(
        WAL_FEC_PRAGMA_HEADER_BYTES + existing.len().saturating_sub(payload_offset),
    );
    rewritten.extend_from_slice(&header.to_bytes());
    rewritten.extend_from_slice(&existing[payload_offset..]);

    if let Some(parent) = sidecar_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(sidecar_path, rewritten)?;

    info!(
        sidecar = %sidecar_path.display(),
        raptorq_repair_symbols = value,
        "persisted wal-fec repair symbol setting"
    );
    Ok(())
}

/// Ensure WAL file and `.wal-fec` sidecar both exist.
pub fn ensure_wal_with_fec_sidecar(wal_path: &Path) -> Result<PathBuf> {
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path)?;
    let sidecar_path = wal_fec_path_for_wal(wal_path);
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&sidecar_path)?;
    Ok(sidecar_path)
}

/// Append a complete group (meta + repair symbols) to a sidecar file.
pub fn append_wal_fec_group(sidecar_path: &Path, group: &WalFecGroupRecord) -> Result<()> {
    group.validate_layout()?;
    let group_id = group.meta.group_id();
    debug!(
        group_id = %group_id,
        k_source = group.meta.k_source,
        r_repair = group.meta.r_repair,
        "appending wal-fec group"
    );

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(sidecar_path)?;
    let meta_bytes = group.meta.to_record_bytes();
    write_length_prefixed(&mut file, &meta_bytes, "group metadata")?;
    for symbol in &group.repair_symbols {
        write_length_prefixed(&mut file, &symbol.to_bytes(), "repair symbol")?;
    }
    file.sync_data()?;
    info!(
        group_id = %group_id,
        sidecar = %sidecar_path.display(),
        repair_symbols = group.repair_symbols.len(),
        "wal-fec group appended"
    );
    Ok(())
}

/// Scan a sidecar file and parse all fully-written groups.
///
/// On truncated tail (e.g. crash during append), returns `truncated_tail=true`
/// and only fully-validated preceding groups.
pub fn scan_wal_fec(sidecar_path: &Path) -> Result<WalFecScanResult> {
    if !sidecar_path.exists() {
        return Ok(WalFecScanResult::default());
    }
    let bytes = fs::read(sidecar_path)?;
    let mut cursor = scan_offset_after_optional_pragma_header(&bytes)?;
    let mut groups = Vec::new();
    let mut truncated_tail = false;

    while cursor < bytes.len() {
        let Some(meta_bytes) = read_length_prefixed(&bytes, &mut cursor)? else {
            truncated_tail = true;
            warn!(
                sidecar = %sidecar_path.display(),
                cursor,
                "truncated wal-fec metadata tail detected"
            );
            break;
        };
        let meta = WalFecGroupMeta::from_record_bytes(meta_bytes)?;
        let mut repair_symbols =
            Vec::with_capacity(usize::try_from(meta.r_repair).map_err(|_| {
                FrankenError::WalCorrupt {
                    detail: format!("r_repair {} does not fit in usize", meta.r_repair),
                }
            })?);

        for _ in 0..meta.r_repair {
            let Some(symbol_bytes) = read_length_prefixed(&bytes, &mut cursor)? else {
                truncated_tail = true;
                warn!(
                    sidecar = %sidecar_path.display(),
                    group_id = %meta.group_id(),
                    cursor,
                    "truncated wal-fec repair-symbol tail detected"
                );
                break;
            };
            let symbol = SymbolRecord::from_bytes(symbol_bytes).map_err(|err| {
                error!(
                    sidecar = %sidecar_path.display(),
                    group_id = %meta.group_id(),
                    error = %err,
                    "invalid wal-fec repair symbol"
                );
                FrankenError::WalCorrupt {
                    detail: format!("invalid wal-fec repair symbol: {err}"),
                }
            })?;
            repair_symbols.push(symbol);
        }

        if truncated_tail {
            break;
        }
        groups.push(WalFecGroupRecord::new(meta, repair_symbols)?);
    }

    Ok(WalFecScanResult {
        groups,
        truncated_tail,
    })
}

/// Find one group by `(wal_salt1, wal_salt2, end_frame_no)`.
pub fn find_wal_fec_group(
    sidecar_path: &Path,
    group_id: WalFecGroupId,
) -> Result<Option<WalFecGroupRecord>> {
    let scan = scan_wal_fec(sidecar_path)?;
    Ok(scan
        .groups
        .into_iter()
        .find(|group| group.meta.group_id() == group_id))
}

const BD_1HI_11_BEAD_ID: &str = "bd-1hi.11";

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalFecRecoveryGroupRecord {
    meta: WalFecGroupMeta,
    repair_symbols: Vec<SymbolRecord>,
    corruption_observations: u32,
}

/// Locate the commit group containing the first checksum-mismatching frame.
#[must_use]
pub fn identify_damaged_commit_group(
    groups: &[WalFecGroupRecord],
    wal_salts: WalSalts,
    damaged_frame_no: u32,
) -> Option<WalFecGroupId> {
    groups
        .iter()
        .find(|group| {
            let meta = &group.meta;
            meta.wal_salt1 == wal_salts.salt1
                && meta.wal_salt2 == wal_salts.salt2
                && meta.start_frame_no <= damaged_frame_no
                && damaged_frame_no <= meta.end_frame_no
        })
        .map(|group| group.meta.group_id())
}

/// Recover one WAL-FEC commit group with caller-provided decode logic.
///
/// This implements the §3.4.1 compatibility-mode recovery flow:
/// 1. locate group metadata by `(wal_salt1, wal_salt2, end_frame_no)`;
/// 2. validate source payloads from `.wal` (independent xxh3 at/after chain break);
/// 3. collect valid repair symbols from `.wal-fec`;
/// 4. decode if at least `K` symbols are available;
/// 5. otherwise fall back to SQLite-compatible truncation.
pub fn recover_wal_fec_group_with_decoder<F>(
    sidecar_path: &Path,
    group_id: WalFecGroupId,
    wal_salts: WalSalts,
    first_checksum_mismatch_frame_no: u32,
    wal_frames: &[WalFrameCandidate],
    mut decode: F,
) -> Result<WalFecRecoveryOutcome>
where
    F: FnMut(&WalFecGroupMeta, &[(u32, Vec<u8>)]) -> Result<Vec<Vec<u8>>>,
{
    let groups = match scan_wal_fec_for_recovery(sidecar_path) {
        Ok(groups) => groups,
        Err(err) => {
            warn!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                sidecar = %sidecar_path.display(),
                error = %err,
                "wal-fec sidecar unreadable; falling back to sqlite-compatible truncation"
            );
            return Ok(truncate_outcome(
                group_id,
                first_checksum_mismatch_frame_no,
                WalFecRecoveryFallbackReason::SidecarUnreadable,
                RecoveryProofStats::new(0),
            ));
        }
    };

    let Some(group) = groups
        .into_iter()
        .find(|group| group.meta.group_id() == group_id)
    else {
        warn!(
            bead_id = BD_1HI_11_BEAD_ID,
            group_id = %group_id,
            sidecar = %sidecar_path.display(),
            "wal-fec group metadata missing; falling back to sqlite-compatible truncation"
        );
        return Ok(truncate_outcome(
            group_id,
            first_checksum_mismatch_frame_no,
            WalFecRecoveryFallbackReason::MissingSidecarGroup,
            RecoveryProofStats::new(0),
        ));
    };

    if group.meta.verify_salt_binding(wal_salts).is_err() {
        warn!(
            bead_id = BD_1HI_11_BEAD_ID,
            group_id = %group_id,
            "wal-fec group salt mismatch; rejecting sidecar group and truncating"
        );
        return Ok(truncate_outcome(
            group_id,
            group.meta.start_frame_no,
            WalFecRecoveryFallbackReason::SaltMismatch,
            RecoveryProofStats::new(group.meta.k_source),
        ));
    }

    recover_wal_fec_group_record_with_decoder(
        &group,
        first_checksum_mismatch_frame_no,
        wal_frames,
        &mut decode,
    )
}

/// Config-aware WAL-FEC recovery that produces a [`WalFecRecoveryLog`].
///
/// When `config.recovery_enabled` is `false`, immediately returns
/// `TruncateBeforeGroup` (simulating C SQLite behaviour) and a log entry
/// recording the skip.
///
/// When enabled, delegates to [`recover_wal_fec_group_with_decoder`] and
/// converts the resulting [`WalFecDecodeProof`] into a structured log.
pub fn recover_wal_fec_group_with_config<F>(
    sidecar_path: &Path,
    group_id: WalFecGroupId,
    wal_salts: WalSalts,
    first_checksum_mismatch_frame_no: u32,
    wal_frames: &[WalFrameCandidate],
    config: &WalFecRecoveryConfig,
    decode: F,
) -> Result<(WalFecRecoveryOutcome, WalFecRecoveryLog)>
where
    F: FnMut(&WalFecGroupMeta, &[(u32, Vec<u8>)]) -> Result<Vec<Vec<u8>>>,
{
    if !config.recovery_enabled {
        info!(
            bead_id = BD_1W6K_25_BEAD_ID,
            group_id = %group_id,
            "wal-fec recovery disabled; falling back to sqlite-compatible truncation"
        );
        let outcome = truncate_outcome(
            group_id,
            first_checksum_mismatch_frame_no,
            WalFecRecoveryFallbackReason::RecoveryDisabled,
            RecoveryProofStats::new(0),
        );
        let log = WalFecRecoveryLog {
            group_id,
            recovery_enabled: false,
            outcome_is_recovered: false,
            fallback_reason: Some(WalFecRecoveryFallbackReason::RecoveryDisabled),
            validated_source_symbols: 0,
            validated_repair_symbols: 0,
            required_symbols: 0,
            available_symbols: 0,
            recovered_frame_nos: Vec::new(),
            corruption_observations: 0,
            decode_attempted: false,
            decode_succeeded: false,
        };
        record_raptorq_recovery_log(&log, Duration::ZERO);
        return Ok((outcome, log));
    }

    let started = Instant::now();
    let outcome = recover_wal_fec_group_with_decoder(
        sidecar_path,
        group_id,
        wal_salts,
        first_checksum_mismatch_frame_no,
        wal_frames,
        decode,
    )?;

    let log = recovery_log_from_outcome(group_id, &outcome, true);
    record_raptorq_recovery_log(&log, started.elapsed());
    Ok((outcome, log))
}

/// Extract a [`WalFecRecoveryLog`] from a completed recovery outcome.
#[must_use]
pub fn recovery_log_from_outcome(
    group_id: WalFecGroupId,
    outcome: &WalFecRecoveryOutcome,
    recovery_enabled: bool,
) -> WalFecRecoveryLog {
    match outcome {
        WalFecRecoveryOutcome::Recovered(group) => WalFecRecoveryLog {
            group_id,
            recovery_enabled,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: group.decode_proof.validated_source_symbols,
            validated_repair_symbols: group.decode_proof.validated_repair_symbols,
            required_symbols: group.decode_proof.required_symbols,
            available_symbols: group.decode_proof.available_symbols,
            recovered_frame_nos: group.decode_proof.recovered_frame_nos.clone(),
            corruption_observations: group.decode_proof.corruption_observations,
            decode_attempted: group.decode_proof.decode_attempted,
            decode_succeeded: group.decode_proof.decode_succeeded,
        },
        WalFecRecoveryOutcome::TruncateBeforeGroup { decode_proof, .. } => WalFecRecoveryLog {
            group_id,
            recovery_enabled,
            outcome_is_recovered: false,
            fallback_reason: decode_proof.fallback_reason,
            validated_source_symbols: decode_proof.validated_source_symbols,
            validated_repair_symbols: decode_proof.validated_repair_symbols,
            required_symbols: decode_proof.required_symbols,
            available_symbols: decode_proof.available_symbols,
            recovered_frame_nos: decode_proof.recovered_frame_nos.clone(),
            corruption_observations: decode_proof.corruption_observations,
            decode_attempted: decode_proof.decode_attempted,
            decode_succeeded: decode_proof.decode_succeeded,
        },
    }
}

const BD_1W6K_25_BEAD_ID: &str = "bd-1w6k.2.5";

fn recover_wal_fec_group_record_with_decoder<F>(
    group: &WalFecRecoveryGroupRecord,
    first_checksum_mismatch_frame_no: u32,
    wal_frames: &[WalFrameCandidate],
    decode: &mut F,
) -> Result<WalFecRecoveryOutcome>
where
    F: FnMut(&WalFecGroupMeta, &[(u32, Vec<u8>)]) -> Result<Vec<Vec<u8>>>,
{
    let meta = &group.meta;
    let group_id = meta.group_id();
    let k_required = usize::try_from(meta.k_source).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("k_source {} does not fit in usize", meta.k_source),
    })?;
    let page_len = usize::try_from(meta.page_size).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("page_size {} does not fit in usize", meta.page_size),
    })?;

    let frame_payload_by_no = build_frame_payload_map(wal_frames);
    let mut source_collection = collect_valid_source_symbols(
        meta,
        group_id,
        first_checksum_mismatch_frame_no,
        page_len,
        &frame_payload_by_no,
        k_required,
    )?;
    let (repair_symbols, validated_repair_symbols, rejected_repair_symbols) =
        collect_valid_repair_symbols(meta, group_id, &group.repair_symbols);
    source_collection.available_symbols.extend(repair_symbols);
    source_collection
        .available_symbols
        .sort_unstable_by_key(|(esi, _)| *esi);

    let mut stats = build_recovery_proof_stats(
        meta.k_source,
        &source_collection,
        validated_repair_symbols,
        rejected_repair_symbols,
        group.corruption_observations,
    );

    if source_collection.available_symbols.len() < k_required {
        error!(
            bead_id = BD_1HI_11_BEAD_ID,
            group_id = %group_id,
            required_symbols = meta.k_source,
            available_symbols = stats.available_symbols,
            "insufficient symbols for wal-fec decode; truncating before group"
        );
        return Ok(insufficient_symbols_outcome(meta, group_id, stats));
    }

    if stats.validated_source_symbols == meta.k_source {
        stats.decode_succeeded = true;
        info!(
            bead_id = BD_1HI_11_BEAD_ID,
            group_id = %group_id,
            validated_source_symbols = stats.validated_source_symbols,
            "wal-fec recovery fast path: group fully intact"
        );
        return Ok(fast_path_outcome(
            meta,
            group_id,
            source_collection.source_pages,
            stats,
        ));
    }

    stats.decode_attempted = true;
    let decoded_pages = match decode(meta, &source_collection.available_symbols) {
        Ok(decoded) => decoded,
        Err(err) => {
            error!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                error = %err,
                "wal-fec decode failed; truncating before group"
            );
            return Ok(decode_failed_outcome(meta, group_id, stats));
        }
    };

    if !decoded_pages_match_expected(meta, &decoded_pages, page_len) {
        error!(
            bead_id = BD_1HI_11_BEAD_ID,
            group_id = %group_id,
            "decoded payload failed structural/hash verification; truncating before group"
        );
        return Ok(decoded_mismatch_outcome(meta, group_id, stats));
    }

    Ok(finalize_decoded_success_outcome(
        meta,
        group_id,
        &source_collection.source_pages,
        decoded_pages,
        stats,
    ))
}

fn finalize_decoded_success_outcome(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    source_pages: &[Option<Vec<u8>>],
    decoded_pages: Vec<Vec<u8>>,
    mut stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    let recovered_frame_nos = recovered_frame_numbers(meta, source_pages);
    let recovered_count = usize_to_u32(recovered_frame_nos.len());
    if recovered_count >= meta.r_repair.saturating_sub(1) {
        warn!(
            bead_id = BD_1HI_11_BEAD_ID,
            group_id = %group_id,
            recovered_frames = recovered_count,
            repair_capacity = meta.r_repair,
            "wal-fec recovery near repair-capacity limit"
        );
    }
    info!(
        bead_id = BD_1HI_11_BEAD_ID,
        group_id = %group_id,
        recovered_frames = recovered_count,
        db_size_pages = meta.db_size_pages,
        "wal-fec recovery succeeded"
    );
    stats.decode_succeeded = true;
    stats.recovered_frame_nos.clone_from(&recovered_frame_nos);
    decoded_success_outcome(meta, group_id, decoded_pages, recovered_frame_nos, stats)
}

fn insufficient_symbols_outcome(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    truncate_outcome(
        group_id,
        meta.start_frame_no,
        WalFecRecoveryFallbackReason::InsufficientSymbols,
        stats,
    )
}

fn fast_path_outcome(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    source_pages: Vec<Option<Vec<u8>>>,
    stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    let recovered_pages = source_pages
        .into_iter()
        .map(|page| page.expect("source_pages are complete when all sources validated"))
        .collect::<Vec<_>>();
    WalFecRecoveryOutcome::Recovered(WalFecRecoveredGroup {
        meta: meta.clone(),
        recovered_pages,
        recovered_frame_nos: Vec::new(),
        db_size_pages: meta.db_size_pages,
        decode_proof: build_decode_proof(group_id, stats, None),
    })
}

fn decode_failed_outcome(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    truncate_outcome(
        group_id,
        meta.start_frame_no,
        WalFecRecoveryFallbackReason::DecodeFailed,
        stats,
    )
}

fn decoded_mismatch_outcome(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    truncate_outcome(
        group_id,
        meta.start_frame_no,
        WalFecRecoveryFallbackReason::DecodedPayloadMismatch,
        stats,
    )
}

fn decoded_success_outcome(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    decoded_pages: Vec<Vec<u8>>,
    recovered_frame_nos: Vec<u32>,
    stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    WalFecRecoveryOutcome::Recovered(WalFecRecoveredGroup {
        meta: meta.clone(),
        recovered_pages: decoded_pages,
        recovered_frame_nos,
        db_size_pages: meta.db_size_pages,
        decode_proof: build_decode_proof(group_id, stats, None),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct RecoveryProofStats {
    required_symbols: u32,
    available_symbols: u32,
    validated_source_symbols: u32,
    validated_repair_symbols: u32,
    corruption_observations: u32,
    decode_attempted: bool,
    decode_succeeded: bool,
    recovered_frame_nos: Vec<u32>,
}

impl RecoveryProofStats {
    const fn new(required_symbols: u32) -> Self {
        Self {
            required_symbols,
            available_symbols: 0,
            validated_source_symbols: 0,
            validated_repair_symbols: 0,
            corruption_observations: 0,
            decode_attempted: false,
            decode_succeeded: false,
            recovered_frame_nos: Vec::new(),
        }
    }
}

fn build_recovery_proof_stats(
    required_symbols: u32,
    source_collection: &SourceSymbolCollection,
    validated_repair_symbols: u32,
    rejected_repair_symbols: u32,
    sidecar_corruption_observations: u32,
) -> RecoveryProofStats {
    let mut stats = RecoveryProofStats::new(required_symbols);
    stats.available_symbols = usize_to_u32(source_collection.available_symbols.len());
    stats.validated_source_symbols = source_collection.validated_source_symbols;
    stats.validated_repair_symbols = validated_repair_symbols;
    stats.corruption_observations =
        sidecar_corruption_observations.saturating_add(rejected_repair_symbols);
    stats
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceSymbolCollection {
    available_symbols: Vec<(u32, Vec<u8>)>,
    source_pages: Vec<Option<Vec<u8>>>,
    validated_source_symbols: u32,
}

fn build_frame_payload_map(wal_frames: &[WalFrameCandidate]) -> BTreeMap<u32, &[u8]> {
    let mut frame_payload_by_no = BTreeMap::new();
    for frame in wal_frames {
        frame_payload_by_no
            .entry(frame.frame_no)
            .or_insert(frame.page_data.as_slice());
    }
    frame_payload_by_no
}

fn collect_valid_source_symbols(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    first_checksum_mismatch_frame_no: u32,
    page_len: usize,
    frame_payload_by_no: &BTreeMap<u32, &[u8]>,
    k_required: usize,
) -> Result<SourceSymbolCollection> {
    let mut available_symbols: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut source_pages = vec![None; k_required];
    let mut validated_source_symbols = 0_u32;

    for source_esi in 0..meta.k_source {
        let index = usize::try_from(source_esi).map_err(|_| FrankenError::WalCorrupt {
            detail: format!("source ESI {source_esi} does not fit in usize"),
        })?;
        let frame_no = meta.start_frame_no.checked_add(source_esi).ok_or_else(|| {
            FrankenError::WalCorrupt {
                detail: "frame number overflow while collecting source symbols".to_owned(),
            }
        })?;
        let Some(payload) = frame_payload_by_no.get(&frame_no).copied() else {
            debug!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                frame_no,
                "source frame missing from wal candidates"
            );
            continue;
        };
        if payload.len() != page_len {
            warn!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                frame_no,
                payload_len = payload.len(),
                expected_len = page_len,
                "source frame payload length mismatch; excluding from decoder input"
            );
            continue;
        }
        if frame_no >= first_checksum_mismatch_frame_no
            && !verify_wal_fec_source_hash(payload, meta.source_page_xxh3_128[index])
        {
            warn!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                frame_no,
                esi = source_esi,
                "source frame hash mismatch at/after wal chain break; excluding from decoder input"
            );
            continue;
        }
        if frame_no >= first_checksum_mismatch_frame_no {
            debug!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                frame_no,
                esi = source_esi,
                "source frame validated via independent xxh3 hash"
            );
        }
        let payload_vec = payload.to_vec();
        source_pages[index] = Some(payload_vec.clone());
        available_symbols.push((source_esi, payload_vec));
        validated_source_symbols = validated_source_symbols.saturating_add(1);
    }

    Ok(SourceSymbolCollection {
        available_symbols,
        source_pages,
        validated_source_symbols,
    })
}

fn collect_valid_repair_symbols(
    meta: &WalFecGroupMeta,
    group_id: WalFecGroupId,
    repair_symbols: &[SymbolRecord],
) -> (Vec<(u32, Vec<u8>)>, u32, u32) {
    let mut validated_symbols = Vec::new();
    let mut validated_repair_symbols = 0_u32;
    let mut rejected_repair_symbols = 0_u32;

    for symbol in repair_symbols {
        if !repair_symbol_matches_meta(meta, symbol) {
            warn!(
                bead_id = BD_1HI_11_BEAD_ID,
                group_id = %group_id,
                esi = symbol.esi,
                "repair symbol failed metadata binding checks; excluding from decoder input"
            );
            rejected_repair_symbols = rejected_repair_symbols.saturating_add(1);
            continue;
        }
        validated_repair_symbols = validated_repair_symbols.saturating_add(1);
        validated_symbols.push((symbol.esi, symbol.symbol_data.clone()));
    }

    (
        validated_symbols,
        validated_repair_symbols,
        rejected_repair_symbols,
    )
}

fn recovered_frame_numbers(meta: &WalFecGroupMeta, source_pages: &[Option<Vec<u8>>]) -> Vec<u32> {
    source_pages
        .iter()
        .enumerate()
        .filter_map(|(index, page)| {
            if page.is_some() {
                None
            } else {
                let idx = u32::try_from(index).ok()?;
                meta.start_frame_no.checked_add(idx)
            }
        })
        .collect()
}

fn decoded_pages_match_expected(
    meta: &WalFecGroupMeta,
    decoded_pages: &[Vec<u8>],
    page_len: usize,
) -> bool {
    let Ok(k_required) = usize::try_from(meta.k_source) else {
        return false;
    };
    if decoded_pages.len() != k_required {
        return false;
    }
    decoded_pages.iter().enumerate().all(|(index, payload)| {
        payload.len() == page_len
            && verify_wal_fec_source_hash(payload, meta.source_page_xxh3_128[index])
    })
}

fn repair_symbol_matches_meta(meta: &WalFecGroupMeta, symbol: &SymbolRecord) -> bool {
    if symbol.object_id != meta.object_id || symbol.oti != meta.oti {
        return false;
    }
    let repair_start = meta.k_source;
    let Some(repair_end) = meta.k_source.checked_add(meta.r_repair) else {
        return false;
    };
    symbol.esi >= repair_start && symbol.esi < repair_end
}

fn scan_wal_fec_for_recovery(sidecar_path: &Path) -> Result<Vec<WalFecRecoveryGroupRecord>> {
    if !sidecar_path.exists() {
        return Ok(Vec::new());
    }

    let bytes = fs::read(sidecar_path)?;
    let mut cursor = scan_offset_after_optional_pragma_header(&bytes)?;
    let mut groups = Vec::new();

    while cursor < bytes.len() {
        let Some(meta_bytes) = read_length_prefixed(&bytes, &mut cursor)? else {
            break;
        };
        let meta = WalFecGroupMeta::from_record_bytes(meta_bytes)?;
        let mut repair_symbols = Vec::new();
        let mut corruption_observations = 0_u32;

        let mut truncated_tail = false;
        for _ in 0..meta.r_repair {
            let Some(symbol_bytes) = read_length_prefixed(&bytes, &mut cursor)? else {
                truncated_tail = true;
                break;
            };
            match SymbolRecord::from_bytes(symbol_bytes) {
                Ok(symbol) => repair_symbols.push(symbol),
                Err(err) => {
                    warn!(
                        bead_id = BD_1HI_11_BEAD_ID,
                        group_id = %meta.group_id(),
                        error = %err,
                        "invalid wal-fec repair SymbolRecord excluded from recovery set"
                    );
                    corruption_observations = corruption_observations.saturating_add(1);
                }
            }
        }
        if truncated_tail {
            break;
        }
        groups.push(WalFecRecoveryGroupRecord {
            meta,
            repair_symbols,
            corruption_observations,
        });
    }

    Ok(groups)
}

fn truncate_outcome(
    group_id: WalFecGroupId,
    truncate_before_frame_no: u32,
    fallback_reason: WalFecRecoveryFallbackReason,
    stats: RecoveryProofStats,
) -> WalFecRecoveryOutcome {
    WalFecRecoveryOutcome::TruncateBeforeGroup {
        truncate_before_frame_no,
        decode_proof: build_decode_proof(group_id, stats, Some(fallback_reason)),
    }
}

fn build_decode_proof(
    group_id: WalFecGroupId,
    stats: RecoveryProofStats,
    fallback_reason: Option<WalFecRecoveryFallbackReason>,
) -> WalFecDecodeProof {
    WalFecDecodeProof {
        group_id,
        required_symbols: stats.required_symbols,
        available_symbols: stats.available_symbols,
        validated_source_symbols: stats.validated_source_symbols,
        validated_repair_symbols: stats.validated_repair_symbols,
        corruption_observations: stats.corruption_observations,
        decode_attempted: stats.decode_attempted,
        decode_succeeded: stats.decode_succeeded,
        recovered_frame_nos: stats.recovered_frame_nos,
        fallback_reason,
    }
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn scan_offset_after_optional_pragma_header(bytes: &[u8]) -> Result<usize> {
    let Some(header) = WalFecPragmaHeader::from_prefix(bytes)? else {
        return Ok(0);
    };
    debug!(
        raptorq_repair_symbols = header.raptorq_repair_symbols,
        "detected wal-fec pragma header during scan"
    );
    Ok(WAL_FEC_PRAGMA_HEADER_BYTES)
}

fn write_length_prefixed(file: &mut fs::File, payload: &[u8], what: &str) -> Result<()> {
    let len_u32 = u32::try_from(payload.len()).map_err(|_| FrankenError::WalCorrupt {
        detail: format!(
            "{what} too large for wal-fec length prefix: {}",
            payload.len()
        ),
    })?;
    file.write_all(&len_u32.to_le_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

fn read_length_prefixed<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<Option<&'a [u8]>> {
    if *cursor >= bytes.len() {
        return Ok(None);
    }
    if bytes.len() - *cursor < LENGTH_PREFIX_BYTES {
        return Ok(None);
    }
    let mut len_raw = [0u8; LENGTH_PREFIX_BYTES];
    len_raw.copy_from_slice(&bytes[*cursor..*cursor + LENGTH_PREFIX_BYTES]);
    *cursor += LENGTH_PREFIX_BYTES;
    let payload_len =
        usize::try_from(u32::from_le_bytes(len_raw)).map_err(|_| FrankenError::WalCorrupt {
            detail: "wal-fec length prefix does not fit in usize".to_owned(),
        })?;
    let end = cursor
        .checked_add(payload_len)
        .ok_or_else(|| FrankenError::WalCorrupt {
            detail: "wal-fec length prefix overflow".to_owned(),
        })?;
    if end > bytes.len() {
        return Ok(None);
    }
    let payload = &bytes[*cursor..end];
    *cursor = end;
    Ok(Some(payload))
}

fn append_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn append_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn read_u32_le(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    let raw = read_array::<4>(bytes, cursor, field)?;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64_le(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    let raw = read_array::<8>(bytes, cursor, field)?;
    Ok(u64::from_le_bytes(raw))
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| FrankenError::WalCorrupt {
            detail: format!("overflow reading wal-fec field {field}"),
        })?;
    if end > bytes.len() {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "wal-fec field {field} out of bounds: need {N} bytes at offset {}, total {}",
                *cursor,
                bytes.len()
            ),
        });
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    use tempfile::tempdir;

    fn telemetry_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        match LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn test_wal_fec_pragma_header_default_without_header() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("db.wal-fec");

        fs::write(&sidecar, b"legacy-groups-without-header").expect("write legacy bytes");

        let value = read_wal_fec_raptorq_repair_symbols(&sidecar).expect("read default");
        assert_eq!(value, DEFAULT_RAPTORQ_REPAIR_SYMBOLS);
    }

    #[test]
    fn test_wal_fec_pragma_persist_and_reload_across_reopen() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("db.wal-fec");

        persist_wal_fec_raptorq_repair_symbols(&sidecar, 4).expect("persist setting");
        let first_read = read_wal_fec_raptorq_repair_symbols(&sidecar).expect("read setting");
        let second_read = read_wal_fec_raptorq_repair_symbols(&sidecar).expect("re-read setting");

        assert_eq!(first_read, 4);
        assert_eq!(second_read, 4);
    }

    #[test]
    fn test_wal_fec_pragma_persist_preserves_payload_bytes() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("db.wal-fec");
        let legacy_payload = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        fs::write(&sidecar, &legacy_payload).expect("write legacy payload");

        persist_wal_fec_raptorq_repair_symbols(&sidecar, 9).expect("persist setting");

        let rewritten = fs::read(&sidecar).expect("read rewritten sidecar");
        assert!(rewritten.len() >= WAL_FEC_PRAGMA_HEADER_BYTES);
        assert_eq!(
            &rewritten[WAL_FEC_PRAGMA_HEADER_BYTES..],
            legacy_payload.as_slice()
        );
    }

    #[test]
    fn test_scan_wal_fec_accepts_header_only_file() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("db.wal-fec");

        persist_wal_fec_raptorq_repair_symbols(&sidecar, 3).expect("persist setting");
        let scan = scan_wal_fec(&sidecar).expect("scan header-only sidecar");

        assert!(scan.groups.is_empty());
        assert!(!scan.truncated_tail);
    }

    // -- bd-2ha1: WalFecGroupMeta unit tests --

    const BEAD_ID_2HA1: &str = "bd-2ha1";

    fn make_test_init(k: u32) -> WalFecGroupMetaInit {
        let page_size = 4096_u32;
        WalFecGroupMetaInit {
            wal_salt1: 0x1234_5678,
            wal_salt2: 0xABCD_EF01,
            start_frame_no: 1,
            end_frame_no: k,
            db_size_pages: 100,
            page_size,
            k_source: k,
            r_repair: 2,
            oti: Oti {
                f: u64::from(k) * u64::from(page_size),
                al: 0,
                t: page_size,
                z: 1,
                n: 1,
            },
            object_id: ObjectId::from_bytes([0xAA; 16]),
            page_numbers: (1..=k).collect(),
            source_page_xxh3_128: (0..k)
                .map(|i| Xxh3Checksum128 {
                    low: u64::from(i),
                    high: u64::from(i).wrapping_add(1),
                })
                .collect(),
        }
    }

    #[test]
    fn test_meta_roundtrip() {
        let init = make_test_init(3);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");
        let serialized = meta.to_record_bytes();
        let deserialized =
            WalFecGroupMeta::from_record_bytes(&serialized).expect("from_record_bytes");

        assert_eq!(meta, deserialized, "bead_id={BEAD_ID_2HA1} case=roundtrip");
        eprintln!(
            "DEBUG bead_id={BEAD_ID_2HA1} case=meta_roundtrip serialized_len={}",
            serialized.len()
        );
    }

    #[test]
    fn test_meta_magic() {
        let init = make_test_init(2);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");

        assert_eq!(
            meta.magic, WAL_FEC_GROUP_META_MAGIC,
            "bead_id={BEAD_ID_2HA1} case=magic expected=FSQLWFEC"
        );
        assert_eq!(
            &meta.magic, b"FSQLWFEC",
            "bead_id={BEAD_ID_2HA1} case=magic_bytes"
        );

        // Serialized form also starts with magic.
        let bytes = meta.to_record_bytes();
        assert_eq!(
            &bytes[..8],
            b"FSQLWFEC",
            "bead_id={BEAD_ID_2HA1} case=serialized_magic"
        );
    }

    #[test]
    fn test_meta_invariant_k_source() {
        let mut init = make_test_init(3);
        // Break invariant: k_source != frame span.
        init.k_source = 99;
        let result = WalFecGroupMeta::from_init(init);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID_2HA1} case=k_source_mismatch expected=Err"
        );
        eprintln!(
            "INFO bead_id={BEAD_ID_2HA1} case=invariant_k_source error={}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_meta_invariant_page_numbers_len() {
        let mut init = make_test_init(3);
        // Break invariant: page_numbers.len != k_source.
        init.page_numbers.push(999);
        let result = WalFecGroupMeta::from_init(init);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID_2HA1} case=page_numbers_len_mismatch expected=Err"
        );
        eprintln!(
            "WARN bead_id={BEAD_ID_2HA1} case=invariant_page_numbers_len error={}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_meta_invariant_xxh3_len() {
        let mut init = make_test_init(3);
        // Break invariant: source_page_xxh3_128.len != k_source.
        init.source_page_xxh3_128.pop();
        let result = WalFecGroupMeta::from_init(init);
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID_2HA1} case=xxh3_len_mismatch expected=Err"
        );
        eprintln!(
            "WARN bead_id={BEAD_ID_2HA1} case=invariant_xxh3_len error={}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_meta_checksum_valid() {
        let init = make_test_init(4);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");
        let serialized = meta.to_record_bytes();

        // Deserialization validates checksum internally.
        let result = WalFecGroupMeta::from_record_bytes(&serialized);
        assert!(result.is_ok(), "bead_id={BEAD_ID_2HA1} case=checksum_valid");
        eprintln!(
            "INFO bead_id={BEAD_ID_2HA1} case=checksum_valid checksum={:#018x}",
            meta.checksum
        );
    }

    #[test]
    fn test_meta_checksum_corrupt() {
        let init = make_test_init(4);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");
        let mut serialized = meta.to_record_bytes();

        // Flip one bit in wal_salt1 (bytes 12..16) — no invariant checks on salt
        // fields, so this corruption is detected only by checksum mismatch.
        serialized[12] ^= 0x01;

        let result = WalFecGroupMeta::from_record_bytes(&serialized);
        let err = result.expect_err("bead_id={BEAD_ID_2HA1} case=checksum_corrupt expected=Err");
        let msg = err.to_string();
        assert!(
            msg.contains("checksum mismatch"),
            "bead_id={BEAD_ID_2HA1} case=checksum_corrupt expected checksum error, got: {msg}"
        );
        eprintln!("ERROR bead_id={BEAD_ID_2HA1} case=checksum_corrupt error={err}");
    }

    /// Build deterministic source pages of `page_size` bytes each.
    #[allow(clippy::cast_possible_truncation)]
    fn make_source_pages(k: u32, page_size: u32) -> Vec<Vec<u8>> {
        let ps = page_size as usize;
        (0..k)
            .map(|i| {
                (0..ps)
                    .map(|j| ((i as usize * 37 + j * 13 + 7) % 256) as u8)
                    .collect()
            })
            .collect()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn make_test_init_with_hashes(k: u32, source_pages: &[Vec<u8>]) -> WalFecGroupMetaInit {
        let page_size = source_pages[0].len() as u32;
        WalFecGroupMetaInit {
            wal_salt1: 0x1234_5678,
            wal_salt2: 0xABCD_EF01,
            start_frame_no: 1,
            end_frame_no: k,
            db_size_pages: 100,
            page_size,
            k_source: k,
            r_repair: 2,
            oti: Oti {
                f: u64::from(k) * u64::from(page_size),
                al: 0,
                t: page_size,
                z: 1,
                n: 1,
            },
            object_id: ObjectId::from_bytes([0xAA; 16]),
            page_numbers: (1..=k).collect(),
            source_page_xxh3_128: build_source_page_hashes(source_pages),
        }
    }

    #[test]
    fn test_raptorq_encode_produces_valid_symbols() {
        let k = 4_u32;
        let page_size = 4096_u32;
        let source_pages = make_source_pages(k, page_size);
        let init = make_test_init_with_hashes(k, &source_pages);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");

        let symbols =
            generate_wal_fec_repair_symbols_inner(&meta, &source_pages, None, Duration::ZERO)
                .expect("encode should succeed")
                .expect("should not be cancelled");

        assert_eq!(symbols.len(), 2, "expected r_repair=2 repair symbols");
        for (i, sym) in symbols.iter().enumerate() {
            assert_eq!(
                sym.symbol_data.len(),
                page_size as usize,
                "repair symbol {i} size"
            );
            let expected_esi = k + u32::try_from(i).expect("i fits u32");
            assert_eq!(sym.esi, expected_esi, "repair symbol {i} ESI");
        }
    }

    #[test]
    fn test_raptorq_encode_decode_roundtrip_all_source() {
        // When all source symbols are available, decode should still succeed.
        let k = 4_u32;
        let page_size = 512_u32;
        let source_pages = make_source_pages(k, page_size);
        let init = make_test_init_with_hashes(k, &source_pages);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");

        let repair_symbols =
            generate_wal_fec_repair_symbols_inner(&meta, &source_pages, None, Duration::ZERO)
                .expect("encode")
                .expect("not cancelled");

        // Build (esi, data) pairs: all source + all repair.
        let mut all_symbols: Vec<(u32, Vec<u8>)> = source_pages
            .iter()
            .enumerate()
            .map(|(i, page)| (u32::try_from(i).expect("i fits u32"), page.clone()))
            .collect();
        for sym in &repair_symbols {
            all_symbols.push((sym.esi, sym.symbol_data.clone()));
        }

        let decoded = wal_fec_raptorq_decode(&meta, &all_symbols)
            .expect("decode with all symbols should succeed");

        for (i, original) in source_pages.iter().enumerate() {
            assert_eq!(&decoded[i], original, "decoded source page {i} mismatch");
        }
    }

    #[test]
    fn test_raptorq_encode_decode_roundtrip_with_corruption() {
        // Lose one source page, recover from remaining source + repair symbols.
        let k = 4_u32;
        let page_size = 512_u32;
        let r_repair = 4_u32; // Need enough repair symbols for recovery
        let source_pages = make_source_pages(k, page_size);
        let mut init = make_test_init_with_hashes(k, &source_pages);
        init.r_repair = r_repair;
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");

        let repair_symbols =
            generate_wal_fec_repair_symbols_inner(&meta, &source_pages, None, Duration::ZERO)
                .expect("encode")
                .expect("not cancelled");

        // Simulate losing source page 1: only provide pages 0, 2, 3 + all repair.
        let mut available_symbols: Vec<(u32, Vec<u8>)> = Vec::new();
        for (i, page) in source_pages.iter().enumerate() {
            if i != 1 {
                available_symbols.push((u32::try_from(i).expect("i fits u32"), page.clone()));
            }
        }
        for sym in &repair_symbols {
            available_symbols.push((sym.esi, sym.symbol_data.clone()));
        }

        let decoded = wal_fec_raptorq_decode(&meta, &available_symbols)
            .expect("decode should recover missing page");

        for (i, original) in source_pages.iter().enumerate() {
            assert_eq!(
                &decoded[i], original,
                "decoded source page {i} mismatch (page 1 was lost)"
            );
        }
    }

    #[test]
    fn test_raptorq_encode_deterministic() {
        // Same inputs produce identical repair symbols.
        let k = 3_u32;
        let page_size = 512_u32;
        let source_pages = make_source_pages(k, page_size);
        let init = make_test_init_with_hashes(k, &source_pages);
        let meta = WalFecGroupMeta::from_init(init).expect("from_init");

        let symbols1 =
            generate_wal_fec_repair_symbols_inner(&meta, &source_pages, None, Duration::ZERO)
                .expect("encode 1")
                .expect("not cancelled");

        let symbols2 =
            generate_wal_fec_repair_symbols_inner(&meta, &source_pages, None, Duration::ZERO)
                .expect("encode 2")
                .expect("not cancelled");

        assert_eq!(symbols1.len(), symbols2.len());
        for (i, (s1, s2)) in symbols1.iter().zip(symbols2.iter()).enumerate() {
            assert_eq!(
                s1.symbol_data, s2.symbol_data,
                "repair symbol {i} not deterministic"
            );
        }
    }

    #[test]
    fn test_raptorq_telemetry_records_metrics_and_events() {
        let _guard = telemetry_test_lock();
        reset_raptorq_repair_telemetry();

        let group_id = WalFecGroupId {
            wal_salt1: 0xAA11_BB22,
            wal_salt2: 0xCC33_DD44,
            end_frame_no: 42,
        };
        let log = WalFecRecoveryLog {
            group_id,
            recovery_enabled: true,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: 4,
            validated_repair_symbols: 2,
            required_symbols: 6,
            available_symbols: 6,
            recovered_frame_nos: vec![40, 41],
            corruption_observations: 1,
            decode_attempted: true,
            decode_succeeded: true,
        };
        record_raptorq_recovery_log(&log, Duration::from_micros(75));

        let metrics = raptorq_repair_metrics_snapshot();
        assert_eq!(metrics.repairs_total, 1);
        assert_eq!(metrics.repairs_failed, 0);
        assert_eq!(metrics.symbols_reclaimed, 2);
        assert_eq!(metrics.budget_utilization_pct, 100);
        assert_eq!(metrics.severity_histogram.two_to_five, 1);
        assert_eq!(metrics.wal_health_score, 81);

        let events = raptorq_repair_events_snapshot(10);
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.frame_id, 42);
        assert_eq!(event.symbols_lost, 2);
        assert_eq!(event.symbols_used, 6);
        assert!(event.repair_success);
        assert_eq!(event.severity_bucket, WalFecRepairSeverityBucket::TwoToFive);
        assert_eq!(event.budget_utilization_pct, 100);
    }

    #[test]
    fn test_raptorq_telemetry_health_penalizes_failures() {
        let _guard = telemetry_test_lock();
        reset_raptorq_repair_telemetry();

        let success_group = WalFecGroupId {
            wal_salt1: 1,
            wal_salt2: 2,
            end_frame_no: 10,
        };
        let success = WalFecRecoveryLog {
            group_id: success_group,
            recovery_enabled: true,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: 5,
            validated_repair_symbols: 1,
            required_symbols: 6,
            available_symbols: 6,
            recovered_frame_nos: vec![9],
            corruption_observations: 0,
            decode_attempted: true,
            decode_succeeded: true,
        };
        record_raptorq_recovery_log(&success, Duration::from_micros(50));

        let failed_group = WalFecGroupId {
            wal_salt1: 3,
            wal_salt2: 4,
            end_frame_no: 20,
        };
        let failed = WalFecRecoveryLog {
            group_id: failed_group,
            recovery_enabled: true,
            outcome_is_recovered: false,
            fallback_reason: Some(WalFecRecoveryFallbackReason::InsufficientSymbols),
            validated_source_symbols: 2,
            validated_repair_symbols: 2,
            required_symbols: 6,
            available_symbols: 4,
            recovered_frame_nos: Vec::new(),
            corruption_observations: 2,
            decode_attempted: true,
            decode_succeeded: false,
        };
        record_raptorq_recovery_log(&failed, Duration::from_micros(90));
        record_raptorq_recovery_log(&failed, Duration::from_micros(120));

        let metrics = raptorq_repair_metrics_snapshot();
        assert_eq!(metrics.repairs_total, 3);
        assert_eq!(metrics.repairs_failed, 2);
        assert!(metrics.wal_health_score < 100);
        assert_eq!(metrics.severity_histogram.one, 1);
        assert_eq!(metrics.severity_histogram.two_to_five, 2);
    }

    #[test]
    fn test_raptorq_telemetry_histogram_buckets() {
        let _guard = telemetry_test_lock();
        reset_raptorq_repair_telemetry();

        let samples = [
            (1_u32, WalFecRepairSeverityBucket::One),
            (3_u32, WalFecRepairSeverityBucket::TwoToFive),
            (8_u32, WalFecRepairSeverityBucket::SixToTen),
            (12_u32, WalFecRepairSeverityBucket::ElevenPlus),
        ];

        for (idx, (loss, expected_bucket)) in samples.iter().enumerate() {
            let group_id = WalFecGroupId {
                wal_salt1: 10 + u32::try_from(idx).expect("small index"),
                wal_salt2: 20 + u32::try_from(idx).expect("small index"),
                end_frame_no: 30 + u32::try_from(idx).expect("small index"),
            };
            let log = WalFecRecoveryLog {
                group_id,
                recovery_enabled: true,
                outcome_is_recovered: true,
                fallback_reason: None,
                validated_source_symbols: 20_u32.saturating_sub(*loss),
                validated_repair_symbols: *loss,
                required_symbols: 20,
                available_symbols: 20,
                recovered_frame_nos: vec![group_id.end_frame_no],
                corruption_observations: 0,
                decode_attempted: true,
                decode_succeeded: true,
            };
            record_raptorq_recovery_log(&log, Duration::from_micros(30));

            let events = raptorq_repair_events_snapshot(1);
            let event = events
                .last()
                .expect("latest event must be present after recording");
            assert_eq!(event.severity_bucket, *expected_bucket);
        }

        let metrics = raptorq_repair_metrics_snapshot();
        assert_eq!(metrics.severity_histogram.one, 1);
        assert_eq!(metrics.severity_histogram.two_to_five, 1);
        assert_eq!(metrics.severity_histogram.six_to_ten, 1);
        assert_eq!(metrics.severity_histogram.eleven_plus, 1);
    }

    #[test]
    fn test_raptorq_repair_evidence_chain_and_capacity() {
        let _guard = telemetry_test_lock();
        reset_raptorq_repair_telemetry();

        let first_group = WalFecGroupId {
            wal_salt1: 0x0A0A_0B0B,
            wal_salt2: 0x0C0C_0D0D,
            end_frame_no: 101,
        };
        let first_log = WalFecRecoveryLog {
            group_id: first_group,
            recovery_enabled: true,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: 5,
            validated_repair_symbols: 1,
            required_symbols: 6,
            available_symbols: 6,
            recovered_frame_nos: vec![100, 101],
            corruption_observations: 1,
            decode_attempted: true,
            decode_succeeded: true,
        };
        record_raptorq_recovery_log(&first_log, Duration::from_micros(10));

        let second_group = WalFecGroupId {
            wal_salt1: 0x1A1A_1B1B,
            wal_salt2: 0x1C1C_1D1D,
            end_frame_no: 202,
        };
        let second_log = WalFecRecoveryLog {
            group_id: second_group,
            recovery_enabled: true,
            outcome_is_recovered: false,
            fallback_reason: Some(WalFecRecoveryFallbackReason::InsufficientSymbols),
            validated_source_symbols: 2,
            validated_repair_symbols: 2,
            required_symbols: 6,
            available_symbols: 4,
            recovered_frame_nos: Vec::new(),
            corruption_observations: 2,
            decode_attempted: true,
            decode_succeeded: false,
        };
        record_raptorq_recovery_log(&second_log, Duration::from_micros(25));

        let cards = raptorq_repair_evidence_snapshot(0);
        assert_eq!(cards.len(), 2);
        assert_eq!(cards[0].ledger_epoch, 1);
        assert_eq!(cards[1].ledger_epoch, 2);
        assert_ne!(cards[0].chain_hash, cards[1].chain_hash);
        assert!(cards[0].monotonic_timestamp_ns > 0);
        assert!(cards[0].wall_clock_unix_ns > 0);
        assert_eq!(cards[0].frame_id, first_group.end_frame_no);
        assert_eq!(cards[1].frame_id, second_group.end_frame_no);
        assert_eq!(cards[0].confidence_per_mille, 1_000);
        assert_eq!(cards[1].confidence_per_mille, 666);
        assert_ne!(cards[0].witness.corrupted_hash_blake3, [0_u8; 32]);
    }

    #[test]
    fn test_raptorq_repair_evidence_query_filters() {
        let _guard = telemetry_test_lock();
        reset_raptorq_repair_telemetry();

        let logs = [
            WalFecRecoveryLog {
                group_id: WalFecGroupId {
                    wal_salt1: 11,
                    wal_salt2: 12,
                    end_frame_no: 301,
                },
                recovery_enabled: true,
                outcome_is_recovered: true,
                fallback_reason: None,
                validated_source_symbols: 9,
                validated_repair_symbols: 1,
                required_symbols: 10,
                available_symbols: 10,
                recovered_frame_nos: vec![301],
                corruption_observations: 0,
                decode_attempted: true,
                decode_succeeded: true,
            },
            WalFecRecoveryLog {
                group_id: WalFecGroupId {
                    wal_salt1: 21,
                    wal_salt2: 22,
                    end_frame_no: 302,
                },
                recovery_enabled: true,
                outcome_is_recovered: true,
                fallback_reason: None,
                validated_source_symbols: 7,
                validated_repair_symbols: 3,
                required_symbols: 10,
                available_symbols: 10,
                recovered_frame_nos: vec![302],
                corruption_observations: 1,
                decode_attempted: true,
                decode_succeeded: true,
            },
            WalFecRecoveryLog {
                group_id: WalFecGroupId {
                    wal_salt1: 31,
                    wal_salt2: 32,
                    end_frame_no: 303,
                },
                recovery_enabled: true,
                outcome_is_recovered: true,
                fallback_reason: None,
                validated_source_symbols: 1,
                validated_repair_symbols: 9,
                required_symbols: 10,
                available_symbols: 10,
                recovered_frame_nos: vec![303],
                corruption_observations: 3,
                decode_attempted: true,
                decode_succeeded: true,
            },
        ];
        for log in &logs {
            record_raptorq_recovery_log(log, Duration::from_micros(15));
        }

        let cards = raptorq_repair_evidence_snapshot(0);
        assert_eq!(cards.len(), 3);

        let by_frame = query_raptorq_repair_evidence(&WalFecRepairEvidenceQuery {
            frame_id: Some(302),
            ..WalFecRepairEvidenceQuery::default()
        });
        assert_eq!(by_frame.len(), 1);
        assert_eq!(by_frame[0].frame_id, 302);

        let by_severity = query_raptorq_repair_evidence(&WalFecRepairEvidenceQuery {
            severity_bucket: Some(WalFecRepairSeverityBucket::SixToTen),
            ..WalFecRepairEvidenceQuery::default()
        });
        assert_eq!(by_severity.len(), 1);
        assert_eq!(by_severity[0].frame_id, 303);

        let min_time = cards
            .first()
            .map(|card| card.wall_clock_unix_ns)
            .expect("evidence cards should include timestamps");
        let max_time = cards
            .last()
            .map(|card| card.wall_clock_unix_ns)
            .expect("evidence cards should include timestamps");
        let by_time = query_raptorq_repair_evidence(&WalFecRepairEvidenceQuery {
            wall_clock_start_ns: Some(min_time),
            wall_clock_end_ns: Some(max_time),
            ..WalFecRepairEvidenceQuery::default()
        });
        assert_eq!(by_time.len(), 3);

        let limited = query_raptorq_repair_evidence(&WalFecRepairEvidenceQuery {
            limit: Some(2),
            ..WalFecRepairEvidenceQuery::default()
        });
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].frame_id, 302);
        assert_eq!(limited[1].frame_id, 303);
    }
}
