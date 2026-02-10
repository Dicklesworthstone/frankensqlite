//! WAL-FEC sidecar format (`.wal-fec`) for self-healing WAL durability (§3.4.1).
//!
//! The sidecar is append-only. Each group is encoded as:
//! 1. length-prefixed [`WalFecGroupMeta`]
//! 2. `R` length-prefixed ECS [`SymbolRecord`] repair symbols (`esi = K..K+R-1`)
//!
//! Source symbols remain in `.wal` frames and are never duplicated in sidecar.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::Write;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
    let mut symbols = Vec::with_capacity(usize::try_from(meta.r_repair).map_err(|_| {
        FrankenError::WalCorrupt {
            detail: format!("r_repair {} does not fit in usize", meta.r_repair),
        }
    })?);

    for repair_index in 0..meta.r_repair {
        if let Some(flag) = cancel_flag {
            if flag.load(Ordering::SeqCst) {
                return Ok(None);
            }
        }
        let mut payload = vec![0_u8; symbol_len];
        let seed = derive_repair_seed(meta, repair_index);
        let seed_bytes = seed.to_le_bytes();
        let repair_index_usize =
            usize::try_from(repair_index).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("repair_index {repair_index} does not fit in usize"),
            })?;

        for byte_index in 0..symbol_len {
            let mut mixed = seed_bytes[byte_index % seed_bytes.len()];
            for (page_index, page) in source_pages.iter().enumerate() {
                let read_index = (byte_index + page_index + repair_index_usize) % symbol_len;
                let source_byte = page[read_index];
                let tweak_raw = (page_index + byte_index) & 0xFF;
                let tweak = u8::try_from(tweak_raw).map_err(|_| FrankenError::WalCorrupt {
                    detail: format!("tweak value {tweak_raw} does not fit in u8"),
                })?;
                mixed ^= source_byte;
                mixed = mixed.rotate_left(u32::from(tweak & 0x07)) ^ tweak;
            }
            payload[byte_index] = mixed;
        }

        if per_symbol_delay > Duration::ZERO {
            thread::sleep(per_symbol_delay);
        }
        let esi =
            meta.k_source
                .checked_add(repair_index)
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: "repair symbol ESI overflow".to_owned(),
                })?;
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

    use tempfile::tempdir;

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
}
