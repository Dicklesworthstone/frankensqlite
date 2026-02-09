//! WAL-FEC sidecar format (`.wal-fec`) for self-healing WAL durability (§3.4.1).
//!
//! The sidecar is append-only. Each group is encoded as:
//! 1. length-prefixed [`WalFecGroupMeta`]
//! 2. `R` length-prefixed ECS [`SymbolRecord`] repair symbols (`esi = K..K+R-1`)
//!
//! Source symbols remain in `.wal` frames and are never duplicated in sidecar.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, Oti, PageSize, SymbolRecord};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

use crate::checksum::{WalSalts, Xxh3Checksum128, wal_fec_source_hash_xxh3_128};

/// Magic bytes for [`WalFecGroupMeta`].
pub const WAL_FEC_GROUP_META_MAGIC: [u8; 8] = *b"FSQLWFEC";
/// Current [`WalFecGroupMeta`] wire version.
pub const WAL_FEC_GROUP_META_VERSION: u32 = 1;

const LENGTH_PREFIX_BYTES: usize = 4;
const META_FIXED_PREFIX_BYTES: usize = 8 + 4 + (8 * 4) + 22 + 16;
const META_CHECKSUM_BYTES: usize = 8;

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
                high: u64::from_le_bytes(
                    digest[8..].try_into().expect("8-byte high hash slice"),
                ),
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
        let mut bytes = self.to_record_bytes_without_checksum();
        xxh3_64(&bytes.split_off(0))
    }

    fn to_record_bytes_without_checksum(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.serialized_len_without_prefix() - META_CHECKSUM_BYTES);
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
        if self.r_repair == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "r_repair must be >= 1 for wal-fec groups".to_owned(),
            });
        }
        if self.page_numbers.len()
            != usize::try_from(self.k_source).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("k_source {} does not fit in usize", self.k_source),
            })?
        {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "page_numbers length {} must equal k_source {}",
                    self.page_numbers.len(),
                    self.k_source
                ),
            });
        }
        if self.source_page_xxh3_128.len()
            != usize::try_from(self.k_source).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("k_source {} does not fit in usize", self.k_source),
            })?
        {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "source_page_xxh3_128 length {} must equal k_source {}",
                    self.source_page_xxh3_128.len(),
                    self.k_source
                ),
            });
        }
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
        if self.db_size_pages == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "db_size_pages must be non-zero commit frame size".to_owned(),
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
    if wal_name.ends_with("-wal") {
        PathBuf::from(format!("{wal_name}-fec"))
    } else if wal_name.ends_with(".wal") {
        PathBuf::from(format!("{wal_name}-fec"))
    } else {
        PathBuf::from(format!("{wal_name}.wal-fec"))
    }
}

/// Ensure WAL file and `.wal-fec` sidecar both exist.
pub fn ensure_wal_with_fec_sidecar(wal_path: &Path) -> Result<PathBuf> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path)?;
    let sidecar_path = wal_fec_path_for_wal(wal_path);
    OpenOptions::new()
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

    let mut file = OpenOptions::new()
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
    let mut cursor = 0usize;
    let mut groups = Vec::new();
    let mut truncated_tail = false;

    while cursor < bytes.len() {
        let meta_bytes = match read_length_prefixed(&bytes, &mut cursor)? {
            Some(record) => record,
            None => {
                truncated_tail = true;
                warn!(
                    sidecar = %sidecar_path.display(),
                    cursor,
                    "truncated wal-fec metadata tail detected"
                );
                break;
            }
        };
        let meta = WalFecGroupMeta::from_record_bytes(meta_bytes)?;
        let mut repair_symbols = Vec::with_capacity(
            usize::try_from(meta.r_repair).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("r_repair {} does not fit in usize", meta.r_repair),
            })?,
        );

        for _ in 0..meta.r_repair {
            let symbol_bytes = match read_length_prefixed(&bytes, &mut cursor)? {
                Some(record) => record,
                None => {
                    truncated_tail = true;
                    warn!(
                        sidecar = %sidecar_path.display(),
                        group_id = %meta.group_id(),
                        cursor,
                        "truncated wal-fec repair-symbol tail detected"
                    );
                    break;
                }
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

fn write_length_prefixed(file: &mut File, payload: &[u8], what: &str) -> Result<()> {
    let len_u32 = u32::try_from(payload.len()).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("{what} too large for wal-fec length prefix: {}", payload.len()),
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
    let payload_len = usize::try_from(u32::from_le_bytes(len_raw)).map_err(|_| {
        FrankenError::WalCorrupt {
            detail: "wal-fec length prefix does not fit in usize".to_owned(),
        }
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
    let end = cursor.checked_add(N).ok_or_else(|| FrankenError::WalCorrupt {
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

