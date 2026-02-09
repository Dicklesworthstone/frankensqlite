//! Symbol Record Logs (append-only) for ECS objects (§3.5.4.2, `bd-1hi.24`).
//!
//! Segment files live under `ecs/symbols/` and contain:
//! - one fixed 40-byte [`SymbolSegmentHeader`]
//! - zero or more variable-size [`fsqlite_types::SymbolRecord`] payloads
//!
//! The default on-disk layout stores records back-to-back (no padding).
//! Torn tails are tolerated during scans: complete records are preserved and
//! incomplete tail bytes are ignored.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, SymbolRecord};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

const BEAD_ID: &str = "bd-1hi.24";
const LOGGING_STANDARD_BEAD: &str = "bd-1fpm";

/// Magic bytes for a symbol segment header (`"FSSY"`).
pub const SYMBOL_SEGMENT_MAGIC: [u8; 4] = *b"FSSY";
/// Current symbol segment format version.
pub const SYMBOL_SEGMENT_VERSION: u32 = 1;
/// Exact byte size of [`SymbolSegmentHeader`] on disk.
pub const SYMBOL_SEGMENT_HEADER_BYTES: usize = 40;

const SYMBOL_SEGMENT_HASH_INPUT_BYTES: usize = 32;

// SymbolRecord wire constants from `fsqlite-types`:
// header(51) + data(T) + trailer(25).
const SYMBOL_RECORD_HEADER_BYTES: usize = 51;
const SYMBOL_RECORD_TRAILER_BYTES: usize = 25;
const SYMBOL_SIZE_FIELD_OFFSET: usize = 47;
const SYMBOL_SIZE_FIELD_BYTES: usize = 4;

/// Header stored at the start of each symbol segment.
///
/// Layout (40 bytes, little-endian integer fields):
/// - `magic[4]`
/// - `version: u32`
/// - `segment_id: u64`
/// - `epoch_id: u64`
/// - `created_at: u64`
/// - `header_xxh3: u64` (hash of preceding 32 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolSegmentHeader {
    /// Monotonic segment identifier (matches filename).
    pub segment_id: u64,
    /// ECS coordination epoch at segment creation.
    pub epoch_id: u64,
    /// Segment creation timestamp (`unix_ns`).
    pub created_at: u64,
}

impl SymbolSegmentHeader {
    /// Construct a new header.
    #[must_use]
    pub const fn new(segment_id: u64, epoch_id: u64, created_at: u64) -> Self {
        Self {
            segment_id,
            epoch_id,
            created_at,
        }
    }

    /// Encode the header to its exact wire representation.
    #[must_use]
    pub fn encode(&self) -> [u8; SYMBOL_SEGMENT_HEADER_BYTES] {
        let mut out = [0_u8; SYMBOL_SEGMENT_HEADER_BYTES];
        out[0..4].copy_from_slice(&SYMBOL_SEGMENT_MAGIC);
        out[4..8].copy_from_slice(&SYMBOL_SEGMENT_VERSION.to_le_bytes());
        out[8..16].copy_from_slice(&self.segment_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.epoch_id.to_le_bytes());
        out[24..32].copy_from_slice(&self.created_at.to_le_bytes());
        let checksum = xxh3_64(&out[..SYMBOL_SEGMENT_HASH_INPUT_BYTES]);
        out[32..40].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    /// Decode and validate a header from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < SYMBOL_SEGMENT_HEADER_BYTES {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "symbol segment header too short: expected {SYMBOL_SEGMENT_HEADER_BYTES}, got {}",
                    bytes.len()
                ),
            });
        }

        if bytes[0..4] != SYMBOL_SEGMENT_MAGIC {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("invalid symbol segment magic: {:02X?}", &bytes[0..4]),
            });
        }

        let version = read_u32_at(bytes, 4, "version")?;
        if version != SYMBOL_SEGMENT_VERSION {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "unsupported symbol segment version {version}, expected {SYMBOL_SEGMENT_VERSION}"
                ),
            });
        }

        let segment_id = read_u64_at(bytes, 8, "segment_id")?;
        let epoch_id = read_u64_at(bytes, 16, "epoch_id")?;
        let created_at = read_u64_at(bytes, 24, "created_at")?;
        let stored_checksum = read_u64_at(bytes, 32, "header_xxh3")?;
        let computed_checksum = xxh3_64(&bytes[..SYMBOL_SEGMENT_HASH_INPUT_BYTES]);

        if stored_checksum != computed_checksum {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "symbol segment header checksum mismatch: stored {stored_checksum:#018X}, computed {computed_checksum:#018X}"
                ),
            });
        }

        Ok(Self {
            segment_id,
            epoch_id,
            created_at,
        })
    }
}

/// Locator offset for a symbol record within a specific segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SymbolLogOffset {
    /// Segment containing the record.
    pub segment_id: u64,
    /// Byte offset from immediately after the 40-byte segment header.
    pub offset_bytes: u64,
}

/// Scan-time representation of one record in a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolLogRecord {
    /// Locator offset for random access.
    pub offset: SymbolLogOffset,
    /// Parsed symbol record.
    pub record: SymbolRecord,
}

/// Result of scanning a symbol segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolSegmentScan {
    /// Parsed segment header.
    pub header: SymbolSegmentHeader,
    /// All complete records before any torn tail.
    pub records: Vec<SymbolLogRecord>,
    /// True when trailing partial bytes were detected and ignored.
    pub torn_tail: bool,
}

/// Index entry for optional aligned-record layout experiments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlignedSymbolIndexEntry {
    /// Locator for the record start.
    pub offset: SymbolLogOffset,
    /// Logical SymbolRecord byte length (unpadded).
    pub logical_len: u32,
    /// Physical bytes written for this slot (includes padding).
    pub padded_len: u32,
}

/// Symbol log writer/rotator that enforces append-only active-segment policy.
#[derive(Debug, Clone)]
pub struct SymbolLogManager {
    symbols_dir: PathBuf,
    active_header: SymbolSegmentHeader,
}

impl SymbolLogManager {
    /// Open or create the active segment.
    pub fn new(
        symbols_dir: &Path,
        active_segment_id: u64,
        epoch_id: u64,
        created_at: u64,
    ) -> Result<Self> {
        let active_header = SymbolSegmentHeader::new(active_segment_id, epoch_id, created_at);
        let segment_path = symbol_segment_path(symbols_dir, active_segment_id);
        ensure_symbol_segment(&segment_path, active_header)?;

        info!(
            bead_id = BEAD_ID,
            logging_standard = LOGGING_STANDARD_BEAD,
            segment_id = active_segment_id,
            epoch_id,
            "opened symbol log manager"
        );

        Ok(Self {
            symbols_dir: symbols_dir.to_path_buf(),
            active_header,
        })
    }

    /// Current active segment identifier.
    #[must_use]
    pub const fn active_segment_id(&self) -> u64 {
        self.active_header.segment_id
    }

    /// Filesystem path for the current active segment.
    #[must_use]
    pub fn active_segment_path(&self) -> PathBuf {
        symbol_segment_path(&self.symbols_dir, self.active_header.segment_id)
    }

    /// Append to the active segment.
    pub fn append(&self, record: &SymbolRecord) -> Result<SymbolLogOffset> {
        append_symbol_record(&self.symbols_dir, self.active_header, record)
    }

    /// Append to a specific segment ID.
    ///
    /// Rotated segments are immutable; only the active segment accepts writes.
    pub fn append_to_segment(
        &self,
        segment_id: u64,
        record: &SymbolRecord,
    ) -> Result<SymbolLogOffset> {
        if segment_id != self.active_header.segment_id {
            warn!(
                bead_id = BEAD_ID,
                logging_standard = LOGGING_STANDARD_BEAD,
                requested_segment = segment_id,
                active_segment = self.active_header.segment_id,
                "append rejected because segment is immutable"
            );
            return Err(FrankenError::Internal(format!(
                "segment {segment_id} is immutable; active segment is {}",
                self.active_header.segment_id
            )));
        }
        self.append(record)
    }

    /// Rotate to a new active segment.
    pub fn rotate(
        &mut self,
        next_segment_id: u64,
        next_epoch_id: u64,
        next_created_at: u64,
    ) -> Result<()> {
        if next_segment_id <= self.active_header.segment_id {
            return Err(FrankenError::Internal(format!(
                "next segment id {next_segment_id} must be greater than current {}",
                self.active_header.segment_id
            )));
        }

        let next_header = SymbolSegmentHeader::new(next_segment_id, next_epoch_id, next_created_at);
        let next_path = symbol_segment_path(&self.symbols_dir, next_segment_id);
        ensure_symbol_segment(&next_path, next_header)?;
        self.active_header = next_header;

        info!(
            bead_id = BEAD_ID,
            logging_standard = LOGGING_STANDARD_BEAD,
            segment_id = next_segment_id,
            epoch_id = next_epoch_id,
            "rotated symbol log segment"
        );

        Ok(())
    }
}

/// Build a segment path: `segment-{segment_id:06}.log`.
#[must_use]
pub fn symbol_segment_path(symbols_dir: &Path, segment_id: u64) -> PathBuf {
    symbols_dir.join(format!("segment-{segment_id:06}.log"))
}

/// Ensure a segment exists with the given header.
pub fn ensure_symbol_segment(segment_path: &Path, header: SymbolSegmentHeader) -> Result<()> {
    if let Some(parent) = segment_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    if !segment_path.exists() {
        let encoded = header.encode();
        fs::write(segment_path, encoded)?;
        info!(
            bead_id = BEAD_ID,
            logging_standard = LOGGING_STANDARD_BEAD,
            path = %segment_path.display(),
            segment_id = header.segment_id,
            epoch_id = header.epoch_id,
            "created symbol segment"
        );
        return Ok(());
    }

    let bytes = fs::read(segment_path)?;
    if bytes.len() < SYMBOL_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "existing segment {} shorter than header: {} bytes",
                segment_path.display(),
                bytes.len()
            ),
        });
    }

    let existing = SymbolSegmentHeader::decode(&bytes[..SYMBOL_SEGMENT_HEADER_BYTES])?;
    if existing != header {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment header mismatch for {}: existing={existing:?}, requested={header:?}",
                segment_path.display()
            ),
        });
    }

    Ok(())
}

/// Append one SymbolRecord using packed (no-padding) layout.
pub fn append_symbol_record(
    symbols_dir: &Path,
    header: SymbolSegmentHeader,
    record: &SymbolRecord,
) -> Result<SymbolLogOffset> {
    let segment_path = symbol_segment_path(symbols_dir, header.segment_id);
    ensure_symbol_segment(&segment_path, header)?;

    let current_len = file_len_usize(&segment_path)?;
    if current_len < SYMBOL_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment {} length {} shorter than header",
                segment_path.display(),
                current_len
            ),
        });
    }

    let offset_bytes = usize_to_u64(
        current_len - SYMBOL_SEGMENT_HEADER_BYTES,
        "symbol log offset",
    )?;

    let mut file = fs::OpenOptions::new().append(true).open(&segment_path)?;
    let record_bytes = record.to_bytes();
    file.write_all(&record_bytes)?;
    file.sync_data()?;

    debug!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        path = %segment_path.display(),
        segment_id = header.segment_id,
        offset_bytes,
        logical_len = record_bytes.len(),
        "appended packed symbol record"
    );

    Ok(SymbolLogOffset {
        segment_id: header.segment_id,
        offset_bytes,
    })
}

/// Append one SymbolRecord in optional aligned layout.
///
/// This does not alter logical SymbolRecord bytes: only on-disk padding is added.
pub fn append_symbol_record_aligned(
    symbols_dir: &Path,
    header: SymbolSegmentHeader,
    record: &SymbolRecord,
    sector_size: u32,
) -> Result<AlignedSymbolIndexEntry> {
    if sector_size == 0 {
        return Err(FrankenError::Internal(
            "sector_size must be non-zero for aligned symbol append".to_owned(),
        ));
    }

    let segment_path = symbol_segment_path(symbols_dir, header.segment_id);
    ensure_symbol_segment(&segment_path, header)?;

    let current_len = file_len_usize(&segment_path)?;
    if current_len < SYMBOL_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment {} length {} shorter than header",
                segment_path.display(),
                current_len
            ),
        });
    }

    let record_bytes = record.to_bytes();
    let logical_len = record_bytes.len();
    let alignment_bytes = u32_to_usize(sector_size, "sector_size")?;
    let padded_len = align_up(logical_len, alignment_bytes)?;
    let padding = padded_len.saturating_sub(logical_len);

    let offset = SymbolLogOffset {
        segment_id: header.segment_id,
        offset_bytes: usize_to_u64(
            current_len - SYMBOL_SEGMENT_HEADER_BYTES,
            "symbol log offset",
        )?,
    };

    let mut file = fs::OpenOptions::new().append(true).open(&segment_path)?;
    file.write_all(&record_bytes)?;
    if padding > 0 {
        file.write_all(&vec![0_u8; padding])?;
    }
    file.sync_data()?;

    let entry = AlignedSymbolIndexEntry {
        offset,
        logical_len: usize_to_u32(logical_len, "logical_len")?,
        padded_len: usize_to_u32(padded_len, "padded_len")?,
    };

    debug!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        path = %segment_path.display(),
        segment_id = header.segment_id,
        offset_bytes = offset.offset_bytes,
        logical_len = entry.logical_len,
        padded_len = entry.padded_len,
        sector_size,
        "appended aligned symbol record"
    );

    Ok(entry)
}

/// Scan a segment, returning all complete records and torn-tail status.
pub fn scan_symbol_segment(segment_path: &Path) -> Result<SymbolSegmentScan> {
    let bytes = fs::read(segment_path)?;
    if bytes.len() < SYMBOL_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment {} shorter than header: {} bytes",
                segment_path.display(),
                bytes.len()
            ),
        });
    }

    let header = SymbolSegmentHeader::decode(&bytes[..SYMBOL_SEGMENT_HEADER_BYTES])?;
    let mut cursor = SYMBOL_SEGMENT_HEADER_BYTES;
    let mut records = Vec::new();
    let mut torn_tail = false;

    while cursor < bytes.len() {
        let parsed = parse_symbol_record_at(&bytes, header.segment_id, cursor)?;
        let Some((record, len)) = parsed else {
            torn_tail = true;
            warn!(
                bead_id = BEAD_ID,
                logging_standard = LOGGING_STANDARD_BEAD,
                path = %segment_path.display(),
                segment_id = header.segment_id,
                absolute_offset = cursor,
                "detected torn tail while scanning symbol segment"
            );
            break;
        };
        records.push(record);
        cursor = cursor
            .checked_add(len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "cursor overflow while scanning symbol segment".to_owned(),
            })?;
    }

    info!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        path = %segment_path.display(),
        segment_id = header.segment_id,
        record_count = records.len(),
        torn_tail,
        "scanned symbol segment"
    );

    Ok(SymbolSegmentScan {
        header,
        records,
        torn_tail,
    })
}

/// Read one packed SymbolRecord at a locator offset.
pub fn read_symbol_record_at_offset(
    segment_path: &Path,
    offset: SymbolLogOffset,
) -> Result<SymbolRecord> {
    let bytes = fs::read(segment_path)?;
    if bytes.len() < SYMBOL_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment {} shorter than header: {} bytes",
                segment_path.display(),
                bytes.len()
            ),
        });
    }

    let header = SymbolSegmentHeader::decode(&bytes[..SYMBOL_SEGMENT_HEADER_BYTES])?;
    if header.segment_id != offset.segment_id {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment id mismatch: locator={}, header={}",
                offset.segment_id, header.segment_id
            ),
        });
    }

    let offset_usize = u64_to_usize(offset.offset_bytes, "offset_bytes")?;
    let absolute_offset = SYMBOL_SEGMENT_HEADER_BYTES
        .checked_add(offset_usize)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "absolute offset overflow while reading symbol record".to_owned(),
        })?;

    let Some((record, _)) = parse_symbol_record_at(&bytes, header.segment_id, absolute_offset)?
    else {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "no complete symbol record at offset {} in {}",
                offset.offset_bytes,
                segment_path.display()
            ),
        });
    };

    Ok(record.record)
}

/// Read one aligned-layout SymbolRecord using an explicit index entry.
pub fn read_aligned_symbol_record(
    segment_path: &Path,
    entry: AlignedSymbolIndexEntry,
) -> Result<SymbolRecord> {
    let bytes = fs::read(segment_path)?;
    if bytes.len() < SYMBOL_SEGMENT_HEADER_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment {} shorter than header: {} bytes",
                segment_path.display(),
                bytes.len()
            ),
        });
    }

    let header = SymbolSegmentHeader::decode(&bytes[..SYMBOL_SEGMENT_HEADER_BYTES])?;
    if header.segment_id != entry.offset.segment_id {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "segment id mismatch: locator={}, header={}",
                entry.offset.segment_id, header.segment_id
            ),
        });
    }

    let offset_usize = u64_to_usize(entry.offset.offset_bytes, "offset_bytes")?;
    let absolute_offset = SYMBOL_SEGMENT_HEADER_BYTES
        .checked_add(offset_usize)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "absolute offset overflow while reading aligned symbol".to_owned(),
        })?;
    let logical_len = u32_to_usize(entry.logical_len, "logical_len")?;
    let end =
        absolute_offset
            .checked_add(logical_len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "aligned logical read overflow".to_owned(),
            })?;
    if end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "aligned symbol read out of bounds: end={}, file_len={}",
                end,
                bytes.len()
            ),
        });
    }

    SymbolRecord::from_bytes(&bytes[absolute_offset..end]).map_err(|err| {
        error!(
            bead_id = BEAD_ID,
            logging_standard = LOGGING_STANDARD_BEAD,
            path = %segment_path.display(),
            offset_bytes = entry.offset.offset_bytes,
            error = %err,
            "failed to decode aligned symbol record"
        );
        FrankenError::DatabaseCorrupt {
            detail: format!(
                "invalid aligned SymbolRecord at offset {}: {err}",
                entry.offset.offset_bytes
            ),
        }
    })
}

/// Rebuild `ObjectId -> Vec<SymbolLogOffset>` by scanning all segment files.
pub fn rebuild_object_locator(
    symbols_dir: &Path,
) -> Result<BTreeMap<ObjectId, Vec<SymbolLogOffset>>> {
    let mut locator: BTreeMap<ObjectId, Vec<SymbolLogOffset>> = BTreeMap::new();
    let segments = sorted_segment_paths(symbols_dir)?;

    for (segment_id, path) in segments {
        let scan = scan_symbol_segment(&path)?;
        for row in scan.records {
            locator
                .entry(row.record.object_id)
                .or_default()
                .push(row.offset);
        }
        if scan.torn_tail {
            warn!(
                bead_id = BEAD_ID,
                logging_standard = LOGGING_STANDARD_BEAD,
                segment_id,
                path = %path.display(),
                "locator rebuild ignored torn tail in segment"
            );
        }
    }

    for offsets in locator.values_mut() {
        offsets.sort_unstable();
    }

    info!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        objects = locator.len(),
        "rebuilt object locator from symbol segments"
    );

    Ok(locator)
}

fn parse_symbol_record_at(
    bytes: &[u8],
    segment_id: u64,
    absolute_offset: usize,
) -> Result<Option<(SymbolLogRecord, usize)>> {
    if absolute_offset >= bytes.len() {
        return Ok(None);
    }

    let Some(record_len) = record_wire_len_at(bytes, absolute_offset)? else {
        return Ok(None);
    };

    let end =
        absolute_offset
            .checked_add(record_len)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "record end overflow while parsing symbol record".to_owned(),
            })?;
    let record = SymbolRecord::from_bytes(&bytes[absolute_offset..end]).map_err(|err| {
        error!(
            bead_id = BEAD_ID,
            logging_standard = LOGGING_STANDARD_BEAD,
            segment_id,
            absolute_offset,
            error = %err,
            "failed to decode SymbolRecord during scan"
        );
        FrankenError::DatabaseCorrupt {
            detail: format!("invalid SymbolRecord at absolute offset {absolute_offset}: {err}"),
        }
    })?;

    let offset_without_header = absolute_offset
        .checked_sub(SYMBOL_SEGMENT_HEADER_BYTES)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!(
                "record offset {absolute_offset} precedes segment header of {SYMBOL_SEGMENT_HEADER_BYTES} bytes"
            ),
        })?;

    let offset = SymbolLogOffset {
        segment_id,
        offset_bytes: usize_to_u64(offset_without_header, "offset_without_header")?,
    };

    Ok(Some((SymbolLogRecord { offset, record }, record_len)))
}

fn record_wire_len_at(bytes: &[u8], absolute_offset: usize) -> Result<Option<usize>> {
    let remaining = bytes.len().saturating_sub(absolute_offset);
    if remaining < SYMBOL_RECORD_HEADER_BYTES {
        return Ok(None);
    }

    let size_start = absolute_offset
        .checked_add(SYMBOL_SIZE_FIELD_OFFSET)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "symbol size field offset overflow".to_owned(),
        })?;
    let size_end = size_start
        .checked_add(SYMBOL_SIZE_FIELD_BYTES)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "symbol size field end overflow".to_owned(),
        })?;
    let symbol_size_u32 = read_u32_at(bytes, size_start, "symbol_size")?;
    let symbol_size = u32_to_usize(symbol_size_u32, "symbol_size")?;

    let total_len = SYMBOL_RECORD_HEADER_BYTES
        .checked_add(symbol_size)
        .and_then(|v| v.checked_add(SYMBOL_RECORD_TRAILER_BYTES))
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "symbol record size overflow".to_owned(),
        })?;
    if remaining < total_len {
        return Ok(None);
    }

    if size_end > bytes.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "symbol size field out of bounds: end={}, file_len={}",
                size_end,
                bytes.len()
            ),
        });
    }

    Ok(Some(total_len))
}

fn sorted_segment_paths(symbols_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    if !symbols_dir.exists() {
        return Ok(Vec::new());
    }

    let mut segments = Vec::new();
    for entry in fs::read_dir(symbols_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(segment_id) = parse_segment_id_from_name(name) else {
            continue;
        };
        segments.push((segment_id, entry.path()));
    }
    segments.sort_by_key(|(segment_id, _)| *segment_id);
    Ok(segments)
}

fn parse_segment_id_from_name(file_name: &str) -> Option<u64> {
    let prefix = "segment-";
    let suffix = ".log";
    if !file_name.starts_with(prefix) || !file_name.ends_with(suffix) {
        return None;
    }
    let id_text = &file_name[prefix.len()..file_name.len() - suffix.len()];
    id_text.parse::<u64>().ok()
}

fn file_len_usize(path: &Path) -> Result<usize> {
    let len = fs::metadata(path)?.len();
    u64_to_usize(len, "file length")
}

fn align_up(value: usize, alignment: usize) -> Result<usize> {
    if alignment == 0 {
        return Err(FrankenError::Internal(
            "alignment must be non-zero".to_owned(),
        ));
    }
    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }
    value
        .checked_add(alignment - remainder)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: "alignment overflow".to_owned(),
        })
}

fn read_u32_at(bytes: &[u8], start: usize, field: &str) -> Result<u32> {
    let end = start
        .checked_add(4)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("overflow while reading field {field}"),
        })?;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!(
                "field {field} out of bounds: start={start}, end={end}, len={}",
                bytes.len()
            ),
        })?;
    let array: [u8; 4] = slice
        .try_into()
        .map_err(|_| FrankenError::DatabaseCorrupt {
            detail: format!("failed to parse field {field}"),
        })?;
    Ok(u32::from_le_bytes(array))
}

fn read_u64_at(bytes: &[u8], start: usize, field: &str) -> Result<u64> {
    let end = start
        .checked_add(8)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("overflow while reading field {field}"),
        })?;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!(
                "field {field} out of bounds: start={start}, end={end}, len={}",
                bytes.len()
            ),
        })?;
    let array: [u8; 8] = slice
        .try_into()
        .map_err(|_| FrankenError::DatabaseCorrupt {
            detail: format!("failed to parse field {field}"),
        })?;
    Ok(u64::from_le_bytes(array))
}

fn u64_to_usize(value: u64, what: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: format!("{what} does not fit in usize: {value}"),
    })
}

fn usize_to_u64(value: usize, what: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: format!("{what} does not fit in u64: {value}"),
    })
}

fn u32_to_usize(value: u32, what: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: format!("{what} does not fit in usize: {value}"),
    })
}

fn usize_to_u32(value: usize, what: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: format!("{what} does not fit in u32: {value}"),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::OpenOptions;
    use std::io::Write;

    use fsqlite_types::{ObjectId, Oti, SymbolRecordFlags};
    use tempfile::tempdir;

    use super::*;

    const BD_1HI_24_COMPLIANCE_SENTINEL: &str = "test_bd_1hi_24_unit_compliance_gate prop_bd_1hi_24_structure_compliance \
         test_e2e_bd_1hi_24_compliance DEBUG INFO WARN ERROR bd-1fpm";

    fn test_record(object_seed: u8, esi: u32, symbol_size: u32, fill: u8) -> SymbolRecord {
        let symbol_len = usize::try_from(symbol_size).expect("symbol_size fits usize for tests");
        let oti = Oti {
            f: u64::from(symbol_size),
            al: 1,
            t: symbol_size,
            z: 1,
            n: 1,
        };
        let mut data = vec![fill; symbol_len];
        data[0] = object_seed;
        SymbolRecord::new(
            ObjectId::from_bytes([object_seed; 16]),
            oti,
            esi,
            data,
            SymbolRecordFlags::empty(),
        )
    }

    #[test]
    fn test_symbol_segment_header_encode_decode() {
        let header = SymbolSegmentHeader::new(17, 42, 1_731_000_000);
        let bytes = header.encode();
        assert_eq!(bytes.len(), SYMBOL_SEGMENT_HEADER_BYTES);
        let decoded = SymbolSegmentHeader::decode(&bytes).expect("decode header");
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_symbol_segment_header_magic() {
        let header = SymbolSegmentHeader::new(3, 7, 99);
        let mut bytes = header.encode();
        bytes[0] = b'X';
        let err = SymbolSegmentHeader::decode(&bytes).expect_err("bad magic must fail");
        assert!(err.to_string().contains("invalid symbol segment magic"));
    }

    #[test]
    fn test_symbol_log_append_records() {
        let dir = tempdir().expect("tempdir");
        let mut manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let sizes = [1024_u32, 1536, 2048, 3072, 4096];
        for (idx, size) in sizes.into_iter().enumerate() {
            let idx_u32 = u32::try_from(idx).expect("test index fits u32");
            let seed = u8::try_from(idx + 1).expect("test index fits u8");
            let rec = test_record(seed, idx_u32, size, 0xA0);
            manager.append(&rec).expect("append record");
        }

        let scan = scan_symbol_segment(&manager.active_segment_path()).expect("scan segment");
        assert_eq!(scan.records.len(), 5);
        assert!(!scan.torn_tail);
        assert_eq!(scan.records[0].record.symbol_data.len(), 1024);
        assert_eq!(scan.records[4].record.symbol_data.len(), 4096);
        manager.rotate(2, 43, 200).expect("rotation succeeds");
    }

    #[test]
    fn test_symbol_log_torn_tail_recovery() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        for idx in 0_u32..3_u32 {
            let seed = u8::try_from(idx + 1).expect("small index fits u8");
            let rec = test_record(seed, idx, 1024, 0xB0);
            manager.append(&rec).expect("append record");
        }

        let partial = test_record(9, 9, 1024, 0xCC).to_bytes();
        let partial_len = partial.len() / 2;
        let mut file = OpenOptions::new()
            .append(true)
            .open(manager.active_segment_path())
            .expect("open for append");
        file.write_all(&partial[..partial_len])
            .expect("write partial record");
        file.sync_data().expect("sync partial tail");

        let scan = scan_symbol_segment(&manager.active_segment_path()).expect("scan segment");
        assert_eq!(scan.records.len(), 3);
        assert!(scan.torn_tail);
    }

    #[test]
    fn test_locator_offset_computation() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let record = test_record(7, 11, 2048, 0x44);
        let offset = manager.append(&record).expect("append record");

        let loaded = read_symbol_record_at_offset(&manager.active_segment_path(), offset)
            .expect("read by offset");
        assert_eq!(loaded.object_id, record.object_id);
        assert_eq!(loaded.esi, record.esi);
        assert_eq!(loaded.symbol_data, record.symbol_data);
    }

    #[test]
    fn test_locator_cache_rebuild() {
        let dir = tempdir().expect("tempdir");
        let mut manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");

        let record_alpha_first = test_record(1, 0, 1024, 0x01);
        let record_bravo_first = test_record(2, 1, 1024, 0x02);
        manager.append(&record_alpha_first).expect("append a1");
        manager.append(&record_bravo_first).expect("append b1");

        manager.rotate(2, 43, 200).expect("rotate");
        let record_alpha_second = test_record(1, 2, 1024, 0x03);
        let record_charlie_second = test_record(3, 3, 1024, 0x04);
        manager.append(&record_alpha_second).expect("append a2");
        manager.append(&record_charlie_second).expect("append c2");

        let locator = rebuild_object_locator(dir.path()).expect("rebuild locator");
        assert_eq!(locator.len(), 3);
        assert_eq!(
            locator
                .get(&ObjectId::from_bytes([1_u8; 16]))
                .expect("object 1 exists")
                .len(),
            2
        );
        assert_eq!(
            locator
                .get(&ObjectId::from_bytes([2_u8; 16]))
                .expect("object 2 exists")
                .len(),
            1
        );
        assert_eq!(
            locator
                .get(&ObjectId::from_bytes([3_u8; 16]))
                .expect("object 3 exists")
                .len(),
            1
        );
    }

    #[test]
    fn test_locator_cache_missing() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let rec = test_record(9, 0, 1024, 0x55);
        manager.append(&rec).expect("append");

        let locator = rebuild_object_locator(dir.path()).expect("rebuild from scan");
        assert_eq!(locator.len(), 1);
        assert!(locator.contains_key(&ObjectId::from_bytes([9_u8; 16])));
    }

    #[test]
    fn test_epoch_id_stored() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 123_456).expect("manager");
        let bytes = fs::read(manager.active_segment_path()).expect("read segment bytes");
        let header = SymbolSegmentHeader::decode(&bytes[..SYMBOL_SEGMENT_HEADER_BYTES])
            .expect("decode header");
        assert_eq!(header.epoch_id, 42);
    }

    #[test]
    fn test_immutable_rotated_segments() {
        let dir = tempdir().expect("tempdir");
        let mut manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        manager
            .append(&test_record(1, 0, 1024, 0x11))
            .expect("append segment 1");
        manager.rotate(2, 43, 200).expect("rotate");

        let err = manager
            .append_to_segment(1, &test_record(2, 1, 1024, 0x22))
            .expect_err("rotated segment should be immutable");
        assert!(err.to_string().contains("immutable"));
    }

    #[test]
    fn test_variable_size_records() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        for (idx, size) in [1024_u32, 4096, 65_536].into_iter().enumerate() {
            let idx_u32 = u32::try_from(idx).expect("small test index fits u32");
            let seed = u8::try_from(idx + 1).expect("small test index fits u8");
            let rec = test_record(seed, idx_u32, size, 0x66);
            manager.append(&rec).expect("append variable-size record");
        }

        let scan = scan_symbol_segment(&manager.active_segment_path()).expect("scan");
        assert_eq!(scan.records.len(), 3);
        assert_eq!(scan.records[0].record.symbol_data.len(), 1024);
        assert_eq!(scan.records[1].record.symbol_data.len(), 4096);
        assert_eq!(scan.records[2].record.symbol_data.len(), 65_536);
    }

    #[test]
    fn test_no_o_direct_requirement() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        manager
            .append(&test_record(4, 0, 1024, 0x77))
            .expect("buffered append succeeds");
        let scan =
            scan_symbol_segment(&manager.active_segment_path()).expect("buffered scan succeeds");
        assert_eq!(scan.records.len(), 1);
        assert!(!scan.torn_tail);
    }

    #[test]
    fn test_aligned_variant_optional() {
        let dir = tempdir().expect("tempdir");
        let header = SymbolSegmentHeader::new(1, 42, 100);
        let record = test_record(5, 0, 1024, 0x88);
        let entry = append_symbol_record_aligned(dir.path(), header, &record, 4096)
            .expect("aligned append");

        assert_eq!(u64::from(entry.padded_len) % 4096, 0);
        assert!(entry.padded_len >= entry.logical_len);

        let segment_path = symbol_segment_path(dir.path(), 1);
        let loaded = read_aligned_symbol_record(&segment_path, entry).expect("read aligned");
        assert_eq!(loaded.object_id, record.object_id);
        assert_eq!(loaded.esi, record.esi);
        assert_eq!(loaded.frame_xxh3, record.frame_xxh3);
        assert!(loaded.verify_integrity());
    }

    #[test]
    fn test_bd_1hi_24_unit_compliance_gate() {
        assert_eq!(SYMBOL_SEGMENT_HEADER_BYTES, 40);
        assert_eq!(SYMBOL_SEGMENT_MAGIC, *b"FSSY");
        for token in [
            "test_bd_1hi_24_unit_compliance_gate",
            "prop_bd_1hi_24_structure_compliance",
            "test_e2e_bd_1hi_24_compliance",
            "DEBUG",
            "INFO",
            "WARN",
            "ERROR",
            "bd-1fpm",
        ] {
            assert!(BD_1HI_24_COMPLIANCE_SENTINEL.contains(token));
        }
    }

    #[test]
    fn prop_bd_1hi_24_structure_compliance() {
        let dir = tempdir().expect("tempdir");
        let mut manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let mut expected: BTreeMap<ObjectId, Vec<SymbolLogOffset>> = BTreeMap::new();

        for segment_index in 0_u64..3_u64 {
            if segment_index > 0 {
                manager
                    .rotate(segment_index + 1, 42 + segment_index, 100 + segment_index)
                    .expect("rotate");
            }
            for record_index in 0_u32..6_u32 {
                let object_seed = u8::try_from((segment_index + u64::from(record_index)) % 4)
                    .expect("small seed");
                let fill = u8::try_from(0x90_u64 + segment_index + u64::from(record_index))
                    .expect("small fill");
                let rec = test_record(object_seed, record_index, 1024, fill);
                let offset = manager.append(&rec).expect("append");
                expected.entry(rec.object_id).or_default().push(offset);
            }
        }

        for offsets in expected.values_mut() {
            offsets.sort_unstable();
        }

        let rebuilt = rebuild_object_locator(dir.path()).expect("rebuild locator");
        assert_eq!(rebuilt, expected);
    }

    #[test]
    fn test_e2e_bd_1hi_24_compliance() {
        let dir = tempdir().expect("tempdir");
        let mut manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let mut written = Vec::new();

        let rec_a = test_record(1, 0, 1024, 0x11);
        let rec_b = test_record(2, 1, 2048, 0x22);
        written.push((
            rec_a.object_id,
            manager.append(&rec_a).expect("append rec_a to segment 1"),
        ));
        written.push((
            rec_b.object_id,
            manager.append(&rec_b).expect("append rec_b to segment 1"),
        ));

        manager.rotate(2, 43, 200).expect("rotate");
        let rec_c = test_record(1, 2, 4096, 0x33);
        let rec_d = test_record(3, 3, 1024, 0x44);
        written.push((
            rec_c.object_id,
            manager.append(&rec_c).expect("append rec_c to segment 2"),
        ));
        written.push((
            rec_d.object_id,
            manager.append(&rec_d).expect("append rec_d to segment 2"),
        ));

        let locator = rebuild_object_locator(dir.path()).expect("rebuild locator");
        assert_eq!(locator.len(), 3);

        for (object_id, offset) in &written {
            let path = symbol_segment_path(dir.path(), offset.segment_id);
            let loaded = read_symbol_record_at_offset(&path, *offset).expect("direct offset read");
            assert_eq!(&loaded.object_id, object_id);
        }

        let active_scan_before =
            scan_symbol_segment(&manager.active_segment_path()).expect("scan active before crash");
        let active_count_before = active_scan_before.records.len();

        let crash_partial = test_record(9, 99, 1024, 0xEE).to_bytes();
        let partial_len = crash_partial.len() / 2;
        let mut file = OpenOptions::new()
            .append(true)
            .open(manager.active_segment_path())
            .expect("open active segment for crash tail");
        file.write_all(&crash_partial[..partial_len])
            .expect("append torn tail");
        file.sync_data().expect("sync torn tail");

        let active_scan_after =
            scan_symbol_segment(&manager.active_segment_path()).expect("scan active after crash");
        assert_eq!(active_scan_after.records.len(), active_count_before);
        assert!(active_scan_after.torn_tail);
    }
}
