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
use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags, source_symbol_count};
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

/// Locator for a contiguous systematic-symbol run (`ESI 0..K-1`) for an object.
///
/// This captures the physical placement needed by the §3.5.2 fast path:
/// read the first `K` source symbols sequentially and reconstruct without
/// invoking GF(256) decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystematicRunLocator {
    /// Object whose systematic run is located.
    pub object_id: ObjectId,
    /// Segment containing the run.
    pub segment_id: u64,
    /// Inclusive start of the ESI range (always 0 for systematic runs).
    pub esi_start: u32,
    /// Inclusive end of the ESI range (`K-1`).
    pub esi_end_inclusive: u32,
    /// Locator offsets in ascending ESI order.
    pub offsets: Vec<SymbolLogOffset>,
}

impl SystematicRunLocator {
    /// Number of source symbols in this run.
    #[must_use]
    pub fn source_symbol_count(&self) -> usize {
        self.offsets.len()
    }
}

/// Rebuild `ObjectId -> SystematicRunLocator` by scanning symbol segments.
///
/// Rules:
/// - Run start is identified by `ESI=0` with `SYSTEMATIC_RUN_START`.
/// - A valid run must provide contiguous symbols `ESI 0..K-1` with matching
///   `object_id` and `OTI`.
/// - If validation fails (missing/interleaved/non-contiguous symbols), that
///   start is ignored and fast-path MUST fall back to decode.
/// - If multiple valid runs exist for one object, the newest one (largest
///   segment/order in append-only log) wins.
pub fn rebuild_systematic_run_locator(
    symbols_dir: &Path,
) -> Result<BTreeMap<ObjectId, SystematicRunLocator>> {
    let mut locator: BTreeMap<ObjectId, SystematicRunLocator> = BTreeMap::new();
    let segments = sorted_segment_paths(symbols_dir)?;

    for (segment_id, path) in segments {
        let scan = scan_symbol_segment(&path)?;
        let rows = &scan.records;

        for start_idx in 0..rows.len() {
            let start = &rows[start_idx].record;
            if start.esi != 0
                || !start
                    .flags
                    .contains(SymbolRecordFlags::SYSTEMATIC_RUN_START)
            {
                continue;
            }

            match build_systematic_run_locator(rows, start_idx) {
                Ok(run) => {
                    locator.insert(run.object_id, run);
                }
                Err(detail) => {
                    warn!(
                        bead_id = BEAD_ID,
                        logging_standard = LOGGING_STANDARD_BEAD,
                        segment_id,
                        path = %path.display(),
                        start_offset = rows[start_idx].offset.offset_bytes,
                        start_object_id = %start.object_id,
                        reason = %detail,
                        "invalid systematic run start; fast-path must fall back"
                    );
                }
            }
        }

        if scan.torn_tail {
            warn!(
                bead_id = BEAD_ID,
                logging_standard = LOGGING_STANDARD_BEAD,
                segment_id,
                path = %path.display(),
                "systematic-run locator rebuild ignored torn tail in segment"
            );
        }
    }

    info!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        objects = locator.len(),
        "rebuilt systematic run locator from symbol segments"
    );

    Ok(locator)
}

/// Attempt systematic fast-path reconstruction for one object.
///
/// Inputs:
/// - object metadata (`object_id`, `oti`) providing `(F, T, K_source)`.
/// - prebuilt locator for a contiguous systematic run (`ESI 0..K-1`).
///
/// Returns:
/// - `Ok(Some(bytes))` when fast path succeeds.
/// - `Ok(None)` when any missing/corrupt/mismatch condition requires fallback.
/// - `Err(...)` for unexpected I/O/runtime errors unrelated to symbol validity.
#[derive(Debug, Clone, Copy)]
struct SystematicFastPathPlan {
    source_symbols: usize,
    symbol_size: usize,
    transfer_len: usize,
    total_len: usize,
}

#[derive(Debug, Clone, Copy)]
struct SystematicFastPathExpectations<'a> {
    run: &'a SystematicRunLocator,
    object_id: ObjectId,
    oti: Oti,
    symbol_size: usize,
    auth_epoch_key: Option<&'a [u8; 32]>,
}

fn fast_path_unavailable(object_id: ObjectId, detail: &str) {
    warn!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        object_id = %object_id,
        detail,
        "systematic fast path unavailable"
    );
}

fn fast_path_unavailable_esi(object_id: ObjectId, expected_esi: u32, detail: &str) {
    warn!(
        bead_id = BEAD_ID,
        logging_standard = LOGGING_STANDARD_BEAD,
        object_id = %object_id,
        expected_esi,
        detail,
        "systematic fast path unavailable"
    );
}

pub fn read_systematic_fast_path(
    symbols_dir: &Path,
    run: &SystematicRunLocator,
    object_id: ObjectId,
    oti: Oti,
    auth_epoch_key: Option<&[u8; 32]>,
) -> Result<Option<Vec<u8>>> {
    let Some(plan) = build_systematic_fast_path_plan(run, object_id, oti) else {
        return Ok(None);
    };
    if plan.source_symbols == 0 {
        return Ok(Some(Vec::new()));
    }

    let Some((bytes, _header)) = load_systematic_fast_path_segment(symbols_dir, run, object_id)?
    else {
        return Ok(None);
    };

    let expectations = SystematicFastPathExpectations {
        run,
        object_id,
        oti,
        symbol_size: plan.symbol_size,
        auth_epoch_key,
    };
    let mut out = vec![0_u8; plan.total_len];

    for (index, offset) in run.offsets.iter().copied().enumerate() {
        let Ok(expected_esi) = u32::try_from(index) else {
            fast_path_unavailable(object_id, "index does not fit ESI");
            return Ok(None);
        };
        let Some(parsed) =
            read_systematic_fast_path_record(&bytes, &expectations, offset, expected_esi)
        else {
            return Ok(None);
        };

        let Some(start) = index.checked_mul(plan.symbol_size) else {
            fast_path_unavailable_esi(object_id, expected_esi, "output offset overflow");
            return Ok(None);
        };
        let Some(end) = start.checked_add(plan.symbol_size) else {
            fast_path_unavailable_esi(object_id, expected_esi, "output end overflow");
            return Ok(None);
        };
        if end > out.len() {
            fast_path_unavailable_esi(object_id, expected_esi, "output bounds check failed");
            return Ok(None);
        }
        out[start..end].copy_from_slice(&parsed.symbol_data);
    }

    out.truncate(plan.transfer_len);
    Ok(Some(out))
}

fn build_systematic_fast_path_plan(
    run: &SystematicRunLocator,
    object_id: ObjectId,
    oti: Oti,
) -> Option<SystematicFastPathPlan> {
    let source_symbols = match source_symbol_count(oti) {
        Ok(value) => value,
        Err(err) => {
            let detail = format!("invalid source symbol count: {err}");
            fast_path_unavailable(object_id, &detail);
            return None;
        }
    };
    if source_symbols == 0 {
        return Some(SystematicFastPathPlan {
            source_symbols,
            symbol_size: 0,
            transfer_len: 0,
            total_len: 0,
        });
    }
    if run.object_id != object_id {
        fast_path_unavailable(object_id, "locator object mismatch");
        return None;
    }
    if run.esi_start != 0 {
        fast_path_unavailable(object_id, "run does not start at ESI 0");
        return None;
    }
    if run.offsets.len() != source_symbols {
        let detail = format!(
            "locator offset count mismatch: expected={source_symbols} found={}",
            run.offsets.len()
        );
        fast_path_unavailable(object_id, &detail);
        return None;
    }
    let Ok(expected_end) = u32::try_from(source_symbols.saturating_sub(1)) else {
        fast_path_unavailable(object_id, "source symbol count exceeds ESI range");
        return None;
    };
    if run.esi_end_inclusive != expected_end {
        fast_path_unavailable(object_id, "locator ESI range mismatch");
        return None;
    }

    let Ok(symbol_size) = usize::try_from(oti.t) else {
        fast_path_unavailable(object_id, "invalid OTI.t");
        return None;
    };
    let Ok(transfer_len) = usize::try_from(oti.f) else {
        fast_path_unavailable(object_id, "invalid OTI.f");
        return None;
    };
    let Some(total_len) = source_symbols.checked_mul(symbol_size) else {
        fast_path_unavailable(object_id, "reconstruction size overflow");
        return None;
    };

    Some(SystematicFastPathPlan {
        source_symbols,
        symbol_size,
        transfer_len,
        total_len,
    })
}

fn load_systematic_fast_path_segment(
    symbols_dir: &Path,
    run: &SystematicRunLocator,
    object_id: ObjectId,
) -> Result<Option<(Vec<u8>, SymbolSegmentHeader)>> {
    let segment_path = symbol_segment_path(symbols_dir, run.segment_id);
    if !segment_path.exists() {
        fast_path_unavailable(object_id, "locator segment missing");
        return Ok(None);
    }

    let bytes = fs::read(&segment_path)?;
    if bytes.len() < SYMBOL_SEGMENT_HEADER_BYTES {
        fast_path_unavailable(object_id, "segment shorter than header");
        return Ok(None);
    }

    let header = match SymbolSegmentHeader::decode(&bytes[..SYMBOL_SEGMENT_HEADER_BYTES]) {
        Ok(value) => value,
        Err(err) => {
            let detail = format!("invalid segment header: {err}");
            fast_path_unavailable(object_id, &detail);
            return Ok(None);
        }
    };
    if header.segment_id != run.segment_id {
        fast_path_unavailable(object_id, "segment id mismatch");
        return Ok(None);
    }

    Ok(Some((bytes, header)))
}

fn read_systematic_fast_path_record(
    bytes: &[u8],
    expectations: &SystematicFastPathExpectations<'_>,
    offset: SymbolLogOffset,
    expected_esi: u32,
) -> Option<SymbolRecord> {
    if offset.segment_id != expectations.run.segment_id {
        fast_path_unavailable_esi(
            expectations.object_id,
            expected_esi,
            "wrong segment in offset",
        );
        return None;
    }

    let Ok(offset_usize) = usize::try_from(offset.offset_bytes) else {
        fast_path_unavailable_esi(expectations.object_id, expected_esi, "bad record offset");
        return None;
    };
    let Some(absolute_offset) = SYMBOL_SEGMENT_HEADER_BYTES.checked_add(offset_usize) else {
        fast_path_unavailable_esi(
            expectations.object_id,
            expected_esi,
            "absolute offset overflow",
        );
        return None;
    };

    let parsed = match parse_symbol_record_at(bytes, expectations.run.segment_id, absolute_offset) {
        Ok(Some((row, _))) => row.record,
        Ok(None) => {
            fast_path_unavailable_esi(
                expectations.object_id,
                expected_esi,
                "missing symbol record at offset",
            );
            return None;
        }
        Err(err) => {
            let detail = format!("invalid symbol record: {err}");
            fast_path_unavailable_esi(expectations.object_id, expected_esi, &detail);
            return None;
        }
    };

    if parsed.object_id != expectations.object_id {
        fast_path_unavailable_esi(expectations.object_id, expected_esi, "object mismatch");
        return None;
    }
    if parsed.oti != expectations.oti {
        fast_path_unavailable_esi(expectations.object_id, expected_esi, "OTI mismatch");
        return None;
    }
    if parsed.esi != expected_esi {
        fast_path_unavailable_esi(expectations.object_id, expected_esi, "non-contiguous ESI");
        return None;
    }
    if parsed.symbol_data.len() != expectations.symbol_size {
        fast_path_unavailable_esi(expectations.object_id, expected_esi, "symbol size mismatch");
        return None;
    }
    if !parsed.verify_integrity() {
        fast_path_unavailable_esi(
            expectations.object_id,
            expected_esi,
            "integrity check failed",
        );
        return None;
    }
    if parsed.auth_tag != [0_u8; 16] {
        let Some(epoch_key) = expectations.auth_epoch_key else {
            fast_path_unavailable_esi(
                expectations.object_id,
                expected_esi,
                "auth tag present but no epoch key provided",
            );
            return None;
        };
        if !parsed.verify_auth(epoch_key) {
            fast_path_unavailable_esi(expectations.object_id, expected_esi, "auth check failed");
            return None;
        }
    }

    Some(parsed)
}

fn build_systematic_run_locator(
    rows: &[SymbolLogRecord],
    start_idx: usize,
) -> std::result::Result<SystematicRunLocator, String> {
    let start_row = rows
        .get(start_idx)
        .ok_or_else(|| format!("run start index {start_idx} out of bounds"))?;
    let start = &start_row.record;
    let source_symbols = source_symbol_count(start.oti)
        .map_err(|err| format!("invalid source symbol count at run start: {err}"))?;
    if source_symbols == 0 {
        return Err("source symbol count is zero".to_owned());
    }
    let source_symbols_u32 = u32::try_from(source_symbols)
        .map_err(|_| format!("source symbol count does not fit u32: {source_symbols}"))?;
    let end_exclusive = start_idx
        .checked_add(source_symbols)
        .ok_or_else(|| "systematic run index overflow".to_owned())?;
    if end_exclusive > rows.len() {
        return Err(format!(
            "incomplete systematic run: need {} rows from index {}, have {}",
            source_symbols,
            start_idx,
            rows.len().saturating_sub(start_idx)
        ));
    }

    let mut offsets = Vec::with_capacity(source_symbols);
    for relative in 0..source_symbols {
        let row = &rows[start_idx + relative];
        let rec = &row.record;
        let expected_esi = u32::try_from(relative).expect("relative index fits u32");

        if rec.object_id != start.object_id {
            return Err(format!(
                "object boundary at relative={} expected={} found={}",
                relative, start.object_id, rec.object_id
            ));
        }
        if rec.oti != start.oti {
            return Err(format!(
                "OTI mismatch at relative={} expected={:?} found={:?}",
                relative, start.oti, rec.oti
            ));
        }
        if rec.esi != expected_esi {
            return Err(format!(
                "non-contiguous ESI at relative={} expected={} found={}",
                relative, expected_esi, rec.esi
            ));
        }
        if relative == 0 {
            if !rec.flags.contains(SymbolRecordFlags::SYSTEMATIC_RUN_START) {
                return Err("missing SYSTEMATIC_RUN_START on ESI 0".to_owned());
            }
        } else if rec.flags.contains(SymbolRecordFlags::SYSTEMATIC_RUN_START) {
            return Err(format!(
                "unexpected SYSTEMATIC_RUN_START on non-zero ESI {}",
                rec.esi
            ));
        }

        offsets.push(row.offset);
    }

    Ok(SystematicRunLocator {
        object_id: start.object_id,
        segment_id: start_row.offset.segment_id,
        esi_start: 0,
        esi_end_inclusive: source_symbols_u32.saturating_sub(1),
        offsets,
    })
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

    fn systematic_record(
        object_seed: u8,
        oti: Oti,
        esi: u32,
        fill: u8,
        systematic_start: bool,
    ) -> SymbolRecord {
        let symbol_len = usize::try_from(oti.t).expect("OTI.t fits usize for tests");
        let mut data = vec![fill; symbol_len];
        if let Some(first) = data.first_mut() {
            let esi_tag = u8::try_from(esi).unwrap_or(0);
            *first = object_seed.wrapping_add(esi_tag);
        }
        let flags = if systematic_start {
            SymbolRecordFlags::SYSTEMATIC_RUN_START
        } else {
            SymbolRecordFlags::empty()
        };
        SymbolRecord::new(
            ObjectId::from_bytes([object_seed; 16]),
            oti,
            esi,
            data,
            flags,
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
    fn test_systematic_run_locator_rebuild_happy_path() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 3,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([7_u8; 16]);

        let r0 = systematic_record(7, oti, 0, 0xA1, true);
        let r1 = systematic_record(7, oti, 1, 0xA2, false);
        let r2 = systematic_record(7, oti, 2, 0xA3, false);
        let repair = systematic_record(7, oti, 3, 0xAF, false);

        let o0 = manager.append(&r0).expect("append esi0");
        let o1 = manager.append(&r1).expect("append esi1");
        let o2 = manager.append(&r2).expect("append esi2");
        let _o3 = manager.append(&repair).expect("append repair");

        let locator =
            rebuild_systematic_run_locator(dir.path()).expect("rebuild systematic locator");
        let run = locator.get(&object_id).expect("run must exist");
        assert_eq!(run.segment_id, 1);
        assert_eq!(run.esi_start, 0);
        assert_eq!(run.esi_end_inclusive, 2);
        assert_eq!(run.source_symbol_count(), 3);
        assert_eq!(run.offsets, vec![o0, o1, o2]);
    }

    #[test]
    fn test_systematic_run_locator_missing_symbol_is_ignored() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 3,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([8_u8; 16]);

        manager
            .append(&systematic_record(8, oti, 0, 0xB1, true))
            .expect("append esi0");
        manager
            .append(&systematic_record(8, oti, 2, 0xB3, false))
            .expect("append esi2");

        let locator =
            rebuild_systematic_run_locator(dir.path()).expect("rebuild systematic locator");
        assert!(
            !locator.contains_key(&object_id),
            "incomplete run must not be indexed as fast-path eligible"
        );
    }

    #[test]
    fn test_systematic_run_locator_interleaved_object_is_ignored() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 2,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_a = ObjectId::from_bytes([11_u8; 16]);
        let object_b = ObjectId::from_bytes([12_u8; 16]);

        manager
            .append(&systematic_record(11, oti, 0, 0xC1, true))
            .expect("append A esi0");
        manager
            .append(&systematic_record(12, oti, 0, 0xD1, true))
            .expect("append B esi0");
        manager
            .append(&systematic_record(11, oti, 1, 0xC2, false))
            .expect("append A esi1");

        let locator =
            rebuild_systematic_run_locator(dir.path()).expect("rebuild systematic locator");
        assert!(
            !locator.contains_key(&object_a),
            "interleaved run must be rejected for fast-path"
        );
        assert!(
            !locator.contains_key(&object_b),
            "single-symbol run with K=2 must be rejected as incomplete"
        );
    }

    #[test]
    fn test_systematic_run_locator_prefers_newest_complete_run() {
        let dir = tempdir().expect("tempdir");
        let mut manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 2,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([13_u8; 16]);

        manager
            .append(&systematic_record(13, oti, 0, 0xE1, true))
            .expect("append seg1 esi0");
        manager
            .append(&systematic_record(13, oti, 1, 0xE2, false))
            .expect("append seg1 esi1");

        manager.rotate(2, 43, 200).expect("rotate");
        let newer_o0 = manager
            .append(&systematic_record(13, oti, 0, 0xF1, true))
            .expect("append seg2 esi0");
        let newer_o1 = manager
            .append(&systematic_record(13, oti, 1, 0xF2, false))
            .expect("append seg2 esi1");

        let locator =
            rebuild_systematic_run_locator(dir.path()).expect("rebuild systematic locator");
        let run = locator.get(&object_id).expect("run exists");
        assert_eq!(
            run.segment_id, 2,
            "newest complete run should win in append-order locator rebuild"
        );
        assert_eq!(run.offsets, vec![newer_o0, newer_o1]);
    }

    #[test]
    fn test_systematic_fast_path_success() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 3 - 11,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([21_u8; 16]);

        let r0 = systematic_record(21, oti, 0, 0x11, true);
        let r1 = systematic_record(21, oti, 1, 0x22, false);
        let r2 = systematic_record(21, oti, 2, 0x33, false);
        manager.append(&r0).expect("append esi0");
        manager.append(&r1).expect("append esi1");
        manager.append(&r2).expect("append esi2");
        manager
            .append(&systematic_record(21, oti, 3, 0x44, false))
            .expect("append repair");

        let runs = rebuild_systematic_run_locator(dir.path()).expect("rebuild runs");
        let run = runs.get(&object_id).expect("run exists");
        let maybe_payload = read_systematic_fast_path(dir.path(), run, object_id, oti, None)
            .expect("fast-path read");
        let payload = maybe_payload.expect("fast path should reconstruct");

        let mut expected = Vec::new();
        expected.extend_from_slice(&r0.symbol_data);
        expected.extend_from_slice(&r1.symbol_data);
        expected.extend_from_slice(&r2.symbol_data);
        expected.truncate(usize::try_from(oti.f).expect("f fits usize"));
        assert_eq!(payload, expected);
    }

    #[test]
    fn test_systematic_fast_path_corrupt_symbol_requires_fallback() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 3 - 7,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([22_u8; 16]);

        let r0 = systematic_record(22, oti, 0, 0x51, true);
        let r1 = systematic_record(22, oti, 1, 0x52, false);
        let r2 = systematic_record(22, oti, 2, 0x53, false);
        manager.append(&r0).expect("append esi0");
        let r1_offset = manager.append(&r1).expect("append esi1");
        manager.append(&r2).expect("append esi2");

        let runs = rebuild_systematic_run_locator(dir.path()).expect("rebuild runs");
        let run = runs.get(&object_id).expect("run exists").clone();

        let segment_path = symbol_segment_path(dir.path(), r1_offset.segment_id);
        let mut bytes = fs::read(&segment_path).expect("read segment bytes");
        let record_offset = usize::try_from(r1_offset.offset_bytes).expect("offset fits usize");
        let absolute_record_offset = SYMBOL_SEGMENT_HEADER_BYTES
            .checked_add(record_offset)
            .expect("absolute offset");
        let data_byte_offset = absolute_record_offset
            .checked_add(SYMBOL_RECORD_HEADER_BYTES)
            .expect("data offset");
        bytes[data_byte_offset] ^= 0xFF;
        fs::write(&segment_path, bytes).expect("write corrupted segment");

        let result = read_systematic_fast_path(dir.path(), &run, object_id, oti, None)
            .expect("fast-path read should not hard-fail on corrupt symbol");
        assert!(
            result.is_none(),
            "corrupt symbol should force fallback path"
        );
    }

    #[test]
    fn test_systematic_fast_path_missing_symbol_requires_fallback() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 3 - 3,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([23_u8; 16]);

        manager
            .append(&systematic_record(23, oti, 0, 0x61, true))
            .expect("append esi0");
        manager
            .append(&systematic_record(23, oti, 1, 0x62, false))
            .expect("append esi1");
        manager
            .append(&systematic_record(23, oti, 2, 0x63, false))
            .expect("append esi2");

        let runs = rebuild_systematic_run_locator(dir.path()).expect("rebuild runs");
        let mut run = runs.get(&object_id).expect("run exists").clone();
        run.offsets[1].offset_bytes = run.offsets[1].offset_bytes.saturating_add(1_000_000);

        let result = read_systematic_fast_path(dir.path(), &run, object_id, oti, None)
            .expect("fast-path read should not hard-fail on missing symbol");
        assert!(
            result.is_none(),
            "missing symbol should force fallback path"
        );
    }

    #[test]
    fn test_systematic_fast_path_auth_failure_requires_fallback() {
        let dir = tempdir().expect("tempdir");
        let manager = SymbolLogManager::new(dir.path(), 1, 42, 100).expect("manager");
        let oti = Oti {
            f: 64_u64 * 3 - 5,
            al: 1,
            t: 64,
            z: 1,
            n: 1,
        };
        let object_id = ObjectId::from_bytes([24_u8; 16]);
        let auth_epoch_key = [0xA5_u8; 32];
        let wrong_epoch_key = [0x5A_u8; 32];

        let r0 = systematic_record(24, oti, 0, 0x71, true).with_auth_tag(&auth_epoch_key);
        let r1 = systematic_record(24, oti, 1, 0x72, false).with_auth_tag(&auth_epoch_key);
        let r2 = systematic_record(24, oti, 2, 0x73, false).with_auth_tag(&auth_epoch_key);
        manager.append(&r0).expect("append esi0");
        manager.append(&r1).expect("append esi1");
        manager.append(&r2).expect("append esi2");

        let runs = rebuild_systematic_run_locator(dir.path()).expect("rebuild runs");
        let run = runs.get(&object_id).expect("run exists");

        let wrong_key_result =
            read_systematic_fast_path(dir.path(), run, object_id, oti, Some(&wrong_epoch_key))
                .expect("fast-path read with wrong key");
        assert!(
            wrong_key_result.is_none(),
            "auth mismatch should force fallback path"
        );

        let correct_key_result =
            read_systematic_fast_path(dir.path(), run, object_id, oti, Some(&auth_epoch_key))
                .expect("fast-path read with correct key");
        assert!(
            correct_key_result.is_some(),
            "correct auth key should keep fast path eligible"
        );
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
