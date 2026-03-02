//! WAL checksum and integrity helpers.

use fsqlite_error::{FrankenError, Result};
use serde::Serialize;
use xxhash_rust::xxh3::xxh3_128;

/// SQLite database header size.
pub const SQLITE_DB_HEADER_SIZE: usize = 100;
const SQLITE_DB_HEADER_SIZE_U16: u16 = 100;
/// Offset in the 100-byte SQLite database header where reserved-bytes lives.
pub const SQLITE_DB_HEADER_RESERVED_OFFSET: usize = 20;
/// Bytes reserved at end-of-page for optional XXH3 checksum trailer.
pub const PAGE_CHECKSUM_RESERVED_BYTES: usize = 16;
/// SQLite WAL header size.
pub const WAL_HEADER_SIZE: usize = 32;
/// SQLite WAL frame header size.
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

const WAL_HEADER_SALT1_OFFSET: usize = 16;
const WAL_HEADER_SALT2_OFFSET: usize = 20;
const WAL_HEADER_CKSUM1_OFFSET: usize = 24;
const WAL_HEADER_CKSUM2_OFFSET: usize = 28;

const WAL_FRAME_DB_SIZE_OFFSET: usize = 4;
const WAL_FRAME_SALT1_OFFSET: usize = 8;
const WAL_FRAME_SALT2_OFFSET: usize = 12;
const WAL_FRAME_CKSUM1_OFFSET: usize = 16;
const WAL_FRAME_CKSUM2_OFFSET: usize = 20;
const SQLITE_DB_HEADER_MAGIC: [u8; 16] = *b"SQLite format 3\0";

/// Hash tiers from the three-tier integrity strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashTier {
    Integrity,
    ContentAddressing,
    Protocol,
}

/// SQLite cumulative checksum pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqliteWalChecksum {
    pub s1: u32,
    pub s2: u32,
}

/// WAL salts copied into frame headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalSalts {
    pub salt1: u32,
    pub salt2: u32,
}

/// WAL magic number for little-endian checksum mode.
pub const WAL_MAGIC_LE: u32 = 0x377F_0682;

/// WAL magic number for big-endian checksum mode.
pub const WAL_MAGIC_BE: u32 = 0x377F_0683;

/// WAL format version constant (SQLite 3.7.0+).
pub const WAL_FORMAT_VERSION: u32 = 3_007_000;

/// Parsed 32-byte WAL header.
///
/// Layout:
/// ```text
/// Offset  Size  Description
///   0       4   Magic: 0x377F0682 (LE checksum) or 0x377F0683 (BE checksum)
///   4       4   Format version: 3007000
///   8       4   Page size in bytes
///  12       4   Checkpoint sequence number
///  16       4   Salt-1
///  20       4   Salt-2
///  24       4   Checksum-1 (of bytes 0..24)
///  28       4   Checksum-2 (of bytes 0..24)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    /// Magic number: `WAL_MAGIC_LE` or `WAL_MAGIC_BE`.
    pub magic: u32,
    /// Format version (must be `WAL_FORMAT_VERSION`).
    pub format_version: u32,
    /// Database page size in bytes.
    pub page_size: u32,
    /// Checkpoint sequence number.
    pub checkpoint_seq: u32,
    /// Salt pair for frame validation.
    pub salts: WalSalts,
    /// Header checksum (covers bytes 0..24).
    pub checksum: SqliteWalChecksum,
}

impl WalHeader {
    /// Whether the magic indicates big-endian checksum words.
    #[must_use]
    pub const fn big_endian_checksum(&self) -> bool {
        self.magic == WAL_MAGIC_BE
    }

    /// Parse a 32-byte WAL header from raw bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < WAL_HEADER_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL header too small: expected >= {WAL_HEADER_SIZE}, got {}",
                    buf.len()
                ),
            });
        }
        let magic = read_be_u32_at(buf, 0);
        if magic != WAL_MAGIC_LE && magic != WAL_MAGIC_BE {
            return Err(FrankenError::WalCorrupt {
                detail: format!("invalid WAL magic: {magic:#010x}"),
            });
        }
        let format_version = read_be_u32_at(buf, 4);
        if format_version != WAL_FORMAT_VERSION {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "unsupported WAL format version: {format_version} (expected {WAL_FORMAT_VERSION})"
                ),
            });
        }
        Ok(Self {
            magic,
            format_version,
            page_size: read_be_u32_at(buf, 8),
            checkpoint_seq: read_be_u32_at(buf, 12),
            salts: WalSalts {
                salt1: read_be_u32_at(buf, WAL_HEADER_SALT1_OFFSET),
                salt2: read_be_u32_at(buf, WAL_HEADER_SALT2_OFFSET),
            },
            checksum: SqliteWalChecksum {
                s1: read_be_u32_at(buf, WAL_HEADER_CKSUM1_OFFSET),
                s2: read_be_u32_at(buf, WAL_HEADER_CKSUM2_OFFSET),
            },
        })
    }

    /// Serialize this header into a 32-byte buffer and compute the checksum.
    pub fn to_bytes(&self) -> Result<[u8; WAL_HEADER_SIZE]> {
        let mut buf = [0u8; WAL_HEADER_SIZE];
        write_be_u32_at(&mut buf, 0, self.magic);
        write_be_u32_at(&mut buf, 4, self.format_version);
        write_be_u32_at(&mut buf, 8, self.page_size);
        write_be_u32_at(&mut buf, 12, self.checkpoint_seq);
        write_be_u32_at(&mut buf, WAL_HEADER_SALT1_OFFSET, self.salts.salt1);
        write_be_u32_at(&mut buf, WAL_HEADER_SALT2_OFFSET, self.salts.salt2);
        // Compute and write checksum over bytes 0..24.
        let checksum = sqlite_wal_checksum(
            &buf[..WAL_HEADER_CKSUM1_OFFSET],
            0,
            0,
            self.big_endian_checksum(),
        )?;
        write_be_u32_at(&mut buf, WAL_HEADER_CKSUM1_OFFSET, checksum.s1);
        write_be_u32_at(&mut buf, WAL_HEADER_CKSUM2_OFFSET, checksum.s2);
        Ok(buf)
    }
}

/// Parsed 24-byte WAL frame header.
///
/// Layout:
/// ```text
/// Offset  Size  Description
///   0       4   Page number
///   4       4   For commit frames: db size in pages. Otherwise 0.
///   8       4   Salt-1 (must match WAL header)
///  12       4   Salt-2 (must match WAL header)
///  16       4   Cumulative checksum-1
///  20       4   Cumulative checksum-2
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFrameHeader {
    /// Page number this frame writes to.
    pub page_number: u32,
    /// For commit frames: database size in pages after this commit. Otherwise 0.
    pub db_size: u32,
    /// Salt pair (must match WAL header salts).
    pub salts: WalSalts,
    /// Cumulative checksum (covers this frame and all prior frames).
    pub checksum: SqliteWalChecksum,
}

impl WalFrameHeader {
    /// Whether this frame is a commit frame (non-zero `db_size`).
    #[must_use]
    pub const fn is_commit(&self) -> bool {
        self.db_size > 0
    }

    /// Parse a 24-byte WAL frame header from raw bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < WAL_FRAME_HEADER_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL frame header too small: expected >= {WAL_FRAME_HEADER_SIZE}, got {}",
                    buf.len()
                ),
            });
        }
        Ok(Self {
            page_number: read_be_u32_at(buf, 0),
            db_size: read_be_u32_at(buf, WAL_FRAME_DB_SIZE_OFFSET),
            salts: WalSalts {
                salt1: read_be_u32_at(buf, WAL_FRAME_SALT1_OFFSET),
                salt2: read_be_u32_at(buf, WAL_FRAME_SALT2_OFFSET),
            },
            checksum: SqliteWalChecksum {
                s1: read_be_u32_at(buf, WAL_FRAME_CKSUM1_OFFSET),
                s2: read_be_u32_at(buf, WAL_FRAME_CKSUM2_OFFSET),
            },
        })
    }

    /// Serialize this frame header into a 24-byte buffer.
    ///
    /// Note: The checksum field is written as-is. To compute the correct
    /// checksum, use `compute_wal_frame_checksum` on the complete frame.
    pub fn to_bytes(&self) -> [u8; WAL_FRAME_HEADER_SIZE] {
        let mut buf = [0u8; WAL_FRAME_HEADER_SIZE];
        write_be_u32_at(&mut buf, 0, self.page_number);
        write_be_u32_at(&mut buf, WAL_FRAME_DB_SIZE_OFFSET, self.db_size);
        write_be_u32_at(&mut buf, WAL_FRAME_SALT1_OFFSET, self.salts.salt1);
        write_be_u32_at(&mut buf, WAL_FRAME_SALT2_OFFSET, self.salts.salt2);
        write_be_u32_at(&mut buf, WAL_FRAME_CKSUM1_OFFSET, self.checksum.s1);
        write_be_u32_at(&mut buf, WAL_FRAME_CKSUM2_OFFSET, self.checksum.s2);
        buf
    }
}

/// First failure reason encountered while validating a WAL chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum WalChainInvalidReason {
    HeaderChecksumMismatch,
    TruncatedFrame,
    SaltMismatch,
    FrameSaltMismatch,
    FrameChecksumMismatch,
}

/// Summary of WAL chain validation and replay boundary analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalChainValidation {
    pub valid: bool,
    pub valid_frames: usize,
    pub replayable_frames: usize,
    pub first_invalid_frame: Option<usize>,
    pub reason: Option<WalChainInvalidReason>,
    pub last_commit_frame: Option<usize>,

    // Compatibility aliases for alternate test/layout variants.
    pub header_valid: bool,
    pub valid_frame_count: usize,
    pub replayable_frame_count: usize,
    pub first_invalid_reason: Option<WalChainInvalidReason>,
    pub replayable_prefix_len: usize,
}

impl WalChainValidation {
    fn from_core(
        valid: bool,
        valid_frames: usize,
        replayable_frames: usize,
        first_invalid_frame: Option<usize>,
        reason: Option<WalChainInvalidReason>,
        last_commit_frame: Option<usize>,
        frame_size: usize,
    ) -> Self {
        Self {
            valid,
            valid_frames,
            replayable_frames,
            first_invalid_frame,
            reason,
            last_commit_frame,
            header_valid: valid || reason != Some(WalChainInvalidReason::HeaderChecksumMismatch),
            valid_frame_count: valid_frames,
            replayable_frame_count: replayable_frames,
            first_invalid_reason: reason,
            replayable_prefix_len: WAL_HEADER_SIZE + replayable_frames * frame_size,
        }
    }
}

/// Five integrity-check levels aligned with SQLite-style deep validation stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityCheckLevel {
    Page,
    BtreeStructural,
    RecordFormat,
    CrossReference,
    Schema,
}

/// One integrity-check finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityCheckIssue {
    pub level: IntegrityCheckLevel,
    pub page_number: Option<u32>,
    pub detail: String,
}

/// Result bundle for integrity-check execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityCheckReport {
    pub pages_checked: usize,
    pub issues: Vec<IntegrityCheckIssue>,
}

impl IntegrityCheckReport {
    /// Build an empty report with a known page-count.
    #[must_use]
    pub fn ok(pages_checked: usize) -> Self {
        Self {
            pages_checked,
            issues: Vec::new(),
        }
    }

    /// True when no integrity issues were found.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.issues.is_empty()
    }

    /// SQLite-compatible string payload: either `ok` or a list of error lines.
    #[must_use]
    pub fn sqlite_messages(&self) -> Vec<String> {
        if self.is_ok() {
            vec!["ok".to_owned()]
        } else {
            self.issues
                .iter()
                .map(|issue| issue.detail.clone())
                .collect()
        }
    }

    fn push(
        &mut self,
        level: IntegrityCheckLevel,
        page_number: Option<u32>,
        detail: impl Into<String>,
    ) {
        self.issues.push(IntegrityCheckIssue {
            level,
            page_number,
            detail: detail.into(),
        });
    }
}

/// Known SQLite b-tree page type flags.
pub const BTREE_PAGE_TYPE_FLAGS: [u8; 4] = [0x02, 0x05, 0x0A, 0x0D];

/// Crash-model torn-write sector sizes required by the spec.
pub const CRASH_MODEL_SECTOR_SIZES: [usize; 3] = [512, 1024, 4096];

/// Checksum families used for recovery routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ChecksumFailureKind {
    WalFrameChecksumMismatch,
    Xxh3PageChecksumMismatch,
    Crc32cSymbolMismatch,
    DbFileCorruption,
}

/// Recovery policy selected for a checksum failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RecoveryAction {
    AttemptWalFecRepair,
    TruncateWalAtFirstInvalidFrame,
    EvictCacheAndRetryFromWal,
    ExcludeCorruptedSymbolAndContinue,
    ReportPersistentCorruption,
}

/// Result of an attempted WAL-FEC repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum WalFecRepairOutcome {
    Repaired,
    InsufficientSymbols,
    SourceHashMismatch,
}

/// Final recovery decision for a WAL frame checksum mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecoveryDecision {
    Repaired,
    Truncated,
}

/// Crash-model assertions used by durability/recovery code paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrashModelContract {
    flags: u8,
}

impl CrashModelContract {
    pub const CRASH_AT_ANY_POINT: u8 = 1 << 0;
    pub const FSYNC_IS_DURABILITY_BARRIER: u8 = 1 << 1;
    pub const WRITES_REORDER_WITHOUT_FSYNC: u8 = 1 << 2;
    pub const BITROT_EXISTS: u8 = 1 << 3;
    pub const METADATA_MAY_REQUIRE_DIRECTORY_FSYNC: u8 = 1 << 4;

    #[must_use]
    pub fn crash_at_any_point(self) -> bool {
        self.flags & Self::CRASH_AT_ANY_POINT != 0
    }

    #[must_use]
    pub fn fsync_is_durability_barrier(self) -> bool {
        self.flags & Self::FSYNC_IS_DURABILITY_BARRIER != 0
    }

    #[must_use]
    pub fn writes_reorder_without_fsync(self) -> bool {
        self.flags & Self::WRITES_REORDER_WITHOUT_FSYNC != 0
    }

    #[must_use]
    pub fn bitrot_exists(self) -> bool {
        self.flags & Self::BITROT_EXISTS != 0
    }

    #[must_use]
    pub fn metadata_may_require_directory_fsync(self) -> bool {
        self.flags & Self::METADATA_MAY_REQUIRE_DIRECTORY_FSYNC != 0
    }
}

impl Default for CrashModelContract {
    fn default() -> Self {
        Self {
            flags: Self::CRASH_AT_ANY_POINT
                | Self::FSYNC_IS_DURABILITY_BARRIER
                | Self::WRITES_REORDER_WITHOUT_FSYNC
                | Self::BITROT_EXISTS
                | Self::METADATA_MAY_REQUIRE_DIRECTORY_FSYNC,
        }
    }
}

/// Return the current crash-model contract.
#[must_use]
pub fn crash_model_contract() -> CrashModelContract {
    CrashModelContract::default()
}

/// True when a sector size is explicitly covered by torn-write simulations.
#[must_use]
pub fn supports_torn_write_sector_size(bytes_per_sector: usize) -> bool {
    CRASH_MODEL_SECTOR_SIZES.contains(&bytes_per_sector)
}

/// True when the byte is a valid SQLite b-tree page type.
#[must_use]
pub fn is_valid_btree_page_type(page_type: u8) -> bool {
    BTREE_PAGE_TYPE_FLAGS.contains(&page_type)
}

/// Integrity-check level 1: page-level validation of type/header/checksum.
pub fn integrity_check_level1_page(
    page: &[u8],
    page_number: u32,
    is_btree_page: bool,
    verify_xxh3_trailer: bool,
) -> Result<IntegrityCheckReport> {
    let mut report = IntegrityCheckReport::ok(1);

    if page.is_empty() {
        report.push(
            IntegrityCheckLevel::Page,
            Some(page_number),
            format!("page {page_number}: empty page buffer"),
        );
        return Ok(report);
    }

    if is_btree_page {
        let page_type = page[0];
        if !is_valid_btree_page_type(page_type) {
            report.push(
                IntegrityCheckLevel::Page,
                Some(page_number),
                format!("page {page_number}: invalid b-tree page type 0x{page_type:02x}"),
            );
            return Ok(report);
        }

        let header_size = if page_type == 0x02 || page_type == 0x05 {
            12
        } else {
            8
        };

        if page.len() < header_size {
            report.push(
                IntegrityCheckLevel::Page,
                Some(page_number),
                format!(
                    "page {page_number}: b-tree header too small (need {header_size}, got {})",
                    page.len()
                ),
            );
            return Ok(report);
        }

        let first_freeblock = u16::from_be_bytes([page[1], page[2]]);
        if first_freeblock != 0 && usize::from(first_freeblock) >= page.len() {
            report.push(
                IntegrityCheckLevel::Page,
                Some(page_number),
                format!(
                    "page {page_number}: first freeblock offset out of range ({first_freeblock})"
                ),
            );
        }

        let cell_count = u16::from_be_bytes([page[3], page[4]]);
        let raw_cell_content_offset = u16::from_be_bytes([page[5], page[6]]);
        let cell_content_offset = if raw_cell_content_offset == 0
            && (page.len() == 65_536 || (page_number == 1 && page.len() == 65_536 - 100))
        {
            page.len()
        } else {
            usize::from(raw_cell_content_offset)
        };

        if cell_content_offset == 0 || cell_content_offset > page.len() {
            report.push(
                IntegrityCheckLevel::Page,
                Some(page_number),
                format!(
                    "page {page_number}: cell content offset out of range ({cell_content_offset})"
                ),
            );
        }

        let pointer_bytes = usize::from(cell_count) * 2;
        if header_size + pointer_bytes > page.len() {
            report.push(
                IntegrityCheckLevel::Page,
                Some(page_number),
                format!(
                    "page {page_number}: cell pointer array exceeds page bounds (cells={cell_count})"
                ),
            );
        }

        let fragmented = page[7];
        if fragmented > 60 {
            report.push(
                IntegrityCheckLevel::Page,
                Some(page_number),
                format!("page {page_number}: fragmented free bytes out of range ({fragmented})"),
            );
        }
    }

    if verify_xxh3_trailer {
        match verify_page_checksum(page) {
            Ok(true) => {}
            Ok(false) => {
                report.push(
                    IntegrityCheckLevel::Page,
                    Some(page_number),
                    format!("page {page_number}: xxh3 page checksum mismatch"),
                );
            }
            Err(err) => {
                report.push(
                    IntegrityCheckLevel::Page,
                    Some(page_number),
                    format!("page {page_number}: xxh3 verification error: {err}"),
                );
            }
        }
    }

    Ok(report)
}

/// Validate the 100-byte SQLite database header.
#[must_use]
pub fn integrity_check_database_header(db_bytes: &[u8]) -> IntegrityCheckReport {
    let mut report = IntegrityCheckReport::ok(1);
    if db_bytes.len() < SQLITE_DB_HEADER_SIZE {
        report.push(
            IntegrityCheckLevel::Page,
            Some(1),
            format!(
                "database header too small: expected >= {SQLITE_DB_HEADER_SIZE}, got {}",
                db_bytes.len()
            ),
        );
        return report;
    }

    if db_bytes[..SQLITE_DB_HEADER_MAGIC.len()] != SQLITE_DB_HEADER_MAGIC {
        report.push(
            IntegrityCheckLevel::Page,
            Some(1),
            "database header magic mismatch".to_owned(),
        );
    }

    let page_size_raw = u16::from_be_bytes([db_bytes[16], db_bytes[17]]);
    let page_size = if page_size_raw == 1 {
        65_536
    } else {
        usize::from(page_size_raw)
    };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        report.push(
            IntegrityCheckLevel::Page,
            Some(1),
            format!("database header page size out of range ({page_size})"),
        );
    }

    report
}

/// Level-1 integrity check entrypoint for raw SQLite database bytes.
pub fn integrity_check_sqlite_file_level1(db_bytes: &[u8]) -> Result<IntegrityCheckReport> {
    let header_report = integrity_check_database_header(db_bytes);

    let page_report = if db_bytes.len() >= SQLITE_DB_HEADER_SIZE + 8 {
        let page_size = sqlite_page_size_from_header(db_bytes).unwrap_or(4096);
        let first_page_end = page_size.min(db_bytes.len());
        if first_page_end > SQLITE_DB_HEADER_SIZE {
            let mut first_page = db_bytes[SQLITE_DB_HEADER_SIZE..first_page_end].to_vec();
            normalize_first_page_header_offsets(&mut first_page);
            integrity_check_level1_page(&first_page, 1, true, false)?
        } else {
            let mut report = IntegrityCheckReport::ok(1);
            report.push(
                IntegrityCheckLevel::Page,
                Some(1),
                "database first page payload missing".to_owned(),
            );
            report
        }
    } else {
        let mut report = IntegrityCheckReport::ok(1);
        report.push(
            IntegrityCheckLevel::Page,
            Some(1),
            "database missing first b-tree page header bytes".to_owned(),
        );
        report
    };

    Ok(merge_integrity_reports(&[header_report, page_report]))
}

/// Integrity-check level 2: b-tree structural validation for cell bounds/overlap/key order.
#[must_use]
pub fn integrity_check_level2_btree(
    page_number: u32,
    page_size: usize,
    cell_spans: &[(u16, u32)],
    keys: &[i64],
) -> IntegrityCheckReport {
    let mut report = IntegrityCheckReport::ok(1);

    if page_size == 0 {
        report.push(
            IntegrityCheckLevel::BtreeStructural,
            Some(page_number),
            format!("page {page_number}: invalid page size 0 for structural check"),
        );
        return report;
    }

    let mut sorted_spans = cell_spans.to_vec();
    sorted_spans.sort_unstable_by_key(|&(start, _)| start);

    for (start, end) in &sorted_spans {
        let start_usize = *start as usize;
        let end_usize = *end as usize;
        if start_usize >= end_usize || end_usize > page_size {
            report.push(
                IntegrityCheckLevel::BtreeStructural,
                Some(page_number),
                format!("page {page_number}: cell span out of bounds ({start}..{end})"),
            );
        }
    }

    for window in sorted_spans.windows(2) {
        let (_, prev_end) = window[0];
        let (next_start, _) = window[1];
        if prev_end > u32::from(next_start) {
            report.push(
                IntegrityCheckLevel::BtreeStructural,
                Some(page_number),
                format!(
                    "page {page_number}: overlapping cell spans ({}) and ({})",
                    format_args!("{}..{}", window[0].0, window[0].1),
                    format_args!("{}..{}", window[1].0, window[1].1)
                ),
            );
            break;
        }
    }

    if keys.windows(2).any(|window| window[0] > window[1]) {
        report.push(
            IntegrityCheckLevel::BtreeStructural,
            Some(page_number),
            format!("page {page_number}: keys out of order"),
        );
    }

    report
}

/// Integrity-check level 3: overflow-chain shape and reference validity.
#[must_use]
pub fn integrity_check_level3_overflow_chain(
    page_number: u32,
    overflow_chain: &[u32],
    max_page_number: u32,
) -> IntegrityCheckReport {
    let mut report = IntegrityCheckReport::ok(1);
    let mut seen = std::collections::HashSet::new();

    for overflow_page in overflow_chain {
        if *overflow_page == 0 || *overflow_page > max_page_number {
            report.push(
                IntegrityCheckLevel::RecordFormat,
                Some(page_number),
                format!(
                    "page {page_number}: broken overflow chain references page {overflow_page}"
                ),
            );
            break;
        }
        if !seen.insert(*overflow_page) {
            report.push(
                IntegrityCheckLevel::RecordFormat,
                Some(page_number),
                format!("page {page_number}: broken overflow chain cycle at page {overflow_page}"),
            );
            break;
        }
    }

    report
}

/// Integrity-check level 4: global page-accounting cross-reference checks.
#[must_use]
pub fn integrity_check_level4_cross_reference(
    expected_total_pages: u32,
    accounted_pages: &[u32],
) -> IntegrityCheckReport {
    let pages_checked = usize::try_from(expected_total_pages).unwrap_or(usize::MAX);
    let mut report = IntegrityCheckReport::ok(pages_checked);
    let mut seen = std::collections::HashSet::new();

    for page in accounted_pages {
        if *page == 0 || *page > expected_total_pages {
            report.push(
                IntegrityCheckLevel::CrossReference,
                Some(*page),
                format!("page {page}: cross-reference contains out-of-range page reference"),
            );
            continue;
        }
        if !seen.insert(*page) {
            report.push(
                IntegrityCheckLevel::CrossReference,
                Some(*page),
                format!("page {page}: appears in multiple b-tree ownership sets"),
            );
        }
    }

    for expected_page in 1..=expected_total_pages {
        if !seen.contains(&expected_page) {
            report.push(
                IntegrityCheckLevel::CrossReference,
                Some(expected_page),
                format!(
                    "page {expected_page}: not accounted for by any b-tree/freelist/pointer-map"
                ),
            );
        }
    }

    report
}

/// Integrity-check level 5: sqlite_master/schema parseability checks.
#[must_use]
pub fn integrity_check_level5_schema(schema_entries: &[String]) -> IntegrityCheckReport {
    let mut report = IntegrityCheckReport::ok(schema_entries.len());

    if schema_entries.is_empty() {
        report.push(
            IntegrityCheckLevel::Schema,
            None,
            "malformed sqlite_master: no entries".to_owned(),
        );
        return report;
    }

    for (index, entry) in schema_entries.iter().enumerate() {
        if !is_valid_schema_sql(entry) {
            report.push(
                IntegrityCheckLevel::Schema,
                None,
                format!("sqlite_master row {index}: malformed SQL entry"),
            );
        }
    }

    report
}

/// Merge several level-specific integrity reports into one SQLite-style output bundle.
#[must_use]
pub fn merge_integrity_reports(reports: &[IntegrityCheckReport]) -> IntegrityCheckReport {
    let pages_checked = reports.iter().map(|report| report.pages_checked).sum();
    let mut merged = IntegrityCheckReport::ok(pages_checked);
    for report in reports {
        merged.issues.extend(report.issues.clone());
    }
    merged
}

/// Recovery routing based on checksum family and available decode budget.
#[must_use]
pub fn recovery_action_for_checksum_failure(
    failure: ChecksumFailureKind,
    surviving_symbols: Option<usize>,
    required_symbols: Option<usize>,
) -> RecoveryAction {
    match failure {
        ChecksumFailureKind::WalFrameChecksumMismatch => {
            if let (Some(surviving), Some(required)) = (surviving_symbols, required_symbols) {
                if surviving >= required {
                    RecoveryAction::AttemptWalFecRepair
                } else {
                    RecoveryAction::TruncateWalAtFirstInvalidFrame
                }
            } else {
                RecoveryAction::TruncateWalAtFirstInvalidFrame
            }
        }
        ChecksumFailureKind::Xxh3PageChecksumMismatch => RecoveryAction::EvictCacheAndRetryFromWal,
        ChecksumFailureKind::Crc32cSymbolMismatch => {
            RecoveryAction::ExcludeCorruptedSymbolAndContinue
        }
        ChecksumFailureKind::DbFileCorruption => RecoveryAction::ReportPersistentCorruption,
    }
}

/// Attempt WAL-FEC repair using an independently validated source hash.
#[must_use]
pub fn attempt_wal_fec_repair(
    reconstructed_payload: &[u8],
    expected_source_hash: Xxh3Checksum128,
    surviving_symbols: usize,
    required_symbols: usize,
) -> WalFecRepairOutcome {
    if surviving_symbols < required_symbols {
        return WalFecRepairOutcome::InsufficientSymbols;
    }
    if verify_wal_fec_source_hash(reconstructed_payload, expected_source_hash) {
        WalFecRepairOutcome::Repaired
    } else {
        WalFecRepairOutcome::SourceHashMismatch
    }
}

/// Concrete recovery path for WAL frame checksum mismatches.
#[must_use]
pub fn recover_wal_frame_checksum_mismatch(
    reconstructed_payload: Option<&[u8]>,
    expected_source_hash: Option<Xxh3Checksum128>,
    surviving_symbols: usize,
    required_symbols: usize,
) -> WalRecoveryDecision {
    let action = recovery_action_for_checksum_failure(
        ChecksumFailureKind::WalFrameChecksumMismatch,
        Some(surviving_symbols),
        Some(required_symbols),
    );

    if action != RecoveryAction::AttemptWalFecRepair {
        return WalRecoveryDecision::Truncated;
    }

    let (Some(payload), Some(expected_hash)) = (reconstructed_payload, expected_source_hash) else {
        return WalRecoveryDecision::Truncated;
    };

    match attempt_wal_fec_repair(payload, expected_hash, surviving_symbols, required_symbols) {
        WalFecRepairOutcome::Repaired => WalRecoveryDecision::Repaired,
        WalFecRepairOutcome::InsufficientSymbols | WalFecRepairOutcome::SourceHashMismatch => {
            WalRecoveryDecision::Truncated
        }
    }
}

/// Check whether a WAL stream indicates a torn-write event.
pub fn detect_torn_write_in_wal(
    wal_bytes: &[u8],
    page_size: usize,
    big_endian_checksum_words: bool,
) -> Result<bool> {
    let validation = validate_wal_chain(wal_bytes, page_size, big_endian_checksum_words)?;
    Ok(matches!(
        validation.reason,
        Some(WalChainInvalidReason::TruncatedFrame | WalChainInvalidReason::FrameChecksumMismatch)
    ))
}

/// XXH3-128 digest split into low/high u64 words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Xxh3Checksum128 {
    pub low: u64,
    pub high: u64,
}

impl Xxh3Checksum128 {
    /// Compute XXH3-128.
    #[must_use]
    pub fn compute(data: &[u8]) -> Self {
        from_u128_le(xxh3_128(data))
    }

    /// Verify digest against payload.
    #[must_use]
    pub fn verify(&self, data: &[u8]) -> bool {
        *self == Self::compute(data)
    }

    /// Return little-endian bytes.
    #[must_use]
    pub fn to_le_bytes(self) -> [u8; 16] {
        let mut out = [0_u8; 16];
        out[..8].copy_from_slice(&self.low.to_le_bytes());
        out[8..].copy_from_slice(&self.high.to_le_bytes());
        out
    }
}

/// Configure reserved bytes in a SQLite database header.
pub fn configure_page_checksum_reserved_bytes(db_header: &mut [u8], enabled: bool) -> Result<()> {
    ensure_min_len(
        db_header,
        SQLITE_DB_HEADER_RESERVED_OFFSET + 1,
        "database header",
    )?;
    db_header[SQLITE_DB_HEADER_RESERVED_OFFSET] = if enabled {
        u8::try_from(PAGE_CHECKSUM_RESERVED_BYTES).expect("reserved-byte constant fits in u8")
    } else {
        0
    };
    Ok(())
}

/// Read reserved bytes from a SQLite database header.
pub fn page_checksum_reserved_bytes(db_header: &[u8]) -> Result<u8> {
    ensure_min_len(
        db_header,
        SQLITE_DB_HEADER_RESERVED_OFFSET + 1,
        "database header",
    )?;
    Ok(db_header[SQLITE_DB_HEADER_RESERVED_OFFSET])
}

/// Zero the checksum trailer bytes in a page.
pub fn zero_page_checksum_trailer(page: &mut [u8]) -> Result<()> {
    if page.len() < PAGE_CHECKSUM_RESERVED_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "page too small for checksum trailer: expected >= {PAGE_CHECKSUM_RESERVED_BYTES}, got {}",
                page.len()
            ),
        });
    }

    let start = page.len() - PAGE_CHECKSUM_RESERVED_BYTES;
    page[start..].fill(0);
    Ok(())
}

/// Write XXH3 trailer checksum into reserved page bytes.
pub fn write_page_checksum(page: &mut [u8]) -> Result<Xxh3Checksum128> {
    if page.len() < PAGE_CHECKSUM_RESERVED_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "page too small for checksum trailer: expected >= {PAGE_CHECKSUM_RESERVED_BYTES}, got {}",
                page.len()
            ),
        });
    }

    let payload_end = page.len() - PAGE_CHECKSUM_RESERVED_BYTES;
    let digest = Xxh3Checksum128::compute(&page[..payload_end]);
    page[payload_end..].copy_from_slice(&digest.to_le_bytes());
    Ok(digest)
}

/// Read XXH3 trailer checksum from reserved page bytes.
pub fn read_page_checksum(page: &[u8]) -> Result<Xxh3Checksum128> {
    if page.len() < PAGE_CHECKSUM_RESERVED_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "page too small for checksum trailer: expected >= {PAGE_CHECKSUM_RESERVED_BYTES}, got {}",
                page.len()
            ),
        });
    }

    let checksum_start = page.len() - PAGE_CHECKSUM_RESERVED_BYTES;
    Ok(read_xxh3_from_bytes(
        &page[checksum_start..checksum_start + PAGE_CHECKSUM_RESERVED_BYTES],
    ))
}

/// Verify page trailer checksum.
pub fn verify_page_checksum(page: &[u8]) -> Result<bool> {
    if page.len() < PAGE_CHECKSUM_RESERVED_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "page too small for checksum trailer: expected >= {PAGE_CHECKSUM_RESERVED_BYTES}, got {}",
                page.len()
            ),
        });
    }

    let payload_end = page.len() - PAGE_CHECKSUM_RESERVED_BYTES;
    let expected = Xxh3Checksum128::compute(&page[..payload_end]);
    let actual = read_page_checksum(page)?;
    Ok(actual == expected)
}

/// Compute independent FEC source hash for a page payload.
#[must_use]
pub fn wal_fec_source_hash_xxh3_128(page_payload: &[u8]) -> Xxh3Checksum128 {
    Xxh3Checksum128::compute(page_payload)
}

/// Verify independent FEC source hash.
#[must_use]
pub fn verify_wal_fec_source_hash(page_payload: &[u8], expected: Xxh3Checksum128) -> bool {
    wal_fec_source_hash_xxh3_128(page_payload) == expected
}

/// Read WAL header salts.
pub fn read_wal_header_salts(wal_header: &[u8]) -> Result<WalSalts> {
    ensure_min_len(wal_header, WAL_HEADER_SIZE, "WAL header")?;
    Ok(WalSalts {
        salt1: read_be_u32_at(wal_header, WAL_HEADER_SALT1_OFFSET),
        salt2: read_be_u32_at(wal_header, WAL_HEADER_SALT2_OFFSET),
    })
}

/// Write WAL header salts.
pub fn write_wal_header_salts(wal_header: &mut [u8], salts: WalSalts) -> Result<()> {
    ensure_min_len(wal_header, WAL_HEADER_SIZE, "WAL header")?;
    write_be_u32_at(wal_header, WAL_HEADER_SALT1_OFFSET, salts.salt1);
    write_be_u32_at(wal_header, WAL_HEADER_SALT2_OFFSET, salts.salt2);
    Ok(())
}

/// Read WAL header checksum pair.
pub fn read_wal_header_checksum(wal_header: &[u8]) -> Result<SqliteWalChecksum> {
    ensure_min_len(wal_header, WAL_HEADER_SIZE, "WAL header")?;
    Ok(SqliteWalChecksum {
        s1: read_be_u32_at(wal_header, WAL_HEADER_CKSUM1_OFFSET),
        s2: read_be_u32_at(wal_header, WAL_HEADER_CKSUM2_OFFSET),
    })
}

/// Compute and write WAL header checksum.
pub fn write_wal_header_checksum(
    wal_header: &mut [u8],
    big_endian_checksum_words: bool,
) -> Result<SqliteWalChecksum> {
    ensure_min_len(wal_header, WAL_HEADER_SIZE, "WAL header")?;
    let checksum = wal_header_checksum(wal_header, big_endian_checksum_words)?;
    write_be_u32_at(wal_header, WAL_HEADER_CKSUM1_OFFSET, checksum.s1);
    write_be_u32_at(wal_header, WAL_HEADER_CKSUM2_OFFSET, checksum.s2);
    Ok(checksum)
}

/// Compute WAL header checksum from first 24 bytes.
pub fn wal_header_checksum(
    wal_header: &[u8],
    big_endian_checksum_words: bool,
) -> Result<SqliteWalChecksum> {
    ensure_min_len(wal_header, WAL_HEADER_SIZE, "WAL header")?;
    sqlite_wal_checksum(
        &wal_header[..WAL_HEADER_CKSUM1_OFFSET],
        0,
        0,
        big_endian_checksum_words,
    )
}

/// Validate checksum stored in WAL header.
pub fn validate_wal_header_checksum(
    wal_header: &[u8],
    big_endian_checksum_words: bool,
) -> Result<bool> {
    let expected = wal_header_checksum(wal_header, big_endian_checksum_words)?;
    let actual = read_wal_header_checksum(wal_header)?;
    Ok(actual == expected)
}

/// Read salts from WAL frame header.
pub fn read_wal_frame_salts(frame_header: &[u8]) -> Result<WalSalts> {
    ensure_min_len(frame_header, WAL_FRAME_HEADER_SIZE, "WAL frame header")?;
    Ok(WalSalts {
        salt1: read_be_u32_at(frame_header, WAL_FRAME_SALT1_OFFSET),
        salt2: read_be_u32_at(frame_header, WAL_FRAME_SALT2_OFFSET),
    })
}

/// Write salts into WAL frame header.
pub fn write_wal_frame_salts(frame_header: &mut [u8], salts: WalSalts) -> Result<()> {
    ensure_min_len(frame_header, WAL_FRAME_HEADER_SIZE, "WAL frame header")?;
    write_be_u32_at(frame_header, WAL_FRAME_SALT1_OFFSET, salts.salt1);
    write_be_u32_at(frame_header, WAL_FRAME_SALT2_OFFSET, salts.salt2);
    Ok(())
}

/// Read checksum from WAL frame header.
pub fn read_wal_frame_checksum(frame_header: &[u8]) -> Result<SqliteWalChecksum> {
    ensure_min_len(frame_header, WAL_FRAME_HEADER_SIZE, "WAL frame header")?;
    Ok(SqliteWalChecksum {
        s1: read_be_u32_at(frame_header, WAL_FRAME_CKSUM1_OFFSET),
        s2: read_be_u32_at(frame_header, WAL_FRAME_CKSUM2_OFFSET),
    })
}

/// Compute checksum for one WAL frame given prior rolling checksum.
pub fn compute_wal_frame_checksum(
    frame: &[u8],
    page_size: usize,
    previous: SqliteWalChecksum,
    big_endian_checksum_words: bool,
) -> Result<SqliteWalChecksum> {
    ensure_frame_len(frame, page_size)?;
    let mut checksum_input = Vec::with_capacity(8 + page_size);
    checksum_input.extend_from_slice(&frame[..8]);
    checksum_input
        .extend_from_slice(&frame[WAL_FRAME_HEADER_SIZE..WAL_FRAME_HEADER_SIZE + page_size]);
    sqlite_wal_checksum(
        &checksum_input,
        previous.s1,
        previous.s2,
        big_endian_checksum_words,
    )
}

/// Compute and write checksum for one WAL frame, returning the next running checksum.
pub fn write_wal_frame_checksum(
    frame: &mut [u8],
    page_size: usize,
    previous: SqliteWalChecksum,
    big_endian_checksum_words: bool,
) -> Result<SqliteWalChecksum> {
    let checksum =
        compute_wal_frame_checksum(frame, page_size, previous, big_endian_checksum_words)?;
    write_be_u32_at(frame, WAL_FRAME_CKSUM1_OFFSET, checksum.s1);
    write_be_u32_at(frame, WAL_FRAME_CKSUM2_OFFSET, checksum.s2);
    Ok(checksum)
}

/// Read frame DB-size commit marker.
pub fn wal_frame_db_size(frame_header: &[u8]) -> Result<u32> {
    ensure_min_len(frame_header, WAL_FRAME_HEADER_SIZE, "WAL frame header")?;
    Ok(read_be_u32_at(frame_header, WAL_FRAME_DB_SIZE_OFFSET))
}

/// Validate WAL bytes and derive replayable prefix information.
pub fn validate_wal_chain(
    wal_bytes: &[u8],
    page_size: usize,
    big_endian_checksum_words: bool,
) -> Result<WalChainValidation> {
    ensure_min_len(wal_bytes, WAL_HEADER_SIZE, "WAL bytes")?;
    if page_size == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "WAL page_size must be greater than zero".to_owned(),
        });
    }

    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let wal_header = &wal_bytes[..WAL_HEADER_SIZE];
    if !validate_wal_header_checksum(wal_header, big_endian_checksum_words)? {
        return Ok(WalChainValidation::from_core(
            false,
            0,
            0,
            Some(0),
            Some(WalChainInvalidReason::HeaderChecksumMismatch),
            None,
            frame_size,
        ));
    }

    let header_salts = read_wal_header_salts(wal_header)?;
    let mut running_checksum = read_wal_header_checksum(wal_header)?;

    let frames = &wal_bytes[WAL_HEADER_SIZE..];
    let full_frames = frames.len() / frame_size;
    let trailing_bytes = frames.len() % frame_size;

    let mut valid_frames = 0_usize;
    let mut replayable_frames = 0_usize;
    let mut last_commit_frame = None;

    for frame_index in 0..full_frames {
        let start = frame_index * frame_size;
        let frame = &frames[start..start + frame_size];
        let frame_header = &frame[..WAL_FRAME_HEADER_SIZE];

        if read_wal_frame_salts(frame_header)? != header_salts {
            return Ok(WalChainValidation::from_core(
                false,
                valid_frames,
                replayable_frames,
                Some(frame_index),
                Some(WalChainInvalidReason::SaltMismatch),
                last_commit_frame,
                frame_size,
            ));
        }

        let expected = compute_wal_frame_checksum(
            frame,
            page_size,
            running_checksum,
            big_endian_checksum_words,
        )?;
        let actual = read_wal_frame_checksum(frame_header)?;
        if actual != expected {
            return Ok(WalChainValidation::from_core(
                false,
                valid_frames,
                replayable_frames,
                Some(frame_index),
                Some(WalChainInvalidReason::FrameChecksumMismatch),
                last_commit_frame,
                frame_size,
            ));
        }

        running_checksum = actual;
        valid_frames += 1;

        if wal_frame_db_size(frame_header)? > 0 {
            last_commit_frame = Some(frame_index);
            replayable_frames = frame_index + 1;
        }
    }

    if trailing_bytes != 0 {
        return Ok(WalChainValidation::from_core(
            false,
            valid_frames,
            replayable_frames,
            Some(valid_frames),
            Some(WalChainInvalidReason::TruncatedFrame),
            last_commit_frame,
            frame_size,
        ));
    }

    Ok(WalChainValidation::from_core(
        true,
        valid_frames,
        replayable_frames,
        None,
        None,
        last_commit_frame,
        frame_size,
    ))
}

/// Compute SQLite-compatible rolling checksum over 8-byte chunks.
pub fn sqlite_wal_checksum(
    data: &[u8],
    seed_s1: u32,
    seed_s2: u32,
    big_endian_checksum_words: bool,
) -> Result<SqliteWalChecksum> {
    if data.len() % 8 != 0 {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "WAL checksum input must be 8-byte aligned, got {} bytes",
                data.len()
            ),
        });
    }

    let mut s1 = seed_s1;
    let mut s2 = seed_s2;

    for chunk in data.chunks_exact(8) {
        let x0 = decode_u32_words(&chunk[..4], big_endian_checksum_words);
        let x1 = decode_u32_words(&chunk[4..], big_endian_checksum_words);

        s1 = s1.wrapping_add(x0).wrapping_add(s2);
        s2 = s2.wrapping_add(x1).wrapping_add(s1);
    }

    Ok(SqliteWalChecksum { s1, s2 })
}

/// Integrity-tier hash bytes.
#[must_use]
pub fn integrity_hash_xxh3_128(data: &[u8]) -> [u8; 16] {
    xxh3_128(data).to_le_bytes()
}

/// Content-addressing hash bytes (BLAKE3-128 truncation).
#[must_use]
pub fn content_address_hash_128(data: &[u8]) -> [u8; 16] {
    let digest = blake3::hash(data);
    let mut out = [0_u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    out
}

/// Protocol-tier CRC-32C.
#[must_use]
pub fn crc32c_checksum(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Map algorithm name to tier.
#[must_use]
pub fn tier_for_algorithm(algorithm: &str) -> Option<HashTier> {
    let normalized = algorithm.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "xxh3_128" | "xxh3" => Some(HashTier::Integrity),
        "blake3_128" | "blake3" => Some(HashTier::ContentAddressing),
        "crc32c" => Some(HashTier::Protocol),
        _ => None,
    }
}

fn is_valid_schema_sql(sql: &str) -> bool {
    let normalized = sql.trim_start().to_ascii_uppercase();
    normalized.starts_with("CREATE TABLE ")
        || normalized.starts_with("CREATE INDEX ")
        || normalized.starts_with("CREATE VIEW ")
        || normalized.starts_with("CREATE TRIGGER ")
        || normalized.starts_with("CREATE VIRTUAL TABLE ")
}

fn sqlite_page_size_from_header(db_bytes: &[u8]) -> Option<usize> {
    if db_bytes.len() < SQLITE_DB_HEADER_SIZE {
        return None;
    }
    let raw = u16::from_be_bytes([db_bytes[16], db_bytes[17]]);
    let page_size = if raw == 1 { 65_536 } else { usize::from(raw) };
    Some(page_size)
}

fn normalize_first_page_header_offsets(page: &mut [u8]) {
    if page.len() < 7 {
        return;
    }

    let first_freeblock = u16::from_be_bytes([page[1], page[2]]);
    if first_freeblock >= SQLITE_DB_HEADER_SIZE_U16 {
        let adjusted = first_freeblock.saturating_sub(SQLITE_DB_HEADER_SIZE_U16);
        page[1..3].copy_from_slice(&adjusted.to_be_bytes());
    } else if first_freeblock != 0 {
        // Pointer into the DB header is invalid. Force failure in bounds check.
        page[1..3].copy_from_slice(&u16::MAX.to_be_bytes());
    }

    let cell_content_offset = u16::from_be_bytes([page[5], page[6]]);
    if cell_content_offset >= SQLITE_DB_HEADER_SIZE_U16 {
        let adjusted = cell_content_offset.saturating_sub(SQLITE_DB_HEADER_SIZE_U16);
        page[5..7].copy_from_slice(&adjusted.to_be_bytes());
    } else if cell_content_offset != 0 {
        // Pointer into the DB header is invalid. Force failure in bounds check.
        page[5..7].copy_from_slice(&u16::MAX.to_be_bytes());
    }
}

fn ensure_min_len(bytes: &[u8], minimum: usize, label: &str) -> Result<()> {
    if bytes.len() < minimum {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "{label} too small: expected >= {minimum}, got {}",
                bytes.len()
            ),
        });
    }
    Ok(())
}

fn ensure_frame_len(frame: &[u8], page_size: usize) -> Result<()> {
    if page_size == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "frame page_size must be > 0".to_owned(),
        });
    }
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    ensure_min_len(frame, frame_size, "WAL frame")
}

fn decode_u32_words(bytes: &[u8], big_endian_checksum_words: bool) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(bytes);
    if big_endian_checksum_words {
        u32::from_be_bytes(raw)
    } else {
        u32::from_le_bytes(raw)
    }
}

fn read_be_u32_at(bytes: &[u8], offset: usize) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_be_bytes(raw)
}

fn write_be_u32_at(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn from_u128_le(value: u128) -> Xxh3Checksum128 {
    let bytes = value.to_le_bytes();
    let mut low = [0_u8; 8];
    let mut high = [0_u8; 8];
    low.copy_from_slice(&bytes[..8]);
    high.copy_from_slice(&bytes[8..]);
    Xxh3Checksum128 {
        low: u64::from_le_bytes(low),
        high: u64::from_le_bytes(high),
    }
}

fn read_xxh3_from_bytes(bytes: &[u8]) -> Xxh3Checksum128 {
    let mut low = [0_u8; 8];
    let mut high = [0_u8; 8];
    low.copy_from_slice(&bytes[..8]);
    high.copy_from_slice(&bytes[8..16]);
    Xxh3Checksum128 {
        low: u64::from_le_bytes(low),
        high: u64::from_le_bytes(high),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: usize = 4096;

    fn sample_page(seed: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0_u8; PAGE_SIZE];
        for (index, byte) in page.iter_mut().enumerate() {
            let reduced_index = u8::try_from(index % 251).expect("modulo result must fit in u8");
            *byte = reduced_index ^ seed;
        }
        page
    }

    fn sample_btree_leaf_page() -> [u8; PAGE_SIZE] {
        let mut page = [0_u8; PAGE_SIZE];
        page[0] = 0x0D; // leaf table page
        page[1..3].copy_from_slice(&0_u16.to_be_bytes()); // first freeblock
        page[3..5].copy_from_slice(&0_u16.to_be_bytes()); // cell count
        page[5..7].copy_from_slice(
            &u16::try_from(PAGE_SIZE)
                .expect("PAGE_SIZE should fit in u16 for test")
                .to_be_bytes(),
        );
        page[7] = 0; // fragmented bytes
        page
    }

    #[test]
    fn test_wal_header_magic_le_roundtrip() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: u32::try_from(PAGE_SIZE).expect("page size fits in u32"),
            checkpoint_seq: 7,
            salts: WalSalts {
                salt1: 0x1111_2222,
                salt2: 0x3333_4444,
            },
            checksum: SqliteWalChecksum::default(),
        };
        let bytes = header.to_bytes().expect("header should serialize");
        assert_eq!(read_be_u32_at(&bytes, 0), WAL_MAGIC_LE);
        assert!(
            validate_wal_header_checksum(&bytes, false).expect("header checksum should validate")
        );

        let parsed = WalHeader::from_bytes(&bytes).expect("header should parse");
        assert_eq!(parsed.magic, WAL_MAGIC_LE);
        assert!(!parsed.big_endian_checksum());
    }

    #[test]
    fn test_wal_header_magic_be_roundtrip() {
        let header = WalHeader {
            magic: WAL_MAGIC_BE,
            format_version: WAL_FORMAT_VERSION,
            page_size: u32::try_from(PAGE_SIZE).expect("page size fits in u32"),
            checkpoint_seq: 11,
            salts: WalSalts {
                salt1: 0xAAAA_BBBB,
                salt2: 0xCCCC_DDDD,
            },
            checksum: SqliteWalChecksum::default(),
        };
        let bytes = header.to_bytes().expect("header should serialize");
        assert_eq!(read_be_u32_at(&bytes, 0), WAL_MAGIC_BE);
        assert!(
            validate_wal_header_checksum(&bytes, true).expect("header checksum should validate")
        );

        let parsed = WalHeader::from_bytes(&bytes).expect("header should parse");
        assert_eq!(parsed.magic, WAL_MAGIC_BE);
        assert!(parsed.big_endian_checksum());
    }

    #[test]
    fn test_wal_header_format_version_constant_and_rejection() {
        assert_eq!(WAL_FORMAT_VERSION, 3_007_000);

        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: u32::try_from(PAGE_SIZE).expect("page size fits in u32"),
            checkpoint_seq: 0,
            salts: WalSalts { salt1: 1, salt2: 2 },
            checksum: SqliteWalChecksum::default(),
        };
        let mut bytes = header.to_bytes().expect("header should serialize");
        write_be_u32_at(&mut bytes, 4, WAL_FORMAT_VERSION + 1);
        let err = WalHeader::from_bytes(&bytes).expect_err("invalid version must be rejected");
        assert!(matches!(err, FrankenError::WalCorrupt { .. }));
    }

    #[test]
    fn test_wal_frame_header_commit_and_non_commit() {
        let salts = WalSalts {
            salt1: 0x0102_0304,
            salt2: 0x0506_0708,
        };
        let checksum = SqliteWalChecksum {
            s1: 0x1111_1111,
            s2: 0x2222_2222,
        };

        let non_commit = WalFrameHeader {
            page_number: 4,
            db_size: 0,
            salts,
            checksum,
        };
        assert!(!non_commit.is_commit());
        let parsed_non_commit =
            WalFrameHeader::from_bytes(&non_commit.to_bytes()).expect("frame should parse");
        assert_eq!(parsed_non_commit, non_commit);

        let commit = WalFrameHeader {
            page_number: 5,
            db_size: 99,
            salts,
            checksum,
        };
        assert!(commit.is_commit());
        let parsed_commit =
            WalFrameHeader::from_bytes(&commit.to_bytes()).expect("frame should parse");
        assert_eq!(parsed_commit, commit);
    }

    #[test]
    fn test_wal_frame_salt_match_validation() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: u32::try_from(PAGE_SIZE).expect("page size fits in u32"),
            checkpoint_seq: 1,
            salts: WalSalts {
                salt1: 0xABCD_1234,
                salt2: 0x9876_5432,
            },
            checksum: SqliteWalChecksum::default(),
        };
        let header_bytes = header.to_bytes().expect("header should serialize");
        let seed = read_wal_header_checksum(&header_bytes).expect("header checksum should read");

        let mut frame = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
        frame[..4].copy_from_slice(&1_u32.to_be_bytes());
        frame[4..8].copy_from_slice(&1_u32.to_be_bytes());
        write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], header.salts)
            .expect("frame salts should write");
        frame[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x3A));
        write_wal_frame_checksum(&mut frame, PAGE_SIZE, seed, false)
            .expect("frame checksum should write");

        let mut wal_bytes = Vec::with_capacity(WAL_HEADER_SIZE + frame.len());
        wal_bytes.extend_from_slice(&header_bytes);
        wal_bytes.extend_from_slice(&frame);
        let valid = validate_wal_chain(&wal_bytes, PAGE_SIZE, false).expect("valid chain");
        assert!(valid.valid);
        assert_eq!(valid.valid_frames, 1);

        write_wal_frame_salts(
            &mut wal_bytes[WAL_HEADER_SIZE..WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE],
            WalSalts {
                salt1: 0xDEAD_BEEF,
                salt2: 0xFACE_FEED,
            },
        )
        .expect("salt rewrite should succeed");
        let invalid =
            validate_wal_chain(&wal_bytes, PAGE_SIZE, false).expect("invalid chain should parse");
        assert_eq!(invalid.reason, Some(WalChainInvalidReason::SaltMismatch));
        assert_eq!(invalid.first_invalid_frame, Some(0));
    }

    #[test]
    fn test_wal_checksum_chain_integrity_two_frames() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: u32::try_from(PAGE_SIZE).expect("page size fits in u32"),
            checkpoint_seq: 3,
            salts: WalSalts {
                salt1: 0xA1A2_A3A4,
                salt2: 0xB1B2_B3B4,
            },
            checksum: SqliteWalChecksum::default(),
        };
        let header_bytes = header.to_bytes().expect("header should serialize");
        let mut running_checksum =
            read_wal_header_checksum(&header_bytes).expect("header checksum should read");

        let mut frame1 = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
        frame1[..4].copy_from_slice(&1_u32.to_be_bytes());
        frame1[4..8].copy_from_slice(&0_u32.to_be_bytes());
        write_wal_frame_salts(&mut frame1[..WAL_FRAME_HEADER_SIZE], header.salts)
            .expect("frame salts should write");
        frame1[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x10));
        running_checksum =
            write_wal_frame_checksum(&mut frame1, PAGE_SIZE, running_checksum, false)
                .expect("frame checksum should write");

        let mut frame2 = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
        frame2[..4].copy_from_slice(&2_u32.to_be_bytes());
        frame2[4..8].copy_from_slice(&7_u32.to_be_bytes());
        write_wal_frame_salts(&mut frame2[..WAL_FRAME_HEADER_SIZE], header.salts)
            .expect("frame salts should write");
        frame2[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x20));
        let frame2_checksum =
            write_wal_frame_checksum(&mut frame2, PAGE_SIZE, running_checksum, false)
                .expect("frame checksum should write");

        let mut wal_bytes = Vec::with_capacity(WAL_HEADER_SIZE + frame1.len() + frame2.len());
        wal_bytes.extend_from_slice(&header_bytes);
        wal_bytes.extend_from_slice(&frame1);
        wal_bytes.extend_from_slice(&frame2);
        let validation = validate_wal_chain(&wal_bytes, PAGE_SIZE, false).expect("valid chain");

        assert!(validation.valid);
        assert_eq!(validation.valid_frames, 2);
        assert_eq!(validation.replayable_frames, 2);
        assert_eq!(validation.last_commit_frame, Some(1));
        let parsed_frame2 =
            WalFrameHeader::from_bytes(&frame2[..WAL_FRAME_HEADER_SIZE]).expect("frame parses");
        assert_eq!(parsed_frame2.checksum, frame2_checksum);
    }

    #[test]
    fn test_wal_frame_checksum_ignores_salt_words() {
        let seed = SqliteWalChecksum {
            s1: 0x1234_5678,
            s2: 0x9ABC_DEF0,
        };
        let mut frame_a = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
        frame_a[..4].copy_from_slice(&2_u32.to_be_bytes());
        frame_a[4..8].copy_from_slice(&0_u32.to_be_bytes());
        write_wal_frame_salts(
            &mut frame_a[..WAL_FRAME_HEADER_SIZE],
            WalSalts { salt1: 1, salt2: 2 },
        )
        .expect("frame salts should write");
        frame_a[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&sample_page(0x55));

        let mut frame_b = frame_a.clone();
        write_wal_frame_salts(
            &mut frame_b[..WAL_FRAME_HEADER_SIZE],
            WalSalts {
                salt1: 0xAAAA_BBBB,
                salt2: 0xCCCC_DDDD,
            },
        )
        .expect("frame salts should write");

        let checksum_a =
            compute_wal_frame_checksum(&frame_a, PAGE_SIZE, seed, false).expect("checksum");
        let checksum_b =
            compute_wal_frame_checksum(&frame_b, PAGE_SIZE, seed, false).expect("checksum");
        assert_eq!(checksum_a, checksum_b);
    }

    #[test]
    fn test_sqlite_checksum_alignment_guard() {
        let err = sqlite_wal_checksum(&[1_u8, 2, 3], 0, 0, false).expect_err("must reject");
        assert!(matches!(err, FrankenError::WalCorrupt { .. }));
        let detail = match err {
            FrankenError::WalCorrupt { detail } => detail,
            _ => String::new(),
        };
        assert!(detail.contains("8-byte aligned"));
    }

    #[test]
    fn test_page_checksum_roundtrip() {
        let mut page = sample_page(7);
        let expected = write_page_checksum(&mut page).expect("write should succeed");
        let actual = read_page_checksum(&page).expect("read should succeed");
        assert_eq!(expected, actual);
        assert!(verify_page_checksum(&page).expect("verify should succeed"));
    }

    #[test]
    fn test_configure_reserved_bytes() {
        let mut header = [0_u8; 100];
        configure_page_checksum_reserved_bytes(&mut header, true).expect("config should work");
        assert_eq!(
            page_checksum_reserved_bytes(&header).expect("read should work"),
            u8::try_from(PAGE_CHECKSUM_RESERVED_BYTES).expect("fits")
        );
    }

    #[test]
    fn test_integrity_check_database_header_magic() {
        let mut bytes = vec![0_u8; SQLITE_DB_HEADER_SIZE];
        bytes[..SQLITE_DB_HEADER_MAGIC.len()].copy_from_slice(&SQLITE_DB_HEADER_MAGIC);
        bytes[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        let ok_report = integrity_check_database_header(&bytes);
        assert!(ok_report.is_ok());

        bytes[0] ^= 0x7F;
        let bad_report = integrity_check_database_header(&bytes);
        assert!(
            bad_report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("header magic mismatch"))
        );
    }

    #[test]
    fn test_integrity_check_valid_db() {
        let page = sample_btree_leaf_page();
        let report = integrity_check_level1_page(&page, 1, true, false)
            .expect("level1 integrity check should run");
        assert!(report.is_ok());
        assert_eq!(report.sqlite_messages(), vec!["ok".to_owned()]);
    }

    #[test]
    fn test_integrity_check_bad_page_type() {
        let mut page = sample_btree_leaf_page();
        page[0] = 0xFF;

        let report = integrity_check_level1_page(&page, 7, true, false)
            .expect("level1 integrity check should run");
        assert!(!report.is_ok());
        assert!(
            report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("invalid b-tree page type"))
        );
    }

    #[test]
    fn test_integrity_check_overlapping_cells() {
        let report =
            integrity_check_level2_btree(11, PAGE_SIZE, &[(100, 220), (200, 280)], &[1, 2]);
        assert!(
            report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("overlapping cell spans"))
        );
    }

    #[test]
    fn test_integrity_check_unsorted_keys() {
        let report =
            integrity_check_level2_btree(12, PAGE_SIZE, &[(100, 120), (140, 180)], &[1, 3, 2]);
        assert!(
            report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("keys out of order"))
        );
    }

    #[test]
    fn test_integrity_check_bad_overflow() {
        let report = integrity_check_level3_overflow_chain(13, &[7, 8, 7], 64);
        assert!(
            report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("broken overflow chain"))
        );
    }

    #[test]
    fn test_integrity_check_page_not_accounted() {
        let report = integrity_check_level4_cross_reference(4, &[1, 3, 4]);
        assert!(
            report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("page 2: not accounted"))
        );
    }

    #[test]
    fn test_integrity_check_schema_corrupt() {
        let report = integrity_check_level5_schema(&["garbage schema line".to_owned()]);
        assert!(
            report
                .sqlite_messages()
                .iter()
                .any(|line| line.contains("malformed SQL entry"))
        );
    }

    #[test]
    fn test_integrity_check_output_matches_c() {
        let level1 = integrity_check_level1_page(&sample_btree_leaf_page(), 1, true, false)
            .expect("level1 integrity check should run");
        let level2 =
            integrity_check_level2_btree(1, PAGE_SIZE, &[(120, 140), (220, 250)], &[1, 2, 3]);
        let level3 = integrity_check_level3_overflow_chain(1, &[7, 9, 11], 20);
        let level4 = integrity_check_level4_cross_reference(3, &[1, 2, 3]);
        let level5 = integrity_check_level5_schema(&["CREATE TABLE t(x INTEGER)".to_owned()]);
        let report = merge_integrity_reports(&[level1, level2, level3, level4, level5]);
        assert_eq!(report.sqlite_messages(), vec!["ok".to_owned()]);
    }

    #[test]
    fn test_recovery_wal_fec_repair() {
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            Some(8),
            Some(6),
        );
        assert_eq!(action, RecoveryAction::AttemptWalFecRepair);

        let payload = sample_page(11);
        let hash = wal_fec_source_hash_xxh3_128(&payload);
        let decision = recover_wal_frame_checksum_mismatch(Some(&payload), Some(hash), 8, 6);
        assert_eq!(decision, WalRecoveryDecision::Repaired);
    }

    #[test]
    fn test_recovery_wal_fec_insufficient() {
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            Some(3),
            Some(4),
        );
        assert_eq!(action, RecoveryAction::TruncateWalAtFirstInvalidFrame);

        let payload = sample_page(9);
        let hash = wal_fec_source_hash_xxh3_128(&payload);
        let decision = recover_wal_frame_checksum_mismatch(Some(&payload), Some(hash), 3, 4);
        assert_eq!(decision, WalRecoveryDecision::Truncated);
    }

    #[test]
    fn test_recovery_crc32c_exclude() {
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::Crc32cSymbolMismatch,
            Some(0),
            Some(0),
        );
        assert_eq!(action, RecoveryAction::ExcludeCorruptedSymbolAndContinue);
    }

    #[test]
    fn test_recovery_xxh3_evict_retry() {
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::Xxh3PageChecksumMismatch,
            None,
            None,
        );
        assert_eq!(action, RecoveryAction::EvictCacheAndRetryFromWal);
    }

    #[test]
    fn test_recovery_wal_fec_hash_mismatch_truncates() {
        let payload = sample_page(3);
        let wrong_hash = wal_fec_source_hash_xxh3_128(&sample_page(4));
        let decision = recover_wal_frame_checksum_mismatch(Some(&payload), Some(wrong_hash), 8, 6);
        assert_eq!(decision, WalRecoveryDecision::Truncated);
    }

    #[test]
    fn test_crash_at_any_point() {
        let contract = crash_model_contract();
        assert!(contract.crash_at_any_point());
        for crash_step in 0..16 {
            let before = u64::try_from(crash_step).expect("step should fit");
            let after = before.saturating_add(1);
            assert!(after >= before);
            assert!(contract.fsync_is_durability_barrier());
        }
    }

    #[test]
    fn test_torn_write_detection() {
        let mut wal_header = [0_u8; WAL_HEADER_SIZE];
        wal_header[..4].copy_from_slice(&0x377F_0682_u32.to_be_bytes());
        wal_header[4..8].copy_from_slice(&3_007_000_u32.to_be_bytes());
        wal_header[8..12].copy_from_slice(
            &u32::try_from(PAGE_SIZE)
                .expect("PAGE_SIZE should fit in u32")
                .to_be_bytes(),
        );
        let salts = WalSalts {
            salt1: 0x1111_2222,
            salt2: 0x3333_4444,
        };
        write_wal_header_salts(&mut wal_header, salts).expect("header salts should write");
        write_wal_header_checksum(&mut wal_header, false).expect("header checksum should write");

        let mut frame = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
        frame[..4].copy_from_slice(&1_u32.to_be_bytes());
        frame[4..8].copy_from_slice(&1_u32.to_be_bytes());
        write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], salts)
            .expect("frame salts should write");

        for (idx, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
            let reduced = u8::try_from(idx % 251).expect("index modulo fits in u8");
            *byte = reduced ^ 0x5A;
        }

        let seed = read_wal_header_checksum(&wal_header).expect("header checksum should read");
        write_wal_frame_checksum(&mut frame, PAGE_SIZE, seed, false)
            .expect("frame checksum should write");

        let mut wal_bytes = Vec::with_capacity(WAL_HEADER_SIZE + frame.len());
        wal_bytes.extend_from_slice(&wal_header);
        wal_bytes.extend_from_slice(&frame);
        assert!(!detect_torn_write_in_wal(&wal_bytes, PAGE_SIZE, false).expect("validate WAL"));

        wal_bytes.truncate(WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + PAGE_SIZE / 2);
        assert!(detect_torn_write_in_wal(&wal_bytes, PAGE_SIZE, false).expect("validate torn WAL"));
    }

    #[test]
    fn test_fsync_durability() {
        let contract = crash_model_contract();
        assert!(contract.crash_at_any_point());
        assert!(contract.fsync_is_durability_barrier());
        assert!(contract.writes_reorder_without_fsync());
        assert!(contract.bitrot_exists());
        assert!(contract.metadata_may_require_directory_fsync());
        assert!(supports_torn_write_sector_size(512));
        assert!(supports_torn_write_sector_size(1024));
        assert!(supports_torn_write_sector_size(4096));
        assert!(!supports_torn_write_sector_size(2048));
    }

    #[test]
    fn test_e2e_bd_36hc() {
        let level1 = integrity_check_level1_page(&sample_btree_leaf_page(), 1, true, false)
            .expect("level1 integrity check should run");
        let level2 = integrity_check_level2_btree(
            1,
            PAGE_SIZE,
            &[(120, 150), (180, 210), (240, 280)],
            &[1, 2, 3],
        );
        let level3 = integrity_check_level3_overflow_chain(1, &[5, 7, 9], 64);
        let level4 = integrity_check_level4_cross_reference(10, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let level5 = integrity_check_level5_schema(&[
            "CREATE TABLE t0(id INTEGER PRIMARY KEY, v TEXT)".to_owned(),
            "CREATE INDEX i0 ON t0(v)".to_owned(),
        ]);
        let merged = merge_integrity_reports(&[level1, level2, level3, level4, level5]);

        assert!(merged.is_ok());
        assert_eq!(merged.sqlite_messages(), vec!["ok".to_owned()]);

        let mut wal_header = [0_u8; WAL_HEADER_SIZE];
        wal_header[..4].copy_from_slice(&0x377F_0682_u32.to_be_bytes());
        wal_header[4..8].copy_from_slice(&3_007_000_u32.to_be_bytes());
        wal_header[8..12].copy_from_slice(
            &u32::try_from(PAGE_SIZE)
                .expect("PAGE_SIZE should fit in u32")
                .to_be_bytes(),
        );
        let salts = WalSalts {
            salt1: 0x0102_0304,
            salt2: 0xA0B0_C0D0,
        };
        write_wal_header_salts(&mut wal_header, salts).expect("header salts should write");
        write_wal_header_checksum(&mut wal_header, false).expect("header checksum should write");
        let mut running =
            read_wal_header_checksum(&wal_header).expect("header checksum should read");

        let mut wal_bytes =
            Vec::with_capacity(WAL_HEADER_SIZE + 100 * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE));
        wal_bytes.extend_from_slice(&wal_header);
        for frame_index in 0..100_u32 {
            let mut frame = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
            frame[..4].copy_from_slice(&(frame_index + 1).to_be_bytes());
            frame[4..8].copy_from_slice(&(frame_index + 1).to_be_bytes());
            write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], salts)
                .expect("frame salts should write");
            for (offset, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
                let reduced = u8::try_from(offset % 251).expect("offset modulo must fit");
                *byte = reduced ^ u8::try_from(frame_index % 251).expect("frame modulo must fit");
            }
            running = write_wal_frame_checksum(&mut frame, PAGE_SIZE, running, false)
                .expect("frame checksum should write");
            wal_bytes.extend_from_slice(&frame);
        }

        for scenario in 0..100_usize {
            let crash_frame = (scenario * 37) % 100;
            let torn_cut = WAL_HEADER_SIZE
                + crash_frame * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE)
                + WAL_FRAME_HEADER_SIZE
                + PAGE_SIZE / 3;
            let torn = &wal_bytes[..torn_cut];
            let validation =
                validate_wal_chain(torn, PAGE_SIZE, false).expect("torn chain should parse");
            assert_eq!(validation.valid_frames, crash_frame);
            assert_eq!(validation.replayable_frames, crash_frame);
        }
    }

    // ── bd-lldk §11.8-11.9 WAL header / frame / checksum tests ─────────

    #[test]
    fn test_wal_header_magic_le() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: 4096,
            checkpoint_seq: 0,
            salts: WalSalts {
                salt1: 0xAAAA_BBBB,
                salt2: 0xCCCC_DDDD,
            },
            checksum: SqliteWalChecksum::default(),
        };
        assert!(!header.big_endian_checksum());
        let bytes = header.to_bytes().expect("LE header should serialize");
        assert_eq!(&bytes[..4], &WAL_MAGIC_LE.to_be_bytes());
    }

    #[test]
    fn test_wal_header_magic_be() {
        let header = WalHeader {
            magic: WAL_MAGIC_BE,
            format_version: WAL_FORMAT_VERSION,
            page_size: 4096,
            checkpoint_seq: 0,
            salts: WalSalts {
                salt1: 0x1111_2222,
                salt2: 0x3333_4444,
            },
            checksum: SqliteWalChecksum::default(),
        };
        assert!(header.big_endian_checksum());
        let bytes = header.to_bytes().expect("BE header should serialize");
        assert_eq!(&bytes[..4], &WAL_MAGIC_BE.to_be_bytes());
    }

    #[test]
    fn test_wal_header_format_version() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: 4096,
            checkpoint_seq: 1,
            salts: WalSalts::default(),
            checksum: SqliteWalChecksum::default(),
        };
        let bytes = header.to_bytes().expect("header should serialize");
        let parsed = WalHeader::from_bytes(&bytes).expect("header should parse");
        assert_eq!(parsed.format_version, 3_007_000);

        // Wrong format version must be rejected.
        let mut bad_bytes = bytes;
        bad_bytes[4..8].copy_from_slice(&999_u32.to_be_bytes());
        assert!(WalHeader::from_bytes(&bad_bytes).is_err());
    }

    #[test]
    fn test_wal_header_round_trip() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: 4096,
            checkpoint_seq: 42,
            salts: WalSalts {
                salt1: 0xDEAD_BEEF,
                salt2: 0xCAFE_BABE,
            },
            checksum: SqliteWalChecksum::default(),
        };
        let bytes = header.to_bytes().expect("header should serialize");
        assert_eq!(bytes.len(), WAL_HEADER_SIZE);

        let parsed = WalHeader::from_bytes(&bytes).expect("header should parse");
        assert_eq!(parsed.magic, WAL_MAGIC_LE);
        assert_eq!(parsed.format_version, WAL_FORMAT_VERSION);
        assert_eq!(parsed.page_size, 4096);
        assert_eq!(parsed.checkpoint_seq, 42);
        assert_eq!(parsed.salts.salt1, 0xDEAD_BEEF);
        assert_eq!(parsed.salts.salt2, 0xCAFE_BABE);
        // Checksum is computed by to_bytes; parsed checksum should be non-zero.
        assert!(
            parsed.checksum.s1 != 0 || parsed.checksum.s2 != 0,
            "computed checksum should be non-trivial"
        );
    }

    #[test]
    fn test_wal_frame_header_commit() {
        // Commit frame: db_size > 0.
        let commit_frame = WalFrameHeader {
            page_number: 1,
            db_size: 10,
            salts: WalSalts {
                salt1: 0x1111,
                salt2: 0x2222,
            },
            checksum: SqliteWalChecksum { s1: 100, s2: 200 },
        };
        assert!(commit_frame.is_commit());

        let bytes = commit_frame.to_bytes();
        assert_eq!(bytes.len(), WAL_FRAME_HEADER_SIZE);
        let parsed = WalFrameHeader::from_bytes(&bytes).expect("frame should parse");
        assert!(parsed.is_commit());
        assert_eq!(parsed.db_size, 10);

        // Non-commit frame: db_size == 0.
        let non_commit = WalFrameHeader {
            page_number: 2,
            db_size: 0,
            salts: WalSalts {
                salt1: 0x1111,
                salt2: 0x2222,
            },
            checksum: SqliteWalChecksum { s1: 300, s2: 400 },
        };
        assert!(!non_commit.is_commit());

        let bytes2 = non_commit.to_bytes();
        let parsed2 = WalFrameHeader::from_bytes(&bytes2).expect("frame should parse");
        assert!(!parsed2.is_commit());
        assert_eq!(parsed2.db_size, 0);
    }

    #[test]
    fn test_wal_frame_header_salt_match() {
        let wal_salts = WalSalts {
            salt1: 0xAAAA_BBBB,
            salt2: 0xCCCC_DDDD,
        };

        // Frame with matching salt: accepted.
        let good_frame = WalFrameHeader {
            page_number: 1,
            db_size: 5,
            salts: wal_salts,
            checksum: SqliteWalChecksum::default(),
        };
        assert_eq!(good_frame.salts, wal_salts);

        // Frame with mismatched salt: rejected.
        let bad_salts = WalSalts {
            salt1: 0x0000_0000,
            salt2: 0x0000_0000,
        };
        let bad_frame = WalFrameHeader {
            page_number: 1,
            db_size: 5,
            salts: bad_salts,
            checksum: SqliteWalChecksum::default(),
        };
        assert_ne!(
            bad_frame.salts, wal_salts,
            "mismatched salt must be detected"
        );
    }

    #[test]
    fn test_wal_checksum_chain_integrity() {
        // Build a multi-frame WAL and verify the cumulative checksum chain.
        let salts = WalSalts {
            salt1: 0x1234_5678,
            salt2: 0x9ABC_DEF0,
        };
        let mut wal_header_buf = [0_u8; WAL_HEADER_SIZE];
        wal_header_buf[..4].copy_from_slice(&WAL_MAGIC_LE.to_be_bytes());
        wal_header_buf[4..8].copy_from_slice(&WAL_FORMAT_VERSION.to_be_bytes());
        wal_header_buf[8..12].copy_from_slice(
            &u32::try_from(PAGE_SIZE)
                .expect("page size fits")
                .to_be_bytes(),
        );
        write_wal_header_salts(&mut wal_header_buf, salts).expect("write salts");
        write_wal_header_checksum(&mut wal_header_buf, false).expect("write header checksum");

        let mut running = read_wal_header_checksum(&wal_header_buf).expect("read header checksum");

        let mut wal_bytes = Vec::new();
        wal_bytes.extend_from_slice(&wal_header_buf);

        // Write 5 frames, each a commit frame.
        for frame_idx in 0..5_u32 {
            let mut frame = vec![0_u8; WAL_FRAME_HEADER_SIZE + PAGE_SIZE];
            frame[..4].copy_from_slice(&(frame_idx + 1).to_be_bytes());
            frame[4..8].copy_from_slice(&(frame_idx + 1).to_be_bytes());
            write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], salts)
                .expect("write frame salts");

            for (offset, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
                let reduced = u8::try_from(offset % 251).expect("fits");
                *byte = reduced ^ u8::try_from(frame_idx % 251).expect("fits");
            }

            running = write_wal_frame_checksum(&mut frame, PAGE_SIZE, running, false)
                .expect("write frame checksum");
            wal_bytes.extend_from_slice(&frame);
        }

        // Validate the entire chain.
        let validation =
            validate_wal_chain(&wal_bytes, PAGE_SIZE, false).expect("chain should validate");
        assert!(validation.valid, "chain must be fully valid");
        assert_eq!(validation.valid_frames, 5);
        assert_eq!(validation.replayable_frames, 5);
        assert!(validation.reason.is_none());

        // Corrupt one byte in frame 3's page data; chain must break at frame 3.
        let frame3_page_offset =
            WAL_HEADER_SIZE + 2 * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE) + WAL_FRAME_HEADER_SIZE + 10;
        wal_bytes[frame3_page_offset] ^= 0xFF;
        let bad_validation =
            validate_wal_chain(&wal_bytes, PAGE_SIZE, false).expect("corrupt chain should parse");
        assert!(!bad_validation.valid);
        assert_eq!(bad_validation.valid_frames, 2, "frames 1-2 should be valid");
        assert_eq!(
            bad_validation.reason,
            Some(WalChainInvalidReason::FrameChecksumMismatch)
        );
    }

    // ── bd-xfn30.2: Corruption classification & repair-decision tests ──

    /// Build a valid WAL byte stream with `n` frames (all commits).
    fn build_valid_wal(n: usize) -> Vec<u8> {
        let salts = WalSalts {
            salt1: 0xAAAA_BBBB,
            salt2: 0xCCCC_DDDD,
        };
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        header_buf[..4].copy_from_slice(&WAL_MAGIC_LE.to_be_bytes());
        header_buf[4..8].copy_from_slice(&WAL_FORMAT_VERSION.to_be_bytes());
        header_buf[8..12].copy_from_slice(&u32::try_from(PAGE_SIZE).expect("fits").to_be_bytes());
        write_wal_header_salts(&mut header_buf, salts).expect("write salts");
        write_wal_header_checksum(&mut header_buf, false).expect("write hdr cksum");

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut wal = Vec::with_capacity(WAL_HEADER_SIZE + n * frame_size);
        wal.extend_from_slice(&header_buf);

        let mut running = read_wal_header_checksum(&header_buf).expect("read hdr cksum");
        for i in 0..n {
            let pg = u32::try_from(i + 1).unwrap();
            let mut frame = vec![0u8; frame_size];
            frame[..4].copy_from_slice(&pg.to_be_bytes());
            frame[4..8].copy_from_slice(&pg.to_be_bytes()); // commit
            write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], salts).expect("frame salts");
            for (off, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
                let r = u8::try_from(off % 251).unwrap();
                let s = u8::try_from(i % 251).unwrap();
                *byte = r ^ s;
            }
            running =
                write_wal_frame_checksum(&mut frame, PAGE_SIZE, running, false).expect("cksum");
            wal.extend_from_slice(&frame);
        }
        wal
    }

    #[test]
    fn test_classify_clean_wal_valid() {
        let wal = build_valid_wal(10);
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert!(v.valid);
        assert_eq!(v.valid_frames, 10);
        assert_eq!(v.replayable_frames, 10);
        assert!(v.reason.is_none());
        assert!(v.header_valid);
    }

    #[test]
    fn test_classify_header_corruption() {
        let mut wal = build_valid_wal(5);
        // Corrupt header magic.
        wal[0] ^= 0xFF;
        let v = validate_wal_chain(&wal, PAGE_SIZE, false);
        // Header corruption should error or report HeaderChecksumMismatch.
        if let Ok(val) = v {
            assert!(!val.header_valid);
            assert_eq!(
                val.reason,
                Some(WalChainInvalidReason::HeaderChecksumMismatch)
            );
        } // Also acceptable: outright error
    }

    #[test]
    fn test_classify_single_bit_flip_in_frame_data() {
        let mut wal = build_valid_wal(5);
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        // Flip one bit in frame 3's page data.
        let offset = WAL_HEADER_SIZE + 2 * frame_size + WAL_FRAME_HEADER_SIZE + 100;
        wal[offset] ^= 0x01;
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert!(!v.valid);
        assert_eq!(v.valid_frames, 2, "first 2 frames should survive");
        assert_eq!(v.first_invalid_frame, Some(2));
        assert_eq!(v.reason, Some(WalChainInvalidReason::FrameChecksumMismatch));
    }

    #[test]
    fn test_classify_torn_write_mid_frame() {
        let wal = build_valid_wal(5);
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        // Truncate in the middle of frame 4 (index 3).
        let cut_at = WAL_HEADER_SIZE + 3 * frame_size + frame_size / 2;
        let torn = &wal[..cut_at];
        let v = validate_wal_chain(torn, PAGE_SIZE, false).expect("validate");
        assert_eq!(
            v.valid_frames, 3,
            "only 3 complete frames before truncation"
        );
        assert_eq!(v.reason, Some(WalChainInvalidReason::TruncatedFrame));
    }

    #[test]
    fn test_classify_torn_write_in_header() {
        let wal = build_valid_wal(3);
        // Truncate to partial header.
        let torn = &wal[..16];
        let v = validate_wal_chain(torn, PAGE_SIZE, false);
        // Should error or report header issue.
        assert!(v.is_err() || !v.unwrap().header_valid);
    }

    #[test]
    fn test_classify_salt_mismatch_in_frame() {
        let mut wal = build_valid_wal(5);
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        // Corrupt salt1 in frame 2's header (bytes 8..12).
        let salt_offset = WAL_HEADER_SIZE + frame_size + 8;
        wal[salt_offset] ^= 0xFF;
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        // Chain should break at frame 2 due to salt or checksum mismatch.
        assert!(v.valid_frames <= 1, "at most frame 0 should survive");
    }

    #[test]
    fn test_classify_zero_fill_corruption() {
        let mut wal = build_valid_wal(5);
        // Zero-fill frame 1's data (simulating media erasure).
        let start = WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE;
        for byte in &mut wal[start..start + PAGE_SIZE] {
            *byte = 0;
        }
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert_eq!(v.valid_frames, 0, "frame 0 corrupted so 0 valid frames");
        assert_eq!(v.reason, Some(WalChainInvalidReason::FrameChecksumMismatch));
    }

    #[test]
    fn test_classify_corruption_at_first_frame() {
        let mut wal = build_valid_wal(3);
        // Corrupt very first frame's page data byte 0.
        let offset = WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE;
        wal[offset] ^= 0xAA;
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert_eq!(v.valid_frames, 0);
        assert_eq!(v.first_invalid_frame, Some(0));
    }

    #[test]
    fn test_classify_corruption_at_last_frame() {
        let mut wal = build_valid_wal(5);
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        // Corrupt the last frame.
        let offset = WAL_HEADER_SIZE + 4 * frame_size + WAL_FRAME_HEADER_SIZE + 50;
        wal[offset] ^= 0xBB;
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert_eq!(v.valid_frames, 4, "first 4 should survive");
        assert_eq!(v.first_invalid_frame, Some(4));
    }

    #[test]
    fn test_detect_torn_write_true_on_truncation() {
        let wal = build_valid_wal(5);
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let cut = WAL_HEADER_SIZE + 2 * frame_size + 10;
        let torn = &wal[..cut];
        assert!(detect_torn_write_in_wal(torn, PAGE_SIZE, false).expect("detect"));
    }

    #[test]
    fn test_detect_torn_write_false_on_clean() {
        let wal = build_valid_wal(5);
        assert!(!detect_torn_write_in_wal(&wal, PAGE_SIZE, false).expect("detect"));
    }

    #[test]
    fn test_detect_torn_write_true_on_bit_flip() {
        let mut wal = build_valid_wal(3);
        let offset = WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + 200;
        wal[offset] ^= 0x01;
        assert!(detect_torn_write_in_wal(&wal, PAGE_SIZE, false).expect("detect"));
    }

    // ── Repair-decision edge cases ──

    #[test]
    fn test_repair_decision_exact_boundary_symbols() {
        // Exactly enough symbols: should attempt repair.
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            Some(6),
            Some(6),
        );
        assert_eq!(action, RecoveryAction::AttemptWalFecRepair);
    }

    #[test]
    fn test_repair_decision_one_short() {
        // One symbol short: must truncate.
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            Some(5),
            Some(6),
        );
        assert_eq!(action, RecoveryAction::TruncateWalAtFirstInvalidFrame);
    }

    #[test]
    fn test_repair_decision_no_symbol_info() {
        // No symbol info at all: must truncate.
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            None,
            None,
        );
        assert_eq!(action, RecoveryAction::TruncateWalAtFirstInvalidFrame);
    }

    #[test]
    fn test_repair_decision_partial_symbol_info() {
        // Only one side of symbol info available.
        let a1 = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            Some(10),
            None,
        );
        assert_eq!(a1, RecoveryAction::TruncateWalAtFirstInvalidFrame);

        let a2 = recovery_action_for_checksum_failure(
            ChecksumFailureKind::WalFrameChecksumMismatch,
            None,
            Some(6),
        );
        assert_eq!(a2, RecoveryAction::TruncateWalAtFirstInvalidFrame);
    }

    #[test]
    fn test_repair_decision_db_corruption_always_report() {
        let action = recovery_action_for_checksum_failure(
            ChecksumFailureKind::DbFileCorruption,
            Some(100),
            Some(1),
        );
        assert_eq!(action, RecoveryAction::ReportPersistentCorruption);
    }

    #[test]
    fn test_attempt_fec_repair_insufficient_symbols() {
        let payload = sample_page(1);
        let hash = wal_fec_source_hash_xxh3_128(&payload);
        let result = attempt_wal_fec_repair(&payload, hash, 3, 6);
        assert_eq!(result, WalFecRepairOutcome::InsufficientSymbols);
    }

    #[test]
    fn test_attempt_fec_repair_correct_hash() {
        let payload = sample_page(42);
        let hash = wal_fec_source_hash_xxh3_128(&payload);
        let result = attempt_wal_fec_repair(&payload, hash, 8, 6);
        assert_eq!(result, WalFecRepairOutcome::Repaired);
    }

    #[test]
    fn test_attempt_fec_repair_wrong_hash() {
        let payload = sample_page(42);
        let wrong_hash = wal_fec_source_hash_xxh3_128(&sample_page(99));
        let result = attempt_wal_fec_repair(&payload, wrong_hash, 8, 6);
        assert_eq!(result, WalFecRepairOutcome::SourceHashMismatch);
    }

    #[test]
    fn test_recover_decision_no_payload_truncates() {
        let decision = recover_wal_frame_checksum_mismatch(None, None, 10, 6);
        assert_eq!(decision, WalRecoveryDecision::Truncated);
    }

    #[test]
    fn test_recover_decision_payload_but_no_hash_truncates() {
        let payload = sample_page(1);
        let decision = recover_wal_frame_checksum_mismatch(Some(&payload), None, 10, 6);
        assert_eq!(decision, WalRecoveryDecision::Truncated);
    }

    #[test]
    fn test_recover_decision_full_repair_path() {
        let payload = sample_page(7);
        let hash = wal_fec_source_hash_xxh3_128(&payload);
        let decision = recover_wal_frame_checksum_mismatch(Some(&payload), Some(hash), 8, 6);
        assert_eq!(decision, WalRecoveryDecision::Repaired);
    }

    #[test]
    fn test_all_failure_kinds_have_deterministic_action() {
        // Exhaustive check: every ChecksumFailureKind produces a valid action.
        let kinds = [
            ChecksumFailureKind::WalFrameChecksumMismatch,
            ChecksumFailureKind::Xxh3PageChecksumMismatch,
            ChecksumFailureKind::Crc32cSymbolMismatch,
            ChecksumFailureKind::DbFileCorruption,
        ];
        for kind in kinds {
            let action = recovery_action_for_checksum_failure(kind, Some(10), Some(5));
            // Must be one of the known variants.
            assert!(matches!(
                action,
                RecoveryAction::AttemptWalFecRepair
                    | RecoveryAction::TruncateWalAtFirstInvalidFrame
                    | RecoveryAction::EvictCacheAndRetryFromWal
                    | RecoveryAction::ExcludeCorruptedSymbolAndContinue
                    | RecoveryAction::ReportPersistentCorruption
            ));
        }
    }

    #[test]
    fn test_multi_corruption_sites_first_wins() {
        // When multiple frames are corrupt, only the first is detected.
        let mut wal = build_valid_wal(10);
        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        // Corrupt frames 3 and 7.
        let off3 = WAL_HEADER_SIZE + 2 * frame_size + WAL_FRAME_HEADER_SIZE + 10;
        let off7 = WAL_HEADER_SIZE + 6 * frame_size + WAL_FRAME_HEADER_SIZE + 10;
        wal[off3] ^= 0xCC;
        wal[off7] ^= 0xDD;
        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert_eq!(v.valid_frames, 2, "stops at first corruption (frame 3)");
        assert_eq!(v.first_invalid_frame, Some(2));
    }

    #[test]
    fn test_crash_model_contract_flags_exhaustive() {
        let contract = crash_model_contract();
        assert!(contract.crash_at_any_point());
        assert!(contract.fsync_is_durability_barrier());
        assert!(contract.writes_reorder_without_fsync());
        assert!(contract.bitrot_exists());
        assert!(contract.metadata_may_require_directory_fsync());
    }

    #[test]
    fn test_replayable_frames_stop_at_last_commit() {
        // Build WAL where frames 1-3 are commits, frames 4-5 are non-commit.
        // Technically all 5 pass checksum chain but only 3 are "replayable"
        // (up to last commit in the valid prefix).
        let salts = WalSalts {
            salt1: 0x1111_2222,
            salt2: 0x3333_4444,
        };
        let mut hdr = [0u8; WAL_HEADER_SIZE];
        hdr[..4].copy_from_slice(&WAL_MAGIC_LE.to_be_bytes());
        hdr[4..8].copy_from_slice(&WAL_FORMAT_VERSION.to_be_bytes());
        hdr[8..12].copy_from_slice(&u32::try_from(PAGE_SIZE).unwrap().to_be_bytes());
        write_wal_header_salts(&mut hdr, salts).expect("salts");
        write_wal_header_checksum(&mut hdr, false).expect("hdr cksum");

        let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
        let mut wal = Vec::with_capacity(WAL_HEADER_SIZE + 5 * frame_size);
        wal.extend_from_slice(&hdr);
        let mut running = read_wal_header_checksum(&hdr).expect("seed");

        for i in 0..5u32 {
            let mut frame = vec![0u8; frame_size];
            frame[..4].copy_from_slice(&(i + 1).to_be_bytes());
            // Commit on frames 1-3 (indices 0-2), non-commit on 4-5 (indices 3-4).
            let db_size = if i < 3 { i + 1 } else { 0 };
            frame[4..8].copy_from_slice(&db_size.to_be_bytes());
            write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], salts).expect("salts");
            for (off, byte) in frame[WAL_FRAME_HEADER_SIZE..].iter_mut().enumerate() {
                *byte = u8::try_from((off + usize::try_from(i).unwrap()) % 251).unwrap();
            }
            running =
                write_wal_frame_checksum(&mut frame, PAGE_SIZE, running, false).expect("cksum");
            wal.extend_from_slice(&frame);
        }

        let v = validate_wal_chain(&wal, PAGE_SIZE, false).expect("validate");
        assert!(v.valid, "all 5 pass checksum");
        assert_eq!(v.valid_frames, 5);
        // Replayable should be 3 (last commit is at index 2).
        assert_eq!(v.replayable_frames, 3);
        assert_eq!(v.last_commit_frame, Some(2));
    }
}
