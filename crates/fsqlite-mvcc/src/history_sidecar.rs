//! Durable `.fsqlite-history` commit-snapshot sidecar.
//!
//! The sidecar is lookup metadata only. It never substitutes for the WAL or
//! for durably retained page versions. The format is deliberately explicit:
//! one 4096-byte header slot followed by immutable 4096-byte record slots whose
//! first 64 bytes are the v1 record. See
//! `docs/design/fsqlite-history-format.md` for the normative byte layout.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use fsqlite_error::FrankenError;
use fsqlite_types::flags::{AccessFlags, VfsOpenFlags};
use fsqlite_types::{Cx, LockLevel};
use fsqlite_vfs::traits::{SyncKind, Vfs, VfsFile};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Size of every history header and record slot.
pub const HISTORY_SLOT_SIZE: usize = 4096;
/// Size of the stable v1 record prefix in each record slot.
pub const HISTORY_RECORD_V1_SIZE: usize = 64;
/// Sparse-index sampling interval.
pub const HISTORY_INDEX_STRIDE: u64 = 1024;
/// Current history file format version.
pub const HISTORY_FORMAT_VERSION: u16 = 1;
/// Current record prefix version.
pub const HISTORY_RECORD_VERSION: u16 = 1;
/// Marks a record as an independently retained checkpoint anchor.
pub const HISTORY_FLAG_CHECKPOINT_ANCHOR: u32 = 1;

const HISTORY_MAGIC: [u8; 8] = *b"FSQLHST1";
const INDEX_MAGIC: [u8; 8] = *b"FSQLHIX1";
const INDEX_ENTRY_HASH_DOMAIN: [u8; 8] = *b"FSQLHIXE";
const HEADER_V1_SIZE: usize = 64;
const INDEX_HEADER_V1_SIZE: usize = 80;
const INDEX_ENTRY_SIZE: usize = 24;
const HEADER_CHECKSUM_RANGE: std::ops::Range<usize> = 52..60;
const RECORD_CHECKSUM_RANGE: std::ops::Range<usize> = 32..40;
const INDEX_HEADER_CHECKSUM_RANGE: std::ops::Range<usize> = 72..80;

/// Stable identity of one logical database history lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatabaseHistoryId(pub [u8; 16]);

impl DatabaseHistoryId {
    /// The all-zero identity is reserved and cannot identify a history file.
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.0.iter().any(|byte| *byte != 0)
    }
}

/// Immutable identity and generation metadata stored in the first slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryHeader {
    /// Persistent identity of the logical database history.
    pub database_history_id: DatabaseHistoryId,
    /// Generation of the history coordinate/retention format.
    pub format_generation: u64,
    /// Recovered database/WAL lineage generation.
    pub database_generation: u64,
}

impl HistoryHeader {
    /// Construct a valid v1 header.
    pub fn new(
        database_history_id: DatabaseHistoryId,
        format_generation: u64,
        database_generation: u64,
    ) -> Result<Self, HistoryError> {
        if !database_history_id.is_valid() {
            return Err(HistoryError::InvalidInput(
                "database_history_id must not be all zero".to_owned(),
            ));
        }
        if format_generation == 0 {
            return Err(HistoryError::InvalidInput(
                "format_generation must be non-zero".to_owned(),
            ));
        }
        if database_generation == 0 {
            return Err(HistoryError::InvalidInput(
                "database_generation must be non-zero".to_owned(),
            ));
        }
        Ok(Self {
            database_history_id,
            format_generation,
            database_generation,
        })
    }

    /// Encode this header into its complete 4096-byte slot.
    #[must_use]
    pub fn encode_slot(self) -> [u8; HISTORY_SLOT_SIZE] {
        let mut slot = [0_u8; HISTORY_SLOT_SIZE];
        slot[0..8].copy_from_slice(&HISTORY_MAGIC);
        put_u16(&mut slot, 8, HISTORY_FORMAT_VERSION);
        put_u16(
            &mut slot,
            10,
            u16::try_from(HEADER_V1_SIZE).expect("header size fits u16"),
        );
        put_u32(
            &mut slot,
            12,
            u32::try_from(HISTORY_SLOT_SIZE).expect("slot size fits u32"),
        );
        put_u16(
            &mut slot,
            16,
            u16::try_from(HISTORY_RECORD_V1_SIZE).expect("record size fits u16"),
        );
        put_u16(&mut slot, 18, 1); // BLAKE3-64, little-endian truncation.
        put_u64(&mut slot, 20, self.format_generation);
        slot[28..44].copy_from_slice(&self.database_history_id.0);
        put_u64(&mut slot, 44, self.database_generation);
        let checksum = checksum_with_zeroed_range(&slot[..HEADER_V1_SIZE], HEADER_CHECKSUM_RANGE);
        put_u64(&mut slot, 52, checksum);
        slot
    }

    /// Decode and validate a complete header slot.
    pub fn decode_slot(slot: &[u8]) -> Result<Self, HistoryError> {
        if slot.len() < HISTORY_SLOT_SIZE {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: format!(
                    "short header slot: expected {HISTORY_SLOT_SIZE} bytes, got {}",
                    slot.len()
                ),
            });
        }
        if slot[0..8] != HISTORY_MAGIC {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "history magic mismatch".to_owned(),
            });
        }
        let version = get_u16(slot, 8);
        if version != HISTORY_FORMAT_VERSION {
            return Err(HistoryError::UnsupportedFormatVersion(version));
        }
        if get_u16(slot, 10) != u16::try_from(HEADER_V1_SIZE).expect("header size fits u16")
            || get_u32(slot, 12) != u32::try_from(HISTORY_SLOT_SIZE).expect("slot size fits u32")
            || get_u16(slot, 16)
                != u16::try_from(HISTORY_RECORD_V1_SIZE).expect("record size fits u16")
            || get_u16(slot, 18) != 1
        {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "unsupported header dimensions or hash algorithm".to_owned(),
            });
        }
        let stored_checksum = get_u64(slot, 52);
        let actual_checksum =
            checksum_with_zeroed_range(&slot[..HEADER_V1_SIZE], HEADER_CHECKSUM_RANGE);
        if stored_checksum != actual_checksum {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "header checksum mismatch".to_owned(),
            });
        }
        if slot[60..HEADER_V1_SIZE].iter().any(|byte| *byte != 0) {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "v1 header reserved bytes are non-zero".to_owned(),
            });
        }
        let mut id = [0_u8; 16];
        id.copy_from_slice(&slot[28..44]);
        Self::new(DatabaseHistoryId(id), get_u64(slot, 20), get_u64(slot, 44))
    }
}

/// Caller-supplied fields for an appended commit snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRecordDraft {
    /// Canonical durable commit coordinate.
    pub commit_seq: u64,
    /// Historical catalog root from which table/index roots are discovered.
    pub catalog_root_page: u64,
    /// Informational wall-clock timestamp in Unix nanoseconds.
    pub wall_ts_unix_nanos: u64,
    /// Schema epoch visible at this commit.
    pub schema_epoch: u64,
    /// V1 history flags.
    pub flags: u32,
}

impl HistoryRecordDraft {
    fn validate(self) -> Result<(), HistoryError> {
        if self.commit_seq == 0 {
            return Err(HistoryError::InvalidInput(
                "commit_seq must be non-zero".to_owned(),
            ));
        }
        if self.catalog_root_page == 0 {
            return Err(HistoryError::InvalidInput(
                "catalog_root_page must be non-zero".to_owned(),
            ));
        }
        if self.flags & !HISTORY_FLAG_CHECKPOINT_ANCHOR != 0 {
            return Err(HistoryError::InvalidInput(format!(
                "unknown v1 history flags: {:#x}",
                self.flags & !HISTORY_FLAG_CHECKPOINT_ANCHOR
            )));
        }
        Ok(())
    }
}

/// Decoded stable v1 record prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRecord {
    /// Canonical durable commit coordinate.
    pub commit_seq: u64,
    /// Historical catalog root coordinate.
    pub catalog_root_page: u64,
    /// Informational wall-clock timestamp in Unix nanoseconds.
    pub wall_ts_unix_nanos: u64,
    /// Hash stored by the immediately preceding record, or zero at the anchor.
    pub prev_record_blake3_64: u64,
    /// BLAKE3-64 of this v1 record with this field zeroed.
    pub this_record_blake3_64: u64,
    /// Schema epoch visible at this commit.
    pub schema_epoch: u64,
    /// V1 history flags.
    pub flags: u32,
    /// Record prefix version.
    pub record_version: u16,
}

impl HistoryRecord {
    fn from_draft(draft: HistoryRecordDraft, previous_hash: u64) -> Result<Self, HistoryError> {
        draft.validate()?;
        let mut record = Self {
            commit_seq: draft.commit_seq,
            catalog_root_page: draft.catalog_root_page,
            wall_ts_unix_nanos: draft.wall_ts_unix_nanos,
            prev_record_blake3_64: previous_hash,
            this_record_blake3_64: 0,
            schema_epoch: draft.schema_epoch,
            flags: draft.flags,
            record_version: HISTORY_RECORD_VERSION,
        };
        record.this_record_blake3_64 = record.computed_checksum();
        Ok(record)
    }

    /// Encode the stable 64-byte v1 prefix.
    #[must_use]
    pub fn encode(self) -> [u8; HISTORY_RECORD_V1_SIZE] {
        let mut bytes = [0_u8; HISTORY_RECORD_V1_SIZE];
        put_u64(&mut bytes, 0, self.commit_seq);
        put_u64(&mut bytes, 8, self.catalog_root_page);
        put_u64(&mut bytes, 16, self.wall_ts_unix_nanos);
        put_u64(&mut bytes, 24, self.prev_record_blake3_64);
        put_u64(&mut bytes, 32, self.this_record_blake3_64);
        put_u64(&mut bytes, 40, self.schema_epoch);
        put_u32(&mut bytes, 48, self.flags);
        put_u16(&mut bytes, 52, self.record_version);
        // bytes 54..64 are v1 padding/reserved and remain zero.
        bytes
    }

    /// Decode and validate the stable v1 prefix. Bytes later in the enclosing
    /// 4096-byte slot are intentionally ignored.
    pub fn decode(bytes: &[u8]) -> Result<Self, HistoryError> {
        if bytes.len() < HISTORY_RECORD_V1_SIZE {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: format!(
                    "short record prefix: expected {HISTORY_RECORD_V1_SIZE} bytes, got {}",
                    bytes.len()
                ),
            });
        }
        let record_version = get_u16(bytes, 52);
        if record_version != HISTORY_RECORD_VERSION {
            return Err(HistoryError::UnsupportedRecordVersion(record_version));
        }
        if bytes[54..HISTORY_RECORD_V1_SIZE]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "v1 record padding/reserved bytes are non-zero".to_owned(),
            });
        }
        let record = Self {
            commit_seq: get_u64(bytes, 0),
            catalog_root_page: get_u64(bytes, 8),
            wall_ts_unix_nanos: get_u64(bytes, 16),
            prev_record_blake3_64: get_u64(bytes, 24),
            this_record_blake3_64: get_u64(bytes, 32),
            schema_epoch: get_u64(bytes, 40),
            flags: get_u32(bytes, 48),
            record_version,
        };
        HistoryRecordDraft {
            commit_seq: record.commit_seq,
            catalog_root_page: record.catalog_root_page,
            wall_ts_unix_nanos: record.wall_ts_unix_nanos,
            schema_epoch: record.schema_epoch,
            flags: record.flags,
        }
        .validate()?;
        if record.computed_checksum() != record.this_record_blake3_64 {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "record checksum mismatch".to_owned(),
            });
        }
        Ok(record)
    }

    fn computed_checksum(self) -> u64 {
        checksum_with_zeroed_range(&self.encode(), RECORD_CHECKSUM_RANGE)
    }

    fn encode_slot(self) -> [u8; HISTORY_SLOT_SIZE] {
        let mut slot = [0_u8; HISTORY_SLOT_SIZE];
        slot[..HISTORY_RECORD_V1_SIZE].copy_from_slice(&self.encode());
        slot
    }
}

/// Exact identity and recovery state expected by a sidecar opener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryExpectations {
    /// Expected persistent logical history identity.
    pub database_history_id: DatabaseHistoryId,
    /// Expected history coordinate/retention format generation.
    pub format_generation: u64,
    /// Expected recovered database/WAL lineage generation.
    pub database_generation: u64,
    /// Last commit known durable and reachable after main database/WAL recovery.
    pub recovered_commit_horizon: u64,
}

impl HistoryExpectations {
    fn header(self) -> Result<HistoryHeader, HistoryError> {
        HistoryHeader::new(
            self.database_history_id,
            self.format_generation,
            self.database_generation,
        )
    }
}

/// Result of validating and, when safe, repairing a sidecar tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Records retained after recovery.
    pub valid_records: u64,
    /// Complete records beyond the recovered horizon or an invalid final slot.
    pub truncated_records: u64,
    /// Bytes removed from an incomplete final slot.
    pub truncated_partial_bytes: u64,
    /// Durable final length after recovery.
    pub final_file_len: u64,
}

/// One sparse-index sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SparseIndexEntry {
    /// Commit sequence found at this sample.
    pub commit_seq: u64,
    /// Byte offset of the corresponding record slot in the history file.
    pub byte_offset: u64,
}

/// Validated sparse index bound to one exact history tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseIndex {
    entries: Vec<SparseIndexEntry>,
    history_record_count: u64,
    last_record_hash: u64,
}

impl SparseIndex {
    /// Validated samples in ascending commit order.
    #[must_use]
    pub fn entries(&self) -> &[SparseIndexEntry] {
        &self.entries
    }

    /// Number of history records to which this index is bound.
    #[must_use]
    pub const fn history_record_count(&self) -> u64 {
        self.history_record_count
    }

    /// Build a sparse index for a monotone commit-sequence source.
    pub fn build_with<F>(
        history_record_count: u64,
        last_record_hash: u64,
        mut read_commit_seq: F,
    ) -> Result<Self, HistoryError>
    where
        F: FnMut(u64) -> Result<u64, HistoryError>,
    {
        let capacity_u64 = history_record_count.div_ceil(HISTORY_INDEX_STRIDE);
        let capacity = usize::try_from(capacity_u64).map_err(|_| {
            HistoryError::InvalidInput("sparse index exceeds address space".to_owned())
        })?;
        let mut entries = Vec::with_capacity(capacity);
        let mut index = 0_u64;
        let mut previous_seq = 0_u64;
        while index < history_record_count {
            let commit_seq = read_commit_seq(index)?;
            if commit_seq <= previous_seq {
                return Err(HistoryError::Corrupt {
                    slot: Some(index),
                    reason: "commit sequence is not strictly increasing".to_owned(),
                });
            }
            entries.push(SparseIndexEntry {
                commit_seq,
                byte_offset: record_offset(index)?,
            });
            previous_seq = commit_seq;
            index = index.checked_add(HISTORY_INDEX_STRIDE).ok_or_else(|| {
                HistoryError::InvalidInput("sparse index position overflow".to_owned())
            })?;
        }
        Ok(Self {
            entries,
            history_record_count,
            last_record_hash,
        })
    }
}

/// Instrumentation for one bisect operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SearchStats {
    /// Sparse-index entries compared during binary search.
    pub index_probes: u64,
    /// History records inspected after choosing a range.
    pub record_probes: u64,
    /// All VFS reads issued against the history file from lookup entry.
    pub history_read_calls: u64,
    /// All VFS reads issued against the sparse-index file from lookup entry.
    pub index_read_calls: u64,
    /// Bytes requested by all counted history-file reads.
    pub history_bytes_read: u64,
    /// Bytes requested by all counted sparse-index-file reads.
    pub index_bytes_read: u64,
}

/// Position and proof counters returned by a floor lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOutcome {
    /// Record index whose commit sequence is greatest and no later than target.
    pub record_index: Option<u64>,
    /// Measured lookup work.
    pub stats: SearchStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistoryTail {
    record_count: u64,
    last_record_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SparseIndexBinding {
    tail: HistoryTail,
    entry_count: u64,
}

/// Linear floor lookup used as the benchmark/control implementation.
pub fn linear_bisect_floor_with<F>(
    record_count: u64,
    target_commit_seq: u64,
    mut read_commit_seq: F,
) -> Result<SearchOutcome, HistoryError>
where
    F: FnMut(u64) -> Result<u64, HistoryError>,
{
    let mut outcome = SearchOutcome {
        record_index: None,
        stats: SearchStats::default(),
    };
    for index in 0..record_count {
        let commit_seq = read_commit_seq(index)?;
        outcome.stats.record_probes += 1;
        if commit_seq > target_commit_seq {
            break;
        }
        outcome.record_index = Some(index);
    }
    Ok(outcome)
}

/// Sparse-index floor lookup. Binary search is `O(log N)` and the fixed-size
/// refinement window reads at most 1024 history records.
pub fn sparse_bisect_floor_with<F>(
    sparse_index: &SparseIndex,
    target_commit_seq: u64,
    mut read_commit_seq: F,
) -> Result<SearchOutcome, HistoryError>
where
    F: FnMut(u64) -> Result<u64, HistoryError>,
{
    let mut outcome = SearchOutcome {
        record_index: None,
        stats: SearchStats::default(),
    };
    if sparse_index.history_record_count == 0 {
        return Ok(outcome);
    }

    let mut low = 0_usize;
    let mut high = sparse_index.entries.len();
    while low < high {
        let mid = low + (high - low) / 2;
        outcome.stats.index_probes += 1;
        if sparse_index.entries[mid].commit_seq <= target_commit_seq {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let start_index = if low == 0 {
        0
    } else {
        u64::try_from(low - 1)
            .expect("index position fits u64")
            .checked_mul(HISTORY_INDEX_STRIDE)
            .ok_or_else(|| HistoryError::InvalidInput("lookup position overflow".to_owned()))?
    };
    let end_index = start_index
        .saturating_add(HISTORY_INDEX_STRIDE)
        .min(sparse_index.history_record_count);
    for index in start_index..end_index {
        let commit_seq = read_commit_seq(index)?;
        outcome.stats.record_probes += 1;
        if commit_seq > target_commit_seq {
            break;
        }
        outcome.record_index = Some(index);
    }
    Ok(outcome)
}

/// Durable history-sidecar failure.
#[derive(Debug, Error)]
pub enum HistoryError {
    /// VFS operation failed.
    #[error(transparent)]
    Storage(#[from] FrankenError),
    /// Caller supplied a value forbidden by the v1 contract.
    #[error("invalid history input: {0}")]
    InvalidInput(String),
    /// Stable bytes failed structural, checksum, or chain validation.
    #[error("history corruption at slot {slot:?}: {reason}")]
    Corrupt {
        /// Zero-based record slot, or `None` for the file header/index.
        slot: Option<u64>,
        /// Exact validation failure.
        reason: String,
    },
    /// The optional sparse-index cache is malformed or stale.
    #[error("history sparse-index corruption: {0}")]
    CorruptIndex(String),
    /// The sidecar belongs to another logical database history.
    #[error("history identity mismatch: expected {expected:?}, found {actual:?}")]
    IdentityMismatch {
        /// Expected persistent identity.
        expected: DatabaseHistoryId,
        /// Header identity.
        actual: DatabaseHistoryId,
    },
    /// The history coordinate format generation is stale or too new.
    #[error("history format generation mismatch: expected {expected}, found {actual}")]
    FormatGenerationMismatch {
        /// Expected generation.
        expected: u64,
        /// Header generation.
        actual: u64,
    },
    /// The sidecar belongs to another recovered database/WAL lineage.
    #[error("history database generation mismatch: expected {expected}, found {actual}")]
    DatabaseGenerationMismatch {
        /// Expected recovered generation.
        expected: u64,
        /// Header generation.
        actual: u64,
    },
    /// File format version is not understood by this reader.
    #[error("unsupported history format version {0}")]
    UnsupportedFormatVersion(u16),
    /// Record prefix version is not understood by this reader.
    #[error("unsupported history record version {0}")]
    UnsupportedRecordVersion(u16),
    /// Requested commit does not have a retained lookup record.
    #[error("history not retained at or before commit {0}")]
    HistoryNotRetained(u64),
    /// The optional sparse-index cache does not exist yet.
    #[error("history sparse index is unavailable")]
    SparseIndexUnavailable,
}

/// Append-only history log attached to one database pathname.
pub struct HistoryLog<'a, V: Vfs> {
    cx: &'a Cx,
    vfs: &'a V,
    history_path: PathBuf,
    index_path: PathBuf,
    expectations: HistoryExpectations,
}

impl<'a, V: Vfs> HistoryLog<'a, V> {
    /// Bind a history log to a database pathname and recovered generation.
    #[must_use]
    pub fn new(
        cx: &'a Cx,
        vfs: &'a V,
        database_path: &Path,
        expectations: HistoryExpectations,
    ) -> Self {
        Self {
            cx,
            vfs,
            history_path: suffix_path(database_path, ".fsqlite-history"),
            index_path: suffix_path(database_path, ".fsqlite-history-idx"),
            expectations,
        }
    }

    /// Canonical history pathname.
    #[must_use]
    pub fn history_path(&self) -> &Path {
        &self.history_path
    }

    /// Canonical sparse-index pathname.
    #[must_use]
    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    /// Create and durably publish an empty sidecar, or validate an existing one.
    pub fn initialize(&self) -> Result<(), HistoryError> {
        if self
            .vfs
            .access(self.cx, &self.history_path, AccessFlags::EXISTS)?
        {
            let mut file = self.open_history(false)?;
            let result = self.validate_header(&file).map(|_| ());
            let close_result = file.close(self.cx).map_err(HistoryError::from);
            return result.and(close_result);
        }

        let flags = VfsOpenFlags::MAIN_JOURNAL
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::EXCLUSIVE
            | VfsOpenFlags::READWRITE;
        let (mut file, _) = self.vfs.open(self.cx, Some(&self.history_path), flags)?;
        let header = self.expectations.header()?.encode_slot();
        let result: Result<(), FrankenError> = (|| {
            file.write(self.cx, &header, 0)?;
            file.durable_sync(self.cx, SyncKind::FullDurable)?;
            self.vfs
                .sync_parent_directory(self.cx, &self.history_path)?;
            Ok(())
        })();
        let close_result = file.close(self.cx).map_err(HistoryError::from);
        result.map_err(HistoryError::from).and(close_result)
    }

    /// Validate identity, chain, slot boundaries, and recovered horizon;
    /// truncate only a provable invalid/torn tail and durably publish repair.
    pub fn recover(&self) -> Result<RecoveryReport, HistoryError> {
        let mut file = self.open_history(false)?;
        file.lock(self.cx, LockLevel::Exclusive)?;
        let result = self.recover_locked(&mut file);
        let unlock_result = file
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = file.close(self.cx).map_err(HistoryError::from);
        preserve_value_after_cleanup(result, unlock_result, close_result)
    }

    /// Append one commit record and issue a full-durable barrier.
    pub fn append(&self, draft: HistoryRecordDraft) -> Result<HistoryRecord, HistoryError> {
        let mut appended = self.append_batch(std::slice::from_ref(&draft))?;
        Ok(appended.remove(0))
    }

    /// Append a batch under one short-lived sidecar lock and one full-durable
    /// barrier. The batch is encoded as individually immutable record slots.
    pub fn append_batch(
        &self,
        drafts: &[HistoryRecordDraft],
    ) -> Result<Vec<HistoryRecord>, HistoryError> {
        if drafts.is_empty() {
            return Ok(Vec::new());
        }
        let mut file = self.open_history(false)?;
        file.lock(self.cx, LockLevel::Exclusive)?;
        let result = (|| {
            let recovery = self.recover_locked(&mut file)?;
            let mut previous = if recovery.valid_records == 0 {
                None
            } else {
                Some(self.read_record(&file, recovery.valid_records - 1)?)
            };
            let mut appended = Vec::with_capacity(drafts.len());
            for draft in drafts {
                if draft.commit_seq > self.expectations.recovered_commit_horizon {
                    return Err(HistoryError::InvalidInput(format!(
                        "commit {} exceeds recovered durable horizon {}",
                        draft.commit_seq, self.expectations.recovered_commit_horizon
                    )));
                }
                if let Some(prior) = previous {
                    if draft.commit_seq <= prior.commit_seq {
                        return Err(HistoryError::InvalidInput(format!(
                            "commit sequence {} does not advance tail {}",
                            draft.commit_seq, prior.commit_seq
                        )));
                    }
                } else if draft.flags & HISTORY_FLAG_CHECKPOINT_ANCHOR == 0 {
                    return Err(HistoryError::InvalidInput(
                        "first retained record must be a checkpoint anchor".to_owned(),
                    ));
                }
                let record = HistoryRecord::from_draft(
                    *draft,
                    previous.map_or(0, |record| record.this_record_blake3_64),
                )?;
                let index = recovery
                    .valid_records
                    .checked_add(u64::try_from(appended.len()).map_err(|_| {
                        HistoryError::InvalidInput("record count exceeds u64".to_owned())
                    })?)
                    .ok_or_else(|| {
                        HistoryError::InvalidInput("record count overflow".to_owned())
                    })?;
                file.write(self.cx, &record.encode_slot(), record_offset(index)?)?;
                appended.push(record);
                previous = Some(record);
            }
            file.durable_sync(self.cx, SyncKind::FullDurable)?;
            Ok(appended)
        })();
        let unlock_result = file
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = file.close(self.cx).map_err(HistoryError::from);
        preserve_value_after_cleanup(result, unlock_result, close_result)
    }

    /// Read every validated retained record. This never mutates a malformed
    /// tail; callers run [`Self::recover`] first when repair is authorized.
    pub fn read_all(&self) -> Result<Vec<HistoryRecord>, HistoryError> {
        let mut file = self.open_history(true)?;
        file.lock(self.cx, LockLevel::Shared)?;
        let result = (|| {
            self.validate_header(&file)?;
            let (record_count, partial_bytes) = record_count_and_partial(&file, self.cx)?;
            if partial_bytes != 0 {
                return Err(HistoryError::Corrupt {
                    slot: Some(record_count),
                    reason: format!("incomplete final slot ({partial_bytes} bytes)"),
                });
            }
            self.read_and_validate_prefix(&file, record_count)
        })();
        let unlock_result = file
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = file.close(self.cx).map_err(HistoryError::from);
        preserve_value_after_cleanup(result, unlock_result, close_result)
    }

    /// Rebuild and durably publish the optional sparse-index cache.
    pub fn rebuild_sparse_index(&self) -> Result<SparseIndex, HistoryError> {
        let mut stats = SearchStats::default();
        self.rebuild_sparse_index_counted(&mut stats)
    }

    fn rebuild_sparse_index_counted(
        &self,
        stats: &mut SearchStats,
    ) -> Result<SparseIndex, HistoryError> {
        let mut history = self.open_history(true)?;
        history.lock(self.cx, LockLevel::Shared)?;
        let result = (|| {
            self.validate_header_counted(&history, stats)?;
            let (record_count, partial) = record_count_and_partial(&history, self.cx)?;
            if partial != 0 {
                return Err(HistoryError::Corrupt {
                    slot: Some(record_count),
                    reason: "cannot index an incomplete history tail".to_owned(),
                });
            }
            let capacity =
                usize::try_from(record_count.div_ceil(HISTORY_INDEX_STRIDE)).map_err(|_| {
                    HistoryError::InvalidInput("sparse index exceeds address space".to_owned())
                })?;
            let mut entries = Vec::with_capacity(capacity);
            let mut previous = None;
            for position in 0..record_count {
                let record = self.read_record_counted(&history, position, stats)?;
                self.validate_record_position(position, record, previous)?;
                if position % HISTORY_INDEX_STRIDE == 0 {
                    entries.push(SparseIndexEntry {
                        commit_seq: record.commit_seq,
                        byte_offset: record_offset(position)?,
                    });
                }
                previous = Some(record);
            }
            let index = SparseIndex {
                entries,
                history_record_count: record_count,
                last_record_hash: previous.map_or(0, |record| record.this_record_blake3_64),
            };
            self.write_sparse_index(&index)?;
            Ok(index)
        })();
        let unlock_result = history
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = history.close(self.cx).map_err(HistoryError::from);
        preserve_value_after_cleanup(result, unlock_result, close_result)
    }

    /// Locate the greatest retained commit no later than `target_commit_seq`,
    /// rebuilding a missing, stale, or corrupt optional index before lookup.
    pub fn lookup_floor(
        &self,
        target_commit_seq: u64,
    ) -> Result<(HistoryRecord, SearchStats), HistoryError> {
        // Index preparation and the final lookup deliberately use short-lived
        // locks. An append can therefore race between them. Rebind the pair
        // rather than ever combining an index for one tail with another.
        let mut stats = SearchStats::default();
        for _ in 0..3 {
            let tail = self.read_tail_snapshot_counted(&mut stats)?;
            let (mut index_file, binding) = match self.open_sparse_index_for_tail(tail, &mut stats)
            {
                Ok(opened) => opened,
                Err(HistoryError::CorruptIndex(_)) => continue,
                Err(error) => return Err(error),
            };
            let mut history = match self.open_history(true) {
                Ok(history) => history,
                Err(error) => {
                    let _ = unlock_and_close(&mut index_file, self.cx);
                    return Err(error);
                }
            };
            if let Err(error) = history.lock(self.cx, LockLevel::Shared) {
                let _ = history.close(self.cx);
                let _ = unlock_and_close(&mut index_file, self.cx);
                return Err(HistoryError::from(error));
            }
            let result = (|| {
                self.validate_header_counted(&history, &mut stats)?;
                if self.read_tail_locked_counted(&history, &mut stats)? != tail
                    || binding.tail != tail
                {
                    return Ok(None);
                }
                self.lookup_with_index_files(
                    &history,
                    &index_file,
                    binding,
                    target_commit_seq,
                    &mut stats,
                )
                .map(Some)
            })();
            let unlock_result = history
                .unlock(self.cx, LockLevel::None)
                .map_err(HistoryError::from);
            let close_result = history.close(self.cx).map_err(HistoryError::from);
            let result = preserve_value_after_cleanup(result, unlock_result, close_result);
            let index_unlock_result = index_file
                .unlock(self.cx, LockLevel::None)
                .map_err(HistoryError::from);
            let index_close_result = index_file.close(self.cx).map_err(HistoryError::from);
            let found =
                match preserve_value_after_cleanup(result, index_unlock_result, index_close_result)
                {
                    Ok(found) => found,
                    Err(HistoryError::CorruptIndex(_)) => {
                        self.rebuild_sparse_index_counted(&mut stats)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
            if let Some(record) = found {
                return Ok((record, stats));
            }
        }
        Err(HistoryError::Corrupt {
            slot: None,
            reason: "history tail changed repeatedly during sparse-index lookup".to_owned(),
        })
    }

    fn open_history(&self, read_only: bool) -> Result<V::File, HistoryError> {
        let mode = if read_only {
            VfsOpenFlags::READONLY
        } else {
            VfsOpenFlags::READWRITE
        };
        let flags = VfsOpenFlags::MAIN_JOURNAL | mode;
        self.vfs
            .open(self.cx, Some(&self.history_path), flags)
            .map(|(file, _)| file)
            .map_err(HistoryError::from)
    }

    fn validate_header(&self, file: &V::File) -> Result<HistoryHeader, HistoryError> {
        let mut slot = [0_u8; HISTORY_SLOT_SIZE];
        read_exact_at(file, self.cx, &mut slot, 0)?;
        let header = HistoryHeader::decode_slot(&slot)?;
        if header.database_history_id != self.expectations.database_history_id {
            return Err(HistoryError::IdentityMismatch {
                expected: self.expectations.database_history_id,
                actual: header.database_history_id,
            });
        }
        if header.format_generation != self.expectations.format_generation {
            return Err(HistoryError::FormatGenerationMismatch {
                expected: self.expectations.format_generation,
                actual: header.format_generation,
            });
        }
        if header.database_generation != self.expectations.database_generation {
            return Err(HistoryError::DatabaseGenerationMismatch {
                expected: self.expectations.database_generation,
                actual: header.database_generation,
            });
        }
        Ok(header)
    }

    fn validate_header_counted(
        &self,
        file: &V::File,
        stats: &mut SearchStats,
    ) -> Result<HistoryHeader, HistoryError> {
        let mut slot = [0_u8; HISTORY_SLOT_SIZE];
        read_exact_at_counted(
            file,
            self.cx,
            &mut slot,
            0,
            &mut stats.history_read_calls,
            &mut stats.history_bytes_read,
        )?;
        let header = HistoryHeader::decode_slot(&slot)?;
        if header.database_history_id != self.expectations.database_history_id {
            return Err(HistoryError::IdentityMismatch {
                expected: self.expectations.database_history_id,
                actual: header.database_history_id,
            });
        }
        if header.format_generation != self.expectations.format_generation {
            return Err(HistoryError::FormatGenerationMismatch {
                expected: self.expectations.format_generation,
                actual: header.format_generation,
            });
        }
        if header.database_generation != self.expectations.database_generation {
            return Err(HistoryError::DatabaseGenerationMismatch {
                expected: self.expectations.database_generation,
                actual: header.database_generation,
            });
        }
        Ok(header)
    }

    fn read_tail_snapshot_counted(
        &self,
        stats: &mut SearchStats,
    ) -> Result<HistoryTail, HistoryError> {
        let mut history = self.open_history(true)?;
        history.lock(self.cx, LockLevel::Shared)?;
        let result = (|| {
            self.validate_header_counted(&history, stats)?;
            self.read_tail_locked_counted(&history, stats)
        })();
        let unlock_result = history
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = history.close(self.cx).map_err(HistoryError::from);
        preserve_value_after_cleanup(result, unlock_result, close_result)
    }

    fn read_tail_locked_counted(
        &self,
        history: &V::File,
        stats: &mut SearchStats,
    ) -> Result<HistoryTail, HistoryError> {
        let (record_count, partial_bytes) = record_count_and_partial(history, self.cx)?;
        if partial_bytes != 0 {
            return Err(HistoryError::Corrupt {
                slot: Some(record_count),
                reason: format!("incomplete final slot ({partial_bytes} bytes)"),
            });
        }
        let last_record_hash = if record_count == 0 {
            0
        } else {
            let tail = self.read_record_counted(history, record_count - 1, stats)?;
            if tail.commit_seq > self.expectations.recovered_commit_horizon {
                return Err(HistoryError::Corrupt {
                    slot: Some(record_count - 1),
                    reason: format!(
                        "commit {} exceeds recovered durable horizon {}",
                        tail.commit_seq, self.expectations.recovered_commit_horizon
                    ),
                });
            }
            if record_count == 1
                && (tail.prev_record_blake3_64 != 0
                    || tail.flags & HISTORY_FLAG_CHECKPOINT_ANCHOR == 0)
            {
                return Err(HistoryError::Corrupt {
                    slot: Some(0),
                    reason: "first record is not a zero-linked checkpoint anchor".to_owned(),
                });
            }
            tail.this_record_blake3_64
        };
        Ok(HistoryTail {
            record_count,
            last_record_hash,
        })
    }

    fn open_sparse_index_for_tail(
        &self,
        tail: HistoryTail,
        stats: &mut SearchStats,
    ) -> Result<(V::File, SparseIndexBinding), HistoryError> {
        match self.try_open_sparse_index_for_tail(tail, stats) {
            Ok(opened) => Ok(opened),
            Err(HistoryError::HistoryNotRetained(_) | HistoryError::CorruptIndex(_)) => {
                self.rebuild_sparse_index_counted(stats)?;
                self.try_open_sparse_index_for_tail(tail, stats)
            }
            Err(error) => Err(error),
        }
    }

    fn try_open_sparse_index_for_tail(
        &self,
        tail: HistoryTail,
        stats: &mut SearchStats,
    ) -> Result<(V::File, SparseIndexBinding), HistoryError> {
        if !self
            .vfs
            .access(self.cx, &self.index_path, AccessFlags::EXISTS)?
        {
            return Err(HistoryError::HistoryNotRetained(tail.record_count));
        }
        let flags = VfsOpenFlags::MAIN_JOURNAL | VfsOpenFlags::READONLY;
        let (mut file, _) = self.vfs.open(self.cx, Some(&self.index_path), flags)?;
        if let Err(error) = file.lock(self.cx, LockLevel::Shared) {
            let _ = file.close(self.cx);
            return Err(HistoryError::from(error));
        }
        match self.read_sparse_index_binding(&file, tail, stats) {
            Ok(binding) => Ok((file, binding)),
            Err(error) => {
                let unlock_result = file
                    .unlock(self.cx, LockLevel::None)
                    .map_err(HistoryError::from);
                let close_result = file.close(self.cx).map_err(HistoryError::from);
                let cleanup_result = unlock_result.and(close_result);
                match cleanup_result {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(cleanup_error),
                }
            }
        }
    }

    fn read_sparse_index_binding(
        &self,
        file: &V::File,
        tail: HistoryTail,
        stats: &mut SearchStats,
    ) -> Result<SparseIndexBinding, HistoryError> {
        let mut header = [0_u8; HISTORY_SLOT_SIZE];
        read_exact_at_counted(
            file,
            self.cx,
            &mut header,
            0,
            &mut stats.index_read_calls,
            &mut stats.index_bytes_read,
        )?;
        decode_index_binding(&header, file.file_size(self.cx)?, self.expectations, tail)
    }

    fn read_index_entry(
        &self,
        file: &V::File,
        binding: SparseIndexBinding,
        ordinal: u64,
        stats: &mut SearchStats,
    ) -> Result<SparseIndexEntry, HistoryError> {
        if ordinal >= binding.entry_count {
            return Err(HistoryError::CorruptIndex(
                "sparse index probe exceeds entry count".to_owned(),
            ));
        }
        let mut bytes = [0_u8; INDEX_ENTRY_SIZE];
        let offset = index_entry_offset(ordinal)?;
        read_exact_at_counted(
            file,
            self.cx,
            &mut bytes,
            offset,
            &mut stats.index_read_calls,
            &mut stats.index_bytes_read,
        )?;
        decode_index_entry(&bytes, self.expectations, binding.tail, ordinal)
    }

    fn read_validated_index_probe(
        &self,
        history: &V::File,
        index_file: &V::File,
        binding: SparseIndexBinding,
        ordinal: u64,
        stats: &mut SearchStats,
    ) -> Result<SparseIndexEntry, HistoryError> {
        let entry = self.read_index_entry(index_file, binding, ordinal, stats)?;
        let record_index = ordinal
            .checked_mul(HISTORY_INDEX_STRIDE)
            .ok_or_else(|| HistoryError::CorruptIndex("sample position overflow".to_owned()))?;
        let record = self.read_record_counted(history, record_index, stats)?;
        if record.commit_seq != entry.commit_seq {
            return Err(HistoryError::CorruptIndex(format!(
                "sample {ordinal} does not match authoritative history"
            )));
        }
        if record.commit_seq > self.expectations.recovered_commit_horizon {
            return Err(HistoryError::Corrupt {
                slot: Some(record_index),
                reason: format!(
                    "commit {} exceeds recovered durable horizon {}",
                    record.commit_seq, self.expectations.recovered_commit_horizon
                ),
            });
        }
        Ok(entry)
    }

    fn lookup_with_index_files(
        &self,
        history: &V::File,
        index_file: &V::File,
        binding: SparseIndexBinding,
        target_commit_seq: u64,
        stats: &mut SearchStats,
    ) -> Result<HistoryRecord, HistoryError> {
        if binding.entry_count == 0 {
            return Err(HistoryError::HistoryNotRetained(target_commit_seq));
        }
        let mut low = 0_u64;
        let mut high = binding.entry_count;
        while low < high {
            let mid = low + (high - low) / 2;
            let entry =
                self.read_validated_index_probe(history, index_file, binding, mid, stats)?;
            stats.index_probes += 1;
            if entry.commit_seq <= target_commit_seq {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        let start_index = if low == 0 {
            0
        } else {
            (low - 1)
                .checked_mul(HISTORY_INDEX_STRIDE)
                .ok_or_else(|| HistoryError::CorruptIndex("lookup position overflow".to_owned()))?
        };
        let end_index = start_index
            .saturating_add(HISTORY_INDEX_STRIDE)
            .min(binding.tail.record_count);
        let mut previous = if start_index == 0 {
            None
        } else {
            Some(self.read_record_counted(history, start_index - 1, stats)?)
        };
        let mut candidate = None;
        for position in start_index..end_index {
            let record = self.read_record_counted(history, position, stats)?;
            self.validate_record_position(position, record, previous)?;
            stats.record_probes += 1;
            previous = Some(record);
            if record.commit_seq > target_commit_seq {
                break;
            }
            candidate = Some(record);
        }
        candidate.ok_or(HistoryError::HistoryNotRetained(target_commit_seq))
    }

    fn validate_record_position(
        &self,
        position: u64,
        record: HistoryRecord,
        previous: Option<HistoryRecord>,
    ) -> Result<(), HistoryError> {
        if let Some(previous) = previous {
            if record.prev_record_blake3_64 != previous.this_record_blake3_64 {
                return Err(HistoryError::Corrupt {
                    slot: Some(position),
                    reason: "record hash chain does not link to predecessor".to_owned(),
                });
            }
            if record.commit_seq <= previous.commit_seq {
                return Err(HistoryError::Corrupt {
                    slot: Some(position),
                    reason: "commit sequence is not strictly increasing".to_owned(),
                });
            }
        } else if position != 0
            || record.prev_record_blake3_64 != 0
            || record.flags & HISTORY_FLAG_CHECKPOINT_ANCHOR == 0
        {
            return Err(HistoryError::Corrupt {
                slot: Some(position),
                reason: "first record is not a zero-linked checkpoint anchor".to_owned(),
            });
        }
        if record.commit_seq > self.expectations.recovered_commit_horizon {
            return Err(HistoryError::Corrupt {
                slot: Some(position),
                reason: format!(
                    "commit {} exceeds recovered durable horizon {}",
                    record.commit_seq, self.expectations.recovered_commit_horizon
                ),
            });
        }
        Ok(())
    }

    fn recover_locked(&self, file: &mut V::File) -> Result<RecoveryReport, HistoryError> {
        self.validate_header(file)?;
        let original_len = file.file_size(self.cx)?;
        let (record_count, partial_bytes) = record_count_and_partial(file, self.cx)?;
        let mut records: Vec<HistoryRecord> = Vec::new();
        let mut valid_records = record_count;
        for index in 0..record_count {
            match self.read_record(file, index) {
                Ok(record) => {
                    if let Some(previous) = records.last() {
                        if record.prev_record_blake3_64 != previous.this_record_blake3_64 {
                            if index + 1 == record_count {
                                valid_records = index;
                                break;
                            }
                            return Err(HistoryError::Corrupt {
                                slot: Some(index),
                                reason: "record hash chain does not link to predecessor".to_owned(),
                            });
                        }
                        if record.commit_seq <= previous.commit_seq {
                            if index + 1 == record_count {
                                valid_records = index;
                                break;
                            }
                            return Err(HistoryError::Corrupt {
                                slot: Some(index),
                                reason: "commit sequence is not strictly increasing".to_owned(),
                            });
                        }
                    } else if record.prev_record_blake3_64 != 0
                        || record.flags & HISTORY_FLAG_CHECKPOINT_ANCHOR == 0
                    {
                        return Err(HistoryError::Corrupt {
                            slot: Some(0),
                            reason: "first record is not a zero-linked checkpoint anchor"
                                .to_owned(),
                        });
                    }
                    if record.commit_seq > self.expectations.recovered_commit_horizon {
                        valid_records = index;
                        break;
                    }
                    records.push(record);
                }
                Err(error)
                    if index + 1 == record_count
                        && matches!(error, HistoryError::Corrupt { .. }) =>
                {
                    valid_records = index;
                    break;
                }
                Err(error) => return Err(error_at_slot(error, index)),
            }
        }

        let final_len = record_offset(valid_records)?;
        let needs_truncate = final_len != original_len;
        if needs_truncate {
            file.truncate(self.cx, final_len)?;
            file.durable_sync(self.cx, SyncKind::FullDurable)?;
        }
        Ok(RecoveryReport {
            valid_records,
            truncated_records: record_count - valid_records,
            truncated_partial_bytes: partial_bytes,
            final_file_len: final_len,
        })
    }

    fn read_record(&self, file: &V::File, index: u64) -> Result<HistoryRecord, HistoryError> {
        let mut bytes = [0_u8; HISTORY_RECORD_V1_SIZE];
        read_exact_at(file, self.cx, &mut bytes, record_offset(index)?)?;
        HistoryRecord::decode(&bytes).map_err(|error| error_at_slot(error, index))
    }

    fn read_record_counted(
        &self,
        file: &V::File,
        index: u64,
        stats: &mut SearchStats,
    ) -> Result<HistoryRecord, HistoryError> {
        let mut bytes = [0_u8; HISTORY_RECORD_V1_SIZE];
        read_exact_at_counted(
            file,
            self.cx,
            &mut bytes,
            record_offset(index)?,
            &mut stats.history_read_calls,
            &mut stats.history_bytes_read,
        )?;
        HistoryRecord::decode(&bytes).map_err(|error| error_at_slot(error, index))
    }

    fn read_and_validate_prefix(
        &self,
        file: &V::File,
        record_count: u64,
    ) -> Result<Vec<HistoryRecord>, HistoryError> {
        let capacity = usize::try_from(record_count).map_err(|_| {
            HistoryError::InvalidInput("record count exceeds address space".to_owned())
        })?;
        let mut records: Vec<HistoryRecord> = Vec::with_capacity(capacity);
        for index in 0..record_count {
            let record = self.read_record(file, index)?;
            self.validate_record_position(index, record, records.last().copied())?;
            records.push(record);
        }
        Ok(records)
    }

    fn write_sparse_index(&self, index: &SparseIndex) -> Result<(), HistoryError> {
        let bytes = encode_index(self.expectations, index)?;
        let exists = self
            .vfs
            .access(self.cx, &self.index_path, AccessFlags::EXISTS)?;
        let flags = VfsOpenFlags::MAIN_JOURNAL
            | VfsOpenFlags::READWRITE
            | if exists {
                VfsOpenFlags::empty()
            } else {
                VfsOpenFlags::CREATE | VfsOpenFlags::EXCLUSIVE
            };
        let (mut file, _) = self.vfs.open(self.cx, Some(&self.index_path), flags)?;
        file.lock(self.cx, LockLevel::Exclusive)?;
        let result: Result<(), FrankenError> = (|| {
            let unpublished = {
                let mut bytes = bytes.clone();
                bytes[INDEX_HEADER_CHECKSUM_RANGE].fill(0);
                bytes
            };
            file.truncate(self.cx, 0)?;
            file.write(self.cx, &unpublished, 0)?;
            file.durable_sync(self.cx, SyncKind::FullDurable)?;
            file.write(self.cx, &bytes[..HISTORY_SLOT_SIZE], 0)?;
            file.durable_sync(self.cx, SyncKind::FullDurable)?;
            if !exists {
                self.vfs.sync_parent_directory(self.cx, &self.index_path)?;
            }
            Ok(())
        })();
        let unlock_result = file
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = file.close(self.cx).map_err(HistoryError::from);
        result
            .map_err(HistoryError::from)
            .and(unlock_result)
            .and(close_result)
    }

    #[cfg(test)]
    fn read_sparse_index(
        &self,
        expected_record_count: u64,
        expected_last_hash: u64,
    ) -> Result<SparseIndex, HistoryError> {
        if !self
            .vfs
            .access(self.cx, &self.index_path, AccessFlags::EXISTS)?
        {
            return Err(HistoryError::HistoryNotRetained(expected_record_count));
        }
        let flags = VfsOpenFlags::MAIN_JOURNAL | VfsOpenFlags::READONLY;
        let (mut file, _) = self.vfs.open(self.cx, Some(&self.index_path), flags)?;
        file.lock(self.cx, LockLevel::Shared)?;
        let result = (|| {
            let file_len = file.file_size(self.cx)?;
            let byte_len = usize::try_from(file_len).map_err(|_| HistoryError::Corrupt {
                slot: None,
                reason: "sparse index exceeds address space".to_owned(),
            })?;
            let mut bytes = vec![0_u8; byte_len];
            read_exact_at(&file, self.cx, &mut bytes, 0)?;
            decode_index(
                &bytes,
                self.expectations,
                expected_record_count,
                expected_last_hash,
            )
        })();
        let unlock_result = file
            .unlock(self.cx, LockLevel::None)
            .map_err(HistoryError::from);
        let close_result = file.close(self.cx).map_err(HistoryError::from);
        preserve_value_after_cleanup(result, unlock_result, close_result)
    }
}

fn preserve_value_after_cleanup<T>(
    result: Result<T, HistoryError>,
    unlock_result: Result<(), HistoryError>,
    close_result: Result<(), HistoryError>,
) -> Result<T, HistoryError> {
    let cleanup_result = unlock_result.and(close_result);
    match (result, cleanup_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn unlock_and_close<F: VfsFile>(file: &mut F, cx: &Cx) -> Result<(), HistoryError> {
    let unlock_result = file.unlock(cx, LockLevel::None).map_err(HistoryError::from);
    let close_result = file.close(cx).map_err(HistoryError::from);
    unlock_result.and(close_result)
}

fn encode_index(
    expectations: HistoryExpectations,
    index: &SparseIndex,
) -> Result<Vec<u8>, HistoryError> {
    let entries_len = index
        .entries
        .len()
        .checked_mul(INDEX_ENTRY_SIZE)
        .ok_or_else(|| HistoryError::InvalidInput("sparse index byte size overflow".to_owned()))?;
    let total_len = HISTORY_SLOT_SIZE
        .checked_add(entries_len)
        .ok_or_else(|| HistoryError::InvalidInput("sparse index byte size overflow".to_owned()))?;
    let mut bytes = vec![0_u8; total_len];
    bytes[0..8].copy_from_slice(&INDEX_MAGIC);
    put_u16(&mut bytes, 8, HISTORY_FORMAT_VERSION);
    put_u16(
        &mut bytes,
        10,
        u16::try_from(INDEX_HEADER_V1_SIZE).expect("index header size fits u16"),
    );
    put_u32(
        &mut bytes,
        12,
        u32::try_from(INDEX_ENTRY_SIZE).expect("index entry size fits u32"),
    );
    put_u32(
        &mut bytes,
        16,
        u32::try_from(HISTORY_INDEX_STRIDE).expect("index stride fits u32"),
    );
    bytes[24..40].copy_from_slice(&expectations.database_history_id.0);
    put_u64(&mut bytes, 40, expectations.format_generation);
    put_u64(&mut bytes, 48, expectations.database_generation);
    put_u64(&mut bytes, 56, index.history_record_count);
    put_u64(&mut bytes, 64, index.last_record_hash);
    for (position, entry) in index.entries.iter().enumerate() {
        let offset = HISTORY_SLOT_SIZE + position * INDEX_ENTRY_SIZE;
        put_u64(&mut bytes, offset, entry.commit_seq);
        put_u64(&mut bytes, offset + 8, entry.byte_offset);
        let ordinal = u64::try_from(position).map_err(|_| {
            HistoryError::InvalidInput("sparse index position exceeds u64".to_owned())
        })?;
        let checksum = index_entry_checksum(
            expectations,
            HistoryTail {
                record_count: index.history_record_count,
                last_record_hash: index.last_record_hash,
            },
            ordinal,
            *entry,
        );
        put_u64(&mut bytes, offset + 16, checksum);
    }
    let checksum =
        checksum_with_zeroed_range(&bytes[..INDEX_HEADER_V1_SIZE], INDEX_HEADER_CHECKSUM_RANGE);
    put_u64(&mut bytes, 72, checksum);
    Ok(bytes)
}

#[cfg(test)]
fn decode_index(
    bytes: &[u8],
    expectations: HistoryExpectations,
    expected_record_count: u64,
    expected_last_hash: u64,
) -> Result<SparseIndex, HistoryError> {
    if bytes.len() < HISTORY_SLOT_SIZE {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: "sparse index header is incomplete".to_owned(),
        });
    }
    if bytes[0..8] != INDEX_MAGIC
        || get_u16(bytes, 8) != HISTORY_FORMAT_VERSION
        || get_u16(bytes, 10)
            != u16::try_from(INDEX_HEADER_V1_SIZE).expect("index header size fits u16")
        || get_u32(bytes, 12) != u32::try_from(INDEX_ENTRY_SIZE).expect("index entry size fits u32")
        || get_u32(bytes, 16) != u32::try_from(HISTORY_INDEX_STRIDE).expect("index stride fits u32")
    {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: "sparse index format mismatch".to_owned(),
        });
    }
    if bytes[20..24].iter().any(|byte| *byte != 0)
        || bytes[80..HISTORY_SLOT_SIZE].iter().any(|byte| *byte != 0)
    {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: "sparse index reserved bytes are non-zero".to_owned(),
        });
    }
    let actual_checksum =
        checksum_with_zeroed_range(&bytes[..INDEX_HEADER_V1_SIZE], INDEX_HEADER_CHECKSUM_RANGE);
    if get_u64(bytes, 72) == 0 || get_u64(bytes, 72) != actual_checksum {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: "sparse index is unpublished or has a checksum mismatch".to_owned(),
        });
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[24..40]);
    if DatabaseHistoryId(id) != expectations.database_history_id
        || get_u64(bytes, 40) != expectations.format_generation
        || get_u64(bytes, 48) != expectations.database_generation
        || get_u64(bytes, 56) != expected_record_count
        || get_u64(bytes, 64) != expected_last_hash
    {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: "sparse index is stale for this history tail".to_owned(),
        });
    }
    let expected_entries = expected_record_count.div_ceil(HISTORY_INDEX_STRIDE);
    let expected_entries =
        usize::try_from(expected_entries).map_err(|_| HistoryError::Corrupt {
            slot: None,
            reason: "sparse index entry count exceeds address space".to_owned(),
        })?;
    let expected_len = HISTORY_SLOT_SIZE
        .checked_add(
            expected_entries
                .checked_mul(INDEX_ENTRY_SIZE)
                .ok_or_else(|| HistoryError::Corrupt {
                    slot: None,
                    reason: "sparse index byte size overflow".to_owned(),
                })?,
        )
        .ok_or_else(|| HistoryError::Corrupt {
            slot: None,
            reason: "sparse index byte size overflow".to_owned(),
        })?;
    if bytes.len() != expected_len {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: "sparse index length mismatch".to_owned(),
        });
    }
    let mut entries = Vec::with_capacity(expected_entries);
    for position in 0..expected_entries {
        let offset = HISTORY_SLOT_SIZE + position * INDEX_ENTRY_SIZE;
        let entry = SparseIndexEntry {
            commit_seq: get_u64(bytes, offset),
            byte_offset: get_u64(bytes, offset + 8),
        };
        let position_u64 = u64::try_from(position).map_err(|_| HistoryError::Corrupt {
            slot: None,
            reason: "sparse index position exceeds u64".to_owned(),
        })?;
        let record_index = position_u64
            .checked_mul(HISTORY_INDEX_STRIDE)
            .ok_or_else(|| HistoryError::Corrupt {
                slot: None,
                reason: "sparse index record position overflow".to_owned(),
            })?;
        if entry.byte_offset != record_offset(record_index)?
            || entries
                .last()
                .is_some_and(|prior: &SparseIndexEntry| prior.commit_seq >= entry.commit_seq)
        {
            return Err(HistoryError::Corrupt {
                slot: None,
                reason: "sparse index entries are unaligned or non-monotone".to_owned(),
            });
        }
        let tail = HistoryTail {
            record_count: expected_record_count,
            last_record_hash: expected_last_hash,
        };
        if get_u64(bytes, offset + 16)
            != index_entry_checksum(expectations, tail, position_u64, entry)
        {
            return Err(HistoryError::CorruptIndex(format!(
                "sparse index entry {position_u64} checksum mismatch"
            )));
        }
        entries.push(entry);
    }
    Ok(SparseIndex {
        entries,
        history_record_count: expected_record_count,
        last_record_hash: expected_last_hash,
    })
}

fn decode_index_binding(
    header: &[u8],
    file_len: u64,
    expectations: HistoryExpectations,
    tail: HistoryTail,
) -> Result<SparseIndexBinding, HistoryError> {
    if header.len() < HISTORY_SLOT_SIZE {
        return Err(HistoryError::CorruptIndex(
            "sparse index header is incomplete".to_owned(),
        ));
    }
    if header[0..8] != INDEX_MAGIC
        || get_u16(header, 8) != HISTORY_FORMAT_VERSION
        || get_u16(header, 10)
            != u16::try_from(INDEX_HEADER_V1_SIZE).expect("index header size fits u16")
        || get_u32(header, 12)
            != u32::try_from(INDEX_ENTRY_SIZE).expect("index entry size fits u32")
        || get_u32(header, 16)
            != u32::try_from(HISTORY_INDEX_STRIDE).expect("index stride fits u32")
    {
        return Err(HistoryError::CorruptIndex(
            "sparse index format mismatch".to_owned(),
        ));
    }
    if header[20..24].iter().any(|byte| *byte != 0)
        || header[INDEX_HEADER_V1_SIZE..HISTORY_SLOT_SIZE]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(HistoryError::CorruptIndex(
            "sparse index reserved bytes are non-zero".to_owned(),
        ));
    }
    let actual_checksum =
        checksum_with_zeroed_range(&header[..INDEX_HEADER_V1_SIZE], INDEX_HEADER_CHECKSUM_RANGE);
    if get_u64(header, 72) == 0 || get_u64(header, 72) != actual_checksum {
        return Err(HistoryError::CorruptIndex(
            "sparse index header is unpublished or corrupt".to_owned(),
        ));
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&header[24..40]);
    if DatabaseHistoryId(id) != expectations.database_history_id
        || get_u64(header, 40) != expectations.format_generation
        || get_u64(header, 48) != expectations.database_generation
        || get_u64(header, 56) != tail.record_count
        || get_u64(header, 64) != tail.last_record_hash
    {
        return Err(HistoryError::CorruptIndex(
            "sparse index is stale for this history tail".to_owned(),
        ));
    }
    let entry_count = tail.record_count.div_ceil(HISTORY_INDEX_STRIDE);
    let expected_len = u64::try_from(HISTORY_SLOT_SIZE)
        .expect("history slot size fits u64")
        .checked_add(
            entry_count
                .checked_mul(u64::try_from(INDEX_ENTRY_SIZE).expect("entry size fits u64"))
                .ok_or_else(|| {
                    HistoryError::CorruptIndex("sparse index byte size overflow".to_owned())
                })?,
        )
        .ok_or_else(|| HistoryError::CorruptIndex("sparse index byte size overflow".to_owned()))?;
    if file_len != expected_len {
        return Err(HistoryError::CorruptIndex(
            "sparse index length mismatch".to_owned(),
        ));
    }
    Ok(SparseIndexBinding { tail, entry_count })
}

fn decode_index_entry(
    bytes: &[u8],
    expectations: HistoryExpectations,
    tail: HistoryTail,
    ordinal: u64,
) -> Result<SparseIndexEntry, HistoryError> {
    if bytes.len() != INDEX_ENTRY_SIZE {
        return Err(HistoryError::CorruptIndex(
            "sparse index entry is incomplete".to_owned(),
        ));
    }
    let entry = SparseIndexEntry {
        commit_seq: get_u64(bytes, 0),
        byte_offset: get_u64(bytes, 8),
    };
    let record_index = ordinal
        .checked_mul(HISTORY_INDEX_STRIDE)
        .ok_or_else(|| HistoryError::CorruptIndex("sample position overflow".to_owned()))?;
    if entry.commit_seq == 0 || entry.byte_offset != record_offset(record_index)? {
        return Err(HistoryError::CorruptIndex(format!(
            "sparse index entry {ordinal} is invalid"
        )));
    }
    if get_u64(bytes, 16) != index_entry_checksum(expectations, tail, ordinal, entry) {
        return Err(HistoryError::CorruptIndex(format!(
            "sparse index entry {ordinal} checksum mismatch"
        )));
    }
    Ok(entry)
}

fn index_entry_checksum(
    expectations: HistoryExpectations,
    tail: HistoryTail,
    ordinal: u64,
    entry: SparseIndexEntry,
) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&INDEX_ENTRY_HASH_DOMAIN);
    hasher.update(&expectations.database_history_id.0);
    hasher.update(&expectations.format_generation.to_le_bytes());
    hasher.update(&expectations.database_generation.to_le_bytes());
    hasher.update(&tail.record_count.to_le_bytes());
    hasher.update(&tail.last_record_hash.to_le_bytes());
    hasher.update(&ordinal.to_le_bytes());
    hasher.update(&entry.commit_seq.to_le_bytes());
    hasher.update(&entry.byte_offset.to_le_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("eight-byte hash"))
}

fn index_entry_offset(ordinal: u64) -> Result<u64, HistoryError> {
    u64::try_from(HISTORY_SLOT_SIZE)
        .expect("history slot size fits u64")
        .checked_add(
            ordinal
                .checked_mul(u64::try_from(INDEX_ENTRY_SIZE).expect("entry size fits u64"))
                .ok_or_else(|| {
                    HistoryError::CorruptIndex("sparse index entry offset overflow".to_owned())
                })?,
        )
        .ok_or_else(|| HistoryError::CorruptIndex("sparse index entry offset overflow".to_owned()))
}

fn read_exact_at_counted<F: VfsFile>(
    file: &F,
    cx: &Cx,
    buffer: &mut [u8],
    offset: u64,
    read_calls: &mut u64,
    bytes_read: &mut u64,
) -> Result<(), HistoryError> {
    *read_calls = read_calls.saturating_add(1);
    *bytes_read = bytes_read.saturating_add(u64::try_from(buffer.len()).unwrap_or(u64::MAX));
    read_exact_at(file, cx, buffer, offset)
}

fn read_exact_at<F: VfsFile>(
    file: &F,
    cx: &Cx,
    buffer: &mut [u8],
    offset: u64,
) -> Result<(), HistoryError> {
    let read = file.read(cx, buffer, offset)?;
    if read != buffer.len() {
        return Err(HistoryError::Storage(FrankenError::ShortRead {
            expected: buffer.len(),
            actual: read,
        }));
    }
    Ok(())
}

fn record_count_and_partial<F: VfsFile>(file: &F, cx: &Cx) -> Result<(u64, u64), HistoryError> {
    let file_len = file.file_size(cx)?;
    let header_len = u64::try_from(HISTORY_SLOT_SIZE).expect("slot size fits u64");
    if file_len < header_len {
        return Err(HistoryError::Corrupt {
            slot: None,
            reason: format!("incomplete header slot ({file_len} bytes)"),
        });
    }
    let payload_len = file_len - header_len;
    Ok((payload_len / header_len, payload_len % header_len))
}

fn record_offset(index: u64) -> Result<u64, HistoryError> {
    index
        .checked_add(1)
        .and_then(|slot| {
            slot.checked_mul(u64::try_from(HISTORY_SLOT_SIZE).expect("slot size fits u64"))
        })
        .ok_or_else(|| HistoryError::InvalidInput("history record offset overflow".to_owned()))
}

fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn error_at_slot(error: HistoryError, slot: u64) -> HistoryError {
    match error {
        HistoryError::Corrupt { reason, .. } => HistoryError::Corrupt {
            slot: Some(slot),
            reason,
        },
        other => other,
    }
}

fn checksum_with_zeroed_range(bytes: &[u8], zeroed: std::ops::Range<usize>) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[..zeroed.start]);
    hasher.update(&[0_u8; 8]);
    hasher.update(&bytes[zeroed.end..]);
    let digest = hasher.finalize();
    u64::from_le_bytes(
        digest.as_bytes()[..8]
            .try_into()
            .expect("BLAKE3 digest contains eight bytes"),
    )
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("validated fixed-width field"),
    )
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("validated fixed-width field"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated fixed-width field"),
    )
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use fsqlite_vfs::memory::{MemoryFile, MemoryVfs};
    use fsqlite_vfs::shm::ShmRegion;
    use proptest::prelude::*;

    const HISTORY_ID: DatabaseHistoryId = DatabaseHistoryId([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ]);

    fn expectations(horizon: u64) -> HistoryExpectations {
        HistoryExpectations {
            database_history_id: HISTORY_ID,
            format_generation: 7,
            database_generation: 11,
            recovered_commit_horizon: horizon,
        }
    }

    fn draft(commit_seq: u64) -> HistoryRecordDraft {
        HistoryRecordDraft {
            commit_seq,
            catalog_root_page: commit_seq.saturating_mul(3).saturating_add(1),
            wall_ts_unix_nanos: 1_700_000_000_000_000_000_u64.saturating_add(commit_seq),
            schema_epoch: commit_seq / 17,
            flags: u32::from(commit_seq == 1) * HISTORY_FLAG_CHECKPOINT_ANCHOR,
        }
    }

    fn raw_open<V: Vfs>(vfs: &V, cx: &Cx, path: &Path) -> Result<V::File, FrankenError> {
        vfs.open(
            cx,
            Some(path),
            VfsOpenFlags::MAIN_JOURNAL | VfsOpenFlags::READWRITE,
        )
        .map(|(file, _)| file)
    }

    const NO_READ_FAULT: u64 = u64::MAX;

    #[derive(Debug, Clone)]
    struct ReadFaultVfs {
        inner: MemoryVfs,
        fail_offset: Arc<AtomicU64>,
    }

    impl ReadFaultVfs {
        fn new() -> Self {
            Self {
                inner: MemoryVfs::new(),
                fail_offset: Arc::new(AtomicU64::new(NO_READ_FAULT)),
            }
        }

        fn fail_reads_at(&self, offset: u64) {
            self.fail_offset.store(offset, Ordering::SeqCst);
        }

        fn clear_read_fault(&self) {
            self.fail_offset.store(NO_READ_FAULT, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct ReadFaultFile {
        inner: MemoryFile,
        fail_offset: Arc<AtomicU64>,
    }

    impl Vfs for ReadFaultVfs {
        type File = ReadFaultFile;

        fn name(&self) -> &'static str {
            "history-read-fault"
        }

        fn open(
            &self,
            cx: &Cx,
            path: Option<&Path>,
            flags: VfsOpenFlags,
        ) -> Result<(Self::File, VfsOpenFlags), FrankenError> {
            let (inner, actual_flags) = self.inner.open(cx, path, flags)?;
            Ok((
                ReadFaultFile {
                    inner,
                    fail_offset: Arc::clone(&self.fail_offset),
                },
                actual_flags,
            ))
        }

        fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<(), FrankenError> {
            self.inner.delete(cx, path, sync_dir)
        }

        fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool, FrankenError> {
            self.inner.access(cx, path, flags)
        }

        fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf, FrankenError> {
            self.inner.full_pathname(cx, path)
        }

        fn is_memory(&self) -> bool {
            true
        }
    }

    impl VfsFile for ReadFaultFile {
        fn close(&mut self, cx: &Cx) -> Result<(), FrankenError> {
            self.inner.close(cx)
        }

        fn file_identity(&self) -> Result<Option<fsqlite_vfs::traits::FileIdentity>, FrankenError> {
            self.inner.file_identity()
        }

        fn read(&self, cx: &Cx, buffer: &mut [u8], offset: u64) -> Result<usize, FrankenError> {
            if self.fail_offset.load(Ordering::SeqCst) == offset {
                return Err(FrankenError::Io(std::io::Error::other(
                    "injected history read failure",
                )));
            }
            self.inner.read(cx, buffer, offset)
        }

        fn write(&mut self, cx: &Cx, buffer: &[u8], offset: u64) -> Result<(), FrankenError> {
            self.inner.write(cx, buffer, offset)
        }

        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<(), FrankenError> {
            self.inner.truncate(cx, size)
        }

        fn sync(
            &mut self,
            cx: &Cx,
            flags: fsqlite_types::flags::SyncFlags,
        ) -> Result<(), FrankenError> {
            self.inner.sync(cx, flags)
        }

        fn file_size(&self, cx: &Cx) -> Result<u64, FrankenError> {
            self.inner.file_size(cx)
        }

        fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<(), FrankenError> {
            self.inner.lock(cx, level)
        }

        fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<(), FrankenError> {
            self.inner.unlock(cx, level)
        }

        fn check_reserved_lock(&self, cx: &Cx) -> Result<bool, FrankenError> {
            self.inner.check_reserved_lock(cx)
        }

        fn shm_map(
            &mut self,
            cx: &Cx,
            region: u32,
            size: u32,
            extend: bool,
        ) -> Result<ShmRegion, FrankenError> {
            self.inner.shm_map(cx, region, size, extend)
        }

        fn shm_lock(
            &mut self,
            cx: &Cx,
            offset: u32,
            count: u32,
            flags: u32,
        ) -> Result<(), FrankenError> {
            self.inner.shm_lock(cx, offset, count, flags)
        }

        fn shm_barrier(&self) {
            self.inner.shm_barrier();
        }

        fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<(), FrankenError> {
            self.inner.shm_unmap(cx, delete)
        }
    }

    #[test]
    fn header_round_trip_binds_identity_and_generations() {
        let header = expectations(99).header().expect("valid header");
        let encoded = header.encode_slot();
        assert_eq!(
            HistoryHeader::decode_slot(&encoded).expect("decode"),
            header
        );
        assert_eq!(&encoded[..8], b"FSQLHST1");
        assert!(encoded[HEADER_V1_SIZE..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn file_image_encoding_is_canonical_little_endian() {
        let draft = HistoryRecordDraft {
            commit_seq: 0x0102_0304_0506_0708,
            catalog_root_page: 0x1112_1314_1516_1718,
            wall_ts_unix_nanos: 0x2122_2324_2526_2728,
            schema_epoch: 0x3132_3334_3536_3738,
            flags: HISTORY_FLAG_CHECKPOINT_ANCHOR,
        };
        let record = HistoryRecord::from_draft(draft, 0).expect("record");
        assert_eq!(record.this_record_blake3_64, 0xee30_7fc1_7b69_10bb);
        let bytes = record.encode();
        let expected = [
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13,
            0x12, 0x11, 0x28, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0xbb, 0x10, 0x69, 0x7b, 0xc1, 0x7f, 0x30, 0xee, 0x38, 0x37,
            0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(bytes, expected);
        assert_eq!(HistoryRecord::decode(&bytes).expect("decode"), record);

        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("cross-endian-golden.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(draft.commit_seq));
        log.initialize().expect("initialize golden file");
        assert_eq!(log.append(draft).expect("append golden record"), record);
        let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open golden file");
        let mut image = [0_u8; HISTORY_SLOT_SIZE * 2];
        assert_eq!(
            file.read(&cx, &mut image, 0).expect("read golden file"),
            image.len()
        );
        file.close(&cx).expect("close golden file");
        assert_eq!(
            blake3::hash(&image).to_hex().as_str(),
            "5cf4cdfbcc7f6256ba3a836e7f0c18111cb63112a24cf367a9fe27bcd156eb33"
        );
    }

    #[test]
    fn first_record_requires_catalog_anchor() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let log = HistoryLog::new(&cx, &vfs, Path::new("anchor.db"), expectations(2));
        log.initialize().expect("initialize");
        let mut first = draft(2);
        first.flags = 0;
        assert!(matches!(
            log.append(first),
            Err(HistoryError::InvalidInput(message)) if message.contains("checkpoint anchor")
        ));
    }

    #[test]
    fn recovery_rejects_identity_and_generation_mismatches() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("identity.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(1));
        log.initialize().expect("initialize");

        let wrong_identity = HistoryLog::new(
            &cx,
            &vfs,
            path,
            HistoryExpectations {
                database_history_id: DatabaseHistoryId([0x44; 16]),
                ..expectations(1)
            },
        );
        assert!(matches!(
            wrong_identity.recover(),
            Err(HistoryError::IdentityMismatch { .. })
        ));

        let wrong_format = HistoryLog::new(
            &cx,
            &vfs,
            path,
            HistoryExpectations {
                format_generation: 8,
                ..expectations(1)
            },
        );
        assert!(matches!(
            wrong_format.recover(),
            Err(HistoryError::FormatGenerationMismatch { .. })
        ));

        let wrong_database = HistoryLog::new(
            &cx,
            &vfs,
            path,
            HistoryExpectations {
                database_generation: 12,
                ..expectations(1)
            },
        );
        assert!(matches!(
            wrong_database.recover(),
            Err(HistoryError::DatabaseGenerationMismatch { .. })
        ));
    }

    #[test]
    fn recovered_commit_horizon_truncates_only_valid_suffix() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("horizon.db");
        let writer = HistoryLog::new(&cx, &vfs, path, expectations(3));
        writer.initialize().expect("initialize");
        writer
            .append_batch(&[draft(1), draft(2), draft(3)])
            .expect("append");

        let recovered = HistoryLog::new(&cx, &vfs, path, expectations(2));
        let report = recovered.recover().expect("recover");
        assert_eq!(report.valid_records, 2);
        assert_eq!(report.truncated_records, 1);
        assert_eq!(
            report.final_file_len,
            u64::try_from(HISTORY_SLOT_SIZE * 3).expect("file length")
        );
        assert_eq!(
            recovered
                .read_all()
                .expect("read")
                .iter()
                .map(|record| record.commit_seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn interior_corruption_is_never_truncated_as_a_torn_tail() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("interior-corruption.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(2));
        log.initialize().expect("initialize");
        log.append_batch(&[draft(1), draft(2)]).expect("append");
        let original_len = u64::try_from(HISTORY_SLOT_SIZE * 3).expect("file length");

        let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open raw");
        file.write(
            &cx,
            &[0xff],
            u64::try_from(HISTORY_SLOT_SIZE).expect("offset"),
        )
        .expect("corrupt first record");
        file.close(&cx).expect("close raw");

        assert!(matches!(
            log.recover(),
            Err(HistoryError::Corrupt { slot: Some(0), .. })
        ));
        let file = raw_open(&vfs, &cx, log.history_path()).expect("reopen raw");
        assert_eq!(file.file_size(&cx).expect("size"), original_len);
    }

    #[test]
    fn unsupported_final_record_version_is_not_truncated() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("unsupported-tail-version.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(2));
        log.initialize().expect("initialize");
        log.append_batch(&[draft(1), draft(2)]).expect("append");
        let original_len = record_offset(2).expect("file length");

        let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open raw");
        file.write(
            &cx,
            &2_u16.to_le_bytes(),
            record_offset(1).expect("final record offset") + 52,
        )
        .expect("write unsupported version");
        file.close(&cx).expect("close raw");

        assert!(matches!(
            log.recover(),
            Err(HistoryError::UnsupportedRecordVersion(2))
        ));
        let file = raw_open(&vfs, &cx, log.history_path()).expect("reopen raw");
        assert_eq!(file.file_size(&cx).expect("size"), original_len);
    }

    #[test]
    fn final_record_storage_error_is_not_truncated() {
        let cx = Cx::new();
        let vfs = ReadFaultVfs::new();
        let path = Path::new("storage-error-tail.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(2));
        log.initialize().expect("initialize");
        log.append_batch(&[draft(1), draft(2)]).expect("append");
        let original_len = record_offset(2).expect("file length");

        vfs.fail_reads_at(record_offset(1).expect("final record offset"));
        assert!(matches!(log.recover(), Err(HistoryError::Storage(_))));
        vfs.clear_read_fault();

        let file = raw_open(&vfs, &cx, log.history_path()).expect("reopen raw");
        assert_eq!(file.file_size(&cx).expect("size"), original_len);
        assert_eq!(
            log.read_all()
                .expect("read intact history")
                .iter()
                .map(|record| record.commit_seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn crash_mid_append_every_byte_offset_truncates_exactly_one_slot() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("every-byte-crash.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(2));
        log.initialize().expect("initialize");
        let first = log.append(draft(1)).expect("first record");
        let second = HistoryRecord::from_draft(draft(2), first.this_record_blake3_64)
            .expect("second record")
            .encode_slot();
        let stable_len = record_offset(1).expect("stable length");

        // Exhaustive rather than sampled: model SIGKILL after every byte of
        // the 4096-byte slot write and run the ordinary restart repair path.
        for partial_len in 0..HISTORY_SLOT_SIZE {
            let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open raw");
            file.truncate(&cx, stable_len).expect("reset tail");
            if partial_len != 0 {
                file.write(
                    &cx,
                    &second[..partial_len],
                    record_offset(1).expect("second offset"),
                )
                .expect("write partial record");
            }
            file.close(&cx).expect("close raw");

            let report = log.recover().expect("restart recovery");
            assert_eq!(report.valid_records, 1, "partial_len={partial_len}");
            assert_eq!(
                report.final_file_len, stable_len,
                "partial_len={partial_len}"
            );
            assert_eq!(
                report.truncated_partial_bytes,
                u64::try_from(partial_len).expect("partial length"),
                "partial_len={partial_len}"
            );
        }

        let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open raw");
        file.write(&cx, &second, record_offset(1).expect("second offset"))
            .expect("write full slot");
        file.close(&cx).expect("close raw");
        assert_eq!(log.recover().expect("recover full slot").valid_records, 2);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn crash_prefix_property_never_exposes_torn_record(partial_len in 0_usize..HISTORY_SLOT_SIZE) {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let path = Path::new("proptest-crash.db");
            let log = HistoryLog::new(&cx, &vfs, path, expectations(2));
            log.initialize().expect("initialize");
            let first = log.append(draft(1)).expect("first record");
            let second = HistoryRecord::from_draft(draft(2), first.this_record_blake3_64)
                .expect("second record")
                .encode_slot();
            let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open raw");
            if partial_len != 0 {
                file.write(
                    &cx,
                    &second[..partial_len],
                    record_offset(1).expect("second offset"),
                )
                .expect("write partial slot");
            }
            file.close(&cx).expect("close raw");
            let report = log.recover().expect("restart recovery");
            prop_assert_eq!(report.valid_records, 1);
            prop_assert_eq!(report.final_file_len, record_offset(1).expect("stable length"));
        }
    }

    #[test]
    fn v1_reader_ignores_hypothetical_v2_slot_extension() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("forward-compatible.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(1));
        log.initialize().expect("initialize");
        let record = HistoryRecord::from_draft(draft(1), 0).expect("record");
        let mut extended_slot = record.encode_slot();
        extended_slot[HISTORY_RECORD_V1_SIZE..128].fill(0xa5);
        let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open raw");
        file.write(
            &cx,
            &extended_slot,
            record_offset(0).expect("record offset"),
        )
        .expect("write extended slot");
        file.close(&cx).expect("close raw");

        assert_eq!(log.recover().expect("recover").valid_records, 1);
        assert_eq!(log.read_all().expect("read"), vec![record]);
    }

    #[test]
    fn sparse_index_is_bound_to_exact_tail_and_caps_refinement() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("index.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(4097));
        log.initialize().expect("initialize");
        let drafts: Vec<_> = (1..=4096).map(draft).collect();
        log.append_batch(&drafts).expect("append");
        let index = log.rebuild_sparse_index().expect("build index");
        assert_eq!(index.entries().len(), 4);
        let (record, stats) = log.lookup_floor(3500).expect("lookup");
        assert_eq!(record.commit_seq, 3500);
        assert!(stats.index_probes <= 3);
        assert!(stats.record_probes <= HISTORY_INDEX_STRIDE);

        log.append(draft(4097)).expect("append after index");
        let (record, _) = log.lookup_floor(4097).expect("rebuild stale index");
        assert_eq!(record.commit_seq, 4097);
    }

    #[test]
    fn sparse_index_entry_must_match_history_record_even_with_valid_checksum() {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let path = Path::new("index-entry-validation.db");
        let log = HistoryLog::new(&cx, &vfs, path, expectations(2048));
        log.initialize().expect("initialize");
        let drafts: Vec<_> = (1..=2048).map(draft).collect();
        log.append_batch(&drafts).expect("append");
        let mut index = log.rebuild_sparse_index().expect("build index");
        index.entries[0].commit_seq = 2;
        log.write_sparse_index(&index)
            .expect("publish internally consistent but incorrect index");

        // The disk binary search compares each probed entry with the
        // authoritative history record. It rejects the wrong entry despite
        // its valid tail-bound checksum and rebuilds before answering.
        let (record, _) = log.lookup_floor(1).expect("lookup rebuilds index");
        assert_eq!(record.commit_seq, 1);
        let repaired = log
            .read_sparse_index(
                2048,
                log.read_all().expect("records")[2047].this_record_blake3_64,
            )
            .expect("read repaired index");
        assert_eq!(repaired.entries()[0].commit_seq, 1);
    }

    #[test]
    fn ten_million_record_probe_proof_is_logarithmic_plus_fixed_window() {
        const RECORD_COUNT: u64 = 10_000_000;
        let last_hash = 0xfeed_face_cafe_beef;
        let index = SparseIndex::build_with(RECORD_COUNT, last_hash, |position| Ok(position + 1))
            .expect("synthetic index");
        let target = RECORD_COUNT - 17;
        let linear = linear_bisect_floor_with(RECORD_COUNT, target, |position| Ok(position + 1))
            .expect("linear lookup");
        let sparse = sparse_bisect_floor_with(&index, target, |position| Ok(position + 1))
            .expect("sparse lookup");
        assert_eq!(linear.record_index, sparse.record_index);
        assert_eq!(linear.stats.record_probes, target + 1);
        assert!(sparse.stats.index_probes <= 14);
        assert!(sparse.stats.record_probes <= HISTORY_INDEX_STRIDE);
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn ten_million_record_vfs_lookup_does_not_scan_the_full_history() {
        use fsqlite_vfs::unix::UnixVfs;

        const RECORD_COUNT: u64 = 10_000_000;
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join("ten-million-lookup.db");
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let log = HistoryLog::new(&cx, &vfs, &database_path, expectations(RECORD_COUNT));
        log.initialize().expect("initialize sparse history");

        let mut file = raw_open(&vfs, &cx, log.history_path()).expect("open sparse history");
        file.truncate(
            &cx,
            record_offset(RECORD_COUNT).expect("logical file length"),
        )
        .expect("create 40.96 GB sparse extent");

        // Materialize each sample and its predecessor. All other early slots
        // remain sparse holes, so this test would fail immediately if the
        // production lookup regressed to `read_all` or another full scan.
        let mut position = 0;
        while position < RECORD_COUNT {
            if position == 0 {
                let anchor = HistoryRecord::from_draft(draft(1), 0).expect("anchor");
                file.write(
                    &cx,
                    &anchor.encode_slot(),
                    record_offset(0).expect("anchor offset"),
                )
                .expect("write anchor sample");
            } else {
                let predecessor =
                    HistoryRecord::from_draft(draft(position), position.rotate_left(17).max(1))
                        .expect("sample predecessor");
                file.write(
                    &cx,
                    &predecessor.encode_slot(),
                    record_offset(position - 1).expect("predecessor offset"),
                )
                .expect("write sample predecessor");
                let sample = HistoryRecord::from_draft(
                    draft(position + 1),
                    predecessor.this_record_blake3_64,
                )
                .expect("sample");
                file.write(
                    &cx,
                    &sample.encode_slot(),
                    record_offset(position).expect("sample offset"),
                )
                .expect("write sample");
            }
            position = position
                .checked_add(HISTORY_INDEX_STRIDE)
                .expect("sample position");
        }

        // The final lookup window is a real contiguous hash chain. It
        // overwrites the last independently materialized sample pair.
        let final_sample = (RECORD_COUNT - 1) / HISTORY_INDEX_STRIDE * HISTORY_INDEX_STRIDE;
        let predecessor_position = final_sample - 1;
        let mut previous =
            HistoryRecord::from_draft(draft(predecessor_position + 1), 0xa5a5_5a5a_f00d_cafe)
                .expect("final-window predecessor");
        file.write(
            &cx,
            &previous.encode_slot(),
            record_offset(predecessor_position).expect("final predecessor offset"),
        )
        .expect("write final predecessor");
        for position in final_sample..RECORD_COUNT {
            let record =
                HistoryRecord::from_draft(draft(position + 1), previous.this_record_blake3_64)
                    .expect("final-window record");
            file.write(
                &cx,
                &record.encode_slot(),
                record_offset(position).expect("final-window offset"),
            )
            .expect("write final-window record");
            previous = record;
        }
        assert_eq!(
            file.file_size(&cx).expect("sparse history size"),
            record_offset(RECORD_COUNT).expect("logical file length")
        );
        file.close(&cx).expect("close sparse history");

        let index = SparseIndex::build_with(
            RECORD_COUNT,
            previous.this_record_blake3_64,
            |sample_position| Ok(sample_position + 1),
        )
        .expect("build exact-tail sparse index");
        log.write_sparse_index(&index)
            .expect("publish sparse index");

        let target = RECORD_COUNT - 17;
        let (cold_record, cold_stats) = log.lookup_floor(target).expect("cold VFS lookup");
        let (second_record, second_stats) = log.lookup_floor(target).expect("second VFS lookup");
        assert_eq!(cold_record.commit_seq, target);
        assert_eq!(second_record, cold_record);
        for stats in [cold_stats, second_stats] {
            assert!(stats.index_probes <= 14);
            assert!(stats.record_probes <= HISTORY_INDEX_STRIDE);
            assert!(stats.index_read_calls <= 15, "{stats:?}");
            assert!(
                stats.history_read_calls <= HISTORY_INDEX_STRIDE + 20,
                "{stats:?}"
            );
            assert!(stats.index_bytes_read <= 4_432, "{stats:?}");
            assert!(stats.history_bytes_read <= 80_000, "{stats:?}");
        }
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn ten_thousand_random_records_survive_full_durable_round_trip() {
        use fsqlite_vfs::unix::UnixVfs;

        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join("round-trip.db");
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        let log = HistoryLog::new(&cx, &vfs, &database_path, expectations(10_000));
        log.initialize().expect("durable initialize");

        let mut state = 0x3b5d_6f71_8293_a4b5_u64;
        let mut drafts = Vec::with_capacity(10_000);
        for commit_seq in 1..=10_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            drafts.push(HistoryRecordDraft {
                commit_seq,
                catalog_root_page: state.max(1),
                wall_ts_unix_nanos: state.rotate_left(19),
                schema_epoch: state.rotate_right(11),
                flags: u32::from(commit_seq == 1) * HISTORY_FLAG_CHECKPOINT_ANCHOR,
            });
        }
        let expected = log.append_batch(&drafts).expect("append and fsync");
        let serialized = serde_json::to_vec(&expected).expect("serialize records");
        let serde_round_trip: Vec<HistoryRecord> =
            serde_json::from_slice(&serialized).expect("deserialize records");
        assert_eq!(serde_round_trip, expected);
        drop(log);

        let reopened = HistoryLog::new(&cx, &vfs, &database_path, expectations(10_000));
        assert_eq!(
            reopened.recover().expect("restart recovery").valid_records,
            10_000
        );
        assert_eq!(reopened.read_all().expect("read back"), expected);
    }
}
