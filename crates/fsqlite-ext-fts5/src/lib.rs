//! FTS5 full-text search extension (§14.2).
//!
//! Provides: tokenizer API (unicode61, ascii, porter, trigram), inverted index,
//! boolean query parsing (implicit AND, OR, NOT binary-only, phrase, prefix,
//! NEAR, column filter, caret), BM25 ranking, FTS5 virtual table with content
//! modes, schema validation, and secure-delete / contentless-delete /
//! contentless-unindexed / insttoken / locale blob / tokendata configuration.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_func::ScalarFunction;
use fsqlite_func::vtab::{
    ColumnContext, IndexInfo, ShadowTablePolicy, TransactionalVtabState, VirtualTable,
    VirtualTableCursor, VtabIntegrityPolicy, VtabLifecyclePolicy, VtabModuleMetadata,
    VtabRiskLevel,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::serial_type::{read_varint, write_varint};
use fsqlite_types::value::{SmallText, SqliteValue};
use smallvec::SmallVec;
use tracing::debug;

// ---------------------------------------------------------------------------
// Extension name
// ---------------------------------------------------------------------------

#[must_use]
pub const fn extension_name() -> &'static str {
    "fts5"
}

// ---------------------------------------------------------------------------
// Configuration (existing + expanded)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentMode {
    Stored,
    Contentless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteAction {
    Reject,
    Tombstone,
    PhysicalPurge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailMode {
    Full,
    Column,
    None,
}

impl std::fmt::Display for DetailMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Column => write!(f, "column"),
            Self::None => write!(f, "none"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools, clippy::struct_field_names)]
pub struct Fts5Config {
    secure_delete: bool,
    content_mode: ContentMode,
    contentless_delete: bool,
    contentless_unindexed: bool,
    columnsize: bool,
    detail: DetailMode,
    insttoken: bool,
    locale: bool,
    tokendata: bool,
}

const FTS5_CONFIG_VERSION: i64 = 4;
const FTS5_CONFIG_VERSION_SECURE_DELETE: i64 = 5;
const FTS5_DEFAULT_PAGE_SIZE: i64 = 4050;
const FTS5_MAX_PAGE_SIZE: i64 = 64 * 1024;
const FTS5_DEFAULT_AUTOMERGE: i64 = 4;
const FTS5_DEFAULT_USERMERGE: i64 = 4;
const FTS5_DEFAULT_CRISISMERGE: i64 = 16;
const FTS5_MAX_SEGMENT: i64 = 2000;
const FTS5_MAX_SEGMENT_U64: u64 = 2000;
const FTS5_DEFAULT_HASHSIZE: i64 = 1024 * 1024;
const FTS5_DEFAULT_DELETE_AUTOMERGE: i64 = 10;
pub const FTS5_AVERAGES_ROWID: i64 = 1;
pub const FTS5_STRUCTURE_ROWID: i64 = 10;
const FTS5_STRUCTURE_V2_MARKER: [u8; 4] = [0xFF, 0x00, 0x00, 0x01];
const FTS5_DATA_ID_BITS: u64 = 16;
const FTS5_DATA_DLI_BITS: u64 = 1;
const FTS5_DATA_HEIGHT_BITS: u64 = 5;
const FTS5_DATA_PAGE_BITS: u64 = 31;
const FTS5_DATA_PAGE_MASK: u64 = (1 << FTS5_DATA_PAGE_BITS) - 1;
const FTS5_DATA_HEIGHT_MASK: u64 = (1 << FTS5_DATA_HEIGHT_BITS) - 1;
const FTS5_DATA_DLI_MASK: u64 = (1 << FTS5_DATA_DLI_BITS) - 1;
const FTS5_DATA_ID_LIMIT: u64 = 1 << FTS5_DATA_ID_BITS;
const FTS5_IDX_MAX_BTREE_PAGE: u32 = (1 << FTS5_DATA_PAGE_BITS) - 1;
const FTS5_MAIN_PREFIX_BYTE: u8 = b'0';
const FTS5_SEGMENT_CHECKSUM_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FTS5_SEGMENT_CHECKSUM_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ConfigRecord {
    pub key: String,
    pub value: SqliteValue,
}

impl Fts5ConfigRecord {
    #[must_use]
    pub fn integer(key: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            value: SqliteValue::Integer(value),
        }
    }

    #[must_use]
    pub fn text(key: impl Into<String>, value: impl Into<String> + AsRef<str>) -> Self {
        Self {
            key: key.into(),
            value: SqliteValue::Text(SmallText::from_string(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ContentRow {
    pub rowid: i64,
    pub values: Vec<String>,
}

impl Fts5ContentRow {
    #[must_use]
    pub fn new(rowid: i64, values: Vec<String>) -> Self {
        Self { rowid, values }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5DocsizeRow {
    pub rowid: i64,
    pub column_token_counts: Vec<u32>,
}

impl Fts5DocsizeRow {
    #[must_use]
    pub fn new(rowid: i64, column_token_counts: Vec<u32>) -> Self {
        Self {
            rowid,
            column_token_counts,
        }
    }

    #[must_use]
    pub fn total_tokens(&self) -> u32 {
        self.column_token_counts.iter().copied().sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5DataRow {
    pub id: i64,
    pub block: Vec<u8>,
}

impl Fts5DataRow {
    #[must_use]
    pub fn new(id: i64, block: Vec<u8>) -> Self {
        Self { id, block }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5AveragesRecord {
    pub total_rows: u64,
    pub column_token_totals: Vec<u64>,
}

impl Fts5AveragesRecord {
    #[must_use]
    pub fn new(total_rows: u64, column_token_totals: Vec<u64>) -> Self {
        Self {
            total_rows,
            column_token_totals,
        }
    }

    #[must_use]
    pub fn from_docsize_rows(
        total_rows: u64,
        column_count: usize,
        rows: &[Fts5DocsizeRow],
    ) -> Self {
        let mut column_token_totals = vec![0; column_count];
        for row in rows {
            for (column, count) in row
                .column_token_counts
                .iter()
                .copied()
                .take(column_count)
                .enumerate()
            {
                column_token_totals[column] += u64::from(count);
            }
        }
        Self {
            total_rows,
            column_token_totals,
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        if self.total_rows == 0 && self.column_token_totals.iter().all(|total| *total == 0) {
            return Vec::new();
        }

        let mut block = Vec::with_capacity((self.column_token_totals.len() + 1) * 9);
        fts5_push_varint(&mut block, self.total_rows);
        for total in &self.column_token_totals {
            fts5_push_varint(&mut block, *total);
        }
        block
    }

    pub fn decode(block: &[u8], column_count: usize) -> Result<Self> {
        if block.is_empty() {
            return Ok(Self {
                total_rows: 0,
                column_token_totals: vec![0; column_count],
            });
        }

        let mut offset = 0;
        let total_rows = fts5_read_varint(block, &mut offset, "averages row count")?;
        let mut column_token_totals = vec![0; column_count];
        for total in &mut column_token_totals {
            if offset == block.len() {
                break;
            }
            *total = fts5_read_varint(block, &mut offset, "averages column total")?;
        }
        if offset != block.len() {
            return Err(fts5_data_error(
                "trailing bytes after averages column totals",
            ));
        }

        Ok(Self {
            total_rows,
            column_token_totals,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5StructureSegment {
    pub segid: u32,
    pub pgno_first: u32,
    pub pgno_last: u32,
    pub origin_lower: u64,
    pub origin_upper: u64,
    pub tombstone_page_count: u32,
    pub tombstone_entry_count: u64,
    pub entry_count: u64,
}

impl Fts5StructureSegment {
    #[must_use]
    pub const fn new(segid: u32, pgno_first: u32, pgno_last: u32) -> Self {
        Self {
            segid,
            pgno_first,
            pgno_last,
            origin_lower: 0,
            origin_upper: 0,
            tombstone_page_count: 0,
            tombstone_entry_count: 0,
            entry_count: 0,
        }
    }

    #[must_use]
    pub const fn with_origin_tracking(
        mut self,
        origin_lower: u64,
        origin_upper: u64,
        tombstone_page_count: u32,
        tombstone_entry_count: u64,
        entry_count: u64,
    ) -> Self {
        self.origin_lower = origin_lower;
        self.origin_upper = origin_upper;
        self.tombstone_page_count = tombstone_page_count;
        self.tombstone_entry_count = tombstone_entry_count;
        self.entry_count = entry_count;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5StructureLevel {
    pub merge_inputs: u32,
    pub segments: Vec<Fts5StructureSegment>,
}

impl Fts5StructureLevel {
    #[must_use]
    pub fn new(merge_inputs: u32, segments: Vec<Fts5StructureSegment>) -> Self {
        Self {
            merge_inputs,
            segments,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5StructureRecord {
    pub cookie: u32,
    pub write_counter: u64,
    pub origin_counter: u64,
    pub levels: Vec<Fts5StructureLevel>,
}

impl Fts5StructureRecord {
    #[must_use]
    pub fn empty_legacy(cookie: u32) -> Self {
        Self {
            cookie,
            write_counter: 0,
            origin_counter: 0,
            levels: Vec::new(),
        }
    }

    #[must_use]
    pub fn empty_with_origin_tracking(cookie: u32) -> Self {
        Self {
            cookie,
            write_counter: 0,
            origin_counter: 1,
            levels: Vec::new(),
        }
    }

    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.levels.iter().map(|level| level.segments.len()).sum()
    }

    #[must_use]
    pub fn uses_origin_tracking(&self) -> bool {
        self.origin_counter > 0
            || self.levels.iter().any(|level| {
                level.segments.iter().any(|segment| {
                    segment.origin_lower != 0
                        || segment.origin_upper != 0
                        || segment.tombstone_page_count != 0
                        || segment.tombstone_entry_count != 0
                        || segment.entry_count != 0
                })
            })
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let use_origin_tracking = self.uses_origin_tracking();
        let segment_count = self.segment_count();
        let mut block = Vec::with_capacity(4 + usize::from(use_origin_tracking) * 4 + 27);

        block.extend_from_slice(&self.cookie.to_be_bytes());
        if use_origin_tracking {
            block.extend_from_slice(&FTS5_STRUCTURE_V2_MARKER);
        }
        fts5_push_varint(
            &mut block,
            u64::try_from(self.levels.len()).unwrap_or(u64::MAX),
        );
        fts5_push_varint(&mut block, u64::try_from(segment_count).unwrap_or(u64::MAX));
        fts5_push_varint(&mut block, self.write_counter);

        for level in &self.levels {
            fts5_push_varint(&mut block, u64::from(level.merge_inputs));
            fts5_push_varint(&mut block, level.segments.len() as u64);
            for segment in &level.segments {
                fts5_push_varint(&mut block, u64::from(segment.segid));
                fts5_push_varint(&mut block, u64::from(segment.pgno_first));
                fts5_push_varint(&mut block, u64::from(segment.pgno_last));
                if use_origin_tracking {
                    fts5_push_varint(&mut block, segment.origin_lower);
                    fts5_push_varint(&mut block, segment.origin_upper);
                    fts5_push_varint(&mut block, u64::from(segment.tombstone_page_count));
                    fts5_push_varint(&mut block, segment.tombstone_entry_count);
                    fts5_push_varint(&mut block, segment.entry_count);
                }
            }
        }

        block
    }

    pub fn decode(block: &[u8]) -> Result<Self> {
        if block.len() < 4 {
            return Err(fts5_data_error("structure record missing cookie"));
        }

        let cookie = u32::from_be_bytes([block[0], block[1], block[2], block[3]]);
        let mut offset = 4;
        let use_origin_tracking = block
            .get(offset..)
            .is_some_and(|tail| tail.starts_with(&FTS5_STRUCTURE_V2_MARKER));
        if use_origin_tracking {
            offset += FTS5_STRUCTURE_V2_MARKER.len();
        }

        let level_count = fts5_read_bounded_usize(block, &mut offset, "structure level count")?;
        let mut remaining_segments =
            fts5_read_bounded_usize(block, &mut offset, "structure segment count")?;
        let write_counter = fts5_read_varint(block, &mut offset, "structure write counter")?;
        let mut levels = Vec::with_capacity(level_count);
        let mut previous_level_merge_inputs = 0;
        let mut max_origin_upper = 0;

        for level_index in 0..level_count {
            let merge_inputs = fts5_read_u32(block, &mut offset, "structure merge count")?;
            let segment_total = fts5_read_bounded_usize(block, &mut offset, "level segment count")?;
            let merge_inputs_usize = usize::try_from(merge_inputs).unwrap_or(usize::MAX);
            if segment_total < merge_inputs_usize {
                return Err(fts5_data_error("level merge count exceeds segment count"));
            }
            if remaining_segments < segment_total {
                return Err(fts5_data_error("structure segment count underflow"));
            }
            remaining_segments -= segment_total;

            let mut segments = Vec::with_capacity(segment_total);
            for _ in 0..segment_total {
                let segid = fts5_read_u32(block, &mut offset, "segment id")?;
                let pgno_first = fts5_read_u32(block, &mut offset, "segment first page")?;
                let pgno_last = fts5_read_u32(block, &mut offset, "segment last page")?;
                if segid == 0 {
                    return Err(fts5_data_error("segment id must be non-zero"));
                }
                if pgno_first == 0 {
                    return Err(fts5_data_error("segment first page must be non-zero"));
                }
                if pgno_last < pgno_first {
                    return Err(fts5_data_error("segment last page precedes first page"));
                }

                let mut segment = Fts5StructureSegment::new(segid, pgno_first, pgno_last);
                if use_origin_tracking {
                    segment.origin_lower =
                        fts5_read_varint(block, &mut offset, "segment origin lower")?;
                    segment.origin_upper =
                        fts5_read_varint(block, &mut offset, "segment origin upper")?;
                    segment.tombstone_page_count =
                        fts5_read_u32(block, &mut offset, "segment tombstone pages")?;
                    segment.tombstone_entry_count =
                        fts5_read_varint(block, &mut offset, "segment tombstone entries")?;
                    segment.entry_count =
                        fts5_read_varint(block, &mut offset, "segment entry count")?;
                    max_origin_upper = max_origin_upper.max(segment.origin_upper);
                }
                segments.push(segment);
            }

            if level_index > 0 && previous_level_merge_inputs > 0 && segments.is_empty() {
                return Err(fts5_data_error(
                    "merge level must be followed by a non-empty level",
                ));
            }
            if level_index + 1 == level_count && merge_inputs > 0 {
                return Err(fts5_data_error("top level cannot have an active merge"));
            }
            previous_level_merge_inputs = merge_inputs;
            levels.push(Fts5StructureLevel {
                merge_inputs,
                segments,
            });
        }

        if remaining_segments != 0 {
            return Err(fts5_data_error("structure segment count mismatch"));
        }
        if offset != block.len() {
            return Err(fts5_data_error("trailing bytes after structure record"));
        }

        Ok(Self {
            cookie,
            write_counter,
            origin_counter: if use_origin_tracking {
                max_origin_upper.saturating_add(1)
            } else {
                0
            },
            levels,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5DataMetadata {
    pub averages: Option<Fts5AveragesRecord>,
    pub structure: Option<Fts5StructureRecord>,
}

impl Fts5DataMetadata {
    pub fn decode_rows(rows: &[Fts5DataRow], column_count: usize) -> Result<Self> {
        let mut averages = None;
        let mut structure = None;

        for row in rows {
            match row.id {
                FTS5_AVERAGES_ROWID => {
                    if averages.is_some() {
                        return Err(fts5_data_error("duplicate averages row"));
                    }
                    averages = Some(Fts5AveragesRecord::decode(&row.block, column_count)?);
                }
                FTS5_STRUCTURE_ROWID => {
                    if structure.is_some() {
                        return Err(fts5_data_error("duplicate structure row"));
                    }
                    structure = Some(Fts5StructureRecord::decode(&row.block)?);
                }
                _ => {}
            }
        }

        Ok(Self {
            averages,
            structure,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5DataRowid {
    Averages,
    Structure,
    SegmentLeaf { segid: u32, pgno: u32 },
    DoclistIndex { segid: u32, height: u8, pgno: u32 },
    Tombstone { segid: u32, hash_pgno: u32 },
}

impl Fts5DataRowid {
    pub fn encode(self) -> Result<i64> {
        match self {
            Self::Averages => Ok(FTS5_AVERAGES_ROWID),
            Self::Structure => Ok(FTS5_STRUCTURE_ROWID),
            Self::SegmentLeaf { segid, pgno } => {
                fts5_data_rowid(segid, false, 0, pgno, "segment leaf")
            }
            Self::DoclistIndex {
                segid,
                height,
                pgno,
            } => fts5_data_rowid(segid, true, height, pgno, "doclist index"),
            Self::Tombstone { segid, hash_pgno } => {
                let tombstone_segid = u64::from(segid) + FTS5_DATA_ID_LIMIT;
                fts5_data_rowid_from_parts(tombstone_segid, false, 0, hash_pgno)
            }
        }
    }

    pub fn decode(rowid: i64) -> Result<Self> {
        match rowid {
            FTS5_AVERAGES_ROWID => return Ok(Self::Averages),
            FTS5_STRUCTURE_ROWID => return Ok(Self::Structure),
            _ => {}
        }

        let raw = u64::try_from(rowid)
            .map_err(|_| fts5_data_error("negative segment rowid in %_data"))?;
        let pgno = u32::try_from(raw & FTS5_DATA_PAGE_MASK)
            .map_err(|_| fts5_data_error("page number exceeds u32"))?;
        let height = u8::try_from((raw >> FTS5_DATA_PAGE_BITS) & FTS5_DATA_HEIGHT_MASK)
            .map_err(|_| fts5_data_error("dlidx height exceeds u8"))?;
        let dlidx =
            ((raw >> (FTS5_DATA_PAGE_BITS + FTS5_DATA_HEIGHT_BITS)) & FTS5_DATA_DLI_MASK) != 0;
        let segid_raw = raw >> (FTS5_DATA_PAGE_BITS + FTS5_DATA_HEIGHT_BITS + FTS5_DATA_DLI_BITS);

        if dlidx {
            let segid =
                u32::try_from(segid_raw).map_err(|_| fts5_data_error("dlidx segid overflow"))?;
            return Ok(Self::DoclistIndex {
                segid,
                height,
                pgno,
            });
        }

        if segid_raw >= FTS5_DATA_ID_LIMIT {
            let segid = u32::try_from(segid_raw - FTS5_DATA_ID_LIMIT)
                .map_err(|_| fts5_data_error("tombstone segid overflow"))?;
            return Ok(Self::Tombstone {
                segid,
                hash_pgno: pgno,
            });
        }

        let segid =
            u32::try_from(segid_raw).map_err(|_| fts5_data_error("segment segid overflow"))?;
        Ok(Self::SegmentLeaf { segid, pgno })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5IdxRow {
    pub segid: u32,
    pub term: Vec<u8>,
    pub btree_page: u32,
    pub has_doclist_index: bool,
}

impl Fts5IdxRow {
    #[must_use]
    pub fn new(
        segid: u32,
        term: impl Into<Vec<u8>>,
        btree_page: u32,
        has_doclist_index: bool,
    ) -> Self {
        Self {
            segid,
            term: term.into(),
            btree_page,
            has_doclist_index,
        }
    }

    pub fn encoded_pgno(&self) -> Result<i64> {
        if self.btree_page > FTS5_IDX_MAX_BTREE_PAGE {
            return Err(fts5_data_error("%_idx btree page exceeds 31-bit limit"));
        }
        Ok((i64::from(self.btree_page) << 1) | i64::from(self.has_doclist_index))
    }

    pub fn from_encoded_pgno(
        segid: u32,
        term: impl Into<Vec<u8>>,
        encoded_pgno: i64,
    ) -> Result<Self> {
        if encoded_pgno < 0 {
            return Err(fts5_data_error("negative %_idx pgno"));
        }
        let encoded =
            u64::try_from(encoded_pgno).map_err(|_| fts5_data_error("%_idx pgno overflow"))?;
        let btree_page = u32::try_from(encoded >> 1)
            .map_err(|_| fts5_data_error("%_idx btree page overflow"))?;
        if btree_page > FTS5_IDX_MAX_BTREE_PAGE {
            return Err(fts5_data_error("%_idx btree page exceeds 31-bit limit"));
        }
        Ok(Self {
            segid,
            term: term.into(),
            btree_page,
            has_doclist_index: (encoded & 1) != 0,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ColumnPositions {
    pub column: u32,
    pub offsets: Vec<u32>,
}

impl Fts5ColumnPositions {
    #[must_use]
    pub fn new(column: u32, offsets: Vec<u32>) -> Self {
        Self { column, offsets }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5Poslist {
    pub delete: bool,
    pub columns: Vec<Fts5ColumnPositions>,
}

impl Fts5Poslist {
    #[must_use]
    pub fn new(delete: bool, columns: Vec<Fts5ColumnPositions>) -> Self {
        Self { delete, columns }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        let mut columns = self.columns.clone();
        columns.sort_unstable_by_key(|column| column.column);

        for column in columns {
            if column.column != 0 || !body.is_empty() {
                body.push(0x01);
                fts5_push_varint(&mut body, u64::from(column.column));
            }
            let mut previous_offset = 0_u32;
            let mut offsets = column.offsets;
            offsets.sort_unstable();
            for offset in offsets {
                if offset < previous_offset {
                    return Err(fts5_data_error("poslist offsets are not monotonic"));
                }
                fts5_push_varint(&mut body, u64::from(offset - previous_offset + 2));
                previous_offset = offset;
            }
        }

        let body_len = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let header = body_len
            .checked_mul(2)
            .and_then(|value| value.checked_add(u64::from(self.delete)))
            .ok_or_else(|| fts5_data_error("poslist too large"))?;
        let mut encoded = Vec::with_capacity(body.len() + 9);
        fts5_push_varint(&mut encoded, header);
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(block: &[u8], offset: &mut usize) -> Result<Self> {
        let header = fts5_read_varint(block, offset, "poslist size")?;
        let body_len =
            usize::try_from(header >> 1).map_err(|_| fts5_data_error("poslist body too large"))?;
        let delete = (header & 1) != 0;
        let end = offset
            .checked_add(body_len)
            .ok_or_else(|| fts5_data_error("poslist length overflow"))?;
        if end > block.len() {
            return Err(fts5_data_error("truncated poslist body"));
        }

        let mut columns = Vec::new();
        let mut current_column = 0;
        while *offset < end {
            if block[*offset] == 0x01 {
                *offset += 1;
                current_column = fts5_read_u32(block, offset, "poslist column")?;
                if *offset >= end {
                    return Err(fts5_data_error("poslist column has no offsets"));
                }
            }

            let mut offsets = Vec::new();
            let mut previous_offset = 0_u32;
            while *offset < end && block[*offset] != 0x01 {
                let delta = fts5_read_u32(block, offset, "poslist offset delta")?;
                if delta < 2 {
                    return Err(fts5_data_error("poslist offset delta below 2"));
                }
                let offset_value = previous_offset
                    .checked_add(delta - 2)
                    .ok_or_else(|| fts5_data_error("poslist offset overflow"))?;
                offsets.push(offset_value);
                previous_offset = offset_value;
            }
            if offsets.is_empty() {
                return Err(fts5_data_error("poslist column has no offsets"));
            }
            columns.push(Fts5ColumnPositions {
                column: current_column,
                offsets,
            });
        }

        Ok(Self { delete, columns })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5DoclistEntry {
    pub rowid: u64,
    pub poslist: Fts5Poslist,
}

impl Fts5DoclistEntry {
    #[must_use]
    pub fn new(rowid: u64, poslist: Fts5Poslist) -> Self {
        Self { rowid, poslist }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5Doclist {
    pub entries: Vec<Fts5DoclistEntry>,
}

impl Fts5Doclist {
    #[must_use]
    pub fn new(entries: Vec<Fts5DoclistEntry>) -> Self {
        Self { entries }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        let mut previous_rowid = 0_u64;
        for (index, entry) in self.entries.iter().enumerate() {
            if index == 0 {
                fts5_push_varint(&mut encoded, entry.rowid);
            } else {
                if entry.rowid <= previous_rowid {
                    return Err(fts5_data_error(
                        "doclist rowids must be strictly increasing",
                    ));
                }
                fts5_push_varint(&mut encoded, entry.rowid - previous_rowid);
            }
            encoded.extend_from_slice(&entry.poslist.encode()?);
            previous_rowid = entry.rowid;
        }
        Ok(encoded)
    }

    pub fn decode(block: &[u8]) -> Result<Self> {
        let mut offset = 0;
        let mut entries = Vec::new();
        let mut previous_rowid = 0_u64;
        while offset < block.len() {
            let raw_rowid = fts5_read_varint(block, &mut offset, "doclist rowid")?;
            let rowid = if entries.is_empty() {
                raw_rowid
            } else {
                if raw_rowid == 0 {
                    return Err(fts5_data_error("doclist rowid delta must be positive"));
                }
                previous_rowid
                    .checked_add(raw_rowid)
                    .ok_or_else(|| fts5_data_error("doclist rowid overflow"))?
            };
            let poslist = Fts5Poslist::decode(block, &mut offset)?;
            entries.push(Fts5DoclistEntry { rowid, poslist });
            previous_rowid = rowid;
        }
        Ok(Self { entries })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5SegmentTerm {
    pub term: Vec<u8>,
    pub doclist: Fts5Doclist,
}

impl Fts5SegmentTerm {
    #[must_use]
    pub fn new(term: impl Into<Vec<u8>>, doclist: Fts5Doclist) -> Self {
        Self {
            term: term.into(),
            doclist,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5SegmentLeaf {
    pub first_rowid_offset: u16,
    pub terms: Vec<Fts5SegmentTerm>,
}

impl Fts5SegmentLeaf {
    #[must_use]
    pub fn new(terms: Vec<Fts5SegmentTerm>) -> Self {
        Self {
            first_rowid_offset: 0,
            terms,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut page = vec![0, 0, 0, 0];
        let mut term_offsets = Vec::with_capacity(self.terms.len());
        let mut previous_term: &[u8] = &[];
        let mut first_rowid_offset = 0;

        for (index, entry) in self.terms.iter().enumerate() {
            if index > 0 && entry.term.as_slice() <= previous_term {
                return Err(fts5_data_error("segment terms must be strictly increasing"));
            }
            let term_offset = u16::try_from(page.len())
                .map_err(|_| fts5_data_error("segment leaf term offset exceeds u16"))?;
            term_offsets.push(term_offset);

            if index == 0 {
                fts5_push_varint(
                    &mut page,
                    u64::try_from(entry.term.len()).unwrap_or(u64::MAX),
                );
                page.extend_from_slice(&entry.term);
            } else {
                let prefix_len = common_prefix_len(previous_term, &entry.term);
                fts5_push_varint(&mut page, u64::try_from(prefix_len).unwrap_or(u64::MAX));
                fts5_push_varint(
                    &mut page,
                    u64::try_from(entry.term.len() - prefix_len).unwrap_or(u64::MAX),
                );
                page.extend_from_slice(&entry.term[prefix_len..]);
            }

            if index == 0 {
                first_rowid_offset = u16::try_from(page.len())
                    .map_err(|_| fts5_data_error("segment leaf rowid offset exceeds u16"))?;
            }
            page.extend_from_slice(&entry.doclist.encode()?);
            previous_term = &entry.term;
        }

        let footer_offset = u16::try_from(page.len())
            .map_err(|_| fts5_data_error("segment leaf footer offset exceeds u16"))?;
        let mut previous_offset = 0;
        for offset in term_offsets {
            let delta = offset
                .checked_sub(previous_offset)
                .ok_or_else(|| fts5_data_error("segment footer offset underflow"))?;
            fts5_push_varint(&mut page, u64::from(delta));
            previous_offset = offset;
        }

        write_be_u16(&mut page[0..2], first_rowid_offset);
        write_be_u16(&mut page[2..4], footer_offset);
        Ok(page)
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.len() < 4 {
            return Err(fts5_data_error("segment leaf missing header"));
        }
        let first_rowid_offset = read_be_u16(&page[0..2]);
        let footer_offset = usize::from(read_be_u16(&page[2..4]));
        if !(4..=page.len()).contains(&footer_offset) {
            return Err(fts5_data_error("segment leaf footer offset out of range"));
        }

        let mut footer_cursor = footer_offset;
        let mut term_offsets = Vec::new();
        let mut previous_term_offset = 0_u16;
        while footer_cursor < page.len() {
            let delta = fts5_read_u32(page, &mut footer_cursor, "segment footer offset delta")?;
            let next_offset = u32::from(previous_term_offset)
                .checked_add(delta)
                .ok_or_else(|| fts5_data_error("segment footer offset overflow"))?;
            let next_offset = u16::try_from(next_offset)
                .map_err(|_| fts5_data_error("segment footer offset exceeds u16"))?;
            let next_offset_usize = usize::from(next_offset);
            if next_offset_usize < 4 || next_offset_usize >= footer_offset {
                return Err(fts5_data_error("segment term offset out of range"));
            }
            term_offsets.push(next_offset);
            previous_term_offset = next_offset;
        }

        let mut terms = Vec::with_capacity(term_offsets.len());
        let mut previous_term = Vec::new();
        for (index, term_offset) in term_offsets.iter().copied().enumerate() {
            let entry_end = term_offsets
                .get(index + 1)
                .map_or(footer_offset, |offset| usize::from(*offset));
            let mut cursor = usize::from(term_offset);

            let term = if index == 0 {
                let term_len = fts5_read_usize(page, &mut cursor, "segment first term")?;
                let end = cursor
                    .checked_add(term_len)
                    .ok_or_else(|| fts5_data_error("segment first term length overflow"))?;
                if end > entry_end {
                    return Err(fts5_data_error("segment first term extends past entry"));
                }
                let term = page[cursor..end].to_vec();
                cursor = end;
                term
            } else {
                let prefix_len = fts5_read_usize(page, &mut cursor, "segment term prefix")?;
                let suffix_len = fts5_read_usize(page, &mut cursor, "segment term suffix")?;
                if prefix_len > previous_term.len() {
                    return Err(fts5_data_error("segment term prefix exceeds previous term"));
                }
                let suffix_end = cursor
                    .checked_add(suffix_len)
                    .ok_or_else(|| fts5_data_error("segment term suffix length overflow"))?;
                if suffix_end > entry_end {
                    return Err(fts5_data_error("segment term suffix extends past entry"));
                }
                let mut term = previous_term[..prefix_len].to_vec();
                term.extend_from_slice(&page[cursor..suffix_end]);
                cursor = suffix_end;
                term
            };

            let doclist = Fts5Doclist::decode(&page[cursor..entry_end])?;
            previous_term.clone_from(&term);
            terms.push(Fts5SegmentTerm { term, doclist });
        }

        Ok(Self {
            first_rowid_offset,
            terms,
        })
    }

    pub fn to_data_row(&self, segid: u32, pgno: u32) -> Result<Fts5DataRow> {
        Ok(Fts5DataRow::new(
            Fts5DataRowid::SegmentLeaf { segid, pgno }.encode()?,
            self.encode()?,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5DlidxPage {
    pub flags: u8,
    pub leaf_pgno: u32,
    pub first_rowid: u64,
    pub rowid_deltas: Vec<Option<u64>>,
}

impl Fts5DlidxPage {
    #[must_use]
    pub fn new(
        flags: u8,
        leaf_pgno: u32,
        first_rowid: u64,
        rowid_deltas: Vec<Option<u64>>,
    ) -> Self {
        Self {
            flags,
            leaf_pgno,
            first_rowid,
            rowid_deltas,
        }
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        (self.flags & 0x01) == 0
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut page = Vec::with_capacity(1 + 18 + self.rowid_deltas.len() * 9);
        page.push(self.flags);
        fts5_push_varint(&mut page, u64::from(self.leaf_pgno));
        fts5_push_varint(&mut page, self.first_rowid);
        for delta in &self.rowid_deltas {
            fts5_push_varint(&mut page, delta.unwrap_or(0));
        }
        page
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.is_empty() {
            return Err(fts5_data_error("dlidx page missing flags"));
        }
        let flags = page[0];
        let mut offset = 1;
        let leaf_pgno = fts5_read_u32(page, &mut offset, "dlidx leaf page")?;
        let first_rowid = fts5_read_varint(page, &mut offset, "dlidx first rowid")?;
        let mut rowid_deltas = Vec::new();
        while offset < page.len() {
            let delta = fts5_read_varint(page, &mut offset, "dlidx rowid delta")?;
            rowid_deltas.push((delta != 0).then_some(delta));
        }

        Ok(Self {
            flags,
            leaf_pgno,
            first_rowid,
            rowid_deltas,
        })
    }

    pub fn to_data_row(&self, segid: u32, height: u8, pgno: u32) -> Result<Fts5DataRow> {
        Ok(Fts5DataRow::new(
            Fts5DataRowid::DoclistIndex {
                segid,
                height,
                pgno,
            }
            .encode()?,
            self.encode(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5TombstonePage {
    pub key_size: u8,
    pub rowid_zero: bool,
    pub slots: Vec<Option<u64>>,
}

impl Fts5TombstonePage {
    #[must_use]
    pub fn new(key_size: u8, rowid_zero: bool, slots: Vec<Option<u64>>) -> Self {
        Self {
            key_size,
            rowid_zero,
            slots,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.key_size != 4 && self.key_size != 8 {
            return Err(fts5_data_error("tombstone key size must be 4 or 8"));
        }
        let key_size = usize::from(self.key_size);
        let mut page = vec![0; 8 + self.slots.len() * key_size];
        page[0] = self.key_size;
        page[1] = u8::from(self.rowid_zero);
        let entry_count = self.slots.iter().filter(|slot| slot.is_some()).count();
        let entry_count = u32::try_from(entry_count)
            .map_err(|_| fts5_data_error("tombstone entry count exceeds u32"))?;
        write_be_u32(&mut page[4..8], entry_count);

        for (index, slot) in self.slots.iter().enumerate() {
            let Some(rowid) = slot else {
                continue;
            };
            let offset = 8 + index * key_size;
            if self.key_size == 4 {
                let rowid = u32::try_from(*rowid)
                    .map_err(|_| fts5_data_error("rowid exceeds 4-byte tombstone slot"))?;
                write_be_u32(&mut page[offset..offset + 4], rowid);
            } else {
                write_be_u64(&mut page[offset..offset + 8], *rowid);
            }
        }

        Ok(page)
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.len() < 8 {
            return Err(fts5_data_error("tombstone page missing header"));
        }
        let key_size = if page[0] == 4 { 4 } else { 8 };
        let key_size_usize = usize::from(key_size);
        let payload_len = page.len() - 8;
        if payload_len % key_size_usize != 0 {
            return Err(fts5_data_error("tombstone slot area is misaligned"));
        }
        let declared_entries = read_be_u32(&page[4..8]);
        let mut slots = Vec::with_capacity(payload_len / key_size_usize);
        let mut actual_entries = 0_u32;
        for slot in page[8..].chunks_exact(key_size_usize) {
            let value = if key_size == 4 {
                u64::from(read_be_u32(slot))
            } else {
                read_be_u64(slot)
            };
            if value == 0 {
                slots.push(None);
            } else {
                actual_entries = actual_entries
                    .checked_add(1)
                    .ok_or_else(|| fts5_data_error("tombstone entry count overflow"))?;
                slots.push(Some(value));
            }
        }
        if actual_entries != declared_entries {
            return Err(fts5_data_error("tombstone entry count mismatch"));
        }

        Ok(Self {
            key_size,
            rowid_zero: page[1] != 0,
            slots,
        })
    }

    #[must_use]
    pub fn contains_rowid(&self, hash_page_count: u32, rowid: u64) -> bool {
        if rowid == 0 {
            return self.rowid_zero;
        }
        if hash_page_count == 0 || self.slots.is_empty() {
            return false;
        }
        let hash_page_count = u64::from(hash_page_count);
        let slot_count = u64::try_from(self.slots.len()).unwrap_or(u64::MAX);
        let mut slot = usize::try_from((rowid / hash_page_count) % slot_count).unwrap_or(0);
        for _ in 0..self.slots.len() {
            match self.slots[slot] {
                Some(value) if value == rowid => return true,
                None => return false,
                _ => slot = (slot + 1) % self.slots.len(),
            }
        }
        false
    }

    pub fn to_data_row(&self, segid: u32, hash_pgno: u32) -> Result<Fts5DataRow> {
        Ok(Fts5DataRow::new(
            Fts5DataRowid::Tombstone { segid, hash_pgno }.encode()?,
            self.encode()?,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5PendingIndex {
    Main,
    Prefix { ordinal: u8, length: usize },
}

impl Fts5PendingIndex {
    #[must_use]
    pub const fn marker(self) -> u8 {
        match self {
            Self::Main => FTS5_MAIN_PREFIX_BYTE,
            Self::Prefix { ordinal, .. } => FTS5_MAIN_PREFIX_BYTE + ordinal + 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fts5PendingTermKey(Vec<u8>);

impl Fts5PendingTermKey {
    #[must_use]
    pub fn new(index: Fts5PendingIndex, term: &str) -> Self {
        let mut key = Vec::with_capacity(term.len() + 1);
        key.push(index.marker());
        key.extend_from_slice(term.as_bytes());
        Self(key)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Fts5PendingDoc {
    delete: bool,
    columns: BTreeMap<u32, Vec<u32>>,
}

impl Fts5PendingDoc {
    fn push_position(&mut self, column: u32, position: u32) {
        self.columns.entry(column).or_default().push(position);
    }

    fn to_poslist(&self) -> Fts5Poslist {
        Fts5Poslist::new(
            self.delete,
            self.columns
                .iter()
                .map(|(column, offsets)| Fts5ColumnPositions::new(*column, offsets.clone()))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5PendingHash {
    terms: BTreeMap<Fts5PendingTermKey, BTreeMap<u64, Fts5PendingDoc>>,
    prefix_lengths: Vec<usize>,
    detail: DetailMode,
    tokendata: bool,
    pending_bytes: usize,
    row_count: usize,
}

impl Fts5PendingHash {
    #[must_use]
    pub fn new(prefix_lengths: &[usize], detail: DetailMode, tokendata: bool) -> Self {
        let mut prefix_lengths = prefix_lengths.to_vec();
        prefix_lengths.sort_unstable();
        prefix_lengths.dedup();
        Self {
            terms: BTreeMap::new(),
            prefix_lengths,
            detail,
            tokendata,
            pending_bytes: 0,
            row_count: 0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    #[must_use]
    pub const fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    #[must_use]
    pub fn should_flush(&self, metadata: &Fts5ConfigMetadata) -> bool {
        usize::try_from(metadata.hash_size).is_ok_and(|limit| self.pending_bytes >= limit)
    }

    pub fn add_document(
        &mut self,
        rowid: i64,
        column_values: &[String],
        indexed_columns: &[bool],
        tokenizer: &dyn Fts5Tokenizer,
    ) -> Result<()> {
        let rowid =
            u64::try_from(rowid).map_err(|_| fts5_data_error("pending rowid is negative"))?;
        let prefix_lengths = self.prefix_lengths.clone();
        let detail = self.detail;
        let tokendata = self.tokendata;
        self.row_count = self.row_count.saturating_add(1);

        for (column, value) in column_values.iter().enumerate() {
            if matches!(indexed_columns.get(column), Some(false)) {
                continue;
            }
            let column = u32::try_from(column)
                .map_err(|_| fts5_data_error("pending column index exceeds u32"))?;
            let stored_column = if detail == DetailMode::None {
                0
            } else {
                column
            };
            let mut position = 0_u32;
            tokenizer.visit_tokens(value, &mut |term, _start, _end, colocated| {
                let stored_position = if detail == DetailMode::Full {
                    position
                } else {
                    0
                };
                let term = if tokendata {
                    tokendata_query_key(term)
                } else {
                    term
                };
                self.push_occurrence(
                    Fts5PendingTermKey::new(Fts5PendingIndex::Main, term),
                    rowid,
                    stored_column,
                    stored_position,
                );
                for (ordinal, prefix_length) in prefix_lengths.iter().copied().enumerate() {
                    if let Some(prefix) = prefix_slice(term, prefix_length)
                        && let Ok(ordinal) = u8::try_from(ordinal)
                    {
                        self.push_occurrence(
                            Fts5PendingTermKey::new(
                                Fts5PendingIndex::Prefix {
                                    ordinal,
                                    length: prefix_length,
                                },
                                prefix,
                            ),
                            rowid,
                            stored_column,
                            stored_position,
                        );
                    }
                }
                if !colocated {
                    position = position.saturating_add(1);
                }
            });
        }

        Ok(())
    }

    fn push_occurrence(&mut self, key: Fts5PendingTermKey, rowid: u64, column: u32, position: u32) {
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(key.as_bytes().len())
            .saturating_add(24);
        self.terms
            .entry(key)
            .or_default()
            .entry(rowid)
            .or_default()
            .push_position(column, position);
    }

    fn push_index_posting(
        &mut self,
        index: Fts5PendingIndex,
        term: &str,
        posting: &Posting,
    ) -> Result<()> {
        let rowid = u64::try_from(posting.docid)
            .map_err(|_| fts5_data_error("pending rowid is negative"))?;
        let key = Fts5PendingTermKey::new(index, term);
        for position in &posting.positions {
            self.push_occurrence(key.clone(), rowid, posting.column, *position);
        }
        Ok(())
    }

    pub fn flush_to_segment(
        &self,
        segid: u32,
        mut structure: Fts5StructureRecord,
    ) -> Result<Fts5PendingFlush> {
        if self.is_empty() {
            return Err(fts5_data_error("cannot flush an empty pending hash"));
        }
        let leaf = Fts5SegmentLeaf::decode(&self.to_segment_leaf().encode()?)?;
        let data_row = leaf.to_data_row(segid, 1)?;
        let segment = if structure.uses_origin_tracking() {
            Fts5StructureSegment::new(segid, 1, 1).with_origin_tracking(
                structure.origin_counter,
                structure.origin_counter,
                0,
                0,
                u64::try_from(self.row_count).unwrap_or(u64::MAX),
            )
        } else {
            Fts5StructureSegment::new(segid, 1, 1)
        };

        if structure.levels.is_empty() {
            structure
                .levels
                .push(Fts5StructureLevel::new(0, vec![segment]));
        } else {
            structure.levels[0].segments.push(segment);
        }
        structure.write_counter = structure.write_counter.saturating_add(1);
        if structure.uses_origin_tracking() {
            structure.origin_counter = structure.origin_counter.saturating_add(1);
        }

        let structure_row = Fts5DataRow::new(FTS5_STRUCTURE_ROWID, structure.encode());
        Ok(Fts5PendingFlush {
            data_rows: vec![data_row, structure_row],
            idx_rows: Vec::new(),
            leaf,
            structure,
            pending_bytes: self.pending_bytes,
        })
    }

    fn to_segment_leaf(&self) -> Fts5SegmentLeaf {
        Fts5SegmentLeaf::new(
            self.terms
                .iter()
                .map(|(term, docs)| {
                    Fts5SegmentTerm::new(
                        term.as_bytes().to_vec(),
                        Fts5Doclist::new(
                            docs.iter()
                                .map(|(rowid, doc)| Fts5DoclistEntry::new(*rowid, doc.to_poslist()))
                                .collect(),
                        ),
                    )
                })
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5PendingFlush {
    pub data_rows: Vec<Fts5DataRow>,
    pub idx_rows: Vec<Fts5IdxRow>,
    pub leaf: Fts5SegmentLeaf,
    pub structure: Fts5StructureRecord,
    pub pending_bytes: usize,
}

impl Fts5PendingFlush {
    pub fn hotspot_profile(&self) -> Result<Fts5ShadowWriteHotspotProfile> {
        Fts5ShadowWriteHotspotProfile::from_rows(&self.data_rows, &self.idx_rows)
    }
}

/// The persist payload for one contentless INSERT statement.
///
/// Encoded as an incremental segment append rather than a full re-encode of the
/// inverted index. See [`Fts5Table::encode_incremental_insert_flush`] (bd-sf8dx).
#[derive(Debug, Clone)]
pub struct Fts5IncrementalInsertFlush {
    /// The new segment's leaf `_data` row, to be appended. `None` when the
    /// inserted rows produced no indexable tokens (e.g. empty/all-unindexed
    /// content): no segment is created, but averages/docsize still advance.
    pub leaf_data_row: Option<Fts5DataRow>,
    /// The updated structure `_data` row (rowid [`FTS5_STRUCTURE_ROWID`]),
    /// replacing the prior structure (now with the appended segment).
    pub structure_data_row: Fts5DataRow,
    /// The updated averages `_data` row (rowid [`FTS5_AVERAGES_ROWID`]).
    pub averages_data_row: Fts5DataRow,
    /// Per-column docsize rows for the newly inserted documents, to append to
    /// the `_docsize` shadow table.
    pub docsize_rows: Vec<Fts5DocsizeRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5ShadowWriteMitigation {
    NoWrites,
    AppendOnlyRowsBeforeSingleMetadataCommit,
    AppendOnlyRowsBeforeBatchedHotWrites,
    RequiresMergeBatching,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fts5ShadowWriteClass {
    SegmentLeaf { segid: u32, pgno: u32 },
    DoclistIndex { segid: u32, height: u8, pgno: u32 },
    Tombstone { segid: u32, hash_pgno: u32 },
    IdxPage { segid: u32, btree_page: u32 },
    AveragesRecord,
    StructureRecord,
}

impl Fts5ShadowWriteClass {
    #[must_use]
    pub const fn is_append_only_data(self) -> bool {
        matches!(
            self,
            Self::SegmentLeaf { .. } | Self::DoclistIndex { .. } | Self::Tombstone { .. }
        )
    }

    #[must_use]
    pub const fn is_hot_write(self) -> bool {
        matches!(
            self,
            Self::IdxPage { .. } | Self::AveragesRecord | Self::StructureRecord
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ShadowWriteHotspotProfile {
    pub data_row_writes: usize,
    pub idx_row_writes: usize,
    pub append_only_data_writes: usize,
    pub segment_leaf_writes: usize,
    pub doclist_index_writes: usize,
    pub tombstone_writes: usize,
    pub averages_record_writes: usize,
    pub structure_record_writes: usize,
    pub hot_write_count: usize,
    pub append_only_rows_precede_hot_rows: bool,
    pub unique_segment_pages: Vec<(u32, u32)>,
    pub unique_doclist_index_pages: Vec<(u32, u8, u32)>,
    pub unique_tombstone_pages: Vec<(u32, u32)>,
    pub unique_idx_pages: Vec<(u32, u32)>,
    pub publication_order: Vec<Fts5ShadowWriteClass>,
    pub mitigation: Fts5ShadowWriteMitigation,
}

impl Fts5ShadowWriteHotspotProfile {
    pub fn from_rows(data_rows: &[Fts5DataRow], idx_rows: &[Fts5IdxRow]) -> Result<Self> {
        let mut segment_pages = BTreeSet::new();
        let mut doclist_index_pages = BTreeSet::new();
        let mut tombstone_pages = BTreeSet::new();
        let mut idx_pages = BTreeSet::new();
        let mut publication_order = Vec::with_capacity(data_rows.len() + idx_rows.len());

        let mut segment_leaf_writes = 0;
        let mut doclist_index_writes = 0;
        let mut tombstone_writes = 0;
        let mut averages_record_writes = 0;
        let mut structure_record_writes = 0;

        for row in data_rows {
            let write_class = match Fts5DataRowid::decode(row.id)? {
                Fts5DataRowid::Averages => {
                    averages_record_writes += 1;
                    Fts5ShadowWriteClass::AveragesRecord
                }
                Fts5DataRowid::Structure => {
                    structure_record_writes += 1;
                    Fts5ShadowWriteClass::StructureRecord
                }
                Fts5DataRowid::SegmentLeaf { segid, pgno } => {
                    segment_leaf_writes += 1;
                    segment_pages.insert((segid, pgno));
                    Fts5ShadowWriteClass::SegmentLeaf { segid, pgno }
                }
                Fts5DataRowid::DoclistIndex {
                    segid,
                    height,
                    pgno,
                } => {
                    doclist_index_writes += 1;
                    doclist_index_pages.insert((segid, height, pgno));
                    Fts5ShadowWriteClass::DoclistIndex {
                        segid,
                        height,
                        pgno,
                    }
                }
                Fts5DataRowid::Tombstone { segid, hash_pgno } => {
                    tombstone_writes += 1;
                    tombstone_pages.insert((segid, hash_pgno));
                    Fts5ShadowWriteClass::Tombstone { segid, hash_pgno }
                }
            };
            publication_order.push(write_class);
        }

        for row in idx_rows {
            idx_pages.insert((row.segid, row.btree_page));
            publication_order.push(Fts5ShadowWriteClass::IdxPage {
                segid: row.segid,
                btree_page: row.btree_page,
            });
        }

        let idx_row_writes = idx_rows.len();
        let append_only_data_writes = segment_leaf_writes + doclist_index_writes + tombstone_writes;
        let metadata_writes = averages_record_writes + structure_record_writes;
        let hot_write_count = metadata_writes + idx_row_writes;
        let append_only_rows_precede_hot_rows =
            Self::append_only_rows_precede_hot_rows(&publication_order);
        let mitigation = Self::mitigation(
            data_rows.len() + idx_row_writes,
            hot_write_count,
            append_only_rows_precede_hot_rows,
            metadata_writes,
        );

        Ok(Self {
            data_row_writes: data_rows.len(),
            idx_row_writes,
            append_only_data_writes,
            segment_leaf_writes,
            doclist_index_writes,
            tombstone_writes,
            averages_record_writes,
            structure_record_writes,
            hot_write_count,
            append_only_rows_precede_hot_rows,
            unique_segment_pages: segment_pages.into_iter().collect(),
            unique_doclist_index_pages: doclist_index_pages.into_iter().collect(),
            unique_tombstone_pages: tombstone_pages.into_iter().collect(),
            unique_idx_pages: idx_pages.into_iter().collect(),
            publication_order,
            mitigation,
        })
    }

    fn append_only_rows_precede_hot_rows(publication_order: &[Fts5ShadowWriteClass]) -> bool {
        let mut seen_hot_write = false;
        for write_class in publication_order {
            if write_class.is_hot_write() {
                seen_hot_write = true;
            } else if seen_hot_write && write_class.is_append_only_data() {
                return false;
            }
        }
        true
    }

    const fn mitigation(
        write_count: usize,
        hot_write_count: usize,
        append_only_rows_precede_hot_rows: bool,
        metadata_writes: usize,
    ) -> Fts5ShadowWriteMitigation {
        if write_count == 0 {
            return Fts5ShadowWriteMitigation::NoWrites;
        }
        if append_only_rows_precede_hot_rows && hot_write_count <= 1 && metadata_writes <= 1 {
            return Fts5ShadowWriteMitigation::AppendOnlyRowsBeforeSingleMetadataCommit;
        }
        if append_only_rows_precede_hot_rows && metadata_writes <= 2 {
            return Fts5ShadowWriteMitigation::AppendOnlyRowsBeforeBatchedHotWrites;
        }
        Fts5ShadowWriteMitigation::RequiresMergeBatching
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5MergeKind {
    Auto,
    Crisis,
    Optimize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fts5MergePlan {
    pub kind: Fts5MergeKind,
    pub level: usize,
    pub segment_count: usize,
    pub merge_inputs: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fts5MergeScheduler {
    pub automerge: usize,
    pub usermerge: usize,
    pub crisismerge: usize,
}

impl Fts5MergeScheduler {
    #[must_use]
    pub fn from_metadata(metadata: &Fts5ConfigMetadata) -> Self {
        Self {
            automerge: usize::try_from(metadata.automerge.max(0)).unwrap_or(0),
            usermerge: usize::try_from(metadata.usermerge.max(0)).unwrap_or(0),
            crisismerge: usize::try_from(metadata.crisismerge.max(0)).unwrap_or(0),
        }
    }

    #[must_use]
    pub fn next_plan(&self, structure: &Fts5StructureRecord) -> Option<Fts5MergePlan> {
        for (level, level_record) in structure.levels.iter().enumerate() {
            let segment_count = level_record.segments.len();
            if self.crisismerge > 0 && segment_count >= self.crisismerge {
                return Some(Fts5MergePlan {
                    kind: Fts5MergeKind::Crisis,
                    level,
                    segment_count,
                    merge_inputs: u32::try_from(segment_count).unwrap_or(u32::MAX),
                });
            }
        }

        for (level, level_record) in structure.levels.iter().enumerate() {
            let segment_count = level_record.segments.len();
            if self.automerge > 0 && segment_count >= self.automerge {
                return Some(Fts5MergePlan {
                    kind: Fts5MergeKind::Auto,
                    level,
                    segment_count,
                    merge_inputs: u32::try_from(self.usermerge.min(segment_count))
                        .unwrap_or(u32::MAX),
                });
            }
        }

        None
    }

    #[must_use]
    pub fn optimize_plan(&self, structure: &Fts5StructureRecord) -> Option<Fts5MergePlan> {
        structure
            .levels
            .iter()
            .enumerate()
            .max_by_key(|(_level, level_record)| level_record.segments.len())
            .and_then(|(level, level_record)| {
                let segment_count = level_record.segments.len();
                (segment_count > 1).then(|| Fts5MergePlan {
                    kind: Fts5MergeKind::Optimize,
                    level,
                    segment_count,
                    merge_inputs: u32::try_from(segment_count).unwrap_or(u32::MAX),
                })
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5LazyPostings {
    pub entries: Vec<Fts5DoclistEntry>,
}

impl Fts5LazyPostings {
    #[must_use]
    pub fn new(entries: Vec<Fts5DoclistEntry>) -> Self {
        Self { entries }
    }

    #[must_use]
    pub fn rowids(&self) -> Vec<u64> {
        self.entries.iter().map(|entry| entry.rowid).collect()
    }

    #[must_use]
    pub fn union_rowids(&self, other: &Self) -> Vec<u64> {
        self.entries
            .iter()
            .chain(&other.entries)
            .map(|entry| entry.rowid)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[must_use]
    pub fn intersect_rowids(&self, other: &Self) -> Vec<u64> {
        let right: BTreeSet<_> = other.entries.iter().map(|entry| entry.rowid).collect();
        self.entries
            .iter()
            .map(|entry| entry.rowid)
            .filter(|rowid| right.contains(rowid))
            .collect()
    }

    #[must_use]
    pub fn phrase_rowids(&self, next: &Self) -> Vec<u64> {
        self.rowids_with_position_match(next, |left, right| right == left.saturating_add(1))
    }

    #[must_use]
    pub fn near_rowids(&self, other: &Self, distance: u32) -> Vec<u64> {
        self.rowids_with_position_match(other, |left, right| left.abs_diff(right) <= distance)
    }

    fn rowids_with_position_match(
        &self,
        other: &Self,
        matches_positions: impl Fn(u32, u32) -> bool,
    ) -> Vec<u64> {
        let mut right_by_rowid: BTreeMap<u64, &Fts5DoclistEntry> = BTreeMap::new();
        for entry in &other.entries {
            right_by_rowid.insert(entry.rowid, entry);
        }

        let mut rowids = Vec::new();
        for left in &self.entries {
            let Some(right) = right_by_rowid.get(&left.rowid) else {
                continue;
            };
            if postings_have_position_match(left, right, &matches_positions) {
                rowids.push(left.rowid);
            }
        }
        rowids
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5LazyTermMatch {
    pub term: Vec<u8>,
    pub postings: Fts5LazyPostings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5SegmentIntegrityReport {
    pub segment_count: usize,
    pub leaf_page_count: usize,
    pub idx_row_count: usize,
    pub term_count: usize,
    pub checksum: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Fts5SegmentRowSet<'a> {
    data_rows: &'a [Fts5DataRow],
    idx_rows: &'a [Fts5IdxRow],
}

impl<'a> Fts5SegmentRowSet<'a> {
    #[must_use]
    pub const fn new(data_rows: &'a [Fts5DataRow], idx_rows: &'a [Fts5IdxRow]) -> Self {
        Self {
            data_rows,
            idx_rows,
        }
    }

    #[must_use]
    pub fn reader(&self, segment: &'a Fts5StructureSegment) -> Fts5SegmentReader<'a> {
        Fts5SegmentReader {
            segment,
            data_rows: self.data_rows,
            idx_rows: self.idx_rows,
        }
    }

    pub fn integrity_report(
        &self,
        structure: &Fts5StructureRecord,
    ) -> Result<Fts5SegmentIntegrityReport> {
        let mut seen_segids = BTreeSet::new();
        let mut known_segids = BTreeSet::new();
        let mut leaf_page_count = 0;
        let mut term_count = 0;
        let mut checksum = FTS5_SEGMENT_CHECKSUM_OFFSET;

        for level in &structure.levels {
            for segment in &level.segments {
                if !seen_segids.insert(segment.segid) {
                    return Err(fts5_data_error("duplicate segment id in structure"));
                }
                known_segids.insert(segment.segid);
                let reader = self.reader(segment);
                for pgno in segment.pgno_first..=segment.pgno_last {
                    let row = reader
                        .data_row(pgno)
                        .ok_or_else(|| fts5_data_error("missing segment leaf page"))?;
                    checksum = fts5_checksum_i64(checksum, row.id);
                    checksum = fts5_checksum_bytes(checksum, &row.block);
                    let leaf = Fts5SegmentLeaf::decode(&row.block)?;
                    term_count += leaf.terms.len();
                    leaf_page_count += 1;
                }
            }
        }

        let mut idx_row_count = 0;
        for row in self.idx_rows {
            if !known_segids.contains(&row.segid) {
                return Err(fts5_data_error("%_idx row references unknown segment"));
            }
            checksum = fts5_checksum_u32(checksum, row.segid);
            checksum = fts5_checksum_bytes(checksum, &row.term);
            checksum = fts5_checksum_u32(checksum, row.btree_page);
            checksum = fts5_checksum_u8(checksum, u8::from(row.has_doclist_index));
            idx_row_count += 1;
        }

        for row in self.data_rows {
            match Fts5DataRowid::decode(row.id)? {
                Fts5DataRowid::SegmentLeaf { segid, .. }
                | Fts5DataRowid::DoclistIndex { segid, .. }
                | Fts5DataRowid::Tombstone { segid, .. } => {
                    if !known_segids.contains(&segid) {
                        return Err(fts5_data_error("%_data row references unknown segment"));
                    }
                }
                Fts5DataRowid::Averages | Fts5DataRowid::Structure => {}
            }
        }

        Ok(Fts5SegmentIntegrityReport {
            segment_count: seen_segids.len(),
            leaf_page_count,
            idx_row_count,
            term_count,
            checksum,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Fts5SegmentReader<'a> {
    segment: &'a Fts5StructureSegment,
    data_rows: &'a [Fts5DataRow],
    idx_rows: &'a [Fts5IdxRow],
}

impl<'a> Fts5SegmentReader<'a> {
    #[must_use]
    pub const fn segment(&self) -> &'a Fts5StructureSegment {
        self.segment
    }

    pub fn leaf(&self, pgno: u32) -> Result<Option<Fts5SegmentLeaf>> {
        if pgno < self.segment.pgno_first || pgno > self.segment.pgno_last {
            return Err(fts5_data_error("segment leaf page outside structure range"));
        }
        self.data_row(pgno)
            .map(|row| Fts5SegmentLeaf::decode(&row.block))
            .transpose()
    }

    #[must_use]
    pub fn term_cursor(&self) -> Fts5SegmentTermCursor<'a> {
        self.term_cursor_from(self.segment.pgno_first)
    }

    #[must_use]
    pub fn term_cursor_from(&self, pgno: u32) -> Fts5SegmentTermCursor<'a> {
        Fts5SegmentTermCursor {
            reader: *self,
            next_pgno: pgno,
            current_terms: Vec::new().into_iter(),
        }
    }

    pub fn exact_postings(&self, term: &[u8]) -> Result<Option<Fts5LazyPostings>> {
        let mut cursor = self.term_cursor_from(self.candidate_page(term));
        for entry in &mut cursor {
            let entry = entry?;
            match entry.term.as_slice().cmp(term) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(Fts5LazyPostings::new(entry.doclist.entries)));
                }
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    pub fn prefix_matches(&self, prefix: &[u8]) -> Result<Vec<Fts5LazyTermMatch>> {
        let mut matches = Vec::new();
        let mut cursor = self.term_cursor_from(self.candidate_page(prefix));
        for entry in &mut cursor {
            let entry = entry?;
            if entry.term.starts_with(prefix) {
                matches.push(Fts5LazyTermMatch {
                    term: entry.term,
                    postings: Fts5LazyPostings::new(entry.doclist.entries),
                });
                continue;
            }
            if !matches.is_empty() || entry.term.as_slice() > prefix {
                break;
            }
        }
        Ok(matches)
    }

    fn candidate_page(&self, term: &[u8]) -> u32 {
        self.idx_rows
            .iter()
            .filter(|row| row.segid == self.segment.segid && row.term.as_slice() <= term)
            .max_by(|left, right| left.term.cmp(&right.term))
            .map_or(self.segment.pgno_first, |row| {
                row.btree_page
                    .clamp(self.segment.pgno_first, self.segment.pgno_last)
            })
    }

    fn data_row(&self, pgno: u32) -> Option<&'a Fts5DataRow> {
        self.data_rows.iter().find(|row| {
            matches!(
                Fts5DataRowid::decode(row.id),
                Ok(Fts5DataRowid::SegmentLeaf { segid, pgno: row_pgno })
                    if segid == self.segment.segid && row_pgno == pgno
            )
        })
    }
}

pub struct Fts5SegmentTermCursor<'a> {
    reader: Fts5SegmentReader<'a>,
    next_pgno: u32,
    current_terms: std::vec::IntoIter<Fts5SegmentTerm>,
}

impl Iterator for Fts5SegmentTermCursor<'_> {
    type Item = Result<Fts5SegmentTerm>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(term) = self.current_terms.next() {
                return Some(Ok(term));
            }
            if self.next_pgno > self.reader.segment.pgno_last {
                return None;
            }

            match self.reader.leaf(self.next_pgno) {
                Ok(Some(leaf)) => {
                    self.next_pgno = self.next_pgno.saturating_add(1);
                    self.current_terms = leaf.terms.into_iter();
                }
                Ok(None) => {
                    return Some(Err(fts5_data_error("missing segment leaf page")));
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ShadowRows {
    pub data: Vec<Fts5DataRow>,
    pub idx: Vec<Fts5IdxRow>,
    pub config: Vec<Fts5ConfigRecord>,
    pub content: Vec<Fts5ContentRow>,
    pub docsize: Vec<Fts5DocsizeRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ShadowOpenReport {
    pub metadata: Fts5ConfigMetadata,
    pub averages: Option<Fts5AveragesRecord>,
    pub structure: Option<Fts5StructureRecord>,
    pub integrity: Option<Fts5SegmentIntegrityReport>,
    pub content_row_count: usize,
    pub docsize_row_count: usize,
    pub max_seen_rowid: Option<i64>,
    pub bound_without_rebuild: bool,
}

#[derive(Debug, Clone)]
pub struct Fts5ShadowOpen {
    pub table: Fts5Table,
    pub report: Fts5ShadowOpenReport,
}

#[derive(Debug, Clone)]
enum Fts5ScoreSource {
    InMemory(InvertedIndex),
    Shadow(Fts5ShadowRows),
}

#[derive(Debug, Clone)]
pub struct Fts5ScoreSnapshot {
    columns: Vec<String>,
    tokenizer_name: String,
    detail: DetailMode,
    source: Fts5ScoreSource,
}

impl Fts5ScoreSnapshot {
    pub fn query_terms_for_queries(
        &self,
        queries: &[&str],
    ) -> std::result::Result<Vec<String>, Fts5QueryError> {
        let tokenizer = create_tokenizer(&self.tokenizer_name)
            .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));
        query_terms_for_query_strings(&self.columns, queries, tokenizer.as_ref(), self.detail)
    }

    pub fn bm25_score_for_terms_with_weights(
        &self,
        rowid: i64,
        query_terms: &[String],
        weights: &[f64],
    ) -> std::result::Result<f64, Fts5QueryError> {
        match &self.source {
            Fts5ScoreSource::InMemory(index) => Ok(bm25_score(index, rowid, query_terms, weights)),
            Fts5ScoreSource::Shadow(rows) => {
                let tokenizer = create_tokenizer(&self.tokenizer_name)
                    .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));
                Fts5ShadowQuery::new(rows, &self.columns, tokenizer.as_ref(), self.detail)?
                    .bm25_score(rowid, query_terms, weights)
            }
        }
    }
}

impl Fts5ShadowRows {
    #[must_use]
    pub fn full_scan_rows(&self) -> Vec<(i64, Vec<String>)> {
        let mut rows = self
            .content
            .iter()
            .map(|row| (row.rowid, row.values.clone()))
            .collect::<BTreeMap<_, _>>();

        for row in &self.docsize {
            rows.entry(row.rowid).or_default();
        }

        rows.into_iter().collect()
    }

    #[must_use]
    pub fn row_count(&self, column_count: usize) -> usize {
        if let Some(row) = self.data.iter().find(|row| row.id == FTS5_AVERAGES_ROWID)
            && let Ok(averages) = Fts5AveragesRecord::decode(&row.block, column_count)
        {
            return usize::try_from(averages.total_rows).unwrap_or(usize::MAX);
        }

        self.content
            .iter()
            .map(|row| row.rowid)
            .chain(self.docsize.iter().map(|row| row.rowid))
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn validate_for_open(&self, column_count: usize) -> Result<Fts5ShadowOpenReport> {
        let metadata = Fts5ConfigMetadata::decode_rows(&self.config)?;
        let data = Fts5DataMetadata::decode_rows(&self.data, column_count)?;

        for row in &self.content {
            if row.values.len() != column_count {
                return Err(fts5_data_error("content row column count mismatch"));
            }
        }
        for row in &self.docsize {
            if row.column_token_counts.len() != column_count {
                return Err(fts5_data_error("docsize row column count mismatch"));
            }
        }

        let integrity = if let Some(structure) = data.structure.as_ref()
            && structure.segment_count() > 0
        {
            Some(Fts5SegmentRowSet::new(&self.data, &self.idx).integrity_report(structure)?)
        } else {
            None
        };

        let max_seen_rowid = self
            .content
            .iter()
            .map(|row| row.rowid)
            .chain(self.docsize.iter().map(|row| row.rowid))
            .max();

        Ok(Fts5ShadowOpenReport {
            metadata,
            averages: data.averages,
            structure: data.structure,
            integrity,
            content_row_count: self.content.len(),
            docsize_row_count: self.docsize.len(),
            max_seen_rowid,
            bound_without_rebuild: true,
        })
    }

    /// Build [`Fts5ShadowRows`] from raw storage rows read out of the live
    /// FTS5 shadow tables, so a reopened on-disk index can be answered from
    /// its PERSISTED posting lists instead of re-tokenizing `_content`.
    ///
    /// Inputs are `(rowid, columns)` exactly as produced by the core b-tree
    /// table reader for the rowid-backed shadow tables:
    /// - `data`    — `<tbl>_data(id INTEGER PRIMARY KEY, block BLOB)`
    /// - `docsize` — `<tbl>_docsize(id INTEGER PRIMARY KEY, sz BLOB)`
    /// - `content` — `<tbl>_content(id, c0, c1, …)`
    ///
    /// `_idx` and `_config` are `WITHOUT ROWID` tables that the table reader
    /// cannot walk; they are reconstructed locally instead. `_idx` only
    /// accelerates per-term page lookup (its absence falls back to scanning a
    /// segment from `pgno_first`, which is still correct), and `_config` is
    /// synthesized with the current on-disk format version so
    /// [`Fts5ShadowRows::validate_for_open`] accepts the binding; the runtime
    /// `Fts5Config` already carries the declared options from `connect`.
    ///
    /// For `INTEGER PRIMARY KEY` shadow tables the stored record may carry a
    /// leading rowid-alias placeholder, so BLOB payloads are taken from the
    /// trailing columns.
    #[must_use]
    pub fn from_storage_rows(
        data: &[(i64, Vec<SqliteValue>)],
        docsize: &[(i64, Vec<SqliteValue>)],
        content: &[(i64, Vec<SqliteValue>)],
        column_count: usize,
    ) -> Self {
        let data_rows = data
            .iter()
            .map(|(id, cols)| {
                let block = cols
                    .iter()
                    .rev()
                    .find_map(SqliteValue::as_blob_bytes)
                    .map_or_else(Vec::new, <[u8]>::to_vec);
                Fts5DataRow::new(*id, block)
            })
            .collect::<Vec<_>>();

        let docsize_rows = docsize
            .iter()
            .map(|(id, cols)| {
                let blob = cols
                    .iter()
                    .rev()
                    .find_map(SqliteValue::as_blob_bytes)
                    .unwrap_or(&[]);
                Fts5DocsizeRow::new(*id, decode_docsize_blob(blob, column_count))
            })
            .collect::<Vec<_>>();

        let content_rows = content
            .iter()
            .map(|(rowid, cols)| {
                let start = cols.len().saturating_sub(column_count);
                let values = cols[start..].iter().map(SqliteValue::to_text).collect();
                Fts5ContentRow::new(*rowid, values)
            })
            .collect::<Vec<_>>();

        Self {
            data: data_rows,
            idx: Vec::new(),
            config: vec![Fts5ConfigRecord::integer("version", FTS5_CONFIG_VERSION)],
            content: content_rows,
            docsize: docsize_rows,
        }
    }
}

/// Lazy on-disk access to a persisted FTS5 table's shadow b-trees, so a
/// reopened index can be answered by reading ONLY what a query needs instead
/// of rebuilding the whole in-memory index.
///
/// Implemented by the host (fsqlite-core) over a live read transaction and
/// consumed by the lazy query path. Keys are the shadow tables' own keys:
/// - `_data`: id 1 = averages, 10 = structure, else `fts5_data_rowid(...)`.
/// - `_docsize` / `_content`: the document rowid.
///
/// All methods take `&mut self` because the host implementation drives a
/// b-tree cursor; they MUST read from the same transaction snapshot as the
/// owning query so results are MVCC-consistent.
pub trait Fts5OnDiskReader {
    /// Raw `_data.block` bytes for an encoded id, or `None` if the row is absent.
    fn read_data_block(&mut self, id: i64) -> Result<Option<Vec<u8>>>;

    /// The `_idx` leaf page to begin scanning for `term` within segment
    /// `segid`: the page of the largest indexed term `<= term`. `None` means
    /// "no indexed term `<= term` in this segment" — the caller starts at the
    /// segment's first page (still correct, just scans from the start).
    fn idx_candidate_page(&mut self, segid: u32, term: &[u8]) -> Result<Option<u32>>;

    /// The `_docsize` row (per-column token counts) for `rowid`, or `None`.
    fn read_docsize(&mut self, rowid: i64, column_count: usize) -> Result<Option<Fts5DocsizeRow>>;

    /// The `_content` row (column text values) for `rowid`, or `None`.
    fn read_content(&mut self, rowid: i64, column_count: usize) -> Result<Option<Vec<String>>>;
}

/// In-memory [`Fts5OnDiskReader`] over the decoded shadow rows currently bound
/// to a reopened table. It lets the recency-correct lazy enumeration
/// ([`collect_doclist_recency`]) run directly over a [`Fts5ShadowRows`] without
/// a pager, which is how a reopened CONTENTLESS table hydrates its in-memory
/// index from persisted segments before the first mutation. Only the two
/// segment-reading primitives are meaningful here; `_docsize`/`_content`
/// point-reads are unused by the term enumeration (a contentless table keeps no
/// `_content` rows anyway).
struct Fts5ShadowRowReader<'a> {
    data: &'a [Fts5DataRow],
    idx: &'a [Fts5IdxRow],
}

impl Fts5OnDiskReader for Fts5ShadowRowReader<'_> {
    fn read_data_block(&mut self, id: i64) -> Result<Option<Vec<u8>>> {
        Ok(self
            .data
            .iter()
            .find(|row| row.id == id)
            .map(|row| row.block.clone()))
    }

    fn idx_candidate_page(&mut self, segid: u32, term: &[u8]) -> Result<Option<u32>> {
        Ok(self
            .idx
            .iter()
            .filter(|row| row.segid == segid && row.term.as_slice() <= term)
            .max_by(|left, right| left.term.cmp(&right.term))
            .map(|row| row.btree_page))
    }

    fn read_docsize(
        &mut self,
        _rowid: i64,
        _column_count: usize,
    ) -> Result<Option<Fts5DocsizeRow>> {
        Ok(None)
    }

    fn read_content(&mut self, _rowid: i64, _column_count: usize) -> Result<Option<Vec<String>>> {
        Ok(None)
    }
}

/// Lazily fetch the doclist for an exact `term` within one on-disk segment,
/// reading only the leaf pages from the term's candidate page onward. Mirrors
/// the leaf scan in [`Fts5SegmentReader::exact_postings`] but sources leaf
/// blocks through an [`Fts5OnDiskReader`] instead of an in-memory slice, so a
/// reopened index is queried by reading just the pages a term touches.
fn lazy_segment_exact_postings(
    reader: &mut dyn Fts5OnDiskReader,
    segment: &Fts5StructureSegment,
    term: &[u8],
) -> Result<Vec<Fts5DoclistEntry>> {
    let start = reader
        .idx_candidate_page(segment.segid, term)?
        .unwrap_or(segment.pgno_first)
        .clamp(segment.pgno_first, segment.pgno_last);
    let mut pgno = start;
    while pgno <= segment.pgno_last {
        let id = fts5_data_rowid(segment.segid, false, 0, pgno, "segment leaf")?;
        let block = reader
            .read_data_block(id)?
            .ok_or_else(|| fts5_data_error("missing segment leaf page"))?;
        let leaf = Fts5SegmentLeaf::decode(&block)?;
        for seg_term in leaf.terms {
            match seg_term.term.as_slice().cmp(term) {
                std::cmp::Ordering::Equal => return Ok(seg_term.doclist.entries),
                std::cmp::Ordering::Greater => return Ok(Vec::new()),
                std::cmp::Ordering::Less => {}
            }
        }
        pgno = pgno.saturating_add(1);
    }
    Ok(Vec::new())
}

/// Return the doclist for an exact `term` within one on-disk segment as a
/// single `(stored_term, entries)` group, trying both the main-index marker key
/// and the raw token (a given segment uses one encoding, so the first non-empty
/// lookup is authoritative). Empty when the term is absent from the segment.
fn lazy_segment_exact_group(
    reader: &mut dyn Fts5OnDiskReader,
    segment: &Fts5StructureSegment,
    term: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<Fts5DoclistEntry>)>> {
    for key in segment_lookup_keys(term) {
        let entries = lazy_segment_exact_postings(reader, segment, &key)?;
        if !entries.is_empty() {
            return Ok(vec![(key, entries)]);
        }
    }
    Ok(Vec::new())
}

/// True if `rowid` is recorded in `segment`'s tombstone hash — a deleted doc
/// whose doclist entries still physically live in the segment. Reads only the
/// single hash page the rowid maps to (`rowid % tombstone_page_count`). A
/// segment with no tombstone pages can never tombstone a rowid.
fn rowid_tombstoned_in_segment(
    reader: &mut dyn Fts5OnDiskReader,
    segment: &Fts5StructureSegment,
    rowid: u64,
) -> Result<bool> {
    let count = segment.tombstone_page_count;
    if count == 0 {
        return Ok(false);
    }
    let hash_pgno = u32::try_from(rowid % u64::from(count)).unwrap_or(0);
    let id = Fts5DataRowid::Tombstone {
        segid: segment.segid,
        hash_pgno,
    }
    .encode()?;
    let Some(block) = reader.read_data_block(id)? else {
        return Ok(false);
    };
    Ok(Fts5TombstonePage::decode(&block)?.contains_rowid(count, rowid))
}

/// Merge per-segment `(stored_term, entries)` groups across `structure` in FTS5
/// recency order.
///
/// Newest segment first, keeping the FIRST (newest) occurrence of each
/// `(term, rowid)` and dropping it when that occurrence is a delete marker or
/// the rowid is tombstoned in its segment. This is the on-disk counterpart of
/// FTS5's multi-segment doclist merge (`fts5MultiIterNew`): levels iterate
/// ascending (level 0 newest) and segments within a level iterate in reverse
/// (the structure stores them oldest→newest).
///
/// Recency is keyed by `(term, rowid)`, NOT by `rowid` alone, so an UPDATE that
/// deletes term `t` for a row while inserting a different term `t2` for the same
/// row does not let `t`'s delete marker suppress the live `t2` entry — essential
/// for correct prefix queries that span several terms of one document.
fn collect_doclist_recency<F>(
    reader: &mut dyn Fts5OnDiskReader,
    structure: &Fts5StructureRecord,
    mut per_segment: F,
) -> Result<Vec<Fts5DoclistEntry>>
where
    F: FnMut(
        &mut dyn Fts5OnDiskReader,
        &Fts5StructureSegment,
    ) -> Result<Vec<(Vec<u8>, Vec<Fts5DoclistEntry>)>>,
{
    let mut out = Vec::new();
    let mut seen: HashSet<(Vec<u8>, u64)> = HashSet::new();
    for level in &structure.levels {
        for segment in level.segments.iter().rev() {
            for (term, entries) in per_segment(reader, segment)? {
                for entry in entries {
                    if !seen.insert((term.clone(), entry.rowid)) {
                        // An older segment's occurrence of this (term, rowid):
                        // shadowed by the newer one already recorded.
                        continue;
                    }
                    if entry.poslist.delete {
                        // Delete marker is the newest occurrence: rowid is dead
                        // for this term (and shadows older live occurrences).
                        continue;
                    }
                    if rowid_tombstoned_in_segment(reader, segment, entry.rowid)? {
                        continue;
                    }
                    out.push(entry);
                }
            }
        }
    }
    Ok(out)
}

/// Collect every LIVE posting across `structure`'s segments, grouped by stored
/// term, applying the same multi-segment recency + tombstone semantics as
/// [`collect_doclist_recency`] but PRESERVING the per-term identity (which the
/// flat merge discards). Each returned term still carries its stored index
/// marker byte (`FTS5_MAIN_PREFIX_BYTE ++ token` for main-index terms).
///
/// Used to hydrate a reopened contentless table's in-memory inverted index from
/// its persisted segments: an empty prefix enumerates all main-index terms,
/// recency keeps the newest `(term, rowid)` occurrence, and deletes/tombstones
/// are dropped so the rebuilt index matches what a live query would see.
fn collect_live_term_groups(
    reader: &mut dyn Fts5OnDiskReader,
    structure: &Fts5StructureRecord,
) -> Result<Vec<(Vec<u8>, Vec<Fts5DoclistEntry>)>> {
    let mut by_term: BTreeMap<Vec<u8>, Vec<Fts5DoclistEntry>> = BTreeMap::new();
    let mut seen: HashSet<(Vec<u8>, u64)> = HashSet::new();
    for level in &structure.levels {
        for segment in level.segments.iter().rev() {
            for (term, entries) in lazy_segment_prefix_groups(reader, segment, &[])? {
                for entry in entries {
                    if !seen.insert((term.clone(), entry.rowid)) {
                        // Older segment's (term, rowid): shadowed by the newer one.
                        continue;
                    }
                    if entry.poslist.delete {
                        // Newest occurrence is a delete marker: rowid is dead.
                        continue;
                    }
                    if rowid_tombstoned_in_segment(reader, segment, entry.rowid)? {
                        continue;
                    }
                    by_term.entry(term.clone()).or_default().push(entry);
                }
            }
        }
    }
    Ok(by_term.into_iter().collect())
}

/// Lazily collect the LIVE doclist entries for an exact `term`.
///
/// Reads leaf pages on demand via `reader` across every on-disk segment of
/// `structure`, applying FTS5 multi-segment recency + tombstone semantics (a
/// deleted or updated-away document is correctly excluded even though its
/// original posting still physically exists in an older segment). Parity
/// counterpart of `Fts5ShadowQuery::exact_entries` for the lazy (reopened) path.
pub fn lazy_exact_doclist_entries(
    reader: &mut dyn Fts5OnDiskReader,
    structure: &Fts5StructureRecord,
    term: &[u8],
) -> Result<Vec<Fts5DoclistEntry>> {
    collect_doclist_recency(reader, structure, |reader, segment| {
        lazy_segment_exact_group(reader, segment, term)
    })
}

/// Collect the matching terms within one on-disk segment whose stored term
/// begins with `prefix` (exact byte key, index marker already applied), each as
/// a `(stored_term, entries)` group preserving the term identity so the recency
/// merge can key on `(term, rowid)`. Reads only the leaf pages from the prefix's
/// candidate page onward; mirrors [`Fts5SegmentReader::prefix_matches`].
fn lazy_segment_prefix_term_groups(
    reader: &mut dyn Fts5OnDiskReader,
    segment: &Fts5StructureSegment,
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<Fts5DoclistEntry>)>> {
    let start = reader
        .idx_candidate_page(segment.segid, prefix)?
        .unwrap_or(segment.pgno_first)
        .clamp(segment.pgno_first, segment.pgno_last);
    let mut groups = Vec::new();
    let mut pgno = start;
    while pgno <= segment.pgno_last {
        let id = fts5_data_rowid(segment.segid, false, 0, pgno, "segment leaf")?;
        let block = reader
            .read_data_block(id)?
            .ok_or_else(|| fts5_data_error("missing segment leaf page"))?;
        let leaf = Fts5SegmentLeaf::decode(&block)?;
        for seg_term in leaf.terms {
            if seg_term.term.starts_with(prefix) {
                groups.push((seg_term.term, seg_term.doclist.entries));
                continue;
            }
            // Segment terms are sorted: once we have collected a match and moved
            // past the prefix run, or we have scanned past where the prefix could
            // first appear, no later term in this segment can match.
            if !groups.is_empty() || seg_term.term.as_slice() > prefix {
                return Ok(groups);
            }
        }
        pgno = pgno.saturating_add(1);
    }
    Ok(groups)
}

/// Per-segment prefix lookup over both the marked and raw lookup keys (a segment
/// uses one encoding, so the first non-empty result is authoritative).
fn lazy_segment_prefix_groups(
    reader: &mut dyn Fts5OnDiskReader,
    segment: &Fts5StructureSegment,
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<Fts5DoclistEntry>)>> {
    for key in segment_lookup_keys(prefix) {
        let groups = lazy_segment_prefix_term_groups(reader, segment, &key)?;
        if !groups.is_empty() {
            return Ok(groups);
        }
    }
    Ok(Vec::new())
}

/// Lazily collect the LIVE doclist entries for every term beginning with
/// `prefix`.
///
/// Reads leaf pages on demand via `reader` across every on-disk segment of
/// `structure`, applying FTS5 multi-segment recency + tombstone semantics per
/// `(term, rowid)`.
///
/// Parity counterpart of `Fts5ShadowQuery::prefix_entries` for the lazy
/// (reopened) path; the dual main/raw lookup keys make it correct against both
/// stock-SQLite segments (terms stored as `FTS5_MAIN_PREFIX_BYTE ++ token`) and
/// frankensqlite-encoded segments.
pub fn lazy_prefix_doclist_entries(
    reader: &mut dyn Fts5OnDiskReader,
    structure: &Fts5StructureRecord,
    prefix: &[u8],
) -> Result<Vec<Fts5DoclistEntry>> {
    collect_doclist_recency(reader, structure, |reader, segment| {
        lazy_segment_prefix_groups(reader, segment, prefix)
    })
}

/// Decode an FTS5 `_docsize.sz` blob into per-column token counts. The blob is
/// a sequence of varints (one per stored column).
///
/// The result is normalized to exactly `column_count` entries: indexed columns
/// are stored first, so any trailing UNINDEXED columns are correctly padded
/// with zero, and a malformed or truncated blob degrades to zero lengths
/// rather than failing the open.
///
/// Validated against real cass `_docsize` blobs (e.g. `01010202130000` ->
/// `[1,1,2,2,19,0,0]`; multi-byte `82081301050D0000` -> `[264,19,1,5,13,0,0]`).
#[must_use]
pub fn decode_docsize_blob(blob: &[u8], column_count: usize) -> Vec<u32> {
    let mut counts = Vec::with_capacity(column_count);
    let mut offset = 0_usize;
    while offset < blob.len() {
        match fts5_read_varint(blob, &mut offset, "docsize column token count") {
            Ok(value) => counts.push(u32::try_from(value).unwrap_or(u32::MAX)),
            Err(_) => break,
        }
    }
    counts.resize(column_count, 0);
    counts
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ConfigMetadata {
    pub format_version: i64,
    pub page_size: i64,
    pub automerge: i64,
    pub usermerge: i64,
    pub crisismerge: i64,
    pub hash_size: i64,
    pub delete_merge: i64,
    pub rank: Option<String>,
    pub secure_delete: bool,
    pub insttoken: bool,
}

impl Default for Fts5ConfigMetadata {
    fn default() -> Self {
        Self {
            format_version: FTS5_CONFIG_VERSION,
            page_size: FTS5_DEFAULT_PAGE_SIZE,
            automerge: FTS5_DEFAULT_AUTOMERGE,
            usermerge: FTS5_DEFAULT_USERMERGE,
            crisismerge: FTS5_DEFAULT_CRISISMERGE,
            hash_size: FTS5_DEFAULT_HASHSIZE,
            delete_merge: FTS5_DEFAULT_DELETE_AUTOMERGE,
            rank: None,
            secure_delete: false,
            insttoken: false,
        }
    }
}

impl Fts5ConfigMetadata {
    #[must_use]
    pub fn from_runtime_config(config: Fts5Config) -> Self {
        Self {
            secure_delete: config.secure_delete,
            insttoken: config.insttoken,
            ..Self::default()
        }
    }

    pub fn apply_to_runtime_config(&self, config: &mut Fts5Config) {
        config.secure_delete = self.secure_delete;
        config.insttoken = self.insttoken;
    }

    #[must_use]
    pub fn encode_rows(&self) -> Vec<Fts5ConfigRecord> {
        let mut rows = Vec::with_capacity(10);

        push_non_default_integer(
            &mut rows,
            "automerge",
            self.automerge,
            FTS5_DEFAULT_AUTOMERGE,
        );
        push_non_default_integer(
            &mut rows,
            "crisismerge",
            self.crisismerge,
            FTS5_DEFAULT_CRISISMERGE,
        );
        push_non_default_integer(
            &mut rows,
            "deletemerge",
            self.delete_merge,
            FTS5_DEFAULT_DELETE_AUTOMERGE,
        );
        push_non_default_integer(&mut rows, "hashsize", self.hash_size, FTS5_DEFAULT_HASHSIZE);
        if self.insttoken {
            rows.push(Fts5ConfigRecord::integer("insttoken", 1));
        }
        push_non_default_integer(&mut rows, "pgsz", self.page_size, FTS5_DEFAULT_PAGE_SIZE);
        if let Some(rank) = self.rank.as_ref() {
            rows.push(Fts5ConfigRecord::text("rank", rank.clone()));
        }
        if self.secure_delete {
            rows.push(Fts5ConfigRecord::integer("secure-delete", 1));
        }
        push_non_default_integer(
            &mut rows,
            "usermerge",
            self.usermerge,
            FTS5_DEFAULT_USERMERGE,
        );
        rows.push(Fts5ConfigRecord::integer("version", self.format_version));
        rows.sort_by(|left, right| left.key.cmp(&right.key));
        rows
    }

    pub fn decode_rows(rows: &[Fts5ConfigRecord]) -> Result<Self> {
        let mut metadata = Self::default();
        let mut seen_version = None;

        for row in rows {
            let key = row.key.trim();
            if key.eq_ignore_ascii_case("version") {
                seen_version = config_integer_value(&row.value);
                continue;
            }
            apply_config_metadata_row(&mut metadata, key, &row.value);
        }

        let Some(version) = seen_version else {
            return Err(FrankenError::function_error(
                "fts5: missing version row in %_config",
            ));
        };
        if version != FTS5_CONFIG_VERSION && version != FTS5_CONFIG_VERSION_SECURE_DELETE {
            return Err(FrankenError::function_error(format!(
                "invalid fts5 file format (found {version}, expected {FTS5_CONFIG_VERSION} or {FTS5_CONFIG_VERSION_SECURE_DELETE}) - run 'rebuild'"
            )));
        }

        metadata.format_version = version;
        Ok(metadata)
    }
}

fn push_non_default_integer(
    rows: &mut Vec<Fts5ConfigRecord>,
    key: &'static str,
    value: i64,
    default: i64,
) {
    if value != default {
        rows.push(Fts5ConfigRecord::integer(key, value));
    }
}

fn config_integer_value(value: &SqliteValue) -> Option<i64> {
    match value {
        SqliteValue::Integer(value) => Some(*value),
        SqliteValue::Text(text) => text.as_str().trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn apply_config_metadata_row(metadata: &mut Fts5ConfigMetadata, key: &str, value: &SqliteValue) {
    match key.to_ascii_lowercase().as_str() {
        "pgsz" => {
            if let Some(page_size) = config_integer_value(value)
                && (32..=FTS5_MAX_PAGE_SIZE).contains(&page_size)
            {
                metadata.page_size = page_size;
            }
        }
        "hashsize" => {
            if let Some(hash_size) = config_integer_value(value)
                && hash_size > 0
            {
                metadata.hash_size = hash_size;
            }
        }
        "automerge" => {
            if let Some(mut automerge) = config_integer_value(value)
                && (0..=64).contains(&automerge)
            {
                if automerge == 1 {
                    automerge = FTS5_DEFAULT_AUTOMERGE;
                }
                metadata.automerge = automerge;
            }
        }
        "usermerge" => {
            if let Some(usermerge) = config_integer_value(value)
                && (2..=16).contains(&usermerge)
            {
                metadata.usermerge = usermerge;
            }
        }
        "crisismerge" => {
            if let Some(mut crisismerge) = config_integer_value(value)
                && crisismerge >= 0
            {
                if crisismerge <= 1 {
                    crisismerge = FTS5_DEFAULT_CRISISMERGE;
                }
                if crisismerge >= FTS5_MAX_SEGMENT {
                    crisismerge = FTS5_MAX_SEGMENT - 1;
                }
                metadata.crisismerge = crisismerge;
            }
        }
        "deletemerge" => {
            if let Some(mut delete_merge) = config_integer_value(value) {
                if delete_merge < 0 {
                    delete_merge = FTS5_DEFAULT_DELETE_AUTOMERGE;
                }
                if delete_merge > 100 {
                    delete_merge = 0;
                }
                metadata.delete_merge = delete_merge;
            }
        }
        "rank" => {
            metadata.rank = Some(value.to_text());
        }
        "secure-delete" => {
            if let Some(value) = config_integer_value(value)
                && value >= 0
            {
                metadata.secure_delete = value != 0;
            }
        }
        "insttoken" => {
            if let Some(value) = config_integer_value(value)
                && value >= 0
            {
                metadata.insttoken = value != 0;
            }
        }
        _ => {}
    }
}

fn fts5_data_error(detail: impl Into<String>) -> FrankenError {
    FrankenError::function_error(format!("fts5: corrupt %_data record: {}", detail.into()))
}

fn fts5_push_varint(block: &mut Vec<u8>, value: u64) {
    let mut buf = [0; 9];
    let len = write_varint(&mut buf, value);
    block.extend_from_slice(&buf[..len]);
}

fn fts5_read_varint(block: &[u8], offset: &mut usize, field: &str) -> Result<u64> {
    if *offset >= block.len() {
        return Err(fts5_data_error(format!("missing {field}")));
    }
    let (value, len) = read_varint(&block[*offset..])
        .ok_or_else(|| fts5_data_error(format!("truncated varint for {field}")))?;
    *offset += len;
    Ok(value)
}

fn fts5_read_usize(block: &[u8], offset: &mut usize, field: &str) -> Result<usize> {
    let value = fts5_read_varint(block, offset, field)?;
    usize::try_from(value).map_err(|_| fts5_data_error(format!("{field} exceeds usize")))
}

fn fts5_read_bounded_usize(block: &[u8], offset: &mut usize, field: &str) -> Result<usize> {
    let value = fts5_read_varint(block, offset, field)?;
    if value > FTS5_MAX_SEGMENT_U64 {
        return Err(fts5_data_error(format!("{field} exceeds FTS5 maximum")));
    }
    usize::try_from(value).map_err(|_| fts5_data_error(format!("{field} exceeds usize")))
}

fn fts5_read_u32(block: &[u8], offset: &mut usize, field: &str) -> Result<u32> {
    let value = fts5_read_varint(block, offset, field)?;
    u32::try_from(value).map_err(|_| fts5_data_error(format!("{field} exceeds u32")))
}

fn fts5_data_rowid(segid: u32, dlidx: bool, height: u8, pgno: u32, kind: &str) -> Result<i64> {
    if segid == 0 {
        return Err(fts5_data_error(format!("{kind} segid must be non-zero")));
    }
    if u64::from(segid) >= FTS5_DATA_ID_LIMIT {
        return Err(fts5_data_error(format!(
            "{kind} segid exceeds 16-bit limit"
        )));
    }
    fts5_data_rowid_from_parts(u64::from(segid), dlidx, height, pgno)
}

fn fts5_data_rowid_from_parts(segid: u64, dlidx: bool, height: u8, pgno: u32) -> Result<i64> {
    if u64::from(height) > FTS5_DATA_HEIGHT_MASK {
        return Err(fts5_data_error("dlidx height exceeds 5-bit limit"));
    }
    if u64::from(pgno) > FTS5_DATA_PAGE_MASK {
        return Err(fts5_data_error("page number exceeds 31-bit limit"));
    }

    let rowid = (segid << (FTS5_DATA_PAGE_BITS + FTS5_DATA_HEIGHT_BITS + FTS5_DATA_DLI_BITS))
        | (u64::from(dlidx) << (FTS5_DATA_PAGE_BITS + FTS5_DATA_HEIGHT_BITS))
        | (u64::from(height) << FTS5_DATA_PAGE_BITS)
        | u64::from(pgno);
    i64::try_from(rowid).map_err(|_| fts5_data_error("segment rowid exceeds i64"))
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn read_be_u16(bytes: &[u8]) -> u16 {
    u16::from_be_bytes([bytes[0], bytes[1]])
}

fn write_be_u16(bytes: &mut [u8], value: u16) {
    bytes[..2].copy_from_slice(&value.to_be_bytes());
}

fn read_be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn write_be_u32(bytes: &mut [u8], value: u32) {
    bytes[..4].copy_from_slice(&value.to_be_bytes());
}

fn read_be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

fn write_be_u64(bytes: &mut [u8], value: u64) {
    bytes[..8].copy_from_slice(&value.to_be_bytes());
}

fn fts5_checksum_u8(checksum: u64, value: u8) -> u64 {
    (checksum ^ u64::from(value)).wrapping_mul(FTS5_SEGMENT_CHECKSUM_PRIME)
}

fn fts5_checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum = fts5_checksum_u8(checksum, *byte);
    }
    checksum
}

fn fts5_checksum_u32(checksum: u64, value: u32) -> u64 {
    fts5_checksum_bytes(checksum, &value.to_be_bytes())
}

fn fts5_checksum_i64(checksum: u64, value: i64) -> u64 {
    fts5_checksum_bytes(checksum, &value.to_be_bytes())
}

fn postings_have_position_match(
    left: &Fts5DoclistEntry,
    right: &Fts5DoclistEntry,
    matches_positions: &impl Fn(u32, u32) -> bool,
) -> bool {
    for left_column in &left.poslist.columns {
        let Some(right_column) = right
            .poslist
            .columns
            .iter()
            .find(|column| column.column == left_column.column)
        else {
            continue;
        };
        for left_offset in &left_column.offsets {
            if right_column
                .offsets
                .iter()
                .any(|right_offset| matches_positions(*left_offset, *right_offset))
            {
                return true;
            }
        }
    }
    false
}

impl Fts5Config {
    #[must_use]
    pub const fn new(content_mode: ContentMode) -> Self {
        Self {
            secure_delete: false,
            content_mode,
            contentless_delete: false,
            contentless_unindexed: false,
            columnsize: true,
            detail: DetailMode::Full,
            insttoken: false,
            locale: false,
            tokendata: false,
        }
    }

    #[must_use]
    pub const fn secure_delete_enabled(self) -> bool {
        self.secure_delete
    }

    #[must_use]
    pub const fn contentless_delete_enabled(self) -> bool {
        self.contentless_delete
    }

    #[must_use]
    pub const fn contentless_unindexed_enabled(self) -> bool {
        self.contentless_unindexed
    }

    #[must_use]
    pub const fn columnsize_enabled(self) -> bool {
        self.columnsize
    }

    #[must_use]
    pub const fn detail_mode(self) -> DetailMode {
        self.detail
    }

    #[must_use]
    pub const fn insttoken_enabled(self) -> bool {
        self.insttoken
    }

    #[must_use]
    pub const fn locale_enabled(self) -> bool {
        self.locale
    }

    #[must_use]
    pub const fn tokendata_enabled(self) -> bool {
        self.tokendata
    }

    #[must_use]
    pub const fn content_mode(self) -> ContentMode {
        self.content_mode
    }

    #[must_use]
    pub const fn delete_action(self) -> DeleteAction {
        match self.content_mode {
            ContentMode::Stored => {
                if self.secure_delete {
                    DeleteAction::PhysicalPurge
                } else {
                    DeleteAction::Tombstone
                }
            }
            ContentMode::Contentless => {
                if !self.contentless_delete {
                    DeleteAction::Reject
                } else if self.secure_delete {
                    DeleteAction::PhysicalPurge
                } else {
                    DeleteAction::Tombstone
                }
            }
        }
    }

    pub fn apply_control_command(&mut self, command: &str) -> bool {
        let trimmed = command.trim();
        let Some((raw_key, raw_value)) = trimmed.split_once('=') else {
            return false;
        };

        let key = raw_key.trim().to_ascii_lowercase();
        let Some(value) = parse_bool_like(raw_value) else {
            return false;
        };

        match key.as_str() {
            "secure-delete" | "secure_delete" => {
                self.secure_delete = value;
                true
            }
            "contentless_delete" => {
                self.contentless_delete = value;
                true
            }
            "insttoken" => {
                self.insttoken = value;
                true
            }
            _ => false,
        }
    }

    #[must_use]
    pub fn config_metadata(self) -> Fts5ConfigMetadata {
        Fts5ConfigMetadata::from_runtime_config(self)
    }

    #[must_use]
    pub fn encode_config_rows(self) -> Vec<Fts5ConfigRecord> {
        self.config_metadata().encode_rows()
    }

    pub fn apply_config_rows(&mut self, rows: &[Fts5ConfigRecord]) -> Result<Fts5ConfigMetadata> {
        let metadata = Fts5ConfigMetadata::decode_rows(rows)?;
        metadata.apply_to_runtime_config(self);
        Ok(metadata)
    }
}

fn validate_contentless_options(config: Fts5Config) -> Result<()> {
    if config.contentless_delete && config.content_mode != ContentMode::Contentless {
        return Err(FrankenError::function_error(
            "contentless_delete=1 requires a contentless table",
        ));
    }
    if config.contentless_delete && !config.columnsize {
        return Err(FrankenError::function_error(
            "contentless_delete=1 is incompatible with columnsize=0",
        ));
    }
    if config.contentless_unindexed && config.content_mode != ContentMode::Contentless {
        return Err(FrankenError::function_error(
            "contentless_unindexed=1 requires a contentless table",
        ));
    }
    Ok(())
}

impl Default for Fts5Config {
    fn default() -> Self {
        Self::new(ContentMode::Stored)
    }
}

fn parse_bool_like(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Some(true),
        "0" | "off" | "false" => Some(false),
        _ => None,
    }
}

fn parse_columnsize_option(value: &str) -> Option<bool> {
    match value.trim() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

fn parse_detail_option(value: &str) -> Option<DetailMode> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("full") {
        Some(DetailMode::Full)
    } else if value.eq_ignore_ascii_case("column") {
        Some(DetailMode::Column)
    } else if value.eq_ignore_ascii_case("none") {
        Some(DetailMode::None)
    } else {
        None
    }
}

fn tokendata_query_key(term: &str) -> &str {
    match term.as_bytes().iter().position(|byte| *byte == 0) {
        Some(nul_pos) => {
            if let Some(query_key) = term.get(..nul_pos) {
                query_key
            } else {
                term
            }
        }
        None => term,
    }
}

fn parse_prefix_option(value: &str) -> Option<Vec<usize>> {
    let mut prefix_lengths = Vec::new();
    for segment in value.split_ascii_whitespace() {
        let prefix_length = segment.parse::<usize>().ok()?;
        if prefix_length == 0 {
            return None;
        }
        prefix_lengths.push(prefix_length);
    }
    (!prefix_lengths.is_empty()).then_some(prefix_lengths)
}

fn parse_option_assignment(input: &str) -> Option<(&str, &str)> {
    let (key, value) = input.split_once('=')?;
    Some((key.trim(), value.trim()))
}

fn unquote_fts_arg(value: &str) -> &str {
    let trimmed = value.trim();
    for quote in ['\'', '"', '`'] {
        if let Some(unquoted) = strip_fts_quote_pair(trimmed, quote) {
            return unquoted;
        }
    }
    trimmed
}

fn strip_fts_quote_pair(value: &str, quote: char) -> Option<&str> {
    value.strip_prefix(quote)?.strip_suffix(quote)
}

fn unquote_fts_identifier(value: &str) -> &str {
    value
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '[' | ']'))
}

fn parse_column_declaration(input: &str) -> Result<Option<(String, bool)>> {
    let mut segments = input.split_whitespace();
    let Some(raw_column) = segments.next() else {
        return Ok(None);
    };

    let column = unquote_fts_identifier(raw_column);
    if column.is_empty() {
        return Ok(None);
    }

    let mut indexed = true;
    let mut expect_collation_name = false;
    for segment in segments {
        if expect_collation_name {
            expect_collation_name = false;
            continue;
        }

        match segment.to_ascii_lowercase().as_str() {
            "unindexed" => indexed = false,
            "collate" => expect_collation_name = true,
            _ => {
                return Err(FrankenError::function_error(format!(
                    "fts5: unsupported column option '{segment}'"
                )));
            }
        }
    }

    if expect_collation_name {
        return Err(FrankenError::function_error(
            "fts5: COLLATE column option requires a collation name",
        ));
    }

    Ok(Some((column.to_owned(), indexed)))
}

fn validate_column_names(table_name: &str, columns: &[String]) -> Result<()> {
    let normalized_table_name = table_name.to_ascii_lowercase();
    let mut seen = HashSet::with_capacity(columns.len());

    for column in columns {
        let normalized = column.to_ascii_lowercase();
        match normalized.as_str() {
            "rowid" => {
                return Err(FrankenError::function_error(
                    "fts5: column name 'rowid' is reserved",
                ));
            }
            "rank" => {
                return Err(FrankenError::function_error(
                    "fts5: column name 'rank' is reserved",
                ));
            }
            _ if !normalized_table_name.is_empty() && normalized == normalized_table_name => {
                return Err(FrankenError::function_error(format!(
                    "fts5: column name '{column}' conflicts with table name"
                )));
            }
            _ => {}
        }

        if !seen.insert(normalized) {
            return Err(FrankenError::function_error(format!(
                "fts5: duplicate column name '{column}'"
            )));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tokenizer API
// ---------------------------------------------------------------------------

/// A single token produced by an FTS5 tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5Token {
    /// The normalized term (lowercased, stemmed, etc.).
    pub term: String,
    /// Byte offset of the start of this token in the original text.
    pub start: usize,
    /// Byte offset of the end of this token in the original text.
    pub end: usize,
    /// Whether this token is colocated with the previous one (synonym).
    pub colocated: bool,
}

/// Trait for FTS5 tokenizers.
pub trait Fts5Tokenizer: Send + Sync {
    /// Return the tokenizer name.
    fn name(&self) -> &'static str;

    /// Visit each token in the input text without forcing callers to materialize
    /// an intermediate token vector.
    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool));

    /// Tokenize the input text, producing a list of tokens.
    fn tokenize(&self, text: &str) -> Vec<Fts5Token> {
        let mut tokens = Vec::new();
        self.visit_tokens(text, &mut |term, start, end, colocated| {
            tokens.push(Fts5Token {
                term: term.to_owned(),
                start,
                end,
                colocated,
            });
        });
        tokens
    }
}

/// Unicode61 tokenizer: splits on non-alphanumeric characters, lowercases.
#[derive(Debug)]
pub struct Unicode61Tokenizer {
    /// Characters to treat as separators (empty = default Unicode categories).
    pub separators: String,
    /// Characters to treat as token characters (override default).
    pub token_chars: String,
    /// Whether to remove diacritics (0=no, 1=non-ASCII, 2=all).
    pub remove_diacritics: u8,
}

impl Default for Unicode61Tokenizer {
    fn default() -> Self {
        Self {
            separators: String::new(),
            token_chars: String::new(),
            remove_diacritics: 1,
        }
    }
}

impl Unicode61Tokenizer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn option_contains(option_chars: &str, ch: char) -> bool {
        if ch.is_ascii() {
            let mut encoded = [0; 4];
            let needle = ch.encode_utf8(&mut encoded).as_bytes()[0];
            option_chars.as_bytes().contains(&needle)
        } else {
            option_chars.contains(ch)
        }
    }

    fn is_token_char(&self, ch: char) -> bool {
        if !self.token_chars.is_empty() && Self::option_contains(&self.token_chars, ch) {
            return true;
        }
        if !self.separators.is_empty() && Self::option_contains(&self.separators, ch) {
            return false;
        }
        ch.is_alphanumeric()
    }

    fn normalized_char(&self, ch: char) -> char {
        if self.remove_diacritics == 0 {
            return ch;
        }
        latin_diacritic_base(ch).unwrap_or(ch)
    }
}

impl Fts5Tokenizer for Unicode61Tokenizer {
    fn name(&self) -> &'static str {
        "unicode61"
    }

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        let mut token_start = None;
        let mut current_term = String::new();
        let mut borrowed_ascii_token = false;

        for (byte_idx, ch) in text.char_indices() {
            if self.is_token_char(ch) {
                if token_start.is_none() {
                    token_start = Some(byte_idx);
                    current_term.clear();
                    borrowed_ascii_token = true;
                }
                if borrowed_ascii_token {
                    if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                        continue;
                    }
                    borrowed_ascii_token = false;
                    if let Some(start) = token_start {
                        current_term.push_str(&text[start..byte_idx]);
                    }
                }
                for lc in self.normalized_char(ch).to_lowercase() {
                    current_term.push(lc);
                }
            } else if let Some(start) = token_start.take() {
                if borrowed_ascii_token {
                    sink(&text[start..byte_idx], start, byte_idx, false);
                } else if !current_term.is_empty() {
                    sink(current_term.as_str(), start, byte_idx, false);
                    current_term.clear();
                }
            }
        }

        // Flush trailing token.
        if let Some(start) = token_start {
            if borrowed_ascii_token {
                sink(&text[start..], start, text.len(), false);
            } else if !current_term.is_empty() {
                sink(current_term.as_str(), start, text.len(), false);
            }
        }
    }
}

/// ASCII tokenizer: folds ASCII letters and treats all non-ASCII codepoints as
/// token characters.
#[derive(Debug, Default)]
pub struct AsciiTokenizer;

impl AsciiTokenizer {
    #[inline]
    fn normalized_token_char(ch: char) -> Option<char> {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            Some(ch)
        } else if ch.is_ascii_uppercase() {
            Some(ch.to_ascii_lowercase())
        } else if ch.is_ascii() {
            None
        } else {
            Some(ch)
        }
    }
}

impl Fts5Tokenizer for AsciiTokenizer {
    fn name(&self) -> &'static str {
        "ascii"
    }

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        let mut token_start = None;
        let mut token_buf: Option<String> = None;

        for (byte_idx, ch) in text.char_indices() {
            if let Some(term_ch) = Self::normalized_token_char(ch) {
                let start = if let Some(start) = token_start {
                    start
                } else {
                    token_start = Some(byte_idx);
                    token_buf = None;
                    byte_idx
                };

                if term_ch != ch {
                    let buf = token_buf.get_or_insert_with(|| {
                        let mut folded = String::new();
                        folded.push_str(&text[start..byte_idx]);
                        folded
                    });
                    buf.push(term_ch);
                } else if let Some(buf) = token_buf.as_mut() {
                    buf.push(term_ch);
                }
            } else if let Some(start) = token_start.take() {
                if let Some(buf) = token_buf.take() {
                    sink(buf.as_str(), start, byte_idx, false);
                } else {
                    sink(&text[start..byte_idx], start, byte_idx, false);
                }
            }
        }

        if let Some(start) = token_start {
            if let Some(buf) = token_buf {
                sink(buf.as_str(), start, text.len(), false);
            } else {
                sink(&text[start..], start, text.len(), false);
            }
        }
    }
}

/// Porter stemmer tokenizer: wraps another tokenizer and applies Porter
/// stemming to each term.
pub struct PorterTokenizer {
    inner: Box<dyn Fts5Tokenizer>,
}

impl std::fmt::Debug for PorterTokenizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PorterTokenizer")
            .field("inner", &self.inner.name())
            .finish()
    }
}

impl PorterTokenizer {
    pub fn new(inner: Box<dyn Fts5Tokenizer>) -> Self {
        Self { inner }
    }
}

impl Fts5Tokenizer for PorterTokenizer {
    fn name(&self) -> &'static str {
        "porter"
    }

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        self.inner
            .visit_tokens(text, &mut |term, start, end, colocated| {
                let stemmed = porter_stem(term);
                sink(stemmed.as_str(), start, end, colocated);
            });
    }
}

/// Simplified Porter stemmer (covers common English suffixes).
fn porter_stem(word: &str) -> String {
    let mut s = word.to_owned();

    // Step 1a: plurals
    if let Some(base) = s.strip_suffix("sses") {
        s = format!("{base}ss");
    } else if let Some(base) = s.strip_suffix("ies") {
        s = format!("{base}i");
    } else if s.ends_with('s') && !s.ends_with("ss") && s.len() > 3 {
        s.pop();
    }

    // Step 1b: -ed, -ing
    if let Some(base) = s.strip_suffix("eed") {
        if base.len() > 1 {
            s = format!("{base}ee");
        }
    } else if let Some(base) = s.strip_suffix("ed") {
        if contains_vowel(base) {
            s = base.to_owned();
            step1b_fixup(&mut s);
        }
    } else if let Some(base) = s.strip_suffix("ing") {
        if contains_vowel(base) {
            s = base.to_owned();
            step1b_fixup(&mut s);
        }
    }

    // Step 1c: terminal y -> i if stem contains vowel
    if s.ends_with('y') && s.len() > 2 && contains_vowel(&s[..s.len() - 1]) {
        s.pop();
        s.push('i');
    }

    // Step 2: double-suffix removal (common cases)
    apply_step2(&mut s);

    // Step 3: more suffixes
    apply_step3(&mut s);

    s
}

fn contains_vowel(s: &str) -> bool {
    let mut has_previous = false;
    let mut previous_was_vowel = false;

    for ch in s.chars() {
        let is_vowel = porter_is_vowel(ch, has_previous, previous_was_vowel);
        if is_vowel {
            return true;
        }
        has_previous = true;
        previous_was_vowel = is_vowel;
    }
    false
}

fn porter_is_vowel(ch: char, has_previous: bool, previous_was_vowel: bool) -> bool {
    matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u') || (ch == 'y' && has_previous && !previous_was_vowel)
}

fn step1b_fixup(s: &mut String) {
    if s.ends_with("at") || s.ends_with("bl") || s.ends_with("iz") {
        s.push('e');
    } else if s.len() >= 2 {
        let bytes = s.as_bytes();
        let last = bytes[bytes.len() - 1];
        let prev = bytes[bytes.len() - 2];
        if last == prev && last.is_ascii_lowercase() && !matches!(last, b'l' | b's' | b'z') {
            s.pop();
        }
    }
}

fn apply_step2(s: &mut String) {
    let replacements: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("alism", "al"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("ousli", "ous"),
        ("biliti", "ble"),
        ("logi", "log"),
    ];

    for (suffix, replacement) in replacements {
        if let Some(base) = s.strip_suffix(suffix) {
            if measure(base) > 0 {
                *s = format!("{base}{replacement}");
                return;
            }
        }
    }
}

fn apply_step3(s: &mut String) {
    let replacements: &[(&str, &str)] = &[
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];

    for (suffix, replacement) in replacements {
        if let Some(base) = s.strip_suffix(suffix) {
            if measure(base) > 0 {
                *s = format!("{base}{replacement}");
                return;
            }
        }
    }
}

/// Compute the "measure" m of a stem (number of VC sequences).
fn measure(s: &str) -> u32 {
    let mut m = 0u32;
    let mut in_vowel_seq = false;
    let mut has_previous = false;
    let mut previous_was_vowel = false;

    for ch in s.chars() {
        let is_vowel = porter_is_vowel(ch, has_previous, previous_was_vowel);
        if is_vowel {
            in_vowel_seq = true;
        } else if in_vowel_seq {
            m += 1;
            in_vowel_seq = false;
        }
        has_previous = true;
        previous_was_vowel = is_vowel;
    }

    m
}

/// Trigram tokenizer: generates all 3-character substrings of the input.
#[derive(Debug, Default)]
pub struct TrigramTokenizer {
    /// Whether matching is case-sensitive.
    pub case_sensitive: bool,
    /// Whether to remove common Latin diacritics before generating trigrams.
    pub remove_diacritics: bool,
}

impl Fts5Tokenizer for TrigramTokenizer {
    fn name(&self) -> &'static str {
        "trigram"
    }

    fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
        let mut window = SmallVec::<[(usize, char); 3]>::new();
        let mut term = String::new();
        for item in text.char_indices() {
            if window.len() == 3 {
                let _ = window.remove(0);
            }
            window.push(item);
            if window.len() < 3 {
                continue;
            }
            let start = window[0].0;
            let end_char = window[2];
            let end = end_char.0 + end_char.1.len_utf8();
            term.clear();
            for (_, ch) in &window {
                push_trigram_char(&mut term, *ch, self.case_sensitive, self.remove_diacritics);
            }
            sink(term.as_str(), start, end, false);
        }
    }
}

fn push_trigram_char(term: &mut String, ch: char, case_sensitive: bool, remove_diacritics: bool) {
    let ch = if remove_diacritics {
        latin_diacritic_base(ch).unwrap_or(ch)
    } else {
        ch
    };

    if case_sensitive {
        term.push(ch);
    } else {
        push_case_folded_trigram_char(term, ch);
    }
}

fn push_case_folded_trigram_char(term: &mut String, ch: char) {
    if ch.is_ascii() {
        term.push(ch.to_ascii_lowercase());
    } else {
        term.extend(ch.to_lowercase());
    }
}

fn latin_diacritic_base(ch: char) -> Option<char> {
    Some(match ch {
        'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' | 'à' | 'á' | 'â' | 'ã' | 'ä' | 'å'
        | 'ā' | 'ă' | 'ą' => 'a',
        'Ç' | 'Ć' | 'Ĉ' | 'Ċ' | 'Č' | 'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => 'c',
        'Ð' | 'Ď' | 'Đ' | 'ð' | 'ď' | 'đ' => 'd',
        'È' | 'É' | 'Ê' | 'Ë' | 'Ē' | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' | 'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ'
        | 'ė' | 'ę' | 'ě' => 'e',
        'Ĝ' | 'Ğ' | 'Ġ' | 'Ģ' | 'ĝ' | 'ğ' | 'ġ' | 'ģ' => 'g',
        'Ĥ' | 'Ħ' | 'ĥ' | 'ħ' => 'h',
        'Ì' | 'Í' | 'Î' | 'Ï' | 'Ĩ' | 'Ī' | 'Ĭ' | 'Į' | 'İ' | 'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī'
        | 'ĭ' | 'į' | 'ı' => 'i',
        'Ĵ' | 'ĵ' => 'j',
        'Ķ' | 'ķ' => 'k',
        'Ĺ' | 'Ļ' | 'Ľ' | 'Ŀ' | 'Ł' | 'ĺ' | 'ļ' | 'ľ' | 'ŀ' | 'ł' => 'l',
        'Ñ' | 'Ń' | 'Ņ' | 'Ň' | 'ñ' | 'ń' | 'ņ' | 'ň' => 'n',
        'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ø' | 'Ō' | 'Ŏ' | 'Ő' | 'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø'
        | 'ō' | 'ŏ' | 'ő' => 'o',
        'Ŕ' | 'Ŗ' | 'Ř' | 'ŕ' | 'ŗ' | 'ř' => 'r',
        'Ś' | 'Ŝ' | 'Ş' | 'Š' | 'ś' | 'ŝ' | 'ş' | 'š' => 's',
        'Ţ' | 'Ť' | 'Ŧ' | 'ţ' | 'ť' | 'ŧ' => 't',
        'Ù' | 'Ú' | 'Û' | 'Ü' | 'Ũ' | 'Ū' | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' | 'ù' | 'ú' | 'û' | 'ü' | 'ũ'
        | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => 'u',
        'Ŵ' | 'ŵ' => 'w',
        'Ý' | 'Ŷ' | 'Ÿ' | 'ý' | 'ÿ' | 'ŷ' => 'y',
        'Ź' | 'Ż' | 'Ž' | 'ź' | 'ż' | 'ž' => 'z',
        _ => return None,
    })
}

fn split_fts5_tokenizer_spec(spec: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = spec.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                if chars.peek() == Some(&quote_ch) {
                    let _ = chars.next();
                    current.push(quote_ch);
                } else {
                    quote = None;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch.is_ascii_whitespace() {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

fn take_tokenizer_option<'a>(parts: &'a [String], index: &mut usize) -> Option<(&'a str, &'a str)> {
    let raw = parts.get(*index)?;
    *index += 1;
    if let Some((key, value)) = raw.split_once('=') {
        return Some((key, value));
    }

    let value = parts.get(*index)?;
    *index += 1;
    Some((raw.as_str(), value.as_str()))
}

fn unicode61_tokenizer_from_args(args: &[String]) -> Option<Unicode61Tokenizer> {
    let mut tokenizer = Unicode61Tokenizer::new();
    let mut index = 0;

    while index < args.len() {
        let (key, value) = take_tokenizer_option(args, &mut index)?;
        match key.to_ascii_lowercase().as_str() {
            "tokenchars" | "token_chars" => value.clone_into(&mut tokenizer.token_chars),
            "separators" => value.clone_into(&mut tokenizer.separators),
            "remove_diacritics" => {
                let remove_diacritics = value.parse::<u8>().ok()?;
                if remove_diacritics > 2 {
                    return None;
                }
                tokenizer.remove_diacritics = remove_diacritics;
            }
            _ => return None,
        }
    }

    Some(tokenizer)
}

fn trigram_tokenizer_from_args(args: &[String]) -> Option<TrigramTokenizer> {
    let mut tokenizer = TrigramTokenizer::default();
    let mut index = 0;

    while let Some((key, value)) = take_tokenizer_option(args, &mut index) {
        match key.to_ascii_lowercase().as_str() {
            "case_sensitive" => {
                tokenizer.case_sensitive = parse_columnsize_option(value)?;
            }
            "remove_diacritics" => {
                tokenizer.remove_diacritics = parse_columnsize_option(value)?;
            }
            _ => return None,
        }
    }

    if tokenizer.case_sensitive && tokenizer.remove_diacritics {
        return None;
    }

    Some(tokenizer)
}

fn create_tokenizer_from_parts(parts: &[String]) -> Option<Box<dyn Fts5Tokenizer>> {
    let name = parts.first()?.to_ascii_lowercase();
    let args = &parts[1..];

    match name.as_str() {
        "unicode61" => Some(Box::new(unicode61_tokenizer_from_args(args)?)),
        "ascii" => Some(Box::new(AsciiTokenizer)),
        "porter" => {
            let inner = if args.is_empty() {
                Box::new(Unicode61Tokenizer::new()) as Box<dyn Fts5Tokenizer>
            } else {
                create_tokenizer_from_parts(args)?
            };
            Some(Box::new(PorterTokenizer::new(inner)))
        }
        "trigram" => Some(Box::new(trigram_tokenizer_from_args(args)?)),
        _ => None,
    }
}

/// Create a tokenizer by name with optional arguments.
#[must_use]
pub fn create_tokenizer(name: &str) -> Option<Box<dyn Fts5Tokenizer>> {
    let parts = split_fts5_tokenizer_spec(name);
    create_tokenizer_from_parts(&parts)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5TokenizeReason {
    Document,
    Query,
    Prefix,
    Auxiliary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fts5TokenizeRequest<'a> {
    pub text: &'a str,
    pub reason: Fts5TokenizeReason,
    pub locale: Option<&'a str>,
}

impl<'a> Fts5TokenizeRequest<'a> {
    #[must_use]
    pub const fn new(text: &'a str, reason: Fts5TokenizeReason) -> Self {
        Self {
            text,
            reason,
            locale: None,
        }
    }

    #[must_use]
    pub const fn with_locale(mut self, locale: &'a str) -> Self {
        self.locale = Some(locale);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5ExtensionContext<'a> {
    pub table_name: &'a str,
    pub columns: &'a [String],
    pub query_terms: &'a [String],
    pub rowid: Option<i64>,
    pub row: Option<&'a [String]>,
    pub locale: Option<&'a str>,
}

impl<'a> Fts5ExtensionContext<'a> {
    #[must_use]
    pub const fn new(table_name: &'a str, columns: &'a [String]) -> Self {
        Self {
            table_name,
            columns,
            query_terms: &[],
            rowid: None,
            row: None,
            locale: None,
        }
    }

    #[must_use]
    pub const fn with_query_terms(mut self, query_terms: &'a [String]) -> Self {
        self.query_terms = query_terms;
        self
    }

    #[must_use]
    pub const fn with_row(mut self, rowid: i64, row: &'a [String]) -> Self {
        self.rowid = Some(rowid);
        self.row = Some(row);
        self
    }

    #[must_use]
    pub const fn with_locale(mut self, locale: &'a str) -> Self {
        self.locale = Some(locale);
        self
    }
}

type Fts5TokenizerFactory = Arc<dyn Fn(&[String]) -> Option<Box<dyn Fts5Tokenizer>> + Send + Sync>;
type Fts5AuxiliaryFunction =
    Arc<dyn Fn(&Fts5ExtensionContext<'_>, &[SqliteValue]) -> Result<SqliteValue> + Send + Sync>;

#[derive(Clone)]
pub struct Fts5ExtensionApi {
    tokenizers: BTreeMap<String, Fts5TokenizerFactory>,
    auxiliaries: BTreeMap<String, Fts5AuxiliaryFunction>,
}

impl std::fmt::Debug for Fts5ExtensionApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fts5ExtensionApi")
            .field("tokenizers", &self.tokenizer_names())
            .field("auxiliaries", &self.auxiliary_names())
            .finish()
    }
}

impl Default for Fts5ExtensionApi {
    fn default() -> Self {
        Self::with_builtins()
    }
}

impl Fts5ExtensionApi {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tokenizers: BTreeMap::new(),
            auxiliaries: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_builtins() -> Self {
        let mut api = Self::new();
        let _ = api.register_tokenizer("unicode61", |args| {
            Some(Box::new(unicode61_tokenizer_from_args(args)?))
        });
        let _ = api.register_tokenizer("ascii", |args| {
            args.is_empty()
                .then_some(Box::new(AsciiTokenizer) as Box<dyn Fts5Tokenizer>)
        });
        let _ = api.register_tokenizer("porter", |args| {
            let inner = if args.is_empty() {
                Box::new(Unicode61Tokenizer::new()) as Box<dyn Fts5Tokenizer>
            } else {
                create_tokenizer_from_parts(args)?
            };
            Some(Box::new(PorterTokenizer::new(inner)))
        });
        let _ = api.register_tokenizer("trigram", |args| {
            Some(Box::new(trigram_tokenizer_from_args(args)?))
        });
        let _ = api.register_auxiliary("rowid", |ctx, _args| {
            Ok(ctx.rowid.map_or(SqliteValue::Null, SqliteValue::Integer))
        });
        let _ = api.register_auxiliary("column_count", |ctx, _args| {
            Ok(SqliteValue::Integer(
                i64::try_from(ctx.columns.len()).unwrap_or(i64::MAX),
            ))
        });
        api
    }

    pub fn register_tokenizer<F>(
        &mut self,
        name: impl AsRef<str>,
        factory: F,
    ) -> Option<Fts5TokenizerFactory>
    where
        F: Fn(&[String]) -> Option<Box<dyn Fts5Tokenizer>> + Send + Sync + 'static,
    {
        self.tokenizers
            .insert(canonical_fts5_api_name(name.as_ref()), Arc::new(factory))
    }

    #[must_use]
    pub fn find_tokenizer(&self, name: &str) -> Option<Fts5TokenizerFactory> {
        self.tokenizers
            .get(&canonical_fts5_api_name(name))
            .map(Arc::clone)
    }

    #[must_use]
    pub fn create_tokenizer(&self, spec: &str) -> Option<Box<dyn Fts5Tokenizer>> {
        let parts = split_fts5_tokenizer_spec(spec);
        let name = parts.first()?;
        self.find_tokenizer(name)?.as_ref()(&parts[1..])
    }

    pub fn tokenize(&self, spec: &str, request: Fts5TokenizeRequest<'_>) -> Result<Vec<Fts5Token>> {
        let tokenizer = self.create_tokenizer(spec).ok_or_else(|| {
            FrankenError::function_error(format!("fts5: unknown tokenizer '{spec}'"))
        })?;
        let mut tokens = tokenizer.tokenize(request.text);
        if request.reason == Fts5TokenizeReason::Prefix {
            tokens.retain(|token| !token.term.is_empty());
        }
        Ok(tokens)
    }

    pub fn register_auxiliary<F>(
        &mut self,
        name: impl AsRef<str>,
        function: F,
    ) -> Option<Fts5AuxiliaryFunction>
    where
        F: Fn(&Fts5ExtensionContext<'_>, &[SqliteValue]) -> Result<SqliteValue>
            + Send
            + Sync
            + 'static,
    {
        self.auxiliaries
            .insert(canonical_fts5_api_name(name.as_ref()), Arc::new(function))
    }

    #[must_use]
    pub fn find_auxiliary(&self, name: &str) -> Option<Fts5AuxiliaryFunction> {
        self.auxiliaries
            .get(&canonical_fts5_api_name(name))
            .map(Arc::clone)
    }

    pub fn invoke_auxiliary(
        &self,
        name: &str,
        context: &Fts5ExtensionContext<'_>,
        args: &[SqliteValue],
    ) -> Result<SqliteValue> {
        let function = self.find_auxiliary(name).ok_or_else(|| {
            FrankenError::function_error(format!("fts5: unknown auxiliary function '{name}'"))
        })?;
        function(context, args)
    }

    #[must_use]
    pub fn tokenizer_names(&self) -> Vec<String> {
        self.tokenizers.keys().cloned().collect()
    }

    #[must_use]
    pub fn auxiliary_names(&self) -> Vec<String> {
        self.auxiliaries.keys().cloned().collect()
    }
}

fn canonical_fts5_api_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

// ---------------------------------------------------------------------------
// Query parsing
// ---------------------------------------------------------------------------

/// Token kinds in an FTS5 query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fts5QueryTokenKind {
    Term,
    Phrase,
    And,
    Or,
    Not,
    Near,
    Plus,
    LParen,
    RParen,
    ColumnFilter,
    Prefix,
    Caret,
}

/// A token in an FTS5 query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5QueryToken {
    pub kind: Fts5QueryTokenKind,
    pub lexeme: String,
}

/// FTS5 query validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fts5QueryError {
    EmptyQuery,
    UnclosedPhrase,
    UnbalancedParentheses,
    UnaryNotForbidden,
    InvalidColumnFilter(String),
    InvalidNearSyntax,
    InvalidPhraseSyntax,
    ShadowStorage(String),
    UnsupportedByDetailMode {
        detail: DetailMode,
        feature: &'static str,
    },
}

impl std::fmt::Display for Fts5QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQuery => write!(f, "empty FTS5 query"),
            Self::UnclosedPhrase => write!(f, "unclosed phrase literal"),
            Self::UnbalancedParentheses => write!(f, "unbalanced parentheses"),
            Self::UnaryNotForbidden => {
                write!(f, "FTS5 NOT is binary-only; unary NOT is not allowed")
            }
            Self::InvalidColumnFilter(col) => write!(f, "invalid column filter: {col}"),
            Self::InvalidNearSyntax => write!(f, "invalid NEAR syntax"),
            Self::InvalidPhraseSyntax => write!(f, "invalid phrase syntax"),
            Self::ShadowStorage(detail) => write!(f, "FTS5 shadow storage error: {detail}"),
            Self::UnsupportedByDetailMode { detail, feature } => {
                write!(f, "detail={detail} does not support {feature}")
            }
        }
    }
}

/// Parse an FTS5 query string into tokens.
///
/// FTS5 uses implicit AND (two adjacent terms are ANDed together).
/// NOT is binary-only (requires a left operand).
pub fn parse_fts5_query(query: &str) -> std::result::Result<Vec<Fts5QueryToken>, Fts5QueryError> {
    let tokens = tokenize_fts5_query(query)?;
    validate_fts5_parentheses(&tokens)?;
    validate_fts5_not_binary(&tokens)?;
    Ok(insert_implicit_and(&tokens))
}

fn tokenize_fts5_query(query: &str) -> std::result::Result<Vec<Fts5QueryToken>, Fts5QueryError> {
    let normalized_query = normalize_column_filter_syntax(query);
    let mut chars = normalized_query.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            let _ = chars.next();
            continue;
        }

        if ch == '(' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::LParen,
                lexeme: "(".to_owned(),
            });
            continue;
        }

        if ch == ')' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::RParen,
                lexeme: ")".to_owned(),
            });
            continue;
        }

        if ch == '+' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::Plus,
                lexeme: "+".to_owned(),
            });
            continue;
        }

        if ch == '^' {
            let _ = chars.next();
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::Caret,
                lexeme: "^".to_owned(),
            });
            continue;
        }

        if ch == '"' {
            let _ = chars.next();
            let mut phrase = String::new();
            let mut closed = false;

            while let Some(phrase_ch) = chars.next() {
                if phrase_ch == '"' {
                    if chars.peek() == Some(&'"') {
                        // Escaped double-quote.
                        let _ = chars.next();
                        phrase.push('"');
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    phrase.push(phrase_ch);
                }
            }

            if !closed {
                return Err(Fts5QueryError::UnclosedPhrase);
            }
            if !phrase.is_empty() {
                tokens.push(Fts5QueryToken {
                    kind: Fts5QueryTokenKind::Phrase,
                    lexeme: phrase,
                });
            }
            continue;
        }

        // Read a word.
        let mut word = String::new();
        while let Some(word_ch) = chars.peek().copied() {
            if word_ch.is_ascii_whitespace() || matches!(word_ch, '(' | ')' | '"' | '+' | '^') {
                break;
            }
            let _ = chars.next();
            word.push(word_ch);
        }

        if word.is_empty() {
            continue;
        }

        push_query_word_tokens(&word, &mut tokens);
    }

    if tokens.is_empty() {
        return Err(Fts5QueryError::EmptyQuery);
    }
    Ok(tokens)
}

fn normalize_column_filter_syntax(query: &str) -> Cow<'_, str> {
    if !query.as_bytes().contains(&b':') {
        return Cow::Borrowed(query);
    }

    let chars: Vec<char> = query.chars().collect();
    let mut normalized = String::with_capacity(query.len());
    let mut idx = 0;

    while idx < chars.len() {
        if chars[idx] == '-' {
            let filter_start = skip_query_whitespace(&chars, idx + 1);
            if let Some((filter, next_idx)) = read_column_filter_at(&chars, filter_start) {
                normalized.push('-');
                normalized.push_str(&filter);
                idx = next_idx;
                continue;
            }
        }

        if let Some((filter, next_idx)) = read_column_filter_at(&chars, idx) {
            normalized.push_str(&filter);
            idx = next_idx;
            continue;
        }

        normalized.push(chars[idx]);
        idx += 1;
    }

    Cow::Owned(normalized)
}

fn skip_query_whitespace(chars: &[char], mut idx: usize) -> usize {
    while idx < chars.len() && chars[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn read_column_filter_at(chars: &[char], idx: usize) -> Option<(String, usize)> {
    if chars.get(idx) == Some(&'{') {
        return read_braced_column_filter_at(chars, idx);
    }

    read_single_column_filter_at(chars, idx)
}

fn read_braced_column_filter_at(chars: &[char], idx: usize) -> Option<(String, usize)> {
    let mut end = idx + 1;
    while end < chars.len() && chars[end] != '}' {
        end += 1;
    }
    if end >= chars.len() {
        return None;
    }

    let colon_idx = skip_query_whitespace(chars, end + 1);
    if chars.get(colon_idx) != Some(&':') {
        return None;
    }

    let inner: String = chars[idx + 1..end].iter().collect();
    let columns = inner
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    if columns.is_empty() {
        return None;
    }

    Some((format!("{{{columns}}}:"), colon_idx + 1))
}

fn read_single_column_filter_at(chars: &[char], idx: usize) -> Option<(String, usize)> {
    let mut end = idx;
    while end < chars.len()
        && !chars[end].is_ascii_whitespace()
        && !matches!(chars[end], ':' | '(' | ')' | '"' | '^')
    {
        end += 1;
    }
    if end == idx {
        return None;
    }

    let colon_idx = skip_query_whitespace(chars, end);
    if chars.get(colon_idx) != Some(&':') {
        return None;
    }

    let column: String = chars[idx..end].iter().collect();
    Some((format!("{column}:"), colon_idx + 1))
}

fn push_query_word_tokens(word: &str, tokens: &mut Vec<Fts5QueryToken>) {
    if word.ends_with(':') {
        let col_name = word.trim_end_matches(':');
        if !col_name.is_empty() {
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::ColumnFilter,
                lexeme: col_name.to_owned(),
            });
        }
        return;
    }

    if let Some((column_name, remainder)) = word.split_once(':')
        && !column_name.is_empty()
        && !remainder.is_empty()
    {
        tokens.push(Fts5QueryToken {
            kind: Fts5QueryTokenKind::ColumnFilter,
            lexeme: column_name.to_owned(),
        });
        push_query_word_tokens(remainder, tokens);
        return;
    }

    if word.ends_with('*') {
        let base = word.trim_end_matches('*');
        if !base.is_empty() {
            tokens.push(Fts5QueryToken {
                kind: Fts5QueryTokenKind::Prefix,
                lexeme: base.to_owned(),
            });
            return;
        }
    }

    let upper = word.to_ascii_uppercase();
    let kind = match upper.as_str() {
        "AND" => Fts5QueryTokenKind::And,
        "OR" => Fts5QueryTokenKind::Or,
        "NOT" => Fts5QueryTokenKind::Not,
        "NEAR" => Fts5QueryTokenKind::Near,
        _ => Fts5QueryTokenKind::Term,
    };

    tokens.push(Fts5QueryToken {
        kind,
        lexeme: word.to_owned(),
    });
}

fn validate_fts5_parentheses(tokens: &[Fts5QueryToken]) -> std::result::Result<(), Fts5QueryError> {
    let mut depth = 0u32;
    for token in tokens {
        match token.kind {
            Fts5QueryTokenKind::LParen => depth = depth.saturating_add(1),
            Fts5QueryTokenKind::RParen => {
                if depth == 0 {
                    return Err(Fts5QueryError::UnbalancedParentheses);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Fts5QueryError::UnbalancedParentheses);
    }
    Ok(())
}

fn validate_fts5_not_binary(tokens: &[Fts5QueryToken]) -> std::result::Result<(), Fts5QueryError> {
    for (i, token) in tokens.iter().enumerate() {
        if token.kind == Fts5QueryTokenKind::Not {
            // In FTS5, NOT is binary-only. It must have a left operand.
            if i == 0 {
                return Err(Fts5QueryError::UnaryNotForbidden);
            }
            // Check the token to the left is an expression-ending token.
            let left = &tokens[i - 1];
            if !matches!(
                left.kind,
                Fts5QueryTokenKind::Term
                    | Fts5QueryTokenKind::Phrase
                    | Fts5QueryTokenKind::Prefix
                    | Fts5QueryTokenKind::RParen
            ) {
                return Err(Fts5QueryError::UnaryNotForbidden);
            }
        }
    }
    Ok(())
}

/// Insert implicit AND tokens between adjacent expressions in FTS5.
fn insert_implicit_and(tokens: &[Fts5QueryToken]) -> Vec<Fts5QueryToken> {
    let mut result = Vec::with_capacity(tokens.len() * 2);

    for (i, token) in tokens.iter().enumerate() {
        if i > 0 {
            let prev = &tokens[i - 1];
            let prev_ends = matches!(
                prev.kind,
                Fts5QueryTokenKind::Term
                    | Fts5QueryTokenKind::Phrase
                    | Fts5QueryTokenKind::Prefix
                    | Fts5QueryTokenKind::RParen
            );
            let cur_starts = matches!(
                token.kind,
                Fts5QueryTokenKind::Term
                    | Fts5QueryTokenKind::Phrase
                    | Fts5QueryTokenKind::Prefix
                    | Fts5QueryTokenKind::LParen
                    | Fts5QueryTokenKind::Caret
                    | Fts5QueryTokenKind::ColumnFilter
                    | Fts5QueryTokenKind::Near
            );

            if prev_ends && cur_starts {
                result.push(Fts5QueryToken {
                    kind: Fts5QueryTokenKind::And,
                    lexeme: "AND".to_owned(),
                });
            }
        }
        result.push(token.clone());
    }

    result
}

// ---------------------------------------------------------------------------
// Query evaluation
// ---------------------------------------------------------------------------

/// A parsed FTS5 query expression tree.
#[derive(Debug, Clone)]
pub enum Fts5Expr {
    Term(String),
    Prefix(String),
    Phrase(Vec<String>),
    PhrasePrefix(Vec<String>, String),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>, Box<Self>),
    Near(Vec<Fts5NearOperand>, u32),
    ColumnFilter(String, Box<Self>),
    InitialToken(Box<Self>),
}

/// A phrase-like operand inside an FTS5 NEAR group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fts5NearOperand {
    Term(String),
    Prefix(String),
    Phrase(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fts5ColumnFilterSpec {
    exclude: bool,
    columns: SmallVec<[String; 4]>,
}

fn parse_column_filter_spec(raw: &str) -> Option<Fts5ColumnFilterSpec> {
    let trimmed = raw.trim();
    let (exclude, body) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest.trim())
    } else {
        (false, trimmed)
    };

    if body.is_empty() {
        return None;
    }

    let mut columns = SmallVec::new();
    if let Some(inner) = body
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
    {
        for column in inner.split(|ch: char| ch == ',' || ch.is_ascii_whitespace()) {
            let column = unquote_fts_identifier(column);
            if !column.is_empty() {
                columns.push(column.to_owned());
            }
        }
    } else {
        let column = unquote_fts_identifier(body);
        if !column.is_empty() {
            columns.push(column.to_owned());
        }
    }

    (!columns.is_empty()).then_some(Fts5ColumnFilterSpec { exclude, columns })
}

fn near_operand_from_token(token: &Fts5QueryToken) -> Option<Fts5NearOperand> {
    match token.kind {
        Fts5QueryTokenKind::Term if !token.lexeme.trim().is_empty() => {
            Some(Fts5NearOperand::Term(token.lexeme.trim().to_owned()))
        }
        Fts5QueryTokenKind::Prefix if !token.lexeme.trim().is_empty() => {
            Some(Fts5NearOperand::Prefix(token.lexeme.trim().to_owned()))
        }
        Fts5QueryTokenKind::Phrase => {
            let terms: Vec<String> = token
                .lexeme
                .split_whitespace()
                .map(str::to_lowercase)
                .collect();
            (!terms.is_empty()).then_some(Fts5NearOperand::Phrase(terms))
        }
        _ => None,
    }
}

fn append_phrase_token(
    token: &Fts5QueryToken,
    words: &mut Vec<String>,
    prefix: &mut Option<String>,
) -> std::result::Result<(), Fts5QueryError> {
    if prefix.is_some() {
        return Err(Fts5QueryError::InvalidPhraseSyntax);
    }

    match token.kind {
        Fts5QueryTokenKind::Term if !token.lexeme.trim().is_empty() => {
            words.push(token.lexeme.trim().to_owned());
            Ok(())
        }
        Fts5QueryTokenKind::Phrase => {
            let phrase_words = token
                .lexeme
                .split_whitespace()
                .map(str::to_lowercase)
                .collect::<Vec<_>>();
            if phrase_words.is_empty() {
                return Err(Fts5QueryError::InvalidPhraseSyntax);
            }
            words.extend(phrase_words);
            Ok(())
        }
        Fts5QueryTokenKind::Prefix if !token.lexeme.trim().is_empty() => {
            *prefix = Some(token.lexeme.trim().to_owned());
            Ok(())
        }
        _ => Err(Fts5QueryError::InvalidPhraseSyntax),
    }
}

fn parse_phrase_expr(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let Some(first) = tokens.first() else {
        return Err(Fts5QueryError::EmptyQuery);
    };

    let mut words = Vec::new();
    let mut prefix = None;
    append_phrase_token(first, &mut words, &mut prefix)?;
    let mut rest = &tokens[1..];
    let mut saw_plus = false;

    while rest
        .first()
        .is_some_and(|token| token.kind == Fts5QueryTokenKind::Plus)
    {
        saw_plus = true;
        let Some(next) = rest.get(1) else {
            return Err(Fts5QueryError::InvalidPhraseSyntax);
        };
        append_phrase_token(next, &mut words, &mut prefix)?;
        rest = &rest[2..];
    }

    if let Some(prefix) = prefix {
        if saw_plus {
            return Ok((Fts5Expr::PhrasePrefix(words, prefix), rest));
        }
        return Ok((Fts5Expr::Prefix(prefix), rest));
    }

    if saw_plus || first.kind == Fts5QueryTokenKind::Phrase {
        return Ok((Fts5Expr::Phrase(words), rest));
    }

    let Some(term) = words.into_iter().next() else {
        return Err(Fts5QueryError::InvalidPhraseSyntax);
    };
    Ok((Fts5Expr::Term(term), rest))
}

/// Build an expression tree from parsed FTS5 query tokens.
pub fn build_expr(tokens: &[Fts5QueryToken]) -> std::result::Result<Fts5Expr, Fts5QueryError> {
    let (expr, rest) = parse_or(tokens)?;
    if !rest.is_empty() {
        return Err(Fts5QueryError::InvalidPhraseSyntax);
    }
    Ok(expr)
}

fn parse_or(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let (mut left, mut rest) = parse_and(tokens)?;

    while let Some(token) = rest.first() {
        if token.kind == Fts5QueryTokenKind::Or {
            let (right, r) = parse_and(&rest[1..])?;
            left = Fts5Expr::Or(Box::new(left), Box::new(right));
            rest = r;
        } else {
            break;
        }
    }

    Ok((left, rest))
}

fn parse_and(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let (mut left, mut rest) = parse_primary(tokens)?;

    while let Some(token) = rest.first() {
        if token.kind == Fts5QueryTokenKind::And {
            let (right, r) = parse_primary(&rest[1..])?;
            left = Fts5Expr::And(Box::new(left), Box::new(right));
            rest = r;
        } else if token.kind == Fts5QueryTokenKind::Not {
            let (right, r) = parse_primary(&rest[1..])?;
            left = Fts5Expr::Not(Box::new(left), Box::new(right));
            rest = r;
        } else {
            break;
        }
    }

    Ok((left, rest))
}

#[allow(clippy::too_many_lines)]
fn parse_primary(
    tokens: &[Fts5QueryToken],
) -> std::result::Result<(Fts5Expr, &[Fts5QueryToken]), Fts5QueryError> {
    let Some(token) = tokens.first() else {
        return Err(Fts5QueryError::EmptyQuery);
    };

    match token.kind {
        Fts5QueryTokenKind::Term | Fts5QueryTokenKind::Prefix | Fts5QueryTokenKind::Phrase => {
            parse_phrase_expr(tokens)
        }
        Fts5QueryTokenKind::LParen => {
            let (expr, rest) = parse_or(&tokens[1..])?;
            if let Some(close) = rest.first() {
                if close.kind == Fts5QueryTokenKind::RParen {
                    return Ok((expr, &rest[1..]));
                }
            }
            Err(Fts5QueryError::UnbalancedParentheses)
        }
        Fts5QueryTokenKind::Caret => {
            let (inner, rest) = parse_primary(&tokens[1..])?;
            Ok((Fts5Expr::InitialToken(Box::new(inner)), rest))
        }
        Fts5QueryTokenKind::ColumnFilter => {
            let col = token.lexeme.clone();
            let (inner, rest) = parse_primary(&tokens[1..])?;
            Ok((Fts5Expr::ColumnFilter(col, Box::new(inner)), rest))
        }
        Fts5QueryTokenKind::Near => {
            let mut rest = &tokens[1..];
            if !rest
                .first()
                .is_some_and(|t| t.kind == Fts5QueryTokenKind::LParen)
            {
                return Err(Fts5QueryError::InvalidNearSyntax);
            }
            rest = &rest[1..]; // skip (
            let mut operands = Vec::new();
            let mut distance = 10u32; // default NEAR distance
            let mut expect_distance = false;

            while let Some(t) = rest.first() {
                if t.kind == Fts5QueryTokenKind::And {
                    rest = &rest[1..];
                    continue;
                }

                if t.kind == Fts5QueryTokenKind::RParen {
                    if expect_distance || operands.len() < 2 {
                        return Err(Fts5QueryError::InvalidNearSyntax);
                    }
                    rest = &rest[1..];
                    break;
                }

                if expect_distance {
                    if t.kind != Fts5QueryTokenKind::Term {
                        return Err(Fts5QueryError::InvalidNearSyntax);
                    }
                    let lexeme = t.lexeme.trim();
                    if lexeme.is_empty() {
                        return Err(Fts5QueryError::InvalidNearSyntax);
                    }
                    let raw_distance = lexeme.strip_prefix(',').unwrap_or(lexeme);
                    distance = raw_distance
                        .parse::<u32>()
                        .map_err(|_| Fts5QueryError::InvalidNearSyntax)?;
                    expect_distance = false;
                    rest = &rest[1..];
                    continue;
                }

                if t.kind == Fts5QueryTokenKind::Term {
                    let lexeme = t.lexeme.trim();
                    if lexeme.is_empty() {
                        return Err(Fts5QueryError::InvalidNearSyntax);
                    }

                    if lexeme == "," {
                        if operands.len() < 2 {
                            return Err(Fts5QueryError::InvalidNearSyntax);
                        }
                        expect_distance = true;
                        rest = &rest[1..];
                        continue;
                    }

                    if let Some((raw_term, raw_distance)) = lexeme.split_once(',') {
                        let term = raw_term.trim();
                        let trailing = raw_distance.trim();

                        if term.is_empty() {
                            if operands.len() < 2 || trailing.is_empty() {
                                return Err(Fts5QueryError::InvalidNearSyntax);
                            }
                            distance = trailing
                                .parse::<u32>()
                                .map_err(|_| Fts5QueryError::InvalidNearSyntax)?;
                        } else {
                            operands.push(Fts5NearOperand::Term(term.to_owned()));
                            if trailing.is_empty() {
                                expect_distance = true;
                            } else {
                                distance = trailing
                                    .parse::<u32>()
                                    .map_err(|_| Fts5QueryError::InvalidNearSyntax)?;
                            }
                        }
                        rest = &rest[1..];
                        continue;
                    }
                }

                let Some(operand) = near_operand_from_token(t) else {
                    return Err(Fts5QueryError::InvalidNearSyntax);
                };
                operands.push(operand);
                rest = &rest[1..];
            }

            if expect_distance || operands.len() < 2 {
                return Err(Fts5QueryError::InvalidNearSyntax);
            }

            Ok((Fts5Expr::Near(operands, distance), rest))
        }
        _ => Err(Fts5QueryError::EmptyQuery),
    }
}

// ---------------------------------------------------------------------------
// Inverted index
// ---------------------------------------------------------------------------

/// A posting in the inverted index.
type Positions = SmallVec<[u32; 4]>;
type PostingList = SmallVec<[Posting; 1]>;

#[derive(Debug, Clone)]
pub struct Posting {
    pub docid: i64,
    pub column: u32,
    pub positions: Positions,
}

/// Reverse map: for one document, the exact set of index keys it contributed
/// postings to, so [`InvertedIndex::remove_document`] can prune only that
/// document's own terms instead of scanning the entire index.
///
/// `terms` holds the main-index keys (already normalized through
/// `index_key`). `prefix_terms` holds, per prefix length, the prefix keys this
/// document contributed to that prefix index. Both are deduplicated sets
/// because a single document contributes the same term/prefix once per
/// (column, position) occurrence but stores it under one map key.
#[derive(Debug, Clone, Default)]
struct DocTermRefs {
    terms: HashSet<SmallText>,
    prefix_terms: HashMap<usize, HashSet<SmallText>>,
}

/// In-memory inverted index for FTS5.
#[derive(Debug, Clone)]
pub struct InvertedIndex {
    /// term -> list of postings
    index: HashMap<SmallText, PostingList>,
    /// prefix length -> (prefix term -> list of postings)
    prefix_indexes: HashMap<usize, HashMap<SmallText, PostingList>>,
    /// How much positional detail is retained for each posting.
    detail: DetailMode,
    /// Whether terms may carry token-data suffixes after the first NUL byte.
    tokendata: bool,
    /// Set of rowids currently present in the index.
    doc_ids: HashSet<i64>,
    /// Total token count per document (for BM25 avgdl)
    doc_lengths: Option<HashMap<i64, u32>>,
    /// docid -> the index keys that document contributed to. Maintained on
    /// every insert so `remove_document` is O(terms-in-doc) rather than
    /// O(total-index-size).
    doc_terms: HashMap<i64, DocTermRefs>,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::with_options(true, &[], DetailMode::Full)
    }

    #[must_use]
    pub fn with_column_sizes(track_column_sizes: bool) -> Self {
        Self::with_options(track_column_sizes, &[], DetailMode::Full)
    }

    #[must_use]
    pub fn with_options(
        track_column_sizes: bool,
        prefix_lengths: &[usize],
        detail: DetailMode,
    ) -> Self {
        Self::with_options_and_tokendata(track_column_sizes, prefix_lengths, detail, false)
    }

    #[must_use]
    pub fn with_options_and_tokendata(
        track_column_sizes: bool,
        prefix_lengths: &[usize],
        detail: DetailMode,
        tokendata: bool,
    ) -> Self {
        Self {
            index: HashMap::new(),
            prefix_indexes: prefix_lengths
                .iter()
                .copied()
                .map(|prefix_length| (prefix_length, HashMap::new()))
                .collect(),
            detail,
            tokendata,
            doc_ids: HashSet::new(),
            doc_lengths: track_column_sizes.then(HashMap::new),
            doc_terms: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn tracks_column_sizes(&self) -> bool {
        self.doc_lengths.is_some()
    }

    #[must_use]
    pub fn tracks_prefix_length(&self, prefix_length: usize) -> bool {
        self.prefix_indexes.contains_key(&prefix_length)
    }

    #[must_use]
    pub const fn detail_mode(&self) -> DetailMode {
        self.detail
    }

    #[must_use]
    pub const fn tokendata_enabled(&self) -> bool {
        self.tokendata
    }

    fn index_key<'a>(&self, term: &'a str) -> &'a str {
        if self.tokendata {
            tokendata_query_key(term)
        } else {
            term
        }
    }

    // `SmallText` carries a `OnceLock` for lazy long-string interning, which
    // trips `mutable_key_type` at the `HashSet<SmallText>` reverse-map sites
    // even though its `Hash`/`Eq` are content-only and never observe the
    // single-write interior mutability (same rationale as the
    // `HashMap<SmallText, _>` sites elsewhere in this crate).
    #[allow(clippy::mutable_key_type)]
    fn append_position(&mut self, term: &str, docid: i64, column: u32, position: u32) {
        let term = self.index_key(term);
        let stored_column = if self.detail == DetailMode::None {
            0
        } else {
            column
        };
        let stored_position = if self.detail == DetailMode::Full {
            position
        } else {
            0
        };
        let doc_refs = self.doc_terms.entry(docid).or_default();
        append_position_to_postings(
            self.index.entry(SmallText::from(term)).or_default(),
            docid,
            stored_column,
            stored_position,
        );
        // Record the exact main-index key this document touched so removal can
        // target it directly. `HashSet::insert` collapses the repeated
        // (column, position) occurrences of the same term to a single key.
        if !doc_refs.terms.contains(term) {
            doc_refs.terms.insert(SmallText::from(term));
        }

        for (prefix_length, prefix_index) in &mut self.prefix_indexes {
            if let Some(prefix) = prefix_slice(term, *prefix_length) {
                append_position_to_postings(
                    prefix_index.entry(SmallText::from(prefix)).or_default(),
                    docid,
                    stored_column,
                    stored_position,
                );
                let prefix_refs = doc_refs.prefix_terms.entry(*prefix_length).or_default();
                if !prefix_refs.contains(prefix) {
                    prefix_refs.insert(SmallText::from(prefix));
                }
            }
        }
    }

    /// Replay one persisted doclist entry (read back from on-disk segments) into
    /// this index, reproducing the postings `add_document` would have created for
    /// the originally-indexed positions. Used to hydrate a reopened CONTENTLESS
    /// table's in-memory index from its persisted segments before mutation —
    /// contentless tables keep no `_content` rows to re-tokenize, so the postings
    /// are recovered directly from the segment poslists. `term` is the raw token
    /// (the stored index marker already stripped); `append_position` re-applies
    /// the marker and maintains the reverse map and prefix indexes.
    fn replay_persisted_entry(&mut self, term: &str, docid: i64, poslist: &Fts5Poslist) {
        self.doc_ids.insert(docid);
        if poslist.columns.is_empty() {
            // detail=none (or a positionless posting): record presence only.
            self.append_position(term, docid, 0, 0);
            return;
        }
        for column in &poslist.columns {
            if column.offsets.is_empty() {
                self.append_position(term, docid, column.column, 0);
            } else {
                for &position in &column.offsets {
                    self.append_position(term, docid, column.column, position);
                }
            }
        }
    }

    /// Index a document's tokens for a given column.
    pub fn add_document(&mut self, docid: i64, column: u32, tokens: &[Fts5Token]) {
        self.doc_ids.insert(docid);
        // Build term -> positions map for this document+column.
        let mut term_positions: HashMap<&str, Positions> = HashMap::new();
        #[allow(clippy::cast_possible_truncation)]
        for (pos, token) in tokens.iter().enumerate() {
            term_positions
                .entry(&token.term)
                .or_default()
                .push(pos as u32);
        }

        for (term, positions) in term_positions {
            for position in positions {
                self.append_position(term, docid, column, position);
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        let new_len = tokens.len() as u32;
        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            *doc_lengths.entry(docid).or_insert(0) += new_len;
        }
    }

    /// Index a raw text value directly from a tokenizer, avoiding a temporary
    /// `Vec<Fts5Token>` on hot ingest paths.
    pub fn add_text(&mut self, docid: i64, column: u32, tokenizer: &dyn Fts5Tokenizer, text: &str) {
        self.doc_ids.insert(docid);
        let mut token_count = 0_u32;
        tokenizer.visit_tokens(text, &mut |term, _start, _end, _| {
            let position = token_count;
            token_count = token_count.saturating_add(1);
            self.append_position(term, docid, column, position);
        });

        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            *doc_lengths.entry(docid).or_insert(0) += token_count;
        }
    }

    /// Remove a document from the index.
    ///
    /// Uses the `doc_terms` reverse map to touch only the index keys this
    /// document actually contributed to — O(distinct-terms-in-doc) — instead
    /// of scanning every posting list in the index (the old O(index-size)
    /// behavior, quadratic across an incremental re-index / upsert workload).
    ///
    /// The result is byte-identical to the old full-scan removal: a key the
    /// document never touched cannot have its posting list emptied by removing
    /// the document, so restricting the scan to the recorded keys produces the
    /// same pruning decisions and the same surviving postings.
    // `SmallText` interior mutability (lazy interning `OnceLock`) is never
    // observed through `Hash`/`Eq`; the reverse-map `HashSet<SmallText>` keys
    // are content-addressed and stable. See `append_position`.
    #[allow(clippy::mutable_key_type)]
    pub fn remove_document(&mut self, docid: i64) {
        if let Some(doc_refs) = self.doc_terms.remove(&docid) {
            for term in &doc_refs.terms {
                if let Some(postings) = self.index.get_mut(term) {
                    postings.retain(|posting| posting.docid != docid);
                    if postings.is_empty() {
                        self.index.remove(term);
                    }
                }
            }
            for (prefix_length, prefix_keys) in &doc_refs.prefix_terms {
                if let Some(prefix_index) = self.prefix_indexes.get_mut(prefix_length) {
                    for prefix in prefix_keys {
                        if let Some(postings) = prefix_index.get_mut(prefix) {
                            postings.retain(|posting| posting.docid != docid);
                            if postings.is_empty() {
                                prefix_index.remove(prefix);
                            }
                        }
                    }
                }
            }
        }
        self.doc_ids.remove(&docid);
        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            doc_lengths.remove(&docid);
        }
    }

    /// Look up postings for a term.
    #[must_use]
    pub fn get_postings(&self, term: &str) -> &[Posting] {
        self.index
            .get(self.index_key(term))
            .map_or(&[], SmallVec::as_slice)
    }

    /// Look up postings for terms matching a prefix.
    #[must_use]
    pub fn get_prefix_postings(&self, prefix: &str) -> Vec<&Posting> {
        let prefix = self.index_key(prefix);
        if let Some(prefix_index) = self.prefix_indexes.get(&prefix.chars().count()) {
            return prefix_index
                .get(prefix)
                .map_or_else(Vec::new, |postings| postings.iter().collect());
        }

        let mut result = Vec::new();
        for (term, postings) in &self.index {
            if term.starts_with(prefix) {
                result.extend(postings);
            }
        }
        result
    }

    /// Get the number of documents containing a term.
    #[must_use]
    pub fn doc_frequency(&self, term: &str) -> u64 {
        let postings = self.get_postings(term);
        let mut unique_docs: Vec<i64> = postings.iter().map(|p| p.docid).collect();
        unique_docs.sort_unstable();
        unique_docs.dedup();
        u64::try_from(unique_docs.len()).unwrap_or(u64::MAX)
    }

    /// Get term frequency for a term in a specific document.
    #[must_use]
    pub fn term_frequency(&self, term: &str, docid: i64) -> u32 {
        self.get_postings(term)
            .iter()
            .filter(|p| p.docid == docid)
            .map(|p| u32::try_from(p.positions.len()).unwrap_or(u32::MAX))
            .sum()
    }

    /// Get total document count.
    #[must_use]
    pub fn total_docs(&self) -> u64 {
        u64::try_from(self.doc_ids.len()).unwrap_or(u64::MAX)
    }

    /// Get average document length.
    #[must_use]
    pub fn avg_doc_length(&self) -> f64 {
        if let Some(doc_lengths) = self.doc_lengths.as_ref() {
            if doc_lengths.is_empty() {
                return 0.0;
            }
            let total: u64 = doc_lengths.values().map(|v| u64::from(*v)).sum();
            return total as f64 / doc_lengths.len() as f64;
        }

        if self.doc_ids.is_empty() {
            return 0.0;
        }
        let total: u64 = self
            .doc_ids
            .iter()
            .map(|docid| u64::from(self.doc_length(*docid)))
            .sum();
        total as f64 / self.doc_ids.len() as f64
    }

    /// Get a specific document's length.
    #[must_use]
    pub fn doc_length(&self, docid: i64) -> u32 {
        if let Some(doc_lengths) = self.doc_lengths.as_ref() {
            return doc_lengths.get(&docid).copied().unwrap_or(0);
        }

        self.index
            .values()
            .flat_map(|postings| postings.iter())
            .filter(|posting| posting.docid == docid)
            .map(|posting| u32::try_from(posting.positions.len()).unwrap_or(u32::MAX))
            .sum()
    }

    #[must_use]
    pub fn docsize_row(&self, docid: i64, column_count: usize) -> Fts5DocsizeRow {
        let mut counts = vec![0_u32; column_count];
        for posting in self
            .index
            .values()
            .flat_map(|postings| postings.iter())
            .filter(|posting| posting.docid == docid)
        {
            let Ok(column) = usize::try_from(posting.column) else {
                continue;
            };
            if let Some(count) = counts.get_mut(column) {
                *count = count
                    .saturating_add(u32::try_from(posting.positions.len()).unwrap_or(u32::MAX));
            }
        }
        Fts5DocsizeRow::new(docid, counts)
    }

    /// Compute the per-column docsize row for *every* document in a single
    /// O(postings) sweep, instead of re-scanning the whole index once per
    /// document.
    ///
    /// [`Self::docsize_row`] answers one document by filtering every posting,
    /// which is O(postings). Calling it once per document — as
    /// `Fts5Table::encode_docsize_rows` did — is O(documents × postings),
    /// i.e. quadratic in corpus size, and was the dominant cost in a full FTS
    /// rebuild: it wedged `cass index --full` on a single CPU core above
    /// ~15-30 MB of indexed content (cass#301). This sweeps the postings once,
    /// accumulating per-`(docid, column)` token counts into a map.
    ///
    /// `extra_docids` seeds documents that exist outside the inverted index
    /// (e.g. the table's content rowids); together with every indexed
    /// `doc_id` this guarantees documents with no postings still emit a
    /// zero-count row. Output is sorted by rowid to preserve the previous
    /// deterministic ordering and is row-for-row identical to mapping
    /// [`Self::docsize_row`] over the same docid set.
    #[must_use]
    pub fn accumulate_docsize_rows<I>(
        &self,
        column_count: usize,
        extra_docids: I,
    ) -> Vec<Fts5DocsizeRow>
    where
        I: IntoIterator<Item = i64>,
    {
        let mut counts_by_doc: HashMap<i64, Vec<u32>> = HashMap::new();
        for docid in self.doc_ids.iter().copied().chain(extra_docids) {
            counts_by_doc
                .entry(docid)
                .or_insert_with(|| vec![0_u32; column_count]);
        }
        for postings in self.index.values() {
            for posting in postings {
                let Ok(column) = usize::try_from(posting.column) else {
                    continue;
                };
                if let Some(counts) = counts_by_doc.get_mut(&posting.docid)
                    && let Some(count) = counts.get_mut(column)
                {
                    *count = count
                        .saturating_add(u32::try_from(posting.positions.len()).unwrap_or(u32::MAX));
                }
            }
        }
        let mut rows: Vec<Fts5DocsizeRow> = counts_by_doc
            .into_iter()
            .map(|(rowid, counts)| Fts5DocsizeRow::new(rowid, counts))
            .collect();
        rows.sort_unstable_by_key(|row| row.rowid);
        rows
    }

    pub fn apply_docsize_row(&mut self, row: &Fts5DocsizeRow) {
        self.doc_ids.insert(row.rowid);
        if let Some(doc_lengths) = self.doc_lengths.as_mut() {
            doc_lengths.insert(row.rowid, row.total_tokens());
        }
    }

    pub fn build_pending_hash(&self, prefix_lengths: &[usize]) -> Result<Fts5PendingHash> {
        let mut normalized_prefix_lengths = prefix_lengths.to_vec();
        normalized_prefix_lengths.sort_unstable();
        normalized_prefix_lengths.dedup();
        let mut pending = Fts5PendingHash::new(
            &normalized_prefix_lengths,
            self.detail_mode(),
            self.tokendata_enabled(),
        );

        for (term, postings) in &self.index {
            for posting in postings {
                pending.push_index_posting(Fts5PendingIndex::Main, term, posting)?;
                for (ordinal, prefix_length) in
                    normalized_prefix_lengths.iter().copied().enumerate()
                {
                    if let Some(prefix) = prefix_slice(term, prefix_length)
                        && let Ok(ordinal) = u8::try_from(ordinal)
                    {
                        pending.push_index_posting(
                            Fts5PendingIndex::Prefix {
                                ordinal,
                                length: prefix_length,
                            },
                            prefix,
                            posting,
                        )?;
                    }
                }
            }
        }

        pending.row_count = self.doc_ids.len();
        Ok(pending)
    }
}

fn append_position_to_postings(postings: &mut PostingList, docid: i64, column: u32, position: u32) {
    if let Some(last_posting) = postings.last_mut()
        && last_posting.docid == docid
        && last_posting.column == column
    {
        last_posting.positions.push(position);
    } else {
        let mut positions = Positions::new();
        positions.push(position);
        postings.push(Posting {
            docid,
            column,
            positions,
        });
    }
}

fn prefix_slice(term: &str, prefix_length: usize) -> Option<&str> {
    if prefix_length == 0 {
        return None;
    }

    let mut chars = term.char_indices();
    for idx in 0..prefix_length {
        let (offset, ch) = chars.next()?;
        if idx + 1 == prefix_length {
            return Some(&term[..offset + ch.len_utf8()]);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// BM25 ranking
// ---------------------------------------------------------------------------

/// Standard BM25 parameters.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
const BM25_IDF_FLOOR: f64 = 1.0e-6;

/// Compute BM25 score for a document against a set of query terms.
///
/// Lower values mean better matches (following SQLite FTS5 convention where
/// `rank` returns negative BM25 scores).
#[must_use]
#[allow(clippy::similar_names)]
pub fn bm25_score(
    index: &InvertedIndex,
    docid: i64,
    query_terms: &[String],
    weights: &[f64],
) -> f64 {
    let n = index.total_docs() as f64;
    let avgdl = index.avg_doc_length();
    let dl = f64::from(index.doc_length(docid));

    let mut score = 0.0;

    for term in query_terms {
        let df_int = index.doc_frequency(term);
        if df_int == 0 {
            continue;
        }
        let df = df_int as f64;

        let idf = ((n - df + 0.5) / (df + 0.5)).ln().max(BM25_IDF_FLOOR);

        let mut weighted_tf = 0.0;
        let postings = index.get_postings(term);
        for posting in postings {
            if posting.docid != docid {
                continue;
            }
            let tf = posting.positions.len() as f64;
            let col_weight = usize::try_from(posting.column)
                .ok()
                .and_then(|column| weights.get(column).copied())
                .unwrap_or(1.0);
            weighted_tf += col_weight * tf;
        }

        if weighted_tf == 0.0 {
            continue;
        }

        let denom = if avgdl > 0.0 {
            BM25_K1.mul_add(1.0 - BM25_B + BM25_B * dl / avgdl, weighted_tf)
        } else {
            weighted_tf + BM25_K1
        };

        score += idf * (weighted_tf * (BM25_K1 + 1.0)) / denom;
    }

    // Return negative score (lower = better, SQLite FTS5 convention).
    -score
}

// ---------------------------------------------------------------------------
// Query evaluation against index
// ---------------------------------------------------------------------------

/// Evaluate an FTS5 expression against the inverted index, returning
/// matching document IDs.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_expr(index: &InvertedIndex, expr: &Fts5Expr) -> Vec<i64> {
    evaluate_expr_impl(index, expr, &[], None)
}

fn evaluate_expr_for_columns(
    index: &InvertedIndex,
    expr: &Fts5Expr,
    columns: &[String],
) -> Vec<i64> {
    evaluate_expr_impl(index, expr, columns, None)
}

fn tokenize_query_leaf(tokenizer: &dyn Fts5Tokenizer, text: &str) -> Vec<String> {
    tokenizer
        .tokenize(text)
        .into_iter()
        .map(|token| token.term)
        .collect()
}

fn normalize_query_term_expr(term: String, tokenizer: &dyn Fts5Tokenizer) -> Fts5Expr {
    let mut terms = tokenize_query_leaf(tokenizer, &term).into_iter();
    let Some(first) = terms.next() else {
        return Fts5Expr::Term(term.to_lowercase());
    };

    terms.fold(Fts5Expr::Term(first), |left, right| {
        Fts5Expr::And(Box::new(left), Box::new(Fts5Expr::Term(right)))
    })
}

fn normalize_query_phrase_terms(phrase: &[String], tokenizer: &dyn Fts5Tokenizer) -> Vec<String> {
    let joined = phrase.join(" ");
    let terms = tokenize_query_leaf(tokenizer, &joined);
    if terms.is_empty() {
        phrase.iter().map(|term| term.to_lowercase()).collect()
    } else {
        terms
    }
}

fn normalize_near_operand_with_tokenizer(
    operand: Fts5NearOperand,
    tokenizer: &dyn Fts5Tokenizer,
) -> Fts5NearOperand {
    match operand {
        Fts5NearOperand::Term(term) => {
            let terms = tokenize_query_leaf(tokenizer, &term);
            match terms.as_slice() {
                [] => Fts5NearOperand::Term(term.to_lowercase()),
                [single] => Fts5NearOperand::Term(single.clone()),
                _ => Fts5NearOperand::Phrase(terms),
            }
        }
        Fts5NearOperand::Prefix(prefix) => {
            let terms = tokenize_query_leaf(tokenizer, &prefix);
            let normalized = terms
                .first()
                .cloned()
                .unwrap_or_else(|| prefix.to_lowercase());
            Fts5NearOperand::Prefix(normalized)
        }
        Fts5NearOperand::Phrase(words) => {
            Fts5NearOperand::Phrase(normalize_query_phrase_terms(&words, tokenizer))
        }
    }
}

fn normalize_query_expr_with_tokenizer(expr: Fts5Expr, tokenizer: &dyn Fts5Tokenizer) -> Fts5Expr {
    match expr {
        Fts5Expr::Term(term) => normalize_query_term_expr(term, tokenizer),
        Fts5Expr::Prefix(prefix) => {
            let terms = tokenize_query_leaf(tokenizer, &prefix);
            let normalized = terms
                .first()
                .cloned()
                .unwrap_or_else(|| prefix.to_lowercase());
            Fts5Expr::Prefix(normalized)
        }
        Fts5Expr::Phrase(words) => {
            Fts5Expr::Phrase(normalize_query_phrase_terms(&words, tokenizer))
        }
        Fts5Expr::PhrasePrefix(words, prefix) => {
            let normalized_words = normalize_query_phrase_terms(&words, tokenizer);
            let terms = tokenize_query_leaf(tokenizer, &prefix);
            let normalized_prefix = terms
                .first()
                .cloned()
                .unwrap_or_else(|| prefix.to_lowercase());
            Fts5Expr::PhrasePrefix(normalized_words, normalized_prefix)
        }
        Fts5Expr::And(left, right) => Fts5Expr::And(
            Box::new(normalize_query_expr_with_tokenizer(*left, tokenizer)),
            Box::new(normalize_query_expr_with_tokenizer(*right, tokenizer)),
        ),
        Fts5Expr::Or(left, right) => Fts5Expr::Or(
            Box::new(normalize_query_expr_with_tokenizer(*left, tokenizer)),
            Box::new(normalize_query_expr_with_tokenizer(*right, tokenizer)),
        ),
        Fts5Expr::Not(left, right) => Fts5Expr::Not(
            Box::new(normalize_query_expr_with_tokenizer(*left, tokenizer)),
            Box::new(normalize_query_expr_with_tokenizer(*right, tokenizer)),
        ),
        Fts5Expr::Near(operands, distance) => Fts5Expr::Near(
            operands
                .into_iter()
                .map(|operand| normalize_near_operand_with_tokenizer(operand, tokenizer))
                .collect(),
            distance,
        ),
        Fts5Expr::ColumnFilter(column_name, inner) => Fts5Expr::ColumnFilter(
            column_name,
            Box::new(normalize_query_expr_with_tokenizer(*inner, tokenizer)),
        ),
        Fts5Expr::InitialToken(inner) => Fts5Expr::InitialToken(Box::new(
            normalize_query_expr_with_tokenizer(*inner, tokenizer),
        )),
    }
}

fn evaluate_query_string(
    index: &InvertedIndex,
    columns: &[String],
    query: &str,
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<(Vec<i64>, Vec<String>), Fts5QueryError> {
    let tokens = parse_fts5_query(query)?;
    let mut expr = build_expr(&tokens)?;
    if let Some(tokenizer) = tokenizer {
        expr = normalize_query_expr_with_tokenizer(expr, tokenizer);
    }
    validate_detail_mode(&expr, index.detail_mode())?;
    validate_column_filters(&expr, columns)?;
    let matching_docs = evaluate_expr_for_columns(index, &expr, columns);
    let query_terms = extract_query_terms(&expr);
    Ok((matching_docs, query_terms))
}

fn evaluate_query_strings(
    index: &InvertedIndex,
    columns: &[String],
    queries: &[&str],
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<(Vec<i64>, Vec<String>), Fts5QueryError> {
    let mut combined_docs: Option<Vec<i64>> = None;
    let mut query_terms = Vec::new();

    for query in queries {
        let (matching_docs, match_terms) = evaluate_query_string(index, columns, query, tokenizer)?;
        query_terms.extend(match_terms);
        combined_docs = Some(match combined_docs {
            Some(existing) => intersect_sorted(&existing, &matching_docs),
            None => matching_docs,
        });
    }

    Ok((combined_docs.unwrap_or_default(), query_terms))
}

fn query_terms_for_query_strings(
    columns: &[String],
    queries: &[&str],
    tokenizer: &dyn Fts5Tokenizer,
    detail: DetailMode,
) -> std::result::Result<Vec<String>, Fts5QueryError> {
    let mut query_terms = Vec::new();
    for query in queries {
        let tokens = parse_fts5_query(query)?;
        let expr = normalize_query_expr_with_tokenizer(build_expr(&tokens)?, tokenizer);
        validate_detail_mode(&expr, detail)?;
        validate_column_filters(&expr, columns)?;
        query_terms.extend(extract_query_terms(&expr));
    }
    Ok(query_terms)
}

fn search_rows_with_weights_from_parts(
    index: &InvertedIndex,
    columns: &[String],
    documents: &HashMap<i64, Vec<String>>,
    queries: &[&str],
    weights: &[f64],
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
    let (matching_docs, query_terms) = evaluate_query_strings(index, columns, queries, tokenizer)?;

    let mut results: Vec<(i64, f64, Vec<String>)> = matching_docs
        .into_iter()
        .map(|docid| {
            let score = bm25_score(index, docid, &query_terms, weights);
            let row = documents.get(&docid).cloned().unwrap_or_default();
            (docid, score, row)
        })
        .collect();

    results.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(results)
}

fn search_docids_with_weights_from_parts(
    index: &InvertedIndex,
    columns: &[String],
    queries: &[&str],
    weights: &[f64],
    tokenizer: Option<&dyn Fts5Tokenizer>,
) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
    let (matching_docs, query_terms) = evaluate_query_strings(index, columns, queries, tokenizer)?;

    let mut results: Vec<(i64, f64)> = matching_docs
        .into_iter()
        .map(|docid| (docid, bm25_score(index, docid, &query_terms, weights)))
        .collect();

    results.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(results)
}

fn evaluate_expr_impl(
    index: &InvertedIndex,
    expr: &Fts5Expr,
    columns: &[String],
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    match expr {
        Fts5Expr::Term(term) => {
            let mut docs: Vec<i64> = index
                .get_postings(term)
                .iter()
                .filter(|posting| posting_matches_allowed_columns(posting.column, allowed_columns))
                .map(|p| p.docid)
                .collect();
            docs.sort_unstable();
            docs.dedup();
            docs
        }
        Fts5Expr::Prefix(prefix) => {
            let mut docs: Vec<i64> = index
                .get_prefix_postings(prefix)
                .iter()
                .filter(|posting| posting_matches_allowed_columns(posting.column, allowed_columns))
                .map(|p| p.docid)
                .collect();
            docs.sort_unstable();
            docs.dedup();
            docs
        }
        Fts5Expr::Phrase(words) => evaluate_phrase(index, words, allowed_columns),
        Fts5Expr::And(left, right) => {
            let left_docs = evaluate_expr_impl(index, left, columns, allowed_columns);
            let right_docs = evaluate_expr_impl(index, right, columns, allowed_columns);
            intersect_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Or(left, right) => {
            let left_docs = evaluate_expr_impl(index, left, columns, allowed_columns);
            let right_docs = evaluate_expr_impl(index, right, columns, allowed_columns);
            union_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Not(left, right) => {
            let left_docs = evaluate_expr_impl(index, left, columns, allowed_columns);
            let right_docs = evaluate_expr_impl(index, right, columns, allowed_columns);
            difference_sorted(&left_docs, &right_docs)
        }
        Fts5Expr::Near(terms, distance) => evaluate_near(index, terms, *distance, allowed_columns),
        Fts5Expr::PhrasePrefix(words, prefix) => {
            evaluate_phrase_prefix(index, words, prefix, allowed_columns)
        }
        Fts5Expr::ColumnFilter(column_filter, inner) => {
            let Some(filter_spec) = parse_column_filter_spec(column_filter) else {
                return Vec::new();
            };
            let Some(resolved_columns) = resolve_column_filter_columns(columns, &filter_spec)
            else {
                return Vec::new();
            };
            let combined_columns = combine_allowed_columns(allowed_columns, &resolved_columns);
            evaluate_expr_impl(index, inner, columns, Some(combined_columns.as_slice()))
        }
        Fts5Expr::InitialToken(inner) => {
            // For initial token (^), filter results to those where the term/phrase
            // appears at position 0.
            match inner.as_ref() {
                Fts5Expr::Term(term) => {
                    let mut docs: Vec<i64> = index
                        .get_postings(term)
                        .iter()
                        .filter(|posting| {
                            posting.positions.contains(&0)
                                && posting_matches_allowed_columns(posting.column, allowed_columns)
                        })
                        .map(|p| p.docid)
                        .collect();
                    docs.sort_unstable();
                    docs.dedup();
                    docs
                }
                Fts5Expr::Prefix(prefix) => {
                    let mut docs: Vec<i64> = index
                        .get_prefix_postings(prefix)
                        .iter()
                        .filter(|posting| {
                            posting.positions.contains(&0)
                                && posting_matches_allowed_columns(posting.column, allowed_columns)
                        })
                        .map(|p| p.docid)
                        .collect();
                    docs.sort_unstable();
                    docs.dedup();
                    docs
                }
                Fts5Expr::Phrase(words) => {
                    // Evaluate phrase but require the first word to be at position 0.
                    if words.is_empty() {
                        return Vec::new();
                    }
                    let first_postings = index.get_postings(&words[0]);
                    let mut result = Vec::new();

                    for first_p in first_postings {
                        // Optimization: if first word isn't at 0, this doc can't match ^phrase.
                        if !first_p.positions.contains(&0)
                            || !posting_matches_allowed_columns(first_p.column, allowed_columns)
                        {
                            continue;
                        }

                        // Check subsequent words
                        let mut match_found = true;
                        for (offset, word) in words.iter().enumerate().skip(1) {
                            #[allow(clippy::cast_possible_truncation)]
                            let target_pos = offset as u32; // implied start_pos = 0
                            let found = index.get_postings(word).iter().any(|p| {
                                p.docid == first_p.docid
                                    && p.column == first_p.column
                                    && p.positions.contains(&target_pos)
                            });
                            if !found {
                                match_found = false;
                                break;
                            }
                        }
                        if match_found && !result.contains(&first_p.docid) {
                            result.push(first_p.docid);
                        }
                    }
                    result.sort_unstable();
                    result
                }
                Fts5Expr::PhrasePrefix(words, prefix) => {
                    let mut docs: Vec<i64> =
                        phrase_prefix_spans(index, words, prefix, allowed_columns)
                            .into_iter()
                            .filter(|span| span.start == 0)
                            .map(|span| span.docid)
                            .collect();
                    docs.sort_unstable();
                    docs.dedup();
                    docs
                }
                _ => evaluate_expr_impl(index, inner, columns, allowed_columns),
            }
        }
    }
}

fn posting_matches_allowed_columns(column: u32, allowed_columns: Option<&[u32]>) -> bool {
    match allowed_columns {
        Some(allowed) => allowed.contains(&column),
        None => true,
    }
}

fn resolve_allowed_columns(columns: &[String], column_name: &str) -> Vec<u32> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(idx, candidate)| {
            if candidate.eq_ignore_ascii_case(column_name) {
                u32::try_from(idx).ok()
            } else {
                None
            }
        })
        .collect()
}

fn resolve_column_filter_columns(
    columns: &[String],
    filter_spec: &Fts5ColumnFilterSpec,
) -> Option<Vec<u32>> {
    let mut selected = Vec::with_capacity(filter_spec.columns.len());
    for column_name in &filter_spec.columns {
        let resolved = resolve_allowed_columns(columns, column_name);
        if resolved.is_empty() {
            return None;
        }
        selected.extend(resolved);
    }
    selected.sort_unstable();
    selected.dedup();

    if !filter_spec.exclude {
        return Some(selected);
    }

    let mut allowed = Vec::with_capacity(columns.len().saturating_sub(selected.len()));
    for idx in 0..columns.len() {
        let Ok(column_id) = u32::try_from(idx) else {
            continue;
        };
        if !selected.contains(&column_id) {
            allowed.push(column_id);
        }
    }
    Some(allowed)
}

fn combine_allowed_columns(existing: Option<&[u32]>, resolved: &[u32]) -> Vec<u32> {
    match existing {
        Some(existing) => existing
            .iter()
            .copied()
            .filter(|column| resolved.contains(column))
            .collect(),
        None => resolved.to_vec(),
    }
}

fn validate_column_filters(
    expr: &Fts5Expr,
    columns: &[String],
) -> std::result::Result<(), Fts5QueryError> {
    match expr {
        Fts5Expr::And(left, right) | Fts5Expr::Or(left, right) | Fts5Expr::Not(left, right) => {
            validate_column_filters(left, columns)?;
            validate_column_filters(right, columns)
        }
        Fts5Expr::ColumnFilter(column_filter, inner) => {
            let Some(filter_spec) = parse_column_filter_spec(column_filter) else {
                return Err(Fts5QueryError::InvalidColumnFilter(column_filter.clone()));
            };
            for column_name in &filter_spec.columns {
                if resolve_allowed_columns(columns, column_name).is_empty() {
                    return Err(Fts5QueryError::InvalidColumnFilter(column_name.clone()));
                }
            }
            validate_column_filters(inner, columns)
        }
        Fts5Expr::InitialToken(inner) => validate_column_filters(inner, columns),
        Fts5Expr::Term(_)
        | Fts5Expr::Prefix(_)
        | Fts5Expr::Phrase(_)
        | Fts5Expr::PhrasePrefix(_, _)
        | Fts5Expr::Near(_, _) => Ok(()),
    }
}

fn validate_detail_mode(
    expr: &Fts5Expr,
    detail: DetailMode,
) -> std::result::Result<(), Fts5QueryError> {
    match expr {
        Fts5Expr::And(left, right) | Fts5Expr::Or(left, right) | Fts5Expr::Not(left, right) => {
            validate_detail_mode(left, detail)?;
            validate_detail_mode(right, detail)
        }
        Fts5Expr::Phrase(_) | Fts5Expr::PhrasePrefix(_, _) => {
            if detail == DetailMode::Full {
                Ok(())
            } else {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "phrase queries",
                })
            }
        }
        Fts5Expr::Near(_, _) => {
            if detail == DetailMode::Full {
                Ok(())
            } else {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "NEAR queries",
                })
            }
        }
        Fts5Expr::InitialToken(inner) => {
            if detail == DetailMode::Full {
                validate_detail_mode(inner, detail)
            } else {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "initial-token queries",
                })
            }
        }
        Fts5Expr::ColumnFilter(_, inner) => {
            if detail == DetailMode::None {
                Err(Fts5QueryError::UnsupportedByDetailMode {
                    detail,
                    feature: "column filters",
                })
            } else {
                validate_detail_mode(inner, detail)
            }
        }
        Fts5Expr::Term(_) | Fts5Expr::Prefix(_) => Ok(()),
    }
}

fn evaluate_phrase(
    index: &InvertedIndex,
    words: &[String],
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    if words.is_empty() {
        return Vec::new();
    }

    // Get postings for first word.
    let first_postings = index.get_postings(&words[0]);
    if first_postings.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();

    // For each document that has the first word, check if subsequent words
    // appear in consecutive positions.
    for first_p in first_postings {
        if !posting_matches_allowed_columns(first_p.column, allowed_columns) {
            continue;
        }
        'positions: for &start_pos in &first_p.positions {
            for (offset, word) in words.iter().enumerate().skip(1) {
                #[allow(clippy::cast_possible_truncation)]
                let target_pos = start_pos + offset as u32; // implied start_pos = 0
                let found = index.get_postings(word).iter().any(|p| {
                    p.docid == first_p.docid
                        && p.column == first_p.column
                        && p.positions.contains(&target_pos)
                });
                if !found {
                    continue 'positions;
                }
            }
            // All words found in consecutive positions.
            if !result.contains(&first_p.docid) {
                result.push(first_p.docid);
            }
        }
    }

    result
}

#[derive(Debug, Clone, Copy)]
struct Fts5NearSpan {
    docid: i64,
    column: u32,
    start: u32,
    end: u32,
}

fn phrase_prefix_spans(
    index: &InvertedIndex,
    words: &[String],
    prefix: &str,
    allowed_columns: Option<&[u32]>,
) -> Vec<Fts5NearSpan> {
    if words.is_empty() {
        return near_operand_spans(
            index,
            &Fts5NearOperand::Prefix(prefix.to_owned()),
            allowed_columns,
        );
    }

    let mut spans = Vec::new();
    for first_p in index.get_postings(&words[0]) {
        if !posting_matches_allowed_columns(first_p.column, allowed_columns) {
            continue;
        }

        'positions: for &start_pos in &first_p.positions {
            for (offset, word) in words.iter().enumerate().skip(1) {
                #[allow(clippy::cast_possible_truncation)]
                let target_pos = start_pos + offset as u32;
                let found = index.get_postings(word).iter().any(|posting| {
                    posting.docid == first_p.docid
                        && posting.column == first_p.column
                        && posting.positions.contains(&target_pos)
                });
                if !found {
                    continue 'positions;
                }
            }

            #[allow(clippy::cast_possible_truncation)]
            let prefix_pos = start_pos + words.len() as u32;
            let prefix_found = index
                .get_prefix_postings(prefix)
                .into_iter()
                .any(|posting| {
                    posting.docid == first_p.docid
                        && posting.column == first_p.column
                        && posting.positions.contains(&prefix_pos)
                });
            if prefix_found {
                spans.push(Fts5NearSpan {
                    docid: first_p.docid,
                    column: first_p.column,
                    start: start_pos,
                    end: prefix_pos,
                });
            }
        }
    }
    spans
}

fn evaluate_phrase_prefix(
    index: &InvertedIndex,
    words: &[String],
    prefix: &str,
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    let mut result: Vec<i64> = phrase_prefix_spans(index, words, prefix, allowed_columns)
        .into_iter()
        .map(|span| span.docid)
        .collect();
    result.sort_unstable();
    result.dedup();
    result
}

fn near_operand_spans(
    index: &InvertedIndex,
    operand: &Fts5NearOperand,
    allowed_columns: Option<&[u32]>,
) -> Vec<Fts5NearSpan> {
    match operand {
        Fts5NearOperand::Term(term) => index
            .get_postings(term)
            .iter()
            .filter(|posting| posting_matches_allowed_columns(posting.column, allowed_columns))
            .flat_map(|posting| {
                posting.positions.iter().map(|position| Fts5NearSpan {
                    docid: posting.docid,
                    column: posting.column,
                    start: *position,
                    end: *position,
                })
            })
            .collect(),
        Fts5NearOperand::Prefix(prefix) => index
            .get_prefix_postings(prefix)
            .into_iter()
            .filter(|posting| posting_matches_allowed_columns(posting.column, allowed_columns))
            .flat_map(|posting| {
                posting.positions.iter().map(|position| Fts5NearSpan {
                    docid: posting.docid,
                    column: posting.column,
                    start: *position,
                    end: *position,
                })
            })
            .collect(),
        Fts5NearOperand::Phrase(words) => phrase_near_spans(index, words, allowed_columns),
    }
}

fn phrase_near_spans(
    index: &InvertedIndex,
    words: &[String],
    allowed_columns: Option<&[u32]>,
) -> Vec<Fts5NearSpan> {
    if words.is_empty() {
        return Vec::new();
    }

    let mut spans = Vec::new();
    for first_p in index.get_postings(&words[0]) {
        if !posting_matches_allowed_columns(first_p.column, allowed_columns) {
            continue;
        }

        'positions: for &start_pos in &first_p.positions {
            for (offset, word) in words.iter().enumerate().skip(1) {
                #[allow(clippy::cast_possible_truncation)]
                let target_pos = start_pos + offset as u32;
                let found = index.get_postings(word).iter().any(|posting| {
                    posting.docid == first_p.docid
                        && posting.column == first_p.column
                        && posting.positions.contains(&target_pos)
                });
                if !found {
                    continue 'positions;
                }
            }

            #[allow(clippy::cast_possible_truncation)]
            spans.push(Fts5NearSpan {
                docid: first_p.docid,
                column: first_p.column,
                start: start_pos,
                end: start_pos + (words.len() - 1) as u32,
            });
        }
    }
    spans
}

fn near_clump_distance(spans: &[Fts5NearSpan]) -> u32 {
    let (Some(min_end), Some(max_start)) = (
        spans.iter().map(|span| span.end).min(),
        spans.iter().map(|span| span.start).max(),
    ) else {
        return 0;
    };
    max_start.saturating_sub(min_end).saturating_sub(1)
}

fn find_near_clump(
    operand_spans: &[Vec<Fts5NearSpan>],
    operand_order: &[usize],
    next_order_index: usize,
    selected: &mut SmallVec<[Fts5NearSpan; 8]>,
    distance: u32,
) -> bool {
    if next_order_index == operand_order.len() {
        return near_clump_distance(selected) <= distance;
    }

    let Some(anchor_span) = selected.first() else {
        return false;
    };
    let docid = anchor_span.docid;
    let column = anchor_span.column;
    let next_operand = operand_order[next_order_index];

    for span in operand_spans[next_operand]
        .iter()
        .filter(|span| span.docid == docid && span.column == column)
    {
        selected.push(*span);
        let matches = near_clump_distance(selected) <= distance
            && find_near_clump(
                operand_spans,
                operand_order,
                next_order_index + 1,
                selected,
                distance,
            );
        selected.pop();
        if matches {
            return true;
        }
    }

    false
}

fn evaluate_near(
    index: &InvertedIndex,
    operands: &[Fts5NearOperand],
    distance: u32,
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    if operands.len() < 2 {
        return Vec::new();
    }

    let operand_spans: Vec<Vec<Fts5NearSpan>> = operands
        .iter()
        .map(|operand| near_operand_spans(index, operand, allowed_columns))
        .collect();
    if operand_spans.iter().any(Vec::is_empty) {
        return Vec::new();
    }

    let mut operand_order: Vec<usize> = (0..operand_spans.len()).collect();
    operand_order.sort_by_key(|&operand_index| operand_spans[operand_index].len());
    let anchor_operand = operand_order[0];
    let mut result = Vec::new();

    for anchor_span in &operand_spans[anchor_operand] {
        let mut selected = SmallVec::new();
        selected.push(*anchor_span);
        if find_near_clump(&operand_spans, &operand_order, 1, &mut selected, distance)
            && !result.contains(&anchor_span.docid)
        {
            result.push(anchor_span.docid);
        }
    }

    result.sort_unstable();
    result
}

fn intersect_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

fn union_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                result.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

fn difference_sorted(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() {
        if j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => {
                    result.push(a[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
            }
        } else {
            result.push(a[i]);
            i += 1;
        }
    }
    result
}

fn shadow_query_storage_error(error: FrankenError) -> Fts5QueryError {
    Fts5QueryError::ShadowStorage(error.to_string())
}

fn rowid_u64_to_i64(rowid: u64) -> Option<i64> {
    i64::try_from(rowid).ok()
}

fn shadow_entry_matches_allowed_columns(
    entry: &Fts5DoclistEntry,
    allowed_columns: Option<&[u32]>,
) -> bool {
    !entry.poslist.delete
        && entry
            .poslist
            .columns
            .iter()
            .any(|column| posting_matches_allowed_columns(column.column, allowed_columns))
}

fn shadow_rowids_from_entries(
    entries: &[Fts5DoclistEntry],
    allowed_columns: Option<&[u32]>,
) -> Vec<i64> {
    let mut rowids: Vec<i64> = entries
        .iter()
        .filter(|entry| shadow_entry_matches_allowed_columns(entry, allowed_columns))
        .filter_map(|entry| rowid_u64_to_i64(entry.rowid))
        .collect();
    rowids.sort_unstable();
    rowids.dedup();
    rowids
}

fn shadow_spans_from_entries(
    entries: &[Fts5DoclistEntry],
    allowed_columns: Option<&[u32]>,
) -> Vec<Fts5NearSpan> {
    let mut spans = Vec::new();
    for entry in entries {
        if entry.poslist.delete {
            continue;
        }
        let Some(rowid) = rowid_u64_to_i64(entry.rowid) else {
            continue;
        };
        for column in &entry.poslist.columns {
            if !posting_matches_allowed_columns(column.column, allowed_columns) {
                continue;
            }
            spans.extend(column.offsets.iter().copied().map(|offset| Fts5NearSpan {
                docid: rowid,
                column: column.column,
                start: offset,
                end: offset,
            }));
        }
    }
    spans.sort_unstable_by_key(|span| (span.docid, span.column, span.start, span.end));
    spans.dedup_by_key(|span| (span.docid, span.column, span.start, span.end));
    spans
}

fn shadow_rowids_from_spans(spans: Vec<Fts5NearSpan>) -> Vec<i64> {
    let mut rowids: Vec<i64> = spans.into_iter().map(|span| span.docid).collect();
    rowids.sort_unstable();
    rowids.dedup();
    rowids
}

fn shadow_content_for_rowid(rows: &Fts5ShadowRows, rowid: i64) -> Option<&[String]> {
    rows.content
        .iter()
        .find(|row| row.rowid == rowid)
        .map(|row| row.values.as_slice())
}

fn segment_lookup_keys(term: &[u8]) -> SmallVec<[Vec<u8>; 2]> {
    let mut keys = SmallVec::new();
    let mut main_key = Vec::with_capacity(term.len().saturating_add(1));
    main_key.push(FTS5_MAIN_PREFIX_BYTE);
    main_key.extend_from_slice(term);
    keys.push(main_key);
    keys.push(term.to_vec());
    keys
}

/// The data-access primitives an FTS5 ranked search needs from a doclist
/// source, plus the full query engine built on top of them as default methods.
///
/// The boolean/phrase/NEAR evaluation, column filtering, and BM25 ranking are
/// IDENTICAL across implementations because they live here once. The in-memory
/// [`Fts5ShadowQuery`] (reads decoded shadow rows) and the lazy on-disk
/// [`Fts5LazyQuery`] (point-reads persisted segments via an
/// [`Fts5OnDiskReader`]) differ ONLY in the primitive implementations, so the
/// two paths produce identical rowids, ordering, and scores — parity by
/// construction, which is what the lazy rework must not break.
pub(crate) trait Fts5DoclistProvider {
    /// Declared column names (length = column count).
    fn columns(&self) -> &[String];

    /// Tokenizer used to normalize query terms.
    fn tokenizer(&self) -> &dyn Fts5Tokenizer;

    /// Detail mode (full/column/none) — governs phrase/NEAR availability.
    fn detail(&self) -> DetailMode;

    /// Doclist entries for an EXACT `term`, FTS5 delete markers removed.
    fn exact_entries(
        &self,
        term: &str,
    ) -> std::result::Result<Vec<Fts5DoclistEntry>, Fts5QueryError>;

    /// Doclist entries for every indexed term that has `prefix`, delete markers
    /// removed.
    fn prefix_entries(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<Fts5DoclistEntry>, Fts5QueryError>;

    /// Token count for `rowid` across indexed columns (0 if unknown). BM25 `dl`.
    fn doc_length(&self, rowid: i64) -> std::result::Result<u32, Fts5QueryError>;

    /// Total indexed documents (BM25 `N`).
    fn total_docs(&self) -> std::result::Result<u64, Fts5QueryError>;

    /// Average document length in tokens (BM25 `avgdl`).
    fn avg_doc_length(&self) -> std::result::Result<f64, Fts5QueryError>;

    // --- shared query engine (default methods; do not override) ------------

    /// Run one or more MATCH queries (combined with AND) and return matching
    /// `(rowid, bm25)` pairs sorted by ascending score then rowid.
    fn search_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        let mut combined_docs: Option<Vec<i64>> = None;
        let mut query_terms = Vec::new();

        for query in queries {
            let tokens = parse_fts5_query(query)?;
            let mut expr = build_expr(&tokens)?;
            expr = normalize_query_expr_with_tokenizer(expr, self.tokenizer());
            validate_detail_mode(&expr, self.detail())?;
            validate_column_filters(&expr, self.columns())?;
            let matching_docs = self.evaluate_expr(&expr, None)?;
            query_terms.extend(extract_query_terms(&expr));
            combined_docs = Some(match combined_docs {
                Some(existing) => intersect_sorted(&existing, &matching_docs),
                None => matching_docs,
            });
        }

        let mut results = Vec::new();
        for rowid in combined_docs.unwrap_or_default() {
            results.push((rowid, self.bm25_score(rowid, &query_terms, weights)?));
        }
        results.sort_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(results)
    }

    fn term_rowids(
        &self,
        term: &str,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<i64>, Fts5QueryError> {
        Ok(shadow_rowids_from_entries(
            &self.exact_entries(term)?,
            allowed_columns,
        ))
    }

    fn prefix_rowids(
        &self,
        prefix: &str,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<i64>, Fts5QueryError> {
        Ok(shadow_rowids_from_entries(
            &self.prefix_entries(prefix)?,
            allowed_columns,
        ))
    }

    fn term_spans(
        &self,
        term: &str,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<Fts5NearSpan>, Fts5QueryError> {
        Ok(shadow_spans_from_entries(
            &self.exact_entries(term)?,
            allowed_columns,
        ))
    }

    fn prefix_spans(
        &self,
        prefix: &str,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<Fts5NearSpan>, Fts5QueryError> {
        Ok(shadow_spans_from_entries(
            &self.prefix_entries(prefix)?,
            allowed_columns,
        ))
    }

    fn phrase_spans(
        &self,
        words: &[String],
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<Fts5NearSpan>, Fts5QueryError> {
        let Some(first) = words.first() else {
            return Ok(Vec::new());
        };

        let mut spans = self.term_spans(first, allowed_columns)?;
        for word in &words[1..] {
            let next_spans = self.term_spans(word, allowed_columns)?;
            let mut combined = Vec::new();
            for left in &spans {
                combined.extend(
                    next_spans
                        .iter()
                        .filter(|right| {
                            left.docid == right.docid
                                && left.column == right.column
                                && right.start == left.end.saturating_add(1)
                        })
                        .map(|right| Fts5NearSpan {
                            docid: left.docid,
                            column: left.column,
                            start: left.start,
                            end: right.end,
                        }),
                );
            }
            combined.sort_unstable_by_key(|span| (span.docid, span.column, span.start, span.end));
            combined.dedup_by_key(|span| (span.docid, span.column, span.start, span.end));
            spans = combined;
            if spans.is_empty() {
                break;
            }
        }
        Ok(spans)
    }

    fn phrase_prefix_spans(
        &self,
        words: &[String],
        prefix: &str,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<Fts5NearSpan>, Fts5QueryError> {
        if words.is_empty() {
            return self.prefix_spans(prefix, allowed_columns);
        }

        let phrase_spans = self.phrase_spans(words, allowed_columns)?;
        let prefix_spans = self.prefix_spans(prefix, allowed_columns)?;
        let mut combined = Vec::new();
        for left in &phrase_spans {
            combined.extend(
                prefix_spans
                    .iter()
                    .filter(|right| {
                        left.docid == right.docid
                            && left.column == right.column
                            && right.start == left.end.saturating_add(1)
                    })
                    .map(|right| Fts5NearSpan {
                        docid: left.docid,
                        column: left.column,
                        start: left.start,
                        end: right.end,
                    }),
            );
        }
        combined.sort_unstable_by_key(|span| (span.docid, span.column, span.start, span.end));
        combined.dedup_by_key(|span| (span.docid, span.column, span.start, span.end));
        Ok(combined)
    }

    fn near_operand_spans(
        &self,
        operand: &Fts5NearOperand,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<Fts5NearSpan>, Fts5QueryError> {
        match operand {
            Fts5NearOperand::Term(term) => self.term_spans(term, allowed_columns),
            Fts5NearOperand::Prefix(prefix) => self.prefix_spans(prefix, allowed_columns),
            Fts5NearOperand::Phrase(words) => self.phrase_spans(words, allowed_columns),
        }
    }

    fn near_rowids(
        &self,
        operands: &[Fts5NearOperand],
        distance: u32,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<i64>, Fts5QueryError> {
        if operands.len() < 2 {
            return Ok(Vec::new());
        }

        let operand_spans: Vec<Vec<Fts5NearSpan>> = operands
            .iter()
            .map(|operand| self.near_operand_spans(operand, allowed_columns))
            .collect::<std::result::Result<_, _>>()?;
        if operand_spans.iter().any(Vec::is_empty) {
            return Ok(Vec::new());
        }

        let mut operand_order: Vec<usize> = (0..operand_spans.len()).collect();
        operand_order.sort_by_key(|&operand_index| operand_spans[operand_index].len());
        let anchor_operand = operand_order[0];
        let mut result = Vec::new();

        for anchor_span in &operand_spans[anchor_operand] {
            let mut selected = SmallVec::new();
            selected.push(*anchor_span);
            if find_near_clump(&operand_spans, &operand_order, 1, &mut selected, distance)
                && !result.contains(&anchor_span.docid)
            {
                result.push(anchor_span.docid);
            }
        }

        result.sort_unstable();
        Ok(result)
    }

    fn evaluate_initial_expr(
        &self,
        expr: &Fts5Expr,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<i64>, Fts5QueryError> {
        let spans = match expr {
            Fts5Expr::Term(term) => self.term_spans(term, allowed_columns)?,
            Fts5Expr::Prefix(prefix) => self.prefix_spans(prefix, allowed_columns)?,
            Fts5Expr::Phrase(words) => self.phrase_spans(words, allowed_columns)?,
            Fts5Expr::PhrasePrefix(words, prefix) => {
                self.phrase_prefix_spans(words, prefix, allowed_columns)?
            }
            _ => return self.evaluate_expr(expr, allowed_columns),
        };
        Ok(shadow_rowids_from_spans(
            spans.into_iter().filter(|span| span.start == 0).collect(),
        ))
    }

    fn evaluate_expr(
        &self,
        expr: &Fts5Expr,
        allowed_columns: Option<&[u32]>,
    ) -> std::result::Result<Vec<i64>, Fts5QueryError> {
        match expr {
            Fts5Expr::Term(term) => self.term_rowids(term, allowed_columns),
            Fts5Expr::Prefix(prefix) => self.prefix_rowids(prefix, allowed_columns),
            Fts5Expr::Phrase(words) => Ok(shadow_rowids_from_spans(
                self.phrase_spans(words, allowed_columns)?,
            )),
            Fts5Expr::PhrasePrefix(words, prefix) => Ok(shadow_rowids_from_spans(
                self.phrase_prefix_spans(words, prefix, allowed_columns)?,
            )),
            Fts5Expr::And(left, right) => {
                let left_docs = self.evaluate_expr(left, allowed_columns)?;
                let right_docs = self.evaluate_expr(right, allowed_columns)?;
                Ok(intersect_sorted(&left_docs, &right_docs))
            }
            Fts5Expr::Or(left, right) => {
                let left_docs = self.evaluate_expr(left, allowed_columns)?;
                let right_docs = self.evaluate_expr(right, allowed_columns)?;
                Ok(union_sorted(&left_docs, &right_docs))
            }
            Fts5Expr::Not(left, right) => {
                let left_docs = self.evaluate_expr(left, allowed_columns)?;
                let right_docs = self.evaluate_expr(right, allowed_columns)?;
                Ok(difference_sorted(&left_docs, &right_docs))
            }
            Fts5Expr::Near(operands, distance) => {
                self.near_rowids(operands, *distance, allowed_columns)
            }
            Fts5Expr::ColumnFilter(column_filter, inner) => {
                let Some(filter_spec) = parse_column_filter_spec(column_filter) else {
                    return Ok(Vec::new());
                };
                let Some(resolved_columns) =
                    resolve_column_filter_columns(self.columns(), &filter_spec)
                else {
                    return Ok(Vec::new());
                };
                let combined_columns = combine_allowed_columns(allowed_columns, &resolved_columns);
                self.evaluate_expr(inner, Some(combined_columns.as_slice()))
            }
            Fts5Expr::InitialToken(inner) => self.evaluate_initial_expr(inner, allowed_columns),
        }
    }

    fn doc_frequency(&self, term: &str) -> std::result::Result<u64, Fts5QueryError> {
        Ok(self
            .exact_entries(term)?
            .into_iter()
            .filter(|entry| !entry.poslist.delete)
            .filter_map(|entry| rowid_u64_to_i64(entry.rowid))
            .collect::<BTreeSet<_>>()
            .len()
            .try_into()
            .unwrap_or(u64::MAX))
    }

    fn term_column_frequencies(
        &self,
        term: &str,
        rowid: i64,
    ) -> std::result::Result<Vec<(u32, u32)>, Fts5QueryError> {
        let mut frequencies = BTreeMap::new();
        for entry in self.exact_entries(term)? {
            if rowid_u64_to_i64(entry.rowid) != Some(rowid) || entry.poslist.delete {
                continue;
            }
            for column in entry.poslist.columns {
                let count = u32::try_from(column.offsets.len()).unwrap_or(u32::MAX);
                *frequencies.entry(column.column).or_insert(0_u32) = frequencies
                    .get(&column.column)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(count);
            }
        }
        Ok(frequencies.into_iter().collect())
    }

    fn bm25_score(
        &self,
        rowid: i64,
        query_terms: &[String],
        weights: &[f64],
    ) -> std::result::Result<f64, Fts5QueryError> {
        let n = self.total_docs()? as f64;
        let avgdl = self.avg_doc_length()?;
        let dl = f64::from(self.doc_length(rowid)?);
        let mut score = 0.0;

        for term in query_terms {
            let df_int = self.doc_frequency(term)?;
            if df_int == 0 {
                continue;
            }
            let df = df_int as f64;
            let idf = ((n - df + 0.5) / (df + 0.5)).ln().max(BM25_IDF_FLOOR);

            let mut weighted_tf = 0.0;
            for (column, tf_u32) in self.term_column_frequencies(term, rowid)? {
                let tf = f64::from(tf_u32);
                let column_weight = usize::try_from(column)
                    .ok()
                    .and_then(|index| weights.get(index).copied())
                    .unwrap_or(1.0);
                weighted_tf += column_weight * tf;
            }

            if weighted_tf == 0.0 {
                continue;
            }

            let denom = if avgdl > 0.0 {
                BM25_K1.mul_add(1.0 - BM25_B + BM25_B * dl / avgdl, weighted_tf)
            } else {
                weighted_tf + BM25_K1
            };
            score += idf * (weighted_tf * (BM25_K1 + 1.0)) / denom;
        }

        Ok(-score)
    }
}

/// Reader-native MATCH evaluator for shadow-backed FTS5 tables.
pub struct Fts5ShadowQuery<'a> {
    rows: &'a Fts5ShadowRows,
    columns: &'a [String],
    tokenizer: &'a dyn Fts5Tokenizer,
    detail: DetailMode,
    structure: Option<Fts5StructureRecord>,
    averages: Option<Fts5AveragesRecord>,
}

impl<'a> Fts5ShadowQuery<'a> {
    pub fn new(
        rows: &'a Fts5ShadowRows,
        columns: &'a [String],
        tokenizer: &'a dyn Fts5Tokenizer,
        detail: DetailMode,
    ) -> std::result::Result<Self, Fts5QueryError> {
        let data = Fts5DataMetadata::decode_rows(rows.data.as_slice(), columns.len())
            .map_err(shadow_query_storage_error)?;
        Ok(Self {
            rows,
            columns,
            tokenizer,
            detail,
            structure: data.structure,
            averages: data.averages,
        })
    }

    pub fn search(
        &self,
        query: &str,
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        self.search_queries_with_weights(&[query], weights)
    }

    /// Public entry point preserved for callers; the actual ranked-search
    /// engine lives once on `Fts5DoclistProvider::search_with_weights`.
    pub fn search_queries_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        self.search_with_weights(queries, weights)
    }
}

impl Fts5DoclistProvider for Fts5ShadowQuery<'_> {
    fn columns(&self) -> &[String] {
        self.columns
    }

    fn tokenizer(&self) -> &dyn Fts5Tokenizer {
        self.tokenizer
    }

    fn detail(&self) -> DetailMode {
        self.detail
    }

    fn exact_entries(
        &self,
        term: &str,
    ) -> std::result::Result<Vec<Fts5DoclistEntry>, Fts5QueryError> {
        let Some(structure) = self.structure.as_ref() else {
            return Ok(Vec::new());
        };

        let row_set = Fts5SegmentRowSet::new(&self.rows.data, &self.rows.idx);
        let mut entries = Vec::new();
        for key in segment_lookup_keys(term.as_bytes()) {
            for level in &structure.levels {
                for segment in &level.segments {
                    if let Some(postings) = row_set
                        .reader(segment)
                        .exact_postings(&key)
                        .map_err(shadow_query_storage_error)?
                    {
                        entries.extend(
                            postings
                                .entries
                                .into_iter()
                                .filter(|entry| !entry.poslist.delete),
                        );
                    }
                }
            }
        }
        Ok(entries)
    }

    fn prefix_entries(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<Fts5DoclistEntry>, Fts5QueryError> {
        let Some(structure) = self.structure.as_ref() else {
            return Ok(Vec::new());
        };

        let row_set = Fts5SegmentRowSet::new(&self.rows.data, &self.rows.idx);
        let mut entries = Vec::new();
        for key in segment_lookup_keys(prefix.as_bytes()) {
            for level in &structure.levels {
                for segment in &level.segments {
                    for term_match in row_set
                        .reader(segment)
                        .prefix_matches(&key)
                        .map_err(shadow_query_storage_error)?
                    {
                        entries.extend(
                            term_match
                                .postings
                                .entries
                                .into_iter()
                                .filter(|entry| !entry.poslist.delete),
                        );
                    }
                }
            }
        }
        Ok(entries)
    }

    fn total_docs(&self) -> std::result::Result<u64, Fts5QueryError> {
        if let Some(averages) = self.averages.as_ref()
            && averages.total_rows > 0
        {
            return Ok(averages.total_rows);
        }

        Ok(self
            .rows
            .content
            .iter()
            .map(|row| row.rowid)
            .chain(self.rows.docsize.iter().map(|row| row.rowid))
            .collect::<BTreeSet<_>>()
            .len()
            .try_into()
            .unwrap_or(u64::MAX))
    }

    fn avg_doc_length(&self) -> std::result::Result<f64, Fts5QueryError> {
        if let Some(averages) = self.averages.as_ref()
            && averages.total_rows > 0
        {
            let total_tokens: u64 = averages.column_token_totals.iter().sum();
            return Ok(total_tokens as f64 / averages.total_rows as f64);
        }

        if self.rows.docsize.is_empty() {
            return Ok(0.0);
        }
        let total_tokens: u64 = self
            .rows
            .docsize
            .iter()
            .map(|row| u64::from(row.total_tokens()))
            .sum();
        Ok(total_tokens as f64 / self.rows.docsize.len() as f64)
    }

    fn doc_length(&self, rowid: i64) -> std::result::Result<u32, Fts5QueryError> {
        Ok(self
            .rows
            .docsize
            .iter()
            .find(|row| row.rowid == rowid)
            .map_or(0, Fts5DocsizeRow::total_tokens))
    }
}

/// Lazy on-disk MATCH evaluator for reopened FTS5 indexes.
///
/// Answers FTS5 queries by point-reading ONLY the persisted segments/postings a
/// query touches through an [`Fts5OnDiskReader`], instead of rebuilding the
/// whole in-memory inverted index on every connection.
///
/// It shares the entire ranked-search engine (boolean/phrase/NEAR evaluation,
/// column filters, BM25) with [`Fts5ShadowQuery`] via `Fts5DoclistProvider`,
/// so a reopened on-disk index returns IDENTICAL rowids, ordering, and scores to
/// the in-memory path — only the doclist primitives differ (on-demand segment
/// reads vs. an in-memory row set).
///
/// Per-query caches collapse the several consultations a single MATCH makes of
/// each term's postings (expression evaluation, then BM25 `df`/`tf`) and each
/// result's `_docsize` into ONE on-disk scan/read apiece, so cost stays bounded
/// by the doclists a query actually touches rather than the whole corpus.
pub struct Fts5LazyQuery<'a> {
    reader: std::cell::RefCell<&'a mut dyn Fts5OnDiskReader>,
    columns: &'a [String],
    tokenizer: &'a dyn Fts5Tokenizer,
    detail: DetailMode,
    structure: Fts5StructureRecord,
    averages: Option<Fts5AveragesRecord>,
    column_count: usize,
    exact_cache: std::cell::RefCell<HashMap<String, std::rc::Rc<Vec<Fts5DoclistEntry>>>>,
    prefix_cache: std::cell::RefCell<HashMap<String, std::rc::Rc<Vec<Fts5DoclistEntry>>>>,
    docsize_cache: std::cell::RefCell<HashMap<i64, u32>>,
}

impl<'a> Fts5LazyQuery<'a> {
    /// Build a lazy query over `reader`, reading the structure (id=10) and
    /// averages (id=1) records ONCE up front. A missing structure row means an
    /// empty index (every query yields nothing); a missing averages row leaves
    /// BM25 `N`/`avgdl` at zero (a freshly flushed segment always writes it).
    pub fn new(
        reader: &'a mut dyn Fts5OnDiskReader,
        columns: &'a [String],
        tokenizer: &'a dyn Fts5Tokenizer,
        detail: DetailMode,
    ) -> std::result::Result<Self, Fts5QueryError> {
        let column_count = columns.len();
        let structure = match reader
            .read_data_block(FTS5_STRUCTURE_ROWID)
            .map_err(shadow_query_storage_error)?
        {
            Some(block) => {
                Fts5StructureRecord::decode(&block).map_err(shadow_query_storage_error)?
            }
            None => Fts5StructureRecord::empty_legacy(0),
        };
        let averages = match reader
            .read_data_block(FTS5_AVERAGES_ROWID)
            .map_err(shadow_query_storage_error)?
        {
            Some(block) => Some(
                Fts5AveragesRecord::decode(&block, column_count)
                    .map_err(shadow_query_storage_error)?,
            ),
            None => None,
        };
        Ok(Self {
            reader: std::cell::RefCell::new(reader),
            columns,
            tokenizer,
            detail,
            structure,
            averages,
            column_count,
            exact_cache: std::cell::RefCell::new(HashMap::new()),
            prefix_cache: std::cell::RefCell::new(HashMap::new()),
            docsize_cache: std::cell::RefCell::new(HashMap::new()),
        })
    }

    /// Run a single MATCH query, returning `(rowid, bm25)` pairs sorted by score.
    pub fn search(
        &self,
        query: &str,
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        self.search_with_weights(&[query], weights)
    }

    /// Run one or more MATCH queries (AND-combined), mirroring
    /// [`Fts5ShadowQuery::search_queries_with_weights`].
    pub fn search_queries_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        self.search_with_weights(queries, weights)
    }

    /// Point-read the `_content` row for a (result) `rowid`, so projection reads
    /// only the LIMITed result set rather than the whole corpus.
    pub fn content_for_rowid(
        &self,
        rowid: i64,
    ) -> std::result::Result<Option<Vec<String>>, Fts5QueryError> {
        self.reader
            .borrow_mut()
            .read_content(rowid, self.column_count)
            .map_err(shadow_query_storage_error)
    }
}

impl Fts5DoclistProvider for Fts5LazyQuery<'_> {
    fn columns(&self) -> &[String] {
        self.columns
    }

    fn tokenizer(&self) -> &dyn Fts5Tokenizer {
        self.tokenizer
    }

    fn detail(&self) -> DetailMode {
        self.detail
    }

    fn exact_entries(
        &self,
        term: &str,
    ) -> std::result::Result<Vec<Fts5DoclistEntry>, Fts5QueryError> {
        if let Some(cached) = self.exact_cache.borrow().get(term) {
            return Ok((**cached).clone());
        }
        let entries = lazy_exact_doclist_entries(
            &mut **self.reader.borrow_mut(),
            &self.structure,
            term.as_bytes(),
        )
        .map_err(shadow_query_storage_error)?;
        self.exact_cache
            .borrow_mut()
            .insert(term.to_owned(), std::rc::Rc::new(entries.clone()));
        Ok(entries)
    }

    fn prefix_entries(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<Fts5DoclistEntry>, Fts5QueryError> {
        if let Some(cached) = self.prefix_cache.borrow().get(prefix) {
            return Ok((**cached).clone());
        }
        let entries = lazy_prefix_doclist_entries(
            &mut **self.reader.borrow_mut(),
            &self.structure,
            prefix.as_bytes(),
        )
        .map_err(shadow_query_storage_error)?;
        self.prefix_cache
            .borrow_mut()
            .insert(prefix.to_owned(), std::rc::Rc::new(entries.clone()));
        Ok(entries)
    }

    fn doc_length(&self, rowid: i64) -> std::result::Result<u32, Fts5QueryError> {
        if let Some(cached) = self.docsize_cache.borrow().get(&rowid) {
            return Ok(*cached);
        }
        let length = self
            .reader
            .borrow_mut()
            .read_docsize(rowid, self.column_count)
            .map_err(shadow_query_storage_error)?
            .as_ref()
            .map_or(0, Fts5DocsizeRow::total_tokens);
        self.docsize_cache.borrow_mut().insert(rowid, length);
        Ok(length)
    }

    fn total_docs(&self) -> std::result::Result<u64, Fts5QueryError> {
        Ok(self
            .averages
            .as_ref()
            .map_or(0, |averages| averages.total_rows))
    }

    fn avg_doc_length(&self) -> std::result::Result<f64, Fts5QueryError> {
        let Some(averages) = self.averages.as_ref() else {
            return Ok(0.0);
        };
        if averages.total_rows == 0 {
            return Ok(0.0);
        }
        let total_tokens: u64 = averages.column_token_totals.iter().sum();
        Ok(total_tokens as f64 / averages.total_rows as f64)
    }
}

// ---------------------------------------------------------------------------
// FTS5 Virtual Table
// ---------------------------------------------------------------------------

/// FTS5 virtual table: full-text search index.
#[derive(Debug, Clone)]
pub struct Fts5Table {
    /// Column names.
    columns: Vec<String>,
    /// Whether each column participates in the inverted index.
    indexed_columns: Vec<bool>,
    /// Configuration.
    config: Fts5Config,
    /// Tokenizer.
    tokenizer_name: String,
    /// Configured prefix index lengths.
    prefix_lengths: Vec<usize>,
    /// Inverted index.
    index: InvertedIndex,
    /// Stored document content: docid -> (col0, col1, ...).
    documents: HashMap<i64, Vec<String>>,
    /// Shadow-table rows bound from an existing on-disk FTS5 table.
    shadow_rows: Option<Fts5ShadowRows>,
    /// Lazy on-disk mode: the table was reopened over persisted `_data`
    /// posting segments and is answered by point-reading those segments via a
    /// host-supplied [`Fts5OnDiskReader`] instead of loading the whole index.
    lazy_on_disk: bool,
    /// Cached live document count for lazy mode, read from the persisted
    /// averages row so `COUNT(*)` does not scan or materialize the index.
    lazy_doc_count: usize,
    /// Locale metadata decoded from fts5_locale() values: (docid, column) -> locale.
    row_locales: HashMap<(i64, usize), SmallText>,
    /// Next auto-generated rowid.
    next_rowid: i64,
    /// Snapshot-backed transaction/savepoint state for live VTAB writes.
    txn_state: TransactionalVtabState<Fts5TableSnapshot>,
}

#[derive(Debug, Clone)]
struct Fts5TableSnapshot {
    config: Fts5Config,
    tokenizer_name: String,
    prefix_lengths: Vec<usize>,
    indexed_columns: Vec<bool>,
    index: InvertedIndex,
    documents: HashMap<i64, Vec<String>>,
    shadow_rows: Option<Fts5ShadowRows>,
    lazy_on_disk: bool,
    lazy_doc_count: usize,
    row_locales: HashMap<(i64, usize), SmallText>,
    next_rowid: i64,
}

struct DecodedColumnValues {
    values: Vec<String>,
    locales: Vec<(usize, SmallText)>,
}

impl Fts5Table {
    /// Create a new FTS5 table with the given column names.
    #[must_use]
    pub fn with_columns(columns: Vec<String>) -> Self {
        let indexed_columns = vec![true; columns.len()];
        Self {
            columns,
            indexed_columns,
            config: Fts5Config::default(),
            tokenizer_name: "unicode61".to_owned(),
            prefix_lengths: Vec::new(),
            index: InvertedIndex::with_options(true, &[], DetailMode::Full),
            documents: HashMap::new(),
            shadow_rows: None,
            lazy_on_disk: false,
            lazy_doc_count: 0,
            row_locales: HashMap::new(),
            next_rowid: 1,
            txn_state: TransactionalVtabState::default(),
        }
    }

    fn snapshot_state(&self) -> Fts5TableSnapshot {
        Fts5TableSnapshot {
            config: self.config,
            tokenizer_name: self.tokenizer_name.clone(),
            prefix_lengths: self.prefix_lengths.clone(),
            indexed_columns: self.indexed_columns.clone(),
            index: self.index.clone(),
            documents: self.documents.clone(),
            shadow_rows: self.shadow_rows.clone(),
            lazy_on_disk: self.lazy_on_disk,
            lazy_doc_count: self.lazy_doc_count,
            row_locales: self.row_locales.clone(),
            next_rowid: self.next_rowid,
        }
    }

    fn restore_state(&mut self, snapshot: Fts5TableSnapshot) {
        self.config = snapshot.config;
        self.tokenizer_name = snapshot.tokenizer_name;
        self.prefix_lengths = snapshot.prefix_lengths;
        self.indexed_columns = snapshot.indexed_columns;
        self.index = snapshot.index;
        self.documents = snapshot.documents;
        self.shadow_rows = snapshot.shadow_rows;
        self.lazy_on_disk = snapshot.lazy_on_disk;
        self.lazy_doc_count = snapshot.lazy_doc_count;
        self.row_locales = snapshot.row_locales;
        self.next_rowid = snapshot.next_rowid;
    }

    fn restore_transaction_snapshot(&mut self, snapshot: Option<Fts5TableSnapshot>) -> bool {
        if let Some(snapshot) = snapshot {
            self.restore_state(snapshot);
            true
        } else {
            false
        }
    }

    fn index_document_with_tokenizer(
        &mut self,
        rowid: i64,
        column_values: &[String],
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        #[allow(clippy::cast_possible_truncation)]
        for (col_idx, text) in column_values.iter().enumerate() {
            if matches!(self.indexed_columns.get(col_idx), Some(false)) {
                continue;
            }
            self.index.add_text(rowid, col_idx as u32, tokenizer, text);
        }
    }

    fn store_document_with_tokenizer_and_locales(
        &mut self,
        rowid: i64,
        column_values: Vec<String>,
        locales: Vec<(usize, SmallText)>,
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        self.materialize_shadow_rows_for_mutation();
        if self.documents.contains_key(&rowid) {
            self.index.remove_document(rowid);
        }
        self.row_locales.retain(|(existing_rowid, _column), _| {
            !matches!(existing_rowid.cmp(&rowid), std::cmp::Ordering::Equal)
        });
        self.row_locales.extend(
            locales
                .into_iter()
                .map(|(column, tag)| ((rowid, column), tag)),
        );
        self.index_document_with_tokenizer(rowid, &column_values, tokenizer);
        self.next_rowid = self.next_rowid.max(rowid.saturating_add(1));
        let stored_values = self.content_values_for_storage(&column_values);
        self.documents.insert(rowid, stored_values);
        self.shadow_rows = None;
        debug!(rowid, "fts5: indexed document");
    }

    fn content_values_for_storage(&self, column_values: &[String]) -> Vec<String> {
        if self.config.content_mode != ContentMode::Contentless {
            return column_values.to_vec();
        }

        column_values
            .iter()
            .enumerate()
            .map(|(column, value)| {
                if self.config.contentless_unindexed
                    && matches!(self.indexed_columns.get(column), Some(false))
                {
                    value.clone()
                } else {
                    String::new()
                }
            })
            .collect()
    }

    fn store_document_with_tokenizer(
        &mut self,
        rowid: i64,
        column_values: Vec<String>,
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        self.store_document_with_tokenizer_and_locales(rowid, column_values, Vec::new(), tokenizer);
    }

    pub fn create_tokenizer_instance(&self) -> Box<dyn Fts5Tokenizer> {
        create_tokenizer(&self.tokenizer_name)
            .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()))
    }

    pub fn open_shadow_rows(
        cx: &Cx,
        args: &[&str],
        rows: &Fts5ShadowRows,
    ) -> Result<Fts5ShadowOpen> {
        let mut table = Self::connect(cx, args)?;
        let report = rows.validate_for_open(table.columns.len())?;
        report.metadata.apply_to_runtime_config(&mut table.config);
        table.index = InvertedIndex::with_options_and_tokendata(
            table.config.columnsize_enabled(),
            &table.prefix_lengths,
            table.config.detail_mode(),
            table.config.tokendata_enabled(),
        );
        if let Some(rowid) = report.max_seen_rowid {
            table.next_rowid = rowid.saturating_add(1).max(1);
        }
        table.shadow_rows = Some(rows.clone());
        Ok(Fts5ShadowOpen { table, report })
    }

    /// Bind PERSISTED shadow rows onto an already-connected table so reads are
    /// answered from the on-disk posting-list index instead of an in-memory
    /// index rebuilt by re-tokenizing `_content`. This is the reopen/reload
    /// counterpart to [`Fts5Table::open_shadow_rows`] (which connects a fresh
    /// table): it preserves the declared options resolved by `connect` and
    /// only layers the shadow-backed query state on top.
    pub fn bind_shadow_rows(&mut self, rows: Fts5ShadowRows) -> Result<Fts5ShadowOpenReport> {
        let report = rows.validate_for_open(self.columns.len())?;
        self.bind_validated_shadow_rows(rows, &report);
        Ok(report)
    }

    fn bind_validated_shadow_rows(&mut self, rows: Fts5ShadowRows, report: &Fts5ShadowOpenReport) {
        report.metadata.apply_to_runtime_config(&mut self.config);
        self.index = InvertedIndex::with_options_and_tokendata(
            self.config.columnsize_enabled(),
            &self.prefix_lengths,
            self.config.detail_mode(),
            self.config.tokendata_enabled(),
        );
        self.documents.clear();
        self.row_locales.clear();
        if let Some(rowid) = report.max_seen_rowid {
            self.next_rowid = rowid.saturating_add(1).max(1);
        }
        self.shadow_rows = Some(rows);
    }

    fn materialize_shadow_rows_for_mutation(&mut self) {
        let Some(rows) = self.shadow_rows.take() else {
            return;
        };
        let Fts5ShadowRows {
            content, docsize, ..
        } = rows;
        let documents = content
            .into_iter()
            .map(|row| (row.rowid, row.values))
            .collect();
        self.rebuild_documents(documents);
        self.apply_docsize_rows(&docsize);
    }

    /// Hydrate the in-memory inverted index from the persisted on-disk segments
    /// of a reopened CONTENTLESS table, so an incremental mutation appends to the
    /// existing postings instead of dropping them.
    ///
    /// A no-op for content-backed tables (their index is rebuilt by
    /// re-tokenizing the bound `_content` rows in
    /// [`Self::materialize_shadow_rows_for_mutation`]) and for tables with no
    /// persisted segments. On success the lazy shadow binding is consumed, so the
    /// subsequent `materialize_shadow_rows_for_mutation` becomes a no-op and the
    /// new row is appended to the fully-hydrated index.
    fn hydrate_contentless_index_from_segments(&mut self) -> Result<()> {
        if self.config.content_mode != ContentMode::Contentless {
            return Ok(());
        }
        let Some(rows) = self.shadow_rows.as_ref() else {
            return Ok(());
        };
        let metadata = Fts5DataMetadata::decode_rows(&rows.data, self.columns.len())?;
        let Some(structure) = metadata.structure else {
            return Ok(());
        };
        if structure.segment_count() == 0 {
            return Ok(());
        }
        let groups = {
            let mut reader = Fts5ShadowRowReader {
                data: &rows.data,
                idx: &rows.idx,
            };
            collect_live_term_groups(&mut reader, &structure)?
        };
        let docsize = rows.docsize.clone();
        // Last use of `rows` (the &self.shadow_rows borrow) — released here so the
        // index/docsize writes below can take `self` mutably.
        let mut index = InvertedIndex::with_options_and_tokendata(
            self.config.columnsize_enabled(),
            &self.prefix_lengths,
            self.config.detail_mode(),
            self.config.tokendata_enabled(),
        );
        for (stored_term, entries) in groups {
            // `collect_live_term_groups` (empty prefix) yields only MAIN-index
            // terms; `append_position` rebuilds the prefix indexes from the raw
            // token. A given segment stores terms in ONE encoding: stock SQLite
            // and frankensqlite's own writer mark main terms as
            // `FTS5_MAIN_PREFIX_BYTE ++ token`, but some segments store the raw
            // token, so strip the marker when present and otherwise use the term
            // as-is.
            let raw = match stored_term.split_first() {
                Some((&FTS5_MAIN_PREFIX_BYTE, rest)) => rest,
                _ => stored_term.as_slice(),
            };
            let term = String::from_utf8_lossy(raw);
            for entry in entries {
                let Ok(docid) = i64::try_from(entry.rowid) else {
                    continue;
                };
                index.replay_persisted_entry(&term, docid, &entry.poslist);
            }
        }
        self.index = index;
        self.apply_docsize_rows(&docsize);
        self.shadow_rows = None;
        Ok(())
    }

    /// Prepare a (possibly lazily reopened) table for an in-place mutation.
    ///
    /// For a CONTENTLESS table reopened with persisted segments this hydrates the
    /// in-memory index from those segments (the postings are otherwise
    /// unrecoverable — contentless keeps no `_content` rows); for every other
    /// table it is a cheap no-op and the standard content materialization runs at
    /// mutation time.
    pub fn ensure_shadow_rows_materializable_for_mutation(&mut self) -> Result<()> {
        self.hydrate_contentless_index_from_segments()
    }

    pub fn ensure_rebuild_supported(&self) -> Result<()> {
        if self.config.content_mode == ContentMode::Contentless {
            return Err(FrankenError::function_error(
                "fts5: rebuild is not available with contentless tables",
            ));
        }
        Ok(())
    }

    fn has_document_or_shadow_rowid(&self, rowid: i64) -> bool {
        self.documents.contains_key(&rowid)
            || self.shadow_rows.as_ref().is_some_and(|rows| {
                rows.content.iter().any(|row| row.rowid == rowid)
                    || rows.docsize.iter().any(|row| row.rowid == rowid)
            })
    }

    fn shadow_rows_have_persisted_segments(report: &Fts5ShadowOpenReport) -> bool {
        report
            .structure
            .as_ref()
            .is_some_and(|structure| structure.segment_count() > 0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn allocate_rowid(&mut self) -> i64 {
        let rowid = self.next_rowid;
        self.next_rowid = self.next_rowid.saturating_add(1);
        rowid
    }

    pub fn insert_document_owned_with_tokenizer(
        &mut self,
        rowid: i64,
        column_values: Vec<String>,
        tokenizer: &dyn Fts5Tokenizer,
    ) {
        self.store_document_with_tokenizer(rowid, column_values, tokenizer);
    }

    fn insert_document_owned(&mut self, rowid: i64, column_values: Vec<String>) {
        let tokenizer = self.create_tokenizer_instance();
        self.store_document_with_tokenizer(rowid, column_values, tokenizer.as_ref());
    }

    fn decode_column_values(&self, values: &[SqliteValue]) -> Result<DecodedColumnValues> {
        let mut column_values = Vec::with_capacity(values.len());
        let mut locales = Vec::new();

        for (column, value) in values.iter().enumerate() {
            if let Some(blob) = value.as_blob_bytes()
                && let Some((tag, text)) = decode_fts5_locale_blob(blob)
            {
                if !self.config.locale_enabled() {
                    return Err(FrankenError::function_error(
                        "fts5_locale() requires locale=1",
                    ));
                }
                column_values.push(text.to_owned());
                if self.indexed_columns.get(column).copied().unwrap_or(true) {
                    locales.push((column, SmallText::new(tag)));
                }
                continue;
            }
            column_values.push(value.to_text());
        }

        Ok(DecodedColumnValues {
            values: column_values,
            locales,
        })
    }

    /// Insert a document into the FTS5 table.
    pub fn insert_document(&mut self, rowid: i64, column_values: &[String]) {
        self.insert_document_owned(rowid, column_values.to_vec());
    }

    /// Delete a document from the FTS5 table.
    pub fn delete_document(&mut self, rowid: i64) {
        self.materialize_shadow_rows_for_mutation();
        self.index.remove_document(rowid);
        self.documents.remove(&rowid);
        self.shadow_rows = None;
        self.row_locales.retain(|(existing_rowid, _column), _| {
            !matches!(existing_rowid.cmp(&rowid), std::cmp::Ordering::Equal)
        });
        debug!(rowid, "fts5: removed document");
    }

    /// Rebuild the in-memory index and rowid allocator from persisted rows.
    pub fn rebuild_documents(&mut self, rows: Vec<(i64, Vec<String>)>) {
        self.index = InvertedIndex::with_options_and_tokendata(
            self.config.columnsize_enabled(),
            &self.prefix_lengths,
            self.config.detail_mode(),
            self.config.tokendata_enabled(),
        );
        self.documents = HashMap::with_capacity(rows.len());
        self.shadow_rows = None;
        self.clear_lazy_on_disk();
        self.row_locales.clear();
        self.next_rowid = 1;
        let tokenizer = create_tokenizer(&self.tokenizer_name)
            .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));
        for (rowid, columns) in rows {
            self.index_document_with_tokenizer(rowid, &columns, tokenizer.as_ref());
            self.next_rowid = self.next_rowid.max(rowid.saturating_add(1));
            self.documents.insert(rowid, columns);
        }
    }

    /// Search the FTS5 table with a query, returning matching rowids ranked
    /// by BM25.
    pub fn search(&self, query: &str) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();
        self.search_queries_with_weights(&[query], &weights)
    }

    pub fn search_queries_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64)>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        if let Some(rows) = self.shadow_rows.as_ref() {
            return Fts5ShadowQuery::new(
                rows,
                &self.columns,
                tokenizer.as_ref(),
                self.config.detail_mode(),
            )?
            .search_queries_with_weights(queries, weights);
        }
        search_docids_with_weights_from_parts(
            &self.index,
            &self.columns,
            queries,
            weights,
            Some(tokenizer.as_ref()),
        )
    }

    pub fn search_rows(
        &self,
        query: &str,
    ) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
        let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();
        self.search_rows_for_queries_with_weights(&[query], &weights)
    }

    pub fn search_rows_for_queries_with_weights(
        &self,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        if let Some(rows) = self.shadow_rows.as_ref() {
            return Fts5ShadowQuery::new(
                rows,
                &self.columns,
                tokenizer.as_ref(),
                self.config.detail_mode(),
            )?
            .search_queries_with_weights(queries, weights)
            .map(|results| {
                results
                    .into_iter()
                    .map(|(rowid, score)| {
                        let columns = shadow_content_for_rowid(rows, rowid)
                            .map_or_else(Vec::new, <[String]>::to_vec);
                        (rowid, score, columns)
                    })
                    .collect()
            });
        }
        search_rows_with_weights_from_parts(
            &self.index,
            &self.columns,
            &self.documents,
            queries,
            weights,
            Some(tokenizer.as_ref()),
        )
    }

    /// Enter lazy on-disk mode: the table is answered by point-reading the
    /// persisted `_data` segments via a host [`Fts5OnDiskReader`] rather than
    /// loading the whole index into memory. `doc_count` is the persisted live
    /// document count from the averages row.
    pub fn mark_lazy_on_disk(&mut self, doc_count: usize) {
        self.lazy_on_disk = true;
        self.lazy_doc_count = doc_count;
        self.documents.clear();
        self.shadow_rows = None;
        self.index = InvertedIndex::with_options_and_tokendata(
            self.config.columnsize_enabled(),
            &self.prefix_lengths,
            self.config.detail_mode(),
            self.config.tokendata_enabled(),
        );
    }

    /// Whether the table is in lazy on-disk mode.
    #[must_use]
    pub const fn is_lazy_on_disk(&self) -> bool {
        self.lazy_on_disk
    }

    /// Leave lazy on-disk mode after the host materializes table rows.
    pub fn clear_lazy_on_disk(&mut self) {
        self.lazy_on_disk = false;
        self.lazy_doc_count = 0;
    }

    /// Answer MATCH queries in lazy mode by point-reading persisted on-disk
    /// segments through `reader`, projecting `_content` only for result rows.
    pub fn search_rows_lazy(
        &self,
        reader: &mut dyn Fts5OnDiskReader,
        queries: &[&str],
        weights: &[f64],
    ) -> std::result::Result<Vec<(i64, f64, Vec<String>)>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        let query = Fts5LazyQuery::new(
            reader,
            &self.columns,
            tokenizer.as_ref(),
            self.config.detail_mode(),
        )?;
        let ranked = query.search_queries_with_weights(queries, weights)?;
        let mut out = Vec::with_capacity(ranked.len());
        for (rowid, score) in ranked {
            let columns = query.content_for_rowid(rowid)?.unwrap_or_default();
            out.push((rowid, score, columns));
        }
        Ok(out)
    }

    pub fn query_terms_for_queries(
        &self,
        queries: &[&str],
    ) -> std::result::Result<Vec<String>, Fts5QueryError> {
        let tokenizer = self.create_tokenizer_instance();
        query_terms_for_query_strings(
            &self.columns,
            queries,
            tokenizer.as_ref(),
            self.config.detail_mode(),
        )
    }

    #[must_use]
    pub fn auxiliary_score_snapshot(&self) -> Fts5ScoreSnapshot {
        let source = self.shadow_rows.as_ref().map_or_else(
            || Fts5ScoreSource::InMemory(self.index.clone()),
            |rows| Fts5ScoreSource::Shadow(rows.clone()),
        );
        Fts5ScoreSnapshot {
            columns: self.columns.clone(),
            tokenizer_name: self.tokenizer_name.clone(),
            detail: self.config.detail_mode(),
            source,
        }
    }

    #[must_use]
    pub fn all_rows(&self) -> Vec<(i64, Vec<String>)> {
        // When the table is bound to persisted shadow rows (reopened on-disk
        // index, no in-memory documents), full-scan rows come from `_content`
        // plus `_docsize` rowids for contentless tables.
        let mut rows: Vec<(i64, Vec<String>)> = if self.documents.is_empty()
            && let Some(shadow) = self.shadow_rows.as_ref()
        {
            shadow.full_scan_rows()
        } else {
            self.documents
                .iter()
                .map(|(rowid, columns)| (*rowid, columns.clone()))
                .collect()
        };
        rows.sort_by_key(|(rowid, _)| *rowid);
        rows
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        if self.lazy_on_disk {
            return self.lazy_doc_count;
        }
        if self.documents.is_empty()
            && let Some(shadow) = self.shadow_rows.as_ref()
        {
            return shadow.row_count(self.columns.len());
        }
        self.documents.len()
    }

    /// Get document content for a rowid.
    #[must_use]
    pub fn get_document(&self, rowid: i64) -> Option<&[String]> {
        self.documents.get(&rowid).map(Vec::as_slice)
    }

    /// Get locale metadata decoded from an fts5_locale() column value.
    #[must_use]
    pub fn get_locale(&self, rowid: i64, column: usize) -> Option<&str> {
        self.row_locales
            .get(&(rowid, column))
            .map(SmallText::as_str)
    }

    #[must_use]
    pub fn locale_value(&self, rowid: i64, column: usize) -> SqliteValue {
        if !self.config.locale_enabled() {
            return SqliteValue::Null;
        }
        self.get_locale(rowid, column)
            .map_or(SqliteValue::Null, |tag| {
                SqliteValue::Text(SmallText::new(tag))
            })
    }

    #[must_use]
    pub fn all_locales(&self) -> Vec<(i64, usize, String)> {
        let mut locales: Vec<(i64, usize, String)> = self
            .row_locales
            .iter()
            .map(|((rowid, column), tag)| (*rowid, *column, tag.to_string()))
            .collect();
        locales.sort_unstable_by_key(|entry| (entry.0, entry.1));
        locales
    }

    /// Get the FTS5 config.
    #[must_use]
    pub fn config(&self) -> &Fts5Config {
        &self.config
    }

    /// Get a mutable reference to the FTS5 config.
    pub fn config_mut(&mut self) -> &mut Fts5Config {
        &mut self.config
    }

    #[must_use]
    pub fn config_metadata(&self) -> Fts5ConfigMetadata {
        self.config.config_metadata()
    }

    #[must_use]
    pub fn encode_config_rows(&self) -> Vec<Fts5ConfigRecord> {
        self.config.encode_config_rows()
    }

    pub fn apply_config_rows(&mut self, rows: &[Fts5ConfigRecord]) -> Result<Fts5ConfigMetadata> {
        self.config.apply_config_rows(rows)
    }

    #[must_use]
    pub fn encode_content_rows(&self) -> Vec<Fts5ContentRow> {
        if self.config.content_mode == ContentMode::Contentless
            && !self.config.contentless_unindexed
        {
            return Vec::new();
        }

        // Bound to persisted shadow rows (reopened table, no in-memory
        // documents): the validated `_content` rows ARE the content. Serving
        // them keeps point lookups and re-encoding lossless without
        // materializing the whole table.
        if self.documents.is_empty()
            && let Some(shadow) = self.shadow_rows.as_ref()
        {
            let mut rows = shadow.content.clone();
            rows.sort_unstable_by_key(|row| row.rowid);
            return rows;
        }

        let mut rows: Vec<Fts5ContentRow> = self
            .documents
            .iter()
            .filter_map(|(rowid, values)| {
                if self.config.content_mode == ContentMode::Contentless
                    && values.iter().all(String::is_empty)
                {
                    None
                } else {
                    Some(Fts5ContentRow::new(*rowid, values.clone()))
                }
            })
            .collect();
        rows.sort_unstable_by_key(|row| row.rowid);
        rows
    }

    #[must_use]
    pub fn lookup_content_row(&self, rowid: i64) -> Option<Fts5ContentRow> {
        self.encode_content_rows()
            .into_iter()
            .find(|row| row.rowid == rowid)
    }

    pub fn apply_content_rows(&mut self, rows: &[Fts5ContentRow]) {
        let rows = rows
            .iter()
            .map(|row| (row.rowid, row.values.clone()))
            .collect();
        self.rebuild_documents(rows);
    }

    #[must_use]
    pub fn encode_docsize_rows(&self) -> Vec<Fts5DocsizeRow> {
        if !self.config.columnsize_enabled() {
            return Vec::new();
        }

        // Bound to persisted shadow rows: serve the validated `_docsize`
        // rows directly (mirrors `encode_content_rows` / `all_rows`).
        if self.documents.is_empty()
            && self.index.doc_ids.is_empty()
            && let Some(shadow) = self.shadow_rows.as_ref()
        {
            let mut rows = shadow.docsize.clone();
            rows.sort_unstable_by_key(|row| row.rowid);
            return rows;
        }

        // Single O(postings) sweep across every document rather than an
        // O(documents × postings) per-document rescan. The previous
        // `docids.map(|id| self.index.docsize_row(id, ..))` re-filtered the
        // entire posting set once per document, which is quadratic in corpus
        // size and wedged full FTS rebuilds above ~15-30 MB of content
        // (cass#301). `accumulate_docsize_rows` seeds the content rowids
        // (`self.documents.keys()`) plus every indexed `doc_id` and returns
        // rows sorted by rowid, so the output is identical to the old path.
        self.index
            .accumulate_docsize_rows(self.columns.len(), self.documents.keys().copied())
    }

    #[must_use]
    pub fn lookup_docsize_row(&self, rowid: i64) -> Option<Fts5DocsizeRow> {
        self.encode_docsize_rows()
            .into_iter()
            .find(|row| row.rowid == rowid)
    }

    pub fn apply_docsize_rows(&mut self, rows: &[Fts5DocsizeRow]) {
        if !self.config.columnsize_enabled() {
            return;
        }
        if self.index.doc_lengths.is_none() {
            self.index.doc_lengths = Some(HashMap::new());
        }
        for row in rows {
            self.index.apply_docsize_row(row);
            self.next_rowid = self.next_rowid.max(row.rowid.saturating_add(1));
        }
    }

    #[must_use]
    pub fn encode_data_rows(&self) -> Vec<Fts5DataRow> {
        let docsize_rows = self.encode_docsize_rows();
        let averages = Fts5AveragesRecord::from_docsize_rows(
            u64::try_from(self.row_count()).unwrap_or(u64::MAX),
            self.columns.len(),
            &docsize_rows,
        );
        let structure = if self.config.contentless_delete_enabled() {
            Fts5StructureRecord::empty_with_origin_tracking(0)
        } else {
            Fts5StructureRecord::empty_legacy(0)
        };

        let mut rows = vec![Fts5DataRow::new(FTS5_AVERAGES_ROWID, averages.encode())];
        let pending = if self.config.content_mode == ContentMode::Contentless {
            self.index.build_pending_hash(&self.prefix_lengths)
        } else {
            self.build_pending_hash()
        };
        if let Ok(pending) = pending
            && !pending.is_empty()
            && let Ok(flush) = pending.flush_to_segment(1, structure.clone())
        {
            rows.extend(flush.data_rows);
            return rows;
        }

        rows.push(Fts5DataRow::new(FTS5_STRUCTURE_ROWID, structure.encode()));
        rows
    }

    pub fn decode_data_rows(&self, rows: &[Fts5DataRow]) -> Result<Fts5DataMetadata> {
        Fts5DataMetadata::decode_rows(rows, self.columns.len())
    }

    pub fn build_pending_hash(&self) -> Result<Fts5PendingHash> {
        let tokenizer = self.create_tokenizer_instance();
        let mut pending = Fts5PendingHash::new(
            &self.prefix_lengths,
            self.config.detail_mode(),
            self.config.tokendata_enabled(),
        );
        for (rowid, values) in self.all_rows() {
            pending.add_document(rowid, &values, &self.indexed_columns, tokenizer.as_ref())?;
        }
        Ok(pending)
    }

    pub fn flush_pending_segment(
        &self,
        segid: u32,
        structure: Fts5StructureRecord,
    ) -> Result<Fts5PendingFlush> {
        self.build_pending_hash()?
            .flush_to_segment(segid, structure)
    }

    /// Encode the persist payload for a contentless INSERT as an incremental
    /// segment append.
    ///
    /// Appends one segment onto `existing_structure` instead of re-encoding the
    /// whole inverted index ([`Self::encode_data_rows`]).
    ///
    /// `new_docs` are the `(rowid, post-rowid column args)` pairs for the rows
    /// the current INSERT added — the same `args[2..]` the vtable `update` saw,
    /// decoded identically via `Self::decode_column_values`. Only those rows
    /// are tokenized, so the work is O(new rows) instead of the full
    /// re-encode's O(table); a batch rebuild is then O(table) overall instead
    /// of O(table^2), which is the root cause of the cass#301 `index --full`
    /// wedge (bd-sf8dx).
    ///
    /// The caller passes a fresh `next_segid` (one past the current max) and,
    /// on success, appends `leaf_data_row`, replaces `structure_data_row` and
    /// `averages_data_row`, and appends `docsize_rows` to `_docsize`. Live
    /// MATCH keeps reading the in-memory `self.index`; reopen rehydrates from
    /// the multi-segment structure via `collect_live_term_groups`, so the extra
    /// segments are transparent to readers.
    ///
    /// Returns `None` when `new_docs` is empty (nothing to persist).
    pub fn encode_incremental_insert_flush(
        &self,
        new_docs: &[(i64, &[SqliteValue])],
        existing_structure: &Fts5StructureRecord,
        existing_averages: &Fts5AveragesRecord,
        next_segid: u32,
    ) -> Result<Option<Fts5IncrementalInsertFlush>> {
        if new_docs.is_empty() {
            return Ok(None);
        }
        let tokenizer = self.create_tokenizer_instance();
        // A delta index holding ONLY this statement's rows, tokenized exactly as
        // the live insert (`index_document_with_tokenizer` -> `add_text`), so
        // its postings and docsizes match the in-memory index for these rows.
        let mut delta = InvertedIndex::with_options_and_tokendata(
            self.config.columnsize_enabled(),
            &self.prefix_lengths,
            self.config.detail_mode(),
            self.config.tokendata_enabled(),
        );
        let mut new_rowids = Vec::with_capacity(new_docs.len());
        for (rowid, column_args) in new_docs.iter().copied() {
            new_rowids.push(rowid);
            let DecodedColumnValues { values, .. } = self.decode_column_values(column_args)?;
            #[allow(clippy::cast_possible_truncation)]
            for (col_idx, text) in values.iter().enumerate() {
                if matches!(self.indexed_columns.get(col_idx), Some(false)) {
                    continue;
                }
                delta.add_text(rowid, col_idx as u32, tokenizer.as_ref(), text);
            }
        }

        // Per-column docsize rows for the new docs. `delta` holds only the new
        // docs, so this canonical accumulation is O(new) yet row-for-row
        // identical to the full-encode path for these rowids.
        let docsize_rows =
            delta.accumulate_docsize_rows(self.columns.len(), new_rowids.iter().copied());

        // Updated averages = old running totals + the new docs' contributions.
        let mut column_token_totals = existing_averages.column_token_totals.clone();
        if column_token_totals.len() < self.columns.len() {
            column_token_totals.resize(self.columns.len(), 0);
        }
        for row in &docsize_rows {
            for (col, count) in row.column_token_counts.iter().copied().enumerate() {
                if let Some(total) = column_token_totals.get_mut(col) {
                    *total = total.saturating_add(u64::from(count));
                }
            }
        }
        let total_rows = existing_averages
            .total_rows
            .saturating_add(u64::try_from(new_docs.len()).unwrap_or(u64::MAX));
        let averages_data_row = Fts5DataRow::new(
            FTS5_AVERAGES_ROWID,
            Fts5AveragesRecord::new(total_rows, column_token_totals).encode(),
        );

        // Build and append the new segment. When the new rows produced no
        // indexable tokens, leave the structure unchanged but still advance the
        // averages/docsize (matching the full path's row accounting).
        let pending = delta.build_pending_hash(&self.prefix_lengths)?;
        if pending.is_empty() {
            return Ok(Some(Fts5IncrementalInsertFlush {
                leaf_data_row: None,
                structure_data_row: Fts5DataRow::new(
                    FTS5_STRUCTURE_ROWID,
                    existing_structure.encode(),
                ),
                averages_data_row,
                docsize_rows,
            }));
        }
        let flush = pending.flush_to_segment(next_segid, existing_structure.clone())?;
        let structure_data_row = Fts5DataRow::new(FTS5_STRUCTURE_ROWID, flush.structure.encode());
        let leaf_data_row = flush
            .data_rows
            .into_iter()
            .find(|row| row.id != FTS5_STRUCTURE_ROWID);
        Ok(Some(Fts5IncrementalInsertFlush {
            leaf_data_row,
            structure_data_row,
            averages_data_row,
            docsize_rows,
        }))
    }

    #[must_use]
    pub fn encode_shadow_rows(&self) -> Fts5ShadowRows {
        Fts5ShadowRows {
            data: self.encode_data_rows(),
            idx: Vec::new(),
            config: self.encode_config_rows(),
            content: self.encode_content_rows(),
            docsize: self.encode_docsize_rows(),
        }
    }

    pub fn apply_shadow_rows(&mut self, rows: &Fts5ShadowRows) -> Result<Fts5ConfigMetadata> {
        let report = rows.validate_for_open(self.columns.len())?;
        if Self::shadow_rows_have_persisted_segments(&report) {
            self.bind_validated_shadow_rows(rows.clone(), &report);
            return Ok(report.metadata);
        }

        let metadata = self.apply_config_rows(&rows.config)?;
        if self.config.content_mode() != ContentMode::Contentless || !rows.content.is_empty() {
            self.apply_content_rows(&rows.content);
        } else {
            self.rebuild_documents(Vec::new());
        }
        self.apply_docsize_rows(&rows.docsize);
        Ok(metadata)
    }

    /// Get column names.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    #[must_use]
    pub fn indexed_columns(&self) -> &[bool] {
        &self.indexed_columns
    }

    #[must_use]
    pub fn index(&self) -> &InvertedIndex {
        &self.index
    }
}

/// Extract all leaf-level terms from an expression tree for BM25 scoring.
fn extract_query_terms(expr: &Fts5Expr) -> Vec<String> {
    fn near_operand_terms(operand: &Fts5NearOperand) -> Vec<String> {
        match operand {
            Fts5NearOperand::Term(term) | Fts5NearOperand::Prefix(term) => vec![term.clone()],
            Fts5NearOperand::Phrase(terms) => terms.clone(),
        }
    }

    match expr {
        Fts5Expr::Term(t) => vec![t.clone()],
        Fts5Expr::Prefix(p) => vec![p.clone()],
        Fts5Expr::Phrase(words) => words.clone(),
        Fts5Expr::PhrasePrefix(words, prefix) => {
            let mut terms = words.clone();
            terms.push(prefix.clone());
            terms
        }
        Fts5Expr::And(l, r) | Fts5Expr::Or(l, r) => {
            let mut terms = extract_query_terms(l);
            terms.extend(extract_query_terms(r));
            terms
        }
        Fts5Expr::Not(left, _) => extract_query_terms(left),
        Fts5Expr::Near(operands, _) => operands.iter().flat_map(near_operand_terms).collect(),
        Fts5Expr::ColumnFilter(_, inner) | Fts5Expr::InitialToken(inner) => {
            extract_query_terms(inner)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fts5HighlightTerm {
    term: String,
    prefix: bool,
}

impl Fts5HighlightTerm {
    fn exact(term: &str) -> Self {
        Self {
            term: term.to_lowercase(),
            prefix: false,
        }
    }

    fn prefix(term: &str) -> Self {
        Self {
            term: term.to_lowercase(),
            prefix: true,
        }
    }

    fn matches_token(&self, token: &str) -> bool {
        if self.prefix {
            token.starts_with(&self.term)
        } else {
            token.eq(self.term.as_str())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fts5HighlightPattern {
    parts: Vec<Fts5HighlightTerm>,
}

impl Fts5HighlightPattern {
    fn singleton(term: Fts5HighlightTerm) -> Self {
        Self { parts: vec![term] }
    }

    fn exact_phrase(words: &[String]) -> Self {
        Self {
            parts: words
                .iter()
                .map(|word| Fts5HighlightTerm::exact(word))
                .collect(),
        }
    }

    fn phrase_prefix(words: &[String], prefix: &str) -> Self {
        let mut parts = Vec::with_capacity(words.len() + 1);
        parts.extend(words.iter().map(|word| Fts5HighlightTerm::exact(word)));
        parts.push(Fts5HighlightTerm::prefix(prefix));
        Self { parts }
    }

    fn token_count(&self) -> usize {
        self.parts.len()
    }

    fn matches_at(&self, tokens: &[Fts5Token], start: usize) -> Option<usize> {
        let end = start.checked_add(self.parts.len())?;
        if self.parts.is_empty() || end > tokens.len() {
            return None;
        }

        tokens
            .get(start..end)?
            .iter()
            .zip(&self.parts)
            .all(|(token, part)| part.matches_token(&token.term))
            .then_some(end)
    }
}

fn highlight_patterns_from_terms(terms: &[Fts5HighlightTerm]) -> Vec<Fts5HighlightPattern> {
    let mut patterns = Vec::with_capacity(terms.len());
    patterns.extend(terms.iter().cloned().map(Fts5HighlightPattern::singleton));
    patterns
}

#[cfg(test)]
fn extract_highlight_terms(expr: &Fts5Expr) -> Vec<Fts5HighlightTerm> {
    fn near_operand_terms(operand: &Fts5NearOperand) -> Vec<Fts5HighlightTerm> {
        match operand {
            Fts5NearOperand::Term(term) => vec![Fts5HighlightTerm::exact(term)],
            Fts5NearOperand::Prefix(prefix) => vec![Fts5HighlightTerm::prefix(prefix)],
            Fts5NearOperand::Phrase(terms) => terms
                .iter()
                .map(|term| Fts5HighlightTerm::exact(term))
                .collect(),
        }
    }

    match expr {
        Fts5Expr::Term(term) => vec![Fts5HighlightTerm::exact(term)],
        Fts5Expr::Prefix(prefix) => vec![Fts5HighlightTerm::prefix(prefix)],
        Fts5Expr::Phrase(words) => words
            .iter()
            .map(|term| Fts5HighlightTerm::exact(term))
            .collect(),
        Fts5Expr::PhrasePrefix(words, prefix) => {
            let mut terms: Vec<Fts5HighlightTerm> = words
                .iter()
                .map(|term| Fts5HighlightTerm::exact(term))
                .collect();
            terms.push(Fts5HighlightTerm::prefix(prefix));
            terms
        }
        Fts5Expr::And(left, right) | Fts5Expr::Or(left, right) => {
            let mut terms = extract_highlight_terms(left);
            terms.extend(extract_highlight_terms(right));
            terms
        }
        Fts5Expr::Not(left, _) => extract_highlight_terms(left),
        Fts5Expr::Near(operands, _) => operands.iter().flat_map(near_operand_terms).collect(),
        Fts5Expr::ColumnFilter(_, inner) | Fts5Expr::InitialToken(inner) => {
            extract_highlight_terms(inner)
        }
    }
}

fn extract_highlight_patterns(expr: &Fts5Expr) -> Vec<Fts5HighlightPattern> {
    fn near_operand_patterns(operand: &Fts5NearOperand) -> Vec<Fts5HighlightPattern> {
        match operand {
            Fts5NearOperand::Term(term) => {
                vec![Fts5HighlightPattern::singleton(Fts5HighlightTerm::exact(
                    term,
                ))]
            }
            Fts5NearOperand::Prefix(prefix) => {
                vec![Fts5HighlightPattern::singleton(Fts5HighlightTerm::prefix(
                    prefix,
                ))]
            }
            Fts5NearOperand::Phrase(terms) => {
                vec![Fts5HighlightPattern::exact_phrase(terms)]
            }
        }
    }

    match expr {
        Fts5Expr::Term(term) => vec![Fts5HighlightPattern::singleton(Fts5HighlightTerm::exact(
            term,
        ))],
        Fts5Expr::Prefix(prefix) => {
            vec![Fts5HighlightPattern::singleton(Fts5HighlightTerm::prefix(
                prefix,
            ))]
        }
        Fts5Expr::Phrase(words) => vec![Fts5HighlightPattern::exact_phrase(words)],
        Fts5Expr::PhrasePrefix(words, prefix) => {
            vec![Fts5HighlightPattern::phrase_prefix(words, prefix)]
        }
        Fts5Expr::And(left, right) | Fts5Expr::Or(left, right) => {
            let mut patterns = extract_highlight_patterns(left);
            patterns.extend(extract_highlight_patterns(right));
            patterns
        }
        Fts5Expr::Not(left, _) => extract_highlight_patterns(left),
        Fts5Expr::Near(operands, _) => operands.iter().flat_map(near_operand_patterns).collect(),
        Fts5Expr::ColumnFilter(_, inner) | Fts5Expr::InitialToken(inner) => {
            extract_highlight_patterns(inner)
        }
    }
}

// ---------------------------------------------------------------------------
// VirtualTable implementation
// ---------------------------------------------------------------------------

impl VirtualTable for Fts5Table {
    type Cursor = Fts5Cursor;

    fn module_metadata(_args: &[&str]) -> VtabModuleMetadata
    where
        Self: Sized,
    {
        VtabModuleMetadata::shadow_owning(
            VtabLifecyclePolicy::SeparateCreateAndConnect,
            VtabIntegrityPolicy::ShadowAware,
            VtabRiskLevel::innocuous(),
        )
    }

    fn shadow_table_policy(vtab_name: &str, table_name: &str) -> ShadowTablePolicy
    where
        Self: Sized,
    {
        let lower_table_name = table_name.to_ascii_lowercase();
        let lower_vtab_name = vtab_name.to_ascii_lowercase();
        let Some(suffix) = lower_table_name
            .strip_prefix(&lower_vtab_name)
            .filter(|suffix| suffix.starts_with('_'))
        else {
            return ShadowTablePolicy::ordinary();
        };

        match suffix {
            "_data" | "_idx" | "_config" | "_content" | "_docsize" => {
                ShadowTablePolicy::owned_shadow()
            }
            _ => ShadowTablePolicy::ordinary(),
        }
    }

    fn connect(_cx: &Cx, args: &[&str]) -> Result<Self>
    where
        Self: Sized,
    {
        let mut columns: Vec<String> = Vec::new();
        let mut indexed_columns: Vec<bool> = Vec::new();
        let mut config = Fts5Config::default();
        let mut tokenizer_name = "unicode61".to_owned();
        let mut prefix_lengths = Vec::new();

        if args.len() > 3 {
            for raw in &args[3..] {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if let Some((key, value)) = parse_option_assignment(trimmed) {
                    let key_lower = key.to_ascii_lowercase();
                    let value_unquoted_raw = unquote_fts_arg(value);
                    let value_unquoted = value_unquoted_raw.to_ascii_lowercase();
                    match key_lower.as_str() {
                        "tokenize" => {
                            if create_tokenizer(value_unquoted_raw).is_none() {
                                return Err(FrankenError::function_error(
                                    "fts5: unsupported tokenizer specification",
                                ));
                            }
                            value_unquoted_raw.clone_into(&mut tokenizer_name);
                        }
                        "content" => {
                            if value_unquoted.is_empty() {
                                config.content_mode = ContentMode::Contentless;
                            } else {
                                config.content_mode = ContentMode::Stored;
                            }
                        }
                        "content_rowid" => {
                            // Names the rowid column of the external content
                            // table (SQLite FTS5 external_content_tables). The
                            // declared rowid IS the implicit rowid for fsqlite's
                            // engine, and external-content synchronization is
                            // handled at the trigger layer rather than inside
                            // the vtab, so this option is informational here.
                            // Match upstream SQLite/rusqlite by accepting any
                            // non-empty identifier and rejecting an empty value.
                            if value_unquoted_raw.is_empty() {
                                return Err(FrankenError::function_error(
                                    "fts5: content_rowid must name a column",
                                ));
                            }
                        }
                        "contentless_delete" => {
                            config.contentless_delete = parse_columnsize_option(
                                value_unquoted.as_str(),
                            )
                            .ok_or_else(|| {
                                FrankenError::function_error(
                                    "fts5: contentless_delete must be 0 or 1",
                                )
                            })?;
                        }
                        "secure_delete" | "secure-delete" => {
                            config.secure_delete = parse_bool_like(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error(
                                        "fts5: secure_delete must be a boolean value",
                                    )
                                })?;
                        }
                        "contentless_unindexed" => {
                            config.contentless_unindexed = parse_columnsize_option(
                                value_unquoted.as_str(),
                            )
                            .ok_or_else(|| {
                                FrankenError::function_error(
                                    "fts5: contentless_unindexed must be 0 or 1",
                                )
                            })?;
                        }
                        "columnsize" => {
                            config.columnsize = parse_columnsize_option(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error("fts5: columnsize must be 0 or 1")
                                })?;
                        }
                        "detail" => {
                            config.detail = parse_detail_option(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error(
                                        "fts5: detail must be full, column, or none",
                                    )
                                })?;
                        }
                        "prefix" => {
                            prefix_lengths.extend(
                                parse_prefix_option(value_unquoted.as_str()).ok_or_else(|| {
                                    FrankenError::function_error(
                                        "fts5: prefix must be a whitespace separated list of positive integers",
                                    )
                                })?,
                            );
                        }
                        "insttoken" => {
                            config.insttoken = parse_bool_like(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error(
                                        "fts5: insttoken must be a boolean value",
                                    )
                                })?;
                        }
                        "locale" => {
                            config.locale = parse_columnsize_option(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error("fts5: locale must be 0 or 1")
                                })?;
                        }
                        "tokendata" => {
                            config.tokendata = parse_columnsize_option(value_unquoted.as_str())
                                .ok_or_else(|| {
                                    FrankenError::function_error("fts5: tokendata must be 0 or 1")
                                })?;
                        }
                        _ => {
                            return Err(FrankenError::function_error(format!(
                                "fts5: unsupported option '{key}'"
                            )));
                        }
                    }
                    continue;
                }

                if let Some((column, indexed)) = parse_column_declaration(trimmed)? {
                    columns.push(column);
                    indexed_columns.push(indexed);
                }
            }
        }

        validate_contentless_options(config)?;

        if columns.is_empty() {
            columns.push("content".to_owned());
            indexed_columns.push(true);
        }
        validate_column_names(args.get(2).copied().unwrap_or_default(), &columns)?;
        prefix_lengths.sort_unstable();
        prefix_lengths.dedup();

        debug!(
            columns = ?columns,
            tokenizer = %tokenizer_name,
            content_mode = ?config.content_mode,
            secure_delete = config.secure_delete,
            contentless_delete = config.contentless_delete,
            contentless_unindexed = config.contentless_unindexed,
            detail = ?config.detail_mode(),
            insttoken = config.insttoken,
            locale = config.locale,
            tokendata = config.tokendata,
            indexed_columns = ?indexed_columns,
            prefix_lengths = ?prefix_lengths,
            "fts5: connecting virtual table"
        );

        let mut table = Self::with_columns(columns);
        table.indexed_columns = indexed_columns;
        table.config = config;
        table.tokenizer_name = tokenizer_name;
        table.prefix_lengths = prefix_lengths;
        table.index = InvertedIndex::with_options_and_tokendata(
            table.config.columnsize_enabled(),
            &table.prefix_lengths,
            table.config.detail_mode(),
            table.config.tokendata_enabled(),
        );
        Ok(table)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Check if there's a MATCH constraint.
        let mut has_match = false;
        let mut next_argv_index = 1;
        for (i, constraint) in info.constraints.iter().enumerate() {
            if constraint.op == fsqlite_func::vtab::ConstraintOp::Match && constraint.usable {
                info.constraint_usage[i].argv_index = next_argv_index;
                info.constraint_usage[i].omit = true;
                has_match = true;
                next_argv_index += 1;
            }
        }

        if has_match {
            info.estimated_cost = 10.0;
            info.estimated_rows = 10;
            info.idx_num = 1; // MATCH query
        } else {
            info.estimated_cost = 1_000_000.0;
            #[allow(clippy::cast_possible_wrap)]
            {
                info.estimated_rows = self.documents.len() as i64;
            }
            info.idx_num = 0; // full scan
        }

        Ok(())
    }

    fn open(&self) -> Result<Fts5Cursor> {
        Ok(Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: self.columns.clone(),
            tokenizer_name: self.tokenizer_name.clone(),
            detail: self.config.detail_mode(),
            index: self.index.clone(),
            documents: self.documents.clone(),
            shadow_rows: self.shadow_rows.clone(),
        })
    }

    fn begin(&mut self, _cx: &Cx) -> Result<()> {
        self.txn_state.begin(self.snapshot_state());
        Ok(())
    }

    fn sync_txn(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    fn update(&mut self, _cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        if args.is_empty() {
            return Err(FrankenError::function_error("fts5: empty update args"));
        }

        // DELETE: args[0] = old rowid, args len == 1
        if args.len() == 1 && !args[0].is_null() {
            let rowid = args[0].to_integer();
            if self.config.content_mode == ContentMode::Contentless
                && !self.config.contentless_delete
            {
                return Err(FrankenError::function_error(
                    "fts5: cannot delete from contentless table without contentless_delete=1",
                ));
            }
            if !self.has_document_or_shadow_rowid(rowid) {
                return Ok(None);
            }
            self.ensure_shadow_rows_materializable_for_mutation()?;
            self.delete_document(rowid);
            return Ok(None);
        }

        // INSERT: args[0] = Null (no old rowid)
        if args[0].is_null() {
            let rowid = if args.len() > 1 && !args[1].is_null() {
                args[1].to_integer()
            } else {
                if self.config.content_mode == ContentMode::Contentless
                    && !self.config.columnsize_enabled()
                {
                    return Err(FrankenError::function_error(
                        "fts5: contentless tables with columnsize=0 require an explicit rowid",
                    ));
                }
                let r = self.next_rowid;
                self.next_rowid += 1;
                r
            };
            if self.has_document_or_shadow_rowid(rowid) {
                return Err(FrankenError::PrimaryKeyViolation);
            }
            self.ensure_shadow_rows_materializable_for_mutation()?;

            let column_args = match args.get(2..) {
                Some(values) => values,
                None => &[],
            };
            let DecodedColumnValues {
                values: col_values,
                locales,
            } = self.decode_column_values(column_args)?;
            let tokenizer = self.create_tokenizer_instance();
            self.store_document_with_tokenizer_and_locales(
                rowid,
                col_values,
                locales,
                tokenizer.as_ref(),
            );
            return Ok(Some(rowid));
        }

        // UPDATE: validate rowid movement before mutating so conflict failures
        // preserve the old row and its index postings.
        let old_rowid = args[0].to_integer();
        if self.config.content_mode == ContentMode::Contentless && !self.config.contentless_delete {
            return Err(FrankenError::function_error(
                "fts5: cannot update contentless table without contentless_delete=1",
            ));
        }
        let new_rowid = if args.len() > 1 && !args[1].is_null() {
            args[1].to_integer()
        } else {
            old_rowid
        };
        if old_rowid != new_rowid && self.has_document_or_shadow_rowid(new_rowid) {
            return Err(FrankenError::PrimaryKeyViolation);
        }
        if !self.has_document_or_shadow_rowid(old_rowid) {
            return Err(FrankenError::Internal(
                "fts5 update referenced a missing rowid".to_owned(),
            ));
        }
        self.ensure_shadow_rows_materializable_for_mutation()?;

        let column_args = match args.get(2..) {
            Some(values) => values,
            None => &[],
        };
        let DecodedColumnValues {
            values: col_values,
            locales,
        } = self.decode_column_values(column_args)?;
        self.delete_document(old_rowid);
        let tokenizer = self.create_tokenizer_instance();
        self.store_document_with_tokenizer_and_locales(
            new_rowid,
            col_values,
            locales,
            tokenizer.as_ref(),
        );
        Ok(None)
    }

    fn commit(&mut self, _cx: &Cx) -> Result<()> {
        self.txn_state.commit();
        Ok(())
    }

    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        let snapshot = self.txn_state.rollback();
        self.restore_transaction_snapshot(snapshot);
        Ok(())
    }

    fn savepoint(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        self.txn_state.savepoint(n, self.snapshot_state());
        Ok(())
    }

    fn release(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        self.txn_state.release(n);
        Ok(())
    }

    fn rollback_to(&mut self, _cx: &Cx, n: i32) -> Result<()> {
        let snapshot = self.txn_state.rollback_to(n);
        self.restore_transaction_snapshot(snapshot);
        Ok(())
    }
}

/// FTS5 cursor for scanning query results.
#[derive(Debug)]
pub struct Fts5Cursor {
    /// Matching (rowid, score) tuples.
    results: Vec<(i64, f64)>,
    /// Current position in results.
    position: usize,
    /// Column names.
    columns: Vec<String>,
    /// Tokenizer spec copied from the table at cursor-open time.
    tokenizer_name: String,
    /// Detail mode copied from the table at cursor-open time.
    detail: DetailMode,
    /// Snapshot of the inverted index at cursor-open time.
    index: InvertedIndex,
    /// Snapshot of stored documents at cursor-open time.
    documents: HashMap<i64, Vec<String>>,
    /// Shadow-table rows copied from an opened stock FTS5 table.
    shadow_rows: Option<Fts5ShadowRows>,
}

impl VirtualTableCursor for Fts5Cursor {
    fn filter(
        &mut self,
        _cx: &Cx,
        idx_num: i32,
        _idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        self.results.clear();
        self.position = 0;

        if idx_num == 1 {
            // MATCH query: every filter arg is an additional MATCH expression
            // on the same table, combined with AND like SQLite does.
            if !args.is_empty() {
                let weights: Vec<f64> = self.columns.iter().map(|_| 1.0).collect();
                let queries: Vec<String> = args.iter().map(SqliteValue::to_text).collect();
                let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();
                let tokenizer = create_tokenizer(&self.tokenizer_name)
                    .unwrap_or_else(|| Box::new(Unicode61Tokenizer::new()));
                self.results = if let Some(rows) = self.shadow_rows.as_ref() {
                    Fts5ShadowQuery::new(rows, &self.columns, tokenizer.as_ref(), self.detail)
                        .and_then(|query| query.search_queries_with_weights(&query_refs, &weights))
                } else {
                    search_docids_with_weights_from_parts(
                        &self.index,
                        &self.columns,
                        &query_refs,
                        &weights,
                        Some(tokenizer.as_ref()),
                    )
                }
                .map_err(|e| FrankenError::function_error(format!("fts5 query error: {e}")))?;
            }
        } else {
            // Full table scan (idx_num == 0): return all documents.
            let mut rows: Vec<(i64, f64)> = if let Some(shadow_rows) = self.shadow_rows.as_ref() {
                shadow_rows
                    .full_scan_rows()
                    .into_iter()
                    .map(|(rowid, _)| (rowid, 0.0))
                    .collect()
            } else {
                self.documents
                    .keys()
                    .copied()
                    .map(|rowid| (rowid, 0.0))
                    .collect()
            };
            rows.sort_by_key(|(rowid, _)| *rowid);
            self.results = rows;
        }

        Ok(())
    }

    fn next(&mut self, _cx: &Cx) -> Result<()> {
        self.position += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.position >= self.results.len()
    }

    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        let Some((rowid, score)) = self.results.get(self.position) else {
            ctx.set_value(SqliteValue::Null);
            return Ok(());
        };

        #[allow(clippy::cast_sign_loss)]
        let col_idx = col as usize;

        let is_rank_column = col == -1 || (col >= 0 && col_idx == self.columns.len());

        // Column -1 or column == num_columns is the rank.
        if is_rank_column {
            ctx.set_value(SqliteValue::Float(*score));
        } else if let Some(cols) = self.documents.get(rowid)
            && let Some(val) = cols.get(col_idx)
        {
            ctx.set_value(SqliteValue::Text(SmallText::new(val.as_str())));
        } else if let Some(rows) = self.shadow_rows.as_ref()
            && let Some(cols) = shadow_content_for_rowid(rows, *rowid)
            && let Some(val) = cols.get(col_idx)
        {
            ctx.set_value(SqliteValue::Text(SmallText::new(val.as_str())));
        } else {
            ctx.set_value(SqliteValue::Null);
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        self.results
            .get(self.position)
            .map_or(Ok(0), |(rowid, _)| Ok(*rowid))
    }
}

impl Fts5Cursor {
    /// Set the query results for this cursor (used in integrated search).
    pub fn set_results(&mut self, results: Vec<(i64, f64, Vec<String>)>) {
        self.documents = results
            .iter()
            .map(|(rowid, _, columns)| (*rowid, columns.clone()))
            .collect();
        self.results = results
            .into_iter()
            .map(|(rowid, score, _columns)| (rowid, score))
            .collect();
        self.position = 0;
    }
}

// ---------------------------------------------------------------------------
// Highlight / Snippet helpers
// ---------------------------------------------------------------------------

/// Generate a highlighted version of text with matching terms wrapped in
/// markers.
#[must_use]
pub fn highlight(text: &str, terms: &[String], open_tag: &str, close_tag: &str) -> String {
    let highlight_terms: Vec<Fts5HighlightTerm> = terms
        .iter()
        .map(|term| Fts5HighlightTerm::exact(term))
        .collect();
    let patterns = highlight_patterns_from_terms(&highlight_terms);
    highlight_with_patterns(text, &patterns, open_tag, close_tag)
}

#[cfg(test)]
fn highlight_with_terms(
    text: &str,
    terms: &[Fts5HighlightTerm],
    open_tag: &str,
    close_tag: &str,
) -> String {
    let patterns = highlight_patterns_from_terms(terms);
    highlight_with_patterns(text, &patterns, open_tag, close_tag)
}

fn highlight_spans(tokens: &[Fts5Token], patterns: &[Fts5HighlightPattern]) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut token_index = 0;

    while token_index < tokens.len() {
        let best_match = patterns
            .iter()
            .filter(|pattern| pattern.token_count() > 0)
            .filter_map(|pattern| {
                pattern
                    .matches_at(tokens, token_index)
                    .map(|end_index| (end_index, pattern.token_count()))
            })
            .max_by_key(|(end_index, token_count)| (*end_index, *token_count));

        if let Some((end_index, _)) = best_match {
            if let (Some(first), Some(last)) = (
                tokens.get(token_index),
                tokens.get(end_index.saturating_sub(1)),
            ) {
                match spans.last_mut() {
                    Some((_, previous_end)) if first.start < *previous_end => {
                        *previous_end = (*previous_end).max(last.end);
                    }
                    _ => spans.push((first.start, last.end)),
                }
            }
            token_index = end_index;
        } else {
            token_index += 1;
        }
    }

    spans
}

fn highlight_with_patterns(
    text: &str,
    patterns: &[Fts5HighlightPattern],
    open_tag: &str,
    close_tag: &str,
) -> String {
    if patterns.is_empty() {
        return text.to_owned();
    }

    let tokenizer = Unicode61Tokenizer::new();
    let tokens = tokenizer.tokenize(text);
    let spans = highlight_spans(&tokens, patterns);

    let mut result =
        String::with_capacity(text.len() + spans.len() * (open_tag.len() + close_tag.len()));
    let mut last_end = 0;

    for (start, end) in spans {
        result.push_str(&text[last_end..start]);
        result.push_str(open_tag);
        result.push_str(&text[start..end]);
        result.push_str(close_tag);
        last_end = end;
    }

    result.push_str(&text[last_end..]);
    result
}

/// Generate a snippet of text around matching terms.
#[must_use]
#[allow(clippy::similar_names)]
pub fn snippet(
    text: &str,
    terms: &[String],
    open_tag: &str,
    close_tag: &str,
    ellipsis: &str,
    max_tokens: usize,
) -> String {
    let highlight_terms: Vec<Fts5HighlightTerm> = terms
        .iter()
        .map(|term| Fts5HighlightTerm::exact(term))
        .collect();
    let patterns = highlight_patterns_from_terms(&highlight_terms);
    snippet_with_patterns(text, &patterns, open_tag, close_tag, ellipsis, max_tokens)
}

#[cfg(test)]
fn snippet_with_terms(
    text: &str,
    terms: &[Fts5HighlightTerm],
    open_tag: &str,
    close_tag: &str,
    ellipsis: &str,
    max_tokens: usize,
) -> String {
    let patterns = highlight_patterns_from_terms(terms);
    snippet_with_patterns(text, &patterns, open_tag, close_tag, ellipsis, max_tokens)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fts5SnippetWindow {
    first: usize,
    last_exclusive: usize,
    distinct: u32,
    total: u32,
}

fn score_snippet_window(
    tokens: &[Fts5Token],
    patterns: &[Fts5HighlightPattern],
    first: usize,
    last_exclusive: usize,
) -> Fts5SnippetWindow {
    const MAX_PATTERN_BITS: usize = 128;

    let mut seen = 0_u128;
    let mut overflow_seen = SmallVec::<[usize; 4]>::new();
    let mut total = 0_u32;

    for token_index in first..last_exclusive {
        for (pattern_index, pattern) in patterns.iter().enumerate() {
            if pattern
                .matches_at(tokens, token_index)
                .is_some_and(|match_end| match_end <= last_exclusive)
            {
                total = total.saturating_add(1);
                if pattern_index < MAX_PATTERN_BITS {
                    seen |= 1_u128 << pattern_index;
                } else if !overflow_seen.contains(&pattern_index) {
                    overflow_seen.push(pattern_index);
                }
            }
        }
    }

    let overflow_distinct = u32::try_from(overflow_seen.len()).unwrap_or(u32::MAX);
    Fts5SnippetWindow {
        first,
        last_exclusive,
        distinct: seen.count_ones().saturating_add(overflow_distinct),
        total,
    }
}

fn snippet_window_is_better(candidate: Fts5SnippetWindow, best: Fts5SnippetWindow) -> bool {
    candidate.distinct > best.distinct
        || (candidate.distinct == best.distinct
            && (candidate.total > best.total
                || (candidate.total == best.total && candidate.first < best.first)))
}

fn select_snippet_window(
    tokens: &[Fts5Token],
    patterns: &[Fts5HighlightPattern],
    max_tokens: usize,
) -> Fts5SnippetWindow {
    let token_count = max_tokens.min(tokens.len());
    if token_count == 0 {
        return Fts5SnippetWindow {
            first: 0,
            last_exclusive: 0,
            distinct: 0,
            total: 0,
        };
    }

    let mut best = score_snippet_window(tokens, patterns, 0, token_count);
    for first in 1..=tokens.len() - token_count {
        let candidate = score_snippet_window(tokens, patterns, first, first + token_count);
        if snippet_window_is_better(candidate, best) {
            best = candidate;
        }
    }

    best
}

#[allow(clippy::similar_names)]
fn snippet_with_patterns(
    text: &str,
    patterns: &[Fts5HighlightPattern],
    open_tag: &str,
    close_tag: &str,
    ellipsis: &str,
    max_tokens: usize,
) -> String {
    if max_tokens == 0 {
        return String::new();
    }

    let tokenizer = Unicode61Tokenizer::new();
    let tokens = tokenizer.tokenize(text);

    let window = select_snippet_window(&tokens, patterns, max_tokens);

    let mut result = String::new();
    if window.first > 0 {
        result.push_str(ellipsis);
    }

    let window_tokens = &tokens[window.first..window.last_exclusive];
    if let (Some(first), Some(last)) = (window_tokens.first(), window_tokens.last()) {
        let slice = &text[first.start..last.end];

        // Highlight matching terms within the snippet.
        result.push_str(&highlight_with_patterns(
            slice, patterns, open_tag, close_tag,
        ));
    }

    if window.last_exclusive < tokens.len() {
        result.push_str(ellipsis);
    }

    result
}

// ---------------------------------------------------------------------------
// Scalar functions for FTS5
// ---------------------------------------------------------------------------

fn fallback_highlight_terms(query: &str) -> Vec<Fts5HighlightTerm> {
    let mut terms = Vec::new();
    let mut skip_next_leaf = false;

    for raw in query.split_whitespace() {
        let trimmed = raw
            .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '(' | ')' | ','))
            .trim_start_matches('^')
            .to_ascii_lowercase();
        let prefix = trimmed.ends_with('*');
        let term = trimmed.trim_end_matches('*');
        if term.is_empty() || matches!(term, "and" | "or" | "near") {
            continue;
        }
        if term == "not" {
            skip_next_leaf = true;
            continue;
        }
        if skip_next_leaf {
            skip_next_leaf = false;
            continue;
        }
        if prefix {
            terms.push(Fts5HighlightTerm::prefix(term));
        } else {
            terms.push(Fts5HighlightTerm::exact(term));
        }
    }

    terms
}

fn fallback_highlight_patterns(query: &str) -> Vec<Fts5HighlightPattern> {
    highlight_patterns_from_terms(&fallback_highlight_terms(query))
}

fn highlight_patterns_from_query_text(query: &str) -> Vec<Fts5HighlightPattern> {
    parse_fts5_query(query)
        .and_then(|tokens| build_expr(&tokens))
        .map_or_else(
            |_| fallback_highlight_patterns(query),
            |expr| extract_highlight_patterns(&expr),
        )
}

#[cfg(test)]
fn highlight_terms_from_query_text(query: &str) -> Vec<Fts5HighlightTerm> {
    parse_fts5_query(query)
        .and_then(|tokens| build_expr(&tokens))
        .map_or_else(
            |_| fallback_highlight_terms(query),
            |expr| extract_highlight_terms(&expr),
        )
}

/// highlight(text, query, open, close) — highlight FTS5 query terms in text.
pub struct Fts5HighlightFunc;

impl ScalarFunction for Fts5HighlightFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.iter().any(|arg| matches!(arg, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }

        let text = args[0].to_text();
        let query = args[1].to_text();
        let open_tag = args[2].to_text();
        let close_tag = args[3].to_text();
        let patterns = highlight_patterns_from_query_text(&query);

        Ok(SqliteValue::Text(SmallText::from_string(
            highlight_with_patterns(&text, &patterns, &open_tag, &close_tag),
        )))
    }

    fn num_args(&self) -> i32 {
        4
    }

    fn name(&self) -> &'static str {
        "highlight"
    }
}

/// snippet(text, query, open, close, ellipsis, max_tokens) — derive a
/// highlighted snippet from text using FTS5 query terms.
pub struct Fts5SnippetFunc;

impl ScalarFunction for Fts5SnippetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.iter().any(|arg| matches!(arg, SqliteValue::Null)) {
            return Ok(SqliteValue::Null);
        }

        let text = args[0].to_text();
        let query = args[1].to_text();
        let open_tag = args[2].to_text();
        let close_tag = args[3].to_text();
        let ellipsis = args[4].to_text();
        let max_tokens = usize::try_from(args[5].to_integer()).unwrap_or(0);
        let patterns = highlight_patterns_from_query_text(&query);

        Ok(SqliteValue::Text(SmallText::from_string(
            snippet_with_patterns(
                &text, &patterns, &open_tag, &close_tag, &ellipsis, max_tokens,
            ),
        )))
    }

    fn num_args(&self) -> i32 {
        6
    }

    fn name(&self) -> &'static str {
        "snippet"
    }
}

/// fts5_source_id() — returns the FTS5 extension version string.
pub struct Fts5SourceIdFunc;

impl ScalarFunction for Fts5SourceIdFunc {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Text(SmallText::new(
            "fts5: FrankenSQLite FTS5 extension",
        )))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &'static str {
        "fts5_source_id"
    }
}

/// fts5_insttoken(query) preserves and marks its MATCH argument.
///
/// `SqliteValue` does not carry SQLite subtypes yet, so this mirrors SQLite's
/// value-preserving behavior and leaves subtype-aware planning to the VDBE
/// boundary.
pub struct Fts5InsttokenFunc;

impl ScalarFunction for Fts5InsttokenFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(value) = args.first() else {
            return Err(FrankenError::function_error(
                "fts5_insttoken() expects 1 argument",
            ));
        };
        Ok(value.to_owned())
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "fts5_insttoken"
    }
}

const FTS5_LOCALE_HEADER: [u8; 4] = [0x00, 0xE0, 0xB2, 0xEB];

fn sqlite_text_for_fts5_locale(value: &SqliteValue) -> Option<Cow<'_, str>> {
    match value {
        SqliteValue::Null => None,
        SqliteValue::Text(text) => Some(Cow::Borrowed(text.as_str())),
        _ => Some(Cow::Owned(value.to_text())),
    }
}

fn encode_fts5_locale_blob(locale: &str, text: &str) -> Vec<u8> {
    let locale_bytes = locale.as_bytes();
    let text_bytes = text.as_bytes();
    let mut blob =
        Vec::with_capacity(FTS5_LOCALE_HEADER.len() + locale_bytes.len() + 1 + text_bytes.len());
    blob.extend_from_slice(&FTS5_LOCALE_HEADER);
    blob.extend_from_slice(locale_bytes);
    blob.push(0);
    blob.extend_from_slice(text_bytes);
    blob
}

fn decode_fts5_locale_blob(blob: &[u8]) -> Option<(&str, &str)> {
    let body = blob.strip_prefix(&FTS5_LOCALE_HEADER)?;
    let nul_pos = body.iter().position(|byte| *byte == 0)?;
    let tag = std::str::from_utf8(body.get(..nul_pos)?).ok()?;
    if tag.is_empty() {
        return None;
    }
    let text_start = nul_pos.checked_add(1)?;
    let text = std::str::from_utf8(body.get(text_start..)?).ok()?;
    Some((tag, text))
}

/// fts5_locale(locale, text) returns text or a SQLite-compatible locale blob.
pub struct Fts5LocaleFunc;

impl ScalarFunction for Fts5LocaleFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let [locale_value, text_value] = args else {
            return Err(FrankenError::function_error(
                "fts5_locale() expects 2 arguments",
            ));
        };

        let locale = sqlite_text_for_fts5_locale(locale_value);
        let text = sqlite_text_for_fts5_locale(text_value);
        let Some(locale) = locale.as_deref().filter(|value| !value.is_empty()) else {
            return Ok(text.map_or(SqliteValue::Null, |value| {
                SqliteValue::Text(SmallText::from_string(value.into_owned()))
            }));
        };

        Ok(SqliteValue::Blob(
            encode_fts5_locale_blob(locale, text.as_deref().unwrap_or("")).into(),
        ))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &'static str {
        "fts5_locale"
    }
}

/// Register FTS5 scalar functions into a `FunctionRegistry`.
pub fn register_fts5_scalars(registry: &mut fsqlite_func::FunctionRegistry) {
    registry.register_scalar(Fts5HighlightFunc);
    registry.register_scalar(Fts5SnippetFunc);
    registry.register_scalar(Fts5SourceIdFunc);
    registry.register_scalar(Fts5InsttokenFunc);
    registry.register_scalar(Fts5LocaleFunc);
    debug!("fts5: registered scalar functions");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ColumnStructure {
        name: String,
        indexed: bool,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5PostingStructure {
        docid: i64,
        column: u32,
        positions: Vec<u32>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TermStructure {
        term: String,
        postings: Vec<Fts5PostingStructure>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TableStructure {
        columns: Vec<Fts5ColumnStructure>,
        rows: Vec<(i64, Vec<String>)>,
        terms: Vec<Fts5TermStructure>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ExtensionApiStructure {
        tokenizers: Vec<String>,
        auxiliaries: Vec<String>,
        document_tokens: Vec<Fts5Token>,
        query_tokens: Vec<Fts5Token>,
        prefix_tokens: Vec<Fts5Token>,
        aux_value: SqliteValue,
        missing_aux_error: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ColumnFilterSetStructure {
        tokens: Vec<(Fts5QueryTokenKind, String)>,
        braced_matches: Vec<i64>,
        negative_matches: Vec<i64>,
        complement_matches: Vec<i64>,
        invalid_error: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5NearPhraseStructure {
        operands: Vec<Fts5NearOperand>,
        distance: u32,
        phrase_matches: Vec<i64>,
        prefix_matches: Vec<i64>,
        query_terms: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5NearDistanceStructure {
        adjacent_terms: Vec<i64>,
        one_gap_terms: Vec<i64>,
        adjacent_phrases: Vec<i64>,
        one_gap_phrases: Vec<i64>,
        reordered_doc_example: Vec<i64>,
        multi_phrase_doc_example: Vec<i64>,
        too_tight_doc_example: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5PhraseConcatStructure {
        tokens: Vec<(Fts5QueryTokenKind, String)>,
        exact_expr: Fts5Expr,
        prefix_expr: Fts5Expr,
        exact_matches: Vec<i64>,
        prefix_matches: Vec<i64>,
        malformed_error: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TightPhraseConcatStructure {
        tokens: Vec<(Fts5QueryTokenKind, String)>,
        expr: Fts5Expr,
        adjacent_matches: Vec<i64>,
        separated_matches: Vec<i64>,
        column_filtered_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5InsttokenStructure {
        config: Fts5Config,
        columns: Vec<String>,
        rows: Vec<(i64, Vec<String>)>,
        terms: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5LocaleStructure {
        config: Fts5Config,
        columns: Vec<String>,
        rows: Vec<(i64, Vec<String>)>,
        terms: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5LocaleBlobStorageStructure {
        config: Fts5Config,
        indexed_columns: Vec<bool>,
        rows: Vec<(i64, Vec<String>)>,
        locales: Vec<(i64, usize, String)>,
        matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5LocaleUnindexedDiscardStructure {
        indexed_columns: Vec<bool>,
        rows: Vec<(i64, Vec<String>)>,
        locales: Vec<(i64, usize, String)>,
        indexed_matches: Vec<i64>,
        unindexed_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TokendataStructure {
        config: Fts5Config,
        terms: Vec<String>,
        rows: Vec<(i64, Vec<String>)>,
        matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ConfigMetadataStructure {
        rows: Vec<(String, String)>,
        decoded: Fts5ConfigMetadata,
        runtime: Fts5Config,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5DataRowsStructure {
        data_blocks: Vec<(i64, Vec<u8>)>,
        averages: Fts5AveragesRecord,
        structure: Fts5StructureRecord,
        prefix_lengths: Vec<usize>,
        prefix_terms_len2: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5SegmentCodecStructure {
        idx_row: Fts5IdxRow,
        idx_pgno: i64,
        data_rowids: Vec<(i64, Fts5DataRowid)>,
        leaf: Fts5SegmentLeaf,
        leaf_bytes: Vec<u8>,
        dlidx: Fts5DlidxPage,
        tombstone: Fts5TombstonePage,
        tombstone_hits: Vec<(u64, bool)>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5PendingFlushStructure {
        term_count: usize,
        pending_bytes: usize,
        should_flush: bool,
        data_rowids: Vec<(i64, Fts5DataRowid)>,
        terms: Vec<Vec<u8>>,
        structure: Fts5StructureRecord,
        merge_plan: Option<Fts5MergePlan>,
        optimize_plan: Option<Fts5MergePlan>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ShadowWriteHotspotProfileStructure {
        flush_profile: Fts5ShadowWriteHotspotProfile,
        idx_profile: Fts5ShadowWriteHotspotProfile,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5SegmentReaderStructure {
        cursor_terms: Vec<Vec<u8>>,
        exact_brown_rowids: Vec<u64>,
        prefix_ru_terms: Vec<Vec<u8>>,
        union_brown_fox: Vec<u64>,
        phrase_brown_fox: Vec<u64>,
        near_brown_fox: Vec<u64>,
        integrity: Fts5SegmentIntegrityReport,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ShadowRowsStructure {
        data: Vec<(i64, Vec<u8>)>,
        idx: Vec<Fts5IdxRow>,
        config: Vec<(String, String)>,
        content: Vec<Fts5ContentRow>,
        docsize: Vec<Fts5DocsizeRow>,
        matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ShadowOpenStructure {
        report: Fts5ShadowOpenReport,
        table_empty: bool,
        next_rowid: i64,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ShadowQueryStructure {
        exact_matches: Vec<i64>,
        prefix_matches: Vec<i64>,
        phrase_matches: Vec<i64>,
        near_matches: Vec<i64>,
        initial_matches: Vec<i64>,
        column_filtered_matches: Vec<i64>,
        boolean_or_matches: Vec<i64>,
        boolean_not_matches: Vec<i64>,
        cursor_body_value: Option<SqliteValue>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5HighlightPrefixStructure {
        parsed_terms: Vec<Fts5HighlightTerm>,
        prefix_highlight: String,
        phrase_prefix_snippet: String,
        fallback_prefix_highlight: String,
        exact_highlight: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5PhraseSpanStructure {
        phrase_patterns: Vec<Fts5HighlightPattern>,
        rendered_phrase: String,
        rendered_prefix_snippet: String,
        negated_rhs_rendering: String,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5SnippetWindowStructure {
        scored_window: Fts5SnippetWindow,
        rendered_snippet: String,
        no_match_window: Fts5SnippetWindow,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TransactionRollbackStructure {
        savepoint_rows: Vec<(i64, Vec<String>)>,
        savepoint_matches: Vec<i64>,
        full_rows: Vec<(i64, Vec<String>)>,
        full_matches: Vec<i64>,
        full_locales: Vec<(i64, usize, String)>,
        reused_auto_rowid: Option<i64>,
        rows_after_auto: Vec<(i64, Vec<String>)>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5UpdateFailureCleanupStructure {
        error: String,
        rows_after_error: Vec<(i64, Vec<String>)>,
        old_matches: Vec<i64>,
        replacement_matches: Vec<i64>,
        locales_after_error: Vec<(i64, usize, String)>,
        committed_rows: Vec<(i64, Vec<String>)>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ContentlessUpdateStructure {
        reject_error: String,
        rows_after_reject: Vec<(i64, Vec<String>)>,
        matches_after_reject: Vec<i64>,
        updated_rows: Vec<(i64, Vec<String>)>,
        updated_old_matches: Vec<i64>,
        updated_new_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5PostingPruneStructure {
        terms_before: Vec<String>,
        terms_after: Vec<String>,
        prefix_terms_before: Vec<String>,
        prefix_terms_after: Vec<String>,
        remaining_docs_for_alpha: Vec<i64>,
        remaining_docs_for_world: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5NotTermStructure {
        query_terms: Vec<String>,
        highlight_terms: Vec<Fts5HighlightTerm>,
        search_matches: Vec<i64>,
        positive_highlight: String,
        fallback_highlight_terms: Vec<Fts5HighlightTerm>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TrigramCaseSensitiveStructure {
        tokenizer: String,
        terms: Vec<String>,
        rows: Vec<(i64, Vec<String>)>,
        upper_matches: Vec<i64>,
        lower_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TrigramStreamingStructure {
        tokens: Vec<(String, usize, usize)>,
        short_input_tokens: usize,
        accented_matches: Vec<i64>,
        ascii_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5AsciiTokenizerStructure {
        tokens: Vec<(String, usize, usize)>,
        terms: Vec<String>,
        upper_matches: Vec<i64>,
        lower_matches: Vec<i64>,
        numeric_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5TrigramCaseFoldStructure {
        insensitive_tokens: Vec<(String, usize, usize)>,
        sensitive_terms: Vec<String>,
        diacritic_terms: Vec<String>,
        upper_matches: Vec<i64>,
        lower_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5PorterYVowelStructure {
        stems: Vec<(String, String)>,
        vowel_checks: Vec<(String, bool)>,
        measures: Vec<(String, u32)>,
        cry_matches: Vec<i64>,
        fly_matches: Vec<i64>,
        sky_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5ContentlessUnindexedStructure {
        config: Fts5Config,
        indexed_columns: Vec<bool>,
        rows: Vec<(i64, Vec<String>)>,
        indexed_matches: Vec<i64>,
        unindexed_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5Unicode61DiacriticsStructure {
        tokenizer: String,
        terms: Vec<String>,
        rows: Vec<(i64, Vec<String>)>,
        ascii_matches: Vec<i64>,
        accent_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5Unicode61OptionValidationStructure {
        accepted_terms: Vec<String>,
        rejected_specs: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5Unicode61CharClassStructure {
        tokens: Vec<(String, usize, usize)>,
        terms: Vec<String>,
        section_matches: Vec<i64>,
        alpha_matches: Vec<i64>,
        cafe_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5QuotedOptionStructure {
        unquoted: Vec<(String, String)>,
        tokenizer: String,
        prefix_lengths: Vec<usize>,
        terms: Vec<String>,
        cafe_matches: Vec<i64>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct Fts5DetailOptionStructure {
        parsed: Vec<(String, Option<DetailMode>)>,
        config_detail: DetailMode,
        index_detail: DetailMode,
        matches: Vec<i64>,
        phrase_error: String,
    }

    struct TokendataTestTokenizer;

    impl Fts5Tokenizer for TokendataTestTokenizer {
        fn name(&self) -> &'static str {
            "tokendata_test"
        }

        fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
            let mut token_with_data = String::new();
            for token in text.split_whitespace() {
                token_with_data.clear();
                token_with_data.push_str(token);
                token_with_data.push('\0');
                token_with_data.push_str("payload");
                sink(token_with_data.as_str(), 0, token.len(), false);
            }
        }
    }

    #[derive(Debug)]
    struct SynonymTestTokenizer {
        suffix: String,
    }

    impl Fts5Tokenizer for SynonymTestTokenizer {
        fn name(&self) -> &'static str {
            "synonym_test"
        }

        fn visit_tokens(&self, text: &str, sink: &mut dyn FnMut(&str, usize, usize, bool)) {
            let mut synonym = String::new();
            for token in text.split_whitespace() {
                let start = text.find(token).unwrap_or(0);
                let end = start + token.len();
                let normalized = token.to_ascii_lowercase();
                sink(normalized.as_str(), start, end, false);
                synonym.clear();
                synonym.push_str(&normalized);
                synonym.push_str(&self.suffix);
                sink(synonym.as_str(), start, end, true);
            }
        }
    }

    fn near_term(term: &str) -> Fts5NearOperand {
        Fts5NearOperand::Term(term.to_owned())
    }

    fn near_phrase(terms: &[&str]) -> Fts5NearOperand {
        Fts5NearOperand::Phrase(terms.iter().map(ToString::to_string).collect())
    }

    fn near_prefix(prefix: &str) -> Fts5NearOperand {
        Fts5NearOperand::Prefix(prefix.to_owned())
    }

    fn search_rowids(table: &Fts5Table, query: &str) -> std::result::Result<Vec<i64>, String> {
        Ok(table
            .search(query)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect())
    }

    fn table_structure(table: &Fts5Table) -> Fts5TableStructure {
        let columns = table
            .columns
            .iter()
            .zip(&table.indexed_columns)
            .map(|(name, indexed)| Fts5ColumnStructure {
                name: name.clone(),
                indexed: *indexed,
            })
            .collect();
        let mut terms: Vec<Fts5TermStructure> = table
            .index
            .index
            .iter()
            .map(|(term, postings)| Fts5TermStructure {
                term: term.as_str().to_owned(),
                postings: postings
                    .iter()
                    .map(|posting| Fts5PostingStructure {
                        docid: posting.docid,
                        column: posting.column,
                        positions: posting.positions.iter().copied().collect(),
                    })
                    .collect(),
            })
            .collect();
        terms.sort_by(|left, right| left.term.cmp(&right.term));

        Fts5TableStructure {
            columns,
            rows: table.all_rows(),
            terms,
        }
    }

    fn sorted_small_text_terms<'a>(terms: impl Iterator<Item = &'a SmallText>) -> Vec<String> {
        let mut terms: Vec<String> = terms.map(|term| term.as_str().to_owned()).collect();
        terms.sort();
        terms
    }

    fn indexed_terms(index: &InvertedIndex) -> Vec<String> {
        sorted_small_text_terms(index.index.keys())
    }

    fn indexed_prefix_terms(index: &InvertedIndex, prefix_length: usize) -> Vec<String> {
        index
            .prefix_indexes
            .get(&prefix_length)
            .map_or_else(Vec::new, |prefix_index| {
                sorted_small_text_terms(prefix_index.keys())
            })
    }

    fn posting_docids(postings: &[Posting]) -> Vec<i64> {
        let mut docids: Vec<i64> = postings.iter().map(|posting| posting.docid).collect();
        docids.sort_unstable();
        docids.dedup();
        docids
    }

    #[test]
    fn test_extension_name_matches_crate_suffix() {
        let expected = env!("CARGO_PKG_NAME")
            .strip_prefix("fsqlite-ext-")
            .expect("extension crates should use fsqlite-ext-* naming");
        assert_eq!(extension_name(), expected);
    }

    // -- Config tests (preserved from original) --

    #[test]
    fn test_secure_delete_enable_command() {
        let mut config = Fts5Config::default();
        assert!(config.apply_control_command("secure-delete=1"));
        assert!(config.secure_delete_enabled());
        assert_eq!(config.delete_action(), DeleteAction::PhysicalPurge);
    }

    #[test]
    fn test_secure_delete_disable_command() {
        let mut config = Fts5Config::default();
        assert!(config.apply_control_command("secure_delete=true"));
        assert!(config.secure_delete_enabled());
        assert!(config.apply_control_command("secure-delete=0"));
        assert!(!config.secure_delete_enabled());
        assert_eq!(config.delete_action(), DeleteAction::Tombstone);
    }

    #[test]
    fn test_invalid_control_command_is_ignored() {
        let mut config = Fts5Config::default();
        assert!(!config.apply_control_command("secure-delete=maybe"));
        assert!(!config.apply_control_command("integrity-check=1"));
        assert_eq!(config.delete_action(), DeleteAction::Tombstone);
    }

    #[test]
    fn test_fts5_config_rows_default_stock_shape() {
        let rows = Fts5Config::default().encode_config_rows();
        assert_eq!(rows, vec![Fts5ConfigRecord::integer("version", 4)]);
    }

    #[test]
    fn test_fts5_config_rows_persist_runtime_toggles() {
        let mut config = Fts5Config::default();
        assert!(config.apply_control_command("secure-delete=1"));
        assert!(config.apply_control_command("insttoken=1"));

        assert_eq!(
            config.encode_config_rows(),
            vec![
                Fts5ConfigRecord::integer("insttoken", 1),
                Fts5ConfigRecord::integer("secure-delete", 1),
                Fts5ConfigRecord::integer("version", 4),
            ]
        );
    }

    #[test]
    fn test_fts5_config_metadata_decodes_stock_rows() {
        let rows = vec![
            Fts5ConfigRecord::integer("version", 5),
            Fts5ConfigRecord::integer("pgsz", 64),
            Fts5ConfigRecord::integer("hashsize", 2048),
            Fts5ConfigRecord::integer("automerge", 8),
            Fts5ConfigRecord::integer("usermerge", 16),
            Fts5ConfigRecord::integer("crisismerge", 5000),
            Fts5ConfigRecord::integer("deletemerge", 101),
            Fts5ConfigRecord::text("rank", "bm25(10.0)"),
            Fts5ConfigRecord::integer("secure-delete", 2),
            Fts5ConfigRecord::integer("insttoken", 1),
            Fts5ConfigRecord::integer("unknown", 99),
        ];

        let metadata = Fts5ConfigMetadata::decode_rows(&rows).unwrap();
        assert_eq!(metadata.format_version, 5);
        assert_eq!(metadata.page_size, 64);
        assert_eq!(metadata.hash_size, 2048);
        assert_eq!(metadata.automerge, 8);
        assert_eq!(metadata.usermerge, 16);
        assert_eq!(metadata.crisismerge, 1999);
        assert_eq!(metadata.delete_merge, 0);
        assert_eq!(metadata.rank.as_deref(), Some("bm25(10.0)"));
        assert!(metadata.secure_delete);
        assert!(metadata.insttoken);
    }

    #[test]
    fn test_fts5_config_metadata_rejects_missing_or_unknown_version() {
        let missing = Fts5ConfigMetadata::decode_rows(&[Fts5ConfigRecord::integer("pgsz", 64)])
            .expect_err("version row is required");
        assert!(missing.to_string().contains("missing version"));

        let unknown = Fts5ConfigMetadata::decode_rows(&[Fts5ConfigRecord::integer("version", 6)])
            .expect_err("unknown version should fail");
        assert!(
            unknown
                .to_string()
                .contains("invalid fts5 file format (found 6")
        );
    }

    #[test]
    fn test_fts5_table_config_row_round_trip() {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        let metadata = table
            .apply_config_rows(&[
                Fts5ConfigRecord::integer("secure-delete", 1),
                Fts5ConfigRecord::integer("insttoken", 1),
                Fts5ConfigRecord::integer("version", 5),
            ])
            .unwrap();

        assert_eq!(metadata.format_version, 5);
        assert!(table.config().secure_delete_enabled());
        assert!(table.config().insttoken_enabled());
        assert_eq!(
            table.encode_config_rows(),
            vec![
                Fts5ConfigRecord::integer("insttoken", 1),
                Fts5ConfigRecord::integer("secure-delete", 1),
                Fts5ConfigRecord::integer("version", 4),
            ]
        );
    }

    /// The pre-cass#301 reference: one full posting rescan per document via
    /// [`InvertedIndex::docsize_row`] (O(documents × postings)). The
    /// single-pass [`Fts5Table::encode_docsize_rows`] must produce identical
    /// rows.
    fn reference_docsize_rows(table: &Fts5Table) -> Vec<Fts5DocsizeRow> {
        let mut docids: Vec<i64> = table
            .documents
            .keys()
            .copied()
            .chain(table.index.doc_ids.iter().copied())
            .collect();
        docids.sort_unstable();
        docids.dedup();
        docids
            .into_iter()
            .map(|id| table.index.docsize_row(id, table.columns.len()))
            .collect()
    }

    fn fill_docsize_corpus(table: &mut Fts5Table, docs: i64) {
        const VOCAB: &[&str] = &[
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
        ];
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for rowid in 1..=docs {
            // Title: 0-3 tokens — sometimes empty, exercising a zero-count column.
            let title_len = (next() % 4) as usize;
            let title = (0..title_len)
                .map(|_| VOCAB[(next() as usize) % VOCAB.len()])
                .collect::<Vec<_>>()
                .join(" ");
            // Body: 1-12 tokens with repeats so positions accumulate per term.
            let body_len = 1 + (next() % 12) as usize;
            let body = (0..body_len)
                .map(|_| VOCAB[(next() as usize) % VOCAB.len()])
                .collect::<Vec<_>>()
                .join(" ");
            table.insert_document(rowid, &[title, body]);
        }
    }

    #[test]
    fn encode_docsize_rows_matches_per_document_reference() {
        let cx = Cx::new();
        let mut table =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        assert!(
            table.config().columnsize_enabled(),
            "columnsize must default on for this equivalence test"
        );

        fill_docsize_corpus(&mut table, 400);

        let expected = reference_docsize_rows(&table);
        let actual = table.encode_docsize_rows();

        assert_eq!(
            actual, expected,
            "single-pass encode_docsize_rows must equal the per-document reference"
        );
        assert_eq!(
            actual.len(),
            400,
            "every document must emit exactly one row"
        );
        for row in &actual {
            assert_eq!(
                row.column_token_counts.len(),
                2,
                "each docsize row carries one token count per column"
            );
        }
    }

    /// Scaling guard for cass#301: the previous per-document rescan was
    /// O(documents × postings), so a full FTS rebuild wedged a CPU core above
    /// ~15-30 MB of content. The single-pass encoder is O(postings); 20k docs
    /// must encode near-instantly. Ignored by default (build a larger corpus);
    /// run with `cargo test ... encode_docsize_rows_scales_linearly -- --ignored`.
    #[test]
    #[ignore = "perf scaling guard for cass#301; run explicitly with --ignored"]
    fn encode_docsize_rows_scales_linearly() {
        use std::time::Instant;
        let cx = Cx::new();
        let mut table =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        fill_docsize_corpus(&mut table, 20_000);

        let start = Instant::now();
        let rows = table.encode_docsize_rows();
        let elapsed = start.elapsed();

        assert_eq!(rows.len(), 20_000);
        assert_eq!(rows, reference_docsize_rows(&table));
        // The O(N²) path took many seconds/minutes at this size; O(N) is well
        // under a second. A generous 5 s bound still catches a regression.
        assert!(
            elapsed.as_secs() < 5,
            "encode_docsize_rows for 20k docs took {elapsed:?}; expected O(postings), not O(docs × postings)"
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_config_metadata_codec() {
        let rows = vec![
            Fts5ConfigRecord::integer("automerge", 1),
            Fts5ConfigRecord::integer("crisismerge", 0),
            Fts5ConfigRecord::integer("deletemerge", -1),
            Fts5ConfigRecord::integer("hashsize", 4096),
            Fts5ConfigRecord::integer("insttoken", 1),
            Fts5ConfigRecord::integer("pgsz", 128),
            Fts5ConfigRecord::text("rank", "bm25(3.0, 1.0)"),
            Fts5ConfigRecord::integer("secure-delete", 1),
            Fts5ConfigRecord::integer("usermerge", 8),
            Fts5ConfigRecord::integer("version", 5),
        ];
        let decoded = Fts5ConfigMetadata::decode_rows(&rows).unwrap();
        let mut runtime = Fts5Config::default();
        decoded.apply_to_runtime_config(&mut runtime);
        let snapshot = Fts5ConfigMetadataStructure {
            rows: decoded
                .encode_rows()
                .into_iter()
                .map(|record| (record.key, record.value.to_text()))
                .collect(),
            decoded,
            runtime,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5ConfigMetadataStructure {
    rows: [
        (
            "hashsize",
            "4096",
        ),
        (
            "insttoken",
            "1",
        ),
        (
            "pgsz",
            "128",
        ),
        (
            "rank",
            "bm25(3.0, 1.0)",
        ),
        (
            "secure-delete",
            "1",
        ),
        (
            "usermerge",
            "8",
        ),
        (
            "version",
            "5",
        ),
    ],
    decoded: Fts5ConfigMetadata {
        format_version: 5,
        page_size: 128,
        automerge: 4,
        usermerge: 8,
        crisismerge: 16,
        hash_size: 4096,
        delete_merge: 10,
        rank: Some(
            "bm25(3.0, 1.0)",
        ),
        secure_delete: true,
        insttoken: true,
    },
    runtime: Fts5Config {
        secure_delete: true,
        content_mode: Stored,
        contentless_delete: false,
        contentless_unindexed: false,
        columnsize: true,
        detail: Full,
        insttoken: true,
        locale: false,
        tokendata: false,
    },
}"#
        );
    }

    #[test]
    fn test_fts5_data_averages_record_round_trip() {
        let averages = Fts5AveragesRecord::new(2, vec![4, 5]);
        let block = averages.encode();
        assert_eq!(block, vec![2, 4, 5]);
        assert_eq!(Fts5AveragesRecord::decode(&block, 2).unwrap(), averages);

        let empty = Fts5AveragesRecord::new(0, vec![0, 0]);
        assert!(empty.encode().is_empty());
        assert_eq!(Fts5AveragesRecord::decode(&[], 2).unwrap(), empty);
    }

    #[test]
    fn test_fts5_data_structure_record_round_trip_legacy_and_v2() {
        let legacy = Fts5StructureRecord {
            cookie: 0x0102_0304,
            write_counter: 300,
            origin_counter: 0,
            levels: vec![
                Fts5StructureLevel::new(0, vec![Fts5StructureSegment::new(7, 1, 3)]),
                Fts5StructureLevel::new(0, Vec::new()),
            ],
        };
        let legacy_block = legacy.encode();
        assert_eq!(
            legacy_block,
            vec![1, 2, 3, 4, 2, 1, 0x82, 0x2C, 0, 1, 7, 1, 3, 0, 0]
        );
        assert_eq!(Fts5StructureRecord::decode(&legacy_block).unwrap(), legacy);

        let v2 = Fts5StructureRecord {
            cookie: 9,
            write_counter: 512,
            origin_counter: 43,
            levels: vec![Fts5StructureLevel::new(
                0,
                vec![Fts5StructureSegment::new(11, 1, 8).with_origin_tracking(40, 42, 2, 3, 99)],
            )],
        };
        let v2_block = v2.encode();
        assert_eq!(&v2_block[4..8], &FTS5_STRUCTURE_V2_MARKER);
        assert_eq!(Fts5StructureRecord::decode(&v2_block).unwrap(), v2);
    }

    #[test]
    fn test_fts5_data_records_reject_malformed_input() {
        let truncated_average = Fts5AveragesRecord::decode(&[1, 2, 0x80], 2)
            .expect_err("truncated averages varint should fail");
        assert!(truncated_average.to_string().contains("truncated"));

        let trailing_average = Fts5AveragesRecord::decode(&[1, 2, 3], 1)
            .expect_err("extra averages varint should fail");
        assert!(trailing_average.to_string().contains("trailing"));

        let truncated_structure = Fts5StructureRecord::decode(&[0, 0, 0, 0, 0x80])
            .expect_err("truncated structure varint should fail");
        assert!(truncated_structure.to_string().contains("truncated"));

        let bad_segment_count = [0, 0, 0, 0, 1, 0, 0, 0, 1, 7, 1, 3];
        let count_error = Fts5StructureRecord::decode(&bad_segment_count)
            .expect_err("segment total underflow should fail");
        assert!(count_error.to_string().contains("segment count"));
    }

    #[test]
    fn test_fts5_structural_snapshot_data_rows_codec() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "prefix='2 3'"],
        )
        .unwrap();
        table.insert_document(
            11,
            &["rust index".to_owned(), "shadow table rows".to_owned()],
        );

        let data = table.encode_data_rows();
        let metadata = table.decode_data_rows(&data).unwrap();
        let snapshot = Fts5DataRowsStructure {
            data_blocks: data.iter().map(|row| (row.id, row.block.clone())).collect(),
            averages: metadata.averages.unwrap(),
            structure: metadata.structure.unwrap(),
            prefix_lengths: table.prefix_lengths.clone(),
            prefix_terms_len2: indexed_prefix_terms(&table.index, 2),
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5DataRowsStructure {
    data_blocks: [
        (
            1,
            [
                1,
                2,
                3,
            ],
        ),
        (
            137438953473,
            [
                0,
                11,
                0,
                145,
                6,
                48,
                105,
                110,
                100,
                101,
                120,
                11,
                2,
                3,
                1,
                4,
                114,
                111,
                119,
                115,
                11,
                6,
                1,
                1,
                4,
                2,
                3,
                117,
                115,
                116,
                11,
                2,
                2,
                1,
                6,
                115,
                104,
                97,
                100,
                111,
                119,
                11,
                6,
                1,
                1,
                2,
                1,
                5,
                116,
                97,
                98,
                108,
                101,
                11,
                6,
                1,
                1,
                3,
                0,
                3,
                49,
                105,
                110,
                11,
                2,
                3,
                1,
                2,
                114,
                111,
                11,
                6,
                1,
                1,
                4,
                2,
                1,
                117,
                11,
                2,
                2,
                1,
                2,
                115,
                104,
                11,
                6,
                1,
                1,
                2,
                1,
                2,
                116,
                97,
                11,
                6,
                1,
                1,
                3,
                0,
                4,
                50,
                105,
                110,
                100,
                11,
                2,
                3,
                1,
                3,
                114,
                111,
                119,
                11,
                6,
                1,
                1,
                4,
                2,
                2,
                117,
                115,
                11,
                2,
                2,
                1,
                3,
                115,
                104,
                97,
                11,
                6,
                1,
                1,
                2,
                1,
                3,
                116,
                97,
                98,
                11,
                6,
                1,
                1,
                3,
                4,
                10,
                11,
                8,
                13,
                12,
                8,
                9,
                6,
                9,
                9,
                9,
                10,
                7,
                10,
            ],
        ),
        (
            10,
            [
                0,
                0,
                0,
                0,
                1,
                1,
                1,
                0,
                1,
                1,
                1,
                1,
            ],
        ),
    ],
    averages: Fts5AveragesRecord {
        total_rows: 1,
        column_token_totals: [
            2,
            3,
        ],
    },
    structure: Fts5StructureRecord {
        cookie: 0,
        write_counter: 1,
        origin_counter: 0,
        levels: [
            Fts5StructureLevel {
                merge_inputs: 0,
                segments: [
                    Fts5StructureSegment {
                        segid: 1,
                        pgno_first: 1,
                        pgno_last: 1,
                        origin_lower: 0,
                        origin_upper: 0,
                        tombstone_page_count: 0,
                        tombstone_entry_count: 0,
                        entry_count: 0,
                    },
                ],
            },
        ],
    },
    prefix_lengths: [
        2,
        3,
    ],
    prefix_terms_len2: [
        "in",
        "ro",
        "ru",
        "sh",
        "ta",
    ],
}"#
        );
    }

    fn sample_segment_leaf() -> Fts5SegmentLeaf {
        let mut leaf = Fts5SegmentLeaf::new(vec![
            Fts5SegmentTerm::new(
                b"rust".to_vec(),
                Fts5Doclist::new(vec![
                    Fts5DoclistEntry::new(
                        7,
                        Fts5Poslist::new(
                            false,
                            vec![
                                Fts5ColumnPositions::new(0, vec![1, 4]),
                                Fts5ColumnPositions::new(2, vec![9]),
                            ],
                        ),
                    ),
                    Fts5DoclistEntry::new(
                        12,
                        Fts5Poslist::new(true, vec![Fts5ColumnPositions::new(0, vec![2])]),
                    ),
                ]),
            ),
            Fts5SegmentTerm::new(
                b"rusty".to_vec(),
                Fts5Doclist::new(vec![Fts5DoclistEntry::new(
                    20,
                    Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(1, vec![0])]),
                )]),
            ),
        ]);
        leaf.first_rowid_offset = 9;
        leaf
    }

    fn sample_lazy_segment_rows() -> (Fts5StructureRecord, Vec<Fts5DataRow>, Vec<Fts5IdxRow>) {
        let first_leaf = Fts5SegmentLeaf::new(vec![
            Fts5SegmentTerm::new(
                b"brown".to_vec(),
                Fts5Doclist::new(vec![
                    Fts5DoclistEntry::new(
                        1,
                        Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(0, vec![1])]),
                    ),
                    Fts5DoclistEntry::new(
                        4,
                        Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(0, vec![3])]),
                    ),
                ]),
            ),
            Fts5SegmentTerm::new(
                b"fox".to_vec(),
                Fts5Doclist::new(vec![
                    Fts5DoclistEntry::new(
                        1,
                        Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(0, vec![2])]),
                    ),
                    Fts5DoclistEntry::new(
                        3,
                        Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(0, vec![0])]),
                    ),
                ]),
            ),
        ]);
        let second_leaf = Fts5SegmentLeaf::new(vec![
            Fts5SegmentTerm::new(
                b"rust".to_vec(),
                Fts5Doclist::new(vec![Fts5DoclistEntry::new(
                    7,
                    Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(0, vec![1, 4])]),
                )]),
            ),
            Fts5SegmentTerm::new(
                b"rusty".to_vec(),
                Fts5Doclist::new(vec![Fts5DoclistEntry::new(
                    20,
                    Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(1, vec![0])]),
                )]),
            ),
        ]);
        let structure = Fts5StructureRecord {
            cookie: 0,
            write_counter: 2,
            origin_counter: 0,
            levels: vec![Fts5StructureLevel::new(
                0,
                vec![Fts5StructureSegment::new(5, 1, 2)],
            )],
        };
        let data_rows = vec![
            first_leaf.to_data_row(5, 1).unwrap(),
            second_leaf.to_data_row(5, 2).unwrap(),
        ];
        let idx_rows = vec![Fts5IdxRow::new(5, b"rust".to_vec(), 2, false)];
        (structure, data_rows, idx_rows)
    }

    fn sample_shadow_query_rows(base: &Fts5Table) -> Fts5ShadowRows {
        let (structure, mut data_rows, idx_rows) = sample_lazy_segment_rows();
        let docsize = vec![
            Fts5DocsizeRow::new(1, vec![2, 1]),
            Fts5DocsizeRow::new(3, vec![1, 1]),
            Fts5DocsizeRow::new(4, vec![4, 1]),
            Fts5DocsizeRow::new(7, vec![3, 1]),
            Fts5DocsizeRow::new(20, vec![1, 1]),
        ];
        data_rows.insert(
            0,
            Fts5DataRow::new(
                FTS5_AVERAGES_ROWID,
                Fts5AveragesRecord::from_docsize_rows(5, base.columns().len(), &docsize).encode(),
            ),
        );
        data_rows.insert(
            1,
            Fts5DataRow::new(FTS5_STRUCTURE_ROWID, structure.encode()),
        );
        Fts5ShadowRows {
            data: data_rows,
            idx: idx_rows,
            config: base.encode_config_rows(),
            content: vec![
                Fts5ContentRow::new(1, vec!["quick brown".to_owned(), "fox".to_owned()]),
                Fts5ContentRow::new(3, vec!["fox".to_owned(), "sly".to_owned()]),
                Fts5ContentRow::new(4, vec!["slow brown bear".to_owned(), "plain".to_owned()]),
                Fts5ContentRow::new(7, vec!["rust engine rust".to_owned(), "plain".to_owned()]),
                Fts5ContentRow::new(20, vec!["plain".to_owned(), "rusty".to_owned()]),
            ],
            docsize,
        }
    }

    fn add_prefixed_brown_segment(rows: &mut Fts5ShadowRows) -> Fts5StructureRecord {
        let structure = Fts5StructureRecord {
            cookie: 0,
            write_counter: 3,
            origin_counter: 0,
            levels: vec![Fts5StructureLevel::new(
                0,
                vec![
                    Fts5StructureSegment::new(5, 1, 2),
                    Fts5StructureSegment::new(6, 1, 1),
                ],
            )],
        };
        let structure_row = rows
            .data
            .iter_mut()
            .find(|row| row.id == FTS5_STRUCTURE_ROWID)
            .expect("sample rows include structure metadata");
        structure_row.block = structure.encode();

        let prefixed_brown = Fts5SegmentLeaf::new(vec![Fts5SegmentTerm::new(
            b"0brown".to_vec(),
            Fts5Doclist::new(vec![Fts5DoclistEntry::new(
                30,
                Fts5Poslist::new(false, vec![Fts5ColumnPositions::new(0, vec![0])]),
            )]),
        )]);
        rows.data.push(prefixed_brown.to_data_row(6, 1).unwrap());

        structure
    }

    struct SliceOnDiskReader {
        data: Vec<Fts5DataRow>,
        idx: Vec<Fts5IdxRow>,
    }

    impl Fts5OnDiskReader for SliceOnDiskReader {
        fn read_data_block(&mut self, id: i64) -> Result<Option<Vec<u8>>> {
            Ok(self
                .data
                .iter()
                .find(|row| row.id == id)
                .map(|row| row.block.clone()))
        }

        fn idx_candidate_page(&mut self, segid: u32, term: &[u8]) -> Result<Option<u32>> {
            Ok(self
                .idx
                .iter()
                .filter(|row| row.segid == segid && row.term.as_slice() <= term)
                .max_by(|left, right| left.term.cmp(&right.term))
                .map(|row| row.btree_page))
        }

        fn read_docsize(
            &mut self,
            _rowid: i64,
            _column_count: usize,
        ) -> Result<Option<Fts5DocsizeRow>> {
            Ok(None)
        }

        fn read_content(
            &mut self,
            _rowid: i64,
            _column_count: usize,
        ) -> Result<Option<Vec<String>>> {
            Ok(None)
        }
    }

    #[test]
    fn test_fts5_shadow_query_merges_raw_and_prefixed_segment_keys() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let mut rows = sample_shadow_query_rows(&base);
        add_prefixed_brown_segment(&mut rows);

        let tokenizer = create_tokenizer("unicode61").unwrap();
        let query =
            Fts5ShadowQuery::new(&rows, base.columns(), tokenizer.as_ref(), DetailMode::Full)
                .unwrap();
        let rowids = shadow_rowids_from_entries(&query.exact_entries("brown").unwrap(), None);
        assert_eq!(rowids, vec![1, 4, 30]);

        let prefix_rowids = shadow_rowids_from_entries(&query.prefix_entries("br").unwrap(), None);
        assert_eq!(prefix_rowids, vec![1, 4, 30]);
    }

    #[test]
    fn test_fts5_lazy_exact_doclist_merges_raw_and_prefixed_segment_keys() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let mut rows = sample_shadow_query_rows(&base);
        let structure = add_prefixed_brown_segment(&mut rows);
        let mut reader = SliceOnDiskReader {
            data: rows.data,
            idx: rows.idx,
        };

        let mut rowids: Vec<u64> = lazy_exact_doclist_entries(&mut reader, &structure, b"brown")
            .unwrap()
            .into_iter()
            .map(|entry| entry.rowid)
            .collect();
        rowids.sort_unstable();
        rowids.dedup();
        assert_eq!(rowids, vec![1, 4, 30]);
    }

    #[test]
    fn test_fts5_idx_and_data_rowid_codecs_round_trip() {
        let idx = Fts5IdxRow::new(7, b"rusty".to_vec(), 3, true);
        assert_eq!(idx.encoded_pgno().unwrap(), 7);
        assert_eq!(
            Fts5IdxRow::from_encoded_pgno(7, b"rusty".to_vec(), 7).unwrap(),
            idx
        );

        let rowids = [
            Fts5DataRowid::SegmentLeaf { segid: 7, pgno: 3 },
            Fts5DataRowid::DoclistIndex {
                segid: 7,
                height: 2,
                pgno: 3,
            },
            Fts5DataRowid::Tombstone {
                segid: 7,
                hash_pgno: 1,
            },
        ];
        for rowid in rowids {
            let encoded = rowid.encode().unwrap();
            assert_eq!(Fts5DataRowid::decode(encoded).unwrap(), rowid);
        }
    }

    #[test]
    fn test_fts5_segment_leaf_dlidx_and_tombstone_round_trip() {
        let leaf = sample_segment_leaf();
        let encoded_leaf = leaf.encode().unwrap();
        assert_eq!(
            encoded_leaf,
            vec![
                0, 9, 0, 27, 4, 114, 117, 115, 116, 7, 10, 3, 5, 1, 2, 11, 5, 3, 4, 4, 1, 121, 20,
                6, 1, 1, 2, 4, 15,
            ]
        );
        assert_eq!(Fts5SegmentLeaf::decode(&encoded_leaf).unwrap(), leaf);

        let dlidx = Fts5DlidxPage::new(0, 3, 7, vec![Some(5), None, Some(9)]);
        assert!(dlidx.is_root());
        assert_eq!(dlidx.encode(), vec![0, 3, 7, 5, 0, 9]);
        assert_eq!(Fts5DlidxPage::decode(&dlidx.encode()).unwrap(), dlidx);

        let tombstone = Fts5TombstonePage::new(4, true, vec![None, Some(9), None, Some(15)]);
        assert_eq!(
            tombstone.encode().unwrap(),
            vec![
                4, 1, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 15,
            ]
        );
        let decoded_tombstone = Fts5TombstonePage::decode(&tombstone.encode().unwrap()).unwrap();
        assert_eq!(decoded_tombstone, tombstone);
        assert!(decoded_tombstone.contains_rowid(1, 0));
        assert!(decoded_tombstone.contains_rowid(1, 9));
        assert!(decoded_tombstone.contains_rowid(1, 15));
        assert!(!decoded_tombstone.contains_rowid(1, 8));
    }

    #[test]
    fn test_fts5_segment_codecs_reject_malformed_payloads() {
        let unsorted_doclist = Fts5Doclist::new(vec![
            Fts5DoclistEntry::new(9, Fts5Poslist::new(false, Vec::new())),
            Fts5DoclistEntry::new(7, Fts5Poslist::new(false, Vec::new())),
        ]);
        assert!(
            unsorted_doclist
                .encode()
                .expect_err("doclist rowids must be ordered")
                .to_string()
                .contains("strictly increasing")
        );

        let mut bad_leaf = sample_segment_leaf().encode().unwrap();
        bad_leaf[2] = 0;
        bad_leaf[3] = 3;
        assert!(
            Fts5SegmentLeaf::decode(&bad_leaf)
                .expect_err("footer before header should fail")
                .to_string()
                .contains("footer offset")
        );

        let bad_tombstone_count = vec![4, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 5];
        assert!(
            Fts5TombstonePage::decode(&bad_tombstone_count)
                .expect_err("declared tombstone count must match slots")
                .to_string()
                .contains("entry count")
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_segment_codecs() {
        let idx_row = Fts5IdxRow::new(7, b"rusty".to_vec(), 3, true);
        let leaf = Fts5SegmentLeaf::decode(&sample_segment_leaf().encode().unwrap()).unwrap();
        let dlidx = Fts5DlidxPage::decode(
            &Fts5DlidxPage::new(0, 3, 7, vec![Some(5), None, Some(9)]).encode(),
        )
        .unwrap();
        let tombstone = Fts5TombstonePage::decode(
            &Fts5TombstonePage::new(4, true, vec![None, Some(9), None, Some(15)])
                .encode()
                .unwrap(),
        )
        .unwrap();
        let data_rowids = vec![
            Fts5DataRowid::SegmentLeaf { segid: 7, pgno: 3 },
            Fts5DataRowid::DoclistIndex {
                segid: 7,
                height: 2,
                pgno: 3,
            },
            Fts5DataRowid::Tombstone {
                segid: 7,
                hash_pgno: 1,
            },
        ]
        .into_iter()
        .map(|rowid| (rowid.encode().unwrap(), rowid))
        .collect();

        let snapshot = Fts5SegmentCodecStructure {
            idx_pgno: idx_row.encoded_pgno().unwrap(),
            idx_row,
            data_rowids,
            leaf_bytes: leaf.encode().unwrap(),
            leaf,
            dlidx,
            tombstone_hits: vec![
                (0, tombstone.contains_rowid(1, 0)),
                (8, tombstone.contains_rowid(1, 8)),
                (9, tombstone.contains_rowid(1, 9)),
                (15, tombstone.contains_rowid(1, 15)),
            ],
            tombstone,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r"Fts5SegmentCodecStructure {
    idx_row: Fts5IdxRow {
        segid: 7,
        term: [
            114,
            117,
            115,
            116,
            121,
        ],
        btree_page: 3,
        has_doclist_index: true,
    },
    idx_pgno: 7,
    data_rowids: [
        (
            962072674307,
            SegmentLeaf {
                segid: 7,
                pgno: 3,
            },
        ),
        (
            1035087118339,
            DoclistIndex {
                segid: 7,
                height: 2,
                pgno: 3,
            },
        ),
        (
            9008161327415297,
            Tombstone {
                segid: 7,
                hash_pgno: 1,
            },
        ),
    ],
    leaf: Fts5SegmentLeaf {
        first_rowid_offset: 9,
        terms: [
            Fts5SegmentTerm {
                term: [
                    114,
                    117,
                    115,
                    116,
                ],
                doclist: Fts5Doclist {
                    entries: [
                        Fts5DoclistEntry {
                            rowid: 7,
                            poslist: Fts5Poslist {
                                delete: false,
                                columns: [
                                    Fts5ColumnPositions {
                                        column: 0,
                                        offsets: [
                                            1,
                                            4,
                                        ],
                                    },
                                    Fts5ColumnPositions {
                                        column: 2,
                                        offsets: [
                                            9,
                                        ],
                                    },
                                ],
                            },
                        },
                        Fts5DoclistEntry {
                            rowid: 12,
                            poslist: Fts5Poslist {
                                delete: true,
                                columns: [
                                    Fts5ColumnPositions {
                                        column: 0,
                                        offsets: [
                                            2,
                                        ],
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
            Fts5SegmentTerm {
                term: [
                    114,
                    117,
                    115,
                    116,
                    121,
                ],
                doclist: Fts5Doclist {
                    entries: [
                        Fts5DoclistEntry {
                            rowid: 20,
                            poslist: Fts5Poslist {
                                delete: false,
                                columns: [
                                    Fts5ColumnPositions {
                                        column: 1,
                                        offsets: [
                                            0,
                                        ],
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
        ],
    },
    leaf_bytes: [
        0,
        9,
        0,
        27,
        4,
        114,
        117,
        115,
        116,
        7,
        10,
        3,
        5,
        1,
        2,
        11,
        5,
        3,
        4,
        4,
        1,
        121,
        20,
        6,
        1,
        1,
        2,
        4,
        15,
    ],
    dlidx: Fts5DlidxPage {
        flags: 0,
        leaf_pgno: 3,
        first_rowid: 7,
        rowid_deltas: [
            Some(
                5,
            ),
            None,
            Some(
                9,
            ),
        ],
    },
    tombstone: Fts5TombstonePage {
        key_size: 4,
        rowid_zero: true,
        slots: [
            None,
            Some(
                9,
            ),
            None,
            Some(
                15,
            ),
        ],
    },
    tombstone_hits: [
        (
            0,
            true,
        ),
        (
            8,
            false,
        ),
        (
            9,
            true,
        ),
        (
            15,
            true,
        ),
    ],
}"
        );
    }

    #[test]
    fn test_fts5_pending_hash_flush_creates_segment_and_structure_rows() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "prefix='2'"],
        )
        .unwrap();
        table.insert_document(7, &["rust fts".to_owned(), "rusty rust".to_owned()]);

        let pending = table.build_pending_hash().unwrap();
        assert_eq!(pending.row_count(), 1);
        assert_eq!(pending.term_count(), 5);

        let flush = pending
            .flush_to_segment(4, Fts5StructureRecord::empty_legacy(0))
            .unwrap();
        assert_eq!(flush.data_rows.len(), 2);
        assert_eq!(
            Fts5DataRowid::decode(flush.data_rows[0].id).unwrap(),
            Fts5DataRowid::SegmentLeaf { segid: 4, pgno: 1 }
        );
        assert_eq!(flush.data_rows[1].id, FTS5_STRUCTURE_ROWID);
        assert_eq!(
            Fts5SegmentLeaf::decode(&flush.data_rows[0].block).unwrap(),
            flush.leaf
        );
        assert_eq!(flush.structure.levels[0].segments[0].segid, 4);
        assert_eq!(flush.structure.write_counter, 1);
    }

    #[test]
    fn test_fts5_pending_flush_hotspot_profile_keeps_single_structure_commit() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"]).unwrap();
        table.insert_document(7, &["rust fts storage".to_owned()]);

        let flush = table
            .build_pending_hash()
            .unwrap()
            .flush_to_segment(4, Fts5StructureRecord::empty_legacy(0))
            .unwrap();
        let profile = flush.hotspot_profile().unwrap();

        assert_eq!(profile.data_row_writes, 2);
        assert_eq!(profile.segment_leaf_writes, 1);
        assert_eq!(profile.structure_record_writes, 1);
        assert_eq!(profile.idx_row_writes, 0);
        assert_eq!(profile.hot_write_count, 1);
        assert_eq!(profile.unique_segment_pages, vec![(4, 1)]);
        assert!(profile.append_only_rows_precede_hot_rows);
        assert_eq!(
            profile.publication_order,
            vec![
                Fts5ShadowWriteClass::SegmentLeaf { segid: 4, pgno: 1 },
                Fts5ShadowWriteClass::StructureRecord
            ]
        );
        assert_eq!(
            profile.mitigation,
            Fts5ShadowWriteMitigation::AppendOnlyRowsBeforeSingleMetadataCommit
        );
    }

    #[test]
    fn test_fts5_shadow_write_hotspot_profile_classifies_idx_page_writes() {
        let data_rows = vec![
            Fts5DataRow::new(
                Fts5DataRowid::SegmentLeaf { segid: 8, pgno: 1 }
                    .encode()
                    .unwrap(),
                Vec::new(),
            ),
            Fts5DataRow::new(
                Fts5DataRowid::DoclistIndex {
                    segid: 8,
                    height: 1,
                    pgno: 3,
                }
                .encode()
                .unwrap(),
                Vec::new(),
            ),
            Fts5DataRow::new(FTS5_STRUCTURE_ROWID, Vec::new()),
        ];
        let idx_rows = vec![
            Fts5IdxRow::new(8, b"alpha".to_vec(), 2, true),
            Fts5IdxRow::new(8, b"beta".to_vec(), 2, false),
        ];

        let profile = Fts5ShadowWriteHotspotProfile::from_rows(&data_rows, &idx_rows).unwrap();

        assert_eq!(profile.append_only_data_writes, 2);
        assert_eq!(profile.doclist_index_writes, 1);
        assert_eq!(profile.idx_row_writes, 2);
        assert_eq!(profile.hot_write_count, 3);
        assert_eq!(profile.unique_doclist_index_pages, vec![(8, 1, 3)]);
        assert_eq!(profile.unique_idx_pages, vec![(8, 2)]);
        assert!(profile.append_only_rows_precede_hot_rows);
        assert_eq!(
            profile.mitigation,
            Fts5ShadowWriteMitigation::AppendOnlyRowsBeforeBatchedHotWrites
        );
    }

    #[test]
    fn test_fts5_merge_scheduler_plans_auto_crisis_and_optimize() {
        let scheduler = Fts5MergeScheduler {
            automerge: 4,
            usermerge: 2,
            crisismerge: 6,
        };
        let structure = Fts5StructureRecord {
            cookie: 0,
            write_counter: 6,
            origin_counter: 0,
            levels: vec![Fts5StructureLevel::new(
                0,
                (1..=4)
                    .map(|segid| Fts5StructureSegment::new(segid, 1, 1))
                    .collect(),
            )],
        };

        assert_eq!(
            scheduler.next_plan(&structure),
            Some(Fts5MergePlan {
                kind: Fts5MergeKind::Auto,
                level: 0,
                segment_count: 4,
                merge_inputs: 2,
            })
        );
        assert_eq!(
            scheduler.optimize_plan(&structure),
            Some(Fts5MergePlan {
                kind: Fts5MergeKind::Optimize,
                level: 0,
                segment_count: 4,
                merge_inputs: 4,
            })
        );

        let crisis = Fts5StructureRecord {
            levels: vec![Fts5StructureLevel::new(
                0,
                (1..=6)
                    .map(|segid| Fts5StructureSegment::new(segid, 1, 1))
                    .collect(),
            )],
            ..structure
        };
        assert_eq!(
            scheduler.next_plan(&crisis).map(|plan| plan.kind),
            Some(Fts5MergeKind::Crisis)
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_pending_flush_and_merge_scheduler() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "prefix='2'"],
        )
        .unwrap();
        table.insert_document(7, &["rust fts".to_owned(), "rusty rust".to_owned()]);

        let pending = table.build_pending_hash().unwrap();
        let metadata = Fts5ConfigMetadata {
            hash_size: 64,
            ..Default::default()
        };
        let flush = pending
            .flush_to_segment(4, Fts5StructureRecord::empty_legacy(0))
            .unwrap();
        let scheduler = Fts5MergeScheduler {
            automerge: 4,
            usermerge: 2,
            crisismerge: 16,
        };
        let merge_structure = Fts5StructureRecord {
            cookie: 0,
            write_counter: 4,
            origin_counter: 0,
            levels: vec![Fts5StructureLevel::new(
                0,
                (1..=4)
                    .map(|segid| Fts5StructureSegment::new(segid, 1, 1))
                    .collect(),
            )],
        };
        let snapshot = Fts5PendingFlushStructure {
            term_count: pending.term_count(),
            pending_bytes: pending.pending_bytes(),
            should_flush: pending.should_flush(&metadata),
            data_rowids: flush
                .data_rows
                .iter()
                .map(|row| (row.id, Fts5DataRowid::decode(row.id).unwrap()))
                .collect(),
            terms: flush
                .leaf
                .terms
                .iter()
                .map(|term| term.term.clone())
                .collect(),
            structure: flush.structure,
            merge_plan: scheduler.next_plan(&merge_structure),
            optimize_plan: scheduler.optimize_plan(&merge_structure),
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r"Fts5PendingFlushStructure {
    term_count: 5,
    pending_bytes: 224,
    should_flush: true,
    data_rowids: [
        (
            549755813889,
            SegmentLeaf {
                segid: 4,
                pgno: 1,
            },
        ),
        (
            10,
            Structure,
        ),
    ],
    terms: [
        [
            48,
            102,
            116,
            115,
        ],
        [
            48,
            114,
            117,
            115,
            116,
        ],
        [
            48,
            114,
            117,
            115,
            116,
            121,
        ],
        [
            49,
            102,
            116,
        ],
        [
            49,
            114,
            117,
        ],
    ],
    structure: Fts5StructureRecord {
        cookie: 0,
        write_counter: 1,
        origin_counter: 0,
        levels: [
            Fts5StructureLevel {
                merge_inputs: 0,
                segments: [
                    Fts5StructureSegment {
                        segid: 4,
                        pgno_first: 1,
                        pgno_last: 1,
                        origin_lower: 0,
                        origin_upper: 0,
                        tombstone_page_count: 0,
                        tombstone_entry_count: 0,
                        entry_count: 0,
                    },
                ],
            },
        ],
    },
    merge_plan: Some(
        Fts5MergePlan {
            kind: Auto,
            level: 0,
            segment_count: 4,
            merge_inputs: 2,
        },
    ),
    optimize_plan: Some(
        Fts5MergePlan {
            kind: Optimize,
            level: 0,
            segment_count: 4,
            merge_inputs: 4,
        },
    ),
}"
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_shadow_write_hotspot_profile() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"]).unwrap();
        table.insert_document(7, &["rust fts storage".to_owned()]);
        let flush_profile = table
            .build_pending_hash()
            .unwrap()
            .flush_to_segment(4, Fts5StructureRecord::empty_legacy(0))
            .unwrap()
            .hotspot_profile()
            .unwrap();

        let idx_data_rows = vec![
            Fts5DataRow::new(
                Fts5DataRowid::SegmentLeaf { segid: 8, pgno: 1 }
                    .encode()
                    .unwrap(),
                Vec::new(),
            ),
            Fts5DataRow::new(
                Fts5DataRowid::DoclistIndex {
                    segid: 8,
                    height: 1,
                    pgno: 3,
                }
                .encode()
                .unwrap(),
                Vec::new(),
            ),
            Fts5DataRow::new(FTS5_STRUCTURE_ROWID, Vec::new()),
        ];
        let idx_rows = vec![
            Fts5IdxRow::new(8, b"alpha".to_vec(), 2, true),
            Fts5IdxRow::new(8, b"beta".to_vec(), 2, false),
        ];
        let snapshot = Fts5ShadowWriteHotspotProfileStructure {
            flush_profile,
            idx_profile: Fts5ShadowWriteHotspotProfile::from_rows(&idx_data_rows, &idx_rows)
                .unwrap(),
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r"Fts5ShadowWriteHotspotProfileStructure {
    flush_profile: Fts5ShadowWriteHotspotProfile {
        data_row_writes: 2,
        idx_row_writes: 0,
        append_only_data_writes: 1,
        segment_leaf_writes: 1,
        doclist_index_writes: 0,
        tombstone_writes: 0,
        averages_record_writes: 0,
        structure_record_writes: 1,
        hot_write_count: 1,
        append_only_rows_precede_hot_rows: true,
        unique_segment_pages: [
            (
                4,
                1,
            ),
        ],
        unique_doclist_index_pages: [],
        unique_tombstone_pages: [],
        unique_idx_pages: [],
        publication_order: [
            SegmentLeaf {
                segid: 4,
                pgno: 1,
            },
            StructureRecord,
        ],
        mitigation: AppendOnlyRowsBeforeSingleMetadataCommit,
    },
    idx_profile: Fts5ShadowWriteHotspotProfile {
        data_row_writes: 3,
        idx_row_writes: 2,
        append_only_data_writes: 2,
        segment_leaf_writes: 1,
        doclist_index_writes: 1,
        tombstone_writes: 0,
        averages_record_writes: 0,
        structure_record_writes: 1,
        hot_write_count: 3,
        append_only_rows_precede_hot_rows: true,
        unique_segment_pages: [
            (
                8,
                1,
            ),
        ],
        unique_doclist_index_pages: [
            (
                8,
                1,
                3,
            ),
        ],
        unique_tombstone_pages: [],
        unique_idx_pages: [
            (
                8,
                2,
            ),
        ],
        publication_order: [
            SegmentLeaf {
                segid: 8,
                pgno: 1,
            },
            DoclistIndex {
                segid: 8,
                height: 1,
                pgno: 3,
            },
            StructureRecord,
            IdxPage {
                segid: 8,
                btree_page: 2,
            },
            IdxPage {
                segid: 8,
                btree_page: 2,
            },
        ],
        mitigation: AppendOnlyRowsBeforeBatchedHotWrites,
    },
}"
        );
    }

    #[test]
    fn test_fts5_lazy_segment_reader_exact_prefix_and_merge_primitives() {
        let (structure, data_rows, idx_rows) = sample_lazy_segment_rows();
        let row_set = Fts5SegmentRowSet::new(&data_rows, &idx_rows);
        let reader = row_set.reader(&structure.levels[0].segments[0]);

        let terms: Vec<Vec<u8>> = reader
            .term_cursor()
            .map(|term| term.unwrap().term)
            .collect();
        assert_eq!(
            terms,
            vec![
                b"brown".to_vec(),
                b"fox".to_vec(),
                b"rust".to_vec(),
                b"rusty".to_vec()
            ]
        );

        let brown = reader.exact_postings(b"brown").unwrap().unwrap();
        let fox = reader.exact_postings(b"fox").unwrap().unwrap();
        assert_eq!(brown.rowids(), vec![1, 4]);
        assert_eq!(fox.rowids(), vec![1, 3]);
        assert_eq!(brown.union_rowids(&fox), vec![1, 3, 4]);
        assert_eq!(brown.intersect_rowids(&fox), vec![1]);
        assert_eq!(brown.phrase_rowids(&fox), vec![1]);
        assert_eq!(brown.near_rowids(&fox, 2), vec![1]);

        let prefix_terms: Vec<Vec<u8>> = reader
            .prefix_matches(b"ru")
            .unwrap()
            .into_iter()
            .map(|term| term.term)
            .collect();
        assert_eq!(prefix_terms, vec![b"rust".to_vec(), b"rusty".to_vec()]);
    }

    #[test]
    fn test_fts5_segment_integrity_rejects_missing_and_orphan_rows() {
        let (structure, mut data_rows, idx_rows) = sample_lazy_segment_rows();
        let row_set = Fts5SegmentRowSet::new(&data_rows[..1], &idx_rows);
        assert!(
            row_set
                .integrity_report(&structure)
                .expect_err("missing second leaf should fail")
                .to_string()
                .contains("missing segment leaf")
        );

        data_rows.push(Fts5SegmentLeaf::new(Vec::new()).to_data_row(6, 1).unwrap());
        let row_set = Fts5SegmentRowSet::new(&data_rows, &idx_rows);
        assert!(
            row_set
                .integrity_report(&structure)
                .expect_err("orphan segment row should fail")
                .to_string()
                .contains("unknown segment")
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_lazy_segment_reader() {
        let (structure, data_rows, idx_rows) = sample_lazy_segment_rows();
        let row_set = Fts5SegmentRowSet::new(&data_rows, &idx_rows);
        let reader = row_set.reader(&structure.levels[0].segments[0]);
        let brown = reader.exact_postings(b"brown").unwrap().unwrap();
        let fox = reader.exact_postings(b"fox").unwrap().unwrap();
        let snapshot = Fts5SegmentReaderStructure {
            cursor_terms: reader
                .term_cursor()
                .map(|term| term.unwrap().term)
                .collect(),
            exact_brown_rowids: brown.rowids(),
            prefix_ru_terms: reader
                .prefix_matches(b"ru")
                .unwrap()
                .into_iter()
                .map(|term| term.term)
                .collect(),
            union_brown_fox: brown.union_rowids(&fox),
            phrase_brown_fox: brown.phrase_rowids(&fox),
            near_brown_fox: brown.near_rowids(&fox, 2),
            integrity: row_set.integrity_report(&structure).unwrap(),
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r"Fts5SegmentReaderStructure {
    cursor_terms: [
        [
            98,
            114,
            111,
            119,
            110,
        ],
        [
            102,
            111,
            120,
        ],
        [
            114,
            117,
            115,
            116,
        ],
        [
            114,
            117,
            115,
            116,
            121,
        ],
    ],
    exact_brown_rowids: [
        1,
        4,
    ],
    prefix_ru_terms: [
        [
            114,
            117,
            115,
            116,
        ],
        [
            114,
            117,
            115,
            116,
            121,
        ],
    ],
    union_brown_fox: [
        1,
        3,
        4,
    ],
    phrase_brown_fox: [
        1,
    ],
    near_brown_fox: [
        1,
    ],
    integrity: Fts5SegmentIntegrityReport {
        segment_count: 1,
        leaf_page_count: 2,
        idx_row_count: 1,
        term_count: 4,
        checksum: 3861481483356166922,
    },
}"
        );
    }

    #[test]
    fn test_fts5_shadow_rows_round_trip_stored_content_and_docsize() {
        let cx = Cx::new();
        let mut table =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        table.insert_document(
            7,
            &["rust guide".to_owned(), "storage backend codec".to_owned()],
        );
        table.insert_document(9, &["sqlite notes".to_owned(), "rust fts".to_owned()]);

        let rows = table.encode_shadow_rows();
        assert_eq!(
            rows.content,
            vec![
                Fts5ContentRow::new(
                    7,
                    vec!["rust guide".to_owned(), "storage backend codec".to_owned()]
                ),
                Fts5ContentRow::new(9, vec!["sqlite notes".to_owned(), "rust fts".to_owned()]),
            ]
        );
        assert_eq!(
            rows.docsize,
            vec![
                Fts5DocsizeRow::new(7, vec![2, 3]),
                Fts5DocsizeRow::new(9, vec![2, 2]),
            ]
        );

        let mut reopened =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        reopened.apply_shadow_rows(&rows).unwrap();
        let mut matches: Vec<i64> = reopened
            .search("rust")
            .unwrap()
            .into_iter()
            .map(|(rowid, _rank)| rowid)
            .collect();
        matches.sort_unstable();
        assert_eq!(matches, vec![7, 9]);
        assert_eq!(
            reopened.lookup_content_row(7),
            Some(Fts5ContentRow::new(
                7,
                vec!["rust guide".to_owned(), "storage backend codec".to_owned()]
            ))
        );
        assert_eq!(
            reopened.lookup_docsize_row(7),
            Some(Fts5DocsizeRow::new(7, vec![2, 3]))
        );
    }

    #[test]
    fn test_fts5_shadow_rows_contentless_emits_docsize_without_content() {
        let cx = Cx::new();
        let mut table =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "content=''"]).unwrap();
        table.insert_document(3, &["alpha beta gamma".to_owned()]);

        assert!(table.encode_content_rows().is_empty());
        assert_eq!(
            table.lookup_docsize_row(3),
            Some(Fts5DocsizeRow::new(3, vec![3]))
        );
        assert_eq!(table.lookup_content_row(3), None);

        let rows = table.encode_shadow_rows();
        let opened = Fts5Table::open_shadow_rows(
            &cx,
            &["fts5", "main", "docs", "body", "content=''"],
            &rows,
        )
        .unwrap();
        assert_eq!(opened.table.row_count(), 1);
        assert_eq!(opened.table.all_rows(), vec![(3_i64, Vec::<String>::new())]);

        let mut cursor =
            fsqlite_func::vtab::ErasedVtabInstance::open_cursor(&opened.table).unwrap();
        cursor.erased_filter(&cx, 0, None, &[]).unwrap();
        assert!(!cursor.erased_eof());
        assert_eq!(cursor.erased_rowid().unwrap(), 3);
        cursor.erased_next(&cx).unwrap();
        assert!(cursor.erased_eof());
    }

    #[test]
    fn test_fts5_shadow_rows_contentless_unindexed_keeps_only_unindexed_content() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title UNINDEXED",
                "body",
                "content=''",
                "contentless_unindexed=1",
            ],
        )
        .unwrap();
        table.insert_document(5, &["visible title".to_owned(), "search body".to_owned()]);

        assert_eq!(
            table.encode_content_rows(),
            vec![Fts5ContentRow::new(
                5,
                vec!["visible title".to_owned(), String::new()]
            )]
        );
        assert_eq!(
            table.lookup_docsize_row(5),
            Some(Fts5DocsizeRow::new(5, vec![0, 2]))
        );
        assert_eq!(search_rowids(&table, "visible").unwrap(), Vec::<i64>::new());
        assert_eq!(search_rowids(&table, "search").unwrap(), vec![5]);
    }

    #[test]
    fn test_fts5_open_shadow_rows_binds_stock_segments_without_rebuild() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let (structure, mut data_rows, idx_rows) = sample_lazy_segment_rows();
        data_rows.insert(
            0,
            Fts5DataRow::new(
                FTS5_AVERAGES_ROWID,
                Fts5AveragesRecord::new(2, vec![3, 4]).encode(),
            ),
        );
        data_rows.insert(
            1,
            Fts5DataRow::new(FTS5_STRUCTURE_ROWID, structure.encode()),
        );
        let rows = Fts5ShadowRows {
            data: data_rows,
            idx: idx_rows,
            config: base.encode_config_rows(),
            content: vec![
                Fts5ContentRow::new(1, vec!["quick brown".to_owned(), "fox".to_owned()]),
                Fts5ContentRow::new(4, vec!["slow brown".to_owned(), "bear".to_owned()]),
            ],
            docsize: vec![
                Fts5DocsizeRow::new(1, vec![2, 1]),
                Fts5DocsizeRow::new(4, vec![2, 1]),
            ],
        };

        let opened =
            Fts5Table::open_shadow_rows(&cx, &["fts5", "main", "docs", "title", "body"], &rows)
                .unwrap();
        assert!(opened.table.is_empty());
        assert_eq!(opened.table.next_rowid, 5);
        assert!(opened.report.bound_without_rebuild);
        assert_eq!(opened.report.content_row_count, 2);
        assert_eq!(opened.report.docsize_row_count, 2);
        assert_eq!(opened.report.integrity.unwrap().term_count, 4);
    }

    #[test]
    fn test_fts5_apply_shadow_rows_binds_segments_without_rebuild_and_hydrates_on_write() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let rows = sample_shadow_query_rows(&base);
        let mut reopened =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();

        reopened.apply_shadow_rows(&rows).unwrap();

        assert!(
            reopened.is_empty(),
            "persisted segments should bind without hydrating content rows"
        );
        assert!(
            reopened.shadow_rows.is_some(),
            "apply_shadow_rows should keep segment-backed query state"
        );
        assert_eq!(search_rowids(&reopened, "brown").unwrap(), vec![1, 4]);

        reopened.insert_document(9, &["fresh rust".to_owned(), "new row".to_owned()]);

        assert!(
            reopened.shadow_rows.is_none(),
            "first write should hydrate shadow content before mutating"
        );
        assert_eq!(search_rowids(&reopened, "brown").unwrap(), vec![1, 4]);
        assert_eq!(search_rowids(&reopened, "fresh").unwrap(), vec![9]);
    }

    #[test]
    fn test_fts5_shadow_rows_failed_update_preserves_lazy_binding() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let rows = sample_shadow_query_rows(&base);
        let mut reopened =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();

        reopened.apply_shadow_rows(&rows).unwrap();
        let err = reopened
            .update(
                &cx,
                &[
                    SqliteValue::Integer(99),
                    SqliteValue::Integer(99),
                    SqliteValue::Text(SmallText::from_string("missing row")),
                    SqliteValue::Text(SmallText::from_string("should not hydrate")),
                ],
            )
            .expect_err("missing row update should fail");

        assert!(err.to_string().contains("missing rowid"));
        assert!(
            reopened.shadow_rows.is_some(),
            "failed validation should not consume lazy shadow rows"
        );
        assert!(
            reopened.is_empty(),
            "failed validation should not hydrate content rows"
        );
        assert_eq!(search_rowids(&reopened, "brown").unwrap(), vec![1, 4]);
    }

    #[test]
    fn test_fts5_contentless_lazy_shadow_mutation_hydrates_and_preserves_segments() {
        // bd-sf8dx / cass y8n3i: a reopened contentless table with persisted
        // segments must hydrate its in-memory index from those segments on the
        // first mutation (the postings are otherwise unrecoverable — contentless
        // keeps no `_content` rows), then append the new row WITHOUT dropping the
        // old postings.
        let cx = Cx::new();
        let base = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "body",
                "content=''",
                "contentless_delete=1",
            ],
        )
        .unwrap();
        let mut rows = sample_shadow_query_rows(&base);
        rows.content.clear();
        let mut reopened = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "body",
                "content=''",
                "contentless_delete=1",
            ],
        )
        .unwrap();

        reopened.apply_shadow_rows(&rows).unwrap();
        assert_eq!(search_rowids(&reopened, "brown").unwrap(), vec![1, 4]);

        reopened
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(99),
                    SqliteValue::Text(SmallText::from_string("fresh")),
                    SqliteValue::Text(SmallText::from_string("row")),
                ],
            )
            .expect("contentless lazy mutation hydrates segments and appends");

        // The lazy shadow binding is consumed by the hydration.
        assert!(reopened.shadow_rows.is_none());
        // Old postings survive the hydrate+append...
        assert_eq!(search_rowids(&reopened, "brown").unwrap(), vec![1, 4]);
        // ...and the freshly inserted row is searchable.
        assert_eq!(search_rowids(&reopened, "fresh").unwrap(), vec![99]);
    }

    #[test]
    fn test_fts5_open_shadow_rows_rejects_inconsistent_stock_layout() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let rows = Fts5ShadowRows {
            data: base.encode_data_rows(),
            idx: Vec::new(),
            config: base.encode_config_rows(),
            content: vec![Fts5ContentRow::new(1, vec!["only one column".to_owned()])],
            docsize: Vec::new(),
        };

        assert!(
            Fts5Table::open_shadow_rows(&cx, &["fts5", "main", "docs", "title", "body"], &rows)
                .expect_err("content rows must match table columns")
                .to_string()
                .contains("content row column count")
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_shadow_rows() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "body",
                "secure-delete=1",
                "insttoken=1",
            ],
        )
        .unwrap();
        table.insert_document(
            11,
            &["rust index".to_owned(), "shadow table rows".to_owned()],
        );
        let rows = table.encode_shadow_rows();
        let snapshot = Fts5ShadowRowsStructure {
            data: rows
                .data
                .iter()
                .map(|row| (row.id, row.block.clone()))
                .collect(),
            idx: rows.idx.clone(),
            config: rows
                .config
                .iter()
                .map(|record| (record.key.clone(), record.value.to_text()))
                .collect(),
            content: rows.content.clone(),
            docsize: rows.docsize.clone(),
            matches: search_rowids(&table, "shadow").unwrap(),
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5ShadowRowsStructure {
    data: [
        (
            1,
            [
                1,
                2,
                3,
            ],
        ),
        (
            137438953473,
            [
                0,
                11,
                0,
                58,
                6,
                48,
                105,
                110,
                100,
                101,
                120,
                11,
                2,
                3,
                1,
                4,
                114,
                111,
                119,
                115,
                11,
                6,
                1,
                1,
                4,
                2,
                3,
                117,
                115,
                116,
                11,
                2,
                2,
                1,
                6,
                115,
                104,
                97,
                100,
                111,
                119,
                11,
                6,
                1,
                1,
                2,
                1,
                5,
                116,
                97,
                98,
                108,
                101,
                11,
                6,
                1,
                1,
                3,
                4,
                10,
                11,
                8,
                13,
            ],
        ),
        (
            10,
            [
                0,
                0,
                0,
                0,
                1,
                1,
                1,
                0,
                1,
                1,
                1,
                1,
            ],
        ),
    ],
    idx: [],
    config: [
        (
            "insttoken",
            "1",
        ),
        (
            "secure-delete",
            "1",
        ),
        (
            "version",
            "4",
        ),
    ],
    content: [
        Fts5ContentRow {
            rowid: 11,
            values: [
                "rust index",
                "shadow table rows",
            ],
        },
    ],
    docsize: [
        Fts5DocsizeRow {
            rowid: 11,
            column_token_counts: [
                2,
                3,
            ],
        },
    ],
    matches: [
        11,
    ],
}"#
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_shadow_open_without_rebuild() {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let (structure, mut data_rows, idx_rows) = sample_lazy_segment_rows();
        data_rows.insert(
            0,
            Fts5DataRow::new(
                FTS5_AVERAGES_ROWID,
                Fts5AveragesRecord::new(2, vec![3, 4]).encode(),
            ),
        );
        data_rows.insert(
            1,
            Fts5DataRow::new(FTS5_STRUCTURE_ROWID, structure.encode()),
        );
        let rows = Fts5ShadowRows {
            data: data_rows,
            idx: idx_rows,
            config: base.encode_config_rows(),
            content: vec![
                Fts5ContentRow::new(1, vec!["quick brown".to_owned(), "fox".to_owned()]),
                Fts5ContentRow::new(4, vec!["slow brown".to_owned(), "bear".to_owned()]),
            ],
            docsize: vec![
                Fts5DocsizeRow::new(1, vec![2, 1]),
                Fts5DocsizeRow::new(4, vec![2, 1]),
            ],
        };
        let opened =
            Fts5Table::open_shadow_rows(&cx, &["fts5", "main", "docs", "title", "body"], &rows)
                .unwrap();
        let snapshot = Fts5ShadowOpenStructure {
            report: opened.report,
            table_empty: opened.table.is_empty(),
            next_rowid: opened.table.next_rowid,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r"Fts5ShadowOpenStructure {
    report: Fts5ShadowOpenReport {
        metadata: Fts5ConfigMetadata {
            format_version: 4,
            page_size: 4050,
            automerge: 4,
            usermerge: 4,
            crisismerge: 16,
            hash_size: 1048576,
            delete_merge: 10,
            rank: None,
            secure_delete: false,
            insttoken: false,
        },
        averages: Some(
            Fts5AveragesRecord {
                total_rows: 2,
                column_token_totals: [
                    3,
                    4,
                ],
            },
        ),
        structure: Some(
            Fts5StructureRecord {
                cookie: 0,
                write_counter: 2,
                origin_counter: 0,
                levels: [
                    Fts5StructureLevel {
                        merge_inputs: 0,
                        segments: [
                            Fts5StructureSegment {
                                segid: 5,
                                pgno_first: 1,
                                pgno_last: 2,
                                origin_lower: 0,
                                origin_upper: 0,
                                tombstone_page_count: 0,
                                tombstone_entry_count: 0,
                                entry_count: 0,
                            },
                        ],
                    },
                ],
            },
        ),
        integrity: Some(
            Fts5SegmentIntegrityReport {
                segment_count: 1,
                leaf_page_count: 2,
                idx_row_count: 1,
                term_count: 4,
                checksum: 3861481483356166922,
            },
        ),
        content_row_count: 2,
        docsize_row_count: 2,
        max_seen_rowid: Some(
            4,
        ),
        bound_without_rebuild: true,
    },
    table_empty: true,
    next_rowid: 5,
}"
        );
    }

    #[test]
    fn test_fts5_shadow_query_matches_reader_segments_without_hydration()
    -> std::result::Result<(), String> {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let rows = sample_shadow_query_rows(&base);
        let opened =
            Fts5Table::open_shadow_rows(&cx, &["fts5", "main", "docs", "title", "body"], &rows)
                .map_err(|err| err.to_string())?;

        assert!(opened.table.is_empty());
        assert_eq!(
            opened
                .table
                .search("brown")
                .map_err(|err| err.to_string())?[0]
                .0,
            1
        );
        assert_eq!(
            opened
                .table
                .search("body:ru*")
                .map_err(|err| err.to_string())?
                .into_iter()
                .map(|(rowid, _score)| rowid)
                .collect::<Vec<_>>(),
            vec![20]
        );
        assert_eq!(
            opened
                .table
                .search(r#""brown fox""#)
                .map_err(|err| err.to_string())?
                .into_iter()
                .map(|(rowid, _score)| rowid)
                .collect::<Vec<_>>(),
            vec![1]
        );

        let mut cursor = opened.table.open().map_err(|err| err.to_string())?;
        cursor
            .filter(
                &cx,
                1,
                None,
                &[SqliteValue::Text(SmallText::from_string("body:ru*"))],
            )
            .map_err(|err| err.to_string())?;
        assert_eq!(cursor.rowid().map_err(|err| err.to_string())?, 20);
        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 1).map_err(|err| err.to_string())?;
        assert_eq!(
            ctx.take_value(),
            Some(SqliteValue::Text(SmallText::from_string("rusty")))
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_shadow_query() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let base = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        let rows = sample_shadow_query_rows(&base);
        let opened =
            Fts5Table::open_shadow_rows(&cx, &["fts5", "main", "docs", "title", "body"], &rows)
                .map_err(|err| err.to_string())?;
        let rowids = |query: &str| -> std::result::Result<Vec<i64>, String> {
            opened
                .table
                .search(query)
                .map_err(|err| err.to_string())
                .map(|results| results.into_iter().map(|(rowid, _score)| rowid).collect())
        };

        let mut cursor = opened.table.open().map_err(|err| err.to_string())?;
        cursor
            .filter(
                &cx,
                1,
                None,
                &[SqliteValue::Text(SmallText::from_string("body:ru*"))],
            )
            .map_err(|err| err.to_string())?;
        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 1).map_err(|err| err.to_string())?;

        let snapshot = Fts5ShadowQueryStructure {
            exact_matches: rowids("brown")?,
            prefix_matches: rowids("ru*")?,
            phrase_matches: rowids(r#""brown fox""#)?,
            near_matches: rowids("NEAR(brown fox, 2)")?,
            initial_matches: rowids("^fox")?,
            column_filtered_matches: rowids("body:ru*")?,
            boolean_or_matches: rowids("brown OR fox")?,
            boolean_not_matches: rowids("brown NOT fox")?,
            cursor_body_value: ctx.take_value(),
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5ShadowQueryStructure {
    exact_matches: [
        1,
        4,
    ],
    prefix_matches: [
        7,
        20,
    ],
    phrase_matches: [
        1,
    ],
    near_matches: [
        1,
    ],
    initial_matches: [
        3,
    ],
    column_filtered_matches: [
        20,
    ],
    boolean_or_matches: [
        1,
        3,
        4,
    ],
    boolean_not_matches: [
        4,
    ],
    cursor_body_value: Some(
        Text(
            "rusty",
        ),
    ),
}"#
        );
        Ok(())
    }

    #[test]
    fn test_insttoken_control_command() {
        let mut config = Fts5Config::default();
        assert!(!config.insttoken_enabled());
        assert!(config.apply_control_command("insttoken=1"));
        assert!(config.insttoken_enabled());
        assert!(config.apply_control_command("insttoken=off"));
        assert!(!config.insttoken_enabled());
    }

    #[test]
    fn test_contentless_delete_rejects_without_toggle() {
        let config = Fts5Config::new(ContentMode::Contentless);
        assert_eq!(config.delete_action(), DeleteAction::Reject);
    }

    #[test]
    fn test_contentless_delete_tombstone_mode() {
        let mut config = Fts5Config::new(ContentMode::Contentless);
        assert!(config.apply_control_command("contentless_delete=1"));
        assert_eq!(config.delete_action(), DeleteAction::Tombstone);
    }

    #[test]
    fn test_contentless_delete_secure_delete_combo() {
        let mut config = Fts5Config::new(ContentMode::Contentless);
        assert!(config.apply_control_command("contentless_delete=1"));
        assert!(config.apply_control_command("secure-delete=on"));
        assert_eq!(config.delete_action(), DeleteAction::PhysicalPurge);
    }

    // -- Tokenizer tests --

    #[test]
    fn test_unicode61_tokenizer_basic() {
        let tok = Unicode61Tokenizer::new();
        let tokens = tok.tokenize("Hello, World! This is a Test.");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world", "this", "is", "a", "test"]);
    }

    #[test]
    fn test_unicode61_tokenizer_unicode() {
        let tok = Unicode61Tokenizer::new();
        let tokens = tok.tokenize("café résumé naïve");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["cafe", "resume", "naive"]);
    }

    #[test]
    fn test_unicode61_remove_diacritics_zero_preserves_latin_marks() {
        let tok = create_tokenizer("unicode61 remove_diacritics 0").unwrap();
        let tokens = tok.tokenize("café résumé naïve");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["café", "résumé", "naïve"]);
    }

    #[test]
    fn test_unicode61_remove_diacritics_option() {
        let tok = create_tokenizer("unicode61 remove_diacritics 2").unwrap();
        let tokens = tok.tokenize("café résumé naïve");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["cafe", "resume", "naive"]);
    }

    #[test]
    fn test_create_tokenizer_unicode61_rejects_invalid_options() {
        for spec in [
            "unicode61 unknown 1",
            "unicode61 remove_diacritics maybe",
            "unicode61 remove_diacritics 3",
            "unicode61 tokenchars",
        ] {
            assert!(
                create_tokenizer(spec).is_none(),
                "invalid unicode61 spec should fail: {spec}"
            );
        }
    }

    #[test]
    fn test_fts5_structural_snapshot_unicode61_option_validation() {
        let tokenizer = create_tokenizer("unicode61 tokenchars=-_ remove_diacritics=2").unwrap();
        let accepted_terms = tokenizer
            .tokenize("café-file_name")
            .into_iter()
            .map(|token| token.term)
            .collect();
        let rejected_specs = [
            "unicode61 bogus 1",
            "unicode61 remove_diacritics=-1",
            "unicode61 remove_diacritics=4",
            "unicode61 separators",
        ]
        .into_iter()
        .filter(|spec| create_tokenizer(spec).is_none())
        .map(str::to_owned)
        .collect();
        let snapshot = Fts5Unicode61OptionValidationStructure {
            accepted_terms,
            rejected_specs,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5Unicode61OptionValidationStructure {
    accepted_terms: [
        "cafe-file_name",
    ],
    rejected_specs: [
        "unicode61 bogus 1",
        "unicode61 remove_diacritics=-1",
        "unicode61 remove_diacritics=4",
        "unicode61 separators",
    ],
}"#
        );
    }

    #[test]
    fn test_unicode61_option_char_class_fast_path() {
        let tok = create_tokenizer("unicode61 tokenchars '§-' separators 'é_' remove_diacritics 0")
            .unwrap();
        let tokens = tok.tokenize("sec§tion well-known café alpha_beta");
        let terms: Vec<&str> = tokens.iter().map(|token| token.term.as_str()).collect();

        assert_eq!(
            terms,
            vec!["sec§tion", "well-known", "caf", "alpha", "beta"]
        );
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 9);
        assert_eq!(tokens[2].start, 21);
        assert_eq!(tokens[2].end, 24);
    }

    #[test]
    fn test_fts5_structural_snapshot_unicode61_char_class() -> std::result::Result<(), String> {
        let tokenizer_spec = "unicode61 tokenchars '§-' separators 'é_' remove_diacritics 0";
        let tok = create_tokenizer(tokenizer_spec).unwrap();
        let tokens = tok
            .tokenize("sec§tion well-known café alpha_beta")
            .into_iter()
            .map(|token| (token.term, token.start, token.end))
            .collect();

        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "unicode_docs",
                "body",
                "tokenize=\"unicode61 tokenchars '§-' separators 'é_' remove_diacritics 0\"",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["sec§tion well-known café alpha_beta".to_owned()]);
        table.insert_document(2, &["section alpha cafe".to_owned()]);
        let structure = table_structure(&table);

        let mut section_matches = search_rowids(&table, "sec§tion")?;
        section_matches.sort_unstable();
        let mut alpha_matches = search_rowids(&table, "alpha")?;
        alpha_matches.sort_unstable();
        let mut cafe_matches = search_rowids(&table, "cafe")?;
        cafe_matches.sort_unstable();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5Unicode61CharClassStructure {
                    tokens,
                    terms: structure.terms.into_iter().map(|term| term.term).collect(),
                    section_matches,
                    alpha_matches,
                    cafe_matches,
                }
            ),
            r#"Fts5Unicode61CharClassStructure {
    tokens: [
        (
            "sec§tion",
            0,
            9,
        ),
        (
            "well-known",
            10,
            20,
        ),
        (
            "caf",
            21,
            24,
        ),
        (
            "alpha",
            27,
            32,
        ),
        (
            "beta",
            33,
            37,
        ),
    ],
    terms: [
        "alpha",
        "beta",
        "caf",
        "cafe",
        "section",
        "sec§tion",
        "well-known",
    ],
    section_matches: [
        1,
    ],
    alpha_matches: [
        1,
        2,
    ],
    cafe_matches: [
        2,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_unquote_fts_arg_strips_matching_quote_pairs() {
        assert_eq!(
            unquote_fts_arg(" 'unicode61 remove_diacritics 2' "),
            "unicode61 remove_diacritics 2"
        );
        assert_eq!(unquote_fts_arg("\"café\""), "café");
        assert_eq!(unquote_fts_arg("`2 3`"), "2 3");
        assert_eq!(unquote_fts_arg("'mismatch`"), "'mismatch`");
        assert_eq!(unquote_fts_arg("''"), "");
        assert_eq!(unquote_fts_arg("plain"), "plain");
    }

    #[test]
    fn test_fts5_structural_snapshot_quoted_options() -> std::result::Result<(), String> {
        let unquoted = [
            "'unicode61 remove_diacritics 2'",
            "\"2 3\"",
            "`quoted`",
            "'mismatch`",
            "plain",
        ]
        .into_iter()
        .map(|raw| (raw.to_owned(), unquote_fts_arg(raw).to_owned()))
        .collect();

        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "quoted_docs",
                "body",
                "tokenize='unicode61 remove_diacritics 2'",
                "prefix='2 3'",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["café".to_owned()]);
        let structure = table_structure(&table);
        let mut cafe_matches = search_rowids(&table, "cafe")?;
        cafe_matches.sort_unstable();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5QuotedOptionStructure {
                    unquoted,
                    tokenizer: table.tokenizer_name.clone(),
                    prefix_lengths: table.prefix_lengths.clone(),
                    terms: structure.terms.into_iter().map(|term| term.term).collect(),
                    cafe_matches,
                }
            ),
            r#"Fts5QuotedOptionStructure {
    unquoted: [
        (
            "'unicode61 remove_diacritics 2'",
            "unicode61 remove_diacritics 2",
        ),
        (
            "\"2 3\"",
            "2 3",
        ),
        (
            "`quoted`",
            "quoted",
        ),
        (
            "'mismatch`",
            "'mismatch`",
        ),
        (
            "plain",
            "plain",
        ),
    ],
    tokenizer: "unicode61 remove_diacritics 2",
    prefix_lengths: [
        2,
        3,
    ],
    terms: [
        "cafe",
    ],
    cafe_matches: [
        1,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_detail_option_parses_case_insensitively_without_allocating() {
        assert_eq!(parse_detail_option("FULL"), Some(DetailMode::Full));
        assert_eq!(parse_detail_option(" Column "), Some(DetailMode::Column));
        assert_eq!(parse_detail_option("NoNe"), Some(DetailMode::None));
        assert_eq!(parse_detail_option("offsets"), None);
        assert_eq!(parse_detail_option(""), None);
    }

    #[test]
    fn test_fts5_structural_snapshot_detail_option_casefold() -> std::result::Result<(), String> {
        let parsed = ["FULL", " Column ", "NoNe", "offsets"]
            .into_iter()
            .map(|value| (value.to_owned(), parse_detail_option(value)))
            .collect();

        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "detail_docs", "body", "detail=CoLuMn"],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["alpha beta".to_owned()]);
        let mut matches = search_rowids(&table, "alpha")?;
        matches.sort_unstable();
        let phrase_error = table
            .search("\"alpha beta\"")
            .expect_err("detail=column should reject phrase queries")
            .to_string();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5DetailOptionStructure {
                    parsed,
                    config_detail: table.config.detail_mode(),
                    index_detail: table.index.detail_mode(),
                    matches,
                    phrase_error,
                }
            ),
            r#"Fts5DetailOptionStructure {
    parsed: [
        (
            "FULL",
            Some(
                Full,
            ),
        ),
        (
            " Column ",
            Some(
                Column,
            ),
        ),
        (
            "NoNe",
            Some(
                None,
            ),
        ),
        (
            "offsets",
            None,
        ),
    ],
    config_detail: Column,
    index_detail: Column,
    matches: [
        1,
    ],
    phrase_error: "detail=column does not support phrase queries",
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_unicode61_default_diacritics()
    -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"])
            .map_err(|err| err.to_string())?;
        table.insert_document(11, &["café résumé naïve".to_owned()]);

        let structure = table_structure(&table);
        let ascii_matches = table
            .search("cafe")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let accent_matches = table
            .search("résumé")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let snapshot = Fts5Unicode61DiacriticsStructure {
            tokenizer: table.tokenizer_name.clone(),
            terms: structure.terms.into_iter().map(|term| term.term).collect(),
            rows: structure.rows,
            ascii_matches,
            accent_matches,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5Unicode61DiacriticsStructure {
    tokenizer: "unicode61",
    terms: [
        "cafe",
        "naive",
        "resume",
    ],
    rows: [
        (
            11,
            [
                "café résumé naïve",
            ],
        ),
    ],
    ascii_matches: [
        11,
    ],
    accent_matches: [
        11,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_table_search_unicode61_remove_diacritics() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "tokenize='unicode61 remove_diacritics 2'",
            ],
        )
        .unwrap();
        table.insert_document(5, &["café résumé".to_owned()]);

        assert_eq!(table.search("cafe").unwrap()[0].0, 5);
        assert_eq!(table.search("résumé").unwrap()[0].0, 5);
        assert!(table.search("naive").unwrap().is_empty());
    }

    #[test]
    fn test_fts5_structural_snapshot_unicode61_remove_diacritics() -> std::result::Result<(), String>
    {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "tokenize='unicode61 remove_diacritics 2'",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(9, &["café résumé naïve".to_owned()]);

        let structure = table_structure(&table);
        let ascii_matches = table
            .search("cafe")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let accent_matches = table
            .search("résumé")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let snapshot = Fts5Unicode61DiacriticsStructure {
            tokenizer: table.tokenizer_name.clone(),
            terms: structure.terms.into_iter().map(|term| term.term).collect(),
            rows: structure.rows,
            ascii_matches,
            accent_matches,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5Unicode61DiacriticsStructure {
    tokenizer: "unicode61 remove_diacritics 2",
    terms: [
        "cafe",
        "naive",
        "resume",
    ],
    rows: [
        (
            9,
            [
                "café résumé naïve",
            ],
        ),
    ],
    ascii_matches: [
        9,
    ],
    accent_matches: [
        9,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_unicode61_tokenizer_offsets() {
        let tok = Unicode61Tokenizer::new();
        let tokens = tok.tokenize("abc def");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 3);
        assert_eq!(tokens[1].start, 4);
        assert_eq!(tokens[1].end, 7);
    }

    #[test]
    fn test_ascii_tokenizer() {
        let tok = AsciiTokenizer;
        let tokens = tok.tokenize("Hello World 123");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world", "123"]);
    }

    #[test]
    fn test_ascii_tokenizer_lazy_casefold_offsets() {
        let tok = AsciiTokenizer;
        let tokens = tok.tokenize("ABCédef 123XYZ");
        let tokens: Vec<(String, usize, usize)> = tokens
            .into_iter()
            .map(|token| (token.term, token.start, token.end))
            .collect();

        assert_eq!(
            tokens,
            vec![("abcédef".to_owned(), 0, 8), ("123xyz".to_owned(), 9, 15),]
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_ascii_tokenizer_lazy_casefold()
    -> std::result::Result<(), String> {
        let tok = AsciiTokenizer;
        let tokens = tok
            .tokenize("ABCédef 123XYZ")
            .into_iter()
            .map(|token| (token.term, token.start, token.end))
            .collect();

        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "ascii_docs", "body", "tokenize='ascii'"],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["ABCédef 123XYZ".to_owned()]);
        table.insert_document(2, &["abc DEF".to_owned()]);
        let structure = table_structure(&table);

        let mut upper_matches = search_rowids(&table, "ABC")?;
        upper_matches.sort_unstable();
        let mut lower_matches = search_rowids(&table, "def")?;
        lower_matches.sort_unstable();
        let mut numeric_matches = search_rowids(&table, "123XYZ")?;
        numeric_matches.sort_unstable();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5AsciiTokenizerStructure {
                    tokens,
                    terms: structure.terms.into_iter().map(|term| term.term).collect(),
                    upper_matches,
                    lower_matches,
                    numeric_matches,
                }
            ),
            r#"Fts5AsciiTokenizerStructure {
    tokens: [
        (
            "abcédef",
            0,
            8,
        ),
        (
            "123xyz",
            9,
            15,
        ),
    ],
    terms: [
        "123xyz",
        "abc",
        "abcédef",
        "def",
    ],
    upper_matches: [
        2,
    ],
    lower_matches: [
        2,
    ],
    numeric_matches: [
        1,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_porter_tokenizer_stemming() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        let tokens = tok.tokenize("running jumps connected");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms[0], "run");
        assert_eq!(terms[1], "jump");
        assert_eq!(terms[2], "connect");
    }

    #[test]
    fn test_porter_stem_plurals() {
        assert_eq!(porter_stem("caresses"), "caress");
        assert_eq!(porter_stem("ponies"), "poni");
        assert_eq!(porter_stem("cats"), "cat");
    }

    #[test]
    fn test_trigram_tokenizer() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("abcde");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["abc", "bcd", "cde"]);
    }

    #[test]
    fn test_trigram_tokenizer_short_input() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("ab");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_create_tokenizer_by_name() {
        assert!(create_tokenizer("unicode61").is_some());
        assert!(create_tokenizer("ascii").is_some());
        assert!(create_tokenizer("porter").is_some());
        assert!(create_tokenizer("trigram").is_some());
        assert!(create_tokenizer("nonexistent").is_none());
    }

    #[test]
    fn test_fts5_extension_api_registers_custom_tokenizer_and_aux()
    -> std::result::Result<(), String> {
        let mut api = Fts5ExtensionApi::with_builtins();
        let _ = api.register_tokenizer("synonym", |args| {
            let suffix = args.first().cloned().unwrap_or_else(|| "_syn".to_owned());
            Some(Box::new(SynonymTestTokenizer { suffix }))
        });
        let _ = api.register_auxiliary("query_term_count", |ctx, args| {
            let extra = args.first().map_or(0, SqliteValue::to_integer);
            Ok(SqliteValue::Integer(
                i64::try_from(ctx.query_terms.len()).unwrap_or(i64::MAX) + extra,
            ))
        });

        let tokens = api
            .tokenize(
                "synonym _alias",
                Fts5TokenizeRequest::new("Rust", Fts5TokenizeReason::Query).with_locale("en_US"),
            )
            .map_err(|err| err.to_string())?;
        assert_eq!(
            tokens,
            vec![
                Fts5Token {
                    term: "rust".to_owned(),
                    start: 0,
                    end: 4,
                    colocated: false,
                },
                Fts5Token {
                    term: "rust_alias".to_owned(),
                    start: 0,
                    end: 4,
                    colocated: true,
                },
            ]
        );

        let columns = vec!["body".to_owned()];
        let terms = vec!["rust".to_owned(), "sqlite".to_owned()];
        let context = Fts5ExtensionContext::new("docs", &columns)
            .with_query_terms(&terms)
            .with_locale("en_US");
        let value = api
            .invoke_auxiliary("query_term_count", &context, &[SqliteValue::Integer(3)])
            .map_err(|err| err.to_string())?;
        assert_eq!(value, SqliteValue::Integer(5));
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_extension_api() -> std::result::Result<(), String> {
        let mut api = Fts5ExtensionApi::with_builtins();
        let _ = api.register_tokenizer("synonym", |args| {
            let suffix = args.first().cloned().unwrap_or_else(|| "_syn".to_owned());
            Some(Box::new(SynonymTestTokenizer { suffix }))
        });
        let _ = api.register_auxiliary("row_summary", |ctx, args| {
            let label = args.first().map_or_else(String::new, SqliteValue::to_text);
            let rowid = ctx.rowid.unwrap_or_default();
            let locale = ctx.locale.unwrap_or("none");
            Ok(SqliteValue::Text(SmallText::from_string(format!(
                "{label}:{rowid}:{locale}:{}",
                ctx.query_terms.join("|")
            ))))
        });

        let columns = vec!["title".to_owned(), "body".to_owned()];
        let row = vec!["Rust".to_owned(), "SQLite".to_owned()];
        let query_terms = vec!["rust".to_owned(), "sqlite".to_owned()];
        let context = Fts5ExtensionContext::new("docs", &columns)
            .with_query_terms(&query_terms)
            .with_row(42, &row)
            .with_locale("en_US");

        let missing_aux_error = api
            .invoke_auxiliary("missing_aux", &context, &[])
            .expect_err("missing custom aux should be explicit")
            .to_string();
        let snapshot = Fts5ExtensionApiStructure {
            tokenizers: api.tokenizer_names(),
            auxiliaries: api.auxiliary_names(),
            document_tokens: api
                .tokenize(
                    "unicode61",
                    Fts5TokenizeRequest::new("Café Rust", Fts5TokenizeReason::Document),
                )
                .map_err(|err| err.to_string())?,
            query_tokens: api
                .tokenize(
                    "synonym _q",
                    Fts5TokenizeRequest::new("Rust", Fts5TokenizeReason::Query)
                        .with_locale("en_US"),
                )
                .map_err(|err| err.to_string())?,
            prefix_tokens: api
                .tokenize(
                    "trigram",
                    Fts5TokenizeRequest::new("Prefix", Fts5TokenizeReason::Prefix),
                )
                .map_err(|err| err.to_string())?,
            aux_value: api
                .invoke_auxiliary(
                    "row_summary",
                    &context,
                    &[SqliteValue::Text(SmallText::from_string("hit"))],
                )
                .map_err(|err| err.to_string())?,
            missing_aux_error,
        };

        assert_eq!(
            format!("{snapshot:#?}"),
            r#"Fts5ExtensionApiStructure {
    tokenizers: [
        "ascii",
        "porter",
        "synonym",
        "trigram",
        "unicode61",
    ],
    auxiliaries: [
        "column_count",
        "row_summary",
        "rowid",
    ],
    document_tokens: [
        Fts5Token {
            term: "cafe",
            start: 0,
            end: 5,
            colocated: false,
        },
        Fts5Token {
            term: "rust",
            start: 6,
            end: 10,
            colocated: false,
        },
    ],
    query_tokens: [
        Fts5Token {
            term: "rust",
            start: 0,
            end: 4,
            colocated: false,
        },
        Fts5Token {
            term: "rust_q",
            start: 0,
            end: 4,
            colocated: true,
        },
    ],
    prefix_tokens: [
        Fts5Token {
            term: "pre",
            start: 0,
            end: 3,
            colocated: false,
        },
        Fts5Token {
            term: "ref",
            start: 1,
            end: 4,
            colocated: false,
        },
        Fts5Token {
            term: "efi",
            start: 2,
            end: 5,
            colocated: false,
        },
        Fts5Token {
            term: "fix",
            start: 3,
            end: 6,
            colocated: false,
        },
    ],
    aux_value: Text(
        "hit:42:en_US:rust|sqlite",
    ),
    missing_aux_error: "fts5: unknown auxiliary function 'missing_aux'",
}"#
        );
        Ok(())
    }

    // -- Query parsing tests --

    #[test]
    fn test_fts5_query_implicit_and() {
        let tokens = parse_fts5_query("hello world").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::And,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_fts5_query_or() {
        let tokens = parse_fts5_query("hello OR world").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Or,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_fts5_query_not_binary_only() {
        // Unary NOT is forbidden in FTS5.
        let err = parse_fts5_query("NOT hello").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnaryNotForbidden);
    }

    #[test]
    fn test_fts5_query_binary_not() {
        let tokens = parse_fts5_query("hello NOT world").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Not,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_fts5_query_phrase() {
        let tokens = parse_fts5_query(r#""exact phrase""#).unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::Phrase);
        assert_eq!(tokens[0].lexeme, "exact phrase");
    }

    #[test]
    fn test_fts5_query_prefix() {
        let tokens = parse_fts5_query("hel*").unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::Prefix);
        assert_eq!(tokens[0].lexeme, "hel");
    }

    #[test]
    fn test_fts5_query_phrase_concatenation_tokens() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query(r#""one two" + three"#).map_err(|err| err.to_string())?;
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|token| token.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::Phrase,
                Fts5QueryTokenKind::Plus,
                Fts5QueryTokenKind::Term,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_fts5_query_column_filter() {
        let tokens = parse_fts5_query("title: hello").unwrap();
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "title");
    }

    #[test]
    fn test_fts5_query_inline_column_filter() {
        let tokens = parse_fts5_query("title:hello").unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "title");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
        assert_eq!(tokens[1].lexeme, "hello");
    }

    #[test]
    fn test_fts5_query_inline_column_filter_prefix() {
        let tokens = parse_fts5_query("title:hel*").unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "title");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Prefix);
        assert_eq!(tokens[1].lexeme, "hel");
    }

    #[test]
    fn test_fts5_query_braced_column_filter_set() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query("{title body}: rust").map_err(|err| err.to_string())?;
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "{title,body}");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
        assert_eq!(tokens[1].lexeme, "rust");
        Ok(())
    }

    #[test]
    fn test_fts5_query_negative_column_filter() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query("- tag : rust").map_err(|err| err.to_string())?;
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "-tag");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
        assert_eq!(tokens[1].lexeme, "rust");
        Ok(())
    }

    #[test]
    fn test_fts5_query_negative_braced_column_filter_set() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query("- {title body}: rust").map_err(|err| err.to_string())?;
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::ColumnFilter);
        assert_eq!(tokens[0].lexeme, "-{title,body}");
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
        assert_eq!(tokens[1].lexeme, "rust");
        Ok(())
    }

    #[test]
    fn test_fts5_query_unbalanced_parens() {
        let err = parse_fts5_query("(hello").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnbalancedParentheses);
    }

    #[test]
    fn test_fts5_query_unclosed_phrase() {
        let err = parse_fts5_query(r#""unclosed"#).unwrap_err();
        assert_eq!(err, Fts5QueryError::UnclosedPhrase);
    }

    #[test]
    fn test_fts5_query_complex() {
        let tokens = parse_fts5_query("(hello OR world) NOT goodbye").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                Fts5QueryTokenKind::LParen,
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Or,
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::RParen,
                Fts5QueryTokenKind::Not,
                Fts5QueryTokenKind::Term,
            ]
        );
    }

    // -- Inverted index tests --

    #[test]
    fn test_inverted_index_add_and_query() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        let tokens = tok.tokenize("hello world");
        index.add_document(1, 0, &tokens);

        let tokens = tok.tokenize("hello rust");
        index.add_document(2, 0, &tokens);

        assert_eq!(index.total_docs(), 2);
        assert_eq!(index.doc_frequency("hello"), 2);
        assert_eq!(index.doc_frequency("world"), 1);
        assert_eq!(index.doc_frequency("rust"), 1);
        assert_eq!(index.term_frequency("hello", 1), 1);
    }

    #[test]
    fn test_inverted_index_tokendata_uses_query_key_before_nul() {
        let mut index =
            InvertedIndex::with_options_and_tokendata(true, &[3], DetailMode::Full, true);
        let tokens = vec![
            Fts5Token {
                term: "alpha\0noun".to_owned(),
                start: 0,
                end: 5,
                colocated: false,
            },
            Fts5Token {
                term: "beta\0verb".to_owned(),
                start: 6,
                end: 10,
                colocated: false,
            },
        ];
        index.add_document(1, 0, &tokens);

        assert!(index.tokendata_enabled());
        assert_eq!(tokendata_query_key("alpha\0query"), "alpha");
        assert_eq!(index.doc_frequency("alpha"), 1);
        assert_eq!(index.doc_frequency("alpha\0noun"), 1);
        assert_eq!(index.get_prefix_postings("alp").len(), 1);
        assert!(index.get_postings("noun").is_empty());
    }

    #[test]
    fn test_inverted_index_add_text_matches_tokenized_path() {
        let tok = Unicode61Tokenizer::new();
        let text = "hello world hello";

        let mut via_tokens = InvertedIndex::new();
        via_tokens.add_document(1, 0, &tok.tokenize(text));

        let mut via_stream = InvertedIndex::new();
        via_stream.add_text(1, 0, &tok, text);

        assert_eq!(via_stream.total_docs(), via_tokens.total_docs());
        assert_eq!(
            via_stream.doc_frequency("hello"),
            via_tokens.doc_frequency("hello")
        );
        assert_eq!(
            via_stream.term_frequency("hello", 1),
            via_tokens.term_frequency("hello", 1)
        );
        assert_eq!(via_stream.doc_length(1), via_tokens.doc_length(1));

        let expected_positions: Vec<u32> = via_tokens.get_postings("hello")[0]
            .positions
            .iter()
            .copied()
            .collect();
        let actual_positions: Vec<u32> = via_stream.get_postings("hello")[0]
            .positions
            .iter()
            .copied()
            .collect();
        assert_eq!(actual_positions, expected_positions);
    }

    #[test]
    fn test_inverted_index_add_text_preserves_case_folded_terms() {
        let tok = Unicode61Tokenizer::new();
        let text = "HELLO hello";

        let mut via_tokens = InvertedIndex::new();
        via_tokens.add_document(1, 0, &tok.tokenize(text));

        let mut via_stream = InvertedIndex::new();
        via_stream.add_text(1, 0, &tok, text);

        assert_eq!(
            via_stream.doc_frequency("hello"),
            via_tokens.doc_frequency("hello")
        );
        assert_eq!(
            via_stream.term_frequency("hello", 1),
            via_tokens.term_frequency("hello", 1)
        );
    }

    #[test]
    fn test_inverted_index_add_text_preserves_porter_terms() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        let text = "CATS cats";

        let mut via_tokens = InvertedIndex::new();
        via_tokens.add_document(1, 0, &tok.tokenize(text));

        let mut via_stream = InvertedIndex::new();
        via_stream.add_text(1, 0, &tok, text);

        assert_eq!(
            via_stream.doc_frequency("cat"),
            via_tokens.doc_frequency("cat")
        );
        assert_eq!(
            via_stream.term_frequency("cat", 1),
            via_tokens.term_frequency("cat", 1)
        );
    }

    #[test]
    fn test_inverted_index_remove_document() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));

        index.remove_document(1);
        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_frequency("hello"), 1);
        assert_eq!(index.doc_frequency("world"), 0);
    }

    #[test]
    fn test_inverted_index_remove_document_prunes_empty_buckets() {
        let mut index = InvertedIndex::with_options(true, &[3, 4], DetailMode::Full);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("alpine solo"));
        index.add_document(2, 0, &tok.tokenize("alpha world"));

        assert!(index.index.contains_key("alpine"));
        assert!(index.index.contains_key("solo"));
        assert!(index.prefix_indexes.get(&4).unwrap().contains_key("alpi"));
        assert!(index.prefix_indexes.get(&3).unwrap().contains_key("sol"));

        index.remove_document(1);

        assert_eq!(index.total_docs(), 1);
        assert!(!index.index.contains_key("alpine"));
        assert!(!index.index.contains_key("solo"));
        assert!(!index.prefix_indexes.get(&4).unwrap().contains_key("alpi"));
        assert!(!index.prefix_indexes.get(&3).unwrap().contains_key("sol"));
        assert_eq!(posting_docids(index.get_postings("alpha")), vec![2]);
        assert_eq!(posting_docids(index.get_postings("world")), vec![2]);
    }

    #[test]
    fn test_fts5_structural_snapshot_posting_prune() {
        let mut index = InvertedIndex::with_options(true, &[3, 4], DetailMode::Full);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("alpine solo"));
        index.add_document(2, 0, &tok.tokenize("alpha world"));
        index.add_document(3, 0, &tok.tokenize("world"));
        let terms_before = indexed_terms(&index);
        let prefix_terms_before = indexed_prefix_terms(&index, 4);

        index.remove_document(1);

        assert_eq!(
            format!(
                "{:#?}",
                Fts5PostingPruneStructure {
                    terms_before,
                    terms_after: indexed_terms(&index),
                    prefix_terms_before,
                    prefix_terms_after: indexed_prefix_terms(&index, 4),
                    remaining_docs_for_alpha: posting_docids(index.get_postings("alpha")),
                    remaining_docs_for_world: posting_docids(index.get_postings("world")),
                }
            ),
            r#"Fts5PostingPruneStructure {
    terms_before: [
        "alpha",
        "alpine",
        "solo",
        "world",
    ],
    terms_after: [
        "alpha",
        "world",
    ],
    prefix_terms_before: [
        "alph",
        "alpi",
        "solo",
        "worl",
    ],
    prefix_terms_after: [
        "alph",
        "worl",
    ],
    remaining_docs_for_alpha: [
        2,
    ],
    remaining_docs_for_world: [
        2,
        3,
    ],
}"#
        );
    }

    /// Faithful reimplementation of the *old* O(index-size) full-scan removal,
    /// used only by the equivalence proof test below.
    // `SmallText` interior mutability is content-only w.r.t. Hash/Eq (see
    // `append_position`); the `HashMap<SmallText, _>::retain` calls are safe.
    #[allow(clippy::mutable_key_type)]
    fn legacy_full_scan_remove(index: &mut InvertedIndex, docid: i64) {
        index.index.retain(|_, postings| {
            postings.retain(|posting| posting.docid != docid);
            !postings.is_empty()
        });
        for prefix_index in index.prefix_indexes.values_mut() {
            prefix_index.retain(|_, postings| {
                postings.retain(|posting| posting.docid != docid);
                !postings.is_empty()
            });
        }
        index.doc_ids.remove(&docid);
        if let Some(doc_lengths) = index.doc_lengths.as_mut() {
            doc_lengths.remove(&docid);
        }
    }

    /// One posting flattened to a sortable, content-only tuple
    /// `(docid, column, sorted positions)`.
    type PostingFingerprint = (i64, u32, Vec<u32>);

    /// Canonical, order-independent fingerprint of the observable index state:
    /// every main and prefix term with its sorted `(docid, column, positions)`
    /// postings, plus `doc_ids` and `doc_lengths`. Two indexes with the same
    /// fingerprint are byte-identical in every queryable respect (the
    /// `doc_terms` reverse map is internal bookkeeping and intentionally
    /// excluded — only its *effect* on the postings/ids is observable).
    //
    // `SmallText` interior mutability is content-only w.r.t. Hash/Eq; the
    // `HashMap<SmallText, _>` reads here are safe (see `append_position`).
    #[allow(clippy::mutable_key_type)]
    fn index_fingerprint(index: &InvertedIndex) -> String {
        fn postings_fingerprint(postings: &PostingList) -> Vec<PostingFingerprint> {
            let mut rows: Vec<PostingFingerprint> = postings
                .iter()
                .map(|posting| {
                    let mut positions: Vec<u32> = posting.positions.to_vec();
                    positions.sort_unstable();
                    (posting.docid, posting.column, positions)
                })
                .collect();
            rows.sort();
            rows
        }
        fn term_map_fingerprint(map: &HashMap<SmallText, PostingList>) -> String {
            let mut entries: Vec<(String, Vec<PostingFingerprint>)> = map
                .iter()
                .map(|(term, postings)| (term.to_string(), postings_fingerprint(postings)))
                .collect();
            entries.sort();
            format!("{entries:?}")
        }

        let main = term_map_fingerprint(&index.index);

        let mut prefix_lengths: Vec<usize> = index.prefix_indexes.keys().copied().collect();
        prefix_lengths.sort_unstable();
        let prefixes: Vec<(usize, String)> = prefix_lengths
            .iter()
            .map(|len| {
                (
                    *len,
                    term_map_fingerprint(index.prefix_indexes.get(len).unwrap()),
                )
            })
            .collect();

        let mut doc_ids: Vec<i64> = index.doc_ids.iter().copied().collect();
        doc_ids.sort_unstable();

        let doc_lengths = index.doc_lengths.as_ref().map(|lengths| {
            let mut rows: Vec<(i64, u32)> = lengths.iter().map(|(k, v)| (*k, *v)).collect();
            rows.sort();
            rows
        });

        format!("main={main}|prefixes={prefixes:?}|doc_ids={doc_ids:?}|doc_lengths={doc_lengths:?}")
    }

    /// Proof that the new reverse-map `remove_document` leaves the index in
    /// EXACTLY the state the old full-scan removal would have, across plain,
    /// prefixed, shared-term, multi-column, and `DetailMode::None` shapes —
    /// for every removed-document subset and for repeated/idempotent removals.
    #[test]
    fn test_remove_document_reverse_map_matches_legacy_full_scan() {
        let tok = Unicode61Tokenizer::new();
        // Documents deliberately share terms ("rust", "engine", "fast"),
        // exercise prefix overlap, multiple columns, and varying lengths.
        let docs: &[(i64, &[(u32, &str)])] = &[
            (1, &[(0, "rust engine is fast and safe"), (1, "rust rocks")]),
            (2, &[(0, "the engine runs fast"), (1, "engineering rust")]),
            (3, &[(0, "fast safe systems"), (1, "rust rust rust")]),
            (4, &[(0, "engine engine engine"), (1, "safety first")]),
            (5, &[(0, "unique zebra xylophone"), (1, "lonely token")]),
        ];

        let configs: &[(bool, &[usize], DetailMode)] = &[
            (true, &[], DetailMode::Full),
            (true, &[2, 3, 4], DetailMode::Full),
            (true, &[3], DetailMode::Column),
            (true, &[2, 4], DetailMode::None),
            (false, &[3], DetailMode::Full),
        ];

        // Every interesting removal subset of {1..=5} plus a no-op id (99) and
        // a duplicate removal to prove idempotency.
        let removal_sequences: &[&[i64]] = &[
            &[1],
            &[3],
            &[5],
            &[1, 2],
            &[2, 4],
            &[1, 3, 5],
            &[1, 2, 3, 4, 5],
            &[3, 3], // idempotent repeat
            &[99],   // absent doc
            &[2, 99, 2],
        ];

        for (track_sizes, prefixes, detail) in configs.iter().copied() {
            for sequence in removal_sequences {
                let build = || {
                    let mut index = InvertedIndex::with_options(track_sizes, prefixes, detail);
                    for (docid, columns) in docs {
                        for (column, text) in *columns {
                            index.add_text(*docid, *column, &tok, text);
                        }
                    }
                    index
                };

                let mut new_index = build();
                let mut legacy_index = build();

                // Sanity: identical construction.
                assert_eq!(
                    index_fingerprint(&new_index),
                    index_fingerprint(&legacy_index),
                    "construction diverged for cfg=({track_sizes},{prefixes:?},{detail:?})"
                );

                for docid in *sequence {
                    new_index.remove_document(*docid);
                    legacy_full_scan_remove(&mut legacy_index, *docid);
                }

                assert_eq!(
                    index_fingerprint(&new_index),
                    index_fingerprint(&legacy_index),
                    "reverse-map removal diverged from full-scan for \
                     cfg=({track_sizes},{prefixes:?},{detail:?}) seq={sequence:?}"
                );

                // Query-result equivalence on a representative term set,
                // including a prefix probe and per-doc term frequency.
                for term in ["rust", "engine", "fast", "safe", "zebra", "absent"] {
                    assert_eq!(
                        posting_docids(new_index.get_postings(term)),
                        posting_docids(legacy_index.get_postings(term)),
                        "postings diverged for term {term:?} seq={sequence:?}"
                    );
                    assert_eq!(
                        new_index.doc_frequency(term),
                        legacy_index.doc_frequency(term),
                        "doc_frequency diverged for term {term:?} seq={sequence:?}"
                    );
                    for docid in 1..=5 {
                        assert_eq!(
                            new_index.term_frequency(term, docid),
                            legacy_index.term_frequency(term, docid),
                            "term_frequency diverged for term {term:?} doc {docid} seq={sequence:?}"
                        );
                    }
                }
                assert_eq!(new_index.total_docs(), legacy_index.total_docs());

                // The new path must keep its reverse map consistent: a removed
                // document leaves NO reverse entry and NO surviving postings.
                for docid in 1..=5 {
                    if !new_index.doc_ids.contains(&docid) {
                        assert!(
                            !new_index.doc_terms.contains_key(&docid),
                            "removed doc {docid} still has a doc_terms entry seq={sequence:?}"
                        );
                        assert!(
                            !any_postings_for_doc(&new_index, docid),
                            "removed doc {docid} still has postings seq={sequence:?}"
                        );
                    }
                }
            }
        }
    }

    /// True if any posting in the main index references `docid`.
    fn any_postings_for_doc(index: &InvertedIndex, docid: i64) -> bool {
        index
            .index
            .values()
            .any(|postings| postings.iter().any(|posting| posting.docid == docid))
    }

    /// Faithful reimplementation of the *old* O(index-size) full-scan
    /// `remove_document`, used by the scaling microbenchmark to measure the
    /// before/after speedup head-to-head on the same corpus.
    fn legacy_full_scan_remove_for_bench(index: &mut InvertedIndex, docid: i64) {
        legacy_full_scan_remove(index, docid);
    }

    /// Microbenchmark proving the reverse-map `remove_document` removes the
    /// O(total-distinct-terms) dictionary-scan factor from the incremental
    /// upsert / re-index path — the exact shape of cass's FTS5 `--watch`.
    ///
    /// The old `remove_document` scanned EVERY posting list in the index on
    /// each call: a single delete was O(distinct-terms-in-corpus), so doing one
    /// per document over a corpus of `N` docs was O(N * distinct-terms) — i.e.
    /// quadratic, because an FTS dictionary grows with the corpus (most tokens
    /// are document-specific). The reverse map touches only the terms the
    /// removed document actually contributed, eliminating the dictionary scan.
    ///
    /// Two regimes are measured head-to-head against the old full scan on
    /// identical inputs:
    ///
    /// * **`unique`** — every document's terms are document-specific, so the
    ///   dictionary IS the whole index. The old per-op cost scales linearly
    ///   with `N` (overall quadratic); the reverse-map per-op cost is FLAT.
    ///   This is the gated claim: per-op growth `< 1.5x` when `N` doubles.
    /// * **`mixed`** — a few corpus-spanning common terms plus document
    ///   specific ones. The reverse map still wins by a large, widening
    ///   constant factor (it skips the dictionary), but a residual
    ///   O(shared-posting-list-length) `retain` cost remains for the common
    ///   terms in BOTH paths, so neither is flat here. Reported, not gated as
    ///   flat — closing that residual is a separate, larger structural change
    ///   (a per-term docid index) tracked outside this fix.
    ///
    /// Ignored by default (timing-sensitive). Run with:
    ///   cargo test -p fsqlite-ext-fts5 --release -- --ignored --nocapture \
    ///       bench_remove_document_upsert_scaling
    #[test]
    #[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
    fn bench_remove_document_upsert_scaling() {
        use std::time::Instant;

        // Index a pre-tokenized document directly via the same append path
        // add_text uses, with tokenization already done so it is outside the
        // timed window.
        fn index_terms(index: &mut InvertedIndex, docid: i64, terms: &[String]) {
            index.doc_ids.insert(docid);
            #[allow(clippy::cast_possible_truncation)]
            for (pos, term) in terms.iter().enumerate() {
                index.append_position(term, docid, 0, pos as u32);
            }
            if let Some(doc_lengths) = index.doc_lengths.as_mut() {
                #[allow(clippy::cast_possible_truncation)]
                let len = terms.len() as u32;
                *doc_lengths.entry(docid).or_insert(0) += len;
            }
        }

        struct Lcg(u64);
        impl Lcg {
            fn next_in(&mut self, n: u64) -> u64 {
                self.0 = self
                    .0
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                (self.0 >> 33) % n
            }
        }

        // Run one regime: build a corpus with `terms_for(docid)`, then perform
        // `N` random delete-then-reinsert upserts on each path. Returns the
        // per-op (new, old) timings at each `N` and asserts state-equivalence.
        let run_regime = |label: &str, terms_for: &dyn Fn(i64) -> Vec<String>, gate_flat: bool| {
            println!("\n[{label}] remove_document upsert scaling — reverse-map vs old full scan:");
            println!(
                "{:>8}  {:>16}  {:>16}  {:>9}",
                "corpus_N", "new ns/upsert", "old ns/upsert", "speedup"
            );

            let build = |n: i64| -> (InvertedIndex, Vec<Vec<String>>) {
                let mut index = InvertedIndex::with_options(true, &[], DetailMode::Full);
                let mut all_terms = Vec::with_capacity(n as usize);
                for docid in 0..n {
                    let terms = terms_for(docid);
                    index_terms(&mut index, docid, &terms);
                    all_terms.push(terms);
                }
                (index, all_terms)
            };

            let mut prev_new_per_op: Option<f64> = None;
            for &n in &[1000_i64, 2000, 4000, 8000] {
                let mut rng = Lcg(0x1234_5678_9abc_def0 ^ (n as u64));
                let targets: Vec<i64> = (0..n).map(|_| rng.next_in(n as u64) as i64).collect();

                // --- New reverse-map path ---
                let (mut new_index, all_terms) = build(n);
                let start = Instant::now();
                for &docid in &targets {
                    new_index.remove_document(docid);
                    index_terms(&mut new_index, docid, &all_terms[docid as usize]);
                }
                let new_per_op = start.elapsed().as_nanos() as f64 / n as f64;

                // --- Old full-scan path (same inputs) ---
                let (mut old_index, all_terms) = build(n);
                let start = Instant::now();
                for &docid in &targets {
                    legacy_full_scan_remove_for_bench(&mut old_index, docid);
                    index_terms(&mut old_index, docid, &all_terms[docid as usize]);
                }
                let old_per_op = start.elapsed().as_nanos() as f64 / n as f64;

                // Both paths must end in identical observable state.
                assert_eq!(
                    index_fingerprint(&new_index),
                    index_fingerprint(&old_index),
                    "[{label}] new and old upsert workloads diverged at N={n}"
                );

                let speedup = old_per_op / new_per_op;
                println!("{n:>8}  {new_per_op:>16.1}  {old_per_op:>16.1}  {speedup:>8.1}x");

                // The new path must always beat the old full scan, advantage
                // widening with corpus size (old scan is O(dictionary) per op).
                assert!(
                    speedup > 1.0,
                    "[{label}] reverse-map path was not faster than old full scan at \
                     N={n} (new {new_per_op:.1} ns vs old {old_per_op:.1} ns)"
                );

                // Flatness gate only for the unique-term regime, where the
                // reverse map removes the ONLY super-constant factor.
                if gate_flat && let Some(prev) = prev_new_per_op {
                    let growth = new_per_op / prev;
                    assert!(
                        growth < 1.5,
                        "[{label}] reverse-map per-upsert grew {growth:.2}x when corpus \
                         doubled to N={n} (prev {prev:.1} ns -> {new_per_op:.1} ns); \
                         expected ~flat"
                    );
                }
                prev_new_per_op = Some(new_per_op);
            }
        };

        // unique: dictionary == whole index; reverse map => flat per-op.
        run_regime(
            "unique",
            &|docid| (0..12).map(|k| format!("d{docid}t{k}")).collect(),
            true,
        );

        // mixed: corpus-spanning common terms + doc-specific ones; large,
        // widening constant-factor win but a residual O(shared-list) retain.
        run_regime(
            "mixed",
            &|docid| {
                const COMMON: &[&str] = &["the", "and", "data", "system"];
                let mut terms: Vec<String> = COMMON.iter().map(|s| (*s).to_string()).collect();
                for k in 0..10 {
                    terms.push(format!("d{docid}t{k}"));
                }
                terms
            },
            false,
        );
    }

    #[test]
    fn test_inverted_index_prefix_search() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello help heap"));
        index.add_document(2, 0, &tok.tokenize("world wide web"));

        let results = index.get_prefix_postings("hel");
        let docs: Vec<i64> = results.iter().map(|p| p.docid).collect();
        assert!(docs.contains(&1));
        assert!(!docs.contains(&2));
    }

    #[test]
    fn test_prefix_index_population_and_cleanup() {
        let mut index = InvertedIndex::with_options(true, &[3], DetailMode::Full);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello help"));
        index.add_document(2, 0, &tok.tokenize("world"));

        let results = index.get_prefix_postings("hel");
        let docs: Vec<i64> = results.iter().map(|posting| posting.docid).collect();
        assert!(index.tracks_prefix_length(3));
        assert!(docs.contains(&1));
        assert!(!docs.contains(&2));

        index.remove_document(1);
        assert!(index.get_prefix_postings("hel").is_empty());
    }

    #[test]
    fn test_inverted_index_detail_none_collapses_columns() {
        let mut index = InvertedIndex::with_options(true, &[], DetailMode::None);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello"));
        index.add_document(1, 1, &tok.tokenize("hello"));

        let postings = index.get_postings("hello");
        assert_eq!(index.detail_mode(), DetailMode::None);
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].column, 0);
        assert_eq!(postings[0].positions.len(), 2);
    }

    // -- BM25 tests --

    #[test]
    fn test_bm25_ranking_orders_by_relevance() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // Doc 1: "rust" appears 3 times
        index.add_document(1, 0, &tok.tokenize("rust rust rust programming"));
        // Doc 2: "rust" appears 1 time
        index.add_document(2, 0, &tok.tokenize("rust programming language features"));
        // Doc 3: no "rust"
        index.add_document(3, 0, &tok.tokenize("python programming language"));

        let query_terms = vec!["rust".to_owned()];
        let weights = vec![1.0];

        let score1 = bm25_score(&index, 1, &query_terms, &weights);
        let score2 = bm25_score(&index, 2, &query_terms, &weights);
        let score3 = bm25_score(&index, 3, &query_terms, &weights);

        // Lower score = better match (negative BM25).
        assert!(score1 < score2, "doc1 should rank higher (more rust)");
        assert!(
            score2 < score3,
            "doc2 should rank higher than doc3 (has rust)"
        );
        assert!(score3.abs() < f64::EPSILON, "doc3 should have score ~0");
    }

    // -- Expression evaluation tests --

    #[test]
    fn test_evaluate_term() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::Term("hello".to_owned());
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1, 2]);
    }

    #[test]
    fn test_evaluate_and() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::And(
            Box::new(Fts5Expr::Term("hello".to_owned())),
            Box::new(Fts5Expr::Term("world".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_or() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("rust lang"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::Or(
            Box::new(Fts5Expr::Term("hello".to_owned())),
            Box::new(Fts5Expr::Term("rust".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1, 2]);
    }

    #[test]
    fn test_evaluate_not() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("hello rust"));
        index.add_document(3, 0, &tok.tokenize("goodbye world"));

        let expr = Fts5Expr::Not(
            Box::new(Fts5Expr::Term("hello".to_owned())),
            Box::new(Fts5Expr::Term("world".to_owned())),
        );
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![2]);
    }

    #[test]
    fn test_evaluate_phrase() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("the quick brown fox"));
        index.add_document(2, 0, &tok.tokenize("brown the quick fox"));

        let expr = Fts5Expr::Phrase(vec!["quick".to_owned(), "brown".to_owned()]);
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_prefix() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello help heap"));
        index.add_document(2, 0, &tok.tokenize("world wide web"));

        let expr = Fts5Expr::Prefix("hel".to_owned());
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_initial_token() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("hello world"));
        index.add_document(2, 0, &tok.tokenize("world hello"));

        let expr = Fts5Expr::InitialToken(Box::new(Fts5Expr::Term("hello".to_owned())));
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    // -- FTS5 Table integration tests --

    #[test]
    fn test_fts5_table_insert_and_search() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(
            1,
            &["the quick brown fox jumps over the lazy dog".to_owned()],
        );
        table.insert_document(2, &["the quick red car drives over the bridge".to_owned()]);
        table.insert_document(3, &["a lazy cat sleeps on the mat".to_owned()]);

        let results = table.search("quick").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn test_fts5_table_search_implicit_and() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);

        let results = table.search("hello world").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn test_fts5_table_search_or() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["rust lang".to_owned()]);
        table.insert_document(3, &["goodbye world".to_owned()]);

        let results = table.search("hello OR rust").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_fts5_table_delete_document() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);

        table.delete_document(1);

        let results = table.search("hello").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn test_fts5_table_multicolumn() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);

        table.insert_document(
            1,
            &[
                "Rust Programming".to_owned(),
                "Rust is a systems language".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "Python Guide".to_owned(),
                "Python is interpreted".to_owned(),
            ],
        );

        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn test_fts5_table_search_column_filter() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);

        table.insert_document(1, &["Rust title".to_owned(), "plain body".to_owned()]);
        table.insert_document(2, &["Plain title".to_owned(), "rust body".to_owned()]);

        let title_results = table.search("title:rust").unwrap();
        assert_eq!(title_results.len(), 1);
        assert_eq!(title_results[0].0, 1);

        let body_results = table.search("body:rust").unwrap();
        assert_eq!(body_results.len(), 1);
        assert_eq!(body_results[0].0, 2);
    }

    #[test]
    fn test_fts5_table_search_invalid_column_filter() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);
        table.insert_document(1, &["Rust title".to_owned(), "plain body".to_owned()]);

        let error = table
            .search("summary:rust")
            .expect_err("unknown column filters should fail");
        assert_eq!(
            error,
            Fts5QueryError::InvalidColumnFilter("summary".to_owned())
        );
    }

    #[test]
    fn test_fts5_table_search_column_filter_set_and_negative() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec![
            "title".to_owned(),
            "body".to_owned(),
            "tag".to_owned(),
        ]);
        table.insert_document(
            1,
            &[
                "rust title".to_owned(),
                "plain body".to_owned(),
                "meta tag".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "plain title".to_owned(),
                "rust body".to_owned(),
                "meta tag".to_owned(),
            ],
        );
        table.insert_document(
            3,
            &[
                "plain title".to_owned(),
                "plain body".to_owned(),
                "rust tag".to_owned(),
            ],
        );

        let mut braced_matches: Vec<i64> = table
            .search("{title body}:rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        braced_matches.sort_unstable();
        assert_eq!(braced_matches, vec![1, 2]);

        let mut negative_matches: Vec<i64> = table
            .search("- tag : rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        negative_matches.sort_unstable();
        assert_eq!(negative_matches, vec![1, 2]);

        let mut complement_matches: Vec<i64> = table
            .search("- {title body}: rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        complement_matches.sort_unstable();
        assert_eq!(complement_matches, vec![3]);
        Ok(())
    }

    #[test]
    fn test_fts5_table_search_invalid_column_filter_set() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);
        table.insert_document(1, &["Rust title".to_owned(), "plain body".to_owned()]);

        let Err(error) = table.search("{title summary}:rust") else {
            return Err("unknown column inside a set should fail".to_owned());
        };
        assert_eq!(
            error,
            Fts5QueryError::InvalidColumnFilter("summary".to_owned())
        );
        Ok(())
    }

    #[test]
    fn test_fts5_table_search_phrase_concatenation() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["one two three".to_owned()]);
        table.insert_document(2, &["one gap two three".to_owned()]);

        let matches = table
            .search(r#""one two" + three"#)
            .map_err(|err| err.to_string())?;
        assert_eq!(
            matches
                .into_iter()
                .map(|(rowid, _score)| rowid)
                .collect::<Vec<_>>(),
            vec![1]
        );
        Ok(())
    }

    #[test]
    fn test_fts5_table_search_phrase_concatenation_final_prefix() -> std::result::Result<(), String>
    {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["one two three".to_owned()]);
        table.insert_document(2, &["one two throne".to_owned()]);
        table.insert_document(3, &["one two four".to_owned()]);

        let mut matches = table
            .search("one + two + thr*")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect::<Vec<_>>();
        matches.sort_unstable();
        assert_eq!(matches, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn test_fts5_query_tight_phrase_concatenation() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query("one+two").map_err(|err| err.to_string())?;
        assert_eq!(
            tokens.iter().map(|token| token.kind).collect::<Vec<_>>(),
            vec![
                Fts5QueryTokenKind::Term,
                Fts5QueryTokenKind::Plus,
                Fts5QueryTokenKind::Term,
            ]
        );
        assert!(matches!(
            build_expr(&tokens).map_err(|err| err.to_string())?,
            Fts5Expr::Phrase(_)
        ));

        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["one two".to_owned()]);
        table.insert_document(2, &["one gap two".to_owned()]);

        assert_eq!(search_rowids(&table, "one+two")?, vec![1]);
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_tight_phrase_concatenation() -> std::result::Result<(), String>
    {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);
        table.insert_document(1, &["one two".to_owned(), "plain body".to_owned()]);
        table.insert_document(2, &["one gap two".to_owned(), "one two".to_owned()]);
        table.insert_document(3, &["plain title".to_owned(), "one gap two".to_owned()]);

        let query = "one+two";
        let tokens = parse_fts5_query(query)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|token| (token.kind, token.lexeme))
            .collect::<Vec<_>>();
        let expr = build_expr(&parse_fts5_query(query).map_err(|err| err.to_string())?)
            .map_err(|err| err.to_string())?;
        let mut adjacent_matches = search_rowids(&table, query)?;
        adjacent_matches.sort_unstable();
        let mut separated_matches = search_rowids(&table, r#""one gap two""#)?;
        separated_matches.sort_unstable();
        let mut column_filtered_matches = search_rowids(&table, "title:one+two")?;
        column_filtered_matches.sort_unstable();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5TightPhraseConcatStructure {
                    tokens,
                    expr,
                    adjacent_matches,
                    separated_matches,
                    column_filtered_matches,
                }
            ),
            r#"Fts5TightPhraseConcatStructure {
    tokens: [
        (
            Term,
            "one",
        ),
        (
            Plus,
            "+",
        ),
        (
            Term,
            "two",
        ),
    ],
    expr: Phrase(
        [
            "one",
            "two",
        ],
    ),
    adjacent_matches: [
        1,
        2,
    ],
    separated_matches: [
        2,
        3,
    ],
    column_filtered_matches: [
        1,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_phrase_concatenation() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["one two three".to_owned()]);
        table.insert_document(2, &["one gap two three".to_owned()]);
        table.insert_document(3, &["one two throne".to_owned()]);

        let exact_query = r#""one two" + three"#;
        let prefix_query = "one + two + thr*";
        let tokens = parse_fts5_query(exact_query)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|token| (token.kind, token.lexeme))
            .collect();
        let exact_expr = build_expr(&parse_fts5_query(exact_query).map_err(|err| err.to_string())?)
            .map_err(|err| err.to_string())?;
        let prefix_expr =
            build_expr(&parse_fts5_query(prefix_query).map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())?;
        let exact_matches = table
            .search(exact_query)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let mut prefix_matches = table
            .search(prefix_query)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect::<Vec<_>>();
        prefix_matches.sort_unstable();
        let Err(malformed_error) =
            build_expr(&parse_fts5_query("one +").map_err(|err| err.to_string())?)
        else {
            return Err("dangling phrase concatenation should fail".to_owned());
        };
        let malformed_error = malformed_error.to_string();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5PhraseConcatStructure {
                    tokens,
                    exact_expr,
                    prefix_expr,
                    exact_matches,
                    prefix_matches,
                    malformed_error,
                }
            ),
            r#"Fts5PhraseConcatStructure {
    tokens: [
        (
            Phrase,
            "one two",
        ),
        (
            Plus,
            "+",
        ),
        (
            Term,
            "three",
        ),
    ],
    exact_expr: Phrase(
        [
            "one",
            "two",
            "three",
        ],
    ),
    prefix_expr: PhrasePrefix(
        [
            "one",
            "two",
        ],
        "thr",
    ),
    exact_matches: [
        1,
    ],
    prefix_matches: [
        1,
        3,
    ],
    malformed_error: "invalid phrase syntax",
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_column_filter_sets() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec![
            "title".to_owned(),
            "body".to_owned(),
            "tag".to_owned(),
        ]);
        table.insert_document(
            1,
            &[
                "rust title".to_owned(),
                "plain body".to_owned(),
                "meta tag".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "plain title".to_owned(),
                "rust body".to_owned(),
                "meta tag".to_owned(),
            ],
        );
        table.insert_document(
            3,
            &[
                "plain title".to_owned(),
                "plain body".to_owned(),
                "rust tag".to_owned(),
            ],
        );

        let tokens = parse_fts5_query("- {title body}: rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|token| (token.kind, token.lexeme))
            .collect();
        let mut braced_matches: Vec<i64> = table
            .search("{title body}:rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        braced_matches.sort_unstable();
        let mut negative_matches: Vec<i64> = table
            .search("- tag : rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        negative_matches.sort_unstable();
        let mut complement_matches: Vec<i64> = table
            .search("- {title body}: rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        complement_matches.sort_unstable();
        let Err(invalid_error) = table.search("{title missing}:rust") else {
            return Err("unknown column inside a set should fail".to_owned());
        };
        let invalid_error = invalid_error.to_string();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5ColumnFilterSetStructure {
                    tokens,
                    braced_matches,
                    negative_matches,
                    complement_matches,
                    invalid_error,
                }
            ),
            r#"Fts5ColumnFilterSetStructure {
    tokens: [
        (
            ColumnFilter,
            "-{title,body}",
        ),
        (
            Term,
            "rust",
        ),
    ],
    braced_matches: [
        1,
        2,
    ],
    negative_matches: [
        1,
        2,
    ],
    complement_matches: [
        3,
    ],
    invalid_error: "invalid column filter: missing",
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_table_detail_column_rejects_offset_queries() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "detail=column"],
        )
        .unwrap();
        table.insert_document(1, &["rust title".to_owned(), "rust body".to_owned()]);

        let term_results = table.search("title:rust").unwrap();
        assert_eq!(term_results.len(), 1);
        assert_eq!(term_results[0].0, 1);

        assert_eq!(
            table.search(r#""rust title""#).unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::Column,
                feature: "phrase queries",
            }
        );
        assert_eq!(
            table.search("NEAR(rust title, 5)").unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::Column,
                feature: "NEAR queries",
            }
        );
        assert_eq!(
            table.search("^rust").unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::Column,
                feature: "initial-token queries",
            }
        );
    }

    #[test]
    fn test_fts5_table_detail_none_rejects_column_filters() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "body", "detail=none"],
        )
        .unwrap();
        table.insert_document(1, &["rust title".to_owned(), "plain body".to_owned()]);
        table.insert_document(2, &["plain title".to_owned(), "rust body".to_owned()]);

        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            table.search("title:rust").unwrap_err(),
            Fts5QueryError::UnsupportedByDetailMode {
                detail: DetailMode::None,
                feature: "column filters",
            }
        );
    }

    #[test]
    fn test_fts5_table_bm25_ranking_order() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["rust rust rust is great".to_owned()]);
        table.insert_document(2, &["rust is a programming language".to_owned()]);

        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 2);
        // Doc 1 has more occurrences of "rust", so it should rank higher
        // (lower negative score).
        assert_eq!(results[0].0, 1);
    }

    // -- Virtual table trait tests --

    #[test]
    fn test_fts5_vtab_connect() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body"]).unwrap();
        assert_eq!(vtab.columns(), &["title", "body"]);
    }

    #[test]
    fn test_fts5_vtab_default_column() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5"]).unwrap();
        assert_eq!(vtab.columns(), &["content"]);
    }

    #[test]
    fn test_fts5_vtab_connect_applies_options() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "body UNINDEXED",
                "tokenize='porter'",
                "content=''",
                "contentless_unindexed=1",
                "columnsize=0",
                "detail='column'",
                "prefix='2 3'",
                "insttoken=1",
                "locale=1",
                "tokendata=1",
            ],
        )
        .unwrap();
        assert_eq!(vtab.columns(), &["title", "body"]);
        assert_eq!(vtab.tokenizer_name, "porter");
        assert_eq!(vtab.config.content_mode(), ContentMode::Contentless);
        assert!(vtab.config.contentless_unindexed_enabled());
        assert!(!vtab.config.columnsize_enabled());
        assert_eq!(vtab.config.detail_mode(), DetailMode::Column);
        assert!(vtab.config.insttoken_enabled());
        assert!(vtab.config.locale_enabled());
        assert!(vtab.config.tokendata_enabled());
        assert_eq!(vtab.indexed_columns(), &[true, false]);
        assert_eq!(vtab.prefix_lengths, vec![2, 3]);
        assert!(!vtab.index().tracks_column_sizes());
        assert_eq!(vtab.index().detail_mode(), DetailMode::Column);
        assert!(vtab.index().tracks_prefix_length(2));
        assert!(vtab.index().tracks_prefix_length(3));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_reserved_column_names() {
        let cx = Cx::new();
        for (column, expected) in [
            ("rowid", "column name 'rowid' is reserved"),
            ("Rank", "column name 'rank' is reserved"),
        ] {
            let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", column])
                .expect_err("reserved column name should fail");
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?}, got {err}"
            );
        }
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_column_matching_table_name() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "Docs"])
            .expect_err("column matching table name should fail");
        assert!(
            err.to_string()
                .contains("column name 'Docs' conflicts with table name")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_duplicate_column_names() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "\"Title\""])
            .expect_err("duplicate column name should fail");
        assert!(
            err.to_string()
                .contains("fts5: duplicate column name 'Title'")
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_schema_column_validation() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "mail",
                "sender",
                "\"subject\" UNINDEXED",
                "body COLLATE nocase",
            ],
        )
        .map_err(|err| err.to_string())?;

        let structure = table_structure(&table);
        assert_eq!(
            format!("{structure:#?}"),
            r#"Fts5TableStructure {
    columns: [
        Fts5ColumnStructure {
            name: "sender",
            indexed: true,
        },
        Fts5ColumnStructure {
            name: "subject",
            indexed: false,
        },
        Fts5ColumnStructure {
            name: "body",
            indexed: true,
        },
    ],
    rows: [],
    terms: [],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_vtab_connect_accepts_valid_contentless_delete() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "content=''",
                "contentless_delete=1",
            ],
        )
        .unwrap();

        assert_eq!(vtab.config.content_mode(), ContentMode::Contentless);
        assert!(vtab.config.contentless_delete_enabled());
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_unknown_option() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "mystery=1"])
            .expect_err("unsupported option should fail");
        assert!(err.to_string().contains("unsupported option"));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_tokenizer_spec() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "tokenize='trigram case_sensitive 1 remove_diacritics 1'",
            ],
        )
        .expect_err("invalid tokenizer should fail");
        assert!(
            err.to_string()
                .contains("unsupported tokenizer specification")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_columnsize() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "columnsize=2"])
            .expect_err("invalid columnsize should fail");
        assert!(err.to_string().contains("columnsize must be 0 or 1"));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_contentless_unindexed() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "content=''",
                "contentless_unindexed=true",
            ],
        )
        .expect_err("invalid contentless_unindexed should fail");
        assert!(
            err.to_string()
                .contains("contentless_unindexed must be 0 or 1")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_contentless_delete() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "content=''",
                "contentless_delete=maybe",
            ],
        )
        .expect_err("invalid contentless_delete should fail");
        assert!(
            err.to_string()
                .contains("contentless_delete must be 0 or 1")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_secure_delete() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "secure-delete=maybe"],
        )
        .expect_err("invalid secure-delete should fail");
        assert!(
            err.to_string()
                .contains("secure_delete must be a boolean value")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_contentless_delete_on_stored_table() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "contentless_delete=1"],
        )
        .expect_err("contentless_delete should require contentless mode");
        assert!(
            err.to_string()
                .contains("contentless_delete=1 requires a contentless table")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_contentless_delete_columnsize_zero() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "content=''",
                "contentless_delete=1",
                "columnsize=0",
            ],
        )
        .expect_err("contentless_delete should reject columnsize=0");
        assert!(
            err.to_string()
                .contains("contentless_delete=1 is incompatible with columnsize=0")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_contentless_unindexed_on_stored_table() {
        let cx = Cx::new();
        let err = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "contentless_unindexed=1"],
        )
        .expect_err("contentless_unindexed should require contentless mode");
        assert!(
            err.to_string()
                .contains("contentless_unindexed=1 requires a contentless table")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_accumulates_prefix_options() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "prefix='3 2'",
                "prefix='4 3'",
            ],
        )
        .unwrap();

        assert_eq!(vtab.prefix_lengths, vec![2, 3, 4]);
        assert!(vtab.index().tracks_prefix_length(2));
        assert!(vtab.index().tracks_prefix_length(3));
        assert!(vtab.index().tracks_prefix_length(4));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_prefix_option() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "prefix='2 0'"])
            .expect_err("invalid prefix should fail");
        assert!(
            err.to_string()
                .contains("prefix must be a whitespace separated list of positive integers")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_detail() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "detail='offsets'"])
            .expect_err("invalid detail should fail");
        assert!(
            err.to_string()
                .contains("detail must be full, column, or none")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_insttoken() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "insttoken=maybe"])
            .expect_err("invalid insttoken should fail");
        assert!(
            err.to_string()
                .contains("insttoken must be a boolean value")
        );
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_locale() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "locale=true"])
            .expect_err("invalid locale should fail");
        assert!(err.to_string().contains("locale must be 0 or 1"));
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_invalid_tokendata() {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "tokendata=true"])
            .expect_err("invalid tokendata should fail");
        assert!(err.to_string().contains("tokendata must be 0 or 1"));
    }

    #[test]
    fn test_fts5_vtab_unindexed_column_is_stored_but_not_searched()
    -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut vtab =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "uuid UNINDEXED"])
                .map_err(|err| err.to_string())?;
        vtab.insert_document(1, &["rust guide".to_owned(), "uuidonly".to_owned()]);

        assert_eq!(vtab.indexed_columns(), &[true, false]);
        let stored = vtab
            .get_document(1)
            .ok_or_else(|| "row should be stored".to_owned())?;
        assert_eq!(stored.first().map(String::as_str), Some("rust guide"));
        assert_eq!(stored.get(1).map(String::as_str), Some("uuidonly"));

        let title_results = vtab.search("rust").map_err(|err| err.to_string())?;
        assert_eq!(title_results.len(), 1);
        assert_eq!(title_results.first().map(|(rowid, _)| *rowid), Some(1));
        assert!(
            vtab.search("uuidonly")
                .map_err(|err| err.to_string())?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn test_fts5_contentless_hides_all_column_values() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "uuid UNINDEXED",
                "content=''",
            ],
        )
        .map_err(|err| err.to_string())?;
        vtab.insert_document(1, &["rust guide".to_owned(), "uuidonly".to_owned()]);

        assert_eq!(
            vtab.get_document(1)
                .ok_or_else(|| "row should be stored".to_owned())?,
            &["".to_owned(), "".to_owned()]
        );
        assert_eq!(
            vtab.search("rust")
                .map_err(|err| err.to_string())?
                .first()
                .map(|(rowid, _score)| *rowid),
            Some(1)
        );
        assert!(
            vtab.search("uuidonly")
                .map_err(|err| err.to_string())?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn test_fts5_contentless_unindexed_keeps_only_unindexed_values()
    -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "uuid UNINDEXED",
                "content=''",
                "contentless_unindexed=1",
            ],
        )
        .map_err(|err| err.to_string())?;
        vtab.insert_document(1, &["rust guide".to_owned(), "uuidonly".to_owned()]);

        assert_eq!(
            vtab.get_document(1)
                .ok_or_else(|| "row should be stored".to_owned())?,
            &["".to_owned(), "uuidonly".to_owned()]
        );
        assert_eq!(
            vtab.search("rust")
                .map_err(|err| err.to_string())?
                .first()
                .map(|(rowid, _score)| *rowid),
            Some(1)
        );
        assert!(
            vtab.search("uuidonly")
                .map_err(|err| err.to_string())?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn test_fts5_contentless_update_rejects_without_toggle() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "content=''"])
            .map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(9),
                    SqliteValue::Text(SmallText::from_string("stable token")),
                ],
            )
            .map_err(|err| err.to_string())?;

        let err = table
            .update(
                &cx,
                &[
                    SqliteValue::Integer(9),
                    SqliteValue::Integer(9),
                    SqliteValue::Text(SmallText::from_string("replacement token")),
                ],
            )
            .map_err(|err| err.to_string())
            .expect_err("contentless update should require contentless_delete=1");

        assert!(err.contains("cannot update contentless table without contentless_delete=1"));
        assert_eq!(search_rowids(&table, "stable")?, vec![9]);
        assert!(search_rowids(&table, "replacement")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_contentless_unindexed() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "title",
                "uuid UNINDEXED",
                "content=''",
                "contentless_unindexed=1",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(7, &["rust guide".to_owned(), "uuidonly".to_owned()]);

        let indexed_matches = table
            .search("rust")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let unindexed_matches = table
            .search("uuidonly")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5ContentlessUnindexedStructure {
                    config: *table.config(),
                    indexed_columns: table.indexed_columns().to_vec(),
                    rows: table.all_rows(),
                    indexed_matches,
                    unindexed_matches,
                }
            ),
            r#"Fts5ContentlessUnindexedStructure {
    config: Fts5Config {
        secure_delete: false,
        content_mode: Contentless,
        contentless_delete: false,
        contentless_unindexed: true,
        columnsize: true,
        detail: Full,
        insttoken: false,
        locale: false,
        tokendata: false,
    },
    indexed_columns: [
        true,
        false,
    ],
    rows: [
        (
            7,
            [
                "",
                "uuidonly",
            ],
        ),
    ],
    indexed_matches: [
        7,
    ],
    unindexed_matches: [],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_contentless_update_mode() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut reject_table =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "content=''"])
                .map_err(|err| err.to_string())?;
        reject_table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(3),
                    SqliteValue::Text(SmallText::from_string("old token")),
                ],
            )
            .map_err(|err| err.to_string())?;
        let reject_error = reject_table
            .update(
                &cx,
                &[
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(3),
                    SqliteValue::Text(SmallText::from_string("new token")),
                ],
            )
            .map_err(|err| err.to_string())
            .expect_err("contentless update should fail without contentless_delete=1");

        let mut update_table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "content=''",
                "contentless_delete=1",
            ],
        )
        .map_err(|err| err.to_string())?;
        update_table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(4),
                    SqliteValue::Text(SmallText::from_string("old token")),
                ],
            )
            .map_err(|err| err.to_string())?;
        update_table
            .update(
                &cx,
                &[
                    SqliteValue::Integer(4),
                    SqliteValue::Integer(4),
                    SqliteValue::Text(SmallText::from_string("new token")),
                ],
            )
            .map_err(|err| err.to_string())?;

        assert_eq!(
            format!(
                "{:#?}",
                Fts5ContentlessUpdateStructure {
                    reject_error,
                    rows_after_reject: reject_table.all_rows(),
                    matches_after_reject: search_rowids(&reject_table, "old OR new")?,
                    updated_rows: update_table.all_rows(),
                    updated_old_matches: search_rowids(&update_table, "old")?,
                    updated_new_matches: search_rowids(&update_table, "new")?,
                }
            ),
            r#"Fts5ContentlessUpdateStructure {
    reject_error: "fts5: cannot update contentless table without contentless_delete=1",
    rows_after_reject: [
        (
            3,
            [
                "",
            ],
        ),
    ],
    matches_after_reject: [
        3,
    ],
    updated_rows: [
        (
            4,
            [
                "",
            ],
        ),
    ],
    updated_old_matches: [],
    updated_new_matches: [
        4,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_vtab_unindexed_column_filter_returns_no_matches() -> std::result::Result<(), String>
    {
        let cx = Cx::new();
        let mut vtab =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "title", "body UNINDEXED"])
                .map_err(|err| err.to_string())?;
        vtab.insert_document(1, &["plain title".to_owned(), "rust body".to_owned()]);

        assert!(
            vtab.search("body:rust")
                .map_err(|err| err.to_string())?
                .is_empty()
        );
        let title_results = vtab.search("title:plain").map_err(|err| err.to_string())?;
        assert_eq!(title_results.first().map(|(rowid, _)| *rowid), Some(1));
        Ok(())
    }

    #[test]
    fn test_fts5_vtab_connect_rejects_unknown_column_option() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let err = Fts5Table::connect(&cx, &["fts5", "main", "docs", "title INDEXED"])
            .err()
            .ok_or_else(|| "unsupported column option should fail".to_owned())?;
        assert!(err.to_string().contains("unsupported column option"));
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_unindexed_columns() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &["fts5", "main", "docs", "title", "uuid UNINDEXED", "body"],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(
            1,
            &[
                "Rust guide".to_owned(),
                "uuidonly".to_owned(),
                "systems language".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "Search guide".to_owned(),
                "secretmarker".to_owned(),
                "query language".to_owned(),
            ],
        );

        assert_eq!(
            format!("{:#?}", table_structure(&table)),
            r#"Fts5TableStructure {
    columns: [
        Fts5ColumnStructure {
            name: "title",
            indexed: true,
        },
        Fts5ColumnStructure {
            name: "uuid",
            indexed: false,
        },
        Fts5ColumnStructure {
            name: "body",
            indexed: true,
        },
    ],
    rows: [
        (
            1,
            [
                "Rust guide",
                "uuidonly",
                "systems language",
            ],
        ),
        (
            2,
            [
                "Search guide",
                "secretmarker",
                "query language",
            ],
        ),
    ],
    terms: [
        Fts5TermStructure {
            term: "guide",
            postings: [
                Fts5PostingStructure {
                    docid: 1,
                    column: 0,
                    positions: [
                        1,
                    ],
                },
                Fts5PostingStructure {
                    docid: 2,
                    column: 0,
                    positions: [
                        1,
                    ],
                },
            ],
        },
        Fts5TermStructure {
            term: "language",
            postings: [
                Fts5PostingStructure {
                    docid: 1,
                    column: 2,
                    positions: [
                        1,
                    ],
                },
                Fts5PostingStructure {
                    docid: 2,
                    column: 2,
                    positions: [
                        1,
                    ],
                },
            ],
        },
        Fts5TermStructure {
            term: "query",
            postings: [
                Fts5PostingStructure {
                    docid: 2,
                    column: 2,
                    positions: [
                        0,
                    ],
                },
            ],
        },
        Fts5TermStructure {
            term: "rust",
            postings: [
                Fts5PostingStructure {
                    docid: 1,
                    column: 0,
                    positions: [
                        0,
                    ],
                },
            ],
        },
        Fts5TermStructure {
            term: "search",
            postings: [
                Fts5PostingStructure {
                    docid: 2,
                    column: 0,
                    positions: [
                        0,
                    ],
                },
            ],
        },
        Fts5TermStructure {
            term: "systems",
            postings: [
                Fts5PostingStructure {
                    docid: 1,
                    column: 2,
                    positions: [
                        0,
                    ],
                },
            ],
        },
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_insttoken() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "insttoken=1"])
            .map_err(|err| err.to_string())?;
        table.insert_document(7, &["prefix present precise".to_owned()]);

        let mut terms: Vec<String> = table.index.index.keys().map(ToString::to_string).collect();
        terms.sort();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5InsttokenStructure {
                    config: *table.config(),
                    columns: table.columns().to_vec(),
                    rows: table.all_rows(),
                    terms,
                }
            ),
            r#"Fts5InsttokenStructure {
    config: Fts5Config {
        secure_delete: false,
        content_mode: Stored,
        contentless_delete: false,
        contentless_unindexed: false,
        columnsize: true,
        detail: Full,
        insttoken: true,
        locale: false,
        tokendata: false,
    },
    columns: [
        "body",
    ],
    rows: [
        (
            7,
            [
                "prefix present precise",
            ],
        ),
    ],
    terms: [
        "precise",
        "prefix",
        "present",
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_locale() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "locale=1"])
            .map_err(|err| err.to_string())?;
        table.insert_document(11, &["cafe creme".to_owned()]);

        let mut terms: Vec<String> = table.index.index.keys().map(ToString::to_string).collect();
        terms.sort();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5LocaleStructure {
                    config: *table.config(),
                    columns: table.columns().to_vec(),
                    rows: table.all_rows(),
                    terms,
                }
            ),
            r#"Fts5LocaleStructure {
    config: Fts5Config {
        secure_delete: false,
        content_mode: Stored,
        contentless_delete: false,
        contentless_unindexed: false,
        columnsize: true,
        detail: Full,
        insttoken: false,
        locale: true,
        tokendata: false,
    },
    columns: [
        "body",
    ],
    rows: [
        (
            11,
            [
                "cafe creme",
            ],
        ),
    ],
    terms: [
        "cafe",
        "creme",
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_locale_blob_storage() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "locale=1"])
            .map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(29),
                    SqliteValue::Blob(encode_fts5_locale_blob("tr_TR", "Istanbul").into()),
                ],
            )
            .map_err(|err| err.to_string())?;

        let matches = table
            .search("istanbul")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let actual = format!(
            "{:#?}",
            Fts5LocaleBlobStorageStructure {
                config: *table.config(),
                indexed_columns: table.indexed_columns().to_vec(),
                rows: table.all_rows(),
                locales: table.all_locales(),
                matches,
            }
        );
        let expected = r#"Fts5LocaleBlobStorageStructure {
    config: Fts5Config {
        secure_delete: false,
        content_mode: Stored,
        contentless_delete: false,
        contentless_unindexed: false,
        columnsize: true,
        detail: Full,
        insttoken: false,
        locale: true,
        tokendata: false,
    },
    indexed_columns: [
        true,
    ],
    rows: [
        (
            29,
            [
                "Istanbul",
            ],
        ),
    ],
    locales: [
        (
            29,
            0,
            "tr_TR",
        ),
    ],
    matches: [
        29,
    ],
}"#;
        assert!(actual.as_bytes().eq(expected.as_bytes()));
        Ok(())
    }

    #[test]
    fn test_fts5_vtab_update_rejects_locale_blob_without_locale_option() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"]).unwrap();

        let err = table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(41),
                    SqliteValue::Blob(encode_fts5_locale_blob("en_US", "hello").into()),
                ],
            )
            .expect_err("fts5_locale blob must require locale=1");

        assert!(err.to_string().contains("fts5_locale() requires locale=1"));
        assert!(table.get_document(41).is_none());
        assert!(table.search("hello").unwrap().is_empty());
    }

    #[test]
    fn test_fts5_locale_value_returns_text_or_null() {
        let cx = Cx::new();
        let mut table =
            Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "locale=1"]).unwrap();
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(5),
                    SqliteValue::Blob(encode_fts5_locale_blob("en_US", "hello").into()),
                ],
            )
            .unwrap();

        assert_eq!(
            table.locale_value(5, 0),
            SqliteValue::Text(SmallText::from_string("en_US"))
        );
        assert_eq!(table.locale_value(5, 1), SqliteValue::Null);

        let mut no_locale_table = Fts5Table::with_columns(vec!["body".to_owned()]);
        no_locale_table.insert_document(5, &["hello".to_owned()]);
        assert_eq!(no_locale_table.locale_value(5, 0), SqliteValue::Null);
    }

    #[test]
    fn test_fts5_structural_snapshot_locale_unindexed_discard() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "external_id UNINDEXED",
                "locale=1",
            ],
        )
        .map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(43),
                    SqliteValue::Blob(encode_fts5_locale_blob("en_US", "localized body").into()),
                    SqliteValue::Blob(encode_fts5_locale_blob("fr_FR", "secret marker").into()),
                ],
            )
            .map_err(|err| err.to_string())?;

        let indexed_matches = table
            .search("localized")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let unindexed_matches = table
            .search("secret")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let actual = format!(
            "{:#?}",
            Fts5LocaleUnindexedDiscardStructure {
                indexed_columns: table.indexed_columns().to_vec(),
                rows: table.all_rows(),
                locales: table.all_locales(),
                indexed_matches,
                unindexed_matches,
            }
        );
        let expected = r#"Fts5LocaleUnindexedDiscardStructure {
    indexed_columns: [
        true,
        false,
    ],
    rows: [
        (
            43,
            [
                "localized body",
                "secret marker",
            ],
        ),
    ],
    locales: [
        (
            43,
            0,
            "en_US",
        ),
    ],
    indexed_matches: [
        43,
    ],
    unindexed_matches: [],
}"#;
        assert!(actual.as_bytes().eq(expected.as_bytes()));
        assert_eq!(table.locale_value(43, 1), SqliteValue::Null);
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_tokendata() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "tokendata=1"])
            .map_err(|err| err.to_string())?;
        table.insert_document_owned_with_tokenizer(
            17,
            vec!["alpha beta".to_owned()],
            &TokendataTestTokenizer,
        );

        let mut terms: Vec<String> = table.index.index.keys().map(ToString::to_string).collect();
        terms.sort();
        let matches = table
            .search("alpha")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5TokendataStructure {
                    config: *table.config(),
                    terms,
                    rows: table.all_rows(),
                    matches,
                }
            ),
            r#"Fts5TokendataStructure {
    config: Fts5Config {
        secure_delete: false,
        content_mode: Stored,
        contentless_delete: false,
        contentless_unindexed: false,
        columnsize: true,
        detail: Full,
        insttoken: false,
        locale: false,
        tokendata: true,
    },
    terms: [
        "alpha",
        "beta",
    ],
    rows: [
        (
            17,
            [
                "alpha beta",
            ],
        ),
    ],
    matches: [
        17,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_trigram_case_sensitive() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "tri",
                "body",
                "tokenize='trigram case_sensitive 1'",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["ABC".to_owned()]);
        table.insert_document(2, &["abc".to_owned()]);

        let mut terms: Vec<String> = table.index.index.keys().map(ToString::to_string).collect();
        terms.sort();
        let upper_matches = table
            .search("ABC")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let lower_matches = table
            .search("abc")
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();

        let actual = format!(
            "{:#?}",
            Fts5TrigramCaseSensitiveStructure {
                tokenizer: table.tokenizer_name.clone(),
                terms,
                rows: table.all_rows(),
                upper_matches,
                lower_matches,
            }
        );
        let expected = r#"Fts5TrigramCaseSensitiveStructure {
    tokenizer: "trigram case_sensitive 1",
    terms: [
        "ABC",
        "abc",
    ],
    rows: [
        (
            1,
            [
                "ABC",
            ],
        ),
        (
            2,
            [
                "abc",
            ],
        ),
    ],
    upper_matches: [
        1,
    ],
    lower_matches: [
        2,
    ],
}"#;
        assert!(actual.as_bytes().eq(expected.as_bytes()));
        Ok(())
    }

    #[test]
    fn test_fts5_vtab_update_insert() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        let result = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text(SmallText::from_string("hello world")),
                ],
            )
            .unwrap();
        assert_eq!(result, Some(1));
        assert!(vtab.get_document(1).is_some());
    }

    #[test]
    fn test_fts5_vtab_metadata_declares_owned_shadow_tables() {
        let metadata = Fts5Table::module_metadata(&["fts5", "main", "docs", "body"]);
        assert!(metadata.owns_shadow_tables);
        assert_eq!(
            metadata.lifecycle,
            VtabLifecyclePolicy::SeparateCreateAndConnect
        );
        assert_eq!(metadata.integrity, VtabIntegrityPolicy::ShadowAware);

        for shadow_name in [
            "docs_data",
            "docs_idx",
            "docs_config",
            "docs_content",
            "docs_docsize",
        ] {
            let policy = Fts5Table::shadow_table_policy("docs", shadow_name);
            assert!(policy.is_shadow(), "{shadow_name} should be FTS5-owned");
            assert!(
                !policy.allows_direct_dml(),
                "{shadow_name} should reject user-authored DML"
            );
            assert!(
                !policy.allows_schema_ddl(),
                "{shadow_name} should reject user-authored DDL/trigger changes"
            );
            assert!(
                policy.allows_module_internal_write(),
                "{shadow_name} should allow FTS5-owned internal writes"
            );
        }

        let ordinary_policy = Fts5Table::shadow_table_policy("docs", "docs_segments");
        assert!(!ordinary_policy.is_shadow());
        assert!(ordinary_policy.allows_direct_dml());
        assert!(ordinary_policy.allows_schema_ddl());
        assert!(!Fts5Table::shadow_table_policy("docs", "other_data").is_shadow());
    }

    #[test]
    fn test_fts5_vtab_update_decodes_locale_blob() {
        let cx = Cx::new();
        let mut vtab =
            Fts5Table::connect(&cx, &["fts5", "main", "t", "content", "locale=1"]).unwrap();

        let result = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(7),
                    SqliteValue::Blob(encode_fts5_locale_blob("tr_TR", "Istanbul").into()),
                ],
            )
            .unwrap();

        assert_eq!(result, Some(7));
        assert_eq!(
            vtab.get_document(7)
                .and_then(|columns| columns.first())
                .map(String::as_str),
            Some("Istanbul")
        );
        assert!(matches!(vtab.get_locale(7, 0), Some("tr_TR")));
        assert_eq!(
            vtab.search("istanbul")
                .unwrap()
                .first()
                .map(|(rowid, _score)| *rowid),
            Some(7)
        );
    }

    #[test]
    fn test_fts5_vtab_update_delete() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("hello")),
            ],
        )
        .unwrap();

        vtab.update(&cx, &[SqliteValue::Integer(1)]).unwrap();
        assert!(vtab.get_document(1).is_none());
    }

    // -- Highlight/Snippet tests --

    #[test]
    fn test_highlight_basic() {
        let result = highlight(
            "the quick brown fox",
            &["quick".to_owned(), "fox".to_owned()],
            "<b>",
            "</b>",
        );
        assert_eq!(result, "the <b>quick</b> brown <b>fox</b>");
    }

    #[test]
    fn test_highlight_scalar_func_matches_prefix_query_tokens() {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("pre prefix prevent post")),
                SqliteValue::Text(SmallText::from_string("pre*")),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string(
                "<b>pre</b> <b>prefix</b> <b>prevent</b> post"
            ))
        );
    }

    #[test]
    fn test_highlight_scalar_func_preserves_phrase_span() -> std::result::Result<(), String> {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("alpha beta gamma")),
                SqliteValue::Text(SmallText::from_string(r#""alpha beta""#)),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .map_err(|err| err.to_string())?;

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("<b>alpha beta</b> gamma"))
        );
        Ok(())
    }

    #[test]
    fn test_snippet_scalar_func_matches_phrase_prefix_query() {
        let func = Fts5SnippetFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("alpha one two threefold omega")),
                SqliteValue::Text(SmallText::from_string("one + two + thr*")),
                SqliteValue::Text(SmallText::from_string("[")),
                SqliteValue::Text(SmallText::from_string("]")),
                SqliteValue::Text(SmallText::from_string("...")),
                SqliteValue::Integer(4),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("alpha [one two threefold]..."))
        );
    }

    #[test]
    fn test_snippet_scalar_func_selects_densest_query_window() -> std::result::Result<(), String> {
        let func = Fts5SnippetFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("alpha one gap gap two three omega")),
                SqliteValue::Text(SmallText::from_string("one OR two OR three")),
                SqliteValue::Text(SmallText::from_string("[")),
                SqliteValue::Text(SmallText::from_string("]")),
                SqliteValue::Text(SmallText::from_string("...")),
                SqliteValue::Integer(3),
            ])
            .map_err(|err| err.to_string())?;

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("...gap [two] [three]..."))
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_highlight_prefix_terms() {
        assert_eq!(
            format!(
                "{:#?}",
                Fts5HighlightPrefixStructure {
                    parsed_terms: highlight_terms_from_query_text("one + two + thr*"),
                    prefix_highlight: highlight_with_terms(
                        "pre prefix prevent post",
                        &highlight_terms_from_query_text("pre*"),
                        "<b>",
                        "</b>",
                    ),
                    phrase_prefix_snippet: snippet_with_terms(
                        "alpha one two threefold omega",
                        &highlight_terms_from_query_text("one + two + thr*"),
                        "[",
                        "]",
                        "...",
                        4,
                    ),
                    fallback_prefix_highlight: highlight_with_terms(
                        "prelude prefix other",
                        &highlight_terms_from_query_text("(pre*"),
                        "<i>",
                        "</i>",
                    ),
                    exact_highlight: highlight(
                        "prefix prevent pre",
                        &["pre".to_owned()],
                        "<b>",
                        "</b>",
                    ),
                }
            ),
            r#"Fts5HighlightPrefixStructure {
    parsed_terms: [
        Fts5HighlightTerm {
            term: "one",
            prefix: false,
        },
        Fts5HighlightTerm {
            term: "two",
            prefix: false,
        },
        Fts5HighlightTerm {
            term: "thr",
            prefix: true,
        },
    ],
    prefix_highlight: "<b>pre</b> <b>prefix</b> <b>prevent</b> post",
    phrase_prefix_snippet: "alpha [one] [two] [threefold]...",
    fallback_prefix_highlight: "<i>prelude</i> <i>prefix</i> other",
    exact_highlight: "prefix prevent <b>pre</b>",
}"#
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_phrase_highlight_spans() {
        assert_eq!(
            format!(
                "{:#?}",
                Fts5PhraseSpanStructure {
                    phrase_patterns: highlight_patterns_from_query_text(r#""alpha beta" OR gam*"#),
                    rendered_phrase: highlight_with_patterns(
                        "alpha beta gamma",
                        &highlight_patterns_from_query_text(r#""alpha beta""#),
                        "<b>",
                        "</b>",
                    ),
                    rendered_prefix_snippet: snippet_with_patterns(
                        "alpha one two threefold omega",
                        &highlight_patterns_from_query_text("one + two + thr*"),
                        "[",
                        "]",
                        "...",
                        4,
                    ),
                    negated_rhs_rendering: highlight_with_patterns(
                        "alpha beta gamma",
                        &highlight_patterns_from_query_text(r#""alpha beta" NOT gamma"#),
                        "<b>",
                        "</b>",
                    ),
                }
            ),
            r#"Fts5PhraseSpanStructure {
    phrase_patterns: [
        Fts5HighlightPattern {
            parts: [
                Fts5HighlightTerm {
                    term: "alpha",
                    prefix: false,
                },
                Fts5HighlightTerm {
                    term: "beta",
                    prefix: false,
                },
            ],
        },
        Fts5HighlightPattern {
            parts: [
                Fts5HighlightTerm {
                    term: "gam",
                    prefix: true,
                },
            ],
        },
    ],
    rendered_phrase: "<b>alpha beta</b> gamma",
    rendered_prefix_snippet: "alpha [one two threefold]...",
    negated_rhs_rendering: "<b>alpha beta</b> gamma",
}"#
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_snippet_window_scoring() {
        let text = "alpha one gap gap two three omega";
        let patterns = highlight_patterns_from_query_text("one OR two OR three");
        let tokenizer = Unicode61Tokenizer::new();
        let tokens = tokenizer.tokenize(text);

        assert_eq!(
            format!(
                "{:#?}",
                Fts5SnippetWindowStructure {
                    scored_window: select_snippet_window(&tokens, &patterns, 3),
                    rendered_snippet: snippet_with_patterns(text, &patterns, "[", "]", "...", 3,),
                    no_match_window: select_snippet_window(
                        &tokens,
                        &highlight_patterns_from_query_text("absent"),
                        3,
                    ),
                }
            ),
            r#"Fts5SnippetWindowStructure {
    scored_window: Fts5SnippetWindow {
        first: 3,
        last_exclusive: 6,
        distinct: 2,
        total: 2,
    },
    rendered_snippet: "...gap [two] [three]...",
    no_match_window: Fts5SnippetWindow {
        first: 0,
        last_exclusive: 3,
        distinct: 0,
        total: 0,
    },
}"#
        );
    }

    #[test]
    fn test_fts5_vtab_rollback_restores_inserted_rows() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"])
            .map_err(|err| err.to_string())?;

        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text(SmallText::from_string("stable root")),
                ],
            )
            .map_err(|err| err.to_string())?;
        table.begin(&cx).map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(2),
                    SqliteValue::Text(SmallText::from_string("transient branch")),
                ],
            )
            .map_err(|err| err.to_string())?;

        assert_eq!(search_rowids(&table, "transient")?, vec![2]);
        table.rollback(&cx).map_err(|err| err.to_string())?;

        assert_eq!(table.all_rows(), vec![(1, vec!["stable root".to_owned()])]);
        assert!(search_rowids(&table, "transient")?.is_empty());
        assert_eq!(search_rowids(&table, "stable")?, vec![1]);
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_transaction_rollback() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body", "locale=1"])
            .map_err(|err| err.to_string())?;

        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Blob(encode_fts5_locale_blob("tr_TR", "stable root").into()),
                ],
            )
            .map_err(|err| err.to_string())?;
        table.begin(&cx).map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(2),
                    SqliteValue::Text(SmallText::from_string("transient branch")),
                ],
            )
            .map_err(|err| err.to_string())?;
        table.savepoint(&cx, 1).map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(3),
                    SqliteValue::Text(SmallText::from_string("deep branch")),
                ],
            )
            .map_err(|err| err.to_string())?;
        table.rollback_to(&cx, 1).map_err(|err| err.to_string())?;
        let savepoint_rows = table.all_rows();
        let savepoint_matches = search_rowids(&table, "transient OR deep")?;

        table.rollback(&cx).map_err(|err| err.to_string())?;
        let full_rows = table.all_rows();
        let full_matches = search_rowids(&table, "stable OR transient OR deep")?;
        let full_locales = table.all_locales();
        let reused_auto_rowid = table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text(SmallText::from_string("auto after rollback")),
                ],
            )
            .map_err(|err| err.to_string())?;

        assert_eq!(
            format!(
                "{:#?}",
                Fts5TransactionRollbackStructure {
                    savepoint_rows,
                    savepoint_matches,
                    full_rows,
                    full_matches,
                    full_locales,
                    reused_auto_rowid,
                    rows_after_auto: table.all_rows(),
                }
            ),
            r#"Fts5TransactionRollbackStructure {
    savepoint_rows: [
        (
            1,
            [
                "stable root",
            ],
        ),
        (
            2,
            [
                "transient branch",
            ],
        ),
    ],
    savepoint_matches: [
        2,
    ],
    full_rows: [
        (
            1,
            [
                "stable root",
            ],
        ),
    ],
    full_matches: [
        1,
    ],
    full_locales: [
        (
            1,
            0,
            "tr_TR",
        ),
    ],
    reused_auto_rowid: Some(
        2,
    ),
    rows_after_auto: [
        (
            1,
            [
                "stable root",
            ],
        ),
        (
            2,
            [
                "auto after rollback",
            ],
        ),
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_update_decode_failure_preserves_existing_row() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"])
            .map_err(|err| err.to_string())?;

        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text(SmallText::from_string("stable old token")),
                ],
            )
            .map_err(|err| err.to_string())?;
        let err = table
            .update(
                &cx,
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(1),
                    SqliteValue::Blob(encode_fts5_locale_blob("tr_TR", "replacement token").into()),
                ],
            )
            .expect_err("locale blob update should fail when locale=0");

        assert!(
            err.to_string().contains("fts5_locale() requires locale=1"),
            "unexpected update failure: {err}"
        );
        assert_eq!(
            table.all_rows(),
            vec![(1, vec!["stable old token".to_owned()])]
        );
        assert_eq!(search_rowids(&table, "stable")?, vec![1]);
        assert!(search_rowids(&table, "replacement")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_update_failure_cleanup() -> std::result::Result<(), String> {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(&cx, &["fts5", "main", "docs", "body"])
            .map_err(|err| err.to_string())?;

        table.begin(&cx).map_err(|err| err.to_string())?;
        table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text(SmallText::from_string("stable old token")),
                ],
            )
            .map_err(|err| err.to_string())?;
        table.savepoint(&cx, 1).map_err(|err| err.to_string())?;
        let error = table
            .update(
                &cx,
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(1),
                    SqliteValue::Blob(encode_fts5_locale_blob("tr_TR", "replacement token").into()),
                ],
            )
            .expect_err("decode failure must not mutate the old row")
            .to_string();
        let rows_after_error = table.all_rows();
        let old_matches = search_rowids(&table, "stable")?;
        let replacement_matches = search_rowids(&table, "replacement")?;
        let locales_after_error = table.all_locales();
        table.release(&cx, 1).map_err(|err| err.to_string())?;
        table.commit(&cx).map_err(|err| err.to_string())?;

        assert_eq!(
            format!(
                "{:#?}",
                Fts5UpdateFailureCleanupStructure {
                    error,
                    rows_after_error,
                    old_matches,
                    replacement_matches,
                    locales_after_error,
                    committed_rows: table.all_rows(),
                }
            ),
            r#"Fts5UpdateFailureCleanupStructure {
    error: "fts5_locale() requires locale=1",
    rows_after_error: [
        (
            1,
            [
                "stable old token",
            ],
        ),
    ],
    old_matches: [
        1,
    ],
    replacement_matches: [],
    locales_after_error: [],
    committed_rows: [
        (
            1,
            [
                "stable old token",
            ],
        ),
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_not_terms_are_not_auxiliary_match_terms() -> std::result::Result<(), String> {
        let expr = build_expr(&parse_fts5_query("rust NOT web").map_err(|err| err.to_string())?)
            .map_err(|err| err.to_string())?;

        assert_eq!(extract_query_terms(&expr), vec!["rust"]);
        assert_eq!(
            extract_highlight_terms(&expr),
            vec![Fts5HighlightTerm::exact("rust")]
        );
        Ok(())
    }

    #[test]
    fn test_highlight_scalar_func_ignores_not_rhs_terms() {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("rust web sqlite")),
                SqliteValue::Text(SmallText::from_string("rust NOT web")),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("<b>rust</b> web sqlite"))
        );
    }

    #[test]
    fn test_fts5_structural_snapshot_not_auxiliary_terms() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["rust systems".to_owned()]);
        table.insert_document(2, &["rust web".to_owned()]);

        assert_eq!(
            format!(
                "{:#?}",
                Fts5NotTermStructure {
                    query_terms: table
                        .query_terms_for_queries(&["rust NOT web"])
                        .map_err(|err| err.to_string())?,
                    highlight_terms: highlight_terms_from_query_text("rust NOT web"),
                    search_matches: search_rowids(&table, "rust NOT web")?,
                    positive_highlight: highlight_with_terms(
                        "rust web sqlite",
                        &highlight_terms_from_query_text("rust NOT web"),
                        "<b>",
                        "</b>",
                    ),
                    fallback_highlight_terms: highlight_terms_from_query_text("(rust NOT web"),
                }
            ),
            r#"Fts5NotTermStructure {
    query_terms: [
        "rust",
    ],
    highlight_terms: [
        Fts5HighlightTerm {
            term: "rust",
            prefix: false,
        },
    ],
    search_matches: [
        1,
    ],
    positive_highlight: "<b>rust</b> web sqlite",
    fallback_highlight_terms: [
        Fts5HighlightTerm {
            term: "rust",
            prefix: false,
        },
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_snippet_with_ellipsis() {
        let text = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
        let result = snippet(text, &["delta".to_owned()], "<b>", "</b>", "...", 5);
        assert!(result.contains("<b>delta</b>"));
        assert!(result.contains("..."));
    }

    // -- Scalar function tests --

    #[test]
    fn test_fts5_source_id_func() {
        let func = Fts5SourceIdFunc;
        let result = func.invoke(&[]).unwrap();
        if let SqliteValue::Text(s) = result {
            assert!(s.contains("FTS5"));
        } else {
            panic!("expected text result");
        }
    }

    #[test]
    fn test_register_fts5_scalars() {
        let mut registry = fsqlite_func::FunctionRegistry::new();
        register_fts5_scalars(&mut registry);
        assert!(registry.find_scalar("highlight", 4).is_some());
        assert!(registry.find_scalar("snippet", 6).is_some());
        assert!(registry.find_scalar("fts5_source_id", 0).is_some());
        assert!(registry.find_scalar("fts5_insttoken", 1).is_some());
        assert!(registry.find_scalar("fts5_locale", 2).is_some());
    }

    #[test]
    fn test_fts5_insttoken_func_passthrough() {
        let func = Fts5InsttokenFunc;
        assert_eq!(func.num_args(), 1);
        assert_eq!(func.name(), "fts5_insttoken");

        let query = SqliteValue::Text(SmallText::from_string("pre*"));
        assert_eq!(func.invoke(&[query.clone()]).unwrap(), query);
        assert_eq!(
            func.invoke(&[SqliteValue::Null]).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_fts5_locale_func_encodes_locale_blob() {
        let func = Fts5LocaleFunc;
        assert_eq!(func.num_args(), 2);
        assert_eq!(func.name(), "fts5_locale");

        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("tr_TR")),
                SqliteValue::Text(SmallText::from_string("Istanbul")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Blob(encode_fts5_locale_blob("tr_TR", "Istanbul").into())
        );
    }

    #[test]
    fn test_fts5_locale_blob_decode_round_trip() {
        let blob = encode_fts5_locale_blob("ja_JP", "Tokyo");
        assert!(matches!(
            decode_fts5_locale_blob(&blob),
            Some(("ja_JP", "Tokyo"))
        ));
        assert!(decode_fts5_locale_blob(b"plain text").is_none());
        assert!(decode_fts5_locale_blob(&encode_fts5_locale_blob("", "text")).is_none());
    }

    #[test]
    fn test_fts5_locale_func_empty_locale_returns_text() {
        let func = Fts5LocaleFunc;

        assert_eq!(
            func.invoke(&[
                SqliteValue::Text(SmallText::from_string("")),
                SqliteValue::Integer(42),
            ])
            .unwrap(),
            SqliteValue::Text(SmallText::from_string("42"))
        );
        assert_eq!(
            func.invoke(&[
                SqliteValue::Text(SmallText::from_string("")),
                SqliteValue::Null,
            ])
            .unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_highlight_scalar_func_uses_query_text() {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("the quick brown fox")),
                SqliteValue::Text(SmallText::from_string("quick OR fox")),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("the <b>quick</b> brown <b>fox</b>"))
        );
    }

    #[test]
    fn test_highlight_scalar_func_falls_back_for_invalid_query() {
        let func = Fts5HighlightFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("hello world")),
                SqliteValue::Text(SmallText::from_string("(hello")),
                SqliteValue::Text(SmallText::from_string("<b>")),
                SqliteValue::Text(SmallText::from_string("</b>")),
            ])
            .unwrap();

        assert_eq!(
            result,
            SqliteValue::Text(SmallText::from_string("<b>hello</b> world"))
        );
    }

    #[test]
    fn test_snippet_scalar_func_uses_query_text() {
        let func = Fts5SnippetFunc;
        let result = func
            .invoke(&[
                SqliteValue::Text(SmallText::from_string("alpha beta gamma delta epsilon")),
                SqliteValue::Text(SmallText::from_string("delta")),
                SqliteValue::Text(SmallText::from_string("[")),
                SqliteValue::Text(SmallText::from_string("]")),
                SqliteValue::Text(SmallText::from_string("...")),
                SqliteValue::Integer(3),
            ])
            .unwrap();

        let SqliteValue::Text(snippet_text) = result else {
            panic!("snippet() should return text");
        };
        assert!(snippet_text.contains("[delta]"));
    }

    // -- Full query pipeline test --

    #[test]
    fn test_fts5_full_query_pipeline() {
        let mut table = Fts5Table::with_columns(vec!["title".to_owned(), "body".to_owned()]);

        table.insert_document(
            1,
            &[
                "Introduction to Rust".to_owned(),
                "Rust is a systems programming language focused on safety".to_owned(),
            ],
        );
        table.insert_document(
            2,
            &[
                "Python for Data Science".to_owned(),
                "Python is widely used in data science and machine learning".to_owned(),
            ],
        );
        table.insert_document(
            3,
            &[
                "Rust Web Development".to_owned(),
                "Building web applications with Rust and Actix".to_owned(),
            ],
        );

        // Test implicit AND
        let results = table.search("rust safety").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);

        // Test OR
        let results = table.search("safety OR data").unwrap();
        assert_eq!(results.len(), 2);

        // Test NOT
        let results = table.search("rust NOT web").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);

        // Test phrase
        let results = table.search(r#""data science""#).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);

        // Test prefix
        let results = table.search("prog*").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    // -- Near query test --

    #[test]
    fn test_fts5_near_query() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // "hello" at pos 0, "world" at pos 1 -> within distance 5
        index.add_document(1, 0, &tok.tokenize("hello world foo bar"));
        // "hello" at pos 0, "world" at pos 4 -> within distance 5
        index.add_document(2, 0, &tok.tokenize("hello a b c world"));
        // "hello" at pos 0, "world" at pos 7 -> NOT within distance 5
        index.add_document(3, 0, &tok.tokenize("hello a b c d e f world"));

        let expr = Fts5Expr::Near(vec![near_term("hello"), near_term("world")], 5);
        let docs = evaluate_expr(&index, &expr);
        assert!(docs.contains(&1));
        assert!(docs.contains(&2));
        assert!(!docs.contains(&3));
    }

    #[test]
    fn test_fts5_near_distance_counts_intervening_terms() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello gap world".to_owned()]);
        table.insert_document(3, &["hello gap gap world".to_owned()]);

        assert_eq!(search_rowids(&table, "NEAR(hello world, 0)")?, vec![1]);
        assert_eq!(search_rowids(&table, "NEAR(hello world, 1)")?, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn test_fts5_near_distance_counts_phrase_boundaries() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["alpha beta gamma delta".to_owned()]);
        table.insert_document(2, &["alpha beta gap gamma delta".to_owned()]);
        table.insert_document(3, &["alpha beta gap gap gamma delta".to_owned()]);

        let adjacent = r#"NEAR("alpha beta" "gamma delta", 0)"#;
        let one_gap = r#"NEAR("alpha beta" "gamma delta", 1)"#;
        assert_eq!(search_rowids(&table, adjacent)?, vec![1]);
        assert_eq!(search_rowids(&table, one_gap)?, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn test_fts5_near_phrase_operands() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["hello world near rust language".to_owned()]);
        table.insert_document(2, &["hello world gap gap rust language".to_owned()]);

        let matches = table
            .search(r#"NEAR("hello world" "rust language", 2)"#)
            .map_err(|err| err.to_string())?;
        assert_eq!(
            matches
                .into_iter()
                .map(|(rowid, _score)| rowid)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        Ok(())
    }

    #[test]
    fn test_fts5_near_prefix_operand() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["hello world near rustacean".to_owned()]);
        table.insert_document(2, &["hello world gap gap rustacean".to_owned()]);

        let matches = table
            .search(r#"NEAR("hello world" rusta* , 2)"#)
            .map_err(|err| err.to_string())?;
        assert_eq!(
            matches
                .into_iter()
                .map(|(rowid, _score)| rowid)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_near_phrase_operands() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["hello world near rust language".to_owned()]);
        table.insert_document(2, &["hello world gap gap rust language".to_owned()]);
        table.insert_document(3, &["hello world near rustacean".to_owned()]);

        let phrase_query = r#"NEAR("hello world" "rust language", 2)"#;
        let expr = build_expr(&parse_fts5_query(phrase_query).map_err(|err| err.to_string())?)
            .map_err(|err| err.to_string())?;
        let (operands, distance) = match expr {
            Fts5Expr::Near(operands, distance) => (operands, distance),
            other => return Err(format!("expected NEAR expression, got {other:?}")),
        };
        let phrase_matches = table
            .search(phrase_query)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let prefix_matches = table
            .search(r#"NEAR("hello world" rusta* , 2)"#)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(rowid, _score)| rowid)
            .collect();
        let query_terms = table
            .query_terms_for_queries(&[phrase_query])
            .map_err(|err| err.to_string())?;

        assert_eq!(
            format!(
                "{:#?}",
                Fts5NearPhraseStructure {
                    operands,
                    distance,
                    phrase_matches,
                    prefix_matches,
                    query_terms,
                }
            ),
            r#"Fts5NearPhraseStructure {
    operands: [
        Phrase(
            [
                "hello",
                "world",
            ],
        ),
        Phrase(
            [
                "rust",
                "language",
            ],
        ),
    ],
    distance: 2,
    phrase_matches: [
        1,
        2,
    ],
    prefix_matches: [
        3,
    ],
    query_terms: [
        "hello",
        "world",
        "rust",
        "language",
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_near_distance_clumps() -> std::result::Result<(), String> {
        let mut table = Fts5Table::with_columns(vec!["body".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello gap world".to_owned()]);
        table.insert_document(3, &["hello gap gap world".to_owned()]);
        table.insert_document(4, &["alpha beta gamma delta".to_owned()]);
        table.insert_document(5, &["alpha beta gap gamma delta".to_owned()]);
        table.insert_document(6, &["a b c d x x x e f x".to_owned()]);

        assert_eq!(
            format!(
                "{:#?}",
                Fts5NearDistanceStructure {
                    adjacent_terms: search_rowids(&table, "NEAR(hello world, 0)")?,
                    one_gap_terms: search_rowids(&table, "NEAR(hello world, 1)")?,
                    adjacent_phrases: search_rowids(
                        &table,
                        r#"NEAR("alpha beta" "gamma delta", 0)"#,
                    )?,
                    one_gap_phrases: search_rowids(
                        &table,
                        r#"NEAR("alpha beta" "gamma delta", 1)"#,
                    )?,
                    reordered_doc_example: search_rowids(&table, "NEAR(e d, 3)")?,
                    multi_phrase_doc_example: search_rowids(
                        &table,
                        r#"NEAR("a b c d" "b c" "e f", 4)"#,
                    )?,
                    too_tight_doc_example: search_rowids(
                        &table,
                        r#"NEAR("a b c d" "b c" "e f", 3)"#,
                    )?,
                }
            ),
            r"Fts5NearDistanceStructure {
    adjacent_terms: [
        1,
    ],
    one_gap_terms: [
        1,
        2,
    ],
    adjacent_phrases: [
        4,
    ],
    one_gap_phrases: [
        4,
        5,
    ],
    reordered_doc_example: [
        6,
    ],
    multi_phrase_doc_example: [
        6,
    ],
    too_tight_doc_example: [],
}"
        );
        Ok(())
    }

    // -- Edge case tests --

    #[test]
    fn test_empty_query_error() {
        let err = parse_fts5_query("").unwrap_err();
        assert_eq!(err, Fts5QueryError::EmptyQuery);
    }

    #[test]
    fn test_whitespace_only_query_error() {
        let err = parse_fts5_query("   ").unwrap_err();
        assert_eq!(err, Fts5QueryError::EmptyQuery);
    }

    #[test]
    fn test_doc_length_tracking() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("one two three"));
        index.add_document(2, 0, &tok.tokenize("a"));

        assert_eq!(index.doc_length(1), 3);
        assert_eq!(index.doc_length(2), 1);
        assert!((index.avg_doc_length() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_doc_length_tracking_falls_back_when_columnsize_disabled() {
        let mut index = InvertedIndex::with_column_sizes(false);
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("one two"));
        index.add_document(1, 1, &tok.tokenize("three"));
        index.add_document(2, 0, &tok.tokenize("solo"));

        assert!(!index.tracks_column_sizes());
        assert_eq!(index.doc_length(1), 3);
        assert_eq!(index.doc_length(2), 1);
        assert!((index.avg_doc_length() - 2.0).abs() < f64::EPSILON);

        index.remove_document(1);
        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_length(1), 0);
        assert_eq!(index.doc_length(2), 1);
    }

    #[test]
    fn test_fts5_config_default() {
        let config = Fts5Config::default();
        assert_eq!(config.content_mode(), ContentMode::Stored);
        assert!(!config.secure_delete_enabled());
        assert!(!config.contentless_delete_enabled());
        assert!(!config.contentless_unindexed_enabled());
        assert!(config.columnsize_enabled());
        assert_eq!(config.detail_mode(), DetailMode::Full);
        assert!(!config.insttoken_enabled());
        assert!(!config.locale_enabled());
        assert!(!config.tokendata_enabled());
    }

    #[test]
    fn test_query_error_display() {
        assert_eq!(
            format!("{}", Fts5QueryError::EmptyQuery),
            "empty FTS5 query"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::UnaryNotForbidden),
            "FTS5 NOT is binary-only; unary NOT is not allowed"
        );
    }

    // -- Additional edge case tests --

    #[test]
    fn test_query_error_display_all_variants() {
        assert_eq!(
            format!("{}", Fts5QueryError::UnclosedPhrase),
            "unclosed phrase literal"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::UnbalancedParentheses),
            "unbalanced parentheses"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::InvalidColumnFilter("foo".to_owned())),
            "invalid column filter: foo"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::InvalidNearSyntax),
            "invalid NEAR syntax"
        );
        assert_eq!(
            format!("{}", Fts5QueryError::InvalidPhraseSyntax),
            "invalid phrase syntax"
        );
        assert_eq!(
            format!(
                "{}",
                Fts5QueryError::UnsupportedByDetailMode {
                    detail: DetailMode::Column,
                    feature: "phrase queries",
                }
            ),
            "detail=column does not support phrase queries"
        );
    }

    #[test]
    fn test_parse_bool_like_all_values() {
        assert_eq!(parse_bool_like("1"), Some(true));
        assert_eq!(parse_bool_like("on"), Some(true));
        assert_eq!(parse_bool_like("true"), Some(true));
        assert_eq!(parse_bool_like("TRUE"), Some(true));
        assert_eq!(parse_bool_like("  On  "), Some(true));
        assert_eq!(parse_bool_like("0"), Some(false));
        assert_eq!(parse_bool_like("off"), Some(false));
        assert_eq!(parse_bool_like("false"), Some(false));
        assert_eq!(parse_bool_like("FALSE"), Some(false));
        assert_eq!(parse_bool_like("maybe"), None);
        assert_eq!(parse_bool_like(""), None);
    }

    #[test]
    fn test_config_apply_no_equals() {
        let mut config = Fts5Config::default();
        assert!(!config.apply_control_command("noequals"));
    }

    #[test]
    fn test_unicode61_custom_separators() {
        let tok = Unicode61Tokenizer {
            separators: ".".to_owned(),
            token_chars: String::new(),
            remove_diacritics: 0,
        };
        let tokens = tok.tokenize("hello.world");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["hello", "world"]);
    }

    #[test]
    fn test_unicode61_custom_token_chars() {
        let tok = Unicode61Tokenizer {
            separators: String::new(),
            token_chars: "-".to_owned(),
            remove_diacritics: 0,
        };
        let tokens = tok.tokenize("well-known");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        // '-' is treated as a token character, so the whole thing is one token.
        assert_eq!(terms, vec!["well-known"]);
    }

    #[test]
    fn test_tokenizer_spec_preserves_unicode61_tokenchars() {
        let tok = create_tokenizer("unicode61 tokenchars '-_./:@#$%'").unwrap();
        let tokens = tok.tokenize("AuthController.ts my_function");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();

        assert_eq!(terms, vec!["authcontroller.ts", "my_function"]);
    }

    #[test]
    fn test_unicode61_empty_input() {
        let tok = Unicode61Tokenizer::new();
        assert!(tok.tokenize("").is_empty());
    }

    #[test]
    fn test_unicode61_only_separators() {
        let tok = Unicode61Tokenizer::new();
        assert!(tok.tokenize("   ...   ").is_empty());
    }

    #[test]
    fn test_ascii_tokenizer_keeps_non_ascii_token_chars() {
        let tok = AsciiTokenizer;
        let tokens = tok.tokenize("café hello");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["café", "hello"]);
    }

    #[test]
    fn test_ascii_tokenizer_empty() {
        let tok = AsciiTokenizer;
        assert!(tok.tokenize("").is_empty());
    }

    #[test]
    fn test_ascii_tokenizer_name() {
        let tok = AsciiTokenizer;
        assert_eq!(Fts5Tokenizer::name(&tok), "ascii");
    }

    #[test]
    fn test_porter_tokenizer_debug() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        let debug = format!("{tok:?}");
        assert!(debug.contains("PorterTokenizer"));
        assert!(debug.contains("unicode61"));
    }

    #[test]
    fn test_porter_tokenizer_name() {
        let tok = PorterTokenizer::new(Box::new(Unicode61Tokenizer::new()));
        assert_eq!(Fts5Tokenizer::name(&tok), "porter");
    }

    #[test]
    fn test_porter_stem_step1b_at_suffix() {
        // "conflated" -> strip "ed" -> "conflat" -> fixup: ends with "at" -> "conflate"
        assert_eq!(porter_stem("conflated"), "conflate");
    }

    #[test]
    fn test_porter_stem_step1b_bl_suffix() {
        assert_eq!(porter_stem("troubled"), "trouble");
    }

    #[test]
    fn test_porter_stem_step1b_iz_suffix() {
        assert_eq!(porter_stem("sized"), "size");
    }

    #[test]
    fn test_porter_stem_step1b_double_consonant() {
        // "hopping" -> strip "ing" -> "hopp" -> double consonant, not l/s/z -> "hop"
        assert_eq!(porter_stem("hopping"), "hop");
    }

    #[test]
    fn test_porter_stem_eed() {
        // "agreed" -> "eed" suffix -> base "agr" (len > 1) -> "agree"
        assert_eq!(porter_stem("agreed"), "agree");
    }

    #[test]
    fn test_porter_stem_terminal_y() {
        // "happy" -> step1c: terminal y with vowel in stem -> "happi"
        assert_eq!(porter_stem("happy"), "happi");
    }

    #[test]
    fn test_porter_stem_treats_y_as_vowel_after_consonant() {
        assert!(contains_vowel("cry"));
        assert!(contains_vowel("fly"));
        assert!(!contains_vowel("sk"));
        assert_eq!(porter_stem("crying"), "cry");
        assert_eq!(porter_stem("flying"), "fly");
        assert_eq!(porter_stem("sky"), "sky");
    }

    #[test]
    fn test_fts5_structural_snapshot_porter_y_vowel() -> std::result::Result<(), String> {
        let words = ["crying", "flying", "happy", "sky"];
        let stems = words
            .into_iter()
            .map(|word| (word.to_owned(), porter_stem(word)))
            .collect();
        let vowel_checks = ["cr", "cry", "fly", "sky"]
            .into_iter()
            .map(|word| (word.to_owned(), contains_vowel(word)))
            .collect();
        let measures = ["cr", "cry", "trouble", "relate"]
            .into_iter()
            .map(|word| (word.to_owned(), measure(word)))
            .collect();

        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "body",
                "tokenize='porter unicode61'",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["crying flying".to_owned()]);
        table.insert_document(2, &["sky".to_owned()]);

        assert_eq!(
            format!(
                "{:#?}",
                Fts5PorterYVowelStructure {
                    stems,
                    vowel_checks,
                    measures,
                    cry_matches: search_rowids(&table, "cry")?,
                    fly_matches: search_rowids(&table, "fly")?,
                    sky_matches: search_rowids(&table, "sky")?,
                }
            ),
            r#"Fts5PorterYVowelStructure {
    stems: [
        (
            "crying",
            "cry",
        ),
        (
            "flying",
            "fly",
        ),
        (
            "happy",
            "happi",
        ),
        (
            "sky",
            "sky",
        ),
    ],
    vowel_checks: [
        (
            "cr",
            false,
        ),
        (
            "cry",
            true,
        ),
        (
            "fly",
            true,
        ),
        (
            "sky",
            true,
        ),
    ],
    measures: [
        (
            "cr",
            0,
        ),
        (
            "cry",
            0,
        ),
        (
            "trouble",
            1,
        ),
        (
            "relate",
            2,
        ),
    ],
    cry_matches: [
        1,
    ],
    fly_matches: [
        1,
    ],
    sky_matches: [
        2,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_porter_stem_step2_ational() {
        assert_eq!(porter_stem("relational"), "relate");
    }

    #[test]
    fn test_porter_stem_step3_ful() {
        assert_eq!(porter_stem("hopeful"), "hope");
    }

    #[test]
    fn test_porter_stem_short_word() {
        assert_eq!(porter_stem("a"), "a");
        assert_eq!(porter_stem("an"), "an");
    }

    #[test]
    fn test_measure_function() {
        assert_eq!(measure(""), 0);
        assert_eq!(measure("a"), 0);
        assert_eq!(measure("ab"), 1);
        assert_eq!(measure("abc"), 1);
        assert_eq!(measure("abab"), 2);
    }

    #[test]
    fn test_contains_vowel_function() {
        assert!(contains_vowel("hello"));
        assert!(contains_vowel("a"));
        assert!(!contains_vowel("xzz"));
        assert!(!contains_vowel(""));
    }

    #[test]
    fn test_trigram_case_sensitive() {
        let tok = TrigramTokenizer {
            case_sensitive: true,
            remove_diacritics: false,
        };
        let tokens = tok.tokenize("ABC");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "ABC");
    }

    #[test]
    fn test_trigram_case_insensitive() {
        let tok = TrigramTokenizer {
            case_sensitive: false,
            remove_diacritics: false,
        };
        let tokens = tok.tokenize("ABC");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "abc");
    }

    #[test]
    fn test_create_tokenizer_trigram_case_sensitive_arg() {
        let tok = create_tokenizer("trigram case_sensitive 1").unwrap();
        let tokens = tok.tokenize("ABC");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "ABC");
    }

    #[test]
    fn test_create_tokenizer_trigram_remove_diacritics_arg() {
        let tok = create_tokenizer("trigram remove_diacritics 1").unwrap();
        let tokens = tok.tokenize("ábC");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "abc");
    }

    #[test]
    fn test_create_tokenizer_trigram_rejects_invalid_options() {
        assert!(create_tokenizer("trigram case_sensitive 1 remove_diacritics 1").is_none());
        assert!(create_tokenizer("trigram case_sensitive maybe").is_none());
    }

    #[test]
    fn test_trigram_unicode() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("café");
        assert!(!tokens.is_empty());
        // "café" has 4 chars, so we get 2 trigrams.
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn test_trigram_streaming_preserves_unicode_offsets() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("éABC");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].term, "éab");
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 4);
        assert_eq!(tokens[1].term, "abc");
        assert_eq!(tokens[1].start, 2);
        assert_eq!(tokens[1].end, 5);
    }

    #[test]
    fn test_trigram_case_fold_fast_path_preserves_unicode_lowercase() {
        let insensitive = TrigramTokenizer {
            case_sensitive: false,
            remove_diacritics: false,
        };
        let tokens = insensitive.tokenize("ABCΣ");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].term, "abc");
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 3);
        assert_eq!(tokens[1].term, "bcσ");
        assert_eq!(tokens[1].start, 1);
        assert_eq!(tokens[1].end, 5);

        let sensitive = TrigramTokenizer {
            case_sensitive: true,
            remove_diacritics: false,
        };
        let sensitive_terms: Vec<String> = sensitive
            .tokenize("ABCΣ")
            .into_iter()
            .map(|token| token.term)
            .collect();
        assert_eq!(sensitive_terms, vec!["ABC", "BCΣ"]);
    }

    #[test]
    fn test_fts5_structural_snapshot_trigram_case_fold() -> std::result::Result<(), String> {
        let insensitive = TrigramTokenizer {
            case_sensitive: false,
            remove_diacritics: false,
        };
        let insensitive_tokens = insensitive
            .tokenize("ABCΣ")
            .into_iter()
            .map(|token| (token.term, token.start, token.end))
            .collect();
        let sensitive = TrigramTokenizer {
            case_sensitive: true,
            remove_diacritics: false,
        };
        let sensitive_terms = sensitive
            .tokenize("ABCΣ")
            .into_iter()
            .map(|token| token.term)
            .collect();
        let diacritic_terms = TrigramTokenizer {
            case_sensitive: false,
            remove_diacritics: true,
        }
        .tokenize("ÉAB")
        .into_iter()
        .map(|token| token.term)
        .collect();

        let cx = Cx::new();
        let mut table =
            Fts5Table::connect(&cx, &["fts5", "main", "tri", "body", "tokenize='trigram'"])
                .map_err(|err| err.to_string())?;
        table.insert_document(1, &["ABCΣ".to_owned()]);
        table.insert_document(2, &["abcσ".to_owned()]);
        let mut upper_matches = search_rowids(&table, "ABCΣ")?;
        upper_matches.sort_unstable();
        let mut lower_matches = search_rowids(&table, "abcσ")?;
        lower_matches.sort_unstable();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5TrigramCaseFoldStructure {
                    insensitive_tokens,
                    sensitive_terms,
                    diacritic_terms,
                    upper_matches,
                    lower_matches,
                }
            ),
            r#"Fts5TrigramCaseFoldStructure {
    insensitive_tokens: [
        (
            "abc",
            0,
            3,
        ),
        (
            "bcσ",
            1,
            5,
        ),
    ],
    sensitive_terms: [
        "ABC",
        "BCΣ",
    ],
    diacritic_terms: [
        "eab",
    ],
    upper_matches: [
        1,
        2,
    ],
    lower_matches: [
        1,
        2,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_fts5_structural_snapshot_trigram_streaming() -> std::result::Result<(), String> {
        let tok = TrigramTokenizer::default();
        let tokens = tok
            .tokenize("éABC")
            .into_iter()
            .map(|token| (token.term, token.start, token.end))
            .collect();
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "tri",
                "body",
                "tokenize='trigram remove_diacritics 1'",
            ],
        )
        .map_err(|err| err.to_string())?;
        table.insert_document(1, &["éABC".to_owned()]);
        table.insert_document(2, &["zabc".to_owned()]);
        let mut accented_matches = search_rowids(&table, "eab")?;
        accented_matches.sort_unstable();
        let mut ascii_matches = search_rowids(&table, "abc")?;
        ascii_matches.sort_unstable();

        assert_eq!(
            format!(
                "{:#?}",
                Fts5TrigramStreamingStructure {
                    tokens,
                    short_input_tokens: tok.tokenize("éA").len(),
                    accented_matches,
                    ascii_matches,
                }
            ),
            r#"Fts5TrigramStreamingStructure {
    tokens: [
        (
            "éab",
            0,
            4,
        ),
        (
            "abc",
            2,
            5,
        ),
    ],
    short_input_tokens: 0,
    accented_matches: [
        1,
    ],
    ascii_matches: [
        1,
        2,
    ],
}"#
        );
        Ok(())
    }

    #[test]
    fn test_trigram_exact_3_chars() {
        let tok = TrigramTokenizer::default();
        let tokens = tok.tokenize("abc");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].term, "abc");
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 3);
    }

    #[test]
    fn test_trigram_tokenizer_name() {
        let tok = TrigramTokenizer::default();
        assert_eq!(Fts5Tokenizer::name(&tok), "trigram");
    }

    #[test]
    fn test_create_tokenizer_case_insensitive() {
        assert!(create_tokenizer("UNICODE61").is_some());
        assert!(create_tokenizer("ASCII").is_some());
        assert!(create_tokenizer("Porter").is_some());
        assert!(create_tokenizer("Trigram").is_some());
    }

    #[test]
    fn test_query_caret_token() {
        let tokens = parse_fts5_query("^hello").unwrap();
        assert_eq!(tokens[0].kind, Fts5QueryTokenKind::Caret);
        assert_eq!(tokens[1].kind, Fts5QueryTokenKind::Term);
    }

    #[test]
    fn test_query_nested_parens() {
        let tokens = parse_fts5_query("((hello))").unwrap();
        let kinds: Vec<Fts5QueryTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert_eq!(kinds[0], Fts5QueryTokenKind::LParen);
        assert_eq!(kinds[1], Fts5QueryTokenKind::LParen);
    }

    #[test]
    fn test_query_extra_close_paren() {
        let err = parse_fts5_query(")hello").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnbalancedParentheses);
    }

    #[test]
    fn test_query_empty_phrase_ignored() {
        // Empty phrase (just "") should be ignored and result in EmptyQuery.
        let err = parse_fts5_query(r#""""#).unwrap_err();
        assert_eq!(err, Fts5QueryError::EmptyQuery);
    }

    #[test]
    fn test_query_not_after_or_is_unary() {
        let err = parse_fts5_query("hello OR NOT world").unwrap_err();
        assert_eq!(err, Fts5QueryError::UnaryNotForbidden);
    }

    #[test]
    fn test_build_expr_term() {
        let tokens = parse_fts5_query("hello").unwrap();
        let expr = build_expr(&tokens).unwrap();
        assert!(matches!(expr, Fts5Expr::Term(_)));
    }

    #[test]
    fn test_build_expr_and_or_precedence() {
        // "a OR b c" should parse as "a OR (b AND c)" due to AND having higher precedence.
        let tokens = parse_fts5_query("a OR b c").unwrap();
        let expr = build_expr(&tokens).unwrap();
        assert!(matches!(expr, Fts5Expr::Or(_, _)));
    }

    #[test]
    fn test_build_expr_phrase_concatenation() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query(r#""one two" + three"#).map_err(|err| err.to_string())?;
        let expr = build_expr(&tokens).map_err(|err| err.to_string())?;
        match expr {
            Fts5Expr::Phrase(words) => {
                assert_eq!(
                    words,
                    vec!["one".to_owned(), "two".to_owned(), "three".to_owned()]
                );
            }
            other => return Err(format!("expected phrase expression, got {other:?}")),
        }
        Ok(())
    }

    #[test]
    fn test_build_expr_phrase_concatenation_final_prefix() -> std::result::Result<(), String> {
        let tokens = parse_fts5_query("one + two + thr*").map_err(|err| err.to_string())?;
        let expr = build_expr(&tokens).map_err(|err| err.to_string())?;
        match expr {
            Fts5Expr::PhrasePrefix(words, prefix) => {
                assert_eq!(words, vec!["one".to_owned(), "two".to_owned()]);
                assert_eq!(prefix, "thr");
            }
            other => return Err(format!("expected phrase-prefix expression, got {other:?}")),
        }
        Ok(())
    }

    #[test]
    fn test_build_expr_near_default_distance() {
        let tokens = parse_fts5_query("NEAR(hello world)").unwrap();
        let expr = build_expr(&tokens).unwrap();
        match expr {
            Fts5Expr::Near(operands, distance) => {
                assert_eq!(operands, vec![near_term("hello"), near_term("world")]);
                assert_eq!(distance, 10);
            }
            other => panic!("expected NEAR expression, got {other:?}"),
        }
    }

    #[test]
    fn test_build_expr_near_explicit_distance() {
        let tokens = parse_fts5_query("NEAR(hello world, 5)").unwrap();
        let expr = build_expr(&tokens).unwrap();
        match expr {
            Fts5Expr::Near(operands, distance) => {
                assert_eq!(operands, vec![near_term("hello"), near_term("world")]);
                assert_eq!(distance, 5);
            }
            other => panic!("expected NEAR expression, got {other:?}"),
        }
    }

    #[test]
    fn test_build_expr_near_inline_distance_token() {
        let tokens = parse_fts5_query("NEAR(hello world,5)").unwrap();
        let expr = build_expr(&tokens).unwrap();
        match expr {
            Fts5Expr::Near(operands, distance) => {
                assert_eq!(operands, vec![near_term("hello"), near_term("world")]);
                assert_eq!(distance, 5);
            }
            other => panic!("expected NEAR expression, got {other:?}"),
        }
    }

    #[test]
    fn test_build_expr_near_phrase_and_prefix_operands() -> std::result::Result<(), String> {
        let tokens =
            parse_fts5_query(r#"NEAR("hello world" rust*)"#).map_err(|err| err.to_string())?;
        let expr = build_expr(&tokens).map_err(|err| err.to_string())?;
        match expr {
            Fts5Expr::Near(operands, distance) => {
                assert_eq!(
                    operands,
                    vec![near_phrase(&["hello", "world"]), near_prefix("rust")]
                );
                assert_eq!(distance, 10);
            }
            other => return Err(format!("expected NEAR expression, got {other:?}")),
        }
        Ok(())
    }

    #[test]
    fn test_build_expr_near_rejects_boolean_operator() {
        let tokens = parse_fts5_query("NEAR(hello OR world)").unwrap();
        let err = build_expr(&tokens).unwrap_err();
        assert_eq!(err, Fts5QueryError::InvalidNearSyntax);
    }

    #[test]
    fn test_build_expr_near_invalid() {
        // NEAR without parens is invalid.
        let tokens = parse_fts5_query("NEAR hello").unwrap();
        let err = build_expr(&tokens);
        assert!(err.is_err());
    }

    #[test]
    fn test_inverted_index_empty() {
        let index = InvertedIndex::new();
        assert_eq!(index.total_docs(), 0);
        assert_eq!(index.doc_frequency("anything"), 0);
        assert_eq!(index.term_frequency("anything", 1), 0);
        assert!(index.avg_doc_length().abs() < f64::EPSILON);
        assert_eq!(index.doc_length(1), 0);
        assert!(index.get_postings("nothing").is_empty());
        assert!(index.get_prefix_postings("n").is_empty());
    }

    #[test]
    fn test_inverted_index_multi_column() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        index.add_document(1, 0, &tok.tokenize("title words"));
        index.add_document(1, 1, &tok.tokenize("body words here"));

        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_length(1), 5); // 2 + 3
        assert_eq!(index.doc_frequency("words"), 1); // same docid
    }

    #[test]
    fn test_inverted_index_remove_nonexistent() {
        let mut index = InvertedIndex::new();
        index.remove_document(999); // should not panic
        assert_eq!(index.total_docs(), 0);
    }

    #[test]
    fn test_evaluate_phrase_empty() {
        let index = InvertedIndex::new();
        let expr = Fts5Expr::Phrase(vec![]);
        let docs = evaluate_expr(&index, &expr);
        assert!(docs.is_empty());
    }

    #[test]
    fn test_initial_token_prefix() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // Doc 1: "hello world" (starts with 'hel')
        index.add_document(1, 0, &tok.tokenize("hello world"));
        // Doc 2: "world hello" (contains 'hel' but not at start)
        index.add_document(2, 0, &tok.tokenize("world hello"));

        let expr = build_expr(&parse_fts5_query("^ hel*").unwrap()).unwrap();
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_initial_token_phrase() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();

        // Doc 1: "hello world" (matches ^ "hello world")
        index.add_document(1, 0, &tok.tokenize("hello world"));
        // Doc 2: "say hello world" (contains phrase but not at start)
        index.add_document(2, 0, &tok.tokenize("say hello world"));

        let expr = build_expr(&parse_fts5_query("^ \"hello world\"").unwrap()).unwrap();
        let docs = evaluate_expr(&index, &expr);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_evaluate_near_single_term() {
        let index = InvertedIndex::new();
        let expr = Fts5Expr::Near(vec![near_term("only")], 5);
        let docs = evaluate_expr(&index, &expr);
        assert!(docs.is_empty());
    }

    #[test]
    fn test_evaluate_column_filter() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();
        let columns = vec!["title".to_owned(), "body".to_owned()];
        index.add_document(1, 0, &tok.tokenize("hello title"));
        index.add_document(1, 1, &tok.tokenize("plain body"));
        index.add_document(2, 0, &tok.tokenize("plain title"));
        index.add_document(2, 1, &tok.tokenize("hello body"));

        let expr = Fts5Expr::ColumnFilter(
            "title".to_owned(),
            Box::new(Fts5Expr::Term("hello".to_owned())),
        );
        let docs = evaluate_expr_for_columns(&index, &expr, &columns);
        assert_eq!(docs, vec![1]);
    }

    #[test]
    fn test_intersect_sorted_disjoint() {
        let result = intersect_sorted(&[1, 3, 5], &[2, 4, 6]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_intersect_sorted_empty() {
        assert!(intersect_sorted(&[], &[1, 2, 3]).is_empty());
        assert!(intersect_sorted(&[1, 2, 3], &[]).is_empty());
    }

    #[test]
    fn test_union_sorted_no_overlap() {
        let result = union_sorted(&[1, 3], &[2, 4]);
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_union_sorted_empty() {
        assert_eq!(union_sorted(&[], &[1, 2]), vec![1, 2]);
        assert_eq!(union_sorted(&[1, 2], &[]), vec![1, 2]);
    }

    #[test]
    fn test_difference_sorted_all_excluded() {
        let result = difference_sorted(&[1, 2, 3], &[1, 2, 3]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_difference_sorted_none_excluded() {
        let result = difference_sorted(&[1, 2, 3], &[4, 5]);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_bm25_no_matching_terms() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();
        index.add_document(1, 0, &tok.tokenize("hello world"));

        let score = bm25_score(&index, 1, &["nonexistent".to_owned()], &[1.0]);
        assert!(score.abs() < f64::EPSILON);
    }

    #[test]
    fn test_bm25_weighted_columns() {
        let mut index = InvertedIndex::new();
        let tok = Unicode61Tokenizer::new();
        index.add_document(1, 0, &tok.tokenize("rust"));
        index.add_document(1, 1, &tok.tokenize("other stuff"));

        let low_weight = bm25_score(&index, 1, &["rust".to_owned()], &[0.1, 1.0]);
        let high_weight = bm25_score(&index, 1, &["rust".to_owned()], &[10.0, 1.0]);

        // Higher column weight should produce a more negative (better) score.
        assert!(high_weight < low_weight);
    }

    #[test]
    fn test_fts5_table_get_document() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);

        let doc = table.get_document(1).unwrap();
        assert_eq!(doc, &["hello world"]);

        assert!(table.get_document(99).is_none());
    }

    #[test]
    fn test_fts5_table_search_no_results() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);

        let results = table.search("nonexistent").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_fts5_table_search_normalizes_query_with_porter_tokenizer() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "content",
                "tokenize='porter unicode61 remove_diacritics 2'",
            ],
        )
        .unwrap();
        table.insert_document(1, &["I am running the tests".to_owned()]);

        let results = table.search("\"running\"").unwrap();

        assert_eq!(
            results.iter().map(|(rowid, _)| *rowid).collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn test_fts5_table_search_normalizes_query_with_code_tokenchars() {
        let cx = Cx::new();
        let mut table = Fts5Table::connect(
            &cx,
            &[
                "fts5",
                "main",
                "docs",
                "content",
                "tokenize=\"unicode61 tokenchars '-_./:@#$%'\"",
            ],
        )
        .unwrap();
        table.insert_document(1, &["Call my_function in AuthController.ts".to_owned()]);

        let identifier_results = table.search("\"my_function\"").unwrap();
        let filename_results = table.search("\"AuthController.ts\"").unwrap();

        assert_eq!(
            identifier_results
                .iter()
                .map(|(rowid, _)| *rowid)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            filename_results
                .iter()
                .map(|(rowid, _)| *rowid)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn test_fts5_table_auto_rowid() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        // Insert without explicit rowid.
        let r1 = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text(SmallText::from_string("first")),
                ],
            )
            .unwrap();
        let r2 = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text(SmallText::from_string("second")),
                ],
            )
            .unwrap();

        assert_ne!(r1, r2);
    }

    #[test]
    fn test_fts5_vtab_update_modify() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("original")),
            ],
        )
        .unwrap();

        // Update: old_rowid=1, new_rowid=1, new_content="modified"
        vtab.update(
            &cx,
            &[
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("modified")),
            ],
        )
        .unwrap();

        let doc = vtab.get_document(1).unwrap();
        assert_eq!(doc, &["modified"]);
        assert!(vtab.search("original").unwrap().is_empty());
        assert_eq!(vtab.search("modified").unwrap()[0].0, 1);
    }

    #[test]
    fn test_fts5_vtab_insert_duplicate_rowid_preserves_original() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Integer(1),
                SqliteValue::Text(SmallText::from_string("alpha original")),
            ],
        )
        .unwrap();

        let err = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text(SmallText::from_string("beta duplicate")),
                ],
            )
            .unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));

        assert_eq!(vtab.get_document(1).unwrap(), &["alpha original"]);
        assert_eq!(vtab.search("alpha").unwrap()[0].0, 1);
        assert!(vtab.search("beta").unwrap().is_empty());
    }

    #[test]
    fn test_fts5_vtab_update_rowid_conflict_preserves_original_entries() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();

        for (rowid, text) in [(1, "alpha one"), (2, "beta two")] {
            vtab.update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Integer(rowid),
                    SqliteValue::Text(SmallText::from_string(text)),
                ],
            )
            .unwrap();
        }

        let err = vtab
            .update(
                &cx,
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(2),
                    SqliteValue::Text(SmallText::from_string("gamma conflict")),
                ],
            )
            .unwrap_err();
        assert!(matches!(err, FrankenError::PrimaryKeyViolation));

        assert_eq!(vtab.get_document(1).unwrap(), &["alpha one"]);
        assert_eq!(vtab.get_document(2).unwrap(), &["beta two"]);
        assert_eq!(vtab.search("alpha").unwrap()[0].0, 1);
        assert_eq!(vtab.search("beta").unwrap()[0].0, 2);
        assert!(vtab.search("gamma").unwrap().is_empty());
    }

    #[test]
    fn test_fts5_insert_document_replaces_existing_rowid_index_entries() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);

        table.insert_document(1, &["old token".to_owned()]);
        table.insert_document(1, &["new token".to_owned()]);

        assert_eq!(table.get_document(1).unwrap(), &["new token"]);
        assert!(table.search("old").unwrap().is_empty());
        assert_eq!(table.search("new").unwrap()[0].0, 1);
    }

    #[test]
    fn test_fts5_vtab_update_empty_args() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        assert!(vtab.update(&cx, &[]).is_err());
    }

    #[test]
    fn test_fts5_rebuild_documents_preserves_documents_and_next_rowid() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["stale".to_owned()]);

        table.rebuild_documents(vec![
            (3, vec!["hello world".to_owned()]),
            (7, vec!["second doc".to_owned()]),
        ]);

        let expected_doc_3 = vec!["hello world".to_owned()];
        let expected_doc_7 = vec!["second doc".to_owned()];
        assert_eq!(table.get_document(3), Some(expected_doc_3.as_slice()));
        assert_eq!(table.get_document(7), Some(expected_doc_7.as_slice()));
        assert!(table.get_document(1).is_none());

        let cx = Cx::new();
        let rowid = table
            .update(
                &cx,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Text(SmallText::from_string("third doc")),
                ],
            )
            .expect("insert after rebuild")
            .expect("rowid");
        assert_eq!(rowid, 8);
    }

    #[test]
    fn test_fts5_vtab_contentless_delete_rejected() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        *vtab.config_mut() = Fts5Config::new(ContentMode::Contentless);

        vtab.insert_document(1, &["data".to_owned()]);

        let result = vtab.update(&cx, &[SqliteValue::Integer(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_fts5_vtab_contentless_columnsize_zero_requires_explicit_rowid() {
        let cx = Cx::new();
        let mut vtab = Fts5Table::connect(
            &cx,
            &["fts5", "main", "t", "content", "content=''", "columnsize=0"],
        )
        .unwrap();

        let result = vtab.update(
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Text(SmallText::from_string("contentless row")),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("require an explicit rowid")
        );
    }

    #[test]
    fn test_fts5_cursor_set_results() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            detail: DetailMode::Full,
            index: InvertedIndex::new(),
            documents: HashMap::new(),
            shadow_rows: None,
        };

        assert!(cursor.eof());

        cursor.set_results(vec![
            (1, -1.5, vec!["hello".to_owned()]),
            (2, -0.5, vec!["world".to_owned()]),
        ]);

        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 1);

        let cx = Cx::new();
        cursor.next(&cx).unwrap();
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 2);

        cursor.next(&cx).unwrap();
        assert!(cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 0); // past end returns 0
    }

    #[test]
    fn test_fts5_cursor_column() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            detail: DetailMode::Full,
            index: InvertedIndex::new(),
            documents: HashMap::new(),
            shadow_rows: None,
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(
            ctx.take_value(),
            Some(SqliteValue::Text(SmallText::from_string("hello world")))
        );

        // Rank column (beyond column count).
        let mut ctx2 = ColumnContext::new();
        cursor.column(&mut ctx2, 1).unwrap();
        assert_eq!(ctx2.take_value(), Some(SqliteValue::Float(-1.0)));
    }

    #[test]
    fn test_fts5_cursor_column_past_end_returns_null() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            detail: DetailMode::Full,
            index: InvertedIndex::new(),
            documents: HashMap::new(),
            shadow_rows: None,
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let cx = Cx::new();
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_fts5_cursor_column_out_of_range_returns_null() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            detail: DetailMode::Full,
            index: InvertedIndex::new(),
            documents: HashMap::new(),
            shadow_rows: None,
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 2).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_fts5_cursor_column_negative_out_of_range_returns_null() {
        let mut cursor = Fts5Cursor {
            results: Vec::new(),
            position: 0,
            columns: vec!["content".to_owned()],
            tokenizer_name: "unicode61".to_owned(),
            detail: DetailMode::Full,
            index: InvertedIndex::new(),
            documents: HashMap::new(),
            shadow_rows: None,
        };

        cursor.set_results(vec![(1, -1.0, vec!["hello world".to_owned()])]);

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, -2).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
    }

    #[test]
    fn test_highlight_no_matches() {
        let result = highlight("hello world", &["nonexistent".to_owned()], "<b>", "</b>");
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_highlight_empty_text() {
        let result = highlight("", &["hello".to_owned()], "<b>", "</b>");
        assert_eq!(result, "");
    }

    #[test]
    fn test_snippet_no_matches() {
        let result = snippet(
            "hello world",
            &["nonexistent".to_owned()],
            "<b>",
            "</b>",
            "...",
            3,
        );
        // Should still return something from the start.
        assert!(!result.is_empty());
    }

    #[test]
    fn test_snippet_empty_text() {
        let result = snippet("", &["hello".to_owned()], "<b>", "</b>", "...", 5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_query_terms() {
        let expr = Fts5Expr::And(
            Box::new(Fts5Expr::Term("Hello".to_owned())),
            Box::new(Fts5Expr::Or(
                Box::new(Fts5Expr::Prefix("Wor".to_owned())),
                Box::new(Fts5Expr::Phrase(vec![
                    "exact".to_owned(),
                    "match".to_owned(),
                ])),
            )),
        );
        let terms = extract_query_terms(&expr);
        assert_eq!(terms, vec!["Hello", "Wor", "exact", "match"]);
    }

    #[test]
    fn test_fts5_source_id_func_num_args() {
        let func = Fts5SourceIdFunc;
        assert_eq!(func.num_args(), 0);
        assert_eq!(func.name(), "fts5_source_id");
    }

    #[test]
    fn test_fts5_config_content_mode_accessor() {
        let config = Fts5Config::new(ContentMode::Contentless);
        assert_eq!(config.content_mode(), ContentMode::Contentless);
    }

    #[test]
    fn test_fts5_table_config_accessors() {
        let mut table = Fts5Table::with_columns(vec!["c".to_owned()]);
        assert_eq!(table.config().content_mode(), ContentMode::Stored);
        assert_eq!(table.config().detail_mode(), DetailMode::Full);
        table.config_mut().apply_control_command("secure-delete=1");
        assert!(table.config().secure_delete_enabled());
    }

    #[test]
    fn test_fts5_token_colocated_field() {
        let token = Fts5Token {
            term: "test".to_owned(),
            start: 0,
            end: 4,
            colocated: true,
        };
        assert!(token.colocated);
    }

    // -----------------------------------------------------------------------
    // bd-6i2s required: FTS5 secure-delete table-level tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fts5_secure_delete_removes() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.config_mut().apply_control_command("secure-delete=1");

        table.insert_document(1, &["sensitive data here".to_owned()]);
        table.insert_document(2, &["public information".to_owned()]);

        // Verify document found before delete.
        let before = table.search("sensitive").unwrap();
        assert_eq!(before.len(), 1);

        // Delete with secure-delete enabled.
        table.delete_document(1);

        // After delete, "sensitive" should return no results.
        let after = table.search("sensitive").unwrap();
        assert!(
            after.is_empty(),
            "secure-deleted term should not be searchable"
        );
    }

    #[test]
    fn test_fts5_secure_delete_integrity() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.config_mut().apply_control_command("secure-delete=1");

        for i in 0..10 {
            table.insert_document(i, &[format!("document number {i}")]);
        }

        // Delete half the documents.
        for i in (0..10).step_by(2) {
            table.delete_document(i);
        }

        // Remaining documents should still be searchable.
        let results = table.search("document").unwrap();
        assert_eq!(results.len(), 5, "5 remaining docs should be found");

        // Each remaining doc should have an odd ID.
        for (rowid, _) in &results {
            assert!(rowid % 2 == 1, "only odd-numbered docs should remain");
        }
    }

    #[test]
    fn test_fts5_contentless_delete_tombstone() {
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        *table.config_mut() = Fts5Config::new(ContentMode::Contentless);
        table
            .config_mut()
            .apply_control_command("contentless_delete=1");

        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);

        table.delete_document(1);

        // Deleted entry should no longer match.
        let results = table.search("world").unwrap();
        assert!(
            results.is_empty(),
            "tombstoned entry should not match queries"
        );

        // Non-deleted entry should still match.
        let results = table.search("rust").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fts5_vtab_best_index_full_scan() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        let mut info = IndexInfo::new(vec![], vec![]);
        vtab.best_index(&mut info).unwrap();
        assert_eq!(info.idx_num, 0); // full scan
        assert!(info.estimated_cost > 100_000.0);
    }

    #[test]
    fn test_fts5_vtab_best_index_assigns_distinct_match_argv_slots() {
        let cx = Cx::new();
        let vtab = Fts5Table::connect(&cx, &["fts5", "main", "t", "content"]).unwrap();
        let mut info = IndexInfo::new(
            vec![
                fsqlite_func::vtab::IndexConstraint {
                    column: 0,
                    op: fsqlite_func::vtab::ConstraintOp::Match,
                    usable: true,
                },
                fsqlite_func::vtab::IndexConstraint {
                    column: 0,
                    op: fsqlite_func::vtab::ConstraintOp::Match,
                    usable: true,
                },
            ],
            vec![],
        );
        vtab.best_index(&mut info).unwrap();
        assert_eq!(info.idx_num, 1);
        assert_eq!(info.constraint_usage[0].argv_index, 1);
        assert_eq!(info.constraint_usage[1].argv_index, 2);
        assert!(info.constraint_usage.iter().all(|usage| usage.omit));
    }

    #[test]
    fn test_fts5_cursor_filter_intersects_multiple_match_args() {
        let cx = Cx::new();
        let mut table = Fts5Table::with_columns(vec!["content".to_owned()]);
        table.insert_document(1, &["hello world".to_owned()]);
        table.insert_document(2, &["hello rust".to_owned()]);
        table.insert_document(3, &["world only".to_owned()]);

        let mut cursor = table.open().unwrap();
        cursor
            .filter(
                &cx,
                1,
                None,
                &[
                    SqliteValue::Text(SmallText::from_string("hello")),
                    SqliteValue::Text(SmallText::from_string("world")),
                ],
            )
            .unwrap();

        assert_eq!(cursor.rowid().unwrap(), 1);
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());
    }
}
