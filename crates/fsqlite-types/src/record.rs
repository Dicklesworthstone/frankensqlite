//! SQLite record format serialization and deserialization.
//!
//! A SQLite record consists of a header followed by data. The header contains
//! the size of the header itself (as a varint) followed by serial type codes
//! (each as a varint) for every column. The data section contains the column
//! values packed sequentially according to their serial types.
//!
//! See: <https://www.sqlite.org/fileformat.html#record_format>

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;

use smallvec::SmallVec;

#[cfg(all(
    feature = "nightly-simd",
    target_arch = "x86_64",
    not(target_arch = "wasm32")
))]
use std::simd::{
    Simd,
    cmp::{SimdPartialEq, SimdPartialOrd},
};

use crate::serial_type::{
    SerialTypeClass, classify_serial_type, integer_serial_type_and_len, read_varint,
    serial_type_for_blob, serial_type_for_integer, serial_type_for_text, serial_type_len,
    varint_len, write_varint,
};
use crate::value::{SqliteValue, pool_acquire, pool_return_reusable};

static FSQLITE_RECORD_PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);
static FSQLITE_RECORD_PARSE_CALLS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_PARSE_INTO_CALLS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_PARSE_COLUMN_CALLS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_BYTES_SCANNED: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_VEC_CAPACITY_SLOTS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_DECODE_TIME_NS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_VALUE_NULLS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_VALUE_INTEGERS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_VALUE_FLOATS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_VALUE_TEXTS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_VALUE_BLOBS: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_TEXT_BYTES: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_BLOB_BYTES: AtomicU64 = AtomicU64::new(0);
static FSQLITE_RECORD_PARSE_CALLS_BY_SCOPE: [AtomicU64; RECORD_PROFILE_SCOPE_COUNT] =
    [const { AtomicU64::new(0) }; RECORD_PROFILE_SCOPE_COUNT];
static FSQLITE_RECORD_PARSE_INTO_CALLS_BY_SCOPE: [AtomicU64; RECORD_PROFILE_SCOPE_COUNT] =
    [const { AtomicU64::new(0) }; RECORD_PROFILE_SCOPE_COUNT];
static FSQLITE_RECORD_PARSE_COLUMN_CALLS_BY_SCOPE: [AtomicU64; RECORD_PROFILE_SCOPE_COUNT] =
    [const { AtomicU64::new(0) }; RECORD_PROFILE_SCOPE_COUNT];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum RecordProfileScope {
    Unattributed = 0,
    CoreConnection = 1,
    CoreCompatPersist = 2,
    VdbeEngine = 3,
    VdbeVectorizedScan = 4,
    BtreeCursor = 5,
}

impl RecordProfileScope {
    #[must_use]
    const fn as_index(self) -> usize {
        self as usize
    }
}

const RECORD_PROFILE_SCOPE_COUNT: usize = RecordProfileScope::BtreeCursor as usize + 1;

thread_local! {
    static CURRENT_RECORD_PROFILE_SCOPE: Cell<RecordProfileScope> =
        const { Cell::new(RecordProfileScope::Unattributed) };
    static FSQLITE_RECORD_PROFILE_THREAD_OVERRIDE: Cell<Option<bool>> =
        const { Cell::new(None) };
}

#[derive(Debug)]
pub struct RecordProfileScopeGuard {
    previous: Option<RecordProfileScope>,
}

impl Drop for RecordProfileScopeGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            CURRENT_RECORD_PROFILE_SCOPE.with(|current| current.set(previous));
        }
    }
}

#[must_use]
pub fn enter_record_profile_scope(scope: RecordProfileScope) -> RecordProfileScopeGuard {
    if !record_profile_enabled() {
        return RecordProfileScopeGuard { previous: None };
    }
    let previous = CURRENT_RECORD_PROFILE_SCOPE.with(|current| {
        let previous = current.get();
        current.set(scope);
        previous
    });
    RecordProfileScopeGuard {
        previous: Some(previous),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ValueTypeProfileSnapshot {
    pub null_count: u64,
    pub integer_count: u64,
    pub float_count: u64,
    pub text_count: u64,
    pub blob_count: u64,
    pub text_bytes: u64,
    pub blob_bytes: u64,
}

impl ValueTypeProfileSnapshot {
    #[must_use]
    pub fn total_values(self) -> u64 {
        self.null_count
            .saturating_add(self.integer_count)
            .saturating_add(self.float_count)
            .saturating_add(self.text_count)
            .saturating_add(self.blob_count)
    }

    pub fn saturating_add_assign(&mut self, other: Self) {
        self.null_count = self.null_count.saturating_add(other.null_count);
        self.integer_count = self.integer_count.saturating_add(other.integer_count);
        self.float_count = self.float_count.saturating_add(other.float_count);
        self.text_count = self.text_count.saturating_add(other.text_count);
        self.blob_count = self.blob_count.saturating_add(other.blob_count);
        self.text_bytes = self.text_bytes.saturating_add(other.text_bytes);
        self.blob_bytes = self.blob_bytes.saturating_add(other.blob_bytes);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecordHotPathProfileSnapshot {
    pub parse_record_calls: u64,
    pub parse_record_into_calls: u64,
    pub parse_record_column_calls: u64,
    pub record_bytes_scanned: u64,
    pub record_vec_capacity_slots: u64,
    pub decode_time_ns: u64,
    pub decoded_values: ValueTypeProfileSnapshot,
    pub callsite_breakdown: RecordProfileScopeBreakdownSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecordProfileScopeCountersSnapshot {
    pub parse_record_calls: u64,
    pub parse_record_into_calls: u64,
    pub parse_record_column_calls: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecordProfileScopeBreakdownSnapshot {
    pub unattributed: RecordProfileScopeCountersSnapshot,
    pub core_connection: RecordProfileScopeCountersSnapshot,
    pub core_compat_persist: RecordProfileScopeCountersSnapshot,
    pub vdbe_engine: RecordProfileScopeCountersSnapshot,
    pub vdbe_vectorized_scan: RecordProfileScopeCountersSnapshot,
    pub btree_cursor: RecordProfileScopeCountersSnapshot,
}

pub fn set_record_profile_enabled(enabled: bool) {
    FSQLITE_RECORD_PROFILE_ENABLED.store(enabled, AtomicOrdering::Relaxed);
}

pub fn set_record_profile_thread_override(enabled: Option<bool>) {
    FSQLITE_RECORD_PROFILE_THREAD_OVERRIDE.with(|c| c.set(enabled));
}

#[must_use]
pub fn record_profile_thread_override() -> Option<bool> {
    FSQLITE_RECORD_PROFILE_THREAD_OVERRIDE.with(|c| c.get())
}

#[must_use]
pub fn record_profile_enabled() -> bool {
    if let Some(enabled) = record_profile_thread_override() {
        return enabled;
    }
    FSQLITE_RECORD_PROFILE_ENABLED.load(AtomicOrdering::Relaxed)
}

pub fn reset_record_profile() {
    FSQLITE_RECORD_PARSE_CALLS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_PARSE_INTO_CALLS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_PARSE_COLUMN_CALLS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_BYTES_SCANNED.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_VEC_CAPACITY_SLOTS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_DECODE_TIME_NS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_VALUE_NULLS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_VALUE_INTEGERS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_VALUE_FLOATS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_VALUE_TEXTS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_VALUE_BLOBS.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_TEXT_BYTES.store(0, AtomicOrdering::Relaxed);
    FSQLITE_RECORD_BLOB_BYTES.store(0, AtomicOrdering::Relaxed);
    for counter in &FSQLITE_RECORD_PARSE_CALLS_BY_SCOPE {
        counter.store(0, AtomicOrdering::Relaxed);
    }
    for counter in &FSQLITE_RECORD_PARSE_INTO_CALLS_BY_SCOPE {
        counter.store(0, AtomicOrdering::Relaxed);
    }
    for counter in &FSQLITE_RECORD_PARSE_COLUMN_CALLS_BY_SCOPE {
        counter.store(0, AtomicOrdering::Relaxed);
    }
}

#[must_use]
pub fn record_profile_snapshot() -> RecordHotPathProfileSnapshot {
    RecordHotPathProfileSnapshot {
        parse_record_calls: FSQLITE_RECORD_PARSE_CALLS.load(AtomicOrdering::Relaxed),
        parse_record_into_calls: FSQLITE_RECORD_PARSE_INTO_CALLS.load(AtomicOrdering::Relaxed),
        parse_record_column_calls: FSQLITE_RECORD_PARSE_COLUMN_CALLS.load(AtomicOrdering::Relaxed),
        record_bytes_scanned: FSQLITE_RECORD_BYTES_SCANNED.load(AtomicOrdering::Relaxed),
        record_vec_capacity_slots: FSQLITE_RECORD_VEC_CAPACITY_SLOTS.load(AtomicOrdering::Relaxed),
        decode_time_ns: FSQLITE_RECORD_DECODE_TIME_NS.load(AtomicOrdering::Relaxed),
        decoded_values: ValueTypeProfileSnapshot {
            null_count: FSQLITE_RECORD_VALUE_NULLS.load(AtomicOrdering::Relaxed),
            integer_count: FSQLITE_RECORD_VALUE_INTEGERS.load(AtomicOrdering::Relaxed),
            float_count: FSQLITE_RECORD_VALUE_FLOATS.load(AtomicOrdering::Relaxed),
            text_count: FSQLITE_RECORD_VALUE_TEXTS.load(AtomicOrdering::Relaxed),
            blob_count: FSQLITE_RECORD_VALUE_BLOBS.load(AtomicOrdering::Relaxed),
            text_bytes: FSQLITE_RECORD_TEXT_BYTES.load(AtomicOrdering::Relaxed),
            blob_bytes: FSQLITE_RECORD_BLOB_BYTES.load(AtomicOrdering::Relaxed),
        },
        callsite_breakdown: RecordProfileScopeBreakdownSnapshot {
            unattributed: record_profile_scope_snapshot(RecordProfileScope::Unattributed),
            core_connection: record_profile_scope_snapshot(RecordProfileScope::CoreConnection),
            core_compat_persist: record_profile_scope_snapshot(
                RecordProfileScope::CoreCompatPersist,
            ),
            vdbe_engine: record_profile_scope_snapshot(RecordProfileScope::VdbeEngine),
            vdbe_vectorized_scan: record_profile_scope_snapshot(
                RecordProfileScope::VdbeVectorizedScan,
            ),
            btree_cursor: record_profile_scope_snapshot(RecordProfileScope::BtreeCursor),
        },
    }
}

#[derive(Debug, Clone, Copy)]
enum RecordParseKind {
    Full,
    Into,
    Column,
}

#[must_use]
fn current_record_profile_scope() -> RecordProfileScope {
    CURRENT_RECORD_PROFILE_SCOPE.with(Cell::get)
}

fn note_record_profile_parse_call(kind: RecordParseKind) {
    let idx = current_record_profile_scope().as_index();
    match kind {
        RecordParseKind::Full => {
            FSQLITE_RECORD_PARSE_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
            FSQLITE_RECORD_PARSE_CALLS_BY_SCOPE[idx].fetch_add(1, AtomicOrdering::Relaxed);
        }
        RecordParseKind::Into => {
            FSQLITE_RECORD_PARSE_INTO_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
            FSQLITE_RECORD_PARSE_INTO_CALLS_BY_SCOPE[idx].fetch_add(1, AtomicOrdering::Relaxed);
        }
        RecordParseKind::Column => {
            FSQLITE_RECORD_PARSE_COLUMN_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
            FSQLITE_RECORD_PARSE_COLUMN_CALLS_BY_SCOPE[idx].fetch_add(1, AtomicOrdering::Relaxed);
        }
    }
}

#[must_use]
fn record_profile_scope_snapshot(scope: RecordProfileScope) -> RecordProfileScopeCountersSnapshot {
    let idx = scope.as_index();
    RecordProfileScopeCountersSnapshot {
        parse_record_calls: FSQLITE_RECORD_PARSE_CALLS_BY_SCOPE[idx].load(AtomicOrdering::Relaxed),
        parse_record_into_calls: FSQLITE_RECORD_PARSE_INTO_CALLS_BY_SCOPE[idx]
            .load(AtomicOrdering::Relaxed),
        parse_record_column_calls: FSQLITE_RECORD_PARSE_COLUMN_CALLS_BY_SCOPE[idx]
            .load(AtomicOrdering::Relaxed),
    }
}

fn note_decoded_value(value: &SqliteValue) {
    match value {
        SqliteValue::Null => {
            FSQLITE_RECORD_VALUE_NULLS.fetch_add(1, AtomicOrdering::Relaxed);
        }
        SqliteValue::Integer(_) => {
            FSQLITE_RECORD_VALUE_INTEGERS.fetch_add(1, AtomicOrdering::Relaxed);
        }
        SqliteValue::Float(_) => {
            FSQLITE_RECORD_VALUE_FLOATS.fetch_add(1, AtomicOrdering::Relaxed);
        }
        SqliteValue::Text(text) => {
            FSQLITE_RECORD_VALUE_TEXTS.fetch_add(1, AtomicOrdering::Relaxed);
            FSQLITE_RECORD_TEXT_BYTES.fetch_add(
                u64::try_from(text.len()).unwrap_or(u64::MAX),
                AtomicOrdering::Relaxed,
            );
        }
        SqliteValue::Blob(blob) => {
            FSQLITE_RECORD_VALUE_BLOBS.fetch_add(1, AtomicOrdering::Relaxed);
            FSQLITE_RECORD_BLOB_BYTES.fetch_add(
                u64::try_from(blob.len()).unwrap_or(u64::MAX),
                AtomicOrdering::Relaxed,
            );
        }
    }
}

/// Parse a serialized record into a list of `SqliteValue`s.
///
/// The input `data` should be the complete record (header + body).
/// Returns `None` if the record is malformed.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record(data: &[u8]) -> Option<Vec<SqliteValue>> {
    // A typical record has ~4-8 columns. We can estimate capacity from data len / 8 as a heuristic,
    // clamped between 4 and 64, to avoid reallocation for the majority of rows.
    let cap = (data.len() / 8).clamp(4, 64);
    if record_profile_enabled() {
        note_record_profile_parse_call(RecordParseKind::Full);
        FSQLITE_RECORD_VEC_CAPACITY_SLOTS.fetch_add(
            u64::try_from(cap).unwrap_or(u64::MAX),
            AtomicOrdering::Relaxed,
        );
    }
    let mut values = Vec::with_capacity(cap);
    parse_record_into(data, &mut values)?;
    Some(values)
}

/// Parse a serialized record into an existing `Vec<SqliteValue>`.
///
/// Existing slots are reused when possible so repeated row decodes can keep
/// text/blob backing storage alive across iterations.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record_into(data: &[u8], values: &mut Vec<SqliteValue>) -> Option<()> {
    let profile_enabled = record_profile_enabled();
    let start = profile_enabled.then(Instant::now);
    if profile_enabled {
        note_record_profile_parse_call(RecordParseKind::Into);
        FSQLITE_RECORD_BYTES_SCANNED.fetch_add(
            u64::try_from(data.len()).unwrap_or(u64::MAX),
            AtomicOrdering::Relaxed,
        );
    }
    if data.is_empty() {
        return None;
    }

    // Read the header size.
    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    // Parse serial types and values in a single pass.
    let mut offset = hdr_varint_len;
    let mut body_offset = header_size;
    let mut decoded_count = 0usize;

    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;

        let value_len_u64 = serial_type_len(serial_type)?;
        let value_len = usize::try_from(value_len_u64).unwrap_or(usize::MAX);

        let end = body_offset.checked_add(value_len)?;

        if end > data.len() {
            return None;
        }

        let value_bytes = &data[body_offset..end];
        // Reuse caller-owned scratch slots so repeated row decodes keep
        // existing text/blob backing storage alive across iterations.
        if let Some(slot) = values.get_mut(decoded_count) {
            decode_value_into(serial_type, value_bytes, slot, profile_enabled)?;
        } else {
            let value = decode_value(serial_type, value_bytes, profile_enabled)?;
            values.push(value);
        }
        body_offset = end;
        decoded_count += 1;
    }

    if body_offset != data.len() {
        return None;
    }

    recycle_values_from(values, decoded_count);

    if let Some(start) = start {
        FSQLITE_RECORD_DECODE_TIME_NS.fetch_add(
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            AtomicOrdering::Relaxed,
        );
    }
    Some(())
}

/// Parse a single column from a serialized record, avoiding allocating for other columns.
///
/// Returns `None` if the record is malformed or if `col_idx` is out of bounds.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record_column(data: &[u8], col_idx: usize) -> Option<SqliteValue> {
    let profile_enabled = record_profile_enabled();
    let start = profile_enabled.then(Instant::now);
    if profile_enabled {
        note_record_profile_parse_call(RecordParseKind::Column);
        FSQLITE_RECORD_BYTES_SCANNED.fetch_add(
            u64::try_from(data.len()).unwrap_or(u64::MAX),
            AtomicOrdering::Relaxed,
        );
    }
    if data.is_empty() {
        return None;
    }

    // Read the header size.
    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    let mut offset = hdr_varint_len;
    let mut body_offset = header_size;
    let mut current_idx = 0;

    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;

        let value_len_u64 = serial_type_len(serial_type)?;
        let value_len = usize::try_from(value_len_u64).unwrap_or(usize::MAX);
        let end = body_offset.checked_add(value_len)?;

        if end > data.len() {
            return None;
        }

        if current_idx == col_idx {
            let value_bytes = &data[body_offset..end];
            let value = decode_value(serial_type, value_bytes, profile_enabled);
            if let Some(start) = start {
                FSQLITE_RECORD_DECODE_TIME_NS.fetch_add(
                    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    AtomicOrdering::Relaxed,
                );
            }
            return value;
        }

        body_offset = end;
        current_idx += 1;
    }

    None
}

/// Count the number of serialized columns in a SQLite record without decoding
/// the values themselves.
///
/// Returns `None` if the record is malformed.
#[allow(clippy::cast_possible_truncation)]
pub fn record_column_count(data: &[u8]) -> Option<usize> {
    if data.is_empty() {
        return None;
    }

    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    let mut offset = hdr_varint_len;
    let mut count = 0_usize;

    while offset < header_size {
        let (_, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;
        count += 1;
    }

    Some(count)
}

// ---------------------------------------------------------------------------
// Lazy column decode — Record Offset Table
// ---------------------------------------------------------------------------

/// Pre-parsed record header offset for a single column.
///
/// Stores the serial type and byte range within the record body so that
/// individual columns can be decoded on demand without scanning the entire
/// record's data section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnOffset {
    /// SQLite serial type code for this column.
    pub serial_type: u64,
    /// Start offset of the value bytes within the record (from byte 0).
    pub body_offset: u32,
    /// Length of the value bytes.
    pub value_len: u32,
}

/// Projected offsets parsed from a record header without materializing a full
/// offset table.
#[derive(Debug, Clone, Copy)]
pub struct ProjectedColumnOffsets {
    /// Number of serial-type entries in the record header.
    pub column_count: usize,
    /// Offset for the primary projected column, if that column exists in the
    /// stored record image.
    pub primary: Option<ColumnOffset>,
    /// Offset for an optional secondary projected column, if requested and
    /// present in the stored record image.
    pub secondary: Option<ColumnOffset>,
}

/// Numeric shape of a SQLite record value for aggregate fast paths.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumericColumnValue {
    /// SQL NULL, including NaN REAL values after SQLite normalization.
    Null,
    /// Integer value.
    Integer(i64),
    /// Floating-point value.
    Float(f64),
    /// Text or blob. Callers that need exact SQLite numeric coercion should
    /// fall back to the ordinary aggregate implementation.
    NonNumeric,
}

/// Parse only the record header into a caller-owned offset table.
///
/// This is the lazy-decode counterpart to [`parse_record_into`]:
/// it scans the header to learn each column's serial type and byte range
/// but does **not** decode any column values.  Individual columns can
/// then be decoded on demand via [`decode_value`] with the corresponding
/// [`ColumnOffset`].
///
/// Returns `Some(column_count)` on success, `None` if the header is
/// malformed.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record_header_into(data: &[u8], offsets: &mut Vec<ColumnOffset>) -> Option<usize> {
    offsets.clear();
    if data.is_empty() {
        return None;
    }

    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    let mut offset = hdr_varint_len;
    let mut body_offset = header_size;

    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;

        let value_len_u64 = serial_type_len(serial_type)?;
        let value_len = usize::try_from(value_len_u64).unwrap_or(usize::MAX);
        let body_start = body_offset;
        body_offset = body_offset.checked_add(value_len)?;

        if body_offset > data.len() {
            return None;
        }

        offsets.push(ColumnOffset {
            serial_type,
            body_offset: u32::try_from(body_start).ok()?,
            value_len: u32::try_from(value_len).ok()?,
        });
    }

    // Validate that the body section is fully consumed.
    if body_offset != data.len() {
        return None;
    }

    Some(offsets.len())
}

/// Parse the record header and retain offsets for at most two projected
/// columns.
///
/// This keeps the same malformed-record checks as [`parse_record_header_into`]
/// but avoids clearing and filling a caller-owned `Vec<ColumnOffset>` when a
/// scan only needs one column.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record_projected_column_offsets(
    data: &[u8],
    primary_index: usize,
    secondary_index: Option<usize>,
) -> Option<ProjectedColumnOffsets> {
    if data.is_empty() {
        return None;
    }

    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    let mut offset = hdr_varint_len;
    let mut body_offset = header_size;
    let mut column_index = 0usize;
    let mut primary = None;
    let mut secondary = None;

    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;

        let value_len_u64 = serial_type_len(serial_type)?;
        let value_len = usize::try_from(value_len_u64).unwrap_or(usize::MAX);
        let body_start = body_offset;
        body_offset = body_offset.checked_add(value_len)?;

        if body_offset > data.len() {
            return None;
        }

        if column_index == primary_index || secondary_index == Some(column_index) {
            let column = ColumnOffset {
                serial_type,
                body_offset: u32::try_from(body_start).ok()?,
                value_len: u32::try_from(value_len).ok()?,
            };
            if column_index == primary_index {
                primary = Some(column);
            }
            if secondary_index == Some(column_index) {
                secondary = Some(column);
            }
        }

        column_index = column_index.checked_add(1)?;
    }

    if body_offset != data.len() {
        return None;
    }

    Some(ProjectedColumnOffsets {
        column_count: column_index,
        primary,
        secondary,
    })
}

/// Parse only the record header from a partial record prefix.
///
/// Unlike [`parse_record_header_into`], this only requires the full header
/// bytes to be present. The body bytes may be truncated because callers can
/// fetch more payload after learning which column end offset is needed.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record_header_prefix_into(
    data: &[u8],
    offsets: &mut Vec<ColumnOffset>,
) -> Option<usize> {
    offsets.clear();
    if data.is_empty() {
        return None;
    }

    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);
    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    let mut offset = hdr_varint_len;
    let mut body_offset = header_size;

    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;

        let value_len_u64 = serial_type_len(serial_type)?;
        let value_len = usize::try_from(value_len_u64).unwrap_or(usize::MAX);
        let body_start = body_offset;
        body_offset = body_offset.checked_add(value_len)?;

        offsets.push(ColumnOffset {
            serial_type,
            body_offset: u32::try_from(body_start).ok()?,
            value_len: u32::try_from(value_len).ok()?,
        });
    }

    Some(offsets.len())
}

/// Decode a single column from previously-parsed offset table + raw record data.
///
/// This avoids re-scanning the header on each column access and only
/// materializes the value for the requested column.
#[inline]
pub fn decode_column_from_offset(
    data: &[u8],
    col: &ColumnOffset,
    profile_enabled: bool,
) -> Option<SqliteValue> {
    let start = col.body_offset as usize;
    let end = start.checked_add(col.value_len as usize)?;
    if end > data.len() {
        return None;
    }
    decode_value(col.serial_type, &data[start..end], profile_enabled)
}

/// Decode only the numeric aggregate-relevant shape of a projected column.
///
/// Text/blob values are reported as [`NumericColumnValue::NonNumeric`] without
/// allocation or UTF-8 validation; callers that need SQLite's text-to-number
/// coercions must fall back to the full aggregate path.
#[inline]
pub fn decode_numeric_column_from_offset(
    data: &[u8],
    col: &ColumnOffset,
) -> Option<NumericColumnValue> {
    let start = col.body_offset as usize;
    let end = start.checked_add(col.value_len as usize)?;
    if end > data.len() {
        return None;
    }
    let bytes = &data[start..end];

    match classify_serial_type(col.serial_type) {
        // Reserved serial types (10, 11): zero-length internal codes. The
        // tolerant decoder maps them to NULL after the record scanner has
        // preserved SQLite's zero-length layout.
        SerialTypeClass::Null | SerialTypeClass::Reserved => Some(NumericColumnValue::Null),
        SerialTypeClass::Zero => Some(NumericColumnValue::Integer(0)),
        SerialTypeClass::One => Some(NumericColumnValue::Integer(1)),
        SerialTypeClass::Integer => {
            Some(NumericColumnValue::Integer(decode_big_endian_signed(bytes)))
        }
        SerialTypeClass::Float => {
            if bytes.len() != 8 {
                return None;
            }
            let bits = u64::from_be_bytes(bytes.try_into().ok()?);
            let value = f64::from_bits(bits);
            Some(if value.is_nan() {
                NumericColumnValue::Null
            } else {
                NumericColumnValue::Float(value)
            })
        }
        SerialTypeClass::Text | SerialTypeClass::Blob => Some(NumericColumnValue::NonNumeric),
    }
}

/// Decode a single column, reusing the previous row's cached `Arc` when the
/// raw bytes are identical (bd-db300.4.4.2 — K1 copy elimination).
///
/// For Text/Blob columns that repeat across consecutive rows (common in
/// JOINs, GROUP BY, denormalized tables), this avoids the malloc+memcpy+free
/// cycle by returning `Arc::clone` of the existing cached value instead of
/// allocating a fresh `Arc::from(bytes)`.
///
/// Cost when values match: one `memcmp(len)` + `Arc::clone` (~5ns).
/// Cost when values differ: one `memcmp(len)` + normal `decode_value`.
/// Scalars (Integer, Float, Null): no overhead — hint check is skipped.
#[inline]
pub fn decode_column_from_offset_reuse(
    data: &[u8],
    col: &ColumnOffset,
    hint: Option<&SqliteValue>,
    profile_enabled: bool,
) -> Option<SqliteValue> {
    let start = col.body_offset as usize;
    let end = start.checked_add(col.value_len as usize)?;
    if end > data.len() {
        return None;
    }
    let bytes = &data[start..end];

    // Fast path: if the hint's raw bytes match, reuse its Arc allocation.
    if let Some(hint) = hint {
        match (classify_serial_type(col.serial_type), hint) {
            (SerialTypeClass::Text, SqliteValue::Text(arc)) if arc.as_bytes_direct() == bytes => {
                if profile_enabled {
                    note_decoded_value(hint);
                }
                return Some(SqliteValue::Text(arc.clone()));
            }
            (SerialTypeClass::Blob, SqliteValue::Blob(arc)) if arc.as_ref() == bytes => {
                if profile_enabled {
                    note_decoded_value(hint);
                }
                return Some(SqliteValue::Blob(Arc::clone(arc)));
            }
            _ => {}
        }
    }

    decode_value(col.serial_type, bytes, profile_enabled)
}

/// Caller-owned scratch for lazy record decode and row materialization.
///
/// This scratch is intended to be owned by a single cursor-like object and
/// reused only while that owner remains positioned on the same physical record
/// image. Callers must invalidate it when the row image changes.
///
/// The scratch chooses a cursor-local reusable buffer model rather than an
/// arena model:
/// - `header_offsets` caches the parsed record layout for lazy decode
/// - `values` reuses existing `SqliteValue` backing storage across decodes
/// - `decoded_mask` tracks which columns have been materialized for narrow rows
///
/// Nothing in this type is shared across cursors, statements, or transactions.
#[derive(Debug, Default, Clone)]
pub struct RecordDecodeScratch {
    header_offsets: Vec<ColumnOffset>,
    values: Vec<SqliteValue>,
    decoded_mask: u64,
}

#[inline]
fn grow_value_slots(values: &mut Vec<SqliteValue>, target_len: usize) {
    while values.len() < target_len {
        values.push(pool_acquire().unwrap_or(SqliteValue::Null));
    }
}

#[inline]
fn recycle_values_from(values: &mut Vec<SqliteValue>, start: usize) {
    for value in values.drain(start..) {
        pool_return_reusable(value);
    }
}

#[inline]
fn clear_value_slots(values: &mut [SqliteValue]) {
    for value in values {
        let old_value = std::mem::replace(value, SqliteValue::Null);
        pool_return_reusable(old_value);
    }
}

impl RecordDecodeScratch {
    /// Prepare the scratch for a new serialized record.
    ///
    /// Returns `Some(true)` when the record is eagerly decoded because it has
    /// more than 64 columns, `Some(false)` when lazy decode remains active, or
    /// `None` when the record is malformed.
    #[must_use]
    pub fn prepare_for_record(&mut self, record: &[u8]) -> Option<bool> {
        let col_count = match parse_record_header_into(record, &mut self.header_offsets) {
            Some(col_count) => col_count,
            None => {
                self.invalidate();
                return None;
            }
        };
        if col_count > 64 {
            recycle_values_from(&mut self.values, 0);
            if parse_record_into(record, &mut self.values).is_none() {
                self.invalidate();
                return None;
            }
            self.decoded_mask = u64::MAX;
            Some(true)
        } else {
            if self.values.len() > col_count {
                recycle_values_from(&mut self.values, col_count);
            } else if self.values.len() < col_count {
                grow_value_slots(&mut self.values, col_count);
            }
            self.decoded_mask = 0;
            Some(false)
        }
    }

    /// Prepare the scratch from a partial record prefix.
    ///
    /// This parses only the record header and sizes the reusable value slots
    /// so callers can later fetch and decode just the requested columns.
    #[must_use]
    pub fn prepare_for_record_prefix(&mut self, record: &[u8]) -> Option<usize> {
        let col_count = match parse_record_header_prefix_into(record, &mut self.header_offsets) {
            Some(col_count) => col_count,
            None => {
                self.invalidate();
                return None;
            }
        };

        if self.values.len() > col_count {
            recycle_values_from(&mut self.values, col_count);
        } else if self.values.len() < col_count {
            grow_value_slots(&mut self.values, col_count);
        }
        self.decoded_mask = 0;
        Some(col_count)
    }

    /// Drop the cached layout and decoded values while preserving value slots.
    pub fn invalidate(&mut self) {
        self.header_offsets.clear();
        clear_value_slots(&mut self.values);
        self.decoded_mask = 0;
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.header_offsets.is_empty()
    }

    #[must_use]
    pub fn column_count(&self) -> usize {
        self.header_offsets.len()
    }

    #[must_use]
    pub fn column_offset(&self, idx: usize) -> Option<&ColumnOffset> {
        self.header_offsets.get(idx)
    }

    #[must_use]
    pub fn cached_value(&self, idx: usize) -> Option<&SqliteValue> {
        self.values.get(idx)
    }

    #[must_use]
    pub fn cached_value_ready(&self, idx: usize) -> bool {
        if idx < 64 {
            self.decoded_mask & (1_u64 << idx) != 0
        } else {
            self.decoded_mask == u64::MAX
        }
    }

    pub fn cache_decoded(&mut self, idx: usize, value: SqliteValue) {
        if idx >= self.values.len() {
            grow_value_slots(&mut self.values, idx + 1);
        }
        let old_value = std::mem::replace(&mut self.values[idx], value);
        pool_return_reusable(old_value);
        if idx < 64 {
            self.decoded_mask |= 1_u64 << idx;
        }
    }
}

/// Decode only the first `n` columns from a serialized record.
///
/// This is the sort-key extraction path: for ORDER BY on 2 columns of a
/// 20-column record, we decode only 2 columns instead of 20.  The raw
/// blob is kept separately for output.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record_prefix(data: &[u8], max_cols: usize) -> Option<Vec<SqliteValue>> {
    if max_cols == 0 {
        return Some(Vec::new());
    }
    if data.is_empty() {
        return None;
    }

    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = usize::try_from(header_size_u64).unwrap_or(usize::MAX);
    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    let mut values = Vec::with_capacity(max_cols.min(16));
    let mut offset = hdr_varint_len;
    let mut body_offset = header_size;
    let profile_enabled = record_profile_enabled();

    while offset < header_size && values.len() < max_cols {
        let (serial_type, consumed) = read_varint(&data[offset..header_size])?;
        offset += consumed;

        let value_len_u64 = serial_type_len(serial_type)?;
        let value_len = usize::try_from(value_len_u64).unwrap_or(usize::MAX);
        let end = body_offset.checked_add(value_len)?;
        if end > data.len() {
            return None;
        }

        let value = decode_value(serial_type, &data[body_offset..end], profile_enabled)?;
        values.push(value);
        body_offset = end;
    }

    Some(values)
}

/// Supported compile-time header slots for `OP_MakeRecord`.
///
/// These are the cases where codegen can prove the SQLite record header width
/// up front, so execution only needs to memcpy a header template and patch the
/// remaining one-byte serial types in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrecomputedSerialTypeKind {
    /// INTEGER PRIMARY KEY aliases are stored as the rowid, so the record
    /// payload always contains a NULL placeholder for that column.
    NullPlaceholder,
    /// Any runtime value whose serial type fits in a single-byte varint.
    ///
    /// This covers NULL, all INTEGER encodings, REAL, and short TEXT/BLOB
    /// values whose serial-type codes stay below 128. It lets hot repeated
    /// inserts learn a compact header template from the first execution and
    /// reuse it as long as subsequent rows stay in the same one-byte envelope.
    AnyOneByteVarintOrNull,
    /// STRICT INTEGER column after `TypeCheck`/`Affinity`: either NULL or an
    /// integer serial type in the 0/1..=9 range (always a one-byte varint).
    IntegerOrNull,
    /// STRICT REAL column after `TypeCheck`/`Affinity`: either NULL or REAL.
    /// NaN still normalizes to NULL during serialization.
    RealOrNull,
}

impl PrecomputedSerialTypeKind {
    fn header_varint_len(self) -> usize {
        match self {
            Self::NullPlaceholder
            | Self::AnyOneByteVarintOrNull
            | Self::IntegerOrNull
            | Self::RealOrNull => 1,
        }
    }

    const fn max_payload_len(self) -> usize {
        match self {
            Self::NullPlaceholder => 0,
            Self::AnyOneByteVarintOrNull => 57,
            Self::IntegerOrNull | Self::RealOrNull => 8,
        }
    }

    #[inline]
    fn serial_byte_and_payload_len(self, value: &SqliteValue) -> Option<(u8, usize)> {
        match self {
            Self::NullPlaceholder => Some((0, 0)),
            Self::AnyOneByteVarintOrNull => {
                let (serial_type, payload_len) = serialized_value_layout(value);
                (varint_len(serial_type) == 1)
                    .then(|| (u8::try_from(serial_type).unwrap_or(0), payload_len))
            }
            Self::IntegerOrNull => match value {
                SqliteValue::Null => Some((0, 0)),
                SqliteValue::Integer(i) => {
                    let serial_type = serial_type_for_integer(*i);
                    let payload_len = match serial_type {
                        8 | 9 => 0,
                        1 => 1,
                        2 => 2,
                        3 => 3,
                        4 => 4,
                        5 => 6,
                        6 => 8,
                        _ => unreachable!("integer serial type must be in 1..=9"),
                    };
                    Some((u8::try_from(serial_type).unwrap_or(0), payload_len))
                }
                _ => None,
            },
            Self::RealOrNull => match value {
                SqliteValue::Null => Some((0, 0)),
                SqliteValue::Float(f) => {
                    if f.is_nan() {
                        Some((0, 0))
                    } else {
                        Some((7, 8))
                    }
                }
                _ => None,
            },
        }
    }

    const fn needs_runtime_patch(self) -> bool {
        matches!(
            self,
            Self::AnyOneByteVarintOrNull | Self::IntegerOrNull | Self::RealOrNull
        )
    }
}

/// A precomputed SQLite record-header template plus per-column slot metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrecomputedRecordHeader {
    /// Fully encoded header bytes, including the header-size varint.
    ///
    /// Dynamic serial-type slots are initialized to `0` and patched in-place at
    /// execution time.
    pub template: Vec<u8>,
    /// Slot metadata in column order.
    pub slots: Vec<PrecomputedSerialTypeSlot>,
    /// Worst-case payload bytes for all supported runtime values.
    pub max_body_size: usize,
}

impl PrecomputedRecordHeader {
    /// Build a header template for a sequence of supported serial-type slots.
    #[must_use]
    pub fn new(kinds: &[PrecomputedSerialTypeKind]) -> Self {
        let header_content_size = kinds
            .iter()
            .map(|kind| kind.header_varint_len())
            .sum::<usize>();
        let header_size = compute_header_size(header_content_size);
        let mut template = vec![0; header_size];
        let mut offset = write_varint(
            template.as_mut_slice(),
            u64::try_from(header_size).unwrap_or(u64::MAX),
        );
        let mut slots = Vec::with_capacity(kinds.len());
        let mut max_body_size = 0usize;

        for &kind in kinds {
            let slot_offset = offset;
            if kind.needs_runtime_patch() {
                template[offset] = 0;
                offset += 1;
            } else {
                offset += write_varint(&mut template[offset..], 0);
            }
            slots.push(PrecomputedSerialTypeSlot {
                kind,
                header_offset: slot_offset,
            });
            max_body_size += kind.max_payload_len();
        }

        debug_assert_eq!(offset, header_size);
        Self {
            template,
            slots,
            max_body_size,
        }
    }

    #[must_use]
    pub fn column_count(&self) -> usize {
        self.slots.len()
    }
}

/// Column-order metadata for a precomputed record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrecomputedSerialTypeSlot {
    /// Compile-time slot behavior.
    pub kind: PrecomputedSerialTypeKind,
    /// Offset within [`PrecomputedRecordHeader::template`] where this slot's
    /// serial-type varint begins.
    pub header_offset: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct IntegerEncoding {
    serial_type: u8,
    payload_len: u8,
}

impl IntegerEncoding {
    #[inline]
    fn from_serial_type(serial_type: u8) -> Self {
        let payload_len = match serial_type {
            8 | 9 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 => 8,
            _ => unreachable!("integer serial type must be in 1..=9"),
        };
        Self {
            serial_type,
            payload_len,
        }
    }
}

#[inline]
fn scalar_integer_encoding(value: i64) -> IntegerEncoding {
    let serial_type = u8::try_from(serial_type_for_integer(value)).unwrap_or(0);
    IntegerEncoding::from_serial_type(serial_type)
}

#[cfg(all(
    feature = "nightly-simd",
    target_arch = "x86_64",
    not(target_arch = "wasm32")
))]
#[inline]
fn avx2_available() -> bool {
    std::arch::is_x86_feature_detected!("avx2")
}

#[cfg(not(all(
    feature = "nightly-simd",
    target_arch = "x86_64",
    not(target_arch = "wasm32")
)))]
#[inline]
const fn avx2_available() -> bool {
    false
}

#[cfg(all(
    feature = "nightly-simd",
    target_arch = "x86_64",
    not(target_arch = "wasm32")
))]
#[inline]
fn classify_integer_block_simd(values: [i64; 4]) -> [IntegerEncoding; 4] {
    let values = Simd::<i64, 4>::from_array(values);
    let eq_zero = values.simd_eq(Simd::splat(0));
    let eq_one = values.simd_eq(Simd::splat(1));
    let fits_i8 = values.simd_ge(Simd::splat(-128)) & values.simd_le(Simd::splat(127));
    let fits_i16 = values.simd_ge(Simd::splat(-32_768)) & values.simd_le(Simd::splat(32_767));
    let fits_i24 = values.simd_ge(Simd::splat(-8_388_608)) & values.simd_le(Simd::splat(8_388_607));
    let fits_i32 =
        values.simd_ge(Simd::splat(-2_147_483_648)) & values.simd_le(Simd::splat(2_147_483_647));
    let fits_i48 = values.simd_ge(Simd::splat(-140_737_488_355_328))
        & values.simd_le(Simd::splat(140_737_488_355_327));

    let eq_zero = eq_zero.to_array();
    let eq_one = eq_one.to_array();
    let fits_i8 = fits_i8.to_array();
    let fits_i16 = fits_i16.to_array();
    let fits_i24 = fits_i24.to_array();
    let fits_i32 = fits_i32.to_array();
    let fits_i48 = fits_i48.to_array();

    std::array::from_fn(|idx| {
        let serial_type = if eq_zero[idx] {
            8
        } else if eq_one[idx] {
            9
        } else if fits_i8[idx] {
            1
        } else if fits_i16[idx] {
            2
        } else if fits_i24[idx] {
            3
        } else if fits_i32[idx] {
            4
        } else if fits_i48[idx] {
            5
        } else {
            6
        };
        IntegerEncoding::from_serial_type(serial_type)
    })
}

#[cfg(not(all(
    feature = "nightly-simd",
    target_arch = "x86_64",
    not(target_arch = "wasm32")
)))]
#[inline]
fn classify_integer_block_simd(values: [i64; 4]) -> [IntegerEncoding; 4] {
    values.map(scalar_integer_encoding)
}

#[inline]
fn classify_integer_block(values: [i64; 4], use_simd: bool) -> [IntegerEncoding; 4] {
    if use_simd {
        classify_integer_block_simd(values)
    } else {
        values.map(scalar_integer_encoding)
    }
}

#[inline]
fn write_integer_payload(value: i64, payload_len: usize, dst: &mut [u8]) {
    if payload_len == 0 {
        return;
    }
    let bytes = value.to_be_bytes();
    dst.copy_from_slice(&bytes[8 - payload_len..]);
}

#[inline]
fn write_classified_integer_block(
    values: &[i64],
    layouts: &[IntegerEncoding],
    buf: &mut [u8],
    header_offset: &mut usize,
    body_offset: &mut usize,
) {
    for (value, layout) in values.iter().zip(layouts.iter()) {
        buf[*header_offset] = layout.serial_type;
        *header_offset += 1;

        let payload_len = usize::from(layout.payload_len);
        let body_end = *body_offset + payload_len;
        write_integer_payload(*value, payload_len, &mut buf[*body_offset..body_end]);
        *body_offset = body_end;
    }
}

fn serialize_integer_record_with_classifier<'a, I>(
    values: I,
    buf: &mut Vec<u8>,
    use_simd: bool,
) -> bool
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    let mut body_size = 0usize;
    let mut column_count = 0usize;
    let mut block_values = [0_i64; 4];
    let mut block_len = 0usize;

    for value in values.clone() {
        let SqliteValue::Integer(integer) = value else {
            return false;
        };

        block_values[block_len] = *integer;
        block_len += 1;
        column_count += 1;

        if block_len == 4 {
            let layouts = classify_integer_block(block_values, use_simd);
            body_size += layouts
                .iter()
                .map(|layout| usize::from(layout.payload_len))
                .sum::<usize>();
            block_len = 0;
        }
    }

    for value in block_values.iter().take(block_len) {
        body_size += usize::from(scalar_integer_encoding(*value).payload_len);
    }

    let header_size = compute_header_size(column_count);
    let total_size = header_size + body_size;
    buf.clear();
    buf.resize(total_size, 0);

    let mut header_offset = write_varint(
        buf.as_mut_slice(),
        u64::try_from(header_size).unwrap_or(u64::MAX),
    );
    let mut body_offset = header_size;
    let mut encode_block_values = [0_i64; 4];
    let mut encode_block_len = 0usize;

    for value in values {
        let SqliteValue::Integer(integer) = value else {
            return false;
        };

        encode_block_values[encode_block_len] = *integer;
        encode_block_len += 1;

        if encode_block_len == 4 {
            let layouts = classify_integer_block(encode_block_values, use_simd);
            write_classified_integer_block(
                &encode_block_values,
                &layouts,
                buf.as_mut_slice(),
                &mut header_offset,
                &mut body_offset,
            );
            encode_block_len = 0;
        }
    }

    if encode_block_len > 0 {
        let mut layouts = [IntegerEncoding::default(); 4];
        for idx in 0..encode_block_len {
            layouts[idx] = scalar_integer_encoding(encode_block_values[idx]);
        }
        write_classified_integer_block(
            &encode_block_values[..encode_block_len],
            &layouts[..encode_block_len],
            buf.as_mut_slice(),
            &mut header_offset,
            &mut body_offset,
        );
    }

    debug_assert_eq!(header_offset, header_size);
    debug_assert_eq!(body_offset, total_size);
    true
}

/// Serialize an all-integer record using a safe 4-lane classifier.
///
/// On x86_64 hosts with AVX2 available at runtime, the serial-type
/// classification step uses nightly `portable_simd`; other hosts fall back to
/// the same scalar encoding logic.
pub fn simd_serialize_integer_record<'a, I>(values: I, buf: &mut Vec<u8>) -> bool
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    serialize_integer_record_with_classifier(values, buf, avx2_available())
}

/// Serialize a list of `SqliteValue` into the SQLite record format.
pub fn serialize_record(values: &[SqliteValue]) -> Vec<u8> {
    serialize_record_iter(values.iter())
}

/// Serialize a list of `SqliteValue` references into the SQLite record format.
pub fn serialize_record_refs(values: &[&SqliteValue]) -> Vec<u8> {
    serialize_record_iter(values.iter().copied())
}

/// Core serialization logic.
///
/// Matches SQLite's `OP_MakeRecord` shape: one pass to measure header/body
/// sizes while caching each column layout, one pass to write serial types +
/// payload bytes directly into the destination buffer.
pub fn serialize_record_iter<'a, I>(values: I) -> Vec<u8>
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    let mut buf = Vec::new();
    serialize_record_iter_into_impl(values, &mut buf);
    buf
}

/// Serialize a record into a reusable buffer, avoiding allocation.
///
/// Same logic as `serialize_record_iter` but writes into `buf` (clearing it
/// first and reusing its capacity). This eliminates the per-row `Vec<u8>`
/// allocation in the `MakeRecord` opcode hot path.
pub fn serialize_record_iter_into<'a, I>(values: I, buf: &mut Vec<u8>)
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    serialize_record_iter_into_impl(values, buf);
}

/// Serialize a record using a compile-time precomputed header template.
///
/// Returns `false` when the runtime values do not match the header contract,
/// allowing callers to fall back to the generic path.
pub fn serialize_record_iter_with_precomputed_header_into<'a, I>(
    values: I,
    header: &PrecomputedRecordHeader,
    buf: &mut Vec<u8>,
) -> bool
where
    I: Iterator<Item = &'a SqliteValue>,
{
    const STACK_PRECOMPUTED_RECORD_SLOTS: usize = 16;
    const APPEND_PRECOMPUTED_RECORD_MAX_BODY_SIZE: usize = 384;

    if header.slots.len() == 1 {
        return serialize_single_slot_precomputed_record_into(values, header, buf);
    }

    if header.slots.len() > STACK_PRECOMPUTED_RECORD_SLOTS
        || header.max_body_size <= APPEND_PRECOMPUTED_RECORD_MAX_BODY_SIZE
    {
        return serialize_record_iter_with_precomputed_header_append_into(values, header, buf);
    }

    let header_size = header.template.len();
    let mut body_size = 0usize;
    let mut value_iter = values;
    let mut value_refs: [Option<&'a SqliteValue>; STACK_PRECOMPUTED_RECORD_SLOTS] =
        [None; STACK_PRECOMPUTED_RECORD_SLOTS];
    let mut serial_bytes = [0_u8; STACK_PRECOMPUTED_RECORD_SLOTS];
    let mut payload_lens = [0_usize; STACK_PRECOMPUTED_RECORD_SLOTS];

    for (idx, slot) in header.slots.iter().enumerate() {
        let Some(value) = value_iter.next() else {
            return false;
        };
        let Some((serial_byte, payload_len)) = slot.kind.serial_byte_and_payload_len(value) else {
            return false;
        };
        let Some(next_body_size) = body_size.checked_add(payload_len) else {
            return false;
        };
        body_size = next_body_size;
        value_refs[idx] = Some(value);
        serial_bytes[idx] = serial_byte;
        payload_lens[idx] = payload_len;
    }
    if value_iter.next().is_some() {
        return false;
    }

    let Some(total_size) = header_size.checked_add(body_size) else {
        return false;
    };

    buf.clear();
    buf.resize(total_size, 0);
    buf[..header_size].copy_from_slice(&header.template);

    let mut body_offset = header_size;
    for (idx, slot) in header.slots.iter().enumerate() {
        let Some(value) = value_refs[idx] else {
            return false;
        };
        if slot.kind.needs_runtime_patch() {
            debug_assert!(slot.header_offset < header_size);
            buf[slot.header_offset] = serial_bytes[idx];
        }
        let payload_len = payload_lens[idx];
        let body_end = body_offset + payload_len;
        encode_serialized_value(value, payload_len, &mut buf[body_offset..body_end]);
        body_offset = body_end;
    }

    debug_assert_eq!(body_offset, total_size);
    true
}

fn serialize_single_slot_precomputed_record_into<'a, I>(
    values: I,
    header: &PrecomputedRecordHeader,
    buf: &mut Vec<u8>,
) -> bool
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let mut values = values;
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }

    let slot = &header.slots[0];
    let Some((serial_byte, payload_len)) = slot.kind.serial_byte_and_payload_len(value) else {
        return false;
    };
    let header_size = header.template.len();
    let Some(total_size) = header_size.checked_add(payload_len) else {
        return false;
    };

    buf.clear();
    buf.resize(total_size, 0);
    buf[..header_size].copy_from_slice(&header.template);
    if slot.kind.needs_runtime_patch() {
        debug_assert!(slot.header_offset < header_size);
        buf[slot.header_offset] = serial_byte;
    }
    encode_serialized_value(value, payload_len, &mut buf[header_size..]);
    true
}

fn serialize_record_iter_with_precomputed_header_append_into<'a, I>(
    values: I,
    header: &PrecomputedRecordHeader,
    buf: &mut Vec<u8>,
) -> bool
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let header_size = header.template.len();
    let Some(total_capacity) = header_size.checked_add(header.max_body_size) else {
        buf.clear();
        return false;
    };
    buf.clear();
    buf.reserve(total_capacity);
    buf.extend_from_slice(&header.template);

    let mut value_iter = values;
    for slot in &header.slots {
        let Some(value) = value_iter.next() else {
            buf.clear();
            return false;
        };
        let Some((serial_byte, payload_len)) = slot.kind.serial_byte_and_payload_len(value) else {
            buf.clear();
            return false;
        };
        if slot.kind.needs_runtime_patch() {
            debug_assert!(slot.header_offset < header_size);
            buf[slot.header_offset] = serial_byte;
        }
        append_serialized_value(value, payload_len, buf);
    }
    if value_iter.next().is_some() {
        buf.clear();
        return false;
    }

    true
}

fn append_serialized_value(value: &SqliteValue, payload_len: usize, buf: &mut Vec<u8>) {
    let start_len = buf.len();
    match value {
        SqliteValue::Null => {}
        SqliteValue::Integer(i) => {
            if payload_len != 0 {
                let bytes = i.to_be_bytes();
                buf.extend_from_slice(&bytes[8 - payload_len..]);
            }
        }
        SqliteValue::Float(f) => {
            if !f.is_nan() {
                buf.extend_from_slice(&f.to_bits().to_be_bytes());
            }
        }
        SqliteValue::Text(s) => {
            buf.extend_from_slice(s.as_bytes_direct());
        }
        SqliteValue::Blob(b) => {
            buf.extend_from_slice(b);
        }
    }
    debug_assert_eq!(buf.len() - start_len, payload_len);
}

/// Compute the exact total record size (header + body) for a value iterator
/// against a precomputed-header template.
///
/// Returns `Some(total_size)` when every value matches its corresponding slot
/// contract, or `None` when a slot rejects its value (caller must fall back to
/// the generic path). The header portion of the record has fixed size
/// (`header.template.len()`); only the body varies with actual payload bytes.
///
/// This is intended for "measure first, write into an exact-sized slot"
/// workflows (see [`serialize_record_iter_with_precomputed_header_into_slice`]),
/// where the caller needs to carve space in a page buffer before serializing
/// to avoid an intermediate `Vec<u8>` copy.
pub fn record_iter_with_precomputed_header_exact_size<'a, I>(
    values: I,
    header: &PrecomputedRecordHeader,
) -> Option<usize>
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let mut body_size = 0usize;
    let mut value_iter = values;
    for slot in &header.slots {
        let value = value_iter.next()?;
        let (_serial_byte, payload_len) = slot.kind.serial_byte_and_payload_len(value)?;
        body_size = body_size.checked_add(payload_len)?;
    }
    if value_iter.next().is_some() {
        return None;
    }
    header.template.len().checked_add(body_size)
}

/// Serialize a record using a compile-time precomputed header template,
/// writing directly into a caller-provided exact-sized byte slice.
///
/// The slice length MUST equal the value returned by
/// [`record_iter_with_precomputed_header_exact_size`] for the same
/// `(values, header)` pair — i.e. the caller has already measured the record
/// and carved a slot in its destination (e.g. a page buffer).
///
/// On success returns `Ok(())`. Returns `Err(())` when values don't match the
/// header contract or when `dst.len()` disagrees with the expected size; the
/// partial state of `dst` is unspecified on error and the caller must treat
/// its contents as garbage.
///
/// This path avoids an intermediate `Vec<u8>` allocation + memcpy on the
/// INSERT hot path: previously records went
/// `record_scratch Vec -> page buffer memcpy`; this writes straight into the
/// page's pre-reserved payload region.
#[allow(clippy::result_unit_err)]
pub fn serialize_record_iter_with_precomputed_header_into_slice<'a, I>(
    values: I,
    header: &PrecomputedRecordHeader,
    dst: &mut [u8],
) -> Result<(), ()>
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let header_size = header.template.len();
    if dst.len() < header_size {
        return Err(());
    }
    dst[..header_size].copy_from_slice(&header.template);

    let mut body_offset = header_size;
    let mut value_iter = values;
    for slot in &header.slots {
        let Some(value) = value_iter.next() else {
            return Err(());
        };
        let Some((serial_byte, payload_len)) = slot.kind.serial_byte_and_payload_len(value) else {
            return Err(());
        };
        if slot.kind.needs_runtime_patch() {
            if slot.header_offset >= header_size {
                return Err(());
            }
            dst[slot.header_offset] = serial_byte;
        }
        let end = body_offset.checked_add(payload_len).ok_or(())?;
        if end > dst.len() {
            return Err(());
        }
        encode_serialized_value(value, payload_len, &mut dst[body_offset..end]);
        body_offset = end;
    }
    if value_iter.next().is_some() {
        return Err(());
    }
    if body_offset != dst.len() {
        return Err(());
    }
    Ok(())
}

/// Compute the exact total record size (header + body) for a value iterator.
///
/// The returned size matches what [`serialize_record_iter_into`] would write
/// given the same values. This lets callers pre-reserve an exact-sized
/// destination slot (e.g. in a page buffer) before calling
/// [`serialize_record_iter_into_slice`].
///
/// Requires a cloneable iterator because size measurement is a separate pass
/// from encoding.
#[must_use]
pub fn record_iter_exact_size<'a, I>(values: I) -> usize
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let mut header_content_size = 0usize;
    let mut body_size = 0usize;
    for value in values {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_content_size = header_content_size.saturating_add(varint_len(serial_type));
        body_size = body_size.saturating_add(payload_len);
    }
    let header_size = compute_header_size(header_content_size);
    header_size.saturating_add(body_size)
}

/// Serialize a record using the generic encoder, writing directly into a
/// caller-provided exact-sized byte slice.
///
/// The slice length MUST equal the value returned by
/// [`record_iter_exact_size`] for the same value sequence.
///
/// On success returns `Ok(())`. On size mismatch or encoder anomaly returns
/// `Err(())`; the partial state of `dst` is unspecified on error.
#[allow(clippy::result_unit_err)]
pub fn serialize_record_iter_into_slice<'a, I>(values: I, dst: &mut [u8]) -> Result<(), ()>
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    let mut header_content_size = 0usize;
    let mut body_size = 0usize;
    for value in values.clone() {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_content_size = header_content_size
            .checked_add(varint_len(serial_type))
            .ok_or(())?;
        body_size = body_size.checked_add(payload_len).ok_or(())?;
    }
    let header_size = compute_header_size(header_content_size);
    let total_size = header_size.checked_add(body_size).ok_or(())?;
    if dst.len() != total_size {
        return Err(());
    }

    let mut header_offset = write_varint(dst, u64::try_from(header_size).unwrap_or(u64::MAX));
    let mut body_offset = header_size;

    for value in values {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_offset += write_varint(&mut dst[header_offset..], serial_type);
        let body_end = body_offset.checked_add(payload_len).ok_or(())?;
        if body_end > dst.len() {
            return Err(());
        }
        encode_serialized_value(value, payload_len, &mut dst[body_offset..body_end]);
        body_offset = body_end;
    }
    if header_offset != header_size || body_offset != total_size {
        return Err(());
    }
    Ok(())
}

/// One-shot record serialization plan with precomputed serial-type layout.
#[derive(Debug)]
pub struct PlannedRecordSerialization<'a> {
    layouts: SmallVec<[(&'a SqliteValue, u64, usize); 16]>,
    header_content_size: usize,
    body_size: usize,
}

impl PlannedRecordSerialization<'_> {
    /// Return the exact record byte length this plan will write.
    #[must_use]
    pub fn exact_size(&self) -> usize {
        compute_header_size(self.header_content_size).saturating_add(self.body_size)
    }

    /// Write the planned record into an exact-sized destination slice.
    ///
    /// # Errors
    ///
    /// Returns `Err(())` if `dst.len()` differs from [`Self::exact_size`] or if
    /// arithmetic bounds checks fail while writing the record.
    #[allow(clippy::result_unit_err)]
    pub fn write_into_slice(self, dst: &mut [u8]) -> Result<(), ()> {
        let header_size = compute_header_size(self.header_content_size);
        let total_size = header_size.checked_add(self.body_size).ok_or(())?;
        if dst.len() != total_size {
            return Err(());
        }

        let mut header_offset = write_varint(dst, u64::try_from(header_size).unwrap_or(u64::MAX));
        let mut body_offset = header_size;
        for (value, serial_type, payload_len) in self.layouts {
            header_offset += write_varint(&mut dst[header_offset..], serial_type);
            let body_end = body_offset.checked_add(payload_len).ok_or(())?;
            if body_end > dst.len() {
                return Err(());
            }
            encode_serialized_value(value, payload_len, &mut dst[body_offset..body_end]);
            body_offset = body_end;
        }

        if header_offset != header_size || body_offset != total_size {
            return Err(());
        }
        Ok(())
    }
}

/// Precompute the serial types and payload widths needed to serialize a record.
///
/// Callers that need to reserve an exact destination slot before writing can
/// use [`PlannedRecordSerialization::exact_size`] and then consume the plan with
/// [`PlannedRecordSerialization::write_into_slice`], avoiding a second layout
/// pass and an intermediate `Vec<u8>` payload buffer.
#[must_use]
pub fn plan_record_iter_serialization<'a, I>(values: I) -> PlannedRecordSerialization<'a>
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let mut header_content_size = 0usize;
    let mut body_size = 0usize;
    let mut layouts: SmallVec<[(&'a SqliteValue, u64, usize); 16]> = SmallVec::new();

    for value in values {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_content_size = header_content_size.saturating_add(varint_len(serial_type));
        body_size = body_size.saturating_add(payload_len);
        layouts.push((value, serial_type, payload_len));
    }

    PlannedRecordSerialization {
        layouts,
        header_content_size,
        body_size,
    }
}

/// Build a reusable record-header template for value tuples whose serial types
/// all fit in a single-byte varint.
#[must_use]
pub fn try_build_runtime_precomputed_record_header(
    values: &[SqliteValue],
) -> Option<PrecomputedRecordHeader> {
    if values
        .iter()
        .any(|value| varint_len(serialized_value_layout(value).0) != 1)
    {
        return None;
    }

    Some(PrecomputedRecordHeader::new(&vec![
        PrecomputedSerialTypeKind::AnyOneByteVarintOrNull;
        values.len()
    ]))
}

/// Compute the total header size (including the header-size varint itself).
#[allow(clippy::cast_possible_truncation)]
fn compute_header_size(content_size: usize) -> usize {
    // If content plus the size byte is <= 127, the header-size varint is
    // exactly one byte. Most INSERT rows have only a few serial-type bytes.
    if content_size <= 126 {
        return content_size + 1;
    }

    // Start with a guess and iterate.
    let mut header_size = content_size + 1; // +1 for the minimum varint
    loop {
        let needed = varint_len(header_size as u64) + content_size;
        if needed <= header_size {
            return header_size;
        }
        header_size = needed;
    }
}

#[inline]
fn serialized_value_layout(value: &SqliteValue) -> (u64, usize) {
    match value {
        SqliteValue::Null => (0, 0),
        SqliteValue::Integer(i) => integer_serial_type_and_len(*i),
        // SQLite normalizes NaN to NULL for deterministic storage.
        SqliteValue::Float(f) => {
            if f.is_nan() {
                (0, 0)
            } else {
                (7, 8)
            }
        }
        SqliteValue::Text(s) => {
            let len = s.len();
            (
                serial_type_for_text(u64::try_from(len).unwrap_or(u64::MAX)),
                len,
            )
        }
        SqliteValue::Blob(b) => {
            let len = b.len();
            (
                serial_type_for_blob(u64::try_from(len).unwrap_or(u64::MAX)),
                len,
            )
        }
    }
}

fn serialize_record_iter_into_impl<'a, I>(values: I, buf: &mut Vec<u8>)
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    if values.size_hint().1.is_some_and(|len| len <= 2) {
        serialize_record_iter_into_impl_two_pass(values, buf);
    } else {
        serialize_record_iter_into_impl_cached_layout(values, buf);
    }
}

fn serialize_record_iter_into_impl_two_pass<'a, I>(values: I, buf: &mut Vec<u8>)
where
    I: Iterator<Item = &'a SqliteValue> + Clone,
{
    let mut header_content_size = 0usize;
    let mut body_size = 0usize;

    for value in values.clone() {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_content_size += varint_len(serial_type);
        body_size += payload_len;
    }

    let header_size = compute_header_size(header_content_size);
    let total_size = header_size + body_size;
    buf.clear();
    if buf.capacity() < total_size {
        buf.reserve(total_size - buf.capacity());
    }
    buf.resize(header_size, 0);

    let mut header_offset = write_varint(
        buf.as_mut_slice(),
        u64::try_from(header_size).unwrap_or(u64::MAX),
    );

    for value in values {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_offset += write_varint(&mut buf[header_offset..], serial_type);
        append_serialized_value(value, payload_len, buf);
    }

    debug_assert_eq!(header_offset, header_size);
    debug_assert_eq!(buf.len(), total_size);
}

fn serialize_record_iter_into_impl_cached_layout<'a, I>(values: I, buf: &mut Vec<u8>)
where
    I: Iterator<Item = &'a SqliteValue>,
{
    let mut header_content_size = 0usize;
    let mut body_size = 0usize;
    let mut layouts: SmallVec<[(&'a SqliteValue, u64, usize); 16]> = SmallVec::new();

    for value in values {
        let (serial_type, payload_len) = serialized_value_layout(value);
        header_content_size += varint_len(serial_type);
        body_size += payload_len;
        layouts.push((value, serial_type, payload_len));
    }

    write_serialized_record_from_layouts(layouts.into_iter(), header_content_size, body_size, buf);
}

fn write_serialized_record_from_layouts<'a, I>(
    layouts: I,
    header_content_size: usize,
    body_size: usize,
    buf: &mut Vec<u8>,
) where
    I: Iterator<Item = (&'a SqliteValue, u64, usize)>,
{
    let header_size = compute_header_size(header_content_size);
    let total_size = header_size + body_size;
    buf.clear();
    if buf.capacity() < total_size {
        buf.reserve(total_size - buf.capacity());
    }
    buf.resize(header_size, 0);

    let mut header_offset = write_varint(
        buf.as_mut_slice(),
        u64::try_from(header_size).unwrap_or(u64::MAX),
    );

    for (value, serial_type, payload_len) in layouts {
        header_offset += write_varint(&mut buf[header_offset..], serial_type);
        append_serialized_value(value, payload_len, buf);
    }

    debug_assert_eq!(header_offset, header_size);
    debug_assert_eq!(buf.len(), total_size);
}

/// Vectorized two-pass batch encoder for the SQLite record format.
///
/// MonetDB/X100-style columnar encoding: pass 1 computes per-row header and
/// body sizes (driving the varint-header/payload layout decisions), pass 2
/// writes all header varints and all payload bytes into `out` with no
/// per-row allocation. Each row's starting byte offset inside `out` is
/// recorded in `offsets`, allowing callers to slice out individual records.
///
/// **Byte-identical guarantee.** For any row `r`, the bytes in
/// `out[offsets[i]..offsets[i+1]]` (or `out[offsets[i]..]` for the final row)
/// are exactly what [`serialize_record`] would produce when invoked on
/// `rows[i]` alone.  This is enforced by a proptest and by the callers'
/// expectations for the SQLite record format — the on-disk layout is not
/// allowed to change.
///
/// The function never fails for valid input (the `Result` return type exists
/// for forward-compatibility with encoder backends that might reject oversize
/// headers; today all rows are accepted unconditionally).
///
/// # Parameters
///
/// - `rows`: a slice of row views, each row a slice of values.
/// - `out`: destination buffer. **Cleared** at entry and resized to hold all
///   records contiguously. Capacity is preserved.
/// - `offsets`: destination for per-row starting offsets.  **Cleared** at
///   entry. `offsets[i]` is the byte index of row `i` inside `out`.  The
///   total byte length of `out` after the call gives the record boundary for
///   the final row.
///
/// # Errors
///
/// Reserved for future backends; today always returns `Ok(())`.
pub fn encode_batch(
    rows: &[&[SqliteValue]],
    out: &mut Vec<u8>,
    offsets: &mut Vec<usize>,
) -> fsqlite_error::Result<()> {
    out.clear();
    offsets.clear();
    if rows.is_empty() {
        return Ok(());
    }
    offsets.reserve(rows.len());

    if encode_batch_integer_columns(rows, out, offsets) {
        return Ok(());
    }

    if encode_batch_homogeneous_into(rows, out, offsets) {
        return Ok(());
    }

    // ── Pass 1: measure header + body sizes per row, compute total size ─
    //
    // We store (header_size, body_size) per row in a column-oriented scratch
    // so pass 2 can stream writes without re-measuring. header_size already
    // includes the leading header-size varint.
    let mut sizes: Vec<(usize, usize)> = Vec::with_capacity(rows.len());
    let mut total_size: usize = 0;
    for row in rows {
        let mut header_content_size = 0usize;
        let mut body_size = 0usize;
        for value in row.iter() {
            let (serial_type, payload_len) = serialized_value_layout(value);
            header_content_size = header_content_size
                .checked_add(varint_len(serial_type))
                .ok_or_else(|| {
                    fsqlite_error::FrankenError::Internal(
                        "encode_batch: row header size overflow".to_owned(),
                    )
                })?;
            body_size = body_size.checked_add(payload_len).ok_or_else(|| {
                fsqlite_error::FrankenError::Internal(
                    "encode_batch: row body size overflow".to_owned(),
                )
            })?;
        }
        let header_size = compute_header_size(header_content_size);
        sizes.push((header_size, body_size));
        total_size = total_size
            .checked_add(header_size)
            .and_then(|t| t.checked_add(body_size))
            .ok_or_else(|| {
                fsqlite_error::FrankenError::Internal(
                    "encode_batch: total batch size overflow".to_owned(),
                )
            })?;
    }

    // Resize exactly once. Already-allocated capacity is preserved.
    out.resize(total_size, 0);

    // ── Pass 2: write header varints and payload bytes row-by-row ──────
    //
    // We keep two running cursors (header_offset, body_offset) per row, but
    // unlike the scalar path these live in a shared buffer whose layout was
    // pre-decided in pass 1. This is the X100-style "materialize after the
    // plan is fixed" pattern; in hot INSERT workloads it removes per-row
    // Vec growth from `resize` and lets the inner encoder loop issue a
    // single large write to `out` rather than touching the allocator.
    let mut row_start: usize = 0;
    for (row, &(header_size, body_size)) in rows.iter().zip(sizes.iter()) {
        offsets.push(row_start);
        let row_total = header_size + body_size;
        let row_slice = &mut out[row_start..row_start + row_total];

        // Write the leading header-size varint (sized to describe the whole
        // header including itself, just like the scalar encoder).
        let mut header_offset =
            write_varint(row_slice, u64::try_from(header_size).unwrap_or(u64::MAX));
        let mut body_offset = header_size;

        for value in row.iter() {
            let (serial_type, payload_len) = serialized_value_layout(value);
            header_offset += write_varint(&mut row_slice[header_offset..], serial_type);
            encode_serialized_value(
                value,
                payload_len,
                &mut row_slice[body_offset..body_offset + payload_len],
            );
            body_offset += payload_len;
        }

        debug_assert_eq!(header_offset, header_size);
        debug_assert_eq!(body_offset, row_total);
        row_start += row_total;
    }
    debug_assert_eq!(row_start, total_size);

    Ok(())
}

fn encode_batch_integer_columns(
    rows: &[&[SqliteValue]],
    out: &mut Vec<u8>,
    offsets: &mut Vec<usize>,
) -> bool {
    if rows.len() < ENCODE_BATCH_HOMOGENEOUS_MIN_ROWS {
        return false;
    }

    let Some(first_row) = rows.first() else {
        return false;
    };
    let column_count = first_row.len();
    if column_count == 0 {
        return false;
    }
    for row in rows {
        if row.len() != column_count
            || row
                .iter()
                .any(|value| !matches!(value, SqliteValue::Integer(_)))
        {
            return false;
        }
    }

    let layout_count = match rows.len().checked_mul(column_count) {
        Some(count) => count,
        None => return false,
    };
    let mut body_sizes = vec![0usize; rows.len()];
    let mut layouts = vec![IntegerEncoding::default(); layout_count];
    let use_simd = avx2_available();

    for column_idx in 0..column_count {
        let mut row_idx = 0usize;
        while row_idx + 4 <= rows.len() {
            let mut values = [0_i64; 4];
            for lane in 0..4 {
                let SqliteValue::Integer(value) = &rows[row_idx + lane][column_idx] else {
                    return false;
                };
                values[lane] = *value;
            }

            let encoded = classify_integer_block(values, use_simd);
            for (lane, layout) in encoded.into_iter().enumerate() {
                let target_row = row_idx + lane;
                layouts[target_row * column_count + column_idx] = layout;
                body_sizes[target_row] =
                    match body_sizes[target_row].checked_add(usize::from(layout.payload_len)) {
                        Some(size) => size,
                        None => return false,
                    };
            }
            row_idx += 4;
        }

        while row_idx < rows.len() {
            let SqliteValue::Integer(value) = &rows[row_idx][column_idx] else {
                return false;
            };
            let layout = scalar_integer_encoding(*value);
            layouts[row_idx * column_count + column_idx] = layout;
            body_sizes[row_idx] =
                match body_sizes[row_idx].checked_add(usize::from(layout.payload_len)) {
                    Some(size) => size,
                    None => return false,
                };
            row_idx += 1;
        }
    }

    let header_size = compute_header_size(column_count);
    let mut total_size = 0usize;
    for body_size in &body_sizes {
        let Some(row_total) = header_size.checked_add(*body_size) else {
            return false;
        };
        total_size = match total_size.checked_add(row_total) {
            Some(size) => size,
            None => return false,
        };
    }

    out.resize(total_size, 0);
    let mut row_start = 0usize;
    for (row_idx, row) in rows.iter().enumerate() {
        offsets.push(row_start);
        let row_total = header_size + body_sizes[row_idx];
        let row_slice = &mut out[row_start..row_start + row_total];
        let mut header_offset =
            write_varint(row_slice, u64::try_from(header_size).unwrap_or(u64::MAX));

        for column_idx in 0..column_count {
            let layout = layouts[row_idx * column_count + column_idx];
            row_slice[header_offset] = layout.serial_type;
            header_offset += 1;
        }

        let mut body_offset = header_size;
        for column_idx in 0..column_count {
            let SqliteValue::Integer(value) = &row[column_idx] else {
                return false;
            };
            let payload_len = usize::from(layouts[row_idx * column_count + column_idx].payload_len);
            let body_end = body_offset + payload_len;
            write_integer_payload(*value, payload_len, &mut row_slice[body_offset..body_end]);
            body_offset = body_end;
        }

        debug_assert_eq!(header_offset, header_size);
        debug_assert_eq!(body_offset, row_total);
        row_start += row_total;
    }
    debug_assert_eq!(row_start, total_size);
    true
}

/// Minimum batch size for the homogeneous fast path. Below this, the
/// existing `encode_batch` is already cheap enough that the homogeneity
/// probe overhead is not amortized.
const ENCODE_BATCH_HOMOGENEOUS_MIN_ROWS: usize = 16;

fn collect_row_serial_layouts(row: &[SqliteValue]) -> SmallVec<[(u64, usize); 16]> {
    row.iter().map(serialized_value_layout).collect()
}

fn row_matches_serial_layouts(row: &[SqliteValue], layouts: &[(u64, usize)]) -> bool {
    row.len() == layouts.len()
        && row
            .iter()
            .zip(layouts.iter())
            .all(|(value, layout)| serialized_value_layout(value) == *layout)
}

/// Cheap homogeneity probe — O(R·C) equality check of each row's
/// (serial_type, payload_len) tuple against the first row.
///
/// Returns `true` iff all rows have identical column counts and identical
/// `(serial_type, payload_len)` at every column position. The SQLite
/// record format encodes TEXT/BLOB length into the serial type itself, so
/// identical serial types already imply identical payload lengths for
/// those classes; we return the pair for clarity and to keep the fast
/// path honest if the layout definition ever shifts.
///
/// Returns `false` for fewer than two rows (no comparison possible —
/// callers should use the slow path, which handles degenerate sizes
/// uniformly).
#[must_use]
pub fn rows_have_identical_serial_types(rows: &[&[SqliteValue]]) -> bool {
    if rows.len() < 2 {
        return false;
    }
    let template_layouts = collect_row_serial_layouts(rows[0]);
    rows[1..]
        .iter()
        .all(|row| row_matches_serial_layouts(row, &template_layouts))
}

/// Homogeneous-batch fast path for `encode_batch`.
///
/// Returns `None` if the batch is heterogeneous, empty, or below the
/// size threshold `ENCODE_BATCH_HOMOGENEOUS_MIN_ROWS`. Otherwise
/// computes the record header ONCE and `memcpy`s it into every row
/// slot, then writes each row's body bytes in a tight loop amenable
/// to auto-vectorization.
///
/// Byte-identical to `encode_batch`'s `out` payload.
///
/// # Invariant
///
/// SQLite serial types for TEXT (`>= 13`, odd) and BLOB (`>= 12`, even)
/// encode their byte length directly. Two rows with "same TEXT column
/// but different string lengths" therefore have DIFFERENT serial types
/// and [`rows_have_identical_serial_types`] correctly returns `false`.
/// This fast path is only valid when every row's payload layout is
/// bit-for-bit identical in shape.
#[must_use]
pub fn encode_batch_homogeneous(rows: &[&[SqliteValue]]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut offsets = Vec::new();
    encode_batch_homogeneous_into(rows, &mut out, &mut offsets).then_some(out)
}

fn encode_batch_homogeneous_into(
    rows: &[&[SqliteValue]],
    out: &mut Vec<u8>,
    offsets: &mut Vec<usize>,
) -> bool {
    if rows.len() < ENCODE_BATCH_HOMOGENEOUS_MIN_ROWS {
        return false;
    }
    let template_layouts = collect_row_serial_layouts(rows[0]);
    if rows[1..]
        .iter()
        .any(|row| !row_matches_serial_layouts(row, &template_layouts))
    {
        return false;
    }

    // Compute header content size and body size once.
    let mut header_content_size = 0usize;
    let mut body_size = 0usize;
    for &(serial_type, payload_len) in &template_layouts {
        let Some(next_header_content_size) =
            header_content_size.checked_add(varint_len(serial_type))
        else {
            return false;
        };
        let Some(next_body_size) = body_size.checked_add(payload_len) else {
            return false;
        };
        header_content_size = next_header_content_size;
        body_size = next_body_size;
    }
    let header_size = compute_header_size(header_content_size);
    let Some(row_total) = header_size.checked_add(body_size) else {
        return false;
    };
    let Some(total_size) = row_total.checked_mul(rows.len()) else {
        return false;
    };

    // Build the single reusable header template.
    let mut header_template = SmallVec::<[u8; 64]>::new();
    header_template.resize(header_size, 0);
    let mut hoff = write_varint(
        header_template.as_mut_slice(),
        u64::try_from(header_size).unwrap_or(u64::MAX),
    );
    for &(serial_type, _) in &template_layouts {
        hoff += write_varint(&mut header_template[hoff..], serial_type);
    }
    debug_assert_eq!(hoff, header_size);

    // Allocate the full output, memcpy the header into each row slot,
    // then write bodies in a tight loop.
    out.resize(total_size, 0);
    offsets.reserve(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let row_start = i * row_total;
        offsets.push(row_start);
        let header_end = row_start + header_size;
        out[row_start..header_end].copy_from_slice(&header_template);

        let mut body_offset = header_end;
        for (value, &(_, payload_len)) in row.iter().zip(template_layouts.iter()) {
            let end = body_offset + payload_len;
            encode_serialized_value(value, payload_len, &mut out[body_offset..end]);
            body_offset = end;
        }
        debug_assert_eq!(body_offset, row_start + row_total);
    }

    true
}

/// Fresh-buffer convenience variant of [`encode_batch`].
///
/// Returns a freshly allocated `Vec<u8>` matching [`encode_batch`]'s
/// `out` payload byte-for-byte. Call [`encode_batch`] directly when row
/// offsets are needed alongside the payload.
///
/// # Errors
///
/// Propagates any error from [`encode_batch`] (today none).
pub fn encode_batch_auto(rows: &[&[SqliteValue]]) -> fsqlite_error::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut offsets = Vec::new();
    encode_batch(rows, &mut out, &mut offsets)?;
    Ok(out)
}

/// Decode a value from its serial type and raw bytes.
///
/// Public so that record-offset-table consumers can perform lazy
/// per-column decoding without re-parsing the header.
#[allow(clippy::cast_possible_truncation)]
pub fn decode_value(serial_type: u64, bytes: &[u8], profile_enabled: bool) -> Option<SqliteValue> {
    match classify_serial_type(serial_type) {
        // Serial types 10 and 11 are reserved for SQLite internal use and
        // are not expected in well-formed database files. Their serialized
        // length is nevertheless zero in SQLite's internal size table; the
        // tolerant value decoder maps that zero-length payload to NULL.
        SerialTypeClass::Null | SerialTypeClass::Reserved => {
            return Some(profile_decoded_value(SqliteValue::Null, profile_enabled));
        }
        SerialTypeClass::Zero => {
            return Some(profile_decoded_value(
                SqliteValue::Integer(0),
                profile_enabled,
            ));
        }
        SerialTypeClass::One => {
            return Some(profile_decoded_value(
                SqliteValue::Integer(1),
                profile_enabled,
            ));
        }
        SerialTypeClass::Integer => {
            return Some(profile_decoded_value(
                SqliteValue::Integer(decode_big_endian_signed(bytes)),
                profile_enabled,
            ));
        }
        SerialTypeClass::Float => {
            if bytes.len() != 8 {
                return None;
            }
            let bits = u64::from_be_bytes(bytes.try_into().ok()?);
            let value = f64::from_bits(bits);
            return Some(profile_decoded_value(
                if value.is_nan() {
                    SqliteValue::Null
                } else {
                    SqliteValue::Float(value)
                },
                profile_enabled,
            ));
        }
        SerialTypeClass::Text | SerialTypeClass::Blob => {}
    }

    let mut slot = pool_acquire().unwrap_or(SqliteValue::Null);
    decode_value_into(serial_type, bytes, &mut slot, profile_enabled)?;
    Some(slot)
}

#[inline]
fn profile_decoded_value(value: SqliteValue, profile_enabled: bool) -> SqliteValue {
    if profile_enabled {
        note_decoded_value(&value);
    }
    value
}

#[inline]
fn replace_decoded_slot(slot: &mut SqliteValue, value: SqliteValue) {
    let old_value = std::mem::replace(slot, value);
    pool_return_reusable(old_value);
}

fn decode_value_into(
    serial_type: u64,
    bytes: &[u8],
    slot: &mut SqliteValue,
    profile_enabled: bool,
) -> Option<()> {
    match classify_serial_type(serial_type) {
        // Reserved serial types (10, 11): decode the zero-length internal
        // code as NULL. See `decode_value` for the rationale.
        SerialTypeClass::Null | SerialTypeClass::Reserved => {
            replace_decoded_slot(slot, SqliteValue::Null);
        }
        SerialTypeClass::Zero => {
            replace_decoded_slot(slot, SqliteValue::Integer(0));
        }
        SerialTypeClass::One => {
            replace_decoded_slot(slot, SqliteValue::Integer(1));
        }
        SerialTypeClass::Integer => {
            replace_decoded_slot(slot, SqliteValue::Integer(decode_big_endian_signed(bytes)));
        }
        SerialTypeClass::Float => {
            if bytes.len() != 8 {
                return None;
            }
            let bits = u64::from_be_bytes(bytes.try_into().ok()?);
            let value = f64::from_bits(bits);
            replace_decoded_slot(
                slot,
                if value.is_nan() {
                    SqliteValue::Null
                } else {
                    SqliteValue::Float(value)
                },
            );
        }
        SerialTypeClass::Text => {
            // Fast path: if the slot already holds a TEXT value whose bytes
            // exactly match the incoming record bytes, we can skip the
            // `from_utf8` validation entirely — the existing slot contents
            // are UTF-8 by construction, and equal bytes means the incoming
            // bytes are equally valid. This is the common case when a
            // `SqliteValue` slot is reused across rows that happen to share
            // a TEXT column value (e.g. repeated enum-like strings, or when
            // the VDBE decodes the same row back into the same register).
            //
            // `as_bytes_direct` avoids the internal `from_utf8` cost of
            // `SmallText::as_str` for the inline variant.
            if let SqliteValue::Text(existing) = slot {
                if existing.as_bytes_direct() == bytes {
                    if profile_enabled {
                        note_decoded_value(slot);
                    }
                    return Some(());
                }
                // Bytes differ: validate the incoming bytes as UTF-8 once,
                // then overwrite the slot reusing its heap allocation when
                // possible.
                //
                // OPT-UTF8: simdutf8::basic::from_utf8 is a SIMD-accelerated
                // drop-in for std::str::from_utf8, typically 3-10x faster on
                // ASCII/majority-ASCII payloads which dominate TEXT columns
                // in real workloads.
                let text = simdutf8::basic::from_utf8(bytes).ok()?;
                existing.overwrite(text);
                if profile_enabled {
                    note_decoded_value(slot);
                }
                return Some(());
            }
            // Slot did not hold a TEXT value — validate once and construct
            // a fresh `SqliteValue::Text`.
            let text = simdutf8::basic::from_utf8(bytes).ok()?;
            replace_decoded_slot(slot, SqliteValue::Text(text.into()));
        }
        SerialTypeClass::Blob => {
            if let SqliteValue::Blob(existing) = slot {
                if existing.as_ref() == bytes {
                    if profile_enabled {
                        note_decoded_value(slot);
                    }
                    return Some(());
                }
                if existing.len() == bytes.len()
                    && let Some(existing_bytes) = Arc::get_mut(existing)
                {
                    existing_bytes.copy_from_slice(bytes);
                    if profile_enabled {
                        note_decoded_value(slot);
                    }
                    return Some(());
                }
            }
            replace_decoded_slot(slot, SqliteValue::Blob(Arc::from(bytes)));
        }
    }

    if profile_enabled {
        note_decoded_value(slot);
    }
    Some(())
}

/// Decode a big-endian signed integer of 1-8 bytes.
#[allow(clippy::cast_possible_wrap)]
fn decode_big_endian_signed(bytes: &[u8]) -> i64 {
    match bytes.len() {
        0 => 0,
        1 => bytes[0] as i8 as i64,
        2 => i16::from_be_bytes(bytes.try_into().unwrap()) as i64,
        3 => {
            let mut buf = [if bytes[0] & 0x80 != 0 { 0xFF } else { 0 }; 4];
            buf[1..4].copy_from_slice(bytes);
            i32::from_be_bytes(buf) as i64
        }
        4 => i32::from_be_bytes(bytes.try_into().unwrap()) as i64,
        6 => {
            let mut buf = [if bytes[0] & 0x80 != 0 { 0xFF } else { 0 }; 8];
            buf[2..8].copy_from_slice(bytes);
            i64::from_be_bytes(buf)
        }
        8 => i64::from_be_bytes(bytes.try_into().unwrap()),
        _ => {
            let negative = bytes.first().is_some_and(|&b| b & 0x80 != 0);
            let mut value: u64 = if negative { u64::MAX } else { 0 };
            for &b in bytes {
                value = (value << 8) | u64::from(b);
            }
            value as i64
        }
    }
}

/// Encode a `SqliteValue` into its serial type byte representation.
#[inline]
fn encode_serialized_value(value: &SqliteValue, payload_len: usize, buf: &mut [u8]) {
    debug_assert_eq!(buf.len(), payload_len);
    match value {
        SqliteValue::Null => {} // serial type 0: no data
        SqliteValue::Integer(i) => {
            if payload_len == 0 {
                return;
            }
            let bytes = i.to_be_bytes();
            // Take the least significant `payload_len` bytes.
            buf.copy_from_slice(&bytes[8 - payload_len..]);
        }
        SqliteValue::Float(f) => {
            if f.is_nan() {
                return;
            }
            let bits = f.to_bits();
            buf.copy_from_slice(&bits.to_be_bytes());
        }
        SqliteValue::Text(s) => {
            buf.copy_from_slice(s.as_bytes_direct());
        }
        SqliteValue::Blob(b) => {
            buf.copy_from_slice(b);
        }
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;
    use crate::value::SmallText;

    struct RecordProfileThreadOverrideGuard {
        previous: Option<bool>,
    }

    impl RecordProfileThreadOverrideGuard {
        fn enabled() -> Self {
            let previous = record_profile_thread_override();
            set_record_profile_thread_override(Some(true));
            Self { previous }
        }
    }

    impl Drop for RecordProfileThreadOverrideGuard {
        fn drop(&mut self) {
            set_record_profile_thread_override(self.previous);
        }
    }

    #[test]
    fn empty_record() {
        let data = serialize_record(&[]);
        assert!(!data.is_empty());
        let values = parse_record(&data).unwrap();
        assert!(values.is_empty());
    }

    /// Regression for the bug that wedged the live MCP Agent Mail backend.
    ///
    /// Canonical SQLite's `sqlite3VdbeSerialTypeLen` treats serial types 10
    /// and 11 as zero-length payloads (the `aSize[]` table in vdbeaux.c
    /// has both as 0). Records containing these types appear in real
    /// databases despite the spec calling 10/11 "reserved for internal
    /// use" — they round-trip without complaint under canonical SQLite.
    ///
    /// Prior to the fix, fsqlite-types::serial_type_len returned `None`
    /// for 10/11, which made `parse_record_into` bail with the cursor-
    /// layer error `"malformed index key record while extracting rowid"`.
    /// That mismatch between the bespoke parser and canonical SQLite was
    /// the root of the futile-recovery spin loop observed at runtime.
    ///
    /// This test hand-builds a record with the header column-type sequence
    /// `[text-len-0, type-10, type-11, int8]` and asserts:
    ///   1. The record parses cleanly (no Option::None / DatabaseCorrupt).
    ///   2. Types 10 and 11 decode to `SqliteValue::Null` — matching the
    ///      canonical "treat reserved as zero-length" behavior.
    ///   3. The trailing 1-byte integer decodes correctly, proving the
    ///      record scanner advances past zero-length reserved columns
    ///      rather than getting wedged on them.
    #[test]
    fn record_with_reserved_serial_types_10_and_11_parses_as_null() {
        // Hand-built record bytes:
        //   header_size (varint=5)
        //   col 0: serial 13 (text, len 0)
        //   col 1: serial 10 (reserved, len 0)
        //   col 2: serial 11 (reserved, len 0)
        //   col 3: serial 1  (8-bit signed integer)
        // body:
        //   col 0: <0 bytes>
        //   col 1: <0 bytes>
        //   col 2: <0 bytes>
        //   col 3: 0x2A (= 42i8)
        let record: [u8; 6] = [0x05, 0x0D, 0x0A, 0x0B, 0x01, 0x2A];

        let values = parse_record(&record).expect(
            "record containing serial types 10/11 must parse cleanly; \
             returning None here was the spin-loop trigger fixed by \
             aligning serial_type_len with canonical SQLite's aSize table",
        );
        assert_eq!(values.len(), 4, "expected 4 columns, got {values:?}");

        // Column 0: empty text.
        let SqliteValue::Text(text) = &values[0] else {
            assert!(
                matches!(values[0], SqliteValue::Text(_)),
                "col 0 should be Text(\"\"), got {:?}",
                values[0]
            );
            return;
        };
        assert_eq!(
            text.as_str(),
            "",
            "empty-text column should decode as the empty string"
        );

        // Column 1: serial type 10 → NULL.
        assert!(
            matches!(values[1], SqliteValue::Null),
            "serial type 10 must decode as NULL (canonical aSize[10]=0); got {:?}",
            values[1]
        );
        // Column 2: serial type 11 → NULL.
        assert!(
            matches!(values[2], SqliteValue::Null),
            "serial type 11 must decode as NULL (canonical aSize[11]=0); got {:?}",
            values[2]
        );

        // Column 3: 1-byte signed integer 42, proving the parser advanced
        // past the two zero-length reserved columns without consuming any
        // body bytes.
        assert!(
            matches!(values[3], SqliteValue::Integer(42)),
            "trailing int8 must decode as 42; got {:?} — parser advanced \
             incorrectly past types 10/11",
            values[3]
        );

        // Also verify the slot-reusing path (parse_record_into) so we know
        // the fix landed in both decode entry points.
        let mut slots = Vec::new();
        parse_record_into(&record, &mut slots)
            .expect("parse_record_into must also tolerate reserved types");
        assert_eq!(slots.len(), 4);
        assert!(matches!(slots[1], SqliteValue::Null));
        assert!(matches!(slots[2], SqliteValue::Null));
        assert!(matches!(slots[3], SqliteValue::Integer(42)));
    }

    // -----------------------------------------------------------------------
    // OPT-A2: zero-copy slice-based serializer smoke tests.
    // -----------------------------------------------------------------------

    #[test]
    fn opt_a2_slice_serializer_matches_vec_for_empty() {
        let values: [SqliteValue; 0] = [];
        let via_vec = serialize_record(&values);
        let exact = record_iter_exact_size(values.iter());
        assert_eq!(exact, via_vec.len());
        let mut dst = vec![0u8; exact];
        serialize_record_iter_into_slice(values.iter(), dst.as_mut_slice()).unwrap();
        assert_eq!(dst, via_vec);
    }

    #[test]
    fn opt_a2_slice_serializer_matches_vec_for_ints() {
        let values = [
            SqliteValue::Integer(0),
            SqliteValue::Integer(-1),
            SqliteValue::Integer(127),
            SqliteValue::Integer(128),
            SqliteValue::Integer(i64::MAX),
            SqliteValue::Integer(i64::MIN),
        ];
        let via_vec = serialize_record(&values);
        let exact = record_iter_exact_size(values.iter());
        assert_eq!(exact, via_vec.len());
        let mut dst = vec![0u8; exact];
        serialize_record_iter_into_slice(values.iter(), dst.as_mut_slice()).unwrap();
        assert_eq!(dst, via_vec);
    }

    #[test]
    fn opt_a2_slice_serializer_matches_vec_for_mixed() {
        let values = [
            SqliteValue::Null,
            SqliteValue::Integer(42),
            SqliteValue::Float(1.5),
            SqliteValue::Text(SmallText::new("hello")),
            SqliteValue::Blob(Arc::from(&[1u8, 2, 3, 4][..])),
        ];
        let via_vec = serialize_record(&values);
        let exact = record_iter_exact_size(values.iter());
        assert_eq!(exact, via_vec.len());
        let mut dst = vec![0u8; exact];
        serialize_record_iter_into_slice(values.iter(), dst.as_mut_slice()).unwrap();
        assert_eq!(dst, via_vec);

        // Round-trip parse.
        let parsed = parse_record(&dst).unwrap();
        assert_eq!(parsed.len(), 5);
        assert!(parsed[0].is_null());
        assert_eq!(parsed[1].as_integer(), Some(42));
    }

    #[test]
    fn planned_record_serialization_matches_vec_for_large_mixed_record() {
        let values = [
            SqliteValue::Null,
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Float(2.25),
            SqliteValue::Text(SmallText::new(
                "large text payload that forces the generic header planner path",
            )),
            SqliteValue::Blob(Arc::from(&[9u8, 8, 7, 6, 5, 4, 3, 2][..])),
        ];
        let expected = serialize_record(&values);
        let plan = plan_record_iter_serialization(values.iter());
        assert_eq!(plan.exact_size(), expected.len());

        let mut dst = vec![0u8; plan.exact_size()];
        plan.write_into_slice(dst.as_mut_slice())
            .expect("planned slice serialize must succeed");
        assert_eq!(dst, expected);
        assert_eq!(parse_record(&dst).unwrap(), values);
    }

    #[test]
    fn opt_a2_slice_serializer_rejects_size_mismatch() {
        let values = [SqliteValue::Integer(1), SqliteValue::Integer(2)];
        let exact = record_iter_exact_size(values.iter());
        // Too small.
        let mut too_small = vec![0u8; exact - 1];
        assert!(serialize_record_iter_into_slice(values.iter(), too_small.as_mut_slice()).is_err());
        // Too large.
        let mut too_large = vec![0u8; exact + 1];
        assert!(serialize_record_iter_into_slice(values.iter(), too_large.as_mut_slice()).is_err());
    }

    #[test]
    fn opt_a2_precomputed_header_slice_serializer_matches_vec() {
        let values = [
            SqliteValue::Integer(10),
            SqliteValue::Integer(-5),
            SqliteValue::Integer(0),
        ];
        let header = try_build_runtime_precomputed_record_header(&values)
            .expect("all-integer values should yield a 1-byte-varint header");

        let exact = record_iter_with_precomputed_header_exact_size(values.iter(), &header)
            .expect("exact size available for header-matching values");
        let mut dst = vec![0u8; exact];
        serialize_record_iter_with_precomputed_header_into_slice(
            values.iter(),
            &header,
            dst.as_mut_slice(),
        )
        .expect("slice serialize must succeed");

        let mut via_vec = Vec::new();
        assert!(serialize_record_iter_with_precomputed_header_into(
            values.iter(),
            &header,
            &mut via_vec,
        ));
        assert_eq!(dst, via_vec);
    }

    #[test]
    fn opt_a2_precomputed_header_vec_serializer_reuses_capacity_without_tail() {
        let values = [
            SqliteValue::Integer(127),
            SqliteValue::Text(SmallText::new("abc")),
            SqliteValue::Blob(Arc::from([0xDE_u8, 0xAD, 0xBE].as_slice())),
            SqliteValue::Null,
        ];
        let header = PrecomputedRecordHeader::new(&[
            PrecomputedSerialTypeKind::AnyOneByteVarintOrNull,
            PrecomputedSerialTypeKind::AnyOneByteVarintOrNull,
            PrecomputedSerialTypeKind::AnyOneByteVarintOrNull,
            PrecomputedSerialTypeKind::AnyOneByteVarintOrNull,
        ]);
        assert_eq!(header.column_count(), values.len());
        let expected = serialize_record(&values);
        let exact = record_iter_with_precomputed_header_exact_size(values.iter(), &header)
            .expect("exact size available for one-byte-varint values");
        assert_eq!(exact, expected.len());

        let mut buf = vec![0xA5; 1024];
        let capacity = buf.capacity();
        assert!(serialize_record_iter_with_precomputed_header_into(
            values.iter(),
            &header,
            &mut buf,
        ));

        assert_eq!(buf, expected);
        assert_eq!(buf.len(), exact);
        assert_eq!(buf.capacity(), capacity);
        assert_eq!(parse_record(&buf).unwrap(), values);
    }

    #[test]
    fn opt_a2_precomputed_header_single_slot_vec_serializer_matches_generic() {
        let header =
            PrecomputedRecordHeader::new(&[PrecomputedSerialTypeKind::AnyOneByteVarintOrNull]);
        let cases = [
            SqliteValue::Null,
            SqliteValue::Integer(0),
            SqliteValue::Integer(127),
            SqliteValue::Float(1.25),
            SqliteValue::Text(SmallText::new("abc")),
            SqliteValue::Blob(Arc::from([0xCA_u8, 0xFE].as_slice())),
        ];

        for value in cases {
            let values = [value];
            let expected = serialize_record(&values);
            let mut buf = vec![0xA5; 32];
            let capacity = buf.capacity();
            assert!(serialize_record_iter_with_precomputed_header_into(
                values.iter(),
                &header,
                &mut buf,
            ));
            assert_eq!(buf, expected);
            assert_eq!(buf.len(), expected.len());
            assert_eq!(buf.capacity(), capacity);
            assert_eq!(parse_record(&buf).unwrap(), values.as_slice());
        }
    }

    #[test]
    fn opt_a2_precomputed_header_append_serializer_clears_partial_buffer_on_reject() {
        let kinds = vec![PrecomputedSerialTypeKind::IntegerOrNull; 17];
        let header = PrecomputedRecordHeader::new(&kinds);
        let mut mismatched_values = vec![SqliteValue::Integer(1); 16];
        mismatched_values.push(SqliteValue::Text(SmallText::new("not an integer")));
        let mut buf = vec![0xA5; 64];

        assert!(
            !serialize_record_iter_with_precomputed_header_into(
                mismatched_values.iter(),
                &header,
                &mut buf,
            ),
            "append fallback should reject values that do not match the header contract"
        );
        assert!(
            buf.is_empty(),
            "failed append fallback must not leave a partial record in the caller buffer"
        );

        let extra_values = vec![SqliteValue::Integer(1); 18];
        buf.extend_from_slice(b"stale");
        assert!(
            !serialize_record_iter_with_precomputed_header_into(
                extra_values.iter(),
                &header,
                &mut buf,
            ),
            "append fallback should reject extra values after serializing all header slots"
        );
        assert!(
            buf.is_empty(),
            "extra-value rejection must also clear the partially serialized record"
        );
    }

    #[test]
    fn null_record() {
        let values = vec![SqliteValue::Null];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].is_null());
    }

    #[test]
    fn integer_zero() {
        let values = vec![SqliteValue::Integer(0)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(0));
    }

    #[test]
    fn integer_one() {
        let values = vec![SqliteValue::Integer(1)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(1));
    }

    #[test]
    fn parse_record_into_decodes_text_correctly() {
        let first = serialize_record(&[SqliteValue::Text(SmallText::new("hello reusable buffer"))]);
        let second = serialize_record(&[SqliteValue::Text(SmallText::new("shorter"))]);
        let mut values = Vec::new();

        parse_record_into(&first, &mut values).expect("first decode");
        assert_eq!(values[0].as_text(), Some("hello reusable buffer"));

        parse_record_into(&second, &mut values).expect("second decode");
        assert_eq!(values[0].as_text(), Some("shorter"));
    }

    #[test]
    fn decode_text_fast_path_equal_bytes_preserves_value() {
        // Regression guard for the OPT-6 fast path: when the slot already
        // holds a TEXT value whose bytes equal the incoming record bytes,
        // decoding must succeed without rewriting the slot — and the
        // resulting `&str` view must be identical to the input.
        let original = "round-trip test string";
        let mut slot = SqliteValue::Text(SmallText::new(original));

        let serial_type = serial_type_for_text(original.len() as u64);
        decode_value_into(serial_type, original.as_bytes(), &mut slot, false)
            .expect("decode via equal-bytes fast path");

        assert_eq!(slot.as_text(), Some(original));

        // Exercise again with an inline-sized string (tests SmallText::Inline
        // repr + `as_bytes_direct` alignment with the incoming record bytes).
        let short = "abc";
        let mut slot = SqliteValue::Text(SmallText::new(short));
        decode_value_into(
            serial_type_for_text(short.len() as u64),
            short.as_bytes(),
            &mut slot,
            false,
        )
        .expect("decode inline text via equal-bytes fast path");
        assert_eq!(slot.as_text(), Some(short));
    }

    #[test]
    fn decode_text_rejects_invalid_utf8_when_slot_differs() {
        // Regression guard: even when the fast-path equality check fails,
        // the subsequent `from_utf8` validation must still reject malformed
        // bytes. This covers the non-fast-path write into an existing Text
        // slot.
        let mut slot = SqliteValue::Text(SmallText::new("previous value"));
        let invalid: &[u8] = &[0xFF, 0xFE, 0xFD]; // not valid UTF-8
        let result = decode_value_into(serial_type_for_text(3), invalid, &mut slot, false);
        assert!(result.is_none(), "invalid UTF-8 must be rejected");
    }

    #[test]
    fn decode_text_rejects_invalid_utf8_when_slot_non_text() {
        // Regression guard: the `replace_decoded_slot` arm (slot held a
        // non-Text value) must also run `from_utf8` validation before
        // constructing a new `SqliteValue::Text`.
        let mut slot = SqliteValue::Integer(7);
        let invalid: &[u8] = &[0xFF, 0xFE, 0xFD];
        let result = decode_value_into(serial_type_for_text(3), invalid, &mut slot, false);
        assert!(result.is_none(), "invalid UTF-8 must be rejected");
    }

    #[test]
    fn parse_record_into_decodes_blob_correctly() {
        let first = serialize_record(&[SqliteValue::Blob(Arc::from(
            [1u8, 2, 3, 4, 5, 6, 7, 8].as_slice(),
        ))]);
        let second = serialize_record(&[SqliteValue::Blob(Arc::from([9u8, 10, 11].as_slice()))]);
        let mut values = Vec::new();

        parse_record_into(&first, &mut values).expect("first decode");
        assert_eq!(values[0].as_blob(), Some(&[1u8, 2, 3, 4, 5, 6, 7, 8][..]));

        parse_record_into(&second, &mut values).expect("second decode");
        assert_eq!(values[0].as_blob(), Some(&[9u8, 10, 11][..]));
    }

    #[test]
    fn decode_value_reuses_unique_blob_buffer_for_same_length_payload() {
        let mut slot = SqliteValue::Blob(Arc::from([1_u8, 2, 3, 4].as_slice()));
        let SqliteValue::Blob(existing) = &slot else {
            assert!(matches!(&slot, SqliteValue::Blob(_)));
            return;
        };
        let original_ptr = Arc::as_ptr(existing);

        decode_value_into(serial_type_for_blob(4), &[9_u8, 8, 7, 6], &mut slot, false)
            .expect("decode succeeds");

        assert!(matches!(&slot, SqliteValue::Blob(_)));
        let updated = if let SqliteValue::Blob(updated) = &slot {
            updated
        } else {
            return;
        };
        assert_eq!(Arc::as_ptr(updated), original_ptr);
        assert_eq!(updated.as_ref(), &[9_u8, 8, 7, 6]);
    }

    #[test]
    fn decode_column_from_offset_reuse_reuses_matching_text_hint() {
        let text = "this text is long enough to force shared heap storage";
        let record = serialize_record(&[SqliteValue::Text(SmallText::new(text))]);
        let mut offsets = Vec::new();
        assert_eq!(parse_record_header_into(&record, &mut offsets), Some(1));

        let hint = SqliteValue::Text(SmallText::from_arc(Arc::<str>::from(text)));
        let SqliteValue::Text(existing) = &hint else {
            assert!(matches!(&hint, SqliteValue::Text(_)));
            return;
        };
        let original_ptr = existing.as_str().as_ptr();

        let decoded = decode_column_from_offset_reuse(&record, &offsets[0], Some(&hint), false)
            .expect("decode succeeds");
        assert!(matches!(&decoded, SqliteValue::Text(_)));
        let reused = if let SqliteValue::Text(reused) = decoded {
            reused
        } else {
            return;
        };
        assert_eq!(reused.as_str().as_ptr(), original_ptr);
        assert_eq!(reused.as_str(), text);
    }

    #[test]
    fn decode_column_from_offset_reuse_reuses_matching_blob_hint() {
        let blob = [0xDE_u8, 0xAD, 0xBE, 0xEF, 0xFA, 0xCE];
        let record = serialize_record(&[SqliteValue::Blob(Arc::from(blob.as_slice()))]);
        let mut offsets = Vec::new();
        assert_eq!(parse_record_header_into(&record, &mut offsets), Some(1));

        let hint = SqliteValue::Blob(Arc::from(blob.as_slice()));
        let SqliteValue::Blob(existing) = &hint else {
            assert!(matches!(&hint, SqliteValue::Blob(_)));
            return;
        };
        let original_ptr = Arc::as_ptr(existing);

        let decoded = decode_column_from_offset_reuse(&record, &offsets[0], Some(&hint), false)
            .expect("decode succeeds");
        assert!(matches!(&decoded, SqliteValue::Blob(_)));
        let reused = if let SqliteValue::Blob(reused) = decoded {
            reused
        } else {
            return;
        };
        assert_eq!(Arc::as_ptr(&reused), original_ptr);
        assert_eq!(reused.as_ref(), blob.as_slice());
    }

    #[test]
    fn projected_column_offsets_match_full_header_parse() {
        let record = serialize_record(&[
            SqliteValue::Integer(11),
            SqliteValue::Text(SmallText::new("skip me")),
            SqliteValue::Float(2.5),
        ]);
        let mut offsets = Vec::new();
        assert_eq!(parse_record_header_into(&record, &mut offsets), Some(3));

        let projected = parse_record_projected_column_offsets(&record, 2, Some(0))
            .expect("projected header parse should succeed");
        assert_eq!(projected.column_count, 3);
        assert_eq!(projected.primary, Some(offsets[2]));
        assert_eq!(projected.secondary, Some(offsets[0]));
        assert_eq!(
            decode_numeric_column_from_offset(&record, &projected.primary.unwrap()),
            Some(NumericColumnValue::Float(2.5))
        );
    }

    #[test]
    fn numeric_column_decode_classifies_text_blob_without_materializing() {
        let record = serialize_record(&[
            SqliteValue::Text(SmallText::new("123")),
            SqliteValue::Blob(Arc::from([1_u8, 2, 3].as_slice())),
        ]);
        let projected = parse_record_projected_column_offsets(&record, 0, Some(1))
            .expect("projected header parse should succeed");

        assert_eq!(
            decode_numeric_column_from_offset(&record, &projected.primary.unwrap()),
            Some(NumericColumnValue::NonNumeric)
        );
        assert_eq!(
            decode_numeric_column_from_offset(&record, &projected.secondary.unwrap()),
            Some(NumericColumnValue::NonNumeric)
        );
    }

    #[test]
    fn invalidate_returns_reusable_values_to_pool() {
        use crate::value::{pool_clear, pool_len};

        pool_clear();
        let mut scratch = RecordDecodeScratch::default();
        scratch.cache_decoded(
            0,
            SqliteValue::Text("hello world this is a long pooled string".into()),
        );
        scratch.cache_decoded(1, SqliteValue::Blob(Arc::from([0xCA_u8, 0xFE].as_slice())));

        assert_eq!(pool_len(), 0);
        scratch.invalidate();

        assert_eq!(pool_len(), 2);
        pool_clear();
    }

    #[test]
    fn record_decode_scratch_reuses_small_record_state() {
        let record = serialize_record(&[
            SqliteValue::Integer(7),
            SqliteValue::Text(SmallText::new("scratch-owned")),
        ]);
        let mut scratch = RecordDecodeScratch::default();

        let eager = scratch
            .prepare_for_record(&record)
            .expect("record layout should decode");
        assert!(!eager);
        assert_eq!(scratch.column_count(), 2);
        assert!(!scratch.cached_value_ready(1));

        let decoded = decode_column_from_offset(
            &record,
            scratch
                .column_offset(1)
                .expect("column offset should exist"),
            false,
        )
        .expect("column should decode");
        scratch.cache_decoded(1, decoded);

        assert!(scratch.cached_value_ready(1));
        assert_eq!(
            scratch.cached_value(1).and_then(SqliteValue::as_text),
            Some("scratch-owned")
        );

        scratch.invalidate();
        assert!(scratch.is_empty());
        assert!(!scratch.cached_value_ready(1));
        assert!(scratch.cached_value(1).is_some_and(SqliteValue::is_null));
        assert_eq!(
            scratch.values.len(),
            2,
            "invalidate should preserve narrow-record value slots for reuse"
        );
    }

    #[test]
    fn record_decode_scratch_eagerly_materializes_wide_records() {
        let values: Vec<_> = (0_i64..65).map(SqliteValue::Integer).collect();
        let record = serialize_record(&values);
        let mut scratch = RecordDecodeScratch::default();

        let eager = scratch
            .prepare_for_record(&record)
            .expect("wide record layout should decode");
        assert!(eager);
        assert_eq!(scratch.column_count(), 65);
        assert!(scratch.cached_value_ready(64));
        assert_eq!(
            scratch.cached_value(64).and_then(SqliteValue::as_integer),
            Some(64)
        );
    }

    #[test]
    fn parse_record_header_prefix_accepts_truncated_body() {
        let record = serialize_record(&[
            SqliteValue::Integer(42),
            SqliteValue::Text(SmallText::new("hello")),
            SqliteValue::Blob(Arc::from([1_u8, 2, 3, 4].as_slice())),
        ]);
        let (header_size_u64, _) = read_varint(&record).expect("record header should parse");
        let header_size = usize::try_from(header_size_u64).expect("header size should fit");
        let prefix = &record[..=header_size];

        let mut offsets = Vec::new();
        let count = parse_record_header_prefix_into(prefix, &mut offsets)
            .expect("prefix parser should accept truncated body");

        assert_eq!(count, 3);
        assert_eq!(offsets[0].body_offset as usize, header_size);
        assert_eq!(offsets[1].body_offset as usize, header_size + 1);
        assert_eq!(offsets[2].body_offset as usize, header_size + 6);
    }

    #[test]
    fn integer_small() {
        let values = vec![SqliteValue::Integer(42)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(42));
    }

    #[test]
    fn integer_negative() {
        let values = vec![SqliteValue::Integer(-1)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(-1));
    }

    #[test]
    fn integer_large() {
        for &val in &[i64::MIN, i64::MAX, 0x7FFF_FFFF_FFFF, -0x7FFF_FFFF_FFFF] {
            let values = vec![SqliteValue::Integer(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(
                parsed[0].as_integer(),
                Some(val),
                "roundtrip failed for {val}"
            );
        }
    }

    #[test]
    fn float_value() {
        let values = vec![SqliteValue::Float(3.14)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_float(), Some(3.14));
    }

    #[test]
    fn float_special_values() {
        for &val in &[0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY] {
            let values = vec![SqliteValue::Float(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(parsed[0].as_float().unwrap().to_bits(), val.to_bits());
        }
    }

    #[test]
    fn float_nan_normalized_to_null_for_storage() {
        let values = vec![SqliteValue::Float(f64::NAN)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].is_null());
    }

    #[test]
    fn text_value() {
        let values = vec![SqliteValue::Text(SmallText::new("hello world"))];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_text(), Some("hello world"));
    }

    #[test]
    fn text_empty() {
        let values = vec![SqliteValue::Text(SmallText::new(""))];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_text(), Some(""));
    }

    #[test]
    fn blob_value() {
        let values = vec![SqliteValue::Blob(Arc::from(
            [0xDEu8, 0xAD, 0xBE, 0xEF].as_slice(),
        ))];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_blob(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn blob_empty() {
        let values = vec![SqliteValue::Blob(Arc::from(&[] as &[u8]))];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_blob(), Some(&[][..]));
    }

    #[test]
    fn mixed_record() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::Text(SmallText::new("hello")),
            SqliteValue::Null,
            SqliteValue::Float(2.718),
            SqliteValue::Blob(Arc::from([1u8, 2, 3].as_slice())),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
        ];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();

        assert_eq!(parsed.len(), 7);
        assert_eq!(parsed[0].as_integer(), Some(42));
        assert_eq!(parsed[1].as_text(), Some("hello"));
        assert!(parsed[2].is_null());
        assert_eq!(parsed[3].as_float(), Some(2.718));
        assert_eq!(parsed[4].as_blob(), Some(&[1, 2, 3][..]));
        assert_eq!(parsed[5].as_integer(), Some(0));
        assert_eq!(parsed[6].as_integer(), Some(1));
    }

    #[test]
    fn test_serial_type_roundtrip_all_categories() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(i64::from(i8::MIN)),
            SqliteValue::Integer(i64::from(i16::MIN)),
            SqliteValue::Integer(-8_388_608), // 24-bit boundary
            SqliteValue::Integer(i64::from(i32::MIN)),
            SqliteValue::Integer(-140_737_488_355_328), // 48-bit boundary
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Float(-1234.5),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Blob(Arc::from([0xDEu8, 0xAD, 0xBE, 0xEF].as_slice())),
            SqliteValue::Text(SmallText::new("serial-type-text")),
        ];

        let encoded = serialize_record(&values);
        let parsed = parse_record(&encoded).expect("record must decode");
        assert_eq!(parsed.len(), values.len());

        assert!(parsed[0].is_null());
        assert_eq!(parsed[1].as_integer(), Some(i64::from(i8::MIN)));
        assert_eq!(parsed[2].as_integer(), Some(i64::from(i16::MIN)));
        assert_eq!(parsed[3].as_integer(), Some(-8_388_608));
        assert_eq!(parsed[4].as_integer(), Some(i64::from(i32::MIN)));
        assert_eq!(parsed[5].as_integer(), Some(-140_737_488_355_328));
        assert_eq!(parsed[6].as_integer(), Some(i64::MIN));
        assert_eq!(
            parsed[7].as_float().map(f64::to_bits),
            Some((-1234.5f64).to_bits())
        );
        assert_eq!(parsed[8].as_integer(), Some(0));
        assert_eq!(parsed[9].as_integer(), Some(1));
        assert_eq!(parsed[10].as_blob(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
        assert_eq!(parsed[11].as_text(), Some("serial-type-text"));
    }

    #[test]
    fn integer_size_boundaries() {
        // Verify that integers at size boundaries roundtrip correctly.
        let test_values: &[i64] = &[
            0,
            1,
            -1,
            127,
            -128,
            128,
            -129,
            32767,
            -32768,
            32768,
            8_388_607,
            -8_388_608,
            8_388_608,
            2_147_483_647,
            -2_147_483_648,
            2_147_483_648,
            0x0000_7FFF_FFFF_FFFF,
            -0x0000_8000_0000_0000,
            0x0000_8000_0000_0000,
            i64::MAX,
            i64::MIN,
        ];

        for &val in test_values {
            let values = vec![SqliteValue::Integer(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(
                parsed[0].as_integer(),
                Some(val),
                "roundtrip failed for {val}"
            );
        }
    }

    #[test]
    fn decode_big_endian_signed_cases() {
        assert_eq!(decode_big_endian_signed(&[]), 0);
        assert_eq!(decode_big_endian_signed(&[42]), 42);
        assert_eq!(decode_big_endian_signed(&[0xFF]), -1);
        assert_eq!(decode_big_endian_signed(&[0x00, 0x80]), 128);
        assert_eq!(decode_big_endian_signed(&[0xFF, 0x7F]), -129);
    }

    #[test]
    fn malformed_record_too_short() {
        // Header says it's 10 bytes but we only have 2.
        let data = [10, 0];
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn malformed_record_body_truncated() {
        // Header has serial type 6 (8-byte int) but body is empty.
        let data = [2, 6]; // header_size=2, serial_type=6, body missing
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn large_text_roundtrip() {
        let big_text = "x".repeat(10_000);
        let values = vec![SqliteValue::Text(SmallText::new(big_text.as_str()))];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed[0].as_text(), Some(big_text.as_str()));
    }

    #[test]
    fn header_size_computation() {
        // For small records, header size varint is 1 byte.
        assert_eq!(compute_header_size(0), 1);
        assert_eq!(compute_header_size(1), 2);
        assert_eq!(compute_header_size(126), 127);
        // At 127+, the header size varint becomes 2 bytes.
        assert_eq!(compute_header_size(127), 129);
    }

    #[test]
    fn parse_empty_data_is_malformed() {
        assert!(parse_record(&[]).is_none());
    }

    #[test]
    fn malformed_record_invalid_utf8_text() {
        let data = [0x02, 0x0F, 0xFF];
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn malformed_record_rejects_trailing_body_bytes() {
        let data = [0x02, 0x01, 0x2A, 0x63];
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn record_column_count_matches_encoded_values() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(42),
            SqliteValue::Text(SmallText::new("hello")),
            SqliteValue::Blob(Arc::from([1u8, 2, 3].as_slice())),
        ];
        let data = serialize_record(&values);
        assert_eq!(record_column_count(&data), Some(values.len()));
    }

    #[test]
    fn test_record_format_null_vector() {
        let data = serialize_record(&[SqliteValue::Null]);
        assert_eq!(data, vec![0x02, 0x00]);
    }

    #[test]
    fn test_record_format_int8_vector() {
        let data = serialize_record(&[SqliteValue::Integer(42)]);
        assert_eq!(data, vec![0x02, 0x01, 0x2A]);
    }

    #[test]
    fn test_record_format_int16_vector() {
        let data = serialize_record(&[SqliteValue::Integer(256)]);
        assert_eq!(data, vec![0x02, 0x02, 0x01, 0x00]);
    }

    #[test]
    fn test_record_format_int64_vector() {
        let value = 0x0102_0304_0506_0708_i64;
        let data = serialize_record(&[SqliteValue::Integer(value)]);
        assert_eq!(
            data,
            vec![0x02, 0x06, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_record_format_float_vector() {
        let data = serialize_record(&[SqliteValue::Float(3.14)]);
        assert_eq!(
            data,
            vec![0x02, 0x07, 0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F]
        );
    }

    #[test]
    fn test_record_format_zero_one_constants_vectors() {
        let zero = serialize_record(&[SqliteValue::Integer(0)]);
        assert_eq!(zero, vec![0x02, 0x08]);

        let one = serialize_record(&[SqliteValue::Integer(1)]);
        assert_eq!(one, vec![0x02, 0x09]);
    }

    #[test]
    fn test_record_format_text_blob_vectors() {
        let text = serialize_record(&[SqliteValue::Text(SmallText::new("hello"))]);
        assert_eq!(text, vec![0x02, 0x17, 0x68, 0x65, 0x6C, 0x6C, 0x6F]);

        let blob = serialize_record(&[SqliteValue::Blob(Arc::from([0xCAu8, 0xFE].as_slice()))]);
        assert_eq!(blob, vec![0x02, 0x10, 0xCA, 0xFE]);
    }

    #[test]
    fn test_record_format_worked_example_exact_bytes() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::Text(SmallText::new("hello")),
            SqliteValue::Float(3.14),
            SqliteValue::Null,
            SqliteValue::Blob(Arc::from([0xCAu8, 0xFE].as_slice())),
        ];
        let data = serialize_record(&values);
        let expected = vec![
            0x06, 0x01, 0x17, 0x07, 0x00, 0x10, 0x2A, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x40, 0x09,
            0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F, 0xCA, 0xFE,
        ];
        assert_eq!(data, expected);
        assert_eq!(data.len(), 22);
    }

    #[test]
    fn test_record_header_size_includes_self_for_one_and_ten_columns() {
        let one_col = serialize_record(&[SqliteValue::Integer(42)]);
        let (one_header_size, one_consumed) = read_varint(&one_col).expect("valid one-col header");
        assert_eq!(one_consumed, 1);
        assert_eq!(one_header_size, 2);

        let ten_values: Vec<SqliteValue> = (2_i64..=11).map(SqliteValue::Integer).collect();
        let ten_col = serialize_record(&ten_values);
        let (ten_header_size, ten_consumed) = read_varint(&ten_col).expect("valid ten-col header");
        assert_eq!(ten_consumed, 1);
        assert_eq!(ten_header_size, 11);
        assert!(ten_col[1..11].iter().all(|&serial| serial == 0x01));
    }

    // -----------------------------------------------------------------------
    // bd-2sm1 §17.2 proptest: record format round-trip
    // -----------------------------------------------------------------------

    use proptest::prelude::*;

    /// Strategy to generate an arbitrary `SqliteValue`.
    fn arb_sqlite_value() -> BoxedStrategy<SqliteValue> {
        prop_oneof![
            5 => Just(SqliteValue::Null),
            10 => any::<i64>().prop_map(SqliteValue::Integer),
            5 => prop_oneof![
                Just(0_i64),
                Just(1_i64),
                Just(-1_i64),
                Just(127_i64),
                Just(-128_i64),
                Just(128_i64),
                Just(32767_i64),
                Just(-32768_i64),
                Just(8_388_607_i64),
                Just(-8_388_608_i64),
                Just(2_147_483_647_i64),
                Just(-2_147_483_648_i64),
                Just(i64::MAX),
                Just(i64::MIN),
            ].prop_map(SqliteValue::Integer),
            // Exclude NaN/Inf since they roundtrip but NaN != NaN in PartialEq.
            5 => (-1e15_f64..1e15_f64).prop_map(SqliteValue::Float),
            5 => prop_oneof![
                Just(0.0_f64),
                Just(-0.0_f64),
                Just(1.0_f64),
                Just(-1.0_f64),
                Just(3.14_f64),
                Just(f64::MAX),
                Just(f64::MIN),
                Just(f64::MIN_POSITIVE),
            ].prop_map(SqliteValue::Float),
            10 => "[a-zA-Z0-9 _]{0,200}".prop_map(|s: String| SqliteValue::Text(SmallText::from_string(s))),
            5 => proptest::collection::vec(any::<u8>(), 0..200)
                .prop_map(|v: Vec<u8>| SqliteValue::Blob(Arc::from(v.as_slice()))),
        ]
        .boxed()
    }

    /// Bitwise equality for SqliteValue (handles NaN and -0.0 correctly).
    fn values_bitwise_eq(a: &SqliteValue, b: &SqliteValue) -> bool {
        match (a, b) {
            (SqliteValue::Null, SqliteValue::Null) => true,
            (SqliteValue::Integer(x), SqliteValue::Integer(y)) => x == y,
            (SqliteValue::Float(x), SqliteValue::Float(y)) => x.to_bits() == y.to_bits(),
            (SqliteValue::Text(x), SqliteValue::Text(y)) => x == y,
            (SqliteValue::Blob(x), SqliteValue::Blob(y)) => x == y,
            _ => false,
        }
    }

    fn arb_integer_record_values() -> BoxedStrategy<Vec<SqliteValue>> {
        prop::collection::vec(
            prop_oneof![
                10 => any::<i64>(),
                1 => Just(0_i64),
                1 => Just(1_i64),
                1 => Just(-1_i64),
                1 => Just(127_i64),
                1 => Just(-128_i64),
                1 => Just(128_i64),
                1 => Just(-129_i64),
                1 => Just(32_767_i64),
                1 => Just(-32_768_i64),
                1 => Just(32_768_i64),
                1 => Just(-32_769_i64),
                1 => Just(8_388_607_i64),
                1 => Just(-8_388_608_i64),
                1 => Just(8_388_608_i64),
                1 => Just(-8_388_609_i64),
                1 => Just(2_147_483_647_i64),
                1 => Just(-2_147_483_648_i64),
                1 => Just(2_147_483_648_i64),
                1 => Just(-2_147_483_649_i64),
                1 => Just(140_737_488_355_327_i64),
                1 => Just(-140_737_488_355_328_i64),
                1 => Just(140_737_488_355_328_i64),
                1 => Just(-140_737_488_355_329_i64),
                1 => Just(i64::MAX),
                1 => Just(i64::MIN),
            ]
            .prop_map(SqliteValue::Integer),
            0..160,
        )
        .boxed()
    }

    fn arb_boundary_length_sqlite_value() -> BoxedStrategy<SqliteValue> {
        prop_oneof![
            2 => Just(SqliteValue::Null),
            4 => prop_oneof![
                Just(0_i64),
                Just(1_i64),
                Just(-1_i64),
                Just(127_i64),
                Just(-128_i64),
                Just(128_i64),
                Just(32_767_i64),
                Just(-32_768_i64),
                Just(i64::MAX),
                Just(i64::MIN),
            ].prop_map(SqliteValue::Integer),
            2 => prop_oneof![
                Just(0.0_f64),
                Just(-0.0_f64),
                Just(1.0_f64),
                Just(-1.0_f64),
                Just(f64::MIN_POSITIVE),
            ].prop_map(SqliteValue::Float),
            5 => prop_oneof![
                Just(0_usize),
                Just(1_usize),
                Just(2_usize),
                Just(56_usize),
                Just(57_usize),
                Just(58_usize),
                Just(59_usize),
                Just(120_usize),
                Just(121_usize),
                Just(122_usize),
            ]
            .prop_map(|len| SqliteValue::Text(SmallText::from_string("x".repeat(len)))),
            5 => prop_oneof![
                Just(0_usize),
                Just(1_usize),
                Just(2_usize),
                Just(56_usize),
                Just(57_usize),
                Just(58_usize),
                Just(59_usize),
                Just(120_usize),
                Just(121_usize),
                Just(122_usize),
            ]
            .prop_map(|len| SqliteValue::Blob(Arc::from(vec![0xAB; len].as_slice()))),
        ]
        .boxed()
    }

    #[test]
    fn simd_integer_record_matches_scalar_record_bytes() {
        let row = vec![
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Integer(-128),
            SqliteValue::Integer(32_767),
            SqliteValue::Integer(32_768),
            SqliteValue::Integer(8_388_607),
            SqliteValue::Integer(8_388_608),
            SqliteValue::Integer(i64::MIN),
        ];

        let mut fast = Vec::new();
        assert!(simd_serialize_integer_record(row.iter(), &mut fast));
        assert_eq!(fast, serialize_record(&row));
    }

    #[test]
    fn simd_integer_record_scalar_fallback_matches_scalar_record_bytes() {
        let row = vec![
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Integer(-128),
            SqliteValue::Integer(32_767),
            SqliteValue::Integer(32_768),
            SqliteValue::Integer(8_388_607),
            SqliteValue::Integer(8_388_608),
            SqliteValue::Integer(i64::MIN),
        ];

        let mut scalar_fallback = Vec::new();
        assert!(serialize_integer_record_with_classifier(
            row.iter(),
            &mut scalar_fallback,
            false,
        ));
        assert_eq!(scalar_fallback, serialize_record(&row));
    }

    #[test]
    fn simd_integer_record_rejects_non_integer_rows_without_mutating_buffer() {
        let row = vec![
            SqliteValue::Integer(7),
            SqliteValue::Text("not-an-integer".into()),
            SqliteValue::Integer(9),
        ];

        let mut fast = Vec::from([0xAA, 0xBB]);
        assert!(!simd_serialize_integer_record(row.iter(), &mut fast));
        assert_eq!(fast, vec![0xAA, 0xBB]);
    }

    #[test]
    fn simd_integer_record_handles_empty_rows() {
        let row = Vec::<SqliteValue>::new();

        let mut fast = Vec::new();
        assert!(simd_serialize_integer_record(row.iter(), &mut fast));
        assert_eq!(fast, serialize_record(&row));
    }

    #[test]
    fn simd_integer_record_handles_large_headers() {
        let row = (0_i64..140).map(SqliteValue::Integer).collect::<Vec<_>>();

        let mut fast = Vec::new();
        assert!(simd_serialize_integer_record(row.iter(), &mut fast));
        assert_eq!(fast, serialize_record(&row));
    }

    #[test]
    fn record_profile_scope_breakdown_tracks_and_restores_nested_scopes() {
        let _record_profile_guard = RecordProfileThreadOverrideGuard::enabled();
        reset_record_profile();

        let encoded = serialize_record(&[
            SqliteValue::Integer(7),
            SqliteValue::Text(SmallText::new("alpha")),
        ]);

        {
            let _outer = enter_record_profile_scope(RecordProfileScope::CoreConnection);
            let _ = parse_record(&encoded).expect("core decode should succeed");
            {
                let _inner = enter_record_profile_scope(RecordProfileScope::VdbeEngine);
                let _ = parse_record_column(&encoded, 1).expect("column decode should succeed");
            }
            let mut scratch = Vec::new();
            parse_record_into(&encoded, &mut scratch).expect("reused decode should succeed");
        }

        let snapshot = record_profile_snapshot();
        // The scoped counters below are owned entirely by this test. Aggregate
        // process-wide counters can legitimately move under parallel test
        // execution while profiling is enabled, so the assertions stay focused
        // on the scope breakdown we are exercising here.
        assert_eq!(
            snapshot
                .callsite_breakdown
                .core_connection
                .parse_record_calls,
            1
        );
        assert_eq!(
            snapshot
                .callsite_breakdown
                .core_connection
                .parse_record_into_calls,
            2
        );
        assert_eq!(
            snapshot
                .callsite_breakdown
                .vdbe_engine
                .parse_record_column_calls,
            1
        );
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2000))]

        /// INV-PBT-3: Record encode/decode round-trip for arbitrary value vectors.
        #[test]
        fn prop_record_roundtrip_arbitrary(
            values in proptest::collection::vec(arb_sqlite_value(), 0..100)
        ) {
            let encoded = serialize_record(&values);
            let decoded = parse_record(&encoded)
                .expect("serialize_record output must always parse");
            prop_assert_eq!(
                values.len(),
                decoded.len(),
                "bead_id=bd-2sm1 case=record_roundtrip_len_mismatch expected={} got={}",
                values.len(),
                decoded.len()
            );
            for (i, (orig, parsed)) in values.iter().zip(decoded.iter()).enumerate() {
                prop_assert!(
                    values_bitwise_eq(orig, parsed),
                    "bead_id=bd-2sm1 case=record_roundtrip_value_mismatch col={} orig={:?} parsed={:?}",
                    i,
                    orig,
                    parsed
                );
            }
        }

        /// INV-PBT-3: Edge cases at varint encoding boundaries.
        #[test]
        fn prop_record_roundtrip_varint_boundaries(
            n_cols in 1_usize..50
        ) {
            // Use values at varint boundaries to stress header encoding.
            let boundary_values: Vec<i64> = vec![
                0, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768,
                8_388_607, -8_388_608, 2_147_483_647, -2_147_483_648,
                i64::MAX, i64::MIN,
            ];
            let values: Vec<SqliteValue> = (0..n_cols)
                .map(|i| SqliteValue::Integer(boundary_values[i % boundary_values.len()]))
                .collect();
            let encoded = serialize_record(&values);
            let decoded = parse_record(&encoded).expect("valid boundary record");
            prop_assert_eq!(
                values.len(),
                decoded.len(),
                "bead_id=bd-2sm1 case=varint_boundary_len"
            );
            for (i, (orig, parsed)) in values.iter().zip(decoded.iter()).enumerate() {
                prop_assert!(
                    values_bitwise_eq(orig, parsed),
                    "bead_id=bd-2sm1 case=varint_boundary_mismatch col={}",
                    i
                );
            }
        }

        /// INV-PBT-3: Empty and single-value records.
        #[test]
        fn prop_record_roundtrip_single_value(value in arb_sqlite_value()) {
            let values = vec![value];
            let encoded = serialize_record(&values);
            let decoded = parse_record(&encoded).expect("valid single-value record");
            prop_assert_eq!(decoded.len(), 1, "bead_id=bd-2sm1 case=single_value_len");
            prop_assert!(
                values_bitwise_eq(&values[0], &decoded[0]),
                "bead_id=bd-2sm1 case=single_value_mismatch orig={:?} parsed={:?}",
                values[0],
                decoded[0]
            );
        }

        /// bd-7ij9b: Stress record-header varint boundaries where text/blob
        /// serial types cross from one-byte to two-byte header entries.
        #[test]
        fn prop_record_roundtrip_header_varint_boundaries(
            values in proptest::collection::vec(arb_boundary_length_sqlite_value(), 0..24)
        ) {
            let via_vec = serialize_record(&values);
            let exact = record_iter_exact_size(values.iter());
            prop_assert_eq!(
                exact,
                via_vec.len(),
                "bead_id=bd-7ij9b case=exact_size_mismatch exact={} vec_len={}",
                exact,
                via_vec.len()
            );

            let mut via_slice = vec![0u8; exact];
            serialize_record_iter_into_slice(values.iter(), via_slice.as_mut_slice())
                .expect("boundary-focused slice serialize must succeed");
            prop_assert_eq!(
                via_slice.as_slice(),
                via_vec.as_slice(),
                "bead_id=bd-7ij9b case=slice_vs_vec_mismatch values={:?}",
                values
            );

            let parsed = parse_record(&via_vec).expect("serialized record must parse");
            prop_assert_eq!(
                parsed.len(),
                values.len(),
                "bead_id=bd-7ij9b case=parse_len_mismatch expected={} got={}",
                values.len(),
                parsed.len()
            );

            let mut scratch = Vec::new();
            parse_record_into(&via_vec, &mut scratch).expect("serialized record must parse into scratch");
            prop_assert_eq!(
                scratch.len(),
                values.len(),
                "bead_id=bd-7ij9b case=parse_into_len_mismatch expected={} got={}",
                values.len(),
                scratch.len()
            );

            for (i, ((expected, decoded), reused)) in values
                .iter()
                .zip(parsed.iter())
                .zip(scratch.iter())
                .enumerate()
            {
                prop_assert!(
                    values_bitwise_eq(expected, decoded),
                    "bead_id=bd-7ij9b case=parse_mismatch col={} expected={expected:?} decoded={decoded:?}",
                    i
                );
                prop_assert!(
                    values_bitwise_eq(expected, reused),
                    "bead_id=bd-7ij9b case=parse_into_mismatch col={} expected={expected:?} decoded={reused:?}",
                    i
                );
            }
        }

        /// INV-PBT-4: Lazy header-only parse + per-column decode matches full parse.
        #[test]
        fn prop_lazy_decode_matches_full(
            values in proptest::collection::vec(arb_sqlite_value(), 1..50)
        ) {
            let encoded = serialize_record(&values);
            let full = parse_record(&encoded).expect("full parse must succeed");
            let mut offsets = Vec::new();
            let col_count = parse_record_header_into(&encoded, &mut offsets)
                .expect("header-only parse must succeed");
            prop_assert_eq!(col_count, full.len(), "column count mismatch");
            for i in 0..col_count {
                let lazy_val = decode_column_from_offset(&encoded, &offsets[i], false)
                    .expect("lazy decode must succeed");
                prop_assert!(
                    values_bitwise_eq(&full[i], &lazy_val),
                    "lazy decode mismatch at col {i}: full={:?} lazy={:?}",
                    full[i],
                    lazy_val,
                );
            }
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(10_000))]

        #[test]
        fn prop_simd_integer_record_matches_scalar(
            values in arb_integer_record_values()
        ) {
            let mut fast = Vec::new();
            prop_assert!(
                simd_serialize_integer_record(values.iter(), &mut fast),
                "integer-only rows must be accepted by the SIMD record serializer",
            );
            prop_assert_eq!(
                fast,
                serialize_record(&values),
                "SIMD/scalar record serialization diverged for values={:?}",
                values,
            );
        }
    }

    proptest::prop_compose! {
        fn arb_precomputed_header_case()
            (kind in 0_u8..3, i in any::<i64>(), f in any::<f64>())
            -> (PrecomputedSerialTypeKind, SqliteValue, SqliteValue)
        {
            match kind {
                0 => (
                    PrecomputedSerialTypeKind::NullPlaceholder,
                    SqliteValue::Integer(i),
                    SqliteValue::Null,
                ),
                1 => {
                    if i.rem_euclid(3) == 0 {
                        (
                            PrecomputedSerialTypeKind::IntegerOrNull,
                            SqliteValue::Null,
                            SqliteValue::Null,
                        )
                    } else {
                        let value = SqliteValue::Integer(i);
                        (
                            PrecomputedSerialTypeKind::IntegerOrNull,
                            value.clone(),
                            value,
                        )
                    }
                }
                _ => {
                    if i.rem_euclid(3) == 0 {
                        (
                            PrecomputedSerialTypeKind::RealOrNull,
                            SqliteValue::Null,
                            SqliteValue::Null,
                        )
                    } else {
                        let value = SqliteValue::Float(f);
                        (
                            PrecomputedSerialTypeKind::RealOrNull,
                            value.clone(),
                            value,
                        )
                    }
                }
            }
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(512))]

        #[test]
        fn prop_precomputed_header_matches_generic_record(
            cases in proptest::collection::vec(arb_precomputed_header_case(), 1..16)
        ) {
            let kinds: Vec<_> = cases.iter().map(|(kind, _, _)| *kind).collect();
            let source_values: Vec<_> = cases.iter().map(|(_, source, _)| source.clone()).collect();
            let generic_values: Vec<_> = cases.iter().map(|(_, _, generic)| generic.clone()).collect();
            let header = PrecomputedRecordHeader::new(&kinds);
            let mut encoded = Vec::new();

            prop_assert!(
                serialize_record_iter_with_precomputed_header_into(
                    source_values.iter(),
                    &header,
                    &mut encoded,
                ),
                "precomputed header should accept generated supported values",
            );

            prop_assert_eq!(
                encoded,
                serialize_record(&generic_values),
                "precomputed header encoding must match the generic record serializer",
            );
        }

        // -------------------------------------------------------------------
        // OPT-A2: slice-based serializers must produce byte-identical output
        // to the Vec-based serializers.  This is the correctness guarantee
        // that lets callers serialize directly into a page-buffer slot
        // without an intermediate `record_scratch: Vec<u8>`.
        // -------------------------------------------------------------------

        #[test]
        fn prop_serialize_record_iter_into_slice_matches_vec(
            values in proptest::collection::vec(arb_precomputed_header_case(), 0..16)
                .prop_map(|cases| cases.into_iter().map(|(_, _, g)| g).collect::<Vec<_>>())
        ) {
            // Generic encoder: Vec path vs slice path.
            let via_vec = serialize_record(&values);
            let exact = record_iter_exact_size(values.iter());
            prop_assert_eq!(exact, via_vec.len(),
                "record_iter_exact_size must match serialize_record length");

            let mut dst = vec![0u8; exact];
            serialize_record_iter_into_slice(values.iter(), dst.as_mut_slice())
                .expect("slice serialize must succeed at exact size");
            prop_assert_eq!(dst, via_vec,
                "slice-based generic serializer must produce byte-identical output");
        }

        #[test]
        fn prop_serialize_record_iter_with_precomputed_header_into_slice_matches_vec(
            cases in proptest::collection::vec(arb_precomputed_header_case(), 1..16)
        ) {
            let kinds: Vec<_> = cases.iter().map(|(kind, _, _)| *kind).collect();
            let source_values: Vec<_> = cases.iter().map(|(_, source, _)| source.clone()).collect();
            let header = PrecomputedRecordHeader::new(&kinds);

            // Measure then slice-serialize.
            let exact = record_iter_with_precomputed_header_exact_size(
                source_values.iter(),
                &header,
            ).expect("exact size for supported values");
            let mut dst = vec![0u8; exact];
            serialize_record_iter_with_precomputed_header_into_slice(
                source_values.iter(),
                &header,
                dst.as_mut_slice(),
            ).expect("precomputed-header slice serialize must succeed at exact size");

            // Compare against the Vec path.
            let mut via_vec = Vec::new();
            prop_assert!(serialize_record_iter_with_precomputed_header_into(
                source_values.iter(),
                &header,
                &mut via_vec,
            ));
            prop_assert_eq!(dst, via_vec,
                "precomputed-header slice-based serializer must produce byte-identical output");
        }
    }

    // -----------------------------------------------------------------------
    // bd-gieaf Track R: record encoding correctness, zero-alloc verification
    // -----------------------------------------------------------------------

    #[test]
    fn bd_gieaf_record_scratch_reuse_1000_records_no_realloc_after_warmup() {
        let mut scratch = Vec::<u8>::new();

        // Phase 1: Warmup — first 100 rows allow the buffer to grow to accommodate
        // the largest record shape in the workload. The blob column varies 1-32 bytes
        // based on row_id % 32, so we need ~32 rows minimum for full coverage.
        const WARMUP_ROWS: i64 = 100;
        const TOTAL_ROWS: i64 = 1000;

        let generate_row = |row_id: i64| -> Vec<SqliteValue> {
            vec![
                SqliteValue::Integer(row_id),
                SqliteValue::Integer(row_id.saturating_mul(17).saturating_sub(500)),
                SqliteValue::Float((row_id as f64).mul_add(1.25, -100.5)),
                SqliteValue::Text(SmallText::from_string(format!("row-{row_id:05}"))),
                SqliteValue::Blob(Arc::from(
                    (0_u8..((row_id as usize % 32) + 1) as u8).collect::<Vec<_>>(),
                )),
                if row_id % 5 == 0 {
                    SqliteValue::Null
                } else {
                    SqliteValue::Integer(row_id % 97 - 48)
                },
            ]
        };

        // Warmup phase: allow reallocations
        for row_id in 0_i64..WARMUP_ROWS {
            let values = generate_row(row_id);
            serialize_record_iter_into(values.iter(), &mut scratch);

            let fresh = serialize_record(&values);
            assert_eq!(
                scratch, fresh,
                "bead_id=bd-gieaf scenario=SCRATCH-REUSE row={row_id} scratch and fresh outputs must match"
            );
        }

        let capacity_after_warmup = scratch.capacity();
        let mut realloc_count_after_warmup = 0_usize;

        // Steady-state phase: verify zero reallocations
        for row_id in WARMUP_ROWS..TOTAL_ROWS {
            let values = generate_row(row_id);
            let capacity_before = scratch.capacity();

            serialize_record_iter_into(values.iter(), &mut scratch);

            if scratch.capacity() != capacity_before {
                realloc_count_after_warmup += 1;
            }

            let fresh = serialize_record(&values);
            assert_eq!(
                scratch, fresh,
                "bead_id=bd-gieaf scenario=SCRATCH-REUSE row={row_id} scratch and fresh outputs must match"
            );
        }

        eprintln!(
            "INFO bead_id=bd-gieaf scenario=SCRATCH-REUSE-1000 warmup_rows={} steady_rows={} capacity_after_warmup={} reallocs_after_warmup={} replay_command=\"cargo test -p fsqlite-types -- bd_gieaf --nocapture\"",
            WARMUP_ROWS,
            TOTAL_ROWS - WARMUP_ROWS,
            capacity_after_warmup,
            realloc_count_after_warmup
        );
        assert_eq!(
            realloc_count_after_warmup, 0,
            "bead_id=bd-gieaf scratch buffer should not reallocate after warmup phase"
        );
    }

    #[test]
    fn bd_gieaf_record_max_columns_128_roundtrip() {
        let values: Vec<SqliteValue> = (0_i64..128)
            .map(|i| match i % 5 {
                0 => SqliteValue::Null,
                1 => SqliteValue::Integer(i * 1000 + 7),
                2 => SqliteValue::Float((i as f64) * 3.14159),
                3 => SqliteValue::Text(SmallText::from_string(format!("col-{i:03}"))),
                _ => SqliteValue::Blob(Arc::from(vec![
                    (i % 256) as u8,
                    ((i + 1) % 256) as u8,
                    ((i + 2) % 256) as u8,
                ])),
            })
            .collect();

        let encoded = serialize_record(&values);
        let decoded = parse_record(&encoded).expect("128-column record must decode");

        assert_eq!(
            decoded.len(),
            128,
            "bead_id=bd-gieaf scenario=MAX-COLUMNS-128 column count mismatch"
        );
        for (i, (orig, parsed)) in values.iter().zip(decoded.iter()).enumerate() {
            assert!(
                values_bitwise_eq(orig, parsed),
                "bead_id=bd-gieaf scenario=MAX-COLUMNS-128 col={i} mismatch: orig={orig:?} parsed={parsed:?}"
            );
        }
        eprintln!(
            "INFO bead_id=bd-gieaf scenario=MAX-COLUMNS-128 columns=128 encoded_bytes={} replay_command=\"cargo test -p fsqlite-types -- bd_gieaf --nocapture\"",
            encoded.len()
        );
    }

    #[test]
    fn bd_gieaf_record_all_types_comprehensive_coverage() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Integer(-1),
            SqliteValue::Integer(127),
            SqliteValue::Integer(-128),
            SqliteValue::Integer(32767),
            SqliteValue::Integer(-32768),
            SqliteValue::Integer(8_388_607),
            SqliteValue::Integer(-8_388_608),
            SqliteValue::Integer(2_147_483_647),
            SqliteValue::Integer(-2_147_483_648),
            SqliteValue::Integer(i64::MAX),
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Float(0.0),
            SqliteValue::Float(-0.0),
            SqliteValue::Float(3.14159265358979),
            SqliteValue::Float(f64::MAX),
            SqliteValue::Float(f64::MIN_POSITIVE),
            SqliteValue::Text(SmallText::new("")),
            SqliteValue::Text(SmallText::new("hello")),
            SqliteValue::Text(SmallText::from_string("x".repeat(200))),
            SqliteValue::Blob(Arc::from(&[] as &[u8])),
            SqliteValue::Blob(Arc::from([0xDE_u8, 0xAD, 0xBE, 0xEF].as_slice())),
            SqliteValue::Blob(Arc::from(vec![0xCA_u8; 100])),
        ];

        let encoded = serialize_record(&values);
        let decoded = parse_record(&encoded).expect("all-types record must decode");

        assert_eq!(
            decoded.len(),
            values.len(),
            "bead_id=bd-gieaf scenario=ALL-TYPES column count mismatch"
        );
        for (i, (orig, parsed)) in values.iter().zip(decoded.iter()).enumerate() {
            assert!(
                values_bitwise_eq(orig, parsed),
                "bead_id=bd-gieaf scenario=ALL-TYPES col={i} mismatch: orig={orig:?} parsed={parsed:?}"
            );
        }
        eprintln!(
            "INFO bead_id=bd-gieaf scenario=ALL-TYPES columns={} encoded_bytes={} replay_command=\"cargo test -p fsqlite-types -- bd_gieaf --nocapture\"",
            values.len(),
            encoded.len()
        );
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(10_000))]

        #[test]
        fn bd_gieaf_prop_scratch_buffer_matches_fresh_allocation(
            values in proptest::collection::vec(arb_sqlite_value(), 0..50)
        ) {
            let mut scratch = Vec::new();
            serialize_record_iter_into(values.iter(), &mut scratch);
            let fresh = serialize_record(&values);

            prop_assert_eq!(
                scratch,
                fresh,
                "bead_id=bd-gieaf scenario=SCRATCH-MATCHES-FRESH scratch and fresh allocation must produce identical bytes"
            );
        }
    }

    // ── encode_batch byte-identical guarantee ─────────────────────────────
    //
    // encode_batch must produce the exact concatenation of per-row scalar
    // encodings for *any* mix of rows (heterogeneous arity, mixed types,
    // empty rows, nested extremes). If the output drifts by even one byte
    // the on-disk record format is violated, so this is a hard requirement,
    // not an optimization knob.

    fn concat_scalar_encodings(rows: &[Vec<SqliteValue>]) -> (Vec<u8>, Vec<usize>) {
        let mut bytes = Vec::new();
        let mut offsets = Vec::with_capacity(rows.len());
        for row in rows {
            offsets.push(bytes.len());
            let encoded = serialize_record(row);
            bytes.extend_from_slice(&encoded);
        }
        (bytes, offsets)
    }

    fn assert_encode_batch_matches_scalar(rows: &[Vec<SqliteValue>]) {
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        let mut out = Vec::new();
        let mut offsets = Vec::new();
        encode_batch(&row_refs, &mut out, &mut offsets).expect("encode_batch must succeed");

        let (expected_bytes, expected_offsets) = concat_scalar_encodings(rows);
        assert_eq!(
            out, expected_bytes,
            "encode_batch payload must match concat of scalar encodings"
        );
        assert_eq!(
            offsets, expected_offsets,
            "encode_batch offsets must match per-row scalar start offsets"
        );

        // Per-row slice property: each offset range round-trips through
        // parse_record just like the scalar output.
        for (i, row) in rows.iter().enumerate() {
            let end = offsets.get(i + 1).copied().unwrap_or(out.len());
            let slice = &out[offsets[i]..end];
            let parsed = parse_record(slice).expect("each batch slice must be a valid record");
            assert_eq!(parsed.len(), row.len());
            for (orig, got) in row.iter().zip(parsed.iter()) {
                assert!(
                    values_bitwise_eq(orig, got),
                    "encode_batch row {i} value mismatch: orig={orig:?} got={got:?}"
                );
            }
        }
    }

    #[test]
    fn encode_batch_empty_batch_clears_outputs() {
        let mut out = vec![0xAA_u8; 32];
        let mut offsets = vec![999_usize, 1_234];
        encode_batch(&[], &mut out, &mut offsets).expect("empty batch must succeed");
        assert!(out.is_empty(), "empty batch must clear out buffer");
        assert!(offsets.is_empty(), "empty batch must clear offsets");
    }

    #[test]
    fn encode_batch_single_row_matches_scalar() {
        let rows = vec![vec![
            SqliteValue::Integer(42),
            SqliteValue::Text("hello".into()),
            SqliteValue::Null,
            SqliteValue::Float(3.14),
        ]];
        assert_encode_batch_matches_scalar(&rows);
    }

    #[test]
    fn encode_batch_heterogeneous_rows_preserve_offsets() {
        let rows = vec![
            vec![SqliteValue::Integer(0)],
            vec![],
            vec![
                SqliteValue::Null,
                SqliteValue::Blob(Arc::from([1_u8, 2, 3, 4, 5].as_slice())),
            ],
            vec![
                SqliteValue::Integer(i64::MAX),
                SqliteValue::Integer(i64::MIN),
                SqliteValue::Float(-0.0),
            ],
        ];
        assert_encode_batch_matches_scalar(&rows);
    }

    #[test]
    fn encode_batch_thousand_row_batch_matches_scalar() {
        // 1000 rows, 4 columns each, rotating types.
        let mut rows = Vec::with_capacity(1000);
        for i in 0_i64..1000 {
            rows.push(vec![
                SqliteValue::Integer(i),
                SqliteValue::Integer(i.wrapping_mul(31).wrapping_sub(7)),
                match i % 4 {
                    0 => SqliteValue::Null,
                    1 => SqliteValue::Text(format!("row-{i}").into()),
                    2 => SqliteValue::Float(f64::from(i32::try_from(i).unwrap_or(0)) * 0.5),
                    _ => SqliteValue::Blob(Arc::from(format!("blob-{i}").as_bytes())),
                },
                SqliteValue::Integer(-i),
            ]);
        }
        assert_encode_batch_matches_scalar(&rows);
    }

    #[test]
    fn encode_batch_preserves_caller_capacity() {
        let rows = vec![vec![SqliteValue::Integer(7)]];
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        let mut out = Vec::with_capacity(4096);
        let initial_cap = out.capacity();
        let mut offsets = Vec::with_capacity(32);
        encode_batch(&row_refs, &mut out, &mut offsets).expect("encode must succeed");
        assert!(out.capacity() >= initial_cap, "capacity must not shrink");
    }

    #[test]
    fn encode_batch_multi_row_uses_preallocated_contiguous_buffer() {
        let rows = vec![
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("alpha".into()),
                SqliteValue::Null,
            ],
            vec![
                SqliteValue::Integer(i64::MIN),
                SqliteValue::Blob(Arc::from([0xCA_u8, 0xFE, 0xBA, 0xBE].as_slice())),
                SqliteValue::Float(-0.0),
            ],
            vec![
                SqliteValue::Integer(i64::MAX),
                SqliteValue::Text("omega".into()),
                SqliteValue::Float(42.25),
            ],
        ];
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        let (expected_bytes, expected_offsets) = concat_scalar_encodings(&rows);
        let mut out = Vec::with_capacity(expected_bytes.len());
        let initial_cap = out.capacity();
        let mut offsets = Vec::with_capacity(rows.len());

        encode_batch(&row_refs, &mut out, &mut offsets).expect("encode_batch must succeed");

        assert_eq!(
            out, expected_bytes,
            "multi-row batch must stay byte-identical to scalar record encoding"
        );
        assert_eq!(
            offsets, expected_offsets,
            "multi-row offsets must describe each record in the contiguous output"
        );
        assert_eq!(
            out.capacity(),
            initial_cap,
            "multi-row batch should fill the preplanned contiguous buffer without growing it"
        );
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(100))]

        #[test]
        fn encode_batch_matches_naive_concat_for_arbitrary_batches(
            rows in proptest::collection::vec(
                proptest::collection::vec(arb_sqlite_value(), 0..12),
                0..20,
            )
        ) {
            assert_encode_batch_matches_scalar(&rows);
        }

        #[test]
        fn prop_encode_batch_integer_columns_matches_scalar(
            (row_count, column_count, values) in (16usize..64, 1usize..8).prop_flat_map(
                |(row_count, column_count)| {
                    proptest::collection::vec(any::<i64>(), row_count * column_count)
                        .prop_map(move |values| (row_count, column_count, values))
                }
            )
        ) {
            let rows: Vec<Vec<SqliteValue>> = values
                .chunks_exact(column_count)
                .map(|chunk| {
                    chunk
                        .iter()
                        .copied()
                        .map(SqliteValue::Integer)
                        .collect::<Vec<_>>()
                })
                .collect();
            prop_assert_eq!(rows.len(), row_count);

            let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
            let mut fast = Vec::new();
            let mut offsets = Vec::new();
            prop_assert!(
                encode_batch_integer_columns(&row_refs, &mut fast, &mut offsets),
                "all-integer batches above threshold must take the column encoder",
            );

            let (expected_bytes, expected_offsets) = concat_scalar_encodings(&rows);
            prop_assert_eq!(fast, expected_bytes);
            prop_assert_eq!(offsets, expected_offsets);
        }
    }

    // ── Homogeneous batch fast path (encode_batch_homogeneous) ─────────────

    fn encode_batch_as_vec(rows: &[Vec<SqliteValue>]) -> Vec<u8> {
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        let mut out = Vec::new();
        let mut offsets = Vec::new();
        encode_batch(&row_refs, &mut out, &mut offsets).expect("encode_batch must succeed");
        out
    }

    #[test]
    fn encode_batch_homogeneous_1000_rows_matches_scalar() {
        // 1000 rows of (i64, i64, TEXT[20]). Integer serial types vary with
        // magnitude (and values 0/1 have special zero-payload types), so we
        // bias both integer columns into the large-magnitude 8-byte serial
        // class (serial type 6, u64 magnitude > 0x7FFF_FFFF_FFFF) to keep
        // layout identical across rows. All text values have length 20.
        let mut rows: Vec<Vec<SqliteValue>> = Vec::with_capacity(1000);
        let large_base: i64 = 0x1_0000_0000_0000; // forces serial_type == 6
        for i in 0_i64..1000 {
            // Format as fixed 20-character string.
            let text = format!("{i:020}");
            assert_eq!(text.len(), 20);
            rows.push(vec![
                SqliteValue::Integer(large_base + i),
                SqliteValue::Integer(large_base + i.wrapping_mul(31)),
                SqliteValue::Text(text.into()),
            ]);
        }

        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        assert!(
            rows_have_identical_serial_types(&row_refs),
            "rows must be detected as homogeneous"
        );

        let fast = encode_batch_homogeneous(&row_refs)
            .expect("homogeneous fast path must succeed for 1000-row i64/i64/TEXT20 batch");
        let slow = encode_batch_as_vec(&rows);
        assert_eq!(
            fast, slow,
            "homogeneous fast-path output must be byte-identical to encode_batch"
        );
    }

    #[test]
    fn record_batch_mixed_columns_preserves_offsets_and_reuses_buffers() {
        let mut rows: Vec<Vec<SqliteValue>> = Vec::with_capacity(32);
        let large_base: i64 = 0x0010_0000_0000_0000; // forces serial_type == 6
        for i in 0_i64..32 {
            let text = format!("name_{i:04}");
            assert_eq!(text.len(), 9);
            let first_blob_byte = u8::try_from(i & 0xFF).expect("masked byte fits");
            let second_blob_byte =
                u8::try_from(i.wrapping_mul(3) & 0xFF).expect("masked byte fits");
            rows.push(vec![
                SqliteValue::Integer(large_base + i),
                SqliteValue::Text(text.into()),
                SqliteValue::Integer(large_base + i.wrapping_mul(7)),
                SqliteValue::Float(f64::from(i32::try_from(i).unwrap_or(0)) * 0.25),
                SqliteValue::Blob(Arc::from(
                    [first_blob_byte, second_blob_byte, 0xA5, 0x5A].as_slice(),
                )),
                SqliteValue::Null,
            ]);
        }
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        let (expected_bytes, expected_offsets) = concat_scalar_encodings(&rows);
        let mut fast = Vec::with_capacity(expected_bytes.len());
        let fast_capacity = fast.capacity();
        let mut fast_offsets = Vec::with_capacity(rows.len());
        let fast_offsets_capacity = fast_offsets.capacity();

        assert!(
            encode_batch_homogeneous_into(&row_refs, &mut fast, &mut fast_offsets),
            "fixed-layout mixed-column rows should use the homogeneous encoder"
        );
        assert_eq!(fast, expected_bytes);
        assert_eq!(fast_offsets, expected_offsets);
        assert_eq!(
            fast.capacity(),
            fast_capacity,
            "homogeneous encoder should fill the preallocated contiguous buffer without growing it"
        );
        assert_eq!(
            fast_offsets.capacity(),
            fast_offsets_capacity,
            "homogeneous encoder should fill the preallocated offset table without growing it"
        );

        for _ in 0..256 {
            fast.clear();
            fast_offsets.clear();
            assert!(
                encode_batch_homogeneous_into(&row_refs, &mut fast, &mut fast_offsets),
                "fixed-layout mixed-column rows should keep using the homogeneous encoder"
            );
            assert_eq!(fast, expected_bytes);
            assert_eq!(fast_offsets, expected_offsets);
            assert_eq!(fast.capacity(), fast_capacity);
            assert_eq!(fast_offsets.capacity(), fast_offsets_capacity);
        }

        let mut public = Vec::with_capacity(expected_bytes.len());
        let mut public_offsets = Vec::with_capacity(rows.len());
        encode_batch(&row_refs, &mut public, &mut public_offsets)
            .expect("public batch encoder must succeed");
        assert_eq!(public, expected_bytes);
        assert_eq!(public_offsets, expected_offsets);
    }

    #[test]
    fn encode_batch_integer_columns_varying_varint_widths_matches_scalar() {
        let boundary_values = [
            0_i64,
            1,
            -1,
            127,
            -128,
            128,
            32_767,
            -32_768,
            32_768,
            8_388_607,
            -8_388_608,
            8_388_608,
            2_147_483_647,
            -2_147_483_648,
            2_147_483_648,
            140_737_488_355_327,
            -140_737_488_355_328,
            140_737_488_355_328,
            i64::MAX,
            i64::MIN,
        ];
        let mut rows = Vec::with_capacity(64);
        for i in 0..64usize {
            rows.push(vec![
                SqliteValue::Integer(boundary_values[i % boundary_values.len()]),
                SqliteValue::Integer(boundary_values[(i + 7) % boundary_values.len()]),
                SqliteValue::Integer(i64::try_from(i).unwrap_or(0).wrapping_mul(31) - 9),
                SqliteValue::Integer(i64::try_from(i).unwrap_or(0).wrapping_neg()),
            ]);
        }
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        assert!(
            !rows_have_identical_serial_types(&row_refs),
            "varying integer widths should bypass the old homogeneous serial-type path"
        );

        let mut fast = Vec::new();
        let mut offsets = Vec::new();
        assert!(
            encode_batch_integer_columns(&row_refs, &mut fast, &mut offsets),
            "all-integer rows should take the SIMD column encoder"
        );

        let (expected_bytes, expected_offsets) = concat_scalar_encodings(&rows);
        assert_eq!(fast, expected_bytes);
        assert_eq!(offsets, expected_offsets);
    }

    #[test]
    fn encode_batch_homogeneous_heterogeneous_returns_none() {
        // Build 32 rows (above threshold) but rotate types so serial types
        // differ row-to-row.
        let mut rows: Vec<Vec<SqliteValue>> = Vec::with_capacity(32);
        for i in 0_i64..32 {
            rows.push(vec![
                SqliteValue::Integer(i),
                match i % 3 {
                    0 => SqliteValue::Null,
                    1 => SqliteValue::Text("hi".into()),
                    _ => SqliteValue::Float(1.5),
                },
            ]);
        }
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        assert!(
            !rows_have_identical_serial_types(&row_refs),
            "mixed-type rows must NOT probe as homogeneous"
        );
        assert!(
            encode_batch_homogeneous(&row_refs).is_none(),
            "heterogeneous batch must refuse the fast path"
        );
    }

    #[test]
    fn encode_batch_homogeneous_below_threshold_returns_none() {
        // 10 rows, perfectly homogeneous, but below the 16-row threshold.
        let rows: Vec<Vec<SqliteValue>> = (0_i64..10)
            .map(|i| vec![SqliteValue::Integer(i), SqliteValue::Integer(i * 2)])
            .collect();
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        assert!(
            encode_batch_homogeneous(&row_refs).is_none(),
            "N=10 batch must fall back (below threshold)"
        );
        // encode_batch_auto must still produce correct output via the fallback.
        let auto = encode_batch_auto(&row_refs).expect("encode_batch_auto fallback must succeed");
        let slow = encode_batch_as_vec(&rows);
        assert_eq!(auto, slow, "auto fallback must match encode_batch bytes");
    }

    #[test]
    fn encode_batch_homogeneous_varying_text_lengths_returns_none() {
        // INVARIANT: SQLite TEXT serial types are `2*N + 13` where N is the
        // byte length of the text. Two rows with text of different lengths
        // therefore have DIFFERENT serial types, so the layout-equality
        // prober correctly classifies them as heterogeneous.
        let mut rows: Vec<Vec<SqliteValue>> = Vec::with_capacity(32);
        for i in 0_i64..32 {
            // Alternate between length-3 and length-7 text.
            let text = if i % 2 == 0 {
                "abc".to_owned()
            } else {
                "abcdefg".to_owned()
            };
            rows.push(vec![
                SqliteValue::Integer(i),
                SqliteValue::Text(text.into()),
            ]);
        }
        let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
        assert!(
            !rows_have_identical_serial_types(&row_refs),
            "varying-length TEXT rows must be classified heterogeneous (different serial types)"
        );
        assert!(
            encode_batch_homogeneous(&row_refs).is_none(),
            "varying TEXT length rows must refuse the fast path"
        );
        // And auto fallback must still produce correct bytes.
        let auto = encode_batch_auto(&row_refs).expect("encode_batch_auto must succeed");
        let slow = encode_batch_as_vec(&rows);
        assert_eq!(auto, slow);
    }

    #[test]
    fn encode_batch_homogeneous_fuzz_100_random_batches() {
        use proptest::prelude::*;
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;

        // For each trial, pick a random column-type signature and fill 16..64
        // rows with that exact signature. We use a deterministic runner so the
        // test is reproducible but still exercises a wide variety of layouts.
        let mut runner = TestRunner::deterministic();
        for trial in 0..100 {
            // Strategy: pick column count 1..=6, and for each column pick a
            // "type kind" (Null, small-int, large-int, float, fixed-text,
            // fixed-blob). Then generate N=16..48 rows matching that signature.
            let signature_strategy = proptest::collection::vec(0_u8..6_u8, 1..=6);
            let signature_tree = signature_strategy
                .new_tree(&mut runner)
                .expect("strategy must produce a value");
            let signature: Vec<u8> = signature_tree.current();

            let n_rows: usize = 16 + (trial % 32);
            let mut rows: Vec<Vec<SqliteValue>> = Vec::with_capacity(n_rows);
            for r in 0..n_rows {
                let mut row: Vec<SqliteValue> = Vec::with_capacity(signature.len());
                for (c, kind) in signature.iter().enumerate() {
                    // Same kind in same column ⇒ same serial_type; vary values
                    // within the kind's serial-type class so bodies differ.
                    let seed = (trial as i64) * 1_000 + (r as i64) * 17 + (c as i64) * 3;
                    // Keep values away from 0 and 1 (special serial types
                    // 8 and 9) so the 1-byte-int class is stable. The
                    // large-int branch forces serial type 6 (8-byte signed).
                    let small_iv = 2_i64 + (seed.rem_euclid(100));
                    let large_iv = 0x0010_0000_0000_0000_i64 + seed.rem_euclid(1_000);
                    #[allow(clippy::cast_precision_loss)]
                    let fv = (seed as f64).mul_add(0.125, 1.0);
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let blob_bytes: [u8; 4] = [
                        (seed & 0xFF) as u8,
                        ((seed >> 8) & 0xFF) as u8,
                        ((seed >> 16) & 0xFF) as u8,
                        ((seed >> 24) & 0xFF) as u8,
                    ];
                    let val = match kind {
                        0 => SqliteValue::Null,
                        1 => SqliteValue::Integer(small_iv),
                        2 => SqliteValue::Integer(large_iv),
                        3 => SqliteValue::Float(fv),
                        4 => SqliteValue::Text(
                            format!("{:05}", seed.unsigned_abs() % 100_000).into(),
                        ),
                        _ => SqliteValue::Blob(Arc::from(blob_bytes.as_slice())),
                    };
                    row.push(val);
                }
                rows.push(row);
            }

            let row_refs: Vec<&[SqliteValue]> = rows.iter().map(Vec::as_slice).collect();
            assert!(
                rows_have_identical_serial_types(&row_refs),
                "trial {trial}: generator must produce homogeneous rows"
            );
            let fast = encode_batch_homogeneous(&row_refs).unwrap_or_else(|| {
                assert_eq!(
                    rows.len(),
                    0,
                    "trial {trial}: homogeneous encode must succeed (N={n_rows})"
                );
                Vec::new()
            });
            let slow = encode_batch_as_vec(&rows);
            assert_eq!(
                fast, slow,
                "trial {trial}: fast/slow mismatch for signature {signature:?}"
            );

            // encode_batch_auto must also match.
            let auto = encode_batch_auto(&row_refs)
                .expect("encode_batch_auto must succeed on homogeneous");
            assert_eq!(auto, slow, "trial {trial}: encode_batch_auto mismatch");
        }
    }
}
