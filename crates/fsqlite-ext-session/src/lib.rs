use fsqlite_types::serial_type::{read_varint, varint_len, write_varint};
use fsqlite_types::value::{SmallText, SqliteValue};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Changeset table header marker byte ('T').
const CHANGESET_TABLE_HEADER_BYTE: u8 = 0x54;

/// Patchset table header marker byte ('P').
const PATCHSET_TABLE_HEADER_BYTE: u8 = 0x50;

/// Legacy alias retained for internal tests that refer to the changeset header byte.
#[cfg(test)]
const TABLE_HEADER_BYTE: u8 = CHANGESET_TABLE_HEADER_BYTE;

/// Operation codes used in changeset/patchset binary format.
const OP_INSERT: u8 = 0x12; // 18
const OP_DELETE: u8 = 0x09; // 9
const OP_UPDATE: u8 = 0x17; // 23

/// Value type markers in the changeset binary format.
const VAL_UNDEFINED: u8 = 0x00;
const VAL_INTEGER: u8 = 0x01;
const VAL_REAL: u8 = 0x02;
const VAL_TEXT: u8 = 0x03;
const VAL_BLOB: u8 = 0x04;
const VAL_NULL: u8 = 0x05;

// ---------------------------------------------------------------------------
// Wire kind and typed format errors
// ---------------------------------------------------------------------------

/// Which SQLite session wire format a decoded [`Changeset`] carries.
///
/// SQLite keeps the two formats distinct at the API level:
/// `sqlite3changeset_invert()` rejects patchsets and
/// `sqlite3changeset_concat()` refuses to mix kinds. The tag lets the Rust
/// API enforce the same boundaries with typed errors instead of silently
/// producing malformed output (GH#394).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangesetKind {
    /// Full changeset: UPDATE rows carry complete old and new records.
    Changeset,
    /// Compact patchset: UPDATE rows carry only primary-key old values.
    Patchset,
}

impl ChangesetKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Changeset => "changeset",
            Self::Patchset => "patchset",
        }
    }
}

/// Typed decode/format failure for changeset and patchset blobs.
///
/// Decoding never silently normalizes foreign bytes: an operation byte or
/// indirect byte outside SQLite's canonical encoding is reported with its
/// offset so callers can distinguish corruption from truncation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangesetFormatError {
    /// The blob ended before the record that starts at `offset` was complete.
    Truncated { offset: usize },
    /// The table header at `offset` is not a valid header for the requested
    /// wire kind, or its column/primary-key metadata is malformed.
    MalformedTableHeader { offset: usize },
    /// The operation byte at `offset` is not INSERT, DELETE, or UPDATE.
    InvalidOperation { offset: usize, byte: u8 },
    /// The indirect byte at `offset` is neither `0x00` nor `0x01`.
    InvalidIndirectFlag { offset: usize, byte: u8 },
    /// A column value at `offset` has an unknown type tag or is truncated.
    MalformedValue { offset: usize },
    /// A patchset record references a primary-key layout that does not match
    /// its table header (for example a DELETE on a table without key columns).
    MalformedPatchsetRecord { offset: usize },
    /// The operation is defined only for one wire kind (`invert` on a
    /// patchset) or the operands disagree (`concat` across kinds).
    KindMismatch {
        expected: ChangesetKind,
        found: ChangesetKind,
    },
}

impl std::fmt::Display for ChangesetFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated { offset } => {
                write!(f, "changeset truncated at byte offset {offset}")
            }
            Self::MalformedTableHeader { offset } => {
                write!(
                    f,
                    "malformed changeset table header at byte offset {offset}"
                )
            }
            Self::InvalidOperation { offset, byte } => write!(
                f,
                "invalid changeset operation byte {byte:#04x} at byte offset {offset}"
            ),
            Self::InvalidIndirectFlag { offset, byte } => write!(
                f,
                "invalid changeset indirect byte {byte:#04x} at byte offset {offset} (expected 0x00 or 0x01)"
            ),
            Self::MalformedValue { offset } => {
                write!(f, "malformed changeset value at byte offset {offset}")
            }
            Self::MalformedPatchsetRecord { offset } => {
                write!(f, "malformed patchset record at byte offset {offset}")
            }
            Self::KindMismatch { expected, found } => write!(
                f,
                "expected a {} but found a {}",
                expected.as_str(),
                found.as_str()
            ),
        }
    }
}

impl std::error::Error for ChangesetFormatError {}

// ---------------------------------------------------------------------------
// Public API — extension name
// ---------------------------------------------------------------------------

#[must_use]
pub const fn extension_name() -> &'static str {
    "session"
}

// ---------------------------------------------------------------------------
// Change operations
// ---------------------------------------------------------------------------

/// The kind of DML operation recorded in a changeset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Delete,
    Update,
}

impl ChangeOp {
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        match self {
            Self::Insert => OP_INSERT,
            Self::Delete => OP_DELETE,
            Self::Update => OP_UPDATE,
        }
    }

    /// Decode an operation byte from the changeset format.
    ///
    /// Returns `None` for unrecognised bytes.
    #[must_use]
    pub const fn from_byte(b: u8) -> Option<Self> {
        match b {
            OP_INSERT => Some(Self::Insert),
            OP_DELETE => Some(Self::Delete),
            OP_UPDATE => Some(Self::Update),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Conflict types and actions
// ---------------------------------------------------------------------------

/// The category of conflict encountered while applying a changeset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    /// The row exists but its current values differ from the expected old values.
    Data,
    /// The row to update or delete does not exist in the target database.
    NotFound,
    /// A unique-constraint violation occurred (e.g. duplicate key on INSERT).
    Conflict,
    /// A non-unique constraint violation occurred (CHECK, NOT NULL, etc.).
    Constraint,
    /// A foreign-key constraint violation occurred.
    ForeignKey,
}

/// The action the caller wants the apply engine to take for a conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictAction {
    /// Skip this change and continue applying the rest of the changeset.
    OmitChange,
    /// Overwrite the conflicting row with the incoming change.
    Replace,
    /// Abort the entire apply operation immediately.
    Abort,
}

// ---------------------------------------------------------------------------
// Changeset value encoding / decoding
// ---------------------------------------------------------------------------

/// A single column value in the changeset binary format.
///
/// `Undefined` is used in UPDATE records for columns that did not change.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangesetValue {
    Undefined,
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl ChangesetValue {
    /// Convert from a [`SqliteValue`].
    #[must_use]
    pub fn from_sqlite(val: &SqliteValue) -> Self {
        match val {
            SqliteValue::Null => Self::Null,
            SqliteValue::Integer(i) => Self::Integer(*i),
            SqliteValue::Float(f) => Self::Real(*f),
            SqliteValue::Text(s) => Self::Text(s.to_string()),
            SqliteValue::Blob(b) => Self::Blob(b.to_vec()),
        }
    }

    /// Convert to a [`SqliteValue`], mapping `Undefined` to `Null`.
    #[must_use]
    pub fn to_sqlite(&self) -> SqliteValue {
        use std::sync::Arc;
        match self {
            Self::Undefined | Self::Null => SqliteValue::Null,
            Self::Integer(i) => SqliteValue::Integer(*i),
            Self::Real(f) => SqliteValue::Float(*f),
            Self::Text(s) => SqliteValue::Text(SmallText::new(s.as_str())),
            Self::Blob(b) => SqliteValue::Blob(Arc::from(b.as_slice())),
        }
    }

    /// Encode this value into the changeset binary format, appending to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Undefined => {
                out.push(VAL_UNDEFINED);
            }
            Self::Null => {
                out.push(VAL_NULL);
            }
            Self::Integer(i) => {
                out.push(VAL_INTEGER);
                out.extend_from_slice(&i.to_be_bytes());
            }
            Self::Real(f) => {
                out.push(VAL_REAL);
                out.extend_from_slice(&f.to_be_bytes());
            }
            Self::Text(s) => {
                out.push(VAL_TEXT);
                let bytes = s.as_bytes();
                let mut vbuf = [0u8; 9];
                let vlen = write_varint(&mut vbuf, bytes.len() as u64);
                out.extend_from_slice(&vbuf[..vlen]);
                out.extend_from_slice(bytes);
            }
            Self::Blob(b) => {
                out.push(VAL_BLOB);
                let mut vbuf = [0u8; 9];
                let vlen = write_varint(&mut vbuf, b.len() as u64);
                out.extend_from_slice(&vbuf[..vlen]);
                out.extend_from_slice(b);
            }
        }
    }

    /// Decode a single value from `data` starting at `pos`.
    ///
    /// Returns `(value, bytes_consumed)` or `None` on malformed input.
    pub fn decode(data: &[u8], pos: usize) -> Option<(Self, usize)> {
        let type_byte = *data.get(pos)?;
        let mut offset = pos + 1;
        match type_byte {
            VAL_UNDEFINED => Some((Self::Undefined, offset - pos)),
            VAL_NULL => Some((Self::Null, offset - pos)),
            VAL_INTEGER => {
                let end = offset + 8;
                if data.len() < end {
                    return None;
                }
                let arr: [u8; 8] = data[offset..end].try_into().ok()?;
                Some((Self::Integer(i64::from_be_bytes(arr)), end - pos))
            }
            VAL_REAL => {
                let end = offset + 8;
                if data.len() < end {
                    return None;
                }
                let arr: [u8; 8] = data[offset..end].try_into().ok()?;
                Some((Self::Real(f64::from_be_bytes(arr)), end - pos))
            }
            VAL_TEXT => {
                let (len, vlen) = read_varint(&data[offset..])?;
                offset += vlen;
                let len = usize::try_from(len).ok()?;
                let end = offset + len;
                if data.len() < end {
                    return None;
                }
                let s = std::str::from_utf8(&data[offset..end]).ok()?;
                Some((Self::Text(s.into()), end - pos))
            }
            VAL_BLOB => {
                let (len, vlen) = read_varint(&data[offset..])?;
                offset += vlen;
                let len = usize::try_from(len).ok()?;
                let end = offset + len;
                if data.len() < end {
                    return None;
                }
                Some((Self::Blob(data[offset..end].to_vec()), end - pos))
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Table info carried in the changeset
// ---------------------------------------------------------------------------

/// Per-table metadata stored in the changeset header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Number of columns.
    pub column_count: usize,
    /// For each column, `true` if it is part of the primary key.
    pub pk_flags: Vec<bool>,
}

impl TableInfo {
    fn encode_with_header(&self, out: &mut Vec<u8>, header_byte: u8) {
        out.push(header_byte);
        let mut vbuf = [0u8; 9];
        let vlen = write_varint(&mut vbuf, self.column_count as u64);
        out.extend_from_slice(&vbuf[..vlen]);
        for &pk in &self.pk_flags {
            out.push(u8::from(pk));
        }
        out.extend_from_slice(self.name.as_bytes());
        out.push(0x00); // NUL terminator
    }

    /// Encode the table header into changeset binary format.
    pub fn encode(&self, out: &mut Vec<u8>) {
        self.encode_with_header(out, CHANGESET_TABLE_HEADER_BYTE);
    }

    /// Encode the table header into patchset binary format.
    pub fn encode_patchset(&self, out: &mut Vec<u8>) {
        self.encode_with_header(out, PATCHSET_TABLE_HEADER_BYTE);
    }

    /// Decode a table header starting at `pos`.
    ///
    /// Returns `(TableInfo, bytes_consumed)` or `None`.
    fn decode_with_header(data: &[u8], pos: usize, header_byte: u8) -> Option<(Self, usize)> {
        if *data.get(pos)? != header_byte {
            return None;
        }
        let mut offset = pos + 1;
        let (col_count, vlen) = read_varint(&data[offset..])?;
        offset += vlen;
        let col_count = usize::try_from(col_count).ok()?;
        if data.len() < offset + col_count {
            return None;
        }
        let pk_flags: Vec<bool> = data[offset..offset + col_count]
            .iter()
            .map(|&b| b != 0)
            .collect();
        offset += col_count;
        // Read NUL-terminated table name.
        let name_start = offset;
        let nul_pos = data[name_start..].iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&data[name_start..name_start + nul_pos])
            .ok()?
            .to_owned();
        offset = name_start + nul_pos + 1;
        Some((
            Self {
                name,
                column_count: col_count,
                pk_flags,
            },
            offset - pos,
        ))
    }

    pub fn decode(data: &[u8], pos: usize) -> Option<(Self, usize)> {
        Self::decode_with_header(data, pos, CHANGESET_TABLE_HEADER_BYTE)
    }

    pub fn decode_patchset(data: &[u8], pos: usize) -> Option<(Self, usize)> {
        Self::decode_with_header(data, pos, PATCHSET_TABLE_HEADER_BYTE)
    }
}

// ---------------------------------------------------------------------------
// Change row
// ---------------------------------------------------------------------------

/// A single row change recorded in a changeset.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetRow {
    pub op: ChangeOp,
    /// SQLite's per-change indirect flag (`sqlite3changeset_op()` `pbIndirect`).
    ///
    /// `true` marks a change that every contributing operation recorded while
    /// the session was in indirect mode (trigger/foreign-key side effects in
    /// SQLite). The flag travels byte-for-byte through both wire formats and
    /// through changeset inversion; only the canonical bytes `0x00`/`0x01` are
    /// accepted on decode (GH#394).
    pub indirect: bool,
    /// For DELETE and UPDATE: the old column values. Empty for INSERT.
    pub old_values: Vec<ChangesetValue>,
    /// For INSERT and UPDATE: the new column values. Empty for DELETE.
    pub new_values: Vec<ChangesetValue>,
}

/// Decode the indirect byte that follows every operation byte.
///
/// SQLite writes exactly `0x00` (direct) or `0x01` (indirect). Any other value
/// is rejected as a format error rather than silently normalized, so a
/// decode→encode round trip can never rewrite a foreign byte.
fn decode_indirect_flag(data: &[u8], offset: usize) -> Result<bool, ChangesetFormatError> {
    match data.get(offset) {
        Some(0x00) => Ok(false),
        Some(0x01) => Ok(true),
        Some(&byte) => Err(ChangesetFormatError::InvalidIndirectFlag { offset, byte }),
        None => Err(ChangesetFormatError::Truncated { offset }),
    }
}

impl ChangesetRow {
    /// Encode this row change into changeset binary format.
    pub fn encode_changeset(&self, out: &mut Vec<u8>) {
        out.push(self.op.as_byte());
        out.push(u8::from(self.indirect));
        match self.op {
            ChangeOp::Insert => {
                for v in &self.new_values {
                    v.encode(out);
                }
            }
            ChangeOp::Delete => {
                for v in &self.old_values {
                    v.encode(out);
                }
            }
            ChangeOp::Update => {
                for v in &self.old_values {
                    v.encode(out);
                }
                for v in &self.new_values {
                    v.encode(out);
                }
            }
        }
    }

    /// Encode this row change into patchset binary format.
    ///
    /// For INSERT and DELETE this is identical to changeset encoding.
    /// For UPDATE the patchset stores a single record whose PK slots contain
    /// the original key values and whose non-PK slots contain changed new
    /// values or [`ChangesetValue::Undefined`] for unchanged columns.
    pub fn encode_patchset(&self, out: &mut Vec<u8>, pk_flags: &[bool]) {
        out.push(self.op.as_byte());
        out.push(u8::from(self.indirect));
        match self.op {
            ChangeOp::Insert => {
                for v in &self.new_values {
                    v.encode(out);
                }
            }
            ChangeOp::Delete => {
                for (i, v) in self.old_values.iter().enumerate() {
                    if pk_flags.get(i).copied().unwrap_or(false) {
                        v.encode(out);
                    }
                }
            }
            ChangeOp::Update => {
                for (index, new_value) in self.new_values.iter().enumerate() {
                    if pk_flags.get(index).copied().unwrap_or(false) {
                        self.old_values
                            .get(index)
                            .unwrap_or(&ChangesetValue::Undefined)
                            .encode(out);
                    } else {
                        new_value.encode(out);
                    }
                }
            }
        }
    }

    /// Decode the operation and indirect bytes that open every row record.
    fn decode_row_prefix(
        data: &[u8],
        pos: usize,
    ) -> Result<(ChangeOp, bool), ChangesetFormatError> {
        let op_byte = *data
            .get(pos)
            .ok_or(ChangesetFormatError::Truncated { offset: pos })?;
        let op = ChangeOp::from_byte(op_byte).ok_or(ChangesetFormatError::InvalidOperation {
            offset: pos,
            byte: op_byte,
        })?;
        let indirect = decode_indirect_flag(data, pos + 1)?;
        Ok((op, indirect))
    }

    /// Decode `n` consecutive column values starting at `*offset`.
    fn decode_values(
        data: &[u8],
        offset: &mut usize,
        n: usize,
    ) -> Result<Vec<ChangesetValue>, ChangesetFormatError> {
        let mut vals = Vec::with_capacity(n);
        for _ in 0..n {
            let (v, consumed) = ChangesetValue::decode(data, *offset)
                .ok_or(ChangesetFormatError::MalformedValue { offset: *offset })?;
            *offset += consumed;
            vals.push(v);
        }
        Ok(vals)
    }

    /// Decode one changeset row starting at `pos`, given the column count.
    ///
    /// Returns the row and the number of bytes consumed. Non-canonical
    /// operation or indirect bytes are rejected with a typed
    /// [`ChangesetFormatError`] rather than being normalized.
    pub fn decode_changeset(
        data: &[u8],
        pos: usize,
        col_count: usize,
    ) -> Result<(Self, usize), ChangesetFormatError> {
        let (op, indirect) = Self::decode_row_prefix(data, pos)?;
        let mut offset = pos + 2;

        let (old_values, new_values) = match op {
            ChangeOp::Insert => {
                let new_values = Self::decode_values(data, &mut offset, col_count)?;
                (Vec::new(), new_values)
            }
            ChangeOp::Delete => {
                let old_values = Self::decode_values(data, &mut offset, col_count)?;
                (old_values, Vec::new())
            }
            ChangeOp::Update => {
                let old_values = Self::decode_values(data, &mut offset, col_count)?;
                let new_values = Self::decode_values(data, &mut offset, col_count)?;
                (old_values, new_values)
            }
        };

        Ok((
            Self {
                op,
                indirect,
                old_values,
                new_values,
            },
            offset - pos,
        ))
    }

    /// Decode one patchset row starting at `pos`.
    ///
    /// Patchset UPDATE rows only store primary-key old values. Non-PK old
    /// values are reconstructed as [`ChangesetValue::Undefined`] so the
    /// decoded row can reuse the normal apply path.
    pub fn decode_patchset(
        data: &[u8],
        pos: usize,
        col_count: usize,
        pk_flags: &[bool],
    ) -> Result<(Self, usize), ChangesetFormatError> {
        if pk_flags.len() != col_count {
            return Err(ChangesetFormatError::MalformedPatchsetRecord { offset: pos });
        }

        let (op, indirect) = Self::decode_row_prefix(data, pos)?;
        let mut offset = pos + 2;

        let (old_values, new_values) = match op {
            ChangeOp::Insert => {
                let new_values = Self::decode_values(data, &mut offset, col_count)?;
                (Vec::new(), new_values)
            }
            ChangeOp::Delete => {
                let pk_count = pk_flags.iter().filter(|&&is_pk| is_pk).count();
                if pk_count == 0 {
                    return Err(ChangesetFormatError::MalformedPatchsetRecord { offset: pos });
                }
                let pk_old_values = Self::decode_values(data, &mut offset, pk_count)?;
                let mut old_values = Vec::with_capacity(col_count);
                let mut pk_iter = pk_old_values.into_iter();
                for is_pk in pk_flags {
                    if *is_pk {
                        old_values.push(pk_iter.next().ok_or(
                            ChangesetFormatError::MalformedPatchsetRecord { offset: pos },
                        )?);
                    } else {
                        old_values.push(ChangesetValue::Undefined);
                    }
                }
                (old_values, Vec::new())
            }
            ChangeOp::Update => {
                let record = Self::decode_values(data, &mut offset, col_count)?;
                let mut old_values = Vec::with_capacity(col_count);
                let mut new_values = Vec::with_capacity(col_count);
                for (index, value) in record.into_iter().enumerate() {
                    if pk_flags.get(index).copied().unwrap_or(false) {
                        old_values.push(value);
                        new_values.push(ChangesetValue::Undefined);
                    } else {
                        old_values.push(ChangesetValue::Undefined);
                        new_values.push(value);
                    }
                }
                (old_values, new_values)
            }
        };

        Ok((
            Self {
                op,
                indirect,
                old_values,
                new_values,
            },
            offset - pos,
        ))
    }

    /// Invert this change: INSERT becomes DELETE, DELETE becomes INSERT,
    /// UPDATE swaps old and new values. The indirect flag is preserved, as
    /// `sqlite3changeset_invert()` copies it unchanged.
    #[must_use]
    pub fn invert(&self) -> Self {
        match self.op {
            ChangeOp::Insert => Self {
                op: ChangeOp::Delete,
                indirect: self.indirect,
                old_values: self.new_values.clone(),
                new_values: Vec::new(),
            },
            ChangeOp::Delete => Self {
                op: ChangeOp::Insert,
                indirect: self.indirect,
                old_values: Vec::new(),
                new_values: self.old_values.clone(),
            },
            ChangeOp::Update => Self {
                op: ChangeOp::Update,
                indirect: self.indirect,
                old_values: self.new_values.clone(),
                new_values: self.old_values.clone(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Per-table changeset section
// ---------------------------------------------------------------------------

/// All row changes for a single table within a changeset.
#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeset {
    pub info: TableInfo,
    pub rows: Vec<ChangesetRow>,
}

impl TableChangeset {
    /// Encode this table section in changeset format.
    pub fn encode_changeset(&self, out: &mut Vec<u8>) {
        self.info.encode(out);
        for row in &self.rows {
            row.encode_changeset(out);
        }
    }

    /// Encode this table section in patchset format.
    pub fn encode_patchset(&self, out: &mut Vec<u8>) {
        self.info.encode_patchset(out);
        for row in &self.rows {
            row.encode_patchset(out, &self.info.pk_flags);
        }
    }
}

// ---------------------------------------------------------------------------
// Full changeset
// ---------------------------------------------------------------------------

/// A complete changeset covering one or more tables.
#[derive(Debug, Clone, PartialEq)]
pub struct Changeset {
    /// Which wire format this container was decoded from or is meant to
    /// encode as. Session output and [`Changeset::new`] are full changesets;
    /// [`Changeset::decode_patchset`] yields [`ChangesetKind::Patchset`].
    pub kind: ChangesetKind,
    pub tables: Vec<TableChangeset>,
}

impl Changeset {
    /// Create an empty full changeset.
    #[must_use]
    pub fn new() -> Self {
        Self {
            kind: ChangesetKind::Changeset,
            tables: Vec::new(),
        }
    }

    /// Encode the entire changeset in binary format.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for tc in &self.tables {
            tc.encode_changeset(&mut out);
        }
        out
    }

    /// Encode the entire changeset as a patchset (compact form).
    #[must_use]
    pub fn encode_patchset(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for tc in &self.tables {
            tc.encode_patchset(&mut out);
        }
        out
    }

    /// Decode a changeset from its binary representation.
    ///
    /// Every row's indirect byte is preserved; non-canonical bytes and any
    /// other structural damage are reported as a typed
    /// [`ChangesetFormatError`].
    pub fn decode(data: &[u8]) -> Result<Self, ChangesetFormatError> {
        let mut tables = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let (info, consumed) = TableInfo::decode(data, pos)
                .ok_or(ChangesetFormatError::MalformedTableHeader { offset: pos })?;
            pos += consumed;
            let mut rows = Vec::new();
            // Read rows until we hit another table header or end of data.
            while pos < data.len() && data[pos] != CHANGESET_TABLE_HEADER_BYTE {
                let (row, consumed) = ChangesetRow::decode_changeset(data, pos, info.column_count)?;
                pos += consumed;
                rows.push(row);
            }
            tables.push(TableChangeset { info, rows });
        }
        Ok(Self {
            kind: ChangesetKind::Changeset,
            tables,
        })
    }

    /// Decode a patchset from its binary representation.
    ///
    /// The result is tagged [`ChangesetKind::Patchset`]; it can be encoded
    /// back with [`Changeset::encode_patchset`] and applied, but cannot be
    /// inverted or concatenated with a full changeset.
    pub fn decode_patchset(data: &[u8]) -> Result<Self, ChangesetFormatError> {
        let mut tables = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let (info, consumed) = TableInfo::decode_patchset(data, pos)
                .ok_or(ChangesetFormatError::MalformedTableHeader { offset: pos })?;
            pos += consumed;
            let mut rows = Vec::new();
            while pos < data.len() && data[pos] != PATCHSET_TABLE_HEADER_BYTE {
                let (row, consumed) =
                    ChangesetRow::decode_patchset(data, pos, info.column_count, &info.pk_flags)?;
                pos += consumed;
                rows.push(row);
            }
            tables.push(TableChangeset { info, rows });
        }
        Ok(Self {
            kind: ChangesetKind::Patchset,
            tables,
        })
    }

    /// Invert the changeset: every INSERT becomes DELETE, every DELETE
    /// becomes INSERT, every UPDATE swaps old and new values. Each row keeps
    /// its indirect flag.
    ///
    /// Like `sqlite3changeset_invert()`, inversion is defined only for full
    /// changesets: a patchset lacks the non-key old values an inverted UPDATE
    /// or DELETE needs, so it is rejected with
    /// [`ChangesetFormatError::KindMismatch`].
    pub fn invert(&self) -> Result<Self, ChangesetFormatError> {
        if self.kind != ChangesetKind::Changeset {
            return Err(ChangesetFormatError::KindMismatch {
                expected: ChangesetKind::Changeset,
                found: self.kind,
            });
        }
        Ok(Self {
            kind: self.kind,
            tables: self
                .tables
                .iter()
                .map(|tc| TableChangeset {
                    info: tc.info.clone(),
                    rows: tc.rows.iter().map(ChangesetRow::invert).collect(),
                })
                .collect(),
        })
    }

    /// Append every table section of `other` onto this container, keeping
    /// each appended row's indirect flag.
    ///
    /// Both operands must share a wire kind; mixing a changeset with a
    /// patchset is rejected with [`ChangesetFormatError::KindMismatch`]. This
    /// does not merge same-key operations the way
    /// `sqlite3changeset_concat()` does; it is a section-level append.
    pub fn concat(&mut self, other: &Self) -> Result<(), ChangesetFormatError> {
        if self.kind != other.kind {
            return Err(ChangesetFormatError::KindMismatch {
                expected: self.kind,
                found: other.kind,
            });
        }
        for tc in &other.tables {
            self.tables.push(tc.clone());
        }
        Ok(())
    }
}

impl Default for Changeset {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Session — change tracker
// ---------------------------------------------------------------------------

/// A recorded change entry tracked by a [`Session`].
#[derive(Debug, Clone)]
struct TrackedChange {
    table_name: String,
    op: ChangeOp,
    /// Snapshot of [`Session::indirect`] at record time.
    indirect: bool,
    old_values: Vec<ChangesetValue>,
    new_values: Vec<ChangesetValue>,
}

/// Metadata about a table being tracked by a [`Session`].
#[derive(Debug, Clone)]
struct TrackedTable {
    name: String,
    column_count: usize,
    pk_flags: Vec<bool>,
}

fn assert_tracked_change_width(change: &TrackedChange, tracked: &TrackedTable) {
    match change.op {
        ChangeOp::Insert => assert_eq!(
            change.new_values.len(),
            tracked.column_count,
            "insert values length must match attached table column_count"
        ),
        ChangeOp::Delete => assert_eq!(
            change.old_values.len(),
            tracked.column_count,
            "delete values length must match attached table column_count"
        ),
        ChangeOp::Update => {
            assert_eq!(
                change.old_values.len(),
                tracked.column_count,
                "update old_values length must match attached table column_count"
            );
            assert_eq!(
                change.new_values.len(),
                tracked.column_count,
                "update new_values length must match attached table column_count"
            );
        }
    }
}

fn has_primary_key(pk_flags: &[bool]) -> bool {
    pk_flags.iter().any(|is_pk| *is_pk)
}

fn primary_key_values_are_trackable(values: &[ChangesetValue], pk_flags: &[bool]) -> bool {
    pk_flags
        .iter()
        .enumerate()
        .filter(|(_, is_pk)| **is_pk)
        .all(|(index, _)| {
            values.get(index).is_some_and(|value| {
                !matches!(value, ChangesetValue::Null | ChangesetValue::Undefined)
            })
        })
}

fn materialize_sparse_update(
    base_row: &[ChangesetValue],
    sparse_values: &[ChangesetValue],
) -> Vec<ChangesetValue> {
    base_row
        .iter()
        .zip(sparse_values.iter())
        .map(|(base, delta)| {
            if *delta == ChangesetValue::Undefined {
                base.clone()
            } else {
                delta.clone()
            }
        })
        .collect()
}

fn canonical_old_values(
    old_row: &[ChangesetValue],
    new_row: &[ChangesetValue],
    pk_flags: &[bool],
) -> Vec<ChangesetValue> {
    old_row
        .iter()
        .zip(new_row.iter())
        .enumerate()
        .map(|(index, (old, new))| {
            if pk_flags.get(index).copied().unwrap_or(false) || old != new {
                old.clone()
            } else {
                ChangesetValue::Undefined
            }
        })
        .collect()
}

fn canonical_new_values(
    old_row: &[ChangesetValue],
    new_row: &[ChangesetValue],
) -> Vec<ChangesetValue> {
    old_row
        .iter()
        .zip(new_row.iter())
        .map(|(old, new)| {
            if old == new {
                ChangesetValue::Undefined
            } else {
                new.clone()
            }
        })
        .collect()
}

fn primary_key_matches(
    left: &[ChangesetValue],
    right: &[ChangesetValue],
    pk_flags: &[bool],
) -> bool {
    pk_flags
        .iter()
        .enumerate()
        .filter(|&(_, &is_pk)| is_pk)
        .all(|(index, _)| {
            left.get(index)
                .zip(right.get(index))
                .is_some_and(|(lhs, rhs)| lhs == rhs)
        })
}

fn primary_key_changed(
    old_row: &[ChangesetValue],
    new_row: &[ChangesetValue],
    pk_flags: &[bool],
) -> bool {
    !primary_key_matches(old_row, new_row, pk_flags)
}

/// The accumulated state of one primary key inside a [`Session`].
///
/// Mirrors SQLite's per-key `SessionChange`: `before` is the row as it stood
/// when the session first touched the key, `after` is its current image, and
/// `indirect` is the logical AND of every contributing operation's indirect
/// flag. An entry whose images cancel out (a net no-op) is retained rather
/// than discarded so later operations on the same key keep the earlier direct
/// provenance (GH#394).
#[derive(Debug, Clone)]
struct PendingRowChange {
    /// Primary-key values that identify this entry. Taken from the first
    /// contributing operation and refreshed whenever the key moves.
    key: Vec<ChangesetValue>,
    before: Option<Vec<ChangesetValue>>,
    after: Option<Vec<ChangesetValue>>,
    indirect: bool,
}

impl PendingRowChange {
    fn from_tracked(change: &TrackedChange, column_count: usize) -> Self {
        match change.op {
            ChangeOp::Insert => Self {
                key: change.new_values.clone(),
                before: None,
                after: Some(change.new_values.clone()),
                indirect: change.indirect,
            },
            ChangeOp::Delete => Self {
                key: change.old_values.clone(),
                before: Some(change.old_values.clone()),
                after: None,
                indirect: change.indirect,
            },
            ChangeOp::Update => {
                debug_assert_eq!(change.old_values.len(), column_count);
                debug_assert_eq!(change.new_values.len(), column_count);
                let after = materialize_sparse_update(&change.old_values, &change.new_values);
                Self {
                    key: after.clone(),
                    before: Some(change.old_values.clone()),
                    after: Some(after),
                    indirect: change.indirect,
                }
            }
        }
    }

    fn matches_change(&self, change: &TrackedChange, pk_flags: &[bool]) -> bool {
        match change.op {
            // An INSERT can only follow a key that is currently absent: one
            // that was deleted, or one that was inserted and deleted again.
            ChangeOp::Insert => {
                self.after.is_none() && primary_key_matches(&self.key, &change.new_values, pk_flags)
            }
            ChangeOp::Delete | ChangeOp::Update => self
                .after
                .as_ref()
                .is_some_and(|after| primary_key_matches(after, &change.old_values, pk_flags)),
        }
    }

    fn merge(&mut self, change: &TrackedChange, column_count: usize) {
        // SQLite: a change is indirect only if every contributing operation
        // was recorded in indirect mode.
        self.indirect &= change.indirect;
        match change.op {
            ChangeOp::Insert => {
                self.after = Some(change.new_values.clone());
            }
            ChangeOp::Delete => {
                self.after = None;
            }
            ChangeOp::Update => {
                debug_assert_eq!(change.old_values.len(), column_count);
                debug_assert_eq!(change.new_values.len(), column_count);
                if let Some(current_row) = self.after.as_ref() {
                    let after = materialize_sparse_update(current_row, &change.new_values);
                    self.key.clone_from(&after);
                    self.after = Some(after);
                }
            }
        }
    }

    fn into_changeset_rows(self, pk_flags: &[bool]) -> Vec<ChangesetRow> {
        let indirect = self.indirect;
        match (self.before, self.after) {
            (None, None) => Vec::new(),
            (None, Some(new_row)) => vec![ChangesetRow {
                op: ChangeOp::Insert,
                indirect,
                old_values: Vec::new(),
                new_values: new_row,
            }],
            (Some(old_row), None) => vec![ChangesetRow {
                op: ChangeOp::Delete,
                indirect,
                old_values: old_row,
                new_values: Vec::new(),
            }],
            (Some(old_row), Some(new_row)) => {
                if old_row == new_row {
                    Vec::new()
                } else if primary_key_changed(&old_row, &new_row, pk_flags) {
                    // Both derived rows inherit the accumulated provenance.
                    vec![
                        ChangesetRow {
                            op: ChangeOp::Delete,
                            indirect,
                            old_values: old_row,
                            new_values: Vec::new(),
                        },
                        ChangesetRow {
                            op: ChangeOp::Insert,
                            indirect,
                            old_values: Vec::new(),
                            new_values: new_row,
                        },
                    ]
                } else {
                    vec![ChangesetRow {
                        op: ChangeOp::Update,
                        indirect,
                        old_values: canonical_old_values(&old_row, &new_row, pk_flags),
                        new_values: canonical_new_values(&old_row, &new_row),
                    }]
                }
            }
        }
    }
}

/// A session that records database changes for later extraction as a
/// changeset or patchset.
///
/// In a real database engine this would hook into the DML pipeline. For now
/// it provides a programmatic API for recording changes and generating the
/// binary changeset/patchset encoding.
#[derive(Debug)]
pub struct Session {
    tables: Vec<TrackedTable>,
    changes: Vec<TrackedChange>,
    /// Current `sqlite3session_indirect()` mode; snapshotted by every
    /// `record_*` call. Defaults to direct.
    indirect: bool,
}

impl Session {
    fn tracked_table(&self, table: &str) -> Option<&TrackedTable> {
        self.tables.iter().find(|tracked| tracked.name == table)
    }

    fn validate_attached_row_width(&self, table: &str, values: &[ChangesetValue], kind: &str) {
        if let Some(tracked) = self.tracked_table(table) {
            assert_eq!(
                values.len(),
                tracked.column_count,
                "attached table row width mismatch for {kind}: table `{table}` expects {} columns but got {}",
                tracked.column_count,
                values.len()
            );
        }
    }

    fn validate_attached_update_width(
        &self,
        table: &str,
        old_values: &[ChangesetValue],
        new_values: &[ChangesetValue],
    ) {
        if let Some(tracked) = self.tracked_table(table) {
            assert_eq!(
                old_values.len(),
                tracked.column_count,
                "attached table row width mismatch for update old values: table `{table}` expects {} columns but got {}",
                tracked.column_count,
                old_values.len()
            );
            assert_eq!(
                new_values.len(),
                tracked.column_count,
                "attached table row width mismatch for update new values: table `{table}` expects {} columns but got {}",
                tracked.column_count,
                new_values.len()
            );
        }
    }

    /// Create a new, empty session.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            changes: Vec::new(),
            indirect: false,
        }
    }

    /// Set the session's indirect mode (`sqlite3session_indirect()`).
    ///
    /// While `true`, every subsequently recorded change is marked indirect.
    /// A key touched by both direct and indirect operations is emitted as
    /// direct, matching SQLite's "indirect only if every contributing change
    /// was indirect" rule.
    pub fn set_indirect(&mut self, indirect: bool) {
        self.indirect = indirect;
    }

    /// Current indirect mode. New sessions start direct.
    #[must_use]
    pub const fn indirect(&self) -> bool {
        self.indirect
    }

    /// Attach a table for change tracking.
    ///
    /// `pk_flags` indicates which columns are part of the primary key.
    ///
    /// SQLite session changesets only track tables with an explicit primary
    /// key. Attached tables with no key columns are ignored when generating
    /// changesets or patchsets.
    pub fn attach_table(&mut self, name: &str, column_count: usize, pk_flags: Vec<bool>) {
        assert_eq!(
            pk_flags.len(),
            column_count,
            "pk_flags length must match column_count"
        );
        self.tables.push(TrackedTable {
            name: name.to_owned(),
            column_count,
            pk_flags,
        });
    }

    /// Record an INSERT operation.
    pub fn record_insert(&mut self, table: &str, new_values: Vec<ChangesetValue>) {
        self.validate_attached_row_width(table, &new_values, "insert");
        self.changes.push(TrackedChange {
            table_name: table.to_owned(),
            op: ChangeOp::Insert,
            indirect: self.indirect,
            old_values: Vec::new(),
            new_values,
        });
    }

    /// Record a DELETE operation.
    pub fn record_delete(&mut self, table: &str, old_values: Vec<ChangesetValue>) {
        self.validate_attached_row_width(table, &old_values, "delete");
        self.changes.push(TrackedChange {
            table_name: table.to_owned(),
            op: ChangeOp::Delete,
            indirect: self.indirect,
            old_values,
            new_values: Vec::new(),
        });
    }

    /// Record an UPDATE operation.
    ///
    /// `old_values` and `new_values` must have the same length. Use
    /// [`ChangesetValue::Undefined`] for columns that did not change.
    pub fn record_update(
        &mut self,
        table: &str,
        old_values: Vec<ChangesetValue>,
        new_values: Vec<ChangesetValue>,
    ) {
        self.validate_attached_update_width(table, &old_values, &new_values);
        self.changes.push(TrackedChange {
            table_name: table.to_owned(),
            op: ChangeOp::Update,
            indirect: self.indirect,
            old_values,
            new_values,
        });
    }

    /// Generate a [`Changeset`] from all recorded changes.
    #[must_use]
    pub fn changeset(&self) -> Changeset {
        self.build_changeset_impl()
    }

    /// Generate a patchset (compact binary format).
    #[must_use]
    pub fn patchset(&self) -> Vec<u8> {
        let cs = self.build_changeset_impl();
        cs.encode_patchset()
    }

    /// Internal: collate tracked changes into per-table changeset sections.
    fn build_changeset_impl(&self) -> Changeset {
        let mut tables = Vec::new();
        // Emit tables in the order they were attached (deterministic).
        for tracked in &self.tables {
            if !has_primary_key(&tracked.pk_flags) {
                continue;
            }

            let mut pending = Vec::<PendingRowChange>::new();
            for change in self
                .changes
                .iter()
                .filter(|change| change.table_name == tracked.name)
            {
                assert_tracked_change_width(change, tracked);
                let key_source = match change.op {
                    ChangeOp::Insert => &change.new_values,
                    ChangeOp::Delete | ChangeOp::Update => &change.old_values,
                };
                if !primary_key_values_are_trackable(key_source, &tracked.pk_flags) {
                    continue;
                }
                if let Some(index) = pending
                    .iter()
                    .position(|existing| existing.matches_change(change, &tracked.pk_flags))
                {
                    // Net no-op entries stay in `pending`: their images emit
                    // nothing, but their indirect provenance must survive for
                    // any later operation on the same key.
                    pending[index].merge(change, tracked.column_count);
                } else {
                    pending.push(PendingRowChange::from_tracked(change, tracked.column_count));
                }
            }

            let rows = pending
                .into_iter()
                .flat_map(|change| change.into_changeset_rows(&tracked.pk_flags))
                .collect::<Vec<_>>();
            if !rows.is_empty() {
                tables.push(TableChangeset {
                    info: TableInfo {
                        name: tracked.name.clone(),
                        column_count: tracked.column_count,
                        pk_flags: tracked.pk_flags.clone(),
                    },
                    rows,
                });
            }
        }
        // Changes for unattached tables, or attached tables without an
        // explicit primary key, are intentionally dropped to match SQLite
        // session semantics.
        Changeset {
            kind: ChangesetKind::Changeset,
            tables,
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Changeset application
// ---------------------------------------------------------------------------

/// Outcome of applying a changeset to a target dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// All changes were applied (some may have been skipped via `OmitChange`).
    Success { applied: usize, skipped: usize },
    /// The apply was aborted by the conflict handler.
    Aborted { applied: usize },
}

/// A simple in-memory "database" for testing changeset application.
///
/// Maps `table_name -> Vec<row>` where each row is `Vec<SqliteValue>`.
/// This is intentionally minimal; the real apply engine would operate on
/// the B-tree layer.
#[derive(Debug, Clone, Default)]
pub struct SimpleTarget {
    pub tables: std::collections::HashMap<String, Vec<Vec<SqliteValue>>>,
}

/// Result of applying a single row change: `Ok(applied)` or `Err(applied)`
/// meaning abort with that many previously applied rows.
type RowApplyResult = Result<bool, usize>;

impl SimpleTarget {
    /// Apply a changeset to this target, using `handler` for conflict
    /// resolution.
    pub fn apply<F>(&mut self, changeset: &Changeset, mut handler: F) -> ApplyOutcome
    where
        F: FnMut(ConflictType, &ChangesetRow) -> ConflictAction,
    {
        let original_tables = self.tables.clone();
        let mut touched_tables = Vec::new();
        let mut applied = 0usize;
        let mut skipped = 0usize;

        for tc in &changeset.tables {
            if !touched_tables
                .iter()
                .any(|name: &String| name == &tc.info.name)
            {
                touched_tables.push(tc.info.name.clone());
            }
            let rows = self.tables.entry(tc.info.name.clone()).or_default();
            for change in &tc.rows {
                let result = match change.op {
                    ChangeOp::Insert => {
                        Self::apply_insert(rows, &tc.info.pk_flags, change, &mut handler, applied)
                    }
                    ChangeOp::Delete => {
                        Self::apply_delete(rows, &tc.info.pk_flags, change, &mut handler, applied)
                    }
                    ChangeOp::Update => {
                        Self::apply_update(rows, &tc.info.pk_flags, change, &mut handler, applied)
                    }
                };
                match result {
                    Ok(true) => applied += 1,
                    Ok(false) => skipped += 1,
                    Err(n) => {
                        self.tables = original_tables;
                        for table_name in touched_tables {
                            self.tables.entry(table_name).or_default();
                        }
                        return ApplyOutcome::Aborted { applied: n };
                    }
                }
            }
        }
        ApplyOutcome::Success { applied, skipped }
    }

    fn apply_insert<F>(
        rows: &mut Vec<Vec<SqliteValue>>,
        pk_flags: &[bool],
        change: &ChangesetRow,
        handler: &mut F,
        applied: usize,
    ) -> RowApplyResult
    where
        F: FnMut(ConflictType, &ChangesetRow) -> ConflictAction,
    {
        let new_row: Vec<SqliteValue> = change
            .new_values
            .iter()
            .map(ChangesetValue::to_sqlite)
            .collect();
        if Self::find_row_by_pk(rows, pk_flags, &new_row).is_some() {
            match handler(ConflictType::Conflict, change) {
                ConflictAction::OmitChange => return Ok(false),
                ConflictAction::Replace => {
                    let idx =
                        Self::find_row_by_pk(rows, pk_flags, &new_row).expect("row just found");
                    rows[idx] = new_row;
                    return Ok(true);
                }
                ConflictAction::Abort => return Err(applied),
            }
        }
        rows.push(new_row);
        Ok(true)
    }

    fn apply_delete<F>(
        rows: &mut Vec<Vec<SqliteValue>>,
        pk_flags: &[bool],
        change: &ChangesetRow,
        handler: &mut F,
        applied: usize,
    ) -> RowApplyResult
    where
        F: FnMut(ConflictType, &ChangesetRow) -> ConflictAction,
    {
        let pk_target: Vec<SqliteValue> = change
            .old_values
            .iter()
            .map(ChangesetValue::to_sqlite)
            .collect();
        if let Some(idx) = Self::find_row_by_pk(rows, pk_flags, &pk_target) {
            let old_match =
                change
                    .old_values
                    .iter()
                    .zip(rows[idx].iter())
                    .all(|(cv, sv)| match cv {
                        ChangesetValue::Undefined => true,
                        _ => cv.to_sqlite() == *sv,
                    });
            if !old_match {
                match handler(ConflictType::Data, change) {
                    ConflictAction::OmitChange => return Ok(false),
                    ConflictAction::Replace => {
                        rows.remove(idx);
                        return Ok(true);
                    }
                    ConflictAction::Abort => return Err(applied),
                }
            }
            rows.remove(idx);
            Ok(true)
        } else {
            match handler(ConflictType::NotFound, change) {
                ConflictAction::OmitChange => Ok(false),
                ConflictAction::Replace | ConflictAction::Abort => Err(applied),
            }
        }
    }

    fn apply_update<F>(
        rows: &mut Vec<Vec<SqliteValue>>,
        pk_flags: &[bool],
        change: &ChangesetRow,
        handler: &mut F,
        applied: usize,
    ) -> RowApplyResult
    where
        F: FnMut(ConflictType, &ChangesetRow) -> ConflictAction,
    {
        let old_row: Vec<SqliteValue> = change
            .old_values
            .iter()
            .map(ChangesetValue::to_sqlite)
            .collect();
        if let Some(idx) = Self::find_row_by_pk(rows, pk_flags, &old_row) {
            let old_match =
                change
                    .old_values
                    .iter()
                    .zip(rows[idx].iter())
                    .all(|(cv, sv)| match cv {
                        ChangesetValue::Undefined => true,
                        _ => cv.to_sqlite() == *sv,
                    });
            if !old_match {
                match handler(ConflictType::Data, change) {
                    ConflictAction::OmitChange => return Ok(false),
                    ConflictAction::Replace => {}
                    ConflictAction::Abort => return Err(applied),
                }
            }
            let original_row = rows[idx].clone();
            let mut updated_row = original_row.clone();
            for (i, nv) in change.new_values.iter().enumerate() {
                if *nv != ChangesetValue::Undefined
                    && let Some(cell) = updated_row.get_mut(i)
                {
                    *cell = nv.to_sqlite();
                }
            }

            let pk_changed = pk_flags
                .iter()
                .enumerate()
                .filter(|&(_, &is_pk)| is_pk)
                .any(|(i, _)| original_row.get(i) != updated_row.get(i));
            if pk_changed
                && let Some(conflict_idx) =
                    rows.iter().enumerate().find_map(|(candidate_idx, row)| {
                        (candidate_idx != idx
                            && pk_flags
                                .iter()
                                .enumerate()
                                .filter(|&(_, &is_pk)| is_pk)
                                .all(|(i, _)| {
                                    row.get(i)
                                        .zip(updated_row.get(i))
                                        .is_some_and(|(a, b)| a == b)
                                }))
                        .then_some(candidate_idx)
                    })
            {
                match handler(ConflictType::Conflict, change) {
                    ConflictAction::OmitChange => return Ok(false),
                    ConflictAction::Replace => {
                        rows.remove(conflict_idx);
                        let target_idx = if conflict_idx < idx { idx - 1 } else { idx };
                        rows[target_idx] = updated_row;
                        return Ok(true);
                    }
                    ConflictAction::Abort => return Err(applied),
                }
            }
            rows[idx] = updated_row;
            Ok(true)
        } else {
            match handler(ConflictType::NotFound, change) {
                ConflictAction::OmitChange => Ok(false),
                ConflictAction::Replace | ConflictAction::Abort => Err(applied),
            }
        }
    }

    fn find_row_by_pk(
        rows: &[Vec<SqliteValue>],
        pk_flags: &[bool],
        target: &[SqliteValue],
    ) -> Option<usize> {
        if !has_primary_key(pk_flags) {
            return rows.iter().position(|row| row == target);
        }
        rows.iter().position(|row| {
            pk_flags
                .iter()
                .enumerate()
                .filter(|&(_, &is_pk)| is_pk)
                .all(|(i, _)| row.get(i).zip(target.get(i)).is_some_and(|(a, b)| a == b))
        })
    }
}

// ---------------------------------------------------------------------------
// Varint helpers (re-exported for convenience)
// ---------------------------------------------------------------------------

/// Compute the byte length of a varint-encoded value.
#[must_use]
pub const fn changeset_varint_len(value: u64) -> usize {
    varint_len(value)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_name_matches_crate_suffix() {
        let expected = env!("CARGO_PKG_NAME")
            .strip_prefix("fsqlite-ext-")
            .expect("extension crates should use fsqlite-ext-* naming");
        assert_eq!(extension_name(), expected);
    }

    // -----------------------------------------------------------------------
    // ChangeOp round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_change_op_byte_roundtrip() {
        for op in [ChangeOp::Insert, ChangeOp::Delete, ChangeOp::Update] {
            assert_eq!(ChangeOp::from_byte(op.as_byte()), Some(op));
        }
        assert_eq!(ChangeOp::from_byte(0xFF), None);
    }

    #[test]
    fn test_change_op_byte_values() {
        assert_eq!(ChangeOp::Insert.as_byte(), 18);
        assert_eq!(ChangeOp::Delete.as_byte(), 9);
        assert_eq!(ChangeOp::Update.as_byte(), 23);
    }

    // -----------------------------------------------------------------------
    // ChangesetValue encoding / decoding
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_value_undefined() {
        let mut buf = Vec::new();
        ChangesetValue::Undefined.encode(&mut buf);
        assert_eq!(buf, [VAL_UNDEFINED]);
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Undefined);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_changeset_value_null() {
        let mut buf = Vec::new();
        ChangesetValue::Null.encode(&mut buf);
        assert_eq!(buf, [VAL_NULL]);
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Null);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_changeset_value_integer() {
        let mut buf = Vec::new();
        ChangesetValue::Integer(42).encode(&mut buf);
        assert_eq!(buf[0], VAL_INTEGER);
        assert_eq!(&buf[1..], 42_i64.to_be_bytes());
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Integer(42));
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_changeset_value_integer_negative() {
        let mut buf = Vec::new();
        ChangesetValue::Integer(-12_345).encode(&mut buf);
        let (val, _) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Integer(-12_345));
    }

    #[test]
    fn test_changeset_value_real() {
        let mut buf = Vec::new();
        ChangesetValue::Real(1.23).encode(&mut buf);
        assert_eq!(buf[0], VAL_REAL);
        assert_eq!(&buf[1..], 1.23_f64.to_be_bytes());
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Real(1.23));
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_changeset_value_text() {
        let mut buf = Vec::new();
        ChangesetValue::Text("hello".into()).encode(&mut buf);
        assert_eq!(buf[0], VAL_TEXT);
        // varint(5) = 0x05, then b"hello"
        assert_eq!(buf[1], 5);
        assert_eq!(&buf[2..], b"hello");
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Text("hello".into()));
        assert_eq!(consumed, 7); // 1 type + 1 varint + 5 data
    }

    #[test]
    fn test_changeset_value_text_empty() {
        let mut buf = Vec::new();
        ChangesetValue::Text(String::new()).encode(&mut buf);
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Text(String::new()));
        assert_eq!(consumed, 2); // 1 type + 1 varint(0)
    }

    #[test]
    fn test_changeset_value_blob() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let mut buf = Vec::new();
        ChangesetValue::Blob(data.clone()).encode(&mut buf);
        assert_eq!(buf[0], VAL_BLOB);
        assert_eq!(buf[1], 4); // varint(4)
        assert_eq!(&buf[2..], &data);
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Blob(data));
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_changeset_value_decode_bad_type() {
        assert!(ChangesetValue::decode(&[0xFF], 0).is_none());
    }

    #[test]
    fn test_changeset_value_decode_truncated() {
        // Integer needs 9 bytes total, give only 5.
        assert!(ChangesetValue::decode(&[VAL_INTEGER, 0, 0, 0, 0], 0).is_none());
    }

    // -----------------------------------------------------------------------
    // TableInfo encoding / decoding
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_info_roundtrip() {
        let info = TableInfo {
            name: "users".to_owned(),
            column_count: 3,
            pk_flags: vec![true, false, false],
        };
        let mut buf = Vec::new();
        info.encode(&mut buf);

        assert_eq!(buf[0], TABLE_HEADER_BYTE);
        let (decoded, consumed) = TableInfo::decode(&buf, 0).unwrap();
        assert_eq!(decoded, info);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_table_info_header_byte() {
        let info = TableInfo {
            name: "t".to_owned(),
            column_count: 1,
            pk_flags: vec![true],
        };
        let mut buf = Vec::new();
        info.encode(&mut buf);
        assert_eq!(buf[0], 0x54); // 'T'
    }

    #[test]
    fn test_table_info_nul_terminated_name() {
        let info = TableInfo {
            name: "orders".to_owned(),
            column_count: 2,
            pk_flags: vec![true, false],
        };
        let mut buf = Vec::new();
        info.encode(&mut buf);
        // Last byte should be NUL terminator.
        assert_eq!(*buf.last().unwrap(), 0x00);
    }

    // -----------------------------------------------------------------------
    // Session — basic tracking
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_create() {
        let session = Session::new();
        assert!(session.tables.is_empty());
        assert!(session.changes.is_empty());
    }

    #[test]
    fn test_session_attach_table() {
        let mut session = Session::new();
        session.attach_table("users", 3, vec![true, false, false]);
        assert_eq!(session.tables.len(), 1);
        assert_eq!(session.tables[0].name, "users");
    }

    #[test]
    fn test_session_record_insert() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
        );
        let cs = session.changeset();
        assert_eq!(cs.tables.len(), 1);
        assert_eq!(cs.tables[0].rows.len(), 1);
        assert_eq!(cs.tables[0].rows[0].op, ChangeOp::Insert);
    }

    #[test]
    fn test_session_record_delete() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_delete(
            "t",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
        );
        let cs = session.changeset();
        assert_eq!(cs.tables[0].rows[0].op, ChangeOp::Delete);
    }

    #[test]
    fn test_session_record_update() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_update(
            "t",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
            vec![ChangesetValue::Undefined, ChangesetValue::Text("b".into())],
        );
        let cs = session.changeset();
        let row = &cs.tables[0].rows[0];
        assert_eq!(row.op, ChangeOp::Update);
        assert_eq!(row.old_values[1], ChangesetValue::Text("a".into()));
        assert_eq!(row.new_values[0], ChangesetValue::Undefined);
        assert_eq!(row.new_values[1], ChangesetValue::Text("b".into()));
    }

    #[test]
    fn test_session_multiple_tables() {
        let mut session = Session::new();
        session.attach_table("a", 1, vec![true]);
        session.attach_table("b", 1, vec![true]);
        session.record_insert("a", vec![ChangesetValue::Integer(1)]);
        session.record_insert("b", vec![ChangesetValue::Integer(2)]);
        let cs = session.changeset();
        assert_eq!(cs.tables.len(), 2);
        assert_eq!(cs.tables[0].info.name, "a");
        assert_eq!(cs.tables[1].info.name, "b");
    }

    #[test]
    fn test_session_pk_columns() {
        let mut session = Session::new();
        session.attach_table("t", 3, vec![true, false, true]);
        let cs = session.changeset();
        // Even with no changes, table metadata is not emitted (no rows).
        assert!(cs.tables.is_empty());
        // Add a change so the table shows up.
        session.record_insert(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("x".into()),
                ChangesetValue::Integer(2),
            ],
        );
        let cs = session.changeset();
        assert_eq!(cs.tables[0].info.pk_flags, vec![true, false, true]);
    }

    // -----------------------------------------------------------------------
    // Changeset binary format
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_binary_format() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("hi".into()),
            ],
        );
        let encoded = session.changeset().encode();
        // Table header: 'T', varint(2), pk[1,0], "t\0"
        assert_eq!(encoded[0], 0x54);
        // Verify we can decode it back.
        let decoded = Changeset::decode(&encoded).unwrap();
        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.tables[0].info.name, "t");
        assert_eq!(decoded.tables[0].rows[0].op, ChangeOp::Insert);
    }

    #[test]
    fn test_session_changeset_coalesces_net_row_effects() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
        );
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("bob".into()),
                ChangesetValue::Integer(50),
            ],
        );
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("bob".into()),
                ChangesetValue::Integer(50),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Undefined,
                ChangesetValue::Integer(75),
            ],
        );
        session.record_delete(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
        );

        let changeset = session.changeset();
        assert_eq!(changeset.tables.len(), 1);
        assert_eq!(changeset.tables[0].rows.len(), 1);
        assert_eq!(changeset.tables[0].rows[0].op, ChangeOp::Insert);
        assert_eq!(
            changeset.tables[0].rows[0].new_values,
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("bob".into()),
                ChangesetValue::Integer(75),
            ]
        );
    }

    #[test]
    fn test_session_changeset_tracks_follow_on_changes_after_pk_update() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
        );
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Undefined,
                ChangesetValue::Undefined,
            ],
        );
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("ally".into()),
                ChangesetValue::Undefined,
            ],
        );

        let changeset = session.changeset();
        assert_eq!(changeset.tables.len(), 1);
        assert_eq!(changeset.tables[0].rows.len(), 1);
        assert_eq!(changeset.tables[0].rows[0].op, ChangeOp::Insert);
        assert_eq!(
            changeset.tables[0].rows[0].new_values,
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("ally".into()),
                ChangesetValue::Integer(100),
            ]
        );
    }

    #[test]
    fn test_session_changeset_existing_row_pk_update_emits_delete_then_insert() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("ally".into()),
                ChangesetValue::Undefined,
            ],
        );

        let changeset = session.changeset();
        assert_eq!(changeset.tables.len(), 1);
        assert_eq!(changeset.tables[0].rows.len(), 2);
        assert_eq!(changeset.tables[0].rows[0].op, ChangeOp::Delete);
        assert_eq!(
            changeset.tables[0].rows[0].old_values,
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ]
        );
        assert_eq!(changeset.tables[0].rows[1].op, ChangeOp::Insert);
        assert_eq!(
            changeset.tables[0].rows[1].new_values,
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("ally".into()),
                ChangesetValue::Integer(100),
            ]
        );
    }

    #[test]
    fn test_session_changeset_existing_row_pk_update_then_delete_emits_delete_only() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("ally".into()),
                ChangesetValue::Undefined,
            ],
        );
        session.record_delete(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("ally".into()),
                ChangesetValue::Integer(100),
            ],
        );

        let changeset = session.changeset();
        assert_eq!(changeset.tables.len(), 1);
        assert_eq!(changeset.tables[0].rows.len(), 1);
        assert_eq!(changeset.tables[0].rows[0].op, ChangeOp::Delete);
        assert_eq!(
            changeset.tables[0].rows[0].old_values,
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "attached table row width mismatch for update new values")]
    fn test_session_record_update_rejects_attached_table_width_mismatch() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("ally".into()),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "attached table row width mismatch for insert")]
    fn test_session_record_insert_rejects_attached_table_width_mismatch() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
            ],
        );
    }

    #[test]
    fn test_session_pk_update_emits_delete_and_insert() {
        let mut session = Session::new();
        session.attach_table("accounts", 2, vec![true, false]);
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
            ],
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("alicia".into()),
            ],
        );

        let changeset = session.changeset();
        assert_eq!(changeset.tables.len(), 1);
        assert_eq!(changeset.tables[0].rows.len(), 2);
        assert_eq!(changeset.tables[0].rows[0].op, ChangeOp::Delete);
        assert_eq!(
            changeset.tables[0].rows[0].old_values,
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
            ]
        );
        assert_eq!(changeset.tables[0].rows[1].op, ChangeOp::Insert);
        assert_eq!(
            changeset.tables[0].rows[1].new_values,
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("alicia".into()),
            ]
        );
    }

    #[test]
    fn test_session_pk_evolution_coalesces_to_net_effect() {
        let mut session = Session::new();
        session.attach_table("accounts", 2, vec![true, false]);
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
            ],
        );
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
            ],
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("alicia".into()),
            ],
        );
        session.record_delete(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("alicia".into()),
            ],
        );

        let changeset = session.changeset();
        assert!(changeset.tables.is_empty());
    }

    #[test]
    #[should_panic(expected = "insert values length must match attached table column_count")]
    fn test_session_changeset_rejects_malformed_insert_attached_after_recording() {
        let mut session = Session::new();
        session.record_insert("accounts", vec![ChangesetValue::Integer(1)]);
        session.attach_table("accounts", 2, vec![true, false]);

        let _ = session.changeset();
    }

    #[test]
    fn test_changeset_roundtrip() {
        let mut session = Session::new();
        session.attach_table("users", 3, vec![true, false, false]);
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("Alice".into()),
                ChangesetValue::Integer(30),
            ],
        );
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("Bob".into()),
                ChangesetValue::Integer(25),
            ],
        );
        session.record_delete(
            "users",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("Alice".into()),
                ChangesetValue::Integer(30),
            ],
        );
        session.record_update(
            "users",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("Bob".into()),
                ChangesetValue::Integer(25),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("Robert".into()),
                ChangesetValue::Undefined,
            ],
        );

        let cs = session.changeset();
        let encoded = cs.encode();
        let decoded = Changeset::decode(&encoded).unwrap();
        assert_eq!(decoded, cs);
    }

    // -----------------------------------------------------------------------
    // Changeset inversion
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_invert_insert() {
        let row = ChangesetRow {
            op: ChangeOp::Insert,
            indirect: false,
            old_values: Vec::new(),
            new_values: vec![ChangesetValue::Integer(1)],
        };
        let inv = row.invert();
        assert_eq!(inv.op, ChangeOp::Delete);
        assert_eq!(inv.old_values, vec![ChangesetValue::Integer(1)]);
        assert!(inv.new_values.is_empty());
    }

    #[test]
    fn test_changeset_invert_delete() {
        let row = ChangesetRow {
            op: ChangeOp::Delete,
            indirect: false,
            old_values: vec![ChangesetValue::Integer(1)],
            new_values: Vec::new(),
        };
        let inv = row.invert();
        assert_eq!(inv.op, ChangeOp::Insert);
        assert!(inv.old_values.is_empty());
        assert_eq!(inv.new_values, vec![ChangesetValue::Integer(1)]);
    }

    #[test]
    fn test_changeset_invert_update() {
        let row = ChangesetRow {
            op: ChangeOp::Update,
            indirect: false,
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old".into()),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new".into()),
            ],
        };
        let inv = row.invert();
        assert_eq!(inv.op, ChangeOp::Update);
        assert_eq!(inv.old_values[0], ChangesetValue::Undefined);
        assert_eq!(inv.old_values[1], ChangesetValue::Text("new".into()));
        assert_eq!(inv.new_values[0], ChangesetValue::Integer(1));
        assert_eq!(inv.new_values[1], ChangesetValue::Text("old".into()));
    }

    // -----------------------------------------------------------------------
    // Changeset concat
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_concat() {
        let mut cs1 = Changeset::new();
        cs1.tables.push(TableChangeset {
            info: TableInfo {
                name: "a".to_owned(),
                column_count: 1,
                pk_flags: vec![true],
            },
            rows: vec![ChangesetRow {
                op: ChangeOp::Insert,
                indirect: false,
                old_values: Vec::new(),
                new_values: vec![ChangesetValue::Integer(1)],
            }],
        });
        let cs2 = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "b".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    indirect: false,
                    old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(2)],
                }],
            }],
        };
        cs1.concat(&cs2).expect("same-kind concat");
        assert_eq!(cs1.tables.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Patchset format
    // -----------------------------------------------------------------------

    #[test]
    fn test_patchset_format_omits_old_values() {
        let mut session = Session::new();
        session.attach_table("t", 3, vec![true, false, false]);
        session.record_update(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old_name".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".into()),
                ChangesetValue::Undefined,
            ],
        );
        let changeset_bytes = session.changeset().encode();
        let patchset_bytes = session.patchset();
        // Patchset should be smaller (omits non-PK old values).
        assert!(
            patchset_bytes.len() < changeset_bytes.len(),
            "patchset ({}) should be smaller than changeset ({})",
            patchset_bytes.len(),
            changeset_bytes.len(),
        );
    }

    #[test]
    fn test_patchset_update_uses_single_record_layout() {
        let pk_flags = vec![true, false, false];
        let row = ChangesetRow {
            op: ChangeOp::Update,
            indirect: false,
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old_name".into()),
                ChangesetValue::Integer(100),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".into()),
                ChangesetValue::Undefined,
            ],
        };

        let mut patchset = Vec::new();
        row.encode_patchset(&mut patchset, &pk_flags);

        let mut expected = vec![OP_UPDATE, 0x00];
        ChangesetValue::Integer(1).encode(&mut expected);
        ChangesetValue::Text("new_name".into()).encode(&mut expected);
        ChangesetValue::Undefined.encode(&mut expected);
        assert_eq!(patchset, expected);
    }

    #[test]
    fn test_patchset_insert_same_as_changeset() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
        );
        let changeset_bytes = session.changeset().encode();
        let patchset_bytes = session.patchset();
        assert_eq!(changeset_bytes[1..], patchset_bytes[1..]);
        assert_eq!(changeset_bytes[0], CHANGESET_TABLE_HEADER_BYTE);
        assert_eq!(patchset_bytes[0], PATCHSET_TABLE_HEADER_BYTE);
    }

    #[test]
    fn test_patchset_decode_rehydrates_pk_old_values() {
        let mut session = Session::new();
        session.attach_table("t", 3, vec![true, false, false]);
        session.record_update(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old_name".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".into()),
                ChangesetValue::Undefined,
            ],
        );

        let patchset_bytes = session.patchset();
        let decoded = Changeset::decode_patchset(&patchset_bytes).unwrap();
        let row = &decoded.tables[0].rows[0];

        assert_eq!(decoded.encode_patchset(), patchset_bytes);
        assert_eq!(
            row.old_values,
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Undefined,
                ChangesetValue::Undefined,
            ]
        );
        assert_eq!(
            row.new_values,
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".into()),
                ChangesetValue::Undefined,
            ]
        );
    }

    #[test]
    fn test_patchset_decode_truncated_update_returns_none() {
        let mut session = Session::new();
        session.attach_table("t", 3, vec![true, false, false]);
        session.record_update(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old_name".into()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".into()),
                ChangesetValue::Undefined,
            ],
        );

        let mut patchset_bytes = session.patchset();
        patchset_bytes.pop();
        assert!(Changeset::decode_patchset(&patchset_bytes).is_err());
    }

    #[test]
    fn test_patchset_apply_matches_changeset_apply() {
        let mut session = Session::new();
        session.attach_table("accounts", 3, vec![true, false, false]);
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
        );
        session.record_insert(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("bob".into()),
                ChangesetValue::Integer(50),
            ],
        );
        session.record_update(
            "accounts",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("bob".into()),
                ChangesetValue::Integer(50),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Undefined,
                ChangesetValue::Integer(75),
            ],
        );
        session.record_delete(
            "accounts",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("alice".into()),
                ChangesetValue::Integer(100),
            ],
        );

        let changeset = session.changeset();
        let decoded_patchset = Changeset::decode_patchset(&session.patchset()).unwrap();

        let mut changeset_target = SimpleTarget::default();
        let mut patchset_target = SimpleTarget::default();
        let changeset_outcome = changeset_target.apply(&changeset, |_, _| ConflictAction::Abort);
        let patchset_outcome =
            patchset_target.apply(&decoded_patchset, |_, _| ConflictAction::Abort);

        assert_eq!(
            changeset_outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0,
            }
        );
        assert_eq!(patchset_outcome, changeset_outcome);
        assert_eq!(patchset_target.tables, changeset_target.tables);
        assert_eq!(
            changeset_target.tables["accounts"],
            vec![vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("bob".into()),
                SqliteValue::Integer(75),
            ]]
        );
    }

    // -----------------------------------------------------------------------
    // Apply — successful cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_insert() {
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    indirect: false,
                    old_values: Vec::new(),
                    new_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("hello".into()),
                    ],
                }],
            }],
        };

        let mut target = SimpleTarget::default();
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0
            }
        );
        assert_eq!(
            target.tables["t"],
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("hello".into())
            ]]
        );
    }

    #[test]
    fn test_apply_delete() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("hello".into()),
            ]],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("hello".into()),
                    ],
                    new_values: Vec::new(),
                }],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0
            }
        );
        assert!(target.tables["t"].is_empty());
    }

    #[test]
    fn test_apply_update() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("old".into()),
            ]],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("old".into()),
                    ],
                    new_values: vec![
                        ChangesetValue::Undefined,
                        ChangesetValue::Text("new".into()),
                    ],
                }],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0
            }
        );
        assert_eq!(
            target.tables["t"][0],
            vec![SqliteValue::Integer(1), SqliteValue::Text("new".into())]
        );
    }

    // -----------------------------------------------------------------------
    // Apply — conflict scenarios
    // -----------------------------------------------------------------------

    #[test]
    fn test_conflict_not_found() {
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(999)],
                    new_values: Vec::new(),
                }],
            }],
        };
        let mut target = SimpleTarget::default();
        let mut conflict_seen = None;
        let outcome = target.apply(&cs, |ct, _| {
            conflict_seen = Some(ct);
            ConflictAction::OmitChange
        });
        assert_eq!(conflict_seen, Some(ConflictType::NotFound));
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 1
            }
        );
    }

    #[test]
    fn test_conflict_data() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("actual".into()),
            ]],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("expected".into()),
                    ],
                    new_values: Vec::new(),
                }],
            }],
        };

        let mut conflict_seen = None;
        let outcome = target.apply(&cs, |ct, _| {
            conflict_seen = Some(ct);
            ConflictAction::OmitChange
        });
        assert_eq!(conflict_seen, Some(ConflictType::Data));
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 1
            }
        );
    }

    #[test]
    fn test_conflict_unique_insert() {
        let mut target = SimpleTarget::default();
        target
            .tables
            .insert("t".to_owned(), vec![vec![SqliteValue::Integer(1)]]);

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    indirect: false,
                    old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(1)], // Duplicate PK
                }],
            }],
        };

        let mut conflict_seen = None;
        let outcome = target.apply(&cs, |ct, _| {
            conflict_seen = Some(ct);
            ConflictAction::OmitChange
        });
        assert_eq!(conflict_seen, Some(ConflictType::Conflict));
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 1
            }
        );
    }

    #[test]
    fn test_conflict_omit_skips() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1)],
                    new_values: Vec::new(),
                }],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::OmitChange);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 1
            }
        );
    }

    #[test]
    fn test_conflict_replace_insert() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("old".into()),
            ]],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    indirect: false,
                    old_values: Vec::new(),
                    new_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("replaced".into()),
                    ],
                }],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::Replace);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0
            }
        );
        assert_eq!(
            target.tables["t"][0],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("replaced".into())
            ]
        );
    }

    #[test]
    fn test_conflict_abort_stops_apply() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![
                    ChangesetRow {
                        op: ChangeOp::Delete,
                        indirect: false,
                        old_values: vec![ChangesetValue::Integer(1)],
                        new_values: Vec::new(),
                    },
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![ChangesetValue::Integer(2)],
                    },
                ],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(outcome, ApplyOutcome::Aborted { applied: 0 });
        // Second row should NOT have been applied.
        assert!(!target.tables.contains_key("t") || target.tables["t"].is_empty());
    }

    // -----------------------------------------------------------------------
    // Full round-trip: session → changeset → apply → verify
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_full_roundtrip() {
        // Build changeset via session.
        let mut session = Session::new();
        session.attach_table("users", 3, vec![true, false, false]);
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("Alice".into()),
                ChangesetValue::Integer(30),
            ],
        );
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("Bob".into()),
                ChangesetValue::Integer(25),
            ],
        );

        let cs = session.changeset();

        // Apply to empty target.
        let mut target = SimpleTarget::default();
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 2,
                skipped: 0
            }
        );
        assert_eq!(target.tables["users"].len(), 2);
        assert_eq!(
            target.tables["users"][0][1],
            SqliteValue::Text("Alice".into())
        );
        assert_eq!(
            target.tables["users"][1][1],
            SqliteValue::Text("Bob".into())
        );
    }

    #[test]
    fn test_changeset_invert_undoes_changes() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
        );

        let cs = session.changeset();
        let inv = cs.invert().expect("changeset inversion");

        // Apply original changeset.
        let mut target = SimpleTarget::default();
        target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(target.tables["t"].len(), 1);

        // Apply inverted changeset — should remove the row.
        target.apply(&inv, |_, _| ConflictAction::Abort);
        assert!(target.tables["t"].is_empty());
    }

    // -----------------------------------------------------------------------
    // ChangesetValue <-> SqliteValue conversion
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_value_from_sqlite() {
        assert_eq!(
            ChangesetValue::from_sqlite(&SqliteValue::Null),
            ChangesetValue::Null
        );
        assert_eq!(
            ChangesetValue::from_sqlite(&SqliteValue::Integer(42)),
            ChangesetValue::Integer(42)
        );
        assert_eq!(
            ChangesetValue::from_sqlite(&SqliteValue::Float(1.5)),
            ChangesetValue::Real(1.5)
        );
        assert_eq!(
            ChangesetValue::from_sqlite(&SqliteValue::Text("x".into())),
            ChangesetValue::Text("x".into())
        );
        assert_eq!(
            ChangesetValue::from_sqlite(&SqliteValue::Blob(vec![1, 2].into())),
            ChangesetValue::Blob(vec![1, 2])
        );
    }

    #[test]
    fn test_changeset_value_to_sqlite() {
        assert_eq!(ChangesetValue::Undefined.to_sqlite(), SqliteValue::Null);
        assert_eq!(ChangesetValue::Null.to_sqlite(), SqliteValue::Null);
        assert_eq!(
            ChangesetValue::Integer(7).to_sqlite(),
            SqliteValue::Integer(7)
        );
        assert_eq!(
            ChangesetValue::Real(2.5).to_sqlite(),
            SqliteValue::Float(2.5)
        );
        assert_eq!(
            ChangesetValue::Text("hi".into()).to_sqlite(),
            SqliteValue::Text("hi".into())
        );
        assert_eq!(
            ChangesetValue::Blob(vec![0xAB]).to_sqlite(),
            SqliteValue::Blob(vec![0xAB].into())
        );
    }

    // -----------------------------------------------------------------------
    // ChangeOp edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_change_op_from_byte_exhaustive_invalid() {
        for b in 0..=255u8 {
            if matches!(b, 0x12 | 0x09 | 0x17) {
                assert!(ChangeOp::from_byte(b).is_some());
            } else {
                assert!(
                    ChangeOp::from_byte(b).is_none(),
                    "byte {b:#x} should be None"
                );
            }
        }
    }

    #[test]
    fn test_change_op_copy_clone_eq() {
        let a = ChangeOp::Insert;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(ChangeOp::Insert, ChangeOp::Delete);
        assert_ne!(ChangeOp::Delete, ChangeOp::Update);
    }

    #[test]
    fn test_change_op_debug() {
        let s = format!("{:?}", ChangeOp::Insert);
        assert_eq!(s, "Insert");
    }

    // -----------------------------------------------------------------------
    // ChangesetValue edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_value_integer_boundaries() {
        for &val in &[i64::MIN, i64::MAX, 0, -1, 1] {
            let mut buf = Vec::new();
            ChangesetValue::Integer(val).encode(&mut buf);
            let (decoded, _) = ChangesetValue::decode(&buf, 0).unwrap();
            assert_eq!(decoded, ChangesetValue::Integer(val));
        }
    }

    #[test]
    fn test_changeset_value_real_special() {
        for &val in &[
            0.0,
            -0.0,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::EPSILON,
        ] {
            let mut buf = Vec::new();
            ChangesetValue::Real(val).encode(&mut buf);
            let (decoded, _) = ChangesetValue::decode(&buf, 0).unwrap();
            assert_eq!(decoded, ChangesetValue::Real(val));
        }
    }

    #[test]
    fn test_changeset_value_real_nan_roundtrip() {
        let mut buf = Vec::new();
        ChangesetValue::Real(f64::NAN).encode(&mut buf);
        let (decoded, _) = ChangesetValue::decode(&buf, 0).unwrap();
        if let ChangesetValue::Real(f) = decoded {
            assert!(f.is_nan());
        } else {
            panic!("expected Real");
        }
    }

    #[test]
    fn test_changeset_value_blob_empty() {
        let mut buf = Vec::new();
        ChangesetValue::Blob(Vec::new()).encode(&mut buf);
        let (decoded, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(decoded, ChangesetValue::Blob(Vec::new()));
        assert_eq!(consumed, 2); // type + varint(0)
    }

    #[test]
    fn test_changeset_value_text_unicode() {
        let text = "\u{1F600}\u{1F4A9}\u{2603}"; // emoji + snowman
        let mut buf = Vec::new();
        ChangesetValue::Text(text.into()).encode(&mut buf);
        let (decoded, _) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(decoded, ChangesetValue::Text(text.into()));
    }

    #[test]
    fn test_changeset_value_decode_at_offset() {
        let mut buf = Vec::new();
        ChangesetValue::Null.encode(&mut buf); // 1 byte
        ChangesetValue::Integer(42).encode(&mut buf); // 9 bytes
        let (val, consumed) = ChangesetValue::decode(&buf, 1).unwrap();
        assert_eq!(val, ChangesetValue::Integer(42));
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_changeset_value_decode_empty_slice() {
        assert!(ChangesetValue::decode(&[], 0).is_none());
    }

    #[test]
    fn test_changeset_value_decode_offset_beyond_len() {
        assert!(ChangesetValue::decode(&[VAL_NULL], 5).is_none());
    }

    #[test]
    fn test_changeset_value_decode_truncated_real() {
        assert!(ChangesetValue::decode(&[VAL_REAL, 0, 0, 0], 0).is_none());
    }

    #[test]
    fn test_changeset_value_decode_truncated_text() {
        // Type byte + varint(10) but only 3 content bytes
        let mut buf = vec![VAL_TEXT, 10, b'a', b'b', b'c'];
        assert!(ChangesetValue::decode(&buf, 0).is_none());
        // Fix: provide exactly 10 bytes
        buf.extend_from_slice(&[0; 7]);
        // Non-UTF8 bytes should fail
        buf[5] = 0xFF;
        assert!(ChangesetValue::decode(&buf, 0).is_none());
    }

    #[test]
    fn test_changeset_value_decode_truncated_blob() {
        let buf = vec![VAL_BLOB, 5, 1, 2]; // says 5 bytes, only has 2
        assert!(ChangesetValue::decode(&buf, 0).is_none());
    }

    // -----------------------------------------------------------------------
    // ChangesetValue <-> SqliteValue round-trip
    // -----------------------------------------------------------------------

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_changeset_value_sqlite_roundtrip_all_types() {
        let values = vec![
            SqliteValue::Null,
            SqliteValue::Integer(0),
            SqliteValue::Integer(i64::MAX),
            SqliteValue::Float(3.14),
            SqliteValue::Text("".into()),
            SqliteValue::Text("test".into()),
            SqliteValue::Blob(vec![].into()),
            SqliteValue::Blob(vec![1, 2, 3].into()),
        ];
        for sv in &values {
            let cv = ChangesetValue::from_sqlite(sv);
            let back = cv.to_sqlite();
            assert_eq!(&back, sv);
        }
    }

    // -----------------------------------------------------------------------
    // TableInfo edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_info_single_column() {
        let info = TableInfo {
            name: "x".to_owned(),
            column_count: 1,
            pk_flags: vec![true],
        };
        let mut buf = Vec::new();
        info.encode(&mut buf);
        let (decoded, consumed) = TableInfo::decode(&buf, 0).unwrap();
        assert_eq!(decoded, info);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_table_info_no_pk_columns() {
        let info = TableInfo {
            name: "t".to_owned(),
            column_count: 3,
            pk_flags: vec![false, false, false],
        };
        let mut buf = Vec::new();
        info.encode(&mut buf);
        let (decoded, _) = TableInfo::decode(&buf, 0).unwrap();
        assert_eq!(decoded.pk_flags, vec![false, false, false]);
    }

    #[test]
    fn test_table_info_unicode_name() {
        let info = TableInfo {
            name: "\u{00FC}berschrift".to_owned(),
            column_count: 1,
            pk_flags: vec![true],
        };
        let mut buf = Vec::new();
        info.encode(&mut buf);
        let (decoded, _) = TableInfo::decode(&buf, 0).unwrap();
        assert_eq!(decoded.name, "\u{00FC}berschrift");
    }

    #[test]
    fn test_table_info_decode_wrong_header() {
        assert!(TableInfo::decode(&[0x00, 0x01, 0x01, b't', 0x00], 0).is_none());
    }

    #[test]
    fn test_table_info_decode_truncated() {
        assert!(TableInfo::decode(&[TABLE_HEADER_BYTE], 0).is_none());
        assert!(TableInfo::decode(&[TABLE_HEADER_BYTE, 3, 1], 0).is_none());
    }

    #[test]
    fn test_table_info_decode_at_offset() {
        let mut buf = vec![0xFF, 0xFF]; // padding
        let info = TableInfo {
            name: "t".to_owned(),
            column_count: 1,
            pk_flags: vec![true],
        };
        info.encode(&mut buf);
        let (decoded, _) = TableInfo::decode(&buf, 2).unwrap();
        assert_eq!(decoded, info);
    }

    // -----------------------------------------------------------------------
    // ChangesetRow edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_row_invert_double_is_identity() {
        let row = ChangesetRow {
            op: ChangeOp::Update,
            indirect: false,
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old".into()),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new".into()),
            ],
        };
        let double_inverted = row.invert().invert();
        assert_eq!(double_inverted, row);
    }

    #[test]
    fn test_changeset_row_encode_decode_all_ops() {
        let col_count = 2;
        for op in [ChangeOp::Insert, ChangeOp::Delete, ChangeOp::Update] {
            let row = match op {
                ChangeOp::Insert => ChangesetRow {
                    op,
                    indirect: false,
                    old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(1), ChangesetValue::Null],
                },
                ChangeOp::Delete => ChangesetRow {
                    op,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Null],
                    new_values: Vec::new(),
                },
                ChangeOp::Update => ChangesetRow {
                    op,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
                    new_values: vec![ChangesetValue::Undefined, ChangesetValue::Text("b".into())],
                },
            };
            let mut buf = Vec::new();
            row.encode_changeset(&mut buf);
            let (decoded, consumed) = ChangesetRow::decode_changeset(&buf, 0, col_count).unwrap();
            assert_eq!(decoded, row);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn test_changeset_row_decode_bad_op() {
        assert_eq!(
            ChangesetRow::decode_changeset(&[0xFF, VAL_NULL], 0, 1),
            Err(ChangesetFormatError::InvalidOperation {
                offset: 0,
                byte: 0xFF
            })
        );
    }

    // -----------------------------------------------------------------------
    // Patchset UPDATE: PK-only old values
    // -----------------------------------------------------------------------

    #[test]
    fn test_patchset_update_only_pk_old() {
        let pk_flags = vec![true, false, false];
        let row = ChangesetRow {
            op: ChangeOp::Update,
            indirect: false,
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old_name".into()),
                ChangesetValue::Integer(100),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".into()),
                ChangesetValue::Undefined,
            ],
        };
        let mut cs_buf = Vec::new();
        row.encode_changeset(&mut cs_buf);
        let mut ps_buf = Vec::new();
        row.encode_patchset(&mut ps_buf, &pk_flags);
        assert!(ps_buf.len() < cs_buf.len());
    }

    #[test]
    fn test_patchset_delete_omits_non_pk_old_values() {
        let pk_flags = vec![true, false];
        let row = ChangesetRow {
            op: ChangeOp::Delete,
            indirect: false,
            old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
            new_values: Vec::new(),
        };
        let mut cs_buf = Vec::new();
        row.encode_changeset(&mut cs_buf);
        let mut ps_buf = Vec::new();
        row.encode_patchset(&mut ps_buf, &pk_flags);
        assert!(ps_buf.len() < cs_buf.len());
    }

    // -----------------------------------------------------------------------
    // Session: explicit PK requirement
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_unattached_table_ignored() {
        let mut session = Session::new();
        session.record_insert("auto", vec![ChangesetValue::Integer(1)]);
        let cs = session.changeset();
        assert!(cs.tables.is_empty());
    }

    #[test]
    fn test_session_attached_table_without_pk_is_ignored() {
        let mut session = Session::new();
        session.attach_table("auto", 2, vec![false, false]);
        session.record_insert(
            "auto",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
        );
        let cs = session.changeset();
        assert!(cs.tables.is_empty());
    }

    #[test]
    fn test_session_rows_with_null_primary_key_are_ignored() {
        let mut session = Session::new();
        session.attach_table("accounts", 2, vec![true, false]);
        session.record_insert(
            "accounts",
            vec![ChangesetValue::Null, ChangesetValue::Text("alice".into())],
        );
        session.record_update(
            "accounts",
            vec![ChangesetValue::Null, ChangesetValue::Text("alice".into())],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("alice_2".into()),
            ],
        );
        session.record_delete(
            "accounts",
            vec![ChangesetValue::Null, ChangesetValue::Text("alice_2".into())],
        );

        let changeset = session.changeset();
        assert!(changeset.tables.is_empty());
    }

    #[test]
    fn test_session_empty_changeset() {
        let session = Session::new();
        let cs = session.changeset();
        assert!(cs.tables.is_empty());
        assert!(cs.encode().is_empty());
    }

    #[test]
    fn test_session_empty_patchset() {
        let session = Session::new();
        assert!(session.patchset().is_empty());
    }

    #[test]
    fn test_session_default_trait() {
        let session = Session::default();
        assert!(session.tables.is_empty());
    }

    // -----------------------------------------------------------------------
    // Changeset edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_default_trait() {
        let cs = Changeset::default();
        assert!(cs.tables.is_empty());
    }

    #[test]
    fn test_changeset_empty_encode_decode() {
        let cs = Changeset::new();
        let encoded = cs.encode();
        assert!(encoded.is_empty());
        let decoded = Changeset::decode(&encoded).unwrap();
        assert!(decoded.tables.is_empty());
    }

    #[test]
    fn test_changeset_invert_is_self_inverse() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![ChangesetValue::Integer(1), ChangesetValue::Text("a".into())],
        );
        session.record_delete(
            "t",
            vec![ChangesetValue::Integer(2), ChangesetValue::Text("b".into())],
        );
        session.record_update(
            "t",
            vec![ChangesetValue::Integer(3), ChangesetValue::Text("c".into())],
            vec![ChangesetValue::Undefined, ChangesetValue::Text("d".into())],
        );

        let cs = session.changeset();
        let double_inv = cs
            .invert()
            .expect("first inversion")
            .invert()
            .expect("second inversion");
        assert_eq!(double_inv, cs);
    }

    #[test]
    fn test_changeset_multi_table_encode_decode() {
        let mut session = Session::new();
        session.attach_table("a", 1, vec![true]);
        session.attach_table("b", 2, vec![true, false]);
        session.record_insert("a", vec![ChangesetValue::Integer(1)]);
        session.record_insert(
            "b",
            vec![ChangesetValue::Integer(2), ChangesetValue::Text("x".into())],
        );
        session.record_delete("a", vec![ChangesetValue::Integer(3)]);

        let cs = session.changeset();
        let encoded = cs.encode();
        let decoded = Changeset::decode(&encoded).unwrap();
        assert_eq!(decoded, cs);
    }

    // -----------------------------------------------------------------------
    // Apply: additional conflict scenarios
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_update_data_conflict_replace() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("actual".into()),
            ]],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("expected".into()),
                    ],
                    new_values: vec![
                        ChangesetValue::Undefined,
                        ChangesetValue::Text("new".into()),
                    ],
                }],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::Replace);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0
            }
        );
        assert_eq!(target.tables["t"][0][1], SqliteValue::Text("new".into()));
    }

    #[test]
    fn test_apply_update_pk_conflict_omit() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
            ],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("alice".into()),
                    ],
                    new_values: vec![
                        ChangesetValue::Integer(2),
                        ChangesetValue::Text("ally".into()),
                    ],
                }],
            }],
        };

        let mut conflict_seen = None;
        let outcome = target.apply(&cs, |conflict, _| {
            conflict_seen = Some(conflict);
            ConflictAction::OmitChange
        });
        assert_eq!(conflict_seen, Some(ConflictType::Conflict));
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 1,
            }
        );
        assert_eq!(
            target.tables["t"],
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into()),],
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into()),],
            ]
        );
    }

    #[test]
    fn test_apply_update_pk_conflict_replace_overwrites_conflicting_row() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("alice".into())],
                vec![SqliteValue::Integer(2), SqliteValue::Text("bob".into())],
            ],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("alice".into()),
                    ],
                    new_values: vec![
                        ChangesetValue::Integer(2),
                        ChangesetValue::Text("ally".into()),
                    ],
                }],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::Replace);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0,
            }
        );
        assert_eq!(
            target.tables["t"],
            vec![vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("ally".into()),
            ]]
        );
    }

    #[test]
    fn test_apply_delete_data_conflict_replace_removes() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("actual".into()),
            ]],
        );

        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("expected".into()),
                    ],
                    new_values: Vec::new(),
                }],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::Replace);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 1,
                skipped: 0
            }
        );
        assert!(target.tables["t"].is_empty());
    }

    #[test]
    fn test_apply_update_not_found_abort() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1)],
                    new_values: vec![ChangesetValue::Integer(2)],
                }],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(outcome, ApplyOutcome::Aborted { applied: 0 });
    }

    #[test]
    fn test_apply_delete_not_found_replace_aborts() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1)],
                    new_values: Vec::new(),
                }],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Replace);
        assert_eq!(outcome, ApplyOutcome::Aborted { applied: 0 });
        assert!(target.tables["t"].is_empty());
    }

    #[test]
    fn test_apply_update_not_found_replace_aborts() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    indirect: false,
                    old_values: vec![ChangesetValue::Integer(1)],
                    new_values: vec![ChangesetValue::Integer(2)],
                }],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Replace);
        assert_eq!(outcome, ApplyOutcome::Aborted { applied: 0 });
        assert!(target.tables["t"].is_empty());
    }

    #[test]
    fn test_apply_abort_rolls_back_prior_successes() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![ChangesetValue::Integer(1)],
                    },
                    ChangesetRow {
                        op: ChangeOp::Delete,
                        indirect: false,
                        old_values: vec![ChangesetValue::Integer(2)],
                        new_values: Vec::new(),
                    },
                ],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(outcome, ApplyOutcome::Aborted { applied: 1 });
        assert!(target.tables["t"].is_empty());
    }

    #[test]
    fn test_apply_multiple_rows_mixed() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(1),
                            ChangesetValue::Text("a".into()),
                        ],
                    },
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(2),
                            ChangesetValue::Text("b".into()),
                        ],
                    },
                ],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 2,
                skipped: 0
            }
        );
        assert_eq!(target.tables["t"].len(), 2);
    }

    #[test]
    fn test_apply_insert_without_pk_uses_full_row_identity() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            kind: ChangesetKind::Changeset,
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![false, false],
                },
                rows: vec![
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(1),
                            ChangesetValue::Text("a".into()),
                        ],
                    },
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(2),
                            ChangesetValue::Text("b".into()),
                        ],
                    },
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        indirect: false,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(1),
                            ChangesetValue::Text("a".into()),
                        ],
                    },
                ],
            }],
        };

        let outcome = target.apply(&cs, |_, _| ConflictAction::OmitChange);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 2,
                skipped: 1
            }
        );
        assert_eq!(target.tables["t"].len(), 2);
        assert_eq!(
            target.tables["t"],
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("a".into())],
                vec![SqliteValue::Integer(2), SqliteValue::Text("b".into())],
            ]
        );
    }

    #[test]
    fn test_apply_empty_changeset() {
        let mut target = SimpleTarget::default();
        let cs = Changeset::new();
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 0
            }
        );
    }

    // -----------------------------------------------------------------------
    // TableChangeset encoding
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_changeset_encode_patchset() {
        let tc = TableChangeset {
            info: TableInfo {
                name: "t".to_owned(),
                column_count: 2,
                pk_flags: vec![true, false],
            },
            rows: vec![ChangesetRow {
                op: ChangeOp::Insert,
                indirect: false,
                old_values: Vec::new(),
                new_values: vec![ChangesetValue::Integer(1), ChangesetValue::Null],
            }],
        };
        let mut cs_buf = Vec::new();
        tc.encode_changeset(&mut cs_buf);
        let mut ps_buf = Vec::new();
        tc.encode_patchset(&mut ps_buf);
        assert_eq!(cs_buf[0], CHANGESET_TABLE_HEADER_BYTE);
        assert_eq!(ps_buf[0], PATCHSET_TABLE_HEADER_BYTE);
        assert_eq!(cs_buf[1..], ps_buf[1..]);
    }

    // -----------------------------------------------------------------------
    // changeset_varint_len
    // -----------------------------------------------------------------------

    #[test]
    fn test_changeset_varint_len_values() {
        assert_eq!(changeset_varint_len(0), 1);
        assert_eq!(changeset_varint_len(127), 1);
        assert_eq!(changeset_varint_len(128), 2);
        assert!(changeset_varint_len(u64::MAX) > 0);
    }

    // -----------------------------------------------------------------------
    // ConflictType / ConflictAction traits
    // -----------------------------------------------------------------------

    #[test]
    fn test_conflict_type_eq() {
        assert_eq!(ConflictType::Data, ConflictType::Data);
        assert_ne!(ConflictType::Data, ConflictType::NotFound);
        assert_ne!(ConflictType::Conflict, ConflictType::Constraint);
        assert_ne!(ConflictType::Constraint, ConflictType::ForeignKey);
    }

    #[test]
    fn test_conflict_action_eq() {
        assert_eq!(ConflictAction::OmitChange, ConflictAction::OmitChange);
        assert_ne!(ConflictAction::OmitChange, ConflictAction::Replace);
        assert_ne!(ConflictAction::Replace, ConflictAction::Abort);
    }

    #[test]
    fn test_conflict_type_debug() {
        assert_eq!(format!("{:?}", ConflictType::ForeignKey), "ForeignKey");
    }

    // -----------------------------------------------------------------------
    // Extension name
    // -----------------------------------------------------------------------

    #[test]
    fn test_extension_name_value() {
        assert_eq!(extension_name(), "session");
    }

    // -----------------------------------------------------------------------
    // ApplyOutcome
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_outcome_debug() {
        let outcome = ApplyOutcome::Success {
            applied: 5,
            skipped: 2,
        };
        let s = format!("{:?}", outcome);
        assert!(s.contains('5'));
        assert!(s.contains('2'));
    }

    #[test]
    fn test_apply_outcome_aborted_eq() {
        assert_eq!(
            ApplyOutcome::Aborted { applied: 3 },
            ApplyOutcome::Aborted { applied: 3 }
        );
        assert_ne!(
            ApplyOutcome::Aborted { applied: 3 },
            ApplyOutcome::Aborted { applied: 4 }
        );
    }
}

/// GH#394: per-change indirect flag preservation, typed rejection of
/// non-canonical bytes, wire-kind enforcement, and SQLite-parity provenance
/// merging.
///
/// Stock SQLite is the oracle for every wire-level claim. The byte fixtures
/// below were captured from the `sqlite3` 3.46.1 shell with this script
/// (`.session indirect` toggles `sqlite3session_indirect()`):
///
/// ```text
/// CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
/// INSERT INTO t VALUES(1,'a',10);
/// INSERT INTO t VALUES(2,'b',20);
/// .session open main s
/// .session attach t
/// .session indirect 1
/// INSERT INTO t VALUES(3,'c',30);
/// .session indirect 0
/// UPDATE t SET score=11 WHERE id=1;
/// .session indirect 1
/// DELETE FROM t WHERE id=2;
/// .session changeset mixed.changeset
/// .session patchset mixed.patchset
/// .session close
/// .session open main s2
/// .session attach t
/// .session indirect 1
/// INSERT INTO t VALUES(9,'z',90);
/// .session changeset single.changeset
/// ```
///
/// `stock_cli_regenerates_identical_fixtures` re-runs that script against
/// the installed shell when one with session support is available, and the
/// `libsqlite3` session C API (through `python3` + `ctypes`) reads
/// `sqlite3changeset_op()`'s `pbIndirect` back out of bytes this crate
/// wrote. Both oracles skip loudly when the host lacks them; every other
/// assertion runs unconditionally against the captured bytes.
#[cfg(test)]
mod indirect_flag_tests {
    use super::*;
    use std::process::Command;

    const STOCK_SCHEMA_SQL: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INTEGER);\n\
         INSERT INTO t VALUES(1,'a',10);\n\
         INSERT INTO t VALUES(2,'b',20);\n";

    const STOCK_MIXED_SESSION_SCRIPT: &str = ".session open main s\n\
         .session attach t\n\
         .session indirect 1\n\
         INSERT INTO t VALUES(3,'c',30);\n\
         .session indirect 0\n\
         UPDATE t SET score=11 WHERE id=1;\n\
         .session indirect 1\n\
         DELETE FROM t WHERE id=2;\n";

    const STOCK_SINGLE_SESSION_SCRIPT: &str = ".session open main s2\n\
         .session attach t\n\
         .session indirect 1\n\
         INSERT INTO t VALUES(9,'z',90);\n";

    /// `mixed.changeset`: direct UPDATE(id=1), indirect DELETE(id=2),
    /// indirect INSERT(id=3), in the order SQLite emitted them.
    const STOCK_MIXED_CHANGESET: &[u8] = &[
        0x54, 0x03, 0x01, 0x00, 0x00, 0x74, 0x00, 0x17, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x09, 0x01, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0x01, 0x62, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x14, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x03, 0x01,
        0x63, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1e,
    ];

    /// `mixed.patchset`: the same three changes in patchset form.
    const STOCK_MIXED_PATCHSET: &[u8] = &[
        0x50, 0x03, 0x01, 0x00, 0x00, 0x74, 0x00, 0x17, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x09, 0x01,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x03, 0x03, 0x01, 0x63, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x1e,
    ];

    /// `single.changeset`: one indirect INSERT(id=9).
    const STOCK_SINGLE_INDIRECT_CHANGESET: &[u8] = &[
        0x54, 0x03, 0x01, 0x00, 0x00, 0x74, 0x00, 0x12, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x09, 0x03, 0x01, 0x7a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5a,
    ];

    /// `(op, pbIndirect)` per row of the mixed fixtures, derived from the
    /// `.session indirect` toggles in the capture script.
    const STOCK_MIXED_OPS: &[(ChangeOp, bool)] = &[
        (ChangeOp::Update, false),
        (ChangeOp::Delete, true),
        (ChangeOp::Insert, true),
    ];

    /// Python `ctypes` shim over the host `libsqlite3` session API: prints
    /// one `<op> <indirect>` line per change of the blob at `argv[1]`.
    const CTYPES_ITERATOR_SCRIPT: &str = r#"
import ctypes, ctypes.util, sys
lib_name = ctypes.util.find_library("sqlite3")
if not lib_name:
    sys.exit(3)
lib = ctypes.CDLL(lib_name)
for symbol in ("sqlite3changeset_start", "sqlite3changeset_next",
               "sqlite3changeset_op", "sqlite3changeset_finalize"):
    if not hasattr(lib, symbol):
        sys.exit(3)
blob = open(sys.argv[1], "rb").read()
buf = ctypes.create_string_buffer(blob, len(blob))
it = ctypes.c_void_p()
lib.sqlite3changeset_start.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_int, ctypes.c_void_p]
lib.sqlite3changeset_next.argtypes = [ctypes.c_void_p]
lib.sqlite3changeset_op.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p),
                                    ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int),
                                    ctypes.POINTER(ctypes.c_int)]
lib.sqlite3changeset_finalize.argtypes = [ctypes.c_void_p]
rc = lib.sqlite3changeset_start(ctypes.byref(it), len(blob), buf)
if rc != 0:
    sys.exit(4)
ops = {18: "insert", 9: "delete", 23: "update"}
while lib.sqlite3changeset_next(it) == 100:
    table = ctypes.c_char_p()
    ncol = ctypes.c_int()
    op = ctypes.c_int()
    indirect = ctypes.c_int()
    if lib.sqlite3changeset_op(it, ctypes.byref(table), ctypes.byref(ncol),
                               ctypes.byref(op), ctypes.byref(indirect)) != 0:
        sys.exit(4)
    print(ops.get(op.value, "unknown"), "1" if indirect.value else "0")
if lib.sqlite3changeset_finalize(it) != 0:
    sys.exit(4)
"#;

    /// Read `(op, pbIndirect)` per change through the host SQLite session
    /// C API. `None` when the host offers no usable oracle (no `python3`, no
    /// `libsqlite3`, or one built without `SQLITE_ENABLE_SESSION`).
    /// Unique scratch directory per oracle invocation. Tests run in
    /// parallel inside one process, so the process id alone is not enough
    /// to keep two oracle inputs apart.
    fn unique_scratch_dir(prefix: &str) -> Option<std::path::PathBuf> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).ok()?;
        Some(dir)
    }

    fn stock_ops(blob: &[u8]) -> Option<Vec<(ChangeOp, bool)>> {
        let dir = unique_scratch_dir("fsqlite-session-oracle")?;
        let script = dir.join("iterate.py");
        let input = dir.join("input.bin");
        std::fs::write(&script, CTYPES_ITERATOR_SCRIPT).ok()?;
        std::fs::write(&input, blob).ok()?;
        let output = Command::new("python3")
            .arg(&script)
            .arg(&input)
            .output()
            .ok()?;
        let _ = std::fs::remove_file(&script);
        let _ = std::fs::remove_file(&input);
        let _ = std::fs::remove_dir(&dir);
        match output.status.code() {
            Some(0) => {}
            Some(3) => return None,
            other => panic!(
                "libsqlite3 session oracle failed (exit {other:?}): {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        }
        let stdout = String::from_utf8(output.stdout).expect("oracle output is utf-8");
        Some(
            stdout
                .lines()
                .map(|line| {
                    let (op, indirect) = line.split_once(' ').expect("oracle line shape");
                    let op = match op {
                        "insert" => ChangeOp::Insert,
                        "delete" => ChangeOp::Delete,
                        "update" => ChangeOp::Update,
                        other => panic!("oracle reported unknown op {other}"),
                    };
                    (op, indirect == "1")
                })
                .collect(),
        )
    }

    /// Assert that stock SQLite reads exactly `expected` flags from `blob`,
    /// or report a loud skip when the host has no session-capable SQLite.
    fn assert_stock_reads(blob: &[u8], expected: &[(ChangeOp, bool)]) {
        match stock_ops(blob) {
            Some(ops) => assert_eq!(ops, expected, "stock SQLite pbIndirect view"),
            None => eprintln!(
                "SKIP libsqlite3 session oracle unavailable on this host; \
                 embedded-fixture assertions still ran"
            ),
        }
    }

    fn our_ops(changeset: &Changeset) -> Vec<(ChangeOp, bool)> {
        changeset
            .tables
            .iter()
            .flat_map(|table| table.rows.iter().map(|row| (row.op, row.indirect)))
            .collect()
    }

    /// Encode every row of `tables` (changeset or patchset form) and sort
    /// the encodings so two containers can be compared independent of row
    /// order.
    fn sorted_rows(tables: &[TableChangeset], patchset: bool) -> Vec<Vec<u8>> {
        let mut rows: Vec<Vec<u8>> = tables
            .iter()
            .flat_map(|table| {
                table.rows.iter().map(move |row| {
                    let mut bytes = Vec::new();
                    if patchset {
                        row.encode_patchset(&mut bytes, &table.info.pk_flags);
                    } else {
                        row.encode_changeset(&mut bytes);
                    }
                    bytes
                })
            })
            .collect();
        rows.sort();
        rows
    }

    /// Run the capture script against the installed `sqlite3` shell and
    /// return `(changeset, patchset)` bytes, or `None` when the shell is
    /// missing or lacks session support.
    fn regenerate_with_cli(session_script: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let dir = unique_scratch_dir("fsqlite-session-cli")?;
        let db = dir.join("fixture.db");
        let changeset = dir.join("out.changeset");
        let patchset = dir.join("out.patchset");
        let script = format!(
            "{STOCK_SCHEMA_SQL}{session_script}.session changeset {}\n.session patchset {}\n",
            changeset.display(),
            patchset.display()
        );
        let mut child = Command::new("sqlite3")
            .arg(&db)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .ok()?;
        {
            use std::io::Write;
            let mut stdin = child.stdin.take()?;
            stdin.write_all(script.as_bytes()).ok()?;
        }
        let output = child.wait_with_output().ok()?;
        let result = if output.status.success() {
            std::fs::read(&changeset)
                .ok()
                .zip(std::fs::read(&patchset).ok())
        } else {
            None
        };
        let _ = std::fs::remove_dir_all(&dir);
        result
    }

    #[test]
    fn stock_cli_regenerates_identical_fixtures() {
        let Some((mixed_changeset, mixed_patchset)) =
            regenerate_with_cli(STOCK_MIXED_SESSION_SCRIPT)
        else {
            eprintln!("SKIP sqlite3 shell with session support unavailable on this host");
            return;
        };
        assert_eq!(mixed_changeset, STOCK_MIXED_CHANGESET);
        assert_eq!(mixed_patchset, STOCK_MIXED_PATCHSET);
        let (single_changeset, _) = regenerate_with_cli(STOCK_SINGLE_SESSION_SCRIPT)
            .expect("shell already proved session support");
        assert_eq!(single_changeset, STOCK_SINGLE_INDIRECT_CHANGESET);
    }

    #[test]
    fn stock_changeset_indirect_flags_decode_and_re_encode_byte_identical() {
        let decoded = Changeset::decode(STOCK_MIXED_CHANGESET).expect("decode stock changeset");
        assert_eq!(decoded.kind, ChangesetKind::Changeset);
        let ours = our_ops(&decoded);
        assert_eq!(ours, STOCK_MIXED_OPS);
        assert_stock_reads(STOCK_MIXED_CHANGESET, &ours);
        assert_eq!(
            decoded.encode(),
            STOCK_MIXED_CHANGESET,
            "decode→encode must preserve every indirect byte"
        );
    }

    #[test]
    fn stock_patchset_indirect_flags_decode_and_re_encode_byte_identical() {
        let decoded =
            Changeset::decode_patchset(STOCK_MIXED_PATCHSET).expect("decode stock patchset");
        assert_eq!(decoded.kind, ChangesetKind::Patchset);
        let ours = our_ops(&decoded);
        assert_eq!(ours, STOCK_MIXED_OPS);
        assert_stock_reads(STOCK_MIXED_PATCHSET, &ours);
        assert_eq!(
            decoded.encode_patchset(),
            STOCK_MIXED_PATCHSET,
            "patchset decode→encode must preserve every indirect byte"
        );
    }

    #[test]
    fn stock_sqlite_reads_indirect_flags_written_by_our_session() {
        let mut session = Session::new();
        session.attach_table("t", 3, vec![true, false, false]);
        session.set_indirect(true);
        session.record_insert(
            "t",
            vec![
                ChangesetValue::Integer(3),
                ChangesetValue::Text("c".to_owned()),
                ChangesetValue::Integer(30),
            ],
        );
        session.set_indirect(false);
        session.record_update(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
                ChangesetValue::Integer(10),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Undefined,
                ChangesetValue::Integer(11),
            ],
        );
        session.set_indirect(true);
        session.record_delete(
            "t",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("b".to_owned()),
                ChangesetValue::Integer(20),
            ],
        );
        let changeset = session.changeset();
        let expected = vec![
            (ChangeOp::Insert, true),
            (ChangeOp::Update, false),
            (ChangeOp::Delete, true),
        ];
        assert_eq!(our_ops(&changeset), expected);

        // The same logical operations recorded with the same indirect
        // toggles must produce exactly the rows stock SQLite produced (row
        // order differs: SQLite emits its hash-table order), in both formats.
        let stock_changeset = Changeset::decode(STOCK_MIXED_CHANGESET).expect("stock decode");
        assert_eq!(
            sorted_rows(&changeset.tables, false),
            sorted_rows(&stock_changeset.tables, false)
        );
        let stock_patchset =
            Changeset::decode_patchset(STOCK_MIXED_PATCHSET).expect("stock patchset decode");
        assert_eq!(
            sorted_rows(&changeset.tables, true),
            sorted_rows(&stock_patchset.tables, true)
        );

        assert_stock_reads(&changeset.encode(), &expected);
        assert_stock_reads(&changeset.encode_patchset(), &expected);
    }

    /// Offset of the first row's indirect byte in a single-table blob for
    /// table `t` with three columns: header byte, column-count varint, three
    /// pk flags, "t\0", then the op byte.
    const FIRST_ROW_INDIRECT_OFFSET: usize = 1 + 1 + 3 + 2 + 1;

    #[test]
    fn noncanonical_indirect_byte_is_rejected_not_normalized() {
        let mut changeset_blob = STOCK_MIXED_CHANGESET.to_vec();
        let mut patchset_blob = STOCK_MIXED_PATCHSET.to_vec();
        assert!(matches!(
            changeset_blob[FIRST_ROW_INDIRECT_OFFSET],
            0x00 | 0x01
        ));
        changeset_blob[FIRST_ROW_INDIRECT_OFFSET] = 0x02;
        assert_eq!(
            Changeset::decode(&changeset_blob),
            Err(ChangesetFormatError::InvalidIndirectFlag {
                offset: FIRST_ROW_INDIRECT_OFFSET,
                byte: 0x02,
            })
        );

        patchset_blob[FIRST_ROW_INDIRECT_OFFSET] = 0xFF;
        assert_eq!(
            Changeset::decode_patchset(&patchset_blob),
            Err(ChangesetFormatError::InvalidIndirectFlag {
                offset: FIRST_ROW_INDIRECT_OFFSET,
                byte: 0xFF,
            })
        );

        // Truncation right before the indirect byte is a distinct typed error.
        let truncated = &changeset_blob[..FIRST_ROW_INDIRECT_OFFSET];
        assert_eq!(
            Changeset::decode(truncated),
            Err(ChangesetFormatError::Truncated {
                offset: FIRST_ROW_INDIRECT_OFFSET,
            })
        );
    }

    #[test]
    fn inversion_preserves_indirect_flags_and_rejects_patchsets() {
        let decoded = Changeset::decode(STOCK_MIXED_CHANGESET).expect("decode");
        let inverted = decoded.invert().expect("changeset inversion");
        let expected: Vec<(ChangeOp, bool)> = our_ops(&decoded)
            .into_iter()
            .map(|(op, indirect)| {
                let op = match op {
                    ChangeOp::Insert => ChangeOp::Delete,
                    ChangeOp::Delete => ChangeOp::Insert,
                    ChangeOp::Update => ChangeOp::Update,
                };
                (op, indirect)
            })
            .collect();
        assert_eq!(our_ops(&inverted), expected);
        assert_stock_reads(&inverted.encode(), &expected);

        let patchset = Changeset::decode_patchset(STOCK_MIXED_PATCHSET).expect("decode patchset");
        assert_eq!(
            patchset.invert(),
            Err(ChangesetFormatError::KindMismatch {
                expected: ChangesetKind::Changeset,
                found: ChangesetKind::Patchset,
            })
        );
    }

    #[test]
    fn concat_rejects_mixed_kinds_and_keeps_appended_flags() {
        let mut changeset = Changeset::decode(STOCK_MIXED_CHANGESET).expect("decode");
        let patchset = Changeset::decode_patchset(STOCK_MIXED_PATCHSET).expect("decode patchset");
        assert_eq!(
            changeset.concat(&patchset),
            Err(ChangesetFormatError::KindMismatch {
                expected: ChangesetKind::Changeset,
                found: ChangesetKind::Patchset,
            })
        );
        let other = Changeset::decode(STOCK_SINGLE_INDIRECT_CHANGESET).expect("decode other");
        changeset.concat(&other).expect("same-kind concat");
        let ops = our_ops(&changeset);
        assert_eq!(ops.last(), Some(&(ChangeOp::Insert, true)));
        assert_stock_reads(&changeset.encode(), &ops);
    }

    fn row(id: i64, name: &str, score: i64) -> Vec<ChangesetValue> {
        vec![
            ChangesetValue::Integer(id),
            ChangesetValue::Text(name.to_owned()),
            ChangesetValue::Integer(score),
        ]
    }

    fn sparse_score(score: i64) -> Vec<ChangesetValue> {
        vec![
            ChangesetValue::Undefined,
            ChangesetValue::Undefined,
            ChangesetValue::Integer(score),
        ]
    }

    fn attached_session() -> Session {
        let mut session = Session::new();
        session.attach_table("t", 3, vec![true, false, false]);
        session
    }

    #[test]
    fn session_is_direct_by_default_and_snapshots_the_mode_per_record() {
        let mut session = attached_session();
        assert!(!session.indirect());
        session.record_insert("t", row(1, "a", 1));
        session.set_indirect(true);
        assert!(session.indirect());
        session.record_insert("t", row(2, "b", 2));
        // Flipping the mode afterwards must not rewrite earlier records.
        session.set_indirect(false);
        assert_eq!(
            our_ops(&session.changeset()),
            vec![(ChangeOp::Insert, false), (ChangeOp::Insert, true)]
        );
    }

    #[test]
    fn indirect_provenance_is_the_logical_and_of_every_contributor() {
        // I(indirect) + U(indirect) stays indirect.
        let mut session = attached_session();
        session.set_indirect(true);
        session.record_insert("t", row(1, "a", 1));
        session.record_update("t", row(1, "a", 1), sparse_score(2));
        assert_eq!(
            our_ops(&session.changeset()),
            vec![(ChangeOp::Insert, true)]
        );

        // I(direct) + U(indirect) becomes direct.
        let mut session = attached_session();
        session.record_insert("t", row(1, "a", 1));
        session.set_indirect(true);
        session.record_update("t", row(1, "a", 1), sparse_score(2));
        assert_eq!(
            our_ops(&session.changeset()),
            vec![(ChangeOp::Insert, false)]
        );

        // D(indirect) + I(direct) on a pre-existing key becomes a direct UPDATE.
        let mut session = attached_session();
        session.set_indirect(true);
        session.record_delete("t", row(1, "a", 1));
        session.set_indirect(false);
        session.record_insert("t", row(1, "a", 5));
        assert_eq!(
            our_ops(&session.changeset()),
            vec![(ChangeOp::Update, false)]
        );

        // Unrelated keys keep independent provenance.
        let mut session = attached_session();
        session.set_indirect(true);
        session.record_insert("t", row(1, "a", 1));
        session.set_indirect(false);
        session.record_insert("t", row(2, "b", 2));
        assert_eq!(
            our_ops(&session.changeset()),
            vec![(ChangeOp::Insert, true), (ChangeOp::Insert, false)]
        );
    }

    #[test]
    fn primary_key_change_marks_both_derived_rows() {
        let mut session = attached_session();
        session.set_indirect(true);
        session.record_update(
            "t",
            row(1, "a", 1),
            vec![
                ChangesetValue::Integer(7),
                ChangesetValue::Undefined,
                ChangesetValue::Undefined,
            ],
        );
        let changeset = session.changeset();
        assert_eq!(
            our_ops(&changeset),
            vec![(ChangeOp::Delete, true), (ChangeOp::Insert, true)]
        );
        assert_stock_reads(&changeset.encode(), &our_ops(&changeset));
    }

    #[test]
    fn net_no_op_retains_direct_provenance_for_later_indirect_changes() {
        let mut session = attached_session();
        session.record_update("t", row(1, "a", 0), sparse_score(1));
        session.record_update("t", row(1, "a", 1), sparse_score(0));
        // The key is back to its original image: nothing to emit yet.
        assert!(session.changeset().tables.is_empty());
        session.set_indirect(true);
        session.record_update("t", row(1, "a", 0), sparse_score(2));
        let changeset = session.changeset();
        assert_eq!(our_ops(&changeset), vec![(ChangeOp::Update, false)]);
        assert_eq!(
            changeset.tables[0].rows[0].new_values,
            sparse_score(2),
            "the accumulated image must be the final value, not a replay"
        );
        assert_stock_reads(&changeset.encode(), &[(ChangeOp::Update, false)]);
    }

    #[test]
    fn apply_conflict_handler_observes_the_indirect_flag() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::from("a"),
                SqliteValue::Integer(1),
            ]],
        );
        let mut session = attached_session();
        session.set_indirect(true);
        session.record_insert("t", row(1, "a", 1));
        let changeset = session.changeset();

        let mut observed = Vec::new();
        let outcome = target.apply(&changeset, |conflict, change| {
            observed.push((conflict, change.op, change.indirect));
            ConflictAction::OmitChange
        });
        assert_eq!(
            outcome,
            ApplyOutcome::Success {
                applied: 0,
                skipped: 1
            }
        );
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].1, ChangeOp::Insert);
        assert!(
            observed[0].2,
            "the conflict handler must see the indirect flag"
        );
    }
}
