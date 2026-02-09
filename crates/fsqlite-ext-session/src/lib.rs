use fsqlite_types::serial_type::{read_varint, varint_len, write_varint};
use fsqlite_types::value::SqliteValue;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Table header marker byte ('T').
const TABLE_HEADER_BYTE: u8 = 0x54;

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
            SqliteValue::Text(s) => Self::Text(s.clone()),
            SqliteValue::Blob(b) => Self::Blob(b.clone()),
        }
    }

    /// Convert to a [`SqliteValue`], mapping `Undefined` to `Null`.
    #[must_use]
    pub fn to_sqlite(&self) -> SqliteValue {
        match self {
            Self::Undefined | Self::Null => SqliteValue::Null,
            Self::Integer(i) => SqliteValue::Integer(*i),
            Self::Real(f) => SqliteValue::Float(*f),
            Self::Text(s) => SqliteValue::Text(s.clone()),
            Self::Blob(b) => SqliteValue::Blob(b.clone()),
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
                Some((Self::Text(s.to_owned()), end - pos))
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
    /// Encode the table header into changeset binary format.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(TABLE_HEADER_BYTE);
        let mut vbuf = [0u8; 9];
        let vlen = write_varint(&mut vbuf, self.column_count as u64);
        out.extend_from_slice(&vbuf[..vlen]);
        for &pk in &self.pk_flags {
            out.push(u8::from(pk));
        }
        out.extend_from_slice(self.name.as_bytes());
        out.push(0x00); // NUL terminator
    }

    /// Decode a table header starting at `pos`.
    ///
    /// Returns `(TableInfo, bytes_consumed)` or `None`.
    pub fn decode(data: &[u8], pos: usize) -> Option<(Self, usize)> {
        if *data.get(pos)? != TABLE_HEADER_BYTE {
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
}

// ---------------------------------------------------------------------------
// Change row
// ---------------------------------------------------------------------------

/// A single row change recorded in a changeset.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesetRow {
    pub op: ChangeOp,
    /// For DELETE and UPDATE: the old column values. Empty for INSERT.
    pub old_values: Vec<ChangesetValue>,
    /// For INSERT and UPDATE: the new column values. Empty for DELETE.
    pub new_values: Vec<ChangesetValue>,
}

impl ChangesetRow {
    /// Encode this row change into changeset binary format.
    pub fn encode_changeset(&self, out: &mut Vec<u8>) {
        out.push(self.op.as_byte());
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
    /// For UPDATE the old values are omitted — only PK columns (from old
    /// values) plus new values are written.
    pub fn encode_patchset(&self, out: &mut Vec<u8>, pk_flags: &[bool]) {
        out.push(self.op.as_byte());
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
                // Patchset UPDATE: emit PK old values only, then new values.
                for (i, v) in self.old_values.iter().enumerate() {
                    if pk_flags.get(i).copied().unwrap_or(false) {
                        v.encode(out);
                    }
                }
                for v in &self.new_values {
                    v.encode(out);
                }
            }
        }
    }

    /// Decode one changeset row starting at `pos`, given the column count.
    pub fn decode_changeset(data: &[u8], pos: usize, col_count: usize) -> Option<(Self, usize)> {
        let op = ChangeOp::from_byte(*data.get(pos)?)?;
        let mut offset = pos + 1;

        let decode_n = |data: &[u8], offset: &mut usize, n: usize| -> Option<Vec<ChangesetValue>> {
            let mut vals = Vec::with_capacity(n);
            for _ in 0..n {
                let (v, consumed) = ChangesetValue::decode(data, *offset)?;
                *offset += consumed;
                vals.push(v);
            }
            Some(vals)
        };

        let (old_values, new_values) = match op {
            ChangeOp::Insert => {
                let new_values = decode_n(data, &mut offset, col_count)?;
                (Vec::new(), new_values)
            }
            ChangeOp::Delete => {
                let old_values = decode_n(data, &mut offset, col_count)?;
                (old_values, Vec::new())
            }
            ChangeOp::Update => {
                let old_values = decode_n(data, &mut offset, col_count)?;
                let new_values = decode_n(data, &mut offset, col_count)?;
                (old_values, new_values)
            }
        };

        Some((
            Self {
                op,
                old_values,
                new_values,
            },
            offset - pos,
        ))
    }

    /// Invert this change: INSERT becomes DELETE, DELETE becomes INSERT,
    /// UPDATE swaps old and new values.
    #[must_use]
    pub fn invert(&self) -> Self {
        match self.op {
            ChangeOp::Insert => Self {
                op: ChangeOp::Delete,
                old_values: self.new_values.clone(),
                new_values: Vec::new(),
            },
            ChangeOp::Delete => Self {
                op: ChangeOp::Insert,
                old_values: Vec::new(),
                new_values: self.old_values.clone(),
            },
            ChangeOp::Update => Self {
                op: ChangeOp::Update,
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
        self.info.encode(out);
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
    pub tables: Vec<TableChangeset>,
}

impl Changeset {
    /// Create an empty changeset.
    #[must_use]
    pub fn new() -> Self {
        Self { tables: Vec::new() }
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
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut tables = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let (info, consumed) = TableInfo::decode(data, pos)?;
            pos += consumed;
            let mut rows = Vec::new();
            // Read rows until we hit another table header or end of data.
            while pos < data.len() && data[pos] != TABLE_HEADER_BYTE {
                let (row, consumed) = ChangesetRow::decode_changeset(data, pos, info.column_count)?;
                pos += consumed;
                rows.push(row);
            }
            tables.push(TableChangeset { info, rows });
        }
        Some(Self { tables })
    }

    /// Invert the changeset: every INSERT becomes DELETE, every DELETE
    /// becomes INSERT, every UPDATE swaps old and new values.
    #[must_use]
    pub fn invert(&self) -> Self {
        Self {
            tables: self
                .tables
                .iter()
                .map(|tc| TableChangeset {
                    info: tc.info.clone(),
                    rows: tc.rows.iter().map(ChangesetRow::invert).collect(),
                })
                .collect(),
        }
    }

    /// Concatenate another changeset onto this one.
    pub fn concat(&mut self, other: &Self) {
        for tc in &other.tables {
            self.tables.push(tc.clone());
        }
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
}

impl Session {
    /// Create a new, empty session.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            changes: Vec::new(),
        }
    }

    /// Attach a table for change tracking.
    ///
    /// `pk_flags` indicates which columns are part of the primary key.
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
        self.changes.push(TrackedChange {
            table_name: table.to_owned(),
            op: ChangeOp::Insert,
            old_values: Vec::new(),
            new_values,
        });
    }

    /// Record a DELETE operation.
    pub fn record_delete(&mut self, table: &str, old_values: Vec<ChangesetValue>) {
        self.changes.push(TrackedChange {
            table_name: table.to_owned(),
            op: ChangeOp::Delete,
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
        self.changes.push(TrackedChange {
            table_name: table.to_owned(),
            op: ChangeOp::Update,
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
        let mut table_map: std::collections::HashMap<String, Vec<ChangesetRow>> =
            std::collections::HashMap::new();

        for change in &self.changes {
            table_map
                .entry(change.table_name.clone())
                .or_default()
                .push(ChangesetRow {
                    op: change.op,
                    old_values: change.old_values.clone(),
                    new_values: change.new_values.clone(),
                });
        }

        let mut tables = Vec::new();
        // Emit tables in the order they were attached (deterministic).
        for tracked in &self.tables {
            if let Some(rows) = table_map.remove(&tracked.name) {
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
        // Any changes to tables not explicitly attached are appended with
        // inferred metadata (all columns non-PK, count from first row).
        for (name, rows) in table_map {
            let col_count = rows.first().map_or(0, |r| {
                if r.new_values.is_empty() {
                    r.old_values.len()
                } else {
                    r.new_values.len()
                }
            });
            tables.push(TableChangeset {
                info: TableInfo {
                    name,
                    column_count: col_count,
                    pk_flags: vec![false; col_count],
                },
                rows,
            });
        }
        Changeset { tables }
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
        let mut applied = 0usize;
        let mut skipped = 0usize;

        for tc in &changeset.tables {
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
                    Err(n) => return ApplyOutcome::Aborted { applied: n },
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
        let old_row: Vec<SqliteValue> = change
            .old_values
            .iter()
            .map(ChangesetValue::to_sqlite)
            .collect();
        if let Some(idx) = Self::find_row_by_pk(rows, pk_flags, &old_row) {
            if rows[idx] != old_row {
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
                ConflictAction::OmitChange | ConflictAction::Replace => Ok(false),
                ConflictAction::Abort => Err(applied),
            }
        }
    }

    fn apply_update<F>(
        rows: &mut [Vec<SqliteValue>],
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
            let row = &mut rows[idx];
            for (i, nv) in change.new_values.iter().enumerate() {
                if *nv != ChangesetValue::Undefined {
                    if let Some(cell) = row.get_mut(i) {
                        *cell = nv.to_sqlite();
                    }
                }
            }
            Ok(true)
        } else {
            match handler(ConflictType::NotFound, change) {
                ConflictAction::OmitChange | ConflictAction::Replace => Ok(false),
                ConflictAction::Abort => Err(applied),
            }
        }
    }

    fn find_row_by_pk(
        rows: &[Vec<SqliteValue>],
        pk_flags: &[bool],
        target: &[SqliteValue],
    ) -> Option<usize> {
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
        ChangesetValue::Text("hello".to_owned()).encode(&mut buf);
        assert_eq!(buf[0], VAL_TEXT);
        // varint(5) = 0x05, then b"hello"
        assert_eq!(buf[1], 5);
        assert_eq!(&buf[2..], b"hello");
        let (val, consumed) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(val, ChangesetValue::Text("hello".to_owned()));
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
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
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
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
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
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("b".to_owned()),
            ],
        );
        let cs = session.changeset();
        let row = &cs.tables[0].rows[0];
        assert_eq!(row.op, ChangeOp::Update);
        assert_eq!(row.old_values[1], ChangesetValue::Text("a".to_owned()));
        assert_eq!(row.new_values[0], ChangesetValue::Undefined);
        assert_eq!(row.new_values[1], ChangesetValue::Text("b".to_owned()));
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
                ChangesetValue::Text("x".to_owned()),
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
                ChangesetValue::Text("hi".to_owned()),
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
    fn test_changeset_roundtrip() {
        let mut session = Session::new();
        session.attach_table("users", 3, vec![true, false, false]);
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("Alice".to_owned()),
                ChangesetValue::Integer(30),
            ],
        );
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("Bob".to_owned()),
                ChangesetValue::Integer(25),
            ],
        );
        session.record_delete(
            "users",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("Alice".to_owned()),
                ChangesetValue::Integer(30),
            ],
        );
        session.record_update(
            "users",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("Bob".to_owned()),
                ChangesetValue::Integer(25),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("Robert".to_owned()),
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
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old".to_owned()),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new".to_owned()),
            ],
        };
        let inv = row.invert();
        assert_eq!(inv.op, ChangeOp::Update);
        assert_eq!(inv.old_values[0], ChangesetValue::Undefined);
        assert_eq!(inv.old_values[1], ChangesetValue::Text("new".to_owned()));
        assert_eq!(inv.new_values[0], ChangesetValue::Integer(1));
        assert_eq!(inv.new_values[1], ChangesetValue::Text("old".to_owned()));
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
                old_values: Vec::new(),
                new_values: vec![ChangesetValue::Integer(1)],
            }],
        });
        let cs2 = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "b".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(2)],
                }],
            }],
        };
        cs1.concat(&cs2);
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
                ChangesetValue::Text("old_name".to_owned()),
                ChangesetValue::Integer(100),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".to_owned()),
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
    fn test_patchset_insert_same_as_changeset() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
        );
        let changeset_bytes = session.changeset().encode();
        let patchset_bytes = session.patchset();
        // For INSERT, patchset and changeset are identical.
        assert_eq!(changeset_bytes, patchset_bytes);
    }

    // -----------------------------------------------------------------------
    // Apply — successful cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_insert() {
        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    old_values: Vec::new(),
                    new_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("hello".to_owned()),
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
                SqliteValue::Text("hello".to_owned())
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
                SqliteValue::Text("hello".to_owned()),
            ]],
        );

        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("hello".to_owned()),
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
                SqliteValue::Text("old".to_owned()),
            ]],
        );

        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("old".to_owned()),
                    ],
                    new_values: vec![
                        ChangesetValue::Undefined,
                        ChangesetValue::Text("new".to_owned()),
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
            vec![SqliteValue::Integer(1), SqliteValue::Text("new".to_owned())]
        );
    }

    // -----------------------------------------------------------------------
    // Apply — conflict scenarios
    // -----------------------------------------------------------------------

    #[test]
    fn test_conflict_not_found() {
        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
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
                SqliteValue::Text("actual".to_owned()),
            ]],
        );

        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("expected".to_owned()),
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
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
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
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
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
                SqliteValue::Text("old".to_owned()),
            ]],
        );

        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Insert,
                    old_values: Vec::new(),
                    new_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("replaced".to_owned()),
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
                SqliteValue::Text("replaced".to_owned())
            ]
        );
    }

    #[test]
    fn test_conflict_abort_stops_apply() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![
                    ChangesetRow {
                        op: ChangeOp::Delete,
                        old_values: vec![ChangesetValue::Integer(1)],
                        new_values: Vec::new(),
                    },
                    ChangesetRow {
                        op: ChangeOp::Insert,
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
                ChangesetValue::Text("Alice".to_owned()),
                ChangesetValue::Integer(30),
            ],
        );
        session.record_insert(
            "users",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("Bob".to_owned()),
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
            SqliteValue::Text("Alice".to_owned())
        );
        assert_eq!(
            target.tables["users"][1][1],
            SqliteValue::Text("Bob".to_owned())
        );
    }

    #[test]
    fn test_changeset_invert_undoes_changes() {
        let mut session = Session::new();
        session.attach_table("t", 2, vec![true, false]);
        session.record_insert(
            "t",
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
        );

        let cs = session.changeset();
        let inv = cs.invert();

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
            ChangesetValue::from_sqlite(&SqliteValue::Text("x".to_owned())),
            ChangesetValue::Text("x".to_owned())
        );
        assert_eq!(
            ChangesetValue::from_sqlite(&SqliteValue::Blob(vec![1, 2])),
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
            ChangesetValue::Text("hi".to_owned()).to_sqlite(),
            SqliteValue::Text("hi".to_owned())
        );
        assert_eq!(
            ChangesetValue::Blob(vec![0xAB]).to_sqlite(),
            SqliteValue::Blob(vec![0xAB])
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
        ChangesetValue::Text(text.to_owned()).encode(&mut buf);
        let (decoded, _) = ChangesetValue::decode(&buf, 0).unwrap();
        assert_eq!(decoded, ChangesetValue::Text(text.to_owned()));
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
            SqliteValue::Text(String::new()),
            SqliteValue::Text("test".to_owned()),
            SqliteValue::Blob(vec![]),
            SqliteValue::Blob(vec![1, 2, 3]),
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
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old".to_owned()),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new".to_owned()),
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
                    old_values: Vec::new(),
                    new_values: vec![ChangesetValue::Integer(1), ChangesetValue::Null],
                },
                ChangeOp::Delete => ChangesetRow {
                    op,
                    old_values: vec![ChangesetValue::Integer(1), ChangesetValue::Null],
                    new_values: Vec::new(),
                },
                ChangeOp::Update => ChangesetRow {
                    op,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("a".to_owned()),
                    ],
                    new_values: vec![
                        ChangesetValue::Undefined,
                        ChangesetValue::Text("b".to_owned()),
                    ],
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
        assert!(ChangesetRow::decode_changeset(&[0xFF, VAL_NULL], 0, 1).is_none());
    }

    // -----------------------------------------------------------------------
    // Patchset UPDATE: PK-only old values
    // -----------------------------------------------------------------------

    #[test]
    fn test_patchset_update_only_pk_old() {
        let pk_flags = vec![true, false, false];
        let row = ChangesetRow {
            op: ChangeOp::Update,
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("old_name".to_owned()),
                ChangesetValue::Integer(100),
            ],
            new_values: vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("new_name".to_owned()),
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
    fn test_patchset_delete_same_as_changeset() {
        let pk_flags = vec![true, false];
        let row = ChangesetRow {
            op: ChangeOp::Delete,
            old_values: vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
            new_values: Vec::new(),
        };
        let mut cs_buf = Vec::new();
        row.encode_changeset(&mut cs_buf);
        let mut ps_buf = Vec::new();
        row.encode_patchset(&mut ps_buf, &pk_flags);
        assert_eq!(cs_buf, ps_buf);
    }

    // -----------------------------------------------------------------------
    // Session: unattached table
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_unattached_table_inferred() {
        let mut session = Session::new();
        // Record changes without attaching the table first
        session.record_insert("auto", vec![ChangesetValue::Integer(1)]);
        let cs = session.changeset();
        assert_eq!(cs.tables.len(), 1);
        assert_eq!(cs.tables[0].info.name, "auto");
        assert_eq!(cs.tables[0].info.column_count, 1);
        assert_eq!(cs.tables[0].info.pk_flags, vec![false]); // all non-PK
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
            vec![
                ChangesetValue::Integer(1),
                ChangesetValue::Text("a".to_owned()),
            ],
        );
        session.record_delete(
            "t",
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("b".to_owned()),
            ],
        );
        session.record_update(
            "t",
            vec![
                ChangesetValue::Integer(3),
                ChangesetValue::Text("c".to_owned()),
            ],
            vec![
                ChangesetValue::Undefined,
                ChangesetValue::Text("d".to_owned()),
            ],
        );

        let cs = session.changeset();
        let double_inv = cs.invert().invert();
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
            vec![
                ChangesetValue::Integer(2),
                ChangesetValue::Text("x".to_owned()),
            ],
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
                SqliteValue::Text("actual".to_owned()),
            ]],
        );

        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("expected".to_owned()),
                    ],
                    new_values: vec![
                        ChangesetValue::Undefined,
                        ChangesetValue::Text("new".to_owned()),
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
            target.tables["t"][0][1],
            SqliteValue::Text("new".to_owned())
        );
    }

    #[test]
    fn test_apply_delete_data_conflict_replace_removes() {
        let mut target = SimpleTarget::default();
        target.tables.insert(
            "t".to_owned(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("actual".to_owned()),
            ]],
        );

        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Delete,
                    old_values: vec![
                        ChangesetValue::Integer(1),
                        ChangesetValue::Text("expected".to_owned()),
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
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 1,
                    pk_flags: vec![true],
                },
                rows: vec![ChangesetRow {
                    op: ChangeOp::Update,
                    old_values: vec![ChangesetValue::Integer(1)],
                    new_values: vec![ChangesetValue::Integer(2)],
                }],
            }],
        };
        let outcome = target.apply(&cs, |_, _| ConflictAction::Abort);
        assert_eq!(outcome, ApplyOutcome::Aborted { applied: 0 });
    }

    #[test]
    fn test_apply_multiple_rows_mixed() {
        let mut target = SimpleTarget::default();
        let cs = Changeset {
            tables: vec![TableChangeset {
                info: TableInfo {
                    name: "t".to_owned(),
                    column_count: 2,
                    pk_flags: vec![true, false],
                },
                rows: vec![
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(1),
                            ChangesetValue::Text("a".to_owned()),
                        ],
                    },
                    ChangesetRow {
                        op: ChangeOp::Insert,
                        old_values: Vec::new(),
                        new_values: vec![
                            ChangesetValue::Integer(2),
                            ChangesetValue::Text("b".to_owned()),
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
                old_values: Vec::new(),
                new_values: vec![ChangesetValue::Integer(1), ChangesetValue::Null],
            }],
        };
        let mut cs_buf = Vec::new();
        tc.encode_changeset(&mut cs_buf);
        let mut ps_buf = Vec::new();
        tc.encode_patchset(&mut ps_buf);
        // For INSERT, patchset = changeset
        assert_eq!(cs_buf, ps_buf);
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
