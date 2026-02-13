//! Vectorized columnar batch format for VDBE execution.
//!
//! Bead: `bd-14vp7.1`
//!
//! This module defines a deterministic batch container used by upcoming
//! vectorized operators (scan/filter/join/aggregate). The batch model is:
//! - columnar data buffers (one per column),
//! - validity bitmap (1 bit per row),
//! - selection vector (active row indices),
//! - Arrow-compatible buffer contracts for zero-copy interchange.

use std::fmt;
use std::sync::Arc;

use fsqlite_types::value::SqliteValue;

/// Default number of rows in a vectorized batch.
pub const DEFAULT_BATCH_ROW_CAPACITY: usize = 1_024;
/// Default SIMD alignment target for fixed-width value buffers.
pub const DEFAULT_SIMD_ALIGNMENT_BYTES: usize = 32;

/// Physical storage type for a vectorized column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnVectorType {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Binary,
    Text,
}

/// Batch schema entry for a single column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub vector_type: ColumnVectorType,
}

impl ColumnSpec {
    /// Create a new column specification.
    #[must_use]
    pub fn new(name: impl Into<String>, vector_type: ColumnVectorType) -> Self {
        Self {
            name: name.into(),
            vector_type,
        }
    }
}

/// Error type for vectorized batch construction/conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchFormatError {
    message: String,
}

impl BatchFormatError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for BatchFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BatchFormatError {}

/// Fixed-width typed data with explicit alignment metadata.
#[derive(Clone)]
pub struct AlignedValues<T> {
    values: Arc<[T]>,
    alignment: usize,
}

impl<T> fmt::Debug for AlignedValues<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedValues")
            .field("len", &self.values.len())
            .field("alignment", &self.alignment)
            .field("values", &self.values)
            .field("address", &self.address())
            .finish()
    }
}

impl<T> PartialEq for AlignedValues<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.alignment == other.alignment && self.as_slice() == other.as_slice()
    }
}

impl<T> Eq for AlignedValues<T> where T: Eq {}

impl<T> AlignedValues<T> {
    /// Create aligned values from a typed vector.
    pub fn from_vec(values: Vec<T>, alignment: usize) -> Result<Self, BatchFormatError> {
        let required_alignment = alignment.max(std::mem::align_of::<T>());
        if !required_alignment.is_power_of_two() {
            return Err(BatchFormatError::new(format!(
                "alignment must be power-of-two, got {required_alignment}"
            )));
        }

        Ok(Self {
            values: Arc::from(values),
            alignment: required_alignment,
        })
    }

    /// Return typed immutable view.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.values
    }

    /// Number of elements.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true when no elements are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Raw address of the first element.
    #[must_use]
    pub fn address(&self) -> usize {
        if self.values.is_empty() {
            return 0;
        }
        self.values.as_ptr() as usize
    }

    /// Check whether first element pointer is aligned to `alignment` bytes.
    #[must_use]
    pub fn is_aligned_to(&self, alignment: usize) -> bool {
        if self.values.is_empty() {
            return true;
        }
        alignment != 0 && self.address() % alignment == 0
    }

    /// Declared alignment contract for this buffer.
    #[must_use]
    pub const fn alignment(&self) -> usize {
        self.alignment
    }
}

/// Packed null-validity bitmap (1 bit per row, 1 means value present).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullBitmap {
    bytes: Arc<[u8]>,
    len: usize,
}

impl NullBitmap {
    /// Create an all-valid bitmap.
    #[must_use]
    pub fn all_valid(len: usize) -> Self {
        let byte_len = len.div_ceil(8);
        Self {
            bytes: Arc::from(vec![0xFF; byte_len]),
            len,
        }
    }

    /// Create from raw bitmap bytes.
    pub fn from_bytes(bytes: Arc<[u8]>, len: usize) -> Result<Self, BatchFormatError> {
        let expected = len.div_ceil(8);
        if bytes.len() < expected {
            return Err(BatchFormatError::new(format!(
                "validity bitmap too short: expected at least {expected} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self { bytes, len })
    }

    /// Number of rows represented.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true when the bitmap has no rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Packed bytes backing the bitmap.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns true when row is valid (non-NULL).
    #[must_use]
    pub fn is_valid(&self, row_idx: usize) -> bool {
        if row_idx >= self.len {
            return false;
        }
        let byte = self.bytes[row_idx / 8];
        let bit = row_idx % 8;
        ((byte >> bit) & 1) == 1
    }

    /// Move out packed bytes for zero-copy interchange.
    #[must_use]
    pub fn into_bytes(self) -> Arc<[u8]> {
        self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NullBitmapBuilder {
    bytes: Vec<u8>,
    len: usize,
}

impl NullBitmapBuilder {
    fn with_capacity(len: usize) -> Self {
        Self {
            bytes: vec![0_u8; len.div_ceil(8)],
            len: 0,
        }
    }

    fn push(&mut self, is_valid: bool) {
        let row_idx = self.len;
        if is_valid {
            let byte_idx = row_idx / 8;
            let bit = row_idx % 8;
            self.bytes[byte_idx] |= 1_u8 << bit;
        }
        self.len += 1;
    }

    fn finish(self) -> NullBitmap {
        NullBitmap {
            bytes: Arc::from(self.bytes),
            len: self.len,
        }
    }
}

/// Active row mask for predicate pushdown without copying rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Arc<[u16]>,
}

impl SelectionVector {
    /// Identity selection: every row is active in row order.
    pub fn identity(row_count: usize) -> Result<Self, BatchFormatError> {
        let mut values = Vec::with_capacity(row_count);
        for idx in 0..row_count {
            let converted = u16::try_from(idx).map_err(|_| {
                BatchFormatError::new(format!(
                    "row index {idx} does not fit into selection vector entry"
                ))
            })?;
            values.push(converted);
        }
        Ok(Self {
            indices: Arc::from(values),
        })
    }

    /// Create from explicit row indices.
    pub fn from_indices(indices: Vec<u16>) -> Self {
        Self {
            indices: Arc::from(indices),
        }
    }

    /// Active row indices.
    #[must_use]
    pub fn as_slice(&self) -> &[u16] {
        &self.indices
    }

    /// Number of selected rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns true when no rows are active.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Move out storage for zero-copy interchange.
    #[must_use]
    pub fn into_storage(self) -> Arc<[u16]> {
        self.indices
    }

    /// Rebuild from shared storage.
    #[must_use]
    pub fn from_storage(storage: Arc<[u16]>) -> Self {
        Self { indices: storage }
    }

    fn validate_against_row_count(&self, row_count: usize) -> Result<(), BatchFormatError> {
        for &idx in self.as_slice() {
            let row_idx = usize::from(idx);
            if row_idx >= row_count {
                return Err(BatchFormatError::new(format!(
                    "selection index {row_idx} out of bounds for row_count {row_count}"
                )));
            }
        }
        Ok(())
    }
}

/// Column payload in columnar form.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Int8(AlignedValues<i8>),
    Int16(AlignedValues<i16>),
    Int32(AlignedValues<i32>),
    Int64(AlignedValues<i64>),
    Float32(AlignedValues<f32>),
    Float64(AlignedValues<f64>),
    Binary {
        offsets: Arc<[u32]>,
        data: Arc<[u8]>,
    },
    Text {
        offsets: Arc<[u32]>,
        data: Arc<[u8]>,
    },
}

impl ColumnData {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Int8(values) => values.len(),
            Self::Int16(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Binary { offsets, .. } | Self::Text { offsets, .. } => {
                offsets.len().saturating_sub(1)
            }
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    fn value_address(&self) -> usize {
        match self {
            Self::Int8(values) => values.address(),
            Self::Int16(values) => values.address(),
            Self::Int32(values) => values.address(),
            Self::Int64(values) => values.address(),
            Self::Float32(values) => values.address(),
            Self::Float64(values) => values.address(),
            Self::Binary { offsets, .. } | Self::Text { offsets, .. } => offsets.as_ptr() as usize,
        }
    }

    #[must_use]
    fn is_aligned_to(&self, alignment: usize) -> bool {
        match self {
            Self::Int8(values) => values.is_aligned_to(alignment),
            Self::Int16(values) => values.is_aligned_to(alignment),
            Self::Int32(values) => values.is_aligned_to(alignment),
            Self::Int64(values) => values.is_aligned_to(alignment),
            Self::Float32(values) => values.is_aligned_to(alignment),
            Self::Float64(values) => values.is_aligned_to(alignment),
            Self::Binary { offsets, .. } | Self::Text { offsets, .. } => {
                alignment != 0 && (offsets.as_ptr() as usize) % alignment == 0
            }
        }
    }
}

/// A typed column with validity bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub spec: ColumnSpec,
    pub data: ColumnData,
    pub validity: NullBitmap,
}

impl Column {
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Vectorized batch container exchanged between operators.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    row_count: usize,
    capacity: usize,
    columns: Vec<Column>,
    selection: SelectionVector,
}

impl Batch {
    /// Build a columnar batch from row-oriented values.
    ///
    /// # Errors
    ///
    /// Returns an error when row width mismatches schema, when values fail type
    /// coercion/range checks, or when row count exceeds capacity.
    pub fn from_rows(
        rows: &[Vec<SqliteValue>],
        specs: &[ColumnSpec],
        capacity: usize,
    ) -> Result<Self, BatchFormatError> {
        if rows.len() > capacity {
            return Err(BatchFormatError::new(format!(
                "row count {} exceeds capacity {capacity}",
                rows.len()
            )));
        }

        let mut builders = builders_for_specs(specs, rows.len());
        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != specs.len() {
                return Err(BatchFormatError::new(format!(
                    "row {row_idx} width {} does not match schema width {}",
                    row.len(),
                    specs.len()
                )));
            }
            append_row(&mut builders, specs, row, row_idx)?;
        }

        let columns = finalize_builders(builders, DEFAULT_SIMD_ALIGNMENT_BYTES)?;
        let selection = SelectionVector::identity(rows.len())?;
        Ok(Self {
            row_count: rows.len(),
            capacity,
            columns,
            selection,
        })
    }

    /// Replace the active row mask for this batch.
    ///
    /// # Errors
    ///
    /// Returns an error when any selected index is out of bounds.
    pub fn apply_selection(&mut self, selection: SelectionVector) -> Result<(), BatchFormatError> {
        selection.validate_against_row_count(self.row_count)?;
        self.selection = selection;
        Ok(())
    }

    /// Number of rows in this batch.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Configured batch capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Column collection.
    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Selection vector.
    #[must_use]
    pub const fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Export into Arrow-compatible layout without copying data buffers.
    #[must_use]
    pub fn into_arrow_compatible(self) -> ArrowCompatibleBatch {
        let mut columns = Vec::with_capacity(self.columns.len());
        for column in self.columns {
            let validity_len = column.validity.len();
            columns.push(ArrowCompatibleColumn {
                spec: column.spec,
                data: column.data,
                validity_bytes: column.validity.into_bytes(),
                validity_len,
            });
        }

        ArrowCompatibleBatch {
            row_count: self.row_count,
            capacity: self.capacity,
            columns,
            selection: self.selection.into_storage(),
        }
    }

    /// Import from Arrow-compatible layout without copying data buffers.
    ///
    /// # Errors
    ///
    /// Returns an error when lengths or bitmap sizes are inconsistent.
    pub fn from_arrow_compatible(layout: ArrowCompatibleBatch) -> Result<Self, BatchFormatError> {
        let mut columns = Vec::with_capacity(layout.columns.len());
        for column in layout.columns {
            let validity = NullBitmap::from_bytes(column.validity_bytes, column.validity_len)?;
            let data_len = column.data.len();
            if data_len != validity.len() {
                return Err(BatchFormatError::new(format!(
                    "column {} has data length {data_len} but validity length {}",
                    column.spec.name,
                    validity.len()
                )));
            }
            columns.push(Column {
                spec: column.spec,
                data: column.data,
                validity,
            });
        }

        let selection = SelectionVector::from_storage(layout.selection);
        selection.validate_against_row_count(layout.row_count)?;

        Ok(Self {
            row_count: layout.row_count,
            capacity: layout.capacity,
            columns,
            selection,
        })
    }

    /// Verify alignment contracts for each column value buffer.
    #[must_use]
    pub fn verify_alignment(&self, alignment_bytes: usize) -> AlignmentReport {
        let mut columns = Vec::with_capacity(self.columns.len());
        let mut overall_aligned = true;

        for column in &self.columns {
            let aligned = column.data.is_aligned_to(alignment_bytes);
            if !aligned {
                overall_aligned = false;
            }
            columns.push(ColumnAlignment {
                column_name: column.spec.name.clone(),
                vector_type: column.spec.vector_type,
                value_address: column.data.value_address(),
                aligned,
            });
        }

        AlignmentReport {
            requested_alignment: alignment_bytes,
            overall_aligned,
            columns,
        }
    }
}

/// Zero-copy Arrow-compatible batch contract.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrowCompatibleBatch {
    pub row_count: usize,
    pub capacity: usize,
    pub columns: Vec<ArrowCompatibleColumn>,
    pub selection: Arc<[u16]>,
}

/// Zero-copy Arrow-compatible column contract.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrowCompatibleColumn {
    pub spec: ColumnSpec,
    pub data: ColumnData,
    pub validity_bytes: Arc<[u8]>,
    pub validity_len: usize,
}

/// Alignment check outcome for one column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnAlignment {
    pub column_name: String,
    pub vector_type: ColumnVectorType,
    pub value_address: usize,
    pub aligned: bool,
}

/// Batch-wide alignment verification report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlignmentReport {
    pub requested_alignment: usize,
    pub overall_aligned: bool,
    pub columns: Vec<ColumnAlignment>,
}

#[derive(Debug)]
enum ColumnBuilder {
    Int8(ColumnSpec, Vec<i8>, NullBitmapBuilder),
    Int16(ColumnSpec, Vec<i16>, NullBitmapBuilder),
    Int32(ColumnSpec, Vec<i32>, NullBitmapBuilder),
    Int64(ColumnSpec, Vec<i64>, NullBitmapBuilder),
    Float32(ColumnSpec, Vec<f32>, NullBitmapBuilder),
    Float64(ColumnSpec, Vec<f64>, NullBitmapBuilder),
    Binary(ColumnSpec, Vec<u32>, Vec<u8>, NullBitmapBuilder),
    Text(ColumnSpec, Vec<u32>, Vec<u8>, NullBitmapBuilder),
}

fn builders_for_specs(specs: &[ColumnSpec], row_capacity: usize) -> Vec<ColumnBuilder> {
    specs
        .iter()
        .cloned()
        .map(|spec| match spec.vector_type {
            ColumnVectorType::Int8 => ColumnBuilder::Int8(
                spec,
                Vec::with_capacity(row_capacity),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Int16 => ColumnBuilder::Int16(
                spec,
                Vec::with_capacity(row_capacity),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Int32 => ColumnBuilder::Int32(
                spec,
                Vec::with_capacity(row_capacity),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Int64 => ColumnBuilder::Int64(
                spec,
                Vec::with_capacity(row_capacity),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Float32 => ColumnBuilder::Float32(
                spec,
                Vec::with_capacity(row_capacity),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Float64 => ColumnBuilder::Float64(
                spec,
                Vec::with_capacity(row_capacity),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Binary => ColumnBuilder::Binary(
                spec,
                vec![0_u32],
                Vec::new(),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
            ColumnVectorType::Text => ColumnBuilder::Text(
                spec,
                vec![0_u32],
                Vec::new(),
                NullBitmapBuilder::with_capacity(row_capacity),
            ),
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
fn append_row(
    builders: &mut [ColumnBuilder],
    specs: &[ColumnSpec],
    row: &[SqliteValue],
    row_idx: usize,
) -> Result<(), BatchFormatError> {
    #[allow(clippy::cast_possible_truncation)]
    for (col_idx, value) in row.iter().enumerate() {
        let spec = &specs[col_idx];
        match (&mut builders[col_idx], value) {
            (ColumnBuilder::Int8(_, values, validity), SqliteValue::Null) => {
                values.push(0);
                validity.push(false);
            }
            (ColumnBuilder::Int8(_, values, validity), SqliteValue::Integer(i)) => {
                values.push(cast_integer::<i8>(*i, spec, row_idx)?);
                validity.push(true);
            }
            (ColumnBuilder::Int16(_, values, validity), SqliteValue::Null) => {
                values.push(0);
                validity.push(false);
            }
            (ColumnBuilder::Int16(_, values, validity), SqliteValue::Integer(i)) => {
                values.push(cast_integer::<i16>(*i, spec, row_idx)?);
                validity.push(true);
            }
            (ColumnBuilder::Int32(_, values, validity), SqliteValue::Null) => {
                values.push(0);
                validity.push(false);
            }
            (ColumnBuilder::Int32(_, values, validity), SqliteValue::Integer(i)) => {
                values.push(cast_integer::<i32>(*i, spec, row_idx)?);
                validity.push(true);
            }
            (ColumnBuilder::Int64(_, values, validity), SqliteValue::Null) => {
                values.push(0);
                validity.push(false);
            }
            (ColumnBuilder::Int64(_, values, validity), SqliteValue::Integer(i)) => {
                values.push(*i);
                validity.push(true);
            }
            (ColumnBuilder::Float32(_, values, validity), SqliteValue::Null) => {
                values.push(0.0);
                validity.push(false);
            }
            (ColumnBuilder::Float32(_, values, validity), SqliteValue::Integer(i)) => {
                values.push(*i as f32);
                validity.push(true);
            }
            (ColumnBuilder::Float32(_, values, validity), SqliteValue::Float(v)) => {
                values.push(*v as f32);
                validity.push(true);
            }
            (ColumnBuilder::Float64(_, values, validity), SqliteValue::Null) => {
                values.push(0.0);
                validity.push(false);
            }
            (ColumnBuilder::Float64(_, values, validity), SqliteValue::Integer(i)) => {
                values.push(*i as f64);
                validity.push(true);
            }
            (ColumnBuilder::Float64(_, values, validity), SqliteValue::Float(v)) => {
                values.push(*v);
                validity.push(true);
            }
            (ColumnBuilder::Binary(_, offsets, _data, validity), SqliteValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                validity.push(false);
            }
            (ColumnBuilder::Binary(_, offsets, data, validity), SqliteValue::Blob(bytes)) => {
                data.extend(bytes);
                let next_offset = u32::try_from(data.len()).map_err(|_| {
                    BatchFormatError::new(format!(
                        "binary column {} exceeds 4 GiB payload",
                        spec.name
                    ))
                })?;
                offsets.push(next_offset);
                validity.push(true);
            }
            (ColumnBuilder::Text(_, offsets, _data, validity), SqliteValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                validity.push(false);
            }
            (ColumnBuilder::Text(_, offsets, data, validity), SqliteValue::Text(text)) => {
                data.extend_from_slice(text.as_bytes());
                let next_offset = u32::try_from(data.len()).map_err(|_| {
                    BatchFormatError::new(format!(
                        "text column {} exceeds 4 GiB payload",
                        spec.name
                    ))
                })?;
                offsets.push(next_offset);
                validity.push(true);
            }
            _ => {
                return Err(BatchFormatError::new(format!(
                    "type mismatch at row {row_idx}, column {} (expected {:?}, got {})",
                    spec.name,
                    spec.vector_type,
                    describe_sqlite_value(value)
                )));
            }
        }
    }
    Ok(())
}

fn cast_integer<T>(value: i64, spec: &ColumnSpec, row_idx: usize) -> Result<T, BatchFormatError>
where
    T: TryFrom<i64>,
{
    T::try_from(value).map_err(|_| {
        BatchFormatError::new(format!(
            "integer out of range at row {row_idx}, column {} value={value}",
            spec.name
        ))
    })
}

fn describe_sqlite_value(value: &SqliteValue) -> &'static str {
    match value {
        SqliteValue::Null => "NULL",
        SqliteValue::Integer(_) => "INTEGER",
        SqliteValue::Float(_) => "FLOAT",
        SqliteValue::Text(_) => "TEXT",
        SqliteValue::Blob(_) => "BLOB",
    }
}

fn finalize_builders(
    builders: Vec<ColumnBuilder>,
    alignment: usize,
) -> Result<Vec<Column>, BatchFormatError> {
    let mut columns = Vec::with_capacity(builders.len());
    for builder in builders {
        let column = match builder {
            ColumnBuilder::Int8(spec, values, validity) => Column {
                spec,
                data: ColumnData::Int8(AlignedValues::from_vec(values, alignment)?),
                validity: validity.finish(),
            },
            ColumnBuilder::Int16(spec, values, validity) => Column {
                spec,
                data: ColumnData::Int16(AlignedValues::from_vec(values, alignment)?),
                validity: validity.finish(),
            },
            ColumnBuilder::Int32(spec, values, validity) => Column {
                spec,
                data: ColumnData::Int32(AlignedValues::from_vec(values, alignment)?),
                validity: validity.finish(),
            },
            ColumnBuilder::Int64(spec, values, validity) => Column {
                spec,
                data: ColumnData::Int64(AlignedValues::from_vec(values, alignment)?),
                validity: validity.finish(),
            },
            ColumnBuilder::Float32(spec, values, validity) => Column {
                spec,
                data: ColumnData::Float32(AlignedValues::from_vec(values, alignment)?),
                validity: validity.finish(),
            },
            ColumnBuilder::Float64(spec, values, validity) => Column {
                spec,
                data: ColumnData::Float64(AlignedValues::from_vec(values, alignment)?),
                validity: validity.finish(),
            },
            ColumnBuilder::Binary(spec, offsets, data, validity) => Column {
                spec,
                data: ColumnData::Binary {
                    offsets: Arc::from(offsets),
                    data: Arc::from(data),
                },
                validity: validity.finish(),
            },
            ColumnBuilder::Text(spec, offsets, data, validity) => Column {
                spec,
                data: ColumnData::Text {
                    offsets: Arc::from(offsets),
                    data: Arc::from(data),
                },
                validity: validity.finish(),
            },
        };
        columns.push(column);
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEAD_ID: &str = "bd-14vp7.1";

    fn sample_specs() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec::new("id", ColumnVectorType::Int64),
            ColumnSpec::new("score", ColumnVectorType::Float64),
            ColumnSpec::new("payload", ColumnVectorType::Binary),
            ColumnSpec::new("name", ColumnVectorType::Text),
        ]
    }

    fn sample_rows() -> Vec<Vec<SqliteValue>> {
        vec![
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(1.5),
                SqliteValue::Blob(vec![1, 2, 3]),
                SqliteValue::Text("alice".to_owned()),
            ],
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Null,
                SqliteValue::Blob(vec![9, 8]),
                SqliteValue::Text("bob".to_owned()),
            ],
        ]
    }

    #[test]
    fn batch_from_rows_builds_columnar_layout() {
        let specs = sample_specs();
        let rows = sample_rows();
        let batch = Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY)
            .expect("batch should build from rows");

        assert_eq!(batch.row_count(), 2, "bead_id={BEAD_ID} row_count mismatch");
        assert_eq!(
            batch.columns().len(),
            4,
            "bead_id={BEAD_ID} column count mismatch"
        );

        let id_column = &batch.columns()[0];
        let score_column = &batch.columns()[1];
        let payload_column = &batch.columns()[2];

        assert!(
            matches!(&id_column.data, ColumnData::Int64(_)),
            "bead_id={BEAD_ID} expected int64 id column"
        );
        if let ColumnData::Int64(values) = &id_column.data {
            assert_eq!(
                values.as_slice(),
                &[1, 2],
                "bead_id={BEAD_ID} id values mismatch"
            );
        }

        assert!(
            matches!(&score_column.data, ColumnData::Float64(_)),
            "bead_id={BEAD_ID} expected float64 score column"
        );
        if let ColumnData::Float64(values) = &score_column.data {
            let first = values.as_slice()[0];
            let second = values.as_slice()[1];
            assert!(
                (first - 1.5).abs() < f64::EPSILON,
                "bead_id={BEAD_ID} score row0 mismatch value={first}"
            );
            assert!(
                second.abs() < f64::EPSILON,
                "bead_id={BEAD_ID} null sentinel mismatch value={second}"
            );
            assert!(
                !score_column.validity.is_valid(1),
                "bead_id={BEAD_ID} row1 should be NULL"
            );
        }

        assert!(
            matches!(&payload_column.data, ColumnData::Binary { .. }),
            "bead_id={BEAD_ID} expected binary payload column"
        );
        if let ColumnData::Binary { offsets, data } = &payload_column.data {
            assert_eq!(
                offsets.as_ref(),
                &[0, 3, 5],
                "bead_id={BEAD_ID} payload offsets mismatch"
            );
            assert_eq!(
                data.as_ref(),
                &[1, 2, 3, 9, 8],
                "bead_id={BEAD_ID} payload data mismatch"
            );
        }
    }

    #[test]
    fn arrow_roundtrip_is_zero_copy() {
        let specs = sample_specs();
        let rows = sample_rows();
        let batch = Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY)
            .expect("batch should build from rows");

        let exported = batch.clone().into_arrow_compatible();
        let imported = Batch::from_arrow_compatible(exported.clone())
            .expect("arrow-compatible import should succeed");

        assert_eq!(
            imported.row_count(),
            batch.row_count(),
            "bead_id={BEAD_ID} row_count drift"
        );

        let lhs = &batch.columns()[0].data;
        let rhs = &exported.columns[0].data;
        assert!(
            matches!(lhs, ColumnData::Int64(_)) && matches!(rhs, ColumnData::Int64(_)),
            "bead_id={BEAD_ID} expected int64 column"
        );
        if let (ColumnData::Int64(original), ColumnData::Int64(exported_values)) = (lhs, rhs) {
            assert_eq!(original.as_slice(), exported_values.as_slice());
            assert!(
                Arc::ptr_eq(&original.values, &exported_values.values),
                "bead_id={BEAD_ID} expected zero-copy storage share"
            );
        }
    }

    #[test]
    fn alignment_report_captures_column_addresses() {
        let specs = sample_specs();
        let rows = sample_rows();
        let batch = Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY)
            .expect("batch should build from rows");

        let report = batch.verify_alignment(DEFAULT_SIMD_ALIGNMENT_BYTES);
        assert_eq!(
            report.columns.len(),
            4,
            "bead_id={BEAD_ID} alignment report width mismatch"
        );
        assert!(
            report
                .columns
                .iter()
                .all(|column| !column.column_name.is_empty()),
            "bead_id={BEAD_ID} every alignment row should include column name"
        );
    }

    #[test]
    fn selection_vector_identity_matches_row_count() {
        let selection = SelectionVector::identity(4).expect("identity selection should build");
        assert_eq!(
            selection.as_slice(),
            &[0, 1, 2, 3],
            "bead_id={BEAD_ID} selection mismatch"
        );
    }

    #[test]
    fn rejects_row_width_mismatch() {
        let specs = sample_specs();
        let rows = vec![vec![SqliteValue::Integer(1)]];
        let error = Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY)
            .expect_err("row width mismatch should fail");
        assert!(
            error.to_string().contains("row 0 width"),
            "bead_id={BEAD_ID} expected row width message, got {error}"
        );
    }
}
