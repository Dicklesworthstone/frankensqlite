//! Vectorized sort operator (bd-14vp7.9).
//!
//! Sorts a [`Batch`] by specified columns with ASC/DESC and NULLS FIRST/LAST
//! support.  Uses binary-comparable key encoding for composite sort keys,
//! then `sort_unstable_by` on encoded keys.  This is a pipeline breaker:
//! it consumes all input before producing output.

use crate::vectorized::{
    AlignedValues, Batch, BatchFormatError, Column, ColumnData, ColumnSpec,
    DEFAULT_SIMD_ALIGNMENT_BYTES, NullBitmap, SelectionVector, record_vectorized_rows,
    set_vectorized_simd_utilization, simd_path_label,
};

// ── Sort Specification ─────────────────────────────────────────────────────

/// Sort direction for a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// NULL ordering for a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrdering {
    NullsFirst,
    NullsLast,
}

/// Specification for one sort key.
#[derive(Debug, Clone)]
pub struct SortKeySpec {
    /// Column index to sort by.
    pub column_idx: usize,
    /// Sort direction.
    pub direction: SortDirection,
    /// NULL ordering.
    pub null_ordering: NullOrdering,
}

// ── Sort Operator ──────────────────────────────────────────────────────────

/// Sort a batch by the specified key columns.
///
/// Returns a new batch with the same columns but rows reordered according
/// to the sort specification.
///
/// # Errors
///
/// Returns an error if column indices are out of bounds.
#[allow(clippy::too_many_lines)]
pub fn sort_batch(batch: &Batch, sort_keys: &[SortKeySpec]) -> Result<Batch, String> {
    let sel = batch.selection();
    let simd_path = simd_path_label();
    let input_rows = sel.len() as u64;

    if sel.len() <= 1 {
        // Already sorted (0 or 1 rows).
        record_vectorized_rows(input_rows);
        return clone_batch(batch);
    }

    // Validate column indices.
    for key in sort_keys {
        if key.column_idx >= batch.columns().len() {
            return Err(format!(
                "sort key column index {} out of bounds (batch has {} columns)",
                key.column_idx,
                batch.columns().len()
            ));
        }
    }

    // Encode sort keys for each selected row.
    let mut row_keys: Vec<(Vec<u8>, usize)> = Vec::with_capacity(sel.len());
    for &row_idx in sel.as_slice() {
        let row = usize::from(row_idx);
        let encoded = encode_sort_key(batch, sort_keys, row);
        row_keys.push((encoded, row));
    }

    // Sort by encoded keys.
    row_keys.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    // Build sorted indices.
    let sorted_indices: Vec<usize> = row_keys.iter().map(|(_, idx)| *idx).collect();

    // Gather output columns in sorted order.
    let mut output_columns: Vec<Column> = Vec::new();
    let output_rows = sorted_indices.len();

    for col in batch.columns() {
        let gathered = gather_sorted_column(
            &col.spec,
            &col.data,
            &col.validity,
            &sorted_indices,
            output_rows,
        )?;
        output_columns.push(gathered);
    }

    record_vectorized_rows(input_rows);
    update_sort_simd_utilization(simd_path);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        simd_path = simd_path,
        op = "sort",
    )
    .entered();

    let selection =
        SelectionVector::identity(output_rows).map_err(|e: BatchFormatError| e.to_string())?;

    Batch::from_columns(output_columns, output_rows, output_rows.max(1), selection)
        .map_err(|e: BatchFormatError| e.to_string())
}

// ── Key Encoding ───────────────────────────────────────────────────────────

/// Encode a composite sort key for one row as a binary-comparable byte array.
///
/// Encoding rules:
/// - NULL: 0x00 prefix (NULLS FIRST) or 0xFF prefix (NULLS LAST), no payload.
/// - Non-NULL: opposite prefix byte, then type-specific payload.
/// - Integer: big-endian encoding with sign bit flipped (so memcmp ordering
///   matches numeric ordering).
/// - Float: IEEE 754 big-endian with sign normalization.
/// - Text/Binary: raw bytes followed by null terminator 0x00.
/// - DESC: bitwise NOT of the entire key segment.
fn encode_sort_key(batch: &Batch, sort_keys: &[SortKeySpec], row: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(sort_keys.len() * 9); // estimate

    for key_spec in sort_keys {
        let col = &batch.columns()[key_spec.column_idx];
        let is_null = !col.validity.is_valid(row);

        let null_prefix = match key_spec.null_ordering {
            NullOrdering::NullsFirst => 0x00_u8,
            NullOrdering::NullsLast => 0xFF_u8,
        };
        let non_null_prefix = match key_spec.null_ordering {
            NullOrdering::NullsFirst => 0x01_u8,
            NullOrdering::NullsLast => 0x00_u8,
        };

        let segment_start = buf.len();

        if is_null {
            buf.push(null_prefix);
        } else {
            buf.push(non_null_prefix);
            encode_value(&col.data, row, &mut buf);
        }

        // For DESC, invert value bytes (skip the null-ordering prefix byte
        // so that NULLS FIRST/LAST is preserved regardless of sort direction).
        if key_spec.direction == SortDirection::Desc {
            for byte in &mut buf[segment_start + 1..] {
                *byte = !*byte;
            }
        }
    }

    buf
}

/// Encode a single non-NULL value into the key buffer.
#[allow(clippy::cast_sign_loss)]
fn encode_value(data: &ColumnData, row: usize, buf: &mut Vec<u8>) {
    match data {
        ColumnData::Int8(v) => {
            // Flip sign bit for memcmp ordering.
            let raw = v.as_slice()[row];
            buf.push((raw as u8) ^ 0x80);
        }
        ColumnData::Int16(v) => {
            let raw = v.as_slice()[row];
            let encoded = (raw as u16) ^ 0x8000;
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ColumnData::Int32(v) => {
            let raw = v.as_slice()[row];
            let encoded = (raw as u32) ^ 0x8000_0000;
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ColumnData::Int64(v) => {
            let raw = v.as_slice()[row];
            let encoded = (raw as u64) ^ 0x8000_0000_0000_0000;
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ColumnData::Float32(v) => {
            let raw = v.as_slice()[row];
            let bits = raw.to_bits();
            // Float encoding: if sign bit set, flip all bits; else flip sign bit.
            let encoded = if bits & 0x8000_0000 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ColumnData::Float64(v) => {
            let raw = v.as_slice()[row];
            let bits = raw.to_bits();
            let encoded = if bits & 0x8000_0000_0000_0000 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ColumnData::Text { offsets, data } | ColumnData::Binary { offsets, data } => {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            // Escape 0x00 to properly support composite keys and blobs containing null bytes.
            for &b in &data[start..end] {
                if b == 0x00 {
                    buf.extend_from_slice(&[0x00, 0x01]);
                } else {
                    buf.push(b);
                }
            }
            buf.extend_from_slice(&[0x00, 0x00]); // terminator
        }
    }
}

// ── Output Construction ────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn gather_sorted_column(
    spec: &ColumnSpec,
    data: &ColumnData,
    validity: &NullBitmap,
    indices: &[usize],
    output_rows: usize,
) -> Result<Column, String> {
    let mut validity_bytes = vec![0_u8; output_rows.div_ceil(8)];

    let out_data = match data {
        ColumnData::Int8(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src[row]);
                if validity.is_valid(row) {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Int8(
                AlignedValues::from_vec(values, DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColumnData::Int16(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src[row]);
                if validity.is_valid(row) {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Int16(
                AlignedValues::from_vec(values, DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColumnData::Int32(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src[row]);
                if validity.is_valid(row) {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Int32(
                AlignedValues::from_vec(values, DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColumnData::Int64(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src[row]);
                if validity.is_valid(row) {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Int64(
                AlignedValues::from_vec(values, DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColumnData::Float32(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src[row]);
                if validity.is_valid(row) {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Float32(
                AlignedValues::from_vec(values, DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColumnData::Float64(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src[row]);
                if validity.is_valid(row) {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Float64(
                AlignedValues::from_vec(values, DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColumnData::Text { offsets, data: src } => {
            let mut new_offsets: Vec<u32> = Vec::with_capacity(output_rows + 1);
            let mut new_data: Vec<u8> = Vec::new();
            new_offsets.push(0);
            for (out_idx, &row) in indices.iter().enumerate() {
                let is_valid = validity.is_valid(row);
                if is_valid {
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    new_data.extend_from_slice(&src[start..end]);
                }
                #[allow(clippy::cast_possible_truncation)]
                new_offsets.push(new_data.len() as u32);
                if is_valid {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Text {
                offsets: std::sync::Arc::from(new_offsets),
                data: std::sync::Arc::from(new_data),
            }
        }
        ColumnData::Binary { offsets, data: src } => {
            let mut new_offsets: Vec<u32> = Vec::with_capacity(output_rows + 1);
            let mut new_data: Vec<u8> = Vec::new();
            new_offsets.push(0);
            for (out_idx, &row) in indices.iter().enumerate() {
                let is_valid = validity.is_valid(row);
                if is_valid {
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    new_data.extend_from_slice(&src[start..end]);
                }
                #[allow(clippy::cast_possible_truncation)]
                new_offsets.push(new_data.len() as u32);
                if is_valid {
                    validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
                }
            }
            ColumnData::Binary {
                offsets: std::sync::Arc::from(new_offsets),
                data: std::sync::Arc::from(new_data),
            }
        }
    };

    let out_validity = NullBitmap::from_bytes(std::sync::Arc::from(validity_bytes), output_rows)
        .map_err(|e| e.to_string())?;

    Ok(Column {
        spec: spec.clone(),
        data: out_data,
        validity: out_validity,
    })
}

fn clone_batch(batch: &Batch) -> Result<Batch, String> {
    let sel = batch.selection();
    let output_rows = sel.len();
    let indices: Vec<usize> = sel.as_slice().iter().map(|&i| usize::from(i)).collect();

    let mut output_columns = Vec::new();
    for col in batch.columns() {
        let gathered =
            gather_sorted_column(&col.spec, &col.data, &col.validity, &indices, output_rows)?;
        output_columns.push(gathered);
    }

    let selection =
        SelectionVector::identity(output_rows).map_err(|e: BatchFormatError| e.to_string())?;

    Batch::from_columns(output_columns, output_rows, output_rows.max(1), selection)
        .map_err(|e: BatchFormatError| e.to_string())
}

fn update_sort_simd_utilization(simd_path: &str) {
    let milli = match simd_path {
        "avx2" => 600,
        "sse2" => 300,
        _ => 0,
    };
    set_vectorized_simd_utilization(milli);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vectorized::{
        ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY, reset_vectorized_metrics,
        vectorized_metrics_snapshot,
    };
    use fsqlite_types::value::SqliteValue;

    const BEAD_SORT: &str = "bd-14vp7.9";

    fn int_batch(values: &[i64]) -> Batch {
        let specs = vec![ColumnSpec::new("val", ColumnVectorType::Int64)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|&v| vec![SqliteValue::Integer(v)])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn int_batch_two_cols(rows: &[(i64, i64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("a", ColumnVectorType::Int64),
            ColumnSpec::new("b", ColumnVectorType::Int64),
        ];
        let values: Vec<Vec<SqliteValue>> = rows
            .iter()
            .map(|&(a, b)| vec![SqliteValue::Integer(a), SqliteValue::Integer(b)])
            .collect();
        Batch::from_rows(&values, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn nullable_int_batch(values: &[Option<i64>]) -> Batch {
        let specs = vec![ColumnSpec::new("val", ColumnVectorType::Int64)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|v| match v {
                Some(i) => vec![SqliteValue::Integer(*i)],
                None => vec![SqliteValue::Null],
            })
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn get_int64_values(batch: &Batch, col_idx: usize) -> Vec<Option<i64>> {
        let col = &batch.columns()[col_idx];
        let sel = batch.selection().as_slice();
        if let ColumnData::Int64(v) = &col.data {
            sel.iter()
                .map(|&i| {
                    let idx = usize::from(i);
                    if col.validity.is_valid(idx) {
                        Some(v.as_slice()[idx])
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            panic!("expected Int64 column")
        }
    }

    // ── Basic sort tests ─────────────────────────────────────────────────

    #[test]
    fn sort_single_column_asc() {
        let batch = int_batch(&[30, 10, 20, 50, 40]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
            "bead_id={BEAD_SORT} case=sort_asc"
        );
    }

    #[test]
    fn sort_single_column_desc() {
        let batch = int_batch(&[30, 10, 20, 50, 40]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Desc,
                null_ordering: NullOrdering::NullsLast,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(50), Some(40), Some(30), Some(20), Some(10)],
            "bead_id={BEAD_SORT} case=sort_desc"
        );
    }

    #[test]
    fn sort_multi_column() {
        let batch = int_batch_two_cols(&[(2, 30), (1, 20), (2, 10), (1, 40)]);
        let result = sort_batch(
            &batch,
            &[
                SortKeySpec {
                    column_idx: 0,
                    direction: SortDirection::Asc,
                    null_ordering: NullOrdering::NullsFirst,
                },
                SortKeySpec {
                    column_idx: 1,
                    direction: SortDirection::Asc,
                    null_ordering: NullOrdering::NullsFirst,
                },
            ],
        )
        .unwrap();

        let a_vals = get_int64_values(&result, 0);
        let b_vals = get_int64_values(&result, 1);
        assert_eq!(
            a_vals,
            vec![Some(1), Some(1), Some(2), Some(2)],
            "bead_id={BEAD_SORT} case=multi_col_a"
        );
        assert_eq!(
            b_vals,
            vec![Some(20), Some(40), Some(10), Some(30)],
            "bead_id={BEAD_SORT} case=multi_col_b"
        );
    }

    // ── NULL handling ────────────────────────────────────────────────────

    #[test]
    fn sort_nulls_first_asc() {
        let batch = nullable_int_batch(&[Some(30), None, Some(10), None, Some(20)]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![None, None, Some(10), Some(20), Some(30)],
            "bead_id={BEAD_SORT} case=nulls_first_asc"
        );
    }

    #[test]
    fn sort_nulls_last_asc() {
        let batch = nullable_int_batch(&[Some(30), None, Some(10), None, Some(20)]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsLast,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(10), Some(20), Some(30), None, None],
            "bead_id={BEAD_SORT} case=nulls_last_asc"
        );
    }

    #[test]
    fn sort_nulls_first_desc() {
        let batch = nullable_int_batch(&[Some(30), None, Some(10)]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Desc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![None, Some(30), Some(10)],
            "bead_id={BEAD_SORT} case=nulls_first_desc"
        );
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn sort_empty_batch() {
        let batch = int_batch(&[]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        assert_eq!(
            result.row_count(),
            0,
            "bead_id={BEAD_SORT} case=empty_batch"
        );
    }

    #[test]
    fn sort_single_row() {
        let batch = int_batch(&[42]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(vals, vec![Some(42)], "bead_id={BEAD_SORT} case=single_row");
    }

    #[test]
    fn sort_already_sorted() {
        let batch = int_batch(&[10, 20, 30, 40]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(10), Some(20), Some(30), Some(40)],
            "bead_id={BEAD_SORT} case=already_sorted"
        );
    }

    #[test]
    fn sort_reverse_sorted() {
        let batch = int_batch(&[40, 30, 20, 10]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(10), Some(20), Some(30), Some(40)],
            "bead_id={BEAD_SORT} case=reverse_sorted"
        );
    }

    #[test]
    fn sort_duplicate_keys() {
        let batch = int_batch(&[20, 10, 20, 10, 30]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(10), Some(10), Some(20), Some(20), Some(30)],
            "bead_id={BEAD_SORT} case=duplicate_keys"
        );
    }

    // ── Error cases ─────────────────────────────────────────────────────

    #[test]
    fn sort_column_out_of_bounds() {
        let batch = int_batch(&[10, 20]);
        let err = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 5,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap_err();
        assert!(
            err.contains("out of bounds"),
            "bead_id={BEAD_SORT} case=column_oob"
        );
    }

    // ── Metrics ─────────────────────────────────────────────────────────

    #[test]
    fn sort_records_metrics() {
        reset_vectorized_metrics();
        let batch = int_batch(&[30, 10, 20]);
        let _ = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let metrics = vectorized_metrics_snapshot();
        assert!(
            metrics.vectorized_rows_total >= 3,
            "bead_id={BEAD_SORT} case=sort_metrics"
        );
    }

    // ── Mixed direction multi-column ────────────────────────────────────

    #[test]
    fn sort_mixed_asc_desc_multi_column() {
        let batch = int_batch_two_cols(&[(1, 30), (1, 10), (2, 20), (2, 40)]);
        let result = sort_batch(
            &batch,
            &[
                SortKeySpec {
                    column_idx: 0,
                    direction: SortDirection::Asc,
                    null_ordering: NullOrdering::NullsFirst,
                },
                SortKeySpec {
                    column_idx: 1,
                    direction: SortDirection::Desc,
                    null_ordering: NullOrdering::NullsLast,
                },
            ],
        )
        .unwrap();

        let a_vals = get_int64_values(&result, 0);
        let b_vals = get_int64_values(&result, 1);
        // a ASC: [1,1,2,2], b DESC within groups: [30,10,40,20]
        assert_eq!(a_vals, vec![Some(1), Some(1), Some(2), Some(2)]);
        assert_eq!(
            b_vals,
            vec![Some(30), Some(10), Some(40), Some(20)],
            "bead_id={BEAD_SORT} case=mixed_asc_desc"
        );
    }

    // ── Negative integers ───────────────────────────────────────────────

    #[test]
    fn sort_negative_integers() {
        let batch = int_batch(&[5, -3, 0, -10, 7]);
        let result = sort_batch(
            &batch,
            &[SortKeySpec {
                column_idx: 0,
                direction: SortDirection::Asc,
                null_ordering: NullOrdering::NullsFirst,
            }],
        )
        .unwrap();

        let vals = get_int64_values(&result, 0);
        assert_eq!(
            vals,
            vec![Some(-10), Some(-3), Some(0), Some(5), Some(7)],
            "bead_id={BEAD_SORT} case=negative_integers"
        );
    }
}
