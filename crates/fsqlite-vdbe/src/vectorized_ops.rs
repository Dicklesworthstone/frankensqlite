//! Vectorized operators: filter, project, hash (bd-1rw.5).
//!
//! Column-at-a-time operators that consume and produce [`Batch`] values.
//! Each operator:
//! - emits a `vectorized_batch` tracing span with `batch_size`, `selectivity`,
//!   and `simd_path` fields,
//! - records rows processed via [`record_vectorized_rows`],
//! - uses auto-vectorization-friendly tight loops over contiguous arrays.

use crate::vectorized::{
    Batch, ColumnData, SelectionVector, record_vectorized_rows, set_vectorized_simd_utilization,
    simd_path_label,
};

// ── Comparison Predicate ────────────────────────────────────────────────────

/// Comparison operation for vectorized filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

// ── Vectorized Filter ───────────────────────────────────────────────────────

/// Apply a comparison predicate against a constant value on a typed column,
/// producing a new [`SelectionVector`] containing only matching row indices.
///
/// The filter respects the existing selection vector: only currently-selected
/// rows are tested.
///
/// # Errors
///
/// Returns an error (as `String`) if `column_idx` is out of bounds or the
/// column type does not match the predicate value type.
pub fn filter_batch_int64(
    batch: &Batch,
    column_idx: usize,
    op: CompareOp,
    value: i64,
) -> Result<SelectionVector, String> {
    let column = batch
        .columns()
        .get(column_idx)
        .ok_or_else(|| format!("column index {column_idx} out of bounds"))?;

    let ColumnData::Int64(aligned) = &column.data else {
        return Err(format!(
            "column {column_idx} is not Int64, cannot apply int64 filter"
        ));
    };

    let data = aligned.as_slice();
    let sel = batch.selection();
    let simd_path = simd_path_label();

    // Tight loop over selected indices — auto-vectorization friendly.
    let mut result = Vec::with_capacity(sel.len());
    for &idx in sel.as_slice() {
        let row = usize::from(idx);
        if row < data.len() && column.validity.is_valid(row) && compare_i64(data[row], op, value) {
            result.push(idx);
        }
    }

    let input_rows = sel.len() as u64;
    let output_rows = result.len();
    let selectivity_milli = (output_rows as u64 * 1000)
        .checked_div(input_rows)
        .unwrap_or(0);

    record_vectorized_rows(input_rows);
    update_filter_simd_utilization(simd_path);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        selectivity = selectivity_milli as f64 / 1000.0,
        simd_path = simd_path,
        op = "filter_int64",
    )
    .entered();
    tracing::debug!(
        batch_size = input_rows,
        output_rows,
        selectivity = selectivity_milli as f64 / 1000.0,
        simd_path,
        "filter_int64 complete"
    );

    Ok(SelectionVector::from_indices(result))
}

/// Apply a float64 comparison predicate against a constant value.
pub fn filter_batch_float64(
    batch: &Batch,
    column_idx: usize,
    op: CompareOp,
    value: f64,
) -> Result<SelectionVector, String> {
    let column = batch
        .columns()
        .get(column_idx)
        .ok_or_else(|| format!("column index {column_idx} out of bounds"))?;

    let ColumnData::Float64(aligned) = &column.data else {
        return Err(format!(
            "column {column_idx} is not Float64, cannot apply float64 filter"
        ));
    };

    let data = aligned.as_slice();
    let sel = batch.selection();
    let simd_path = simd_path_label();

    let mut result = Vec::with_capacity(sel.len());
    for &idx in sel.as_slice() {
        let row = usize::from(idx);
        if row < data.len() && column.validity.is_valid(row) && compare_f64(data[row], op, value) {
            result.push(idx);
        }
    }

    let input_rows = sel.len() as u64;
    let output_rows = result.len();
    let selectivity_milli = (output_rows as u64 * 1000)
        .checked_div(input_rows)
        .unwrap_or(0);

    record_vectorized_rows(input_rows);
    update_filter_simd_utilization(simd_path);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        selectivity = selectivity_milli as f64 / 1000.0,
        simd_path = simd_path,
        op = "filter_float64",
    )
    .entered();

    Ok(SelectionVector::from_indices(result))
}

// ── Vectorized Project ──────────────────────────────────────────────────────

/// Extract a column subset from a batch, producing a new batch with only the
/// specified columns.  The selection vector and row count are preserved.
///
/// # Errors
///
/// Returns an error if any column index is out of bounds.
pub fn project_batch(batch: &Batch, column_indices: &[usize]) -> Result<Batch, String> {
    use crate::vectorized::BatchFormatError;

    let simd_path = simd_path_label();
    let input_rows = batch.selection().len() as u64;

    let mut columns = Vec::with_capacity(column_indices.len());
    for &idx in column_indices {
        let column = batch
            .columns()
            .get(idx)
            .ok_or_else(|| format!("project: column index {idx} out of bounds"))?;
        columns.push(column.clone());
    }

    record_vectorized_rows(input_rows);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        selectivity = 1.0_f64,
        simd_path = simd_path,
        op = "project",
    )
    .entered();

    // Build batch directly from components.
    let projected = Batch::from_columns(
        columns,
        batch.row_count(),
        batch.capacity(),
        batch.selection().clone(),
    )
    .map_err(|e: BatchFormatError| e.to_string())?;

    Ok(projected)
}

// ── Vectorized Hash ─────────────────────────────────────────────────────────

/// Compute per-row hash values for the specified columns of a batch.
///
/// Uses xxHash-style mixing for fast, well-distributed hash values suitable
/// for hash-join and hash-aggregate partitioning.  Only selected rows are
/// hashed; unselected rows get 0.
///
/// Returns a Vec of hash values, one per selected row, in selection order.
pub fn hash_batch_columns(batch: &Batch, column_indices: &[usize]) -> Result<Vec<u64>, String> {
    let sel = batch.selection();
    let simd_path = simd_path_label();
    let input_rows = sel.len() as u64;

    let mut hashes = Vec::with_capacity(sel.len());
    for &idx in sel.as_slice() {
        let row = usize::from(idx);
        let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis
        for &col_idx in column_indices {
            let column = batch
                .columns()
                .get(col_idx)
                .ok_or_else(|| format!("hash: column index {col_idx} out of bounds"))?;
            h = hash_combine(h, hash_column_value(&column.data, row));
        }
        hashes.push(h);
    }

    record_vectorized_rows(input_rows);

    // Update SIMD utilization: on x86_64 with AVX2 we get auto-vectorized mixing.
    let simd_milli = if simd_path == "avx2" {
        850 // ~85% SIMD utilization estimate with tight int loops
    } else if simd_path == "sse2" {
        500
    } else {
        0
    };
    set_vectorized_simd_utilization(simd_milli);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        selectivity = 1.0_f64,
        simd_path = simd_path,
        op = "hash",
    )
    .entered();

    Ok(hashes)
}

// ── Selection Vector Composition ────────────────────────────────────────────

/// Compute the intersection (AND) of two selection vectors.
///
/// Produces a new selection vector containing only row indices present in
/// *both* inputs.  Output is sorted ascending.
#[must_use]
pub fn and_selection(lhs: &SelectionVector, rhs: &SelectionVector) -> SelectionVector {
    // Both inputs are sorted, so use a merge-intersect.
    let a = lhs.as_slice();
    let b = rhs.as_slice();
    let mut result = Vec::with_capacity(a.len().min(b.len()));
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
    SelectionVector::from_indices(result)
}

/// Compute the union (OR) of two selection vectors.
///
/// Produces a new selection vector containing all row indices present in
/// *either* input.  Output is sorted ascending with duplicates removed.
#[must_use]
pub fn or_selection(lhs: &SelectionVector, rhs: &SelectionVector) -> SelectionVector {
    let a = lhs.as_slice();
    let b = rhs.as_slice();
    let mut result = Vec::with_capacity(a.len() + b.len());
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
    SelectionVector::from_indices(result)
}

// ── Typed Filter Operators ─────────────────────────────────────────────────

/// Apply an int32 comparison predicate against a constant value.
pub fn filter_batch_int32(
    batch: &Batch,
    column_idx: usize,
    op: CompareOp,
    value: i32,
) -> Result<SelectionVector, String> {
    let column = batch
        .columns()
        .get(column_idx)
        .ok_or_else(|| format!("column index {column_idx} out of bounds"))?;

    let ColumnData::Int32(aligned) = &column.data else {
        return Err(format!(
            "column {column_idx} is not Int32, cannot apply int32 filter"
        ));
    };

    let data = aligned.as_slice();
    let sel = batch.selection();
    let simd_path = simd_path_label();

    let mut result = Vec::with_capacity(sel.len());
    for &idx in sel.as_slice() {
        let row = usize::from(idx);
        if row < data.len() && column.validity.is_valid(row) && compare_i32(data[row], op, value) {
            result.push(idx);
        }
    }

    let input_rows = sel.len() as u64;
    let output_rows = result.len();
    let selectivity_milli = (output_rows as u64 * 1000)
        .checked_div(input_rows)
        .unwrap_or(0);

    record_vectorized_rows(input_rows);
    update_filter_simd_utilization(simd_path);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        selectivity = selectivity_milli as f64 / 1000.0,
        simd_path = simd_path,
        op = "filter_int32",
    )
    .entered();

    Ok(SelectionVector::from_indices(result))
}

/// Apply a text (UTF-8 string) comparison predicate against a constant value.
///
/// Comparison uses lexicographic byte ordering (consistent with SQLite's
/// BINARY collation).
pub fn filter_batch_text(
    batch: &Batch,
    column_idx: usize,
    op: CompareOp,
    value: &str,
) -> Result<SelectionVector, String> {
    let column = batch
        .columns()
        .get(column_idx)
        .ok_or_else(|| format!("column index {column_idx} out of bounds"))?;

    let ColumnData::Text { offsets, data } = &column.data else {
        return Err(format!(
            "column {column_idx} is not Text, cannot apply text filter"
        ));
    };

    let sel = batch.selection();
    let simd_path = simd_path_label();
    let value_bytes = value.as_bytes();

    let mut result = Vec::with_capacity(sel.len());
    for &idx in sel.as_slice() {
        let row = usize::from(idx);
        if !column.validity.is_valid(row) {
            continue;
        }
        let start = offsets.get(row).copied().unwrap_or(0) as usize;
        let end = offsets.get(row + 1).copied().unwrap_or(0) as usize;
        let row_bytes = data.get(start..end).unwrap_or(&[]);
        if compare_bytes(row_bytes, op, value_bytes) {
            result.push(idx);
        }
    }

    let input_rows = sel.len() as u64;
    record_vectorized_rows(input_rows);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        simd_path = simd_path,
        op = "filter_text",
    )
    .entered();

    Ok(SelectionVector::from_indices(result))
}

/// Apply a binary (blob) comparison predicate against a constant value.
///
/// Comparison uses raw byte ordering.  Only Eq and Ne are well-defined for
/// binary data; the remaining operators use lexicographic byte order.
pub fn filter_batch_binary(
    batch: &Batch,
    column_idx: usize,
    op: CompareOp,
    value: &[u8],
) -> Result<SelectionVector, String> {
    let column = batch
        .columns()
        .get(column_idx)
        .ok_or_else(|| format!("column index {column_idx} out of bounds"))?;

    let ColumnData::Binary { offsets, data } = &column.data else {
        return Err(format!(
            "column {column_idx} is not Binary, cannot apply binary filter"
        ));
    };

    let sel = batch.selection();
    let simd_path = simd_path_label();

    let mut result = Vec::with_capacity(sel.len());
    for &idx in sel.as_slice() {
        let row = usize::from(idx);
        if !column.validity.is_valid(row) {
            continue;
        }
        let start = offsets.get(row).copied().unwrap_or(0) as usize;
        let end = offsets.get(row + 1).copied().unwrap_or(0) as usize;
        let row_bytes = data.get(start..end).unwrap_or(&[]);
        if compare_bytes(row_bytes, op, value) {
            result.push(idx);
        }
    }

    let input_rows = sel.len() as u64;
    record_vectorized_rows(input_rows);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        simd_path = simd_path,
        op = "filter_binary",
    )
    .entered();

    Ok(SelectionVector::from_indices(result))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Update SIMD utilization gauge based on the active path.
fn update_filter_simd_utilization(simd_path: &str) {
    let milli = match simd_path {
        "avx2" => 750,
        "sse2" => 400,
        _ => 0,
    };
    set_vectorized_simd_utilization(milli);
}

#[inline]
fn compare_i32(lhs: i32, op: CompareOp, rhs: i32) -> bool {
    match op {
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Lt => lhs < rhs,
        CompareOp::Le => lhs <= rhs,
        CompareOp::Gt => lhs > rhs,
        CompareOp::Ge => lhs >= rhs,
    }
}

#[inline]
fn compare_i64(lhs: i64, op: CompareOp, rhs: i64) -> bool {
    match op {
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Lt => lhs < rhs,
        CompareOp::Le => lhs <= rhs,
        CompareOp::Gt => lhs > rhs,
        CompareOp::Ge => lhs >= rhs,
    }
}

#[inline]
fn compare_f64(lhs: f64, op: CompareOp, rhs: f64) -> bool {
    match op {
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Lt => lhs < rhs,
        CompareOp::Le => lhs <= rhs,
        CompareOp::Gt => lhs > rhs,
        CompareOp::Ge => lhs >= rhs,
    }
}

#[inline]
fn compare_bytes(lhs: &[u8], op: CompareOp, rhs: &[u8]) -> bool {
    match op {
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Lt => lhs < rhs,
        CompareOp::Le => lhs <= rhs,
        CompareOp::Gt => lhs > rhs,
        CompareOp::Ge => lhs >= rhs,
    }
}

/// Hash a single column value at a given row index.
#[inline]
#[allow(clippy::cast_sign_loss)]
fn hash_column_value(data: &ColumnData, row: usize) -> u64 {
    match data {
        ColumnData::Int8(v) => v.as_slice().get(row).map_or(0, |&x| x as u64),
        ColumnData::Int16(v) => v.as_slice().get(row).map_or(0, |&x| x as u64),
        ColumnData::Int32(v) => v.as_slice().get(row).map_or(0, |&x| x as u64),
        ColumnData::Int64(v) => v.as_slice().get(row).map_or(0, |&x| x as u64),
        ColumnData::Float32(v) => v.as_slice().get(row).map_or(0, |&x| {
            let val = if x == 0.0 { 0.0 } else { x };
            u64::from(val.to_bits())
        }),
        ColumnData::Float64(v) => v.as_slice().get(row).map_or(0, |&x| {
            let val = if x == 0.0 { 0.0 } else { x };
            val.to_bits()
        }),
        ColumnData::Binary { offsets, data } | ColumnData::Text { offsets, data } => {
            let start = offsets.get(row).copied().unwrap_or(0) as usize;
            let end = offsets.get(row + 1).copied().unwrap_or(0) as usize;
            fnv1a_bytes(data.get(start..end).unwrap_or(&[]))
        }
    }
}

/// FNV-1a hash of a byte slice.
#[inline]
fn fnv1a_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Combine two hash values using a multiply-xorshift mixer.
#[inline]
fn hash_combine(h: u64, val: u64) -> u64 {
    let mixed = h ^ val.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    mixed.wrapping_mul(0x517c_c1b7_2722_0a95)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vectorized::{
        Batch, ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY, reset_vectorized_metrics,
        vectorized_metrics_snapshot,
    };
    use fsqlite_types::value::SqliteValue;

    fn int64_batch(values: &[i64]) -> Batch {
        let specs = vec![ColumnSpec::new("val", ColumnVectorType::Int64)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|&v| vec![SqliteValue::Integer(v)])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    fn float64_batch(values: &[f64]) -> Batch {
        let specs = vec![ColumnSpec::new("val", ColumnVectorType::Float64)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|&v| vec![SqliteValue::Float(v)])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    fn multi_column_batch() -> Batch {
        let specs = vec![
            ColumnSpec::new("id", ColumnVectorType::Int64),
            ColumnSpec::new("score", ColumnVectorType::Float64),
        ];
        let rows = vec![
            vec![SqliteValue::Integer(1), SqliteValue::Float(10.5)],
            vec![SqliteValue::Integer(2), SqliteValue::Float(20.0)],
            vec![SqliteValue::Integer(3), SqliteValue::Float(15.5)],
            vec![SqliteValue::Integer(4), SqliteValue::Float(5.0)],
        ];
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    // ── Filter tests ────────────────────────────────────────────────────

    #[test]
    fn filter_int64_eq() {
        let batch = int64_batch(&[10, 20, 30, 20, 40]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Eq, 20).unwrap();
        assert_eq!(sel.as_slice(), &[1, 3]);
    }

    #[test]
    fn filter_int64_lt() {
        let batch = int64_batch(&[10, 20, 30, 5, 40]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Lt, 20).unwrap();
        assert_eq!(sel.as_slice(), &[0, 3]);
    }

    #[test]
    fn filter_int64_gt() {
        let batch = int64_batch(&[10, 20, 30, 5, 40]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Gt, 20).unwrap();
        assert_eq!(sel.as_slice(), &[2, 4]);
    }

    #[test]
    fn filter_int64_le() {
        let batch = int64_batch(&[10, 20, 30, 5, 40]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Le, 20).unwrap();
        assert_eq!(sel.as_slice(), &[0, 1, 3]);
    }

    #[test]
    fn filter_int64_ge() {
        let batch = int64_batch(&[10, 20, 30, 5, 40]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Ge, 20).unwrap();
        assert_eq!(sel.as_slice(), &[1, 2, 4]);
    }

    #[test]
    fn filter_int64_ne() {
        let batch = int64_batch(&[10, 20, 30]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Ne, 20).unwrap();
        assert_eq!(sel.as_slice(), &[0, 2]);
    }

    #[test]
    fn filter_float64_lt() {
        let batch = float64_batch(&[1.0, 2.5, 3.0, 0.5]);
        let sel = filter_batch_float64(&batch, 0, CompareOp::Lt, 2.0).unwrap();
        assert_eq!(sel.as_slice(), &[0, 3]);
    }

    #[test]
    fn filter_respects_existing_selection() {
        let mut batch = int64_batch(&[10, 20, 30, 40, 50]);
        // Pre-filter: only rows 1, 2, 3 are active.
        batch
            .apply_selection(SelectionVector::from_indices(vec![1, 2, 3]))
            .unwrap();
        let sel = filter_batch_int64(&batch, 0, CompareOp::Gt, 25).unwrap();
        // Only rows 2 (30) and 3 (40) match.
        assert_eq!(sel.as_slice(), &[2, 3]);
    }

    #[test]
    fn filter_empty_batch() {
        let batch = int64_batch(&[]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Eq, 0).unwrap();
        assert!(sel.is_empty());
    }

    #[test]
    fn filter_column_type_mismatch() {
        let batch = float64_batch(&[1.0, 2.0]);
        let err = filter_batch_int64(&batch, 0, CompareOp::Eq, 1).unwrap_err();
        assert!(err.contains("not Int64"));
    }

    #[test]
    fn filter_column_out_of_bounds() {
        let batch = int64_batch(&[1, 2]);
        let err = filter_batch_int64(&batch, 5, CompareOp::Eq, 1).unwrap_err();
        assert!(err.contains("out of bounds"));
    }

    // ── Metrics tests ───────────────────────────────────────────────────

    #[test]
    fn filter_records_metrics() {
        reset_vectorized_metrics();
        let batch = int64_batch(&[10, 20, 30]);
        let _ = filter_batch_int64(&batch, 0, CompareOp::Eq, 20).unwrap();
        let metrics = vectorized_metrics_snapshot();
        assert!(
            metrics.vectorized_rows_total >= 3,
            "should have recorded at least 3 rows"
        );
    }

    // ── Project tests ───────────────────────────────────────────────────

    #[test]
    fn project_selects_column_subset() {
        let batch = multi_column_batch();
        let projected = project_batch(&batch, &[1]).unwrap();
        assert_eq!(projected.columns().len(), 1);
        assert_eq!(projected.columns()[0].spec.name, "score");
        assert_eq!(projected.row_count(), 4);
    }

    #[test]
    fn project_reorders_columns() {
        let batch = multi_column_batch();
        let projected = project_batch(&batch, &[1, 0]).unwrap();
        assert_eq!(projected.columns().len(), 2);
        assert_eq!(projected.columns()[0].spec.name, "score");
        assert_eq!(projected.columns()[1].spec.name, "id");
    }

    #[test]
    fn project_out_of_bounds() {
        let batch = multi_column_batch();
        let err = project_batch(&batch, &[5]).unwrap_err();
        assert!(err.contains("out of bounds"));
    }

    // ── Hash tests ──────────────────────────────────────────────────────

    #[test]
    fn hash_produces_deterministic_values() {
        let batch = int64_batch(&[10, 20, 30]);
        let hashes1 = hash_batch_columns(&batch, &[0]).unwrap();
        let hashes2 = hash_batch_columns(&batch, &[0]).unwrap();
        assert_eq!(hashes1, hashes2, "hashes should be deterministic");
    }

    #[test]
    fn hash_different_values_differ() {
        let batch = int64_batch(&[10, 20]);
        let hashes = hash_batch_columns(&batch, &[0]).unwrap();
        assert_ne!(
            hashes[0], hashes[1],
            "different values should hash differently"
        );
    }

    #[test]
    fn hash_respects_selection_vector() {
        let mut batch = int64_batch(&[10, 20, 30, 40]);
        batch
            .apply_selection(SelectionVector::from_indices(vec![0, 2]))
            .unwrap();
        let hashes = hash_batch_columns(&batch, &[0]).unwrap();
        assert_eq!(hashes.len(), 2, "should only hash selected rows");
    }

    #[test]
    fn hash_multi_column() {
        let batch = multi_column_batch();
        let hashes_single = hash_batch_columns(&batch, &[0]).unwrap();
        let hashes_multi = hash_batch_columns(&batch, &[0, 1]).unwrap();
        // Multi-column hash should differ from single-column.
        assert_ne!(hashes_single, hashes_multi);
    }

    // ── SIMD detection test ─────────────────────────────────────────────

    #[test]
    fn simd_path_detection() {
        let label = simd_path_label();
        // On any modern x86_64, at least SSE2 should be available.
        #[cfg(target_arch = "x86_64")]
        assert!(
            label == "avx2" || label == "sse2",
            "expected avx2 or sse2 on x86_64, got {label}"
        );
        #[cfg(not(target_arch = "x86_64"))]
        assert_eq!(label, "scalar");
    }

    // ── Selection Vector Composition tests (bd-14vp7.3) ─────────────────

    const BEAD_FILTER: &str = "bd-14vp7.3";

    #[test]
    fn and_selection_intersection() {
        let a = SelectionVector::from_indices(vec![0, 1, 2, 3]);
        let b = SelectionVector::from_indices(vec![1, 3, 4]);
        let result = and_selection(&a, &b);
        assert_eq!(
            result.as_slice(),
            &[1, 3],
            "bead_id={BEAD_FILTER} case=and_intersection"
        );
    }

    #[test]
    fn and_selection_disjoint() {
        let a = SelectionVector::from_indices(vec![0, 2]);
        let b = SelectionVector::from_indices(vec![1, 3]);
        let result = and_selection(&a, &b);
        assert!(result.is_empty(), "bead_id={BEAD_FILTER} case=and_disjoint");
    }

    #[test]
    fn and_selection_empty_input() {
        let a = SelectionVector::from_indices(vec![0, 1, 2]);
        let b = SelectionVector::from_indices(vec![]);
        let result = and_selection(&a, &b);
        assert!(
            result.is_empty(),
            "bead_id={BEAD_FILTER} case=and_empty_input"
        );
    }

    #[test]
    fn or_selection_union() {
        let a = SelectionVector::from_indices(vec![0, 2]);
        let b = SelectionVector::from_indices(vec![1, 2, 3]);
        let result = or_selection(&a, &b);
        assert_eq!(
            result.as_slice(),
            &[0, 1, 2, 3],
            "bead_id={BEAD_FILTER} case=or_union"
        );
    }

    #[test]
    fn or_selection_identical() {
        let a = SelectionVector::from_indices(vec![1, 3]);
        let b = SelectionVector::from_indices(vec![1, 3]);
        let result = or_selection(&a, &b);
        assert_eq!(
            result.as_slice(),
            &[1, 3],
            "bead_id={BEAD_FILTER} case=or_identical_no_dupes"
        );
    }

    #[test]
    fn or_selection_empty() {
        let a = SelectionVector::from_indices(vec![]);
        let b = SelectionVector::from_indices(vec![2, 5]);
        let result = or_selection(&a, &b);
        assert_eq!(
            result.as_slice(),
            &[2, 5],
            "bead_id={BEAD_FILTER} case=or_empty_lhs"
        );
    }

    #[test]
    fn and_or_composition_multi_predicate() {
        // Simulate: (val > 10) AND (val < 40) using composition.
        let batch = int64_batch(&[5, 10, 20, 30, 40, 50]);
        let gt10 = filter_batch_int64(&batch, 0, CompareOp::Gt, 10).unwrap();
        let lt40 = filter_batch_int64(&batch, 0, CompareOp::Lt, 40).unwrap();
        let result = and_selection(&gt10, &lt40);
        // Rows 2 (20), 3 (30) match both predicates.
        assert_eq!(
            result.as_slice(),
            &[2, 3],
            "bead_id={BEAD_FILTER} case=and_composition"
        );
    }

    #[test]
    fn or_composition_multi_predicate() {
        // Simulate: (val < 10) OR (val > 40) using composition.
        let batch = int64_batch(&[5, 10, 20, 30, 40, 50]);
        let lt10 = filter_batch_int64(&batch, 0, CompareOp::Lt, 10).unwrap();
        let gt40 = filter_batch_int64(&batch, 0, CompareOp::Gt, 40).unwrap();
        let result = or_selection(&lt10, &gt40);
        // Row 0 (5) and row 5 (50).
        assert_eq!(
            result.as_slice(),
            &[0, 5],
            "bead_id={BEAD_FILTER} case=or_composition"
        );
    }

    // ── Typed filter tests (bd-14vp7.3) ─────────────────────────────────

    fn int32_batch(values: &[i32]) -> Batch {
        let specs = vec![ColumnSpec::new("val", ColumnVectorType::Int32)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|&v| vec![SqliteValue::Integer(i64::from(v))])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    fn text_batch(values: &[&str]) -> Batch {
        let specs = vec![ColumnSpec::new("name", ColumnVectorType::Text)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|&v| vec![SqliteValue::Text(v.to_owned())])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    fn binary_batch(values: &[&[u8]]) -> Batch {
        let specs = vec![ColumnSpec::new("data", ColumnVectorType::Binary)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|v| vec![SqliteValue::Blob(v.to_vec())])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    #[test]
    fn filter_int32_eq() {
        let batch = int32_batch(&[10, 20, 30, 20]);
        let sel = filter_batch_int32(&batch, 0, CompareOp::Eq, 20).unwrap();
        assert_eq!(
            sel.as_slice(),
            &[1, 3],
            "bead_id={BEAD_FILTER} case=int32_eq"
        );
    }

    #[test]
    fn filter_int32_lt() {
        let batch = int32_batch(&[5, 15, 25]);
        let sel = filter_batch_int32(&batch, 0, CompareOp::Lt, 15).unwrap();
        assert_eq!(sel.as_slice(), &[0], "bead_id={BEAD_FILTER} case=int32_lt");
    }

    #[test]
    fn filter_int32_type_mismatch() {
        let batch = int64_batch(&[1, 2]);
        let err = filter_batch_int32(&batch, 0, CompareOp::Eq, 1).unwrap_err();
        assert!(
            err.contains("not Int32"),
            "bead_id={BEAD_FILTER} case=int32_type_mismatch"
        );
    }

    #[test]
    fn filter_text_eq() {
        let batch = text_batch(&["alice", "bob", "charlie", "alice"]);
        let sel = filter_batch_text(&batch, 0, CompareOp::Eq, "alice").unwrap();
        assert_eq!(
            sel.as_slice(),
            &[0, 3],
            "bead_id={BEAD_FILTER} case=text_eq"
        );
    }

    #[test]
    fn filter_text_lt() {
        let batch = text_batch(&["banana", "apple", "cherry"]);
        let sel = filter_batch_text(&batch, 0, CompareOp::Lt, "banana").unwrap();
        assert_eq!(sel.as_slice(), &[1], "bead_id={BEAD_FILTER} case=text_lt");
    }

    #[test]
    fn filter_text_ge() {
        let batch = text_batch(&["a", "b", "c", "d"]);
        let sel = filter_batch_text(&batch, 0, CompareOp::Ge, "c").unwrap();
        assert_eq!(
            sel.as_slice(),
            &[2, 3],
            "bead_id={BEAD_FILTER} case=text_ge"
        );
    }

    #[test]
    fn filter_text_type_mismatch() {
        let batch = int64_batch(&[1, 2]);
        let err = filter_batch_text(&batch, 0, CompareOp::Eq, "x").unwrap_err();
        assert!(
            err.contains("not Text"),
            "bead_id={BEAD_FILTER} case=text_type_mismatch"
        );
    }

    #[test]
    fn filter_binary_eq() {
        let batch = binary_batch(&[&[1, 2], &[3, 4], &[1, 2]]);
        let sel = filter_batch_binary(&batch, 0, CompareOp::Eq, &[1, 2]).unwrap();
        assert_eq!(
            sel.as_slice(),
            &[0, 2],
            "bead_id={BEAD_FILTER} case=binary_eq"
        );
    }

    #[test]
    fn filter_binary_ne() {
        let batch = binary_batch(&[&[1, 2], &[3, 4], &[1, 2]]);
        let sel = filter_batch_binary(&batch, 0, CompareOp::Ne, &[1, 2]).unwrap();
        assert_eq!(sel.as_slice(), &[1], "bead_id={BEAD_FILTER} case=binary_ne");
    }

    #[test]
    fn filter_binary_type_mismatch() {
        let batch = int64_batch(&[1]);
        let err = filter_batch_binary(&batch, 0, CompareOp::Eq, &[]).unwrap_err();
        assert!(
            err.contains("not Binary"),
            "bead_id={BEAD_FILTER} case=binary_type_mismatch"
        );
    }

    // ── NULL semantics tests (bd-14vp7.3) ───────────────────────────────

    fn int64_batch_with_nulls(values: &[Option<i64>]) -> Batch {
        let specs = vec![ColumnSpec::new("val", ColumnVectorType::Int64)];
        let rows: Vec<Vec<SqliteValue>> = values
            .iter()
            .map(|v| match v {
                Some(i) => vec![SqliteValue::Integer(*i)],
                None => vec![SqliteValue::Null],
            })
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    #[test]
    fn filter_null_rows_excluded_from_eq() {
        let batch = int64_batch_with_nulls(&[Some(10), None, Some(10), None]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Eq, 10).unwrap();
        // NULL rows are never selected.
        assert_eq!(
            sel.as_slice(),
            &[0, 2],
            "bead_id={BEAD_FILTER} case=null_excluded_eq"
        );
    }

    #[test]
    fn filter_null_rows_excluded_from_ne() {
        let batch = int64_batch_with_nulls(&[Some(10), None, Some(20)]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Ne, 10).unwrap();
        // NULL is not != 10 either; NULL comparisons produce false.
        assert_eq!(
            sel.as_slice(),
            &[2],
            "bead_id={BEAD_FILTER} case=null_excluded_ne"
        );
    }

    #[test]
    fn filter_all_nulls_returns_empty() {
        let batch = int64_batch_with_nulls(&[None, None, None]);
        let sel = filter_batch_int64(&batch, 0, CompareOp::Ge, 0).unwrap();
        assert!(
            sel.is_empty(),
            "bead_id={BEAD_FILTER} case=all_nulls_empty_result"
        );
    }

    // ── SIMD utilization metric test (bd-14vp7.3) ───────────────────────

    #[test]
    fn filter_updates_simd_utilization() {
        reset_vectorized_metrics();
        let batch = int64_batch(&[10, 20, 30]);
        let _ = filter_batch_int64(&batch, 0, CompareOp::Eq, 20).unwrap();
        let metrics = vectorized_metrics_snapshot();
        // On x86_64 with AVX2 or SSE2, utilization should be non-zero.
        #[cfg(target_arch = "x86_64")]
        assert!(
            metrics.simd_utilization_milli > 0,
            "bead_id={BEAD_FILTER} case=simd_utilization_nonzero"
        );
        #[cfg(not(target_arch = "x86_64"))]
        assert_eq!(metrics.simd_utilization_milli, 0);
    }

    // ── Row-at-a-time equivalence proof (bd-14vp7.3) ────────────────────

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn filter_matches_row_at_a_time_evaluation() {
        // Build a batch and compute filter result both vectorized and row-at-a-time.
        let values = [5_i64, 10, 15, 20, 25, 30, 35, 40];
        let batch = int64_batch(&values);

        // Vectorized result.
        let vec_sel = filter_batch_int64(&batch, 0, CompareOp::Le, 20).unwrap();

        // Row-at-a-time reference.
        let row_sel: Vec<u16> = values
            .iter()
            .enumerate()
            .filter(|&(_, v)| *v <= 20)
            .map(|(i, _)| i as u16)
            .collect();

        assert_eq!(
            vec_sel.as_slice(),
            &row_sel,
            "bead_id={BEAD_FILTER} case=vectorized_matches_row_at_a_time"
        );
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn filter_text_matches_row_at_a_time() {
        let values = ["delta", "alpha", "gamma", "beta"];
        let batch = text_batch(&values);
        let vec_sel = filter_batch_text(&batch, 0, CompareOp::Lt, "delta").unwrap();

        let row_sel: Vec<u16> = values
            .iter()
            .enumerate()
            .filter(|&(_, v)| v.as_bytes() < b"delta")
            .map(|(i, _)| i as u16)
            .collect();

        assert_eq!(
            vec_sel.as_slice(),
            &row_sel,
            "bead_id={BEAD_FILTER} case=text_vectorized_matches_row_at_a_time"
        );
    }

    // ── Chained multi-predicate test (bd-14vp7.3) ───────────────────────

    #[test]
    fn chained_filter_on_multi_column_batch() {
        let batch = multi_column_batch();
        // id > 1 AND score < 16.0
        let id_filter = filter_batch_int64(&batch, 0, CompareOp::Gt, 1).unwrap();
        // Apply id_filter as selection before score filter.
        let mut filtered_batch = batch;
        filtered_batch.apply_selection(id_filter).unwrap();
        let score_filter = filter_batch_float64(&filtered_batch, 1, CompareOp::Lt, 16.0).unwrap();
        // id=2/score=20 fails score<16, id=3/score=15.5 passes, id=4/score=5.0 passes.
        assert_eq!(
            score_filter.as_slice(),
            &[2, 3],
            "bead_id={BEAD_FILTER} case=chained_multi_column"
        );
    }
}
