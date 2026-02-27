//! Vectorized aggregation operators (bd-14vp7.5).
//!
//! Column-at-a-time hash and ordered aggregation over [`Batch`] values.
//! Supports COUNT, SUM, AVG, MIN, MAX, and TOTAL.
//!
//! Each operator emits a `vectorized_batch` tracing span and records rows
//! processed via [`record_vectorized_rows`].

use std::collections::HashMap;

use crate::vectorized::{
    Batch, BatchFormatError, Column, ColumnData, ColumnSpec, ColumnVectorType, NullBitmap,
    SelectionVector, record_vectorized_rows, set_vectorized_simd_utilization, simd_path_label,
};
use crate::vectorized_ops::hash_batch_columns;

// ── Aggregate Function Enum ────────────────────────────────────────────────

/// Supported aggregate functions for vectorized aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    /// COUNT(*) — counts all rows including NULLs.
    CountStar,
    /// COUNT(col) — counts non-NULL values in the specified column.
    Count,
    /// SUM(col) — sum of non-NULL values (NULL if all NULL).
    Sum,
    /// AVG(col) — average of non-NULL values (NULL if all NULL).
    Avg,
    /// MIN(col) — minimum non-NULL value (NULL if all NULL).
    Min,
    /// MAX(col) — maximum non-NULL value (NULL if all NULL).
    Max,
    /// TOTAL(col) — like SUM but returns 0.0 instead of NULL for empty sets.
    Total,
}

/// Specification of one aggregate computation.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    /// The aggregate function to apply.
    pub op: AggregateOp,
    /// Column index to aggregate over (ignored for `CountStar`).
    pub column_idx: usize,
    /// Output column name.
    pub output_name: String,
}

// ── Aggregate Accumulator ──────────────────────────────────────────────────

/// Internal accumulator state for a single aggregate function.
#[derive(Debug, Clone)]
struct Accumulator {
    op: AggregateOp,
    count: i64,
    sum_i64: i64,
    sum_f64: f64,
    min_i64: Option<i64>,
    max_i64: Option<i64>,
    min_f64: Option<f64>,
    max_f64: Option<f64>,
    is_float: bool,
}

impl Accumulator {
    fn new(op: AggregateOp) -> Self {
        Self {
            op,
            count: 0,
            sum_i64: 0,
            sum_f64: 0.0,
            min_i64: None,
            max_i64: None,
            min_f64: None,
            max_f64: None,
            is_float: false,
        }
    }

    fn update_i64(&mut self, val: i64) {
        self.count += 1;
        self.sum_i64 = self.sum_i64.wrapping_add(val);
        self.sum_f64 += val as f64;
        self.min_i64 = Some(self.min_i64.map_or(val, |m| m.min(val)));
        self.max_i64 = Some(self.max_i64.map_or(val, |m| m.max(val)));
    }

    fn update_f64(&mut self, val: f64) {
        self.is_float = true;
        self.count += 1;
        self.sum_f64 += val;
        self.min_f64 = Some(self.min_f64.map_or(val, |m| m.min(val)));
        self.max_f64 = Some(self.max_f64.map_or(val, |m| m.max(val)));
    }

    fn update_count_star(&mut self) {
        self.count += 1;
    }

    /// Finalize to an `i64` result, or `None` if the aggregate is NULL.
    fn finalize_i64(&self) -> Option<i64> {
        match self.op {
            AggregateOp::CountStar | AggregateOp::Count => Some(self.count),
            AggregateOp::Sum => {
                if self.count == 0 {
                    None
                } else {
                    Some(self.sum_i64)
                }
            }
            AggregateOp::Avg => {
                if self.count == 0 {
                    None
                } else {
                    Some(self.sum_i64 / self.count)
                }
            }
            AggregateOp::Min => self.min_i64,
            AggregateOp::Max => self.max_i64,
            AggregateOp::Total => Some(self.sum_i64),
        }
    }

    /// Finalize to an `f64` result, or `None` if the aggregate is NULL.
    fn finalize_f64(&self) -> Option<f64> {
        match self.op {
            AggregateOp::CountStar | AggregateOp::Count => Some(self.count as f64),
            AggregateOp::Sum => {
                if self.count == 0 {
                    None
                } else {
                    Some(self.sum_f64)
                }
            }
            AggregateOp::Avg => {
                if self.count == 0 {
                    None
                } else {
                    Some(self.sum_f64 / self.count as f64)
                }
            }
            AggregateOp::Min => self.min_f64,
            AggregateOp::Max => self.max_f64,
            AggregateOp::Total => Some(self.sum_f64),
        }
    }
}

// ── Hash Aggregation ───────────────────────────────────────────────────────

/// Perform hash-based aggregation over a batch.
///
/// Groups rows by the specified `group_by_columns`, then computes each
/// aggregate in `agg_specs` per group.  Returns a result batch with group key
/// columns followed by aggregate result columns.
///
/// # Errors
///
/// Returns an error if column indices are out of bounds or column types are
/// unsupported for the requested aggregate.
#[allow(clippy::too_many_lines)]
pub fn aggregate_batch_hash(
    batch: &Batch,
    group_by_columns: &[usize],
    agg_specs: &[AggregateSpec],
) -> Result<Batch, String> {
    let sel = batch.selection();
    let simd_path = simd_path_label();
    let input_rows = sel.len() as u64;

    // Hash group-by keys for bucket assignment.
    let group_hashes = if group_by_columns.is_empty() {
        vec![0_u64; sel.len()]
    } else {
        hash_batch_columns(batch, group_by_columns)?
    };

    // Map from hash -> list of (group_key, global_index_into_ordered)
    let mut group_map: HashMap<u64, Vec<(Vec<GroupKeyValue>, usize)>> = HashMap::new();
    let mut group_keys_ordered: Vec<Vec<GroupKeyValue>> = Vec::new();
    let mut all_accumulators: Vec<Vec<Accumulator>> = Vec::new();

    // SQLite semantics: aggregation without GROUP BY always returns 1 row, even if input is empty.
    if group_by_columns.is_empty() && sel.is_empty() {
        group_keys_ordered.push(vec![]);
        all_accumulators.push(agg_specs.iter().map(|s| Accumulator::new(s.op)).collect());
    }

    for (sel_idx, &row_idx) in sel.as_slice().iter().enumerate() {
        let row = usize::from(row_idx);
        let hash = group_hashes[sel_idx];
        let key = extract_group_key(batch, group_by_columns, row)?;

        // Look up or insert group.
        let bucket = group_map.entry(hash).or_default();
        let ordered_idx = match bucket.iter().find(|(k, _)| *k == key) {
            Some((_, idx)) => *idx,
            None => {
                let accs: Vec<Accumulator> =
                    agg_specs.iter().map(|s| Accumulator::new(s.op)).collect();
                let idx = all_accumulators.len();
                bucket.push((key.clone(), idx));
                group_keys_ordered.push(key);
                all_accumulators.push(accs);
                idx
            }
        };

        // Update accumulators.
        let accs = &mut all_accumulators[ordered_idx];

        for (agg_idx, spec) in agg_specs.iter().enumerate() {
            if spec.op == AggregateOp::CountStar {
                accs[agg_idx].update_count_star();
            } else {
                let col = batch
                    .columns()
                    .get(spec.column_idx)
                    .ok_or_else(|| format!("agg column index {} out of bounds", spec.column_idx))?;
                if !col.validity.is_valid(row) {
                    continue;
                }
                update_accumulator(&mut accs[agg_idx], &col.data, row);
            }
        }
    }

    // Build output batch.
    let num_groups = group_keys_ordered.len();

    // Build group-key output columns.
    let mut output_columns: Vec<Column> = Vec::new();
    for (key_idx, &col_idx) in group_by_columns.iter().enumerate() {
        let src_col = batch
            .columns()
            .get(col_idx)
            .ok_or_else(|| format!("group-by column index {col_idx} out of bounds"))?;
        let column =
            build_group_key_column(&src_col.spec, &group_keys_ordered, key_idx, num_groups)?;
        output_columns.push(column);
    }

    // Build aggregate result columns.
    for (agg_idx, spec) in agg_specs.iter().enumerate() {
        let column = build_agg_result_column(spec, &all_accumulators, agg_idx, num_groups)?;
        output_columns.push(column);
    }

    record_vectorized_rows(input_rows);
    update_agg_simd_utilization(simd_path);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        groups = num_groups,
        simd_path = simd_path,
        op = "aggregate_hash",
    )
    .entered();

    let selection =
        SelectionVector::identity(num_groups).map_err(|e: BatchFormatError| e.to_string())?;

    Batch::from_columns(output_columns, num_groups, num_groups.max(1), selection)
        .map_err(|e: BatchFormatError| e.to_string())
}

// ── Ordered Aggregation ────────────────────────────────────────────────────

/// Perform ordered aggregation over a batch that is already sorted by group keys.
///
/// Detects group boundaries via comparison and aggregates within each group.
/// More efficient than hash aggregation when input is pre-sorted.
///
/// # Errors
///
/// Returns an error if column indices are out of bounds.
#[allow(clippy::too_many_lines)]
pub fn aggregate_batch_ordered(
    batch: &Batch,
    group_by_columns: &[usize],
    agg_specs: &[AggregateSpec],
) -> Result<Batch, String> {
    let sel = batch.selection();
    let simd_path = simd_path_label();
    let input_rows = sel.len() as u64;

    let mut group_keys_ordered: Vec<Vec<GroupKeyValue>> = Vec::new();
    let mut all_accumulators: Vec<Vec<Accumulator>> = Vec::new();
    let mut current_key: Option<Vec<GroupKeyValue>> = None;

    // SQLite semantics: aggregation without GROUP BY always returns 1 row, even if input is empty.
    if group_by_columns.is_empty() && sel.is_empty() {
        group_keys_ordered.push(vec![]);
        all_accumulators.push(agg_specs.iter().map(|s| Accumulator::new(s.op)).collect());
    }

    for &row_idx in sel.as_slice() {
        let row = usize::from(row_idx);
        let key = extract_group_key(batch, group_by_columns, row)?;

        let is_new_group = current_key.as_ref().is_none_or(|k| *k != key);
        if is_new_group {
            current_key = Some(key.clone());
            group_keys_ordered.push(key);
            let accs: Vec<Accumulator> = agg_specs.iter().map(|s| Accumulator::new(s.op)).collect();
            all_accumulators.push(accs);
        }

        let accs = all_accumulators.last_mut().expect("at least one group");
        for (agg_idx, spec) in agg_specs.iter().enumerate() {
            if spec.op == AggregateOp::CountStar {
                accs[agg_idx].update_count_star();
            } else {
                let col = batch
                    .columns()
                    .get(spec.column_idx)
                    .ok_or_else(|| format!("agg column index {} out of bounds", spec.column_idx))?;
                if !col.validity.is_valid(row) {
                    continue;
                }
                update_accumulator(&mut accs[agg_idx], &col.data, row);
            }
        }
    }

    let num_groups = group_keys_ordered.len();
    let mut output_columns: Vec<Column> = Vec::new();

    for (key_idx, &col_idx) in group_by_columns.iter().enumerate() {
        let src_col = batch
            .columns()
            .get(col_idx)
            .ok_or_else(|| format!("group-by column index {col_idx} out of bounds"))?;
        let column =
            build_group_key_column(&src_col.spec, &group_keys_ordered, key_idx, num_groups)?;
        output_columns.push(column);
    }

    for (agg_idx, spec) in agg_specs.iter().enumerate() {
        let column = build_agg_result_column(spec, &all_accumulators, agg_idx, num_groups)?;
        output_columns.push(column);
    }

    record_vectorized_rows(input_rows);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        groups = num_groups,
        simd_path = simd_path,
        op = "aggregate_ordered",
    )
    .entered();

    let selection =
        SelectionVector::identity(num_groups).map_err(|e: BatchFormatError| e.to_string())?;

    Batch::from_columns(output_columns, num_groups, num_groups.max(1), selection)
        .map_err(|e: BatchFormatError| e.to_string())
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// A single group key value for comparison.
#[derive(Debug, Clone, PartialEq)]
enum GroupKeyValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

fn extract_group_key(
    batch: &Batch,
    group_by_columns: &[usize],
    row: usize,
) -> Result<Vec<GroupKeyValue>, String> {
    let mut key = Vec::with_capacity(group_by_columns.len());
    for &col_idx in group_by_columns {
        let col = batch
            .columns()
            .get(col_idx)
            .ok_or_else(|| format!("group-by column index {col_idx} out of bounds"))?;
        if !col.validity.is_valid(row) {
            key.push(GroupKeyValue::Null);
            continue;
        }
        let val = match &col.data {
            ColumnData::Int8(v) => GroupKeyValue::Integer(i64::from(v.as_slice()[row])),
            ColumnData::Int16(v) => GroupKeyValue::Integer(i64::from(v.as_slice()[row])),
            ColumnData::Int32(v) => GroupKeyValue::Integer(i64::from(v.as_slice()[row])),
            ColumnData::Int64(v) => GroupKeyValue::Integer(v.as_slice()[row]),
            ColumnData::Float32(v) => GroupKeyValue::Float(f64::from(v.as_slice()[row])),
            ColumnData::Float64(v) => GroupKeyValue::Float(v.as_slice()[row]),
            ColumnData::Text { offsets, data } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let s = String::from_utf8_lossy(&data[start..end]).into_owned();
                GroupKeyValue::Text(s)
            }
            ColumnData::Binary { offsets, data } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                GroupKeyValue::Blob(data[start..end].to_vec())
            }
        };
        key.push(val);
    }
    Ok(key)
}

#[allow(clippy::cast_sign_loss)]
fn update_accumulator(acc: &mut Accumulator, data: &ColumnData, row: usize) {
    match data {
        ColumnData::Int8(v) => acc.update_i64(i64::from(v.as_slice()[row])),
        ColumnData::Int16(v) => acc.update_i64(i64::from(v.as_slice()[row])),
        ColumnData::Int32(v) => acc.update_i64(i64::from(v.as_slice()[row])),
        ColumnData::Int64(v) => acc.update_i64(v.as_slice()[row]),
        ColumnData::Float32(v) => acc.update_f64(f64::from(v.as_slice()[row])),
        ColumnData::Float64(v) => acc.update_f64(v.as_slice()[row]),
        // For text/binary, only COUNT makes sense; update count.
        ColumnData::Text { .. } | ColumnData::Binary { .. } => acc.update_count_star(),
    }
}

fn build_group_key_column(
    spec: &ColumnSpec,
    group_keys: &[Vec<GroupKeyValue>],
    key_idx: usize,
    num_groups: usize,
) -> Result<Column, String> {
    let mut validity_bytes = vec![0_u8; num_groups.div_ceil(8)];

    let set_valid = |validity_bytes: &mut [u8], group_idx: usize| {
        let byte_idx = group_idx / 8;
        let bit = group_idx % 8;
        validity_bytes[byte_idx] |= 1_u8 << bit;
    };

    let data = match spec.vector_type {
        ColumnVectorType::Int64 => {
            let mut values = Vec::with_capacity(num_groups);
            for (group_idx, key) in group_keys.iter().enumerate() {
                let val = &key[key_idx];
                let i = match val {
                    GroupKeyValue::Null => {
                        values.push(0_i64);
                        continue;
                    }
                    GroupKeyValue::Integer(v) => *v,
                    #[allow(clippy::cast_possible_truncation)]
                    GroupKeyValue::Float(v) => *v as i64,
                    GroupKeyValue::Text(_) | GroupKeyValue::Blob(_) => 0,
                };
                values.push(i);
                set_valid(&mut validity_bytes, group_idx);
            }
            let aligned = crate::vectorized::AlignedValues::from_vec(
                values,
                crate::vectorized::DEFAULT_SIMD_ALIGNMENT_BYTES,
            )
            .map_err(|e| e.to_string())?;
            ColumnData::Int64(aligned)
        }
        ColumnVectorType::Float64 => {
            let mut values = Vec::with_capacity(num_groups);
            for (group_idx, key) in group_keys.iter().enumerate() {
                let val = &key[key_idx];
                let f = match val {
                    GroupKeyValue::Null => {
                        values.push(0.0_f64);
                        continue;
                    }
                    #[allow(clippy::cast_precision_loss)]
                    GroupKeyValue::Integer(v) => *v as f64,
                    GroupKeyValue::Float(v) => *v,
                    GroupKeyValue::Text(_) | GroupKeyValue::Blob(_) => 0.0,
                };
                values.push(f);
                set_valid(&mut validity_bytes, group_idx);
            }
            let aligned = crate::vectorized::AlignedValues::from_vec(
                values,
                crate::vectorized::DEFAULT_SIMD_ALIGNMENT_BYTES,
            )
            .map_err(|e| e.to_string())?;
            ColumnData::Float64(aligned)
        }
        ColumnVectorType::Text => {
            let mut new_offsets: Vec<u32> = Vec::with_capacity(num_groups + 1);
            let mut new_data: Vec<u8> = Vec::new();
            new_offsets.push(0);
            for (group_idx, key) in group_keys.iter().enumerate() {
                let val = &key[key_idx];
                match val {
                    GroupKeyValue::Null => {}
                    GroupKeyValue::Text(t) => {
                        new_data.extend_from_slice(t.as_bytes());
                        set_valid(&mut validity_bytes, group_idx);
                    }
                    _ => {
                        // fallback coercion not fully implemented
                        set_valid(&mut validity_bytes, group_idx);
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                new_offsets.push(new_data.len() as u32);
            }
            ColumnData::Text {
                offsets: std::sync::Arc::from(new_offsets),
                data: std::sync::Arc::from(new_data),
            }
        }
        ColumnVectorType::Binary => {
            let mut new_offsets: Vec<u32> = Vec::with_capacity(num_groups + 1);
            let mut new_data: Vec<u8> = Vec::new();
            new_offsets.push(0);
            for (group_idx, key) in group_keys.iter().enumerate() {
                let val = &key[key_idx];
                match val {
                    GroupKeyValue::Null => {}
                    GroupKeyValue::Blob(b) => {
                        new_data.extend_from_slice(b);
                        set_valid(&mut validity_bytes, group_idx);
                    }
                    _ => {
                        // fallback coercion not fully implemented
                        set_valid(&mut validity_bytes, group_idx);
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                new_offsets.push(new_data.len() as u32);
            }
            ColumnData::Binary {
                offsets: std::sync::Arc::from(new_offsets),
                data: std::sync::Arc::from(new_data),
            }
        }
        _ => {
            return Err(format!(
                "unsupported group key type: {:?}",
                spec.vector_type
            ));
        }
    };

    let validity = NullBitmap::from_bytes(std::sync::Arc::from(validity_bytes), num_groups)
        .map_err(|e| e.to_string())?;

    Ok(Column {
        spec: spec.clone(),
        data,
        validity,
    })
}

fn build_agg_result_column(
    spec: &AggregateSpec,
    all_accumulators: &[Vec<Accumulator>],
    agg_idx: usize,
    num_groups: usize,
) -> Result<Column, String> {
    let needs_float = matches!(spec.op, AggregateOp::Avg | AggregateOp::Total)
        || all_accumulators.iter().any(|accs| accs[agg_idx].is_float);

    if needs_float {
        build_float_agg_column(spec, all_accumulators, agg_idx, num_groups)
    } else {
        build_int_agg_column(spec, all_accumulators, agg_idx, num_groups)
    }
}

fn build_int_agg_column(
    spec: &AggregateSpec,
    all_accumulators: &[Vec<Accumulator>],
    agg_idx: usize,
    num_groups: usize,
) -> Result<Column, String> {
    let mut values = Vec::with_capacity(num_groups);
    let mut validity_bytes = vec![0_u8; num_groups.div_ceil(8)];

    for (group_idx, accs) in all_accumulators.iter().enumerate() {
        match accs[agg_idx].finalize_i64() {
            Some(v) => {
                values.push(v);
                let byte_idx = group_idx / 8;
                let bit = group_idx % 8;
                validity_bytes[byte_idx] |= 1_u8 << bit;
            }
            None => {
                values.push(0);
            }
        }
    }

    let aligned = crate::vectorized::AlignedValues::from_vec(
        values,
        crate::vectorized::DEFAULT_SIMD_ALIGNMENT_BYTES,
    )
    .map_err(|e| e.to_string())?;

    let validity = NullBitmap::from_bytes(std::sync::Arc::from(validity_bytes), num_groups)
        .map_err(|e| e.to_string())?;

    Ok(Column {
        spec: ColumnSpec::new(&spec.output_name, ColumnVectorType::Int64),
        data: ColumnData::Int64(aligned),
        validity,
    })
}

fn build_float_agg_column(
    spec: &AggregateSpec,
    all_accumulators: &[Vec<Accumulator>],
    agg_idx: usize,
    num_groups: usize,
) -> Result<Column, String> {
    let mut values = Vec::with_capacity(num_groups);
    let mut validity_bytes = vec![0_u8; num_groups.div_ceil(8)];

    for (group_idx, accs) in all_accumulators.iter().enumerate() {
        match accs[agg_idx].finalize_f64() {
            Some(v) => {
                values.push(v);
                let byte_idx = group_idx / 8;
                let bit = group_idx % 8;
                validity_bytes[byte_idx] |= 1_u8 << bit;
            }
            None => {
                values.push(0.0);
            }
        }
    }

    let aligned = crate::vectorized::AlignedValues::from_vec(
        values,
        crate::vectorized::DEFAULT_SIMD_ALIGNMENT_BYTES,
    )
    .map_err(|e| e.to_string())?;

    let validity = NullBitmap::from_bytes(std::sync::Arc::from(validity_bytes), num_groups)
        .map_err(|e| e.to_string())?;

    Ok(Column {
        spec: ColumnSpec::new(&spec.output_name, ColumnVectorType::Float64),
        data: ColumnData::Float64(aligned),
        validity,
    })
}

/// Update SIMD utilization gauge based on the active path.
fn update_agg_simd_utilization(simd_path: &str) {
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
        Batch, ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY, reset_vectorized_metrics,
        vectorized_metrics_snapshot,
    };
    use fsqlite_types::value::SqliteValue;

    const BEAD_AGG: &str = "bd-14vp7.5";

    fn int64_batch_grouped(groups: &[(i64, i64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("group_id", ColumnVectorType::Int64),
            ColumnSpec::new("value", ColumnVectorType::Int64),
        ];
        let rows: Vec<Vec<SqliteValue>> = groups
            .iter()
            .map(|&(g, v)| vec![SqliteValue::Integer(g), SqliteValue::Integer(v)])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    fn single_column_batch(values: &[Option<i64>]) -> Batch {
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

    fn float_grouped_batch(groups: &[(i64, f64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("group_id", ColumnVectorType::Int64),
            ColumnSpec::new("value", ColumnVectorType::Float64),
        ];
        let rows: Vec<Vec<SqliteValue>> = groups
            .iter()
            .map(|&(g, v)| vec![SqliteValue::Integer(g), SqliteValue::Float(v)])
            .collect();
        Batch::from_rows(&rows, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch should build")
    }

    // ── Hash aggregation tests ──────────────────────────────────────────

    #[test]
    fn hash_agg_count_star_no_groups() {
        let batch = single_column_batch(&[Some(10), Some(20), Some(30)]);
        let result = aggregate_batch_hash(
            &batch,
            &[],
            &[AggregateSpec {
                op: AggregateOp::CountStar,
                column_idx: 0,
                output_name: "cnt".to_owned(),
            }],
        )
        .unwrap();

        assert_eq!(
            result.row_count(),
            1,
            "bead_id={BEAD_AGG} case=count_star_no_groups"
        );
        let col = &result.columns()[0];
        if let ColumnData::Int64(v) = &col.data {
            assert_eq!(v.as_slice()[0], 3);
        }
    }

    #[test]
    fn hash_agg_sum_with_groups() {
        let batch = int64_batch_grouped(&[(1, 10), (1, 20), (2, 30), (2, 40), (2, 50)]);
        let result = aggregate_batch_hash(
            &batch,
            &[0],
            &[AggregateSpec {
                op: AggregateOp::Sum,
                column_idx: 1,
                output_name: "total".to_owned(),
            }],
        )
        .unwrap();

        assert_eq!(
            result.row_count(),
            2,
            "bead_id={BEAD_AGG} case=sum_with_groups"
        );
        // Group 1: sum=30, Group 2: sum=120
        let agg_col = &result.columns()[1];
        if let ColumnData::Int64(v) = &agg_col.data {
            let sums = v.as_slice();
            assert!(sums.contains(&30), "bead_id={BEAD_AGG} case=group1_sum");
            assert!(sums.contains(&120), "bead_id={BEAD_AGG} case=group2_sum");
        }
    }

    #[test]
    fn hash_agg_count_excludes_nulls() {
        let batch = single_column_batch(&[Some(10), None, Some(30), None]);
        let result = aggregate_batch_hash(
            &batch,
            &[],
            &[AggregateSpec {
                op: AggregateOp::Count,
                column_idx: 0,
                output_name: "cnt".to_owned(),
            }],
        )
        .unwrap();

        let col = &result.columns()[0];
        if let ColumnData::Int64(v) = &col.data {
            assert_eq!(
                v.as_slice()[0],
                2,
                "bead_id={BEAD_AGG} case=count_excludes_nulls"
            );
        }
    }

    #[test]
    fn hash_agg_min_max() {
        let batch = int64_batch_grouped(&[(1, 5), (1, 15), (1, 10)]);
        let result = aggregate_batch_hash(
            &batch,
            &[0],
            &[
                AggregateSpec {
                    op: AggregateOp::Min,
                    column_idx: 1,
                    output_name: "min_val".to_owned(),
                },
                AggregateSpec {
                    op: AggregateOp::Max,
                    column_idx: 1,
                    output_name: "max_val".to_owned(),
                },
            ],
        )
        .unwrap();

        let min_col = &result.columns()[1];
        let max_col = &result.columns()[2];
        if let (ColumnData::Int64(min_v), ColumnData::Int64(max_v)) = (&min_col.data, &max_col.data)
        {
            assert_eq!(min_v.as_slice()[0], 5, "bead_id={BEAD_AGG} case=min");
            assert_eq!(max_v.as_slice()[0], 15, "bead_id={BEAD_AGG} case=max");
        }
    }

    #[test]
    fn hash_agg_avg_float() {
        let batch = float_grouped_batch(&[(1, 10.0), (1, 20.0), (1, 30.0)]);
        let result = aggregate_batch_hash(
            &batch,
            &[0],
            &[AggregateSpec {
                op: AggregateOp::Avg,
                column_idx: 1,
                output_name: "avg_val".to_owned(),
            }],
        )
        .unwrap();

        let avg_col = &result.columns()[1];
        if let ColumnData::Float64(v) = &avg_col.data {
            let avg = v.as_slice()[0];
            assert!(
                (avg - 20.0).abs() < f64::EPSILON,
                "bead_id={BEAD_AGG} case=avg_float expected=20.0 got={avg}"
            );
        }
    }

    #[test]
    fn hash_agg_total_returns_zero_for_empty() {
        let batch = single_column_batch(&[]);
        let result = aggregate_batch_hash(
            &batch,
            &[],
            &[AggregateSpec {
                op: AggregateOp::Total,
                column_idx: 0,
                output_name: "total".to_owned(),
            }],
        )
        .unwrap();

        // 1 row should be returned for a scalar aggregate on an empty set.
        assert_eq!(result.row_count(), 1, "bead_id={BEAD_AGG} case=total_empty");

        let col = &result.columns()[0];
        if let ColumnData::Float64(v) = &col.data {
            assert_eq!(
                v.as_slice()[0],
                0.0,
                "TOTAL() on empty set should return 0.0"
            );
        } else {
            panic!("Expected Float64 column");
        }
    }

    #[test]
    fn hash_agg_sum_all_nulls_is_null() {
        let batch = single_column_batch(&[None, None, None]);
        let result = aggregate_batch_hash(
            &batch,
            &[],
            &[AggregateSpec {
                op: AggregateOp::Sum,
                column_idx: 0,
                output_name: "sum_val".to_owned(),
            }],
        )
        .unwrap();

        let col = &result.columns()[0];
        // SUM of all NULLs should be NULL (validity bit unset).
        assert!(
            !col.validity.is_valid(0),
            "bead_id={BEAD_AGG} case=sum_all_nulls_is_null"
        );
    }

    // ── Ordered aggregation tests ───────────────────────────────────────

    #[test]
    fn ordered_agg_matches_hash_agg() {
        // Input already sorted by group key.
        let batch = int64_batch_grouped(&[(1, 10), (1, 20), (2, 30), (2, 40)]);
        let specs = vec![AggregateSpec {
            op: AggregateOp::Sum,
            column_idx: 1,
            output_name: "total".to_owned(),
        }];

        let hash_result = aggregate_batch_hash(&batch, &[0], &specs).unwrap();
        let ordered_result = aggregate_batch_ordered(&batch, &[0], &specs).unwrap();

        assert_eq!(
            hash_result.row_count(),
            ordered_result.row_count(),
            "bead_id={BEAD_AGG} case=ordered_matches_hash_groups"
        );

        // Both should produce same aggregate values.
        let hash_agg = &hash_result.columns()[1];
        let ordered_agg = &ordered_result.columns()[1];
        if let (ColumnData::Int64(h), ColumnData::Int64(o)) = (&hash_agg.data, &ordered_agg.data) {
            assert_eq!(
                h.as_slice(),
                o.as_slice(),
                "bead_id={BEAD_AGG} case=ordered_matches_hash_values"
            );
        }
    }

    #[test]
    fn ordered_agg_single_group() {
        let batch = int64_batch_grouped(&[(1, 5), (1, 10), (1, 15)]);
        let result = aggregate_batch_ordered(
            &batch,
            &[0],
            &[AggregateSpec {
                op: AggregateOp::Count,
                column_idx: 1,
                output_name: "cnt".to_owned(),
            }],
        )
        .unwrap();

        assert_eq!(
            result.row_count(),
            1,
            "bead_id={BEAD_AGG} case=ordered_single_group"
        );
        let cnt_col = &result.columns()[1];
        if let ColumnData::Int64(v) = &cnt_col.data {
            assert_eq!(v.as_slice()[0], 3, "bead_id={BEAD_AGG} case=ordered_count");
        }
    }

    // ── Metrics test ────────────────────────────────────────────────────

    #[test]
    fn agg_records_metrics() {
        reset_vectorized_metrics();
        let batch = int64_batch_grouped(&[(1, 10), (2, 20)]);
        let _ = aggregate_batch_hash(
            &batch,
            &[0],
            &[AggregateSpec {
                op: AggregateOp::CountStar,
                column_idx: 0,
                output_name: "cnt".to_owned(),
            }],
        )
        .unwrap();
        let metrics = vectorized_metrics_snapshot();
        assert!(
            metrics.vectorized_rows_total >= 2,
            "bead_id={BEAD_AGG} case=agg_metrics_recorded"
        );
    }

    #[test]
    fn agg_column_out_of_bounds() {
        let batch = single_column_batch(&[Some(1)]);
        let err = aggregate_batch_hash(
            &batch,
            &[],
            &[AggregateSpec {
                op: AggregateOp::Sum,
                column_idx: 5,
                output_name: "bad".to_owned(),
            }],
        )
        .unwrap_err();
        assert!(
            err.contains("out of bounds"),
            "bead_id={BEAD_AGG} case=column_out_of_bounds"
        );
    }

    #[test]
    fn hash_agg_multiple_agg_functions() {
        let batch = int64_batch_grouped(&[(1, 10), (1, 20), (1, 30)]);
        let result = aggregate_batch_hash(
            &batch,
            &[0],
            &[
                AggregateSpec {
                    op: AggregateOp::CountStar,
                    column_idx: 0,
                    output_name: "cnt".to_owned(),
                },
                AggregateSpec {
                    op: AggregateOp::Sum,
                    column_idx: 1,
                    output_name: "sum".to_owned(),
                },
                AggregateSpec {
                    op: AggregateOp::Min,
                    column_idx: 1,
                    output_name: "min".to_owned(),
                },
                AggregateSpec {
                    op: AggregateOp::Max,
                    column_idx: 1,
                    output_name: "max".to_owned(),
                },
            ],
        )
        .unwrap();

        assert_eq!(result.row_count(), 1);
        // Columns: group_id, cnt, sum, min, max
        assert_eq!(
            result.columns().len(),
            5,
            "bead_id={BEAD_AGG} case=multi_agg_columns"
        );
    }
}
