//! Vectorized hash-join operator (bd-14vp7.4).
//!
//! Two-phase equi-join: build a hash table from the build side, then probe
//! with the probe side to produce output batches.  Supports inner, left outer,
//! semi, and anti join variants.

use std::collections::HashMap;

use crate::vectorized::{
    AlignedValues, Batch, BatchFormatError, Column, ColumnData, ColumnSpec, ColumnVectorType,
    DEFAULT_SIMD_ALIGNMENT_BYTES, NullBitmap, SelectionVector, record_vectorized_rows,
    set_vectorized_simd_utilization, simd_path_label,
};
use crate::vectorized_ops::hash_batch_columns;

// ── Join Type ──────────────────────────────────────────────────────────────

/// Supported join variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner join: emit only matching pairs.
    Inner,
    /// Left outer join: emit all probe rows; NULL-pad build columns on miss.
    Left,
    /// Semi join: emit probe rows that have at least one build match (no dups).
    Semi,
    /// Anti join: emit probe rows with no build match.
    Anti,
}

// ── Build-Side Hash Table ──────────────────────────────────────────────────

/// A single row captured from the build side.
#[derive(Debug, Clone)]
struct BuildRow {
    /// Key column values for equality matching.
    key: Vec<KeyValue>,
    /// Index of this row within the build batch.
    row_index: usize,
}

/// Comparable key value extracted from a column.
#[derive(Debug, Clone, PartialEq)]
enum KeyValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// Hash table built from one or more build-side batches.
#[derive(Debug)]
pub struct HashJoinTable {
    /// Dense array of all valid build rows.
    rows: Vec<BuildRow>,
    /// Maps a hash to the index of the first row in `rows` (the head of the collision chain).
    head: HashMap<u64, usize>,
    /// Parallel array to `rows`. Maps a row index to the next row index in the collision chain.
    next: Vec<Option<usize>>,
    /// The build batch (for column extraction during output).
    build_batch: Batch,
    /// Column indices used for join keys on the build side.
    build_key_columns: Vec<usize>,
}

/// Build a hash table from a build-side batch.
///
/// `build_key_columns` identifies which columns form the join key.
///
/// # Errors
///
/// Returns an error if key column indices are out of bounds.
pub fn hash_join_build(
    build_batch: Batch,
    build_key_columns: &[usize],
) -> Result<HashJoinTable, String> {
    let sel = build_batch.selection();
    let hashes = hash_batch_columns(&build_batch, build_key_columns)?;

    let mut rows = Vec::with_capacity(sel.len());
    let mut head: HashMap<u64, usize> = HashMap::new();
    let mut next: Vec<Option<usize>> = Vec::with_capacity(sel.len());

    for (sel_idx, &row_idx) in sel.as_slice().iter().enumerate() {
        let row = usize::from(row_idx);
        let key = extract_key(&build_batch, build_key_columns, row)?;

        // Skip NULL keys (SQL join semantics: NULL != NULL).
        if key.iter().any(|k| matches!(k, KeyValue::Null)) {
            continue;
        }

        let hash = hashes[sel_idx];
        let current_idx = rows.len();

        rows.push(BuildRow {
            key,
            row_index: row,
        });

        if let Some(&prev_head) = head.get(&hash) {
            next.push(Some(prev_head));
        } else {
            next.push(None);
        }
        head.insert(hash, current_idx);
    }

    Ok(HashJoinTable {
        rows,
        head,
        next,
        build_batch,
        build_key_columns: build_key_columns.to_vec(),
    })
}

// ── Probe Phase ────────────────────────────────────────────────────────────

/// Probe the hash table with a probe batch and produce a result batch.
///
/// `probe_key_columns` identifies which probe columns correspond to the
/// build key columns.  The output batch contains probe columns followed
/// by build columns (excluding build key columns to avoid duplication).
///
/// # Errors
///
/// Returns an error if column indices are out of bounds.
#[allow(clippy::too_many_lines)]
pub fn hash_join_probe(
    table: &HashJoinTable,
    probe_batch: &Batch,
    probe_key_columns: &[usize],
    join_type: JoinType,
) -> Result<Batch, String> {
    let sel = probe_batch.selection();
    let simd_path = simd_path_label();
    let input_rows = sel.len() as u64;

    let probe_hashes = hash_batch_columns(probe_batch, probe_key_columns)?;

    // Collect (probe_row, build_row) match pairs.
    let mut probe_indices: Vec<usize> = Vec::new();
    let mut build_indices: Vec<Option<usize>> = Vec::new();
    let mut probe_matched = vec![false; probe_batch.row_count()];

    for (sel_idx, &row_idx) in sel.as_slice().iter().enumerate() {
        let probe_row = usize::from(row_idx);
        let probe_key = extract_key(probe_batch, probe_key_columns, probe_row)?;

        // NULL keys never match.
        if probe_key.iter().any(|k| matches!(k, KeyValue::Null)) {
            match join_type {
                JoinType::Left | JoinType::Anti => {
                    probe_indices.push(probe_row);
                    build_indices.push(None);
                }
                JoinType::Inner | JoinType::Semi => {}
            }
            continue;
        }

        let hash = probe_hashes[sel_idx];
        let matches = find_build_matches(table, hash, &probe_key);

        if matches.is_empty() {
            match join_type {
                JoinType::Left | JoinType::Anti => {
                    probe_indices.push(probe_row);
                    build_indices.push(None);
                }
                JoinType::Inner | JoinType::Semi => {}
            }
        } else {
            probe_matched[probe_row] = true;
            match join_type {
                JoinType::Inner | JoinType::Left => {
                    for &build_row in &matches {
                        probe_indices.push(probe_row);
                        build_indices.push(Some(build_row));
                    }
                }
                JoinType::Semi => {
                    // Only emit once per probe row.
                    probe_indices.push(probe_row);
                    build_indices.push(Some(matches[0]));
                }
                JoinType::Anti => {
                    // Has match → skip for anti join.
                }
            }
        }
    }

    let output_rows = probe_indices.len();

    // Build output columns: probe columns + non-key build columns.
    let mut output_columns: Vec<Column> = Vec::new();

    // Probe columns (gathered by probe_indices).
    for probe_col in probe_batch.columns() {
        let col = gather_column(
            &probe_col.spec,
            &probe_col.data,
            &probe_col.validity,
            &probe_indices,
            output_rows,
        )?;
        output_columns.push(col);
    }

    // Build non-key columns (gathered by build_indices, NULL-padded for misses).
    let build_key_set: std::collections::HashSet<usize> =
        table.build_key_columns.iter().copied().collect();
    for (col_idx, build_col) in table.build_batch.columns().iter().enumerate() {
        if build_key_set.contains(&col_idx) {
            continue;
        }
        let col = gather_column_nullable(
            &build_col.spec,
            &build_col.data,
            &build_col.validity,
            &build_indices,
            output_rows,
        )?;
        output_columns.push(col);
    }

    record_vectorized_rows(input_rows);
    update_join_simd_utilization(simd_path);

    let _span = tracing::debug_span!(
        "vectorized_batch",
        batch_size = input_rows,
        output_rows = output_rows,
        simd_path = simd_path,
        join_type = ?join_type,
        op = "hash_join_probe",
    )
    .entered();

    if output_rows == 0 {
        // Return empty batch with correct schema.
        let specs: Vec<ColumnSpec> = output_columns.iter().map(|c| c.spec.clone()).collect();
        return empty_batch_with_schema(&specs);
    }

    let selection =
        SelectionVector::identity(output_rows).map_err(|e: BatchFormatError| e.to_string())?;

    Batch::from_columns(output_columns, output_rows, output_rows.max(1), selection)
        .map_err(|e: BatchFormatError| e.to_string())
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn extract_key(batch: &Batch, key_columns: &[usize], row: usize) -> Result<Vec<KeyValue>, String> {
    let mut key = Vec::with_capacity(key_columns.len());
    for &col_idx in key_columns {
        let col = batch
            .columns()
            .get(col_idx)
            .ok_or_else(|| format!("join key column index {col_idx} out of bounds"))?;
        if !col.validity.is_valid(row) {
            key.push(KeyValue::Null);
            continue;
        }
        let val = match &col.data {
            ColumnData::Int8(v) => KeyValue::Integer(i64::from(v.as_slice()[row])),
            ColumnData::Int16(v) => KeyValue::Integer(i64::from(v.as_slice()[row])),
            ColumnData::Int32(v) => KeyValue::Integer(i64::from(v.as_slice()[row])),
            ColumnData::Int64(v) => KeyValue::Integer(v.as_slice()[row]),
            ColumnData::Float32(v) => KeyValue::Float(f64::from(v.as_slice()[row])),
            ColumnData::Float64(v) => KeyValue::Float(v.as_slice()[row]),
            ColumnData::Text { offsets, data } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                KeyValue::Text(String::from_utf8_lossy(&data[start..end]).into_owned())
            }
            ColumnData::Binary { offsets, data } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                KeyValue::Blob(data[start..end].to_vec())
            }
        };
        key.push(val);
    }
    Ok(key)
}

fn find_build_matches(table: &HashJoinTable, hash: u64, probe_key: &[KeyValue]) -> Vec<usize> {
    let mut matches = Vec::new();
    let mut current = table.head.get(&hash).copied();
    while let Some(idx) = current {
        let build_row = &table.rows[idx];
        if build_row.key == *probe_key {
            matches.push(build_row.row_index);
        }
        current = table.next[idx];
    }
    // We inserted items by pushing onto the head, meaning we traverse from newest to oldest.
    // We reverse to maintain the stable insertion order of the original build side, matching the old HashMap bucket ordering.
    matches.reverse();
    matches
}

/// Gather values from a column at specified row indices.
fn gather_column(
    spec: &ColumnSpec,
    data: &ColumnData,
    validity: &NullBitmap,
    indices: &[usize],
    output_rows: usize,
) -> Result<Column, String> {
    let no_extra_nulls = vec![false; output_rows];
    gather_column_inner(spec, data, validity, indices, output_rows, &no_extra_nulls)
}

/// Gather values from a column at optional row indices (None → NULL).
fn gather_column_nullable(
    spec: &ColumnSpec,
    data: &ColumnData,
    validity: &NullBitmap,
    indices: &[Option<usize>],
    output_rows: usize,
) -> Result<Column, String> {
    // Convert Option<usize> to usize with a sentinel, tracking nulls separately.
    let mut flat_indices = Vec::with_capacity(output_rows);
    let mut extra_nulls = vec![false; output_rows];
    for (out_idx, idx) in indices.iter().enumerate() {
        if let Some(i) = idx {
            flat_indices.push(*i);
        } else {
            flat_indices.push(0); // sentinel; will be overridden by null
            extra_nulls[out_idx] = true;
        }
    }
    gather_column_inner(
        spec,
        data,
        validity,
        &flat_indices,
        output_rows,
        &extra_nulls,
    )
}

#[allow(clippy::too_many_lines)]
fn gather_column_inner(
    spec: &ColumnSpec,
    data: &ColumnData,
    validity: &NullBitmap,
    indices: &[usize],
    output_rows: usize,
    extra_nulls: &[bool],
) -> Result<Column, String> {
    let mut validity_bytes = vec![0_u8; output_rows.div_ceil(8)];

    let set_valid = |validity_bytes: &mut [u8],
                     out_idx: usize,
                     row: usize,
                     extra_nulls: &[bool],
                     validity: &NullBitmap| {
        if !extra_nulls[out_idx] && validity.is_valid(row) {
            validity_bytes[out_idx / 8] |= 1_u8 << (out_idx % 8);
        }
    };

    let out_data = match data {
        ColumnData::Int8(v) => {
            let src = v.as_slice();
            let mut values = Vec::with_capacity(output_rows);
            for (out_idx, &row) in indices.iter().enumerate() {
                values.push(src.get(row).copied().unwrap_or_default());
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                values.push(src.get(row).copied().unwrap_or_default());
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                values.push(src.get(row).copied().unwrap_or_default());
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                values.push(src.get(row).copied().unwrap_or_default());
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                values.push(src.get(row).copied().unwrap_or_default());
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                values.push(src.get(row).copied().unwrap_or_default());
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                let start = offsets.get(row).copied().unwrap_or_default() as usize;
                let end = offsets.get(row + 1).copied().unwrap_or_default() as usize;
                if start <= end && end <= src.len() {
                    new_data.extend_from_slice(&src[start..end]);
                }
                #[allow(clippy::cast_possible_truncation)]
                new_offsets.push(new_data.len() as u32);
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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
                let start = offsets.get(row).copied().unwrap_or_default() as usize;
                let end = offsets.get(row + 1).copied().unwrap_or_default() as usize;
                if start <= end && end <= src.len() {
                    new_data.extend_from_slice(&src[start..end]);
                }
                #[allow(clippy::cast_possible_truncation)]
                new_offsets.push(new_data.len() as u32);
                set_valid(&mut validity_bytes, out_idx, row, extra_nulls, validity);
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

fn empty_batch_with_schema(specs: &[ColumnSpec]) -> Result<Batch, String> {
    let mut columns = Vec::with_capacity(specs.len());
    for spec in specs {
        let data = match spec.vector_type {
            ColumnVectorType::Int8 => ColumnData::Int8(
                AlignedValues::from_vec(Vec::<i8>::new(), DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            ),
            ColumnVectorType::Int16 => ColumnData::Int16(
                AlignedValues::from_vec(Vec::<i16>::new(), DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            ),
            ColumnVectorType::Int32 => ColumnData::Int32(
                AlignedValues::from_vec(Vec::<i32>::new(), DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            ),
            ColumnVectorType::Int64 => ColumnData::Int64(
                AlignedValues::from_vec(Vec::<i64>::new(), DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            ),
            ColumnVectorType::Float32 => ColumnData::Float32(
                AlignedValues::from_vec(Vec::<f32>::new(), DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            ),
            ColumnVectorType::Float64 => ColumnData::Float64(
                AlignedValues::from_vec(Vec::<f64>::new(), DEFAULT_SIMD_ALIGNMENT_BYTES)
                    .map_err(|e| e.to_string())?,
            ),
            ColumnVectorType::Text => ColumnData::Text {
                offsets: std::sync::Arc::from(vec![0_u32]),
                data: std::sync::Arc::from(Vec::<u8>::new()),
            },
            ColumnVectorType::Binary => ColumnData::Binary {
                offsets: std::sync::Arc::from(vec![0_u32]),
                data: std::sync::Arc::from(Vec::<u8>::new()),
            },
        };
        let validity = NullBitmap::from_bytes(std::sync::Arc::from(Vec::<u8>::new()), 0)
            .map_err(|e| e.to_string())?;
        columns.push(Column {
            spec: spec.clone(),
            data,
            validity,
        });
    }
    let selection = SelectionVector::identity(0).map_err(|e: BatchFormatError| e.to_string())?;
    Batch::from_columns(columns, 0, 1, selection).map_err(|e: BatchFormatError| e.to_string())
}

/// Update SIMD utilization gauge.
fn update_join_simd_utilization(simd_path: &str) {
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
        DEFAULT_BATCH_ROW_CAPACITY, reset_vectorized_metrics, vectorized_metrics_snapshot,
    };
    use fsqlite_types::value::SqliteValue;

    const BEAD_HJ: &str = "bd-14vp7.4";

    fn build_batch(rows: &[(i64, i64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("id", ColumnVectorType::Int64),
            ColumnSpec::new("build_val", ColumnVectorType::Int64),
        ];
        let values: Vec<Vec<SqliteValue>> = rows
            .iter()
            .map(|&(id, v)| vec![SqliteValue::Integer(id), SqliteValue::Integer(v)])
            .collect();
        Batch::from_rows(&values, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn probe_batch(rows: &[(i64, i64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("id", ColumnVectorType::Int64),
            ColumnSpec::new("probe_val", ColumnVectorType::Int64),
        ];
        let values: Vec<Vec<SqliteValue>> = rows
            .iter()
            .map(|&(id, v)| vec![SqliteValue::Integer(id), SqliteValue::Integer(v)])
            .collect();
        Batch::from_rows(&values, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn build_batch_nullable(rows: &[(Option<i64>, i64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("id", ColumnVectorType::Int64),
            ColumnSpec::new("build_val", ColumnVectorType::Int64),
        ];
        let values: Vec<Vec<SqliteValue>> = rows
            .iter()
            .map(|&(id, v)| {
                vec![
                    id.map_or(SqliteValue::Null, SqliteValue::Integer),
                    SqliteValue::Integer(v),
                ]
            })
            .collect();
        Batch::from_rows(&values, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    fn probe_batch_nullable(rows: &[(Option<i64>, i64)]) -> Batch {
        let specs = vec![
            ColumnSpec::new("id", ColumnVectorType::Int64),
            ColumnSpec::new("probe_val", ColumnVectorType::Int64),
        ];
        let values: Vec<Vec<SqliteValue>> = rows
            .iter()
            .map(|&(id, v)| {
                vec![
                    id.map_or(SqliteValue::Null, SqliteValue::Integer),
                    SqliteValue::Integer(v),
                ]
            })
            .collect();
        Batch::from_rows(&values, &specs, DEFAULT_BATCH_ROW_CAPACITY).expect("batch")
    }

    // ── Inner join tests ─────────────────────────────────────────────────

    #[test]
    fn inner_join_basic() {
        let build = build_batch(&[(1, 100), (2, 200), (3, 300)]);
        let probe = probe_batch(&[(1, 10), (2, 20), (4, 40)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        // Only keys 1 and 2 match.
        assert_eq!(
            result.row_count(),
            2,
            "bead_id={BEAD_HJ} case=inner_join_basic"
        );

        // Check probe values present.
        let probe_val_col = &result.columns()[1];
        if let ColumnData::Int64(v) = &probe_val_col.data {
            let vals: Vec<i64> = result
                .selection()
                .as_slice()
                .iter()
                .map(|&i| v.as_slice()[usize::from(i)])
                .collect();
            assert!(vals.contains(&10));
            assert!(vals.contains(&20));
        }
    }

    #[test]
    fn inner_join_duplicate_keys() {
        let build = build_batch(&[(1, 100), (1, 101)]);
        let probe = probe_batch(&[(1, 10)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        // 1 probe row matches 2 build rows → 2 output rows.
        assert_eq!(
            result.row_count(),
            2,
            "bead_id={BEAD_HJ} case=inner_join_duplicate_keys"
        );
    }

    #[test]
    fn inner_join_no_matches() {
        let build = build_batch(&[(1, 100)]);
        let probe = probe_batch(&[(2, 20)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        assert_eq!(
            result.row_count(),
            0,
            "bead_id={BEAD_HJ} case=inner_join_no_matches"
        );
    }

    // ── Left outer join tests ────────────────────────────────────────────

    #[test]
    fn left_join_basic() {
        let build = build_batch(&[(1, 100), (2, 200)]);
        let probe = probe_batch(&[(1, 10), (3, 30)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Left).unwrap();

        // Both probe rows emitted; key 3 has NULL build columns.
        assert_eq!(
            result.row_count(),
            2,
            "bead_id={BEAD_HJ} case=left_join_basic"
        );

        // Build value column (index 2, after probe's 2 columns).
        let build_val_col = &result.columns()[2];
        let sel = result.selection().as_slice();

        // Row matching key=1 should be valid.
        // Row matching key=3 should be NULL.
        let mut found_valid = false;
        let mut found_null = false;
        for &i in sel {
            let idx = usize::from(i);
            if build_val_col.validity.is_valid(idx) {
                found_valid = true;
            } else {
                found_null = true;
            }
        }
        assert!(found_valid, "bead_id={BEAD_HJ} case=left_join_has_match");
        assert!(found_null, "bead_id={BEAD_HJ} case=left_join_has_null");
    }

    // ── Semi join tests ──────────────────────────────────────────────────

    #[test]
    fn semi_join_deduplicates() {
        let build = build_batch(&[(1, 100), (1, 101), (2, 200)]);
        let probe = probe_batch(&[(1, 10), (3, 30)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Semi).unwrap();

        // Only probe row with key=1 matches (once, despite 2 build rows).
        assert_eq!(
            result.row_count(),
            1,
            "bead_id={BEAD_HJ} case=semi_join_dedup"
        );
    }

    // ── Anti join tests ──────────────────────────────────────────────────

    #[test]
    fn anti_join_returns_non_matching() {
        let build = build_batch(&[(1, 100), (2, 200)]);
        let probe = probe_batch(&[(1, 10), (3, 30), (4, 40)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Anti).unwrap();

        // Probe rows 3 and 4 have no build match.
        assert_eq!(
            result.row_count(),
            2,
            "bead_id={BEAD_HJ} case=anti_join_non_matching"
        );
    }

    // ── NULL key handling ────────────────────────────────────────────────

    #[test]
    fn null_keys_never_match_inner() {
        let build = build_batch_nullable(&[(Some(1), 100), (None, 200)]);
        let probe = probe_batch_nullable(&[(Some(1), 10), (None, 20)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        // Only key=1 matches; NULL≠NULL.
        assert_eq!(
            result.row_count(),
            1,
            "bead_id={BEAD_HJ} case=null_key_no_match"
        );
    }

    #[test]
    fn null_keys_left_join_produce_null_build() {
        let build = build_batch_nullable(&[(Some(1), 100)]);
        let probe = probe_batch_nullable(&[(None, 10)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Left).unwrap();

        // Probe row with NULL key emitted with NULL build columns.
        assert_eq!(
            result.row_count(),
            1,
            "bead_id={BEAD_HJ} case=null_key_left"
        );
        let build_val_col = &result.columns()[2];
        assert!(
            !build_val_col.validity.is_valid(0),
            "bead_id={BEAD_HJ} case=null_key_left_null_build"
        );
    }

    // ── Empty inputs ─────────────────────────────────────────────────────

    #[test]
    fn empty_build_inner_join() {
        let build = build_batch(&[]);
        let probe = probe_batch(&[(1, 10)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        assert_eq!(result.row_count(), 0, "bead_id={BEAD_HJ} case=empty_build");
    }

    #[test]
    fn empty_probe() {
        let build = build_batch(&[(1, 100)]);
        let probe = probe_batch(&[]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        assert_eq!(result.row_count(), 0, "bead_id={BEAD_HJ} case=empty_probe");
    }

    // ── Metrics ──────────────────────────────────────────────────────────

    #[test]
    fn join_records_metrics() {
        reset_vectorized_metrics();
        let build = build_batch(&[(1, 100)]);
        let probe = probe_batch(&[(1, 10)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let _ = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        let metrics = vectorized_metrics_snapshot();
        assert!(
            metrics.vectorized_rows_total >= 1,
            "bead_id={BEAD_HJ} case=join_metrics"
        );
    }

    // ── Column out of bounds ─────────────────────────────────────────────

    #[test]
    fn build_key_out_of_bounds() {
        let build = build_batch(&[(1, 100)]);
        let err = hash_join_build(build, &[5]).unwrap_err();
        assert!(
            err.contains("out of bounds"),
            "bead_id={BEAD_HJ} case=build_key_oob"
        );
    }

    // ── Multi-column key join ────────────────────────────────────────────

    #[test]
    fn multi_column_key_join() {
        let build_specs = vec![
            ColumnSpec::new("a", ColumnVectorType::Int64),
            ColumnSpec::new("b", ColumnVectorType::Int64),
            ColumnSpec::new("build_val", ColumnVectorType::Int64),
        ];
        let build_rows: Vec<Vec<SqliteValue>> = vec![
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(10),
                SqliteValue::Integer(100),
            ],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(20),
                SqliteValue::Integer(200),
            ],
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Integer(10),
                SqliteValue::Integer(300),
            ],
        ];
        let build =
            Batch::from_rows(&build_rows, &build_specs, DEFAULT_BATCH_ROW_CAPACITY).unwrap();

        let probe_specs = vec![
            ColumnSpec::new("a", ColumnVectorType::Int64),
            ColumnSpec::new("b", ColumnVectorType::Int64),
            ColumnSpec::new("probe_val", ColumnVectorType::Int64),
        ];
        let probe_rows: Vec<Vec<SqliteValue>> = vec![
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(10),
                SqliteValue::Integer(1000),
            ],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(30),
                SqliteValue::Integer(2000),
            ],
        ];
        let probe =
            Batch::from_rows(&probe_rows, &probe_specs, DEFAULT_BATCH_ROW_CAPACITY).unwrap();

        let table = hash_join_build(build, &[0, 1]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0, 1], JoinType::Inner).unwrap();

        // Only (1, 10) matches.
        assert_eq!(
            result.row_count(),
            1,
            "bead_id={BEAD_HJ} case=multi_column_key"
        );
    }

    // ── Output schema correctness ────────────────────────────────────────

    #[test]
    fn output_schema_probe_then_build_non_key() {
        let build = build_batch(&[(1, 100)]);
        let probe = probe_batch(&[(1, 10)]);

        let table = hash_join_build(build, &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();

        // Schema: probe_id, probe_val, build_val (build_id excluded as key).
        assert_eq!(result.columns().len(), 3);
        assert_eq!(result.columns()[0].spec.name, "id");
        assert_eq!(result.columns()[1].spec.name, "probe_val");
        assert_eq!(result.columns()[2].spec.name, "build_val");
    }
}
