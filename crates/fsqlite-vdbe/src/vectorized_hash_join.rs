//! Vectorized hash-join operator (bd-14vp7.4).
//!
//! Two-phase equi-join: build a hash table from the build side, then probe
//! with the probe side to produce output batches.  Supports inner, left outer,
//! semi, and anti join variants.

use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use crate::vectorized::{
    AlignedValues, Batch, BatchFormatError, Column, ColumnData, ColumnSpec, ColumnVectorType,
    DEFAULT_SIMD_ALIGNMENT_BYTES, NullBitmap, SelectionVector, record_vectorized_rows,
    set_vectorized_simd_utilization, simd_path_label,
};

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

/// Canonical binary-equality key, shared by hashing and collision checks.
/// INTEGER and exactly integral REAL values share one representation; no
/// integer is rounded to REAL. NaNs are NULL and never participate in a join.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyValue {
    Null,
    Integer(i64),
    FloatBits(u64),
    Text(Vec<u8>),
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
/// Keys use SQLite numeric equality and byte-exact text/blob equality. The
/// caller must apply comparison affinities or nonbinary collations beforehand.
///
/// # Errors
///
/// Returns an error if key column indices are out of bounds.
pub fn hash_join_build(
    build_batch: Batch,
    build_key_columns: &[usize],
) -> Result<HashJoinTable, String> {
    if build_key_columns.is_empty() {
        return Err("hash join build requires at least one key column".to_owned());
    }

    validate_key_columns(&build_batch, build_key_columns)?;
    let sel = build_batch.selection();

    let mut rows = Vec::with_capacity(sel.len());
    let mut head: HashMap<u64, usize> = HashMap::new();
    let mut next: Vec<Option<usize>> = Vec::with_capacity(sel.len());

    for &row_idx in sel.as_slice() {
        let row = usize::from(row_idx);
        let key = extract_key(&build_batch, build_key_columns, row)?;

        // Skip NULL keys (SQL join semantics: NULL != NULL).
        if key.iter().any(|k| matches!(k, KeyValue::Null)) {
            continue;
        }

        let hash = hash_key(&key);
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

    // Convert prepend chains to build-selection order once. Streaming probes
    // can now resume directly inside a bucket without collecting and reversing
    // every matching row (or allocating a Cartesian product).
    for first in head.values_mut() {
        let mut previous = *first;
        let mut current = next[previous].take();
        while let Some(index) = current {
            current = next[index];
            next[index] = Some(previous);
            previous = index;
        }
        *first = previous;
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
/// Returns an error for invalid keys or more than 65,536 output rows. Use
/// [`HashJoinProbe`] to consume larger joins incrementally. The size check
/// happens before gathering output payloads, not after constructing them.
pub fn hash_join_probe(
    table: &HashJoinTable,
    probe_batch: &Batch,
    probe_key_columns: &[usize],
    join_type: JoinType,
) -> Result<Batch, String> {
    let probe = HashJoinProbe::new(
        table, probe_batch, probe_key_columns, join_type, MAX_JOIN_BATCH_ROWS,
    )?;
    let mut position = ProbePosition::default();
    let pairs = probe.collect_pairs(&mut position)?;
    if probe.next_pair(&mut position)?.is_some() {
        return Err("hash join result exceeds one batch; use HashJoinProbe".to_owned());
    }
    let result = materialize_join_pairs(table, probe_batch, &pairs)?;
    record_probe_batch(position.selection_offset, result.row_count(), join_type);
    Ok(result)
}

const MAX_JOIN_BATCH_ROWS: usize = u16::MAX as usize + 1;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveProbe {
    row: usize,
    key: Vec<KeyValue>,
    next_build: Option<usize>,
    matched: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ProbePosition {
    selection_offset: usize,
    active: Option<ActiveProbe>,
}

#[derive(Default)]
struct JoinPairs {
    probe: Vec<usize>,
    build: Vec<Option<usize>>,
}

/// Resumable hash-join output, including joins larger than a selection vector.
///
/// Each call emits at most the configured row limit and resumes inside the
/// current collision chain. Probe selection order (including repeated entries)
/// and build selection order are preserved. Semi/anti joins stop searching at
/// the first match. Output schema matches [`hash_join_probe`] for every variant.
///
/// This bounds result-pair staging and output row counts, not input tables,
/// key sizes, variable-width payload bytes or total process memory. Consume
/// and release batches rather than collecting them all to retain that benefit.
/// No input or table is mutated. Dropping the probe abandons only its progress.
#[derive(Debug)]
pub struct HashJoinProbe<'a> {
    table: &'a HashJoinTable,
    probe_batch: &'a Batch,
    key_columns: Vec<usize>,
    join_type: JoinType,
    max_output_rows: usize,
    position: ProbePosition,
}

impl<'a> HashJoinProbe<'a> {
    /// Start a probe with an output row limit in `1..=65_536`.
    ///
    /// # Errors
    /// Rejects invalid key indices, differing key arities, or an invalid limit,
    /// including for empty inputs. No output payload is gathered at admission.
    pub fn new(
        table: &'a HashJoinTable,
        probe_batch: &'a Batch,
        key_columns: &[usize],
        join_type: JoinType,
        max_output_rows: usize,
    ) -> Result<Self, String> {
        if key_columns.is_empty() {
            return Err("hash join probe requires at least one key column".to_owned());
        }
        if table.build_key_columns.len() != key_columns.len() {
            return Err(format!(
                "hash join key arity mismatch: build has {} key columns, probe has {}",
                table.build_key_columns.len(), key_columns.len(),
            ));
        }
        validate_key_columns(probe_batch, key_columns)?;
        if !(1..=MAX_JOIN_BATCH_ROWS).contains(&max_output_rows) {
            return Err("hash join output row limit must be in 1..=65536".to_owned());
        }
        Ok(Self {
            table,
            probe_batch,
            key_columns: key_columns.to_vec(),
            join_type,
            max_output_rows,
            position: ProbePosition::default(),
        })
    }

    /// Produce the next nonempty batch, or `None` once exhausted.
    ///
    /// # Errors
    /// A key, allocation or output-materialization error leaves progress
    /// unchanged: the failed batch has not been consumed. Already returned
    /// batches remain consumed. The caller may retry or abandon the probe.
    pub fn next_batch(&mut self) -> Result<Option<Batch>, String> {
        let mut position = self.position.clone();
        let pairs = self.collect_pairs(&mut position)?;
        let result = if pairs.probe.is_empty() {
            None
        } else {
            Some(materialize_join_pairs(self.table, self.probe_batch, &pairs)?)
        };
        let consumed = position.selection_offset - self.position.selection_offset;
        self.position = position;
        record_probe_batch(consumed, pairs.probe.len(), self.join_type);
        Ok(result)
    }

    fn collect_pairs(&self, position: &mut ProbePosition) -> Result<JoinPairs, String> {
        let mut pairs = JoinPairs::default();
        while pairs.probe.len() < self.max_output_rows {
            let Some((probe, build)) = self.next_pair(position)? else {
                break;
            };
            pairs.probe.try_reserve(1).map_err(|error| error.to_string())?;
            pairs.build.try_reserve(1).map_err(|error| error.to_string())?;
            pairs.probe.push(probe);
            pairs.build.push(build);
        }
        Ok(pairs)
    }

    fn next_pair(&self, position: &mut ProbePosition) -> Result<Option<(usize, Option<usize>)>, String> {
        loop {
            let mut active = if let Some(active) = position.active.take() {
                active
            } else {
                let Some(&row) = self.probe_batch.selection().as_slice().get(position.selection_offset) else {
                    return Ok(None);
                };
                let row = usize::from(row);
                let key = extract_key(self.probe_batch, &self.key_columns, row)?;
                let next_build = if key.iter().any(|value| matches!(value, KeyValue::Null)) {
                    None
                } else {
                    self.table.head.get(&hash_key(&key)).copied()
                };
                position.selection_offset += 1;
                ActiveProbe { row, key, next_build, matched: false }
            };
            while let Some(index) = active.next_build {
                active.next_build = self.table.next[index];
                let build = &self.table.rows[index];
                if build.key != active.key {
                    continue; // A hash collision is not equality.
                }
                active.matched = true;
                match self.join_type {
                    JoinType::Inner | JoinType::Left => {
                        let pair = (active.row, Some(build.row_index));
                        position.active = Some(active);
                        return Ok(Some(pair));
                    }
                    JoinType::Semi => return Ok(Some((active.row, Some(build.row_index)))),
                    JoinType::Anti => break,
                }
            }
            if !active.matched && matches!(self.join_type, JoinType::Left | JoinType::Anti) {
                return Ok(Some((active.row, None)));
            }
        }
    }
}

fn materialize_join_pairs(
    table: &HashJoinTable,
    probe_batch: &Batch,
    pairs: &JoinPairs,
) -> Result<Batch, String> {
    let output_rows = pairs.probe.len();

    // Build output columns: probe columns + non-key build columns.
    let mut output_columns: Vec<Column> = Vec::new();

    // Probe columns (gathered by probe_indices).
    for probe_col in probe_batch.columns() {
        let col = gather_column(
            &probe_col.spec,
            &probe_col.data,
            &probe_col.validity,
            &pairs.probe,
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
            &pairs.build,
            output_rows,
        )?;
        output_columns.push(col);
    }

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

fn validate_key_columns(batch: &Batch, columns: &[usize]) -> Result<(), String> {
    for &column in columns {
        if column >= batch.columns().len() {
            return Err(format!("join key column index {column} out of bounds"));
        }
    }
    Ok(())
}

fn hash_key(key: &[KeyValue]) -> u64 {
    // Hash the same canonical representation that equality compares. Hashing
    // physical column bytes separately would separate INTEGER 1 from REAL 1.0
    // (and different integer widths) before collision checks can see them.
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[allow(clippy::cast_possible_truncation)] // integral, in-range REAL only
fn float_key(value: f64) -> KeyValue {
    if value.is_nan() {
        return KeyValue::Null;
    }
    // The upper bound is exclusive: i64::MAX rounds UP to 2^63 as f64.
    // Casting all integers to f64 instead would alias neighbors above 2^53.
    if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
        && value.fract() == 0.0
    {
        KeyValue::Integer(value as i64)
    } else {
        KeyValue::FloatBits(value.to_bits())
    }
}

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
            ColumnData::Float32(v) => float_key(f64::from(v.as_slice()[row])),
            ColumnData::Float64(v) => float_key(v.as_slice()[row]),
            ColumnData::Text { offsets, data } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let bytes = data.get(start..end)
                    .ok_or_else(|| "invalid text join key offsets".to_owned())?;
                KeyValue::Text(bytes.to_vec())
            }
            ColumnData::Binary { offsets, data } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let bytes = data.get(start..end)
                    .ok_or_else(|| "invalid binary join key offsets".to_owned())?;
                KeyValue::Blob(bytes.to_vec())
            }
        };
        key.push(val);
    }
    Ok(key)
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
                if !extra_nulls[out_idx] && validity.is_valid(row) {
                    append_variable_value(&mut new_data, offsets, src, row)?;
                }
                new_offsets.push(u32::try_from(new_data.len())
                    .map_err(|_| "join text output exceeds 4 GiB".to_owned())?);
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
                if !extra_nulls[out_idx] && validity.is_valid(row) {
                    append_variable_value(&mut new_data, offsets, src, row)?;
                }
                new_offsets.push(u32::try_from(new_data.len())
                    .map_err(|_| "join binary output exceeds 4 GiB".to_owned())?);
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

fn append_variable_value(
    output: &mut Vec<u8>,
    offsets: &[u32],
    source: &[u8],
    row: usize,
) -> Result<(), String> {
    let start = offsets.get(row).ok_or_else(|| "missing join payload offset".to_owned())?;
    let end = offsets.get(row + 1).ok_or_else(|| "missing join payload end".to_owned())?;
    let bytes = source.get(*start as usize..*end as usize)
        .ok_or_else(|| "invalid join payload offsets".to_owned())?;
    output.len().checked_add(bytes.len()).and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| "join variable-width output exceeds 4 GiB".to_owned())?;
    output.try_reserve(bytes.len()).map_err(|error| error.to_string())?;
    output.extend_from_slice(bytes);
    Ok(())
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

fn record_probe_batch(input_rows: usize, output_rows: usize, join_type: JoinType) {
    let simd_path = simd_path_label();
    record_vectorized_rows(u64::try_from(input_rows).unwrap_or(u64::MAX));
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

    #[test]
    fn build_key_empty_rejected() {
        let build = build_batch(&[(1, 100)]);
        let err = hash_join_build(build, &[]).unwrap_err();
        assert!(
            err.contains("at least one key"),
            "bead_id={BEAD_HJ} case=build_key_empty"
        );
    }

    #[test]
    fn probe_key_empty_rejected() {
        let build = build_batch(&[(1, 100)]);
        let probe = probe_batch(&[(1, 10)]);
        let table = hash_join_build(build, &[0]).unwrap();
        let err = hash_join_probe(&table, &probe, &[], JoinType::Inner).unwrap_err();
        assert!(
            err.contains("at least one key"),
            "bead_id={BEAD_HJ} case=probe_key_empty"
        );
    }

    #[test]
    fn key_arity_mismatch_rejected() {
        let build_specs = vec![
            ColumnSpec::new("a", ColumnVectorType::Int64),
            ColumnSpec::new("b", ColumnVectorType::Int64),
            ColumnSpec::new("build_val", ColumnVectorType::Int64),
        ];
        let build_rows: Vec<Vec<SqliteValue>> = vec![vec![
            SqliteValue::Integer(1),
            SqliteValue::Integer(10),
            SqliteValue::Integer(100),
        ]];
        let build =
            Batch::from_rows(&build_rows, &build_specs, DEFAULT_BATCH_ROW_CAPACITY).unwrap();

        let probe_specs = vec![
            ColumnSpec::new("a", ColumnVectorType::Int64),
            ColumnSpec::new("probe_val", ColumnVectorType::Int64),
        ];
        let probe_rows: Vec<Vec<SqliteValue>> =
            vec![vec![SqliteValue::Integer(1), SqliteValue::Integer(1000)]];
        let probe =
            Batch::from_rows(&probe_rows, &probe_specs, DEFAULT_BATCH_ROW_CAPACITY).unwrap();

        let table = hash_join_build(build, &[0, 1]).unwrap();
        let err = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap_err();
        assert!(
            err.contains("arity mismatch"),
            "bead_id={BEAD_HJ} case=key_arity_mismatch"
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

    fn numeric_batch(values: &[SqliteValue], kind: ColumnVectorType) -> Batch {
        let specs = [
            ColumnSpec::new("key", kind),
            ColumnSpec::new("ordinal", ColumnVectorType::Int64),
        ];
        let rows: Vec<_> = values.iter().enumerate().map(|(i, value)| {
            vec![value.clone(), SqliteValue::Integer(i64::try_from(i).unwrap())]
        }).collect();
        Batch::from_rows(&rows, &specs, rows.len().max(1)).unwrap()
    }

    fn joined_ordinals(batch: &Batch) -> Vec<(i64, Option<i64>)> {
        let ColumnData::Int64(probe) = &batch.columns()[1].data else {
            panic!("probe ordinal must be integer");
        };
        let build_column = &batch.columns()[2];
        let ColumnData::Int64(build) = &build_column.data else {
            panic!("build ordinal must be integer");
        };
        batch.selection().as_slice().iter().map(|&row| {
            let row = usize::from(row);
            (probe.as_slice()[row], build_column.validity.is_valid(row)
                .then(|| build.as_slice()[row]))
        }).collect()
    }

    #[test]
    fn numeric_keys_match_across_every_physical_width() {
        let kinds = [ColumnVectorType::Int8, ColumnVectorType::Int16,
            ColumnVectorType::Int32, ColumnVectorType::Int64,
            ColumnVectorType::Float32, ColumnVectorType::Float64];
        let values: Vec<_> = [-7, 0, 1, 42].into_iter().map(SqliteValue::Integer).collect();
        for build_kind in kinds {
            for probe_kind in kinds {
                let table = hash_join_build(numeric_batch(&values, build_kind), &[0]).unwrap();
                let probe = numeric_batch(&values, probe_kind);
                let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();
                assert_eq!(joined_ordinals(&result),
                    vec![(0, Some(0)), (1, Some(1)), (2, Some(2)), (3, Some(3))],
                    "{build_kind:?} -> {probe_kind:?}");
            }
        }
    }

    #[test]
    fn numeric_join_does_not_round_large_integer_keys() {
        let integers = [i64::MIN, -1, 0, 1, 9_007_199_254_740_992,
            9_007_199_254_740_993, i64::MAX];
        let reals = [-9_223_372_036_854_775_808.0, -1.0, -0.0, 1.0,
            9_007_199_254_740_992.0, 9_223_372_036_854_775_808.0, 1.5,
            f64::INFINITY, f64::NEG_INFINITY];
        let build = numeric_batch(&integers.map(SqliteValue::Integer), ColumnVectorType::Int64);
        let probe = numeric_batch(&reals.map(SqliteValue::Float), ColumnVectorType::Float64);
        let table = hash_join_build(build.clone(), &[0]).unwrap();
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();
        assert_eq!(joined_ordinals(&result),
            vec![(0, Some(0)), (1, Some(1)), (2, Some(2)), (3, Some(3)), (4, Some(4))]);
        let reversed = hash_join_build(probe, &[0]).unwrap();
        let result = hash_join_probe(&reversed, &build, &[0], JoinType::Inner).unwrap();
        assert_eq!(joined_ordinals(&result),
            vec![(0, Some(0)), (1, Some(1)), (2, Some(2)), (3, Some(3)), (4, Some(4))]);
    }

    #[test]
    fn nan_keys_are_nonmatching_even_with_validity_bits_set() {
        // Bypass row conversion so the operator itself must normalize NaN.
        let mut columns = numeric_batch(
            &[SqliteValue::Integer(0), SqliteValue::Integer(1)],
            ColumnVectorType::Float64,
        ).columns().to_vec();
        columns[0].data = ColumnData::Float64(AlignedValues::from_vec(
            vec![f64::NAN, 1.0], DEFAULT_SIMD_ALIGNMENT_BYTES,
        ).unwrap());
        let batch = Batch::from_columns(columns, 2, 2, SelectionVector::identity(2).unwrap()).unwrap();
        assert!(batch.columns()[0].validity.is_valid(0));
        let table = hash_join_build(batch.clone(), &[0]).unwrap();
        for (kind, expected) in [
            (JoinType::Inner, vec![(1, Some(1))]),
            (JoinType::Semi, vec![(1, Some(1))]),
            (JoinType::Left, vec![(0, None), (1, Some(1))]),
            (JoinType::Anti, vec![(0, None)]),
        ] {
            let result = hash_join_probe(&table, &batch, &[0], kind).unwrap();
            assert_eq!(joined_ordinals(&result), expected, "{kind:?}");
        }
    }

    #[test]
    fn canonical_float_hashes_preserve_zero_infinities_and_fractional_values() {
        for real in [0.0, -0.0] {
            assert_eq!(float_key(real), KeyValue::Integer(0));
            assert_eq!(hash_key(&[float_key(real)]), hash_key(&[KeyValue::Integer(0)]));
        }
        for real in [0.5, -0.5, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(float_key(real), KeyValue::FloatBits(real.to_bits()));
        }
        assert_eq!(float_key(f64::from_bits(0x7FF0_0000_0000_0001)), KeyValue::Null);
        assert_eq!(float_key(f64::from_bits(0xFFF8_0000_0000_0001)), KeyValue::Null);
        let reals = [SqliteValue::Float(f64::INFINITY), SqliteValue::Float(-0.0),
            SqliteValue::Float(0.5), SqliteValue::Float(f64::NEG_INFINITY)];
        let batch = numeric_batch(&reals, ColumnVectorType::Float64);
        let table = hash_join_build(batch.clone(), &[0]).unwrap();
        assert_eq!(joined_ordinals(&hash_join_probe(&table, &batch, &[0], JoinType::Inner).unwrap()),
            vec![(0, Some(0)), (1, Some(1)), (2, Some(2)), (3, Some(3))]);
    }

    #[test]
    fn composite_numeric_keys_respect_selection_order_and_column_mapping() {
        let build = Batch::from_rows(&[
            vec![SqliteValue::Integer(1), SqliteValue::Integer(2), SqliteValue::Integer(10)],
            vec![SqliteValue::Integer(1), SqliteValue::Integer(3), SqliteValue::Integer(11)],
        ], &[
            ColumnSpec::new("a", ColumnVectorType::Int16),
            ColumnSpec::new("b", ColumnVectorType::Float32),
            ColumnSpec::new("payload", ColumnVectorType::Int64),
        ], 2).unwrap();
        let mut probe = Batch::from_rows(&[
            vec![SqliteValue::Integer(3), SqliteValue::Float(1.0)],
            vec![SqliteValue::Integer(2), SqliteValue::Float(1.0)],
        ], &[
            ColumnSpec::new("b", ColumnVectorType::Int64),
            ColumnSpec::new("a", ColumnVectorType::Float64),
        ], 2).unwrap();
        probe.apply_selection(SelectionVector::from_indices(vec![1, 0, 1])).unwrap();
        let table = hash_join_build(build, &[0, 1]).unwrap();
        let result = hash_join_probe(&table, &probe, &[1, 0], JoinType::Inner).unwrap();
        let ColumnData::Int64(payload) = &result.columns()[2].data else { panic!("payload"); };
        assert_eq!(payload.as_slice(), &[10, 11, 10]);
    }

    #[test]
    fn binary_keys_preserve_raw_bytes_and_storage_class() {
        assert_ne!(KeyValue::Text(vec![0x80]), KeyValue::Text(vec![0x81]));
        assert_ne!(KeyValue::Text(b"1".to_vec()), KeyValue::Integer(1));
        assert_ne!(KeyValue::Text(b"1".to_vec()), KeyValue::Blob(b"1".to_vec()));
        let batch = |bytes: Vec<u8>| {
            Batch::from_columns(vec![Column {
                spec: ColumnSpec::new("key", ColumnVectorType::Text),
                data: ColumnData::Text {
                    offsets: std::sync::Arc::from(vec![0, u32::try_from(bytes.len()).unwrap()]),
                    data: std::sync::Arc::from(bytes),
                },
                validity: NullBitmap::from_bytes(std::sync::Arc::from(vec![1_u8]), 1).unwrap(),
            }], 1, 1, SelectionVector::identity(1).unwrap()).unwrap()
        };
        for bytes in [vec![0x80], b"a\0b".to_vec()] {
            let table = hash_join_build(batch(bytes.clone()), &[0]).unwrap();
            assert_eq!(hash_join_probe(&table, &batch(bytes), &[0], JoinType::Inner).unwrap().row_count(), 1);
            assert_eq!(hash_join_probe(&table, &batch(vec![0x81]), &[0], JoinType::Inner).unwrap().row_count(), 0);
        }
    }

    #[test]
    fn invalid_key_indices_are_rejected_on_empty_batches() {
        assert!(hash_join_build(build_batch(&[]), &[2]).unwrap_err().contains("out of bounds"));
        let table = hash_join_build(build_batch(&[]), &[0]).unwrap();
        assert!(hash_join_probe(&table, &probe_batch(&[]), &[2], JoinType::Inner)
            .unwrap_err().contains("out of bounds"));
    }

    #[test]
    fn streaming_join_crosses_selection_vector_limit_without_losing_pairs() {
        let build_rows: Vec<_> = (0..300).map(|row| (1, row)).collect();
        let probe_rows: Vec<_> = (0..300).map(|row| (1, row)).collect();
        let table = hash_join_build(build_batch(&build_rows), &[0]).unwrap();
        let probe = probe_batch(&probe_rows);
        let mut stream = HashJoinProbe::new(&table, &probe, &[0], JoinType::Inner, 257).unwrap();
        let mut count = 0_i64;
        while let Some(batch) = stream.next_batch().unwrap() {
            assert!((1..=257).contains(&batch.row_count()));
            for (probe_row, build_row) in joined_ordinals(&batch) {
                assert_eq!((probe_row, build_row), (count / 300, Some(count % 300)));
                count += 1;
            }
        }
        assert_eq!(count, 90_000);
        assert!(stream.next_batch().unwrap().is_none());
        assert!(hash_join_probe(&table, &probe, &[0], JoinType::Inner)
            .unwrap_err().contains("use HashJoinProbe"));
    }

    #[test]
    fn single_batch_join_accepts_exactly_all_u16_indices() {
        let rows: Vec<_> = (0..256).map(|row| (1, row)).collect();
        let table = hash_join_build(build_batch(&rows), &[0]).unwrap();
        let result = hash_join_probe(&table, &probe_batch(&rows), &[0], JoinType::Inner).unwrap();
        assert_eq!(result.row_count(), 65_536);
        assert_eq!(result.selection().as_slice().last(), Some(&u16::MAX));
        let pairs = joined_ordinals(&result);
        assert_eq!(pairs.first(), Some(&(0, Some(0))));
        assert_eq!(pairs.last(), Some(&(255, Some(255))));
    }

    #[test]
    fn streaming_variants_preserve_selected_duplicates_order_and_null_padding() {
        let mut build = build_batch_nullable(&[
            (Some(1), 100), (None, 200), (Some(1), 101), (Some(2), 102),
        ]);
        build.apply_selection(SelectionVector::from_indices(vec![2, 0, 2, 1, 3])).unwrap();
        let mut probe = probe_batch_nullable(&[
            (Some(1), 10), (None, 20), (Some(3), 30), (Some(2), 40),
        ]);
        probe.apply_selection(SelectionVector::from_indices(vec![3, 0, 1, 0, 2])).unwrap();
        let table = hash_join_build(build, &[0]).unwrap();
        for (kind, expected) in [
            (JoinType::Inner, vec![(40, Some(102)), (10, Some(101)), (10, Some(100)),
                (10, Some(101)), (10, Some(101)), (10, Some(100)), (10, Some(101))]),
            (JoinType::Left, vec![(40, Some(102)), (10, Some(101)), (10, Some(100)),
                (10, Some(101)), (20, None), (10, Some(101)), (10, Some(100)),
                (10, Some(101)), (30, None)]),
            (JoinType::Semi, vec![(40, Some(102)), (10, Some(101)), (10, Some(101))]),
            (JoinType::Anti, vec![(20, None), (30, None)]),
        ] {
            for limit in [1, 2, 3, 7, 100] {
                let mut stream = HashJoinProbe::new(&table, &probe, &[0], kind, limit).unwrap();
                let mut actual = Vec::new();
                while let Some(batch) = stream.next_batch().unwrap() {
                    assert!(batch.row_count() <= limit);
                    assert_eq!(batch.columns().len(), 3);
                    actual.extend(joined_ordinals(&batch));
                }
                assert_eq!(actual, expected, "{kind:?} limit={limit}");
                assert!(stream.next_batch().unwrap().is_none());
            }
        }
    }

    #[test]
    fn streaming_probe_checks_equality_after_forced_hash_collisions() {
        let mut table = hash_join_build(build_batch(&[(2, 200), (1, 100), (3, 300), (1, 101)]), &[0]).unwrap();
        // Deliberately place every key in one chain without needing a hash
        // preimage. Matches at both ends of a batch still require exact keys.
        table.next = vec![Some(1), Some(2), Some(3), None];
        for key in [1, 2, 3, 4] {
            table.head.insert(hash_key(&[KeyValue::Integer(key)]), 0);
        }
        let probe = probe_batch(&[(1, 10), (4, 40), (2, 20)]);
        let mut stream = HashJoinProbe::new(&table, &probe, &[0], JoinType::Left, 1).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = stream.next_batch().unwrap() {
            actual.extend(joined_ordinals(&batch));
        }
        assert_eq!(actual, vec![(10, Some(100)), (10, Some(101)), (40, None), (20, Some(200))]);
    }

    #[test]
    fn streaming_materialization_error_does_not_consume_the_failed_chunk() {
        let mut columns = build_batch(&[(1, 100), (2, 200)]).columns().to_vec();
        columns[1].spec = ColumnSpec::new("payload", ColumnVectorType::Text);
        columns[1].data = ColumnData::Text {
            offsets: std::sync::Arc::from(vec![0, 1, 3]),
            data: std::sync::Arc::from(vec![b'x']), // Second value is truncated.
        };
        let build = Batch::from_columns(columns, 2, 2, SelectionVector::identity(2).unwrap()).unwrap();
        let table = hash_join_build(build, &[0]).unwrap();
        let probe = probe_batch(&[(1, 10), (2, 20)]);
        let mut stream = HashJoinProbe::new(&table, &probe, &[0], JoinType::Inner, 1).unwrap();
        assert_eq!(stream.next_batch().unwrap().unwrap().row_count(), 1);
        let before = stream.position.clone();
        for _ in 0..2 {
            assert!(stream.next_batch().unwrap_err().contains("invalid join payload offsets"));
            assert_eq!(stream.position, before, "a rejected chunk must remain retryable");
        }
        // A larger initial chunk must also reject atomically, even though its
        // first row could have been emitted successfully on its own.
        let mut stream = HashJoinProbe::new(&table, &probe, &[0], JoinType::Inner, 2).unwrap();
        assert!(stream.next_batch().is_err());
        assert_eq!(stream.position, ProbePosition::default());
    }

    #[test]
    fn streaming_limits_and_empty_inputs_are_well_defined() {
        let table = hash_join_build(build_batch(&[]), &[0]).unwrap();
        let empty = probe_batch(&[]);
        for limit in [0, 65_537, usize::MAX] {
            assert!(HashJoinProbe::new(&table, &empty, &[0], JoinType::Inner, limit).is_err());
        }
        let mut stream = HashJoinProbe::new(&table, &empty, &[0], JoinType::Inner, 1).unwrap();
        assert!(stream.next_batch().unwrap().is_none());
        assert!(stream.next_batch().unwrap().is_none());
        let empty_result = hash_join_probe(&table, &empty, &[0], JoinType::Inner).unwrap();
        assert_eq!(empty_result.row_count(), 0);
        assert_eq!(empty_result.columns().len(), 3);
        let probe = probe_batch(&[(1, 10), (2, 20)]);
        for kind in [JoinType::Left, JoinType::Anti] {
            let mut stream = HashJoinProbe::new(&table, &probe, &[0], kind, 1).unwrap();
            assert_eq!(joined_ordinals(&stream.next_batch().unwrap().unwrap()), vec![(10, None)]);
            assert_eq!(joined_ordinals(&stream.next_batch().unwrap().unwrap()), vec![(20, None)]);
            assert!(stream.next_batch().unwrap().is_none());
        }
    }

    #[test]
    fn abandoned_probe_does_not_mutate_reusable_build_table() {
        let table = hash_join_build(build_batch(&[(1, 100), (1, 101)]), &[0]).unwrap();
        let probe = probe_batch(&[(1, 10)]);
        {
            let mut stream = HashJoinProbe::new(&table, &probe, &[0], JoinType::Inner, 1).unwrap();
            assert_eq!(joined_ordinals(&stream.next_batch().unwrap().unwrap()), vec![(10, Some(100))]);
        }
        let result = hash_join_probe(&table, &probe, &[0], JoinType::Inner).unwrap();
        assert_eq!(joined_ordinals(&result), vec![(10, Some(100)), (10, Some(101))]);
    }
}
