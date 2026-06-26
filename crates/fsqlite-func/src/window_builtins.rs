//! Built-in window functions (S13.5, bd-14i6).
//!
//! Implements: row_number, rank, dense_rank, percent_rank, cume_dist,
//! ntile, lag, lead, first_value, last_value, nth_value, plus
//! aggregate-as-window variants including group_concat and string_agg.
//!
//! These functions implement the [`WindowFunction`] trait.  The VDBE is
//! responsible for partitioning, ordering, and frame management; these
//! implementations provide the per-row computation logic.
//!
//! # Design Notes
//!
//! Pure-numbering functions (row_number, rank, dense_rank) track position
//! via step() and expose the current value through value().  The ORDER BY
//! column is passed as args\[0\] so the function can detect peer-group
//! boundaries.
//!
//! Buffer-based functions (lag, lead, first_value, last_value, nth_value)
//! maintain an internal VecDeque of values and expose frame-relative
//! access through value().
#![allow(
    clippy::unnecessary_literal_bound,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::float_cmp,
    clippy::match_same_arms,
    clippy::similar_names
)]

use std::collections::VecDeque;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;

use crate::{FunctionRegistry, WindowFunction};

// ═══════════════════════════════════════════════════════════════════════════
// row_number()
// ═══════════════════════════════════════════════════════════════════════════

pub struct RowNumberState {
    counter: i64,
}

pub struct RowNumberFunc;

impl WindowFunction for RowNumberFunc {
    type State = RowNumberState;

    fn initial_state(&self) -> Self::State {
        RowNumberState { counter: 0 }
    }

    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.counter += 1;
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.counter -= 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.counter))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.counter))
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn name(&self) -> &str {
        "row_number"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// rank()
// ═══════════════════════════════════════════════════════════════════════════

pub struct RankState {
    row_number: i64,
    rank: i64,
    last_order_value: Option<SqliteValue>,
}

pub struct RankFunc;

impl WindowFunction for RankFunc {
    type State = RankState;

    fn initial_state(&self) -> Self::State {
        RankState {
            row_number: 0,
            rank: 0,
            last_order_value: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.row_number += 1;
        let current = args.first().cloned().unwrap_or(SqliteValue::Null);
        let is_new_peer = match &state.last_order_value {
            None => true,
            Some(last) => &current != last,
        };
        if is_new_peer {
            state.rank = state.row_number;
            state.last_order_value = Some(current);
        }
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        // rank() uses UNBOUNDED PRECEDING to CURRENT ROW; inverse is a no-op.
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.rank))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.rank))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        0
    }

    fn max_args(&self) -> Option<i32> {
        Some(0)
    }

    fn name(&self) -> &str {
        "rank"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// dense_rank()
// ═══════════════════════════════════════════════════════════════════════════

pub struct DenseRankState {
    dense_rank: i64,
    last_order_value: Option<SqliteValue>,
}

pub struct DenseRankFunc;

impl WindowFunction for DenseRankFunc {
    type State = DenseRankState;

    fn initial_state(&self) -> Self::State {
        DenseRankState {
            dense_rank: 0,
            last_order_value: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let current = args.first().cloned().unwrap_or(SqliteValue::Null);
        let is_new_peer = match &state.last_order_value {
            None => true,
            Some(last) => &current != last,
        };
        if is_new_peer {
            state.dense_rank += 1;
            state.last_order_value = Some(current);
        }
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.dense_rank))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.dense_rank))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        0
    }

    fn max_args(&self) -> Option<i32> {
        Some(0)
    }

    fn name(&self) -> &str {
        "dense_rank"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// percent_rank()
// ═══════════════════════════════════════════════════════════════════════════

/// State for `percent_rank()`.
///
/// The VDBE must step() all rows in the partition first (to compute ranks
/// and partition size), then iterate by calling value() and inverse() for
/// each output row.
pub struct PercentRankState {
    partition_size: i64,
    ranks: Vec<i64>,
    cursor: usize,
    step_row_number: i64,
    current_rank: i64,
    last_order_value: Option<SqliteValue>,
}

pub struct PercentRankFunc;

impl WindowFunction for PercentRankFunc {
    type State = PercentRankState;

    fn initial_state(&self) -> Self::State {
        PercentRankState {
            partition_size: 0,
            ranks: Vec::new(),
            cursor: 0,
            step_row_number: 0,
            current_rank: 0,
            last_order_value: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.step_row_number += 1;
        state.partition_size += 1;
        let current = args.first().cloned().unwrap_or(SqliteValue::Null);
        let is_new_peer = match &state.last_order_value {
            None => true,
            Some(last) => &current != last,
        };
        if is_new_peer {
            state.current_rank = state.step_row_number;
            state.last_order_value = Some(current);
        }
        state.ranks.push(state.current_rank);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.cursor += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        if state.partition_size <= 1 {
            return Ok(SqliteValue::Float(0.0));
        }
        let rank = state.ranks.get(state.cursor).copied().unwrap_or(1);
        let pr = (rank - 1) as f64 / (state.partition_size - 1) as f64;
        Ok(SqliteValue::Float(pr))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        0
    }

    fn max_args(&self) -> Option<i32> {
        Some(0)
    }

    fn name(&self) -> &str {
        "percent_rank"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// cume_dist()
// ═══════════════════════════════════════════════════════════════════════════

/// State for `cume_dist()`.
///
/// The VDBE must step() all rows first, then iterate with value()+inverse().
/// cume_dist = (last peer position) / partition_size.
pub struct CumeDistState {
    partition_size: i64,
    current_row: usize,
    cume_positions: Vec<i64>,
    peer_start: usize,
    last_order_value: Option<SqliteValue>,
}

pub struct CumeDistFunc;

impl WindowFunction for CumeDistFunc {
    type State = CumeDistState;

    fn initial_state(&self) -> Self::State {
        CumeDistState {
            partition_size: 0,
            current_row: 0,
            cume_positions: Vec::new(),
            peer_start: 0,
            last_order_value: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let current = args.first().cloned().unwrap_or(SqliteValue::Null);
        let is_new_peer = match &state.last_order_value {
            None => true,
            Some(last) => &current != last,
        };
        if is_new_peer {
            let peer_end = state.partition_size;
            if let Some(slots) = state.cume_positions.get_mut(state.peer_start..) {
                for slot in slots {
                    *slot = peer_end;
                }
            }
            state.peer_start = state.cume_positions.len();
            state.last_order_value = Some(current);
        }
        state.partition_size += 1;
        state.cume_positions.push(0);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.current_row += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        if state.partition_size == 0 {
            return Ok(SqliteValue::Float(0.0));
        }
        let peer_end = state
            .cume_positions
            .get(state.current_row)
            .copied()
            .filter(|position| *position != 0)
            .unwrap_or(state.partition_size);
        let cd = peer_end as f64 / state.partition_size as f64;
        Ok(SqliteValue::Float(cd))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        0
    }

    fn max_args(&self) -> Option<i32> {
        Some(0)
    }

    fn name(&self) -> &str {
        "cume_dist"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ntile(N)
// ═══════════════════════════════════════════════════════════════════════════

/// State for `ntile(N)`.
///
/// The VDBE must step() all rows first, then iterate with value()+inverse().
/// Distributes rows into N groups.  If partition_size % N != 0, the first
/// (partition_size % N) groups get one extra row.
pub struct NtileState {
    partition_size: i64,
    n: i64,
    current_row: i64,
}

pub struct NtileFunc;

const INVALID_NTILE_ARGUMENT: &str = "argument of ntile must be a positive integer";

impl WindowFunction for NtileFunc {
    type State = NtileState;

    fn initial_state(&self) -> Self::State {
        NtileState {
            partition_size: 0,
            n: 1,
            current_row: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if state.partition_size == 0 {
            let n = args.first().map_or(0, SqliteValue::to_integer);
            if n <= 0 {
                return Err(FrankenError::function_error(INVALID_NTILE_ARGUMENT));
            }
            state.n = n;
        }
        state.partition_size += 1;
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.current_row += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        if state.partition_size == 0 {
            return Ok(SqliteValue::Integer(1));
        }
        let n = state.n;
        let sz = state.partition_size;
        let row = state.current_row + 1; // 1-based

        // Group size: first (sz % n) groups get (sz / n + 1) rows,
        // remaining groups get (sz / n) rows.
        let base = sz / n;
        let extra = sz % n;
        // Rows in "large" groups: extra * (base + 1).
        let large_rows = extra * (base + 1);

        let bucket = if row <= large_rows {
            // In one of the first `extra` groups (each of size base+1).
            (row - 1) / (base + 1) + 1
        } else {
            // In one of the remaining groups (each of size base).
            let adjusted = row - large_rows;
            if base == 0 {
                // More buckets than rows; each remaining row gets its own bucket.
                extra + adjusted
            } else {
                extra + (adjusted - 1) / base + 1
            }
        };
        Ok(SqliteValue::Integer(bucket))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "ntile"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// lag(X [, offset [, default]])
// ═══════════════════════════════════════════════════════════════════════════

fn numeric_prefix_len(bytes: &[u8]) -> usize {
    let mut idx = 0;
    if bytes
        .get(idx)
        .is_some_and(|byte| matches!(*byte, b'+' | b'-'))
    {
        idx += 1;
    }

    let mut saw_digit = false;
    while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
        idx += 1;
        saw_digit = true;
    }

    if bytes.get(idx) == Some(&b'.') {
        idx += 1;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
            saw_digit = true;
        }
    }

    if !saw_digit {
        return 0;
    }

    let mantissa_end = idx;
    if bytes
        .get(idx)
        .is_some_and(|byte| matches!(*byte, b'e' | b'E'))
    {
        idx += 1;
        if bytes
            .get(idx)
            .is_some_and(|byte| matches!(*byte, b'+' | b'-'))
        {
            idx += 1;
        }
        let exp_start = idx;
        while bytes.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        if idx == exp_start {
            return mantissa_end;
        }
    }

    idx
}

fn trim_ascii_start(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    bytes.get(start..).unwrap_or(&[])
}

fn lag_lead_bytes_offset(bytes: &[u8]) -> Option<i64> {
    let trimmed = trim_ascii_start(bytes);
    let prefix_len = numeric_prefix_len(trimmed);
    if prefix_len == 0 {
        return Some(0);
    }
    let prefix = trimmed
        .get(..prefix_len)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())?;
    if prefix
        .as_bytes()
        .iter()
        .any(|byte| matches!(*byte, b'.' | b'e' | b'E'))
    {
        prefix.parse().ok().and_then(integral_f64_to_i64)
    } else {
        prefix
            .parse()
            .ok()
            .or_else(|| prefix.parse().ok().and_then(integral_f64_to_i64))
    }
}

fn lag_lead_text_offset(text: &str) -> Option<i64> {
    lag_lead_bytes_offset(text.as_bytes())
}

fn lag_lead_offset_arg(value: Option<&SqliteValue>) -> Option<i64> {
    match value {
        None => Some(1),
        Some(SqliteValue::Null) => None,
        Some(SqliteValue::Integer(offset)) => Some(*offset),
        Some(SqliteValue::Float(offset)) => integral_f64_to_i64(*offset),
        Some(SqliteValue::Text(text)) => lag_lead_text_offset(text),
        Some(SqliteValue::Blob(bytes)) => lag_lead_bytes_offset(bytes),
    }
}

/// State for `lag()`: maintains a buffer of previous values.
pub struct LagState {
    buffer: Vec<SqliteValue>,
    offsets: Vec<Option<i64>>,
    defaults: Vec<SqliteValue>,
    current_row: i64,
}

pub struct LagFunc;

impl WindowFunction for LagFunc {
    type State = LagState;

    fn initial_state(&self) -> Self::State {
        LagState {
            buffer: Vec::new(),
            offsets: Vec::new(),
            defaults: Vec::new(),
            current_row: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let val = args.first().cloned().unwrap_or(SqliteValue::Null);
        let offset = lag_lead_offset_arg(args.get(1));
        let default_val = args.get(2).cloned().unwrap_or(SqliteValue::Null);
        state.buffer.push(val);
        state.offsets.push(offset);
        state.defaults.push(default_val);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.current_row += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let current_index = usize::try_from(state.current_row).unwrap_or(usize::MAX);
        let default_val = state
            .defaults
            .get(current_index)
            .cloned()
            .unwrap_or(SqliteValue::Null);
        let Some(offset) = state.offsets.get(current_index).copied().flatten() else {
            return Ok(default_val);
        };
        let target = state.current_row - offset;
        let Ok(target_index) = usize::try_from(target) else {
            return Ok(default_val);
        };
        Ok(state
            .buffer
            .get(target_index)
            .cloned()
            .unwrap_or(default_val))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1 // 1, 2, or 3 args
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(3)
    }

    fn name(&self) -> &str {
        "lag"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// lead(X [, offset [, default]])
// ═══════════════════════════════════════════════════════════════════════════

/// State for `lead()`: maintains a buffer and reads ahead.
pub struct LeadState {
    buffer: Vec<SqliteValue>,
    offsets: Vec<Option<i64>>,
    defaults: Vec<SqliteValue>,
    current_row: i64,
}

pub struct LeadFunc;

impl WindowFunction for LeadFunc {
    type State = LeadState;

    fn initial_state(&self) -> Self::State {
        LeadState {
            buffer: Vec::new(),
            offsets: Vec::new(),
            defaults: Vec::new(),
            current_row: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let val = args.first().cloned().unwrap_or(SqliteValue::Null);
        let offset = lag_lead_offset_arg(args.get(1));
        let default_val = args.get(2).cloned().unwrap_or(SqliteValue::Null);
        state.buffer.push(val);
        state.offsets.push(offset);
        state.defaults.push(default_val);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.current_row += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let current_index = usize::try_from(state.current_row).unwrap_or(usize::MAX);
        let default_val = state
            .defaults
            .get(current_index)
            .cloned()
            .unwrap_or(SqliteValue::Null);
        let Some(offset) = state.offsets.get(current_index).copied().flatten() else {
            return Ok(default_val);
        };
        let target = state.current_row + offset;
        if target < 0 || target >= state.buffer.len() as i64 {
            return Ok(default_val);
        }
        Ok(state.buffer[target as usize].clone())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(3)
    }

    fn name(&self) -> &str {
        "lead"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// first_value(X)
// ═══════════════════════════════════════════════════════════════════════════

pub struct FirstValueState {
    first: Option<SqliteValue>,
}

pub struct FirstValueFunc;

impl WindowFunction for FirstValueFunc {
    type State = FirstValueState;

    fn initial_state(&self) -> Self::State {
        FirstValueState { first: None }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if state.first.is_none() {
            state.first = Some(args.first().cloned().unwrap_or(SqliteValue::Null));
        }
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        // When the first row exits the frame, we need to clear so the next
        // step() captures the new first value.  For simplicity, we use a
        // VecDeque-based approach in FirstValueFrameFunc below.  This basic
        // version handles the common UNBOUNDED PRECEDING case correctly.
        state.first = None;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(state.first.clone().unwrap_or(SqliteValue::Null))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.first.unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "first_value"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// last_value(X)
// ═══════════════════════════════════════════════════════════════════════════

pub struct LastValueState {
    frame: VecDeque<SqliteValue>,
}

pub struct LastValueFunc;

impl WindowFunction for LastValueFunc {
    type State = LastValueState;

    fn initial_state(&self) -> Self::State {
        LastValueState {
            frame: VecDeque::new(),
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state
            .frame
            .push_back(args.first().cloned().unwrap_or(SqliteValue::Null));
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.frame.pop_front();
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(state.frame.back().cloned().unwrap_or(SqliteValue::Null))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.frame.back().cloned().unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "last_value"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// nth_value(X, N)
// ═══════════════════════════════════════════════════════════════════════════

pub struct NthValueState {
    frame: VecDeque<SqliteValue>,
    n: i64,
}

pub struct NthValueFunc;

const INVALID_NTH_VALUE_ARGUMENT: &str = "second argument to nth_value must be a positive integer";

fn integral_f64_to_i64(value: f64) -> Option<i64> {
    const I64_MIN_AS_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_EXCLUSIVE_AS_F64: f64 = 9_223_372_036_854_775_808.0;

    if !value.is_finite()
        || value.fract() != 0.0
        || !(I64_MIN_AS_F64..I64_MAX_EXCLUSIVE_AS_F64).contains(&value)
    {
        return None;
    }
    Some(value as i64)
}

fn parse_integral_text(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed
        .parse()
        .ok()
        .or_else(|| trimmed.parse().ok().and_then(integral_f64_to_i64))
}

fn nth_value_positive_integer_arg(value: Option<&SqliteValue>) -> Result<i64> {
    let Some(value) = value else {
        return Err(FrankenError::function_error(INVALID_NTH_VALUE_ARGUMENT));
    };
    let n = match value {
        SqliteValue::Integer(n) => Some(*n),
        SqliteValue::Float(n) => integral_f64_to_i64(*n),
        SqliteValue::Text(text) => parse_integral_text(text),
        SqliteValue::Null | SqliteValue::Blob(_) => None,
    };
    match n {
        Some(n) if n > 0 => Ok(n),
        _ => Err(FrankenError::function_error(INVALID_NTH_VALUE_ARGUMENT)),
    }
}

impl WindowFunction for NthValueFunc {
    type State = NthValueState;

    fn initial_state(&self) -> Self::State {
        NthValueState {
            frame: VecDeque::new(),
            n: 1,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let val = args.first().cloned().unwrap_or(SqliteValue::Null);
        let n = nth_value_positive_integer_arg(args.get(1))?;
        // Capture N from second arg on first call.
        if state.frame.is_empty() {
            state.n = n;
        }
        state.frame.push_back(val);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.frame.pop_front();
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let idx = (state.n - 1) as usize;
        Ok(state.frame.get(idx).cloned().unwrap_or(SqliteValue::Null))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "nth_value"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Aggregate-as-window functions: SUM, AVG, COUNT, MIN, MAX, TOTAL
// SQLite allows any aggregate function to be used as a window function.
// ═══════════════════════════════════════════════════════════════════════════

pub struct WindowSumState {
    sum: f64,
    err: f64,
    has_value: bool,
    is_int: bool,
    int_sum: i64,
    overflowed: bool,
}

/// Kahan-Babuska-Neumaier compensated summation step matching upstream
/// aggregate precision behavior.
#[inline]
fn kbn_step(sum: &mut f64, err: &mut f64, value: f64) {
    let s = *sum;
    let t = s + value;
    if s.abs() > value.abs() {
        *err += (s - t) + value;
    } else {
        *err += (value - t) + s;
    }
    *sum = t;
}

pub struct WindowSumFunc;

impl WindowFunction for WindowSumFunc {
    type State = WindowSumState;

    fn initial_state(&self) -> Self::State {
        WindowSumState {
            sum: 0.0,
            err: 0.0,
            has_value: false,
            is_int: true,
            int_sum: 0,
            overflowed: false,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            return Ok(());
        }
        let value = args[0].to_sum_numeric_value();
        if value.is_null() {
            return Ok(());
        }
        state.has_value = true;
        match value {
            SqliteValue::Integer(i) => {
                if state.is_int && !state.overflowed {
                    match state.int_sum.checked_add(i) {
                        Some(s) => state.int_sum = s,
                        None => state.overflowed = true,
                    }
                }
                kbn_step(&mut state.sum, &mut state.err, i as f64);
            }
            SqliteValue::Float(f) => {
                state.is_int = false;
                kbn_step(&mut state.sum, &mut state.err, f);
            }
            SqliteValue::Null | SqliteValue::Text(_) | SqliteValue::Blob(_) => {}
        }
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            return Ok(());
        }
        let value = args[0].to_sum_numeric_value();
        match value {
            SqliteValue::Integer(i) => {
                if state.is_int && !state.overflowed {
                    match state.int_sum.checked_sub(i) {
                        Some(s) => state.int_sum = s,
                        None => state.overflowed = true,
                    }
                }
                kbn_step(&mut state.sum, &mut state.err, -(i as f64));
            }
            SqliteValue::Float(f) => {
                state.is_int = false;
                kbn_step(&mut state.sum, &mut state.err, -f);
            }
            SqliteValue::Null | SqliteValue::Text(_) | SqliteValue::Blob(_) => {}
        }
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        if !state.has_value {
            return Ok(SqliteValue::Null);
        }
        if state.is_int && state.overflowed {
            return Err(FrankenError::IntegerOverflow);
        }
        if state.is_int {
            Ok(SqliteValue::Integer(state.int_sum))
        } else {
            Ok(SqliteValue::Float(state.sum + state.err))
        }
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "SUM"
    }
}

pub struct WindowTotalFunc;

impl WindowFunction for WindowTotalFunc {
    type State = f64;

    fn initial_state(&self) -> Self::State {
        0.0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if !args.is_empty() && !args[0].is_null() {
            *state += args[0].to_float();
        }
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if !args.is_empty() && !args[0].is_null() {
            *state -= args[0].to_float();
        }
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Float(*state))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Float(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "TOTAL"
    }
}

pub struct WindowCountState {
    count: i64,
}

pub struct WindowCountFunc;

impl WindowFunction for WindowCountFunc {
    type State = WindowCountState;

    fn initial_state(&self) -> Self::State {
        WindowCountState { count: 0 }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        // COUNT(*) has 0 args → count all rows; COUNT(x) skips NULLs.
        if args.is_empty() || !args[0].is_null() {
            state.count += 1;
        }
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || !args[0].is_null() {
            state.count -= 1;
        }
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.count))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state.count))
    }

    fn num_args(&self) -> i32 {
        -1 // variadic: COUNT(*) = 0 args, COUNT(x) = 1 arg
    }

    fn min_args(&self) -> i32 {
        0
    }

    fn max_args(&self) -> Option<i32> {
        Some(1)
    }

    fn name(&self) -> &str {
        "COUNT"
    }
}

pub struct WindowMinState {
    min: Option<SqliteValue>,
}

pub struct WindowMinFunc;

impl WindowFunction for WindowMinFunc {
    type State = WindowMinState;

    fn initial_state(&self) -> Self::State {
        WindowMinState { min: None }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            return Ok(());
        }
        state.min = Some(match state.min.take() {
            None => args[0].clone(),
            Some(cur) => {
                if cmp_values(&args[0], &cur) == std::cmp::Ordering::Less {
                    args[0].clone()
                } else {
                    cur
                }
            }
        });
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        // MIN inverse is not efficiently invertible; no-op for unbounded frames.
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(state.min.clone().unwrap_or(SqliteValue::Null))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.min.unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "MIN"
    }
}

pub struct WindowMaxState {
    max: Option<SqliteValue>,
}

pub struct WindowMaxFunc;

impl WindowFunction for WindowMaxFunc {
    type State = WindowMaxState;

    fn initial_state(&self) -> Self::State {
        WindowMaxState { max: None }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            return Ok(());
        }
        state.max = Some(match state.max.take() {
            None => args[0].clone(),
            Some(cur) => {
                if cmp_values(&args[0], &cur) == std::cmp::Ordering::Greater {
                    args[0].clone()
                } else {
                    cur
                }
            }
        });
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(state.max.clone().unwrap_or(SqliteValue::Null))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.max.unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "MAX"
    }
}

pub struct WindowAvgState {
    sum: f64,
    err: f64,
    count: i64,
}

pub struct WindowAvgFunc;

impl WindowFunction for WindowAvgFunc {
    type State = WindowAvgState;

    fn initial_state(&self) -> Self::State {
        WindowAvgState {
            sum: 0.0,
            err: 0.0,
            count: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            return Ok(());
        }
        kbn_step(&mut state.sum, &mut state.err, args[0].to_float());
        state.count += 1;
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            return Ok(());
        }
        kbn_step(&mut state.sum, &mut state.err, -args[0].to_float());
        state.count -= 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        if state.count == 0 {
            Ok(SqliteValue::Null)
        } else {
            #[allow(clippy::cast_precision_loss)]
            Ok(SqliteValue::Float(
                (state.sum + state.err) / state.count as f64,
            ))
        }
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "AVG"
    }
}

pub struct WindowGroupConcatState {
    result: String,
    has_value: bool,
}

fn window_group_concat_step(state: &mut WindowGroupConcatState, args: &[SqliteValue]) {
    if args.is_empty() || args[0].is_null() {
        return;
    }
    if state.has_value {
        match args.get(1) {
            Some(separator) if !separator.is_null() => {
                if let Some(text) = separator.as_text_str() {
                    state.result.push_str(text);
                } else {
                    state.result.push_str(&separator.to_text());
                }
            }
            Some(_) => {}
            None => state.result.push(','),
        }
    }
    if let Some(text) = args[0].as_text_str() {
        state.result.push_str(text);
    } else {
        state.result.push_str(&args[0].to_text());
    }
    state.has_value = true;
}

fn window_group_concat_value(state: &WindowGroupConcatState) -> SqliteValue {
    if state.has_value {
        SqliteValue::Text(state.result.clone().into())
    } else {
        SqliteValue::Null
    }
}

pub struct WindowGroupConcatFunc;

impl WindowFunction for WindowGroupConcatFunc {
    type State = WindowGroupConcatState;

    fn initial_state(&self) -> Self::State {
        WindowGroupConcatState {
            result: String::new(),
            has_value: false,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        window_group_concat_step(state, args);
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        // Sliding ROWS/RANGE/GROUPS frames are recomputed by the connection-level
        // window executor; the remaining paths never evict rows from the left edge.
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(window_group_concat_value(state))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(window_group_concat_value(&state))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        1
    }

    fn max_args(&self) -> Option<i32> {
        Some(2)
    }

    fn name(&self) -> &str {
        "group_concat"
    }
}

pub struct WindowStringAggFunc;

impl WindowFunction for WindowStringAggFunc {
    type State = WindowGroupConcatState;

    fn initial_state(&self) -> Self::State {
        WindowGroupConcatState {
            result: String::new(),
            has_value: false,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        window_group_concat_step(state, args);
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        Ok(window_group_concat_value(state))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(window_group_concat_value(&state))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "string_agg"
    }
}

/// Compare two SQLite values using type-aware ordering.
pub fn cmp_values(a: &SqliteValue, b: &SqliteValue) -> std::cmp::Ordering {
    a.cmp(b)
}

// ── Registration ──────────────────────────────────────────────────────────

/// Register all S13.5 window functions.
pub fn register_window_builtins(registry: &mut FunctionRegistry) {
    registry.register_window(RowNumberFunc);
    registry.register_window(RankFunc);
    registry.register_window(DenseRankFunc);
    registry.register_window(PercentRankFunc);
    registry.register_window(CumeDistFunc);
    registry.register_window(NtileFunc);
    registry.register_window(LagFunc);
    registry.register_window(LeadFunc);
    registry.register_window(FirstValueFunc);
    registry.register_window(LastValueFunc);
    registry.register_window(NthValueFunc);

    // Aggregate-as-window functions (SQLite allows any aggregate as a window fn).
    registry.register_window(WindowSumFunc);
    registry.register_window(WindowTotalFunc);
    registry.register_window(WindowCountFunc);
    registry.register_window(WindowMinFunc);
    registry.register_window(WindowMaxFunc);
    registry.register_window(WindowAvgFunc);
    registry.register_window(WindowGroupConcatFunc);
    registry.register_window(WindowStringAggFunc);
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i64) -> SqliteValue {
        SqliteValue::Integer(v)
    }

    fn float(v: f64) -> SqliteValue {
        SqliteValue::Float(v)
    }

    fn text(s: &str) -> SqliteValue {
        SqliteValue::Text(s.into())
    }

    fn blob(bytes: &[u8]) -> SqliteValue {
        SqliteValue::Blob(std::sync::Arc::from(bytes))
    }

    fn null() -> SqliteValue {
        SqliteValue::Null
    }

    fn assert_function_error(err: FrankenError, expected: &str) {
        assert!(
            matches!(&err, FrankenError::FunctionError(message) if message == expected),
            "expected function error {expected:?}, got {err:?}"
        );
    }

    fn assert_float_near(value: &SqliteValue, expected: f64) {
        assert!(
            matches!(value, SqliteValue::Float(_)),
            "expected Float, got {value:?}"
        );
        if let SqliteValue::Float(actual) = value {
            assert!(
                (*actual - expected).abs() < 1e-10,
                "expected {expected}, got {actual}"
            );
        }
    }

    /// Simulate a partition by calling step() for each row, collecting
    /// value() after each step.  Returns the vector of per-row results.
    /// Suitable for progressive functions (row_number, rank, dense_rank).
    fn run_window_partition<F: WindowFunction>(
        func: &F,
        rows: &[Vec<SqliteValue>],
    ) -> Vec<SqliteValue> {
        let mut state = func.initial_state();
        let mut results = Vec::new();
        for row in rows {
            func.step(&mut state, row).unwrap();
            results.push(func.value(&state).unwrap());
        }
        results
    }

    /// Two-pass partition simulation: step all rows first (pass 1), then
    /// iterate calling value()+inverse() for each row (pass 2).
    /// Required for functions that need full partition size (ntile,
    /// percent_rank, cume_dist).
    fn run_window_two_pass<F: WindowFunction>(
        func: &F,
        rows: &[Vec<SqliteValue>],
    ) -> Vec<SqliteValue> {
        let mut state = func.initial_state();
        // Pass 1: step all rows.
        for row in rows {
            func.step(&mut state, row).unwrap();
        }
        // Pass 2: read values, advance cursor via inverse().
        let mut results = Vec::new();
        for (i, _) in rows.iter().enumerate() {
            results.push(func.value(&state).unwrap());
            if i < rows.len() - 1 {
                func.inverse(&mut state, &[]).unwrap();
            }
        }
        results
    }

    // ── row_number ───────────────────────────────────────────────────

    #[test]
    fn test_row_number_basic() {
        let results =
            run_window_partition(&RowNumberFunc, &[vec![], vec![], vec![], vec![], vec![]]);
        assert_eq!(results, vec![int(1), int(2), int(3), int(4), int(5)]);
    }

    #[test]
    fn test_row_number_partition_reset() {
        // Partition 1: 3 rows.
        let r1 = run_window_partition(&RowNumberFunc, &[vec![], vec![], vec![]]);
        assert_eq!(r1, vec![int(1), int(2), int(3)]);

        // Partition 2: 2 rows (fresh state).
        let r2 = run_window_partition(&RowNumberFunc, &[vec![], vec![]]);
        assert_eq!(r2, vec![int(1), int(2)]);
    }

    // ── rank ─────────────────────────────────────────────────────────

    #[test]
    fn test_rank_with_ties() {
        // Values: [1, 2, 2, 3] -> ranks: [1, 2, 2, 4]
        let results = run_window_partition(
            &RankFunc,
            &[vec![int(1)], vec![int(2)], vec![int(2)], vec![int(3)]],
        );
        assert_eq!(results, vec![int(1), int(2), int(2), int(4)]);
    }

    #[test]
    fn test_rank_no_ties() {
        let results =
            run_window_partition(&RankFunc, &[vec![int(10)], vec![int(20)], vec![int(30)]]);
        assert_eq!(results, vec![int(1), int(2), int(3)]);
    }

    // ── dense_rank ───────────────────────────────────────────────────

    #[test]
    fn test_dense_rank_with_ties() {
        // Values: [1, 2, 2, 3] -> dense_ranks: [1, 2, 2, 3]
        let results = run_window_partition(
            &DenseRankFunc,
            &[vec![int(1)], vec![int(2)], vec![int(2)], vec![int(3)]],
        );
        assert_eq!(results, vec![int(1), int(2), int(2), int(3)]);
    }

    #[test]
    fn test_dense_rank_multiple_ties() {
        // Values: [1, 1, 2, 2, 3] -> dense_ranks: [1, 1, 2, 2, 3]
        let results = run_window_partition(
            &DenseRankFunc,
            &[
                vec![int(1)],
                vec![int(1)],
                vec![int(2)],
                vec![int(2)],
                vec![int(3)],
            ],
        );
        assert_eq!(results, vec![int(1), int(1), int(2), int(2), int(3)]);
    }

    // ── percent_rank ─────────────────────────────────────────────────

    #[test]
    fn test_percent_rank_single_row() {
        let results = run_window_two_pass(&PercentRankFunc, &[vec![int(1)]]);
        assert_eq!(results, vec![SqliteValue::Float(0.0)]);
    }

    #[test]
    fn test_percent_rank_formula() {
        // 4 rows, values [1, 2, 2, 3] -> ranks [1, 2, 2, 4]
        // percent_rank = (rank - 1) / (N - 1) = (rank - 1) / 3
        let results = run_window_two_pass(
            &PercentRankFunc,
            &[vec![int(1)], vec![int(2)], vec![int(2)], vec![int(3)]],
        );
        // Row 1: (1-1)/3 = 0.0
        // Row 2: (2-1)/3 = 0.333...
        // Row 3: (2-1)/3 = 0.333... (same rank as row 2)
        // Row 4: (4-1)/3 = 1.0
        assert_float_near(&results[0], 0.0);
        assert_float_near(&results[1], 1.0 / 3.0);
        assert_float_near(&results[2], 1.0 / 3.0);
        assert_float_near(&results[3], 1.0);
    }

    #[test]
    fn test_percent_rank_without_order_treats_partition_as_one_peer_group() {
        let results = run_window_two_pass(&PercentRankFunc, &[vec![], vec![], vec![]]);
        for value in results {
            assert_float_near(&value, 0.0);
        }
    }

    // ── cume_dist ────────────────────────────────────────────────────

    #[test]
    fn test_cume_dist_distinct() {
        // 4 distinct values: cume_dist = [0.25, 0.5, 0.75, 1.0]
        let results = run_window_two_pass(
            &CumeDistFunc,
            &[vec![int(1)], vec![int(2)], vec![int(3)], vec![int(4)]],
        );
        for (i, expected) in [0.25, 0.5, 0.75, 1.0].iter().enumerate() {
            assert_float_near(&results[i], *expected);
        }
    }

    #[test]
    fn test_cume_dist_with_ties() {
        // Values [1, 2, 2, 3] -> last peer positions [1, 3, 3, 4].
        let results = run_window_two_pass(
            &CumeDistFunc,
            &[vec![int(1)], vec![int(2)], vec![int(2)], vec![int(3)]],
        );
        for (i, expected) in [0.25, 0.75, 0.75, 1.0].iter().enumerate() {
            assert_float_near(&results[i], *expected);
        }
    }

    #[test]
    fn test_cume_dist_without_order_treats_partition_as_one_peer_group() {
        let results = run_window_two_pass(&CumeDistFunc, &[vec![], vec![], vec![]]);
        for value in results {
            assert_float_near(&value, 1.0);
        }
    }

    #[test]
    fn test_cume_dist_null_peers_share_same_peer_group() {
        let results =
            run_window_two_pass(&CumeDistFunc, &[vec![null()], vec![null()], vec![int(1)]]);
        assert_float_near(&results[0], 2.0 / 3.0);
        assert_float_near(&results[1], 2.0 / 3.0);
        assert_float_near(&results[2], 1.0);
    }

    // ── ntile ────────────────────────────────────────────────────────

    #[test]
    fn test_ntile_even() {
        // ntile(4) over 8 rows: groups of 2 each -> [1,1,2,2,3,3,4,4]
        let rows: Vec<Vec<SqliteValue>> = (0..8).map(|_| vec![int(4)]).collect();
        let results = run_window_two_pass(&NtileFunc, &rows);
        assert_eq!(
            results,
            vec![
                int(1),
                int(1),
                int(2),
                int(2),
                int(3),
                int(3),
                int(4),
                int(4)
            ]
        );
    }

    #[test]
    fn test_ntile_uneven() {
        // ntile(3) over 10 rows: groups of 4,3,3
        let rows: Vec<Vec<SqliteValue>> = (0..10).map(|_| vec![int(3)]).collect();
        let results = run_window_two_pass(&NtileFunc, &rows);
        assert_eq!(
            results,
            vec![
                int(1),
                int(1),
                int(1),
                int(1),
                int(2),
                int(2),
                int(2),
                int(3),
                int(3),
                int(3)
            ]
        );
    }

    #[test]
    fn test_ntile_more_buckets_than_rows() {
        // ntile(10) over 3 rows: [1, 2, 3]
        let rows: Vec<Vec<SqliteValue>> = (0..3).map(|_| vec![int(10)]).collect();
        let results = run_window_two_pass(&NtileFunc, &rows);
        assert_eq!(results, vec![int(1), int(2), int(3)]);
    }

    #[test]
    fn test_ntile_rejects_non_positive_argument() {
        for n in [0, -1] {
            let mut state = NtileFunc.initial_state();
            let err = NtileFunc.step(&mut state, &[int(n)]).unwrap_err();
            assert_function_error(err, INVALID_NTILE_ARGUMENT);
        }
    }

    // ── lag ──────────────────────────────────────────────────────────

    #[test]
    fn test_lag_default() {
        // lag(X) with default offset=1: previous row's value, NULL for first.
        let results = run_window_two_pass(&LagFunc, &[vec![int(10)], vec![int(20)], vec![int(30)]]);
        assert_eq!(results, vec![null(), int(10), int(20)]);
    }

    #[test]
    fn test_lag_offset_3() {
        // lag(X, 3): 3 rows back.
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), int(3)],
                vec![int(20), int(3)],
                vec![int(30), int(3)],
                vec![int(40), int(3)],
                vec![int(50), int(3)],
            ],
        );
        assert_eq!(results, vec![null(), null(), null(), int(10), int(20)]);
    }

    #[test]
    fn test_lag_default_value() {
        // lag(X, 1, -1): returns -1 when no previous row.
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), int(1), int(-1)],
                vec![int(20), int(1), int(-1)],
            ],
        );
        assert_eq!(results, vec![int(-1), int(10)]);
    }

    #[test]
    fn test_lag_null_offset_returns_default_for_each_row() {
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), null(), text("N/A")],
                vec![int(20), null(), text("N/A")],
            ],
        );
        assert_eq!(results, vec![text("N/A"), text("N/A")]);
    }

    #[test]
    fn test_lag_uses_current_row_offset_and_default() {
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), int(1), text("first")],
                vec![int(20), null(), text("null-offset")],
                vec![int(30), int(1), text("third")],
                vec![int(40), int(2), text("fourth")],
            ],
        );
        assert_eq!(
            results,
            vec![text("first"), text("null-offset"), int(20), int(20)]
        );
    }

    #[test]
    fn test_lag_negative_offset_reads_following_row() {
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), int(-1), text("N/A")],
                vec![int(20), int(-1), text("N/A")],
                vec![int(30), int(-1), text("N/A")],
            ],
        );
        assert_eq!(results, vec![int(20), int(30), text("N/A")]);
    }

    #[test]
    fn test_lag_fractional_offset_uses_default() {
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), float(1.5), text("N/A")],
                vec![int(20), float(1.5), text("N/A")],
                vec![int(30), float(1.5), text("N/A")],
            ],
        );
        assert_eq!(results, vec![text("N/A"), text("N/A"), text("N/A")]);
    }

    #[test]
    fn test_lag_nonnumeric_text_offset_reads_current_row() {
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), text("abc"), text("N/A")],
                vec![int(20), text("abc"), text("N/A")],
                vec![int(30), text("abc"), text("N/A")],
            ],
        );
        assert_eq!(results, vec![int(10), int(20), int(30)]);
    }

    #[test]
    fn test_lag_integral_text_prefix_offset() {
        let results = run_window_two_pass(
            &LagFunc,
            &[
                vec![int(10), text("2.0x"), text("N/A")],
                vec![int(20), text("2e0x"), text("N/A")],
                vec![int(30), text("2x"), text("N/A")],
            ],
        );
        assert_eq!(results, vec![text("N/A"), text("N/A"), int(10)]);
    }

    // ── lead ─────────────────────────────────────────────────────────

    #[test]
    fn test_lead_default() {
        // lead(X): next row's value, NULL for last.
        // For lead, we need to step all rows first (to build the buffer),
        // then call inverse + value for each row.
        let func = LeadFunc;
        let mut state = func.initial_state();
        let rows = [int(10), int(20), int(30)];

        // Step all rows to build the buffer.
        for row in &rows {
            func.step(&mut state, std::slice::from_ref(row)).unwrap();
        }

        // Now iterate: first row is at current_row=0.
        let mut results = Vec::new();
        for _ in &rows {
            results.push(func.value(&state).unwrap());
            func.inverse(&mut state, &[]).unwrap();
        }
        assert_eq!(results, vec![int(20), int(30), null()]);
    }

    #[test]
    fn test_lead_offset_2() {
        let func = LeadFunc;
        let mut state = func.initial_state();
        let rows = [int(10), int(20), int(30), int(40), int(50)];

        for row in &rows {
            func.step(&mut state, &[row.clone(), int(2)]).unwrap();
        }

        let mut results = Vec::new();
        for _ in &rows {
            results.push(func.value(&state).unwrap());
            func.inverse(&mut state, &[]).unwrap();
        }
        assert_eq!(results, vec![int(30), int(40), int(50), null(), null()]);
    }

    #[test]
    fn test_lead_default_value() {
        let func = LeadFunc;
        let mut state = func.initial_state();
        let rows = [int(10), int(20)];

        for row in &rows {
            func.step(&mut state, &[row.clone(), int(1), text("N/A")])
                .unwrap();
        }

        let mut results = Vec::new();
        for _ in &rows {
            results.push(func.value(&state).unwrap());
            func.inverse(&mut state, &[]).unwrap();
        }
        assert_eq!(results, vec![int(20), text("N/A")]);
    }

    #[test]
    fn test_lead_null_offset_returns_default_for_each_row() {
        let func = LeadFunc;
        let mut state = func.initial_state();
        let rows = [int(10), int(20)];

        for row in &rows {
            func.step(&mut state, &[row.clone(), null(), text("N/A")])
                .unwrap();
        }

        let mut results = Vec::new();
        for _ in &rows {
            results.push(func.value(&state).unwrap());
            func.inverse(&mut state, &[]).unwrap();
        }
        assert_eq!(results, vec![text("N/A"), text("N/A")]);
    }

    #[test]
    fn test_lead_uses_current_row_offset_and_default() {
        let func = LeadFunc;
        let mut state = func.initial_state();
        let rows = [
            vec![int(10), int(1), text("first")],
            vec![int(20), null(), text("null-offset")],
            vec![int(30), int(1), text("third")],
        ];

        for row in &rows {
            func.step(&mut state, row).unwrap();
        }

        let mut results = Vec::new();
        for _ in &rows {
            results.push(func.value(&state).unwrap());
            func.inverse(&mut state, &[]).unwrap();
        }
        assert_eq!(results, vec![int(20), text("null-offset"), text("third")]);
    }

    #[test]
    fn test_lead_negative_offset_reads_previous_row() {
        let func = LeadFunc;
        let mut state = func.initial_state();
        let rows = [int(10), int(20), int(30)];

        for row in &rows {
            func.step(&mut state, &[row.clone(), int(-1), text("N/A")])
                .unwrap();
        }

        let mut results = Vec::new();
        for _ in &rows {
            results.push(func.value(&state).unwrap());
            func.inverse(&mut state, &[]).unwrap();
        }
        assert_eq!(results, vec![text("N/A"), int(10), int(20)]);
    }

    #[test]
    fn test_lead_fractional_offset_uses_default() {
        let results = run_window_two_pass(
            &LeadFunc,
            &[
                vec![int(10), float(1.5), text("N/A")],
                vec![int(20), float(1.5), text("N/A")],
                vec![int(30), float(1.5), text("N/A")],
            ],
        );
        assert_eq!(results, vec![text("N/A"), text("N/A"), text("N/A")]);
    }

    #[test]
    fn test_lead_integral_blob_offset() {
        let results = run_window_two_pass(
            &LeadFunc,
            &[
                vec![int(10), blob(b"2.0x"), text("N/A")],
                vec![int(20), blob(b"2e0x"), text("N/A")],
                vec![int(30), blob(b"2x"), text("N/A")],
            ],
        );
        assert_eq!(results, vec![int(30), text("N/A"), text("N/A")]);
    }

    // ── first_value ──────────────────────────────────────────────────

    #[test]
    fn test_first_value_basic() {
        let results = run_window_partition(
            &FirstValueFunc,
            &[vec![int(10)], vec![int(20)], vec![int(30)]],
        );
        // With default frame (UNBOUNDED PRECEDING to CURRENT ROW),
        // first_value is always the first row's value.
        assert_eq!(results, vec![int(10), int(10), int(10)]);
    }

    // ── last_value ───────────────────────────────────────────────────

    #[test]
    fn test_last_value_default_frame() {
        // With default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        // last_value returns the current row's value.
        let results = run_window_partition(
            &LastValueFunc,
            &[vec![int(10)], vec![int(20)], vec![int(30)]],
        );
        assert_eq!(results, vec![int(10), int(20), int(30)]);
    }

    #[test]
    fn test_last_value_unbounded_following() {
        // With UNBOUNDED FOLLOWING frame, step all rows first,
        // then value() returns the true last value.
        let func = LastValueFunc;
        let mut state = func.initial_state();
        func.step(&mut state, &[int(10)]).unwrap();
        func.step(&mut state, &[int(20)]).unwrap();
        func.step(&mut state, &[int(30)]).unwrap();
        assert_eq!(func.value(&state).unwrap(), int(30));
    }

    // ── nth_value ────────────────────────────────────────────────────

    #[test]
    fn test_nth_value_basic() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        // Step 5 rows; N=3.
        func.step(&mut state, &[int(10), int(3)]).unwrap();
        func.step(&mut state, &[int(20), int(3)]).unwrap();
        func.step(&mut state, &[int(30), int(3)]).unwrap();
        func.step(&mut state, &[int(40), int(3)]).unwrap();
        func.step(&mut state, &[int(50), int(3)]).unwrap();
        assert_eq!(func.value(&state).unwrap(), int(30));
    }

    #[test]
    fn test_nth_value_out_of_range() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        func.step(&mut state, &[int(10), int(100)]).unwrap();
        func.step(&mut state, &[int(20), int(100)]).unwrap();
        // Frame has 2 rows but N=100.
        assert_eq!(func.value(&state).unwrap(), null());
    }

    #[test]
    fn test_nth_value_n_zero() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        let err = func.step(&mut state, &[int(10), int(0)]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    #[test]
    fn test_nth_value_rejects_negative_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        let err = func.step(&mut state, &[int(10), int(-1)]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    #[test]
    fn test_nth_value_accepts_integral_real_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        func.step(&mut state, &[int(10), float(2.0)]).unwrap();
        func.step(&mut state, &[int(20), float(2.0)]).unwrap();
        assert_eq!(func.value(&state).unwrap(), int(20));
    }

    #[test]
    fn test_nth_value_accepts_integral_text_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        func.step(&mut state, &[int(10), text("2e0")]).unwrap();
        func.step(&mut state, &[int(20), text("2.0")]).unwrap();
        assert_eq!(func.value(&state).unwrap(), int(20));
    }

    #[test]
    fn test_nth_value_rejects_fractional_real_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        let err = func.step(&mut state, &[int(10), float(1.5)]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    #[test]
    fn test_nth_value_rejects_fractional_text_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        let err = func.step(&mut state, &[int(10), text("1.5")]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    #[test]
    fn test_nth_value_rejects_text_numeric_prefix_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        let err = func.step(&mut state, &[int(10), text("2x")]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    #[test]
    fn test_nth_value_rejects_blob_integer_n() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        let err = func.step(&mut state, &[int(10), blob(b"2")]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    #[test]
    fn test_nth_value_rejects_non_positive_n_after_first_row() {
        let func = NthValueFunc;
        let mut state = func.initial_state();
        func.step(&mut state, &[int(10), int(1)]).unwrap();
        let err = func.step(&mut state, &[int(20), int(0)]).unwrap_err();
        assert_function_error(err, INVALID_NTH_VALUE_ARGUMENT);
    }

    // ── sum / total / avg ────────────────────────────────────────────

    #[test]
    fn test_window_sum_text_integer_literals_stay_integer() {
        let results = run_window_partition(&WindowSumFunc, &[vec![text("1")], vec![text("2")]]);
        assert_eq!(results, vec![int(1), int(3)]);
    }

    #[test]
    fn test_window_sum_non_numeric_text_returns_real_zero() {
        let results = run_window_partition(&WindowSumFunc, &[vec![text("abc")]]);
        assert_eq!(results, vec![float(0.0)]);
    }

    #[test]
    fn test_window_sum_overflow_suppressed_after_float_input() {
        let func = WindowSumFunc;
        let mut state = func.initial_state();

        func.step(&mut state, &[int(i64::MAX)]).unwrap();
        func.step(&mut state, &[int(1)]).unwrap();
        assert_eq!(
            func.value(&state).unwrap_err().to_string(),
            "integer overflow"
        );

        func.step(&mut state, &[float(0.5)]).unwrap();
        assert!(matches!(func.value(&state).unwrap(), SqliteValue::Float(_)));
    }

    // ── min / max ────────────────────────────────────────────────────

    #[test]
    fn test_window_min_max_use_sqlite_storage_class_order() {
        let text_value = text("z");
        let blob_value = blob(b"\0");

        let mut min_state = WindowMinFunc.initial_state();
        WindowMinFunc
            .step(&mut min_state, std::slice::from_ref(&blob_value))
            .unwrap();
        WindowMinFunc
            .step(&mut min_state, std::slice::from_ref(&text_value))
            .unwrap();
        assert_eq!(WindowMinFunc.value(&min_state).unwrap(), text_value);

        let mut max_state = WindowMaxFunc.initial_state();
        WindowMaxFunc
            .step(&mut max_state, std::slice::from_ref(&text_value))
            .unwrap();
        WindowMaxFunc
            .step(&mut max_state, std::slice::from_ref(&blob_value))
            .unwrap();
        assert_eq!(WindowMaxFunc.value(&max_state).unwrap(), blob_value);
    }

    // ── group_concat / string_agg ────────────────────────────────────

    #[test]
    fn test_window_group_concat_running_default_separator() {
        let results = run_window_partition(
            &WindowGroupConcatFunc,
            &[vec![text("a")], vec![text("b")], vec![text("c")]],
        );
        assert_eq!(results, vec![text("a"), text("a,b"), text("a,b,c")]);
    }

    #[test]
    fn test_window_group_concat_running_custom_separator() {
        let results = run_window_partition(
            &WindowGroupConcatFunc,
            &[
                vec![text("a"), text(" | ")],
                vec![text("b"), text(" | ")],
                vec![text("c"), text(" | ")],
            ],
        );
        assert_eq!(results, vec![text("a"), text("a | b"), text("a | b | c")]);
    }

    #[test]
    fn test_window_group_concat_skips_null_and_uses_current_row_separator() {
        let results = run_window_partition(
            &WindowGroupConcatFunc,
            &[
                vec![text("a"), text("-")],
                vec![null(), text("?")],
                vec![text("b"), text("+")],
                vec![text("c"), text("*")],
            ],
        );
        assert_eq!(
            results,
            vec![text("a"), text("a"), text("a+b"), text("a+b*c")]
        );
    }

    #[test]
    fn test_window_string_agg_alias_through_registry() {
        let mut reg = FunctionRegistry::new();
        register_window_builtins(&mut reg);

        let sa = reg.find_window("string_agg", 2).unwrap();
        let mut state = sa.initial_state();
        sa.step(&mut state, &[text("a"), text(";")]).unwrap();
        assert_eq!(sa.value(&state).unwrap(), text("a"));
        sa.step(&mut state, &[text("b"), text(";")]).unwrap();
        assert_eq!(sa.value(&state).unwrap(), text("a;b"));
    }

    // ── Registration ─────────────────────────────────────────────────

    #[test]
    fn test_register_window_builtins_all_present() {
        let mut reg = FunctionRegistry::new();
        register_window_builtins(&mut reg);

        let expected_variadic = [
            "row_number",
            "rank",
            "dense_rank",
            "percent_rank",
            "cume_dist",
            "lag",
            "lead",
        ];
        for name in expected_variadic {
            assert!(
                reg.find_window(name, 0).is_some()
                    || reg.find_window(name, 1).is_some()
                    || reg.find_window(name, -1).is_some(),
                "window function '{name}' not registered"
            );
        }

        assert!(
            reg.find_window("ntile", 1).is_some(),
            "ntile(1) not registered"
        );
        assert!(
            reg.find_window("first_value", 1).is_some(),
            "first_value(1) not registered"
        );
        assert!(
            reg.find_window("last_value", 1).is_some(),
            "last_value(1) not registered"
        );
        assert!(
            reg.find_window("nth_value", 2).is_some(),
            "nth_value(2) not registered"
        );
        assert!(
            reg.find_window("group_concat", 1).is_some(),
            "group_concat(1) not registered"
        );
        assert!(
            reg.find_window("group_concat", 2).is_some(),
            "group_concat(2) not registered"
        );
        assert!(
            reg.find_window("string_agg", 2).is_some(),
            "string_agg(2) not registered"
        );

        for (name, arity) in [
            ("rank", 1),
            ("dense_rank", 1),
            ("percent_rank", 1),
            ("cume_dist", 1),
            ("lag", 0),
            ("lag", 4),
            ("lead", 0),
            ("lead", 4),
            ("count", 2),
            ("group_concat", 0),
            ("group_concat", 3),
        ] {
            let f = reg
                .find_window(name, arity)
                .expect("known window with wrong arity returns erroring window");
            let mut state = f.initial_state();
            let err = f
                .step(&mut state, &[])
                .expect_err("invalid window arity should fail");
            let expected = format!("wrong number of arguments to function {name}()");
            assert!(
                matches!(&err, FrankenError::FunctionError(message) if message == &expected),
                "unexpected error for {name}/{arity}: {err:?}"
            );
        }
    }

    // ── E2E: full lifecycle through registry ─────────────────────────

    #[test]
    fn test_e2e_window_row_number_through_registry() {
        let mut reg = FunctionRegistry::new();
        register_window_builtins(&mut reg);

        let rn = reg.find_window("row_number", 0).unwrap();
        let mut state = rn.initial_state();
        rn.step(&mut state, &[]).unwrap();
        assert_eq!(rn.value(&state).unwrap(), int(1));
        rn.step(&mut state, &[]).unwrap();
        assert_eq!(rn.value(&state).unwrap(), int(2));
        rn.step(&mut state, &[]).unwrap();
        assert_eq!(rn.value(&state).unwrap(), int(3));
    }

    #[test]
    fn test_e2e_window_rank_through_registry() {
        let mut reg = FunctionRegistry::new();
        register_window_builtins(&mut reg);

        let rank = reg.find_window("rank", 0).unwrap();
        let mut state = rank.initial_state();
        // [1, 2, 2, 3] -> [1, 2, 2, 4]
        rank.step(&mut state, &[int(1)]).unwrap();
        assert_eq!(rank.value(&state).unwrap(), int(1));
        rank.step(&mut state, &[int(2)]).unwrap();
        assert_eq!(rank.value(&state).unwrap(), int(2));
        rank.step(&mut state, &[int(2)]).unwrap();
        assert_eq!(rank.value(&state).unwrap(), int(2));
        rank.step(&mut state, &[int(3)]).unwrap();
        assert_eq!(rank.value(&state).unwrap(), int(4));
    }
}
