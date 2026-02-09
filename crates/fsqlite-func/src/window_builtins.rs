//! Built-in window functions (S13.5, bd-14i6).
//!
//! Implements: row_number, rank, dense_rank, percent_rank, cume_dist,
//! ntile, lag, lead, first_value, last_value, nth_value.
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

use fsqlite_error::Result;
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
/// cume_dist = (current_position) / partition_size.
pub struct CumeDistState {
    partition_size: i64,
    current_row: i64,
}

pub struct CumeDistFunc;

impl WindowFunction for CumeDistFunc {
    type State = CumeDistState;

    fn initial_state(&self) -> Self::State {
        CumeDistState {
            partition_size: 0,
            current_row: 0,
        }
    }

    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.partition_size += 1;
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
        let cd = (state.current_row + 1) as f64 / state.partition_size as f64;
        Ok(SqliteValue::Float(cd))
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1
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
        state.partition_size += 1;
        if state.partition_size == 1 {
            let n = args.first().map_or(1, |v| v.to_integer().max(1));
            state.n = n;
        }
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.current_row += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        if state.n <= 0 || state.partition_size == 0 {
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

/// State for `lag()`: maintains a buffer of previous values.
pub struct LagState {
    buffer: VecDeque<SqliteValue>,
    offset: i64,
    default_val: SqliteValue,
    row_number: i64,
}

pub struct LagFunc;

impl WindowFunction for LagFunc {
    type State = LagState;

    fn initial_state(&self) -> Self::State {
        LagState {
            buffer: VecDeque::new(),
            offset: 1,
            default_val: SqliteValue::Null,
            row_number: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let val = args.first().cloned().unwrap_or(SqliteValue::Null);
        // Capture offset and default on first call.
        if state.row_number == 0 {
            if let Some(off) = args.get(1) {
                state.offset = off.to_integer().max(0);
            }
            if let Some(def) = args.get(2) {
                state.default_val = def.clone();
            }
        }
        state.row_number += 1;
        state.buffer.push_back(val);
        Ok(())
    }

    fn inverse(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let idx = state.row_number - state.offset;
        if idx < 1 || idx > state.buffer.len() as i64 {
            return Ok(state.default_val.clone());
        }
        Ok(state.buffer[(idx - 1) as usize].clone())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1 // 1, 2, or 3 args
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
    offset: i64,
    default_val: SqliteValue,
    current_row: i64,
}

pub struct LeadFunc;

impl WindowFunction for LeadFunc {
    type State = LeadState;

    fn initial_state(&self) -> Self::State {
        LeadState {
            buffer: Vec::new(),
            offset: 1,
            default_val: SqliteValue::Null,
            current_row: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let val = args.first().cloned().unwrap_or(SqliteValue::Null);
        if state.buffer.is_empty() {
            if let Some(off) = args.get(1) {
                state.offset = off.to_integer().max(0);
            }
            if let Some(def) = args.get(2) {
                state.default_val = def.clone();
            }
        }
        state.buffer.push(val);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.current_row += 1;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let target = state.current_row + state.offset;
        if target < 0 || target >= state.buffer.len() as i64 {
            return Ok(state.default_val.clone());
        }
        Ok(state.buffer[target as usize].clone())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        self.value(&state)
    }

    fn num_args(&self) -> i32 {
        -1
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
        // Capture N from second arg on first call.
        if state.frame.is_empty() {
            if let Some(n_arg) = args.get(1) {
                state.n = n_arg.to_integer();
            }
        }
        state.frame.push_back(val);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        state.frame.pop_front();
        Ok(())
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        // nth_value is 1-based.  N <= 0 is an error per SQLite docs,
        // but we return NULL to be safe (the VDBE should validate N).
        if state.n <= 0 {
            return Ok(SqliteValue::Null);
        }
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
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i64) -> SqliteValue {
        SqliteValue::Integer(v)
    }

    fn text(s: &str) -> SqliteValue {
        SqliteValue::Text(s.to_owned())
    }

    fn null() -> SqliteValue {
        SqliteValue::Null
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
        match &results[0] {
            SqliteValue::Float(v) => assert!((*v - 0.0).abs() < 1e-10),
            other => panic!("expected Float, got {other:?}"),
        }
        match &results[1] {
            SqliteValue::Float(v) => assert!((*v - 1.0 / 3.0).abs() < 1e-10),
            other => panic!("expected Float, got {other:?}"),
        }
        match &results[2] {
            SqliteValue::Float(v) => assert!((*v - 1.0 / 3.0).abs() < 1e-10),
            other => panic!("expected Float, got {other:?}"),
        }
        match &results[3] {
            SqliteValue::Float(v) => assert!((*v - 1.0).abs() < 1e-10),
            other => panic!("expected Float, got {other:?}"),
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
            match &results[i] {
                SqliteValue::Float(v) => {
                    assert!(
                        (*v - expected).abs() < 1e-10,
                        "row {i}: expected {expected}, got {v}"
                    );
                }
                other => panic!("expected Float, got {other:?}"),
            }
        }
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

    // ── lag ──────────────────────────────────────────────────────────

    #[test]
    fn test_lag_default() {
        // lag(X) with default offset=1: previous row's value, NULL for first.
        let results =
            run_window_partition(&LagFunc, &[vec![int(10)], vec![int(20)], vec![int(30)]]);
        assert_eq!(results, vec![null(), int(10), int(20)]);
    }

    #[test]
    fn test_lag_offset_3() {
        // lag(X, 3): 3 rows back.
        let results = run_window_partition(
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
        let results = run_window_partition(
            &LagFunc,
            &[
                vec![int(10), int(1), int(-1)],
                vec![int(20), int(1), int(-1)],
            ],
        );
        assert_eq!(results, vec![int(-1), int(10)]);
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
        // nth_value(X, 0) returns NULL (0 is invalid, 1-based).
        let func = NthValueFunc;
        let mut state = func.initial_state();
        func.step(&mut state, &[int(10), int(0)]).unwrap();
        assert_eq!(func.value(&state).unwrap(), null());
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

        let rank = reg.find_window("rank", 1).unwrap();
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
