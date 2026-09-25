//! Built-in aggregate functions (§13.4).
//!
//! Implements: avg, count, group_concat, string_agg, max, min, sum, total,
//! median, percentile, percentile_cont, percentile_disc.
//!
//! # NULL handling
//! All aggregate functions skip NULL values (except `count(*)` which counts
//! all rows). Empty-set behavior:
//! - avg / sum / max / min / median → NULL
//! - total → 0.0
//! - count → 0
#![allow(
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::match_same_arms,
    clippy::items_after_statements,
    clippy::float_cmp,
    clippy::cast_sign_loss,
    clippy::suboptimal_flops
)]

use std::cmp::Ordering;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;

use crate::builtins::statement_text_encoding;
use crate::{AggregateFunction, FunctionRegistry};

// ─── Kahan compensated summation ──────────────────────────────────────────

/// Kahan-Babuska-Neumaier compensated summation step.  Uses magnitude-aware
/// error term selection to match the precision behavior of C SQLite's
/// `kahanBabuskaNeumaierStep` aggregate helper.
#[inline]
fn kahan_add(sum: &mut f64, compensation: &mut f64, value: f64) {
    let s = *sum;
    let t = s + value;
    // Compensation is only meaningful for finite arithmetic. In particular,
    // inf - inf in the error term must not turn a valid infinite sum into NaN.
    // Opposite infinities still produce NaN, normalized to SQL NULL at output.
    if !t.is_finite() {
        *sum = t;
        *compensation = 0.0;
        return;
    }
    if s.abs() > value.abs() {
        *compensation += (s - t) + value;
    } else {
        *compensation += (value - t) + s;
    }
    *sum = t;
}

/// Preserve the low bits of an i64 when entering floating-point accumulation.
/// Removing 14 low bits leaves at most 49 significant bits, exactly representable
/// in f64. Subtracting the signed remainder is safe even for i64::MIN/MAX.
#[inline]
fn split_sum_integer(value: i64) -> (f64, f64) {
    if (-4_503_599_627_370_496..4_503_599_627_370_496).contains(&value) {
        (value as f64, 0.0)
    } else {
        let low = value % 16_384;
        ((value - low) as f64, low as f64)
    }
}

#[inline]
fn kahan_add_integer(sum: &mut f64, compensation: &mut f64, value: i64) {
    let (high, low) = split_sum_integer(value);
    kahan_add(sum, compensation, high);
    if low != 0.0 {
        kahan_add(sum, compensation, low);
    }
}

fn aggregate_float(value: f64) -> SqliteValue {
    if value.is_nan() {
        SqliteValue::Null
    } else {
        SqliteValue::Float(value)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// avg(X)
// ═══════════════════════════════════════════════════════════════════════════

pub struct AvgState {
    sum: SumState,
    count: i64,
}

pub struct AvgFunc;

impl AggregateFunction for AvgFunc {
    type State = AvgState;

    fn initial_state(&self) -> Self::State {
        AvgState {
            sum: SumFunc.initial_state(),
            count: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if state.sum.add_value(&args[0]) {
            state.count += 1;
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        if state.count == 0 {
            Ok(SqliteValue::Null)
        } else {
            Ok(aggregate_float(state.sum.real_total() / state.count as f64))
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "avg"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// count(*) and count(X)
// ═══════════════════════════════════════════════════════════════════════════

/// `count(*)` — counts all rows including those with NULL values.
pub struct CountStarFunc;

impl AggregateFunction for CountStarFunc {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> Result<()> {
        *state += 1;
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        0 // count(*) takes no column argument
    }

    fn name(&self) -> &str {
        "count"
    }
}

/// `count(X)` — counts non-NULL values of X.
pub struct CountFunc;

impl AggregateFunction for CountFunc {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if !args[0].is_null() {
            *state += 1;
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "count"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// group_concat(X [, SEP])
// ═══════════════════════════════════════════════════════════════════════════

pub struct GroupConcatState {
    /// Incrementally built result string.  C SQLite appends
    /// `separator + value` at each step (separator only before 2nd+ value),
    /// using the separator from *that row's* argument, not a single global one.
    result: String,
    has_value: bool,
}

pub struct GroupConcatFunc;

#[inline]
fn push_group_concat_text(result: &mut String, value: &SqliteValue) {
    if let Some(text) = value.as_text_str() {
        result.push_str(text);
    } else {
        result.push_str(&value.to_text());
    }
}

impl AggregateFunction for GroupConcatFunc {
    type State = GroupConcatState;

    fn initial_state(&self) -> Self::State {
        GroupConcatState {
            result: String::new(),
            has_value: false,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args[0].is_null() {
            return Ok(());
        }
        if state.has_value {
            match args.get(1) {
                Some(separator) if !separator.is_null() => {
                    push_group_concat_text(&mut state.result, separator);
                }
                Some(_) => {}
                None => state.result.push(','),
            }
        }
        push_group_concat_text(&mut state.result, &args[0]);
        state.has_value = true;
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        if state.has_value {
            Ok(SqliteValue::Text(state.result.into()))
        } else {
            Ok(SqliteValue::Null)
        }
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
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

// ═══════════════════════════════════════════════════════════════════════════
// max(X) — aggregate, single arg
// ═══════════════════════════════════════════════════════════════════════════

pub struct AggMaxFunc;

impl AggregateFunction for AggMaxFunc {
    type State = Option<SqliteValue>;

    fn initial_state(&self) -> Self::State {
        None
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args[0].is_null() {
            return Ok(());
        }
        let candidate = &args[0];
        match state {
            None => *state = Some(candidate.clone()),
            Some(current) => {
                if candidate.cmp_binary_in(current, statement_text_encoding())
                    == Ordering::Greater
                {
                    *state = Some(candidate.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "max"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// min(X) — aggregate, single arg
// ═══════════════════════════════════════════════════════════════════════════

pub struct AggMinFunc;

impl AggregateFunction for AggMinFunc {
    type State = Option<SqliteValue>;

    fn initial_state(&self) -> Self::State {
        None
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args[0].is_null() {
            return Ok(());
        }
        let candidate = &args[0];
        match state {
            None => *state = Some(candidate.clone()),
            Some(current) => {
                if candidate.cmp_binary_in(current, statement_text_encoding()) == Ordering::Less {
                    *state = Some(candidate.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(state.unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "min"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// sum(X)
// ═══════════════════════════════════════════════════════════════════════════

/// State for `sum()`: tracks whether all values are integers, the running
/// integer sum, and the float sum as fallback.  Uses Kahan compensated
/// summation for the float path to match C SQLite's precision.
pub struct SumState {
    int_sum: i64,
    float_sum: f64,
    float_compensation: f64,
    all_integer: bool,
    has_values: bool,
    overflowed: bool,
}

impl SumState {
    /// Return whether a non-NULL input was accumulated. SUM, TOTAL and AVG
    /// share both numeric coercion and the exact-integer prefix; AVG alone
    /// counts inputs, and SUM alone reports an all-integer overflow.
    fn add_value(&mut self, value: &SqliteValue) -> bool {
        let value = value.to_sum_numeric_value();
        if value.is_null() || matches!(value, SqliteValue::Float(v) if v.is_nan()) {
            return false;
        }
        self.has_values = true;
        let exact = self.all_integer && !self.overflowed;
        match value {
            SqliteValue::Integer(value) => {
                if exact {
                    if let Some(total) = self.int_sum.checked_add(value) {
                        self.int_sum = total;
                        return true;
                    }
                    (self.float_sum, self.float_compensation) = split_sum_integer(self.int_sum);
                    self.overflowed = true;
                }
                kahan_add_integer(&mut self.float_sum, &mut self.float_compensation, value);
            }
            SqliteValue::Float(value) => {
                if exact {
                    (self.float_sum, self.float_compensation) = split_sum_integer(self.int_sum);
                }
                self.all_integer = false;
                kahan_add(&mut self.float_sum, &mut self.float_compensation, value);
            }
            SqliteValue::Null | SqliteValue::Text(_) | SqliteValue::Blob(_) => {}
        }
        true
    }

    fn real_total(&self) -> f64 {
        if self.all_integer && !self.overflowed {
            self.int_sum as f64
        } else {
            self.float_sum + self.float_compensation
        }
    }
}

pub struct SumFunc;

impl AggregateFunction for SumFunc {
    type State = SumState;

    fn initial_state(&self) -> Self::State {
        SumState {
            int_sum: 0,
            float_sum: 0.0,
            float_compensation: 0.0,
            all_integer: true,
            has_values: false,
            overflowed: false,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.add_value(&args[0]);
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        if !state.has_values {
            return Ok(SqliteValue::Null);
        }
        if state.all_integer && state.overflowed {
            return Err(FrankenError::IntegerOverflow);
        }
        if state.all_integer {
            Ok(SqliteValue::Integer(state.int_sum))
        } else {
            Ok(aggregate_float(state.real_total()))
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sum"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// total(X) — always returns float, 0.0 for empty set, never overflows.
// ═══════════════════════════════════════════════════════════════════════════

pub struct TotalFunc;

/// State for `total()`: the shared exact/compensated accumulator without SUM's
/// integer-overflow error at finalization.
pub struct TotalState {
    sum: SumState,
}

impl AggregateFunction for TotalFunc {
    type State = TotalState;

    fn initial_state(&self) -> Self::State {
        TotalState {
            sum: SumFunc.initial_state(),
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.sum.add_value(&args[0]);
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(aggregate_float(state.sum.real_total()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "total"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// median(X) — equivalent to percentile_cont(X, 0.5)
// ═══════════════════════════════════════════════════════════════════════════

pub struct MedianFunc;

impl AggregateFunction for MedianFunc {
    type State = Vec<f64>;

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if let Some(value) = percentile_input(&args[0], self.name())? {
            state.push(value);
        }
        Ok(())
    }

    fn finalize(&self, mut state: Self::State) -> Result<SqliteValue> {
        if state.is_empty() {
            return Ok(SqliteValue::Null);
        }
        state.sort_unstable_by(f64::total_cmp);
        Ok(SqliteValue::Float(percentile_cont_impl(&state, 0.5)))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "median"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// percentile(Y, P) — P in 0..100
// ═══════════════════════════════════════════════════════════════════════════

pub struct PercentileState {
    values: Vec<f64>,
    /// The first row's fraction, normalized to [0, 1] for all three functions.
    /// NULL Y rows still establish and validate the fraction.
    p: Option<f64>,
}

pub struct PercentileFunc;

impl AggregateFunction for PercentileFunc {
    type State = PercentileState;

    fn initial_state(&self) -> Self::State {
        PercentileState {
            values: Vec::new(),
            p: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        percentile_step(state, args, 100.0, self.name())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        percentile_finalize(state, false)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "percentile"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// percentile_cont(Y, P) — P in 0..1, continuous interpolation
// ═══════════════════════════════════════════════════════════════════════════

pub struct PercentileContFunc;

impl AggregateFunction for PercentileContFunc {
    type State = PercentileState;

    fn initial_state(&self) -> Self::State {
        PercentileState {
            values: Vec::new(),
            p: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        percentile_step(state, args, 1.0, self.name())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        percentile_finalize(state, false)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "percentile_cont"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// percentile_disc(Y, P) — P in 0..1, discrete (returns actual value)
// ═══════════════════════════════════════════════════════════════════════════

pub struct PercentileDiscFunc;

impl AggregateFunction for PercentileDiscFunc {
    type State = PercentileState;

    fn initial_state(&self) -> Self::State {
        PercentileState {
            values: Vec::new(),
            p: None,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        percentile_step(state, args, 1.0, self.name())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        percentile_finalize(state, true)
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "percentile_disc"
    }
}

// ── Shared percentile helpers ─────────────────────────────────────────────

/// Percentile data must have numeric storage class: unlike SUM, numeric text
/// and blobs are not coerced. SQLite normalizes an input NaN to NULL.
fn percentile_input(value: &SqliteValue, name: &str) -> Result<Option<f64>> {
    match value {
        SqliteValue::Null => Ok(None),
        SqliteValue::Integer(value) => Ok(Some(*value as f64)),
        SqliteValue::Float(value) if value.is_nan() => Ok(None),
        SqliteValue::Float(value) if value.is_finite() => Ok(Some(*value)),
        SqliteValue::Float(_) => Err(FrankenError::FunctionError(format!(
            "Inf input to {name}()"
        ))),
        SqliteValue::Text(_) | SqliteValue::Blob(_) => Err(FrankenError::FunctionError(format!(
            "input to {name}() is not numeric"
        ))),
    }
}

fn percentile_step(
    state: &mut PercentileState,
    args: &[SqliteValue],
    scale: f64,
    name: &str,
) -> Result<()> {
    // P follows SQLite's numeric-type conversion (which accepts fully numeric
    // TEXT), not SUM's permissive prefix conversion or Y's storage-class rule.
    let p = match args[1]
        .clone()
        .apply_affinity(fsqlite_types::TypeAffinity::Numeric)
    {
        SqliteValue::Integer(value) => value as f64 / scale,
        SqliteValue::Float(value) => value / scale,
        SqliteValue::Null | SqliteValue::Text(_) | SqliteValue::Blob(_) => f64::NAN,
    };
    if !p.is_finite() || !(0.0..=1.0).contains(&p) {
        return Err(FrankenError::FunctionError(format!(
            "the fraction argument to {name}() is not between 0.0 and {scale:.1}"
        )));
    }
    // SQLite compares normalized fractions against the FIRST row, including
    // rows whose Y is NULL. Do not update the baseline and allow gradual drift.
    if state.p.is_some_and(|first| (first - p).abs() > 0.001) {
        return Err(FrankenError::FunctionError(format!(
            "the fraction argument to {name}() is not the same for all input rows"
        )));
    }
    let value = percentile_input(&args[0], name)?;
    if state.p.is_none() {
        state.p = Some(p);
    }
    if let Some(value) = value {
        state.values.push(value);
    }
    Ok(())
}

fn percentile_finalize(mut state: PercentileState, discrete: bool) -> Result<SqliteValue> {
    if state.values.is_empty() {
        return Ok(SqliteValue::Null);
    }
    let p = state.p.ok_or_else(|| {
        FrankenError::FunctionError("percentile fraction was not initialized".to_owned())
    })?;
    state.values.sort_unstable_by(f64::total_cmp);
    let result = if discrete {
        // SQLite takes the lower endpoint of the continuous rank, not the
        // nearest-rank definition ceil(P*N)-1 used by some other databases.
        let index = (p * (state.values.len() - 1) as f64).floor() as usize;
        state.values[index]
    } else {
        percentile_cont_impl(&state.values, p)
    };
    Ok(SqliteValue::Float(result))
}

/// Continuous percentile with linear interpolation.
/// `sorted` must be nonempty and sorted ascending; `p` has been validated.
fn percentile_cont_impl(sorted: &[f64], p: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    debug_assert!((0.0..=1.0).contains(&p));
    let rank = p * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let frac = rank - lower as f64;
        // Weighted endpoints avoid overflowing the difference between large
        // finite values of opposite signs.
        sorted[lower] * (1.0 - frac) + sorted[upper] * frac
    }
}

// ── Registration ──────────────────────────────────────────────────────────

/// Register all §13.4 aggregate functions into the given registry.
pub fn register_aggregate_builtins(registry: &mut FunctionRegistry) {
    registry.register_aggregate(AvgFunc);
    registry.register_aggregate(CountStarFunc);
    registry.register_aggregate(CountFunc);
    registry.register_aggregate(GroupConcatFunc);
    registry.register_aggregate(AggMaxFunc);
    registry.register_aggregate(AggMinFunc);
    registry.register_aggregate(SumFunc);
    registry.register_aggregate(TotalFunc);
    registry.register_aggregate(MedianFunc);
    registry.register_aggregate(PercentileFunc);
    registry.register_aggregate(PercentileContFunc);
    registry.register_aggregate(PercentileDiscFunc);

    // string_agg is an alias for group_concat with mandatory separator.
    struct StringAggFunc;
    impl AggregateFunction for StringAggFunc {
        type State = GroupConcatState;

        fn initial_state(&self) -> Self::State {
            GroupConcatState {
                result: String::new(),
                has_value: false,
            }
        }

        fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
            GroupConcatFunc.step(state, args)
        }

        fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
            GroupConcatFunc.finalize(state)
        }

        fn num_args(&self) -> i32 {
            2 // string_agg requires separator
        }

        fn name(&self) -> &str {
            "string_agg"
        }
    }
    registry.register_aggregate(StringAggFunc);
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;

    fn int(v: i64) -> SqliteValue {
        SqliteValue::Integer(v)
    }

    fn float(v: f64) -> SqliteValue {
        SqliteValue::Float(v)
    }

    fn null() -> SqliteValue {
        SqliteValue::Null
    }

    fn text(s: &str) -> SqliteValue {
        SqliteValue::Text(s.into())
    }

    fn assert_float_eq(result: &SqliteValue, expected: f64) {
        match result {
            SqliteValue::Float(v) => {
                assert!((v - expected).abs() < EPS, "expected {expected}, got {v}");
            }
            other => {
                assert!(
                    matches!(other, SqliteValue::Float(_)),
                    "expected Float({expected}), got {other:?}"
                );
            }
        }
    }

    /// Helper: run an aggregate over a list of single-arg row values.
    fn run_agg<F: AggregateFunction>(func: &F, rows: &[SqliteValue]) -> SqliteValue {
        let mut state = func.initial_state();
        for row in rows {
            func.step(&mut state, std::slice::from_ref(row)).unwrap();
        }
        func.finalize(state).unwrap()
    }

    /// Helper: run an aggregate over a list of two-arg row values.
    fn run_agg2<F: AggregateFunction>(
        func: &F,
        rows: &[(SqliteValue, SqliteValue)],
    ) -> SqliteValue {
        let mut state = func.initial_state();
        for (a, b) in rows {
            func.step(&mut state, &[a.clone(), b.clone()]).unwrap();
        }
        func.finalize(state).unwrap()
    }

    #[test]
    fn test_aggregate_oracle_edges_2026_08() {
        // Oracle: sqlite3 3.46.1. sum() of integers stays integer and ERRORS on
        // i64 overflow (does NOT promote to real); total() is always real and 0.0
        // for an empty/all-NULL input; sum() of all-NULL -> NULL; count() skips
        // NULLs; max()/min() use SQLite's storage-class ordering (int sorts before
        // text) and ignore NULLs; group_concat default separator is ",".
        assert_eq!(run_agg(&SumFunc, &[int(1), int(2)]), int(3));
        assert_eq!(run_agg(&SumFunc, &[null(), null()]), null());
        assert_eq!(
            run_agg(&TotalFunc, &[int(1), int(2)]),
            SqliteValue::Float(3.0)
        );
        assert_eq!(
            run_agg(&TotalFunc, &[null(), null()]),
            SqliteValue::Float(0.0)
        );
        assert_float_eq(&run_agg(&AvgFunc, &[int(1), int(2)]), 1.5);
        assert_eq!(run_agg(&CountFunc, &[int(1), null(), int(3)]), int(2));
        assert_eq!(
            run_agg(&AggMaxFunc, &[int(1), text("a"), null()]),
            text("a")
        );
        assert_eq!(run_agg(&AggMinFunc, &[int(1), text("a"), null()]), int(1));
        assert_eq!(
            run_agg(&GroupConcatFunc, &[int(1), int(2), int(3)]),
            text("1,2,3")
        );

        // sum() of all-integer input ERRORS on i64 overflow rather than wrapping
        // or promoting to real (C SQLite raises "integer overflow" while stepping;
        // frank raises it at finalize — observationally identical for a query).
        let sum = SumFunc;
        let mut st = sum.initial_state();
        sum.step(&mut st, &[int(i64::MAX)]).unwrap();
        sum.step(&mut st, &[int(1)]).unwrap();
        assert!(
            sum.finalize(st).is_err(),
            "sum() must error on i64 overflow"
        );
        // But a real value in the mix switches to float accumulation (no error),
        // matching C SQLite's `approx` flag.
        let mut st2 = sum.initial_state();
        sum.step(&mut st2, &[int(i64::MAX)]).unwrap();
        sum.step(&mut st2, &[int(1)]).unwrap();
        sum.step(&mut st2, &[float(0.5)]).unwrap();
        assert!(
            matches!(sum.finalize(st2), Ok(SqliteValue::Float(_))),
            "sum() with a real present returns the float sum, not an overflow error"
        );
    }

    // ── avg ───────────────────────────────────────────────────────────

    #[test]
    fn test_avg_basic() {
        let r = run_agg(&AvgFunc, &[int(1), int(2), int(3), int(4), int(5)]);
        assert_float_eq(&r, 3.0);
    }

    #[test]
    fn test_avg_with_nulls() {
        let r = run_agg(&AvgFunc, &[int(1), null(), int(3)]);
        assert_float_eq(&r, 2.0);
    }

    #[test]
    fn test_avg_empty() {
        let r = run_agg(&AvgFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
    }

    #[test]
    fn test_avg_returns_real() {
        let r = run_agg(&AvgFunc, &[int(2), int(4)]);
        assert!(matches!(r, SqliteValue::Float(_)));
    }

    // ── count ─────────────────────────────────────────────────────────

    #[test]
    fn test_count_star() {
        // count(*) counts all rows including NULLs.
        let mut state = CountStarFunc.initial_state();
        CountStarFunc.step(&mut state, &[]).unwrap(); // row 1
        CountStarFunc.step(&mut state, &[]).unwrap(); // row 2
        CountStarFunc.step(&mut state, &[]).unwrap(); // row 3
        let r = CountStarFunc.finalize(state).unwrap();
        assert_eq!(r, int(3));
    }

    #[test]
    fn test_count_column() {
        let r = run_agg(&CountFunc, &[int(1), null(), int(3), null(), int(5)]);
        assert_eq!(r, int(3));
    }

    #[test]
    fn test_count_empty() {
        let r = run_agg(&CountFunc, &[]);
        assert_eq!(r, int(0));
    }

    // ── group_concat ──────────────────────────────────────────────────

    #[test]
    fn test_group_concat_basic() {
        let r = run_agg(&GroupConcatFunc, &[text("a"), text("b"), text("c")]);
        assert_eq!(r, SqliteValue::Text("a,b,c".into()));
    }

    #[test]
    fn test_group_concat_custom_sep() {
        let rows = vec![
            (text("a"), text("; ")),
            (text("b"), text("; ")),
            (text("c"), text("; ")),
        ];
        let r = run_agg2(&GroupConcatFunc, &rows);
        assert_eq!(r, SqliteValue::Text("a; b; c".into()));
    }

    #[test]
    fn test_group_concat_null_skipped() {
        let r = run_agg(&GroupConcatFunc, &[text("a"), null(), text("c")]);
        assert_eq!(r, SqliteValue::Text("a,c".into()));
    }

    #[test]
    fn test_group_concat_empty() {
        let r = run_agg(&GroupConcatFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
    }

    #[test]
    fn test_group_concat_varying_separator() {
        // C SQLite uses the separator from each row's argument, not a single
        // global separator. SELECT group_concat(val, sep) with varying sep
        // produces a+b*c, not a*b*c (the old bug used the last-seen sep).
        let rows = vec![
            (text("a"), text("-")),
            (text("b"), text("+")),
            (text("c"), text("*")),
        ];
        let r = run_agg2(&GroupConcatFunc, &rows);
        assert_eq!(r, SqliteValue::Text("a+b*c".into()));
    }

    #[test]
    fn test_group_concat_single_value() {
        let r = run_agg(&GroupConcatFunc, &[text("only")]);
        assert_eq!(r, SqliteValue::Text("only".into()));
    }

    #[test]
    fn test_group_concat_integer_values_coerced_to_text() {
        let r = run_agg(&GroupConcatFunc, &[int(1), int(2), int(3)]);
        assert_eq!(r, SqliteValue::Text("1,2,3".into()));
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_group_concat_text_rows() {
        use std::hint::black_box;
        use std::time::Instant;

        const ROWS: usize = 200_000;
        const REPEATS: usize = 5;

        let rows: Vec<SqliteValue> = (0..ROWS).map(|_| text("payload")).collect();
        let mut best_ns = u128::MAX;
        let mut result_len = 0usize;

        for _ in 0..REPEATS {
            let started = Instant::now();
            let result = black_box(run_agg(&GroupConcatFunc, black_box(rows.as_slice())));
            let elapsed_ns = started.elapsed().as_nanos();
            if elapsed_ns < best_ns {
                best_ns = elapsed_ns;
            }
            result_len = match result {
                SqliteValue::Text(text) => text.len(),
                SqliteValue::Null
                | SqliteValue::Integer(_)
                | SqliteValue::Float(_)
                | SqliteValue::Blob(_) => 0,
            };
        }

        println!(
            "group_concat_text_rows rows={ROWS} repeats={REPEATS} best_ns={best_ns} result_len={result_len}"
        );
    }

    // ── max (aggregate) ───────────────────────────────────────────────

    #[test]
    fn test_max_aggregate() {
        let r = run_agg(&AggMaxFunc, &[int(3), int(7), int(1), int(5)]);
        assert_eq!(r, int(7));
    }

    #[test]
    fn test_max_aggregate_null_skipped() {
        let r = run_agg(&AggMaxFunc, &[int(3), null(), int(7), null()]);
        assert_eq!(r, int(7));
    }

    #[test]
    fn test_max_aggregate_empty() {
        let r = run_agg(&AggMaxFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
    }

    // ── min (aggregate) ───────────────────────────────────────────────

    #[test]
    fn test_min_aggregate() {
        let r = run_agg(&AggMinFunc, &[int(3), int(7), int(1), int(5)]);
        assert_eq!(r, int(1));
    }

    #[test]
    fn test_min_aggregate_null_skipped() {
        let r = run_agg(&AggMinFunc, &[int(3), null(), int(1), null()]);
        assert_eq!(r, int(1));
    }

    #[test]
    fn test_min_aggregate_empty() {
        let r = run_agg(&AggMinFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
    }

    // ── sum ───────────────────────────────────────────────────────────

    #[test]
    fn test_sum_integers() {
        let r = run_agg(&SumFunc, &[int(1), int(2), int(3)]);
        assert_eq!(r, int(6));
    }

    #[test]
    fn test_sum_reals() {
        let r = run_agg(&SumFunc, &[float(1.5), float(2.5)]);
        assert_float_eq(&r, 4.0);
    }

    #[test]
    fn test_sum_empty_null() {
        let r = run_agg(&SumFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
    }

    #[test]
    fn test_sum_overflow_error() {
        let mut state = SumFunc.initial_state();
        SumFunc.step(&mut state, &[int(i64::MAX)]).unwrap();
        SumFunc.step(&mut state, &[int(1)]).unwrap();
        let err = SumFunc.finalize(state);
        assert!(err.is_err(), "sum should raise overflow error");
    }

    #[test]
    fn test_sum_later_real_value_clears_integer_overflow_error() {
        let r = run_agg(&SumFunc, &[int(i64::MAX), int(1), float(0.5)]);
        assert_float_eq(&r, 9_223_372_036_854_776_000.0);
    }

    #[test]
    fn test_sum_integer_text_preserves_overflow_error() -> Result<()> {
        let mut state = SumFunc.initial_state();
        SumFunc.step(&mut state, &[text("9223372036854775807")])?;
        SumFunc.step(&mut state, &[text("1")])?;
        let err = SumFunc.finalize(state);
        assert!(err.is_err(), "integer-text sum should raise overflow");
        Ok(())
    }

    #[test]
    fn test_sum_integer_text_later_real_clears_overflow_error() {
        let r = run_agg(
            &SumFunc,
            &[text("9223372036854775807"), text("1"), text("0.5")],
        );
        assert_float_eq(&r, 9_223_372_036_854_776_000.0);
    }

    #[test]
    fn test_sum_prefix_text_uses_real_accumulator() {
        let r = run_agg(&SumFunc, &[text("123abc"), int(1)]);
        assert_float_eq(&r, 124.0);
    }

    #[test]
    fn test_sum_unicode_whitespace_text_uses_sqlite_ascii_space_rules() {
        let leading = run_agg(&SumFunc, &[text("\u{00a0}123"), int(1)]);
        assert_float_eq(&leading, 1.0);

        let trailing = run_agg(&SumFunc, &[text("123\u{00a0}"), int(1)]);
        assert_float_eq(&trailing, 124.0);
    }

    #[test]
    fn test_sum_null_skipped() {
        let r = run_agg(&SumFunc, &[int(1), null(), int(3)]);
        assert_eq!(r, int(4));
    }

    // ── total ─────────────────────────────────────────────────────────

    #[test]
    fn test_total_basic() {
        let r = run_agg(&TotalFunc, &[int(1), int(2), int(3)]);
        assert_float_eq(&r, 6.0);
    }

    #[test]
    fn test_total_empty_zero() {
        let r = run_agg(&TotalFunc, &[]);
        assert_float_eq(&r, 0.0);
    }

    #[test]
    fn test_total_no_overflow() {
        // total uses f64 and never overflows.
        let r = run_agg(&TotalFunc, &[int(i64::MAX), int(i64::MAX)]);
        assert!(matches!(r, SqliteValue::Float(_)));
    }

    // ── shared numeric accumulation ──────────────────────────────────

    #[test]
    fn test_numeric_aggregates_preserve_large_integer_cancellation() {
        // SQLite 3.46.1 oracle: converting each input to f64 first loses the
        // unit difference, even though the exact integer prefix fits in i64.
        for rows in [
            vec![int(i64::MAX), int(i64::MIN), float(0.0)],
            vec![float(0.0), int(i64::MAX), int(i64::MIN)],
            vec![int(i64::MAX), float(0.0), int(i64::MIN)],
            vec![
                text("9223372036854775807"),
                text("-9223372036854775808"),
                float(0.0),
            ],
        ] {
            assert_eq!(run_agg(&SumFunc, &rows), float(-1.0));
            assert_eq!(run_agg(&TotalFunc, &rows), float(-1.0));
            assert_eq!(run_agg(&AvgFunc, &rows), float(-1.0 / 3.0));
        }
        let rows = [
            int(9_007_199_254_740_993),
            int(-9_007_199_254_740_992),
            float(0.0),
        ];
        assert_eq!(run_agg(&SumFunc, &rows), float(1.0));
        assert_eq!(run_agg(&TotalFunc, &rows), float(1.0));
        assert_eq!(run_agg(&AvgFunc, &rows), float(1.0 / 3.0));
    }

    #[test]
    fn test_total_and_avg_keep_exact_integer_prefix() {
        let rows = [int(i64::MAX), int(i64::MIN)];
        assert_eq!(run_agg(&SumFunc, &rows), int(-1));
        assert_eq!(run_agg(&TotalFunc, &rows), float(-1.0));
        assert_eq!(run_agg(&AvgFunc, &rows), float(-0.5));
    }

    #[test]
    fn test_numeric_aggregates_recover_low_bits_after_integer_overflow() {
        for (rows, expected) in [
            (
                vec![int(i64::MAX), int(1), int(i64::MIN), float(0.5)],
                0.5,
            ),
            (
                vec![int(i64::MIN), int(-1), int(i64::MAX), float(0.5)],
                -1.5,
            ),
        ] {
            assert_eq!(run_agg(&SumFunc, &rows), float(expected));
            assert_eq!(run_agg(&TotalFunc, &rows), float(expected));
            assert_eq!(run_agg(&AvgFunc, &rows), float(expected / 4.0));
        }
    }

    #[test]
    fn test_all_integer_overflow_remains_an_error_after_cancellation() {
        let rows = [int(i64::MAX), int(1), int(i64::MIN)];
        let mut state = SumFunc.initial_state();
        for row in &rows {
            SumFunc.step(&mut state, std::slice::from_ref(row)).unwrap();
        }
        assert!(matches!(
            SumFunc.finalize(state),
            Err(FrankenError::IntegerOverflow)
        ));
        assert_eq!(run_agg(&TotalFunc, &rows), float(0.0));
        assert_eq!(run_agg(&AvgFunc, &rows), float(0.0));
    }

    #[test]
    fn test_numeric_aggregates_preserve_signed_infinities() {
        for infinity in [f64::INFINITY, f64::NEG_INFINITY] {
            for rows in [
                vec![float(infinity)],
                vec![float(infinity), float(infinity)],
                vec![int(1), float(infinity), int(-1)],
            ] {
                assert_eq!(run_agg(&SumFunc, &rows), float(infinity));
                assert_eq!(run_agg(&TotalFunc, &rows), float(infinity));
                assert_eq!(run_agg(&AvgFunc, &rows), float(infinity));
            }
        }
    }

    #[test]
    fn test_indeterminate_numeric_aggregates_return_null_not_nan() {
        for rows in [
            vec![float(f64::INFINITY), float(f64::NEG_INFINITY)],
            vec![
                int(1),
                float(f64::NEG_INFINITY),
                int(2),
                float(f64::INFINITY),
                int(3),
            ],
        ] {
            assert_eq!(run_agg(&SumFunc, &rows), null());
            assert_eq!(run_agg(&TotalFunc, &rows), null());
            assert_eq!(run_agg(&AvgFunc, &rows), null());
        }
    }

    #[test]
    fn test_finite_numeric_overflow_preserves_infinity() {
        for sign in [1.0, -1.0] {
            let rows = [
                float(sign * 1e308),
                float(sign * 1e308),
                float(-sign * 1e308),
            ];
            let expected = float(sign * f64::INFINITY);
            assert_eq!(run_agg(&SumFunc, &rows), expected);
            assert_eq!(run_agg(&TotalFunc, &rows), expected);
            assert_eq!(run_agg(&AvgFunc, &rows), expected);
        }
    }

    #[test]
    fn test_nan_inputs_have_null_numeric_aggregate_semantics() {
        let rows = [float(f64::NAN), null()];
        assert_eq!(run_agg(&SumFunc, &rows), null());
        assert_eq!(run_agg(&TotalFunc, &rows), float(0.0));
        assert_eq!(run_agg(&AvgFunc, &rows), null());
        let rows = [float(f64::NAN), int(2), null(), int(4)];
        assert_eq!(run_agg(&SumFunc, &rows), int(6));
        assert_eq!(run_agg(&TotalFunc, &rows), float(6.0));
        assert_eq!(run_agg(&AvgFunc, &rows), float(3.0));
    }

    #[test]
    fn test_numeric_aggregate_text_and_blob_coercion_counts_non_numeric_values() {
        let rows = [
            SqliteValue::Blob(vec![b'2'].into()),
            text("3xyz"),
            text("not numeric"),
            null(),
            int(5),
        ];
        assert_eq!(run_agg(&SumFunc, &rows), float(10.0));
        assert_eq!(run_agg(&TotalFunc, &rows), float(10.0));
        assert_eq!(run_agg(&AvgFunc, &rows), float(2.5));
    }

    #[test]
    fn test_numeric_aggregate_registry_states_are_independent() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for name in ["sum", "total", "avg"] {
            let aggregate = registry.find_aggregate(name, 1).unwrap();
            let mut first = aggregate.initial_state();
            let mut second = aggregate.initial_state();
            aggregate.step(&mut first, &[int(i64::MAX)]).unwrap();
            aggregate.step(&mut second, &[float(f64::INFINITY)]).unwrap();
            aggregate.step(&mut first, &[int(i64::MIN)]).unwrap();
            aggregate.step(&mut first, &[float(0.0)]).unwrap();
            assert_eq!(aggregate.finalize(second).unwrap(), float(f64::INFINITY));
            let expected = if name == "avg" { -1.0 / 3.0 } else { -1.0 };
            assert_eq!(aggregate.finalize(first).unwrap(), float(expected));
        }
    }

    #[test]
    fn test_numeric_aggregates_retain_compensated_fractional_residue() {
        let rows = [float(1e16), float(1.0), float(-1e16)];
        assert_eq!(run_agg(&SumFunc, &rows), float(1.0));
        assert_eq!(run_agg(&TotalFunc, &rows), float(1.0));
        assert_eq!(run_agg(&AvgFunc, &rows), float(1.0 / 3.0));
    }

    // ── median ────────────────────────────────────────────────────────

    #[test]
    fn test_median_basic() {
        let r = run_agg(&MedianFunc, &[int(1), int(2), int(3), int(4), int(5)]);
        assert_float_eq(&r, 3.0);
    }

    #[test]
    fn test_median_even() {
        let r = run_agg(&MedianFunc, &[int(1), int(2), int(3), int(4)]);
        assert_float_eq(&r, 2.5);
    }

    #[test]
    fn test_median_null_skipped() {
        let r = run_agg(&MedianFunc, &[int(1), null(), int(3)]);
        assert_float_eq(&r, 2.0);
    }

    #[test]
    fn test_median_empty() {
        let r = run_agg(&MedianFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
    }

    // ── percentile ────────────────────────────────────────────────────

    #[test]
    fn test_percentile_50() {
        // percentile(col, 50) = median
        let rows: Vec<(SqliteValue, SqliteValue)> = vec![
            (int(1), float(50.0)),
            (int(2), float(50.0)),
            (int(3), float(50.0)),
            (int(4), float(50.0)),
            (int(5), float(50.0)),
        ];
        let r = run_agg2(&PercentileFunc, &rows);
        assert_float_eq(&r, 3.0);
    }

    #[test]
    fn test_percentile_0() {
        let rows: Vec<(SqliteValue, SqliteValue)> = vec![
            (int(10), float(0.0)),
            (int(20), float(0.0)),
            (int(30), float(0.0)),
        ];
        let r = run_agg2(&PercentileFunc, &rows);
        assert_float_eq(&r, 10.0);
    }

    #[test]
    fn test_percentile_100() {
        let rows: Vec<(SqliteValue, SqliteValue)> = vec![
            (int(10), float(100.0)),
            (int(20), float(100.0)),
            (int(30), float(100.0)),
        ];
        let r = run_agg2(&PercentileFunc, &rows);
        assert_float_eq(&r, 30.0);
    }

    // ── percentile_cont ───────────────────────────────────────────────

    #[test]
    fn test_percentile_cont_basic() {
        let rows: Vec<(SqliteValue, SqliteValue)> = vec![
            (int(1), float(0.5)),
            (int(2), float(0.5)),
            (int(3), float(0.5)),
            (int(4), float(0.5)),
            (int(5), float(0.5)),
        ];
        let r = run_agg2(&PercentileContFunc, &rows);
        assert_float_eq(&r, 3.0);
    }

    // ── percentile_disc ───────────────────────────────────────────────

    #[test]
    fn test_percentile_disc_basic() {
        let rows: Vec<(SqliteValue, SqliteValue)> = vec![
            (int(1), float(0.5)),
            (int(2), float(0.5)),
            (int(3), float(0.5)),
            (int(4), float(0.5)),
            (int(5), float(0.5)),
        ];
        let r = run_agg2(&PercentileDiscFunc, &rows);
        // Discrete: returns an actual input value.
        match r {
            SqliteValue::Float(v) => {
                // Should be one of the actual input values (3.0 for 0.5 in 5 items).
                assert!(
                    [1.0, 2.0, 3.0, 4.0, 5.0].contains(&v),
                    "expected actual value, got {v}"
                );
            }
            other => {
                assert!(
                    matches!(other, SqliteValue::Float(_)),
                    "expected Float, got {other:?}"
                );
            }
        }
    }

    #[test]
    fn test_percentile_disc_no_interpolation() {
        // With 4 items at p=0.5, cont would interpolate, disc should not.
        let rows: Vec<(SqliteValue, SqliteValue)> = vec![
            (int(10), float(0.5)),
            (int(20), float(0.5)),
            (int(30), float(0.5)),
            (int(40), float(0.5)),
        ];
        let r = run_agg2(&PercentileDiscFunc, &rows);
        match r {
            SqliteValue::Float(v) => {
                // Must be one of {10, 20, 30, 40}, not 25.0.
                assert!(
                    [10.0, 20.0, 30.0, 40.0].contains(&v),
                    "disc must not interpolate: got {v}"
                );
            }
            other => {
                assert!(
                    matches!(other, SqliteValue::Float(_)),
                    "expected Float, got {other:?}"
                );
            }
        }
    }

    // ── percentile validation and SQLite rank semantics ───────────────

    #[test]
    fn test_percentile_invalid_fractions_error_even_for_null_data() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for (name, maximum) in [
            ("percentile", 100.0),
            ("percentile_cont", 1.0),
            ("percentile_disc", 1.0),
        ] {
            let aggregate = registry.find_aggregate(name, 2).unwrap();
            for p in [
                null(),
                text("not numeric"),
                text("0.5xyz"),
                text(""),
                SqliteValue::Blob(vec![b'0'].into()),
                float(f64::NAN),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
                float(-1.0),
                float(maximum + 1.0),
            ] {
                for y in [null(), int(10)] {
                    let mut state = aggregate.initial_state();
                    let error = aggregate.step(&mut state, &[y, p.clone()]).unwrap_err();
                    assert!(matches!(error, FrankenError::FunctionError(_)));
                }
            }
        }
    }

    #[test]
    fn test_percentile_numeric_text_fraction_is_not_numeric_text_data() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for (name, fraction) in [
            ("percentile", " \t5e1\r\n"),
            ("percentile_cont", " \t.5\r\n"),
            ("percentile_disc", " \t.5\r\n"),
        ] {
            let aggregate = registry.find_aggregate(name, 2).unwrap();
            let mut state = aggregate.initial_state();
            for value in [10, 20, 30] {
                aggregate
                    .step(&mut state, &[int(value), text(fraction)])
                    .unwrap();
            }
            assert_eq!(aggregate.finalize(state).unwrap(), float(20.0));
            let mut state = aggregate.initial_state();
            assert!(
                aggregate
                    .step(&mut state, &[text("10"), text(fraction)])
                    .is_err()
            );
        }
    }

    #[test]
    fn test_median_and_percentiles_reject_non_numeric_and_infinite_data() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for (name, arity) in [
            ("median", 1),
            ("percentile", 2),
            ("percentile_cont", 2),
            ("percentile_disc", 2),
        ] {
            let aggregate = registry.find_aggregate(name, arity).unwrap();
            for value in [
                text("12"),
                text("invalid"),
                SqliteValue::Blob(vec![b'1', b'2'].into()),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
            ] {
                let mut state = aggregate.initial_state();
                let args = if arity == 1 {
                    vec![value]
                } else {
                    vec![value, float(0.5)]
                };
                let error = aggregate.step(&mut state, &args).unwrap_err();
                assert!(matches!(error, FrankenError::FunctionError(_)));
            }
        }
    }

    #[test]
    fn test_percentile_fraction_is_checked_on_null_rows() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for (name, scale) in [
            ("percentile", 100.0),
            ("percentile_cont", 1.0),
            ("percentile_disc", 1.0),
        ] {
            let aggregate = registry.find_aggregate(name, 2).unwrap();
            for (first, second) in [(null(), int(10)), (int(10), null())] {
                let mut state = aggregate.initial_state();
                aggregate
                    .step(&mut state, &[first, float(0.5 * scale)])
                    .unwrap();
                assert!(
                    aggregate
                        .step(&mut state, &[second, float(0.6 * scale)])
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn test_percentile_fraction_tolerance_is_normalized_and_does_not_drift() {
        for (name, scale) in [
            ("percentile", 100.0),
            ("percentile_cont", 1.0),
            ("percentile_disc", 1.0),
        ] {
            let mut state = PercentileState {
                values: Vec::new(),
                p: None,
            };
            percentile_step(&mut state, &[null(), float(0.5 * scale)], scale, name).unwrap();
            percentile_step(
                &mut state,
                &[int(10), float(0.50075 * scale)],
                scale,
                name,
            )
            .unwrap();
            assert!(
                percentile_step(
                    &mut state,
                    &[int(20), float(0.5015 * scale)],
                    scale,
                    name,
                )
                .is_err()
            );
            assert_eq!(state.p, Some(0.5));
            assert_eq!(state.values, vec![10.0]);
        }
    }

    #[test]
    fn test_percentile_disc_uses_lower_continuous_rank() {
        for (p, expected) in [
            (0.0, 10.0),
            (0.26, 10.0),
            (0.5, 20.0),
            (0.75, 30.0),
            (0.99, 30.0),
            (1.0, 40.0),
        ] {
            let rows: Vec<_> = [40, 10, 30, 20]
                .into_iter()
                .map(|y| (int(y), float(p)))
                .collect();
            assert_eq!(run_agg2(&PercentileDiscFunc, &rows), float(expected));
        }
        let rows = [(int(10), float(0.99)), (int(20), float(0.99))];
        assert_eq!(run_agg2(&PercentileDiscFunc, &rows), float(10.0));
    }

    #[test]
    fn test_percentile_cont_interpolation_and_percent_scale_agree() {
        for (p, expected) in [(0.0, 10.0), (0.25, 17.5), (0.5, 25.0), (1.0, 40.0)] {
            let rows: Vec<_> = [40, 10, 30, 20]
                .into_iter()
                .map(|y| (int(y), float(p)))
                .collect();
            assert_eq!(run_agg2(&PercentileContFunc, &rows), float(expected));
            let rows: Vec<_> = rows
                .into_iter()
                .map(|(y, _)| (y, float(p * 100.0)))
                .collect();
            assert_eq!(run_agg2(&PercentileFunc, &rows), float(expected));
        }
    }

    #[test]
    fn test_percentile_empty_singleton_and_nan_data() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for (name, arity) in [
            ("median", 1),
            ("percentile", 2),
            ("percentile_cont", 2),
            ("percentile_disc", 2),
        ] {
            let aggregate = registry.find_aggregate(name, arity).unwrap();
            assert_eq!(aggregate.finalize(aggregate.initial_state()).unwrap(), null());
            let mut state = aggregate.initial_state();
            for value in [null(), float(f64::NAN)] {
                let args = if arity == 1 {
                    vec![value]
                } else {
                    vec![value, float(0.5)]
                };
                aggregate.step(&mut state, &args).unwrap();
            }
            assert_eq!(aggregate.finalize(state).unwrap(), null());
            let mut state = aggregate.initial_state();
            for value in [null(), float(f64::NAN), int(7)] {
                let args = if arity == 1 {
                    vec![value]
                } else {
                    vec![value, float(0.5)]
                };
                aggregate.step(&mut state, &args).unwrap();
            }
            assert_eq!(aggregate.finalize(state).unwrap(), float(7.0));
        }
    }

    #[test]
    fn test_median_finite_extreme_values_do_not_overflow_interpolation() {
        let rows = [float(-f64::MAX), float(f64::MAX)];
        assert_eq!(run_agg(&MedianFunc, &rows), float(0.0));
        let rows = [
            (float(-f64::MAX), float(0.5)),
            (float(f64::MAX), float(0.5)),
        ];
        assert_eq!(run_agg2(&PercentileContFunc, &rows), float(0.0));
    }

    #[test]
    fn test_percentile_rejected_row_does_not_change_state() {
        let mut state = PercentileContFunc.initial_state();
        assert!(
            PercentileContFunc
                .step(&mut state, &[text("invalid"), float(0.25)])
                .is_err()
        );
        assert_eq!(state.p, None);
        assert!(state.values.is_empty());
        PercentileContFunc
            .step(&mut state, &[int(10), float(0.5)])
            .unwrap();
        assert!(PercentileContFunc.step(&mut state, &[int(20), null()]).is_err());
        assert_eq!(state.p, Some(0.5));
        assert_eq!(state.values, vec![10.0]);
    }

    #[test]
    fn test_percentile_typed_affinity_keeps_group_fractions_independent() {
        let mut registry = FunctionRegistry::new();
        register_aggregate_builtins(&mut registry);
        for (name, scale) in [
            ("percentile", 100.0),
            ("percentile_cont", 1.0),
            ("percentile_disc", 1.0),
        ] {
            let aggregate = registry.find_aggregate(name, 2).unwrap();
            let mut first = aggregate.initial_state();
            let mut second = aggregate.initial_state();
            let first_p = text(&(0.25 * scale).to_string());
            let second_p = text(&(0.75 * scale).to_string());
            for (a, b) in [(10, 100), (20, 200)] {
                aggregate
                    .step(&mut first, &[int(a), first_p.clone()])
                    .unwrap();
                aggregate
                    .step(&mut second, &[int(b), second_p.clone()])
                    .unwrap();
            }
            assert!(matches!(first_p, SqliteValue::Text(_)));
            assert!(matches!(second_p, SqliteValue::Text(_)));
            let (expected_first, expected_second) = if name == "percentile_disc" {
                (10.0, 100.0)
            } else {
                (12.5, 175.0)
            };
            assert_eq!(aggregate.finalize(first).unwrap(), float(expected_first));
            assert_eq!(aggregate.finalize(second).unwrap(), float(expected_second));
        }
    }

    // ── string_agg (alias) ────────────────────────────────────────────

    #[test]
    fn test_string_agg_alias() {
        let mut reg = FunctionRegistry::new();
        register_aggregate_builtins(&mut reg);
        let sa = reg
            .find_aggregate("string_agg", 2)
            .expect("string_agg registered");
        let mut state = sa.initial_state();
        sa.step(&mut state, &[text("a"), text(",")]).unwrap();
        sa.step(&mut state, &[text("b"), text(",")]).unwrap();
        let r = sa.finalize(state).unwrap();
        assert_eq!(r, SqliteValue::Text("a,b".into()));
    }

    // ── registration ──────────────────────────────────────────────────

    #[test]
    fn test_register_aggregate_builtins_all_present() {
        let mut reg = FunctionRegistry::new();
        register_aggregate_builtins(&mut reg);

        let expected = [
            ("avg", 1),
            ("count", 0), // count(*)
            ("count", 1), // count(X)
            ("max", 1),
            ("min", 1),
            ("sum", 1),
            ("total", 1),
            ("median", 1),
            ("percentile", 2),
            ("percentile_cont", 2),
            ("percentile_disc", 2),
            ("string_agg", 2),
        ];

        for (name, arity) in expected {
            assert!(
                reg.find_aggregate(name, arity).is_some(),
                "aggregate '{name}/{arity}' not registered"
            );
        }

        // group_concat is variadic
        assert!(reg.find_aggregate("group_concat", 1).is_some());
        assert!(reg.find_aggregate("group_concat", 2).is_some());

        let group_concat_zero = reg.find_aggregate("group_concat", 0).unwrap();
        let err = group_concat_zero
            .finalize(group_concat_zero.initial_state())
            .expect_err("group_concat() should reject zero arguments");
        assert!(
            matches!(&err, FrankenError::FunctionError(message)
                if message == "wrong number of arguments to function group_concat()"),
            "unexpected error: {err:?}"
        );
    }

    // ── E2E: full lifecycle through registry ──────────────────────────

    #[test]
    fn test_e2e_registry_invoke_aggregates() {
        let mut reg = FunctionRegistry::new();
        register_aggregate_builtins(&mut reg);

        // avg through registry
        let avg = reg.find_aggregate("avg", 1).unwrap();
        let mut state = avg.initial_state();
        avg.step(&mut state, &[int(10)]).unwrap();
        avg.step(&mut state, &[int(20)]).unwrap();
        avg.step(&mut state, &[int(30)]).unwrap();
        let r = avg.finalize(state).unwrap();
        assert_float_eq(&r, 20.0);

        // sum through registry
        let sum = reg.find_aggregate("sum", 1).unwrap();
        let mut state = sum.initial_state();
        sum.step(&mut state, &[int(1)]).unwrap();
        sum.step(&mut state, &[int(2)]).unwrap();
        sum.step(&mut state, &[int(3)]).unwrap();
        let r = sum.finalize(state).unwrap();
        assert_eq!(r, int(6));
    }
}
