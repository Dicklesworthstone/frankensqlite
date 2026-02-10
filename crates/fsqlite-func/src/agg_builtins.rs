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

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;

use crate::{AggregateFunction, FunctionRegistry};

// ─── Kahan compensated summation ──────────────────────────────────────────

/// Kahan compensated summation step.  Maintains a running compensation term
/// that captures the low-order bits lost during each addition, matching the
/// precision behavior of C SQLite's aggregate accumulator.
#[inline]
fn kahan_add(sum: &mut f64, compensation: &mut f64, value: f64) {
    let y = value - *compensation;
    let t = *sum + y;
    *compensation = (t - *sum) - y;
    *sum = t;
}

// ═══════════════════════════════════════════════════════════════════════════
// avg(X)
// ═══════════════════════════════════════════════════════════════════════════

pub struct AvgState {
    sum: f64,
    compensation: f64,
    count: i64,
}

pub struct AvgFunc;

impl AggregateFunction for AvgFunc {
    type State = AvgState;

    fn initial_state(&self) -> Self::State {
        AvgState {
            sum: 0.0,
            compensation: 0.0,
            count: 0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if !args[0].is_null() {
            kahan_add(&mut state.sum, &mut state.compensation, args[0].to_float());
            state.count += 1;
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        if state.count == 0 {
            Ok(SqliteValue::Null)
        } else {
            Ok(SqliteValue::Float(state.sum / state.count as f64))
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
    values: Vec<String>,
    separator: String,
}

pub struct GroupConcatFunc;

impl AggregateFunction for GroupConcatFunc {
    type State = GroupConcatState;

    fn initial_state(&self) -> Self::State {
        GroupConcatState {
            values: Vec::new(),
            separator: ",".to_owned(),
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if args[0].is_null() {
            return Ok(());
        }
        // Set separator from second arg on first call if provided.
        if args.len() > 1 && state.values.is_empty() && !args[1].is_null() {
            state.separator = args[1].to_text();
        }
        state.values.push(args[0].to_text());
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        if state.values.is_empty() {
            Ok(SqliteValue::Null)
        } else {
            Ok(SqliteValue::Text(state.values.join(&state.separator)))
        }
    }

    fn num_args(&self) -> i32 {
        -1 // 1 or 2 args
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
                if candidate > current {
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
                if candidate < current {
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
        if args[0].is_null() {
            return Ok(());
        }
        state.has_values = true;
        match &args[0] {
            SqliteValue::Integer(i) => {
                if state.all_integer && !state.overflowed {
                    match state.int_sum.checked_add(*i) {
                        Some(s) => state.int_sum = s,
                        None => state.overflowed = true,
                    }
                }
                kahan_add(
                    &mut state.float_sum,
                    &mut state.float_compensation,
                    *i as f64,
                );
            }
            other => {
                state.all_integer = false;
                kahan_add(
                    &mut state.float_sum,
                    &mut state.float_compensation,
                    other.to_float(),
                );
            }
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        if !state.has_values {
            return Ok(SqliteValue::Null);
        }
        if state.overflowed {
            return Err(FrankenError::IntegerOverflow);
        }
        if state.all_integer {
            Ok(SqliteValue::Integer(state.int_sum))
        } else {
            Ok(SqliteValue::Float(state.float_sum))
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

/// State for `total()`: Kahan compensated accumulator.
pub struct TotalState {
    sum: f64,
    compensation: f64,
}

impl AggregateFunction for TotalFunc {
    type State = TotalState;

    fn initial_state(&self) -> Self::State {
        TotalState {
            sum: 0.0,
            compensation: 0.0,
        }
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        if !args[0].is_null() {
            kahan_add(&mut state.sum, &mut state.compensation, args[0].to_float());
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Float(state.sum))
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
        if !args[0].is_null() {
            state.push(args[0].to_float());
        }
        Ok(())
    }

    fn finalize(&self, mut state: Self::State) -> Result<SqliteValue> {
        if state.is_empty() {
            return Ok(SqliteValue::Null);
        }
        state.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let result = percentile_cont_impl(&state, 0.5);
        Ok(SqliteValue::Float(result))
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
        if !args[0].is_null() {
            state.values.push(args[0].to_float());
        }
        // Capture P from the second argument (constant expression).
        if state.p.is_none() && args.len() > 1 && !args[1].is_null() {
            state.p = Some(args[1].to_float());
        }
        Ok(())
    }

    fn finalize(&self, mut state: Self::State) -> Result<SqliteValue> {
        if state.values.is_empty() {
            return Ok(SqliteValue::Null);
        }
        let p = state.p.unwrap_or(50.0);
        state
            .values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        // Convert P from 0-100 to 0-1 for the shared implementation.
        let result = percentile_cont_impl(&state.values, p / 100.0);
        Ok(SqliteValue::Float(result))
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
        if !args[0].is_null() {
            state.values.push(args[0].to_float());
        }
        if state.p.is_none() && args.len() > 1 && !args[1].is_null() {
            state.p = Some(args[1].to_float());
        }
        Ok(())
    }

    fn finalize(&self, mut state: Self::State) -> Result<SqliteValue> {
        if state.values.is_empty() {
            return Ok(SqliteValue::Null);
        }
        let p = state.p.unwrap_or(0.5);
        state
            .values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let result = percentile_cont_impl(&state.values, p);
        Ok(SqliteValue::Float(result))
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
        if !args[0].is_null() {
            state.values.push(args[0].to_float());
        }
        if state.p.is_none() && args.len() > 1 && !args[1].is_null() {
            state.p = Some(args[1].to_float());
        }
        Ok(())
    }

    fn finalize(&self, mut state: Self::State) -> Result<SqliteValue> {
        if state.values.is_empty() {
            return Ok(SqliteValue::Null);
        }
        let p = state.p.unwrap_or(0.5);
        state
            .values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        // Discrete: pick the value at the ceiling index.
        let n = state.values.len();
        let idx = ((p * n as f64).ceil() as usize)
            .saturating_sub(1)
            .min(n - 1);
        Ok(SqliteValue::Float(state.values[idx]))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "percentile_disc"
    }
}

// ── Shared percentile helper ──────────────────────────────────────────────

/// Continuous percentile with linear interpolation.
/// `sorted` must be sorted ascending. `p` is in [0, 1].
fn percentile_cont_impl(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let p = p.clamp(0.0, 1.0);
    let rank = p * (n - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let frac = rank - lower as f64;
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
                values: Vec::new(),
                separator: ",".to_owned(),
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
        SqliteValue::Text(s.to_owned())
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
        assert_eq!(r, SqliteValue::Text("a,b,c".to_owned()));
    }

    #[test]
    fn test_group_concat_custom_sep() {
        let rows = vec![
            (text("a"), text("; ")),
            (text("b"), text("; ")),
            (text("c"), text("; ")),
        ];
        let r = run_agg2(&GroupConcatFunc, &rows);
        assert_eq!(r, SqliteValue::Text("a; b; c".to_owned()));
    }

    #[test]
    fn test_group_concat_null_skipped() {
        let r = run_agg(&GroupConcatFunc, &[text("a"), null(), text("c")]);
        assert_eq!(r, SqliteValue::Text("a,c".to_owned()));
    }

    #[test]
    fn test_group_concat_empty() {
        let r = run_agg(&GroupConcatFunc, &[]);
        assert_eq!(r, SqliteValue::Null);
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
        assert_eq!(r, SqliteValue::Text("a,b".to_owned()));
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
