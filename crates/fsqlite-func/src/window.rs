//! Window function trait with sliding-window support.
//!
//! Window functions extend aggregate semantics with the ability to
//! efficiently process sliding window frames via the `inverse` method.
//! This enables O(1) per-row computation for frames like
//! `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`.
#![allow(clippy::unnecessary_literal_bound)]

use std::any::Any;

use fsqlite_error::Result;
use fsqlite_types::SqliteValue;

use crate::FunctionArity;

/// A window SQL function (e.g. `SUM() OVER (...)`, custom moving averages).
///
/// Window functions extend aggregates with:
/// - [`inverse`](Self::inverse): remove a row from the frame (enables O(1) sliding windows)
/// - [`value`](Self::value): peek at the current result without consuming state
///
/// This trait is **open** (user-implementable).
///
/// # State Lifecycle
///
/// 1. [`initial_state`](Self::initial_state) creates a fresh accumulator.
/// 2. For each row in the frame: [`step`](Self::step) adds, [`inverse`](Self::inverse) removes.
/// 3. After each step/inverse: [`value`](Self::value) returns the current result.
/// 4. At partition end: [`finalize`](Self::finalize) consumes state and returns the final value.
pub trait WindowFunction: Send + Sync {
    /// The per-partition accumulator type.
    type State: Send;

    /// Create a fresh accumulator.
    fn initial_state(&self) -> Self::State;

    /// Add a row to the window frame.
    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Remove a row from the window frame (sliding window support).
    ///
    /// This is the key difference from [`AggregateFunction`](crate::AggregateFunction):
    /// by supporting removal, the engine can maintain a running window
    /// in O(1) time per row rather than recomputing the entire frame.
    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()>;

    /// Return the current result without consuming state.
    ///
    /// Called after each step/inverse to provide the windowed value
    /// for the current row. Must be callable multiple times.
    fn value(&self, state: &Self::State) -> Result<SqliteValue>;

    /// Consume the accumulator and produce the final result.
    fn finalize(&self, state: Self::State) -> Result<SqliteValue>;

    /// The number of arguments this function accepts (`-1` = variadic).
    fn num_args(&self) -> i32;

    /// Minimum accepted SQL argument count for a variadic function.
    ///
    /// Some built-in window functions receive ORDER BY values internally even
    /// when their SQL call syntax accepts no visible arguments. These bounds
    /// describe SQL-visible arity, not the runtime step argument slice. The
    /// default is zero; fixed-arity functions do not consult this method.
    fn min_args(&self) -> i32 {
        0
    }

    /// Maximum accepted SQL argument count for a variadic function.
    ///
    /// The default is unbounded. Fixed-arity functions are matched directly
    /// from [`Self::num_args`] and do not consult this method.
    fn max_args(&self) -> Option<i32> {
        None
    }

    /// Return the complete SQL-visible arity contract in one metadata call.
    ///
    /// Registries use this method exactly once before publication, preventing
    /// a reentrant or stateful [`Self::num_args`] implementation from producing
    /// a key and bounds from different observations.
    fn arity(&self) -> FunctionArity {
        let declared = self.num_args();
        if declared >= 0 {
            FunctionArity::exact(declared)
        } else {
            FunctionArity::variadic(self.min_args(), self.max_args())
        }
    }

    /// The function name, used in error messages and EXPLAIN output.
    fn name(&self) -> &str;
}

/// Type-erased adapter for [`WindowFunction`], analogous to
/// [`AggregateAdapter`](crate::AggregateAdapter).
pub struct WindowAdapter<F> {
    inner: F,
}

impl<F> WindowAdapter<F> {
    /// Wrap a concrete window function for type-erased storage.
    pub const fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> WindowFunction for WindowAdapter<F>
where
    F: WindowFunction,
    F::State: 'static,
{
    type State = Box<dyn Any + Send>;

    fn initial_state(&self) -> Self::State {
        Box::new(self.inner.initial_state())
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let concrete = state
            .downcast_mut::<F::State>()
            .expect("window state type mismatch");
        self.inner.step(concrete, args)
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        let concrete = state
            .downcast_mut::<F::State>()
            .expect("window state type mismatch");
        self.inner.inverse(concrete, args)
    }

    fn value(&self, state: &Self::State) -> Result<SqliteValue> {
        let concrete = state
            .downcast_ref::<F::State>()
            .expect("window state type mismatch");
        self.inner.value(concrete)
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        let concrete = *state
            .downcast::<F::State>()
            .expect("window state type mismatch");
        self.inner.finalize(concrete)
    }

    fn num_args(&self) -> i32 {
        self.inner.num_args()
    }

    fn min_args(&self) -> i32 {
        self.inner.min_args()
    }

    fn max_args(&self) -> Option<i32> {
        self.inner.max_args()
    }

    fn arity(&self) -> FunctionArity {
        self.inner.arity()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Mock: window sum --

    struct WindowSum;

    impl WindowFunction for WindowSum {
        type State = i64;

        fn initial_state(&self) -> i64 {
            0
        }

        fn step(&self, state: &mut i64, args: &[SqliteValue]) -> Result<()> {
            *state += args[0].to_integer();
            Ok(())
        }

        fn inverse(&self, state: &mut i64, args: &[SqliteValue]) -> Result<()> {
            *state -= args[0].to_integer();
            Ok(())
        }

        fn value(&self, state: &i64) -> Result<SqliteValue> {
            Ok(SqliteValue::Integer(*state))
        }

        fn finalize(&self, state: i64) -> Result<SqliteValue> {
            Ok(SqliteValue::Integer(state))
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "window_sum"
        }
    }

    #[test]
    fn test_window_function_step_and_inverse() {
        let f = WindowSum;
        let mut state = f.initial_state();

        // Simulate frame [10, 20, 30]
        f.step(&mut state, &[SqliteValue::Integer(10)]).unwrap();
        f.step(&mut state, &[SqliteValue::Integer(20)]).unwrap();
        f.step(&mut state, &[SqliteValue::Integer(30)]).unwrap();
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(60));

        // Slide: remove 10, add 40 -> frame [20, 30, 40]
        f.inverse(&mut state, &[SqliteValue::Integer(10)]).unwrap();
        f.step(&mut state, &[SqliteValue::Integer(40)]).unwrap();
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(90));

        // Slide: remove 20 -> frame [30, 40]
        f.inverse(&mut state, &[SqliteValue::Integer(20)]).unwrap();
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(70));
    }

    #[test]
    fn test_window_function_value_without_consuming() {
        let f = WindowSum;
        let mut state = f.initial_state();

        f.step(&mut state, &[SqliteValue::Integer(42)]).unwrap();

        // value() can be called multiple times without consuming state.
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(42));
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(42));
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(42));

        // State is still valid after multiple value() calls.
        f.step(&mut state, &[SqliteValue::Integer(8)]).unwrap();
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(50));
    }

    #[test]
    fn test_window_function_finalize_consumes() {
        let f = WindowSum;
        let mut state = f.initial_state();

        f.step(&mut state, &[SqliteValue::Integer(10)]).unwrap();
        f.step(&mut state, &[SqliteValue::Integer(32)]).unwrap();

        // finalize consumes state and produces final value.
        let result = f.finalize(state).unwrap();
        assert_eq!(result, SqliteValue::Integer(42));
        // `state` is moved — cannot be used after finalize (enforced by Rust move semantics).
    }
}
