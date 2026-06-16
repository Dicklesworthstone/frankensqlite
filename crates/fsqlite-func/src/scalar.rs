//! Scalar (row-level) function trait.
//!
//! Scalar functions compute a single output value from zero or more input
//! values. They are stateless across rows: each invocation is independent.
//!
//! This trait is **open** (user-implementable), unlike the sealed pager/btree
//! traits. Extension authors implement `ScalarFunction` to register custom
//! SQL functions.
//!
//! # Send + Sync
//!
//! Scalar functions may be shared across threads via `Arc` for use by
//! concurrent query executors. Implementations must be thread-safe.
//!
//! # Cx Exception
//!
//! `invoke` does **not** take `&Cx` because deterministic scalar functions
//! are pure computations (§9 cross-cutting rule: "Pure computation
//! exceptions: deterministic ScalarFunction::invoke without I/O need not
//! take Cx").
#![allow(clippy::unnecessary_literal_bound)]

use fsqlite_error::Result;
use fsqlite_types::SqliteValue;

/// A scalar (row-level) SQL function.
///
/// Scalar functions are invoked once per row and return a single value.
/// They are stored in the [`FunctionRegistry`](crate::FunctionRegistry) as
/// `Arc<dyn ScalarFunction>`.
///
/// # Error Handling
///
/// - Return [`FrankenError::FunctionError`](fsqlite_error::FrankenError::FunctionError)
///   for domain errors (e.g. `abs(i64::MIN)`).
/// - Return [`FrankenError::TooBig`](fsqlite_error::FrankenError::TooBig)
///   if the result exceeds `SQLITE_MAX_LENGTH`.
/// SQLite's JSON subtype tag (`'J'` = 74), attached to TEXT/BLOB values
/// produced by JSON functions so downstream JSON constructors embed them as
/// parsed JSON rather than quoting them as ordinary strings.
pub const JSON_SUBTYPE: u32 = 74;

pub trait ScalarFunction: Send + Sync {
    /// Execute this function on the given arguments.
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue>;

    /// Execute this function with knowledge of each argument's value subtype
    /// (e.g. [`JSON_SUBTYPE`] for values returned by JSON functions).
    ///
    /// The default ignores subtypes and delegates to [`Self::invoke`]. JSON
    /// constructors (`json_object`, `json_array`, the JSON mutators) override
    /// this so a JSON-subtyped TEXT argument is embedded as a JSON value
    /// instead of being quoted as a plain string — matching C SQLite, which
    /// keys this behaviour off `sqlite3_value_subtype()`.
    fn invoke_with_arg_subtypes(
        &self,
        args: &[SqliteValue],
        _arg_subtypes: &[u32],
    ) -> Result<SqliteValue> {
        self.invoke(args)
    }

    /// The subtype this function tags onto its result value, if any.
    ///
    /// Returns [`JSON_SUBTYPE`] for functions whose result is JSON text so the
    /// engine can propagate the tag to the destination register. Defaults to
    /// `None` (no subtype).
    fn result_subtype(&self) -> Option<u32> {
        None
    }

    /// Whether this function is deterministic (same inputs → same output).
    ///
    /// Deterministic functions enable constant folding and other query
    /// planner optimizations. Defaults to `true`.
    fn is_deterministic(&self) -> bool {
        true
    }

    /// The number of arguments this function accepts.
    ///
    /// `-1` means variadic (any number of arguments).
    fn num_args(&self) -> i32;

    /// Minimum accepted argument count for variadic functions.
    ///
    /// Fixed-arity functions default to their exact arity. Variadic functions
    /// default to accepting zero arguments unless an implementation tightens
    /// the bound to match SQLite's function surface.
    fn min_args(&self) -> i32 {
        self.num_args().max(0)
    }

    /// Maximum accepted argument count, or `None` for unbounded variadic
    /// functions.
    fn max_args(&self) -> Option<i32> {
        (self.num_args() >= 0).then(|| self.num_args())
    }

    /// Return whether this function accepts `num_args` arguments.
    fn accepts_arg_count(&self, num_args: i32) -> bool {
        num_args >= self.min_args() && self.max_args().is_none_or(|max| num_args <= max)
    }

    /// The function name, used in error messages and EXPLAIN output.
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fsqlite_error::FrankenError;

    use super::*;

    // -- Mock: add_one(x) -> x + 1 --

    struct AddOne;

    impl ScalarFunction for AddOne {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            match &args[0] {
                SqliteValue::Integer(i) => Ok(SqliteValue::Integer(i + 1)),
                SqliteValue::Float(f) => Ok(SqliteValue::Float(f + 1.0)),
                SqliteValue::Null => Ok(SqliteValue::Null),
                SqliteValue::Text(s) => {
                    let n: i64 = s.parse().unwrap_or(0);
                    Ok(SqliteValue::Integer(n + 1))
                }
                SqliteValue::Blob(_) => Ok(SqliteValue::Integer(1)),
            }
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "add_one"
        }
    }

    // -- Mock: non-deterministic --

    struct NonDeterministic;

    impl ScalarFunction for NonDeterministic {
        fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
            Ok(SqliteValue::Integer(42))
        }

        fn is_deterministic(&self) -> bool {
            false
        }

        fn num_args(&self) -> i32 {
            0
        }

        fn name(&self) -> &str {
            "random_ish"
        }
    }

    // -- Mock: variadic concat --

    struct Concat;

    impl ScalarFunction for Concat {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            let mut result = String::new();
            for arg in args {
                result.push_str(&arg.to_text());
            }
            Ok(SqliteValue::Text(result.into()))
        }

        fn num_args(&self) -> i32 {
            -1
        }

        fn name(&self) -> &str {
            "concat"
        }
    }

    // -- Mock: domain error --

    struct SafeAbs;

    impl ScalarFunction for SafeAbs {
        fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
            match &args[0] {
                SqliteValue::Integer(i) => {
                    if *i == i64::MIN {
                        return Err(FrankenError::function_error("abs(i64::MIN) would overflow"));
                    }
                    Ok(SqliteValue::Integer(i.abs()))
                }
                _ => Ok(args[0].clone()),
            }
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "safe_abs"
        }
    }

    // -- Mock: too-big error --

    struct BigResult;

    impl ScalarFunction for BigResult {
        fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
            Err(FrankenError::TooBig)
        }

        fn num_args(&self) -> i32 {
            0
        }

        fn name(&self) -> &str {
            "big_result"
        }
    }

    // -- Tests --

    #[test]
    fn test_scalar_function_invoke_basic() {
        let f = AddOne;
        // Integer
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(41)]).unwrap(),
            SqliteValue::Integer(42)
        );
        // Float
        assert_eq!(
            f.invoke(&[SqliteValue::Float(1.5)]).unwrap(),
            SqliteValue::Float(2.5)
        );
        // Null
        assert!(f.invoke(&[SqliteValue::Null]).unwrap().is_null());
        // Text (numeric coercion)
        assert_eq!(
            f.invoke(&[SqliteValue::Text("99".into())]).unwrap(),
            SqliteValue::Integer(100)
        );
    }

    #[test]
    fn test_scalar_function_deterministic_flag() {
        let det = AddOne;
        assert!(det.is_deterministic());

        let non_det = NonDeterministic;
        assert!(!non_det.is_deterministic());
    }

    #[test]
    fn test_scalar_function_variadic() {
        let f = Concat;
        assert_eq!(f.num_args(), -1);
        assert_eq!(f.min_args(), 0);
        assert_eq!(f.max_args(), None);
        assert!(f.accepts_arg_count(0));
        assert!(f.accepts_arg_count(3));

        // 0 args
        assert_eq!(f.invoke(&[]).unwrap(), SqliteValue::Text("".into()));

        // 1 arg
        assert_eq!(
            f.invoke(&[SqliteValue::Text("hello".into())]).unwrap(),
            SqliteValue::Text("hello".into())
        );

        // many args
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("a".into()),
                SqliteValue::Text("b".into()),
                SqliteValue::Text("c".into()),
            ])
            .unwrap(),
            SqliteValue::Text("abc".into())
        );
    }

    #[test]
    fn test_scalar_function_error_domain() {
        let f = SafeAbs;
        let err = f.invoke(&[SqliteValue::Integer(i64::MIN)]).unwrap_err();
        assert!(
            matches!(err, FrankenError::FunctionError(ref msg) if msg.contains("overflow")),
            "expected FunctionError, got {err:?}"
        );
    }

    #[test]
    fn test_scalar_function_too_big_error() {
        let f = BigResult;
        let err = f.invoke(&[]).unwrap_err();
        assert!(matches!(err, FrankenError::TooBig));
    }

    #[test]
    fn test_scalar_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AddOne>();

        // Can be stored in Arc
        let f: Arc<dyn ScalarFunction> = Arc::new(AddOne);
        let f2 = Arc::clone(&f);
        let handle = std::thread::spawn(move || f2.invoke(&[SqliteValue::Integer(0)]));
        let _ = f.invoke(&[SqliteValue::Integer(1)]);
        handle.join().unwrap().unwrap();
    }
}
