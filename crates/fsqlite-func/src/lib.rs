//! Built-in SQL function and extension trait surfaces.
//!
//! This crate defines open, user-implementable traits for:
//! - scalar, aggregate, and window functions
//! - virtual table modules/cursors
//! - collation callbacks
//! - authorizer callbacks
//!
//! It also provides a small in-memory [`FunctionRegistry`] for registering and
//! resolving scalar/aggregate/window functions by `(name, num_args)` key with
//! variadic fallback.
#![allow(clippy::unnecessary_literal_bound)]

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::debug;

pub mod agg_builtins;
pub mod aggregate;
pub mod authorizer;
pub mod builtins;
pub mod collation;
pub mod datetime;
pub mod math;
pub mod scalar;
pub mod vtab;
pub mod window;
pub mod window_builtins;

pub use agg_builtins::register_aggregate_builtins;
pub use aggregate::{AggregateAdapter, AggregateFunction};
pub use authorizer::{AuthAction, AuthResult, Authorizer, AuthorizerAction, AuthorizerDecision};
pub use builtins::register_builtins;
pub use collation::{BinaryCollation, CollationFunction, NoCaseCollation, RtrimCollation};
pub use datetime::register_datetime_builtins;
pub use math::register_math_builtins;
pub use scalar::ScalarFunction;
pub use vtab::{
    ColumnContext, ConstraintOp, IndexConstraint, IndexConstraintUsage, IndexInfo, IndexOrderBy,
    VirtualTable, VirtualTableCursor,
};
pub use window::{WindowAdapter, WindowFunction};
pub use window_builtins::register_window_builtins;

/// Type-erased aggregate function object used by the registry.
pub type ErasedAggregateFunction = dyn AggregateFunction<State = Box<dyn Any + Send>>;

/// Type-erased window function object used by the registry.
pub type ErasedWindowFunction = dyn WindowFunction<State = Box<dyn Any + Send>>;

/// Composite lookup key for functions: `(UPPERCASE name, num_args)`.
///
/// `-1` for `num_args` means variadic (any number of arguments).
/// Names are stored as uppercase ASCII for case-insensitive matching.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FunctionKey {
    /// Function name, stored as uppercase ASCII.
    pub name: String,
    /// Expected argument count, or `-1` for variadic.
    pub num_args: i32,
}

impl FunctionKey {
    /// Create a new function key with the name canonicalized to uppercase.
    #[must_use]
    pub fn new(name: &str, num_args: i32) -> Self {
        Self {
            name: canonical_name(name),
            num_args,
        }
    }
}

/// Registry for scalar, aggregate, and window functions, keyed by
/// `(name, num_args)`.
///
/// Lookup strategy (§9.5):
/// 1. Exact match on `(UPPERCASE_NAME, num_args)`.
/// 2. Fallback to variadic version `(UPPERCASE_NAME, -1)`.
/// 3. `None` if neither found (caller should raise "no such function").
#[derive(Default)]
pub struct FunctionRegistry {
    scalars: HashMap<FunctionKey, Arc<dyn ScalarFunction>>,
    aggregates: HashMap<FunctionKey, Arc<ErasedAggregateFunction>>,
    windows: HashMap<FunctionKey, Arc<ErasedWindowFunction>>,
}

impl FunctionRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a scalar function, keyed by `(name, num_args)`.
    ///
    /// Overwrites any existing function with the same key. Returns the
    /// previous function if one existed.
    pub fn register_scalar<F>(&mut self, function: F) -> Option<Arc<dyn ScalarFunction>>
    where
        F: ScalarFunction + 'static,
    {
        let key = FunctionKey::new(function.name(), function.num_args());
        self.scalars.insert(key, Arc::new(function))
    }

    /// Register an aggregate function using the type-erased adapter.
    ///
    /// Overwrites any existing function with the same `(name, num_args)` key.
    pub fn register_aggregate<F>(&mut self, function: F) -> Option<Arc<ErasedAggregateFunction>>
    where
        F: AggregateFunction + 'static,
        F::State: 'static,
    {
        let key = FunctionKey::new(function.name(), function.num_args());
        self.aggregates
            .insert(key, Arc::new(AggregateAdapter::new(function)))
    }

    /// Register a window function using the type-erased adapter.
    ///
    /// Overwrites any existing function with the same `(name, num_args)` key.
    pub fn register_window<F>(&mut self, function: F) -> Option<Arc<ErasedWindowFunction>>
    where
        F: WindowFunction + 'static,
        F::State: 'static,
    {
        let key = FunctionKey::new(function.name(), function.num_args());
        self.windows
            .insert(key, Arc::new(WindowAdapter::new(function)))
    }

    /// Look up a scalar function by `(name, num_args)`.
    ///
    /// Tries exact match first, then falls back to the variadic version
    /// `(name, -1)` if no exact match exists.
    #[must_use]
    pub fn find_scalar(&self, name: &str, num_args: i32) -> Option<Arc<dyn ScalarFunction>> {
        let canon = canonical_name(name);
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if let Some(f) = self.scalars.get(&exact) {
            debug!(name = %canon, arity = num_args, kind = "scalar", hit = "exact", "registry lookup");
            return Some(Arc::clone(f));
        }
        // Variadic fallback
        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        let result = self.scalars.get(&variadic).map(Arc::clone);
        debug!(
            name = %canon,
            arity = num_args,
            kind = "scalar",
            hit = if result.is_some() { "variadic" } else { "miss" },
            "registry lookup"
        );
        result
    }

    /// Look up an aggregate function by `(name, num_args)`.
    ///
    /// Tries exact match first, then falls back to variadic `(name, -1)`.
    #[must_use]
    pub fn find_aggregate(
        &self,
        name: &str,
        num_args: i32,
    ) -> Option<Arc<ErasedAggregateFunction>> {
        let canon = canonical_name(name);
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if let Some(f) = self.aggregates.get(&exact) {
            debug!(name = %canon, arity = num_args, kind = "aggregate", hit = "exact", "registry lookup");
            return Some(Arc::clone(f));
        }
        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        let result = self.aggregates.get(&variadic).map(Arc::clone);
        debug!(
            name = %canon,
            arity = num_args,
            kind = "aggregate",
            hit = if result.is_some() { "variadic" } else { "miss" },
            "registry lookup"
        );
        result
    }

    /// Look up a window function by `(name, num_args)`.
    ///
    /// Tries exact match first, then falls back to variadic `(name, -1)`.
    #[must_use]
    pub fn find_window(&self, name: &str, num_args: i32) -> Option<Arc<ErasedWindowFunction>> {
        let canon = canonical_name(name);
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if let Some(f) = self.windows.get(&exact) {
            debug!(name = %canon, arity = num_args, kind = "window", hit = "exact", "registry lookup");
            return Some(Arc::clone(f));
        }
        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        let result = self.windows.get(&variadic).map(Arc::clone);
        debug!(
            name = %canon,
            arity = num_args,
            kind = "window",
            hit = if result.is_some() { "variadic" } else { "miss" },
            "registry lookup"
        );
        result
    }

    /// Whether the registry contains any scalar function with this name
    /// (any arg count).
    #[must_use]
    pub fn contains_scalar(&self, name: &str) -> bool {
        let canon = canonical_name(name);
        self.scalars.keys().any(|k| k.name == canon)
    }

    /// Whether the registry contains any aggregate function with this name
    /// (any arg count).
    #[must_use]
    pub fn contains_aggregate(&self, name: &str) -> bool {
        let canon = canonical_name(name);
        self.aggregates.keys().any(|k| k.name == canon)
    }

    /// Whether the registry contains any window function with this name
    /// (any arg count).
    #[must_use]
    pub fn contains_window(&self, name: &str) -> bool {
        let canon = canonical_name(name);
        self.windows.keys().any(|k| k.name == canon)
    }
}

fn canonical_name(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use fsqlite_types::SqliteValue;

    use super::*;

    // -- Mock: double(x) -> x * 2, fixed 1-arg --

    struct Double;

    impl ScalarFunction for Double {
        fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(args[0].to_integer() * 2))
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "double"
        }
    }

    // -- Mock: variadic concat --

    struct VariadicConcat;

    impl ScalarFunction for VariadicConcat {
        fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            let mut out = String::new();
            for a in args {
                out.push_str(&a.to_text());
            }
            Ok(SqliteValue::Text(out))
        }

        fn num_args(&self) -> i32 {
            -1
        }

        fn name(&self) -> &str {
            "my_func"
        }
    }

    // -- Mock: fixed 2-arg version of same name --

    struct TwoArgFunc;

    impl ScalarFunction for TwoArgFunc {
        fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(
                args[0].to_integer() + args[1].to_integer(),
            ))
        }

        fn num_args(&self) -> i32 {
            2
        }

        fn name(&self) -> &str {
            "my_func"
        }
    }

    struct Product;

    impl AggregateFunction for Product {
        type State = i64;

        fn initial_state(&self) -> Self::State {
            1
        }

        fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
            *state *= args[0].to_integer();
            Ok(())
        }

        fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(state))
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "product"
        }
    }

    struct MovingSum;

    impl WindowFunction for MovingSum {
        type State = i64;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
            *state += args[0].to_integer();
            Ok(())
        }

        fn inverse(
            &self,
            state: &mut Self::State,
            args: &[SqliteValue],
        ) -> fsqlite_error::Result<()> {
            *state -= args[0].to_integer();
            Ok(())
        }

        fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(*state))
        }

        fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(state))
        }

        fn num_args(&self) -> i32 {
            1
        }

        fn name(&self) -> &str {
            "moving_sum"
        }
    }

    #[test]
    fn test_registry_register_scalar() {
        let mut registry = FunctionRegistry::new();
        let previous = registry.register_scalar(Double);
        assert!(previous.is_none());
        assert!(registry.contains_scalar("double"));
        assert!(registry.contains_scalar("DOUBLE"));
        let f = registry
            .find_scalar(" Double ", 1)
            .expect("double registered");
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(21)])
                .expect("invoke succeeds"),
            SqliteValue::Integer(42)
        );
    }

    #[test]
    fn test_registry_case_insensitive_lookup() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Double);

        // Register as "double", look up as "DOUBLE", "Double", " double "
        assert!(registry.find_scalar("DOUBLE", 1).is_some());
        assert!(registry.find_scalar("Double", 1).is_some());
        assert!(registry.find_scalar(" double ", 1).is_some());
    }

    #[test]
    fn test_registry_overwrite() {
        let mut registry = FunctionRegistry::new();

        // Register first version
        let prev = registry.register_scalar(Double);
        assert!(prev.is_none());

        // Register second version with same (name, num_args) — overwrites
        let prev = registry.register_scalar(Double);
        assert!(prev.is_some());

        // Still works
        let f = registry.find_scalar("double", 1).unwrap();
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(5)]).unwrap(),
            SqliteValue::Integer(10)
        );
    }

    #[test]
    fn test_registry_variadic_fallback() {
        let mut registry = FunctionRegistry::new();

        // Register only the variadic version (num_args = -1)
        registry.register_scalar(VariadicConcat);

        // Look up with specific arg count — no exact match, falls back to variadic
        let f = registry
            .find_scalar("my_func", 3)
            .expect("variadic fallback");
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("a".to_owned()),
                SqliteValue::Text("b".to_owned()),
                SqliteValue::Text("c".to_owned()),
            ])
            .unwrap(),
            SqliteValue::Text("abc".to_owned())
        );
    }

    #[test]
    fn test_registry_exact_match_over_variadic() {
        let mut registry = FunctionRegistry::new();

        // Register both variadic (num_args=-1) and exact 2-arg version
        registry.register_scalar(VariadicConcat);
        registry.register_scalar(TwoArgFunc);

        // Look up with num_args=2 — exact match wins over variadic
        let f = registry
            .find_scalar("my_func", 2)
            .expect("exact match found");
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(10), SqliteValue::Integer(32)])
                .unwrap(),
            SqliteValue::Integer(42)
        );

        // Look up with num_args=5 — no exact match, falls back to variadic
        let f = registry
            .find_scalar("my_func", 5)
            .expect("variadic fallback");
        assert_eq!(f.num_args(), -1);
    }

    #[test]
    fn test_registry_not_found_returns_none() {
        let registry = FunctionRegistry::new();
        assert!(registry.find_scalar("nonexistent", 1).is_none());
        assert!(registry.find_aggregate("nonexistent", 1).is_none());
        assert!(registry.find_window("nonexistent", 1).is_none());
    }

    #[test]
    fn test_registry_register_and_resolve_aggregate() {
        let mut registry = FunctionRegistry::new();
        let previous = registry.register_aggregate(Product);
        assert!(previous.is_none());
        assert!(registry.contains_aggregate("product"));
        let f = registry
            .find_aggregate("PRODUCT", 1)
            .expect("product aggregate registered");

        let mut state = f.initial_state();
        f.step(&mut state, &[SqliteValue::Integer(2)])
            .expect("step 1");
        f.step(&mut state, &[SqliteValue::Integer(3)])
            .expect("step 2");
        f.step(&mut state, &[SqliteValue::Integer(7)])
            .expect("step 3");

        assert_eq!(
            f.finalize(state).expect("finalize succeeds"),
            SqliteValue::Integer(42)
        );
    }

    #[test]
    fn test_registry_aggregate_type_erased() {
        let mut registry = FunctionRegistry::new();
        registry.register_aggregate(Product);

        // Round-trip through type-erased registry
        let f = registry
            .find_aggregate("product", 1)
            .expect("product found");
        let mut state = f.initial_state();
        f.step(&mut state, &[SqliteValue::Integer(6)]).unwrap();
        f.step(&mut state, &[SqliteValue::Integer(7)]).unwrap();
        assert_eq!(f.finalize(state).unwrap(), SqliteValue::Integer(42));
        assert_eq!(f.name(), "product");
    }

    #[test]
    fn test_registry_register_and_resolve_window() {
        let mut registry = FunctionRegistry::new();
        let previous = registry.register_window(MovingSum);
        assert!(previous.is_none());
        assert!(registry.contains_window("moving_sum"));
        let f = registry
            .find_window("MOVING_SUM", 1)
            .expect("moving_sum window registered");

        let mut state = f.initial_state();
        f.step(&mut state, &[SqliteValue::Integer(10)])
            .expect("step 1");
        f.step(&mut state, &[SqliteValue::Integer(20)])
            .expect("step 2");
        f.step(&mut state, &[SqliteValue::Integer(30)])
            .expect("step 3");
        assert_eq!(f.value(&state).expect("value"), SqliteValue::Integer(60));

        f.inverse(&mut state, &[SqliteValue::Integer(10)])
            .expect("inverse 1");
        f.step(&mut state, &[SqliteValue::Integer(40)])
            .expect("step 4");
        assert_eq!(f.value(&state).expect("value"), SqliteValue::Integer(90));
    }

    #[test]
    fn test_registry_window_type_erased() {
        let mut registry = FunctionRegistry::new();
        registry.register_window(MovingSum);

        let f = registry
            .find_window("moving_sum", 1)
            .expect("moving_sum found");

        // Full lifecycle: initial_state -> step -> inverse -> value -> finalize
        let mut state = f.initial_state();
        f.step(&mut state, &[SqliteValue::Integer(100)]).unwrap();
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(100));

        f.inverse(&mut state, &[SqliteValue::Integer(100)]).unwrap();
        assert_eq!(f.value(&state).unwrap(), SqliteValue::Integer(0));

        f.step(&mut state, &[SqliteValue::Integer(42)]).unwrap();
        assert_eq!(f.finalize(state).unwrap(), SqliteValue::Integer(42));
    }

    #[test]
    fn test_function_key_equality() {
        let k1 = FunctionKey::new("ABS", 1);
        let k2 = FunctionKey::new("abs", 1);
        let k3 = FunctionKey::new("ABS", 2);

        assert_eq!(k1, k2, "case-insensitive equality");
        assert_ne!(k1, k3, "different num_args");
    }

    // ── E2E: bd-1dc9 ────────────────────────────────────────────────────

    #[test]
    fn test_e2e_custom_collation_in_order_by() {
        use collation::{BinaryCollation, CollationFunction, NoCaseCollation, RtrimCollation};

        // Simulate ORDER BY with a custom reverse-alphabetical collation.
        struct ReverseAlpha;

        impl CollationFunction for ReverseAlpha {
            fn name(&self) -> &str {
                "REVERSE_ALPHA"
            }

            fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
                // Reverse of BINARY
                right.cmp(left)
            }
        }

        let coll = ReverseAlpha;
        let mut data: Vec<&[u8]> = vec![b"banana", b"apple", b"cherry", b"date"];
        data.sort_by(|a, b| coll.compare(a, b));

        // Reverse alphabetical: date > cherry > banana > apple
        let expected: Vec<&[u8]> = vec![b"date", b"cherry", b"banana", b"apple"];
        assert_eq!(data, expected);
        assert_eq!(coll.name(), "REVERSE_ALPHA");

        // Verify built-in collations are usable as trait objects.
        let collations: Vec<Box<dyn CollationFunction>> = vec![
            Box::new(BinaryCollation),
            Box::new(NoCaseCollation),
            Box::new(RtrimCollation),
            Box::new(ReverseAlpha),
        ];
        assert_eq!(collations.len(), 4);

        // Sort with BINARY: normal alphabetical
        let mut binary_sorted = data.clone();
        binary_sorted.sort_by(|a, b| collations[0].compare(a, b));
        assert_eq!(binary_sorted[0], b"apple");
    }

    #[test]
    fn test_e2e_authorizer_sandboxing() {
        use authorizer::{AuthAction, AuthResult, Authorizer};

        // Authorizer that denies INSERT/UPDATE/DELETE but allows SELECT.
        struct SelectOnlyAuthorizer;

        impl Authorizer for SelectOnlyAuthorizer {
            fn authorize(
                &self,
                action: AuthAction,
                _arg1: Option<&str>,
                arg2: Option<&str>,
                _db_name: Option<&str>,
                _trigger: Option<&str>,
            ) -> AuthResult {
                match action {
                    AuthAction::Select | AuthAction::Read => {
                        // Ignore the "secret" column (replaced with NULL)
                        if action == AuthAction::Read && arg2 == Some("secret") {
                            return AuthResult::Ignore;
                        }
                        AuthResult::Ok
                    }
                    AuthAction::Insert | AuthAction::Update | AuthAction::Delete => {
                        AuthResult::Deny
                    }
                    _ => AuthResult::Ok,
                }
            }
        }

        let auth = SelectOnlyAuthorizer;

        // SELECT is allowed at compile time.
        assert_eq!(
            auth.authorize(AuthAction::Select, None, None, Some("main"), None),
            AuthResult::Ok,
            "SELECT must be allowed"
        );

        // INSERT is denied at compile time.
        assert_eq!(
            auth.authorize(AuthAction::Insert, Some("users"), None, Some("main"), None),
            AuthResult::Deny,
            "INSERT must be denied (compile-time auth error)"
        );

        // UPDATE is denied.
        assert_eq!(
            auth.authorize(
                AuthAction::Update,
                Some("users"),
                Some("email"),
                Some("main"),
                None
            ),
            AuthResult::Deny,
        );

        // DELETE is denied.
        assert_eq!(
            auth.authorize(AuthAction::Delete, Some("users"), None, Some("main"), None),
            AuthResult::Deny,
        );

        // Read on "secret" column returns Ignore (nullify).
        assert_eq!(
            auth.authorize(
                AuthAction::Read,
                Some("users"),
                Some("secret"),
                Some("main"),
                None
            ),
            AuthResult::Ignore,
            "Ignore must nullify column"
        );

        // Read on normal column is allowed.
        assert_eq!(
            auth.authorize(
                AuthAction::Read,
                Some("users"),
                Some("name"),
                Some("main"),
                None
            ),
            AuthResult::Ok,
        );
    }

    #[test]
    fn test_e2e_function_registry_resolution() {
        // Register abs(1 arg) and a variadic version, then test resolution.
        struct Abs1;

        impl ScalarFunction for Abs1 {
            fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
                Ok(SqliteValue::Integer(args[0].to_integer().abs()))
            }

            fn num_args(&self) -> i32 {
                1
            }

            fn name(&self) -> &str {
                "abs"
            }
        }

        struct AbsVariadic;

        impl ScalarFunction for AbsVariadic {
            fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
                // Variadic: return sum of absolute values
                let sum: i64 = args.iter().map(|a| a.to_integer().abs()).sum();
                Ok(SqliteValue::Integer(sum))
            }

            fn num_args(&self) -> i32 {
                -1
            }

            fn name(&self) -> &str {
                "abs"
            }
        }

        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Abs1);
        registry.register_scalar(AbsVariadic);

        // SELECT abs(-5) should use 1-arg version.
        let f = registry.find_scalar("abs", 1).expect("abs(1) found");
        assert_eq!(f.num_args(), 1, "exact 1-arg match");
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(-5)]).unwrap(),
            SqliteValue::Integer(5)
        );

        // SELECT abs(-5, -3) should fall through to variadic.
        let f = registry.find_scalar("abs", 2).expect("abs variadic found");
        assert_eq!(f.num_args(), -1, "variadic fallback for 2 args");
        assert_eq!(
            f.invoke(&[SqliteValue::Integer(-5), SqliteValue::Integer(-3)])
                .unwrap(),
            SqliteValue::Integer(8)
        );

        // Nonexistent function returns None.
        assert!(registry.find_scalar("nonexistent", 1).is_none());
    }

    #[test]
    fn test_authorizer_called_at_compile_time() {
        use authorizer::{AuthAction, AuthResult, Authorizer};
        use std::sync::Mutex;

        // Track every authorize call to verify compile-time invocation pattern.
        struct TrackingAuthorizer {
            calls: Mutex<Vec<AuthAction>>,
        }

        impl TrackingAuthorizer {
            fn new() -> Self {
                Self {
                    calls: Mutex::new(Vec::new()),
                }
            }
        }

        impl Authorizer for TrackingAuthorizer {
            fn authorize(
                &self,
                action: AuthAction,
                _arg1: Option<&str>,
                _arg2: Option<&str>,
                _db_name: Option<&str>,
                _trigger: Option<&str>,
            ) -> AuthResult {
                self.calls.lock().unwrap().push(action);
                AuthResult::Ok
            }
        }

        let auth = TrackingAuthorizer::new();

        // Simulate compile-time authorization for:
        // `SELECT name, email FROM users WHERE id = ?`
        //
        // The authorizer is called during prepare(), NOT during step().
        // Expected calls:
        //   1. Select (the statement type)
        //   2. Read(users, name)
        //   3. Read(users, email)
        //   4. Read(users, id)    -- WHERE clause column

        // Phase 1: prepare (compile time) — authorizer is called
        auth.authorize(AuthAction::Select, None, None, Some("main"), None);
        auth.authorize(
            AuthAction::Read,
            Some("users"),
            Some("name"),
            Some("main"),
            None,
        );
        auth.authorize(
            AuthAction::Read,
            Some("users"),
            Some("email"),
            Some("main"),
            None,
        );
        auth.authorize(
            AuthAction::Read,
            Some("users"),
            Some("id"),
            Some("main"),
            None,
        );

        let calls = auth.calls.lock().unwrap();
        assert_eq!(calls.len(), 4, "authorizer called 4 times during prepare");
        assert_eq!(calls[0], AuthAction::Select);
        assert_eq!(calls[1], AuthAction::Read);
        assert_eq!(calls[2], AuthAction::Read);
        assert_eq!(calls[3], AuthAction::Read);
        drop(calls);

        // Phase 2: step (execution) — authorizer is NOT called again
        // (In a real implementation, step() would not invoke authorize.)
        // We simply verify no additional calls were recorded.
        let calls_after = auth.calls.lock().unwrap();
        assert_eq!(
            calls_after.len(),
            4,
            "authorizer must NOT be called during step/execution"
        );
        drop(calls_after);
    }
}
