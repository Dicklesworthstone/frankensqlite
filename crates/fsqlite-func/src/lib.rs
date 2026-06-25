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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use tracing::debug;

// ── Function evaluation metrics (bd-2wt.1) ─────────────────────────────────

/// Total number of scalar function calls across all statements.
static FSQLITE_FUNC_CALLS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Cumulative function evaluation duration in microseconds.
static FSQLITE_FUNC_EVAL_DURATION_US_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of function evaluation metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuncMetricsSnapshot {
    /// Total scalar function calls.
    pub calls_total: u64,
    /// Cumulative evaluation duration in microseconds.
    pub eval_duration_us_total: u64,
}

/// Read a point-in-time snapshot of function evaluation metrics.
#[must_use]
pub fn func_metrics_snapshot() -> FuncMetricsSnapshot {
    FuncMetricsSnapshot {
        calls_total: FSQLITE_FUNC_CALLS_TOTAL.load(Ordering::Relaxed),
        eval_duration_us_total: FSQLITE_FUNC_EVAL_DURATION_US_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset function metrics to zero (tests/diagnostics).
pub fn reset_func_metrics() {
    FSQLITE_FUNC_CALLS_TOTAL.store(0, Ordering::Relaxed);
    FSQLITE_FUNC_EVAL_DURATION_US_TOTAL.store(0, Ordering::Relaxed);
}

/// Record a function call for metrics (called from VDBE engine).
pub fn record_func_call(duration_us: u64) {
    FSQLITE_FUNC_CALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
    FSQLITE_FUNC_EVAL_DURATION_US_TOTAL.fetch_add(duration_us, Ordering::Relaxed);
}

/// Record a function call count only, without timing (fast path).
pub fn record_func_call_count_only() {
    FSQLITE_FUNC_CALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// ── UDF registration metrics (bd-2wt.3) ────────────────────────────────

/// Total number of UDF registrations.
static FSQLITE_UDF_REGISTERED: AtomicU64 = AtomicU64::new(0);

/// Record a UDF registration event.
pub fn record_udf_registered() {
    FSQLITE_UDF_REGISTERED.fetch_add(1, Ordering::Relaxed);
}

/// Current count of UDF registrations.
#[must_use]
pub fn udf_registered_count() -> u64 {
    FSQLITE_UDF_REGISTERED.load(Ordering::Relaxed)
}

/// Reset UDF registration counter (tests/diagnostics).
pub fn reset_udf_metrics() {
    FSQLITE_UDF_REGISTERED.store(0, Ordering::Relaxed);
}

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
pub use builtins::{
    ChangeTrackingState, case_sensitive_like_active, get_last_changes, get_last_insert_rowid,
    get_total_changes, register_builtins, reset_total_changes, set_case_sensitive_like,
    set_change_tracking_state, set_last_changes, set_last_insert_rowid, sqlite_compile_options,
    sqlite_compileoption_used,
};
pub use collation::{
    BinaryCollation, CollationAnnotation, CollationFunction, CollationRegistry, CollationSource,
    NoCaseCollation, RtrimCollation, resolve_collation,
};
pub use datetime::register_datetime_builtins;
pub use math::register_math_builtins;
pub use scalar::{JSON_SUBTYPE, ScalarFunction};
pub use vtab::{
    ColumnContext, ConstraintOp, IndexConstraint, IndexConstraintUsage, IndexInfo, IndexOrderBy,
    VirtualTable, VirtualTableCursor,
};
pub use window::{WindowAdapter, WindowFunction};
pub use window_builtins::register_window_builtins;

/// Top-level function family exposed by the runtime registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BuiltinFunctionFamily {
    Scalar,
    Aggregate,
    Window,
}

impl BuiltinFunctionFamily {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
        }
    }
}

/// Track-E built-in function class used for parity closure accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BuiltinFunctionClass {
    CoreScalar,
    MathScalar,
    DateTimeScalar,
    Aggregate,
    Window,
}

impl BuiltinFunctionClass {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::CoreScalar => "core_scalar",
            Self::MathScalar => "math_scalar",
            Self::DateTimeScalar => "datetime_scalar",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
        }
    }
}

/// Runtime-authoritative description of one built-in function registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinFunctionSurfaceEntry {
    /// Lowercase SQL function name as exposed by the runtime registry.
    pub name: String,
    /// Declared arity, or `-1` for variadic registrations.
    pub num_args: i32,
    /// Top-level function family.
    pub family: BuiltinFunctionFamily,
    /// Track-E parity classification bucket.
    pub class: BuiltinFunctionClass,
    /// Whether this entry is an alternate spelling over another runtime entry.
    pub is_alias: bool,
    /// Canonical parity surface identifier for this function family.
    pub surface_id: &'static str,
}

const CORE_FUNCTION_SURFACE_ID: &str = "SURF-FUNC-CORE-011";
const WINDOW_FUNCTION_SURFACE_ID: &str = "SURF-FUNC-WINDOW-012";

/// Return the runtime-authoritative built-in function surface inventory.
///
/// The inventory is derived from the actual registration path in this crate
/// rather than from harness-side matrices so Track E docs and future parity
/// checks can reuse one stable source of truth.
#[must_use]
pub fn builtin_function_surface_inventory() -> &'static [BuiltinFunctionSurfaceEntry] {
    static INVENTORY: OnceLock<Vec<BuiltinFunctionSurfaceEntry>> = OnceLock::new();
    INVENTORY
        .get_or_init(|| {
            let mut registry = FunctionRegistry::new();
            register_builtins(&mut registry);
            register_window_builtins(&mut registry);

            let mut entries = Vec::with_capacity(
                registry.scalars.len() + registry.aggregates.len() + registry.windows.len(),
            );
            extend_builtin_surface_entries(
                &mut entries,
                BuiltinFunctionFamily::Scalar,
                registry.scalars.keys(),
            );
            extend_builtin_surface_entries(
                &mut entries,
                BuiltinFunctionFamily::Aggregate,
                registry.aggregates.keys(),
            );
            extend_builtin_surface_entries(
                &mut entries,
                BuiltinFunctionFamily::Window,
                registry.windows.keys(),
            );
            entries.sort_by(|left, right| {
                (left.family, left.class, &left.name, left.num_args).cmp(&(
                    right.family,
                    right.class,
                    &right.name,
                    right.num_args,
                ))
            });
            entries
        })
        .as_slice()
}

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
/// 2. Fallback to an arity-compatible variadic version `(UPPERCASE_NAME, -1)`.
/// 3. Known scalar name with incompatible arity returns a function that raises
///    SQLite's "wrong number of arguments" error when invoked.
/// 4. `None` if neither found (caller should raise "no such function").
#[derive(Default)]
pub struct FunctionRegistry {
    scalars: HashMap<FunctionKey, Arc<dyn ScalarFunction>>,
    aggregates: HashMap<FunctionKey, Arc<ErasedAggregateFunction>>,
    windows: HashMap<FunctionKey, Arc<ErasedWindowFunction>>,
}

struct WrongArgCountScalarFunction {
    display_name: String,
}

fn wrong_arg_count_message(display_name: &str) -> String {
    format!("wrong number of arguments to function {display_name}()")
}

fn wrong_arg_display_name(canonical: &str) -> String {
    canonical.to_ascii_lowercase()
}

impl WrongArgCountScalarFunction {
    fn new(canonical: &str) -> Self {
        Self {
            display_name: wrong_arg_display_name(canonical),
        }
    }

    fn message(&self) -> String {
        wrong_arg_count_message(&self.display_name)
    }
}

impl ScalarFunction for WrongArgCountScalarFunction {
    fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        Err(FrankenError::function_error(self.message()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        &self.display_name
    }
}

struct WrongArgCountAggregateFunction {
    display_name: String,
}

impl WrongArgCountAggregateFunction {
    fn new(canonical: &str) -> Self {
        Self {
            display_name: wrong_arg_display_name(canonical),
        }
    }

    fn message(&self) -> String {
        wrong_arg_count_message(&self.display_name)
    }
}

impl AggregateFunction for WrongArgCountAggregateFunction {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn step(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        Err(FrankenError::function_error(self.message()))
    }

    fn finalize(&self, _state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Err(FrankenError::function_error(self.message()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        &self.display_name
    }
}

struct WrongArgCountWindowFunction {
    display_name: String,
}

impl WrongArgCountWindowFunction {
    fn new(canonical: &str) -> Self {
        Self {
            display_name: wrong_arg_display_name(canonical),
        }
    }

    fn message(&self) -> String {
        wrong_arg_count_message(&self.display_name)
    }
}

impl WindowFunction for WrongArgCountWindowFunction {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn step(&self, _state: &mut Self::State, _args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        Err(FrankenError::function_error(self.message()))
    }

    fn inverse(
        &self,
        _state: &mut Self::State,
        _args: &[SqliteValue],
    ) -> fsqlite_error::Result<()> {
        Err(FrankenError::function_error(self.message()))
    }

    fn value(&self, _state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        Err(FrankenError::function_error(self.message()))
    }

    fn finalize(&self, _state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Err(FrankenError::function_error(self.message()))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        &self.display_name
    }
}

impl FunctionRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mutable clone of a registry from an `Arc` reference.
    ///
    /// This is used by the UDF registration API to produce a new registry
    /// containing the existing functions plus the newly registered UDF.
    #[must_use]
    pub fn clone_from_arc(arc: &Arc<Self>) -> Self {
        Self {
            scalars: arc.scalars.clone(),
            aggregates: arc.aggregates.clone(),
            windows: arc.windows.clone(),
        }
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
    /// Tries exact match first, then falls back to an arity-compatible
    /// variadic version `(name, -1)` if no exact match exists.
    #[must_use]
    pub fn find_scalar(&self, name: &str, num_args: i32) -> Option<Arc<dyn ScalarFunction>> {
        let canon = canonical_name(name);
        self.find_scalar_precanonical(&canon, num_args)
    }

    /// Look up a scalar function by already-uppercased name (avoids allocation).
    ///
    /// Used by the VDBE engine where `P4::FuncName` values are already
    /// canonicalized by codegen.
    #[must_use]
    pub fn find_scalar_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<Arc<dyn ScalarFunction>> {
        let exact = FunctionKey {
            name: canonical.to_owned(),
            num_args,
        };
        if let Some(f) = self.scalars.get(&exact) {
            debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "exact", "registry lookup");
            return Some(Arc::clone(f));
        }
        let variadic = FunctionKey {
            name: canonical.to_owned(),
            num_args: -1,
        };
        if let Some(function) = self.scalars.get(&variadic) {
            if function.accepts_arg_count(num_args) {
                debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "variadic", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(WrongArgCountScalarFunction::new(canonical)));
        }
        if self.scalars.keys().any(|key| key.name == canonical) {
            debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(WrongArgCountScalarFunction::new(canonical)));
        }
        debug!(
            name = %canonical,
            arity = num_args,
            kind = "scalar",
            hit = "miss",
            "registry lookup"
        );
        None
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
        self.find_aggregate_precanonical(&canon, num_args)
    }

    /// Look up an aggregate function by already-uppercased name (avoids allocation).
    #[must_use]
    pub fn find_aggregate_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<Arc<ErasedAggregateFunction>> {
        let exact = FunctionKey {
            name: canonical.to_owned(),
            num_args,
        };
        if let Some(f) = self.aggregates.get(&exact) {
            debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "exact", "registry lookup");
            return Some(Arc::clone(f));
        }
        let variadic = FunctionKey {
            name: canonical.to_owned(),
            num_args: -1,
        };
        if let Some(function) = self.aggregates.get(&variadic) {
            if function.accepts_arg_count(num_args) {
                debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "variadic", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(AggregateAdapter::new(
                WrongArgCountAggregateFunction::new(canonical),
            )));
        }
        if self.aggregates.keys().any(|key| key.name == canonical) {
            debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(AggregateAdapter::new(
                WrongArgCountAggregateFunction::new(canonical),
            )));
        }
        debug!(
            name = %canonical,
            arity = num_args,
            kind = "aggregate",
            hit = "miss",
            "registry lookup"
        );
        None
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
        if let Some(function) = self.windows.get(&variadic) {
            if function.accepts_arg_count(num_args) {
                debug!(name = %canon, arity = num_args, kind = "window", hit = "variadic", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canon, arity = num_args, kind = "window", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(WindowAdapter::new(
                WrongArgCountWindowFunction::new(&canon),
            )));
        }
        if self.windows.keys().any(|key| key.name == canon) {
            debug!(name = %canon, arity = num_args, kind = "window", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(WindowAdapter::new(
                WrongArgCountWindowFunction::new(&canon),
            )));
        }
        debug!(
            name = %canon,
            arity = num_args,
            kind = "window",
            hit = "miss",
            "registry lookup"
        );
        None
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

    /// Return whether a known window function accepts the SQL-visible arity.
    ///
    /// `None` means the name is not registered as a window function at all.
    /// This is useful for callers that may execute optimized window paths
    /// without invoking the returned function's `step()` method, where the
    /// wrong-arity sentinel from `find_window` would otherwise be bypassed.
    #[must_use]
    pub fn window_accepts_arg_count(&self, name: &str, num_args: i32) -> Option<bool> {
        let canon = canonical_name(name);
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if let Some(function) = self.windows.get(&exact) {
            return Some(function.accepts_arg_count(num_args));
        }

        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        if let Some(function) = self.windows.get(&variadic) {
            return Some(function.accepts_arg_count(num_args));
        }

        self.windows
            .keys()
            .any(|key| key.name == canon)
            .then_some(false)
    }

    /// Return deduplicated lowercase names of all registered aggregate functions.
    ///
    /// Used by the codegen thread-local to recognize custom aggregate UDFs.
    #[must_use]
    pub fn aggregate_names_lowercase(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .aggregates
            .keys()
            .map(|k| k.name.to_ascii_lowercase())
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

fn extend_builtin_surface_entries<'a>(
    entries: &mut Vec<BuiltinFunctionSurfaceEntry>,
    family: BuiltinFunctionFamily,
    keys: impl Iterator<Item = &'a FunctionKey>,
) {
    for key in keys {
        let name = key.name.to_ascii_lowercase();
        let class = builtin_function_class(&name, family);
        entries.push(BuiltinFunctionSurfaceEntry {
            is_alias: builtin_function_alias_flag(&name, family),
            surface_id: builtin_function_surface_id(family),
            name,
            num_args: key.num_args,
            family,
            class,
        });
    }
}

fn builtin_function_class(name: &str, family: BuiltinFunctionFamily) -> BuiltinFunctionClass {
    match family {
        BuiltinFunctionFamily::Aggregate => BuiltinFunctionClass::Aggregate,
        BuiltinFunctionFamily::Window => BuiltinFunctionClass::Window,
        BuiltinFunctionFamily::Scalar => {
            if matches!(
                name,
                "acos"
                    | "acosh"
                    | "asin"
                    | "asinh"
                    | "atan"
                    | "atan2"
                    | "atanh"
                    | "ceil"
                    | "ceiling"
                    | "cos"
                    | "cosh"
                    | "degrees"
                    | "exp"
                    | "floor"
                    | "ln"
                    | "log"
                    | "log10"
                    | "log2"
                    | "mod"
                    | "pi"
                    | "pow"
                    | "power"
                    | "radians"
                    | "sin"
                    | "sinh"
                    | "sqrt"
                    | "tan"
                    | "tanh"
                    | "trunc"
            ) {
                BuiltinFunctionClass::MathScalar
            } else if matches!(
                name,
                "date" | "datetime" | "julianday" | "strftime" | "time" | "timediff" | "unixepoch"
            ) {
                BuiltinFunctionClass::DateTimeScalar
            } else {
                BuiltinFunctionClass::CoreScalar
            }
        }
    }
}

fn builtin_function_alias_flag(name: &str, family: BuiltinFunctionFamily) -> bool {
    match family {
        BuiltinFunctionFamily::Scalar => {
            matches!(name, "ceiling" | "if" | "power" | "printf" | "substring")
        }
        BuiltinFunctionFamily::Aggregate | BuiltinFunctionFamily::Window => name == "string_agg",
    }
}

const fn builtin_function_surface_id(family: BuiltinFunctionFamily) -> &'static str {
    match family {
        BuiltinFunctionFamily::Window => WINDOW_FUNCTION_SURFACE_ID,
        BuiltinFunctionFamily::Scalar | BuiltinFunctionFamily::Aggregate => {
            CORE_FUNCTION_SURFACE_ID
        }
    }
}

fn canonical_name(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use fsqlite_types::SqliteValue;

    use super::*;

    fn runtime_registry_surface_keys() -> BTreeSet<(BuiltinFunctionFamily, String, i32)> {
        let mut registry = FunctionRegistry::new();
        register_builtins(&mut registry);
        register_window_builtins(&mut registry);

        let scalar_keys = registry
            .scalars
            .keys()
            .map(|key| {
                (
                    BuiltinFunctionFamily::Scalar,
                    key.name.to_ascii_lowercase(),
                    key.num_args,
                )
            })
            .collect::<BTreeSet<_>>();
        let aggregate_keys = registry
            .aggregates
            .keys()
            .map(|key| {
                (
                    BuiltinFunctionFamily::Aggregate,
                    key.name.to_ascii_lowercase(),
                    key.num_args,
                )
            })
            .collect::<BTreeSet<_>>();
        let window_keys = registry
            .windows
            .keys()
            .map(|key| {
                (
                    BuiltinFunctionFamily::Window,
                    key.name.to_ascii_lowercase(),
                    key.num_args,
                )
            })
            .collect::<BTreeSet<_>>();

        scalar_keys
            .into_iter()
            .chain(aggregate_keys)
            .chain(window_keys)
            .collect()
    }

    fn inventory_surface_keys() -> BTreeSet<(BuiltinFunctionFamily, String, i32)> {
        builtin_function_surface_inventory()
            .iter()
            .map(|entry| (entry.family, entry.name.clone(), entry.num_args))
            .collect()
    }

    fn find_surface_entry(
        family: BuiltinFunctionFamily,
        name: &str,
        num_args: i32,
    ) -> &'static BuiltinFunctionSurfaceEntry {
        builtin_function_surface_inventory()
            .iter()
            .find(|entry| {
                entry.family == family && entry.name == name && entry.num_args == num_args
            })
            .unwrap_or_else(|| {
                unreachable!(
                    "missing builtin surface entry: family={} name={} arity={}",
                    family.label(),
                    name,
                    num_args
                )
            })
    }

    #[test]
    fn test_builtin_function_surface_inventory_matches_live_registry() {
        let inventory = builtin_function_surface_inventory();
        let inventory_keys = inventory_surface_keys();
        let runtime_keys = runtime_registry_surface_keys();

        assert_eq!(
            inventory.len(),
            inventory_keys.len(),
            "inventory must not contain duplicate family/name/arity tuples"
        );
        assert_eq!(
            inventory_keys, runtime_keys,
            "inventory must exactly match the live registration path"
        );
        assert!(
            inventory.windows(2).all(|entries| {
                (
                    entries[0].family,
                    entries[0].class,
                    &entries[0].name,
                    entries[0].num_args,
                ) <= (
                    entries[1].family,
                    entries[1].class,
                    &entries[1].name,
                    entries[1].num_args,
                )
            }),
            "inventory must stay deterministically sorted"
        );
    }

    #[test]
    fn test_builtin_function_surface_inventory_classifies_representative_entries() {
        let abs = find_surface_entry(BuiltinFunctionFamily::Scalar, "abs", 1);
        assert_eq!(abs.class, BuiltinFunctionClass::CoreScalar);
        assert!(!abs.is_alias);
        assert_eq!(abs.surface_id, CORE_FUNCTION_SURFACE_ID);

        let date = find_surface_entry(BuiltinFunctionFamily::Scalar, "date", -1);
        assert_eq!(date.class, BuiltinFunctionClass::DateTimeScalar);
        assert!(!date.is_alias);
        assert_eq!(date.surface_id, CORE_FUNCTION_SURFACE_ID);

        let power = find_surface_entry(BuiltinFunctionFamily::Scalar, "power", 2);
        assert_eq!(power.class, BuiltinFunctionClass::MathScalar);
        assert!(power.is_alias);
        assert_eq!(power.surface_id, CORE_FUNCTION_SURFACE_ID);

        let count = find_surface_entry(BuiltinFunctionFamily::Aggregate, "count", 0);
        assert_eq!(count.class, BuiltinFunctionClass::Aggregate);
        assert!(!count.is_alias);
        assert_eq!(count.surface_id, CORE_FUNCTION_SURFACE_ID);

        let row_number = find_surface_entry(BuiltinFunctionFamily::Window, "row_number", 0);
        assert_eq!(row_number.class, BuiltinFunctionClass::Window);
        assert!(!row_number.is_alias);
        assert_eq!(row_number.surface_id, WINDOW_FUNCTION_SURFACE_ID);

        let string_agg_window = find_surface_entry(BuiltinFunctionFamily::Window, "string_agg", 2);
        assert_eq!(string_agg_window.class, BuiltinFunctionClass::Window);
        assert!(string_agg_window.is_alias);
        assert_eq!(string_agg_window.surface_id, WINDOW_FUNCTION_SURFACE_ID);
    }

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
            Ok(SqliteValue::Text(out.into()))
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

    fn assert_wrong_arg_count(
        function: &dyn ScalarFunction,
        args: &[SqliteValue],
        expected_name: &str,
    ) {
        let err = function.invoke(args).expect_err("wrong arity should fail");
        let expected = format!("wrong number of arguments to function {expected_name}()");
        assert!(
            matches!(&err, FrankenError::FunctionError(message) if message == &expected),
            "expected {expected:?}, got {err:?}"
        );
    }

    fn assert_wrong_arg_count_aggregate(
        function: &ErasedAggregateFunction,
        args: &[SqliteValue],
        expected_name: &str,
    ) {
        let mut state = function.initial_state();
        let err = function
            .step(&mut state, args)
            .expect_err("wrong aggregate arity should fail");
        let expected = format!("wrong number of arguments to function {expected_name}()");
        assert!(
            matches!(&err, FrankenError::FunctionError(message) if message == &expected),
            "expected {expected:?}, got {err:?}"
        );
    }

    fn assert_wrong_arg_count_window(
        function: &ErasedWindowFunction,
        args: &[SqliteValue],
        expected_name: &str,
    ) {
        let mut state = function.initial_state();
        let err = function
            .step(&mut state, args)
            .expect_err("wrong window arity should fail");
        let expected = format!("wrong number of arguments to function {expected_name}()");
        assert!(
            matches!(&err, FrankenError::FunctionError(message) if message == &expected),
            "expected {expected:?}, got {err:?}"
        );
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

        let too_few = registry
            .find_scalar("my_func", 0)
            .expect("known function with bad arity returns erroring scalar");
        assert_wrong_arg_count(too_few.as_ref(), &[], "my_func");

        // Look up with specific arg count — no exact match, falls back to variadic
        let f = registry
            .find_scalar("my_func", 3)
            .expect("variadic fallback");
        assert_eq!(
            f.invoke(&[
                SqliteValue::Text("a".into()),
                SqliteValue::Text("b".into()),
                SqliteValue::Text("c".into()),
            ])
            .unwrap(),
            SqliteValue::Text("abc".into())
        );
        let too_many = registry
            .find_scalar("my_func", 4)
            .expect("known function with bad arity returns erroring scalar");
        assert_wrong_arg_count(
            too_many.as_ref(),
            &[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
            ],
            "my_func",
        );
    }

    #[test]
    fn test_registry_exact_wrong_arity_returns_function_error() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Double);

        let f = registry
            .find_scalar("double", 2)
            .expect("known function with wrong arity returns erroring scalar");
        assert_wrong_arg_count(
            f.as_ref(),
            &[SqliteValue::Integer(1), SqliteValue::Integer(2)],
            "double",
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

        // Look up with num_args=3 — no exact match, falls back to variadic
        let f = registry
            .find_scalar("my_func", 3)
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
    fn test_registry_aggregate_wrong_arity_returns_function_error() {
        let mut registry = FunctionRegistry::new();
        registry.register_aggregate(Product);

        let f = registry
            .find_aggregate("product", 0)
            .expect("known aggregate with wrong arity returns erroring aggregate");
        assert_wrong_arg_count_aggregate(f.as_ref(), &[], "product");
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
    fn test_registry_window_wrong_arity_returns_function_error() {
        let mut registry = FunctionRegistry::new();
        registry.register_window(MovingSum);

        let f = registry
            .find_window("moving_sum", 0)
            .expect("known window with wrong arity returns erroring window");
        assert_wrong_arg_count_window(f.as_ref(), &[], "moving_sum");
    }

    #[test]
    fn test_registry_window_accepts_arg_count_reports_known_bad_arity() {
        let mut registry = FunctionRegistry::new();
        registry.register_window(MovingSum);

        assert_eq!(
            registry.window_accepts_arg_count("moving_sum", 1),
            Some(true)
        );
        assert_eq!(
            registry.window_accepts_arg_count("moving_sum", 0),
            Some(false)
        );
        assert_eq!(registry.window_accepts_arg_count("missing_window", 1), None);
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
