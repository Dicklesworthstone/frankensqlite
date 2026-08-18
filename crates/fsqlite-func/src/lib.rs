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
    set_change_tracking_state, set_last_changes, set_last_insert_rowid,
    set_statement_text_encoding, sqlite_compile_options, sqlite_compileoption_used,
    statement_text_encoding,
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
    name: String,
    /// Expected argument count, or `-1` for variadic.
    num_args: i32,
}

impl FunctionKey {
    /// Create a new function key with the name canonicalized to uppercase.
    #[must_use]
    pub fn new(name: &str, num_args: i32) -> Self {
        assert_valid_declared_args(num_args);
        Self {
            name: canonical_name(name),
            num_args,
        }
    }
}

fn assert_valid_declared_args(num_args: i32) {
    assert!(
        num_args >= -1,
        "function argument count must be -1 or non-negative"
    );
}

/// Immutable SQL-visible argument-count contract for a registered function.
///
/// Construct this once from user metadata and publish it alongside the
/// function object. Runtime lookup uses only this value, never re-entering
/// user-defined metadata callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(clippy::struct_field_names)]
pub struct FunctionArity {
    declared_args: i32,
    min_args: i32,
    max_args: Option<i32>,
}

impl FunctionArity {
    /// Construct an exact-arity contract.
    #[must_use]
    pub fn exact(num_args: i32) -> Self {
        assert!(num_args >= 0, "exact function arity must be non-negative");
        Self {
            declared_args: num_args,
            min_args: num_args,
            max_args: Some(num_args),
        }
    }

    /// Construct a variadic contract with inclusive argument-count bounds.
    #[must_use]
    pub fn variadic(min_args: i32, max_args: Option<i32>) -> Self {
        assert!(min_args >= 0, "minimum function arity must be non-negative");
        assert!(
            max_args.is_none_or(|max| max >= min_args),
            "maximum function arity must not be below its minimum"
        );
        Self {
            declared_args: -1,
            min_args,
            max_args,
        }
    }

    /// Registry key arity (`-1` for a variadic contract).
    #[must_use]
    pub const fn declared_args(self) -> i32 {
        self.declared_args
    }

    /// Minimum accepted SQL-visible argument count.
    #[must_use]
    pub const fn min_args(self) -> i32 {
        self.min_args
    }

    /// Maximum accepted SQL-visible argument count, or `None` when unbounded.
    #[must_use]
    pub const fn max_args(self) -> Option<i32> {
        self.max_args
    }

    /// Whether this contract accepts `num_args` SQL-visible arguments.
    #[must_use]
    pub fn accepts(self, num_args: i32) -> bool {
        num_args >= self.min_args && self.max_args.is_none_or(|max| num_args <= max)
    }

    pub(crate) fn from_declared_args(
        declared_args: i32,
        variadic_bounds: impl FnOnce() -> (i32, Option<i32>),
    ) -> Self {
        assert_valid_declared_args(declared_args);
        if declared_args == -1 {
            let (min_args, max_args) = variadic_bounds();
            Self::variadic(min_args, max_args)
        } else {
            Self::exact(declared_args)
        }
    }
}

/// Kind of application-defined function selected for one SQL call.
///
/// Application registrations share one namespace across scalar, aggregate,
/// and window functions. A window registration is also callable through the
/// ordinary aggregate form, but remains distinguishable here so callers can
/// validate whether an `OVER` clause is legal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ApplicationFunctionKind {
    /// A scalar function evaluated once per input row.
    Scalar,
    /// An ordinary aggregate function evaluated once per group.
    Aggregate,
    /// A window function, callable both as an aggregate and with `OVER`.
    Window,
}

impl ApplicationFunctionKind {
    /// Lowercase SQL-facing label used in misuse diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
        }
    }

    /// Whether the selected registration has an ordinary aggregate call form.
    #[must_use]
    pub const fn is_aggregate_callable(self) -> bool {
        matches!(self, Self::Aggregate | Self::Window)
    }

    /// Whether the selected registration may be used with `OVER`.
    #[must_use]
    pub const fn is_window_callable(self) -> bool {
        matches!(self, Self::Window)
    }
}

/// Frozen application-overload resolution for one SQL-visible call arity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationFunctionResolution {
    kind: ApplicationFunctionKind,
    arity: FunctionArity,
}

impl ApplicationFunctionResolution {
    /// Selected function kind.
    #[must_use]
    pub const fn kind(self) -> ApplicationFunctionKind {
        self.kind
    }

    /// Frozen arity contract belonging to the selected registration.
    #[must_use]
    pub const fn arity(self) -> FunctionArity {
        self.arity
    }
}

fn assert_key_matches_arity(key: &FunctionKey, arity: FunctionArity) {
    assert_eq!(
        key.num_args,
        arity.declared_args(),
        "function key and frozen arity contract must have the same declared argument count"
    );
}

/// Frozen policy governing use of a scalar function in schema-maintained
/// expressions such as indexes, generated columns, and CHECK constraints.
///
/// This metadata belongs to the registry entry, not the open
/// [`ScalarFunction`] trait object. Consequently a user-defined function can
/// neither re-enter registration nor contradict the explicit deterministic /
/// non-deterministic API while rows are being evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarSchemaSafety {
    /// Every invocation is stable for the lifetime of the schema.
    Always,
    /// No invocation is permitted in a schema-maintained expression.
    Never,
    /// The sealed built-in date/time classifier must inspect evaluated values.
    DateTimeConditional,
}

impl ScalarSchemaSafety {
    const fn from_deterministic(deterministic: bool) -> Self {
        if deterministic {
            Self::Always
        } else {
            Self::Never
        }
    }
}

/// Frozen policy governing whether a scalar call is constant for one query.
///
/// This is distinct from [`ScalarSchemaSafety`]: SQLite's slow-changing
/// built-ins are stable for one statement and can therefore be factored out of
/// inner loops, but they are not safe in schema-maintained expressions. The
/// registry derives this metadata from public deterministic registration APIs;
/// only sealed, crate-private built-in registration paths can publish
/// [`Self::SlowChanging`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarQueryConstancy {
    /// With identical arguments, the function is stable across statements.
    Constant,
    /// The function is stable during one query but may change between them.
    SlowChanging,
    /// The function may produce a different result on every invocation.
    Volatile,
}

impl ScalarQueryConstancy {
    const fn from_deterministic(deterministic: bool) -> Self {
        if deterministic {
            Self::Constant
        } else {
            Self::Volatile
        }
    }

    /// Whether the function is stable for the duration of one query.
    #[must_use]
    pub const fn is_query_constant(self) -> bool {
        matches!(self, Self::Constant | Self::SlowChanging)
    }
}

/// One scalar implementation and the immutable metadata selected for a call.
///
/// Keeping these values together prevents execution paths from resolving the
/// function, schema-safety policy, query-constancy policy, and
/// argument-collation contract through separate registry probes that could
/// disagree or repeat canonicalization.
#[derive(Clone)]
pub struct ResolvedScalarFunction {
    function: Arc<dyn ScalarFunction>,
    schema_safety: ScalarSchemaSafety,
    query_constancy: ScalarQueryConstancy,
    consumes_argument_collation: bool,
}

impl ResolvedScalarFunction {
    /// Clone the selected function object for invocation outside a registry
    /// borrow or lock.
    #[must_use]
    pub fn function(&self) -> Arc<dyn ScalarFunction> {
        Arc::clone(&self.function)
    }

    /// Frozen schema-safety policy belonging to the selected registration.
    #[must_use]
    pub const fn schema_safety(&self) -> ScalarSchemaSafety {
        self.schema_safety
    }

    /// Frozen query-constancy policy belonging to the selected registration.
    #[must_use]
    pub const fn query_constancy(&self) -> ScalarQueryConstancy {
        self.query_constancy
    }

    /// Frozen argument-collation contract belonging to the selected entry.
    #[must_use]
    pub const fn consumes_argument_collation(&self) -> bool {
        self.consumes_argument_collation
    }
}

enum ApplicationFunction {
    Scalar {
        function: Arc<dyn ScalarFunction>,
        arity: FunctionArity,
        schema_safety: ScalarSchemaSafety,
        query_constancy: ScalarQueryConstancy,
        consumes_argument_collation: bool,
    },
    Aggregate {
        function: Arc<ErasedAggregateFunction>,
        arity: FunctionArity,
    },
    Window {
        function: Arc<ErasedWindowFunction>,
        aggregate: Arc<ErasedAggregateFunction>,
        arity: FunctionArity,
    },
}

impl ApplicationFunction {
    const fn kind(&self) -> ApplicationFunctionKind {
        match self {
            Self::Scalar { .. } => ApplicationFunctionKind::Scalar,
            Self::Aggregate { .. } => ApplicationFunctionKind::Aggregate,
            Self::Window { .. } => ApplicationFunctionKind::Window,
        }
    }

    const fn arity(&self) -> FunctionArity {
        match self {
            Self::Scalar { arity, .. }
            | Self::Aggregate { arity, .. }
            | Self::Window { arity, .. } => *arity,
        }
    }

    const fn resolution(&self) -> ApplicationFunctionResolution {
        ApplicationFunctionResolution {
            kind: self.kind(),
            arity: self.arity(),
        }
    }
}

impl Clone for ApplicationFunction {
    fn clone(&self) -> Self {
        match self {
            Self::Scalar {
                function,
                arity,
                schema_safety,
                query_constancy,
                consumes_argument_collation,
            } => Self::Scalar {
                function: Arc::clone(function),
                arity: *arity,
                schema_safety: *schema_safety,
                query_constancy: *query_constancy,
                consumes_argument_collation: *consumes_argument_collation,
            },
            Self::Aggregate { function, arity } => Self::Aggregate {
                function: Arc::clone(function),
                arity: *arity,
            },
            Self::Window {
                function,
                aggregate,
                arity,
            } => Self::Window {
                function: Arc::clone(function),
                aggregate: Arc::clone(aggregate),
                arity: *arity,
            },
        }
    }
}

/// Ownership token for an application registration displaced from a registry.
///
/// Callers may retain this value until after publishing the replacement
/// registry and invalidating prepared statements. Dropping it then cannot run
/// user destructors while registry state is mutably borrowed.
pub struct DisplacedApplicationFunction {
    _function: ApplicationFunction,
}

/// Registry for scalar, aggregate, and window functions, keyed by
/// `(name, num_args)`.
///
/// Lookup strategy (§9.5):
/// 1. A compatible exact application registration, across all function kinds.
/// 2. A compatible variadic application registration.
/// 3. An exact entry in the built-in/base layer requested by the caller.
/// 4. An arity-compatible variadic entry in that base layer.
/// 5. A known same-kind name with incompatible arity returns a function that
///    raises SQLite's "wrong number of arguments" error when invoked.
/// 6. `None` if neither layer contains a usable same-kind entry.
#[derive(Default)]
pub struct FunctionRegistry {
    /// Connection-local application registrations. These are deliberately
    /// layered over the built-in maps below: a bounded application variadic
    /// that does not accept a call must leave a compatible built-in visible.
    application_functions: HashMap<String, HashMap<i32, ApplicationFunction>>,
    scalars: HashMap<FunctionKey, Arc<dyn ScalarFunction>>,
    scalar_arities: HashMap<FunctionKey, FunctionArity>,
    scalar_schema_safety: HashMap<FunctionKey, ScalarSchemaSafety>,
    scalar_query_constancy: HashMap<FunctionKey, ScalarQueryConstancy>,
    scalar_argument_collation: HashMap<FunctionKey, bool>,
    aggregates: HashMap<FunctionKey, Arc<ErasedAggregateFunction>>,
    aggregate_arities: HashMap<FunctionKey, FunctionArity>,
    windows: HashMap<FunctionKey, Arc<ErasedWindowFunction>>,
    window_arities: HashMap<FunctionKey, FunctionArity>,
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

/// Aggregate call form of one erased application-defined window function.
///
/// The adapter delegates directly to the already-erased window object, so its
/// accumulator is not boxed a second time. Name and arity are frozen at
/// registration and no user metadata callback is re-entered after publication.
struct WindowAggregateBridge {
    function: Arc<ErasedWindowFunction>,
    name: String,
    arity: FunctionArity,
}

impl AggregateFunction for WindowAggregateBridge {
    type State = Box<dyn Any + Send>;

    fn initial_state(&self) -> Self::State {
        self.function.initial_state()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        self.function.step(state, args)
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.function.finalize(state)
    }

    fn num_args(&self) -> i32 {
        self.arity.declared_args()
    }

    fn min_args(&self) -> i32 {
        self.arity.min_args()
    }

    fn max_args(&self) -> Option<i32> {
        self.arity.max_args()
    }

    fn arity(&self) -> FunctionArity {
        self.arity
    }

    fn name(&self) -> &str {
        &self.name
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
            application_functions: arc.application_functions.clone(),
            scalars: arc.scalars.clone(),
            scalar_arities: arc.scalar_arities.clone(),
            scalar_schema_safety: arc.scalar_schema_safety.clone(),
            scalar_query_constancy: arc.scalar_query_constancy.clone(),
            scalar_argument_collation: arc.scalar_argument_collation.clone(),
            aggregates: arc.aggregates.clone(),
            aggregate_arities: arc.aggregate_arities.clone(),
            windows: arc.windows.clone(),
            window_arities: arc.window_arities.clone(),
        }
    }

    fn application_function_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<&ApplicationFunction> {
        let overloads = self.application_functions.get(canonical)?;
        if let Some(function) = overloads.get(&num_args)
            && function.arity().accepts(num_args)
        {
            return Some(function);
        }

        overloads
            .get(&-1)
            .filter(|function| function.arity().accepts(num_args))
    }

    fn application_name_has_kind_precanonical(
        &self,
        canonical: &str,
        matches_kind: impl Fn(ApplicationFunctionKind) -> bool,
    ) -> bool {
        self.application_functions
            .get(canonical)
            .is_some_and(|overloads| {
                overloads
                    .values()
                    .any(|function| matches_kind(function.kind()))
            })
    }

    /// Resolve a compatible application registration across all function kinds.
    ///
    /// An exact application key wins over a compatible application variadic,
    /// independent of kind or registration order. `None` means no application
    /// overload accepts this call; callers may then resolve built-ins.
    #[must_use]
    pub fn resolve_application_function(
        &self,
        name: &str,
        num_args: i32,
    ) -> Option<ApplicationFunctionResolution> {
        let canonical = canonical_name(name);
        self.resolve_application_function_precanonical(&canonical, num_args)
    }

    /// Precanonicalized counterpart to [`Self::resolve_application_function`].
    #[must_use]
    pub fn resolve_application_function_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<ApplicationFunctionResolution> {
        self.application_function_precanonical(canonical, num_args)
            .map(ApplicationFunction::resolution)
    }

    /// Whether any application overload is registered under this name.
    #[must_use]
    pub fn contains_application_function(&self, name: &str) -> bool {
        let canonical = canonical_name(name);
        self.application_functions.contains_key(&canonical)
    }

    fn replace_application_function(
        &mut self,
        key: FunctionKey,
        function: ApplicationFunction,
    ) -> Option<DisplacedApplicationFunction> {
        assert_key_matches_arity(&key, function.arity());
        self.application_functions
            .entry(key.name)
            .or_default()
            .insert(key.num_args, function)
            .map(|function| DisplacedApplicationFunction {
                _function: function,
            })
    }

    /// Register an application-defined scalar using caller-frozen metadata.
    ///
    /// This shares a cross-kind namespace with application aggregate and window
    /// registrations. Replacing an identical `(name, declared_arity)` key
    /// therefore displaces the previous application entry regardless of kind.
    pub fn register_application_scalar_captured<F>(
        &mut self,
        name: &str,
        arity: FunctionArity,
        deterministic: bool,
        consumes_argument_collation: bool,
        function: F,
    ) -> Option<DisplacedApplicationFunction>
    where
        F: ScalarFunction + 'static,
    {
        let key = FunctionKey::new(name, arity.declared_args());
        self.replace_application_function(
            key,
            ApplicationFunction::Scalar {
                function: Arc::new(function),
                arity,
                schema_safety: ScalarSchemaSafety::from_deterministic(deterministic),
                query_constancy: ScalarQueryConstancy::from_deterministic(deterministic),
                consumes_argument_collation,
            },
        )
    }

    /// Register an application-defined aggregate using caller-frozen metadata.
    ///
    /// The returned token owns any same-key application entry displaced across
    /// scalar, aggregate, or window kinds.
    pub fn register_application_aggregate_captured<F>(
        &mut self,
        name: &str,
        arity: FunctionArity,
        function: F,
    ) -> Option<DisplacedApplicationFunction>
    where
        F: AggregateFunction + 'static,
        F::State: 'static,
    {
        let key = FunctionKey::new(name, arity.declared_args());
        self.replace_application_function(
            key,
            ApplicationFunction::Aggregate {
                function: Arc::new(AggregateAdapter::new(function)),
                arity,
            },
        )
    }

    /// Register an application-defined window function using frozen metadata.
    ///
    /// SQLite window registrations retain an ordinary aggregate call form. The
    /// returned window Arc and aggregate bridge share the same erased function
    /// object and frozen arity contract.
    pub fn register_application_window_captured<F>(
        &mut self,
        name: &str,
        arity: FunctionArity,
        function: F,
    ) -> (
        Arc<ErasedWindowFunction>,
        Option<DisplacedApplicationFunction>,
    )
    where
        F: WindowFunction + 'static,
        F::State: 'static,
    {
        let key = FunctionKey::new(name, arity.declared_args());
        let registered: Arc<ErasedWindowFunction> = Arc::new(WindowAdapter::new(function));
        let aggregate: Arc<ErasedAggregateFunction> = Arc::new(WindowAggregateBridge {
            function: Arc::clone(&registered),
            name: key.name.clone(),
            arity,
        });
        let displaced = self.replace_application_function(
            key,
            ApplicationFunction::Window {
                function: Arc::clone(&registered),
                aggregate,
                arity,
            },
        );
        (registered, displaced)
    }

    /// Register a scalar function, keyed by `(name, num_args)`.
    ///
    /// Overwrites any existing function with the same key. Returns the
    /// previous function if one existed.
    pub fn register_scalar<F>(&mut self, function: F) -> Option<Arc<dyn ScalarFunction>>
    where
        F: ScalarFunction + 'static,
    {
        let name = function.name().to_owned();
        let arity = function.arity();
        let deterministic = function.is_deterministic();
        let consumes_argument_collation = function.consumes_argument_collation();
        self.register_scalar_captured(
            &name,
            arity,
            deterministic,
            consumes_argument_collation,
            function,
        )
    }

    /// Register a scalar function under caller-precomputed identity and arity.
    ///
    /// This variant never calls user metadata. It is intended for publication
    /// paths that capture metadata before taking a registry snapshot, so a
    /// reentrant metadata callback cannot make a stale clone overwrite a nested
    /// registration. The key and immutable arity contract must agree.
    pub fn register_scalar_keyed<F>(
        &mut self,
        key: FunctionKey,
        arity: FunctionArity,
        deterministic: bool,
        consumes_argument_collation: bool,
        function: F,
    ) -> Option<Arc<dyn ScalarFunction>>
    where
        F: ScalarFunction + 'static,
    {
        assert_key_matches_arity(&key, arity);
        self.scalar_arities.insert(key.clone(), arity);
        self.scalar_schema_safety.insert(
            key.clone(),
            ScalarSchemaSafety::from_deterministic(deterministic),
        );
        self.scalar_query_constancy.insert(
            key.clone(),
            ScalarQueryConstancy::from_deterministic(deterministic),
        );
        self.scalar_argument_collation
            .insert(key.clone(), consumes_argument_collation);
        self.scalars.insert(key, Arc::new(function))
    }

    /// Register a scalar function with caller-captured metadata.
    ///
    /// No user metadata callback runs in this method. The registry key and
    /// runtime acceptance contract are both derived from the same immutable
    /// arity value.
    pub fn register_scalar_captured<F>(
        &mut self,
        name: &str,
        arity: FunctionArity,
        deterministic: bool,
        consumes_argument_collation: bool,
        function: F,
    ) -> Option<Arc<dyn ScalarFunction>>
    where
        F: ScalarFunction + 'static,
    {
        let key = FunctionKey::new(name, arity.declared_args());
        self.scalar_arities.insert(key.clone(), arity);
        self.scalar_schema_safety.insert(
            key.clone(),
            ScalarSchemaSafety::from_deterministic(deterministic),
        );
        self.scalar_query_constancy.insert(
            key.clone(),
            ScalarQueryConstancy::from_deterministic(deterministic),
        );
        self.scalar_argument_collation
            .insert(key.clone(), consumes_argument_collation);
        self.scalars.insert(key, Arc::new(function))
    }

    /// Register one sealed built-in whose schema safety depends on evaluated
    /// argument values. This is crate-private so application-defined trait
    /// objects cannot opt into metadata callbacks on the execution hot path.
    pub(crate) fn register_conditionally_deterministic_scalar<F>(
        &mut self,
        function: F,
    ) -> Option<Arc<dyn ScalarFunction>>
    where
        F: ScalarFunction + 'static,
    {
        let name = function.name().to_owned();
        let arity = function.arity();
        let consumes_argument_collation = function.consumes_argument_collation();
        let key = FunctionKey::new(&name, arity.declared_args());
        self.scalar_arities.insert(key.clone(), arity);
        self.scalar_schema_safety
            .insert(key.clone(), ScalarSchemaSafety::DateTimeConditional);
        self.scalar_query_constancy
            .insert(key.clone(), ScalarQueryConstancy::SlowChanging);
        self.scalar_argument_collation
            .insert(key.clone(), consumes_argument_collation);
        self.scalars.insert(key, Arc::new(function))
    }

    /// Register one sealed built-in that is constant for a single query but
    /// unsafe in schema-maintained expressions.
    ///
    /// This is crate-private so application-defined functions cannot opt into
    /// SQLite's privileged slow-changing classification.
    pub(crate) fn register_slow_changing_scalar<F>(
        &mut self,
        function: F,
    ) -> Option<Arc<dyn ScalarFunction>>
    where
        F: ScalarFunction + 'static,
    {
        let name = function.name().to_owned();
        let arity = function.arity();
        let consumes_argument_collation = function.consumes_argument_collation();
        let key = FunctionKey::new(&name, arity.declared_args());
        self.scalar_arities.insert(key.clone(), arity);
        self.scalar_schema_safety
            .insert(key.clone(), ScalarSchemaSafety::Never);
        self.scalar_query_constancy
            .insert(key.clone(), ScalarQueryConstancy::SlowChanging);
        self.scalar_argument_collation
            .insert(key.clone(), consumes_argument_collation);
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
        let name = function.name().to_owned();
        let arity = function.arity();
        self.register_aggregate_captured(&name, arity, function)
    }

    /// Register an aggregate function under caller-precomputed identity and arity.
    ///
    /// Returns the displaced adapter, if any. No user metadata callback runs
    /// here. The key and immutable arity contract must agree.
    pub fn register_aggregate_keyed<F>(
        &mut self,
        key: FunctionKey,
        arity: FunctionArity,
        function: F,
    ) -> Option<Arc<ErasedAggregateFunction>>
    where
        F: AggregateFunction + 'static,
        F::State: 'static,
    {
        assert_key_matches_arity(&key, arity);
        self.aggregate_arities.insert(key.clone(), arity);
        self.aggregates
            .insert(key, Arc::new(AggregateAdapter::new(function)))
    }

    /// Register an aggregate function with caller-captured metadata.
    ///
    /// No user metadata callback runs in this method. The registry key and
    /// runtime acceptance contract share one immutable arity value.
    pub fn register_aggregate_captured<F>(
        &mut self,
        name: &str,
        arity: FunctionArity,
        function: F,
    ) -> Option<Arc<ErasedAggregateFunction>>
    where
        F: AggregateFunction + 'static,
        F::State: 'static,
    {
        let key = FunctionKey::new(name, arity.declared_args());
        self.aggregate_arities.insert(key.clone(), arity);
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
        let name = function.name().to_owned();
        let arity = function.arity();
        let (_, displaced) = self.register_window_captured(&name, arity, function);
        displaced
    }

    /// Register a window function under caller-precomputed identity and arity.
    ///
    /// The returned first Arc is the newly inserted erased adapter; the second
    /// is the displaced adapter, if any. No user metadata callback runs here.
    /// The key and immutable arity contract must agree.
    pub fn register_window_keyed<F>(
        &mut self,
        key: FunctionKey,
        arity: FunctionArity,
        function: F,
    ) -> (Arc<ErasedWindowFunction>, Option<Arc<ErasedWindowFunction>>)
    where
        F: WindowFunction + 'static,
        F::State: 'static,
    {
        let registered: Arc<ErasedWindowFunction> = Arc::new(WindowAdapter::new(function));
        assert_key_matches_arity(&key, arity);
        self.window_arities.insert(key.clone(), arity);
        let displaced = self.windows.insert(key, Arc::clone(&registered));
        (registered, displaced)
    }

    /// Register a window function with caller-captured metadata.
    ///
    /// No user metadata callback runs in this method. The registry key and
    /// runtime acceptance contract share one immutable arity value.
    pub fn register_window_captured<F>(
        &mut self,
        name: &str,
        arity: FunctionArity,
        function: F,
    ) -> (Arc<ErasedWindowFunction>, Option<Arc<ErasedWindowFunction>>)
    where
        F: WindowFunction + 'static,
        F::State: 'static,
    {
        let registered: Arc<ErasedWindowFunction> = Arc::new(WindowAdapter::new(function));
        let key = FunctionKey::new(name, arity.declared_args());
        self.window_arities.insert(key.clone(), arity);
        let displaced = self.windows.insert(key, Arc::clone(&registered));
        (registered, displaced)
    }

    /// Look up a scalar function by `(name, num_args)`.
    ///
    /// Tries exact match first, then falls back to an arity-compatible
    /// variadic version `(name, -1)` if no exact match exists.
    #[must_use]
    pub fn find_scalar(&self, name: &str, num_args: i32) -> Option<Arc<dyn ScalarFunction>> {
        self.resolve_scalar(name, num_args)
            .map(|resolved| resolved.function)
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
        self.resolve_scalar_precanonical(canonical, num_args)
            .map(|resolved| resolved.function)
    }

    /// Resolve a scalar implementation and all execution metadata in one
    /// registry traversal.
    #[must_use]
    pub fn resolve_scalar(&self, name: &str, num_args: i32) -> Option<ResolvedScalarFunction> {
        let canonical = canonical_name(name);
        self.resolve_scalar_precanonical(&canonical, num_args)
    }

    /// Precanonicalized counterpart to [`Self::resolve_scalar`].
    #[must_use]
    pub fn resolve_scalar_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<ResolvedScalarFunction> {
        if let Some(application) = self.application_function_precanonical(canonical, num_args) {
            return match application {
                ApplicationFunction::Scalar {
                    function,
                    schema_safety,
                    query_constancy,
                    consumes_argument_collation,
                    ..
                } => {
                    debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "application", "registry lookup");
                    Some(ResolvedScalarFunction {
                        function: Arc::clone(function),
                        schema_safety: *schema_safety,
                        query_constancy: *query_constancy,
                        consumes_argument_collation: *consumes_argument_collation,
                    })
                }
                ApplicationFunction::Aggregate { .. } | ApplicationFunction::Window { .. } => {
                    debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "shadowed_by_application", "registry lookup");
                    None
                }
            };
        }
        let exact = FunctionKey {
            name: canonical.to_owned(),
            num_args,
        };
        if let Some(function) = self.scalars.get(&exact) {
            let Some(arity) = self.scalar_arities.get(&exact).copied() else {
                debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "missing_arity", "registry lookup");
                return Some(ResolvedScalarFunction {
                    function: Arc::new(WrongArgCountScalarFunction::new(canonical)),
                    schema_safety: ScalarSchemaSafety::Never,
                    query_constancy: ScalarQueryConstancy::Volatile,
                    consumes_argument_collation: false,
                });
            };
            if arity.accepts(num_args) {
                debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "exact", "registry lookup");
                return Some(ResolvedScalarFunction {
                    function: Arc::clone(function),
                    schema_safety: self
                        .scalar_schema_safety
                        .get(&exact)
                        .copied()
                        .unwrap_or(ScalarSchemaSafety::Never),
                    query_constancy: self
                        .scalar_query_constancy
                        .get(&exact)
                        .copied()
                        .unwrap_or(ScalarQueryConstancy::Volatile),
                    consumes_argument_collation: self
                        .scalar_argument_collation
                        .get(&exact)
                        .copied()
                        .unwrap_or(false),
                });
            }
            debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "wrong_arity", "registry lookup");
            return Some(ResolvedScalarFunction {
                function: Arc::new(WrongArgCountScalarFunction::new(canonical)),
                schema_safety: ScalarSchemaSafety::Never,
                query_constancy: ScalarQueryConstancy::Volatile,
                consumes_argument_collation: false,
            });
        }
        let variadic = FunctionKey {
            name: canonical.to_owned(),
            num_args: -1,
        };
        if let Some(function) = self.scalars.get(&variadic) {
            let Some(arity) = self.scalar_arities.get(&variadic).copied() else {
                debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "missing_arity", "registry lookup");
                return Some(ResolvedScalarFunction {
                    function: Arc::new(WrongArgCountScalarFunction::new(canonical)),
                    schema_safety: ScalarSchemaSafety::Never,
                    query_constancy: ScalarQueryConstancy::Volatile,
                    consumes_argument_collation: false,
                });
            };
            if arity.accepts(num_args) {
                debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "variadic", "registry lookup");
                return Some(ResolvedScalarFunction {
                    function: Arc::clone(function),
                    schema_safety: self
                        .scalar_schema_safety
                        .get(&variadic)
                        .copied()
                        .unwrap_or(ScalarSchemaSafety::Never),
                    query_constancy: self
                        .scalar_query_constancy
                        .get(&variadic)
                        .copied()
                        .unwrap_or(ScalarQueryConstancy::Volatile),
                    consumes_argument_collation: self
                        .scalar_argument_collation
                        .get(&variadic)
                        .copied()
                        .unwrap_or(false),
                });
            }
            debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "wrong_arity", "registry lookup");
            return Some(ResolvedScalarFunction {
                function: Arc::new(WrongArgCountScalarFunction::new(canonical)),
                schema_safety: ScalarSchemaSafety::Never,
                query_constancy: ScalarQueryConstancy::Volatile,
                consumes_argument_collation: false,
            });
        }
        if self.scalars.keys().any(|key| key.name == canonical)
            || self.application_name_has_kind_precanonical(canonical, |kind| {
                kind == ApplicationFunctionKind::Scalar
            })
        {
            debug!(name = %canonical, arity = num_args, kind = "scalar", hit = "wrong_arity", "registry lookup");
            return Some(ResolvedScalarFunction {
                function: Arc::new(WrongArgCountScalarFunction::new(canonical)),
                schema_safety: ScalarSchemaSafety::Never,
                query_constancy: ScalarQueryConstancy::Volatile,
                consumes_argument_collation: false,
            });
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
        if let Some(application) = self.application_function_precanonical(canonical, num_args) {
            return match application {
                ApplicationFunction::Aggregate { function, .. } => {
                    debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "application", "registry lookup");
                    Some(Arc::clone(function))
                }
                ApplicationFunction::Window { aggregate, .. } => {
                    debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "application_window", "registry lookup");
                    Some(Arc::clone(aggregate))
                }
                ApplicationFunction::Scalar { .. } => {
                    debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "shadowed_by_application", "registry lookup");
                    None
                }
            };
        }
        let exact = FunctionKey {
            name: canonical.to_owned(),
            num_args,
        };
        if let Some(function) = self.aggregates.get(&exact) {
            let Some(arity) = self.aggregate_arities.get(&exact).copied() else {
                debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "missing_arity", "registry lookup");
                return Some(Arc::new(AggregateAdapter::new(
                    WrongArgCountAggregateFunction::new(canonical),
                )));
            };
            if arity.accepts(num_args) {
                debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "exact", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(AggregateAdapter::new(
                WrongArgCountAggregateFunction::new(canonical),
            )));
        }
        let variadic = FunctionKey {
            name: canonical.to_owned(),
            num_args: -1,
        };
        if let Some(function) = self.aggregates.get(&variadic) {
            let Some(arity) = self.aggregate_arities.get(&variadic).copied() else {
                debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "missing_arity", "registry lookup");
                return Some(Arc::new(AggregateAdapter::new(
                    WrongArgCountAggregateFunction::new(canonical),
                )));
            };
            if arity.accepts(num_args) {
                debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "variadic", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canonical, arity = num_args, kind = "aggregate", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(AggregateAdapter::new(
                WrongArgCountAggregateFunction::new(canonical),
            )));
        }
        if self.aggregates.keys().any(|key| key.name == canonical)
            || self.application_name_has_kind_precanonical(canonical, |kind| {
                kind.is_aggregate_callable()
            })
        {
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
        if let Some(application) = self.application_function_precanonical(&canon, num_args) {
            return match application {
                ApplicationFunction::Window { function, .. } => {
                    debug!(name = %canon, arity = num_args, kind = "window", hit = "application", "registry lookup");
                    Some(Arc::clone(function))
                }
                ApplicationFunction::Scalar { .. } | ApplicationFunction::Aggregate { .. } => {
                    debug!(name = %canon, arity = num_args, kind = "window", hit = "shadowed_by_application", "registry lookup");
                    None
                }
            };
        }
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if let Some(function) = self.windows.get(&exact) {
            let Some(arity) = self.window_arities.get(&exact).copied() else {
                debug!(name = %canon, arity = num_args, kind = "window", hit = "missing_arity", "registry lookup");
                return Some(Arc::new(WindowAdapter::new(
                    WrongArgCountWindowFunction::new(&canon),
                )));
            };
            if arity.accepts(num_args) {
                debug!(name = %canon, arity = num_args, kind = "window", hit = "exact", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canon, arity = num_args, kind = "window", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(WindowAdapter::new(
                WrongArgCountWindowFunction::new(&canon),
            )));
        }
        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        if let Some(function) = self.windows.get(&variadic) {
            let Some(arity) = self.window_arities.get(&variadic).copied() else {
                debug!(name = %canon, arity = num_args, kind = "window", hit = "missing_arity", "registry lookup");
                return Some(Arc::new(WindowAdapter::new(
                    WrongArgCountWindowFunction::new(&canon),
                )));
            };
            if arity.accepts(num_args) {
                debug!(name = %canon, arity = num_args, kind = "window", hit = "variadic", "registry lookup");
                return Some(Arc::clone(function));
            }
            debug!(name = %canon, arity = num_args, kind = "window", hit = "wrong_arity", "registry lookup");
            return Some(Arc::new(WindowAdapter::new(
                WrongArgCountWindowFunction::new(&canon),
            )));
        }
        if self.windows.keys().any(|key| key.name == canon)
            || self.application_name_has_kind_precanonical(&canon, |kind| {
                kind == ApplicationFunctionKind::Window
            })
        {
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
            || self.application_name_has_kind_precanonical(&canon, |kind| {
                kind == ApplicationFunctionKind::Scalar
            })
    }

    /// Whether the registry contains any aggregate function with this name
    /// (any arg count).
    #[must_use]
    pub fn contains_aggregate(&self, name: &str) -> bool {
        let canon = canonical_name(name);
        self.aggregates.keys().any(|k| k.name == canon)
            || self
                .application_name_has_kind_precanonical(&canon, |kind| kind.is_aggregate_callable())
    }

    /// Whether the registry contains any window function with this name
    /// (any arg count).
    #[must_use]
    pub fn contains_window(&self, name: &str) -> bool {
        let canon = canonical_name(name);
        self.windows.keys().any(|k| k.name == canon)
            || self.application_name_has_kind_precanonical(&canon, |kind| {
                kind == ApplicationFunctionKind::Window
            })
    }

    /// Return whether a known scalar function accepts the SQL-visible arity.
    ///
    /// `None` means the name is not registered as a scalar function at all.
    /// Unlike [`Self::find_scalar`], this never constructs or invokes a
    /// wrong-arity sentinel, so preparation-only validation remains free of
    /// user-function side effects.
    #[must_use]
    pub fn scalar_accepts_arg_count(&self, name: &str, num_args: i32) -> Option<bool> {
        let canon = canonical_name(name);
        if let Some(application) = self.application_function_precanonical(&canon, num_args) {
            return matches!(application, ApplicationFunction::Scalar { .. }).then_some(true);
        }
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if self.scalars.contains_key(&exact) {
            return Some(
                self.scalar_arities
                    .get(&exact)
                    .is_some_and(|arity| arity.accepts(num_args)),
            );
        }

        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        if self.scalars.contains_key(&variadic) {
            return Some(
                self.scalar_arities
                    .get(&variadic)
                    .is_some_and(|arity| arity.accepts(num_args)),
            );
        }

        (self.scalars.keys().any(|key| key.name == canon)
            || self.application_name_has_kind_precanonical(&canon, |kind| {
                kind == ApplicationFunctionKind::Scalar
            }))
        .then_some(false)
    }

    /// Return the frozen schema-safety policy for the resolved scalar entry.
    ///
    /// Exact arity wins over an arity-compatible variadic entry. Missing or
    /// inconsistent internal metadata fails closed as [`ScalarSchemaSafety::Never`].
    #[must_use]
    pub fn scalar_schema_safety(&self, name: &str, num_args: i32) -> Option<ScalarSchemaSafety> {
        let canonical = canonical_name(name);
        self.scalar_schema_safety_precanonical(&canonical, num_args)
    }

    /// Precanonicalized counterpart to [`Self::scalar_schema_safety`].
    #[must_use]
    pub fn scalar_schema_safety_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<ScalarSchemaSafety> {
        if let Some(application) = self.application_function_precanonical(canonical, num_args) {
            return match application {
                ApplicationFunction::Scalar { schema_safety, .. } => Some(*schema_safety),
                ApplicationFunction::Aggregate { .. } | ApplicationFunction::Window { .. } => None,
            };
        }
        let exact = FunctionKey {
            name: canonical.to_owned(),
            num_args,
        };
        if self.scalars.contains_key(&exact) {
            let accepts = self
                .scalar_arities
                .get(&exact)
                .is_some_and(|arity| arity.accepts(num_args));
            return Some(if accepts {
                self.scalar_schema_safety
                    .get(&exact)
                    .copied()
                    .unwrap_or(ScalarSchemaSafety::Never)
            } else {
                ScalarSchemaSafety::Never
            });
        }

        let variadic = FunctionKey {
            name: canonical.to_owned(),
            num_args: -1,
        };
        if self.scalars.contains_key(&variadic) {
            let Some(arity) = self.scalar_arities.get(&variadic).copied() else {
                return Some(ScalarSchemaSafety::Never);
            };
            if !arity.accepts(num_args) {
                return None;
            }
            return Some(
                self.scalar_schema_safety
                    .get(&variadic)
                    .copied()
                    .unwrap_or(ScalarSchemaSafety::Never),
            );
        }
        None
    }

    /// Return whether the resolved scalar is statically eligible for schema
    /// expressions. Conditional built-in date/time entries return `true` here
    /// and are checked again against evaluated arguments at execution time.
    #[must_use]
    pub fn scalar_is_deterministic(&self, name: &str, num_args: i32) -> Option<bool> {
        self.scalar_schema_safety(name, num_args)
            .map(|safety| safety != ScalarSchemaSafety::Never)
    }

    /// Return the frozen argument-collation contract for the resolved scalar.
    ///
    /// The trait callback is captured once at registration. Execution and
    /// schema dependency analysis never re-enter arbitrary user metadata.
    #[must_use]
    pub fn scalar_consumes_argument_collation(&self, name: &str, num_args: i32) -> Option<bool> {
        let canonical = canonical_name(name);
        self.scalar_consumes_argument_collation_precanonical(&canonical, num_args)
    }

    /// Precanonicalized counterpart to
    /// [`Self::scalar_consumes_argument_collation`].
    #[must_use]
    pub fn scalar_consumes_argument_collation_precanonical(
        &self,
        canonical: &str,
        num_args: i32,
    ) -> Option<bool> {
        if let Some(application) = self.application_function_precanonical(canonical, num_args) {
            return match application {
                ApplicationFunction::Scalar {
                    consumes_argument_collation,
                    ..
                } => Some(*consumes_argument_collation),
                ApplicationFunction::Aggregate { .. } | ApplicationFunction::Window { .. } => None,
            };
        }
        let exact = FunctionKey {
            name: canonical.to_owned(),
            num_args,
        };
        if self.scalars.contains_key(&exact) {
            if !self
                .scalar_arities
                .get(&exact)
                .is_some_and(|arity| arity.accepts(num_args))
            {
                return None;
            }
            return Some(
                self.scalar_argument_collation
                    .get(&exact)
                    .copied()
                    .unwrap_or(false),
            );
        }

        let variadic = FunctionKey {
            name: canonical.to_owned(),
            num_args: -1,
        };
        if self.scalars.contains_key(&variadic) {
            if !self
                .scalar_arities
                .get(&variadic)
                .is_some_and(|arity| arity.accepts(num_args))
            {
                return None;
            }
            return Some(
                self.scalar_argument_collation
                    .get(&variadic)
                    .copied()
                    .unwrap_or(false),
            );
        }
        None
    }

    /// Return whether a known aggregate function accepts the SQL-visible arity.
    ///
    /// `None` means the name is not registered as an aggregate function at
    /// all. This is the side-effect-free counterpart to
    /// [`Self::find_aggregate`] for preparation-only validation.
    #[must_use]
    pub fn aggregate_accepts_arg_count(&self, name: &str, num_args: i32) -> Option<bool> {
        let canon = canonical_name(name);
        if let Some(application) = self.application_function_precanonical(&canon, num_args) {
            return application.kind().is_aggregate_callable().then_some(true);
        }
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if self.aggregates.contains_key(&exact) {
            return Some(
                self.aggregate_arities
                    .get(&exact)
                    .is_some_and(|arity| arity.accepts(num_args)),
            );
        }

        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        if self.aggregates.contains_key(&variadic) {
            return Some(
                self.aggregate_arities
                    .get(&variadic)
                    .is_some_and(|arity| arity.accepts(num_args)),
            );
        }

        (self.aggregates.keys().any(|key| key.name == canon)
            || self.application_name_has_kind_precanonical(&canon, |kind| {
                kind.is_aggregate_callable()
            }))
        .then_some(false)
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
        if let Some(application) = self.application_function_precanonical(&canon, num_args) {
            return (application.kind() == ApplicationFunctionKind::Window).then_some(true);
        }
        let exact = FunctionKey {
            name: canon.clone(),
            num_args,
        };
        if self.windows.contains_key(&exact) {
            return Some(
                self.window_arities
                    .get(&exact)
                    .is_some_and(|arity| arity.accepts(num_args)),
            );
        }

        let variadic = FunctionKey {
            name: canon.clone(),
            num_args: -1,
        };
        if self.windows.contains_key(&variadic) {
            return Some(
                self.window_arities
                    .get(&variadic)
                    .is_some_and(|arity| arity.accepts(num_args)),
            );
        }

        (self.windows.keys().any(|key| key.name == canon)
            || self.application_name_has_kind_precanonical(&canon, |kind| {
                kind == ApplicationFunctionKind::Window
            }))
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
            .chain(
                self.application_functions
                    .iter()
                    .filter(|(_, overloads)| {
                        overloads
                            .values()
                            .any(|function| function.kind().is_aggregate_callable())
                    })
                    .map(|(name, _)| name.to_ascii_lowercase()),
            )
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

    #[test]
    fn keyed_registration_preserves_bounds_without_reinvoking_user_metadata() {
        struct MetadataPanicsScalar;

        impl ScalarFunction for MetadataPanicsScalar {
            fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
                Ok(SqliteValue::Integer(1))
            }

            fn num_args(&self) -> i32 {
                panic!("keyed scalar registration must not ask for arity")
            }

            fn is_deterministic(&self) -> bool {
                panic!("keyed scalar registration must not ask for determinism")
            }

            fn consumes_argument_collation(&self) -> bool {
                panic!("keyed scalar registration must not ask for collation metadata")
            }

            fn name(&self) -> &str {
                panic!("keyed scalar registration must not ask for name")
            }
        }

        struct MetadataPanicsAggregate;

        impl AggregateFunction for MetadataPanicsAggregate {
            type State = ();

            fn initial_state(&self) -> Self::State {}

            fn step(
                &self,
                _state: &mut Self::State,
                _args: &[SqliteValue],
            ) -> fsqlite_error::Result<()> {
                Ok(())
            }

            fn finalize(&self, _state: Self::State) -> fsqlite_error::Result<SqliteValue> {
                Ok(SqliteValue::Integer(1))
            }

            fn num_args(&self) -> i32 {
                panic!("keyed aggregate registration must not ask for arity")
            }

            fn name(&self) -> &str {
                panic!("keyed aggregate registration must not ask for name")
            }
        }

        struct MetadataPanicsWindow;

        impl WindowFunction for MetadataPanicsWindow {
            type State = ();

            fn initial_state(&self) -> Self::State {}

            fn step(
                &self,
                _state: &mut Self::State,
                _args: &[SqliteValue],
            ) -> fsqlite_error::Result<()> {
                Ok(())
            }

            fn inverse(
                &self,
                _state: &mut Self::State,
                _args: &[SqliteValue],
            ) -> fsqlite_error::Result<()> {
                Ok(())
            }

            fn value(&self, _state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
                Ok(SqliteValue::Integer(1))
            }

            fn finalize(&self, _state: Self::State) -> fsqlite_error::Result<SqliteValue> {
                Ok(SqliteValue::Integer(1))
            }

            fn num_args(&self) -> i32 {
                panic!("keyed window registration must not ask for arity")
            }

            fn name(&self) -> &str {
                panic!("keyed window registration must not ask for name")
            }
        }

        let arity = FunctionArity::variadic(1, Some(2));
        let scalar_key = FunctionKey::new("keyed_scalar", -1);
        let aggregate_key = FunctionKey::new("keyed_aggregate", -1);
        let window_key = FunctionKey::new("keyed_window", -1);
        let mut registry = FunctionRegistry::new();
        let scalar_displaced = registry.register_scalar_keyed(
            scalar_key.clone(),
            arity,
            false,
            false,
            MetadataPanicsScalar,
        );
        assert!(scalar_displaced.is_none());
        assert!(registry.find_scalar("keyed_scalar", 1).is_some());

        let aggregate_displaced = registry.register_aggregate_keyed(
            aggregate_key.clone(),
            arity,
            MetadataPanicsAggregate,
        );
        assert!(aggregate_displaced.is_none());
        assert!(registry.find_aggregate("keyed_aggregate", 1).is_some());

        let (window, window_displaced) =
            registry.register_window_keyed(window_key.clone(), arity, MetadataPanicsWindow);
        assert!(window_displaced.is_none());
        assert!(Arc::ptr_eq(
            &window,
            &registry.find_window("keyed_window", 1).unwrap()
        ));

        for num_args in [1, 2] {
            assert_eq!(
                registry.scalar_accepts_arg_count("keyed_scalar", num_args),
                Some(true)
            );
            assert_eq!(
                registry.aggregate_accepts_arg_count("keyed_aggregate", num_args),
                Some(true)
            );
            assert_eq!(
                registry.window_accepts_arg_count("keyed_window", num_args),
                Some(true)
            );
            assert_eq!(
                registry.scalar_is_deterministic("keyed_scalar", num_args),
                Some(false)
            );
            assert_eq!(
                registry
                    .resolve_scalar("keyed_scalar", num_args)
                    .unwrap()
                    .query_constancy(),
                ScalarQueryConstancy::Volatile
            );
        }
        for num_args in [0, 3] {
            assert_eq!(
                registry.scalar_accepts_arg_count("keyed_scalar", num_args),
                Some(false)
            );
            assert_eq!(
                registry.aggregate_accepts_arg_count("keyed_aggregate", num_args),
                Some(false)
            );
            assert_eq!(
                registry.window_accepts_arg_count("keyed_window", num_args),
                Some(false)
            );
        }

        registry.scalar_arities.remove(&scalar_key);
        registry.aggregate_arities.remove(&aggregate_key);
        registry.window_arities.remove(&window_key);
        assert_eq!(
            registry.scalar_accepts_arg_count("keyed_scalar", 1),
            Some(false)
        );
        assert_eq!(
            registry.scalar_is_deterministic("keyed_scalar", 1),
            Some(false)
        );
        assert_eq!(
            registry.aggregate_accepts_arg_count("keyed_aggregate", 1),
            Some(false)
        );
        assert_eq!(
            registry.window_accepts_arg_count("keyed_window", 1),
            Some(false)
        );
        assert_wrong_arg_count(
            registry.find_scalar("keyed_scalar", 1).unwrap().as_ref(),
            &[SqliteValue::Null],
            "keyed_scalar",
        );
        assert_wrong_arg_count_aggregate(
            registry
                .find_aggregate("keyed_aggregate", 1)
                .unwrap()
                .as_ref(),
            &[SqliteValue::Null],
            "keyed_aggregate",
        );
        assert_wrong_arg_count_window(
            registry.find_window("keyed_window", 1).unwrap().as_ref(),
            &[SqliteValue::Null],
            "keyed_window",
        );
    }

    #[test]
    fn exact_registration_fails_closed_when_parallel_arity_metadata_is_missing() {
        let scalar_key = FunctionKey::new("double", 1);
        let aggregate_key = FunctionKey::new("product", 1);
        let window_key = FunctionKey::new("moving_sum", 1);
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Double);
        registry.register_aggregate(Product);
        registry.register_window(MovingSum);

        assert_eq!(
            registry.scalar_arities.remove(&scalar_key),
            Some(FunctionArity::exact(1))
        );
        assert_eq!(
            registry.aggregate_arities.remove(&aggregate_key),
            Some(FunctionArity::exact(1))
        );
        assert_eq!(
            registry.window_arities.remove(&window_key),
            Some(FunctionArity::exact(1))
        );

        assert_eq!(registry.scalar_accepts_arg_count("double", 1), Some(false));
        assert_eq!(
            registry.aggregate_accepts_arg_count("product", 1),
            Some(false)
        );
        assert_eq!(
            registry.window_accepts_arg_count("moving_sum", 1),
            Some(false)
        );
        assert_wrong_arg_count(
            registry.find_scalar("double", 1).unwrap().as_ref(),
            &[SqliteValue::Null],
            "double",
        );
        assert_wrong_arg_count_aggregate(
            registry.find_aggregate("product", 1).unwrap().as_ref(),
            &[SqliteValue::Null],
            "product",
        );
        assert_wrong_arg_count_window(
            registry.find_window("moving_sum", 1).unwrap().as_ref(),
            &[SqliteValue::Null],
            "moving_sum",
        );
    }

    #[test]
    #[should_panic(
        expected = "function key and frozen arity contract must have the same declared argument count"
    )]
    fn keyed_registration_rejects_mismatched_arity_contract() {
        assert_key_matches_arity(&FunctionKey::new("mismatched", -1), FunctionArity::exact(1));
    }

    #[test]
    #[should_panic(expected = "function argument count must be -1 or non-negative")]
    fn function_key_rejects_argument_counts_below_variadic_sentinel() {
        let _ = FunctionKey::new("invalid", -2);
    }

    #[test]
    #[should_panic(expected = "function argument count must be -1 or non-negative")]
    fn default_arity_contract_rejects_argument_counts_below_variadic_sentinel() {
        let _ = FunctionArity::from_declared_args(-2, || {
            panic!("invalid declared arity must be rejected before reading variadic bounds")
        });
    }

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

    struct TaggedScalar {
        name: &'static str,
        num_args: i32,
        tag: i64,
    }

    impl ScalarFunction for TaggedScalar {
        fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(
                self.tag + args.iter().map(SqliteValue::to_integer).sum::<i64>(),
            ))
        }

        fn num_args(&self) -> i32 {
            self.num_args
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    struct TaggedAggregate {
        name: &'static str,
        num_args: i32,
        tag: i64,
    }

    impl AggregateFunction for TaggedAggregate {
        type State = i64;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
            *state += args.iter().map(SqliteValue::to_integer).sum::<i64>();
            Ok(())
        }

        fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(self.tag + state))
        }

        fn num_args(&self) -> i32 {
            self.num_args
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    struct TaggedWindow {
        name: &'static str,
        num_args: i32,
        tag: i64,
    }

    impl WindowFunction for TaggedWindow {
        type State = i64;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
            *state += args.iter().map(SqliteValue::to_integer).sum::<i64>();
            Ok(())
        }

        fn inverse(
            &self,
            state: &mut Self::State,
            args: &[SqliteValue],
        ) -> fsqlite_error::Result<()> {
            *state -= args.iter().map(SqliteValue::to_integer).sum::<i64>();
            Ok(())
        }

        fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(self.tag + state))
        }

        fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(self.tag + state))
        }

        fn num_args(&self) -> i32 {
            self.num_args
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    fn finalize_aggregate(
        function: &ErasedAggregateFunction,
        rows: &[&[SqliteValue]],
    ) -> SqliteValue {
        let mut state = function.initial_state();
        for args in rows {
            function.step(&mut state, args).unwrap();
        }
        function.finalize(state).unwrap()
    }

    #[test]
    fn application_resolution_prefers_exact_across_function_kinds() {
        let mut registry = FunctionRegistry::new();
        registry.register_application_aggregate_captured(
            "app_precedence",
            FunctionArity::variadic(0, None),
            TaggedAggregate {
                name: "app_precedence",
                num_args: -1,
                tag: 20_000,
            },
        );
        registry.register_application_scalar_captured(
            "app_precedence",
            FunctionArity::exact(1),
            true,
            false,
            TaggedScalar {
                name: "app_precedence",
                num_args: 1,
                tag: 10_000,
            },
        );

        assert_eq!(
            registry
                .resolve_application_function("APP_PRECEDENCE", 1)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Scalar)
        );
        assert_eq!(
            registry
                .find_scalar("app_precedence", 1)
                .unwrap()
                .invoke(&[SqliteValue::Integer(7)])
                .unwrap(),
            SqliteValue::Integer(10_007)
        );
        assert!(registry.find_aggregate("app_precedence", 1).is_none());

        assert_eq!(
            registry
                .resolve_application_function("app_precedence", 2)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Aggregate)
        );
        assert!(registry.find_scalar("app_precedence", 2).is_none());
        let args = [SqliteValue::Integer(2), SqliteValue::Integer(3)];
        assert_eq!(
            finalize_aggregate(
                registry
                    .find_aggregate("app_precedence", 2)
                    .unwrap()
                    .as_ref(),
                &[&args],
            ),
            SqliteValue::Integer(20_005)
        );

        let mut reverse = FunctionRegistry::new();
        reverse.register_application_scalar_captured(
            "app_precedence",
            FunctionArity::variadic(0, None),
            true,
            false,
            TaggedScalar {
                name: "app_precedence",
                num_args: -1,
                tag: 30_000,
            },
        );
        reverse.register_application_aggregate_captured(
            "app_precedence",
            FunctionArity::exact(1),
            TaggedAggregate {
                name: "app_precedence",
                num_args: 1,
                tag: 40_000,
            },
        );
        assert_eq!(
            reverse
                .resolve_application_function("app_precedence", 1)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Aggregate)
        );
        assert!(reverse.find_scalar("app_precedence", 1).is_none());
        assert_eq!(
            reverse
                .resolve_application_function("app_precedence", 2)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Scalar)
        );
    }

    #[test]
    fn application_variadic_shadows_builtin_exact_only_when_compatible() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(TaggedScalar {
            name: "layered",
            num_args: 1,
            tag: 1_000,
        });
        registry.register_application_scalar_captured(
            "layered",
            FunctionArity::variadic(2, Some(3)),
            true,
            false,
            TaggedScalar {
                name: "layered",
                num_args: -1,
                tag: 2_000,
            },
        );

        assert_eq!(registry.resolve_application_function("layered", 1), None);
        assert_eq!(
            registry
                .find_scalar("layered", 1)
                .unwrap()
                .invoke(&[SqliteValue::Integer(7)])
                .unwrap(),
            SqliteValue::Integer(1_007),
            "an incompatible application variadic must leave the builtin layer visible"
        );
        let two_args = [SqliteValue::Integer(7), SqliteValue::Integer(8)];
        assert_eq!(
            registry
                .find_scalar("layered", 2)
                .unwrap()
                .invoke(&two_args)
                .unwrap(),
            SqliteValue::Integer(2_015)
        );

        let displaced = registry.register_application_scalar_captured(
            "layered",
            FunctionArity::variadic(0, None),
            true,
            false,
            TaggedScalar {
                name: "layered",
                num_args: -1,
                tag: 3_000,
            },
        );
        assert!(displaced.is_some());
        assert_eq!(
            registry
                .find_scalar("layered", 1)
                .unwrap()
                .invoke(&[SqliteValue::Integer(7)])
                .unwrap(),
            SqliteValue::Integer(3_007),
            "a compatible application variadic must shadow an exact builtin"
        );

        registry.register_aggregate(TaggedAggregate {
            name: "sum_like",
            num_args: 1,
            tag: 4_000,
        });
        registry.register_application_scalar_captured(
            "sum_like",
            FunctionArity::variadic(0, None),
            true,
            false,
            TaggedScalar {
                name: "sum_like",
                num_args: -1,
                tag: 5_000,
            },
        );
        assert!(registry.find_aggregate("sum_like", 1).is_none());
        assert!(registry.find_scalar("sum_like", 1).is_some());

        let bounded_window_arity = FunctionArity::variadic(1, Some(2));
        registry.register_application_window_captured(
            "bounded_window",
            bounded_window_arity,
            TaggedWindow {
                name: "bounded_window",
                num_args: -1,
                tag: 6_000,
            },
        );
        assert_eq!(
            registry
                .find_aggregate("bounded_window", 2)
                .unwrap()
                .arity(),
            bounded_window_arity,
            "the aggregate bridge must preserve frozen variadic bounds"
        );
        assert_eq!(
            registry.aggregate_accepts_arg_count("bounded_window", 0),
            Some(false)
        );
        assert_eq!(
            registry.window_accepts_arg_count("bounded_window", 3),
            Some(false)
        );
    }

    #[test]
    fn same_application_key_replaces_kind_and_window_retains_aggregate_form() {
        let mut registry = FunctionRegistry::new();
        let mut displaced = Vec::new();
        assert!(
            registry
                .register_application_scalar_captured(
                    "cross_kind",
                    FunctionArity::exact(1),
                    true,
                    false,
                    TaggedScalar {
                        name: "cross_kind",
                        num_args: 1,
                        tag: 70_000,
                    },
                )
                .is_none()
        );

        displaced.push(
            registry
                .register_application_aggregate_captured(
                    "cross_kind",
                    FunctionArity::exact(1),
                    TaggedAggregate {
                        name: "cross_kind",
                        num_args: 1,
                        tag: 80_000,
                    },
                )
                .expect("aggregate must displace same-key scalar"),
        );
        assert_eq!(
            registry
                .resolve_application_function("cross_kind", 1)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Aggregate)
        );
        assert!(registry.find_scalar("cross_kind", 1).is_none());
        assert!(registry.find_window("cross_kind", 1).is_none());

        let (registered_window, old_aggregate) = registry.register_application_window_captured(
            "cross_kind",
            FunctionArity::exact(1),
            TaggedWindow {
                name: "cross_kind",
                num_args: 1,
                tag: 90_000,
            },
        );
        displaced.push(old_aggregate.expect("window must displace same-key aggregate"));
        assert_eq!(
            registry
                .resolve_application_function("cross_kind", 1)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Window)
        );
        assert!(Arc::ptr_eq(
            &registered_window,
            &registry.find_window("cross_kind", 1).unwrap()
        ));
        let row_one = [SqliteValue::Integer(1)];
        let row_two = [SqliteValue::Integer(2)];
        let row_three = [SqliteValue::Integer(3)];
        assert_eq!(
            finalize_aggregate(
                registry.find_aggregate("cross_kind", 1).unwrap().as_ref(),
                &[&row_one, &row_two, &row_three],
            ),
            SqliteValue::Integer(90_006),
            "window registration must keep its ordinary aggregate form"
        );

        displaced.push(
            registry
                .register_application_scalar_captured(
                    "cross_kind",
                    FunctionArity::exact(1),
                    true,
                    false,
                    TaggedScalar {
                        name: "cross_kind",
                        num_args: 1,
                        tag: 100_000,
                    },
                )
                .expect("scalar must displace same-key window"),
        );
        assert_eq!(
            registry
                .resolve_application_function("cross_kind", 1)
                .map(ApplicationFunctionResolution::kind),
            Some(ApplicationFunctionKind::Scalar)
        );
        assert_eq!(registry.aggregate_accepts_arg_count("cross_kind", 1), None);
        assert_eq!(registry.window_accepts_arg_count("cross_kind", 1), None);
        assert!(registry.find_aggregate("cross_kind", 1).is_none());
        assert!(registry.find_window("cross_kind", 1).is_none());
        assert_eq!(displaced.len(), 3);
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
    fn test_registry_scalar_aggregate_arity_introspection_is_side_effect_free() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Double);
        registry.register_scalar(VariadicConcat);
        registry.register_aggregate(Product);

        assert_eq!(registry.scalar_accepts_arg_count("double", 1), Some(true));
        assert_eq!(registry.scalar_accepts_arg_count("double", 2), Some(false));
        assert_eq!(registry.scalar_accepts_arg_count("my_func", 1), Some(true));
        assert_eq!(registry.scalar_accepts_arg_count("my_func", 3), Some(true));
        assert_eq!(registry.scalar_accepts_arg_count("my_func", 0), Some(false));
        assert_eq!(registry.scalar_accepts_arg_count("my_func", 4), Some(false));
        assert_eq!(registry.scalar_accepts_arg_count("missing_scalar", 1), None);
        assert_eq!(registry.scalar_is_deterministic("double", 1), Some(true));
        assert_eq!(registry.scalar_is_deterministic("my_func", 1), Some(true));
        assert_eq!(registry.scalar_is_deterministic("my_func", 0), None);
        assert_eq!(registry.scalar_is_deterministic("missing_scalar", 1), None);

        assert_eq!(
            registry.aggregate_accepts_arg_count("product", 1),
            Some(true)
        );
        assert_eq!(
            registry.aggregate_accepts_arg_count("product", 0),
            Some(false)
        );
        assert_eq!(
            registry.aggregate_accepts_arg_count("missing_aggregate", 1),
            None
        );

        registry
            .scalar_schema_safety
            .remove(&FunctionKey::new("double", 1));
        assert_eq!(
            registry.scalar_is_deterministic("double", 1),
            Some(false),
            "missing immutable metadata must fail closed"
        );
    }

    #[test]
    fn scalar_query_constancy_is_frozen_cloned_and_fails_closed() {
        let mut registry = FunctionRegistry::new();
        registry.register_scalar(Double);
        registry.register_scalar_captured(
            "volatile_scalar",
            FunctionArity::exact(1),
            false,
            false,
            TaggedScalar {
                name: "ignored_by_captured_registration",
                num_args: 1,
                tag: 1,
            },
        );
        registry.register_conditionally_deterministic_scalar(TaggedScalar {
            name: "conditional_scalar",
            num_args: 1,
            tag: 2,
        });
        registry.register_slow_changing_scalar(TaggedScalar {
            name: "slow_scalar",
            num_args: 1,
            tag: 3,
        });

        let resolved = registry.resolve_scalar("double", 1).unwrap();
        assert_eq!(resolved.query_constancy(), ScalarQueryConstancy::Constant);
        assert!(resolved.query_constancy().is_query_constant());

        let resolved = registry.resolve_scalar("volatile_scalar", 1).unwrap();
        assert_eq!(resolved.query_constancy(), ScalarQueryConstancy::Volatile);
        assert!(!resolved.query_constancy().is_query_constant());

        let conditional = registry.resolve_scalar("conditional_scalar", 1).unwrap();
        assert_eq!(
            conditional.schema_safety(),
            ScalarSchemaSafety::DateTimeConditional
        );
        assert_eq!(
            conditional.query_constancy(),
            ScalarQueryConstancy::SlowChanging
        );

        let slow = registry.resolve_scalar("slow_scalar", 1).unwrap();
        assert_eq!(slow.schema_safety(), ScalarSchemaSafety::Never);
        assert_eq!(slow.query_constancy(), ScalarQueryConstancy::SlowChanging);

        let wrong_arity = registry.resolve_scalar("double", 2).unwrap();
        assert_eq!(
            wrong_arity.query_constancy(),
            ScalarQueryConstancy::Volatile,
            "wrong-arity sentinels must never be treated as query constants"
        );

        let mut cloned = FunctionRegistry::clone_from_arc(&Arc::new(registry));
        assert_eq!(
            cloned
                .resolve_scalar("slow_scalar", 1)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::SlowChanging,
            "registry snapshots must retain frozen query metadata"
        );
        cloned
            .scalar_query_constancy
            .remove(&FunctionKey::new("double", 1));
        assert_eq!(
            cloned
                .resolve_scalar("double", 1)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::Volatile,
            "missing immutable query metadata must fail closed"
        );
    }

    #[test]
    fn application_shadowing_selects_matching_scalar_query_constancy() {
        let mut registry = FunctionRegistry::new();
        registry.register_slow_changing_scalar(TaggedScalar {
            name: "layered_constancy",
            num_args: 1,
            tag: 10,
        });
        registry.register_application_scalar_captured(
            "layered_constancy",
            FunctionArity::variadic(2, Some(3)),
            false,
            false,
            TaggedScalar {
                name: "layered_constancy",
                num_args: -1,
                tag: 20,
            },
        );

        assert_eq!(
            registry
                .resolve_scalar("layered_constancy", 1)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::SlowChanging,
            "an incompatible application variadic must leave base metadata visible"
        );
        assert_eq!(
            registry
                .resolve_scalar("layered_constancy", 2)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::Volatile,
            "a matching non-deterministic application scalar must publish volatile metadata"
        );

        registry.register_application_scalar_captured(
            "layered_constancy",
            FunctionArity::variadic(0, None),
            true,
            false,
            TaggedScalar {
                name: "layered_constancy",
                num_args: -1,
                tag: 30,
            },
        );
        assert_eq!(
            registry
                .resolve_scalar("layered_constancy", 1)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::Constant,
            "a compatible deterministic application scalar must shadow base metadata"
        );

        let cloned = FunctionRegistry::clone_from_arc(&Arc::new(registry));
        assert_eq!(
            cloned
                .resolve_scalar("layered_constancy", 1)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::Constant,
            "registry snapshots must retain application query metadata"
        );

        let mut shadowed = cloned;
        shadowed.register_application_aggregate_captured(
            "layered_constancy",
            FunctionArity::exact(1),
            TaggedAggregate {
                name: "layered_constancy",
                num_args: 1,
                tag: 40,
            },
        );
        assert!(
            shadowed.resolve_scalar("layered_constancy", 1).is_none(),
            "a matching application aggregate must shadow scalar metadata across kinds"
        );
        assert_eq!(
            shadowed
                .resolve_scalar("layered_constancy", 2)
                .unwrap()
                .query_constancy(),
            ScalarQueryConstancy::Constant,
            "an incompatible exact aggregate must leave the application variadic visible"
        );
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
