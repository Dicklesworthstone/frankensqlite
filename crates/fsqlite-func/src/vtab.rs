//! Virtual table and cursor traits (§9.3).
//!
//! Virtual tables expose external data sources as SQL tables. They follow
//! the SQLite xCreate/xConnect/xBestIndex/xFilter/xNext protocol.
//!
//! These traits are **open** (user-implementable). Extension authors
//! implement them to create custom virtual table modules.
//!
//! # Cx on I/O Methods
//!
//! Methods that perform I/O accept `&Cx` for cancellation and deadline
//! propagation. Lightweight accessors (`eof`, `column`, `rowid`) do not
//! require `&Cx` since they operate on already-fetched row data.

use std::any::Any;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::SqliteValue;
use fsqlite_types::cx::Cx;

// ---------------------------------------------------------------------------
// Query planner types
// ---------------------------------------------------------------------------

/// Comparison operator for an index constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintOp {
    Eq,
    Gt,
    Le,
    Lt,
    Ge,
    Match,
    Like,
    Glob,
    Regexp,
    Ne,
    IsNot,
    IsNotNull,
    IsNull,
    Is,
}

/// A single constraint from the WHERE clause that the planner is considering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConstraint {
    /// Column index (0-based; `-1` for rowid).
    pub column: i32,
    /// The comparison operator.
    pub op: ConstraintOp,
    /// Whether the planner considers this constraint usable.
    pub usable: bool,
}

/// A single ORDER BY term from the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexOrderBy {
    /// Column index (0-based).
    pub column: i32,
    /// `true` if descending, `false` if ascending.
    pub desc: bool,
}

/// Per-constraint usage information set by `best_index`.
#[derive(Debug, Clone, Default)]
pub struct IndexConstraintUsage {
    /// 1-based index into the `args` array passed to `filter`.
    /// Non-positive values mean this constraint supplies no argument. All
    /// positive values returned by one `best_index` call must be unique and
    /// form a contiguous sequence starting at 1.
    pub argv_index: i32,
    /// If `true`, the vtab guarantees this constraint is satisfied and
    /// the core need not double-check it. This is only effective when
    /// `argv_index` is positive; constraints that supply no filter argument
    /// remain subject to core evaluation.
    pub omit: bool,
}

/// Information exchanged between the query planner and virtual table
/// during index selection.
///
/// The planner fills `constraints` and `order_by`. The vtab fills
/// `constraint_usage`, `idx_num`, `idx_str`, `order_by_consumed`,
/// `estimated_cost`, and `estimated_rows`.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// WHERE clause constraints the planner is considering. This is read-only
    /// input: implementations must not mutate, reorder, or resize it.
    pub constraints: Vec<IndexConstraint>,
    /// ORDER BY terms from the query. This is read-only input and must not be
    /// mutated, reordered, or resized.
    pub order_by: Vec<IndexOrderBy>,
    /// How each constraint maps to filter arguments (vtab fills this). The
    /// vector length is fixed by the core and must remain equal to
    /// `constraints.len()`.
    pub constraint_usage: Vec<IndexConstraintUsage>,
    /// Integer identifier for the chosen index strategy.
    pub idx_num: i32,
    /// Optional string identifier for the chosen index strategy.
    pub idx_str: Option<String>,
    /// Whether the vtab guarantees the output is already sorted.
    pub order_by_consumed: bool,
    /// Estimated cost of the scan (lower is better).
    pub estimated_cost: f64,
    /// Estimated number of rows returned.
    pub estimated_rows: i64,
}

impl IndexInfo {
    /// Create a new `IndexInfo` with the given constraints and order-by terms.
    #[must_use]
    pub fn new(constraints: Vec<IndexConstraint>, order_by: Vec<IndexOrderBy>) -> Self {
        let usage_len = constraints.len();
        Self {
            constraints,
            order_by,
            constraint_usage: vec![IndexConstraintUsage::default(); usage_len],
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1_000_000.0,
            estimated_rows: 1_000_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Column context
// ---------------------------------------------------------------------------

/// A context object passed to [`VirtualTableCursor::column`] for writing
/// the column value.
///
/// Analogous to C SQLite's `sqlite3_context*` used with `sqlite3_result_*`.
#[derive(Debug, Default)]
pub struct ColumnContext {
    value: Option<SqliteValue>,
}

impl ColumnContext {
    /// Create a new empty column context.
    #[must_use]
    pub fn new() -> Self {
        Self { value: None }
    }

    /// Set the value for this column.
    pub fn set_value(&mut self, val: SqliteValue) {
        self.value = Some(val);
    }

    /// Take the value out of this context, leaving `None`.
    pub fn take_value(&mut self) -> Option<SqliteValue> {
        self.value.take()
    }
}

/// Snapshot-backed transaction/savepoint state for mutable virtual tables.
///
/// Virtual table implementations that keep their authoritative state in memory
/// can use this helper to participate in connection-level `BEGIN`/`COMMIT`/
/// `ROLLBACK` and savepoint recovery without wiring their own savepoint stack.
#[derive(Debug, Clone)]
pub struct TransactionalVtabState<S: Clone> {
    base_snapshot: Option<S>,
    savepoints: Vec<(i32, S)>,
}

impl<S: Clone> Default for TransactionalVtabState<S> {
    fn default() -> Self {
        Self {
            base_snapshot: None,
            savepoints: Vec::new(),
        }
    }
}

impl<S: Clone> TransactionalVtabState<S> {
    /// Mark the start of a virtual-table transaction.
    pub fn begin(&mut self, snapshot: S) {
        if self.base_snapshot.is_none() {
            self.base_snapshot = Some(snapshot);
            self.savepoints.clear();
        }
    }

    /// Drop all transactional snapshots after a successful commit.
    pub fn commit(&mut self) {
        self.base_snapshot = None;
        self.savepoints.clear();
    }

    /// Return the transaction-begin snapshot for a full rollback.
    pub fn rollback(&mut self) -> Option<S> {
        let snapshot = self.base_snapshot.take();
        self.savepoints.clear();
        snapshot
    }

    /// Record the current state at savepoint `level`.
    pub fn savepoint(&mut self, level: i32, snapshot: S) {
        if self.base_snapshot.is_none() {
            return;
        }
        self.savepoints.retain(|(existing, _)| *existing < level);
        self.savepoints.push((level, snapshot));
    }

    /// Drop savepoint snapshots at `level` and deeper.
    pub fn release(&mut self, level: i32) {
        if self.base_snapshot.is_none() {
            return;
        }
        self.savepoints.retain(|(existing, _)| *existing < level);
    }

    /// Return the snapshot recorded for savepoint `level`, keeping that
    /// savepoint active and discarding deeper ones.
    ///
    /// If the virtual table joined the transaction after outer savepoints were
    /// already active, SQLite only gives it a snapshot for the current level.
    /// Falling back to the transaction-begin snapshot lets `ROLLBACK TO` an
    /// older savepoint restore the correct pre-transaction state.
    pub fn rollback_to(&mut self, level: i32) -> Option<S> {
        self.base_snapshot.as_ref()?;
        let snapshot = self
            .savepoints
            .iter()
            .rfind(|(existing, _)| *existing == level)
            .map(|(_, snapshot)| snapshot.clone())
            .or_else(|| self.base_snapshot.clone());
        if snapshot.is_some() {
            self.savepoints.retain(|(existing, _)| *existing <= level);
        }
        snapshot
    }
}

// ---------------------------------------------------------------------------
// Module metadata
// ---------------------------------------------------------------------------

/// Classification for a schema object named by a virtual-table module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShadowTableKind {
    /// The name is not a module-owned shadow table.
    #[default]
    Ordinary,
    /// The name is a module-owned shadow table.
    Shadow,
}

/// Access decision for a shadow-table operation class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShadowTableAccess {
    /// The operation is permitted by the module policy.
    #[default]
    Allow,
    /// The operation is rejected by the module policy.
    Deny,
}

impl ShadowTableAccess {
    /// Whether the access decision permits the operation.
    #[must_use]
    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// Policy returned by a module when the core asks whether a table name is a
/// shadow table of a virtual table instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowTablePolicy {
    /// Whether the table is ordinary or shadow-owned.
    pub kind: ShadowTableKind,
    /// User-authored INSERT/UPDATE/DELETE against this schema object.
    pub direct_dml: ShadowTableAccess,
    /// User-authored schema changes such as CREATE TRIGGER on this object.
    pub schema_ddl: ShadowTableAccess,
    /// Writes performed by the owning module while applying its own lifecycle.
    pub module_internal_write: ShadowTableAccess,
}

impl ShadowTablePolicy {
    /// Policy for an ordinary, non-shadow table.
    #[must_use]
    pub const fn ordinary() -> Self {
        Self {
            kind: ShadowTableKind::Ordinary,
            direct_dml: ShadowTableAccess::Allow,
            schema_ddl: ShadowTableAccess::Allow,
            module_internal_write: ShadowTableAccess::Allow,
        }
    }

    /// Policy for a module-owned shadow table.
    #[must_use]
    pub const fn owned_shadow() -> Self {
        Self {
            kind: ShadowTableKind::Shadow,
            direct_dml: ShadowTableAccess::Deny,
            schema_ddl: ShadowTableAccess::Deny,
            module_internal_write: ShadowTableAccess::Allow,
        }
    }

    /// Whether the table is module-owned shadow state.
    #[must_use]
    pub const fn is_shadow(self) -> bool {
        matches!(self.kind, ShadowTableKind::Shadow)
    }

    /// Whether user-authored INSERT/UPDATE/DELETE should be accepted.
    #[must_use]
    pub const fn allows_direct_dml(self) -> bool {
        self.direct_dml.is_allowed()
    }

    /// Whether user-authored schema DDL such as CREATE TRIGGER should be accepted.
    #[must_use]
    pub const fn allows_schema_ddl(self) -> bool {
        self.schema_ddl.is_allowed()
    }

    /// Whether the owning module may write this object internally.
    #[must_use]
    pub const fn allows_module_internal_write(self) -> bool {
        self.module_internal_write.is_allowed()
    }
}

impl Default for ShadowTablePolicy {
    fn default() -> Self {
        Self::ordinary()
    }
}

/// Lifecycle shape a module exposes to the connection/catalog layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VtabLifecyclePolicy {
    /// `create` and `connect` are effectively the same operation.
    #[default]
    Simple,
    /// The module distinguishes create-time and connect-time lifecycle.
    SeparateCreateAndConnect,
}

/// Integrity surface advertised by a module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VtabIntegrityPolicy {
    /// No module-specific integrity entry point is exposed.
    #[default]
    None,
    /// Integrity checks are module-defined and may inspect shadow state.
    ShadowAware,
}

/// Defensive/risk metadata analogous to SQLite's vtab safety flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VtabRiskLevel {
    /// Safe to invoke in defensive contexts.
    pub innocuous: bool,
    /// Must not be invoked from schema or trigger contexts.
    pub direct_only: bool,
    /// May consult objects outside the current schema.
    pub uses_all_schemas: bool,
}

impl VtabRiskLevel {
    /// Risk profile for an innocuous module.
    #[must_use]
    pub const fn innocuous() -> Self {
        Self {
            innocuous: true,
            direct_only: false,
            uses_all_schemas: false,
        }
    }
}

/// Module-level metadata that future catalog and defensive checks can consult
/// without hard-coding FTS5-specific behavior in unrelated layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VtabModuleMetadata {
    /// Whether the module owns any shadow tables.
    pub owns_shadow_tables: bool,
    /// Whether create/connect semantics differ.
    pub lifecycle: VtabLifecyclePolicy,
    /// Whether the module exposes integrity hooks.
    pub integrity: VtabIntegrityPolicy,
    /// Defensive-execution metadata.
    pub risk: VtabRiskLevel,
}

impl VtabModuleMetadata {
    /// Metadata for ordinary modules with no shadow-table contract.
    #[must_use]
    pub const fn ordinary() -> Self {
        Self {
            owns_shadow_tables: false,
            lifecycle: VtabLifecyclePolicy::Simple,
            integrity: VtabIntegrityPolicy::None,
            risk: VtabRiskLevel::innocuous(),
        }
    }

    /// Metadata for a shadow-owning module.
    #[must_use]
    pub const fn shadow_owning(
        lifecycle: VtabLifecyclePolicy,
        integrity: VtabIntegrityPolicy,
        risk: VtabRiskLevel,
    ) -> Self {
        Self {
            owns_shadow_tables: true,
            lifecycle,
            integrity,
            risk,
        }
    }
}

impl Default for VtabModuleMetadata {
    fn default() -> Self {
        Self::ordinary()
    }
}

// ---------------------------------------------------------------------------
// VirtualTable trait
// ---------------------------------------------------------------------------

/// A virtual table module.
///
/// Virtual tables expose external data sources as SQL tables. This trait
/// covers the full lifecycle: creation, connection, scanning, mutation,
/// and destruction.
///
/// This trait is **open** (user-implementable). The `Sized` bound on
/// constructor methods (`create`, `connect`) allows the trait to be used
/// as `dyn VirtualTable<Cursor = C>` for other methods.
///
/// # Default Implementations
///
/// Most methods have sensible defaults. At minimum, you must implement
/// `connect`, `best_index`, and `open`.
#[allow(clippy::missing_errors_doc)]
pub trait VirtualTable: Send + Sync {
    /// The cursor type for scanning this virtual table.
    type Cursor: VirtualTableCursor;

    /// Static metadata for the module as a whole.
    fn module_metadata(_args: &[&str]) -> VtabModuleMetadata
    where
        Self: Sized,
    {
        VtabModuleMetadata::ordinary()
    }

    /// Determine whether `table_name` is a module-owned shadow table for the
    /// virtual table instance named `vtab_name`.
    fn shadow_table_policy(_vtab_name: &str, _table_name: &str) -> ShadowTablePolicy
    where
        Self: Sized,
    {
        ShadowTablePolicy::ordinary()
    }

    /// Called for `CREATE VIRTUAL TABLE`.
    ///
    /// `args` follows SQLite's canonical module ABI: module name, database
    /// name, virtual-table name, then the arguments from the `USING` clause.
    /// Implementations must not also accept a module-only compatibility shape.
    ///
    /// May create backing storage. Default delegates to `connect`
    /// (suitable for eponymous virtual tables).
    fn create(cx: &Cx, args: &[&str]) -> Result<Self>
    where
        Self: Sized,
    {
        Self::connect(cx, args)
    }

    /// Called for subsequent opens of an existing virtual table. `args` uses
    /// the same canonical SQLite module ABI as [`create`](Self::create).
    fn connect(cx: &Cx, args: &[&str]) -> Result<Self>
    where
        Self: Sized;

    /// Inform the query planner about available indexes and their costs.
    /// Implementations may mutate output fields, but `constraints` and
    /// `order_by` are immutable inputs and the length of `constraint_usage`
    /// must be preserved. Positive argument slots must be unique and contiguous
    /// from 1 through the number of supplied filter arguments.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;

    /// Open a new scan cursor.
    fn open(&self) -> Result<Self::Cursor>;

    /// Drop a virtual table instance (opposite of `connect`).
    fn disconnect(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Called for `DROP VIRTUAL TABLE` — destroy backing storage.
    ///
    /// Default delegates to `disconnect`.
    fn destroy(&mut self, cx: &Cx) -> Result<()> {
        self.disconnect(cx)
    }

    /// INSERT/UPDATE/DELETE on the virtual table.
    ///
    /// - `args[0]`: old rowid (`None` for INSERT)
    /// - `args[1]`: new rowid
    /// - `args[2..]`: column values
    ///
    /// Returns the new rowid for INSERT, `None` for UPDATE/DELETE.
    ///
    /// Default returns [`FrankenError::ReadOnly`] (read-only virtual tables).
    fn update(&mut self, _cx: &Cx, _args: &[SqliteValue]) -> Result<Option<i64>> {
        Err(FrankenError::ReadOnly)
    }

    /// Begin a virtual table transaction.
    fn begin(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Sync a virtual table transaction (phase 1 of 2PC).
    fn sync_txn(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Commit a virtual table transaction.
    fn commit(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Roll back a virtual table transaction.
    fn rollback(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    /// Rename the virtual table.
    ///
    /// Default returns [`FrankenError::Unsupported`].
    fn rename(&mut self, _cx: &Cx, _new_name: &str) -> Result<()> {
        Err(FrankenError::Unsupported)
    }

    /// Create a savepoint at level `n`.
    fn savepoint(&mut self, _cx: &Cx, _n: i32) -> Result<()> {
        Ok(())
    }

    /// Release savepoint at level `n`.
    fn release(&mut self, _cx: &Cx, _n: i32) -> Result<()> {
        Ok(())
    }

    /// Roll back to savepoint at level `n`.
    fn rollback_to(&mut self, _cx: &Cx, _n: i32) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// VirtualTableCursor trait
// ---------------------------------------------------------------------------

/// A cursor for scanning a virtual table.
///
/// Cursors are `Send` but **NOT** `Sync` — they are single-threaded
/// scan objects bound to a specific filter invocation.
///
/// # Lifecycle
///
/// 1. [`filter`](Self::filter) begins a scan with planner-chosen parameters.
/// 2. Iterate: check [`eof`](Self::eof), read [`column`](Self::column)/[`rowid`](Self::rowid), advance with [`next`](Self::next).
/// 3. The cursor is dropped when the scan is complete.
#[allow(clippy::missing_errors_doc)]
pub trait VirtualTableCursor: Send {
    /// Begin a scan with the filter parameters chosen by `best_index`.
    fn filter(
        &mut self,
        cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()>;

    /// Advance to the next row.
    fn next(&mut self, cx: &Cx) -> Result<()>;

    /// Whether the cursor has moved past the last row.
    fn eof(&self) -> bool;

    /// Write the value of column `col` into `ctx`.
    fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()>;

    /// Return the rowid of the current row.
    fn rowid(&self) -> Result<i64>;
}

// ---------------------------------------------------------------------------
// Module factory & type erasure
// ---------------------------------------------------------------------------

/// A type-erased virtual table module factory.
///
/// Registered with the connection via `register_module("name", factory)`.
/// When `CREATE VIRTUAL TABLE ... USING name(args)` is executed, the
/// factory's `create` method is called to produce a concrete vtab instance.
#[allow(clippy::missing_errors_doc)]
pub trait VtabModuleFactory: Send + Sync {
    /// Create a new virtual table instance for `CREATE VIRTUAL TABLE`.
    ///
    /// `args` is exactly `[module, database, table, module_args...]`, matching
    /// SQLite's xCreate/xConnect contract.
    fn create(&self, cx: &Cx, args: &[&str]) -> Result<Box<dyn ErasedVtabInstance>>;

    /// Connect to an existing virtual table (subsequent opens), using the same
    /// canonical argv shape as [`create`](Self::create).
    fn connect(&self, cx: &Cx, args: &[&str]) -> Result<Box<dyn ErasedVtabInstance>> {
        self.create(cx, args)
    }

    /// Column names and affinities for the virtual table schema. `args` uses
    /// the same canonical argv shape as [`create`](Self::create).
    fn column_info(&self, _args: &[&str]) -> Vec<(String, char)> {
        Vec::new()
    }

    /// Static metadata for the module as a whole.
    fn module_metadata(&self, _args: &[&str]) -> VtabModuleMetadata {
        VtabModuleMetadata::ordinary()
    }

    /// Determine whether `table_name` is a module-owned shadow table for the
    /// virtual table instance named `vtab_name`.
    fn shadow_table_policy(&self, _vtab_name: &str, _table_name: &str) -> ShadowTablePolicy {
        ShadowTablePolicy::ordinary()
    }
}

mod erased_instance_sealed {
    use super::VirtualTable;

    pub trait Sealed {}

    impl<T: VirtualTable + 'static> Sealed for T where T::Cursor: 'static {}
}

/// A type-erased virtual table instance.
///
/// This implementation detail is sealed so extension authors implement the
/// safe [`VirtualTable`] surface instead. In particular, this guarantees the
/// `as_any` accessors remain mechanical downcasts rather than user callbacks.
#[allow(clippy::missing_errors_doc)]
#[allow(private_bounds)]
pub trait ErasedVtabInstance: Send + Sync + erased_instance_sealed::Sealed {
    /// Return this instance as `Any` for downcasting to concrete extension types.
    fn as_any(&self) -> &dyn Any;
    /// Return this instance as mutable `Any` for downcasting to concrete extension types.
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// Open a new scan cursor.
    fn open_cursor(&self) -> Result<Box<dyn ErasedVtabCursor>>;
    /// INSERT/UPDATE/DELETE on the virtual table.
    fn update(&mut self, cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>>;
    /// Begin a virtual table transaction.
    fn begin(&mut self, cx: &Cx) -> Result<()>;
    /// Sync a virtual table transaction.
    fn sync_txn(&mut self, cx: &Cx) -> Result<()>;
    /// Commit a virtual table transaction.
    fn commit(&mut self, cx: &Cx) -> Result<()>;
    /// Roll back a virtual table transaction.
    fn rollback(&mut self, cx: &Cx) -> Result<()>;
    /// Create a savepoint at level `n`.
    fn savepoint(&mut self, cx: &Cx, n: i32) -> Result<()>;
    /// Release savepoint at level `n`.
    fn release(&mut self, cx: &Cx, n: i32) -> Result<()>;
    /// Roll back to savepoint at level `n`.
    fn rollback_to(&mut self, cx: &Cx, n: i32) -> Result<()>;
    /// Disconnect this live instance without destroying its backing storage.
    fn disconnect(&mut self, cx: &Cx) -> Result<()>;
    /// Destroy the virtual table.
    fn destroy(&mut self, cx: &Cx) -> Result<()>;
    /// Rename the virtual table.
    fn rename(&mut self, cx: &Cx, new_name: &str) -> Result<()>;
    /// Inform the query planner about available indexes.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;
}

/// A type-erased virtual table cursor.
#[allow(clippy::missing_errors_doc)]
pub trait ErasedVtabCursor: Send {
    /// Begin a scan with filter parameters.
    fn erased_filter(
        &mut self,
        cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()>;
    /// Advance to the next row.
    fn erased_next(&mut self, cx: &Cx) -> Result<()>;
    /// Whether the cursor has moved past the last row.
    fn erased_eof(&self) -> bool;
    /// Write the value of column `col` into `ctx`.
    fn erased_column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()>;
    /// Return the rowid of the current row.
    fn erased_rowid(&self) -> Result<i64>;
}

/// Blanket `ErasedVtabCursor` for any concrete cursor.
impl<C: VirtualTableCursor + 'static> ErasedVtabCursor for C {
    fn erased_filter(
        &mut self,
        cx: &Cx,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[SqliteValue],
    ) -> Result<()> {
        VirtualTableCursor::filter(self, cx, idx_num, idx_str, args)
    }
    fn erased_next(&mut self, cx: &Cx) -> Result<()> {
        VirtualTableCursor::next(self, cx)
    }
    fn erased_eof(&self) -> bool {
        VirtualTableCursor::eof(self)
    }
    fn erased_column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
        VirtualTableCursor::column(self, ctx, col)
    }
    fn erased_rowid(&self) -> Result<i64> {
        VirtualTableCursor::rowid(self)
    }
}

/// Blanket `ErasedVtabInstance` for any concrete `VirtualTable`.
impl<T: VirtualTable + 'static> ErasedVtabInstance for T
where
    T::Cursor: 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn open_cursor(&self) -> Result<Box<dyn ErasedVtabCursor>> {
        let cursor = VirtualTable::open(self)?;
        Ok(Box::new(cursor))
    }
    fn update(&mut self, cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        VirtualTable::update(self, cx, args)
    }
    fn begin(&mut self, cx: &Cx) -> Result<()> {
        VirtualTable::begin(self, cx)
    }
    fn sync_txn(&mut self, cx: &Cx) -> Result<()> {
        VirtualTable::sync_txn(self, cx)
    }
    fn commit(&mut self, cx: &Cx) -> Result<()> {
        VirtualTable::commit(self, cx)
    }
    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        VirtualTable::rollback(self, cx)
    }
    fn savepoint(&mut self, cx: &Cx, n: i32) -> Result<()> {
        VirtualTable::savepoint(self, cx, n)
    }
    fn release(&mut self, cx: &Cx, n: i32) -> Result<()> {
        VirtualTable::release(self, cx, n)
    }
    fn rollback_to(&mut self, cx: &Cx, n: i32) -> Result<()> {
        VirtualTable::rollback_to(self, cx, n)
    }
    fn disconnect(&mut self, cx: &Cx) -> Result<()> {
        VirtualTable::disconnect(self, cx)
    }
    fn destroy(&mut self, cx: &Cx) -> Result<()> {
        VirtualTable::destroy(self, cx)
    }
    fn rename(&mut self, cx: &Cx, new_name: &str) -> Result<()> {
        VirtualTable::rename(self, cx, new_name)
    }
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        VirtualTable::best_index(self, info)
    }
}

/// Create a `VtabModuleFactory` from a `VirtualTable` type.
pub fn module_factory_from<T>() -> impl VtabModuleFactory
where
    T: VirtualTable + 'static,
    T::Cursor: 'static,
{
    struct Factory<T: Send + Sync>(std::marker::PhantomData<T>);

    impl<T: VirtualTable + 'static> VtabModuleFactory for Factory<T>
    where
        T::Cursor: 'static,
    {
        fn create(&self, cx: &Cx, args: &[&str]) -> Result<Box<dyn ErasedVtabInstance>> {
            let vtab = T::create(cx, args)?;
            Ok(Box::new(vtab))
        }
        fn connect(&self, cx: &Cx, args: &[&str]) -> Result<Box<dyn ErasedVtabInstance>> {
            let vtab = T::connect(cx, args)?;
            Ok(Box::new(vtab))
        }

        fn module_metadata(&self, args: &[&str]) -> VtabModuleMetadata {
            T::module_metadata(args)
        }

        fn shadow_table_policy(&self, vtab_name: &str, table_name: &str) -> ShadowTablePolicy {
            T::shadow_table_policy(vtab_name, table_name)
        }
    }

    Factory::<T>(std::marker::PhantomData)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;

    // -- Mock: generate_series(start, stop) virtual table --

    struct GenerateSeries {
        destroyed: bool,
    }

    struct GenerateSeriesCursor {
        start: i64,
        stop: i64,
        current: i64,
    }

    impl VirtualTable for GenerateSeries {
        type Cursor = GenerateSeriesCursor;

        fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
            Ok(Self { destroyed: false })
        }

        fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
            info.estimated_cost = 10.0;
            info.estimated_rows = 100;
            info.idx_num = 1;

            // Mark constraint 0 as consumed, mapped to filter arg 1.
            if !info.constraints.is_empty() && info.constraints[0].usable {
                info.constraint_usage[0].argv_index = 1;
                info.constraint_usage[0].omit = true;
            }
            Ok(())
        }

        fn open(&self) -> Result<GenerateSeriesCursor> {
            Ok(GenerateSeriesCursor {
                start: 0,
                stop: 0,
                current: 0,
            })
        }

        fn destroy(&mut self, _cx: &Cx) -> Result<()> {
            self.destroyed = true;
            Ok(())
        }
    }

    impl VirtualTableCursor for GenerateSeriesCursor {
        fn filter(
            &mut self,
            _cx: &Cx,
            _idx_num: i32,
            _idx_str: Option<&str>,
            args: &[SqliteValue],
        ) -> Result<()> {
            self.start = args.first().map_or(1, SqliteValue::to_integer);
            self.stop = args.get(1).map_or(10, SqliteValue::to_integer);
            self.current = self.start;
            Ok(())
        }

        fn next(&mut self, _cx: &Cx) -> Result<()> {
            self.current += 1;
            Ok(())
        }

        fn eof(&self) -> bool {
            self.current > self.stop
        }

        fn column(&self, ctx: &mut ColumnContext, _col: i32) -> Result<()> {
            if self.eof() {
                ctx.set_value(SqliteValue::Null);
                return Ok(());
            }
            ctx.set_value(SqliteValue::Integer(self.current));
            Ok(())
        }

        fn rowid(&self) -> Result<i64> {
            Ok(if self.eof() { 0 } else { self.current })
        }
    }

    // -- Mock: read-only vtab for default update test --

    struct ReadOnlyVtab;

    struct ReadOnlyCursor;

    impl VirtualTable for ReadOnlyVtab {
        type Cursor = ReadOnlyCursor;

        fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
            Ok(Self)
        }

        fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
            Ok(())
        }

        fn open(&self) -> Result<ReadOnlyCursor> {
            Ok(ReadOnlyCursor)
        }
    }

    impl VirtualTableCursor for ReadOnlyCursor {
        fn filter(
            &mut self,
            _cx: &Cx,
            _idx_num: i32,
            _idx_str: Option<&str>,
            _args: &[SqliteValue],
        ) -> Result<()> {
            Ok(())
        }

        fn next(&mut self, _cx: &Cx) -> Result<()> {
            Ok(())
        }

        fn eof(&self) -> bool {
            true
        }

        fn column(&self, ctx: &mut ColumnContext, _col: i32) -> Result<()> {
            ctx.set_value(SqliteValue::Null);
            Ok(())
        }

        fn rowid(&self) -> Result<i64> {
            Ok(0)
        }
    }

    // -- Mock: writable vtab for insert test --

    struct WritableVtab {
        rows: Vec<(i64, Vec<SqliteValue>)>,
        next_rowid: i64,
    }

    struct WritableCursor {
        rows: Vec<(i64, Vec<SqliteValue>)>,
        pos: usize,
    }

    impl VirtualTable for WritableVtab {
        type Cursor = WritableCursor;

        fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
            Ok(Self {
                rows: Vec::new(),
                next_rowid: 1,
            })
        }

        fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
            Ok(())
        }

        fn open(&self) -> Result<WritableCursor> {
            Ok(WritableCursor {
                rows: self.rows.clone(),
                pos: 0,
            })
        }

        fn update(&mut self, _cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
            // args[0] = old rowid (Null for INSERT)
            if args[0].is_null() {
                // INSERT
                let rowid = self.next_rowid;
                self.next_rowid += 1;
                let cols: Vec<SqliteValue> = args[2..].to_vec();
                self.rows.push((rowid, cols));
                return Ok(Some(rowid));
            }
            Ok(None)
        }
    }

    impl VirtualTableCursor for WritableCursor {
        fn filter(
            &mut self,
            _cx: &Cx,
            _idx_num: i32,
            _idx_str: Option<&str>,
            _args: &[SqliteValue],
        ) -> Result<()> {
            self.pos = 0;
            Ok(())
        }

        fn next(&mut self, _cx: &Cx) -> Result<()> {
            self.pos += 1;
            Ok(())
        }

        fn eof(&self) -> bool {
            self.pos >= self.rows.len()
        }

        fn column(&self, ctx: &mut ColumnContext, col: i32) -> Result<()> {
            if self.eof() {
                ctx.set_value(SqliteValue::Null);
                return Ok(());
            }

            #[allow(clippy::cast_sign_loss)]
            let col_idx = col as usize;
            if let Some((_, cols)) = self.rows.get(self.pos) {
                if let Some(val) = cols.get(col_idx) {
                    ctx.set_value(val.clone());
                    return Ok(());
                }
            }
            ctx.set_value(SqliteValue::Null);
            Ok(())
        }

        fn rowid(&self) -> Result<i64> {
            self.rows
                .get(self.pos)
                .map_or(Ok(0), |(rowid, _)| Ok(*rowid))
        }
    }

    struct ShadowOwningVtab;

    impl VirtualTable for ShadowOwningVtab {
        type Cursor = ReadOnlyCursor;

        fn module_metadata(_args: &[&str]) -> VtabModuleMetadata {
            VtabModuleMetadata::shadow_owning(
                VtabLifecyclePolicy::SeparateCreateAndConnect,
                VtabIntegrityPolicy::ShadowAware,
                VtabRiskLevel {
                    innocuous: false,
                    direct_only: true,
                    uses_all_schemas: false,
                },
            )
        }

        fn shadow_table_policy(vtab_name: &str, table_name: &str) -> ShadowTablePolicy {
            let Some((owner, suffix)) = table_name.rsplit_once('_') else {
                return ShadowTablePolicy::ordinary();
            };

            if owner == vtab_name
                && matches!(suffix, "config" | "content" | "data" | "docsize" | "idx")
            {
                return ShadowTablePolicy::owned_shadow();
            }

            ShadowTablePolicy::ordinary()
        }

        fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
            Ok(Self)
        }

        fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
            Ok(())
        }

        fn open(&self) -> Result<Self::Cursor> {
            Ok(ReadOnlyCursor)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct HookSnapshot {
        version: i32,
    }

    struct HookAwareVtab {
        version: i32,
        syncs: usize,
        tx_state: TransactionalVtabState<HookSnapshot>,
    }

    impl VirtualTable for HookAwareVtab {
        type Cursor = ReadOnlyCursor;

        fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
            Ok(Self {
                version: 7,
                syncs: 0,
                tx_state: TransactionalVtabState::default(),
            })
        }

        fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
            Ok(())
        }

        fn open(&self) -> Result<Self::Cursor> {
            Ok(ReadOnlyCursor)
        }

        fn begin(&mut self, _cx: &Cx) -> Result<()> {
            self.tx_state.begin(HookSnapshot {
                version: self.version,
            });
            Ok(())
        }

        fn sync_txn(&mut self, _cx: &Cx) -> Result<()> {
            self.syncs += 1;
            Ok(())
        }

        fn savepoint(&mut self, _cx: &Cx, n: i32) -> Result<()> {
            self.tx_state.savepoint(
                n,
                HookSnapshot {
                    version: self.version,
                },
            );
            Ok(())
        }

        fn release(&mut self, _cx: &Cx, n: i32) -> Result<()> {
            self.tx_state.release(n);
            Ok(())
        }

        fn rollback_to(&mut self, _cx: &Cx, n: i32) -> Result<()> {
            if let Some(snapshot) = self.tx_state.rollback_to(n) {
                self.version = snapshot.version;
            }
            Ok(())
        }

        fn commit(&mut self, _cx: &Cx) -> Result<()> {
            self.tx_state.commit();
            Ok(())
        }

        fn rollback(&mut self, _cx: &Cx) -> Result<()> {
            if let Some(snapshot) = self.tx_state.rollback() {
                self.version = snapshot.version;
            }
            Ok(())
        }
    }

    // -- Tests --

    #[test]
    fn test_vtab_create_vs_connect() {
        let cx = Cx::new();

        // create delegates to connect by default.
        let vtab = GenerateSeries::create(&cx, &[]).unwrap();
        assert!(!vtab.destroyed);

        // connect works directly.
        let vtab2 = GenerateSeries::connect(&cx, &[]).unwrap();
        assert!(!vtab2.destroyed);
    }

    #[test]
    fn test_vtab_best_index_populates_info() {
        let cx = Cx::new();
        let vtab = GenerateSeries::connect(&cx, &[]).unwrap();

        let mut info = IndexInfo::new(
            vec![IndexConstraint {
                column: 0,
                op: ConstraintOp::Gt,
                usable: true,
            }],
            vec![],
        );

        VirtualTable::best_index(&vtab, &mut info).unwrap();

        assert_eq!(info.idx_num, 1);
        assert!((info.estimated_cost - 10.0).abs() < f64::EPSILON);
        assert_eq!(info.estimated_rows, 100);
        assert_eq!(info.constraint_usage[0].argv_index, 1);
        assert!(info.constraint_usage[0].omit);
    }

    #[test]
    fn test_vtab_cursor_filter_next_eof() {
        let cx = Cx::new();
        let vtab = GenerateSeries::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();

        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Integer(1), SqliteValue::Integer(3)],
            )
            .unwrap();

        let mut values = Vec::new();
        while !cursor.eof() {
            let mut ctx = ColumnContext::new();
            cursor.column(&mut ctx, 0).unwrap();
            let rowid = cursor.rowid().unwrap();
            values.push((rowid, ctx.take_value().unwrap()));
            cursor.next(&cx).unwrap();
        }

        assert_eq!(values.len(), 3);
        assert_eq!(values[0], (1, SqliteValue::Integer(1)));
        assert_eq!(values[1], (2, SqliteValue::Integer(2)));
        assert_eq!(values[2], (3, SqliteValue::Integer(3)));
    }

    #[test]
    fn test_generate_series_cursor_past_end_returns_null_and_zero_rowid() {
        let cx = Cx::new();
        let vtab = GenerateSeries::connect(&cx, &[]).unwrap();
        let mut cursor = vtab.open().unwrap();

        cursor
            .filter(
                &cx,
                0,
                None,
                &[SqliteValue::Integer(1), SqliteValue::Integer(1)],
            )
            .unwrap();
        cursor.next(&cx).unwrap();
        assert!(cursor.eof());

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn test_writable_cursor_missing_column_returns_null() {
        let cx = Cx::new();
        let mut vtab = WritableVtab::connect(&cx, &[]).unwrap();
        VirtualTable::update(
            &mut vtab,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Text("hello".into()),
            ],
        )
        .unwrap();

        let mut cursor = vtab.open().unwrap();
        cursor.filter(&cx, 0, None, &[]).unwrap();

        let mut ctx = ColumnContext::new();
        cursor.column(&mut ctx, 3).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));

        cursor.next(&cx).unwrap();
        assert!(cursor.eof());
        cursor.column(&mut ctx, 0).unwrap();
        assert_eq!(ctx.take_value(), Some(SqliteValue::Null));
        assert_eq!(cursor.rowid().unwrap(), 0);
    }

    #[test]
    fn test_vtab_update_insert() {
        let cx = Cx::new();
        let mut vtab = WritableVtab::connect(&cx, &[]).unwrap();

        // INSERT: args[0] = Null (no old rowid), args[1] = new rowid (ignored),
        // args[2..] = column values
        let result = VirtualTable::update(
            &mut vtab,
            &cx,
            &[
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Text("hello".into()),
            ],
        )
        .unwrap();

        assert_eq!(result, Some(1));
        assert_eq!(vtab.rows.len(), 1);
        assert_eq!(vtab.rows[0].0, 1);
    }

    #[test]
    fn test_vtab_update_readonly_default() {
        let cx = Cx::new();
        let mut vtab = ReadOnlyVtab::connect(&cx, &[]).unwrap();
        let err = VirtualTable::update(&mut vtab, &cx, &[SqliteValue::Null]).unwrap_err();
        assert!(matches!(err, FrankenError::ReadOnly));
    }

    #[test]
    fn test_vtab_destroy_vs_disconnect() {
        let cx = Cx::new();

        // Default: destroy delegates to disconnect (both no-ops for ReadOnlyVtab).
        let mut vtab = ReadOnlyVtab::connect(&cx, &[]).unwrap();
        VirtualTable::disconnect(&mut vtab, &cx).unwrap();
        VirtualTable::destroy(&mut vtab, &cx).unwrap();

        // Custom destroy sets a flag.
        let mut vtab = GenerateSeries::connect(&cx, &[]).unwrap();
        assert!(!vtab.destroyed);
        VirtualTable::destroy(&mut vtab, &cx).unwrap();
        assert!(vtab.destroyed);
    }

    #[test]
    fn test_vtab_cursor_send_but_not_sync() {
        fn assert_send<T: Send>() {}
        assert_send::<GenerateSeriesCursor>();

        // VirtualTableCursor is Send but NOT Sync.
        // We can't directly test "not Sync" at runtime, but we can verify
        // the trait bound: VirtualTableCursor: Send (not Send + Sync).
        // The type GenerateSeriesCursor IS actually Sync by coincidence
        // (all fields are i64), but the trait doesn't require it.
        // The key point: the trait signature says Send, not Send + Sync.
    }

    #[test]
    fn test_column_context_lifecycle() {
        let mut ctx = ColumnContext::new();
        assert!(ctx.take_value().is_none());

        ctx.set_value(SqliteValue::Integer(42));
        assert_eq!(ctx.take_value(), Some(SqliteValue::Integer(42)));

        // After take, it's None again.
        assert!(ctx.take_value().is_none());
    }

    #[test]
    fn test_index_info_new() {
        let info = IndexInfo::new(
            vec![
                IndexConstraint {
                    column: 0,
                    op: ConstraintOp::Eq,
                    usable: true,
                },
                IndexConstraint {
                    column: 1,
                    op: ConstraintOp::Gt,
                    usable: false,
                },
            ],
            vec![IndexOrderBy {
                column: 0,
                desc: false,
            }],
        );

        assert_eq!(info.constraints.len(), 2);
        assert_eq!(info.order_by.len(), 1);
        assert_eq!(info.constraint_usage.len(), 2);
        assert_eq!(info.idx_num, 0);
        assert!(info.idx_str.is_none());
        assert!(!info.order_by_consumed);
    }

    #[test]
    fn test_transactional_vtab_state_tracks_savepoints() {
        let mut state = TransactionalVtabState::default();

        state.begin(1_i32);
        state.savepoint(0, 2);
        state.savepoint(1, 3);
        assert_eq!(state.rollback_to(1), Some(3));
        state.release(1);
        assert_eq!(state.rollback(), Some(1));
        assert_eq!(state.rollback(), None);
    }

    #[test]
    fn test_transactional_vtab_state_uses_base_for_late_enlistment() {
        let mut state = TransactionalVtabState::default();

        state.begin(7_i32);
        state.savepoint(2, 9);

        assert_eq!(state.rollback_to(1), Some(7));
        assert_eq!(state.rollback(), Some(7));
    }

    #[test]
    fn test_shadow_table_policy_defaults_to_ordinary() {
        let policy = ReadOnlyVtab::shadow_table_policy("docs", "docs_data");
        assert_eq!(policy, ShadowTablePolicy::ordinary());
        assert!(!policy.is_shadow());
        assert!(policy.allows_direct_dml());
        assert!(policy.allows_schema_ddl());
        assert!(policy.allows_module_internal_write());
    }

    #[test]
    fn test_owned_shadow_policy_blocks_user_dml_and_schema_ddl() {
        let policy = ShadowTablePolicy::owned_shadow();

        assert!(policy.is_shadow());
        assert!(!policy.allows_direct_dml());
        assert!(!policy.allows_schema_ddl());
        assert!(policy.allows_module_internal_write());
    }

    #[test]
    fn test_shadow_owning_module_metadata_is_forwarded_by_factory() {
        let factory = module_factory_from::<ShadowOwningVtab>();
        let metadata = factory.module_metadata(&[]);

        assert!(metadata.owns_shadow_tables);
        assert_eq!(
            metadata.lifecycle,
            VtabLifecyclePolicy::SeparateCreateAndConnect
        );
        assert_eq!(metadata.integrity, VtabIntegrityPolicy::ShadowAware);
        assert!(metadata.risk.direct_only);
        assert!(!metadata.risk.innocuous);
    }

    #[test]
    fn test_shadow_owning_module_matches_owned_shadow_tables() {
        let factory = module_factory_from::<ShadowOwningVtab>();

        let owned = factory.shadow_table_policy("docs", "docs_data");
        let other_owner = factory.shadow_table_policy("docs", "posts_data");
        let unrelated = factory.shadow_table_policy("docs", "docs_segments");

        assert_eq!(owned.kind, ShadowTableKind::Shadow);
        assert!(!owned.allows_direct_dml());
        assert!(!owned.allows_schema_ddl());
        assert!(owned.allows_module_internal_write());
        assert!(!other_owner.is_shadow());
        assert!(!unrelated.is_shadow());
        assert!(unrelated.allows_direct_dml());
    }

    #[test]
    fn test_erased_vtab_instance_forwards_transaction_hooks() {
        let cx = Cx::new();
        let mut erased: Box<dyn ErasedVtabInstance> =
            Box::new(HookAwareVtab::connect(&cx, &[]).unwrap());

        erased.begin(&cx).unwrap();
        {
            let hook = erased
                .as_any_mut()
                .downcast_mut::<HookAwareVtab>()
                .expect("hook-aware vtab");
            hook.version = 9;
        }
        erased.savepoint(&cx, 0).unwrap();
        {
            let hook = erased
                .as_any_mut()
                .downcast_mut::<HookAwareVtab>()
                .expect("hook-aware vtab");
            hook.version = 11;
        }
        erased.rollback_to(&cx, 0).unwrap();
        erased.release(&cx, 0).unwrap();
        erased.sync_txn(&cx).unwrap();
        erased.rollback(&cx).unwrap();

        let hook = erased
            .as_any_mut()
            .downcast_mut::<HookAwareVtab>()
            .expect("hook-aware vtab");
        assert_eq!(hook.version, 7);
        assert_eq!(hook.syncs, 1);
    }
}
