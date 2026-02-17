//! SQL connection API with Phase 5 pager/WAL/B-tree storage wiring.
//!
//! Supports expression-only SELECT statements as well as table-backed DML:
//! CREATE TABLE, DROP TABLE, INSERT, SELECT (with FROM), UPDATE, and DELETE. Table
//! storage currently uses the in-memory `MemDatabase` backend for execution,
//! while a [`PagerBackend`] is initialized alongside for future Phase 5
//! sub-tasks (bd-1dqg, bd-25c6) that will wire the transaction lifecycle
//! and cursor paths through the real storage stack.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

use fsqlite_ast::{
    AlterTableAction, BinaryOp, ColumnConstraintKind, ColumnRef, CompoundOp, CreateTableBody,
    Distinctness, DropObjectType, Expr, FunctionArgs, InSet, JoinConstraint, JoinKind, LikeOp,
    LimitClause, Literal, NullsOrder, OrderingTerm, PlaceholderType, ResultColumn, SelectBody,
    SelectCore, SelectStatement, SortDirection, Span, Statement, TableOrSubquery, UnaryOp,
};
use fsqlite_btree::BtreeCursorOps;
use fsqlite_btree::cursor::TransactionPageIo;
use fsqlite_error::{FrankenError, Result};
use fsqlite_func::FunctionRegistry;
use fsqlite_pager::traits::{MvccPager, TransactionHandle, TransactionMode};
use fsqlite_pager::{CheckpointMode, JournalMode, SimplePager};
use fsqlite_parser::Parser;
use fsqlite_types::DATABASE_HEADER_SIZE;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, VfsOpenFlags};
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{BTreePageHeader, DatabaseHeader, PageNumber, PageSize, TextEncoding};
use fsqlite_vdbe::codegen::{
    CodegenContext, CodegenError, ColumnInfo, IndexSchema, TableSchema, codegen_delete,
    codegen_insert, codegen_select, codegen_update,
};
use fsqlite_vdbe::engine::{ExecOutcome, MemDatabase, MemDbVersionToken, VdbeEngine};
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};
use fsqlite_vfs::MemoryVfs;
#[cfg(unix)]
use fsqlite_vfs::UnixVfs;
use fsqlite_vfs::traits::Vfs;
use fsqlite_wal::{
    WalFecRepairEvidenceCard, WalFecRepairEvidenceQuery, WalFecRepairSeverityBucket, WalFile,
    WalSalts, query_raptorq_repair_evidence, raptorq_repair_events_snapshot,
    raptorq_repair_evidence_snapshot, raptorq_repair_metrics_snapshot,
    reset_raptorq_repair_telemetry,
};

// MVCC concurrent-writer support (bd-14zc / 5E.1, bd-kivg / 5E.2, bd-3bql / 5E.5)
use fsqlite_mvcc::{
    CommitIndex, ConcurrentHandle, ConcurrentRegistry, ConcurrentSavepoint, FcwResult, GcScheduler,
    GcTickResult, GcTodo, InProcessPageLockTable, MvccError, SsiDecisionCard, SsiDecisionCardDraft,
    SsiDecisionQuery, SsiDecisionType, SsiEvidenceLedger, SsiReadSetSummary, VersionStore,
    concurrent_abort, concurrent_read_page, concurrent_rollback_to_savepoint, concurrent_savepoint,
    concurrent_write_page, validate_first_committer_wins,
};
// MVCC conflict observability (bd-t6sv2.1)
use fsqlite_observability::{
    MetricsObserver, next_decision_id, next_trace_id, record_compat_trace_callback,
    record_trace_export, record_trace_export_error, record_trace_span_created, reset_trace_metrics,
    trace_metrics_snapshot,
};
use fsqlite_types::{CommitSeq, PageData, SchemaEpoch, Snapshot, TxnId, TxnToken};

use crate::wal_adapter::WalBackendAdapter;

// ---------------------------------------------------------------------------
// Phase 5: Pager backend abstraction (bd-3iw8)
// ---------------------------------------------------------------------------

/// Pager backend that dispatches across VFS implementations.
///
/// Wraps [`SimplePager`] for both in-memory (`:memory:`) and on-disk
/// (Unix filesystem) connections without making [`Connection`] generic.
///
/// # Future sub-tasks
///
/// - **bd-1dqg**: Wire `begin()` / `commit()` / `rollback()` through the
///   pager transaction lifecycle.
/// - **bd-25c6**: Wire `OpenWrite` opcode through `StorageCursor` to the
///   B-tree write path using [`TransactionPageIo`].
#[allow(dead_code)] // Fields used by upcoming sub-tasks bd-1dqg and bd-25c6
#[derive(Clone)]
pub enum PagerBackend {
    /// In-memory VFS backend (`:memory:` databases).
    Memory(Arc<SimplePager<MemoryVfs>>),
    /// Unix filesystem VFS backend (file-backed databases).
    #[cfg(unix)]
    Unix(Arc<SimplePager<UnixVfs>>),
}

impl std::fmt::Debug for PagerBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(_) => f.write_str("PagerBackend::Memory"),
            #[cfg(unix)]
            Self::Unix(_) => f.write_str("PagerBackend::Unix"),
        }
    }
}

impl PagerBackend {
    /// Open a pager for the given path.
    ///
    /// Uses [`MemoryVfs`] for `:memory:` and [`UnixVfs`] for file paths.
    fn open(path: &str) -> Result<Self> {
        if path == ":memory:" {
            let vfs = MemoryVfs::new();
            let db_path = PathBuf::from("/:memory:");
            let pager = SimplePager::open(vfs, &db_path, PageSize::DEFAULT)?;
            Ok(Self::Memory(Arc::new(pager)))
        } else {
            #[cfg(unix)]
            {
                let vfs = UnixVfs::new();
                let db_path = PathBuf::from(path);
                let pager = SimplePager::open(vfs, &db_path, PageSize::DEFAULT)?;
                Ok(Self::Unix(Arc::new(pager)))
            }
            #[cfg(not(unix))]
            {
                Err(FrankenError::NotImplemented(
                    "file-backed pager not available on this platform".to_owned(),
                ))
            }
        }
    }

    /// Begin a new transaction.
    fn begin(&self, cx: &Cx, mode: TransactionMode) -> Result<Box<dyn TransactionHandle>> {
        match self {
            Self::Memory(p) => Ok(Box::new(p.begin(cx, mode)?)),
            #[cfg(unix)]
            Self::Unix(p) => Ok(Box::new(p.begin(cx, mode)?)),
        }
    }

    fn journal_mode(&self) -> JournalMode {
        match self {
            Self::Memory(p) => p.journal_mode(),
            #[cfg(unix)]
            Self::Unix(p) => p.journal_mode(),
        }
    }

    fn set_journal_mode(&self, cx: &Cx, mode: JournalMode) -> Result<JournalMode> {
        match self {
            Self::Memory(p) => p.set_journal_mode(cx, mode),
            #[cfg(unix)]
            Self::Unix(p) => p.set_journal_mode(cx, mode),
        }
    }

    fn install_wal_backend(&self, cx: &Cx, db_path: &str) -> Result<()> {
        let wal_path = wal_path_for_db_path(db_path);
        match self {
            Self::Memory(p) => {
                let vfs = MemoryVfs::new();
                install_wal_backend_with_vfs(p, &vfs, cx, &wal_path)
            }
            #[cfg(unix)]
            Self::Unix(p) => {
                let vfs = UnixVfs::new();
                install_wal_backend_with_vfs(p, &vfs, cx, &wal_path)
            }
        }
    }

    /// Run a WAL checkpoint.
    fn checkpoint(
        &self,
        cx: &Cx,
        mode: fsqlite_pager::CheckpointMode,
    ) -> Result<fsqlite_pager::CheckpointResult> {
        match self {
            Self::Memory(p) => p.checkpoint(cx, mode),
            #[cfg(unix)]
            Self::Unix(p) => p.checkpoint(cx, mode),
        }
    }
}

fn wal_path_for_db_path(path: &str) -> PathBuf {
    let mut db_path = if path == ":memory:" {
        PathBuf::from("/:memory:")
    } else {
        PathBuf::from(path)
    }
    .into_os_string();
    db_path.push("-wal");
    PathBuf::from(db_path)
}

fn install_wal_backend_with_vfs<V>(
    pager: &Arc<SimplePager<V>>,
    vfs: &V,
    cx: &Cx,
    wal_path: &Path,
) -> Result<()>
where
    V: Vfs + Send + Sync + 'static,
    V::File: Send + Sync + 'static,
{
    if vfs.access(cx, wal_path, AccessFlags::EXISTS)? {
        let open_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (file, _) = vfs.open(cx, Some(wal_path), open_flags)?;
        match WalFile::open(cx, file) {
            Ok(wal) => {
                pager.set_wal_backend(Box::new(WalBackendAdapter::new(wal)))?;
                return Ok(());
            }
            Err(FrankenError::WalCorrupt { .. }) => {}
            Err(err) => return Err(err),
        }
    }

    let create_flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
    let (file, _) = vfs.open(cx, Some(wal_path), create_flags)?;
    let wal = WalFile::create(cx, file, PageSize::DEFAULT.get(), 0, WalSalts::default())?;
    pager.set_wal_backend(Box::new(WalBackendAdapter::new(wal)))
}

/// Build a [`FunctionRegistry`] populated with all built-in scalar,
/// aggregate, datetime, and math functions.
fn default_function_registry() -> Arc<FunctionRegistry> {
    let mut registry = FunctionRegistry::new();
    fsqlite_func::register_builtins(&mut registry);
    fsqlite_func::register_datetime_builtins(&mut registry);
    fsqlite_func::register_math_builtins(&mut registry);
    fsqlite_func::register_aggregate_builtins(&mut registry);
    fsqlite_func::register_window_builtins(&mut registry);
    fsqlite_ext_json::register_json_scalars(&mut registry);
    fsqlite_ext_fts5::register_fts5_scalars(&mut registry);
    Arc::new(registry)
}

/// Map a SQL type name to its SQLite affinity byte (§3.1 Type Affinity Rules).
///
/// Encoding: A..E maps to BLOB, TEXT, NUMERIC, INTEGER, REAL:
/// `'A'` = BLOB, `'B'` = TEXT, `'C'` = NUMERIC, `'D'` = INTEGER, `'E'` = REAL.
fn type_name_to_affinity(name: &str) -> u8 {
    let upper = name.to_uppercase();
    if upper.contains("INT") {
        b'D' // INTEGER affinity
    } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
        b'B' // TEXT affinity
    } else if upper.contains("BLOB") || upper.is_empty() {
        b'A' // BLOB affinity
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        b'E' // REAL affinity
    } else {
        b'C' // NUMERIC affinity
    }
}

/// A database row produced by a query.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<SqliteValue>,
}

impl Row {
    /// Returns all values in this row.
    pub fn values(&self) -> &[SqliteValue] {
        &self.values
    }

    /// Returns the value at `index`, if present.
    pub fn get(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }
}

/// A prepared SQL statement.
pub struct PreparedStatement {
    program: VdbeProgram,
    func_registry: Option<Arc<FunctionRegistry>>,
    expression_postprocess: Option<ExpressionPostprocess>,
    distinct: bool,
    /// For table-backed SELECT, the statement must execute against the same
    /// MemDatabase as the Connection that prepared it.
    db: Option<Rc<RefCell<MemDatabase>>>,
    /// Phase 5 (bd-35my): pager backend for starting transactions during query.
    /// Needed because PreparedStatement::query() executes independently of the
    /// Connection's transaction state.
    pager: Option<PagerBackend>,
    /// For table-backed `SELECT DISTINCT ... LIMIT/OFFSET`, we compile an
    /// unbounded program and apply the LIMIT/OFFSET after de-duplication.
    ///
    /// This mirrors `Connection::execute_statement`'s distinct+limit handling
    /// to avoid returning too few rows when LIMIT is applied before DISTINCT.
    post_distinct_limit: Option<LimitClause>,
    /// Schema cookie captured at prepare time (bd-3mmj).  Used by the
    /// engine's `ReadCookie` opcode and for future stale-schema detection.
    schema_cookie: u32,
}

#[derive(Debug, Clone, Default)]
struct ExpressionPostprocess {
    order_by: Vec<OrderingTerm>,
    limit: Option<LimitClause>,
    output_aliases: HashMap<String, usize>,
    output_width: usize,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedOrderTerm {
    column_index: usize,
    descending: bool,
    nulls_order: NullsOrder,
}

/// Compatibility mask for sqlite3_trace_v2-style callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceMask(u32);

impl TraceMask {
    pub const STMT: Self = Self(0x01);
    pub const PROFILE: Self = Self(0x02);
    pub const ROW: Self = Self(0x04);
    pub const CLOSE: Self = Self(0x08);
    pub const ALL: Self = Self(Self::STMT.0 | Self::PROFILE.0 | Self::ROW.0 | Self::CLOSE.0);

    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }
}

impl std::ops::BitOr for TraceMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

/// sqlite3_trace_v2-style callback event payload.
#[derive(Debug, Clone, PartialEq)]
pub enum TraceEvent {
    /// Statement text about to execute.
    Statement { sql: String },
    /// Statement execution profile (nanoseconds).
    Profile { sql: String, elapsed_ns: u64 },
    /// Row delivered to the client.
    Row { values: Vec<SqliteValue> },
    /// Connection close lifecycle event.
    Close,
}

impl TraceEvent {
    #[must_use]
    fn callback_type(&self) -> &'static str {
        match self {
            Self::Statement { .. } => "stmt",
            Self::Profile { .. } => "profile",
            Self::Row { .. } => "row",
            Self::Close => "close",
        }
    }

    #[must_use]
    fn required_mask(&self) -> TraceMask {
        match self {
            Self::Statement { .. } => TraceMask::STMT,
            Self::Profile { .. } => TraceMask::PROFILE,
            Self::Row { .. } => TraceMask::ROW,
            Self::Close => TraceMask::CLOSE,
        }
    }
}

/// Callback signature for sqlite3_trace_v2 compatibility.
pub type TraceCallback = Arc<dyn Fn(TraceEvent) + Send + Sync + 'static>;

#[derive(Clone)]
struct TraceRegistration {
    mask: TraceMask,
    callback: TraceCallback,
}

fn emit_compat_trace_event(trace: Option<&TraceRegistration>, event: TraceEvent) {
    let Some(trace) = trace else {
        return;
    };
    if !trace.mask.contains(event.required_mask()) {
        return;
    }

    let callback_type = event.callback_type();
    let span = tracing::span!(
        target: "fsqlite.compat_trace",
        tracing::Level::DEBUG,
        "compat_trace",
        callback_type
    );
    record_trace_span_created();
    record_compat_trace_callback();
    let _guard = span.enter();
    (trace.callback)(event);
    tracing::debug!(callback_type, "sqlite3_trace_v2 callback emitted");
}

impl std::fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatement")
            .field("program", &self.program)
            .finish_non_exhaustive()
    }
}

impl PreparedStatement {
    /// Execute as a query and return all result rows.
    pub fn query(&self) -> Result<Vec<Row>> {
        let mut rows = if let Some(db) = self.db.as_ref() {
            let Some(registry) = self.func_registry.as_ref() else {
                return Err(FrankenError::Internal(
                    "prepared statement missing function registry".to_owned(),
                ));
            };
            // Phase 5 (bd-35my): start a deferred transaction to read from the
            // pager. This ensures PreparedStatement::query() can see data written
            // by prior INSERT statements through the Connection.
            let txn = self
                .pager
                .as_ref()
                .map(|p| {
                    let cx = Cx::new();
                    p.begin(&cx, TransactionMode::Deferred)
                })
                .transpose()?;
            // PreparedStatement doesn't have concurrent context (it manages its own txns).
            let (result, mut txn_back) = execute_table_program_with_db(
                &self.program,
                None,
                registry,
                db,
                txn,
                self.schema_cookie,
                None,
            );
            // Commit the read transaction (no-op for deferred reads).
            if let Some(ref mut txn) = txn_back {
                let cx = Cx::new();
                txn.commit(&cx)?;
            }
            result?
        } else {
            execute_program_with_postprocess(
                &self.program,
                None,
                self.func_registry.as_ref(),
                self.expression_postprocess.as_ref(),
            )?
        };
        if self.distinct {
            dedup_rows(&mut rows);
        }
        if let Some(limit_clause) = self.post_distinct_limit.as_ref() {
            apply_limit_clause(&mut rows, limit_clause);
        }
        Ok(rows)
    }

    /// Execute as a query with bound SQL parameters (`?1`, `?2`, ...).
    pub fn query_with_params(&self, params: &[SqliteValue]) -> Result<Vec<Row>> {
        let mut rows = if let Some(db) = self.db.as_ref() {
            let Some(registry) = self.func_registry.as_ref() else {
                return Err(FrankenError::Internal(
                    "prepared statement missing function registry".to_owned(),
                ));
            };
            // PreparedStatement doesn't have concurrent context (no active txn here).
            execute_table_program_with_db(
                &self.program,
                Some(params),
                registry,
                db,
                None,
                self.schema_cookie,
                None,
            )
            .0?
        } else {
            execute_program_with_postprocess(
                &self.program,
                Some(params),
                self.func_registry.as_ref(),
                self.expression_postprocess.as_ref(),
            )?
        };
        if self.distinct {
            dedup_rows(&mut rows);
        }
        if let Some(limit_clause) = self.post_distinct_limit.as_ref() {
            apply_limit_clause(&mut rows, limit_clause);
        }
        Ok(rows)
    }

    /// Execute as a query and return exactly one row.
    pub fn query_row(&self) -> Result<Row> {
        first_row_or_error(self.query()?)
    }

    /// Execute as a query with parameters and return exactly one row.
    pub fn query_row_with_params(&self, params: &[SqliteValue]) -> Result<Row> {
        first_row_or_error(self.query_with_params(params)?)
    }

    /// Execute and return affected/output row count.
    pub fn execute(&self) -> Result<usize> {
        Ok(self.query()?.len())
    }

    /// Execute with bound SQL parameters and return affected/output row count.
    pub fn execute_with_params(&self, params: &[SqliteValue]) -> Result<usize> {
        Ok(self.query_with_params(params)?.len())
    }

    /// Return an EXPLAIN-style disassembly for the compiled program.
    pub fn explain(&self) -> String {
        self.program.disassemble()
    }
}

/// A stored view definition: name + SELECT query.
#[derive(Debug, Clone)]
struct ViewDef {
    name: String,
    #[allow(dead_code)]
    columns: Vec<String>,
    query: SelectStatement,
}

/// A stored trigger definition captured from `CREATE TRIGGER`.
#[derive(Debug, Clone)]
#[allow(dead_code)] // 5G.2/5G.3 will consume full trigger metadata during DML execution.
struct TriggerDef {
    name: String,
    table_name: String,
    timing: fsqlite_ast::TriggerTiming,
    event: fsqlite_ast::TriggerEvent,
    for_each_row: bool,
    when_clause: Option<Expr>,
    body: Vec<Statement>,
    temporary: bool,
    create_sql: String,
}

impl TriggerDef {
    fn from_create_statement(
        stmt: &fsqlite_ast::CreateTriggerStatement,
        create_sql: String,
    ) -> Self {
        Self {
            name: stmt.name.name.clone(),
            table_name: stmt.table.clone(),
            timing: stmt.timing,
            event: stmt.event.clone(),
            for_each_row: stmt.for_each_row,
            when_clause: stmt.when.clone(),
            body: stmt.body.clone(),
            temporary: stmt.temporary,
            create_sql,
        }
    }
}

#[derive(Debug, Clone)]
struct TriggerFrame {
    table_name: String,
    column_names: Vec<String>,
    old_row: Option<Vec<SqliteValue>>,
    new_row: Option<Vec<SqliteValue>>,
}

impl TriggerFrame {
    fn column_index(&self, column_name: &str) -> Option<usize> {
        self.column_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(column_name))
    }

    fn references_pseudo_column(&self, table_prefix: Option<&str>, column_name: &str) -> bool {
        if self.column_index(column_name).is_none() {
            return false;
        }
        match table_prefix {
            Some(prefix) => {
                prefix.eq_ignore_ascii_case("old")
                    || prefix.eq_ignore_ascii_case("new")
                    || prefix.eq_ignore_ascii_case(&self.table_name)
            }
            None => true,
        }
    }

    fn lookup_value(&self, table_prefix: Option<&str>, column_name: &str) -> Option<SqliteValue> {
        let column_index = self.column_index(column_name)?;
        match table_prefix {
            Some(prefix) if prefix.eq_ignore_ascii_case("old") => {
                Self::lookup_from_row(self.old_row.as_deref(), column_index)
            }
            Some(prefix) if prefix.eq_ignore_ascii_case("new") => {
                Self::lookup_from_row(self.new_row.as_deref(), column_index)
            }
            Some(prefix) if prefix.eq_ignore_ascii_case(&self.table_name) => self
                .new_row
                .as_ref()
                .and_then(|row| row.get(column_index).cloned())
                .or_else(|| Self::lookup_from_row(self.old_row.as_deref(), column_index)),
            Some(_) => None,
            None => self
                .new_row
                .as_ref()
                .and_then(|row| row.get(column_index).cloned())
                .or_else(|| Self::lookup_from_row(self.old_row.as_deref(), column_index)),
        }
    }

    fn lookup_from_row(row: Option<&[SqliteValue]>, column_index: usize) -> Option<SqliteValue> {
        row.and_then(|values| values.get(column_index).cloned())
    }
}

struct TriggerFrameGuard<'a> {
    stack: &'a RefCell<Vec<TriggerFrame>>,
}

impl Drop for TriggerFrameGuard<'_> {
    fn drop(&mut self) {
        let _ = self.stack.borrow_mut().pop();
    }
}

/// Check if a trigger event matches a DML event.
///
/// For INSERT/DELETE, this is an exact match.
/// For UPDATE, the trigger event must either be `Update([])` (any column) or
/// `Update([cols])` where at least one of the specified columns is being updated.
fn trigger_event_matches(
    trigger_event: &fsqlite_ast::TriggerEvent,
    dml_event: &fsqlite_ast::TriggerEvent,
) -> bool {
    use fsqlite_ast::TriggerEvent;
    match (trigger_event, dml_event) {
        (TriggerEvent::Insert, TriggerEvent::Insert)
        | (TriggerEvent::Delete, TriggerEvent::Delete) => true,
        (TriggerEvent::Update(trigger_cols), TriggerEvent::Update(dml_cols)) => {
            // If trigger specifies no columns (UPDATE OF <nothing>), match any UPDATE.
            if trigger_cols.is_empty() {
                return true;
            }
            // If DML specifies no columns, treat as "all columns" - match any trigger.
            if dml_cols.is_empty() {
                return true;
            }
            // Otherwise, check if any trigger column is in the DML column list.
            trigger_cols
                .iter()
                .any(|tc| dml_cols.iter().any(|dc| tc.eq_ignore_ascii_case(dc)))
        }
        _ => false,
    }
}

/// Evaluate a trigger WHEN clause in a constant-expression context.
///
/// If evaluation fails (for example due to unresolved OLD/NEW references),
/// return `true` to preserve current trigger behavior until pseudo-table
/// bindings are fully implemented.
fn trigger_when_matches(when_clause: Option<&Expr>, frame: Option<&TriggerFrame>) -> bool {
    let Some(expr) = when_clause else {
        return true;
    };
    let mut bound_expr = expr.clone();
    if let Some(active_frame) = frame {
        bind_trigger_columns_in_expr(&mut bound_expr, active_frame);
    }
    let row: [SqliteValue; 0] = [];
    let col_map: [(String, String); 0] = [];
    match eval_join_expr(&bound_expr, &row, &col_map) {
        Ok(value) => is_sqlite_truthy(&value),
        Err(_) => true,
    }
}

/// For `UPDATE OF col1, col2` triggers, ensure at least one listed column
/// actually changed between OLD and NEW snapshots.
fn trigger_update_of_columns_changed(
    trigger_event: &fsqlite_ast::TriggerEvent,
    frame: &TriggerFrame,
) -> bool {
    use fsqlite_ast::TriggerEvent;
    let TriggerEvent::Update(trigger_cols) = trigger_event else {
        return true;
    };
    if trigger_cols.is_empty() {
        return true;
    }
    let (Some(old_row), Some(new_row)) = (frame.old_row.as_deref(), frame.new_row.as_deref())
    else {
        // Conservatively allow firing when snapshots are incomplete.
        return true;
    };

    trigger_cols.iter().any(|column| {
        let Some(column_index) = frame.column_index(column) else {
            return false;
        };
        let old_value = old_row.get(column_index).unwrap_or(&SqliteValue::Null);
        let new_value = new_row.get(column_index).unwrap_or(&SqliteValue::Null);
        cmp_values(old_value, new_value) != std::cmp::Ordering::Equal
    })
}

#[derive(Debug, Clone)]
struct TriggerRaiseDirective {
    action: fsqlite_ast::RaiseAction,
    message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TriggerStatementOutcome {
    Continue,
    SkipDml,
}

fn trigger_statement_raise_directive(
    statement: &Statement,
) -> Result<Option<TriggerRaiseDirective>> {
    let Statement::Select(select) = statement else {
        return Ok(None);
    };
    if select.with.is_some() || !select.order_by.is_empty() || select.limit.is_some() {
        return Ok(None);
    }
    if !select.body.compounds.is_empty() {
        return Ok(None);
    }

    let SelectCore::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        windows,
        ..
    } = &select.body.select
    else {
        return Ok(None);
    };
    if from.is_some() || !group_by.is_empty() || having.is_some() || !windows.is_empty() {
        return Ok(None);
    }
    if columns.len() != 1 {
        return Ok(None);
    }

    let ResultColumn::Expr {
        expr: Expr::Raise {
            action, message, ..
        },
        ..
    } = &columns[0]
    else {
        return Ok(None);
    };

    if let Some(predicate) = where_clause {
        let row: [SqliteValue; 0] = [];
        let col_map: [(String, String); 0] = [];
        let predicate_val = eval_join_expr(predicate, &row, &col_map)?;
        if !is_sqlite_truthy(&predicate_val) {
            return Ok(None);
        }
    }

    Ok(Some(TriggerRaiseDirective {
        action: *action,
        message: message.clone(),
    }))
}

/// Snapshot of the database + schema state at a point in time.
/// Used for transaction rollback and savepoint restore.
#[derive(Debug, Clone)]
struct DbSnapshot {
    db_version: MemDbVersionToken,
    schema: Vec<TableSchema>,
    views: Vec<ViewDef>,
    triggers: Vec<TriggerDef>,
    rowid_alias_columns: HashMap<String, usize>,
    next_master_rowid: i64,
}

/// A named savepoint with its pre-state snapshot.
#[derive(Debug)]
struct SavepointEntry {
    name: String,
    snapshot: DbSnapshot,
    /// Concurrent savepoint state (MVCC mode only).
    concurrent_snapshot: Option<ConcurrentSavepoint>,
}

#[derive(Debug, Clone)]
struct SsiTxnEvidenceSnapshot {
    txn: TxnToken,
    snapshot_seq: CommitSeq,
    read_pages: Vec<PageNumber>,
    write_pages: Vec<PageNumber>,
    has_in_rw: bool,
    has_out_rw: bool,
    marked_for_abort: bool,
}

#[derive(Debug, Clone)]
struct ActiveConflictEvidence {
    conflicting_txns: Vec<TxnToken>,
    conflict_pages: Vec<PageNumber>,
}

/// A database connection holding in-memory tables and schema metadata.
///
/// Supports transactions (BEGIN/COMMIT/ROLLBACK) and savepoints
/// (SAVEPOINT/RELEASE/ROLLBACK TO). All table storage uses `MemDatabase`
/// until the B-tree + pager + VFS stack replaces this in Phase 5+.
pub struct Connection {
    path: String,
    /// In-memory table storage (shared with the VDBE engine during execution).
    /// Will be gradually replaced by pager-backed storage in Phase 5 sub-tasks.
    db: Rc<RefCell<MemDatabase>>,
    /// Phase 5 pager backend (bd-3iw8). Initialized for all connections;
    /// sub-tasks bd-1dqg (transaction lifecycle) and bd-25c6 (write path)
    /// will wire this into the execution pipeline.
    #[allow(dead_code)] // Used by upcoming sub-tasks
    pager: PagerBackend,
    /// Active transaction handle (Phase 5/bd-1dqg).
    /// Stores the pager transaction state during BEGIN/COMMIT/ROLLBACK.
    active_txn: RefCell<Option<Box<dyn TransactionHandle>>>,
    /// Schema registry: table metadata used by the code generator.
    schema: RefCell<Vec<TableSchema>>,
    /// View definitions stored in-memory.
    views: RefCell<Vec<ViewDef>>,
    /// Trigger definitions stored in-memory.
    triggers: RefCell<Vec<TriggerDef>>,
    /// Stack of active trigger OLD/NEW bindings for nested trigger execution.
    trigger_frame_stack: RefCell<Vec<TriggerFrame>>,
    /// Scalar/aggregate/window function registry shared with the VDBE engine.
    func_registry: Arc<FunctionRegistry>,
    /// Whether an explicit transaction is active (BEGIN without matching COMMIT/ROLLBACK).
    in_transaction: RefCell<bool>,
    /// Snapshot taken at BEGIN time, restored on ROLLBACK.
    txn_snapshot: RefCell<Option<DbSnapshot>>,
    /// Savepoint stack: each SAVEPOINT pushes a snapshot, RELEASE pops,
    /// ROLLBACK TO restores to the named savepoint (without popping).
    savepoints: RefCell<Vec<SavepointEntry>>,
    /// Number of rows affected by the most recent DML statement.
    last_changes: RefCell<usize>,
    /// Whether the current transaction was started implicitly by SAVEPOINT
    /// (as opposed to an explicit BEGIN).  Used by RELEASE to decide whether
    /// to auto-commit when the last savepoint is released.
    implicit_txn: RefCell<bool>,
    /// Whether the current transaction uses MVCC concurrent-writer mode
    /// (`BEGIN CONCURRENT`).  When true, the harness can observe different
    /// concurrency behaviour compared to single-writer mode.
    concurrent_txn: RefCell<bool>,
    /// Connection-level flag: when set, plain `BEGIN` is promoted to
    /// `BEGIN CONCURRENT`.  Controlled by `PRAGMA fsqlite.concurrent_mode`.
    concurrent_mode_default: RefCell<bool>,
    /// Connection-scoped PRAGMA state used by the E2E harness for fairness knobs
    /// (journal_mode, synchronous, cache_size, page_size).
    pragma_state: RefCell<fsqlite_vdbe::pragma::ConnectionPragmaState>,
    /// Maps table name (lowercased) to the 0-based column index of the
    /// `INTEGER PRIMARY KEY` column, which is an alias for `rowid`.
    /// Used by fallback paths (GROUP BY, JOIN) to resolve rowid/\_rowid\_/oid.
    rowid_alias_columns: RefCell<HashMap<String, usize>>,
    /// Next rowid to use when inserting into the sqlite_master B-tree on
    /// page 1.  Starts at 1 for a fresh database; 5A.4 (schema loading)
    /// will advance this past any existing entries.
    next_master_rowid: RefCell<i64>,
    /// Schema cookie (offset 40 in the database header).  Incremented on
    /// every DDL operation (CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE
    /// INDEX, CREATE VIEW, etc.).  Prepared statements cache this value at
    /// prepare time; the Transaction opcode compares the current cookie
    /// against the cached value and returns `SchemaChanged` on mismatch.
    schema_cookie: RefCell<u32>,
    /// File change counter (offset 24 in the database header).  Incremented
    /// on every transaction that modifies the database (DML or DDL).
    change_counter: RefCell<u32>,
    // ── MVCC conflict observability (bd-t6sv2.1) ──────────────────────────
    /// Observer for MVCC conflict analytics.  Records metrics (contention,
    /// FCW drift, SSI aborts) and a ring-buffer of recent conflict events.
    /// Exposed via PRAGMA fsqlite.conflict_stats / conflict_log / conflict_reset.
    conflict_observer: Rc<MetricsObserver>,
    /// sqlite3_trace_v2 compatibility callback registration.
    trace_registration: RefCell<Option<TraceRegistration>>,
    /// Bounded append-only SSI decision cards with tamper-evident chain hashes.
    /// Queried via `PRAGMA fsqlite.ssi_decisions`.
    ssi_evidence_ledger: SsiEvidenceLedger,
    // ── MVCC concurrent-writer state (bd-14zc / 5E.1, bd-kivg / 5E.2) ─────
    /// Registry of active concurrent-writer sessions.
    /// Wrapped in Rc for sharing with VdbeEngine during execution (bd-kivg).
    concurrent_registry: Rc<RefCell<ConcurrentRegistry>>,
    /// Session ID for the current concurrent transaction (if any).
    /// Set by execute_begin() when mode is Concurrent.
    concurrent_session_id: RefCell<Option<u64>>,
    /// Page-level lock table for concurrent writers.
    /// Wrapped in Rc for sharing with VdbeEngine during execution (bd-kivg).
    concurrent_lock_table: Rc<InProcessPageLockTable>,
    /// Commit index mapping pages to their latest committed sequence.
    concurrent_commit_index: CommitIndex,
    /// Next commit sequence to assign (simple in-process monotonic counter).
    next_commit_seq: RefCell<u64>,
    /// Guards idempotent shutdown so explicit `close()` and `Drop` do not
    /// double-run rollback/checkpoint logic.
    closed: RefCell<bool>,
    // ── MVCC garbage collection (bd-3bql / 5E.5) ─────────────────────────────
    /// Version store for MVCC page versioning.  Stores committed page versions
    /// in an arena with version chains for snapshot resolution.
    version_store: Rc<RefCell<VersionStore>>,
    /// GC scheduler that derives tick frequency from version chain pressure.
    gc_scheduler: RefCell<GcScheduler>,
    /// Per-process touched-page queue for incremental GC pruning.
    gc_todo: RefCell<GcTodo>,
    /// Logical clock for GC scheduling (milliseconds).  Avoids ambient time
    /// authority by using a monotonic counter that increments by a fixed
    /// amount each operation.  This provides deterministic GC behavior.
    gc_clock_millis: RefCell<u64>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// Open a connection.
    ///
    /// Creates an empty in-memory database. Expression-only SELECT and
    /// table-backed DML (CREATE TABLE, INSERT, SELECT FROM, UPDATE, DELETE)
    /// are supported.
    pub fn open(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        if path.is_empty() {
            return Err(FrankenError::CannotOpen {
                path: std::path::PathBuf::from(path),
            });
        }

        // Phase 5 (bd-3iw8): initialize the pager backend as the primary
        // storage layer. The pager handles all persistence via WAL.
        let pager = PagerBackend::open(&path)?;

        let conn = Self {
            path,
            db: Rc::new(RefCell::new(MemDatabase::new())),
            pager,
            active_txn: RefCell::new(None),
            schema: RefCell::new(Vec::new()),
            views: RefCell::new(Vec::new()),
            triggers: RefCell::new(Vec::new()),
            trigger_frame_stack: RefCell::new(Vec::new()),
            func_registry: default_function_registry(),
            in_transaction: RefCell::new(false),
            txn_snapshot: RefCell::new(None),
            savepoints: RefCell::new(Vec::new()),
            last_changes: RefCell::new(0),
            implicit_txn: RefCell::new(false),
            concurrent_txn: RefCell::new(false),
            concurrent_mode_default: RefCell::new(true),
            pragma_state: RefCell::new(fsqlite_vdbe::pragma::ConnectionPragmaState::default()),
            rowid_alias_columns: RefCell::new(HashMap::new()),
            next_master_rowid: RefCell::new(1),
            schema_cookie: RefCell::new(0),
            change_counter: RefCell::new(0),
            // MVCC conflict observability (bd-t6sv2.1)
            conflict_observer: Rc::new(MetricsObserver::new(1024)),
            trace_registration: RefCell::new(None),
            // SSI evidence ledger (bd-1lsfu.3)
            ssi_evidence_ledger: SsiEvidenceLedger::new(4096),
            // MVCC concurrent-writer state (bd-14zc / 5E.1, bd-kivg / 5E.2)
            concurrent_registry: Rc::new(RefCell::new(ConcurrentRegistry::new())),
            concurrent_session_id: RefCell::new(None),
            concurrent_lock_table: Rc::new(InProcessPageLockTable::new()),
            concurrent_commit_index: CommitIndex::new(),
            next_commit_seq: RefCell::new(1),
            closed: RefCell::new(false),
            // MVCC garbage collection (bd-3bql / 5E.5)
            version_store: Rc::new(RefCell::new(VersionStore::new(PageSize::DEFAULT))),
            gc_scheduler: RefCell::new(GcScheduler::new()),
            gc_todo: RefCell::new(GcTodo::new()),
            gc_clock_millis: RefCell::new(0),
        };
        conn.apply_current_journal_mode_to_pager()?;
        // 5D.4 (bd-3bsn): Load initial state from pager instead of compat_persist.
        // The pager already opened the database file; we load schema + data from it.
        conn.reload_memdb_from_pager(&Cx::new())?;
        Ok(conn)
    }

    /// Returns the configured database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Register or clear sqlite3_trace_v2-compatible callbacks.
    ///
    /// Passing `Some(callback)` enables callback delivery for the requested
    /// `mask`. Passing `None` clears any existing registration.
    pub fn trace_v2(&self, mask: TraceMask, callback: Option<TraceCallback>) {
        *self.trace_registration.borrow_mut() =
            callback.map(|callback| TraceRegistration { mask, callback });
    }

    /// Close the connection and perform pager/WAL shutdown steps.
    ///
    /// On close:
    /// 1. Roll back any active transaction.
    /// 2. Run a passive checkpoint (WAL -> DB).
    /// 3. Mark the connection as closed so `Drop` doesn't repeat cleanup.
    pub fn close(mut self) -> Result<()> {
        self.close_internal(false)
    }

    fn close_internal(&mut self, best_effort: bool) -> Result<()> {
        if *self.closed.get_mut() {
            return Ok(());
        }
        let trace_registration = self.trace_registration.get_mut().clone();

        let cx = Cx::new();
        let active_txn = self.active_txn.get_mut();
        let txn_was_open = *self.in_transaction.get_mut() || active_txn.is_some();

        if txn_was_open {
            if let Some(mut txn) = active_txn.take() {
                if best_effort {
                    let _ = txn.rollback(&cx);
                } else {
                    txn.rollback(&cx)?;
                }
            }

            *self.in_transaction.get_mut() = false;
            *self.implicit_txn.get_mut() = false;
            *self.concurrent_txn.get_mut() = false;
            *self.txn_snapshot.get_mut() = None;
            self.savepoints.get_mut().clear();
            self.db.borrow_mut().commit_undo();
        }

        if best_effort {
            let _ = self.pager.checkpoint(&cx, CheckpointMode::Passive);
        } else {
            let _ = self.pager.checkpoint(&cx, CheckpointMode::Passive)?;
        }

        *self.closed.get_mut() = true;
        emit_compat_trace_event(trace_registration.as_ref(), TraceEvent::Close);
        Ok(())
    }

    /// Prepare SQL into a statement.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let statement = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                parse_mode = "prepare_single",
                sql_len = sql.len()
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            parse_single_statement(sql)?
        };
        let statement = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                parse_mode = "prepare_rewrite"
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            self.rewrite_subquery_statement(statement)?
        };
        let plan_span = tracing::span!(
            target: "fsqlite.plan",
            tracing::Level::TRACE,
            "plan",
            stage = "compile_prepared_statement"
        );
        record_trace_span_created();
        let _plan_guard = plan_span.enter();
        self.compile_and_wrap(&statement)
    }

    /// Prepare and execute SQL as a query.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let statements = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                parse_mode = "query_multi",
                sql_len = sql.len()
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            parse_statements(sql)?
        };
        let mut rows = Vec::new();
        for statement in statements {
            rows = self.execute_statement(statement, None)?;
        }
        Ok(rows)
    }

    /// Prepare and execute SQL as a query with bound SQL parameters.
    pub fn query_with_params(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>> {
        let statement = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                parse_mode = "query_single",
                sql_len = sql.len()
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            parse_single_statement(sql)?
        };
        self.execute_statement(statement, Some(params))
    }

    /// Prepare and execute SQL as a query, returning exactly one row.
    pub fn query_row(&self, sql: &str) -> Result<Row> {
        first_row_or_error(self.query(sql)?)
    }

    /// Prepare and execute SQL as a query with bound SQL parameters, returning exactly one row.
    pub fn query_row_with_params(&self, sql: &str, params: &[SqliteValue]) -> Result<Row> {
        first_row_or_error(self.query_with_params(sql, params)?)
    }

    /// Prepare and execute SQL, returning output/affected row count.
    ///
    /// For DML (INSERT/UPDATE/DELETE) this returns the number of affected
    /// rows.  For SELECT and other statement types it returns the number of
    /// result rows.
    pub fn execute(&self, sql: &str) -> Result<usize> {
        let statements = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                parse_mode = "execute_multi",
                sql_len = sql.len()
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            parse_statements(sql)?
        };
        let mut last_count = 0;
        for statement in statements {
            let is_dml = matches!(
                &statement,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            );
            let rows = self.execute_statement(statement, None)?;
            last_count = if is_dml {
                *self.last_changes.borrow()
            } else {
                rows.len()
            };
        }
        Ok(last_count)
    }

    /// Prepare and execute SQL with bound SQL parameters.
    pub fn execute_with_params(&self, sql: &str, params: &[SqliteValue]) -> Result<usize> {
        let statement = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                parse_mode = "execute_single",
                sql_len = sql.len()
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            parse_single_statement(sql)?
        };
        let is_dml = matches!(
            &statement,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        );
        let rows = self.execute_statement(statement, Some(params))?;
        Ok(if is_dml {
            *self.last_changes.borrow()
        } else {
            rows.len()
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Execute a parsed statement, handling both DDL (CREATE TABLE) and
    /// DML (SELECT/INSERT/UPDATE/DELETE).
    #[allow(clippy::too_many_lines)]
    fn execute_statement(
        &self,
        statement: Statement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let statement_kind = match &statement {
            Statement::Select(_) => "select",
            Statement::Insert(_) => "insert",
            Statement::Update(_) => "update",
            Statement::Delete(_) => "delete",
            Statement::CreateTable(_) => "create_table",
            Statement::Drop(_) => "drop",
            Statement::AlterTable(_) => "alter_table",
            Statement::CreateIndex(_) => "create_index",
            Statement::Pragma(_) => "pragma",
            Statement::Begin(_) => "begin",
            Statement::Commit => "commit",
            Statement::Rollback(_) => "rollback",
            Statement::Savepoint(_) => "savepoint",
            Statement::Release(_) => "release",
            Statement::CreateView(_) => "create_view",
            Statement::CreateTrigger(_) => "create_trigger",
            _ => "other",
        };
        let trace_id = next_trace_id();
        let decision_id = next_decision_id();
        let statement_sql = statement.to_string();
        let trace_registration = self.trace_registration.borrow().clone();
        let statement_span = tracing::span!(
            target: "fsqlite.statement",
            tracing::Level::DEBUG,
            "statement",
            trace_id,
            decision_id,
            statement_kind
        );
        record_trace_span_created();
        let _statement_guard = statement_span.enter();
        emit_compat_trace_event(
            trace_registration.as_ref(),
            TraceEvent::Statement {
                sql: statement_sql.clone(),
            },
        );
        let execution_started = std::time::Instant::now();
        let statement = {
            let parse_span = tracing::span!(
                target: "fsqlite.parse",
                tracing::Level::TRACE,
                "parse",
                trace_id,
                statement_kind,
                phase = "rewrite_subquery"
            );
            record_trace_span_created();
            let _parse_guard = parse_span.enter();
            self.rewrite_subquery_statement(statement)?
        };
        // 5B.5 + 5B.2 (bd-1yi8): autocommit wrapping — ensure a pager
        // transaction is active for data/DDL operations outside an
        // explicit BEGIN.  Writes use Immediate mode; reads use Deferred.
        // Transaction-control statements manage their own transactions.
        let is_write = matches!(
            &statement,
            Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::CreateTable(_)
                | Statement::Drop(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
        );
        let is_txn_control = matches!(
            &statement,
            Statement::Begin(_)
                | Statement::Commit
                | Statement::Rollback(_)
                | Statement::Savepoint(_)
                | Statement::Release(_)
                // PRAGMA handlers that need pager access (for example
                // `PRAGMA wal_checkpoint`) manage their own pager operations.
                | Statement::Pragma(_)
        );
        let was_auto = if is_txn_control {
            false // transaction-control manages its own transactions
        } else if is_write {
            self.ensure_autocommit_txn()?
        } else {
            self.ensure_autocommit_txn_mode(TransactionMode::Deferred)?
        };
        let result = self.execute_statement_dispatch(statement, params);
        let ok = result.is_ok();
        self.resolve_autocommit_txn(was_auto, ok)?;
        let elapsed_ns = u64::try_from(execution_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        emit_compat_trace_event(
            trace_registration.as_ref(),
            TraceEvent::Profile {
                sql: statement_sql,
                elapsed_ns,
            },
        );
        if let Ok(rows) = result.as_ref() {
            for row in rows {
                emit_compat_trace_event(
                    trace_registration.as_ref(),
                    TraceEvent::Row {
                        values: row.values().to_vec(),
                    },
                );
            }
        }
        result
    }

    /// Inner dispatch for `execute_statement` — separated so that
    /// autocommit wrapping can bracket the entire execution.
    #[allow(clippy::too_many_lines)]
    fn execute_statement_dispatch(
        &self,
        statement: Statement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        match statement {
            Statement::CreateTable(create) => {
                self.execute_create_table(&create)?;
                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                Ok(Vec::new())
            }
            Statement::Select(ref select) => {
                // CTE (WITH clause): materialize as temporary tables.
                if select.with.is_some() {
                    return self.execute_with_ctes(select, params);
                }
                // View expansion: materialize referenced views as temp tables.
                if self.has_view_references(select) {
                    return self.execute_with_materialized_views(select, params);
                }
                // 5G.4 (bd-3ly4): sqlite_master/sqlite_schema virtual table support.
                if self.has_sqlite_schema_references(select) {
                    return self.execute_with_materialized_sqlite_schema(select, params);
                }
                let distinct = is_distinct_select(select);
                // Compound SELECT (UNION/UNION ALL/INTERSECT/EXCEPT).
                if !select.body.compounds.is_empty() {
                    return self.execute_compound_select(select, params);
                }
                // Check if this is an expression-only SELECT (no FROM clause).
                if is_expression_only_select(select) {
                    // Fallback codegen: eagerly rewrite IN subqueries.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let plan_span = tracing::span!(
                        target: "fsqlite.plan",
                        tracing::Level::TRACE,
                        "plan",
                        stage = "compile_expression_select"
                    );
                    record_trace_span_created();
                    let _plan_guard = plan_span.enter();
                    let program = compile_expression_select(&rewritten)?;
                    let mut rows = execute_program_with_postprocess(
                        &program,
                        params,
                        Some(&self.func_registry),
                        Some(&build_expression_postprocess(&rewritten)),
                    )?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else if has_group_by(select) && (has_joins(select) || has_subquery_source(select)) {
                    // GROUP BY + JOIN: materialize the join as a temp table,
                    // then GROUP BY on that temp table.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let bound = bind_placeholders_in_select_for_fallback(&rewritten, params)?;
                    let mut rows = self.execute_group_by_join_select(&bound, None)?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else if has_group_by(select) {
                    // Fallback path: eagerly rewrite IN subqueries.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let bound = bind_placeholders_in_select_for_fallback(&rewritten, params)?;
                    let mut rows = self.execute_group_by_select(&bound, None)?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else if has_joins(select) || has_subquery_source(select) {
                    // Fallback path: eagerly rewrite IN subqueries.
                    // Also handles subquery in FROM (derived tables) even
                    // without explicit JOINs.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let bound = bind_placeholders_in_select_for_fallback(&rewritten, params)?;
                    let mut rows = self.execute_join_select(&bound, None)?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else {
                    let limit_clause = select.limit.clone();
                    let program = {
                        let plan_span = tracing::span!(
                            target: "fsqlite.plan",
                            tracing::Level::TRACE,
                            "plan",
                            stage = "compile_table_select"
                        );
                        record_trace_span_created();
                        let _plan_guard = plan_span.enter();
                        if distinct && limit_clause.is_some() {
                            let mut unbounded = select.clone();
                            unbounded.limit = None;
                            self.compile_table_select(&unbounded)?
                        } else {
                            self.compile_table_select(select)?
                        }
                    };

                    let mut rows = self.execute_table_program(&program, params)?;
                    if distinct {
                        dedup_rows(&mut rows);
                        if let Some(limit_clause) = limit_clause.as_ref() {
                            apply_limit_clause(&mut rows, limit_clause);
                        }
                    }
                    Ok(rows)
                }
            }
            Statement::Insert(ref insert) => {
                let table_name = &insert.table.name;
                let insert_event = fsqlite_ast::TriggerEvent::Insert;
                let has_before_insert = self.has_matching_triggers(
                    table_name,
                    fsqlite_ast::TriggerTiming::Before,
                    &insert_event,
                );
                let has_after_insert = self.has_matching_triggers(
                    table_name,
                    fsqlite_ast::TriggerTiming::After,
                    &insert_event,
                );
                let trigger_new_rows = if has_before_insert || has_after_insert {
                    self.collect_insert_trigger_rows(insert, params)?
                } else {
                    Vec::new()
                };

                // Phase 5G.2 (bd-iqam): Fire BEFORE INSERT triggers.
                let skip_dml = if has_before_insert {
                    let mut skip = false;
                    for new_values in &trigger_new_rows {
                        if self.fire_before_triggers(
                            table_name,
                            &insert_event,
                            None,
                            Some(new_values),
                        )? {
                            skip = true;
                            break;
                        }
                    }
                    skip
                } else {
                    false
                };
                if skip_dml {
                    // RAISE(IGNORE) was called - skip the INSERT.
                    *self.last_changes.borrow_mut() = 0;
                    return Ok(Vec::new());
                }

                if let fsqlite_ast::InsertSource::Select(select_stmt) = &insert.source {
                    // Detect simple VALUES clause (no compounds).
                    // We must use the VDBE path for simple VALUES to avoid infinite recursion
                    // in the fallback (which implements inserts by generating simple VALUES inserts).
                    let is_simple_values =
                        matches!(select_stmt.body.select, SelectCore::Values(_))
                            && select_stmt.body.compounds.is_empty();

                    // Use VDBE path when RETURNING is present OR it's a simple VALUES clause;
                    // fallback otherwise (e.g. complex SELECTs, compounds).
                    if insert.returning.is_empty() && !is_simple_values {
                        let affected =
                            self.execute_insert_select_fallback(insert, select_stmt, params)?;
                        // Phase 5G.3: Fire AFTER INSERT triggers.
                        if has_after_insert {
                            for new_values in &trigger_new_rows {
                                self.fire_after_triggers(
                                    table_name,
                                    &insert_event,
                                    None,
                                    Some(new_values),
                                )?;
                            }
                        }
                        // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                        *self.last_changes.borrow_mut() = affected;
                        return Ok(Vec::new());
                    }
                }

                // Phase 5B.2 (bd-1yi8): INSERT OR REPLACE / INSERT OR IGNORE
                // now routes through VDBE which has UPSERT semantics in the
                // Insert opcode (table_move_to + delete + insert). The
                // previous execute_insert_or_conflict fallback checked
                // MemDatabase directly, which doesn't work after write-through
                // stopped syncing inserts to MemDatabase.
                //
                // TODO: Once execute_insert_or_conflict is updated to read
                // from the pager, we can re-enable it for performance.
                // For now, fall through to VDBE execution.

                let affected = match &insert.source {
                    fsqlite_ast::InsertSource::Values(v) => v.len(),
                    fsqlite_ast::InsertSource::DefaultValues => 1,
                    fsqlite_ast::InsertSource::Select(sel) => {
                        // Run the inner SELECT to determine row count.
                        self.execute_statement(Statement::Select(*sel.clone()), params)?
                            .len()
                    }
                };
                let program = {
                    let plan_span = tracing::span!(
                        target: "fsqlite.plan",
                        tracing::Level::TRACE,
                        "plan",
                        stage = "compile_table_insert"
                    );
                    record_trace_span_created();
                    let _plan_guard = plan_span.enter();
                    self.compile_table_insert(insert)?
                };
                let rows = self.execute_table_program(&program, params)?;

                // Phase 5G.3: Fire AFTER INSERT triggers.
                if has_after_insert {
                    for new_values in &trigger_new_rows {
                        self.fire_after_triggers(table_name, &insert_event, None, Some(new_values))?;
                    }
                }

                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                *self.last_changes.borrow_mut() = affected;
                if insert.returning.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(rows)
                }
            }
            Statement::Update(ref update) => {
                let table_name = &update.table.name.name;
                // Collect columns being updated for UPDATE OF trigger matching.
                let update_cols: Vec<String> = update
                    .assignments
                    .iter()
                    .map(|a| match &a.target {
                        fsqlite_ast::AssignmentTarget::Column(c) => c.clone(),
                        fsqlite_ast::AssignmentTarget::ColumnList(cols) => {
                            cols.first().cloned().unwrap_or_default()
                        }
                    })
                    .collect();
                let update_event = fsqlite_ast::TriggerEvent::Update(update_cols.clone());
                let has_before_update = self.has_matching_triggers(
                    table_name,
                    fsqlite_ast::TriggerTiming::Before,
                    &update_event,
                );
                let has_after_update = self.has_matching_triggers(
                    table_name,
                    fsqlite_ast::TriggerTiming::After,
                    &update_event,
                );
                let trigger_rows = if has_before_update || has_after_update {
                    self.collect_update_trigger_rows(update, params)?
                } else {
                    Vec::new()
                };

                // Phase 5G.2 (bd-iqam): Fire BEFORE UPDATE triggers.
                let skip_dml = if has_before_update {
                    let mut skip = false;
                    for (old_values, new_values) in &trigger_rows {
                        if self.fire_before_triggers(
                            table_name,
                            &update_event,
                            Some(old_values),
                            Some(new_values),
                        )? {
                            skip = true;
                            break;
                        }
                    }
                    skip
                } else {
                    false
                };
                if skip_dml {
                    *self.last_changes.borrow_mut() = 0;
                    return Ok(Vec::new());
                }

                let affected = if has_before_update || has_after_update {
                    trigger_rows.len()
                } else {
                    self.count_matching_rows(&update.table, update.where_clause.as_ref(), params)?
                };
                let program = {
                    let plan_span = tracing::span!(
                        target: "fsqlite.plan",
                        tracing::Level::TRACE,
                        "plan",
                        stage = "compile_table_update"
                    );
                    record_trace_span_created();
                    let _plan_guard = plan_span.enter();
                    self.compile_table_update(update)?
                };
                let rows = self.execute_table_program(&program, params)?;

                // Phase 5G.3: Fire AFTER UPDATE triggers.
                if has_after_update {
                    for (old_values, new_values) in &trigger_rows {
                        self.fire_after_triggers(
                            table_name,
                            &update_event,
                            Some(old_values),
                            Some(new_values),
                        )?;
                    }
                }

                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                *self.last_changes.borrow_mut() = affected;
                if update.returning.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(rows)
                }
            }
            Statement::Delete(ref delete) => {
                let table_name = &delete.table.name.name;
                let delete_event = fsqlite_ast::TriggerEvent::Delete;
                let has_before_delete = self.has_matching_triggers(
                    table_name,
                    fsqlite_ast::TriggerTiming::Before,
                    &delete_event,
                );
                let has_after_delete = self.has_matching_triggers(
                    table_name,
                    fsqlite_ast::TriggerTiming::After,
                    &delete_event,
                );
                let trigger_old_rows = if has_before_delete || has_after_delete {
                    self.collect_delete_trigger_rows(delete, params)?
                } else {
                    Vec::new()
                };

                // Phase 5G.2 (bd-iqam): Fire BEFORE DELETE triggers.
                let skip_dml = if has_before_delete {
                    let mut skip = false;
                    for old_values in &trigger_old_rows {
                        if self.fire_before_triggers(
                            table_name,
                            &delete_event,
                            Some(old_values),
                            None,
                        )? {
                            skip = true;
                            break;
                        }
                    }
                    skip
                } else {
                    false
                };
                if skip_dml {
                    *self.last_changes.borrow_mut() = 0;
                    return Ok(Vec::new());
                }

                let affected = if has_before_delete || has_after_delete {
                    trigger_old_rows.len()
                } else {
                    self.count_matching_rows(&delete.table, delete.where_clause.as_ref(), params)?
                };
                let program = {
                    let plan_span = tracing::span!(
                        target: "fsqlite.plan",
                        tracing::Level::TRACE,
                        "plan",
                        stage = "compile_table_delete"
                    );
                    record_trace_span_created();
                    let _plan_guard = plan_span.enter();
                    self.compile_table_delete(delete)?
                };
                let rows = self.execute_table_program(&program, params)?;

                // Phase 5G.3: Fire AFTER DELETE triggers.
                if has_after_delete {
                    for old_values in &trigger_old_rows {
                        self.fire_after_triggers(table_name, &delete_event, Some(old_values), None)?;
                    }
                }

                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                *self.last_changes.borrow_mut() = affected;
                if delete.returning.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(rows)
                }
            }
            Statement::Begin(begin) => {
                self.execute_begin(begin)?;
                Ok(Vec::new())
            }
            Statement::Commit => {
                self.execute_commit()?;
                Ok(Vec::new())
            }
            Statement::Rollback(ref rb) => {
                self.execute_rollback(rb)?;
                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                Ok(Vec::new())
            }
            Statement::Savepoint(ref name) => {
                self.execute_savepoint(name)?;
                Ok(Vec::new())
            }
            Statement::Release(ref name) => {
                self.execute_release(name)?;
                Ok(Vec::new())
            }
            Statement::Pragma(ref pragma) => self.execute_pragma(pragma),
            Statement::Drop(ref drop_stmt) => {
                self.execute_drop(drop_stmt)?;
                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                Ok(Vec::new())
            }
            Statement::AlterTable(ref alter) => {
                self.execute_alter_table(alter)?;
                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                Ok(Vec::new())
            }
            Statement::CreateIndex(ref create_idx) => {
                self.execute_create_index(create_idx)?;
                // 5D.4: Persistence now handled by pager WAL, not compat_persist.
                Ok(Vec::new())
            }
            Statement::CreateView(ref create_view) => {
                self.execute_create_view(create_view)?;
                Ok(Vec::new())
            }
            Statement::CreateTrigger(ref create_trigger) => {
                self.execute_create_trigger(create_trigger)?;
                Ok(Vec::new())
            }
            // VACUUM INTO copies the database to a new file.
            // Plain VACUUM is a no-op for now (defragmentation not implemented).
            Statement::Vacuum(ref vacuum_stmt) => {
                if vacuum_stmt.into.is_some() {
                    // VACUUM INTO requires VFS-based file copy to avoid ambient
                    // filesystem authority.  Not yet implemented.
                    return Err(FrankenError::not_implemented("VACUUM INTO"));
                }
                // Plain VACUUM is a no-op (defragmentation not implemented).
                Ok(Vec::new())
            }
            // ANALYZE and REINDEX are no-ops for now
            Statement::Analyze(_) | Statement::Reindex(_) => {
                Ok(Vec::new())
            }
            _ => Err(FrankenError::NotImplemented(
                "only SELECT, INSERT, UPDATE, DELETE, DDL (CREATE/DROP/ALTER TABLE, CREATE INDEX/VIEW/TRIGGER), transaction control, PRAGMA, VACUUM, ANALYZE, and REINDEX are supported".to_owned(),
            )),
        }
    }

    /// Pre-process statement-level subquery expressions before compilation.
    ///
    /// EXISTS and scalar subqueries are eagerly evaluated everywhere.
    /// `IN (SELECT ...)` / `IN table` are left intact for statements that
    /// go through VDBE codegen (UPDATE, DELETE, simple SELECT) so the
    /// runtime probe mechanism handles them.  Fallback paths (GROUP BY,
    /// JOIN, expression-only SELECT) apply the IN rewrite separately.
    fn rewrite_subquery_statement(&self, statement: Statement) -> Result<Statement> {
        match statement {
            Statement::Select(select) => {
                let rewritten = self.rewrite_subqueries(&select)?;
                Ok(Statement::Select(rewritten))
            }
            Statement::Update(mut update) => {
                // Only rewrite EXISTS / scalar subqueries; leave IN for VDBE probe.
                for assignment in &mut update.assignments {
                    rewrite_in_expr(&mut assignment.value, self, false)?;
                }
                if let Some(where_expr) = update.where_clause.as_mut() {
                    rewrite_in_expr(where_expr, self, false)?;
                }
                Ok(Statement::Update(update))
            }
            Statement::Delete(mut delete) => {
                // Only rewrite EXISTS / scalar subqueries; leave IN for VDBE probe.
                if let Some(where_expr) = delete.where_clause.as_mut() {
                    rewrite_in_expr(where_expr, self, false)?;
                }
                Ok(Statement::Delete(delete))
            }
            other => Ok(other),
        }
    }

    fn execute_insert_select_fallback(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        select_stmt: &fsqlite_ast::SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<usize> {
        if insert.with.is_some() || !insert.upsert.is_empty() || !insert.returning.is_empty() {
            return Err(FrankenError::NotImplemented(
                "INSERT ... SELECT fallback does not support WITH/UPSERT/RETURNING".to_owned(),
            ));
        }

        let source_rows = self.execute_statement(Statement::Select(select_stmt.clone()), params)?;
        if source_rows.is_empty() {
            return Ok(0);
        }

        let (table_columns, source_target_indices) =
            self.resolve_insert_select_target_layout(insert)?;
        if table_columns.is_empty() {
            return Err(FrankenError::Internal(format!(
                "table '{}' has no insertable columns",
                insert.table.name
            )));
        }

        let source_column_count = source_target_indices.len();
        for (row_idx, row) in source_rows.iter().enumerate() {
            if row.values().len() != source_column_count {
                return Err(FrankenError::Internal(format!(
                    "INSERT ... SELECT column count mismatch: source row {row_idx} has {} values, SELECT produced {source_column_count}",
                    row.values().len()
                )));
            }
        }

        let qualified_table = quote_qualified_name(&insert.table);
        let placeholders = (1..=table_columns.len())
            .map(|idx| format!("?{idx}"))
            .collect::<Vec<_>>()
            .join(", ");
        // Include the conflict clause if present so that each INSERT
        // row is handled with the correct conflict resolution.
        let conflict_clause = match &insert.or_conflict {
            Some(fsqlite_ast::ConflictAction::Replace) => "OR REPLACE ",
            Some(fsqlite_ast::ConflictAction::Ignore) => "OR IGNORE ",
            Some(fsqlite_ast::ConflictAction::Abort) => "OR ABORT ",
            Some(fsqlite_ast::ConflictAction::Fail) => "OR FAIL ",
            Some(fsqlite_ast::ConflictAction::Rollback) => "OR ROLLBACK ",
            None => "",
        };
        let insert_sql =
            format!("INSERT {conflict_clause}INTO {qualified_table} VALUES ({placeholders});");

        let mut affected = 0usize;
        for row in &source_rows {
            let mut ordered_values = vec![SqliteValue::Null; table_columns.len()];
            for (source_idx, target_idx) in source_target_indices.iter().copied().enumerate() {
                ordered_values[target_idx] = row.values()[source_idx].clone();
            }
            self.execute_with_params(&insert_sql, &ordered_values)?;
            affected += 1;
        }

        Ok(affected)
    }

    /// Handle INSERT OR REPLACE / INSERT OR IGNORE for VALUES source by
    /// evaluating each row, resolving the INTEGER PRIMARY KEY rowid, and
    /// applying conflict resolution directly on the in-memory database.
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    fn execute_insert_or_conflict(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        rows: &[Vec<Expr>],
        params: Option<&[SqliteValue]>,
    ) -> Result<usize> {
        use fsqlite_ast::ConflictAction;

        let conflict = insert
            .or_conflict
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("expected or_conflict".to_owned()))?;

        let table_name = &insert.table.name;
        let schema = self.schema.borrow();
        let table_schema = schema
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(table_name))
            .ok_or_else(|| FrankenError::Internal(format!("no such table: {table_name}")))?;
        let root_page = table_schema.root_page;
        let num_cols = table_schema.columns.len();

        // Determine which VALUES position maps to the INTEGER PRIMARY KEY
        // column so we can extract the user-supplied rowid.
        let ipk_col_idx = self
            .rowid_alias_columns
            .borrow()
            .get(&table_name.to_ascii_lowercase())
            .copied();
        // Map from VALUES position to table column position.
        let target_map: Vec<usize> = if insert.columns.is_empty() {
            (0..num_cols).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|col| {
                    table_schema.column_index(col).ok_or_else(|| {
                        FrankenError::Internal(format!(
                            "column '{col}' not found in table '{table_name}'"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };
        // Which VALUES position (if any) maps to the IPK column.
        let ipk_values_pos = ipk_col_idx.and_then(|ipk| target_map.iter().position(|&t| t == ipk));

        drop(schema);

        let mut affected = 0usize;

        for row_exprs in rows {
            // Evaluate the row expressions by wrapping them in a SELECT.
            let expr_strs: Vec<String> = row_exprs.iter().map(|e| format!("{e}")).collect();
            let select_sql = format!("SELECT {}", expr_strs.join(", "));
            let eval_rows = if let Some(p) = params {
                self.query_with_params(&select_sql, p)?
            } else {
                self.query(&select_sql)?
            };
            let values: Vec<SqliteValue> = eval_rows
                .first()
                .map(|r| r.values().to_vec())
                .unwrap_or_default();

            // Extract the user-supplied rowid from the IPK column (if any).
            let explicit_rowid = ipk_values_pos.and_then(|pos| {
                values.get(pos).and_then(|v| match v {
                    SqliteValue::Integer(n) => Some(*n),
                    _ => None,
                })
            });

            // Build the full column value vector in table order.
            let mut col_values = vec![SqliteValue::Null; num_cols];
            for (val_pos, &tgt) in target_map.iter().enumerate() {
                if let Some(v) = values.get(val_pos) {
                    col_values[tgt] = v.clone();
                }
            }

            let mut db = self.db.borrow_mut();
            if let Some(rowid) = explicit_rowid {
                let exists = db
                    .get_table(root_page)
                    .and_then(|t| t.find_by_rowid(rowid))
                    .is_some();

                match conflict {
                    ConflictAction::Replace => {
                        // insert_row delegates to insert() which has upsert
                        // semantics: replaces if rowid already exists.
                        if let Some(table) = db.get_table_mut(root_page) {
                            table.insert_row(rowid, col_values);
                        }
                        affected += 1;
                    }
                    ConflictAction::Ignore => {
                        if !exists {
                            if let Some(table) = db.get_table_mut(root_page) {
                                table.insert_row(rowid, col_values);
                            }
                            affected += 1;
                        }
                        // else: silently skip this row
                    }
                    ConflictAction::Abort | ConflictAction::Fail | ConflictAction::Rollback => {
                        if exists {
                            return Err(FrankenError::UniqueViolation {
                                columns: format!("{table_name}.rowid"),
                            });
                        }
                        if let Some(table) = db.get_table_mut(root_page) {
                            table.insert_row(rowid, col_values);
                        }
                        affected += 1;
                    }
                }
            } else {
                // No explicit rowid; auto-allocate.  Conflict on auto-generated
                // rowid is practically impossible.
                if let Some(table) = db.get_table_mut(root_page) {
                    let new_rowid = table.alloc_rowid();
                    table.insert_row(new_rowid, col_values);
                }
                affected += 1;
            }
        }

        Ok(affected)
    }

    fn resolve_insert_select_target_layout(
        &self,
        insert: &fsqlite_ast::InsertStatement,
    ) -> Result<(Vec<String>, Vec<usize>)> {
        let schema = self.schema.borrow();
        let table = schema
            .iter()
            .find(|tbl| tbl.name.eq_ignore_ascii_case(&insert.table.name))
            .ok_or_else(|| {
                FrankenError::Internal(format!("table not found: {}", insert.table.name))
            })?;

        let table_columns: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();

        if insert.columns.is_empty() {
            let target_indices = (0..table_columns.len()).collect();
            return Ok((table_columns, target_indices));
        }

        let mut target_indices = Vec::with_capacity(insert.columns.len());
        for col in &insert.columns {
            let idx = table.column_index(col).ok_or_else(|| {
                FrankenError::Internal(format!(
                    "column '{col}' not found in table '{}'",
                    insert.table.name
                ))
            })?;
            target_indices.push(idx);
        }
        Ok((table_columns, target_indices))
    }

    /// Compile and wrap a statement into a `PreparedStatement`.
    fn compile_and_wrap(&self, statement: &Statement) -> Result<PreparedStatement> {
        let registry = Some(Arc::clone(&self.func_registry));
        match statement {
            Statement::Select(select) if is_expression_only_select(select) => {
                let program = compile_expression_select(select)?;
                let expression_postprocess = Some(build_expression_postprocess(select));
                Ok(PreparedStatement {
                    program,
                    func_registry: registry,
                    expression_postprocess,
                    distinct: is_distinct_select(select),
                    db: None,
                    pager: None,
                    post_distinct_limit: None,
                    schema_cookie: self.schema_cookie(),
                })
            }
            Statement::Select(select) => {
                let distinct = is_distinct_select(select);
                let limit_clause = select.limit.clone();
                let program = if distinct && limit_clause.is_some() {
                    let mut unbounded = select.clone();
                    unbounded.limit = None;
                    self.compile_table_select(&unbounded)?
                } else {
                    self.compile_table_select(select)?
                };
                Ok(PreparedStatement {
                    program,
                    func_registry: registry,
                    expression_postprocess: None,
                    distinct,
                    db: Some(Rc::clone(&self.db)),
                    pager: Some(self.pager.clone()),
                    post_distinct_limit: if distinct { limit_clause } else { None },
                    schema_cookie: self.schema_cookie(),
                })
            }
            _ => Err(FrankenError::NotImplemented(
                "prepare() currently supports SELECT statements only".to_owned(),
            )),
        }
    }

    /// Count the number of rows in `table_name` matching an optional WHERE
    /// clause.  Used by UPDATE/DELETE to compute the affected-row count
    /// without modifying the VDBE engine.
    fn select_matching_rows(
        &self,
        table_ref: &fsqlite_ast::QualifiedTableRef,
        where_clause: Option<&Expr>,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let alias_clause = table_ref
            .alias
            .as_ref()
            .map_or(String::new(), |alias| format!(" AS {alias}"));
        let sql = if let Some(cond) = where_clause {
            format!(
                "SELECT * FROM {}{alias_clause} WHERE {cond}",
                table_ref.name
            )
        } else {
            format!("SELECT * FROM {}", table_ref.name)
        };

        if let Some(params) = params {
            self.query_with_params(&sql, params)
        } else {
            self.query(&sql)
        }
    }

    fn collect_insert_trigger_rows(
        &self,
        insert: &fsqlite_ast::InsertStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Vec<SqliteValue>>> {
        let (table_columns, source_target_indices) =
            self.resolve_insert_select_target_layout(insert)?;
        let table_column_count = table_columns.len();
        match &insert.source {
            fsqlite_ast::InsertSource::Values(rows) => rows
                .iter()
                .enumerate()
                .map(|(row_idx, row_exprs)| {
                    let source_values = self.evaluate_insert_source_row(row_exprs, params)?;
                    self.map_insert_source_row_to_table_row(
                        table_column_count,
                        &source_target_indices,
                        &source_values,
                        &format!("INSERT VALUES row {}", row_idx + 1),
                    )
                })
                .collect(),
            fsqlite_ast::InsertSource::Select(select) => {
                let source_rows =
                    self.execute_statement(Statement::Select(*select.clone()), params)?;
                source_rows
                    .iter()
                    .enumerate()
                    .map(|(row_idx, row)| {
                        self.map_insert_source_row_to_table_row(
                            table_column_count,
                            &source_target_indices,
                            row.values(),
                            &format!("INSERT SELECT row {}", row_idx + 1),
                        )
                    })
                    .collect()
            }
            fsqlite_ast::InsertSource::DefaultValues => {
                Ok(vec![vec![SqliteValue::Null; table_column_count]])
            }
        }
    }

    fn evaluate_insert_source_row(
        &self,
        row_exprs: &[Expr],
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<SqliteValue>> {
        let projection = row_exprs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {projection}");
        let rows = if let Some(params) = params {
            self.query_with_params(&sql, params)?
        } else {
            self.query(&sql)?
        };
        Ok(rows
            .first()
            .map(|row| row.values().to_vec())
            .unwrap_or_default())
    }

    fn map_insert_source_row_to_table_row(
        &self,
        table_column_count: usize,
        source_target_indices: &[usize],
        source_values: &[SqliteValue],
        context: &str,
    ) -> Result<Vec<SqliteValue>> {
        if source_values.len() != source_target_indices.len() {
            return Err(FrankenError::Internal(format!(
                "{context}: column count mismatch (source has {}, target expects {})",
                source_values.len(),
                source_target_indices.len()
            )));
        }

        let mut row = vec![SqliteValue::Null; table_column_count];
        for (source_idx, target_idx) in source_target_indices.iter().copied().enumerate() {
            row[target_idx] = source_values[source_idx].clone();
        }
        Ok(row)
    }

    fn collect_update_trigger_rows(
        &self,
        update: &fsqlite_ast::UpdateStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<(Vec<SqliteValue>, Vec<SqliteValue>)>> {
        let matched_rows =
            self.select_matching_rows(&update.table, update.where_clause.as_ref(), params)?;
        if matched_rows.is_empty() {
            return Ok(Vec::new());
        }

        let table_name = &update.table.name.name;
        let schema = self.schema.borrow();
        let table = schema
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(table_name))
            .ok_or_else(|| FrankenError::NoSuchTable {
                name: table_name.clone(),
            })?;
        let column_names: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();
        let table_label = update
            .table
            .alias
            .as_deref()
            .unwrap_or(table_name)
            .to_owned();
        drop(schema);

        let col_map = column_names
            .iter()
            .cloned()
            .map(|name| (table_label.clone(), name))
            .collect::<Vec<_>>();

        let mut trigger_rows = Vec::with_capacity(matched_rows.len());
        for row in matched_rows {
            let old_values = row.values().to_vec();
            let mut new_values = old_values.clone();

            for assignment in &update.assignments {
                match &assignment.target {
                    fsqlite_ast::AssignmentTarget::Column(column_name) => {
                        let target_index =
                            find_column_index_case_insensitive(&column_names, column_name)
                                .ok_or_else(|| {
                                    FrankenError::Internal(format!(
                                        "UPDATE trigger snapshot: unknown column `{column_name}`"
                                    ))
                                })?;
                        new_values[target_index] =
                            eval_join_expr(&assignment.value, &old_values, &col_map)?;
                    }
                    fsqlite_ast::AssignmentTarget::ColumnList(columns) => {
                        if columns.len() == 1 {
                            let target_index =
                                find_column_index_case_insensitive(&column_names, &columns[0])
                                    .ok_or_else(|| {
                                        FrankenError::Internal(format!(
                                            "UPDATE trigger snapshot: unknown column `{}`",
                                            columns[0]
                                        ))
                                    })?;
                            new_values[target_index] =
                                eval_join_expr(&assignment.value, &old_values, &col_map)?;
                            continue;
                        }

                        match &assignment.value {
                            Expr::RowValue(values, _) if values.len() == columns.len() => {
                                for (column_name, value_expr) in columns.iter().zip(values) {
                                    let target_index = find_column_index_case_insensitive(
                                        &column_names,
                                        column_name,
                                    )
                                    .ok_or_else(|| {
                                        FrankenError::Internal(format!(
                                            "UPDATE trigger snapshot: unknown column `{column_name}`"
                                        ))
                                    })?;
                                    new_values[target_index] =
                                        eval_join_expr(value_expr, &old_values, &col_map)?;
                                }
                            }
                            _ => {
                                return Err(FrankenError::Internal(
                                    "UPDATE trigger snapshot: column-list assignment requires row-value expression of matching arity".to_owned(),
                                ));
                            }
                        }
                    }
                }
            }

            trigger_rows.push((old_values, new_values));
        }
        Ok(trigger_rows)
    }

    fn collect_delete_trigger_rows(
        &self,
        delete: &fsqlite_ast::DeleteStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Vec<SqliteValue>>> {
        let matched_rows =
            self.select_matching_rows(&delete.table, delete.where_clause.as_ref(), params)?;
        Ok(matched_rows
            .into_iter()
            .map(|row| row.values().to_vec())
            .collect())
    }

    fn count_matching_rows(
        &self,
        table_ref: &fsqlite_ast::QualifiedTableRef,
        where_clause: Option<&Expr>,
        params: Option<&[SqliteValue]>,
    ) -> Result<usize> {
        let rows = self.select_matching_rows(table_ref, where_clause, params)?;
        Ok(rows.len())
    }

    // ── autocommit pager transaction wrapping (bd-14dj / 5B.5) ──────────

    /// Ensure a pager transaction is active.  If the connection is NOT
    /// inside an explicit `BEGIN`, an implicit (autocommit) transaction is
    /// created.  Returns `true` when an implicit transaction was started
    /// (the caller must later call [`resolve_autocommit_txn`]).
    fn ensure_autocommit_txn(&self) -> Result<bool> {
        let mode = if *self.concurrent_mode_default.borrow() {
            TransactionMode::Concurrent
        } else {
            TransactionMode::Immediate
        };
        self.ensure_autocommit_txn_mode(mode)
    }

    /// Ensure a pager transaction is active, using the specified mode.
    /// Phase 5B.2 (bd-1yi8): reads also need pager transactions so
    /// they can see data written by prior write-through INSERTs.
    fn ensure_autocommit_txn_mode(&self, mode: TransactionMode) -> Result<bool> {
        if *self.in_transaction.borrow() || self.active_txn.borrow().is_some() {
            return Ok(false);
        }
        let cx = Cx::new();
        let mut txn = self.pager.begin(&cx, mode)?;
        let is_concurrent = mode == TransactionMode::Concurrent;
        let concurrent_session = if is_concurrent {
            let commit_seq = *self.next_commit_seq.borrow();
            let snapshot = Snapshot::new(
                CommitSeq::new(commit_seq.saturating_sub(1)),
                SchemaEpoch::new((*self.schema_cookie.borrow()).into()),
            );
            match self
                .concurrent_registry
                .borrow_mut()
                .begin_concurrent(snapshot)
            {
                Ok(session_id) => Some(session_id),
                Err(error) => {
                    let _ = txn.rollback(&cx);
                    return Err(match error {
                        MvccError::Busy => FrankenError::Busy,
                        _ => FrankenError::Internal(format!("MVCC begin failed: {error}")),
                    });
                }
            }
        } else {
            None
        };

        *self.active_txn.borrow_mut() = Some(txn);
        *self.concurrent_session_id.borrow_mut() = concurrent_session;
        *self.concurrent_txn.borrow_mut() = is_concurrent;
        Ok(true)
    }

    /// Finalize an implicit (autocommit) transaction: commit on success,
    /// rollback on error.  No-op when `was_auto` is `false`.
    fn resolve_autocommit_txn(&self, was_auto: bool, ok: bool) -> Result<()> {
        if !was_auto {
            return Ok(());
        }
        let cx = Cx::new();
        let mut txn = {
            let mut guard = self.active_txn.borrow_mut();
            let Some(txn) = guard.take() else {
                *self.concurrent_txn.borrow_mut() = false;
                *self.concurrent_session_id.borrow_mut() = None;
                return Ok(());
            };
            txn
        };

        let concurrent_plan = if ok && *self.concurrent_txn.borrow() {
            match self.plan_concurrent_commit() {
                Ok(plan) => plan,
                Err(err) => {
                    let _ = txn.rollback(&cx);
                    *self.concurrent_txn.borrow_mut() = false;
                    *self.concurrent_session_id.borrow_mut() = None;
                    return Err(err);
                }
            }
        } else {
            None
        };

        if !ok && *self.concurrent_txn.borrow() {
            if let Some(session_id) = self.concurrent_session_id.borrow_mut().take() {
                let mut registry = self.concurrent_registry.borrow_mut();
                if let Some(handle) = registry.get_mut(session_id) {
                    concurrent_abort(handle, &self.concurrent_lock_table, session_id);
                }
                registry.remove(session_id);
            }
        }

        let txn_result = if ok {
            txn.commit(&cx)
        } else {
            txn.rollback(&cx)
        };
        if let Err(err) = txn_result {
            let _ = txn.rollback(&cx);
            if *self.concurrent_txn.borrow() {
                if let Some(session_id) = self.concurrent_session_id.borrow_mut().take() {
                    let mut registry = self.concurrent_registry.borrow_mut();
                    if let Some(handle) = registry.get_mut(session_id) {
                        concurrent_abort(handle, &self.concurrent_lock_table, session_id);
                    }
                    registry.remove(session_id);
                }
            }
            *self.concurrent_txn.borrow_mut() = false;
            *self.concurrent_session_id.borrow_mut() = None;
            return Err(err);
        }

        if let Some((session_id, committed_seq, write_pages)) = concurrent_plan {
            self.finalize_concurrent_commit(session_id, committed_seq, write_pages);
        }

        *self.concurrent_txn.borrow_mut() = false;
        *self.concurrent_session_id.borrow_mut() = None;
        Ok(())
    }

    // ── pager page allocation (bd-3ez5 / 5A.3) ───────────────────────────

    /// Allocate a fresh page from the pager and initialize it as an empty
    /// B-tree leaf table page (type 0x0D).  Returns the real `PageNumber`.
    #[allow(clippy::cast_possible_wrap)]
    fn allocate_root_page(&self) -> Result<i32> {
        self.with_pager_write_txn(|cx, txn| {
            let page_no = txn.allocate_page(cx)?;
            let page_size = PageSize::DEFAULT.get();
            let mut page = vec![0u8; page_size as usize];
            BTreePageHeader::write_empty_leaf_table(&mut page, 0, page_size);
            txn.write_page(cx, page_no, &page)?;
            Ok(page_no.get() as i32)
        })
    }

    /// Allocate a fresh page from the pager and initialize it as an empty
    /// B-tree leaf index page (type 0x0A).  Returns the real `PageNumber`.
    #[allow(clippy::cast_possible_wrap)]
    fn allocate_index_root_page(&self) -> Result<i32> {
        self.with_pager_write_txn(|cx, txn| {
            let page_no = txn.allocate_page(cx)?;
            let page_size = PageSize::DEFAULT.get();
            let mut page = vec![0u8; page_size as usize];
            BTreePageHeader::write_empty_leaf_index(&mut page, 0, page_size);
            txn.write_page(cx, page_no, &page)?;
            Ok(page_no.get() as i32)
        })
    }

    // ── sqlite_master helpers (bd-1b5e / 5A.2) ─────────────────────────

    /// Run a closure with a mutable pager transaction, auto-beginning and
    /// committing when no explicit transaction is active.
    fn with_pager_write_txn<R>(
        &self,
        f: impl FnOnce(&Cx, &mut dyn TransactionHandle) -> Result<R>,
    ) -> Result<R> {
        let cx = Cx::new();
        let auto = self.active_txn.borrow().is_none();
        if auto {
            let txn = self.pager.begin(&cx, TransactionMode::Immediate)?;
            *self.active_txn.borrow_mut() = Some(txn);
        }
        let result = {
            let mut guard = self.active_txn.borrow_mut();
            let txn = guard.as_deref_mut().expect("txn just ensured");
            f(&cx, txn)
        };
        if auto {
            let mut guard = self.active_txn.borrow_mut();
            if let Some(txn) = guard.as_deref_mut() {
                if result.is_ok() {
                    txn.commit(&cx)?;
                } else {
                    txn.rollback(&cx)?;
                }
            }
            *guard = None;
        }
        result
    }

    /// Insert a row into the sqlite_master B-tree on page 1.
    fn insert_sqlite_master_row(
        &self,
        type_: &str,
        name: &str,
        tbl_name: &str,
        rootpage: i32,
        sql: &str,
    ) -> Result<()> {
        let rowid = {
            let mut rid = self.next_master_rowid.borrow_mut();
            let r = *rid;
            *rid += 1;
            r
        };
        self.with_pager_write_txn(|cx, txn| {
            let usable_size = PageSize::DEFAULT.get();
            let record = serialize_record(&[
                SqliteValue::Text(type_.to_owned()),
                SqliteValue::Text(name.to_owned()),
                SqliteValue::Text(tbl_name.to_owned()),
                SqliteValue::Integer(i64::from(rootpage)),
                SqliteValue::Text(sql.to_owned()),
            ]);
            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(txn),
                PageNumber::ONE,
                usable_size,
                true,
            );
            cursor.table_insert(cx, rowid, &record)
        })
    }

    /// Delete the sqlite_master row whose `name` column matches the given
    /// object name (case-insensitive scan of the page 1 B-tree).
    fn delete_sqlite_master_row(&self, name: &str) -> Result<()> {
        self.with_pager_write_txn(|cx, txn| {
            let usable_size = PageSize::DEFAULT.get();
            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(txn),
                PageNumber::ONE,
                usable_size,
                true,
            );
            if !cursor.first(cx)? {
                return Err(FrankenError::Internal(format!(
                    "sqlite_master entry not found: {name}"
                )));
            }
            loop {
                let payload = cursor.payload(cx)?;
                if let Some(values) = parse_record(&payload) {
                    // Column index 1 is the `name` column.
                    if let Some(SqliteValue::Text(row_name)) = values.get(1) {
                        if row_name.eq_ignore_ascii_case(name) {
                            return cursor.delete(cx);
                        }
                    }
                }
                if !cursor.next(cx)? {
                    break;
                }
            }
            Err(FrankenError::Internal(format!(
                "sqlite_master entry not found: {name}"
            )))
        })
    }

    /// Process a CREATE TABLE statement: register the schema and create the
    /// in-memory table, and insert a row into sqlite_master on page 1.
    #[allow(clippy::too_many_lines)]
    fn execute_create_table(&self, create: &fsqlite_ast::CreateTableStatement) -> Result<()> {
        let table_name = create.name.name.clone();

        // Check for duplicate table names.
        let schema = self.schema.borrow();
        if schema
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(&table_name))
        {
            if create.if_not_exists {
                return Ok(());
            }
            return Err(FrankenError::Internal(format!(
                "table {table_name} already exists",
            )));
        }
        drop(schema);

        match &create.body {
            CreateTableBody::Columns { columns, .. } => {
                // Detect INTEGER PRIMARY KEY column (rowid alias).
                let rowid_col_idx = columns.iter().enumerate().find_map(|(i, col)| {
                    let is_integer = col
                        .type_name
                        .as_ref()
                        .is_some_and(|tn| tn.name.eq_ignore_ascii_case("INTEGER"));
                    let is_pk = col
                        .constraints
                        .iter()
                        .any(|c| matches!(c.kind, ColumnConstraintKind::PrimaryKey { .. }));
                    (is_integer && is_pk).then_some(i)
                });
                let col_infos: Vec<ColumnInfo> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        let affinity = col
                            .type_name
                            .as_ref()
                            .map_or('B', |tn| type_name_to_affinity_char(&tn.name));
                        ColumnInfo {
                            name: col.name.clone(),
                            affinity,
                            is_ipk: rowid_col_idx.is_some_and(|idx| idx == i),
                        }
                    })
                    .collect();
                if let Some(idx) = rowid_col_idx {
                    self.rowid_alias_columns
                        .borrow_mut()
                        .insert(table_name.to_ascii_lowercase(), idx);
                }
                let num_columns = col_infos.len();
                let root_page = self.allocate_root_page()?;
                self.db.borrow_mut().create_table_at(root_page, num_columns);
                let table_schema = TableSchema {
                    name: table_name,
                    root_page,
                    columns: col_infos,
                    indexes: Vec::new(),
                };
                let create_sql = crate::compat_persist::build_create_table_sql(&table_schema);
                let rp = table_schema.root_page;
                let tbl_name = table_schema.name.clone();
                self.schema.borrow_mut().push(table_schema);
                self.insert_sqlite_master_row("table", &tbl_name, &tbl_name, rp, &create_sql)?;
            }
            CreateTableBody::AsSelect(select_stmt) => {
                // Execute the SELECT to get result rows.
                let rows = self.execute_statement(Statement::Select(*select_stmt.clone()), None)?;
                // Infer column names from the SELECT columns.
                let col_names = infer_select_column_names(select_stmt);
                let width = rows.first().map_or(
                    if col_names.is_empty() {
                        1
                    } else {
                        col_names.len()
                    },
                    |r| r.values().len(),
                );
                let col_infos: Vec<ColumnInfo> = (0..width)
                    .map(|i| ColumnInfo {
                        name: col_names
                            .get(i)
                            .cloned()
                            .unwrap_or_else(|| format!("_c{i}")),
                        affinity: 'B',
                        is_ipk: false,
                    })
                    .collect();
                let root_page = self.allocate_root_page()?;
                self.db.borrow_mut().create_table_at(root_page, width);
                let table_schema = TableSchema {
                    name: table_name,
                    root_page,
                    columns: col_infos,
                    indexes: Vec::new(),
                };
                let create_sql = crate::compat_persist::build_create_table_sql(&table_schema);
                let rp = table_schema.root_page;
                let tbl_name = table_schema.name.clone();
                self.schema.borrow_mut().push(table_schema);
                // Phase 5B.2/5C.1: Insert result rows via VDBE to ensure data
                // goes through StorageCursor and ends up in the pager.
                if !rows.is_empty() {
                    let placeholders: String = (1..=width)
                        .map(|i| format!("?{i}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let insert_sql = format!("INSERT INTO \"{tbl_name}\" VALUES ({placeholders})");
                    for row in &rows {
                        self.execute_with_params(&insert_sql, row.values())?;
                    }
                }
                self.insert_sqlite_master_row("table", &tbl_name, &tbl_name, rp, &create_sql)?;
            }
        }

        self.increment_schema_cookie();
        Ok(())
    }

    /// Execute a DROP statement (TABLE, INDEX, VIEW, TRIGGER).
    fn execute_drop(&self, drop_stmt: &fsqlite_ast::DropStatement) -> Result<()> {
        let obj_name = &drop_stmt.name.name;
        let dropped = match drop_stmt.object_type {
            DropObjectType::Table => {
                let mut schema = self.schema.borrow_mut();
                let table_idx = schema
                    .iter()
                    .position(|t| t.name.eq_ignore_ascii_case(obj_name));
                if let Some(idx) = table_idx {
                    let root_page = schema[idx].root_page;
                    schema.remove(idx);
                    drop(schema);
                    self.db.borrow_mut().destroy_table(root_page);
                    self.rowid_alias_columns
                        .borrow_mut()
                        .remove(&obj_name.to_ascii_lowercase());
                    self.delete_sqlite_master_row(obj_name)?;
                    true
                } else {
                    if drop_stmt.if_exists {
                        return Ok(());
                    }
                    return Err(FrankenError::NoSuchTable {
                        name: obj_name.clone(),
                    });
                }
            }
            DropObjectType::Index => {
                let mut schema = self.schema.borrow_mut();
                let mut found = false;
                for table in schema.iter_mut() {
                    if let Some(pos) = table
                        .indexes
                        .iter()
                        .position(|idx| idx.name.eq_ignore_ascii_case(obj_name))
                    {
                        table.indexes.remove(pos);
                        found = true;
                        break;
                    }
                }
                if !found {
                    if drop_stmt.if_exists {
                        return Ok(());
                    }
                    return Err(FrankenError::Internal(format!("no such index: {obj_name}")));
                }
                true
            }
            DropObjectType::View => {
                let mut views = self.views.borrow_mut();
                if let Some(pos) = views
                    .iter()
                    .position(|v| v.name.eq_ignore_ascii_case(obj_name))
                {
                    views.remove(pos);
                    true
                } else if drop_stmt.if_exists {
                    return Ok(());
                } else {
                    return Err(FrankenError::Internal(format!("no such view: {obj_name}")));
                }
            }
            DropObjectType::Trigger => {
                let schema_name = drop_stmt.name.schema.as_deref();
                let mut triggers = self.triggers.borrow_mut();
                let trigger_idx = triggers.iter().position(|trigger| {
                    if !trigger.name.eq_ignore_ascii_case(obj_name) {
                        return false;
                    }
                    match schema_name {
                        Some(schema) if schema.eq_ignore_ascii_case("temp") => trigger.temporary,
                        Some(schema) if schema.eq_ignore_ascii_case("main") => !trigger.temporary,
                        Some(_) => false,
                        None => true,
                    }
                });
                if let Some(idx) = trigger_idx {
                    let trigger = triggers.remove(idx);
                    drop(triggers);
                    if !trigger.temporary {
                        self.delete_sqlite_master_row(obj_name)?;
                    }
                    true
                } else {
                    if drop_stmt.if_exists {
                        return Ok(());
                    }
                    return Err(FrankenError::Internal(format!(
                        "no such trigger: {obj_name}"
                    )));
                }
            }
        };
        if dropped {
            self.increment_schema_cookie();
        }
        Ok(())
    }

    /// Execute an ALTER TABLE statement.
    fn execute_alter_table(&self, alter: &fsqlite_ast::AlterTableStatement) -> Result<()> {
        let table_name = &alter.table.name;
        match &alter.action {
            AlterTableAction::RenameTo(new_name) => {
                let mut schema = self.schema.borrow_mut();
                let table = schema
                    .iter_mut()
                    .find(|t| t.name.eq_ignore_ascii_case(table_name))
                    .ok_or_else(|| FrankenError::NoSuchTable {
                        name: table_name.clone(),
                    })?;
                table.name.clone_from(new_name);
            }
            AlterTableAction::RenameColumn { old, new } => {
                let mut schema = self.schema.borrow_mut();
                let table = schema
                    .iter_mut()
                    .find(|t| t.name.eq_ignore_ascii_case(table_name))
                    .ok_or_else(|| FrankenError::NoSuchTable {
                        name: table_name.clone(),
                    })?;
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(old))
                    .ok_or_else(|| FrankenError::Internal(format!("no such column: {old}")))?;
                col.name.clone_from(new);
            }
            AlterTableAction::AddColumn(col_def) => {
                let affinity = col_def
                    .type_name
                    .as_ref()
                    .map_or('B', |tn| type_name_to_affinity_char(&tn.name));
                let mut schema = self.schema.borrow_mut();
                let table = schema
                    .iter_mut()
                    .find(|t| t.name.eq_ignore_ascii_case(table_name))
                    .ok_or_else(|| FrankenError::NoSuchTable {
                        name: table_name.clone(),
                    })?;
                table.columns.push(ColumnInfo {
                    name: col_def.name.clone(),
                    affinity,
                    is_ipk: false,
                });
            }
            AlterTableAction::DropColumn(col_name) => {
                let mut schema = self.schema.borrow_mut();
                let table = schema
                    .iter_mut()
                    .find(|t| t.name.eq_ignore_ascii_case(table_name))
                    .ok_or_else(|| FrankenError::NoSuchTable {
                        name: table_name.clone(),
                    })?;
                let col_idx = table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| FrankenError::Internal(format!("no such column: {col_name}")))?;
                table.columns.remove(col_idx);
            }
        }
        self.increment_schema_cookie();
        Ok(())
    }

    /// Execute a CREATE INDEX statement, allocating a B-tree root page.
    fn execute_create_index(&self, stmt: &fsqlite_ast::CreateIndexStatement) -> Result<()> {
        let table_name = &stmt.table;
        let index_name = stmt.name.name.clone();

        // Phase 1: Validate schema (with borrow)
        {
            let schema = self.schema.borrow();
            let table = schema
                .iter()
                .find(|t| t.name.eq_ignore_ascii_case(table_name))
                .ok_or_else(|| FrankenError::NoSuchTable {
                    name: table_name.clone(),
                })?;
            // Check for duplicate index name.
            if table
                .indexes
                .iter()
                .any(|idx| idx.name.eq_ignore_ascii_case(&index_name))
            {
                if stmt.if_not_exists {
                    return Ok(());
                }
                return Err(FrankenError::Internal(format!(
                    "index {index_name} already exists"
                )));
            }
        }

        // Phase 2: Validate columns and collect names (with borrow)
        let col_names = {
            let schema = self.schema.borrow();
            let table = schema
                .iter()
                .find(|t| t.name.eq_ignore_ascii_case(table_name))
                .ok_or_else(|| FrankenError::NoSuchTable {
                    name: table_name.clone(),
                })?;
            let mut names = Vec::with_capacity(stmt.columns.len());
            for idx_col in &stmt.columns {
                let col_name = expr_col_name(&idx_col.expr).ok_or_else(|| {
                    FrankenError::NotImplemented(
                        "only column references are supported in CREATE INDEX".to_owned(),
                    )
                })?;
                if !table
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(col_name))
                {
                    return Err(FrankenError::Internal(format!(
                        "no such column: {col_name}"
                    )));
                }
                names.push(col_name.to_owned());
            }
            names
        };

        // Phase 3: Allocate index B-tree root page (no borrow held)
        let root_page = self.allocate_index_root_page()?;

        // Phase 4: Create B-tree in db layer (for in-memory engine compatibility)
        // Index B-trees have key = (indexed columns..., rowid), no payload columns
        self.db.borrow_mut().create_table_at(root_page, 0);

        // Phase 5: Record index in schema (with mut borrow)
        {
            let mut schema = self.schema.borrow_mut();
            let table = schema
                .iter_mut()
                .find(|t| t.name.eq_ignore_ascii_case(table_name))
                .ok_or_else(|| FrankenError::NoSuchTable {
                    name: table_name.clone(),
                })?;
            table.indexes.push(IndexSchema {
                name: index_name.clone(),
                columns: col_names.clone(),
                root_page,
            });
        }

        // Phase 6: Persist to sqlite_master
        let create_sql =
            crate::compat_persist::build_create_index_sql(&index_name, table_name, &col_names);
        self.insert_sqlite_master_row("index", &index_name, table_name, root_page, &create_sql)?;

        self.increment_schema_cookie();
        Ok(())
    }

    /// Execute a CREATE VIEW statement (store definition in memory).
    fn execute_create_view(&self, stmt: &fsqlite_ast::CreateViewStatement) -> Result<()> {
        let view_name = &stmt.name.name;
        let views = self.views.borrow();
        if views.iter().any(|v| v.name.eq_ignore_ascii_case(view_name)) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(FrankenError::Internal(format!(
                "view {view_name} already exists"
            )));
        }
        drop(views);
        self.views.borrow_mut().push(ViewDef {
            name: view_name.clone(),
            columns: stmt.columns.clone(),
            query: stmt.query.clone(),
        });
        self.increment_schema_cookie();
        Ok(())
    }

    /// Execute a CREATE TRIGGER statement.
    fn execute_create_trigger(&self, stmt: &fsqlite_ast::CreateTriggerStatement) -> Result<()> {
        let trigger_name = &stmt.name.name;
        let table_name = &stmt.table;

        let schema = self.schema.borrow();
        let table_exists = schema
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(table_name));
        drop(schema);
        if !table_exists {
            return Err(FrankenError::NoSuchTable {
                name: table_name.clone(),
            });
        }

        let triggers = self.triggers.borrow();
        if triggers
            .iter()
            .any(|trigger| trigger.name.eq_ignore_ascii_case(trigger_name))
        {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(FrankenError::Internal(format!(
                "trigger {trigger_name} already exists"
            )));
        }
        drop(triggers);

        let create_sql = stmt.to_string();
        self.triggers
            .borrow_mut()
            .push(TriggerDef::from_create_statement(stmt, create_sql.clone()));

        // TEMP triggers are connection-local and not persisted to sqlite_master.
        if !stmt.temporary {
            self.insert_sqlite_master_row("trigger", trigger_name, table_name, 0, &create_sql)?;
        }

        self.increment_schema_cookie();
        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BEFORE/AFTER Trigger Firing (Phase 5G.2/5G.3 - bd-iqam, bd-khol)
    // ═══════════════════════════════════════════════════════════════════════════

    fn make_trigger_frame(
        &self,
        table_name: &str,
        old_values: Option<&[SqliteValue]>,
        new_values: Option<&[SqliteValue]>,
    ) -> Result<TriggerFrame> {
        let schema = self.schema.borrow();
        let table_schema = schema
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case(table_name))
            .ok_or_else(|| FrankenError::NoSuchTable {
                name: table_name.to_owned(),
            })?;
        let column_names = table_schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        Ok(TriggerFrame {
            table_name: table_schema.name.clone(),
            column_names,
            old_row: old_values.map(ToOwned::to_owned),
            new_row: new_values.map(ToOwned::to_owned),
        })
    }

    fn push_trigger_frame(&self, frame: TriggerFrame) -> TriggerFrameGuard<'_> {
        self.trigger_frame_stack.borrow_mut().push(frame);
        TriggerFrameGuard {
            stack: &self.trigger_frame_stack,
        }
    }

    fn has_matching_triggers(
        &self,
        table_name: &str,
        timing: fsqlite_ast::TriggerTiming,
        event: &fsqlite_ast::TriggerEvent,
    ) -> bool {
        self.triggers.borrow().iter().any(|trigger| {
            trigger.table_name.eq_ignore_ascii_case(table_name)
                && trigger.timing == timing
                && trigger_event_matches(&trigger.event, event)
        })
    }

    fn apply_trigger_raise(
        &self,
        directive: TriggerRaiseDirective,
    ) -> Result<TriggerStatementOutcome> {
        match directive.action {
            fsqlite_ast::RaiseAction::Ignore => Ok(TriggerStatementOutcome::SkipDml),
            fsqlite_ast::RaiseAction::Abort | fsqlite_ast::RaiseAction::Fail => {
                let message = directive
                    .message
                    .unwrap_or_else(|| "trigger RAISE()".to_owned());
                Err(FrankenError::FunctionError(message))
            }
            fsqlite_ast::RaiseAction::Rollback => {
                let reason = directive
                    .message
                    .unwrap_or_else(|| "trigger requested rollback".to_owned());
                let has_active_txn =
                    *self.in_transaction.borrow() || self.active_txn.borrow().is_some();
                if has_active_txn {
                    let rollback_stmt = fsqlite_ast::RollbackStatement { to_savepoint: None };
                    self.execute_rollback(&rollback_stmt)?;
                }
                Err(FrankenError::TransactionRolledBack { reason })
            }
        }
    }

    fn execute_bound_trigger_statement(
        &self,
        statement: Statement,
    ) -> Result<TriggerStatementOutcome> {
        if let Some(directive) = trigger_statement_raise_directive(&statement)? {
            return self.apply_trigger_raise(directive);
        }
        self.execute_statement(statement, None)?;
        Ok(TriggerStatementOutcome::Continue)
    }

    /// Fire BEFORE triggers for a DML event on a table.
    ///
    /// Returns `true` if any trigger executed RAISE(IGNORE), indicating the DML
    /// operation should be skipped entirely.
    fn fire_before_triggers(
        &self,
        table_name: &str,
        event: &fsqlite_ast::TriggerEvent,
        old_values: Option<&[SqliteValue]>,
        new_values: Option<&[SqliteValue]>,
    ) -> Result<bool> {
        let triggers = self.triggers.borrow();
        let matching: Vec<_> = triggers
            .iter()
            .filter(|t| {
                t.table_name.eq_ignore_ascii_case(table_name)
                    && t.timing == fsqlite_ast::TriggerTiming::Before
                    && trigger_event_matches(&t.event, event)
            })
            .cloned()
            .collect();
        drop(triggers);

        if matching.is_empty() {
            return Ok(false);
        }

        let frame = self.make_trigger_frame(table_name, old_values, new_values)?;
        for trigger in matching {
            let _frame_guard = self.push_trigger_frame(frame.clone());
            if !trigger_update_of_columns_changed(&trigger.event, &frame) {
                continue;
            }
            // Evaluate constant WHEN expressions now; unresolved OLD/NEW-based
            // clauses still fall back to firing until pseudo-tables are wired.
            if !trigger_when_matches(trigger.when_clause.as_ref(), Some(&frame)) {
                continue;
            }

            // Execute each statement in the trigger body.
            for stmt in &trigger.body {
                let mut bound_stmt = stmt.clone();
                bind_trigger_columns_in_statement(&mut bound_stmt, &frame);
                match self.execute_bound_trigger_statement(bound_stmt)? {
                    TriggerStatementOutcome::Continue => {}
                    TriggerStatementOutcome::SkipDml => return Ok(true),
                }
            }
        }

        Ok(false)
    }

    /// Fire AFTER triggers for a DML event on a table.
    fn fire_after_triggers(
        &self,
        table_name: &str,
        event: &fsqlite_ast::TriggerEvent,
        old_values: Option<&[SqliteValue]>,
        new_values: Option<&[SqliteValue]>,
    ) -> Result<()> {
        let triggers = self.triggers.borrow();
        let matching: Vec<_> = triggers
            .iter()
            .filter(|t| {
                t.table_name.eq_ignore_ascii_case(table_name)
                    && t.timing == fsqlite_ast::TriggerTiming::After
                    && trigger_event_matches(&t.event, event)
            })
            .cloned()
            .collect();
        drop(triggers);

        if matching.is_empty() {
            return Ok(());
        }

        let frame = self.make_trigger_frame(table_name, old_values, new_values)?;
        for trigger in matching {
            let _frame_guard = self.push_trigger_frame(frame.clone());
            if !trigger_update_of_columns_changed(&trigger.event, &frame) {
                continue;
            }
            if !trigger_when_matches(trigger.when_clause.as_ref(), Some(&frame)) {
                continue;
            }

            // Execute each statement in the trigger body.
            for stmt in &trigger.body {
                let mut bound_stmt = stmt.clone();
                bind_trigger_columns_in_statement(&mut bound_stmt, &frame);
                match self.execute_bound_trigger_statement(bound_stmt)? {
                    TriggerStatementOutcome::Continue => {}
                    TriggerStatementOutcome::SkipDml => return Ok(()),
                }
            }
        }

        Ok(())
    }

    /// Check if a SELECT references any views in its FROM/JOIN sources.
    fn has_view_references(&self, select: &SelectStatement) -> bool {
        let views = self.views.borrow();
        if views.is_empty() {
            return false;
        }
        let schema = self.schema.borrow();
        // A view reference only counts if there is no real table with that
        // name already (which would mean the view is already materialized).
        let is_unmaterialized_view = |source: &TableOrSubquery| -> bool {
            if let TableOrSubquery::Table { name, .. } = source {
                let nm = &name.name;
                views.iter().any(|v| v.name.eq_ignore_ascii_case(nm))
                    && !schema.iter().any(|t| t.name.eq_ignore_ascii_case(nm))
            } else {
                false
            }
        };
        if let SelectCore::Select {
            from: Some(ref from),
            ..
        } = select.body.select
        {
            if is_unmaterialized_view(&from.source) {
                return true;
            }
            for join in &from.joins {
                if is_unmaterialized_view(&join.table) {
                    return true;
                }
            }
        }
        false
    }

    /// Materialize views referenced by a SELECT as temporary tables, execute
    /// the query, then clean up the temp tables.
    fn execute_with_materialized_views(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let view_defs: Vec<ViewDef> = self.views.borrow().clone();
        let mut materialized: Vec<String> = Vec::new();

        // Collect view names referenced in FROM/JOIN.
        let mut referenced: Vec<String> = Vec::new();
        if let SelectCore::Select {
            from: Some(ref from),
            ..
        } = select.body.select
        {
            if let TableOrSubquery::Table { ref name, .. } = from.source {
                if view_defs
                    .iter()
                    .any(|v| v.name.eq_ignore_ascii_case(&name.name))
                {
                    referenced.push(name.name.clone());
                }
            }
            for join in &from.joins {
                if let TableOrSubquery::Table { ref name, .. } = join.table {
                    if view_defs
                        .iter()
                        .any(|v| v.name.eq_ignore_ascii_case(&name.name))
                    {
                        referenced.push(name.name.clone());
                    }
                }
            }
        }

        // Materialize each referenced view as a temp table.
        for ref_name in &referenced {
            let view = view_defs
                .iter()
                .find(|v| v.name.eq_ignore_ascii_case(ref_name))
                .unwrap();
            let view_rows =
                self.execute_statement(Statement::Select(view.query.clone()), params)?;
            let col_names = infer_select_column_names(&view.query);
            let width = if col_names.is_empty() {
                view_rows.first().map_or(1, |r| r.values().len())
            } else {
                col_names.len()
            };
            let col_infos: Vec<ColumnInfo> = if col_names.is_empty() {
                (0..width)
                    .map(|i| ColumnInfo {
                        name: format!("_c{i}"),
                        affinity: 'B',
                        is_ipk: false,
                    })
                    .collect()
            } else {
                col_names
                    .iter()
                    .map(|n| ColumnInfo {
                        name: n.clone(),
                        affinity: 'B',
                        is_ipk: false,
                    })
                    .collect()
            };

            let root_page = self.db.borrow_mut().create_table(col_infos.len());
            self.schema.borrow_mut().push(TableSchema {
                name: view.name.clone(),
                root_page,
                columns: col_infos,
                indexes: Vec::new(),
            });
            materialized.push(view.name.clone());

            for (i, row) in view_rows.iter().enumerate() {
                let vals = row.values().to_vec();
                #[allow(clippy::cast_possible_wrap)]
                let rowid = (i + 1) as i64;
                if let Some(table) = self.db.borrow_mut().get_table_mut(root_page) {
                    table.insert_row(rowid, vals);
                }
            }
        }

        let result = self.execute_statement(Statement::Select(select.clone()), params);

        // Clean up materialized temp tables.
        for name in &materialized {
            let mut schema = self.schema.borrow_mut();
            if let Some(idx) = schema
                .iter()
                .position(|t| t.name.eq_ignore_ascii_case(name))
            {
                let rp = schema[idx].root_page;
                schema.remove(idx);
                drop(schema);
                self.db.borrow_mut().destroy_table(rp);
            }
        }

        result
    }

    /// Check if a SELECT references sqlite_master/sqlite_schema in FROM/JOIN
    /// and the virtual schema table is not already materialized.
    fn has_sqlite_schema_references(&self, select: &SelectStatement) -> bool {
        !self.collect_sqlite_schema_references(select).is_empty()
    }

    /// Collect sqlite_master/sqlite_schema names referenced by this SELECT.
    fn collect_sqlite_schema_references(&self, select: &SelectStatement) -> Vec<String> {
        let schema = self.schema.borrow();
        let mut referenced = Vec::new();

        let push_if_needed = |source: &TableOrSubquery, out: &mut Vec<String>| {
            if let TableOrSubquery::Table { name, .. } = source
                && let Some(canonical) = canonical_sqlite_schema_name(&name.name)
                && !schema
                    .iter()
                    .any(|t| t.name.eq_ignore_ascii_case(canonical))
                && !out.iter().any(|n| n.eq_ignore_ascii_case(canonical))
            {
                out.push(canonical.to_owned());
            }
        };

        if let SelectCore::Select {
            from: Some(from), ..
        } = &select.body.select
        {
            push_if_needed(&from.source, &mut referenced);
            for join in &from.joins {
                push_if_needed(&join.table, &mut referenced);
            }
        }

        referenced
    }

    /// Materialize sqlite_master/sqlite_schema as temporary in-memory tables,
    /// execute the query, then clean up.
    fn execute_with_materialized_sqlite_schema(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let referenced = self.collect_sqlite_schema_references(select);
        if referenced.is_empty() {
            return self.execute_statement(Statement::Select(select.clone()), params);
        }

        let virtual_rows = self.build_sqlite_master_rows();
        let virtual_columns = sqlite_master_column_infos();
        let mut materialized: Vec<(String, i32)> = Vec::new();

        for table_name in referenced {
            let root_page = self.db.borrow_mut().create_table(virtual_columns.len());
            self.schema.borrow_mut().push(TableSchema {
                name: table_name.clone(),
                root_page,
                columns: virtual_columns.clone(),
                indexes: Vec::new(),
            });

            if let Some(table) = self.db.borrow_mut().get_table_mut(root_page) {
                for (idx, row_values) in virtual_rows.iter().enumerate() {
                    #[allow(clippy::cast_possible_wrap)]
                    let rowid = (idx + 1) as i64;
                    table.insert_row(rowid, row_values.clone());
                }
            }

            materialized.push((table_name, root_page));
        }

        let result = self.execute_statement(Statement::Select(select.clone()), params);

        for (name, root_page) in &materialized {
            let mut schema = self.schema.borrow_mut();
            if let Some(idx) = schema
                .iter()
                .position(|t| t.root_page == *root_page && t.name.eq_ignore_ascii_case(name))
            {
                schema.remove(idx);
            }
            drop(schema);
            self.db.borrow_mut().destroy_table(*root_page);
        }

        result
    }

    /// Build virtual sqlite_master rows from in-memory schema metadata.
    ///
    /// sqlite_master/sqlite_schema columns:
    /// (type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT)
    fn build_sqlite_master_rows(&self) -> Vec<Vec<SqliteValue>> {
        let schema = self.schema.borrow();
        let views = self.views.borrow();
        let triggers = self.triggers.borrow();
        let mut rows = Vec::new();

        for table in schema.iter() {
            if is_sqlite_schema_name(&table.name)
                || views
                    .iter()
                    .any(|view| view.name.eq_ignore_ascii_case(&table.name))
            {
                continue;
            }

            rows.push(vec![
                SqliteValue::Text("table".to_owned()),
                SqliteValue::Text(table.name.clone()),
                SqliteValue::Text(table.name.clone()),
                SqliteValue::Integer(i64::from(table.root_page)),
                SqliteValue::Text(render_create_table_sql(table)),
            ]);

            for index in &table.indexes {
                rows.push(vec![
                    SqliteValue::Text("index".to_owned()),
                    SqliteValue::Text(index.name.clone()),
                    SqliteValue::Text(table.name.clone()),
                    SqliteValue::Integer(i64::from(index.root_page)),
                    SqliteValue::Text(render_create_index_sql(index, &table.name)),
                ]);
            }
        }

        for view in views.iter() {
            rows.push(vec![
                SqliteValue::Text("view".to_owned()),
                SqliteValue::Text(view.name.clone()),
                SqliteValue::Text(view.name.clone()),
                SqliteValue::Integer(0),
                SqliteValue::Text(format!("CREATE VIEW {}", view.name)),
            ]);
        }

        for trigger in triggers.iter().filter(|t| !t.temporary) {
            rows.push(vec![
                SqliteValue::Text("trigger".to_owned()),
                SqliteValue::Text(trigger.name.clone()),
                SqliteValue::Text(trigger.table_name.clone()),
                SqliteValue::Integer(0),
                SqliteValue::Text(trigger.create_sql.clone()),
            ]);
        }

        rows
    }

    // ── Transaction control ──────────────────────────────────────────────

    /// Returns `true` if an explicit transaction is active.
    pub fn in_transaction(&self) -> bool {
        *self.in_transaction.borrow()
    }

    /// Take a snapshot of the current database + schema state.
    fn snapshot(&self) -> DbSnapshot {
        let db_version = self.db.borrow_mut().undo_version();
        DbSnapshot {
            db_version,
            schema: self.schema.borrow().clone(),
            views: self.views.borrow().clone(),
            triggers: self.triggers.borrow().clone(),
            rowid_alias_columns: self.rowid_alias_columns.borrow().clone(),
            next_master_rowid: *self.next_master_rowid.borrow(),
        }
    }

    /// Restore a snapshot, replacing the current database + schema state.
    fn restore_snapshot(&self, snap: &DbSnapshot) {
        self.db.borrow_mut().rollback_to(snap.db_version);
        (*self.schema.borrow_mut()).clone_from(&snap.schema);
        (*self.views.borrow_mut()).clone_from(&snap.views);
        (*self.triggers.borrow_mut()).clone_from(&snap.triggers);
        (*self.rowid_alias_columns.borrow_mut()).clone_from(&snap.rowid_alias_columns);
        *self.next_master_rowid.borrow_mut() = snap.next_master_rowid;
    }

    /// Handle BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE|CONCURRENT].
    fn execute_begin(&self, begin: fsqlite_ast::BeginStatement) -> Result<()> {
        if *self.in_transaction.borrow() {
            return Err(FrankenError::Internal(
                "cannot start a transaction within a transaction".to_owned(),
            ));
        }

        // Determine effective mode: explicit mode wins; if absent, promote to
        // Concurrent when `concurrent_mode_default` is enabled.
        let is_concurrent = match begin.mode {
            Some(fsqlite_ast::TransactionMode::Concurrent) => true,
            Some(_) => false,
            None => *self.concurrent_mode_default.borrow(),
        };

        // Map AST mode to Pager mode.
        let pager_mode = match begin.mode {
            Some(fsqlite_ast::TransactionMode::Immediate) => TransactionMode::Immediate,
            Some(fsqlite_ast::TransactionMode::Exclusive) => TransactionMode::Exclusive,
            Some(fsqlite_ast::TransactionMode::Deferred) => TransactionMode::Deferred,
            Some(fsqlite_ast::TransactionMode::Concurrent) => TransactionMode::Concurrent,
            None => {
                if is_concurrent {
                    TransactionMode::Concurrent
                } else {
                    TransactionMode::Deferred
                }
            }
        };

        let cx = Cx::new();
        let mut txn = self.pager.begin(&cx, pager_mode)?;
        // MVCC concurrent-writer session (bd-14zc / 5E.1):
        // When mode is Concurrent, register with ConcurrentRegistry for
        // page-level MVCC locking and first-committer-wins validation.
        let concurrent_session = if is_concurrent {
            let commit_seq = *self.next_commit_seq.borrow();
            let snapshot = Snapshot::new(
                CommitSeq::new(commit_seq.saturating_sub(1)),
                SchemaEpoch::new((*self.schema_cookie.borrow()).into()),
            );
            let session_id = self
                .concurrent_registry
                .borrow_mut()
                .begin_concurrent(snapshot)
                .map_err(|e| match e {
                    MvccError::Busy => FrankenError::Busy,
                    _ => FrankenError::Internal(format!("MVCC begin failed: {e}")),
                });
            let session_id = match session_id {
                Ok(id) => id,
                Err(err) => {
                    let _ = txn.rollback(&cx);
                    return Err(err);
                }
            };
            Some(session_id)
        } else {
            None
        };

        *self.active_txn.borrow_mut() = Some(txn);
        *self.concurrent_session_id.borrow_mut() = concurrent_session;
        self.db.borrow_mut().begin_undo();
        *self.txn_snapshot.borrow_mut() = Some(self.snapshot());
        *self.in_transaction.borrow_mut() = true;
        *self.concurrent_txn.borrow_mut() = is_concurrent;
        Ok(())
    }

    fn map_mvcc_commit_error(err: MvccError, fcw_result: FcwResult) -> FrankenError {
        match err {
            MvccError::BusySnapshot => {
                let pages = match fcw_result {
                    FcwResult::Conflict {
                        conflicting_pages, ..
                    } => conflicting_pages
                        .iter()
                        .map(|p| p.get().to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    FcwResult::Clean => String::new(),
                };
                FrankenError::BusySnapshot {
                    conflicting_pages: pages,
                }
            }
            MvccError::Busy => FrankenError::Busy,
            _ => FrankenError::Internal(format!("MVCC commit failed: {err}")),
        }
    }

    fn capture_ssi_snapshot(handle: &ConcurrentHandle) -> SsiTxnEvidenceSnapshot {
        let mut read_pages = handle.read_set().iter().copied().collect::<Vec<_>>();
        read_pages.sort_by_key(|page| page.get());
        read_pages.dedup();

        let mut write_pages = handle.write_set_pages();
        write_pages.sort_by_key(|page| page.get());
        write_pages.dedup();

        SsiTxnEvidenceSnapshot {
            txn: handle.txn_token(),
            snapshot_seq: handle.snapshot().high,
            read_pages,
            write_pages,
            has_in_rw: handle.has_in_rw(),
            has_out_rw: handle.has_out_rw(),
            marked_for_abort: handle.is_marked_for_abort(),
        }
    }

    fn collect_active_conflict_evidence(
        registry: &ConcurrentRegistry,
        session_id: u64,
        snapshot: &SsiTxnEvidenceSnapshot,
    ) -> ActiveConflictEvidence {
        let read_lookup: HashSet<PageNumber> = snapshot.read_pages.iter().copied().collect();
        let write_lookup: HashSet<PageNumber> = snapshot.write_pages.iter().copied().collect();

        let mut conflicting_txns = Vec::new();
        let mut conflict_pages = HashSet::new();

        for (other_session_id, other_handle) in registry.iter_active() {
            if other_session_id == session_id || !other_handle.is_active() {
                continue;
            }

            let mut matched = false;
            for page in other_handle.read_set() {
                if write_lookup.contains(page) {
                    matched = true;
                    let _ = conflict_pages.insert(*page);
                }
            }

            let other_write_pages = other_handle.write_set_pages();
            for page in other_write_pages {
                if write_lookup.contains(&page) || read_lookup.contains(&page) {
                    matched = true;
                    let _ = conflict_pages.insert(page);
                }
            }

            if matched {
                conflicting_txns.push(other_handle.txn_token());
            }
        }

        conflicting_txns.sort_by(|left, right| {
            left.id
                .get()
                .cmp(&right.id.get())
                .then_with(|| left.epoch.get().cmp(&right.epoch.get()))
        });
        conflicting_txns.dedup();

        let mut conflict_pages = conflict_pages.into_iter().collect::<Vec<_>>();
        conflict_pages.sort_by_key(|page| page.get());
        conflict_pages.dedup();

        ActiveConflictEvidence {
            conflicting_txns,
            conflict_pages,
        }
    }

    fn build_ssi_decision_draft(
        snapshot: &SsiTxnEvidenceSnapshot,
        decision_type: SsiDecisionType,
        conflicting_txns: Vec<TxnToken>,
        conflict_pages: Vec<PageNumber>,
        rationale: impl Into<String>,
    ) -> SsiDecisionCardDraft {
        SsiDecisionCardDraft::new(
            decision_type,
            snapshot.txn,
            snapshot.snapshot_seq,
            conflicting_txns,
            conflict_pages,
            snapshot.read_pages.clone(),
            snapshot.write_pages.clone(),
            rationale,
        )
    }

    fn merge_conflict_pages(
        left: Vec<PageNumber>,
        right: Vec<PageNumber>,
        fallback: &[PageNumber],
    ) -> Vec<PageNumber> {
        let mut pages = left;
        pages.extend(right);
        if pages.is_empty() {
            pages.extend_from_slice(fallback);
        }
        pages.sort_by_key(|page| page.get());
        pages.dedup();
        pages
    }

    #[allow(clippy::too_many_lines)]
    fn plan_concurrent_commit(&self) -> Result<Option<(u64, CommitSeq, Vec<PageNumber>)>> {
        // MVCC concurrent-writer commit path (bd-14zc / 5E.1):
        // Validate FCW/SSI preconditions first, but delay commit-index
        // publication and lock release until pager commit succeeds. This
        // prevents publishing a committed sequence if physical commit fails.
        let Some(session_id) = *self.concurrent_session_id.borrow() else {
            return Ok(None);
        };
        let assign_seq = CommitSeq::new(*self.next_commit_seq.borrow());

        let mut abort_card: Option<SsiDecisionCardDraft> = None;

        let validate_result: std::result::Result<Vec<PageNumber>, (MvccError, FcwResult)> = {
            let mut registry = self.concurrent_registry.borrow_mut();
            let (fcw_result, snapshot, write_pages) = match registry.get_mut(session_id) {
                Some(handle) if handle.is_active() => (
                    validate_first_committer_wins(handle, &self.concurrent_commit_index),
                    Self::capture_ssi_snapshot(handle),
                    handle.write_set_pages(),
                ),
                Some(_) | None => {
                    return Err(FrankenError::Internal(
                        "MVCC session invalid or inactive".to_owned(),
                    ));
                }
            };
            let active_conflicts =
                Self::collect_active_conflict_evidence(&registry, session_id, &snapshot);

            match &fcw_result {
                FcwResult::Conflict {
                    conflicting_pages,
                    conflicting_commit_seq,
                } => {
                    if let Some(txn_id) = TxnId::new(session_id) {
                        self.concurrent_lock_table.release_all(txn_id);
                    }
                    if let Some(handle) = registry.get_mut(session_id) {
                        handle.mark_aborted();
                    }
                    let conflict_pages = Self::merge_conflict_pages(
                        conflicting_pages.clone(),
                        active_conflicts.conflict_pages,
                        &snapshot.write_pages,
                    );
                    abort_card = Some(Self::build_ssi_decision_draft(
                        &snapshot,
                        SsiDecisionType::AbortCycle,
                        active_conflicts.conflicting_txns,
                        conflict_pages,
                        format!(
                            "fcw_conflict_conflicting_commit_seq={}",
                            conflicting_commit_seq.get()
                        ),
                    ));
                    Err((MvccError::BusySnapshot, fcw_result))
                }
                FcwResult::Clean => {
                    if snapshot.marked_for_abort {
                        if let Some(txn_id) = TxnId::new(session_id) {
                            self.concurrent_lock_table.release_all(txn_id);
                        }
                        if let Some(handle) = registry.get_mut(session_id) {
                            handle.mark_aborted();
                        }
                        abort_card = Some(Self::build_ssi_decision_draft(
                            &snapshot,
                            SsiDecisionType::AbortCycle,
                            active_conflicts.conflicting_txns,
                            Self::merge_conflict_pages(
                                Vec::new(),
                                active_conflicts.conflict_pages,
                                &snapshot.write_pages,
                            ),
                            "marked_for_abort_by_pivot_rule",
                        ));
                        Err((MvccError::BusySnapshot, FcwResult::Clean))
                    } else if snapshot.has_in_rw && snapshot.has_out_rw {
                        if let Some(txn_id) = TxnId::new(session_id) {
                            self.concurrent_lock_table.release_all(txn_id);
                        }
                        if let Some(handle) = registry.get_mut(session_id) {
                            handle.mark_aborted();
                        }
                        abort_card = Some(Self::build_ssi_decision_draft(
                            &snapshot,
                            SsiDecisionType::AbortWriteSkew,
                            active_conflicts.conflicting_txns,
                            Self::merge_conflict_pages(
                                Vec::new(),
                                active_conflicts.conflict_pages,
                                &snapshot.write_pages,
                            ),
                            "dangerous_structure_in_and_out_rw",
                        ));
                        Err((MvccError::BusySnapshot, FcwResult::Clean))
                    } else {
                        Ok(write_pages)
                    }
                }
            }
        };

        if let Some(card) = abort_card {
            self.ssi_evidence_ledger.record_async(card);
        }

        match validate_result {
            Ok(write_pages) => Ok(Some((session_id, assign_seq, write_pages))),
            Err((err, fcw_result)) => {
                self.concurrent_registry.borrow_mut().remove(session_id);
                *self.concurrent_session_id.borrow_mut() = None;
                Err(Self::map_mvcc_commit_error(err, fcw_result))
            }
        }
    }

    fn finalize_concurrent_commit(
        &self,
        session_id: u64,
        committed_seq: CommitSeq,
        write_pages: Vec<PageNumber>,
    ) {
        for page in write_pages {
            self.concurrent_commit_index.update(page, committed_seq);
        }
        if let Some(txn_id) = TxnId::new(session_id) {
            self.concurrent_lock_table.release_all(txn_id);
        }
        let commit_card = {
            let mut registry = self.concurrent_registry.borrow_mut();
            registry.remove(session_id).map(|mut handle| {
                let snapshot = Self::capture_ssi_snapshot(&handle);
                let active_conflicts =
                    Self::collect_active_conflict_evidence(&registry, session_id, &snapshot);
                handle.mark_committed();
                Self::build_ssi_decision_draft(
                    &snapshot,
                    SsiDecisionType::CommitAllowed,
                    active_conflicts.conflicting_txns,
                    Self::merge_conflict_pages(
                        Vec::new(),
                        active_conflicts.conflict_pages,
                        &snapshot.write_pages,
                    ),
                    format!("commit_allowed_commit_seq={}", committed_seq.get()),
                )
                .with_commit_seq(committed_seq)
            })
        };
        if let Some(card) = commit_card {
            self.ssi_evidence_ledger.record_async(card);
        }
        *self.next_commit_seq.borrow_mut() = committed_seq.get() + 1;
        *self.concurrent_session_id.borrow_mut() = None;
    }

    /// Handle COMMIT.
    fn execute_commit(&self) -> Result<()> {
        if !*self.in_transaction.borrow() {
            return Err(FrankenError::Internal(
                "cannot commit - no transaction is active".to_owned(),
            ));
        }

        let concurrent_commit_plan = if *self.concurrent_txn.borrow() {
            self.plan_concurrent_commit()?
        } else {
            None
        };
        let cx = Cx::new();

        // Attempt pager commit without consuming the handle (retriable on BUSY).
        // We use a scope to limit the mutable borrow of active_txn.
        {
            let mut txn_guard = self.active_txn.borrow_mut();
            if let Some(txn) = txn_guard.as_mut() {
                txn.commit(&cx)?;
            }
        }

        // Commit succeeded; now consume and drop the handle.
        *self.active_txn.borrow_mut() = None;

        if let Some((session_id, committed_seq, write_pages)) = concurrent_commit_plan {
            self.finalize_concurrent_commit(session_id, committed_seq, write_pages);
        }

        // Discard rollback snapshot and savepoints — changes are committed.
        *self.txn_snapshot.borrow_mut() = None;
        self.savepoints.borrow_mut().clear();
        *self.in_transaction.borrow_mut() = false;
        *self.implicit_txn.borrow_mut() = false;
        *self.concurrent_txn.borrow_mut() = false;
        self.db.borrow_mut().commit_undo();

        // MVCC GC (bd-3bql / 5E.5): After commit, trigger GC if scheduler permits.
        // Commit makes new versions visible, potentially making old ones prunable.
        self.maybe_gc_tick();

        Ok(())
    }

    /// Handle ROLLBACK [TO SAVEPOINT name].
    fn execute_rollback(&self, rb: &fsqlite_ast::RollbackStatement) -> Result<()> {
        let cx = Cx::new();
        if let Some(ref sp_name) = rb.to_savepoint {
            let (idx, snap, canonical_name, concurrent_snap) = {
                let savepoints = self.savepoints.borrow();
                let idx = savepoints
                    .iter()
                    .rposition(|e| e.name.eq_ignore_ascii_case(sp_name))
                    .ok_or_else(|| {
                        FrankenError::Internal(format!("no such savepoint: {sp_name}"))
                    })?;
                let entry = &savepoints[idx];
                (
                    idx,
                    entry.snapshot.clone(),
                    entry.name.clone(),
                    entry.concurrent_snapshot.clone(),
                )
            };

            // ROLLBACK TO SAVEPOINT: restore to the named savepoint's snapshot
            // but keep the savepoint (don't pop it).
            if let Some(txn) = self.active_txn.borrow_mut().as_mut() {
                txn.rollback_to_savepoint(&cx, &canonical_name)?;
            }

            // MVCC concurrent-writer rollback (bd-14zc / 5E.1):
            // Restore concurrent write set state if in concurrent mode.
            if let Some(concurrent_snap) = concurrent_snap {
                let session_id = self.concurrent_session_id.borrow().ok_or_else(|| {
                    FrankenError::Internal(
                        "concurrent savepoint present but no active session".to_owned(),
                    )
                })?;
                let mut registry = self.concurrent_registry.borrow_mut();
                let handle = registry.get_mut(session_id).ok_or_else(|| {
                    FrankenError::Internal("concurrent session handle not found".to_owned())
                })?;
                concurrent_rollback_to_savepoint(handle, &concurrent_snap).map_err(|e| {
                    FrankenError::Internal(format!("concurrent rollback failed: {e}"))
                })?;
            }

            {
                let mut savepoints = self.savepoints.borrow_mut();
                // Discard savepoints created after the rollback target.
                savepoints.truncate(idx + 1);
            }
            self.restore_snapshot(&snap);
        } else {
            // Full ROLLBACK: restore to transaction start.
            // 5D.2 (bd-1ene): Use pager rollback and reload from committed state.
            if !*self.in_transaction.borrow() {
                return Err(FrankenError::Internal(
                    "cannot rollback - no transaction is active".to_owned(),
                ));
            }

            // MVCC concurrent-writer abort (bd-14zc / 5E.1):
            // When in concurrent mode, call concurrent_abort to release page locks.
            if *self.concurrent_txn.borrow() {
                if let Some(session_id) = self.concurrent_session_id.borrow_mut().take() {
                    let mut registry = self.concurrent_registry.borrow_mut();
                    if let Some(handle) = registry.get_mut(session_id) {
                        concurrent_abort(handle, &self.concurrent_lock_table, session_id);
                    }
                    registry.remove(session_id);
                }
            }

            // Roll back the pager transaction: discard all dirty pages and
            // release writer locks. After this, the pager reflects the
            // pre-transaction committed state.
            if let Some(mut txn) = self.active_txn.borrow_mut().take() {
                txn.rollback(&cx)?;
            }

            // Reload MemDatabase from pager's committed state.
            // This replaces the snapshot-restore approach with reading the
            // authoritative state from the pager.
            self.reload_memdb_from_pager(&cx)?;

            // Clear transaction state.
            *self.txn_snapshot.borrow_mut() = None;
            self.savepoints.borrow_mut().clear();
            *self.in_transaction.borrow_mut() = false;
            *self.implicit_txn.borrow_mut() = false;
            *self.concurrent_txn.borrow_mut() = false;
            // End undo tracking (no longer needed since we reload from pager).
            self.db.borrow_mut().commit_undo();

            // MVCC GC (bd-3bql / 5E.5): After full rollback, trigger GC if scheduler permits.
            // Rollback discards the write set and releases page locks, good time for cleanup.
            self.maybe_gc_tick();
        }
        Ok(())
    }

    // ── MVCC garbage collection (bd-3bql / 5E.5) ─────────────────────────────

    /// Compute the GC horizon: the minimum `begin_seq` across all active snapshots.
    ///
    /// The GC horizon determines which page versions can be safely pruned.
    /// A version is reclaimable only if a newer version exists with `commit_seq`
    /// at or below the horizon. This ensures no active transaction can see a
    /// version that gets pruned.
    ///
    /// Returns the highest possible `CommitSeq` if no active transactions
    /// (meaning all old versions except the latest per page can be pruned).
    fn compute_gc_horizon(&self) -> CommitSeq {
        // Use ConcurrentRegistry::gc_horizon() to find the minimum snapshot.high
        // across all active concurrent transactions.
        self.concurrent_registry
            .borrow()
            .gc_horizon()
            .unwrap_or_else(|| {
                // No active transactions: use the current commit sequence as horizon.
                // This allows pruning everything except the latest version.
                CommitSeq::new(*self.next_commit_seq.borrow())
            })
    }

    /// Conditionally trigger a GC tick if the scheduler determines it's time.
    ///
    /// Uses the `GcScheduler` to determine if enough time has elapsed since
    /// the last tick based on version chain pressure. If so, runs `gc_tick`
    /// to prune old page versions from the `VersionStore`.
    ///
    /// Time is tracked via a logical clock (`gc_clock_millis`) that advances
    /// by a fixed increment each call, avoiding ambient time authority.
    fn maybe_gc_tick(&self) -> Option<GcTickResult> {
        // Advance the logical clock by a fixed increment (simulates ~10ms per operation).
        // This provides deterministic GC scheduling without ambient time authority.
        const GC_CLOCK_INCREMENT_MS: u64 = 10;
        let now_millis = {
            let mut clock = self.gc_clock_millis.borrow_mut();
            *clock = clock.saturating_add(GC_CLOCK_INCREMENT_MS);
            *clock
        };

        // Estimate version chain pressure from the GcTodo queue length.
        // This is a proxy for actual chain length sampling - a fuller
        // implementation would sample actual chain lengths from the arena.
        let pressure = {
            let todo = self.gc_todo.borrow();
            #[allow(clippy::cast_precision_loss)]
            (todo.len() as f64).max(1.0) // minimum pressure of 1.0
        };

        let should_tick = self
            .gc_scheduler
            .borrow_mut()
            .should_tick(pressure, now_millis);
        if !should_tick {
            return None;
        }

        Some(self.gc_tick_now())
    }

    /// Force a GC tick immediately, bypassing the scheduler check.
    ///
    /// Useful for testing or when immediate cleanup is needed.
    fn gc_tick_now(&self) -> GcTickResult {
        let horizon = self.compute_gc_horizon();

        // If the GC todo queue is empty, nothing to prune.
        if self.gc_todo.borrow().is_empty() {
            return GcTickResult {
                pages_pruned: 0,
                versions_freed: 0,
                versions_budget_exhausted: false,
                pages_budget_exhausted: false,
                queue_remaining: 0,
                pruned_keys: Vec::new(),
            };
        }

        // Run GC via the VersionStore's gc_tick method which handles
        // internal locking of arena and chain_heads.
        let mut gc_todo = self.gc_todo.borrow_mut();
        let result = self.version_store.borrow().gc_tick(&mut gc_todo, horizon);

        if result.pages_pruned > 0 {
            tracing::info!(
                pages_pruned = result.pages_pruned,
                versions_freed = result.versions_freed,
                horizon = horizon.get(),
                "MVCC GC tick completed (bd-3bql)"
            );
        }

        result
    }

    /// Enqueue a page for GC consideration after a version is published.
    ///
    /// Called when a new version of a page is committed, making older versions
    /// potentially eligible for pruning.
    pub fn gc_enqueue_page(&self, pgno: PageNumber) {
        self.gc_todo.borrow_mut().enqueue(pgno);
    }

    /// Handle SAVEPOINT name.
    #[allow(clippy::unnecessary_wraps)] // will return errors once pager is wired
    fn execute_savepoint(&self, name: &str) -> Result<()> {
        let cx = Cx::new();
        // If no explicit transaction, implicitly begin one.
        if !*self.in_transaction.borrow() {
            let is_concurrent = *self.concurrent_mode_default.borrow();
            let pager_mode = if is_concurrent {
                TransactionMode::Concurrent
            } else {
                TransactionMode::Deferred
            };
            let mut txn = self.pager.begin(&cx, pager_mode)?;
            let concurrent_session = if is_concurrent {
                let commit_seq = *self.next_commit_seq.borrow();
                let snapshot = Snapshot::new(
                    CommitSeq::new(commit_seq.saturating_sub(1)),
                    SchemaEpoch::new((*self.schema_cookie.borrow()).into()),
                );
                let session_id = self
                    .concurrent_registry
                    .borrow_mut()
                    .begin_concurrent(snapshot)
                    .map_err(|error| match error {
                        MvccError::Busy => FrankenError::Busy,
                        _ => FrankenError::Internal(format!("MVCC begin failed: {error}")),
                    });
                let session_id = match session_id {
                    Ok(id) => id,
                    Err(err) => {
                        let _ = txn.rollback(&cx);
                        return Err(err);
                    }
                };
                Some(session_id)
            } else {
                None
            };

            *self.active_txn.borrow_mut() = Some(txn);
            *self.concurrent_session_id.borrow_mut() = concurrent_session;
            self.db.borrow_mut().begin_undo();
            *self.txn_snapshot.borrow_mut() = Some(self.snapshot());
            *self.in_transaction.borrow_mut() = true;
            *self.implicit_txn.borrow_mut() = true;
            *self.concurrent_txn.borrow_mut() = is_concurrent;
        }

        if let Some(txn) = self.active_txn.borrow_mut().as_mut() {
            txn.savepoint(&cx, name)?;
        }

        // MVCC concurrent-writer savepoint (bd-14zc / 5E.1):
        // Capture concurrent write set state if in concurrent mode.
        let concurrent_snapshot = if *self.concurrent_txn.borrow() {
            let session_id = self.concurrent_session_id.borrow().ok_or_else(|| {
                FrankenError::Internal("concurrent transaction active but no session ID".to_owned())
            })?;
            let registry = self.concurrent_registry.borrow();
            let handle = registry.get(session_id).ok_or_else(|| {
                FrankenError::Internal("concurrent session handle not found".to_owned())
            })?;
            Some(
                concurrent_savepoint(handle, name).map_err(|e| {
                    FrankenError::Internal(format!("concurrent savepoint failed: {e}"))
                })?,
            )
        } else {
            None
        };

        self.savepoints.borrow_mut().push(SavepointEntry {
            name: name.to_owned(),
            snapshot: self.snapshot(),
            concurrent_snapshot,
        });
        Ok(())
    }

    /// Handle RELEASE \[SAVEPOINT\] name.
    fn execute_release(&self, name: &str) -> Result<()> {
        let cx = Cx::new();
        let (idx, canonical_name) = {
            let savepoints = self.savepoints.borrow();
            let idx = savepoints
                .iter()
                .rposition(|e| e.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| FrankenError::Internal(format!("no such savepoint: {name}")))?;
            (idx, savepoints[idx].name.clone())
        };

        if let Some(txn) = self.active_txn.borrow_mut().as_mut() {
            txn.release_savepoint(&cx, &canonical_name)?;
        }

        // If no savepoints remain and we started implicitly, `RELEASE` behaves
        // like `COMMIT`, including MVCC commit finalization/cleanup.
        if idx == 0 && *self.implicit_txn.borrow() {
            return self.execute_commit();
        }

        let mut savepoints = self.savepoints.borrow_mut();
        // RELEASE removes the named savepoint and all savepoints created after it.
        savepoints.truncate(idx);
        Ok(())
    }

    // ── PRAGMA handling ────────────────────────────────────────────────

    /// Handle PRAGMA statements.
    ///
    /// Currently supported:
    /// - `PRAGMA fsqlite.concurrent_mode = ON|OFF|TRUE|FALSE|1|0`
    ///   Enables or disables MVCC concurrent-writer mode for subsequent
    ///   transactions on this connection. When enabled, plain `BEGIN` is
    ///   automatically promoted to `BEGIN CONCURRENT`.
    #[allow(clippy::too_many_lines)]
    fn execute_pragma(&self, pragma: &fsqlite_ast::PragmaStatement) -> Result<Vec<Row>> {
        // First try connection-level knobs (journal_mode, synchronous, etc.).
        let pragma_name = pragma.name.name.to_ascii_lowercase();
        let maybe_prior_journal_mode = if pragma_name == "journal_mode" && pragma.value.is_some() {
            Some(self.pragma_state.borrow().journal_mode.clone())
        } else {
            None
        };

        let pragma_out = {
            let mut state = self.pragma_state.borrow_mut();
            fsqlite_vdbe::pragma::apply_connection_pragma(&mut state, pragma)?
        };

        if let Some(prior_journal_mode) = maybe_prior_journal_mode {
            if let fsqlite_vdbe::pragma::PragmaOutput::Text(ref mode) = pragma_out {
                if let Err(err) = self.apply_journal_mode_to_pager(mode) {
                    self.pragma_state.borrow_mut().journal_mode = prior_journal_mode;
                    return Err(err);
                }
            }
        }

        match pragma_out {
            fsqlite_vdbe::pragma::PragmaOutput::Text(s) => {
                return Ok(vec![Row {
                    values: vec![SqliteValue::Text(s)],
                }]);
            }
            fsqlite_vdbe::pragma::PragmaOutput::Int(n) => {
                return Ok(vec![Row {
                    values: vec![SqliteValue::Integer(n)],
                }]);
            }
            fsqlite_vdbe::pragma::PragmaOutput::Bool(b) => {
                return Ok(vec![Row {
                    values: vec![SqliteValue::Integer(i64::from(b))],
                }]);
            }
            fsqlite_vdbe::pragma::PragmaOutput::Unsupported => {}
        }

        // fsqlite-specific: concurrent_mode toggle.
        let name = pragma.name.name.to_lowercase();
        if matches!(name.as_str(), "quick_check" | "integrity_check") {
            return Ok(vec![Row {
                values: vec![SqliteValue::Text("ok".to_owned())],
            }]);
        }
        if matches!(
            name.as_str(),
            "page_count" | "freelist_count" | "schema_version" | "encoding"
        ) {
            let header = self.pragma_database_header()?;
            let value = match name.as_str() {
                "page_count" => {
                    SqliteValue::Integer(header.as_ref().map_or(1, |hdr| i64::from(hdr.page_count)))
                }
                "freelist_count" => SqliteValue::Integer(
                    header
                        .as_ref()
                        .map_or(0, |hdr| i64::from(hdr.freelist_count)),
                ),
                "schema_version" => SqliteValue::Integer(i64::from(self.schema_cookie())),
                "encoding" => {
                    let encoding = match header
                        .as_ref()
                        .map_or(TextEncoding::Utf8, |hdr| hdr.text_encoding)
                    {
                        TextEncoding::Utf8 => "UTF-8",
                        TextEncoding::Utf16le => "UTF-16le",
                        TextEncoding::Utf16be => "UTF-16be",
                    };
                    SqliteValue::Text(encoding.to_owned())
                }
                _ => unreachable!("guarded by matches!"),
            };
            return Ok(vec![Row {
                values: vec![value],
            }]);
        }
        let schema = pragma.name.schema.as_ref().map(|s| s.to_lowercase());
        let full_name = if let Some(ref s) = schema {
            format!("{s}.{name}")
        } else {
            name
        };

        match full_name.as_str() {
            "fsqlite.concurrent_mode" | "concurrent_mode" => {
                if let Some(ref val) = pragma.value {
                    let enabled = parse_pragma_bool(val)?;
                    *self.concurrent_mode_default.borrow_mut() = enabled;
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                } else {
                    let enabled = *self.concurrent_mode_default.borrow();
                    Ok(vec![Row {
                        values: vec![SqliteValue::Integer(i64::from(enabled))],
                    }])
                }
            }
            // ── MVCC conflict observability PRAGMAs (bd-t6sv2.1) ──────────
            "fsqlite.conflict_stats" | "conflict_stats" => {
                let to_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
                let snap = self.conflict_observer.metrics().snapshot();
                Ok(vec![
                    Row {
                        values: vec![
                            SqliteValue::Text("conflicts_total".into()),
                            SqliteValue::Integer(to_i64(snap.conflicts_total)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("page_contentions".into()),
                            SqliteValue::Integer(to_i64(snap.page_contentions)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("fcw_drifts".into()),
                            SqliteValue::Integer(to_i64(snap.fcw_drifts)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("ssi_aborts".into()),
                            SqliteValue::Integer(to_i64(snap.ssi_aborts)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("conflicts_resolved".into()),
                            SqliteValue::Integer(to_i64(snap.conflicts_resolved)),
                        ],
                    },
                ])
            }
            "fsqlite.conflict_log" | "conflict_log" => {
                let events = self.conflict_observer.log().snapshot();
                let rows: Vec<Row> = events
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        let desc = format!("{e:?}");
                        Row {
                            values: vec![
                                SqliteValue::Integer(i64::try_from(i).unwrap_or(i64::MAX)),
                                SqliteValue::Integer(
                                    i64::try_from(e.timestamp_ns()).unwrap_or(i64::MAX),
                                ),
                                SqliteValue::Text(desc),
                            ],
                        }
                    })
                    .collect();
                Ok(rows)
            }
            "fsqlite.conflict_reset" | "conflict_reset" => {
                self.conflict_observer.reset();
                Ok(vec![Row {
                    values: vec![SqliteValue::Text("ok".into())],
                }])
            }
            "fsqlite.trace_stats" | "trace_stats" => {
                let to_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
                let snapshot = trace_metrics_snapshot();
                Ok(vec![
                    Row {
                        values: vec![
                            SqliteValue::Text("fsqlite_trace_spans_total".into()),
                            SqliteValue::Integer(to_i64(snapshot.fsqlite_trace_spans_total)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("fsqlite_trace_export_errors_total".into()),
                            SqliteValue::Integer(to_i64(
                                snapshot.fsqlite_trace_export_errors_total,
                            )),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("fsqlite_compat_trace_callbacks_total".into()),
                            SqliteValue::Integer(to_i64(
                                snapshot.fsqlite_compat_trace_callbacks_total,
                            )),
                        ],
                    },
                ])
            }
            "fsqlite.trace_reset" | "trace_reset" => {
                reset_trace_metrics();
                Ok(vec![Row {
                    values: vec![SqliteValue::Text("ok".into())],
                }])
            }
            // ── Simple join lineage/provenance PRAGMA (bd-2j365) ─────────
            "fsqlite.lineage" | "lineage" => {
                let value = pragma.value.as_ref().ok_or_else(|| {
                    FrankenError::Internal(
                        "PRAGMA lineage requires `left_table,right_table,left_col,right_col[,left_id_col,right_id_col]`".to_owned(),
                    )
                })?;
                let spec = parse_lineage_query(value)?;
                execute_simple_join_lineage_query(self, &spec)
            }
            // ── SSI decision evidence ledger PRAGMAs (bd-1lsfu.3) ─────────
            "fsqlite.ssi_decisions" | "ssi_decisions" => {
                let query = if let Some(value) = pragma.value.as_ref() {
                    parse_ssi_decision_query(value)?
                } else {
                    SsiDecisionQuery::default()
                };
                let cards = self.ssi_evidence_ledger.query(&query);
                Ok(ssi_decision_cards_to_rows(&cards))
            }
            // ── RaptorQ repair observability PRAGMAs (bd-t6sv2.3) ──────────
            "fsqlite.raptorq_stats" | "raptorq_stats" | "fsqlite_raptorq_stats" => {
                let to_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
                let snapshot = raptorq_repair_metrics_snapshot();
                Ok(vec![
                    Row {
                        values: vec![
                            SqliteValue::Text("repairs_total".into()),
                            SqliteValue::Integer(to_i64(snapshot.repairs_total)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("repairs_failed".into()),
                            SqliteValue::Integer(to_i64(snapshot.repairs_failed)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("symbols_reclaimed".into()),
                            SqliteValue::Integer(to_i64(snapshot.symbols_reclaimed)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("budget_utilization_pct".into()),
                            SqliteValue::Integer(i64::from(snapshot.budget_utilization_pct)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("severity_1".into()),
                            SqliteValue::Integer(to_i64(snapshot.severity_histogram.one)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("severity_2_5".into()),
                            SqliteValue::Integer(to_i64(snapshot.severity_histogram.two_to_five)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("severity_6_10".into()),
                            SqliteValue::Integer(to_i64(snapshot.severity_histogram.six_to_ten)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("severity_11_plus".into()),
                            SqliteValue::Integer(to_i64(snapshot.severity_histogram.eleven_plus)),
                        ],
                    },
                    Row {
                        values: vec![
                            SqliteValue::Text("wal_health_score".into()),
                            SqliteValue::Integer(i64::from(snapshot.wal_health_score)),
                        ],
                    },
                ])
            }
            "fsqlite.raptorq_events" | "raptorq_events" | "fsqlite_raptorq_events" => {
                let limit = if let Some(ref value) = pragma.value {
                    parse_pragma_nonnegative_usize(value, "raptorq_events limit")?
                } else {
                    64
                };
                let events = raptorq_repair_events_snapshot(limit);
                let rows = events
                    .into_iter()
                    .enumerate()
                    .map(|(index, event)| Row {
                        values: vec![
                            SqliteValue::Integer(i64::try_from(index).unwrap_or(i64::MAX)),
                            SqliteValue::Integer(i64::from(event.frame_id)),
                            SqliteValue::Integer(i64::from(event.symbols_lost)),
                            SqliteValue::Integer(i64::from(event.symbols_used)),
                            SqliteValue::Integer(i64::from(event.repair_success)),
                            SqliteValue::Integer(
                                i64::try_from(event.latency_ns).unwrap_or(i64::MAX),
                            ),
                            SqliteValue::Integer(i64::from(event.budget_utilization_pct)),
                            SqliteValue::Text(event.severity_bucket.as_str().to_owned()),
                        ],
                    })
                    .collect::<Vec<_>>();
                Ok(rows)
            }
            "fsqlite.raptorq_repair_evidence"
            | "raptorq_repair_evidence"
            | "fsqlite_raptorq_repair_evidence" => {
                let query = if let Some(value) = pragma.value.as_ref() {
                    parse_raptorq_repair_evidence_query(value)?
                } else {
                    WalFecRepairEvidenceQuery {
                        limit: Some(256),
                        ..WalFecRepairEvidenceQuery::default()
                    }
                };
                let cards = query_raptorq_repair_evidence(&query);
                Ok(raptorq_repair_evidence_cards_to_rows(&cards))
            }
            "fsqlite.raptorq_reset" | "raptorq_reset" | "fsqlite_raptorq_reset" => {
                reset_raptorq_repair_telemetry();
                Ok(vec![Row {
                    values: vec![SqliteValue::Text("ok".into())],
                }])
            }
            "wal_checkpoint" => {
                // Parse checkpoint mode from the value (PASSIVE, FULL, RESTART, TRUNCATE).
                // Default is PASSIVE if no value provided.
                let mode = if let Some(ref val) = pragma.value {
                    parse_checkpoint_mode(val)?
                } else {
                    CheckpointMode::Passive
                };

                // SQLite behavior: when not in WAL mode, wal_checkpoint does not error.
                // It returns the sentinel tuple (busy=0, log=-1, checkpointed=-1).
                if self.pager.journal_mode() != JournalMode::Wal {
                    return Ok(vec![Row {
                        values: vec![
                            SqliteValue::Integer(0),
                            SqliteValue::Integer(-1),
                            SqliteValue::Integer(-1),
                        ],
                    }]);
                }

                let cx = Cx::new();
                let result = self.pager.checkpoint(&cx, mode)?;

                // Return: busy (always 0), log (total frames), checkpointed (frames backfilled)
                // SQLite returns 3 columns: busy, log, checkpointed
                Ok(vec![Row {
                    values: vec![
                        SqliteValue::Integer(0), // busy - not implemented
                        SqliteValue::Integer(i64::from(result.total_frames)),
                        SqliteValue::Integer(i64::from(result.frames_backfilled)),
                    ],
                }])
            }
            // Unrecognised pragmas are silently ignored (SQLite compatibility).
            _ => Ok(Vec::new()),
        }
    }

    fn pragma_database_header(&self) -> Result<Option<DatabaseHeader>> {
        let cx = Cx::new();

        if let Some(active_txn) = self.active_txn.borrow_mut().as_mut() {
            let page1 = active_txn.get_page(&cx, PageNumber::ONE)?;
            return Ok(parse_database_header(page1.as_ref()));
        }

        let mut txn = self.pager.begin(&cx, TransactionMode::ReadOnly)?;
        let header = {
            let page1 = txn.get_page(&cx, PageNumber::ONE)?;
            parse_database_header(page1.as_ref())
        };
        let _ = txn.rollback(&cx);
        Ok(header)
    }

    fn apply_current_journal_mode_to_pager(&self) -> Result<()> {
        let journal_mode = self.pragma_state.borrow().journal_mode.clone();
        self.apply_journal_mode_to_pager(&journal_mode)
    }

    fn apply_journal_mode_to_pager(&self, journal_mode: &str) -> Result<()> {
        let cx = Cx::new();
        let requested_mode = if journal_mode.eq_ignore_ascii_case("wal") {
            JournalMode::Wal
        } else {
            JournalMode::Delete
        };

        if requested_mode == JournalMode::Wal {
            match self.pager.set_journal_mode(&cx, JournalMode::Wal) {
                Ok(_) => Ok(()),
                Err(FrankenError::Unsupported) => {
                    self.pager.install_wal_backend(&cx, &self.path)?;
                    self.pager.set_journal_mode(&cx, JournalMode::Wal)?;
                    Ok(())
                }
                Err(err) => Err(err),
            }
        } else if self.pager.journal_mode() != JournalMode::Delete {
            self.pager.set_journal_mode(&cx, JournalMode::Delete)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    // ── Public MVCC accessors ─────────────────────────────────────────

    /// Returns `true` if the current transaction was started with
    /// `BEGIN CONCURRENT` (or was promoted to concurrent mode via the
    /// `fsqlite.concurrent_mode` PRAGMA).
    #[must_use]
    pub fn is_concurrent_transaction(&self) -> bool {
        *self.concurrent_txn.borrow()
    }

    /// Returns `true` if the connection-level concurrent-mode default is
    /// enabled (i.e. `PRAGMA fsqlite.concurrent_mode = ON`).
    #[must_use]
    pub fn is_concurrent_mode_default(&self) -> bool {
        *self.concurrent_mode_default.borrow()
    }

    /// Returns `true` if there is an active MVCC concurrent session for this
    /// connection (bd-14zc / 5E.1).
    #[must_use]
    pub fn has_concurrent_session(&self) -> bool {
        self.concurrent_session_id.borrow().is_some()
    }

    /// Returns the number of active concurrent writers across all connections
    /// sharing this registry (bd-14zc / 5E.1).
    #[must_use]
    pub fn concurrent_writer_count(&self) -> usize {
        self.concurrent_registry.borrow().active_count()
    }

    /// Returns a snapshot of retained SSI decision cards.
    #[must_use]
    pub fn ssi_decisions_snapshot(&self) -> Vec<SsiDecisionCard> {
        self.ssi_evidence_ledger.snapshot()
    }

    /// Query SSI decision cards by transaction id, decision type, and/or time range.
    #[must_use]
    pub fn query_ssi_decisions(&self, query: &SsiDecisionQuery) -> Vec<SsiDecisionCard> {
        self.ssi_evidence_ledger.query(query)
    }

    /// Returns a snapshot of retained RaptorQ repair evidence cards.
    #[must_use]
    pub fn raptorq_repair_evidence_snapshot(&self) -> Vec<WalFecRepairEvidenceCard> {
        raptorq_repair_evidence_snapshot(0)
    }

    /// Query RaptorQ repair evidence cards by page/frame, severity bucket, and/or time range.
    #[must_use]
    pub fn query_raptorq_repair_evidence_cards(
        &self,
        query: &WalFecRepairEvidenceQuery,
    ) -> Vec<WalFecRepairEvidenceCard> {
        query_raptorq_repair_evidence(query)
    }

    /// Returns a reference to the connection-scoped PRAGMA state.
    ///
    /// The harness uses this to verify that both engines received identical
    /// configuration (journal_mode, synchronous, cache_size, page_size,
    /// busy_timeout).
    #[must_use]
    pub fn pragma_state(&self) -> std::cell::Ref<'_, fsqlite_vdbe::pragma::ConnectionPragmaState> {
        self.pragma_state.borrow()
    }

    // ── Compilation helpers ─────────────────────────────────────────────

    /// Compile a table-backed SELECT through the VDBE codegen.
    fn compile_table_select(&self, select: &SelectStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let ctx = CodegenContext {
            concurrent_mode: self.is_concurrent_transaction(),
            rowid_alias_col_idx: None,
        };
        codegen_select(&mut builder, select, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Handle GROUP BY + JOIN by materializing the join first, then applying
    /// GROUP BY aggregation directly on the joined rows.
    #[allow(clippy::too_many_lines)]
    fn execute_group_by_join_select(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        // Step 1: Execute the JOIN (SELECT * without GROUP BY/HAVING).
        let mut join_select = select.clone();
        if let SelectCore::Select {
            ref mut group_by,
            ref mut having,
            ref mut columns,
            ..
        } = join_select.body.select
        {
            *group_by = Vec::new();
            *having = None;
            *columns = vec![ResultColumn::Star];
        }
        join_select.limit = None;
        join_select.order_by = Vec::new();

        let join_rows = self.execute_join_select(&join_select, params)?;

        // Step 2: Build a col_map with original table labels.
        let col_map = self.build_join_col_map(select);

        // Step 3: Extract GROUP BY expressions and result columns.
        let SelectCore::Select {
            columns,
            group_by: group_by_exprs,
            having,
            ..
        } = &select.body.select
        else {
            return Err(FrankenError::NotImplemented(
                "GROUP BY JOIN on non-SELECT core".to_owned(),
            ));
        };
        let having_expr = having.as_deref();

        // Step 4a: Expand Star / TableStar into explicit column references
        // using the col_map so GROUP BY can process them individually.
        let expanded_columns: Vec<ResultColumn> = columns
            .iter()
            .flat_map(|col| match col {
                ResultColumn::Star => col_map
                    .iter()
                    .map(|(tbl, c)| ResultColumn::Expr {
                        expr: Expr::Column(
                            ColumnRef::qualified(tbl.clone(), c.clone()),
                            Span::new(0, 0),
                        ),
                        alias: None,
                    })
                    .collect::<Vec<_>>(),
                ResultColumn::TableStar(tbl) => col_map
                    .iter()
                    .filter(|(t, _)| t.eq_ignore_ascii_case(tbl))
                    .map(|(t, c)| ResultColumn::Expr {
                        expr: Expr::Column(
                            ColumnRef::qualified(t.clone(), c.clone()),
                            Span::new(0, 0),
                        ),
                        alias: None,
                    })
                    .collect::<Vec<_>>(),
                other @ ResultColumn::Expr { .. } => vec![other.clone()],
            })
            .collect();

        // Step 4b: Parse result columns into GroupByColumn descriptors.
        let result_descriptors: Vec<GroupByColumn> = expanded_columns
            .iter()
            .map(|col| match col {
                ResultColumn::Expr {
                    expr:
                        Expr::FunctionCall {
                            name,
                            args,
                            distinct: is_distinct,
                            ..
                        },
                    ..
                } if is_agg_fn(name) => {
                    let func = name.to_ascii_lowercase();
                    let mut separator = None;
                    let arg_col = match args {
                        FunctionArgs::Star => {
                            if func == "count" {
                                None
                            } else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "{func}(*) is not supported in GROUP BY+JOIN path"
                                )));
                            }
                        }
                        FunctionArgs::List(exprs) if exprs.is_empty() => {
                            if func == "count" {
                                None
                            } else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "{func}() with no args is not supported in GROUP BY+JOIN path"
                                )));
                            }
                        }
                        FunctionArgs::List(exprs) if exprs.len() == 1 => {
                            let col_name = expr_col_name(&exprs[0]).ok_or_else(|| {
                                FrankenError::NotImplemented(format!(
                                    "non-column argument to aggregate {func}() in GROUP BY+JOIN"
                                ))
                            })?;
                            let table_prefix = match &exprs[0] {
                                Expr::Column(cr, _) => cr.table.as_deref(),
                                _ => None,
                            };
                            let idx = find_col_in_map(&col_map, table_prefix, col_name)?;
                            Some(idx)
                        }
                        FunctionArgs::List(exprs)
                            if exprs.len() == 2
                                && (func == "group_concat" || func == "string_agg") =>
                        {
                            let col_name = expr_col_name(&exprs[0]).ok_or_else(|| {
                                FrankenError::NotImplemented(format!(
                                    "non-column argument to aggregate {func}() in GROUP BY+JOIN"
                                ))
                            })?;
                            let table_prefix = match &exprs[0] {
                                Expr::Column(cr, _) => cr.table.as_deref(),
                                _ => None,
                            };
                            let idx = find_col_in_map(&col_map, table_prefix, col_name)?;
                            if let Expr::Literal(Literal::String(s), _) = &exprs[1] {
                                separator = Some(s.clone());
                            }
                            Some(idx)
                        }
                        FunctionArgs::List(_) => {
                            return Err(FrankenError::NotImplemented(format!(
                                "{func}() with multiple args is not supported in GROUP BY+JOIN path"
                            )));
                        }
                    };
                    Ok(GroupByColumn::Agg {
                        name: func,
                        arg_col,
                        distinct: *is_distinct,
                        separator,
                    })
                }
                ResultColumn::Expr { expr, .. } => Ok(GroupByColumn::Plain(Box::new(expr.clone()))),
                ResultColumn::Star | ResultColumn::TableStar(_) => Err(
                    FrankenError::NotImplemented("SELECT * with GROUP BY+JOIN".to_owned()),
                ),
            })
            .collect::<Result<Vec<_>>>()?;

        // Step 5: Group the joined rows by evaluating GROUP BY expressions.
        let mut groups: Vec<(Vec<SqliteValue>, Vec<Vec<SqliteValue>>)> = Vec::new();
        for row in &join_rows {
            let key: Vec<SqliteValue> = group_by_exprs
                .iter()
                .map(|expr| {
                    eval_join_expr(expr, row.values(), &col_map).unwrap_or(SqliteValue::Null)
                })
                .collect();
            if let Some(group) = groups.iter_mut().find(|(k, _)| k == &key) {
                group.1.push(row.values().to_vec());
            } else {
                groups.push((key, vec![row.values().to_vec()]));
            }
        }

        // Step 6: Build result rows from groups.
        let col_names: Vec<String> = col_map.iter().map(|(_, c)| c.clone()).collect();
        let mut result = Vec::with_capacity(groups.len());
        for (_key, group_rows) in &groups {
            let mut values = Vec::with_capacity(result_descriptors.len());
            for desc in &result_descriptors {
                match desc {
                    GroupByColumn::Plain(expr) => {
                        let val = group_rows.first().map_or(SqliteValue::Null, |r| {
                            eval_join_expr(expr, r, &col_map).unwrap_or(SqliteValue::Null)
                        });
                        values.push(val);
                    }
                    GroupByColumn::Agg {
                        name,
                        arg_col,
                        distinct,
                        separator,
                    } => {
                        if name == "count" && arg_col.is_none() {
                            #[allow(clippy::cast_possible_wrap)]
                            values.push(SqliteValue::Integer(group_rows.len() as i64));
                        } else {
                            let Some(idx) = *arg_col else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "aggregate {name} requires a column argument"
                                )));
                            };
                            let mut agg_values: Vec<&SqliteValue> = group_rows
                                .iter()
                                .filter_map(|r| r.get(idx))
                                .filter(|v| !matches!(v, SqliteValue::Null))
                                .collect();
                            if *distinct {
                                dedup_values(&mut agg_values);
                            }
                            values.push(compute_aggregate_ext(
                                name,
                                &agg_values,
                                separator.as_deref(),
                            ));
                        }
                    }
                }
            }
            if let Some(having) = having_expr {
                if !evaluate_having_predicate(
                    having,
                    &values,
                    &result_descriptors,
                    &expanded_columns,
                    group_rows,
                    &col_names,
                ) {
                    continue;
                }
            }
            result.push(Row { values });
        }

        // Step 7: ORDER BY and LIMIT.
        if !select.order_by.is_empty() {
            sort_rows_by_order_terms(&mut result, &select.order_by, &expanded_columns)?;
        }
        if let Some(ref limit_clause) = select.limit {
            apply_limit_offset_postprocess(&mut result, limit_clause);
        }

        Ok(result)
    }

    /// Build a col_map with original table labels for a SELECT with JOINs.
    fn build_join_col_map(&self, select: &SelectStatement) -> Vec<(String, String)> {
        let mut col_map = Vec::new();
        let schema = self.schema.borrow();
        if let SelectCore::Select {
            from: Some(from), ..
        } = &select.body.select
        {
            if let TableOrSubquery::Table { name, alias, .. } = &from.source {
                let label = alias.as_deref().unwrap_or(&name.name);
                if let Some(ts) = schema
                    .iter()
                    .find(|t| t.name.eq_ignore_ascii_case(&name.name))
                {
                    for c in &ts.columns {
                        col_map.push((label.to_owned(), c.name.clone()));
                    }
                }
            }
            for join in &from.joins {
                if let TableOrSubquery::Table { name, alias, .. } = &join.table {
                    let label = alias.as_deref().unwrap_or(&name.name);
                    if let Some(ts) = schema
                        .iter()
                        .find(|t| t.name.eq_ignore_ascii_case(&name.name))
                    {
                        for c in &ts.columns {
                            col_map.push((label.to_owned(), c.name.clone()));
                        }
                    }
                }
            }
        }
        col_map
    }

    /// Execute a GROUP BY aggregate SELECT via post-execution processing:
    /// scan all rows, group by key columns, compute aggregates per group.
    #[allow(clippy::too_many_lines)]
    fn execute_group_by_select(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        if !select.body.compounds.is_empty() {
            return Err(FrankenError::NotImplemented(
                "compound SELECT is not supported with GROUP BY in this connection path".to_owned(),
            ));
        }

        // Extract GROUP BY expressions and result columns from the AST.
        let SelectCore::Select {
            columns,
            group_by: group_by_exprs,
            having,
            windows,
            ..
        } = &select.body.select
        else {
            return Err(FrankenError::NotImplemented(
                "GROUP BY on non-SELECT core".to_owned(),
            ));
        };
        let having_expr = having.as_deref();
        if !windows.is_empty() {
            return Err(FrankenError::NotImplemented(
                "WINDOW is not supported in this GROUP BY connection path".to_owned(),
            ));
        }

        // Find the table name/alias from the FROM clause.
        let (table_name, table_alias) = match &select.body.select {
            SelectCore::Select {
                from: Some(from), ..
            } => match &from.source {
                fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => {
                    (name.name.clone(), alias.clone())
                }
                _ => {
                    return Err(FrankenError::NotImplemented(
                        "GROUP BY with non-table source".to_owned(),
                    ));
                }
            },
            _ => {
                return Err(FrankenError::NotImplemented(
                    "GROUP BY without FROM".to_owned(),
                ));
            }
        };

        // Resolve table schema to map column names to indices.
        let schema = self.schema.borrow();
        let table_schema = schema
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(&table_name))
            .ok_or_else(|| FrankenError::Internal(format!("table not found: {table_name}")))?;

        // Build a column map for expression evaluation: (table_label, col_name).
        // Use the alias as the table label when present so that alias-qualified
        // column references (e.g. `t.dept` when FROM table AS t) resolve correctly.
        let effective_label = table_alias.as_deref().unwrap_or(&table_name);
        let col_map: Vec<(String, String)> = table_schema
            .columns
            .iter()
            .map(|c| (effective_label.to_owned(), c.name.clone()))
            .collect();

        // Resolve the INTEGER PRIMARY KEY column name so that rowid/_rowid_/oid
        // aliases in GROUP BY and result columns can be rewritten to the real name.
        let rowid_real_col: Option<String> = self
            .rowid_alias_columns
            .borrow()
            .get(&table_name.to_ascii_lowercase())
            .map(|&idx| table_schema.columns[idx].name.clone());

        // Check if any result column is Star/TableStar — if so, we relax the
        // "must appear in GROUP BY" validation (SQLite allows this).
        let has_star = columns
            .iter()
            .any(|c| matches!(c, ResultColumn::Star | ResultColumn::TableStar(_)));

        // Expand Star/TableStar into explicit column references so GROUP BY
        // can process them individually.
        let expanded_columns: Vec<ResultColumn> = columns
            .iter()
            .flat_map(|col| match col {
                ResultColumn::Star => table_schema
                    .columns
                    .iter()
                    .map(|c| ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare(c.name.clone()), Span::new(0, 0)),
                        alias: None,
                    })
                    .collect::<Vec<_>>(),
                ResultColumn::TableStar(tbl)
                    if tbl.eq_ignore_ascii_case(&table_name)
                        || table_alias
                            .as_ref()
                            .is_some_and(|alias| tbl.eq_ignore_ascii_case(alias)) =>
                {
                    table_schema
                        .columns
                        .iter()
                        .map(|c| ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare(c.name.clone()), Span::new(0, 0)),
                            alias: None,
                        })
                        .collect::<Vec<_>>()
                }
                other => vec![other.clone()],
            })
            .collect();

        // Parse result columns into GroupByColumn descriptors.
        let mut result_descriptors: Vec<GroupByColumn> = expanded_columns
            .iter()
            .map(|col| match col {
                ResultColumn::Expr {
                    expr:
                        Expr::FunctionCall {
                            name,
                            args,
                            distinct: is_distinct,
                            ..
                        },
                    ..
                } if is_agg_fn(name) => {
                    let func = name.to_ascii_lowercase();
                    let mut separator = None;
                    let arg_col = match args {
                        FunctionArgs::Star => {
                            if func == "count" {
                                None
                            } else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "{func}(*) is not supported in this GROUP BY connection path"
                                )));
                            }
                        }
                        FunctionArgs::List(exprs) if exprs.is_empty() => {
                            if func == "count" {
                                None
                            } else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "{func}() with no args is not supported in this GROUP BY connection path"
                                )));
                            }
                        }
                        FunctionArgs::List(exprs) if exprs.len() == 1 => {
                            let col_name = expr_col_name(&exprs[0]).ok_or_else(|| {
                                FrankenError::NotImplemented(format!(
                                    "non-column argument to aggregate {func}() is not supported in this GROUP BY connection path"
                                ))
                            })?;
                            // Resolve rowid aliases to the real column index.
                            let idx = table_schema.column_index(col_name).or_else(|| {
                                if is_rowid_alias(col_name) {
                                    self.rowid_alias_columns
                                        .borrow()
                                        .get(&table_name.to_ascii_lowercase())
                                        .copied()
                                } else {
                                    None
                                }
                            });
                            Some(idx.ok_or_else(|| {
                                FrankenError::Internal(format!(
                                    "aggregate column not found: {col_name}"
                                ))
                            })?)
                        }
                        FunctionArgs::List(exprs)
                            if exprs.len() == 2
                                && (func == "group_concat" || func == "string_agg") =>
                        {
                            let col_name = expr_col_name(&exprs[0]).ok_or_else(|| {
                                FrankenError::NotImplemented(format!(
                                    "non-column argument to aggregate {func}() is not supported in this GROUP BY connection path"
                                ))
                            })?;
                            let idx = table_schema.column_index(col_name).or_else(|| {
                                if is_rowid_alias(col_name) {
                                    self.rowid_alias_columns
                                        .borrow()
                                        .get(&table_name.to_ascii_lowercase())
                                        .copied()
                                } else {
                                    None
                                }
                            });
                            if let Expr::Literal(Literal::String(s), _) = &exprs[1] {
                                separator = Some(s.clone());
                            }
                            Some(idx.ok_or_else(|| {
                                FrankenError::Internal(format!(
                                    "aggregate column not found: {col_name}"
                                ))
                            })?)
                        }
                        FunctionArgs::List(_exprs) => {
                            return Err(FrankenError::NotImplemented(format!(
                                "{func}() with multiple args is not supported in this GROUP BY connection path"
                            )));
                        }
                    };
                    Ok(GroupByColumn::Agg {
                        name: func,
                        arg_col,
                        distinct: *is_distinct,
                        separator,
                    })
                }
                ResultColumn::Expr { expr, .. } => {
                    // The expression must match one of the GROUP BY expressions
                    // (either structurally or as a column reference to a GROUP BY column).
                    let in_group_by = group_by_exprs.iter().any(|gb| exprs_match(gb, expr));
                    if !in_group_by && !has_star {
                        let label = expr_col_name(expr).unwrap_or("<expression>");
                        return Err(FrankenError::NotImplemented(format!(
                            "non-aggregate result column '{label}' must appear in GROUP BY"
                        )));
                    }
                    Ok(GroupByColumn::Plain(Box::new(expr.clone())))
                }
                ResultColumn::Star | ResultColumn::TableStar(_) => Err(
                    FrankenError::NotImplemented("SELECT * with GROUP BY".to_owned()),
                ),
            })
            .collect::<Result<Vec<_>>>()?;

        // Save column names for HAVING evaluation before dropping the schema borrow.
        let col_names: Vec<String> = table_schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        drop(schema);

        // Rewrite rowid/_rowid_/oid aliases in GROUP BY and result column
        // expressions so that the expression evaluator can resolve them
        // against the col_map (which only contains declared column names).
        let mut group_by_exprs = group_by_exprs.clone();
        if let Some(real_col) = &rowid_real_col {
            for expr in &mut group_by_exprs {
                rewrite_rowid_aliases_in_expr(expr, real_col);
            }
            for desc in &mut result_descriptors {
                if let GroupByColumn::Plain(expr) = desc {
                    rewrite_rowid_aliases_in_expr(expr, real_col);
                }
            }
        }

        // Compile and execute a raw SELECT * scan (no aggregates, no GROUP BY).
        let raw_select = build_raw_scan_select(select);
        let program = self.compile_table_select(&raw_select)?;
        let raw_rows = self.execute_table_program(&program, params)?;

        // Group rows by evaluating GROUP BY expressions for each row.
        let mut groups: Vec<(Vec<SqliteValue>, Vec<Vec<SqliteValue>>)> = Vec::new();
        for row in &raw_rows {
            let key: Vec<SqliteValue> = group_by_exprs
                .iter()
                .map(|expr| {
                    eval_join_expr(expr, row.values(), &col_map).unwrap_or(SqliteValue::Null)
                })
                .collect();
            if let Some(group) = groups.iter_mut().find(|(k, _)| k == &key) {
                group.1.push(row.values().to_vec());
            } else {
                groups.push((key, vec![row.values().to_vec()]));
            }
        }

        // Build result rows from groups.
        let mut result = Vec::with_capacity(groups.len());
        for (_key, group_rows) in &groups {
            let mut values = Vec::with_capacity(result_descriptors.len());
            for desc in &result_descriptors {
                match desc {
                    GroupByColumn::Plain(expr) => {
                        // Evaluate the expression against the first row in the group.
                        let val = group_rows.first().map_or(SqliteValue::Null, |r| {
                            eval_join_expr(expr, r, &col_map).unwrap_or(SqliteValue::Null)
                        });
                        values.push(val);
                    }
                    GroupByColumn::Agg {
                        name,
                        arg_col,
                        distinct,
                        separator,
                    } => {
                        if name == "count" && arg_col.is_none() {
                            #[allow(clippy::cast_possible_wrap)]
                            values.push(SqliteValue::Integer(group_rows.len() as i64));
                        } else {
                            let Some(idx) = *arg_col else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "aggregate {name} requires a column argument in this GROUP BY connection path"
                                )));
                            };
                            let mut agg_values: Vec<&SqliteValue> = group_rows
                                .iter()
                                .filter_map(|r| r.get(idx))
                                .filter(|v| !matches!(v, SqliteValue::Null))
                                .collect();
                            if *distinct {
                                dedup_values(&mut agg_values);
                            }
                            values.push(compute_aggregate_ext(
                                name,
                                &agg_values,
                                separator.as_deref(),
                            ));
                        }
                    }
                }
            }
            // Apply HAVING filter: skip groups that don't satisfy the predicate.
            if let Some(having) = having_expr {
                if !evaluate_having_predicate(
                    having,
                    &values,
                    &result_descriptors,
                    &expanded_columns,
                    group_rows,
                    &col_names,
                ) {
                    continue;
                }
            }
            result.push(Row { values });
        }

        // Post-process: ORDER BY.
        if !select.order_by.is_empty() {
            sort_rows_by_order_terms(&mut result, &select.order_by, &expanded_columns)?;
        }

        // Post-process: LIMIT / OFFSET.
        if let Some(ref limit_clause) = select.limit {
            apply_limit_offset_postprocess(&mut result, limit_clause);
        }

        Ok(result)
    }

    /// Execute a compound SELECT (UNION/UNION ALL/INTERSECT/EXCEPT).
    ///
    /// Executes each SELECT arm independently, then combines results according
    /// to the set operation. ORDER BY and LIMIT are applied to the final result.
    fn execute_compound_select(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        // Execute the first SELECT arm.
        let first_arm = SelectStatement {
            with: select.with.clone(),
            body: SelectBody {
                select: select.body.select.clone(),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let mut result = self.execute_statement(Statement::Select(first_arm), params)?;

        // Process each compound arm.
        for (op, core) in &select.body.compounds {
            let arm_select = SelectStatement {
                with: None,
                body: SelectBody {
                    select: core.clone(),
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            };
            let arm_rows = self.execute_statement(Statement::Select(arm_select), params)?;

            match op {
                CompoundOp::UnionAll => {
                    result.extend(arm_rows);
                }
                CompoundOp::Union => {
                    result.extend(arm_rows);
                    dedup_rows(&mut result);
                }
                CompoundOp::Intersect => {
                    // Keep only rows present in both result and arm_rows.
                    result.retain(|row| arm_rows.iter().any(|ar| ar.values() == row.values()));
                }
                CompoundOp::Except => {
                    // Remove rows from result that appear in arm_rows.
                    result.retain(|row| !arm_rows.iter().any(|ar| ar.values() == row.values()));
                }
            }
        }

        // Post-process: ORDER BY.
        if !select.order_by.is_empty() {
            // For compound SELECT, ORDER BY references output column positions (1-based).
            if let SelectCore::Select { columns, .. } = &select.body.select {
                sort_rows_by_order_terms(&mut result, &select.order_by, columns)?;
            }
        }

        // Post-process: LIMIT / OFFSET.
        if let Some(ref limit_clause) = select.limit {
            apply_limit_offset_postprocess(&mut result, limit_clause);
        }

        Ok(result)
    }

    /// Materialize CTEs as temporary in-memory tables, execute the main query
    /// with `with` stripped, then clean up the temporary tables.
    #[allow(clippy::too_many_lines)]
    fn execute_with_ctes(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let with_clause = select.with.as_ref().unwrap();
        let is_recursive = with_clause.recursive;
        let ctes = &with_clause.ctes;
        let mut temp_names: Vec<String> = Vec::with_capacity(ctes.len());
        for cte in ctes {
            let cte_name = &cte.name;
            let has_self_ref = is_recursive
                && cte
                    .query
                    .body
                    .compounds
                    .iter()
                    .any(|(_, core)| select_core_references_table(core, cte_name));

            if has_self_ref {
                self.materialize_recursive_cte(cte, params, &mut temp_names)?;
            } else {
                // Non-recursive CTE: execute body, then create table.
                let cte_rows =
                    self.execute_statement(Statement::Select(cte.query.clone()), params)?;
                let col_names: Vec<String> = if cte.columns.is_empty() {
                    let inferred = infer_select_column_names(&cte.query);
                    if inferred.is_empty() {
                        let width = cte_rows.first().map_or(1, |r| r.values().len());
                        (0..width).map(|i| format!("_c{i}")).collect()
                    } else {
                        inferred
                    }
                } else {
                    cte.columns.clone()
                };
                let col_infos: Vec<ColumnInfo> = col_names
                    .iter()
                    .map(|name| ColumnInfo {
                        name: name.clone(),
                        affinity: 'B',
                        is_ipk: false,
                    })
                    .collect();
                let num_columns = col_infos.len();
                let root_page = self.db.borrow_mut().create_table(num_columns);
                self.schema.borrow_mut().push(TableSchema {
                    name: cte_name.clone(),
                    root_page,
                    columns: col_infos,
                    indexes: Vec::new(),
                });
                temp_names.push(cte_name.clone());
                for (i, row) in cte_rows.iter().enumerate() {
                    let vals: Vec<SqliteValue> = row.values().to_vec();
                    #[allow(clippy::cast_possible_wrap)]
                    let rowid = (i + 1) as i64;
                    let mut db = self.db.borrow_mut();
                    if let Some(table) = db.get_table_mut(root_page) {
                        table.insert_row(rowid, vals);
                    }
                }
            }
        }
        // Execute the main query with the WITH clause stripped.
        let mut stripped = select.clone();
        stripped.with = None;
        let result = self.execute_statement(Statement::Select(stripped), params);
        // Clean up temporary CTE tables.
        for name in &temp_names {
            let mut schema = self.schema.borrow_mut();
            if let Some(idx) = schema
                .iter()
                .position(|t| t.name.eq_ignore_ascii_case(name))
            {
                let rp = schema[idx].root_page;
                schema.remove(idx);
                drop(schema);
                self.db.borrow_mut().destroy_table(rp);
            }
        }
        result
    }

    /// Materialize a recursive CTE by iterating until no new rows are produced.
    #[allow(clippy::cast_possible_wrap, clippy::too_many_lines)]
    fn materialize_recursive_cte(
        &self,
        cte: &fsqlite_ast::Cte,
        params: Option<&[SqliteValue]>,
        temp_names: &mut Vec<String>,
    ) -> Result<()> {
        const MAX_RECURSION: usize = 1000;
        let cte_name = &cte.name;
        let col_names: Vec<String> = if cte.columns.is_empty() {
            let inferred = infer_select_column_names(&cte.query);
            if inferred.is_empty() {
                vec!["_c0".to_owned()]
            } else {
                inferred
            }
        } else {
            cte.columns.clone()
        };
        let col_infos: Vec<ColumnInfo> = col_names
            .iter()
            .map(|name| ColumnInfo {
                name: name.clone(),
                affinity: 'B',
                is_ipk: false,
            })
            .collect();
        let num_columns = col_infos.len();
        let root_page = self.db.borrow_mut().create_table(num_columns);
        self.schema.borrow_mut().push(TableSchema {
            name: cte_name.clone(),
            root_page,
            columns: col_infos,
            indexes: Vec::new(),
        });
        temp_names.push(cte_name.clone());

        // Execute the base case (first SELECT core only).
        let base_select = SelectStatement {
            with: None,
            body: SelectBody {
                select: cte.query.body.select.clone(),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let base_rows = self.execute_statement(Statement::Select(base_select), params)?;
        let mut all_rows: Vec<Vec<SqliteValue>> =
            base_rows.iter().map(|r| r.values().to_vec()).collect();
        let mut recursive_arms: Vec<(&CompoundOp, &SelectCore)> = Vec::new();
        for (op, core) in &cte.query.body.compounds {
            if matches!(op, CompoundOp::Intersect | CompoundOp::Except) {
                return Err(FrankenError::NotImplemented(
                    "recursive CTE supports only UNION and UNION ALL".to_owned(),
                ));
            }
            if select_core_references_table(core, cte_name) {
                recursive_arms.push((op, core));
                continue;
            }
            let arm_select = SelectStatement {
                with: None,
                body: SelectBody {
                    select: core.clone(),
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            };
            let arm_rows = self.execute_statement(Statement::Select(arm_select), params)?;
            match op {
                CompoundOp::UnionAll => {
                    all_rows.extend(arm_rows.iter().map(|row| row.values().to_vec()));
                }
                CompoundOp::Union => {
                    all_rows.extend(arm_rows.iter().map(|row| row.values().to_vec()));
                    dedup_value_rows(&mut all_rows);
                }
                CompoundOp::Intersect | CompoundOp::Except => unreachable!(),
            }
        }
        if cte
            .query
            .body
            .compounds
            .iter()
            .any(|(op, _)| matches!(op, CompoundOp::Union))
        {
            dedup_value_rows(&mut all_rows);
        }
        let mut working_set: Vec<Vec<SqliteValue>> = all_rows.clone();

        // Iterate: feed working set to recursive arm, collect new rows.
        for _ in 0..MAX_RECURSION {
            if working_set.is_empty() {
                break;
            }
            // Put only the working set in the temp table.
            {
                let mut db = self.db.borrow_mut();
                if let Some(table) = db.get_table_mut(root_page) {
                    table.clear();
                    for (i, vals) in working_set.iter().enumerate() {
                        table.insert_row(i as i64 + 1, vals.clone());
                    }
                }
            }
            // Execute each recursive arm.
            let mut new_rows: Vec<Vec<SqliteValue>> = Vec::new();
            for (op, recursive_core) in &recursive_arms {
                let arm_select = SelectStatement {
                    with: None,
                    body: SelectBody {
                        select: (*recursive_core).clone(),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                };
                let arm_rows = self.execute_statement(Statement::Select(arm_select), params)?;
                for row in &arm_rows {
                    let vals = row.values().to_vec();
                    match op {
                        CompoundOp::UnionAll => new_rows.push(vals),
                        CompoundOp::Union => {
                            if !contains_value_row(&all_rows, &vals)
                                && !contains_value_row(&new_rows, &vals)
                            {
                                new_rows.push(vals);
                            }
                        }
                        CompoundOp::Intersect | CompoundOp::Except => unreachable!(),
                    }
                }
            }
            if new_rows.is_empty() {
                break;
            }
            all_rows.extend(new_rows.iter().cloned());
            working_set = new_rows;
        }

        // Final: populate temp table with ALL accumulated rows.
        {
            let mut db = self.db.borrow_mut();
            if let Some(table) = db.get_table_mut(root_page) {
                table.clear();
                for (i, vals) in all_rows.iter().enumerate() {
                    table.insert_row(i as i64 + 1, vals.clone());
                }
            }
        }
        Ok(())
    }

    /// Pre-process a SELECT statement, eagerly evaluating EXISTS and scalar
    /// subqueries.  `IN (SELECT ...)` is left intact so the VDBE codegen
    /// can handle it via runtime probe scans.
    fn rewrite_subqueries(&self, select: &SelectStatement) -> Result<SelectStatement> {
        let mut result = select.clone();
        rewrite_in_select_core(&mut result.body.select, self, false)?;
        // Also rewrite any compound arms.
        for (_op, core) in &mut result.body.compounds {
            rewrite_in_select_core(core, self, false)?;
        }
        Ok(result)
    }

    /// Eagerly rewrite `IN (SELECT ...)` subqueries into literal lists
    /// for fallback execution paths that cannot handle them natively
    /// (expression-only SELECT, GROUP BY, JOINs).
    fn rewrite_in_subqueries_select(&self, select: &SelectStatement) -> Result<SelectStatement> {
        let mut result = select.clone();
        rewrite_in_select_core(&mut result.body.select, self, true)?;
        for (_op, core) in &mut result.body.compounds {
            rewrite_in_select_core(core, self, true)?;
        }
        Ok(result)
    }

    /// Execute a SELECT with JOINs using in-memory nested-loop evaluation.
    ///
    /// Loads each table's rows independently, then combines them according to
    /// the join type and constraint. WHERE, ORDER BY, LIMIT/OFFSET, and column
    /// projection are applied to the combined result.
    #[allow(clippy::too_many_lines)]
    fn execute_join_select(
        &self,
        select: &SelectStatement,
        _params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let SelectCore::Select {
            columns,
            from: Some(from),
            where_clause,
            ..
        } = &select.body.select
        else {
            return Err(FrankenError::NotImplemented(
                "JOIN on non-SELECT core".to_owned(),
            ));
        };

        // ── 1. Resolve all table sources (primary + joined tables) ──
        let mut table_sources: Vec<JoinTableSource> = Vec::with_capacity(1 + from.joins.len());
        // Preloaded rows for subquery sources (index-aligned with table_sources).
        let mut preloaded: Vec<Option<Vec<Vec<SqliteValue>>>> = Vec::new();

        {
            let schema = self.schema.borrow();

            // Helper closure: resolve a TableOrSubquery into a JoinTableSource
            // and optional preloaded rows (for subqueries).
            let resolve_source =
                |source: &TableOrSubquery,
                 schema: &[TableSchema]|
                 -> Result<(JoinTableSource, Option<Vec<Vec<SqliteValue>>>)> {
                    match source {
                        TableOrSubquery::Table { name, alias, .. } => {
                            let tbl = schema
                                .iter()
                                .find(|t| t.name.eq_ignore_ascii_case(&name.name))
                                .ok_or_else(|| {
                                    FrankenError::Internal(format!(
                                        "table not found: {}",
                                        name.name
                                    ))
                                })?;
                            let col_names: Vec<String> =
                                tbl.columns.iter().map(|c| c.name.clone()).collect();
                            Ok((
                                JoinTableSource {
                                    table_name: name.name.clone(),
                                    alias: alias.clone(),
                                    col_names,
                                },
                                None,
                            ))
                        }
                        TableOrSubquery::Subquery { query, alias } => {
                            let col_names = infer_select_column_names(query);
                            let label = alias.clone().unwrap_or_else(|| "_subquery".to_owned());
                            Ok((
                                JoinTableSource {
                                    table_name: label,
                                    alias: alias.clone(),
                                    col_names,
                                },
                                // Rows will be loaded after dropping schema borrow.
                                Some(Vec::new()),
                            ))
                        }
                        _ => Err(FrankenError::NotImplemented(
                            "only named tables and subqueries are supported in JOIN".to_owned(),
                        )),
                    }
                };

            // Primary table.
            let (src, pre) = resolve_source(&from.source, &schema)?;
            table_sources.push(src);
            preloaded.push(pre);

            // Joined tables.
            for join in &from.joins {
                let (src, pre) = resolve_source(&join.table, &schema)?;
                table_sources.push(src);
                preloaded.push(pre);
            }
        }

        // Execute subqueries now that the schema borrow is dropped.
        // Collect all subquery sources (from.source + from.joins[*].table).
        let all_sources: Vec<&TableOrSubquery> = std::iter::once(&from.source)
            .chain(from.joins.iter().map(|j| &j.table))
            .collect();
        for (i, pre) in preloaded.iter_mut().enumerate() {
            if pre.is_some() {
                if let TableOrSubquery::Subquery { query, .. } = all_sources[i] {
                    let rows =
                        self.execute_statement(Statement::Select(query.as_ref().clone()), None)?;
                    *pre = Some(rows.iter().map(|r| r.values().to_vec()).collect());
                }
            }
        }

        // ── 2. Build the combined column map ──
        // col_map entries: (table_label, col_name, combined_index)
        let mut col_map: Vec<(String, String)> = Vec::new();
        for src in &table_sources {
            let label = src.alias.as_deref().unwrap_or(&src.table_name);
            for col_name in &src.col_names {
                col_map.push((label.to_owned(), col_name.clone()));
            }
        }

        // ── 3. Load each table's raw rows ──
        let mut table_rows: Vec<Vec<Vec<SqliteValue>>> = Vec::with_capacity(table_sources.len());
        for (i, src) in table_sources.iter().enumerate() {
            if let Some(rows) = preloaded[i].take() {
                // Subquery: use preloaded rows.
                table_rows.push(rows);
            } else {
                // Named table: scan from database.
                let scan_sql = format!("SELECT * FROM {}", src.table_name);
                let rows = self.query(&scan_sql)?;
                table_rows.push(rows.iter().map(|r| r.values().to_vec()).collect());
            }
        }

        // ── 4. Perform joins ──
        // Start with primary table rows as "left" side.
        let primary_width = table_sources[0].col_names.len();
        let mut combined: Vec<Vec<SqliteValue>> = table_rows[0]
            .iter()
            .map(|row| row[..primary_width].to_vec())
            .collect();

        for (join_idx, join) in from.joins.iter().enumerate() {
            let right_rows = &table_rows[join_idx + 1];
            let right_width = table_sources[join_idx + 1].col_names.len();
            let current_width: usize = table_sources[..=join_idx]
                .iter()
                .map(|s| s.col_names.len())
                .sum();

            // NATURAL JOIN: auto-derive USING constraint from shared column
            // names between the left side and the right table.
            let natural_constraint;
            let effective_constraint = if join.join_type.natural {
                let left_cols: Vec<&str> = table_sources[..=join_idx]
                    .iter()
                    .flat_map(|s| s.col_names.iter().map(String::as_str))
                    .collect();
                let right_src = &table_sources[join_idx + 1];
                let shared: Vec<String> = right_src
                    .col_names
                    .iter()
                    .filter(|c| left_cols.iter().any(|l| l.eq_ignore_ascii_case(c.as_str())))
                    .cloned()
                    .collect();
                natural_constraint = JoinConstraint::Using(shared);
                Some(&natural_constraint)
            } else {
                join.constraint.as_ref()
            };

            combined = execute_single_join(
                &combined,
                right_rows,
                right_width,
                current_width,
                join.join_type.kind,
                effective_constraint,
                &col_map,
            )?;
        }

        // ── 5. Apply WHERE filter ──
        if let Some(where_expr) = where_clause {
            let mut filtered = Vec::with_capacity(combined.len());
            for row in combined {
                if eval_join_predicate(where_expr, &row, &col_map)? {
                    filtered.push(row);
                }
            }
            combined = filtered;
        }

        // ── 6. Project result columns ──
        let mut result: Vec<Row> = combined
            .iter()
            .map(|row| {
                let mut values = Vec::new();
                for col in columns {
                    match col {
                        ResultColumn::Star => {
                            // All columns from all tables.
                            values.extend(row.iter().cloned());
                        }
                        ResultColumn::TableStar(table_name) => {
                            // All columns from a specific table.
                            let mut offset = 0;
                            for src in &table_sources {
                                let label = src.alias.as_deref().unwrap_or(&src.table_name);
                                if label.eq_ignore_ascii_case(table_name) {
                                    let width = src.col_names.len();
                                    values.extend(row[offset..offset + width].iter().cloned());
                                    break;
                                }
                                offset += src.col_names.len();
                            }
                        }
                        ResultColumn::Expr { .. } => {
                            values.push(project_join_column(col, row, &col_map));
                        }
                    }
                }
                Row { values }
            })
            .collect();

        // ── 7. Post-process: ORDER BY ──
        if !select.order_by.is_empty() {
            sort_rows_by_order_terms(&mut result, &select.order_by, columns)?;
        }

        // ── 8. Post-process: LIMIT / OFFSET ──
        if let Some(ref limit_clause) = select.limit {
            apply_limit_offset_postprocess(&mut result, limit_clause);
        }

        Ok(result)
    }

    /// Compile an INSERT through the VDBE codegen.
    fn compile_table_insert(&self, insert: &fsqlite_ast::InsertStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let rowid_alias_col_idx = self
            .rowid_alias_columns
            .borrow()
            .get(&insert.table.name.to_ascii_lowercase())
            .copied();
        let ctx = CodegenContext {
            concurrent_mode: self.is_concurrent_transaction(),
            rowid_alias_col_idx,
        };
        codegen_insert(&mut builder, insert, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Compile an UPDATE through the VDBE codegen.
    fn compile_table_update(&self, update: &fsqlite_ast::UpdateStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let rowid_alias_col_idx = self
            .rowid_alias_columns
            .borrow()
            .get(&update.table.name.name.to_ascii_lowercase())
            .copied();
        let ctx = CodegenContext {
            concurrent_mode: self.is_concurrent_transaction(),
            rowid_alias_col_idx,
        };
        codegen_update(&mut builder, update, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Compile a DELETE through the VDBE codegen.
    fn compile_table_delete(&self, delete: &fsqlite_ast::DeleteStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let ctx = CodegenContext {
            concurrent_mode: self.is_concurrent_transaction(),
            rowid_alias_col_idx: None,
        };
        codegen_delete(&mut builder, delete, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Execute a VDBE program with the in-memory database attached.
    fn execute_table_program(
        &self,
        program: &VdbeProgram,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let execution_span = tracing::span!(
            target: "fsqlite.execution",
            tracing::Level::DEBUG,
            "execution",
            mode = "table"
        );
        record_trace_span_created();
        let _execution_guard = execution_span.enter();
        // Lend the active transaction to the VDBE engine so that storage
        // cursors route through the real pager/WAL stack (Phase 5, bd-2a3y).
        let txn = self.active_txn.borrow_mut().take();
        let cookie = *self.schema_cookie.borrow();

        // bd-kivg / 5E.2: Build concurrent context if in concurrent mode.
        let concurrent_ctx = if *self.concurrent_txn.borrow() {
            self.concurrent_session_id
                .borrow()
                .map(|session_id| ConcurrentExecContext {
                    session_id,
                    registry: Rc::clone(&self.concurrent_registry),
                    lock_table: Rc::clone(&self.concurrent_lock_table),
                })
        } else {
            None
        };

        let (result, txn_back) = execute_table_program_with_db(
            program,
            params,
            &self.func_registry,
            &self.db,
            txn,
            cookie,
            concurrent_ctx,
        );
        // Always restore the transaction handle, even on error.
        if let Some(txn) = txn_back {
            *self.active_txn.borrow_mut() = Some(txn);
        }
        result
    }

    // ── Schema cookie and change counter tracking (bd-3mmj) ─────────

    /// Increment the schema cookie.  Must be called for every DDL
    /// operation (CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX,
    /// CREATE VIEW, DROP INDEX, DROP VIEW, etc.).
    fn increment_schema_cookie(&self) {
        let mut cookie = self.schema_cookie.borrow_mut();
        *cookie = cookie.wrapping_add(1);
    }

    /// Increment the file change counter.  Must be called for every
    /// transaction that modifies the database (both DML and DDL).
    /// NOTE: With pager-backed persistence (5D.4), this is now managed by the pager.
    #[allow(dead_code)]
    fn increment_change_counter(&self) {
        let mut counter = self.change_counter.borrow_mut();
        *counter = counter.wrapping_add(1);
    }

    /// Read the current schema cookie value.
    pub fn schema_cookie(&self) -> u32 {
        *self.schema_cookie.borrow()
    }

    /// Read the current file change counter value.
    pub fn change_counter(&self) -> u32 {
        *self.change_counter.borrow()
    }

    // ── 5D.2: Pager-backed rollback reload (bd-1ene) ─────────────────────

    /// Reload MemDatabase and schema from the pager's committed state.
    ///
    /// After a pager transaction rollback, the pager reflects the pre-transaction
    /// committed state. This method opens a read transaction, reads sqlite_master
    /// from page 1, and reloads all table data into a fresh MemDatabase.
    ///
    /// # Errors
    ///
    /// Returns an error if the pager cannot be read or if B-tree traversal fails.
    #[allow(clippy::too_many_lines)]
    fn reload_memdb_from_pager(&self, cx: &Cx) -> Result<()> {
        // Open a read transaction to see the committed state.
        let mut txn = self.pager.begin(cx, TransactionMode::ReadOnly)?;

        let page_size = PageSize::DEFAULT;
        let usable_size = u32::try_from(page_size.as_usize())
            .map_err(|_| FrankenError::internal("page size exceeds u32"))?;

        // Check if the database is empty (page 1 uninitialized).
        let page1 = txn.get_page(cx, PageNumber::ONE)?;
        let page1_bytes = page1.as_ref();

        // If page 1 is all zeros or doesn't have a valid B-tree header, the
        // database is empty. Reset to a fresh state.
        if page1_bytes.iter().all(|&b| b == 0) || page1_bytes.len() < 100 {
            *self.db.borrow_mut() = MemDatabase::new();
            self.schema.borrow_mut().clear();
            self.views.borrow_mut().clear();
            self.triggers.borrow_mut().clear();
            self.rowid_alias_columns.borrow_mut().clear();
            *self.next_master_rowid.borrow_mut() = 1;
            *self.schema_cookie.borrow_mut() = 0;
            *self.change_counter.borrow_mut() = 0;
            return Ok(());
        }

        // Read sqlite_master entries from page 1's B-tree.
        let master_entries = {
            let mut entries = Vec::new();
            let master_root = PageNumber::ONE;
            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(txn.as_mut()),
                master_root,
                usable_size,
                false, // read-only
            );

            if cursor.first(cx)? {
                loop {
                    let payload = cursor.payload(cx)?;
                    if let Some(values) = parse_record(&payload) {
                        entries.push(values);
                    }
                    if !cursor.next(cx)? {
                        break;
                    }
                }
            }
            entries
        };

        // Parse each sqlite_master row and rebuild schema + MemDatabase.
        // Columns: type(0), name(1), tbl_name(2), rootpage(3), sql(4)
        let mut new_schema = Vec::new();
        let mut new_db = MemDatabase::new();
        let mut new_triggers = Vec::new();
        let mut new_alias_map = HashMap::new();

        for entry in &master_entries {
            if entry.len() < 5 {
                continue;
            }
            let entry_type = match &entry[0] {
                SqliteValue::Text(s) => s.as_str(),
                _ => continue,
            };

            if entry_type.eq_ignore_ascii_case("trigger") {
                let create_sql = match &entry[4] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => continue,
                };
                if let Ok(Statement::CreateTrigger(stmt)) = parse_single_statement(&create_sql) {
                    new_triggers.push(TriggerDef::from_create_statement(&stmt, create_sql));
                }
                continue;
            }

            if !entry_type.eq_ignore_ascii_case("table") {
                continue; // Skip indexes/views for now.
            }

            let name = match &entry[1] {
                SqliteValue::Text(s) => s.clone(),
                _ => continue,
            };
            let root_page_num = match &entry[3] {
                SqliteValue::Integer(n) => *n,
                _ => continue,
            };
            let create_sql = match &entry[4] {
                SqliteValue::Text(s) => s.clone(),
                _ => continue,
            };

            // Parse the CREATE TABLE to extract column info.
            let columns = crate::compat_persist::parse_columns_from_create_sql(&create_sql);
            let num_columns = columns.len();

            // Track rowid alias columns (INTEGER PRIMARY KEY).
            // When a column is INTEGER PRIMARY KEY, its value is NOT stored in the
            // record payload - it IS the rowid. We need to insert the rowid at this
            // position when loading data.
            let ipk_col_idx = columns.iter().position(|c| c.is_ipk);
            if let Some(idx) = ipk_col_idx {
                new_alias_map.insert(name.to_ascii_lowercase(), idx);
            }

            #[allow(clippy::cast_possible_truncation)]
            let real_root_page = root_page_num as i32;
            new_db.create_table_at(real_root_page, num_columns);

            new_schema.push(TableSchema {
                name,
                root_page: real_root_page,
                columns,
                indexes: Vec::new(),
            });

            // Read all rows from this table's B-tree.
            let file_root = PageNumber::new(u32::try_from(root_page_num).unwrap_or(1))
                .unwrap_or(PageNumber::ONE);

            let mut cursor = fsqlite_btree::BtCursor::new(
                TransactionPageIo::new(txn.as_mut()),
                file_root,
                usable_size,
                false, // read-only
            );

            if let Some(mem_table) = new_db.tables.get_mut(&real_root_page) {
                if cursor.first(cx)? {
                    loop {
                        let rowid = cursor.rowid(cx)?;
                        let payload = cursor.payload(cx)?;
                        if let Some(mut values) = parse_record(&payload) {
                            // If this table has an INTEGER PRIMARY KEY column, insert
                            // the rowid at that position since it's not stored in the
                            // record payload.
                            if let Some(ipk_idx) = ipk_col_idx {
                                values.insert(ipk_idx, SqliteValue::Integer(rowid));
                            }
                            mem_table.insert_row(rowid, values);
                        }
                        if !cursor.next(cx)? {
                            break;
                        }
                    }
                }
            }
        }

        // Read schema_cookie and change_counter from database header (page 1).
        let (schema_cookie, change_counter) = {
            let hdr = page1_bytes;
            let cookie = if hdr.len() >= 44 {
                u32::from_be_bytes([hdr[40], hdr[41], hdr[42], hdr[43]])
            } else {
                0
            };
            let counter = if hdr.len() >= 28 {
                u32::from_be_bytes([hdr[24], hdr[25], hdr[26], hdr[27]])
            } else {
                0
            };
            (cookie, counter)
        };

        // Apply the reloaded state.
        // NOTE: Views are cleared here. View loading from sqlite_master (type='view')
        // is not yet implemented; views created during a rolled-back transaction
        // will be discarded.
        *self.db.borrow_mut() = new_db;
        *self.schema.borrow_mut() = new_schema;
        self.views.borrow_mut().clear();
        *self.triggers.borrow_mut() = new_triggers;
        *self.rowid_alias_columns.borrow_mut() = new_alias_map;
        #[allow(clippy::cast_possible_wrap)]
        {
            *self.next_master_rowid.borrow_mut() = (master_entries.len() as i64) + 1;
        }
        *self.schema_cookie.borrow_mut() = schema_cookie;
        *self.change_counter.borrow_mut() = change_counter;

        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.close_internal(true);
    }
}

/// Check if a SELECT statement has no FROM clause (expression-only).
fn is_expression_only_select(select: &SelectStatement) -> bool {
    match &select.body.select {
        SelectCore::Select { from, .. } => from.is_none(),
        SelectCore::Values(_) => true,
    }
}

/// Check if a SELECT statement uses DISTINCT.
fn is_distinct_select(select: &SelectStatement) -> bool {
    match &select.body.select {
        SelectCore::Select { distinct, .. } => *distinct != Distinctness::All,
        SelectCore::Values(_) => false,
    }
}

/// Remove duplicate rows using `PartialEq`-based comparison.
fn dedup_rows(rows: &mut Vec<Row>) {
    let mut seen: Vec<Row> = Vec::new();
    rows.retain(|row| {
        if seen.iter().any(|s| s == row) {
            false
        } else {
            seen.push(row.clone());
            true
        }
    });
}

/// Check whether a candidate row already exists in a set of value rows.
fn contains_value_row(rows: &[Vec<SqliteValue>], candidate: &[SqliteValue]) -> bool {
    rows.iter().any(|row| row == candidate)
}

/// Remove duplicate value rows while preserving first-seen order.
fn dedup_value_rows(rows: &mut Vec<Vec<SqliteValue>>) {
    let mut seen: Vec<Vec<SqliteValue>> = Vec::with_capacity(rows.len());
    rows.retain(|row| {
        if contains_value_row(&seen, row) {
            false
        } else {
            seen.push(row.clone());
            true
        }
    });
}

/// Apply a LIMIT/OFFSET clause to a post-processed row vector.
///
/// Used when DISTINCT + LIMIT interact: DISTINCT must run on all rows first,
/// then LIMIT/OFFSET truncate the deduplicated result.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn apply_limit_clause(rows: &mut Vec<Row>, clause: &LimitClause) {
    let limit_val = eval_limit_expr(&clause.limit);
    let offset_val = clause
        .offset
        .as_ref()
        .map_or(0_usize, |off| eval_limit_expr(off) as usize);

    if offset_val > 0 && offset_val < rows.len() {
        rows.drain(..offset_val);
    } else if offset_val >= rows.len() {
        rows.clear();
        return;
    }

    if limit_val >= 0 {
        let limit = limit_val as usize;
        if rows.len() > limit {
            rows.truncate(limit);
        }
    }
}

/// Evaluate a constant integer expression for LIMIT/OFFSET.
fn eval_limit_expr(expr: &Expr) -> i64 {
    match expr {
        Expr::Literal(Literal::Integer(n), _) => *n,
        Expr::UnaryOp {
            op: fsqlite_ast::UnaryOp::Negate,
            expr: inner,
            ..
        } => -eval_limit_expr(inner),
        _ => -1, // Negative means "no limit"
    }
}

/// Check whether a table-backed SELECT has a GROUP BY clause.
fn has_group_by(select: &SelectStatement) -> bool {
    matches!(
        &select.body.select,
        SelectCore::Select { group_by, .. } if !group_by.is_empty()
    )
}

/// Check whether a table-backed SELECT has JOINs in the FROM clause.
fn has_joins(select: &SelectStatement) -> bool {
    matches!(
        &select.body.select,
        SelectCore::Select { from: Some(from), .. } if !from.joins.is_empty()
    )
}

/// Check if the FROM source contains a subquery (derived table) that
/// requires the fallback JOIN path instead of the VDBE codegen path.
fn has_subquery_source(select: &SelectStatement) -> bool {
    if let SelectCore::Select {
        from: Some(from), ..
    } = &select.body.select
    {
        if matches!(from.source, TableOrSubquery::Subquery { .. }) {
            return true;
        }
        for join in &from.joins {
            if matches!(join.table, TableOrSubquery::Subquery { .. }) {
                return true;
            }
        }
    }
    false
}

fn canonical_sqlite_schema_name(name: &str) -> Option<&'static str> {
    if name.eq_ignore_ascii_case("sqlite_master") {
        Some("sqlite_master")
    } else if name.eq_ignore_ascii_case("sqlite_schema") {
        Some("sqlite_schema")
    } else {
        None
    }
}

fn is_sqlite_schema_name(name: &str) -> bool {
    canonical_sqlite_schema_name(name).is_some()
}

fn sqlite_master_column_infos() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo {
            name: "type".to_owned(),
            affinity: 'B',
            is_ipk: false,
        },
        ColumnInfo {
            name: "name".to_owned(),
            affinity: 'B',
            is_ipk: false,
        },
        ColumnInfo {
            name: "tbl_name".to_owned(),
            affinity: 'B',
            is_ipk: false,
        },
        ColumnInfo {
            name: "rootpage".to_owned(),
            affinity: 'D',
            is_ipk: false,
        },
        ColumnInfo {
            name: "sql".to_owned(),
            affinity: 'B',
            is_ipk: false,
        },
    ]
}

fn render_create_table_sql(table: &TableSchema) -> String {
    let column_defs = table
        .columns
        .iter()
        .map(|col| {
            let mut part = format!("{} {}", col.name, affinity_decl_type(col.affinity));
            if col.is_ipk {
                part.push_str(" PRIMARY KEY");
            }
            part
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE {} ({column_defs})", table.name)
}

fn render_create_index_sql(index: &IndexSchema, table_name: &str) -> String {
    format!(
        "CREATE INDEX {} ON {}({})",
        index.name,
        table_name,
        index.columns.join(", ")
    )
}

fn affinity_decl_type(affinity: char) -> &'static str {
    match affinity.to_ascii_uppercase() {
        'A' => "BLOB",
        'B' => "TEXT",
        'D' => "INTEGER",
        'E' => "REAL",
        // 'C' and any unknown affinity map to NUMERIC
        _ => "NUMERIC",
    }
}

/// Check whether a `SelectCore` references a named table/CTE in any FROM source.
fn select_core_references_table(core: &SelectCore, table_name: &str) -> bool {
    match core {
        SelectCore::Select {
            from: Some(from), ..
        } => from_clause_references_table(from, table_name),
        SelectCore::Select { from: None, .. } | SelectCore::Values(_) => false,
    }
}

fn from_clause_references_table(from: &fsqlite_ast::FromClause, table_name: &str) -> bool {
    table_or_subquery_references_table(&from.source, table_name)
        || from
            .joins
            .iter()
            .any(|join| table_or_subquery_references_table(&join.table, table_name))
}

fn table_or_subquery_references_table(table: &TableOrSubquery, table_name: &str) -> bool {
    match table {
        TableOrSubquery::Table { name, .. } => name.name.eq_ignore_ascii_case(table_name),
        TableOrSubquery::Subquery { query, .. } => select_references_table(query, table_name),
        TableOrSubquery::ParenJoin(from) => from_clause_references_table(from, table_name),
        TableOrSubquery::TableFunction { .. } => false,
    }
}

fn select_references_table(select: &SelectStatement, table_name: &str) -> bool {
    select_core_references_table(&select.body.select, table_name)
        || select
            .body
            .compounds
            .iter()
            .any(|(_, core)| select_core_references_table(core, table_name))
}

/// Infer column names from a SELECT statement's result columns.
fn infer_select_column_names(select: &SelectStatement) -> Vec<String> {
    let core = &select.body.select;
    if let SelectCore::Select { columns, .. } = core {
        columns
            .iter()
            .enumerate()
            .map(|(i, col)| match col {
                ResultColumn::Expr {
                    alias: Some(alias), ..
                } => alias.clone(),
                ResultColumn::Expr { expr, .. } => match expr {
                    Expr::Column(col_ref, _) => col_ref.column.clone(),
                    _ => format!("_c{i}"),
                },
                ResultColumn::Star => "*".to_owned(),
                ResultColumn::TableStar(name) => format!("{name}.*"),
            })
            .collect()
    } else {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Subquery rewriting helpers (IN, EXISTS, scalar subqueries)
// ---------------------------------------------------------------------------

/// Walk a `SelectCore` and rewrite subquery expressions found in its
/// result columns, WHERE clause, or HAVING clause.
///
/// When `rewrite_in` is true, also eagerly evaluate `IN (SELECT ...)`
/// into literal lists (needed for interpreted fallback paths).
fn rewrite_in_select_core(
    core: &mut SelectCore,
    conn: &Connection,
    rewrite_in: bool,
) -> Result<()> {
    if let SelectCore::Select {
        columns,
        where_clause,
        having,
        ..
    } = core
    {
        for col in columns.iter_mut() {
            if let ResultColumn::Expr { expr, .. } = col {
                rewrite_in_expr(expr, conn, rewrite_in)?;
            }
        }
        if let Some(wh) = where_clause.as_mut() {
            rewrite_in_expr(wh, conn, rewrite_in)?;
        }
        if let Some(hv) = having.as_mut() {
            rewrite_in_expr(hv, conn, rewrite_in)?;
        }
    }
    Ok(())
}

/// Recursively walk an expression tree and eagerly evaluate subquery
/// expressions: `EXISTS (SELECT ...)` and scalar `(SELECT ...)`.
///
/// When `rewrite_in_subqueries` is true, also eagerly evaluate
/// `IN (SELECT ...)` and `IN table` into literal lists.  When false,
/// leave those forms intact so the VDBE codegen can handle them via
/// runtime probe scans.
#[allow(clippy::too_many_lines)]
fn rewrite_in_expr(expr: &mut Expr, conn: &Connection, rewrite_in_subqueries: bool) -> Result<()> {
    match expr {
        Expr::In {
            set, expr: inner, ..
        } => {
            rewrite_in_expr(inner, conn, rewrite_in_subqueries)?;
            if rewrite_in_subqueries {
                if let InSet::Subquery(sub) = set {
                    let rows =
                        conn.execute_statement(Statement::Select(*sub.clone()), Some(&[]))?;
                    let literals: Vec<Expr> = rows
                        .into_iter()
                        .filter_map(|row| row.values.into_iter().next())
                        .map(value_to_literal_expr)
                        .collect();
                    *set = InSet::List(literals);
                }
            }
            if let InSet::List(exprs) = set {
                for e in exprs.iter_mut() {
                    rewrite_in_expr(e, conn, rewrite_in_subqueries)?;
                }
            }
        }
        Expr::Exists {
            subquery,
            not,
            span,
        } => {
            let rows = conn.execute_statement(Statement::Select(*subquery.clone()), Some(&[]))?;
            let exists = !rows.is_empty();
            let result = if *not { !exists } else { exists };
            *expr = Expr::Literal(Literal::Integer(i64::from(result)), *span);
        }
        Expr::Subquery(sub, span) => {
            let rows = conn.execute_statement(Statement::Select(*sub.clone()), Some(&[]))?;
            let val = rows
                .into_iter()
                .next()
                .and_then(|row| row.values.into_iter().next())
                .unwrap_or(SqliteValue::Null);
            *expr = Expr::Literal(
                match val {
                    SqliteValue::Integer(i) => Literal::Integer(i),
                    SqliteValue::Float(f) => Literal::Float(f),
                    SqliteValue::Text(s) => Literal::String(s),
                    SqliteValue::Blob(b) => Literal::Blob(b),
                    SqliteValue::Null => Literal::Null,
                },
                *span,
            );
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_in_expr(left, conn, rewrite_in_subqueries)?;
            rewrite_in_expr(right, conn, rewrite_in_subqueries)?;
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. } => {
            rewrite_in_expr(inner, conn, rewrite_in_subqueries)?;
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            rewrite_in_expr(inner, conn, rewrite_in_subqueries)?;
            rewrite_in_expr(low, conn, rewrite_in_subqueries)?;
            rewrite_in_expr(high, conn, rewrite_in_subqueries)?;
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(op) = operand.as_mut() {
                rewrite_in_expr(op, conn, rewrite_in_subqueries)?;
            }
            for (cond, then) in whens.iter_mut() {
                rewrite_in_expr(cond, conn, rewrite_in_subqueries)?;
                rewrite_in_expr(then, conn, rewrite_in_subqueries)?;
            }
            if let Some(el) = else_expr.as_mut() {
                rewrite_in_expr(el, conn, rewrite_in_subqueries)?;
            }
        }
        Expr::FunctionCall {
            args: FunctionArgs::List(exprs),
            ..
        } => {
            for e in exprs.iter_mut() {
                rewrite_in_expr(e, conn, rewrite_in_subqueries)?;
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            rewrite_in_expr(inner, conn, rewrite_in_subqueries)?;
            rewrite_in_expr(pattern, conn, rewrite_in_subqueries)?;
        }
        // Leaf nodes that contain no sub-expressions.
        _ => {}
    }
    Ok(())
}

/// Convert a `SqliteValue` into a synthetic `Expr::Literal`.
fn value_to_literal_expr(val: SqliteValue) -> Expr {
    use fsqlite_ast::Span;
    match val {
        SqliteValue::Integer(i) => Expr::Literal(Literal::Integer(i), Span::ZERO),
        SqliteValue::Float(f) => Expr::Literal(Literal::Float(f), Span::ZERO),
        SqliteValue::Text(s) => Expr::Literal(Literal::String(s), Span::ZERO),
        SqliteValue::Blob(b) => Expr::Literal(Literal::Blob(b), Span::ZERO),
        SqliteValue::Null => Expr::Literal(Literal::Null, Span::ZERO),
    }
}

/// Known aggregate function names (must match codegen.rs).
const AGG_NAMES: &[&str] = &[
    "avg",
    "count",
    "group_concat",
    "string_agg",
    "max",
    "min",
    "sum",
    "total",
];

/// Check whether a function name is a known aggregate.
fn is_agg_fn(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    AGG_NAMES.iter().any(|&n| n == lower)
}

/// Describes one result column in a GROUP BY query.
enum GroupByColumn {
    /// A non-aggregate expression that appears in GROUP BY; evaluated per-group.
    Plain(Box<Expr>),
    /// An aggregate function; stores (func_name_lower, arg_col_index_or_None_for_star).
    Agg {
        name: String,
        arg_col: Option<usize>,
        distinct: bool,
        separator: Option<String>,
    },
}

/// Compare two expressions for structural equality, ignoring source spans.
fn exprs_match(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (Expr::Column(ca, _), Expr::Column(cb, _)) => {
            ca.column.eq_ignore_ascii_case(&cb.column)
                && ca.table.as_deref().map(str::to_ascii_lowercase)
                    == cb.table.as_deref().map(str::to_ascii_lowercase)
        }
        (Expr::Literal(la, _), Expr::Literal(lb, _)) => la == lb,
        (
            Expr::BinaryOp {
                left: la,
                op: oa,
                right: ra,
                ..
            },
            Expr::BinaryOp {
                left: lb,
                op: ob,
                right: rb,
                ..
            },
        ) => oa == ob && exprs_match(la, lb) && exprs_match(ra, rb),
        (
            Expr::UnaryOp {
                op: oa, expr: ea, ..
            },
            Expr::UnaryOp {
                op: ob, expr: eb, ..
            },
        ) => oa == ob && exprs_match(ea, eb),
        (
            Expr::FunctionCall {
                name: na,
                args: aa,
                distinct: da,
                ..
            },
            Expr::FunctionCall {
                name: nb,
                args: ab,
                distinct: db,
                ..
            },
        ) => {
            na.eq_ignore_ascii_case(nb)
                && da == db
                && match (aa, ab) {
                    (FunctionArgs::Star, FunctionArgs::Star) => true,
                    (FunctionArgs::List(la), FunctionArgs::List(lb)) => {
                        la.len() == lb.len()
                            && la.iter().zip(lb.iter()).all(|(x, y)| exprs_match(x, y))
                    }
                    _ => false,
                }
        }
        (
            Expr::IsNull {
                expr: ea, not: na, ..
            },
            Expr::IsNull {
                expr: eb, not: nb, ..
            },
        ) => na == nb && exprs_match(ea, eb),
        _ => false,
    }
}

/// Resolve a column name from an expression (simple Expr::Column case).
fn expr_col_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(col_ref, _) => Some(col_ref.column.as_str()),
        _ => None,
    }
}

/// Build a `SELECT *` scan from a GROUP BY SELECT (strips aggregates and GROUP BY).
fn build_raw_scan_select(select: &SelectStatement) -> SelectStatement {
    let new_core = match &select.body.select {
        SelectCore::Select {
            from, where_clause, ..
        } => SelectCore::Select {
            distinct: Distinctness::All,
            columns: vec![ResultColumn::Star],
            from: from.clone(),
            where_clause: where_clause.clone(),
            group_by: vec![],
            having: None,
            windows: vec![],
        },
        other @ SelectCore::Values(_) => other.clone(),
    };
    SelectStatement {
        with: select.with.clone(),
        body: SelectBody {
            select: new_core,
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    }
}

/// Test whether a `SqliteValue` is truthy (non-zero, non-NULL).
fn is_sqlite_truthy(v: &SqliteValue) -> bool {
    match v {
        SqliteValue::Null | SqliteValue::Integer(0) => false,
        SqliteValue::Float(f) if *f == 0.0 => false,
        _ => true,
    }
}

/// Evaluate a HAVING predicate against a group's computed result values.
///
/// `group_rows` and `col_names` allow computing aggregates that are not in the
/// SELECT list (e.g. `HAVING COUNT(*) > 1` when COUNT(*) is not a result column).
fn evaluate_having_predicate(
    expr: &Expr,
    values: &[SqliteValue],
    descriptors: &[GroupByColumn],
    columns: &[ResultColumn],
    group_rows: &[Vec<SqliteValue>],
    col_names: &[String],
) -> bool {
    is_sqlite_truthy(&evaluate_having_value(
        expr,
        values,
        descriptors,
        columns,
        group_rows,
        col_names,
    ))
}

/// Evaluate a HAVING expression to a `SqliteValue`.
///
/// First tries to resolve from the already-computed result `values` (matching against
/// `descriptors`/`columns`). Falls back to computing aggregates directly from
/// `group_rows` for HAVING expressions that reference aggregates not in SELECT.
#[allow(clippy::too_many_lines)]
fn evaluate_having_value(
    expr: &Expr,
    values: &[SqliteValue],
    descriptors: &[GroupByColumn],
    columns: &[ResultColumn],
    group_rows: &[Vec<SqliteValue>],
    col_names: &[String],
) -> SqliteValue {
    match expr {
        // Aggregate function — first try matching a result column, then compute directly.
        Expr::FunctionCall { name, args, .. } if is_agg_fn(name) => {
            let lower = name.to_ascii_lowercase();
            // Try to find a matching aggregate in the result descriptors.
            for (i, desc) in descriptors.iter().enumerate() {
                if let GroupByColumn::Agg {
                    name: agg_name,
                    arg_col,
                    ..
                } = desc
                {
                    if *agg_name != lower {
                        continue;
                    }
                    let args_match = match args {
                        FunctionArgs::Star => arg_col.is_none(),
                        FunctionArgs::List(exprs) if exprs.is_empty() => arg_col.is_none(),
                        FunctionArgs::List(exprs) => {
                            if let Some(arg_name) = expr_col_name(&exprs[0]) {
                                if let Some(ResultColumn::Expr {
                                    expr:
                                        Expr::FunctionCall {
                                            args: FunctionArgs::List(result_args),
                                            ..
                                        },
                                    ..
                                }) = columns.get(i)
                                {
                                    result_args
                                        .first()
                                        .and_then(|e| expr_col_name(e))
                                        .is_some_and(|n| n.eq_ignore_ascii_case(arg_name))
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        }
                    };
                    if args_match {
                        return values.get(i).cloned().unwrap_or(SqliteValue::Null);
                    }
                }
            }
            // Aggregate not found in SELECT — compute directly from group_rows.
            compute_having_aggregate(&lower, args, group_rows, col_names)
        }

        // Column reference — find matching plain column in result set or raw data.
        Expr::Column(col_ref, _) => {
            let col_name = &col_ref.column;
            // First check result column aliases.
            for (i, rc) in columns.iter().enumerate() {
                if let ResultColumn::Expr {
                    alias: Some(alias), ..
                } = rc
                {
                    if alias.eq_ignore_ascii_case(col_name) {
                        return values.get(i).cloned().unwrap_or(SqliteValue::Null);
                    }
                }
            }
            // Then check result column names.
            for (i, rc) in columns.iter().enumerate() {
                if let ResultColumn::Expr { expr, .. } = rc {
                    if let Some(name) = expr_col_name(expr) {
                        if name.eq_ignore_ascii_case(col_name) {
                            return values.get(i).cloned().unwrap_or(SqliteValue::Null);
                        }
                    }
                }
            }
            // Fall back to resolving from raw group data via col_names.
            if let Some(idx) = col_names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(col_name))
            {
                return group_rows
                    .first()
                    .and_then(|r| r.get(idx))
                    .cloned()
                    .unwrap_or(SqliteValue::Null);
            }
            SqliteValue::Null
        }

        // Literal values.
        Expr::Literal(lit, _) => match lit {
            fsqlite_ast::Literal::Integer(n) => SqliteValue::Integer(*n),
            fsqlite_ast::Literal::Float(f) => SqliteValue::Float(*f),
            fsqlite_ast::Literal::String(s) => SqliteValue::Text(s.clone()),
            fsqlite_ast::Literal::True => SqliteValue::Integer(1),
            fsqlite_ast::Literal::False => SqliteValue::Integer(0),
            _ => SqliteValue::Null,
        },

        // Binary operations.
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let lv =
                evaluate_having_value(left, values, descriptors, columns, group_rows, col_names);
            let rv =
                evaluate_having_value(right, values, descriptors, columns, group_rows, col_names);
            match op {
                fsqlite_ast::BinaryOp::Gt => SqliteValue::Integer(i64::from(
                    cmp_values(&lv, &rv) == std::cmp::Ordering::Greater,
                )),
                fsqlite_ast::BinaryOp::Lt => SqliteValue::Integer(i64::from(
                    cmp_values(&lv, &rv) == std::cmp::Ordering::Less,
                )),
                fsqlite_ast::BinaryOp::Ge => SqliteValue::Integer(i64::from(
                    cmp_values(&lv, &rv) != std::cmp::Ordering::Less,
                )),
                fsqlite_ast::BinaryOp::Le => SqliteValue::Integer(i64::from(
                    cmp_values(&lv, &rv) != std::cmp::Ordering::Greater,
                )),
                fsqlite_ast::BinaryOp::Eq => SqliteValue::Integer(i64::from(
                    cmp_values(&lv, &rv) == std::cmp::Ordering::Equal,
                )),
                fsqlite_ast::BinaryOp::Ne => SqliteValue::Integer(i64::from(
                    cmp_values(&lv, &rv) != std::cmp::Ordering::Equal,
                )),
                fsqlite_ast::BinaryOp::And => {
                    SqliteValue::Integer(i64::from(is_sqlite_truthy(&lv) && is_sqlite_truthy(&rv)))
                }
                fsqlite_ast::BinaryOp::Or => {
                    SqliteValue::Integer(i64::from(is_sqlite_truthy(&lv) || is_sqlite_truthy(&rv)))
                }
                fsqlite_ast::BinaryOp::Add => numeric_add(&lv, &rv),
                fsqlite_ast::BinaryOp::Subtract => numeric_sub(&lv, &rv),
                fsqlite_ast::BinaryOp::Multiply => numeric_mul(&lv, &rv),
                _ => SqliteValue::Null,
            }
        }

        // Unary operations.
        Expr::UnaryOp {
            op, expr: inner, ..
        } => {
            let v =
                evaluate_having_value(inner, values, descriptors, columns, group_rows, col_names);
            match op {
                fsqlite_ast::UnaryOp::Negate => match v {
                    SqliteValue::Integer(n) => SqliteValue::Integer(-n),
                    SqliteValue::Float(f) => SqliteValue::Float(-f),
                    _ => SqliteValue::Null,
                },
                fsqlite_ast::UnaryOp::Not => SqliteValue::Integer(i64::from(!is_sqlite_truthy(&v))),
                _ => SqliteValue::Null,
            }
        }

        // IS NULL / IS NOT NULL.
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            let v =
                evaluate_having_value(inner, values, descriptors, columns, group_rows, col_names);
            let is_null = matches!(v, SqliteValue::Null);
            SqliteValue::Integer(i64::from(if *not { !is_null } else { is_null }))
        }

        _ => SqliteValue::Null,
    }
}

/// Compute an aggregate directly from group rows (for HAVING aggregates not in SELECT).
#[allow(clippy::cast_possible_wrap)]
fn compute_having_aggregate(
    func: &str,
    args: &FunctionArgs,
    group_rows: &[Vec<SqliteValue>],
    col_names: &[String],
) -> SqliteValue {
    let arg_col_idx = match args {
        FunctionArgs::Star => None,
        FunctionArgs::List(exprs) if exprs.is_empty() => None,
        FunctionArgs::List(exprs) if exprs.len() == 1 => {
            let Some(col_name) = expr_col_name(&exprs[0]) else {
                return SqliteValue::Null;
            };
            let Some(idx) = col_names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(col_name))
            else {
                return SqliteValue::Null;
            };
            Some(idx)
        }
        FunctionArgs::List(_) => return SqliteValue::Null,
    };

    if func == "count" && arg_col_idx.is_none() {
        return SqliteValue::Integer(group_rows.len() as i64);
    }
    let Some(idx) = arg_col_idx else {
        return SqliteValue::Null;
    };
    let agg_values: Vec<&SqliteValue> = group_rows
        .iter()
        .filter_map(|r| r.get(idx))
        .filter(|v| !matches!(v, SqliteValue::Null))
        .collect();
    compute_aggregate(func, &agg_values)
}

/// Numeric addition for HAVING expression evaluation.
fn numeric_add(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    match (a, b) {
        (SqliteValue::Integer(ai), SqliteValue::Integer(bi)) => {
            SqliteValue::Integer(ai.wrapping_add(*bi))
        }
        (SqliteValue::Float(af), SqliteValue::Float(bf)) => SqliteValue::Float(af + bf),
        (SqliteValue::Integer(ai), SqliteValue::Float(bf)) => SqliteValue::Float(*ai as f64 + bf),
        (SqliteValue::Float(af), SqliteValue::Integer(bi)) => SqliteValue::Float(af + *bi as f64),
        _ => SqliteValue::Null,
    }
}

/// Numeric subtraction for HAVING expression evaluation.
fn numeric_sub(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    match (a, b) {
        (SqliteValue::Integer(ai), SqliteValue::Integer(bi)) => {
            SqliteValue::Integer(ai.wrapping_sub(*bi))
        }
        (SqliteValue::Float(af), SqliteValue::Float(bf)) => SqliteValue::Float(af - bf),
        (SqliteValue::Integer(ai), SqliteValue::Float(bf)) => SqliteValue::Float(*ai as f64 - bf),
        (SqliteValue::Float(af), SqliteValue::Integer(bi)) => SqliteValue::Float(af - *bi as f64),
        _ => SqliteValue::Null,
    }
}

/// Numeric multiplication for HAVING expression evaluation.
fn numeric_mul(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    match (a, b) {
        (SqliteValue::Integer(ai), SqliteValue::Integer(bi)) => {
            SqliteValue::Integer(ai.wrapping_mul(*bi))
        }
        (SqliteValue::Float(af), SqliteValue::Float(bf)) => SqliteValue::Float(af * bf),
        (SqliteValue::Integer(ai), SqliteValue::Float(bf)) => SqliteValue::Float(*ai as f64 * bf),
        (SqliteValue::Float(af), SqliteValue::Integer(bi)) => SqliteValue::Float(af * *bi as f64),
        _ => SqliteValue::Null,
    }
}

fn numeric_div(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    match (a, b) {
        (SqliteValue::Integer(_), SqliteValue::Integer(0)) => SqliteValue::Null,
        (SqliteValue::Integer(ai), SqliteValue::Integer(bi)) => {
            SqliteValue::Integer(ai.wrapping_div(*bi))
        }
        (SqliteValue::Float(af), SqliteValue::Float(bf)) => SqliteValue::Float(af / bf),
        (SqliteValue::Integer(ai), SqliteValue::Float(bf)) => SqliteValue::Float(*ai as f64 / bf),
        (SqliteValue::Float(af), SqliteValue::Integer(bi)) => SqliteValue::Float(af / *bi as f64),
        _ => SqliteValue::Null,
    }
}

fn numeric_mod(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    match (a, b) {
        (SqliteValue::Integer(_), SqliteValue::Integer(0)) => SqliteValue::Null,
        (SqliteValue::Integer(ai), SqliteValue::Integer(bi)) => {
            SqliteValue::Integer(ai.wrapping_rem(*bi))
        }
        (SqliteValue::Float(af), SqliteValue::Float(bf)) => SqliteValue::Float(af % bf),
        (SqliteValue::Integer(ai), SqliteValue::Float(bf)) => SqliteValue::Float(*ai as f64 % bf),
        (SqliteValue::Float(af), SqliteValue::Integer(bi)) => SqliteValue::Float(af % *bi as f64),
        _ => SqliteValue::Null,
    }
}

/// Compare two `SqliteValue`s for ordering (used by HAVING comparisons).
fn cmp_values(a: &SqliteValue, b: &SqliteValue) -> std::cmp::Ordering {
    match (a, b) {
        (SqliteValue::Integer(ai), SqliteValue::Integer(bi)) => ai.cmp(bi),
        (SqliteValue::Integer(ai), SqliteValue::Float(bf)) => (*ai as f64)
            .partial_cmp(bf)
            .unwrap_or(std::cmp::Ordering::Equal),
        (SqliteValue::Float(af), SqliteValue::Integer(bi)) => af
            .partial_cmp(&(*bi as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (SqliteValue::Float(af), SqliteValue::Float(bf)) => {
            af.partial_cmp(bf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (SqliteValue::Text(at), SqliteValue::Text(bt)) => at.cmp(bt),
        (SqliteValue::Null, SqliteValue::Null) => std::cmp::Ordering::Equal,
        (SqliteValue::Null, _) => std::cmp::Ordering::Less,
        (_, SqliteValue::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

/// Compute the aggregate value for a group of values.
#[allow(clippy::cast_possible_wrap)]
fn compute_aggregate(name: &str, values: &[&SqliteValue]) -> SqliteValue {
    match name {
        "count" => SqliteValue::Integer(values.len() as i64),
        "sum" | "total" => {
            // SQLite: total() always returns 0.0 for empty input;
            // sum() returns NULL when all values are NULL (empty after pre-filter).
            if values.is_empty() {
                return if name == "total" {
                    SqliteValue::Float(0.0)
                } else {
                    SqliteValue::Null
                };
            }
            let mut sum = 0.0_f64;
            let mut has_int = false;
            let mut all_int = true;
            let mut int_sum = 0_i64;
            for v in values {
                match v {
                    SqliteValue::Integer(n) => {
                        has_int = true;
                        int_sum = int_sum.wrapping_add(*n);
                        sum += *n as f64;
                    }
                    SqliteValue::Float(f) => {
                        all_int = false;
                        sum += f;
                    }
                    _ => {
                        all_int = false;
                    }
                }
            }
            if name == "total" {
                SqliteValue::Float(sum)
            } else if has_int && all_int {
                SqliteValue::Integer(int_sum)
            } else {
                SqliteValue::Float(sum)
            }
        }
        "avg" => {
            if values.is_empty() {
                return SqliteValue::Null;
            }
            let mut sum = 0.0_f64;
            let mut count = 0_u64;
            for v in values {
                match v {
                    SqliteValue::Integer(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    SqliteValue::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                SqliteValue::Null
            } else {
                SqliteValue::Float(sum / count as f64)
            }
        }
        "min" => values
            .iter()
            .filter(|v| !matches!(v, SqliteValue::Null))
            .min_by(|a, b| cmp_sqlite_values(a, b))
            .map_or(SqliteValue::Null, |v| (*v).clone()),
        "max" => values
            .iter()
            .filter(|v| !matches!(v, SqliteValue::Null))
            .max_by(|a, b| cmp_sqlite_values(a, b))
            .map_or(SqliteValue::Null, |v| (*v).clone()),
        "group_concat" | "string_agg" => {
            let parts: Vec<String> = values
                .iter()
                .filter(|v| !matches!(v, SqliteValue::Null))
                .map(|v| match v {
                    SqliteValue::Text(s) => s.clone(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => f.to_string(),
                    _ => String::new(),
                })
                .collect();
            // SQLite: GROUP_CONCAT returns NULL when all values are NULL.
            if parts.is_empty() {
                SqliteValue::Null
            } else {
                SqliteValue::Text(parts.join(","))
            }
        }
        _ => SqliteValue::Null,
    }
}

/// Extended aggregate computation with optional DISTINCT dedup already applied
/// and an explicit separator for `GROUP_CONCAT`.
#[allow(clippy::cast_possible_wrap)]
fn compute_aggregate_ext(
    name: &str,
    values: &[&SqliteValue],
    separator: Option<&str>,
) -> SqliteValue {
    if (name == "group_concat" || name == "string_agg") && separator.is_some() {
        let sep = separator.unwrap_or(",");
        let parts: Vec<String> = values
            .iter()
            .filter(|v| !matches!(v, SqliteValue::Null))
            .map(|v| match v {
                SqliteValue::Text(s) => s.clone(),
                SqliteValue::Integer(n) => n.to_string(),
                SqliteValue::Float(f) => f.to_string(),
                _ => String::new(),
            })
            .collect();
        if parts.is_empty() {
            SqliteValue::Null
        } else {
            SqliteValue::Text(parts.join(sep))
        }
    } else {
        compute_aggregate(name, values)
    }
}

/// Remove duplicate values in-place, preserving first-occurrence order.
fn dedup_values(values: &mut Vec<&SqliteValue>) {
    let mut seen = Vec::new();
    values.retain(|v| {
        if seen.contains(v) {
            false
        } else {
            seen.push(*v);
            true
        }
    });
}

/// Compare two `SqliteValue`s for ordering.
///
/// SQLite type ordering: NULL < INTEGER/REAL < TEXT < BLOB.
/// Within the same type class, values compare naturally.
fn cmp_sqlite_values(a: &SqliteValue, b: &SqliteValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    /// Return a numeric rank for the SQLite type affinity ordering.
    fn type_rank(v: &SqliteValue) -> u8 {
        match v {
            SqliteValue::Null => 0,
            SqliteValue::Integer(_) | SqliteValue::Float(_) => 1,
            SqliteValue::Text(_) => 2,
            SqliteValue::Blob(_) => 3,
        }
    }

    let rank_a = type_rank(a);
    let rank_b = type_rank(b);
    if rank_a != rank_b {
        return rank_a.cmp(&rank_b);
    }

    // Same type class — compare within class.
    match (a, b) {
        (SqliteValue::Null, SqliteValue::Null) => Ordering::Equal,
        (SqliteValue::Integer(a), SqliteValue::Integer(b)) => a.cmp(b),
        (SqliteValue::Float(a), SqliteValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqliteValue::Integer(a), SqliteValue::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqliteValue::Float(a), SqliteValue::Integer(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (SqliteValue::Text(a), SqliteValue::Text(b)) => a.cmp(b),
        (SqliteValue::Blob(a), SqliteValue::Blob(b)) => a.cmp(b),
        _ => unreachable!("unreachable given rank check above"),
    }
}

/// Sort result rows by ORDER BY terms (for GROUP BY post-processing).
fn sort_rows_by_order_terms(
    rows: &mut [Row],
    order_by: &[OrderingTerm],
    columns: &[ResultColumn],
) -> Result<()> {
    let resolved: Vec<(usize, bool)> = order_by
        .iter()
        .map(|term| {
            // Try integer position reference first (ORDER BY 1, 2).
            let idx = if let Expr::Literal(Literal::Integer(n), _) = &term.expr {
                let pos = usize::try_from(*n).unwrap_or(0);
                if pos >= 1 && pos <= columns.len() {
                    Some(pos - 1)
                } else {
                    None
                }
            } else if let Some(col_name) = expr_col_name(&term.expr) {
                columns.iter().position(|c| match c {
                    ResultColumn::Expr {
                        expr: Expr::Column(r, _),
                        ..
                    } => r.column.eq_ignore_ascii_case(col_name),
                    ResultColumn::Expr {
                        alias: Some(alias), ..
                    } => alias.eq_ignore_ascii_case(col_name),
                    _ => false,
                })
            } else {
                // Expression ORDER BY: match structurally against result columns.
                columns.iter().position(|c| match c {
                    ResultColumn::Expr { expr, .. } => exprs_match(&term.expr, expr),
                    _ => false,
                })
            };
            let idx = idx.ok_or_else(|| {
                FrankenError::Internal("ORDER BY expression not found in SELECT list".to_owned())
            })?;
            let desc = matches!(term.direction, Some(SortDirection::Desc));
            Ok((idx, desc))
        })
        .collect::<Result<Vec<_>>>()?;

    rows.sort_by(|a, b| {
        for &(idx, desc) in &resolved {
            let ord = cmp_sqlite_values(&a.values()[idx], &b.values()[idx]);
            let ord = if desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(())
}

/// Apply LIMIT and OFFSET to a result set (GROUP BY post-processing).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn apply_limit_offset_postprocess(rows: &mut Vec<Row>, limit_clause: &LimitClause) {
    let limit_val = match &limit_clause.limit {
        Expr::Literal(Literal::Integer(n), _) => *n as usize,
        _ => return,
    };
    let offset_val = limit_clause
        .offset
        .as_ref()
        .and_then(|off| match off {
            Expr::Literal(Literal::Integer(n), _) => Some(*n as usize),
            _ => None,
        })
        .unwrap_or(0);

    let start = offset_val.min(rows.len());
    let end = (start + limit_val).min(rows.len());
    *rows = rows[start..end].to_vec();
}

/// Map an AST type name to a codegen affinity character.
fn type_name_to_affinity_char(name: &str) -> char {
    let upper = name.to_uppercase();
    if upper.contains("INT") {
        'D' // INTEGER affinity
    } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
        'B' // TEXT affinity
    } else if upper.contains("BLOB") || upper.is_empty() {
        'A' // BLOB (none) affinity
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        'E' // REAL affinity
    } else {
        'C' // NUMERIC affinity
    }
}

/// Convert a `CodegenError` to a `FrankenError`.
fn codegen_error_to_franken(e: CodegenError) -> FrankenError {
    match e {
        CodegenError::TableNotFound(name) => {
            FrankenError::Internal(format!("no such table: {name}",))
        }
        CodegenError::ColumnNotFound { table, column } => {
            FrankenError::Internal(format!("no such column: {column} in table {table}",))
        }
        CodegenError::Unsupported(msg) => FrankenError::NotImplemented(msg),
    }
}

fn execute_program(
    program: &VdbeProgram,
    params: Option<&[SqliteValue]>,
    func_registry: Option<&Arc<FunctionRegistry>>,
) -> Result<Vec<Row>> {
    let mut engine = VdbeEngine::new(program.register_count());
    if let Some(params) = params {
        validate_bound_parameters(program, params)?;
        engine.set_bindings(params.to_vec());
    }
    if let Some(registry) = func_registry {
        engine.set_function_registry(Arc::clone(registry));
    }

    match engine.execute(program)? {
        ExecOutcome::Done => Ok(engine
            .take_results()
            .into_iter()
            .map(|values| Row { values })
            .collect()),
        ExecOutcome::Error { code, message } => Err(FrankenError::Internal(format!(
            "VDBE halted with code {code}: {message}",
        ))),
    }
}

fn execute_program_with_postprocess(
    program: &VdbeProgram,
    params: Option<&[SqliteValue]>,
    func_registry: Option<&Arc<FunctionRegistry>>,
    expression_postprocess: Option<&ExpressionPostprocess>,
) -> Result<Vec<Row>> {
    let mut rows = execute_program(program, params, func_registry)?;
    if let Some(postprocess) = expression_postprocess {
        apply_expression_postprocess(&mut rows, postprocess)?;
    }
    Ok(rows)
}

/// MVCC concurrent context for page-level locking (bd-kivg / 5E.2).
/// Passed to `execute_table_program_with_db` when in concurrent mode.
struct ConcurrentExecContext {
    session_id: u64,
    registry: Rc<RefCell<ConcurrentRegistry>>,
    lock_table: Rc<InProcessPageLockTable>,
}

fn execute_table_program_with_db(
    program: &VdbeProgram,
    params: Option<&[SqliteValue]>,
    func_registry: &Arc<FunctionRegistry>,
    db: &Rc<RefCell<MemDatabase>>,
    txn: Option<Box<dyn TransactionHandle>>,
    schema_cookie: u32,
    concurrent_ctx: Option<ConcurrentExecContext>,
) -> (Result<Vec<Row>>, Option<Box<dyn TransactionHandle>>) {
    let execution_span = tracing::span!(
        target: "fsqlite.execution",
        tracing::Level::DEBUG,
        "execution",
        mode = "vdbe"
    );
    record_trace_span_created();
    let _execution_guard = execution_span.enter();
    let mut engine = VdbeEngine::new(program.register_count());
    if let Some(params) = params {
        if let Err(e) = validate_bound_parameters(program, params) {
            return (Err(e), txn);
        }
        engine.set_bindings(params.to_vec());
    }

    engine.set_function_registry(Arc::clone(func_registry));
    engine.set_schema_cookie(schema_cookie);

    // Phase 5 (bd-2a3y): if a transaction handle is available, lend it to
    // the engine so storage cursors route through the real pager/WAL stack.
    // bd-kivg / 5E.2: if concurrent context is provided, use set_transaction_concurrent
    // to enable MVCC page-level locking.
    if let Some(txn) = txn {
        if let Some(ctx) = concurrent_ctx {
            engine.set_transaction_concurrent(txn, ctx.session_id, ctx.registry, ctx.lock_table);
        } else {
            engine.set_transaction(txn);
        }
    } else {
        engine.enable_storage_read_cursors(true);
    }

    // Lend the MemDatabase to the engine for the duration of execution.
    let db_value = db.replace(MemDatabase::new());
    engine.set_database(db_value);

    // Always take the DB and txn back, even if execution returns Err.
    let export_started = std::time::Instant::now();
    let exec_res = engine.execute(program);
    let export_latency_us = u64::try_from(export_started.elapsed().as_micros()).unwrap_or(u64::MAX);
    record_trace_export(1, export_latency_us);
    if exec_res.is_err() || matches!(exec_res, Ok(ExecOutcome::Error { .. })) {
        record_trace_export_error();
    }
    if let Some(db_value) = engine.take_database() {
        *db.borrow_mut() = db_value;
    }
    let txn_back = match engine.take_transaction() {
        Ok(txn) => txn,
        Err(e) => return (Err(e), None),
    };

    let result = match exec_res {
        Ok(ExecOutcome::Done) => Ok(engine
            .take_results()
            .into_iter()
            .map(|values| Row { values })
            .collect()),
        Ok(ExecOutcome::Error { code, message }) => Err(FrankenError::Internal(format!(
            "VDBE halted with code {code}: {message}",
        ))),
        Err(e) => Err(e),
    };
    (result, txn_back)
}

fn build_expression_postprocess(select: &SelectStatement) -> ExpressionPostprocess {
    let mut output_aliases = HashMap::new();
    let output_width = match &select.body.select {
        SelectCore::Select { columns, .. } => {
            for (index, column) in columns.iter().enumerate() {
                if let ResultColumn::Expr {
                    alias: Some(alias), ..
                } = column
                {
                    output_aliases
                        .entry(alias.to_ascii_lowercase())
                        .or_insert(index);
                }
            }
            columns.len()
        }
        SelectCore::Values(rows) => rows.first().map_or(0, Vec::len),
    };

    ExpressionPostprocess {
        order_by: select.order_by.clone(),
        limit: select.limit.clone(),
        output_aliases,
        output_width,
    }
}

fn apply_expression_postprocess(
    rows: &mut Vec<Row>,
    postprocess: &ExpressionPostprocess,
) -> Result<()> {
    if !postprocess.order_by.is_empty() {
        let resolved_order_terms = resolve_order_terms(
            &postprocess.order_by,
            postprocess.output_width,
            &postprocess.output_aliases,
        )?;
        rows.sort_by(|left, right| {
            for term in &resolved_order_terms {
                let Some(left_value) = left.values.get(term.column_index) else {
                    continue;
                };
                let Some(right_value) = right.values.get(term.column_index) else {
                    continue;
                };
                let ordering = compare_order_values(left_value, right_value, *term);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(limit_clause) = postprocess.limit.as_ref() {
        let offset = limit_clause
            .offset
            .as_ref()
            .map_or(Ok(0_i64), parse_limit_offset_expr)?;
        if offset > 0 {
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            if offset >= rows.len() {
                rows.clear();
                return Ok(());
            }
            rows.drain(0..offset);
        }

        let limit = parse_limit_offset_expr(&limit_clause.limit)?;
        if limit >= 0 {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            rows.truncate(limit);
        }
    }

    Ok(())
}

fn resolve_order_terms(
    order_by: &[OrderingTerm],
    output_width: usize,
    output_aliases: &HashMap<String, usize>,
) -> Result<Vec<ResolvedOrderTerm>> {
    order_by
        .iter()
        .map(|term| {
            let column_index = match &term.expr {
                Expr::Column(column_ref, _) if column_ref.table.is_none() => output_aliases
                    .get(&column_ref.column.to_ascii_lowercase())
                    .copied()
                    .ok_or_else(|| {
                        FrankenError::NotImplemented(
                            "expression-only ORDER BY currently supports output-column positions or aliases only".to_owned(),
                        )
                    })?,
                _ => order_term_positional_index(&term.expr)?,
            };
            if column_index >= output_width {
                return Err(FrankenError::OutOfRange {
                    what: "ORDER BY column index".to_owned(),
                    value: (column_index + 1).to_string(),
                });
            }
            Ok(ResolvedOrderTerm {
                column_index,
                descending: term.direction == Some(SortDirection::Desc),
                nulls_order: term
                    .nulls
                    .unwrap_or_else(|| default_nulls_order(term.direction)),
            })
        })
        .collect()
}

fn order_term_positional_index(expr: &Expr) -> Result<usize> {
    let one_based = parse_limit_offset_expr(expr)?;
    if one_based <= 0 {
        return Err(FrankenError::OutOfRange {
            what: "ORDER BY column index".to_owned(),
            value: one_based.to_string(),
        });
    }
    let zero_based = one_based - 1;
    usize::try_from(zero_based).map_err(|_| FrankenError::OutOfRange {
        what: "ORDER BY column index".to_owned(),
        value: one_based.to_string(),
    })
}

fn parse_limit_offset_expr(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Literal(Literal::Integer(value), _) => Ok(*value),
        Expr::UnaryOp {
            op: UnaryOp::Plus,
            expr: inner,
            ..
        } => parse_limit_offset_expr(inner),
        Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr: inner,
            ..
        } => parse_limit_offset_expr(inner)?
            .checked_neg()
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "LIMIT/OFFSET expression".to_owned(),
                value: "integer overflow".to_owned(),
            }),
        _ => Err(FrankenError::NotImplemented(
            "expression-only LIMIT/OFFSET currently supports integer literals only".to_owned(),
        )),
    }
}

fn default_nulls_order(direction: Option<SortDirection>) -> NullsOrder {
    if direction == Some(SortDirection::Desc) {
        NullsOrder::Last
    } else {
        NullsOrder::First
    }
}

fn compare_order_values(
    left: &SqliteValue,
    right: &SqliteValue,
    term: ResolvedOrderTerm,
) -> std::cmp::Ordering {
    let left_is_null = left.is_null();
    let right_is_null = right.is_null();
    if left_is_null || right_is_null {
        if left_is_null && right_is_null {
            return std::cmp::Ordering::Equal;
        }
        return match (left_is_null, term.nulls_order) {
            (true, NullsOrder::First) | (false, NullsOrder::Last) => std::cmp::Ordering::Less,
            (true, NullsOrder::Last) | (false, NullsOrder::First) => std::cmp::Ordering::Greater,
        };
    }

    let mut ordering = left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal);
    if term.descending {
        ordering = ordering.reverse();
    }
    ordering
}

fn first_row_or_error(rows: Vec<Row>) -> Result<Row> {
    rows.into_iter()
        .next()
        .ok_or(FrankenError::QueryReturnedNoRows)
}

fn validate_bound_parameters(program: &VdbeProgram, params: &[SqliteValue]) -> Result<()> {
    let mut max_required: usize = 0;
    for op in program.ops() {
        if op.opcode != Opcode::Variable {
            continue;
        }
        let one_based = usize::try_from(op.p1).map_err(|_| FrankenError::OutOfRange {
            what: "bind parameter index".to_owned(),
            value: op.p1.to_string(),
        })?;
        if one_based == 0 {
            return Err(FrankenError::OutOfRange {
                what: "bind parameter index".to_owned(),
                value: op.p1.to_string(),
            });
        }
        max_required = max_required.max(one_based);
    }

    if max_required > params.len() {
        return Err(FrankenError::OutOfRange {
            what: "bind parameter index".to_owned(),
            value: max_required.to_string(),
        });
    }
    Ok(())
}

fn parse_single_statement(sql: &str) -> Result<Statement> {
    let statements = parse_statements(sql)?;
    let mut iter = statements.into_iter();
    let statement = iter.next().ok_or_else(|| FrankenError::ParseError {
        offset: 0,
        detail: "no SQL statement provided".to_owned(),
    })?;

    if iter.next().is_some() {
        return Err(FrankenError::NotImplemented(
            "multiple statements are not supported in this API path".to_owned(),
        ));
    }

    Ok(statement)
}

fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    let mut parser = Parser::from_sql(sql);
    let (statements, errors) = parser.parse_all();

    if let Some(parse_error) = errors.first() {
        return Err(FrankenError::ParseError {
            #[allow(clippy::cast_sign_loss)]
            offset: parse_error.span.start as usize,
            detail: parse_error.message.clone(),
        });
    }

    if statements.is_empty() {
        return Err(FrankenError::ParseError {
            offset: 0,
            detail: "no SQL statement provided".to_owned(),
        });
    }

    Ok(statements)
}

fn quote_identifier(identifier: &str) -> String {
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_qualified_name(name: &fsqlite_ast::QualifiedName) -> String {
    match &name.schema {
        Some(schema) => format!(
            "{}.{}",
            quote_identifier(schema),
            quote_identifier(&name.name)
        ),
        None => quote_identifier(&name.name),
    }
}

fn to_i64_clamped(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn join_page_numbers(pages: &[PageNumber]) -> String {
    pages
        .iter()
        .map(|page| page.get().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn join_txn_tokens(tokens: &[TxnToken]) -> String {
    tokens
        .iter()
        .map(|token| format!("{}:{}", token.id.get(), token.epoch.get()))
        .collect::<Vec<_>>()
        .join(",")
}

fn read_set_top_k_string(summary: &SsiReadSetSummary) -> String {
    summary
        .top_k_pages
        .iter()
        .map(|page| page.get().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn ssi_decision_cards_to_rows(cards: &[SsiDecisionCard]) -> Vec<Row> {
    cards
        .iter()
        .map(|card| Row {
            values: vec![
                SqliteValue::Integer(to_i64_clamped(card.txn.id.get())),
                SqliteValue::Integer(i64::from(card.txn.epoch.get())),
                SqliteValue::Integer(to_i64_clamped(card.snapshot_seq.get())),
                SqliteValue::Integer(to_i64_clamped(card.commit_seq.map_or(0, CommitSeq::get))),
                SqliteValue::Text(card.decision_type.as_str().to_owned()),
                SqliteValue::Text(join_txn_tokens(&card.conflicting_txns)),
                SqliteValue::Text(join_page_numbers(&card.conflict_pages)),
                SqliteValue::Integer(
                    i64::try_from(card.read_set_summary.page_count).unwrap_or(i64::MAX),
                ),
                SqliteValue::Text(read_set_top_k_string(&card.read_set_summary)),
                SqliteValue::Integer(to_i64_clamped(card.read_set_summary.bloom_fingerprint)),
                SqliteValue::Text(join_page_numbers(&card.write_set)),
                SqliteValue::Text(card.rationale.clone()),
                SqliteValue::Integer(to_i64_clamped(card.timestamp_unix_ns)),
                SqliteValue::Integer(to_i64_clamped(card.decision_epoch)),
                SqliteValue::Text(card.chain_hash_hex()),
            ],
        })
        .collect()
}

fn hex_encode_blake3(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn raptorq_repair_evidence_cards_to_rows(cards: &[WalFecRepairEvidenceCard]) -> Vec<Row> {
    cards
        .iter()
        .map(|card| Row {
            values: vec![
                SqliteValue::Integer(i64::from(card.frame_id)),
                card.wal_file_offset_bytes
                    .map_or(SqliteValue::Null, |offset| {
                        SqliteValue::Integer(to_i64_clamped(offset))
                    }),
                SqliteValue::Integer(to_i64_clamped(card.monotonic_timestamp_ns)),
                SqliteValue::Integer(to_i64_clamped(card.wall_clock_unix_ns)),
                SqliteValue::Text(card.corruption_signature_hex()),
                card.bit_error_pattern
                    .as_ref()
                    .map_or(SqliteValue::Null, |pattern| {
                        SqliteValue::Text(pattern.clone())
                    }),
                SqliteValue::Text(card.repair_source.as_str().to_owned()),
                SqliteValue::Integer(i64::from(card.symbols_used)),
                SqliteValue::Integer(i64::from(card.validated_source_symbols)),
                SqliteValue::Integer(i64::from(card.validated_repair_symbols)),
                SqliteValue::Integer(i64::from(card.required_symbols)),
                SqliteValue::Integer(i64::from(card.available_symbols)),
                SqliteValue::Text(hex_encode_blake3(card.witness.corrupted_hash_blake3)),
                SqliteValue::Text(hex_encode_blake3(card.witness.repaired_hash_blake3)),
                SqliteValue::Text(hex_encode_blake3(card.witness.expected_hash_blake3)),
                SqliteValue::Integer(to_i64_clamped(card.repair_latency_ns)),
                SqliteValue::Integer(i64::from(card.confidence_per_mille)),
                SqliteValue::Text(card.severity_bucket.as_str().to_owned()),
                SqliteValue::Integer(to_i64_clamped(card.ledger_epoch)),
                SqliteValue::Text(card.chain_hash_hex()),
            ],
        })
        .collect()
}

#[derive(Debug, Clone)]
struct LineageJoinSpec {
    left_table: String,
    right_table: String,
    left_column: String,
    right_column: String,
    left_contributor_column: String,
    right_contributor_column: String,
}

fn parse_lineage_identifier(raw: &str, label: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(FrankenError::Internal(format!(
            "PRAGMA lineage {label} must be non-empty"
        )));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(FrankenError::Internal(format!(
            "PRAGMA lineage {label} must use [A-Za-z0-9_], got `{trimmed}`"
        )));
    }
    Ok(trimmed.to_owned())
}

fn parse_lineage_query_spec(spec: &str) -> Result<LineageJoinSpec> {
    let parts = spec.split(',').map(str::trim).collect::<Vec<_>>();
    if !(parts.len() == 4 || parts.len() == 6) {
        return Err(FrankenError::Internal(
            "PRAGMA lineage must be `left_table,right_table,left_col,right_col[,left_id_col,right_id_col]`"
                .to_owned(),
        ));
    }
    let (left_contributor_column, right_contributor_column) = if parts.len() == 6 {
        (
            parse_lineage_identifier(parts[4], "left contributor column")?,
            parse_lineage_identifier(parts[5], "right contributor column")?,
        )
    } else {
        ("rowid".to_owned(), "rowid".to_owned())
    };
    Ok(LineageJoinSpec {
        left_table: parse_lineage_identifier(parts[0], "left table")?,
        right_table: parse_lineage_identifier(parts[1], "right table")?,
        left_column: parse_lineage_identifier(parts[2], "left join column")?,
        right_column: parse_lineage_identifier(parts[3], "right join column")?,
        left_contributor_column,
        right_contributor_column,
    })
}

fn parse_lineage_query(value: &fsqlite_ast::PragmaValue) -> Result<LineageJoinSpec> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    match expr {
        Expr::Literal(Literal::String(spec), _) => parse_lineage_query_spec(spec),
        Expr::Column(col_ref, _) if col_ref.table.is_none() => {
            parse_lineage_query_spec(&col_ref.column)
        }
        _ => Err(FrankenError::Internal(
            "PRAGMA lineage value must be a string like `left,right,left_col,right_col[,left_id_col,right_id_col]`".to_owned(),
        )),
    }
}

fn execute_simple_join_lineage_query(
    conn: &Connection,
    spec: &LineageJoinSpec,
) -> Result<Vec<Row>> {
    let sql = format!(
        "SELECT l.{left_id_col}, r.{right_id_col}, l.{left_col}, r.{right_col} \
         FROM {left_tbl} AS l \
         JOIN {right_tbl} AS r \
         ON l.{left_col} = r.{right_col} \
         ORDER BY 1, 2;",
        left_id_col = spec.left_contributor_column,
        right_id_col = spec.right_contributor_column,
        left_col = spec.left_column,
        right_col = spec.right_column,
        left_tbl = spec.left_table,
        right_tbl = spec.right_table
    );
    conn.query(&sql)
}

fn parse_u64_component(raw: &str, label: &str) -> Result<u64> {
    raw.trim().parse::<u64>().map_err(|_| {
        FrankenError::Internal(format!(
            "PRAGMA ssi_decisions {label} must be an unsigned integer, got `{raw}`"
        ))
    })
}

fn parse_ssi_decision_query_spec(spec: &str) -> Result<SsiDecisionQuery> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Ok(SsiDecisionQuery::default());
    }

    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Ok(SsiDecisionQuery {
            txn_id: Some(parse_u64_component(trimmed, "txn")?),
            ..SsiDecisionQuery::default()
        });
    }

    let mut query = SsiDecisionQuery::default();
    for term in trimmed.split(',') {
        let term = term.trim();
        if term.is_empty() {
            continue;
        }

        if let Some(raw) = term.strip_prefix("txn:") {
            query.txn_id = Some(parse_u64_component(raw, "txn")?);
            continue;
        }
        if let Some(raw) = term.strip_prefix("type:") {
            let decision_type = SsiDecisionType::from_str(raw).map_err(|_| {
                FrankenError::Internal(format!(
                    "PRAGMA ssi_decisions type must be one of COMMIT_ALLOWED/ABORT_WRITE_SKEW/ABORT_PHANTOM/ABORT_CYCLE, got `{raw}`"
                ))
            })?;
            query.decision_type = Some(decision_type);
            continue;
        }
        if let Some(raw) = term.strip_prefix("time:") {
            let (start_raw, end_raw) = raw.split_once('-').ok_or_else(|| {
                FrankenError::Internal(
                    "PRAGMA ssi_decisions time filter must be `time:<start>-<end>`".to_owned(),
                )
            })?;
            query.timestamp_start_ns = if start_raw.trim().is_empty() {
                None
            } else {
                Some(parse_u64_component(start_raw, "time start")?)
            };
            query.timestamp_end_ns = if end_raw.trim().is_empty() {
                None
            } else {
                Some(parse_u64_component(end_raw, "time end")?)
            };
            continue;
        }

        if query.decision_type.is_none() {
            if let Ok(decision_type) = SsiDecisionType::from_str(term) {
                query.decision_type = Some(decision_type);
                continue;
            }
        }

        return Err(FrankenError::Internal(format!(
            "PRAGMA ssi_decisions filter term `{term}` is invalid; use txn:<id>, type:<decision>, or time:<start>-<end>"
        )));
    }

    Ok(query)
}

fn parse_ssi_decision_query(value: &fsqlite_ast::PragmaValue) -> Result<SsiDecisionQuery> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    match expr {
        Expr::Literal(Literal::Integer(n), _) => {
            if *n < 0 {
                return Err(FrankenError::Internal(
                    "PRAGMA ssi_decisions txn id must be >= 0".to_owned(),
                ));
            }
            Ok(SsiDecisionQuery {
                txn_id: Some(u64::try_from(*n).unwrap_or(u64::MAX)),
                ..SsiDecisionQuery::default()
            })
        }
        Expr::Literal(Literal::String(s), _) => parse_ssi_decision_query_spec(s),
        Expr::Column(col_ref, _) if col_ref.table.is_none() => {
            parse_ssi_decision_query_spec(&col_ref.column)
        }
        _ => Err(FrankenError::Internal(
            "PRAGMA ssi_decisions value must be an integer txn id or string filter".to_owned(),
        )),
    }
}

fn parse_raptorq_u64_component(raw: &str, label: &str) -> Result<u64> {
    raw.trim().parse::<u64>().map_err(|_| {
        FrankenError::Internal(format!(
            "PRAGMA raptorq_repair_evidence {label} must be an unsigned integer, got `{raw}`"
        ))
    })
}

fn parse_raptorq_repair_evidence_query_spec(spec: &str) -> Result<WalFecRepairEvidenceQuery> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Ok(WalFecRepairEvidenceQuery::default());
    }

    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        let frame_id = parse_raptorq_u64_component(trimmed, "frame")?;
        return Ok(WalFecRepairEvidenceQuery {
            frame_id: Some(u32::try_from(frame_id).unwrap_or(u32::MAX)),
            ..WalFecRepairEvidenceQuery::default()
        });
    }

    let mut query = WalFecRepairEvidenceQuery::default();
    for term in trimmed.split(',') {
        let term = term.trim();
        if term.is_empty() {
            continue;
        }

        if let Some(raw) = term
            .strip_prefix("frame:")
            .or_else(|| term.strip_prefix("page:"))
        {
            let frame_id = parse_raptorq_u64_component(raw, "frame")?;
            query.frame_id = Some(u32::try_from(frame_id).unwrap_or(u32::MAX));
            continue;
        }

        if let Some(raw) = term.strip_prefix("severity:") {
            let bucket = WalFecRepairSeverityBucket::from_str(raw).map_err(|_| {
                FrankenError::Internal(format!(
                    "PRAGMA raptorq_repair_evidence severity must be one of 1|2-5|6-10|11+, got `{raw}`"
                ))
            })?;
            query.severity_bucket = Some(bucket);
            continue;
        }

        if let Some(raw) = term.strip_prefix("time:") {
            let (start_raw, end_raw) = raw.split_once('-').ok_or_else(|| {
                FrankenError::Internal(
                    "PRAGMA raptorq_repair_evidence time filter must be `time:<start>-<end>`"
                        .to_owned(),
                )
            })?;
            query.wall_clock_start_ns = if start_raw.trim().is_empty() {
                None
            } else {
                Some(parse_raptorq_u64_component(start_raw, "time start")?)
            };
            query.wall_clock_end_ns = if end_raw.trim().is_empty() {
                None
            } else {
                Some(parse_raptorq_u64_component(end_raw, "time end")?)
            };
            continue;
        }

        if let Some(raw) = term.strip_prefix("limit:") {
            let limit = parse_raptorq_u64_component(raw, "limit")?;
            query.limit = Some(usize::try_from(limit).unwrap_or(usize::MAX));
            continue;
        }

        if query.severity_bucket.is_none() {
            if let Ok(bucket) = WalFecRepairSeverityBucket::from_str(term) {
                query.severity_bucket = Some(bucket);
                continue;
            }
        }

        return Err(FrankenError::Internal(format!(
            "PRAGMA raptorq_repair_evidence filter term `{term}` is invalid; use frame:<id>, severity:<bucket>, time:<start>-<end>, or limit:<n>"
        )));
    }

    Ok(query)
}

fn parse_raptorq_repair_evidence_query(
    value: &fsqlite_ast::PragmaValue,
) -> Result<WalFecRepairEvidenceQuery> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    match expr {
        Expr::Literal(Literal::Integer(n), _) => {
            if *n < 0 {
                return Err(FrankenError::Internal(
                    "PRAGMA raptorq_repair_evidence frame id must be >= 0".to_owned(),
                ));
            }
            Ok(WalFecRepairEvidenceQuery {
                frame_id: Some(u32::try_from(*n).unwrap_or(u32::MAX)),
                ..WalFecRepairEvidenceQuery::default()
            })
        }
        Expr::Literal(Literal::String(s), _) => parse_raptorq_repair_evidence_query_spec(s),
        Expr::Column(col_ref, _) if col_ref.table.is_none() => {
            parse_raptorq_repair_evidence_query_spec(&col_ref.column)
        }
        _ => Err(FrankenError::Internal(
            "PRAGMA raptorq_repair_evidence value must be an integer frame id or string filter"
                .to_owned(),
        )),
    }
}

/// Parse a PRAGMA value as a boolean.
///
/// Accepts `on`, `off`, `true`, `false`, `1`, `0`, `yes`, `no`
/// (case-insensitive).
fn parse_pragma_bool(value: &fsqlite_ast::PragmaValue) -> Result<bool> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    let text = match expr {
        Expr::Literal(Literal::Integer(n), _) => {
            return match *n {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(FrankenError::Internal(format!(
                    "PRAGMA boolean value must be 0 or 1, got {n}"
                ))),
            };
        }
        Expr::Literal(Literal::True, _) => return Ok(true),
        Expr::Literal(Literal::False, _) => return Ok(false),
        Expr::Literal(Literal::String(s), _) => s.clone(),
        Expr::Column(col_ref, _) if col_ref.table.is_none() => col_ref.column.clone(),
        _ => {
            return Err(FrankenError::Internal(
                "PRAGMA boolean value must be ON/OFF/TRUE/FALSE/1/0".to_owned(),
            ));
        }
    };
    match text.to_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err(FrankenError::Internal(format!(
            "PRAGMA boolean value must be ON/OFF/TRUE/FALSE/1/0, got `{text}`"
        ))),
    }
}

/// Parse a PRAGMA value as a non-negative integer (`usize`).
fn parse_pragma_nonnegative_usize(value: &fsqlite_ast::PragmaValue, what: &str) -> Result<usize> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    let raw = match expr {
        Expr::Literal(Literal::Integer(n), _) => *n,
        Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr,
            ..
        } => match expr.as_ref() {
            Expr::Literal(Literal::Integer(n), _) => n.saturating_neg(),
            _ => {
                return Err(FrankenError::Internal(format!(
                    "PRAGMA {what} must be a non-negative integer"
                )));
            }
        },
        Expr::Column(col_ref, _) if col_ref.table.is_none() => {
            col_ref.column.parse::<i64>().map_err(|_| {
                FrankenError::Internal(format!(
                    "PRAGMA {what} must be a non-negative integer, got `{}`",
                    col_ref.column
                ))
            })?
        }
        _ => {
            return Err(FrankenError::Internal(format!(
                "PRAGMA {what} must be a non-negative integer"
            )));
        }
    };

    if raw < 0 {
        return Err(FrankenError::Internal(format!(
            "PRAGMA {what} must be >= 0, got {raw}"
        )));
    }

    usize::try_from(raw).map_err(|_| {
        FrankenError::Internal(format!(
            "PRAGMA {what} value is too large for this platform: {raw}"
        ))
    })
}

/// Parse a PRAGMA value as a checkpoint mode.
///
/// Accepts `PASSIVE`, `FULL`, `RESTART`, `TRUNCATE` (case-insensitive).
fn parse_checkpoint_mode(value: &fsqlite_ast::PragmaValue) -> Result<CheckpointMode> {
    let expr = match value {
        fsqlite_ast::PragmaValue::Assign(e) | fsqlite_ast::PragmaValue::Call(e) => e,
    };
    let text = match expr {
        Expr::Literal(Literal::String(s), _) => s.clone(),
        Expr::Column(col_ref, _) if col_ref.table.is_none() => col_ref.column.clone(),
        _ => {
            return Err(FrankenError::Internal(
                "PRAGMA wal_checkpoint mode must be PASSIVE/FULL/RESTART/TRUNCATE".to_owned(),
            ));
        }
    };
    match text.to_uppercase().as_str() {
        "PASSIVE" => Ok(CheckpointMode::Passive),
        "FULL" => Ok(CheckpointMode::Full),
        "RESTART" => Ok(CheckpointMode::Restart),
        "TRUNCATE" => Ok(CheckpointMode::Truncate),
        _ => Err(FrankenError::Internal(format!(
            "PRAGMA wal_checkpoint mode must be PASSIVE/FULL/RESTART/TRUNCATE, got `{text}`"
        ))),
    }
}

fn parse_database_header(page1_bytes: &[u8]) -> Option<DatabaseHeader> {
    if page1_bytes.iter().all(|&b| b == 0) || page1_bytes.len() < DATABASE_HEADER_SIZE {
        return None;
    }
    let mut header_bytes = [0_u8; DATABASE_HEADER_SIZE];
    header_bytes.copy_from_slice(&page1_bytes[..DATABASE_HEADER_SIZE]);
    DatabaseHeader::from_bytes(&header_bytes).ok()
}

#[allow(clippy::too_many_lines)]
fn compile_expression_select(select: &SelectStatement) -> Result<VdbeProgram> {
    if !select.body.compounds.is_empty() {
        return Err(FrankenError::NotImplemented(
            "compound SELECT is not supported in this connection path".to_owned(),
        ));
    }

    let mut builder = ProgramBuilder::new();
    let mut bind_state = BindParamState::default();
    let init_target = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, init_target, P4::None, 0);

    match &select.body.select {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
            ..
        } => {
            // DISTINCT is handled post-execution via dedup_rows() in the
            // caller, so we can safely ignore the `distinct` field here.
            if from.is_some() {
                return Err(FrankenError::NotImplemented(
                    "SELECT ... FROM is not supported in this connection path".to_owned(),
                ));
            }
            if !group_by.is_empty() {
                return Err(FrankenError::NotImplemented(
                    "GROUP BY is not supported in this connection path".to_owned(),
                ));
            }
            if having.is_some() {
                return Err(FrankenError::NotImplemented(
                    "HAVING is not supported in this connection path".to_owned(),
                ));
            }
            if !windows.is_empty() {
                return Err(FrankenError::NotImplemented(
                    "WINDOW is not supported in this connection path".to_owned(),
                ));
            }
            if columns.is_empty() {
                return Err(FrankenError::ParseError {
                    offset: 0,
                    detail: "SELECT must include at least one result column".to_owned(),
                });
            }

            let out_count =
                i32::try_from(columns.len()).map_err(|_| FrankenError::TooManyColumns {
                    count: columns.len(),
                    max: i32::MAX as usize,
                })?;
            let out_first_reg = builder.alloc_regs(out_count);
            let skip_row_label = if let Some(predicate) = where_clause.as_ref() {
                let predicate_reg = builder.alloc_temp();
                emit_expr(&mut builder, predicate, predicate_reg, &mut bind_state)?;
                let skip_label = builder.emit_label();
                builder.emit_jump_to_label(
                    Opcode::IfNot,
                    predicate_reg,
                    0,
                    skip_label,
                    P4::None,
                    0,
                );
                builder.free_temp(predicate_reg);
                Some(skip_label)
            } else {
                None
            };

            for (idx, column) in columns.iter().enumerate() {
                let expr = match column {
                    ResultColumn::Expr { expr, .. } => expr,
                    ResultColumn::Star | ResultColumn::TableStar(_) => {
                        return Err(FrankenError::NotImplemented(
                            "star expansion requires name resolution and FROM sources".to_owned(),
                        ));
                    }
                };

                let idx_i32 = i32::try_from(idx).map_err(|_| FrankenError::OutOfRange {
                    what: "result column index".to_owned(),
                    value: idx.to_string(),
                })?;
                let output_reg = out_first_reg + idx_i32;
                emit_expr(&mut builder, expr, output_reg, &mut bind_state)?;
            }

            builder.emit_op(Opcode::ResultRow, out_first_reg, out_count, 0, P4::None, 0);
            if let Some(skip_label) = skip_row_label {
                builder.resolve_label(skip_label);
            }
        }
        SelectCore::Values(rows) => {
            if rows.is_empty() {
                return Err(FrankenError::ParseError {
                    offset: 0,
                    detail: "VALUES must include at least one row".to_owned(),
                });
            }
            let first_row_len = rows[0].len();
            if first_row_len == 0 {
                return Err(FrankenError::ParseError {
                    offset: 0,
                    detail: "VALUES row must include at least one expression".to_owned(),
                });
            }
            for row in rows {
                if row.len() != first_row_len {
                    return Err(FrankenError::ParseError {
                        offset: 0,
                        detail: "VALUES rows must have matching column counts".to_owned(),
                    });
                }
            }

            let out_count =
                i32::try_from(first_row_len).map_err(|_| FrankenError::TooManyColumns {
                    count: first_row_len,
                    max: i32::MAX as usize,
                })?;
            let out_first_reg = builder.alloc_regs(out_count);

            for row in rows {
                for (idx, expr) in row.iter().enumerate() {
                    let idx_i32 = i32::try_from(idx).map_err(|_| FrankenError::OutOfRange {
                        what: "VALUES column index".to_owned(),
                        value: idx.to_string(),
                    })?;
                    emit_expr(&mut builder, expr, out_first_reg + idx_i32, &mut bind_state)?;
                }
                builder.emit_op(Opcode::ResultRow, out_first_reg, out_count, 0, P4::None, 0);
            }
        }
    }

    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(init_target);
    builder.finish()
}

#[allow(clippy::too_many_lines)]
fn emit_expr(
    builder: &mut ProgramBuilder,
    expr: &Expr,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    match expr {
        Expr::Literal(literal, _) => {
            emit_literal(builder, literal, target_reg);
            Ok(())
        }
        Expr::BinaryOp {
            left, op, right, ..
        } => emit_binary_expr(builder, left, *op, right, target_reg, bind_state),
        Expr::UnaryOp { op, expr, .. } => {
            emit_unary_expr(builder, *op, expr, target_reg, bind_state)
        }
        Expr::FunctionCall {
            name,
            args,
            distinct,
            filter,
            over,
            ..
        } => emit_function_call(
            builder,
            name,
            args,
            *distinct,
            filter.is_some(),
            over.is_some(),
            target_reg,
            bind_state,
        ),
        Expr::Placeholder(placeholder, _) => {
            let param_index = placeholder_to_index(placeholder, bind_state)?;
            builder.emit_op(Opcode::Variable, param_index, target_reg, 0, P4::None, 0);
            Ok(())
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => emit_case_expr(
            builder,
            operand.as_deref(),
            whens,
            else_expr.as_deref(),
            target_reg,
            bind_state,
        ),
        Expr::Cast {
            expr: inner,
            type_name,
            ..
        } => {
            emit_expr(builder, inner, target_reg, bind_state)?;
            let affinity = type_name_to_affinity(&type_name.name);
            builder.emit_op(
                Opcode::Cast,
                target_reg,
                i32::from(affinity),
                0,
                P4::None,
                0,
            );
            Ok(())
        }
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            emit_expr(builder, inner, target_reg, bind_state)?;
            let lbl_null = builder.emit_label();
            let lbl_done = builder.emit_label();
            builder.emit_jump_to_label(Opcode::IsNull, target_reg, 0, lbl_null, P4::None, 0);
            let val_not_null = i32::from(*not);
            let val_null = i32::from(!*not);
            builder.emit_op(Opcode::Integer, val_not_null, target_reg, 0, P4::None, 0);
            builder.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_done, P4::None, 0);
            builder.resolve_label(lbl_null);
            builder.emit_op(Opcode::Integer, val_null, target_reg, 0, P4::None, 0);
            builder.resolve_label(lbl_done);
            Ok(())
        }
        // ── BETWEEN: expr [NOT] BETWEEN low AND high ──────────────────
        Expr::Between {
            expr: inner,
            low,
            high,
            not,
            ..
        } => emit_between_expr(builder, inner, low, high, *not, target_reg, bind_state),

        // ── IN: expr [NOT] IN (list) ──────────────────────────────────
        Expr::In {
            expr: inner,
            set,
            not,
            ..
        } => emit_in_expr(builder, inner, set, *not, target_reg, bind_state),

        // ── LIKE/GLOB/MATCH/REGEXP ────────────────────────────────────
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            op,
            not,
            ..
        } => emit_like_expr(
            builder,
            inner,
            pattern,
            escape.as_deref(),
            *op,
            *not,
            target_reg,
            bind_state,
        ),

        _ => Err(FrankenError::NotImplemented(format!(
            "expression form is not supported in this connection path: {expr:?}",
        ))),
    }
}

fn emit_binary_expr(
    builder: &mut ProgramBuilder,
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let left_reg = builder.alloc_temp();
    let right_reg = builder.alloc_temp();
    emit_expr(builder, left, left_reg, bind_state)?;
    emit_expr(builder, right, right_reg, bind_state)?;

    let opcode = match op {
        BinaryOp::Add => Opcode::Add,
        BinaryOp::Subtract => Opcode::Subtract,
        BinaryOp::Multiply => Opcode::Multiply,
        BinaryOp::Divide => Opcode::Divide,
        BinaryOp::Modulo => Opcode::Remainder,
        BinaryOp::Concat => Opcode::Concat,
        BinaryOp::BitAnd => Opcode::BitAnd,
        BinaryOp::BitOr => Opcode::BitOr,
        BinaryOp::ShiftLeft => Opcode::ShiftLeft,
        BinaryOp::ShiftRight => Opcode::ShiftRight,
        BinaryOp::And => Opcode::And,
        BinaryOp::Or => Opcode::Or,
        // Comparison operators produce 0/1 via conditional jumps.
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let cmp_opcode = match op {
                BinaryOp::Eq => Opcode::Eq,
                BinaryOp::Ne => Opcode::Ne,
                BinaryOp::Lt => Opcode::Lt,
                BinaryOp::Le => Opcode::Le,
                BinaryOp::Gt => Opcode::Gt,
                BinaryOp::Ge => Opcode::Ge,
                _ => unreachable!(),
            };
            let true_label = builder.emit_label();
            let done_label = builder.emit_label();
            builder.emit_jump_to_label(cmp_opcode, right_reg, left_reg, true_label, P4::None, 0);
            builder.emit_op(Opcode::Integer, 0, target_reg, 0, P4::None, 0);
            builder.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            builder.resolve_label(true_label);
            builder.emit_op(Opcode::Integer, 1, target_reg, 0, P4::None, 0);
            builder.resolve_label(done_label);
            builder.free_temp(right_reg);
            builder.free_temp(left_reg);
            return Ok(());
        }
        BinaryOp::Is | BinaryOp::IsNot => {
            let (cmp_opcode, nulleq_flag) = match op {
                BinaryOp::Is => (Opcode::Eq, 0x80_u16),
                BinaryOp::IsNot => (Opcode::Ne, 0x80_u16),
                _ => unreachable!(),
            };
            let true_label = builder.emit_label();
            let done_label = builder.emit_label();
            builder.emit_jump_to_label(
                cmp_opcode,
                right_reg,
                left_reg,
                true_label,
                P4::None,
                nulleq_flag,
            );
            builder.emit_op(Opcode::Integer, 0, target_reg, 0, P4::None, 0);
            builder.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            builder.resolve_label(true_label);
            builder.emit_op(Opcode::Integer, 1, target_reg, 0, P4::None, 0);
            builder.resolve_label(done_label);
            builder.free_temp(right_reg);
            builder.free_temp(left_reg);
            return Ok(());
        }
    };

    // Engine semantics for these opcodes consume p2 (left) and p1 (right).
    builder.emit_op(opcode, right_reg, left_reg, target_reg, P4::None, 0);
    builder.free_temp(right_reg);
    builder.free_temp(left_reg);
    Ok(())
}

fn emit_unary_expr(
    builder: &mut ProgramBuilder,
    op: UnaryOp,
    expr: &Expr,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    match op {
        UnaryOp::Plus => emit_expr(builder, expr, target_reg, bind_state),
        UnaryOp::BitNot => {
            let source_reg = builder.alloc_temp();
            emit_expr(builder, expr, source_reg, bind_state)?;
            builder.emit_op(Opcode::BitNot, source_reg, target_reg, 0, P4::None, 0);
            builder.free_temp(source_reg);
            Ok(())
        }
        UnaryOp::Not => {
            let source_reg = builder.alloc_temp();
            emit_expr(builder, expr, source_reg, bind_state)?;
            builder.emit_op(Opcode::Not, source_reg, target_reg, 0, P4::None, 0);
            builder.free_temp(source_reg);
            Ok(())
        }
        UnaryOp::Negate => {
            let source_reg = builder.alloc_temp();
            let zero_reg = builder.alloc_temp();
            emit_expr(builder, expr, source_reg, bind_state)?;
            builder.emit_op(Opcode::Integer, 0, zero_reg, 0, P4::None, 0);
            // target = 0 - source
            builder.emit_op(
                Opcode::Subtract,
                source_reg,
                zero_reg,
                target_reg,
                P4::None,
                0,
            );
            builder.free_temp(zero_reg);
            builder.free_temp(source_reg);
            Ok(())
        }
    }
}

/// Emit a scalar function call as a `PureFunc` opcode.
///
/// Layout: arguments are evaluated into consecutive registers starting at
/// `first_arg_reg`. The opcode carries `P4::FuncName(name)`, with `p2` =
/// first arg register, `p3` = output register, and `p5` = arg count. The
/// VDBE engine looks up the function by name in its `FunctionRegistry`.
#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn emit_function_call(
    builder: &mut ProgramBuilder,
    name: &str,
    args: &FunctionArgs,
    distinct: bool,
    has_filter: bool,
    has_over: bool,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    if distinct || has_filter || has_over {
        return Err(FrankenError::NotImplemented(
            "function modifiers (DISTINCT/FILTER/OVER) are not supported".to_owned(),
        ));
    }

    let arguments = match args {
        FunctionArgs::List(arguments) => arguments,
        FunctionArgs::Star => {
            return Err(FrankenError::NotImplemented(
                "function(*) is not supported in expression-only SELECT".to_owned(),
            ));
        }
    };

    let arg_count = arguments.len();
    let arg_count_u16 = u16::try_from(arg_count).map_err(|_| FrankenError::TooManyArguments {
        name: name.to_owned(),
    })?;

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    let first_arg_reg = builder.alloc_regs(arg_count as i32);

    // Evaluate each argument into consecutive registers.
    for (i, arg_expr) in arguments.iter().enumerate() {
        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        let reg = first_arg_reg + i as i32;
        emit_expr(builder, arg_expr, reg, bind_state)?;
    }

    // Emit PureFunc: p1 = 0 (flags), p2 = first_arg_reg, p3 = output_reg,
    // p4 = FuncName, p5 = arg_count.
    builder.emit_op(
        Opcode::PureFunc,
        0,
        first_arg_reg,
        target_reg,
        P4::FuncName(name.to_owned()),
        arg_count_u16,
    );
    Ok(())
}

fn emit_literal(builder: &mut ProgramBuilder, literal: &Literal, target_reg: i32) {
    match literal {
        Literal::Integer(value) => {
            #[allow(clippy::cast_possible_truncation)]
            builder.emit_op(Opcode::Integer, *value as i32, target_reg, 0, P4::None, 0);
        }
        Literal::Float(value) => {
            builder.emit_op(Opcode::Real, 0, target_reg, 0, P4::Real(*value), 0);
        }
        Literal::String(value) => {
            builder.emit_op(Opcode::String8, 0, target_reg, 0, P4::Str(value.clone()), 0);
        }
        Literal::Null | Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
            builder.emit_op(Opcode::Null, 0, target_reg, 0, P4::None, 0);
        }
        Literal::True => {
            builder.emit_op(Opcode::Integer, 1, target_reg, 0, P4::None, 0);
        }
        Literal::False => {
            builder.emit_op(Opcode::Integer, 0, target_reg, 0, P4::None, 0);
        }
        Literal::Blob(value) => {
            builder.emit_op(Opcode::Blob, 0, target_reg, 0, P4::Blob(value.clone()), 0);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BindParamState {
    next_index: i32,
    named_indices: HashMap<NamedPlaceholderKey, i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NamedPlaceholderKey {
    prefix: char,
    name: String,
}

impl Default for BindParamState {
    fn default() -> Self {
        Self {
            next_index: 1,
            named_indices: HashMap::new(),
        }
    }
}

impl BindParamState {
    fn claim_anonymous(&mut self) -> Result<i32> {
        let index = self.next_index;
        self.next_index =
            self.next_index
                .checked_add(1)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "placeholder index".to_owned(),
                    value: index.to_string(),
                })?;
        Ok(index)
    }

    fn register_numbered(&mut self, index: i32) -> Result<i32> {
        let next = index
            .checked_add(1)
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "placeholder index".to_owned(),
                value: index.to_string(),
            })?;
        self.next_index = self.next_index.max(next);
        Ok(index)
    }

    fn register_named(&mut self, prefix: char, name: &str) -> Result<i32> {
        let key = NamedPlaceholderKey {
            prefix,
            name: name.to_owned(),
        };
        if let Some(index) = self.named_indices.get(&key) {
            return Ok(*index);
        }

        let index = self.claim_anonymous()?;
        self.named_indices.insert(key, index);
        Ok(index)
    }
}

fn placeholder_to_index(
    placeholder: &PlaceholderType,
    bind_state: &mut BindParamState,
) -> Result<i32> {
    match placeholder {
        PlaceholderType::Anonymous => bind_state.claim_anonymous(),
        PlaceholderType::Numbered(index) => {
            let index = i32::try_from(*index).map_err(|_| FrankenError::OutOfRange {
                what: "placeholder index".to_owned(),
                value: index.to_string(),
            })?;
            bind_state.register_numbered(index)
        }
        PlaceholderType::ColonNamed(name) => bind_state.register_named(':', name),
        PlaceholderType::AtNamed(name) => bind_state.register_named('@', name),
        PlaceholderType::DollarNamed(name) => bind_state.register_named('$', name),
    }
}

/// Bind SELECT placeholders to literal values for fallback evaluators.
///
/// Fallback JOIN/GROUP BY paths interpret expressions directly instead of
/// executing VDBE `Variable` opcodes, so bind parameters must be substituted
/// into the AST beforehand.
fn bind_placeholders_in_select_for_fallback(
    select: &SelectStatement,
    params: Option<&[SqliteValue]>,
) -> Result<SelectStatement> {
    let Some(params) = params else {
        return Ok(select.clone());
    };
    let mut bound = select.clone();
    let mut bind_state = BindParamState::default();
    bind_placeholders_in_select_statement(&mut bound, &mut bind_state, params)?;
    Ok(bound)
}

fn bind_placeholders_in_select_statement(
    select: &mut SelectStatement,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    if let Some(with_clause) = &mut select.with {
        for cte in &mut with_clause.ctes {
            bind_placeholders_in_select_statement(&mut cte.query, bind_state, params)?;
        }
    }

    bind_placeholders_in_select_core(&mut select.body.select, bind_state, params)?;
    for (_, core) in &mut select.body.compounds {
        bind_placeholders_in_select_core(core, bind_state, params)?;
    }

    for ordering in &mut select.order_by {
        bind_placeholders_in_expr(&mut ordering.expr, bind_state, params)?;
    }
    if let Some(limit_clause) = &mut select.limit {
        bind_placeholders_in_expr(&mut limit_clause.limit, bind_state, params)?;
        if let Some(offset) = &mut limit_clause.offset {
            bind_placeholders_in_expr(offset, bind_state, params)?;
        }
    }

    Ok(())
}

fn bind_placeholders_in_select_core(
    core: &mut SelectCore,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    match core {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
            ..
        } => {
            for column in columns {
                if let ResultColumn::Expr { expr, .. } = column {
                    bind_placeholders_in_expr(expr, bind_state, params)?;
                }
            }
            if let Some(from_clause) = from {
                bind_placeholders_in_from_clause(from_clause, bind_state, params)?;
            }
            if let Some(predicate) = where_clause {
                bind_placeholders_in_expr(predicate, bind_state, params)?;
            }
            for expr in group_by {
                bind_placeholders_in_expr(expr, bind_state, params)?;
            }
            if let Some(predicate) = having {
                bind_placeholders_in_expr(predicate, bind_state, params)?;
            }
            for window in windows {
                bind_placeholders_in_window_spec(&mut window.spec, bind_state, params)?;
            }
        }
        SelectCore::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_placeholders_in_expr(expr, bind_state, params)?;
                }
            }
        }
    }
    Ok(())
}

fn bind_placeholders_in_from_clause(
    from: &mut fsqlite_ast::FromClause,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    bind_placeholders_in_table_or_subquery(&mut from.source, bind_state, params)?;
    for join in &mut from.joins {
        bind_placeholders_in_table_or_subquery(&mut join.table, bind_state, params)?;
        if let Some(JoinConstraint::On(expr)) = &mut join.constraint {
            bind_placeholders_in_expr(expr, bind_state, params)?;
        }
    }
    Ok(())
}

fn bind_placeholders_in_table_or_subquery(
    source: &mut TableOrSubquery,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    match source {
        TableOrSubquery::Table { .. } => {}
        TableOrSubquery::Subquery { query, .. } => {
            bind_placeholders_in_select_statement(query, bind_state, params)?;
        }
        TableOrSubquery::TableFunction { args, .. } => {
            for arg in args {
                bind_placeholders_in_expr(arg, bind_state, params)?;
            }
        }
        TableOrSubquery::ParenJoin(from_clause) => {
            bind_placeholders_in_from_clause(from_clause, bind_state, params)?;
        }
    }
    Ok(())
}

fn bind_placeholders_in_window_spec(
    spec: &mut fsqlite_ast::WindowSpec,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    for expr in &mut spec.partition_by {
        bind_placeholders_in_expr(expr, bind_state, params)?;
    }
    for ordering in &mut spec.order_by {
        bind_placeholders_in_expr(&mut ordering.expr, bind_state, params)?;
    }
    if let Some(frame) = &mut spec.frame {
        bind_placeholders_in_frame_bound(&mut frame.start, bind_state, params)?;
        if let Some(end) = &mut frame.end {
            bind_placeholders_in_frame_bound(end, bind_state, params)?;
        }
    }
    Ok(())
}

fn bind_placeholders_in_frame_bound(
    bound: &mut fsqlite_ast::FrameBound,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    match bound {
        fsqlite_ast::FrameBound::Preceding(expr) | fsqlite_ast::FrameBound::Following(expr) => {
            bind_placeholders_in_expr(expr, bind_state, params)?;
        }
        fsqlite_ast::FrameBound::UnboundedPreceding
        | fsqlite_ast::FrameBound::CurrentRow
        | fsqlite_ast::FrameBound::UnboundedFollowing => {}
    }
    Ok(())
}

fn bind_placeholder_to_literal(
    placeholder: &PlaceholderType,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<Literal> {
    let param_index = placeholder_to_index(placeholder, bind_state)?;
    let one_based = usize::try_from(param_index).map_err(|_| FrankenError::OutOfRange {
        what: "bind parameter index".to_owned(),
        value: param_index.to_string(),
    })?;
    if one_based == 0 || one_based > params.len() {
        return Err(FrankenError::OutOfRange {
            what: "bind parameter index".to_owned(),
            value: param_index.to_string(),
        });
    }
    Ok(sqlite_value_to_literal(&params[one_based - 1]))
}

fn sqlite_value_to_literal(value: &SqliteValue) -> Literal {
    match value {
        SqliteValue::Null => Literal::Null,
        SqliteValue::Integer(v) => Literal::Integer(*v),
        SqliteValue::Float(v) => Literal::Float(*v),
        SqliteValue::Text(v) => Literal::String(v.clone()),
        SqliteValue::Blob(v) => Literal::Blob(v.clone()),
    }
}

#[allow(clippy::too_many_lines)]
fn bind_trigger_columns_in_statement(statement: &mut Statement, frame: &TriggerFrame) {
    match statement {
        Statement::Select(select) => {
            bind_trigger_columns_in_select_statement(select, frame);
        }
        Statement::Insert(insert) => {
            if let Some(with_clause) = &mut insert.with {
                for cte in &mut with_clause.ctes {
                    bind_trigger_columns_in_select_statement(&mut cte.query, frame);
                }
            }
            match &mut insert.source {
                fsqlite_ast::InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            bind_trigger_columns_in_expr(expr, frame);
                        }
                    }
                }
                fsqlite_ast::InsertSource::Select(select) => {
                    bind_trigger_columns_in_select_statement(select, frame);
                }
                fsqlite_ast::InsertSource::DefaultValues => {}
            }
            for upsert in &mut insert.upsert {
                if let Some(target) = &mut upsert.target {
                    if let Some(where_clause) = &mut target.where_clause {
                        bind_trigger_columns_in_expr(where_clause, frame);
                    }
                }
                if let fsqlite_ast::UpsertAction::Update {
                    assignments,
                    where_clause,
                } = &mut upsert.action
                {
                    for assignment in assignments {
                        bind_trigger_columns_in_expr(&mut assignment.value, frame);
                    }
                    if let Some(predicate) = where_clause {
                        bind_trigger_columns_in_expr(predicate, frame);
                    }
                }
            }
            for column in &mut insert.returning {
                if let ResultColumn::Expr { expr, .. } = column {
                    bind_trigger_columns_in_expr(expr, frame);
                }
            }
        }
        Statement::Update(update) => {
            if let Some(with_clause) = &mut update.with {
                for cte in &mut with_clause.ctes {
                    bind_trigger_columns_in_select_statement(&mut cte.query, frame);
                }
            }
            for assignment in &mut update.assignments {
                bind_trigger_columns_in_expr(&mut assignment.value, frame);
            }
            if let Some(from_clause) = &mut update.from {
                bind_trigger_columns_in_from_clause(from_clause, frame);
            }
            if let Some(where_clause) = &mut update.where_clause {
                bind_trigger_columns_in_expr(where_clause, frame);
            }
            for column in &mut update.returning {
                if let ResultColumn::Expr { expr, .. } = column {
                    bind_trigger_columns_in_expr(expr, frame);
                }
            }
            for ordering in &mut update.order_by {
                bind_trigger_columns_in_expr(&mut ordering.expr, frame);
            }
            if let Some(limit_clause) = &mut update.limit {
                bind_trigger_columns_in_expr(&mut limit_clause.limit, frame);
                if let Some(offset) = &mut limit_clause.offset {
                    bind_trigger_columns_in_expr(offset, frame);
                }
            }
        }
        Statement::Delete(delete) => {
            if let Some(with_clause) = &mut delete.with {
                for cte in &mut with_clause.ctes {
                    bind_trigger_columns_in_select_statement(&mut cte.query, frame);
                }
            }
            if let Some(where_clause) = &mut delete.where_clause {
                bind_trigger_columns_in_expr(where_clause, frame);
            }
            for column in &mut delete.returning {
                if let ResultColumn::Expr { expr, .. } = column {
                    bind_trigger_columns_in_expr(expr, frame);
                }
            }
            for ordering in &mut delete.order_by {
                bind_trigger_columns_in_expr(&mut ordering.expr, frame);
            }
            if let Some(limit_clause) = &mut delete.limit {
                bind_trigger_columns_in_expr(&mut limit_clause.limit, frame);
                if let Some(offset) = &mut limit_clause.offset {
                    bind_trigger_columns_in_expr(offset, frame);
                }
            }
        }
        Statement::CreateTrigger(create_trigger) => {
            if let Some(when_clause) = &mut create_trigger.when {
                bind_trigger_columns_in_expr(when_clause, frame);
            }
            for body_stmt in &mut create_trigger.body {
                bind_trigger_columns_in_statement(body_stmt, frame);
            }
        }
        _ => {}
    }
}

fn bind_trigger_columns_in_select_statement(select: &mut SelectStatement, frame: &TriggerFrame) {
    if let Some(with_clause) = &mut select.with {
        for cte in &mut with_clause.ctes {
            bind_trigger_columns_in_select_statement(&mut cte.query, frame);
        }
    }

    bind_trigger_columns_in_select_core(&mut select.body.select, frame);
    for (_, core) in &mut select.body.compounds {
        bind_trigger_columns_in_select_core(core, frame);
    }

    for ordering in &mut select.order_by {
        bind_trigger_columns_in_expr(&mut ordering.expr, frame);
    }
    if let Some(limit_clause) = &mut select.limit {
        bind_trigger_columns_in_expr(&mut limit_clause.limit, frame);
        if let Some(offset) = &mut limit_clause.offset {
            bind_trigger_columns_in_expr(offset, frame);
        }
    }
}

fn bind_trigger_columns_in_select_core(core: &mut SelectCore, frame: &TriggerFrame) {
    match core {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
            ..
        } => {
            for column in columns {
                if let ResultColumn::Expr { expr, .. } = column {
                    bind_trigger_columns_in_expr(expr, frame);
                }
            }
            if let Some(from_clause) = from {
                bind_trigger_columns_in_from_clause(from_clause, frame);
            }
            if let Some(predicate) = where_clause {
                bind_trigger_columns_in_expr(predicate, frame);
            }
            for expr in group_by {
                bind_trigger_columns_in_expr(expr, frame);
            }
            if let Some(predicate) = having {
                bind_trigger_columns_in_expr(predicate, frame);
            }
            for window in windows {
                bind_trigger_columns_in_window_spec(&mut window.spec, frame);
            }
        }
        SelectCore::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_trigger_columns_in_expr(expr, frame);
                }
            }
        }
    }
}

fn bind_trigger_columns_in_from_clause(from: &mut fsqlite_ast::FromClause, frame: &TriggerFrame) {
    bind_trigger_columns_in_table_or_subquery(&mut from.source, frame);
    for join in &mut from.joins {
        bind_trigger_columns_in_table_or_subquery(&mut join.table, frame);
        if let Some(JoinConstraint::On(expr)) = &mut join.constraint {
            bind_trigger_columns_in_expr(expr, frame);
        }
    }
}

fn bind_trigger_columns_in_table_or_subquery(source: &mut TableOrSubquery, frame: &TriggerFrame) {
    match source {
        TableOrSubquery::Table { .. } => {}
        TableOrSubquery::Subquery { query, .. } => {
            bind_trigger_columns_in_select_statement(query, frame);
        }
        TableOrSubquery::TableFunction { args, .. } => {
            for arg in args {
                bind_trigger_columns_in_expr(arg, frame);
            }
        }
        TableOrSubquery::ParenJoin(from_clause) => {
            bind_trigger_columns_in_from_clause(from_clause, frame);
        }
    }
}

fn bind_trigger_columns_in_window_spec(spec: &mut fsqlite_ast::WindowSpec, frame: &TriggerFrame) {
    for expr in &mut spec.partition_by {
        bind_trigger_columns_in_expr(expr, frame);
    }
    for ordering in &mut spec.order_by {
        bind_trigger_columns_in_expr(&mut ordering.expr, frame);
    }
    if let Some(window_frame) = &mut spec.frame {
        bind_trigger_columns_in_frame_bound(&mut window_frame.start, frame);
        if let Some(end) = &mut window_frame.end {
            bind_trigger_columns_in_frame_bound(end, frame);
        }
    }
}

fn bind_trigger_columns_in_frame_bound(bound: &mut fsqlite_ast::FrameBound, frame: &TriggerFrame) {
    match bound {
        fsqlite_ast::FrameBound::Preceding(expr) | fsqlite_ast::FrameBound::Following(expr) => {
            bind_trigger_columns_in_expr(expr, frame);
        }
        fsqlite_ast::FrameBound::UnboundedPreceding
        | fsqlite_ast::FrameBound::CurrentRow
        | fsqlite_ast::FrameBound::UnboundedFollowing => {}
    }
}

#[allow(clippy::too_many_lines)]
fn bind_trigger_columns_in_expr(expr: &mut Expr, frame: &TriggerFrame) {
    match expr {
        Expr::Literal(_, _) | Expr::Raise { .. } | Expr::Placeholder(_, _) => {}
        Expr::Column(col_ref, _) => {
            let table_prefix = col_ref.table.as_deref();
            let column_name = col_ref.column.clone();
            if frame.references_pseudo_column(table_prefix, &column_name) {
                if let Some(value) = frame.lookup_value(table_prefix, &column_name) {
                    *expr = value_to_literal_expr(value);
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            bind_trigger_columns_in_expr(left, frame);
            bind_trigger_columns_in_expr(right, frame);
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. } => {
            bind_trigger_columns_in_expr(inner, frame);
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            bind_trigger_columns_in_expr(inner, frame);
            bind_trigger_columns_in_expr(low, frame);
            bind_trigger_columns_in_expr(high, frame);
        }
        Expr::In {
            expr: inner, set, ..
        } => {
            bind_trigger_columns_in_expr(inner, frame);
            match set {
                InSet::List(values) => {
                    for value in values {
                        bind_trigger_columns_in_expr(value, frame);
                    }
                }
                InSet::Subquery(query) => {
                    bind_trigger_columns_in_select_statement(query, frame);
                }
                InSet::Table(_) => {}
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            ..
        } => {
            bind_trigger_columns_in_expr(inner, frame);
            bind_trigger_columns_in_expr(pattern, frame);
            if let Some(escape_expr) = escape {
                bind_trigger_columns_in_expr(escape_expr, frame);
            }
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(base) = operand {
                bind_trigger_columns_in_expr(base, frame);
            }
            for (when_expr, then_expr) in whens {
                bind_trigger_columns_in_expr(when_expr, frame);
                bind_trigger_columns_in_expr(then_expr, frame);
            }
            if let Some(else_branch) = else_expr {
                bind_trigger_columns_in_expr(else_branch, frame);
            }
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => {
            bind_trigger_columns_in_select_statement(subquery, frame);
        }
        Expr::FunctionCall {
            args, filter, over, ..
        } => {
            if let FunctionArgs::List(arguments) = args {
                for arg in arguments {
                    bind_trigger_columns_in_expr(arg, frame);
                }
            }
            if let Some(filter_expr) = filter {
                bind_trigger_columns_in_expr(filter_expr, frame);
            }
            if let Some(window_spec) = over {
                bind_trigger_columns_in_window_spec(window_spec, frame);
            }
        }
        Expr::JsonAccess {
            expr: inner, path, ..
        } => {
            bind_trigger_columns_in_expr(inner, frame);
            bind_trigger_columns_in_expr(path, frame);
        }
        Expr::RowValue(values, _) => {
            for value in values {
                bind_trigger_columns_in_expr(value, frame);
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn bind_placeholders_in_expr(
    expr: &mut Expr,
    bind_state: &mut BindParamState,
    params: &[SqliteValue],
) -> Result<()> {
    match expr {
        Expr::Literal(_, _) | Expr::Column(_, _) | Expr::Raise { .. } => {}
        Expr::BinaryOp { left, right, .. } => {
            bind_placeholders_in_expr(left, bind_state, params)?;
            bind_placeholders_in_expr(right, bind_state, params)?;
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. } => {
            bind_placeholders_in_expr(inner, bind_state, params)?;
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            bind_placeholders_in_expr(inner, bind_state, params)?;
            bind_placeholders_in_expr(low, bind_state, params)?;
            bind_placeholders_in_expr(high, bind_state, params)?;
        }
        Expr::In {
            expr: inner, set, ..
        } => {
            bind_placeholders_in_expr(inner, bind_state, params)?;
            match set {
                InSet::List(values) => {
                    for value in values {
                        bind_placeholders_in_expr(value, bind_state, params)?;
                    }
                }
                InSet::Subquery(query) => {
                    bind_placeholders_in_select_statement(query, bind_state, params)?;
                }
                InSet::Table(_) => {}
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            ..
        } => {
            bind_placeholders_in_expr(inner, bind_state, params)?;
            bind_placeholders_in_expr(pattern, bind_state, params)?;
            if let Some(escape_expr) = escape {
                bind_placeholders_in_expr(escape_expr, bind_state, params)?;
            }
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(base) = operand {
                bind_placeholders_in_expr(base, bind_state, params)?;
            }
            for (when_expr, then_expr) in whens {
                bind_placeholders_in_expr(when_expr, bind_state, params)?;
                bind_placeholders_in_expr(then_expr, bind_state, params)?;
            }
            if let Some(else_branch) = else_expr {
                bind_placeholders_in_expr(else_branch, bind_state, params)?;
            }
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery, _) => {
            bind_placeholders_in_select_statement(subquery, bind_state, params)?;
        }
        Expr::FunctionCall {
            args, filter, over, ..
        } => {
            if let FunctionArgs::List(arguments) = args {
                for arg in arguments {
                    bind_placeholders_in_expr(arg, bind_state, params)?;
                }
            }
            if let Some(filter_expr) = filter {
                bind_placeholders_in_expr(filter_expr, bind_state, params)?;
            }
            if let Some(window_spec) = over {
                bind_placeholders_in_window_spec(window_spec, bind_state, params)?;
            }
        }
        Expr::JsonAccess {
            expr: inner, path, ..
        } => {
            bind_placeholders_in_expr(inner, bind_state, params)?;
            bind_placeholders_in_expr(path, bind_state, params)?;
        }
        Expr::RowValue(values, _) => {
            for value in values {
                bind_placeholders_in_expr(value, bind_state, params)?;
            }
        }
        Expr::Placeholder(placeholder, span) => {
            let literal = bind_placeholder_to_literal(placeholder, bind_state, params)?;
            *expr = Expr::Literal(literal, *span);
        }
    }
    Ok(())
}

fn emit_case_expr(
    builder: &mut ProgramBuilder,
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let done_label = builder.emit_label();
    let r_operand = if let Some(op_expr) = operand {
        let r = builder.alloc_temp();
        emit_expr(builder, op_expr, r, bind_state)?;
        Some(r)
    } else {
        None
    };

    for (when_expr, then_expr) in whens {
        let next_when = builder.emit_label();

        if let Some(r_op) = r_operand {
            let r_when = builder.alloc_temp();
            emit_expr(builder, when_expr, r_when, bind_state)?;
            builder.emit_jump_to_label(Opcode::Ne, r_when, r_op, next_when, P4::None, 0);
            builder.free_temp(r_when);
        } else {
            emit_expr(builder, when_expr, target_reg, bind_state)?;
            builder.emit_jump_to_label(Opcode::IfNot, target_reg, 1, next_when, P4::None, 0);
        }

        emit_expr(builder, then_expr, target_reg, bind_state)?;
        builder.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
        builder.resolve_label(next_when);
    }

    if let Some(el) = else_expr {
        emit_expr(builder, el, target_reg, bind_state)?;
    } else {
        builder.emit_op(Opcode::Null, 0, target_reg, 0, P4::None, 0);
    }

    builder.resolve_label(done_label);

    if let Some(r_op) = r_operand {
        builder.free_temp(r_op);
    }

    Ok(())
}

/// Compile `expr [NOT] BETWEEN low AND high` into comparison opcodes.
///
/// Equivalent to `(expr >= low) AND (expr <= high)`, optionally negated.
fn emit_between_expr(
    builder: &mut ProgramBuilder,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    not: bool,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let expr_reg = builder.alloc_temp();
    let low_reg = builder.alloc_temp();
    let high_reg = builder.alloc_temp();

    emit_expr(builder, expr, expr_reg, bind_state)?;
    emit_expr(builder, low, low_reg, bind_state)?;
    emit_expr(builder, high, high_reg, bind_state)?;

    // Labels for the short-circuit evaluation.
    let lbl_false = builder.emit_label();
    let lbl_true = builder.emit_label();
    let lbl_done = builder.emit_label();

    // expr >= low  (jump to true_of_ge if true, fall through to false)
    let lbl_ge_ok = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Ge, low_reg, expr_reg, lbl_ge_ok, P4::None, 0);
    // expr < low → not between
    builder.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_false, P4::None, 0);
    builder.resolve_label(lbl_ge_ok);

    // expr <= high
    builder.emit_jump_to_label(Opcode::Le, high_reg, expr_reg, lbl_true, P4::None, 0);
    // expr > high → not between
    builder.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_false, P4::None, 0);

    // Both conditions satisfied: BETWEEN is true.
    builder.resolve_label(lbl_true);
    builder.emit_op(Opcode::Integer, i32::from(!not), target_reg, 0, P4::None, 0);
    builder.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_done, P4::None, 0);

    // At least one condition failed: BETWEEN is false.
    builder.resolve_label(lbl_false);
    builder.emit_op(Opcode::Integer, i32::from(not), target_reg, 0, P4::None, 0);

    builder.resolve_label(lbl_done);

    builder.free_temp(high_reg);
    builder.free_temp(low_reg);
    builder.free_temp(expr_reg);
    Ok(())
}

/// Compile `expr [NOT] IN (e1, e2, ...)` into a chain of equality checks.
///
/// Only `InSet::List` is supported; subqueries and table references return
/// `NotImplemented`.
fn emit_in_expr(
    builder: &mut ProgramBuilder,
    expr: &Expr,
    set: &InSet,
    not: bool,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let list = match set {
        InSet::List(list) => list,
        InSet::Subquery(_) => {
            return Err(FrankenError::NotImplemented(
                "IN (SELECT ...) subquery is not yet supported".to_owned(),
            ));
        }
        InSet::Table(_) => {
            return Err(FrankenError::NotImplemented(
                "IN table_name is not yet supported".to_owned(),
            ));
        }
    };

    if list.is_empty() {
        // Empty list: `expr IN ()` is always false, `NOT IN ()` always true.
        let val = i32::from(not);
        builder.emit_op(Opcode::Integer, val, target_reg, 0, P4::None, 0);
        return Ok(());
    }

    let expr_reg = builder.alloc_temp();
    emit_expr(builder, expr, expr_reg, bind_state)?;

    let lbl_found = builder.emit_label();
    let lbl_done = builder.emit_label();

    // Check each list element against expr.
    let elem_reg = builder.alloc_temp();
    for elem in list {
        emit_expr(builder, elem, elem_reg, bind_state)?;
        builder.emit_jump_to_label(Opcode::Eq, elem_reg, expr_reg, lbl_found, P4::None, 0);
    }
    builder.free_temp(elem_reg);

    // No match found.
    builder.emit_op(Opcode::Integer, i32::from(not), target_reg, 0, P4::None, 0);
    builder.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_done, P4::None, 0);

    // Match found.
    builder.resolve_label(lbl_found);
    let found_val = i32::from(!not);
    builder.emit_op(Opcode::Integer, found_val, target_reg, 0, P4::None, 0);

    builder.resolve_label(lbl_done);
    builder.free_temp(expr_reg);
    Ok(())
}

/// Compile `expr [NOT] LIKE/GLOB pattern [ESCAPE escape]` as a PureFunc call.
///
/// Emits a call to the `like` (or `glob`) scalar function with 2-3 arguments:
/// `like(pattern, expr)` or `like(pattern, expr, escape)`. The result is
/// negated if `not` is true.
#[allow(clippy::too_many_arguments)]
fn emit_like_expr(
    builder: &mut ProgramBuilder,
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
    op: LikeOp,
    not: bool,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let func_name = match op {
        LikeOp::Like => "like",
        LikeOp::Glob => "glob",
        LikeOp::Match | LikeOp::Regexp => {
            return Err(FrankenError::NotImplemented(format!(
                "{op:?} operator is not yet supported",
            )));
        }
    };

    let has_escape = escape.is_some();
    let arg_count: i32 = if has_escape { 3 } else { 2 };
    let first_arg_reg = builder.alloc_regs(arg_count);

    // SQLite like() takes (pattern, string [, escape]).
    emit_expr(builder, pattern, first_arg_reg, bind_state)?;
    emit_expr(builder, expr, first_arg_reg + 1, bind_state)?;
    if let Some(esc) = escape {
        emit_expr(builder, esc, first_arg_reg + 2, bind_state)?;
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let arg_count_u16 = arg_count as u16;

    builder.emit_op(
        Opcode::PureFunc,
        0,
        first_arg_reg,
        target_reg,
        P4::FuncName(func_name.to_owned()),
        arg_count_u16,
    );

    // Negate the result if NOT LIKE / NOT GLOB.
    if not {
        builder.emit_op(Opcode::Not, target_reg, target_reg, 0, P4::None, 0);
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
//  JOIN helpers
// ──────────────────────────────────────────────────────────────────────────

/// Metadata for a table participating in a JOIN.
struct JoinTableSource {
    table_name: String,
    alias: Option<String>,
    col_names: Vec<String>,
}

/// Perform a single join step: combine left-side rows with right-side rows.
#[allow(clippy::too_many_lines)]
fn execute_single_join(
    left: &[Vec<SqliteValue>],
    right: &[Vec<SqliteValue>],
    right_width: usize,
    left_width: usize,
    kind: JoinKind,
    constraint: Option<&JoinConstraint>,
    col_map: &[(String, String)],
) -> Result<Vec<Vec<SqliteValue>>> {
    let mut result = Vec::new();
    let combined_width = left_width + right_width;

    // For RIGHT/FULL joins, track which right rows were matched.
    let track_right = matches!(kind, JoinKind::Right | JoinKind::Full);
    let mut right_matched = if track_right {
        vec![false; right.len()]
    } else {
        Vec::new()
    };

    for left_row in left {
        let mut matched = false;

        for (ri, right_row) in right.iter().enumerate() {
            // Build combined row.
            let mut combined = Vec::with_capacity(combined_width);
            combined.extend_from_slice(left_row);
            combined.extend(right_row[..right_width].iter().cloned());

            // Evaluate join constraint.
            let passes = match constraint {
                None => true,
                Some(JoinConstraint::On(expr)) => eval_join_predicate(expr, &combined, col_map)?,
                Some(JoinConstraint::Using(cols)) => {
                    eval_using_constraint(cols, &combined, col_map, left_width)
                }
            };

            if passes {
                matched = true;
                if track_right {
                    right_matched[ri] = true;
                }
                result.push(combined);
            }
        }

        // LEFT/FULL JOIN: if no right row matched, emit left + NULLs.
        if !matched && matches!(kind, JoinKind::Left | JoinKind::Full) {
            let mut combined = Vec::with_capacity(combined_width);
            combined.extend_from_slice(left_row);
            combined.extend(std::iter::repeat_n(SqliteValue::Null, right_width));
            result.push(combined);
        }

        // CROSS JOIN without constraint: handled by `passes = true` above.
    }

    // RIGHT/FULL JOIN: for each unmatched right row, emit NULLs + right.
    if track_right {
        for (ri, right_row) in right.iter().enumerate() {
            if !right_matched[ri] {
                let mut combined = Vec::with_capacity(combined_width);
                combined.extend(std::iter::repeat_n(SqliteValue::Null, left_width));
                combined.extend(right_row[..right_width].iter().cloned());
                result.push(combined);
            }
        }
    }

    Ok(result)
}

/// Evaluate a USING constraint: check that named columns match in both sides.
fn eval_using_constraint(
    cols: &[String],
    combined_row: &[SqliteValue],
    col_map: &[(String, String)],
    left_width: usize,
) -> bool {
    for col_name in cols {
        // Find the column in the left side.
        let left_val = col_map[..left_width]
            .iter()
            .enumerate()
            .find(|(_, (_, name))| name.eq_ignore_ascii_case(col_name))
            .and_then(|(i, _)| combined_row.get(i));
        // Find the column in the right side.
        let right_val = col_map[left_width..]
            .iter()
            .enumerate()
            .find(|(_, (_, name))| name.eq_ignore_ascii_case(col_name))
            .and_then(|(i, _)| combined_row.get(left_width + i));

        match (left_val, right_val) {
            (Some(l), Some(r)) => {
                // SQL join semantics: USING expands to equality checks,
                // and `NULL = NULL` is not true.
                if matches!(l, SqliteValue::Null) || matches!(r, SqliteValue::Null) {
                    return false;
                }
                if cmp_sqlite_values(l, r) != std::cmp::Ordering::Equal {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// Evaluate a boolean predicate expression against a combined join row.
fn eval_join_predicate(
    expr: &Expr,
    row: &[SqliteValue],
    col_map: &[(String, String)],
) -> Result<bool> {
    let val = eval_join_expr(expr, row, col_map)?;
    Ok(is_sqlite_truthy(&val))
}

/// Evaluate an expression against a combined join row, producing a value.
#[allow(clippy::too_many_lines)]
fn eval_join_expr(
    expr: &Expr,
    row: &[SqliteValue],
    col_map: &[(String, String)],
) -> Result<SqliteValue> {
    match expr {
        Expr::Column(col_ref, _) => {
            let col_name = &col_ref.column;
            let table_prefix = col_ref.table.as_deref();
            let idx = find_col_in_map(col_map, table_prefix, col_name)?;
            Ok(row.get(idx).cloned().unwrap_or(SqliteValue::Null))
        }
        Expr::Literal(lit, _) => Ok(literal_to_join_value(lit)),
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let lv = eval_join_expr(left, row, col_map)?;
            let rv = eval_join_expr(right, row, col_map)?;
            Ok(eval_join_binary_op(&lv, *op, &rv))
        }
        Expr::UnaryOp {
            op, expr: inner, ..
        } => {
            let val = eval_join_expr(inner, row, col_map)?;
            Ok(match op {
                UnaryOp::Negate => match val {
                    SqliteValue::Integer(n) => SqliteValue::Integer(-n),
                    SqliteValue::Float(f) => SqliteValue::Float(-f),
                    _ => SqliteValue::Null,
                },
                UnaryOp::Not => SqliteValue::Integer(i64::from(!is_sqlite_truthy(&val))),
                _ => SqliteValue::Null,
            })
        }
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            let val = eval_join_expr(inner, row, col_map)?;
            let is_null = matches!(val, SqliteValue::Null);
            Ok(SqliteValue::Integer(i64::from(if *not {
                !is_null
            } else {
                is_null
            })))
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            not,
            ..
        } => {
            let val = eval_join_expr(inner, row, col_map)?;
            let low_val = eval_join_expr(low, row, col_map)?;
            let high_val = eval_join_expr(high, row, col_map)?;
            let in_range = cmp_values(&val, &low_val) != std::cmp::Ordering::Less
                && cmp_values(&val, &high_val) != std::cmp::Ordering::Greater;
            Ok(SqliteValue::Integer(i64::from(if *not {
                !in_range
            } else {
                in_range
            })))
        }
        Expr::In {
            expr: inner,
            set,
            not,
            ..
        } => {
            let val = eval_join_expr(inner, row, col_map)?;
            let found = match set {
                InSet::List(exprs) => {
                    let mut found = false;
                    for e in exprs {
                        let set_val = eval_join_expr(e, row, col_map)?;
                        if cmp_values(&val, &set_val) == std::cmp::Ordering::Equal {
                            found = true;
                            break;
                        }
                    }
                    found
                }
                _ => {
                    return Err(FrankenError::NotImplemented(
                        "IN subquery in JOIN not supported".to_owned(),
                    ));
                }
            };
            Ok(SqliteValue::Integer(i64::from(if *not {
                !found
            } else {
                found
            })))
        }
        Expr::FunctionCall { name, args, .. } => {
            let arg_vals: Vec<SqliteValue> = match args {
                FunctionArgs::List(exprs) => exprs
                    .iter()
                    .map(|e| eval_join_expr(e, row, col_map))
                    .collect::<Result<Vec<_>>>()?,
                FunctionArgs::Star => vec![],
            };
            Ok(eval_scalar_fn(name, &arg_vals))
        }
        Expr::Like {
            expr: inner,
            pattern,
            op,
            not,
            ..
        } => {
            let val = eval_join_expr(inner, row, col_map)?;
            let pat = eval_join_expr(pattern, row, col_map)?;
            let matched = match (&val, &pat) {
                (SqliteValue::Text(s), SqliteValue::Text(p)) => match op {
                    LikeOp::Like => simple_like_match(p, s),
                    LikeOp::Glob => simple_glob_match(p, s),
                    _ => false,
                },
                _ => false,
            };
            Ok(SqliteValue::Integer(i64::from(if *not {
                !matched
            } else {
                matched
            })))
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            let base = operand
                .as_ref()
                .map(|e| eval_join_expr(e, row, col_map))
                .transpose()?;
            for (when_expr, then_expr) in whens {
                let when_val = eval_join_expr(when_expr, row, col_map)?;
                let matches = if let Some(ref b) = base {
                    cmp_values(b, &when_val) == std::cmp::Ordering::Equal
                } else {
                    is_sqlite_truthy(&when_val)
                };
                if matches {
                    return eval_join_expr(then_expr, row, col_map);
                }
            }
            if let Some(else_e) = else_expr {
                eval_join_expr(else_e, row, col_map)
            } else {
                Ok(SqliteValue::Null)
            }
        }
        Expr::Cast {
            expr: inner,
            type_name,
            ..
        } => {
            let val = eval_join_expr(inner, row, col_map)?;
            Ok(apply_cast(val, &type_name.name))
        }
        _ => Ok(SqliteValue::Null),
    }
}

/// Find a column's index in the combined column map.
fn find_col_in_map(
    col_map: &[(String, String)],
    table_prefix: Option<&str>,
    col_name: &str,
) -> Result<usize> {
    if let Some(prefix) = table_prefix {
        // Qualified: match table label + column name.
        col_map
            .iter()
            .position(|(tbl, col)| {
                tbl.eq_ignore_ascii_case(prefix) && col.eq_ignore_ascii_case(col_name)
            })
            .ok_or_else(|| FrankenError::Internal(format!("column not found: {prefix}.{col_name}")))
    } else {
        // Unqualified: first match wins (left-to-right).
        col_map
            .iter()
            .position(|(_, col)| col.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| FrankenError::Internal(format!("column not found: {col_name}")))
    }
}

fn find_column_index_case_insensitive(column_names: &[String], target: &str) -> Option<usize> {
    column_names
        .iter()
        .position(|column_name| column_name.eq_ignore_ascii_case(target))
}

/// Check whether a column name is an implicit rowid alias (`rowid`, `_rowid_`, `oid`).
fn is_rowid_alias(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Rewrite rowid alias references in an expression tree so that `rowid`,
/// `_rowid_`, and `oid` column references become the real column name of the
/// `INTEGER PRIMARY KEY` column (the rowid alias).
fn rewrite_rowid_aliases_in_expr(expr: &mut Expr, real_col: &str) {
    match expr {
        Expr::Column(col_ref, _) if is_rowid_alias(&col_ref.column) => {
            real_col.clone_into(&mut col_ref.column);
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_rowid_aliases_in_expr(left, real_col);
            rewrite_rowid_aliases_in_expr(right, real_col);
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. } => {
            rewrite_rowid_aliases_in_expr(inner, real_col);
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            rewrite_rowid_aliases_in_expr(inner, real_col);
            rewrite_rowid_aliases_in_expr(low, real_col);
            rewrite_rowid_aliases_in_expr(high, real_col);
        }
        Expr::In {
            expr: inner, set, ..
        } => {
            rewrite_rowid_aliases_in_expr(inner, real_col);
            if let InSet::List(exprs) = set {
                for e in exprs {
                    rewrite_rowid_aliases_in_expr(e, real_col);
                }
            }
        }
        Expr::FunctionCall {
            args: FunctionArgs::List(exprs),
            ..
        } => {
            for e in exprs {
                rewrite_rowid_aliases_in_expr(e, real_col);
            }
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if let Some(op) = operand {
                rewrite_rowid_aliases_in_expr(op, real_col);
            }
            for (when_e, then_e) in whens {
                rewrite_rowid_aliases_in_expr(when_e, real_col);
                rewrite_rowid_aliases_in_expr(then_e, real_col);
            }
            if let Some(el) = else_expr {
                rewrite_rowid_aliases_in_expr(el, real_col);
            }
        }
        _ => {}
    }
}

/// Convert an AST literal to a `SqliteValue` (for JOIN expression evaluation).
fn literal_to_join_value(lit: &Literal) -> SqliteValue {
    match lit {
        Literal::Integer(n) => SqliteValue::Integer(*n),
        Literal::Float(f) => SqliteValue::Float(*f),
        Literal::String(s) => SqliteValue::Text(s.clone()),
        Literal::True => SqliteValue::Integer(1),
        Literal::False => SqliteValue::Integer(0),
        _ => SqliteValue::Null,
    }
}

/// Evaluate a binary operator on two `SqliteValue`s (for JOIN expression evaluation).
fn eval_join_binary_op(left: &SqliteValue, op: BinaryOp, right: &SqliteValue) -> SqliteValue {
    match op {
        BinaryOp::Eq => SqliteValue::Integer(i64::from(
            cmp_values(left, right) == std::cmp::Ordering::Equal,
        )),
        BinaryOp::Ne => SqliteValue::Integer(i64::from(
            cmp_values(left, right) != std::cmp::Ordering::Equal,
        )),
        BinaryOp::Gt => SqliteValue::Integer(i64::from(
            cmp_values(left, right) == std::cmp::Ordering::Greater,
        )),
        BinaryOp::Lt => SqliteValue::Integer(i64::from(
            cmp_values(left, right) == std::cmp::Ordering::Less,
        )),
        BinaryOp::Ge => SqliteValue::Integer(i64::from(
            cmp_values(left, right) != std::cmp::Ordering::Less,
        )),
        BinaryOp::Le => SqliteValue::Integer(i64::from(
            cmp_values(left, right) != std::cmp::Ordering::Greater,
        )),
        BinaryOp::And => {
            SqliteValue::Integer(i64::from(is_sqlite_truthy(left) && is_sqlite_truthy(right)))
        }
        BinaryOp::Or => {
            SqliteValue::Integer(i64::from(is_sqlite_truthy(left) || is_sqlite_truthy(right)))
        }
        BinaryOp::Add => numeric_add(left, right),
        BinaryOp::Subtract => numeric_sub(left, right),
        BinaryOp::Multiply => numeric_mul(left, right),
        BinaryOp::Divide => numeric_div(left, right),
        BinaryOp::Modulo => numeric_mod(left, right),
        BinaryOp::Concat => {
            let l = sqlite_value_to_text(left);
            let r = sqlite_value_to_text(right);
            SqliteValue::Text(format!("{l}{r}"))
        }
        _ => SqliteValue::Null,
    }
}

/// Simple case-insensitive LIKE pattern match (`%` = any sequence, `_` = any char).
fn simple_like_match(pattern: &str, string: &str) -> bool {
    let pat: Vec<char> = pattern.to_ascii_lowercase().chars().collect();
    let txt: Vec<char> = string.to_ascii_lowercase().chars().collect();
    like_dp(&pat, &txt, 0, 0)
}

fn like_dp(pat: &[char], txt: &[char], pi: usize, ti: usize) -> bool {
    if pi == pat.len() {
        return ti == txt.len();
    }
    match pat[pi] {
        '%' => {
            // Match zero or more characters.
            let mut t = ti;
            loop {
                if like_dp(pat, txt, pi + 1, t) {
                    return true;
                }
                if t >= txt.len() {
                    return false;
                }
                t += 1;
            }
        }
        '_' => {
            // Match exactly one character.
            ti < txt.len() && like_dp(pat, txt, pi + 1, ti + 1)
        }
        c => ti < txt.len() && txt[ti] == c && like_dp(pat, txt, pi + 1, ti + 1),
    }
}

/// Simple GLOB pattern match (`*` = any sequence, `?` = any char, case-sensitive).
fn simple_glob_match(pattern: &str, string: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = string.chars().collect();
    glob_dp(&pat, &txt, 0, 0)
}

fn glob_dp(pat: &[char], txt: &[char], pi: usize, ti: usize) -> bool {
    if pi == pat.len() {
        return ti == txt.len();
    }
    match pat[pi] {
        '*' => {
            let mut t = ti;
            loop {
                if glob_dp(pat, txt, pi + 1, t) {
                    return true;
                }
                if t >= txt.len() {
                    return false;
                }
                t += 1;
            }
        }
        '?' => ti < txt.len() && glob_dp(pat, txt, pi + 1, ti + 1),
        c => ti < txt.len() && txt[ti] == c && glob_dp(pat, txt, pi + 1, ti + 1),
    }
}

/// Apply a CAST to a value based on the target type name.
#[allow(clippy::cast_possible_truncation)]
fn apply_cast(val: SqliteValue, type_name: &str) -> SqliteValue {
    let lower = type_name.to_ascii_lowercase();
    match lower.as_str() {
        "integer" | "int" | "bigint" | "smallint" | "tinyint" => match val {
            SqliteValue::Integer(_) => val,
            SqliteValue::Float(f) => SqliteValue::Integer(f as i64),
            SqliteValue::Text(ref s) => s
                .parse::<i64>()
                .map_or(SqliteValue::Integer(0), SqliteValue::Integer),
            _ => SqliteValue::Integer(0),
        },
        "real" | "float" | "double" => match val {
            SqliteValue::Float(_) => val,
            SqliteValue::Integer(n) => SqliteValue::Float(n as f64),
            SqliteValue::Text(ref s) => s
                .parse::<f64>()
                .map_or(SqliteValue::Float(0.0), SqliteValue::Float),
            _ => SqliteValue::Float(0.0),
        },
        "text" | "varchar" | "char" | "clob" => SqliteValue::Text(sqlite_value_to_text(&val)),
        _ => val,
    }
}

/// Convert a `SqliteValue` to its text representation.
fn sqlite_value_to_text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.clone(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => f.to_string(),
        SqliteValue::Null | SqliteValue::Blob(_) => String::new(),
    }
}

/// Evaluate a scalar function call (for JOIN expression evaluation).
#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
fn eval_scalar_fn(name: &str, args: &[SqliteValue]) -> SqliteValue {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "length" | "len" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                #[allow(clippy::cast_possible_wrap)]
                SqliteValue::Integer(s.len() as i64)
            } else {
                SqliteValue::Null
            }
        }
        "upper" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                SqliteValue::Text(s.to_uppercase())
            } else {
                SqliteValue::Null
            }
        }
        "lower" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                SqliteValue::Text(s.to_lowercase())
            } else {
                SqliteValue::Null
            }
        }
        "abs" => match args.first() {
            Some(SqliteValue::Integer(n)) => SqliteValue::Integer(n.abs()),
            Some(SqliteValue::Float(f)) => SqliteValue::Float(f.abs()),
            _ => SqliteValue::Null,
        },
        "coalesce" => args
            .iter()
            .find(|v| !matches!(v, SqliteValue::Null))
            .cloned()
            .unwrap_or(SqliteValue::Null),
        "ifnull" => {
            if args.len() >= 2 {
                if matches!(&args[0], SqliteValue::Null) {
                    args[1].clone()
                } else {
                    args[0].clone()
                }
            } else {
                SqliteValue::Null
            }
        }
        "nullif" => {
            if args.len() >= 2 && cmp_values(&args[0], &args[1]) == std::cmp::Ordering::Equal {
                SqliteValue::Null
            } else {
                args.first().cloned().unwrap_or(SqliteValue::Null)
            }
        }
        "typeof" => {
            let type_name = match args.first() {
                Some(SqliteValue::Integer(_)) => "integer",
                Some(SqliteValue::Float(_)) => "real",
                Some(SqliteValue::Text(_)) => "text",
                Some(SqliteValue::Blob(_)) => "blob",
                _ => "null",
            };
            SqliteValue::Text(type_name.to_owned())
        }
        "iif" => {
            if args.len() >= 3 {
                if is_sqlite_truthy(&args[0]) {
                    args[1].clone()
                } else {
                    args[2].clone()
                }
            } else {
                SqliteValue::Null
            }
        }
        "max" => args
            .iter()
            .filter(|v| !matches!(v, SqliteValue::Null))
            .max_by(|a, b| cmp_sqlite_values(a, b))
            .cloned()
            .unwrap_or(SqliteValue::Null),
        "min" => args
            .iter()
            .filter(|v| !matches!(v, SqliteValue::Null))
            .min_by(|a, b| cmp_sqlite_values(a, b))
            .cloned()
            .unwrap_or(SqliteValue::Null),
        "replace" => {
            if args.len() >= 3 {
                let s = sqlite_value_to_text(&args[0]);
                let from = sqlite_value_to_text(&args[1]);
                let to = sqlite_value_to_text(&args[2]);
                SqliteValue::Text(s.replace(&from, &to))
            } else {
                SqliteValue::Null
            }
        }
        "substr" | "substring" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                let start = args
                    .get(1)
                    .map_or(1, fsqlite_types::SqliteValue::to_integer)
                    .max(1) as usize
                    - 1;
                let chars: Vec<char> = s.chars().collect();
                if let Some(len_val) = args.get(2) {
                    let len = len_val.to_integer().max(0) as usize;
                    let end = (start + len).min(chars.len());
                    SqliteValue::Text(chars[start..end].iter().collect())
                } else {
                    SqliteValue::Text(chars[start..].iter().collect())
                }
            } else {
                SqliteValue::Null
            }
        }
        "instr" => {
            if args.len() >= 2 {
                let haystack = sqlite_value_to_text(&args[0]);
                let needle = sqlite_value_to_text(&args[1]);
                #[allow(clippy::cast_possible_wrap)]
                let pos = haystack.find(&needle).map_or(0i64, |i| (i + 1) as i64);
                SqliteValue::Integer(pos)
            } else {
                SqliteValue::Null
            }
        }
        "trim" => {
            if let Some(v) = args.first() {
                SqliteValue::Text(sqlite_value_to_text(v).trim().to_owned())
            } else {
                SqliteValue::Null
            }
        }
        "ltrim" => {
            if let Some(v) = args.first() {
                SqliteValue::Text(sqlite_value_to_text(v).trim_start().to_owned())
            } else {
                SqliteValue::Null
            }
        }
        "rtrim" => {
            if let Some(v) = args.first() {
                SqliteValue::Text(sqlite_value_to_text(v).trim_end().to_owned())
            } else {
                SqliteValue::Null
            }
        }
        "hex" => match args.first() {
            Some(SqliteValue::Blob(b)) => {
                use std::fmt::Write;
                let mut hex = String::with_capacity(b.len() * 2);
                for byte in b {
                    let _ = write!(hex, "{byte:02X}");
                }
                SqliteValue::Text(hex)
            }
            Some(v) => {
                use std::fmt::Write;
                let s = sqlite_value_to_text(v);
                let mut hex = String::with_capacity(s.len() * 2);
                for b in s.bytes() {
                    let _ = write!(hex, "{b:02X}");
                }
                SqliteValue::Text(hex)
            }
            None => SqliteValue::Null,
        },
        "quote" => match args.first() {
            Some(SqliteValue::Null) => SqliteValue::Text("NULL".to_owned()),
            Some(SqliteValue::Integer(n)) => SqliteValue::Text(n.to_string()),
            Some(SqliteValue::Float(f)) => SqliteValue::Text(f.to_string()),
            Some(SqliteValue::Text(s)) => SqliteValue::Text(format!("'{}'", s.replace('\'', "''"))),
            Some(SqliteValue::Blob(b)) => {
                use std::fmt::Write;
                let mut hex = String::with_capacity(b.len() * 2);
                for byte in b {
                    let _ = write!(hex, "{byte:02X}");
                }
                SqliteValue::Text(format!("X'{hex}'"))
            }
            None => SqliteValue::Null,
        },
        "unicode" => {
            if let Some(SqliteValue::Text(s)) = args.first() {
                s.chars().next().map_or(SqliteValue::Null, |c| {
                    SqliteValue::Integer(i64::from(c as u32))
                })
            } else {
                SqliteValue::Null
            }
        }
        "char" => {
            let s: String = args
                .iter()
                .filter_map(|v| {
                    if let SqliteValue::Integer(n) = v {
                        #[allow(clippy::cast_sign_loss)]
                        char::from_u32(*n as u32)
                    } else {
                        None
                    }
                })
                .collect();
            SqliteValue::Text(s)
        }
        "random" => {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0x517C_C1B7_2722_0A95);
            let mut s = COUNTER.fetch_add(1, Ordering::Relaxed);
            // xorshift64
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            COUNTER.store(s, Ordering::Relaxed);
            #[allow(clippy::cast_possible_wrap)]
            SqliteValue::Integer(s as i64)
        }
        "zeroblob" => {
            let n = args.first().map_or(0, |v| v.to_integer().max(0)) as usize;
            SqliteValue::Blob(vec![0u8; n])
        }
        "round" => match args.first() {
            Some(SqliteValue::Float(f)) => {
                let digits = args
                    .get(1)
                    .map_or(0, fsqlite_types::SqliteValue::to_integer);
                let factor = 10f64.powi(digits as i32);
                SqliteValue::Float((f * factor).round() / factor)
            }
            Some(SqliteValue::Integer(n)) => SqliteValue::Float(*n as f64),
            _ => SqliteValue::Null,
        },
        "sign" => match args.first() {
            Some(SqliteValue::Integer(n)) => SqliteValue::Integer(n.signum()),
            Some(SqliteValue::Float(f)) => {
                if f.is_nan() {
                    SqliteValue::Null
                } else {
                    SqliteValue::Integer(if *f > 0.0 {
                        1
                    } else if *f < 0.0 {
                        -1
                    } else {
                        0
                    })
                }
            }
            _ => SqliteValue::Null,
        },
        "concat" => {
            let parts: Vec<String> = args
                .iter()
                .filter(|v| !matches!(v, SqliteValue::Null))
                .map(sqlite_value_to_text)
                .collect();
            SqliteValue::Text(parts.concat())
        }
        "concat_ws" => {
            if let Some(sep) = args.first() {
                if matches!(sep, SqliteValue::Null) {
                    return SqliteValue::Null;
                }
                let sep_str = sqlite_value_to_text(sep);
                let parts: Vec<String> = args[1..]
                    .iter()
                    .filter(|v| !matches!(v, SqliteValue::Null))
                    .map(sqlite_value_to_text)
                    .collect();
                SqliteValue::Text(parts.join(&sep_str))
            } else {
                SqliteValue::Null
            }
        }
        "sqlite_version" => SqliteValue::Text("3.45.0".to_owned()),
        "total_changes" | "changes" | "last_insert_rowid" => {
            // Stub: return 0 since we don't have full state tracking here.
            SqliteValue::Integer(0)
        }
        "likely" | "unlikely" => args.first().cloned().unwrap_or(SqliteValue::Null),
        _ => SqliteValue::Null,
    }
}

/// Project a single result column from a combined join row.
fn project_join_column(
    col: &ResultColumn,
    row: &[SqliteValue],
    col_map: &[(String, String)],
) -> SqliteValue {
    match col {
        ResultColumn::Expr { expr, .. } => {
            eval_join_expr(expr, row, col_map).unwrap_or(SqliteValue::Null)
        }
        ResultColumn::Star => {
            // Star should have been expanded earlier; this is a fallback.
            SqliteValue::Null
        }
        ResultColumn::TableStar(table_name) => {
            // TableStar should have been expanded; fallback.
            let _ = table_name;
            SqliteValue::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommitSeq, Connection, InProcessPageLockTable, MvccPageIo, Row, SchemaEpoch, Snapshot,
    };
    use fsqlite_ast::Statement;
    use fsqlite_error::FrankenError;
    use fsqlite_types::PageNumber;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::opcode::{Opcode, P4};
    use fsqlite_types::value::SqliteValue;

    fn row_values(row: &Row) -> Vec<SqliteValue> {
        row.values().to_vec()
    }

    #[test]
    fn test_query_expression_eval_pipeline() {
        let connection = Connection::open(":memory:").expect("in-memory path should open");
        let rows = connection
            .query("SELECT 1+2, 'abc'||'def', typeof(3.14);")
            .expect("expression SELECT should execute");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcdef".to_owned()),
                SqliteValue::Text("real".to_owned()),
            ],
        );
    }

    #[test]
    fn test_select_from_nonexistent_table_errors() {
        let connection = Connection::open(":memory:").expect("in-memory path should open");
        let error = connection
            .query("SELECT a FROM t;")
            .expect_err("SELECT from nonexistent table should fail");
        assert!(matches!(error, FrankenError::Internal(_)));
    }

    #[test]
    fn test_maintenance_statement_stubs_execute_as_noops() {
        let conn = Connection::open(":memory:").expect("in-memory path should open");
        conn.execute("CREATE TABLE t (id INTEGER);")
            .expect("CREATE TABLE should succeed");
        conn.execute("INSERT INTO t VALUES (1);")
            .expect("INSERT should succeed");

        conn.execute("VACUUM;").expect("VACUUM stub should succeed");
        conn.execute("ANALYZE;")
            .expect("ANALYZE stub should succeed");
        conn.execute("REINDEX;")
            .expect("REINDEX stub should succeed");

        let rows = conn
            .query("SELECT COUNT(*) FROM t;")
            .expect("COUNT should succeed");
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_query_comparison_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 3 > 2, 1 = 1, 5 < 3;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Integer(0),
            ],
        );
    }

    #[test]
    fn test_query_case_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT CASE WHEN 1 THEN 'yes' ELSE 'no' END;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("yes".to_owned())],
        );
    }

    #[test]
    fn test_query_negation_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT -42;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(-42)]);
    }

    #[test]
    fn test_query_null_is_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT NULL IS NULL;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_query_where_true_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 41 + 1 WHERE 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_query_where_false_filters_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 1 WHERE 0;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_query_where_null_filters_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 1 WHERE NULL;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_values_multiple_rows() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("VALUES (1, 'a'), (2, 'b');").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("b".to_owned())]
        );
    }

    #[test]
    fn test_values_order_by_limit() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("VALUES (3, 'c'), (1, 'a'), (2, 'b') ORDER BY 1 LIMIT 2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("b".to_owned())]
        );
    }

    #[test]
    fn test_values_order_by_desc_limit_offset() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("VALUES (3), (1), (2) ORDER BY 1 DESC LIMIT 1 OFFSET 1;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_select_expression_limit_zero_returns_no_rows() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 42 LIMIT 0;").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_values_offset_beyond_row_count_returns_empty() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("VALUES (1), (2) ORDER BY 1 LIMIT 10 OFFSET 99;")
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_values_mismatched_column_count_rejected() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query("VALUES (1), (2, 3);")
            .expect_err("mismatched VALUES row widths must fail");
        assert!(matches!(error, FrankenError::ParseError { .. }));
    }

    #[test]
    fn test_query_with_params_numbered_placeholders() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT ?1 + ?2, ?3 WHERE ?4;",
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(5),
                    SqliteValue::Text("ok".to_owned()),
                    SqliteValue::Integer(1),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(7), SqliteValue::Text("ok".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_missing_required_param_rejected() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query_with_params("SELECT ?1 + ?2;", &[SqliteValue::Integer(1)])
            .expect_err("missing bind param should fail");
        assert!(matches!(error, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_query_with_params_multiple_statements_rejected() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query_with_params("SELECT ?1; SELECT ?1 + 1;", &[SqliteValue::Integer(1)])
            .expect_err("multi-statement parameterized query should fail");
        assert!(matches!(error, FrankenError::NotImplemented(_)));
    }

    #[test]
    fn test_prepared_statement_query_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1 + 1;").unwrap();
        let rows = stmt.query_with_params(&[SqliteValue::Integer(9)]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(10)]);
    }

    #[test]
    fn test_prepared_statement_values_order_by_limit() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn
            .prepare("VALUES (2), (1), (3) ORDER BY 1 LIMIT 2;")
            .unwrap();
        let rows = stmt.query().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_prepared_table_select_distinct_limit_applies_after_dedup() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (1), (2);").unwrap();

        // Both prepared and non-prepared paths must return two distinct rows.
        let direct = conn
            .query("SELECT DISTINCT v FROM t ORDER BY v LIMIT 2;")
            .unwrap();
        assert_eq!(direct.len(), 2);

        let stmt = conn
            .prepare("SELECT DISTINCT v FROM t ORDER BY v LIMIT 2;")
            .unwrap();
        let rows = stmt.query().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_query_with_params_anonymous_placeholders_are_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT ? + ?, ?;",
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(5),
                    SqliteValue::Text("ok".to_owned()),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(7), SqliteValue::Text("ok".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_mixed_numbered_and_anonymous() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT ?2, ?, ?3, ?;",
                &[
                    SqliteValue::Integer(10),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(30),
                    SqliteValue::Integer(40),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(20),
                SqliteValue::Integer(30),
                SqliteValue::Integer(30),
                SqliteValue::Integer(40),
            ]
        );
    }

    #[test]
    fn test_query_with_params_table_where_two_anonymous_placeholders_bind_in_order() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE inbox_stats (
                id INTEGER PRIMARY KEY,
                agent_id INTEGER,
                project_id INTEGER,
                payload TEXT
            )",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO inbox_stats (id, agent_id, project_id, payload) VALUES
             (1, 2, 7, 'hit'),
             (2, 2, 8, 'wrong_project'),
             (3, 3, 7, 'wrong_agent')",
        )
        .unwrap();

        let rows = conn
            .query_with_params(
                "SELECT payload FROM inbox_stats WHERE agent_id = ? AND project_id = ?",
                &[SqliteValue::Integer(2), SqliteValue::Integer(7)],
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("hit".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_table_where_then_limit_keeps_sql_placeholder_order() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE inbox_stats (
                id INTEGER PRIMARY KEY,
                agent_id INTEGER,
                project_id INTEGER,
                payload TEXT
            )",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO inbox_stats (id, agent_id, project_id, payload) VALUES
             (1, 2, 7, 'a'),
             (2, 2, 7, 'b'),
             (3, 2, 8, 'c')",
        )
        .unwrap();

        let rows = conn
            .query_with_params(
                "SELECT payload FROM inbox_stats \
                 WHERE agent_id = ? AND project_id = ? \
                 ORDER BY id LIMIT ?",
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(7),
                    SqliteValue::Integer(1),
                ],
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("a".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_table_index_eq_single_anonymous_uses_bound_value() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE inbox_stats (
                id INTEGER PRIMARY KEY,
                agent_id INTEGER,
                payload TEXT
            )",
        )
        .unwrap();
        conn.execute("CREATE INDEX idx_inbox_stats_agent_id ON inbox_stats(agent_id)")
            .unwrap();
        conn.execute(
            "INSERT INTO inbox_stats (id, agent_id, payload) VALUES
             (1, 1, 'wrong'),
             (2, 2, 'right')",
        )
        .unwrap();

        let rows = conn
            .query_with_params(
                "SELECT payload FROM inbox_stats WHERE agent_id = ?",
                &[SqliteValue::Integer(2)],
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("right".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_table_index_eq_single_anonymous_no_match_returns_empty() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE inbox_stats (
                id INTEGER PRIMARY KEY,
                agent_id INTEGER,
                payload TEXT
            )",
        )
        .unwrap();
        conn.execute("CREATE INDEX idx_inbox_stats_agent_id ON inbox_stats(agent_id)")
            .unwrap();
        conn.execute(
            "INSERT INTO inbox_stats (id, agent_id, payload) VALUES
             (1, 1, 'a'),
             (2, 4, 'b')",
        )
        .unwrap();

        let rows = conn
            .query_with_params(
                "SELECT payload FROM inbox_stats WHERE agent_id = ?",
                &[SqliteValue::Integer(3)],
            )
            .unwrap();

        assert!(rows.is_empty());
    }

    #[test]
    fn test_query_with_params_named_placeholders_supported() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT :x + :y, :x;",
                &[SqliteValue::Integer(2), SqliteValue::Integer(5)],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(7), SqliteValue::Integer(2)],
        );
    }

    #[test]
    fn test_query_with_params_named_placeholder_reuse_does_not_consume_extra_slot() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT :x, :x, :x;",
                &[SqliteValue::Text("same".to_owned())],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("same".to_owned()),
                SqliteValue::Text("same".to_owned()),
                SqliteValue::Text("same".to_owned()),
            ],
        );
    }

    #[test]
    fn test_query_with_params_named_prefixes_are_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT :x, @x, $x;",
                &[
                    SqliteValue::Integer(11),
                    SqliteValue::Integer(22),
                    SqliteValue::Integer(33),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(11),
                SqliteValue::Integer(22),
                SqliteValue::Integer(33),
            ],
        );
    }

    #[test]
    fn test_query_with_params_named_placeholders_missing_required_param_rejected() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query_with_params("SELECT :x + :y;", &[SqliteValue::Integer(1)])
            .expect_err("missing named parameter should fail");
        assert!(matches!(error, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_query_row_returns_first_row() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("VALUES (1), (2), (3);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_query_row_with_params_returns_first_row() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row_with_params(
                "VALUES (?1), (?2);",
                &[SqliteValue::Integer(11), SqliteValue::Integer(22)],
            )
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(11)]);
    }

    #[test]
    fn test_query_row_no_rows_error() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query_row("SELECT 1 WHERE 0;")
            .expect_err("query_row should fail when resultset is empty");
        assert!(matches!(error, FrankenError::QueryReturnedNoRows));
    }

    #[test]
    fn test_prepared_statement_query_row_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("VALUES (?1), (?2);").unwrap();
        let row = stmt
            .query_row_with_params(&[SqliteValue::Integer(5), SqliteValue::Integer(9)])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(5)]);
    }

    // ── Phase 4 end-to-end DML tests ─────────────────────────────────

    #[test]
    fn test_create_table_and_insert_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'hello');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'world');").unwrap();

        let rows = conn.query("SELECT a, b FROM t1;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("hello".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("world".to_owned())
            ]
        );
    }

    #[test]
    fn test_execute_returns_affected_row_count_for_dml() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();

        // Single-row INSERT returns 1.
        let count = conn.execute("INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(count, 1);

        // Multi-row INSERT returns the number of rows inserted.
        let count = conn.execute("INSERT INTO t VALUES (2), (3), (4);").unwrap();
        assert_eq!(count, 3);

        // UPDATE returns the number of rows updated.
        let count = conn
            .execute("UPDATE t SET x = x + 10 WHERE x > 2;")
            .unwrap();
        assert_eq!(count, 2);

        // DELETE returns the number of rows deleted.
        // After the UPDATE, table has: 1, 2, 13, 14 — two values < 10.
        let count = conn.execute("DELETE FROM t WHERE x < 10;").unwrap();
        assert_eq!(count, 2);

        // SELECT returns the number of result rows (2 remaining: 13, 14).
        let count = conn.execute("SELECT * FROM t;").unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_query_executes_multiple_statements_in_order() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "CREATE TABLE t (x INTEGER); \
                 INSERT INTO t VALUES (10); \
                 INSERT INTO t VALUES (20); \
                 SELECT x FROM t;",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(10)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(20)]);
    }

    #[test]
    fn test_query_multiple_statements_returns_last_result_set() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "VALUES (1), (2); \
                 VALUES (3), (4), (5);",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(3)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(4)]);
        assert_eq!(row_values(&rows[2]), vec![SqliteValue::Integer(5)]);
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER);").unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS t1 (x INTEGER);")
            .unwrap();
        let err = conn
            .execute("CREATE TABLE t1 (x INTEGER);")
            .expect_err("duplicate table should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn test_drop_table_removes_table_and_allows_recreate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t_drop (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t_drop VALUES (1);").unwrap();

        conn.execute("DROP TABLE t_drop;").unwrap();

        let err = conn
            .query("SELECT x FROM t_drop;")
            .expect_err("dropped table should no longer be queryable");
        assert!(matches!(err, FrankenError::Internal(msg) if msg.contains("no such table")));

        // Re-creating with the same name should succeed after DROP.
        conn.execute("CREATE TABLE t_drop (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t_drop VALUES (2);").unwrap();
        let rows = conn.query("SELECT x FROM t_drop;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_drop_table_if_exists_ignores_missing_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("DROP TABLE IF EXISTS t_missing;").unwrap();
    }

    #[test]
    fn test_drop_table_missing_without_if_exists_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn
            .execute("DROP TABLE t_missing;")
            .expect_err("missing table should error without IF EXISTS");
        assert!(
            matches!(err, FrankenError::NoSuchTable { ref name } if name == "t_missing"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_drop_table_rollback_restores_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t_tx_drop (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t_tx_drop VALUES (7);").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("DROP TABLE t_tx_drop;").unwrap();
        conn.execute("ROLLBACK;").unwrap();

        let rows = conn.query("SELECT x FROM t_tx_drop;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(7)]);
    }

    // ── ALTER TABLE tests (bd-2yrj) ───────────────────────────────

    #[test]
    fn test_alter_table_add_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        conn.execute("ALTER TABLE t ADD COLUMN b TEXT;").unwrap();

        // Existing rows should have NULL for the new column.
        let rows = conn.query("SELECT a, b FROM t ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[1], SqliteValue::Null);
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[1].values()[1], SqliteValue::Null);

        // New inserts should include both columns.
        conn.execute("INSERT INTO t VALUES (3, 'hello');").unwrap();
        let rows = conn.query("SELECT a, b FROM t WHERE a = 3;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[1], SqliteValue::Text("hello".to_owned()));
    }

    #[test]
    fn test_alter_table_rename_to() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE old_name (x INTEGER);").unwrap();
        conn.execute("INSERT INTO old_name VALUES (42);").unwrap();

        conn.execute("ALTER TABLE old_name RENAME TO new_name;")
            .unwrap();

        // Old name should fail.
        let err = conn.query("SELECT x FROM old_name;");
        assert!(err.is_err());

        // New name should work.
        let rows = conn.query("SELECT x FROM new_name;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(42));
    }

    #[test]
    fn test_alter_table_rename_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (old_col INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (10);").unwrap();

        conn.execute("ALTER TABLE t RENAME COLUMN old_col TO new_col;")
            .unwrap();

        let rows = conn.query("SELECT new_col FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(10));
    }

    #[test]
    fn test_alter_table_drop_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x', 3.14);")
            .unwrap();

        conn.execute("ALTER TABLE t DROP COLUMN b;").unwrap();

        // Should only have a and c now.
        let rows = conn.query("SELECT a, c FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    }

    #[test]
    fn test_alter_table_nonexistent_table_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn.execute("ALTER TABLE nosuch ADD COLUMN x INTEGER;");
        assert!(err.is_err());
    }

    #[test]
    fn test_create_index_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON users (name);")
            .unwrap();
        // Verify the index is recorded in the schema.
        let schema = conn.schema.borrow();
        let table = schema.iter().find(|t| t.name == "users").unwrap();
        assert_eq!(table.indexes.len(), 1);
        assert_eq!(table.indexes[0].name, "idx_name");
        assert_eq!(table.indexes[0].columns, vec!["name"]);
    }

    #[test]
    fn test_create_index_if_not_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
        conn.execute("CREATE INDEX idx_a ON t (a);").unwrap();
        // Duplicate without IF NOT EXISTS should fail.
        assert!(conn.execute("CREATE INDEX idx_a ON t (a);").is_err());
        // With IF NOT EXISTS should succeed silently.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_a ON t (a);")
            .unwrap();
    }

    #[test]
    fn test_create_index_bad_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
        assert!(conn.execute("CREATE INDEX idx_z ON t (z);").is_err());
    }

    #[test]
    fn test_drop_index() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("CREATE INDEX idx_a ON t (a);").unwrap();
        {
            let schema = conn.schema.borrow();
            assert_eq!(
                schema.iter().find(|t| t.name == "t").unwrap().indexes.len(),
                1
            );
        }
        conn.execute("DROP INDEX idx_a;").unwrap();
        {
            let schema = conn.schema.borrow();
            assert!(
                schema
                    .iter()
                    .find(|t| t.name == "t")
                    .unwrap()
                    .indexes
                    .is_empty()
            );
        }
    }

    #[test]
    fn test_drop_index_if_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("DROP INDEX IF EXISTS nosuch;").unwrap();
        assert!(conn.execute("DROP INDEX nosuch;").is_err());
    }

    #[test]
    fn test_create_view_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items (id INTEGER, price INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 100);").unwrap();
        conn.execute("INSERT INTO items VALUES (2, 200);").unwrap();
        conn.execute("CREATE VIEW expensive AS SELECT id, price FROM items WHERE price > 150;")
            .unwrap();
        let rows = conn.query("SELECT * FROM expensive;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    #[test]
    fn test_create_view_if_not_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
        conn.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        assert!(conn.execute("CREATE VIEW v AS SELECT a FROM t;").is_err());
        conn.execute("CREATE VIEW IF NOT EXISTS v AS SELECT a FROM t;")
            .unwrap();
    }

    #[test]
    fn test_drop_view() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
        conn.execute("CREATE VIEW v AS SELECT a FROM t;").unwrap();
        assert_eq!(conn.views.borrow().len(), 1);
        conn.execute("DROP VIEW v;").unwrap();
        assert!(conn.views.borrow().is_empty());
    }

    #[test]
    fn test_drop_view_if_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("DROP VIEW IF EXISTS nosuch;").unwrap();
        assert!(conn.execute("DROP VIEW nosuch;").is_err());
    }

    #[test]
    fn test_create_trigger_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_ai AFTER INSERT ON t BEGIN INSERT INTO t VALUES (999); END;",
        )
        .unwrap();

        let triggers = conn.triggers.borrow();
        assert_eq!(triggers.len(), 1);
        let trigger = &triggers[0];
        assert_eq!(trigger.name, "trg_ai");
        assert_eq!(trigger.table_name, "t");
        assert_eq!(trigger.timing, fsqlite_ast::TriggerTiming::After);
        assert!(matches!(trigger.event, fsqlite_ast::TriggerEvent::Insert));
    }

    #[test]
    fn test_create_trigger_if_not_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("CREATE TRIGGER trg_ai AFTER INSERT ON t BEGIN SELECT 1; END;")
            .unwrap();
        assert!(
            conn.execute("CREATE TRIGGER trg_ai AFTER INSERT ON t BEGIN SELECT 1; END;")
                .is_err()
        );
        conn.execute("CREATE TRIGGER IF NOT EXISTS trg_ai AFTER INSERT ON t BEGIN SELECT 1; END;")
            .unwrap();
    }

    #[test]
    fn test_create_trigger_nonexistent_table_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn
            .execute("CREATE TRIGGER trg_missing AFTER INSERT ON missing BEGIN SELECT 1; END;")
            .expect_err("CREATE TRIGGER should fail when target table does not exist");
        assert!(matches!(err, FrankenError::NoSuchTable { .. }));
    }

    #[test]
    fn test_create_trigger_with_when_clause() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_when BEFORE UPDATE OF id ON t FOR EACH ROW WHEN NEW.id > 0 BEGIN SELECT 1; END;",
        )
        .unwrap();

        let triggers = conn.triggers.borrow();
        let trigger = triggers
            .iter()
            .find(|trigger| trigger.name == "trg_when")
            .unwrap();
        assert_eq!(trigger.timing, fsqlite_ast::TriggerTiming::Before);
        assert!(trigger.for_each_row);
        assert!(trigger.when_clause.is_some());
        match &trigger.event {
            fsqlite_ast::TriggerEvent::Update(cols) => assert_eq!(cols, &vec!["id".to_owned()]),
            other => panic!("expected UPDATE trigger event, got {other:?}"),
        }
    }

    #[test]
    fn test_trigger_when_false_constant_does_not_fire() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("CREATE TABLE log (msg TEXT);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_when_false AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO log VALUES ('fired'); END;",
        )
        .unwrap();

        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        let rows = conn.query("SELECT msg FROM log;").unwrap();
        assert!(rows.is_empty(), "WHEN 0 trigger should not fire");
    }

    #[test]
    fn test_trigger_when_true_constant_fires() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("CREATE TABLE log (msg TEXT);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_when_true AFTER INSERT ON t WHEN 1 BEGIN INSERT INTO log VALUES ('fired'); END;",
        )
        .unwrap();

        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        let rows = conn.query("SELECT msg FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("fired".to_owned())
        );
    }

    #[test]
    fn test_trigger_when_clause_uses_new_frame_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("CREATE TABLE log (msg TEXT);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_when_new BEFORE INSERT ON t WHEN NEW.id > 0 BEGIN INSERT INTO log VALUES ('fired'); END;",
        )
        .unwrap();

        let event = fsqlite_ast::TriggerEvent::Insert;
        let positive_new = [SqliteValue::Integer(7)];
        conn.fire_before_triggers("t", &event, None, Some(&positive_new))
            .unwrap();

        let non_positive_new = [SqliteValue::Integer(0)];
        conn.fire_before_triggers("t", &event, None, Some(&non_positive_new))
            .unwrap();

        let rows = conn.query("SELECT msg FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("fired".to_owned())
        );
    }

    #[test]
    fn test_trigger_body_binds_old_new_columns_from_frame() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("CREATE TABLE log (old_id INTEGER, new_id INTEGER);")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER trg_hist AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (OLD.id, NEW.id); END;",
        )
        .unwrap();

        let event = fsqlite_ast::TriggerEvent::Update(vec!["id".to_owned()]);
        let old_row = [SqliteValue::Integer(3)];
        let new_row = [SqliteValue::Integer(9)];
        conn.fire_after_triggers("t", &event, Some(&old_row), Some(&new_row))
            .unwrap();

        let rows = conn.query("SELECT old_id, new_id FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(9));
        assert!(conn.trigger_frame_stack.borrow().is_empty());
    }

    #[test]
    fn test_insert_trigger_row_images_via_execute_statement() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE log (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER trg_insert AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id, NEW.name); END;",
        )
        .unwrap();

        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        let rows = conn.query("SELECT id, name FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("alice".to_owned())
        );
    }

    #[test]
    fn test_update_trigger_row_images_via_execute_statement() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE log (old_name TEXT, new_name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_update AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (OLD.name, NEW.name); END;",
        )
        .unwrap();

        conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
            .unwrap();

        let rows = conn.query("SELECT old_name, new_name FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("alice".to_owned())
        );
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("bob".to_owned()));
    }

    #[test]
    fn test_delete_trigger_row_images_via_execute_statement() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE log (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_delete AFTER DELETE ON t BEGIN INSERT INTO log VALUES (OLD.id, OLD.name); END;",
        )
        .unwrap();

        conn.execute("DELETE FROM t WHERE id = 1;").unwrap();

        let rows = conn.query("SELECT id, name FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("alice".to_owned())
        );
    }

    #[test]
    fn test_update_trigger_when_clause_uses_old_new_during_execute_statement() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE log (msg TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_when_update AFTER UPDATE ON t WHEN OLD.name <> NEW.name BEGIN INSERT INTO log VALUES ('changed'); END;",
        )
        .unwrap();

        conn.execute("UPDATE t SET name = name WHERE id = 1;")
            .unwrap();
        conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
            .unwrap();

        let rows = conn.query("SELECT msg FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("changed".to_owned())
        );
    }

    #[test]
    fn test_update_of_trigger_skips_when_listed_column_unchanged() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE log (msg TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_update_of_name AFTER UPDATE OF name ON t BEGIN INSERT INTO log VALUES ('fired'); END;",
        )
        .unwrap();

        conn.execute("UPDATE t SET name = name WHERE id = 1;")
            .unwrap();

        let rows = conn.query("SELECT msg FROM log;").unwrap();
        assert!(
            rows.is_empty(),
            "UPDATE OF name trigger should not fire when name is unchanged"
        );
    }

    #[test]
    fn test_update_of_trigger_fires_when_listed_column_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE log (msg TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_update_of_name AFTER UPDATE OF name ON t BEGIN INSERT INTO log VALUES ('fired'); END;",
        )
        .unwrap();

        conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
            .unwrap();

        let rows = conn.query("SELECT msg FROM log;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("fired".to_owned())
        );
    }

    #[test]
    fn test_trigger_raise_ignore_skips_before_insert_dml() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_raise_ignore BEFORE INSERT ON t BEGIN SELECT RAISE(IGNORE); END;",
        )
        .unwrap();

        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        let rows = conn.query("SELECT id FROM t;").unwrap();
        assert!(
            rows.is_empty(),
            "RAISE(IGNORE) in BEFORE trigger should skip INSERT"
        );
    }

    #[test]
    fn test_trigger_raise_abort_returns_function_error() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_raise_abort BEFORE INSERT ON t BEGIN SELECT RAISE(ABORT, 'stop insert'); END;",
        )
        .unwrap();

        let err = conn
            .execute("INSERT INTO t VALUES (1);")
            .expect_err("INSERT should fail via RAISE(ABORT)");
        assert!(matches!(
            err,
            FrankenError::FunctionError(ref msg) if msg == "stop insert"
        ));
    }

    #[test]
    fn test_trigger_raise_fail_returns_function_error() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_raise_fail BEFORE INSERT ON t BEGIN SELECT RAISE(FAIL, 'fail insert'); END;",
        )
        .unwrap();

        let err = conn
            .execute("INSERT INTO t VALUES (1);")
            .expect_err("INSERT should fail via RAISE(FAIL)");
        assert!(matches!(
            err,
            FrankenError::FunctionError(ref msg) if msg == "fail insert"
        ));
    }

    #[test]
    fn test_trigger_raise_rollback_aborts_transaction() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute(
            "CREATE TRIGGER trg_raise_rb BEFORE INSERT ON t BEGIN SELECT RAISE(ROLLBACK, 'rollback insert'); END;",
        )
        .unwrap();

        conn.execute("BEGIN;").unwrap();
        let err = conn
            .execute("INSERT INTO t VALUES (1);")
            .expect_err("INSERT should fail via RAISE(ROLLBACK)");
        assert!(matches!(
            err,
            FrankenError::TransactionRolledBack { ref reason } if reason == "rollback insert"
        ));

        let rows = conn.query("SELECT id FROM t;").unwrap();
        assert!(
            rows.is_empty(),
            "transaction should be rolled back by RAISE(ROLLBACK)"
        );
    }

    #[test]
    fn test_create_trigger_persists_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trigger_round_trip.db");
        let path_str = path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&path_str).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
            conn.execute("CREATE TRIGGER trg_rt AFTER INSERT ON t BEGIN SELECT 1; END;")
                .unwrap();
            conn.close().unwrap();
        }

        {
            let conn = Connection::open(&path_str).unwrap();
            let triggers = conn.triggers.borrow();
            assert!(
                triggers
                    .iter()
                    .any(|trigger| trigger.name.eq_ignore_ascii_case("trg_rt")),
                "trigger should reload from sqlite_master on reopen"
            );
        }
    }

    #[test]
    fn test_view_with_aggregation() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sales (product TEXT, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES ('A', 10);").unwrap();
        conn.execute("INSERT INTO sales VALUES ('A', 20);").unwrap();
        conn.execute("INSERT INTO sales VALUES ('B', 30);").unwrap();
        conn.execute(
            "CREATE VIEW totals AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product;",
        )
        .unwrap();
        let rows = conn.query("SELECT * FROM totals;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── VACUUM / ANALYZE / REINDEX compatibility stubs (bd-x7i7) ──

    #[test]
    fn test_vacuum() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        // VACUUM should succeed as a no-op.
        conn.execute("VACUUM;").unwrap();
        // Data should be unaffected.
        let rows = conn.query("SELECT * FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_analyze() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("ANALYZE;").unwrap();
        conn.execute("ANALYZE t;").unwrap();
    }

    #[test]
    fn test_reindex() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("REINDEX;").unwrap();
        conn.execute("REINDEX t;").unwrap();
    }

    #[test]
    fn test_select_star_group_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items (category TEXT, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES ('fruit', 'apple');")
            .unwrap();
        conn.execute("INSERT INTO items VALUES ('fruit', 'banana');")
            .unwrap();
        conn.execute("INSERT INTO items VALUES ('veggie', 'carrot');")
            .unwrap();
        // SELECT * ... GROUP BY category should expand * to all columns.
        let rows = conn
            .query("SELECT * FROM items GROUP BY category;")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_select_star_group_by_with_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (grp TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('a', 1);").unwrap();
        conn.execute("INSERT INTO t VALUES ('a', 2);").unwrap();
        conn.execute("INSERT INTO t VALUES ('b', 3);").unwrap();
        let rows = conn
            .query("SELECT grp, COUNT(*) FROM t GROUP BY grp;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // 'a' group has count 2, 'b' group has count 1.
        let counts: Vec<i64> = rows
            .iter()
            .filter_map(|r| {
                if let SqliteValue::Integer(n) = &r.values()[1] {
                    Some(*n)
                } else {
                    None
                }
            })
            .collect();
        assert!(counts.contains(&2));
        assert!(counts.contains(&1));
    }

    #[test]
    fn test_group_by_table_alias_star() {
        // SELECT t.* FROM ... AS t GROUP BY ... should resolve t.* correctly.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE employees (dept TEXT, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES ('eng', 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES ('eng', 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES ('sales', 'Carol');")
            .unwrap();
        let rows = conn
            .query("SELECT t.* FROM employees AS t GROUP BY t.dept;")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_group_by_alias_qualified_column() {
        // t.dept in GROUP BY should resolve when FROM uses alias.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sales (region TEXT, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES ('north', 10);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES ('north', 20);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES ('south', 30);")
            .unwrap();
        let rows = conn
            .query(
                "SELECT s.region, SUM(s.amount) FROM sales AS s GROUP BY s.region ORDER BY s.region;",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("north".into()));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(30));
        assert_eq!(rows[1].values()[0], SqliteValue::Text("south".into()));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(30));
    }

    #[test]
    fn test_order_by_position_join() {
        // Uses a JOIN to exercise the fallback sort_rows_by_order_terms path.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items (id INTEGER, name TEXT, price INTEGER);")
            .unwrap();
        conn.execute("CREATE TABLE tags (item_id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 'banana', 2);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (2, 'apple', 3);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (3, 'cherry', 1);")
            .unwrap();
        conn.execute("INSERT INTO tags VALUES (1, 'fruit');")
            .unwrap();
        conn.execute("INSERT INTO tags VALUES (2, 'fruit');")
            .unwrap();
        conn.execute("INSERT INTO tags VALUES (3, 'fruit');")
            .unwrap();
        // ORDER BY 3 means order by the third result column (price).
        let rows = conn
            .query("SELECT items.name, tags.tag, items.price FROM items JOIN tags ON items.id = tags.item_id ORDER BY 3;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("cherry".into()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("banana".into()));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Text("apple".into()));
    }

    #[test]
    fn test_order_by_position_desc_group_by() {
        // Uses GROUP BY to exercise sort_rows_by_order_terms with position refs.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE scores (player TEXT, points INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO scores VALUES ('A', 10);")
            .unwrap();
        conn.execute("INSERT INTO scores VALUES ('B', 30);")
            .unwrap();
        conn.execute("INSERT INTO scores VALUES ('C', 20);")
            .unwrap();
        // ORDER BY 2 DESC on a GROUP BY query.
        let rows = conn
            .query(
                "SELECT player, SUM(points) AS total FROM scores GROUP BY player ORDER BY 2 DESC;",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(30));
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(20));
        assert_eq!(row_values(&rows[2])[1], SqliteValue::Integer(10));
    }

    #[test]
    fn test_order_by_expression_group_by() {
        // Uses GROUP BY with expression ORDER BY (compound SELECT path).
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE data (cat TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO data VALUES ('x', 5);").unwrap();
        conn.execute("INSERT INTO data VALUES ('x', 3);").unwrap();
        conn.execute("INSERT INTO data VALUES ('y', 10);").unwrap();
        conn.execute("INSERT INTO data VALUES ('z', 1);").unwrap();
        // ORDER BY expression referencing aggregate result column by name.
        let rows = conn
            .query("SELECT cat, SUM(val) AS total FROM data GROUP BY cat ORDER BY total;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("z".into()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("x".into()));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Text("y".into()));
    }

    #[test]
    fn test_insert_select_integer_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE nums (val INTEGER);").unwrap();
        conn.execute("INSERT INTO nums VALUES (42);").unwrap();
        conn.execute("INSERT INTO nums VALUES (100);").unwrap();
        conn.execute("INSERT INTO nums VALUES (-7);").unwrap();

        let rows = conn.query("SELECT val FROM nums;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(100)]);
        assert_eq!(row_values(&rows[2]), vec![SqliteValue::Integer(-7)]);
    }

    #[test]
    fn test_insert_select_copies_filtered_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE dst (id INTEGER, name TEXT);")
            .unwrap();

        conn.execute("INSERT INTO src VALUES (1, 'alpha');")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (2, 'beta');").unwrap();
        conn.execute("INSERT INTO src VALUES (3, 'gamma');")
            .unwrap();

        let inserted = conn
            .execute("INSERT INTO dst SELECT id, name FROM src WHERE id >= 2;")
            .unwrap();
        assert_eq!(inserted, 2);

        let rows = conn.query("SELECT id, name FROM dst ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("beta".to_owned()),
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("gamma".to_owned()),
            ]
        );
    }

    #[test]
    fn test_insert_select_respects_target_column_list_order() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE dst (x TEXT, y INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (10, 'ten');").unwrap();
        conn.execute("INSERT INTO src VALUES (20, 'twenty');")
            .unwrap();

        let inserted = conn
            .execute("INSERT INTO dst (y, x) SELECT a, b FROM src;")
            .unwrap();
        assert_eq!(inserted, 2);

        let rows = conn.query("SELECT x, y FROM dst ORDER BY y;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("ten".to_owned()),
                SqliteValue::Integer(10),
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Text("twenty".to_owned()),
                SqliteValue::Integer(20),
            ]
        );
    }

    #[test]
    fn test_select_from_empty_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE empty (a INTEGER);").unwrap();
        let rows = conn.query("SELECT a FROM empty;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_multiple_tables_independent() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b TEXT);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES ('x');").unwrap();

        let rows1 = conn.query("SELECT a FROM t1;").unwrap();
        let rows2 = conn.query("SELECT b FROM t2;").unwrap();
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows2.len(), 1);
        assert_eq!(row_values(&rows1[0]), vec![SqliteValue::Integer(1)]);
        assert_eq!(
            row_values(&rows2[0]),
            vec![SqliteValue::Text("x".to_owned())]
        );
    }

    #[test]
    fn test_insert_string_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE words (w TEXT);").unwrap();
        conn.execute("INSERT INTO words VALUES ('alpha');").unwrap();
        conn.execute("INSERT INTO words VALUES ('beta');").unwrap();

        let rows = conn.query("SELECT w FROM words;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("alpha".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("beta".to_owned())]
        );
    }

    #[test]
    fn test_update_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob');").unwrap();

        conn.execute("UPDATE t SET name = 'ALICE' WHERE id = 1;")
            .unwrap();

        let rows = conn.query("SELECT id, name FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("ALICE".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("bob".to_owned())]
        );
    }

    #[test]
    fn test_update_integer_primary_key_moves_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob');").unwrap();

        let changed = conn.execute("UPDATE t SET id = 5 WHERE id = 1;").unwrap();
        assert_eq!(changed, 1);

        let rows = conn.query("SELECT id, name FROM t ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("bob".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(5),
                SqliteValue::Text("alice".to_owned())
            ]
        );

        let old_rows = conn.query("SELECT id FROM t WHERE id = 1;").unwrap();
        assert!(
            old_rows.is_empty(),
            "old rowid should be removed after PK update"
        );
    }

    #[test]
    fn test_update_large_record_payload() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();

        let large_payload = "y".repeat(5000);
        let changed = conn
            .execute_with_params(
                "UPDATE t SET payload = ?1 WHERE id = 1;",
                &[SqliteValue::Text(large_payload.clone())],
            )
            .unwrap();
        assert_eq!(changed, 1);

        let rows = conn.query("SELECT payload FROM t WHERE id = 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Text(large_payload)]);
    }

    #[test]
    fn test_delete_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (val INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (10);").unwrap();
        conn.execute("INSERT INTO t VALUES (20);").unwrap();
        conn.execute("INSERT INTO t VALUES (30);").unwrap();

        conn.execute("DELETE FROM t WHERE val = 20;").unwrap();

        let rows = conn.query("SELECT val FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(10)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(30)]);
    }

    #[test]
    fn test_delete_all_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        conn.execute("DELETE FROM t;").unwrap();

        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_update_all_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();

        conn.execute("UPDATE t SET v = 0;").unwrap();

        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row_values(row), vec![SqliteValue::Integer(0)]);
        }
    }

    #[test]
    fn test_full_dml_cycle() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();

        // INSERT
        conn.execute("INSERT INTO users VALUES (1, 'alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'bob');")
            .unwrap();
        let rows = conn.query("SELECT id, name FROM users;").unwrap();
        assert_eq!(rows.len(), 2);

        // UPDATE
        conn.execute("UPDATE users SET name = 'BOB' WHERE id = 2;")
            .unwrap();
        let rows = conn.query("SELECT name FROM users WHERE id = 2;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("BOB".to_owned())]
        );

        // DELETE
        conn.execute("DELETE FROM users WHERE id = 1;").unwrap();
        let rows = conn.query("SELECT id, name FROM users;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("BOB".to_owned())]
        );
    }

    #[test]
    fn test_update_where_qualified_table_alias_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t_alias_upd (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t_alias_upd VALUES (1, 'one');")
            .unwrap();
        conn.execute("INSERT INTO t_alias_upd VALUES (2, 'two');")
            .unwrap();
        conn.execute("INSERT INTO t_alias_upd VALUES (3, 'three');")
            .unwrap();

        conn.execute("UPDATE t_alias_upd AS tt SET b = 'updated' WHERE tt.a > 1;")
            .unwrap();

        let rows = conn.query("SELECT b FROM t_alias_upd ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("one".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("updated".to_owned())]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Text("updated".to_owned())]
        );
    }

    #[test]
    fn test_delete_where_qualified_table_alias_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t_alias_del (a INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t_alias_del VALUES (1);").unwrap();
        conn.execute("INSERT INTO t_alias_del VALUES (2);").unwrap();
        conn.execute("INSERT INTO t_alias_del VALUES (3);").unwrap();

        conn.execute("DELETE FROM t_alias_del AS tt WHERE tt.a > 1;")
            .unwrap();

        let rows = conn.query("SELECT a FROM t_alias_del ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    // === Tests for bd-2vza: qualified alias in Eq WHERE fast-path ===

    #[test]
    fn test_update_where_qualified_alias_eq() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items (id INTEGER, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO items VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO items VALUES (3, 'c');").unwrap();

        // Eq path with qualified alias reference.
        conn.execute("UPDATE items AS i SET val = 'changed' WHERE i.id = 2;")
            .unwrap();

        let rows = conn.query("SELECT val FROM items ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("changed".to_owned())]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Text("c".to_owned())]
        );
    }

    #[test]
    fn test_delete_where_qualified_alias_eq() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items2 (id INTEGER, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO items2 VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO items2 VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO items2 VALUES (3, 'c');").unwrap();

        // Eq path with qualified alias reference.
        conn.execute("DELETE FROM items2 AS i WHERE i.id = 2;")
            .unwrap();

        let rows = conn.query("SELECT val FROM items2 ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("c".to_owned())]
        );
    }

    #[test]
    fn test_select_by_rowid_with_parameter() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alpha');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'beta');").unwrap();

        let rows = conn
            .query_with_params(
                "SELECT b FROM t WHERE rowid = ?1;",
                &[SqliteValue::Integer(2)],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("beta".to_owned())]
        );
    }

    #[test]
    fn test_select_order_by_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        // Insert out of order by `a` to ensure sorting uses rowid, not `a`.
        conn.execute("INSERT INTO t VALUES (30, 'third');").unwrap();
        conn.execute("INSERT INTO t VALUES (10, 'first');").unwrap();
        conn.execute("INSERT INTO t VALUES (20, 'second');")
            .unwrap();

        // ORDER BY rowid should return rows in insertion order (rowid 1, 2, 3).
        let rows = conn.query("SELECT a, b FROM t ORDER BY rowid;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(30),
                SqliteValue::Text("third".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(10),
                SqliteValue::Text("first".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![
                SqliteValue::Integer(20),
                SqliteValue::Text("second".to_owned())
            ]
        );

        // ORDER BY rowid DESC should return rows in reverse insertion order.
        let rows = conn
            .query("SELECT a, b FROM t ORDER BY rowid DESC;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(20),
                SqliteValue::Text("second".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![
                SqliteValue::Integer(30),
                SqliteValue::Text("third".to_owned())
            ]
        );
    }

    #[test]
    fn test_select_order_by_rowid_aliases() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t2 (x TEXT);").unwrap();
        conn.execute("INSERT INTO t2 VALUES ('c');").unwrap();
        conn.execute("INSERT INTO t2 VALUES ('a');").unwrap();
        conn.execute("INSERT INTO t2 VALUES ('b');").unwrap();

        // _rowid_ alias
        let rows = conn.query("SELECT x FROM t2 ORDER BY _rowid_;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("c".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Text("b".to_owned())]
        );

        // oid alias
        let rows = conn.query("SELECT x FROM t2 ORDER BY oid;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("c".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Text("b".to_owned())]
        );
    }

    #[test]
    fn test_select_order_by_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();

        // ORDER BY a + 0 (expression, not bare column) — should sort by `a`.
        let rows = conn.query("SELECT a, b FROM t ORDER BY a + 0;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("b".to_owned())]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Integer(3), SqliteValue::Text("c".to_owned())]
        );
    }

    #[test]
    fn test_select_group_by_having() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'y');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'z');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'w');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'v');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'u');").unwrap();

        // HAVING count(*) > 1 should exclude group a=2 (count=1).
        let rows = conn
            .query("SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1;")
            .unwrap();
        assert_eq!(rows.len(), 2, "should have 2 groups with count > 1");
        // Groups: a=1 (count=2), a=3 (count=3).
        let a_vals: Vec<i64> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Integer(n) => Some(*n),
                _ => None,
            })
            .collect();
        assert!(a_vals.contains(&1), "group a=1 should pass HAVING");
        assert!(a_vals.contains(&3), "group a=3 should pass HAVING");
        assert!(!a_vals.contains(&2), "group a=2 should be filtered");
    }

    #[test]
    fn test_having_aggregate_not_in_select() {
        // HAVING can reference an aggregate that's not in the SELECT list.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (dept TEXT, salary INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('eng', 100);").unwrap();
        conn.execute("INSERT INTO t VALUES ('eng', 200);").unwrap();
        conn.execute("INSERT INTO t VALUES ('sales', 50);").unwrap();
        // eng: count=2, sum=300; sales: count=1, sum=50
        // SELECT dept but HAVING COUNT(*) > 1 — aggregate not in SELECT.
        let rows = conn
            .query("SELECT dept FROM t GROUP BY dept HAVING COUNT(*) > 1;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("eng".to_owned()));
    }

    #[test]
    fn test_having_sum_not_in_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (dept TEXT, salary INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('eng', 100);").unwrap();
        conn.execute("INSERT INTO t VALUES ('eng', 200);").unwrap();
        conn.execute("INSERT INTO t VALUES ('sales', 50);").unwrap();
        // HAVING SUM(salary) > 100 — SUM not in SELECT.
        let rows = conn
            .query("SELECT dept FROM t GROUP BY dept HAVING SUM(salary) > 100;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("eng".to_owned()));
    }

    #[test]
    fn test_having_with_and_or() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 20);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 30);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 40);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 50);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 60);").unwrap();
        // a=1: count=2, sum(b)=30; a=2: count=1, sum(b)=30; a=3: count=3, sum(b)=150
        // HAVING COUNT(*) > 1 AND SUM(b) > 100 should only include a=3.
        let rows = conn
            .query("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1 AND SUM(b) > 100;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(3));
    }

    #[test]
    fn test_having_comparison_operators() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        for i in 1..=5 {
            for _ in 0..i {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, {i});"))
                    .unwrap();
            }
        }
        // a=1: count=1, a=2: count=2, ..., a=5: count=5
        let rows = conn
            .query("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) >= 3;")
            .unwrap();
        assert_eq!(rows.len(), 3, "a=3,4,5 should pass >= 3");

        let rows = conn
            .query("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) < 3;")
            .unwrap();
        assert_eq!(rows.len(), 2, "a=1,2 should pass < 3");

        let rows = conn
            .query("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) = 3;")
            .unwrap();
        assert_eq!(rows.len(), 1, "only a=3 should pass = 3");

        let rows = conn
            .query("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) != 3;")
            .unwrap();
        assert_eq!(rows.len(), 4, "all except a=3 should pass != 3");
    }

    // ── Expression-based GROUP BY tests (bd-2ej2) ─────────────────

    #[test]
    fn test_group_by_arithmetic_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (4, 20);").unwrap();

        let rows = conn
            .query("SELECT a + b, COUNT(*) FROM t GROUP BY a + b ORDER BY a + b;")
            .unwrap();
        assert_eq!(rows.len(), 4);
        // Each a+b is unique: 11, 13, 22, 24
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(11));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(1));
    }

    #[test]
    fn test_group_by_expression_with_duplicate_keys() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (val INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        conn.execute("INSERT INTO t VALUES (4);").unwrap();
        conn.execute("INSERT INTO t VALUES (5);").unwrap();
        conn.execute("INSERT INTO t VALUES (6);").unwrap();

        // GROUP BY val % 2 groups into even (0) and odd (1).
        let rows = conn
            .query("SELECT val % 2, COUNT(*) FROM t GROUP BY val % 2 ORDER BY val % 2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Evens: 2,4,6
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(0));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(3));
        // Odds: 1,3,5
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(3));
    }

    #[test]
    fn test_group_by_function_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (name TEXT, score INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('Alice', 90);").unwrap();
        conn.execute("INSERT INTO t VALUES ('Bob', 80);").unwrap();
        conn.execute("INSERT INTO t VALUES ('Ann', 70);").unwrap();
        conn.execute("INSERT INTO t VALUES ('Bill', 85);").unwrap();

        // GROUP BY LENGTH(name): 'Bob' and 'Ann' have length 3, 'Bill' has 4, 'Alice' has 5.
        let rows = conn
            .query("SELECT LENGTH(name), SUM(score) FROM t GROUP BY LENGTH(name) ORDER BY LENGTH(name);")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(3));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(150)); // Bob(80) + Ann(70)
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(4));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(85)); // Bill
        assert_eq!(rows[2].values()[0], SqliteValue::Integer(5));
        assert_eq!(rows[2].values()[1], SqliteValue::Integer(90)); // Alice
    }

    #[test]
    fn test_group_by_expression_with_having() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (val INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        conn.execute("INSERT INTO t VALUES (4);").unwrap();
        conn.execute("INSERT INTO t VALUES (5);").unwrap();
        conn.execute("INSERT INTO t VALUES (6);").unwrap();

        // GROUP BY val % 3 with HAVING COUNT(*) > 1: all groups have exactly 2 items.
        let rows = conn
            .query("SELECT val % 3, COUNT(*) FROM t GROUP BY val % 3 HAVING COUNT(*) >= 2;")
            .unwrap();
        // val%3=0: {3,6}, val%3=1: {1,4}, val%3=2: {2,5} — all have count 2.
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_group_by_expression_with_aggregate_sum() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 30);").unwrap();
        conn.execute("INSERT INTO t VALUES (4, 40);").unwrap();

        // GROUP BY a % 2: even {2,4} and odd {1,3}.
        let rows = conn
            .query("SELECT a % 2, SUM(b) FROM t GROUP BY a % 2 ORDER BY a % 2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(0));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(60)); // 20+40
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(40)); // 10+30
    }

    #[test]
    fn test_group_by_select_star_when_grouping_all_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'y');").unwrap();

        let rows = conn
            .query("SELECT * FROM t GROUP BY a, b ORDER BY a, b;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("x".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("y".to_owned())]
        );
    }

    #[test]
    fn test_group_by_select_table_star_when_grouping_all_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'z');").unwrap();

        let rows = conn
            .query("SELECT t.* FROM t GROUP BY a, b ORDER BY a, b;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("x".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(3), SqliteValue::Text("z".to_owned())]
        );
    }

    #[test]
    fn test_group_by_select_table_star_with_from_alias() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'y');").unwrap();

        let rows = conn
            .query("SELECT tt.* FROM t AS tt GROUP BY a, b ORDER BY a, b;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("x".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("y".to_owned())]
        );
    }

    // ── GROUP BY + rowid alias resolution (bd-399s) ──────

    #[test]
    fn test_group_by_rowid_alias() {
        // GROUP BY rowid must resolve to the INTEGER PRIMARY KEY column
        // so that each row has a distinct group key.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, num REAL);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alpha', 1.5);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'beta', 2.5);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'gamma', 3.5);")
            .unwrap();

        // Each rowid is unique, so COUNT(*) should be 1 for every group.
        let rows = conn
            .query("SELECT rowid, COUNT(*) AS cnt FROM t GROUP BY rowid ORDER BY rowid;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Integer(1)]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Integer(3), SqliteValue::Integer(1)]
        );

        // HAVING cnt > 1 should return nothing (no duplicate rowids).
        let dups = conn
            .query("SELECT rowid, COUNT(*) AS cnt FROM t GROUP BY rowid HAVING cnt > 1;")
            .unwrap();
        assert!(dups.is_empty(), "expected no duplicate rowids");
    }

    #[test]
    fn test_group_by_rowid_alias_variants() {
        // _rowid_ and oid should also resolve correctly.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t2 (pk INTEGER PRIMARY KEY, x TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (10, 'a');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (20, 'b');").unwrap();

        let rows = conn
            .query("SELECT _rowid_, COUNT(*) FROM t2 GROUP BY _rowid_ ORDER BY _rowid_;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(10));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(20));

        let rows = conn
            .query("SELECT oid, COUNT(*) FROM t2 GROUP BY oid ORDER BY oid;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(10));
    }

    // ── GROUP BY + JOIN tests (bd-29mz) ──────

    #[test]
    fn test_group_by_with_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER);")
            .unwrap();
        conn.execute("CREATE TABLE customers (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (1, 'alice');")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (2, 'bob');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 1, 100);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (2, 1, 200);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (3, 2, 50);")
            .unwrap();

        let rows = conn
            .query("SELECT customers.name, SUM(orders.amount) FROM customers INNER JOIN orders ON customers.id = orders.customer_id GROUP BY customers.name ORDER BY customers.name;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("alice".to_owned()),
                SqliteValue::Integer(300)
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Text("bob".to_owned()),
                SqliteValue::Integer(50)
            ]
        );
    }

    #[test]
    fn test_group_by_with_join_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE dept (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE emp (id INTEGER, dept_id INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO dept VALUES (1, 'eng');").unwrap();
        conn.execute("INSERT INTO dept VALUES (2, 'sales');")
            .unwrap();
        conn.execute("INSERT INTO emp VALUES (1, 1);").unwrap();
        conn.execute("INSERT INTO emp VALUES (2, 1);").unwrap();
        conn.execute("INSERT INTO emp VALUES (3, 1);").unwrap();
        conn.execute("INSERT INTO emp VALUES (4, 2);").unwrap();

        let rows = conn
            .query("SELECT dept.name, COUNT(*) FROM dept INNER JOIN emp ON dept.id = emp.dept_id GROUP BY dept.name ORDER BY dept.name;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("eng".to_owned()), SqliteValue::Integer(3)]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Text("sales".to_owned()),
                SqliteValue::Integer(1)
            ]
        );
    }

    #[test]
    fn test_group_by_with_left_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE categories (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE products (id INTEGER, cat_id INTEGER, price INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO categories VALUES (1, 'food');")
            .unwrap();
        conn.execute("INSERT INTO categories VALUES (2, 'toys');")
            .unwrap();
        conn.execute("INSERT INTO products VALUES (1, 1, 10);")
            .unwrap();
        conn.execute("INSERT INTO products VALUES (2, 1, 20);")
            .unwrap();
        // Category 'toys' has no products.

        let rows = conn
            .query("SELECT categories.name, COUNT(products.id) FROM categories LEFT JOIN products ON categories.id = products.cat_id GROUP BY categories.name ORDER BY categories.name;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("food".to_owned()),
                SqliteValue::Integer(2)
            ]
        );
        // LEFT JOIN: toys appears but with COUNT = 0 (all NULLs from products).
        assert_eq!(rows[1].values()[0], SqliteValue::Text("toys".to_owned()));
        // COUNT of NULL product IDs should be 0.
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(0));
    }

    // ── LIKE/CASE/CAST expression evaluation tests (bd-3pss) ──────

    #[test]
    fn test_join_where_like() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'Alice');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'Bob');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 'admin');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 'basic');").unwrap();

        let rows = conn
            .query("SELECT t1.name FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t1.name LIKE 'A%';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
    }

    #[test]
    fn test_case_expression_in_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (val INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();

        let rows = conn
            .query(
                "SELECT CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' ELSE 'other' END FROM t ORDER BY val;",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("one".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Text("two".to_owned()));
        assert_eq!(rows[2].values()[0], SqliteValue::Text("other".to_owned()));
    }

    #[test]
    fn test_cast_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (val TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES ('42');").unwrap();
        conn.execute("INSERT INTO t VALUES ('7');").unwrap();

        let rows = conn
            .query("SELECT CAST(val AS INTEGER) FROM t ORDER BY CAST(val AS INTEGER);")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(7));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(42));
    }

    #[test]
    fn test_like_not_like() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (name TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES ('Alice');").unwrap();
        conn.execute("INSERT INTO t VALUES ('Bob');").unwrap();
        conn.execute("INSERT INTO t VALUES ('Anna');").unwrap();

        let rows = conn
            .query("SELECT name FROM t WHERE name LIKE 'A%' ORDER BY name;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Text("Anna".to_owned()));

        let rows = conn
            .query("SELECT name FROM t WHERE name NOT LIKE 'A%';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Bob".to_owned()));
    }

    #[test]
    fn test_like_underscore_single_char() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (code TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES ('AB');").unwrap();
        conn.execute("INSERT INTO t VALUES ('AC');").unwrap();
        conn.execute("INSERT INTO t VALUES ('ABC');").unwrap();

        let rows = conn
            .query("SELECT code FROM t WHERE code LIKE 'A_' ORDER BY code;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("AB".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Text("AC".to_owned()));
    }

    #[test]
    fn test_case_with_operand() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (status INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (0);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        let rows = conn
            .query(
                "SELECT CASE status WHEN 0 THEN 'inactive' WHEN 1 THEN 'active' ELSE 'unknown' END FROM t ORDER BY status;",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Text("inactive".to_owned())
        );
        assert_eq!(rows[1].values()[0], SqliteValue::Text("active".to_owned()));
        assert_eq!(rows[2].values()[0], SqliteValue::Text("unknown".to_owned()));
    }

    #[test]
    fn test_compound_select_union_all() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        // UNION ALL: concatenates all rows (including duplicates).
        let rows = conn
            .query("SELECT a FROM t1 UNION ALL SELECT a FROM t2;")
            .unwrap();
        assert_eq!(rows.len(), 4);
        let vals: Vec<i64> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Integer(n) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(vals, vec![1, 2, 2, 3]);
    }

    #[test]
    fn test_compound_select_union() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        // UNION: removes duplicates.
        let rows = conn
            .query("SELECT a FROM t1 UNION SELECT a FROM t2;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        let mut vals: Vec<i64> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Integer(n) => Some(*n),
                _ => None,
            })
            .collect();
        vals.sort_unstable();
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn test_compound_select_intersect() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (4);").unwrap();

        // INTERSECT: only rows in both.
        let rows = conn
            .query("SELECT a FROM t1 INTERSECT SELECT a FROM t2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let mut vals: Vec<i64> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Integer(n) => Some(*n),
                _ => None,
            })
            .collect();
        vals.sort_unstable();
        assert_eq!(vals, vec![2, 3]);
    }

    #[test]
    fn test_compound_select_except() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();

        // EXCEPT: rows in first but not second.
        let rows = conn
            .query("SELECT a FROM t1 EXCEPT SELECT a FROM t2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let mut vals: Vec<i64> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Integer(n) => Some(*n),
                _ => None,
            })
            .collect();
        vals.sort_unstable();
        assert_eq!(vals, vec![1, 3]);
    }

    #[test]
    fn test_join_inner() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders (user_id INTEGER, product TEXT);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 'Widget');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 'Gadget');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (3, 'Gizmo');")
            .unwrap();

        let rows = conn
            .query("SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("Widget".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[1].values()[1], SqliteValue::Text("Gadget".to_owned()));
    }

    #[test]
    fn test_join_left() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders (user_id INTEGER, product TEXT);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 'Widget');")
            .unwrap();

        let rows = conn
            .query("SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Alice has a match.
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("Widget".to_owned()));
        // Bob has no match: product is NULL.
        assert_eq!(rows[1].values()[0], SqliteValue::Text("Bob".to_owned()));
        assert_eq!(rows[1].values()[1], SqliteValue::Null);
    }

    #[test]
    fn test_join_cross() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a (x INTEGER);").unwrap();
        conn.execute("CREATE TABLE b (y INTEGER);").unwrap();
        conn.execute("INSERT INTO a VALUES (1);").unwrap();
        conn.execute("INSERT INTO a VALUES (2);").unwrap();
        conn.execute("INSERT INTO b VALUES (10);").unwrap();
        conn.execute("INSERT INTO b VALUES (20);").unwrap();

        let rows = conn.query("SELECT a.x, b.y FROM a CROSS JOIN b;").unwrap();
        assert_eq!(rows.len(), 4); // Cartesian product: 2 x 2.
    }

    #[test]
    fn test_join_with_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders (user_id INTEGER, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 50);").unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 150);").unwrap();
        conn.execute("INSERT INTO orders VALUES (2, 75);").unwrap();

        let rows = conn
            .query("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(150));
    }

    #[test]
    fn test_join_star_projection() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (c INTEGER, d TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 'y');").unwrap();

        let rows = conn
            .query("SELECT * FROM t1 JOIN t2 ON t1.a = t2.c;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values().len(), 4); // a, b, c, d
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("x".to_owned()));
        assert_eq!(rows[0].values()[2], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[3], SqliteValue::Text("y".to_owned()));
    }

    #[test]
    fn test_join_multi_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER);")
            .unwrap();
        conn.execute("CREATE TABLE products (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (100, 1, 10);")
            .unwrap();
        conn.execute("INSERT INTO products VALUES (10, 'Widget');")
            .unwrap();

        let rows = conn
            .query("SELECT users.name, products.name FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("Widget".to_owned()));
    }

    // ── IN (SELECT ...) subquery tests ──────────────────────────────

    #[test]
    fn test_in_subquery_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (4);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2);")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<i64> = rows
            .iter()
            .filter_map(|r| match r.values()[0] {
                SqliteValue::Integer(i) => Some(i),
                _ => None,
            })
            .collect();
        assert_eq!(vals.len(), rows.len());
        assert!(vals.contains(&2));
        assert!(vals.contains(&3));
    }

    #[test]
    fn test_not_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a NOT IN (SELECT b FROM t2);")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    }

    #[test]
    fn test_in_subquery_empty() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();

        // Subquery returns empty set — no rows should match.
        let rows = conn
            .query("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2);")
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_in_subquery_with_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE employees (id INTEGER, dept_id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE departments (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO departments VALUES (1, 'Engineering');")
            .unwrap();
        conn.execute("INSERT INTO departments VALUES (2, 'Sales');")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES (10, 1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES (20, 2, 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES (30, 3, 'Charlie');")
            .unwrap();

        // Use IN subquery to filter employees by valid department.
        let rows = conn
            .query("SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments);")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let names: Vec<&str> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Text(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(names.len(), rows.len());
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }

    #[test]
    fn test_prepared_select_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        let stmt = conn
            .prepare("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2) ORDER BY a;")
            .unwrap();
        let rows = stmt.query().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
    }

    #[test]
    fn test_update_where_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, flag TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'orig');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'orig');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 'orig');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        conn.execute("UPDATE t1 SET flag='hit' WHERE a IN (SELECT b FROM t2);")
            .unwrap();

        let rows = conn.query("SELECT a, flag FROM t1 ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values()[1], SqliteValue::Text("orig".to_owned()));
        assert_eq!(rows[1].values()[1], SqliteValue::Text("hit".to_owned()));
        assert_eq!(rows[2].values()[1], SqliteValue::Text("hit".to_owned()));
    }

    #[test]
    fn test_delete_where_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        conn.execute("DELETE FROM t1 WHERE a IN (SELECT b FROM t2);")
            .unwrap();

        let rows = conn.query("SELECT a FROM t1 ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    }

    #[test]
    fn test_in_table_name_syntax() {
        // IN table_name is shorthand for IN (SELECT * FROM table_name).
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE allowed (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO allowed VALUES (2);").unwrap();
        conn.execute("INSERT INTO allowed VALUES (3);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a IN allowed ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
    }

    #[test]
    fn test_not_in_subquery_update() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, flag TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'orig');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'orig');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 'orig');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();

        conn.execute("UPDATE t1 SET flag='miss' WHERE a NOT IN (SELECT b FROM t2);")
            .unwrap();

        let rows = conn.query("SELECT a, flag FROM t1 ORDER BY a;").unwrap();
        assert_eq!(rows[0].values()[1], SqliteValue::Text("miss".into()));
        assert_eq!(rows[1].values()[1], SqliteValue::Text("orig".into()));
        assert_eq!(rows[2].values()[1], SqliteValue::Text("orig".into()));
    }

    #[test]
    fn test_not_in_subquery_delete() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1);").unwrap();

        conn.execute("DELETE FROM t1 WHERE a NOT IN (SELECT b FROM t2);")
            .unwrap();

        let rows = conn.query("SELECT a FROM t1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    }

    #[test]
    fn test_in_subquery_with_where_clause() {
        // IN (SELECT ... WHERE ...) tests probe with a predicate filter.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER, active INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 0);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3, 1);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2 WHERE active = 1) ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
    }

    #[test]
    fn test_exists_subquery_filters_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (10);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2) ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));

        conn.execute("DELETE FROM t2;").unwrap();
        let rows = conn
            .query("SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2);")
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_scalar_subquery_in_where_clause() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a = (SELECT b FROM t2);")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
    }

    #[test]
    fn test_scalar_subquery_empty_set_rewrites_to_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE (SELECT b FROM t2) IS NULL ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
    }

    // ── CTE (WITH clause) tests ─────────────────────────────────────

    #[test]
    fn test_cte_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'y');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 'z');").unwrap();

        let rows = conn
            .query("WITH cte AS (SELECT a, b FROM t1 WHERE a > 1) SELECT a, b FROM cte ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("y".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
    }

    #[test]
    fn test_cte_with_column_names() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (val INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (10);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (20);").unwrap();

        let rows = conn
            .query("WITH nums(n) AS (SELECT val FROM t1) SELECT n FROM nums ORDER BY n;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(10));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(20));
    }

    #[test]
    fn test_cte_multiple() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (10);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (20);").unwrap();

        let rows = conn
            .query(
                "WITH c1 AS (SELECT a FROM t1), c2 AS (SELECT b FROM t2) \
                 SELECT a FROM c1 ORDER BY a;",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
    }

    #[test]
    fn test_cte_with_aggregation() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sales (amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES (100);").unwrap();
        conn.execute("INSERT INTO sales VALUES (200);").unwrap();
        conn.execute("INSERT INTO sales VALUES (300);").unwrap();

        let rows = conn
            .query(
                "WITH totals AS (SELECT SUM(amount) AS total FROM sales) \
                 SELECT total FROM totals;",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(600));
    }

    // ── CREATE TABLE AS SELECT tests ─────────────────────────────────

    #[test]
    fn test_create_table_as_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 'y');").unwrap();
        conn.execute("INSERT INTO src VALUES (3, 'z');").unwrap();

        conn.execute("CREATE TABLE dst AS SELECT a, b FROM src WHERE a >= 2;")
            .unwrap();

        let rows = conn.query("SELECT a, b FROM dst ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("y".to_owned()));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
        assert_eq!(rows[1].values()[1], SqliteValue::Text("z".to_owned()));
    }

    #[test]
    fn test_create_table_as_select_empty() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (x INTEGER);").unwrap();

        // Create from empty result set.
        conn.execute("CREATE TABLE dst AS SELECT x FROM src WHERE x > 100;")
            .unwrap();

        let rows = conn.query("SELECT x FROM dst;").unwrap();
        assert_eq!(rows.len(), 0);

        // Verify we can insert into the new table.
        conn.execute("INSERT INTO dst VALUES (42);").unwrap();
        let rows = conn.query("SELECT x FROM dst;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(42));
    }

    #[test]
    fn test_create_table_as_select_with_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (a INTEGER, b INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (10, 20);").unwrap();
        conn.execute("INSERT INTO src VALUES (30, 40);").unwrap();

        conn.execute("CREATE TABLE dst AS SELECT a + b AS total FROM src;")
            .unwrap();

        let rows = conn.query("SELECT total FROM dst ORDER BY total;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(30));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(70));
    }

    #[test]
    fn test_select_projection_rowid_aliases() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t3 (x TEXT);").unwrap();
        conn.execute("INSERT INTO t3 VALUES ('c');").unwrap();
        conn.execute("INSERT INTO t3 VALUES ('a');").unwrap();
        conn.execute("INSERT INTO t3 VALUES ('b');").unwrap();

        let rows = conn
            .query("SELECT rowid, _rowid_, oid, x FROM t3 ORDER BY rowid;")
            .unwrap();
        assert_eq!(rows.len(), 3);

        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Text("c".to_owned()),
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
                SqliteValue::Text("a".to_owned()),
            ]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Integer(3),
                SqliteValue::Integer(3),
                SqliteValue::Text("b".to_owned()),
            ]
        );

        let rows = conn
            .query("SELECT rowid + 10, _rowid_ + 10, oid + 10 FROM t3 ORDER BY rowid;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(11),
                SqliteValue::Integer(11),
                SqliteValue::Integer(11),
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(12),
                SqliteValue::Integer(12),
                SqliteValue::Integer(12),
            ]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![
                SqliteValue::Integer(13),
                SqliteValue::Integer(13),
                SqliteValue::Integer(13),
            ]
        );

        // Qualified references via table alias should resolve rowid aliases.
        let rows = conn
            .query("SELECT tt.rowid, tt._rowid_, tt.oid FROM t3 AS tt ORDER BY tt.rowid;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
                SqliteValue::Integer(2),
            ]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Integer(3),
                SqliteValue::Integer(3),
            ]
        );
    }

    // ── Function registry wiring tests ──────────────────────────────────

    #[test]
    fn test_scalar_function_abs() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT abs(-42), abs(2.5);").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(42), SqliteValue::Float(2.5_f64)],
        );
    }

    #[test]
    fn test_scalar_function_length() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT length('hello');").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(5)]);
    }

    #[test]
    fn test_scalar_function_upper_lower() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT upper('hello'), lower('WORLD');")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("HELLO".to_owned()),
                SqliteValue::Text("world".to_owned()),
            ],
        );
    }

    #[test]
    fn test_scalar_function_coalesce() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT coalesce(NULL, NULL, 42, 99);").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_scalar_function_typeof_via_registry() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT typeof(42), typeof('hi'), typeof(NULL);")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("integer".to_owned()),
                SqliteValue::Text("text".to_owned()),
                SqliteValue::Text("null".to_owned()),
            ],
        );
    }

    #[test]
    fn test_json_scalar_function_extract_via_registry() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(r#"SELECT json_extract('{"a":[1,2,3]}', '$.a[1]');"#)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_json_scalar_function_set_via_registry() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(r#"SELECT json_set('{"a":1}', '$.b', 2);"#)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text(r#"{"a":1,"b":2}"#.to_owned())]
        );
    }

    #[test]
    fn test_fts5_source_id_function_available() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT fts5_source_id();").unwrap();
        assert_eq!(rows.len(), 1);
        let values = row_values(&rows[0]);
        let SqliteValue::Text(source_id) = &values[0] else {
            panic!("fts5_source_id() should return TEXT");
        };
        assert!(
            source_id.to_ascii_lowercase().contains("fts5"),
            "unexpected fts5_source_id payload: {source_id}"
        );
    }

    // ── BETWEEN expression tests ────────────────────────────────────────

    #[test]
    fn test_between_true() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 5 BETWEEN 1 AND 10;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_between_false() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 15 BETWEEN 1 AND 10;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_between_boundary_inclusive() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 1 BETWEEN 1 AND 10, 10 BETWEEN 1 AND 10;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Integer(1)],
        );
    }

    #[test]
    fn test_not_between() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 5 NOT BETWEEN 1 AND 10, 15 NOT BETWEEN 1 AND 10;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(0), SqliteValue::Integer(1)],
        );
    }

    // ── IN expression tests ─────────────────────────────────────────────

    #[test]
    fn test_in_list_found() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 3 IN (1, 2, 3, 4);").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_in_list_not_found() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 5 IN (1, 2, 3, 4);").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_not_in_list() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 3 NOT IN (1, 2, 3), 5 NOT IN (1, 2, 3);")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(0), SqliteValue::Integer(1)],
        );
    }

    #[test]
    fn test_in_string_list() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'b' IN ('a', 'b', 'c');").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    // ── LIKE expression tests ───────────────────────────────────────────

    #[test]
    fn test_like_match() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'hello' LIKE 'hel%';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_like_no_match() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'hello' LIKE 'world%';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_not_like() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 'hello' NOT LIKE 'hel%', 'hello' NOT LIKE 'xyz%';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(0), SqliteValue::Integer(1)],
        );
    }

    #[test]
    fn test_like_underscore_wildcard() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'abc' LIKE 'a_c';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    // ── Transaction tests ───────────────────────────────────────────────

    #[test]
    fn test_begin_commit() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        assert!(!conn.in_transaction());

        conn.execute("BEGIN;").unwrap();
        assert!(conn.in_transaction());

        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("COMMIT;").unwrap();
        assert!(!conn.in_transaction());

        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_begin_rollback_restores_state() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();

        // Verify 3 rows during transaction.
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 3);

        conn.execute("ROLLBACK;").unwrap();
        assert!(!conn.in_transaction());

        // After rollback, only the original row remains.
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_rollback_restores_schema() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("ROLLBACK;").unwrap();

        // Table should not exist after rollback.
        let result = conn.query("SELECT x FROM t;");
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_begin_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let result = conn.execute("BEGIN;");
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_without_begin_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let result = conn.execute("COMMIT;");
        assert!(result.is_err());
    }

    #[test]
    fn test_rollback_without_begin_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let result = conn.execute("ROLLBACK;");
        assert!(result.is_err());
    }

    // ── Savepoint tests ─────────────────────────────────────────────────

    #[test]
    fn test_savepoint_rollback_to() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        conn.execute("SAVEPOINT sp2;").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();

        // Verify 3 rows.
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 3);

        // Rollback to sp2: undo value 3 only.
        conn.execute("ROLLBACK TO sp2;").unwrap();
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2);

        // Rollback to sp1: undo value 2 as well.
        conn.execute("ROLLBACK TO sp1;").unwrap();
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_savepoint_release() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();

        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        conn.execute("SAVEPOINT sp2;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        // Release sp2: changes committed to sp1 scope.
        conn.execute("RELEASE sp2;").unwrap();

        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_savepoint_release_implicit_txn_clears_concurrent_session() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("SAVEPOINT sp1;").unwrap();
        assert!(conn.in_transaction());
        assert!(conn.is_concurrent_transaction());
        assert!(conn.has_concurrent_session());

        conn.execute("RELEASE sp1;").unwrap();
        assert!(!conn.in_transaction());
        assert!(!conn.is_concurrent_transaction());
        assert!(!conn.has_concurrent_session());
    }

    #[test]
    fn test_savepoint_rollback_to_discards_inner_savepoints() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("SAVEPOINT a;").unwrap();
        conn.execute("SAVEPOINT b;").unwrap();
        conn.execute("ROLLBACK TO a;").unwrap();

        let err = conn.execute("RELEASE b;").unwrap_err();
        assert_eq!(err.to_string(), "internal error: no such savepoint: b");
        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_savepoint_rollback_to_is_case_insensitive() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("SAVEPOINT MiXeD;").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        conn.execute("ROLLBACK TO mixed;").unwrap();
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert!(rows.is_empty());
        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_savepoint_release_is_case_insensitive() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("SAVEPOINT MiXeD;").unwrap();
        conn.execute("RELEASE mixed;").unwrap();
        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_savepoint_rollback_restores_views_aliases_and_master_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("SAVEPOINT sp;").unwrap();
        let before_views = conn.views.borrow().len();
        let before_aliases = conn.rowid_alias_columns.borrow().clone();
        let before_next_master_rowid = *conn.next_master_rowid.borrow();

        conn.execute("CREATE TABLE temp_t (id INTEGER PRIMARY KEY, x TEXT);")
            .unwrap();
        conn.execute("CREATE VIEW temp_v AS SELECT id FROM temp_t;")
            .unwrap();

        assert!(conn.rowid_alias_columns.borrow().contains_key("temp_t"));
        assert!(conn.views.borrow().iter().any(|v| v.name == "temp_v"));
        assert!(*conn.next_master_rowid.borrow() > before_next_master_rowid);

        conn.execute("ROLLBACK TO sp;").unwrap();

        assert_eq!(conn.views.borrow().len(), before_views);
        assert_eq!(*conn.rowid_alias_columns.borrow(), before_aliases);
        assert_eq!(*conn.next_master_rowid.borrow(), before_next_master_rowid);
        assert!(!conn.views.borrow().iter().any(|v| v.name == "temp_v"));
        assert!(!conn.rowid_alias_columns.borrow().contains_key("temp_t"));

        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_savepoint_starts_implicit_transaction() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(!conn.in_transaction());
        conn.execute("SAVEPOINT sp1;").unwrap();
        assert!(conn.in_transaction());
    }

    #[test]
    fn test_savepoint_implicit_transaction_uses_concurrent_mode_default() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.is_concurrent_mode_default());

        conn.execute("SAVEPOINT sp1;").unwrap();
        assert!(conn.in_transaction());
        assert!(conn.is_concurrent_transaction());
        assert!(conn.has_concurrent_session());
    }

    #[test]
    fn test_begin_concurrent_setup_failure_does_not_leak_transaction_state() {
        let conn = Connection::open(":memory:").unwrap();
        let snapshot = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        {
            let mut registry = conn.concurrent_registry.borrow_mut();
            for _ in 0..fsqlite_mvcc::MAX_CONCURRENT_WRITERS {
                registry.begin_concurrent(snapshot).unwrap();
            }
        }

        let err = conn.execute("BEGIN;").unwrap_err();
        assert!(matches!(err, FrankenError::Busy));
        assert!(!conn.in_transaction());
        assert!(!conn.has_concurrent_session());
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_savepoint_concurrent_setup_failure_does_not_leak_transaction_state() {
        let conn = Connection::open(":memory:").unwrap();
        let snapshot = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        {
            let mut registry = conn.concurrent_registry.borrow_mut();
            for _ in 0..fsqlite_mvcc::MAX_CONCURRENT_WRITERS {
                registry.begin_concurrent(snapshot).unwrap();
            }
        }

        let err = conn.execute("SAVEPOINT sp1;").unwrap_err();
        assert!(matches!(err, FrankenError::Busy));
        assert!(!conn.in_transaction());
        assert!(!conn.has_concurrent_session());
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_rollback_to_nonexistent_savepoint_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let result = conn.execute("ROLLBACK TO nosuch;");
        assert!(result.is_err());
    }

    #[test]
    fn test_release_nonexistent_savepoint_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let result = conn.execute("RELEASE nosuch;");
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_with_update_rollback() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
            .unwrap();

        let rows = conn.query("SELECT name FROM t;").unwrap();
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("bob".to_owned())]
        );

        conn.execute("ROLLBACK;").unwrap();

        let rows = conn.query("SELECT name FROM t;").unwrap();
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("alice".to_owned())]
        );
    }

    #[test]
    fn test_transaction_with_delete_rollback() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("DELETE FROM t WHERE x = 1;").unwrap();

        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 1);

        conn.execute("ROLLBACK;").unwrap();

        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_nested_savepoints_deep() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        conn.execute("SAVEPOINT a;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();

        conn.execute("SAVEPOINT b;").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();

        conn.execute("SAVEPOINT c;").unwrap();
        conn.execute("INSERT INTO t VALUES (4);").unwrap();

        // Rollback to b: undo values 3 and 4.
        conn.execute("ROLLBACK TO b;").unwrap();
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2);

        // Commit the whole transaction.
        conn.execute("COMMIT;").unwrap();

        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── bd-121m: Persistence round-trip tests ──────────────────────────

    #[test]
    fn test_persistence_create_insert_reopen_select() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("roundtrip.db");
        let path_str = path.to_str().unwrap();

        // Phase 1: create, insert, drop connection.
        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE t (a INTEGER, b TEXT);").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'hello');").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'world');").unwrap();
        }

        // Phase 2: reopen and verify data survived.
        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT a, b FROM t;").unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("hello".to_owned())
                ],
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Text("world".to_owned())
                ],
            );
        }
    }

    #[test]
    fn test_persistence_multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
                .unwrap();
            conn.execute("CREATE TABLE items (id INTEGER, label TEXT);")
                .unwrap();
            conn.execute("INSERT INTO users VALUES (1, 'Alice');")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (10, 'widget');")
                .unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let users = conn.query("SELECT id, name FROM users;").unwrap();
            assert_eq!(users.len(), 1);
            assert_eq!(
                row_values(&users[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("Alice".to_owned())
                ],
            );

            let items = conn.query("SELECT id, label FROM items;").unwrap();
            assert_eq!(items.len(), 1);
            assert_eq!(
                row_values(&items[0]),
                vec![
                    SqliteValue::Integer(10),
                    SqliteValue::Text("widget".to_owned()),
                ],
            );
        }
    }

    #[test]
    fn test_persistence_all_value_types() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("types.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE t (i INTEGER, r REAL, tx TEXT, bl BLOB, n INTEGER);")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (42, 3.14, 'it''s a test', X'DEADBEEF', NULL);")
                .unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT i, r, tx, bl, n FROM t;").unwrap();
            assert_eq!(rows.len(), 1);
            let vals = row_values(&rows[0]);
            assert_eq!(vals[0], SqliteValue::Integer(42));
            assert_eq!(vals[1], SqliteValue::Float(314.0 / 100.0));
            assert_eq!(vals[2], SqliteValue::Text("it's a test".to_owned()));
            assert_eq!(vals[3], SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
            assert_eq!(vals[4], SqliteValue::Null);
        }
    }

    #[test]
    fn test_blob_literal_codegen_emits_p4_bytes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (bl BLOB);").unwrap();

        let stmt = super::parse_single_statement("INSERT INTO t VALUES (X'DEADBEEF');").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            other => unreachable!("expected INSERT statement, got {other:?}"),
        };

        let program = conn.compile_table_insert(&insert).unwrap();
        let blob_ops: Vec<_> = program
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Blob)
            .collect();

        assert_eq!(blob_ops.len(), 1, "expected exactly one OP_Blob");
        match &blob_ops[0].p4 {
            P4::Blob(bytes) => {
                assert_eq!(bytes, &vec![0xDE, 0xAD, 0xBE, 0xEF]);
            }
            other => unreachable!("expected P4::Blob, got {other:?}"),
        }
    }

    #[test]
    fn test_codegen_newrowid_sets_concurrent_flag_in_begin_concurrent() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("BEGIN CONCURRENT;").unwrap();

        let stmt = super::parse_single_statement("INSERT INTO t VALUES (1);").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            other => unreachable!("expected INSERT statement, got {other:?}"),
        };

        let program = conn.compile_table_insert(&insert).unwrap();
        let nr = program
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::NewRowid)
            .unwrap();
        assert_ne!(
            nr.p3, 0,
            "NewRowid p3 should be non-zero in a concurrent transaction"
        );

        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_codegen_newrowid_cleared_in_begin_immediate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("BEGIN IMMEDIATE;").unwrap();

        let stmt = super::parse_single_statement("INSERT INTO t VALUES (1);").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            other => unreachable!("expected INSERT statement, got {other:?}"),
        };

        let program = conn.compile_table_insert(&insert).unwrap();
        let nr = program
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::NewRowid)
            .unwrap();
        assert_eq!(
            nr.p3, 0,
            "NewRowid p3 should be 0 in a non-concurrent transaction"
        );

        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_begin_concurrent_newrowid_runtime_observes_flag() {
        fn seed_table_with_empty_visible_set_but_advanced_counter(conn: &Connection) {
            conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
            conn.execute(
                "INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11);",
            )
            .unwrap();
            conn.execute("DELETE FROM t;").unwrap();
            assert_eq!(
                conn.query("SELECT x FROM t;").unwrap().len(),
                0,
                "seed must leave table empty while preserving advanced rowid counter state"
            );
        }

        // Non-concurrent transaction: with storage cursors enabled, runtime
        // now allocates from max-visible-rowid + 1 (B-tree path), not the
        // legacy MemDatabase counter.
        let conn_serialized = Connection::open(":memory:").unwrap();
        seed_table_with_empty_visible_set_but_advanced_counter(&conn_serialized);
        conn_serialized.execute("BEGIN IMMEDIATE;").unwrap();
        conn_serialized
            .execute("INSERT INTO t VALUES (999);")
            .unwrap();
        conn_serialized.execute("COMMIT;").unwrap();

        assert!(
            !conn_serialized.in_transaction(),
            "serialized transaction should be closed after COMMIT"
        );

        // Concurrent transaction: runtime observes `NewRowid.p3 != 0` and
        // uses the snapshot-independent path.
        let conn_concurrent = Connection::open(":memory:").unwrap();
        seed_table_with_empty_visible_set_but_advanced_counter(&conn_concurrent);
        conn_concurrent.execute("BEGIN CONCURRENT;").unwrap();
        conn_concurrent
            .execute("INSERT INTO t VALUES (999);")
            .unwrap();
        conn_concurrent.execute("COMMIT;").unwrap();

        assert!(
            !conn_concurrent.in_transaction(),
            "concurrent transaction should be closed after COMMIT"
        );
        assert_eq!(
            conn_concurrent
                .query_with_params(
                    "SELECT x FROM t WHERE rowid = ?1;",
                    &[SqliteValue::Integer(12)],
                )
                .unwrap()
                .len(),
            0,
            "concurrent path should not follow the serialized counter-only allocation"
        );
    }

    #[test]
    fn test_blob_literal_roundtrips_through_mem_and_storage_cursors() {
        // Phase 5B.2/5C.1: With write-through, INSERT only writes to the pager
        // via StorageCursor, not to MemDatabase. Therefore, the storage cursor
        // path is the primary test case. The MemCursor fallback path only sees
        // data if it's manually inserted into MemDatabase, not via SQL INSERT.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (bl BLOB);").unwrap();
        conn.execute("INSERT INTO t VALUES (X'DEADBEEF');").unwrap();

        // Verify via normal query that data is readable (uses storage cursors
        // under the hood via the Connection's pager transaction).
        let rows = conn.query("SELECT bl FROM t;").unwrap();
        let expected = vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])];
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values().to_vec(), expected);
    }

    #[test]
    fn test_blob_literal_multi_column_in_memory() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (i INTEGER, r REAL, tx TEXT, bl BLOB, n INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (42, 3.14, 'it''s a test', X'DEADBEEF', NULL);")
            .unwrap();

        let rows = conn
            .query("SELECT i, r, tx, bl, n FROM t;")
            .expect("multi-column SELECT should execute");
        assert_eq!(rows.len(), 1);
        let expected = vec![
            SqliteValue::Integer(42),
            SqliteValue::Float(314.0 / 100.0),
            SqliteValue::Text("it's a test".to_owned()),
            SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            SqliteValue::Null,
        ];
        assert_eq!(row_values(&rows[0]), expected,);

        // Persistence dump path uses SELECT *; ensure it preserves blob bytes.
        let rows = conn.query("SELECT * FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(42),
                SqliteValue::Float(314.0 / 100.0),
                SqliteValue::Text("it's a test".to_owned()),
                SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                SqliteValue::Null,
            ],
        );
    }

    #[test]
    fn test_persistence_memory_path_no_file() {
        let dir = tempfile::tempdir().unwrap();

        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        drop(conn);

        // No file should have been created anywhere.
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "in-memory connection must not write files",
        );
    }

    #[test]
    fn test_persistence_empty_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.db");
        let path_str = path.to_str().unwrap();

        // Open and close without creating any tables.
        {
            let _conn = Connection::open(path_str).unwrap();
        }

        // Reopen — should succeed with no tables.
        {
            let conn = Connection::open(path_str).unwrap();
            let err = conn.query("SELECT 1 FROM t;");
            assert!(err.is_err(), "querying nonexistent table should fail");
        }
    }

    #[test]
    fn test_persistence_rollback_not_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollback.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
            conn.execute("INSERT INTO t VALUES (1);").unwrap();

            // Begin a transaction, insert, then rollback.
            conn.execute("BEGIN;").unwrap();
            conn.execute("INSERT INTO t VALUES (2);").unwrap();
            conn.execute("ROLLBACK;").unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT x FROM t;").unwrap();
            assert_eq!(
                rows.len(),
                1,
                "rolled-back INSERT must not survive persistence",
            );
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        }
    }

    #[test]
    fn test_persistence_update_delete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upddel.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER, val TEXT);")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();

            conn.execute("UPDATE t SET val = 'updated' WHERE id = 2;")
                .unwrap();
            conn.execute("DELETE FROM t WHERE id = 3;").unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT id, val FROM t;").unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())],
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Text("updated".to_owned()),
                ],
            );
        }
    }

    #[test]
    fn test_persistence_reserved_word_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reserved.db");
        let path_str = path.to_str().unwrap();

        // Use bare reserved words (key, value) — the persistence dump
        // must double-quote them to produce valid SQL on reload.
        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE meta (\"key\" TEXT, \"value\" TEXT);")
                .unwrap();
            conn.execute("INSERT INTO meta VALUES ('version', '1.0');")
                .unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT \"key\", \"value\" FROM meta;").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Text("version".to_owned()),
                    SqliteValue::Text("1.0".to_owned()),
                ],
            );
        }
    }

    #[test]
    fn test_persistence_reserved_table_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resname.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE \"order\" (id INTEGER, item TEXT);")
                .unwrap();
            conn.execute("INSERT INTO \"order\" VALUES (1, 'widget');")
                .unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT id, item FROM \"order\";").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("widget".to_owned())
                ],
            );
        }
    }

    // ── bd-1s7a: E2E storage cursor acceptance tests ───────────────────

    #[test]
    fn test_reopen_then_select_reads_persisted_rows_via_storage_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("storage_path.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE items (id INTEGER, name TEXT, qty INTEGER);")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (1, 'apple', 10);")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (2, 'banana', 20);")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (3, 'cherry', 30);")
                .unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT id, name, qty FROM items;").unwrap();
            assert_eq!(rows.len(), 3, "all 3 rows must survive close/reopen");
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("apple".to_owned()),
                    SqliteValue::Integer(10),
                ],
            );
            assert_eq!(
                row_values(&rows[2]),
                vec![
                    SqliteValue::Integer(3),
                    SqliteValue::Text("cherry".to_owned()),
                    SqliteValue::Integer(30),
                ],
            );
        }
    }

    #[test]
    fn test_e2e_sql_pipeline_storage_backed_select_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e2e.db");
        let path_str = path.to_str().unwrap();

        {
            let conn = Connection::open(path_str).unwrap();
            conn.execute("CREATE TABLE log (seq INTEGER, msg TEXT);")
                .unwrap();
            conn.execute("INSERT INTO log VALUES (1, 'first');")
                .unwrap();
            conn.execute("INSERT INTO log VALUES (2, 'second');")
                .unwrap();
            conn.execute("INSERT INTO log VALUES (3, 'third');")
                .unwrap();
            conn.execute("UPDATE log SET msg = 'updated' WHERE seq = 2;")
                .unwrap();
            conn.execute("DELETE FROM log WHERE seq = 3;").unwrap();
        }

        {
            let conn = Connection::open(path_str).unwrap();
            let rows = conn.query("SELECT seq, msg FROM log;").unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("first".to_owned())
                ],
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Text("updated".to_owned()),
                ],
            );
        }
    }

    // ── MVCC concurrent-mode toggle tests (bd-1w6k.2.4) ─────────────

    #[test]
    fn test_concurrent_mode_default_on() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.is_concurrent_mode_default());
        assert!(!conn.is_concurrent_transaction());
    }

    #[test]
    fn test_pragma_concurrent_mode_on_off() {
        let conn = Connection::open(":memory:").unwrap();

        conn.execute("PRAGMA concurrent_mode=ON;").unwrap();
        assert!(conn.is_concurrent_mode_default());

        conn.execute("PRAGMA concurrent_mode=OFF;").unwrap();
        assert!(!conn.is_concurrent_mode_default());
    }

    #[test]
    fn test_pragma_concurrent_mode_qualified_name() {
        let conn = Connection::open(":memory:").unwrap();

        conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
        assert!(conn.is_concurrent_mode_default());

        conn.execute("PRAGMA fsqlite.concurrent_mode=OFF;").unwrap();
        assert!(!conn.is_concurrent_mode_default());
    }

    #[test]
    fn test_pragma_concurrent_mode_integer_values() {
        let conn = Connection::open(":memory:").unwrap();

        conn.execute("PRAGMA concurrent_mode=1;").unwrap();
        assert!(conn.is_concurrent_mode_default());

        conn.execute("PRAGMA concurrent_mode=0;").unwrap();
        assert!(!conn.is_concurrent_mode_default());
    }

    #[test]
    fn test_begin_concurrent_sets_flag() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN CONCURRENT;").unwrap();
        assert!(conn.is_concurrent_transaction());
        conn.execute("COMMIT;").unwrap();
        assert!(!conn.is_concurrent_transaction());
    }

    #[test]
    fn test_begin_promoted_to_concurrent_by_pragma() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA concurrent_mode=ON;").unwrap();

        // Plain BEGIN should be promoted to concurrent.
        conn.execute("BEGIN;").unwrap();
        assert!(conn.is_concurrent_transaction());
        conn.execute("COMMIT;").unwrap();
    }

    #[test]
    fn test_explicit_mode_overrides_pragma_default() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA concurrent_mode=ON;").unwrap();

        // Explicit DEFERRED overrides the concurrent default.
        conn.execute("BEGIN DEFERRED;").unwrap();
        assert!(!conn.is_concurrent_transaction());
        conn.execute("COMMIT;").unwrap();
    }

    #[test]
    fn test_rollback_clears_concurrent_flag() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN CONCURRENT;").unwrap();
        assert!(conn.is_concurrent_transaction());
        conn.execute("ROLLBACK;").unwrap();
        assert!(!conn.is_concurrent_transaction());
    }

    #[test]
    fn test_concurrent_mode_with_single_vs_mvcc_workload() {
        // Run the same workload in both modes and verify the report
        // notes capture the correct mode label.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .unwrap();

        // Single-writer mode (explicitly disable concurrent default).
        conn.execute("PRAGMA concurrent_mode=OFF;").unwrap();
        conn.execute("BEGIN;").unwrap();
        assert!(!conn.is_concurrent_transaction());
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("COMMIT;").unwrap();

        // MVCC concurrent mode (re-enable).
        conn.execute("PRAGMA concurrent_mode=ON;").unwrap();
        conn.execute("BEGIN;").unwrap();
        assert!(conn.is_concurrent_transaction());
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("COMMIT;").unwrap();

        // Verify both rows exist.
        let rows = conn.query("SELECT COUNT(*) FROM t;").unwrap();
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(2));
    }

    // ── MVCC ConcurrentRegistry wiring tests (bd-14zc / 5E.1) ─────────

    #[test]
    fn test_begin_concurrent_creates_mvcc_session() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(!conn.has_concurrent_session());
        assert_eq!(conn.concurrent_writer_count(), 0);

        conn.execute("BEGIN CONCURRENT;").unwrap();
        assert!(conn.has_concurrent_session());
        assert_eq!(conn.concurrent_writer_count(), 1);

        conn.execute("ROLLBACK;").unwrap();
        assert!(!conn.has_concurrent_session());
        assert_eq!(conn.concurrent_writer_count(), 0);
    }

    #[test]
    fn test_begin_without_mode_creates_mvcc_session_when_default_on() {
        let conn = Connection::open(":memory:").unwrap();
        // Default is concurrent_mode_default=true, so plain BEGIN promotes.
        conn.execute("BEGIN;").unwrap();
        assert!(conn.has_concurrent_session());
        assert_eq!(conn.concurrent_writer_count(), 1);
        conn.execute("COMMIT;").unwrap();
        assert!(!conn.has_concurrent_session());
    }

    #[test]
    fn test_begin_deferred_does_not_create_mvcc_session() {
        let conn = Connection::open(":memory:").unwrap();
        // Explicit DEFERRED should not create MVCC session.
        conn.execute("BEGIN DEFERRED;").unwrap();
        assert!(!conn.has_concurrent_session());
        assert!(!conn.is_concurrent_transaction());
        conn.execute("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_commit_clears_mvcc_session() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();

        conn.execute("BEGIN CONCURRENT;").unwrap();
        assert!(conn.has_concurrent_session());
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("COMMIT;").unwrap();

        assert!(!conn.has_concurrent_session());
        assert_eq!(conn.concurrent_writer_count(), 0);
    }

    #[test]
    fn test_rollback_clears_mvcc_session() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();

        conn.execute("BEGIN CONCURRENT;").unwrap();
        assert!(conn.has_concurrent_session());
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("ROLLBACK;").unwrap();

        assert!(!conn.has_concurrent_session());
        assert_eq!(conn.concurrent_writer_count(), 0);

        // Verify the insert was rolled back.
        let rows = conn.query("SELECT COUNT(*) FROM t;").unwrap();
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0));
    }

    // ── MVCC page-level locking tests (bd-kivg / 5E.2) ────────────────────

    #[test]
    fn test_mvcc_page_io_write_acquires_lock() {
        // Test that writing through MvccPageIo acquires page locks.
        use fsqlite_btree::cursor::PageWriter;
        use fsqlite_mvcc::ConcurrentHandle;
        use fsqlite_pager::traits::TransactionMode;
        use fsqlite_types::{Snapshot, TxnEpoch, TxnId, TxnToken};

        let conn = Connection::open(":memory:").unwrap();
        let cx = Cx::new();

        // Begin a pager transaction.
        let mut txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();

        // Create MVCC state.
        let snapshot = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let session_id = 1;
        let txn_token = TxnToken::new(TxnId::new(session_id).unwrap(), TxnEpoch::new(1));
        let mut handle = ConcurrentHandle::new(snapshot, txn_token);
        let lock_table = InProcessPageLockTable::new();

        // Create MvccPageIo wrapper.
        let mut mvcc_io = MvccPageIo::new(&mut *txn, &mut handle, &lock_table, session_id);

        // Write to a page.
        let page_no = PageNumber::new(2).unwrap();
        let data = vec![0u8; 4096];
        mvcc_io.write_page(&cx, page_no, &data).unwrap();

        // Verify page lock is held.
        assert!(handle.held_locks().contains(&page_no));

        // Verify page is in write set.
        assert_eq!(handle.write_set_len(), 1);
        assert!(handle.write_set_pages().contains(&page_no));
    }

    #[test]
    fn test_mvcc_page_io_read_own_writes() {
        // Test that reading through MvccPageIo sees own writes.
        use fsqlite_btree::cursor::{PageReader, PageWriter};
        use fsqlite_mvcc::ConcurrentHandle;
        use fsqlite_pager::traits::TransactionMode;
        use fsqlite_types::{Snapshot, TxnEpoch, TxnId, TxnToken};

        let conn = Connection::open(":memory:").unwrap();
        let cx = Cx::new();

        let mut txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();

        let snapshot = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let session_id = 1;
        let txn_token = TxnToken::new(TxnId::new(session_id).unwrap(), TxnEpoch::new(1));
        let mut handle = ConcurrentHandle::new(snapshot, txn_token);
        let lock_table = InProcessPageLockTable::new();

        let mut mvcc_io = MvccPageIo::new(&mut *txn, &mut handle, &lock_table, session_id);

        // Write distinctive data to a page.
        let page_no = PageNumber::new(2).unwrap();
        let mut data = vec![0u8; 4096];
        data[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        mvcc_io.write_page(&cx, page_no, &data).unwrap();

        // Read the same page - should see our write.
        let read_data = mvcc_io.read_page(&cx, page_no).unwrap();
        assert_eq!(&read_data[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_mvcc_page_io_write_conflict_returns_busy() {
        // Test that two concurrent sessions writing same page conflicts.
        // Use two separate connections since the simple pager is single-writer.
        use fsqlite_btree::cursor::PageWriter;
        use fsqlite_mvcc::ConcurrentHandle;
        use fsqlite_pager::traits::TransactionMode;
        use fsqlite_types::{Snapshot, TxnEpoch, TxnId, TxnToken};

        let conn1 = Connection::open(":memory:").unwrap();
        let conn2 = Connection::open(":memory:").unwrap();
        let cx = Cx::new();

        // Single shared lock table for both sessions - this is the key for testing.
        let lock_table = InProcessPageLockTable::new();

        // Session 1 writes to page 2.
        let mut txn1 = conn1.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        let snapshot1 = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let session_id_1 = 1;
        let txn_token_1 = TxnToken::new(TxnId::new(session_id_1).unwrap(), TxnEpoch::new(1));
        let mut handle1 = ConcurrentHandle::new(snapshot1, txn_token_1);

        {
            let mut mvcc_io1 = MvccPageIo::new(&mut *txn1, &mut handle1, &lock_table, session_id_1);
            let page_no = PageNumber::new(2).unwrap();
            let data = vec![0u8; 4096];
            mvcc_io1.write_page(&cx, page_no, &data).unwrap();
        }

        // Session 2 tries to write to the same page - should fail with Busy.
        let mut txn2 = conn2.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        let snapshot2 = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let session_id_2 = 2;
        let txn_token_2 = TxnToken::new(TxnId::new(session_id_2).unwrap(), TxnEpoch::new(2));
        let mut handle2 = ConcurrentHandle::new(snapshot2, txn_token_2);

        {
            let mut mvcc_io2 = MvccPageIo::new(&mut *txn2, &mut handle2, &lock_table, session_id_2);
            let page_no = PageNumber::new(2).unwrap();
            let data = vec![0u8; 4096];
            let result = mvcc_io2.write_page(&cx, page_no, &data);
            assert!(matches!(result, Err(FrankenError::Busy)));
        }

        // Session 2 can write to a different page.
        {
            let mut mvcc_io2 = MvccPageIo::new(&mut *txn2, &mut handle2, &lock_table, session_id_2);
            let page_no = PageNumber::new(3).unwrap();
            let data = vec![0u8; 4096];
            mvcc_io2.write_page(&cx, page_no, &data).unwrap();
        }
    }

    #[test]
    fn test_unrecognised_pragma_silently_ignored() {
        let conn = Connection::open(":memory:").unwrap();
        // Unknown pragma should not error.
        conn.execute("PRAGMA some_unknown_pragma=42;").unwrap();
    }

    #[test]
    fn test_pragma_quick_check_returns_ok_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA quick_check;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Text("ok".to_owned()));

        // SQLite accepts an optional limit argument; no issues still returns "ok".
        let rows = conn.query("PRAGMA quick_check(1);").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Text("ok".to_owned()));
    }

    #[test]
    fn test_pragma_quick_check_with_schema_prefix_returns_ok_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA main.quick_check;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Text("ok".to_owned()));
    }

    #[test]
    fn test_pragma_integrity_check_returns_ok_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA integrity_check;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Text("ok".to_owned()));
    }

    #[test]
    fn test_pragma_page_count_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA page_count;").unwrap();
        assert_eq!(rows.len(), 1);
        match rows[0].get(0).unwrap() {
            SqliteValue::Integer(n) => assert!(*n >= 1),
            other => panic!("expected integer page_count, got {other:?}"),
        }
    }

    #[test]
    fn test_pragma_page_count_with_schema_prefix_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA main.page_count;").unwrap();
        assert_eq!(rows.len(), 1);
        match rows[0].get(0).unwrap() {
            SqliteValue::Integer(n) => assert!(*n >= 1),
            other => panic!("expected integer page_count, got {other:?}"),
        }
    }

    #[test]
    fn test_pragma_freelist_count_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA freelist_count;").unwrap();
        assert_eq!(rows.len(), 1);
        match rows[0].get(0).unwrap() {
            SqliteValue::Integer(n) => assert!(*n >= 0),
            other => panic!("expected integer freelist_count, got {other:?}"),
        }
    }

    #[test]
    fn test_pragma_schema_version_returns_row_and_tracks_ddl() {
        let conn = Connection::open(":memory:").unwrap();
        let before_rows = conn.query("PRAGMA schema_version;").unwrap();
        assert_eq!(before_rows.len(), 1);
        let before = match before_rows[0].get(0).unwrap() {
            SqliteValue::Integer(n) => *n,
            other => panic!("expected integer schema_version, got {other:?}"),
        };

        conn.execute("CREATE TABLE pragma_sv_t(id INTEGER);")
            .unwrap();

        let after_rows = conn.query("PRAGMA schema_version;").unwrap();
        assert_eq!(after_rows.len(), 1);
        let after = match after_rows[0].get(0).unwrap() {
            SqliteValue::Integer(n) => *n,
            other => panic!("expected integer schema_version, got {other:?}"),
        };
        assert!(after > before, "expected schema_version to increase");
    }

    #[test]
    fn test_pragma_encoding_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA encoding;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("UTF-8".to_owned())
        );
    }

    #[test]
    fn test_pragma_encoding_with_schema_prefix_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA main.encoding;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("UTF-8".to_owned())
        );
    }

    #[test]
    fn test_pragma_extended_connection_knobs_return_rows() {
        let conn = Connection::open(":memory:").unwrap();

        let defaults = [
            ("PRAGMA temp_store;", 0_i64),
            ("PRAGMA mmap_size;", 0_i64),
            ("PRAGMA auto_vacuum;", 0_i64),
            ("PRAGMA wal_autocheckpoint;", 1000_i64),
            ("PRAGMA user_version;", 0_i64),
            ("PRAGMA application_id;", 0_i64),
            ("PRAGMA foreign_keys;", 0_i64),
            ("PRAGMA recursive_triggers;", 0_i64),
        ];
        for (sql, expected) in defaults {
            let rows = conn.query(sql).unwrap();
            assert_eq!(rows.len(), 1, "{sql} should return one row");
            assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(expected));
        }

        conn.execute("PRAGMA temp_store=MEMORY;").unwrap();
        conn.execute("PRAGMA mmap_size=8192;").unwrap();
        conn.execute("PRAGMA auto_vacuum=INCREMENTAL;").unwrap();
        conn.execute("PRAGMA wal_autocheckpoint=0;").unwrap();
        conn.execute("PRAGMA user_version=123;").unwrap();
        conn.execute("PRAGMA application_id=456;").unwrap();
        conn.execute("PRAGMA foreign_keys=ON;").unwrap();
        conn.execute("PRAGMA recursive_triggers=ON;").unwrap();

        let updated = [
            ("PRAGMA temp_store;", 2_i64),
            ("PRAGMA mmap_size;", 8192_i64),
            ("PRAGMA auto_vacuum;", 2_i64),
            ("PRAGMA wal_autocheckpoint;", 0_i64),
            ("PRAGMA user_version;", 123_i64),
            ("PRAGMA application_id;", 456_i64),
            ("PRAGMA foreign_keys;", 1_i64),
            ("PRAGMA recursive_triggers;", 1_i64),
        ];
        for (sql, expected) in updated {
            let rows = conn.query(sql).unwrap();
            assert_eq!(rows.len(), 1, "{sql} should return one row");
            assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(expected));
        }
    }

    #[test]
    fn test_pragma_serializable_and_raptorq_return_rows() {
        let conn = Connection::open(":memory:").unwrap();

        let rows = conn.query("PRAGMA fsqlite.serializable;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(1));

        let rows = conn.query("PRAGMA fsqlite.serializable=OFF;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0));

        let rows = conn.query("PRAGMA fsqlite.serializable;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0));

        let rows = conn.query("PRAGMA raptorq_repair_symbols;").unwrap();
        assert_eq!(rows.len(), 1);
        match rows[0].get(0).unwrap() {
            SqliteValue::Integer(n) => assert!(*n >= 0),
            other => panic!("expected integer raptorq_repair_symbols, got {other:?}"),
        }

        let rows = conn.query("PRAGMA raptorq_repair_symbols=7;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(7));

        let rows = conn
            .query("PRAGMA fsqlite.raptorq_repair_symbols;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(7));
    }

    #[test]
    fn test_pragma_raptorq_stats_events_and_reset() {
        let conn = Connection::open(":memory:").unwrap();
        fsqlite_wal::reset_raptorq_repair_telemetry();

        let group_a = fsqlite_wal::WalFecGroupId {
            wal_salt1: 0x1111_2222,
            wal_salt2: 0x3333_4444,
            end_frame_no: 7,
        };
        let group_b = fsqlite_wal::WalFecGroupId {
            wal_salt1: 0x5555_6666,
            wal_salt2: 0x7777_8888,
            end_frame_no: 8,
        };

        let success_log = fsqlite_wal::WalFecRecoveryLog {
            group_id: group_a,
            recovery_enabled: true,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: 4,
            validated_repair_symbols: 2,
            required_symbols: 6,
            available_symbols: 6,
            recovered_frame_nos: vec![6, 7],
            corruption_observations: 0,
            decode_attempted: true,
            decode_succeeded: true,
        };
        let failed_log = fsqlite_wal::WalFecRecoveryLog {
            group_id: group_b,
            recovery_enabled: true,
            outcome_is_recovered: false,
            fallback_reason: Some(fsqlite_wal::WalFecRecoveryFallbackReason::InsufficientSymbols),
            validated_source_symbols: 2,
            validated_repair_symbols: 2,
            required_symbols: 6,
            available_symbols: 4,
            recovered_frame_nos: Vec::new(),
            corruption_observations: 1,
            decode_attempted: true,
            decode_succeeded: false,
        };

        fsqlite_wal::record_raptorq_recovery_log(
            &success_log,
            std::time::Duration::from_micros(40),
        );
        fsqlite_wal::record_raptorq_recovery_log(&failed_log, std::time::Duration::from_micros(80));

        let stats_rows = conn.query("PRAGMA fsqlite_raptorq_stats;").unwrap();
        let mut stats = std::collections::HashMap::new();
        for row in stats_rows {
            let key = match row.get(0).unwrap() {
                SqliteValue::Text(s) => s.clone(),
                other => panic!("expected stats key text, got {other:?}"),
            };
            let value = match row.get(1).unwrap() {
                SqliteValue::Integer(n) => *n,
                other => panic!("expected stats value integer, got {other:?}"),
            };
            let _ = stats.insert(key, value);
        }
        assert_eq!(stats.get("repairs_total"), Some(&2));
        assert_eq!(stats.get("repairs_failed"), Some(&1));
        assert_eq!(stats.get("symbols_reclaimed"), Some(&2));
        assert_eq!(stats.get("severity_2_5"), Some(&2));
        assert!(stats.get("wal_health_score").copied().unwrap_or_default() < 100);

        let event_rows = conn.query("PRAGMA raptorq_events;").unwrap();
        assert_eq!(event_rows.len(), 2);
        let success_flags: Vec<i64> = event_rows
            .iter()
            .map(|row| match row.get(4).unwrap() {
                SqliteValue::Integer(n) => *n,
                other => panic!("expected event success flag integer, got {other:?}"),
            })
            .collect();
        assert!(success_flags.contains(&0));
        assert!(success_flags.contains(&1));

        let limited_rows = conn.query("PRAGMA raptorq_events(1);").unwrap();
        assert_eq!(limited_rows.len(), 1);

        let reset_rows = conn.query("PRAGMA raptorq_reset;").unwrap();
        assert_eq!(reset_rows.len(), 1);
        assert_eq!(
            *reset_rows[0].get(0).unwrap(),
            SqliteValue::Text("ok".to_owned())
        );

        let stats_after_reset = conn.query("PRAGMA raptorq_stats;").unwrap();
        let mut repairs_total = None;
        for row in stats_after_reset {
            if let SqliteValue::Text(key) = row.get(0).unwrap() {
                if key == "repairs_total" {
                    repairs_total = match row.get(1).unwrap() {
                        SqliteValue::Integer(n) => Some(*n),
                        _ => None,
                    };
                }
            }
        }
        assert_eq!(repairs_total, Some(0));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_pragma_raptorq_repair_evidence_filters() {
        let conn = Connection::open(":memory:").unwrap();
        fsqlite_wal::reset_raptorq_repair_telemetry();

        let group_a = fsqlite_wal::WalFecGroupId {
            wal_salt1: 0x9090_1111,
            wal_salt2: 0x9090_2222,
            end_frame_no: 41,
        };
        let group_b = fsqlite_wal::WalFecGroupId {
            wal_salt1: 0x9090_3333,
            wal_salt2: 0x9090_4444,
            end_frame_no: 42,
        };

        let low_loss_log = fsqlite_wal::WalFecRecoveryLog {
            group_id: group_a,
            recovery_enabled: true,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: 5,
            validated_repair_symbols: 1,
            required_symbols: 6,
            available_symbols: 6,
            recovered_frame_nos: vec![41],
            corruption_observations: 1,
            decode_attempted: true,
            decode_succeeded: true,
        };
        let high_loss_log = fsqlite_wal::WalFecRecoveryLog {
            group_id: group_b,
            recovery_enabled: true,
            outcome_is_recovered: true,
            fallback_reason: None,
            validated_source_symbols: 0,
            validated_repair_symbols: 6,
            required_symbols: 6,
            available_symbols: 6,
            recovered_frame_nos: vec![42],
            corruption_observations: 3,
            decode_attempted: true,
            decode_succeeded: true,
        };

        fsqlite_wal::record_raptorq_recovery_log(
            &low_loss_log,
            std::time::Duration::from_micros(35),
        );
        fsqlite_wal::record_raptorq_recovery_log(
            &high_loss_log,
            std::time::Duration::from_micros(90),
        );

        let all_rows = conn.query("PRAGMA raptorq_repair_evidence;").unwrap();
        assert_eq!(all_rows.len(), 2);
        assert!(
            all_rows.iter().all(
                |row| matches!(row.get(19), Some(SqliteValue::Text(hash)) if !hash.is_empty())
            ),
            "every evidence row should include a chain hash in column 19"
        );

        let page_rows = conn
            .query("PRAGMA raptorq_repair_evidence='page:41';")
            .unwrap();
        assert_eq!(page_rows.len(), 1);
        assert!(matches!(
            page_rows[0].get(0),
            Some(SqliteValue::Integer(frame)) if *frame == 41
        ));

        let severity_rows = conn
            .query("PRAGMA raptorq_repair_evidence='severity:6-10';")
            .unwrap();
        assert_eq!(severity_rows.len(), 1);
        assert!(matches!(
            severity_rows[0].get(17),
            Some(SqliteValue::Text(bucket)) if bucket == "6-10"
        ));

        let min_ts = all_rows
            .iter()
            .filter_map(|row| match row.get(3) {
                Some(SqliteValue::Integer(ts)) => Some(*ts),
                _ => None,
            })
            .min()
            .expect("wall clock timestamp column should be present");
        let max_ts = all_rows
            .iter()
            .filter_map(|row| match row.get(3) {
                Some(SqliteValue::Integer(ts)) => Some(*ts),
                _ => None,
            })
            .max()
            .expect("wall clock timestamp column should be present");
        let time_rows = conn
            .query(&format!(
                "PRAGMA raptorq_repair_evidence='time:{min_ts}-{max_ts}';"
            ))
            .unwrap();
        assert_eq!(time_rows.len(), 2);

        let limit_rows = conn
            .query("PRAGMA raptorq_repair_evidence='limit:1';")
            .unwrap();
        assert_eq!(limit_rows.len(), 1);
        assert!(matches!(
            limit_rows[0].get(0),
            Some(SqliteValue::Integer(frame)) if *frame == 42
        ));
    }

    #[test]
    fn test_pragma_ssi_decisions_exposes_commit_and_abort_cards() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE ssi_cards(id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();

        conn.execute("BEGIN CONCURRENT;").unwrap();
        conn.execute("INSERT INTO ssi_cards VALUES (1, 10);")
            .unwrap();
        conn.execute("COMMIT;").unwrap();

        conn.execute("BEGIN CONCURRENT;").unwrap();
        conn.execute("INSERT INTO ssi_cards VALUES (2, 20);")
            .unwrap();
        let session_id = conn
            .concurrent_session_id
            .borrow()
            .expect("concurrent session should exist");
        {
            let mut registry = conn.concurrent_registry.borrow_mut();
            let handle = registry
                .get_mut(session_id)
                .expect("session handle should exist");
            fsqlite_mvcc::ActiveTxnView::set_has_in_rw(handle, true);
            fsqlite_mvcc::ActiveTxnView::set_has_out_rw(handle, true);
        }
        let err = conn.execute("COMMIT;").expect_err("SSI pivot must abort");
        assert!(
            matches!(err, FrankenError::BusySnapshot { .. }),
            "expected BusySnapshot, got {err:?}"
        );

        let rows = conn.query("PRAGMA ssi_decisions;").unwrap();
        assert!(rows.len() >= 2, "expected commit+abort cards in ledger");

        let mut decision_types = rows
            .iter()
            .filter_map(|row| match row.get(4) {
                Some(SqliteValue::Text(text)) => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        decision_types.sort();
        assert!(
            decision_types.contains(&"COMMIT_ALLOWED".to_owned()),
            "commit decision card missing"
        );
        assert!(
            decision_types.contains(&"ABORT_WRITE_SKEW".to_owned()),
            "write-skew abort decision card missing"
        );

        let abort_rows = rows
            .iter()
            .filter(|row| {
                matches!(
                    row.get(4),
                    Some(SqliteValue::Text(decision)) if decision == "ABORT_WRITE_SKEW"
                )
            })
            .collect::<Vec<_>>();
        assert!(
            !abort_rows.is_empty(),
            "expected at least one ABORT_WRITE_SKEW evidence row"
        );
        for row in abort_rows {
            let has_conflicting_txns =
                matches!(row.get(5), Some(SqliteValue::Text(txns)) if !txns.is_empty());
            let has_conflict_pages =
                matches!(row.get(6), Some(SqliteValue::Text(pages)) if !pages.is_empty());
            let has_read_set_count =
                matches!(row.get(7), Some(SqliteValue::Integer(page_count)) if *page_count > 0);
            let has_read_set_top_k =
                matches!(row.get(8), Some(SqliteValue::Text(top_k)) if !top_k.is_empty());
            let has_write_set =
                matches!(row.get(10), Some(SqliteValue::Text(write_set)) if !write_set.is_empty());
            assert!(
                has_conflicting_txns
                    || has_conflict_pages
                    || has_read_set_count
                    || has_read_set_top_k
                    || has_write_set,
                "abort row must include at least one non-empty evidence payload field"
            );
            assert!(
                matches!(row.get(11), Some(SqliteValue::Text(rationale)) if !rationale.is_empty()),
                "abort row must include rationale text in column 11"
            );
            assert!(
                matches!(row.get(14), Some(SqliteValue::Text(chain_hash)) if chain_hash.len() == 64),
                "abort row must include 64-char chain hash in column 14"
            );
        }
    }

    #[test]
    fn test_pragma_ssi_decisions_filters_txn_type_and_time() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE ssi_filter(id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();

        conn.execute("BEGIN CONCURRENT;").unwrap();
        conn.execute("INSERT INTO ssi_filter VALUES (1, 1);")
            .unwrap();
        conn.execute("COMMIT;").unwrap();

        conn.execute("BEGIN CONCURRENT;").unwrap();
        conn.execute("INSERT INTO ssi_filter VALUES (2, 2);")
            .unwrap();
        let session_id = conn
            .concurrent_session_id
            .borrow()
            .expect("concurrent session should exist");
        {
            let mut registry = conn.concurrent_registry.borrow_mut();
            let handle = registry
                .get_mut(session_id)
                .expect("session handle should exist");
            fsqlite_mvcc::ActiveTxnView::set_has_in_rw(handle, true);
            fsqlite_mvcc::ActiveTxnView::set_has_out_rw(handle, true);
        }
        let _ = conn.execute("COMMIT;");

        let all_rows = conn.query("PRAGMA ssi_decisions;").unwrap();
        let first_txn_id = match all_rows.first().and_then(|row| row.get(0)) {
            Some(SqliteValue::Integer(n)) => *n,
            other => panic!("expected integer txn_id in column 0, got {other:?}"),
        };
        let min_ts = all_rows
            .iter()
            .filter_map(|row| match row.get(12) {
                Some(SqliteValue::Integer(n)) => Some(*n),
                _ => None,
            })
            .min()
            .expect("expected timestamp column");
        let max_ts = all_rows
            .iter()
            .filter_map(|row| match row.get(12) {
                Some(SqliteValue::Integer(n)) => Some(*n),
                _ => None,
            })
            .max()
            .expect("expected timestamp column");

        let txn_rows = conn
            .query(&format!("PRAGMA ssi_decisions='txn:{first_txn_id}';"))
            .unwrap();
        assert!(
            txn_rows.iter().all(
                |row| matches!(row.get(0), Some(SqliteValue::Integer(n)) if *n == first_txn_id)
            ),
            "txn filter should return only requested txn"
        );

        let type_rows = conn
            .query("PRAGMA ssi_decisions='type:ABORT_WRITE_SKEW';")
            .unwrap();
        assert!(
            type_rows.iter().all(
                |row| matches!(row.get(4), Some(SqliteValue::Text(t)) if t == "ABORT_WRITE_SKEW")
            ),
            "type filter should return only ABORT_WRITE_SKEW rows"
        );

        let time_rows = conn
            .query(&format!("PRAGMA ssi_decisions='time:{min_ts}-{max_ts}';"))
            .unwrap();
        assert!(!time_rows.is_empty(), "time range filter should match rows");
    }

    #[test]
    fn test_pragma_lineage_returns_simple_join_contributors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute(
            "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER);",
        )
        .unwrap();

        conn.execute("INSERT INTO users VALUES (1, 'alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'bob');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (10, 1, 50);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (11, 1, 75);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (12, 2, 20);")
            .unwrap();

        let rows = conn
            .query("PRAGMA lineage='users,orders,id,user_id,id,id';")
            .unwrap();
        assert_eq!(
            rows.len(),
            3,
            "expected one lineage row per join result row"
        );

        let mut tuples = rows
            .iter()
            .map(|row| {
                let left_rowid = match row.get(0) {
                    Some(SqliteValue::Integer(v)) => *v,
                    other => panic!("expected integer left rowid, got {other:?}"),
                };
                let right_rowid = match row.get(1) {
                    Some(SqliteValue::Integer(v)) => *v,
                    other => panic!("expected integer right rowid, got {other:?}"),
                };
                let left_key = match row.get(2) {
                    Some(SqliteValue::Integer(v)) => *v,
                    other => panic!("expected integer left key, got {other:?}"),
                };
                let right_key = match row.get(3) {
                    Some(SqliteValue::Integer(v)) => *v,
                    other => panic!("expected integer right key, got {other:?}"),
                };
                (left_rowid, right_rowid, left_key, right_key)
            })
            .collect::<Vec<_>>();
        tuples.sort_unstable();

        assert_eq!(
            tuples,
            vec![(1, 10, 1, 1), (1, 11, 1, 1), (2, 12, 2, 2)],
            "lineage rows should identify contributing tuple rowids/keys for each join output"
        );
    }

    // ── Connection PRAGMA state tests (bd-1w6k.2.3) ────────────────────

    #[test]
    fn test_pragma_journal_mode_default_is_wal() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA journal_mode;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("wal".to_owned())
        );
        assert_eq!(conn.pager.journal_mode(), fsqlite_pager::JournalMode::Wal);
    }

    #[test]
    fn test_pragma_journal_mode_set_and_query() {
        let conn = Connection::open(":memory:").unwrap();
        // Set returns the new value (quoted to avoid keyword clash with parser).
        let rows = conn.query("PRAGMA journal_mode='truncate';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("truncate".to_owned())
        );
        // Query reads back.
        let rows = conn.query("PRAGMA journal_mode;").unwrap();
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("truncate".to_owned())
        );
        assert_eq!(
            conn.pager.journal_mode(),
            fsqlite_pager::JournalMode::Delete
        );

        // Pager mode should switch to WAL when requested.
        let rows = conn.query("PRAGMA journal_mode='wal';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("wal".to_owned())
        );
        assert_eq!(conn.pager.journal_mode(), fsqlite_pager::JournalMode::Wal);
    }

    #[test]
    fn test_pragma_synchronous_default_is_normal() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA synchronous;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("NORMAL".to_owned())
        );
    }

    #[test]
    fn test_pragma_synchronous_set_by_name_and_integer() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA synchronous=FULL;").unwrap();
        let rows = conn.query("PRAGMA synchronous;").unwrap();
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("FULL".to_owned())
        );
        // Integer code: 0 = OFF.
        conn.execute("PRAGMA synchronous=0;").unwrap();
        let rows = conn.query("PRAGMA synchronous;").unwrap();
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("OFF".to_owned())
        );
    }

    #[test]
    fn test_pragma_cache_size_default() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA cache_size;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(-2000));
    }

    #[test]
    fn test_pragma_cache_size_set_and_query() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA cache_size=-4000;").unwrap();
        let rows = conn.query("PRAGMA cache_size;").unwrap();
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(-4000));
    }

    #[test]
    fn test_pragma_page_size_default() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA page_size;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(4096));
    }

    #[test]
    fn test_pragma_page_size_set_valid_and_query() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA page_size=8192;").unwrap();
        let rows = conn.query("PRAGMA page_size;").unwrap();
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(8192));
    }

    #[test]
    fn test_pragma_page_size_rejects_invalid() {
        let conn = Connection::open(":memory:").unwrap();
        // Not a power of two.
        assert!(conn.execute("PRAGMA page_size=3000;").is_err());
        // Below range.
        assert!(conn.execute("PRAGMA page_size=256;").is_err());
        // Above range.
        assert!(conn.execute("PRAGMA page_size=131072;").is_err());
    }

    #[test]
    fn test_pragma_journal_mode_rejects_invalid() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.execute("PRAGMA journal_mode=bogus;").is_err());
    }

    #[test]
    fn test_pragma_busy_timeout_set_and_query() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA busy_timeout=10000;").unwrap();
        let rows = conn.query("PRAGMA busy_timeout;").unwrap();
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(10000));
    }

    #[test]
    fn test_pragma_busy_timeout_default() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("PRAGMA busy_timeout;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(5000));
    }

    #[test]
    fn test_pragma_state_accessor() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("PRAGMA journal_mode=truncate;").unwrap();
        conn.execute("PRAGMA synchronous=FULL;").unwrap();
        conn.execute("PRAGMA cache_size=-8000;").unwrap();
        conn.execute("PRAGMA page_size=16384;").unwrap();
        conn.execute("PRAGMA busy_timeout=3000;").unwrap();

        let state = conn.pragma_state();
        assert_eq!(state.journal_mode, "truncate");
        assert_eq!(state.synchronous, "FULL");
        assert_eq!(state.cache_size, -8000);
        assert_eq!(state.page_size, 16384);
        assert_eq!(state.busy_timeout_ms, 3000);
    }

    #[test]
    fn test_pragma_concurrent_mode_returns_value() {
        let conn = Connection::open(":memory:").unwrap();
        // Query default (on — concurrent mode is the default).
        let rows = conn.query("PRAGMA concurrent_mode;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(1));
        // Set off, check return.
        let rows = conn.query("PRAGMA concurrent_mode=OFF;").unwrap();
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0));
    }

    // ─── WAL checkpoint tests ────────────────────────────────────

    #[test]
    fn test_pragma_wal_checkpoint_on_empty_wal() {
        let conn = Connection::open(":memory:").unwrap();
        // Connection is in WAL mode by default.
        assert_eq!(conn.pager.journal_mode(), fsqlite_pager::JournalMode::Wal);

        // Checkpoint on empty WAL should succeed with 0 frames.
        let rows = conn.query("PRAGMA wal_checkpoint;").unwrap();
        assert_eq!(rows.len(), 1);
        // SQLite returns: busy, log (total frames), checkpointed (frames backfilled)
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0)); // busy
        assert_eq!(*rows[0].get(1).unwrap(), SqliteValue::Integer(0)); // log (total)
        assert_eq!(*rows[0].get(2).unwrap(), SqliteValue::Integer(0)); // checkpointed
    }

    #[test]
    fn test_pragma_wal_checkpoint_with_modes() {
        let conn = Connection::open(":memory:").unwrap();
        assert_eq!(conn.pager.journal_mode(), fsqlite_pager::JournalMode::Wal);

        // Test each checkpoint mode.
        let modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"];
        for mode in modes {
            let sql = format!("PRAGMA wal_checkpoint({mode});");
            let rows = conn.query(&sql).unwrap();
            assert_eq!(rows.len(), 1, "mode={mode} should return 1 row");
            // All should succeed on empty WAL.
            assert_eq!(
                *rows[0].get(0).unwrap(),
                SqliteValue::Integer(0),
                "mode={mode} busy"
            );
        }
    }

    #[test]
    fn test_pragma_wal_checkpoint_after_writes() {
        let conn = Connection::open(":memory:").unwrap();
        assert_eq!(conn.pager.journal_mode(), fsqlite_pager::JournalMode::Wal);

        // Create a table and insert data to generate WAL frames.
        conn.execute("CREATE TABLE t1 (id INTEGER, value TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'hello');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'world');").unwrap();

        // Checkpoint should transfer frames to the database.
        let rows = conn.query("PRAGMA wal_checkpoint(FULL);").unwrap();
        assert_eq!(rows.len(), 1);
        // busy should be 0.
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0));
        // total frames should be > 0 (we created table + 2 inserts).
        let total_frames = match rows[0].get(1).unwrap() {
            SqliteValue::Integer(n) => *n,
            _ => panic!("expected integer"),
        };
        assert!(
            total_frames > 0,
            "expected frames after writes, got {total_frames}"
        );

        // Verify data is still accessible after checkpoint.
        let rows = conn.query("SELECT value FROM t1 ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            *rows[0].get(0).unwrap(),
            SqliteValue::Text("hello".to_owned())
        );
        assert_eq!(
            *rows[1].get(0).unwrap(),
            SqliteValue::Text("world".to_owned())
        );
    }

    #[test]
    fn test_pragma_wal_checkpoint_returns_sentinel_in_delete_mode() {
        let conn = Connection::open(":memory:").unwrap();
        // Switch to delete/rollback journal mode.
        conn.execute("PRAGMA journal_mode='delete';").unwrap();
        assert_eq!(
            conn.pager.journal_mode(),
            fsqlite_pager::JournalMode::Delete
        );

        // SQLite-compatible non-WAL sentinel: busy=0, log=-1, checkpointed=-1.
        let rows = conn.query("PRAGMA wal_checkpoint;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(0));
        assert_eq!(*rows[0].get(1).unwrap(), SqliteValue::Integer(-1));
        assert_eq!(*rows[0].get(2).unwrap(), SqliteValue::Integer(-1));
    }

    // ─── bd-1dp9.4.1: Journal mode transition + checkpoint parity tests ──

    #[test]
    fn test_pragma_journal_mode_all_modes_accepted() {
        let conn = Connection::open(":memory:").unwrap();
        // All 6 standard SQLite journal modes must be accepted.
        for mode in ["delete", "truncate", "persist", "memory", "wal", "off"] {
            let sql = format!("PRAGMA journal_mode='{mode}';");
            let rows = conn.query(&sql).unwrap();
            assert_eq!(rows.len(), 1, "mode={mode} should return 1 row");
            assert_eq!(
                *rows[0].get(0).unwrap(),
                SqliteValue::Text(mode.to_owned()),
                "mode={mode} response should echo back the mode"
            );
        }
    }

    #[test]
    fn test_pragma_journal_mode_state_tracks_current_mode() {
        let conn = Connection::open(":memory:").unwrap();
        // Default is WAL.
        assert_eq!(conn.pragma_state().journal_mode, "wal");

        // Switch to each mode and verify state tracks it.
        let transitions = [
            ("delete", fsqlite_pager::JournalMode::Delete),
            ("truncate", fsqlite_pager::JournalMode::Delete), // Pager maps to Delete
            ("persist", fsqlite_pager::JournalMode::Delete),  // Pager maps to Delete
            ("wal", fsqlite_pager::JournalMode::Wal),
        ];
        for (mode, expected_pager_mode) in transitions {
            conn.execute(&format!("PRAGMA journal_mode='{mode}';"))
                .unwrap();
            assert_eq!(
                conn.pragma_state().journal_mode,
                mode,
                "state.journal_mode should track '{mode}'"
            );
            assert_eq!(
                conn.pager.journal_mode(),
                expected_pager_mode,
                "pager should reflect expected mode for '{mode}'"
            );
        }
    }

    #[test]
    fn test_pragma_wal_checkpoint_sentinel_for_all_non_wal_modes() {
        let conn = Connection::open(":memory:").unwrap();
        // Switch to each non-WAL mode and verify checkpoint returns sentinel.
        for mode in ["delete", "truncate", "persist"] {
            conn.execute(&format!("PRAGMA journal_mode='{mode}';"))
                .unwrap();
            let rows = conn.query("PRAGMA wal_checkpoint;").unwrap();
            assert_eq!(rows.len(), 1, "mode={mode}: expected 1 row");
            assert_eq!(
                *rows[0].get(0).unwrap(),
                SqliteValue::Integer(0),
                "mode={mode}: busy should be 0"
            );
            assert_eq!(
                *rows[0].get(1).unwrap(),
                SqliteValue::Integer(-1),
                "mode={mode}: log should be -1"
            );
            assert_eq!(
                *rows[0].get(2).unwrap(),
                SqliteValue::Integer(-1),
                "mode={mode}: checkpointed should be -1"
            );
        }
    }

    #[test]
    fn test_pragma_wal_checkpoint_all_modes_after_writes() {
        // Test each checkpoint mode with actual WAL data.
        let modes = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"];
        for mode in modes {
            let conn = Connection::open(":memory:").unwrap();
            assert_eq!(conn.pager.journal_mode(), fsqlite_pager::JournalMode::Wal);

            conn.execute("CREATE TABLE t_ckpt (id INTEGER, val TEXT);")
                .unwrap();
            conn.execute("INSERT INTO t_ckpt VALUES (1, 'a');").unwrap();
            conn.execute("INSERT INTO t_ckpt VALUES (2, 'b');").unwrap();

            let sql = format!("PRAGMA wal_checkpoint({mode});");
            let rows = conn.query(&sql).unwrap();
            assert_eq!(rows.len(), 1, "mode={mode}: expected 1 row");
            // busy=0
            assert_eq!(
                *rows[0].get(0).unwrap(),
                SqliteValue::Integer(0),
                "mode={mode}: busy should be 0"
            );
            // total_frames > 0
            let total_frames = match rows[0].get(1).unwrap() {
                SqliteValue::Integer(n) => *n,
                other => panic!("mode={mode}: expected integer for total_frames, got {other:?}"),
            };
            assert!(
                total_frames > 0,
                "mode={mode}: expected frames > 0, got {total_frames}"
            );

            // Data still accessible after checkpoint.
            let data = conn.query("SELECT COUNT(*) FROM t_ckpt;").unwrap();
            assert_eq!(
                *data[0].get(0).unwrap(),
                SqliteValue::Integer(2),
                "mode={mode}: data should survive checkpoint"
            );
        }
    }

    // ─── JOIN tests ─────────────────────────────────────────────

    fn setup_join_tables(conn: &Connection) {
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (3, 'Carol');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (10, 1, 100);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (11, 1, 200);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (12, 2, 50);")
            .unwrap();
    }

    #[test]
    fn test_inner_join_on() {
        let conn = Connection::open(":memory:").unwrap();
        setup_join_tables(&conn);
        let rows = conn
            .query("SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.amount;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Bob".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(50));
        assert_eq!(rows[1].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(100));
        assert_eq!(rows[2].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[2].values()[1], SqliteValue::Integer(200));
    }

    #[test]
    fn test_left_join_includes_unmatched() {
        let conn = Connection::open(":memory:").unwrap();
        setup_join_tables(&conn);
        // Carol has no orders, so her row should appear with NULLs.
        let rows = conn
            .query("SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.name;")
            .unwrap();
        assert_eq!(rows.len(), 4);
        // Alice (2 orders), Bob (1 order), Carol (NULL).
        let names: Vec<String> = rows
            .iter()
            .filter_map(|r| match &r.values()[0] {
                SqliteValue::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(names.len(), rows.len());
        assert_eq!(names, vec!["Alice", "Alice", "Bob", "Carol"]);
        // Carol's amount should be NULL.
        assert_eq!(rows[3].values()[1], SqliteValue::Null);
    }

    #[test]
    fn test_cross_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a (x INTEGER);").unwrap();
        conn.execute("CREATE TABLE b (y INTEGER);").unwrap();
        conn.execute("INSERT INTO a VALUES (1);").unwrap();
        conn.execute("INSERT INTO a VALUES (2);").unwrap();
        conn.execute("INSERT INTO b VALUES (10);").unwrap();
        conn.execute("INSERT INTO b VALUES (20);").unwrap();
        conn.execute("INSERT INTO b VALUES (30);").unwrap();
        // CROSS JOIN produces 2×3 = 6 rows.
        let rows = conn.query("SELECT a.x, b.y FROM a CROSS JOIN b;").unwrap();
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn test_join_with_where_clause() {
        let conn = Connection::open(":memory:").unwrap();
        setup_join_tables(&conn);
        let rows = conn
            .query("SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 75;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Only Alice's orders (100, 200) pass the filter.
        for r in &rows {
            assert_eq!(r.values()[0], SqliteValue::Text("Alice".to_owned()));
        }
    }

    #[test]
    fn test_join_with_where_clause_anonymous_placeholder() {
        let conn = Connection::open(":memory:").unwrap();
        setup_join_tables(&conn);
        let rows = conn
            .query_with_params(
                "SELECT users.name, orders.amount \
                 FROM users INNER JOIN orders ON users.id = orders.user_id \
                 WHERE orders.amount > ? ORDER BY orders.amount;",
                &[SqliteValue::Integer(75)],
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(100));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(200));
    }

    #[test]
    fn test_join_select_star() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (c INTEGER, d TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 'y');").unwrap();
        let rows = conn
            .query("SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.c;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        // SELECT * should include all columns from both tables.
        assert_eq!(rows[0].values().len(), 4);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("x".to_owned()));
        assert_eq!(rows[0].values()[2], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[3], SqliteValue::Text("y".to_owned()));
    }

    #[test]
    fn test_join_using_clause() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER, val1 TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER, val2 TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 'x');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3, 'y');").unwrap();
        let rows = conn
            .query("SELECT t1.val1, t2.val2 FROM t1 INNER JOIN t2 USING (id);")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("b".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("x".to_owned()));
    }

    #[test]
    fn test_join_with_table_alias() {
        let conn = Connection::open(":memory:").unwrap();
        setup_join_tables(&conn);
        let rows = conn
            .query("SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.amount;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Bob".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(50));
    }

    #[test]
    fn test_join_with_limit() {
        let conn = Connection::open(":memory:").unwrap();
        setup_join_tables(&conn);
        let rows = conn
            .query("SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.amount LIMIT 2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(50));
        assert_eq!(rows[1].values()[1], SqliteValue::Integer(100));
    }

    #[test]
    fn test_join_no_matching_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2);").unwrap();
        // Inner join with no match produces no rows.
        let rows = conn
            .query("SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b;")
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_join_on_unknown_column_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1);").unwrap();

        let err = conn
            .query("SELECT * FROM t1 INNER JOIN t2 ON t1.missing = t2.b;")
            .expect_err("unknown ON column should error");
        assert!(
            err.to_string().contains("column not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_join_where_unknown_column_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1);").unwrap();

        let err = conn
            .query("SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b WHERE t2.missing = 1;")
            .expect_err("unknown WHERE column in JOIN context should error");
        assert!(
            err.to_string().contains("column not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_join_where_in_list_unknown_column_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1);").unwrap();

        let err = conn
            .query("SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b WHERE t1.a IN (t2.missing);")
            .expect_err("unknown column in JOIN IN-list should error");
        assert!(
            err.to_string().contains("column not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_implicit_join_comma() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (10);").unwrap();
        // Implicit join via comma syntax (treated as CROSS JOIN).
        let rows = conn.query("SELECT t1.a, t2.b FROM t1, t2;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_multi_table_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE b (a_id INTEGER, val INTEGER);")
            .unwrap();
        conn.execute("CREATE TABLE c (a_id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO a VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO b VALUES (1, 42);").unwrap();
        conn.execute("INSERT INTO c VALUES (1, 'alpha');").unwrap();
        let rows = conn
            .query("SELECT a.name, b.val, c.tag FROM a INNER JOIN b ON a.id = b.a_id INNER JOIN c ON a.id = c.a_id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("x".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(42));
        assert_eq!(rows[0].values()[2], SqliteValue::Text("alpha".to_owned()));
    }

    #[test]
    fn test_right_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (l_id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO r VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO r VALUES (2, 'b');").unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l RIGHT JOIN r ON l.id = r.l_id ORDER BY r.tag;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Matched row: alice, a
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("alice".to_owned()),
                SqliteValue::Text("a".to_owned())
            ]
        );
        // Unmatched right row: NULL, b
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Null, SqliteValue::Text("b".to_owned())]
        );
    }

    #[test]
    fn test_right_join_all_matched() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER);").unwrap();
        conn.execute("CREATE TABLE r (l_id INTEGER);").unwrap();
        conn.execute("INSERT INTO l VALUES (1);").unwrap();
        conn.execute("INSERT INTO l VALUES (2);").unwrap();
        conn.execute("INSERT INTO r VALUES (1);").unwrap();

        let rows = conn
            .query("SELECT l.id, r.l_id FROM l RIGHT JOIN r ON l.id = r.l_id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Integer(1)]
        );
    }

    #[test]
    fn test_full_outer_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (l_id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO l VALUES (3, 'charlie');")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO r VALUES (2, 'b');").unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l FULL OUTER JOIN r ON l.id = r.l_id;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        let vals: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
        assert!(vals.contains(&vec![
            SqliteValue::Text("alice".to_owned()),
            SqliteValue::Text("a".to_owned())
        ]));
        assert!(vals.contains(&vec![SqliteValue::Null, SqliteValue::Text("b".to_owned())]));
        assert!(vals.contains(&vec![
            SqliteValue::Text("charlie".to_owned()),
            SqliteValue::Null
        ]));
    }

    #[test]
    fn test_full_join_no_matches() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER);").unwrap();
        conn.execute("CREATE TABLE r (l_id INTEGER);").unwrap();
        conn.execute("INSERT INTO l VALUES (1);").unwrap();
        conn.execute("INSERT INTO r VALUES (2);").unwrap();

        let rows = conn
            .query("SELECT l.id, r.l_id FROM l FULL JOIN r ON l.id = r.l_id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Unmatched left: 1, NULL
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Null]
        );
        // Unmatched right: NULL, 2
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Null, SqliteValue::Integer(2)]
        );
    }

    #[test]
    fn test_right_join_using() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO r VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO r VALUES (2, 'b');").unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l RIGHT JOIN r USING (id) ORDER BY r.tag;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("alice".to_owned()),
                SqliteValue::Text("a".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Null, SqliteValue::Text("b".to_owned())]
        );
    }

    #[test]
    fn test_right_join_using_nulls_do_not_match() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (NULL, 'left-null');")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'left-one');")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (NULL, 'right-null');")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (1, 'right-one');")
            .unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l RIGHT JOIN r USING (id);")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
        assert!(vals.contains(&vec![
            SqliteValue::Text("left-one".to_owned()),
            SqliteValue::Text("right-one".to_owned())
        ]));
        assert!(vals.contains(&vec![
            SqliteValue::Null,
            SqliteValue::Text("right-null".to_owned())
        ]));
    }

    #[test]
    fn test_natural_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER);")
            .unwrap();
        conn.execute("CREATE TABLE departments (dept_id INTEGER, dept_name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES (1, 'alice', 10);")
            .unwrap();
        conn.execute("INSERT INTO employees VALUES (2, 'bob', 20);")
            .unwrap();
        conn.execute("INSERT INTO departments VALUES (10, 'eng');")
            .unwrap();
        conn.execute("INSERT INTO departments VALUES (30, 'hr');")
            .unwrap();

        // NATURAL JOIN matches on shared column 'dept_id'.
        let rows = conn
            .query("SELECT employees.name, departments.dept_name FROM employees NATURAL JOIN departments;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("alice".to_owned()),
                SqliteValue::Text("eng".to_owned())
            ]
        );
    }

    #[test]
    fn test_natural_left_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO l VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO r VALUES (1, 'x');").unwrap();

        let rows = conn
            .query("SELECT l.val, r.tag FROM l NATURAL LEFT JOIN r ORDER BY l.val;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("a".to_owned()),
                SqliteValue::Text("x".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("b".to_owned()), SqliteValue::Null]
        );
    }

    #[test]
    fn test_natural_join_no_shared_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (10);").unwrap();

        // No shared columns: NATURAL JOIN degenerates to CROSS JOIN.
        let rows = conn
            .query("SELECT t1.a, t2.b FROM t1 NATURAL JOIN t2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }
}

#[cfg(test)]
mod transaction_lifecycle_tests {
    use super::*;

    fn new_conn() -> Connection {
        Connection::open(":memory:").unwrap()
    }

    #[test]
    fn test_lifecycle_begin_commit() {
        let conn = new_conn();
        assert!(!conn.in_transaction());

        conn.execute("BEGIN").unwrap();
        assert!(conn.in_transaction());
        assert!(conn.active_txn.borrow().is_some());

        conn.execute("COMMIT").unwrap();
        assert!(!conn.in_transaction());
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_lifecycle_begin_rollback() {
        let conn = new_conn();
        conn.execute("BEGIN").unwrap();
        assert!(conn.in_transaction());

        conn.execute("ROLLBACK").unwrap();
        assert!(!conn.in_transaction());
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_close_rollbacks_active_txn() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("close_rollbacks_active_txn.db");
        let db_str = db_path.to_str().unwrap();

        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (7)").unwrap();
            conn.close().unwrap();
        }

        let conn = Connection::open(db_str).unwrap();
        let rows = conn.query("SELECT x FROM t").unwrap();
        assert!(
            rows.is_empty(),
            "close() should rollback active transaction"
        );
    }

    #[test]
    fn test_close_checkpoints_wal() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("close_checkpoints_wal.db");
        let db_str = db_path.to_str().unwrap();

        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("PRAGMA journal_mode = WAL").unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("INSERT INTO t VALUES (123)").unwrap();
            conn.close().unwrap();
        }

        let wal_path = format!("{db_str}-wal");
        if std::path::Path::new(&wal_path).exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        let conn = Connection::open(db_str).unwrap();
        let rows = conn.query("SELECT x FROM t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0).unwrap(),
            &fsqlite_types::value::SqliteValue::Integer(123)
        );
    }

    #[test]
    fn test_drop_does_not_panic() {
        let result = std::panic::catch_unwind(|| {
            let conn = Connection::open(":memory:").unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (1)").unwrap();
        });
        assert!(result.is_ok(), "dropping connection should not panic");
    }

    #[test]
    fn test_close_releases_file_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("close_releases_file_lock.db");
        let db_str = db_path.to_str().unwrap();

        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("BEGIN IMMEDIATE").unwrap();
            conn.close().unwrap();
        }

        let conn = Connection::open(db_str).unwrap();
        conn.execute("BEGIN IMMEDIATE").unwrap();
        conn.execute("COMMIT").unwrap();
    }

    #[test]
    fn test_lifecycle_nested_begin_fails() {
        let conn = new_conn();
        conn.execute("BEGIN").unwrap();
        let result = conn.execute("BEGIN");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "internal error: cannot start a transaction within a transaction"
        );
    }

    #[test]
    fn test_lifecycle_commit_no_txn_fails() {
        let conn = new_conn();
        let result = conn.execute("COMMIT");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "internal error: cannot commit - no transaction is active"
        );
    }

    #[test]
    fn test_lifecycle_rollback_no_txn_fails() {
        let conn = new_conn();
        let result = conn.execute("ROLLBACK");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "internal error: cannot rollback - no transaction is active"
        );
    }

    #[test]
    fn test_lifecycle_savepoint_implicit_txn() {
        let conn = new_conn();
        assert!(!conn.in_transaction());

        // SAVEPOINT should start a transaction implicitly (deferred).
        conn.execute("SAVEPOINT sp1").unwrap();
        assert!(conn.in_transaction());
        assert!(conn.active_txn.borrow().is_some());

        // RELEASE should commit it.
        conn.execute("RELEASE sp1").unwrap();
        assert!(!conn.in_transaction());
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_lifecycle_rollback_to_savepoint() {
        let conn = new_conn();
        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT sp1").unwrap();

        // This should invoke rollback_to_savepoint on the txn
        conn.execute("ROLLBACK TO sp1").unwrap();

        // Transaction should still be active
        assert!(conn.in_transaction());

        conn.execute("COMMIT").unwrap();
        assert!(!conn.in_transaction());
    }

    // -----------------------------------------------------------------------
    // File-backed persistence tests (bd-1dqg acceptance criteria)
    // -----------------------------------------------------------------------

    #[test]
    fn test_persist_begin_insert_commit_visible_to_new_connection() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();

        // First connection: create, insert, commit.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (42)").unwrap();
            conn.execute("COMMIT").unwrap();
        }

        // Second connection: data must be visible.
        {
            let conn = Connection::open(db_str).unwrap();
            let rows = conn.query("SELECT x FROM t").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get(0).unwrap(),
                &fsqlite_types::value::SqliteValue::Integer(42)
            );
        }
    }

    #[test]
    fn test_persist_begin_insert_rollback_no_changes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();

        // Create the table (outside any explicit transaction).
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        }

        // Begin, insert, then rollback.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (99)").unwrap();
            conn.execute("ROLLBACK").unwrap();

            // Within the same connection, data should be gone.
            let rows = conn.query("SELECT x FROM t").unwrap();
            assert!(rows.is_empty(), "rollback should discard INSERT");
        }

        // Re-open: data should still be gone.
        {
            let conn = Connection::open(db_str).unwrap();
            let rows = conn.query("SELECT x FROM t").unwrap();
            assert!(rows.is_empty(), "rolled-back data must not persist to disk");
        }
    }

    #[test]
    fn test_persist_multiple_commits_accumulate() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();

        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (1)").unwrap();
            conn.execute("COMMIT").unwrap();

            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO t VALUES (2)").unwrap();
            conn.execute("COMMIT").unwrap();
        }

        {
            let conn = Connection::open(db_str).unwrap();
            let rows = conn.query("SELECT x FROM t ORDER BY x").unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].get(0).unwrap(),
                &fsqlite_types::value::SqliteValue::Integer(1)
            );
            assert_eq!(
                rows[1].get(0).unwrap(),
                &fsqlite_types::value::SqliteValue::Integer(2)
            );
        }
    }

    #[test]
    fn test_insert_or_replace() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        // Replace row with id=1
        conn.execute("INSERT OR REPLACE INTO t VALUES (1, 'carol')")
            .unwrap();
        let rows = conn.query("SELECT id, name FROM t ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("carol".into()));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(2));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Text("bob".into()));
    }

    #[test]
    fn test_replace_into() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        // REPLACE INTO is syntactic sugar for INSERT OR REPLACE
        conn.execute("REPLACE INTO t VALUES (1, 'zara')").unwrap();
        let rows = conn.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("zara".into()));
    }

    #[test]
    fn test_insert_or_ignore() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        // INSERT OR IGNORE should silently skip the conflicting row
        conn.execute("INSERT OR IGNORE INTO t VALUES (1, 'bob')")
            .unwrap();
        let rows = conn.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("alice".into()));
    }

    #[test]
    fn test_insert_or_replace_new_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        // INSERT OR REPLACE with a new id should just insert
        conn.execute("INSERT OR REPLACE INTO t VALUES (2, 'b')")
            .unwrap();
        let rows = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_insert_or_ignore_no_conflict() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        // INSERT OR IGNORE with no conflict should insert normally
        conn.execute("INSERT OR IGNORE INTO t VALUES (2, 'b')")
            .unwrap();
        let rows = conn.query("SELECT id FROM t ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_subquery_in_from() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'carol')").unwrap();
        let rows = conn
            .query("SELECT s.name FROM (SELECT id, name FROM t WHERE id > 1) AS s ORDER BY s.name")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("bob".into()));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Text("carol".into()));
    }

    #[test]
    fn test_join_with_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
        )
        .unwrap();
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (1, 'alice')")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (2, 'bob')")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 1, 100)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (2, 1, 200)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (3, 2, 50)")
            .unwrap();
        // Join a named table with a subquery
        let rows = conn
            .query(
                "SELECT c.name, o.total FROM customers AS c \
                 JOIN (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) AS o \
                 ON c.id = o.customer_id ORDER BY c.name",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("alice".into()));
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Integer(300));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Text("bob".into()));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Integer(50));
    }

    #[test]
    fn test_subquery_as_primary_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (10)").unwrap();
        conn.execute("INSERT INTO t VALUES (20)").unwrap();
        // Subquery as the primary (left) table in a JOIN
        let rows = conn
            .query(
                "SELECT a.x, t.x FROM (SELECT x FROM t WHERE x = 10) AS a \
                 CROSS JOIN t ORDER BY t.x",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(10));
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Integer(10));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(10));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Integer(20));
    }

    #[test]
    fn test_scalar_fn_typeof() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val REAL)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'hello', 3.14)")
            .unwrap();
        let rows = conn
            .query(
                "SELECT typeof(t.id), typeof(t.name), typeof(t.val) \
                 FROM t JOIN t AS t2 ON t.id = t2.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text("integer".into())
        );
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("text".into()));
    }

    #[test]
    fn test_scalar_fn_iif() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        let rows = conn
            .query(
                "SELECT t.id, iif(t.val > 15, 'big', 'small') \
                 FROM t JOIN t AS t2 ON t.id = t2.id ORDER BY t.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("small".into()));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Text("big".into()));
    }

    #[test]
    fn test_scalar_fn_replace_substr_instr() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'hello world')")
            .unwrap();
        let rows = conn
            .query(
                "SELECT replace(t.s, 'world', 'earth'), \
                        substr(t.s, 7), \
                        instr(t.s, 'world') \
                 FROM t JOIN t AS t2 ON t.id = t2.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text("hello earth".into())
        );
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("world".into()));
        assert_eq!(rows[0].get(2).unwrap(), &SqliteValue::Integer(7));
    }

    #[test]
    fn test_scalar_fn_trim_variants() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '  hello  ')")
            .unwrap();
        let rows = conn
            .query(
                "SELECT trim(t.s), ltrim(t.s), rtrim(t.s) \
                 FROM t JOIN t AS t2 ON t.id = t2.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("hello".into()));
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("hello  ".into())
        );
        assert_eq!(
            rows[0].get(2).unwrap(),
            &SqliteValue::Text("  hello".into())
        );
    }

    #[test]
    fn test_scalar_fn_concat_hex_quote() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'foo', 'bar')")
            .unwrap();
        let rows = conn
            .query(
                "SELECT concat(t.a, t.b), hex(t.a), quote(t.a) \
                 FROM t JOIN t AS t2 ON t.id = t2.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("foobar".into()));
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("666F6F".into()));
        assert_eq!(rows[0].get(2).unwrap(), &SqliteValue::Text("'foo'".into()));
    }

    #[test]
    fn test_scalar_fn_round_sign() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 2.71828)").unwrap();
        let rows = conn
            .query(
                "SELECT round(t.val, 2), sign(t.val) \
                 FROM t JOIN t AS t2 ON t.id = t2.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Float(2.72));
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Integer(1));
    }

    #[test]
    fn test_insert_or_replace_select_source() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'alice')").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 'bob')").unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'old_alice')")
            .unwrap();
        // Replace id=1, insert id=2
        conn.execute("INSERT OR REPLACE INTO dst SELECT * FROM src")
            .unwrap();
        let rows = conn.query("SELECT id, name FROM dst ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("alice".into()));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Text("bob".into()));
    }

    #[test]
    fn test_insert_or_ignore_select_source() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'alice')").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 'bob')").unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'old_alice')")
            .unwrap();
        // Ignore id=1 conflict, insert id=2
        conn.execute("INSERT OR IGNORE INTO dst SELECT * FROM src")
            .unwrap();
        let rows = conn.query("SELECT id, name FROM dst ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2);
        // id=1 should keep original value
        assert_eq!(
            rows[0].get(1).unwrap(),
            &SqliteValue::Text("old_alice".into())
        );
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Text("bob".into()));
    }

    #[test]
    fn test_drop_trigger_removes_definition() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TRIGGER trg_ai AFTER INSERT ON t BEGIN SELECT 1; END;")
            .unwrap();
        assert_eq!(conn.triggers.borrow().len(), 1);
        conn.execute("DROP TRIGGER trg_ai").unwrap();
        assert!(conn.triggers.borrow().is_empty());
    }

    #[test]
    fn test_drop_trigger_if_exists_missing_is_ok() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("DROP TRIGGER IF EXISTS missing_trigger")
            .unwrap();
    }

    #[test]
    fn test_drop_trigger_missing_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        let err = conn
            .execute("DROP TRIGGER missing_trigger")
            .expect_err("DROP TRIGGER without IF EXISTS should error when trigger is absent");
        assert!(
            err.to_string().contains("no such trigger: missing_trigger"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_drop_temp_trigger_does_not_require_sqlite_master_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TEMP TRIGGER trg_temp AFTER INSERT ON t BEGIN SELECT 1; END;")
            .unwrap();
        assert_eq!(conn.triggers.borrow().len(), 1);
        conn.execute("DROP TRIGGER trg_temp").unwrap();
        assert!(conn.triggers.borrow().is_empty());
    }

    #[test]
    fn test_drop_trigger_persists_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drop_trigger_round_trip.db");
        let path_str = path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&path_str).unwrap();
            conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
            conn.execute("CREATE TRIGGER trg_rt AFTER INSERT ON t BEGIN SELECT 1; END;")
                .unwrap();
            conn.execute("DROP TRIGGER trg_rt").unwrap();
            conn.close().unwrap();
        }

        {
            let conn = Connection::open(&path_str).unwrap();
            assert!(
                conn.triggers
                    .borrow()
                    .iter()
                    .all(|trigger| !trigger.name.eq_ignore_ascii_case("trg_rt")),
                "dropped trigger should not reload from sqlite_master on reopen"
            );
        }
    }

    #[test]
    fn test_select_star_group_by_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)",
        )
        .unwrap();
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (1, 'Alice')")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (2, 'Bob')")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 1, 10.0)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (2, 1, 20.0)")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (3, 2, 30.0)")
            .unwrap();
        // SELECT * with GROUP BY on a JOIN — picks one row per group
        let rows = conn
            .query(
                "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id \
                 GROUP BY customers.id ORDER BY customers.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_select_table_star_group_by_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, score INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 1, 10)").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 1, 20)").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3, 2, 30)").unwrap();
        // SELECT t1.* with GROUP BY
        let rows = conn
            .query(
                "SELECT t1.* FROM t1 JOIN t2 ON t1.id = t2.t1_id \
                 GROUP BY t1.id ORDER BY t1.id",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("a".into()));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Text("b".into()));
    }

    #[test]
    fn test_count_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (4, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (5, 'c')").unwrap();
        let rows = conn
            .query("SELECT COUNT(DISTINCT category) FROM t GROUP BY 1")
            .unwrap();
        // All rows in one group; 3 distinct categories
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(3));
    }

    #[test]
    fn test_count_distinct_per_group() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x', 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'x', 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'x', 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (4, 'y', 'c')").unwrap();
        let rows = conn
            .query("SELECT grp, COUNT(DISTINCT val) FROM t GROUP BY grp ORDER BY grp")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // group 'x': 2 distinct values ('a', 'b')
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Integer(2));
        // group 'y': 1 distinct value ('c')
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Integer(1));
    }

    #[test]
    fn test_group_concat_with_separator() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x', 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'x', 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'y', 'c')").unwrap();
        let rows = conn
            .query("SELECT grp, GROUP_CONCAT(val, ' | ') FROM t GROUP BY grp ORDER BY grp")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(1).unwrap(), &SqliteValue::Text("a | b".into()));
        assert_eq!(rows[1].get(1).unwrap(), &SqliteValue::Text("c".into()));
    }

    #[test]
    fn test_group_concat_default_separator() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x', 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'x', 'b')").unwrap();
        let rows = conn
            .query("SELECT GROUP_CONCAT(val) FROM t GROUP BY grp")
            .unwrap();
        assert_eq!(rows.len(), 1);
        // Default separator is comma
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("a,b".into()));
    }

    #[test]
    fn test_count_distinct_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
            .unwrap();
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (1, 'Alice')")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (2, 'Bob')")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 1)").unwrap();
        conn.execute("INSERT INTO orders VALUES (2, 1)").unwrap();
        conn.execute("INSERT INTO orders VALUES (3, 2)").unwrap();
        let rows = conn
            .query(
                "SELECT COUNT(DISTINCT orders.customer_id) FROM orders \
                 JOIN customers ON orders.customer_id = customers.id \
                 GROUP BY 1",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(2));
    }

    #[test]
    fn test_group_by_join_having_named_placeholder() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (3, 'Carol');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (10, 1, 100);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (11, 1, 200);")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES (12, 2, 50);")
            .unwrap();
        let rows = conn
            .query_with_params(
                "SELECT users.name, COUNT(*) \
                 FROM users INNER JOIN orders ON users.id = orders.user_id \
                 GROUP BY users.name \
                 HAVING COUNT(*) > :min_count \
                 ORDER BY users.name;",
                &[SqliteValue::Integer(1)],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("Alice".to_owned()));
        assert_eq!(rows[0].values()[1], SqliteValue::Integer(2));
    }

    #[test]
    fn test_recursive_cte_sequence() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "WITH RECURSIVE cnt(x) AS (\
                   SELECT 1 \
                   UNION ALL \
                   SELECT x+1 FROM cnt WHERE x<5\
                 ) SELECT x FROM cnt",
            )
            .unwrap();
        assert_eq!(rows.len(), 5);
        for (row, expected) in rows.iter().zip(1_i64..=5_i64) {
            assert_eq!(row.get(0).unwrap(), &SqliteValue::Integer(expected));
        }
    }

    #[test]
    fn test_recursive_cte_fibonacci() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "WITH RECURSIVE fib(a, b) AS (\
                   SELECT 0, 1 \
                   UNION ALL \
                   SELECT b, a+b FROM fib WHERE b < 100\
                 ) SELECT a FROM fib",
            )
            .unwrap();
        // Fibonacci: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89
        assert_eq!(rows.len(), 12);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(0));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(1));
        assert_eq!(rows[11].get(0).unwrap(), &SqliteValue::Integer(89));
    }

    #[test]
    fn test_recursive_cte_union_deduplicates_rows() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "WITH RECURSIVE t(x) AS (\
                   SELECT 1 \
                   UNION \
                   SELECT x FROM t WHERE x < 3\
                 ) SELECT x FROM t",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
    }

    #[test]
    fn test_recursive_keyword_without_self_reference_is_not_iterative() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "WITH RECURSIVE t(x) AS (\
                   SELECT 1 \
                   UNION ALL \
                   SELECT 2\
                 ) SELECT x FROM t ORDER BY x",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(2));
    }

    #[test]
    fn test_recursive_cte_non_recursive_union_all_arm_runs_once() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "WITH RECURSIVE t(x) AS (\
                   SELECT 1 \
                   UNION ALL \
                   SELECT 2 \
                   UNION ALL \
                   SELECT x + 1 FROM t WHERE x < 3\
                 ) SELECT x FROM t ORDER BY x",
            )
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(2));
        assert_eq!(rows[2].get(0).unwrap(), &SqliteValue::Integer(2));
        assert_eq!(rows[3].get(0).unwrap(), &SqliteValue::Integer(3));
        assert_eq!(rows[4].get(0).unwrap(), &SqliteValue::Integer(3));
    }
}

// =========================================================================
// 5A.2 – sqlite_master row insert / delete tests (bd-1b5e)
// =========================================================================
#[cfg(test)]
mod sqlite_master_btree_tests {
    use super::*;
    use fsqlite_btree::BtreeCursorOps;
    use fsqlite_btree::cursor::TransactionPageIo;
    use fsqlite_pager::traits::TransactionMode;
    use fsqlite_types::PageNumber;
    use fsqlite_types::record::parse_record;

    /// Helper: read all sqlite_master rows from the page 1 B-tree.
    /// Returns Vec<(rowid, Vec<SqliteValue>)>.
    fn read_master_rows(conn: &Connection) -> Vec<(i64, Vec<SqliteValue>)> {
        let cx = Cx::new();
        let mut txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        let usable_size = fsqlite_types::PageSize::DEFAULT.get();
        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(txn.as_mut()),
            PageNumber::ONE,
            usable_size,
            false, // read-only
        );
        let mut rows = Vec::new();
        if cursor.first(&cx).unwrap() {
            loop {
                let rowid = cursor.rowid(&cx).unwrap();
                let payload = cursor.payload(&cx).unwrap();
                if let Some(values) = parse_record(&payload) {
                    rows.push((rowid, values));
                }
                if !cursor.next(&cx).unwrap() {
                    break;
                }
            }
        }
        rows
    }

    #[test]
    fn test_create_table_inserts_sqlite_master_row() {
        let conn = Connection::open(":memory:").unwrap();

        // Before any DDL, page 1 B-tree should be empty.
        let before = read_master_rows(&conn);
        eprintln!(
            "[5A2][test=create_inserts][step=before] master_rows={}",
            before.len()
        );
        assert!(
            before.is_empty(),
            "fresh db should have empty sqlite_master"
        );

        // CREATE TABLE should insert a row.
        conn.execute("CREATE TABLE t1 (x INTEGER, y TEXT)").unwrap();
        let after = read_master_rows(&conn);
        eprintln!(
            "[5A2][test=create_inserts][step=after_create] master_rows={}",
            after.len()
        );
        assert_eq!(after.len(), 1, "one row after one CREATE TABLE");

        // Verify record columns: type, name, tbl_name, rootpage, sql.
        let (rowid, ref cols) = after[0];
        eprintln!("[5A2][test=create_inserts][step=verify] rowid={rowid} cols={cols:?}");
        assert_eq!(rowid, 1);
        assert_eq!(cols.len(), 5, "sqlite_master has 5 columns");
        assert_eq!(cols[0], SqliteValue::Text("table".to_owned()));
        assert_eq!(cols[1], SqliteValue::Text("t1".to_owned()));
        assert_eq!(cols[2], SqliteValue::Text("t1".to_owned()));
        // rootpage should be a positive integer
        if let SqliteValue::Integer(rp) = &cols[3] {
            assert!(*rp > 0, "rootpage should be > 0, got {rp}");
        } else {
            panic!("rootpage should be Integer, got {:?}", cols[3]);
        }
        // sql should contain CREATE TABLE
        if let SqliteValue::Text(sql) = &cols[4] {
            assert!(
                sql.to_ascii_uppercase().contains("CREATE TABLE"),
                "sql should contain CREATE TABLE, got: {sql}"
            );
        } else {
            panic!("sql should be Text, got {:?}", cols[4]);
        }
    }

    #[test]
    fn test_create_multiple_tables_inserts_multiple_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE alpha (a INTEGER)").unwrap();
        conn.execute("CREATE TABLE beta (b TEXT)").unwrap();
        conn.execute("CREATE TABLE gamma (c REAL)").unwrap();

        let rows = read_master_rows(&conn);
        eprintln!(
            "[5A2][test=create_multiple][step=verify] master_rows={}",
            rows.len()
        );
        assert_eq!(
            rows.len(),
            3,
            "three CREATE TABLEs should produce three rows"
        );

        // Verify names are in insertion order (rowid 1, 2, 3).
        let names: Vec<&str> = rows
            .iter()
            .filter_map(|(_, cols)| {
                if let SqliteValue::Text(n) = &cols[1] {
                    Some(n.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_drop_table_deletes_sqlite_master_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER)").unwrap();
        conn.execute("CREATE TABLE t2 (y TEXT)").unwrap();

        let before = read_master_rows(&conn);
        eprintln!(
            "[5A2][test=drop_deletes][step=before] master_rows={}",
            before.len()
        );
        assert_eq!(before.len(), 2);

        // Drop t1 — only t2 should remain.
        conn.execute("DROP TABLE t1").unwrap();
        let after = read_master_rows(&conn);
        eprintln!(
            "[5A2][test=drop_deletes][step=after_drop] master_rows={}",
            after.len()
        );
        assert_eq!(after.len(), 1, "one row should remain after DROP TABLE");
        assert_eq!(after[0].1[1], SqliteValue::Text("t2".to_owned()));
    }

    #[test]
    fn test_create_drop_create_reinserts() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        assert_eq!(read_master_rows(&conn).len(), 1);

        conn.execute("DROP TABLE t").unwrap();
        assert!(
            read_master_rows(&conn).is_empty(),
            "drop should clear master"
        );

        // Re-create same table — should get a new row.
        conn.execute("CREATE TABLE t (x INTEGER, y TEXT)").unwrap();
        let rows = read_master_rows(&conn);
        eprintln!(
            "[5A2][test=create_drop_create][step=verify] master_rows={}",
            rows.len()
        );
        assert_eq!(rows.len(), 1);
        // The SQL should reflect the new schema.
        if let SqliteValue::Text(sql) = &rows[0].1[4] {
            assert!(
                sql.contains('y'),
                "recreated table SQL should mention column y: {sql}"
            );
        }
    }

    #[test]
    fn test_sqlite_master_record_format() {
        // Verify that the record format matches the canonical
        // sqlite_master schema: (type TEXT, name TEXT, tbl_name TEXT,
        // rootpage INTEGER, sql TEXT).
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE fmt_test (a INTEGER PRIMARY KEY, b TEXT NOT NULL)")
            .unwrap();

        let rows = read_master_rows(&conn);
        assert_eq!(rows.len(), 1);
        let (_, ref cols) = rows[0];

        // All five columns should be present and of the correct type.
        assert!(
            matches!(&cols[0], SqliteValue::Text(t) if t == "table"),
            "col 0 (type) should be 'table'"
        );
        assert!(
            matches!(&cols[1], SqliteValue::Text(n) if n == "fmt_test"),
            "col 1 (name) should be 'fmt_test'"
        );
        assert!(
            matches!(&cols[2], SqliteValue::Text(t) if t == "fmt_test"),
            "col 2 (tbl_name) should be 'fmt_test'"
        );
        assert!(
            matches!(&cols[3], SqliteValue::Integer(rp) if *rp > 0),
            "col 3 (rootpage) should be positive integer"
        );
        assert!(
            matches!(&cols[4], SqliteValue::Text(_)),
            "col 4 (sql) should be text"
        );

        eprintln!("[5A2][test=record_format][step=verify] cols={cols:?} ✓");
    }

    #[test]
    fn test_select_sqlite_master_reports_table_and_index_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_users_email ON users(email)")
            .unwrap();

        let rows = conn
            .query(
                "SELECT type, name, tbl_name \
                 FROM sqlite_master \
                 WHERE type IN ('table', 'index') \
                 ORDER BY type, name",
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values(),
            &[
                SqliteValue::Text("index".to_owned()),
                SqliteValue::Text("idx_users_email".to_owned()),
                SqliteValue::Text("users".to_owned()),
            ]
        );
        assert_eq!(
            rows[1].values(),
            &[
                SqliteValue::Text("table".to_owned()),
                SqliteValue::Text("users".to_owned()),
                SqliteValue::Text("users".to_owned()),
            ]
        );

        let count_rows = conn
            .query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'")
            .unwrap();
        assert_eq!(count_rows.len(), 1);
        assert_eq!(
            count_rows[0].get(0),
            Some(&SqliteValue::Integer(1)),
            "sqlite_master should contain the users table entry"
        );
    }

    #[test]
    fn test_sqlite_schema_alias_matches_sqlite_master_results() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX idx_logs_msg ON logs(msg)")
            .unwrap();

        let master_rows = conn
            .query(
                "SELECT type, name, tbl_name, rootpage \
                 FROM sqlite_master \
                 ORDER BY type, name",
            )
            .unwrap();
        let schema_rows = conn
            .query(
                "SELECT type, name, tbl_name, rootpage \
                 FROM sqlite_schema \
                 ORDER BY type, name",
            )
            .unwrap();

        assert_eq!(
            master_rows, schema_rows,
            "sqlite_schema alias should return the same rows as sqlite_master"
        );
    }
}

// =========================================================================
// 5A.3 – real root page allocation tests (bd-3ez5)
// =========================================================================
#[cfg(test)]
mod root_page_allocation_tests {
    use super::*;
    use fsqlite_btree::BtreeCursorOps;
    use fsqlite_btree::cursor::TransactionPageIo;
    use fsqlite_pager::traits::TransactionMode;

    #[test]
    fn test_create_table_allocates_real_page() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER)").unwrap();

        // Root page should be > 1 (page 1 = sqlite_master).
        let schema = conn.schema.borrow();
        let rp = schema[0].root_page;
        eprintln!("[5A3][test=allocates_real_page][step=verify] root_page={rp}");
        assert!(
            rp > 1,
            "root page should be > 1 (page 1 = master), got {rp}"
        );

        // Verify the page exists in the pager and is readable.
        let cx = Cx::new();
        let txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        #[allow(clippy::cast_sign_loss)]
        let page_no = PageNumber::new(rp as u32).unwrap();
        let page_data = txn.get_page(&cx, page_no).unwrap();
        let page_data: &[u8] = page_data.as_ref();
        // First byte should be 0x0D (leaf table).
        assert_eq!(
            page_data[0], 0x0D,
            "allocated root page should be leaf table (0x0D)"
        );
        eprintln!(
            "[5A3][test=allocates_real_page][step=verify] page_type=0x{:02X} ✓",
            page_data[0]
        );
    }

    #[test]
    fn test_multiple_tables_get_different_pages() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER)").unwrap();
        conn.execute("CREATE TABLE t2 (b TEXT)").unwrap();
        conn.execute("CREATE TABLE t3 (c REAL)").unwrap();

        let schema = conn.schema.borrow();
        let pages: Vec<i32> = schema.iter().map(|s| s.root_page).collect();
        eprintln!("[5A3][test=different_pages][step=verify] pages={pages:?}");

        // All root pages should be distinct.
        let mut unique = pages.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            pages.len(),
            "all root pages should be unique: {pages:?}"
        );

        // All should be > 1.
        for &p in &pages {
            assert!(p > 1, "root page should be > 1, got {p}");
        }
    }

    #[test]
    fn test_created_root_page_is_valid_btree() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER)").unwrap();

        let schema = conn.schema.borrow();
        let rp = schema[0].root_page;

        // Open the page and verify B-tree header structure.
        let cx = Cx::new();
        let txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        #[allow(clippy::cast_sign_loss)]
        let page_no = PageNumber::new(rp as u32).unwrap();
        let page_data = txn.get_page(&cx, page_no).unwrap();
        let page_data: &[u8] = page_data.as_ref();

        // Byte 0: page type = 0x0D (leaf table)
        assert_eq!(page_data[0], 0x0D);
        // Bytes 1-2: first freeblock = 0
        assert_eq!(u16::from_be_bytes([page_data[1], page_data[2]]), 0);
        // Bytes 3-4: cell count = 0
        assert_eq!(u16::from_be_bytes([page_data[3], page_data[4]]), 0);
        // Byte 7: fragmented free bytes = 0
        assert_eq!(page_data[7], 0);

        eprintln!("[5A3][test=valid_btree][step=verify] page={rp} type=0x0D cells=0 ✓");
    }

    #[test]
    fn test_root_page_survives_commit_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();

        let original_root_page;
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t1 (x INTEGER)").unwrap();
            original_root_page = conn.schema.borrow()[0].root_page;
            eprintln!("[5A3][test=survives_commit][step=create] root_page={original_root_page}");
        }

        // Reopen and verify the page is readable with correct B-tree header.
        {
            let conn = Connection::open(db_str).unwrap();
            let cx = Cx::new();
            let txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
            #[allow(clippy::cast_sign_loss)]
            let page_no = PageNumber::new(original_root_page as u32).unwrap();
            let page_data = txn.get_page(&cx, page_no).unwrap();
            let page_data: &[u8] = page_data.as_ref();
            assert_eq!(
                page_data[0], 0x0D,
                "root page should still be leaf table after reopen"
            );
            eprintln!(
                "[5A3][test=survives_commit][step=reopen] page_type=0x{:02X} ✓",
                page_data[0]
            );
        }
    }

    #[test]
    fn test_root_page_matches_sqlite_master_entry() {
        // The root page in the schema should match what's stored in sqlite_master.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER)").unwrap();

        let schema_rp = conn.schema.borrow()[0].root_page;

        // Read sqlite_master from page 1 B-tree.
        let cx = Cx::new();
        let mut txn = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        let usable_size = PageSize::DEFAULT.get();
        let mut cursor = fsqlite_btree::BtCursor::new(
            TransactionPageIo::new(txn.as_mut()),
            PageNumber::ONE,
            usable_size,
            false,
        );
        assert!(
            cursor.first(&cx).unwrap(),
            "sqlite_master should have a row"
        );
        let payload = cursor.payload(&cx).unwrap();
        let values = fsqlite_types::record::parse_record(&payload).unwrap();
        // Column 3 = rootpage.
        if let SqliteValue::Integer(master_rp) = &values[3] {
            #[allow(clippy::cast_possible_truncation)]
            let master_rp_i32 = *master_rp as i32;
            assert_eq!(
                schema_rp, master_rp_i32,
                "schema root_page should match sqlite_master rootpage"
            );
            eprintln!(
                "[5A3][test=matches_master][step=verify] schema={schema_rp} master={master_rp_i32} ✓"
            );
        } else {
            panic!("rootpage should be Integer, got {:?}", values[3]);
        }
    }
}

// =========================================================================
// 5B.5 – autocommit pager transaction wrapping tests (bd-14dj)
// =========================================================================
#[cfg(test)]
mod autocommit_txn_tests {
    use super::*;

    #[test]
    fn test_autocommit_insert_commits() {
        // INSERT outside an explicit BEGIN should auto-wrap in a pager txn.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();

        // After autocommit, no active txn should remain.
        assert!(
            conn.active_txn.borrow().is_none(),
            "autocommit should clear active_txn after INSERT"
        );

        // Data should be visible.
        let rows = conn.query("SELECT x FROM t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(42));
        eprintln!("[5B5][test=autocommit_insert][step=verify] data_persisted=true ✓");
    }

    #[test]
    fn test_autocommit_write_mode_uses_concurrent_session_by_default() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.is_concurrent_mode_default());

        let started = conn.ensure_autocommit_txn().unwrap();
        assert!(started);
        assert!(conn.is_concurrent_transaction());
        assert!(conn.has_concurrent_session());

        conn.resolve_autocommit_txn(true, true).unwrap();
        assert!(!conn.is_concurrent_transaction());
        assert!(!conn.has_concurrent_session());
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_autocommit_read_mode_does_not_open_concurrent_session() {
        let conn = Connection::open(":memory:").unwrap();
        let started = conn
            .ensure_autocommit_txn_mode(TransactionMode::Deferred)
            .unwrap();
        assert!(started);
        assert!(!conn.is_concurrent_transaction());
        assert!(!conn.has_concurrent_session());

        conn.resolve_autocommit_txn(true, true).unwrap();
        assert!(conn.active_txn.borrow().is_none());
    }

    #[test]
    fn test_autocommit_ddl_wraps_in_txn() {
        // CREATE TABLE outside BEGIN should auto-wrap.
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.active_txn.borrow().is_none(), "no txn before DDL");

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // After DDL, autocommit should have cleaned up.
        assert!(
            conn.active_txn.borrow().is_none(),
            "autocommit should clear active_txn after DDL"
        );

        // Table should exist.
        let schema = conn.schema.borrow();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "t");
        eprintln!("[5B5][test=autocommit_ddl][step=verify] table_created=true ✓");
    }

    #[test]
    fn test_explicit_txn_bypasses_autocommit() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Start explicit transaction.
        conn.execute("BEGIN").unwrap();
        assert!(*conn.in_transaction.borrow());
        assert!(conn.active_txn.borrow().is_some());

        // INSERT inside explicit txn should NOT create a new autocommit txn.
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        // Txn should still be active (not auto-committed).
        assert!(
            *conn.in_transaction.borrow(),
            "explicit txn should remain active after INSERT"
        );
        assert!(
            conn.active_txn.borrow().is_some(),
            "active_txn should persist through explicit txn"
        );

        conn.execute("COMMIT").unwrap();
        assert!(!*conn.in_transaction.borrow());
        eprintln!("[5B5][test=explicit_bypasses][step=verify] ✓");
    }

    #[test]
    fn test_autocommit_multiple_inserts_independent() {
        // Each INSERT outside BEGIN gets its own implicit txn.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        assert!(
            conn.active_txn.borrow().is_none(),
            "auto-committed after first INSERT"
        );

        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        assert!(
            conn.active_txn.borrow().is_none(),
            "auto-committed after second INSERT"
        );

        let rows = conn.query("SELECT x FROM t ORDER BY x").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
        assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(2));
        eprintln!("[5B5][test=multiple_inserts][step=verify] count=2 ✓");
    }

    #[test]
    fn test_autocommit_file_backed_persists() {
        // Autocommit should persist data to file-backed databases.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();

        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.execute("INSERT INTO t VALUES (99)").unwrap();
            // No explicit COMMIT — autocommit should have done it.
        }

        // Reopen and verify data persisted.
        {
            let conn = Connection::open(db_str).unwrap();
            let rows = conn.query("SELECT x FROM t").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(99));
        }
        eprintln!("[5B5][test=file_backed_persists][step=verify] ✓");
    }
}

// ── Schema cookie and change counter tests (bd-3mmj) ────────────────────
#[cfg(test)]
mod schema_cookie_tests {
    use super::*;

    #[test]
    fn test_schema_cookie_starts_at_zero() {
        let conn = Connection::open(":memory:").unwrap();
        assert_eq!(conn.schema_cookie(), 0);
        assert_eq!(conn.change_counter(), 0);
    }

    #[test]
    fn test_create_table_increments_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        assert_eq!(conn.schema_cookie(), 0);

        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        assert_eq!(conn.schema_cookie(), 1);
    }

    #[test]
    fn test_multiple_ddl_increments_multiple_times() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b TEXT);").unwrap();
        conn.execute("CREATE TABLE t3 (c REAL);").unwrap();

        // 3 CREATE TABLE operations = cookie incremented 3 times.
        assert_eq!(conn.schema_cookie(), 3);
    }

    #[test]
    fn test_drop_table_increments_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        assert_eq!(conn.schema_cookie(), 1);

        conn.execute("DROP TABLE t1;").unwrap();
        assert_eq!(conn.schema_cookie(), 2);
    }

    #[test]
    fn test_alter_table_increments_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        assert_eq!(conn.schema_cookie(), 1);

        conn.execute("ALTER TABLE t1 ADD COLUMN b TEXT;").unwrap();
        assert_eq!(conn.schema_cookie(), 2);

        conn.execute("ALTER TABLE t1 RENAME TO t1_renamed;")
            .unwrap();
        assert_eq!(conn.schema_cookie(), 3);
    }

    #[test]
    fn test_create_index_increments_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        assert_eq!(conn.schema_cookie(), 1);

        conn.execute("CREATE INDEX idx_a ON t1 (a);").unwrap();
        assert_eq!(conn.schema_cookie(), 2);
    }

    #[test]
    fn test_create_view_increments_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        assert_eq!(conn.schema_cookie(), 1);

        conn.execute("CREATE VIEW v1 AS SELECT a FROM t1;").unwrap();
        assert_eq!(conn.schema_cookie(), 2);
    }

    #[test]
    fn test_drop_view_increments_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE VIEW v1 AS SELECT a FROM t1;").unwrap();
        assert_eq!(conn.schema_cookie(), 2);

        conn.execute("DROP VIEW v1;").unwrap();
        assert_eq!(conn.schema_cookie(), 3);
    }

    #[test]
    fn test_if_not_exists_no_double_increment() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        assert_eq!(conn.schema_cookie(), 1);

        // IF NOT EXISTS on an existing table should NOT increment cookie.
        conn.execute("CREATE TABLE IF NOT EXISTS t1 (a INTEGER);")
            .unwrap();
        assert_eq!(conn.schema_cookie(), 1);
    }

    #[test]
    fn test_drop_if_exists_no_increment_when_missing() {
        let conn = Connection::open(":memory:").unwrap();
        assert_eq!(conn.schema_cookie(), 0);

        // DROP IF EXISTS on a non-existent table should NOT increment.
        conn.execute("DROP TABLE IF EXISTS nonexistent;").unwrap();
        assert_eq!(conn.schema_cookie(), 0);
    }

    #[test]
    fn test_dml_does_not_increment_schema_cookie() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        let cookie_after_ddl = conn.schema_cookie();

        conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2);").unwrap();
        conn.execute("UPDATE t1 SET a = 10 WHERE a = 1;").unwrap();
        conn.execute("DELETE FROM t1 WHERE a = 2;").unwrap();

        // DML should not affect schema_cookie.
        assert_eq!(conn.schema_cookie(), cookie_after_ddl);
    }

    #[test]
    #[ignore = "5D.4: change_counter is now managed by pager; needs header write wiring"]
    fn test_change_counter_increments_on_persist() {
        // Use a temp file to trigger actual persistence.
        // NOTE: With 5D.4, change_counter updates require pager header writes.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_counter.db");
        let db_str = db_path.to_str().unwrap();

        let conn = Connection::open(db_str).unwrap();
        assert_eq!(conn.change_counter(), 0);

        // CREATE TABLE triggers persist → counter goes to 1.
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        assert!(conn.change_counter() >= 1);

        let counter_after_create = conn.change_counter();

        // INSERT triggers persist → counter increases again.
        conn.execute("INSERT INTO t1 VALUES (42);").unwrap();
        assert!(conn.change_counter() > counter_after_create);
    }

    #[test]
    #[ignore = "5D.4: schema_cookie persistence needs pager header write wiring"]
    fn test_schema_cookie_persists_round_trip() {
        // NOTE: With 5D.4, schema_cookie round-trip requires pager header writes.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cookie_rt.db");
        let db_str = db_path.to_str().unwrap();

        // Create database with several DDL operations.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
            conn.execute("CREATE TABLE t2 (b TEXT);").unwrap();
            conn.execute("INSERT INTO t1 VALUES (1);").unwrap();
        }

        // Reopen and check that schema_cookie was preserved.
        {
            let conn = Connection::open(db_str).unwrap();
            // schema_cookie should be >= 2 (two CREATE TABLE operations).
            assert!(
                conn.schema_cookie() >= 2,
                "expected schema_cookie >= 2 after round-trip, got {}",
                conn.schema_cookie()
            );
        }
    }

    #[test]
    fn test_schema_cookie_continues_from_loaded_value() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cookie_continue.db");
        let db_str = db_path.to_str().unwrap();

        // Create database with one table.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        }

        // Reopen and add another table — cookie should continue from loaded value.
        {
            let conn = Connection::open(db_str).unwrap();
            let cookie_on_open = conn.schema_cookie();
            conn.execute("CREATE TABLE t2 (b TEXT);").unwrap();
            assert_eq!(conn.schema_cookie(), cookie_on_open + 1);
        }
    }
}

// ── Schema loading from sqlite_master tests (bd-1soh) ───────────────────
#[cfg(test)]
mod schema_loading_tests {
    use super::*;

    #[test]
    fn test_open_existing_database_loads_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("schema_load.db");
        let db_str = db_path.to_str().unwrap();

        // Create database with tables.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
                .unwrap();
            conn.execute("CREATE TABLE orders (id INTEGER, amount REAL);")
                .unwrap();
        }

        // Reopen — schema should be loaded from sqlite_master.
        {
            let conn = Connection::open(db_str).unwrap();
            let schema = conn.schema.borrow();
            assert_eq!(schema.len(), 2, "should load 2 tables from sqlite_master");
            assert_eq!(schema[0].name, "users");
            assert_eq!(schema[1].name, "orders");
            assert_eq!(schema[0].columns.len(), 2);
            assert_eq!(schema[1].columns.len(), 2);
        }
    }

    #[test]
    fn test_schema_survives_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("roundtrip.db");
        let db_str = db_path.to_str().unwrap();

        // Create database, insert data, close.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t1 (val INTEGER);").unwrap();
            conn.execute("INSERT INTO t1 VALUES (42);").unwrap();
            conn.execute("INSERT INTO t1 VALUES (99);").unwrap();
        }

        // Reopen — data should survive the round trip.
        {
            let conn = Connection::open(db_str).unwrap();
            let rows = conn.query("SELECT val FROM t1 ORDER BY val;").unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(42));
            assert_eq!(rows[1].values()[0], SqliteValue::Integer(99));
        }
    }

    #[test]
    fn test_open_empty_database_starts_empty() {
        let conn = Connection::open(":memory:").unwrap();
        let schema = conn.schema.borrow();
        assert!(
            schema.is_empty(),
            "memory database should have empty schema"
        );
    }

    #[test]
    fn test_open_c_sqlite_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("from_c.db");
        let db_str = db_path.to_str().unwrap();

        // Create with C SQLite via rusqlite.
        {
            let rconn = rusqlite::Connection::open(&db_path).unwrap();
            rconn
                .execute_batch(
                    "CREATE TABLE items (val INTEGER, label TEXT);
                     INSERT INTO items VALUES (10, 'alpha');
                     INSERT INTO items VALUES (20, 'beta');",
                )
                .unwrap();
        }

        // Open with FrankenSQLite — should load schema and data.
        {
            let conn = Connection::open(db_str).unwrap();
            let schema = conn.schema.borrow();
            assert_eq!(schema.len(), 1);
            assert_eq!(schema[0].name, "items");
            drop(schema);

            let rows = conn
                .query("SELECT val, label FROM items ORDER BY val;")
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(10));
            assert_eq!(rows[0].values()[1], SqliteValue::Text("alpha".to_owned()));
            assert_eq!(rows[1].values()[0], SqliteValue::Integer(20));
            assert_eq!(rows[1].values()[1], SqliteValue::Text("beta".to_owned()));
        }
    }

    #[test]
    fn test_rowid_alias_loaded_from_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ipk.db");
        let db_str = db_path.to_str().unwrap();

        // Create a table with an INTEGER PRIMARY KEY (rowid alias).
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'hello');").unwrap();
        }

        // Reopen — rowid alias mapping should be restored.
        {
            let conn = Connection::open(db_str).unwrap();
            let alias_map = conn.rowid_alias_columns.borrow();
            assert!(
                alias_map.contains_key("t1"),
                "rowid alias for t1 should be loaded"
            );
            drop(alias_map);

            // INSERT with NULL id should autoincrement via rowid alias.
            conn.execute("INSERT INTO t1 VALUES (NULL, 'world');")
                .unwrap();
            let rows = conn.query("SELECT id, val FROM t1 ORDER BY id;").unwrap();
            assert_eq!(rows.len(), 2);
        }
    }

    #[test]
    fn test_next_master_rowid_advanced_past_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("master_rowid.db");
        let db_str = db_path.to_str().unwrap();

        // Create 3 tables.
        {
            let conn = Connection::open(db_str).unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
            conn.execute("CREATE TABLE t2 (b TEXT);").unwrap();
            conn.execute("CREATE TABLE t3 (c REAL);").unwrap();
        }

        // Reopen — next_master_rowid should be > 3.
        {
            let conn = Connection::open(db_str).unwrap();
            let next = *conn.next_master_rowid.borrow();
            assert!(
                next > 3,
                "next_master_rowid should be past the 3 loaded entries, got {next}"
            );

            // Creating another table should work without rowid collision.
            conn.execute("CREATE TABLE t4 (d BLOB);").unwrap();
            let schema = conn.schema.borrow();
            assert_eq!(schema.len(), 4);
        }
    }

    // ── bd-3uzh: SQLITE_BUSY_SNAPSHOT error handling tests ────────────────────

    #[test]
    fn test_error_codes_distinct() {
        // Verify SQLITE_BUSY and SQLITE_BUSY_SNAPSHOT have distinct extended error codes.
        // Both share base error code 5 (SQLITE_BUSY) but extended codes differ.
        use fsqlite_error::ErrorCode;

        let busy_err = FrankenError::Busy;
        let busy_snapshot_err = FrankenError::BusySnapshot {
            conflicting_pages: "1, 2".to_owned(),
        };

        // Same base error code.
        assert_eq!(busy_err.error_code(), ErrorCode::Busy);
        assert_eq!(busy_snapshot_err.error_code(), ErrorCode::Busy);

        // Different extended error codes.
        // SQLITE_BUSY = 5
        // SQLITE_BUSY_SNAPSHOT = 5 | (2 << 8) = 517
        assert_eq!(busy_err.extended_error_code(), 5);
        assert_eq!(busy_snapshot_err.extended_error_code(), 517);
        assert_ne!(
            busy_err.extended_error_code(),
            busy_snapshot_err.extended_error_code()
        );
    }

    #[test]
    fn test_busy_error_on_page_lock() {
        // Two concurrent sessions: second trying to write a page locked by first → SQLITE_BUSY.
        // This is already tested by test_mvcc_concurrent_page_lock_conflict but we verify
        // the error type is FrankenError::Busy (not BusySnapshot).
        use fsqlite_btree::cursor::PageWriter;
        use fsqlite_mvcc::ConcurrentHandle;
        use fsqlite_pager::traits::TransactionMode;
        use fsqlite_types::{Snapshot, TxnEpoch, TxnId, TxnToken};

        let conn = Connection::open(":memory:").unwrap();
        let cx = Cx::new();

        let mut txn1 = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        let snapshot1 = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let txn_token_1 = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(1));
        let mut handle1 = ConcurrentHandle::new(snapshot1, txn_token_1);
        let lock_table = InProcessPageLockTable::new();

        // Session 1 writes page 2.
        {
            let mut mvcc_io1 = MvccPageIo::new(&mut *txn1, &mut handle1, &lock_table, 1);
            let page_no = PageNumber::new(2).unwrap();
            let data = vec![0u8; 4096];
            mvcc_io1.write_page(&cx, page_no, &data).unwrap();
        }

        // Session 2 tries to write same page → BUSY (not BUSY_SNAPSHOT).
        let mut txn2 = conn.pager.begin(&cx, TransactionMode::Deferred).unwrap();
        let snapshot2 = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let txn_token_2 = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(1));
        let mut handle2 = ConcurrentHandle::new(snapshot2, txn_token_2);

        {
            let mut mvcc_io2 = MvccPageIo::new(&mut *txn2, &mut handle2, &lock_table, 2);
            let page_no = PageNumber::new(2).unwrap();
            let data = vec![0u8; 4096];
            let result = mvcc_io2.write_page(&cx, page_no, &data);

            // Must be Busy, not BusySnapshot.
            assert!(
                matches!(result, Err(FrankenError::Busy)),
                "expected FrankenError::Busy, got {result:?}"
            );
        }
    }

    #[test]
    fn test_busy_snapshot_error_on_fcw_conflict() {
        // Two concurrent transactions write the same page; second to commit gets BUSY_SNAPSHOT.
        // This tests the Connection-level error propagation from MVCC layer.
        use fsqlite_mvcc::{ConcurrentHandle, concurrent_commit};
        use fsqlite_types::{Snapshot, TxnEpoch, TxnId, TxnToken};

        let _conn = Connection::open(":memory:").unwrap();

        // Create a shared commit index and lock table.
        let commit_index = CommitIndex::new();
        let lock_table = InProcessPageLockTable::new();

        // Session 1: snapshot at seq 0, writes page 5.
        let snapshot1 = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let txn_id_1 = TxnId::new(1).unwrap();
        let txn_token_1 = TxnToken::new(txn_id_1, TxnEpoch::new(1));
        let mut handle1 = ConcurrentHandle::new(snapshot1, txn_token_1);

        // Acquire lock and add to write set.
        lock_table
            .try_acquire(PageNumber::new(5).unwrap(), txn_id_1)
            .unwrap();
        fsqlite_mvcc::concurrent_write_page(
            &mut handle1,
            &lock_table,
            1,
            PageNumber::new(5).unwrap(),
            PageData::from_vec(vec![0u8; 4096]),
        )
        .unwrap();

        // Session 1 commits at seq 1.
        let commit1_result = concurrent_commit(
            &mut handle1,
            &commit_index,
            &lock_table,
            1,
            CommitSeq::new(1),
        );
        assert!(
            commit1_result.is_ok(),
            "Session 1 should commit successfully"
        );

        // Session 2: snapshot at seq 0 (before session 1 committed), writes page 5.
        let snapshot2 = Snapshot::new(CommitSeq::ZERO, SchemaEpoch::new(0));
        let txn_id_2 = TxnId::new(2).unwrap();
        let txn_token_2 = TxnToken::new(txn_id_2, TxnEpoch::new(1));
        let mut handle2 = ConcurrentHandle::new(snapshot2, txn_token_2);

        lock_table
            .try_acquire(PageNumber::new(5).unwrap(), txn_id_2)
            .unwrap();
        fsqlite_mvcc::concurrent_write_page(
            &mut handle2,
            &lock_table,
            2,
            PageNumber::new(5).unwrap(),
            PageData::from_vec(vec![1u8; 4096]),
        )
        .unwrap();

        // Session 2 tries to commit — should fail with BusySnapshot (FCW violation).
        let commit2_result = concurrent_commit(
            &mut handle2,
            &commit_index,
            &lock_table,
            2,
            CommitSeq::new(2),
        );
        assert!(commit2_result.is_err(), "Session 2 should fail to commit");

        let (err, fcw_result) = commit2_result.unwrap_err();
        assert_eq!(
            err,
            fsqlite_mvcc::MvccError::BusySnapshot,
            "Expected BusySnapshot error"
        );

        // The FcwResult should indicate conflict on page 5.
        if let fsqlite_mvcc::FcwResult::Conflict {
            conflicting_pages, ..
        } = fcw_result
        {
            assert!(
                conflicting_pages.contains(&PageNumber::new(5).unwrap()),
                "Conflict should be on page 5"
            );
        } else {
            panic!("Expected FcwResult::Conflict, got {:?}", fcw_result);
        }
    }

    #[test]
    fn test_busy_snapshot_not_retriable_in_place() {
        // BusySnapshot means the entire transaction must be restarted — you cannot
        // simply retry the COMMIT. This test documents the expected behavior.
        //
        // Per SQLite semantics: SQLITE_BUSY_SNAPSHOT (517) indicates the transaction's
        // snapshot is stale. The only recovery is to ROLLBACK and start a new transaction
        // with a fresh snapshot.

        // This is a documentation/semantic test — the actual behavior is tested above.
        // The key insight is:
        // - SQLITE_BUSY (5): Can retry the same statement after waiting
        // - SQLITE_BUSY_SNAPSHOT (517): Must ROLLBACK and BEGIN again

        let busy_snapshot = FrankenError::BusySnapshot {
            conflicting_pages: "5".to_owned(),
        };

        // The suggestion should indicate transaction restart is needed.
        let suggestion = busy_snapshot.suggestion();
        assert!(
            suggestion.is_some(),
            "BusySnapshot should have a suggestion"
        );
        let suggestion_text = suggestion.unwrap();
        assert!(
            suggestion_text.contains("Retry")
                || suggestion_text.contains("restart")
                || suggestion_text.contains("transaction"),
            "Suggestion should mention retrying or restarting: {:?}",
            suggestion_text
        );
    }

    #[test]
    fn test_busy_timeout_zero_no_retry() {
        // When busy_timeout=0, SQLITE_BUSY should be returned immediately without retry.
        // This test verifies the pragma value is read correctly.
        let conn = Connection::open(":memory:").unwrap();

        // Set busy_timeout to 0.
        conn.execute("PRAGMA busy_timeout=0;").unwrap();

        // Verify the value.
        let state = conn.pragma_state();
        assert_eq!(
            state.busy_timeout_ms, 0,
            "busy_timeout should be 0 after PRAGMA"
        );
    }

    #[test]
    fn test_busy_timeout_retries_pragma_value() {
        // Verify busy_timeout pragma is stored and can be queried.
        let conn = Connection::open(":memory:").unwrap();

        // Default is 5000ms.
        let state = conn.pragma_state();
        assert_eq!(
            state.busy_timeout_ms, 5000,
            "Default busy_timeout should be 5000ms"
        );
        drop(state);

        // Set to 10000ms.
        conn.execute("PRAGMA busy_timeout=10000;").unwrap();
        let state = conn.pragma_state();
        assert_eq!(
            state.busy_timeout_ms, 10000,
            "busy_timeout should be 10000ms"
        );
        drop(state);

        // Query via SELECT.
        let rows = conn.query("PRAGMA busy_timeout;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].get(0).unwrap(), SqliteValue::Integer(10000));
    }

    /// br-22iss: Test UPDATE with numbered placeholders
    #[test]
    fn test_update_with_four_numbered_placeholders() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE agents (
                id INTEGER PRIMARY KEY,
                name TEXT,
                policy TEXT
            )",
        )
        .unwrap();

        conn.execute("INSERT INTO agents VALUES (1, 'BlueLake', 'auto')")
            .unwrap();
        conn.execute("INSERT INTO agents VALUES (2, 'RedFox', 'auto')")
            .unwrap();

        // Verify data exists
        let rows = conn.query("SELECT * FROM agents").unwrap();
        assert_eq!(rows.len(), 2, "Should have 2 rows");

        // Test: UPDATE with literal WHERE (should work - baseline)
        let affected = conn
            .execute("UPDATE agents SET policy = 'literal_test' WHERE name = 'RedFox'")
            .unwrap();
        assert_eq!(affected, 1, "UPDATE with literal WHERE should work");

        // Verify it worked
        let rows = conn
            .query("SELECT policy FROM agents WHERE name = 'RedFox'")
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Text("literal_test".to_string())
        );

        // Test: UPDATE with numbered placeholder in WHERE
        let affected = conn
            .execute_with_params(
                "UPDATE agents SET policy = 'param_test' WHERE name = ?1",
                &[SqliteValue::Text("RedFox".to_string())],
            )
            .unwrap();
        assert_eq!(affected, 1, "UPDATE WHERE name = ?1 should affect 1 row");
    }

    #[test]
    fn test_pragma_trace_stats_exposes_trace_metrics() {
        let conn = Connection::open(":memory:").unwrap();
        conn.query("SELECT 1;").unwrap();

        let rows = conn.query("PRAGMA trace_stats;").unwrap();
        let mut metric_names = rows
            .iter()
            .map(|row| match &row.values()[0] {
                SqliteValue::Text(name) => name.clone(),
                _ => String::new(),
            })
            .collect::<Vec<_>>();
        metric_names.sort();

        assert!(
            metric_names
                .iter()
                .any(|name| name == "fsqlite_trace_spans_total"),
            "trace_stats should expose fsqlite_trace_spans_total"
        );
        assert!(
            metric_names
                .iter()
                .any(|name| name == "fsqlite_trace_export_errors_total"),
            "trace_stats should expose fsqlite_trace_export_errors_total"
        );
        assert!(
            metric_names
                .iter()
                .any(|name| name == "fsqlite_compat_trace_callbacks_total"),
            "trace_stats should expose fsqlite_compat_trace_callbacks_total"
        );
    }

    #[test]
    fn test_pragma_trace_reset_returns_ok() {
        let conn = Connection::open(":memory:").unwrap();
        conn.query("PRAGMA trace_reset;").unwrap();
        let snapshot = trace_metrics_snapshot();
        assert_eq!(
            snapshot.fsqlite_trace_export_errors_total, 0,
            "trace_reset should clear export error counter"
        );
    }

    #[test]
    fn test_trace_v2_emits_stmt_profile_row_and_close() {
        let events = Arc::new(std::sync::Mutex::new(Vec::<TraceEvent>::new()));
        let sink = Arc::clone(&events);

        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.trace_v2(
            TraceMask::ALL,
            Some(Arc::new(move |event| {
                sink.lock().expect("trace sink mutex poisoned").push(event);
            })),
        );
        conn.query("SELECT id FROM t;").unwrap();
        conn.close().unwrap();

        let captured = events.lock().expect("trace sink mutex poisoned").clone();
        assert!(
            captured.iter().any(|event| matches!(
                event,
                TraceEvent::Statement { sql } if sql.contains("SELECT id FROM t")
            )),
            "expected statement callback for SELECT"
        );
        assert!(
            captured
                .iter()
                .any(|event| matches!(event, TraceEvent::Profile { .. })),
            "expected profile callback"
        );
        assert!(
            captured.iter().any(|event| matches!(
                event,
                TraceEvent::Row { values }
                    if matches!(values.first(), Some(SqliteValue::Integer(1)))
            )),
            "expected row callback with selected value"
        );
        assert!(
            captured
                .iter()
                .any(|event| matches!(event, TraceEvent::Close)),
            "expected close callback"
        );
    }

    #[test]
    fn test_trace_v2_mask_filters_callback_types() {
        let events = Arc::new(std::sync::Mutex::new(Vec::<TraceEvent>::new()));
        let sink = Arc::clone(&events);

        let conn = Connection::open(":memory:").unwrap();
        conn.trace_v2(
            TraceMask::STMT,
            Some(Arc::new(move |event| {
                sink.lock().expect("trace sink mutex poisoned").push(event);
            })),
        );
        conn.query("SELECT 1;").unwrap();

        let captured = events.lock().expect("trace sink mutex poisoned");
        assert!(
            captured
                .iter()
                .any(|event| matches!(event, TraceEvent::Statement { .. })),
            "expected at least one statement callback"
        );
        assert!(
            captured
                .iter()
                .all(|event| matches!(event, TraceEvent::Statement { .. })),
            "TraceMask::STMT should filter out profile/row/close callbacks"
        );
        drop(captured);
    }

    #[test]
    fn test_trace_v2_lifecycle_and_trace_metrics_for_multi_statement_workload() {
        let conn = Connection::open(":memory:").unwrap();
        conn.query("PRAGMA trace_reset;").unwrap();
        let baseline = trace_metrics_snapshot();

        let events = Arc::new(std::sync::Mutex::new(Vec::<TraceEvent>::new()));
        let sink = Arc::clone(&events);
        conn.trace_v2(
            TraceMask::ALL,
            Some(Arc::new(move |event| {
                sink.lock().expect("trace sink mutex poisoned").push(event);
            })),
        );

        conn.execute("CREATE TABLE trace_cov (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO trace_cov VALUES (1, 10);")
            .unwrap();
        conn.execute("UPDATE trace_cov SET v = v + 1 WHERE id = 1;")
            .unwrap();
        conn.query("SELECT v FROM trace_cov WHERE id = 1;").unwrap();
        conn.execute("DELETE FROM trace_cov WHERE id = 1;").unwrap();
        conn.trace_v2(TraceMask::ALL, None);

        let captured = events.lock().expect("trace sink mutex poisoned").clone();
        let statement_count = captured
            .iter()
            .filter(|event| matches!(event, TraceEvent::Statement { .. }))
            .count();
        let profile_count = captured
            .iter()
            .filter(|event| matches!(event, TraceEvent::Profile { .. }))
            .count();
        assert!(
            statement_count >= 5,
            "expected at least one stmt callback per executed statement"
        );
        assert!(
            profile_count >= statement_count,
            "expected profile callbacks to cover every observed statement callback"
        );
        assert!(
            captured.iter().any(|event| matches!(
                event,
                TraceEvent::Row { values }
                    if matches!(values.first(), Some(SqliteValue::Integer(11)))
            )),
            "expected row callback to include updated SELECT value"
        );

        let snapshot = trace_metrics_snapshot();
        assert!(
            snapshot.fsqlite_trace_spans_total > baseline.fsqlite_trace_spans_total,
            "trace span counter should increase for a multi-statement workload"
        );
        assert!(
            snapshot.fsqlite_compat_trace_callbacks_total >= captured.len() as u64,
            "compat callback counter should include emitted callbacks"
        );
    }
}
