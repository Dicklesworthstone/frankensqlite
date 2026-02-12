//! SQL connection API with Phase 5 pager/WAL/B-tree storage wiring.
//!
//! Supports expression-only SELECT statements as well as table-backed DML:
//! CREATE TABLE, DROP TABLE, INSERT, SELECT (with FROM), UPDATE, and DELETE. Table
//! storage currently uses the in-memory `MemDatabase` backend for execution,
//! while a [`PagerBackend`] is initialized alongside for future Phase 5
//! sub-tasks (bd-1dqg, bd-25c6) that will wire the transaction lifecycle
//! and cursor paths through the real storage stack.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
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
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, VfsOpenFlags};
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{BTreePageHeader, PageNumber, PageSize};
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
use fsqlite_wal::{WalFile, WalSalts};

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
            execute_table_program_with_db(
                &self.program,
                None,
                registry,
                db,
                None,
                self.schema_cookie,
            )
            .0?
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
            execute_table_program_with_db(
                &self.program,
                Some(params),
                registry,
                db,
                None,
                self.schema_cookie,
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

/// Snapshot of the database + schema state at a point in time.
/// Used for transaction rollback and savepoint restore.
#[derive(Debug, Clone)]
struct DbSnapshot {
    db_version: MemDbVersionToken,
    schema: Vec<TableSchema>,
}

/// A named savepoint with its pre-state snapshot.
#[derive(Debug, Clone)]
struct SavepointEntry {
    name: String,
    snapshot: DbSnapshot,
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
    /// Scalar/aggregate/window function registry shared with the VDBE engine.
    func_registry: Arc<FunctionRegistry>,
    /// Whether an explicit transaction is active (BEGIN without matching COMMIT/ROLLBACK).
    in_transaction: RefCell<bool>,
    /// Snapshot taken at BEGIN time, restored on ROLLBACK.
    txn_snapshot: RefCell<Option<DbSnapshot>>,
    /// Savepoint stack: each SAVEPOINT pushes a snapshot, RELEASE pops,
    /// ROLLBACK TO restores to the named savepoint (without popping).
    savepoints: RefCell<Vec<SavepointEntry>>,
    /// Optional on-disk persistence path for non-`:memory:` connections.
    persist_path: Option<String>,
    /// Internal guard to suppress persistence while restoring from disk.
    persist_suspended: RefCell<bool>,
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
        let persist_path = (path != ":memory:").then(|| path.clone());

        // Phase 5 (bd-3iw8): initialize the pager backend alongside
        // the MemDatabase. Sub-tasks bd-1dqg and bd-25c6 will wire
        // transaction lifecycle and cursor paths through this pager.
        let pager = PagerBackend::open(&path)?;

        let conn = Self {
            path,
            db: Rc::new(RefCell::new(MemDatabase::new())),
            pager,
            active_txn: RefCell::new(None),
            schema: RefCell::new(Vec::new()),
            views: RefCell::new(Vec::new()),
            func_registry: default_function_registry(),
            in_transaction: RefCell::new(false),
            txn_snapshot: RefCell::new(None),
            savepoints: RefCell::new(Vec::new()),
            persist_path,
            persist_suspended: RefCell::new(false),
            last_changes: RefCell::new(0),
            implicit_txn: RefCell::new(false),
            concurrent_txn: RefCell::new(false),
            concurrent_mode_default: RefCell::new(true),
            pragma_state: RefCell::new(fsqlite_vdbe::pragma::ConnectionPragmaState::default()),
            rowid_alias_columns: RefCell::new(HashMap::new()),
            next_master_rowid: RefCell::new(1),
            schema_cookie: RefCell::new(0),
            change_counter: RefCell::new(0),
        };
        conn.apply_current_journal_mode_to_pager()?;
        conn.load_persisted_state_if_present()?;
        Ok(conn)
    }

    /// Returns the configured database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Prepare SQL into a statement.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let statement = parse_single_statement(sql)?;
        let statement = self.rewrite_subquery_statement(statement)?;
        self.compile_and_wrap(&statement)
    }

    /// Prepare and execute SQL as a query.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let statements = parse_statements(sql)?;
        let mut rows = Vec::new();
        for statement in statements {
            rows = self.execute_statement(statement, None)?;
        }
        Ok(rows)
    }

    /// Prepare and execute SQL as a query with bound SQL parameters.
    pub fn query_with_params(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>> {
        let statement = parse_single_statement(sql)?;
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
        let statements = parse_statements(sql)?;
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
        let statement = parse_single_statement(sql)?;
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
    fn execute_statement(
        &self,
        statement: Statement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let statement = self.rewrite_subquery_statement(statement)?;
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
                self.persist_if_needed()?;
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
                let distinct = is_distinct_select(select);
                // Compound SELECT (UNION/UNION ALL/INTERSECT/EXCEPT).
                if !select.body.compounds.is_empty() {
                    return self.execute_compound_select(select, params);
                }
                // Check if this is an expression-only SELECT (no FROM clause).
                if is_expression_only_select(select) {
                    // Fallback codegen: eagerly rewrite IN subqueries.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let mut rows = execute_program_with_postprocess(
                        &compile_expression_select(&rewritten)?,
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
                    let mut rows =
                        self.execute_group_by_join_select(&rewritten, params)?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else if has_group_by(select) {
                    // Fallback path: eagerly rewrite IN subqueries.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let mut rows = self.execute_group_by_select(&rewritten, params)?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else if has_joins(select) || has_subquery_source(select) {
                    // Fallback path: eagerly rewrite IN subqueries.
                    // Also handles subquery in FROM (derived tables) even
                    // without explicit JOINs.
                    let rewritten = self.rewrite_in_subqueries_select(select)?;
                    let mut rows = self.execute_join_select(&rewritten, params)?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else {
                    let limit_clause = select.limit.clone();
                    let program = if distinct && limit_clause.is_some() {
                        let mut unbounded = select.clone();
                        unbounded.limit = None;
                        self.compile_table_select(&unbounded)?
                    } else {
                        self.compile_table_select(select)?
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
                if let fsqlite_ast::InsertSource::Select(select_stmt) = &insert.source {
                    // Use VDBE path when RETURNING is present; fallback otherwise.
                    if insert.returning.is_empty() {
                        let affected =
                            self.execute_insert_select_fallback(insert, select_stmt, params)?;
                        self.persist_if_needed()?;
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
                let program = self.compile_table_insert(insert)?;
                let rows = self.execute_table_program(&program, params)?;
                self.persist_if_needed()?;
                *self.last_changes.borrow_mut() = affected;
                if insert.returning.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(rows)
                }
            }
            Statement::Update(ref update) => {
                let affected =
                    self.count_matching_rows(&update.table, update.where_clause.as_ref())?;
                let program = self.compile_table_update(update)?;
                let rows = self.execute_table_program(&program, params)?;
                self.persist_if_needed()?;
                *self.last_changes.borrow_mut() = affected;
                if update.returning.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(rows)
                }
            }
            Statement::Delete(ref delete) => {
                let affected =
                    self.count_matching_rows(&delete.table, delete.where_clause.as_ref())?;
                let program = self.compile_table_delete(delete)?;
                let rows = self.execute_table_program(&program, params)?;
                self.persist_if_needed()?;
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
                self.persist_if_needed()?;
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
                self.persist_if_needed()?;
                Ok(Vec::new())
            }
            Statement::AlterTable(ref alter) => {
                self.execute_alter_table(alter)?;
                self.persist_if_needed()?;
                Ok(Vec::new())
            }
            Statement::CreateIndex(ref create_idx) => {
                self.execute_create_index(create_idx)?;
                self.persist_if_needed()?;
                Ok(Vec::new())
            }
            Statement::CreateView(ref create_view) => {
                self.execute_create_view(create_view)?;
                Ok(Vec::new())
            }
            // Maintenance stubs: these are no-ops for in-memory databases but
            // accepted for SQL compatibility (applications often call them).
            Statement::Vacuum(_) | Statement::Analyze(_) | Statement::Reindex(_) => {
                Ok(Vec::new())
            }
            _ => Err(FrankenError::NotImplemented(
                "only SELECT, INSERT, UPDATE, DELETE, DDL (CREATE/DROP/ALTER TABLE, CREATE INDEX/VIEW), transaction control, PRAGMA, VACUUM, ANALYZE, and REINDEX are supported".to_owned(),
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
    fn count_matching_rows(
        &self,
        table_ref: &fsqlite_ast::QualifiedTableRef,
        where_clause: Option<&Expr>,
    ) -> Result<usize> {
        let alias_clause = table_ref
            .alias
            .as_ref()
            .map_or(String::new(), |a| format!(" AS {a}"));
        let sql = if let Some(cond) = where_clause {
            format!(
                "SELECT * FROM {}{alias_clause} WHERE {cond}",
                table_ref.name
            )
        } else {
            format!("SELECT * FROM {}", table_ref.name)
        };
        Ok(self.query(&sql)?.len())
    }

    // ── autocommit pager transaction wrapping (bd-14dj / 5B.5) ──────────

    /// Ensure a pager transaction is active.  If the connection is NOT
    /// inside an explicit `BEGIN`, an implicit (autocommit) transaction is
    /// created.  Returns `true` when an implicit transaction was started
    /// (the caller must later call [`resolve_autocommit_txn`]).
    fn ensure_autocommit_txn(&self) -> Result<bool> {
        self.ensure_autocommit_txn_mode(TransactionMode::Immediate)
    }

    /// Ensure a pager transaction is active, using the specified mode.
    /// Phase 5B.2 (bd-1yi8): reads also need pager transactions so
    /// they can see data written by prior write-through INSERTs.
    fn ensure_autocommit_txn_mode(&self, mode: TransactionMode) -> Result<bool> {
        if *self.in_transaction.borrow() || self.active_txn.borrow().is_some() {
            return Ok(false);
        }
        let cx = Cx::new();
        let txn = self.pager.begin(&cx, mode)?;
        *self.active_txn.borrow_mut() = Some(txn);
        Ok(true)
    }

    /// Finalize an implicit (autocommit) transaction: commit on success,
    /// rollback on error.  No-op when `was_auto` is `false`.
    fn resolve_autocommit_txn(&self, was_auto: bool, ok: bool) -> Result<()> {
        if !was_auto {
            return Ok(());
        }
        let cx = Cx::new();
        let mut guard = self.active_txn.borrow_mut();
        if let Some(txn) = guard.as_deref_mut() {
            if ok {
                txn.commit(&cx)?;
            } else {
                txn.rollback(&cx)?;
            }
        }
        *guard = None;
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
                // Insert result rows into the new table.
                for (i, row) in rows.iter().enumerate() {
                    let vals: Vec<SqliteValue> = row.values().to_vec();
                    #[allow(clippy::cast_possible_wrap)]
                    let rowid = (i + 1) as i64;
                    let mut db = self.db.borrow_mut();
                    if let Some(table) = db.get_table_mut(rp) {
                        table.insert_row(rowid, vals);
                    }
                }
                self.insert_sqlite_master_row("table", &tbl_name, &tbl_name, rp, &create_sql)?;
            }
        }

        self.increment_schema_cookie();
        Ok(())
    }

    /// Execute a DROP statement (TABLE, INDEX, VIEW).
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
                // Triggers are not implemented; silently succeed (nothing to drop).
                false
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

    /// Execute a CREATE INDEX statement (schema-only; no physical index yet).
    fn execute_create_index(&self, stmt: &fsqlite_ast::CreateIndexStatement) -> Result<()> {
        let table_name = &stmt.table;
        let mut schema = self.schema.borrow_mut();
        let table = schema
            .iter_mut()
            .find(|t| t.name.eq_ignore_ascii_case(table_name))
            .ok_or_else(|| FrankenError::NoSuchTable {
                name: table_name.clone(),
            })?;
        // Check for duplicate index name.
        let index_name = &stmt.name.name;
        if table
            .indexes
            .iter()
            .any(|idx| idx.name.eq_ignore_ascii_case(index_name))
        {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(FrankenError::Internal(format!(
                "index {index_name} already exists"
            )));
        }
        // Validate that all indexed columns exist and collect their names.
        let mut col_names = Vec::with_capacity(stmt.columns.len());
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
            col_names.push(col_name.to_owned());
        }
        // Record the index in the schema for metadata purposes.
        table.indexes.push(IndexSchema {
            name: stmt.name.name.clone(),
            columns: col_names,
            root_page: 0,
        });
        drop(schema);
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
        }
    }

    /// Restore a snapshot, replacing the current database + schema state.
    fn restore_snapshot(&self, snap: &DbSnapshot) {
        self.db.borrow_mut().rollback_to(snap.db_version);
        (*self.schema.borrow_mut()).clone_from(&snap.schema);
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
        let txn = self.pager.begin(&cx, pager_mode)?;
        *self.active_txn.borrow_mut() = Some(txn);

        self.db.borrow_mut().begin_undo();
        *self.txn_snapshot.borrow_mut() = Some(self.snapshot());
        *self.in_transaction.borrow_mut() = true;
        *self.concurrent_txn.borrow_mut() = is_concurrent;
        Ok(())
    }

    /// Handle COMMIT.
    fn execute_commit(&self) -> Result<()> {
        if !*self.in_transaction.borrow() {
            return Err(FrankenError::Internal(
                "cannot commit - no transaction is active".to_owned(),
            ));
        }

        let cx = Cx::new();
        // Attempt commit without consuming the handle (retriable on BUSY).
        // We use a scope to limit the mutable borrow of active_txn.
        {
            let mut txn_guard = self.active_txn.borrow_mut();
            if let Some(txn) = txn_guard.as_mut() {
                txn.commit(&cx)?;
            }
        }

        // Commit succeeded; now consume and drop the handle.
        *self.active_txn.borrow_mut() = None;

        // Discard rollback snapshot and savepoints — changes are committed.
        *self.txn_snapshot.borrow_mut() = None;
        self.savepoints.borrow_mut().clear();
        *self.in_transaction.borrow_mut() = false;
        *self.implicit_txn.borrow_mut() = false;
        *self.concurrent_txn.borrow_mut() = false;
        self.db.borrow_mut().commit_undo();
        Ok(())
    }

    /// Handle ROLLBACK [TO SAVEPOINT name].
    fn execute_rollback(&self, rb: &fsqlite_ast::RollbackStatement) -> Result<()> {
        let cx = Cx::new();
        if let Some(ref sp_name) = rb.to_savepoint {
            // ROLLBACK TO SAVEPOINT: restore to the named savepoint's snapshot
            // but keep the savepoint (don't pop it).
            if let Some(txn) = self.active_txn.borrow_mut().as_mut() {
                txn.rollback_to_savepoint(&cx, sp_name)?;
            }

            let savepoints = self.savepoints.borrow();
            let entry = savepoints
                .iter()
                .rev()
                .find(|e| e.name.eq_ignore_ascii_case(sp_name))
                .ok_or_else(|| FrankenError::Internal(format!("no such savepoint: {sp_name}")))?;
            let snap = entry.snapshot.clone();
            drop(savepoints);
            self.restore_snapshot(&snap);
        } else {
            // Full ROLLBACK: restore to transaction start.
            if !*self.in_transaction.borrow() {
                return Err(FrankenError::Internal(
                    "cannot rollback - no transaction is active".to_owned(),
                ));
            }

            if let Some(mut txn) = self.active_txn.borrow_mut().take() {
                txn.rollback(&cx)?;
            }

            let snap = self.txn_snapshot.borrow().clone();
            if let Some(snap) = &snap {
                self.restore_snapshot(snap);
            }
            *self.txn_snapshot.borrow_mut() = None;
            self.savepoints.borrow_mut().clear();
            *self.in_transaction.borrow_mut() = false;
            *self.implicit_txn.borrow_mut() = false;
            *self.concurrent_txn.borrow_mut() = false;
            self.db.borrow_mut().commit_undo();
        }
        Ok(())
    }

    /// Handle SAVEPOINT name.
    #[allow(clippy::unnecessary_wraps)] // will return errors once pager is wired
    fn execute_savepoint(&self, name: &str) -> Result<()> {
        let cx = Cx::new();
        // If no explicit transaction, implicitly begin one.
        if !*self.in_transaction.borrow() {
            let txn = self.pager.begin(&cx, TransactionMode::Deferred)?;
            *self.active_txn.borrow_mut() = Some(txn);

            self.db.borrow_mut().begin_undo();
            *self.txn_snapshot.borrow_mut() = Some(self.snapshot());
            *self.in_transaction.borrow_mut() = true;
            *self.implicit_txn.borrow_mut() = true;
        }

        if let Some(txn) = self.active_txn.borrow_mut().as_mut() {
            txn.savepoint(&cx, name)?;
        }

        self.savepoints.borrow_mut().push(SavepointEntry {
            name: name.to_owned(),
            snapshot: self.snapshot(),
        });
        Ok(())
    }

    /// Handle RELEASE \[SAVEPOINT\] name.
    fn execute_release(&self, name: &str) -> Result<()> {
        let cx = Cx::new();
        if let Some(txn) = self.active_txn.borrow_mut().as_mut() {
            txn.release_savepoint(&cx, name)?;
        }

        let mut savepoints = self.savepoints.borrow_mut();
        let idx = savepoints
            .iter()
            .rposition(|e| e.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| FrankenError::Internal(format!("no such savepoint: {name}")))?;
        // RELEASE removes the named savepoint and all savepoints created after it.
        savepoints.truncate(idx);
        // If no savepoints remain and we started implicitly, end the transaction.
        if savepoints.is_empty() && *self.implicit_txn.borrow() {
            drop(savepoints);
            // Implicit transaction via savepoint: commit on final release.
            // (If the user did explicit BEGIN, they still need COMMIT.)
            if let Some(mut txn) = self.active_txn.borrow_mut().take() {
                txn.commit(&cx)?;
            }

            *self.txn_snapshot.borrow_mut() = None;
            *self.in_transaction.borrow_mut() = false;
            *self.implicit_txn.borrow_mut() = false;
            self.db.borrow_mut().commit_undo();
        }
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
            "wal_checkpoint" => {
                // Parse checkpoint mode from the value (PASSIVE, FULL, RESTART, TRUNCATE).
                // Default is PASSIVE if no value provided.
                let mode = if let Some(ref val) = pragma.value {
                    parse_checkpoint_mode(val)?
                } else {
                    CheckpointMode::Passive
                };

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
        let ctx = CodegenContext {
            concurrent_mode: self.is_concurrent_transaction(),
            rowid_alias_col_idx: None,
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
        // Lend the active transaction to the VDBE engine so that storage
        // cursors route through the real pager/WAL stack (Phase 5, bd-2a3y).
        let txn = self.active_txn.borrow_mut().take();
        let cookie = *self.schema_cookie.borrow();
        let (result, txn_back) = execute_table_program_with_db(
            program,
            params,
            &self.func_registry,
            &self.db,
            txn,
            cookie,
        );
        // Always restore the transaction handle, even on error.
        if let Some(txn) = txn_back {
            *self.active_txn.borrow_mut() = Some(txn);
        }
        result
    }

    fn load_persisted_state_if_present(&self) -> Result<()> {
        let Some(path) = self.persist_path.as_deref() else {
            return Ok(());
        };
        let path = Path::new(path);
        if !path.exists() {
            return Ok(());
        }

        // Detect file format: real SQLite binary vs legacy SQL text dump.
        if crate::compat_persist::is_sqlite_format(path) {
            let loaded = crate::compat_persist::load_from_sqlite(path)?;
            // 5A.4: populate rowid alias columns from loaded schema.
            let mut alias_map = self.rowid_alias_columns.borrow_mut();
            for ts in &loaded.schema {
                if let Some(idx) = ts.columns.iter().position(|c| c.is_ipk) {
                    alias_map.insert(ts.name.to_ascii_lowercase(), idx);
                }
            }
            drop(alias_map);
            *self.schema.borrow_mut() = loaded.schema;
            *self.db.borrow_mut() = loaded.db;
            // 5A.4: advance next_master_rowid past existing entries so
            // future CREATE TABLE inserts don't collide with loaded rows.
            *self.next_master_rowid.borrow_mut() = loaded.master_row_count + 1;
            // 5A.5: restore schema cookie and change counter from the
            // loaded database header so subsequent DDL/DML increments
            // start from the correct baseline.
            *self.schema_cookie.borrow_mut() = loaded.schema_cookie;
            *self.change_counter.borrow_mut() = loaded.change_counter;
            return Ok(());
        }

        // Legacy SQL text dump fallback.
        let sql_dump = fsqlite_vfs::host_fs::read_to_string(path)?;
        if sql_dump.trim().is_empty() {
            return Ok(());
        }
        self.with_persistence_suspended(|| {
            for statement in parse_statements(&sql_dump)? {
                let _ = self.execute_statement(statement, None)?;
            }
            Ok(())
        })
    }

    fn with_persistence_suspended<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let prev = *self.persist_suspended.borrow();
        *self.persist_suspended.borrow_mut() = true;
        let result = f();
        *self.persist_suspended.borrow_mut() = prev;
        result
    }

    fn persist_if_needed(&self) -> Result<()> {
        let Some(path) = self.persist_path.as_deref() else {
            return Ok(());
        };
        if *self.persist_suspended.borrow() {
            return Ok(());
        }
        // Increment the file change counter on every persist (=write transaction).
        self.increment_change_counter();
        let schema = self.schema.borrow();
        let db = self.db.borrow();
        let cookie = *self.schema_cookie.borrow();
        let counter = *self.change_counter.borrow();
        crate::compat_persist::persist_to_sqlite(Path::new(path), &schema, &db, cookie, counter)
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

fn execute_table_program_with_db(
    program: &VdbeProgram,
    params: Option<&[SqliteValue]>,
    func_registry: &Arc<FunctionRegistry>,
    db: &Rc<RefCell<MemDatabase>>,
    txn: Option<Box<dyn TransactionHandle>>,
    schema_cookie: u32,
) -> (Result<Vec<Row>>, Option<Box<dyn TransactionHandle>>) {
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
    // This also enables storage cursors automatically.
    if let Some(txn) = txn {
        engine.set_transaction(txn);
    } else {
        engine.enable_storage_read_cursors(true);
    }

    // Lend the MemDatabase to the engine for the duration of execution.
    let db_value = db.replace(MemDatabase::new());
    engine.set_database(db_value);

    // Always take the DB and txn back, even if execution returns Err.
    let exec_res = engine.execute(program);
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
    use super::{Connection, Row};
    use fsqlite_ast::Statement;
    use fsqlite_error::FrankenError;
    use fsqlite_types::opcode::{Opcode, P4};
    use fsqlite_types::value::SqliteValue;
    use fsqlite_vdbe::engine::{ExecOutcome, MemDatabase, VdbeEngine};

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
    fn test_savepoint_starts_implicit_transaction() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(!conn.in_transaction());
        conn.execute("SAVEPOINT sp1;").unwrap();
        assert!(conn.in_transaction());
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
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (bl BLOB);").unwrap();
        conn.execute("INSERT INTO t VALUES (X'DEADBEEF');").unwrap();

        let stmt = super::parse_single_statement("SELECT bl FROM t;").unwrap();
        let select = match stmt {
            Statement::Select(select) => select,
            other => unreachable!("expected SELECT statement, got {other:?}"),
        };
        let program = conn.compile_table_select(&select).unwrap();

        // Execute once with mem cursors (storage cursors disabled).
        let db_mem = conn.db.replace(MemDatabase::new());
        let mut engine = VdbeEngine::new(program.register_count());
        engine.enable_storage_read_cursors(false);
        engine.set_database(db_mem);
        let outcome = engine.execute(&program).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        let mem_rows = engine.take_results();
        if let Some(db_after) = engine.take_database() {
            *conn.db.borrow_mut() = db_after;
        }

        // Execute once with storage-backed read cursors enabled.
        let db_storage = conn.db.replace(MemDatabase::new());
        let mut engine = VdbeEngine::new(program.register_count());
        engine.enable_storage_read_cursors(true);
        engine.set_database(db_storage);
        let outcome = engine.execute(&program).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        let storage_rows = engine.take_results();
        if let Some(db_after) = engine.take_database() {
            *conn.db.borrow_mut() = db_after;
        }

        let expected = vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])];
        assert_eq!(mem_rows, vec![expected.clone()]);
        assert_eq!(storage_rows, vec![expected]);
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

    #[test]
    fn test_unrecognised_pragma_silently_ignored() {
        let conn = Connection::open(":memory:").unwrap();
        // Unknown pragma should not error.
        conn.execute("PRAGMA some_unknown_pragma=42;").unwrap();
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
    fn test_pragma_wal_checkpoint_fails_in_delete_mode() {
        let conn = Connection::open(":memory:").unwrap();
        // Switch to delete/rollback journal mode.
        conn.execute("PRAGMA journal_mode='delete';").unwrap();
        assert_eq!(
            conn.pager.journal_mode(),
            fsqlite_pager::JournalMode::Delete
        );

        // Checkpoint should fail in non-WAL mode.
        let result = conn.query("PRAGMA wal_checkpoint;");
        assert!(result.is_err(), "checkpoint should fail in delete mode");
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
    fn test_drop_trigger_noop() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        // DROP TRIGGER should succeed silently even though triggers
        // are not implemented (no-op stub).
        conn.execute("DROP TRIGGER IF EXISTS my_trigger").unwrap();
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
    fn test_change_counter_increments_on_persist() {
        // Use a temp file to trigger actual persistence.
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
    fn test_schema_cookie_persists_round_trip() {
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
}
