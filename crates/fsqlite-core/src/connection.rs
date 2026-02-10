//! SQL connection API for the Phase 4 query pipeline.
//!
//! Supports expression-only SELECT statements as well as table-backed DML:
//! CREATE TABLE, INSERT, SELECT (with FROM), UPDATE, and DELETE. All table
//! storage uses the in-memory `MemDatabase` backend until the B-tree layer
//! is wired in Phase 5.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use fsqlite_ast::{
    BinaryOp, CreateTableBody, Distinctness, Expr, FunctionArgs, InSet, LikeOp, LimitClause,
    Literal, NullsOrder, OrderingTerm, PlaceholderType, ResultColumn, SelectBody, SelectCore,
    SelectStatement, SortDirection, Statement, UnaryOp,
};
use fsqlite_error::{FrankenError, Result};
use fsqlite_func::FunctionRegistry;
use fsqlite_parser::Parser;
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::codegen::{
    CodegenContext, CodegenError, ColumnInfo, TableSchema, codegen_delete, codegen_insert,
    codegen_select, codegen_update,
};
use fsqlite_vdbe::engine::{ExecOutcome, MemDatabase, VdbeEngine};
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};

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
        let mut rows = execute_program_with_postprocess(
            &self.program,
            None,
            self.func_registry.as_ref(),
            self.expression_postprocess.as_ref(),
        )?;
        if self.distinct {
            dedup_rows(&mut rows);
        }
        Ok(rows)
    }

    /// Execute as a query with bound SQL parameters (`?1`, `?2`, ...).
    pub fn query_with_params(&self, params: &[SqliteValue]) -> Result<Vec<Row>> {
        let mut rows = execute_program_with_postprocess(
            &self.program,
            Some(params),
            self.func_registry.as_ref(),
            self.expression_postprocess.as_ref(),
        )?;
        if self.distinct {
            dedup_rows(&mut rows);
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

/// Snapshot of the database + schema state at a point in time.
/// Used for transaction rollback and savepoint restore.
#[derive(Debug, Clone)]
struct DbSnapshot {
    db: MemDatabase,
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
    db: RefCell<MemDatabase>,
    /// Schema registry: table metadata used by the code generator.
    schema: RefCell<Vec<TableSchema>>,
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
        let conn = Self {
            path,
            db: RefCell::new(MemDatabase::new()),
            schema: RefCell::new(Vec::new()),
            func_registry: default_function_registry(),
            in_transaction: RefCell::new(false),
            txn_snapshot: RefCell::new(None),
            savepoints: RefCell::new(Vec::new()),
            persist_path,
            persist_suspended: RefCell::new(false),
            last_changes: RefCell::new(0),
        };
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
    #[allow(clippy::too_many_lines)]
    fn execute_statement(
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
                let distinct = is_distinct_select(select);
                // Check if this is an expression-only SELECT (no FROM clause).
                if is_expression_only_select(select) {
                    let mut rows = execute_program_with_postprocess(
                        &compile_expression_select(select)?,
                        params,
                        Some(&self.func_registry),
                        Some(&build_expression_postprocess(select)),
                    )?;
                    if distinct {
                        dedup_rows(&mut rows);
                    }
                    Ok(rows)
                } else if has_group_by(select) {
                    let mut rows = self.execute_group_by_select(select, params)?;
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
                let affected = match &insert.source {
                    fsqlite_ast::InsertSource::Values(v) => v.len(),
                    fsqlite_ast::InsertSource::DefaultValues => 1,
                    fsqlite_ast::InsertSource::Select(sel) => {
                        // Run the inner SELECT to determine row count.
                        self.execute_statement(Statement::Select(*sel.clone()), None)?
                            .len()
                    }
                };
                let program = self.compile_table_insert(insert)?;
                self.execute_table_program(&program, params)?;
                self.persist_if_needed()?;
                *self.last_changes.borrow_mut() = affected;
                Ok(Vec::new())
            }
            Statement::Update(ref update) => {
                let affected =
                    self.count_matching_rows(&update.table.name, update.where_clause.as_ref())?;
                let program = self.compile_table_update(update)?;
                self.execute_table_program(&program, params)?;
                self.persist_if_needed()?;
                *self.last_changes.borrow_mut() = affected;
                Ok(Vec::new())
            }
            Statement::Delete(ref delete) => {
                let affected =
                    self.count_matching_rows(&delete.table.name, delete.where_clause.as_ref())?;
                let program = self.compile_table_delete(delete)?;
                self.execute_table_program(&program, params)?;
                self.persist_if_needed()?;
                *self.last_changes.borrow_mut() = affected;
                Ok(Vec::new())
            }
            Statement::Begin(begin) => {
                self.execute_begin(begin)?;
                Ok(Vec::new())
            }
            Statement::Commit => {
                self.execute_commit()?;
                self.persist_if_needed()?;
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
            _ => Err(FrankenError::NotImplemented(
                "only SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, and transaction control are supported".to_owned(),
            )),
        }
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
                })
            }
            Statement::Select(select) => {
                let program = self.compile_table_select(select)?;
                Ok(PreparedStatement {
                    program,
                    func_registry: registry,
                    expression_postprocess: None,
                    distinct: is_distinct_select(select),
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
        table_name: &fsqlite_ast::QualifiedName,
        where_clause: Option<&Expr>,
    ) -> Result<usize> {
        let sql = if let Some(cond) = where_clause {
            format!("SELECT * FROM {table_name} WHERE {cond}")
        } else {
            format!("SELECT * FROM {table_name}")
        };
        Ok(self.query(&sql)?.len())
    }

    /// Process a CREATE TABLE statement: register the schema and create the
    /// in-memory table.
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

        let columns = match &create.body {
            CreateTableBody::Columns { columns, .. } => columns,
            CreateTableBody::AsSelect(_) => {
                return Err(FrankenError::NotImplemented(
                    "CREATE TABLE AS SELECT is not supported yet".to_owned(),
                ));
            }
        };

        let col_infos: Vec<ColumnInfo> = columns
            .iter()
            .map(|col| {
                let affinity = col
                    .type_name
                    .as_ref()
                    .map_or('B', |tn| type_name_to_affinity_char(&tn.name));
                ColumnInfo {
                    name: col.name.clone(),
                    affinity,
                }
            })
            .collect();

        let num_columns = col_infos.len();
        let root_page = self.db.borrow_mut().create_table(num_columns);

        self.schema.borrow_mut().push(TableSchema {
            name: table_name,
            root_page,
            columns: col_infos,
            indexes: Vec::new(),
        });

        Ok(())
    }

    // ── Transaction control ──────────────────────────────────────────────

    /// Returns `true` if an explicit transaction is active.
    pub fn in_transaction(&self) -> bool {
        *self.in_transaction.borrow()
    }

    /// Take a snapshot of the current database + schema state.
    fn snapshot(&self) -> DbSnapshot {
        DbSnapshot {
            db: self.db.borrow().clone(),
            schema: self.schema.borrow().clone(),
        }
    }

    /// Restore a snapshot, replacing the current database + schema state.
    fn restore_snapshot(&self, snap: &DbSnapshot) {
        (*self.db.borrow_mut()).clone_from(&snap.db);
        (*self.schema.borrow_mut()).clone_from(&snap.schema);
    }

    /// Handle BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE].
    fn execute_begin(&self, _begin: fsqlite_ast::BeginStatement) -> Result<()> {
        if *self.in_transaction.borrow() {
            return Err(FrankenError::Internal(
                "cannot start a transaction within a transaction".to_owned(),
            ));
        }
        *self.txn_snapshot.borrow_mut() = Some(self.snapshot());
        *self.in_transaction.borrow_mut() = true;
        Ok(())
    }

    /// Handle COMMIT.
    fn execute_commit(&self) -> Result<()> {
        if !*self.in_transaction.borrow() {
            return Err(FrankenError::Internal(
                "cannot commit - no transaction is active".to_owned(),
            ));
        }
        // Discard rollback snapshot and savepoints — changes are committed.
        *self.txn_snapshot.borrow_mut() = None;
        self.savepoints.borrow_mut().clear();
        *self.in_transaction.borrow_mut() = false;
        Ok(())
    }

    /// Handle ROLLBACK [TO SAVEPOINT name].
    fn execute_rollback(&self, rb: &fsqlite_ast::RollbackStatement) -> Result<()> {
        if let Some(ref sp_name) = rb.to_savepoint {
            // ROLLBACK TO SAVEPOINT: restore to the named savepoint's snapshot
            // but keep the savepoint (don't pop it).
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
            let snap = self.txn_snapshot.borrow().clone();
            if let Some(snap) = &snap {
                self.restore_snapshot(snap);
            }
            *self.txn_snapshot.borrow_mut() = None;
            self.savepoints.borrow_mut().clear();
            *self.in_transaction.borrow_mut() = false;
        }
        Ok(())
    }

    /// Handle SAVEPOINT name.
    #[allow(clippy::unnecessary_wraps)] // will return errors once pager is wired
    fn execute_savepoint(&self, name: &str) -> Result<()> {
        // If no explicit transaction, implicitly begin one.
        if !*self.in_transaction.borrow() {
            *self.txn_snapshot.borrow_mut() = Some(self.snapshot());
            *self.in_transaction.borrow_mut() = true;
        }
        self.savepoints.borrow_mut().push(SavepointEntry {
            name: name.to_owned(),
            snapshot: self.snapshot(),
        });
        Ok(())
    }

    /// Handle RELEASE \[SAVEPOINT\] name.
    fn execute_release(&self, name: &str) -> Result<()> {
        let mut savepoints = self.savepoints.borrow_mut();
        let idx = savepoints
            .iter()
            .rposition(|e| e.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| FrankenError::Internal(format!("no such savepoint: {name}")))?;
        // RELEASE removes the named savepoint and all savepoints created after it.
        savepoints.truncate(idx);
        // If no savepoints remain and we started implicitly, end the transaction.
        if savepoints.is_empty() && self.txn_snapshot.borrow().is_some() {
            drop(savepoints);
            // Implicit transaction via savepoint: commit on final release.
            // (If the user did explicit BEGIN, they still need COMMIT.)
        }
        Ok(())
    }

    // ── Compilation helpers ─────────────────────────────────────────────

    /// Compile a table-backed SELECT through the VDBE codegen.
    fn compile_table_select(&self, select: &SelectStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let ctx = CodegenContext::default();
        codegen_select(&mut builder, select, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Execute a GROUP BY aggregate SELECT via post-execution processing.
    ///
    /// 1. Compile and execute a `SELECT *` scan (no aggregates)
    /// 2. Group rows by GROUP BY key columns
    /// 3. Compute aggregates per group
    #[allow(clippy::too_many_lines)]
    fn execute_group_by_select(
        &self,
        select: &SelectStatement,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        if select.with.is_some() {
            return Err(FrankenError::NotImplemented(
                "WITH is not supported with GROUP BY in this connection path".to_owned(),
            ));
        }
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
        if having.is_some() {
            return Err(FrankenError::NotImplemented(
                "HAVING is not supported in this GROUP BY connection path".to_owned(),
            ));
        }
        if !windows.is_empty() {
            return Err(FrankenError::NotImplemented(
                "WINDOW is not supported in this GROUP BY connection path".to_owned(),
            ));
        }

        // Find the table name from the FROM clause.
        let table_name = match &select.body.select {
            SelectCore::Select {
                from: Some(from), ..
            } => match &from.source {
                fsqlite_ast::TableOrSubquery::Table { name, .. } => name.name.clone(),
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

        // Resolve GROUP BY key column indices.
        let group_key_indices: Vec<usize> = group_by_exprs
            .iter()
            .map(|expr| {
                let col_name = expr_col_name(expr).ok_or_else(|| {
                    FrankenError::NotImplemented("GROUP BY with non-column expression".to_owned())
                })?;
                table_schema.column_index(col_name).ok_or_else(|| {
                    FrankenError::Internal(format!("GROUP BY column not found: {col_name}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Parse result columns into GroupByColumn descriptors.
        let result_descriptors: Vec<GroupByColumn> = columns
            .iter()
            .map(|col| match col {
                ResultColumn::Expr {
                    expr: Expr::FunctionCall { name, args, .. },
                    ..
                } if is_agg_fn(name) => {
                    let func = name.to_ascii_lowercase();
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
                            Some(table_schema.column_index(col_name).ok_or_else(|| {
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
                    })
                }
                ResultColumn::Expr { expr, .. } => {
                    let col_name = expr_col_name(expr).ok_or_else(|| {
                        FrankenError::NotImplemented(
                            "GROUP BY with non-column non-aggregate expression".to_owned(),
                        )
                    })?;
                    let idx = table_schema.column_index(col_name).ok_or_else(|| {
                        FrankenError::Internal(format!("result column not found: {col_name}"))
                    })?;
                    if !group_key_indices.contains(&idx) {
                        return Err(FrankenError::NotImplemented(format!(
                            "non-aggregate result column '{col_name}' must appear in GROUP BY"
                        )));
                    }
                    Ok(GroupByColumn::Plain(idx))
                }
                ResultColumn::Star | ResultColumn::TableStar(_) => Err(
                    FrankenError::NotImplemented("SELECT * with GROUP BY".to_owned()),
                ),
            })
            .collect::<Result<Vec<_>>>()?;

        drop(schema);

        // Compile and execute a raw SELECT * scan (no aggregates, no GROUP BY).
        let raw_select = build_raw_scan_select(select);
        let program = self.compile_table_select(&raw_select)?;
        let raw_rows = self.execute_table_program(&program, params)?;

        // Group rows by key columns.
        let mut groups: Vec<(Vec<SqliteValue>, Vec<Vec<SqliteValue>>)> = Vec::new();
        for row in &raw_rows {
            let key: Vec<SqliteValue> = group_key_indices
                .iter()
                .map(|&idx| row.get(idx).cloned().unwrap_or(SqliteValue::Null))
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
                    GroupByColumn::Plain(col_idx) => {
                        // Use the value from the first row in the group.
                        values.push(
                            group_rows
                                .first()
                                .and_then(|r| r.get(*col_idx))
                                .cloned()
                                .unwrap_or(SqliteValue::Null),
                        );
                    }
                    GroupByColumn::Agg { name, arg_col } => {
                        if name == "count" && arg_col.is_none() {
                            #[allow(clippy::cast_possible_wrap)]
                            values.push(SqliteValue::Integer(group_rows.len() as i64));
                        } else {
                            let Some(idx) = *arg_col else {
                                return Err(FrankenError::NotImplemented(format!(
                                    "aggregate {name} requires a column argument in this GROUP BY connection path"
                                )));
                            };
                            let agg_values: Vec<&SqliteValue> = group_rows
                                .iter()
                                .filter_map(|r| r.get(idx))
                                .filter(|v| !matches!(v, SqliteValue::Null))
                                .collect();
                            values.push(compute_aggregate(name, &agg_values));
                        }
                    }
                }
            }
            result.push(Row { values });
        }

        // Post-process: ORDER BY.
        if !select.order_by.is_empty() {
            sort_rows_by_order_terms(&mut result, &select.order_by, columns)?;
        }

        // Post-process: LIMIT / OFFSET.
        if let Some(ref limit_clause) = select.limit {
            apply_limit_offset_postprocess(&mut result, limit_clause);
        }

        Ok(result)
    }

    /// Compile an INSERT through the VDBE codegen.
    fn compile_table_insert(&self, insert: &fsqlite_ast::InsertStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let ctx = CodegenContext::default();
        codegen_insert(&mut builder, insert, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Compile an UPDATE through the VDBE codegen.
    fn compile_table_update(&self, update: &fsqlite_ast::UpdateStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let ctx = CodegenContext::default();
        codegen_update(&mut builder, update, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Compile a DELETE through the VDBE codegen.
    fn compile_table_delete(&self, delete: &fsqlite_ast::DeleteStatement) -> Result<VdbeProgram> {
        let schema = self.schema.borrow();
        let mut builder = ProgramBuilder::new();
        let ctx = CodegenContext::default();
        codegen_delete(&mut builder, delete, &schema, &ctx).map_err(codegen_error_to_franken)?;
        builder.finish()
    }

    /// Execute a VDBE program with the in-memory database attached.
    fn execute_table_program(
        &self,
        program: &VdbeProgram,
        params: Option<&[SqliteValue]>,
    ) -> Result<Vec<Row>> {
        let mut engine = VdbeEngine::new(program.register_count());
        if let Some(params) = params {
            validate_bound_parameters(program, params)?;
            engine.set_bindings(params.to_vec());
        }

        engine.set_function_registry(Arc::clone(&self.func_registry));
        engine.enable_storage_read_cursors(true);

        // Lend the MemDatabase to the engine for the duration of execution.
        let db = self.db.replace(MemDatabase::new());
        engine.set_database(db);

        let outcome = engine.execute(program)?;

        // Take the database back from the engine.
        if let Some(db) = engine.take_database() {
            *self.db.borrow_mut() = db;
        }

        match outcome {
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

    fn load_persisted_state_if_present(&self) -> Result<()> {
        let Some(path) = self.persist_path.as_deref() else {
            return Ok(());
        };
        let path = Path::new(path);
        if !path.exists() {
            return Ok(());
        }
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
        let dump = self.build_persistence_dump()?;
        fsqlite_vfs::host_fs::write(Path::new(path), dump.as_bytes())?;
        Ok(())
    }

    fn build_persistence_dump(&self) -> Result<String> {
        let schema = self.schema.borrow().clone();
        let mut statements = Vec::new();
        for table in &schema {
            statements.push(build_create_table_sql(table));

            let select_sql = build_dump_select_sql(table);
            let rows = self.query(&select_sql)?;
            for row in rows {
                statements.push(build_insert_row_sql(table, row.values()));
            }
        }
        if statements.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!("{}\n", statements.join("\n")))
        }
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
    /// A non-aggregate column reference; stores the column index in the raw row.
    Plain(usize),
    /// An aggregate function; stores (func_name_lower, arg_col_index_or_None_for_star).
    Agg {
        name: String,
        arg_col: Option<usize>,
    },
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

/// Compute the aggregate value for a group of values.
#[allow(clippy::cast_possible_wrap)]
fn compute_aggregate(name: &str, values: &[&SqliteValue]) -> SqliteValue {
    match name {
        "count" => SqliteValue::Integer(values.len() as i64),
        "sum" | "total" => {
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
            SqliteValue::Text(parts.join(","))
        }
        _ => SqliteValue::Null,
    }
}

/// Compare two `SqliteValue`s for ordering (used by min/max aggregates).
fn cmp_sqlite_values(a: &SqliteValue, b: &SqliteValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
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
        _ => Ordering::Equal,
    }
}

/// Sort result rows by ORDER BY terms (for GROUP BY post-processing).
#[allow(dead_code)]
fn sort_rows_by_order_terms(
    rows: &mut [Row],
    order_by: &[OrderingTerm],
    columns: &[ResultColumn],
) -> Result<()> {
    let resolved: Vec<(usize, bool)> = order_by
        .iter()
        .map(|term| {
            let col_name = expr_col_name(&term.expr).ok_or_else(|| {
                FrankenError::NotImplemented(
                    "only column references are supported in GROUP BY ORDER BY".to_owned(),
                )
            })?;
            let idx = columns
                .iter()
                .position(|c| match c {
                    ResultColumn::Expr {
                        expr: Expr::Column(r, _),
                        ..
                    } => r.column.eq_ignore_ascii_case(col_name),
                    ResultColumn::Expr {
                        alias: Some(alias), ..
                    } => alias.eq_ignore_ascii_case(col_name),
                    _ => false,
                })
                .ok_or_else(|| {
                    FrankenError::Internal(format!(
                        "ORDER BY column '{col_name}' not found in SELECT list"
                    ))
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
#[allow(dead_code, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
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
        'd' // INTEGER affinity
    } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
        'C' // TEXT affinity
    } else if upper.contains("BLOB") || upper.is_empty() {
        'B' // BLOB (none) affinity
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        'E' // REAL affinity
    } else {
        'A' // NUMERIC affinity
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

#[allow(clippy::too_many_lines)]
fn compile_expression_select(select: &SelectStatement) -> Result<VdbeProgram> {
    if select.with.is_some() {
        return Err(FrankenError::NotImplemented(
            "WITH is not supported in this connection path".to_owned(),
        ));
    }
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

/// Reconstruct a `CREATE TABLE` statement from a [`TableSchema`].
///
/// Since only the affinity character is stored (not the original SQL type
/// name), we map each affinity back to a canonical type keyword.
fn build_create_table_sql(table: &TableSchema) -> String {
    use std::fmt::Write as _;
    let mut sql = format!("CREATE TABLE \"{}\" (", table.name);
    for (i, col) in table.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let type_kw = affinity_char_to_type(col.affinity);
        let _ = write!(sql, "\"{}\" {type_kw}", col.name);
    }
    sql.push_str(");");
    sql
}

/// Build a `SELECT *` query for dumping all rows from a table.
fn build_dump_select_sql(table: &TableSchema) -> String {
    format!("SELECT * FROM \"{}\";", table.name)
}

/// Build an `INSERT INTO` statement that reproduces a single row.
fn build_insert_row_sql(table: &TableSchema, values: &[SqliteValue]) -> String {
    use std::fmt::Write as _;
    let mut sql = format!("INSERT INTO \"{}\" VALUES (", table.name);
    for (i, val) in values.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        match val {
            SqliteValue::Null => sql.push_str("NULL"),
            SqliteValue::Integer(n) => {
                let _ = write!(sql, "{n}");
            }
            SqliteValue::Float(f) => {
                let _ = write!(sql, "{f:?}");
            }
            SqliteValue::Text(s) => {
                sql.push('\'');
                for ch in s.chars() {
                    if ch == '\'' {
                        sql.push_str("''");
                    } else {
                        sql.push(ch);
                    }
                }
                sql.push('\'');
            }
            SqliteValue::Blob(b) => {
                sql.push_str("X'");
                for byte in b {
                    let _ = write!(sql, "{byte:02X}");
                }
                sql.push('\'');
            }
        }
    }
    sql.push_str(");");
    sql
}

/// Map an affinity character back to a canonical SQL type keyword.
fn affinity_char_to_type(affinity: char) -> &'static str {
    match affinity {
        'd' => "INTEGER",
        'C' => "TEXT",
        'E' => "REAL",
        'A' => "NUMERIC",
        // 'B' (blob) and any unknown affinity default to BLOB.
        _ => "BLOB",
    }
}

#[cfg(test)]
mod tests {
    use super::{Connection, Row};
    use fsqlite_ast::Statement;
    use fsqlite_error::FrankenError;
    use fsqlite_types::opcode::{Opcode, P4};
    use fsqlite_types::value::SqliteValue;
    use fsqlite_vdbe::engine::{ExecOutcome, VdbeEngine};

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
        let db_mem = conn.db.borrow().clone();
        let mut engine = VdbeEngine::new(program.register_count());
        engine.set_database(db_mem);
        let outcome = engine.execute(&program).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        let mem_rows = engine.take_results();

        // Execute once with storage-backed read cursors enabled.
        let db_storage = conn.db.borrow().clone();
        let mut engine = VdbeEngine::new(program.register_count());
        engine.enable_storage_read_cursors(true);
        engine.set_database(db_storage);
        let outcome = engine.execute(&program).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        let storage_rows = engine.take_results();

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
}
