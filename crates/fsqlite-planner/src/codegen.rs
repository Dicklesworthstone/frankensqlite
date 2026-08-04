//! AST-to-VDBE bytecode compilation (§10.6).
//!
//! Translates parsed SQL statements into VDBE register-based instructions
//! using the `ProgramBuilder` from `fsqlite-types`. Handles SELECT, INSERT,
//! UPDATE, and DELETE with correct opcode patterns matching C SQLite behavior.

use fsqlite_ast::{
    BinaryOp as AstBinaryOp, ColumnRef, ConflictAction, DeleteStatement, Expr, FunctionArgs,
    InsertSource, InsertStatement, Literal, PlaceholderType, QualifiedTableRef, ResultColumn,
    SelectCore, SelectStatement, Span, UnaryOp as AstUnaryOp, UpdateStatement,
};
use fsqlite_parser::expr::parse_expr as parse_sql_expr;
use fsqlite_types::SqliteValue;
use fsqlite_types::opcode::{Label, Opcode, P4, ProgramBuilder};

// ---------------------------------------------------------------------------
// INSERT conflict-mode p5 flags (must match fsqlite-vdbe/src/codegen.rs)
// ---------------------------------------------------------------------------

/// ROLLBACK on conflict.
const OE_ROLLBACK: u16 = 1;
/// ABORT on conflict (default).
const OE_ABORT: u16 = 2;
/// FAIL on conflict.
const OE_FAIL: u16 = 3;
/// IGNORE conflicting row.
const OE_IGNORE: u16 = 4;
/// REPLACE conflicting row.
const OE_REPLACE: u16 = 5;

/// Convert AST `ConflictAction` to p5 OE_* flag value.
fn conflict_action_to_oe(action: Option<&ConflictAction>) -> u16 {
    match action {
        Some(ConflictAction::Rollback) => OE_ROLLBACK,
        None | Some(ConflictAction::Abort) => OE_ABORT,
        Some(ConflictAction::Fail) => OE_FAIL,
        Some(ConflictAction::Ignore) => OE_IGNORE,
        Some(ConflictAction::Replace) => OE_REPLACE,
    }
}

// ---------------------------------------------------------------------------
// Schema metadata (minimal info needed for codegen)
// ---------------------------------------------------------------------------

/// Column metadata needed by the code generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Type affinity character: 'd' (integer), 'e' (real), 'B' (blob),
    /// 'C' (text), 'A' (numeric). Lowercase = exact, uppercase = heuristic.
    pub affinity: char,
    /// Default value expression as SQL text (e.g. "'open'", "0").
    pub default_value: Option<String>,
}

/// Index metadata needed for codegen (index-scan SELECT).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSchema {
    /// Index name.
    pub name: String,
    /// Root page number.
    pub root_page: i32,
    /// Indexed column names (leftmost first).
    pub columns: Vec<String>,
    /// Whether this index enforces a UNIQUE constraint.
    pub is_unique: bool,
}

/// Minimal table schema needed by the code generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    /// Table name.
    pub name: String,
    /// Root page of the table's B-tree.
    pub root_page: i32,
    /// Column definitions in storage order.
    pub columns: Vec<ColumnInfo>,
    /// Available indexes.
    pub indexes: Vec<IndexSchema>,
}

impl TableSchema {
    /// Build an affinity string for `MakeRecord` (one char per column).
    #[must_use]
    pub fn affinity_string(&self) -> String {
        self.columns.iter().map(|c| c.affinity).collect()
    }

    /// Find a column's 0-based index by name (case-insensitive).
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Find an index by a column name (returns first index whose leftmost
    /// column matches).
    #[must_use]
    pub fn index_for_column(&self, col_name: &str) -> Option<&IndexSchema> {
        self.indexes.iter().find(|idx| {
            idx.columns
                .first()
                .is_some_and(|c| c.eq_ignore_ascii_case(col_name))
        })
    }
}

/// Configuration for the code generator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodegenContext {
    /// Whether we're in `BEGIN CONCURRENT` mode.
    /// When true, `OP_NewRowid` uses the snapshot-independent allocator.
    pub concurrent_mode: bool,
}

/// Errors during code generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodegenError {
    /// Table not found in schema.
    TableNotFound(String),
    /// Column not found in table.
    ColumnNotFound { table: String, column: String },
    /// Unsupported AST construct for this codegen pass.
    Unsupported(String),
}

impl std::fmt::Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(name) => write!(f, "table not found: {name}"),
            Self::ColumnNotFound { table, column } => {
                write!(f, "column {column} not found in table {table}")
            }
            Self::Unsupported(msg) => write!(f, "unsupported: {msg}"),
        }
    }
}

impl std::error::Error for CodegenError {}

// ---------------------------------------------------------------------------
// Schema lookup helper
// ---------------------------------------------------------------------------

fn find_table<'a>(schema: &'a [TableSchema], name: &str) -> Result<&'a TableSchema, CodegenError> {
    schema
        .iter()
        .find(|t| t.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| CodegenError::TableNotFound(name.to_owned()))
}

fn table_name_from_qualified(qtr: &QualifiedTableRef) -> &str {
    &qtr.name.name
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindParamRef {
    Anonymous,
    Numbered(i32),
}

// ---------------------------------------------------------------------------
// SELECT codegen
// ---------------------------------------------------------------------------

/// Generate VDBE bytecode for a SELECT statement.
///
/// Handles three patterns:
/// 1. **Expression-only**: `SELECT 1+1`, `SELECT abs(-5)` (no FROM)
/// 2. **Rowid lookup**: `SELECT cols FROM t WHERE rowid = ?`
/// 3. **Full table scan**: `SELECT cols FROM t`
///
/// Returns the cursor number used (for composability).
#[allow(clippy::too_many_lines)]
pub fn codegen_select(
    b: &mut ProgramBuilder,
    stmt: &SelectStatement,
    schema: &[TableSchema],
    _ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let core = match &stmt.body.select {
        SelectCore::Select { .. } => &stmt.body.select,
        SelectCore::Values(rows) => return codegen_select_values(b, rows),
    };

    let (columns, from, where_clause) = match core {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            ..
        } => (columns, from, where_clause),
        SelectCore::Values(_) => unreachable!(),
    };

    // Handle SELECT without FROM (expression-only queries like SELECT 1+1).
    if from.is_none() {
        return codegen_select_no_from(b, columns);
    }
    let from_clause = from.as_ref().expect("checked above");

    let table_name = single_table_select_source_name(&from_clause.source)?;

    let table = find_table(schema, table_name)?;
    let cursor = 0_i32;

    // Labels for control flow.
    let end_label = b.emit_label();
    let done_label = b.emit_label();

    // Init: jump to end (standard SQLite pattern).
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (read-only, p2=0).
    b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);

    // Determine output columns and allocate registers.
    let out_col_count = result_column_count(columns, table);
    let out_regs = b.alloc_regs(out_col_count);

    // Check for rowid-equality WHERE clause.
    let rowid_param = extract_rowid_bind_param(where_clause.as_deref());
    // Check for index-usable WHERE clause.
    let index_eq = if rowid_param.is_none() {
        extract_column_eq_bind(where_clause.as_deref())
    } else {
        None
    };
    if where_clause.is_some() && rowid_param.is_none() && index_eq.is_none() {
        return Err(CodegenError::Unsupported(
            "SELECT WHERE currently supports only `rowid = ?` or `indexed_col = ?`".to_owned(),
        ));
    }

    let mut index_cursor_to_close: Option<i32> = None;

    if let Some(param_idx) = rowid_param {
        // --- Rowid-seek SELECT ---
        let rowid_reg = b.alloc_reg();
        b.emit_op(Opcode::Variable, param_idx, rowid_reg, 0, P4::None, 0);
        b.emit_op(
            Opcode::OpenRead,
            cursor,
            table.root_page,
            0,
            P4::Table(table.name.clone()),
            0,
        );
        b.emit_jump_to_label(
            Opcode::SeekRowid,
            cursor,
            rowid_reg,
            done_label,
            P4::None,
            0,
        );

        // Read columns.
        emit_column_reads(b, cursor, columns, table, out_regs)?;

        // ResultRow.
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    } else if let Some((col_name, param_idx)) = &index_eq {
        // --- Index-seek SELECT ---
        let Some(idx_schema) = table.index_for_column(col_name) else {
            return codegen_select_column_eq_scan(
                b,
                cursor,
                table,
                columns,
                out_regs,
                out_col_count,
                done_label,
                end_label,
                col_name,
                *param_idx,
            );
        };
        let idx_cursor = 1_i32;
        index_cursor_to_close = Some(idx_cursor);
        let param_reg = b.alloc_reg();
        b.emit_op(Opcode::Variable, *param_idx, param_reg, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::IsNull, param_reg, 0, done_label, P4::None, 0);

        // Build probe key `[value, i64::MIN]` so SeekGE lands at first duplicate.
        let min_rowid_reg = b.alloc_reg();
        b.emit_op(Opcode::Int64, 0, min_rowid_reg, 0, P4::Int64(i64::MIN), 0);
        let probe_key_reg = b.alloc_reg();
        b.emit_op(Opcode::MakeRecord, param_reg, 2, probe_key_reg, P4::None, 0);

        b.emit_op(
            Opcode::OpenRead,
            cursor,
            table.root_page,
            0,
            P4::Table(table.name.clone()),
            0,
        );
        b.emit_op(
            Opcode::OpenRead,
            idx_cursor,
            idx_schema.root_page,
            0,
            P4::Index(idx_schema.name.clone()),
            0,
        );
        b.emit_jump_to_label(
            Opcode::SeekGE,
            idx_cursor,
            probe_key_reg,
            done_label,
            P4::None,
            0,
        );

        let loop_start = b.current_addr();
        b.emit_jump_to_label(
            Opcode::IdxGT,
            idx_cursor,
            probe_key_reg,
            done_label,
            P4::None,
            1,
        );

        let rowid_reg = b.alloc_reg();
        b.emit_op(Opcode::IdxRowid, idx_cursor, rowid_reg, 0, P4::None, 0);
        let skip_row_label = b.emit_label();
        b.emit_jump_to_label(
            Opcode::SeekRowid,
            cursor,
            rowid_reg,
            skip_row_label,
            P4::None,
            0,
        );

        // Read columns.
        emit_column_reads(b, cursor, columns, table, out_regs)?;

        // ResultRow.
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
        b.resolve_label(skip_row_label);

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let loop_target = loop_start as i32;
        b.emit_op(Opcode::Next, idx_cursor, loop_target, 0, P4::None, 0);
    } else {
        // --- Full table scan ---
        return codegen_select_full_scan(
            b,
            cursor,
            table,
            columns,
            out_regs,
            out_col_count,
            done_label,
            end_label,
        );
    }

    // Done: Close + Halt.
    b.resolve_label(done_label);
    if let Some(idx_cursor) = index_cursor_to_close {
        b.emit_op(Opcode::Close, idx_cursor, 0, 0, P4::None, 0);
    }
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

/// Codegen for a top-level `VALUES (..), (..)` SELECT core.
fn codegen_select_values(b: &mut ProgramBuilder, rows: &[Vec<Expr>]) -> Result<(), CodegenError> {
    let end_label = b.emit_label();

    // Init: jump to end.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Read-only transaction.
    b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);

    let Some(first_row) = rows.first() else {
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);
        return Ok(());
    };

    let out_col_count = i32::try_from(first_row.len())
        .map_err(|_| CodegenError::Unsupported("VALUES row has too many columns".to_owned()))?;

    if rows.iter().any(|row| row.len() != first_row.len()) {
        return Err(CodegenError::Unsupported(
            "VALUES rows must have the same arity".to_owned(),
        ));
    }

    let out_regs = b.alloc_regs(out_col_count);
    for row in rows {
        for (reg, expr) in (out_regs..).zip(row.iter()) {
            emit_expr(b, expr, reg)?;
        }
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    }

    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

fn single_table_select_source_name(
    source: &fsqlite_ast::TableOrSubquery,
) -> Result<&str, CodegenError> {
    match source {
        fsqlite_ast::TableOrSubquery::Table { name, .. } => Ok(&name.name),
        fsqlite_ast::TableOrSubquery::ParenJoin(inner) if inner.joins.is_empty() => {
            single_table_select_source_name(&inner.source)
        }
        fsqlite_ast::TableOrSubquery::ParenJoin(_) => Err(CodegenError::Unsupported(
            "parenthesized JOIN source in single-table SELECT".to_owned(),
        )),
        fsqlite_ast::TableOrSubquery::Subquery { .. } => Err(CodegenError::Unsupported(
            "subquery FROM source in single-table SELECT".to_owned(),
        )),
        fsqlite_ast::TableOrSubquery::TableFunction { .. } => Err(CodegenError::Unsupported(
            "table-valued function FROM source in single-table SELECT".to_owned(),
        )),
    }
}

/// Codegen for a full table scan SELECT.
#[allow(clippy::too_many_arguments)]
fn codegen_select_full_scan(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    columns: &[ResultColumn],
    out_regs: i32,
    out_col_count: i32,
    done_label: Label,
    end_label: Label,
) -> Result<(), CodegenError> {
    b.emit_op(
        Opcode::OpenRead,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // Rewind to first row; jump to done if table is empty.
    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, cursor, 0, done_label, P4::None, 0);

    // Read columns.
    emit_column_reads(b, cursor, columns, table, out_regs)?;

    // ResultRow.
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // Next: jump back to start of loop body (the instruction after Rewind).
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Next, cursor, loop_body, 0, P4::None, 0);

    // Done: Close + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

/// Codegen for `SELECT ... FROM table WHERE column = ?` when no usable index exists.
#[allow(
    clippy::too_many_arguments,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn codegen_select_column_eq_scan(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    columns: &[ResultColumn],
    out_regs: i32,
    out_col_count: i32,
    done_label: Label,
    end_label: Label,
    filter_column: &str,
    param_idx: i32,
) -> Result<(), CodegenError> {
    let filter_col_idx =
        table
            .column_index(filter_column)
            .ok_or_else(|| CodegenError::ColumnNotFound {
                table: table.name.clone(),
                column: filter_column.to_owned(),
            })?;
    let param_reg = b.alloc_reg();
    let value_reg = b.alloc_reg();
    b.emit_op(Opcode::Variable, param_idx, param_reg, 0, P4::None, 0);
    b.emit_op(
        Opcode::OpenRead,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, cursor, 0, done_label, P4::None, 0);

    let skip_row_label = b.emit_label();
    b.emit_op(
        Opcode::Column,
        cursor,
        filter_col_idx as i32,
        value_reg,
        P4::None,
        0,
    );
    b.emit_jump_to_label(
        Opcode::Ne,
        param_reg,
        value_reg,
        skip_row_label,
        P4::None,
        0x10,
    );
    emit_column_reads(b, cursor, columns, table, out_regs)?;
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    b.resolve_label(skip_row_label);

    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Next, cursor, loop_body, 0, P4::None, 0);

    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    b.resolve_label(end_label);

    Ok(())
}

/// Codegen for `SELECT <expr>, ...` without a FROM clause.
///
/// Examples: `SELECT 1+1`, `SELECT abs(-5)`, `SELECT CURRENT_TIMESTAMP`.
/// Produces exactly one result row with the evaluated expressions.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn codegen_select_no_from(
    b: &mut ProgramBuilder,
    columns: &[ResultColumn],
) -> Result<(), CodegenError> {
    // Star or TableStar don't make sense without a table.
    for col in columns {
        if matches!(col, ResultColumn::Star | ResultColumn::TableStar(_)) {
            return Err(CodegenError::Unsupported(
                "SELECT * without FROM".to_owned(),
            ));
        }
    }

    let out_col_count = columns.len() as i32;
    let end_label = b.emit_label();

    // Init: jump to end.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Read-only transaction.
    b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);

    // Allocate output registers and evaluate each expression.
    let out_regs = b.alloc_regs(out_col_count);
    for (reg, col) in (out_regs..).zip(columns.iter()) {
        if let ResultColumn::Expr { expr, .. } = col {
            emit_expr(b, expr, reg)?;
        }
    }

    // Emit a single result row.
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // Halt.
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

// ---------------------------------------------------------------------------
// INSERT codegen
// ---------------------------------------------------------------------------

/// Generate VDBE bytecode for an INSERT statement.
///
/// Pattern: `INSERT INTO t VALUES (?, ?, ...)`
///
/// Init → Transaction(write) → OpenWrite → NewRowid → Variable* →
/// MakeRecord → Insert → Close → Halt
pub fn codegen_insert(
    b: &mut ProgramBuilder,
    stmt: &InsertStatement,
    schema: &[TableSchema],
    ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let table = find_table(schema, &stmt.table.name)?;
    let cursor = 0_i32;

    let end_label = b.emit_label();

    // Init.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (write, p2=1).
    b.emit_op(Opcode::Transaction, 0, 1, 0, P4::None, 0);

    // OpenWrite.
    b.emit_op(
        Opcode::OpenWrite,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    let oe_flag = conflict_action_to_oe(stmt.or_conflict.as_ref());

    match &stmt.source {
        InsertSource::Values(rows) => {
            if rows.is_empty() {
                return Err(CodegenError::Unsupported("empty VALUES".to_owned()));
            }
            codegen_insert_values(
                b,
                rows,
                &stmt.columns,
                cursor,
                table,
                &stmt.returning,
                ctx,
                oe_flag,
            )?;
        }
        InsertSource::Select(select_stmt) => {
            codegen_insert_select(
                b,
                select_stmt,
                &stmt.columns,
                cursor,
                table,
                schema,
                &stmt.returning,
                ctx,
                oe_flag,
            )?;
        }
        InsertSource::DefaultValues => {
            codegen_insert_default_values(b, cursor, table, &stmt.returning, ctx, oe_flag)?;
        }
    }

    // Close + Halt.
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

    Ok(())
}

fn insert_target_indices(
    insert_columns: &[String],
    table: &TableSchema,
) -> Result<Vec<usize>, CodegenError> {
    if insert_columns.is_empty() {
        return Ok((0..table.columns.len()).collect());
    }

    insert_columns
        .iter()
        .map(|column| {
            table
                .column_index(column)
                .ok_or_else(|| CodegenError::ColumnNotFound {
                    table: table.name.clone(),
                    column: column.clone(),
                })
        })
        .collect()
}

fn insert_default_exprs(table: &TableSchema) -> Result<Vec<Expr>, CodegenError> {
    table
        .columns
        .iter()
        .map(|col| default_value_to_expr(table, col))
        .collect()
}

/// Convert a column's DEFAULT value SQL text to an AST expression.
fn default_value_to_expr(table: &TableSchema, col: &ColumnInfo) -> Result<Expr, CodegenError> {
    let span = Span::ZERO;
    let Some(dv) = col.default_value.as_deref() else {
        return Ok(Expr::Literal(Literal::Null, span));
    };
    parse_default_expr(dv).map_err(|err| {
        CodegenError::Unsupported(format!(
            "failed to parse DEFAULT expression `{}` for {}.{}: {err}",
            dv.trim(),
            table.name,
            col.name
        ))
    })
}

/// Parse column DEFAULT SQL text into an expression AST.
fn parse_default_expr(default_sql: &str) -> Result<Expr, fsqlite_parser::ParseError> {
    let trimmed = default_sql.trim();
    parse_sql_expr(trimmed)
}

fn expand_insert_values_row(
    row_values: &[Expr],
    insert_columns: &[String],
    table: &TableSchema,
) -> Result<Vec<Expr>, CodegenError> {
    if insert_columns.is_empty() {
        return Ok(row_values.to_vec());
    }

    let target_indices = insert_target_indices(insert_columns, table)?;
    if row_values.len() != target_indices.len() {
        return Err(CodegenError::Unsupported(format!(
            "INSERT VALUES column count mismatch: {} expressions for {} target columns",
            row_values.len(),
            target_indices.len(),
        )));
    }

    let mut expanded = insert_default_exprs(table)?;
    let mut filled_targets = vec![false; table.columns.len()];
    for (source_idx, target_idx) in target_indices.into_iter().enumerate() {
        if filled_targets[target_idx] {
            continue;
        }
        expanded[target_idx] = row_values[source_idx].clone();
        filled_targets[target_idx] = true;
    }
    Ok(expanded)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_insert_target_regs_from_source(
    b: &mut ProgramBuilder,
    source_reg_base: i32,
    source_count: usize,
    insert_columns: &[String],
    target_table: &TableSchema,
) -> Result<(i32, i32), CodegenError> {
    if insert_columns.is_empty() {
        let count = i32::try_from(source_count)
            .map_err(|_| CodegenError::Unsupported("too many INSERT source columns".to_owned()))?;
        return Ok((source_reg_base, count));
    }

    let target_indices = insert_target_indices(insert_columns, target_table)?;
    if source_count != target_indices.len() {
        return Err(CodegenError::Unsupported(format!(
            "INSERT source column count mismatch: {source_count} values for {} target columns",
            target_indices.len(),
        )));
    }

    let target_count = i32::try_from(target_table.columns.len())
        .map_err(|_| CodegenError::Unsupported("too many target columns".to_owned()))?;
    let val_regs = b.alloc_regs(target_count);
    let default_exprs = insert_default_exprs(target_table)?;
    let mut source_for_target = vec![None; target_table.columns.len()];
    for (source_idx, target_idx) in target_indices.into_iter().enumerate() {
        source_for_target[target_idx].get_or_insert(source_idx);
    }

    for (target_idx, default_expr) in default_exprs.iter().enumerate() {
        let target_reg = val_regs + i32::try_from(target_idx).unwrap_or(0);
        if let Some(source_idx) = source_for_target[target_idx] {
            let source_reg = source_reg_base + i32::try_from(source_idx).unwrap_or(0);
            b.emit_op(Opcode::Copy, source_reg, target_reg, 0, P4::None, 0);
        } else {
            emit_expr(b, default_expr, target_reg)?;
        }
    }

    Ok((val_regs, target_count))
}

/// Emit the INSERT loop for `VALUES (row), (row), ...` (planner path).
#[allow(
    clippy::too_many_arguments,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unnecessary_wraps
)]
fn codegen_insert_values(
    b: &mut ProgramBuilder,
    rows: &[Vec<Expr>],
    insert_columns: &[String],
    cursor: i32,
    table: &TableSchema,
    returning: &[ResultColumn],
    ctx: &CodegenContext,
    oe_flag: u16,
) -> Result<(), CodegenError> {
    let rowid_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);

    let mut param_idx = 1_i32;

    for row_values in rows {
        let expanded_values = expand_insert_values_row(row_values, insert_columns, table)?;
        let n_cols = expanded_values.len();
        let val_regs = b.alloc_regs(n_cols as i32);
        b.emit_op(
            Opcode::NewRowid,
            cursor,
            rowid_reg,
            concurrent_flag,
            P4::None,
            0,
        );

        for (i, val_expr) in expanded_values.iter().enumerate() {
            let reg = val_regs + i as i32;
            match val_expr {
                Expr::Placeholder(pt, _) => {
                    #[allow(clippy::cast_possible_wrap)]
                    let idx = if let fsqlite_ast::PlaceholderType::Numbered(n) = pt {
                        *n as i32
                    } else {
                        let p = param_idx;
                        param_idx += 1;
                        p
                    };
                    b.emit_op(Opcode::Variable, idx, reg, 0, P4::None, 0);
                }
                _ => {
                    // All other expressions (literals, arithmetic, function
                    // calls, CASE, CAST, etc.) are handled by emit_expr.
                    emit_expr(b, val_expr, reg)?;
                }
            }
        }

        let rec_reg = b.alloc_reg();
        let n_cols_i32 = n_cols as i32;
        b.emit_op(
            Opcode::MakeRecord,
            val_regs,
            n_cols_i32,
            rec_reg,
            P4::Affinity(table.affinity_string()),
            0,
        );

        b.emit_op(
            Opcode::Insert,
            cursor,
            rec_reg,
            rowid_reg,
            P4::Table(table.name.clone()),
            oe_flag,
        );

        if !returning.is_empty() {
            b.emit_op(Opcode::ResultRow, rowid_reg, 1, 0, P4::None, 0);
        }
    }

    Ok(())
}

/// Emit an INSERT with DEFAULT VALUES.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unnecessary_wraps
)]
fn codegen_insert_default_values(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    returning: &[ResultColumn],
    ctx: &CodegenContext,
    oe_flag: u16,
) -> Result<(), CodegenError> {
    let rowid_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);
    let n_cols = table.columns.len() as i32;

    b.emit_op(
        Opcode::NewRowid,
        cursor,
        rowid_reg,
        concurrent_flag,
        P4::None,
        0,
    );

    // Allocate registers for columns and evaluate each declared default.
    let val_regs = b.alloc_regs(n_cols);
    let default_exprs = insert_default_exprs(table)?;
    for (idx, default_expr) in default_exprs.iter().enumerate() {
        let reg = val_regs + idx as i32;
        emit_expr(b, default_expr, reg)?;
    }

    let rec_reg = b.alloc_reg();
    b.emit_op(
        Opcode::MakeRecord,
        val_regs,
        n_cols,
        rec_reg,
        P4::Affinity(table.affinity_string()),
        0,
    );

    b.emit_op(
        Opcode::Insert,
        cursor,
        rec_reg,
        rowid_reg,
        P4::Table(table.name.clone()),
        oe_flag,
    );

    if !returning.is_empty() {
        b.emit_op(Opcode::ResultRow, rowid_reg, 1, 0, P4::None, 0);
    }

    Ok(())
}

/// Emit the INSERT loop for `INSERT INTO target SELECT ... FROM source` (planner path).
///
/// Opens the source table for reading, scans all rows, and inserts each
/// into the target table.
#[allow(
    clippy::too_many_arguments,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn codegen_insert_select(
    b: &mut ProgramBuilder,
    select_stmt: &SelectStatement,
    insert_columns: &[String],
    write_cursor: i32,
    target_table: &TableSchema,
    schema: &[TableSchema],
    returning: &[ResultColumn],
    ctx: &CodegenContext,
    oe_flag: u16,
) -> Result<(), CodegenError> {
    if !select_stmt.body.compounds.is_empty() {
        return Err(CodegenError::Unsupported(
            "INSERT ... SELECT with compounds (UNION, etc.)".to_owned(),
        ));
    }

    // Extract columns, FROM, and WHERE from the inner SELECT.
    let (columns, from, where_clause) = match &select_stmt.body.select {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            ..
        } => (columns, from, where_clause),
        SelectCore::Values(rows) => {
            return codegen_insert_values(
                b,
                rows,
                insert_columns,
                write_cursor,
                target_table,
                returning,
                ctx,
                oe_flag,
            );
        }
    };

    // Handle INSERT ... SELECT <exprs> (no FROM clause).
    let Some(from_clause) = from.as_ref() else {
        return codegen_insert_select_expr_only(
            b,
            columns,
            where_clause.as_deref(),
            insert_columns,
            write_cursor,
            target_table,
            returning,
            ctx,
            oe_flag,
        );
    };

    let src_table_name = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, .. } => &name.name,
        _ => {
            return Err(CodegenError::Unsupported(
                "INSERT ... SELECT from non-table source".to_owned(),
            ));
        }
    };

    let src_table = find_table(schema, src_table_name)?;
    let read_cursor = write_cursor + 1;

    let n_source_cols = result_column_count(columns, src_table);
    let rowid_reg = b.alloc_reg();
    let source_regs = b.alloc_regs(n_source_cols);
    let rec_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);

    let done_label = b.emit_label();

    // OpenRead on source table.
    b.emit_op(
        Opcode::OpenRead,
        read_cursor,
        src_table.root_page,
        0,
        P4::Table(src_table.name.clone()),
        0,
    );

    // Rewind to first row; jump to done if source is empty.
    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, read_cursor, 0, done_label, P4::None, 0);

    // Read projected columns from source into val_regs.
    emit_column_reads(b, read_cursor, columns, src_table, source_regs)?;
    let (val_regs, n_cols) = emit_insert_target_regs_from_source(
        b,
        source_regs,
        n_source_cols as usize,
        insert_columns,
        target_table,
    )?;

    // NewRowid for target table.
    b.emit_op(
        Opcode::NewRowid,
        write_cursor,
        rowid_reg,
        concurrent_flag,
        P4::None,
        0,
    );

    // MakeRecord from the read column values.
    b.emit_op(
        Opcode::MakeRecord,
        val_regs,
        n_cols,
        rec_reg,
        P4::Affinity(target_table.affinity_string()),
        0,
    );

    // Insert into target table.
    b.emit_op(
        Opcode::Insert,
        write_cursor,
        rec_reg,
        rowid_reg,
        P4::Table(target_table.name.clone()),
        oe_flag,
    );

    // RETURNING clause: emit ResultRow with rowid if present.
    if !returning.is_empty() {
        b.emit_op(Opcode::ResultRow, rowid_reg, 1, 0, P4::None, 0);
    }

    // Next: advance to next source row.
    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Next, read_cursor, loop_body, 0, P4::None, 0);

    // Done: close source cursor.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, read_cursor, 0, 0, P4::None, 0);

    Ok(())
}

/// Emit INSERT bytecode for `INSERT INTO target SELECT expr1, expr2, ...`
/// (no FROM clause — expression-only single-row insert).
#[allow(
    clippy::too_many_arguments,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn codegen_insert_select_expr_only(
    b: &mut ProgramBuilder,
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    insert_columns: &[String],
    write_cursor: i32,
    target_table: &TableSchema,
    returning: &[ResultColumn],
    ctx: &CodegenContext,
    oe_flag: u16,
) -> Result<(), CodegenError> {
    // Count output columns: Expr → 1, Star/TableStar → 1 (yields NULL without table).
    let n_source_cols = columns
        .iter()
        .map(|c| match c {
            ResultColumn::Expr { .. } | ResultColumn::Star | ResultColumn::TableStar(_) => 1i32,
        })
        .sum::<i32>();

    if n_source_cols == 0 {
        return Err(CodegenError::Unsupported(
            "INSERT ... SELECT with no columns".to_owned(),
        ));
    }

    let rowid_reg = b.alloc_reg();
    let source_regs = b.alloc_regs(n_source_cols);
    let rec_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);
    let done_label = b.emit_label();

    // Optional WHERE filter (e.g., INSERT INTO t SELECT 1 WHERE condition).
    if let Some(where_expr) = where_clause {
        let filter_reg = b.alloc_reg();
        emit_expr(b, where_expr, filter_reg)?;
        b.emit_jump_to_label(Opcode::IfNot, filter_reg, 1, done_label, P4::None, 0);
    }

    // Evaluate each result column expression into val_regs.
    let mut reg = source_regs;
    for col in columns {
        match col {
            ResultColumn::Expr { expr, .. } => {
                emit_expr(b, expr, reg)?;
                reg += 1;
            }
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                // Star without a table produces NULL.
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                reg += 1;
            }
        }
    }
    let (val_regs, n_cols) = emit_insert_target_regs_from_source(
        b,
        source_regs,
        usize::try_from(n_source_cols).unwrap_or(0),
        insert_columns,
        target_table,
    )?;

    // Auto-generate rowid.
    b.emit_op(
        Opcode::NewRowid,
        write_cursor,
        rowid_reg,
        concurrent_flag,
        P4::None,
        0,
    );

    // MakeRecord from evaluated column values.
    b.emit_op(
        Opcode::MakeRecord,
        val_regs,
        n_cols,
        rec_reg,
        P4::Affinity(target_table.affinity_string()),
        0,
    );

    // Insert into target table.
    b.emit_op(
        Opcode::Insert,
        write_cursor,
        rec_reg,
        rowid_reg,
        P4::Table(target_table.name.clone()),
        oe_flag,
    );

    // RETURNING clause: emit ResultRow with rowid if present.
    if !returning.is_empty() {
        b.emit_op(Opcode::ResultRow, rowid_reg, 1, 0, P4::None, 0);
    }

    b.resolve_label(done_label);
    Ok(())
}

// ---------------------------------------------------------------------------
// UPDATE codegen
// ---------------------------------------------------------------------------

/// Generate VDBE bytecode for an UPDATE statement.
///
/// Pattern: `UPDATE t SET col = ? WHERE rowid = ?`
///
/// Reads ALL existing columns, replaces changed ones, writes back complete
/// record (no partial patches — this is normative per §10.6).
#[allow(clippy::too_many_lines)]
pub fn codegen_update(
    b: &mut ProgramBuilder,
    stmt: &UpdateStatement,
    schema: &[TableSchema],
    _ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let table_name = table_name_from_qualified(&stmt.table);
    let table = find_table(schema, table_name)?;
    let cursor = 0_i32;
    let n_cols = table.columns.len();

    let end_label = b.emit_label();
    let done_label = b.emit_label();

    // Init.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (write).
    b.emit_op(Opcode::Transaction, 0, 1, 0, P4::None, 0);

    // Bind parameters: new values first, then rowid.
    // For UPDATE t SET b = ? WHERE rowid = ?, we have two bind params.
    let mut param_idx = 1_i32;

    // Allocate registers for new values.
    let new_val_regs: Vec<(usize, i32)> = stmt
        .assignments
        .iter()
        .map(|assign| {
            let col_name = match &assign.target {
                fsqlite_ast::AssignmentTarget::Column(name) => name.as_str(),
                fsqlite_ast::AssignmentTarget::ColumnList(_) => {
                    return Err(CodegenError::Unsupported(
                        "multi-column SET (a, b) = (...) assignment is not yet supported"
                            .to_owned(),
                    ));
                }
            };
            let col_idx =
                table
                    .column_index(col_name)
                    .ok_or_else(|| CodegenError::ColumnNotFound {
                        table: table.name.clone(),
                        column: col_name.to_owned(),
                    })?;
            let reg = b.alloc_reg();
            Ok((col_idx, reg))
        })
        .collect::<Result<Vec<_>, CodegenError>>()?;

    // Emit ops for new values.
    for (i, assign) in stmt.assignments.iter().enumerate() {
        let (_col_idx, reg) = new_val_regs[i];
        match &assign.value {
            Expr::Placeholder(pt, _) => {
                #[allow(clippy::cast_possible_wrap)]
                let idx = if let fsqlite_ast::PlaceholderType::Numbered(n) = pt {
                    // Keep param_idx ahead of numbered placeholders so
                    // subsequent anonymous `?` parameters get the next index.
                    param_idx = param_idx.max(*n as i32 + 1);
                    *n as i32
                } else {
                    let p = param_idx;
                    param_idx += 1;
                    p
                };
                b.emit_op(Opcode::Variable, idx, reg, 0, P4::None, 0);
            }
            _ => {
                // All other expressions (literals, arithmetic, function
                // calls, CASE, CAST, etc.) are handled by emit_expr.
                emit_expr(b, &assign.value, reg)?;
            }
        }
    }

    // Rowid bind parameter (required).
    let rowid_bind = extract_rowid_bind(stmt.where_clause.as_ref()).ok_or_else(|| {
        CodegenError::Unsupported("UPDATE currently supports only `WHERE rowid = ?`".to_owned())
    })?;
    let rowid_reg = b.alloc_reg();
    let rowid_param = match rowid_bind {
        BindParamRef::Anonymous => param_idx,
        BindParamRef::Numbered(idx) => idx,
    };
    b.emit_op(Opcode::Variable, rowid_param, rowid_reg, 0, P4::None, 0);

    // OpenWrite.
    b.emit_op(
        Opcode::OpenWrite,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // NotExists: if rowid doesn't exist, jump to done.
    b.emit_jump_to_label(
        Opcode::NotExists,
        cursor,
        rowid_reg,
        done_label,
        P4::None,
        0,
    );

    // Read ALL existing columns into registers.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let col_regs = b.alloc_regs(n_cols as i32);
    for i in 0..n_cols {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        b.emit_op(
            Opcode::Column,
            cursor,
            i as i32,
            col_regs + i as i32,
            P4::None,
            0,
        );
    }

    // Overwrite changed columns with new values.
    for (col_idx, new_reg) in &new_val_regs {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let target = col_regs + *col_idx as i32;
        b.emit_op(Opcode::Copy, *new_reg, target, 0, P4::None, 0);
    }

    // MakeRecord with ALL columns.
    let rec_reg = b.alloc_reg();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let n_cols_i32 = n_cols as i32;
    b.emit_op(
        Opcode::MakeRecord,
        col_regs,
        n_cols_i32,
        rec_reg,
        P4::Affinity(table.affinity_string()),
        0,
    );

    // Insert with REPLACE flag (p5=0x08 in C SQLite, we use 0x08).
    b.emit_op(
        Opcode::Insert,
        cursor,
        rec_reg,
        rowid_reg,
        P4::Table(table.name.clone()),
        0x08, // OPFLAG_ISUPDATE
    );

    // Done: Close + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

    Ok(())
}

// ---------------------------------------------------------------------------
// DELETE codegen
// ---------------------------------------------------------------------------

/// Generate VDBE bytecode for a DELETE statement.
///
/// Pattern: `DELETE FROM t WHERE rowid = ?`
///
/// Init → Transaction(write) → Variable → OpenWrite →
/// NotExists → Delete → Close → Halt
pub fn codegen_delete(
    b: &mut ProgramBuilder,
    stmt: &DeleteStatement,
    schema: &[TableSchema],
    _ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let table_name = table_name_from_qualified(&stmt.table);
    let table = find_table(schema, table_name)?;
    let cursor = 0_i32;

    let end_label = b.emit_label();
    let done_label = b.emit_label();

    // Init.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (write).
    b.emit_op(Opcode::Transaction, 0, 1, 0, P4::None, 0);

    // Bind rowid parameter.
    let rowid_reg = b.alloc_reg();
    b.emit_op(Opcode::Variable, 1, rowid_reg, 0, P4::None, 0);

    // OpenWrite.
    b.emit_op(
        Opcode::OpenWrite,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // NotExists: if rowid not found, skip delete.
    b.emit_jump_to_label(
        Opcode::NotExists,
        cursor,
        rowid_reg,
        done_label,
        P4::None,
        0,
    );

    // Delete at cursor position.
    b.emit_op(
        Opcode::Delete,
        cursor,
        0,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // Done: Close + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Count result columns (handling `SELECT *`).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn result_column_count(columns: &[ResultColumn], table: &TableSchema) -> i32 {
    let mut count = 0i32;
    for col in columns {
        match col {
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                count += table.columns.len() as i32;
            }
            ResultColumn::Expr { .. } => count += 1,
        }
    }
    count
}

/// Emit Column instructions to read result columns into registers.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_column_reads(
    b: &mut ProgramBuilder,
    cursor: i32,
    columns: &[ResultColumn],
    table: &TableSchema,
    base_reg: i32,
) -> Result<(), CodegenError> {
    let mut reg = base_reg;
    for col in columns {
        match col {
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                for i in 0..table.columns.len() {
                    b.emit_op(Opcode::Column, cursor, i as i32, reg, P4::None, 0);
                    reg += 1;
                }
            }
            ResultColumn::Expr { expr, .. } => {
                if let Expr::Column(col_ref, _) = expr {
                    if is_rowid_ref(col_ref) {
                        // Emit OP_Rowid: p1=cursor, p2=target register.
                        b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                    } else {
                        let col_idx = table.column_index(&col_ref.column).ok_or_else(|| {
                            CodegenError::ColumnNotFound {
                                table: table.name.clone(),
                                column: col_ref.column.to_string(),
                            }
                        })?;
                        b.emit_op(Opcode::Column, cursor, col_idx as i32, reg, P4::None, 0);
                    }
                } else {
                    // For non-column expressions (literals, placeholders, etc.),
                    // evaluate the expression directly rather than reading a column.
                    emit_expr(b, expr, reg)?;
                }
                reg += 1;
            }
        }
    }
    Ok(())
}

/// Check if a WHERE clause is a simple `rowid = ?` bind parameter.
///
/// Returns the 1-based bind parameter index if so.
fn extract_rowid_bind_param(where_clause: Option<&Expr>) -> Option<i32> {
    extract_rowid_bind(where_clause).map(|bind| match bind {
        BindParamRef::Anonymous => 1,
        BindParamRef::Numbered(idx) => idx,
    })
}

/// Check if a WHERE clause is a simple `rowid = ?` bind parameter.
fn extract_rowid_bind(where_clause: Option<&Expr>) -> Option<BindParamRef> {
    let expr = where_clause?;
    if let Expr::BinaryOp {
        left,
        op: fsqlite_ast::BinaryOp::Eq,
        right,
        ..
    } = expr
    {
        // Check left = rowid column, right = bind param.
        if is_rowid_expr(left) {
            return bind_param_ref(right);
        }
        if is_rowid_expr(right) {
            return bind_param_ref(left);
        }
    }
    None
}

/// Check if a WHERE clause is `col = ?` for an indexed column.
fn extract_column_eq_bind(where_clause: Option<&Expr>) -> Option<(String, i32)> {
    let expr = where_clause?;
    if let Expr::BinaryOp {
        left,
        op: fsqlite_ast::BinaryOp::Eq,
        right,
        ..
    } = expr
    {
        if let (Some(col_name), Some(param_idx)) = (column_name(left), bind_param_index(right)) {
            return Some((col_name, param_idx));
        }
        if let (Some(col_name), Some(param_idx)) = (column_name(right), bind_param_index(left)) {
            return Some((col_name, param_idx));
        }
    }
    None
}

/// Extract a column name from an expression if it's a simple column reference.
fn column_name(expr: &Expr) -> Option<String> {
    if let Expr::Column(col_ref, _) = expr {
        if !is_rowid_ref(col_ref) {
            return Some(col_ref.column.to_string());
        }
    }
    None
}

/// Check if an expression is a rowid reference.
fn is_rowid_expr(expr: &Expr) -> bool {
    if let Expr::Column(col_ref, _) = expr {
        is_rowid_ref(col_ref)
    } else {
        false
    }
}

fn is_rowid_ref(col_ref: &ColumnRef) -> bool {
    let name = col_ref.column.to_ascii_lowercase();
    name == "rowid" || name == "_rowid_" || name == "oid"
}

/// Extract a bind parameter index from a `?` or `?NNN` placeholder.
fn bind_param_index(expr: &Expr) -> Option<i32> {
    bind_param_ref(expr).map(|bind| match bind {
        BindParamRef::Anonymous => 1,
        BindParamRef::Numbered(idx) => idx,
    })
}

/// Extract a bind parameter while preserving anonymous vs numbered form.
fn bind_param_ref(expr: &Expr) -> Option<BindParamRef> {
    if let Expr::Placeholder(pt, _) = expr {
        match pt {
            PlaceholderType::Anonymous => Some(BindParamRef::Anonymous),
            PlaceholderType::Numbered(n) =>
            {
                #[allow(clippy::cast_possible_wrap)]
                Some(BindParamRef::Numbered(*n as i32))
            }
            _ => None,
        }
    } else {
        None
    }
}

/// Map AST binary operator to VDBE opcode.
fn binary_op_to_opcode(op: AstBinaryOp) -> Opcode {
    match op {
        AstBinaryOp::Add => Opcode::Add,
        AstBinaryOp::Subtract => Opcode::Subtract,
        AstBinaryOp::Multiply => Opcode::Multiply,
        AstBinaryOp::Divide => Opcode::Divide,
        AstBinaryOp::Modulo => Opcode::Remainder,
        AstBinaryOp::Concat => Opcode::Concat,
        AstBinaryOp::BitAnd => Opcode::BitAnd,
        AstBinaryOp::BitOr => Opcode::BitOr,
        AstBinaryOp::ShiftLeft => Opcode::ShiftLeft,
        AstBinaryOp::ShiftRight => Opcode::ShiftRight,
        AstBinaryOp::And => Opcode::And,
        AstBinaryOp::Or => Opcode::Or,
        AstBinaryOp::Eq | AstBinaryOp::Is => Opcode::Eq,
        AstBinaryOp::Ne | AstBinaryOp::IsNot => Opcode::Ne,
        AstBinaryOp::Lt => Opcode::Lt,
        AstBinaryOp::Le => Opcode::Le,
        AstBinaryOp::Gt => Opcode::Gt,
        AstBinaryOp::Ge => Opcode::Ge,
    }
}

/// Returns true if `op` is a comparison that produces 1/0 in the result register.
fn is_comparison_op(op: AstBinaryOp) -> bool {
    matches!(
        op,
        AstBinaryOp::Eq
            | AstBinaryOp::Ne
            | AstBinaryOp::Lt
            | AstBinaryOp::Le
            | AstBinaryOp::Gt
            | AstBinaryOp::Ge
            | AstBinaryOp::Is
            | AstBinaryOp::IsNot
    )
}

/// Emit a comparison that produces 1 (true) or 0 (false) in `dest`.
fn emit_comparison_expr(
    b: &mut ProgramBuilder,
    left: &Expr,
    op: AstBinaryOp,
    right: &Expr,
    dest: i32,
) -> Result<(), CodegenError> {
    let lhs = b.alloc_temp();
    let rhs = b.alloc_temp();
    emit_expr(b, left, lhs)?;
    emit_expr(b, right, rhs)?;

    let opcode = binary_op_to_opcode(op);
    // IS / IS NOT need the NULLEQ flag (0x80) so NULL IS NULL → true.
    let p5 = if matches!(op, AstBinaryOp::Is | AstBinaryOp::IsNot) {
        0x80
    } else {
        0
    };
    let true_label = b.emit_label();
    let done_label = b.emit_label();

    // Jump to true_label if comparison holds.
    b.emit_jump_to_label(opcode, rhs, lhs, true_label, P4::None, p5);
    b.emit_op(Opcode::Integer, 0, dest, 0, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
    b.resolve_label(true_label);
    b.emit_op(Opcode::Integer, 1, dest, 0, P4::None, 0);
    b.resolve_label(done_label);

    b.free_temp(lhs);
    b.free_temp(rhs);
    Ok(())
}

/// Convert a SQL type name to a single-char affinity code for CAST.
fn type_to_affinity(type_name: &str) -> char {
    // SQLite affinity characters: A=Blob/None, B=Text, C=Numeric, D=Integer, E=Real
    let upper = type_name.to_uppercase();
    if upper.contains("INT") {
        'D' // integer
    } else if upper.contains("CHAR")
        || upper.contains("CLOB")
        || upper.contains("TEXT")
        || upper.contains("VARCHAR")
    {
        'B' // text
    } else if upper.contains("BLOB") || upper.is_empty() {
        'A' // blob (no affinity)
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        'E' // real
    } else {
        'C' // default: numeric
    }
}

/// Emit an expression value into a register.
///
/// Handles bind parameters, literals, binary/unary ops, IS NULL, CAST,
/// function calls, CASE, and COLLATE expressions.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines
)]
fn emit_expr(b: &mut ProgramBuilder, expr: &Expr, reg: i32) -> Result<(), CodegenError> {
    match expr {
        Expr::Placeholder(pt, _) => {
            let idx = match pt {
                fsqlite_ast::PlaceholderType::Numbered(n) => *n as i32,
                _ => 1, // Anonymous or other — will be renumbered by caller.
            };
            b.emit_op(Opcode::Variable, idx, reg, 0, P4::None, 0);
            Ok(())
        }
        Expr::Literal(lit, _) => match lit {
            Literal::Integer(n) => {
                if let Ok(as_i32) = i32::try_from(*n) {
                    b.emit_op(Opcode::Integer, as_i32, reg, 0, P4::None, 0);
                } else {
                    b.emit_op(Opcode::Int64, 0, reg, 0, P4::Int64(*n), 0);
                }
                Ok(())
            }
            Literal::Float(f) => {
                b.emit_op(Opcode::Real, 0, reg, 0, P4::Real(*f), 0);
                Ok(())
            }
            Literal::String(s) => {
                b.emit_op(Opcode::String8, 0, reg, 0, P4::Str(s.clone()), 0);
                Ok(())
            }
            Literal::Blob(bytes) => {
                b.emit_op(
                    Opcode::Blob,
                    bytes.len() as i32,
                    reg,
                    0,
                    P4::Blob(bytes.clone()),
                    0,
                );
                Ok(())
            }
            Literal::Null => {
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                Ok(())
            }
            Literal::True => {
                b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
                Ok(())
            }
            Literal::False => {
                b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
                Ok(())
            }
            Literal::CurrentTime => {
                // Emit PureFunc call to time('now').
                let arg_reg = b.alloc_temp();
                b.emit_op(Opcode::String8, 0, arg_reg, 0, P4::Str("now".to_owned()), 0);
                b.emit_op(
                    Opcode::PureFunc,
                    0,
                    arg_reg,
                    reg,
                    P4::FuncName("TIME".to_owned()),
                    1,
                );
                b.free_temp(arg_reg);
                Ok(())
            }
            Literal::CurrentDate => {
                let arg_reg = b.alloc_temp();
                b.emit_op(Opcode::String8, 0, arg_reg, 0, P4::Str("now".to_owned()), 0);
                b.emit_op(
                    Opcode::PureFunc,
                    0,
                    arg_reg,
                    reg,
                    P4::FuncName("DATE".to_owned()),
                    1,
                );
                b.free_temp(arg_reg);
                Ok(())
            }
            Literal::CurrentTimestamp => {
                let arg_reg = b.alloc_temp();
                b.emit_op(Opcode::String8, 0, arg_reg, 0, P4::Str("now".to_owned()), 0);
                b.emit_op(
                    Opcode::PureFunc,
                    0,
                    arg_reg,
                    reg,
                    P4::FuncName("DATETIME".to_owned()),
                    1,
                );
                b.free_temp(arg_reg);
                Ok(())
            }
        },
        Expr::BoundOuterValue { value, .. } => emit_sqlite_value(b, value, reg),

        // Binary operations: arithmetic, comparison, logical, bitwise.
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            if is_comparison_op(*op) {
                return emit_comparison_expr(b, left, *op, right, reg);
            }
            // Arithmetic / logical / bitwise: left→reg, right→tmp, apply op.
            let tmp = b.alloc_temp();
            emit_expr(b, left, reg)?;
            emit_expr(b, right, tmp)?;
            let opcode = binary_op_to_opcode(*op);
            // VDBE arithmetic: p1=rhs, p2=lhs, p3=dest
            b.emit_op(opcode, tmp, reg, reg, P4::None, 0);
            b.free_temp(tmp);
            Ok(())
        }

        // Unary operations: negate, plus, NOT, bit-NOT.
        Expr::UnaryOp {
            op, expr: inner, ..
        } => {
            emit_expr(b, inner, reg)?;
            match op {
                AstUnaryOp::Negate => {
                    let tmp = b.alloc_temp();
                    b.emit_op(Opcode::Integer, -1, tmp, 0, P4::None, 0);
                    b.emit_op(Opcode::Multiply, tmp, reg, reg, P4::None, 0);
                    b.free_temp(tmp);
                }
                AstUnaryOp::Plus => {} // no-op
                AstUnaryOp::Not => {
                    b.emit_op(Opcode::Not, reg, reg, 0, P4::None, 0);
                }
                AstUnaryOp::BitNot => {
                    b.emit_op(Opcode::BitNot, reg, reg, 0, P4::None, 0);
                }
            }
            Ok(())
        }

        // IS [NOT] NULL.
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            emit_expr(b, inner, reg)?;
            let true_label = b.emit_label();
            let done_label = b.emit_label();
            if *not {
                b.emit_jump_to_label(Opcode::NotNull, reg, 0, true_label, P4::None, 0);
            } else {
                b.emit_jump_to_label(Opcode::IsNull, reg, 0, true_label, P4::None, 0);
            }
            b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            b.resolve_label(true_label);
            b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
            b.resolve_label(done_label);
            Ok(())
        }

        // CAST(expr AS type).
        Expr::Cast {
            expr: inner,
            type_name,
            ..
        } => {
            emit_expr(b, inner, reg)?;
            let affinity = type_to_affinity(&type_name.name);
            // Cast opcode: p1 = register, p2 = affinity character code.
            // 'A'=65 Blob, 'B'=66 Text, 'C'=67 Numeric, 'D'=68 Integer, 'E'=69 Real
            b.emit_op(Opcode::Cast, reg, i32::from(affinity as u8), 0, P4::None, 0);
            Ok(())
        }

        // Scalar function call.
        Expr::FunctionCall { name, args, .. } => {
            // Function registry stores names in uppercase via canonical_name().
            let canon = name.trim().to_ascii_uppercase();
            match args {
                FunctionArgs::Star => {
                    b.emit_op(Opcode::PureFunc, 0, 0, reg, P4::FuncName(canon), 0);
                }
                FunctionArgs::List(arg_list) => {
                    let n_args = arg_list.len();
                    if n_args == 0 {
                        b.emit_op(Opcode::PureFunc, 0, 0, reg, P4::FuncName(canon), 0);
                    } else {
                        let first_arg_reg = b.alloc_regs(n_args as i32);
                        for (i, arg) in arg_list.iter().enumerate() {
                            emit_expr(b, arg, first_arg_reg + i as i32)?;
                        }
                        b.emit_op(
                            Opcode::PureFunc,
                            0,
                            first_arg_reg,
                            reg,
                            P4::FuncName(canon),
                            n_args as u16,
                        );
                    }
                }
            }
            Ok(())
        }

        // CASE [operand] WHEN ... THEN ... [ELSE ...] END
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            let done_label = b.emit_label();
            if let Some(base_expr) = operand {
                let base_reg = b.alloc_temp();
                emit_expr(b, base_expr, base_reg)?;
                for (when_val, then_val) in whens {
                    let next_label = b.emit_label();
                    let when_reg = b.alloc_temp();
                    emit_expr(b, when_val, when_reg)?;
                    b.emit_jump_to_label(
                        Opcode::Ne,
                        when_reg,
                        base_reg,
                        next_label,
                        P4::None,
                        0x10,
                    );
                    b.free_temp(when_reg);
                    emit_expr(b, then_val, reg)?;
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(next_label);
                }
                b.free_temp(base_reg);
            } else {
                for (when_cond, then_val) in whens {
                    let next_label = b.emit_label();
                    let cond_reg = b.alloc_temp();
                    emit_expr(b, when_cond, cond_reg)?;
                    b.emit_jump_to_label(Opcode::IfNot, cond_reg, 0, next_label, P4::None, 1);
                    b.free_temp(cond_reg);
                    emit_expr(b, then_val, reg)?;
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(next_label);
                }
            }
            if let Some(else_val) = else_expr {
                emit_expr(b, else_val, reg)?;
            } else {
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            }
            b.resolve_label(done_label);
            Ok(())
        }

        // Collate — just evaluate the inner expression.
        Expr::Collate { expr: inner, .. } => emit_expr(b, inner, reg),

        _ => Err(CodegenError::Unsupported(
            "planner expression codegen for this expression type".to_owned(),
        )),
    }
}

/// Emit an already-materialized SQLite value without translating it through
/// the SQL-literal AST, which would discard its exact storage class.
fn emit_sqlite_value(
    b: &mut ProgramBuilder,
    value: &SqliteValue,
    reg: i32,
) -> Result<(), CodegenError> {
    match value {
        SqliteValue::Null => {
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        }
        SqliteValue::Integer(value) => {
            if let Ok(as_i32) = i32::try_from(*value) {
                b.emit_op(Opcode::Integer, as_i32, reg, 0, P4::None, 0);
            } else {
                b.emit_op(Opcode::Int64, 0, reg, 0, P4::Int64(*value), 0);
            }
        }
        SqliteValue::Float(value) => {
            b.emit_op(Opcode::Real, 0, reg, 0, P4::Real(*value), 0);
        }
        SqliteValue::Text(value) => {
            b.emit_op(
                Opcode::String8,
                0,
                reg,
                0,
                P4::Str(value.as_str().to_owned()),
                0,
            );
        }
        SqliteValue::Blob(value) => {
            let len = i32::try_from(value.len()).map_err(|_| {
                CodegenError::Unsupported("bound BLOB value exceeds VDBE operand range".to_owned())
            })?;
            b.emit_op(Opcode::Blob, len, reg, 0, P4::Blob(value.to_vec()), 0);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::{
        Assignment, AssignmentTarget, BinaryOp as AstBinaryOp, BoundCollation, ColumnRef,
        DeleteStatement, Distinctness, Expr, FromClause, InsertSource, InsertStatement, Literal,
        PlaceholderType, QualifiedName, QualifiedTableRef, ResultColumn, SelectBody, SelectCore,
        SelectStatement, Span, TableOrSubquery, UpdateStatement,
    };
    use fsqlite_types::opcode::{Opcode, ProgramBuilder, VdbeProgram};

    fn test_schema() -> Vec<TableSchema> {
        vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "a".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'C',
                    default_value: None,
                },
            ],
            indexes: vec![],
        }]
    }

    fn test_schema_with_index() -> Vec<TableSchema> {
        vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "a".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'C',
                    default_value: None,
                },
            ],
            indexes: vec![IndexSchema {
                name: "idx_t_b".to_owned(),
                root_page: 3,
                columns: vec!["b".to_owned()],
                is_unique: false,
            }],
        }]
    }

    fn from_table(name: &str) -> FromClause {
        FromClause {
            source: TableOrSubquery::Table {
                name: QualifiedName::bare(name),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            joins: vec![],
        }
    }

    fn placeholder(n: u32) -> Expr {
        Expr::Placeholder(PlaceholderType::Numbered(n), Span::ZERO)
    }

    #[test]
    fn test_table_schema_accessors_affinity_index_and_case_insensitive_lookup() {
        let schema = TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'b',
                    default_value: None,
                },
                ColumnInfo {
                    name: "age".to_owned(),
                    affinity: 'c',
                    default_value: None,
                },
            ],
            indexes: vec![IndexSchema {
                name: "idx_age_name".to_owned(),
                root_page: 3,
                columns: vec!["age".to_owned(), "name".to_owned()],
                is_unique: false,
            }],
        };

        // affinity_string: one char per column, in declaration order.
        assert_eq!(schema.affinity_string(), "dbc");

        // column_index: case-insensitive, in declaration order; None for absent.
        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(
            schema.column_index("NAME"),
            Some(1),
            "lookup is case-insensitive"
        );
        assert_eq!(schema.column_index("Age"), Some(2));
        assert_eq!(schema.column_index("missing"), None);

        // index_for_column matches the LEFTMOST index column only (case-insensitive).
        assert_eq!(
            schema.index_for_column("age").map(|i| i.name.as_str()),
            Some("idx_age_name")
        );
        assert_eq!(
            schema.index_for_column("AGE").map(|i| i.name.as_str()),
            Some("idx_age_name"),
            "case-insensitive"
        );
        assert!(
            schema.index_for_column("name").is_none(),
            "a non-leftmost index column is not matched"
        );
        assert!(schema.index_for_column("id").is_none());
    }

    #[test]
    fn test_type_to_affinity_follows_sqlite_rules() {
        // SQLite affinity determination is a case-insensitive substring scan in
        // precedence order: INT->'D', {CHAR,CLOB,TEXT,VARCHAR}->'B',
        // {BLOB, no type}->'A', {REAL,FLOA,DOUB}->'E', else NUMERIC 'C'.
        assert_eq!(type_to_affinity("INTEGER"), 'D');
        assert_eq!(type_to_affinity("int"), 'D');
        assert_eq!(type_to_affinity("BIGINT"), 'D');
        assert_eq!(type_to_affinity("TINYINT"), 'D');
        // SQLite quirk: anything containing "INT" gets INTEGER affinity ("POINT").
        assert_eq!(type_to_affinity("POINT"), 'D');

        assert_eq!(type_to_affinity("TEXT"), 'B');
        assert_eq!(type_to_affinity("VARCHAR(255)"), 'B');
        assert_eq!(type_to_affinity("CHARACTER(20)"), 'B');
        assert_eq!(type_to_affinity("CLOB"), 'B');

        // BLOB or a missing declared type yields no affinity ('A').
        assert_eq!(type_to_affinity("BLOB"), 'A');
        assert_eq!(type_to_affinity(""), 'A');

        assert_eq!(type_to_affinity("REAL"), 'E');
        assert_eq!(type_to_affinity("DOUBLE PRECISION"), 'E');
        assert_eq!(type_to_affinity("FLOAT"), 'E');

        // Everything else falls through to NUMERIC.
        assert_eq!(type_to_affinity("NUMERIC"), 'C');
        assert_eq!(type_to_affinity("DECIMAL(10,2)"), 'C');
        assert_eq!(type_to_affinity("BOOLEAN"), 'C');
        assert_eq!(type_to_affinity("DATETIME"), 'C');
    }

    #[test]
    fn test_rowid_ref_aliases_and_conflict_action_codes() {
        // SQLite recognizes three case-insensitive rowid aliases.
        for name in ["rowid", "ROWID", "_rowid_", "oid", "OID", "RowId"] {
            assert!(
                is_rowid_ref(&ColumnRef::bare(name)),
                "{name} is a rowid alias"
            );
        }
        for name in ["id", "name", "rowid_", "row_id", "_oid_"] {
            assert!(
                !is_rowid_ref(&ColumnRef::bare(name)),
                "{name} is not a rowid alias"
            );
        }

        // ON CONFLICT actions map to OE_ codes; a missing action defaults to ABORT.
        assert_eq!(
            conflict_action_to_oe(None),
            OE_ABORT,
            "default conflict action is ABORT"
        );
        assert_eq!(
            conflict_action_to_oe(Some(&ConflictAction::Abort)),
            OE_ABORT
        );
        assert_eq!(
            conflict_action_to_oe(Some(&ConflictAction::Rollback)),
            OE_ROLLBACK
        );
        assert_eq!(conflict_action_to_oe(Some(&ConflictAction::Fail)), OE_FAIL);
        assert_eq!(
            conflict_action_to_oe(Some(&ConflictAction::Ignore)),
            OE_IGNORE
        );
        assert_eq!(
            conflict_action_to_oe(Some(&ConflictAction::Replace)),
            OE_REPLACE
        );
        // The five OE conflict codes are distinct.
        let codes: std::collections::HashSet<u16> =
            [OE_ROLLBACK, OE_ABORT, OE_FAIL, OE_IGNORE, OE_REPLACE]
                .into_iter()
                .collect();
        assert_eq!(codes.len(), 5, "OE conflict codes must be distinct");
    }

    #[test]
    fn test_emit_comparison_expr_shape_and_is_nulleq_flag() {
        // emit_comparison_expr evaluates both operands into temps, then a
        // comparison opcode jumps to set dest=1, falling through to dest=0 via a
        // Goto. IS / IS NOT additionally set the NULLEQ flag (p5=0x80) so that
        // NULL IS NULL compares true. Neither the emitted shape nor the flag was
        // directly asserted (binary_op_to_opcode only checks Is -> Eq).
        let lit = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);

        // `3 < 5`: eval, eval, Lt(jump), Integer 0, Goto, Integer 1.
        let mut b = ProgramBuilder::new();
        let dest = b.alloc_reg();
        emit_comparison_expr(&mut b, &lit(3), AstBinaryOp::Lt, &lit(5), dest).unwrap();
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Integer, // left operand
                Opcode::Integer, // right operand
                Opcode::Lt,      // comparison jump
                Opcode::Integer, // false branch -> 0
                Opcode::Goto,
                Opcode::Integer, // true branch -> 1
            ]
        ));
        let lt = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Lt)
            .expect("Lt present");
        assert_eq!(lt.p5, 0, "a plain comparison carries no NULLEQ flag");

        // `3 IS 5`: maps to Eq with the NULLEQ flag set (0x80).
        let mut b2 = ProgramBuilder::new();
        let d2 = b2.alloc_reg();
        emit_comparison_expr(&mut b2, &lit(3), AstBinaryOp::Is, &lit(5), d2).unwrap();
        b2.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog2 = b2.finish().unwrap();
        let eq = prog2
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Eq)
            .expect("Eq present");
        assert_eq!(
            eq.p5, 0x80,
            "IS uses the NULLEQ flag so NULL IS NULL is true"
        );
    }

    #[test]
    fn test_is_rowid_ref_recognizes_aliases_case_insensitively() {
        // is_rowid_ref accepts the three rowid aliases (rowid, _rowid_, oid)
        // case-insensitively and rejects everything else. It is exercised
        // indirectly via SELECT codegen but never asserted as a predicate.
        for name in [
            "rowid", "ROWID", "RowId", "_rowid_", "_ROWID_", "oid", "OID",
        ] {
            assert!(
                is_rowid_ref(&ColumnRef::bare(name)),
                "{name} should be a rowid alias"
            );
        }
        for name in ["id", "row_id", "rowid2", "oids", "_rowid", "rowi", ""] {
            assert!(
                !is_rowid_ref(&ColumnRef::bare(name)),
                "{name} should NOT be a rowid alias"
            );
        }
    }

    #[test]
    fn test_binary_op_to_opcode_and_is_comparison_classification() {
        use AstBinaryOp as B;

        // Arithmetic / bitwise / logical / concat map directly...
        assert_eq!(binary_op_to_opcode(B::Add), Opcode::Add);
        assert_eq!(binary_op_to_opcode(B::Subtract), Opcode::Subtract);
        assert_eq!(binary_op_to_opcode(B::Multiply), Opcode::Multiply);
        assert_eq!(binary_op_to_opcode(B::Divide), Opcode::Divide);
        // ...with one rename: Modulo -> Remainder.
        assert_eq!(binary_op_to_opcode(B::Modulo), Opcode::Remainder);
        assert_eq!(binary_op_to_opcode(B::Concat), Opcode::Concat);
        assert_eq!(binary_op_to_opcode(B::BitAnd), Opcode::BitAnd);
        assert_eq!(binary_op_to_opcode(B::BitOr), Opcode::BitOr);
        assert_eq!(binary_op_to_opcode(B::ShiftLeft), Opcode::ShiftLeft);
        assert_eq!(binary_op_to_opcode(B::ShiftRight), Opcode::ShiftRight);
        assert_eq!(binary_op_to_opcode(B::And), Opcode::And);
        assert_eq!(binary_op_to_opcode(B::Or), Opcode::Or);

        // Comparisons; IS / IS NOT collapse onto Eq / Ne at the opcode level.
        assert_eq!(binary_op_to_opcode(B::Eq), Opcode::Eq);
        assert_eq!(binary_op_to_opcode(B::Is), Opcode::Eq);
        assert_eq!(binary_op_to_opcode(B::Ne), Opcode::Ne);
        assert_eq!(binary_op_to_opcode(B::IsNot), Opcode::Ne);
        assert_eq!(binary_op_to_opcode(B::Lt), Opcode::Lt);
        assert_eq!(binary_op_to_opcode(B::Le), Opcode::Le);
        assert_eq!(binary_op_to_opcode(B::Gt), Opcode::Gt);
        assert_eq!(binary_op_to_opcode(B::Ge), Opcode::Ge);

        // is_comparison_op: the six relational ops plus IS / IS NOT, nothing else.
        for op in [B::Eq, B::Ne, B::Lt, B::Le, B::Gt, B::Ge, B::Is, B::IsNot] {
            assert!(is_comparison_op(op), "{op:?} is a comparison");
        }
        for op in [
            B::Add,
            B::Subtract,
            B::Multiply,
            B::Divide,
            B::Modulo,
            B::Concat,
            B::BitAnd,
            B::BitOr,
            B::ShiftLeft,
            B::ShiftRight,
            B::And,
            B::Or,
        ] {
            assert!(!is_comparison_op(op), "{op:?} is not a comparison");
        }
    }

    #[test]
    fn test_extract_column_eq_bind_symmetry_and_rowid_exclusion() {
        let bin = |left: Expr, right: Expr| Expr::BinaryOp {
            left: Box::new(left),
            op: AstBinaryOp::Eq,
            right: Box::new(right),
            span: Span::ZERO,
        };
        let col = |name: &str| Expr::Column(ColumnRef::bare(name), Span::ZERO);

        // `name = ?2` -> ("name", 2).
        let e = bin(col("name"), placeholder(2));
        assert_eq!(
            extract_column_eq_bind(Some(&e)),
            Some(("name".to_owned(), 2))
        );

        // Symmetric form `?3 = age` -> ("age", 3).
        let e = bin(placeholder(3), col("age"));
        assert_eq!(
            extract_column_eq_bind(Some(&e)),
            Some(("age".to_owned(), 3))
        );

        // rowid columns are excluded (they take a separate seek path) -> None.
        let e = bin(col("rowid"), placeholder(1));
        assert_eq!(extract_column_eq_bind(Some(&e)), None);

        // A non-bind right-hand side (`name = age`) -> None.
        let e = bin(col("name"), col("age"));
        assert_eq!(extract_column_eq_bind(Some(&e)), None);

        // A non-equality operator (`name < ?1`) -> None.
        let lt = Expr::BinaryOp {
            left: Box::new(col("name")),
            op: AstBinaryOp::Lt,
            right: Box::new(placeholder(1)),
            span: Span::ZERO,
        };
        assert_eq!(extract_column_eq_bind(Some(&lt)), None);

        // No WHERE clause -> None.
        assert_eq!(extract_column_eq_bind(None), None);
    }

    #[test]
    fn test_extract_rowid_bind_symmetry_and_form_preservation() {
        let eq = |left: Expr, right: Expr| Expr::BinaryOp {
            left: Box::new(left),
            op: AstBinaryOp::Eq,
            right: Box::new(right),
            span: Span::ZERO,
        };
        let col = |name: &str| Expr::Column(ColumnRef::bare(name), Span::ZERO);

        // `rowid = ?2` -> Numbered(2); the *_param form collapses to 2.
        let e = eq(col("rowid"), placeholder(2));
        assert_eq!(
            extract_rowid_bind(Some(&e)),
            Some(BindParamRef::Numbered(2))
        );
        assert_eq!(extract_rowid_bind_param(Some(&e)), Some(2));

        // Symmetric, and the `oid` alias is recognized: `?3 = oid` -> Numbered(3).
        let e = eq(placeholder(3), col("oid"));
        assert_eq!(
            extract_rowid_bind(Some(&e)),
            Some(BindParamRef::Numbered(3))
        );

        // An anonymous placeholder preserves its form; *_param collapses it to 1.
        let e = eq(
            col("rowid"),
            Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO),
        );
        assert_eq!(extract_rowid_bind(Some(&e)), Some(BindParamRef::Anonymous));
        assert_eq!(extract_rowid_bind_param(Some(&e)), Some(1));

        // A non-rowid column is not extracted here (handled by column-eq path).
        let e = eq(col("name"), placeholder(1));
        assert_eq!(extract_rowid_bind(Some(&e)), None);

        // Non-bind RHS and a missing WHERE -> None.
        assert_eq!(extract_rowid_bind(Some(&eq(col("rowid"), col("id")))), None);
        assert_eq!(extract_rowid_bind(None), None);
    }

    #[test]
    fn test_result_column_count_expands_stars() {
        let table = TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "a".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'b',
                    default_value: None,
                },
                ColumnInfo {
                    name: "c".to_owned(),
                    affinity: 'c',
                    default_value: None,
                },
            ],
            indexes: vec![],
        };
        let expr = || ResultColumn::Expr {
            expr: Expr::Literal(Literal::Integer(1), Span::ZERO),
            alias: None,
        };

        assert_eq!(result_column_count(&[], &table), 0);
        assert_eq!(result_column_count(&[expr(), expr()], &table), 2);
        // `*` expands to the table's column count (3); each Expr adds 1.
        assert_eq!(result_column_count(&[ResultColumn::Star], &table), 3);
        assert_eq!(
            result_column_count(&[ResultColumn::Star, expr()], &table),
            4
        );
        // Each star expands independently.
        assert_eq!(
            result_column_count(&[ResultColumn::Star, ResultColumn::Star], &table),
            6
        );

        // `t.*` (TableStar) expands exactly like `*` and composes with a plain
        // star and with an expr. This variant was not previously constructed by
        // the test, even though result_column_count handles it identically.
        let table_star = || ResultColumn::TableStar(QualifiedName::bare("t"));
        assert_eq!(result_column_count(&[table_star()], &table), 3);
        assert_eq!(
            result_column_count(&[table_star(), ResultColumn::Star], &table),
            6
        );
        assert_eq!(result_column_count(&[table_star(), expr()], &table), 4);
    }

    #[test]
    fn test_expr_classifiers_column_name_rowid_and_bind_param() {
        let col = |name: &str| Expr::Column(ColumnRef::bare(name), Span::ZERO);
        let lit = Expr::Literal(Literal::Integer(7), Span::ZERO);

        // column_name: a plain column -> its name; rowid aliases / non-columns -> None.
        assert_eq!(column_name(&col("name")), Some("name".to_owned()));
        assert_eq!(
            column_name(&col("rowid")),
            None,
            "rowid is excluded from column extraction"
        );
        assert_eq!(column_name(&col("OID")), None);
        assert_eq!(column_name(&lit), None);

        // is_rowid_expr: true only for a rowid-alias column expression.
        assert!(is_rowid_expr(&col("rowid")));
        assert!(is_rowid_expr(&col("_rowid_")));
        assert!(!is_rowid_expr(&col("name")));
        assert!(!is_rowid_expr(&lit));

        // bind_param_ref: numbered/anonymous placeholders; non-placeholder -> None.
        assert_eq!(
            bind_param_ref(&placeholder(5)),
            Some(BindParamRef::Numbered(5))
        );
        assert_eq!(
            bind_param_ref(&Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO)),
            Some(BindParamRef::Anonymous)
        );
        assert_eq!(bind_param_ref(&lit), None);
        // bind_param_index collapses anonymous to 1, numbered to its index.
        assert_eq!(bind_param_index(&placeholder(5)), Some(5));
        assert_eq!(
            bind_param_index(&Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO)),
            Some(1)
        );
    }

    #[test]
    fn test_default_value_to_expr_handles_missing_valid_and_invalid_defaults() {
        let no_default = ColumnInfo {
            name: "a".to_owned(),
            affinity: 'd',
            default_value: None,
        };
        let with_default = ColumnInfo {
            name: "b".to_owned(),
            affinity: 'd',
            default_value: Some("42".to_owned()),
        };
        let bad_default = ColumnInfo {
            name: "c".to_owned(),
            affinity: 'd',
            default_value: Some("1 +".to_owned()),
        };
        let table = TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![no_default.clone(), with_default.clone()],
            indexes: vec![],
        };

        // No DEFAULT -> NULL literal.
        assert!(matches!(
            default_value_to_expr(&table, &no_default),
            Ok(Expr::Literal(Literal::Null, _))
        ));
        // A valid DEFAULT parses to an expression.
        assert!(default_value_to_expr(&table, &with_default).is_ok());
        // An unparseable DEFAULT surfaces as a CodegenError::Unsupported.
        assert!(matches!(
            default_value_to_expr(&table, &bad_default),
            Err(CodegenError::Unsupported(_))
        ));

        // insert_default_exprs produces one expr per column; the no-default
        // column yields a NULL literal.
        let defaults = insert_default_exprs(&table).expect("defaults compile");
        assert_eq!(defaults.len(), 2);
        assert!(
            matches!(defaults[0], Expr::Literal(Literal::Null, _)),
            "column a has no default -> NULL"
        );
    }

    #[test]
    fn test_insert_target_indices_resolution_order_and_errors() {
        let table = TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "a".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "c".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
            ],
            indexes: vec![],
        };

        // An empty column list targets all columns in declaration order.
        assert_eq!(insert_target_indices(&[], &table).unwrap(), vec![0, 1, 2]);

        // An explicit list resolves to positions in INSERT order (not table
        // order), case-insensitively.
        assert_eq!(
            insert_target_indices(&["c".to_owned(), "A".to_owned()], &table).unwrap(),
            vec![2, 0]
        );

        // An unknown column is rejected with ColumnNotFound.
        assert!(matches!(
            insert_target_indices(&["a".to_owned(), "missing".to_owned()], &table),
            Err(CodegenError::ColumnNotFound { ref column, .. }) if column == "missing"
        ));
    }

    #[test]
    fn test_expand_insert_values_row_places_values_and_fills_defaults() {
        let lit = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);
        let table = TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "a".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'd',
                    default_value: Some("99".to_owned()),
                },
                ColumnInfo {
                    name: "c".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
            ],
            indexes: vec![],
        };

        // No column list -> values pass through unchanged.
        let passed = expand_insert_values_row(&[lit(1), lit(2), lit(3)], &[], &table).unwrap();
        assert_eq!(passed.len(), 3);
        assert!(matches!(passed[0], Expr::Literal(Literal::Integer(1), _)));
        assert!(matches!(passed[2], Expr::Literal(Literal::Integer(3), _)));

        // INSERT INTO t(c, a) VALUES (10, 20): values land at their TARGET
        // positions (a=20, c=10); the omitted column b takes its DEFAULT (99).
        let expanded = expand_insert_values_row(
            &[lit(10), lit(20)],
            &["c".to_owned(), "a".to_owned()],
            &table,
        )
        .unwrap();
        assert_eq!(expanded.len(), 3);
        assert!(
            matches!(expanded[0], Expr::Literal(Literal::Integer(20), _)),
            "a = 20"
        );
        assert!(
            matches!(expanded[1], Expr::Literal(Literal::Integer(99), _)),
            "b = default 99"
        );
        assert!(
            matches!(expanded[2], Expr::Literal(Literal::Integer(10), _)),
            "c = 10"
        );

        // A value/column count mismatch -> Unsupported.
        assert!(matches!(
            expand_insert_values_row(&[lit(1)], &["a".to_owned(), "b".to_owned()], &table),
            Err(CodegenError::Unsupported(_))
        ));
    }

    #[test]
    fn test_single_table_select_source_name_resolves_table_and_rejects_subquery() {
        // A bare table source yields its name.
        let from = from_table("users");
        assert_eq!(
            single_table_select_source_name(&from.source).unwrap(),
            "users"
        );

        // A subquery FROM source is not a single-table source -> Unsupported.
        let subquery = TableOrSubquery::Subquery {
            query: Box::new(star_select("x")),
            alias: None,
        };
        assert!(matches!(
            single_table_select_source_name(&subquery),
            Err(CodegenError::Unsupported(_))
        ));
    }

    #[test]
    fn test_find_table_is_case_insensitive_and_errors_on_missing() {
        let schema = vec![
            TableSchema {
                name: "Users".to_owned(),
                root_page: 2,
                columns: vec![],
                indexes: vec![],
            },
            TableSchema {
                name: "orders".to_owned(),
                root_page: 3,
                columns: vec![],
                indexes: vec![],
            },
        ];
        // Exact and case-insensitive name matches.
        assert_eq!(find_table(&schema, "Users").unwrap().name, "Users");
        assert_eq!(
            find_table(&schema, "users").unwrap().name,
            "Users",
            "case-insensitive"
        );
        assert_eq!(find_table(&schema, "ORDERS").unwrap().name, "orders");
        // A missing table -> TableNotFound carrying the requested name.
        assert!(matches!(
            find_table(&schema, "missing"),
            Err(CodegenError::TableNotFound(ref n)) if n == "missing"
        ));
    }

    #[test]
    fn test_codegen_error_display_messages() {
        assert_eq!(
            CodegenError::TableNotFound("users".to_owned()).to_string(),
            "table not found: users"
        );
        // The message names the column before the table.
        assert_eq!(
            CodegenError::ColumnNotFound {
                table: "t".to_owned(),
                column: "c".to_owned(),
            }
            .to_string(),
            "column c not found in table t"
        );
        assert_eq!(
            CodegenError::Unsupported("DISTINCT".to_owned()).to_string(),
            "unsupported: DISTINCT"
        );
    }

    #[test]
    fn test_codegen_select_no_from_emits_resultrow_without_cursor() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Literal(Literal::Integer(1), Span::ZERO),
                        alias: None,
                    }],
                    from: None,
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let schema: Vec<TableSchema> = vec![];
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // A no-FROM SELECT evaluates the expression and emits ResultRow + Halt...
        assert!(has_opcodes(
            &prog,
            &[Opcode::Integer, Opcode::ResultRow, Opcode::Halt]
        ));
        // ...and never opens a table cursor.
        assert!(
            prog.ops().iter().all(|op| op.opcode != Opcode::OpenRead),
            "a no-FROM SELECT must not open a read cursor"
        );
    }

    #[test]
    fn test_codegen_select_column_eq_emits_filtered_scan() {
        // SELECT a FROM t WHERE b = ?1 -> a full scan with an equality filter on
        // b (the non-rowid column-eq path), distinct from a plain full scan.
        let stmt = simple_select(&["a"], "t", Some(col_eq_param("b", 1)));
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Loads the bind param, opens the table, reads the filter column and
        // skips non-matching rows (Ne), then emits matches and iterates.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Variable,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::Ne,
                Opcode::ResultRow,
                Opcode::Next,
                Opcode::Halt,
            ]
        ));
    }

    #[test]
    fn test_codegen_select_values_emits_one_resultrow_per_row() {
        let lit = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);
        let values = |rows: Vec<Vec<Expr>>| SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(rows.into()),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let schema: Vec<TableSchema> = vec![];
        let ctx = CodegenContext::default();

        // VALUES (1,2), (3,4) -> one ResultRow per row, no table cursor.
        let stmt = values(vec![vec![lit(1), lit(2)], vec![lit(3), lit(4)]]);
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let resultrows = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::ResultRow)
            .count();
        assert_eq!(resultrows, 2, "one ResultRow per VALUES row");
        assert!(
            prog.ops().iter().all(|op| op.opcode != Opcode::OpenRead),
            "VALUES has no table cursor"
        );
        assert!(has_opcodes(&prog, &[Opcode::ResultRow, Opcode::Halt]));

        // Rows with mismatched arity are rejected.
        let bad = values(vec![vec![lit(1)], vec![lit(1), lit(2)]]);
        let mut b2 = ProgramBuilder::new();
        assert!(matches!(
            codegen_select(&mut b2, &bad, &schema, &ctx),
            Err(CodegenError::Unsupported(_))
        ));
    }

    #[test]
    fn test_codegen_insert_threads_conflict_action_into_insert_op() {
        let make = |conflict: Option<ConflictAction>| InsertStatement {
            with: None,
            or_conflict: conflict,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![placeholder(1), placeholder(2)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();

        // The conflict operand (p5) of the emitted Insert op carries the OE_ code.
        let oe_of = |conflict: Option<ConflictAction>| -> u16 {
            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &make(conflict), &schema, &ctx).unwrap();
            let prog = b.finish().unwrap();
            prog.ops()
                .iter()
                .find(|op| op.opcode == Opcode::Insert)
                .expect("Insert op present")
                .p5
        };

        // A plain INSERT defaults to ABORT; OR IGNORE / OR REPLACE thread theirs.
        assert_eq!(oe_of(None), OE_ABORT);
        assert_eq!(oe_of(Some(ConflictAction::Ignore)), OE_IGNORE);
        assert_eq!(oe_of(Some(ConflictAction::Replace)), OE_REPLACE);
    }

    fn rowid_eq_param() -> Box<Expr> {
        Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(placeholder(1)),
            span: Span::ZERO,
        })
    }

    fn col_eq_param(col: &str, n: u32) -> Box<Expr> {
        Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef::bare(col), Span::ZERO)),
            op: AstBinaryOp::Eq,
            right: Box::new(placeholder(n)),
            span: Span::ZERO,
        })
    }

    fn simple_select(
        cols: &[&str],
        table: &str,
        where_clause: Option<Box<Expr>>,
    ) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: cols
                        .iter()
                        .map(|c| ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare(*c), Span::ZERO),
                            alias: None,
                        })
                        .collect(),
                    from: Some(from_table(table)),
                    where_clause,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        }
    }

    fn star_select(table: &str) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Star],
                    from: Some(from_table(table)),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        }
    }

    fn opcode_sequence(prog: &VdbeProgram) -> Vec<Opcode> {
        prog.ops().iter().map(|op| op.opcode).collect()
    }

    fn has_opcodes(prog: &VdbeProgram, expected: &[Opcode]) -> bool {
        let ops = opcode_sequence(prog);
        // Check that expected opcodes appear in order (not necessarily adjacent).
        let mut ops_iter = ops.iter();
        for expected_op in expected {
            if !ops_iter.any(|op| op == expected_op) {
                return false;
            }
        }
        true
    }

    #[test]
    fn test_emit_expr_literals() {
        let mut b = ProgramBuilder::new();

        let reg_real = b.alloc_reg();
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::Float(3.25), Span::ZERO),
            reg_real,
        )
        .unwrap();

        let reg_blob = b.alloc_reg();
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::Blob(vec![0, 1, 2, 3]), Span::ZERO),
            reg_blob,
        )
        .unwrap();

        let reg_null = b.alloc_reg();
        emit_expr(&mut b, &Expr::Literal(Literal::Null, Span::ZERO), reg_null).unwrap();

        let reg_true = b.alloc_reg();
        emit_expr(&mut b, &Expr::Literal(Literal::True, Span::ZERO), reg_true).unwrap();

        let reg_false = b.alloc_reg();
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::False, Span::ZERO),
            reg_false,
        )
        .unwrap();

        let reg_current_time = b.alloc_reg();
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::CurrentTime, Span::ZERO),
            reg_current_time,
        )
        .unwrap();

        let reg_current_date = b.alloc_reg();
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::CurrentDate, Span::ZERO),
            reg_current_date,
        )
        .unwrap();

        let reg_current_timestamp = b.alloc_reg();
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::CurrentTimestamp, Span::ZERO),
            reg_current_timestamp,
        )
        .unwrap();

        let prog = b.finish().unwrap();
        let ops = prog.ops();
        // 5 simple literals + 3 × (String8 + PureFunc) for current time/date/timestamp = 11
        assert_eq!(ops.len(), 11);

        assert_eq!(ops[0].opcode, Opcode::Real);
        assert_eq!(ops[0].p2, reg_real);
        assert_eq!(ops[0].p4, P4::Real(3.25));

        assert_eq!(ops[1].opcode, Opcode::Blob);
        assert_eq!(ops[1].p1, 4);
        assert_eq!(ops[1].p2, reg_blob);
        assert_eq!(ops[1].p4, P4::Blob(vec![0, 1, 2, 3]));

        assert_eq!(ops[2].opcode, Opcode::Null);
        assert_eq!(ops[2].p2, reg_null);
        assert_eq!(ops[2].p4, P4::None);

        assert_eq!(ops[3].opcode, Opcode::Integer);
        assert_eq!(ops[3].p1, 1);
        assert_eq!(ops[3].p2, reg_true);
        assert_eq!(ops[3].p4, P4::None);

        assert_eq!(ops[4].opcode, Opcode::Integer);
        assert_eq!(ops[4].p1, 0);
        assert_eq!(ops[4].p2, reg_false);
        assert_eq!(ops[4].p4, P4::None);

        // CurrentTime → String8("now") + PureFunc("time")
        assert_eq!(ops[5].opcode, Opcode::String8);
        assert_eq!(ops[6].opcode, Opcode::PureFunc);
        assert_eq!(ops[6].p3, reg_current_time);

        // CurrentDate → String8("now") + PureFunc("date")
        assert_eq!(ops[7].opcode, Opcode::String8);
        assert_eq!(ops[8].opcode, Opcode::PureFunc);
        assert_eq!(ops[8].p3, reg_current_date);

        // CurrentTimestamp → String8("now") + PureFunc("datetime")
        assert_eq!(ops[9].opcode, Opcode::String8);
        assert_eq!(ops[10].opcode, Opcode::PureFunc);
        assert_eq!(ops[10].p3, reg_current_timestamp);
    }

    #[test]
    fn test_emit_expr_bound_outer_values_preserves_storage_class() {
        let values = [
            SqliteValue::Null,
            SqliteValue::Integer(i64::MAX),
            SqliteValue::Float(3.25),
            SqliteValue::Text("outer text".into()),
            SqliteValue::Blob(vec![0, 1, 2, 255].into()),
        ];
        let mut b = ProgramBuilder::new();

        for value in values {
            let reg = b.alloc_reg();
            emit_expr(
                &mut b,
                &Expr::BoundOuterValue {
                    value,
                    collation: BoundCollation::Unspecified,
                    affinity: None,
                    span: Span::ZERO,
                },
                reg,
            )
            .unwrap();
        }

        let program = b.finish().unwrap();
        let ops = program.ops();
        assert_eq!(ops.len(), 5);
        assert_eq!((ops[0].opcode, &ops[0].p4), (Opcode::Null, &P4::None));
        assert_eq!(
            (ops[1].opcode, &ops[1].p4),
            (Opcode::Int64, &P4::Int64(i64::MAX))
        );
        assert_eq!((ops[2].opcode, &ops[2].p4), (Opcode::Real, &P4::Real(3.25)));
        assert_eq!(
            (ops[3].opcode, &ops[3].p4),
            (Opcode::String8, &P4::Str("outer text".to_owned()))
        );
        assert_eq!(ops[4].p1, 4);
        assert_eq!(
            (ops[4].opcode, &ops[4].p4),
            (Opcode::Blob, &P4::Blob(vec![0, 1, 2, 255]))
        );
    }

    #[test]
    fn test_emit_expr_scalar_literal_opcodes() {
        // test_emit_expr_large_integer_literal_uses_int64_opcode covers the
        // Integer small/large split; this pins the opcode (and p1, for booleans)
        // that each remaining scalar literal lowers to.
        let emit_first = |lit: Literal| -> (Opcode, i32) {
            let mut b = ProgramBuilder::new();
            let reg = b.alloc_reg();
            emit_expr(&mut b, &Expr::Literal(lit, Span::ZERO), reg).unwrap();
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            let prog = b.finish().unwrap();
            let op = prog
                .ops()
                .iter()
                .find(|o| o.opcode != Opcode::Halt)
                .expect("a literal op");
            (op.opcode, op.p1)
        };

        assert_eq!(emit_first(Literal::Null).0, Opcode::Null);
        assert_eq!(emit_first(Literal::Float(1.5)).0, Opcode::Real);
        assert_eq!(
            emit_first(Literal::String("hi".to_owned())).0,
            Opcode::String8
        );
        assert_eq!(emit_first(Literal::Blob(vec![1, 2, 3])).0, Opcode::Blob);

        // Boolean literals both lower to Integer, distinguished by p1 (1 / 0).
        assert_eq!(emit_first(Literal::True), (Opcode::Integer, 1));
        assert_eq!(emit_first(Literal::False), (Opcode::Integer, 0));
    }

    #[test]
    fn test_emit_expr_arithmetic_and_unary_ops() {
        // emit_expr lowers a non-comparison BinaryOp to the matching arithmetic
        // opcode (left into dest, right into a temp), and the four unary ops to:
        // Negate -> multiply by -1, Plus -> no-op, Not -> Not, BitNot -> BitNot.
        let lit = |n: i64| Box::new(Expr::Literal(Literal::Integer(n), Span::ZERO));
        let prog_of = |expr: Expr| {
            let mut b = ProgramBuilder::new();
            let reg = b.alloc_reg();
            emit_expr(&mut b, &expr, reg).unwrap();
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.finish().unwrap()
        };

        // 3 + 5 -> Integer, Integer, Add.
        let add = prog_of(Expr::BinaryOp {
            left: lit(3),
            op: AstBinaryOp::Add,
            right: lit(5),
            span: Span::ZERO,
        });
        assert!(has_opcodes(
            &add,
            &[Opcode::Integer, Opcode::Integer, Opcode::Add]
        ));

        // -3 -> operand, Integer(-1), Multiply (negation lowers to x * -1).
        let neg = prog_of(Expr::UnaryOp {
            op: AstUnaryOp::Negate,
            expr: lit(3),
            span: Span::ZERO,
        });
        assert!(has_opcodes(
            &neg,
            &[Opcode::Integer, Opcode::Integer, Opcode::Multiply]
        ));

        // NOT x -> Not; ~x -> BitNot.
        let not = prog_of(Expr::UnaryOp {
            op: AstUnaryOp::Not,
            expr: lit(3),
            span: Span::ZERO,
        });
        assert!(not.ops().iter().any(|o| o.opcode == Opcode::Not));
        let bitnot = prog_of(Expr::UnaryOp {
            op: AstUnaryOp::BitNot,
            expr: lit(3),
            span: Span::ZERO,
        });
        assert!(bitnot.ops().iter().any(|o| o.opcode == Opcode::BitNot));

        // Unary plus is a no-op: only the operand (one Integer) plus the Halt.
        let plus = prog_of(Expr::UnaryOp {
            op: AstUnaryOp::Plus,
            expr: lit(3),
            span: Span::ZERO,
        });
        let non_halt: Vec<Opcode> = plus
            .ops()
            .iter()
            .map(|o| o.opcode)
            .filter(|&o| o != Opcode::Halt)
            .collect();
        assert_eq!(
            non_halt,
            vec![Opcode::Integer],
            "unary plus emits only the operand"
        );
    }

    #[test]
    fn test_emit_expr_cast_threads_affinity_char_into_cast_op_p2() {
        // CAST(expr AS type) emits a Cast op whose p2 is the affinity
        // character's byte value (type_to_affinity applied to the type name).
        // type_to_affinity is unit-tested, but its wire-up into the emitted Cast
        // op was not.
        let cast_p2 = |type_name: &str| -> i32 {
            let mut b = ProgramBuilder::new();
            let reg = b.alloc_reg();
            let expr = Expr::Cast {
                expr: Box::new(Expr::Literal(Literal::Integer(3), Span::ZERO)),
                type_name: fsqlite_ast::TypeName {
                    name: type_name.to_owned(),
                    arg1: None,
                    arg2: None,
                },
                span: Span::ZERO,
            };
            emit_expr(&mut b, &expr, reg).unwrap();
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            let prog = b.finish().unwrap();
            prog.ops()
                .iter()
                .find(|o| o.opcode == Opcode::Cast)
                .expect("Cast op present")
                .p2
        };

        // Affinity char codes: A=Blob(65), B=Text(66), D=Integer(68), E=Real(69).
        assert_eq!(cast_p2("INTEGER"), i32::from(b'D'));
        assert_eq!(cast_p2("TEXT"), i32::from(b'B'));
        assert_eq!(cast_p2("REAL"), i32::from(b'E'));
        assert_eq!(cast_p2("BLOB"), i32::from(b'A'));
    }

    #[test]
    fn test_emit_expr_is_null_vs_is_not_null_guard_opcode() {
        // x IS NULL and x IS NOT NULL both materialize 1/0, but the guard is
        // IsNull vs NotNull respectively. emit_expr's IsNull branch was untested.
        let prog_of = |not: bool| {
            let mut b = ProgramBuilder::new();
            let reg = b.alloc_reg();
            let expr = Expr::IsNull {
                expr: Box::new(Expr::Literal(Literal::Integer(3), Span::ZERO)),
                not,
                span: Span::ZERO,
            };
            emit_expr(&mut b, &expr, reg).unwrap();
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.finish().unwrap()
        };

        // IS NULL: guarded by IsNull, then the 0 / Goto / 1 materialization.
        let is_null = prog_of(false);
        assert!(has_opcodes(
            &is_null,
            &[
                Opcode::IsNull,
                Opcode::Integer,
                Opcode::Goto,
                Opcode::Integer
            ]
        ));
        assert!(!is_null.ops().iter().any(|o| o.opcode == Opcode::NotNull));

        // IS NOT NULL: guarded by NotNull instead.
        let is_not_null = prog_of(true);
        assert!(has_opcodes(
            &is_not_null,
            &[
                Opcode::NotNull,
                Opcode::Integer,
                Opcode::Goto,
                Opcode::Integer
            ]
        ));
        assert!(!is_not_null.ops().iter().any(|o| o.opcode == Opcode::IsNull));
    }

    #[test]
    fn test_emit_expr_function_call_canonicalizes_name_and_threads_arg_count() {
        // emit_expr lowers a scalar function call to PureFunc, canonicalizing the
        // name to uppercase (the registry's canonical form) and threading the
        // argument count into p5.
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let expr = Expr::FunctionCall {
            name: "abs".to_owned(), // lowercase on purpose
            args: FunctionArgs::List(vec![Expr::Literal(Literal::Integer(3), Span::ZERO)]),
            distinct: false,
            order_by: vec![],
            filter: None,
            over: None,
            span: Span::ZERO,
        };
        emit_expr(&mut b, &expr, reg).unwrap();
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();
        let func = prog
            .ops()
            .iter()
            .find(|o| o.opcode == Opcode::PureFunc)
            .expect("PureFunc op present");
        assert!(
            matches!(&func.p4, P4::FuncName(n) if n.as_str() == "ABS"),
            "lowercase 'abs' must be canonicalized to uppercase in P4"
        );
        assert_eq!(func.p5, 1, "one argument threaded into p5");
    }

    #[test]
    fn test_emit_expr_collate_is_transparent() {
        // COLLATE is a no-op at this codegen layer: emit_expr(x COLLATE C) emits
        // exactly what emit_expr(x) would, with no collation-specific opcode.
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let expr = Expr::Collate {
            expr: Box::new(Expr::Literal(Literal::Integer(3), Span::ZERO)),
            collation: "NOCASE".to_owned(),
            span: Span::ZERO,
        };
        emit_expr(&mut b, &expr, reg).unwrap();
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();
        let non_halt: Vec<Opcode> = prog
            .ops()
            .iter()
            .map(|o| o.opcode)
            .filter(|&o| o != Opcode::Halt)
            .collect();
        assert_eq!(
            non_halt,
            vec![Opcode::Integer],
            "COLLATE wraps transparently: only the inner literal's opcode is emitted"
        );
    }

    #[test]
    fn test_emit_expr_case_searched_and_simple_forms() {
        let lit = |n: i64| Expr::Literal(Literal::Integer(n), Span::ZERO);
        let prog_of = |expr: Expr| {
            let mut b = ProgramBuilder::new();
            let reg = b.alloc_reg();
            emit_expr(&mut b, &expr, reg).unwrap();
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.finish().unwrap()
        };

        // Searched CASE (no operand, no ELSE): each WHEN is guarded by IfNot,
        // and the missing ELSE falls through to a Null default.
        let searched = prog_of(Expr::Case {
            operand: None,
            whens: vec![(lit(1), lit(10))],
            else_expr: None,
            span: Span::ZERO,
        });
        assert!(
            searched.ops().iter().any(|o| o.opcode == Opcode::IfNot),
            "searched CASE guards each WHEN with IfNot"
        );
        assert!(
            searched.ops().iter().any(|o| o.opcode == Opcode::Null),
            "a CASE with no ELSE falls through to a Null default"
        );

        // Simple CASE (with operand and an ELSE): WHENs compare against the
        // operand with Ne, and the explicit ELSE means no Null default.
        let simple = prog_of(Expr::Case {
            operand: Some(Box::new(lit(5))),
            whens: vec![(lit(5), lit(10))],
            else_expr: Some(Box::new(lit(0))),
            span: Span::ZERO,
        });
        assert!(
            simple.ops().iter().any(|o| o.opcode == Opcode::Ne),
            "simple CASE compares the operand against each WHEN with Ne"
        );
        assert!(
            !simple.ops().iter().any(|o| o.opcode == Opcode::Null),
            "an explicit ELSE means no implicit Null default"
        );
    }

    #[test]
    fn test_emit_expr_unsupported_expr_returns_error() {
        // emit_expr handles literals, placeholders, binary/unary ops, IsNull,
        // Cast, FunctionCall, Case, and Collate; everything else hits the
        // catch-all and returns Unsupported. A bare column reference is only
        // emittable in a cursor/scan context (via emit_column_reads), not as a
        // free expression, so emit_expr rejects it.
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let err = emit_expr(&mut b, &Expr::Column(ColumnRef::bare("x"), Span::ZERO), reg)
            .expect_err("a free column reference is not emittable by emit_expr");
        assert!(matches!(err, CodegenError::Unsupported(_)));
    }

    #[test]
    fn test_emit_expr_large_integer_literal_uses_int64_opcode() {
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let value = 4_102_444_800_000_000_i64;
        emit_expr(
            &mut b,
            &Expr::Literal(Literal::Integer(value), Span::ZERO),
            reg,
        )
        .unwrap();

        let prog = b.finish().unwrap();
        let ops = prog.ops();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].opcode, Opcode::Int64);
        assert_eq!(ops[0].p2, reg);
        assert_eq!(ops[0].p4, P4::Int64(value));
    }

    // === Test 1: SELECT by rowid ===
    #[test]
    fn test_codegen_select_by_rowid() {
        let stmt = simple_select(&["b"], "t", Some(rowid_eq_param()));
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::Variable,
                Opcode::OpenRead,
                Opcode::SeekRowid,
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));
        // Transaction should be read-only (p2=0).
        let txn = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Transaction)
            .unwrap();
        assert_eq!(txn.p2, 0);
    }

    #[test]
    fn test_codegen_select_parenthesized_single_table_source() -> Result<(), String> {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(FromClause {
                        source: TableOrSubquery::ParenJoin(Box::new(from_table("t"))),
                        joins: vec![],
                    }),
                    where_clause: Some(rowid_eq_param()),
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).map_err(|err| format!("{err:?}"))?;
        let prog = b.finish().map_err(|err| format!("{err:?}"))?;

        if !has_opcodes(
            &prog,
            &[
                Opcode::OpenRead,
                Opcode::SeekRowid,
                Opcode::Column,
                Opcode::ResultRow,
            ],
        ) {
            return Err(format!(
                "parenthesized table source should use table SELECT path, got {:?}",
                opcode_sequence(&prog)
            ));
        }

        let open_read_root = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::OpenRead)
            .map(|op| op.p2);
        if open_read_root != Some(2) {
            return Err(format!(
                "expected OpenRead root page 2, got {open_read_root:?}"
            ));
        }

        Ok(())
    }

    #[test]
    fn test_codegen_select_values_multirow() -> Result<(), String> {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(
                    vec![
                        vec![
                            Expr::Literal(Literal::Integer(1), Span::ZERO),
                            Expr::Literal(Literal::String("alpha".to_owned()), Span::ZERO),
                        ],
                        vec![
                            Expr::Literal(Literal::Integer(2), Span::ZERO),
                            Expr::Literal(Literal::String("beta".to_owned()), Span::ZERO),
                        ],
                    ]
                    .into(),
                ),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &[], &ctx).map_err(|err| format!("{err:?}"))?;
        let prog = b.finish().map_err(|err| format!("{err:?}"))?;

        if !has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::Integer,
                Opcode::String8,
                Opcode::ResultRow,
                Opcode::Integer,
                Opcode::String8,
                Opcode::ResultRow,
                Opcode::Halt,
            ],
        ) {
            return Err(format!(
                "VALUES SELECT should emit one result row per VALUES row, got {:?}",
                opcode_sequence(&prog)
            ));
        }

        let result_row_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::ResultRow && op.p2 == 2)
            .count();
        if result_row_count != 2 {
            return Err(format!(
                "VALUES SELECT should emit two two-column ResultRow ops, got {result_row_count}"
            ));
        }

        Ok(())
    }

    #[test]
    fn test_codegen_select_values_rejects_mismatched_arity() -> Result<(), String> {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(
                    vec![
                        vec![Expr::Literal(Literal::Integer(1), Span::ZERO)],
                        vec![
                            Expr::Literal(Literal::Integer(2), Span::ZERO),
                            Expr::Literal(Literal::Integer(3), Span::ZERO),
                        ],
                    ]
                    .into(),
                ),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();

        match codegen_select(&mut b, &stmt, &[], &ctx) {
            Err(CodegenError::Unsupported(msg)) if msg.contains("same arity") => Ok(()),
            other => Err(format!("expected VALUES arity error, got {other:?}")),
        }
    }

    // === Test 2: INSERT VALUES ===
    #[test]
    fn test_codegen_insert_values() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![placeholder(1), placeholder(2)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::NewRowid,
                Opcode::Variable,
                Opcode::Variable,
                Opcode::MakeRecord,
                Opcode::Insert,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));
        // Transaction should be write (p2=1).
        let txn = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Transaction)
            .unwrap();
        assert_eq!(txn.p2, 1);
    }

    #[test]
    fn test_codegen_insert_values_uses_declared_defaults_for_omitted_columns() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec!["name".to_owned()],
            source: InsertSource::Values(vec![vec![placeholder(1)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'C',
                    default_value: None,
                },
                ColumnInfo {
                    name: "status".to_owned(),
                    affinity: 'C',
                    default_value: Some("'pending'".to_owned()),
                },
            ],
            indexes: vec![],
        }];
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::String8 && op.p4 == P4::Str("pending".to_owned()))
        );
    }

    #[test]
    fn test_codegen_insert_allows_duplicate_target_columns() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec!["b".to_owned(), "b".to_owned()],
            source: InsertSource::Values(vec![vec![placeholder(1), placeholder(2)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
    }

    #[test]
    fn test_codegen_insert_default_values_uses_declared_defaults() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::DefaultValues,
            upsert: vec![],
            returning: vec![],
        };
        let schema = vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "status".to_owned(),
                    affinity: 'C',
                    default_value: Some("'active'".to_owned()),
                },
                ColumnInfo {
                    name: "count".to_owned(),
                    affinity: 'd',
                    default_value: Some("42".to_owned()),
                },
            ],
            indexes: vec![],
        }];
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::String8 && op.p4 == P4::Str("active".to_owned()))
        );
        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::Integer && op.p1 == 42)
        );
    }

    #[test]
    fn test_codegen_insert_default_values_uses_expression_defaults() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::DefaultValues,
            upsert: vec![],
            returning: vec![],
        };
        let schema = vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "total".to_owned(),
                    affinity: 'd',
                    default_value: Some("(40 + 2)".to_owned()),
                },
            ],
            indexes: vec![],
        }];
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Add),
            "expression defaults should compile as expressions, not string literals"
        );
    }

    #[test]
    fn test_codegen_insert_default_values_rejects_unparseable_defaults() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::DefaultValues,
            upsert: vec![],
            returning: vec![],
        };
        let schema = vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![ColumnInfo {
                name: "broken".to_owned(),
                affinity: 'C',
                default_value: Some("('unterminated".to_owned()),
            }],
            indexes: vec![],
        }];
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap_err();
        assert!(
            matches!(err, CodegenError::Unsupported(ref msg) if msg.contains("failed to parse DEFAULT expression")),
            "unexpected error: {err:?}"
        );
    }

    // === Test: INSERT ... SELECT ===
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select_values() {
        // INSERT INTO t VALUES (1) parsed as InsertSource::Select
        let inner_values = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(vec![vec![placeholder(1)]].into()),
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };

        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Select(Box::new(inner_values)),
            upsert: vec![],
            returning: vec![],
        };

        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should look like normal INSERT VALUES
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::NewRowid,
                Opcode::Variable,
                Opcode::MakeRecord,
                Opcode::Insert,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select() {
        // Schema with source "s" and target "t".
        let schema = vec![
            TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo {
                        name: "a".to_owned(),
                        affinity: 'd',
                        default_value: None,
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
                        default_value: None,
                    },
                ],
                indexes: vec![],
            },
            TableSchema {
                name: "s".to_owned(),
                root_page: 3,
                columns: vec![
                    ColumnInfo {
                        name: "x".to_owned(),
                        affinity: 'd',
                        default_value: None,
                    },
                    ColumnInfo {
                        name: "y".to_owned(),
                        affinity: 'C',
                        default_value: None,
                    },
                ],
                indexes: vec![],
            },
        ];

        // INSERT INTO t SELECT * FROM s
        let inner_select = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Star],
                    from: Some(FromClause {
                        source: TableOrSubquery::Table {
                            name: QualifiedName::bare("s"),
                            alias: None,
                            index_hint: None,
                            time_travel: None,
                        },
                        joins: vec![],
                    }),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };

        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Select(Box::new(inner_select)),
            upsert: vec![],
            returning: vec![],
        };

        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::Column,
                Opcode::NewRowid,
                Opcode::MakeRecord,
                Opcode::Insert,
                Opcode::Next,
                Opcode::Close,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // Transaction should be write (p2=1).
        let txn = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Transaction)
            .unwrap();
        assert_eq!(txn.p2, 1);

        // OpenWrite should target table "t" (root_page=2).
        let open_write = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::OpenWrite)
            .unwrap();
        assert_eq!(open_write.p2, 2);

        // OpenRead should target table "s" (root_page=3).
        let open_read = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::OpenRead)
            .unwrap();
        assert_eq!(open_read.p2, 3);
    }

    // === Test: INSERT ... SELECT without FROM (expression-only) ===
    #[test]
    fn test_codegen_insert_select_without_from() {
        // INSERT INTO t SELECT 42, 'hello'
        let inner_select = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: Expr::Literal(Literal::Integer(42), Span::ZERO),
                            alias: None,
                        },
                        ResultColumn::Expr {
                            expr: Expr::Literal(Literal::String("hello".to_owned()), Span::ZERO),
                            alias: None,
                        },
                    ],
                    from: None,
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };

        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Select(Box::new(inner_select)),
            upsert: vec![],
            returning: vec![],
        };

        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should produce a single-row insert without OpenRead/Rewind/Next.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::Integer, // 42
                Opcode::String8, // 'hello'
                Opcode::NewRowid,
                Opcode::MakeRecord,
                Opcode::Insert,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // No OpenRead — no source table.
        assert!(prog.ops().iter().all(|op| op.opcode != Opcode::OpenRead));

        // Transaction should be write (p2=1).
        let txn = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Transaction)
            .unwrap();
        assert_eq!(txn.p2, 1);
    }

    // === Test 3: UPDATE by rowid ===
    #[test]
    fn test_codegen_update_by_rowid() {
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(placeholder(2)),
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Verify: reads ALL columns, then overwrites changed one.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::Variable, // new value
                Opcode::Variable, // rowid
                Opcode::OpenWrite,
                Opcode::NotExists,
                Opcode::Column,     // read existing col a
                Opcode::Column,     // read existing col b
                Opcode::Copy,       // overwrite b with new value
                Opcode::MakeRecord, // pack ALL columns
                Opcode::Insert,     // write back
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // MakeRecord should have 2 columns (the full record).
        let mr = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::MakeRecord)
            .unwrap();
        assert_eq!(mr.p2, 2); // ALL columns, not just the changed one.
    }

    #[test]
    fn test_codegen_update_notexists_jump_skips_insert_to_close() {
        // UPDATE t SET b = ?1 WHERE rowid = ?2 performs a read-modify-write:
        // NotExists guards the whole block, so when the rowid is absent the jump
        // must skip past the Insert (the REPLACE that writes the row back) and
        // land on Close. Otherwise a missing-rowid UPDATE would silently insert a
        // phantom row. `test_codegen_update_by_rowid` only checks the opcode
        // subsequence + MakeRecord arity; this pins the guard's jump target.
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(placeholder(2)),
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = prog.ops();

        let notexists = ops
            .iter()
            .position(|op| op.opcode == Opcode::NotExists)
            .expect("NotExists op present");
        let insert = ops
            .iter()
            .position(|op| op.opcode == Opcode::Insert)
            .expect("Insert (REPLACE write-back) op present");
        let close = ops
            .iter()
            .position(|op| op.opcode == Opcode::Close)
            .expect("Close op present");

        // The write-back Insert sits strictly between the guard and Close, so it
        // is part of the span the NotExists branch hops over.
        assert!(
            notexists < insert,
            "NotExists must precede the write-back Insert"
        );
        assert!(insert < close, "the write-back Insert must precede Close");

        // NotExists jumps to Close when the rowid is absent, skipping the Insert.
        assert_eq!(
            usize::try_from(ops[notexists].p2).unwrap(),
            close,
            "NotExists must jump to Close (skipping the write-back Insert) when the rowid is absent"
        );
    }

    #[test]
    fn test_codegen_update_makerecord_carries_table_affinity_string() {
        // UPDATE re-packs the full row with MakeRecord, whose P4 must be the
        // table's affinity string so each column value is coerced to its declared
        // affinity on write-back. affinity_string()/type_to_affinity are unit
        // tested in isolation, and test_codegen_update_by_rowid checks the
        // MakeRecord column count, but neither pins that the computed affinity
        // string actually reaches the emitted MakeRecord op's P4 operand.
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(placeholder(2)),
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        // Derive the expectation from the schema itself (no brittle literal): for
        // test_schema this is the two column affinities "dC".
        let expected_affinity = schema[0].affinity_string();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let make_record = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::MakeRecord)
            .expect("MakeRecord op present");
        match &make_record.p4 {
            P4::Affinity(aff) => assert_eq!(
                *aff, expected_affinity,
                "MakeRecord P4 affinity must equal the table's affinity_string()"
            ),
            _ => panic!("MakeRecord P4 must be P4::Affinity (got a different P4 variant)"),
        }
    }

    // === Test 4: DELETE by rowid ===
    #[test]
    fn test_codegen_delete_by_rowid() {
        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(placeholder(1)),
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_delete(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::Variable,
                Opcode::OpenWrite,
                Opcode::NotExists,
                Opcode::Delete,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));
    }

    #[test]
    fn test_codegen_delete_notexists_jump_skips_delete_to_close() {
        // DELETE FROM t WHERE rowid = ?1 guards the row delete with NotExists:
        // when the rowid is absent the NotExists jump must skip past Delete and
        // land on Close, so a missing row is a no-op rather than deleting whatever
        // the cursor currently points at. The subsequence check in
        // `test_codegen_delete_by_rowid` only proves the opcodes are present in
        // order; this pins the actual jump target.
        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(placeholder(1)),
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_delete(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = prog.ops();

        let notexists = ops
            .iter()
            .position(|op| op.opcode == Opcode::NotExists)
            .expect("NotExists op present");
        let delete = ops
            .iter()
            .position(|op| op.opcode == Opcode::Delete)
            .expect("Delete op present");
        let close = ops
            .iter()
            .position(|op| op.opcode == Opcode::Close)
            .expect("Close op present");

        // Delete sits strictly between the guard and Close, so it is exactly the
        // instruction the NotExists branch hops over.
        assert!(notexists < delete, "NotExists must precede Delete");
        assert!(delete < close, "Delete must precede Close");

        // NotExists jumps to the Close instruction when the rowid is absent.
        assert_eq!(
            usize::try_from(ops[notexists].p2).unwrap(),
            close,
            "NotExists must jump to Close (skipping Delete) when the rowid is absent"
        );
    }

    // === Test 5: Label resolution ===
    #[test]
    fn test_codegen_label_resolution() {
        let stmt = simple_select(&["a"], "t", Some(rowid_eq_param()));
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // All p2 fields that are jumps should have valid addresses (>= 0).
        for op in prog.ops() {
            if op.opcode.is_jump() {
                assert!(
                    op.p2 >= 0,
                    "unresolved jump at {:?}: p2 = {}",
                    op.opcode,
                    op.p2
                );
                assert!(
                    usize::try_from(op.p2).unwrap() <= prog.len(),
                    "jump target out of range at {:?}: p2 = {} (prog len = {})",
                    op.opcode,
                    op.p2,
                    prog.len()
                );
            }
        }
    }

    // === Test 6: Register allocation ===
    #[test]
    fn test_codegen_register_allocation() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![placeholder(1), placeholder(2)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // All register references (p1, p2, p3 where applicable) should be
        // within the allocated range.
        let max_reg = prog.register_count();
        assert!(max_reg > 0);

        // Variable instructions: p2 is the target register.
        for op in prog.ops() {
            if op.opcode == Opcode::Variable {
                assert!(
                    op.p2 >= 1 && op.p2 <= max_reg,
                    "Variable register out of range: p2 = {}, max = {}",
                    op.p2,
                    max_reg
                );
            }
        }
    }

    // === Test 7: Concurrent mode NewRowid ===
    #[test]
    fn test_codegen_concurrent_newrowid() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![placeholder(1)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext {
            concurrent_mode: true,
        };
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // In concurrent mode, NewRowid p3 should be non-zero.
        let nr = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::NewRowid)
            .unwrap();
        assert_ne!(
            nr.p3, 0,
            "NewRowid p3 should be non-zero in concurrent mode"
        );

        // In non-concurrent mode, p3 should be 0.
        let ctx_normal = CodegenContext::default();
        let mut b2 = ProgramBuilder::new();
        codegen_insert(&mut b2, &stmt, &schema, &ctx_normal).unwrap();
        let prog2 = b2.finish().unwrap();
        let nr2 = prog2
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::NewRowid)
            .unwrap();
        assert_eq!(nr2.p3, 0, "NewRowid p3 should be 0 in normal mode");
    }

    // === Test 8: SELECT full scan ===
    #[test]
    fn test_codegen_select_full_scan() {
        let stmt = star_select("t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::Next,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // ResultRow should cover 2 columns (a and b).
        let rr = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::ResultRow)
            .unwrap();
        assert_eq!(rr.p2, 2);
    }

    // === Test 9: SELECT with index ===
    #[test]
    fn test_codegen_select_with_index() {
        let stmt = simple_select(&["a"], "t", Some(col_eq_param("b", 1)));
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should use OpenRead on both table and index.
        let open_reads = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::OpenRead)
            .count();
        assert_eq!(open_reads, 2, "should open both table and index");

        // Should have SeekGE + IdxGT + IdxRowid + SeekRowid pattern.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::MakeRecord,
                Opcode::OpenRead,
                Opcode::OpenRead,
                Opcode::SeekGE,
                Opcode::IdxGT,
                Opcode::IdxRowid,
                Opcode::SeekRowid,
                Opcode::Column,
                Opcode::ResultRow,
            ]
        ));

        let variable = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Variable)
            .expect("Variable should load index probe parameter");
        let make_record = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::MakeRecord)
            .expect("MakeRecord should encode index probe key");
        assert_eq!(
            make_record.p1, variable.p2,
            "MakeRecord source should be Variable destination register"
        );
        assert_eq!(
            make_record.p2, 2,
            "probe key should include indexed value and synthetic low rowid"
        );
        let int64 = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Int64)
            .expect("Int64 should load i64::MIN for duplicate-range seek lower bound");
        assert_eq!(int64.p4, P4::Int64(i64::MIN));
        assert_eq!(
            make_record.p1 + 1,
            int64.p2,
            "MakeRecord should consume [param_reg, min_rowid_reg]"
        );
        let seek_ge = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::SeekGE)
            .expect("SeekGE should be emitted for index probe");
        assert_eq!(
            seek_ge.p3, make_record.p3,
            "SeekGE must read probe key from MakeRecord destination register"
        );
        let idx_gt = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::IdxGT)
            .expect("IdxGT should bound index equality duplicates");
        assert_eq!(
            idx_gt.p3, make_record.p3,
            "IdxGT must compare against the same probe key as SeekGE"
        );
        assert_eq!(
            idx_gt.p5, 1,
            "IdxGT should compare only the indexed value prefix, not the synthetic low rowid"
        );

        let is_null_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::IsNull)
            .count();
        assert!(
            is_null_count >= 1,
            "indexed equality should guard NULL probe"
        );

        let seek_rowid = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::SeekRowid)
            .expect("SeekRowid should follow IdxRowid");
        assert_ne!(
            seek_rowid.p2, 0,
            "SeekRowid miss target must not jump to pc=0"
        );
        let next = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Next)
            .expect("index equality path must iterate duplicates");
        assert_eq!(next.p1, 1, "Next should advance the index cursor");
    }

    #[test]
    fn test_codegen_select_unindexed_column_eq_uses_filtered_scan() {
        let stmt = simple_select(&["b"], "t", Some(col_eq_param("a", 2)));
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let open_reads = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::OpenRead)
            .count();
        assert_eq!(
            open_reads, 1,
            "unindexed equality should scan the table without opening an index"
        );
        assert!(
            !prog
                .ops()
                .iter()
                .any(|op| matches!(op.opcode, Opcode::SeekGE | Opcode::IdxGT | Opcode::IdxRowid)),
            "unindexed equality should not emit index-probe opcodes"
        );
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::Variable,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::Ne,
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::Next,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        let variable = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Variable)
            .expect("Variable should load the equality parameter");
        assert_eq!(variable.p1, 2, "numbered placeholder should be preserved");
        let ne = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Ne)
            .expect("filtered scan should skip rows that do not match");
        assert_eq!(ne.p1, variable.p2);
        assert_ne!(
            ne.p5 & 0x10,
            0,
            "WHERE equality must skip NULL comparisons instead of returning them"
        );

        let next = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Next)
            .expect("filtered scan should advance the table cursor");
        assert_eq!(next.p1, 0, "Next should advance the table cursor");
    }

    // === Test 10: INSERT RETURNING ===
    #[test]
    fn test_codegen_insert_returning() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![placeholder(1)]]),
            upsert: vec![],
            returning: vec![ResultColumn::Expr {
                expr: Expr::Column(ColumnRef::bare("rowid"), Span::ZERO),
                alias: None,
            }],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // With RETURNING, there should be a ResultRow after Insert.
        assert!(has_opcodes(
            &prog,
            &[Opcode::Insert, Opcode::ResultRow, Opcode::Close,]
        ));
    }

    // ===================================================================
    // CodegenError Display / Error trait tests
    // ===================================================================

    #[test]
    fn test_codegen_error_display_table_not_found() {
        let err = CodegenError::TableNotFound("users".to_owned());
        let msg = err.to_string();
        assert!(msg.contains("table not found"), "got: {msg}");
        assert!(msg.contains("users"), "got: {msg}");
    }

    #[test]
    fn test_codegen_error_display_column_not_found() {
        let err = CodegenError::ColumnNotFound {
            table: "users".to_owned(),
            column: "email".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("email"), "got: {msg}");
        assert!(msg.contains("users"), "got: {msg}");
    }

    #[test]
    fn test_codegen_error_display_unsupported() {
        let err = CodegenError::Unsupported("window functions".to_owned());
        let msg = err.to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
        assert!(msg.contains("window functions"), "got: {msg}");
    }

    #[test]
    fn test_codegen_error_is_error() {
        let err = CodegenError::TableNotFound("t".to_owned());
        assert!(std::error::Error::source(&err).is_none());
    }

    // ===================================================================
    // TableSchema method tests
    // ===================================================================

    #[test]
    fn test_table_schema_affinity_string() {
        let schema = TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo {
                    name: "id".to_owned(),
                    affinity: 'd',
                    default_value: None,
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'C',
                    default_value: None,
                },
                ColumnInfo {
                    name: "amount".to_owned(),
                    affinity: 'e',
                    default_value: None,
                },
            ],
            indexes: vec![],
        };
        assert_eq!(schema.affinity_string(), "dCe");
    }

    #[test]
    fn test_table_schema_column_index() {
        let schema = test_schema();
        // Case-insensitive lookup.
        assert_eq!(schema[0].column_index("a"), Some(0));
        assert_eq!(schema[0].column_index("A"), Some(0));
        assert_eq!(schema[0].column_index("b"), Some(1));
        assert_eq!(schema[0].column_index("z"), None);
    }

    #[test]
    fn test_table_schema_index_for_column() {
        let schema = test_schema_with_index();
        let table = &schema[0];
        // Should find idx_t_b (leftmost column is "b").
        let found = table.index_for_column("b");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "idx_t_b");

        // Case-insensitive.
        let found = table.index_for_column("B");
        assert!(found.is_some());

        // No index on column "a".
        assert!(table.index_for_column("a").is_none());
    }

    #[test]
    fn test_table_schema_affinity_string_empty() {
        let schema = TableSchema {
            name: "empty".to_owned(),
            root_page: 2,
            columns: vec![],
            indexes: vec![],
        };
        assert_eq!(schema.affinity_string(), "");
    }

    // ===================================================================
    // CodegenContext tests
    // ===================================================================

    #[test]
    fn test_codegen_context_default() {
        let ctx = CodegenContext::default();
        assert!(!ctx.concurrent_mode);
    }

    // ===================================================================
    // Codegen error path tests
    // ===================================================================

    #[test]
    fn test_codegen_select_table_not_found() {
        let stmt = star_select("nonexistent");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_select(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::TableNotFound(_)));
    }

    #[test]
    fn test_codegen_insert_table_not_found() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("nonexistent"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![placeholder(1)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_insert(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::TableNotFound(_)));
    }

    #[test]
    fn test_codegen_update_table_not_found() {
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("nonexistent"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![],
            from: None,
            where_clause: None,
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_update(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::TableNotFound(_)));
    }

    #[test]
    fn test_codegen_update_unknown_assignment_column_returns_error() {
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("no_such_col".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(*rowid_eq_param()),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_update(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(
            err,
            CodegenError::ColumnNotFound { ref column, .. } if column == "no_such_col"
        ));
    }

    #[test]
    fn test_codegen_update_requires_rowid_predicate() {
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: None,
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_update(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::Unsupported(_)));
    }

    #[test]
    fn test_codegen_update_rowid_anonymous_bind_is_offset_after_assignments() {
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("rowid"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO)),
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let vars: Vec<_> = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Variable)
            .collect();
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].p1, 1, "first bind should be SET assignment");
        assert_eq!(vars[1].p1, 2, "rowid bind should follow SET binds");
    }

    #[test]
    fn test_codegen_select_unindexed_filter_projected_column_uses_filtered_scan() {
        let stmt = simple_select(&["a"], "t", Some(col_eq_param("a", 1)));
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Variable,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::Ne,
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::Next,
            ]
        ));
        let column_reads: Vec<_> = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Column)
            .collect();
        assert_eq!(
            column_reads.len(),
            2,
            "filtering and projecting the same unindexed column should read it for both the predicate and output"
        );
        assert!(
            column_reads.iter().all(|op| op.p2 == 0),
            "both reads should target column a"
        );
        let ne = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Ne)
            .expect("filtered scan should skip non-matching rows");
        assert_ne!(
            ne.p5 & 0x10,
            0,
            "WHERE equality must skip NULL comparisons instead of returning them"
        );
    }

    #[test]
    fn test_codegen_select_unsupported_projection_expression_is_error() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Between {
                            expr: Box::new(Expr::Literal(Literal::Integer(5), Span::ZERO)),
                            low: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                            high: Box::new(Expr::Literal(Literal::Integer(10), Span::ZERO)),
                            not: false,
                            span: Span::ZERO,
                        },
                        alias: None,
                    }],
                    from: Some(FromClause {
                        source: TableOrSubquery::Table {
                            name: QualifiedName::bare("t"),
                            alias: None,
                            index_hint: None,
                            time_travel: None,
                        },
                        joins: vec![],
                    }),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_select(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::Unsupported(_)));
    }

    #[test]
    fn test_codegen_delete_table_not_found() {
        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("nonexistent"),
                alias: None,
                index_hint: None,
                time_travel: None,
            },
            where_clause: None,
            returning: vec![],
            order_by: vec![],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_delete(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::TableNotFound(_)));
    }

    // ===================================================================
    // Rowid pseudo-column projection tests (bd-3r24)
    // ===================================================================

    #[test]
    fn test_codegen_select_rowid_projection() {
        let stmt = simple_select(&["rowid"], "t", None);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should contain OP_Rowid (not OP_Column) for the rowid reference.
        assert!(
            has_opcodes(&prog, &[Opcode::Rowid, Opcode::ResultRow]),
            "SELECT rowid should emit OP_Rowid"
        );
    }

    #[test]
    fn test_codegen_select_rowid_alias_underscore() {
        let stmt = simple_select(&["_rowid_"], "t", None);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            has_opcodes(&prog, &[Opcode::Rowid, Opcode::ResultRow]),
            "SELECT _rowid_ should emit OP_Rowid"
        );
    }

    #[test]
    fn test_codegen_select_oid_alias() {
        let stmt = simple_select(&["oid"], "t", None);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            has_opcodes(&prog, &[Opcode::Rowid, Opcode::ResultRow]),
            "SELECT oid should emit OP_Rowid"
        );
    }

    #[test]
    fn test_codegen_select_rowid_with_columns() {
        // SELECT rowid, a, b FROM t — mixed pseudo-column and real columns.
        let stmt = simple_select(&["rowid", "a", "b"], "t", None);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should contain OP_Rowid followed by two OP_Column opcodes.
        assert!(
            has_opcodes(
                &prog,
                &[
                    Opcode::Rowid,
                    Opcode::Column,
                    Opcode::Column,
                    Opcode::ResultRow
                ]
            ),
            "SELECT rowid, a, b should emit Rowid + Column + Column"
        );
    }

    #[test]
    fn test_codegen_select_rowid_case_insensitive() {
        // Uppercase ROWID should also be recognized.
        let stmt = simple_select(&["ROWID"], "t", None);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            has_opcodes(&prog, &[Opcode::Rowid, Opcode::ResultRow]),
            "SELECT ROWID should emit OP_Rowid (case-insensitive)"
        );
    }
}
