//! AST-to-VDBE bytecode compilation (§10.6).
//!
//! Translates parsed SQL statements into VDBE register-based instructions
//! using the `ProgramBuilder` from `fsqlite-types`. Handles SELECT, INSERT,
//! UPDATE, and DELETE with correct opcode patterns matching C SQLite behavior.

use fsqlite_ast::{
    ColumnRef, DeleteStatement, Expr, InsertSource, InsertStatement, Literal, PlaceholderType,
    QualifiedTableRef, ResultColumn, SelectCore, SelectStatement, UpdateStatement,
};
use fsqlite_types::opcode::{Label, Opcode, P4, ProgramBuilder};

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
/// Handles two patterns:
/// 1. **Rowid lookup**: `SELECT cols FROM t WHERE rowid = ?`
/// 2. **Full table scan**: `SELECT cols FROM t`
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
        SelectCore::Values(_) => {
            return Err(CodegenError::Unsupported("VALUES in SELECT".to_owned()));
        }
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

    // Determine the table from the FROM clause.
    let from_clause = from
        .as_ref()
        .ok_or_else(|| CodegenError::Unsupported("SELECT without FROM".to_owned()))?;

    let table_name = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, .. } => &name.name,
        _ => {
            return Err(CodegenError::Unsupported(
                "non-table FROM source".to_owned(),
            ));
        }
    };

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
        let idx_schema = table.index_for_column(col_name).ok_or_else(|| {
            CodegenError::Unsupported(format!(
                "SELECT WHERE `{col_name} = ?` requires an index on `{col_name}`"
            ))
        })?;
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
        let idx_key_reg = b.alloc_reg();
        b.emit_op(Opcode::Column, idx_cursor, 0, idx_key_reg, P4::None, 0);
        b.emit_jump_to_label(Opcode::Ne, param_reg, idx_key_reg, done_label, P4::None, 0);

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

    match &stmt.source {
        InsertSource::Values(rows) => {
            if rows.is_empty() {
                return Err(CodegenError::Unsupported("empty VALUES".to_owned()));
            }
            codegen_insert_values(b, rows, cursor, table, &stmt.returning, ctx)?;
        }
        InsertSource::Select(select_stmt) => {
            codegen_insert_select(b, select_stmt, cursor, table, schema, &stmt.returning, ctx)?;
        }
        InsertSource::DefaultValues => {
            return Err(CodegenError::Unsupported("DEFAULT VALUES".to_owned()));
        }
    }

    // Close + Halt.
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

    Ok(())
}

/// Emit the INSERT loop for `VALUES (row), (row), ...` (planner path).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unnecessary_wraps
)]
fn codegen_insert_values(
    b: &mut ProgramBuilder,
    rows: &[Vec<Expr>],
    cursor: i32,
    table: &TableSchema,
    returning: &[ResultColumn],
    ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let rowid_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);

    // Use first row to determine column count.
    let n_cols = rows[0].len();
    let val_regs = b.alloc_regs(n_cols as i32);
    let mut param_idx = 1_i32;

    for row_values in rows {
        b.emit_op(
            Opcode::NewRowid,
            cursor,
            rowid_reg,
            concurrent_flag,
            P4::None,
            0,
        );

        for (i, val_expr) in row_values.iter().enumerate() {
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
                Expr::Literal(_, _) => {
                    emit_expr(b, val_expr, reg)?;
                }
                _ => {
                    return Err(CodegenError::Unsupported(
                        "INSERT currently supports only literal and placeholder values".to_owned(),
                    ));
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
            0,
        );

        if !returning.is_empty() {
            b.emit_op(Opcode::ResultRow, rowid_reg, 1, 0, P4::None, 0);
        }
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
    write_cursor: i32,
    target_table: &TableSchema,
    schema: &[TableSchema],
    returning: &[ResultColumn],
    ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    if !select_stmt.body.compounds.is_empty() {
        return Err(CodegenError::Unsupported(
            "INSERT ... SELECT with compounds (UNION, etc.)".to_owned(),
        ));
    }

    // Extract columns and FROM from the inner SELECT.
    let (columns, from) = match &select_stmt.body.select {
        SelectCore::Select { columns, from, .. } => (columns, from),
        SelectCore::Values(rows) => {
            return codegen_insert_values(b, rows, write_cursor, target_table, returning, ctx);
        }
    };

    let from_clause = from
        .as_ref()
        .ok_or_else(|| CodegenError::Unsupported("INSERT ... SELECT without FROM".to_owned()))?;

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

    let n_cols = result_column_count(columns, src_table);
    let rowid_reg = b.alloc_reg();
    let val_regs = b.alloc_regs(n_cols);
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
    emit_column_reads(b, read_cursor, columns, src_table, val_regs)?;

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
        0,
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
                fsqlite_ast::AssignmentTarget::ColumnList(names) => {
                    names.first().map_or("", |n| n.as_str())
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
            Expr::Literal(_, _) => {
                emit_expr(b, &assign.value, reg)?;
            }
            _ => {
                return Err(CodegenError::Unsupported(
                    "UPDATE currently supports only literal and placeholder assignments".to_owned(),
                ));
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
    b.emit_jump_to_label(Opcode::NotExists, cursor, 0, done_label, P4::None, 0);
    // Patch: NotExists needs the rowid in p3.
    let ne_addr = b.current_addr() - 1;
    if let Some(op) = b.op_at_mut(ne_addr) {
        op.p3 = rowid_reg;
    }

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
    b.emit_jump_to_label(Opcode::NotExists, cursor, 0, done_label, P4::None, 0);
    let ne_addr = b.current_addr() - 1;
    if let Some(op) = b.op_at_mut(ne_addr) {
        op.p3 = rowid_reg;
    }

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
                                column: col_ref.column.clone(),
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
            return Some(col_ref.column.clone());
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

/// Emit an expression value into a register.
///
/// For bind parameters, emits a Variable instruction.
/// For literals, emits the appropriate constant instruction.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
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
            Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
                // Placeholder: emit NULL for now (datetime functions handle these).
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                Ok(())
            }
        },
        _ => Err(CodegenError::Unsupported(
            "planner expression codegen for this expression type".to_owned(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_ast::{
        Assignment, AssignmentTarget, BinaryOp as AstBinaryOp, ColumnRef, DeleteStatement,
        Distinctness, Expr, FromClause, InsertSource, InsertStatement, Literal, PlaceholderType,
        QualifiedName, QualifiedTableRef, ResultColumn, SelectBody, SelectCore, SelectStatement,
        Span, TableOrSubquery, UpdateStatement,
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
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'C',
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
                },
                ColumnInfo {
                    name: "b".to_owned(),
                    affinity: 'C',
                },
            ],
            indexes: vec![IndexSchema {
                name: "idx_t_b".to_owned(),
                root_page: 3,
                columns: vec!["b".to_owned()],
            }],
        }]
    }

    fn from_table(name: &str) -> FromClause {
        FromClause {
            source: TableOrSubquery::Table {
                name: QualifiedName::bare(name),
                alias: None,
                index_hint: None,
            },
            joins: vec![],
        }
    }

    fn placeholder(n: u32) -> Expr {
        Expr::Placeholder(PlaceholderType::Numbered(n), Span::ZERO)
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
        assert_eq!(ops.len(), 8);

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

        assert_eq!(ops[5].opcode, Opcode::Null);
        assert_eq!(ops[5].p2, reg_current_time);

        assert_eq!(ops[6].opcode, Opcode::Null);
        assert_eq!(ops[6].p2, reg_current_date);

        assert_eq!(ops[7].opcode, Opcode::Null);
        assert_eq!(ops[7].p2, reg_current_timestamp);
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

    // === Test: INSERT ... SELECT ===
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select_values() {
        // INSERT INTO t VALUES (1) parsed as InsertSource::Select
        let inner_values = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Values(vec![vec![placeholder(1)]]),
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
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
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
                    },
                    ColumnInfo {
                        name: "y".to_owned(),
                        affinity: 'C',
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

    // === Test 4: DELETE by rowid ===
    #[test]
    fn test_codegen_delete_by_rowid() {
        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
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

        // Should have SeekGE + IdxRowid + SeekRowid pattern.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::MakeRecord,
                Opcode::OpenRead,
                Opcode::OpenRead,
                Opcode::SeekGE,
                Opcode::Column,
                Opcode::Ne,
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
                },
                ColumnInfo {
                    name: "name".to_owned(),
                    affinity: 'C',
                },
                ColumnInfo {
                    name: "amount".to_owned(),
                    affinity: 'e',
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
    fn test_codegen_select_where_without_supported_pattern_is_error() {
        let stmt = simple_select(&["a"], "t", Some(col_eq_param("a", 1)));
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_select(&mut b, &stmt, &schema, &ctx).expect_err("should fail");
        assert!(matches!(err, CodegenError::Unsupported(_)));
    }

    #[test]
    fn test_codegen_select_unsupported_projection_expression_is_error() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::BinaryOp {
                            left: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                            op: AstBinaryOp::Add,
                            right: Box::new(Expr::Literal(Literal::Integer(2), Span::ZERO)),
                            span: Span::ZERO,
                        },
                        alias: None,
                    }],
                    from: Some(FromClause {
                        source: TableOrSubquery::Table {
                            name: QualifiedName::bare("t"),
                            alias: None,
                            index_hint: None,
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
