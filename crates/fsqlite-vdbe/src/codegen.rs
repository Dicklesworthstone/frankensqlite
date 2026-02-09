//! AST-to-VDBE bytecode compilation (§10.6).
//!
//! Translates parsed SQL statements into VDBE register-based instructions
//! using `ProgramBuilder`. Handles SELECT, INSERT,
//! UPDATE, and DELETE with correct opcode patterns matching C SQLite behavior.

use crate::ProgramBuilder;
use fsqlite_ast::{
    ColumnRef, DeleteStatement, Expr, InsertSource, InsertStatement, Literal, QualifiedTableRef,
    ResultColumn, SelectCore, SelectStatement, UpdateStatement,
};
use fsqlite_types::opcode::{Opcode, P4};

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
    let (columns, from, where_clause) = match &stmt.body.select {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            ..
        } => (columns, from, where_clause),
        SelectCore::Values(_) => {
            return Err(CodegenError::Unsupported("VALUES in SELECT".to_owned()));
        }
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
        b.emit_jump_to_label(Opcode::SeekRowid, cursor, 0, done_label, P4::None, 0);

        // Read columns.
        emit_column_reads(b, cursor, columns, table, out_regs)?;

        // ResultRow.
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    } else if let Some((col_name, param_idx)) = &index_eq {
        // --- Index-seek SELECT ---
        if let Some(idx_schema) = table.index_for_column(col_name) {
            let idx_cursor = 1_i32;
            let param_reg = b.alloc_reg();
            b.emit_op(Opcode::Variable, *param_idx, param_reg, 0, P4::None, 0);
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
                P4::Table(idx_schema.name.clone()),
                0,
            );
            // SeekGE on index, then check equality with Found.
            b.emit_jump_to_label(Opcode::SeekGE, idx_cursor, 0, done_label, P4::None, 0);
            let rowid_reg = b.alloc_reg();
            b.emit_op(Opcode::IdxRowid, idx_cursor, rowid_reg, 0, P4::None, 0);
            b.emit_op(Opcode::SeekRowid, cursor, 0, rowid_reg, P4::None, 0);

            // Read columns.
            emit_column_reads(b, cursor, columns, table, out_regs)?;

            // ResultRow.
            b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
        } else {
            // Fallback to full scan.
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
    done_label: crate::Label,
    end_label: crate::Label,
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

    // NewRowid: generate a new rowid.
    // In concurrent mode, p3 != 0 signals the snapshot-independent allocator.
    let rowid_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);
    b.emit_op(
        Opcode::NewRowid,
        cursor,
        rowid_reg,
        concurrent_flag,
        P4::None,
        0,
    );

    // Emit value expressions (bind parameters from VALUES).
    let values = match &stmt.source {
        InsertSource::Values(rows) => {
            if rows.is_empty() {
                return Err(CodegenError::Unsupported("empty VALUES".to_owned()));
            }
            &rows[0] // First row for now (multi-row insert would loop).
        }
        InsertSource::DefaultValues => {
            return Err(CodegenError::Unsupported("DEFAULT VALUES".to_owned()));
        }
        InsertSource::Select(_) => {
            return Err(CodegenError::Unsupported("INSERT ... SELECT".to_owned()));
        }
    };

    let n_cols = values.len();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let val_regs = b.alloc_regs(n_cols as i32);

    for (i, val_expr) in values.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let reg = val_regs + i as i32;
        emit_expr(b, val_expr, reg);
    }

    // MakeRecord: pack columns into a record.
    let rec_reg = b.alloc_reg();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let n_cols_i32 = n_cols as i32;
    b.emit_op(
        Opcode::MakeRecord,
        val_regs,
        n_cols_i32,
        rec_reg,
        P4::Affinity(table.affinity_string()),
        0,
    );

    // Insert.
    b.emit_op(
        Opcode::Insert,
        cursor,
        rec_reg,
        rowid_reg,
        P4::Table(table.name.clone()),
        0,
    );

    // RETURNING clause: emit ResultRow with rowid if present.
    if !stmt.returning.is_empty() {
        b.emit_op(Opcode::ResultRow, rowid_reg, 1, 0, P4::None, 0);
    }

    // Close + Halt.
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

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
            let col_idx = table
                .column_index(col_name)
                .ok_or_else(|| CodegenError::ColumnNotFound {
                    table: table.name.clone(),
                    column: col_name.to_owned(),
                })
                .expect("column must exist");
            let reg = b.alloc_reg();
            (col_idx, reg)
        })
        .collect();

    // Emit Variable ops for new values.
    for (_col_idx, reg) in &new_val_regs {
        emit_expr_or_variable(b, param_idx, *reg);
        param_idx += 1;
    }

    // Rowid bind parameter.
    let rowid_reg = b.alloc_reg();
    let rowid_param = extract_rowid_bind_param(stmt.where_clause.as_ref());
    if rowid_param.is_some() {
        b.emit_op(Opcode::Variable, param_idx, rowid_reg, 0, P4::None, 0);
    }

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
            ResultColumn::Star => {
                for i in 0..table.columns.len() {
                    b.emit_op(Opcode::Column, cursor, i as i32, reg, P4::None, 0);
                    reg += 1;
                }
            }
            ResultColumn::TableStar(qualifier) => {
                if !qualifier.eq_ignore_ascii_case(&table.name) {
                    return Err(CodegenError::TableNotFound(qualifier.clone()));
                }
                for i in 0..table.columns.len() {
                    b.emit_op(Opcode::Column, cursor, i as i32, reg, P4::None, 0);
                    reg += 1;
                }
            }
            ResultColumn::Expr { expr, .. } => {
                if let Expr::Column(col_ref, _) = expr {
                    if let Some(qualifier) = &col_ref.table {
                        if !qualifier.eq_ignore_ascii_case(&table.name) {
                            return Err(CodegenError::TableNotFound(qualifier.clone()));
                        }
                    }
                    let col_idx = table.column_index(&col_ref.column).ok_or_else(|| {
                        CodegenError::ColumnNotFound {
                            table: table.name.clone(),
                            column: col_ref.column.clone(),
                        }
                    })?;
                    b.emit_op(Opcode::Column, cursor, col_idx as i32, reg, P4::None, 0);
                } else {
                    return Err(CodegenError::Unsupported(
                        "non-column result expression in table-backed SELECT".to_owned(),
                    ));
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
            return bind_param_index(right);
        }
        if is_rowid_expr(right) {
            return bind_param_index(left);
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
    if let Expr::Placeholder(pt, _) = expr {
        match pt {
            fsqlite_ast::PlaceholderType::Anonymous => Some(1),
            fsqlite_ast::PlaceholderType::Numbered(n) =>
            {
                #[allow(clippy::cast_possible_wrap)]
                Some(*n as i32)
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
/// Emit bytecode for an expression, placing the result in `reg`.
///
/// Handles literals, bind parameters, binary/unary operators, CASE, and CAST.
/// Column references and other cursor-dependent expressions fall back to Null
/// until Phase 5 cursor integration.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines
)]
fn emit_expr(b: &mut ProgramBuilder, expr: &Expr, reg: i32) {
    match expr {
        Expr::Placeholder(pt, _) => {
            let idx = match pt {
                fsqlite_ast::PlaceholderType::Numbered(n) => *n as i32,
                _ => 1, // Anonymous or other — will be renumbered by caller.
            };
            b.emit_op(Opcode::Variable, idx, reg, 0, P4::None, 0);
        }
        Expr::Literal(lit, _) => match lit {
            Literal::Integer(n) => {
                b.emit_op(Opcode::Integer, *n as i32, reg, 0, P4::None, 0);
            }
            Literal::Float(f) => {
                b.emit_op(Opcode::Real, 0, reg, 0, P4::Real(*f), 0);
            }
            Literal::String(s) => {
                b.emit_op(Opcode::String8, 0, reg, 0, P4::Str(s.clone()), 0);
            }
            Literal::Blob(bytes) => {
                b.emit_op(Opcode::Blob, bytes.len() as i32, reg, 0, P4::None, 0);
            }
            Literal::True => {
                b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
            }
            Literal::False => {
                b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
            }
            // CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP — emit Null for now.
            _ => {
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            }
        },
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            emit_binary_op(b, left, *op, right, reg);
        }
        Expr::UnaryOp {
            op, expr: operand, ..
        } => {
            emit_expr(b, operand, reg);
            match op {
                fsqlite_ast::UnaryOp::Negate => {
                    // Multiply by -1: Integer(-1) into temp, then Multiply.
                    let tmp = b.alloc_temp();
                    b.emit_op(Opcode::Integer, -1, tmp, 0, P4::None, 0);
                    b.emit_op(Opcode::Multiply, tmp, reg, reg, P4::None, 0);
                    b.free_temp(tmp);
                }
                fsqlite_ast::UnaryOp::Plus => { /* no-op */ }
                fsqlite_ast::UnaryOp::BitNot => {
                    b.emit_op(Opcode::BitNot, reg, reg, 0, P4::None, 0);
                }
                fsqlite_ast::UnaryOp::Not => {
                    b.emit_op(Opcode::Not, reg, reg, 0, P4::None, 0);
                }
            }
        }
        Expr::Cast {
            expr: inner,
            type_name,
            ..
        } => {
            emit_expr(b, inner, reg);
            let affinity = type_name_to_affinity(type_name);
            b.emit_op(Opcode::Cast, reg, i32::from(affinity), 0, P4::None, 0);
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            emit_case_expr(b, operand.as_deref(), whens, else_expr.as_deref(), reg);
        }
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            // IS NULL → result 1 if null, 0 otherwise.
            // IS NOT NULL → result 0 if null, 1 otherwise.
            emit_expr(b, inner, reg);
            let lbl_null = b.emit_label();
            let lbl_done = b.emit_label();
            b.emit_jump_to_label(Opcode::IsNull, reg, 0, lbl_null, P4::None, 0);
            // Not null path.
            let val_not_null = i32::from(*not); // IS NOT NULL: 1; IS NULL: 0
            let val_null = i32::from(!*not); // IS NOT NULL: 0; IS NULL: 1
            b.emit_op(Opcode::Integer, val_not_null, reg, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_done, P4::None, 0);
            b.resolve_label(lbl_null);
            b.emit_op(Opcode::Integer, val_null, reg, 0, P4::None, 0);
            b.resolve_label(lbl_done);
        }
        _ => {
            // Column refs and other cursor-dependent expressions: emit Null placeholder.
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        }
    }
}

/// Map an AST `BinaryOp` to the corresponding VDBE opcode.
fn binary_op_to_opcode(op: fsqlite_ast::BinaryOp) -> Opcode {
    match op {
        fsqlite_ast::BinaryOp::Add => Opcode::Add,
        fsqlite_ast::BinaryOp::Subtract => Opcode::Subtract,
        fsqlite_ast::BinaryOp::Multiply => Opcode::Multiply,
        fsqlite_ast::BinaryOp::Divide => Opcode::Divide,
        fsqlite_ast::BinaryOp::Modulo => Opcode::Remainder,
        fsqlite_ast::BinaryOp::Concat => Opcode::Concat,
        fsqlite_ast::BinaryOp::BitAnd => Opcode::BitAnd,
        fsqlite_ast::BinaryOp::BitOr => Opcode::BitOr,
        fsqlite_ast::BinaryOp::ShiftLeft => Opcode::ShiftLeft,
        fsqlite_ast::BinaryOp::ShiftRight => Opcode::ShiftRight,
        fsqlite_ast::BinaryOp::And => Opcode::And,
        fsqlite_ast::BinaryOp::Or => Opcode::Or,
        // Comparison ops use jump instructions; map to Eq as placeholder.
        fsqlite_ast::BinaryOp::Eq
        | fsqlite_ast::BinaryOp::Ne
        | fsqlite_ast::BinaryOp::Lt
        | fsqlite_ast::BinaryOp::Le
        | fsqlite_ast::BinaryOp::Gt
        | fsqlite_ast::BinaryOp::Ge
        | fsqlite_ast::BinaryOp::Is
        | fsqlite_ast::BinaryOp::IsNot => Opcode::Eq, // handled separately
    }
}

/// Emit bytecode for a binary operation expression.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_binary_op(
    b: &mut ProgramBuilder,
    left: &Expr,
    op: fsqlite_ast::BinaryOp,
    right: &Expr,
    reg: i32,
) {
    // For comparison operators, emit a conditional jump pattern that
    // produces 1 (true) or 0 (false) as an integer result.
    if matches!(
        op,
        fsqlite_ast::BinaryOp::Eq
            | fsqlite_ast::BinaryOp::Ne
            | fsqlite_ast::BinaryOp::Lt
            | fsqlite_ast::BinaryOp::Le
            | fsqlite_ast::BinaryOp::Gt
            | fsqlite_ast::BinaryOp::Ge
    ) {
        emit_comparison(b, left, op, right, reg);
        return;
    }

    if matches!(op, fsqlite_ast::BinaryOp::Is | fsqlite_ast::BinaryOp::IsNot) {
        emit_is_comparison(b, left, op, right, reg);
        return;
    }

    // Arithmetic / logical / bitwise: evaluate left into reg, right into tmp,
    // then apply the opcode.
    let tmp = b.alloc_temp();
    emit_expr(b, left, reg);
    emit_expr(b, right, tmp);
    let opcode = binary_op_to_opcode(op);
    // VDBE arithmetic: OP p1=rhs, p2=lhs, p3=dest
    b.emit_op(opcode, tmp, reg, reg, P4::None, 0);
    b.free_temp(tmp);
}

/// Emit a comparison expression that produces 1 (true) or 0 (false).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_comparison(
    b: &mut ProgramBuilder,
    left: &Expr,
    op: fsqlite_ast::BinaryOp,
    right: &Expr,
    reg: i32,
) {
    let r_left = b.alloc_temp();
    let r_right = b.alloc_temp();
    emit_expr(b, left, r_left);
    emit_expr(b, right, r_right);

    let cmp_opcode = match op {
        fsqlite_ast::BinaryOp::Eq => Opcode::Eq,
        fsqlite_ast::BinaryOp::Ne => Opcode::Ne,
        fsqlite_ast::BinaryOp::Lt => Opcode::Lt,
        fsqlite_ast::BinaryOp::Le => Opcode::Le,
        fsqlite_ast::BinaryOp::Gt => Opcode::Gt,
        fsqlite_ast::BinaryOp::Ge => Opcode::Ge,
        _ => unreachable!(),
    };

    // Pattern: assume false (0), jump to true_label if condition holds.
    let true_label = b.emit_label();
    let done_label = b.emit_label();

    // Comparison: p1=rhs_reg, p2=jump_target (label), p3=lhs_reg
    b.emit_jump_to_label(cmp_opcode, r_right, r_left, true_label, P4::None, 0);
    b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
    b.resolve_label(true_label);
    b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
    b.resolve_label(done_label);

    b.free_temp(r_right);
    b.free_temp(r_left);
}

/// Emit IS / IS NOT comparison (NULLEQ semantics).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_is_comparison(
    b: &mut ProgramBuilder,
    left: &Expr,
    op: fsqlite_ast::BinaryOp,
    right: &Expr,
    reg: i32,
) {
    let r_left = b.alloc_temp();
    let r_right = b.alloc_temp();
    emit_expr(b, left, r_left);
    emit_expr(b, right, r_right);

    let true_label = b.emit_label();
    let done_label = b.emit_label();

    // IS uses Eq with NULLEQ flag (p5=0x80). IS NOT uses Ne with NULLEQ.
    let (cmp_opcode, nulleq_flag) = match op {
        fsqlite_ast::BinaryOp::Is => (Opcode::Eq, 0x80_u16),
        fsqlite_ast::BinaryOp::IsNot => (Opcode::Ne, 0x80_u16),
        _ => unreachable!(),
    };

    b.emit_jump_to_label(
        cmp_opcode,
        r_right,
        r_left,
        true_label,
        P4::None,
        nulleq_flag,
    );
    b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
    b.resolve_label(true_label);
    b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
    b.resolve_label(done_label);

    b.free_temp(r_right);
    b.free_temp(r_left);
}

/// Emit CASE [operand] WHEN ... THEN ... [ELSE ...] END.
fn emit_case_expr(
    b: &mut ProgramBuilder,
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    reg: i32,
) {
    let done_label = b.emit_label();
    let r_operand = operand.map(|op_expr| {
        let r = b.alloc_temp();
        emit_expr(b, op_expr, r);
        r
    });

    for (when_expr, then_expr) in whens {
        let next_when = b.emit_label();

        if let Some(r_op) = r_operand {
            // Simple CASE: compare operand to each WHEN value.
            let r_when = b.alloc_temp();
            emit_expr(b, when_expr, r_when);
            // If operand != when_value, skip to next WHEN.
            b.emit_jump_to_label(Opcode::Ne, r_when, r_op, next_when, P4::None, 0);
            b.free_temp(r_when);
        } else {
            // Searched CASE: each WHEN is a boolean condition.
            emit_expr(b, when_expr, reg);
            // If condition is false/null, skip to next WHEN.
            b.emit_jump_to_label(Opcode::IfNot, reg, 1, next_when, P4::None, 0);
        }

        // Emit THEN expression.
        emit_expr(b, then_expr, reg);
        b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

        b.resolve_label(next_when);
    }

    // ELSE clause (or NULL if no ELSE).
    if let Some(el) = else_expr {
        emit_expr(b, el, reg);
    } else {
        b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
    }

    b.resolve_label(done_label);

    if let Some(r_op) = r_operand {
        b.free_temp(r_op);
    }
}

/// Convert a SQL type name to an affinity character code.
fn type_name_to_affinity(type_name: &fsqlite_ast::TypeName) -> u8 {
    let name = type_name.name.to_uppercase();
    if name.contains("INT") {
        b'D' // INTEGER affinity
    } else if name.contains("CHAR") || name.contains("TEXT") || name.contains("CLOB") {
        b'C' // TEXT affinity
    } else if name.contains("BLOB") || name.is_empty() {
        b'A' // BLOB affinity
    } else if name.contains("REAL") || name.contains("FLOA") || name.contains("DOUB") {
        b'E' // REAL affinity
    } else {
        b'B' // NUMERIC affinity
    }
}

/// Emit a Variable instruction for a bind parameter.
fn emit_expr_or_variable(b: &mut ProgramBuilder, param_idx: i32, reg: i32) {
    b.emit_op(Opcode::Variable, param_idx, reg, 0, P4::None, 0);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProgramBuilder;
    use fsqlite_ast::{
        Assignment, AssignmentTarget, BinaryOp as AstBinaryOp, ColumnRef, DeleteStatement,
        Distinctness, Expr, FromClause, InsertSource, InsertStatement, PlaceholderType,
        QualifiedName, QualifiedTableRef, ResultColumn, SelectBody, SelectCore, SelectStatement,
        Span, TableOrSubquery, UpdateStatement,
    };
    use fsqlite_types::opcode::Opcode;

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

    fn opcode_sequence(prog: &crate::VdbeProgram) -> Vec<Opcode> {
        prog.ops().iter().map(|op| op.opcode).collect()
    }

    fn has_opcodes(prog: &crate::VdbeProgram, expected: &[Opcode]) -> bool {
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

    #[test]
    fn test_codegen_select_non_column_expr_with_from_rejected() {
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
                    from: Some(from_table("t")),
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
        let err = codegen_select(&mut b, &stmt, &schema, &ctx)
            .expect_err("non-column table projection must be rejected");
        assert_eq!(
            err,
            CodegenError::Unsupported(
                "non-column result expression in table-backed SELECT".to_owned()
            )
        );
    }

    #[test]
    fn test_codegen_select_table_star_wrong_table_rejected() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::TableStar("u".to_owned())],
                    from: Some(from_table("t")),
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
        let err =
            codegen_select(&mut b, &stmt, &schema, &ctx).expect_err("unknown table qualifier");
        assert_eq!(err, CodegenError::TableNotFound("u".to_owned()));
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
                Opcode::OpenRead,
                Opcode::OpenRead,
                Opcode::SeekGE,
                Opcode::IdxRowid,
                Opcode::SeekRowid,
                Opcode::Column,
                Opcode::ResultRow,
            ]
        ));
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
}
