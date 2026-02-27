//! AST-to-VDBE bytecode compilation (§10.6).
//!
//! Translates parsed SQL statements into VDBE register-based instructions
//! using `ProgramBuilder`. Handles SELECT, INSERT,
//! UPDATE, and DELETE with correct opcode patterns matching C SQLite behavior.

use std::cell::RefCell;

use crate::ProgramBuilder;
use fsqlite_ast::{
    ColumnRef, ConflictAction, DeleteStatement, Distinctness, Expr, FunctionArgs, InsertSource,
    InsertStatement, LimitClause, Literal, OrderingTerm, QualifiedTableRef, ResultColumn,
    SelectCore, SelectStatement, SortDirection, Statement, TableOrSubquery, UpdateStatement,
};
use fsqlite_parser::Parser as SqlParser;
use fsqlite_types::StrictColumnType;
use fsqlite_types::opcode::{Opcode, P4};

// ---------------------------------------------------------------------------
// Thread-local extra aggregate function names for UDF support (bd-2wt.3)
// ---------------------------------------------------------------------------
// Custom aggregate UDFs registered via Connection::register_aggregate_function
// need to be recognized by the codegen so they emit AggStep/AggFinal opcodes
// instead of PureFunc. A thread-local avoids threading the names through
// dozens of internal codegen helpers. Connection is !Send/!Sync so all codegen
// runs on a single thread.

thread_local! {
    static EXTRA_AGG_NAMES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Set extra aggregate function names for the current codegen invocation.
///
/// Called by Connection before codegen to make custom aggregates visible.
/// Names should be lowercase.
pub fn set_extra_aggregate_names(names: Vec<String>) {
    EXTRA_AGG_NAMES.with(|n| *n.borrow_mut() = names);
}

/// Clear extra aggregate function names after codegen completes.
pub fn clear_extra_aggregate_names() {
    EXTRA_AGG_NAMES.with(|n| n.borrow_mut().clear());
}

// ---------------------------------------------------------------------------
// Conflict resolution flags for Insert opcode p5 field
// ---------------------------------------------------------------------------
// These match SQLite's OE_* constants for on-error conflict handling.
// The low 4 bits of p5 encode the conflict action.

/// No conflict clause (default behavior: abort on constraint violation).
const OE_ABORT: u16 = 2;
/// ROLLBACK on conflict.
const OE_ROLLBACK: u16 = 1;
/// FAIL on conflict (abort statement but don't rollback transaction).
const OE_FAIL: u16 = 3;
/// IGNORE conflicting row (skip insert without error).
const OE_IGNORE: u16 = 4;
/// REPLACE conflicting row (delete old, insert new).
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
    /// Type affinity character: 'D'/'d' (integer), 'E'/'e' (real), 'B' (text),
    /// 'C' (numeric), 'A' or other (blob/none).
    pub affinity: char,
    /// True if this column is the INTEGER PRIMARY KEY (rowid alias).
    /// Column reads for IPK columns must emit `Rowid` instead of `Column`
    /// because the value is stored as the B-tree key, not in the data record.
    pub is_ipk: bool,
    /// Type name as written in the CREATE TABLE statement (e.g. "TEXT", "INTEGER").
    pub type_name: Option<String>,
    /// True if the column has a NOT NULL constraint.
    pub notnull: bool,
    /// True if the column has a UNIQUE constraint.
    pub unique: bool,
    /// Default value expression as SQL text (e.g. "'open'", "0", "CURRENT_TIMESTAMP").
    pub default_value: Option<String>,
    /// Strict type for STRICT tables; `None` for non-STRICT tables.
    pub strict_type: Option<StrictColumnType>,
}

impl ColumnInfo {
    /// Create a basic `ColumnInfo` without type/notnull/default metadata.
    #[must_use]
    pub fn basic(name: impl Into<String>, affinity: char, is_ipk: bool) -> Self {
        Self {
            name: name.into(),
            affinity,
            is_ipk,
            type_name: None,
            notnull: false,
            unique: false,
            default_value: None,
            strict_type: None,
        }
    }
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
    /// Whether this table uses SQLite STRICT typing rules.
    pub strict: bool,
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

    /// STRICT type-check pattern for `Opcode::TypeCheck` (`I`,`R`,`T`,`L`,`A`).
    #[must_use]
    pub fn strict_type_pattern(&self) -> Option<String> {
        if !self.strict {
            return None;
        }
        Some(
            self.columns
                .iter()
                .map(|col| strict_type_code(col.strict_type))
                .collect(),
        )
    }
}

fn strict_type_code(strict_type: Option<StrictColumnType>) -> char {
    match strict_type.unwrap_or(StrictColumnType::Any) {
        StrictColumnType::Integer => 'I',
        StrictColumnType::Real => 'R',
        StrictColumnType::Text => 'T',
        StrictColumnType::Blob => 'L',
        StrictColumnType::Any => 'A',
    }
}

fn emit_strict_type_check(b: &mut ProgramBuilder, table: &TableSchema, first_reg: i32) {
    if let Some(pattern) = table.strict_type_pattern() {
        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        let n_cols = table.columns.len() as i32;
        b.emit_op(
            Opcode::TypeCheck,
            first_reg,
            n_cols,
            0,
            P4::Affinity(pattern),
            0,
        );
    }
}

/// Configuration for the code generator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodegenContext {
    /// Whether we're in `BEGIN CONCURRENT` mode.
    /// When true, `OP_NewRowid` uses the snapshot-independent allocator.
    pub concurrent_mode: bool,
    /// Optional column index for an `INTEGER PRIMARY KEY` rowid alias on the
    /// target table. Used by INSERT DEFAULT VALUES to keep the aliased column
    /// in sync with the generated rowid.
    pub rowid_alias_col_idx: Option<usize>,
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

/// Count anonymous placeholders in an expression tree.
///
/// Used by `codegen_update` to correctly number placeholders when bytecode
/// emission order differs from SQL textual order (WHERE is emitted before SET,
/// but SET placeholders appear first in the SQL text).
fn count_anon_placeholders(expr: &Expr) -> u32 {
    match expr {
        Expr::Placeholder(fsqlite_ast::PlaceholderType::Anonymous, _) => 1,
        Expr::Placeholder(_, _) | Expr::Literal(_, _) | Expr::Column(_, _) | Expr::Raise { .. } => {
            0
        }
        Expr::Subquery(subquery, _) | Expr::Exists { subquery, .. } => {
            count_anon_placeholders_in_select(subquery)
        }
        Expr::BinaryOp { left, right, .. } => {
            count_anon_placeholders(left) + count_anon_placeholders(right)
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => count_anon_placeholders(inner),
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            count_anon_placeholders(inner)
                + count_anon_placeholders(low)
                + count_anon_placeholders(high)
        }
        Expr::In {
            expr: inner, set, ..
        } => {
            count_anon_placeholders(inner)
                + match set {
                    fsqlite_ast::InSet::List(items) => {
                        items.iter().map(count_anon_placeholders).sum()
                    }
                    fsqlite_ast::InSet::Subquery(subquery) => {
                        count_anon_placeholders_in_select(subquery)
                    }
                    fsqlite_ast::InSet::Table(_) => 0,
                }
        }
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            ..
        } => {
            count_anon_placeholders(inner)
                + count_anon_placeholders(pattern)
                + escape.as_deref().map_or(0, count_anon_placeholders)
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            operand.as_deref().map_or(0, count_anon_placeholders)
                + whens
                    .iter()
                    .map(|(cond, then_expr)| {
                        count_anon_placeholders(cond) + count_anon_placeholders(then_expr)
                    })
                    .sum::<u32>()
                + else_expr.as_deref().map_or(0, count_anon_placeholders)
        }
        Expr::FunctionCall {
            args, filter, over, ..
        } => {
            let args_count = match args {
                FunctionArgs::List(exprs) => exprs.iter().map(count_anon_placeholders).sum(),
                FunctionArgs::Star => 0,
            };
            args_count
                + filter.as_deref().map_or(0, count_anon_placeholders)
                + over
                    .as_ref()
                    .map_or(0, count_anon_placeholders_in_window_spec)
        }
        Expr::JsonAccess { expr, path, .. } => {
            count_anon_placeholders(expr) + count_anon_placeholders(path)
        }
        Expr::RowValue(items, _) => items.iter().map(count_anon_placeholders).sum(),
    }
}

fn count_anon_placeholders_in_select(select: &SelectStatement) -> u32 {
    let mut count = 0;
    if let Some(with_clause) = &select.with {
        for cte in &with_clause.ctes {
            count += count_anon_placeholders_in_select(&cte.query);
        }
    }
    count += count_anon_placeholders_in_select_core(&select.body.select);
    for (_, core) in &select.body.compounds {
        count += count_anon_placeholders_in_select_core(core);
    }
    for order_term in &select.order_by {
        count += count_anon_placeholders(&order_term.expr);
    }
    if let Some(limit_clause) = &select.limit {
        count += count_anon_placeholders(&limit_clause.limit);
        if let Some(offset) = &limit_clause.offset {
            count += count_anon_placeholders(offset);
        }
    }
    count
}

fn count_anon_placeholders_in_select_core(core: &SelectCore) -> u32 {
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
            let mut count = columns
                .iter()
                .filter_map(|col| match col {
                    ResultColumn::Expr { expr, .. } => Some(count_anon_placeholders(expr)),
                    ResultColumn::Star | ResultColumn::TableStar(_) => None,
                })
                .sum::<u32>();

            if let Some(from_clause) = from {
                count += count_anon_placeholders_in_from_clause(from_clause);
            }
            if let Some(predicate) = where_clause {
                count += count_anon_placeholders(predicate);
            }
            for expr in group_by {
                count += count_anon_placeholders(expr);
            }
            if let Some(predicate) = having {
                count += count_anon_placeholders(predicate);
            }
            for window in windows {
                count += count_anon_placeholders_in_window_spec(&window.spec);
            }
            count
        }
        SelectCore::Values(rows) => rows
            .iter()
            .map(|row| row.iter().map(count_anon_placeholders).sum::<u32>())
            .sum(),
    }
}

fn count_anon_placeholders_in_from_clause(from: &fsqlite_ast::FromClause) -> u32 {
    let mut count = count_anon_placeholders_in_table_or_subquery(&from.source);
    for join in &from.joins {
        count += count_anon_placeholders_in_table_or_subquery(&join.table);
        if let Some(fsqlite_ast::JoinConstraint::On(expr)) = &join.constraint {
            count += count_anon_placeholders(expr);
        }
    }
    count
}

fn count_anon_placeholders_in_table_or_subquery(source: &TableOrSubquery) -> u32 {
    match source {
        TableOrSubquery::Table { .. } => 0,
        TableOrSubquery::Subquery { query, .. } => count_anon_placeholders_in_select(query),
        TableOrSubquery::TableFunction { args, .. } => {
            args.iter().map(count_anon_placeholders).sum()
        }
        TableOrSubquery::ParenJoin(from_clause) => {
            count_anon_placeholders_in_from_clause(from_clause)
        }
    }
}

fn count_anon_placeholders_in_window_spec(spec: &fsqlite_ast::WindowSpec) -> u32 {
    let mut count: u32 = spec.partition_by.iter().map(count_anon_placeholders).sum();
    count += spec
        .order_by
        .iter()
        .map(|term| count_anon_placeholders(&term.expr))
        .sum::<u32>();
    if let Some(frame) = &spec.frame {
        count += count_anon_placeholders_in_frame_bound(&frame.start);
        if let Some(end) = &frame.end {
            count += count_anon_placeholders_in_frame_bound(end);
        }
    }
    count
}

fn count_anon_placeholders_in_frame_bound(bound: &fsqlite_ast::FrameBound) -> u32 {
    match bound {
        fsqlite_ast::FrameBound::Preceding(expr) | fsqlite_ast::FrameBound::Following(expr) => {
            count_anon_placeholders(expr)
        }
        fsqlite_ast::FrameBound::UnboundedPreceding
        | fsqlite_ast::FrameBound::CurrentRow
        | fsqlite_ast::FrameBound::UnboundedFollowing => 0,
    }
}

fn contains_unsupported_in_expr(expr: &Expr) -> bool {
    match expr {
        Expr::In {
            expr: inner, set, ..
        } => {
            contains_unsupported_in_expr(inner)
                || match set {
                    fsqlite_ast::InSet::List(items) => {
                        items.iter().any(contains_unsupported_in_expr)
                    }
                    // `IN (SELECT ...)` / `IN table` are handled by runtime probe
                    // codegen in `emit_expr`.
                    fsqlite_ast::InSet::Subquery(_) | fsqlite_ast::InSet::Table(_) => false,
                }
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_unsupported_in_expr(left) || contains_unsupported_in_expr(right)
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => contains_unsupported_in_expr(inner),
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            contains_unsupported_in_expr(inner)
                || contains_unsupported_in_expr(low)
                || contains_unsupported_in_expr(high)
        }
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            ..
        } => {
            contains_unsupported_in_expr(inner)
                || contains_unsupported_in_expr(pattern)
                || escape.as_deref().is_some_and(contains_unsupported_in_expr)
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            operand.as_deref().is_some_and(contains_unsupported_in_expr)
                || whens.iter().any(|(cond, then_expr)| {
                    contains_unsupported_in_expr(cond) || contains_unsupported_in_expr(then_expr)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(contains_unsupported_in_expr)
        }
        Expr::FunctionCall {
            args: FunctionArgs::List(args),
            ..
        } => args.iter().any(contains_unsupported_in_expr),
        Expr::RowValue(items, _) => items.iter().any(contains_unsupported_in_expr),
        _ => false,
    }
}

fn contains_unsupported_in_result_column(col: &ResultColumn) -> bool {
    match col {
        ResultColumn::Expr { expr, .. } => contains_unsupported_in_expr(expr),
        ResultColumn::Star | ResultColumn::TableStar(_) => false,
    }
}

fn unsupported_in_message() -> String {
    "IN (SELECT ...) / IN table requires rewrite before VDBE codegen".to_owned()
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
    let (columns, from, where_clause, group_by, having, distinct) = match &stmt.body.select {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            having,
            distinct,
            ..
        } => (columns, from, where_clause, group_by, having, *distinct),
        SelectCore::Values(rows) => {
            return codegen_values_select(b, rows);
        }
    };

    if columns.iter().any(contains_unsupported_in_result_column)
        || where_clause
            .as_ref()
            .is_some_and(|expr| contains_unsupported_in_expr(expr))
        || group_by.iter().any(contains_unsupported_in_expr)
        || having
            .as_ref()
            .is_some_and(|expr| contains_unsupported_in_expr(expr))
        || stmt
            .order_by
            .iter()
            .any(|term| contains_unsupported_in_expr(&term.expr))
    {
        return Err(CodegenError::Unsupported(unsupported_in_message()));
    }

    // Handle SELECT without FROM (e.g. SELECT 1, SELECT 1+1, SELECT abs(-5)).
    if from.is_none() {
        codegen_select_without_from(b, columns, where_clause.as_deref());
        return Ok(());
    }

    // Determine the table from the FROM clause.
    let from_clause = from
        .as_ref()
        .ok_or_else(|| CodegenError::Unsupported("SELECT without FROM".to_owned()))?;

    let (table_name, table_alias) = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => (&name.name, alias.as_deref()),
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

    // Check for aggregate columns FIRST, before rowid/index seek optimizations.
    // Aggregates like count(*) require a full scan + AggStep/AggFinal path;
    // the rowid-seek and index-seek paths don't support aggregate functions.
    let is_aggregate = has_aggregate_columns(columns);

    // Check for rowid-equality WHERE clause (only for non-aggregate queries).
    let rowid_param = if is_aggregate {
        None
    } else {
        extract_rowid_bind_param(where_clause.as_deref())
    };
    // Check for index-usable WHERE clause (only for non-aggregate queries).
    // NOTE: Index-seek is disabled because the B-tree cursor Next() doesn't
    // correctly advance through duplicate key entries in non-unique indexes,
    // causing WHERE queries on non-unique indexed columns to return only
    // the first matching row. Fall back to full table scan until the B-tree
    // cursor is fixed. (bd-beads_rust-6ii1)
    let index_eq: Option<(String, i32)> = None;
    let mut index_cursor_to_close = None;

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
        emit_column_reads(b, cursor, columns, table, table_alias, schema, out_regs)?;

        // ResultRow.
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    } else if let Some((col_name, param_idx)) = &index_eq {
        // --- Index-seek SELECT ---
        if let Some(idx_schema) = table.index_for_column(col_name) {
            let idx_cursor = 1_i32;
            index_cursor_to_close = Some(idx_cursor);
            let full_scan_fallback = b.emit_label();

            let param_reg = b.alloc_reg();
            b.emit_op(Opcode::Variable, *param_idx, param_reg, 0, P4::None, 0);

            // Build probe key: [bound_value, i64::MIN] so SeekGE lands on the
            // first duplicate for the bound value.
            let min_rowid_reg = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, min_rowid_reg, 0, P4::Int64(i64::MIN), 0);
            let probe_record_reg = b.alloc_reg();
            b.emit_op(
                Opcode::MakeRecord,
                param_reg,
                2,
                probe_record_reg,
                P4::None,
                0,
            );

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
                probe_record_reg,
                full_scan_fallback,
                P4::None,
                0,
            );

            // Loop over all matching index entries (non-unique indexes may
            // have multiple rows for the same key value).
            let idx_loop_top = b.current_addr();

            // Guard: if the current key >= probe is not equal to the
            // requested value, stop iterating.
            let idx_key_reg = b.alloc_reg();
            b.emit_op(Opcode::Column, idx_cursor, 0, idx_key_reg, P4::None, 0);
            b.emit_jump_to_label(
                Opcode::Ne,
                param_reg,
                idx_key_reg,
                full_scan_fallback,
                P4::None,
                0,
            );

            let rowid_reg = b.alloc_reg();
            b.emit_op(Opcode::IdxRowid, idx_cursor, rowid_reg, 0, P4::None, 0);
            // If SeekRowid can't find the data row, skip to next index entry.
            let idx_skip_label = b.emit_label();
            b.emit_jump_to_label(
                Opcode::SeekRowid,
                cursor,
                rowid_reg,
                idx_skip_label,
                P4::None,
                0,
            );

            // Read columns.
            emit_column_reads(b, cursor, columns, table, table_alias, schema, out_regs)?;

            // ResultRow.
            b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

            // Advance to next index entry and loop back.
            b.resolve_label(idx_skip_label);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            #[allow(clippy::cast_possible_wrap)]
            let idx_loop_body = idx_loop_top as i32;
            b.emit_op(Opcode::Next, idx_cursor, idx_loop_body, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

            // Safety fallback: if index probe cannot produce a verified row
            // (e.g. unavailable/stale index backend), run a full table scan.
            b.resolve_label(full_scan_fallback);
            let loop_start = b.current_addr();
            b.emit_jump_to_label(Opcode::Rewind, cursor, 0, done_label, P4::None, 0);
            let skip_label = b.emit_label();
            if let Some(where_expr) = where_clause.as_deref() {
                emit_where_filter(
                    b,
                    where_expr,
                    cursor,
                    table,
                    table_alias,
                    schema,
                    skip_label,
                );
            }
            emit_column_reads(b, cursor, columns, table, table_alias, schema, out_regs)?;
            b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
            b.resolve_label(skip_label);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let loop_body = (loop_start + 1) as i32;
            b.emit_op(Opcode::Next, cursor, loop_body, 0, P4::None, 0);
        } else {
            // Fallback to full scan.
            return codegen_select_full_scan(
                b,
                cursor,
                table,
                table_alias,
                schema,
                columns,
                where_clause.as_deref(),
                stmt.limit.as_ref(),
                out_regs,
                out_col_count,
                done_label,
                end_label,
            );
        }
    } else if has_aggregate_columns(columns) && !group_by.is_empty() {
        // --- Aggregate query WITH GROUP BY ---
        return codegen_select_group_by_aggregate(
            b,
            cursor,
            table,
            table_alias,
            schema,
            columns,
            where_clause.as_deref(),
            group_by,
            having.as_deref(),
            out_regs,
            out_col_count,
            done_label,
            end_label,
        );
    } else if has_aggregate_columns(columns) {
        // --- Aggregate query (single-group, no GROUP BY) ---
        return codegen_select_aggregate(
            b,
            cursor,
            table,
            table_alias,
            schema,
            columns,
            where_clause.as_deref(),
            out_regs,
            out_col_count,
            done_label,
            end_label,
        );
    } else if !stmt.order_by.is_empty() {
        if let Some(index_plan) = resolve_order_by_index_plan(
            table,
            table_alias,
            columns,
            where_clause.as_deref(),
            &stmt.order_by,
            distinct,
        ) {
            tracing::info!(
                table = %table.name,
                index = %index_plan.index_name,
                covering = index_plan.covering_output.is_some(),
                descending = index_plan.descending,
                "vdbe.order_by.index_bypass"
            );
            return codegen_select_index_ordered_scan(
                b,
                cursor,
                table,
                table_alias,
                schema,
                columns,
                where_clause.as_deref(),
                stmt.limit.as_ref(),
                out_regs,
                out_col_count,
                done_label,
                end_label,
                &index_plan,
            );
        }

        // --- Full table scan with ORDER BY (sorter path) ---
        return codegen_select_ordered_scan(
            b,
            cursor,
            table,
            table_alias,
            schema,
            columns,
            where_clause.as_deref(),
            &stmt.order_by,
            stmt.limit.as_ref(),
            distinct,
            out_regs,
            out_col_count,
            done_label,
            end_label,
        );
    } else if distinct == Distinctness::Distinct {
        // --- Full table scan with DISTINCT ---
        return codegen_select_distinct_scan(
            b,
            cursor,
            table,
            table_alias,
            schema,
            columns,
            where_clause.as_deref(),
            stmt.limit.as_ref(),
            out_regs,
            out_col_count,
            done_label,
            end_label,
        );
    } else {
        // --- Full table scan ---
        return codegen_select_full_scan(
            b,
            cursor,
            table,
            table_alias,
            schema,
            columns,
            where_clause.as_deref(),
            stmt.limit.as_ref(),
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

/// Codegen for a full table scan SELECT with optional WHERE filtering and LIMIT/OFFSET.
#[allow(clippy::too_many_arguments)]
fn codegen_select_full_scan(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    limit_clause: Option<&LimitClause>,
    out_regs: i32,
    out_col_count: i32,
    done_label: crate::Label,
    end_label: crate::Label,
) -> Result<(), CodegenError> {
    // Allocate LIMIT/OFFSET counter registers (if present).
    let limit_reg = limit_clause.map(|lc| {
        let r = b.alloc_reg();
        emit_limit_expr(b, &lc.limit, r);
        r
    });
    let offset_reg = limit_clause.and_then(|lc| {
        lc.offset.as_ref().map(|off_expr| {
            let r = b.alloc_reg();
            emit_limit_expr(b, off_expr, r);
            r
        })
    });

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

    // Evaluate WHERE condition (if any) and skip non-matching rows.
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        emit_where_filter(
            b,
            where_expr,
            cursor,
            table,
            table_alias,
            schema,
            skip_label,
        );
    }

    // OFFSET: if offset counter > 0, decrement by 1 and skip this row.
    if let Some(off_r) = offset_reg {
        b.emit_jump_to_label(Opcode::IfPos, off_r, 1, skip_label, P4::None, 0);
    }

    // Read columns.
    emit_column_reads(b, cursor, columns, table, table_alias, schema, out_regs)?;

    // ResultRow.
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // LIMIT: decrement limit counter; jump to done when zero.
    if let Some(lim_r) = limit_reg {
        b.emit_jump_to_label(Opcode::DecrJumpZero, lim_r, 0, done_label, P4::None, 0);
    }

    // Skip label for WHERE-filtered rows.
    b.resolve_label(skip_label);

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

fn emit_covering_output_reads(
    b: &mut ProgramBuilder,
    index_cursor: i32,
    rowid_reg: i32,
    sources: &[CoveringOutputSource],
    out_regs: i32,
) {
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (offset, source) in sources.iter().enumerate() {
        let target_reg = out_regs + offset as i32;
        match source {
            CoveringOutputSource::IndexColumn(index_col) => {
                b.emit_op(
                    Opcode::Column,
                    index_cursor,
                    *index_col,
                    target_reg,
                    P4::None,
                    0,
                );
            }
            CoveringOutputSource::Rowid => {
                b.emit_op(Opcode::Copy, rowid_reg, target_reg, 0, P4::None, 0);
            }
        }
    }
}

/// Generate VDBE bytecode for an ORDER BY scan that can stream rows directly
/// from an index in sorted order (no sorter temp B-tree).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn codegen_select_index_ordered_scan(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    limit_clause: Option<&LimitClause>,
    out_regs: i32,
    out_col_count: i32,
    done_label: crate::Label,
    end_label: crate::Label,
    index_plan: &OrderByIndexPlan,
) -> Result<(), CodegenError> {
    let index_cursor = cursor + 1;
    let needs_table_lookup = index_plan.covering_output.is_none() || where_clause.is_some();

    // Allocate LIMIT/OFFSET counter registers (if present).
    let limit_reg = limit_clause.map(|lc| {
        let r = b.alloc_reg();
        emit_limit_expr(b, &lc.limit, r);
        r
    });
    let offset_reg = limit_clause.and_then(|lc| {
        lc.offset.as_ref().map(|off_expr| {
            let r = b.alloc_reg();
            emit_limit_expr(b, off_expr, r);
            r
        })
    });

    if needs_table_lookup {
        b.emit_op(
            Opcode::OpenRead,
            cursor,
            table.root_page,
            0,
            P4::Table(table.name.clone()),
            0,
        );
    }

    b.emit_op(
        Opcode::OpenRead,
        index_cursor,
        index_plan.index_root_page,
        0,
        P4::Index(index_plan.index_name.clone()),
        0,
    );

    // Position index cursor at first/last entry depending on ORDER BY direction.
    let loop_start = b.current_addr();
    if index_plan.descending {
        b.emit_jump_to_label(Opcode::Last, index_cursor, 0, done_label, P4::None, 0);
    } else {
        b.emit_jump_to_label(Opcode::Rewind, index_cursor, 0, done_label, P4::None, 0);
    }

    let skip_row = b.emit_label();
    let rowid_reg = b.alloc_reg();
    b.emit_op(Opcode::IdxRowid, index_cursor, rowid_reg, 0, P4::None, 0);

    if needs_table_lookup {
        b.emit_jump_to_label(Opcode::SeekRowid, cursor, rowid_reg, skip_row, P4::None, 0);
    }

    if let Some(where_expr) = where_clause {
        emit_where_filter(b, where_expr, cursor, table, table_alias, schema, skip_row);
    }

    if let Some(covering_output) = &index_plan.covering_output {
        emit_covering_output_reads(b, index_cursor, rowid_reg, covering_output, out_regs);
    } else {
        emit_column_reads(b, cursor, columns, table, table_alias, schema, out_regs)?;
    }

    // OFFSET: if offset counter > 0, decrement by 1 and skip this row.
    if let Some(off_r) = offset_reg {
        b.emit_jump_to_label(Opcode::IfPos, off_r, 1, skip_row, P4::None, 0);
    }

    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // LIMIT: decrement limit counter; jump to done when zero.
    if let Some(lim_r) = limit_reg {
        b.emit_jump_to_label(Opcode::DecrJumpZero, lim_r, 0, done_label, P4::None, 0);
    }

    b.resolve_label(skip_row);

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (loop_start + 1) as i32;
    if index_plan.descending {
        b.emit_op(Opcode::Prev, index_cursor, loop_body, 0, P4::None, 0);
    } else {
        b.emit_op(Opcode::Next, index_cursor, loop_body, 0, P4::None, 0);
    }

    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, index_cursor, 0, 0, P4::None, 0);
    if needs_table_lookup {
        b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    }
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);
    Ok(())
}

/// Generate VDBE bytecode for `SELECT DISTINCT` without ORDER BY.
///
/// Uses a two-pass sorter approach: scan all output columns into the sorter
/// (all columns are sort keys), sort, then iterate and skip adjacent
/// duplicate rows using packed-record comparison.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn codegen_select_distinct_scan(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    limit_clause: Option<&LimitClause>,
    out_regs: i32,
    out_col_count: i32,
    done_label: crate::Label,
    end_label: crate::Label,
) -> Result<(), CodegenError> {
    let num_data_cols = usize::try_from(out_col_count).map_err(|_| {
        CodegenError::Unsupported("negative output column count in DISTINCT SELECT".to_owned())
    })?;

    // Sorter cursor is separate from the table cursor.
    let sorter_cursor = cursor + 1;

    // Open sorter with all output columns as sort keys (ascending).
    let sort_order: String = "+".repeat(num_data_cols);
    b.emit_op(
        Opcode::SorterOpen,
        sorter_cursor,
        out_col_count,
        0,
        P4::Str(sort_order),
        0,
    );

    // Open table for reading.
    b.emit_op(
        Opcode::OpenRead,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // === Pass 1: Scan rows into sorter ===
    let scan_start = b.current_addr();
    let scan_done = b.emit_label();
    b.emit_jump_to_label(Opcode::Rewind, cursor, 0, scan_done, P4::None, 0);

    // WHERE filter.
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        emit_where_filter(
            b,
            where_expr,
            cursor,
            table,
            table_alias,
            schema,
            skip_label,
        );
    }

    // Read output columns into consecutive registers.
    let sorter_base = b.alloc_regs(out_col_count);
    emit_column_reads(b, cursor, columns, table, table_alias, schema, sorter_base)?;

    // MakeRecord from all output columns, then SorterInsert.
    let record_reg = b.alloc_reg();
    b.emit_op(
        Opcode::MakeRecord,
        sorter_base,
        out_col_count,
        record_reg,
        P4::None,
        0,
    );
    b.emit_op(
        Opcode::SorterInsert,
        sorter_cursor,
        record_reg,
        0,
        P4::None,
        0,
    );

    // Skip label (for WHERE-filtered rows).
    b.resolve_label(skip_label);

    // Next row in scan.
    let scan_body = (scan_start + 1) as i32;
    b.emit_op(Opcode::Next, cursor, scan_body, 0, P4::None, 0);

    // End of pass 1: close table cursor.
    b.resolve_label(scan_done);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);

    // === Pass 2: Iterate sorted rows, skipping duplicates ===

    // Allocate LIMIT/OFFSET counters.
    let limit_reg = limit_clause.map(|lc| {
        let r = b.alloc_reg();
        emit_limit_expr(b, &lc.limit, r);
        r
    });
    let offset_reg = limit_clause.and_then(|lc| {
        lc.offset.as_ref().map(|off_expr| {
            let r = b.alloc_reg();
            emit_limit_expr(b, off_expr, r);
            r
        })
    });

    // DISTINCT state (record compare against previous output row).
    let cur_rec = b.alloc_reg();
    let prev_rec = b.alloc_reg();
    b.emit_op(Opcode::Null, 0, prev_rec, 0, P4::None, 0);
    let dup_skip = b.emit_label();

    // SorterSort: sort and position at first row; jump to done if empty.
    b.emit_jump_to_label(
        Opcode::SorterSort,
        sorter_cursor,
        0,
        done_label,
        P4::None,
        0,
    );

    let sort_loop_body = b.current_addr();

    // SorterData: decode current sorted row.
    let sorted_reg = b.alloc_reg();
    b.emit_op(
        Opcode::SorterData,
        sorter_cursor,
        sorted_reg,
        0,
        P4::None,
        0,
    );

    // Extract output columns from sorted record.
    for i in 0..num_data_cols {
        b.emit_op(
            Opcode::Column,
            sorter_cursor,
            i as i32,
            out_regs + i as i32,
            P4::None,
            0,
        );
    }

    // DISTINCT: pack output into a record and compare with previous row.
    b.emit_op(
        Opcode::MakeRecord,
        out_regs,
        out_col_count,
        cur_rec,
        P4::None,
        0,
    );

    // If current record equals previous, skip (duplicate).
    b.emit_jump_to_label(Opcode::Eq, prev_rec, cur_rec, dup_skip, P4::None, 0);

    // Update previous record to current for next comparison.
    b.emit_op(Opcode::Copy, cur_rec, prev_rec, 0, P4::None, 0);

    // OFFSET applies after duplicate elimination.
    let output_skip = b.emit_label();
    if let Some(off_r) = offset_reg {
        b.emit_jump_to_label(Opcode::IfPos, off_r, 1, output_skip, P4::None, 0);
    }

    // ResultRow.
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // LIMIT: decrement limit counter; jump to done when zero.
    if let Some(lim_r) = limit_reg {
        b.emit_jump_to_label(Opcode::DecrJumpZero, lim_r, 0, done_label, P4::None, 0);
    }

    // Duplicate skip label.
    b.resolve_label(dup_skip);

    // Output skip label (for OFFSET-skipped rows).
    b.resolve_label(output_skip);

    // SorterNext: advance to next sorted row.
    b.emit_op(
        Opcode::SorterNext,
        sorter_cursor,
        sort_loop_body as i32,
        0,
        P4::None,
        0,
    );

    // Done: Close sorter + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, sorter_cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

/// Emit a LIMIT or OFFSET expression into a register.
///
/// Handles integer literals and bind parameters; evaluates arbitrary
/// expressions via `emit_expr` for computed limits (e.g. `LIMIT 5+0`).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_limit_expr(b: &mut ProgramBuilder, expr: &Expr, target_reg: i32) {
    match expr {
        Expr::Literal(Literal::Integer(n), _) => {
            if let Ok(as_i32) = i32::try_from(*n) {
                b.emit_op(Opcode::Integer, as_i32, target_reg, 0, P4::None, 0);
            } else {
                b.emit_op(Opcode::Int64, 0, target_reg, 0, P4::Int64(*n), 0);
            }
        }
        Expr::Placeholder(pt, _) => {
            let param_idx = match pt {
                fsqlite_ast::PlaceholderType::Numbered(n) => *n as i32,
                // Anonymous and named placeholders use sequential numbering.
                _ => b.next_anon_placeholder_idx() as i32,
            };
            b.emit_op(Opcode::Variable, param_idx, target_reg, 0, P4::None, 0);
        }
        _ => {
            // Evaluate arbitrary expression (e.g. `5+0`, `abs(-3)`).
            // No table context needed — LIMIT expressions don't reference columns.
            emit_expr(b, expr, target_reg, None);
        }
    }
}

// ---------------------------------------------------------------------------
// ORDER BY codegen (two-pass sorter)
// ---------------------------------------------------------------------------

/// Generate VDBE bytecode for a full-scan SELECT with ORDER BY.
///
/// Uses a two-pass sorter approach:
/// 1. Scan table rows (with WHERE), pack sort-key + data columns into sorter.
/// 2. After sorting, iterate sorted rows and emit `ResultRow`.
///
/// LIMIT/OFFSET are applied in pass 2 (on sorted output).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn codegen_select_ordered_scan(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    order_by: &[OrderingTerm],
    limit_clause: Option<&LimitClause>,
    distinct: Distinctness,
    out_regs: i32,
    out_col_count: i32,
    done_label: crate::Label,
    end_label: crate::Label,
) -> Result<(), CodegenError> {
    // Resolve ORDER BY sources (column indices, rowid, or expressions).
    let sort_keys: Vec<SortKeySource> = order_by
        .iter()
        .map(|term| resolve_sort_key(&term.expr, table, table_alias))
        .collect();

    let num_sort_keys = sort_keys.len();
    let num_data_cols = usize::try_from(out_col_count).map_err(|_| {
        CodegenError::Unsupported("negative output column count in ordered SELECT".to_owned())
    })?;
    let total_sorter_cols = num_sort_keys + num_data_cols;

    // Sorter cursor is separate from the table cursor.
    let sorter_cursor = cursor + 1;

    // Open sorter: p2 = number of key columns, p4 = sort order string.
    let sort_order: String = order_by
        .iter()
        .map(|term| {
            if term.direction == Some(SortDirection::Desc) {
                '-'
            } else {
                '+'
            }
        })
        .collect();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::SorterOpen,
        sorter_cursor,
        num_sort_keys as i32,
        0,
        P4::Str(sort_order),
        0,
    );

    // Open table for reading.
    b.emit_op(
        Opcode::OpenRead,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // === Pass 1: Scan rows into sorter ===
    let scan_start = b.current_addr();
    let scan_done = b.emit_label();
    b.emit_jump_to_label(Opcode::Rewind, cursor, 0, scan_done, P4::None, 0);

    // WHERE filter.
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        emit_where_filter(
            b,
            where_expr,
            cursor,
            table,
            table_alias,
            schema,
            skip_label,
        );
    }

    // Read sort-key columns + data columns into consecutive registers.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let sorter_base = b.alloc_regs(total_sorter_cols as i32);
    {
        let mut reg = sorter_base;
        let scan = ScanCtx {
            cursor,
            table,
            table_alias,
            schema: Some(schema),
        };
        for key in &sort_keys {
            match key {
                SortKeySource::Column(col_idx) => {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    b.emit_op(Opcode::Column, cursor, *col_idx as i32, reg, P4::None, 0);
                }
                SortKeySource::Rowid => {
                    b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                }
                SortKeySource::Expression(expr) => {
                    emit_expr(b, expr, reg, Some(&scan));
                }
            }
            reg += 1;
        }

        // Evaluate result columns (including expressions) and store the final
        // output values in the sorter record.
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        emit_column_reads(
            b,
            cursor,
            columns,
            table,
            table_alias,
            schema,
            sorter_base + num_sort_keys as i32,
        )?;
    }

    // MakeRecord from all sorter columns, then SorterInsert.
    let record_reg = b.alloc_reg();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::MakeRecord,
        sorter_base,
        total_sorter_cols as i32,
        record_reg,
        P4::None,
        0,
    );
    b.emit_op(
        Opcode::SorterInsert,
        sorter_cursor,
        record_reg,
        0,
        P4::None,
        0,
    );

    // Skip label (for WHERE-filtered rows).
    b.resolve_label(skip_label);

    // Next row in scan.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let scan_body = (scan_start + 1) as i32;
    b.emit_op(Opcode::Next, cursor, scan_body, 0, P4::None, 0);

    // End of pass 1: close table cursor.
    b.resolve_label(scan_done);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);

    // === Pass 2: Iterate sorted rows ===

    // Allocate LIMIT/OFFSET counters (before the sort loop).
    let limit_reg = limit_clause.map(|lc| {
        let r = b.alloc_reg();
        emit_limit_expr(b, &lc.limit, r);
        r
    });
    let offset_reg = limit_clause.and_then(|lc| {
        lc.offset.as_ref().map(|off_expr| {
            let r = b.alloc_reg();
            emit_limit_expr(b, off_expr, r);
            r
        })
    });

    // DISTINCT state (used only when DISTINCT is requested).
    let distinct_state = if distinct == Distinctness::Distinct {
        let cur_rec = b.alloc_reg();
        let prev_rec = b.alloc_reg();
        b.emit_op(Opcode::Null, 0, prev_rec, 0, P4::None, 0);
        Some((cur_rec, prev_rec, b.emit_label()))
    } else {
        None
    };

    // SorterSort: sort and position at first row; jump to done if empty.
    b.emit_jump_to_label(
        Opcode::SorterSort,
        sorter_cursor,
        0,
        done_label,
        P4::None,
        0,
    );

    // Save the address of the sort loop body (SorterData target for SorterNext).
    let sort_loop_body = b.current_addr();

    // SorterData: decode current sorted row into a register.
    let sorted_reg = b.alloc_reg();
    b.emit_op(
        Opcode::SorterData,
        sorter_cursor,
        sorted_reg,
        0,
        P4::None,
        0,
    );

    // Extract data columns from the sorted record.
    // The sorter record has sort-key columns first, then data columns.
    // We use Column on the sorter cursor to read individual fields.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for i in 0..num_data_cols {
        let src_col = (num_sort_keys + i) as i32;
        b.emit_op(
            Opcode::Column,
            sorter_cursor,
            src_col,
            out_regs + i as i32,
            P4::None,
            0,
        );
    }

    // DISTINCT: skip rows whose output columns match the previous row.
    // Pack output into a record, compare with previous record; if equal, skip.
    if let Some((cur_rec, prev_rec, skip)) = distinct_state {
        // Pack current output columns into a record.
        b.emit_op(
            Opcode::MakeRecord,
            out_regs,
            out_col_count,
            cur_rec,
            P4::None,
            0,
        );

        // Compare with previous record; if equal (Eq jumps on match), skip.
        b.emit_jump_to_label(Opcode::Eq, prev_rec, cur_rec, skip, P4::None, 0);

        // Update previous record to current.
        b.emit_op(Opcode::Copy, cur_rec, prev_rec, 0, P4::None, 0);
    }

    // OFFSET applies after duplicate elimination.
    let output_skip = b.emit_label();
    if let Some(off_r) = offset_reg {
        b.emit_jump_to_label(Opcode::IfPos, off_r, 1, output_skip, P4::None, 0);
    }

    // ResultRow.
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // LIMIT: decrement limit counter; jump to done when zero.
    if let Some(lim_r) = limit_reg {
        b.emit_jump_to_label(Opcode::DecrJumpZero, lim_r, 0, done_label, P4::None, 0);
    }

    // Resolve DISTINCT skip label (if active).
    if let Some((_, _, skip)) = distinct_state {
        b.resolve_label(skip);
    }

    // Output skip label (for OFFSET-skipped rows).
    b.resolve_label(output_skip);

    // SorterNext: advance to next sorted row, jump back to sort loop body.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::SorterNext,
        sorter_cursor,
        sort_loop_body as i32,
        0,
        P4::None,
        0,
    );

    // Done: Close sorter + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, sorter_cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

// ---------------------------------------------------------------------------
// Aggregate codegen
// ---------------------------------------------------------------------------

/// Known aggregate function names (case-insensitive matching).
const AGGREGATE_FUNCTIONS: &[&str] = &[
    "avg",
    "count",
    "group_concat",
    "string_agg",
    "max",
    "min",
    "sum",
    "total",
    "median",
    "percentile",
    "percentile_cont",
    "percentile_disc",
];

/// Check whether a function name is a known aggregate (built-in or custom UDF).
fn is_aggregate_function(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    AGGREGATE_FUNCTIONS.iter().any(|&n| n == lower)
        || EXTRA_AGG_NAMES.with(|extra| extra.borrow().contains(&lower))
}

/// Check whether any result column contains an aggregate function call.
fn has_aggregate_columns(columns: &[ResultColumn]) -> bool {
    columns.iter().any(|col| {
        if let ResultColumn::Expr { expr, .. } = col {
            is_aggregate_expr(expr)
        } else {
            false
        }
    })
}

/// Check whether an expression contains an aggregate function call.
fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } if is_aggregate_function(name) => true,
        Expr::BinaryOp { left, right, .. } => is_aggregate_expr(left) || is_aggregate_expr(right),
        Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => is_aggregate_expr(inner),
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => is_aggregate_expr(inner) || is_aggregate_expr(low) || is_aggregate_expr(high),
        Expr::In {
            expr: inner, set, ..
        } => {
            if is_aggregate_expr(inner) {
                return true;
            }
            match set {
                fsqlite_ast::InSet::List(items) => items.iter().any(is_aggregate_expr),
                _ => false,
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            escape,
            ..
        } => {
            is_aggregate_expr(inner)
                || is_aggregate_expr(pattern)
                || escape.as_deref().is_some_and(is_aggregate_expr)
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            if operand.as_deref().is_some_and(is_aggregate_expr) {
                return true;
            }
            if whens
                .iter()
                .any(|(cond, then_expr)| is_aggregate_expr(cond) || is_aggregate_expr(then_expr))
            {
                return true;
            }
            if else_expr.as_deref().is_some_and(is_aggregate_expr) {
                return true;
            }
            false
        }
        Expr::FunctionCall {
            args: FunctionArgs::List(args),
            ..
        } => args.iter().any(is_aggregate_expr),
        Expr::RowValue(items, _) => items.iter().any(is_aggregate_expr),
        Expr::JsonAccess {
            expr: inner, path, ..
        } => is_aggregate_expr(inner) || is_aggregate_expr(path),
        _ => false,
    }
}

/// Description of one aggregate column for codegen.
struct AggColumn {
    /// Aggregate function name (lowercased).
    name: String,
    /// Number of arguments (0 for count(*), 1 for sum(col), etc.).
    num_args: i32,
    /// Column index of the argument (for single-arg aggregates), or `None` for count(*) or rowid.
    arg_col_index: Option<usize>,
    /// True if the argument is the INTEGER PRIMARY KEY (rowid) column.
    arg_is_rowid: bool,
    /// True if the aggregate uses DISTINCT (e.g. `COUNT(DISTINCT col)`).
    distinct: bool,
    /// Non-column expression argument (e.g. `SUM(a + b)`), evaluated via `emit_expr`.
    /// `None` when arg is a simple column ref (use `arg_col_index` instead).
    arg_expr: Option<Box<Expr>>,
    /// Additional argument expressions beyond the first (e.g. separator for group_concat).
    extra_args: Vec<Expr>,
    /// FILTER clause expression, e.g. `COUNT(*) FILTER (WHERE x > 5)`.
    /// When present, the AggStep is only executed if this evaluates to true.
    filter: Option<Box<Expr>>,
}

/// Generate VDBE bytecode for a standalone `VALUES` clause.
///
/// Pattern: `Init → Transaction → [for each row: eval exprs → ResultRow] → Halt`
///
/// Handles `VALUES (1, 'a'), (2, 'b')` etc.
fn codegen_values_select(b: &mut ProgramBuilder, rows: &[Vec<Expr>]) -> Result<(), CodegenError> {
    if rows.is_empty() {
        return Err(CodegenError::Unsupported("empty VALUES".to_owned()));
    }

    let end_label = b.emit_label();

    // Init: jump to end (standard SQLite pattern).
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (read-only, p2=0).
    b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);

    // Determine column count from the first row.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let num_cols = rows[0].len() as i32;
    let out_regs = b.alloc_regs(num_cols);

    // Emit each row: evaluate expressions, then ResultRow.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for row in rows {
        for (i, expr) in row.iter().enumerate() {
            let reg = out_regs + i as i32;
            emit_expr(b, expr, reg, None);
        }
        b.emit_op(Opcode::ResultRow, out_regs, num_cols, 0, P4::None, 0);
    }

    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

/// Generate VDBE bytecode for SELECT without FROM clause.
///
/// Pattern: `Init → Transaction → [eval exprs] → ResultRow → Halt`
///
/// Handles `SELECT 1`, `SELECT 1+2, 'abc'`, `SELECT abs(-5)`, etc.
/// If a WHERE clause is present and evaluates to false/NULL, no row is emitted.
fn codegen_select_without_from(
    b: &mut ProgramBuilder,
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
) {
    let end_label = b.emit_label();
    let halt_label = b.emit_label();

    // Init: jump to end (standard SQLite pattern).
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (read-only, p2=0).
    b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);

    // WHERE clause: if present and false/NULL, skip to Halt.
    if let Some(where_expr) = where_clause {
        let cond_reg = b.alloc_temp();
        emit_expr(b, where_expr, cond_reg, None);
        b.emit_jump_to_label(Opcode::IfNot, cond_reg, 1, halt_label, P4::None, 0);
        b.free_temp(cond_reg);
    }

    // Evaluate each result column expression into consecutive output registers.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let num_cols = columns.len() as i32;
    let out_regs = b.alloc_regs(num_cols);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (i, col) in columns.iter().enumerate() {
        let reg = out_regs + i as i32;
        match col {
            ResultColumn::Expr { expr, .. } => {
                emit_expr(b, expr, reg, None);
            }
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                // No table → Star has no meaning; emit NULL.
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            }
        }
    }

    b.emit_op(Opcode::ResultRow, out_regs, num_cols, 0, P4::None, 0);
    b.resolve_label(halt_label);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);
}

/// Generate VDBE bytecode for an aggregate SELECT (no GROUP BY yet).
///
/// Pattern:
/// ```text
/// Init → Transaction → OpenRead → Rewind →
///   [AggStep per aggregate per row] → Next →
/// [AggFinal per aggregate] → ResultRow → Close → Halt
/// ```
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn codegen_select_aggregate(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    out_regs: i32,
    out_col_count: i32,
    done_label: crate::Label,
    end_label: crate::Label,
) -> Result<(), CodegenError> {
    // Parse aggregate columns: extract function name, arg count, arg column index.
    let agg_columns = parse_aggregate_columns(columns, table)?;

    // Allocate one accumulator register per aggregate.
    let accum_base = b.alloc_regs(out_col_count);

    // Initialize accumulators to Null (required by AggStep protocol).
    for i in 0..out_col_count {
        b.emit_op(Opcode::Null, 0, accum_base + i, 0, P4::None, 0);
    }

    // Open table for reading.
    b.emit_op(
        Opcode::OpenRead,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // Rewind to first row; jump to finalize if table is empty.
    let finalize_label = b.emit_label();
    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, cursor, 0, finalize_label, P4::None, 0);

    // WHERE filter.
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        emit_where_filter(
            b,
            where_expr,
            cursor,
            table,
            table_alias,
            schema,
            skip_label,
        );
    }

    // AggStep for each aggregate column.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (i, agg) in agg_columns.iter().enumerate() {
        let accum_reg = accum_base + i as i32;

        // FILTER clause: evaluate and skip AggStep if false/NULL.
        let filter_skip_label = if let Some(ref filter_expr) = agg.filter {
            let skip_lbl = b.emit_label();
            let filter_reg = b.alloc_temp();
            let scan_ctx = ScanCtx {
                cursor,
                table,
                table_alias,
                schema: Some(schema),
            };
            emit_expr(b, filter_expr, filter_reg, Some(&scan_ctx));
            // p3=1: treat NULL as false (skip AggStep).
            b.emit_jump_to_label(Opcode::IfNot, filter_reg, 1, skip_lbl, P4::None, 0);
            b.free_temp(filter_reg);
            Some(skip_lbl)
        } else {
            None
        };

        let distinct_flag = i32::from(agg.distinct);
        if agg.num_args == 0 {
            // count(*): no arguments, p2 is unused (0), p5=0.
            b.emit_op(
                Opcode::AggStep,
                distinct_flag,
                0,
                accum_reg,
                P4::FuncName(agg.name.clone()),
                0,
            );
        } else {
            // Aggregate with arguments: allocate consecutive registers
            // for all args so the engine can read them as a contiguous block.
            let total_args = agg.num_args.max(1);
            // alloc_regs guarantees contiguous register block.
            let arg_base = b.alloc_regs(total_args);

            // First argument.
            if agg.arg_is_rowid {
                b.emit_op(Opcode::Rowid, cursor, arg_base, 0, P4::None, 0);
            } else if let Some(ref expr) = agg.arg_expr {
                let scan_ctx = ScanCtx {
                    cursor,
                    table,
                    table_alias,
                    schema: Some(schema),
                };
                emit_expr(b, expr, arg_base, Some(&scan_ctx));
            } else {
                let col_idx = agg.arg_col_index.unwrap_or(0);
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                b.emit_op(
                    Opcode::Column,
                    cursor,
                    col_idx as i32,
                    arg_base,
                    P4::None,
                    0,
                );
            }

            // Extra arguments (e.g. separator for group_concat).
            if !agg.extra_args.is_empty() {
                let scan_ctx = ScanCtx {
                    cursor,
                    table,
                    table_alias,
                    schema: Some(schema),
                };
                for (j, extra_expr) in agg.extra_args.iter().enumerate() {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    let extra_reg = arg_base + 1 + j as i32;
                    emit_expr(b, extra_expr, extra_reg, Some(&scan_ctx));
                }
            }

            let num_args = u16::try_from(agg.num_args).unwrap_or_default();
            b.emit_op(
                Opcode::AggStep,
                distinct_flag,
                arg_base,
                accum_reg,
                P4::FuncName(agg.name.clone()),
                num_args,
            );
        }

        // Resolve FILTER skip label after AggStep.
        if let Some(skip_lbl) = filter_skip_label {
            b.resolve_label(skip_lbl);
        }
    }

    // Skip label for WHERE-filtered rows.
    b.resolve_label(skip_label);

    // Next: loop back to start of loop body (instruction after Rewind).
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Next, cursor, loop_body, 0, P4::None, 0);

    // Finalize: emit AggFinal for each aggregate.
    b.resolve_label(finalize_label);

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (i, agg) in agg_columns.iter().enumerate() {
        let accum_reg = accum_base + i as i32;
        b.emit_op(
            Opcode::AggFinal,
            accum_reg,
            agg.num_args,
            0,
            P4::FuncName(agg.name.clone()),
            0,
        );
    }

    // Copy accumulator results to output registers.
    // If accum_base != out_regs, copy; otherwise they're already in place.
    if accum_base != out_regs {
        for i in 0..out_col_count {
            b.emit_op(Opcode::Copy, accum_base + i, out_regs + i, 0, P4::None, 0);
        }
    }

    // ResultRow.
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);

    // Done: Close + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End target for Init jump.
    b.resolve_label(end_label);

    Ok(())
}

/// Parse result columns to extract aggregate function metadata.
fn parse_aggregate_columns(
    columns: &[ResultColumn],
    table: &TableSchema,
) -> Result<Vec<AggColumn>, CodegenError> {
    let mut agg_cols = Vec::new();
    for col in columns {
        match col {
            ResultColumn::Expr {
                expr:
                    Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                        filter,
                        ..
                    },
                ..
            } if is_aggregate_function(name) => {
                let lower_name = name.to_ascii_lowercase();
                let filt = filter.clone();
                match args {
                    FunctionArgs::Star => {
                        // count(*)
                        agg_cols.push(AggColumn {
                            name: lower_name,
                            num_args: 0,
                            arg_col_index: None,
                            arg_is_rowid: false,
                            distinct: *distinct,
                            arg_expr: None,
                            extra_args: Vec::new(),
                            filter: filt,
                        });
                    }
                    FunctionArgs::List(exprs) => {
                        if exprs.is_empty() {
                            // count() with no args — treat like count(*)
                            agg_cols.push(AggColumn {
                                name: lower_name,
                                num_args: 0,
                                arg_col_index: None,
                                arg_is_rowid: false,
                                distinct: *distinct,
                                arg_expr: None,
                                extra_args: Vec::new(),
                                filter: filt,
                            });
                        } else {
                            // First argument: try column reference first,
                            // fall back to storing the expression for emit_expr.
                            let (col_idx, is_rowid, expr) =
                                match resolve_column_ref(&exprs[0], table, None) {
                                    Some(SortKeySource::Column(idx)) => (Some(idx), false, None),
                                    Some(SortKeySource::Rowid) => (None, true, None),
                                    _ => (None, false, Some(Box::new(exprs[0].clone()))),
                                };
                            // Extra arguments (e.g. separator for group_concat).
                            let extra: Vec<Expr> = exprs[1..].to_vec();
                            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                            agg_cols.push(AggColumn {
                                name: lower_name,
                                num_args: exprs.len() as i32,
                                arg_col_index: col_idx,
                                arg_is_rowid: is_rowid,
                                distinct: *distinct,
                                arg_expr: expr,
                                extra_args: extra,
                                filter: filt,
                            });
                        }
                    }
                }
            }
            _ => {
                return Err(CodegenError::Unsupported(
                    "mixed aggregate and non-aggregate columns without GROUP BY".to_owned(),
                ));
            }
        }
    }
    Ok(agg_cols)
}

// ---------------------------------------------------------------------------
// GROUP BY aggregate codegen
// ---------------------------------------------------------------------------

/// A GROUP BY key that is either a simple column reference or an arbitrary
/// expression (e.g. `length(name)`, `substr(city, 1, 1)`).
enum GroupByKey {
    /// Direct table column — read via `Opcode::Column`.
    Column(usize),
    /// Arbitrary expression — evaluated via `emit_expr` during the scan phase.
    Expression(Expr),
}

/// Describes one output column in a GROUP BY query.
enum GroupByOutputCol {
    /// A GROUP BY key column. `key_index` is the position within the group key
    /// vector, and `sorter_col` is the column index in the sorter record.
    GroupKey {
        #[allow(dead_code)]
        key_index: usize,
        sorter_col: usize,
    },
    /// An aggregate function column. `agg_index` is the position within the
    /// aggregate accumulator vector.
    Aggregate { agg_index: usize },
    /// A non-grouped column from `SELECT *`. SQLite allows non-grouped,
    /// non-aggregated columns in GROUP BY queries, returning an arbitrary
    /// row's value. `table_col_index` is the column index in the table;
    /// `sorter_col` is the column index in the sorter record; `is_ipk` is
    /// true if this is an INTEGER PRIMARY KEY (rowid alias).
    NonGroupedColumn {
        table_col_index: usize,
        sorter_col: usize,
        is_ipk: bool,
    },
}

/// Parse result columns for a GROUP BY query into output-column descriptors,
/// a list of group keys (column refs or expressions), and aggregate metadata.
///
/// Returns `(output_cols, group_by_keys, agg_columns)`.
#[allow(clippy::type_complexity)]
fn parse_group_by_output(
    columns: &[ResultColumn],
    table: &TableSchema,
    group_by: &[Expr],
) -> Result<(Vec<GroupByOutputCol>, Vec<GroupByKey>, Vec<AggColumn>), CodegenError> {
    // Resolve GROUP BY expressions: column references become Column(idx),
    // arbitrary expressions (e.g. length(name)) become Expression(expr).
    let group_by_keys: Vec<GroupByKey> = group_by
        .iter()
        .map(|expr| {
            if let Some(col_idx) = resolve_column_index(expr, table) {
                GroupByKey::Column(col_idx)
            } else {
                GroupByKey::Expression(expr.clone())
            }
        })
        .collect();

    let mut output_cols = Vec::new();
    let mut agg_columns = Vec::new();

    for col in columns {
        match col {
            ResultColumn::Expr {
                expr:
                    Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                        filter,
                        ..
                    },
                ..
            } if is_aggregate_function(name) => {
                let agg_index = agg_columns.len();
                let lower_name = name.to_ascii_lowercase();
                let filt = filter.clone();
                match args {
                    FunctionArgs::Star => {
                        agg_columns.push(AggColumn {
                            name: lower_name,
                            num_args: 0,
                            arg_col_index: None,
                            arg_is_rowid: false,
                            distinct: *distinct,
                            arg_expr: None,
                            extra_args: Vec::new(),
                            filter: filt,
                        });
                    }
                    FunctionArgs::List(exprs) => {
                        if exprs.is_empty() {
                            agg_columns.push(AggColumn {
                                name: lower_name,
                                num_args: 0,
                                arg_col_index: None,
                                arg_is_rowid: false,
                                distinct: *distinct,
                                arg_expr: None,
                                extra_args: Vec::new(),
                                filter: filt,
                            });
                        } else {
                            // Try column reference first, fall back to expression.
                            let (col_idx, is_rowid, expr) =
                                match resolve_column_ref(&exprs[0], table, None) {
                                    Some(SortKeySource::Column(idx)) => (Some(idx), false, None),
                                    Some(SortKeySource::Rowid) => (None, true, None),
                                    _ => (None, false, Some(Box::new(exprs[0].clone()))),
                                };
                            let extra: Vec<Expr> = exprs[1..].to_vec();
                            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                            agg_columns.push(AggColumn {
                                name: lower_name,
                                num_args: exprs.len() as i32,
                                arg_col_index: col_idx,
                                arg_is_rowid: is_rowid,
                                distinct: *distinct,
                                arg_expr: expr,
                                extra_args: extra,
                                filter: filt,
                            });
                        }
                    }
                }
                output_cols.push(GroupByOutputCol::Aggregate { agg_index });
            }
            ResultColumn::Expr { expr, .. } => {
                // Match result column to a GROUP BY key: try column index
                // first, then structural expression equality.
                let key_index = if let Some(col_idx) = resolve_column_index(expr, table) {
                    group_by_keys
                        .iter()
                        .position(|k| matches!(k, GroupByKey::Column(c) if *c == col_idx))
                } else {
                    group_by_keys
                        .iter()
                        .position(|k| matches!(k, GroupByKey::Expression(e) if e == expr))
                }
                .ok_or_else(|| {
                    CodegenError::Unsupported("result column not in GROUP BY clause".to_owned())
                })?;
                output_cols.push(GroupByOutputCol::GroupKey {
                    key_index,
                    sorter_col: key_index,
                });
            }
            ResultColumn::Star | ResultColumn::TableStar(_) => {
                // Expand * to all table columns. Each column is either a
                // GROUP BY key or a non-grouped column (SQLite allows this,
                // returning an arbitrary row's value for non-grouped cols).
                for (col_idx, col_info) in table.columns.iter().enumerate() {
                    if let Some(key_index) = group_by_keys
                        .iter()
                        .position(|k| matches!(k, GroupByKey::Column(c) if *c == col_idx))
                    {
                        output_cols.push(GroupByOutputCol::GroupKey {
                            key_index,
                            sorter_col: key_index,
                        });
                    } else {
                        // Non-grouped column — sorter_col assigned later
                        // in codegen_select_group_by_aggregate.
                        output_cols.push(GroupByOutputCol::NonGroupedColumn {
                            table_col_index: col_idx,
                            sorter_col: usize::MAX,
                            is_ipk: col_info.is_ipk,
                        });
                    }
                }
            }
        }
    }

    Ok((output_cols, group_by_keys, agg_columns))
}

/// Generate VDBE bytecode for an aggregate SELECT **with GROUP BY**.
///
/// Two-pass pattern:
/// 1. Scan table rows (with WHERE), pack group-key + agg-arg columns into sorter.
/// 2. After sorting, iterate sorted rows detecting group boundaries via key
///    comparison. On each boundary, finalize accumulators and emit `ResultRow`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn codegen_select_group_by_aggregate(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    group_by: &[Expr],
    having: Option<&Expr>,
    out_regs: i32,
    out_col_count: i32,
    done_label: crate::Label,
    end_label: crate::Label,
) -> Result<(), CodegenError> {
    let (mut output_cols, group_by_keys, agg_columns) =
        parse_group_by_output(columns, table, group_by)?;

    let num_group_keys = group_by_keys.len();
    let num_aggs = agg_columns.len();

    // Collect unique table-column indices needed for aggregate arguments.
    let mut agg_arg_table_cols: Vec<usize> = Vec::new();
    for agg in &agg_columns {
        if let Some(ci) = agg.arg_col_index {
            if !agg_arg_table_cols.contains(&ci) {
                agg_arg_table_cols.push(ci);
            }
        }
    }

    // Count expression-arg aggregates (each gets its own sorter slot).
    let num_expr_args = agg_columns.iter().filter(|a| a.arg_expr.is_some()).count();

    // Count aggregates with FILTER clauses (each gets a boolean sorter slot).
    let num_filter_cols = agg_columns.iter().filter(|a| a.filter.is_some()).count();

    // Count non-grouped columns (from SELECT * expansion) and assign sorter slots.
    let num_nongrouped = output_cols
        .iter()
        .filter(|c| matches!(c, GroupByOutputCol::NonGroupedColumn { .. }))
        .count();
    let nongrouped_start =
        num_group_keys + agg_arg_table_cols.len() + num_expr_args + num_filter_cols;
    let mut next_nongrouped_slot = nongrouped_start;
    for col in &mut output_cols {
        if let GroupByOutputCol::NonGroupedColumn { sorter_col, .. } = col {
            *sorter_col = next_nongrouped_slot;
            next_nongrouped_slot += 1;
        }
    }

    // Sorter layout: [group_keys..., col_args..., expr_args..., filter_bools..., nongrouped_cols...]
    let total_sorter_cols = num_group_keys
        + agg_arg_table_cols.len()
        + num_expr_args
        + num_filter_cols
        + num_nongrouped;

    // Map each aggregate's arg to its sorter column index.
    let mut agg_sorter_col: Vec<Option<usize>> = Vec::with_capacity(agg_columns.len());
    let mut next_expr_slot = num_group_keys + agg_arg_table_cols.len();
    for agg in &agg_columns {
        let sorter_col = if agg.arg_expr.is_some() {
            let slot = next_expr_slot;
            next_expr_slot += 1;
            Some(slot)
        } else if let Some(ci) = agg.arg_col_index {
            let Some(pos) = agg_arg_table_cols.iter().position(|&x| x == ci) else {
                return Err(CodegenError::Unsupported(
                    "internal: aggregate argument column missing from sorter layout".to_owned(),
                ));
            };
            Some(num_group_keys + pos)
        } else {
            None
        };
        agg_sorter_col.push(sorter_col);
    }

    // Map each FILTER-bearing aggregate to its boolean sorter column.
    let mut filter_sorter_col: Vec<Option<usize>> = Vec::with_capacity(agg_columns.len());
    let mut next_filter_slot = num_group_keys + agg_arg_table_cols.len() + num_expr_args;
    for agg in &agg_columns {
        if agg.filter.is_some() {
            filter_sorter_col.push(Some(next_filter_slot));
            next_filter_slot += 1;
        } else {
            filter_sorter_col.push(None);
        }
    }

    // Sorter cursor.
    let sorter_cursor = cursor + 1;

    // Open sorter: p2 = number of key columns (for sorting by group keys).
    let sort_order: String = std::iter::repeat_n('+', num_group_keys).collect();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::SorterOpen,
        sorter_cursor,
        num_group_keys as i32,
        0,
        P4::Str(sort_order),
        0,
    );

    // Open table for reading.
    b.emit_op(
        Opcode::OpenRead,
        cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // === Pass 1: Scan rows into sorter ===
    let scan_start = b.current_addr();
    let scan_done = b.emit_label();
    b.emit_jump_to_label(Opcode::Rewind, cursor, 0, scan_done, P4::None, 0);

    // WHERE filter.
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        emit_where_filter(
            b,
            where_expr,
            cursor,
            table,
            table_alias,
            schema,
            skip_label,
        );
    }

    // Read group-key values + agg-arg columns into consecutive registers.
    // For column-based keys, use Opcode::Column; for expression-based keys,
    // evaluate the expression via emit_expr.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let sorter_base = b.alloc_regs(total_sorter_cols as i32);
    {
        let scan_ctx = ScanCtx {
            cursor,
            table,
            table_alias,
            schema: Some(schema),
        };
        let mut reg = sorter_base;
        for key in &group_by_keys {
            match key {
                GroupByKey::Column(col_idx) => {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    b.emit_op(Opcode::Column, cursor, *col_idx as i32, reg, P4::None, 0);
                }
                GroupByKey::Expression(expr) => {
                    emit_expr(b, expr, reg, Some(&scan_ctx));
                }
            }
            reg += 1;
        }
        for &col_idx in &agg_arg_table_cols {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(Opcode::Column, cursor, col_idx as i32, reg, P4::None, 0);
            reg += 1;
        }
        // Expression-arg aggregates: evaluate each expression into its sorter slot.
        for agg in &agg_columns {
            if let Some(ref expr) = agg.arg_expr {
                emit_expr(b, expr, reg, Some(&scan_ctx));
                reg += 1;
            }
        }
        // FILTER clause booleans: evaluate each filter and store 0/1 in sorter.
        for agg in &agg_columns {
            if let Some(ref filter_expr) = agg.filter {
                emit_expr(b, filter_expr, reg, Some(&scan_ctx));
                reg += 1;
            }
        }

        // Non-grouped columns (from SELECT * expansion): store in sorter.
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        for col in &output_cols {
            if let GroupByOutputCol::NonGroupedColumn {
                table_col_index,
                is_ipk,
                ..
            } = col
            {
                if *is_ipk {
                    b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                } else {
                    b.emit_op(
                        Opcode::Column,
                        cursor,
                        *table_col_index as i32,
                        reg,
                        P4::None,
                        0,
                    );
                }
                reg += 1;
            }
        }
    }

    // MakeRecord + SorterInsert.
    let record_reg = b.alloc_reg();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::MakeRecord,
        sorter_base,
        total_sorter_cols as i32,
        record_reg,
        P4::None,
        0,
    );
    b.emit_op(
        Opcode::SorterInsert,
        sorter_cursor,
        record_reg,
        0,
        P4::None,
        0,
    );

    // Skip label (for WHERE-filtered rows).
    b.resolve_label(skip_label);

    // Next row in scan.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let scan_body = (scan_start + 1) as i32;
    b.emit_op(Opcode::Next, cursor, scan_body, 0, P4::None, 0);

    // End of pass 1.
    b.resolve_label(scan_done);
    b.emit_op(Opcode::Close, cursor, 0, 0, P4::None, 0);

    // === Pass 2: Iterate sorted rows, accumulate per-group ===

    // Allocate registers for current group keys, previous group keys, accumulators,
    // and non-grouped column values.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let cur_key_base = b.alloc_regs(num_group_keys as i32);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let prev_key_base = b.alloc_regs(num_group_keys as i32);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let accum_base = b.alloc_regs(num_aggs as i32);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let prev_nongrouped_base = b.alloc_regs(num_nongrouped.max(1) as i32);
    let first_flag = b.alloc_reg();

    // Initialize: first_flag = 1, accumulators = Null.
    b.emit_op(Opcode::Integer, 1, first_flag, 0, P4::None, 0);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for i in 0..num_aggs as i32 {
        b.emit_op(Opcode::Null, 0, accum_base + i, 0, P4::None, 0);
    }

    // SorterSort: sort and position at first row; jump to done if empty.
    b.emit_jump_to_label(
        Opcode::SorterSort,
        sorter_cursor,
        0,
        done_label,
        P4::None,
        0,
    );

    let sort_loop_body = b.current_addr();

    // SorterData: decode current sorted row.
    let sorted_reg = b.alloc_reg();
    b.emit_op(
        Opcode::SorterData,
        sorter_cursor,
        sorted_reg,
        0,
        P4::None,
        0,
    );

    // Read group-key columns from sorter into cur_key registers.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for i in 0..num_group_keys {
        b.emit_op(
            Opcode::Column,
            sorter_cursor,
            i as i32,
            cur_key_base + i as i32,
            P4::None,
            0,
        );
    }

    // If first row, skip group-change comparison.
    let first_row_label = b.emit_label();
    b.emit_jump_to_label(Opcode::IfPos, first_flag, 1, first_row_label, P4::None, 0);

    // Compare current keys to previous keys. If any differ, jump to new_group.
    let new_group_label = b.emit_label();
    let same_group_label = b.emit_label();
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for i in 0..num_group_keys {
        // Ne p1=cur_key, p2=new_group_label, p3=prev_key, p5=0x80 (NULLEQ)
        b.emit_jump_to_label(
            Opcode::Ne,
            cur_key_base + i as i32,
            prev_key_base + i as i32,
            new_group_label,
            P4::None,
            0x80,
        );
    }
    // All keys match — same group.
    b.emit_jump_to_label(Opcode::Goto, 0, 0, same_group_label, P4::None, 0);

    // new_group: finalize previous group and output ResultRow.
    b.resolve_label(new_group_label);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (i, agg) in agg_columns.iter().enumerate() {
        let accum_reg = accum_base + i as i32;
        b.emit_op(
            Opcode::AggFinal,
            accum_reg,
            agg.num_args,
            0,
            P4::FuncName(agg.name.clone()),
            0,
        );
    }
    // Build output row from prev_key + accum + prev_nongrouped.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    {
        let mut ng_idx = 0i32;
        for (i, out_col) in output_cols.iter().enumerate() {
            match out_col {
                GroupByOutputCol::GroupKey { sorter_col, .. } => {
                    b.emit_op(
                        Opcode::Copy,
                        prev_key_base + *sorter_col as i32,
                        out_regs + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                }
                GroupByOutputCol::Aggregate { agg_index } => {
                    b.emit_op(
                        Opcode::Copy,
                        accum_base + *agg_index as i32,
                        out_regs + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                }
                GroupByOutputCol::NonGroupedColumn { .. } => {
                    b.emit_op(
                        Opcode::Copy,
                        prev_nongrouped_base + ng_idx,
                        out_regs + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                    ng_idx += 1;
                }
            }
        }
    }
    // HAVING filter: skip this group's output if HAVING predicate is false.
    let having_skip_label = b.emit_label();
    if let Some(having_expr) = having {
        emit_having_filter(
            b,
            having_expr,
            &output_cols,
            &agg_columns,
            &group_by_keys,
            table,
            out_regs,
            having_skip_label,
        );
    }
    b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    b.resolve_label(having_skip_label);
    // Reset accumulators for next group.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for i in 0..num_aggs as i32 {
        b.emit_op(Opcode::Null, 0, accum_base + i, 0, P4::None, 0);
    }

    // first_row: (jumped here when first_flag was 1, skipping comparison).
    b.resolve_label(first_row_label);

    // same_group: copy current keys to previous, then AggStep.
    b.resolve_label(same_group_label);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for i in 0..num_group_keys {
        b.emit_op(
            Opcode::Copy,
            cur_key_base + i as i32,
            prev_key_base + i as i32,
            0,
            P4::None,
            0,
        );
    }

    // Copy non-grouped columns from sorter to prev_nongrouped registers.
    // These hold the latest (arbitrary) value for each non-grouped column.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    {
        let mut ng_idx = 0i32;
        for col in &output_cols {
            if let GroupByOutputCol::NonGroupedColumn { sorter_col, .. } = col {
                b.emit_op(
                    Opcode::Column,
                    sorter_cursor,
                    *sorter_col as i32,
                    prev_nongrouped_base + ng_idx,
                    P4::None,
                    0,
                );
                ng_idx += 1;
            }
        }
    }

    // AggStep for each aggregate.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (i, agg) in agg_columns.iter().enumerate() {
        let accum_reg = accum_base + i as i32;

        // FILTER clause: read boolean from sorter and skip AggStep if false/NULL.
        let filter_skip_label = if let Some(filt_col) = filter_sorter_col[i] {
            let skip_lbl = b.emit_label();
            let filt_reg = b.alloc_temp();
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(
                Opcode::Column,
                sorter_cursor,
                filt_col as i32,
                filt_reg,
                P4::None,
                0,
            );
            // p3=1: treat NULL as false (skip AggStep).
            b.emit_jump_to_label(Opcode::IfNot, filt_reg, 1, skip_lbl, P4::None, 0);
            b.free_temp(filt_reg);
            Some(skip_lbl)
        } else {
            None
        };

        let distinct_flag = i32::from(agg.distinct);
        if agg.num_args == 0 {
            // count(*): no arguments.
            b.emit_op(
                Opcode::AggStep,
                distinct_flag,
                0,
                accum_reg,
                P4::FuncName(agg.name.clone()),
                0,
            );
        } else {
            let total_args = agg.num_args.max(1);
            let arg_base = b.alloc_regs(total_args);
            let Some(sorter_col) = agg_sorter_col[i] else {
                return Err(CodegenError::Unsupported(
                    "internal: non-zero-arg aggregate missing sorter column".to_owned(),
                ));
            };
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(
                Opcode::Column,
                sorter_cursor,
                sorter_col as i32,
                arg_base,
                P4::None,
                0,
            );
            // Extra arguments (e.g. separator for group_concat):
            // re-evaluate inline since they are typically constant expressions.
            for (j, extra_expr) in agg.extra_args.iter().enumerate() {
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let extra_reg = arg_base + 1 + j as i32;
                emit_expr(b, extra_expr, extra_reg, None);
            }
            let step_p5 = u16::try_from(agg.num_args).unwrap_or_default();
            b.emit_op(
                Opcode::AggStep,
                distinct_flag,
                arg_base,
                accum_reg,
                P4::FuncName(agg.name.clone()),
                step_p5,
            );
        }

        // Resolve FILTER skip label after AggStep.
        if let Some(skip_lbl) = filter_skip_label {
            b.resolve_label(skip_lbl);
        }
    }

    // SorterNext: advance to next sorted row.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::SorterNext,
        sorter_cursor,
        sort_loop_body as i32,
        0,
        P4::None,
        0,
    );

    // After loop: output final group (if any rows were processed).
    // If first_flag is still > 0, table was empty — skip final output.
    b.emit_jump_to_label(Opcode::IfPos, first_flag, 0, done_label, P4::None, 0);

    // Finalize last group.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for (i, agg) in agg_columns.iter().enumerate() {
        let accum_reg = accum_base + i as i32;
        b.emit_op(
            Opcode::AggFinal,
            accum_reg,
            agg.num_args,
            0,
            P4::FuncName(agg.name.clone()),
            0,
        );
    }
    // Build output row from prev_key (last group's keys) + accum + prev_nongrouped.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    {
        let mut ng_idx = 0i32;
        for (i, out_col) in output_cols.iter().enumerate() {
            match out_col {
                GroupByOutputCol::GroupKey { sorter_col, .. } => {
                    b.emit_op(
                        Opcode::Copy,
                        prev_key_base + *sorter_col as i32,
                        out_regs + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                }
                GroupByOutputCol::Aggregate { agg_index } => {
                    b.emit_op(
                        Opcode::Copy,
                        accum_base + *agg_index as i32,
                        out_regs + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                }
                GroupByOutputCol::NonGroupedColumn { .. } => {
                    b.emit_op(
                        Opcode::Copy,
                        prev_nongrouped_base + ng_idx,
                        out_regs + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                    ng_idx += 1;
                }
            }
        }
    }
    // HAVING filter for the final group.
    if let Some(having_expr) = having {
        let final_having_skip = b.emit_label();
        emit_having_filter(
            b,
            having_expr,
            &output_cols,
            &agg_columns,
            &group_by_keys,
            table,
            out_regs,
            final_having_skip,
        );
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
        b.resolve_label(final_having_skip);
    } else {
        b.emit_op(Opcode::ResultRow, out_regs, out_col_count, 0, P4::None, 0);
    }

    // Done: Close sorter + Halt.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, sorter_cursor, 0, 0, P4::None, 0);
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
/// Init → Transaction(write) → OpenWrite → Variable* → (IPK routing |
/// NewRowid) → MakeRecord → Insert → Close → Halt
#[allow(clippy::too_many_lines)]
pub fn codegen_insert(
    b: &mut ProgramBuilder,
    stmt: &InsertStatement,
    schema: &[TableSchema],
    ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let table = find_table(schema, &stmt.table.name)?;
    let table_cursor = 0_i32;

    let end_label = b.emit_label();

    // Init.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (write, p2=1).
    b.emit_op(Opcode::Transaction, 0, 1, 0, P4::None, 0);

    // OpenWrite for table.
    b.emit_op(
        Opcode::OpenWrite,
        table_cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // OpenWrite for each index (bd-so1h: Phase 5I.3).
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    for (idx_offset, index) in table.indexes.iter().enumerate() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        b.emit_op(
            Opcode::OpenWrite,
            idx_cursor,
            index.root_page,
            0,
            P4::Table(index.name.clone()),
            0,
        );
    }

    // Conflict behavior applies uniformly across INSERT sources.
    let oe_flag = conflict_action_to_oe(stmt.or_conflict.as_ref());

    match &stmt.source {
        InsertSource::Values(rows) => {
            if rows.is_empty() {
                return Err(CodegenError::Unsupported("empty VALUES".to_owned()));
            }

            // When an explicit column list is provided, reorder each VALUES
            // row from column-list order to table-schema order, filling
            // unmentioned columns with NULL.  This ensures MakeRecord always
            // packs fields in the order the table schema expects.
            if stmt.columns.is_empty() {
                codegen_insert_values(
                    b,
                    rows,
                    None,
                    table_cursor,
                    table,
                    &stmt.returning,
                    ctx,
                    oe_flag,
                )?;
            } else {
                // Build default expressions: use column DEFAULT if available, else NULL.
                let defaults: Vec<Expr> = table.columns.iter().map(default_value_to_expr).collect();
                let mut explicit_rowids: Vec<Option<Expr>> = Vec::with_capacity(rows.len());
                let mut reordered: Vec<Vec<Expr>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut table_order = defaults.clone();
                    let mut explicit_rowid = None;
                    for (val_pos, col_name) in stmt.columns.iter().enumerate() {
                        if let Some(expr) = row.get(val_pos) {
                            if is_rowid_alias(col_name) {
                                explicit_rowid = Some(expr.clone());
                            } else {
                                let tbl_pos = table.column_index(col_name).ok_or_else(|| {
                                    CodegenError::ColumnNotFound {
                                        table: table.name.clone(),
                                        column: col_name.clone(),
                                    }
                                })?;
                                table_order[tbl_pos] = expr.clone();
                            }
                        }
                    }
                    explicit_rowids.push(explicit_rowid);
                    reordered.push(table_order);
                }
                codegen_insert_values(
                    b,
                    &reordered,
                    Some(&explicit_rowids),
                    table_cursor,
                    table,
                    &stmt.returning,
                    ctx,
                    oe_flag,
                )?;
            }
        }
        InsertSource::Select(select_stmt) => {
            // INSERT ... SELECT: columns arrive in SELECT output order.
            // When a column list is present, remap the IPK index from
            // table-schema position to SELECT output position.
            let select_ctx = if stmt.columns.is_empty() {
                ctx.clone()
            } else if let Some(ipk_schema_idx) = ctx.rowid_alias_col_idx {
                let ipk_col_name = &table.columns[ipk_schema_idx].name;
                let select_pos = stmt
                    .columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(ipk_col_name));
                CodegenContext {
                    concurrent_mode: ctx.concurrent_mode,
                    rowid_alias_col_idx: select_pos,
                }
            } else {
                ctx.clone()
            };
            codegen_insert_select(
                b,
                select_stmt,
                table_cursor,
                table,
                schema,
                &stmt.returning,
                &select_ctx,
                oe_flag,
            )?;
        }
        InsertSource::DefaultValues => {
            // Insert one row using column DEFAULT values (or NULL if none).
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let n_cols = table.columns.len() as i32;
            let concurrent_flag = i32::from(ctx.concurrent_mode);
            let col_regs = b.alloc_regs(n_cols);
            for (idx, col) in table.columns.iter().enumerate() {
                #[allow(clippy::cast_possible_wrap)]
                let reg = col_regs + idx as i32;
                emit_default_value(b, col, reg);
            }
            let rowid_reg = b.alloc_reg();
            if let Some(ipk_idx) = ctx.rowid_alias_col_idx {
                // IPK column has a DEFAULT value — use it when non-NULL,
                // otherwise auto-generate via NewRowid (matching VALUES path).
                #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
                let ipk_reg = col_regs + ipk_idx as i32;
                let auto_label = b.emit_label();
                let done_label = b.emit_label();

                b.emit_jump_to_label(Opcode::IsNull, ipk_reg, 0, auto_label, P4::None, 0);
                b.emit_op(Opcode::Copy, ipk_reg, rowid_reg, 0, P4::None, 0);
                b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

                b.resolve_label(auto_label);
                b.emit_op(
                    Opcode::NewRowid,
                    table_cursor,
                    rowid_reg,
                    concurrent_flag,
                    P4::None,
                    0,
                );
                b.emit_op(Opcode::Copy, rowid_reg, ipk_reg, 0, P4::None, 0);

                b.resolve_label(done_label);
            } else {
                // No IPK column — always auto-generate.
                b.emit_op(
                    Opcode::NewRowid,
                    table_cursor,
                    rowid_reg,
                    concurrent_flag,
                    P4::None,
                    0,
                );
            }
            let rec_reg = b.alloc_reg();
            emit_strict_type_check(b, table, col_regs);
            b.emit_op(
                Opcode::MakeRecord,
                col_regs,
                n_cols,
                rec_reg,
                P4::Affinity(table.affinity_string()),
                0,
            );
            b.emit_op(
                Opcode::Insert,
                table_cursor,
                rec_reg,
                rowid_reg,
                P4::Table(table.name.clone()),
                oe_flag,
            );

            // Index maintenance: insert into each index (bd-so1h).
            emit_index_inserts(b, table, table_cursor, col_regs, rowid_reg);

            if !stmt.returning.is_empty() {
                emit_returning(b, table_cursor, table, &stmt.returning, rowid_reg)?;
            }
        }
    }

    // Close table cursor.
    b.emit_op(Opcode::Close, table_cursor, 0, 0, P4::None, 0);

    // Close index cursors (bd-so1h).
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    for idx_offset in 0..table.indexes.len() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        b.emit_op(Opcode::Close, idx_cursor, 0, 0, P4::None, 0);
    }

    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

    Ok(())
}

/// Emit the INSERT loop for `VALUES (row), (row), ...`.
///
/// # Arguments
/// * `oe_flag` - Conflict resolution flag (OE_ABORT, OE_IGNORE, OE_REPLACE, etc.)
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_arguments
)]
fn codegen_insert_values(
    b: &mut ProgramBuilder,
    rows: &[Vec<Expr>],
    explicit_rowids: Option<&[Option<Expr>]>,
    cursor: i32,
    table: &TableSchema,
    returning: &[ResultColumn],
    ctx: &CodegenContext,
    oe_flag: u16,
) -> Result<(), CodegenError> {
    let n_cols = rows
        .first()
        .ok_or_else(|| CodegenError::Unsupported("empty VALUES".to_owned()))?
        .len();
    let rowid_reg = b.alloc_reg();
    let val_regs = b.alloc_regs(n_cols as i32);
    let rec_reg = b.alloc_reg();
    let concurrent_flag = i32::from(ctx.concurrent_mode);

    let explicit_rowid_reg = explicit_rowids.map(|_| b.alloc_reg());

    for (row_idx, row_values) in rows.iter().enumerate() {
        if row_values.len() != n_cols {
            return Err(CodegenError::Unsupported(
                "VALUES rows must have the same arity".to_owned(),
            ));
        }

        // Emit value expressions into registers.
        for (i, val_expr) in row_values.iter().enumerate() {
            let reg = val_regs + i as i32;
            emit_expr(b, val_expr, reg, None);
        }

        let explicit_rowid_expr = explicit_rowids
            .and_then(|rows| rows.get(row_idx))
            .and_then(Option::as_ref);

        // Rowid determination precedence:
        // 1. explicit rowid/_rowid_/oid in INSERT column list
        // 2. INTEGER PRIMARY KEY column value
        // 3. auto-generated rowid
        if let Some(rowid_expr) = explicit_rowid_expr {
            let rowid_value_reg = explicit_rowid_reg.expect("explicit rowid register allocated");
            emit_expr(b, rowid_expr, rowid_value_reg, None);
            let auto_label = b.emit_label();
            let done_label = b.emit_label();

            b.emit_jump_to_label(Opcode::IsNull, rowid_value_reg, 0, auto_label, P4::None, 0);
            b.emit_op(Opcode::Copy, rowid_value_reg, rowid_reg, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

            b.resolve_label(auto_label);
            b.emit_op(
                Opcode::NewRowid,
                cursor,
                rowid_reg,
                concurrent_flag,
                P4::None,
                0,
            );
            if let Some(ipk_idx) = ctx.rowid_alias_col_idx {
                #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
                let ipk_reg = val_regs + ipk_idx as i32;
                b.emit_op(Opcode::Copy, rowid_reg, ipk_reg, 0, P4::None, 0);
            }

            b.resolve_label(done_label);
        } else if let Some(ipk_idx) = ctx.rowid_alias_col_idx {
            #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
            let ipk_reg = val_regs + ipk_idx as i32;
            let auto_label = b.emit_label();
            let done_label = b.emit_label();

            // If the user-supplied IPK value is NULL, jump to auto-generate.
            b.emit_jump_to_label(Opcode::IsNull, ipk_reg, 0, auto_label, P4::None, 0);

            // Non-NULL path: copy user value into rowid register.
            b.emit_op(Opcode::Copy, ipk_reg, rowid_reg, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

            // NULL path: auto-generate rowid, then sync it back into the
            // IPK column register so MakeRecord includes the real rowid.
            b.resolve_label(auto_label);
            b.emit_op(
                Opcode::NewRowid,
                cursor,
                rowid_reg,
                concurrent_flag,
                P4::None,
                0,
            );
            b.emit_op(Opcode::Copy, rowid_reg, ipk_reg, 0, P4::None, 0);

            b.resolve_label(done_label);
        } else {
            // No IPK column — always auto-generate.
            b.emit_op(
                Opcode::NewRowid,
                cursor,
                rowid_reg,
                concurrent_flag,
                P4::None,
                0,
            );
        }

        // Apply column type affinities before packing the record.
        let aff_str = table.affinity_string();
        b.emit_op(
            Opcode::Affinity,
            val_regs,
            n_cols as i32,
            0,
            P4::Affinity(aff_str.clone()),
            0,
        );

        // MakeRecord: pack columns into a record.
        emit_strict_type_check(b, table, val_regs);
        let n_cols_i32 = n_cols as i32;
        b.emit_op(
            Opcode::MakeRecord,
            val_regs,
            n_cols_i32,
            rec_reg,
            P4::Affinity(aff_str),
            0,
        );

        // Insert with conflict resolution flag.
        b.emit_op(
            Opcode::Insert,
            cursor,
            rec_reg,
            rowid_reg,
            P4::Table(table.name.clone()),
            oe_flag,
        );

        // Index maintenance: insert into each index (bd-so1h).
        emit_index_inserts(b, table, cursor, val_regs, rowid_reg);

        // RETURNING clause: position cursor on inserted row and read columns.
        if !returning.is_empty() {
            emit_returning(b, cursor, table, returning, rowid_reg)?;
        }
    }

    Ok(())
}

/// Emit the INSERT loop for `INSERT INTO target SELECT ... FROM source`.
///
/// Opens the source table for reading (cursor = `write_cursor + 1`), scans
/// rows with an optional WHERE filter, reads projected columns, and inserts
/// each row into the target table.
///
/// # Arguments
/// * `oe_flag` - Conflict resolution flag (OE_ABORT, OE_IGNORE, OE_REPLACE, etc.)
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
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
    oe_flag: u16,
) -> Result<(), CodegenError> {
    // Extract columns, FROM, and WHERE from the inner SELECT.
    let (columns, from, where_clause) = match &select_stmt.body.select {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            ..
        } => (columns, from, where_clause),
        SelectCore::Values(_) => {
            return Err(CodegenError::Unsupported(
                "INSERT ... SELECT with VALUES body".to_owned(),
            ));
        }
    };

    let from_clause = from
        .as_ref()
        .ok_or_else(|| CodegenError::Unsupported("INSERT ... SELECT without FROM".to_owned()))?;

    if columns.iter().any(contains_unsupported_in_result_column)
        || where_clause
            .as_ref()
            .is_some_and(|expr| contains_unsupported_in_expr(expr))
    {
        return Err(CodegenError::Unsupported(unsupported_in_message()));
    }

    let (src_table_name, src_table_alias) = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => (&name.name, alias.as_deref()),
        _ => {
            return Err(CodegenError::Unsupported(
                "INSERT ... SELECT from non-table source".to_owned(),
            ));
        }
    };

    let src_table = find_table(schema, src_table_name)?;
    let read_cursor = write_cursor + 1;

    // Determine the number of output columns from the SELECT.
    let n_cols = result_column_count(columns, src_table);

    // Allocate registers for the scan → insert pipeline.
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

    // WHERE filter on source rows (skip non-matching).
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        emit_where_filter(
            b,
            where_expr,
            read_cursor,
            src_table,
            src_table_alias,
            schema,
            skip_label,
        );
    }

    // Read projected columns from source into val_regs.
    emit_column_reads(
        b,
        read_cursor,
        columns,
        src_table,
        src_table_alias,
        schema,
        val_regs,
    )?;

    // Rowid determination: use IPK column value if present, else auto-generate.
    if let Some(ipk_idx) = ctx.rowid_alias_col_idx {
        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        let ipk_reg = val_regs + ipk_idx as i32;
        let auto_label = b.emit_label();
        let done_rowid = b.emit_label();

        b.emit_jump_to_label(Opcode::IsNull, ipk_reg, 0, auto_label, P4::None, 0);

        // Non-NULL: use the selected value as rowid.
        b.emit_op(Opcode::Copy, ipk_reg, rowid_reg, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::Goto, 0, 0, done_rowid, P4::None, 0);

        // NULL: auto-generate and sync back.
        b.resolve_label(auto_label);
        b.emit_op(
            Opcode::NewRowid,
            write_cursor,
            rowid_reg,
            concurrent_flag,
            P4::None,
            0,
        );
        b.emit_op(Opcode::Copy, rowid_reg, ipk_reg, 0, P4::None, 0);

        b.resolve_label(done_rowid);
    } else {
        b.emit_op(
            Opcode::NewRowid,
            write_cursor,
            rowid_reg,
            concurrent_flag,
            P4::None,
            0,
        );
    }

    // Apply column type affinities before packing the record.
    let aff_str = target_table.affinity_string();
    b.emit_op(
        Opcode::Affinity,
        val_regs,
        n_cols,
        0,
        P4::Affinity(aff_str.clone()),
        0,
    );

    // MakeRecord from the read column values.
    emit_strict_type_check(b, target_table, val_regs);
    b.emit_op(
        Opcode::MakeRecord,
        val_regs,
        n_cols,
        rec_reg,
        P4::Affinity(aff_str),
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

    // Index maintenance: insert into each index (bd-so1h).
    emit_index_inserts(b, target_table, write_cursor, val_regs, rowid_reg);

    // RETURNING clause: position cursor on inserted row and read columns.
    if !returning.is_empty() {
        emit_returning(b, write_cursor, target_table, returning, rowid_reg)?;
    }

    // Skip label for WHERE-filtered rows.
    b.resolve_label(skip_label);

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
    ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let table_name = table_name_from_qualified(&stmt.table);
    let table = find_table(schema, table_name)?;
    let cursor = 0_i32;
    let n_cols = table.columns.len();

    let end_label = b.emit_label();
    let done_label = b.emit_label();

    if stmt
        .where_clause
        .as_ref()
        .is_some_and(contains_unsupported_in_expr)
        || stmt
            .assignments
            .iter()
            .any(|assign| contains_unsupported_in_expr(&assign.value))
    {
        return Err(CodegenError::Unsupported(unsupported_in_message()));
    }

    // Init.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (write).
    b.emit_op(Opcode::Transaction, 0, 1, 0, P4::None, 0);

    // Resolve assignment targets to column indices.
    let assignment_cols: Vec<usize> = stmt
        .assignments
        .iter()
        .map(|assign| {
            let col_name = match &assign.target {
                fsqlite_ast::AssignmentTarget::Column(name) => name.as_str(),
                fsqlite_ast::AssignmentTarget::ColumnList(names) => {
                    names.first().map_or("", |n| n.as_str())
                }
            };
            table
                .column_index(col_name)
                .ok_or_else(|| CodegenError::ColumnNotFound {
                    table: table.name.clone(),
                    column: col_name.to_owned(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // OpenWrite for table.
    let table_cursor = cursor;
    b.emit_op(
        Opcode::OpenWrite,
        table_cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // OpenWrite for each index (bd-2f9t: Phase 5I.5).
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    for (idx_offset, index) in table.indexes.iter().enumerate() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        b.emit_op(
            Opcode::OpenWrite,
            idx_cursor,
            index.root_page,
            0,
            P4::Table(index.name.clone()),
            0,
        );
    }

    // Full table scan: Rewind → loop body → Next.
    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, table_cursor, 0, done_label, P4::None, 0);

    // Count anonymous placeholders in SET assignments.
    // Parameters are numbered in SQL textual order (SET first, then WHERE), but
    // bytecode emits WHERE before SET. We must set the placeholder counter
    // so WHERE placeholders get indices *after* the SET placeholders.
    let set_placeholder_count: u32 = stmt
        .assignments
        .iter()
        .map(|a| count_anon_placeholders(&a.value))
        .sum();
    let where_placeholder_count = stmt
        .where_clause
        .as_ref()
        .map_or(0, count_anon_placeholders);

    // Evaluate WHERE condition (if any) and skip non-matching rows.
    let skip_label = b.emit_label();
    if let Some(where_expr) = &stmt.where_clause {
        // Set placeholder counter to start after SET placeholders.
        b.set_next_anon_placeholder(set_placeholder_count + 1);
        emit_where_filter(
            b,
            where_expr,
            table_cursor,
            table,
            stmt.table.alias.as_deref(),
            schema,
            skip_label,
        );
    }

    // Read ALL existing columns into registers.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let col_regs = b.alloc_regs(n_cols as i32);
    for i in 0..n_cols {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let target_reg = col_regs + i as i32;
        if table.columns.get(i).is_some_and(|col| col.is_ipk) {
            // INTEGER PRIMARY KEY columns alias rowid and are not stored in
            // the record payload. Materialize from Rowid so unchanged UPDATE
            // rewrites preserve the original key instead of generating a new
            // rowid.
            b.emit_op(Opcode::Rowid, table_cursor, target_reg, 0, P4::None, 0);
        } else {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(
                Opcode::Column,
                table_cursor,
                i as i32,
                target_reg,
                P4::None,
                0,
            );
        }
    }

    // Index maintenance (bd-2f9t): Delete OLD index entries BEFORE updating values.
    // col_regs currently contains OLD column values.
    emit_index_deletes(b, table, table_cursor);

    // Evaluate new values from AST expressions and overwrite changed columns.
    // A ScanCtx is required so that column references in SET expressions
    // (e.g., `SET val = val + 5`) resolve to the cursor's current row.
    let update_ctx = ScanCtx {
        cursor: table_cursor,
        table,
        table_alias: stmt.table.alias.as_deref(),
        schema: Some(schema),
    };
    // Reset placeholder counter to 1 for SET expressions (they appear first in SQL text).
    b.set_next_anon_placeholder(1);
    for (assign_idx, col_idx) in assignment_cols.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let target = col_regs + *col_idx as i32;
        emit_expr(
            b,
            &stmt.assignments[assign_idx].value,
            target,
            Some(&update_ctx),
        );
    }

    // Get the current rowid before deleting the old row.
    let old_rowid_reg = b.alloc_reg();
    b.emit_op(Opcode::Rowid, table_cursor, old_rowid_reg, 0, P4::None, 0);

    // UPDATE is delete+insert: remove the current row first, then insert the
    // rewritten record (possibly at a new rowid).
    b.emit_op(Opcode::Delete, table_cursor, 0, 0, P4::None, 0);

    // Determine destination rowid for re-insertion.
    let mut rowid_reg = old_rowid_reg;
    let rowid_alias_col_idx = ctx
        .rowid_alias_col_idx
        .or_else(|| table.columns.iter().position(|col| col.is_ipk));
    if let Some(ipk_idx) = rowid_alias_col_idx {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let ipk_reg = col_regs + ipk_idx as i32;
        let auto_label = b.emit_label();
        let rowid_done_label = b.emit_label();
        let concurrent_flag = i32::from(ctx.concurrent_mode);

        rowid_reg = b.alloc_reg();

        // If the rewritten IPK is NULL, allocate a new rowid.
        b.emit_jump_to_label(Opcode::IsNull, ipk_reg, 0, auto_label, P4::None, 0);
        b.emit_op(Opcode::Copy, ipk_reg, rowid_reg, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::Goto, 0, 0, rowid_done_label, P4::None, 0);

        b.resolve_label(auto_label);
        b.emit_op(
            Opcode::NewRowid,
            table_cursor,
            rowid_reg,
            concurrent_flag,
            P4::None,
            0,
        );
        // Keep the IPK payload column consistent with the chosen rowid.
        b.emit_op(Opcode::Copy, rowid_reg, ipk_reg, 0, P4::None, 0);
        b.resolve_label(rowid_done_label);
    }

    // MakeRecord with ALL columns.
    emit_strict_type_check(b, table, col_regs);
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
        table_cursor,
        rec_reg,
        rowid_reg,
        P4::Table(table.name.clone()),
        0x08, // OPFLAG_ISUPDATE
    );

    // Index maintenance (bd-2f9t): Insert NEW index entries after table insert.
    // col_regs now contains NEW column values.
    emit_index_inserts(b, table, table_cursor, col_regs, rowid_reg);

    // RETURNING clause: position cursor on updated row and read columns.
    if !stmt.returning.is_empty() {
        // RETURNING appears after WHERE in SQL textual order; restore the
        // post-WHERE placeholder index so RETURNING placeholders don't collide
        // with SET placeholder numbering.
        b.set_next_anon_placeholder(set_placeholder_count + where_placeholder_count + 1);
        emit_returning(b, table_cursor, table, &stmt.returning, rowid_reg)?;
    }

    // Skip label for WHERE-filtered rows.
    b.resolve_label(skip_label);

    // Next: jump back to loop body.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Next, table_cursor, loop_body, 0, P4::None, 0);

    // Done: Close index cursors, then table cursor.
    b.resolve_label(done_label);
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    for idx_offset in 0..table.indexes.len() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        b.emit_op(Opcode::Close, idx_cursor, 0, 0, P4::None, 0);
    }
    b.emit_op(Opcode::Close, table_cursor, 0, 0, P4::None, 0);
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
/// Handles both rowid-equality WHERE and general column-based WHERE via
/// a full table scan with filter.
///
/// Init → Transaction(write) → OpenWrite → Rewind → [WHERE filter] →
/// Delete → Next → Close → Halt
pub fn codegen_delete(
    b: &mut ProgramBuilder,
    stmt: &DeleteStatement,
    schema: &[TableSchema],
    _ctx: &CodegenContext,
) -> Result<(), CodegenError> {
    let table_name = table_name_from_qualified(&stmt.table);
    let table = find_table(schema, table_name)?;
    let table_cursor = 0_i32;

    let end_label = b.emit_label();
    let done_label = b.emit_label();

    if stmt
        .where_clause
        .as_ref()
        .is_some_and(contains_unsupported_in_expr)
    {
        return Err(CodegenError::Unsupported(unsupported_in_message()));
    }

    // Init.
    b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);

    // Transaction (write).
    b.emit_op(Opcode::Transaction, 0, 1, 0, P4::None, 0);

    // OpenWrite for table.
    b.emit_op(
        Opcode::OpenWrite,
        table_cursor,
        table.root_page,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // OpenWrite for each index (bd-34se: Phase 5I.4).
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    for (idx_offset, index) in table.indexes.iter().enumerate() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        b.emit_op(
            Opcode::OpenWrite,
            idx_cursor,
            index.root_page,
            0,
            P4::Table(index.name.clone()),
            0,
        );
    }

    // Reverse scan (Last/Prev) so that delete_at(pos) does not shift
    // indices of rows we haven't visited yet.
    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Last, table_cursor, 0, done_label, P4::None, 0);

    // Evaluate WHERE condition (if any) and skip non-matching rows.
    let skip_label = b.emit_label();
    if let Some(where_expr) = &stmt.where_clause {
        emit_where_filter(
            b,
            where_expr,
            table_cursor,
            table,
            stmt.table.alias.as_deref(),
            schema,
            skip_label,
        );
    }

    // RETURNING clause: read columns before deletion (row is still present).
    if !stmt.returning.is_empty() {
        let ret_count = result_column_count(&stmt.returning, table);
        let ret_regs = b.alloc_regs(ret_count);
        emit_column_reads(b, table_cursor, &stmt.returning, table, None, &[], ret_regs)?;
        b.emit_op(Opcode::ResultRow, ret_regs, ret_count, 0, P4::None, 0);
    }

    // Index maintenance: delete from each index before deleting the row (bd-34se).
    // Must read column values while the row is still present.
    emit_index_deletes(b, table, table_cursor);

    // Delete at cursor position.
    b.emit_op(
        Opcode::Delete,
        table_cursor,
        0,
        0,
        P4::Table(table.name.clone()),
        0,
    );

    // Skip label for WHERE-filtered rows.
    b.resolve_label(skip_label);

    // Prev: iterate backwards to avoid index-shift issues.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Prev, table_cursor, loop_body, 0, P4::None, 0);

    // Done: Close table cursor.
    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, table_cursor, 0, 0, P4::None, 0);

    // Close index cursors (bd-34se).
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    for idx_offset in 0..table.indexes.len() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        b.emit_op(Opcode::Close, idx_cursor, 0, 0, P4::None, 0);
    }

    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    // End label.
    b.resolve_label(end_label);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Convert a column's DEFAULT value text to an AST `Expr`.
///
/// Used by INSERT with explicit column lists to fill unmentioned columns
/// with their declared DEFAULT rather than NULL.
fn default_value_to_expr(col: &ColumnInfo) -> Expr {
    let span = fsqlite_ast::Span::ZERO;
    let Some(dv) = col.default_value.as_deref() else {
        return Expr::Literal(Literal::Null, span);
    };
    parse_default_expr(dv)
        .unwrap_or_else(|| Expr::Literal(Literal::String(dv.trim().to_owned()), span))
}

/// Emit a column's DEFAULT value into a register.
///
/// Parses the column's `default_value` SQL text and emits the appropriate
/// opcode. Falls back to `Null` when no default is specified.
fn emit_default_value(b: &mut ProgramBuilder, col: &ColumnInfo, reg: i32) {
    match col.default_value.as_deref() {
        None => {
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        }
        Some(dv) => {
            if let Some(expr) = parse_default_expr(dv) {
                emit_expr(b, &expr, reg, None);
            } else {
                b.emit_op(Opcode::String8, 0, reg, 0, P4::Str(dv.trim().to_owned()), 0);
            }
        }
    }
}

/// Parse column DEFAULT SQL text into an expression AST.
///
/// Returns `None` if parsing fails or if the text does not map to a single
/// expression-only `SELECT`.
fn parse_default_expr(default_sql: &str) -> Option<Expr> {
    let trimmed = default_sql.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut parser = SqlParser::from_sql(&format!("SELECT {trimmed}"));
    let (stmts, errs) = parser.parse_all();
    if !errs.is_empty() || stmts.len() != 1 {
        return None;
    }

    let stmt = stmts.into_iter().next()?;
    let Statement::Select(select_stmt) = stmt else {
        return None;
    };
    if !select_stmt.order_by.is_empty()
        || select_stmt.limit.is_some()
        || !select_stmt.body.compounds.is_empty()
    {
        return None;
    }

    let SelectCore::Select { columns, from, .. } = select_stmt.body.select else {
        return None;
    };
    if from.is_some() || columns.len() != 1 {
        return None;
    }

    match columns.into_iter().next()? {
        ResultColumn::Expr { expr, .. } => Some(expr),
        _ => None,
    }
}

/// Emit `IdxInsert` opcodes for all indexes on the table (bd-so1h: Phase 5I.3).
///
/// For each index, this reads the indexed column values from the provided
/// registers, appends the rowid, builds an index key record, and inserts it.
///
/// # Arguments
/// * `b` - Program builder
/// * `table` - Table schema (includes index definitions)
/// * `table_cursor` - Cursor ID for the table (index cursors are table_cursor + 1, +2, etc.)
/// * `col_regs` - Starting register containing column values in table schema order
/// * `rowid_reg` - Register containing the rowid
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_index_inserts(
    b: &mut ProgramBuilder,
    table: &TableSchema,
    table_cursor: i32,
    col_regs: i32,
    rowid_reg: i32,
) {
    for (idx_offset, index) in table.indexes.iter().enumerate() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        let n_idx_cols = index.columns.len();

        // Allocate registers for index key: (indexed_cols..., rowid).
        let idx_key_regs = b.alloc_regs((n_idx_cols + 1) as i32);

        // Copy indexed column values to key registers.
        for (key_pos, col_name) in index.columns.iter().enumerate() {
            // Find column position in table schema.
            if let Some(col_idx) = table.column_index(col_name) {
                let src_reg = col_regs + col_idx as i32;
                let dst_reg = idx_key_regs + key_pos as i32;
                b.emit_op(Opcode::Copy, src_reg, dst_reg, 0, P4::None, 0);
            }
        }

        // Append rowid as the final key component.
        let rowid_key_reg = idx_key_regs + n_idx_cols as i32;
        b.emit_op(Opcode::Copy, rowid_reg, rowid_key_reg, 0, P4::None, 0);

        // Build the index key record.
        let idx_rec_reg = b.alloc_reg();
        b.emit_op(
            Opcode::MakeRecord,
            idx_key_regs,
            (n_idx_cols + 1) as i32,
            idx_rec_reg,
            P4::None,
            0,
        );

        // Insert into the index.
        // For UNIQUE indexes, set P5=1 and P3=number of indexed columns
        // (excluding the trailing rowid) so the engine can enforce the
        // uniqueness constraint while allowing multiple NULLs.
        let (p3_unique, p5_unique) = if index.is_unique {
            (n_idx_cols as i32, 1)
        } else {
            (0, 0)
        };
        let p4_name = if index.is_unique {
            // Include table name for the error message.
            P4::Table(format!("{}.{}", table.name, index.columns.join(", ")))
        } else {
            P4::Table(index.name.clone())
        };
        b.emit_op(
            Opcode::IdxInsert,
            idx_cursor,
            idx_rec_reg,
            p3_unique,
            p4_name,
            p5_unique,
        );
    }
}

/// Emit `IdxDelete` opcodes for all indexes on the table (bd-34se: Phase 5I.4).
///
/// For each index, this reads the indexed column values from the cursor,
/// reads the rowid, and emits `IdxDelete` with `(p2, p3)` pointing at the key
/// register span. The VDBE engine seeks to that key before deleting.
/// MUST be called BEFORE the table row is deleted, while data is still accessible.
///
/// # Arguments
/// * `b` - Program builder
/// * `table` - Table schema (includes index definitions)
/// * `table_cursor` - Cursor ID for the table (index cursors are table_cursor + 1, +2, etc.)
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_index_deletes(b: &mut ProgramBuilder, table: &TableSchema, table_cursor: i32) {
    for (idx_offset, index) in table.indexes.iter().enumerate() {
        let idx_cursor = table_cursor + 1 + idx_offset as i32;
        let n_idx_cols = index.columns.len();

        // Allocate registers for index key: (indexed_cols..., rowid).
        let idx_key_regs = b.alloc_regs((n_idx_cols + 1) as i32);

        // Read indexed column values from the cursor.
        for (key_pos, col_name) in index.columns.iter().enumerate() {
            if let Some(col_idx) = table.column_index(col_name) {
                let dst_reg = idx_key_regs + key_pos as i32;
                // Check if this column is the INTEGER PRIMARY KEY (rowid alias).
                if table.columns.get(col_idx).is_some_and(|c| c.is_ipk) {
                    b.emit_op(Opcode::Rowid, table_cursor, dst_reg, 0, P4::None, 0);
                } else {
                    b.emit_op(
                        Opcode::Column,
                        table_cursor,
                        col_idx as i32,
                        dst_reg,
                        P4::None,
                        0,
                    );
                }
            }
        }

        // Read rowid and append as the final key component.
        let rowid_key_reg = idx_key_regs + n_idx_cols as i32;
        b.emit_op(Opcode::Rowid, table_cursor, rowid_key_reg, 0, P4::None, 0);

        // Delete from the index.
        b.emit_op(
            Opcode::IdxDelete,
            idx_cursor,
            idx_key_regs,
            (n_idx_cols + 1) as i32,
            P4::Table(index.name.clone()),
            0,
        );
    }
}

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
    table_alias: Option<&str>,
    schema: &[TableSchema],
    base_reg: i32,
) -> Result<(), CodegenError> {
    let mut reg = base_reg;
    for col in columns {
        match col {
            ResultColumn::Star => {
                for (i, ci) in table.columns.iter().enumerate() {
                    if ci.is_ipk {
                        b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                    } else {
                        b.emit_op(Opcode::Column, cursor, i as i32, reg, P4::None, 0);
                    }
                    reg += 1;
                }
            }
            ResultColumn::TableStar(qualifier) => {
                if !matches_table_or_alias(qualifier, table, table_alias) {
                    return Err(CodegenError::TableNotFound(qualifier.clone()));
                }
                for (i, ci) in table.columns.iter().enumerate() {
                    if ci.is_ipk {
                        b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                    } else {
                        b.emit_op(Opcode::Column, cursor, i as i32, reg, P4::None, 0);
                    }
                    reg += 1;
                }
            }
            ResultColumn::Expr { expr, .. } => {
                if let Expr::Column(col_ref, _) = expr {
                    if let Some(qualifier) = &col_ref.table {
                        if !matches_table_or_alias(qualifier, table, table_alias) {
                            return Err(CodegenError::TableNotFound(qualifier.clone()));
                        }
                    }
                    if is_rowid_alias(&col_ref.column) {
                        b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                    } else {
                        let col_idx = table.column_index(&col_ref.column).ok_or_else(|| {
                            CodegenError::ColumnNotFound {
                                table: table.name.clone(),
                                column: col_ref.column.clone(),
                            }
                        })?;
                        if table.columns[col_idx].is_ipk {
                            b.emit_op(Opcode::Rowid, cursor, reg, 0, P4::None, 0);
                        } else {
                            b.emit_op(Opcode::Column, cursor, col_idx as i32, reg, P4::None, 0);
                        }
                    }
                } else {
                    // Evaluate non-column expressions (literals, arithmetic, CASE, CAST, etc.)
                    // against the current scan row.
                    let scan = ScanCtx {
                        cursor,
                        table,
                        table_alias,
                        schema: Some(schema),
                    };
                    emit_expr(b, expr, reg, Some(&scan));
                }
                reg += 1;
            }
        }
    }
    Ok(())
}

/// Emit RETURNING clause opcodes after an INSERT.
///
/// Positions the cursor on the just-inserted row via `SeekRowid`, reads the
/// requested columns, and emits a `ResultRow`.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_returning(
    b: &mut ProgramBuilder,
    cursor: i32,
    table: &TableSchema,
    returning: &[ResultColumn],
    rowid_reg: i32,
) -> Result<(), CodegenError> {
    let skip_returning = b.emit_label();
    b.emit_jump_to_label(
        Opcode::SeekRowid,
        cursor,
        rowid_reg,
        skip_returning,
        P4::None,
        0,
    );
    let ret_count = result_column_count(returning, table);
    let ret_regs = b.alloc_regs(ret_count);
    emit_column_reads(b, cursor, returning, table, None, &[], ret_regs)?;
    b.emit_op(Opcode::ResultRow, ret_regs, ret_count, 0, P4::None, 0);
    b.resolve_label(skip_returning);
    Ok(())
}

/// Emit a HAVING filter for GROUP BY queries.
///
/// Evaluates the HAVING expression against the already-built output row.
/// Aggregate function calls and column references are resolved to the
/// corresponding output registers. If the predicate is false, jumps to
/// `skip_label` (skipping the `ResultRow`).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_arguments
)]
fn emit_having_filter(
    b: &mut ProgramBuilder,
    having_expr: &Expr,
    output_cols: &[GroupByOutputCol],
    agg_columns: &[AggColumn],
    group_by_keys: &[GroupByKey],
    table: &TableSchema,
    out_regs: i32,
    skip_label: crate::Label,
) {
    let result_reg = b.alloc_temp();
    emit_having_expr(
        b,
        having_expr,
        result_reg,
        output_cols,
        agg_columns,
        group_by_keys,
        table,
        out_regs,
    );
    // If result is falsy (0 or NULL), skip this group's ResultRow.
    // p3=1: NULL HAVING → jump (skip row), matching SQLite semantics.
    b.emit_jump_to_label(Opcode::IfNot, result_reg, 1, skip_label, P4::None, 0);
    b.free_temp(result_reg);
}

/// Evaluate a HAVING expression into `dest_reg`.
///
/// Maps aggregate function calls and column references to the output
/// registers that already hold the finalized group results.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
fn emit_having_expr(
    b: &mut ProgramBuilder,
    expr: &Expr,
    dest_reg: i32,
    output_cols: &[GroupByOutputCol],
    agg_columns: &[AggColumn],
    group_by_keys: &[GroupByKey],
    table: &TableSchema,
    out_regs: i32,
) {
    match expr {
        // Aggregate function call — resolve to the corresponding output register.
        Expr::FunctionCall { name, args, .. } if is_aggregate_function(name) => {
            let lower = name.to_ascii_lowercase();
            // Find the matching aggregate by name + argument structure.
            let agg_idx = agg_columns.iter().position(|agg| {
                if agg.name != lower {
                    return false;
                }
                match args {
                    FunctionArgs::Star => agg.num_args == 0,
                    FunctionArgs::List(exprs) => {
                        if exprs.is_empty() {
                            return agg.num_args == 0;
                        }
                        // Match by argument column index.
                        if let Some(ci) = resolve_column_index(&exprs[0], table) {
                            agg.arg_col_index == Some(ci)
                        } else {
                            false
                        }
                    }
                }
            });
            if let Some(ai) = agg_idx {
                // Find the output register for this aggregate.
                for (i, oc) in output_cols.iter().enumerate() {
                    if matches!(oc, GroupByOutputCol::Aggregate { agg_index } if *agg_index == ai) {
                        b.emit_op(Opcode::Copy, out_regs + i as i32, dest_reg, 0, P4::None, 0);
                        return;
                    }
                }
            }
            // Fallback: treat as zero (unknown aggregate in HAVING).
            b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
        }

        // Column reference — resolve to the corresponding group-key output register.
        Expr::Column(col_ref, _) => {
            let col_name = &col_ref.column;
            if let Some(col_idx) = table.columns.iter().position(|c| c.name == *col_name) {
                // Find the output column whose group key maps to this table column.
                for (i, oc) in output_cols.iter().enumerate() {
                    if let GroupByOutputCol::GroupKey { key_index, .. } = oc {
                        if matches!(group_by_keys.get(*key_index), Some(GroupByKey::Column(c)) if *c == col_idx)
                        {
                            b.emit_op(Opcode::Copy, out_regs + i as i32, dest_reg, 0, P4::None, 0);
                            return;
                        }
                    }
                }
            }
            // Fallback: emit NULL for unresolved column.
            b.emit_op(Opcode::Null, 0, dest_reg, 0, P4::None, 0);
        }

        // Binary comparison — evaluate both sides, then compare.
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            let left_reg = b.alloc_temp();
            let right_reg = b.alloc_temp();
            emit_having_expr(
                b,
                left,
                left_reg,
                output_cols,
                agg_columns,
                group_by_keys,
                table,
                out_regs,
            );
            emit_having_expr(
                b,
                right,
                right_reg,
                output_cols,
                agg_columns,
                group_by_keys,
                table,
                out_regs,
            );

            match op {
                fsqlite_ast::BinaryOp::Gt => {
                    let true_label = b.emit_label();
                    let done_label = b.emit_label();
                    b.emit_jump_to_label(Opcode::Gt, right_reg, left_reg, true_label, P4::None, 0);
                    b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(true_label);
                    b.emit_op(Opcode::Integer, 1, dest_reg, 0, P4::None, 0);
                    b.resolve_label(done_label);
                }
                fsqlite_ast::BinaryOp::Lt => {
                    let true_label = b.emit_label();
                    let done_label = b.emit_label();
                    b.emit_jump_to_label(Opcode::Lt, right_reg, left_reg, true_label, P4::None, 0);
                    b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(true_label);
                    b.emit_op(Opcode::Integer, 1, dest_reg, 0, P4::None, 0);
                    b.resolve_label(done_label);
                }
                fsqlite_ast::BinaryOp::Ge => {
                    let true_label = b.emit_label();
                    let done_label = b.emit_label();
                    b.emit_jump_to_label(Opcode::Ge, right_reg, left_reg, true_label, P4::None, 0);
                    b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(true_label);
                    b.emit_op(Opcode::Integer, 1, dest_reg, 0, P4::None, 0);
                    b.resolve_label(done_label);
                }
                fsqlite_ast::BinaryOp::Le => {
                    let true_label = b.emit_label();
                    let done_label = b.emit_label();
                    b.emit_jump_to_label(Opcode::Le, right_reg, left_reg, true_label, P4::None, 0);
                    b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(true_label);
                    b.emit_op(Opcode::Integer, 1, dest_reg, 0, P4::None, 0);
                    b.resolve_label(done_label);
                }
                fsqlite_ast::BinaryOp::Eq => {
                    let true_label = b.emit_label();
                    let done_label = b.emit_label();
                    b.emit_jump_to_label(Opcode::Eq, right_reg, left_reg, true_label, P4::None, 0);
                    b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(true_label);
                    b.emit_op(Opcode::Integer, 1, dest_reg, 0, P4::None, 0);
                    b.resolve_label(done_label);
                }
                fsqlite_ast::BinaryOp::Ne => {
                    let true_label = b.emit_label();
                    let done_label = b.emit_label();
                    b.emit_jump_to_label(Opcode::Ne, right_reg, left_reg, true_label, P4::None, 0);
                    b.emit_op(Opcode::Integer, 0, dest_reg, 0, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                    b.resolve_label(true_label);
                    b.emit_op(Opcode::Integer, 1, dest_reg, 0, P4::None, 0);
                    b.resolve_label(done_label);
                }
                fsqlite_ast::BinaryOp::And => {
                    b.emit_op(Opcode::And, left_reg, right_reg, dest_reg, P4::None, 0);
                }
                fsqlite_ast::BinaryOp::Or => {
                    b.emit_op(Opcode::Or, left_reg, right_reg, dest_reg, P4::None, 0);
                }
                _ => {
                    emit_expr(b, expr, dest_reg, None);
                }
            }
            b.free_temp(right_reg);
            b.free_temp(left_reg);
        }

        // For any other expression, delegate to the standard evaluator.
        _ => {
            emit_expr(b, expr, dest_reg, None);
        }
    }
}

/// Emit a WHERE filter for scan-based UPDATE/DELETE.
///
/// Evaluates the WHERE expression against the current cursor row. If the
/// condition is false, jumps to `skip_label` (skipping the DML operation).
///
/// Handles `col = expr` comparisons by reading the column from the cursor
/// and comparing with the literal/expression value.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_where_filter(
    b: &mut ProgramBuilder,
    where_expr: &Expr,
    cursor: i32,
    table: &TableSchema,
    table_alias: Option<&str>,
    schema: &[TableSchema],
    skip_label: crate::Label,
) {
    match where_expr {
        Expr::BinaryOp {
            left,
            op: fsqlite_ast::BinaryOp::Eq,
            right,
            ..
        } => {
            // Try col = expr or expr = col (with alias-aware qualifier validation).
            let scan = ScanCtx {
                cursor,
                table,
                table_alias,
                schema: Some(schema),
            };
            // Check for COLLATE on either operand.
            let collation_p4 = extract_collation(left)
                .or_else(|| extract_collation(right))
                .map_or(P4::None, |coll| P4::Collation(coll.to_owned()));

            if let Some(resolved) = resolve_column_ref(left, table, table_alias) {
                let col_reg = b.alloc_temp();
                let val_reg = b.alloc_temp();
                match resolved {
                    SortKeySource::Column(idx) => {
                        b.emit_op(Opcode::Column, cursor, idx as i32, col_reg, P4::None, 0);
                    }
                    SortKeySource::Rowid => {
                        b.emit_op(Opcode::Rowid, cursor, col_reg, 0, P4::None, 0);
                    }
                    SortKeySource::Expression(expr) => {
                        emit_expr(b, &expr, col_reg, Some(&scan));
                    }
                }
                emit_expr(b, right, val_reg, Some(&scan));
                // SQL semantics: `col = NULL` is UNKNOWN (false in WHERE). If the
                // value expression evaluates to NULL, skip the row unconditionally.
                b.emit_jump_to_label(Opcode::IsNull, val_reg, 0, skip_label, P4::None, 0);
                // Use NULLEQ flag (0x80) so NULL column != non-NULL value → skip.
                b.emit_jump_to_label(Opcode::Ne, val_reg, col_reg, skip_label, collation_p4, 0x80);
                b.free_temp(val_reg);
                b.free_temp(col_reg);
            } else if let Some(resolved) = resolve_column_ref(right, table, table_alias) {
                let col_reg = b.alloc_temp();
                let val_reg = b.alloc_temp();
                match resolved {
                    SortKeySource::Column(idx) => {
                        b.emit_op(Opcode::Column, cursor, idx as i32, col_reg, P4::None, 0);
                    }
                    SortKeySource::Rowid => {
                        b.emit_op(Opcode::Rowid, cursor, col_reg, 0, P4::None, 0);
                    }
                    SortKeySource::Expression(expr) => {
                        emit_expr(b, &expr, col_reg, Some(&scan));
                    }
                }
                emit_expr(b, left, val_reg, Some(&scan));
                // SQL semantics: `NULL = col` is UNKNOWN (false in WHERE).
                b.emit_jump_to_label(Opcode::IsNull, val_reg, 0, skip_label, P4::None, 0);
                b.emit_jump_to_label(Opcode::Ne, val_reg, col_reg, skip_label, collation_p4, 0x80);
                b.free_temp(val_reg);
                b.free_temp(col_reg);
            } else {
                // Neither side is a column ref (e.g. WHERE 1 = 0, WHERE length(name) = 5).
                // Fall through to generic boolean evaluation.
                let cond_reg = b.alloc_temp();
                emit_expr(b, where_expr, cond_reg, Some(&scan));
                b.emit_jump_to_label(Opcode::IfNot, cond_reg, 1, skip_label, P4::None, 0);
                b.free_temp(cond_reg);
            }
        }
        Expr::BinaryOp {
            left,
            op: fsqlite_ast::BinaryOp::And,
            right,
            ..
        } => {
            // AND: both conditions must pass.
            emit_where_filter(b, left, cursor, table, table_alias, schema, skip_label);
            emit_where_filter(b, right, cursor, table, table_alias, schema, skip_label);
        }
        Expr::BinaryOp {
            left,
            op: fsqlite_ast::BinaryOp::Or,
            right,
            ..
        } => {
            // OR: at least one condition must pass.
            // If left passes → skip right, proceed to row processing.
            // If left fails → try right; if right also fails → skip row.
            let left_skip = b.emit_label();
            let pass_label = b.emit_label();
            emit_where_filter(b, left, cursor, table, table_alias, schema, left_skip);
            // Left passed — jump past right-side evaluation.
            b.emit_jump_to_label(Opcode::Goto, 0, 0, pass_label, P4::None, 0);
            b.resolve_label(left_skip);
            // Left failed — try right.
            emit_where_filter(b, right, cursor, table, table_alias, schema, skip_label);
            b.resolve_label(pass_label);
        }
        _ => {
            // Generic WHERE: evaluate expression with cursor context and test truthiness.
            let scan = ScanCtx {
                cursor,
                table,
                table_alias,
                schema: Some(schema),
            };
            let cond_reg = b.alloc_temp();
            emit_expr(b, where_expr, cond_reg, Some(&scan));
            b.emit_jump_to_label(Opcode::IfNot, cond_reg, 1, skip_label, P4::None, 0);
            b.free_temp(cond_reg);
        }
    }
}

/// Check whether a column name is a rowid alias (`rowid`, `_rowid_`, or `oid`).
fn is_rowid_alias(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

fn matches_table_or_alias(qualifier: &str, table: &TableSchema, table_alias: Option<&str>) -> bool {
    qualifier.eq_ignore_ascii_case(&table.name)
        || table_alias.is_some_and(|alias| qualifier.eq_ignore_ascii_case(alias))
}

/// Source for a sort key: either a table column or the implicit rowid.
enum SortKeySource {
    Column(usize),
    Rowid,
    /// Arbitrary expression (e.g., `a + b`, `LENGTH(name)`, `CASE WHEN ...`).
    Expression(Expr),
}

/// Output source for a covering-index ordered scan.
enum CoveringOutputSource {
    /// Read value from index key column at this position.
    IndexColumn(i32),
    /// Read value from the rowid already extracted via `IdxRowid`.
    Rowid,
}

/// Plan for ORDER BY execution that can bypass the sorter.
struct OrderByIndexPlan {
    index_name: String,
    index_root_page: i32,
    descending: bool,
    /// When present, all output columns can be read from index payload/rowid
    /// and no table row lookup is required.
    covering_output: Option<Vec<CoveringOutputSource>>,
}

/// Resolve an ORDER BY expression to a `SortKeySource`.
///
/// Returns `Column` or `Rowid` for simple column references; falls back to
/// `Expression` for arbitrary expressions (arithmetic, function calls, etc.).
fn resolve_sort_key(expr: &Expr, table: &TableSchema, table_alias: Option<&str>) -> SortKeySource {
    if let Expr::Column(col_ref, _) = expr {
        if let Some(qualifier) = &col_ref.table {
            if !matches_table_or_alias(qualifier, table, table_alias) {
                return SortKeySource::Expression(expr.clone());
            }
        }
        if let Some(idx) = table.column_index(&col_ref.column) {
            return SortKeySource::Column(idx);
        }
        if is_rowid_alias(&col_ref.column) {
            return SortKeySource::Rowid;
        }
    }
    SortKeySource::Expression(expr.clone())
}

/// Resolve a column reference expression to either a column index or rowid.
///
/// Validates that any table qualifier matches the table name or alias.
/// Returns `None` if the expression is not a column reference or the
/// qualifier does not match.
fn resolve_column_ref(
    expr: &Expr,
    table: &TableSchema,
    table_alias: Option<&str>,
) -> Option<SortKeySource> {
    // Unwrap COLLATE wrapper to reach the inner column reference.
    let inner = if let Expr::Collate { expr: inner, .. } = expr {
        inner.as_ref()
    } else {
        expr
    };
    if let Expr::Column(col_ref, _) = inner {
        if let Some(qualifier) = &col_ref.table
            && !matches_table_or_alias(qualifier, table, table_alias)
        {
            return None;
        }
        if let Some(idx) = table.column_index(&col_ref.column) {
            // INTEGER PRIMARY KEY columns are stored as rowid, not in the record payload.
            // Return Rowid so callers emit the Rowid opcode instead of Column.
            if table.columns[idx].is_ipk {
                return Some(SortKeySource::Rowid);
            }
            return Some(SortKeySource::Column(idx));
        }
        if is_rowid_alias(&col_ref.column) {
            return Some(SortKeySource::Rowid);
        }
    }
    None
}

/// Resolve a column reference to its 0-based index (ignoring rowid aliases).
///
/// Convenience wrapper for call sites that only care about real table columns.
fn resolve_column_index(expr: &Expr, table: &TableSchema) -> Option<usize> {
    match resolve_column_ref(expr, table, None) {
        Some(SortKeySource::Column(idx)) => Some(idx),
        _ => None,
    }
}

fn index_column_position(index: &IndexSchema, column_name: &str) -> Option<usize> {
    index
        .columns
        .iter()
        .position(|name| name.eq_ignore_ascii_case(column_name))
}

fn resolve_covering_output_sources(
    columns: &[ResultColumn],
    table: &TableSchema,
    table_alias: Option<&str>,
    index: &IndexSchema,
) -> Option<Vec<CoveringOutputSource>> {
    let mut output = Vec::with_capacity(columns.len());

    for col in columns {
        match col {
            ResultColumn::Expr { expr, .. } => {
                match resolve_column_ref(expr, table, table_alias)? {
                    SortKeySource::Column(col_idx) => {
                        let column_name = &table.columns.get(col_idx)?.name;
                        let index_pos = index_column_position(index, column_name)?;
                        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                        output.push(CoveringOutputSource::IndexColumn(index_pos as i32));
                    }
                    SortKeySource::Rowid => output.push(CoveringOutputSource::Rowid),
                    SortKeySource::Expression(_) => return None,
                }
            }
            ResultColumn::Star | ResultColumn::TableStar(_) => return None,
        }
    }

    Some(output)
}

fn resolve_order_by_index_plan(
    table: &TableSchema,
    table_alias: Option<&str>,
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    order_by: &[OrderingTerm],
    distinct: Distinctness,
) -> Option<OrderByIndexPlan> {
    if order_by.is_empty() || distinct == Distinctness::Distinct {
        return None;
    }

    let mut direction: Option<SortDirection> = None;
    let mut order_columns = Vec::with_capacity(order_by.len());

    for term in order_by {
        if term.nulls.is_some() {
            return None;
        }
        let term_direction = term.direction.unwrap_or(SortDirection::Asc);
        if let Some(existing) = direction {
            if existing != term_direction {
                return None;
            }
        } else {
            direction = Some(term_direction);
        }

        let Expr::Column(col_ref, _) = &term.expr else {
            return None;
        };
        if let Some(qualifier) = &col_ref.table
            && !matches_table_or_alias(qualifier, table, table_alias)
        {
            return None;
        }
        if is_rowid_alias(&col_ref.column) {
            return None;
        }
        order_columns.push(col_ref.column.clone());
    }

    let index = table.indexes.iter().find(|idx| {
        idx.columns.len() >= order_columns.len()
            && order_columns
                .iter()
                .zip(&idx.columns)
                .all(|(order_col, idx_col)| order_col.eq_ignore_ascii_case(idx_col))
    })?;

    let covering_output = if where_clause.is_none() {
        resolve_covering_output_sources(columns, table, table_alias, index)
    } else {
        None
    };

    Some(OrderByIndexPlan {
        index_name: index.name.clone(),
        index_root_page: index.root_page,
        descending: direction == Some(SortDirection::Desc),
        covering_output,
    })
}

/// Resolve result columns to table column indices.
///
/// Returns a Vec of column indices for each output column.
/// `Star` and `TableStar` expand to all table columns.
///
/// NOTE: Currently unused — `emit_column_reads` handles non-column result
/// expressions directly. Kept for potential future index-only scan codegen.
#[allow(
    dead_code,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn resolve_result_column_indices(
    columns: &[ResultColumn],
    table: &TableSchema,
) -> Result<Vec<usize>, CodegenError> {
    let mut indices = Vec::new();
    for col in columns {
        match col {
            ResultColumn::Star => {
                indices.extend(0..table.columns.len());
            }
            ResultColumn::TableStar(qualifier) => {
                if !qualifier.eq_ignore_ascii_case(&table.name) {
                    return Err(CodegenError::TableNotFound(qualifier.clone()));
                }
                indices.extend(0..table.columns.len());
            }
            ResultColumn::Expr { expr, .. } => {
                if let Expr::Column(col_ref, _) = expr {
                    let idx = table.column_index(&col_ref.column).ok_or_else(|| {
                        CodegenError::ColumnNotFound {
                            table: table.name.clone(),
                            column: col_ref.column.clone(),
                        }
                    })?;
                    indices.push(idx);
                } else {
                    return Err(CodegenError::Unsupported(
                        "non-column result expression in table-backed SELECT".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(indices)
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
/// Cursor context for expression emission inside table scans.
///
/// When present, allows `emit_expr` to resolve `Expr::Column` references
/// by emitting `Opcode::Column` against the given cursor.
struct ScanCtx<'a> {
    cursor: i32,
    table: &'a TableSchema,
    table_alias: Option<&'a str>,
    schema: Option<&'a [TableSchema]>,
}

enum InProbeValue<'a> {
    Expr(&'a Expr),
    FirstColumn,
}

struct InProbeSource<'a> {
    table: &'a TableSchema,
    table_alias: Option<&'a str>,
    where_clause: Option<&'a Expr>,
    value: InProbeValue<'a>,
}

fn resolve_in_probe_source<'a>(
    set: &'a fsqlite_ast::InSet,
    schema: &'a [TableSchema],
) -> Option<InProbeSource<'a>> {
    match set {
        fsqlite_ast::InSet::List(_) => None,
        fsqlite_ast::InSet::Table(name) => {
            let table = find_table(schema, &name.name).ok()?;
            if table.columns.is_empty() {
                return None;
            }
            Some(InProbeSource {
                table,
                table_alias: None,
                where_clause: None,
                value: InProbeValue::FirstColumn,
            })
        }
        fsqlite_ast::InSet::Subquery(subquery) => {
            if subquery.with.is_some()
                || !subquery.body.compounds.is_empty()
                || !subquery.order_by.is_empty()
                || subquery.limit.is_some()
            {
                return None;
            }

            let fsqlite_ast::SelectCore::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                windows,
                ..
            } = &subquery.body.select
            else {
                return None;
            };

            if !group_by.is_empty() || having.is_some() || !windows.is_empty() {
                return None;
            }

            let from_clause = from.as_ref()?;
            if !from_clause.joins.is_empty() {
                return None;
            }

            let (table_name, table_alias) = match &from_clause.source {
                fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => {
                    (&name.name, alias.as_deref())
                }
                _ => return None,
            };
            let table = find_table(schema, table_name).ok()?;

            let value = match columns.as_slice() {
                [fsqlite_ast::ResultColumn::Expr { expr, .. }] => InProbeValue::Expr(expr),
                [fsqlite_ast::ResultColumn::Star | fsqlite_ast::ResultColumn::TableStar(_)] => {
                    if table.columns.is_empty() {
                        return None;
                    }
                    InProbeValue::FirstColumn
                }
                _ => return None,
            };

            Some(InProbeSource {
                table,
                table_alias,
                where_clause: where_clause.as_deref(),
                value,
            })
        }
    }
}

/// Attempt to emit bytecode for a complex IN subquery with ORDER BY and/or LIMIT.
///
/// Returns `true` if the subquery was handled, `false` if it cannot be handled
/// (caller should fall back to emitting Null or other behavior).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines
)]
fn try_emit_complex_in_subquery(
    b: &mut ProgramBuilder,
    operand: &Expr,
    subquery: &SelectStatement,
    not: bool,
    reg: i32,
    scan_ctx: &ScanCtx<'_>,
) -> bool {
    let Some(schema) = scan_ctx.schema else {
        return false;
    };

    // Reject WITH and compound queries.
    if subquery.with.is_some() || !subquery.body.compounds.is_empty() {
        return false;
    }

    let fsqlite_ast::SelectCore::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        windows,
        ..
    } = &subquery.body.select
    else {
        return false;
    };

    // Reject GROUP BY, HAVING, windows.
    if !group_by.is_empty() || having.is_some() || !windows.is_empty() {
        return false;
    }

    let Some(from_clause) = from.as_ref() else {
        return false;
    };

    // Reject JOINs.
    if !from_clause.joins.is_empty() {
        return false;
    }

    let (table_name, table_alias) = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => (&name.name, alias.as_deref()),
        _ => return false,
    };

    let Ok(table) = find_table(schema, table_name) else {
        return false;
    };

    // Determine the value expression to compare.
    let value_expr: Option<&Expr> = match columns.as_slice() {
        [fsqlite_ast::ResultColumn::Expr { expr, .. }] => Some(expr),
        [fsqlite_ast::ResultColumn::Star | fsqlite_ast::ResultColumn::TableStar(_)] => {
            if table.columns.is_empty() {
                return false;
            }
            None // Use first column
        }
        _ => return false,
    };

    // --- Begin bytecode emission ---

    // Use cursors well above the main scan cursor to avoid collisions.
    let subq_cursor = scan_ctx.cursor + 128;
    let sorter_cursor = scan_ctx.cursor + 129;

    // Evaluate the operand (value we're checking for membership).
    let r_operand = b.alloc_temp();
    emit_expr(b, operand, r_operand, Some(scan_ctx));

    // Labels for control flow.
    let no_match_label = b.emit_label();
    let matched_label = b.emit_label();
    let done_label = b.emit_label();

    // Check if we have ORDER BY and/or LIMIT.
    let has_order_by = !subquery.order_by.is_empty();
    let has_limit = subquery.limit.is_some();

    if has_order_by || has_limit {
        // Materialize subquery results into a sorter, then probe.

        // Build sort order string.
        let sort_order: String = subquery
            .order_by
            .iter()
            .map(|term| {
                if term.direction == Some(SortDirection::Desc) {
                    '-'
                } else {
                    '+'
                }
            })
            .collect();

        // Number of sort key columns. If no ORDER BY, still need 1 column for the value.
        let num_sort_keys = if has_order_by {
            subquery.order_by.len()
        } else {
            0
        };
        // Sorter holds: sort keys + value column.
        let num_sorter_cols = num_sort_keys + 1;

        // Open sorter.
        b.emit_op(
            Opcode::SorterOpen,
            sorter_cursor,
            num_sort_keys.max(1) as i32,
            0,
            P4::Str(if sort_order.is_empty() {
                "+".to_owned()
            } else {
                sort_order
            }),
            0,
        );

        // Open subquery table for reading.
        b.emit_op(
            Opcode::OpenRead,
            subq_cursor,
            table.root_page,
            0,
            P4::Table(table.name.clone()),
            0,
        );

        // === Pass 1: Scan subquery rows into sorter ===
        let scan_start = b.current_addr();
        let scan_done = b.emit_label();
        b.emit_jump_to_label(Opcode::Rewind, subq_cursor, 0, scan_done, P4::None, 0);

        // WHERE filter.
        let skip_row = b.emit_label();
        if let Some(where_expr) = where_clause.as_deref() {
            emit_where_filter(
                b,
                where_expr,
                subq_cursor,
                table,
                table_alias,
                schema,
                skip_row,
            );
        }

        // Allocate registers for sorter record.
        let sorter_base = b.alloc_regs(num_sorter_cols as i32);
        let subq_scan = ScanCtx {
            cursor: subq_cursor,
            table,
            table_alias,
            schema: Some(schema),
        };

        // Emit sort key columns.
        for (i, term) in subquery.order_by.iter().enumerate() {
            let key_source = resolve_sort_key(&term.expr, table, table_alias);
            match key_source {
                SortKeySource::Column(col_idx) => {
                    b.emit_op(
                        Opcode::Column,
                        subq_cursor,
                        col_idx as i32,
                        sorter_base + i as i32,
                        P4::None,
                        0,
                    );
                }
                SortKeySource::Rowid => {
                    b.emit_op(
                        Opcode::Rowid,
                        subq_cursor,
                        sorter_base + i as i32,
                        0,
                        P4::None,
                        0,
                    );
                }
                SortKeySource::Expression(expr) => {
                    emit_expr(b, &expr, sorter_base + i as i32, Some(&subq_scan));
                }
            }
        }

        // Emit value column (last column in sorter record).
        let value_reg = sorter_base + num_sort_keys as i32;
        match value_expr {
            Some(expr) => emit_expr(b, expr, value_reg, Some(&subq_scan)),
            None => {
                // First column.
                b.emit_op(Opcode::Column, subq_cursor, 0, value_reg, P4::None, 0);
            }
        }

        // MakeRecord + SorterInsert.
        let record_reg = b.alloc_temp();
        b.emit_op(
            Opcode::MakeRecord,
            sorter_base,
            num_sorter_cols as i32,
            record_reg,
            P4::None,
            0,
        );
        b.emit_op(
            Opcode::SorterInsert,
            sorter_cursor,
            record_reg,
            0,
            P4::None,
            0,
        );
        b.free_temp(record_reg);

        // Skip label (for WHERE-filtered rows).
        b.resolve_label(skip_row);

        // Next row.
        let scan_body = (scan_start + 1) as i32;
        b.emit_op(Opcode::Next, subq_cursor, scan_body, 0, P4::None, 0);

        // End of pass 1.
        b.resolve_label(scan_done);
        b.emit_op(Opcode::Close, subq_cursor, 0, 0, P4::None, 0);

        // === Pass 2: Sort and probe ===

        // Initialize LIMIT counter if needed.
        let limit_reg = subquery.limit.as_ref().map(|lc| {
            let r = b.alloc_reg();
            emit_limit_expr(b, &lc.limit, r);
            r
        });

        // SorterSort: sort and position at first row; jump to no_match if empty.
        b.emit_jump_to_label(
            Opcode::SorterSort,
            sorter_cursor,
            0,
            no_match_label,
            P4::None,
            0,
        );

        // Probe loop.
        let probe_loop = b.current_addr();

        // SorterData to extract current row.
        let sorted_reg = b.alloc_temp();
        b.emit_op(
            Opcode::SorterData,
            sorter_cursor,
            sorted_reg,
            0,
            P4::None,
            0,
        );

        // Extract the value column (last column).
        let r_probe = b.alloc_temp();
        b.emit_op(
            Opcode::Column,
            sorter_cursor,
            num_sort_keys as i32,
            r_probe,
            P4::None,
            0,
        );

        // Compare with operand.
        b.emit_jump_to_label(Opcode::Eq, r_probe, r_operand, matched_label, P4::None, 0);

        b.free_temp(r_probe);
        b.free_temp(sorted_reg);

        // LIMIT: decrement counter and stop when zero.
        let continue_label = b.emit_label();
        if let Some(lim_r) = limit_reg {
            // DecrJumpZero: if limit counter reaches zero, jump to no_match.
            b.emit_jump_to_label(Opcode::DecrJumpZero, lim_r, 0, no_match_label, P4::None, 0);
        }
        b.resolve_label(continue_label);

        // SorterNext.
        b.emit_op(
            Opcode::SorterNext,
            sorter_cursor,
            probe_loop as i32,
            0,
            P4::None,
            0,
        );
    } else {
        // No ORDER BY, no LIMIT: simple scan probe (should have been handled
        // by resolve_in_probe_source, but handle here as fallback).
        b.emit_op(
            Opcode::OpenRead,
            subq_cursor,
            table.root_page,
            0,
            P4::Table(table.name.clone()),
            0,
        );

        let loop_start = b.current_addr();
        b.emit_jump_to_label(Opcode::Rewind, subq_cursor, 0, no_match_label, P4::None, 0);

        // WHERE filter.
        let skip_label = b.emit_label();
        if let Some(where_expr) = where_clause.as_deref() {
            emit_where_filter(
                b,
                where_expr,
                subq_cursor,
                table,
                table_alias,
                schema,
                skip_label,
            );
        }

        let subq_scan = ScanCtx {
            cursor: subq_cursor,
            table,
            table_alias,
            schema: Some(schema),
        };

        let r_probe = b.alloc_temp();
        match value_expr {
            Some(expr) => emit_expr(b, expr, r_probe, Some(&subq_scan)),
            None => {
                b.emit_op(Opcode::Column, subq_cursor, 0, r_probe, P4::None, 0);
            }
        }
        b.emit_jump_to_label(Opcode::Eq, r_probe, r_operand, matched_label, P4::None, 0);

        b.resolve_label(skip_label);
        let loop_body = (loop_start + 1) as i32;
        b.emit_op(Opcode::Next, subq_cursor, loop_body, 0, P4::None, 0);

        b.free_temp(r_probe);
    }

    // Fall through to no_match (common to both paths).
    b.emit_jump_to_label(Opcode::Goto, 0, 0, no_match_label, P4::None, 0);

    // --- Result emission (shared by both paths) ---

    b.resolve_label(no_match_label);
    b.emit_op(Opcode::Integer, i32::from(not), reg, 0, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

    b.resolve_label(matched_label);
    b.emit_op(Opcode::Integer, i32::from(!not), reg, 0, P4::None, 0);

    b.resolve_label(done_label);
    // Close cursors.
    if has_order_by || has_limit {
        b.emit_op(Opcode::Close, sorter_cursor, 0, 0, P4::None, 0);
    } else {
        b.emit_op(Opcode::Close, subq_cursor, 0, 0, P4::None, 0);
    }

    b.free_temp(r_operand);

    true
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_in_probe_expr(
    b: &mut ProgramBuilder,
    operand: &Expr,
    set: &fsqlite_ast::InSet,
    not: bool,
    reg: i32,
    ctx: Option<&ScanCtx<'_>>,
) {
    let Some(scan_ctx) = ctx else {
        b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        return;
    };
    let Some(schema) = scan_ctx.schema else {
        b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        return;
    };
    let Some(probe_source) = resolve_in_probe_source(set, schema) else {
        // Try to handle complex subqueries with ORDER BY/LIMIT.
        if let fsqlite_ast::InSet::Subquery(subquery) = set {
            if try_emit_complex_in_subquery(b, operand, subquery, not, reg, scan_ctx) {
                return;
            }
        }
        b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        return;
    };

    // Keep probe cursors far from primary scan/sorter cursors used by main paths.
    let probe_cursor = scan_ctx.cursor + 64;
    let r_operand = b.alloc_temp();
    let r_probe = b.alloc_temp();

    emit_expr(b, operand, r_operand, Some(scan_ctx));

    let null_label = b.emit_label();
    let no_match_label = b.emit_label();
    let matched_label = b.emit_label();
    let done_label = b.emit_label();

    // Three-valued NULL semantics: if operand is NULL, result is NULL.
    b.emit_jump_to_label(Opcode::IsNull, r_operand, 0, null_label, P4::None, 0);

    b.emit_op(
        Opcode::OpenRead,
        probe_cursor,
        probe_source.table.root_page,
        0,
        P4::Table(probe_source.table.name.clone()),
        0,
    );

    // Track whether any subquery row value is NULL (for three-valued IN).
    let r_saw_null = b.alloc_temp();
    b.emit_op(Opcode::Integer, 0, r_saw_null, 0, P4::None, 0);

    let loop_start = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, probe_cursor, 0, no_match_label, P4::None, 0);

    let skip_label = probe_source.where_clause.map(|_| b.emit_label());
    if let (Some(where_expr), Some(skip)) = (probe_source.where_clause, skip_label) {
        emit_where_filter(
            b,
            where_expr,
            probe_cursor,
            probe_source.table,
            probe_source.table_alias,
            schema,
            skip,
        );
    }

    let probe_scan = ScanCtx {
        cursor: probe_cursor,
        table: probe_source.table,
        table_alias: probe_source.table_alias,
        schema: Some(schema),
    };
    match probe_source.value {
        InProbeValue::Expr(expr) => emit_expr(b, expr, r_probe, Some(&probe_scan)),
        InProbeValue::FirstColumn => {
            b.emit_op(Opcode::Column, probe_cursor, 0, r_probe, P4::None, 0);
        }
    }
    b.emit_jump_to_label(Opcode::Eq, r_probe, r_operand, matched_label, P4::None, 0);
    // If probe value was NULL, flag it (Eq never matches NULLs).
    let after_flag = b.emit_label();
    let set_flag = b.emit_label();
    b.emit_jump_to_label(Opcode::IsNull, r_probe, 0, set_flag, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, after_flag, P4::None, 0);
    b.resolve_label(set_flag);
    b.emit_op(Opcode::Integer, 1, r_saw_null, 0, P4::None, 0);
    b.resolve_label(after_flag);

    if let Some(skip) = skip_label {
        b.resolve_label(skip);
    }
    let loop_body = (loop_start + 1) as i32;
    b.emit_op(Opcode::Next, probe_cursor, loop_body, 0, P4::None, 0);

    b.resolve_label(no_match_label);
    // No match.  If any subquery value was NULL → result is NULL.
    b.emit_jump_to_label(Opcode::If, r_saw_null, 0, null_label, P4::None, 0);
    b.emit_op(Opcode::Integer, i32::from(not), reg, 0, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

    b.resolve_label(null_label);
    b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

    b.resolve_label(matched_label);
    b.emit_op(Opcode::Integer, i32::from(!not), reg, 0, P4::None, 0);

    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, probe_cursor, 0, 0, P4::None, 0);

    b.free_temp(r_saw_null);
    b.free_temp(r_probe);
    b.free_temp(r_operand);
}

/// Handles literals, bind parameters, binary/unary operators, CASE, CAST,
/// and (when `ctx` is provided) column references from a table scan cursor.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]
fn emit_expr(b: &mut ProgramBuilder, expr: &Expr, reg: i32, ctx: Option<&ScanCtx<'_>>) {
    match expr {
        Expr::Placeholder(pt, _) => {
            let idx = match pt {
                fsqlite_ast::PlaceholderType::Numbered(n) => *n as i32,
                // Anonymous and named placeholders are assigned sequentially.
                _ => b.next_anon_placeholder_idx() as i32,
            };
            b.emit_op(Opcode::Variable, idx, reg, 0, P4::None, 0);
        }
        Expr::Literal(lit, _) => match lit {
            Literal::Integer(n) => {
                if let Ok(as_i32) = i32::try_from(*n) {
                    b.emit_op(Opcode::Integer, as_i32, reg, 0, P4::None, 0);
                } else {
                    b.emit_op(Opcode::Int64, 0, reg, 0, P4::Int64(*n), 0);
                }
            }
            Literal::Float(f) => {
                b.emit_op(Opcode::Real, 0, reg, 0, P4::Real(*f), 0);
            }
            Literal::String(s) => {
                b.emit_op(Opcode::String8, 0, reg, 0, P4::Str(s.clone()), 0);
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
            }
            Literal::True => {
                b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
            }
            Literal::False => {
                b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
            }
            Literal::CurrentTimestamp | Literal::CurrentDate | Literal::CurrentTime => {
                // Emit current UTC date/time as a string literal.
                // We compute it at codegen time; for fsqlite's single-pass
                // compile+execute model this is equivalent to runtime.
                use std::time::SystemTime;
                let secs = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                // Convert epoch seconds to yyyy-mm-dd HH:MM:SS (UTC).
                let days = secs / 86400;
                let day_secs = secs % 86400;
                let h = day_secs / 3600;
                let m = (day_secs % 3600) / 60;
                let s = day_secs % 60;
                // Compute year/month/day from days since 1970-01-01.
                let (y, mo, d) = epoch_days_to_ymd(days);
                let ts = match *lit {
                    Literal::CurrentTimestamp => {
                        format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{s:02}")
                    }
                    Literal::CurrentDate => format!("{y:04}-{mo:02}-{d:02}"),
                    Literal::CurrentTime => format!("{h:02}:{m:02}:{s:02}"),
                    _ => unreachable!(),
                };
                b.emit_op(Opcode::String8, 0, reg, 0, P4::Str(ts), 0);
            }
            Literal::Null => {
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            }
        },
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            emit_binary_op(b, left, *op, right, reg, ctx);
        }
        Expr::UnaryOp {
            op, expr: operand, ..
        } => {
            emit_expr(b, operand, reg, ctx);
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
            emit_expr(b, inner, reg, ctx);
            let affinity = type_name_to_affinity(type_name);
            b.emit_op(Opcode::Cast, reg, i32::from(affinity), 0, P4::None, 0);
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => {
            emit_case_expr(b, operand.as_deref(), whens, else_expr.as_deref(), reg, ctx);
        }
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            // IS NULL → result 1 if null, 0 otherwise.
            // IS NOT NULL → result 0 if null, 1 otherwise.
            emit_expr(b, inner, reg, ctx);
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
        Expr::Like {
            expr: operand,
            pattern,
            escape,
            op: like_op,
            not,
            ..
        } => {
            let func_name = match like_op {
                fsqlite_ast::LikeOp::Like => "like",
                fsqlite_ast::LikeOp::Glob => "glob",
                fsqlite_ast::LikeOp::Match => "match",
                fsqlite_ast::LikeOp::Regexp => "regexp",
            };
            let nargs: u16 = if escape.is_some() { 3 } else { 2 };
            let arg_base = b.alloc_regs(i32::from(nargs));
            // like(pattern, string [, escape])
            emit_expr(b, pattern, arg_base, ctx);
            emit_expr(b, operand, arg_base + 1, ctx);
            if let Some(esc) = escape {
                emit_expr(b, esc, arg_base + 2, ctx);
            }
            b.emit_op(
                Opcode::PureFunc,
                0,
                arg_base,
                reg,
                P4::FuncName(func_name.to_owned()),
                nargs,
            );
            if *not {
                b.emit_op(Opcode::Not, reg, reg, 0, P4::None, 0);
            }
        }
        Expr::Between {
            expr: operand,
            low,
            high,
            not,
            ..
        } => {
            // BETWEEN low AND high → (operand >= low) AND (operand <= high)
            // with three-valued NULL logic:
            //   NULL BETWEEN x AND y  → NULL
            //   v BETWEEN NULL AND y  → NULL when v <= y, FALSE when v > y
            //   v BETWEEN x AND NULL  → NULL when v >= x, FALSE when v < x
            let r_operand = b.alloc_temp();
            let r_low = b.alloc_temp();
            let r_high = b.alloc_temp();
            emit_expr(b, operand, r_operand, ctx);
            emit_expr(b, low, r_low, ctx);
            emit_expr(b, high, r_high, ctx);
            let false_label = b.emit_label();
            let null_label = b.emit_label();
            let done_label = b.emit_label();
            // If operand is NULL, short-circuit to NULL result.
            b.emit_jump_to_label(Opcode::IsNull, r_operand, 0, null_label, P4::None, 0);
            // Jump to false if operand < low (NULL low → no jump, handled below).
            b.emit_jump_to_label(Opcode::Lt, r_low, r_operand, false_label, P4::None, 0);
            // Jump to false if operand > high (NULL high → no jump, handled below).
            b.emit_jump_to_label(Opcode::Gt, r_high, r_operand, false_label, P4::None, 0);
            // Passed both comparisons.  If either bound was NULL the comparison
            // silently fell through instead of confirming the range, so the
            // correct three-valued result is NULL, not TRUE.
            b.emit_jump_to_label(Opcode::IsNull, r_low, 0, null_label, P4::None, 0);
            b.emit_jump_to_label(Opcode::IsNull, r_high, 0, null_label, P4::None, 0);
            // Genuinely in range with no NULLs involved.
            b.emit_op(Opcode::Integer, i32::from(!*not), reg, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            b.resolve_label(false_label);
            b.emit_op(Opcode::Integer, i32::from(*not), reg, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            b.resolve_label(null_label);
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            b.resolve_label(done_label);
            b.free_temp(r_high);
            b.free_temp(r_low);
            b.free_temp(r_operand);
        }
        Expr::In {
            expr: operand,
            set,
            not,
            ..
        } => {
            if let fsqlite_ast::InSet::List(values) = set {
                // IN (v1, v2, ...) → chain of equality checks with
                // three-valued NULL semantics (SQL standard):
                //   NULL IN (...)            → NULL
                //   v IN (a, NULL, b) miss   → NULL  (NULL in list)
                //   v IN (a, b, c) miss      → FALSE (no NULLs)
                //   v IN (...) hit           → TRUE
                let r_operand = b.alloc_temp();
                emit_expr(b, operand, r_operand, ctx);
                let null_label = b.emit_label();
                let true_label = b.emit_label();
                let done_label = b.emit_label();
                // If operand is NULL, short-circuit to NULL result.
                b.emit_jump_to_label(Opcode::IsNull, r_operand, 0, null_label, P4::None, 0);
                // r_saw_null: set to 1 at runtime if any list element is NULL.
                let r_saw_null = b.alloc_temp();
                b.emit_op(Opcode::Integer, 0, r_saw_null, 0, P4::None, 0);
                let r_val = b.alloc_temp();
                for val_expr in values {
                    emit_expr(b, val_expr, r_val, ctx);
                    b.emit_jump_to_label(Opcode::Eq, r_val, r_operand, true_label, P4::None, 0);
                    // Eq with NULL never jumps.  If this value was NULL, flag it.
                    let next_val = b.emit_label();
                    let set_flag = b.emit_label();
                    b.emit_jump_to_label(Opcode::IsNull, r_val, 0, set_flag, P4::None, 0);
                    b.emit_jump_to_label(Opcode::Goto, 0, 0, next_val, P4::None, 0);
                    b.resolve_label(set_flag);
                    b.emit_op(Opcode::Integer, 1, r_saw_null, 0, P4::None, 0);
                    b.resolve_label(next_val);
                }
                b.free_temp(r_val);
                // No match.  If any list element was NULL → result is NULL.
                b.emit_jump_to_label(Opcode::If, r_saw_null, 0, null_label, P4::None, 0);
                b.free_temp(r_saw_null);
                // Definite no-match with no NULLs → FALSE (or TRUE for NOT IN).
                b.emit_op(Opcode::Integer, i32::from(*not), reg, 0, P4::None, 0);
                b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                b.resolve_label(null_label);
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                b.resolve_label(true_label);
                b.emit_op(Opcode::Integer, i32::from(!*not), reg, 0, P4::None, 0);
                b.resolve_label(done_label);
                b.free_temp(r_operand);
            } else {
                emit_in_probe_expr(b, operand, set, *not, reg, ctx);
            }
        }
        Expr::FunctionCall { name, args, .. } if !is_aggregate_function(name) => {
            // Scalar function call: emit args, then PureFunc.
            match args {
                fsqlite_ast::FunctionArgs::Star => {
                    // func(*) for non-aggregate → 0 args.
                    b.emit_op(Opcode::PureFunc, 0, 0, reg, P4::FuncName(name.clone()), 0);
                }
                fsqlite_ast::FunctionArgs::List(arg_list) => {
                    let Ok(nargs) = u16::try_from(arg_list.len()) else {
                        b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                        return;
                    };
                    let arg_base = b.alloc_regs(i32::from(nargs));
                    for (i, arg_expr) in arg_list.iter().enumerate() {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                        emit_expr(b, arg_expr, arg_base + i as i32, ctx);
                    }
                    b.emit_op(
                        Opcode::PureFunc,
                        0,
                        arg_base,
                        reg,
                        P4::FuncName(name.clone()),
                        nargs,
                    );
                }
            }
        }
        Expr::Column(col_ref, _) => {
            let Some(sc) = ctx else {
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                return;
            };
            if let Some(qualifier) = &col_ref.table {
                if !matches_table_or_alias(qualifier, sc.table, sc.table_alias) {
                    b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
                    return;
                }
            }
            if is_rowid_alias(&col_ref.column) {
                b.emit_op(Opcode::Rowid, sc.cursor, reg, 0, P4::None, 0);
            } else if let Some(col_idx) = sc.table.column_index(&col_ref.column) {
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                if sc.table.columns[col_idx].is_ipk {
                    b.emit_op(Opcode::Rowid, sc.cursor, reg, 0, P4::None, 0);
                } else {
                    b.emit_op(Opcode::Column, sc.cursor, col_idx as i32, reg, P4::None, 0);
                }
            } else {
                // Unknown column — emit Null.
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            }
        }
        Expr::Collate { expr: inner, .. } => {
            // Evaluate the inner expression; collation affects comparisons
            // rather than value computation, so a pass-through is correct.
            emit_expr(b, inner, reg, ctx);
        }
        Expr::Exists { subquery, not, .. } => {
            if let Some(scan_ctx) = ctx {
                if let Some(schema) = scan_ctx.schema {
                    emit_exists_subquery(b, subquery, *not, reg, scan_ctx, schema);
                    return;
                }
            }
            // No schema context — emit 0 (false) for EXISTS, 1 for NOT EXISTS.
            let val = i32::from(*not);
            b.emit_op(Opcode::Integer, val, reg, 0, P4::None, 0);
        }
        Expr::Subquery(subquery, _) => {
            if let Some(scan_ctx) = ctx {
                if let Some(schema) = scan_ctx.schema {
                    emit_scalar_subquery(b, subquery, reg, scan_ctx, schema);
                    return;
                }
            }
            // No schema context — emit NULL.
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        }
        Expr::JsonAccess {
            expr: inner, path, ..
        } => {
            // Rewrite `expr -> path` and `expr ->> path` as json_extract(expr, path).
            // Both arrow variants use json_extract; the subtle -> vs ->> distinction
            // (JSON text vs SQL-native) is handled identically by our json_extract
            // implementation which returns SQL-native values.
            let arg_base = b.alloc_regs(2);
            emit_expr(b, inner, arg_base, ctx);
            emit_expr(b, path, arg_base + 1, ctx);
            b.emit_op(
                Opcode::PureFunc,
                0,
                arg_base,
                reg,
                P4::FuncName("json_extract".to_owned()),
                2,
            );
        }
        _ => {
            // Column refs without scan context and other unhandled expressions: Null.
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        }
    }
}

/// Emit bytecode for an EXISTS or NOT EXISTS subquery expression.
///
/// Pattern: open cursor on subquery table, scan with WHERE filter, set reg to
/// 1 (found) or 0 (not found). For NOT EXISTS, the result is inverted.
#[allow(clippy::too_many_lines)]
fn emit_exists_subquery(
    b: &mut ProgramBuilder,
    subquery: &SelectStatement,
    not: bool,
    reg: i32,
    outer_ctx: &ScanCtx<'_>,
    schema: &[TableSchema],
) {
    // Extract the subquery's FROM table and WHERE clause.
    let (from, where_clause) = match &subquery.body.select {
        SelectCore::Select {
            from, where_clause, ..
        } => (from, where_clause),
        _ => {
            let val = i32::from(not);
            b.emit_op(Opcode::Integer, val, reg, 0, P4::None, 0);
            return;
        }
    };

    let from_clause = match from {
        Some(f) => f,
        None => {
            // EXISTS on a no-FROM query like `EXISTS (SELECT 1)` — always true.
            let val = i32::from(!not);
            b.emit_op(Opcode::Integer, val, reg, 0, P4::None, 0);
            return;
        }
    };

    let (table_name, sub_alias) = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => (&name.name, alias.as_deref()),
        _ => {
            let val = i32::from(not);
            b.emit_op(Opcode::Integer, val, reg, 0, P4::None, 0);
            return;
        }
    };

    let table = match find_table(schema, table_name) {
        Ok(t) => t,
        Err(_) => {
            let val = i32::from(not);
            b.emit_op(Opcode::Integer, val, reg, 0, P4::None, 0);
            return;
        }
    };

    // Use cursor offset far from the main scan cursors.
    let sub_cursor = outer_ctx.cursor + 128;
    let found_label = b.emit_label();
    let done_label = b.emit_label();

    // Default result: 0 (not found) for EXISTS, 1 for NOT EXISTS.
    let default_val = i32::from(not);
    b.emit_op(Opcode::Integer, default_val, reg, 0, P4::None, 0);

    // Open a read cursor on the subquery table.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::OpenRead,
        sub_cursor,
        table.root_page as i32,
        0,
        P4::Int(table.columns.len() as i32),
        0,
    );

    let rewind_addr = b.current_addr();
    b.emit_jump_to_label(Opcode::Rewind, sub_cursor, 0, done_label, P4::None, 0);
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (rewind_addr + 1) as i32;

    // Build scan context for the subquery table so correlated WHERE refs resolve.
    let sub_ctx = ScanCtx {
        cursor: sub_cursor,
        table,
        table_alias: sub_alias,
        schema: Some(schema),
    };

    // Apply WHERE filter if present.
    if let Some(where_expr) = where_clause {
        let r_cond = b.alloc_temp();
        emit_expr_with_fallback(b, where_expr, r_cond, &sub_ctx, Some(outer_ctx));
        let next_label = b.emit_label();
        b.emit_jump_to_label(Opcode::IfNot, r_cond, 1, next_label, P4::None, 0);
        b.free_temp(r_cond);

        // Row matches WHERE — found.
        let found_val = i32::from(!not);
        b.emit_op(Opcode::Integer, found_val, reg, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::Goto, 0, 0, found_label, P4::None, 0);

        b.resolve_label(next_label);
    } else {
        // No WHERE — if any row exists, result is found.
        let found_val = i32::from(!not);
        b.emit_op(Opcode::Integer, found_val, reg, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::Goto, 0, 0, found_label, P4::None, 0);
    }

    // Next row.
    b.emit_op(Opcode::Next, sub_cursor, loop_body, 0, P4::None, 0);

    // Fall through: no row matched.
    b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

    b.resolve_label(found_label);

    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, sub_cursor, 0, 0, P4::None, 0);
}

/// Emit bytecode for a scalar subquery expression `(SELECT expr FROM ...)`.
///
/// Evaluates the subquery and places the first result value into `reg`.
/// If the subquery returns no rows, `reg` is set to NULL.
#[allow(clippy::too_many_lines)]
fn emit_scalar_subquery(
    b: &mut ProgramBuilder,
    subquery: &SelectStatement,
    reg: i32,
    outer_ctx: &ScanCtx<'_>,
    schema: &[TableSchema],
) {
    let (columns, from, where_clause, group_by) = match &subquery.body.select {
        SelectCore::Select {
            columns,
            from,
            where_clause,
            group_by,
            ..
        } => (columns, from, where_clause, group_by),
        _ => {
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            return;
        }
    };

    // No-FROM scalar subquery: `(SELECT 1)`, `(SELECT 1 + 2)`.
    if from.is_none() {
        if let Some(ResultColumn::Expr { expr, .. }) = columns.first() {
            emit_expr(b, expr, reg, Some(outer_ctx));
            return;
        }
        b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
        return;
    }

    let from_clause = from.as_ref().unwrap();
    let (table_name, sub_alias) = match &from_clause.source {
        fsqlite_ast::TableOrSubquery::Table { name, alias, .. } => (&name.name, alias.as_deref()),
        _ => {
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            return;
        }
    };

    let table = match find_table(schema, table_name) {
        Ok(t) => t,
        Err(_) => {
            b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            return;
        }
    };

    // Use cursor offset far from main scan cursors.
    let sub_cursor = outer_ctx.cursor + 129;

    let done_label = b.emit_label();

    // Default: NULL (subquery returns no rows).
    b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    b.emit_op(
        Opcode::OpenRead,
        sub_cursor,
        table.root_page as i32,
        0,
        P4::Int(table.columns.len() as i32),
        0,
    );

    let sub_ctx = ScanCtx {
        cursor: sub_cursor,
        table,
        table_alias: sub_alias,
        schema: Some(schema),
    };

    // Check if this is an aggregate query (e.g., SELECT MAX(x) FROM t).
    let is_agg = has_aggregate_columns(columns);

    if is_agg && group_by.is_empty() {
        // Simple aggregate subquery without GROUP BY:
        // e.g., (SELECT COUNT(*) FROM t), (SELECT MAX(x) FROM t WHERE ...)
        emit_scalar_aggregate_subquery(
            b,
            columns,
            &sub_ctx,
            outer_ctx,
            where_clause.as_deref(),
            reg,
            done_label,
        );
    } else {
        // Non-aggregate scalar subquery: grab first row's first column value.
        let rewind_addr = b.current_addr();
        b.emit_jump_to_label(Opcode::Rewind, sub_cursor, 0, done_label, P4::None, 0);
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let loop_body = (rewind_addr + 1) as i32;

        // Apply WHERE filter.
        let next_label = b.emit_label();
        if let Some(where_expr) = where_clause {
            let r_cond = b.alloc_temp();
            emit_expr_with_fallback(b, where_expr, r_cond, &sub_ctx, Some(outer_ctx));
            b.emit_jump_to_label(Opcode::IfNot, r_cond, 1, next_label, P4::None, 0);
            b.free_temp(r_cond);
        }

        // Evaluate the first result column expression.
        if let Some(ResultColumn::Expr { expr, .. }) = columns.first() {
            emit_expr_with_fallback(b, expr, reg, &sub_ctx, Some(outer_ctx));
        }

        // Got our value — jump to done (only need one row).
        b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

        b.resolve_label(next_label);
        b.emit_op(Opcode::Next, sub_cursor, loop_body, 0, P4::None, 0);
    }

    b.resolve_label(done_label);
    b.emit_op(Opcode::Close, sub_cursor, 0, 0, P4::None, 0);
}

/// Emit bytecode for a simple aggregate scalar subquery (no GROUP BY).
///
/// Handles `(SELECT COUNT(*) FROM t)`, `(SELECT MAX(x) FROM t WHERE ...)`, etc.
fn emit_scalar_aggregate_subquery(
    b: &mut ProgramBuilder,
    columns: &[ResultColumn],
    sub_ctx: &ScanCtx<'_>,
    outer_ctx: &ScanCtx<'_>,
    where_clause: Option<&Expr>,
    reg: i32,
    _done_label: crate::Label,
) {
    // Parse the aggregate columns.
    let Ok(agg_cols) = parse_aggregate_columns(columns, sub_ctx.table) else {
        return;
    };

    if agg_cols.is_empty() {
        return;
    }

    let accum_reg = b.alloc_temp();
    b.emit_op(Opcode::Null, 0, accum_reg, 0, P4::None, 0);

    let finalize_label = b.emit_label();
    let rewind_addr = b.current_addr();
    b.emit_jump_to_label(
        Opcode::Rewind,
        sub_ctx.cursor,
        0,
        finalize_label,
        P4::None,
        0,
    );
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let loop_body = (rewind_addr + 1) as i32;

    // Apply WHERE filter.
    let skip_label = b.emit_label();
    if let Some(where_expr) = where_clause {
        let r_cond = b.alloc_temp();
        emit_expr_with_fallback(b, where_expr, r_cond, sub_ctx, Some(outer_ctx));
        b.emit_jump_to_label(Opcode::IfNot, r_cond, 1, skip_label, P4::None, 0);
        b.free_temp(r_cond);
    }

    // AggStep for each aggregate.
    let agg = &agg_cols[0]; // scalar subquery uses first aggregate only
    let total_args = agg.num_args.max(1);
    let arg_base = b.alloc_regs(total_args);
    if agg.num_args > 0 {
        if agg.arg_is_rowid {
            b.emit_op(Opcode::Rowid, sub_ctx.cursor, arg_base, 0, P4::None, 0);
        } else if let Some(ref expr) = agg.arg_expr {
            emit_expr_with_fallback(b, expr, arg_base, sub_ctx, Some(outer_ctx));
        } else if let Some(col_idx) = agg.arg_col_index {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(
                Opcode::Column,
                sub_ctx.cursor,
                col_idx as i32,
                arg_base,
                P4::None,
                0,
            );
        }
        // Extra arguments (e.g. separator for group_concat).
        for (j, extra_expr) in agg.extra_args.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let extra_reg = arg_base + 1 + j as i32;
            emit_expr_with_fallback(b, extra_expr, extra_reg, sub_ctx, Some(outer_ctx));
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    let num_args = agg.num_args as u16;
    let distinct_flag = i32::from(agg.distinct);
    b.emit_op(
        Opcode::AggStep,
        distinct_flag,
        arg_base,
        accum_reg,
        P4::FuncName(agg.name.clone()),
        num_args,
    );

    b.resolve_label(skip_label);
    b.emit_op(Opcode::Next, sub_ctx.cursor, loop_body, 0, P4::None, 0);

    b.resolve_label(finalize_label);
    b.emit_op(
        Opcode::AggFinal,
        accum_reg,
        agg.num_args,
        0,
        P4::FuncName(agg.name.clone()),
        0,
    );
    // Copy result to target register.
    b.emit_op(Opcode::Copy, accum_reg, reg, 0, P4::None, 0);
    b.free_temp(accum_reg);
}

/// Evaluate an expression with fallback context for correlated subqueries.
///
/// For column references, tries the inner (subquery) context first; if the
/// column doesn't belong to the inner table, falls back to the outer context.
/// For compound expressions, recurses so nested column refs get fallback logic.
fn emit_expr_with_fallback(
    b: &mut ProgramBuilder,
    expr: &Expr,
    reg: i32,
    inner_ctx: &ScanCtx<'_>,
    outer_ctx: Option<&ScanCtx<'_>>,
) {
    match expr {
        Expr::Column(col_ref, _) => {
            if resolve_column_in_ctx(col_ref, inner_ctx).is_some() {
                emit_expr(b, expr, reg, Some(inner_ctx));
            } else if let Some(outer) = outer_ctx {
                emit_expr(b, expr, reg, Some(outer));
            } else {
                b.emit_op(Opcode::Null, 0, reg, 0, P4::None, 0);
            }
        }
        Expr::BinaryOp {
            left, op, right, ..
        } => {
            // Recursively resolve column refs in children, then apply the op.
            let r_left = b.alloc_temp();
            let r_right = b.alloc_temp();
            emit_expr_with_fallback(b, left, r_left, inner_ctx, outer_ctx);
            emit_expr_with_fallback(b, right, r_right, inner_ctx, outer_ctx);

            if matches!(
                op,
                fsqlite_ast::BinaryOp::Eq
                    | fsqlite_ast::BinaryOp::Ne
                    | fsqlite_ast::BinaryOp::Lt
                    | fsqlite_ast::BinaryOp::Le
                    | fsqlite_ast::BinaryOp::Gt
                    | fsqlite_ast::BinaryOp::Ge
            ) {
                let cmp_opcode = match op {
                    fsqlite_ast::BinaryOp::Eq => Opcode::Eq,
                    fsqlite_ast::BinaryOp::Ne => Opcode::Ne,
                    fsqlite_ast::BinaryOp::Lt => Opcode::Lt,
                    fsqlite_ast::BinaryOp::Le => Opcode::Le,
                    fsqlite_ast::BinaryOp::Gt => Opcode::Gt,
                    fsqlite_ast::BinaryOp::Ge => Opcode::Ge,
                    _ => unreachable!(),
                };
                // Comparison: p1=rhs, p3=lhs, label→p2.
                let true_label = b.emit_label();
                let done_label = b.emit_label();
                b.emit_jump_to_label(cmp_opcode, r_right, r_left, true_label, P4::None, 0);
                b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
                b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                b.resolve_label(true_label);
                b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
                b.resolve_label(done_label);
            } else if matches!(op, fsqlite_ast::BinaryOp::Is | fsqlite_ast::BinaryOp::IsNot) {
                let (cmp_opcode, flag) = match op {
                    fsqlite_ast::BinaryOp::Is => (Opcode::Eq, 0x80_u16),
                    _ => (Opcode::Ne, 0x80_u16),
                };
                let true_label = b.emit_label();
                let done_label = b.emit_label();
                b.emit_jump_to_label(cmp_opcode, r_right, r_left, true_label, P4::None, flag);
                b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
                b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
                b.resolve_label(true_label);
                b.emit_op(Opcode::Integer, 1, reg, 0, P4::None, 0);
                b.resolve_label(done_label);
            } else {
                // Arithmetic / logical / bitwise.
                let vdbe_op = binary_op_to_opcode(*op);
                b.emit_op(vdbe_op, r_left, r_right, reg, P4::None, 0);
            }
            b.free_temp(r_left);
            b.free_temp(r_right);
        }
        Expr::UnaryOp {
            op, expr: inner, ..
        } => {
            emit_expr_with_fallback(b, inner, reg, inner_ctx, outer_ctx);
            match op {
                fsqlite_ast::UnaryOp::Negate => {
                    let tmp = b.alloc_temp();
                    b.emit_op(Opcode::Integer, -1, tmp, 0, P4::None, 0);
                    b.emit_op(Opcode::Multiply, tmp, reg, reg, P4::None, 0);
                    b.free_temp(tmp);
                }
                fsqlite_ast::UnaryOp::Plus => {}
                fsqlite_ast::UnaryOp::BitNot => {
                    b.emit_op(Opcode::BitNot, reg, reg, 0, P4::None, 0);
                }
                fsqlite_ast::UnaryOp::Not => {
                    b.emit_op(Opcode::Not, reg, reg, 0, P4::None, 0);
                }
            }
        }
        // For other expression types, use the inner context.
        _ => {
            emit_expr(b, expr, reg, Some(inner_ctx));
        }
    }
}

/// Check if a column reference resolves in a given scan context.
fn resolve_column_in_ctx(col_ref: &ColumnRef, ctx: &ScanCtx<'_>) -> Option<usize> {
    // Qualified: table.column
    if let Some(ref table_name) = col_ref.table {
        if table_name == &ctx.table.name || ctx.table_alias == Some(table_name.as_str()) {
            return ctx
                .table
                .columns
                .iter()
                .position(|c| c.name == col_ref.column);
        }
        return None;
    }
    // Unqualified: just column name
    ctx.table
        .columns
        .iter()
        .position(|c| c.name == col_ref.column)
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
    ctx: Option<&ScanCtx<'_>>,
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
        emit_comparison(b, left, op, right, reg, ctx);
        return;
    }

    if matches!(op, fsqlite_ast::BinaryOp::Is | fsqlite_ast::BinaryOp::IsNot) {
        emit_is_comparison(b, left, op, right, reg, ctx);
        return;
    }

    // Arithmetic / logical / bitwise: evaluate left into reg, right into tmp,
    // then apply the opcode.
    let tmp = b.alloc_temp();
    emit_expr(b, left, reg, ctx);
    emit_expr(b, right, tmp, ctx);
    let opcode = binary_op_to_opcode(op);
    // VDBE arithmetic: OP p1=rhs, p2=lhs, p3=dest
    b.emit_op(opcode, tmp, reg, reg, P4::None, 0);
    b.free_temp(tmp);
}

/// Extract collation name from a `COLLATE` wrapper, if present.
fn extract_collation(expr: &Expr) -> Option<&str> {
    if let Expr::Collate { collation, .. } = expr {
        Some(collation.as_str())
    } else {
        None
    }
}

/// Emit a comparison expression that produces 1 (true) or 0 (false).
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn emit_comparison(
    b: &mut ProgramBuilder,
    left: &Expr,
    op: fsqlite_ast::BinaryOp,
    right: &Expr,
    reg: i32,
    ctx: Option<&ScanCtx<'_>>,
) {
    let r_left = b.alloc_temp();
    let r_right = b.alloc_temp();
    emit_expr(b, left, r_left, ctx);
    emit_expr(b, right, r_right, ctx);

    let cmp_opcode = match op {
        fsqlite_ast::BinaryOp::Eq => Some(Opcode::Eq),
        fsqlite_ast::BinaryOp::Ne => Some(Opcode::Ne),
        fsqlite_ast::BinaryOp::Lt => Some(Opcode::Lt),
        fsqlite_ast::BinaryOp::Le => Some(Opcode::Le),
        fsqlite_ast::BinaryOp::Gt => Some(Opcode::Gt),
        fsqlite_ast::BinaryOp::Ge => Some(Opcode::Ge),
        _ => None,
    };
    let Some(cmp_opcode) = cmp_opcode else {
        b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
        b.free_temp(r_right);
        b.free_temp(r_left);
        return;
    };

    // Check for COLLATE on either operand and propagate to comparison opcode.
    let p4 = extract_collation(left)
        .or_else(|| extract_collation(right))
        .map_or(P4::None, |coll| P4::Collation(coll.to_owned()));

    // Pattern: assume false (0), jump to true_label if condition holds.
    let true_label = b.emit_label();
    let done_label = b.emit_label();

    // Comparison: p1=rhs_reg, p2=jump_target (label), p3=lhs_reg
    b.emit_jump_to_label(cmp_opcode, r_right, r_left, true_label, p4, 0);
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
    ctx: Option<&ScanCtx<'_>>,
) {
    let r_left = b.alloc_temp();
    let r_right = b.alloc_temp();
    emit_expr(b, left, r_left, ctx);
    emit_expr(b, right, r_right, ctx);

    let true_label = b.emit_label();
    let done_label = b.emit_label();

    // IS uses Eq with NULLEQ flag (p5=0x80). IS NOT uses Ne with NULLEQ.
    let cmp_and_flag = match op {
        fsqlite_ast::BinaryOp::Is => Some((Opcode::Eq, 0x80_u16)),
        fsqlite_ast::BinaryOp::IsNot => Some((Opcode::Ne, 0x80_u16)),
        _ => None,
    };
    let Some((cmp_opcode, nulleq_flag)) = cmp_and_flag else {
        b.emit_op(Opcode::Integer, 0, reg, 0, P4::None, 0);
        b.free_temp(r_right);
        b.free_temp(r_left);
        return;
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

/// Emit CASE \[operand\] WHEN ... THEN ... \[ELSE ...\] END.
fn emit_case_expr(
    b: &mut ProgramBuilder,
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    reg: i32,
    ctx: Option<&ScanCtx<'_>>,
) {
    let done_label = b.emit_label();
    let r_operand = operand.map(|op_expr| {
        let r = b.alloc_temp();
        emit_expr(b, op_expr, r, ctx);
        r
    });

    for (when_expr, then_expr) in whens {
        let next_when = b.emit_label();

        if let Some(r_op) = r_operand {
            // Simple CASE: compare operand to each WHEN value.
            let r_when = b.alloc_temp();
            emit_expr(b, when_expr, r_when, ctx);
            // NULL in either operand or WHEN value means no match
            // (NULL = x is UNKNOWN in SQL, which is falsy for CASE).
            b.emit_jump_to_label(Opcode::IsNull, r_op, 0, next_when, P4::None, 0);
            b.emit_jump_to_label(Opcode::IsNull, r_when, 0, next_when, P4::None, 0);
            // If operand != when_value, skip to next WHEN.
            b.emit_jump_to_label(Opcode::Ne, r_when, r_op, next_when, P4::None, 0);
            b.free_temp(r_when);
        } else {
            // Searched CASE: each WHEN is a boolean condition.
            emit_expr(b, when_expr, reg, ctx);
            // If condition is false/null, skip to next WHEN.
            b.emit_jump_to_label(Opcode::IfNot, reg, 1, next_when, P4::None, 0);
        }

        // Emit THEN expression.
        emit_expr(b, then_expr, reg, ctx);
        b.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);

        b.resolve_label(next_when);
    }

    // ELSE clause (or NULL if no ELSE).
    if let Some(el) = else_expr {
        emit_expr(b, el, reg, ctx);
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
    // Encoding: A..E maps to BLOB, TEXT, NUMERIC, INTEGER, REAL:
    // 'A' = BLOB, 'B' = TEXT, 'C' = NUMERIC, 'D' = INTEGER, 'E' = REAL.
    let name = type_name.name.to_uppercase();
    if name.contains("INT") {
        b'D' // INTEGER affinity
    } else if name.contains("CHAR") || name.contains("TEXT") || name.contains("CLOB") {
        b'B' // TEXT affinity
    } else if name.contains("BLOB") || name.is_empty() {
        b'A' // BLOB affinity
    } else if name.contains("REAL") || name.contains("FLOA") || name.contains("DOUB") {
        b'E' // REAL affinity
    } else {
        b'C' // NUMERIC affinity
    }
}

/// Convert days since 1970-01-01 (Unix epoch) to (year, month, day).
fn epoch_days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Civil-date algorithm from Howard Hinnant (public domain).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
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
        Distinctness, Expr, FromClause, InSet, InsertSource, InsertStatement, LimitClause, Literal,
        OrderingTerm, PlaceholderType, QualifiedName, QualifiedTableRef, ResultColumn, SelectBody,
        SelectCore, SelectStatement, SortDirection, Span, TableOrSubquery, UpdateStatement,
    };
    use fsqlite_types::opcode::{Opcode, P4};

    fn test_schema() -> Vec<TableSchema> {
        vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo::basic("a", 'd', false),
                ColumnInfo::basic("b", 'C', false),
            ],
            indexes: vec![],
            strict: false,
        }]
    }

    fn test_schema_with_index() -> Vec<TableSchema> {
        vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo::basic("a", 'd', false),
                ColumnInfo::basic("b", 'C', false),
            ],
            indexes: vec![IndexSchema {
                name: "idx_t_b".to_owned(),
                root_page: 3,
                columns: vec!["b".to_owned()],
                is_unique: false,
            }],
            strict: false,
        }]
    }

    fn test_schema_with_subquery_source() -> Vec<TableSchema> {
        vec![
            TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo {
                        name: "a".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                ],
                indexes: vec![],
                strict: false,
            },
            TableSchema {
                name: "s".to_owned(),
                root_page: 3,
                columns: vec![ColumnInfo::basic("b", 'd', false)],
                indexes: vec![],
                strict: false,
            },
        ]
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

    fn star_select_with_limit(table: &str, limit: i64) -> SelectStatement {
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
            limit: Some(LimitClause {
                limit: Expr::Literal(Literal::Integer(limit), Span::ZERO),
                offset: None,
            }),
        }
    }

    fn star_select_with_limit_offset(table: &str, limit: i64, offset: i64) -> SelectStatement {
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
            limit: Some(LimitClause {
                limit: Expr::Literal(Literal::Integer(limit), Span::ZERO),
                offset: Some(Expr::Literal(Literal::Integer(offset), Span::ZERO)),
            }),
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

    fn schema_with_ipk_alias() -> Vec<TableSchema> {
        vec![TableSchema {
            name: "t".to_owned(),
            root_page: 2,
            columns: vec![
                ColumnInfo::basic("a", 'd', true),
                ColumnInfo::basic("b", 'C', false),
            ],
            indexes: vec![],
            strict: false,
        }]
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
    fn test_codegen_select_ipk_column_uses_rowid_opcode() {
        let stmt = simple_select(&["a"], "t", None);
        let schema = schema_with_ipk_alias();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = opcode_sequence(&prog);

        assert!(
            ops.contains(&Opcode::Rowid),
            "IPK column projection should read rowid"
        );
        assert!(
            !ops.contains(&Opcode::Column),
            "single IPK projection should not read record columns"
        );
    }

    #[test]
    fn test_codegen_select_star_uses_rowid_for_ipk_column() {
        let stmt = star_select("t");
        let schema = schema_with_ipk_alias();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Rewind,
                Opcode::Rowid,
                Opcode::Column,
                Opcode::ResultRow
            ]
        ));
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
                Opcode::Variable,
                Opcode::Variable,
                Opcode::NewRowid,
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
    fn test_codegen_insert_values_large_integer_literal_uses_int64_opcode() {
        let big = 4_102_444_800_000_000_i64;
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![
                Expr::Literal(Literal::Integer(big), Span::ZERO),
                Expr::Literal(Literal::String("payload".to_owned()), Span::ZERO),
            ]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Int64
                && matches!(op.p4, P4::Int64(value) if value == big)),
            "expected OP_Int64 carrying the full i64 literal in INSERT VALUES codegen"
        );
    }

    #[test]
    fn test_emit_limit_expr_large_integer_literal_uses_int64_opcode() {
        let big = 4_102_444_800_000_000_i64;
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let expr = Expr::Literal(Literal::Integer(big), Span::ZERO);

        emit_limit_expr(&mut b, &expr, reg);

        let prog = b.finish().unwrap();
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Int64
                && op.p2 == reg
                && matches!(op.p4, P4::Int64(value) if value == big)),
            "expected OP_Int64 for large LIMIT literals"
        );
    }

    #[test]
    fn test_codegen_insert_values_rejects_mixed_arity_rows() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![
                vec![placeholder(1), placeholder(2)],
                vec![placeholder(3)],
            ]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        let err = codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap_err();
        assert!(
            matches!(err, CodegenError::Unsupported(ref msg) if msg.contains("same arity")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_emit_comparison_invalid_operator_emits_false() {
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let one = Expr::Literal(Literal::Integer(1), Span::ZERO);
        let two = Expr::Literal(Literal::Integer(2), Span::ZERO);

        emit_comparison(&mut b, &one, AstBinaryOp::Add, &two, reg, None);

        let prog = b.finish().unwrap();
        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::Integer && op.p1 == 0 && op.p2 == reg),
            "expected fallback false assignment for invalid comparison op"
        );
    }

    #[test]
    fn test_emit_is_comparison_invalid_operator_emits_false() {
        let mut b = ProgramBuilder::new();
        let reg = b.alloc_reg();
        let one = Expr::Literal(Literal::Integer(1), Span::ZERO);
        let two = Expr::Literal(Literal::Integer(2), Span::ZERO);

        emit_is_comparison(&mut b, &one, AstBinaryOp::Eq, &two, reg, None);

        let prog = b.finish().unwrap();
        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::Integer && op.p1 == 0 && op.p2 == reg),
            "expected fallback false assignment for invalid IS/IS NOT op"
        );
    }

    // === Test: INSERT ... SELECT ===
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select() {
        // Schema with two tables: source "s" and target "t".
        let schema = vec![
            TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo {
                        name: "a".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                ],
                indexes: vec![],
                strict: false,
            },
            TableSchema {
                name: "s".to_owned(),
                root_page: 3,
                columns: vec![
                    ColumnInfo {
                        name: "x".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                    ColumnInfo {
                        name: "y".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                ],
                indexes: vec![],
                strict: false,
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

        // Should contain: Init, Transaction(write), OpenWrite(target),
        // OpenRead(source), Rewind, Column reads, NewRowid, MakeRecord,
        // Insert, Next, Close(source), Close(target), Halt.
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

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select_propagates_or_conflict_to_insert_p5() {
        let schema = vec![
            TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo::basic("a", 'd', false),
                    ColumnInfo::basic("b", 'C', false),
                ],
                indexes: vec![],
                strict: false,
            },
            TableSchema {
                name: "s".to_owned(),
                root_page: 3,
                columns: vec![
                    ColumnInfo::basic("x", 'd', false),
                    ColumnInfo::basic("y", 'C', false),
                ],
                indexes: vec![],
                strict: false,
            },
        ];

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
            or_conflict: Some(fsqlite_ast::ConflictAction::Ignore),
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

        let insert = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::Insert)
            .expect("expected Insert opcode");
        assert_eq!(insert.p5, OE_IGNORE);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select_emits_index_inserts() {
        // Target has one secondary index; source has none.
        let schema = vec![
            TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo::basic("a", 'd', false),
                    ColumnInfo::basic("b", 'C', false),
                ],
                indexes: vec![IndexSchema {
                    name: "idx_t_a".to_owned(),
                    root_page: 4,
                    columns: vec!["a".to_owned()],
                    is_unique: false,
                }],
                strict: false,
            },
            TableSchema {
                name: "s".to_owned(),
                root_page: 3,
                columns: vec![
                    ColumnInfo::basic("x", 'd', false),
                    ColumnInfo::basic("y", 'C', false),
                ],
                indexes: vec![],
                strict: false,
            },
        ];

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

        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::OpenWrite && op.p2 == 4),
            "expected OpenWrite for target secondary index root page"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::IdxInsert),
            "INSERT ... SELECT should maintain target indexes via IdxInsert"
        );
    }

    // === Test: INSERT ... SELECT with specific columns ===
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_codegen_insert_select_with_columns() {
        // Schema with source "s" having 3 columns, target "t" with 2.
        let schema = vec![
            TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo {
                        name: "a".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                    ColumnInfo {
                        name: "b".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                ],
                indexes: vec![],
                strict: false,
            },
            TableSchema {
                name: "s".to_owned(),
                root_page: 3,
                columns: vec![
                    ColumnInfo {
                        name: "x".to_owned(),
                        affinity: 'd',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                    ColumnInfo {
                        name: "y".to_owned(),
                        affinity: 'C',
                        is_ipk: false,
                        type_name: None,
                        notnull: false,
                        unique: false,
                        default_value: None,
                        strict_type: None,
                    },
                    ColumnInfo::basic("z", 'e', false),
                ],
                indexes: vec![],
                strict: false,
            },
        ];

        // INSERT INTO t SELECT x, y FROM s
        let inner_select = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare("x"), Span::ZERO),
                            alias: None,
                        },
                        ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare("y"), Span::ZERO),
                            alias: None,
                        },
                    ],
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

        // Should have exactly 2 Column reads (x and y), not 3.
        let column_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Column)
            .count();
        assert_eq!(column_count, 2);
    }

    // === Test: SELECT DISTINCT full scan ===
    #[test]
    fn test_codegen_select_distinct_full_scan() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::Distinct,
                    columns: vec![ResultColumn::Star],
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
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // DISTINCT scan uses sorter: SorterOpen, Rewind/Next scan,
        // SorterInsert, SorterSort, SorterData, MakeRecord (for dedup),
        // Eq (compare), Copy (update prev), ResultRow, SorterNext.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::SorterOpen,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::Column,
                Opcode::MakeRecord,
                Opcode::SorterInsert,
                Opcode::Next,
                Opcode::Close,
                Opcode::SorterSort,
                Opcode::SorterData,
                Opcode::Column,
                Opcode::Column,
                Opcode::MakeRecord,
                Opcode::Eq,
                Opcode::Copy,
                Opcode::ResultRow,
                Opcode::SorterNext,
            ]
        ));
    }

    #[test]
    fn test_codegen_select_distinct_full_scan_offset_after_dedup() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::Distinct,
                    columns: vec![ResultColumn::Star],
                    from: Some(from_table("t")),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: Some(LimitClause {
                limit: Expr::Literal(Literal::Integer(5), Span::ZERO),
                offset: Some(Expr::Literal(Literal::Integer(1), Span::ZERO)),
            }),
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let eq_pos = prog
            .ops()
            .iter()
            .position(|op| op.opcode == Opcode::Eq)
            .expect("missing DISTINCT Eq opcode");
        let ifpos_pos = prog
            .ops()
            .iter()
            .position(|op| op.opcode == Opcode::IfPos)
            .expect("missing OFFSET IfPos opcode");
        assert!(
            eq_pos < ifpos_pos,
            "DISTINCT dedup must run before OFFSET filtering"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Null),
            "expected DISTINCT previous-record register initialization"
        );
    }

    // === Test: SELECT DISTINCT with ORDER BY ===
    #[test]
    fn test_codegen_select_distinct_with_order_by() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::Distinct,
                    columns: vec![ResultColumn::Star],
                    from: Some(from_table("t")),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::bare("a"), Span::ZERO),
                direction: None,
                nulls: None,
            }],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // ORDER BY + DISTINCT: uses ordered scan with dedup.
        // Should include SorterOpen, Rewind scan, SorterInsert, SorterSort,
        // then SorterData + Column reads + MakeRecord + Eq + Copy + ResultRow.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::SorterOpen,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::SorterInsert,
                Opcode::Next,
                Opcode::SorterSort,
                Opcode::SorterData,
                Opcode::MakeRecord,
                Opcode::Eq,
                Opcode::Copy,
                Opcode::ResultRow,
                Opcode::SorterNext,
            ]
        ));
    }

    #[test]
    fn test_codegen_select_distinct_with_order_by_offset_after_dedup() {
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::Distinct,
                    columns: vec![ResultColumn::Star],
                    from: Some(from_table("t")),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::bare("a"), Span::ZERO),
                direction: Some(SortDirection::Asc),
                nulls: None,
            }],
            limit: Some(LimitClause {
                limit: Expr::Literal(Literal::Integer(5), Span::ZERO),
                offset: Some(Expr::Literal(Literal::Integer(2), Span::ZERO)),
            }),
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let eq_pos = prog
            .ops()
            .iter()
            .position(|op| op.opcode == Opcode::Eq)
            .expect("missing DISTINCT Eq opcode");
        let ifpos_pos = prog
            .ops()
            .iter()
            .position(|op| op.opcode == Opcode::IfPos)
            .expect("missing OFFSET IfPos opcode");
        assert!(
            eq_pos < ifpos_pos,
            "DISTINCT dedup must run before OFFSET filtering"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Null),
            "expected DISTINCT previous-record register initialization"
        );
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

        // Verify scan-based update: Rewind loop, read all columns,
        // overwrite changed column (Variable for bind param), delete old row,
        // then insert rewritten record.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::Rewind,     // full scan
                Opcode::Column,     // read existing col a
                Opcode::Column,     // read existing col b
                Opcode::Variable,   // new value for b
                Opcode::Rowid,      // get current rowid
                Opcode::Delete,     // delete old row
                Opcode::MakeRecord, // pack ALL columns
                Opcode::Insert,     // write back
                Opcode::Next,       // loop
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
    fn test_codegen_update_ipk_assignment_updates_rowid() {
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("a".to_owned()),
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

        let mut schema = test_schema();
        schema[0].columns[0].is_ipk = true;

        let ctx = CodegenContext {
            concurrent_mode: true,
            rowid_alias_col_idx: Some(0),
        };
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let ops: Vec<Opcode> = prog.ops().iter().map(|op| op.opcode).collect();
        assert!(
            ops.contains(&Opcode::Delete),
            "UPDATE must delete old row before reinsert"
        );
        assert!(
            ops.contains(&Opcode::IsNull) && ops.contains(&Opcode::NewRowid),
            "IPK update should handle NULL rowid by generating NewRowid"
        );

        let delete_pos = ops
            .iter()
            .position(|&op| op == Opcode::Delete)
            .expect("Delete opcode should exist");
        let insert_pos = ops
            .iter()
            .position(|&op| op == Opcode::Insert)
            .expect("Insert opcode should exist");
        assert!(
            delete_pos < insert_pos,
            "Delete must execute before Insert in UPDATE rewrite"
        );
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

        // Verify scan-based delete with reverse iteration (Last/Prev).
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::Last,   // start from end
                Opcode::Delete, // delete matching row
                Opcode::Prev,   // iterate backwards
                Opcode::Close,
                Opcode::Halt,
            ]
        ));
    }

    #[test]
    fn test_codegen_update_with_index_emits_keyed_idxdelete() {
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
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let idx_delete = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::IdxDelete)
            .expect("expected IdxDelete for indexed UPDATE");
        assert!(
            idx_delete.p3 > 0,
            "IdxDelete must carry key register count (p3 > 0) so engine seeks by key"
        );
    }

    #[test]
    fn test_codegen_delete_with_index_emits_keyed_idxdelete() {
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
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_delete(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let idx_delete = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::IdxDelete)
            .expect("expected IdxDelete for indexed DELETE");
        assert!(
            idx_delete.p3 > 0,
            "IdxDelete must carry key register count (p3 > 0) so engine seeks by key"
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
            ..CodegenContext::default()
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

    #[test]
    fn test_codegen_concurrent_newrowid_default_values() {
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
        let schema = test_schema();
        let ctx = CodegenContext {
            concurrent_mode: true,
            ..CodegenContext::default()
        };
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let nr = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::NewRowid)
            .unwrap();
        assert_ne!(
            nr.p3, 0,
            "NewRowid p3 should be non-zero in concurrent mode for DEFAULT VALUES"
        );

        // In non-concurrent mode, p3 should be 0 for DEFAULT VALUES as well.
        let ctx_normal = CodegenContext::default();
        let mut b2 = ProgramBuilder::new();
        codegen_insert(&mut b2, &stmt, &schema, &ctx_normal).unwrap();
        let prog2 = b2.finish().unwrap();
        let nr2 = prog2
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::NewRowid)
            .unwrap();
        assert_eq!(
            nr2.p3, 0,
            "NewRowid p3 should be 0 in normal mode for DEFAULT VALUES"
        );
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
    fn test_codegen_select_non_column_expr_with_from_accepted() {
        // Non-column expressions in SELECT list with FROM are now supported
        // via ScanCtx-aware emit_expr in emit_column_reads.
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
        codegen_select(&mut b, &stmt, &schema, &ctx)
            .expect("non-column expression in SELECT list should succeed");
        let prog = b.finish().unwrap();

        // Should contain Add opcode for the 1 + 2 expression.
        assert!(
            has_opcodes(
                &prog,
                &[Opcode::Init, Opcode::OpenRead, Opcode::Rewind, Opcode::Add]
            ),
            "expected Add opcode for expression evaluation"
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

    // === Test 9: SELECT with indexed predicate ===
    #[test]
    fn test_codegen_select_with_index() {
        let stmt = simple_select(&["a"], "t", Some(col_eq_param("b", 1)));
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Index-seek planning is intentionally disabled for now (see
        // codegen_select comment near `index_eq`), so this should compile to
        // a full table scan even when an index exists on the filtered column.
        let open_reads = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::OpenRead)
            .count();
        assert_eq!(
            open_reads, 1,
            "index seek disabled: only table cursor should open"
        );

        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Rewind),
            "expected full scan to rewind the table cursor"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Next),
            "expected full scan loop to advance with Next"
        );
        assert!(
            !prog.ops().iter().any(|op| op.opcode == Opcode::SeekGE),
            "index seek is disabled: no SeekGE expected"
        );
        assert!(
            !prog.ops().iter().any(|op| op.opcode == Opcode::IdxRowid),
            "index seek is disabled: no IdxRowid expected"
        );
        assert!(
            !prog.ops().iter().any(|op| op.opcode == Opcode::SeekRowid),
            "index seek is disabled: no SeekRowid expected in this path"
        );
    }

    #[test]
    fn test_codegen_select_where_in_subquery_supported_without_rewrite() {
        let subquery = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(from_table("s")),
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
        let where_expr = Expr::In {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            set: InSet::Subquery(Box::new(subquery)),
            not: false,
            span: Span::ZERO,
        };
        let stmt = simple_select(&["a"], "t", Some(Box::new(where_expr)));

        let schema = test_schema_with_subquery_source();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let open_reads = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::OpenRead)
            .count();
        assert_eq!(open_reads, 2, "outer + probe OpenRead expected");
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Eq),
            "expected Eq comparison in probe scan"
        );
    }

    #[test]
    fn test_resolve_in_probe_source_subquery_supported() {
        let subquery = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(from_table("s")),
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
        let set = InSet::Subquery(Box::new(subquery));
        let schema = test_schema_with_subquery_source();
        assert!(super::resolve_in_probe_source(&set, &schema).is_some());
    }

    #[test]
    fn test_resolve_in_probe_source_table_supported() {
        let set = InSet::Table(QualifiedName::bare("s"));
        let schema = test_schema_with_subquery_source();
        assert!(super::resolve_in_probe_source(&set, &schema).is_some());
    }

    #[test]
    fn test_codegen_select_where_in_table_supported_without_rewrite() {
        let where_expr = Expr::In {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
            set: InSet::Table(QualifiedName::bare("s")),
            not: false,
            span: Span::ZERO,
        };
        let stmt = simple_select(&["a"], "t", Some(Box::new(where_expr)));

        let schema = test_schema_with_subquery_source();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let open_reads = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::OpenRead)
            .count();
        assert_eq!(open_reads, 2, "outer + probe OpenRead expected");
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Eq),
            "expected Eq comparison in probe scan"
        );
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

    // === Test 11: SELECT with LIMIT ===
    #[test]
    fn test_codegen_select_with_limit() {
        let stmt = star_select_with_limit("t", 10);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should contain Integer (for limit), DecrJumpZero (for countdown).
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Integer,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::DecrJumpZero,
                Opcode::Next,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // DecrJumpZero p1 should be the limit register.
        let djz = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::DecrJumpZero)
            .expect("must have DecrJumpZero");
        assert!(djz.p1 >= 1, "limit register must be allocated");
    }

    // === Test 12: SELECT with LIMIT and OFFSET ===
    #[test]
    fn test_codegen_select_with_limit_offset() {
        let stmt = star_select_with_limit_offset("t", 5, 3);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have both IfPos (offset skip) and DecrJumpZero (limit).
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Integer, // limit value
                Opcode::Integer, // offset value
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::IfPos, // offset countdown
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::DecrJumpZero, // limit countdown
                Opcode::Next,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // Verify IfPos p3 == 1 (decrement by 1).
        let ifpos = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::IfPos)
            .expect("must have IfPos");
        assert_eq!(ifpos.p3, 1, "IfPos should decrement offset by 1");
    }

    // === Test 13: SELECT without LIMIT has no DecrJumpZero ===
    #[test]
    fn test_codegen_select_no_limit_no_decr() {
        let stmt = star_select("t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Without LIMIT, there should be no DecrJumpZero.
        let djz_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::DecrJumpZero)
            .count();
        assert_eq!(djz_count, 0, "no DecrJumpZero without LIMIT");

        // And no IfPos either.
        let ifpos_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::IfPos)
            .count();
        assert_eq!(ifpos_count, 0, "no IfPos without OFFSET");
    }

    // === Test 14: LIMIT labels properly resolved ===
    #[test]
    fn test_codegen_select_limit_labels_resolved() {
        let stmt = star_select_with_limit_offset("t", 10, 5);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // All jump targets should be valid addresses.
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

    // ── ORDER BY test helpers ──

    fn star_select_order_by(table: &str, col: &str, desc: bool) -> SelectStatement {
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
            order_by: vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::bare(col), Span::ZERO),
                direction: if desc {
                    Some(SortDirection::Desc)
                } else {
                    None
                },
                nulls: None,
            }],
            limit: None,
        }
    }

    fn select_col_order_by(
        table: &str,
        select_col: &str,
        order_col: &str,
        desc: bool,
    ) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare(select_col), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(from_table(table)),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::bare(order_col), Span::ZERO),
                direction: if desc {
                    Some(SortDirection::Desc)
                } else {
                    None
                },
                nulls: None,
            }],
            limit: None,
        }
    }

    fn star_select_order_by_with_limit(table: &str, col: &str, limit: i64) -> SelectStatement {
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
            order_by: vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::bare(col), Span::ZERO),
                direction: None,
                nulls: None,
            }],
            limit: Some(LimitClause {
                limit: Expr::Literal(Literal::Integer(limit), Span::ZERO),
                offset: None,
            }),
        }
    }

    fn star_select_order_by_expr(table: &str, expr: Expr) -> SelectStatement {
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
            order_by: vec![OrderingTerm {
                expr,
                direction: None,
                nulls: None,
            }],
            limit: None,
        }
    }

    // === Test 15: SELECT with ORDER BY ===
    #[test]
    fn test_codegen_select_order_by() {
        let stmt = star_select_order_by("t", "a", false);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Two-pass pattern: SorterOpen, OpenRead, scan loop, SorterSort, output loop.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::SorterOpen,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::MakeRecord,
                Opcode::SorterInsert,
                Opcode::Next,
                Opcode::Close,
                Opcode::SorterSort,
                Opcode::SorterData,
                Opcode::Column,
                Opcode::ResultRow,
                Opcode::SorterNext,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // SorterOpen p2 should be 1 (one sort key column).
        let so = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::SorterOpen)
            .unwrap();
        assert_eq!(so.p2, 1, "SorterOpen should have 1 key column");
    }

    // === Test 16: SELECT ORDER BY DESC ===
    #[test]
    fn test_codegen_select_order_by_desc() {
        let stmt = star_select_order_by("t", "b", true);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have SorterOpen with sort order in P4.
        let so = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::SorterOpen)
            .unwrap();
        assert_eq!(so.p2, 1, "SorterOpen should have 1 key column");
        // P4 should contain the '-' (DESC) sort order.
        assert!(
            matches!(&so.p4, P4::Str(s) if s == "-"),
            "SorterOpen P4 should be '-' for DESC, got {:?}",
            so.p4
        );
    }

    #[test]
    fn test_codegen_select_order_by_uses_index_without_sorter() {
        let stmt = star_select_order_by("t", "b", false);
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // ORDER BY on indexed column should stream via index cursor.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::OpenRead, // table
                Opcode::OpenRead, // index
                Opcode::Rewind,
                Opcode::IdxRowid,
                Opcode::SeekRowid,
                Opcode::ResultRow,
                Opcode::Next,
                Opcode::Halt,
            ]
        ));
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::OpenRead
                && matches!(&op.p4, P4::Index(name) if name == "idx_t_b")),
            "expected index cursor open for ORDER BY optimization"
        );

        let sorter_count = prog
            .ops()
            .iter()
            .filter(|op| {
                matches!(
                    op.opcode,
                    Opcode::SorterOpen
                        | Opcode::SorterInsert
                        | Opcode::SorterSort
                        | Opcode::SorterData
                        | Opcode::SorterNext
                )
            })
            .count();
        assert_eq!(
            sorter_count, 0,
            "index-assisted ORDER BY should bypass sorter"
        );
    }

    #[test]
    fn test_codegen_select_order_by_desc_uses_index_reverse_scan_without_sorter() {
        let stmt = star_select_order_by("t", "b", true);
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::OpenRead, // table
                Opcode::OpenRead, // index
                Opcode::Last,
                Opcode::IdxRowid,
                Opcode::SeekRowid,
                Opcode::ResultRow,
                Opcode::Prev,
                Opcode::Halt,
            ]
        ));
        assert!(
            !prog.ops().iter().any(|op| {
                matches!(
                    op.opcode,
                    Opcode::SorterOpen
                        | Opcode::SorterInsert
                        | Opcode::SorterSort
                        | Opcode::SorterData
                        | Opcode::SorterNext
                )
            }),
            "descending index-assisted ORDER BY should bypass sorter"
        );
    }

    #[test]
    fn test_codegen_select_covering_order_by_skips_table_lookup() {
        let stmt = select_col_order_by("t", "b", "b", false);
        let schema = test_schema_with_index();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let table_open_count = prog
            .ops()
            .iter()
            .filter(|op| {
                op.opcode == Opcode::OpenRead && matches!(&op.p4, P4::Table(name) if name == "t")
            })
            .count();
        assert_eq!(
            table_open_count, 0,
            "covering ORDER BY path should not open table cursor"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::OpenRead
                && matches!(&op.p4, P4::Index(name) if name == "idx_t_b")),
            "covering ORDER BY path should open index cursor"
        );
        assert!(
            !prog.ops().iter().any(|op| op.opcode == Opcode::SeekRowid),
            "covering ORDER BY path should not perform table rowid lookups"
        );
        assert!(
            !prog.ops().iter().any(|op| {
                matches!(
                    op.opcode,
                    Opcode::SorterOpen
                        | Opcode::SorterInsert
                        | Opcode::SorterSort
                        | Opcode::SorterData
                        | Opcode::SorterNext
                )
            }),
            "covering ORDER BY path should bypass sorter"
        );
    }

    // === Test 17: SELECT ORDER BY + LIMIT ===
    #[test]
    fn test_codegen_select_order_by_with_limit() {
        let stmt = star_select_order_by_with_limit("t", "a", 5);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have SorterSort + DecrJumpZero (LIMIT on sorted output).
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::SorterOpen,
                Opcode::SorterSort,
                Opcode::SorterData,
                Opcode::ResultRow,
                Opcode::DecrJumpZero,
                Opcode::SorterNext,
            ]
        ));

        // Integer for LIMIT should appear after scan pass.
        let integers: Vec<_> = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Integer)
            .collect();
        assert!(
            integers.iter().any(|op| op.p1 == 5),
            "should have Integer with limit value 5"
        );
    }

    // === Test 17b: ORDER BY arithmetic expression ===
    #[test]
    fn test_codegen_select_order_by_arithmetic_expression() {
        let stmt = star_select_order_by_expr(
            "t",
            Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: AstBinaryOp::Add,
                right: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                span: Span::ZERO,
            },
        );
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::SorterOpen,
                Opcode::OpenRead,
                Opcode::Add,
                Opcode::SorterInsert,
                Opcode::SorterSort,
                Opcode::ResultRow,
            ]
        ));
    }

    // === Test 17c: ORDER BY scalar function expression ===
    #[test]
    fn test_codegen_select_order_by_function_expression() {
        let stmt = star_select_order_by_expr(
            "t",
            Expr::FunctionCall {
                name: "length".to_owned(),
                args: FunctionArgs::List(vec![Expr::Column(ColumnRef::bare("b"), Span::ZERO)]),
                distinct: false,
                filter: None,
                over: None,
                span: Span::ZERO,
            },
        );
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(has_opcodes(
            &prog,
            &[
                Opcode::SorterOpen,
                Opcode::OpenRead,
                Opcode::PureFunc,
                Opcode::SorterInsert,
                Opcode::SorterSort,
                Opcode::ResultRow,
            ]
        ));
    }

    // === Test 18: ORDER BY no sorter without ORDER BY ===
    #[test]
    fn test_codegen_select_no_order_by_no_sorter() {
        let stmt = star_select("t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Without ORDER BY, there should be no sorter opcodes.
        let sorter_count = prog
            .ops()
            .iter()
            .filter(|op| {
                matches!(
                    op.opcode,
                    Opcode::SorterOpen
                        | Opcode::SorterInsert
                        | Opcode::SorterSort
                        | Opcode::SorterData
                        | Opcode::SorterNext
                )
            })
            .count();
        assert_eq!(sorter_count, 0, "no sorter opcodes without ORDER BY");
    }

    // === Test 19: ORDER BY labels properly resolved ===
    #[test]
    fn test_codegen_select_order_by_labels_resolved() {
        let stmt = star_select_order_by("t", "a", false);
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // All jump targets should be valid addresses.
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

        // SorterNext p2 should point to SorterData (within bounds).
        let sn = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::SorterNext)
            .unwrap();
        let target_index = usize::try_from(sn.p2).unwrap();
        let target_op = &prog.ops()[target_index];
        assert_eq!(
            target_op.opcode,
            Opcode::SorterData,
            "SorterNext should jump back to SorterData"
        );
    }

    // === Test: SELECT ORDER BY expression (a + 1) ===
    #[test]
    fn test_codegen_select_order_by_expression() {
        // SELECT * FROM t ORDER BY a + 1
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Star],
                    from: Some(from_table("t")),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![OrderingTerm {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                    op: fsqlite_ast::BinaryOp::Add,
                    right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                    span: Span::ZERO,
                },
                direction: None,
                nulls: None,
            }],
            limit: None,
        };
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should use the sorter (two-pass pattern).
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::SorterOpen,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::SorterInsert,
                Opcode::Next,
                Opcode::SorterSort,
                Opcode::SorterData,
                Opcode::ResultRow,
                Opcode::SorterNext,
                Opcode::Halt,
            ]
        ));

        // The sort key is an expression, so we should see an Add opcode
        // in the first pass (before SorterInsert).
        let sorter_insert_idx = prog
            .ops()
            .iter()
            .position(|op| op.opcode == Opcode::SorterInsert)
            .unwrap();
        let has_add_before_sorter = prog.ops()[..sorter_insert_idx]
            .iter()
            .any(|op| op.opcode == Opcode::Add);
        assert!(
            has_add_before_sorter,
            "expression ORDER BY should emit Add before SorterInsert"
        );
    }

    // ── Aggregate test helpers ──

    /// Build `SELECT count(*) FROM table`.
    fn agg_count_star(table: &str) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::FunctionCall {
                            name: "count".to_owned(),
                            args: FunctionArgs::Star,
                            distinct: false,
                            filter: None,
                            over: None,
                            span: Span::ZERO,
                        },
                        alias: None,
                    }],
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

    /// Build `SELECT func(col) FROM table`.
    fn agg_func_col(func: &str, col: &str, table: &str) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::FunctionCall {
                            name: func.to_owned(),
                            args: FunctionArgs::List(vec![Expr::Column(
                                ColumnRef::bare(col),
                                Span::ZERO,
                            )]),
                            distinct: false,
                            filter: None,
                            over: None,
                            span: Span::ZERO,
                        },
                        alias: None,
                    }],
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

    /// Build `SELECT count(*), sum(col) FROM table`.
    fn agg_count_star_and_sum(col: &str, table: &str) -> SelectStatement {
        SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: Expr::FunctionCall {
                                name: "count".to_owned(),
                                args: FunctionArgs::Star,
                                distinct: false,
                                filter: None,
                                over: None,
                                span: Span::ZERO,
                            },
                            alias: None,
                        },
                        ResultColumn::Expr {
                            expr: Expr::FunctionCall {
                                name: "sum".to_owned(),
                                args: FunctionArgs::List(vec![Expr::Column(
                                    ColumnRef::bare(col),
                                    Span::ZERO,
                                )]),
                                distinct: false,
                                filter: None,
                                over: None,
                                span: Span::ZERO,
                            },
                            alias: None,
                        },
                    ],
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

    // === Test 20: SELECT count(*) ===
    #[test]
    fn test_codegen_select_count_star() {
        let stmt = agg_count_star("t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have: Init, Transaction, Null (accum init), OpenRead,
        // Rewind, AggStep, Next, AggFinal, ResultRow, Close, Halt.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::Null,
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::AggStep,
                Opcode::Next,
                Opcode::AggFinal,
                Opcode::ResultRow,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));

        // ResultRow should cover 1 column.
        let rr = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::ResultRow)
            .unwrap();
        assert_eq!(rr.p2, 1, "count(*) produces 1 result column");

        // AggStep should have P4 = FuncName("count").
        let step = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::AggStep)
            .unwrap();
        assert!(
            matches!(&step.p4, P4::FuncName(f) if f == "count"),
            "AggStep P4 should be FuncName(count), got {:?}",
            step.p4
        );
    }

    // === Test 21: SELECT sum(col) ===
    #[test]
    fn test_codegen_select_sum_col() {
        let stmt = agg_func_col("sum", "a", "t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have Column (read arg) + AggStep in the loop.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::OpenRead,
                Opcode::Rewind,
                Opcode::Column,
                Opcode::AggStep,
                Opcode::Next,
                Opcode::AggFinal,
                Opcode::ResultRow,
            ]
        ));

        // AggStep p5 = 1 (one argument).
        let step = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::AggStep)
            .unwrap();
        assert_eq!(step.p5, 1, "sum(col) should have p5=1 (one arg)");

        // AggFinal P4 should be FuncName("sum").
        let fin = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::AggFinal)
            .unwrap();
        assert!(
            matches!(&fin.p4, P4::FuncName(f) if f == "sum"),
            "AggFinal P4 should be FuncName(sum), got {:?}",
            fin.p4
        );
    }

    // === Test 22: SELECT count(*), sum(a) ===
    #[test]
    fn test_codegen_select_multiple_aggregates() {
        let stmt = agg_count_star_and_sum("a", "t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have two AggStep and two AggFinal.
        let step_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::AggStep)
            .count();
        assert_eq!(step_count, 2, "two aggregates = two AggStep");

        let final_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::AggFinal)
            .count();
        assert_eq!(final_count, 2, "two aggregates = two AggFinal");

        // ResultRow should cover 2 columns.
        let rr = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::ResultRow)
            .unwrap();
        assert_eq!(rr.p2, 2, "two aggregate columns");

        // Verify function names in order: count then sum.
        let steps: Vec<_> = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::AggStep)
            .collect();
        assert!(matches!(&steps[0].p4, P4::FuncName(f) if f == "count"));
        assert!(matches!(&steps[1].p4, P4::FuncName(f) if f == "sum"));
    }

    // === Test 23: Non-aggregate SELECT does not emit AggStep ===
    #[test]
    fn test_codegen_select_no_agg_no_aggstep() {
        let stmt = star_select("t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let agg_count = prog
            .ops()
            .iter()
            .filter(|op| {
                matches!(
                    op.opcode,
                    Opcode::AggStep | Opcode::AggFinal | Opcode::AggValue
                )
            })
            .count();
        assert_eq!(agg_count, 0, "no aggregate opcodes in non-aggregate SELECT");
    }

    // === Test 24: Aggregate labels properly resolved ===
    #[test]
    fn test_codegen_select_aggregate_labels_resolved() {
        let stmt = agg_count_star("t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

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

    // === Test 25: Mixed aggregate + non-aggregate rejected ===
    #[test]
    fn test_codegen_select_mixed_agg_rejected() {
        // SELECT count(*), a FROM t — should be rejected (no GROUP BY).
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: Expr::FunctionCall {
                                name: "count".to_owned(),
                                args: FunctionArgs::Star,
                                distinct: false,
                                filter: None,
                                over: None,
                                span: Span::ZERO,
                            },
                            alias: None,
                        },
                        ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare("a"), Span::ZERO),
                            alias: None,
                        },
                    ],
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
            .expect_err("mixed aggregate/non-aggregate should fail");
        assert!(
            matches!(&err, CodegenError::Unsupported(msg) if msg.contains("mixed")),
            "error should mention mixed columns, got: {err}"
        );
    }

    // === Test 26: AVG aggregate ===
    #[test]
    fn test_codegen_select_avg() {
        let stmt = agg_func_col("avg", "a", "t");
        let schema = test_schema();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // AggStep P4 should be "avg".
        let step = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::AggStep)
            .unwrap();
        assert!(
            matches!(&step.p4, P4::FuncName(f) if f == "avg"),
            "AggStep P4 should be FuncName(avg), got {:?}",
            step.p4
        );

        // AggFinal should also be "avg".
        let fin = prog
            .ops()
            .iter()
            .find(|op| op.opcode == Opcode::AggFinal)
            .unwrap();
        assert!(
            matches!(&fin.p4, P4::FuncName(f) if f == "avg"),
            "AggFinal P4 should be FuncName(avg), got {:?}",
            fin.p4
        );
    }

    // === Test: GROUP BY with HAVING clause ===
    #[test]
    fn test_codegen_select_group_by_having() {
        // SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare("a"), Span::ZERO),
                            alias: None,
                        },
                        ResultColumn::Expr {
                            expr: Expr::FunctionCall {
                                name: "count".to_owned(),
                                args: FunctionArgs::Star,
                                distinct: false,
                                filter: None,
                                over: None,
                                span: Span::ZERO,
                            },
                            alias: None,
                        },
                    ],
                    from: Some(from_table("t")),
                    where_clause: None,
                    group_by: vec![Expr::Column(ColumnRef::bare("a"), Span::ZERO)],
                    having: Some(Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::FunctionCall {
                            name: "count".to_owned(),
                            args: FunctionArgs::Star,
                            distinct: false,
                            filter: None,
                            over: None,
                            span: Span::ZERO,
                        }),
                        op: AstBinaryOp::Gt,
                        right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                        span: Span::ZERO,
                    })),
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
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should produce GROUP BY with HAVING filter: SorterOpen, AggStep,
        // AggFinal, IfNot (HAVING check), ResultRow.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::SorterOpen,
                Opcode::AggStep,
                Opcode::AggFinal,
                Opcode::IfNot, // HAVING filter
                Opcode::ResultRow,
                Opcode::Halt,
            ]
        ));

        // There should be IfNot opcodes (HAVING filter) in the program.
        let if_not_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::IfNot)
            .count();
        assert!(
            if_not_count >= 1,
            "HAVING should generate at least one IfNot, got {if_not_count}"
        );
    }

    // === Test: GROUP BY with FILTER clause emits IfNot ===
    #[test]
    fn test_codegen_select_group_by_filter_emits_ifnot() {
        // SELECT a, count(*) FILTER (WHERE b > 0) FROM t GROUP BY a
        let stmt = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![
                        ResultColumn::Expr {
                            expr: Expr::Column(ColumnRef::bare("a"), Span::ZERO),
                            alias: None,
                        },
                        ResultColumn::Expr {
                            expr: Expr::FunctionCall {
                                name: "count".to_owned(),
                                args: FunctionArgs::Star,
                                distinct: false,
                                filter: Some(Box::new(Expr::BinaryOp {
                                    left: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                                    op: AstBinaryOp::Gt,
                                    right: Box::new(Expr::Literal(Literal::Integer(0), Span::ZERO)),
                                    span: Span::ZERO,
                                })),
                                over: None,
                                span: Span::ZERO,
                            },
                            alias: None,
                        },
                    ],
                    from: Some(from_table("t")),
                    where_clause: None,
                    group_by: vec![Expr::Column(ColumnRef::bare("a"), Span::ZERO)],
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
        codegen_select(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // There should be an IfNot opcode BEFORE AggStep (the FILTER check).
        let agg_step_positions: Vec<usize> = prog
            .ops()
            .iter()
            .enumerate()
            .filter(|(_, op)| op.opcode == Opcode::AggStep)
            .map(|(i, _)| i)
            .collect();

        // There should be at least one AggStep (in the sort iteration loop).
        assert!(
            !agg_step_positions.is_empty(),
            "GROUP BY FILTER should have AggStep"
        );

        // There should be IfNot before AggStep (FILTER check).
        let if_not_before_agg = prog.ops().iter().enumerate().any(|(i, op)| {
            op.opcode == Opcode::IfNot
                && agg_step_positions
                    .iter()
                    .any(|&as_pos| i < as_pos && as_pos - i <= 5)
        });
        assert!(
            if_not_before_agg,
            "GROUP BY FILTER should emit IfNot before AggStep"
        );
    }

    // === Tests for bd-2vza: UPDATE/DELETE WHERE with qualified alias columns ===

    #[test]
    fn test_codegen_update_where_qualified_alias() {
        // UPDATE t AS u SET b = ?1 WHERE u.a = ?2
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: Some("u".to_owned()),
                index_hint: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("u", "a"), Span::ZERO)),
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

        // The qualified alias "u.a" should resolve to Column opcode
        // for filter comparison (Ne), not silently skip filtering.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Rewind,
                Opcode::Column, // read u.a for WHERE comparison
                Opcode::Variable,
                Opcode::Ne, // filter non-matching rows
            ]
        ));
    }

    #[test]
    fn test_codegen_delete_where_qualified_alias() {
        // DELETE FROM t AS u WHERE u.a = ?1
        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: Some("u".to_owned()),
                index_hint: None,
            },
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("u", "a"), Span::ZERO)),
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

        // The qualified alias "u.a" should resolve correctly.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Last,
                Opcode::Column, // read u.a for WHERE comparison
                Opcode::Variable,
                Opcode::Ne, // filter non-matching rows
                Opcode::Delete,
            ]
        ));
    }

    #[test]
    fn test_codegen_update_where_qualified_rowid_alias() {
        // UPDATE t AS u SET b = ?1 WHERE u.rowid = ?2
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: Some("u".to_owned()),
                index_hint: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("b".to_owned()),
                value: placeholder(1),
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::qualified("u", "rowid"), Span::ZERO)),
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

        // The qualified alias "u.rowid" should emit OP_Rowid for the
        // WHERE comparison, not silently skip the filter.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Rewind,
                Opcode::Rowid, // WHERE u.rowid comparison
                Opcode::Variable,
                Opcode::Ne, // filter non-matching rows
            ]
        ));
    }

    #[test]
    fn test_codegen_update_where_in_subquery_supported_without_rewrite() {
        // UPDATE t SET b = ?1 WHERE a IN (SELECT b FROM s)
        let subquery = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(from_table("s")),
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
            where_clause: Some(Expr::In {
                expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                set: InSet::Subquery(Box::new(subquery)),
                not: false,
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };

        let schema = test_schema_with_subquery_source();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::OpenRead && op.p2 == 3),
            "expected subquery probe OpenRead on root page 3"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Eq),
            "expected Eq comparison in IN probe scan"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Insert),
            "expected update writeback Insert"
        );
    }

    #[test]
    fn test_codegen_update_set_subquery_anonymous_placeholder_offsets_where_and_returning() {
        // UPDATE t
        // SET b = a IN (SELECT b FROM s WHERE b = ?)
        // WHERE a = ?
        // RETURNING ?
        //
        // SQL placeholder order: SET-subquery first, WHERE second.
        // Bytecode emission order is WHERE first, then SET; codegen must offset
        // WHERE placeholder numbering so WHERE uses parameter 2.
        let set_subquery = SelectStatement {
            with: None,
            body: SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(from_table("s")),
                    where_clause: Some(Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Column(ColumnRef::bare("b"), Span::ZERO)),
                        op: AstBinaryOp::Eq,
                        right: Box::new(Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO)),
                        span: Span::ZERO,
                    })),
                    group_by: vec![],
                    having: None,
                    windows: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        };
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
                value: Expr::In {
                    expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                    set: InSet::Subquery(Box::new(set_subquery)),
                    not: false,
                    span: Span::ZERO,
                },
            }],
            from: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                op: AstBinaryOp::Eq,
                right: Box::new(Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO)),
                span: Span::ZERO,
            }),
            returning: vec![ResultColumn::Expr {
                expr: Expr::Placeholder(PlaceholderType::Anonymous, Span::ZERO),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

        let schema = test_schema_with_subquery_source();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        let variable_params: Vec<i32> = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Variable)
            .map(|op| op.p1)
            .collect();
        assert_eq!(
            variable_params,
            vec![2, 1, 3],
            "placeholder numbering must follow SQL lexical order across WHERE (emitted first), SET, and RETURNING"
        );
    }

    #[test]
    fn test_codegen_delete_where_in_table_supported_without_rewrite() {
        // DELETE FROM t WHERE a IN s
        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
            },
            where_clause: Some(Expr::In {
                expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                set: InSet::Table(QualifiedName::bare("s")),
                not: false,
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };

        let schema = test_schema_with_subquery_source();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_delete(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::OpenRead && op.p2 == 3),
            "expected IN-table probe OpenRead on root page 3"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Eq),
            "expected Eq comparison in IN probe scan"
        );
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Delete),
            "expected delete operation"
        );
    }

    #[test]
    fn test_codegen_delete_where_not_in_subquery_with_order_by_limit() {
        // DELETE FROM t WHERE a NOT IN (SELECT b FROM s ORDER BY b LIMIT ?1)
        // Tests the complex IN subquery path with ORDER BY and LIMIT.
        let subquery = SelectStatement {
            with: None,
            body: fsqlite_ast::SelectBody {
                select: SelectCore::Select {
                    distinct: Distinctness::All,
                    columns: vec![ResultColumn::Expr {
                        expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                        alias: None,
                    }],
                    from: Some(fsqlite_ast::FromClause {
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
            order_by: vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::bare("b"), Span::ZERO),
                direction: None,
                nulls: None,
            }],
            limit: Some(LimitClause {
                limit: Expr::Placeholder(fsqlite_ast::PlaceholderType::Numbered(1), Span::ZERO),
                offset: None,
            }),
        };

        let stmt = DeleteStatement {
            with: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
            },
            where_clause: Some(Expr::In {
                expr: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                set: InSet::Subquery(Box::new(subquery)),
                not: true,
                span: Span::ZERO,
            }),
            returning: vec![],
            order_by: vec![],
            limit: None,
        };

        let schema = test_schema_with_subquery_source();
        let ctx = CodegenContext::default();
        let mut b = ProgramBuilder::new();
        codegen_delete(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // Should have SorterOpen for materializing the subquery with ORDER BY.
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::SorterOpen),
            "expected SorterOpen for ORDER BY subquery"
        );

        // Should have SorterSort to sort the materialized results.
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::SorterSort),
            "expected SorterSort opcode"
        );

        // Should have SorterInsert to populate the sorter.
        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::SorterInsert),
            "expected SorterInsert opcode"
        );

        // Should have DecrJumpZero for LIMIT handling.
        assert!(
            prog.ops()
                .iter()
                .any(|op| op.opcode == Opcode::DecrJumpZero),
            "expected DecrJumpZero for LIMIT"
        );

        // Should have Delete opcode for the deletion.
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Delete),
            "expected Delete opcode"
        );

        // Should have Variable opcode for the LIMIT parameter.
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Variable),
            "expected Variable opcode for LIMIT parameter"
        );
    }

    #[test]
    fn test_codegen_delete_where_bare_rowid_eq() {
        // DELETE FROM t WHERE rowid = ?1
        // Ensures unqualified rowid in Eq fast-path emits Rowid opcode.
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

        // Bare rowid in DELETE WHERE Eq should emit Rowid + Ne filter.
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Last,
                Opcode::Rowid, // WHERE rowid comparison
                Opcode::Variable,
                Opcode::Ne, // filter non-matching rows
                Opcode::Delete,
            ]
        ));
    }

    // === Test: UPDATE SET with column self-reference (bd-2eau) ===

    #[test]
    fn test_codegen_update_set_column_self_ref() {
        // UPDATE t SET a = a + 1
        // The SET expression `a + 1` should generate a Column opcode to read
        // the current value of `a`, NOT a Null opcode.
        let stmt = UpdateStatement {
            with: None,
            or_conflict: None,
            table: QualifiedTableRef {
                name: QualifiedName::bare("t"),
                alias: None,
                index_hint: None,
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::Column("a".to_owned()),
                value: Expr::BinaryOp {
                    left: Box::new(Expr::Column(ColumnRef::bare("a"), Span::ZERO)),
                    op: AstBinaryOp::Add,
                    right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
                    span: Span::ZERO,
                },
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
        codegen_update(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();

        // The SET expression should emit Column (reading `a`) + Integer(1) + Add,
        // NOT Null. Count Column opcodes — there should be at least 3:
        // 2 from reading all columns, plus 1+ from evaluating `a` in the expression.
        let column_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Column)
            .count();
        assert!(
            column_count >= 3,
            "expected >= 3 Column ops (2 for reading all cols + 1 for SET expr), got {column_count}"
        );

        // There should be an Add opcode for `a + 1`.
        assert!(
            prog.ops().iter().any(|op| op.opcode == Opcode::Add),
            "expected Add opcode for `a + 1` expression"
        );

        // There should be NO Null opcodes for column references.
        // (Null would only appear if the ScanCtx was missing.)
        let null_count = prog
            .ops()
            .iter()
            .filter(|op| op.opcode == Opcode::Null)
            .count();
        assert_eq!(
            null_count, 0,
            "expected 0 Null opcodes (column refs should resolve), got {null_count}"
        );
    }

    // =================================================================
    // IPK codegen tests (bd-3l6e / PARITY-B5)
    // =================================================================

    /// INSERT VALUES with IPK column should emit IsNull+Copy routing, NOT
    /// unconditional NewRowid.
    #[test]
    fn test_codegen_insert_values_ipk_uses_copy_not_new_rowid() {
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
        let schema = schema_with_ipk_alias();
        let ctx = CodegenContext {
            concurrent_mode: false,
            rowid_alias_col_idx: Some(0),
        };
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = opcode_sequence(&prog);

        // Must contain IsNull (conditional branch) and Copy (value→rowid).
        assert!(
            ops.contains(&Opcode::IsNull),
            "IPK INSERT should emit IsNull to check for NULL IPK value"
        );
        assert!(
            ops.contains(&Opcode::Copy),
            "IPK INSERT should emit Copy to move IPK value to rowid register"
        );
        // The sequence must be: Variable (values) → IsNull → Copy → Goto
        //                       (or the NULL path: NewRowid → Copy)
        assert!(has_opcodes(
            &prog,
            &[
                Opcode::Init,
                Opcode::Transaction,
                Opcode::OpenWrite,
                Opcode::Variable,
                Opcode::Variable,
                Opcode::IsNull,
                Opcode::Copy,
                Opcode::MakeRecord,
                Opcode::Insert,
                Opcode::Close,
                Opcode::Halt,
            ]
        ));
    }

    /// INSERT VALUES without IPK should still use unconditional NewRowid.
    #[test]
    fn test_codegen_insert_values_no_ipk_uses_new_rowid() {
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
        let schema = test_schema(); // no IPK
        let ctx = CodegenContext::default(); // rowid_alias_col_idx = None
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = opcode_sequence(&prog);

        assert!(
            ops.contains(&Opcode::NewRowid),
            "non-IPK INSERT should use NewRowid"
        );
        assert!(
            !ops.contains(&Opcode::IsNull),
            "non-IPK INSERT should NOT emit IsNull routing"
        );
    }

    /// INSERT with explicit column list where IPK is in non-first position.
    #[test]
    fn test_codegen_insert_values_ipk_column_list_reorder() {
        // Table: (a INTEGER PRIMARY KEY, b TEXT)
        // INSERT INTO t(b, a) VALUES (?, ?)  →  IPK is at VALUES position 1
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec!["b".to_owned(), "a".to_owned()],
            source: InsertSource::Values(vec![vec![placeholder(1), placeholder(2)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = schema_with_ipk_alias();
        let ctx = CodegenContext {
            concurrent_mode: false,
            rowid_alias_col_idx: Some(0), // IPK is column 0 in schema
        };
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = opcode_sequence(&prog);

        // Should still emit IPK routing (IsNull + Copy) because 'a' is in
        // the column list at position 1.
        assert!(
            ops.contains(&Opcode::IsNull),
            "reordered column list with IPK should emit IsNull"
        );
    }

    /// INSERT with explicit column list that OMITS the IPK column.
    /// The reorder fills the IPK position with NULL, so IsNull routing is
    /// emitted but always takes the auto-generate path.
    #[test]
    fn test_codegen_insert_values_ipk_column_list_omitted() {
        // Table: (a INTEGER PRIMARY KEY, b TEXT)
        // INSERT INTO t(b) VALUES (?)  →  IPK omitted, reorder fills NULL
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec!["b".to_owned()],
            source: InsertSource::Values(vec![vec![placeholder(1)]]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = schema_with_ipk_alias();
        let ctx = CodegenContext {
            concurrent_mode: false,
            rowid_alias_col_idx: Some(0),
        };
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = opcode_sequence(&prog);

        // Reorder fills IPK slot with NULL → IPK routing still emitted
        // (IsNull will always fire, triggering NewRowid).
        assert!(
            ops.contains(&Opcode::NewRowid),
            "omitted IPK should use NewRowid"
        );
        // The reordered row has 2 columns (full table width), not 1.
        let n_null = ops.iter().filter(|&&op| op == Opcode::Null).count();
        assert!(
            n_null >= 1,
            "reorder should emit Null for the omitted IPK column"
        );
    }

    /// Multi-row VALUES with IPK should emit IPK routing for each row.
    #[test]
    fn test_codegen_insert_values_ipk_multi_row() {
        let stmt = InsertStatement {
            with: None,
            or_conflict: None,
            table: QualifiedName::bare("t"),
            alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![
                vec![placeholder(1), placeholder(2)],
                vec![placeholder(3), placeholder(4)],
                vec![placeholder(5), placeholder(6)],
            ]),
            upsert: vec![],
            returning: vec![],
        };
        let schema = schema_with_ipk_alias();
        let ctx = CodegenContext {
            concurrent_mode: false,
            rowid_alias_col_idx: Some(0),
        };
        let mut b = ProgramBuilder::new();
        codegen_insert(&mut b, &stmt, &schema, &ctx).unwrap();
        let prog = b.finish().unwrap();
        let ops = opcode_sequence(&prog);

        // Three rows → three IsNull opcodes (one per row).
        let is_null_count = ops.iter().filter(|&&op| op == Opcode::IsNull).count();
        assert_eq!(
            is_null_count, 3,
            "3-row INSERT with IPK should emit 3 IsNull opcodes, got {is_null_count}"
        );
    }
}
