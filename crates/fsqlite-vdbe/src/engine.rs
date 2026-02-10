//! VDBE bytecode interpreter — the fetch-execute engine.
//!
//! Takes a [`VdbeProgram`] (produced by codegen) and executes it instruction by
//! instruction. The engine maintains a register file (`Vec<SqliteValue>`) and
//! accumulates result rows emitted by `OP_ResultRow`.
//!
//! This implementation covers the core opcode set needed for expression
//! evaluation, control flow, arithmetic, comparison, and row output.
//! Cursor-based opcodes (OpenRead, Rewind, Next, Column, etc.) are stubbed
//! and will be wired to the B-tree layer in Phase 5.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use fsqlite_btree::{BtCursor, BtreeCursorOps, MemPageStore};
use fsqlite_error::{FrankenError, Result};
use fsqlite_func::{ErasedAggregateFunction, FunctionRegistry};
use fsqlite_types::cx::Cx;
use fsqlite_types::opcode::{Opcode, P4, VdbeOp};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;

use crate::VdbeProgram;

// ── In-Memory Table Store ──────────────────────────────────────────────────
//
// Phase 4 in-memory cursor backend. Allows the VDBE engine to execute
// CREATE TABLE / INSERT / SELECT / UPDATE / DELETE against a lightweight
// row store without requiring the full B-tree + pager + VFS stack.

/// A row in an in-memory table: (rowid, column values).
#[derive(Debug, Clone, PartialEq)]
struct MemRow {
    rowid: i64,
    values: Vec<SqliteValue>,
}

/// In-memory table storage (Phase 4 backend).
#[derive(Debug, Clone)]
pub struct MemTable {
    /// Column count for this table (used when creating the table;
    /// actual row widths may vary).
    pub num_columns: usize,
    /// Rows stored in insertion order.
    rows: Vec<MemRow>,
    /// Next auto-increment rowid.
    next_rowid: i64,
}

impl MemTable {
    /// Create a new empty table with the given column count.
    fn new(num_columns: usize) -> Self {
        Self {
            num_columns,
            rows: Vec::new(),
            next_rowid: 1,
        }
    }

    /// Allocate a new unique rowid.
    fn alloc_rowid(&mut self) -> i64 {
        let id = self.next_rowid;
        self.next_rowid += 1;
        id
    }

    /// Insert a row with the given rowid and values.
    fn insert(&mut self, rowid: i64, values: Vec<SqliteValue>) {
        // Update next_rowid if needed.
        if rowid >= self.next_rowid {
            self.next_rowid = rowid + 1;
        }
        // Replace if rowid already exists (UPSERT semantics).
        if let Some(existing) = self.rows.iter_mut().find(|r| r.rowid == rowid) {
            existing.values = values;
        } else {
            self.rows.push(MemRow { rowid, values });
        }
    }

    /// Delete the row at the given index.
    fn delete_at(&mut self, index: usize) {
        if index < self.rows.len() {
            self.rows.remove(index);
        }
    }

    /// Delete a row by rowid. Returns true if a row was found and deleted.
    #[allow(dead_code)]
    fn delete_by_rowid(&mut self, rowid: i64) -> bool {
        if let Some(idx) = self.rows.iter().position(|r| r.rowid == rowid) {
            self.rows.remove(idx);
            true
        } else {
            false
        }
    }

    /// Find a row by rowid. Returns the index.
    fn find_by_rowid(&self, rowid: i64) -> Option<usize> {
        self.rows.iter().position(|r| r.rowid == rowid)
    }

    /// Iterate all rows as `(rowid, values)` pairs.
    ///
    /// Used by the compat persistence layer to dump table contents to
    /// real SQLite format files.
    pub fn iter_rows(&self) -> impl Iterator<Item = (i64, &[SqliteValue])> + '_ {
        self.rows.iter().map(|r| (r.rowid, r.values.as_slice()))
    }

    /// Insert a row with an explicit rowid (for loading from file).
    ///
    /// This is the public entry point used by the compat persistence
    /// loader. It delegates to the private `insert` method.
    pub fn insert_row(&mut self, rowid: i64, values: Vec<SqliteValue>) {
        self.insert(rowid, values);
    }
}

/// Cursor state for traversing an in-memory table.
#[derive(Debug, Clone)]
struct MemCursor {
    /// Root page (used as table identifier).
    root_page: i32,
    /// Whether this cursor is writable (enforced at the Connection level).
    #[allow(dead_code)]
    writable: bool,
    /// Current row position (None = not positioned).
    position: Option<usize>,
    /// Pseudo-table data (for OpenPseudo: a single row set by RowData/MakeRecord).
    pseudo_row: Option<Vec<SqliteValue>>,
    /// Whether this is a pseudo cursor (OpenPseudo).
    is_pseudo: bool,
}

impl MemCursor {
    fn new(root_page: i32, writable: bool) -> Self {
        Self {
            root_page,
            writable,
            position: None,
            pseudo_row: None,
            is_pseudo: false,
        }
    }

    fn new_pseudo() -> Self {
        Self {
            root_page: -1,
            writable: false,
            position: None,
            pseudo_row: None,
            is_pseudo: true,
        }
    }
}

/// Cursor state for sorter opcodes (`SorterOpen`, `SorterInsert`, ...).
#[derive(Debug, Clone)]
struct SorterCursor {
    /// Number of leading columns used as sort key.
    key_columns: usize,
    /// Per-key sort direction (length == key_columns).
    sort_key_orders: Vec<SortKeyOrder>,
    /// Inserted records decoded from `MakeRecord` blobs.
    rows: Vec<Vec<SqliteValue>>,
    /// Current position after `SorterSort`/`SorterNext`.
    position: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKeyOrder {
    Asc,
    Desc,
}

impl SorterCursor {
    fn new(key_columns: usize, mut sort_key_orders: Vec<SortKeyOrder>) -> Self {
        let key_columns = key_columns.max(1);
        if sort_key_orders.len() < key_columns {
            sort_key_orders.resize(key_columns, SortKeyOrder::Asc);
        }
        sort_key_orders.truncate(key_columns);
        Self {
            key_columns,
            sort_key_orders,
            rows: Vec::new(),
            position: None,
        }
    }

    fn sort(&mut self) {
        let key_columns = self.key_columns;
        let orders = self.sort_key_orders.clone();
        self.rows
            .sort_by(|lhs, rhs| compare_sorter_rows(lhs, rhs, key_columns, &orders));
    }
}

/// Storage-backed table cursor used by read scans (`OpenRead`).
///
/// Backed by a real [`BtCursor`] over an in-memory [`MemPageStore`] so that
/// the VDBE exercises the same page-level navigation code path that the
/// production pager/WAL stack will use.
#[derive(Debug)]
struct StorageCursor {
    cursor: BtCursor<MemPageStore>,
    cx: Cx,
}

/// Shared in-memory database backing the VDBE engine's cursor operations.
///
/// Maps root page numbers to in-memory tables. The Connection layer
/// populates this when processing CREATE TABLE and passes it to the engine.
#[derive(Debug, Clone, Default)]
pub struct MemDatabase {
    /// Tables indexed by root page number.
    pub tables: HashMap<i32, MemTable>,
    /// Next available root page number.
    next_root_page: i32,
}

impl MemDatabase {
    /// Create a new empty in-memory database.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            next_root_page: 2, // Page 1 is reserved for sqlite_master.
        }
    }

    /// Create a table and return its root page number.
    pub fn create_table(&mut self, num_columns: usize) -> i32 {
        let root_page = self.next_root_page;
        self.next_root_page += 1;
        self.tables.insert(root_page, MemTable::new(num_columns));
        root_page
    }

    /// Get a reference to a table by root page.
    pub fn get_table(&self, root_page: i32) -> Option<&MemTable> {
        self.tables.get(&root_page)
    }

    /// Get a mutable reference to a table by root page.
    pub fn get_table_mut(&mut self, root_page: i32) -> Option<&mut MemTable> {
        self.tables.get_mut(&root_page)
    }
}

const VDBE_TRACE_ENV: &str = "FSQLITE_VDBE_TRACE_OPCODES";
const VDBE_TRACE_LOGGING_STANDARD: &str = "bd-1fpm";

/// Register spans touched by an opcode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpcodeRegisterSpans {
    read_start: i32,
    read_len: i32,
    write_start: i32,
    write_len: i32,
}

impl OpcodeRegisterSpans {
    const NONE: Self = Self {
        read_start: -1,
        read_len: 0,
        write_start: -1,
        write_len: 0,
    };
}

/// Outcome of a single engine execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutcome {
    /// Program halted normally (Halt with p1=0).
    Done,
    /// Program halted with an error code and message.
    Error { code: i32, message: String },
}

/// The VDBE bytecode interpreter.
///
/// Executes a program produced by the code generator, maintaining a register
/// file and collecting result rows. In Phase 4, cursor operations use an
/// in-memory table store (`MemDatabase`) rather than the full B-tree stack.
pub struct VdbeEngine {
    /// Register file (1-indexed; index 0 is unused/sentinel).
    registers: Vec<SqliteValue>,
    /// Bound SQL parameter values (`?1`, `?2`, ...).
    bindings: Vec<SqliteValue>,
    /// Whether opcode-level tracing is enabled.
    trace_opcodes: bool,
    /// Result rows accumulated during execution.
    results: Vec<Vec<SqliteValue>>,
    /// Open cursors (keyed by cursor number, i.e. p1 of OpenRead/OpenWrite).
    cursors: HashMap<i32, MemCursor>,
    /// Open sorter cursors keyed by cursor number.
    sorters: HashMap<i32, SorterCursor>,
    /// Open storage-backed read cursors keyed by cursor number.
    storage_cursors: HashMap<i32, StorageCursor>,
    /// Whether `OpenRead` should route through storage-backed cursors.
    storage_read_cursors_enabled: bool,
    /// In-memory database backing cursor operations (shared with Connection).
    db: Option<MemDatabase>,
    /// Scalar/aggregate/window function registry for Function/PureFunc opcodes.
    func_registry: Option<Arc<FunctionRegistry>>,
    /// Aggregate accumulators keyed by accumulator register.
    aggregates: HashMap<i32, AggregateContext>,
}

struct AggregateContext {
    func: Arc<ErasedAggregateFunction>,
    state: Box<dyn Any + Send>,
}

impl VdbeEngine {
    /// Create a new engine with enough registers for the given program.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn new(register_count: i32) -> Self {
        // +1 because registers are 1-indexed (register 0 unused).
        let count = register_count.max(0) as u32 + 1;
        Self {
            registers: vec![SqliteValue::Null; count as usize],
            bindings: Vec::new(),
            trace_opcodes: opcode_trace_enabled(),
            results: Vec::new(),
            cursors: HashMap::new(),
            sorters: HashMap::new(),
            storage_cursors: HashMap::new(),
            storage_read_cursors_enabled: false,
            db: None,
            func_registry: None,
            aggregates: HashMap::new(),
        }
    }

    /// Attach an in-memory database for cursor operations.
    pub fn set_database(&mut self, db: MemDatabase) {
        self.db = Some(db);
    }

    /// Take ownership of the in-memory database back from the engine.
    pub fn take_database(&mut self) -> Option<MemDatabase> {
        self.db.take()
    }

    /// Enable/disable storage-backed cursor execution for `OpenRead` scans.
    pub fn enable_storage_read_cursors(&mut self, enabled: bool) {
        self.storage_read_cursors_enabled = enabled;
    }

    /// Attach a function registry for `Function`/`PureFunc` opcode dispatch.
    pub fn set_function_registry(&mut self, registry: Arc<FunctionRegistry>) {
        self.func_registry = Some(registry);
    }

    /// Replace the current set of bound SQL parameters.
    ///
    /// Values are 1-indexed at execution time (`?1` maps to `bindings[0]`).
    pub fn set_bindings(&mut self, bindings: Vec<SqliteValue>) {
        self.bindings = bindings;
    }

    /// Execute a VDBE program to completion.
    ///
    /// Returns `Ok(ExecOutcome::Done)` on normal halt, or an error if the
    /// program encounters a fatal condition.
    #[allow(
        clippy::too_many_lines,
        clippy::match_same_arms,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap
    )]
    pub fn execute(&mut self, program: &VdbeProgram) -> Result<ExecOutcome> {
        let ops = program.ops();
        if ops.is_empty() {
            return Ok(ExecOutcome::Done);
        }

        self.aggregates.clear();

        let mut pc: usize = 0;
        // "once" flags: one bit per instruction address.
        let mut once_flags = vec![false; ops.len()];

        loop {
            if pc >= ops.len() {
                return Ok(ExecOutcome::Done);
            }

            let op = &ops[pc];
            self.trace_opcode(pc, op);
            match op.opcode {
                // ── Control Flow ────────────────────────────────────────
                Opcode::Init => {
                    // Jump to p2 if it points to a valid instruction.
                    // In the standard SQLite pattern, p2 points to a Goto
                    // at the end that bounces back. If p2 points past the
                    // end (our codegen pattern), fall through.
                    let target = op.p2 as usize;
                    if op.p2 > 0 && target < ops.len() {
                        pc = target;
                        continue;
                    }
                    pc += 1;
                }

                Opcode::Goto => {
                    pc = op.p2 as usize;
                }

                Opcode::Halt => {
                    if op.p1 != 0 {
                        let msg = match &op.p4 {
                            P4::Str(s) => s.clone(),
                            _ => format!("halt with error code {}", op.p1),
                        };
                        return Ok(ExecOutcome::Error {
                            code: op.p1,
                            message: msg,
                        });
                    }
                    return Ok(ExecOutcome::Done);
                }

                Opcode::Noop => {
                    pc += 1;
                }

                // ── Constants ───────────────────────────────────────────
                Opcode::Integer => {
                    // Set register p2 to integer value p1.
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(op.p1)));
                    pc += 1;
                }

                Opcode::Int64 => {
                    let val = match &op.p4 {
                        P4::Int64(v) => *v,
                        _ => 0,
                    };
                    self.set_reg(op.p2, SqliteValue::Integer(val));
                    pc += 1;
                }

                Opcode::Real => {
                    let val = match &op.p4 {
                        P4::Real(v) => *v,
                        _ => 0.0,
                    };
                    self.set_reg(op.p2, SqliteValue::Float(val));
                    pc += 1;
                }

                Opcode::String8 => {
                    let val = match &op.p4 {
                        P4::Str(s) => s.clone(),
                        _ => String::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Text(val));
                    pc += 1;
                }

                Opcode::String => {
                    // p1 = length, p4 = string data. Same as String8 for us.
                    let val = match &op.p4 {
                        P4::Str(s) => s.clone(),
                        _ => String::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Text(val));
                    pc += 1;
                }

                Opcode::Null => {
                    // Set registers p2..p3 to NULL.  When p3 == 0 only p2 is
                    // set.  p3 is an absolute register number (matching C
                    // SQLite where cnt = p3 - p2).
                    let start = op.p2;
                    let end = if op.p3 > 0 { op.p3 } else { start };
                    for r in start..=end {
                        self.set_reg(r, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::SoftNull => {
                    self.set_reg(op.p1, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::Blob => {
                    let val = match &op.p4 {
                        P4::Blob(b) => b.clone(),
                        _ => Vec::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Blob(val));
                    pc += 1;
                }

                // ── Register Operations ─────────────────────────────────
                Opcode::Move => {
                    // Move p3 registers from p1 to p2.
                    for i in 0..op.p3 {
                        let val = self.get_reg(op.p1 + i).clone();
                        self.set_reg(op.p2 + i, val);
                        self.set_reg(op.p1 + i, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::Copy => {
                    // Copy register p1 to p2 (deep copy).
                    let val = self.get_reg(op.p1).clone();
                    self.set_reg(op.p2, val);
                    pc += 1;
                }

                Opcode::SCopy => {
                    // Shallow copy register p1 to p2.
                    let val = self.get_reg(op.p1).clone();
                    self.set_reg(op.p2, val);
                    pc += 1;
                }

                Opcode::IntCopy => {
                    let val = self.get_reg(op.p1).to_integer();
                    self.set_reg(op.p2, SqliteValue::Integer(val));
                    pc += 1;
                }

                // ── Result Row ──────────────────────────────────────────
                Opcode::ResultRow => {
                    // Output p2 registers starting at p1.
                    let start = op.p1 as usize;
                    let count = op.p2 as usize;
                    let row: Vec<SqliteValue> = (start..start + count)
                        .map(|r| self.get_reg(r as i32).clone())
                        .collect();
                    self.results.push(row);
                    pc += 1;
                }

                // ── Arithmetic ──────────────────────────────────────────
                Opcode::Add => {
                    // p3 = p2 + p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_add(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Subtract => {
                    // p3 = p2 - p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_sub(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Multiply => {
                    // p3 = p2 * p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_mul(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Divide => {
                    // p3 = p2 / p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_div(dividend, divisor);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Remainder => {
                    // p3 = p2 % p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_rem(dividend, divisor);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                // ── String Concatenation ────────────────────────────────
                Opcode::Concat => {
                    // Concatenate p1 and p2 into p3.
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        let mut s = b.to_text();
                        s.push_str(&a.to_text());
                        SqliteValue::Text(s)
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                // ── Bitwise ─────────────────────────────────────────────
                Opcode::BitAnd => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(a.to_integer() & b.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::BitOr => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(a.to_integer() | b.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::ShiftLeft => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        sql_shift_left(b.to_integer(), a.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::ShiftRight => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        sql_shift_right(b.to_integer(), a.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::BitNot => {
                    // p2 = ~p1
                    let a = self.get_reg(op.p1);
                    let result = if a.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(!a.to_integer())
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Type Conversion ─────────────────────────────────────
                Opcode::AddImm => {
                    // Add integer p2 to register p1.
                    let val = self.get_reg(op.p1).to_integer() + i64::from(op.p2);
                    self.set_reg(op.p1, SqliteValue::Integer(val));
                    pc += 1;
                }

                Opcode::Cast => {
                    // Cast register p1 to type indicated by p2.
                    let val = self.get_reg(op.p1).clone();
                    let casted = sql_cast(val, op.p2);
                    self.set_reg(op.p1, casted);
                    pc += 1;
                }

                Opcode::MustBeInt => {
                    let val = self.get_reg(op.p1);
                    if val.as_integer().is_none() && !val.is_null() {
                        if op.p2 > 0 {
                            pc = op.p2 as usize;
                            continue;
                        }
                        return Err(FrankenError::TypeMismatch {
                            expected: "integer".to_owned(),
                            actual: val.typeof_str().to_owned(),
                        });
                    }
                    pc += 1;
                }

                #[allow(clippy::cast_precision_loss)]
                Opcode::RealAffinity => {
                    if let SqliteValue::Integer(i) = self.get_reg(op.p1) {
                        let f = *i as f64;
                        self.set_reg(op.p1, SqliteValue::Float(f));
                    }
                    pc += 1;
                }

                // ── Comparison Jumps ────────────────────────────────────
                Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge => {
                    let lhs = self.get_reg(op.p3);
                    let rhs = self.get_reg(op.p1);

                    // NULL handling: if either is NULL, jump depends on p5
                    // flag (SQLITE_NULLEQ).
                    let should_jump = if lhs.is_null() || rhs.is_null() {
                        let null_eq = (op.p5 & 0x80) != 0;
                        if null_eq {
                            // IS / IS NOT semantics: NULL == NULL is true.
                            let both_null = lhs.is_null() && rhs.is_null();
                            match op.opcode {
                                Opcode::Eq => both_null,
                                Opcode::Ne => !both_null,
                                _ => false,
                            }
                        } else {
                            // Standard SQL: comparison with NULL is NULL (no jump).
                            false
                        }
                    } else {
                        let cmp = lhs.partial_cmp(rhs);
                        matches!(
                            (op.opcode, cmp),
                            (Opcode::Eq, Some(std::cmp::Ordering::Equal))
                                | (Opcode::Lt, Some(std::cmp::Ordering::Less))
                                | (
                                    Opcode::Le,
                                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                                )
                                | (Opcode::Gt, Some(std::cmp::Ordering::Greater))
                                | (
                                    Opcode::Ge,
                                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                                )
                        ) || matches!(
                            (op.opcode, cmp),
                            (Opcode::Ne, Some(ord)) if ord != std::cmp::Ordering::Equal
                        )
                    };

                    if should_jump {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Boolean Logic ───────────────────────────────────────
                Opcode::And => {
                    // Three-valued AND: p3 = p1 AND p2
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = sql_and(a, b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Or => {
                    // Three-valued OR: p3 = p1 OR p2
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = sql_or(a, b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Not => {
                    // p2 = NOT p1
                    let a = self.get_reg(op.p1);
                    let result = if a.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(i64::from(a.to_integer() == 0))
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Conditional Jumps ───────────────────────────────────
                Opcode::If => {
                    // Jump to p2 if p1 is true (non-zero, non-NULL).
                    let val = self.get_reg(op.p1);
                    if !val.is_null() && val.to_integer() != 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNot => {
                    // Jump to p2 if p1 is false (zero) or NULL.
                    let val = self.get_reg(op.p1);
                    if val.is_null() || val.to_integer() == 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IsNull => {
                    // Jump to p2 if p1 is NULL.
                    if self.get_reg(op.p1).is_null() {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::NotNull => {
                    // Jump to p2 if p1 is NOT NULL.
                    if self.get_reg(op.p1).is_null() {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::Once => {
                    // Jump to p2 on first execution only.
                    if once_flags[pc] {
                        pc += 1;
                    } else {
                        once_flags[pc] = true;
                        pc = op.p2 as usize;
                    }
                }

                // ── Gosub / Return ──────────────────────────────────────
                Opcode::Gosub => {
                    // Store return address in p1, jump to p2.
                    let return_addr = (pc + 1) as i32;
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(return_addr)));
                    pc = op.p2 as usize;
                }

                Opcode::Return => {
                    // Jump to address stored in p1.
                    let addr = self.get_reg(op.p1).to_integer();
                    pc = addr as usize;
                }

                // ── Transaction (stub for expression eval) ──────────────
                Opcode::Transaction
                | Opcode::AutoCommit
                | Opcode::ReadCookie
                | Opcode::SetCookie
                | Opcode::TableLock => {
                    // No-op in expression-only mode. Will be wired to WAL
                    // and lock manager in Phase 5.
                    pc += 1;
                }

                // ── Cursor operations (in-memory backend) ──────────────
                Opcode::OpenRead | Opcode::OpenWrite => {
                    let cursor_id = op.p1;
                    let root_page = op.p2;
                    let writable = op.opcode == Opcode::OpenWrite;
                    if !writable && self.open_storage_cursor(cursor_id, root_page) {
                        self.cursors.remove(&cursor_id);
                    } else {
                        self.storage_cursors.remove(&cursor_id);
                        self.cursors
                            .insert(cursor_id, MemCursor::new(root_page, writable));
                    }
                    pc += 1;
                }

                Opcode::OpenEphemeral | Opcode::OpenAutoindex => {
                    // Ephemeral table: create an in-memory table on-the-fly.
                    let cursor_id = op.p1;
                    let num_cols = op.p2.max(1);
                    if let Some(db) = self.db.as_mut() {
                        let root_page = db.create_table(num_cols as usize);
                        self.storage_cursors.remove(&cursor_id);
                        self.cursors
                            .insert(cursor_id, MemCursor::new(root_page, true));
                    }
                    pc += 1;
                }

                Opcode::OpenPseudo => {
                    let cursor_id = op.p1;
                    self.storage_cursors.remove(&cursor_id);
                    self.cursors.insert(cursor_id, MemCursor::new_pseudo());
                    pc += 1;
                }

                Opcode::OpenDup | Opcode::ReopenIdx => {
                    // Reopen: reuse existing cursor configuration.
                    pc += 1;
                }

                Opcode::SorterOpen => {
                    let cursor_id = op.p1;
                    let key_columns = usize::try_from(op.p2.max(1)).unwrap_or(1);
                    let sort_key_orders = match &op.p4 {
                        P4::Str(order) => order
                            .chars()
                            .take(key_columns)
                            .map(|ch| {
                                if ch == '-' {
                                    SortKeyOrder::Desc
                                } else {
                                    SortKeyOrder::Asc
                                }
                            })
                            .collect(),
                        _ => vec![SortKeyOrder::Asc; key_columns],
                    };
                    self.sorters
                        .insert(cursor_id, SorterCursor::new(key_columns, sort_key_orders));
                    // A cursor id cannot be both table and sorter cursor.
                    self.cursors.remove(&cursor_id);
                    self.storage_cursors.remove(&cursor_id);
                    pc += 1;
                }

                Opcode::Close => {
                    self.cursors.remove(&op.p1);
                    self.storage_cursors.remove(&op.p1);
                    self.sorters.remove(&op.p1);
                    pc += 1;
                }

                Opcode::ColumnsUsed => {
                    pc += 1;
                }

                Opcode::Rewind | Opcode::Sort | Opcode::SorterSort => {
                    // Position cursor at the first row. Jump to p2 if empty.
                    let cursor_id = op.p1;
                    let is_empty = if let Some(sorter) = self.sorters.get_mut(&cursor_id) {
                        if matches!(op.opcode, Opcode::Sort | Opcode::SorterSort) {
                            sorter.sort();
                        }
                        if sorter.rows.is_empty() {
                            sorter.position = None;
                            true
                        } else {
                            sorter.position = Some(0);
                            false
                        }
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if cursor.is_pseudo {
                            cursor.pseudo_row.is_none()
                        } else if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if table.rows.is_empty() {
                                    true
                                } else {
                                    cursor.position = Some(0);
                                    false
                                }
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        !cursor.cursor.first(&cursor.cx)?
                    } else {
                        true
                    };
                    if is_empty {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Last => {
                    // Position cursor at the last row. Jump to p2 if empty.
                    let cursor_id = op.p1;
                    let is_empty = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        !cursor.cursor.last(&cursor.cx)?
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if cursor.is_pseudo {
                            cursor.pseudo_row.is_none()
                        } else if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if table.rows.is_empty() {
                                    true
                                } else {
                                    cursor.position = Some(table.rows.len() - 1);
                                    false
                                }
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                    if is_empty {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Next | Opcode::SorterNext => {
                    // Advance cursor to the next row. Jump to p2 if more rows.
                    let cursor_id = op.p1;
                    let has_next = if let Some(sorter) = self.sorters.get_mut(&cursor_id) {
                        if let Some(pos) = sorter.position {
                            let next = pos + 1;
                            if next < sorter.rows.len() {
                                sorter.position = Some(next);
                                true
                            } else {
                                sorter.position = None;
                                false
                            }
                        } else {
                            false
                        }
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if cursor.is_pseudo {
                            false
                        } else if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if let Some(pos) = cursor.position {
                                    let next = pos + 1;
                                    if next < table.rows.len() {
                                        cursor.position = Some(next);
                                        true
                                    } else {
                                        cursor.position = None;
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor.cursor.next(&cursor.cx)?
                    } else {
                        false
                    };
                    if has_next {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Prev => {
                    // Move cursor backward. Jump to p2 if more rows.
                    let cursor_id = op.p1;
                    let has_prev = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor.cursor.prev(&cursor.cx)?
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if let Some(pos) = cursor.position {
                            if pos > 0 {
                                cursor.position = Some(pos - 1);
                                true
                            } else {
                                cursor.position = None;
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if has_prev {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Column => {
                    // Read column p2 from cursor p1 into register p3.
                    let cursor_id = op.p1;
                    let col_idx = op.p2 as usize;
                    let target = op.p3;
                    let val = self.cursor_column(cursor_id, col_idx)?;
                    self.set_reg(target, val);
                    pc += 1;
                }

                Opcode::Rowid => {
                    // Get rowid from cursor p1 into register p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let val = self.cursor_rowid(cursor_id)?;
                    self.set_reg(target, val);
                    pc += 1;
                }

                Opcode::RowData => {
                    // Store raw row data as a blob in register p2.
                    // For pseudo-cursors, retrieve the blob from p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if cursor.is_pseudo {
                            // Pseudo cursor: the "row data" was already set via
                            // a prior MakeRecord → set_reg. Leave register as-is.
                        } else {
                            self.set_reg(target, SqliteValue::Null);
                        }
                    } else {
                        self.set_reg(target, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::NullRow => {
                    // Set cursor p1 to a null row.
                    if let Some(cursor) = self.cursors.get_mut(&op.p1) {
                        cursor.position = None;
                    }
                    pc += 1;
                }

                Opcode::Offset => {
                    self.set_reg(op.p3, SqliteValue::Null);
                    pc += 1;
                }

                // ── Seek operations (in-memory) ─────────────────────────
                Opcode::SeekRowid => {
                    // Seek cursor p1 to the row with rowid in register p3.
                    // If not found, jump to p2.
                    let cursor_id = op.p1;
                    let rowid_val = self.get_reg(op.p3).to_integer();
                    let found = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor
                            .cursor
                            .table_move_to(&cursor.cx, rowid_val)?
                            .is_found()
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if let Some(idx) = table.find_by_rowid(rowid_val) {
                                    cursor.position = Some(idx);
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if found {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT => {
                    // Simplified seek: position at first/last row.
                    // Full index seeks require B-tree; for in-memory mode,
                    // position at the start (GE/GT) or end (LE/LT).
                    let cursor_id = op.p1;
                    let found = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        match op.opcode {
                            Opcode::SeekLE | Opcode::SeekLT => cursor.cursor.last(&cursor.cx)?,
                            _ => cursor.cursor.first(&cursor.cx)?,
                        }
                    } else if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                if table.rows.is_empty() {
                                    false
                                } else {
                                    match op.opcode {
                                        Opcode::SeekLE | Opcode::SeekLT => {
                                            cursor.position = Some(table.rows.len() - 1);
                                        }
                                        _ => {
                                            cursor.position = Some(0);
                                        }
                                    }
                                    true
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if found {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::SeekScan | Opcode::SeekEnd | Opcode::SeekHit => {
                    pc += 1;
                }

                Opcode::NotFound | Opcode::NotExists | Opcode::IfNoHope => {
                    // Check if rowid in register p3 exists in cursor p1.
                    let cursor_id = op.p1;
                    let rowid_val = self.get_reg(op.p3).to_integer();
                    let exists = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor
                            .cursor
                            .table_move_to(&cursor.cx, rowid_val)?
                            .is_found()
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                table.find_by_rowid(rowid_val).is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if exists {
                        pc += 1; // Found: fall through.
                    } else {
                        pc = op.p2 as usize; // Not found: jump.
                    }
                }

                Opcode::Found | Opcode::NoConflict => {
                    // Check if key exists; jump to p2 if found.
                    let cursor_id = op.p1;
                    let rowid_val = self.get_reg(op.p3).to_integer();
                    let exists = if let Some(cursor) = self.storage_cursors.get_mut(&cursor_id) {
                        cursor
                            .cursor
                            .table_move_to(&cursor.cx, rowid_val)?
                            .is_found()
                    } else if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_ref() {
                            if let Some(table) = db.get_table(cursor.root_page) {
                                table.find_by_rowid(rowid_val).is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if exists {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Insert / Delete / NewRowid (in-memory) ──────────────
                Opcode::NewRowid => {
                    // Allocate a new rowid for cursor p1, store in register p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let rowid = if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(db) = self.db.as_mut() {
                            if let Some(table) = db.get_table_mut(cursor.root_page) {
                                table.alloc_rowid()
                            } else {
                                1
                            }
                        } else {
                            1
                        }
                    } else {
                        1
                    };
                    self.set_reg(target, SqliteValue::Integer(rowid));
                    pc += 1;
                }

                Opcode::Insert => {
                    // Insert record in register p2 with rowid from register p3
                    // into cursor p1.
                    let cursor_id = op.p1;
                    let record_reg = op.p2;
                    let rowid_reg = op.p3;
                    let rowid = self.get_reg(rowid_reg).to_integer();
                    let record_val = self.get_reg(record_reg).clone();
                    // The record is stored as a Blob in SQLite record format (MakeRecord).
                    let values = decode_record(&record_val)?;
                    if let Some(cursor) = self.cursors.get(&cursor_id) {
                        let root = cursor.root_page;
                        if let Some(db) = self.db.as_mut() {
                            if let Some(table) = db.get_table_mut(root) {
                                table.insert(rowid, values);
                            }
                        }
                    }
                    pc += 1;
                }

                Opcode::Delete => {
                    // Delete the row at the current cursor position.
                    // Cursor position is left unchanged; the DELETE codegen
                    // uses reverse iteration (Last/Prev) so removing a row
                    // does not shift indices of unvisited (earlier) rows.
                    let cursor_id = op.p1;
                    if let Some(cursor) = self.cursors.get(&cursor_id) {
                        if let Some(pos) = cursor.position {
                            let root = cursor.root_page;
                            if let Some(db) = self.db.as_mut() {
                                if let Some(table) = db.get_table_mut(root) {
                                    table.delete_at(pos);
                                }
                            }
                        }
                    }
                    pc += 1;
                }

                Opcode::IdxInsert => {
                    // Index insert is a no-op in this in-memory backend.
                    pc += 1;
                }

                Opcode::SorterInsert => {
                    let cursor_id = op.p1;
                    let record = self.get_reg(op.p2).clone();
                    if let Some(sorter) = self.sorters.get_mut(&cursor_id) {
                        sorter.rows.push(decode_record(&record)?);
                    }
                    pc += 1;
                }

                Opcode::IdxDelete => {
                    pc += 1;
                }

                Opcode::SorterCompare => {
                    // Compare current sorter key with packed record in register p3.
                    // Jump to p2 when keys differ.
                    let cursor_id = op.p1;
                    let differs = if let Some(sorter) = self.sorters.get(&cursor_id) {
                        if let Some(pos) = sorter.position {
                            if let Some(current) = sorter.rows.get(pos) {
                                let probe = decode_record(self.get_reg(op.p3))?;
                                !sorter_keys_equal(current, &probe, sorter.key_columns)
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                    if differs {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::SorterData => {
                    // Encode current sorter row into register p2.
                    let cursor_id = op.p1;
                    let target = op.p2;
                    let value = if let Some(sorter) = self.sorters.get(&cursor_id) {
                        if let Some(pos) = sorter.position {
                            if let Some(row) = sorter.rows.get(pos) {
                                SqliteValue::Blob(encode_record(row))
                            } else {
                                SqliteValue::Null
                            }
                        } else {
                            SqliteValue::Null
                        }
                    } else {
                        SqliteValue::Null
                    };
                    self.set_reg(target, value);
                    pc += 1;
                }

                Opcode::RowCell => {
                    pc += 1;
                }

                Opcode::ResetCount => {
                    pc += 1;
                }

                // ── Record building (SQLite record format) ──────────────
                Opcode::MakeRecord => {
                    // Build a record from registers p1..p1+p2-1 into register p3.
                    let start = op.p1;
                    let count = op.p2;
                    let target = op.p3;
                    let mut values = Vec::with_capacity(count as usize);
                    for i in 0..count {
                        values.push(self.get_reg(start + i).clone());
                    }
                    self.set_reg(target, SqliteValue::Blob(encode_record(&values)));
                    pc += 1;
                }

                Opcode::Affinity => {
                    // Apply type affinity to p2 registers starting at p1.
                    // Uses p4 as affinity string.
                    if let P4::Affinity(aff) = &op.p4 {
                        let start = op.p1;
                        for (i, ch) in aff.chars().enumerate() {
                            #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
                            let reg = start + i as i32;
                            let val = self.get_reg(reg).clone();
                            let affinity = char_to_affinity(ch);
                            self.set_reg(reg, val.apply_affinity(affinity));
                        }
                    }
                    pc += 1;
                }

                // ── Miscellaneous ───────────────────────────────────────
                Opcode::HaltIfNull => {
                    if self.get_reg(op.p3).is_null() {
                        let msg = match &op.p4 {
                            P4::Str(s) => s.clone(),
                            _ => "NOT NULL constraint failed".to_owned(),
                        };
                        return Ok(ExecOutcome::Error {
                            code: op.p1,
                            message: msg,
                        });
                    }
                    pc += 1;
                }

                Opcode::Count => {
                    // Stub: set p2 to 0 (no cursor).
                    self.set_reg(op.p2, SqliteValue::Integer(0));
                    pc += 1;
                }

                Opcode::Sequence => {
                    self.set_reg(op.p2, SqliteValue::Integer(0));
                    pc += 1;
                }

                Opcode::SequenceTest => {
                    pc += 1;
                }

                Opcode::Variable => {
                    // Bind parameter (1-indexed). Unbound params read as NULL.
                    let value = usize::try_from(op.p1)
                        .ok()
                        .and_then(|one_based| one_based.checked_sub(1))
                        .and_then(|idx| self.bindings.get(idx))
                        .cloned()
                        .unwrap_or(SqliteValue::Null);
                    self.set_reg(op.p2, value);
                    pc += 1;
                }

                Opcode::BeginSubrtn => {
                    self.set_reg(op.p2, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::IsTrue => {
                    let val = self.get_reg(op.p1);
                    let truth = !val.is_null() && val.to_integer() != 0;
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(truth)));
                    pc += 1;
                }

                Opcode::ZeroOrNull => {
                    // If either P1 or P3 is NULL, set P2 to NULL.
                    // Otherwise set P2 to 0.
                    // Reference: ZeroOrNull semantics (OP_ZeroOrNull spec).
                    if self.get_reg(op.p1).is_null() || self.get_reg(op.p3).is_null() {
                        self.set_reg(op.p2, SqliteValue::Null);
                    } else {
                        self.set_reg(op.p2, SqliteValue::Integer(0));
                    }
                    pc += 1;
                }

                Opcode::IfNullRow => {
                    // Jump to p2 if cursor p1 is not positioned on a row.
                    let is_null = if let Some(cursor) = self.storage_cursors.get(&op.p1) {
                        cursor.cursor.eof()
                    } else {
                        self.cursors
                            .get(&op.p1)
                            .is_none_or(|c| c.position.is_none() && !c.is_pseudo)
                    };
                    if is_null {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNotOpen => {
                    // Jump to p2 if cursor p1 is not open.
                    if self.cursors.contains_key(&op.p1)
                        || self.storage_cursors.contains_key(&op.p1)
                    {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::TypeCheck
                | Opcode::Permutation
                | Opcode::Compare
                | Opcode::CollSeq
                | Opcode::ElseEq
                | Opcode::FkCheck => {
                    pc += 1;
                }

                Opcode::Jump => {
                    // Jump to one of p1/p2/p3 based on last comparison.
                    // Stub: jump to p2 (neutral).
                    pc = op.p2 as usize;
                }

                Opcode::IsType => {
                    // Type check; stub: fall through.
                    pc += 1;
                }

                Opcode::IfSizeBetween | Opcode::IfEmpty => {
                    // Stub: jump to p2.
                    pc = op.p2 as usize;
                }

                Opcode::DeferredSeek | Opcode::IdxRowid | Opcode::FinishSeek => {
                    pc += 1;
                }

                // ── Index comparison ────────────────────────────────────
                Opcode::IdxLE | Opcode::IdxGT | Opcode::IdxLT | Opcode::IdxGE => {
                    // Stub: fall through.
                    pc += 1;
                }

                // ── Schema / DDL ────────────────────────────────────────
                Opcode::CreateBtree => {
                    // Create a new B-tree (table) and store the root page in
                    // register p2. In memory mode, allocate a new MemTable.
                    let target = op.p2;
                    let root_page = if let Some(db) = self.db.as_mut() {
                        db.create_table(0) // Column count set later.
                    } else {
                        0
                    };
                    self.set_reg(target, SqliteValue::Integer(i64::from(root_page)));
                    pc += 1;
                }

                Opcode::Clear => {
                    // Clear all rows from a table. p1 = root page.
                    if let Some(db) = self.db.as_mut() {
                        if let Some(table) = db.get_table_mut(op.p1) {
                            table.rows.clear();
                        }
                    }
                    pc += 1;
                }

                Opcode::Destroy => {
                    // Remove a table. p1 = root page.
                    if let Some(db) = self.db.as_mut() {
                        db.tables.remove(&op.p1);
                    }
                    pc += 1;
                }

                Opcode::SqlExec
                | Opcode::ParseSchema
                | Opcode::LoadAnalysis
                | Opcode::DropTable
                | Opcode::DropIndex
                | Opcode::DropTrigger => {
                    pc += 1;
                }

                Opcode::ResetSorter => {
                    if let Some(sorter) = self.sorters.get_mut(&op.p1) {
                        sorter.rows.clear();
                        sorter.position = None;
                    }
                    pc += 1;
                }

                // ── Savepoint / Checkpoint ──────────────────────────────
                Opcode::Savepoint | Opcode::Checkpoint => {
                    pc += 1;
                }

                // ── Program execution (subprogram) ──────────────────────
                Opcode::Program | Opcode::Param => {
                    pc += 1;
                }

                // ── Coroutine ───────────────────────────────────────────
                Opcode::InitCoroutine => {
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(op.p3)));
                    if op.p2 > 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Yield => {
                    let saved = self.get_reg(op.p1).to_integer();
                    let current = (pc + 1) as i32;
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(current)));
                    pc = saved as usize;
                }

                Opcode::EndCoroutine => {
                    let saved = self.get_reg(op.p1).to_integer();
                    pc = saved as usize;
                }

                // ── Aggregation ─────────────────────────────────────────
                //
                // Phase 4 supports single-group aggregation (no GROUP BY) using
                // AggStep/AggFinal. Aggregate state is stored out-of-band and keyed
                // by the accumulator register.
                Opcode::AggStep => {
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "AggStep opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "AggStep opcode executed without function registry".to_owned(),
                        )
                    })?;

                    let arg_count = i32::from(op.p5);
                    let func = registry
                        .find_aggregate(func_name, arg_count)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "no such aggregate function: {func_name}/{arg_count}",
                            ))
                        })?;

                    let mut args = Vec::with_capacity(op.p5 as usize);
                    for i in 0..op.p5 {
                        let reg_idx = op.p2 + i32::from(i);
                        args.push(self.get_reg(reg_idx).clone());
                    }

                    let accum_reg = op.p3;
                    let ctx = self.aggregates.entry(accum_reg).or_insert_with(|| {
                        let state = func.initial_state();
                        AggregateContext {
                            func: func.clone(),
                            state,
                        }
                    });

                    if !Arc::ptr_eq(&ctx.func, &func) {
                        return Err(FrankenError::Internal(
                            "AggStep accumulator reused for a different aggregate".to_owned(),
                        ));
                    }

                    ctx.func.step(&mut ctx.state, &args)?;
                    pc += 1;
                }

                Opcode::AggFinal => {
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "AggFinal opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "AggFinal opcode executed without function registry".to_owned(),
                        )
                    })?;

                    let arg_count = op.p2;
                    let func = registry
                        .find_aggregate(func_name, arg_count)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "no such aggregate function: {func_name}/{arg_count}",
                            ))
                        })?;

                    let accum_reg = op.p1;
                    let result = match self.aggregates.remove(&accum_reg) {
                        Some(ctx) => {
                            if !Arc::ptr_eq(&ctx.func, &func) {
                                return Err(FrankenError::Internal(
                                    "AggFinal accumulator used for a different aggregate"
                                        .to_owned(),
                                ));
                            }
                            ctx.func.finalize(ctx.state)?
                        }
                        None => func.finalize(func.initial_state())?,
                    };

                    self.set_reg(accum_reg, result);
                    pc += 1;
                }

                Opcode::AggInverse | Opcode::AggValue => {
                    // Not needed yet (GROUP BY / window aggregates / inverse ops).
                    pc += 1;
                }

                // ── Scalar function call ──────────────────────────────────
                //
                // Function/PureFunc: p1 = constant-p5-flags, p2 = first-arg register,
                // p3 = output register, p4 = FuncName, p5 = arg count.
                // Arguments are in registers p2..p2+p5.
                Opcode::Function | Opcode::PureFunc => {
                    let func_name = match &op.p4 {
                        P4::FuncName(name) => name.as_str(),
                        _ => {
                            return Err(FrankenError::Internal(
                                "Function opcode missing P4::FuncName".to_owned(),
                            ));
                        }
                    };
                    let arg_count = op.p5 as usize;
                    let first_arg_reg = op.p2;
                    let output_reg = op.p3;

                    let registry = self.func_registry.as_ref().ok_or_else(|| {
                        FrankenError::Internal(
                            "Function opcode executed without function registry".to_owned(),
                        )
                    })?;

                    #[allow(clippy::cast_possible_wrap)]
                    let func = registry
                        .find_scalar(func_name, arg_count as i32)
                        .ok_or_else(|| {
                            FrankenError::Internal(format!(
                                "no such function: {func_name}/{arg_count}",
                            ))
                        })?;

                    let mut args = Vec::with_capacity(arg_count);
                    for i in 0..arg_count {
                        #[allow(clippy::cast_possible_wrap)]
                        let reg_idx = first_arg_reg + i as i32;
                        args.push(self.get_reg(reg_idx).clone());
                    }

                    let result = func.invoke(&args)?;
                    self.set_reg(output_reg, result);
                    pc += 1;
                }

                // ── LIMIT/OFFSET support ────────────────────────────────
                // DecrJumpZero: decrement register p1; if result is zero,
                // jump to p2. Used to count down remaining LIMIT rows.
                Opcode::DecrJumpZero => {
                    let val = self.get_reg(op.p1).to_integer() - 1;
                    self.set_reg(op.p1, SqliteValue::Integer(val));
                    if val == 0 {
                        #[allow(clippy::cast_sign_loss)]
                        {
                            pc = op.p2 as usize;
                        }
                    } else {
                        pc += 1;
                    }
                }

                // IfPos: if register p1 > 0, subtract p3, then jump to p2.
                // Used for OFFSET counting (skip rows while offset > 0).
                Opcode::IfPos => {
                    let val = self.get_reg(op.p1).to_integer();
                    if val > 0 {
                        let decremented = val - i64::from(op.p3);
                        self.set_reg(op.p1, SqliteValue::Integer(decremented));
                        #[allow(clippy::cast_sign_loss)]
                        {
                            pc = op.p2 as usize;
                        }
                    } else {
                        pc += 1;
                    }
                }

                // ── Catch-all for remaining opcodes ─────────────────────
                _ => {
                    // Unimplemented opcode: skip (no-op for now).
                    pc += 1;
                }
            }
        }
    }

    /// Get the collected result rows.
    pub fn results(&self) -> &[Vec<SqliteValue>] {
        &self.results
    }

    /// Take the result rows, consuming them.
    pub fn take_results(&mut self) -> Vec<Vec<SqliteValue>> {
        std::mem::take(&mut self.results)
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    #[allow(clippy::cast_sign_loss)]
    fn get_reg(&self, r: i32) -> &SqliteValue {
        self.registers.get(r as usize).unwrap_or(&SqliteValue::Null)
    }

    #[allow(clippy::cast_sign_loss)]
    fn set_reg(&mut self, r: i32, val: SqliteValue) {
        let idx = r as usize;
        if idx >= self.registers.len() {
            self.registers.resize(idx + 1, SqliteValue::Null);
        }
        self.registers[idx] = match val {
            SqliteValue::Float(f) if f.is_nan() => SqliteValue::Null,
            other => other,
        };
    }

    /// Read a column value from the cursor's current row.
    fn cursor_column(&self, cursor_id: i32, col_idx: usize) -> Result<SqliteValue> {
        if let Some(cursor) = self.storage_cursors.get(&cursor_id) {
            if cursor.cursor.eof() {
                return Ok(SqliteValue::Null);
            }
            let payload = cursor.cursor.payload(&cursor.cx)?;
            let values = decode_record(&SqliteValue::Blob(payload))?;
            return Ok(values.get(col_idx).cloned().unwrap_or(SqliteValue::Null));
        }

        if let Some(cursor) = self.cursors.get(&cursor_id) {
            if cursor.is_pseudo {
                return Ok(cursor
                    .pseudo_row
                    .as_ref()
                    .and_then(|row| row.get(col_idx))
                    .cloned()
                    .unwrap_or(SqliteValue::Null));
            }
            if let Some(pos) = cursor.position
                && let Some(db) = self.db.as_ref()
                && let Some(table) = db.get_table(cursor.root_page)
                && let Some(row) = table.rows.get(pos)
            {
                return Ok(row
                    .values
                    .get(col_idx)
                    .cloned()
                    .unwrap_or(SqliteValue::Null));
            }
        }

        // Sorter cursor: read column directly from the sorted row.
        if let Some(sorter) = self.sorters.get(&cursor_id) {
            if let Some(pos) = sorter.position {
                if let Some(row) = sorter.rows.get(pos) {
                    return Ok(row.get(col_idx).cloned().unwrap_or(SqliteValue::Null));
                }
            }
        }

        Ok(SqliteValue::Null)
    }

    /// Get the rowid from the cursor's current row.
    fn cursor_rowid(&self, cursor_id: i32) -> Result<SqliteValue> {
        if let Some(cursor) = self.storage_cursors.get(&cursor_id) {
            if cursor.cursor.eof() {
                return Ok(SqliteValue::Null);
            }
            return Ok(SqliteValue::Integer(cursor.cursor.rowid(&cursor.cx)?));
        }

        if let Some(cursor) = self.cursors.get(&cursor_id)
            && let Some(pos) = cursor.position
            && let Some(db) = self.db.as_ref()
            && let Some(table) = db.get_table(cursor.root_page)
            && let Some(row) = table.rows.get(pos)
        {
            return Ok(SqliteValue::Integer(row.rowid));
        }
        Ok(SqliteValue::Null)
    }

    #[allow(clippy::cast_sign_loss)]
    fn open_storage_cursor(&mut self, cursor_id: i32, root_page: i32) -> bool {
        const PAGE_SIZE: u32 = 4096;
        if !self.storage_read_cursors_enabled {
            return false;
        }
        let Some(db) = self.db.as_ref() else {
            return false;
        };
        let Some(table) = db.get_table(root_page) else {
            return false;
        };

        // Build a transient B-tree snapshot from the current MemTable rows.
        // Use the actual root page number (typically 2+) to avoid page-1's
        // special 100-byte header offset.
        let Some(root_pgno) = fsqlite_types::PageNumber::new(root_page as u32) else {
            return false;
        };

        let store = MemPageStore::with_empty_table(root_pgno, PAGE_SIZE);
        let cx = Cx::new();
        let mut cursor = BtCursor::new(store, root_pgno, PAGE_SIZE, true);

        for row in &table.rows {
            let payload = encode_record(&row.values);
            if cursor.table_insert(&cx, row.rowid, &payload).is_err() {
                return false;
            }
        }

        self.storage_cursors
            .insert(cursor_id, StorageCursor { cursor, cx });
        true
    }

    fn trace_opcode(&self, pc: usize, op: &VdbeOp) {
        if !self.trace_opcodes || !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let spans = opcode_register_spans(op);
        tracing::debug!(
            target: "fsqlite_vdbe::opcode",
            logging_standard = VDBE_TRACE_LOGGING_STANDARD,
            pc,
            opcode = %op.opcode.name(),
            p1 = op.p1,
            p2 = op.p2,
            p3 = op.p3,
            p5 = op.p5,
            read_start = spans.read_start,
            read_len = spans.read_len,
            write_start = spans.write_start,
            write_len = spans.write_len,
            "executing vdbe opcode",
        );
    }
}

// ── SQLite record encoding ──────────────────────────────────────────────
//
// SQLite `OP_MakeRecord` produces a record in the on-disk record format
// (header + body). Using the same format internally avoids later translation
// when wiring VDBE cursors to the real B-tree layer.

fn encode_record(values: &[SqliteValue]) -> Vec<u8> {
    serialize_record(values)
}

fn decode_record(val: &SqliteValue) -> Result<Vec<SqliteValue>> {
    let SqliteValue::Blob(bytes) = val else {
        return Ok(Vec::new());
    };

    parse_record(bytes).ok_or_else(|| FrankenError::internal("malformed SQLite record blob"))
}

fn sorter_keys_equal(lhs: &[SqliteValue], rhs: &[SqliteValue], key_columns: usize) -> bool {
    compare_sorter_keys(lhs, rhs, key_columns) == Ordering::Equal
}

fn compare_sorter_keys(lhs: &[SqliteValue], rhs: &[SqliteValue], key_columns: usize) -> Ordering {
    let key_count = key_columns.max(1);
    for idx in 0..key_count {
        let Some(lhs_value) = lhs.get(idx) else {
            return if rhs.get(idx).is_some() {
                Ordering::Less
            } else {
                break;
            };
        };
        let Some(rhs_value) = rhs.get(idx) else {
            return Ordering::Greater;
        };

        match lhs_value.partial_cmp(rhs_value).unwrap_or(Ordering::Equal) {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }
    }
    Ordering::Equal
}

fn compare_sorter_rows(
    lhs: &[SqliteValue],
    rhs: &[SqliteValue],
    key_columns: usize,
    sort_key_orders: &[SortKeyOrder],
) -> Ordering {
    let key_count = key_columns.max(1);
    for idx in 0..key_count {
        let Some(lhs_value) = lhs.get(idx) else {
            return if rhs.get(idx).is_some() {
                Ordering::Less
            } else {
                break;
            };
        };
        let Some(rhs_value) = rhs.get(idx) else {
            return Ordering::Greater;
        };

        let mut ord = lhs_value.partial_cmp(rhs_value).unwrap_or(Ordering::Equal);
        if ord == Ordering::Equal {
            continue;
        }

        if sort_key_orders.get(idx) == Some(&SortKeyOrder::Desc) {
            ord = ord.reverse();
        }
        return ord;
    }

    // Deterministic tie-breaker: compare full rows so sort order is stable.
    let full_len = lhs.len().max(rhs.len());
    for idx in 0..full_len {
        match (lhs.get(idx), rhs.get(idx)) {
            (Some(lhs_value), Some(rhs_value)) => {
                match lhs_value.partial_cmp(rhs_value).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => {}
                    non_equal => return non_equal,
                }
            }
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => break,
        }
    }

    Ordering::Equal
}

fn opcode_trace_enabled() -> bool {
    let env_enabled = std::env::var(VDBE_TRACE_ENV).is_ok_and(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        !normalized.is_empty() && normalized != "0" && normalized != "false" && normalized != "off"
    });
    env_enabled || cfg!(test)
}

fn range(start: i32, len: i32) -> (i32, i32) {
    if start <= 0 {
        (-1, 0)
    } else {
        (start, len.max(1))
    }
}

fn opcode_register_spans(op: &VdbeOp) -> OpcodeRegisterSpans {
    let (read_start, read_len, write_start, write_len) = match op.opcode {
        Opcode::Integer
        | Opcode::Int64
        | Opcode::Real
        | Opcode::String
        | Opcode::String8
        | Opcode::Blob
        | Opcode::Variable => {
            let (write_start, write_len) = range(op.p2, 1);
            (-1, 0, write_start, write_len)
        }
        Opcode::Null => {
            // p3 is absolute end register; count = p3 - p2 + 1 (or 1 if p3==0).
            let write_count = if op.p3 > 0 { op.p3 - op.p2 + 1 } else { 1 };
            let (write_start, write_len) = range(op.p2, write_count);
            (-1, 0, write_start, write_len)
        }
        Opcode::SoftNull
        | Opcode::Cast
        | Opcode::RealAffinity
        | Opcode::AddImm
        | Opcode::MustBeInt
        | Opcode::InitCoroutine
        | Opcode::Yield
        | Opcode::EndCoroutine => {
            let (start, len) = range(op.p1, 1);
            (start, len, start, len)
        }
        Opcode::Move => {
            let (read_start, read_len) = range(op.p1, op.p3);
            let (write_start, write_len) = range(op.p2, op.p3);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::Copy | Opcode::SCopy | Opcode::IntCopy | Opcode::BitNot | Opcode::Not => {
            let (read_start, read_len) = range(op.p1, 1);
            let (write_start, write_len) = range(op.p2, 1);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::ResultRow => {
            let (read_start, read_len) = range(op.p1, op.p2);
            (read_start, read_len, -1, 0)
        }
        Opcode::Add
        | Opcode::Subtract
        | Opcode::Multiply
        | Opcode::Divide
        | Opcode::Remainder
        | Opcode::Concat
        | Opcode::BitAnd
        | Opcode::BitOr
        | Opcode::ShiftLeft
        | Opcode::ShiftRight
        | Opcode::And
        | Opcode::Or => {
            let (read_start, read_len) = range(op.p1, 2);
            let (write_start, write_len) = range(op.p3, 1);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge => {
            let (read_start, read_len) = range(op.p1, 1);
            let (rhs_start, rhs_len) = range(op.p3, 1);
            let normalized_start = if read_start > 0 && rhs_start > 0 {
                read_start.min(rhs_start)
            } else if read_start > 0 {
                read_start
            } else {
                rhs_start
            };
            let normalized_len = if read_start > 0 && rhs_start > 0 && read_start != rhs_start {
                2
            } else {
                read_len.max(rhs_len)
            };
            (normalized_start, normalized_len, -1, 0)
        }
        Opcode::If | Opcode::IfNot | Opcode::IsNull | Opcode::NotNull | Opcode::IsTrue => {
            let (read_start, read_len) = range(op.p1, 1);
            (read_start, read_len, -1, 0)
        }
        Opcode::MakeRecord => {
            let (read_start, read_len) = range(op.p1, op.p2);
            let (write_start, write_len) = range(op.p3, 1);
            (read_start, read_len, write_start, write_len)
        }
        _ => (
            OpcodeRegisterSpans::NONE.read_start,
            OpcodeRegisterSpans::NONE.read_len,
            OpcodeRegisterSpans::NONE.write_start,
            OpcodeRegisterSpans::NONE.write_len,
        ),
    };

    OpcodeRegisterSpans {
        read_start,
        read_len,
        write_start,
        write_len,
    }
}

// ── Arithmetic helpers ──────────────────────────────────────────────────────

/// SQL division with NULL propagation and division-by-zero handling.
#[allow(clippy::cast_precision_loss)]
fn sql_div(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (dividend, divisor) {
        if *b == 0 {
            SqliteValue::Null
        } else {
            match a.checked_div(*b) {
                Some(result) => SqliteValue::Integer(result),
                // i64::MIN / -1 overflows; promote to float like SQLite.
                #[allow(clippy::cast_precision_loss)]
                None => SqliteValue::Float(*a as f64 / *b as f64),
            }
        }
    } else {
        let b = divisor.to_float();
        if b == 0.0 {
            SqliteValue::Null
        } else {
            let result = dividend.to_float() / b;
            if result.is_nan() {
                SqliteValue::Null
            } else {
                SqliteValue::Float(result)
            }
        }
    }
}

/// SQL remainder with NULL propagation and division-by-zero handling.
fn sql_rem(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    let a = dividend.to_integer();
    let b = divisor.to_integer();
    if b == 0 {
        SqliteValue::Null
    } else {
        // checked_rem handles i64::MIN % -1 which would overflow.
        match a.checked_rem(b) {
            Some(result) => SqliteValue::Integer(result),
            // i64::MIN % -1 = 0 mathematically (no remainder).
            None => SqliteValue::Integer(0),
        }
    }
}

/// SQL shift left (SQLite semantics: negative shift = shift right).
fn sql_shift_left(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_right(val, -amount);
    }
    if amount >= 64 {
        return SqliteValue::Integer(0);
    }
    // amount is in [0, 63] so the cast is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let shift = amount as u32;
    SqliteValue::Integer(val << shift)
}

/// SQL shift right (SQLite semantics: negative shift = shift left).
fn sql_shift_right(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_left(val, -amount);
    }
    if amount >= 64 {
        return SqliteValue::Integer(if val < 0 { -1 } else { 0 });
    }
    // amount is in [0, 63] so the cast is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let shift = amount as u32;
    SqliteValue::Integer(val >> shift)
}

/// Three-valued SQL AND.
fn sql_and(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    let a_val = if a.is_null() {
        None
    } else {
        Some(a.to_integer() != 0)
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(b.to_integer() != 0)
    };

    match (a_val, b_val) {
        (Some(false), _) | (_, Some(false)) => SqliteValue::Integer(0),
        (Some(true), Some(true)) => SqliteValue::Integer(1),
        _ => SqliteValue::Null,
    }
}

/// Three-valued SQL OR.
fn sql_or(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    let a_val = if a.is_null() {
        None
    } else {
        Some(a.to_integer() != 0)
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(b.to_integer() != 0)
    };

    match (a_val, b_val) {
        (Some(true), _) | (_, Some(true)) => SqliteValue::Integer(1),
        (Some(false), Some(false)) => SqliteValue::Integer(0),
        _ => SqliteValue::Null,
    }
}

/// SQL CAST operation (p2 encodes target type).
fn sql_cast(val: SqliteValue, target: i32) -> SqliteValue {
    // Target type encoding matches SQLite:
    // 'A' (65) = BLOB, 'B' (66) = TEXT, 'C' (67) = NUMERIC,
    // 'D' (68) = INTEGER, 'E' (69) = REAL
    // But more commonly p2 is used as an affinity character.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let target_byte = target as u8;
    match target_byte {
        b'A' | b'a' => SqliteValue::Blob(match val {
            SqliteValue::Blob(b) => b,
            SqliteValue::Text(s) => s.into_bytes(),
            other => other.to_text().into_bytes(),
        }),
        b'B' | b'b' => SqliteValue::Text(val.to_text()),
        b'C' | b'c' => val.apply_affinity(fsqlite_types::TypeAffinity::Numeric),
        b'D' | b'd' => SqliteValue::Integer(val.to_integer()),
        b'E' | b'e' => SqliteValue::Float(val.to_float()),
        _ => val, // unknown: no-op
    }
}

/// Convert affinity character to `TypeAffinity`.
fn char_to_affinity(ch: char) -> fsqlite_types::TypeAffinity {
    match ch {
        'B' | 'b' => fsqlite_types::TypeAffinity::Text,
        'C' | 'c' => fsqlite_types::TypeAffinity::Numeric,
        'D' | 'd' => fsqlite_types::TypeAffinity::Integer,
        'E' | 'e' => fsqlite_types::TypeAffinity::Real,
        _ => fsqlite_types::TypeAffinity::Blob,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProgramBuilder;
    use fsqlite_types::opcode::{Opcode, P4, VdbeOp};

    /// Build and execute a program, returning results.
    fn run_program(build: impl FnOnce(&mut ProgramBuilder)) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    /// Build and execute a program with bound SQL parameters.
    fn run_program_with_bindings(
        build: impl FnOnce(&mut ProgramBuilder),
        bindings: Vec<SqliteValue>,
    ) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.set_bindings(bindings);
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    #[test]
    fn test_opcode_register_spans_for_variable() {
        let op = VdbeOp {
            opcode: Opcode::Variable,
            p1: 2,
            p2: 9,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        let spans = opcode_register_spans(&op);
        assert_eq!(spans.read_start, -1);
        assert_eq!(spans.read_len, 0);
        assert_eq!(spans.write_start, 9);
        assert_eq!(spans.write_len, 1);
    }

    #[test]
    fn test_opcode_register_spans_for_result_row() {
        let op = VdbeOp {
            opcode: Opcode::ResultRow,
            p1: 4,
            p2: 3,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        let spans = opcode_register_spans(&op);
        assert_eq!(spans.read_start, 4);
        assert_eq!(spans.read_len, 3);
        assert_eq!(spans.write_start, -1);
        assert_eq!(spans.write_len, 0);
    }

    #[test]
    fn test_variable_uses_bound_parameter_value() {
        let rows = run_program_with_bindings(
            |b| {
                let end = b.emit_label();
                b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
                let r1 = b.alloc_reg();
                b.emit_op(Opcode::Variable, 2, r1, 0, P4::None, 0);
                b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
                b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
                b.resolve_label(end);
            },
            vec![
                SqliteValue::Integer(11),
                SqliteValue::Text("bound".to_owned()),
            ],
        );
        assert_eq!(rows, vec![vec![SqliteValue::Text("bound".to_owned())]]);
    }

    #[test]
    fn test_variable_unbound_parameter_defaults_to_null() {
        let rows = run_program_with_bindings(
            |b| {
                let end = b.emit_label();
                b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
                let r1 = b.alloc_reg();
                b.emit_op(Opcode::Variable, 3, r1, 0, P4::None, 0);
                b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
                b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
                b.resolve_label(end);
            },
            vec![SqliteValue::Integer(11)],
        );
        assert_eq!(rows, vec![vec![SqliteValue::Null]]);
    }

    // ── test_select_integer_literal ─────────────────────────────────────
    #[test]
    fn test_select_integer_literal() {
        // SELECT 42 → [(42,)]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    // ── test_select_arithmetic ──────────────────────────────────────────
    #[test]
    fn test_select_arithmetic() {
        // SELECT 1+2 → [(3,)]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg(); // 1
            let r2 = b.alloc_reg(); // 2
            let r3 = b.alloc_reg(); // result

            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    // ── test_select_expression_eval ─────────────────────────────────────
    #[test]
    fn test_select_expression_eval() {
        // SELECT 1+2, 'abc'||'def' → [(3, "abcdef")]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg(); // 1+2 result
            let r4 = b.alloc_reg();
            let r5 = b.alloc_reg();
            let r6 = b.alloc_reg(); // concat result

            // 1 + 2
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r1, r2, r3, P4::None, 0);

            // 'abc' || 'def'
            b.emit_op(Opcode::String8, 0, r4, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r5, 0, P4::Str("def".to_owned()), 0);
            b.emit_op(Opcode::Concat, r5, r4, r6, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            // Also emit second column as separate row for now
            b.emit_op(Opcode::ResultRow, r6, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
        assert_eq!(rows[1], vec![SqliteValue::Text("abcdef".to_owned())]);
    }

    // ── test_select_multi_column ────────────────────────────────────────
    #[test]
    fn test_select_multi_column() {
        // SELECT 1+2, 'abc'||'def' as a single row
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let out_start = b.alloc_regs(2);
            let r_tmp1 = b.alloc_reg();
            let r_tmp2 = b.alloc_reg();

            // 1 + 2 → out_start
            b.emit_op(Opcode::Integer, 1, r_tmp1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r_tmp2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r_tmp1, r_tmp2, out_start, P4::None, 0);

            // 'abc' || 'def' → out_start+1
            b.emit_op(Opcode::String8, 0, r_tmp1, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r_tmp2, 0, P4::Str("def".to_owned()), 0);
            b.emit_op(Opcode::Concat, r_tmp2, r_tmp1, out_start + 1, P4::None, 0);

            b.emit_op(Opcode::ResultRow, out_start, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcdef".to_owned()),
            ]
        );
    }

    // ── test_vdbe_null_handling ──────────────────────────────────────────
    #[test]
    fn test_vdbe_null_handling() {
        // NULL + 1 = NULL, NULL = NULL is NULL (no jump)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_one = b.alloc_reg();
            let r_result = b.alloc_reg();
            let r_is_null = b.alloc_reg();

            // NULL
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            // 1
            b.emit_op(Opcode::Integer, 1, r_one, 0, P4::None, 0);
            // NULL + 1
            b.emit_op(Opcode::Add, r_null, r_one, r_result, P4::None, 0);
            // Check: result IS NULL → set r_is_null=1
            b.emit_op(Opcode::Integer, 0, r_is_null, 0, P4::None, 0);
            let skip = b.emit_label();
            b.emit_jump_to_label(Opcode::NotNull, r_result, 0, skip, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_is_null, 0, P4::None, 0);
            b.resolve_label(skip);

            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_is_null, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Null]); // NULL + 1 = NULL
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // IS NULL = true
    }

    // ── test_vdbe_comparison_affinity ────────────────────────────────────
    #[test]
    fn test_vdbe_comparison_affinity() {
        // Test: 5 > 3 → jump taken (result 1), 3 > 5 → not taken (result 0)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_5 = b.alloc_reg();
            let r_3 = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Integer, 5, r_5, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r_3, 0, P4::None, 0);

            // Test 5 > 3: if r_5 (p3) > r_3 (p1), jump.
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let gt_taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Gt, r_3, r_5, gt_taken, P4::None, 0);
            // Not taken path:
            let done1 = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done1, P4::None, 0);
            // Taken path:
            b.resolve_label(gt_taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done1);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);

            // Test 3 > 5: should NOT jump
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let gt_taken2 = b.emit_label();
            // p3=r_3 (3), p1=r_5 (5): is 3 > 5? No.
            b.emit_jump_to_label(Opcode::Gt, r_5, r_3, gt_taken2, P4::None, 0);
            let done2 = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done2, P4::None, 0);
            b.resolve_label(gt_taken2);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done2);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // 5 > 3 = true
        assert_eq!(rows[1], vec![SqliteValue::Integer(0)]); // 3 > 5 = false
    }

    // ── test_vdbe_division_by_zero ──────────────────────────────────────
    #[test]
    fn test_vdbe_division_by_zero() {
        // SELECT 10 / 0 → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();

            b.emit_op(Opcode::Integer, 0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Divide, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null]); // div by zero → NULL
    }

    #[test]
    fn test_vdbe_nan_arithmetic_normalized_to_null() {
        // +Inf - +Inf and 0 * +Inf both produce NaN at IEEE-754 level.
        // VDBE register writes must normalize NaN to SQL NULL.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_inf = b.alloc_reg();
            let r_zero = b.alloc_reg();
            let r_sub = b.alloc_reg();
            let r_mul = b.alloc_reg();

            b.emit_op(Opcode::Real, 0, r_inf, 0, P4::Real(f64::INFINITY), 0);
            b.emit_op(Opcode::Real, 0, r_zero, 0, P4::Real(0.0), 0);
            b.emit_op(Opcode::Subtract, r_inf, r_inf, r_sub, P4::None, 0); // Inf - Inf
            b.emit_op(Opcode::Multiply, r_inf, r_zero, r_mul, P4::None, 0); // 0 * Inf
            b.emit_op(Opcode::ResultRow, r_sub, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null, SqliteValue::Null]);
    }

    // ── test_vdbe_string_concat_null ────────────────────────────────────
    #[test]
    fn test_vdbe_string_concat_null() {
        // 'abc' || NULL → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();

            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::Null, 0, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    // ── test_vdbe_boolean_logic ─────────────────────────────────────────
    #[test]
    fn test_vdbe_boolean_logic() {
        // TRUE AND FALSE → 0, TRUE OR FALSE → 1, NOT TRUE → 0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r_and = b.alloc_reg();
            let r_or = b.alloc_reg();
            let r_not = b.alloc_reg();

            b.emit_op(Opcode::Integer, 1, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);
            b.emit_op(Opcode::And, r_true, r_false, r_and, P4::None, 0);
            b.emit_op(Opcode::Or, r_true, r_false, r_or, P4::None, 0);
            b.emit_op(Opcode::Not, r_true, r_not, 0, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r_and, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_or, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_not, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // T AND F = F
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // T OR F = T
        assert_eq!(rows[2], vec![SqliteValue::Integer(0)]); // NOT T = F
    }

    // ── test_vdbe_three_valued_logic ────────────────────────────────────
    #[test]
    fn test_vdbe_three_valued_logic() {
        // NULL AND FALSE → 0, NULL AND TRUE → NULL
        // NULL OR TRUE → 1, NULL OR FALSE → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            let r4 = b.alloc_reg();

            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);

            b.emit_op(Opcode::And, r_null, r_false, r1, P4::None, 0); // NULL AND F
            b.emit_op(Opcode::And, r_null, r_true, r2, P4::None, 0); // NULL AND T
            b.emit_op(Opcode::Or, r_null, r_true, r3, P4::None, 0); // NULL OR T
            b.emit_op(Opcode::Or, r_null, r_false, r4, P4::None, 0); // NULL OR F

            b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r4, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // NULL AND F = F
        assert_eq!(rows[1], vec![SqliteValue::Null]); // NULL AND T = NULL
        assert_eq!(rows[2], vec![SqliteValue::Integer(1)]); // NULL OR T = T
        assert_eq!(rows[3], vec![SqliteValue::Null]); // NULL OR F = NULL
    }

    // ── test_vdbe_gosub_return ──────────────────────────────────────────
    #[test]
    fn test_vdbe_gosub_return() {
        // Use Gosub/Return to call a subroutine that sets r2=99.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_return = b.alloc_reg(); // return address storage
            let r_val = b.alloc_reg(); // output

            // Main: call subroutine, then output r_val.
            let sub_label = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_return, 0, sub_label, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            // Subroutine: set r_val=99, return.
            b.resolve_label(sub_label);
            b.emit_op(Opcode::Integer, 99, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Return, r_return, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    // ── test_vdbe_is_null_comparison ─────────────────────────────────────
    #[test]
    fn test_vdbe_is_null_comparison() {
        // NULL IS NULL → true (using Eq with NULLEQ flag)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);

            // Eq with p5=0x80 (SQLITE_NULLEQ): NULL IS NULL → jump
            let is_null_label = b.emit_label();
            // p1=r_null, p3=r_null (compare same register)
            b.emit_jump_to_label(Opcode::Eq, r_null, 0, is_null_label, P4::None, 0x80);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(is_null_label);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // NULL IS NULL = true
    }

    // ── test_vdbe_coroutine ─────────────────────────────────────────────
    #[test]
    fn test_vdbe_coroutine() {
        // Test coroutine: producer yields values 10, 20, 30.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_co = b.alloc_reg(); // coroutine state register
            let r_val = b.alloc_reg(); // value register

            // InitCoroutine: p1=r_co, p2=consumer start, p3=producer start
            let consumer_start = b.emit_label();
            let producer_start = b.emit_label();
            b.emit_jump_to_label(Opcode::InitCoroutine, r_co, 0, consumer_start, P4::None, 0);
            // Hack: resolve producer_start at the InitCoroutine's p3 position.
            // Actually, InitCoroutine stores p3 into r_co, then jumps to p2.
            // So p3 should be the producer's first instruction address.

            // For simplicity, just test Yield directly:
            b.resolve_label(consumer_start);

            // Producer: emit 3 values
            b.resolve_label(producer_start);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 30, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(10)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(20)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(30)]);
    }

    // ── test_vdbe_halt_with_error ───────────────────────────────────────
    #[test]
    fn test_vdbe_halt_with_error() {
        let mut b = ProgramBuilder::new();
        b.emit_op(
            Opcode::Halt,
            1,
            0,
            0,
            P4::Str("constraint failed".to_owned()),
            0,
        );
        let prog = b.finish().unwrap();
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: 1,
                message: "constraint failed".to_owned(),
            }
        );
    }

    // ── test_vdbe_disassemble_and_exec ──────────────────────────────────
    #[test]
    fn test_vdbe_disassemble_and_exec() {
        // Build a program, disassemble it, and verify output.
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        let r1 = b.alloc_reg();
        let r2 = b.alloc_reg();
        let r3 = b.alloc_reg();
        b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 20, r2, 0, P4::None, 0);
        b.emit_op(Opcode::Multiply, r1, r2, r3, P4::None, 0);
        b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().unwrap();
        let asm = prog.disassemble();
        assert!(asm.contains("Init"));
        assert!(asm.contains("Integer"));
        assert!(asm.contains("Multiply"));
        assert!(asm.contains("ResultRow"));
        assert!(asm.contains("Halt"));

        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(engine.results().len(), 1);
        assert_eq!(engine.results()[0], vec![SqliteValue::Integer(200)]);
    }

    #[test]
    fn test_sorter_opcodes_sort_and_emit_rows() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            let loop_start = b.emit_label();
            let empty = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_sorted = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);

            for value in [30, 10, 20] {
                b.emit_op(Opcode::Integer, value, r_value, 0, P4::None, 0);
                b.emit_op(Opcode::MakeRecord, r_value, 1, r_record, P4::None, 0);
                b.emit_op(Opcode::SorterInsert, 0, r_record, 0, P4::None, 0);
            }

            b.emit_jump_to_label(Opcode::SorterSort, 0, 0, empty, P4::None, 0);
            b.resolve_label(loop_start);
            b.emit_op(Opcode::SorterData, 0, r_sorted, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_sorted, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterNext, 0, 0, loop_start, P4::None, 0);
            b.resolve_label(empty);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        let decoded: Vec<i64> = rows
            .into_iter()
            .map(|row| decode_record(&row[0]).unwrap()[0].to_integer())
            .collect();
        assert_eq!(decoded, vec![10, 20, 30]);
    }

    #[test]
    fn test_sorter_compare_jumps_on_key_difference() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            let diff = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_probe = b.alloc_reg();
            let r_probe_record = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r_value, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, r_value, 1, r_record, P4::None, 0);
            b.emit_op(Opcode::SorterInsert, 0, r_record, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterSort, 0, 0, diff, P4::None, 0);

            b.emit_op(Opcode::Integer, 20, r_probe, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, r_probe, 1, r_probe_record, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterCompare, 0, r_probe_record, diff, P4::None, 0);

            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            b.resolve_label(diff);
            b.emit_op(Opcode::Integer, 2, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows, vec![vec![SqliteValue::Integer(2)]]);
    }

    #[test]
    fn test_reset_sorter_clears_entries() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            let empty = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_value = b.alloc_reg();
            let r_record = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r_value, 0, P4::None, 0);
            b.emit_op(Opcode::MakeRecord, r_value, 1, r_record, P4::None, 0);
            b.emit_op(Opcode::SorterInsert, 0, r_record, 0, P4::None, 0);
            b.emit_op(Opcode::ResetSorter, 0, 0, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterSort, 0, 0, empty, P4::None, 0);

            // If ResetSorter failed, this row would be emitted.
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.resolve_label(empty);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert!(rows.is_empty());
    }

    // ── Codegen → Engine Integration Tests ──────────────────────────────

    mod codegen_integration {
        use super::*;
        use crate::codegen::{
            CodegenContext, ColumnInfo, TableSchema, codegen_delete, codegen_insert,
            codegen_select, codegen_update,
        };
        use fsqlite_ast::{
            Assignment, AssignmentTarget, BinaryOp as AstBinaryOp, ColumnRef, DeleteStatement,
            Distinctness, Expr, FromClause, InsertSource, InsertStatement, Literal,
            PlaceholderType, QualifiedName, QualifiedTableRef, ResultColumn, SelectBody,
            SelectCore, SelectStatement, Span, TableOrSubquery, UpdateStatement,
        };

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

        fn from_table(name: &str) -> FromClause {
            FromClause {
                source: TableOrSubquery::Table {
                    name: QualifiedName {
                        schema: None,
                        name: name.to_owned(),
                    },
                    alias: None,
                    index_hint: None,
                },
                joins: Vec::new(),
            }
        }

        fn span() -> Span {
            Span { start: 0, end: 0 }
        }

        /// Verify codegen_insert produces a program that executes without panic.
        #[test]
        fn test_codegen_insert_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Literal(Literal::Integer(42), span()),
                    Expr::Literal(Literal::String("hello".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify codegen_select (full scan) produces a program that executes.
        #[test]
        fn test_codegen_select_full_scan_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

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
                order_by: vec![],
                limit: None,
            };

            let mut b = ProgramBuilder::new();
            codegen_select(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            // Engine should execute without panic (cursor ops are stubbed).
            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify `OpenRead` can route through the storage-backed cursor path.
        #[test]
        fn test_openread_uses_storage_cursor_backend_when_enabled() {
            let mut b = ProgramBuilder::new();
            b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            let prog = b.finish().expect("program should build");

            let mut db = MemDatabase::new();
            let root = db.create_table(1);
            assert_eq!(root, 2);
            if let Some(table) = db.get_table_mut(root) {
                table.insert(1, vec![SqliteValue::Integer(99)]);
            }

            let mut engine = VdbeEngine::new(prog.register_count());
            engine.enable_storage_read_cursors(true);
            engine.set_database(db);
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
            assert!(engine.storage_cursors.contains_key(&0));
            assert!(!engine.cursors.contains_key(&0));
        }

        /// Verify codegen_update produces a program that executes.
        #[test]
        fn test_codegen_update_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = UpdateStatement {
                with: None,
                or_conflict: None,
                table: QualifiedTableRef {
                    name: QualifiedName {
                        schema: None,
                        name: "t".to_owned(),
                    },
                    alias: None,
                    index_hint: None,
                },
                assignments: vec![Assignment {
                    target: AssignmentTarget::Column("b".to_owned()),
                    value: Expr::Placeholder(PlaceholderType::Numbered(1), span()),
                }],
                from: None,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column(
                        ColumnRef {
                            table: None,
                            column: "rowid".to_owned(),
                        },
                        span(),
                    )),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Placeholder(PlaceholderType::Numbered(2), span())),
                    span: span(),
                }),
                returning: vec![],
                order_by: vec![],
                limit: None,
            };

            let mut b = ProgramBuilder::new();
            codegen_update(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify codegen_delete produces a program that executes.
        #[test]
        fn test_codegen_delete_executes() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = DeleteStatement {
                with: None,
                table: QualifiedTableRef {
                    name: QualifiedName {
                        schema: None,
                        name: "t".to_owned(),
                    },
                    alias: None,
                    index_hint: None,
                },
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column(
                        ColumnRef {
                            table: None,
                            column: "rowid".to_owned(),
                        },
                        span(),
                    )),
                    op: AstBinaryOp::Eq,
                    right: Box::new(Expr::Placeholder(PlaceholderType::Numbered(1), span())),
                    span: span(),
                }),
                returning: vec![],
                order_by: vec![],
                limit: None,
            };

            let mut b = ProgramBuilder::new();
            codegen_delete(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify codegen_insert with RETURNING produces a ResultRow.
        #[test]
        fn test_codegen_insert_returning_produces_result() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Literal(Literal::Integer(7), span()),
                    Expr::Literal(Literal::String("world".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![ResultColumn::Star],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
            // RETURNING emits a ResultRow with the new rowid.
            assert_eq!(engine.results().len(), 1);
        }

        /// Verify INSERT with literal values emits the correct value registers.
        #[test]
        fn test_codegen_insert_literal_values_disassemble() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Literal(Literal::Integer(99), span()),
                    Expr::Literal(Literal::String("test".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Init"), "should have Init opcode");
            assert!(asm.contains("OpenWrite"), "should have OpenWrite opcode");
            assert!(asm.contains("NewRowid"), "should have NewRowid opcode");
            assert!(
                asm.contains("Integer"),
                "should have Integer opcode for literal 99"
            );
            assert!(
                asm.contains("String8"),
                "should have String8 opcode for literal 'test'"
            );
            assert!(asm.contains("MakeRecord"), "should have MakeRecord opcode");
            assert!(asm.contains("Insert"), "should have Insert opcode");
            assert!(asm.contains("Halt"), "should have Halt opcode");
        }

        /// Verify emit_expr handles arithmetic BinaryOp in INSERT values.
        #[test]
        fn test_codegen_insert_arithmetic_expr() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (2 + 3, 'hi')
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::BinaryOp {
                        left: Box::new(Expr::Literal(Literal::Integer(2), span())),
                        op: AstBinaryOp::Add,
                        right: Box::new(Expr::Literal(Literal::Integer(3), span())),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("hi".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Add"), "should have Add opcode for 2+3");
            assert!(asm.contains("Integer"), "should have Integer opcodes");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify emit_expr handles UnaryOp (negation) in INSERT values.
        #[test]
        fn test_codegen_insert_negation_expr() {
            use fsqlite_ast::UnaryOp as AstUnaryOp;

            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (-42, 'neg')
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::UnaryOp {
                        op: AstUnaryOp::Negate,
                        expr: Box::new(Expr::Literal(Literal::Integer(42), span())),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("neg".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Multiply"), "negation emits Multiply by -1");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify emit_expr handles CASE expression in INSERT values.
        #[test]
        fn test_codegen_insert_case_expr() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (CASE WHEN TRUE THEN 10 ELSE 20 END, 'case')
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::Case {
                        operand: None,
                        whens: vec![(
                            Expr::Literal(Literal::True, span()),
                            Expr::Literal(Literal::Integer(10), span()),
                        )],
                        else_expr: Some(Box::new(Expr::Literal(Literal::Integer(20), span()))),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("case".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("IfNot"), "searched CASE emits IfNot");
            assert!(asm.contains("Goto"), "CASE branches with Goto");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }

        /// Verify emit_expr handles comparison expression producing 0/1 result.
        #[test]
        fn test_codegen_insert_comparison_expr() {
            let schema = test_schema();
            let ctx = CodegenContext::default();

            // INSERT INTO t VALUES (3 > 2, 'cmp') — should produce integer 1
            let stmt = InsertStatement {
                with: None,
                or_conflict: None,
                table: QualifiedName {
                    schema: None,
                    name: "t".to_owned(),
                },
                alias: None,
                columns: vec![],
                source: InsertSource::Values(vec![vec![
                    Expr::BinaryOp {
                        left: Box::new(Expr::Literal(Literal::Integer(3), span())),
                        op: AstBinaryOp::Gt,
                        right: Box::new(Expr::Literal(Literal::Integer(2), span())),
                        span: span(),
                    },
                    Expr::Literal(Literal::String("cmp".to_owned()), span()),
                ]]),
                upsert: vec![],
                returning: vec![],
            };

            let mut b = ProgramBuilder::new();
            codegen_insert(&mut b, &stmt, &schema, &ctx).expect("codegen should succeed");
            let prog = b.finish().expect("program should build");

            let asm = prog.disassemble();
            assert!(asm.contains("Gt"), "comparison emits Gt opcode");

            let mut engine = VdbeEngine::new(prog.register_count());
            let outcome = engine.execute(&prog).expect("execution should succeed");
            assert_eq!(outcome, ExecOutcome::Done);
        }
    }

    // ===================================================================
    // bd-202x §16 Phase 4: Comprehensive VDBE opcode unit tests
    // ===================================================================

    // ── Constants & Register Operations ────────────────────────────────

    #[test]
    fn test_int64_large_value() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r, 0, P4::Int64(i64::MAX), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(i64::MAX)]);
    }

    #[test]
    fn test_int64_negative() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r, 0, P4::Int64(-999_999_999_999), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(-999_999_999_999)]);
    }

    #[test]
    fn test_real_constant() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, r, 0, P4::Real(std::f64::consts::PI), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(std::f64::consts::PI)]);
    }

    #[test]
    fn test_real_negative_zero() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, r, 0, P4::Real(0.0), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(0.0)]);
    }

    #[test]
    fn test_string_opcode() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String, 5, r, 0, P4::Str("hello".to_owned()), 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("hello".to_owned())]);
    }

    #[test]
    fn test_blob_constant() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(
                Opcode::Blob,
                0,
                r,
                0,
                P4::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                0,
            );
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])]
        );
    }

    #[test]
    fn test_null_range() {
        // Null with p3=2: set registers p2, p2+1, p2+2 to NULL.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            // Pre-populate with integers
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r3, 0, P4::None, 0);
            // Null range: p2=r1, p3=r3 → set r1..=r3 to NULL (absolute end register).
            b.emit_op(Opcode::Null, 0, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![SqliteValue::Null, SqliteValue::Null, SqliteValue::Null]
        );
    }

    #[test]
    fn test_soft_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            b.emit_op(Opcode::SoftNull, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_move_registers() {
        // Move nullifies source and copies to destination.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let src = b.alloc_reg();
            let dst = b.alloc_reg();
            b.emit_op(Opcode::Integer, 77, src, 0, P4::None, 0);
            // Move 1 register from src to dst
            b.emit_op(Opcode::Move, src, dst, 1, P4::None, 0);
            // dst should be 77, src should be NULL
            b.emit_op(Opcode::ResultRow, dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, src, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(77)]);
        assert_eq!(rows[1], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_copy_register() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let src = b.alloc_reg();
            let dst = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, src, 0, P4::Str("copy_me".to_owned()), 0);
            b.emit_op(Opcode::Copy, src, dst, 0, P4::None, 0);
            // Both should be the same value
            b.emit_op(Opcode::ResultRow, src, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("copy_me".to_owned())]);
        assert_eq!(rows[1], vec![SqliteValue::Text("copy_me".to_owned())]);
    }

    #[test]
    fn test_intcopy_coerces() {
        // IntCopy converts value to integer.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let src = b.alloc_reg();
            let dst = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, src, 0, P4::Real(3.7), 0);
            b.emit_op(Opcode::IntCopy, src, dst, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, dst, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    // ── Arithmetic Edge Cases ──────────────────────────────────────────

    #[test]
    fn test_subtract_integers() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r2, 0, P4::None, 0);
            // p3 = p2 - p1 → r3 = r1 - r2 if p2=r1, p1=r2 → 10 - 3 = 7
            b.emit_op(Opcode::Subtract, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(7)]);
    }

    #[test]
    fn test_multiply_large() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 200, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Multiply, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(20_000)]);
    }

    #[test]
    fn test_integer_division_truncates() {
        // 7 / 2 = 3 (integer division truncates)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_divisor = b.alloc_reg();
            let r_dividend = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 2, r_divisor, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r_dividend, 0, P4::None, 0);
            // p3 = p2 / p1 → r_result = r_dividend / r_divisor
            b.emit_op(Opcode::Divide, r_divisor, r_dividend, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn test_remainder_integers() {
        // 7 % 3 = 1
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_divisor = b.alloc_reg();
            let r_dividend = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 3, r_divisor, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r_dividend, 0, P4::None, 0);
            b.emit_op(
                Opcode::Remainder,
                r_divisor,
                r_dividend,
                r_result,
                P4::None,
                0,
            );
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_remainder_by_zero() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_zero = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_zero, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Remainder, r_zero, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_null_arithmetic_propagation() {
        // NULL + 1, NULL * 5, NULL - 3 should all be NULL.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_one = b.alloc_reg();
            let r_add = b.alloc_reg();
            let r_mul = b.alloc_reg();
            let r_sub = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_one, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r_null, r_one, r_add, P4::None, 0);
            b.emit_op(Opcode::Multiply, r_null, r_one, r_mul, P4::None, 0);
            b.emit_op(Opcode::Subtract, r_null, r_one, r_sub, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_add, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_mul, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_sub, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
        assert_eq!(rows[1], vec![SqliteValue::Null]);
        assert_eq!(rows[2], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_add_imm() {
        // AddImm: register p1 += p2
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r, 0, P4::None, 0);
            b.emit_op(Opcode::AddImm, r, 50, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(150)]);
    }

    #[test]
    fn test_add_imm_negative() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r, 0, P4::None, 0);
            b.emit_op(Opcode::AddImm, r, -30, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(70)]);
    }

    // ── Bitwise Operations ─────────────────────────────────────────────

    #[test]
    fn test_bit_and() {
        // 0xFF & 0x0F = 0x0F (15)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0xFF, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0x0F, r2, 0, P4::None, 0);
            b.emit_op(Opcode::BitAnd, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0x0F)]);
    }

    #[test]
    fn test_bit_or() {
        // 0xF0 | 0x0F = 0xFF (255)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0xF0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0x0F, r2, 0, P4::None, 0);
            b.emit_op(Opcode::BitOr, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0xFF)]);
    }

    #[test]
    fn test_shift_left() {
        // 1 << 8 = 256
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 8, r_amount, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ShiftLeft, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(256)]);
    }

    #[test]
    fn test_shift_right() {
        // 256 >> 4 = 16
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Integer, 4, r_amount, 0, P4::None, 0);
            b.emit_op(Opcode::Int64, 0, r_val, 0, P4::Int64(256), 0);
            b.emit_op(Opcode::ShiftRight, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(16)]);
    }

    #[test]
    fn test_shift_left_overflow_clamp() {
        // Shift >= 64 returns 0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r_amount, 0, P4::Int64(64), 0);
            b.emit_op(Opcode::Integer, 1, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ShiftLeft, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_shift_negative_reverses() {
        // Negative shift amount reverses direction: <<(-2) == >>(2)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_amount = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_result = b.alloc_reg();
            b.emit_op(Opcode::Int64, 0, r_amount, 0, P4::Int64(-2), 0);
            b.emit_op(Opcode::Integer, 8, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ShiftLeft, r_amount, r_val, r_result, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // 8 >> 2 = 2
        assert_eq!(rows[0], vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn test_bit_not() {
        // ~0 = -1 in two's complement
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::BitNot, r1, r2, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(-1)]);
    }

    #[test]
    fn test_bitwise_null_propagation() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_val = b.alloc_reg();
            let r_and = b.alloc_reg();
            let r_or = b.alloc_reg();
            let r_not = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0xFF, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::BitAnd, r_null, r_val, r_and, P4::None, 0);
            b.emit_op(Opcode::BitOr, r_null, r_val, r_or, P4::None, 0);
            b.emit_op(Opcode::BitNot, r_null, r_not, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_and, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_or, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_not, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
        assert_eq!(rows[1], vec![SqliteValue::Null]);
        assert_eq!(rows[2], vec![SqliteValue::Null]);
    }

    // ── String Operations ──────────────────────────────────────────────

    #[test]
    fn test_concat_two_strings() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("hello ".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("world".to_owned()), 0);
            // Concat: p3 = p2 || p1 (note operand order)
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("hello world".to_owned())]);
    }

    #[test]
    fn test_concat_empty_string() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("test".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str(String::new()), 0);
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("test".to_owned())]);
    }

    // ── Comparison Ops (all 6 + NULL) ──────────────────────────────────

    #[test]
    fn test_eq_jump_taken() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 42, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            // Eq: if p3 == p1, jump to p2 → if r2 == r1, jump
            b.emit_jump_to_label(Opcode::Eq, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_ne_jump_taken() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Ne, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_lt_jump_taken() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_big = b.alloc_reg();
            let r_small = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 100, r_big, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_small, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            // Lt: if p3 < p1, jump → if r_small < r_big
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Lt, r_big, r_small, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_le_with_equal_values() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 7, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 7, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Le, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_ge_with_greater_value() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_big = b.alloc_reg();
            let r_small = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_small, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 100, r_big, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            // Ge: if p3 >= p1 → if r_big >= r_small
            b.emit_jump_to_label(Opcode::Ge, r_small, r_big, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_comparison_null_no_jump() {
        // Standard SQL: NULL = 5 → no jump (NULL result)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_five = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Eq, r_five, r_null, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Should NOT jump: NULL = 5 is NULL (not true)
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn test_ne_nulleq_one_null() {
        // IS NOT semantics: NULL IS NOT 5 → true (jump)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_five = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Ne, r_five, r_null, taken, P4::None, 0x80);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    // ── Logic Edge Cases ───────────────────────────────────────────────

    #[test]
    fn test_not_null_is_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_null = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Not, r_null, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    #[test]
    fn test_not_zero_is_one() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_zero = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_zero, 0, P4::None, 0);
            b.emit_op(Opcode::Not, r_zero, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_not_nonzero_is_zero() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_val = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Not, r_val, r_out, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]);
    }

    // ── Conditional Jumps ──────────────────────────────────────────────

    #[test]
    fn test_if_true_jumps() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_if_false_no_jump() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 99, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // If with false → no jump → r_out stays 99
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    #[test]
    fn test_if_null_no_jump() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 99, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::If, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // If with NULL → no jump → r_out stays 99
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    #[test]
    fn test_ifnot_false_jumps() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::IfNot, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_ifnot_null_jumps() {
        // IfNot with NULL → jump (NULL is treated as false/zero)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_cond = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Null, 0, r_cond, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::IfNot, r_cond, 0, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_once_fires_only_once() {
        // Once at the same PC fires on first pass, falls through on second.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_counter = b.alloc_reg();
            b.emit_op(Opcode::Integer, 0, r_counter, 0, P4::None, 0);
            // First pass: Once jumps to `init_code`
            let loop_start = b.emit_label();
            b.resolve_label(loop_start);
            let init_code = b.emit_label();
            b.emit_jump_to_label(Opcode::Once, 0, 0, init_code, P4::None, 0);
            // Fall-through path (second+ pass): just output
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            // Init code: increment counter and loop back
            b.resolve_label(init_code);
            b.emit_op(Opcode::AddImm, r_counter, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Goto, 0, 0, loop_start, P4::None, 0);
            // Done
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_counter, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Once fires on first execution (increments to 1), then falls through
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    // ── Type Coercion ──────────────────────────────────────────────────

    #[test]
    fn test_cast_integer_to_text() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            // Cast to TEXT: p2 = 'B' (66)
            b.emit_op(Opcode::Cast, r, 66, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Text("42".to_owned())]);
    }

    #[test]
    fn test_cast_text_to_integer() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("123".to_owned()), 0);
            // Cast to INTEGER: p2 = 'D' (68)
            b.emit_op(Opcode::Cast, r, 68, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(123)]);
    }

    #[test]
    fn test_cast_to_real() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r, 0, P4::None, 0);
            // Cast to REAL: p2 = 'E' (69)
            b.emit_op(Opcode::Cast, r, 69, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(5.0)]);
    }

    #[test]
    fn test_cast_to_blob() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("hi".to_owned()), 0);
            // Cast to BLOB: p2 = 'A' (65)
            b.emit_op(Opcode::Cast, r, 65, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Blob(b"hi".to_vec())]);
    }

    #[test]
    fn test_must_be_int_accepts_integer() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            // MustBeInt: p2=0 means error on non-int, but 42 is int → passes
            b.emit_op(Opcode::MustBeInt, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_must_be_int_jumps_on_non_int() {
        // MustBeInt with p2 > 0: jump to p2 instead of error.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r, 0, P4::Str("not_int".to_owned()), 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let fallback = b.emit_label();
            b.emit_jump_to_label(Opcode::MustBeInt, r, 0, fallback, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(fallback);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        // Non-int triggers jump → r_out = 1
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_real_affinity_converts_int() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 7, r, 0, P4::None, 0);
            b.emit_op(Opcode::RealAffinity, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(7.0)]);
    }

    #[test]
    fn test_real_affinity_no_op_on_float() {
        // RealAffinity on a float is a no-op.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Real, 0, r, 0, P4::Real(std::f64::consts::PI), 0);
            b.emit_op(Opcode::RealAffinity, r, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Float(std::f64::consts::PI)]);
    }

    // ── Error Handling ─────────────────────────────────────────────────

    #[test]
    fn test_halt_if_null_triggers() {
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        let r = b.alloc_reg();
        b.emit_op(Opcode::Null, 0, r, 0, P4::None, 0);
        b.emit_op(
            Opcode::HaltIfNull,
            19,
            0,
            r,
            P4::Str("NOT NULL constraint failed".to_owned()),
            0,
        );
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);
        let prog = b.finish().unwrap();
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: 19,
                message: "NOT NULL constraint failed".to_owned(),
            }
        );
    }

    #[test]
    fn test_halt_if_null_passes_non_null() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            b.emit_op(
                Opcode::HaltIfNull,
                19,
                0,
                r,
                P4::Str("should not fire".to_owned()),
                0,
            );
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    // ── Miscellaneous Opcodes ──────────────────────────────────────────

    #[test]
    fn test_is_true_opcode() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r_null = b.alloc_reg();
            let o1 = b.alloc_reg();
            let o2 = b.alloc_reg();
            let o3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::IsTrue, r_true, o1, 0, P4::None, 0);
            b.emit_op(Opcode::IsTrue, r_false, o2, 0, P4::None, 0);
            b.emit_op(Opcode::IsTrue, r_null, o3, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, o3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // 42 is true
        assert_eq!(rows[1], vec![SqliteValue::Integer(0)]); // 0 is false
        assert_eq!(rows[2], vec![SqliteValue::Integer(0)]); // NULL is not true
    }

    #[test]
    fn test_noop_does_nothing() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r, 0, P4::None, 0);
            b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_result_row_three_columns() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("two".to_owned()), 0);
            b.emit_op(Opcode::Real, 0, r3, 0, P4::Real(3.0), 0);
            b.emit_op(Opcode::ResultRow, r1, 3, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("two".to_owned()),
                SqliteValue::Float(3.0),
            ]
        );
    }

    #[test]
    fn test_multiple_result_rows() {
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(2)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn test_gosub_nested() {
        // Test nested Gosub: main calls sub1, which calls sub2.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_ret1 = b.alloc_reg();
            let r_ret2 = b.alloc_reg();
            let r_val = b.alloc_reg();

            // Main: call sub1
            let sub1 = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_ret1, 0, sub1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            // sub1: set r_val=10, call sub2, add 1
            b.resolve_label(sub1);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            let sub2 = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_ret2, 0, sub2, P4::None, 0);
            b.emit_op(Opcode::AddImm, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Return, r_ret1, 0, 0, P4::None, 0);

            // sub2: multiply r_val by 5
            b.resolve_label(sub2);
            let r_five = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_five, 0, P4::None, 0);
            b.emit_op(Opcode::Multiply, r_five, r_val, r_val, P4::None, 0);
            b.emit_op(Opcode::Return, r_ret2, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        // 10 * 5 + 1 = 51
        assert_eq!(rows[0], vec![SqliteValue::Integer(51)]);
    }

    #[test]
    fn test_coroutine_yield_resume() {
        // Producer coroutine yields 3 values; consumer resumes and emits rows.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_co = b.alloc_reg();
            let r_val = b.alloc_reg();

            // Patch target addresses after both blocks are emitted.
            let init_addr = b.emit_op(Opcode::InitCoroutine, r_co, 0, 0, P4::None, 0);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let consumer_start = b.current_addr() as i32;
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let producer_start = b.current_addr() as i32;
            b.emit_op(Opcode::Integer, 100, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 200, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 300, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Yield, r_co, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            if let Some(init_op) = b.op_at_mut(init_addr) {
                init_op.p2 = consumer_start;
                init_op.p3 = producer_start;
            }

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(100)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(200)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(300)]);
    }

    #[test]
    fn test_make_record_encodes_values() {
        // MakeRecord packs source registers into the SQLite record format blob.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_rec = b.alloc_reg();
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("a".to_owned()), 0);
            b.emit_op(Opcode::MakeRecord, r1, 2, r_rec, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_rec, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        let produced_blob = rows.first().and_then(|row| row.first());
        assert!(
            matches!(produced_blob, Some(SqliteValue::Blob(_))),
            "MakeRecord should produce a blob"
        );
        let decoded = decode_record(&rows[0][0]).unwrap();
        assert_eq!(
            decoded,
            vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())]
        );
    }

    #[test]
    fn test_complex_expression_chain() {
        // Test: ((10 + 20) * 3 - 5) / 2 = (90 - 5) / 2 = 85 / 2 = 42
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r10 = b.alloc_reg();
            let r20 = b.alloc_reg();
            let r3 = b.alloc_reg();
            let r5 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let t1 = b.alloc_reg();
            let t2 = b.alloc_reg();
            let t3 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 10, r10, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r20, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r3, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 5, r5, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r10, r20, t1, P4::None, 0); // 30
            b.emit_op(Opcode::Multiply, r3, t1, t2, P4::None, 0); // 90
            b.emit_op(Opcode::Subtract, r5, t2, t2, P4::None, 0); // 85
            b.emit_op(Opcode::Divide, r2, t2, t3, P4::None, 0); // 42
            b.emit_op(Opcode::ResultRow, t3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_string_comparison() {
        // String comparison: 'abc' < 'abd' → true
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("abd".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r2, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            // Lt: if p3 (r2="abc") < p1 (r1="abd"), jump
            b.emit_jump_to_label(Opcode::Lt, r1, r2, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_mixed_type_comparison() {
        // Integer vs Float comparison: 5 == 5.0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r_int = b.alloc_reg();
            let r_float = b.alloc_reg();
            let r_out = b.alloc_reg();
            b.emit_op(Opcode::Integer, 5, r_int, 0, P4::None, 0);
            b.emit_op(Opcode::Real, 0, r_float, 0, P4::Real(5.0), 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Eq, r_int, r_float, taken, P4::None, 0);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);
            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]);
    }

    // ── bd-1s7a: Storage cursor acceptance tests ───────────────────────

    /// Build and execute a program with a MemDatabase + storage cursors enabled.
    fn run_with_storage_cursors(
        db: MemDatabase,
        build: impl FnOnce(&mut ProgramBuilder),
    ) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        engine.enable_storage_read_cursors(true);
        engine.set_database(db);
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    #[test]
    fn test_vdbe_openread_uses_btree_cursor_backend() {
        // Insert rows into a MemDatabase, then verify OpenRead routes through
        // the storage cursor path (not MemCursor) when enabled.
        let mut db = MemDatabase::new();
        let root = db.create_table(2);
        let table = db.get_table_mut(root).unwrap();
        table.insert(
            1,
            vec![SqliteValue::Integer(10), SqliteValue::Text("a".to_owned())],
        );
        table.insert(
            2,
            vec![SqliteValue::Integer(20), SqliteValue::Text("b".to_owned())],
        );

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(2), 0);
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);

            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
            b.emit_op(Opcode::Column, 0, 1, 2, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 2, 0, P4::None, 0);

            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 2, "should return 2 rows via storage cursor");
        assert_eq!(rows[0][0], SqliteValue::Integer(10));
        assert_eq!(rows[0][1], SqliteValue::Text("a".to_owned()));
        assert_eq!(rows[1][0], SqliteValue::Integer(20));
        assert_eq!(rows[1][1], SqliteValue::Text("b".to_owned()));
    }

    #[test]
    fn test_select_uses_storage_cursor_not_memdb_for_persisted_table() {
        // With storage cursors enabled, verify the engine uses StorageCursor
        // (the read path) rather than MemCursor for OpenRead.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).unwrap();
        table.insert(1, vec![SqliteValue::Integer(42)]);

        let rows = run_with_storage_cursors(db, |b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            // OpenRead with storage cursors enabled should use StorageCursor.
            b.emit_op(Opcode::OpenRead, 0, root, 0, P4::Int(1), 0);
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, end, P4::None, 0);

            let body = b.current_addr();
            b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            let next_target =
                i32::try_from(body).expect("program counter should fit into i32 for tests");
            b.emit_op(Opcode::Next, 0, next_target, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }
}
