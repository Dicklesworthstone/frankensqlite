// bd-gird: §10.7-10.8 VDBE Instruction Format + Coroutines
//
// This crate provides the VDBE (Virtual Database Engine) program builder,
// label resolution, register allocation, coroutine mechanism, and disassembly.
// The foundational types (Opcode, VdbeOp, P4) live in fsqlite-types.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::opcode::{Opcode, P4, VdbeOp};

pub mod codegen;
pub mod engine;
pub mod frame;

// ── Label System ────────────────────────────────────────────────────────────

/// An opaque handle representing a forward-reference label.
///
/// Labels allow codegen to emit jump instructions before the target address
/// is known. All labels MUST be resolved before execution begins; unresolved
/// labels are a codegen bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Label(usize);

/// Internal tracking for label resolution.
#[derive(Debug)]
enum LabelState {
    /// Not yet resolved. Contains the indices of instructions whose `p2`
    /// field should be patched when the label is resolved.
    Unresolved(Vec<usize>),
    /// Resolved to a concrete instruction address.
    Resolved(i32),
}

// ── Sort Order ──────────────────────────────────────────────────────────────

/// Sort direction for key comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending order (default).
    Asc,
    /// Descending order.
    Desc,
}

// ── KeyInfo ─────────────────────────────────────────────────────────────────

/// Describes the key structure for multi-column index comparisons.
///
/// Used by Compare, IdxInsert, IdxDelete, and seek operations. Each field
/// has an associated collation sequence and sort order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyInfo {
    /// Number of key fields.
    pub num_fields: u16,
    /// Collation sequence name per field (one entry per `num_fields`).
    pub collations: Vec<String>,
    /// Sort direction per field.
    pub sort_orders: Vec<SortOrder>,
}

// ── Coroutine State ─────────────────────────────────────────────────────────

/// Tracks the execution state of a coroutine.
///
/// Coroutines in VDBE are cooperative PC-swap state machines (NOT async).
/// `InitCoroutine` initializes the state, `Yield` swaps PCs bidirectionally,
/// and `EndCoroutine` marks exhaustion and returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoroutineState {
    /// The register that stores the yield/resume PC.
    pub yield_reg: i32,
    /// The saved program counter (where to resume).
    pub saved_pc: i32,
    /// Whether the coroutine has been exhausted (EndCoroutine reached).
    pub exhausted: bool,
}

impl CoroutineState {
    /// Create a new coroutine state with the given yield register and
    /// initial body address.
    pub fn new(yield_reg: i32, body_pc: i32) -> Self {
        Self {
            yield_reg,
            saved_pc: body_pc,
            exhausted: false,
        }
    }

    /// Perform a bidirectional PC swap (Yield semantics).
    ///
    /// The current PC is saved into this state, and the previously saved PC
    /// is returned as the new PC to jump to.
    pub fn yield_swap(&mut self, current_pc: i32) -> i32 {
        let resume_at = self.saved_pc;
        self.saved_pc = current_pc;
        resume_at
    }

    /// Mark the coroutine as exhausted (EndCoroutine semantics).
    ///
    /// Returns the saved PC to return to the caller.
    pub fn end(&mut self) -> i32 {
        self.exhausted = true;
        self.saved_pc
    }
}

// ── Register Allocator ──────────────────────────────────────────────────────

/// Sequential register allocator for the VDBE register file.
///
/// Registers are numbered starting at 1 (register 0 is reserved/unused,
/// matching C SQLite convention). The allocator supports both persistent
/// registers (held for statement lifetime) and temporary registers that
/// can be returned to a reuse pool.
#[derive(Debug)]
pub struct RegisterAllocator {
    /// The next register number to allocate (starts at 1).
    next_reg: i32,
    /// Pool of returned temporary registers available for reuse.
    temp_pool: Vec<i32>,
}

impl RegisterAllocator {
    /// Create a new allocator. First allocation returns register 1.
    pub fn new() -> Self {
        Self {
            next_reg: 1,
            temp_pool: Vec::new(),
        }
    }

    /// Allocate a single persistent register.
    pub fn alloc_reg(&mut self) -> i32 {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    /// Allocate a contiguous block of `n` persistent registers.
    ///
    /// Returns the first register number. The block spans `[result, result+n)`.
    pub fn alloc_regs(&mut self, n: i32) -> i32 {
        let first = self.next_reg;
        self.next_reg += n;
        first
    }

    /// Allocate a temporary register (reuses from pool if available).
    pub fn alloc_temp(&mut self) -> i32 {
        self.temp_pool.pop().unwrap_or_else(|| {
            let reg = self.next_reg;
            self.next_reg += 1;
            reg
        })
    }

    /// Return a temporary register to the reuse pool.
    pub fn free_temp(&mut self, reg: i32) {
        self.temp_pool.push(reg);
    }

    /// The total number of registers allocated (high water mark).
    pub fn count(&self) -> i32 {
        self.next_reg - 1
    }
}

impl Default for RegisterAllocator {
    fn default() -> Self {
        Self::new()
    }
}

// ── VDBE Program Builder ────────────────────────────────────────────────────

/// A VDBE bytecode program under construction.
///
/// Provides methods to emit instructions, create/resolve labels for forward
/// jumps, and allocate registers. Once construction is complete, call
/// [`finish`](Self::finish) to validate and extract the final instruction
/// sequence.
#[derive(Debug)]
pub struct ProgramBuilder {
    /// The instruction sequence.
    ops: Vec<VdbeOp>,
    /// Label states (indexed by `Label.0`).
    labels: Vec<LabelState>,
    /// Register allocator.
    regs: RegisterAllocator,
}

impl ProgramBuilder {
    /// Create a new empty program builder.
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            labels: Vec::new(),
            regs: RegisterAllocator::new(),
        }
    }

    // ── Instruction emission ────────────────────────────────────────────

    /// Emit a single instruction and return its address (index in `ops`).
    pub fn emit(&mut self, op: VdbeOp) -> usize {
        let addr = self.ops.len();
        self.ops.push(op);
        addr
    }

    /// Emit a simple instruction from parts.
    pub fn emit_op(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16) -> usize {
        self.emit(VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5,
        })
    }

    /// The current address (index of the next instruction to be emitted).
    pub fn current_addr(&self) -> usize {
        self.ops.len()
    }

    /// Get a reference to the instruction at `addr`.
    pub fn op_at(&self, addr: usize) -> Option<&VdbeOp> {
        self.ops.get(addr)
    }

    /// Get a mutable reference to the instruction at `addr`.
    pub fn op_at_mut(&mut self, addr: usize) -> Option<&mut VdbeOp> {
        self.ops.get_mut(addr)
    }

    // ── Label system ────────────────────────────────────────────────────

    /// Create a new label for forward-reference jumps.
    pub fn emit_label(&mut self) -> Label {
        let id = self.labels.len();
        self.labels.push(LabelState::Unresolved(Vec::new()));
        Label(id)
    }

    /// Emit a jump instruction whose p2 target is a label (forward reference).
    ///
    /// The label's address will be patched into p2 when `resolve_label` is called.
    pub fn emit_jump_to_label(
        &mut self,
        opcode: Opcode,
        p1: i32,
        p3: i32,
        label: Label,
        p4: P4,
        p5: u16,
    ) -> usize {
        let addr = self.emit(VdbeOp {
            opcode,
            p1,
            p2: -1, // placeholder; will be patched
            p3,
            p4,
            p5,
        });

        let idx = label.0;
        match &mut self.labels[idx] {
            LabelState::Unresolved(refs) => refs.push(addr),
            LabelState::Resolved(target) => {
                // Label already resolved; patch immediately.
                self.ops[addr].p2 = *target;
            }
        }

        addr
    }

    /// Resolve a label to the current instruction address.
    ///
    /// All instructions that reference this label have their `p2` patched.
    pub fn resolve_label(&mut self, label: Label) {
        let Ok(target) = i32::try_from(self.ops.len()) else {
            // Keep label unresolved so finish() returns a deterministic internal
            // error instead of panicking on oversized programs.
            return;
        };
        let idx = label.0;

        let refs = match std::mem::replace(&mut self.labels[idx], LabelState::Resolved(target)) {
            LabelState::Unresolved(refs) => refs,
            LabelState::Resolved(_) => {
                // Double resolve is a codegen bug, but we tolerate it
                // if the target is the same.
                return;
            }
        };

        for op_idx in refs {
            self.ops[op_idx].p2 = target;
        }
    }

    /// Resolve a label to a specific address (not necessarily current).
    pub fn resolve_label_to(&mut self, label: Label, address: i32) {
        let idx = label.0;

        let refs = match std::mem::replace(&mut self.labels[idx], LabelState::Resolved(address)) {
            LabelState::Unresolved(refs) => refs,
            LabelState::Resolved(_) => return,
        };

        for op_idx in refs {
            self.ops[op_idx].p2 = address;
        }
    }

    // ── Register allocation (delegates to RegisterAllocator) ────────────

    /// Allocate a single persistent register.
    pub fn alloc_reg(&mut self) -> i32 {
        self.regs.alloc_reg()
    }

    /// Allocate a contiguous block of `n` persistent registers.
    pub fn alloc_regs(&mut self, n: i32) -> i32 {
        self.regs.alloc_regs(n)
    }

    /// Allocate a temporary register (reusable).
    pub fn alloc_temp(&mut self) -> i32 {
        self.regs.alloc_temp()
    }

    /// Return a temporary register to the pool.
    pub fn free_temp(&mut self, reg: i32) {
        self.regs.free_temp(reg);
    }

    /// Total registers allocated (high water mark).
    pub fn register_count(&self) -> i32 {
        self.regs.count()
    }

    // ── Finalization ────────────────────────────────────────────────────

    /// Validate all labels are resolved and return the finished program.
    pub fn finish(self) -> Result<VdbeProgram> {
        // Check for unresolved labels.
        for (i, state) in self.labels.iter().enumerate() {
            if let LabelState::Unresolved(refs) = state {
                if !refs.is_empty() {
                    return Err(FrankenError::Internal(format!(
                        "unresolved label {i} referenced by {} instruction(s)",
                        refs.len()
                    )));
                }
            }
        }

        Ok(VdbeProgram {
            ops: self.ops,
            register_count: self.regs.count(),
        })
    }
}

impl Default for ProgramBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ── VDBE Program ────────────────────────────────────────────────────────────

/// A finalized VDBE bytecode program ready for execution.
#[derive(Debug, Clone, PartialEq)]
pub struct VdbeProgram {
    /// The instruction sequence.
    ops: Vec<VdbeOp>,
    /// Number of registers needed (high water mark from allocation).
    register_count: i32,
}

impl VdbeProgram {
    /// The instruction sequence.
    pub fn ops(&self) -> &[VdbeOp] {
        &self.ops
    }

    /// Number of instructions.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the program is empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Number of registers required.
    pub fn register_count(&self) -> i32 {
        self.register_count
    }

    /// Get the instruction at the given program counter.
    pub fn get(&self, pc: usize) -> Option<&VdbeOp> {
        self.ops.get(pc)
    }

    /// Disassemble the program to a human-readable string.
    ///
    /// Output format matches SQLite's `EXPLAIN` output:
    /// ```text
    /// addr  opcode         p1    p2    p3    p4             p5
    /// ----  ----------     ----  ----  ----  -----          --
    /// 0     Init           0     8     0                    0
    /// ```
    pub fn disassemble(&self) -> String {
        use std::fmt::Write;

        let mut out = std::string::String::with_capacity(self.ops.len() * 60);
        out.push_str("addr  opcode           p1    p2    p3    p4                 p5\n");
        out.push_str("----  ---------------  ----  ----  ----  -----------------  --\n");

        for (addr, op) in self.ops.iter().enumerate() {
            let p4_str = match &op.p4 {
                P4::None => String::new(),
                P4::Int(v) => format!("(int){v}"),
                P4::Int64(v) => format!("(i64){v}"),
                P4::Real(v) => format!("(real){v}"),
                P4::Str(s) => format!("(str){s}"),
                P4::Blob(b) => format!("(blob)[{}B]", b.len()),
                P4::Collation(c) => format!("(coll){c}"),
                P4::FuncName(f) => format!("(func){f}"),
                P4::Table(t) => format!("(tbl){t}"),
                P4::Affinity(a) => format!("(aff){a}"),
            };

            let _ = writeln!(
                &mut out,
                "{addr:<4}  {:<15}  {:<4}  {:<4}  {:<4}  {:<17}  {:<2}",
                op.opcode.name(),
                op.p1,
                op.p2,
                op.p3,
                p4_str,
                op.p5,
            );
        }

        out
    }
}

// ── PRAGMA Handling ──────────────────────────────────────────────────────────

/// Minimal PRAGMA dispatch for early phases.
///
/// The full engine will execute PRAGMA statements through the SQL pipeline,
/// but we keep these handlers in VDBE (the execution boundary) so higher layers
/// can remain declarative.
pub mod pragma {
    use std::path::Path;

    use fsqlite_ast::{Expr, Literal, PragmaStatement, PragmaValue, QualifiedName, UnaryOp};
    use fsqlite_error::{FrankenError, Result};
    use fsqlite_mvcc::TransactionManager;
    use fsqlite_wal::{
        MAX_RAPTORQ_REPAIR_SYMBOLS, persist_wal_fec_raptorq_repair_symbols,
        read_wal_fec_raptorq_repair_symbols,
    };
    use tracing::{debug, error, info, warn};

    /// Result of applying a PRAGMA statement.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PragmaOutput {
        /// PRAGMA not recognized by this handler.
        Unsupported,
        /// PRAGMA yields a boolean value (e.g. query or echo after set).
        Bool(bool),
        /// PRAGMA yields an integer value.
        Int(i64),
        /// PRAGMA yields a text value (e.g. `journal_mode`).
        Text(String),
    }

    /// Connection-level settings controlled by PRAGMA statements.
    ///
    /// These mirror the standard SQLite PRAGMAs that the E2E harness needs to
    /// set consistently across both `sqlite3` and FrankenSQLite runs.  Values
    /// are stored here for future backend wiring (Phase 5+) and are immediately
    /// queryable via `PRAGMA <name>`.
    #[derive(Debug, Clone)]
    pub struct ConnectionPragmaState {
        /// Journal mode (`delete`, `truncate`, `persist`, `memory`, `wal`, `off`).
        pub journal_mode: String,
        /// Synchronous level (`OFF`, `NORMAL`, `FULL`, `EXTRA`).
        pub synchronous: String,
        /// Page cache size (negative = KiB, positive = pages).
        pub cache_size: i64,
        /// Page size in bytes (512..=65536, power of two).
        pub page_size: u32,
        /// Busy timeout in milliseconds for lock contention.
        pub busy_timeout_ms: i64,
    }

    impl Default for ConnectionPragmaState {
        fn default() -> Self {
            Self {
                journal_mode: "wal".to_owned(),
                synchronous: "NORMAL".to_owned(),
                cache_size: -2000,
                page_size: 4096,
                busy_timeout_ms: 5000,
            }
        }
    }

    /// Apply a PRAGMA statement to the provided connection-scoped state.
    ///
    /// Currently supports:
    /// - `PRAGMA fsqlite.serializable`
    /// - `PRAGMA fsqlite.serializable = ON|OFF|TRUE|FALSE|1|0`
    /// - `PRAGMA raptorq_repair_symbols`
    /// - `PRAGMA raptorq_repair_symbols = N` (N in [0, 255])
    ///
    /// Unknown pragmas return [`PragmaOutput::Unsupported`].
    pub fn apply(mgr: &mut TransactionManager, stmt: &PragmaStatement) -> Result<PragmaOutput> {
        apply_with_sidecar(mgr, stmt, None)
    }

    /// Apply a PRAGMA statement with optional `.wal-fec` sidecar persistence.
    pub fn apply_with_sidecar(
        mgr: &mut TransactionManager,
        stmt: &PragmaStatement,
        wal_fec_sidecar_path: Option<&Path>,
    ) -> Result<PragmaOutput> {
        if is_fsqlite_serializable(&stmt.name) {
            return apply_serializable(mgr, stmt);
        }
        if is_raptorq_repair_symbols(&stmt.name) {
            return apply_raptorq_repair_symbols(mgr, stmt, wal_fec_sidecar_path);
        }
        Ok(PragmaOutput::Unsupported)
    }

    /// Apply a PRAGMA to connection-level settings.
    ///
    /// Handles `journal_mode`, `synchronous`, `cache_size`, `page_size`,
    /// `busy_timeout`. Returns `Unsupported` for pragmas not handled at
    /// this layer, allowing the caller to chain with [`apply`].
    pub fn apply_connection_pragma(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        let name = &stmt.name.name;
        if name.eq_ignore_ascii_case("journal_mode") {
            return apply_journal_mode(state, stmt);
        }
        if name.eq_ignore_ascii_case("synchronous") {
            return apply_synchronous(state, stmt);
        }
        if name.eq_ignore_ascii_case("cache_size") {
            return apply_cache_size(state, stmt);
        }
        if name.eq_ignore_ascii_case("page_size") {
            return apply_page_size(state, stmt);
        }
        if name.eq_ignore_ascii_case("busy_timeout") {
            return apply_busy_timeout(state, stmt);
        }
        Ok(PragmaOutput::Unsupported)
    }

    fn apply_journal_mode(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Text(state.journal_mode.clone())),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let mode = parse_text_expr(expr)?;
                let lower = mode.to_ascii_lowercase();
                match lower.as_str() {
                    "delete" | "truncate" | "persist" | "memory" | "wal" | "off" => {
                        state.journal_mode.clone_from(&lower);
                        Ok(PragmaOutput::Text(lower))
                    }
                    _ => Err(FrankenError::TypeMismatch {
                        expected: "delete|truncate|persist|memory|wal|off".to_owned(),
                        actual: mode,
                    }),
                }
            }
        }
    }

    fn apply_synchronous(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Text(state.synchronous.clone())),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_synchronous_value(expr)?;
                state.synchronous.clone_from(&val);
                Ok(PragmaOutput::Text(val))
            }
        }
    }

    fn parse_synchronous_value(expr: &Expr) -> Result<String> {
        // Accept both text names and integer codes (0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA).
        if let Expr::Literal(Literal::Integer(n), _) = expr {
            match n {
                0 => Ok("OFF".to_owned()),
                1 => Ok("NORMAL".to_owned()),
                2 => Ok("FULL".to_owned()),
                3 => Ok("EXTRA".to_owned()),
                _ => Err(FrankenError::OutOfRange {
                    what: "synchronous".to_owned(),
                    value: n.to_string(),
                }),
            }
        } else {
            let text = parse_text_expr(expr)?;
            let upper = text.to_ascii_uppercase();
            match upper.as_str() {
                "OFF" | "NORMAL" | "FULL" | "EXTRA" => Ok(upper),
                _ => Err(FrankenError::TypeMismatch {
                    expected: "OFF|NORMAL|FULL|EXTRA|0|1|2|3".to_owned(),
                    actual: text,
                }),
            }
        }
    }

    fn apply_cache_size(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.cache_size)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                state.cache_size = val;
                Ok(PragmaOutput::Int(val))
            }
        }
    }

    fn apply_page_size(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(i64::from(state.page_size))),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                if !(512..=65536).contains(&val) || !is_power_of_two(val) {
                    return Err(FrankenError::OutOfRange {
                        what: "page_size".to_owned(),
                        value: val.to_string(),
                    });
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                {
                    state.page_size = val as u32;
                }
                Ok(PragmaOutput::Int(val))
            }
        }
    }

    fn apply_busy_timeout(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.busy_timeout_ms)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                state.busy_timeout_ms = val.max(0);
                Ok(PragmaOutput::Int(state.busy_timeout_ms))
            }
        }
    }

    fn is_power_of_two(n: i64) -> bool {
        n > 0 && (n & (n - 1)) == 0
    }

    /// Extract a text value from a PRAGMA assignment expression.
    fn parse_text_expr(expr: &Expr) -> Result<String> {
        match expr {
            Expr::Literal(Literal::String(s), _) => Ok(s.clone()),
            Expr::Column(col, _) => Ok(col.column.clone()),
            Expr::Literal(Literal::Integer(n), _) => Ok(n.to_string()),
            other => Err(FrankenError::TypeMismatch {
                expected: "text or identifier".to_owned(),
                actual: format!("{other:?}"),
            }),
        }
    }

    fn apply_serializable(
        mgr: &mut TransactionManager,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Bool(mgr.ssi_enabled())),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                mgr.set_ssi_enabled(enabled);
                Ok(PragmaOutput::Bool(mgr.ssi_enabled()))
            }
        }
    }

    fn is_fsqlite_serializable(name: &QualifiedName) -> bool {
        name.schema
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("fsqlite"))
            && name.name.eq_ignore_ascii_case("serializable")
    }

    fn is_raptorq_repair_symbols(name: &QualifiedName) -> bool {
        let schema_ok = match name.schema.as_deref() {
            None => true,
            Some(schema) => schema.eq_ignore_ascii_case("fsqlite"),
        };
        schema_ok && name.name.eq_ignore_ascii_case("raptorq_repair_symbols")
    }

    fn apply_raptorq_repair_symbols(
        mgr: &mut TransactionManager,
        stmt: &PragmaStatement,
        wal_fec_sidecar_path: Option<&Path>,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => {
                if let Some(sidecar) = wal_fec_sidecar_path {
                    let persisted = read_wal_fec_raptorq_repair_symbols(sidecar)?;
                    mgr.set_raptorq_repair_symbols(persisted);
                    debug!(
                        sidecar = %sidecar.display(),
                        raptorq_repair_symbols = persisted,
                        "loaded raptorq_repair_symbols from wal-fec sidecar"
                    );
                }
                Ok(PragmaOutput::Int(i64::from(mgr.raptorq_repair_symbols())))
            }
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let requested = parse_raptorq_repair_symbols(expr)?;
                mgr.set_raptorq_repair_symbols(requested);

                if let Some(sidecar) = wal_fec_sidecar_path {
                    persist_wal_fec_raptorq_repair_symbols(sidecar, requested)?;
                    info!(
                        sidecar = %sidecar.display(),
                        raptorq_repair_symbols = requested,
                        "persisted raptorq_repair_symbols to wal-fec sidecar"
                    );
                }

                Ok(PragmaOutput::Int(i64::from(mgr.raptorq_repair_symbols())))
            }
        }
    }

    fn parse_raptorq_repair_symbols(expr: &Expr) -> Result<u8> {
        let raw = parse_integer_expr(expr)?;
        if raw < 0 {
            warn!(
                value = raw,
                "rejecting negative raptorq_repair_symbols value"
            );
            return Err(FrankenError::OutOfRange {
                what: "raptorq_repair_symbols".to_owned(),
                value: raw.to_string(),
            });
        }

        let max = i64::from(MAX_RAPTORQ_REPAIR_SYMBOLS);
        if raw > max {
            warn!(
                value = raw,
                max = MAX_RAPTORQ_REPAIR_SYMBOLS,
                "rejecting out-of-range raptorq_repair_symbols value"
            );
            return Err(FrankenError::OutOfRange {
                what: "raptorq_repair_symbols".to_owned(),
                value: raw.to_string(),
            });
        }

        u8::try_from(raw).map_err(|_| {
            error!(
                value = raw,
                "failed to convert validated raptorq_repair_symbols to u8"
            );
            FrankenError::OutOfRange {
                what: "raptorq_repair_symbols".to_owned(),
                value: raw.to_string(),
            }
        })
    }

    fn parse_integer_expr(expr: &Expr) -> Result<i64> {
        match expr {
            Expr::Literal(Literal::Integer(n), _) => Ok(*n),
            Expr::UnaryOp {
                op: UnaryOp::Negate,
                expr,
                ..
            } => Ok(-parse_integer_expr(expr)?),
            Expr::UnaryOp {
                op: UnaryOp::Plus,
                expr,
                ..
            } => parse_integer_expr(expr),
            Expr::Column(col, _) => {
                col.column
                    .parse::<i64>()
                    .map_err(|_| FrankenError::TypeMismatch {
                        expected: "integer (0..255)".to_owned(),
                        actual: col.column.clone(),
                    })
            }
            other => Err(FrankenError::TypeMismatch {
                expected: "integer (0..255)".to_owned(),
                actual: format!("{other:?}"),
            }),
        }
    }

    fn parse_bool(expr: &Expr) -> Result<bool> {
        let (raw, parsed) = match expr {
            Expr::Literal(Literal::Integer(n), _) => (format!("{n}"), parse_int_bool(*n)),
            Expr::Literal(Literal::String(s), _) => (s.clone(), parse_str_bool(s)),
            Expr::Literal(Literal::True, _) => ("TRUE".to_owned(), Some(true)),
            Expr::Literal(Literal::False, _) => ("FALSE".to_owned(), Some(false)),
            Expr::Column(col, _) => (col.column.clone(), parse_str_bool(&col.column)),
            other => {
                return Err(FrankenError::TypeMismatch {
                    expected: "ON|OFF|TRUE|FALSE|1|0".to_owned(),
                    actual: format!("{other:?}"),
                });
            }
        };

        parsed.ok_or_else(|| FrankenError::TypeMismatch {
            expected: "ON|OFF|TRUE|FALSE|1|0".to_owned(),
            actual: raw,
        })
    }

    fn parse_int_bool(n: i64) -> Option<bool> {
        match n {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    fn parse_str_bool(s: &str) -> Option<bool> {
        if s.eq_ignore_ascii_case("on") || s.eq_ignore_ascii_case("true") {
            Some(true)
        } else if s.eq_ignore_ascii_case("off") || s.eq_ignore_ascii_case("false") {
            Some(false)
        } else if s == "1" {
            Some(true)
        } else if s == "0" {
            Some(false)
        } else {
            None
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── test_vdbe_op_struct_size ─────────────────────────────────────────
    #[test]
    fn test_vdbe_op_struct_size() {
        // Verify VdbeOp fields are accessible and correctly typed.
        let op = VdbeOp {
            opcode: Opcode::Integer,
            p1: 42,
            p2: 1,
            p3: 0,
            p4: P4::None,
            p5: 0,
        };
        assert_eq!(op.opcode, Opcode::Integer);
        assert_eq!(op.p1, 42_i32);
        assert_eq!(op.p2, 1_i32);
        assert_eq!(op.p3, 0_i32);
        assert_eq!(op.p4, P4::None);
        assert_eq!(op.p5, 0_u16);
    }

    // ── test_p4_variant_all_types ───────────────────────────────────────
    #[test]
    fn test_p4_variant_all_types() {
        // Each P4 variant can be constructed and pattern-matched.
        let variants: Vec<P4> = vec![
            P4::None,
            P4::Int(42),
            P4::Int64(i64::MAX),
            P4::Real(1.234_567_89),
            P4::Str("hello".to_owned()),
            P4::Blob(vec![0xDE, 0xAD]),
            P4::Collation("BINARY".to_owned()),
            P4::FuncName("count".to_owned()),
            P4::Table("users".to_owned()),
            P4::Affinity("ddd".to_owned()),
        ];
        assert_eq!(variants.len(), 10);

        // Verify each variant matches itself.
        assert!(matches!(variants[0], P4::None));
        assert!(matches!(variants[1], P4::Int(42)));
        assert!(matches!(variants[2], P4::Int64(i64::MAX)));
        assert!(matches!(variants[3], P4::Real(_)));
        assert!(matches!(variants[4], P4::Str(_)));
        assert!(matches!(variants[5], P4::Blob(_)));
        assert!(matches!(variants[6], P4::Collation(_)));
        assert!(matches!(variants[7], P4::FuncName(_)));
        assert!(matches!(variants[8], P4::Table(_)));
        assert!(matches!(variants[9], P4::Affinity(_)));
    }

    // ── test_label_emit_and_resolve ─────────────────────────────────────
    #[test]
    fn test_label_emit_and_resolve() {
        let mut b = ProgramBuilder::new();

        // Emit two distinct labels.
        let label_a = b.emit_label();
        let label_b = b.emit_label();
        assert_ne!(label_a, label_b);

        // Emit a jump to label_a (forward reference).
        let jump_addr = b.emit_jump_to_label(Opcode::Goto, 0, 0, label_a, P4::None, 0);
        assert_eq!(b.op_at(jump_addr).unwrap().p2, -1); // unresolved placeholder

        // Emit some instructions.
        b.emit_op(Opcode::Integer, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 2, 2, 0, P4::None, 0);

        // Resolve label_a to the current address (2 instructions after the jump).
        b.resolve_label(label_a);

        // The jump's p2 should now be patched to address 3.
        assert_eq!(b.op_at(jump_addr).unwrap().p2, 3);

        // Emit another jump to label_b.
        let jump2 = b.emit_jump_to_label(Opcode::If, 1, 0, label_b, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(label_b);
        assert_eq!(b.op_at(jump2).unwrap().p2, 5);

        // Finish should succeed (all labels resolved).
        let prog = b.finish().unwrap();
        assert_eq!(prog.len(), 5);
    }

    // ── test_unresolved_label_error ─────────────────────────────────────
    #[test]
    fn test_unresolved_label_error() {
        let mut b = ProgramBuilder::new();
        let label = b.emit_label();
        b.emit_jump_to_label(Opcode::Goto, 0, 0, label, P4::None, 0);

        // Don't resolve the label — finish should fail.
        let result = b.finish();
        assert!(result.is_err());
    }

    // ── test_register_alloc_sequential ──────────────────────────────────
    #[test]
    fn test_register_alloc_sequential() {
        let mut alloc = RegisterAllocator::new();

        // Sequential single allocations start at 1.
        assert_eq!(alloc.alloc_reg(), 1);
        assert_eq!(alloc.alloc_reg(), 2);
        assert_eq!(alloc.alloc_reg(), 3);

        // Block allocation returns first register of contiguous block.
        let block_start = alloc.alloc_regs(3);
        assert_eq!(block_start, 4);
        // Next single alloc continues after the block.
        assert_eq!(alloc.alloc_reg(), 7);

        assert_eq!(alloc.count(), 7);
    }

    // ── test_register_temp_pool_reuse ───────────────────────────────────
    #[test]
    fn test_register_temp_pool_reuse() {
        let mut alloc = RegisterAllocator::new();

        let r1 = alloc.alloc_reg(); // 1
        let t1 = alloc.alloc_temp(); // 2 (new allocation)
        let t2 = alloc.alloc_temp(); // 3 (new allocation)
        assert_eq!(r1, 1);
        assert_eq!(t1, 2);
        assert_eq!(t2, 3);

        // Return temps to pool.
        alloc.free_temp(t1);
        alloc.free_temp(t2);

        // Next temp allocations reuse from pool (LIFO order).
        let t3 = alloc.alloc_temp();
        let t4 = alloc.alloc_temp();
        assert_eq!(t3, t2); // 3 (last freed)
        assert_eq!(t4, t1); // 2

        // High water mark unchanged (no new registers needed).
        assert_eq!(alloc.count(), 3);
    }

    // ── test_coroutine_init_yield_end ───────────────────────────────────
    #[test]
    fn test_coroutine_init_yield_end() {
        // InitCoroutine: set yield register to body PC.
        let yield_reg = 1;
        let body_pc = 10;
        let mut co = CoroutineState::new(yield_reg, body_pc);
        assert_eq!(co.yield_reg, yield_reg);
        assert_eq!(co.saved_pc, body_pc);
        assert!(!co.exhausted);

        // Yield: bidirectional PC swap.
        // Caller is at PC=5, coroutine body is at PC=10.
        let resume = co.yield_swap(5);
        assert_eq!(resume, 10); // jump to body
        assert_eq!(co.saved_pc, 5); // caller's PC saved

        // Body yields back: caller at 5, body at 15.
        let resume2 = co.yield_swap(15);
        assert_eq!(resume2, 5); // back to caller
        assert_eq!(co.saved_pc, 15);

        // EndCoroutine: marks exhaustion, returns to caller.
        let final_pc = co.end();
        assert_eq!(final_pc, 15); // returns saved_pc
        assert!(co.exhausted);
    }

    // ── test_coroutine_multi_row_production ─────────────────────────────
    #[test]
    fn test_coroutine_multi_row_production() {
        // Simulate a CTE body producing 5 rows via Yield loop.
        let mut co = CoroutineState::new(1, 10); // body starts at PC=10
        let mut rows_consumed = 0;
        let caller_start_pc = 5;

        // Caller yields to body.
        let mut next_pc = co.yield_swap(caller_start_pc);
        assert_eq!(next_pc, 10); // first entry into body

        // Body produces rows.
        for row in 1..=5 {
            // Body "produces" a row, then yields back to caller.
            let body_pc = 10 + row; // body advances its PC
            next_pc = co.yield_swap(body_pc);
            // Caller resumes at its saved PC.
            assert_eq!(next_pc, caller_start_pc);
            rows_consumed += 1;

            if row < 5 {
                // Caller yields back to body to get next row.
                next_pc = co.yield_swap(caller_start_pc);
                assert_eq!(next_pc, body_pc); // resume body
            }
        }

        assert_eq!(rows_consumed, 5);

        // Body signals exhaustion.
        let final_pc = co.end();
        assert!(co.exhausted);
        assert!(final_pc > 0); // valid return PC
    }

    // ── test_all_opcode_dispatch_coverage ────────────────────────────────
    #[test]
    fn test_all_opcode_dispatch_coverage() {
        // Every Opcode enum variant (1..=190) has a valid name and can be
        // constructed from its byte value. This ensures no gaps in the enum.
        for byte in 1..=190u8 {
            let opcode = Opcode::from_byte(byte);
            assert!(
                opcode.is_some(),
                "Opcode::from_byte({byte}) returned None — gap in opcode enum"
            );
            let opcode = opcode.unwrap();
            let name = opcode.name();
            assert!(!name.is_empty(), "opcode {byte} has empty name");
        }
        assert_eq!(Opcode::COUNT, 191);
    }

    // ── test_p5_flags_u16_range ─────────────────────────────────────────
    #[test]
    fn test_p5_flags_u16_range() {
        // Confirm p5 is u16 and accepts values above 0xFF.
        let op = VdbeOp {
            opcode: Opcode::Eq,
            p1: 1,
            p2: 5,
            p3: 2,
            p4: P4::None,
            p5: 0x1FF, // 511, exceeds u8 range
        };
        assert_eq!(op.p5, 0x1FF);
        assert!(op.p5 > 255);

        let op2 = VdbeOp {
            opcode: Opcode::Noop,
            p1: 0,
            p2: 0,
            p3: 0,
            p4: P4::None,
            p5: u16::MAX,
        };
        assert_eq!(op2.p5, 65535);
    }

    // ── test_program_builder_basic ──────────────────────────────────────
    #[test]
    fn test_program_builder_basic() {
        let mut b = ProgramBuilder::new();

        // Build: Init -> Integer 42 into r1 -> ResultRow r1,1 -> Halt
        let end_label = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        let r1 = b.alloc_reg();
        assert_eq!(r1, 1);
        b.emit_op(Opcode::Integer, 42, r1, 0, P4::None, 0);
        b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        let prog = b.finish().unwrap();
        assert_eq!(prog.len(), 4);
        assert_eq!(prog.register_count(), 1);

        // The Init instruction's p2 should point to address 4 (after Halt).
        assert_eq!(prog.get(0).unwrap().opcode, Opcode::Init);
        assert_eq!(prog.get(0).unwrap().p2, 4);
    }

    // ── test_disassemble ────────────────────────────────────────────────
    #[test]
    fn test_disassemble() {
        let mut b = ProgramBuilder::new();
        b.emit_op(Opcode::Init, 0, 2, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 42, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();

        let asm = prog.disassemble();
        assert!(asm.contains("Init"));
        assert!(asm.contains("Integer"));
        assert!(asm.contains("Halt"));
        assert!(asm.contains("42")); // p1 of Integer
    }

    // ── test_key_info ───────────────────────────────────────────────────
    #[test]
    fn test_key_info() {
        let ki = KeyInfo {
            num_fields: 3,
            collations: vec![
                "BINARY".to_owned(),
                "NOCASE".to_owned(),
                "BINARY".to_owned(),
            ],
            sort_orders: vec![SortOrder::Asc, SortOrder::Desc, SortOrder::Asc],
        };
        assert_eq!(ki.num_fields, 3);
        assert_eq!(ki.collations.len(), 3);
        assert_eq!(ki.sort_orders[1], SortOrder::Desc);
    }

    // ── test_label_already_resolved ─────────────────────────────────────
    #[test]
    fn test_label_already_resolved() {
        // If a label is resolved before a jump references it, the jump
        // should be patched immediately.
        let mut b = ProgramBuilder::new();
        let label = b.emit_label();
        b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
        b.resolve_label(label); // resolved to address 1

        // Now emit a jump referencing the already-resolved label.
        let jump_addr = b.emit_jump_to_label(Opcode::Goto, 0, 0, label, P4::None, 0);
        // p2 should already be patched to 1.
        assert_eq!(b.op_at(jump_addr).unwrap().p2, 1);

        let prog = b.finish().unwrap();
        assert_eq!(prog.len(), 2);
    }

    // ── test_builder_register_via_builder ────────────────────────────────
    #[test]
    fn test_builder_register_via_builder() {
        let mut b = ProgramBuilder::new();
        let r1 = b.alloc_reg();
        let r2 = b.alloc_reg();
        let block = b.alloc_regs(4);
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
        assert_eq!(block, 3);
        assert_eq!(b.register_count(), 6);

        // Temp allocation.
        let t1 = b.alloc_temp();
        assert_eq!(t1, 7);
        b.free_temp(t1);
        let t2 = b.alloc_temp();
        assert_eq!(t2, t1); // reused
    }

    // ── test_resolve_label_to_specific_address ──────────────────────────
    #[test]
    fn test_resolve_label_to_specific_address() {
        let mut b = ProgramBuilder::new();
        let label = b.emit_label();
        let jump_addr = b.emit_jump_to_label(Opcode::Goto, 0, 0, label, P4::None, 0);
        b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);

        // Resolve to a specific address (not current).
        b.resolve_label_to(label, 42);
        assert_eq!(b.op_at(jump_addr).unwrap().p2, 42);
    }

    // ── test_empty_program_finishes ─────────────────────────────────────
    #[test]
    fn test_empty_program_finishes() {
        let b = ProgramBuilder::new();
        let prog = b.finish().unwrap();
        assert!(prog.is_empty());
        assert_eq!(prog.register_count(), 0);
    }

    // ── test_unreferenced_unresolved_label_ok ───────────────────────────
    #[test]
    fn test_unreferenced_unresolved_label_ok() {
        // A label that was created but never referenced or resolved should
        // not cause an error (it's unused, not a dangling reference).
        let mut b = ProgramBuilder::new();
        let _label = b.emit_label();
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();
        assert_eq!(prog.len(), 1);
    }

    // ── PRAGMA handling (bd-iwu.5) ───────────────────────────────────────

    use std::fs;

    use fsqlite_ast::Statement;
    use fsqlite_error::FrankenError;
    use fsqlite_mvcc::{BeginKind, MvccError, TransactionManager};
    use fsqlite_parser::Parser;
    use fsqlite_types::{CommitSeq, ObjectId, Oti, PageData, PageNumber, PageSize};
    use fsqlite_wal::{
        DEFAULT_RAPTORQ_REPAIR_SYMBOLS, WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord,
        WalFecRecoveryOutcome, WalFrameCandidate, WalSalts, append_wal_fec_group,
        build_source_page_hashes, generate_wal_fec_repair_symbols,
        recover_wal_fec_group_with_decoder, scan_wal_fec,
    };
    use tempfile::tempdir;

    fn parse_pragma(sql: &str) -> std::result::Result<fsqlite_ast::PragmaStatement, String> {
        let mut p = Parser::from_sql(sql);
        let stmt = p.parse_statement().expect("parse statement");
        match stmt {
            Statement::Pragma(p) => Ok(p),
            other => Err(format!("expected PRAGMA, got: {other:?}")),
        }
    }

    fn test_page(first_byte: u8) -> PageData {
        let mut page = PageData::zeroed(PageSize::DEFAULT);
        page.as_bytes_mut()[0] = first_byte;
        page
    }

    fn make_source_pages(seed: u8, k_source: u32) -> Vec<Vec<u8>> {
        let page_len = usize::try_from(PageSize::DEFAULT.get()).expect("page size fits usize");
        (0..k_source)
            .map(|idx| {
                let idx_u8 = u8::try_from(idx).expect("test k_source fits u8");
                let mut page = vec![seed.wrapping_add(idx_u8); page_len];
                page[0] = idx_u8;
                page
            })
            .collect()
    }

    fn make_wal_fec_group(
        start_frame_no: u32,
        r_repair: u8,
        seed: u8,
    ) -> (WalFecGroupRecord, Vec<Vec<u8>>) {
        let k_source = 5_u32;
        let source_pages = make_source_pages(seed, k_source);
        let page_size = PageSize::DEFAULT.get();
        let source_hashes = build_source_page_hashes(&source_pages);
        let page_numbers = (0..k_source).map(|i| 10 + i).collect::<Vec<_>>();
        let oti = Oti {
            f: u64::from(k_source) * u64::from(page_size),
            al: 1,
            t: page_size,
            z: 1,
            n: 1,
        };
        let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
            wal_salt1: 0xA11C_E001,
            wal_salt2: 0xA11C_E002,
            start_frame_no,
            end_frame_no: start_frame_no + (k_source - 1),
            db_size_pages: 256,
            page_size,
            k_source,
            r_repair: u32::from(r_repair),
            oti,
            object_id: ObjectId::from_bytes([seed; 16]),
            page_numbers,
            source_page_xxh3_128: source_hashes,
        })
        .expect("meta");
        let repair_symbols =
            generate_wal_fec_repair_symbols(&meta, &source_pages).expect("symbols");
        (
            WalFecGroupRecord::new(meta, repair_symbols).expect("group"),
            source_pages,
        )
    }

    #[test]
    fn test_pragma_serializable_query_returns_current_setting() {
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);

        let stmt = parse_pragma("PRAGMA fsqlite.serializable").expect("parse pragma");
        let out = pragma::apply(&mut mgr, &stmt).unwrap();
        assert_eq!(out, pragma::PragmaOutput::Bool(true));
    }

    #[test]
    fn test_pragma_serializable_set_and_query() {
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);

        let set_off = parse_pragma("PRAGMA fsqlite.serializable = OFF").expect("parse pragma");
        assert_eq!(
            pragma::apply(&mut mgr, &set_off).unwrap(),
            pragma::PragmaOutput::Bool(false)
        );

        let query = parse_pragma("PRAGMA fsqlite.serializable").expect("parse pragma");
        assert_eq!(
            pragma::apply(&mut mgr, &query).unwrap(),
            pragma::PragmaOutput::Bool(false)
        );
    }

    #[test]
    fn test_pragma_scope_per_connection_via_handler() {
        let mut conn_a = TransactionManager::new(PageSize::DEFAULT);
        let mut conn_b = TransactionManager::new(PageSize::DEFAULT);

        let set_off = parse_pragma("PRAGMA fsqlite.serializable = OFF").expect("parse pragma");
        let _ = pragma::apply(&mut conn_a, &set_off).unwrap();

        let query = parse_pragma("PRAGMA fsqlite.serializable").expect("parse pragma");
        assert_eq!(
            pragma::apply(&mut conn_a, &query).unwrap(),
            pragma::PragmaOutput::Bool(false)
        );
        assert_eq!(
            pragma::apply(&mut conn_b, &query).unwrap(),
            pragma::PragmaOutput::Bool(true)
        );
    }

    #[test]
    fn test_pragma_not_retroactive_to_active_txn_via_handler() {
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);

        let mut txn = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut txn, PageNumber::new(1).unwrap(), test_page(0x01))
            .unwrap();
        txn.has_in_rw = true;
        txn.has_out_rw = true;
        assert!(txn.has_dangerous_structure());

        // Flip OFF mid-txn; this must not affect the already-begun transaction.
        let set_off = parse_pragma("PRAGMA fsqlite.serializable = OFF").expect("parse pragma");
        let _ = pragma::apply(&mut mgr, &set_off).unwrap();

        assert_eq!(
            mgr.commit(&mut txn).unwrap_err(),
            MvccError::BusySnapshot,
            "PRAGMA change must not be retroactive to an active txn"
        );
    }

    #[test]
    fn test_e2e_serializable_pragma_switch_changes_behavior() {
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);

        // Run workload with serializable=ON: must abort on dangerous structure.
        let set_on = parse_pragma("PRAGMA fsqlite.serializable = ON").expect("parse pragma");
        let _ = pragma::apply(&mut mgr, &set_on).unwrap();

        let mut txn_on = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut txn_on, PageNumber::new(1).unwrap(), test_page(0x10))
            .unwrap();
        txn_on.has_in_rw = true;
        txn_on.has_out_rw = true;
        assert_eq!(
            mgr.commit(&mut txn_on).unwrap_err(),
            MvccError::BusySnapshot,
            "serializable=ON must enforce SSI (abort)"
        );

        // Run the same workload with serializable=OFF: must commit (plain SI).
        let set_off = parse_pragma("PRAGMA fsqlite.serializable = OFF").expect("parse pragma");
        let _ = pragma::apply(&mut mgr, &set_off).unwrap();

        let mut txn_off = mgr.begin(BeginKind::Concurrent).unwrap();
        mgr.write_page(&mut txn_off, PageNumber::new(2).unwrap(), test_page(0x20))
            .unwrap();
        txn_off.has_in_rw = true;
        txn_off.has_out_rw = true;

        let seq = mgr.commit(&mut txn_off).unwrap();
        assert!(
            seq > CommitSeq::ZERO,
            "serializable=OFF must allow write skew"
        );
    }

    #[test]
    fn test_pragma_raptorq_repair_symbols_default_query() {
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);
        let query = parse_pragma("PRAGMA raptorq_repair_symbols").expect("parse pragma");
        assert_eq!(
            pragma::apply(&mut mgr, &query).expect("query pragma"),
            pragma::PragmaOutput::Int(i64::from(DEFAULT_RAPTORQ_REPAIR_SYMBOLS))
        );
    }

    #[test]
    fn test_bd_1hi_12_unit_compliance_gate() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("unit.wal-fec");
        let db_path = dir.path().join("unit.db");
        fs::write(&db_path, vec![0_u8; 100]).expect("seed db header");

        let mut conn_a = TransactionManager::new(PageSize::DEFAULT);
        let mut conn_b = TransactionManager::new(PageSize::DEFAULT);

        let query = parse_pragma("PRAGMA raptorq_repair_symbols").expect("parse query");
        assert_eq!(
            pragma::apply_with_sidecar(&mut conn_a, &query, Some(&sidecar)).expect("query default"),
            pragma::PragmaOutput::Int(i64::from(DEFAULT_RAPTORQ_REPAIR_SYMBOLS))
        );

        let set_max = parse_pragma("PRAGMA raptorq_repair_symbols = 255").expect("parse set max");
        assert_eq!(
            pragma::apply_with_sidecar(&mut conn_a, &set_max, Some(&sidecar)).expect("set max"),
            pragma::PragmaOutput::Int(255)
        );

        let set_too_high =
            parse_pragma("PRAGMA raptorq_repair_symbols = 256").expect("parse set too high");
        assert!(matches!(
            pragma::apply_with_sidecar(&mut conn_a, &set_too_high, Some(&sidecar)),
            Err(FrankenError::OutOfRange { .. })
        ));

        let set_negative =
            parse_pragma("PRAGMA raptorq_repair_symbols = -1").expect("parse set negative");
        assert!(matches!(
            pragma::apply_with_sidecar(&mut conn_a, &set_negative, Some(&sidecar)),
            Err(FrankenError::OutOfRange { .. })
        ));

        let set_non_integer =
            parse_pragma("PRAGMA raptorq_repair_symbols = ON").expect("parse set non-integer");
        assert!(matches!(
            pragma::apply_with_sidecar(&mut conn_a, &set_non_integer, Some(&sidecar)),
            Err(FrankenError::TypeMismatch { .. })
        ));

        let query_new_conn = parse_pragma("PRAGMA raptorq_repair_symbols").expect("parse query");
        assert_eq!(
            pragma::apply_with_sidecar(&mut conn_b, &query_new_conn, Some(&sidecar))
                .expect("query persisted value"),
            pragma::PragmaOutput::Int(255)
        );

        let set_shared = parse_pragma("PRAGMA raptorq_repair_symbols = 7").expect("parse shared");
        let _ = pragma::apply_with_sidecar(&mut conn_a, &set_shared, Some(&sidecar))
            .expect("persist shared setting");
        assert_eq!(
            pragma::apply_with_sidecar(&mut conn_b, &query_new_conn, Some(&sidecar))
                .expect("cross-connection visibility"),
            pragma::PragmaOutput::Int(7)
        );

        let db_bytes = fs::read(&db_path).expect("read db header");
        assert!(
            db_bytes[72..92].iter().all(|&byte| byte == 0),
            "sqlite header reserved bytes must remain untouched"
        );
    }

    #[test]
    fn prop_bd_1hi_12_structure_compliance() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("property.wal-fec");
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);
        let query = parse_pragma("PRAGMA raptorq_repair_symbols").expect("parse query");

        for value in 0_u16..=255_u16 {
            let sql = format!("PRAGMA raptorq_repair_symbols = {value}");
            let set_stmt = parse_pragma(&sql).expect("parse set statement");
            assert_eq!(
                pragma::apply_with_sidecar(&mut mgr, &set_stmt, Some(&sidecar)).expect("set value"),
                pragma::PragmaOutput::Int(i64::from(value))
            );
            assert_eq!(
                pragma::apply_with_sidecar(&mut mgr, &query, Some(&sidecar)).expect("query value"),
                pragma::PragmaOutput::Int(i64::from(value))
            );
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_bd_1hi_12_compliance() {
        let dir = tempdir().expect("tempdir");
        let sidecar = dir.path().join("e2e.wal-fec");
        let mut mgr = TransactionManager::new(PageSize::DEFAULT);

        let set_zero = parse_pragma("PRAGMA raptorq_repair_symbols = 0").expect("parse set 0");
        let _ = pragma::apply_with_sidecar(&mut mgr, &set_zero, Some(&sidecar)).expect("set 0");
        if mgr.raptorq_repair_symbols() > 0 {
            let (group, _) = make_wal_fec_group(1, mgr.raptorq_repair_symbols(), 0x10);
            append_wal_fec_group(&sidecar, &group).expect("append group");
        }
        let after_zero = scan_wal_fec(&sidecar).expect("scan after zero");
        assert!(
            after_zero.groups.is_empty(),
            "N=0 must produce no .wal-fec groups for new commits"
        );

        let set_one = parse_pragma("PRAGMA raptorq_repair_symbols = 1").expect("parse set 1");
        let _ = pragma::apply_with_sidecar(&mut mgr, &set_one, Some(&sidecar)).expect("set 1");
        let (group_r1, _) = make_wal_fec_group(1, mgr.raptorq_repair_symbols(), 0x11);
        append_wal_fec_group(&sidecar, &group_r1).expect("append r=1 group");

        let set_two = parse_pragma("PRAGMA raptorq_repair_symbols = 2").expect("parse set 2");
        let _ = pragma::apply_with_sidecar(&mut mgr, &set_two, Some(&sidecar)).expect("set 2");
        let (group_r2, _) = make_wal_fec_group(6, mgr.raptorq_repair_symbols(), 0x22);
        append_wal_fec_group(&sidecar, &group_r2).expect("append r=2 group");

        let set_four = parse_pragma("PRAGMA raptorq_repair_symbols = 4").expect("parse set 4");
        let _ = pragma::apply_with_sidecar(&mut mgr, &set_four, Some(&sidecar)).expect("set 4");
        let (group_r4, source_pages_r4) =
            make_wal_fec_group(11, mgr.raptorq_repair_symbols(), 0x33);
        append_wal_fec_group(&sidecar, &group_r4).expect("append r=4 group");

        let scan = scan_wal_fec(&sidecar).expect("scan sidecar");
        assert_eq!(scan.groups.len(), 3);
        assert_eq!(scan.groups[0].repair_symbols.len(), 1);
        assert_eq!(scan.groups[1].repair_symbols.len(), 2);
        assert_eq!(scan.groups[2].repair_symbols.len(), 4);
        assert_eq!(scan.groups[1].meta.r_repair, 2);
        assert_eq!(scan.groups[2].meta.r_repair, 4);

        let group_id = group_r4.meta.group_id();
        let wal_salts = WalSalts {
            salt1: group_r4.meta.wal_salt1,
            salt2: group_r4.meta.wal_salt2,
        };
        let k_source = usize::try_from(group_r4.meta.k_source).expect("k fits usize");

        let mut corrupt_three_frames = Vec::new();
        for (idx, page) in source_pages_r4.iter().enumerate() {
            let mut payload = page.clone();
            if idx < 3 {
                payload[0] ^= 0xFF;
            }
            corrupt_three_frames.push(WalFrameCandidate {
                frame_no: group_r4.meta.start_frame_no + u32::try_from(idx).expect("idx fits u32"),
                page_data: payload,
            });
        }
        let expected_pages = source_pages_r4.clone();
        let recovered = recover_wal_fec_group_with_decoder(
            &sidecar,
            group_id,
            wal_salts,
            group_r4.meta.start_frame_no,
            &corrupt_three_frames,
            move |meta: &WalFecGroupMeta, symbols| {
                if symbols.len() < usize::try_from(meta.k_source).expect("k fits usize") {
                    return Err(FrankenError::WalCorrupt {
                        detail: "insufficient symbols".to_owned(),
                    });
                }
                Ok(expected_pages.clone())
            },
        )
        .expect("recover with <=R corruption");
        assert!(
            matches!(recovered, WalFecRecoveryOutcome::Recovered(_)),
            "expected recovered outcome"
        );
        let WalFecRecoveryOutcome::Recovered(group) = recovered else {
            unreachable!("asserted recovered outcome above");
        };
        assert_eq!(group.recovered_pages.len(), k_source);

        let mut corrupt_five_frames = Vec::new();
        for (idx, page) in source_pages_r4.iter().enumerate() {
            let mut payload = page.clone();
            payload[0] ^= 0x55;
            corrupt_five_frames.push(WalFrameCandidate {
                frame_no: group_r4.meta.start_frame_no + u32::try_from(idx).expect("idx fits u32"),
                page_data: payload,
            });
        }
        let truncated = recover_wal_fec_group_with_decoder(
            &sidecar,
            group_id,
            wal_salts,
            group_r4.meta.start_frame_no,
            &corrupt_five_frames,
            |_meta: &WalFecGroupMeta, _symbols| {
                Err(FrankenError::WalCorrupt {
                    detail: "decoder should not be able to recover".to_owned(),
                })
            },
        )
        .expect("recover with >R corruption");
        assert!(matches!(
            truncated,
            WalFecRecoveryOutcome::TruncateBeforeGroup { .. }
        ));
    }
}
