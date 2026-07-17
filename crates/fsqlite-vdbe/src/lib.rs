// bd-gird: §10.7-10.8 VDBE Instruction Format + Coroutines
//
// This crate provides the VDBE (Virtual Database Engine) program builder,
// label resolution, register allocation, coroutine mechanism, and disassembly.
// The foundational types (Opcode, VdbeOp, P4) live in fsqlite-types.

use hashbrown::{HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::opcode::{Opcode, P4, VdbeOp};
use std::sync::Arc;
use std::time::Instant;

pub mod codegen;
pub mod dataflow;
pub mod engine;
pub mod frame;
pub mod jit;
#[cfg(test)]
mod make_record_simd;
#[cfg(test)]
mod repro_delete_skip;
pub mod vectorized;
pub mod vectorized_agg;
#[cfg(not(target_arch = "wasm32"))]
pub mod vectorized_dispatch;
pub mod vectorized_hash_join;
pub mod vectorized_join;
pub mod vectorized_ops;
pub mod vectorized_scan;
pub mod vectorized_sort;

#[cfg(test)]
mod vectorized_prop_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VdbePipelineStage {
    Decode,
    Execute,
    Commit,
}

impl VdbePipelineStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Execute => "execute",
            Self::Commit => "commit",
        }
    }
}

#[must_use]
pub(crate) struct VdbeProfileMarker {
    stage: VdbePipelineStage,
    started: Option<Instant>,
}

impl Drop for VdbeProfileMarker {
    fn drop(&mut self) {
        let Some(started) = self.started.take() else {
            return;
        };
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        tracing::trace!(
            target: "fsqlite_vdbe::profile",
            stage = self.stage.as_str(),
            event = "end",
            elapsed_ns,
            "vdbe pipeline stage"
        );
    }
}

#[inline(never)]
fn enter_vdbe_profile_stage(stage: VdbePipelineStage) -> VdbeProfileMarker {
    if tracing::enabled!(target: "fsqlite_vdbe::profile", tracing::Level::TRACE) {
        tracing::trace!(
            target: "fsqlite_vdbe::profile",
            stage = stage.as_str(),
            event = "begin",
            "vdbe pipeline stage"
        );
        VdbeProfileMarker {
            stage,
            started: Some(Instant::now()),
        }
    } else {
        VdbeProfileMarker {
            stage,
            started: None,
        }
    }
}

pub(crate) fn enter_vdbe_decode_profile_stage() -> VdbeProfileMarker {
    enter_vdbe_profile_stage(VdbePipelineStage::Decode)
}

pub(crate) fn enter_vdbe_execute_profile_stage() -> VdbeProfileMarker {
    enter_vdbe_profile_stage(VdbePipelineStage::Execute)
}

pub(crate) fn enter_vdbe_commit_profile_stage() -> VdbeProfileMarker {
    enter_vdbe_profile_stage(VdbePipelineStage::Commit)
}

pub fn profile_vdbe_decode_stage<R>(f: impl FnOnce() -> R) -> R {
    let _profile_stage = enter_vdbe_decode_profile_stage();
    f()
}

pub fn profile_vdbe_execute_stage<R>(f: impl FnOnce() -> R) -> R {
    let _profile_stage = enter_vdbe_execute_profile_stage();
    f()
}

pub fn profile_vdbe_commit_stage<R>(f: impl FnOnce() -> R) -> R {
    let _profile_stage = enter_vdbe_commit_profile_stage();
    f()
}

/// Register spans touched by an opcode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpcodeRegisterSpans {
    pub(crate) read_start: i32,
    pub(crate) read_len: i32,
    pub(crate) write_start: i32,
    pub(crate) write_len: i32,
}

impl OpcodeRegisterSpans {
    pub(crate) const NONE: Self = Self {
        read_start: -1,
        read_len: 0,
        write_start: -1,
        write_len: 0,
    };

    pub(crate) fn max_touched_register(self) -> i32 {
        let read_end = if self.read_start > 0 {
            self.read_start + self.read_len - 1
        } else {
            0
        };
        let write_end = if self.write_start > 0 {
            self.write_start + self.write_len - 1
        } else {
            0
        };
        read_end.max(write_end)
    }
}

fn register_range(start: i32, len: i32) -> (i32, i32) {
    if start <= 0 {
        (-1, 0)
    } else {
        (start, len.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JumpTargetBounds {
    Instruction,
    InitEntry,
}

fn verify_jump_target_operand(
    pc: usize,
    opcode: Opcode,
    operand_name: &'static str,
    target: i32,
    op_count: usize,
    bounds: JumpTargetBounds,
) -> Result<()> {
    let Ok(target_usize) = usize::try_from(target) else {
        return Err(FrankenError::Internal(format!(
            "bytecode verification failed at pc {pc}: {} {operand_name} target {target} is negative",
            opcode.name()
        )));
    };

    let in_bounds = match bounds {
        JumpTargetBounds::Instruction => target_usize < op_count,
        JumpTargetBounds::InitEntry => target_usize <= op_count,
    };
    if in_bounds {
        return Ok(());
    }

    let allowed_range = match bounds {
        JumpTargetBounds::Instruction => format!("0..{op_count}"),
        JumpTargetBounds::InitEntry => format!("0..={op_count}"),
    };
    Err(FrankenError::Internal(format!(
        "bytecode verification failed at pc {pc}: {} {operand_name} target {target} is outside {allowed_range}",
        opcode.name()
    )))
}

pub(crate) fn opcode_register_spans(op: &VdbeOp) -> OpcodeRegisterSpans {
    let (read_start, read_len, write_start, write_len) = match op.opcode {
        Opcode::Integer
        | Opcode::Int64
        | Opcode::Real
        | Opcode::String
        | Opcode::String8
        | Opcode::Blob
        | Opcode::Variable => {
            let (write_start, write_len) = register_range(op.p2, 1);
            (-1, 0, write_start, write_len)
        }
        Opcode::Null => {
            let write_count = if op.p3 > 0 { op.p3 - op.p2 + 1 } else { 1 };
            let (write_start, write_len) = register_range(op.p2, write_count);
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
            let (start, len) = register_range(op.p1, 1);
            (start, len, start, len)
        }
        Opcode::Move => {
            let (read_start, read_len) = register_range(op.p1, op.p3);
            let (write_start, write_len) = register_range(op.p2, op.p3);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::Copy => {
            let copy_len = op.p3.saturating_add(1);
            let (read_start, read_len) = register_range(op.p1, copy_len);
            let (write_start, write_len) = register_range(op.p2, copy_len);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::SCopy | Opcode::IntCopy | Opcode::BitNot | Opcode::Not => {
            let (read_start, read_len) = register_range(op.p1, 1);
            let (write_start, write_len) = register_range(op.p2, 1);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::ResultRow => {
            let (read_start, read_len) = register_range(op.p1, op.p2);
            (read_start, read_len, -1, 0)
        }
        // IMPL-13: Fused Integer+ResultRow. P2 is the (write+drain) register.
        Opcode::FusedLiteralResultRow => {
            let (start, len) = register_range(op.p2, 1);
            // The opcode writes the literal into `p2`, then drains it. From a
            // liveness standpoint it both reads and writes that single slot.
            (start, len, start, len)
        }
        Opcode::ColumnSubstrPrefix | Opcode::ColumnOctetLength => {
            let (write_start, write_len) = register_range(op.p3, 1);
            (-1, 0, write_start, write_len)
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
            let (read_start, read_len) = register_range(op.p1, 2);
            let (write_start, write_len) = register_range(op.p3, 1);
            (read_start, read_len, write_start, write_len)
        }
        Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge => {
            let (lhs_start, lhs_len) = register_range(op.p1, 1);
            let (rhs_start, rhs_len) = register_range(op.p3, 1);
            let (normalized_start, normalized_len) = if lhs_start > 0 && rhs_start > 0 {
                let start = lhs_start.min(rhs_start);
                let end = (lhs_start + lhs_len - 1).max(rhs_start + rhs_len - 1);
                (start, end - start + 1)
            } else if lhs_start > 0 {
                (lhs_start, lhs_len)
            } else if rhs_start > 0 {
                (rhs_start, rhs_len)
            } else {
                (-1, 0)
            };
            let (write_start, write_len) = if (op.p5 & 0x20) != 0 {
                register_range(op.p2, 1)
            } else {
                (-1, 0)
            };
            (normalized_start, normalized_len, write_start, write_len)
        }
        Opcode::If | Opcode::IfNot | Opcode::IsNull | Opcode::NotNull | Opcode::IsTrue => {
            let (read_start, read_len) = register_range(op.p1, 1);
            (read_start, read_len, -1, 0)
        }
        Opcode::MakeRecord => {
            let (read_start, read_len) = register_range(op.p1, op.p2);
            let (write_start, write_len) = register_range(op.p3, 1);
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
    ops: smallvec::SmallVec<[VdbeOp; 64]>,
    /// Label states (indexed by `Label.0`).
    labels: Vec<LabelState>,
    /// Register allocator.
    regs: RegisterAllocator,
    /// Counter for anonymous placeholder numbering (1-based).
    next_anon_placeholder: u32,
    /// Table-to-index cursor metadata for REPLACE conflict resolution.
    table_index_meta: HashMap<i32, Vec<fsqlite_types::opcode::IndexCursorMeta>>,
}

impl ProgramBuilder {
    /// Create a new empty program builder.
    pub fn new() -> Self {
        Self {
            ops: smallvec::SmallVec::new(),
            labels: Vec::new(),
            regs: RegisterAllocator::new(),
            next_anon_placeholder: 1,
            table_index_meta: HashMap::new(),
        }
    }

    /// Get the next anonymous placeholder index (1-based) and increment the counter.
    pub fn next_anon_placeholder_idx(&mut self) -> u32 {
        let idx = self.next_anon_placeholder;
        self.next_anon_placeholder += 1;
        idx
    }

    /// Set the anonymous placeholder counter to a specific value.
    /// Used when codegen emission order differs from SQL textual order.
    pub fn set_next_anon_placeholder(&mut self, val: u32) {
        self.next_anon_placeholder = val;
    }

    /// Get the current anonymous placeholder counter without incrementing.
    pub fn current_anon_placeholder(&self) -> u32 {
        self.next_anon_placeholder
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

    // ── Table-index metadata ─────────────────────────────────────────────

    /// Register the index cursors associated with a table cursor.
    ///
    /// Used by the engine during REPLACE conflict resolution to delete
    /// orphaned secondary index entries before replacing the table row.
    pub fn register_table_indexes(
        &mut self,
        table_cursor: i32,
        indexes: Vec<fsqlite_types::opcode::IndexCursorMeta>,
    ) {
        if !indexes.is_empty() {
            self.table_index_meta
                .entry(table_cursor)
                .or_default()
                .extend(indexes);
        }
    }

    // ── Peephole Passes (IMPL-13) ───────────────────────────────────────

    /// Fuse `Integer(lit, reg) + ResultRow(reg, 1)` pairs into
    /// `FusedLiteralResultRow(lit, reg)` + `Noop`.
    ///
    /// Rewrites in place so program counters, jump targets, and the label
    /// tables remain valid without rewiring. The `ResultRow` is replaced
    /// with a `Noop` rather than removed so no following instruction
    /// shifts.
    ///
    /// Conservative preconditions per fusion site:
    /// - The `Integer`'s target register equals the `ResultRow`'s start
    ///   register.
    /// - The `ResultRow` emits exactly one column (`p2 == 1`).
    /// - The `ResultRow` is NOT a resolved jump target from any prior jump
    ///   in this program (a mid-pair jump would otherwise skip the
    ///   `Integer` write and run `ResultRow` against an unrelated register
    ///   value).
    /// - Neither instruction carries a non-`None` P4 payload.
    /// - Both instructions carry P5 == 0 and P3 == 0.
    ///
    /// Returns the number of fusions performed.
    pub fn apply_fuse_literal_result_row(&mut self) -> usize {
        let mut jump_targets: HashSet<i32> = HashSet::new();
        for op in self.ops.iter() {
            if op.opcode.is_jump() {
                jump_targets.insert(op.p2);
            }
        }

        let mut fused = 0usize;
        let len = self.ops.len();
        let mut i = 0;
        while i + 1 < len {
            let is_int = matches!(self.ops[i].opcode, Opcode::Integer)
                && self.ops[i].p3 == 0
                && self.ops[i].p5 == 0
                && matches!(self.ops[i].p4, P4::None);
            let is_row = matches!(self.ops[i + 1].opcode, Opcode::ResultRow)
                && self.ops[i + 1].p2 == 1
                && self.ops[i + 1].p3 == 0
                && self.ops[i + 1].p5 == 0
                && matches!(self.ops[i + 1].p4, P4::None);
            let same_reg = is_int && is_row && self.ops[i].p2 == self.ops[i + 1].p1;
            let row_addr = i32::try_from(i + 1).ok();
            let row_is_target = row_addr.is_some_and(|a| jump_targets.contains(&a));

            if same_reg && !row_is_target {
                let lit = self.ops[i].p1;
                let reg = self.ops[i].p2;
                self.ops[i] = VdbeOp {
                    opcode: Opcode::FusedLiteralResultRow,
                    p1: lit,
                    p2: reg,
                    p3: 0,
                    p4: P4::None,
                    p5: 0,
                };
                self.ops[i + 1] = VdbeOp {
                    opcode: Opcode::Noop,
                    p1: 0,
                    p2: 0,
                    p3: 0,
                    p4: P4::None,
                    p5: 0,
                };
                fused += 1;
                i += 2;
            } else {
                i += 1;
            }
        }
        fused
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
        let bind_parameter_requirement = compute_bind_parameter_requirement(&self.ops);
        let table_index_meta = self
            .table_index_meta
            .into_iter()
            .map(|(table_cursor, indexes)| (table_cursor, indexes.into_boxed_slice()))
            .collect();

        let inferred_register_count = self.ops.iter().fold(0, |max_register, op| {
            max_register.max(opcode_register_spans(op).max_touched_register())
        });
        let has_insert = self.ops.iter().any(|op| op.opcode == Opcode::Insert);
        // bd-perf (V2.1): Peephole pass — fuse NewRowid+MakeRecord+Insert
        // into FusedAppendInsert for simple sequential append patterns.
        let mut ops = self.ops;
        peephole_fuse_append_insert(&mut ops);
        // SAFETY: The VDBE dispatch loop relies on every non-empty program
        // terminating via OP_Halt (bounds check was removed in V2.3).
        // Empty programs are fine — the loop's debug_assert catches pc=0 >= len=0.
        if !ops.is_empty() {
            debug_assert!(
                ops.last().is_some_and(|op| op.opcode == Opcode::Halt),
                "VDBE program does not end with Halt — last opcode is {:?}",
                ops.last().map(|op| op.opcode)
            );
        }
        let requires_attached_memdb = compute_requires_attached_memdb(&ops);
        let requires_version_store = ops.iter().any(|op| op.opcode == Opcode::SetSnapshot);
        let program = VdbeProgram {
            ops,
            register_count: self.regs.count().max(inferred_register_count),
            bind_parameter_requirement,
            table_index_meta: Arc::new(table_index_meta),
            has_insert,
            requires_attached_memdb,
            requires_version_store,
        };
        program.verify_control_flow_targets()?;
        Ok(program)
    }
}

impl Default for ProgramBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// bd-perf (V2.1): Peephole optimizer — fuse NewRowid+MakeRecord+Insert into
/// FusedAppendInsert. Called from `ProgramBuilder::finish()` after label resolution.
fn peephole_fuse_append_insert(ops: &mut smallvec::SmallVec<[VdbeOp; 64]>) {
    let len = ops.len();
    if len < 3 {
        return;
    }
    let mut i = 0;
    while i + 2 < len {
        if ops[i].opcode == Opcode::NewRowid
            && ops[i + 1].opcode == Opcode::MakeRecord
            && ops[i + 2].opcode == Opcode::Insert
        {
            let cursor = ops[i].p1;
            let r_rowid = ops[i].p2;
            let r_start = ops[i + 1].p1;
            let n_cols = ops[i + 1].p2;
            let r_record = ops[i + 1].p3;
            let make_record_p4 = ops[i + 1].p4.clone();
            let insert_cursor = ops[i + 2].p1;
            let insert_record_reg = ops[i + 2].p2;
            let insert_rowid_reg = ops[i + 2].p3;
            let insert_flags = ops[i + 2].p5;
            let oe_flag = insert_flags & 0x0F;

            if cursor == insert_cursor
                && r_record == insert_record_reg
                && r_rowid == insert_rowid_reg
                && oe_flag == 2
            // OE_ABORT only
            {
                ops[i] = VdbeOp {
                    opcode: Opcode::FusedAppendInsert,
                    p1: cursor,
                    p2: r_start,
                    p3: n_cols,
                    p4: make_record_p4,
                    p5: insert_flags,
                };
                ops[i + 1] = VdbeOp {
                    opcode: Opcode::Noop,
                    p1: 0,
                    p2: 0,
                    p3: 0,
                    p4: P4::None,
                    p5: 0,
                };
                ops[i + 2] = VdbeOp {
                    opcode: Opcode::Noop,
                    p1: 0,
                    p2: 0,
                    p3: 0,
                    p4: P4::None,
                    p5: 0,
                };
                i += 3;
                continue;
            }
        }
        i += 1;
    }

    // bd-perf (V2.2): FusedOpenWriteLast DISABLED — Last has a P2 jump-if-empty
    // target that must be preserved. The Noop replacement silently dropped the
    // jump, causing data corruption when tables are empty (rowid 0 inserted
    // instead of jumping past the insert body). The ~5-7ns savings isn't worth
    // the correctness risk. Keep opcode defined for future proper implementation.
}

/// Returns `true` when a finalized program still needs an attached
/// `MemDatabase` to preserve its current semantics.
///
/// This is intentionally conservative. It only returns `false` for programs
/// that stay on storage cursors plus pure register/control-flow opcodes, which
/// lets hot prepared table executions skip the `MemDatabase` handoff entirely.
fn compute_requires_attached_memdb(ops: &[VdbeOp]) -> bool {
    compute_attached_memdb_requirement_reason(ops).is_some()
}

/// Return the first conservative reason a finalized program still needs an
/// attached `MemDatabase`.
fn compute_attached_memdb_requirement_reason(ops: &[VdbeOp]) -> Option<&'static str> {
    let mut storage_cursor_ids = HashSet::new();
    let mut sorter_cursor_ids = HashSet::new();

    for op in ops {
        match op.opcode {
            Opcode::OpenRead | Opcode::OpenWrite | Opcode::FusedOpenWriteLast => {
                storage_cursor_ids.insert(op.p1);
            }
            Opcode::SorterOpen => {
                sorter_cursor_ids.insert(op.p1);
            }
            Opcode::Close => {
                storage_cursor_ids.remove(&op.p1);
                sorter_cursor_ids.remove(&op.p1);
            }
            Opcode::OpenEphemeral
            | Opcode::OpenAutoindex
            | Opcode::OpenPseudo
            | Opcode::OpenDup
            | Opcode::ReopenIdx
            | Opcode::CreateBtree
            | Opcode::Clear
            | Opcode::Destroy
            | Opcode::Pagecount
            | Opcode::Program
            | Opcode::VBegin
            | Opcode::VCreate
            | Opcode::VDestroy
            | Opcode::VOpen
            | Opcode::VCheck
            | Opcode::VInitIn
            | Opcode::VFilter
            | Opcode::VColumn
            | Opcode::VNext
            | Opcode::VRename
            | Opcode::VUpdate => return Some("memdb_or_virtual_table_opcode"),
            Opcode::Rewind
            | Opcode::Last
            | Opcode::Next
            | Opcode::Prev
            | Opcode::Column
            | Opcode::ColumnSubstrPrefix
            | Opcode::ColumnOctetLength
            | Opcode::Count
            | Opcode::SeekLT
            | Opcode::SeekLE
            | Opcode::SeekGE
            | Opcode::SeekGT
            | Opcode::IfNoHope
            | Opcode::NoConflict
            | Opcode::NotFound
            | Opcode::Found
            | Opcode::SeekRowid
            | Opcode::NotExists
            | Opcode::Insert
            | Opcode::Delete
            | Opcode::RowData
            | Opcode::Rowid
            | Opcode::NullRow
            | Opcode::IfNullRow
            | Opcode::IfEmpty
            | Opcode::IfSizeBetween
            | Opcode::IdxInsert
            | Opcode::IdxDelete
            | Opcode::DeferredSeek
            | Opcode::IdxRowid
            | Opcode::FinishSeek
            | Opcode::IdxLE
            | Opcode::IdxGT
            | Opcode::IdxLT
            | Opcode::IdxGE
            | Opcode::SetSnapshot
            | Opcode::CountIndexEqRun
            | Opcode::FusedAppendInsert
                if !storage_cursor_ids.contains(&op.p1) && !sorter_cursor_ids.contains(&op.p1) =>
            {
                return Some("cursor_opcode_without_storage_or_sorter_open");
            }
            _ => {}
        }
    }

    None
}

// ── VDBE Program ────────────────────────────────────────────────────────────

pub(crate) type TableIndexMetaMap = HashMap<i32, Box<[fsqlite_types::opcode::IndexCursorMeta]>>;

/// Static storage role inferred from a VDBE root cursor open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageRootRole {
    Table,
    Index,
    Unknown,
}

/// Static storage access kind inferred from a VDBE root cursor open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageRootAccess {
    Read,
    Write,
}

/// Deterministic storage-root usage emitted by finalized bytecode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageRootUsage {
    pub pc: usize,
    pub cursor_id: i32,
    pub root_page: i32,
    pub access: StorageRootAccess,
    pub role: StorageRootRole,
}

fn storage_root_usage_for_op(pc: usize, op: &VdbeOp) -> Option<StorageRootUsage> {
    let access = match op.opcode {
        Opcode::OpenRead => StorageRootAccess::Read,
        Opcode::OpenWrite | Opcode::FusedOpenWriteLast => StorageRootAccess::Write,
        _ => return None,
    };
    let role = match &op.p4 {
        P4::Table(_) => StorageRootRole::Table,
        P4::Index(_) => StorageRootRole::Index,
        _ => StorageRootRole::Unknown,
    };

    Some(StorageRootUsage {
        pc,
        cursor_id: op.p1,
        root_page: op.p2,
        access,
        role,
    })
}

/// A finalized VDBE bytecode program ready for execution.
#[derive(Debug, Clone, PartialEq)]
pub struct VdbeProgram {
    /// The instruction sequence.
    ops: smallvec::SmallVec<[VdbeOp; 64]>,
    /// Number of registers needed (high water mark from allocation).
    register_count: i32,
    /// Precomputed bind parameter requirement for `Opcode::Variable` opcodes.
    ///
    /// `Ok(max_index)` means all variable opcodes carry valid 1-based indexes.
    /// `Err(raw_index)` stores the first invalid raw index encountered.
    bind_parameter_requirement: std::result::Result<usize, i32>,
    /// Table-to-index cursor metadata for REPLACE conflict resolution.
    table_index_meta: Arc<TableIndexMetaMap>,
    /// Precomputed flag: true when the program contains at least one Insert
    /// opcode, meaning column defaults may be needed during execution.
    has_insert: bool,
    /// Precomputed flag: true when execution still needs an attached
    /// `MemDatabase` to preserve current opcode semantics.
    requires_attached_memdb: bool,
    /// Precomputed flag: true when execution can request historical pages.
    requires_version_store: bool,
}

impl VdbeProgram {
    fn verify_control_flow_targets(&self) -> Result<()> {
        let op_count = self.ops.len();
        for (pc, op) in self.ops.iter().enumerate() {
            match op.opcode {
                Opcode::Init => verify_jump_target_operand(
                    pc,
                    op.opcode,
                    "p2",
                    op.p2,
                    op_count,
                    JumpTargetBounds::InitEntry,
                )?,
                Opcode::Goto
                | Opcode::Gosub
                | Opcode::Once
                | Opcode::If
                | Opcode::IfNot
                | Opcode::IsNull
                | Opcode::NotNull
                | Opcode::Rewind
                | Opcode::Sort
                | Opcode::SorterSort
                | Opcode::Last
                | Opcode::Next
                | Opcode::SorterNext
                | Opcode::Prev
                | Opcode::SeekRowid
                | Opcode::SeekGE
                | Opcode::SeekGT
                | Opcode::SeekLE
                | Opcode::SeekLT
                | Opcode::NotFound
                | Opcode::NotExists
                | Opcode::IfNoHope
                | Opcode::Found
                | Opcode::NoConflict
                | Opcode::SorterCompare
                | Opcode::IfNullRow
                | Opcode::IfNotOpen
                | Opcode::IsType
                | Opcode::IfEmpty
                | Opcode::IfSizeBetween
                | Opcode::IdxRowid
                | Opcode::IdxLE
                | Opcode::IdxGT
                | Opcode::IdxLT
                | Opcode::IdxGE
                | Opcode::DecrJumpZero
                | Opcode::IfPos
                | Opcode::RowSetRead
                | Opcode::RowSetTest
                | Opcode::FkIfZero
                | Opcode::IfNotZero
                | Opcode::IncrVacuum
                | Opcode::Filter
                | Opcode::VFilter
                | Opcode::VNext => verify_jump_target_operand(
                    pc,
                    op.opcode,
                    "p2",
                    op.p2,
                    op_count,
                    JumpTargetBounds::Instruction,
                )?,
                Opcode::MustBeInt | Opcode::InitCoroutine if op.p2 > 0 => {
                    verify_jump_target_operand(
                        pc,
                        op.opcode,
                        "p2",
                        op.p2,
                        op_count,
                        JumpTargetBounds::Instruction,
                    )?;
                }
                Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge
                    if (op.p5 & 0x20) == 0 =>
                {
                    verify_jump_target_operand(
                        pc,
                        op.opcode,
                        "p2",
                        op.p2,
                        op_count,
                        JumpTargetBounds::Instruction,
                    )?;
                }
                Opcode::Jump => {
                    verify_jump_target_operand(
                        pc,
                        op.opcode,
                        "p1",
                        op.p1,
                        op_count,
                        JumpTargetBounds::Instruction,
                    )?;
                    verify_jump_target_operand(
                        pc,
                        op.opcode,
                        "p2",
                        op.p2,
                        op_count,
                        JumpTargetBounds::Instruction,
                    )?;
                    verify_jump_target_operand(
                        pc,
                        op.opcode,
                        "p3",
                        op.p3,
                        op_count,
                        JumpTargetBounds::Instruction,
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

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

    /// Highest 1-based bind parameter index referenced by the program.
    ///
    /// Returns `Ok(0)` when no `Variable` opcodes are present.
    /// Returns `Err(raw_index)` if the bytecode contains an invalid
    /// parameter index (`<= 0` or not representable as `usize`).
    pub fn max_bind_parameter_index(&self) -> std::result::Result<usize, i32> {
        self.bind_parameter_requirement
    }

    /// Get the instruction at the given program counter.
    pub fn get(&self, pc: usize) -> Option<&VdbeOp> {
        self.ops.get(pc)
    }

    /// Table-to-index cursor metadata for REPLACE conflict resolution.
    pub fn table_index_meta(&self) -> &TableIndexMetaMap {
        self.table_index_meta.as_ref()
    }

    /// Returns storage B-tree root usage in deterministic instruction order.
    ///
    /// Conflict-topology and backend-identity diagnostics can use this to tie
    /// bytecode to root-page level heat without ad hoc opcode scans.
    pub fn storage_root_usages(&self) -> impl Iterator<Item = StorageRootUsage> + '_ {
        self.ops
            .iter()
            .enumerate()
            .filter_map(|(pc, op)| storage_root_usage_for_op(pc, op))
    }

    pub(crate) fn shared_table_index_meta(&self) -> &Arc<TableIndexMetaMap> {
        &self.table_index_meta
    }

    /// Returns `true` if the program contains any `Insert` opcodes,
    /// meaning column defaults may be needed during execution.
    /// Precomputed at build time — O(1) at call time.
    pub fn has_insert_ops(&self) -> bool {
        self.has_insert
    }

    /// Returns `true` when this program still requires an attached
    /// `MemDatabase` for opcode semantics.
    pub fn requires_attached_memdb(&self) -> bool {
        self.requires_attached_memdb
    }

    /// Returns the first conservative reason this program still requires an
    /// attached `MemDatabase`, or `None` for storage-only VDBE programs.
    pub fn attached_memdb_requirement_reason(&self) -> Option<&'static str> {
        compute_attached_memdb_requirement_reason(&self.ops)
    }

    /// Returns `true` when this program can read historical page versions.
    pub fn requires_version_store(&self) -> bool {
        self.requires_version_store
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
                P4::FuncNameCollated(f, c) => format!("(func){f} coll={c}"),
                P4::Table(t) => format!("(tbl){t}"),
                P4::Index(i) => format!("(idx){i}"),
                P4::Affinity(a) => format!("(aff){a}"),
                P4::PrecomputedHeader(header) => format!("(hdr)[{}B]", header.template.len()),
                P4::TimeTravelCommitSeq(seq) => format!("(tt-seq){seq}"),
                P4::TimeTravelTimestamp(ts) => format!("(tt-ts){ts}"),
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

fn compute_bind_parameter_requirement(ops: &[VdbeOp]) -> std::result::Result<usize, i32> {
    let mut max_required = 0_usize;
    for op in ops {
        if op.opcode != Opcode::Variable {
            continue;
        }
        let one_based = match usize::try_from(op.p1) {
            Ok(index) if index > 0 => index,
            _ => return Err(op.p1),
        };
        max_required = max_required.max(one_based);
    }
    Ok(max_required)
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
        DEFAULT_RAPTORQ_REPAIR_SYMBOLS, MAX_RAPTORQ_REPAIR_SYMBOLS,
        persist_wal_fec_raptorq_repair_symbols, read_wal_fec_raptorq_repair_symbols,
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
    #[derive(Debug, Clone, Copy)]
    pub enum DifferentialViewsSetting {
        Off,
        On,
    }

    impl DifferentialViewsSetting {
        #[must_use]
        pub const fn is_enabled(&self) -> bool {
            matches!(self, Self::On)
        }

        #[must_use]
        pub const fn from_enabled(enabled: bool) -> Self {
            if enabled { Self::On } else { Self::Off }
        }
    }

    #[derive(Debug, Clone)]
    #[allow(clippy::struct_excessive_bools)]
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
        /// Temporary storage mode (`0` default, `1` file, `2` memory).
        pub temp_store: i64,
        /// Memory-map size in bytes (`PRAGMA mmap_size`).
        pub mmap_size: i64,
        /// Auto-vacuum mode (`0` none, `1` full, `2` incremental).
        pub auto_vacuum: i64,
        /// WAL auto-checkpoint threshold in pages.
        pub wal_autocheckpoint: i64,
        /// User schema version (`PRAGMA user_version`).
        pub user_version: i64,
        /// Application ID (`PRAGMA application_id`).
        pub application_id: i64,
        /// Foreign key enforcement toggle (`PRAGMA foreign_keys`).
        pub foreign_keys: bool,
        /// Recursive trigger toggle (`PRAGMA recursive_triggers`).
        pub recursive_triggers: bool,
        /// Query-only toggle (`PRAGMA query_only`).
        pub query_only: bool,
        /// Connection-level SSI toggle (`PRAGMA fsqlite.serializable`).
        pub serializable: bool,
        /// Differential-view streaming toggle (`PRAGMA fsqlite_differential_views`).
        pub differential_views: DifferentialViewsSetting,
        /// WAL-FEC repair symbol budget (`PRAGMA raptorq_repair_symbols`).
        pub raptorq_repair_symbols: u8,
        /// MVCC maximum committed versions per page chain before eager GC.
        /// `PRAGMA fsqlite.mvcc_max_chain_length`.
        pub mvcc_max_chain_length: usize,
        /// MVCC serialized writer lease duration in seconds.
        /// `PRAGMA fsqlite.mvcc_writer_lease_secs`.
        pub mvcc_writer_lease_secs: u64,
        /// `PRAGMA writable_schema` toggle — allows direct DML on sqlite_master.
        pub writable_schema: bool,
        /// `PRAGMA case_sensitive_like` toggle. When `false` (the default) LIKE
        /// folds ASCII case; when `true` LIKE is byte-exact (case-sensitive).
        pub case_sensitive_like: bool,
        /// `PRAGMA trusted_schema` toggle. Default ON (1), matching the C library
        /// default (`SQLITE_TRUSTED_SCHEMA` unset). Set/readback surface only.
        pub trusted_schema: bool,
        /// `PRAGMA read_uncommitted` toggle (default OFF). Set/readback surface.
        pub read_uncommitted: bool,
        /// `PRAGMA cell_size_check` toggle (default OFF). Set/readback surface.
        pub cell_size_check: bool,
        /// `PRAGMA checkpoint_fullfsync` toggle (default OFF). Set/readback surface.
        pub checkpoint_fullfsync: bool,
        /// `PRAGMA automatic_index` toggle (default ON). Set/readback surface.
        pub automatic_index: bool,
        /// `PRAGMA locking_mode` (`normal` or `exclusive`; default `normal`).
        /// Set/readback surface only.
        pub locking_mode: String,
        /// `PRAGMA secure_delete` tri-state: 0 = OFF, 1 = ON, 2 = FAST (default 0).
        /// Set/readback surface only.
        pub secure_delete: i64,
        /// `PRAGMA threads` auxiliary worker-thread limit (default 0). Advisory:
        /// FrankenSQLite does not spawn SQLite-style sort helper threads, so this
        /// is a stored limit clamped to the stock maximum (8).
        pub threads: i64,
    }

    impl Default for ConnectionPragmaState {
        fn default() -> Self {
            Self {
                journal_mode: "wal".to_owned(),
                synchronous: "NORMAL".to_owned(),
                cache_size: -2000,
                page_size: 4096,
                busy_timeout_ms: 5000,
                temp_store: 0,
                mmap_size: 0,
                auto_vacuum: 0,
                wal_autocheckpoint: 1000,
                user_version: 0,
                application_id: 0,
                foreign_keys: false,
                recursive_triggers: false,
                query_only: false,
                serializable: true,
                differential_views: DifferentialViewsSetting::Off,
                raptorq_repair_symbols: DEFAULT_RAPTORQ_REPAIR_SYMBOLS,
                mvcc_max_chain_length: 64,
                mvcc_writer_lease_secs: 30,
                writable_schema: false,
                case_sensitive_like: false,
                trusted_schema: true,
                read_uncommitted: false,
                cell_size_check: false,
                checkpoint_fullfsync: false,
                automatic_index: true,
                locking_mode: "normal".to_owned(),
                secure_delete: 0,
                threads: 0,
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
    /// Handles common connection-scoped PRAGMAs used by the harness and
    /// compatibility paths. Returns `Unsupported` for pragmas not handled at
    /// this layer, allowing the caller to chain with [`apply`].
    pub fn apply_connection_pragma(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        let name = &stmt.name.name;
        if is_fsqlite_serializable(&stmt.name) {
            return apply_serializable_connection(state, stmt);
        }
        if is_fsqlite_differential_views(&stmt.name) {
            return apply_differential_views_connection(state, stmt);
        }
        if is_raptorq_repair_symbols(&stmt.name) {
            return apply_raptorq_repair_symbols_connection(state, stmt);
        }
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
        if name.eq_ignore_ascii_case("temp_store") {
            return apply_temp_store(state, stmt);
        }
        if name.eq_ignore_ascii_case("mmap_size") {
            return apply_mmap_size(state, stmt);
        }
        if name.eq_ignore_ascii_case("auto_vacuum") {
            return apply_auto_vacuum(state, stmt);
        }
        if name.eq_ignore_ascii_case("wal_autocheckpoint") {
            return apply_wal_autocheckpoint(state, stmt);
        }
        if name.eq_ignore_ascii_case("user_version") {
            return apply_user_version(state, stmt);
        }
        if name.eq_ignore_ascii_case("application_id") {
            return apply_application_id(state, stmt);
        }
        if name.eq_ignore_ascii_case("foreign_keys") {
            return apply_foreign_keys(state, stmt);
        }
        if name.eq_ignore_ascii_case("recursive_triggers") {
            return apply_recursive_triggers(state, stmt);
        }
        if name.eq_ignore_ascii_case("query_only") {
            return apply_query_only(state, stmt);
        }
        if name.eq_ignore_ascii_case("writable_schema") {
            return apply_writable_schema(state, stmt);
        }
        if name.eq_ignore_ascii_case("case_sensitive_like") {
            return apply_case_sensitive_like(state, stmt);
        }
        if name.eq_ignore_ascii_case("trusted_schema") {
            return apply_bool_toggle(&mut state.trusted_schema, stmt);
        }
        if name.eq_ignore_ascii_case("read_uncommitted") {
            return apply_bool_toggle(&mut state.read_uncommitted, stmt);
        }
        if name.eq_ignore_ascii_case("cell_size_check") {
            return apply_bool_toggle(&mut state.cell_size_check, stmt);
        }
        if name.eq_ignore_ascii_case("checkpoint_fullfsync") {
            return apply_bool_toggle(&mut state.checkpoint_fullfsync, stmt);
        }
        if name.eq_ignore_ascii_case("automatic_index") {
            return apply_bool_toggle(&mut state.automatic_index, stmt);
        }
        if name.eq_ignore_ascii_case("locking_mode") {
            return apply_locking_mode(state, stmt);
        }
        if name.eq_ignore_ascii_case("secure_delete") {
            return apply_secure_delete(state, stmt);
        }
        if name.eq_ignore_ascii_case("threads") {
            return apply_threads(state, stmt);
        }
        if is_fsqlite_mvcc_max_chain_length(&stmt.name) {
            return apply_mvcc_max_chain_length(state, stmt);
        }
        if is_fsqlite_mvcc_writer_lease_secs(&stmt.name) {
            return apply_mvcc_writer_lease_secs(state, stmt);
        }
        Ok(PragmaOutput::Unsupported)
    }

    fn apply_serializable_connection(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Bool(state.serializable)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.serializable = enabled;
                Ok(PragmaOutput::Bool(enabled))
            }
        }
    }

    /// Generic boolean-toggle PRAGMA: a bare query echoes the current value and an
    /// assignment stores and echoes the new value, matching C SQLite's integer
    /// 0/1 readback. Used for the set/readback-only pragmas trusted_schema,
    /// read_uncommitted, cell_size_check, checkpoint_fullfsync, and
    /// automatic_index. (GH #282, #278, #262, #281, #283)
    fn apply_bool_toggle(flag: &mut bool, stmt: &PragmaStatement) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Bool(*flag)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                *flag = enabled;
                Ok(PragmaOutput::Bool(enabled))
            }
        }
    }

    /// `PRAGMA locking_mode [= NORMAL|EXCLUSIVE]`. A bare query echoes the current
    /// mode; an assignment accepts NORMAL/EXCLUSIVE case-insensitively and echoes
    /// the lowercased mode. Any other value is ignored and the current mode is
    /// echoed unchanged, matching C SQLite. (GH #273)
    fn apply_locking_mode(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Text(state.locking_mode.clone())),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let requested = parse_text_expr(expr)?.to_ascii_lowercase();
                if requested == "normal" || requested == "exclusive" {
                    state.locking_mode = requested;
                }
                Ok(PragmaOutput::Text(state.locking_mode.clone()))
            }
        }
    }

    /// `PRAGMA secure_delete [= OFF|ON|FAST|0|1|2]`. Tri-state: 0 = OFF, 1 = ON,
    /// 2 = FAST. C SQLite reports the integer on readback and echoes the new value
    /// on assignment. This is the set/readback surface only; the actual
    /// zero-on-delete storage semantics are tracked separately. (GH #277)
    fn apply_secure_delete(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.secure_delete)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let value = parse_secure_delete_value(expr)?;
                state.secure_delete = value;
                Ok(PragmaOutput::Int(value))
            }
        }
    }

    fn parse_secure_delete_value(expr: &Expr) -> Result<i64> {
        if let Expr::Literal(Literal::Integer(n), _) = expr {
            return match *n {
                0..=2 => Ok(*n),
                _ => Err(FrankenError::OutOfRange {
                    what: "secure_delete".to_owned(),
                    value: n.to_string(),
                }),
            };
        }
        if let Ok(text) = parse_text_expr(expr) {
            if text.eq_ignore_ascii_case("fast") {
                return Ok(2);
            }
        }
        // OFF/ON/TRUE/FALSE map to 0/1.
        Ok(i64::from(parse_bool(expr)?))
    }

    /// `PRAGMA threads [= N]`. A bare query reports the current limit; an
    /// assignment with a non-negative N clamps to the stock maximum (8), stores
    /// it, and echoes the effective value. A negative argument leaves the limit
    /// unchanged and just reports it, matching C SQLite. (GH #279)
    fn apply_threads(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        /// Stock `SQLITE_MAX_WORKER_THREADS`.
        const MAX_WORKER_THREADS: i64 = 8;
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.threads)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let n = parse_integer_expr(expr)?;
                if n >= 0 {
                    state.threads = n.min(MAX_WORKER_THREADS);
                }
                Ok(PragmaOutput::Int(state.threads))
            }
        }
    }

    /// `PRAGMA case_sensitive_like = ON|OFF`. SQLite treats this as write-only,
    /// but mirroring the other boolean toggles we also echo the current value on
    /// the no-argument query form. When ON, LIKE becomes byte-exact; the actual
    /// matching behavior is honored by the LIKE evaluation paths that read this
    /// flag (via the connection's pragma state).
    fn apply_case_sensitive_like(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Bool(state.case_sensitive_like)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.case_sensitive_like = enabled;
                Ok(PragmaOutput::Bool(enabled))
            }
        }
    }

    fn apply_raptorq_repair_symbols_connection(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(i64::from(state.raptorq_repair_symbols))),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let value = parse_integer_expr(expr)?;
                if !(0..=i64::from(MAX_RAPTORQ_REPAIR_SYMBOLS)).contains(&value) {
                    return Err(FrankenError::OutOfRange {
                        what: "raptorq_repair_symbols".to_owned(),
                        value: value.to_string(),
                    });
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                {
                    state.raptorq_repair_symbols = value as u8;
                }
                Ok(PragmaOutput::Int(i64::from(state.raptorq_repair_symbols)))
            }
        }
    }

    fn apply_differential_views_connection(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Bool(state.differential_views.is_enabled())),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.differential_views = DifferentialViewsSetting::from_enabled(enabled);
                Ok(PragmaOutput::Bool(enabled))
            }
        }
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

    fn apply_temp_store(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.temp_store)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_temp_store_value(expr)?;
                state.temp_store = val;
                Ok(PragmaOutput::Int(val))
            }
        }
    }

    fn apply_mmap_size(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.mmap_size)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                state.mmap_size = val.max(0);
                Ok(PragmaOutput::Int(state.mmap_size))
            }
        }
    }

    fn apply_auto_vacuum(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.auto_vacuum)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_auto_vacuum_value(expr)?;
                state.auto_vacuum = val;
                Ok(PragmaOutput::Int(val))
            }
        }
    }

    fn apply_wal_autocheckpoint(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.wal_autocheckpoint)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                state.wal_autocheckpoint = val.max(0);
                Ok(PragmaOutput::Int(state.wal_autocheckpoint))
            }
        }
    }

    fn apply_user_version(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.user_version)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                state.user_version = val;
                Ok(PragmaOutput::Int(val))
            }
        }
    }

    fn apply_application_id(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.application_id)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let val = parse_integer_expr(expr)?;
                state.application_id = val;
                Ok(PragmaOutput::Int(val))
            }
        }
    }

    fn apply_foreign_keys(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(i64::from(state.foreign_keys))),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.foreign_keys = enabled;
                Ok(PragmaOutput::Int(i64::from(enabled)))
            }
        }
    }

    fn apply_recursive_triggers(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(i64::from(state.recursive_triggers))),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.recursive_triggers = enabled;
                Ok(PragmaOutput::Int(i64::from(enabled)))
            }
        }
    }

    fn apply_query_only(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(i64::from(state.query_only))),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.query_only = enabled;
                Ok(PragmaOutput::Int(i64::from(enabled)))
            }
        }
    }

    fn apply_writable_schema(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(i64::from(state.writable_schema))),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let enabled = parse_bool(expr)?;
                state.writable_schema = enabled;
                Ok(PragmaOutput::Int(i64::from(enabled)))
            }
        }
    }

    fn parse_temp_store_value(expr: &Expr) -> Result<i64> {
        if let Expr::Literal(Literal::Integer(n), _) = expr {
            return match *n {
                0..=2 => Ok(*n),
                _ => Err(FrankenError::OutOfRange {
                    what: "temp_store".to_owned(),
                    value: n.to_string(),
                }),
            };
        }

        let text = parse_text_expr(expr)?;
        match text.to_ascii_lowercase().as_str() {
            "default" => Ok(0),
            "file" => Ok(1),
            "memory" => Ok(2),
            _ => Err(FrankenError::TypeMismatch {
                expected: "DEFAULT|FILE|MEMORY|0|1|2".to_owned(),
                actual: text,
            }),
        }
    }

    fn parse_auto_vacuum_value(expr: &Expr) -> Result<i64> {
        if let Expr::Literal(Literal::Integer(n), _) = expr {
            return match *n {
                0..=2 => Ok(*n),
                _ => Err(FrankenError::OutOfRange {
                    what: "auto_vacuum".to_owned(),
                    value: n.to_string(),
                }),
            };
        }

        let text = parse_text_expr(expr)?;
        match text.to_ascii_lowercase().as_str() {
            "none" => Ok(0),
            "full" => Ok(1),
            "incremental" => Ok(2),
            _ => Err(FrankenError::TypeMismatch {
                expected: "NONE|FULL|INCREMENTAL|0|1|2".to_owned(),
                actual: text,
            }),
        }
    }

    fn is_power_of_two(n: i64) -> bool {
        n > 0 && (n & (n - 1)) == 0
    }

    /// Extract a text value from a PRAGMA assignment expression.
    fn parse_text_expr(expr: &Expr) -> Result<String> {
        match expr {
            Expr::Literal(Literal::String(s), _) => Ok(s.clone()),
            Expr::Column(col, _) => Ok(col.column.to_string()),
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

    fn is_fsqlite_differential_views(name: &QualifiedName) -> bool {
        match name.schema.as_deref() {
            Some(schema) => {
                schema.eq_ignore_ascii_case("fsqlite")
                    && name.name.eq_ignore_ascii_case("differential_views")
            }
            None => name.name.eq_ignore_ascii_case("fsqlite_differential_views"),
        }
    }

    fn is_raptorq_repair_symbols(name: &QualifiedName) -> bool {
        let schema_ok = match name.schema.as_deref() {
            None => true,
            Some(schema) => schema.eq_ignore_ascii_case("fsqlite"),
        };
        schema_ok && name.name.eq_ignore_ascii_case("raptorq_repair_symbols")
    }

    fn is_fsqlite_mvcc_max_chain_length(name: &QualifiedName) -> bool {
        name.schema
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("fsqlite"))
            && name.name.eq_ignore_ascii_case("mvcc_max_chain_length")
    }

    fn is_fsqlite_mvcc_writer_lease_secs(name: &QualifiedName) -> bool {
        name.schema
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("fsqlite"))
            && name.name.eq_ignore_ascii_case("mvcc_writer_lease_secs")
    }

    fn apply_mvcc_max_chain_length(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.mvcc_max_chain_length as i64)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let value = parse_integer_expr(expr)?;
                if value < 2 {
                    return Err(FrankenError::OutOfRange {
                        what: "fsqlite.mvcc_max_chain_length".into(),
                        value: format!("{value} (minimum 2)"),
                    });
                }
                #[allow(clippy::cast_sign_loss)]
                {
                    state.mvcc_max_chain_length = value as usize;
                }
                // Note: value is stored in pragma_state and will be read by
                // the MVCC layer when creating concurrent execution contexts.
                // The MvccCoordinator's own max_chain_length is set at
                // construction; this PRAGMA value takes effect for new
                // concurrent transactions opened on this connection.
                Ok(PragmaOutput::Int(value))
            }
        }
    }

    fn apply_mvcc_writer_lease_secs(
        state: &mut ConnectionPragmaState,
        stmt: &PragmaStatement,
    ) -> Result<PragmaOutput> {
        match &stmt.value {
            None => Ok(PragmaOutput::Int(state.mvcc_writer_lease_secs as i64)),
            Some(PragmaValue::Assign(expr) | PragmaValue::Call(expr)) => {
                let value = parse_integer_expr(expr)?;
                if value < 1 {
                    return Err(FrankenError::OutOfRange {
                        what: "fsqlite.mvcc_writer_lease_secs".into(),
                        value: format!("{value} (minimum 1)"),
                    });
                }
                #[allow(clippy::cast_sign_loss)]
                {
                    state.mvcc_writer_lease_secs = value as u64;
                }
                // Note: same propagation model as mvcc_max_chain_length above.
                Ok(PragmaOutput::Int(value))
            }
        }
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
                        actual: col.column.to_string(),
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
            Expr::Column(col, _) => (col.column.to_string(), parse_str_bool(&col.column)),
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
            P4::PrecomputedHeader(fsqlite_types::record::PrecomputedRecordHeader::new(&[
                fsqlite_types::record::PrecomputedSerialTypeKind::NullPlaceholder,
                fsqlite_types::record::PrecomputedSerialTypeKind::RealOrNull,
            ])),
        ];
        assert_eq!(variants.len(), 11);

        // Verify each variant matches itself.
        assert!(matches!(variants[0], P4::None));
        assert!(matches!(variants[1], P4::Int(42)));
        assert!(matches!(variants[2], P4::Int64(i64::MAX)));
        assert!(matches!(variants[3], P4::Real(_)));
        assert!(matches!(variants[4], P4::Str(_)));
        assert!(matches!(variants[5], P4::Blob(_)));
        assert!(matches!(variants[6], P4::Collation(_)));
        assert!(matches!(variants[7], P4::FuncName(ref s) if s == "count"));
        assert!(matches!(variants[8], P4::Table(ref s) if s == "users"));
        assert!(matches!(variants[9], P4::Affinity(ref s) if s == "ddd"));
        assert!(matches!(
            variants[10],
            P4::PrecomputedHeader(ref header) if header.template == vec![3, 0, 0]
        ));
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
        b.resolve_label(label_b);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        assert_eq!(b.op_at(jump2).unwrap().p2, 4);

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

    #[test]
    fn test_program_builder_infers_register_count_from_manual_opcode_registers() {
        let mut builder = ProgramBuilder::new();
        builder.emit_op(Opcode::Integer, 11, 3, 0, P4::None, 0);
        builder.emit_op(Opcode::ResultRow, 3, 1, 0, P4::None, 0);
        builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let program = builder.finish().expect("program should build");
        assert_eq!(
            program.register_count(),
            3,
            "bytecode that writes raw registers must still allocate a large enough register file",
        );
    }

    #[test]
    fn test_program_builder_infers_register_count_for_non_contiguous_comparison_operands() {
        let mut builder = ProgramBuilder::new();
        builder.emit_op(Opcode::Eq, 2, 0, 7, P4::None, 0);
        builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let program = builder.finish().expect("program should build");
        assert_eq!(
            program.register_count(),
            7,
            "comparison opcodes must account for both read registers even when they are not contiguous",
        );
    }

    #[test]
    fn test_program_builder_infers_register_count_for_store_p2_comparisons() {
        let mut builder = ProgramBuilder::new();
        builder.emit_op(Opcode::Eq, 2, 9, 7, P4::None, 0x20);
        builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let program = builder.finish().expect("program should build");
        assert_eq!(
            program.register_count(),
            9,
            "SQLITE_STOREP2 comparisons must reserve the destination register in the pre-sized register file",
        );
    }

    #[test]
    fn test_program_builder_rejects_out_of_bounds_goto_target() {
        let mut builder = ProgramBuilder::new();
        builder.emit_op(Opcode::Goto, 0, 99, 0, P4::None, 0);
        builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let err = builder.finish().expect_err("invalid jump target must fail");
        match err {
            FrankenError::Internal(message) => {
                assert!(message.contains("Goto"));
                assert!(message.contains("p2"));
            }
            other => assert!(
                matches!(other, FrankenError::Internal(_)),
                "expected internal verifier error, got {other:?}"
            ),
        }
    }

    #[test]
    fn test_program_builder_rejects_out_of_bounds_jump_branch_target() {
        let mut builder = ProgramBuilder::new();
        builder.emit_op(Opcode::Jump, 0, 1, 42, P4::None, 0);
        builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let err = builder
            .finish()
            .expect_err("invalid branch target must fail");
        match err {
            FrankenError::Internal(message) => {
                assert!(message.contains("Jump"));
                assert!(message.contains("p3"));
            }
            other => assert!(
                matches!(other, FrankenError::Internal(_)),
                "expected internal verifier error, got {other:?}"
            ),
        }
    }

    // ── test_all_opcode_dispatch_coverage ────────────────────────────────
    #[test]
    fn test_all_opcode_dispatch_coverage() {
        // Every assigned Opcode enum byte has a valid name and can be
        // constructed from its byte value. This ensures no gaps in the enum.
        for byte in 1..Opcode::COUNT as u8 {
            let opcode = Opcode::from_byte(byte);
            assert!(
                opcode.is_some(),
                "Opcode::from_byte({byte}) returned None — gap in opcode enum"
            );
            let opcode = opcode.unwrap();
            let name = opcode.name();
            assert!(!name.is_empty(), "opcode {byte} has empty name");
        }
        assert_eq!(Opcode::from_byte(Opcode::COUNT as u8), None);
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
        assert_eq!(prog.max_bind_parameter_index().unwrap(), 0);

        // The Init instruction's p2 should point to address 4 (after Halt).
        assert_eq!(prog.get(0).unwrap().opcode, Opcode::Init);
        assert_eq!(prog.get(0).unwrap().p2, 4);
    }

    #[test]
    fn test_program_precomputes_max_bind_parameter_index() {
        let mut b = ProgramBuilder::new();
        b.emit_op(Opcode::Variable, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Variable, 4, 2, 0, P4::None, 0);
        b.emit_op(Opcode::Variable, 2, 3, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();
        assert_eq!(prog.max_bind_parameter_index(), Ok(4));
    }

    #[test]
    fn test_program_tracks_invalid_bind_parameter_index() {
        let mut b = ProgramBuilder::new();
        b.emit_op(Opcode::Variable, 0, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        let prog = b.finish().unwrap();
        assert_eq!(prog.max_bind_parameter_index(), Err(0));
    }

    #[test]
    fn test_program_storage_only_hot_path_does_not_require_attached_memdb() {
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenWrite, 0, 256, 0, P4::Int(1), 0);
        b.emit_op(Opcode::Integer, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 42, 2, 0, P4::None, 0);
        b.emit_op(Opcode::MakeRecord, 2, 1, 3, P4::None, 0);
        b.emit_op(Opcode::Insert, 0, 3, 1, P4::None, 0);
        b.emit_op(Opcode::Count, 0, 4, 0, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 4, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        assert!(
            !prog.requires_attached_memdb(),
            "storage-only table hot paths should not force a MemDatabase handoff"
        );
    }

    #[test]
    fn test_program_with_ephemeral_cursor_requires_attached_memdb() {
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::OpenEphemeral, 0, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().expect("program should build");
        assert!(
            prog.requires_attached_memdb(),
            "ephemeral table programs still depend on the attached MemDatabase"
        );
    }

    #[test]
    fn test_program_with_sorter_cursor_does_not_require_attached_memdb() -> Result<()> {
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        b.emit_op(Opcode::SorterOpen, 0, 1, 0, P4::Str("+".to_owned()), 0);
        b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish()?;
        assert!(
            !prog.requires_attached_memdb(),
            "sorter-backed temp/exchange state is owned by VDBE and should not force a MemDatabase handoff"
        );
        Ok(())
    }

    #[test]
    fn test_program_builder_accumulates_table_index_meta_by_table_cursor() {
        use fsqlite_types::opcode::IndexCursorMeta;

        let mut b = ProgramBuilder::new();
        b.register_table_indexes(
            3,
            vec![IndexCursorMeta {
                cursor_id: 4,
                column_indices: vec![0, 2],
            }],
        );
        b.register_table_indexes(
            3,
            vec![IndexCursorMeta {
                cursor_id: 5,
                column_indices: vec![1],
            }],
        );

        let prog = b.finish().expect("program should build");
        let metas = prog
            .table_index_meta()
            .get(&3)
            .expect("table cursor metadata should be present");
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].cursor_id, 4);
        assert_eq!(metas[0].column_indices, vec![0, 2]);
        assert_eq!(metas[1].cursor_id, 5);
        assert_eq!(metas[1].column_indices, vec![1]);
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
        b.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);

        // Now emit a jump referencing the already-resolved label.
        let jump_addr = b.emit_jump_to_label(Opcode::Goto, 0, 0, label, P4::None, 0);
        // p2 should already be patched to 1.
        assert_eq!(b.op_at(jump_addr).unwrap().p2, 1);

        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

        let prog = b.finish().unwrap();
        assert_eq!(prog.len(), 4);
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

    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
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
    fn test_connection_pragma_differential_views_default_query_returns_false() {
        let mut state = pragma::ConnectionPragmaState::default();

        let stmt = parse_pragma("PRAGMA fsqlite_differential_views").expect("parse pragma");
        let out = pragma::apply_connection_pragma(&mut state, &stmt).expect("query pragma");
        assert_eq!(out, pragma::PragmaOutput::Bool(false));
    }

    #[test]
    fn test_connection_pragma_differential_views_set_and_query_across_aliases() {
        let mut state = pragma::ConnectionPragmaState::default();

        let set_on = parse_pragma("PRAGMA fsqlite.differential_views = ON").expect("parse pragma");
        assert_eq!(
            pragma::apply_connection_pragma(&mut state, &set_on).expect("set pragma"),
            pragma::PragmaOutput::Bool(true)
        );
        assert!(state.differential_views.is_enabled());

        let query = parse_pragma("PRAGMA fsqlite_differential_views").expect("parse pragma");
        assert_eq!(
            pragma::apply_connection_pragma(&mut state, &query).expect("query pragma"),
            pragma::PragmaOutput::Bool(true)
        );
    }

    #[test]
    fn test_connection_pragma_differential_views_rejects_non_boolean_values() {
        let mut state = pragma::ConnectionPragmaState::default();

        let stmt = parse_pragma("PRAGMA fsqlite_differential_views = 2").expect("parse pragma");
        assert!(matches!(
            pragma::apply_connection_pragma(&mut state, &stmt),
            Err(FrankenError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn test_connection_pragma_query_only_set_and_query() {
        let mut state = pragma::ConnectionPragmaState::default();

        let query = parse_pragma("PRAGMA query_only").expect("parse pragma");
        assert_eq!(
            pragma::apply_connection_pragma(&mut state, &query).expect("query pragma"),
            pragma::PragmaOutput::Int(0)
        );

        let set_on = parse_pragma("PRAGMA query_only = ON").expect("parse pragma");
        assert_eq!(
            pragma::apply_connection_pragma(&mut state, &set_on).expect("set pragma"),
            pragma::PragmaOutput::Int(1)
        );
        assert!(state.query_only);

        assert_eq!(
            pragma::apply_connection_pragma(&mut state, &query).expect("query pragma"),
            pragma::PragmaOutput::Int(1)
        );
    }

    #[test]
    fn test_connection_pragma_query_only_rejects_non_boolean_values() {
        let mut state = pragma::ConnectionPragmaState::default();

        let stmt = parse_pragma("PRAGMA query_only = 2").expect("parse pragma");
        assert!(matches!(
            pragma::apply_connection_pragma(&mut state, &stmt),
            Err(FrankenError::TypeMismatch { .. })
        ));
    }

    fn apply_sql(state: &mut pragma::ConnectionPragmaState, sql: &str) -> pragma::PragmaOutput {
        let stmt = parse_pragma(sql).expect("parse pragma");
        pragma::apply_connection_pragma(state, &stmt).expect("apply pragma")
    }

    #[test]
    fn test_connection_pragma_boolean_readbacks() {
        // GH #282/#278/#262/#281/#283: set/readback surfaces for boolean pragmas.
        use pragma::PragmaOutput::Bool;
        let mut state = pragma::ConnectionPragmaState::default();

        // trusted_schema and automatic_index default ON; the rest default OFF.
        assert_eq!(apply_sql(&mut state, "PRAGMA trusted_schema"), Bool(true));
        assert_eq!(apply_sql(&mut state, "PRAGMA automatic_index"), Bool(true));
        assert_eq!(
            apply_sql(&mut state, "PRAGMA read_uncommitted"),
            Bool(false)
        );
        assert_eq!(apply_sql(&mut state, "PRAGMA cell_size_check"), Bool(false));
        assert_eq!(
            apply_sql(&mut state, "PRAGMA checkpoint_fullfsync"),
            Bool(false)
        );

        // Set then read back each toggle.
        assert_eq!(
            apply_sql(&mut state, "PRAGMA trusted_schema = OFF"),
            Bool(false)
        );
        assert_eq!(apply_sql(&mut state, "PRAGMA trusted_schema"), Bool(false));
        assert_eq!(
            apply_sql(&mut state, "PRAGMA read_uncommitted = ON"),
            Bool(true)
        );
        assert_eq!(
            apply_sql(&mut state, "PRAGMA cell_size_check = 1"),
            Bool(true)
        );
        assert_eq!(
            apply_sql(&mut state, "PRAGMA checkpoint_fullfsync = TRUE"),
            Bool(true)
        );
        assert_eq!(
            apply_sql(&mut state, "PRAGMA automatic_index = 0"),
            Bool(false)
        );
    }

    #[test]
    fn test_connection_pragma_locking_mode_readback() {
        // GH #273: locking_mode echoes normal/exclusive (lowercased).
        use pragma::PragmaOutput::Text;
        let mut state = pragma::ConnectionPragmaState::default();
        assert_eq!(
            apply_sql(&mut state, "PRAGMA locking_mode"),
            Text("normal".to_owned())
        );
        assert_eq!(
            apply_sql(&mut state, "PRAGMA locking_mode = EXCLUSIVE"),
            Text("exclusive".to_owned())
        );
        assert_eq!(
            apply_sql(&mut state, "PRAGMA locking_mode"),
            Text("exclusive".to_owned())
        );
        // An unrecognized value is ignored; the current mode is echoed unchanged.
        assert_eq!(
            apply_sql(&mut state, "PRAGMA locking_mode = bogus"),
            Text("exclusive".to_owned())
        );
    }

    #[test]
    fn test_connection_pragma_secure_delete_tristate() {
        // GH #277: secure_delete is a tri-state integer (0=OFF, 1=ON, 2=FAST).
        use pragma::PragmaOutput::Int;
        let mut state = pragma::ConnectionPragmaState::default();
        assert_eq!(apply_sql(&mut state, "PRAGMA secure_delete"), Int(0));
        assert_eq!(apply_sql(&mut state, "PRAGMA secure_delete = ON"), Int(1));
        assert_eq!(apply_sql(&mut state, "PRAGMA secure_delete = FAST"), Int(2));
        assert_eq!(apply_sql(&mut state, "PRAGMA secure_delete = OFF"), Int(0));
        assert_eq!(apply_sql(&mut state, "PRAGMA secure_delete = 2"), Int(2));
    }

    #[test]
    fn test_connection_pragma_threads_readback() {
        // GH #279: threads reports the stored limit; a set clamps to 8; a
        // negative argument leaves the limit unchanged.
        use pragma::PragmaOutput::Int;
        let mut state = pragma::ConnectionPragmaState::default();
        assert_eq!(apply_sql(&mut state, "PRAGMA threads"), Int(0));
        assert_eq!(apply_sql(&mut state, "PRAGMA threads = 4"), Int(4));
        assert_eq!(apply_sql(&mut state, "PRAGMA threads"), Int(4));
        // Clamps to the stock maximum of 8.
        assert_eq!(apply_sql(&mut state, "PRAGMA threads = 100"), Int(8));
        // A negative argument leaves the current limit unchanged.
        assert_eq!(apply_sql(&mut state, "PRAGMA threads = -1"), Int(8));
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
        let query = parse_pragma("PRAGMA raptorq_repair_symbols").expect("parse query");
        assert_eq!(
            pragma::apply(&mut mgr, &query).expect("query pragma"),
            pragma::PragmaOutput::Int(i64::from(DEFAULT_RAPTORQ_REPAIR_SYMBOLS))
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
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

    #[cfg(not(target_arch = "wasm32"))]
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

    #[cfg(not(target_arch = "wasm32"))]
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
