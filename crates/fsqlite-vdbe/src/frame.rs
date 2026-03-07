// bd-3lj3: §12.8 CREATE TRIGGER — Heap Frame Stack + SQLITE_MAX_TRIGGER_DEPTH
//
// Heap-allocated frame stack for nested trigger/subprogram execution.
// MUST NOT use Rust call-stack recursion. Each trigger invocation pushes a
// VdbeFrame onto a Vec<VdbeFrame>; depth is enforced deterministically via
// SQLITE_MAX_TRIGGER_DEPTH (default 1000). A Cx-budgeted memory ceiling caps
// total register-file bytes across frames.

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

/// Default maximum trigger nesting depth (matches C SQLite `SQLITE_MAX_TRIGGER_DEPTH`).
pub const SQLITE_MAX_TRIGGER_DEPTH: usize = 1000;

/// Result of evaluating a RAISE function inside a trigger body.
///
/// The executor inspects this after each trigger step to determine
/// whether to proceed, skip the DML, or unwind the transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaiseResult {
    /// RAISE(IGNORE): in a BEFORE trigger, skip the DML operation entirely.
    Ignore,
    /// RAISE(ROLLBACK, msg): roll back the entire transaction.
    Rollback(String),
    /// RAISE(ABORT, msg): abort the current statement (undo statement changes).
    Abort(String),
    /// RAISE(FAIL, msg): abort but keep prior statement changes.
    Fail(String),
}

/// Register range in the parent frame for OLD/NEW pseudo-table access.
///
/// In VDBE terms, OLD and NEW reference specific register ranges in the
/// parent frame, passed to the subprogram via `OP_Program` operands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PseudoTableMapping {
    /// First register of the OLD row (before the DML change).
    /// `None` for INSERT triggers (no OLD row).
    pub old_base: Option<i32>,
    /// First register of the NEW row (after the DML change).
    /// `None` for DELETE triggers (no NEW row).
    pub new_base: Option<i32>,
    /// Number of columns in the mapped row.
    pub num_columns: i32,
}

/// A single VDBE execution frame on the heap-allocated frame stack.
///
/// Pushed when entering a trigger body, popped on exit or error.
/// Contains the full interpreter state needed to resume the parent.
#[derive(Debug, Clone)]
pub struct VdbeFrame {
    /// Saved program counter of the parent (instruction to resume after return).
    pub saved_pc: i32,
    /// The register file snapshot for this frame.
    pub registers: Vec<SqliteValue>,
    /// Number of cursors active in this frame.
    pub n_cursor: i32,
    /// Index of the subprogram being executed (references a VdbeProgram table).
    pub subprogram_idx: i32,
    /// The trigger name that caused this frame (for recursion detection).
    pub trigger_name: String,
    /// OLD/NEW pseudo-table register mappings for the parent frame.
    pub pseudo_tables: Option<PseudoTableMapping>,
    /// Whether a RAISE result is pending in this frame.
    pub raise_result: Option<RaiseResult>,
}

impl VdbeFrame {
    /// Estimated memory usage of this frame in bytes.
    ///
    /// Accounts for the register file (8 bytes per `SqliteValue` slot baseline,
    /// plus actual heap data for Text/Blob), trigger name, and fixed overhead.
    #[allow(clippy::cast_possible_truncation)]
    pub fn estimated_memory(&self) -> usize {
        let base_overhead = std::mem::size_of::<Self>();
        let reg_mem: usize = self
            .registers
            .iter()
            .map(|v| {
                std::mem::size_of::<SqliteValue>()
                    + match v {
                        SqliteValue::Text(s) => s.len(),
                        SqliteValue::Blob(b) => b.len(),
                        _ => 0,
                    }
            })
            .sum();
        let name_mem = self.trigger_name.len();
        base_overhead + reg_mem + name_mem
    }
}

/// Heap-allocated frame stack for nested trigger execution.
///
/// Enforces two independent limits:
/// 1. `max_depth`: maximum nesting depth (default `SQLITE_MAX_TRIGGER_DEPTH`).
/// 2. `cx_memory_budget`: maximum total register-file bytes across all frames.
///
/// The `recursive_triggers` flag controls whether self-recursive triggers are
/// allowed (OFF by default, matching `PRAGMA recursive_triggers`).
#[derive(Debug)]
pub struct FrameStack {
    /// The stack of active frames (index 0 = outermost trigger), along with their memory cost at push time.
    frames: Vec<(VdbeFrame, usize)>,
    /// Maximum nesting depth.
    max_depth: usize,
    /// Cx memory budget (total bytes allowed across all frames).
    cx_memory_budget: usize,
    /// Current tracked memory across all active frames.
    current_memory: usize,
    /// Whether self-recursive triggers are allowed.
    recursive_triggers: bool,
}

impl FrameStack {
    /// Create a new frame stack with the given depth limit and memory budget.
    pub fn new(max_depth: usize, cx_memory_budget: usize) -> Self {
        Self {
            frames: Vec::new(),
            max_depth,
            cx_memory_budget,
            current_memory: 0,
            recursive_triggers: false,
        }
    }

    /// Create a frame stack with default settings.
    pub fn with_defaults() -> Self {
        // Default Cx budget: 64 MiB (generous for typical workloads).
        Self::new(SQLITE_MAX_TRIGGER_DEPTH, 64 * 1024 * 1024)
    }

    /// Current nesting depth (number of frames on the stack).
    pub fn depth(&self) -> usize {
        self.frames.len()
    }

    /// Whether the stack is empty.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Set the recursive triggers flag (PRAGMA recursive_triggers).
    pub fn set_recursive_triggers(&mut self, enabled: bool) {
        self.recursive_triggers = enabled;
    }

    /// Whether recursive triggers are enabled.
    pub fn recursive_triggers(&self) -> bool {
        self.recursive_triggers
    }

    /// Current tracked memory usage across all frames.
    pub fn current_memory(&self) -> usize {
        self.current_memory
    }

    /// Push a new frame onto the stack.
    ///
    /// Checks the depth limit, self-recursion policy, and Cx memory budget
    /// BEFORE allocating. Returns `Err` if any limit is exceeded.
    pub fn push_frame(&mut self, frame: VdbeFrame) -> Result<(), FrankenError> {
        // Check depth limit.
        if self.frames.len() >= self.max_depth {
            return Err(FrankenError::Internal(format!(
                "trigger depth limit exceeded (max {})",
                self.max_depth
            )));
        }

        // Check recursive trigger policy.
        if !self.recursive_triggers {
            let is_self_recursive = self
                .frames
                .iter()
                .any(|(f, _)| f.trigger_name == frame.trigger_name);
            if is_self_recursive {
                return Err(FrankenError::Internal(
                    "recursive triggers are disabled (PRAGMA recursive_triggers = OFF)".to_owned(),
                ));
            }
        }

        // Check Cx memory budget BEFORE allocating.
        let frame_mem = frame.estimated_memory();
        let new_total = self.current_memory + frame_mem;
        if new_total > self.cx_memory_budget {
            return Err(FrankenError::OutOfMemory);
        }

        // All checks passed — push the frame.
        self.current_memory = new_total;
        self.frames.push((frame, frame_mem));
        Ok(())
    }

    /// Pop the top frame from the stack.
    ///
    /// Returns the popped frame, or `None` if the stack is empty.
    pub fn pop_frame(&mut self) -> Option<VdbeFrame> {
        let (frame, pushed_mem) = self.frames.pop()?;
        self.current_memory = self.current_memory.saturating_sub(pushed_mem);
        Some(frame)
    }

    /// Peek at the top frame without removing it.
    pub fn top(&self) -> Option<&VdbeFrame> {
        self.frames.last().map(|(f, _)| f)
    }

    /// Apply a mutation to the top frame while preserving the stack's memory-budget invariant.
    ///
    /// Returns `Ok(false)` if the stack is empty. If the mutation would exceed
    /// the configured memory budget, the frame is left unchanged and
    /// `Err(FrankenError::OutOfMemory)` is returned.
    pub fn update_top(
        &mut self,
        update: impl FnOnce(&mut VdbeFrame),
    ) -> Result<bool, FrankenError> {
        let Some((top, recorded_mem)) = self.frames.last_mut() else {
            return Ok(false);
        };

        let mut candidate = top.clone();
        update(&mut candidate);

        let new_mem = candidate.estimated_memory();
        let base_memory = self.current_memory.saturating_sub(*recorded_mem);
        let new_total = base_memory.saturating_add(new_mem);
        if new_total > self.cx_memory_budget {
            return Err(FrankenError::OutOfMemory);
        }

        *top = candidate;
        *recorded_mem = new_mem;
        self.current_memory = new_total;
        Ok(true)
    }

    /// Unwind all frames (cleanup on error). Returns the unwound frames
    /// in pop order (innermost first).
    pub fn unwind_all(&mut self) -> Vec<VdbeFrame> {
        let mut unwound = Vec::with_capacity(self.frames.len());
        while let Some(frame) = self.pop_frame() {
            unwound.push(frame);
        }
        unwound
    }

    /// Unwind frames until the frame at `target_index` is popped.
    ///
    /// `target_index` is a 0-based index into the stack where 0 is the
    /// outermost frame. Returns the unwound frames in pop order (innermost
    /// first). Missing targets are rejected instead of silently unwinding the
    /// full stack.
    pub fn unwind_to_index(&mut self, target_index: usize) -> Result<Vec<VdbeFrame>, FrankenError> {
        if target_index >= self.frames.len() {
            return Err(FrankenError::Internal(format!(
                "no frame at index {target_index}"
            )));
        }

        let mut unwound = Vec::with_capacity(self.frames.len() - target_index);
        while self.frames.len() > target_index {
            if let Some(frame) = self.pop_frame() {
                unwound.push(frame);
            }
        }
        Ok(unwound)
    }
}

/// Helper to create a `VdbeFrame` for testing or simple trigger entry.
pub fn make_frame(
    saved_pc: i32,
    num_registers: usize,
    n_cursor: i32,
    subprogram_idx: i32,
    trigger_name: impl Into<String>,
) -> VdbeFrame {
    VdbeFrame {
        saved_pc,
        registers: vec![SqliteValue::Null; num_registers],
        n_cursor,
        subprogram_idx,
        trigger_name: trigger_name.into(),
        pseudo_tables: None,
        raise_result: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test 1: test_trigger_depth_limit_1000 ──────────────────────────
    #[test]
    fn test_trigger_depth_limit_1000() {
        // Recursive AFTER INSERT trigger with recursive_triggers=ON fires
        // exactly up to depth 1000, then SQLITE_LIMIT on the 1001st push.
        let mut stack = FrameStack::new(SQLITE_MAX_TRIGGER_DEPTH, usize::MAX);
        stack.set_recursive_triggers(true);

        // Push 1000 frames (max depth).
        for i in 0..SQLITE_MAX_TRIGGER_DEPTH {
            let frame = make_frame(
                i32::try_from(i).unwrap(),
                4, // small register file
                0,
                1,
                "trg_recursive",
            );
            stack
                .push_frame(frame)
                .unwrap_or_else(|e| unreachable!("push at depth {i} should succeed: {e}"));
        }
        assert_eq!(stack.depth(), SQLITE_MAX_TRIGGER_DEPTH);

        // The 1001st push must fail.
        let overflow_frame = make_frame(1000, 4, 0, 1, "trg_recursive");
        let err = stack
            .push_frame(overflow_frame)
            .expect_err("push at depth 1001 must fail");
        assert!(
            err.to_string().contains("trigger depth limit exceeded"),
            "expected depth limit error, got: {err}"
        );
    }

    // ── Test 2: test_trigger_no_stack_overflow_at_max_depth ─────────────
    #[test]
    fn test_trigger_no_stack_overflow_at_max_depth() {
        // Reaching depth 1000 does NOT cause Rust stack overflow; returns
        // error code, does not crash. The heap-allocated Vec<VdbeFrame>
        // guarantees no stack growth per trigger.
        let mut stack = FrameStack::new(SQLITE_MAX_TRIGGER_DEPTH, usize::MAX);
        stack.set_recursive_triggers(true);

        for i in 0..SQLITE_MAX_TRIGGER_DEPTH {
            let frame = make_frame(
                i32::try_from(i).unwrap(),
                8, // moderate register file
                2,
                1,
                "trg_deep",
            );
            stack.push_frame(frame).unwrap();
        }

        // Verify all 1000 frames are on the heap.
        assert_eq!(stack.depth(), 1000);

        // Pop all frames cleanly.
        let unwound = stack.unwind_all();
        assert_eq!(unwound.len(), 1000);
        assert!(stack.is_empty());
        assert_eq!(stack.current_memory(), 0);
    }

    // ── Test 3: test_trigger_cx_memory_budget_enforced ──────────────────
    #[test]
    fn test_trigger_cx_memory_budget_enforced() {
        // Large register files per trigger invocation; low Cx budget stops
        // nesting with SQLITE_NOMEM well below depth 1000.

        // Each frame with 1000 registers ~ 1000 * size_of::<SqliteValue>() + overhead.
        // Set budget low enough that it runs out before depth 1000.
        let frame_size_estimate = {
            let sample = make_frame(0, 1000, 0, 1, "trg_mem");
            sample.estimated_memory()
        };

        // Allow exactly 10 frames worth of memory.
        let budget = frame_size_estimate * 10;
        let mut stack = FrameStack::new(SQLITE_MAX_TRIGGER_DEPTH, budget);
        stack.set_recursive_triggers(true);

        let mut pushed = 0;
        for i in 0..SQLITE_MAX_TRIGGER_DEPTH {
            let frame = make_frame(i32::try_from(i).unwrap(), 1000, 0, 1, "trg_mem");
            match stack.push_frame(frame) {
                Ok(()) => pushed += 1,
                Err(e) => {
                    // Must be OutOfMemory, not depth limit.
                    assert!(
                        matches!(e, FrankenError::OutOfMemory),
                        "expected OutOfMemory, got: {e}"
                    );
                    break;
                }
            }
        }

        // Budget should stop us well below 1000.
        assert!(
            pushed < SQLITE_MAX_TRIGGER_DEPTH,
            "expected budget to stop nesting, but pushed all {pushed}"
        );
        assert!(
            pushed <= 10,
            "expected ~10 frames allowed, but got {pushed}"
        );
        assert!(
            pushed >= 10,
            "expected at least 10 frames, but got {pushed}"
        );
    }

    // ── Test 4: test_trigger_recursive_off_prevents_self_fire ───────────
    #[test]
    fn test_trigger_recursive_off_prevents_self_fire() {
        // With recursive_triggers=OFF (default), self-recursive trigger fires
        // exactly once (the initial push succeeds; the second is blocked).
        let mut stack = FrameStack::with_defaults();
        assert!(!stack.recursive_triggers());

        // First push of "trg_self" succeeds.
        let frame1 = make_frame(0, 4, 0, 1, "trg_self");
        stack.push_frame(frame1).expect("first push should succeed");
        assert_eq!(stack.depth(), 1);

        // Second push of the SAME trigger name must fail (self-recursion).
        let frame2 = make_frame(1, 4, 0, 1, "trg_self");
        let err = stack
            .push_frame(frame2)
            .expect_err("self-recursive push must fail when recursive_triggers=OFF");
        assert!(
            err.to_string().contains("recursive triggers are disabled"),
            "expected recursion-disabled error, got: {err}"
        );

        // A DIFFERENT trigger name succeeds (not self-recursive).
        let frame3 = make_frame(2, 4, 0, 2, "trg_other");
        stack
            .push_frame(frame3)
            .expect("different trigger should succeed");
        assert_eq!(stack.depth(), 2);
    }

    // ── Test 5: test_trigger_frame_stack_cleanup_on_error ───────────────
    #[test]
    fn test_trigger_frame_stack_cleanup_on_error() {
        // Chain of 5 triggers where the 5th raises RAISE(ABORT); all 5 frames
        // properly cleaned up, transaction aborted, no memory leaks.
        let mut stack = FrameStack::with_defaults();
        stack.set_recursive_triggers(true);

        // Push 5 different trigger frames.
        for i in 0..5 {
            let frame = make_frame(i, 16, 1, i, format!("trg_chain_{i}"));
            stack.push_frame(frame).unwrap();
        }
        assert_eq!(stack.depth(), 5);

        // The 5th trigger (top of stack) raises RAISE(ABORT, "constraint failed").
        stack
            .update_top(|frame| {
                frame.raise_result = Some(RaiseResult::Abort("constraint failed".to_owned()));
            })
            .expect("raise_result update should succeed");

        // Verify the raise result is accessible.
        let top = stack.top().unwrap();
        assert!(matches!(
            &top.raise_result,
            Some(RaiseResult::Abort(msg)) if msg == "constraint failed"
        ));

        // Unwind all frames (simulates error cleanup).
        let unwound = stack.unwind_all();
        assert_eq!(unwound.len(), 5);

        // Stack is fully cleaned up.
        assert!(stack.is_empty());
        assert_eq!(stack.depth(), 0);
        assert_eq!(stack.current_memory(), 0);

        // Verify the frames came out innermost-first.
        assert_eq!(unwound[0].trigger_name, "trg_chain_4");
        assert_eq!(unwound[4].trigger_name, "trg_chain_0");

        // The first unwound frame should have the RAISE result.
        assert!(unwound[0].raise_result.is_some());
    }

    // ── Test 6: test_trigger_old_new_pseudo_tables ──────────────────────
    #[test]
    fn test_trigger_old_new_pseudo_tables() {
        // BEFORE UPDATE trigger reads OLD.col and NEW.col correctly;
        // modifying NEW changes the value in the register file.
        let mut stack = FrameStack::with_defaults();

        // Parent frame: registers contain the row being updated.
        // OLD values at registers [1..=3], NEW values at [4..=6].
        let mut parent_frame = make_frame(100, 7, 1, 0, "trg_update");
        parent_frame.registers[1] = SqliteValue::Integer(10); // OLD.a
        parent_frame.registers[2] = SqliteValue::Text("hello".to_owned()); // OLD.b
        parent_frame.registers[3] = SqliteValue::Float(std::f64::consts::PI); // OLD.c
        parent_frame.registers[4] = SqliteValue::Integer(20); // NEW.a
        parent_frame.registers[5] = SqliteValue::Text("world".to_owned()); // NEW.b
        parent_frame.registers[6] = SqliteValue::Float(2.72); // NEW.c
        parent_frame.pseudo_tables = Some(PseudoTableMapping {
            old_base: Some(1),
            new_base: Some(4),
            num_columns: 3,
        });
        stack.push_frame(parent_frame).unwrap();

        // Trigger body frame.
        let trigger_frame = make_frame(0, 4, 0, 1, "trg_before_update");
        stack.push_frame(trigger_frame).unwrap();

        // Access OLD/NEW from the parent frame (index 0).
        let parent = &stack.frames[0].0;
        let mapping = parent.pseudo_tables.as_ref().unwrap();

        // Read OLD.a (register at old_base + 0).
        let old_base = usize::try_from(mapping.old_base.expect("old_base must exist"))
            .expect("old_base must be non-negative");
        assert!(matches!(
            parent.registers[old_base],
            SqliteValue::Integer(10)
        ));

        // Read NEW.b (register at new_base + 1).
        let new_base = usize::try_from(mapping.new_base.expect("new_base must exist"))
            .expect("new_base must be non-negative");
        assert!(matches!(&parent.registers[new_base + 1], SqliteValue::Text(s) if s == "world"));

        // Modify NEW.a in parent frame (simulates trigger body setting NEW value).
        let parent_mut = &mut stack.frames[0].0;
        let new_base_mut = usize::try_from(
            parent_mut
                .pseudo_tables
                .as_ref()
                .unwrap()
                .new_base
                .expect("new_base must exist"),
        )
        .expect("new_base must be non-negative");
        parent_mut.registers[new_base_mut] = SqliteValue::Integer(999);

        // Verify the modification persists.
        let parent = &stack.frames[0].0;
        let new_base = usize::try_from(
            parent
                .pseudo_tables
                .as_ref()
                .unwrap()
                .new_base
                .expect("new_base must exist"),
        )
        .expect("new_base must be non-negative");
        assert!(matches!(
            parent.registers[new_base],
            SqliteValue::Integer(999)
        ));

        // Verify OLD is unchanged.
        let old_base = usize::try_from(
            parent
                .pseudo_tables
                .as_ref()
                .unwrap()
                .old_base
                .expect("old_base must exist"),
        )
        .expect("old_base must be non-negative");
        assert!(matches!(
            parent.registers[old_base],
            SqliteValue::Integer(10)
        ));

        // Clean up.
        stack.unwind_all();
        assert!(stack.is_empty());
    }

    // ── Additional coverage ─────────────────────────────────────────────

    #[test]
    fn test_update_top_rejects_budget_bypass() {
        let base_frame = make_frame(0, 1, 0, 0, "trg_budget");
        let budget = base_frame.estimated_memory() + 16;
        let mut stack = FrameStack::new(SQLITE_MAX_TRIGGER_DEPTH, budget);
        stack.push_frame(base_frame.clone()).unwrap();

        let before_memory = stack.current_memory();
        let before_frame = stack.top().unwrap().clone();

        let err = stack
            .update_top(|frame| {
                frame.registers[0] = SqliteValue::Text("x".repeat(512));
            })
            .expect_err("budget-busting top mutation must fail");
        assert!(matches!(err, FrankenError::OutOfMemory));
        assert_eq!(stack.current_memory(), before_memory);
        assert_eq!(
            stack.top().unwrap().estimated_memory(),
            before_frame.estimated_memory()
        );
        assert!(matches!(
            stack.top().unwrap().registers[0],
            SqliteValue::Null
        ));
    }

    #[test]
    fn test_update_top_refreshes_memory_accounting() {
        let base_frame = make_frame(0, 1, 0, 0, "trg_budget_ok");
        let mut stack = FrameStack::new(SQLITE_MAX_TRIGGER_DEPTH, usize::MAX);
        stack.push_frame(base_frame).unwrap();

        stack
            .update_top(|frame| {
                frame.trigger_name.push_str("_expanded");
                frame.registers[0] = SqliteValue::Blob(vec![7; 128]);
            })
            .expect("top mutation should succeed");

        let top = stack.top().unwrap();
        assert_eq!(stack.current_memory(), top.estimated_memory());
        assert_eq!(top.trigger_name, "trg_budget_ok_expanded");
        assert!(matches!(&top.registers[0], SqliteValue::Blob(bytes) if bytes.len() == 128));
    }

    #[test]
    fn test_raise_result_variants() {
        let ignore = RaiseResult::Ignore;
        let rollback = RaiseResult::Rollback("oops".to_owned());
        let abort = RaiseResult::Abort("error".to_owned());
        let fail = RaiseResult::Fail("bad".to_owned());

        assert_eq!(ignore, RaiseResult::Ignore);
        assert_ne!(rollback, abort);
        assert!(matches!(fail, RaiseResult::Fail(msg) if msg == "bad"));
    }

    #[test]
    fn test_pseudo_table_mapping_insert_trigger() {
        // INSERT trigger: no OLD row, only NEW.
        let mapping = PseudoTableMapping {
            old_base: None,
            new_base: Some(1),
            num_columns: 3,
        };
        assert!(mapping.old_base.is_none());
        assert_eq!(mapping.new_base, Some(1));
    }

    #[test]
    fn test_pseudo_table_mapping_delete_trigger() {
        // DELETE trigger: OLD row only, no NEW.
        let mapping = PseudoTableMapping {
            old_base: Some(1),
            new_base: None,
            num_columns: 3,
        };
        assert_eq!(mapping.old_base, Some(1));
        assert!(mapping.new_base.is_none());
    }

    #[test]
    fn test_unwind_to_index() {
        let mut stack = FrameStack::with_defaults();
        stack.set_recursive_triggers(true);

        for i in 0..5 {
            let frame = make_frame(i, 4, 0, i, format!("trg_{i}"));
            stack.push_frame(frame).unwrap();
        }

        // Unwind to index 2 — should pop trg_4, trg_3, trg_2.
        let unwound = stack.unwind_to_index(2).unwrap();
        assert_eq!(unwound.len(), 3);
        assert_eq!(unwound[0].trigger_name, "trg_4");
        assert_eq!(unwound[1].trigger_name, "trg_3");
        assert_eq!(unwound[2].trigger_name, "trg_2");
        assert_eq!(stack.depth(), 2); // trg_0 and trg_1 remain
    }

    #[test]
    fn test_unwind_to_index_missing_target_errors() {
        let mut stack = FrameStack::with_defaults();
        stack.push_frame(make_frame(0, 4, 0, 0, "trg_0")).unwrap();
        stack.push_frame(make_frame(1, 4, 0, 1, "trg_1")).unwrap();

        let err = stack
            .unwind_to_index(2)
            .expect_err("missing target index must not unwind the full stack");
        assert!(err.to_string().contains("no frame at index 2"));
        assert_eq!(stack.depth(), 2);
    }

    #[test]
    fn test_pop_empty_stack() {
        let mut stack = FrameStack::with_defaults();
        assert!(stack.pop_frame().is_none());
        assert!(stack.top().is_none());
    }

    #[test]
    fn test_frame_estimated_memory_with_text_blob() {
        let mut frame = make_frame(0, 3, 0, 0, "trg_mem_test");
        frame.registers[0] = SqliteValue::Text("a".repeat(1024));
        frame.registers[1] = SqliteValue::Blob(vec![0u8; 2048]);
        frame.registers[2] = SqliteValue::Integer(42);

        let mem = frame.estimated_memory();
        // Should account for the 1024-byte string + 2048-byte blob.
        assert!(mem > 3000, "memory estimate too low: {mem}");
    }
}
