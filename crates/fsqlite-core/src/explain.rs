//! EXPLAIN and EXPLAIN QUERY PLAN execution (§12.12, bd-7pxb).
//!
//! EXPLAIN returns VDBE bytecode as a result set with columns:
//!   addr, opcode, p1, p2, p3, p4, p5, comment
//!
//! EXPLAIN QUERY PLAN returns a tree-structured plan with columns:
//!   id, parent, notused, detail

use std::collections::{HashMap, HashSet};

use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_vdbe::VdbeProgram;

// ---------------------------------------------------------------------------
// EXPLAIN result row
// ---------------------------------------------------------------------------

/// A single row from EXPLAIN output (invariant #10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRow {
    /// Instruction address (0-based).
    pub addr: i32,
    /// Opcode name (e.g., "Init", "OpenRead").
    pub opcode: String,
    /// First parameter.
    pub p1: i32,
    /// Second parameter (often a jump target).
    pub p2: i32,
    /// Third parameter.
    pub p3: i32,
    /// Fourth parameter (text representation).
    pub p4: String,
    /// Fifth parameter (flags).
    pub p5: u16,
    /// Comment (auto-generated from opcode semantics).
    pub comment: String,
}

/// Generate EXPLAIN output for a compiled VDBE program.
///
/// Returns one row per instruction with columns:
/// addr, opcode, p1, p2, p3, p4, p5, comment (invariant #10).
#[must_use]
pub fn explain_program(program: &VdbeProgram) -> Vec<ExplainRow> {
    program
        .ops()
        .iter()
        .enumerate()
        .map(|(i, op)| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let addr = i as i32;
            ExplainRow {
                addr,
                opcode: format!("{:?}", op.opcode),
                p1: op.p1,
                p2: op.p2,
                p3: op.p3,
                p4: format!("{:?}", op.p4),
                p5: op.p5,
                comment: opcode_comment(op.opcode, op.p1, op.p2, op.p3),
            }
        })
        .collect()
}

/// Auto-generate a comment for an opcode based on its semantics.
fn opcode_comment(opcode: Opcode, p1: i32, p2: i32, p3: i32) -> String {
    match opcode {
        Opcode::Init => format!("start at {p2}"),
        Opcode::Goto => format!("goto {p2}"),
        Opcode::Halt => {
            if p1 == 0 {
                String::new()
            } else {
                format!("error code {p1}")
            }
        }
        Opcode::Transaction => {
            if p2 == 0 {
                "read transaction".to_owned()
            } else {
                "write transaction".to_owned()
            }
        }
        Opcode::OpenRead | Opcode::OpenWrite => format!("root={p2}"),
        Opcode::Column => format!("r[{p3}]=cursor[{p1}].column[{p2}]"),
        Opcode::ResultRow => format!("output r[{p1}..{p1}+{p2}]"),
        Opcode::Rewind => format!("if eof goto {p2}"),
        Opcode::Next => format!("goto {p2} if more rows"),
        Opcode::Close => format!("close cursor {p1}"),
        _ => String::new(),
    }
}

// ---------------------------------------------------------------------------
// EXPLAIN QUERY PLAN
// ---------------------------------------------------------------------------

/// A single row from EXPLAIN QUERY PLAN output (invariant #11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EqpRow {
    /// Node id in the plan tree.
    pub id: i32,
    /// Parent node id (0 for root nodes).
    pub parent: i32,
    /// Not used (always 0, kept for compatibility).
    pub notused: i32,
    /// Human-readable description of this plan step.
    pub detail: String,
}

/// Generate EXPLAIN QUERY PLAN output for a compiled VDBE program.
///
/// Returns a tree-structured plan with columns: id, parent, notused, detail
/// (invariant #11). The tree structure is expressed via id/parent relationships
/// (invariant #23).
#[must_use]
pub fn explain_query_plan(program: &VdbeProgram) -> Vec<EqpRow> {
    let ops = program.ops();
    let mut rows = Vec::new();
    let mut next_id = 1_i32;
    let mut index_step_by_cursor: HashMap<i32, usize> = HashMap::new();
    let mut index_name_by_cursor: HashMap<i32, String> = HashMap::new();
    let mut index_used = HashSet::new();
    let mut index_with_table_lookup = HashSet::new();
    let mut last_idxrowid_cursor = None;
    let mut saw_sorter = false;

    // Scan for table/index opens and infer index/sorter usage.
    for op in ops {
        match op.opcode {
            Opcode::OpenRead | Opcode::OpenWrite => {
                let scan_type = if op.opcode == Opcode::OpenRead {
                    "SCAN"
                } else {
                    "SEARCH"
                };
                let detail = match &op.p4 {
                    P4::Table(name) => format!("{scan_type} {name}"),
                    P4::Index(name) => format!("{scan_type} INDEX {name}"),
                    _ => format!("{scan_type} {:?}", op.p4),
                };
                rows.push(EqpRow {
                    id: next_id,
                    parent: 0,
                    notused: 0,
                    detail,
                });
                if let P4::Index(name) = &op.p4 {
                    index_step_by_cursor.insert(op.p1, rows.len() - 1);
                    index_name_by_cursor.insert(op.p1, name.clone());
                }
                next_id += 1;
            }
            Opcode::SeekGE
            | Opcode::SeekLE
            | Opcode::SeekGT
            | Opcode::SeekLT
            | Opcode::IdxGE
            | Opcode::IdxGT
            | Opcode::IdxLE
            | Opcode::IdxLT
            | Opcode::Rewind
            | Opcode::Last
            | Opcode::Next
            | Opcode::Prev => {
                if index_name_by_cursor.contains_key(&op.p1) {
                    index_used.insert(op.p1);
                }
            }
            Opcode::IdxRowid => {
                if index_name_by_cursor.contains_key(&op.p1) {
                    index_used.insert(op.p1);
                    last_idxrowid_cursor = Some(op.p1);
                }
            }
            Opcode::SeekRowid => {
                if let Some(index_cursor) = last_idxrowid_cursor {
                    index_with_table_lookup.insert(index_cursor);
                }
                last_idxrowid_cursor = None;
            }
            Opcode::SorterOpen | Opcode::SorterInsert | Opcode::SorterSort => {
                saw_sorter = true;
            }
            _ => {
                last_idxrowid_cursor = None;
            }
        }
    }

    // Annotate index rows as covering vs non-covering.
    for index_cursor in index_used {
        let Some(step_idx) = index_step_by_cursor.get(&index_cursor).copied() else {
            continue;
        };
        let Some(index_name) = index_name_by_cursor.get(&index_cursor) else {
            continue;
        };
        let usage = if index_with_table_lookup.contains(&index_cursor) {
            format!(" USING INDEX {index_name}")
        } else {
            format!(" USING COVERING INDEX {index_name}")
        };
        if !rows[step_idx].detail.contains("USING ") {
            rows[step_idx].detail.push_str(&usage);
        }
    }

    if saw_sorter {
        rows.push(EqpRow {
            id: next_id,
            parent: 0,
            notused: 0,
            detail: "USE TEMP B-TREE FOR ORDER BY".to_owned(),
        });
    }

    // If no table opens found, emit a minimal plan.
    if rows.is_empty() {
        rows.push(EqpRow {
            id: 1,
            parent: 0,
            notused: 0,
            detail: "SCAN CONSTANT ROW".to_owned(),
        });
    }

    rows
}

/// The index access path an aggregate program actually executes.
///
/// bd-2dgf5 / bd-jyyae.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateIndexSeek {
    /// Name of the index the program seeks.
    pub index_name: String,
    /// True when the seek satisfies every aggregate from the index entry alone,
    /// i.e. the program never looks the row up in the table.
    pub covering: bool,
}

/// Read an aggregate's index access path off the *emitted program*.
///
/// bd-2dgf5 / bd-jyyae. `EXPLAIN QUERY PLAN` normally renders from the planner
/// directive, and `planner_select_directive_with_stats` refuses to produce one for
/// an aggregate — so an aggregate that seeks used to print `SCAN t`. The obvious
/// repair, re-checking in the EQP path the same conditions codegen checked, is the
/// bug bd-jyyae is about: the two copies drift, and then EQP confidently describes
/// a plan the program never runs (it once reported `SEARCH` for an `IN (...)` whose
/// program was a bare `Rewind`/`Next` scan).
///
/// So decide nothing here. An index cursor is SEARCHed exactly when a `Seek*`
/// positions it, and the index is COVERING exactly when no `IdxRowid` on that cursor
/// feeds a `SeekRowid` into the table. Both facts are properties of the opcodes.
///
/// Returns `None` unless the program is an aggregate (`AggStep`) driven by exactly
/// one seeked index cursor; every other shape keeps the existing EQP rendering.
#[must_use]
pub fn aggregate_index_seek_facts(program: &VdbeProgram) -> Option<AggregateIndexSeek> {
    let ops = program.ops();
    if !ops.iter().any(|op| op.opcode == Opcode::AggStep) {
        return None;
    }

    let mut index_name_by_cursor: HashMap<i32, &str> = HashMap::new();
    for op in ops {
        if op.opcode == Opcode::OpenRead
            && let P4::Index(name) = &op.p4
        {
            index_name_by_cursor.insert(op.p1, name.as_str());
        }
    }

    let mut seeked: Vec<i32> = Vec::new();
    for op in ops {
        let positions_index = matches!(
            op.opcode,
            Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT
        ) && index_name_by_cursor.contains_key(&op.p1);
        if positions_index && !seeked.contains(&op.p1) {
            seeked.push(op.p1);
        }
    }
    let [cursor] = seeked[..] else {
        return None;
    };

    // A table lookup is an `IdxRowid` on the seeked cursor feeding a `SeekRowid`.
    // Any other opcode in between breaks the pairing, exactly as in
    // `explain_query_plan`'s covering-vs-non-covering annotation.
    let mut rowid_from_seeked_cursor = false;
    let mut table_lookup = false;
    for op in ops {
        match op.opcode {
            Opcode::IdxRowid => rowid_from_seeked_cursor = op.p1 == cursor,
            Opcode::SeekRowid => {
                table_lookup |= rowid_from_seeked_cursor;
                rowid_from_seeked_cursor = false;
            }
            _ => rowid_from_seeked_cursor = false,
        }
    }

    Some(AggregateIndexSeek {
        index_name: (*index_name_by_cursor.get(&cursor)?).to_owned(),
        covering: !table_lookup,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::opcode::{Opcode, P4};
    use fsqlite_vdbe::ProgramBuilder;

    fn build_simple_select_program() -> VdbeProgram {
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();

        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("t".to_owned()), 0);
        b.emit_jump_to_label(Opcode::Rewind, 0, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Next, 0, 4, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Close, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        b.finish().unwrap()
    }

    // === Test 20: EXPLAIN returns VDBE bytecode with correct columns ===
    #[test]
    fn test_explain_returns_bytecode() {
        let prog = build_simple_select_program();
        let rows = explain_program(&prog);

        // Should have rows for each instruction.
        assert!(!rows.is_empty());
        assert_eq!(rows[0].addr, 0);

        // Check column presence (addr, opcode, p1, p2, p3, p4, p5, comment).
        let init_row = &rows[0];
        assert_eq!(init_row.opcode, "Init");
        // p5 is present.
        assert_eq!(init_row.p5, 0);
        // Comment is present.
        assert!(init_row.comment.contains("start at"));
    }

    // === Test 21: EXPLAIN QUERY PLAN returns id, parent, notused, detail ===
    #[test]
    fn test_explain_query_plan_columns() {
        let prog = build_simple_select_program();
        let rows = explain_query_plan(&prog);

        assert!(!rows.is_empty());
        let row = &rows[0];
        // Verify all four columns are present.
        assert!(row.id > 0);
        assert_eq!(row.parent, 0);
        assert_eq!(row.notused, 0);
        assert!(!row.detail.is_empty());
    }

    // === Test 22: EQP detail shows index usage ===
    #[test]
    fn test_explain_query_plan_shows_index() {
        // Build a program that probes an index and then looks up table rows.
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();
        let skip_label = b.emit_label();

        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("t".to_owned()), 0);
        b.emit_op(
            Opcode::OpenRead,
            1,
            3,
            0,
            P4::Index("idx_t_a".to_owned()),
            0,
        );
        b.emit_jump_to_label(Opcode::Rewind, 1, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::IdxRowid, 1, 2, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::SeekRowid, 0, 2, skip_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 0, 1, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
        b.resolve_label(skip_label);
        b.emit_op(Opcode::Next, 1, 5, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        let prog = b.finish().unwrap();
        let rows = explain_query_plan(&prog);

        // Should show index usage.
        let has_index = rows.iter().any(|r| r.detail.contains("USING INDEX"));
        assert!(has_index, "EQP should show index usage, got: {rows:?}");
    }

    // === Test 23: EQP id/parent form correct tree ===
    #[test]
    fn test_explain_query_plan_tree_structure() {
        let prog = build_simple_select_program();
        let rows = explain_query_plan(&prog);

        // Root nodes have parent=0.
        for row in &rows {
            if row.parent == 0 {
                // Root node — id should be positive.
                assert!(row.id > 0);
            } else {
                // Child node — parent should reference an existing node.
                assert!(rows.iter().any(|r| r.id == row.parent));
            }
        }

        // Ids should be unique.
        let mut ids: Vec<i32> = rows.iter().map(|r| r.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), rows.len());
    }

    #[test]
    fn test_explain_query_plan_shows_covering_index() {
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();

        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(
            Opcode::OpenRead,
            1,
            3,
            0,
            P4::Index("idx_t_b".to_owned()),
            0,
        );
        b.emit_jump_to_label(Opcode::Rewind, 1, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::Column, 1, 0, 1, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Next, 1, 4, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        let prog = b.finish().unwrap();
        let rows = explain_query_plan(&prog);
        let has_covering = rows
            .iter()
            .any(|row| row.detail.contains("USING COVERING INDEX idx_t_b"));
        assert!(
            has_covering,
            "EQP should show covering-index usage, got: {rows:?}"
        );
    }

    #[test]
    fn test_explain_query_plan_shows_temp_btree_for_order_by() {
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();

        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("t".to_owned()), 0);
        b.emit_op(Opcode::SorterOpen, 1, 1, 0, P4::Str("+".to_owned()), 0);
        b.emit_jump_to_label(Opcode::Rewind, 0, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 0, 2, P4::None, 0);
        b.emit_op(Opcode::MakeRecord, 2, 1, 3, P4::None, 0);
        b.emit_op(Opcode::SorterInsert, 1, 3, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        let prog = b.finish().unwrap();
        let rows = explain_query_plan(&prog);
        let has_temp_btree = rows
            .iter()
            .any(|row| row.detail == "USE TEMP B-TREE FOR ORDER BY");
        assert!(
            has_temp_btree,
            "EQP should include temp B-tree marker for sorter plans, got: {rows:?}"
        );
    }
}
