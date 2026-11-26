//! EXPLAIN and EXPLAIN QUERY PLAN execution (§12.12, bd-7pxb).
//!
//! EXPLAIN returns VDBE bytecode as a result set with columns:
//!   addr, opcode, p1, p2, p3, p4, p5, comment
//!
//! EXPLAIN QUERY PLAN returns a tree-structured plan with columns:
//!   id, parent, notused, detail

use std::collections::{HashMap, HashSet};

use fsqlite_types::opcode::{Opcode, P4, VdbeOp};
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

/// Temp B-trees / sorters that an emitted VDBE program actually opens for
/// `GROUP BY`, `DISTINCT`, or `ORDER BY` (bd-vyy1q).
///
/// Read purely off the emitted opcodes so `EXPLAIN QUERY PLAN` cannot diverge
/// from execution:
/// - An IN-list probe sorter (`SorterOpen` whose `SorterSort..SorterNext`
///   drain drives `SeekGE`/`SeekGT`/`SeekLE`/`SeekLT` on an `OpenRead` index
///   cursor) is NOT a temp B-tree clause.
/// - A `GROUP BY` sorter has `AggStep` or `AggFinal` inside its drain loop.
/// - Any other top-level sorter (outside `Once`-guarded subquery builds) is an
///   `ORDER BY` sorter.
/// - A top-level `DISTINCT` temp B-tree is an `OpenAutoindex` outside `Once`
///   that is probed with `Found` and populated with `IdxInsert` and never
///   walked (`Rewind`/`Last`/`Next`/`Prev`/`Seek*`), distinguishing it from
///   rowid/index IN-list probe sets and join automatic indexes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProgramTempBtrees {
    /// The program opens a sorter that feeds `GROUP BY` aggregation.
    pub group_by: bool,
    /// The program opens a top-level ephemeral B-tree for `DISTINCT` deduplication.
    pub distinct: bool,
    /// The program opens a sorter for `ORDER BY`.
    pub order_by: bool,
}

impl ProgramTempBtrees {
    /// Canonical SQLite `EXPLAIN QUERY PLAN` detail lines for the active clauses
    /// (`GROUP BY`, then `DISTINCT`, then `ORDER BY`).
    #[must_use]
    pub fn detail_lines(self) -> Vec<&'static str> {
        let mut lines = Vec::new();
        if self.group_by {
            lines.push("USE TEMP B-TREE FOR GROUP BY");
        }
        if self.distinct {
            lines.push("USE TEMP B-TREE FOR DISTINCT");
        }
        if self.order_by {
            lines.push("USE TEMP B-TREE FOR ORDER BY");
        }
        lines
    }
}

/// Classify the temp B-trees and sorters that `program` opens (bd-vyy1q).
#[must_use]
pub fn program_temp_btree_uses(program: &VdbeProgram) -> ProgramTempBtrees {
    let ops = program.ops();
    let mut once_ranges: Vec<(usize, usize)> = Vec::new();
    let mut index_cursors: HashSet<i32> = HashSet::new();
    let mut sorter_opens: Vec<i32> = Vec::new();
    let mut autoindex_opens: Vec<i32> = Vec::new();

    for (addr, op) in ops.iter().enumerate() {
        match op.opcode {
            Opcode::Once => {
                if let Ok(target) = usize::try_from(op.p2)
                    && target > addr
                {
                    once_ranges.push((addr, target));
                }
            }
            Opcode::OpenRead | Opcode::OpenWrite => {
                if matches!(op.p4, P4::Index(_)) {
                    index_cursors.insert(op.p1);
                }
            }
            _ => {}
        }
    }

    let in_once = |addr: usize| -> bool {
        once_ranges
            .iter()
            .any(|&(start, end)| addr > start && addr < end)
    };

    for (addr, op) in ops.iter().enumerate() {
        if in_once(addr) {
            continue;
        }
        match op.opcode {
            Opcode::SorterOpen if !sorter_opens.contains(&op.p1) => {
                sorter_opens.push(op.p1);
            }
            Opcode::OpenAutoindex if !autoindex_opens.contains(&op.p1) => {
                autoindex_opens.push(op.p1);
            }
            _ => {}
        }
    }

    let mut uses = ProgramTempBtrees::default();

    for &cursor in &sorter_opens {
        let Some(sort_addr) = ops
            .iter()
            .position(|op| op.opcode == Opcode::SorterSort && op.p1 == cursor)
        else {
            // Synthetic or incomplete programs with SorterOpen but no SorterSort
            // default to ORDER BY (preserves legacy explain_query_plan behavior).
            uses.order_by = true;
            continue;
        };
        let drain_end = ops
            .iter()
            .enumerate()
            .skip(sort_addr + 1)
            .rfind(|(_, op)| op.opcode == Opcode::SorterNext && op.p1 == cursor)
            .map(|(idx, _)| idx + 1)
            .or_else(|| {
                ops.iter()
                    .enumerate()
                    .skip(sort_addr + 1)
                    .find(|(_, op)| op.opcode == Opcode::Close && op.p1 == cursor)
                    .map(|(idx, _)| idx)
            })
            .unwrap_or(ops.len());
        let drain = &ops[sort_addr..drain_end.min(ops.len())];
        let drives_index_probe = drain.iter().any(|op| {
            matches!(
                op.opcode,
                Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT
            ) && index_cursors.contains(&op.p1)
        });
        if drives_index_probe {
            continue;
        }
        let feeds_aggregates = drain
            .iter()
            .any(|op| matches!(op.opcode, Opcode::AggStep | Opcode::AggFinal));
        if feeds_aggregates {
            uses.group_by = true;
        } else {
            uses.order_by = true;
        }
    }

    if sorter_opens.is_empty()
        && ops.iter().enumerate().any(|(addr, op)| {
            !in_once(addr) && matches!(op.opcode, Opcode::SorterInsert | Opcode::SorterSort)
        })
    {
        uses.order_by = true;
    }

    for &cursor in &autoindex_opens {
        let mut has_found = false;
        let mut has_insert = false;
        let mut walked_or_seeked = false;
        for op in ops {
            if op.p1 != cursor {
                continue;
            }
            match op.opcode {
                Opcode::Found => has_found = true,
                Opcode::IdxInsert => has_insert = true,
                Opcode::Rewind
                | Opcode::Last
                | Opcode::Next
                | Opcode::Prev
                | Opcode::SeekGE
                | Opcode::SeekGT
                | Opcode::SeekLE
                | Opcode::SeekLT => walked_or_seeked = true,
                _ => {}
            }
        }
        if has_found && has_insert && !walked_or_seeked {
            uses.distinct = true;
        }
    }

    uses
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

    // Scan for table/index opens and infer index usage.
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

    for detail in program_temp_btree_uses(program).detail_lines() {
        rows.push(EqpRow {
            id: next_id,
            parent: 0,
            notused: 0,
            detail: detail.to_owned(),
        });
        next_id += 1;
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

/// An index access path that an emitted program actually executes.
///
/// bd-2dgf5 / bd-jyyae.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSeek {
    /// Name of the index the program seeks.
    pub index_name: String,
    /// True when the result can be satisfied from the index entry alone, i.e.
    /// the program never looks the row up in the table.
    pub covering: bool,
}

/// Read an index access path off the *emitted program*.
///
/// bd-2dgf5 / bd-jyyae / GH#381. `EXPLAIN QUERY PLAN` normally renders from the
/// planner directive, but some safe codegen paths have no directive (aggregates
/// and ORDER BY index bypasses). Re-checking the same source-level gates in the
/// EQP path is unsafe because those copies drift: EQP once reported `SEARCH` for
/// an `IN (...)` whose emitted program was a bare `Rewind`/`Next` scan.
///
/// So decide nothing here. An index cursor is SEARCHed exactly when a `Seek*`
/// positions it, and the index is COVERING exactly when no `IdxRowid` on that cursor
/// feeds a `SeekRowid` into the table. Both facts are properties of the opcodes.
///
/// Returns `None` unless exactly one index cursor is positioned by a key seek;
/// ambiguous multi-index shapes keep the existing conservative EQP rendering.
#[must_use]
pub fn program_index_seek_facts(program: &VdbeProgram) -> Option<IndexSeek> {
    let ops = program.ops();

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

    Some(IndexSeek {
        index_name: (*index_name_by_cursor.get(&cursor)?).to_owned(),
        covering: !table_lookup,
    })
}

/// Aggregate-only compatibility wrapper for callers that require that shape.
///
/// The program steps an aggregate, or counts index entries from the b-tree
/// (`IdxCountBefore`, bd-2fb53 / bd-zgbhb / bd-qyd3s), which is a COUNT(*)
/// over the same seek without an `AggStep`.
#[must_use]
pub fn aggregate_index_seek_facts(program: &VdbeProgram) -> Option<IndexSeek> {
    if !program
        .ops()
        .iter()
        .any(|op| matches!(op.opcode, Opcode::AggStep | Opcode::IdxCountBefore))
    {
        return None;
    }
    program_index_seek_facts(program)
}

/// True when the emitted program positions a cursor opened on `index_name`
/// with a range seek or an exact record-key probe (`NoConflict`/`NotFound`).
///
/// bd-jyyae: `EXPLAIN QUERY PLAN` renders the planner directive, but codegen
/// can decline the directive's access path — `IN (SELECT ...)` predicates
/// compile to a full table scan probing an ephemeral membership index, while
/// the directive still claims `SEARCH ... USING INDEX`. The claim is true
/// exactly when a seek opcode positions a cursor opened on that index — a
/// property of the opcodes, not of planner intent. `Rewind`/`Next` on an
/// index cursor is a scan of the index, not a search, and deliberately does
/// not count.
#[must_use]
pub fn program_seeks_named_index(program: &VdbeProgram, index_name: &str) -> bool {
    let ops = program.ops();
    let mut index_cursors: HashSet<i32> = HashSet::new();
    for op in ops {
        if matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite)
            && let P4::Index(name) = &op.p4
            && name == index_name
        {
            index_cursors.insert(op.p1);
        }
    }
    if index_cursors.is_empty() {
        return false;
    }
    ops.iter().any(|op| {
        matches!(
            op.opcode,
            Opcode::SeekGE
                | Opcode::SeekGT
                | Opcode::SeekLE
                | Opcode::SeekLT
                | Opcode::NoConflict
                | Opcode::NotFound
        ) && index_cursors.contains(&op.p1)
    })
}

/// How many leading key columns of `index_name` every range seek on it binds.
///
/// bd-xml0z: `EXPLAIN QUERY PLAN` named only an index's first column even when
/// the program seeks on a two-column key. Read the width off the opcodes: each
/// `Seek*` on a cursor opened on the index takes its key from the `MakeRecord`
/// that last wrote the seek's key register, and the record's fields are the
/// bound key columns up to the rowid floor/ceiling sentinel (`Int64` of
/// `i64::MIN`/`i64::MAX`) that positions the seek before or after equal keys.
///
/// Returns the minimum over all such seeks, or `None` when the index is never
/// range-seeked or a seek key does not come from a `MakeRecord`.
#[must_use]
pub fn program_index_seek_key_width(program: &VdbeProgram, index_name: &str) -> Option<usize> {
    let ops = program.ops();
    let index_cursors: HashSet<i32> = ops
        .iter()
        .filter(|op| {
            matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite)
                && matches!(&op.p4, P4::Index(name) if name == index_name)
        })
        .map(|op| op.p1)
        .collect();
    let mut bound: Option<usize> = None;
    for (seek_addr, op) in ops.iter().enumerate() {
        if !(matches!(
            op.opcode,
            Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT
        ) && index_cursors.contains(&op.p1))
        {
            continue;
        }
        let (record_addr, record) =
            ops[..seek_addr]
                .iter()
                .enumerate()
                .rev()
                .find(|(_, candidate)| {
                    candidate.opcode == Opcode::MakeRecord && candidate.p3 == op.p3
                })?;
        let first_field = record.p1;
        let field_count = usize::try_from(record.p2).ok()?;
        let mut key_columns = 0;
        for field in 0..field_count {
            let register = first_field + i32::try_from(field).ok()?;
            let is_rowid_sentinel = ops[..record_addr]
                .iter()
                .rev()
                .find(|writer| writer.opcode == Opcode::Int64 && writer.p2 == register)
                .is_some_and(|writer| {
                    matches!(writer.p4, P4::Int64(value) if value == i64::MIN || value == i64::MAX)
                });
            if is_rowid_sentinel {
                break;
            }
            key_columns += 1;
        }
        bound = Some(bound.map_or(key_columns, |current| current.min(key_columns)));
    }
    bound
}

/// True when the emitted program probes a cursor opened on `table_name`
/// directly by rowid (`SeekRowid`/`NotExists`).
///
/// bd-jyyae companion to [`program_seeks_named_index`] for the
/// `SEARCH ... USING INTEGER PRIMARY KEY (rowid=?)` directive claim. The
/// probe must target a cursor opened on the named table so that a rowid seek
/// elsewhere in the program (an index's table-lookup leg, a subquery) cannot
/// vouch for a main-table claim.
#[must_use]
pub fn program_seeks_rowid_on_table(program: &VdbeProgram, table_name: &str) -> bool {
    let ops = program.ops();
    let mut table_cursors: HashSet<i32> = HashSet::new();
    for op in ops {
        if matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite)
            && let P4::Table(name) = &op.p4
            && name == table_name
        {
            table_cursors.insert(op.p1);
        }
    }
    if table_cursors.is_empty() {
        return false;
    }
    ops.iter().any(|op| {
        matches!(op.opcode, Opcode::SeekRowid | Opcode::NotExists) && table_cursors.contains(&op.p1)
    })
}

/// True when the program seeks a rowid range on `table_name` (bd-r39kx).
///
/// A cursor opened on the table is positioned with a range seek
/// (`SeekGE`/`SeekGT`/`SeekLE`/`SeekLT` on the table b-tree), and no index is
/// positioned, walked or read: stock reports that as `SEARCH t USING INTEGER
/// PRIMARY KEY (rowid>? ...)`.
#[must_use]
pub fn program_seeks_rowid_range_on_table(program: &VdbeProgram, table_name: &str) -> bool {
    let ops = program.ops();
    let mut table_cursors: HashSet<i32> = HashSet::new();
    let mut index_cursors: HashSet<i32> = HashSet::new();
    for op in ops {
        if matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite) {
            match &op.p4 {
                P4::Table(name) if name == table_name => {
                    table_cursors.insert(op.p1);
                }
                P4::Index(_) => {
                    index_cursors.insert(op.p1);
                }
                _ => {}
            }
        }
    }
    let mut range_seek = false;
    for op in ops {
        let positions = matches!(
            op.opcode,
            Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT
        );
        let walks_or_reads = matches!(
            op.opcode,
            Opcode::Rewind | Opcode::Last | Opcode::Next | Opcode::Prev | Opcode::IdxRowid
        );
        if (positions || walks_or_reads) && index_cursors.contains(&op.p1) {
            return false;
        }
        range_seek |= positions && table_cursors.contains(&op.p1);
    }
    range_seek
}

/// True when the program's ONLY way onto `table_name` is by rowid (GH #415).
///
/// Every access to a cursor opened on the table must be a rowid probe
/// (`SeekRowid`/`NotExists`) and none of those cursors may ever be walked
/// (`Rewind`/`Last`/`Next`/`Prev`), and no named index may be positioned or
/// walked at all — an index-driven program also reaches the table only by
/// `SeekRowid` (its lookup leg), but that is a `SEARCH ... USING INDEX`, not a
/// rowid search. Stricter than [`program_seeks_rowid_on_table`], which a
/// seek-then-scan program (a fast path with a scan fallback) also satisfies.
/// Used to render `SEARCH ... USING INTEGER PRIMARY KEY (rowid=?)` for shapes
/// that carry no planner directive (ORDER BY, aggregates), where the generic
/// bytecode explain would otherwise report every `OpenRead` as a `SCAN`.
#[must_use]
pub fn program_accesses_table_only_by_rowid(program: &VdbeProgram, table_name: &str) -> bool {
    let ops = program.ops();
    let mut table_cursors: HashSet<i32> = HashSet::new();
    let mut index_cursors: HashSet<i32> = HashSet::new();
    for op in ops {
        if matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite) {
            match &op.p4 {
                P4::Table(name) if name == table_name => {
                    table_cursors.insert(op.p1);
                }
                P4::Index(_) => {
                    index_cursors.insert(op.p1);
                }
                _ => {}
            }
        }
    }
    if table_cursors.is_empty() {
        return false;
    }
    let mut seeks = false;
    for op in ops {
        if index_cursors.contains(&op.p1)
            && matches!(
                op.opcode,
                Opcode::SeekGE
                    | Opcode::SeekGT
                    | Opcode::SeekLE
                    | Opcode::SeekLT
                    | Opcode::IdxGE
                    | Opcode::IdxGT
                    | Opcode::IdxLE
                    | Opcode::IdxLT
                    | Opcode::IdxRowid
                    | Opcode::Rewind
                    | Opcode::Last
                    | Opcode::Next
                    | Opcode::Prev
            )
        {
            return false;
        }
        if !table_cursors.contains(&op.p1) {
            continue;
        }
        match op.opcode {
            Opcode::SeekRowid | Opcode::NotExists => seeks = true,
            Opcode::Rewind | Opcode::Last | Opcode::Next | Opcode::Prev => return false,
            _ => {}
        }
    }
    seeks
}

/// True when the emitted program probes a cursor opened on `table_name`
/// directly by a record key via `NoConflict`/`NotFound`.
///
/// This is a WITHOUT ROWID PRIMARY KEY point lookup, whose table b-tree IS
/// keyed by the PK (bd-nd2ju / #377).
/// Companion to [`program_seeks_named_index`] / [`program_seeks_rowid_on_table`]
/// for the `SEARCH ... USING PRIMARY KEY` directive claim. The probe must target
/// a cursor opened on the named table so a record-key seek elsewhere (an index's
/// table-lookup leg on a WITHOUT ROWID secondary index) cannot vouch for a
/// direct-PK claim. Only invoked under a `FullTableScan` directive, where the
/// sole table-cursor record probe a compiled SELECT can carry is the WR-PK
/// lookup (a WR secondary-index equality compiles to an `IndexEquality`
/// directive instead and is verified by the named-index path).
#[must_use]
pub fn program_seeks_without_rowid_pk_on_table(program: &VdbeProgram, table_name: &str) -> bool {
    let ops = program.ops();
    let mut table_cursors: HashSet<i32> = HashSet::new();
    for op in ops {
        if matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite)
            && let P4::Table(name) = &op.p4
            && name == table_name
        {
            table_cursors.insert(op.p1);
        }
    }
    if table_cursors.is_empty() {
        return false;
    }
    ops.iter().any(|op| {
        matches!(op.opcode, Opcode::NoConflict | Opcode::NotFound) && table_cursors.contains(&op.p1)
    })
}

/// bd-r39kx: whether a comparison exits the walk `cursor` starts at
/// `rewind_addr` once the value `reader` loads from the cursor passes an
/// upper bound: right after the `Rewind`, the register `reader` loads (it
/// returns the destination register of a matching opcode) is the LEFT operand
/// (`P3`) of a `Ge` or `Gt` that jumps to the `Rewind`'s exit, as `x < ?` /
/// `x <= ?` compile; only NULL tests may come between.
fn walk_exits_at_upper_bound(
    ops: &[VdbeOp],
    rewind_addr: usize,
    cursor: i32,
    reader: impl Fn(&VdbeOp) -> Option<i32>,
) -> bool {
    let exit = ops[rewind_addr].p2;
    let mut loaded: Option<i32> = None;
    for op in &ops[rewind_addr + 1..] {
        if op.p1 == cursor
            && let Some(register) = reader(op)
        {
            loaded = Some(register);
            continue;
        }
        match op.opcode {
            Opcode::IsNull | Opcode::NotNull => {}
            Opcode::Ge | Opcode::Gt => return loaded == Some(op.p3) && op.p2 == exit,
            _ => return false,
        }
    }
    false
}

/// Opcodes whose `P1` is a cursor that they position or walk.
fn positions_cursor(opcode: Opcode) -> bool {
    matches!(
        opcode,
        Opcode::Rewind
            | Opcode::Last
            | Opcode::Next
            | Opcode::Prev
            | Opcode::SeekGE
            | Opcode::SeekGT
            | Opcode::SeekLE
            | Opcode::SeekLT
            | Opcode::NoConflict
            | Opcode::NotFound
            | Opcode::NotExists
            | Opcode::Count
    )
}

/// bd-r39kx: whether the program walks `table_name` up to a rowid bound.
///
/// The walk starts at its first row and leaves at an upper bound on the rowid
/// (`Rewind`, `Rowid`, `Ge`/`Gt` to the exit), touching no index: stock's
/// `SEARCH t USING INTEGER PRIMARY KEY (rowid<?)`.
#[must_use]
pub fn program_walks_rowid_up_to_bound(program: &VdbeProgram, table_name: &str) -> bool {
    let ops = program.ops();
    let mut table_cursor = None;
    for op in ops {
        if matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite) {
            match &op.p4 {
                P4::Table(name) if name == table_name && table_cursor.is_none() => {
                    table_cursor = Some(op.p1);
                }
                P4::Table(_) | P4::Index(_) => return false,
                _ => {}
            }
        }
    }
    let Some(cursor) = table_cursor else {
        return false;
    };
    let mut rewinds = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| op.p1 == cursor && matches!(op.opcode, Opcode::Rewind | Opcode::Last));
    let (Some((rewind_addr, rewind)), None) = (rewinds.next(), rewinds.next()) else {
        return false;
    };
    rewind.opcode == Opcode::Rewind
        && !ops.iter().any(|op| {
            op.p1 == cursor
                && matches!(
                    op.opcode,
                    Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT
                )
        })
        && walk_exits_at_upper_bound(ops, rewind_addr, cursor, |op| {
            (op.opcode == Opcode::Rowid).then_some(op.p2)
        })
}

/// How a program walks its one index (bd-r39kx).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexWalkKind {
    /// From one end to the other.
    Whole,
    /// From its first entry until the leading key column passes a bound.
    BoundedAbove,
    /// Reads the first or last non-NULL key into a single `min`/`max`.
    Extremum,
    /// Counts its entries (`Count`).
    Count,
}

/// An index a program walks rather than seeks (bd-r39kx).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexWalk {
    /// The index walked.
    pub index_name: String,
    /// No `IdxRowid` on the index feeds a `SeekRowid` into the table.
    pub covering: bool,
    /// How the walk runs.
    pub kind: IndexWalkKind,
}

/// bd-r39kx: the one index a program walks rather than seeks, if any.
///
/// The program opens one index and positions it only with a `Rewind` or
/// `Last` (or counts it with `Count`), and reaches its table, if at all, only
/// by `IdxRowid` + `SeekRowid`. Stock describes
/// that as one row for the source: `SCAN t USING [COVERING] INDEX i`, `SEARCH
/// t USING [COVERING] INDEX i (a<?)` for a walk that stops at an upper bound
/// on the leading column, `SEARCH t USING COVERING INDEX i` for a `min` or
/// `max` read. `None` for any other shape (a seek, two indexes, a table
/// walked on its own), which keeps the existing rendering.
#[must_use]
pub fn program_index_walk(program: &VdbeProgram) -> Option<IndexWalk> {
    let ops = program.ops();
    let mut index: Option<(i32, &str)> = None;
    let mut table_cursor: Option<i32> = None;
    for op in ops {
        if !matches!(op.opcode, Opcode::OpenRead | Opcode::OpenWrite) {
            continue;
        }
        match &op.p4 {
            P4::Index(name) if index.is_none() => index = Some((op.p1, name.as_str())),
            P4::Table(_) if table_cursor.is_none() => table_cursor = Some(op.p1),
            P4::Index(_) | P4::Table(_) => return None,
            _ => {}
        }
    }
    let (cursor, index_name) = index?;
    // The table is only looked up by the rowids the index yields, and the
    // index is never seeked.
    let mut rowid_from_index = false;
    let mut table_lookup = false;
    for op in ops {
        if op.opcode == Opcode::SeekRowid && Some(op.p1) == table_cursor {
            table_lookup |= rowid_from_index;
        } else if positions_cursor(op.opcode)
            && (Some(op.p1) == table_cursor
                || (op.p1 == cursor
                    && !matches!(
                        op.opcode,
                        Opcode::Rewind | Opcode::Last | Opcode::Next | Opcode::Prev | Opcode::Count
                    )))
        {
            return None;
        }
        rowid_from_index = op.opcode == Opcode::IdxRowid && op.p1 == cursor;
    }
    let covering = !table_lookup;
    let positions: Vec<(usize, Opcode)> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| {
            op.p1 == cursor && matches!(op.opcode, Opcode::Rewind | Opcode::Last | Opcode::Count)
        })
        .map(|(addr, op)| (addr, op.opcode))
        .collect();
    let [(position_addr, position)] = positions[..] else {
        return None;
    };
    let extremum_steps = ops
        .iter()
        .filter(|op| op.opcode == Opcode::AggStep)
        .map(|op| match &op.p4 {
            P4::FuncName(name) | P4::FuncNameCollated(name, _) => {
                name.eq_ignore_ascii_case("min") || name.eq_ignore_ascii_case("max")
            }
            _ => false,
        })
        .collect::<Vec<_>>();
    let kind = if position == Opcode::Count {
        IndexWalkKind::Count
    } else if extremum_steps == [true] {
        IndexWalkKind::Extremum
    } else if position == Opcode::Rewind
        && walk_exits_at_upper_bound(ops, position_addr, cursor, |op| {
            (op.opcode == Opcode::Column && op.p2 == 0).then_some(op.p3)
        })
    {
        IndexWalkKind::BoundedAbove
    } else {
        IndexWalkKind::Whole
    };
    Some(IndexWalk {
        index_name: index_name.to_owned(),
        covering,
        kind,
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

    /// The truthful equality-probe shape: `SeekGE` positions the index
    /// cursor, `IdxRowid`/`SeekRowid` fetch the table row (the program
    /// `EXPLAIN SELECT * FROM t WHERE k = 2` actually emits).
    fn build_index_seek_program() -> VdbeProgram {
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
            P4::Index("idx_t_k".to_owned()),
            0,
        );
        b.emit_jump_to_label(Opcode::SeekGE, 1, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::IdxRowid, 1, 9, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::SeekRowid, 0, 9, skip_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 1, 2, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);
        b.resolve_label(skip_label);
        b.emit_op(Opcode::Next, 1, 5, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Close, 1, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        b.finish().unwrap()
    }

    /// The bd-jyyae divergence shape: full `Rewind`/`Next` scan of the table
    /// with an ephemeral membership probe (`OpenAutoindex` + `Found`) — the
    /// program `SELECT * FROM t WHERE k IN (SELECT id FROM r)` actually
    /// emits. No cursor is ever opened on `idx_t_k`, and no seek runs.
    fn build_in_subquery_scan_program() -> VdbeProgram {
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();
        let skip_label = b.emit_label();

        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("t".to_owned()), 0);
        b.emit_jump_to_label(Opcode::Rewind, 0, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 1, 5, P4::None, 0);
        b.emit_op(Opcode::MakeRecord, 5, 1, 7, P4::None, 0);
        b.emit_jump_to_label(Opcode::Found, 12294, 0, skip_label, P4::None, 0);
        b.emit_op(Opcode::Rowid, 0, 1, 0, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
        b.resolve_label(skip_label);
        b.emit_op(Opcode::Next, 0, 4, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Close, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        b.finish().unwrap()
    }

    // === bd-jyyae: directive seek claims must be provable from the opcodes ===

    #[test]
    fn test_program_seeks_named_index_true_for_seek_shape() {
        let prog = build_index_seek_program();
        assert!(program_seeks_named_index(&prog, "idx_t_k"));
        assert_eq!(
            program_index_seek_facts(&prog),
            Some(IndexSeek {
                index_name: "idx_t_k".to_owned(),
                covering: false,
            })
        );
        assert_eq!(
            aggregate_index_seek_facts(&prog),
            None,
            "a non-aggregate seek must not satisfy the aggregate-only wrapper"
        );
    }

    /// A COUNT(*) counted from the b-tree has no AggStep: the seek it
    /// positions with is still an aggregate's covering index search.
    #[test]
    fn test_aggregate_index_seek_facts_accepts_a_counted_index_run() {
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 0, 2, 0, P4::None, 0);
        b.emit_op(
            Opcode::OpenRead,
            1,
            3,
            0,
            P4::Index("idx_t_k".to_owned()),
            0,
        );
        b.emit_jump_to_label(Opcode::SeekGE, 1, 4, done_label, P4::None, 0);
        b.emit_op(Opcode::IdxCountBefore, 1, 2, 4, P4::None, 1);
        b.resolve_label(done_label);
        b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Close, 1, 0, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);
        let prog = b.finish().unwrap();
        assert_eq!(
            aggregate_index_seek_facts(&prog),
            Some(IndexSeek {
                index_name: "idx_t_k".to_owned(),
                covering: true,
            })
        );
    }

    #[test]
    fn test_program_seeks_named_index_false_for_in_subquery_scan() {
        let prog = build_in_subquery_scan_program();
        assert!(
            !program_seeks_named_index(&prog, "idx_t_k"),
            "a Rewind/Next membership-probe scan must not vouch for a SEARCH claim"
        );
    }

    #[test]
    fn test_program_seeks_named_index_false_for_index_only_scan() {
        // An index cursor driven only by Rewind/Next is an index SCAN, not a
        // SEARCH: the equality/range claim must not be vouched for.
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
            P4::Index("idx_t_k".to_owned()),
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
        assert!(!program_seeks_named_index(&prog, "idx_t_k"));
        assert_eq!(
            program_index_seek_facts(&prog),
            None,
            "Rewind/Next is an index scan, not a key search"
        );
    }

    #[test]
    fn test_program_seeks_rowid_on_table_true_for_rowid_probe() {
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();

        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("t".to_owned()), 0);
        b.emit_jump_to_label(Opcode::SeekRowid, 0, 1, done_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 1, 2, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);

        let prog = b.finish().unwrap();
        assert!(program_seeks_rowid_on_table(&prog, "t"));
        assert!(
            !program_seeks_rowid_on_table(&prog, "other"),
            "a rowid probe on t must not vouch for a claim about another table"
        );
    }

    #[test]
    fn test_program_seeks_rowid_on_table_false_for_scan() {
        let prog = build_simple_select_program();
        assert!(!program_seeks_rowid_on_table(&prog, "t"));
    }

    #[test]
    fn test_index_table_lookup_leg_does_not_vouch_for_table_rowid_claim() {
        // The SeekRowid in an index seek's table-lookup leg targets the table
        // cursor, so it legitimately proves a rowid probe on that cursor —
        // but the *index* claim for an unrelated index must still fail.
        let prog = build_index_seek_program();
        assert!(!program_seeks_named_index(&prog, "idx_other"));
    }

    /// GH #415: the rowid-only access check that renders the directive-less
    /// `SEARCH ... USING INTEGER PRIMARY KEY (rowid=?)` row.
    #[test]
    fn test_program_accesses_table_only_by_rowid() {
        // A probe-set loop: an autoindex is walked, the table is only seeked.
        let mut b = ProgramBuilder::new();
        let end_label = b.emit_label();
        let done_label = b.emit_label();
        let next_label = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
        b.emit_op(Opcode::Transaction, 0, 0, 0, P4::None, 0);
        b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("t".to_owned()), 0);
        b.emit_op(Opcode::OpenAutoindex, 1, 1, 0, P4::None, 0);
        b.emit_jump_to_label(Opcode::Rewind, 1, 0, done_label, P4::None, 0);
        b.emit_op(Opcode::Column, 1, 0, 3, P4::None, 0);
        b.emit_jump_to_label(Opcode::SeekRowid, 0, 3, next_label, P4::None, 0);
        b.emit_op(Opcode::Column, 0, 1, 2, P4::None, 0);
        b.emit_op(Opcode::ResultRow, 2, 1, 0, P4::None, 0);
        b.resolve_label(next_label);
        b.emit_op(Opcode::Next, 1, 5, 0, P4::None, 0);
        b.resolve_label(done_label);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end_label);
        let probe_loop = b.finish().unwrap();
        assert!(program_accesses_table_only_by_rowid(&probe_loop, "t"));
        assert!(!program_accesses_table_only_by_rowid(&probe_loop, "other"));

        // An index-driven program reaches the table only by SeekRowid too, but
        // that is `SEARCH ... USING INDEX`, never a rowid search.
        assert!(!program_accesses_table_only_by_rowid(
            &build_index_seek_program(),
            "t"
        ));
        // A full scan walks the table.
        assert!(!program_accesses_table_only_by_rowid(
            &build_simple_select_program(),
            "t"
        ));
    }

    #[test]
    fn test_program_temp_btree_uses_distinguishes_probe_group_distinct_and_order() {
        // 1. IN-list probe sorter only (SorterSort..SeekGE index..SorterNext): no temp B-tree.
        let probe_only = {
            let mut b = ProgramBuilder::new();
            let end_label = b.emit_label();
            let done_label = b.emit_label();
            let next_probe = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
            b.emit_op(
                Opcode::OpenRead,
                2,
                3,
                0,
                P4::Index("idx_c".to_owned()),
                0,
            );
            b.emit_op(Opcode::SorterOpen, 3, 1, 0, P4::Str("+".to_owned()), 0);
            b.emit_op(Opcode::SorterInsert, 3, 4, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::SorterSort, 3, 0, done_label, P4::None, 0);
            let loop_top = b.current_addr();
            b.emit_jump_to_label(Opcode::SeekGE, 2, 5, next_probe, P4::None, 0);
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            b.resolve_label(next_probe);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(Opcode::SorterNext, 3, loop_top as i32, 0, P4::None, 0);
            b.resolve_label(done_label);
            b.emit_op(Opcode::Close, 3, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Close, 2, 0, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end_label);
            b.finish().unwrap()
        };
        assert_eq!(
            program_temp_btree_uses(&probe_only),
            ProgramTempBtrees::default()
        );

        // 2. IN-list probe sorter + ORDER BY sorter + DISTINCT autoindex:
        //    must report DISTINCT and ORDER BY in canonical order, ignoring probe sorter.
        let probe_distinct_order = {
            let mut b = ProgramBuilder::new();
            let end_label = b.emit_label();
            let probe_done = b.emit_label();
            let next_probe = b.emit_label();
            let sort_done = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
            b.emit_op(Opcode::SorterOpen, 1, 1, 0, P4::Str("+".to_owned()), 0);
            b.emit_op(
                Opcode::OpenAutoindex,
                2,
                1,
                0,
                P4::Str("BINARY".to_owned()),
                0,
            );
            b.emit_op(
                Opcode::OpenRead,
                3,
                4,
                0,
                P4::Index("idx_c".to_owned()),
                0,
            );
            b.emit_op(Opcode::SorterOpen, 4, 1, 0, P4::Str("+".to_owned()), 0);
            b.emit_jump_to_label(Opcode::SorterSort, 4, 0, probe_done, P4::None, 0);
            let probe_top = b.current_addr();
            b.emit_jump_to_label(Opcode::SeekGE, 3, 5, next_probe, P4::None, 0);
            b.emit_jump_to_label(Opcode::Found, 2, 6, next_probe, P4::None, 0);
            b.emit_op(Opcode::IdxInsert, 2, 6, 0, P4::None, 0);
            b.emit_op(Opcode::SorterInsert, 1, 7, 0, P4::None, 0);
            b.resolve_label(next_probe);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(Opcode::SorterNext, 4, probe_top as i32, 0, P4::None, 0);
            b.resolve_label(probe_done);
            b.emit_jump_to_label(Opcode::SorterSort, 1, 0, sort_done, P4::None, 0);
            let sort_top = b.current_addr();
            b.emit_op(Opcode::ResultRow, 1, 1, 0, P4::None, 0);
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(Opcode::SorterNext, 1, sort_top as i32, 0, P4::None, 0);
            b.resolve_label(sort_done);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end_label);
            b.finish().unwrap()
        };
        let uses = program_temp_btree_uses(&probe_distinct_order);
        assert_eq!(
            uses,
            ProgramTempBtrees {
                group_by: false,
                distinct: true,
                order_by: true,
            }
        );
        assert_eq!(
            uses.detail_lines(),
            vec!["USE TEMP B-TREE FOR DISTINCT", "USE TEMP B-TREE FOR ORDER BY"]
        );

        // 3. GROUP BY sorter (SorterSort..AggFinal/AggStep..SorterNext):
        //    must report GROUP BY, NOT ORDER BY.
        let group_by_prog = {
            let mut b = ProgramBuilder::new();
            let end_label = b.emit_label();
            let scan_done = b.emit_label();
            let sort_done = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
            b.emit_op(Opcode::SorterOpen, 1, 1, 0, P4::Str("+".to_owned()), 0);
            b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("m".to_owned()), 0);
            b.emit_jump_to_label(Opcode::Rewind, 0, 0, scan_done, P4::None, 0);
            b.emit_op(Opcode::SorterInsert, 1, 4, 0, P4::None, 0);
            b.resolve_label(scan_done);
            b.emit_jump_to_label(Opcode::SorterSort, 1, 0, sort_done, P4::None, 0);
            let sort_top = b.current_addr();
            b.emit_op(
                Opcode::AggFinal,
                7,
                0,
                0,
                P4::FuncName("COUNT".to_owned()),
                0,
            );
            b.emit_op(
                Opcode::AggStep,
                0,
                0,
                7,
                P4::FuncName("COUNT".to_owned()),
                0,
            );
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            b.emit_op(Opcode::SorterNext, 1, sort_top as i32, 0, P4::None, 0);
            b.resolve_label(sort_done);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end_label);
            b.finish().unwrap()
        };
        let eqp_details: Vec<String> = explain_query_plan(&group_by_prog)
            .into_iter()
            .map(|r| r.detail)
            .collect();
        assert_eq!(eqp_details, vec!["SCAN m", "USE TEMP B-TREE FOR GROUP BY"]);

        // 4. Negative controls: Once-guarded subquery autoindex + walked rowid IN autoindex
        //    must NOT be misclassified as DISTINCT or ORDER BY.
        let neg_autoindexes = {
            let mut b = ProgramBuilder::new();
            let end_label = b.emit_label();
            let once_done = b.emit_label();
            let walk_done = b.emit_label();
            let skip = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end_label, P4::None, 0);
            b.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Table("m".to_owned()), 0);
            // Once-guarded IN-subquery membership autoindex + internal sorter.
            b.emit_jump_to_label(Opcode::Once, 0, 0, once_done, P4::None, 0);
            b.emit_op(Opcode::OpenAutoindex, 12290, 1, 0, P4::None, 1);
            b.emit_op(Opcode::SorterOpen, 12291, 1, 0, P4::Str("+".to_owned()), 0);
            b.emit_op(Opcode::IdxInsert, 12290, 3, 0, P4::None, 0);
            b.resolve_label(once_done);
            b.emit_jump_to_label(Opcode::Found, 12290, 3, skip, P4::None, 1);
            // Walked rowid IN-list probe autoindex (deduped via Found+IdxInsert then Rewind).
            b.emit_op(Opcode::OpenAutoindex, 1, 1, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Found, 1, 4, skip, P4::None, 0);
            b.emit_op(Opcode::IdxInsert, 1, 4, 0, P4::None, 0);
            b.emit_jump_to_label(Opcode::Rewind, 1, 0, walk_done, P4::None, 0);
            b.emit_op(Opcode::Next, 1, 0, 0, P4::None, 0);
            b.resolve_label(skip);
            b.resolve_label(walk_done);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end_label);
            b.finish().unwrap()
        };
        assert_eq!(
            program_temp_btree_uses(&neg_autoindexes),
            ProgramTempBtrees::default()
        );
    }
}
