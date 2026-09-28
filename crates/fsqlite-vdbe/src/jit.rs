//! JIT closure compilation for hot prepared VDBE programs (V3.1).
//!
//! Instead of runtime code generation, this "JIT" compiles recognized VDBE
//! program patterns into specialized templates that the engine can execute
//! without walking the full interpreter dispatch loop.
//!
//! Currently supports:
//! - constant single-row `ResultRow` programs
//! - simple sequential parameterized `INSERT` with no indexes/triggers/FKs
//!   and no constraint/RETURNING machinery
//!
//! Everything else falls back to the interpreter.

use fsqlite_types::{
    opcode::{Opcode, P4, VdbeOp},
    record::PrecomputedRecordHeader,
    value::SqliteValue,
};

/// A compiled program template that bypasses the VDBE interpreter.
#[derive(Debug, Clone)]
pub enum CompiledProgram {
    /// Constant single-row result.
    ConstantResultRow(ConstantResultRowTemplate),
    /// Compiled simple sequential INSERT.
    SimpleInsert(SimpleInsertTemplate),
    /// Compiled rowid-equality SELECT: `SELECT cols FROM t WHERE rowid = ?`.
    RowidLookupSelect(RowidLookupSelectTemplate),
    /// Compiled full table scan SELECT: `SELECT cols FROM t`.
    FullScanSelect(FullScanSelectTemplate),
}

/// Template for a compiled rowid-equality SELECT.
///
/// Captures the static program shape: cursor, root page, column extraction
/// indices, and which register/binding holds the rowid key.
#[derive(Debug, Clone)]
pub struct RowidLookupSelectTemplate {
    /// Cursor ID for the source table.
    pub cursor_id: i32,
    /// Root page of the source table.
    pub root_page: i32,
    /// Column indices to extract (from Column opcodes: p2 values).
    pub column_indices: Vec<i32>,
    /// Source of the rowid lookup key.
    pub rowid_source: InsertValueSource,
}

/// Template for a compiled full table scan SELECT.
///
/// Captures: cursor, root page, column extraction indices. Emits one
/// result row per table row (Rewind → Column... → ResultRow → Next loop).
#[derive(Debug, Clone)]
pub struct FullScanSelectTemplate {
    pub cursor_id: i32,
    pub root_page: i32,
    pub column_indices: Vec<i32>,
}

/// One compiled column source for a simple INSERT record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertValueSource {
    /// Read the value from the bound parameter slice at the given zero-based slot.
    Binding(usize),
    /// Embed a constant value directly in the compiled template.
    Constant(SqliteValue),
}

/// Compiled record assembly strategy for a simple INSERT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledRecordBuilder {
    /// Generic SQLite record assembly. This preserves semantics for dynamic
    /// schemas and values whose serial-type shape is not known at compile time.
    Generic,
    /// Use the schema-derived `MakeRecord` header captured at compile time.
    PrecomputedHeader(PrecomputedRecordHeader),
}

impl CompiledRecordBuilder {
    fn from_make_record_p4(p4: &P4, num_cols: i32) -> Self {
        let Ok(num_cols) = usize::try_from(num_cols) else {
            return Self::Generic;
        };
        match p4 {
            P4::PrecomputedHeader(header) if header.column_count() == num_cols => {
                Self::PrecomputedHeader(header.clone())
            }
            _ => Self::Generic,
        }
    }
}

/// Template for a constant single-row result program.
#[derive(Debug, Clone)]
pub struct ConstantResultRowTemplate {
    /// Values emitted by the single `ResultRow`.
    pub values: Vec<SqliteValue>,
}

/// Template for a compiled simple INSERT program.
///
/// Captures the static properties of the INSERT at compile time:
/// cursor ID, root page, column count, value sources, affinity, and MakeRecord metadata.
#[derive(Debug, Clone)]
pub struct SimpleInsertTemplate {
    /// Cursor ID for the target table.
    pub cursor_id: i32,
    /// Root page number of the target table.
    pub root_page: i32,
    /// Number of columns in the record.
    pub num_cols: i32,
    /// Compiled value sources used to materialize each record column.
    pub value_sources: Vec<InsertValueSource>,
    /// Optional affinity string applied to the bound column values.
    pub affinity: Option<String>,
    /// Original `MakeRecord` metadata used for row-image semantics.
    pub record_p4: P4,
    /// Specialized record assembly strategy derived from `record_p4`.
    pub record_builder: CompiledRecordBuilder,
    /// Insert flags (P5 from the Insert opcode).
    pub insert_flags: u16,
}

/// Attempt to compile a VDBE program into a specialized template.
pub fn try_compile_program(ops: &[VdbeOp]) -> Option<CompiledProgram> {
    try_compile_constant_result_row(ops)
        .map(CompiledProgram::ConstantResultRow)
        .or_else(|| try_compile_insert(ops).map(CompiledProgram::SimpleInsert))
        .or_else(|| try_compile_rowid_lookup_select(ops).map(CompiledProgram::RowidLookupSelect))
        .or_else(|| try_compile_full_scan_select(ops).map(CompiledProgram::FullScanSelect))
}

fn ensure_const_register(registers: &mut Vec<Option<SqliteValue>>, reg: i32, value: SqliteValue) {
    if reg <= 0 {
        return;
    }
    let idx = usize::try_from(reg).unwrap_or(0);
    if registers.len() <= idx {
        registers.resize(idx + 1, None);
    }
    registers[idx] = Some(value);
}

fn set_register_source(
    sources: &mut Vec<Option<InsertValueSource>>,
    reg: i32,
    source: Option<InsertValueSource>,
) {
    if reg <= 0 {
        return;
    }
    let idx = usize::try_from(reg).unwrap_or(0);
    if sources.len() <= idx {
        sources.resize(idx + 1, None);
    }
    sources[idx] = source;
}

fn register_source(sources: &[Option<InsertValueSource>], reg: i32) -> Option<InsertValueSource> {
    let idx = usize::try_from(reg).ok()?;
    sources.get(idx).cloned().flatten()
}

fn try_compile_constant_result_row(ops: &[VdbeOp]) -> Option<ConstantResultRowTemplate> {
    let mut registers: Vec<Option<SqliteValue>> = Vec::new();
    let mut result_row = None;

    for op in ops {
        match op.opcode {
            Opcode::Init
            | Opcode::Goto
            | Opcode::Noop
            | Opcode::Transaction
            | Opcode::Halt
            | Opcode::Close => {}
            Opcode::Integer => {
                ensure_const_register(
                    &mut registers,
                    op.p2,
                    SqliteValue::Integer(i64::from(op.p1)),
                );
            }
            Opcode::Int64 => {
                let value = match &op.p4 {
                    P4::Int64(value) => *value,
                    P4::Int(value) => i64::from(*value),
                    _ => return None,
                };
                ensure_const_register(&mut registers, op.p2, SqliteValue::Integer(value));
            }
            Opcode::Real => {
                let value = match &op.p4 {
                    P4::Real(value) => *value,
                    _ => return None,
                };
                ensure_const_register(&mut registers, op.p2, SqliteValue::Float(value));
            }
            Opcode::String | Opcode::String8 => {
                let value = match &op.p4 {
                    P4::Str(value) => SqliteValue::Text(value.clone().into()),
                    _ => return None,
                };
                ensure_const_register(&mut registers, op.p2, value);
            }
            Opcode::Blob => {
                let value = match &op.p4 {
                    P4::Blob(value) => SqliteValue::Blob(value.clone().into()),
                    _ => return None,
                };
                ensure_const_register(&mut registers, op.p2, value);
            }
            Opcode::Null => {
                let end_reg = if op.p3 > 0 { op.p3 } else { op.p2 };
                if end_reg < op.p2 {
                    return None;
                }
                for reg in op.p2..=end_reg {
                    ensure_const_register(&mut registers, reg, SqliteValue::Null);
                }
            }
            Opcode::ResultRow => {
                if result_row.is_some() {
                    return None;
                }
                let count = usize::try_from(op.p2).ok()?;
                let mut values = Vec::with_capacity(count);
                for offset in 0..count {
                    let reg = op.p1.checked_add(i32::try_from(offset).ok()?)?;
                    let value = registers
                        .get(usize::try_from(reg).ok()?)
                        .and_then(|value| value.clone())?;
                    values.push(value);
                }
                result_row = Some(ConstantResultRowTemplate { values });
            }
            _ => return None,
        }
    }

    result_row
}

fn find_value_sources(
    ops: &[VdbeOp],
    first_col_reg: i32,
    num_cols: i32,
) -> Option<Vec<InsertValueSource>> {
    if first_col_reg <= 0 || num_cols <= 0 {
        return None;
    }

    let num_cols_usize = usize::try_from(num_cols).ok()?;
    let mut sources_by_reg = Vec::new();

    for op in ops {
        match op.opcode {
            Opcode::Variable => {
                let binding_index = usize::try_from(op.p1).ok()?.checked_sub(1)?;
                set_register_source(
                    &mut sources_by_reg,
                    op.p2,
                    Some(InsertValueSource::Binding(binding_index)),
                );
            }
            Opcode::Copy => {
                for offset in 0..=op.p3 {
                    let source = register_source(&sources_by_reg, op.p1 + offset);
                    set_register_source(&mut sources_by_reg, op.p2 + offset, source);
                }
            }
            Opcode::SCopy => {
                let source = register_source(&sources_by_reg, op.p1);
                set_register_source(&mut sources_by_reg, op.p2, source);
            }
            Opcode::Integer => {
                set_register_source(
                    &mut sources_by_reg,
                    op.p2,
                    Some(InsertValueSource::Constant(SqliteValue::Integer(
                        i64::from(op.p1),
                    ))),
                );
            }
            Opcode::Int64 => {
                let value = match &op.p4 {
                    P4::Int64(value) => *value,
                    P4::Int(value) => i64::from(*value),
                    _ => return None,
                };
                set_register_source(
                    &mut sources_by_reg,
                    op.p2,
                    Some(InsertValueSource::Constant(SqliteValue::Integer(value))),
                );
            }
            Opcode::Real => {
                let value = match &op.p4 {
                    P4::Real(value) => *value,
                    _ => return None,
                };
                set_register_source(
                    &mut sources_by_reg,
                    op.p2,
                    Some(InsertValueSource::Constant(SqliteValue::Float(value))),
                );
            }
            Opcode::String | Opcode::String8 => {
                let value = match &op.p4 {
                    P4::Str(value) => SqliteValue::Text(value.clone().into()),
                    _ => return None,
                };
                set_register_source(
                    &mut sources_by_reg,
                    op.p2,
                    Some(InsertValueSource::Constant(value)),
                );
            }
            Opcode::Blob => {
                let value = match &op.p4 {
                    P4::Blob(value) => SqliteValue::Blob(value.clone().into()),
                    _ => return None,
                };
                set_register_source(
                    &mut sources_by_reg,
                    op.p2,
                    Some(InsertValueSource::Constant(value)),
                );
            }
            Opcode::Null => {
                let end_reg = if op.p3 > 0 { op.p3 } else { op.p2 };
                if end_reg < op.p2 {
                    return None;
                }
                for reg in op.p2..=end_reg {
                    set_register_source(
                        &mut sources_by_reg,
                        reg,
                        Some(InsertValueSource::Constant(SqliteValue::Null)),
                    );
                }
            }
            _ => {}
        }
    }

    (0..num_cols_usize)
        .map(|offset| register_source(&sources_by_reg, first_col_reg + i32::try_from(offset).ok()?))
        .collect()
}

fn find_affinity(ops: &[VdbeOp], first_col_reg: i32, num_cols: i32) -> Option<String> {
    ops.iter().find_map(|op| {
        if op.opcode != Opcode::Affinity || op.p1 != first_col_reg || op.p2 != num_cols {
            return None;
        }
        match &op.p4 {
            P4::Affinity(affinity) => Some(affinity.clone()),
            _ => None,
        }
    })
}

/// Attempt to compile a VDBE program into a specialized closure.
///
/// Returns `None` if the program doesn't match any known compilable pattern.
/// This is called after the hot-threshold is reached (N executions).
pub fn try_compile_insert(ops: &[VdbeOp]) -> Option<SimpleInsertTemplate> {
    // Scan for the pattern:
    //   ... (setup opcodes: Init, Transaction, OpenWrite, etc.)
    //   NewRowid(cursor, r_rowid)  OR  FusedAppendInsert(cursor, r_start, n_cols)
    //   MakeRecord(r_start, n_cols, r_record)  [if not fused]
    //   Insert(cursor, r_record, r_rowid)      [if not fused]
    //   ... (Close, Halt)
    //
    // Guard: no IdxInsert (= no secondary indexes), no triggers, no FKs.

    // Only support the plain no-index/no-constraint/no-RETURNING path.
    for op in ops {
        match op.opcode {
            Opcode::Init
            | Opcode::Transaction
            | Opcode::OpenWrite
            | Opcode::Variable
            | Opcode::Copy
            | Opcode::SCopy
            | Opcode::Null
            | Opcode::Integer
            | Opcode::Int64
            | Opcode::Real
            | Opcode::String
            | Opcode::String8
            | Opcode::Blob
            | Opcode::NewRowid
            | Opcode::Affinity
            | Opcode::MakeRecord
            | Opcode::Insert
            | Opcode::FusedAppendInsert
            | Opcode::Close
            | Opcode::Halt
            | Opcode::Goto
            | Opcode::Noop => {}
            _ => return None,
        }
    }

    // Look for FusedAppendInsert (already optimized by peephole)
    if let Some(fused) = ops.iter().find(|op| op.opcode == Opcode::FusedAppendInsert) {
        let value_sources = find_value_sources(ops, fused.p2, fused.p3)?;
        return Some(SimpleInsertTemplate {
            cursor_id: fused.p1,
            root_page: find_root_page(ops, fused.p1)?,
            num_cols: fused.p3,
            value_sources,
            affinity: find_affinity(ops, fused.p2, fused.p3),
            record_p4: fused.p4.clone(),
            record_builder: CompiledRecordBuilder::from_make_record_p4(&fused.p4, fused.p3),
            insert_flags: fused.p5,
        });
    }

    // Look for unfused NewRowid + MakeRecord + Insert
    let new_rowid = ops.iter().find(|op| op.opcode == Opcode::NewRowid)?;
    let make_record = ops.iter().find(|op| op.opcode == Opcode::MakeRecord)?;
    let insert = ops.iter().find(|op| op.opcode == Opcode::Insert)?;

    // Verify consistency
    if new_rowid.p1 != insert.p1 {
        return None; // Different cursors
    }
    if make_record.p3 != insert.p2 {
        return None; // Record register mismatch
    }
    let oe_flag = insert.p5 & 0x0F;
    if oe_flag != 2 {
        return None; // Not ABORT mode
    }

    let value_sources = find_value_sources(ops, make_record.p1, make_record.p2)?;

    Some(SimpleInsertTemplate {
        cursor_id: new_rowid.p1,
        root_page: find_root_page(ops, new_rowid.p1)?,
        num_cols: make_record.p2,
        value_sources,
        affinity: find_affinity(ops, make_record.p1, make_record.p2),
        record_p4: make_record.p4.clone(),
        record_builder: CompiledRecordBuilder::from_make_record_p4(&make_record.p4, make_record.p2),
        insert_flags: insert.p5,
    })
}

/// Find the root page for a cursor by scanning OpenWrite opcodes.
///
/// Templates open their cursor by root alone, so a cursor on a non-main
/// database (p3: TEMP or attached) is not compiled; the interpreter routes it.
fn find_root_page(ops: &[VdbeOp], cursor_id: i32) -> Option<i32> {
    ops.iter()
        .find(|op| {
            (op.opcode == Opcode::OpenWrite || op.opcode == Opcode::FusedOpenWriteLast)
                && op.p1 == cursor_id
        })
        .filter(|op| op.p3 == 0)
        .map(|op| op.p2)
}

fn find_read_root_page(ops: &[VdbeOp], cursor_id: i32) -> Option<i32> {
    ops.iter()
        .find(|op| op.opcode == Opcode::OpenRead && op.p1 == cursor_id)
        .filter(|op| op.p3 == 0)
        .map(|op| op.p2)
}

fn try_compile_rowid_lookup_select(ops: &[VdbeOp]) -> Option<RowidLookupSelectTemplate> {
    for op in ops {
        match op.opcode {
            Opcode::Init
            | Opcode::Transaction
            | Opcode::Goto
            | Opcode::Noop
            | Opcode::OpenRead
            | Opcode::Close
            | Opcode::Halt
            | Opcode::Integer
            | Opcode::Int64
            | Opcode::Real
            | Opcode::String
            | Opcode::String8
            | Opcode::Null
            | Opcode::Variable
            | Opcode::Copy
            | Opcode::SCopy
            | Opcode::Move
            | Opcode::SeekRowid
            | Opcode::NotExists
            | Opcode::Column
            | Opcode::ResultRow
            | Opcode::Affinity
            | Opcode::ReadCookie
            | Opcode::TableLock
            | Opcode::RealAffinity => {}
            _ => return None,
        }
    }

    let seek = ops
        .iter()
        .find(|op| op.opcode == Opcode::SeekRowid || op.opcode == Opcode::NotExists)?;
    let cursor_id = seek.p1;
    let rowid_reg = seek.p3;
    let root_page = find_read_root_page(ops, cursor_id)?;

    let rowid_source = resolve_register_source(ops, rowid_reg)?;

    let result_row = ops.iter().find(|op| op.opcode == Opcode::ResultRow)?;
    let first_result_reg = result_row.p1;
    let result_count = usize::try_from(result_row.p2).ok()?;

    let mut column_indices = Vec::with_capacity(result_count);
    for offset in 0..i32::try_from(result_count).ok()? {
        let dest_reg = first_result_reg + offset;
        let col_op = ops
            .iter()
            .find(|op| op.opcode == Opcode::Column && op.p1 == cursor_id && op.p3 == dest_reg)?;
        column_indices.push(col_op.p2);
    }

    Some(RowidLookupSelectTemplate {
        cursor_id,
        root_page,
        column_indices,
        rowid_source,
    })
}

fn resolve_register_source(ops: &[VdbeOp], reg: i32) -> Option<InsertValueSource> {
    for op in ops.iter().rev() {
        match op.opcode {
            Opcode::Variable if op.p2 == reg => {
                let binding_index = usize::try_from(op.p1).ok()?.checked_sub(1)?;
                return Some(InsertValueSource::Binding(binding_index));
            }
            Opcode::Integer if op.p2 == reg => {
                return Some(InsertValueSource::Constant(SqliteValue::Integer(
                    i64::from(op.p1),
                )));
            }
            Opcode::Int64 if op.p2 == reg => {
                let value = match &op.p4 {
                    P4::Int64(v) => *v,
                    P4::Int(v) => i64::from(*v),
                    _ => return None,
                };
                return Some(InsertValueSource::Constant(SqliteValue::Integer(value)));
            }
            Opcode::SCopy if op.p2 == reg => {
                return resolve_register_source(ops, op.p1);
            }
            Opcode::Copy if op.p2 == reg && op.p3 == 0 => {
                return resolve_register_source(ops, op.p1);
            }
            _ => {}
        }
    }
    None
}

fn try_compile_full_scan_select(ops: &[VdbeOp]) -> Option<FullScanSelectTemplate> {
    for op in ops {
        match op.opcode {
            Opcode::Init
            | Opcode::Transaction
            | Opcode::Goto
            | Opcode::Noop
            | Opcode::OpenRead
            | Opcode::Close
            | Opcode::Halt
            | Opcode::Rewind
            | Opcode::Next
            | Opcode::Column
            | Opcode::ResultRow
            | Opcode::Affinity
            | Opcode::ReadCookie
            | Opcode::TableLock
            | Opcode::RealAffinity
            | Opcode::Integer
            | Opcode::Null => {}
            _ => return None,
        }
    }

    let rewind = ops.iter().find(|op| op.opcode == Opcode::Rewind)?;
    let cursor_id = rewind.p1;
    let root_page = find_read_root_page(ops, cursor_id)?;

    ops.iter()
        .find(|op| op.opcode == Opcode::Next && op.p1 == cursor_id)?;

    let result_row = ops.iter().find(|op| op.opcode == Opcode::ResultRow)?;
    let first_result_reg = result_row.p1;
    let result_count = usize::try_from(result_row.p2).ok()?;

    let mut column_indices = Vec::with_capacity(result_count);
    for offset in 0..i32::try_from(result_count).ok()? {
        let dest_reg = first_result_reg + offset;
        let col_op = ops
            .iter()
            .find(|op| op.opcode == Opcode::Column && op.p1 == cursor_id && op.p3 == dest_reg)?;
        column_indices.push(col_op.p2);
    }

    Some(FullScanSelectTemplate {
        cursor_id,
        root_page,
        column_indices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_compile_insert_accepts_copy_remapped_bindings() {
        let ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 13,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Transaction,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenWrite,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::Table("jit_hot_query".to_owned()),
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Variable,
                p1: 1,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Variable,
                p1: 2,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Copy,
                p1: 2,
                p2: 5,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Copy,
                p1: 3,
                p2: 6,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::NewRowid,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Affinity,
                p1: 5,
                p2: 2,
                p3: 0,
                p4: P4::Affinity("DB".to_owned()),
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::MakeRecord,
                p1: 5,
                p2: 2,
                p3: 4,
                p4: P4::Affinity("DB".to_owned()),
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Insert,
                p1: 0,
                p2: 4,
                p3: 1,
                p4: P4::Table("jit_hot_query".to_owned()),
                p5: 2,
            },
            VdbeOp {
                opcode: Opcode::Close,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];

        let template = try_compile_insert(&ops).expect("copy-remapped insert should compile");
        assert_eq!(template.cursor_id, 0);
        assert_eq!(template.root_page, 2);
        assert_eq!(template.num_cols, 2);
        assert_eq!(
            template.value_sources,
            vec![InsertValueSource::Binding(0), InsertValueSource::Binding(1)]
        );
        assert_eq!(template.affinity.as_deref(), Some("DB"));
        assert_eq!(template.record_p4, P4::Affinity("DB".to_owned()));
        assert_eq!(template.record_builder, CompiledRecordBuilder::Generic);
        assert_eq!(template.insert_flags, 2);
    }

    #[test]
    fn try_compile_insert_keeps_precomputed_record_builder() {
        let header = PrecomputedRecordHeader::new(&[
            fsqlite_types::record::PrecomputedSerialTypeKind::NullPlaceholder,
            fsqlite_types::record::PrecomputedSerialTypeKind::IntegerOrNull,
            fsqlite_types::record::PrecomputedSerialTypeKind::RealOrNull,
        ]);
        let ops = vec![
            VdbeOp {
                opcode: Opcode::OpenWrite,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::Table("jit_hot_query".to_owned()),
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Variable,
                p1: 1,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Variable,
                p1: 2,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Null,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::NewRowid,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::MakeRecord,
                p1: 1,
                p2: 3,
                p3: 4,
                p4: P4::PrecomputedHeader(header.clone()),
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Insert,
                p1: 0,
                p2: 4,
                p3: 1,
                p4: P4::Table("jit_hot_query".to_owned()),
                p5: 2,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];

        let template = try_compile_insert(&ops).expect("precomputed INSERT should compile");
        assert_eq!(
            template.record_builder,
            CompiledRecordBuilder::PrecomputedHeader(header)
        );
    }

    #[test]
    fn try_compile_rowid_lookup_select_from_seek_pattern() {
        let ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 8,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Transaction,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenRead,
                p1: 0,
                p2: 5,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Variable,
                p1: 1,
                p2: 1,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::SeekRowid,
                p1: 0,
                p2: 7,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 0,
                p3: 2,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 1,
                p3: 3,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::ResultRow,
                p1: 2,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Close,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];

        let compiled = try_compile_program(&ops);
        let template = match compiled {
            Some(CompiledProgram::RowidLookupSelect(t)) => t,
            other => panic!("expected RowidLookupSelect, got {other:?}"),
        };
        assert_eq!(template.cursor_id, 0);
        assert_eq!(template.root_page, 5);
        assert_eq!(template.column_indices, vec![0, 1]);
        assert_eq!(template.rowid_source, InsertValueSource::Binding(0));
    }

    #[test]
    fn try_compile_full_scan_select_from_rewind_next_pattern() {
        let ops = vec![
            VdbeOp {
                opcode: Opcode::Init,
                p1: 0,
                p2: 9,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Transaction,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::OpenRead,
                p1: 0,
                p2: 3,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Rewind,
                p1: 0,
                p2: 8,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 0,
                p3: 1,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Column,
                p1: 0,
                p2: 1,
                p3: 2,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::ResultRow,
                p1: 1,
                p2: 2,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Next,
                p1: 0,
                p2: 4,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Close,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
            VdbeOp {
                opcode: Opcode::Halt,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::None,
                p5: 0,
            },
        ];

        let compiled = try_compile_program(&ops);
        let template = match compiled {
            Some(CompiledProgram::FullScanSelect(t)) => t,
            other => panic!("expected FullScanSelect, got {other:?}"),
        };
        assert_eq!(template.cursor_id, 0);
        assert_eq!(template.root_page, 3);
        assert_eq!(template.column_indices, vec![0, 1]);

        // bd-cp3yd: the same scan of a TEMP table (OpenRead p3=1) stays
        // interpreted, which routes the cursor to the TEMP MemDatabase.
        let mut temp_ops = ops;
        temp_ops[2].p3 = 1;
        assert!(try_compile_program(&temp_ops).is_none());
    }
}
