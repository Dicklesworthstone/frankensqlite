use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_pager::{
    MvccPager, SimplePager, SimpleTransaction, TransactionHandle, TransactionMode,
};
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{Cx, PageNumber, PageSize};
use fsqlite_vdbe::engine::{MemDatabase, VdbeEngine, set_vdbe_jit_enabled};
use fsqlite_vdbe::{
    ProgramBuilder, VdbeProgram, profile_vdbe_commit_stage, profile_vdbe_decode_stage,
};
use fsqlite_vfs::MemoryVfs;
use std::path::Path;

const EXECUTE_STAGE_OP_REPEATS: [usize; 3] = [64, 256, 1024];
const COMMIT_STAGE_DIRTY_PAGES: [usize; 3] = [2, 8, 32];
const RESET_STAGE_REGISTER_COUNTS: [i32; 2] = [4, 16];

fn decode_stage_row(column_count: usize) -> Vec<SqliteValue> {
    (0..column_count)
        .map(|idx| match idx % 3 {
            0 => SqliteValue::Integer(i64::try_from(idx * 97 + 11).unwrap()),
            1 => SqliteValue::Text(format!("decode-stage-{idx:03}").into()),
            _ => SqliteValue::Blob(vec![u8::try_from((idx % 251) + 1).unwrap(); 24].into()),
        })
        .collect()
}

fn build_execute_stage_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let accumulator = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 0, accumulator, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::AddImm, accumulator, 1, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Integer` constant loads. Each op is a p1 immediate to p2 register write,
/// which isolates whether the hot pre-filter still pays for the small body.
fn build_execute_stage_integer_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for value in 0..op_repeats {
        let immediate = i32::try_from(value).unwrap_or(i32::MAX);
        builder.emit_op(Opcode::Integer, immediate, target, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute integer benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Noop`
/// ops. The opcode body is only `pc += 1`, so this isolates dispatch routing
/// overhead for the smallest executable instruction body.
fn build_execute_stage_noop_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Noop, 0, 0, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute noop benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Goto`
/// ops that each jump to the next instruction. That preserves straight-line
/// execution while isolating unconditional-jump dispatch and target assignment.
fn build_execute_stage_goto_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    for _ in 0..op_repeats {
        let next_addr = i32::try_from(builder.current_addr() + 1).unwrap_or(i32::MAX);
        builder.emit_op(Opcode::Goto, 0, next_addr, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute goto benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// single-column `ResultRow` ops with result collection disabled by the
/// benchmark harness. That isolates the hot pre-filter cost around
/// `discard_reg_range` without retaining benchmark rows.
fn build_execute_stage_resultrow_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let source = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 7, source, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::ResultRow, source, 1, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute resultrow benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `FusedLiteralResultRow` ops. This isolates the peephole opcode's
/// write-then-drain semantics with result collection disabled, matching the
/// generated `Integer(lit, reg) + ResultRow(reg, 1)` fast path without
/// retaining benchmark rows.
fn build_execute_stage_fused_literal_resultrow_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let source = builder.alloc_reg();
    for value in 0..op_repeats {
        let literal = i32::try_from(value).unwrap_or(i32::MAX);
        builder.emit_op(
            Opcode::FusedLiteralResultRow,
            literal,
            source,
            0,
            P4::None,
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute fused literal resultrow benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Int64` constant loads. `Int64` differs from `Integer` by reading the
/// payload from `p4`, so it needs a separate measurement before deciding
/// whether hot-dispatch promotion pays for its match on `P4`.
fn build_execute_stage_int64_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(
            Opcode::Int64,
            0,
            target,
            0,
            P4::Int64(9_223_372_036_854_775_000),
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute int64 benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Real`
/// constant loads. `Real` is a low-frequency hot-dispatch arm that still
/// reads its payload from `p4`, so this benchmark lets us prove whether keeping
/// it in the hot pre-filter is worth the extra match surface.
fn build_execute_stage_real_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Real, 0, target, 0, P4::Real(42.75), 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute real benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Blob`
/// constant loads. The p4 payload is intentionally small and stable so the
/// benchmark isolates dispatch plus register blob-buffer reuse rather than
/// large allocation or memcpy behavior.
fn build_execute_stage_blob_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(
            Opcode::Blob,
            0,
            target,
            0,
            P4::Blob(b"fsqlite-blob-hot".to_vec()),
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute blob benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `String8` constant loads. `String8` is already hot-dispatched separately
/// from `String`, so this benchmark lets us prove whether that older hot arm
/// still earns its place in the pre-filter.
fn build_execute_stage_string8_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(
            Opcode::String8,
            0,
            target,
            0,
            P4::Str("fsqlite-string8-hot".to_owned()),
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute string8 benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `String`
/// constant loads. `String8` is already hot-dispatched, but `String` takes the
/// same write-text body through the cold match and needs separate evidence.
fn build_execute_stage_string_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(
            Opcode::String,
            18,
            target,
            0,
            P4::Str("fsqlite-string-hot".to_owned()),
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute string benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Null`
/// writes into a stable target register. The range-fill form is intentionally
/// not used here so this isolates the common single-register body.
fn build_execute_stage_null_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Null, 0, target, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute null benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `SoftNull` register writes. `SoftNull` writes through p1 rather than p2, so
/// it needs separate coverage from `Null` before any hot-dispatch decision.
fn build_execute_stage_softnull_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::SoftNull, target, 0, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute softnull benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Concat`
/// ops over stable text inputs. Each iteration writes into the same output
/// register without mutating the sources, isolating concat dispatch and the
/// text-text fast path.
fn build_execute_stage_concat_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(
        Opcode::String8,
        0,
        lhs,
        0,
        P4::Str("fsqlite-concat-left".to_owned()),
        0,
    );
    builder.emit_op(
        Opcode::String8,
        0,
        rhs,
        0,
        P4::Str("fsqlite-concat-right".to_owned()),
        0,
    );
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Concat, rhs, lhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute concat benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Add`
/// ops over stable integer inputs. The opcode writes into a separate output
/// register so every iteration exercises the same integer-add body without
/// changing the source operands.
fn build_execute_stage_add_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 17, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 25, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Add, rhs, lhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute add benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Subtract` ops over stable integer inputs. The output register is distinct
/// so every iteration exercises the same subtraction body without changing the
/// source operands.
fn build_execute_stage_subtract_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 17, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Subtract, rhs, lhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute subtract benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Multiply` ops over stable integer inputs. Like the Add benchmark, the
/// output register is distinct so every iteration exercises the same
/// multiplication body without perturbing the operands.
fn build_execute_stage_multiply_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 7, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 6, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Multiply, rhs, lhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute multiply benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Divide` ops over stable non-zero integer inputs. The output register is
/// distinct so every iteration exercises the same division body without
/// changing the divisor or dividend.
fn build_execute_stage_divide_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let divisor = builder.alloc_reg();
    let dividend = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 7, divisor, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 84, dividend, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Divide, divisor, dividend, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute divide benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Remainder` ops over stable non-zero integer inputs. The output register is
/// distinct so every iteration exercises modulo semantics without changing
/// either operand.
fn build_execute_stage_remainder_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let divisor = builder.alloc_reg();
    let dividend = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 7, divisor, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 86, dividend, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Remainder, divisor, dividend, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute remainder benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `BitAnd`
/// ops over stable non-NULL integer inputs. The output register is distinct so
/// each iteration exercises the same bitwise body without changing operands.
fn build_execute_stage_bitand_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 0xFF, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 0x0F, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::BitAnd, lhs, rhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute bitand benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `BitOr`
/// ops over stable non-NULL integer inputs. The output register is distinct so
/// each iteration exercises the same bitwise body without changing operands.
fn build_execute_stage_bitor_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 0xF0, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 0x0F, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::BitOr, lhs, rhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute bitor benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `BitNot`
/// ops over a stable non-NULL integer input. The output register is distinct so
/// every iteration exercises the common integer complement body without
/// changing the operand.
fn build_execute_stage_bitnot_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let value = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 0x0F, value, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::BitNot, value, out, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute bitnot benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `ShiftLeft` ops over stable non-NULL integer inputs. The shift amount is
/// kept small so the benchmark isolates dispatch/body cost, not overflow-edge
/// handling.
fn build_execute_stage_shiftleft_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let amount = builder.alloc_reg();
    let value = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 3, amount, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 0x11, value, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::ShiftLeft, amount, value, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute shiftleft benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `ShiftRight` ops over stable non-NULL integer inputs. The shift amount is
/// kept small so the benchmark isolates dispatch/body cost, not sign-extension
/// edge behavior.
fn build_execute_stage_shiftright_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let amount = builder.alloc_reg();
    let value = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 3, amount, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 0x1100, value, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::ShiftRight, amount, value, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute shiftright benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Variable` loads. The benchmark seeds one owned binding on the engine, so
/// each opcode exercises the common bound-parameter path: convert p1 from
/// one-based to zero-based, read the binding, clone it, and write the target
/// register.
fn build_execute_stage_variable_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let target = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Variable, 1, target, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute variable benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// single-register `Copy` ops. The source register holds an `Integer`, so
/// the body reduces to `clone + set_reg_fast` per dispatch — the work is
/// small enough that the hot-path pre-filter vs main-match routing is the
/// dominant cost, which is exactly the effect we want to measure.
fn build_execute_stage_copy_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let src = builder.alloc_reg();
    let dst = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, src, 0, P4::None, 0);
    for _ in 0..op_repeats {
        // p1=src, p2=dst, p3=0 (copy a single register)
        builder.emit_op(Opcode::Copy, src, dst, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute copy benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// single-register `SCopy` (shallow-copy) ops. Like the Copy variant, the
/// source holds an `Integer`, so the body is `clone + set_reg_fast` per
/// dispatch — isolating the hot-path pre-filter vs main-match routing
/// cost for the SCopy arm specifically.
fn build_execute_stage_scopy_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let src = builder.alloc_reg();
    let dst = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, src, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::SCopy, src, dst, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute scopy benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `IntCopy` ops. The source register holds a `Real`, so every iteration
/// exercises the opcode's integer coercion body instead of degenerating into a
/// plain integer copy.
fn build_execute_stage_intcopy_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let src = builder.alloc_reg();
    let dst = builder.alloc_reg();
    builder.emit_op(Opcode::Real, 0, src, 0, P4::Real(42.75), 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::IntCopy, src, dst, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute intcopy benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// single-register `Move` ops. Unlike `Copy`/`SCopy`, `Move` drains the source
/// register, so the benchmark alternates direction between two registers to
/// keep every iteration on the non-empty transfer path.
fn build_execute_stage_move_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let left = builder.alloc_reg();
    let right = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, left, 0, P4::None, 0);
    for idx in 0..op_repeats {
        let (src, dst) = if idx % 2 == 0 {
            (left, right)
        } else {
            (right, left)
        };
        builder.emit_op(Opcode::Move, src, dst, 1, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute move benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `DecrJumpZero` ops (the canonical LIMIT counter opcode). The counter
/// is seeded with `op_repeats + 1` so every dispatched opcode hits the
/// decrement-and-fall-through path — none jump to the halt target,
/// giving a stable per-op cost that isolates the hot-path pre-filter
/// routing from the mostly-taken branch predictor.
fn build_execute_stage_decrjumpzero_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    let halt = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let counter = builder.alloc_reg();
    // Seed so op_repeats decrements leave the counter at 1 (never zero,
    // so the jump is never taken).
    let seed = i32::try_from(op_repeats + 1).unwrap_or(i32::MAX);
    builder.emit_op(Opcode::Integer, seed, counter, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_jump_to_label(Opcode::DecrJumpZero, counter, 0, halt, P4::None, 0);
    }
    builder.resolve_label(halt);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute decrjumpzero benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `IfPos` ops (the canonical OFFSET counter opcode). Each op's p2 jump
/// target is the instruction immediately after it, so a "jump" is
/// semantically equivalent to a fall-through for execution sequencing
/// but still exercises the opcode's taken-branch body (register read,
/// subtract, write-back, pc reassignment). p3=1 makes each op decrement
/// the counter by one; counter is seeded with `op_repeats + 1` so the
/// val>0 branch is taken every iteration.
fn build_execute_stage_ifpos_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let counter = builder.alloc_reg();
    let seed = i32::try_from(op_repeats + 1).unwrap_or(i32::MAX);
    builder.emit_op(Opcode::Integer, seed, counter, 0, P4::None, 0);
    for _ in 0..op_repeats {
        let next = builder.emit_label();
        // p1=counter, p3=1 (decrement by one), p2=next (the very next
        // instruction — so this is an always-taken, fall-through-style jump).
        builder.emit_jump_to_label(Opcode::IfPos, counter, 1, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute ifpos benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `IfNotZero` ops. The counter is seeded with `op_repeats + 1`, so every
/// dispatched opcode takes the nonzero branch, decrements the counter, and
/// jumps to the immediately-next instruction. That keeps execution linear
/// while exercising the real branch body.
fn build_execute_stage_ifnotzero_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let counter = builder.alloc_reg();
    let seed = i32::try_from(op_repeats + 1).unwrap_or(i32::MAX);
    builder.emit_op(Opcode::Integer, seed, counter, 0, P4::None, 0);
    for _ in 0..op_repeats {
        let next = builder.emit_label();
        builder.emit_jump_to_label(Opcode::IfNotZero, counter, 0, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute ifnotzero benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `OffsetLimit` ops over stable LIMIT/OFFSET registers. This exercises the
/// common positive-limit path: read two integer registers, saturating-add them,
/// and write the combined counter into a third register.
fn build_execute_stage_offsetlimit_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let limit = builder.alloc_reg();
    let offset = builder.alloc_reg();
    let combined = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 50, limit, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 7, offset, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::OffsetLimit, limit, offset, combined, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute offsetlimit benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// single-register `Affinity` ops. The register starts as an integer and the
/// affinity string is `D`, so each opcode exercises the common no-conversion
/// integer-affinity path without mixing in per-iteration setup work.
fn build_execute_stage_affinity_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let value = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, value, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(
            Opcode::Affinity,
            value,
            1,
            0,
            P4::Affinity("D".to_owned()),
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute affinity benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `IsNull` ops (the canonical NULL-test / NOT NULL-constraint opcode,
/// 87 codegen sites — highest-frequency unpromoted opcode at the time
/// this bench was added). The source register is seeded with `Null`,
/// so each op exercises the taken-branch path: `is_null` returns true
/// → `pc = op.p2`. Each op's p2 jump target is the instruction
/// immediately after it, so the always-taken jump is semantically
/// equivalent to a fall-through for execution sequencing but still
/// runs the real branch body. This isolates the hot-path pre-filter
/// vs main-match routing cost for the IsNull arm specifically.
fn build_execute_stage_isnull_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let probe = builder.alloc_reg();
    builder.emit_op(Opcode::Null, 0, probe, 0, P4::None, 0);
    for _ in 0..op_repeats {
        let next = builder.emit_label();
        builder.emit_jump_to_label(Opcode::IsNull, probe, 0, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute isnull benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `NotNull` ops. This mirrors the IsNull benchmark's always-taken-jump shape:
/// the source register is seeded with an integer, so each op observes NOT NULL
/// and jumps to the immediately-next instruction. That keeps execution linear
/// while exercising the real branch body.
fn build_execute_stage_notnull_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let probe = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 1, probe, 0, P4::None, 0);
    for _ in 0..op_repeats {
        let next = builder.emit_label();
        builder.emit_jump_to_label(Opcode::NotNull, probe, 0, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute notnull benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `And`
/// ops over stable non-NULL boolean inputs. Each opcode reads two registers,
/// applies SQLite three-valued AND semantics, and writes into one stable
/// destination register without changing the source operands.
fn build_execute_stage_and_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 1, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 0, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::And, lhs, rhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute and benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Or`
/// ops over stable non-NULL boolean inputs. Each opcode reads two registers,
/// applies SQLite three-valued OR semantics, and writes into one stable
/// destination register without changing the source operands.
fn build_execute_stage_or_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 0, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 1, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Or, lhs, rhs, out, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute or benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `ZeroOrNull` ops over stable non-NULL integer inputs. The opcode writes
/// into one destination register without mutating either source, isolating the
/// common non-NULL body from per-iteration setup.
fn build_execute_stage_zeroornull_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 7, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 11, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::ZeroOrNull, lhs, out, rhs, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute zeroornull benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Column` ops against a single positioned storage cursor. The cursor is
/// opened on a one-row table and Rewound to the only row, so each dispatched
/// `Column` op runs the realistic direct-column/fallback body without any
/// cursor motion between iterations.
fn build_execute_stage_column_program(root_page: i32, op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let halt = builder.emit_label();
    builder.emit_op(Opcode::OpenWrite, 0, root_page, 0, P4::Int(1), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, halt, P4::None, 0);
    let r_out = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Column, 0, 0, r_out, P4::None, 0);
    }
    builder.resolve_label(halt);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder
        .finish()
        .expect("pipeline execute column benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `ColumnSubstrPrefix` ops against a positioned storage cursor. This covers
/// the direct raw-bytes prefix path without benchmarking scalar function setup.
fn build_execute_stage_column_substr_prefix_program(
    root_page: i32,
    op_repeats: usize,
) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let halt = builder.emit_label();
    builder.emit_op(Opcode::OpenWrite, 0, root_page, 0, P4::Int(1), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, halt, P4::None, 0);
    let r_out = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::ColumnSubstrPrefix, 0, 0, r_out, P4::Int(8), 0);
    }
    builder.resolve_label(halt);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder
        .finish()
        .expect("pipeline execute column-substr-prefix benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `Rowid` ops against a single positioned storage cursor.  The cursor
/// is opened on a one-row table and Rewound to the only row, so each
/// dispatched `Rowid` op runs the realistic body shape (one
/// `storage_cursors` HashMap probe + one `cursor.rowid` call + one
/// `set_reg_fast`) without any cursor motion in between.  This isolates
/// the hot-path pre-filter vs main-match routing cost for the `Rowid`
/// arm specifically — same shape pattern as the `IsNull`/`IfPos`
/// /`IfNot` benches, where the body is uniform across ops and dispatch
/// routing is the variable being measured.
fn build_execute_stage_rowid_program(root_page: i32, op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    // The bytecode verifier requires Rewind p2 to be strictly `<
    // op_count`, so the Rewind EOF target points at a real instruction
    // — the program-end Halt — via a label resolved *at* that Halt.
    // The single-row table guarantees Rewind never takes the EOF branch
    // in the bench loop, but the verifier still demands a valid
    // in-bounds target.  Init is omitted: the engine starts at pc=0
    // unconditionally.
    let halt = builder.emit_label();
    builder.emit_op(Opcode::OpenWrite, 0, root_page, 0, P4::Int(1), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, halt, P4::None, 0);
    let r_out = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Rowid, 0, r_out, 0, P4::None, 0);
    }
    builder.resolve_label(halt);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder
        .finish()
        .expect("pipeline execute rowid benchmark program should build")
}

/// Mirrors `build_execute_stage_rowid_program` but emits `IdxRowid`
/// in the body loop.  Both opcodes route through `cursor_rowid`, so
/// this isolates dispatch routing cost for the `IdxRowid` arm specifically
/// — same shape pattern as the Rowid bench.
fn build_execute_stage_idx_rowid_program(root_page: i32, op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let halt = builder.emit_label();
    builder.emit_op(Opcode::OpenWrite, 0, root_page, 0, P4::Int(1), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, halt, P4::None, 0);
    let r_out = builder.alloc_reg();
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::IdxRowid, 0, r_out, 0, P4::None, 0);
    }
    builder.resolve_label(halt);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder
        .finish()
        .expect("pipeline execute idx_rowid benchmark program should build")
}

fn build_execute_stage_ifnot_program(op_repeats: usize) -> VdbeProgram {
    // Mirrors the IsNull builder's always-taken-jump shape: each
    // IfNot's p2 jump target is the immediately-next instruction, so
    // the body runs the real branch (falsy → take jump) but execution
    // sequencing stays linear.  The probe register is seeded to 0
    // (falsy) so the branch is always taken — same shape pattern as
    // ifpos/isnull, exercising the dispatch + body without polluting
    // the timing with side-effects.
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let probe = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 0, probe, 0, P4::None, 0);
    for _ in 0..op_repeats {
        let next = builder.emit_label();
        builder.emit_jump_to_label(Opcode::IfNot, probe, 0, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute ifnot benchmark program should build")
}

fn build_execute_stage_if_program(op_repeats: usize) -> VdbeProgram {
    // Mirrors the IfNot benchmark's linear branch shape: each If jump target is
    // the immediately-next instruction, so the opcode executes the taken branch
    // body while preserving straight-line control flow for stable per-op timing.
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let probe = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 1, probe, 0, P4::None, 0);
    for _ in 0..op_repeats {
        let next = builder.emit_label();
        builder.emit_jump_to_label(Opcode::If, probe, 0, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute if benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// two-field `Compare` ops. The first field is equal and the second differs, so
/// each opcode runs the realistic multi-register comparison body without
/// involving collation locks or a following `Jump`.
fn build_execute_stage_compare_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let left_first = builder.alloc_reg();
    let left_second = builder.alloc_reg();
    let right_first = builder.alloc_reg();
    let right_second = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, left_first, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 9, left_second, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 42, right_first, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 11, right_second, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Compare, left_first, right_first, 2, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute compare benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a mixed stream of
/// comparison-jump ops. Each jump target is the immediately-next instruction,
/// so taken and fall-through cases both preserve straight-line execution while
/// still exercising the real comparison branch body.
fn build_execute_stage_comparison_jump_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = builder.alloc_reg();
    let rhs = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 7, lhs, 0, P4::None, 0);
    builder.emit_op(Opcode::Integer, 11, rhs, 0, P4::None, 0);
    let opcodes = [
        Opcode::Eq,
        Opcode::Ne,
        Opcode::Lt,
        Opcode::Le,
        Opcode::Gt,
        Opcode::Ge,
    ];
    for opcode in opcodes.iter().copied().cycle().take(op_repeats) {
        let next = builder.emit_label();
        builder.emit_jump_to_label(opcode, rhs, lhs, next, P4::None, 0);
        builder.resolve_label(next);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute comparison-jump benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of
/// `IsTrue` ops. The probe register is seeded to a truthy integer and
/// each op writes the coerced boolean result into one stable destination
/// register, isolating the opcode's truthiness read + integer write body
/// from unrelated cursor or row-output work.
fn build_execute_stage_istrue_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let probe = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 1, probe, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::IsTrue, probe, out, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute istrue benchmark program should build")
}

/// Build a dispatch-dominated program whose inner loop is a stream of `Not`
/// ops over a stable integer input. Each opcode reads p1, computes SQLite
/// truthiness, and writes the boolean result to p2. Keeping p1 and p2 distinct
/// avoids alternating source values while still exercising the real non-null
/// body.
fn build_execute_stage_not_program(op_repeats: usize) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let end = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let probe = builder.alloc_reg();
    let out = builder.alloc_reg();
    builder.emit_op(Opcode::Integer, 42, probe, 0, P4::None, 0);
    for _ in 0..op_repeats {
        builder.emit_op(Opcode::Not, probe, out, 0, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(end);
    builder
        .finish()
        .expect("pipeline execute not benchmark program should build")
}

fn prepare_commit_stage_fixture(dirty_pages: usize) -> (Cx, SimpleTransaction<MemoryVfs>) {
    let cx = Cx::new();
    let pager = SimplePager::open_with_cx(
        &cx,
        MemoryVfs::new(),
        Path::new("/:memory:"),
        PageSize::DEFAULT,
    )
    .expect("pipeline commit benchmark should open pager");
    let mut txn = pager
        .begin(&cx, TransactionMode::Immediate)
        .expect("pipeline commit benchmark should begin transaction");
    let page_bytes = PageSize::DEFAULT.as_usize();
    txn.write_page(&cx, PageNumber::ONE, &vec![0xA5; page_bytes])
        .expect("pipeline commit benchmark should dirty page one");
    for page_idx in 1..dirty_pages {
        let page_no = txn
            .allocate_page(&cx)
            .expect("pipeline commit benchmark should allocate page");
        let fill = u8::try_from((page_idx % 251) + 1).unwrap();
        txn.write_page(&cx, page_no, &vec![fill; page_bytes])
            .expect("pipeline commit benchmark should dirty page");
    }
    (cx, txn)
}

fn bench_vdbe_decode_stage(c: &mut Criterion) {
    let mut group = c.benchmark_group("vdbe_pipeline_decode");

    for column_count in [8_usize, 32, 128] {
        let record = serialize_record(&decode_stage_row(column_count));
        group.throughput(Throughput::Bytes(
            u64::try_from(record.len()).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(column_count),
            &record,
            |b, record| {
                b.iter(|| {
                    let decoded = profile_vdbe_decode_stage(|| {
                        parse_record(black_box(record.as_slice()))
                            .expect("pipeline decode benchmark should parse record")
                    });
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_integer_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_integer");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_integer_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute integer benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_noop_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_noop");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_noop_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute noop benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_goto_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_goto");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_goto_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute goto benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_resultrow_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_resultrow");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_resultrow_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute resultrow benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_fused_literal_resultrow_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_fused_literal_resultrow");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_fused_literal_resultrow_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine.execute(program).expect(
                        "pipeline execute fused literal resultrow benchmark should execute",
                    );
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_int64_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_int64");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_int64_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute int64 benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_real_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_real");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_real_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute real benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_blob_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_blob");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_blob_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute blob benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_string8_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_string8");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_string8_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute string8 benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_string_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_string");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_string_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute string benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_null_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_null");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_null_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute null benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_softnull_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_softnull");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_softnull_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute softnull benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_concat_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_concat");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_concat_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute concat benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_add_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_add");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_add_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute add benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_subtract_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_subtract");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_subtract_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute subtract benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_multiply_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_multiply");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_multiply_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute multiply benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_divide_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_divide");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_divide_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute divide benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_remainder_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_remainder");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_remainder_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute remainder benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_bitand_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_bitand");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_bitand_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute bitand benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_bitor_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_bitor");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_bitor_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute bitor benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_bitnot_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_bitnot");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_bitnot_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute bitnot benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_shiftleft_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_shiftleft");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_shiftleft_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute shiftleft benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_shiftright_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_shiftright");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_shiftright_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute shiftright benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_variable_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_variable");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_variable_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                engine.set_bindings_slice(&[SqliteValue::Integer(42)]);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute variable benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_copy_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_copy");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_copy_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute copy benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_scopy_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_scopy");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_scopy_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute scopy benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_intcopy_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_intcopy");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_intcopy_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute intcopy benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_move_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_move");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_move_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute move benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_decrjumpzero_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_decrjumpzero");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_decrjumpzero_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute decrjumpzero benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_ifpos_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_ifpos");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_ifpos_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute ifpos benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_ifnotzero_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_ifnotzero");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_ifnotzero_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute ifnotzero benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_offsetlimit_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_offsetlimit");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_offsetlimit_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute offsetlimit benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_affinity_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_affinity");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_affinity_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute affinity benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_isnull_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_isnull");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_isnull_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute isnull benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_notnull_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_notnull");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_notnull_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute notnull benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_and_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_and");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_and_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute and benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_or_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_or");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_or_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute or benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_zeroornull_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_zeroornull");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_zeroornull_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute zeroornull benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_column_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_column");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        db.get_table_mut(root)
            .expect("table should exist")
            .insert_row(1, vec![SqliteValue::Integer(42)]);
        let program = build_execute_stage_column_program(root, op_repeats);

        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &(program, db),
            |b, (program, db)| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                engine.enable_storage_cursors(true);
                engine.set_database(db.clone());
                engine.set_reject_mem_fallback(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute column benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_column_substr_prefix_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_column_substr_prefix");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        db.get_table_mut(root)
            .expect("table should exist")
            .insert_row(
                1,
                vec![SqliteValue::Text(
                    "column-prefix-benchmark-row".to_owned().into(),
                )],
            );
        let program = build_execute_stage_column_substr_prefix_program(root, op_repeats);

        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &(program, db),
            |b, (program, db)| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                engine.enable_storage_cursors(true);
                engine.set_database(db.clone());
                engine.set_reject_mem_fallback(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute column substr prefix benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_rowid_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_rowid");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        // Build a fresh single-row MemDatabase per param so each bench
        // run gets an independent root-page id (engine takes ownership
        // of the database).  Rowid bodies hit `storage_cursors` after
        // OpenWrite/Rewind position the cursor on the only row.
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        db.get_table_mut(root)
            .expect("table should exist")
            .insert_row(1, vec![SqliteValue::Integer(42)]);
        let program = build_execute_stage_rowid_program(root, op_repeats);

        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &(program, db),
            |b, (program, db)| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                engine.enable_storage_cursors(true);
                engine.set_database(db.clone());
                engine.set_reject_mem_fallback(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute rowid benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_idx_rowid_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_idx_rowid");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        db.get_table_mut(root)
            .expect("table should exist")
            .insert_row(1, vec![SqliteValue::Integer(42)]);
        let program = build_execute_stage_idx_rowid_program(root, op_repeats);

        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &(program, db),
            |b, (program, db)| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                engine.enable_storage_cursors(true);
                engine.set_database(db.clone());
                engine.set_reject_mem_fallback(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute idx_rowid benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_ifnot_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_ifnot");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_ifnot_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute ifnot benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_if_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_if");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_if_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute if benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_compare_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_compare");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_compare_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute compare benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_comparison_jump_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_comparison_jump");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_comparison_jump_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute comparison-jump benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_istrue_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_istrue");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_istrue_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute istrue benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_execute_not_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_not");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let program = build_execute_stage_not_program(op_repeats);
        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute not benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_commit_stage(c: &mut Criterion) {
    let mut group = c.benchmark_group("vdbe_pipeline_commit");

    for dirty_pages in COMMIT_STAGE_DIRTY_PAGES {
        group.throughput(Throughput::Elements(
            u64::try_from(dirty_pages).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(dirty_pages),
            &dirty_pages,
            |b, &dirty_pages| {
                b.iter_batched(
                    || prepare_commit_stage_fixture(dirty_pages),
                    |(cx, mut txn)| {
                        profile_vdbe_commit_stage(|| {
                            txn.commit(&cx)
                                .expect("pipeline commit benchmark should commit");
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn build_execute_stage_next_program(root_page: i32) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let halt = builder.emit_label();
    builder.emit_op(Opcode::OpenWrite, 0, root_page, 0, P4::Int(1), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, halt, P4::None, 0);
    let body = i32::try_from(builder.current_addr()).expect("body addr fits i32");
    builder.emit_op(Opcode::Next, 0, body, 0, P4::None, 0);
    builder.resolve_label(halt);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder
        .finish()
        .expect("pipeline execute next benchmark program should build")
}

fn bench_vdbe_execute_next_stage(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("vdbe_pipeline_execute_next");

    for op_repeats in EXECUTE_STAGE_OP_REPEATS {
        let mut db = MemDatabase::new();
        let root = db.create_table(1);
        let table = db.get_table_mut(root).expect("table should exist");
        for i in 1..=op_repeats {
            table.insert_row(
                i64::try_from(i).unwrap_or(1),
                vec![SqliteValue::Integer(i64::try_from(i).unwrap_or(0))],
            );
        }
        let program = build_execute_stage_next_program(root);

        group.throughput(Throughput::Elements(
            u64::try_from(op_repeats).unwrap_or(u64::MAX),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(op_repeats),
            &(program, db),
            |b, (program, db)| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                engine.set_collect_result_rows(false);
                engine.set_database(db.clone());
                engine.set_reject_mem_fallback(false);
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("pipeline execute next benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn bench_vdbe_engine_reset_for_reuse(c: &mut Criterion) {
    let execution_cx = Cx::new();
    let mut group = c.benchmark_group("vdbe_engine_reset_for_reuse");

    for register_count in RESET_STAGE_REGISTER_COUNTS {
        let mut engine =
            VdbeEngine::new_with_execution_cx(register_count, &execution_cx, PageSize::DEFAULT);
        group.bench_with_input(
            BenchmarkId::from_parameter(register_count),
            &register_count,
            |b, register_count| {
                b.iter(|| {
                    engine.reset_for_reuse(*register_count, &execution_cx, PageSize::DEFAULT);
                    black_box(&engine);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_vdbe_decode_stage,
    bench_vdbe_execute_stage,
    bench_vdbe_execute_integer_stage,
    bench_vdbe_execute_noop_stage,
    bench_vdbe_execute_goto_stage,
    bench_vdbe_execute_resultrow_stage,
    bench_vdbe_execute_fused_literal_resultrow_stage,
    bench_vdbe_execute_int64_stage,
    bench_vdbe_execute_real_stage,
    bench_vdbe_execute_blob_stage,
    bench_vdbe_execute_string8_stage,
    bench_vdbe_execute_string_stage,
    bench_vdbe_execute_null_stage,
    bench_vdbe_execute_softnull_stage,
    bench_vdbe_execute_concat_stage,
    bench_vdbe_execute_add_stage,
    bench_vdbe_execute_subtract_stage,
    bench_vdbe_execute_multiply_stage,
    bench_vdbe_execute_divide_stage,
    bench_vdbe_execute_remainder_stage,
    bench_vdbe_execute_bitand_stage,
    bench_vdbe_execute_bitor_stage,
    bench_vdbe_execute_bitnot_stage,
    bench_vdbe_execute_shiftleft_stage,
    bench_vdbe_execute_shiftright_stage,
    bench_vdbe_execute_variable_stage,
    bench_vdbe_execute_copy_stage,
    bench_vdbe_execute_scopy_stage,
    bench_vdbe_execute_intcopy_stage,
    bench_vdbe_execute_move_stage,
    bench_vdbe_execute_decrjumpzero_stage,
    bench_vdbe_execute_ifpos_stage,
    bench_vdbe_execute_ifnotzero_stage,
    bench_vdbe_execute_offsetlimit_stage,
    bench_vdbe_execute_affinity_stage,
    bench_vdbe_execute_isnull_stage,
    bench_vdbe_execute_notnull_stage,
    bench_vdbe_execute_and_stage,
    bench_vdbe_execute_or_stage,
    bench_vdbe_execute_zeroornull_stage,
    bench_vdbe_execute_column_stage,
    bench_vdbe_execute_column_substr_prefix_stage,
    bench_vdbe_execute_ifnot_stage,
    bench_vdbe_execute_if_stage,
    bench_vdbe_execute_compare_stage,
    bench_vdbe_execute_comparison_jump_stage,
    bench_vdbe_execute_istrue_stage,
    bench_vdbe_execute_not_stage,
    bench_vdbe_execute_rowid_stage,
    bench_vdbe_execute_idx_rowid_stage,
    bench_vdbe_execute_next_stage,
    bench_vdbe_engine_reset_for_reuse,
    bench_vdbe_commit_stage
);
criterion_main!(benches);
