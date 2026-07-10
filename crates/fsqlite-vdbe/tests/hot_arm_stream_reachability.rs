//! bd-kbuck: prove the `vdbe_pipeline_execute_<opcode>` benchmark streams actually
//! dispatch the opcode whose hot-dispatch arm the ledger's rejected candidates removed.
//!
//! `docs/progress/perf-negative-results.md` marks `Opcode::Add` and `Opcode::ZeroOrNull`
//! hot-dispatch removal as do-not-retry, on the strength of an A/B over those streams.
//! That evidence is worth nothing unless the stream reaches the pruned arm. In sibling
//! repos the same assumption failed: an auto-selector routed the benchmark input to a
//! different implementation, so the "rejected" lever never executed and the do-not-retry
//! rows were measuring dead code.
//!
//! Sampled self-time cannot settle it here — the remote builder returns bench text but
//! not the bench binary, so `perf` cannot be pointed at it. Exact per-opcode execution
//! counters settle it more strongly anyway: they count dispatches instead of estimating
//! them from samples. This is not a formality. `ProgramBuilder::finish()` runs peephole
//! fusion, and a JIT exists, so "the builder emitted N ops" does not imply "the
//! interpreter dispatched N ops".
//!
//! This lives in its own test binary on purpose. `set_vdbe_metrics_enabled` and the
//! opcode counters are PROCESS-GLOBAL, so running this beside the crate's other tests
//! makes every concurrently executing test contribute to the same counters (and
//! `reset_vdbe_metrics()` zeroes them mid-measurement). An earlier revision lived in
//! `engine.rs` and failed exactly that way — the same defect as bd-948sd. One test, one
//! process, no races.

use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_vdbe::engine::{
    VdbeEngine, reset_vdbe_metrics, set_vdbe_jit_enabled, set_vdbe_metrics_enabled,
    vdbe_metrics_snapshot,
};
// `ProgramBuilder::finish()` yields `fsqlite_vdbe::VdbeProgram`, which is a distinct
// type from the identically named `fsqlite_types::opcode::VdbeProgram`.
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};

/// Sizes from `EXECUTE_STAGE_OP_REPEATS` in `benches/pipeline_stages.rs`.
const EXECUTE_STAGE_OP_REPEATS: [usize; 3] = [64, 256, 1024];

/// The two ledger-blocked hot arms, paired with the operand order their benchmark uses.
///
/// `Add` takes `(rhs, lhs, out)` and `ZeroOrNull` takes `(lhs, out, rhs)`; carrying the
/// emit closure alongside the opcode keeps this faithful to the bench and leaves no
/// unreachable match arm to panic in.
type EmitOp = fn(&mut ProgramBuilder, i32, i32, i32);

const HOT_ARM_STREAMS: [(Opcode, EmitOp); 2] = [
    (Opcode::Add, |b, lhs, rhs, out| {
        b.emit_op(Opcode::Add, rhs, lhs, out, P4::None, 0);
    }),
    (Opcode::ZeroOrNull, |b, lhs, rhs, out| {
        b.emit_op(Opcode::ZeroOrNull, lhs, out, rhs, P4::None, 0);
    }),
];

/// Rebuild the benchmark's single-opcode stream over stable integer inputs.
///
/// Mirrors `build_execute_stage_add_program` / `build_execute_stage_zeroornull_program`
/// in `benches/pipeline_stages.rs`, including operand order.
fn stream_program(emit: EmitOp, op_repeats: usize) -> VdbeProgram {
    let mut b = ProgramBuilder::new();
    let end = b.emit_label();
    b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
    let lhs = b.alloc_reg();
    let rhs = b.alloc_reg();
    let out = b.alloc_reg();
    b.emit_op(Opcode::Integer, 17, lhs, 0, P4::None, 0);
    b.emit_op(Opcode::Integer, 25, rhs, 0, P4::None, 0);
    for _ in 0..op_repeats {
        emit(&mut b, lhs, rhs, out);
    }
    b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    b.resolve_label(end);
    b.finish().expect("stream program should build")
}

#[test]
fn hot_arm_bench_streams_dispatch_their_opcodes() {
    // The benchmark disables the JIT; `FSQLITE_JIT_ENABLED` also defaults to false, so
    // the arm the candidates removed is the arm both the bench and production execute.
    set_vdbe_jit_enabled(false);
    set_vdbe_metrics_enabled(true);

    for (op, emit) in HOT_ARM_STREAMS {
        for op_repeats in EXECUTE_STAGE_OP_REPEATS {
            let program = stream_program(emit, op_repeats);
            let execution_cx = Cx::new();
            let mut engine = VdbeEngine::new_with_execution_cx(
                program.register_count(),
                &execution_cx,
                PageSize::DEFAULT,
            );
            engine.set_collect_result_rows(false);

            reset_vdbe_metrics();
            engine.execute(&program).expect("stream program should run");

            let name = format!("{op:?}");
            let dispatched = vdbe_metrics_snapshot()
                .opcode_execution_totals
                .iter()
                .find(|entry| entry.opcode == name)
                .map_or(0, |entry| entry.total);

            assert_eq!(
                dispatched,
                op_repeats as u64,
                "vdbe_pipeline_execute_{} at {op_repeats} ops dispatched {dispatched} \
                 {name} ops, expected {op_repeats}. A count of 0 means the ledger's \
                 hot-dispatch-removal reject for {name} was measured on a stream that \
                 never reaches the pruned arm, and that do-not-retry row must be reopened.",
                name.to_ascii_lowercase()
            );
        }
    }

    set_vdbe_metrics_enabled(false);
}
