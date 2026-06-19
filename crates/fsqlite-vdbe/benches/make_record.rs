use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::record::{
    PrecomputedRecordHeader, PrecomputedSerialTypeKind, encode_batch, serialize_record,
};
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{Cx, PageSize};
use fsqlite_vdbe::ProgramBuilder;
use fsqlite_vdbe::engine::{VdbeEngine, set_vdbe_jit_enabled};

const MAKE_RECORD_REPEATS: usize = 256;
const BATCH_RECORD_ROW_COUNTS: [usize; 3] = [16, 128, 1024];

fn fixed_schema_row(column_count: usize) -> Vec<SqliteValue> {
    (0..column_count)
        .map(|idx| {
            let idx_i64 = i64::try_from(idx).expect("benchmark column count should fit into i64");
            let value = (idx_i64 * 97_531) - 49_152;
            SqliteValue::Integer(value)
        })
        .collect()
}

fn integer_header(column_count: usize) -> PrecomputedRecordHeader {
    let kinds = vec![PrecomputedSerialTypeKind::IntegerOrNull; column_count];
    PrecomputedRecordHeader::new(&kinds)
}

fn build_make_record_program(column_count: usize, p4: P4) -> fsqlite_vdbe::VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let first_reg = builder.alloc_regs(i32::try_from(column_count).expect("column count fits"));
    let r_record = builder.alloc_reg();

    for idx in 0..column_count {
        let reg = first_reg + i32::try_from(idx).expect("column index fits");
        let value = match idx % 4 {
            0 => i32::try_from(idx).expect("small integer literal fits"),
            1 => 200 + i32::try_from(idx).expect("small integer literal fits"),
            2 => 70_000 + i32::try_from(idx).expect("small integer literal fits"),
            _ => 5_000_000 + i32::try_from(idx).expect("small integer literal fits"),
        };
        builder.emit_op(Opcode::Integer, value, reg, 0, P4::None, 0);
    }

    for _ in 0..MAKE_RECORD_REPEATS {
        builder.emit_op(
            Opcode::MakeRecord,
            first_reg,
            i32::try_from(column_count).expect("column count fits"),
            r_record,
            p4.clone(),
            0,
        );
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

    builder.finish().expect("benchmark program should build")
}

fn bench_make_record_fixed_schema(c: &mut Criterion) {
    set_vdbe_jit_enabled(false);
    let mut group = c.benchmark_group("make_record_fixed_schema");

    for column_count in [4_usize, 8, 16, 32] {
        let row = fixed_schema_row(column_count);
        let header = integer_header(column_count);
        let record_bytes = u64::try_from(serialize_record(&row).len()).unwrap_or(u64::MAX);
        let total_bytes =
            record_bytes.saturating_mul(u64::try_from(MAKE_RECORD_REPEATS).unwrap_or(0));
        group.throughput(Throughput::Bytes(total_bytes));

        let generic_program =
            build_make_record_program(column_count, P4::Affinity("D".repeat(column_count)));
        group.bench_with_input(
            BenchmarkId::new("generic", column_count),
            &generic_program,
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("generic MakeRecord benchmark should execute");
                    black_box(outcome);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("precomputed_header", column_count),
            &build_make_record_program(column_count, P4::PrecomputedHeader(header)),
            |b, program| {
                let execution_cx = Cx::new();
                let mut engine = VdbeEngine::new_with_execution_cx(
                    program.register_count(),
                    &execution_cx,
                    PageSize::DEFAULT,
                );
                b.iter(|| {
                    let outcome = engine
                        .execute(program)
                        .expect("precomputed MakeRecord benchmark should execute");
                    black_box(outcome);
                });
            },
        );
    }

    group.finish();
}

fn batch_record_rows(row_count: usize) -> Vec<Vec<SqliteValue>> {
    let large_base: i64 = 0x0010_0000_0000_0000; // forces integer serial_type == 6
    (0..row_count)
        .map(|idx| {
            let idx_i64 = i64::try_from(idx).expect("benchmark row count should fit into i64");
            let idx_f64 =
                f64::from(i32::try_from(idx).expect("benchmark row count should fit into i32"));
            let first_blob_byte = u8::try_from(idx & 0xFF).expect("masked byte fits");
            let second_blob_byte =
                u8::try_from(idx.wrapping_mul(3) & 0xFF).expect("masked byte fits");
            vec![
                SqliteValue::Integer(large_base + idx_i64),
                SqliteValue::Text(format!("name_{idx:04}").into()),
                SqliteValue::Integer(large_base + idx_i64 * 7),
                SqliteValue::Float(idx_f64 * 0.25),
                SqliteValue::Blob(Arc::from(
                    [first_blob_byte, second_blob_byte, 0xA5, 0x5A].as_slice(),
                )),
                SqliteValue::Null,
            ]
        })
        .collect()
}

fn scalar_batch_bytes(rows: &[Vec<SqliteValue>]) -> Vec<u8> {
    let mut out = Vec::new();
    for row in rows {
        out.extend_from_slice(&serialize_record(row));
    }
    out
}

fn bench_record_batch_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_encoding");

    for row_count in BATCH_RECORD_ROW_COUNTS {
        let rows = batch_record_rows(row_count);
        let expected = scalar_batch_bytes(&rows);
        let expected_len = expected.len();
        group.throughput(Throughput::Bytes(
            u64::try_from(expected_len).unwrap_or(u64::MAX),
        ));

        group.bench_with_input(
            BenchmarkId::new("scalar_loop", row_count),
            &rows,
            |b, rows| {
                let mut out = Vec::with_capacity(expected_len);
                b.iter(|| {
                    out.clear();
                    for row in rows {
                        let record = serialize_record(black_box(row));
                        out.extend_from_slice(&record);
                    }
                    black_box(&out);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("encode_batch", row_count),
            &rows,
            |b, rows| {
                let row_refs = rows.iter().map(Vec::as_slice).collect::<Vec<_>>();
                let mut out = Vec::with_capacity(expected_len);
                let mut offsets = Vec::with_capacity(rows.len());
                encode_batch(&row_refs, &mut out, &mut offsets)
                    .expect("batch encoder should encode benchmark rows");
                assert_eq!(
                    out, expected,
                    "batch benchmark setup must stay byte-identical to scalar encoding"
                );
                b.iter(|| {
                    encode_batch(black_box(&row_refs), &mut out, &mut offsets)
                        .expect("batch encoder should encode benchmark rows");
                    black_box((&out, &offsets));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_make_record_fixed_schema,
    bench_record_batch_encoding
);
criterion_main!(benches);
