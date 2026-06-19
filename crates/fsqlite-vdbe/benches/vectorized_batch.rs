use std::collections::HashMap;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::vectorized::{Batch, ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY};
use fsqlite_vdbe::vectorized_join::{TrieRelation, TrieRow};

fn benchmark_specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec::new("id", ColumnVectorType::Int64),
        ColumnSpec::new("score", ColumnVectorType::Float64),
        ColumnSpec::new("payload", ColumnVectorType::Binary),
        ColumnSpec::new("name", ColumnVectorType::Text),
    ]
}

fn build_rows(row_count: usize) -> Vec<Vec<SqliteValue>> {
    let mut rows = Vec::with_capacity(row_count);
    for row_id in 0..row_count {
        let payload = vec![
            u8::try_from(row_id & 0xFF).expect("low byte should fit into u8"),
            u8::try_from((row_id >> 8) & 0xFF).expect("middle byte should fit into u8"),
            u8::try_from((row_id >> 16) & 0xFF).expect("high byte should fit into u8"),
        ];
        rows.push(vec![
            SqliteValue::Integer(i64::try_from(row_id).expect("row id should fit into i64")),
            SqliteValue::Float(row_id as f64 * 1.25),
            SqliteValue::Blob(payload.into()),
            SqliteValue::Text(format!("row-{row_id:04}").into()),
        ]);
    }
    rows
}

fn build_trie_rows(row_count: usize) -> Vec<TrieRow> {
    let mut rows = Vec::with_capacity(row_count);
    for row_id in 0..row_count {
        let high = i64::try_from(row_id / 32).expect("high key should fit into i64");
        let low = i64::try_from(row_id % 32).expect("low key should fit into i64");
        rows.push(TrieRow::new(
            vec![SqliteValue::Integer(high), SqliteValue::Integer(low)],
            row_id,
        ));
    }
    rows
}

fn build_hash_index(rows: &[TrieRow]) -> HashMap<(i64, i64), Vec<usize>> {
    let mut index: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
    for row in rows {
        let SqliteValue::Integer(high) = row.key[0] else {
            continue;
        };
        let SqliteValue::Integer(low) = row.key[1] else {
            continue;
        };
        index
            .entry((high, low))
            .or_default()
            .push(row.payload_row_index);
    }
    index
}

fn bench_batch_construction(c: &mut Criterion) {
    let specs = benchmark_specs();
    let mut group = c.benchmark_group("vectorized_batch_construction");

    for row_count in [64_usize, 256, DEFAULT_BATCH_ROW_CAPACITY] {
        let rows = build_rows(row_count);
        group.bench_with_input(BenchmarkId::from_parameter(row_count), &rows, |b, rows| {
            b.iter(|| {
                let batch = Batch::from_rows(rows, &specs, DEFAULT_BATCH_ROW_CAPACITY)
                    .expect("batch construction should succeed");
                black_box(batch);
            });
        });
    }

    group.finish();
}

fn bench_trie_vs_hash_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("vectorized_join_index_build");

    for row_count in [1_024_usize, 4_096_usize, 16_384_usize] {
        let rows = build_trie_rows(row_count);
        group.bench_with_input(
            BenchmarkId::new("trie_build", row_count),
            &rows,
            |b, rows| {
                b.iter(|| {
                    let trie = TrieRelation::from_sorted_rows(rows.clone())
                        .expect("trie build should succeed");
                    black_box(trie.node_count());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("hash_build", row_count),
            &rows,
            |b, rows| {
                b.iter(|| {
                    let index = build_hash_index(rows);
                    black_box(index.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_batch_construction, bench_trie_vs_hash_build);
criterion_main!(benches);
