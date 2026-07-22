// Ten-million-record history bisect benchmark (`bd-hsi34`).
//
// The corpus is synthetic and contiguous so this benchmark measures lookup
// complexity without allocating a 40.96 GB logical history file. Production
// file lookup uses the same `linear_bisect_floor_with` and
// `sparse_bisect_floor_with` algorithms with a VFS-backed record reader.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_mvcc::history_sidecar::{
    SparseIndex, linear_bisect_floor_with, sparse_bisect_floor_with,
};

const RECORD_COUNT: u64 = 10_000_000;
const TARGET_COMMIT_SEQ: u64 = RECORD_COUNT - 17;

fn history_bisect(c: &mut Criterion) {
    let index = SparseIndex::build_with(RECORD_COUNT, 0xfeed_face_cafe_beef, |position| {
        Ok(position + 1)
    })
    .expect("build ten-million-record sparse index");
    let mut group = c.benchmark_group("history_bisect_10m");
    group.throughput(Throughput::Elements(RECORD_COUNT));
    group.bench_function("linear", |bencher| {
        bencher.iter(|| {
            linear_bisect_floor_with(RECORD_COUNT, black_box(TARGET_COMMIT_SEQ), |position| {
                Ok(position + 1)
            })
            .expect("linear bisect")
        });
    });
    group.bench_function("sparse_1024", |bencher| {
        bencher.iter(|| {
            sparse_bisect_floor_with(&index, black_box(TARGET_COMMIT_SEQ), |position| {
                Ok(position + 1)
            })
            .expect("sparse bisect")
        });
    });
    group.finish();
}

criterion_group!(benches, history_bisect);
criterion_main!(benches);
