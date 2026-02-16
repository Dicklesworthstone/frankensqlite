use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::PageNumber;
use fsqlite_vdbe::vectorized_dispatch::{
    DEFAULT_L2_CACHE_BYTES, DEFAULT_PAGE_SIZE_BYTES, DispatcherConfig, PipelineId, PipelineKind,
    WorkStealingDispatcher, build_pipeline_tasks, partition_page_morsels_auto_tuned,
};

fn synthetic_task_cost(task_id: usize, worker_id: usize) -> u64 {
    let mut state = ((task_id as u64) << 32) | worker_id as u64;
    for round in 0_u64..128_u64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005_u64)
            .wrapping_add(round + 1);
        state ^= state.rotate_left(13);
    }
    state
}

fn bench_dispatcher_scaling(c: &mut Criterion) {
    let start_page = PageNumber::new(1).expect("start page should be non-zero");
    let end_page = PageNumber::new(4_096).expect("end page should be non-zero");
    let morsels = partition_page_morsels_auto_tuned(
        start_page,
        end_page,
        DEFAULT_L2_CACHE_BYTES,
        DEFAULT_PAGE_SIZE_BYTES,
        2,
    )
    .expect("morsel partitioning should succeed");
    let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

    let mut group = c.benchmark_group("vectorized_dispatch_scaling");
    let throughput = u64::try_from(tasks.len()).unwrap_or(u64::MAX);
    group.throughput(Throughput::Elements(throughput));

    for worker_threads in [1_usize, 2, 4, 8] {
        let config = DispatcherConfig {
            worker_threads,
            numa_nodes: 2.min(worker_threads),
        };
        group.bench_with_input(
            BenchmarkId::from_parameter(worker_threads),
            &config,
            |b, config| {
                b.iter(|| {
                    let dispatcher =
                        WorkStealingDispatcher::try_new(*config).expect("dispatcher should build");
                    let reports = dispatcher
                        .execute_with_barriers(std::slice::from_ref(&tasks), |task, worker_id| {
                            synthetic_task_cost(task.task_id, worker_id)
                        })
                        .expect("dispatch should succeed");
                    let checksum = reports[0]
                        .completed
                        .iter()
                        .fold(0_u64, |acc, done| acc ^ done.result);
                    criterion::black_box(checksum);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_dispatcher_scaling);
criterion_main!(benches);
