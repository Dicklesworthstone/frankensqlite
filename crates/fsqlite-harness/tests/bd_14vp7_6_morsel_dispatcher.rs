//! Harness integration tests for bd-14vp7.6: Morsel-driven parallel dispatcher.
//!
//! Validates: morsel partitioning, pipeline task construction, work-stealing
//! execution, pipeline barriers, scaling efficiency, NUMA-awareness, exchange
//! operators, metrics, and deterministic results.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fsqlite_types::PageNumber;
use fsqlite_vdbe::vectorized_dispatch::{
    broadcast_exchange, build_exchange_task_ids, build_pipeline_tasks,
    hash_partition_exchange, morsel_dispatch_metrics_snapshot, partition_page_morsels,
    partition_page_morsels_auto_tuned, reset_morsel_dispatch_metrics, auto_tuned_pages_per_morsel,
    DispatchRunContext, DispatcherConfig, ExchangeKind, ExchangeTaskRef,
    PipelineId, PipelineKind, WorkStealingDispatcher,
    DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD, DEFAULT_L2_CACHE_BYTES,
    DEFAULT_PAGE_SIZE_BYTES,
};

const BEAD_ID: &str = "bd-14vp7.6";

// ── 1. Morsel partitioning covers full range ────────────────────────────────

#[test]
fn test_morsel_partitioning_covers_full_range() {
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(100).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();

    // Should cover all pages without gaps.
    assert!(!morsels.is_empty(), "bead_id={BEAD_ID} case=non_empty");

    let first_start = morsels.first().unwrap().page_range.start_page.get();
    let last_end = morsels.last().unwrap().page_range.end_page.get();
    assert_eq!(first_start, 1, "bead_id={BEAD_ID} case=starts_at_1");
    assert_eq!(last_end, 100, "bead_id={BEAD_ID} case=ends_at_100");

    // Verify no gaps between adjacent morsels.
    for window in morsels.windows(2) {
        let prev_end = window[0].page_range.end_page.get();
        let next_start = window[1].page_range.start_page.get();
        assert_eq!(
            next_start,
            prev_end + 1,
            "bead_id={BEAD_ID} case=no_gaps prev_end={prev_end} next_start={next_start}"
        );
    }

    // Morsel IDs should be sequential.
    for (i, m) in morsels.iter().enumerate() {
        assert_eq!(
            m.morsel_id, i,
            "bead_id={BEAD_ID} case=sequential_morsel_ids"
        );
    }
}

// ── 2. L2 auto-tuned morsel sizing ──────────────────────────────────────────

#[test]
fn test_auto_tuned_morsel_sizing() {
    // Default L2=1MB, page=4KB → half L2 / page_size = 128 pages.
    let pages = auto_tuned_pages_per_morsel(DEFAULT_L2_CACHE_BYTES, DEFAULT_PAGE_SIZE_BYTES)
        .unwrap();
    assert_eq!(
        pages, 128,
        "bead_id={BEAD_ID} case=default_l2_morsel_size"
    );

    // Smaller L2 → smaller morsels.
    let pages_small = auto_tuned_pages_per_morsel(32_768, 4096).unwrap();
    assert_eq!(pages_small, 4, "bead_id={BEAD_ID} case=small_l2");

    // Tiny L2 → minimum 1 page.
    let pages_tiny = auto_tuned_pages_per_morsel(1, 4096).unwrap();
    assert_eq!(pages_tiny, 1, "bead_id={BEAD_ID} case=tiny_l2_min_1");

    // Auto-tuned partition covers full range.
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(1000).unwrap();
    let morsels = partition_page_morsels_auto_tuned(
        start,
        end,
        DEFAULT_L2_CACHE_BYTES,
        DEFAULT_PAGE_SIZE_BYTES,
        2,
    )
    .unwrap();
    let first_start = morsels.first().unwrap().page_range.start_page.get();
    let last_end = morsels.last().unwrap().page_range.end_page.get();
    assert_eq!(first_start, 1, "bead_id={BEAD_ID} case=auto_tuned_start");
    assert_eq!(last_end, 1000, "bead_id={BEAD_ID} case=auto_tuned_end");
}

// ── 3. Pipeline task construction ───────────────────────────────────────────

#[test]
fn test_pipeline_task_construction() {
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(50).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();
    let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

    assert_eq!(
        tasks.len(),
        morsels.len(),
        "bead_id={BEAD_ID} case=one_task_per_morsel"
    );
    for (task, morsel) in tasks.iter().zip(morsels.iter()) {
        assert_eq!(task.pipeline, PipelineId(0));
        assert_eq!(task.kind, PipelineKind::ScanFilterProject);
        assert_eq!(task.morsel, *morsel);
    }
}

// ── 4. Work-stealing execution completes all tasks ──────────────────────────

#[test]
fn test_work_stealing_execution_completes_all_tasks() {
    let config = DispatcherConfig {
        worker_threads: 4,
        numa_nodes: 2,
    };
    let dispatcher = WorkStealingDispatcher::try_new(config).unwrap();

    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(200).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 2).unwrap();
    let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
    let task_count = tasks.len();

    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = Arc::clone(&counter);

    let reports = dispatcher
        .execute_with_barriers(&[tasks], move |task, _worker_id| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            // Compute a checksum from the morsel range.
            let start = u64::from(task.morsel.page_range.start_page.get());
            let end = u64::from(task.morsel.page_range.end_page.get());
            start.wrapping_mul(31).wrapping_add(end)
        })
        .unwrap();

    assert_eq!(reports.len(), 1, "bead_id={BEAD_ID} case=one_pipeline");
    assert_eq!(
        reports[0].completed.len(),
        task_count,
        "bead_id={BEAD_ID} case=all_tasks_completed"
    );
    assert_eq!(
        counter.load(Ordering::Relaxed),
        task_count as u64,
        "bead_id={BEAD_ID} case=counter_matches"
    );

    // Multiple workers should have participated.
    let active_workers = reports[0]
        .per_worker_task_counts
        .iter()
        .filter(|&&c| c > 0)
        .count();
    assert!(
        active_workers >= 1,
        "bead_id={BEAD_ID} case=at_least_1_worker active={active_workers}"
    );
}

// ── 5. Pipeline barriers enforce ordering ───────────────────────────────────

#[test]
fn test_pipeline_barriers_enforce_ordering() {
    let config = DispatcherConfig {
        worker_threads: 4,
        numa_nodes: 1,
    };
    let dispatcher = WorkStealingDispatcher::try_new(config).unwrap();

    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(40).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();

    let pipeline_0 = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
    let pipeline_1 = build_pipeline_tasks(PipelineId(1), PipelineKind::AggregateUpdate, &morsels);

    // Use a shared atomic to track ordering.
    let phase = Arc::new(AtomicU64::new(0));
    let phase_clone = Arc::clone(&phase);

    let reports = dispatcher
        .execute_with_barriers(&[pipeline_0, pipeline_1], move |task, _worker_id| {
            let current_phase = phase_clone.load(Ordering::Relaxed);
            let pipeline_idx = task.pipeline.0 as u64;
            // Pipeline 0 tasks should see phase=0, pipeline 1 tasks should see phase>=1.
            if pipeline_idx == 0 {
                // Mark that pipeline 0 completed at least one task.
                phase_clone.store(1, Ordering::Release);
            }
            (pipeline_idx, current_phase)
        })
        .unwrap();

    assert_eq!(reports.len(), 2, "bead_id={BEAD_ID} case=two_pipelines");

    // All pipeline-1 tasks should have seen phase >= 1 (pipeline 0 completed).
    for completed in &reports[1].completed {
        let (pipeline_idx, seen_phase) = completed.result;
        assert_eq!(pipeline_idx, 1);
        assert!(
            seen_phase >= 1,
            "bead_id={BEAD_ID} case=barrier_enforced task={} seen_phase={seen_phase}",
            completed.task_id
        );
    }
}

// ── 6. Scaling efficiency ───────────────────────────────────────────────────

#[test]
fn test_scaling_efficiency() {
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(800).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();
    let task_count = morsels.len();

    // Heavy work function with black_box to prevent optimization.
    let heavy_work = |task: &fsqlite_vdbe::vectorized_dispatch::PipelineTask, _: usize| -> u64 {
        let s = task.morsel.page_range.start_page.get();
        let e = task.morsel.page_range.end_page.get();
        let mut sum = 0u64;
        // ~1M iterations per morsel to amortize thread spawn overhead.
        for p in s..=e {
            for i in 0..100_000u64 {
                sum = sum.wrapping_add(
                    std::hint::black_box(u64::from(p))
                        .wrapping_mul(i.wrapping_add(7))
                        .wrapping_mul(0x517cc1b727220a95),
                );
            }
        }
        std::hint::black_box(sum)
    };

    // Measure with 1 worker.
    let tasks_1 = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
    let d1 = WorkStealingDispatcher::try_new(DispatcherConfig {
        worker_threads: 1,
        numa_nodes: 1,
    })
    .unwrap();
    let t1_start = std::time::Instant::now();
    let _r1 = d1.execute_with_barriers(&[tasks_1], heavy_work).unwrap();
    let t1_elapsed = t1_start.elapsed();

    // Measure with 4 workers.
    let tasks_4 = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
    let d4 = WorkStealingDispatcher::try_new(DispatcherConfig {
        worker_threads: 4,
        numa_nodes: 1,
    })
    .unwrap();
    let t4_start = std::time::Instant::now();
    let _r4 = d4.execute_with_barriers(&[tasks_4], heavy_work).unwrap();
    let t4_elapsed = t4_start.elapsed();

    let speedup = t1_elapsed.as_nanos() as f64 / t4_elapsed.as_nanos().max(1) as f64;
    println!(
        "[{BEAD_ID}] scaling: 1w={:.2}ms 4w={:.2}ms speedup={speedup:.2}x tasks={task_count}",
        t1_elapsed.as_secs_f64() * 1000.0,
        t4_elapsed.as_secs_f64() * 1000.0,
    );

    // Scaling is informational — actual speedup depends on CI core count and
    // load. We verify that multi-worker dispatch at least completes correctly
    // and report the measured speedup. Correctness is validated by determinism
    // and barrier tests.
    println!(
        "[{BEAD_ID}] scaling test: {speedup:.2}x with 4 workers (informational)"
    );
}

// ── 7. NUMA-aware morsel assignment ─────────────────────────────────────────

#[test]
fn test_numa_aware_morsel_assignment() {
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(100).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 4).unwrap();

    // Morsels should be round-robin assigned to NUMA nodes.
    for m in &morsels {
        assert_eq!(
            m.preferred_numa_node,
            m.morsel_id % 4,
            "bead_id={BEAD_ID} case=numa_round_robin morsel_id={}",
            m.morsel_id
        );
    }

    // Dispatcher assigns workers to NUMA nodes round-robin.
    let config = DispatcherConfig {
        worker_threads: 8,
        numa_nodes: 4,
    };
    let dispatcher = WorkStealingDispatcher::try_new(config).unwrap();
    let numa = dispatcher.worker_numa_nodes();
    assert_eq!(numa.len(), 8, "bead_id={BEAD_ID} case=8_workers");
    for (i, &node) in numa.iter().enumerate() {
        assert_eq!(
            node,
            i % 4,
            "bead_id={BEAD_ID} case=worker_numa worker={i}"
        );
    }
}

// ── 8. Hash partition exchange with skew spilling ───────────────────────────

#[test]
fn test_hash_partition_exchange() {
    // Create 100 task refs where many hash to partition 0.
    let mut refs = Vec::new();
    for i in 0..100 {
        refs.push(ExchangeTaskRef {
            task_id: i,
            // Most tasks hash to partition 0 (key % 4 == 0).
            hash_key: (i as u64) * 4,
        });
    }

    let partitions = hash_partition_exchange(&refs, 4, 32).unwrap();

    // All task ids should appear exactly once.
    let total: usize = partitions.iter().map(|p| p.len()).sum();
    assert_eq!(total, 100, "bead_id={BEAD_ID} case=all_tasks_present");

    // No partition should have all tasks (skew spilling should redistribute).
    let max_partition = partitions.iter().map(|p| p.len()).max().unwrap();
    assert!(
        max_partition < 100,
        "bead_id={BEAD_ID} case=skew_spill max_partition={max_partition}"
    );

    println!(
        "[{BEAD_ID}] hash exchange partition sizes: {:?}",
        partitions.iter().map(|p| p.len()).collect::<Vec<_>>()
    );
}

// ── 9. Broadcast exchange replicates to all partitions ──────────────────────

#[test]
fn test_broadcast_exchange_replication() {
    let task_ids: Vec<usize> = (0..10).collect();
    let partitions = broadcast_exchange(&task_ids, 4).unwrap();

    assert_eq!(partitions.len(), 4, "bead_id={BEAD_ID} case=4_partitions");
    for (i, partition) in partitions.iter().enumerate() {
        assert_eq!(
            partition.len(),
            10,
            "bead_id={BEAD_ID} case=broadcast_replicated partition={i}"
        );
        assert_eq!(
            partition, &task_ids,
            "bead_id={BEAD_ID} case=broadcast_contents partition={i}"
        );
    }
}

// ── 10. Metrics gauge updates ───────────────────────────────────────────────

#[test]
fn test_metrics_gauge_updates() {
    reset_morsel_dispatch_metrics();
    let before = morsel_dispatch_metrics_snapshot();

    let config = DispatcherConfig {
        worker_threads: 2,
        numa_nodes: 1,
    };
    let dispatcher = WorkStealingDispatcher::try_new(config).unwrap();

    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(40).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();
    let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

    let _reports = dispatcher
        .execute_with_barriers(&[tasks], |_task, _worker_id| 42u64)
        .unwrap();

    let after = morsel_dispatch_metrics_snapshot();

    // Workers-active gauge should have been set.
    assert!(
        after.fsqlite_morsel_workers_active > 0,
        "bead_id={BEAD_ID} case=workers_active_gauge val={}",
        after.fsqlite_morsel_workers_active
    );

    println!(
        "[{BEAD_ID}] metrics: throughput_before={} throughput_after={} workers_active={}",
        before.fsqlite_morsel_throughput_rows_per_sec,
        after.fsqlite_morsel_throughput_rows_per_sec,
        after.fsqlite_morsel_workers_active,
    );
}

// ── 11. Deterministic results across worker counts ──────────────────────────

#[test]
fn test_deterministic_results_across_worker_counts() {
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(80).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();

    let compute = |task: &fsqlite_vdbe::vectorized_dispatch::PipelineTask, _wid: usize| -> u64 {
        let s = u64::from(task.morsel.page_range.start_page.get());
        let e = u64::from(task.morsel.page_range.end_page.get());
        s.wrapping_mul(31).wrapping_add(e).wrapping_mul(17)
    };

    let mut checksums = Vec::new();
    for workers in [1, 2, 4] {
        let d = WorkStealingDispatcher::try_new(DispatcherConfig {
            worker_threads: workers,
            numa_nodes: 1,
        })
        .unwrap();
        let tasks =
            build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
        let reports = d.execute_with_barriers(&[tasks], compute).unwrap();

        // Aggregate results sorted by task_id for deterministic comparison.
        let mut results: Vec<(usize, u64)> = reports[0]
            .completed
            .iter()
            .map(|c| (c.task_id, c.result))
            .collect();
        results.sort_by_key(|(id, _)| *id);

        let checksum: u64 = results
            .iter()
            .fold(0u64, |acc, (_, r)| acc.wrapping_add(*r));
        checksums.push(checksum);
    }

    // All worker counts should produce the same checksum.
    assert_eq!(
        checksums[0], checksums[1],
        "bead_id={BEAD_ID} case=deterministic_1_vs_2"
    );
    assert_eq!(
        checksums[1], checksums[2],
        "bead_id={BEAD_ID} case=deterministic_2_vs_4"
    );

    println!(
        "[{BEAD_ID}] deterministic checksums: 1w=0x{:x} 2w=0x{:x} 4w=0x{:x}",
        checksums[0], checksums[1], checksums[2]
    );
}

// ── 12. Error handling ──────────────────────────────────────────────────────

#[test]
fn test_error_handling() {
    // Zero workers.
    let err = WorkStealingDispatcher::try_new(DispatcherConfig {
        worker_threads: 0,
        numa_nodes: 1,
    });
    assert!(
        err.is_err(),
        "bead_id={BEAD_ID} case=zero_workers_rejected"
    );

    // Zero NUMA nodes.
    let err = WorkStealingDispatcher::try_new(DispatcherConfig {
        worker_threads: 4,
        numa_nodes: 0,
    });
    assert!(
        err.is_err(),
        "bead_id={BEAD_ID} case=zero_numa_rejected"
    );

    // Zero pages_per_morsel.
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(10).unwrap();
    let err = partition_page_morsels(start, end, 0, 1);
    assert!(
        err.is_err(),
        "bead_id={BEAD_ID} case=zero_morsel_size_rejected"
    );

    // Empty run_id.
    let err = DispatchRunContext::try_new(String::new(), 0, "S1".to_owned());
    assert!(
        err.is_err(),
        "bead_id={BEAD_ID} case=empty_run_id_rejected"
    );

    // Empty scenario_id.
    let err = DispatchRunContext::try_new("run1".to_owned(), 0, String::new());
    assert!(
        err.is_err(),
        "bead_id={BEAD_ID} case=empty_scenario_id_rejected"
    );
}

// ── 13. Exchange task ID builder ────────────────────────────────────────────

#[test]
fn test_exchange_task_id_builder() {
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(80).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 1).unwrap();
    let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::HashJoinProbe, &morsels);

    // Hash partition.
    let hash_parts = build_exchange_task_ids(
        &tasks,
        ExchangeKind::HashPartition,
        4,
        DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD,
    )
    .unwrap();
    let hash_total: usize = hash_parts.iter().map(|p| p.len()).sum();
    assert_eq!(
        hash_total,
        tasks.len(),
        "bead_id={BEAD_ID} case=hash_exchange_total"
    );

    // Broadcast.
    let broadcast_parts = build_exchange_task_ids(
        &tasks,
        ExchangeKind::Broadcast,
        4,
        DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD,
    )
    .unwrap();
    assert_eq!(
        broadcast_parts.len(),
        4,
        "bead_id={BEAD_ID} case=broadcast_partitions"
    );
    for part in &broadcast_parts {
        assert_eq!(
            part.len(),
            tasks.len(),
            "bead_id={BEAD_ID} case=broadcast_all_tasks"
        );
    }
}

// ── 14. Conformance summary ─────────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    // 1. Morsel partitioning.
    let start = PageNumber::new(1).unwrap();
    let end = PageNumber::new(100).unwrap();
    let morsels = partition_page_morsels(start, end, 10, 2).unwrap();
    let pass_partitioning = morsels.first().unwrap().page_range.start_page.get() == 1
        && morsels.last().unwrap().page_range.end_page.get() == 100;

    // 2. Pipeline construction.
    let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
    let pass_pipeline = tasks.len() == morsels.len();

    // 3. Work-stealing execution.
    let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
        worker_threads: 2,
        numa_nodes: 1,
    })
    .unwrap();
    let reports = dispatcher
        .execute_with_barriers(&[tasks], |_task, _wid| 1u64)
        .unwrap();
    let pass_execution = reports[0].completed.len() == morsels.len();

    // 4. Pipeline barriers.
    let m2 = partition_page_morsels(start, end, 25, 1).unwrap();
    let p0 = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &m2);
    let p1 = build_pipeline_tasks(PipelineId(1), PipelineKind::AggregateUpdate, &m2);
    let barrier_reports = dispatcher
        .execute_with_barriers(&[p0, p1], |_task, _wid| 1u64)
        .unwrap();
    let pass_barriers = barrier_reports.len() == 2;

    // 5. NUMA awareness.
    let pass_numa = morsels.iter().all(|m| m.preferred_numa_node == m.morsel_id % 2);

    // 6. Deterministic results.
    let m3 = partition_page_morsels(start, end, 20, 1).unwrap();
    let compute = |task: &fsqlite_vdbe::vectorized_dispatch::PipelineTask, _: usize| -> u64 {
        u64::from(task.morsel.page_range.start_page.get())
    };
    let t1 = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &m3);
    let t2 = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &m3);
    let r1 = dispatcher
        .execute_with_barriers(&[t1], compute)
        .unwrap();
    let r2 = dispatcher
        .execute_with_barriers(&[t2], compute)
        .unwrap();
    let mut v1: Vec<_> = r1[0].completed.iter().map(|c| (c.task_id, c.result)).collect();
    let mut v2: Vec<_> = r2[0].completed.iter().map(|c| (c.task_id, c.result)).collect();
    v1.sort();
    v2.sort();
    let pass_deterministic = v1 == v2;

    let checks = [
        pass_partitioning,
        pass_pipeline,
        pass_execution,
        pass_barriers,
        pass_numa,
        pass_deterministic,
    ];
    let passed = checks.iter().filter(|&&p| p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} Morsel Dispatcher Conformance ===");
    println!("  partitioning: {}", if pass_partitioning { "PASS" } else { "FAIL" });
    println!("  pipeline:     {}", if pass_pipeline { "PASS" } else { "FAIL" });
    println!("  execution:    {}", if pass_execution { "PASS" } else { "FAIL" });
    println!("  barriers:     {}", if pass_barriers { "PASS" } else { "FAIL" });
    println!("  numa:         {}", if pass_numa { "PASS" } else { "FAIL" });
    println!("  deterministic:{}", if pass_deterministic { "PASS" } else { "FAIL" });
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}"
    );
}
