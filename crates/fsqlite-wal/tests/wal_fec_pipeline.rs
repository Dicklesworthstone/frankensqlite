use std::path::Path;
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;
use asupersync::time::{sleep, wall_now};
use fsqlite_types::{ObjectId, Oti, cx::Cx};
use fsqlite_wal::{
    WalFecGroupMeta, WalFecGroupMetaInit, WalFecRepairPipeline, WalFecRepairPipelineConfig,
    WalFecRepairWorkItem, build_source_page_hashes, find_wal_fec_group,
    generate_wal_fec_repair_symbols, scan_wal_fec,
};
use tempfile::tempdir;

const PAGE_SIZE: u32 = 4096;

fn test_runtime() -> asupersync::runtime::Runtime {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 1)
        .build()
        .expect("runtime should build")
}

fn test_cx() -> Cx {
    Cx::default()
}

fn sample_payload(seed: u8) -> Vec<u8> {
    let page_len = usize::try_from(PAGE_SIZE).expect("PAGE_SIZE should fit in usize");
    let mut payload = vec![0_u8; page_len];
    for (index, byte) in payload.iter_mut().enumerate() {
        let index_mod = u8::try_from(index % 251).expect("modulo result should fit in u8");
        *byte = index_mod ^ seed;
    }
    payload
}

fn sample_source_pages(k_source: u32, seed_base: u8) -> Vec<Vec<u8>> {
    (0..k_source)
        .map(|index| {
            let seed = seed_base.wrapping_add(u8::try_from(index).expect("index should fit in u8"));
            sample_payload(seed)
        })
        .collect()
}

fn sample_meta_from_pages(
    start_frame_no: u32,
    r_repair: u32,
    wal_salt1: u32,
    wal_salt2: u32,
    object_tag: &[u8],
    db_size_pages: u32,
    source_pages: &[Vec<u8>],
) -> WalFecGroupMeta {
    let k_source =
        u32::try_from(source_pages.len()).expect("source page count should fit in u32 for tests");
    let end_frame_no = start_frame_no + (k_source - 1);
    let source_hashes = build_source_page_hashes(source_pages);
    let page_numbers = (0..k_source).map(|index| index + 100).collect::<Vec<_>>();
    let object_id = ObjectId::derive_from_canonical_bytes(object_tag);
    let oti = Oti {
        f: u64::from(k_source) * u64::from(PAGE_SIZE),
        al: 1,
        t: PAGE_SIZE,
        z: 1,
        n: 1,
    };
    WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1,
        wal_salt2,
        start_frame_no,
        end_frame_no,
        db_size_pages,
        page_size: PAGE_SIZE,
        k_source,
        r_repair,
        oti,
        object_id,
        page_numbers,
        source_page_xxh3_128: source_hashes,
    })
    .expect("sample metadata should be valid")
}

fn sample_work_item(
    sidecar_path: &Path,
    start_frame_no: u32,
    k_source: u32,
    r_repair: u32,
    seed_base: u8,
    object_tag: &[u8],
    db_size_pages: u32,
) -> WalFecRepairWorkItem {
    let source_pages = sample_source_pages(k_source, seed_base);
    let meta = sample_meta_from_pages(
        start_frame_no,
        r_repair,
        0x1111_2222,
        0x3333_4444,
        object_tag,
        db_size_pages,
        &source_pages,
    );
    WalFecRepairWorkItem::new(sidecar_path.to_path_buf(), meta, source_pages)
        .expect("work item should validate")
}

#[test]
fn test_bd_1hi_10_unit_compliance_gate() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("unit.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 8,
                per_symbol_delay: Duration::from_millis(2),
            },
        )
        .expect("pipeline should start");

        let work_item = sample_work_item(&sidecar_path, 1, 4, 2, 8, b"bd-1hi.10-unit", 512);
        pipeline.enqueue(work_item).expect("enqueue should succeed");

        assert!(
            pipeline.flush(&cx, Duration::from_secs(3)).await,
            "pipeline should drain within timeout"
        );
        let stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
        assert_eq!(stats.completed_jobs, 1);
        assert_eq!(stats.failed_jobs, 0);
    });
}

#[test]
fn prop_bd_1hi_10_structure_compliance() {
    for k_source in 1..=8 {
        for r_repair in 1..=4 {
            let source_pages = sample_source_pages(
                k_source,
                u8::try_from(k_source + r_repair).expect("small loop values should fit in u8"),
            );
            let meta = sample_meta_from_pages(
                10,
                r_repair,
                0xCAFE_BABE,
                0xFACE_C0DE,
                b"bd-1hi.10-prop",
                1024,
                &source_pages,
            );
            let symbols_first = generate_wal_fec_repair_symbols(&meta, &source_pages)
                .expect("generation should work");
            let symbols_second = generate_wal_fec_repair_symbols(&meta, &source_pages)
                .expect("generation should be deterministic");

            assert_eq!(
                symbols_first.len(),
                usize::try_from(r_repair).expect("small r should fit usize")
            );
            assert_eq!(symbols_first, symbols_second);
            for (index, symbol) in symbols_first.iter().enumerate() {
                assert_eq!(
                    symbol.esi,
                    meta.k_source + u32::try_from(index).expect("small index should fit u32")
                );
            }
        }
    }
}

#[test]
fn test_repair_generation_pipelined() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("pipeline.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 16,
                per_symbol_delay: Duration::from_millis(20),
            },
        )
        .expect("pipeline should start");

        for index in 0_u32..3 {
            let tag = format!("pipeline-{index}");
            let item = sample_work_item(
                &sidecar_path,
                (index * 4) + 1,
                4,
                3,
                u8::try_from(index + 7).expect("small index should fit u8"),
                tag.as_bytes(),
                2048 + index,
            );
            pipeline.enqueue(item).expect("enqueue should succeed");
        }

        sleep(wall_now(), Duration::from_millis(25)).await;
        let mid_stats = pipeline.stats();
        assert!(
            mid_stats.pending_jobs >= 1,
            "at least one queued/in-flight job should remain while worker is generating symbols"
        );
        assert!(
            mid_stats.max_pending_jobs >= 2,
            "pipeline should observe buffered jobs while processing"
        );

        assert!(
            pipeline.flush(&cx, Duration::from_secs(10)).await,
            "pipeline should eventually catch up"
        );
        let final_stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
        assert_eq!(final_stats.completed_jobs, 3);
        let scan = scan_wal_fec(&sidecar_path).expect("sidecar scan should succeed");
        assert_eq!(scan.groups.len(), 3);
        assert!(!scan.truncated_tail);
    });
}

#[test]
fn test_repair_generation_off_commit_path() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("off-path.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 8,
                per_symbol_delay: Duration::from_millis(80),
            },
        )
        .expect("pipeline should start");
        let item = sample_work_item(&sidecar_path, 1, 4, 4, 21, b"off-path", 4096);

        let enqueue_started = Instant::now();
        pipeline.enqueue(item).expect("enqueue should not block");
        let enqueue_elapsed = enqueue_started.elapsed();
        assert!(
            enqueue_elapsed < Duration::from_millis(75),
            "enqueue should stay off commit path; elapsed={enqueue_elapsed:?}"
        );
        assert_eq!(pipeline.stats().pending_jobs, 1);

        assert!(
            pipeline.flush(&cx, Duration::from_secs(10)).await,
            "pipeline should drain after asynchronous generation"
        );
        let stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
        assert_eq!(stats.completed_jobs, 1);
    });
}

#[test]
fn test_repair_generation_catches_up() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("catch-up.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 32,
                per_symbol_delay: Duration::from_millis(5),
            },
        )
        .expect("pipeline should start");

        for index in 0_u32..8 {
            let tag = format!("catchup-{index}");
            let item = sample_work_item(
                &sidecar_path,
                (index * 3) + 1,
                3,
                2,
                u8::try_from(index + 11).expect("small index should fit u8"),
                tag.as_bytes(),
                500 + index,
            );
            pipeline.enqueue(item).expect("enqueue should succeed");
        }

        assert!(
            pipeline.flush(&cx, Duration::from_secs(20)).await,
            "pipeline should catch up after burst"
        );
        let stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
        assert_eq!(stats.completed_jobs, 8);
        assert_eq!(stats.failed_jobs, 0);
        assert!(
            stats.max_pending_jobs >= 2,
            "catch-up requires queueing beyond immediate execution"
        );

        let scan = scan_wal_fec(&sidecar_path).expect("sidecar scan should succeed");
        assert_eq!(scan.groups.len(), 8);
    });
}

#[test]
fn test_repair_generation_backpressure_queue_full() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("queue-full.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 1,
                per_symbol_delay: Duration::from_millis(120),
            },
        )
        .expect("pipeline should start");

        let item_a = sample_work_item(&sidecar_path, 1, 4, 3, 31, b"queue-a", 900);
        let item_b = sample_work_item(&sidecar_path, 5, 4, 3, 33, b"queue-b", 904);
        let item_c = sample_work_item(&sidecar_path, 9, 4, 3, 35, b"queue-c", 908);

        pipeline.enqueue(item_a).expect("enqueue A should succeed");
        let second = pipeline.enqueue(item_b);
        let third = pipeline.enqueue(item_c);
        let queue_full_error = second
            .err()
            .or_else(|| third.err())
            .expect("at least one enqueue must fail from queue-full backpressure");
        assert!(
            queue_full_error.to_string().contains("queue full"),
            "expected queue-full backpressure error, got {queue_full_error}"
        );

        pipeline.cancel();
        let _ = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
    });
}

#[test]
fn test_repair_generation_shutdown_drains_pending_jobs() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("shutdown-drain.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 16,
                per_symbol_delay: Duration::from_millis(10),
            },
        )
        .expect("pipeline should start");

        for index in 0_u32..4 {
            let tag = format!("drain-{index}");
            let item = sample_work_item(
                &sidecar_path,
                (index * 4) + 1,
                4,
                2,
                u8::try_from(index + 41).expect("small index should fit u8"),
                tag.as_bytes(),
                1_200 + index,
            );
            pipeline.enqueue(item).expect("enqueue should succeed");
        }

        let stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should drain queue");
        assert_eq!(stats.completed_jobs, 4);
        assert_eq!(stats.failed_jobs, 0);
        assert_eq!(stats.canceled_jobs, 0);

        let scan = scan_wal_fec(&sidecar_path).expect("sidecar scan should succeed");
        assert_eq!(scan.groups.len(), 4);
    });
}

#[test]
#[allow(clippy::cast_precision_loss)]
fn test_repair_generation_commit_path_overhead_under_one_percent() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("throughput-window.wal-fec");
    let commits = 80_u32;
    let simulated_commit_cost = Duration::from_millis(4);
    let mut queued_work = Vec::with_capacity(usize::try_from(commits).expect("small count"));
    for index in 0..commits {
        let tag = format!("overhead-{index}");
        queued_work.push(sample_work_item(
            &sidecar_path,
            (index * 4) + 1,
            4,
            2,
            u8::try_from((index % 200) + 51).expect("small index should fit u8"),
            tag.as_bytes(),
            2_000 + index,
        ));
    }

    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let baseline_start = Instant::now();
        for _ in 0..commits {
            sleep(wall_now(), simulated_commit_cost).await;
        }
        let baseline_elapsed = baseline_start.elapsed();

        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 256,
                per_symbol_delay: Duration::from_millis(12),
            },
        )
        .expect("pipeline should start");

        let async_start = Instant::now();
        for item in queued_work {
            sleep(wall_now(), simulated_commit_cost).await;
            pipeline
                .enqueue(item)
                .expect("enqueue should remain non-blocking under bounded queue");
        }
        let async_elapsed = async_start.elapsed();

        assert!(
            pipeline.flush(&cx, Duration::from_secs(40)).await,
            "pipeline should catch up after throughput run"
        );
        let stats = pipeline.shutdown(&cx).await.expect("shutdown should succeed");
        assert_eq!(stats.failed_jobs, 0);

        let baseline_secs = baseline_elapsed.as_secs_f64().max(f64::EPSILON);
        let async_secs = async_elapsed.as_secs_f64();
        let overhead_ratio = ((async_secs - baseline_secs) / baseline_secs).max(0.0);
        assert!(
            overhead_ratio <= 0.01,
            "critical-path overhead should remain <=1%; baseline={baseline_elapsed:?} async={async_elapsed:?} overhead={:.2}%",
            overhead_ratio * 100.0
        );
    });
}

#[test]
fn test_repair_generation_cancel_safe() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("cancel.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 4,
                per_symbol_delay: Duration::from_millis(100),
            },
        )
        .expect("pipeline should start");

        let item = sample_work_item(&sidecar_path, 1, 4, 4, 17, b"cancel-safe", 777);
        pipeline.enqueue(item).expect("enqueue should succeed");
        sleep(wall_now(), Duration::from_millis(35)).await;

        pipeline.cancel();
        assert!(
            pipeline.flush(&cx, Duration::from_secs(2)).await,
            "pipeline should settle once the canceled work item drains"
        );
        let stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
        assert!(
            stats.canceled_jobs >= 1,
            "in-flight job should be canceled without partial append"
        );

        let scan = scan_wal_fec(&sidecar_path).expect("scan should succeed");
        assert!(
            scan.groups.is_empty(),
            "cancel-safe behavior must avoid partially written groups"
        );
    });
}

#[test]
fn test_pipeline_worker_inherits_parent_cancellation() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("parent-cancel.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 4,
                per_symbol_delay: Duration::from_millis(100),
            },
        )
        .expect("pipeline should start");

        let item_a = sample_work_item(&sidecar_path, 1, 4, 4, 19, b"parent-cancel-a", 1_700);
        let item_b = sample_work_item(&sidecar_path, 5, 4, 4, 23, b"parent-cancel-b", 1_704);
        pipeline.enqueue(item_a).expect("enqueue A should succeed");
        pipeline.enqueue(item_b).expect("enqueue B should succeed");
        sleep(wall_now(), Duration::from_millis(35)).await;

        cx.cancel();
        let err = pipeline
            .shutdown(&cx)
            .await
            .expect_err("shutdown should surface inherited parent cancellation");
        assert!(
            err.to_string()
                .contains("wal-fec repair worker task cancelled after processing work"),
            "expected inherited parent-cancellation failure, got {err}"
        );

        let stats = pipeline.stats();
        assert_eq!(
            stats.pending_jobs, 0,
            "queued work should be drained on failure"
        );
        assert!(
            stats.canceled_jobs >= 1,
            "in-flight or queued work should be counted as canceled after parent cancellation"
        );

        let scan = scan_wal_fec(&sidecar_path).expect("scan should succeed");
        assert!(
            scan.groups.is_empty(),
            "parent cancellation must avoid partially written wal-fec groups"
        );
    });
}

#[test]
fn test_e2e_bd_1hi_10_compliance() {
    let temp_dir = tempdir().expect("tempdir should be created");
    let sidecar_path = temp_dir.path().join("e2e.wal-fec");
    let runtime = test_runtime();
    let handle = runtime.handle();

    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 8,
                per_symbol_delay: Duration::from_millis(2),
            },
        )
        .expect("pipeline should start");

        let item_a = sample_work_item(&sidecar_path, 1, 3, 2, 1, b"e2e-a", 100);
        let item_b = sample_work_item(&sidecar_path, 4, 3, 2, 9, b"e2e-b", 103);
        let item_c = sample_work_item(&sidecar_path, 7, 3, 2, 21, b"e2e-c", 106);
        let target_group_id = item_b.meta.group_id();

        pipeline.enqueue(item_a).expect("enqueue A");
        pipeline.enqueue(item_b).expect("enqueue B");
        pipeline.enqueue(item_c).expect("enqueue C");
        assert!(
            pipeline.flush(&cx, Duration::from_secs(10)).await,
            "pipeline should fully drain in e2e run"
        );
        let stats = pipeline
            .shutdown(&cx)
            .await
            .expect("shutdown should succeed");
        assert_eq!(stats.completed_jobs, 3);

        let scan = scan_wal_fec(&sidecar_path).expect("scan should succeed");
        assert_eq!(scan.groups.len(), 3);
        assert!(!scan.truncated_tail);
        let found = find_wal_fec_group(&sidecar_path, target_group_id)
            .expect("lookup should succeed")
            .expect("target group should exist");
        assert_eq!(found.meta.group_id(), target_group_id);
        assert_eq!(
            found.repair_symbols.len(),
            usize::try_from(found.meta.r_repair).expect("small r should fit usize")
        );
    });
}
