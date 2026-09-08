use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use asupersync::runtime::RuntimeBuilder;
use asupersync::time::{sleep, wall_now};
use fsqlite_types::{ObjectId, Oti, cx::Cx};
use fsqlite_wal::{
    WalFecGroupMeta, WalFecGroupMetaInit, WalFecRepairPipeline, WalFecRepairPipelineConfig,
    WalFecRepairWorkItem, build_source_page_hashes, find_wal_fec_group,
    generate_wal_fec_repair_symbols, persist_wal_fec_raptorq_repair_symbols, scan_wal_fec,
};
use tempfile::tempdir;

const PAGE_SIZE: u32 = 4096;

#[derive(Clone, Default)]
struct RepairLogCapture {
    events: Arc<Mutex<Vec<RepairLogEvent>>>,
}

struct RepairLogEvent {
    level: tracing::Level,
    fields: BTreeMap<String, String>,
}

impl tracing::field::Visit for RepairLogEvent {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for RepairLogCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "fsqlite_wal::wal_fec"
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut captured = RepairLogEvent {
            level: *event.metadata().level(),
            fields: BTreeMap::new(),
        };
        event.record(&mut captured);
        self.events.lock().unwrap().push(captured);
    }
}

#[test]
fn repair_pipeline_emits_bounded_diagnostics_with_exact_totals() {
    const CHILD_ENV: &str = "FSQLITE_WAL_FEC_LOG_CAPTURE_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        // A process-wide collector sees the blocking-pool thread as well as
        // the async worker, without changing other tests' tracing or timing.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "repair_pipeline_emits_bounded_diagnostics_with_exact_totals",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let capture = RepairLogCapture::default();
    tracing::subscriber::set_global_default(capture.clone()).unwrap();
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("diagnostics.wal-fec");
    persist_wal_fec_raptorq_repair_symbols(&path, 1).unwrap();
    let runtime = test_runtime();
    let handle = runtime.handle();
    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 1,
                per_symbol_delay: Duration::ZERO,
            },
        )
        .unwrap();
        let producer = pipeline.producer().unwrap();
        let permit = producer.try_reserve().unwrap();
        for _ in 0..32 {
            assert!(matches!(
                producer.try_reserve(),
                Err(fsqlite_error::FrankenError::Busy)
            ));
        }
        assert_eq!(
            pipeline.stats().pending_jobs,
            0,
            "reserved capacity is not durable backlog"
        );
        // Hold capacity without runnable work: neither producer admission nor
        // standalone enqueue can race the worker into an available slot.
        assert!(
            pipeline
                .enqueue(sample_work_item(&path, 65, 1, 1, 4, b"log-capture", 512))
                .is_err()
        );
        drop(permit);
        for frame in 1..=65 {
            pipeline
                .enqueue(sample_work_item(&path, frame, 1, 1, 4, b"log-capture", 512))
                .unwrap();
            assert!(pipeline.flush(&cx, Duration::from_secs(30)).await);
        }
        let mut invalid = sample_work_item(&path, 66, 1, 1, 4, b"log-capture", 512);
        invalid.source_pages.clear();
        pipeline.enqueue(invalid).unwrap();
        let stats = pipeline.shutdown(&cx).await.unwrap();
        assert_eq!(stats.completed_jobs, 65);
        assert_eq!(stats.failed_jobs, 1);
        assert_eq!(stats.pending_jobs, 0);
    });
    assert_eq!(scan_wal_fec(&path).unwrap().groups.len(), 65);
    let events = capture.events.lock().unwrap();
    let matching = |message: &str| {
        events
            .iter()
            .filter(|event| {
                event
                    .fields
                    .get("message")
                    .is_some_and(|value| value == message)
            })
            .collect::<Vec<_>>()
    };
    let groups = matching("wal-fec group repair generation completed");
    assert_eq!(groups.len(), 2, "DEBUG samples the first and 64th group");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.level == tracing::Level::DEBUG)
            .count(),
        groups.len(),
        "lower-level helpers must not emit an unsampled DEBUG event per group"
    );
    for (event, total) in groups.iter().zip(["1", "64"]) {
        assert_eq!(event.level, tracing::Level::DEBUG);
        assert_eq!(event.fields["processed_groups"], total);
        assert_eq!(event.fields["source_frames"], "1");
        assert_eq!(event.fields["repair_symbols"], "1");
        assert!(event.fields.contains_key("group_id"));
    }
    let pressure =
        matching("wal-fec repair admission capacity exhausted (including reserved slots)");
    assert_eq!(
        pressure.len(),
        6,
        "WARN retries are sampled at powers of two"
    );
    for (event, total) in pressure.iter().zip(["1", "2", "4", "8", "16", "32"]) {
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.fields["rejected_admissions"], total);
        assert_eq!(event.fields["pending_jobs"], "0");
        assert_eq!(event.fields["queue_capacity"], "1");
    }
    let backlog =
        matching("wal-fec repair backlog near queue limit (pending includes in-flight work)");
    assert!(!backlog.is_empty());
    assert_eq!(backlog[0].level, tracing::Level::WARN);
    assert_eq!(backlog[0].fields["pending_jobs"], "1");
    let summaries = matching("wal-fec repair worker throughput summary");
    assert_eq!(
        summaries.len(),
        2,
        "INFO reports periodic and final totals, not each job"
    );
    let settings = matching("persisted wal-fec repair symbol setting (new file)");
    assert_eq!(settings.len(), 1);
    assert_eq!(settings[0].level, tracing::Level::INFO);
    assert_eq!(settings[0].fields["raptorq_repair_symbols"], "1");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.level == tracing::Level::INFO)
            .count(),
        summaries.len() + settings.len(),
        "INFO is limited to settings and summaries, including lower-level helpers"
    );
    assert_eq!(summaries[0].fields["completed_jobs"], "64");
    assert_eq!(summaries[0].fields["final_summary"], "false");
    let final_event = summaries[1];
    assert_eq!(final_event.level, tracing::Level::INFO);
    for (field, value) in [
        ("completed_jobs", "65"),
        ("failed_jobs", "1"),
        ("canceled_jobs", "0"),
        ("pending_jobs", "0"),
        ("processed_groups", "65"),
        ("rejected_admissions", "33"),
        ("final_summary", "true"),
    ] {
        assert_eq!(final_event.fields[field], value);
    }
    let rate = final_event.fields["completed_jobs_per_second"]
        .parse::<f64>()
        .unwrap();
    let seconds = final_event.fields["elapsed_seconds"]
        .parse::<f64>()
        .unwrap();
    assert!(rate.is_finite() && rate > 0.0 && seconds > 0.0);
    assert!(rate.mul_add(seconds, -65.0).abs() < 1e-6);
    let failures = matching("wal-fec repair work item failed");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].level, tracing::Level::ERROR);
    assert!(failures[0].fields.contains_key("error"));
    for event in groups
        .iter()
        .chain(&pressure)
        .chain(&backlog)
        .chain(&summaries)
        .chain(&failures)
    {
        assert_eq!(
            event.fields["pipeline_id"],
            final_event.fields["pipeline_id"]
        );
    }
    assert!(matching("wal-fec repair work item completed").is_empty());
}

#[test]
fn repair_pipeline_refuses_inline_blocking_fallback() {
    let runtime = RuntimeBuilder::current_thread().build().unwrap();
    let handle = runtime.handle();
    runtime.block_on(async {
        let result =
            WalFecRepairPipeline::start(&handle, &test_cx(), WalFecRepairPipelineConfig::default());
        assert!(matches!(
            result,
            Err(fsqlite_error::FrankenError::BackgroundWorkerFailed(detail))
                if detail.contains("caller-owned runtime blocking pool")
        ));
    });
}

#[test]
fn durable_admission_reserves_before_sync_and_abort_releases_capacity() {
    let runtime = test_runtime();
    let handle = runtime.handle();
    runtime.block_on(async {
        let cx = test_cx();
        let mut pipeline = WalFecRepairPipeline::start(
            &handle,
            &cx,
            WalFecRepairPipelineConfig {
                queue_capacity: 1,
                per_symbol_delay: Duration::ZERO,
            },
        )
        .unwrap();
        let producer = pipeline.producer().unwrap();
        let permit = producer.try_reserve().unwrap();
        assert!(matches!(
            producer.try_reserve(),
            Err(fsqlite_error::FrankenError::Busy)
        ));
        assert_eq!(
            pipeline.stats().pending_jobs,
            0,
            "a reservation is not durable work"
        );
        drop(permit); // Models failed WAL fsync; no work may be submitted.
        drop(
            producer
                .try_reserve()
                .expect("aborted reservation releases capacity"),
        );
        let stats = pipeline.shutdown(&cx).await.unwrap();
        assert_eq!(stats.completed_jobs, 0);
        assert!(
            producer.try_reserve().is_err(),
            "retained endpoints cannot submit after shutdown"
        );
    });
}

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
