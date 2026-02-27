//! Deterministic chain-memory benchmark scenarios for `bd-2y306.4`.

use std::{collections::HashSet, env, fs, path::PathBuf, time::Instant};

use fsqlite_mvcc::{BeginKind, GLOBAL_EBR_METRICS, MvccError, TransactionManager};
use fsqlite_types::{PageData, PageNumber, PageSize};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::json;

const BEAD_ID: &str = "bd-2y306.4";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const DEFAULT_SEED: u64 = 2_306_040_001;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_2y306_4_chain_memory_benchmarks -- --nocapture --test-threads=1";

const PAGE_BASE: u32 = 730_640;
const HOT_PAGE: u32 = PAGE_BASE;
const DEFAULT_PAGE_POOL: u32 = 10_000;
const DEFAULT_WRITERS: u32 = 100;
const DEFAULT_OPS: u32 = 4_000;
const SAMPLE_EVERY: u32 = 64;

#[derive(Debug, Clone, Copy)]
enum WorkloadKind {
    HotPage,
    Uniform,
    Zipfian,
}

#[derive(Debug, Clone)]
struct Percentiles {
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

impl Percentiles {
    fn from_samples(samples: &[u64]) -> Self {
        if samples.is_empty() {
            return Self {
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
            };
        }

        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let last = sorted.len().saturating_sub(1);

        let idx50 = last / 2;
        let idx95 = (last.saturating_mul(95)) / 100;
        let idx99 = (last.saturating_mul(99)) / 100;

        Self {
            p50: sorted[idx50],
            p95: sorted[idx95],
            p99: sorted[idx99],
            max: sorted[last],
        }
    }
}

#[derive(Debug, Clone)]
struct WorkloadMetrics {
    workload: &'static str,
    mode: &'static str,
    seed: u64,
    ops: u32,
    writers: u32,
    page_pool: u32,
    elapsed_ms: u64,
    busy_commits: u32,
    commit_latency_us: Percentiles,
    peak_chain_len: usize,
    final_max_chain_len: usize,
    peak_active_versions: usize,
    final_active_versions: usize,
    distinct_pages_touched: usize,
    memory_overhead_ratio: f64,
    gc_freed_delta: u64,
    gc_blocked_delta: u64,
    max_chain_length_observed: u64,
    avg_chain_length_observed: f64,
    active_series: Vec<(u32, usize)>,
}

#[derive(Debug, Clone)]
struct WorkloadComparison {
    workload: &'static str,
    bounded: WorkloadMetrics,
    unbounded: WorkloadMetrics,
}

#[derive(Debug, Clone)]
struct LongReaderMetrics {
    seed: u64,
    ops_per_phase: u32,
    chain_before_release: usize,
    chain_after_release: usize,
    busy_before_release: u32,
    busy_after_release: u32,
    gc_freed_delta: u64,
    gc_blocked_delta: u64,
    elapsed_ms: u64,
}

fn page_size() -> PageSize {
    PageSize::new(4096).expect("fixed page size must be valid")
}

fn test_data(byte: u8) -> PageData {
    let mut data = PageData::zeroed(page_size());
    data.as_bytes_mut()[0] = byte;
    data
}

fn page_from_index(idx: u32) -> PageNumber {
    PageNumber::new(idx).expect("page number must be valid")
}

fn workload_name(kind: WorkloadKind) -> &'static str {
    match kind {
        WorkloadKind::HotPage => "single_hot_page",
        WorkloadKind::Uniform => "uniform_random",
        WorkloadKind::Zipfian => "zipfian_skewed",
    }
}

fn build_zipf_cdf(page_pool: u32, s: f64) -> Vec<f64> {
    let mut weights = Vec::with_capacity(usize::try_from(page_pool).expect("pool fits usize"));
    let mut sum = 0.0_f64;

    for rank in 1..=page_pool {
        let weight = 1.0_f64 / (f64::from(rank)).powf(s);
        weights.push(weight);
        sum += weight;
    }

    let mut cdf = Vec::with_capacity(weights.len());
    let mut running = 0.0_f64;
    for weight in weights {
        running += weight / sum;
        cdf.push(running.min(1.0));
    }

    cdf
}

fn sample_zipf_index(cdf: &[f64], rng: &mut StdRng) -> usize {
    let draw = rng.gen_range(0.0_f64..1.0_f64);
    match cdf.binary_search_by(|probe| probe.total_cmp(&draw)) {
        Ok(idx) => idx,
        Err(idx) => idx.min(cdf.len().saturating_sub(1)),
    }
}

fn sample_active_versions(
    mgr: &TransactionManager,
    touched_pages: &HashSet<u32>,
) -> (usize, usize) {
    let mut total = 0_usize;
    let mut max_chain = 0_usize;

    for &page_idx in touched_pages {
        let pgno = page_from_index(page_idx);
        let chain_len = mgr.version_store().chain_length(pgno);
        total = total.saturating_add(chain_len);
        max_chain = max_chain.max(chain_len);
    }

    (total, max_chain)
}

fn run_workload(kind: WorkloadKind, bounded: bool, seed: u64) -> WorkloadMetrics {
    let mode = if bounded {
        "ebr_bounded"
    } else {
        "no_ebr_control"
    };
    let max_chain = if bounded { 64 } else { 1_000_000 };
    let warn_chain = if bounded { 32 } else { 500_000 };

    let mut mgr = TransactionManager::new(page_size());
    mgr.set_busy_timeout_ms(2);
    mgr.set_max_chain_length(max_chain);
    mgr.set_chain_length_warning(warn_chain);

    let zipf_cdf = build_zipf_cdf(DEFAULT_PAGE_POOL, 1.15);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut touched_pages = HashSet::new();
    let mut commit_latency_us = Vec::with_capacity(usize::try_from(DEFAULT_OPS).expect("ops fits"));
    let mut busy_commits = 0_u32;
    let mut peak_chain_len = 0_usize;
    let mut peak_active_versions = 0_usize;
    let mut active_series = Vec::new();

    let before = GLOBAL_EBR_METRICS.snapshot();
    let started = Instant::now();

    for step in 0..DEFAULT_OPS {
        let page_idx = match kind {
            WorkloadKind::HotPage => HOT_PAGE,
            WorkloadKind::Uniform => {
                let offset = rng.gen_range(0..DEFAULT_PAGE_POOL);
                PAGE_BASE.saturating_add(offset)
            }
            WorkloadKind::Zipfian => {
                let sampled = sample_zipf_index(&zipf_cdf, &mut rng);
                let sampled_u32 = u32::try_from(sampled).expect("zipf index fits u32");
                PAGE_BASE.saturating_add(sampled_u32)
            }
        };

        let pgno = page_from_index(page_idx);
        touched_pages.insert(page_idx);

        let mut txn = mgr
            .begin(BeginKind::Concurrent)
            .expect("begin concurrent writer");
        let byte = u8::try_from(step % 251).expect("modulo bounds for u8");
        mgr.write_page(&mut txn, pgno, test_data(byte))
            .expect("write page");

        let commit_start = Instant::now();
        match mgr.commit(&mut txn) {
            Ok(_) => {}
            Err(MvccError::Busy) => {
                busy_commits = busy_commits.saturating_add(1);
            }
            Err(other) => panic!("unexpected commit error: {other:?}"),
        }
        let commit_elapsed = commit_start.elapsed();
        let commit_us = u64::try_from(commit_elapsed.as_micros()).unwrap_or(u64::MAX);
        commit_latency_us.push(commit_us);

        let chain_len = mgr.version_store().chain_length(pgno);
        peak_chain_len = peak_chain_len.max(chain_len);

        if step % SAMPLE_EVERY == 0 || step.saturating_add(1) == DEFAULT_OPS {
            let (active_versions, _) = sample_active_versions(&mgr, &touched_pages);
            peak_active_versions = peak_active_versions.max(active_versions);
            active_series.push((step.saturating_add(1), active_versions));
        }
    }

    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let after = GLOBAL_EBR_METRICS.snapshot();

    let (final_active_versions, final_max_chain_len) = sample_active_versions(&mgr, &touched_pages);
    let distinct_pages = touched_pages.len();
    let memory_overhead_ratio = if distinct_pages == 0 {
        0.0
    } else {
        final_active_versions as f64 / distinct_pages as f64
    };

    WorkloadMetrics {
        workload: workload_name(kind),
        mode,
        seed,
        ops: DEFAULT_OPS,
        writers: DEFAULT_WRITERS,
        page_pool: DEFAULT_PAGE_POOL,
        elapsed_ms,
        busy_commits,
        commit_latency_us: Percentiles::from_samples(&commit_latency_us),
        peak_chain_len,
        final_max_chain_len,
        peak_active_versions,
        final_active_versions,
        distinct_pages_touched: distinct_pages,
        memory_overhead_ratio,
        gc_freed_delta: after.gc_freed_count.saturating_sub(before.gc_freed_count),
        gc_blocked_delta: after
            .gc_blocked_count
            .saturating_sub(before.gc_blocked_count),
        max_chain_length_observed: after.max_chain_length_observed,
        avg_chain_length_observed: after.avg_chain_length(),
        active_series,
    }
}

fn run_comparison(kind: WorkloadKind, base_seed: u64) -> WorkloadComparison {
    let bounded = run_workload(kind, true, base_seed ^ 0xB0_00_u64);
    let unbounded = run_workload(kind, false, base_seed ^ 0x0B_00_u64);

    WorkloadComparison {
        workload: workload_name(kind),
        bounded,
        unbounded,
    }
}

fn run_long_reader_scenario(seed: u64) -> LongReaderMetrics {
    let mut mgr = TransactionManager::new(page_size());
    mgr.set_busy_timeout_ms(2);
    mgr.set_max_chain_length(64);
    mgr.set_chain_length_warning(32);

    let mut rng = StdRng::seed_from_u64(seed);
    let hot_pgno = page_from_index(HOT_PAGE.saturating_add(40_000));

    let mut seed_txn = mgr.begin(BeginKind::Concurrent).expect("seed begin");
    mgr.write_page(&mut seed_txn, hot_pgno, test_data(0x11))
        .expect("seed write");
    mgr.commit(&mut seed_txn).expect("seed commit");

    let mut reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
    let _ = mgr.read_page(&mut reader, hot_pgno).expect("reader pin");

    let before = GLOBAL_EBR_METRICS.snapshot();
    let started = Instant::now();

    let mut busy_before_release = 0_u32;
    let mut busy_after_release = 0_u32;
    let ops_per_phase = 512_u32;

    for step in 0..ops_per_phase {
        let mut writer = mgr
            .begin(BeginKind::Concurrent)
            .expect("writer begin phase1");
        let byte = u8::try_from((step + rng.gen_range(1..17)) % 251).expect("u8 bounded");
        mgr.write_page(&mut writer, hot_pgno, test_data(byte))
            .expect("writer write phase1");
        if let Err(MvccError::Busy) = mgr.commit(&mut writer) {
            busy_before_release = busy_before_release.saturating_add(1);
        }
    }

    let chain_before_release = mgr.version_store().chain_length(hot_pgno);

    mgr.abort(&mut reader);

    for step in 0..ops_per_phase {
        let mut writer = mgr
            .begin(BeginKind::Concurrent)
            .expect("writer begin phase2");
        let byte = u8::try_from((step + rng.gen_range(5..29)) % 251).expect("u8 bounded");
        mgr.write_page(&mut writer, hot_pgno, test_data(byte))
            .expect("writer write phase2");
        if let Err(MvccError::Busy) = mgr.commit(&mut writer) {
            busy_after_release = busy_after_release.saturating_add(1);
        }
    }

    let chain_after_release = mgr.version_store().chain_length(hot_pgno);
    let after = GLOBAL_EBR_METRICS.snapshot();

    LongReaderMetrics {
        seed,
        ops_per_phase,
        chain_before_release,
        chain_after_release,
        busy_before_release,
        busy_after_release,
        gc_freed_delta: after.gc_freed_count.saturating_sub(before.gc_freed_count),
        gc_blocked_delta: after
            .gc_blocked_count
            .saturating_sub(before.gc_blocked_count),
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    }
}

fn metrics_to_json(metrics: &WorkloadMetrics) -> serde_json::Value {
    json!({
        "workload": metrics.workload,
        "mode": metrics.mode,
        "seed": metrics.seed,
        "ops": metrics.ops,
        "writers": metrics.writers,
        "page_pool": metrics.page_pool,
        "elapsed_ms": metrics.elapsed_ms,
        "busy_commits": metrics.busy_commits,
        "commit_latency_us": {
            "p50": metrics.commit_latency_us.p50,
            "p95": metrics.commit_latency_us.p95,
            "p99": metrics.commit_latency_us.p99,
            "max": metrics.commit_latency_us.max
        },
        "peak_chain_len": metrics.peak_chain_len,
        "final_max_chain_len": metrics.final_max_chain_len,
        "peak_active_versions": metrics.peak_active_versions,
        "final_active_versions": metrics.final_active_versions,
        "distinct_pages_touched": metrics.distinct_pages_touched,
        "memory_overhead_ratio": metrics.memory_overhead_ratio,
        "gc_freed_delta": metrics.gc_freed_delta,
        "gc_blocked_delta": metrics.gc_blocked_delta,
        "max_chain_length_observed": metrics.max_chain_length_observed,
        "avg_chain_length_observed": metrics.avg_chain_length_observed,
        "active_series": metrics.active_series,
    })
}

fn comparison_to_json(comparison: &WorkloadComparison) -> serde_json::Value {
    json!({
        "workload": comparison.workload,
        "bounded": metrics_to_json(&comparison.bounded),
        "unbounded": metrics_to_json(&comparison.unbounded),
    })
}

fn long_reader_to_json(metrics: &LongReaderMetrics) -> serde_json::Value {
    json!({
        "seed": metrics.seed,
        "ops_per_phase": metrics.ops_per_phase,
        "chain_before_release": metrics.chain_before_release,
        "chain_after_release": metrics.chain_after_release,
        "busy_before_release": metrics.busy_before_release,
        "busy_after_release": metrics.busy_after_release,
        "gc_freed_delta": metrics.gc_freed_delta,
        "gc_blocked_delta": metrics.gc_blocked_delta,
        "elapsed_ms": metrics.elapsed_ms,
    })
}

#[test]
fn bd_2y306_4_workloads_plateau_vs_unbounded_control() {
    let run_id = "bd-2y306.4-workload-comparison";
    let trace_id = 2_306_040_111_u64;
    let scenario_id = "CHAIN-MEMORY-BENCH-WORKLOADS";

    let hot = run_comparison(WorkloadKind::HotPage, DEFAULT_SEED ^ 0x10);
    let uniform = run_comparison(WorkloadKind::Uniform, DEFAULT_SEED ^ 0x20);
    let zipf = run_comparison(WorkloadKind::Zipfian, DEFAULT_SEED ^ 0x30);

    assert!(
        hot.bounded.peak_active_versions < hot.unbounded.peak_active_versions,
        "bead_id={BEAD_ID} case=hot_page_peak_not_reduced run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} bounded_peak={} unbounded_peak={}",
        hot.bounded.peak_active_versions,
        hot.unbounded.peak_active_versions
    );

    for comparison in [&hot, &uniform, &zipf] {
        assert!(
            comparison.bounded.final_active_versions <= comparison.unbounded.final_active_versions,
            "bead_id={BEAD_ID} case=final_active_not_bounded run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} workload={} bounded_final={} unbounded_final={}",
            comparison.workload,
            comparison.bounded.final_active_versions,
            comparison.unbounded.final_active_versions
        );
        assert!(
            comparison.bounded.memory_overhead_ratio <= comparison.unbounded.memory_overhead_ratio,
            "bead_id={BEAD_ID} case=memory_overhead_ratio_not_improved run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} workload={} bounded_ratio={} unbounded_ratio={}",
            comparison.workload,
            comparison.bounded.memory_overhead_ratio,
            comparison.unbounded.memory_overhead_ratio
        );
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={} hot_peak_bounded={} hot_peak_unbounded={} uniform_peak_bounded={} uniform_peak_unbounded={} zipf_peak_bounded={} zipf_peak_unbounded={} log_standard_ref={LOG_STANDARD_REF}",
        DEFAULT_SEED,
        hot.bounded.peak_active_versions,
        hot.unbounded.peak_active_versions,
        uniform.bounded.peak_active_versions,
        uniform.unbounded.peak_active_versions,
        zipf.bounded.peak_active_versions,
        zipf.unbounded.peak_active_versions,
    );
}

#[test]
fn bd_2y306_4_long_reader_impact_documented_and_recovery_bounded() {
    let run_id = "bd-2y306.4-long-reader";
    let trace_id = 2_306_040_112_u64;
    let scenario_id = "CHAIN-MEMORY-BENCH-LONG-READER";

    let metrics = run_long_reader_scenario(DEFAULT_SEED ^ 0x40);

    assert!(
        metrics.busy_before_release > 0,
        "bead_id={BEAD_ID} case=expected_backpressure_with_pinned_reader run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} busy_before_release={}",
        metrics.busy_before_release
    );
    assert!(
        metrics.chain_after_release <= 64,
        "bead_id={BEAD_ID} case=chain_not_rebounded_after_reader_release run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} chain_after_release={}",
        metrics.chain_after_release
    );
    assert!(
        metrics.gc_freed_delta > 0,
        "bead_id={BEAD_ID} case=expected_gc_reclamation_after_reader_release run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} gc_freed_delta={}",
        metrics.gc_freed_delta
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={} chain_before_release={} chain_after_release={} busy_before_release={} busy_after_release={} gc_freed_delta={} gc_blocked_delta={} log_standard_ref={LOG_STANDARD_REF}",
        metrics.seed,
        metrics.chain_before_release,
        metrics.chain_after_release,
        metrics.busy_before_release,
        metrics.busy_after_release,
        metrics.gc_freed_delta,
        metrics.gc_blocked_delta,
    );
}

#[test]
fn bd_2y306_4_chain_memory_benchmark_e2e_replay_emits_artifact() {
    let seed = env::var("SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED);
    let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| seed.to_string());
    let run_id = env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-seed-{seed}"));
    let scenario_id =
        env::var("SCENARIO_ID").unwrap_or_else(|_| "CHAIN-MEMORY-BENCH-E2E".to_owned());

    let hot = run_comparison(WorkloadKind::HotPage, seed ^ 0x101);
    let uniform = run_comparison(WorkloadKind::Uniform, seed ^ 0x202);
    let zipf = run_comparison(WorkloadKind::Zipfian, seed ^ 0x303);
    let long_reader = run_long_reader_scenario(seed ^ 0x404);

    assert!(
        hot.bounded.peak_active_versions < hot.unbounded.peak_active_versions,
        "bead_id={BEAD_ID} case=e2e_hot_page_peak_not_reduced run_id={run_id} trace_id={trace_id} scenario_id={scenario_id}"
    );
    assert!(
        long_reader.chain_after_release <= 64,
        "bead_id={BEAD_ID} case=e2e_long_reader_recovery_not_bounded run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} chain_after_release={}",
        long_reader.chain_after_release
    );

    if let Ok(path) = env::var("FSQLITE_CHAIN_MEMORY_BENCH_ARTIFACT") {
        let artifact_path = PathBuf::from(path);
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).expect("create artifact dir");
        }

        let artifact = json!({
            "bead_id": BEAD_ID,
            "run_id": run_id,
            "trace_id": trace_id,
            "scenario_id": scenario_id,
            "seed": seed,
            "log_standard_ref": LOG_STANDARD_REF,
            "overall_status": "pass",
            "replay_command": REPLAY_COMMAND,
            "workload_comparisons": [
                comparison_to_json(&hot),
                comparison_to_json(&uniform),
                comparison_to_json(&zipf),
            ],
            "long_reader": long_reader_to_json(&long_reader),
        });

        let payload = serde_json::to_vec_pretty(&artifact).expect("serialize artifact");
        fs::write(&artifact_path, payload).expect("write artifact");
        eprintln!(
            "DEBUG bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} artifact_path={} replay_command={REPLAY_COMMAND}",
            artifact_path.display()
        );
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} hot_peak_bounded={} hot_peak_unbounded={} long_reader_chain_before={} long_reader_chain_after={} long_reader_busy_before={} long_reader_busy_after={} log_standard_ref={LOG_STANDARD_REF}",
        hot.bounded.peak_active_versions,
        hot.unbounded.peak_active_versions,
        long_reader.chain_before_release,
        long_reader.chain_after_release,
        long_reader.busy_before_release,
        long_reader.busy_after_release,
    );
}
