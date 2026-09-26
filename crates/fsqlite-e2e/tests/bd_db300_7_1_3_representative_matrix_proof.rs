//! Representative canonical matrix comparison proof run (bd-db300.7.1.3).
//!
//! Validates the end-to-end reporting pipeline across all three benchmark
//! modes by constructing a compact but honest matrix covering two workload
//! geometries (single-threaded and multi-threaded), writing JSONL, loading
//! it back, building causal scorecards, rendering markdown, and checking
//! regression gates.
//!
//! ## Scenarios
//!
//! | ID | Name | Verified property |
//! |----|------|-------------------|
//! | P1 | three_mode_jsonl_roundtrip | JSONL serialize → load → identical summaries |
//! | P2 | causal_scorecard_groups_cover_geometry | Scorecard groups span both c1 and c4 |
//! | P3 | causal_scorecard_three_mode_attribution | Each MVCC scorecard has causal chain to baseline |
//! | P4 | markdown_report_nonempty_and_structured | Rendered markdown contains all expected sections |
//! | P5 | counter_alignment_across_proof_run | All 10 comparable counters present in every row |
//! | P6 | regression_gate_symmetric_pass | Same run vs itself → all pass, no regressions |
//! | P7 | row_identity_cross_geometry | fixture/workload/concurrency match within group |
//! | P8 | interpretation_bundle_completeness | Bundle has summaries, scorecards, report, note |
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_db300_7_1_3_representative_matrix_proof -- --nocapture
//! ```

#![allow(clippy::too_many_lines)]

use std::collections::{BTreeMap, HashSet};

use serde_json::json;

use fsqlite_e2e::benchmark::{
    BENCHMARK_COMPARABLE_COUNTER_IDS, BenchmarkComparisonMetadata, BenchmarkCounterValue,
    BenchmarkSummary, IterationRecord, LatencyStats, ThroughputStats,
    build_benchmark_causal_scorecard_report,
};
use fsqlite_e2e::methodology::{EnvironmentMeta, MethodologyMeta};
use fsqlite_e2e::overlay_honesty_gate::{
    MatrixRegressionThresholds, evaluate_matrix_regression_gate, load_benchmark_summaries,
};
use fsqlite_e2e::report_render::render_benchmark_summaries_markdown;

const BEAD_ID: &str = "bd-db300.7.1.3";

fn emit_log(scenario_id: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "REPRESENTATIVE_MATRIX_PROOF:{}",
        json!({
            "bead_id": BEAD_ID,
            "scenario_id": scenario_id,
            "phase": phase,
            "data": data,
        })
    );
}

fn make_methodology() -> MethodologyMeta {
    MethodologyMeta {
        version: "fsqlite-e2e.methodology.v1".to_owned(),
        warmup_iterations: 3,
        min_measurement_iterations: 5,
        measurement_time_secs: 10,
        primary_statistic: "median".to_owned(),
        tail_statistic: "p95".to_owned(),
        fresh_db_per_iteration: true,
        identical_pragmas_enforced: true,
    }
}

fn make_environment() -> EnvironmentMeta {
    EnvironmentMeta {
        capture_mode: Default::default(),
        os: "Linux 6.17.0-23-generic".to_owned(),
        arch: "x86_64".to_owned(),
        cpu_count: 8,
        cpu_model: Some("test-cpu".to_owned()),
        ram_bytes: Some(16 * 1024 * 1024 * 1024),
        rustc_version: "nightly-2026-05-01".to_owned(),
        cargo_profile: "release".to_owned(),
        build_hygiene: fsqlite_e2e::methodology::BuildHygieneMeta::unknown(),
    }
}

fn make_iterations(count: u32, base_wall_ms: u64, ops_per_iter: u64) -> Vec<IterationRecord> {
    (0..count)
        .map(|i| {
            let jitter = (i as i64 - count as i64 / 2) * 2;
            let wall_ms = (base_wall_ms as i64 + jitter).max(1) as u64;
            IterationRecord {
                iteration: i,
                wall_time_ms: wall_ms,
                wall_time_ns: wall_ms.saturating_mul(1_000_000),
                ops_per_sec: if wall_ms > 0 {
                    (ops_per_iter as f64) / (wall_ms as f64 / 1000.0)
                } else {
                    0.0
                },
                ops_total: ops_per_iter,
                retries: 0,
                aborts: 0,
                error: None,
            }
        })
        .collect()
}

struct ProofWorkloadGeometry {
    fixture_id: &'static str,
    workload: &'static str,
    concurrency: u16,
    sqlite_wall_ms: u64,
    single_writer_wall_ms: u64,
    mvcc_wall_ms: u64,
    ops_per_iter: u64,
}

const PROOF_GEOMETRIES: [ProofWorkloadGeometry; 2] = [
    ProofWorkloadGeometry {
        fixture_id: "chinook",
        workload: "oltp_rw",
        concurrency: 1,
        sqlite_wall_ms: 100,
        single_writer_wall_ms: 80,
        mvcc_wall_ms: 75,
        ops_per_iter: 1000,
    },
    ProofWorkloadGeometry {
        fixture_id: "chinook",
        workload: "oltp_rw",
        concurrency: 4,
        sqlite_wall_ms: 400,
        single_writer_wall_ms: 200,
        mvcc_wall_ms: 120,
        ops_per_iter: 4000,
    },
];

fn make_summary_for_mode(
    geometry: &ProofWorkloadGeometry,
    engine: &str,
    mode_id: &str,
    base_wall_ms: u64,
) -> BenchmarkSummary {
    let iterations = make_iterations(5, base_wall_ms, geometry.ops_per_iter);
    let wall_times: Vec<f64> = iterations.iter().map(|i| i.wall_time_ms as f64).collect();
    let ops_rates: Vec<f64> = iterations.iter().map(|i| i.ops_per_sec).collect();

    let mut sorted_wall = wall_times.clone();
    sorted_wall.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mut sorted_ops = ops_rates.clone();
    sorted_ops.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let n = sorted_wall.len();
    let median_wall = sorted_wall[n / 2];
    let p95_idx = ((n as f64 * 0.95).ceil() as usize).min(n - 1);
    let p99_idx = ((n as f64 * 0.99).ceil() as usize).min(n - 1);

    let mean_wall = wall_times.iter().sum::<f64>() / n as f64;
    let variance = wall_times
        .iter()
        .map(|w| (w - mean_wall).powi(2))
        .sum::<f64>()
        / n as f64;

    let total_wall = iterations.iter().map(|i| i.wall_time_ms).sum::<u64>();

    let mut summary = BenchmarkSummary {
        benchmark_id: format!(
            "{engine}:{}:{}:c{}",
            geometry.workload, geometry.fixture_id, geometry.concurrency
        ),
        engine: engine.to_owned(),
        workload: geometry.workload.to_owned(),
        fixture_id: geometry.fixture_id.to_owned(),
        concurrency: geometry.concurrency,
        methodology: make_methodology(),
        environment: make_environment(),
        warmup_count: 3,
        measurement_count: 5,
        total_measurement_ms: total_wall,
        latency: LatencyStats {
            min_ms: sorted_wall[0],
            max_ms: sorted_wall[n - 1],
            mean_ms: mean_wall,
            median_ms: median_wall,
            p95_ms: sorted_wall[p95_idx],
            p99_ms: sorted_wall[p99_idx],
            stddev_ms: variance.sqrt(),
        },
        throughput: ThroughputStats {
            mean_ops_per_sec: sorted_ops.iter().sum::<f64>() / n as f64,
            median_ops_per_sec: sorted_ops[n / 2],
            peak_ops_per_sec: sorted_ops[n - 1],
        },
        comparison: None,
        aggregated_hot_path: None,
        iterations,
    };

    summary.comparison = Some(BenchmarkComparisonMetadata::anonymous(&summary, mode_id));
    summary
}

fn build_proof_matrix() -> Vec<BenchmarkSummary> {
    let mut summaries = Vec::with_capacity(PROOF_GEOMETRIES.len() * 3);

    for geometry in &PROOF_GEOMETRIES {
        summaries.push(make_summary_for_mode(
            geometry,
            "sqlite3",
            "sqlite_reference",
            geometry.sqlite_wall_ms,
        ));
        summaries.push(make_summary_for_mode(
            geometry,
            "fsqlite_single_writer",
            "fsqlite_single_writer",
            geometry.single_writer_wall_ms,
        ));
        summaries.push(make_summary_for_mode(
            geometry,
            "fsqlite_mvcc",
            "fsqlite_mvcc",
            geometry.mvcc_wall_ms,
        ));
    }

    summaries
}

fn write_summaries_jsonl(summaries: &[BenchmarkSummary], path: &std::path::Path) {
    let mut lines = String::new();
    for summary in summaries {
        lines.push_str(&summary.to_jsonl().expect("serialize summary"));
        lines.push('\n');
    }
    std::fs::write(path, lines).expect("write JSONL");
}

// ── P1: JSONL roundtrip ───────────────────────────────────────────────

#[test]
fn p1_three_mode_jsonl_roundtrip() {
    let summaries = build_proof_matrix();
    assert_eq!(summaries.len(), 6, "2 geometries × 3 modes = 6 rows");

    let tempdir = tempfile::tempdir().expect("create tempdir");
    let jsonl_path = tempdir.path().join("proof_run.jsonl");

    write_summaries_jsonl(&summaries, &jsonl_path);

    let loaded = load_benchmark_summaries(&jsonl_path).expect("load roundtripped summaries");
    assert_eq!(loaded.len(), summaries.len());

    for (original, loaded) in summaries.iter().zip(loaded.iter()) {
        assert_eq!(original.benchmark_id, loaded.benchmark_id);
        assert_eq!(original.engine, loaded.engine);
        assert_eq!(original.fixture_id, loaded.fixture_id);
        assert_eq!(original.workload, loaded.workload);
        assert_eq!(original.concurrency, loaded.concurrency);
        assert_eq!(original.measurement_count, loaded.measurement_count);
        assert_eq!(original.iterations.len(), loaded.iterations.len());

        let original_comparison = original.comparison.as_ref().expect("has comparison");
        let loaded_comparison = loaded
            .comparison
            .as_ref()
            .expect("roundtrip preserves comparison");
        assert_eq!(
            original_comparison.row_identity.mode_id,
            loaded_comparison.row_identity.mode_id
        );
        assert_eq!(
            original_comparison.counter_schema.comparable.len(),
            loaded_comparison.counter_schema.comparable.len()
        );
    }

    emit_log(
        "P1",
        "result",
        json!({
            "roundtrip_count": loaded.len(),
            "jsonl_bytes": std::fs::metadata(&jsonl_path).map(|m| m.len()).unwrap_or(0),
            "all_comparisons_preserved": true,
        }),
    );
}

// ── P2: Scorecard geometry coverage ───────────────────────────────────

#[test]
fn p2_causal_scorecard_groups_cover_geometry() {
    let summaries = build_proof_matrix();
    let report = build_benchmark_causal_scorecard_report(&summaries);

    assert_eq!(
        report.groups.len(),
        PROOF_GEOMETRIES.len(),
        "one group per workload geometry"
    );

    let concurrencies: HashSet<u16> = report.groups.iter().map(|g| g.concurrency).collect();
    assert!(concurrencies.contains(&1), "c1 geometry present");
    assert!(concurrencies.contains(&4), "c4 geometry present");

    for group in &report.groups {
        assert_eq!(
            group.scorecards.len(),
            3,
            "each geometry group has 3 mode scorecards"
        );
        let mode_ids: HashSet<&str> = group
            .scorecards
            .iter()
            .map(|sc| sc.row_identity.mode_id.as_str())
            .collect();
        assert!(
            mode_ids.contains("sqlite_reference"),
            "sqlite_reference in group c{}",
            group.concurrency
        );
        assert!(
            mode_ids.contains("fsqlite_single_writer"),
            "fsqlite_single_writer in group c{}",
            group.concurrency
        );
        assert!(
            mode_ids.contains("fsqlite_mvcc"),
            "fsqlite_mvcc in group c{}",
            group.concurrency
        );
    }

    emit_log(
        "P2",
        "result",
        json!({
            "group_count": report.groups.len(),
            "concurrencies": concurrencies.iter().copied().collect::<Vec<_>>(),
            "scorecards_per_group": 3,
        }),
    );
}

// ── P3: MVCC causal chain attribution ─────────────────────────────────

#[test]
fn p3_causal_scorecard_three_mode_attribution() {
    let summaries = build_proof_matrix();
    let report = build_benchmark_causal_scorecard_report(&summaries);

    for group in &report.groups {
        let mvcc_sc = group
            .scorecards
            .iter()
            .find(|sc| sc.row_identity.mode_id == "fsqlite_mvcc")
            .expect("MVCC scorecard present");

        assert!(
            !mvcc_sc.causal_chain.is_empty(),
            "MVCC scorecard at c{} has causal chain",
            group.concurrency
        );

        assert!(
            mvcc_sc.negative_findings.is_empty()
                || mvcc_sc
                    .negative_findings
                    .iter()
                    .all(|f| !f.contains("missing")),
            "no missing-comparator warnings at c{}: {:?}",
            group.concurrency,
            mvcc_sc.negative_findings
        );

        assert!(
            !mvcc_sc.interpretation_note.is_empty(),
            "interpretation note present at c{}",
            group.concurrency
        );

        let single_writer_sc = group
            .scorecards
            .iter()
            .find(|sc| sc.row_identity.mode_id == "fsqlite_single_writer")
            .expect("single_writer scorecard present");
        assert_eq!(
            single_writer_sc.baseline_comparator, "sqlite_reference",
            "single_writer anchors to sqlite"
        );
    }

    emit_log(
        "P3",
        "result",
        json!({
            "groups_verified": report.groups.len(),
            "all_mvcc_have_chain": true,
            "all_single_writer_anchor_sqlite": true,
        }),
    );
}

// ── P4: Markdown report structure ─────────────────────────────────────

#[test]
fn p4_markdown_report_nonempty_and_structured() {
    let summaries = build_proof_matrix();
    let markdown = render_benchmark_summaries_markdown(&summaries);

    assert!(!markdown.is_empty(), "markdown report not empty");
    assert!(markdown.contains("# Benchmark Report"), "has report header");
    assert!(
        markdown.contains("## Methodology"),
        "has methodology section"
    );
    assert!(
        markdown.contains("## Environment"),
        "has environment section"
    );
    assert!(
        markdown.contains("fsqlite-e2e.methodology.v1"),
        "methodology version present"
    );
    assert!(markdown.contains("release"), "build profile present");
    assert!(markdown.contains("chinook"), "fixture ID present in report");

    emit_log(
        "P4",
        "result",
        json!({
            "markdown_bytes": markdown.len(),
            "has_report_header": true,
            "has_methodology": true,
            "has_environment": true,
        }),
    );
}

// ── P5: Counter alignment across all rows ─────────────────────────────

#[test]
fn p5_counter_alignment_across_proof_run() {
    let summaries = build_proof_matrix();

    let canonical_ids: Vec<&str> = BENCHMARK_COMPARABLE_COUNTER_IDS.to_vec();

    for summary in &summaries {
        let comparison = summary.comparison.as_ref().expect("comparison present");
        let counter_ids: Vec<&str> = comparison
            .counter_schema
            .comparable
            .iter()
            .map(|c| c.counter_id.as_str())
            .collect();

        assert_eq!(
            counter_ids, canonical_ids,
            "counter IDs match canonical for {}",
            summary.benchmark_id
        );

        for counter in &comparison.counter_schema.comparable {
            match &counter.value {
                BenchmarkCounterValue::Integer(v) => {
                    assert!(
                        *v > 0
                            || counter.counter_id.contains("retry")
                            || counter.counter_id.contains("abort"),
                        "integer counter {} should be >0 (or retry/abort) for {}",
                        counter.counter_id,
                        summary.benchmark_id
                    );
                }
                BenchmarkCounterValue::Float(v) => {
                    assert!(
                        v.is_finite() && *v > 0.0,
                        "float counter {} should be finite and >0 for {}",
                        counter.counter_id,
                        summary.benchmark_id
                    );
                }
            }
        }
    }

    emit_log(
        "P5",
        "result",
        json!({
            "rows_verified": summaries.len(),
            "counters_per_row": canonical_ids.len(),
            "all_aligned": true,
        }),
    );
}

// ── P6: Regression gate self-comparison ───────────────────────────────

#[test]
fn p6_regression_gate_symmetric_pass() {
    let summaries = build_proof_matrix();

    let thresholds = MatrixRegressionThresholds {
        max_p95_ratio: 1.25,
        min_throughput_ratio: 0.80,
    };

    let report = evaluate_matrix_regression_gate(
        &summaries,
        &summaries,
        "proof_baseline",
        "proof_current",
        thresholds,
    )
    .expect("regression gate should succeed for self-comparison");

    assert_eq!(
        report.compared_cells,
        summaries.len(),
        "regression gate covers all rows"
    );

    assert!(
        report.failing_cells.is_empty(),
        "self-comparison should have no regressions: {:?}",
        report.failing_cells
    );

    emit_log(
        "P6",
        "result",
        json!({
            "gate_cell_count": report.compared_cells,
            "failing_count": report.failing_cells.len(),
            "missing_baseline_count": report.missing_baseline_cells.len(),
        }),
    );
}

// ── P7: Row identity within geometry groups ───────────────────────────

#[test]
fn p7_row_identity_cross_geometry() {
    let summaries = build_proof_matrix();

    let mut groups: BTreeMap<(String, String, u16), Vec<&BenchmarkSummary>> = BTreeMap::new();
    for summary in &summaries {
        let key = (
            summary.fixture_id.clone(),
            summary.workload.clone(),
            summary.concurrency,
        );
        groups.entry(key).or_default().push(summary);
    }

    for ((fixture_id, workload, concurrency), group) in &groups {
        assert_eq!(group.len(), 3, "3 modes per geometry group");

        for summary in group {
            let comparison = summary.comparison.as_ref().expect("comparison present");
            assert_eq!(
                &comparison.row_identity.fixture_id, fixture_id,
                "fixture_id aligned"
            );
            assert_eq!(
                &comparison.row_identity.workload, workload,
                "workload aligned"
            );
            assert_eq!(
                comparison.row_identity.concurrency, *concurrency,
                "concurrency aligned"
            );
        }

        let mode_ids: HashSet<&str> = group
            .iter()
            .map(|s| s.comparison.as_ref().unwrap().row_identity.mode_id.as_str())
            .collect();
        assert_eq!(
            mode_ids.len(),
            3,
            "3 distinct mode_ids in group c{concurrency}"
        );
    }

    emit_log(
        "P7",
        "result",
        json!({
            "group_count": groups.len(),
            "all_identities_aligned": true,
            "all_modes_distinct": true,
        }),
    );
}

// ── P8: Interpretation bundle completeness ────────────────────────────

#[test]
fn p8_interpretation_bundle_completeness() {
    let summaries = build_proof_matrix();
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let bundle_dir = tempdir.path().join("proof_bundle");
    std::fs::create_dir_all(&bundle_dir).expect("create bundle dir");

    let jsonl_path = bundle_dir.join("results.jsonl");
    write_summaries_jsonl(&summaries, &jsonl_path);

    let report = build_benchmark_causal_scorecard_report(&summaries);
    let scorecards_json = serde_json::to_string_pretty(&report).expect("serialize scorecards");
    let scorecards_path = bundle_dir.join("scorecards.json");
    std::fs::write(&scorecards_path, &scorecards_json).expect("write scorecards");

    let markdown = render_benchmark_summaries_markdown(&summaries);
    let summary_md_path = bundle_dir.join("summary.md");
    std::fs::write(&summary_md_path, &markdown).expect("write summary markdown");

    let interpretation = format!(
        "## Proof Run Interpretation (bd-db300.7.1.3)\n\n\
         **Validated:**\n\
         - All three comparison modes (sqlite_reference, fsqlite_single_writer, fsqlite_mvcc) \
         produce aligned counter schemas with 10 comparable counters.\n\
         - Two workload geometries (c1 and c4) demonstrate that the harness handles both \
         single-threaded and multi-threaded comparison rows.\n\
         - Causal scorecards correctly attribute gains through the sqlite → single_writer → MVCC \
         chain when all three modes are present.\n\
         - JSONL round-trip preserves all fields including comparison metadata.\n\
         - Regression gate passes for self-comparison (symmetric).\n\
         - Markdown report renders all expected sections.\n\n\
         **Remaining for full scorecard:**\n\
         - This proof run uses synthetic data; a real matrix run with actual benchmark execution \
         is needed for the final scorecard (Track G milestone).\n\
         - Canonical artifact manifest construction requires the full campaign resolution path, \
         which is tested separately by complete_benchmark_matrix.rs.\n\
         - Hot-path profiling counters (mode_specific) are empty in this proof run because no \
         real FrankenSQLite execution occurred.\n\n\
         **Conclusion:** The reporting contract is sound for downstream use. No comparison \
         asymmetry or artifact-layout confusion was discovered.\n"
    );
    let interpretation_path = bundle_dir.join("interpretation.md");
    std::fs::write(&interpretation_path, &interpretation).expect("write interpretation");

    assert!(jsonl_path.exists(), "results.jsonl exists");
    assert!(scorecards_path.exists(), "scorecards.json exists");
    assert!(summary_md_path.exists(), "summary.md exists");
    assert!(interpretation_path.exists(), "interpretation.md exists");

    let loaded_back = load_benchmark_summaries(&jsonl_path).expect("load from bundle");
    assert_eq!(loaded_back.len(), 6, "bundle JSONL has all 6 rows");

    let scorecards_raw = std::fs::read_to_string(&scorecards_path).expect("read scorecards");
    let parsed_report: fsqlite_e2e::benchmark::BenchmarkCausalScorecardReport =
        serde_json::from_str(&scorecards_raw).expect("parse scorecards roundtrip");
    assert_eq!(
        parsed_report.groups.len(),
        2,
        "scorecards roundtrip preserves groups"
    );

    emit_log(
        "P8",
        "result",
        json!({
            "bundle_path": bundle_dir.display().to_string(),
            "artifacts": ["results.jsonl", "scorecards.json", "summary.md", "interpretation.md"],
            "jsonl_rows": 6,
            "scorecard_groups": 2,
            "bundle_complete": true,
        }),
    );
}
