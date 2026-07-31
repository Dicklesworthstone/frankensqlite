//! Contract tests for the bd-zywqc.2 performance regression guard.
//!
//! These tests invoke the real shell analyzer against complete, v6-shaped
//! benchmark evidence. They deliberately avoid running the benchmark itself.

#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

const ITERATIONS: usize = 21;
const CONTRACT_BOOTSTRAP_REPETITIONS: usize = 10_000;
const ROWS_PER_THREAD: usize = 500;
const SQLITE_ELAPSED_NS: u64 = 1_000_000_000;
const SETTINGS_INTERPRETATION: &str = "Both engines proved the listed effective PRAGMA values; equal names and readbacks do not establish cross-engine semantic equivalence.";
const ACCOUNTING_INTERPRETATION: &str = "offered and committed writes share one row unit; attempted_writes counts physical INSERT calls; retried_operations records the existing engine-specific retry unit and is provenance only, not a cross-engine comparison metric.";
const TIMING_INTERPRETATION: &str = "workload_elapsed_ns begins only after every worker has opened and proved its effective settings, and ends at the last worker's transaction terminal point before connection teardown; worker_startup_elapsed_ns is reported separately.";
const NON_CITABLE_REASON: &str = "v6 adds fail-closed settings, committed-work, integrity, timing, retry-policy, and configuration receipts, but bd-uh1fv still requires external watchdog, sanitized environment, matched retry/deadline semantics, complete build/toolchain provenance, counterbalanced topology receipts, immutable manifest, and independent verification.";
static NEXT_RUN_ID: AtomicUsize = AtomicUsize::new(1);

struct GateRun {
    output: Output,
    result_path: PathBuf,
}

impl Deref for GateRun {
    type Target = Output;

    fn deref(&self) -> &Self::Target {
        &self.output
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fsqlite-e2e must live under crates/")
        .to_path_buf()
}

fn script_path() -> PathBuf {
    repo_root().join("scripts/perf_regression_gate.sh")
}

fn embedded_analyzer_source() -> String {
    let script = fs::read_to_string(script_path()).expect("read regression gate script");
    let marker = "<<'PYEOF'\n";
    let start = script.find(marker).expect("find embedded analyzer start") + marker.len();
    let end = script.rfind("\nPYEOF").expect("find embedded analyzer end");
    script[start..end].to_owned()
}

fn embedded_analyzer_definitions() -> String {
    let source = embedded_analyzer_source();
    let marker = "\ntry:\n    current_report = read_json(";
    let end = source.rfind(marker).expect("find analyzer entry point");
    source[..end].to_owned()
}

fn measured_analyzer_output(
    current_report: &Path,
    baseline_path: &Path,
    result_path: &Path,
    capture_baseline: bool,
    run_id: &str,
    expected_history_path: &Path,
) -> GateRun {
    let mut child = Command::new("python3")
        .arg("-")
        .arg(current_report)
        .arg(baseline_path)
        .arg(result_path)
        .arg("contract-test-commit")
        .arg(ROWS_PER_THREAD.to_string())
        .arg(if capture_baseline { "true" } else { "false" })
        .arg("fsqlite.perf_regression_gate.baseline.v3")
        .arg("fsqlite.perf_regression_gate.result.v2")
        .arg("fsqlite-e2e.mt_mvcc_bench_report.v6")
        .arg(ITERATIONS.to_string())
        .arg("0.05")
        .arg("0.0")
        .arg("measured")
        .arg(run_id)
        .arg(expected_history_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn embedded measured-mode analyzer");
    let mut stdin = child.stdin.take().expect("capture analyzer stdin");
    stdin
        .write_all(embedded_analyzer_source().as_bytes())
        .expect("write embedded analyzer source");
    drop(stdin);
    GateRun {
        output: child
            .wait_with_output()
            .expect("wait for embedded analyzer"),
        result_path: result_path.to_path_buf(),
    }
}

fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create fixture parent");
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(value).expect("serialize fixture"),
    )
    .expect("write fixture");
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("read JSON artifact"))
        .expect("parse JSON artifact")
}

fn gate_output(fixture: &TempDir, current_report: &Path, capture_baseline: bool) -> GateRun {
    let run_id = format!("contract-{}", NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed));
    gate_output_with_run_id(fixture, current_report, capture_baseline, &run_id)
}

fn gate_output_with_run_id(
    fixture: &TempDir,
    current_report: &Path,
    capture_baseline: bool,
    run_id: &str,
) -> GateRun {
    let result_path = fixture
        .path()
        .join("target/regression_gate_runs")
        .join(run_id)
        .join("result.json");
    let mut command = Command::new("bash");
    command
        .env("FSQLITE_REGGATE_RUN_ID", run_id)
        .arg(script_path())
        .arg("--analyze-only")
        .arg(current_report)
        .arg("--target-dir")
        .arg(fixture.path().join("target"))
        .arg("--baseline-dir")
        .arg(fixture.path().join("baselines"))
        .arg("--rows")
        .arg(ROWS_PER_THREAD.to_string());
    if capture_baseline {
        command.arg("--capture-baseline");
    }
    GateRun {
        output: command.output().expect("run regression guard"),
        result_path,
    }
}

fn output_detail(output: &GateRun) -> String {
    format!(
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile(mut values: Vec<f64>, quantile: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    let rank = quantile * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return values[lower];
    }
    let fraction = rank - lower as f64;
    (values[upper] - values[lower]).mul_add(fraction, values[lower])
}

#[allow(clippy::cast_precision_loss)]
fn ratio_stats(ratios: &[f64]) -> (f64, f64, f64, f64, f64) {
    let median = percentile(ratios.to_vec(), 0.50);
    let mean = ratios.iter().sum::<f64>() / ratios.len() as f64;
    let variance = if ratios.len() > 1 {
        ratios
            .iter()
            .map(|ratio| (ratio - mean).powi(2))
            .sum::<f64>()
            / (ratios.len() - 1) as f64
    } else {
        0.0
    };
    let cv_pct = if mean == 0.0 {
        0.0
    } else {
        variance.sqrt() / mean.abs() * 100.0
    };
    let mad = percentile(
        ratios.iter().map(|ratio| (ratio - median).abs()).collect(),
        0.50,
    );
    let mut state = 0x7a25_2026_c011_cafe_u64;
    let mut bootstrap_medians = Vec::with_capacity(CONTRACT_BOOTSTRAP_REPETITIONS);
    let ratio_count = u64::try_from(ratios.len()).expect("fixture sample count fits u64");
    for _ in 0..CONTRACT_BOOTSTRAP_REPETITIONS {
        let mut resample = Vec::with_capacity(ratios.len());
        for _ in ratios {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let sample_index =
                usize::try_from(state % ratio_count).expect("fixture sample index fits usize");
            resample.push(ratios[sample_index]);
        }
        bootstrap_medians.push(percentile(resample, 0.50));
    }
    bootstrap_medians.sort_by(f64::total_cmp);
    let low = bootstrap_medians[CONTRACT_BOOTSTRAP_REPETITIONS * 25 / 1_000];
    let high = bootstrap_medians
        [(CONTRACT_BOOTSTRAP_REPETITIONS * 975 / 1_000).min(CONTRACT_BOOTSTRAP_REPETITIONS - 1)];
    (median, low, high, cv_pct, mad)
}

fn median_ci_contract(claim_ratios: &[f64]) -> Value {
    let null_ratios = vec![1.0; claim_ratios.len()];
    let (null_median, null_low, null_high, null_cv, null_mad) = ratio_stats(&null_ratios);
    let (claim_median, claim_low, claim_high, claim_cv, claim_mad) = ratio_stats(claim_ratios);
    let null_radius = (null_low - 1.0_f64).abs().max((null_high - 1.0).abs());
    let decisive_effect = (2.0 * null_radius).max(0.01);
    let min_decidable_gain = 1.0 + decisive_effect;
    let max_decidable_regression = 1.0 - decisive_effect;
    let verdict = if claim_low > min_decidable_gain {
        "FSQLITE_FASTER"
    } else if claim_high < max_decidable_regression {
        "FSQLITE_SLOWER"
    } else {
        "INCONCLUSIVE"
    };
    json!({
        "null_ratio_median": null_median,
        "null_ratio_ci95_low": null_low,
        "null_ratio_ci95_high": null_high,
        "null_ratio_cv_pct": null_cv,
        "null_ratio_mad": null_mad,
        "claim_ratio_median": claim_median,
        "claim_ratio_ci95_low": claim_low,
        "claim_ratio_ci95_high": claim_high,
        "claim_ratio_cv_pct": claim_cv,
        "claim_ratio_mad": claim_mad,
        "null_radius": null_radius,
        "min_decidable_gain": min_decidable_gain,
        "max_decidable_regression": max_decidable_regression,
        "claim_margin": null,
        "cv_gate": "never",
        "verdict": verdict,
    })
}

fn settings(concurrent_mode: &str) -> Value {
    json!({
        "page_size_bytes": 4096,
        "journal_mode": "wal",
        "synchronous": "normal",
        "cache_size": -64000,
        "busy_timeout_ms": 5000,
        "wal_autocheckpoint_pages": 1000,
        "concurrent_mode": concurrent_mode,
    })
}

fn elapsed_seconds(elapsed_ns: u64) -> f64 {
    Duration::from_nanos(elapsed_ns).as_secs_f64()
}

fn configuration(threads: usize) -> Value {
    json!({
        "writers": threads,
        "available_parallelism": 64,
        "max_supported_writers": 128,
        "wal_autocheckpoint_pages": 1000,
        "wal_autocheckpoint_overridden": false,
        "offered_writes_per_sample": threads * ROWS_PER_THREAD,
        "retry_policy": {
            "csqlite_busy_timeout_ms": 5000,
            "csqlite_max_operation_retries": 512,
            "csqlite_retry_sleep_ms": 1,
            "csqlite_retry_unit": "individual INSERT or COMMIT operation",
            "csqlite_retry_algorithm": "csqlite.per-operation.fixed-1ms.busy-or-locked.max-512.v1",
            "fsqlite_transaction_timeout_ms": 5000,
            "fsqlite_max_transaction_retries": 512,
            "fsqlite_retry_sleep_base_ms": 1,
            "fsqlite_retry_sleep_cap_ms": 29,
            "fsqlite_retry_unit": "whole BEGIN CONCURRENT transaction attempt",
            "fsqlite_retry_backoff_algorithm": "fsqlite.whole-transaction.step-exp-every-8-cap-25ms-plus-thread-attempt-jitter-0-to-4ms.max-512-or-timeout.v1",
            "fsqlite_retryable_errors": "Busy|BusyRecovery|BusySnapshot|DatabaseLocked|WriteConflict|SerializationFailure|PageBufferCapacityExhausted",
            "fsqlite_timeout_overridden": false,
        },
        "status": "supported",
        "comparison_eligible": true,
        "measured": true,
        "reason": "complete deterministic contract fixture",
    })
}

fn sample(threads: usize, elapsed_ns: u64, concurrent_mode: &str) -> Value {
    let offered_writes = threads * ROWS_PER_THREAD;
    json!({
        "worker_startup_elapsed_ns": 1_000_000,
        "workload_elapsed_ns": elapsed_ns,
        "settings": settings(concurrent_mode),
        "accounting": {
            "offered_writes": offered_writes,
            "attempted_writes": offered_writes,
            "succeeded_writes": offered_writes,
            "retried_operations": 0,
            "failed_writes": 0,
            "worker_reported_failed_writes": 0,
            "exact": true,
            "diagnostics": [],
        },
        "committed_state": {
            "expected_rows": offered_writes,
            "observed_rows": offered_writes,
            "expected_id_sum": 42,
            "observed_id_sum": 42,
            "expected_payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "observed_payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "integrity_check": ["ok"],
            "valid": true,
            "diagnostics": [],
        },
    })
}

#[allow(clippy::cast_precision_loss)]
fn thread_report(threads: usize, sqlite_elapsed_ns: &[u64], fsqlite_elapsed_ns: &[u64]) -> Value {
    assert_eq!(sqlite_elapsed_ns.len(), fsqlite_elapsed_ns.len());
    let sqlite_samples = sqlite_elapsed_ns
        .iter()
        .map(|elapsed| sample(threads, *elapsed, "sqlite_wal_single_writer"))
        .collect::<Vec<_>>();
    let fsqlite_samples = fsqlite_elapsed_ns
        .iter()
        .map(|elapsed| sample(threads, *elapsed, "fsqlite_mvcc_on"))
        .collect::<Vec<_>>();
    let offered_writes = (threads * ROWS_PER_THREAD) as f64;
    let sqlite_wps = sqlite_elapsed_ns
        .iter()
        .map(|elapsed| offered_writes / elapsed_seconds(*elapsed))
        .collect::<Vec<_>>();
    let fsqlite_wps = fsqlite_elapsed_ns
        .iter()
        .map(|elapsed| offered_writes / elapsed_seconds(*elapsed))
        .collect::<Vec<_>>();
    let ratios = fsqlite_wps
        .iter()
        .zip(&sqlite_wps)
        .map(|(fsqlite, sqlite)| fsqlite / sqlite)
        .collect::<Vec<_>>();
    let fsqlite_ms = fsqlite_elapsed_ns
        .iter()
        .map(|elapsed| elapsed_seconds(*elapsed) * 1_000.0)
        .collect::<Vec<_>>();
    let sqlite_ms = sqlite_elapsed_ns
        .iter()
        .map(|elapsed| elapsed_seconds(*elapsed) * 1_000.0)
        .collect::<Vec<_>>();
    let ratio_median = percentile(ratios.clone(), 0.50);
    let fsqlite_ms_p50 = percentile(fsqlite_ms.clone(), 0.50);
    let sqlite_ms_p50 = percentile(sqlite_ms.clone(), 0.50);
    let receipt = configuration(threads);
    json!({
        "threads": threads,
        "fsqlite_wps_p50": percentile(fsqlite_wps.clone(), 0.50),
        "fsqlite_wps_p95": percentile(fsqlite_wps.clone(), 0.95),
        "fsqlite_wps_p99": percentile(fsqlite_wps, 0.99),
        "sqlite_wps_p50": percentile(sqlite_wps.clone(), 0.50),
        "sqlite_wps_p95": percentile(sqlite_wps.clone(), 0.95),
        "sqlite_wps_p99": percentile(sqlite_wps, 0.99),
        "throughput_ratio": ratio_median,
        "fsqlite_ms_p50": fsqlite_ms_p50,
        "fsqlite_ms_p95": percentile(fsqlite_ms.clone(), 0.95),
        "fsqlite_ms_p99": percentile(fsqlite_ms, 0.99),
        "sqlite_ms_p50": sqlite_ms_p50,
        "sqlite_ms_p95": percentile(sqlite_ms.clone(), 0.95),
        "sqlite_ms_p99": percentile(sqlite_ms, 0.99),
        "time_ratio": fsqlite_ms_p50 / sqlite_ms_p50,
        "fsqlite_failed_rows": 0,
        "sqlite_failed_rows": 0,
        "median_ci_contract": median_ci_contract(&ratios),
        "truth": {
            "configuration": receipt,
            "null_c_a_samples": sqlite_samples.clone(),
            "null_c_b_samples": sqlite_samples.clone(),
            "sqlite_samples": sqlite_samples,
            "fsqlite_samples": fsqlite_samples,
        },
    })
}

fn report(one_thread_ns: Vec<u64>, eight_thread_ns: Vec<u64>) -> Value {
    report_with_engine_elapsed(
        vec![SQLITE_ELAPSED_NS; one_thread_ns.len()],
        one_thread_ns,
        vec![SQLITE_ELAPSED_NS; eight_thread_ns.len()],
        eight_thread_ns,
    )
}

fn report_with_engine_elapsed(
    one_thread_sqlite_ns: Vec<u64>,
    one_thread_fsqlite_ns: Vec<u64>,
    eight_thread_sqlite_ns: Vec<u64>,
    eight_thread_fsqlite_ns: Vec<u64>,
) -> Value {
    json!({
        "schema_version": "fsqlite-e2e.mt_mvcc_bench_report.v6",
        "citable": false,
        "non_citable_reason": NON_CITABLE_REASON,
        "settings_interpretation": SETTINGS_INTERPRETATION,
        "accounting_interpretation": ACCOUNTING_INTERPRETATION,
        "timing_interpretation": TIMING_INTERPRETATION,
        "workload_shape": "shared_table",
        "rows_per_thread": ROWS_PER_THREAD,
        "iterations": one_thread_fsqlite_ns.len(),
        "configuration_receipts": [configuration(1), configuration(8)],
        "thread_results": [
            thread_report(1, &one_thread_sqlite_ns, &one_thread_fsqlite_ns),
            thread_report(8, &eight_thread_sqlite_ns, &eight_thread_fsqlite_ns),
        ],
        "pass_over_pass_gate": {
            "schema_version": "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1",
            "history_json_path": "disposable",
            "threshold_ratio_drop_pct": 5.0,
            "status": "no_prior_report",
            "previous_report_found": false,
            "comparable_pair_count": 0,
            "regressions": [],
        },
    })
}

fn constant_elapsed(elapsed_ns: u64) -> Vec<u64> {
    vec![elapsed_ns; ITERATIONS]
}

fn fixture_report(elapsed_ns: u64) -> Value {
    report(constant_elapsed(elapsed_ns), constant_elapsed(elapsed_ns))
}

fn capture(fixture: &TempDir, current_path: &Path, report: &Value) {
    write_json(current_path, report);
    let output = gate_output(fixture, current_path, true);
    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["verdict"], "baseline_captured");
    assert_eq!(result["release_evidence"], false);
    assert_eq!(result["release_eligible"], false);
    assert_eq!(
        fs::read(
            output
                .result_path
                .parent()
                .expect("result run directory")
                .join("current.json")
        )
        .expect("read analyze-only snapshot"),
        fs::read(current_path).expect("read analyze-only source"),
        "analyze-only mode must preserve an invocation-scoped input snapshot"
    );
    let baseline = read_json(&fixture.path().join("baselines/latest.json"));
    let capture_run_id = output
        .result_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .expect("capture result has a UTF-8 run directory");
    assert_eq!(
        baseline["schema_version"],
        "fsqlite.perf_regression_gate.baseline.v3"
    );
    assert_eq!(baseline["capture_run_id"], capture_run_id);
    assert_eq!(
        baseline["report_history_json_path"],
        report["pass_over_pass_gate"]["history_json_path"]
    );
    assert_eq!(baseline["release_evidence"], false);
    assert_eq!(baseline["release_eligible"], false);
    assert!(
        result["baseline_version_path"]
            .as_str()
            .expect("versioned baseline path")
            .contains("/versions/")
    );
    assert!(
        Path::new(
            result["baseline_candidate_path"]
                .as_str()
                .expect("candidate baseline path")
        )
        .is_file(),
        "the fully fsynced candidate remains available for recovery"
    );
    assert_eq!(
        result["baseline_envelope_sha256"]
            .as_str()
            .expect("baseline envelope digest")
            .len(),
        64
    );
}

fn assert_invalid_capture(report: &Value) {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    write_json(&current_path, report);

    let output = gate_output(&fixture, &current_path, true);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    assert!(!fixture.path().join("baselines/latest.json").exists());
    let result = read_json(&output.result_path);
    assert_eq!(result["verdict"], "invalid_evidence");
    assert_eq!(result["release_eligible"], false);
}

#[test]
fn missing_baseline_fails_closed_without_implicit_capture() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    write_json(&current_path, &fixture_report(833_333_333));

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    assert!(!fixture.path().join("baselines/latest.json").exists());
}

#[test]
fn validated_capture_and_unchanged_comparison_pass_without_mutating_baseline() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let baseline_report = fixture_report(833_333_333);
    capture(&fixture, &current_path, &baseline_report);
    let baseline_path = fixture.path().join("baselines/latest.json");
    let baseline_before = fs::read(&baseline_path).expect("read captured baseline");

    write_json(&current_path, &baseline_report);
    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    assert_eq!(
        fs::read(&baseline_path).expect("reread baseline"),
        baseline_before,
        "comparison must not rewrite the immutable baseline"
    );
    let result = read_json(&output.result_path);
    assert_eq!(result["guard_status"], "passed");
    assert_eq!(result["verdict"], "diagnostic_only");
    assert_eq!(result["release_evidence"], false);
    assert_eq!(result["release_eligible"], false);
    assert!(
        result["comparisons"]
            .as_array()
            .expect("comparisons")
            .iter()
            .all(|comparison| comparison["status"] == "passed")
    );
}

#[test]
fn baseline_history_binding_is_independent_of_the_current_report_history() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let mut baseline_report = fixture_report(833_333_333);
    baseline_report["pass_over_pass_gate"]["history_json_path"] =
        json!("baseline-run/disposable_history.json");
    capture(&fixture, &current_path, &baseline_report);

    let baseline = read_json(&fixture.path().join("baselines/latest.json"));
    assert_eq!(
        baseline["report_history_json_path"],
        "baseline-run/disposable_history.json"
    );

    let mut current_report = baseline_report;
    current_report["pass_over_pass_gate"]["history_json_path"] =
        json!("current-run/disposable_history.json");
    write_json(&current_path, &current_report);
    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
}

#[test]
fn measured_mode_binds_current_and_baseline_to_their_own_run_histories() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let baseline_path = fixture.path().join("baselines/latest.json");
    let baseline_report_path = fixture.path().join("baseline-report.json");
    let baseline_history = fixture
        .path()
        .join("target/regression_gate_runs/measured-baseline/disposable_history.json");
    let mut baseline_report = fixture_report(833_333_333);
    baseline_report["pass_over_pass_gate"]["history_json_path"] =
        json!(baseline_history.to_string_lossy());
    write_json(&baseline_report_path, &baseline_report);
    let capture_result = fixture.path().join("results/capture.json");

    let capture = measured_analyzer_output(
        &baseline_report_path,
        &baseline_path,
        &capture_result,
        true,
        "measured-baseline",
        &baseline_history,
    );
    assert_eq!(
        capture.status.code(),
        Some(0),
        "{}",
        output_detail(&capture)
    );

    let current_report_path = fixture.path().join("current-report.json");
    let current_history = fixture
        .path()
        .join("target/regression_gate_runs/measured-current/disposable_history.json");
    let mut current_report = baseline_report;
    current_report["pass_over_pass_gate"]["history_json_path"] =
        json!(current_history.to_string_lossy());
    write_json(&current_report_path, &current_report);
    let mismatched_history = fixture
        .path()
        .join("target/regression_gate_runs/measured-other/disposable_history.json");
    let mismatch_result = fixture.path().join("results/history-mismatch.json");
    let mismatch = measured_analyzer_output(
        &current_report_path,
        &baseline_path,
        &mismatch_result,
        false,
        "measured-current-mismatch",
        &mismatched_history,
    );
    assert_eq!(
        mismatch.status.code(),
        Some(2),
        "{}",
        output_detail(&mismatch)
    );
    assert!(
        read_json(&mismatch_result)["error"]
            .as_str()
            .expect("history mismatch error")
            .contains("does not match its bound history path")
    );

    let comparison_result = fixture.path().join("results/comparison.json");

    let comparison = measured_analyzer_output(
        &current_report_path,
        &baseline_path,
        &comparison_result,
        false,
        "measured-current",
        &current_history,
    );
    assert_eq!(
        comparison.status.code(),
        Some(0),
        "{}",
        output_detail(&comparison)
    );
}

#[test]
fn relative_paths_beginning_with_a_dash_are_safe_operands() {
    let fixture = tempfile::tempdir().expect("tempdir");
    write_json(
        &fixture.path().join("-current.json"),
        &fixture_report(833_333_333),
    );
    let run_id = format!(
        "contract-dash-{}",
        NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed)
    );
    let output = Command::new("bash")
        .current_dir(fixture.path())
        .env("FSQLITE_REGGATE_RUN_ID", &run_id)
        .arg(script_path())
        .arg("--analyze-only")
        .arg("-current.json")
        .arg("--target-dir")
        .arg("-target")
        .arg("--baseline-dir")
        .arg("-baselines")
        .arg("--rows")
        .arg(ROWS_PER_THREAD.to_string())
        .arg("--capture-baseline")
        .output()
        .expect("run regression guard with leading-dash paths");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        fixture
            .path()
            .join("-target/regression_gate_runs")
            .join(run_id)
            .join("result.json")
            .is_file()
    );
}

#[test]
fn statistically_proven_absolute_fsqlite_regression_fails() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(833_333_333));
    write_json(&current_path, &fixture_report(909_090_909));

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(1), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["verdict"], "failed");
    assert!(
        result["comparisons"]
            .as_array()
            .expect("comparisons")
            .iter()
            .all(|comparison| comparison["status"] == "regression")
    );
}

#[test]
fn exact_five_percent_single_writer_boundary_passes() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(
        &fixture,
        &current_path,
        &report(
            constant_elapsed(950_000_000),
            constant_elapsed(SQLITE_ELAPSED_NS),
        ),
    );
    write_json(
        &current_path,
        &report(
            constant_elapsed(SQLITE_ELAPSED_NS),
            constant_elapsed(SQLITE_ELAPSED_NS),
        ),
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["comparisons"][0]["status"], "passed");
    let observed_drop = result["comparisons"][0]["relative_delta_pct"]
        .as_f64()
        .expect("numeric observed drop");
    assert!((observed_drop + 5.0).abs() < 1.0e-9);
}

#[test]
fn single_writer_gate_uses_arithmetic_mean_instead_of_median() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(SQLITE_ELAPSED_NS));
    let mut mean_faster_but_median_slower = vec![1_100_000_000; 11];
    mean_faster_but_median_slower.extend(vec![500_000_000; 10]);
    write_json(
        &current_path,
        &report(
            mean_faster_but_median_slower,
            constant_elapsed(SQLITE_ELAPSED_NS),
        ),
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(
        result["comparisons"][0]["metric"],
        "fsqlite_wps_arithmetic_mean"
    );
    assert_eq!(result["comparisons"][0]["status"], "passed");
}

#[test]
fn four_percent_single_writer_drop_is_within_its_five_percent_budget() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(SQLITE_ELAPSED_NS));
    write_json(
        &current_path,
        &report(
            constant_elapsed(1_041_666_667),
            constant_elapsed(SQLITE_ELAPSED_NS),
        ),
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["comparisons"][0]["status"], "passed");
    assert_eq!(result["comparisons"][0]["max_allowed_drop_pct"], 5.0);
}

#[test]
fn four_percent_eight_writer_drop_violates_zero_regression_budget() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(SQLITE_ELAPSED_NS));
    write_json(
        &current_path,
        &report(
            constant_elapsed(SQLITE_ELAPSED_NS),
            constant_elapsed(1_041_666_667),
        ),
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(1), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["comparisons"][0]["status"], "passed");
    assert_eq!(result["comparisons"][1]["status"], "regression");
    assert_eq!(result["comparisons"][1]["max_allowed_drop_pct"], 0.0);
}

#[test]
fn equal_ratio_does_not_hide_absolute_fsqlite_regression() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(SQLITE_ELAPSED_NS));
    write_json(
        &current_path,
        &report_with_engine_elapsed(
            constant_elapsed(2 * SQLITE_ELAPSED_NS),
            constant_elapsed(2 * SQLITE_ELAPSED_NS),
            constant_elapsed(2 * SQLITE_ELAPSED_NS),
            constant_elapsed(2 * SQLITE_ELAPSED_NS),
        ),
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(1), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert!(
        result["comparisons"]
            .as_array()
            .expect("comparisons")
            .iter()
            .all(|comparison| comparison["status"] == "regression")
    );
    assert_eq!(
        result["comparisons"][0]["current_fsqlite_to_csqlite_ratio_median_diagnostic"],
        1.0
    );
}

#[test]
fn csqlite_only_speedup_does_not_create_an_fsqlite_regression() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(SQLITE_ELAPSED_NS));
    write_json(
        &current_path,
        &report_with_engine_elapsed(
            constant_elapsed(SQLITE_ELAPSED_NS / 2),
            constant_elapsed(SQLITE_ELAPSED_NS),
            constant_elapsed(SQLITE_ELAPSED_NS / 2),
            constant_elapsed(SQLITE_ELAPSED_NS),
        ),
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert!(
        result["comparisons"]
            .as_array()
            .expect("comparisons")
            .iter()
            .all(|comparison| comparison["status"] == "passed")
    );
    assert_eq!(
        result["comparisons"][0]["current_fsqlite_to_csqlite_ratio_median_diagnostic"],
        0.5
    );
}

#[test]
fn insufficient_sample_count_is_invalid_even_during_capture() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let short_report = report(vec![833_333_333; 3], vec![833_333_333; 3]);
    write_json(&current_path, &short_report);

    let output = gate_output(&fixture, &current_path, true);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    assert!(!fixture.path().join("baselines/latest.json").exists());
}

#[test]
fn excessive_sample_count_is_rejected_before_bootstrapping() {
    let oversized_report = report(vec![833_333_333; 22], vec![833_333_333; 22]);

    assert_invalid_capture(&oversized_report);
}

#[test]
fn missing_real_p50_field_is_invalid_instead_of_becoming_zero() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let mut malformed = fixture_report(833_333_333);
    malformed["thread_results"][0]
        .as_object_mut()
        .expect("thread result object")
        .remove("fsqlite_wps_p50");
    write_json(&current_path, &malformed);

    let output = gate_output(&fixture, &current_path, true);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["verdict"], "invalid_evidence");
}

#[test]
fn serial_fsqlite_settings_are_invalid() {
    let mut malformed = fixture_report(833_333_333);
    for row in malformed["thread_results"]
        .as_array_mut()
        .expect("thread results")
    {
        for sample in row["truth"]["fsqlite_samples"]
            .as_array_mut()
            .expect("fsqlite samples")
        {
            sample["settings"]["concurrent_mode"] = json!("fsqlite_mvcc_off");
        }
    }

    assert_invalid_capture(&malformed);
}

#[test]
fn incomplete_median_ci_contract_is_invalid() {
    let mut malformed = fixture_report(833_333_333);
    malformed["thread_results"][0]["median_ci_contract"]
        .as_object_mut()
        .expect("median CI contract")
        .remove("claim_ratio_ci95_low");

    assert_invalid_capture(&malformed);
}

#[test]
fn wrong_retry_identity_is_invalid() {
    let mut malformed = fixture_report(833_333_333);
    malformed["configuration_receipts"][0]["retry_policy"]["csqlite_retry_algorithm"] =
        json!("wrong-retry-algorithm");
    malformed["thread_results"][0]["truth"]["configuration"] =
        malformed["configuration_receipts"][0].clone();

    assert_invalid_capture(&malformed);
}

#[test]
fn malformed_receipt_types_and_contract_constants_are_invalid() {
    let mut boolean_pair_count = fixture_report(833_333_333);
    boolean_pair_count["pass_over_pass_gate"]["comparable_pair_count"] = json!(false);
    assert_invalid_capture(&boolean_pair_count);

    let mut negative_retry_count = fixture_report(833_333_333);
    negative_retry_count["thread_results"][0]["truth"]["fsqlite_samples"][0]["accounting"]["retried_operations"] =
        json!(-1);
    assert_invalid_capture(&negative_retry_count);

    let mut string_id_sums = fixture_report(833_333_333);
    string_id_sums["thread_results"][0]["truth"]["fsqlite_samples"][0]["committed_state"]["expected_id_sum"] =
        json!("42");
    string_id_sums["thread_results"][0]["truth"]["fsqlite_samples"][0]["committed_state"]["observed_id_sum"] =
        json!("42");
    assert_invalid_capture(&string_id_sums);

    let mut inconsistent_capacity = fixture_report(833_333_333);
    inconsistent_capacity["configuration_receipts"][1]["available_parallelism"] = json!(32);
    inconsistent_capacity["thread_results"][1]["truth"]["configuration"] =
        inconsistent_capacity["configuration_receipts"][1].clone();
    assert_invalid_capture(&inconsistent_capacity);

    let mut wrong_allocator_ceiling = fixture_report(833_333_333);
    wrong_allocator_ceiling["configuration_receipts"][0]["max_supported_writers"] = json!(127);
    wrong_allocator_ceiling["thread_results"][0]["truth"]["configuration"] =
        wrong_allocator_ceiling["configuration_receipts"][0].clone();
    assert_invalid_capture(&wrong_allocator_ceiling);

    let mut rewritten_interpretation = fixture_report(833_333_333);
    rewritten_interpretation["timing_interpretation"] = json!("approximately equivalent timing");
    assert_invalid_capture(&rewritten_interpretation);
}

#[test]
fn citable_v6_claim_is_invalid_for_this_diagnostic_gate() {
    let mut malformed = fixture_report(833_333_333);
    malformed["citable"] = json!(true);

    assert_invalid_capture(&malformed);
}

#[test]
fn summary_latency_that_disagrees_with_raw_samples_is_invalid() {
    let mut malformed = fixture_report(833_333_333);
    malformed["thread_results"][0]["fsqlite_ms_p50"] = json!(123.0);

    assert_invalid_capture(&malformed);
}

#[test]
fn legacy_raw_report_is_not_accepted_as_a_baseline_envelope() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let current = fixture_report(833_333_333);
    write_json(&current_path, &current);
    write_json(&fixture.path().join("baselines/latest.json"), &current);

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert!(
        result["error"]
            .as_str()
            .expect("error string")
            .contains("recapture explicitly")
    );
}

#[test]
fn configuration_drift_is_invalid() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(833_333_333));
    let mut current = fixture_report(833_333_333);
    current["configuration_receipts"][1]["wal_autocheckpoint_pages"] = json!(2000);
    current["thread_results"][1]["truth"]["configuration"] =
        current["configuration_receipts"][1].clone();
    write_json(&current_path, &current);

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
}

#[test]
fn noisy_threshold_overlap_is_inconclusive_and_fails_closed() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(SQLITE_ELAPSED_NS));
    let mut noisy = vec![1_063_829_787; 11]; // approximately 0.94x
    noisy.extend(vec![980_392_157; 10]); // approximately 1.02x
    write_json(&current_path, &report(noisy.clone(), noisy));

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(1), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["comparisons"][0]["status"], "passed");
    assert_eq!(result["comparisons"][1]["status"], "inconclusive");
}

#[test]
fn realistic_unchanged_noise_exposes_zero_margin_contract_blocker() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let mut noisy_unchanged = vec![950_000_000; 10];
    noisy_unchanged.extend(vec![1_050_000_000; 11]);
    let unchanged_report = report(noisy_unchanged.clone(), noisy_unchanged);
    capture(&fixture, &current_path, &unchanged_report);
    write_json(&current_path, &unchanged_report);

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(1), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["comparisons"][0]["status"], "passed");
    assert_eq!(result["comparisons"][1]["status"], "inconclusive");
    assert_eq!(result["comparisons"][1]["max_allowed_drop_pct"], 0.0);
}

#[test]
fn baseline_report_tampering_breaks_content_addressing() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(833_333_333));
    let baseline_path = fixture.path().join("baselines/latest.json");
    let mut baseline = read_json(&baseline_path);
    baseline["report"]["rows_per_thread"] = json!(ROWS_PER_THREAD + 1);
    write_json(&baseline_path, &baseline);
    write_json(&current_path, &fixture_report(833_333_333));

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert!(
        result["error"]
            .as_str()
            .expect("error string")
            .contains("content-addressed")
    );
}

#[test]
fn baseline_metadata_tampering_breaks_content_addressing() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &fixture_report(833_333_333));
    let baseline_path = fixture.path().join("baselines/latest.json");
    let mut baseline = read_json(&baseline_path);
    baseline["analyzer_commit"] = json!("metadata-only-tamper");
    write_json(&baseline_path, &baseline);
    write_json(&current_path, &fixture_report(833_333_333));

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert!(
        result["error"]
            .as_str()
            .expect("error string")
            .contains("content-addressed")
    );
}

#[test]
fn separately_copied_latest_and_version_files_are_invalid() {
    let source_fixture = tempfile::tempdir().expect("source tempdir");
    let source_current = source_fixture.path().join("current.json");
    capture(
        &source_fixture,
        &source_current,
        &fixture_report(833_333_333),
    );
    let source_baseline = read_json(&source_fixture.path().join("baselines/latest.json"));
    let source_result_root = source_fixture.path().join("target/regression_gate_runs");
    let source_result_path = fs::read_dir(&source_result_root)
        .expect("read source run directory")
        .next()
        .expect("source run entry")
        .expect("read source run entry")
        .path()
        .join("result.json");
    let source_result = read_json(&source_result_path);
    let digest = source_result["baseline_envelope_sha256"]
        .as_str()
        .expect("source baseline digest");

    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    write_json(&current_path, &fixture_report(833_333_333));
    write_json(
        &fixture.path().join("baselines/latest.json"),
        &source_baseline,
    );
    write_json(
        &fixture
            .path()
            .join("baselines/versions")
            .join(format!("{digest}.json")),
        &source_baseline,
    );

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert!(
        result["error"]
            .as_str()
            .expect("error string")
            .contains("not the matching content-addressed version")
    );
}

#[test]
fn interrupted_candidate_does_not_wedge_future_capture() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let interrupted_candidate = fixture
        .path()
        .join("baselines/versions/candidates/interrupted/baseline.json");
    write_json(&interrupted_candidate, &json!({"partial": true}));

    capture(&fixture, &current_path, &fixture_report(833_333_333));

    assert_eq!(
        read_json(&interrupted_candidate),
        json!({"partial": true}),
        "capture must preserve and ignore a prior interrupted candidate"
    );
}

#[test]
fn identical_existing_version_publication_fsyncs_versions_directory() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let first_destination = fixture.path().join("baselines/first.json");
    let first_result = fixture.path().join("results/first-result.json");
    let mut source = embedded_analyzer_definitions();
    source.push_str(
        r#"

first_destination = Path(baseline_path)
baseline = {
    "schema_version": baseline_schema,
    "capture_run_id": run_id,
    "payload": "identical-content-addressed-baseline",
}
first_version, first_candidate, first_digest = publish_baseline_json(
    first_destination, baseline, run_id
)
assert Path(first_version).is_file()
assert os.path.samefile(first_destination, first_version)

result_path = str(Path(result_path).with_name("second-result.json"))
second_destination = first_destination.with_name("second.json")
versions_directory = first_destination.parent / "versions"
versions_stat = os.stat(versions_directory)
versions_identity = (versions_stat.st_dev, versions_stat.st_ino)
versions_fsync_count = 0
real_os_fsync = os.fsync

def recording_os_fsync(descriptor):
    global versions_fsync_count
    descriptor_stat = os.fstat(descriptor)
    real_os_fsync(descriptor)
    if (descriptor_stat.st_dev, descriptor_stat.st_ino) == versions_identity:
        versions_fsync_count += 1

os.fsync = recording_os_fsync
try:
    assert Path(first_version).is_file()
    second_version, second_candidate, second_digest = publish_baseline_json(
        second_destination, baseline, run_id
    )
finally:
    os.fsync = real_os_fsync

assert first_version == second_version
assert first_digest == second_digest
assert first_candidate != second_candidate
assert versions_fsync_count >= 1
assert os.path.samefile(second_destination, first_version)
print(json.dumps({
    "same_version": True,
    "versions_fsync_count": versions_fsync_count,
}))
"#,
    );

    let mut child = Command::new("python3")
        .arg("-")
        .arg(fixture.path().join("unused-current.json"))
        .arg(&first_destination)
        .arg(&first_result)
        .arg("contract-test-commit")
        .arg(ROWS_PER_THREAD.to_string())
        .arg("true")
        .arg("fsqlite.perf_regression_gate.baseline.v3")
        .arg("fsqlite.perf_regression_gate.result.v2")
        .arg("fsqlite-e2e.mt_mvcc_bench_report.v6")
        .arg(ITERATIONS.to_string())
        .arg("0.05")
        .arg("0.0")
        .arg("measured")
        .arg("identical-publication")
        .arg(fixture.path().join("disposable-history.json"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn baseline publication analyzer");
    child
        .stdin
        .take()
        .expect("capture analyzer stdin")
        .write_all(source.as_bytes())
        .expect("write baseline publication analyzer");
    let output = child
        .wait_with_output()
        .expect("wait for baseline publication analyzer");
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: Value = serde_json::from_slice(&output.stdout).expect("parse fsync receipt");
    assert_eq!(receipt["same_version"], true);
    assert!(
        receipt["versions_fsync_count"]
            .as_u64()
            .expect("numeric versions-directory fsync count")
            >= 1
    );
}

#[test]
fn duplicate_run_id_fails_before_reusing_artifacts() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    write_json(&current_path, &fixture_report(833_333_333));
    let run_id = "contract-run-id-collision";

    let first = gate_output_with_run_id(&fixture, &current_path, true, run_id);
    assert_eq!(first.status.code(), Some(0), "{}", output_detail(&first));
    let result_before = fs::read(&first.result_path).expect("read first result");

    let second = gate_output_with_run_id(&fixture, &current_path, false, run_id);

    assert_eq!(second.status.code(), Some(2), "{}", output_detail(&second));
    assert!(String::from_utf8_lossy(&second.stderr).contains("exclusively create run directory"));
    assert_eq!(
        fs::read(&first.result_path).expect("reread first result"),
        result_before,
        "a duplicate run ID must not overwrite an existing result"
    );
}

#[test]
fn explicit_capture_still_refuses_to_overwrite_an_existing_baseline() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let current = fixture_report(833_333_333);
    capture(&fixture, &current_path, &current);
    let baseline_path = fixture.path().join("baselines/latest.json");
    let baseline_before = fs::read(&baseline_path).expect("read captured baseline");
    write_json(&current_path, &fixture_report(909_090_909));

    let output = gate_output(&fixture, &current_path, true);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    assert_eq!(
        fs::read(&baseline_path).expect("reread baseline"),
        baseline_before,
        "capture mode must not silently replace an existing baseline"
    );
}

#[test]
fn explicit_capture_refuses_a_broken_baseline_symlink() {
    use std::os::unix::fs::symlink;

    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    write_json(&current_path, &fixture_report(833_333_333));
    let baseline_dir = fixture.path().join("baselines");
    fs::create_dir_all(&baseline_dir).expect("create baseline fixture directory");
    let missing_target = fixture.path().join("missing-baseline-target.json");
    let baseline_path = baseline_dir.join("latest.json");
    symlink(&missing_target, &baseline_path).expect("create broken baseline symlink");

    let output = gate_output(&fixture, &current_path, true);

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    assert!(
        fs::symlink_metadata(&baseline_path)
            .expect("baseline symlink must remain present")
            .file_type()
            .is_symlink()
    );
    assert!(
        !missing_target.exists(),
        "capture must not follow a broken baseline symlink and create its target"
    );
}
