//! Contract tests for the bd-zywqc.2 performance regression guard.
//!
//! These tests invoke the real shell analyzer against complete v9 evidence and
//! exercise its measured path with a fake Cargo executable. They never run the
//! benchmark itself.

#![cfg(unix)]

// Retain the former v7 fixture suite as historical executable documentation.
// The active gate rejects v7, so these tests are intentionally not compiled.
#[rustfmt::skip]
#[cfg(any())]
mod legacy_v7 {

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
const BASELINE_SCHEMA: &str = "fsqlite.perf_regression_gate.baseline.v4";
const GATE_SCHEMA: &str = "fsqlite.perf_regression_gate.result.v3";
const REPORT_SCHEMA: &str = "fsqlite-e2e.mt_mvcc_bench_report.v7";
const EXECUTABLE_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CARGO_LOCK_SHA256: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const INVOCATION_SHA256: &str = "a344f09c2c54e4ded0af8977858be0ac3d06f3722cbfb4100450990d3e63bb17";
const SETTINGS_INTERPRETATION: &str = "Both engines proved the listed effective PRAGMA values; equal names and readbacks do not establish cross-engine semantic equivalence.";
const ACCOUNTING_INTERPRETATION: &str = "offered and committed writes share one row unit; attempted_writes counts physical INSERT calls; retried_operations records the existing engine-specific retry unit and is provenance only, not a cross-engine comparison metric.";
const TIMING_INTERPRETATION: &str = "workload_elapsed_ns begins only after every worker has opened and proved its effective settings, and ends at the last worker's transaction terminal point before connection teardown; worker_startup_elapsed_ns is reported separately.";
const NON_CITABLE_REASON: &str = "v7 binds the running executable, build/runtime source identity, Cargo.lock, invocation, toolchain, and measurement host to this same-invocation comparison, but bd-uh1fv still requires external watchdog, sanitized environment, matched retry/deadline semantics, a build-attested resolved dependency/feature-graph digest, counterbalanced topology receipts, immutable manifest, retained baseline history, and independent verification.";
const RELEASE_REGRESSION_SCOPE: &str = "Narrow same-process, same-host F/C writer-throughput comparison for only the requested mt-mvcc-bench workload/configurations; this report does not cover the shipped release profile, other workloads or platforms, long-term baseline retention, independent reproduction, or overall release eligibility.";
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
        .arg(BASELINE_SCHEMA)
        .arg(GATE_SCHEMA)
        .arg(REPORT_SCHEMA)
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

fn measured_shell_output_with_fake_cargo(
    fixture: &TempDir,
    run_id: &str,
    create_history: bool,
    capture_baseline: bool,
) -> Output {
    use std::os::unix::fs::PermissionsExt as _;

    let target_dir = fixture.path().join("target");
    let history_path = target_dir
        .join("regression_gate_runs")
        .join(run_id)
        .join("disposable_history.json");
    let mut report = fixture_report(833_333_333);
    report["pass_over_pass_gate"]["history_json_path"] = json!(history_path.to_string_lossy());
    report["subject_identity"]["build_source"]["build_nonce"] = json!(run_id);
    let report_path = fixture.path().join("fake-cargo-report.json");
    write_json(&report_path, &report);

    let fake_bin = fixture.path().join("fake-bin");
    fs::create_dir_all(&fake_bin).expect("create fake Cargo directory");
    let fake_cargo = fake_bin.join("cargo");
    fs::write(
        &fake_cargo,
r#"#!/usr/bin/env bash
set -euo pipefail
[[ "$PWD" == "$FSQLITE_FAKE_REPO_ROOT" ]]
if [[ "${1:-}" == "-V" ]]; then
    printf '%s\n' 'cargo fixture'
    exit 0
fi
json_output=''
history_path=''
for argument in "$@"; do
    case "$argument" in
        --json-output=*) json_output="${argument#--json-output=}" ;;
        --history-json=*) history_path="${argument#--history-json=}" ;;
    esac
done
[[ -n "$json_output" ]]
[[ -n "$history_path" ]]
[[ "${FSQLITE_BENCH_BUILD_NONCE:-}" == "$FSQLITE_FAKE_EXPECTED_NONCE" ]]
cp -- "$FSQLITE_FAKE_REPORT" "$json_output"
if [[ "$FSQLITE_FAKE_CREATE_HISTORY" == "true" ]]; then
    printf '%s\n' '{"forged":true}' > "$history_path"
fi
"#,
    )
    .expect("write fake Cargo executable");
    let mut permissions = fs::metadata(&fake_cargo)
        .expect("read fake Cargo metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_cargo, permissions).expect("make fake Cargo executable");

    let inherited_path = std::env::var_os("PATH").unwrap_or_default();
    let mut path_entries = vec![fake_bin];
    path_entries.extend(std::env::split_paths(&inherited_path));
    let test_path = std::env::join_paths(path_entries).expect("join fake Cargo PATH");

    let mut command = Command::new("bash");
    command
        .env("FSQLITE_REGGATE_RUN_ID", run_id)
        .env("FSQLITE_FAKE_EXPECTED_NONCE", run_id)
        .env("FSQLITE_FAKE_REPORT", &report_path)
        .env(
            "FSQLITE_FAKE_CREATE_HISTORY",
            if create_history { "true" } else { "false" },
        )
        .env("PATH", test_path)
        .arg(script_path())
        .arg("--target-dir")
        .arg(&target_dir)
        .arg("--baseline-dir")
        .arg(fixture.path().join("baselines"))
        .arg("--rows")
        .arg(ROWS_PER_THREAD.to_string());
    if capture_baseline {
        command.arg("--capture-baseline");
    }
    command
        .output()
        .expect("run measured regression guard with fake Cargo")
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

fn file_snapshot(hash: &str) -> Value {
    json!({
        "sha256": hash,
        "bytes_read": 3,
        "metadata_size_bytes": 3,
        "unix_device": 7,
        "unix_inode": 11,
        "error": null,
    })
}

fn subject_identity() -> Value {
    json!({
        "executable": {
            "current_exe_path": "/fixture/mt-mvcc-bench",
            "canonical_path": "/fixture/mt-mvcc-bench",
            "path_resolution_error": null,
            "process_id": 42,
            "before_measurement": file_snapshot(EXECUTABLE_SHA256),
            "after_measurement": file_snapshot(EXECUTABLE_SHA256),
            "unchanged_during_measurement": true,
        },
        "build_source": {
            "workspace_root": "/fixture/frankensqlite",
            "git_sha": "fixture-sha",
            "git_branch": "main",
            "git_tree_state": "clean",
            "build_nonce": "fixture-nonce",
            "build_input_tracking": "complete",
        },
        "runtime_source": {
            "before_measurement": {
                "workspace_root": "/fixture/frankensqlite",
                "canonical_workspace_root": "/fixture/frankensqlite",
                "git_sha": "fixture-sha",
                "git_branch": "main",
                "git_tree_state": "clean",
                "matches_build_git_sha": true,
                "discovery_errors": [],
            },
            "after_measurement": {
                "workspace_root": "/fixture/frankensqlite",
                "canonical_workspace_root": "/fixture/frankensqlite",
                "git_sha": "fixture-sha",
                "git_branch": "main",
                "git_tree_state": "clean",
                "matches_build_git_sha": true,
                "discovery_errors": [],
            },
            "same_clean_git_identity_at_capture_points": true,
            "stability_limitation": "fixture limitation",
        },
        "cargo_lock": {
            "embedded_build_sha256": CARGO_LOCK_SHA256,
            "embedded_build_size_bytes": 3,
            "runtime_path": "/fixture/frankensqlite/Cargo.lock",
            "before_measurement": file_snapshot(CARGO_LOCK_SHA256),
            "after_measurement": file_snapshot(CARGO_LOCK_SHA256),
            "before_matches_embedded_build": true,
            "after_matches_embedded_build": true,
            "unchanged_at_capture_points": true,
        },
    })
}

fn comparison_environment() -> Value {
    json!({
        "build_configuration": {
            "cargo_profile": "release",
            "selected_profile": "release-perf",
            "profile_label": "release-perf",
            "opt_level": "3",
            "debug": "false",
            "target": "fixture-target",
            "build_host": "fixture-host",
            "enabled_features": [],
            "rustflags": {
                "cargo_encoded_rustflags_present": false,
                "encoded_hex": "",
                "decoded_arguments": [],
                "decode_error": null,
            },
            "profile_overrides_hex": "",
            "native_build_overrides_hex": "",
            "rustc_version_verbose": "rustc fixture",
            "cargo_version": "cargo fixture",
            "resolved_dependency_feature_graph_sha256": null,
            "resolved_dependency_feature_graph_limitation": "fixture limitation",
        },
        "invocation": {
            "argv_lossy": ["mt-mvcc-bench"],
            "argv_raw_hex": ["6d742d6d7663632d62656e6368"],
            "raw_encoding": "unix_os_str_bytes",
            "length_prefixed_argv_sha256": INVOCATION_SHA256,
        },
        "measurement_host": {
            "host": {
                "hostname": "fixture-host",
                "cpu_model": "fixture-cpu",
                "available_parallelism": 64,
                "cpu_online": "0-63",
                "cpu_present": "0-63",
                "cpu_possible": "0-63",
                "cpu_isolated": null,
                "cpu_topology": {
                    "logical_cpu_directories": 64,
                    "physical_package_count": 1,
                    "physical_core_count": 32,
                },
                "scaling_governors_by_cpu": {},
                "kernel_release": "fixture-kernel",
                "kernel_version": "fixture-version",
                "numa_online_nodes": "0",
                "numa_possible_nodes": "0",
                "numa_node_directories": 1,
                "unavailable_fields": ["cpu_isolated", "scaling_governors_by_cpu"],
            },
            "before_measurement": {
                "unix_epoch_millis": 1,
                "process_cpu_affinity_mask": "ffffffffffffffff",
                "process_cpu_affinity_list": "0-63",
                "proc_self_cgroup": "0::/fixture",
                "cpuset_cpus_effective": "0-63",
                "cpuset_mems_effective": "0",
                "load_average": "0.00 0.00 0.00 1/1 1",
                "pressure_cpu": "some avg10=0.00",
                "pressure_memory": "some avg10=0.00",
                "pressure_io": "some avg10=0.00",
            },
            "after_measurement": {
                "unix_epoch_millis": 2,
                "process_cpu_affinity_mask": "ffffffffffffffff",
                "process_cpu_affinity_list": "0-63",
                "proc_self_cgroup": "0::/fixture",
                "cpuset_cpus_effective": "0-63",
                "cpuset_mems_effective": "0",
                "load_average": "0.00 0.00 0.00 1/1 1",
                "pressure_cpu": "some avg10=0.00",
                "pressure_memory": "some avg10=0.00",
                "pressure_io": "some avg10=0.00",
            },
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
        "schema_version": REPORT_SCHEMA,
        "citable": false,
        "measurement_evidence_valid": true,
        "non_citable_reason": NON_CITABLE_REASON,
        "release_regression_scope": RELEASE_REGRESSION_SCOPE,
        "subject_identity": subject_identity(),
        "comparison_environment": comparison_environment(),
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
            "status": "disabled_non_citable",
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
    assert_eq!(baseline["schema_version"], BASELINE_SCHEMA);
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
fn measured_v7_run_requires_history_path_to_remain_absent() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let run_id = "measured-v7-no-history";

    let output = measured_shell_output_with_fake_cargo(&fixture, run_id, false, true);

    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fixture.path().join("baselines/latest.json").is_file());
    assert!(
        !fixture
            .path()
            .join("target/regression_gate_runs")
            .join(run_id)
            .join("disposable_history.json")
            .exists(),
        "a successful non-citable v7 run must not create history"
    );
}

#[test]
fn measured_v7_run_rejects_any_history_write_before_analysis() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let run_id = "measured-v7-forged-history";

    let output = measured_shell_output_with_fake_cargo(&fixture, run_id, true, true);

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("non-citable v7 benchmark unexpectedly created its history path")
    );
    assert!(!fixture.path().join("baselines/latest.json").exists());
}

#[test]
fn measured_baseline_and_candidate_bind_their_own_build_nonces() {
    let fixture = tempfile::tempdir().expect("tempdir");

    let capture =
        measured_shell_output_with_fake_cargo(&fixture, "measured-v7-baseline-nonce", false, true);
    assert_eq!(
        capture.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&capture.stdout),
        String::from_utf8_lossy(&capture.stderr)
    );

    let comparison = measured_shell_output_with_fake_cargo(
        &fixture,
        "measured-v7-candidate-nonce",
        false,
        false,
    );
    assert_eq!(
        comparison.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&comparison.stdout),
        String::from_utf8_lossy(&comparison.stderr)
    );
}

#[test]
fn measured_report_rejects_a_stale_build_nonce() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let run_id = "measured-v7-expected-nonce";
    let history_path = fixture
        .path()
        .join("target/regression_gate_runs")
        .join(run_id)
        .join("disposable_history.json");
    let mut report = fixture_report(833_333_333);
    report["pass_over_pass_gate"]["history_json_path"] = json!(history_path.to_string_lossy());
    report["subject_identity"]["build_source"]["build_nonce"] = json!("stale-build-nonce");
    let report_path = fixture.path().join("stale-build-report.json");
    write_json(&report_path, &report);
    let result_path = fixture.path().join("results/stale-build-nonce.json");

    let output = measured_analyzer_output(
        &report_path,
        &fixture.path().join("baselines/latest.json"),
        &result_path,
        true,
        run_id,
        &history_path,
    );

    assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
    assert!(
        read_json(&result_path)["error"]
            .as_str()
            .expect("stale nonce error")
            .contains("build_nonce does not match its measured run")
    );
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
fn v7_measurement_environment_drift_fails_closed() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let baseline_report = fixture_report(833_333_333);
    capture(&fixture, &current_path, &baseline_report);

    for drift in ["cpu_model", "hostname", "affinity", "target", "profile"] {
        let mut current = baseline_report.clone();
        match drift {
            "cpu_model" => {
                current["comparison_environment"]["measurement_host"]["host"]["cpu_model"] =
                    json!("different-cpu");
            }
            "hostname" => {
                current["comparison_environment"]["measurement_host"]["host"]["hostname"] =
                    json!("different-host");
            }
            "affinity" => {
                for capture_point in ["before_measurement", "after_measurement"] {
                    current["comparison_environment"]["measurement_host"][capture_point]["process_cpu_affinity_list"] =
                        json!("0-31");
                }
            }
            "target" => {
                current["comparison_environment"]["build_configuration"]["target"] =
                    json!("different-target");
            }
            "profile" => {
                current["comparison_environment"]["build_configuration"]["selected_profile"] =
                    json!("release");
            }
            _ => unreachable!("complete drift fixture list"),
        }
        write_json(&current_path, &current);

        let output = gate_output(&fixture, &current_path, false);

        assert_eq!(
            output.status.code(),
            Some(2),
            "drift={drift}\n{}",
            output_detail(&output)
        );
        let error = read_json(&output.result_path)["error"]
            .as_str()
            .expect("environment drift error")
            .to_owned();
        assert!(
            error.contains("incompatible v7 measurement environments")
                || error.contains("was not built with the release-perf profile"),
            "drift={drift}: {error}"
        );
    }
}

#[test]
fn expected_per_run_v7_identity_and_dynamic_host_changes_remain_comparable() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let baseline_report = fixture_report(833_333_333);
    capture(&fixture, &current_path, &baseline_report);
    let mut current = baseline_report;
    current["subject_identity"]["build_source"]["git_sha"] = json!("candidate-sha");
    current["subject_identity"]["runtime_source"]["before_measurement"]["git_sha"] =
        json!("candidate-sha");
    current["subject_identity"]["runtime_source"]["after_measurement"]["git_sha"] =
        json!("candidate-sha");
    current["comparison_environment"]["invocation"]["argv_lossy"] =
        json!(["mt-mvcc-bench", "--different-output-path"]);
    current["comparison_environment"]["measurement_host"]["before_measurement"]["unix_epoch_millis"] =
        json!(10);
    current["comparison_environment"]["measurement_host"]["after_measurement"]["unix_epoch_millis"] =
        json!(20);
    current["comparison_environment"]["measurement_host"]["before_measurement"]["load_average"] =
        json!("1.00 2.00 3.00 1/1 1");
    current["comparison_environment"]["measurement_host"]["after_measurement"]["pressure_cpu"] =
        json!("some avg10=1.00");
    write_json(&current_path, &current);

    let output = gate_output(&fixture, &current_path, false);

    assert_eq!(output.status.code(), Some(0), "{}", output_detail(&output));
    let result = read_json(&output.result_path);
    assert_eq!(result["verdict"], "diagnostic_only");
    assert_eq!(result["release_evidence"], false);
    assert_eq!(result["release_eligible"], false);
}

#[test]
fn baseline_history_path_receipt_is_independent_of_the_current_report_path() {
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
fn measured_mode_binds_current_and_baseline_to_their_absent_history_paths() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let baseline_path = fixture.path().join("baselines/latest.json");
    let baseline_report_path = fixture.path().join("baseline-report.json");
    let baseline_history = fixture
        .path()
        .join("target/regression_gate_runs/measured-baseline/disposable_history.json");
    let mut baseline_report = fixture_report(833_333_333);
    baseline_report["pass_over_pass_gate"]["history_json_path"] =
        json!(baseline_history.to_string_lossy());
    baseline_report["subject_identity"]["build_source"]["build_nonce"] = json!("measured-baseline");
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
    current_report["subject_identity"]["build_source"]["build_nonce"] = json!("measured-current");
    let mismatched_history = fixture
        .path()
        .join("target/regression_gate_runs/measured-other/disposable_history.json");
    let mismatch_result = fixture.path().join("results/history-mismatch.json");
    let mut mismatch_report = current_report.clone();
    mismatch_report["subject_identity"]["build_source"]["build_nonce"] =
        json!("measured-current-mismatch");
    write_json(&current_report_path, &mismatch_report);
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

    let mut forged_history_report = current_report.clone();
    forged_history_report["pass_over_pass_gate"]["previous_report_found"] = json!(true);
    forged_history_report["subject_identity"]["build_source"]["build_nonce"] =
        json!("measured-current-forged-history");
    write_json(&current_report_path, &forged_history_report);
    let forged_result = fixture.path().join("results/forged-history.json");
    let forged = measured_analyzer_output(
        &current_report_path,
        &baseline_path,
        &forged_result,
        false,
        "measured-current-forged-history",
        &current_history,
    );
    assert_eq!(forged.status.code(), Some(2), "{}", output_detail(&forged));
    assert!(
        read_json(&forged_result)["error"]
            .as_str()
            .expect("forged history error")
            .contains("unexpectedly found per-run history")
    );
    write_json(&current_report_path, &current_report);

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
fn citable_v7_claim_is_invalid_for_this_diagnostic_gate() {
    let mut malformed = fixture_report(833_333_333);
    malformed["citable"] = json!(true);

    assert_invalid_capture(&malformed);
}

#[test]
fn invalid_measurement_evidence_cannot_seed_a_diagnostic_baseline() {
    let mut malformed = fixture_report(833_333_333);
    malformed["measurement_evidence_valid"] = json!(false);

    assert_invalid_capture(&malformed);
}

#[test]
fn v7_provenance_receipts_and_release_perf_profile_are_required() {
    for field in ["subject_identity", "comparison_environment"] {
        let mut missing = fixture_report(833_333_333);
        missing
            .as_object_mut()
            .expect("fixture report object")
            .remove(field);
        assert_invalid_capture(&missing);
    }

    let mut missing_executable = fixture_report(833_333_333);
    missing_executable["subject_identity"]
        .as_object_mut()
        .expect("subject identity object")
        .remove("executable");
    assert_invalid_capture(&missing_executable);

    let mut empty_executable = fixture_report(833_333_333);
    empty_executable["subject_identity"]["executable"] = json!({});
    assert_invalid_capture(&empty_executable);

    let mut missing_host_capture = fixture_report(833_333_333);
    missing_host_capture["comparison_environment"]["measurement_host"]
        .as_object_mut()
        .expect("measurement host object")
        .remove("after_measurement");
    assert_invalid_capture(&missing_host_capture);

    let mut wrong_profile = fixture_report(833_333_333);
    wrong_profile["comparison_environment"]["build_configuration"]["selected_profile"] =
        json!("release");
    assert_invalid_capture(&wrong_profile);

    let mut unstable_placement = fixture_report(833_333_333);
    unstable_placement["comparison_environment"]["measurement_host"]["after_measurement"]["process_cpu_affinity_list"] =
        json!("0-31");
    assert_invalid_capture(&unstable_placement);
}

#[test]
fn v7_executable_receipt_must_be_complete_and_stable() {
    let valid = fixture_report(833_333_333);
    let mut reported_changed = valid.clone();
    reported_changed["subject_identity"]["executable"]["unchanged_during_measurement"] =
        json!(false);
    assert_invalid_capture(&reported_changed);

    let mut indeterminate_stability = valid.clone();
    indeterminate_stability["subject_identity"]["executable"]["unchanged_during_measurement"] =
        Value::Null;
    assert_invalid_capture(&indeterminate_stability);

    let mut snapshot_error = valid.clone();
    snapshot_error["subject_identity"]["executable"]["before_measurement"]["error"] =
        json!("fixture read failure");
    assert_invalid_capture(&snapshot_error);

    let mut malformed_digest = valid.clone();
    malformed_digest["subject_identity"]["executable"]["before_measurement"]["sha256"] =
        json!("not-a-sha256");
    assert_invalid_capture(&malformed_digest);

    let mut forged_stability = valid.clone();
    forged_stability["subject_identity"]["executable"]["after_measurement"]["unix_inode"] =
        json!(12);
    assert_invalid_capture(&forged_stability);

    let mut unresolved_path = valid;
    unresolved_path["subject_identity"]["executable"]["canonical_path"] = Value::Null;
    unresolved_path["subject_identity"]["executable"]["path_resolution_error"] =
        json!("fixture canonicalization failure");
    assert_invalid_capture(&unresolved_path);
}

#[test]
fn v7_build_and_runtime_source_must_be_clean_bound_and_stable() {
    let valid = fixture_report(833_333_333);
    let mut dirty_build = valid.clone();
    dirty_build["subject_identity"]["build_source"]["git_tree_state"] = json!("dirty");
    assert_invalid_capture(&dirty_build);

    let mut unknown_nonce = valid.clone();
    unknown_nonce["subject_identity"]["build_source"]["build_nonce"] = json!("unknown");
    assert_invalid_capture(&unknown_nonce);

    let mut incomplete_input_tracking = valid.clone();
    incomplete_input_tracking["subject_identity"]["build_source"]["build_input_tracking"] =
        json!("unavailable");
    assert_invalid_capture(&incomplete_input_tracking);

    let mut dirty_runtime = valid.clone();
    dirty_runtime["subject_identity"]["runtime_source"]["before_measurement"]["git_tree_state"] =
        json!("dirty");
    assert_invalid_capture(&dirty_runtime);

    let mut mismatched_build = valid.clone();
    mismatched_build["subject_identity"]["runtime_source"]["after_measurement"]["matches_build_git_sha"] =
        json!(false);
    assert_invalid_capture(&mismatched_build);

    let mut forged_match_boolean = valid.clone();
    forged_match_boolean["subject_identity"]["runtime_source"]["after_measurement"]["git_sha"] =
        json!("different-fixture-sha");
    assert_invalid_capture(&forged_match_boolean);

    let mut discovery_error = valid.clone();
    discovery_error["subject_identity"]["runtime_source"]["before_measurement"]["discovery_errors"] =
        json!(["fixture git failure"]);
    assert_invalid_capture(&discovery_error);

    let mut changed_branch = valid.clone();
    changed_branch["subject_identity"]["runtime_source"]["after_measurement"]["git_branch"] =
        json!("release-candidate");
    assert_invalid_capture(&changed_branch);

    let mut indeterminate_stability = valid;
    indeterminate_stability["subject_identity"]["runtime_source"]["same_clean_git_identity_at_capture_points"] =
        Value::Null;
    assert_invalid_capture(&indeterminate_stability);
}

#[test]
fn v7_cargo_lock_receipt_must_match_the_build_and_remain_stable() {
    let valid = fixture_report(833_333_333);
    let mut mismatched_embedded_build = valid.clone();
    mismatched_embedded_build["subject_identity"]["cargo_lock"]["before_matches_embedded_build"] =
        json!(false);
    assert_invalid_capture(&mismatched_embedded_build);

    let mut indeterminate_match = valid.clone();
    indeterminate_match["subject_identity"]["cargo_lock"]["after_matches_embedded_build"] =
        Value::Null;
    assert_invalid_capture(&indeterminate_match);

    let mut reported_changed = valid.clone();
    reported_changed["subject_identity"]["cargo_lock"]["unchanged_at_capture_points"] =
        json!(false);
    assert_invalid_capture(&reported_changed);

    let mut snapshot_error = valid.clone();
    snapshot_error["subject_identity"]["cargo_lock"]["after_measurement"]["error"] =
        json!("fixture lockfile read failure");
    assert_invalid_capture(&snapshot_error);

    let mut forged_embedded_digest = valid.clone();
    forged_embedded_digest["subject_identity"]["cargo_lock"]["embedded_build_sha256"] =
        json!(EXECUTABLE_SHA256);
    assert_invalid_capture(&forged_embedded_digest);

    let mut forged_stability = valid;
    forged_stability["subject_identity"]["cargo_lock"]["after_measurement"]["unix_inode"] =
        json!(12);
    assert_invalid_capture(&forged_stability);
}

#[test]
fn v7_build_flags_and_invocation_must_be_decodable_and_self_consistent() {
    let valid = fixture_report(833_333_333);
    let mut rustflags_decode_error = valid.clone();
    rustflags_decode_error["comparison_environment"]["build_configuration"]["rustflags"]["decode_error"] =
        json!("fixture decode failure");
    assert_invalid_capture(&rustflags_decode_error);

    let mut inconsistent_decoded_rustflags = valid.clone();
    inconsistent_decoded_rustflags["comparison_environment"]["build_configuration"]["rustflags"]
        ["decoded_arguments"] = json!(["-Ctarget-cpu=native"]);
    assert_invalid_capture(&inconsistent_decoded_rustflags);

    let mut malformed_profile_overrides = valid.clone();
    malformed_profile_overrides["comparison_environment"]["build_configuration"]["profile_overrides_hex"] =
        json!("not-hex");
    assert_invalid_capture(&malformed_profile_overrides);

    let mut missing_raw_argument = valid.clone();
    missing_raw_argument["comparison_environment"]["invocation"]["argv_raw_hex"] = json!([]);
    assert_invalid_capture(&missing_raw_argument);

    let mut malformed_raw_argument = valid.clone();
    malformed_raw_argument["comparison_environment"]["invocation"]["argv_raw_hex"][0] =
        json!("XYZ");
    assert_invalid_capture(&malformed_raw_argument);

    let mut forged_invocation_digest = valid;
    forged_invocation_digest["comparison_environment"]["invocation"]["length_prefixed_argv_sha256"] =
        json!(EXECUTABLE_SHA256);
    assert_invalid_capture(&forged_invocation_digest);
}

#[test]
fn v7_host_receipt_requires_essential_identity_and_stable_placement() {
    let valid = fixture_report(833_333_333);
    let mut missing_cpu_model = valid.clone();
    missing_cpu_model["comparison_environment"]["measurement_host"]["host"]["cpu_model"] =
        Value::Null;
    assert_invalid_capture(&missing_cpu_model);

    let mut missing_topology = valid.clone();
    missing_topology["comparison_environment"]["measurement_host"]["host"]["cpu_topology"]["logical_cpu_directories"] =
        Value::Null;
    assert_invalid_capture(&missing_topology);

    let mut missing_placement = valid.clone();
    missing_placement["comparison_environment"]["measurement_host"]["before_measurement"]["cpuset_cpus_effective"] =
        Value::Null;
    assert_invalid_capture(&missing_placement);

    let mut changed_placement = valid.clone();
    changed_placement["comparison_environment"]["measurement_host"]["after_measurement"]["proc_self_cgroup"] =
        json!("0::/different-fixture");
    assert_invalid_capture(&changed_placement);

    let mut reversed_timestamps = valid.clone();
    reversed_timestamps["comparison_environment"]["measurement_host"]["after_measurement"]["unix_epoch_millis"] =
        json!(0);
    assert_invalid_capture(&reversed_timestamps);

    let mut capacity_mismatch = valid.clone();
    capacity_mismatch["comparison_environment"]["measurement_host"]["host"]["available_parallelism"] =
        json!(32);
    assert_invalid_capture(&capacity_mismatch);

    let mut contradictory_unavailable_fields = valid;
    contradictory_unavailable_fields["comparison_environment"]["measurement_host"]["host"]["unavailable_fields"] =
        json!([]);
    assert_invalid_capture(&contradictory_unavailable_fields);
}

#[test]
fn optional_v7_host_and_dependency_fields_may_remain_unavailable() {
    let mut report = fixture_report(833_333_333);
    report["comparison_environment"]["measurement_host"]["host"]["numa_online_nodes"] = Value::Null;
    report["comparison_environment"]["measurement_host"]["host"]["numa_possible_nodes"] =
        Value::Null;
    report["comparison_environment"]["measurement_host"]["host"]["numa_node_directories"] =
        Value::Null;
    report["comparison_environment"]["measurement_host"]["host"]["cpu_topology"]["physical_package_count"] =
        Value::Null;
    report["comparison_environment"]["measurement_host"]["host"]["cpu_topology"]["physical_core_count"] =
        Value::Null;
    report["comparison_environment"]["measurement_host"]["host"]["unavailable_fields"] = json!([
        "numa_online_nodes",
        "numa_possible_nodes",
        "numa_node_directories",
        "cpu_isolated",
        "scaling_governors_by_cpu",
    ]);
    for capture in ["before_measurement", "after_measurement"] {
        for field in [
            "load_average",
            "pressure_cpu",
            "pressure_memory",
            "pressure_io",
        ] {
            report["comparison_environment"]["measurement_host"][capture][field] = Value::Null;
        }
    }

    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    capture(&fixture, &current_path, &report);
}

#[test]
fn v7_schema_and_scope_contract_deviations_are_invalid() {
    let mut legacy_schema = fixture_report(833_333_333);
    legacy_schema["schema_version"] = json!("fsqlite-e2e.mt_mvcc_bench_report.v6");
    assert_invalid_capture(&legacy_schema);

    let mut rewritten_reason = fixture_report(833_333_333);
    rewritten_reason["non_citable_reason"] = json!("trust this report");
    assert_invalid_capture(&rewritten_reason);

    let mut broadened_scope = fixture_report(833_333_333);
    broadened_scope["release_regression_scope"] = json!("all release workloads and platforms");
    assert_invalid_capture(&broadened_scope);
}

#[test]
fn v7_report_cannot_smuggle_a_release_claim() {
    for field in ["release_eligible", "release_evidence"] {
        let mut malformed = fixture_report(833_333_333);
        malformed[field] = json!(true);
        assert_invalid_capture(&malformed);
    }
}

#[test]
fn embedded_pass_over_pass_receipt_must_stay_disabled_and_empty() {
    let mut forged_pass = fixture_report(833_333_333);
    forged_pass["pass_over_pass_gate"]["status"] = json!("passed");
    assert_invalid_capture(&forged_pass);

    let mut forged_pair = fixture_report(833_333_333);
    forged_pair["pass_over_pass_gate"]["comparable_pair_count"] = json!(1);
    assert_invalid_capture(&forged_pair);

    let mut forged_regression = fixture_report(833_333_333);
    forged_regression["pass_over_pass_gate"]["regressions"] = json!([{
        "threads": 8,
        "previous_ratio": 1.0,
        "current_ratio": 0.9,
        "ratio_drop_pct": 10.0,
    }]);
    assert_invalid_capture(&forged_regression);
}

#[test]
fn analyze_only_history_presence_is_diagnostic_not_comparability() {
    let fixture = tempfile::tempdir().expect("tempdir");
    let current_path = fixture.path().join("current.json");
    let mut report = fixture_report(833_333_333);
    report["pass_over_pass_gate"]["previous_report_found"] = json!(true);
    write_json(&current_path, &report);

    let capture = gate_output(&fixture, &current_path, true);
    assert_eq!(
        capture.status.code(),
        Some(0),
        "{}",
        output_detail(&capture)
    );
    let baseline = read_json(&fixture.path().join("baselines/latest.json"));
    assert_eq!(
        baseline["report"]["pass_over_pass_gate"]["status"],
        "disabled_non_citable"
    );
    assert_eq!(
        baseline["report"]["pass_over_pass_gate"]["comparable_pair_count"],
        0
    );
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
        .arg(BASELINE_SCHEMA)
        .arg(GATE_SCHEMA)
        .arg(REPORT_SCHEMA)
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

}

mod v9 {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use tempfile::TempDir;

    const ITERATIONS: usize = 21;
    const ROWS_PER_THREAD: usize = 500;
    const REPORT_SCHEMA: &str = "fsqlite-e2e.mt_mvcc_bench_report.v9";
    const BASELINE_SCHEMA: &str = "fsqlite.perf_regression_gate.baseline.v6";
    const GATE_SCHEMA: &str = "fsqlite.perf_regression_gate.result.v5";
    const FIXTURE_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const EXECUTABLE_SHA256: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const CARGO_LOCK_SHA256: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const NON_CITABLE_REASON: &str = "v9 extends the explicit one-row transaction/retry-unit contract with retryable statement-preparation truth under the shared worker deadline and an exact v2 FSQLite retry identity; it retains an optional build-attested resolved dependency/feature-graph digest, but remains non-citable: bd-uh1fv still requires an external watchdog, sanitized environment, matched retry/deadline semantics, counterbalanced topology receipts, immutable manifest, retained baseline history, and independent verification; a default build also leaves the graph digest unavailable.";
    const RELEASE_SCOPE: &str = "Narrow same-process, same-host F/C writer-throughput comparison for only this report's attested selected Cargo profile and the requested mt-mvcc-bench workload/configurations; this report does not cover other workloads or platforms, long-term baseline retention, independent reproduction, or overall release eligibility.";
    const SETTINGS_INTERPRETATION: &str = "Both engines proved the listed effective PRAGMA values; equal names and readbacks do not establish cross-engine semantic equivalence.";
    const ACCOUNTING_INTERPRETATION: &str = "offered and committed writes share one row unit; attempted_writes counts physical INSERT calls; retried_operations records the existing engine-specific retry unit and is provenance only, not a cross-engine comparison metric.";
    const TIMING_INTERPRETATION: &str = "workload_elapsed_ns begins only after every worker has opened and proved its effective settings, and ends at the last worker's transaction terminal point before connection teardown; worker_startup_elapsed_ns is reported separately.";
    const GRAPH_LIMITATION: &str = "available: the lowercase SHA-256 was supplied at build time through the rerun-sensitive FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256 attestation input";
    const CSQLITE_RETRY_UNIT: &str = "whole one-row BEGIN/INSERT/COMMIT transaction attempt";
    const FSQLITE_RETRY_UNIT: &str =
        "statement preparation or whole one-row BEGIN CONCURRENT/INSERT/COMMIT transaction attempt";
    const CSQLITE_RETRY_ALGORITHM: &str = "csqlite.whole-one-row-transaction.fixed-1ms.busy-or-locked.max-512-or-shared-worker-timeout.v1";
    const FSQLITE_RETRY_ALGORITHM: &str = "fsqlite.prepare-or-whole-one-row-transaction.step-exp-every-8-cap-25ms-plus-thread-attempt-jitter-0-to-4ms.max-512-or-shared-worker-timeout.v2";
    const V8_FSQLITE_RETRY_UNIT: &str =
        "whole one-row BEGIN CONCURRENT/INSERT/COMMIT transaction attempt";
    const V8_FSQLITE_RETRY_ALGORITHM: &str = "fsqlite.whole-one-row-transaction.step-exp-every-8-cap-25ms-plus-thread-attempt-jitter-0-to-4ms.max-512-or-shared-worker-timeout.v1";
    const RETRYABLE_ERRORS: &str = "Busy|BusyRecovery|BusySnapshot|DatabaseLocked|WriteConflict|SerializationFailure|PageBufferCapacityExhausted";
    const SQLITE_ELAPSED_NS: u64 = 1_000_000_000;
    static NEXT_RUN_ID: AtomicUsize = AtomicUsize::new(1);

    fn unresolved_release_coverage() -> Value {
        json!([
            "32-writer cell",
            "balanced, write-heavy, and read-heavy macro workloads",
            "INSERT, SELECT-by-primary-key, and UPDATE micro workloads",
            "calibration receipt",
            "synthetic-regression sensitivity proof",
            "flamegraph evidence",
            "rolling 30-day retained baselines",
            "historical baseline/current paired block deltas; this gate uses independent samples",
            "non-host release platforms",
            "external watchdog enforcement",
            "sanitized benchmark environment",
            "matched cross-engine retry/deadline semantics",
            "counterbalanced topology receipts",
            "immutable measurement manifest",
            "independent verification",
            "pinned-host enforcement",
        ])
    }

    fn assert_diagnostic_only(value: &Value) {
        assert_eq!(value["release_evidence"], false);
        assert_eq!(value["release_eligible"], false);
        assert_eq!(
            value["unresolved_release_coverage"],
            unresolved_release_coverage(),
        );
    }

    #[derive(Clone)]
    struct ReportPair {
        release: Value,
        release_perf: Value,
    }

    struct GateRun {
        output: Output,
        result_path: PathBuf,
        run_dir: PathBuf,
    }

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("fsqlite-e2e must live under crates/")
            .canonicalize()
            .expect("canonicalize repository root")
    }

    fn script_path() -> PathBuf {
        repo_root().join("scripts/perf_regression_gate.sh")
    }

    fn sha256(bytes: &[u8]) -> String {
        lower_hex(&Sha256::digest(bytes))
    }

    fn lower_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    fn graph_value(target: &str) -> Value {
        json!({
            "schema_version": "fsqlite.dependency_feature_graph.v1",
            "command": [
                "cargo", "tree", "--locked", "--offline", "-p", "fsqlite-e2e",
                "-e", "features,no-dev", "--no-default-features", "--target", target,
            ],
            "target": target,
            "tree": "fsqlite-e2e v0.1.0 (${WORKSPACE_ROOT}/crates/fsqlite-e2e)\n",
        })
    }

    fn canonical_graph_bytes(graph: &Value) -> Vec<u8> {
        let object = graph.as_object().expect("graph object");
        let sorted = object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut bytes = serde_json::to_vec(&sorted).expect("serialize canonical graph");
        bytes.push(b'\n');
        bytes
    }

    fn graph_bytes(target: &str) -> Vec<u8> {
        canonical_graph_bytes(&graph_value(target))
    }

    fn write_graph(path: &Path, target: &str) -> String {
        let bytes = graph_bytes(target);
        fs::write(path, &bytes).expect("write graph fixture");
        sha256(&bytes)
    }

    fn write_graph_value(path: &Path, graph: &Value) -> String {
        let bytes = canonical_graph_bytes(graph);
        fs::write(path, &bytes).expect("write graph fixture");
        sha256(&bytes)
    }

    fn snapshot(digest: &str) -> Value {
        json!({
            "sha256": digest,
            "bytes_read": 3,
            "metadata_size_bytes": 3,
            "unix_device": 7,
            "unix_inode": 11,
            "error": null,
        })
    }

    fn subject_identity(commit: &str, nonce: &str) -> Value {
        json!({
            "executable": {
                "current_exe_path": "/fixture/mt-mvcc-bench",
                "canonical_path": "/fixture/mt-mvcc-bench",
                "path_resolution_error": null,
                "process_id": 42,
                "before_measurement": snapshot(EXECUTABLE_SHA256),
                "after_measurement": snapshot(EXECUTABLE_SHA256),
                "unchanged_during_measurement": true,
            },
            "build_source": {
                "workspace_root": "/fixture/frankensqlite",
                "git_sha": commit,
                "git_branch": "main",
                "git_tree_state": "clean",
                "build_nonce": nonce,
                "build_input_tracking": "complete",
            },
            "runtime_source": {
                "before_measurement": runtime_source(commit),
                "after_measurement": runtime_source(commit),
                "same_clean_git_identity_at_capture_points": true,
                "stability_limitation": "fixture captures only before and after identity",
            },
            "cargo_lock": {
                "embedded_build_sha256": CARGO_LOCK_SHA256,
                "embedded_build_size_bytes": 3,
                "runtime_path": "/fixture/frankensqlite/Cargo.lock",
                "before_measurement": snapshot(CARGO_LOCK_SHA256),
                "after_measurement": snapshot(CARGO_LOCK_SHA256),
                "before_matches_embedded_build": true,
                "after_matches_embedded_build": true,
                "unchanged_at_capture_points": true,
            },
        })
    }

    fn runtime_source(commit: &str) -> Value {
        json!({
            "workspace_root": "/fixture/frankensqlite",
            "canonical_workspace_root": "/fixture/frankensqlite",
            "git_sha": commit,
            "git_branch": "main",
            "git_tree_state": "clean",
            "matches_build_git_sha": true,
            "discovery_errors": [],
        })
    }

    fn invocation(arguments: &[String]) -> Value {
        let raw = arguments
            .iter()
            .map(|argument| lower_hex(argument.as_bytes()))
            .collect::<Vec<_>>();
        let mut canonical = Vec::new();
        for argument in arguments {
            canonical.extend_from_slice(
                &u64::try_from(argument.len())
                    .expect("argument length fits u64")
                    .to_le_bytes(),
            );
            canonical.extend_from_slice(argument.as_bytes());
        }
        json!({
            "argv_lossy": arguments,
            "argv_raw_hex": raw,
            "raw_encoding": "unix_os_str_bytes",
            "length_prefixed_argv_sha256": sha256(&canonical),
        })
    }

    fn build_configuration(profile: &str, target: &str, graph_digest: &str) -> Value {
        let (selected, label, opt_level) = match profile {
            "release" => ("release", "release", "z"),
            "release-perf" => ("release-perf", "release-perf", "3"),
            _ => panic!("unexpected fixture profile"),
        };
        json!({
            "cargo_profile": "release",
            "selected_profile": selected,
            "profile_label": label,
            "opt_level": opt_level,
            "debug": "false",
            "target": target,
            "build_host": "fixture-host-target",
            "enabled_features": [],
            "rustflags": {
                "cargo_encoded_rustflags_present": false,
                "encoded_hex": "",
                "decoded_arguments": [],
                "decode_error": null,
            },
            "profile_overrides_hex": "",
            "native_build_overrides_hex": "",
            "rustc_version_verbose": "rustc fixture",
            "cargo_version": "cargo fixture",
            "resolved_dependency_feature_graph_sha256": graph_digest,
            "resolved_dependency_feature_graph_limitation": GRAPH_LIMITATION,
        })
    }

    fn measurement_host() -> Value {
        json!({
            "host": {
                "hostname": "fixture-host",
                "cpu_model": "fixture-cpu",
                "available_parallelism": 64,
                "cpu_online": "0-63",
                "cpu_present": "0-63",
                "cpu_possible": "0-63",
                "cpu_isolated": null,
                "cpu_topology": {
                    "logical_cpu_directories": 64,
                    "physical_package_count": 1,
                    "physical_core_count": 32,
                },
                "scaling_governors_by_cpu": {},
                "kernel_release": "fixture-kernel",
                "kernel_version": "fixture-version",
                "numa_online_nodes": "0",
                "numa_possible_nodes": "0",
                "numa_node_directories": 1,
                "unavailable_fields": ["cpu_isolated", "scaling_governors_by_cpu"],
            },
            "before_measurement": dynamic_host(1),
            "after_measurement": dynamic_host(2),
        })
    }

    fn dynamic_host(timestamp: usize) -> Value {
        json!({
            "unix_epoch_millis": timestamp,
            "process_cpu_affinity_mask": "ffffffffffffffff",
            "process_cpu_affinity_list": "0-63",
            "proc_self_cgroup": "0::/fixture",
            "cpuset_cpus_effective": "0-63",
            "cpuset_mems_effective": "0",
            "load_average": "0.00 0.00 0.00 1/1 1",
            "pressure_cpu": "some avg10=0.00",
            "pressure_memory": "some avg10=0.00",
            "pressure_io": "some avg10=0.00",
        })
    }

    fn retry_policy(threads: usize) -> Value {
        let timeout_ms = (5 + threads * ROWS_PER_THREAD / 5_000) * 1_000;
        json!({
            "csqlite_busy_timeout_ms": 5000,
            "csqlite_max_operation_retries": 0,
            "csqlite_max_transaction_retries": 512,
            "csqlite_retry_sleep_ms": 1,
            "csqlite_retry_unit": CSQLITE_RETRY_UNIT,
            "csqlite_retry_algorithm": CSQLITE_RETRY_ALGORITHM,
            "shared_worker_retry_timeout_ms": timeout_ms,
            "shared_worker_retry_timeout_overridden": false,
            "fsqlite_transaction_timeout_ms": timeout_ms,
            "fsqlite_max_transaction_retries": 512,
            "fsqlite_retry_sleep_base_ms": 1,
            "fsqlite_retry_sleep_cap_ms": 29,
            "fsqlite_retry_unit": FSQLITE_RETRY_UNIT,
            "fsqlite_retry_backoff_algorithm": FSQLITE_RETRY_ALGORITHM,
            "fsqlite_retryable_errors": RETRYABLE_ERRORS,
            "fsqlite_timeout_overridden": false,
        })
    }

    fn configuration(threads: usize) -> Value {
        json!({
            "writers": threads,
            "available_parallelism": 64,
            "max_supported_writers": 128,
            "wal_autocheckpoint_pages": 1000,
            "wal_autocheckpoint_overridden": false,
            "offered_writes_per_sample": threads * ROWS_PER_THREAD,
            "retry_policy": retry_policy(threads),
            "status": "supported",
            "comparison_eligible": true,
            "measured": true,
            "reason": "complete deterministic v9 contract fixture",
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

    fn sample(threads: usize, elapsed_ns: u64, concurrent_mode: &str) -> Value {
        let offered = threads * ROWS_PER_THREAD;
        json!({
            "worker_startup_elapsed_ns": 1_000_000,
            "workload_elapsed_ns": elapsed_ns,
            "settings": settings(concurrent_mode),
            "accounting": {
                "offered_writes": offered,
                "attempted_writes": offered,
                "succeeded_writes": offered,
                "retried_operations": 0,
                "failed_writes": 0,
                "worker_reported_failed_writes": 0,
                "exact": true,
                "diagnostics": [],
            },
            "committed_state": {
                "expected_rows": offered,
                "observed_rows": offered,
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
    fn thread_report(threads: usize, fsqlite_elapsed_ns: u64) -> Value {
        let sqlite_samples = (0..ITERATIONS)
            .map(|_| sample(threads, SQLITE_ELAPSED_NS, "sqlite_wal_single_writer"))
            .collect::<Vec<_>>();
        let fsqlite_samples = (0..ITERATIONS)
            .map(|_| sample(threads, fsqlite_elapsed_ns, "fsqlite_mvcc_on"))
            .collect::<Vec<_>>();
        let offered = (threads * ROWS_PER_THREAD) as f64;
        let sqlite_wps = offered / (SQLITE_ELAPSED_NS as f64 / 1_000_000_000.0);
        let fsqlite_wps = offered / (fsqlite_elapsed_ns as f64 / 1_000_000_000.0);
        let ratio = fsqlite_wps / sqlite_wps;
        let verdict = if ratio > 1.01 {
            "FSQLITE_FASTER"
        } else if ratio < 0.99 {
            "FSQLITE_SLOWER"
        } else {
            "INCONCLUSIVE"
        };
        let round_order_receipts = (0..ITERATIONS)
            .map(|round_index| {
                let execution_order = if round_index % 2 == 0 {
                    json!([
                        "csqlite_null_a",
                        "csqlite_null_b",
                        "csqlite_baseline",
                        "fsqlite_candidate"
                    ])
                } else {
                    json!([
                        "fsqlite_candidate",
                        "csqlite_baseline",
                        "csqlite_null_b",
                        "csqlite_null_a"
                    ])
                };
                json!({"round_index": round_index, "execution_order": execution_order})
            })
            .collect::<Vec<_>>();
        json!({
            "threads": threads,
            "fsqlite_wps_p50": fsqlite_wps,
            "fsqlite_wps_p95": fsqlite_wps,
            "fsqlite_wps_p99": fsqlite_wps,
            "sqlite_wps_p50": sqlite_wps,
            "sqlite_wps_p95": sqlite_wps,
            "sqlite_wps_p99": sqlite_wps,
            "throughput_ratio": ratio,
            "fsqlite_ms_p50": fsqlite_elapsed_ns as f64 / 1_000_000.0,
            "fsqlite_ms_p95": fsqlite_elapsed_ns as f64 / 1_000_000.0,
            "fsqlite_ms_p99": fsqlite_elapsed_ns as f64 / 1_000_000.0,
            "sqlite_ms_p50": 1000.0,
            "sqlite_ms_p95": 1000.0,
            "sqlite_ms_p99": 1000.0,
            "time_ratio": fsqlite_elapsed_ns as f64 / SQLITE_ELAPSED_NS as f64,
            "fsqlite_failed_rows": 0,
            "sqlite_failed_rows": 0,
            "median_ci_contract": {
                "null_ratio_median": 1.0,
                "null_ratio_ci95_low": 1.0,
                "null_ratio_ci95_high": 1.0,
                "null_ratio_cv_pct": 0.0,
                "null_ratio_mad": 0.0,
                "claim_ratio_median": ratio,
                "claim_ratio_ci95_low": ratio,
                "claim_ratio_ci95_high": ratio,
                "claim_ratio_cv_pct": 0.0,
                "claim_ratio_mad": 0.0,
                "null_radius": 0.0,
                "min_decidable_gain": 1.01,
                "max_decidable_regression": 0.99,
                "claim_margin": null,
                "cv_gate": "never",
                "verdict": verdict,
            },
            "truth": {
                "configuration": configuration(threads),
                "round_order_receipts": round_order_receipts,
                "null_c_a_samples": sqlite_samples.clone(),
                "null_c_b_samples": sqlite_samples.clone(),
                "sqlite_samples": sqlite_samples,
                "fsqlite_samples": fsqlite_samples,
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn report(
        profile: &str,
        commit: &str,
        nonce: &str,
        target: &str,
        graph_digest: &str,
        history_path: &str,
        output_path: &str,
        elapsed: [u64; 3],
    ) -> Value {
        let argv = vec![
            "mt-mvcc-bench".to_owned(),
            "--threads=1,8,16".to_owned(),
            format!("--rows-per-thread={ROWS_PER_THREAD}"),
            format!("--iters={ITERATIONS}"),
            "--separate-tables".to_owned(),
            "--one-row-per-transaction".to_owned(),
            format!("--history-json={history_path}"),
            format!("--json-output={output_path}"),
        ];
        json!({
            "schema_version": REPORT_SCHEMA,
            "citable": false,
            "measurement_evidence_valid": true,
            "non_citable_reason": NON_CITABLE_REASON,
            "release_regression_scope": RELEASE_SCOPE,
            "subject_identity": subject_identity(commit, nonce),
            "comparison_environment": {
                "build_configuration": build_configuration(profile, target, graph_digest),
                "invocation": invocation(&argv),
                "measurement_host": measurement_host(),
            },
            "settings_interpretation": SETTINGS_INTERPRETATION,
            "accounting_interpretation": ACCOUNTING_INTERPRETATION,
            "timing_interpretation": TIMING_INTERPRETATION,
            "workload_shape": "separate_tables",
            "transaction_contract": {
                "granularity": "one_row_per_transaction",
                "rows_per_transaction": 1,
                "prepared_statement_scope": "one successfully prepared statement per worker, reused across row transactions; transient preparation failures retry under the shared worker deadline",
                "duplicate_after_ambiguous_commit_policy": "fail_closed; a duplicate is never accepted as proof of exact id+payload",
                "csqlite_retry_unit": CSQLITE_RETRY_UNIT,
                "fsqlite_retry_unit": FSQLITE_RETRY_UNIT,
            },
            "rows_per_thread": ROWS_PER_THREAD,
            "iterations": ITERATIONS,
            "configuration_receipts": [configuration(1), configuration(8), configuration(16)],
            "thread_results": [
                thread_report(1, elapsed[0]),
                thread_report(8, elapsed[1]),
                thread_report(16, elapsed[2]),
            ],
            "pass_over_pass_gate": {
                "schema_version": "fsqlite-e2e.mt_mvcc_bench.pass_over_pass.v1",
                "history_json_path": history_path,
                "threshold_ratio_drop_pct": 5.0,
                "status": "disabled_non_citable",
                "previous_report_found": false,
                "comparable_pair_count": 0,
                "regressions": [],
            },
        })
    }

    fn fixture_pair(fixture: &TempDir, elapsed: [u64; 3]) -> (ReportPair, PathBuf) {
        let graph_path = fixture.path().join("graph.json");
        let digest = write_graph(&graph_path, "fixture-target");
        let release = report(
            "release",
            FIXTURE_COMMIT,
            "fixture-release-nonce",
            "fixture-target",
            &digest,
            "fixture-release-history.json",
            "fixture-release-output.json",
            elapsed,
        );
        let release_perf = report(
            "release-perf",
            FIXTURE_COMMIT,
            "fixture-release-perf-nonce",
            "fixture-target",
            &digest,
            "fixture-release-perf-history.json",
            "fixture-release-perf-output.json",
            elapsed,
        );
        (
            ReportPair {
                release,
                release_perf,
            },
            graph_path,
        )
    }

    fn write_json(path: &Path, value: &Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create JSON parent");
        }
        fs::write(
            path,
            serde_json::to_vec_pretty(value).expect("serialize JSON"),
        )
        .expect("write JSON");
    }

    fn read_json(path: &Path) -> Value {
        serde_json::from_slice(&fs::read(path).expect("read JSON")).expect("parse JSON")
    }

    fn gate_run(fixture: &TempDir, pair: &ReportPair, graph_path: &Path, capture: bool) -> GateRun {
        let run_id = format!(
            "v9-contract-{}",
            NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed)
        );
        let release_path = fixture.path().join("input.release.json");
        let release_perf_path = fixture.path().join("input.release-perf.json");
        write_json(&release_path, &pair.release);
        write_json(&release_perf_path, &pair.release_perf);
        let target = fixture.path().join("target");
        let run_dir = target.join("regression_gate_runs").join(&run_id);
        let mut command = Command::new("bash");
        command
            .env("FSQLITE_REGGATE_RUN_ID", &run_id)
            .arg(script_path())
            .arg("--analyze-only-release")
            .arg(&release_path)
            .arg("--analyze-only-release-perf")
            .arg(&release_perf_path)
            .arg("--graph-artifact")
            .arg(graph_path)
            .arg("--expected-commit")
            .arg(FIXTURE_COMMIT)
            .arg("--target-dir")
            .arg(&target)
            .arg("--baseline-dir")
            .arg(fixture.path().join("baselines"))
            .arg("--rows")
            .arg(ROWS_PER_THREAD.to_string())
            .arg("--max-drop-16t")
            .arg("0.10")
            .arg("--max-scaling-drop-8-over-1")
            .arg("0.10")
            .arg("--max-scaling-drop-16-over-8")
            .arg("0.10");
        if capture {
            command.arg("--capture-baseline");
        }
        GateRun {
            output: command.output().expect("run v9 gate"),
            result_path: run_dir.join("result.json"),
            run_dir,
        }
    }

    fn output_detail(run: &GateRun) -> String {
        format!(
            "status={:?}\nstdout:\n{}\nstderr:\n{}",
            run.output.status.code(),
            String::from_utf8_lossy(&run.output.stdout),
            String::from_utf8_lossy(&run.output.stderr),
        )
    }

    fn capture(fixture: &TempDir, pair: &ReportPair, graph: &Path) {
        let run = gate_run(fixture, pair, graph, true);
        assert_eq!(run.output.status.code(), Some(0), "{}", output_detail(&run));
        let result = read_json(&run.result_path);
        assert_eq!(result["schema_version"], GATE_SCHEMA);
        assert_eq!(result["verdict"], "baseline_captured");
        assert_diagnostic_only(&result);
        assert!(
            result["diagnostic_margin_policy"]["16t_absolute"]["source"]
                .as_str()
                .expect("16t margin source")
                .contains("acceptance-owner value remains unresolved"),
        );
        let baseline = read_json(&fixture.path().join("baselines/latest.json"));
        assert_eq!(baseline["schema_version"], BASELINE_SCHEMA);
        assert!(baseline["profiles"]["release"]["report"].is_object());
        assert!(baseline["profiles"]["release-perf"]["report"].is_object());
        assert_diagnostic_only(&baseline);
        assert!(run.run_dir.join("current.release.json").is_file());
        assert!(run.run_dir.join("current.release-perf.json").is_file());
        assert!(run.run_dir.join("dependency-feature-graph.json").is_file());
    }

    fn assert_invalid(pair: &ReportPair, mutation_label: &str) -> String {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (_, graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        let run = gate_run(&fixture, pair, &graph, true);
        assert_eq!(
            run.output.status.code(),
            Some(2),
            "mutation={mutation_label}\n{}",
            output_detail(&run),
        );
        let result = read_json(&run.result_path);
        assert_eq!(result["verdict"], "invalid_evidence");
        assert_diagnostic_only(&result);
        result["error"]
            .as_str()
            .expect("invalid evidence error")
            .to_owned()
    }

    #[test]
    fn capture_binds_both_profiles_graph_and_unresolved_release_scope() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (pair, graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        capture(&fixture, &pair, &graph);
    }

    #[test]
    fn unchanged_dual_profile_comparison_passes_as_diagnostic_only() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (pair, graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        capture(&fixture, &pair, &graph);
        let run = gate_run(&fixture, &pair, &graph, false);
        assert_eq!(run.output.status.code(), Some(0), "{}", output_detail(&run));
        let result = read_json(&run.result_path);
        assert_diagnostic_only(&result);
        assert_eq!(result["guard_status"], "passed");
        assert_eq!(result["verdict"], "diagnostic_only");
        assert_eq!(
            result["absolute_comparisons"]
                .as_array()
                .expect("absolute comparisons")
                .len(),
            6,
        );
        assert_eq!(
            result["scaling_comparisons"]
                .as_array()
                .expect("scaling comparisons")
                .len(),
            4,
        );
        assert!(
            result["absolute_comparisons"]
                .as_array()
                .expect("absolute comparisons")
                .iter()
                .all(|comparison| comparison["sampling_design"]
                    == "independent_two_sample_bootstrap"),
        );
    }

    #[test]
    fn baseline_and_current_dependency_graphs_are_attested_independently() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (baseline, baseline_graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        capture(&fixture, &baseline, &baseline_graph);

        let mut current_graph = graph_value("fixture-target");
        current_graph["tree"] = json!(
            "fsqlite-e2e v0.1.0 (${WORKSPACE_ROOT}/crates/fsqlite-e2e)\n+-- comparator-only-dependency v1.0.0\n"
        );
        let current_graph_path = fixture.path().join("current-graph.json");
        let current_graph_digest = write_graph_value(&current_graph_path, &current_graph);
        let mut current = baseline.clone();
        for report in [&mut current.release, &mut current.release_perf] {
            report["comparison_environment"]["build_configuration"]["resolved_dependency_feature_graph_sha256"] =
                json!(current_graph_digest.clone());
        }

        let run = gate_run(&fixture, &current, &current_graph_path, false);
        assert_eq!(run.output.status.code(), Some(0), "{}", output_detail(&run));
        let result = read_json(&run.result_path);
        assert_diagnostic_only(&result);
        assert_ne!(
            result["baseline_dependency_feature_graph_sha256"],
            result["current_dependency_feature_graph_sha256"],
        );
        assert_eq!(result["guard_status"], "passed");
    }

    #[test]
    fn baseline_envelope_paths_are_typed_and_bound_in_analyze_only_mode() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (pair, graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        capture(&fixture, &pair, &graph);

        let baseline_dir = fixture.path().join("baselines");
        let mut baseline = read_json(&baseline_dir.join("latest.json"));
        baseline["profiles"]["release"]["report_history_json_path"] = json!(false);
        let mut payload = serde_json::to_vec_pretty(&baseline).expect("serialize forged baseline");
        payload.push(b'\n');
        let digest = sha256(&payload);
        fs::rename(&baseline_dir, fixture.path().join("original-baselines"))
            .expect("retain original baseline directory");
        fs::create_dir_all(baseline_dir.join("versions"))
            .expect("create forged baseline versions directory");
        let version = baseline_dir.join("versions").join(format!("{digest}.json"));
        fs::write(&version, payload).expect("write forged content-addressed baseline");
        fs::hard_link(&version, baseline_dir.join("latest.json"))
            .expect("publish forged baseline hard link");

        let run = gate_run(&fixture, &pair, &graph, false);
        assert_eq!(run.output.status.code(), Some(2), "{}", output_detail(&run));
        let result = read_json(&run.result_path);
        assert_diagnostic_only(&result);
        assert!(
            result["error"]
                .as_str()
                .expect("invalid evidence error")
                .contains("report_history_json_path must be a non-empty string"),
        );
    }

    #[test]
    fn active_gate_rejects_prior_schemas_and_requires_both_exact_profiles() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["schema_version"] = json!("fsqlite-e2e.mt_mvcc_bench_report.v7");
        assert!(assert_invalid(&pair, "v7 schema").contains("prior schemas are not accepted"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["schema_version"] = json!("fsqlite-e2e.mt_mvcc_bench_report.v8");
        assert!(assert_invalid(&pair, "v8 schema").contains("prior schemas are not accepted"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release_perf = pair.release.clone();
        assert!(assert_invalid(&pair, "duplicated release profile").contains("release-perf"));
    }

    #[test]
    fn graph_attestation_and_intended_commit_are_fail_closed() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["comparison_environment"]["build_configuration"]["resolved_dependency_feature_graph_sha256"] =
            json!("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        assert!(assert_invalid(&pair, "graph digest").contains("exact retained"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["subject_identity"]["build_source"]["git_sha"] =
            json!("dddddddddddddddddddddddddddddddddddddddd");
        for point in ["before_measurement", "after_measurement"] {
            pair.release["subject_identity"]["runtime_source"][point]["git_sha"] =
                json!("dddddddddddddddddddddddddddddddddddddddd");
        }
        assert!(assert_invalid(&pair, "intended commit").contains("intended commit"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["subject_identity"]["build_source"]["git_sha"] = json!("not-a-git-sha");
        assert!(assert_invalid(&pair, "malformed Git SHA").contains("Git object ID"));
    }

    #[test]
    fn integer_contract_fields_reject_boolean_and_floating_point_aliases() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["rows_per_thread"] = json!(false);
        assert!(assert_invalid(&pair, "boolean rows").contains("must be an integer"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["configuration_receipts"][0]["max_supported_writers"] = json!(128.0);
        pair.release["thread_results"][0]["truth"]["configuration"] =
            pair.release["configuration_receipts"][0].clone();
        assert!(
            assert_invalid(&pair, "floating max writers")
                .contains("max_supported_writers must be an integer"),
        );

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["thread_results"][0]["truth"]["sqlite_samples"][0]["accounting"]["failed_writes"] =
            json!(false);
        assert!(assert_invalid(&pair, "boolean failed writes").contains("must be an integer"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        for field in ["expected_id_sum", "observed_id_sum"] {
            pair.release["thread_results"][0]["truth"]["sqlite_samples"][0]["committed_state"]
                [field] = json!(true);
        }
        assert!(assert_invalid(&pair, "boolean id sums").contains("must be an integer"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["thread_results"][0]["truth"]["round_order_receipts"][0]["round_index"] =
            json!(0.0);
        assert!(assert_invalid(&pair, "floating round index").contains("must be the integer 0"),);
    }

    #[test]
    fn host_receipts_require_complete_typed_topology_and_dynamic_fields() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["comparison_environment"]["measurement_host"]["host"]["cpu_topology"]
            .as_object_mut()
            .expect("topology object")
            .remove("logical_cpu_directories");
        assert!(
            assert_invalid(&pair, "missing topology")
                .contains("logical_cpu_directories must be present"),
        );

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["comparison_environment"]["measurement_host"]["host"]["scaling_governors_by_cpu"] =
            json!({"cpu0": false});
        pair.release["comparison_environment"]["measurement_host"]["host"]["unavailable_fields"] =
            json!(["cpu_isolated"]);
        assert!(assert_invalid(&pair, "malformed governor").contains("non-empty string"));

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["comparison_environment"]["measurement_host"]["host"]["unavailable_fields"] =
            json!(["cpu_isolated"]);
        assert!(
            assert_invalid(&pair, "contradictory unavailable fields")
                .contains("unavailable_fields"),
        );

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["comparison_environment"]["measurement_host"]["before_measurement"]
            .as_object_mut()
            .expect("dynamic host object")
            .remove("load_average");
        assert!(
            assert_invalid(&pair, "missing load receipt").contains("load_average must be present"),
        );

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["comparison_environment"]["measurement_host"]["after_measurement"]["pressure_cpu"] =
            json!("");
        assert!(assert_invalid(&pair, "empty pressure receipt").contains("non-empty string"));
    }

    #[test]
    fn transaction_shared_deadline_and_round_order_receipts_are_exact() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["transaction_contract"]["rows_per_transaction"] = json!(2);
        assert!(assert_invalid(&pair, "transaction granularity").contains("must be the integer 1"),);

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["configuration_receipts"][2]["retry_policy"]["shared_worker_retry_timeout_ms"] =
            json!(5000);
        pair.release["thread_results"][2]["truth"]["configuration"] =
            pair.release["configuration_receipts"][2].clone();
        assert!(
            assert_invalid(&pair, "shared deadline").contains("shared_worker_retry_timeout_ms"),
        );

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["thread_results"][0]["truth"]["round_order_receipts"][1]["execution_order"] =
            json!([
                "csqlite_null_a",
                "csqlite_null_b",
                "csqlite_baseline",
                "fsqlite_candidate",
            ]);
        assert!(assert_invalid(&pair, "round order").contains("round_order_receipts"));
    }

    #[test]
    fn v9_rejects_the_v8_transaction_only_retry_contract() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        pair.release["transaction_contract"]["fsqlite_retry_unit"] = json!(V8_FSQLITE_RETRY_UNIT);
        for index in 0..3 {
            pair.release["configuration_receipts"][index]["retry_policy"]["fsqlite_retry_unit"] =
                json!(V8_FSQLITE_RETRY_UNIT);
            pair.release["thread_results"][index]["truth"]["configuration"] =
                pair.release["configuration_receipts"][index].clone();
        }
        assert!(
            assert_invalid(&pair, "v8 transaction-only retry unit")
                .contains("transaction_contract.fsqlite_retry_unit"),
        );

        let fixture = tempfile::tempdir().expect("tempdir");
        let (mut pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        for index in 0..3 {
            pair.release["configuration_receipts"][index]["retry_policy"]["fsqlite_retry_backoff_algorithm"] =
                json!(V8_FSQLITE_RETRY_ALGORITHM);
            pair.release["thread_results"][index]["truth"]["configuration"] =
                pair.release["configuration_receipts"][index].clone();
        }
        assert!(
            assert_invalid(&pair, "v8 transaction-only retry algorithm")
                .contains("retry_policy.fsqlite_retry_backoff_algorithm"),
        );
    }

    #[test]
    fn any_profile_cell_regression_fails_the_combined_verdict() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (baseline, graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        capture(&fixture, &baseline, &graph);
        let (mut current, _) = fixture_pair(
            &fixture,
            [SQLITE_ELAPSED_NS, 1_250_000_000, SQLITE_ELAPSED_NS],
        );
        current.release = baseline.release;
        let run = gate_run(&fixture, &current, &graph, false);
        assert_eq!(run.output.status.code(), Some(1), "{}", output_detail(&run));
        let result = read_json(&run.result_path);
        assert_diagnostic_only(&result);
        assert_eq!(result["guard_status"], "failed");
        assert!(
            result["absolute_comparisons"]
                .as_array()
                .expect("absolute comparisons")
                .iter()
                .any(|comparison| comparison["profile"] == "release-perf"
                    && comparison["threads"] == 8
                    && comparison["status"] == "regression"),
        );
    }

    #[test]
    fn scaling_retention_is_independent_and_combined_with_absolute_cells() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let (baseline, graph) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        capture(&fixture, &baseline, &graph);
        let (current, _) = fixture_pair(
            &fixture,
            [SQLITE_ELAPSED_NS, 500_000_000, SQLITE_ELAPSED_NS],
        );
        let run = gate_run(&fixture, &current, &graph, false);
        assert_eq!(run.output.status.code(), Some(1), "{}", output_detail(&run));
        let result = read_json(&run.result_path);
        assert_diagnostic_only(&result);
        assert!(
            result["scaling_comparisons"]
                .as_array()
                .expect("scaling comparisons")
                .iter()
                .any(|comparison| comparison["scaling"] == "16/8"
                    && comparison["sampling_design"] == "independent_four_sample_bootstrap"
                    && comparison["status"] == "regression"),
        );
    }

    #[test]
    fn unset_new_policy_margins_are_rejected_before_analysis() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let output = Command::new("bash")
            .env("FSQLITE_REGGATE_RUN_ID", "missing-policy-margins")
            .arg(script_path())
            .arg("--target-dir")
            .arg(fixture.path().join("target"))
            .arg("--baseline-dir")
            .arg(fixture.path().join("baselines"))
            .output()
            .expect("run gate without policy margins");
        assert_eq!(output.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("--max-drop-16t must be an explicit finite fraction"),
        );
    }

    #[test]
    fn analyze_only_normalizes_relative_paths_before_entering_repo_root() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let foreign = fixture.path().join("foreign-working-directory");
        let inputs = foreign.join("inputs");
        fs::create_dir_all(&inputs).expect("create relative-path fixture inputs");
        let (pair, _) = fixture_pair(&fixture, [SQLITE_ELAPSED_NS; 3]);
        write_json(&inputs.join("release.json"), &pair.release);
        write_json(&inputs.join("release-perf.json"), &pair.release_perf);
        let run_id = "v9-relative-analyze-only-paths";
        let output = Command::new("bash")
            .current_dir(&foreign)
            .env("FSQLITE_REGGATE_RUN_ID", run_id)
            .arg(script_path())
            .arg("--capture-baseline")
            .arg("--analyze-only-release")
            .arg("inputs/release.json")
            .arg("--analyze-only-release-perf")
            .arg("inputs/release-perf.json")
            .arg("--graph-artifact")
            .arg("../graph.json")
            .arg("--expected-commit")
            .arg(FIXTURE_COMMIT)
            .arg("--target-dir")
            .arg("relative-target")
            .arg("--baseline-dir")
            .arg("relative-baselines")
            .arg("--rows")
            .arg(ROWS_PER_THREAD.to_string())
            .arg("--max-drop-16t")
            .arg("0.10")
            .arg("--max-scaling-drop-8-over-1")
            .arg("0.10")
            .arg("--max-scaling-drop-16-over-8")
            .arg("0.10")
            .output()
            .expect("run analyze-only gate from foreign working directory");
        assert_eq!(
            output.status.code(),
            Some(0),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        let result = read_json(
            &foreign
                .join("relative-target/regression_gate_runs")
                .join(run_id)
                .join("result.json"),
        );
        assert_diagnostic_only(&result);
        assert!(foreign.join("relative-baselines/latest.json").is_file());
    }

    fn rustc_host() -> String {
        let output = Command::new("rustc")
            .arg("-vV")
            .output()
            .expect("query rustc host");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("UTF-8 rustc output")
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .expect("rustc host line")
            .to_owned()
    }

    // The embedded fake-Cargo script intentionally contains Bash `${...}`
    // parameter expansions, which are not Rust formatting placeholders.
    #[allow(clippy::literal_string_with_formatting_args)]
    fn measured_fake_cargo_run(
        fixture: &TempDir,
        run_id: &str,
        create_history_profile: Option<&str>,
    ) -> Output {
        let target_dir = fixture.path().join("target");
        let run_dir = target_dir.join("regression_gate_runs").join(run_id);
        let target = rustc_host();
        let expected_graph_bytes = graph_bytes(&target);
        let graph_digest = sha256(&expected_graph_bytes);
        let commit = FIXTURE_COMMIT;
        let release_history = run_dir.join("disposable_history.release.json");
        let release_output = run_dir.join("current.release.json");
        let release_perf_history = run_dir.join("disposable_history.release-perf.json");
        let release_perf_output = run_dir.join("current.release-perf.json");
        let release_report = report(
            "release",
            commit,
            &format!("{run_id}.release"),
            &target,
            &graph_digest,
            &release_history.to_string_lossy(),
            &release_output.to_string_lossy(),
            [SQLITE_ELAPSED_NS; 3],
        );
        let release_perf_report = report(
            "release-perf",
            commit,
            &format!("{run_id}.release-perf"),
            &target,
            &graph_digest,
            &release_perf_history.to_string_lossy(),
            &release_perf_output.to_string_lossy(),
            [SQLITE_ELAPSED_NS; 3],
        );
        let release_fixture = fixture.path().join("fake-release.json");
        let release_perf_fixture = fixture.path().join("fake-release-perf.json");
        write_json(&release_fixture, &release_report);
        write_json(&release_perf_fixture, &release_perf_report);

        let fake_bin = fixture.path().join("fake-bin");
        fs::create_dir_all(&fake_bin).expect("create fake bin");
        let fake_cargo = fake_bin.join("cargo");
        fs::write(
            &fake_cargo,
            r#"#!/usr/bin/env bash
set -euo pipefail
[[ "$PWD" == "$FSQLITE_FAKE_REPO_ROOT" ]]
if [[ "${1:-}" == "-V" ]]; then
    printf '%s\n' 'cargo fixture'
    exit 0
fi
if [[ "${1:-}" == "tree" ]]; then
    [[ "$*" == *"--locked --offline -p fsqlite-e2e -e features,no-dev --no-default-features --target $FSQLITE_FAKE_TARGET"* ]]
    printf 'fsqlite-e2e v0.1.0 (%s/crates/fsqlite-e2e)\n' "$FSQLITE_FAKE_REPO_ROOT"
    exit 0
fi
profile="${FSQLITE_BENCH_PROFILE_NAME:-}"
[[ "$profile" == "release" || "$profile" == "release-perf" ]]
[[ "${FSQLITE_BENCH_BUILD_NONCE:-}" == "$FSQLITE_FAKE_RUN_ID.$profile" ]]
[[ "${FSQLITE_BENCH_RESOLVED_DEPENDENCY_FEATURE_GRAPH_SHA256:-}" == "$FSQLITE_FAKE_GRAPH_SHA256" ]]
[[ "${CARGO_TARGET_DIR:-}" == "$FSQLITE_FAKE_TARGET_DIR/regression_gate_builds/$FSQLITE_FAKE_RUN_ID/$profile" ]]
expected=(run --locked --offline -p fsqlite-e2e --no-default-features --target "$FSQLITE_FAKE_TARGET" --bin mt-mvcc-bench --profile "$profile" -- --threads=1,8,16 --rows-per-thread=500 --iters=21 --separate-tables --one-row-per-transaction)
actual=("$@")
[[ "${#actual[@]}" -eq $((${#expected[@]} + 2)) ]]
for index in "${!expected[@]}"; do
    [[ "${actual[$index]}" == "${expected[$index]}" ]]
done
json_output=''
history_path=''
for argument in "$@"; do
    case "$argument" in
        --json-output=*) json_output="${argument#--json-output=}" ;;
        --history-json=*) history_path="${argument#--history-json=}" ;;
    esac
done
[[ "$json_output" == "$FSQLITE_FAKE_RUN_DIR/current.$profile.json" ]]
[[ "$history_path" == "$FSQLITE_FAKE_RUN_DIR/disposable_history.$profile.json" ]]
if [[ "$profile" == "release" ]]; then
    cp "$FSQLITE_FAKE_RELEASE_REPORT" "$json_output"
else
    cp "$FSQLITE_FAKE_RELEASE_PERF_REPORT" "$json_output"
fi
printf '%s|%s|%s\n' "$profile" "$CARGO_TARGET_DIR" "$FSQLITE_BENCH_BUILD_NONCE" >> "$FSQLITE_FAKE_CALL_LOG"
if [[ "$FSQLITE_FAKE_CREATE_HISTORY_PROFILE" == "$profile" ]]; then
    printf '%s\n' '{"forged":true}' > "$history_path"
fi
"#,
        )
        .expect("write fake Cargo");
        let fake_git = fake_bin.join("git");
        fs::write(
            &fake_git,
            r#"#!/usr/bin/env bash
set -euo pipefail
[[ "$#" -eq 4 ]]
[[ "$1" == "-C" ]]
[[ "$2" == "$FSQLITE_FAKE_REPO_ROOT" ]]
[[ "$3" == "rev-parse" ]]
[[ "$4" == "HEAD" ]]
printf '%s\n' "$FSQLITE_FAKE_COMMIT"
"#,
        )
        .expect("write fake Git");
        for (path, label) in [(&fake_cargo, "Cargo"), (&fake_git, "Git")] {
            let mut permissions = fs::metadata(path)
                .unwrap_or_else(|error| panic!("fake {label} metadata: {error}"))
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions)
                .unwrap_or_else(|error| panic!("make fake {label} executable: {error}"));
        }
        let inherited_path = std::env::var_os("PATH").unwrap_or_default();
        let mut path_entries = vec![fake_bin];
        path_entries.extend(std::env::split_paths(&inherited_path));
        let path = std::env::join_paths(path_entries).expect("join fake PATH");
        Command::new("bash")
            .env("PATH", path)
            .env("FSQLITE_REGGATE_RUN_ID", run_id)
            .env("FSQLITE_FAKE_TARGET", &target)
            .env("FSQLITE_FAKE_REPO_ROOT", repo_root())
            .env("FSQLITE_FAKE_COMMIT", commit)
            .env("FSQLITE_FAKE_RUN_ID", run_id)
            .env("FSQLITE_FAKE_RUN_DIR", &run_dir)
            .env("FSQLITE_FAKE_TARGET_DIR", &target_dir)
            .env("FSQLITE_FAKE_GRAPH_SHA256", &graph_digest)
            .env("FSQLITE_FAKE_RELEASE_REPORT", &release_fixture)
            .env("FSQLITE_FAKE_RELEASE_PERF_REPORT", &release_perf_fixture)
            .env(
                "FSQLITE_FAKE_CALL_LOG",
                fixture.path().join("fake-cargo.calls"),
            )
            .env(
                "FSQLITE_FAKE_CREATE_HISTORY_PROFILE",
                create_history_profile.unwrap_or_default(),
            )
            .arg(script_path())
            .arg("--capture-baseline")
            .arg("--target-dir")
            .arg(&target_dir)
            .arg("--baseline-dir")
            .arg(fixture.path().join("baselines"))
            .arg("--rows")
            .arg(ROWS_PER_THREAD.to_string())
            .arg("--max-drop-16t")
            .arg("0.10")
            .arg("--max-scaling-drop-8-over-1")
            .arg("0.10")
            .arg("--max-scaling-drop-16-over-8")
            .arg("0.10")
            .output()
            .expect("run measured gate with fake Cargo")
    }

    #[test]
    fn measured_shell_runs_both_profiles_with_isolated_receipts() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let run_id = "measured-v9-dual-profile";
        let output = measured_fake_cargo_run(&fixture, run_id, None);
        assert_eq!(
            output.status.code(),
            Some(0),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        let calls = fs::read_to_string(fixture.path().join("fake-cargo.calls"))
            .expect("read fake Cargo calls");
        assert_eq!(calls.lines().count(), 2);
        assert!(calls.contains("release|"));
        assert!(calls.contains("release-perf|"));
        let run_dir = fixture
            .path()
            .join("target/regression_gate_runs")
            .join(run_id);
        for artifact in [
            "current.release.json",
            "current.release-perf.json",
            "dependency-feature-graph.json",
            "bench.release.log",
            "bench.release-perf.log",
        ] {
            assert!(run_dir.join(artifact).is_file(), "missing {artifact}");
        }
        assert_diagnostic_only(&read_json(&run_dir.join("result.json")));
    }

    #[test]
    fn measured_shell_rejects_history_created_by_the_second_profile() {
        let fixture = tempfile::tempdir().expect("tempdir");
        let output =
            measured_fake_cargo_run(&fixture, "measured-v9-forged-history", Some("release-perf"));
        assert_eq!(output.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("non-citable v9 benchmark unexpectedly created history"),
        );
        assert!(!fixture.path().join("baselines/latest.json").exists());
    }
}
