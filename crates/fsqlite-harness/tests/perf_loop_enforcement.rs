#![cfg(unix)]

#[path = "../src/perf_loop.rs"]
mod perf_loop;

use std::fs;
use std::path::PathBuf;

use perf_loop::{
    BASELINE_LAYOUT_BEAD_ID, DETERMINISTIC_MEASUREMENT_BEAD_ID, GOLDEN_BEHAVIOR_LOCK_BEAD_ID,
    MeasurementArtifactBundle, OPPORTUNITY_MATRIX_BEAD_ID, OPPORTUNITY_SCORE_THRESHOLD,
    OpportunityMatrix, OpportunityMatrixEntry, OptimizationLever, PROFILING_COOKBOOK_BEAD_ID,
    PerfLoopError, PerfSmokeArtifacts, PerfSmokeReport, PerfSmokeSystem, ProfilingArtifactReport,
    ScheduleEvent, build_measurement_artifact_bundle, canonical_profiling_cookbook_commands,
    capture_behavior_lock_checksums, capture_golden_checksums, compute_opportunity_score,
    compute_trace_fingerprint, enforce_baseline_capture, enforce_behavior_lock_ci,
    enforce_extreme_optimization_loop, enforce_one_lever_rule, enforce_opportunity_matrix_gate,
    enforce_opportunity_matrix_required, enforce_opportunity_score_gate,
    enforce_profiling_toolchain_presence_with, ensure_baseline_layout, evaluate_opportunity_matrix,
    load_and_validate_smoke_report, parse_git_diff_changed_paths, record_measurement_env,
    record_profiling_metadata, replay_schedule_from_fingerprint, run_deterministic_measurement,
    validate_baseline_layout, validate_conformance_artifacts_included,
    validate_cookbook_commands_exist, validate_flamegraph_output, validate_hyperfine_json_output,
    validate_measurement_env, validate_opportunity_matrix, validate_perf_smoke_report,
    validate_profiling_artifact_paths, validate_profiling_artifact_report,
    validate_profiling_metadata, verify_golden_checksums, write_baseline_gitkeep_files,
    write_measurement_artifact_bundle,
};
use serde_json::json;
use tempfile::TempDir;

const BEAD_ID: &str = perf_loop::BEAD_ID;

fn make_golden_fixture() -> (TempDir, PathBuf, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let golden_dir = temp.path().join("golden_outputs");
    fs::create_dir_all(&golden_dir).expect("create golden dir");

    fs::write(golden_dir.join("query_rows.txt"), "alice|1\nbob|2\n").expect("write golden rows");
    fs::write(golden_dir.join("error_codes.txt"), "SQLITE_OK\n").expect("write golden codes");

    let checksum_file = temp.path().join("golden_checksums.txt");
    capture_golden_checksums(&golden_dir, &checksum_file).expect("capture checksums");
    (temp, golden_dir, checksum_file)
}

fn make_conformance_golden_fixture() -> (TempDir, PathBuf, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let golden_dir = temp.path().join("golden_outputs");
    fs::create_dir_all(&golden_dir).expect("create golden dir");

    fs::write(golden_dir.join("query_rows.txt"), "alice|1\nbob|2\n").expect("write golden rows");
    fs::write(golden_dir.join("error_codes.txt"), "SQLITE_OK\n").expect("write golden codes");
    fs::write(golden_dir.join("CommitMarker.json"), r#"{"commit_seq":1}"#)
        .expect("write commit marker");
    fs::write(golden_dir.join("CommitProof.json"), r#"{"proof":"ok"}"#)
        .expect("write commit proof");
    fs::write(
        golden_dir.join("AbortWitness.json"),
        r#"{"witness":"none"}"#,
    )
    .expect("write abort witness");

    let checksum_file = temp.path().join("golden_checksums.txt");
    capture_behavior_lock_checksums(&golden_dir, &checksum_file)
        .expect("capture behavior-lock checksums");
    (temp, golden_dir, checksum_file)
}

#[test]
fn test_loop_one_lever_only_uses_git_diff_heuristics() {
    let same_lever_diff = r#"
diff --git a/crates/fsqlite-mvcc/src/lifecycle.rs b/crates/fsqlite-mvcc/src/lifecycle.rs
index 1111111..2222222 100644
--- a/crates/fsqlite-mvcc/src/lifecycle.rs
+++ b/crates/fsqlite-mvcc/src/lifecycle.rs
diff --git a/crates/fsqlite-mvcc/src/core_types.rs b/crates/fsqlite-mvcc/src/core_types.rs
index 3333333..4444444 100644
--- a/crates/fsqlite-mvcc/src/core_types.rs
+++ b/crates/fsqlite-mvcc/src/core_types.rs
diff --git a/crates/fsqlite-mvcc/tests/lifecycle_tests.rs b/crates/fsqlite-mvcc/tests/lifecycle_tests.rs
index 5555555..6666666 100644
--- a/crates/fsqlite-mvcc/tests/lifecycle_tests.rs
+++ b/crates/fsqlite-mvcc/tests/lifecycle_tests.rs
"#;
    let same_paths = parse_git_diff_changed_paths(same_lever_diff);
    let lever = enforce_one_lever_rule(&same_paths).expect("same lever should pass");
    assert_eq!(
        lever,
        OptimizationLever::Concurrency,
        "bead_id={BEAD_ID} expected concurrency-only change set"
    );

    let multi_lever_diff = r#"
diff --git a/crates/fsqlite-mvcc/src/lifecycle.rs b/crates/fsqlite-mvcc/src/lifecycle.rs
index 1111111..2222222 100644
--- a/crates/fsqlite-mvcc/src/lifecycle.rs
+++ b/crates/fsqlite-mvcc/src/lifecycle.rs
diff --git a/crates/fsqlite-vfs/src/unix.rs b/crates/fsqlite-vfs/src/unix.rs
index 7777777..8888888 100644
--- a/crates/fsqlite-vfs/src/unix.rs
+++ b/crates/fsqlite-vfs/src/unix.rs
"#;
    let mixed_paths = parse_git_diff_changed_paths(multi_lever_diff);
    let error = enforce_one_lever_rule(&mixed_paths).expect_err("multi-lever should fail");
    assert!(
        matches!(error, PerfLoopError::MultipleOptimizationLevers { .. }),
        "bead_id={BEAD_ID} expected multi-lever violation, got {error:?}"
    );
}

#[test]
fn test_baseline_capture_required() {
    let (_temp, golden_dir, checksum_file) = make_golden_fixture();
    let changed_paths = vec![PathBuf::from("crates/fsqlite-mvcc/src/lifecycle.rs")];

    let missing_baseline = golden_dir.join("baseline_missing.json");
    let error = enforce_extreme_optimization_loop(
        &changed_paths,
        &missing_baseline,
        &golden_dir,
        &checksum_file,
    )
    .expect_err("missing baseline must fail gate");

    assert!(
        matches!(error, PerfLoopError::MissingBaselineArtifact { .. }),
        "bead_id={BEAD_ID} expected MissingBaselineArtifact, got {error:?}"
    );
    if let PerfLoopError::MissingBaselineArtifact { path } = error {
        assert_eq!(
            path, missing_baseline,
            "bead_id={BEAD_ID} missing baseline should report path"
        );
    }

    let baseline = golden_dir.join("baseline_perf.json");
    fs::write(&baseline, "{\"p50\":100,\"p95\":140,\"p99\":170}").expect("write baseline");
    enforce_baseline_capture(&baseline).expect("baseline capture should pass");
}

#[test]
fn test_golden_unchanged_behavior_lock() {
    let (_temp, golden_dir, checksum_file) = make_golden_fixture();

    verify_golden_checksums(&golden_dir, &checksum_file)
        .expect("golden verification should pass before mutation");

    fs::write(golden_dir.join("query_rows.txt"), "alice|1\nbob|999\n")
        .expect("mutate golden output");

    let error = verify_golden_checksums(&golden_dir, &checksum_file)
        .expect_err("golden verification should fail after mutation");
    assert!(
        matches!(error, PerfLoopError::GoldenChecksumMismatch { .. }),
        "bead_id={BEAD_ID} expected checksum mismatch, got {error:?}"
    );
}

#[test]
fn test_checksum_capture() {
    let (_temp, _golden_dir, checksum_file) = make_conformance_golden_fixture();
    let contents = fs::read_to_string(&checksum_file).expect("read checksums");
    assert!(
        !contents.trim().is_empty(),
        "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} checksum file should not be empty"
    );
}

#[test]
fn test_checksum_verify() {
    let (_temp, golden_dir, checksum_file) = make_conformance_golden_fixture();
    verify_golden_checksums(&golden_dir, &checksum_file).expect("verify should pass initially");

    fs::write(
        golden_dir.join("CommitProof.json"),
        r#"{"proof":"mutated"}"#,
    )
    .expect("mutate commit proof");
    let error = verify_golden_checksums(&golden_dir, &checksum_file)
        .expect_err("verify should fail after mutation");
    assert!(
        matches!(error, PerfLoopError::GoldenChecksumMismatch { .. }),
        "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} expected checksum mismatch after mutation, got {error:?}"
    );
}

#[test]
fn test_conformance_artifacts_included() {
    let temp = TempDir::new().expect("tempdir");
    let golden_dir = temp.path().join("golden_outputs");
    fs::create_dir_all(&golden_dir).expect("create dir");
    fs::write(golden_dir.join("CommitMarker.json"), "{}").expect("write marker");
    fs::write(golden_dir.join("CommitProof.json"), "{}").expect("write proof");

    let error = validate_conformance_artifacts_included(&golden_dir)
        .expect_err("missing abort witness must fail");
    assert!(
        matches!(error, PerfLoopError::MissingConformanceArtifact { .. }),
        "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} expected missing conformance artifact, got {error:?}"
    );

    fs::write(golden_dir.join("AbortWitness.json"), "{}").expect("write witness");
    validate_conformance_artifacts_included(&golden_dir)
        .expect("all required conformance artifacts should pass");
}

#[test]
fn test_behavior_lock_ci() {
    let (_temp, golden_dir, checksum_file) = make_conformance_golden_fixture();
    enforce_behavior_lock_ci(true, &golden_dir, &checksum_file)
        .expect("perf-only gate should pass");

    fs::write(
        golden_dir.join("AbortWitness.json"),
        r#"{"witness":"changed"}"#,
    )
    .expect("mutate witness");
    let error = enforce_behavior_lock_ci(true, &golden_dir, &checksum_file)
        .expect_err("perf-only gate should fail on mismatch");
    assert!(
        matches!(error, PerfLoopError::GoldenChecksumMismatch { .. }),
        "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} expected behavior-lock mismatch failure, got {error:?}"
    );
}

#[test]
fn test_directory_layout() {
    let temp = TempDir::new().expect("tempdir");
    let root = temp.path().join("baselines");

    ensure_baseline_layout(&root).expect("layout creation should pass");
    validate_baseline_layout(&root).expect("layout validation should pass");
    write_baseline_gitkeep_files(&root).expect("gitkeep creation should pass");

    for name in perf_loop::REQUIRED_BASELINE_DIRS {
        assert!(
            root.join(name).is_dir(),
            "bead_id={BASELINE_LAYOUT_BEAD_ID} missing directory {name}"
        );
        assert!(
            root.join(name).join(".gitkeep").is_file(),
            "bead_id={BASELINE_LAYOUT_BEAD_ID} missing .gitkeep in {name}"
        );
    }
}

#[test]
fn test_artifact_schema() {
    let report_value = json!({
        "generated_at": "2026-02-09T00:00:00Z",
        "scenario_id": "mvcc_100_writers_zipf_s_0_99",
        "command": "cargo bench --bench mvcc_stress",
        "seed": "3735928559",
        "trace_fingerprint": "sha256:abcd",
        "git_sha": "deadbeef",
        "config_hash": "sha256:cafef00d",
        "alpha_total": 0.01,
        "alpha_policy": "bonferroni",
        "metric_count": 12,
        "artifacts": {
            "criterion_dir": "target/criterion",
            "baseline_path": "baselines/criterion/baseline_20260207_000000.json",
            "latest_path": "baselines/criterion/baseline_latest.json"
        },
        "env": {
            "RUSTFLAGS": "-C force-frame-pointers=yes"
        },
        "system": {
            "os": "linux",
            "arch": "x86_64",
            "kernel": "Linux-6.x"
        }
    });

    let report: PerfSmokeReport =
        serde_json::from_value(report_value).expect("schema must deserialize");
    validate_perf_smoke_report(&report).expect("schema must validate");
}

#[test]
fn test_required_fields() {
    let report = PerfSmokeReport {
        generated_at: String::new(),
        scenario_id: "scenario".to_string(),
        command: "cargo bench".to_string(),
        seed: "1".to_string(),
        trace_fingerprint: "sha256:abc".to_string(),
        git_sha: "deadbeef".to_string(),
        config_hash: "sha256:cfg".to_string(),
        alpha_total: 0.01,
        alpha_policy: "bonferroni".to_string(),
        metric_count: 1,
        artifacts: PerfSmokeArtifacts {
            criterion_dir: "target/criterion".to_string(),
            baseline_path: "baselines/criterion/base.json".to_string(),
            latest_path: "baselines/criterion/latest.json".to_string(),
        },
        env: std::collections::BTreeMap::new(),
        system: PerfSmokeSystem {
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            kernel: "6.x".to_string(),
        },
    };

    let error = validate_perf_smoke_report(&report).expect_err("missing generated_at must fail");
    assert!(
        matches!(
            error,
            PerfLoopError::InvalidSmokeReportField {
                field: "generated_at"
            }
        ),
        "bead_id={BASELINE_LAYOUT_BEAD_ID} expected generated_at validation error, got {error:?}"
    );
}

#[test]
fn test_artifact_deser() {
    let report = PerfSmokeReport {
        generated_at: "2026-02-09T00:00:00Z".to_string(),
        scenario_id: "mvcc_smoke".to_string(),
        command: "cargo bench --bench mvcc_smoke".to_string(),
        seed: "424242".to_string(),
        trace_fingerprint: "sha256:trace".to_string(),
        git_sha: "0123456".to_string(),
        config_hash: "sha256:config".to_string(),
        alpha_total: 0.01,
        alpha_policy: "bonferroni".to_string(),
        metric_count: 6,
        artifacts: PerfSmokeArtifacts {
            criterion_dir: "target/criterion".to_string(),
            baseline_path: "baselines/criterion/base.json".to_string(),
            latest_path: "baselines/criterion/latest.json".to_string(),
        },
        env: std::collections::BTreeMap::from([(
            "RUSTFLAGS".to_string(),
            "-C force-frame-pointers=yes".to_string(),
        )]),
        system: PerfSmokeSystem {
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            kernel: "6.x".to_string(),
        },
    };

    let encoded = serde_json::to_string_pretty(&report).expect("serialize");
    let decoded: PerfSmokeReport = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(
        decoded, report,
        "bead_id={BASELINE_LAYOUT_BEAD_ID} serde roundtrip must preserve report"
    );
}

#[test]
fn test_e2e_bd_3cl3_4() {
    let temp = TempDir::new().expect("tempdir");
    let baselines_root = temp.path().join("baselines");
    ensure_baseline_layout(&baselines_root).expect("layout create");
    validate_baseline_layout(&baselines_root).expect("layout validate");

    fs::write(
        baselines_root.join("criterion").join("summary.json"),
        r#"{"bench":"mvcc_smoke","metric":"throughput"}"#,
    )
    .expect("write criterion summary");
    fs::write(
        baselines_root.join("hyperfine").join("cli.json"),
        r#"{"command":"fsqlite-cli","runs":10}"#,
    )
    .expect("write hyperfine summary");

    let smoke_report = PerfSmokeReport {
        generated_at: "2026-02-09T00:00:00Z".to_string(),
        scenario_id: "mvcc_smoke".to_string(),
        command: "cargo bench --bench mvcc_smoke".to_string(),
        seed: "3735928559".to_string(),
        trace_fingerprint: "sha256:trace".to_string(),
        git_sha: "deadbeef".to_string(),
        config_hash: "sha256:cfg".to_string(),
        alpha_total: 0.01,
        alpha_policy: "bonferroni".to_string(),
        metric_count: 12,
        artifacts: PerfSmokeArtifacts {
            criterion_dir: "target/criterion".to_string(),
            baseline_path: "baselines/criterion/base.json".to_string(),
            latest_path: "baselines/criterion/latest.json".to_string(),
        },
        env: std::collections::BTreeMap::from([(
            "RUSTFLAGS".to_string(),
            "-C force-frame-pointers=yes".to_string(),
        )]),
        system: PerfSmokeSystem {
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            kernel: "Linux-6.x".to_string(),
        },
    };

    let smoke_path = baselines_root.join("smoke").join("report.json");
    fs::write(
        &smoke_path,
        serde_json::to_vec_pretty(&smoke_report).expect("serialize smoke report"),
    )
    .expect("write smoke report");

    let loaded = load_and_validate_smoke_report(&smoke_path).expect("load+validate smoke report");
    assert_eq!(
        loaded.scenario_id, "mvcc_smoke",
        "bead_id={BASELINE_LAYOUT_BEAD_ID} e2e smoke report should survive roundtrip"
    );
}

fn fixed_schedule() -> Vec<ScheduleEvent> {
    vec![
        ScheduleEvent {
            actor: "writer-1".to_string(),
            action: "read page=17".to_string(),
        },
        ScheduleEvent {
            actor: "writer-2".to_string(),
            action: "write page=42".to_string(),
        },
        ScheduleEvent {
            actor: "writer-1".to_string(),
            action: "commit".to_string(),
        },
    ]
}

fn fixed_env() -> std::collections::BTreeMap<String, String> {
    record_measurement_env(
        "-C force-frame-pointers=yes",
        "perf,smoke",
        "lab",
        "deadbeef",
        "linux-x86_64",
    )
}

#[test]
fn test_seed_determinism() {
    let schedule = fixed_schedule();
    let env = fixed_env();

    let first = run_deterministic_measurement("mvcc_concurrent_writer", 4242, &schedule, &env)
        .expect("deterministic run should succeed");
    let second = run_deterministic_measurement("mvcc_concurrent_writer", 4242, &schedule, &env)
        .expect("deterministic run should succeed");

    assert_eq!(
        first, second,
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} same seed+schedule must produce identical output"
    );
}

#[test]
fn test_fingerprint_stability() {
    let schedule = fixed_schedule();
    let first = compute_trace_fingerprint(&schedule).expect("fingerprint generation must pass");
    let second = compute_trace_fingerprint(&schedule).expect("fingerprint generation must pass");

    assert_eq!(
        first, second,
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} fixed schedule fingerprint must be stable"
    );
}

#[test]
fn test_env_recording() {
    let env = fixed_env();
    validate_measurement_env(&env).expect("env metadata should be valid");
    let schedule = fixed_schedule();

    let measurement =
        run_deterministic_measurement("mvcc_env_capture", 7, &schedule, &env).expect("run");

    for key in ["RUSTFLAGS", "FEATURE_FLAGS", "GIT_SHA", "MODE", "PLATFORM"] {
        assert!(
            measurement.env.contains_key(key),
            "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} missing env key {key}"
        );
    }
    assert_eq!(
        measurement.git_sha, "deadbeef",
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} git sha should be propagated from env metadata"
    );
}

#[test]
fn test_schedule_replay() {
    let schedule = fixed_schedule();
    let fingerprint = compute_trace_fingerprint(&schedule).expect("fingerprint");
    let replayed = replay_schedule_from_fingerprint(&schedule, &fingerprint).expect("replay");

    assert_eq!(
        replayed, schedule,
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} replay should preserve exact interleaving"
    );
}

#[test]
fn test_e2e_bench_reproducible_same_seed() {
    let schedule = fixed_schedule();
    let env = fixed_env();

    let first =
        run_deterministic_measurement("mvcc_e2e_seed", 1337, &schedule, &env).expect("first run");
    let second =
        run_deterministic_measurement("mvcc_e2e_seed", 1337, &schedule, &env).expect("second run");

    let first_bundle = build_measurement_artifact_bundle(&first).expect("first bundle");
    let second_bundle = build_measurement_artifact_bundle(&second).expect("second bundle");

    assert_eq!(
        first_bundle, second_bundle,
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} e2e deterministic run should produce identical artifact bundles"
    );
}

#[test]
fn test_e2e_schedule_fingerprint_record_and_replay() {
    let schedule = fixed_schedule();
    let env = fixed_env();
    let measurement =
        run_deterministic_measurement("mvcc_schedule_replay", 2026, &schedule, &env).expect("run");

    let replayed =
        replay_schedule_from_fingerprint(&measurement.schedule, &measurement.trace_fingerprint)
            .expect("replay");
    assert_eq!(
        replayed, measurement.schedule,
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} recorded schedule must replay from fingerprint"
    );
}

#[test]
fn test_e2e_artifact_bundle_complete() {
    let temp = TempDir::new().expect("tempdir");
    let output_path = temp.path().join("measurement_bundle.json");

    let measurement =
        run_deterministic_measurement("mvcc_artifact_bundle", 99, &fixed_schedule(), &fixed_env())
            .expect("measurement");
    let bundle = build_measurement_artifact_bundle(&measurement).expect("bundle");
    write_measurement_artifact_bundle(&output_path, &bundle).expect("write bundle");

    let raw = fs::read_to_string(&output_path).expect("read bundle");
    let decoded: MeasurementArtifactBundle = serde_json::from_str(&raw).expect("decode bundle");

    assert!(
        !decoded.trace_id.is_empty(),
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} trace_id must be present"
    );
    assert!(
        decoded.schedule_fingerprint.starts_with("sha256:"),
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} schedule fingerprint must be sha256-prefixed"
    );
    assert!(
        decoded.env_fingerprint.starts_with("sha256:"),
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} env fingerprint must be sha256-prefixed"
    );
    assert_eq!(
        decoded.seed, 99,
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} seed must be persisted in artifact bundle"
    );
    assert_eq!(
        decoded.git_sha, "deadbeef",
        "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} git sha must be persisted in artifact bundle"
    );
}

fn high_value_opportunity_matrix() -> OpportunityMatrix {
    OpportunityMatrix {
        scenario_id: "mvcc_hot_path".to_string(),
        threshold: OPPORTUNITY_SCORE_THRESHOLD,
        entries: vec![OpportunityMatrixEntry {
            hotspot: "fsqlite_mvcc::scheduler::dispatch".to_string(),
            impact: 5,
            confidence: 4,
            effort: 2,
        }],
    }
}

#[test]
fn test_score_formula() {
    let entry = OpportunityMatrixEntry {
        hotspot: "fsqlite_pager::arc_cache::replace".to_string(),
        impact: 5,
        confidence: 3,
        effort: 5,
    };
    let score = compute_opportunity_score(&entry).expect("score");
    assert_eq!(
        score, 3.0,
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} expected (5*3)/5 = 3.0"
    );
}

#[test]
fn test_gate_rejection() {
    let entry = OpportunityMatrixEntry {
        hotspot: "fsqlite_vdbe::opcode::noop".to_string(),
        impact: 1,
        confidence: 1,
        effort: 5,
    };
    let error =
        enforce_opportunity_score_gate(&entry, OPPORTUNITY_SCORE_THRESHOLD).expect_err("reject");
    assert!(
        matches!(error, PerfLoopError::OpportunityScoreBelowThreshold { .. }),
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} expected below-threshold rejection, got {error:?}"
    );
}

#[test]
fn test_zero_confidence() {
    let entry = OpportunityMatrixEntry {
        hotspot: String::new(),
        impact: 5,
        confidence: 0,
        effort: 2,
    };
    let score = compute_opportunity_score(&entry).expect("score");
    assert_eq!(
        score, 0.0,
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} unnamed hotspot must force score to 0"
    );
}

#[test]
fn test_matrix_serialization() {
    let matrix = high_value_opportunity_matrix();
    let encoded = serde_json::to_string_pretty(&matrix).expect("encode matrix");
    let decoded: OpportunityMatrix = serde_json::from_str(&encoded).expect("decode matrix");
    assert_eq!(
        decoded, matrix,
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} matrix serialization roundtrip must be lossless"
    );
}

#[test]
fn test_e2e_opportunity_matrix_required() {
    let error = enforce_opportunity_matrix_required(None).expect_err("missing matrix should fail");
    assert!(
        matches!(error, PerfLoopError::MissingOpportunityMatrix),
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} expected missing-matrix gate failure, got {error:?}"
    );
}

#[test]
fn test_e2e_opportunity_matrix_score_gate() {
    let mut matrix = high_value_opportunity_matrix();
    validate_opportunity_matrix(&matrix).expect("valid matrix");
    let decisions = enforce_opportunity_matrix_gate(&matrix).expect("gate should pass");
    assert_eq!(
        decisions.len(),
        1,
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} expected one scored opportunity"
    );
    assert!(
        decisions[0].selected,
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} expected score to pass threshold"
    );

    matrix.entries[0].impact = 1;
    matrix.entries[0].confidence = 1;
    matrix.entries[0].effort = 5;
    let error = enforce_opportunity_matrix_gate(&matrix).expect_err("gate should reject");
    assert!(
        matches!(error, PerfLoopError::OpportunityScoreBelowThreshold { .. }),
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} expected score gate rejection, got {error:?}"
    );
}

#[test]
fn test_e2e_matrix_serialization_roundtrip() {
    let matrix = high_value_opportunity_matrix();
    let decisions = evaluate_opportunity_matrix(&matrix).expect("evaluate");

    let artifact = json!({
        "trace_id": "trace-1234abcd",
        "hotspot": matrix.entries[0].hotspot,
        "impact": matrix.entries[0].impact,
        "confidence": matrix.entries[0].confidence,
        "effort": matrix.entries[0].effort,
        "score": decisions[0].score,
        "threshold": decisions[0].threshold,
    });

    let encoded = serde_json::to_string_pretty(&artifact).expect("encode artifact");
    let decoded: serde_json::Value = serde_json::from_str(&encoded).expect("decode artifact");
    assert_eq!(
        decoded["trace_id"], "trace-1234abcd",
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} E2E artifact must preserve trace_id"
    );
    assert_eq!(
        decoded["threshold"], OPPORTUNITY_SCORE_THRESHOLD,
        "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} E2E artifact must preserve threshold"
    );
}

#[test]
fn test_flamegraph_generation() {
    let temp = TempDir::new().expect("tempdir");
    let flamegraph_path = temp.path().join("flamegraph.svg");
    fs::write(
        &flamegraph_path,
        r#"<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>"#,
    )
    .expect("write flamegraph");

    validate_flamegraph_output(&flamegraph_path).expect("svg flamegraph should validate");
}

#[test]
fn test_hyperfine_json_output() {
    let temp = TempDir::new().expect("tempdir");
    let hyperfine_path = temp.path().join("hyperfine.json");
    fs::write(
        &hyperfine_path,
        r#"{"command":"fsqlite-cli --bench","results":[{"mean":1.2}]}"#,
    )
    .expect("write hyperfine");

    validate_hyperfine_json_output(&hyperfine_path).expect("hyperfine json should validate");
}

#[test]
fn test_metadata_completeness() {
    let metadata = record_profiling_metadata(
        "deadbeef",
        "mvcc_hot_path?writers=32",
        "42",
        "RUSTFLAGS=-C force-frame-pointers=yes;features=perf",
        "Linux x86_64",
    );
    validate_profiling_metadata(&metadata).expect("metadata should be complete");
}

#[test]
fn test_cookbook_commands_exist() {
    let commands = canonical_profiling_cookbook_commands(
        "mvcc_stress",
        "mvcc_hot_path",
        "cargo bench --bench mvcc_stress",
    );
    validate_cookbook_commands_exist(&commands).expect("canonical commands should validate");
}

fn profiling_artifact_report_fixture() -> ProfilingArtifactReport {
    let metadata = record_profiling_metadata(
        "deadbeef",
        "mvcc_hot_path?writers=32",
        "42",
        "RUSTFLAGS=-C force-frame-pointers=yes;features=perf",
        "Linux x86_64",
    );
    let artifact_paths = std::collections::BTreeMap::from([
        (
            "flamegraph".to_string(),
            "artifacts/flamegraph.svg".to_string(),
        ),
        (
            "hyperfine".to_string(),
            "artifacts/hyperfine.json".to_string(),
        ),
        (
            "heaptrack".to_string(),
            "artifacts/heaptrack.out".to_string(),
        ),
        ("strace".to_string(), "artifacts/strace.txt".to_string()),
    ]);
    ProfilingArtifactReport {
        trace_id: "trace-prof-0001".to_string(),
        scenario_id: "mvcc_hot_path".to_string(),
        git_sha: "deadbeef".to_string(),
        artifact_paths,
        metadata,
    }
}

#[test]
fn test_e2e_profile_and_attach_artifacts() {
    let temp = TempDir::new().expect("tempdir");
    let artifacts_dir = temp.path().join("artifacts");
    fs::create_dir_all(&artifacts_dir).expect("create artifacts dir");
    fs::write(
        artifacts_dir.join("flamegraph.svg"),
        r#"<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>"#,
    )
    .expect("write flamegraph");
    fs::write(
        artifacts_dir.join("hyperfine.json"),
        r#"{"command":"cargo bench --bench mvcc_stress","results":[{"mean":1.2}]}"#,
    )
    .expect("write hyperfine");
    fs::write(artifacts_dir.join("heaptrack.out"), "heaptrack-report").expect("write heaptrack");
    fs::write(artifacts_dir.join("strace.txt"), "strace-summary").expect("write strace");

    let report = profiling_artifact_report_fixture();
    validate_profiling_artifact_report(&report).expect("report schema should validate");
    validate_profiling_artifact_paths(temp.path(), &report).expect("artifact paths should exist");
}

#[test]
fn test_e2e_toolchain_presence_gate() {
    let available = enforce_profiling_toolchain_presence_with(|tool| Some(format!("{tool} 1.0.0")))
        .expect("all tools available should pass");
    assert_eq!(
        available.missing.len(),
        0,
        "bead_id={PROFILING_COOKBOOK_BEAD_ID} no tools should be missing in passing gate"
    );

    let error = enforce_profiling_toolchain_presence_with(|tool| {
        if tool == "heaptrack" {
            None
        } else {
            Some(format!("{tool} 1.0.0"))
        }
    })
    .expect_err("missing tool should fail gate");
    match error {
        PerfLoopError::ToolUnavailable { tool, remediation } => {
            assert_eq!(
                tool, "heaptrack",
                "bead_id={PROFILING_COOKBOOK_BEAD_ID} missing tool should be reported explicitly"
            );
            assert!(
                remediation.contains("install"),
                "bead_id={PROFILING_COOKBOOK_BEAD_ID} remediation guidance should be present"
            );
        }
        other => {
            panic!("bead_id={PROFILING_COOKBOOK_BEAD_ID} expected ToolUnavailable, got {other:?}")
        }
    }
}

#[test]
fn test_e2e_perf_change_requires_behavior_lock() {
    let (_temp, golden_dir, checksum_file) = make_conformance_golden_fixture();
    enforce_behavior_lock_ci(true, &golden_dir, &checksum_file)
        .expect("perf-only behavior lock should pass on unchanged outputs");

    fs::write(golden_dir.join("query_rows.txt"), "alice|1\nbob|999\n")
        .expect("mutate conformance output");
    let error = enforce_behavior_lock_ci(true, &golden_dir, &checksum_file)
        .expect_err("perf-only behavior lock must fail on drift");
    assert!(
        matches!(error, PerfLoopError::GoldenChecksumMismatch { .. }),
        "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} expected mismatch failure for perf drift, got {error:?}"
    );
}

#[test]
fn test_e2e_conformance_artifacts_included_in_lock() {
    let temp = TempDir::new().expect("tempdir");
    let golden_dir = temp.path().join("golden_outputs");
    fs::create_dir_all(&golden_dir).expect("create dir");
    fs::write(golden_dir.join("query_rows.txt"), "alice|1").expect("write rows");
    fs::write(golden_dir.join("error_codes.txt"), "SQLITE_OK").expect("write codes");
    fs::write(golden_dir.join("CommitMarker.json"), "{}").expect("write marker");
    fs::write(golden_dir.join("CommitProof.json"), "{}").expect("write proof");

    let checksum_file = temp.path().join("golden_checksums.txt");
    capture_golden_checksums(&golden_dir, &checksum_file).expect("capture checksums");

    let error = enforce_behavior_lock_ci(true, &golden_dir, &checksum_file)
        .expect_err("missing AbortWitness should fail conformance inclusion gate");
    assert!(
        matches!(
            error,
            PerfLoopError::MissingConformanceArtifact {
                name: "AbortWitness"
            }
        ),
        "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} expected AbortWitness inclusion failure, got {error:?}"
    );
}
