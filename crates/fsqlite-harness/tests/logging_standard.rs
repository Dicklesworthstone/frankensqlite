use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tempfile::tempdir;

use fsqlite_harness::log::{
    ConformanceDiff, LOG_SCHEMA_VERSION, LifecycleEventKind, PerfBaselineArtifact,
    REQUIRED_BUNDLE_FILES, RunStatus, detect_optimization_levers, init_repro_bundle,
    validate_bundle, validate_bundle_meta, validate_events_jsonl, validate_perf_optimization_loop,
    validate_required_files,
};

const BEAD_ID: &str = "bd-1fpm";
const PERF_LOOP_BEAD_ID: &str = "bd-3cl3.1";

#[test]
fn test_log_bundle_meta_json_schema_valid() {
    let temp = tempdir().expect("tempdir should be created");
    let mut bundle = init_repro_bundle(temp.path(), "harness", "meta_schema", 4242)
        .expect("bundle initialization should succeed");

    bundle
        .emit_event(LifecycleEventKind::Setup, "setup", BTreeMap::new())
        .expect("setup event should be emitted");

    let bundle_root = bundle
        .finish(RunStatus::Passed)
        .expect("bundle finalization should succeed");

    let meta = validate_bundle_meta(&bundle_root).expect("meta schema should validate");
    assert_eq!(
        meta.schema_version, LOG_SCHEMA_VERSION,
        "bead_id={BEAD_ID} case=meta_schema_version"
    );
    assert_eq!(meta.suite, "harness", "bead_id={BEAD_ID} case=meta_suite");
    assert_eq!(
        meta.case_id, "meta_schema",
        "bead_id={BEAD_ID} case=meta_case_id"
    );
    assert_eq!(meta.seed, 4242, "bead_id={BEAD_ID} case=meta_seed");
}

#[test]
fn test_events_jsonl_is_valid_jsonl() {
    let temp = tempdir().expect("tempdir should be created");
    let mut bundle = init_repro_bundle(temp.path(), "harness", "jsonl_validation", 7)
        .expect("bundle initialization should succeed");

    let mut payload = BTreeMap::new();
    payload.insert("phase".to_string(), Value::String("core".to_string()));
    payload.insert("step_idx".to_string(), json!(1));

    bundle
        .emit_event(LifecycleEventKind::Step, "do_work", payload)
        .expect("step event should be emitted");

    let bundle_root = bundle
        .finish(RunStatus::Passed)
        .expect("bundle finalization should succeed");

    let events = validate_events_jsonl(&bundle_root).expect("events.jsonl should parse");
    assert!(
        !events.is_empty(),
        "bead_id={BEAD_ID} case=events_non_empty"
    );
    assert_eq!(
        events.first().map(|event| event.kind),
        Some(LifecycleEventKind::RunStart),
        "bead_id={BEAD_ID} case=events_start"
    );
    assert_eq!(
        events.last().map(|event| event.kind),
        Some(LifecycleEventKind::RunEnd),
        "bead_id={BEAD_ID} case=events_end"
    );
}

#[test]
fn test_bundle_contains_required_files() {
    let temp = tempdir().expect("tempdir should be created");
    let incomplete_root = temp.path().join("incomplete_bundle");
    std::fs::create_dir_all(&incomplete_root).expect("incomplete root should be created");
    std::fs::write(incomplete_root.join("meta.json"), "{}").expect("meta stub should be written");

    let error =
        validate_required_files(&incomplete_root).expect_err("missing files must fail validation");
    let rendered = error.to_string();
    for required in REQUIRED_BUNDLE_FILES {
        if required != "meta.json" {
            assert!(
                rendered.contains(required),
                "bead_id={BEAD_ID} case=required_file_missing required={required} err={rendered}"
            );
        }
    }
}

#[test]
fn test_e2e_harness_emits_repro_bundle_on_failure() {
    let temp = tempdir().expect("tempdir should be created");
    let bundle_root = run_known_failing_harness_case(temp.path())
        .expect("known failing harness case should still emit bundle");

    validate_bundle(&bundle_root).expect("bundle should satisfy required validation checks");

    assert!(
        bundle_root.join("db_snapshot.json").is_file(),
        "bead_id={BEAD_ID} case=e2e_db_snapshot_present"
    );
    assert!(
        bundle_root.join("db-wal").is_file(),
        "bead_id={BEAD_ID} case=e2e_wal_present"
    );
    assert!(
        bundle_root.join("oracle_diff.json").is_file(),
        "bead_id={BEAD_ID} case=e2e_oracle_diff_present"
    );
}

#[test]
fn test_loop_one_lever_only() {
    let changed_paths = vec![
        PathBuf::from("crates/fsqlite-core/src/lib.rs"),
        PathBuf::from("crates/fsqlite-mvcc/src/lifecycle.rs"),
    ];

    let detected = detect_optimization_levers(&changed_paths);
    assert_eq!(
        detected,
        vec!["fsqlite-core".to_string(), "fsqlite-mvcc".to_string()],
        "bead_id={PERF_LOOP_BEAD_ID} case=lever_detection_multicrate"
    );

    let err = validate_perf_optimization_loop(&changed_paths, None, None, None)
        .expect_err("multi-lever optimization change must be rejected");
    let rendered = err.to_string();
    assert!(
        rendered.contains("multiple optimization levers"),
        "bead_id={PERF_LOOP_BEAD_ID} case=reject_multi_lever err={rendered}"
    );
}

#[test]
fn test_baseline_capture_required() {
    let changed_paths = vec![PathBuf::from("crates/fsqlite-core/src/region.rs")];

    let err = validate_perf_optimization_loop(&changed_paths, None, None, None)
        .expect_err("optimization change without baseline must fail");
    let rendered = err.to_string();
    assert!(
        rendered.contains("missing baseline artifact"),
        "bead_id={PERF_LOOP_BEAD_ID} case=baseline_required err={rendered}"
    );
}

#[test]
fn test_golden_unchanged() {
    let temp = tempdir().expect("tempdir should be created");
    let baseline_path = temp.path().join("perf_baseline.json");
    let golden_before = temp.path().join("golden-before.bin");
    let golden_after = temp.path().join("golden-after.bin");

    write_perf_baseline_artifact(&baseline_path);
    std::fs::write(&golden_before, b"golden-lock").expect("golden before should be written");
    std::fs::write(&golden_after, b"golden-lock").expect("golden after should be written");

    let changed_paths = vec![PathBuf::from("crates/fsqlite-core/src/commit_repair.rs")];
    let report = validate_perf_optimization_loop(
        &changed_paths,
        Some(&baseline_path),
        Some(&golden_before),
        Some(&golden_after),
    )
    .expect("single-lever optimization with matching golden outputs should pass");

    assert_eq!(
        report.lever_keys,
        vec!["fsqlite-core".to_string()],
        "bead_id={PERF_LOOP_BEAD_ID} case=single_lever_ok"
    );
    let baseline = report
        .baseline
        .expect("baseline should be present for optimization report");
    assert_eq!(
        baseline.scenario_id, "hot_page_read",
        "bead_id={PERF_LOOP_BEAD_ID} case=baseline_scenario"
    );
    assert_eq!(
        report.golden_before_sha256, report.golden_after_sha256,
        "bead_id={PERF_LOOP_BEAD_ID} case=golden_checksum_lock"
    );
}

fn write_perf_baseline_artifact(path: &Path) {
    let baseline = PerfBaselineArtifact {
        trace_id: "trace-123".to_string(),
        scenario_id: "hot_page_read".to_string(),
        git_sha: "deadbeef".to_string(),
        artifact_paths: vec![
            "artifacts/baseline.json".to_string(),
            "artifacts/profile.folded".to_string(),
        ],
        p50_micros: 100,
        p95_micros: 140,
        p99_micros: 180,
        throughput_ops_per_sec: 50_000,
        alloc_count: 2_500,
    };
    let bytes = serde_json::to_vec_pretty(&baseline).expect("baseline json should serialize");
    std::fs::write(path, bytes).expect("baseline artifact should be written");
}

fn run_known_failing_harness_case(base_dir: &Path) -> fsqlite_error::Result<PathBuf> {
    let mut bundle = init_repro_bundle(base_dir, "harness_e2e", "known_failure", 1337)?;

    bundle.emit_event(
        LifecycleEventKind::Setup,
        "setup",
        BTreeMap::from([("stage".to_string(), Value::String("begin".to_string()))]),
    )?;

    bundle.append_stdout("running known failing case")?;
    bundle.append_stderr("assertion failed: expected 1 got 2")?;

    bundle.write_artifact_json("db_snapshot.json", &json!({ "tables": [] }))?;
    bundle.write_artifact_json("db-wal", &json!({ "frames": [] }))?;

    bundle.record_conformance_diff(&ConformanceDiff {
        case_id: "known_failure".to_string(),
        sql: "SELECT 1".to_string(),
        params: "[]".to_string(),
        oracle_result: "[[1]]".to_string(),
        franken_result: "[[2]]".to_string(),
        diff: "[{\"index\":0,\"expected\":1,\"actual\":2}]".to_string(),
    })?;

    bundle.emit_event(
        LifecycleEventKind::Assertion,
        "assertion_failed",
        BTreeMap::from([("reason".to_string(), Value::String("mismatch".to_string()))]),
    )?;

    bundle.finish(RunStatus::Failed)
}
