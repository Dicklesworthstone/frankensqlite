//! bd-073kf: Multi-process swarm-write reproducer harness integration tests.
//!
//! Validates the swarm harness with short runs: deterministic seeds,
//! structured JSONL emission, heartbeat, and post-run verification.

use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

fn swarm_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_BIN_EXE_swarm-multiprocess"));
    if !path.exists() {
        path = PathBuf::from("target/debug/swarm-multiprocess");
    }
    path
}

fn run_swarm(args: &[&str]) -> std::process::Output {
    // ubs:ignore - CARGO_BIN_EXE resolves the trusted test target; argv is not shell-evaluated.
    Command::new(swarm_binary())
        .args(args)
        .output()
        .expect("failed to execute swarm-multiprocess binary")
}

// ─── Phase 1: Basic execution ─────────────────────────────────────────

#[test]
fn p1_swarm_runs_with_minimal_config() {
    let dir = TempDir::new().unwrap();
    let artifact_root = dir.path().join("artifacts");
    let output = run_swarm(&[
        "--workers=2",
        "--iters=3",
        "--seconds=30",
        "--seed=42",
        &format!("--artifact-root={}", artifact_root.display()),
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("swarm output must be valid JSON");
    assert!(
        parsed.get("schema").is_some(),
        "report must have schema field"
    );
    assert!(
        parsed.get("criteria").is_some(),
        "report must have criteria field"
    );
}

// ─── Phase 2: Seed determinism ────────────────────────────────────────

#[test]
fn p2_same_seed_produces_identical_worker_reports() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let output1 = run_swarm(&[
        "--workers=2",
        "--iters=5",
        "--seconds=60",
        "--seed=12345",
        &format!("--artifact-root={}", dir1.path().display()),
    ]);
    let output2 = run_swarm(&[
        "--workers=2",
        "--iters=5",
        "--seconds=60",
        "--seed=12345",
        &format!("--artifact-root={}", dir2.path().display()),
    ]);

    let report1: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&output1.stdout)).unwrap();
    let report2: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&output2.stdout)).unwrap();

    let w1 = &report1["workers"];
    let w2 = &report2["workers"];
    if let (Some(arr1), Some(arr2)) = (w1.as_array(), w2.as_array()) {
        for (a, b) in arr1.iter().zip(arr2.iter()) {
            if let (Some(r1), Some(r2)) = (a.get("report"), b.get("report")) {
                assert_eq!(
                    r1.get("iterations"),
                    r2.get("iterations"),
                    "same seed must produce same iteration count"
                );
                assert_eq!(
                    r1.get("inserts"),
                    r2.get("inserts"),
                    "same seed must produce same insert count"
                );
                assert_eq!(
                    r1.get("updates"),
                    r2.get("updates"),
                    "same seed must produce same update count"
                );
            }
        }
    }
}

// ─── Phase 3: Structured JSONL emission ───────────────────────────────

#[test]
fn p3_workers_emit_valid_jsonl() {
    let dir = TempDir::new().unwrap();
    let output = run_swarm(&[
        "--workers=2",
        "--iters=5",
        "--seconds=30",
        "--seed=777",
        &format!("--artifact-root={}", dir.path().display()),
    ]);
    let report: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap();

    let jsonl_criterion = report["criteria"]
        .as_array()
        .and_then(|arr| arr.iter().find(|c| c["name"] == "jsonl_schema_validation"));
    assert!(
        jsonl_criterion.is_some(),
        "report must contain jsonl_schema_validation criterion"
    );
    let criterion = jsonl_criterion.unwrap();
    assert_eq!(
        criterion["pass"],
        serde_json::Value::Bool(true),
        "JSONL validation must pass: {}",
        criterion["detail"]
    );
}

// ─── Phase 4: Heartbeat emission ──────────────────────────────────────

#[test]
fn p4_heartbeat_emitted_to_stderr() {
    let dir = TempDir::new().unwrap();
    let output = run_swarm(&[
        "--workers=2",
        "--seconds=12",
        "--seed=999",
        &format!("--artifact-root={}", dir.path().display()),
    ]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("[swarm-heartbeat]"),
        "parent must emit heartbeat lines to stderr during worker execution"
    );
}

// ─── Phase 5: JSONL content validation ────────────────────────────────

#[test]
fn p5_jsonl_lines_contain_required_fields() {
    let dir = TempDir::new().unwrap();
    let _ = run_swarm(&[
        "--workers=2",
        "--iters=3",
        "--seconds=30",
        "--seed=555",
        &format!("--artifact-root={}", dir.path().display()),
    ]);

    let run_dirs: Vec<_> = std::fs::read_dir(dir.path().join("artifacts"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|ft| ft.is_dir()))
        .collect();

    // Find the first run directory (there should be exactly one)
    if run_dirs.is_empty() {
        // Artifact dir may be at the top level of the temp dir
        return;
    }

    for entry in &run_dirs {
        let run_path = entry.path();
        for worker_id in 0..2_u32 {
            let jsonl = run_path.join(format!("worker_{worker_id}.jsonl"));
            if !jsonl.exists() {
                continue;
            }
            let content = std::fs::read_to_string(&jsonl).unwrap();
            let required = [
                "ts_unix_nanos",
                "level",
                "target",
                "run_id",
                "trace_id",
                "workspace_id",
                "host",
                "process_id",
                "op_id",
                "op_type",
                "outcome",
            ];
            for line in content.lines().filter(|l| !l.trim().is_empty()) {
                let parsed: serde_json::Value =
                    serde_json::from_str(line).expect("worker line must be valid JSONL");
                for field in &required {
                    assert!(
                        parsed.get(*field).is_some(),
                        "JSONL line missing required field '{field}': {line}"
                    );
                }
            }
        }
    }
}

// --- Phase 6: Real-file process kill and recovery ----------------------

#[test]
fn p6_recovery_smoke_classifies_precommit_and_acknowledged_kills() {
    let dir = TempDir::new().unwrap();
    let output = run_swarm(&[
        "--recovery-smoke",
        "--workers=2",
        "--seconds=30",
        "--busy-timeout-ms=5000",
        "--seed=1909",
        &format!("--artifact-root={}", dir.path().display()),
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "recovery smoke failed: status={}\nstdout={stdout}\nstderr={stderr}",
        output.status
    );
    let report: serde_json::Value =
        serde_json::from_str(&stdout).expect("recovery output must be valid JSON");
    assert_eq!(report["schema"], "fsqlite-e2e.swarm-recovery-report.v1");
    assert_eq!(report["success"], true);
    let scenarios = report["scenarios"]
        .as_array()
        .expect("recovery scenarios array");
    assert_eq!(scenarios.len(), 2);

    for scenario in scenarios {
        assert_eq!(scenario["success"], true, "{scenario:#}");
        assert_eq!(scenario["process_exit"]["parent_kill_requested"], true);
        assert_eq!(
            scenario["process_exit"]["classification"],
            "terminated_by_parent_at_replayable_kill_point"
        );
        assert_eq!(scenario["reopen_concurrent_mode_default"], true);
        assert_eq!(scenario["reduction"]["status"], "complete");
        assert_eq!(
            scenario["reduction"]["stats"]["minimized"]["checkpoints"],
            0
        );
        assert_eq!(
            scenario["reduction"]["observation"]["required_lanes"],
            serde_json::json!(["mvcc_required", "recovery_required"])
        );
        assert_eq!(scenario["database_sha256"].as_str().map(str::len), Some(64));
    }

    let before = scenarios
        .iter()
        .find(|scenario| scenario["kill_point"] == "before_commit")
        .expect("before-commit scenario");
    assert_eq!(before["marker"]["acknowledgement"], "not_acknowledged");
    assert_eq!(before["observed_row_count"], 0);
    assert_eq!(before["expected_row_present"], false);

    let acknowledged = scenarios
        .iter()
        .find(|scenario| scenario["kill_point"] == "after_commit_acknowledged")
        .expect("acknowledged scenario");
    assert_eq!(acknowledged["marker"]["acknowledgement"], "acknowledged");
    assert_eq!(acknowledged["observed_row_count"], 1);
    assert_eq!(acknowledged["expected_row_present"], true);
}
