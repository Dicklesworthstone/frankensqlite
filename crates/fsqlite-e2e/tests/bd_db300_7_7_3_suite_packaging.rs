//! bd-db300.7.7.3: CI and operator packaging contract validation.
//!
//! Validates that the verify-suite binary produces correct packaging artifacts,
//! retention class escalation, pass/fail signatures, structured log fields,
//! counterexample bundles, rerun entrypoints, and CI/local command prefixes.
//!
//! Every test invokes the real `realdb-e2e verify-suite` binary — no mocks.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn realdb_e2e_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_realdb-e2e"))
}

fn fresh_output_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("fsqlite-g773-tests").join(label);
    if dir.exists() {
        let _ = fs::remove_dir_all(&dir);
    }
    fs::create_dir_all(&dir).expect("create output dir");
    dir
}

fn run_verify_suite(args: &[&str], output_dir: &Path) -> std::process::Output {
    let mut cmd = Command::new(realdb_e2e_binary());
    cmd.arg("verify-suite");
    cmd.args(args);
    cmd.arg("--output-dir");
    cmd.arg(output_dir);
    cmd.arg("--pretty");
    cmd.output().expect("failed to run realdb-e2e verify-suite")
}

fn load_package_json(output_dir: &Path) -> serde_json::Value {
    let path = output_dir.join("suite_package.json");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn load_log_jsonl(output_dir: &Path) -> Vec<serde_json::Value> {
    let path = output_dir.join("logs/verify_suite.jsonl");
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("parse JSONL line: {e}")))
        .collect()
}

// ── P1: retention class escalation ─────────────────────────────────────

#[test]
fn p1_quick_no_shadow_produces_quick_run_retention() {
    let dir = fresh_output_dir("p1_quick_run");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--verification-depth",
            "quick",
            "--shadow-mode",
            "off",
        ],
        &dir,
    );
    assert!(out.status.success(), "exit code: {}", out.status);
    let pkg = load_package_json(&dir);
    assert_eq!(pkg["retention_class"], "quick_run");
    assert_eq!(pkg["pass_fail_signature"], "pass.quick_contract");
}

#[test]
fn p1_full_no_shadow_produces_full_proof_retention() {
    let dir = fresh_output_dir("p1_full_proof");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--verification-depth",
            "full",
            "--shadow-mode",
            "off",
        ],
        &dir,
    );
    assert!(out.status.success(), "exit code: {}", out.status);
    let pkg = load_package_json(&dir);
    assert_eq!(pkg["retention_class"], "full_proof");
    assert_eq!(pkg["pass_fail_signature"], "pass.full_contract");
}

#[test]
fn p1_quick_with_shadow_forced_escalates_to_full_proof() {
    let dir = fresh_output_dir("p1_shadow_escalation");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--verification-depth",
            "quick",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "clean",
        ],
        &dir,
    );
    assert!(out.status.success(), "exit code: {}", out.status);
    let pkg = load_package_json(&dir);
    assert_eq!(pkg["retention_class"], "full_proof");
    assert_eq!(pkg["pass_fail_signature"], "pass.shadow_clean");
}

#[test]
fn p1_diverged_verdict_produces_failure_bundle() {
    let dir = fresh_output_dir("p1_failure_bundle");
    let cx_path = dir.join("counterexamples/shadow_counterexample_bundle.json");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--verification-depth",
            "quick",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "diverged",
            "--kill-switch-state",
            "tripped",
            "--divergence-class",
            "semantic_result_mismatch",
            "--counterexample-bundle",
            cx_path.to_str().unwrap(),
        ],
        &dir,
    );
    assert!(out.status.success(), "exit code: {}", out.status);
    let pkg = load_package_json(&dir);
    assert_eq!(pkg["retention_class"], "failure_bundle");
    assert_eq!(pkg["pass_fail_signature"], "fail.shadow_divergence");
    assert!(cx_path.exists(), "counterexample bundle must be written");
}

#[test]
fn p1_tripped_kill_switch_forces_failure_bundle() {
    let dir = fresh_output_dir("p1_tripped_ks");
    let cx_path = dir.join("cx.json");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "diverged",
            "--kill-switch-state",
            "tripped",
            "--divergence-class",
            "state_hash_mismatch",
            "--counterexample-bundle",
            cx_path.to_str().unwrap(),
        ],
        &dir,
    );
    assert!(out.status.success(), "exit code: {}", out.status);
    let pkg = load_package_json(&dir);
    assert_eq!(pkg["retention_class"], "failure_bundle");
}

// ── P2: pass/fail signature completeness ───────────────────────────────

#[test]
fn p2_pending_shadow_execution_signature() {
    let dir = fresh_output_dir("p2_pending");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--shadow-mode",
            "sampled",
            "--shadow-verdict",
            "pending_execution",
        ],
        &dir,
    );
    assert!(out.status.success());
    let pkg = load_package_json(&dir);
    assert_eq!(pkg["pass_fail_signature"], "pending.shadow_execution");
}

#[test]
fn p2_all_five_signatures_distinct() {
    let combos: &[(&[&str], &str)] = &[
        (
            &["--shadow-mode", "off", "--verification-depth", "quick"],
            "pass.quick_contract",
        ),
        (
            &["--shadow-mode", "off", "--verification-depth", "full"],
            "pass.full_contract",
        ),
        (
            &[
                "--shadow-mode",
                "forced",
                "--shadow-verdict",
                "pending_execution",
            ],
            "pending.shadow_execution",
        ),
        (
            &["--shadow-mode", "forced", "--shadow-verdict", "clean"],
            "pass.shadow_clean",
        ),
        (
            &[
                "--shadow-mode",
                "forced",
                "--shadow-verdict",
                "diverged",
                "--kill-switch-state",
                "tripped",
                "--divergence-class",
                "invariant_violation",
            ],
            "fail.shadow_divergence",
        ),
    ];
    let mut seen = HashSet::new();
    for (i, (args, expected)) in combos.iter().enumerate() {
        let dir = fresh_output_dir(&format!("p2_sig_{i}"));
        let mut full_args: Vec<&str> = vec!["--mode", "fsqlite_mvcc"];
        full_args.extend_from_slice(args);
        let counterexample_bundle = if *expected == "fail.shadow_divergence" {
            let cx_path_str = dir.join("cx.json");
            Some(cx_path_str.to_str().unwrap().to_owned())
        } else {
            None
        };
        if let Some(cx_str) = counterexample_bundle.as_deref() {
            full_args.push("--counterexample-bundle");
            full_args.push(cx_str);
        }
        let out = run_verify_suite(&full_args, &dir);
        assert!(out.status.success(), "combo {i} failed: {}", out.status);
        let pkg = load_package_json(&dir);
        let sig = pkg["pass_fail_signature"].as_str().unwrap().to_owned();
        assert_eq!(&sig, *expected, "combo {i}: expected {expected}, got {sig}");
        seen.insert(sig);
    }
    assert_eq!(seen.len(), 5, "all 5 signatures must be distinct");
}

// ── P3: CI vs local entrypoint prefixes ────────────────────────────────

#[test]
fn p3_local_context_has_no_rch_prefix() {
    let dir = fresh_output_dir("p3_local");
    let out = run_verify_suite(
        &["--mode", "sqlite_reference", "--execution-context", "local"],
        &dir,
    );
    assert!(out.status.success());
    let pkg = load_package_json(&dir);
    let local = pkg["local_entrypoint"].as_str().unwrap();
    let ci = pkg["ci_entrypoint"].as_str().unwrap();
    assert!(
        local.starts_with("cargo "),
        "local_entrypoint must start with 'cargo': {local}"
    );
    assert!(
        ci.starts_with("rch exec -- cargo "),
        "ci_entrypoint must start with 'rch exec -- cargo': {ci}"
    );
}

#[test]
fn p3_ci_context_rerun_uses_rch() {
    let dir = fresh_output_dir("p3_ci_rerun");
    let out = run_verify_suite(
        &["--mode", "fsqlite_mvcc", "--execution-context", "ci"],
        &dir,
    );
    assert!(out.status.success());
    let pkg = load_package_json(&dir);
    let rerun = pkg["rerun_entrypoint"].as_str().unwrap();
    assert!(
        rerun.starts_with("rch exec -- "),
        "CI rerun_entrypoint must start with 'rch exec -- ': {rerun}"
    );
}

// ── P4: artifact directory structure ───────────────────────────────────

#[test]
fn p4_artifacts_written_to_output_dir() {
    let dir = fresh_output_dir("p4_artifacts");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_single_writer",
            "--verification-depth",
            "full",
            "--shadow-mode",
            "off",
        ],
        &dir,
    );
    assert!(out.status.success());
    assert!(
        dir.join("suite_package.json").exists(),
        "suite_package.json"
    );
    assert!(dir.join("suite_summary.md").exists(), "suite_summary.md");
    assert!(
        dir.join("logs/verify_suite.jsonl").exists(),
        "logs/verify_suite.jsonl"
    );
    assert!(
        dir.join("rerun_entrypoint.sh").exists(),
        "rerun_entrypoint.sh"
    );
    assert!(
        dir.join("focused_rerun_entrypoint.sh").exists(),
        "focused_rerun_entrypoint.sh"
    );
}

#[test]
fn p4_rerun_scripts_are_executable() {
    let dir = fresh_output_dir("p4_exec");
    let out = run_verify_suite(&["--mode", "fsqlite_mvcc", "--shadow-mode", "off"], &dir);
    assert!(out.status.success());
    for name in &["rerun_entrypoint.sh", "focused_rerun_entrypoint.sh"] {
        let path = dir.join(name);
        assert!(path.exists(), "{name} must exist");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode();
            assert!(
                mode & 0o111 != 0,
                "{name} must be executable (mode={mode:#o})"
            );
        }
    }
}

// ── P5: structured JSONL log field completeness ────────────────────────

#[test]
fn p5_jsonl_log_contains_all_required_packaging_fields() {
    let dir = fresh_output_dir("p5_log_fields");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--placement-profile",
            "recommended_pinned",
            "--verification-depth",
            "full",
            "--activation-regime",
            "red_path_correctness",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "clean",
        ],
        &dir,
    );
    assert!(out.status.success());
    let logs = load_log_jsonl(&dir);
    assert!(!logs.is_empty(), "JSONL log must have at least one line");
    let entry = &logs[0];
    let required_fields = [
        "trace_id",
        "scenario_id",
        "suite_id",
        "execution_context",
        "verification_depth",
        "activation_regime",
        "shadow_mode",
        "shadow_verdict",
        "kill_switch_state",
        "artifact_root",
        "retention_class",
        "rerun_entrypoint",
        "pass_fail_signature",
        "divergence_class",
        "mode",
        "placement_profile_id",
    ];
    let mut missing = Vec::new();
    for field in &required_fields {
        if entry.get(*field).is_none() {
            missing.push(*field);
        }
    }
    assert!(
        missing.is_empty(),
        "JSONL log missing fields: {missing:?}\nGot keys: {:?}",
        entry.as_object().map(|o| o.keys().collect::<Vec<_>>())
    );
}

// ── P6: counterexample bundle structure ────────────────────────────────

#[test]
fn p6_counterexample_bundle_has_required_fields() {
    let dir = fresh_output_dir("p6_cx_bundle");
    let cx_path = dir.join("counterexamples/shadow_counterexample_bundle.json");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "diverged",
            "--kill-switch-state",
            "tripped",
            "--divergence-class",
            "fallback_contract_breach",
            "--counterexample-bundle",
            cx_path.to_str().unwrap(),
            "--first-failure-diagnostics",
            "page 42 hash mismatch after checkpoint",
        ],
        &dir,
    );
    assert!(out.status.success());
    assert!(cx_path.exists(), "counterexample bundle file must exist");
    let raw = fs::read_to_string(&cx_path).unwrap();
    let bundle: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let required = [
        "schema_version",
        "trace_id",
        "scenario_id",
        "suite_id",
        "mode",
        "activation_regime",
        "shadow_mode",
        "shadow_verdict",
        "kill_switch_state",
        "divergence_class",
        "rerun_entrypoint",
        "focused_rerun_entrypoint",
        "first_failure_diagnostics",
    ];
    let mut missing = Vec::new();
    for field in &required {
        if bundle.get(*field).is_none() {
            missing.push(*field);
        }
    }
    assert!(
        missing.is_empty(),
        "counterexample bundle missing: {missing:?}\nGot: {:?}",
        bundle.as_object().map(|o| o.keys().collect::<Vec<_>>())
    );
    assert_eq!(bundle["divergence_class"], "fallback_contract_breach");
    assert!(
        bundle["first_failure_diagnostics"]
            .as_str()
            .unwrap()
            .contains("page 42"),
        "diagnostics must include user-provided text"
    );
}

// ── P7: shadow contract validation rejects illegal combos ──────────────

#[test]
fn p7_shadow_off_rejects_non_not_run_verdict() {
    let dir = fresh_output_dir("p7_bad_shadow");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--shadow-mode",
            "off",
            "--shadow-verdict",
            "clean",
        ],
        &dir,
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "must exit 2 for contract violation"
    );
}

#[test]
fn p7_diverged_auto_generates_counterexample_bundle() {
    let dir = fresh_output_dir("p7_auto_cx");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "diverged",
            "--kill-switch-state",
            "tripped",
            "--divergence-class",
            "invariant_violation",
        ],
        &dir,
    );
    assert!(out.status.success(), "auto-cx must succeed: {}", out.status);
    let pkg = load_package_json(&dir);
    assert!(
        pkg["counterexample_bundle"].is_string(),
        "counterexample_bundle must be auto-populated"
    );
    let cx_path_str = pkg["counterexample_bundle"].as_str().unwrap();
    assert!(
        Path::new(cx_path_str).exists(),
        "auto-generated counterexample bundle must exist at {cx_path_str}"
    );
}

#[test]
fn p7_divergence_class_without_diverged_verdict_rejected() {
    let dir = fresh_output_dir("p7_bad_divergence");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--shadow-mode",
            "forced",
            "--shadow-verdict",
            "clean",
            "--divergence-class",
            "state_hash_mismatch",
        ],
        &dir,
    );
    assert_eq!(out.status.code(), Some(2));
}

// ── P8: all three modes × two contexts produce valid packages ──────────

#[test]
fn p8_all_modes_produce_valid_packages() {
    for (i, mode) in ["sqlite_reference", "fsqlite_mvcc", "fsqlite_single_writer"]
        .iter()
        .enumerate()
    {
        let dir = fresh_output_dir(&format!("p8_mode_{i}"));
        let out = run_verify_suite(&["--mode", mode], &dir);
        assert!(out.status.success(), "mode {mode} failed: {}", out.status);
        let pkg = load_package_json(&dir);
        assert_eq!(pkg["mode"], *mode, "mode field must match for {mode}");
        assert_eq!(
            pkg["schema_version"], "fsqlite-e2e.verify_suite_package.v2",
            "schema version for {mode}"
        );
    }
}

// ── P9: default output directory layout ────────────────────────────────

#[test]
fn p9_default_output_dir_encodes_parameters() {
    let mut cmd = Command::new(realdb_e2e_binary());
    cmd.args([
        "verify-suite",
        "--mode",
        "fsqlite_mvcc",
        "--placement-profile",
        "adversarial_cross_node",
        "--verification-depth",
        "full",
        "--activation-regime",
        "many_core_parallel",
        "--shadow-mode",
        "sampled",
        "--shadow-verdict",
        "pending_execution",
        "--pretty",
    ]);
    let out = cmd.output().expect("run");
    assert!(out.status.success());
    let pkg: serde_json::Value = serde_json::from_slice(&out.stdout).expect("parse stdout JSON");
    let artifact_root = pkg["artifact_root"].as_str().unwrap();
    assert!(
        artifact_root.contains("fsqlite_mvcc"),
        "artifact_root must encode mode: {artifact_root}"
    );
    assert!(
        artifact_root.contains("adversarial_cross_node"),
        "artifact_root must encode placement profile: {artifact_root}"
    );
    assert!(
        artifact_root.contains("full"),
        "artifact_root must encode depth: {artifact_root}"
    );
    assert!(
        artifact_root.contains("many_core_parallel"),
        "artifact_root must encode regime: {artifact_root}"
    );
    assert!(
        artifact_root.contains("sampled"),
        "artifact_root must encode shadow mode: {artifact_root}"
    );
}

// ── P10: summary markdown contains actionable information ──────────────

#[test]
fn p10_summary_md_contains_key_fields() {
    let dir = fresh_output_dir("p10_summary");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--verification-depth",
            "full",
            "--shadow-mode",
            "off",
        ],
        &dir,
    );
    assert!(out.status.success());
    let summary = fs::read_to_string(dir.join("suite_summary.md")).expect("read suite_summary.md");
    let required_fragments = [
        "suite_id:",
        "mode:",
        "verification_depth:",
        "retention_class:",
        "pass_fail_signature:",
        "rerun_entrypoint:",
        "artifact_root:",
    ];
    for frag in &required_fragments {
        assert!(
            summary.contains(frag),
            "suite_summary.md must contain '{frag}'"
        );
    }
}

// ── P11: inline bundle emission ────────────────────────────────────────

#[test]
fn p11_emit_inline_bundle_on_stderr() {
    let dir = fresh_output_dir("p11_inline");
    let mut cmd = Command::new(realdb_e2e_binary());
    cmd.args([
        "verify-suite",
        "--mode",
        "fsqlite_mvcc",
        "--shadow-mode",
        "off",
        "--emit-inline-bundle",
        "--output-dir",
    ]);
    cmd.arg(&dir);
    let out = cmd.output().expect("run");
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("VERIFY_SUITE_BUNDLE_JSON="),
        "stderr must contain inline bundle prefix"
    );
    let bundle_line = stderr
        .lines()
        .find(|l| l.starts_with("VERIFY_SUITE_BUNDLE_JSON="))
        .expect("inline bundle line");
    let json_str = &bundle_line["VERIFY_SUITE_BUNDLE_JSON=".len()..];
    let parsed: serde_json::Value =
        serde_json::from_str(json_str).expect("inline bundle must be valid JSON");
    assert!(parsed["suite_id"].is_string());
}

// ── P12: activation regime enumeration ─────────────────────────────────

#[test]
fn p12_all_five_activation_regimes_accepted() {
    let regimes = [
        "red_path_correctness",
        "low_concurrency_fixed_cost",
        "mid_concurrency_scaling",
        "many_core_parallel",
        "hostile_or_unclassified",
    ];
    for (i, regime) in regimes.iter().enumerate() {
        let dir = fresh_output_dir(&format!("p12_regime_{i}"));
        let out = run_verify_suite(
            &[
                "--mode",
                "fsqlite_mvcc",
                "--activation-regime",
                regime,
                "--shadow-mode",
                "off",
            ],
            &dir,
        );
        assert!(
            out.status.success(),
            "regime {regime} must be accepted: {}",
            out.status
        );
        let pkg = load_package_json(&dir);
        assert_eq!(pkg["activation_regime"], *regime);
    }
}

// ── P13: package JSON ↔ stdout round-trip ──────────────────────────────

#[test]
fn p13_stdout_json_matches_file_package() {
    let dir = fresh_output_dir("p13_roundtrip");
    let out = run_verify_suite(
        &[
            "--mode",
            "fsqlite_mvcc",
            "--verification-depth",
            "quick",
            "--shadow-mode",
            "off",
            "--pretty",
        ],
        &dir,
    );
    assert!(out.status.success());
    let stdout_json: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("parse stdout JSON");
    let file_json = load_package_json(&dir);
    assert_eq!(stdout_json, file_json, "stdout must match file package");
}
