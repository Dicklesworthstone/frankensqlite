//! Oracle preflight doctor classification and remediation contract tests (bd-2yqp6.2.5).

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use fsqlite_harness::differential_v2::TARGET_SQLITE_VERSION;
use fsqlite_harness::oracle_preflight_doctor::{
    DoctorConfig, DoctorOutcome, RemediationClass, run_oracle_preflight_doctor,
};
use tempfile::TempDir;

const BEAD_ID: &str = "bd-2yqp6.2.5";

#[cfg(unix)]
fn make_fake_sqlite_binary(dir: &Path, version: &str) -> PathBuf {
    let script_path = dir.join("fake_sqlite3.sh");
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nif [[ \"${{1:-}}\" == \"--version\" ]]; then\n  echo \"{}\"\n  exit 0\nfi\necho \"unsupported\" >&2\nexit 2\n",
        version
    );
    fs::write(&script_path, script).expect("write fake sqlite script");
    let mut permissions = fs::metadata(&script_path)
        .expect("fake sqlite metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script_path, permissions).expect("set fake sqlite permissions");
    script_path
}

fn write_fixture(workspace_root: &Path) -> PathBuf {
    let fixture_dir = workspace_root.join("crates/fsqlite-harness/conformance");
    fs::create_dir_all(&fixture_dir).expect("create fixture dir");
    let fixture_path = fixture_dir.join("001_basic_doctor_fixture.json");
    let fixture_json = r#"{
  "id": "001_basic_doctor_fixture",
  "description": "Doctor fixture for deterministic ingest checks",
  "ops": [
    { "op": "open", "path": ":memory:" },
    { "op": "exec", "sql": "CREATE TABLE t(a INTEGER, b TEXT)" },
    { "op": "exec", "sql": "INSERT INTO t VALUES(1, 'x')" },
    { "op": "query", "sql": "SELECT a, b FROM t ORDER BY a", "expect": { "columns": ["a", "b"], "rows": [["1", "x"]], "ordered": true } }
  ],
  "fsqlite_modes": ["compatibility"]
}"#;
    fs::write(&fixture_path, fixture_json).expect("write fixture");
    fixture_path
}

fn write_manifest(workspace_root: &Path) -> PathBuf {
    let manifest_path = workspace_root.join("corpus_manifest.toml");
    fs::write(
        &manifest_path,
        r#"[meta]
schema_version = "1.0.0"
bead_id = "bd-2yqp6.2.5"
generated_at = "2026-02-27T00:00:00Z"
"#,
    )
    .expect("write manifest");
    manifest_path
}

#[cfg(unix)]
fn base_config(workspace_root: &Path, sqlite_binary: PathBuf) -> DoctorConfig {
    let mut config = DoctorConfig::new(workspace_root.to_path_buf());
    config.fixtures_dir = workspace_root.join("crates/fsqlite-harness/conformance");
    config.fixture_manifest_path = workspace_root.join("corpus_manifest.toml");
    config.oracle_binary_override = Some(sqlite_binary);
    config.min_fixture_json_files = 1;
    config.min_fixture_entries = 1;
    config.min_fixture_sql_statements = 2;
    TARGET_SQLITE_VERSION.clone_into(&mut config.expected_sqlite_version_prefix);
    config.run_id = format!("{BEAD_ID}-test-run");
    "trace-bd-2yqp6.2.5-test".clone_into(&mut config.trace_id);
    "DIFF-ORACLE-PREFLIGHT-B5-TEST".clone_into(&mut config.scenario_id);
    config.seed = 4_242;
    config.generated_unix_ms = 1_700_000_000_000;
    config
}

#[cfg(unix)]
#[test]
fn doctor_reports_green_for_ready_configuration() {
    let workspace = TempDir::new().expect("temp workspace");
    write_fixture(workspace.path());
    write_manifest(workspace.path());
    let sqlite_binary = make_fake_sqlite_binary(workspace.path(), "3.52.0-test");
    let config = base_config(workspace.path(), sqlite_binary);

    let report = run_oracle_preflight_doctor(&config);
    assert_eq!(report.outcome, DoctorOutcome::Green);
    assert!(report.certifying);
    assert!(
        report.findings.is_empty(),
        "expected no findings in healthy config, got {:?}",
        report.findings
    );
}

#[cfg(unix)]
#[test]
fn doctor_classifies_missing_binary_as_red_with_remediation_command() {
    let workspace = TempDir::new().expect("temp workspace");
    write_fixture(workspace.path());
    write_manifest(workspace.path());

    let missing_binary = workspace.path().join("sqlite3-missing");
    let config = base_config(workspace.path(), missing_binary);
    let report = run_oracle_preflight_doctor(&config);

    assert_eq!(report.outcome, DoctorOutcome::Red);
    let finding = report
        .findings
        .iter()
        .find(|finding| finding.remediation_class == RemediationClass::MissingBinary)
        .expect("missing-binary finding");
    assert!(
        finding.fix_command.contains("sqlite3"),
        "expected sqlite remediation command, got {}",
        finding.fix_command
    );
}

#[cfg(unix)]
#[test]
fn doctor_classifies_self_compare_risk_as_red() {
    let workspace = TempDir::new().expect("temp workspace");
    write_fixture(workspace.path());
    write_manifest(workspace.path());
    let sqlite_binary = make_fake_sqlite_binary(workspace.path(), "3.52.0-test");
    let mut config = base_config(workspace.path(), sqlite_binary);
    config.expected_reference_identity = "frankensqlite".to_owned();

    let report = run_oracle_preflight_doctor(&config);
    assert_eq!(report.outcome, DoctorOutcome::Red);
    let finding = report
        .findings
        .iter()
        .find(|finding| finding.remediation_class == RemediationClass::SelfCompareRisk)
        .expect("self-compare finding");
    assert!(
        finding
            .fix_command
            .contains("--expected-reference-identity csqlite-oracle"),
        "expected wiring remediation command, got {}",
        finding.fix_command
    );
}

#[cfg(unix)]
#[test]
fn doctor_classifies_stale_manifest_as_yellow_non_certifying() {
    let workspace = TempDir::new().expect("temp workspace");
    write_manifest(workspace.path());
    thread::sleep(Duration::from_millis(1_100));
    write_fixture(workspace.path());
    let sqlite_binary = make_fake_sqlite_binary(workspace.path(), "3.52.0-test");
    let config = base_config(workspace.path(), sqlite_binary);

    let report = run_oracle_preflight_doctor(&config);
    assert_eq!(report.outcome, DoctorOutcome::Yellow);
    assert!(!report.certifying);
    let finding = report
        .findings
        .iter()
        .find(|finding| finding.remediation_class == RemediationClass::StaleManifest)
        .expect("stale-manifest finding");
    assert!(
        finding.fix_command.contains("differential_manifest_runner"),
        "expected stale manifest remediation command, got {}",
        finding.fix_command
    );
}

#[cfg(not(unix))]
#[test]
fn oracle_preflight_doctor_tests_are_unix_only() {
    // The fake sqlite3 script helper uses unix executable permissions.
}
