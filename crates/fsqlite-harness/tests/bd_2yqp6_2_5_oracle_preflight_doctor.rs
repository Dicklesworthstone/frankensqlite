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
use sha2::{Digest, Sha256};
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    fsqlite_harness::bytes_to_lower_hex(hasher.finalize())
}

fn fixture_tree_hash(fixtures_dir: &Path) -> String {
    let fixture_path = fixtures_dir.join("001_basic_doctor_fixture.json");
    let bytes = fs::read(&fixture_path).expect("read doctor fixture for tree hash");
    let file_hash = sha256_hex(&bytes);
    let payload = format!(
        "001_basic_doctor_fixture.json\t{}\t{file_hash}\n",
        bytes.len()
    );
    sha256_hex(payload.as_bytes())
}

fn write_manifest(workspace_root: &Path) -> PathBuf {
    let manifest_path = workspace_root.join("corpus_manifest.toml");
    let fixtures_dir = workspace_root.join("crates/fsqlite-harness/conformance");
    let fixture_content_hash = fixture_tree_hash(&fixtures_dir);
    fs::write(
        &manifest_path,
        format!(
            r#"[meta]
schema_version = "1.0.0"
bead_id = "bd-2yqp6.2.5"
generated_at = "2026-02-27T00:00:00Z"

[fixture_roots]
schema_version = "1.0.0"
fixtures_dir = "crates/fsqlite-harness/conformance"
fixtures_dir_aliases = ["./crates/fsqlite-harness/conformance"]
slt_dir = "conformance/slt"
slt_dir_aliases = ["./conformance/slt"]
min_fixture_json_files = 1
min_fixture_entries = 1
min_fixture_sql_statements = 2
min_slt_files = 1
min_slt_entries = 1
min_slt_sql_statements = 1
required_category_families = [
  "ddl",
  "dml",
  "joins",
  "windows",
  "ctes",
  "triggers",
  "views",
  "functions",
  "pragma",
  "error_paths",
]

[[fixture_roots.hash_locked_roots]]
root_id = "doctor-fixtures"
path = "crates/fsqlite-harness/conformance"
kind = "fixture_json"
hash_algorithm = "sha256-tree-v1"
include_extensions = ["json"]
content_hash = "{fixture_content_hash}"
min_files = 1

[[category_floors]]
category = "ddl"
min_entries = 1

[[category_floors]]
category = "dml"
min_entries = 1

[[category_floors]]
category = "joins"
min_entries = 1

[[category_floors]]
category = "windows"
min_entries = 1

[[category_floors]]
category = "ctes"
min_entries = 1

[[category_floors]]
category = "triggers"
min_entries = 1

[[category_floors]]
category = "views"
min_entries = 1

[[category_floors]]
category = "functions"
min_entries = 1

[[category_floors]]
category = "pragma"
min_entries = 1

[[category_floors]]
category = "error_paths"
min_entries = 1
"#
        ),
    )
    .expect("write manifest");
    manifest_path
}

#[cfg(unix)]
fn base_config(workspace_root: &Path, sqlite_binary: PathBuf) -> DoctorConfig {
    let mut config = DoctorConfig::new(workspace_root.to_path_buf());
    config.fixture_manifest_path = workspace_root.join("corpus_manifest.toml");
    config.min_fixture_json_files = 1;
    config.min_fixture_entries = 1;
    config.min_fixture_sql_statements = 2;
    config.oracle_binary_override = Some(sqlite_binary);
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
    assert!(report.replay_command.contains("--fixture-manifest-path"));
    assert!(report.replay_command.contains("--fixtures-dir"));
    assert!(report.replay_command.contains("--min-fixture-json-files 1"));
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
    write_fixture(workspace.path());
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
