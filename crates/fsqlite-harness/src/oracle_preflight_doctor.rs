//! Oracle preflight doctor for parity-cert readiness (bd-2yqp6.2.5).
//!
//! Produces deterministic readiness diagnostics before differential/parity
//! lanes run, with actionable remediation commands and red/yellow/green
//! severity semantics.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::corpus_ingest::{CorpusBuilder, ingest_conformance_fixtures_with_report};
use crate::differential_v2::TARGET_SQLITE_VERSION;
use crate::oracle::{find_sqlite3_binary, verify_oracle_version};

/// Owning bead identifier.
pub const BEAD_ID: &str = "bd-2yqp6.2.5";
/// Schema version for machine-readable doctor reports.
pub const DOCTOR_SCHEMA_VERSION: &str = "1.0.0";
/// Default scenario identifier.
pub const DEFAULT_SCENARIO_ID: &str = "DIFF-ORACLE-PREFLIGHT-B5";
/// Default fixtures directory relative to workspace root.
pub const DEFAULT_FIXTURES_DIR: &str = "crates/fsqlite-harness/conformance";
/// Default fixture-manifest path relative to workspace root.
pub const DEFAULT_FIXTURE_MANIFEST_PATH: &str = "corpus_manifest.toml";
/// Default fixture count sanity floor.
pub const DEFAULT_MIN_FIXTURE_JSON_FILES: usize = 8;
/// Default fixture entry sanity floor.
pub const DEFAULT_MIN_FIXTURE_ENTRIES: usize = 8;
/// Default SQL statement sanity floor.
pub const DEFAULT_MIN_FIXTURE_SQL_STATEMENTS: usize = 40;
/// Canonical parity subject identity label.
pub const DEFAULT_EXPECTED_SUBJECT_IDENTITY: &str = "frankensqlite";
/// Canonical oracle reference identity label.
pub const DEFAULT_EXPECTED_REFERENCE_IDENTITY: &str = "csqlite-oracle";

/// Severity for doctor findings and aggregate readiness.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DoctorOutcome {
    /// Ready for certifying parity runs.
    Green,
    /// Non-certifying warning; execution may proceed with explicit caveats.
    Yellow,
    /// Blocking failure; parity pipeline must hard-fail.
    Red,
}

impl DoctorOutcome {
    const fn rank(self) -> u8 {
        match self {
            Self::Green => 0,
            Self::Yellow => 1,
            Self::Red => 2,
        }
    }
}

impl std::fmt::Display for DoctorOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Green => formatter.write_str("green"),
            Self::Yellow => formatter.write_str("yellow"),
            Self::Red => formatter.write_str("red"),
        }
    }
}

/// Stable remediation classes for deterministic triage/reporting.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemediationClass {
    /// C SQLite binary cannot be located or invoked.
    MissingBinary,
    /// Located oracle version does not satisfy expected target.
    VersionDrift,
    /// Differential wiring risks accidental self-compare.
    SelfCompareRisk,
    /// Invalid doctor or harness configuration.
    InvalidConfig,
    /// Fixture manifest appears stale relative to fixture corpus.
    StaleManifest,
}

/// Single deterministic doctor finding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorFinding {
    /// Severity of this finding.
    pub outcome: DoctorOutcome,
    /// Stable remediation classification.
    pub remediation_class: RemediationClass,
    /// Short deterministic summary.
    pub summary: String,
    /// Additional context for diagnosis.
    pub details: String,
    /// One-command remediation suggestion.
    pub fix_command: String,
}

/// First failure details for fast diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FirstFailureDiagnosis {
    /// Stable remediation classification.
    pub remediation_class: RemediationClass,
    /// Short deterministic summary.
    pub summary: String,
    /// One-command remediation suggestion.
    pub fix_command: String,
}

/// Check-level telemetry emitted in every report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OraclePreflightChecks {
    /// Expected subject identity.
    pub expected_subject_identity: String,
    /// Expected reference identity.
    pub expected_reference_identity: String,
    /// Expected oracle version prefix (target contract).
    pub expected_sqlite_version_prefix: String,
    /// Effective fixture directory.
    pub fixtures_dir: String,
    /// Effective fixture manifest path.
    pub fixture_manifest_path: String,
    /// Resolved oracle binary path if discovered.
    pub oracle_binary_path: Option<String>,
    /// Resolved oracle version string if discovered.
    pub oracle_version: Option<String>,
    /// Fixture ingestion counters.
    pub fixture_json_files_seen: usize,
    /// Fixture ingestion counters.
    pub fixture_entries_ingested: usize,
    /// Fixture ingestion counters.
    pub fixture_sql_statements_ingested: usize,
    /// Number of skipped fixture files during ingestion.
    pub skipped_fixture_files: usize,
    /// Manifest mtime in unix ms when available.
    pub fixture_manifest_mtime_unix_ms: Option<u128>,
    /// Latest fixture mtime in unix ms when available.
    pub latest_fixture_mtime_unix_ms: Option<u128>,
}

/// Deterministic machine-readable doctor report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OraclePreflightReport {
    /// Report schema version.
    pub schema_version: String,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Correlation run identifier.
    pub run_id: String,
    /// Correlation trace identifier.
    pub trace_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Deterministic seed.
    pub seed: u64,
    /// Deterministic generation timestamp.
    pub generated_unix_ms: u128,
    /// Aggregate outcome for this doctor run.
    pub outcome: DoctorOutcome,
    /// Whether this run is certifying (`green` only).
    pub certifying: bool,
    /// Runtime in milliseconds for diagnosis and regression tracking.
    pub timing_ms: u64,
    /// Stable first failure for user-facing explainers.
    pub first_failure: Option<FirstFailureDiagnosis>,
    /// Deterministic findings.
    pub findings: Vec<DoctorFinding>,
    /// Structured check telemetry.
    pub checks: OraclePreflightChecks,
    /// Deterministic local replay command.
    pub replay_command: String,
}

/// Input contract for doctor execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorConfig {
    /// Workspace root used to resolve default paths.
    pub workspace_root: PathBuf,
    /// Fixture directory for ingest sanity checks.
    pub fixtures_dir: PathBuf,
    /// Fixture manifest path for freshness checks.
    pub fixture_manifest_path: PathBuf,
    /// Minimum fixture JSON files required.
    pub min_fixture_json_files: usize,
    /// Minimum fixture entries required.
    pub min_fixture_entries: usize,
    /// Minimum fixture SQL statements required.
    pub min_fixture_sql_statements: usize,
    /// Expected oracle version prefix (for drift detection).
    pub expected_sqlite_version_prefix: String,
    /// Expected parity subject identity label.
    pub expected_subject_identity: String,
    /// Expected parity reference identity label.
    pub expected_reference_identity: String,
    /// Optional forced sqlite3 binary path for deterministic tests/CI.
    pub oracle_binary_override: Option<PathBuf>,
    /// Correlation run identifier.
    pub run_id: String,
    /// Correlation trace identifier.
    pub trace_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Deterministic seed.
    pub seed: u64,
    /// Deterministic generation timestamp.
    pub generated_unix_ms: u128,
}

impl DoctorConfig {
    /// Create a config with deterministic defaults rooted at `workspace_root`.
    #[must_use]
    pub fn new(workspace_root: PathBuf) -> Self {
        let generated_unix_ms = now_unix_ms();
        let run_id = format!("{BEAD_ID}-doctor-{generated_unix_ms}");
        let trace_id = build_trace_id(&run_id);
        let fixtures_dir = workspace_root.join(DEFAULT_FIXTURES_DIR);
        let fixture_manifest_path = workspace_root.join(DEFAULT_FIXTURE_MANIFEST_PATH);
        Self {
            workspace_root,
            fixtures_dir,
            fixture_manifest_path,
            min_fixture_json_files: DEFAULT_MIN_FIXTURE_JSON_FILES,
            min_fixture_entries: DEFAULT_MIN_FIXTURE_ENTRIES,
            min_fixture_sql_statements: DEFAULT_MIN_FIXTURE_SQL_STATEMENTS,
            expected_sqlite_version_prefix: TARGET_SQLITE_VERSION.to_owned(),
            expected_subject_identity: DEFAULT_EXPECTED_SUBJECT_IDENTITY.to_owned(),
            expected_reference_identity: DEFAULT_EXPECTED_REFERENCE_IDENTITY.to_owned(),
            oracle_binary_override: None,
            run_id,
            trace_id,
            scenario_id: DEFAULT_SCENARIO_ID.to_owned(),
            seed: 424_242,
            generated_unix_ms,
        }
    }

    /// Render deterministic replay command for this configuration.
    #[must_use]
    pub fn replay_command(&self) -> String {
        let mut replay = format!(
            "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --workspace-root {} --fixtures-dir {} --fixture-manifest-path {} --run-id {} --trace-id {} --scenario-id {} --seed {} --generated-unix-ms {} --min-fixture-json-files {} --min-fixture-entries {} --min-fixture-sql-statements {} --expected-sqlite-version-prefix {} --expected-subject-identity {} --expected-reference-identity {}",
            self.workspace_root.display(),
            self.fixtures_dir.display(),
            self.fixture_manifest_path.display(),
            self.run_id,
            self.trace_id,
            self.scenario_id,
            self.seed,
            self.generated_unix_ms,
            self.min_fixture_json_files,
            self.min_fixture_entries,
            self.min_fixture_sql_statements,
            self.expected_sqlite_version_prefix,
            self.expected_subject_identity,
            self.expected_reference_identity,
        );
        if let Some(path) = &self.oracle_binary_override {
            let _ = write!(replay, " --oracle-binary {}", path.display());
        }
        replay
    }
}

impl Default for DoctorConfig {
    fn default() -> Self {
        Self::new(default_workspace_root())
    }
}

/// Execute the oracle preflight doctor and return a deterministic report.
#[must_use]
pub fn run_oracle_preflight_doctor(config: &DoctorConfig) -> OraclePreflightReport {
    let started_at = Instant::now();
    let mut findings = Vec::new();
    let mut checks = OraclePreflightChecks {
        expected_subject_identity: config.expected_subject_identity.clone(),
        expected_reference_identity: config.expected_reference_identity.clone(),
        expected_sqlite_version_prefix: config.expected_sqlite_version_prefix.clone(),
        fixtures_dir: config.fixtures_dir.display().to_string(),
        fixture_manifest_path: config.fixture_manifest_path.display().to_string(),
        oracle_binary_path: None,
        oracle_version: None,
        fixture_json_files_seen: 0,
        fixture_entries_ingested: 0,
        fixture_sql_statements_ingested: 0,
        skipped_fixture_files: 0,
        fixture_manifest_mtime_unix_ms: None,
        latest_fixture_mtime_unix_ms: None,
    };

    validate_config(config, &mut findings);
    let sqlite3_path = resolve_oracle_binary(config, &mut findings);
    if let Some(path) = sqlite3_path.as_ref() {
        checks.oracle_binary_path = Some(path.display().to_string());
        check_oracle_version(path, config, &mut findings, &mut checks);
    }
    check_identity_wiring(config, &mut findings);
    check_fixture_ingest(config, &mut findings, &mut checks);
    check_manifest_freshness(config, &mut findings, &mut checks);

    let outcome = aggregate_outcome(&findings);
    let first_failure = findings
        .iter()
        .find(|finding| finding.outcome != DoctorOutcome::Green)
        .map(|finding| FirstFailureDiagnosis {
            remediation_class: finding.remediation_class,
            summary: finding.summary.clone(),
            fix_command: finding.fix_command.clone(),
        });

    OraclePreflightReport {
        schema_version: DOCTOR_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        run_id: config.run_id.clone(),
        trace_id: config.trace_id.clone(),
        scenario_id: config.scenario_id.clone(),
        seed: config.seed,
        generated_unix_ms: config.generated_unix_ms,
        outcome,
        certifying: outcome == DoctorOutcome::Green,
        timing_ms: elapsed_millis(started_at.elapsed()),
        first_failure,
        findings,
        checks,
        replay_command: config.replay_command(),
    }
}

fn default_workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn build_trace_id(run_id: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(run_id.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    format!("trace-{}", &hex[..16])
}

fn elapsed_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn aggregate_outcome(findings: &[DoctorFinding]) -> DoctorOutcome {
    findings
        .iter()
        .map(|finding| finding.outcome)
        .max_by_key(|outcome| outcome.rank())
        .unwrap_or(DoctorOutcome::Green)
}

fn validate_config(config: &DoctorConfig, findings: &mut Vec<DoctorFinding>) {
    if config.scenario_id.trim().is_empty() {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::InvalidConfig,
            summary: "invalid scenario_id configuration".to_owned(),
            details: "--scenario-id must be non-empty".to_owned(),
            fix_command:
                "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --scenario-id DIFF-ORACLE-PREFLIGHT-B5"
                    .to_owned(),
        });
    }
    if config.min_fixture_json_files == 0
        || config.min_fixture_entries == 0
        || config.min_fixture_sql_statements == 0
    {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::InvalidConfig,
            summary: "invalid fixture sanity thresholds".to_owned(),
            details: "all --min-fixture-* thresholds must be > 0".to_owned(),
            fix_command: "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --min-fixture-json-files 8 --min-fixture-entries 8 --min-fixture-sql-statements 40".to_owned(),
        });
    }
    if config.expected_sqlite_version_prefix.trim().is_empty() {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::InvalidConfig,
            summary: "empty expected sqlite version prefix".to_owned(),
            details: "--expected-sqlite-version-prefix must be non-empty".to_owned(),
            fix_command: format!(
                "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --expected-sqlite-version-prefix {TARGET_SQLITE_VERSION}"
            ),
        });
    }
}

fn resolve_oracle_binary(
    config: &DoctorConfig,
    findings: &mut Vec<DoctorFinding>,
) -> Option<PathBuf> {
    if let Some(path) = &config.oracle_binary_override {
        if path.is_file() {
            return Some(path.clone());
        }
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::MissingBinary,
            summary: "sqlite3 oracle binary override path is missing".to_owned(),
            details: format!("--oracle-binary path does not exist: {}", path.display()),
            fix_command: "sudo apt-get update && sudo apt-get install -y sqlite3".to_owned(),
        });
        return None;
    }

    match find_sqlite3_binary() {
        Ok(path) => Some(path),
        Err(error) => {
            findings.push(DoctorFinding {
                outcome: DoctorOutcome::Yellow,
                remediation_class: RemediationClass::MissingBinary,
                summary: "sqlite3 oracle binary not found".to_owned(),
                details: error.to_string(),
                fix_command: "sudo apt-get update && sudo apt-get install -y sqlite3".to_owned(),
            });
            None
        }
    }
}

fn check_oracle_version(
    sqlite3_path: &Path,
    config: &DoctorConfig,
    findings: &mut Vec<DoctorFinding>,
    checks: &mut OraclePreflightChecks,
) {
    match verify_oracle_version(sqlite3_path) {
        Ok(version) => {
            checks.oracle_version = Some(version.clone());
            if !version.starts_with(&config.expected_sqlite_version_prefix) {
                findings.push(DoctorFinding {
                    outcome: DoctorOutcome::Yellow,
                    remediation_class: RemediationClass::VersionDrift,
                    summary: "oracle version drift detected".to_owned(),
                    details: format!(
                        "expected sqlite3 --version prefix '{}' but observed '{}'",
                        config.expected_sqlite_version_prefix, version
                    ),
                    fix_command: "sqlite3 --version && cargo test -p fsqlite-harness --test bd_2yqp6_1_3_sqlite_version_contract -- --nocapture".to_owned(),
                });
            }
        }
        Err(error) => {
            findings.push(DoctorFinding {
                outcome: DoctorOutcome::Yellow,
                remediation_class: RemediationClass::VersionDrift,
                summary: "failed to verify sqlite3 oracle version".to_owned(),
                details: error.to_string(),
                fix_command: "sqlite3 --version".to_owned(),
            });
        }
    }
}

fn check_identity_wiring(config: &DoctorConfig, findings: &mut Vec<DoctorFinding>) {
    let subject = config.expected_subject_identity.trim();
    let reference = config.expected_reference_identity.trim();

    if subject.is_empty() || reference.is_empty() {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::InvalidConfig,
            summary: "identity configuration is empty".to_owned(),
            details: "expected subject/reference identity labels must both be non-empty".to_owned(),
            fix_command:
                "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --expected-subject-identity frankensqlite --expected-reference-identity csqlite-oracle"
                    .to_owned(),
        });
        return;
    }

    let subject_lc = subject.to_ascii_lowercase();
    let reference_lc = reference.to_ascii_lowercase();
    let canonical_subject = DEFAULT_EXPECTED_SUBJECT_IDENTITY;
    let canonical_reference = DEFAULT_EXPECTED_REFERENCE_IDENTITY;
    let looks_like_self_compare = subject_lc == reference_lc
        || reference_lc.contains("franken")
        || reference_lc != canonical_reference
        || subject_lc != canonical_subject;

    if looks_like_self_compare {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::SelfCompareRisk,
            summary: "differential wiring risks self-compare or non-oracle reference".to_owned(),
            details: format!(
                "expected subject='{}', reference='{}'; observed subject='{}', reference='{}'",
                canonical_subject, canonical_reference, subject, reference
            ),
            fix_command:
                "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --expected-subject-identity frankensqlite --expected-reference-identity csqlite-oracle"
                    .to_owned(),
        });
    }
}

fn check_fixture_ingest(
    config: &DoctorConfig,
    findings: &mut Vec<DoctorFinding>,
    checks: &mut OraclePreflightChecks,
) {
    if !config.fixtures_dir.is_dir() {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Red,
            remediation_class: RemediationClass::InvalidConfig,
            summary: "fixtures directory missing".to_owned(),
            details: format!(
                "fixtures directory does not exist: {}",
                config.fixtures_dir.display()
            ),
            fix_command: format!(
                "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --fixtures-dir {}",
                config.workspace_root.join(DEFAULT_FIXTURES_DIR).display()
            ),
        });
        return;
    }

    let mut builder = CorpusBuilder::new(config.seed);
    match ingest_conformance_fixtures_with_report(&config.fixtures_dir, &mut builder) {
        Ok(report) => {
            checks.fixture_json_files_seen = report.fixture_json_files_seen;
            checks.fixture_entries_ingested = report.fixture_entries_ingested;
            checks.fixture_sql_statements_ingested = report.sql_statements_ingested;
            checks.skipped_fixture_files = report.skipped_files.len();
            validate_fixture_thresholds(config, &report, findings);

            if !report.skipped_files.is_empty() {
                let skipped = summarize_skipped(&report);
                findings.push(DoctorFinding {
                    outcome: DoctorOutcome::Yellow,
                    remediation_class: RemediationClass::StaleManifest,
                    summary: "fixture ingest skipped one or more fixture files".to_owned(),
                    details: format!("skipped examples: {skipped}"),
                    fix_command: format!(
                        "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --fixtures-dir {}",
                        config.workspace_root.display(),
                        config.fixtures_dir.display()
                    ),
                });
            }
        }
        Err(error) => {
            findings.push(DoctorFinding {
                outcome: DoctorOutcome::Red,
                remediation_class: RemediationClass::InvalidConfig,
                summary: "fixture ingestion failed".to_owned(),
                details: error,
                fix_command: format!(
                    "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --fixtures-dir {}",
                    config.workspace_root.join(DEFAULT_FIXTURES_DIR).display()
                ),
            });
        }
    }
}

fn validate_fixture_thresholds(
    config: &DoctorConfig,
    report: &crate::corpus_ingest::FixtureIngestReport,
    findings: &mut Vec<DoctorFinding>,
) {
    let mut violations = Vec::new();
    if report.fixture_json_files_seen < config.min_fixture_json_files {
        violations.push(format!(
            "fixture_json_files_seen={} < min_fixture_json_files={}",
            report.fixture_json_files_seen, config.min_fixture_json_files
        ));
    }
    if report.fixture_entries_ingested < config.min_fixture_entries {
        violations.push(format!(
            "fixture_entries_ingested={} < min_fixture_entries={}",
            report.fixture_entries_ingested, config.min_fixture_entries
        ));
    }
    if report.sql_statements_ingested < config.min_fixture_sql_statements {
        violations.push(format!(
            "fixture_sql_statements_ingested={} < min_fixture_sql_statements={}",
            report.sql_statements_ingested, config.min_fixture_sql_statements
        ));
    }
    if violations.is_empty() {
        return;
    }
    findings.push(DoctorFinding {
        outcome: DoctorOutcome::Red,
        remediation_class: RemediationClass::InvalidConfig,
        summary: "fixture sanity thresholds failed".to_owned(),
        details: violations.join("; "),
        fix_command: format!(
            "cargo run -p fsqlite-harness --bin oracle_preflight_doctor_runner -- --fixtures-dir {} --min-fixture-json-files {} --min-fixture-entries {} --min-fixture-sql-statements {}",
            config.fixtures_dir.display(),
            config.min_fixture_json_files,
            config.min_fixture_entries,
            config.min_fixture_sql_statements,
        ),
    });
}

fn summarize_skipped(report: &crate::corpus_ingest::FixtureIngestReport) -> String {
    let mut skipped = report
        .skipped_files
        .iter()
        .map(|detail| format!("{} ({})", detail.file, detail.reason))
        .collect::<Vec<_>>();
    skipped.sort();
    skipped.truncate(5);
    skipped.join(", ")
}

fn check_manifest_freshness(
    config: &DoctorConfig,
    findings: &mut Vec<DoctorFinding>,
    checks: &mut OraclePreflightChecks,
) {
    let manifest_path = &config.fixture_manifest_path;
    if !manifest_path.is_file() {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Yellow,
            remediation_class: RemediationClass::StaleManifest,
            summary: "fixture manifest file is missing".to_owned(),
            details: format!("missing fixture manifest: {}", manifest_path.display()),
            fix_command: format!(
                "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --fixtures-dir {}",
                config.workspace_root.display(),
                config.fixtures_dir.display()
            ),
        });
        return;
    }

    match fs::read_to_string(manifest_path) {
        Ok(content) => {
            if content.trim().is_empty() {
                findings.push(DoctorFinding {
                    outcome: DoctorOutcome::Red,
                    remediation_class: RemediationClass::InvalidConfig,
                    summary: "fixture manifest is empty".to_owned(),
                    details: format!("manifest file has zero semantic content: {}", manifest_path.display()),
                    fix_command: format!(
                        "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --fixtures-dir {}",
                        config.workspace_root.display(),
                        config.fixtures_dir.display()
                    ),
                });
            }
        }
        Err(error) => {
            findings.push(DoctorFinding {
                outcome: DoctorOutcome::Red,
                remediation_class: RemediationClass::InvalidConfig,
                summary: "failed to read fixture manifest".to_owned(),
                details: format!("manifest read failed: {error}"),
                fix_command: format!("ls -l {}", manifest_path.display()),
            });
        }
    }

    checks.fixture_manifest_mtime_unix_ms = manifest_mtime_unix_ms(manifest_path);
    checks.latest_fixture_mtime_unix_ms = latest_fixture_mtime_unix_ms(&config.fixtures_dir);

    if let (Some(manifest_mtime), Some(latest_fixture_mtime)) = (
        checks.fixture_manifest_mtime_unix_ms,
        checks.latest_fixture_mtime_unix_ms,
    ) && manifest_mtime < latest_fixture_mtime
    {
        findings.push(DoctorFinding {
            outcome: DoctorOutcome::Yellow,
            remediation_class: RemediationClass::StaleManifest,
            summary: "fixture manifest appears stale relative to fixture corpus".to_owned(),
            details: format!(
                "fixture_manifest_mtime_unix_ms={} < latest_fixture_mtime_unix_ms={}",
                manifest_mtime, latest_fixture_mtime
            ),
            fix_command: format!(
                "cargo run -p fsqlite-harness --bin differential_manifest_runner -- --workspace-root {} --fixtures-dir {}",
                config.workspace_root.display(),
                config.fixtures_dir.display()
            ),
        });
    }
}

fn manifest_mtime_unix_ms(path: &Path) -> Option<u128> {
    let metadata = fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    system_time_to_unix_ms(modified)
}

fn latest_fixture_mtime_unix_ms(dir: &Path) -> Option<u128> {
    let entries = fs::read_dir(dir).ok()?;
    let mut latest: Option<u128> = None;
    for entry_result in entries {
        let entry = entry_result.ok()?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(extension) = path.extension() else {
            continue;
        };
        if extension != "json" {
            continue;
        }
        let Some(modified) = entry.metadata().ok().and_then(|meta| meta.modified().ok()) else {
            continue;
        };
        let Some(modified_ms) = system_time_to_unix_ms(modified) else {
            continue;
        };
        latest = Some(latest.map_or(modified_ms, |current| current.max(modified_ms)));
    }
    latest
}

fn system_time_to_unix_ms(time: SystemTime) -> Option<u128> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis())
}
