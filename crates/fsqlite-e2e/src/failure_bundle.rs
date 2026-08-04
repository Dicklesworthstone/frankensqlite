//! Unified failure artifact bundle schema and collector.
//!
//! Bead: bd-mblr.4.4.1
//!
//! This module defines a standardized schema for capturing all artifacts needed
//! to reproduce and diagnose any E2E test failure. The bundle is self-contained
//! and can be used for offline debugging, CI artifact archiving, and issue reports.
//!
//! ## Bundle Contents
//!
//! Every failure bundle contains:
//!
//! - `manifest.json` - Bundle metadata and index
//! - `repro.sh` - Shell script to reproduce the failure
//! - `environment.json` - Runtime environment (versions, settings, system info)
//!
//! Optional contents (based on failure type):
//!
//! - `database/` - Database snapshots (*.db, *.wal files)
//! - `logs/` - Structured logs (JSONL format)
//! - `diffs/` - Schema and data differences
//! - `workload/` - OpLog and seed manifest
//!
//! ## Schema Version
//!
//! The bundle format is directly versioned so readers reject unknown schemas
//! and require an explicit migration.
//! Current version: `2.0`

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use fsqlite_harness::failure_bundle::ExecutionLaneEvidence;

use crate::{FRANKEN_SEED, HarnessSettings};

/// Current bundle schema version.
pub const BUNDLE_SCHEMA_VERSION: &str = "2.0";
const SECS_PER_DAY: u64 = 86_400;
const SECS_PER_HOUR: u64 = 3_600;
const SECS_PER_MIN: u64 = 60;

// ─── Bundle Manifest (manifest.json) ────────────────────────────────────

/// The top-level manifest that describes a failure bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureBundleManifest {
    /// Schema version for forward compatibility.
    pub schema_version: String,

    /// Unique bundle identifier (typically UUID or timestamp-based).
    pub bundle_id: String,

    /// Required and positively observed engine execution paths.
    pub execution_lane_evidence: ExecutionLaneEvidence,

    /// When the failure occurred (ISO 8601).
    pub failure_timestamp: String,

    /// When the bundle was created (ISO 8601).
    pub bundle_timestamp: String,

    /// Scenario that failed.
    pub scenario: ScenarioInfo,

    /// Seed and reproducibility information.
    pub reproducibility: ReproducibilityInfo,

    /// Environment snapshot.
    pub environment: EnvironmentInfo,

    /// Failure details.
    pub failure: FailureInfo,

    /// Index of files in the bundle.
    pub files: Vec<BundleFile>,
}

impl FailureBundleManifest {
    /// Validate the directly versioned manifest and its embedded lane proof.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.schema_version != BUNDLE_SCHEMA_VERSION {
            errors.push(format!(
                "schema_version must equal {BUNDLE_SCHEMA_VERSION}; direct migration required"
            ));
        }
        errors.extend(self.execution_lane_evidence.validate());
        if self.execution_lane_evidence.scenario_id != self.scenario.scenario_id {
            errors
                .push("execution_lane.scenario_id does not match manifest scenario_id".to_owned());
        }
        let mut paths = HashSet::new();
        for file in &self.files {
            if !is_safe_bundle_relative_path(&file.path) {
                errors.push(format!("bundle file path is unsafe: {}", file.path));
            }
            if matches!(file.path.as_str(), "manifest.json" | "repro.sh") {
                errors.push(format!("bundle file path is reserved: {}", file.path));
            }
            if !paths.insert(file.path.as_str()) {
                errors.push(format!("bundle file path is duplicated: {}", file.path));
            }
        }
        errors
    }
}

/// Read and strictly validate an existing failure-bundle manifest.
pub fn read_failure_bundle_manifest(
    path: impl AsRef<Path>,
) -> std::io::Result<FailureBundleManifest> {
    let bytes = std::fs::read(path)?;
    let manifest: FailureBundleManifest = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut errors = manifest.validate();
    let bundle_dir = path.as_ref().parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "manifest path has no parent",
        )
    })?;
    for file in &manifest.files {
        if !is_safe_bundle_relative_path(&file.path) {
            continue;
        }
        let artifact_path = bundle_dir.join(&file.path);
        match std::fs::read(&artifact_path) {
            Ok(bytes) => {
                let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                if actual_size != file.size_bytes {
                    errors.push(format!("bundle file size mismatch: {}", file.path));
                }
                let actual_hash = crate::bytes_to_lower_hex(sha2::Sha256::digest(&bytes));
                if actual_hash != file.sha256 {
                    errors.push(format!("bundle file hash mismatch: {}", file.path));
                }
            }
            Err(error) => errors.push(format!("bundle file unreadable: {}: {error}", file.path)),
        }
    }
    if !bundle_dir.join("repro.sh").is_file() {
        errors.push("bundle repro.sh is missing".to_owned());
    }
    if !errors.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            errors.join("; "),
        ));
    }
    Ok(manifest)
}

fn is_safe_bundle_relative_path(path: &str) -> bool {
    !path.is_empty()
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// Information about the scenario that failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioInfo {
    /// Scenario ID (e.g., "CON-3", "TXN-1").
    pub scenario_id: String,

    /// Human-readable scenario name.
    pub scenario_name: String,

    /// Category (e.g., "concurrency", "transaction", "corruption").
    pub category: String,

    /// Test entrypoint (e.g., "cargo test -p fsqlite-e2e --test ssi_write_skew").
    pub test_command: String,
}

/// Reproducibility information for exact replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReproducibilityInfo {
    /// Base seed used for this run.
    pub seed: u64,

    /// RNG algorithm specification.
    pub rng_algorithm: String,

    /// RNG library version.
    pub rng_version: String,

    /// Fixture ID if applicable.
    pub fixture_id: Option<String>,

    /// Workload preset name if applicable.
    pub workload_preset: Option<String>,

    /// Worker count for concurrent scenarios.
    pub worker_count: Option<u16>,

    /// Full replay command.
    pub replay_command: String,
}

/// Environment snapshot at failure time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    /// FrankenSQLite version/commit.
    pub fsqlite_version: String,

    /// Rust compiler version.
    pub rustc_version: String,

    /// Target triple (e.g., "x86_64-unknown-linux-gnu").
    pub target_triple: String,

    /// Operating system.
    pub os: String,

    /// CPU architecture.
    pub cpu_arch: String,

    /// Harness settings at failure time.
    pub harness_settings: HarnessSettingsSnapshot,

    /// Environment variables relevant to the test.
    pub env_vars: HashMap<String, String>,
}

/// Snapshot of harness settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessSettingsSnapshot {
    pub journal_mode: String,
    pub synchronous: String,
    pub cache_size: i64,
    pub page_size: u32,
    pub busy_timeout_ms: u32,
    pub concurrent_mode: bool,
}

impl From<&HarnessSettings> for HarnessSettingsSnapshot {
    fn from(settings: &HarnessSettings) -> Self {
        Self {
            journal_mode: settings.journal_mode.clone(),
            synchronous: settings.synchronous.clone(),
            cache_size: settings.cache_size,
            page_size: settings.page_size,
            busy_timeout_ms: settings.busy_timeout_ms,
            concurrent_mode: settings.concurrent_mode,
        }
    }
}

/// Details about the failure itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureInfo {
    /// Failure type classification.
    pub failure_type: FailureType,

    /// Error message.
    pub error_message: String,

    /// Stack trace if available.
    pub stack_trace: Option<String>,

    /// Exit code if applicable.
    pub exit_code: Option<i32>,

    /// Assertion that failed (if assertion failure).
    pub failed_assertion: Option<String>,

    /// Expected vs actual values (for comparison failures).
    pub expected: Option<String>,
    pub actual: Option<String>,
}

/// Classification of failure types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureType {
    /// Assertion failure in test code.
    Assertion,
    /// Panic/crash in production code.
    Panic,
    /// Timeout exceeded.
    Timeout,
    /// Divergence between engines (correctness failure).
    Divergence,
    /// Database corruption detected.
    Corruption,
    /// SSI/MVCC conflict (expected or unexpected).
    SsiConflict,
    /// Recovery failure.
    RecoveryFailure,
    /// IO/filesystem error.
    IoError,
    /// Unknown/other failure.
    Unknown,
}

/// A file included in the bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleFile {
    /// Relative path within the bundle.
    pub path: String,

    /// File size in bytes.
    pub size_bytes: u64,

    /// SHA-256 hash of file contents.
    pub sha256: String,

    /// MIME type or category.
    pub content_type: String,

    /// Human-readable description.
    pub description: String,
}

// ─── Bundle Builder ─────────────────────────────────────────────────────

/// Builder for creating failure bundles.
#[derive(Debug)]
pub struct FailureBundleBuilder {
    bundle_id: String,
    output_dir: PathBuf,
    scenario: Option<ScenarioInfo>,
    execution_lane_evidence: Option<ExecutionLaneEvidence>,
    reproducibility: Option<ReproducibilityInfo>,
    environment: Option<EnvironmentInfo>,
    failure: Option<FailureInfo>,
    files: Vec<(String, Vec<u8>, String, String)>, // (path, content, type, description)
}

impl FailureBundleBuilder {
    /// Create a new builder with auto-generated bundle ID.
    #[must_use]
    pub fn new(output_base: impl AsRef<Path>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let bundle_id = format!("failure_{timestamp}");
        let output_dir = output_base.as_ref().join(&bundle_id);

        Self {
            bundle_id,
            output_dir,
            scenario: None,
            execution_lane_evidence: None,
            reproducibility: None,
            environment: None,
            failure: None,
            files: Vec::new(),
        }
    }

    /// Set the scenario information.
    #[must_use]
    pub fn scenario(mut self, info: ScenarioInfo) -> Self {
        self.scenario = Some(info);
        self
    }

    /// Set fail-closed execution-lane evidence for this failure.
    #[must_use]
    pub fn execution_lane_evidence(mut self, evidence: ExecutionLaneEvidence) -> Self {
        self.execution_lane_evidence = Some(evidence);
        self
    }

    /// Set reproducibility information with default seed.
    #[must_use]
    pub fn reproducibility_default(mut self, scenario_id: &str) -> Self {
        self.reproducibility = Some(ReproducibilityInfo {
            seed: FRANKEN_SEED,
            rng_algorithm: "StdRng/ChaCha12".to_owned(),
            rng_version: "rand 0.8".to_owned(),
            fixture_id: None,
            workload_preset: None,
            worker_count: None,
            replay_command: format!("cargo test -p fsqlite-e2e -- {scenario_id} --nocapture"),
        });
        self
    }

    /// Set full reproducibility information.
    #[must_use]
    pub fn reproducibility(mut self, info: ReproducibilityInfo) -> Self {
        self.reproducibility = Some(info);
        self
    }

    /// Set environment information.
    #[must_use]
    pub fn environment(mut self, info: EnvironmentInfo) -> Self {
        self.environment = Some(info);
        self
    }

    /// Set environment from current system.
    #[must_use]
    pub fn environment_auto(mut self, settings: &HarnessSettings) -> Self {
        let mut env_vars = HashMap::new();
        for key in ["E2E_SEED", "RUST_BACKTRACE", "RUST_LOG"] {
            if let Ok(val) = std::env::var(key) {
                env_vars.insert(key.to_owned(), val);
            }
        }

        self.environment = Some(EnvironmentInfo {
            fsqlite_version: env!("CARGO_PKG_VERSION").to_owned(),
            rustc_version: rustc_version(),
            target_triple: infer_target_triple(),
            os: std::env::consts::OS.to_owned(),
            cpu_arch: std::env::consts::ARCH.to_owned(),
            harness_settings: HarnessSettingsSnapshot::from(settings),
            env_vars,
        });
        self
    }

    /// Set failure information.
    #[must_use]
    pub fn failure(mut self, info: FailureInfo) -> Self {
        self.failure = Some(info);
        self
    }

    /// Add a file to the bundle.
    #[must_use]
    pub fn add_file(
        mut self,
        relative_path: impl Into<String>,
        content: Vec<u8>,
        content_type: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.files.push((
            relative_path.into(),
            content,
            content_type.into(),
            description.into(),
        ));
        self
    }

    /// Add a text file to the bundle.
    #[must_use]
    pub fn add_text_file(
        self,
        relative_path: impl Into<String>,
        content: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.add_file(
            relative_path,
            content.into().into_bytes(),
            "text/plain",
            description,
        )
    }

    /// Add a JSON file to the bundle.
    #[must_use]
    pub fn add_json_file<T: Serialize>(
        self,
        relative_path: impl Into<String>,
        data: &T,
        description: impl Into<String>,
    ) -> Self {
        let json = serde_json::to_string_pretty(data).unwrap_or_else(|_| "{}".to_owned());
        self.add_file(
            relative_path,
            json.into_bytes(),
            "application/json",
            description,
        )
    }

    /// Build the bundle and write to disk.
    ///
    /// Returns the path to the bundle directory.
    pub fn build(self) -> std::io::Result<PathBuf> {
        use sha2::{Digest, Sha256};
        use std::fs;
        use std::io::Write;

        let execution_lane_evidence = self.execution_lane_evidence.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "execution_lane_evidence is required",
            )
        })?;
        let lane_errors = execution_lane_evidence.validate();
        if !lane_errors.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid execution_lane_evidence: {}",
                    lane_errors.join("; ")
                ),
            ));
        }
        let scenario = self.scenario.unwrap_or_else(|| ScenarioInfo {
            scenario_id: "UNKNOWN".to_owned(),
            scenario_name: "Unknown scenario".to_owned(),
            category: "unknown".to_owned(),
            test_command: String::new(),
        });
        if execution_lane_evidence.scenario_id != scenario.scenario_id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "execution_lane.scenario_id does not match manifest scenario_id",
            ));
        }
        let mut paths = HashSet::new();
        for (path, ..) in &self.files {
            if !is_safe_bundle_relative_path(path) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("bundle file path is unsafe: {path}"),
                ));
            }
            if matches!(path.as_str(), "manifest.json" | "repro.sh") {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("bundle file path is reserved: {path}"),
                ));
            }
            if !paths.insert(path.as_str()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("bundle file path is duplicated: {path}"),
                ));
            }
        }

        // Validate all evidence before creating any artifact paths so a
        // cancelled or rejected capture cannot leave a partial bundle.
        fs::create_dir_all(&self.output_dir)?;

        // Write files and collect metadata.
        let mut bundle_files = Vec::new();
        for (path, content, content_type, description) in &self.files {
            let full_path = self.output_dir.join(path);
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)?;
            }

            let mut file = fs::File::create(&full_path)?;
            file.write_all(content)?;

            let hash = crate::bytes_to_lower_hex(Sha256::digest(content));
            bundle_files.push(BundleFile {
                path: path.clone(),
                size_bytes: content.len() as u64,
                sha256: hash,
                content_type: content_type.clone(),
                description: description.clone(),
            });
        }

        // Build manifest.
        let now = iso8601_now();
        let manifest = FailureBundleManifest {
            schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
            bundle_id: self.bundle_id.clone(),
            execution_lane_evidence,
            failure_timestamp: now.clone(),
            bundle_timestamp: now,
            scenario,
            reproducibility: self.reproducibility.unwrap_or_else(|| ReproducibilityInfo {
                seed: FRANKEN_SEED,
                rng_algorithm: "StdRng/ChaCha12".to_owned(),
                rng_version: "rand 0.8".to_owned(),
                fixture_id: None,
                workload_preset: None,
                worker_count: None,
                replay_command: String::new(),
            }),
            environment: self.environment.unwrap_or_else(|| EnvironmentInfo {
                fsqlite_version: env!("CARGO_PKG_VERSION").to_owned(),
                rustc_version: "unknown".to_owned(),
                target_triple: "unknown".to_owned(),
                os: std::env::consts::OS.to_owned(),
                cpu_arch: std::env::consts::ARCH.to_owned(),
                harness_settings: HarnessSettingsSnapshot {
                    journal_mode: "wal".to_owned(),
                    synchronous: "NORMAL".to_owned(),
                    cache_size: -2000,
                    page_size: 4096,
                    busy_timeout_ms: 5000,
                    concurrent_mode: true,
                },
                env_vars: HashMap::new(),
            }),
            failure: self.failure.unwrap_or_else(|| FailureInfo {
                failure_type: FailureType::Unknown,
                error_message: "Unknown error".to_owned(),
                stack_trace: None,
                exit_code: None,
                failed_assertion: None,
                expected: None,
                actual: None,
            }),
            files: bundle_files,
        };

        // Write the repro script before publishing the manifest. A valid
        // manifest is the bundle's final completion marker.
        let repro_script = generate_repro_script(&manifest);
        let repro_path = self.output_dir.join("repro.sh");
        fs::write(&repro_path, repro_script)?;

        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        let manifest_path = self.output_dir.join("manifest.json");
        fs::write(&manifest_path, manifest_json)?;

        Ok(self.output_dir)
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────

fn rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .map_or_else(
            |_| "unknown".to_owned(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_owned(),
        )
}

/// Infer the target triple from available environment constants.
///
/// Constructs a target triple like "x86_64-unknown-linux-gnu" from:
/// - `std::env::consts::ARCH` (e.g., "x86_64")
/// - `std::env::consts::OS` (e.g., "linux")
/// - `std::env::consts::FAMILY` (e.g., "unix")
fn infer_target_triple() -> String {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    // Infer vendor and environment based on OS.
    let (vendor, env) = match os {
        "linux" => ("unknown", "gnu"),
        "macos" => ("apple", "darwin"),
        "windows" => ("pc", "msvc"),
        "freebsd" | "openbsd" | "netbsd" => ("unknown", ""),
        _ => ("unknown", "unknown"),
    };

    if env.is_empty() {
        format!("{arch}-{vendor}-{os}")
    } else {
        format!("{arch}-{vendor}-{os}-{env}")
    }
}

fn iso8601_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();

    // Convert Unix timestamp to ISO 8601 format (UTC).
    // Simple implementation without external chrono crate.
    // Days since Unix epoch calculation.
    let days_since_epoch = secs / SECS_PER_DAY;
    let remaining = secs % SECS_PER_DAY;
    let hour = remaining / SECS_PER_HOUR;
    let minute = (remaining % SECS_PER_HOUR) / SECS_PER_MIN;
    let second = remaining % SECS_PER_MIN;

    // Calculate year/month/day from days since 1970-01-01.
    // Using a simplified algorithm that handles leap years.
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
fn days_to_ymd(days: u64) -> (u32, u32, u32) {
    // Days in each month (non-leap year).
    const DAYS_IN_MONTH: [u64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    fn is_leap_year(year: u64) -> bool {
        (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
    }

    fn days_in_year(year: u64) -> u64 {
        if is_leap_year(year) { 366 } else { 365 }
    }

    let mut remaining_days = days;
    let mut year: u64 = 1970;

    // Find the year.
    loop {
        let days_this_year = days_in_year(year);
        if remaining_days < days_this_year {
            break;
        }
        remaining_days -= days_this_year;
        year += 1;
    }

    // Find the month.
    let mut month: u64 = 1;
    for (i, &days_in_month) in DAYS_IN_MONTH.iter().enumerate() {
        let days_this_month = if i == 1 && is_leap_year(year) {
            29
        } else {
            days_in_month
        };
        if remaining_days < days_this_month {
            break;
        }
        remaining_days -= days_this_month;
        month += 1;
    }

    let day = remaining_days + 1; // Days are 1-indexed.

    (
        u32::try_from(year).unwrap_or(u32::MAX),
        u32::try_from(month).unwrap_or(12),
        u32::try_from(day).unwrap_or(31),
    )
}

fn generate_repro_script(manifest: &FailureBundleManifest) -> String {
    use std::fmt::Write as _;

    let mut script = String::new();
    script.push_str("#!/bin/bash\n");
    script.push_str("# Reproduction script for failure bundle\n");
    let _ = writeln!(script, "# Bundle ID: {}", manifest.bundle_id);
    let _ = writeln!(script, "# Scenario: {}", manifest.scenario.scenario_id);
    script.push('\n');
    script.push_str("set -e\n");
    script.push('\n');
    let _ = writeln!(script, "export E2E_SEED={}", manifest.reproducibility.seed);
    script.push('\n');
    script.push_str("# Replay command:\n");
    let _ = writeln!(script, "{}", manifest.reproducibility.replay_command);
    script
}

// ─── Convenience Functions ──────────────────────────────────────────────

/// Create a failure bundle for an assertion failure.
pub fn bundle_assertion_failure(
    output_base: impl AsRef<Path>,
    scenario_id: &str,
    scenario_name: &str,
    assertion: &str,
    expected: Option<&str>,
    actual: Option<&str>,
    settings: &HarnessSettings,
) -> std::io::Result<PathBuf> {
    FailureBundleBuilder::new(output_base)
        .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
            "unreported",
            scenario_id,
            scenario_id,
            "assertion",
        ))
        .scenario(ScenarioInfo {
            scenario_id: scenario_id.to_owned(),
            scenario_name: scenario_name.to_owned(),
            category: "test".to_owned(),
            test_command: format!("cargo test -p fsqlite-e2e -- {scenario_id}"),
        })
        .reproducibility_default(scenario_id)
        .environment_auto(settings)
        .failure(FailureInfo {
            failure_type: FailureType::Assertion,
            error_message: format!("Assertion failed: {assertion}"),
            stack_trace: None,
            exit_code: Some(1),
            failed_assertion: Some(assertion.to_owned()),
            expected: expected.map(str::to_owned),
            actual: actual.map(str::to_owned),
        })
        .build()
}

/// Create a failure bundle for a divergence (correctness) failure.
pub fn bundle_divergence_failure(
    output_base: impl AsRef<Path>,
    fixture_id: &str,
    expected_hash: &str,
    actual_hash: &str,
    settings: &HarnessSettings,
) -> std::io::Result<PathBuf> {
    let scenario_id = format!("CMP-divergence-{fixture_id}");
    FailureBundleBuilder::new(output_base)
        .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
            "unreported",
            fixture_id,
            &scenario_id,
            "differential",
        ))
        .scenario(ScenarioInfo {
            scenario_id,
            scenario_name: format!("Divergence in {fixture_id}"),
            category: "compatibility".to_owned(),
            test_command: format!(
                "cargo run -p fsqlite-e2e --bin realdb_e2e -- run --db {fixture_id}"
            ),
        })
        .reproducibility(ReproducibilityInfo {
            seed: FRANKEN_SEED,
            rng_algorithm: "StdRng/ChaCha12".to_owned(),
            rng_version: "rand 0.8".to_owned(),
            fixture_id: Some(fixture_id.to_owned()),
            workload_preset: None,
            worker_count: None,
            replay_command: format!(
                "cargo run -p fsqlite-e2e --bin realdb_e2e -- --seed {} run --db {fixture_id}",
                FRANKEN_SEED
            ),
        })
        .environment_auto(settings)
        .failure(FailureInfo {
            failure_type: FailureType::Divergence,
            error_message: format!("Hash mismatch: expected {expected_hash}, got {actual_hash}"),
            stack_trace: None,
            exit_code: Some(1),
            failed_assertion: Some("hash equality".to_owned()),
            expected: Some(expected_hash.to_owned()),
            actual: Some(actual_hash.to_owned()),
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_schema_version() {
        assert_eq!(BUNDLE_SCHEMA_VERSION, "2.0");
    }

    #[test]
    fn test_failure_type_serialization() {
        let types = [
            (FailureType::Assertion, "assertion"),
            (FailureType::Panic, "panic"),
            (FailureType::Divergence, "divergence"),
            (FailureType::SsiConflict, "ssi_conflict"),
        ];

        for (ft, expected) in types {
            let json = serde_json::to_string(&ft).unwrap();
            assert!(json.contains(expected), "Expected {expected} in {json}");
        }
    }

    #[test]
    fn test_harness_settings_snapshot() {
        let settings = HarnessSettings::default();
        let snapshot = HarnessSettingsSnapshot::from(&settings);

        assert_eq!(snapshot.journal_mode, "wal");
        assert!(snapshot.concurrent_mode);
    }

    #[test]
    fn test_builder_creates_bundle() {
        let tmp = tempfile::tempdir().unwrap();
        let bundle_path = FailureBundleBuilder::new(tmp.path())
            .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
                "trace-test",
                "run-test",
                "TEST-1",
                "test",
            ))
            .scenario(ScenarioInfo {
                scenario_id: "TEST-1".to_owned(),
                scenario_name: "Test scenario".to_owned(),
                category: "test".to_owned(),
                test_command: "cargo test".to_owned(),
            })
            .reproducibility_default("TEST-1")
            .failure(FailureInfo {
                failure_type: FailureType::Assertion,
                error_message: "Test error".to_owned(),
                stack_trace: None,
                exit_code: Some(1),
                failed_assertion: Some("x == y".to_owned()),
                expected: Some("42".to_owned()),
                actual: Some("43".to_owned()),
            })
            .add_text_file("logs/test.log", "Test log content", "Test log")
            .build()
            .unwrap();

        assert!(bundle_path.exists());
        assert!(bundle_path.join("manifest.json").exists());
        assert!(bundle_path.join("repro.sh").exists());
        assert!(bundle_path.join("logs/test.log").exists());

        // Verify manifest contents.
        let manifest_json = std::fs::read_to_string(bundle_path.join("manifest.json")).unwrap();
        let manifest = read_failure_bundle_manifest(bundle_path.join("manifest.json")).unwrap();
        assert_eq!(manifest.schema_version, "2.0");
        assert_eq!(manifest.scenario.scenario_id, "TEST-1");
        assert_eq!(manifest.failure.failure_type, FailureType::Assertion);
        assert!(manifest.execution_lane_evidence.requirement_satisfied);

        let serialized = serde_json::to_string(&manifest).unwrap();
        let decoded: FailureBundleManifest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(serialized, serde_json::to_string(&decoded).unwrap());
        let legacy = serialized
            .replace("\"schema_version\":\"2.0\",", "\"schema_version\":\"1.0\",")
            .replace(
                &format!(
                    "\"execution_lane_evidence\":{},",
                    serde_json::to_string(&manifest.execution_lane_evidence).unwrap()
                ),
                "",
            );
        assert!(serde_json::from_str::<FailureBundleManifest>(&legacy).is_err());
        assert!(manifest_json.contains("execution_lane_evidence"));

        std::fs::write(bundle_path.join("logs/test.log"), "tampered").unwrap();
        let error = read_failure_bundle_manifest(bundle_path.join("manifest.json"))
            .expect_err("a changed artifact must invalidate its bundle");
        assert!(error.to_string().contains("bundle file"), "{error}");
    }

    #[test]
    fn invalid_lane_evidence_fails_before_artifact_creation() {
        let tmp = tempfile::tempdir().unwrap();
        let mut evidence =
            ExecutionLaneEvidence::semantic_only("trace-test", "run-test", "TEST-CANCEL", "test");
        evidence.observed_lanes.clear();
        let result = FailureBundleBuilder::new(tmp.path())
            .execution_lane_evidence(evidence)
            .scenario(ScenarioInfo {
                scenario_id: "TEST-CANCEL".to_owned(),
                scenario_name: "Cancelled capture".to_owned(),
                category: "test".to_owned(),
                test_command: "cargo test".to_owned(),
            })
            .add_text_file("logs/partial.log", "must not be written", "partial")
            .build();
        assert!(result.is_err());
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }

    #[test]
    fn mismatched_lane_scenario_fails_before_artifact_creation() {
        let tmp = tempfile::tempdir().unwrap();
        let result = FailureBundleBuilder::new(tmp.path())
            .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
                "trace-test",
                "run-test",
                "OTHER-SCENARIO",
                "test",
            ))
            .scenario(ScenarioInfo {
                scenario_id: "TEST-CORRELATION".to_owned(),
                scenario_name: "Correlation mismatch".to_owned(),
                category: "test".to_owned(),
                test_command: "cargo test".to_owned(),
            })
            .add_text_file("logs/partial.log", "must not be written", "partial")
            .build();
        let error = result.expect_err("scenario mismatch must fail closed");
        assert!(
            error
                .to_string()
                .contains("scenario_id does not match manifest scenario_id")
        );
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }

    #[test]
    fn interrupted_manifest_write_is_never_accepted_as_a_bundle() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest_path = tmp.path().join("manifest.json");
        std::fs::write(
            &manifest_path,
            br#"{"schema_version":"2.0","bundle_id":"interrupted""#,
        )
        .unwrap();

        let error = read_failure_bundle_manifest(&manifest_path)
            .expect_err("a truncated manifest must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn manifest_is_not_a_completion_marker_until_repro_exists() {
        let source = tempfile::tempdir().unwrap();
        let bundle_path = bundle_assertion_failure(
            source.path(),
            "TEST-INCOMPLETE",
            "Incomplete publication",
            "manifest published last",
            None,
            None,
            &HarnessSettings::default(),
        )
        .unwrap();
        let interrupted = tempfile::tempdir().unwrap();
        let manifest_path = interrupted.path().join("manifest.json");
        std::fs::copy(bundle_path.join("manifest.json"), &manifest_path).unwrap();

        let error = read_failure_bundle_manifest(&manifest_path)
            .expect_err("a manifest published before repro.sh must not be accepted");
        assert!(error.to_string().contains("repro.sh is missing"), "{error}");
    }

    #[test]
    fn unsafe_artifact_path_fails_before_bundle_creation() {
        let tmp = tempfile::tempdir().unwrap();
        let result = FailureBundleBuilder::new(tmp.path())
            .execution_lane_evidence(ExecutionLaneEvidence::semantic_only(
                "trace-test",
                "run-test",
                "TEST-UNSAFE-PATH",
                "test",
            ))
            .scenario(ScenarioInfo {
                scenario_id: "TEST-UNSAFE-PATH".to_owned(),
                scenario_name: "Unsafe artifact path".to_owned(),
                category: "test".to_owned(),
                test_command: "cargo test".to_owned(),
            })
            .add_text_file("../escaped.log", "must not be written", "unsafe")
            .build();

        let error = result.expect_err("parent traversal must fail closed");
        assert!(error.to_string().contains("path is unsafe"), "{error}");
        assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    }

    #[test]
    fn test_iso8601_format() {
        let timestamp = iso8601_now();
        // Should match ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
        // Example: "2024-02-12T15:30:45Z"
        assert_eq!(timestamp.len(), 20, "ISO 8601 timestamp should be 20 chars");
        assert!(timestamp.ends_with('Z'), "Should end with Z (UTC)");
        assert_eq!(&timestamp[4..5], "-", "Should have hyphen at position 4");
        assert_eq!(&timestamp[7..8], "-", "Should have hyphen at position 7");
        assert_eq!(&timestamp[10..11], "T", "Should have T at position 10");
        assert_eq!(&timestamp[13..14], ":", "Should have colon at position 13");
        assert_eq!(&timestamp[16..17], ":", "Should have colon at position 16");
        // Year should be >= 2024 (reasonable sanity check).
        let year: u32 = timestamp[0..4].parse().expect("year should parse");
        assert!(year >= 2024, "Year {year} should be >= 2024");
    }

    #[test]
    fn test_days_to_ymd_epoch() {
        // Day 0 should be 1970-01-01.
        let (y, m, d) = days_to_ymd(0);
        assert_eq!((y, m, d), (1970, 1, 1), "Day 0 is 1970-01-01");
    }

    #[test]
    fn test_days_to_ymd_known_date() {
        // 2024-02-12 is day 19765 since epoch (leap year).
        // Let's verify a known date: 2000-01-01 is day 10957.
        let (y, m, d) = days_to_ymd(10957);
        assert_eq!((y, m, d), (2000, 1, 1), "Day 10957 is 2000-01-01");
    }

    #[test]
    fn test_target_triple_format() {
        let triple = infer_target_triple();
        // Should have at least 3 parts separated by hyphens.
        let parts: Vec<&str> = triple.split('-').collect();
        assert!(
            parts.len() >= 3,
            "Target triple {triple:?} should have at least 3 parts"
        );
        // First part should be the architecture.
        assert_eq!(parts[0], std::env::consts::ARCH);
    }
}
