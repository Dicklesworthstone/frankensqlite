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
//! The bundle format is versioned to support forward compatibility.
//! Current version: `1.0`

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::{FRANKEN_SEED, HarnessSettings};

/// Current bundle schema version.
pub const BUNDLE_SCHEMA_VERSION: &str = "1.0";

// ─── Bundle Manifest (manifest.json) ────────────────────────────────────

/// The top-level manifest that describes a failure bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureBundleManifest {
    /// Schema version for forward compatibility.
    pub schema_version: String,

    /// Unique bundle identifier (typically UUID or timestamp-based).
    pub bundle_id: String,

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
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let bundle_id = format!("failure_{timestamp}");
        let output_dir = output_base.as_ref().join(&bundle_id);

        Self {
            bundle_id,
            output_dir,
            scenario: None,
            reproducibility: None,
            environment: None,
            failure: None,
            files: Vec::new(),
        }
    }

    /// Set the scenario information.
    pub fn scenario(mut self, info: ScenarioInfo) -> Self {
        self.scenario = Some(info);
        self
    }

    /// Set reproducibility information with default seed.
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
    pub fn reproducibility(mut self, info: ReproducibilityInfo) -> Self {
        self.reproducibility = Some(info);
        self
    }

    /// Set environment information.
    pub fn environment(mut self, info: EnvironmentInfo) -> Self {
        self.environment = Some(info);
        self
    }

    /// Set environment from current system.
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
    pub fn failure(mut self, info: FailureInfo) -> Self {
        self.failure = Some(info);
        self
    }

    /// Add a file to the bundle.
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

        // Create output directory.
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

            let hash = format!("{:x}", Sha256::digest(content));
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
            failure_timestamp: now.clone(),
            bundle_timestamp: now,
            scenario: self.scenario.unwrap_or_else(|| ScenarioInfo {
                scenario_id: "UNKNOWN".to_owned(),
                scenario_name: "Unknown scenario".to_owned(),
                category: "unknown".to_owned(),
                test_command: "".to_owned(),
            }),
            reproducibility: self.reproducibility.unwrap_or_else(|| ReproducibilityInfo {
                seed: FRANKEN_SEED,
                rng_algorithm: "StdRng/ChaCha12".to_owned(),
                rng_version: "rand 0.8".to_owned(),
                fixture_id: None,
                workload_preset: None,
                worker_count: None,
                replay_command: "".to_owned(),
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

        // Write manifest.
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        let manifest_path = self.output_dir.join("manifest.json");
        fs::write(&manifest_path, manifest_json)?;

        // Write repro script.
        let repro_script = generate_repro_script(&manifest);
        let repro_path = self.output_dir.join("repro.sh");
        fs::write(&repro_path, repro_script)?;

        Ok(self.output_dir)
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────

fn rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|_| "unknown".to_owned())
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
    const SECS_PER_DAY: u64 = 86400;
    const SECS_PER_HOUR: u64 = 3600;
    const SECS_PER_MIN: u64 = 60;

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
    const DAYS_IN_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    fn is_leap_year(year: u32) -> bool {
        (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
    }

    fn days_in_year(year: u32) -> u32 {
        if is_leap_year(year) { 366 } else { 365 }
    }

    let mut remaining_days = days as u32;
    let mut year = 1970u32;

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
    let mut month = 1u32;
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

    (year, month, day)
}

fn generate_repro_script(manifest: &FailureBundleManifest) -> String {
    let mut script = String::new();
    script.push_str("#!/bin/bash\n");
    script.push_str("# Reproduction script for failure bundle\n");
    script.push_str(&format!("# Bundle ID: {}\n", manifest.bundle_id));
    script.push_str(&format!("# Scenario: {}\n", manifest.scenario.scenario_id));
    script.push_str("\n");
    script.push_str("set -e\n");
    script.push_str("\n");
    script.push_str(&format!(
        "export E2E_SEED={}\n",
        manifest.reproducibility.seed
    ));
    script.push_str("\n");
    script.push_str("# Replay command:\n");
    script.push_str(&format!("{}\n", manifest.reproducibility.replay_command));
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
    FailureBundleBuilder::new(output_base)
        .scenario(ScenarioInfo {
            scenario_id: format!("CMP-divergence-{fixture_id}"),
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
        assert_eq!(BUNDLE_SCHEMA_VERSION, "1.0");
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
        let manifest: FailureBundleManifest = serde_json::from_str(&manifest_json).unwrap();
        assert_eq!(manifest.schema_version, "1.0");
        assert_eq!(manifest.scenario.scenario_id, "TEST-1");
        assert_eq!(manifest.failure.failure_type, FailureType::Assertion);
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
