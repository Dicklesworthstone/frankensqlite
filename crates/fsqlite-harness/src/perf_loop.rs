//! Extreme optimization loop enforcement utilities (§17.8.1, bead `bd-3cl3.1`).
//!
//! This module provides CI-friendly gates for:
//! - one optimization lever per perf commit (git-diff heuristic),
//! - mandatory baseline artifact presence,
//! - golden output checksum capture/verification.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bead identifier for log/assert correlation.
pub const BEAD_ID: &str = "bd-3cl3.1";
/// Deterministic measurement discipline bead identifier.
pub const DETERMINISTIC_MEASUREMENT_BEAD_ID: &str = "bd-3cl3.2";
/// Opportunity matrix gate bead identifier.
pub const OPPORTUNITY_MATRIX_BEAD_ID: &str = "bd-3cl3.3";
/// Golden checksum behavior lock bead identifier.
pub const GOLDEN_BEHAVIOR_LOCK_BEAD_ID: &str = "bd-3cl3.6";
/// Profiling cookbook gate bead identifier.
pub const PROFILING_COOKBOOK_BEAD_ID: &str = "bd-3cl3.5";
/// Baseline layout bead identifier.
pub const BASELINE_LAYOUT_BEAD_ID: &str = "bd-3cl3.4";

/// Required baseline artifact directory names (§17.8.4).
pub const REQUIRED_BASELINE_DIRS: [&str; 5] = [
    "criterion",
    "hyperfine",
    "alloc_census",
    "syscalls",
    "smoke",
];

/// Required environment metadata keys for reproducible measurement artifacts (§17.8.2).
pub const REQUIRED_MEASUREMENT_ENV_KEYS: [&str; 5] =
    ["RUSTFLAGS", "FEATURE_FLAGS", "MODE", "GIT_SHA", "PLATFORM"];
/// Minimum score required to pass the opportunity matrix gate.
pub const OPPORTUNITY_SCORE_THRESHOLD: f64 = 2.0;
/// Required profiling metadata keys.
pub const REQUIRED_PROFILING_METADATA_KEYS: [&str; 5] =
    ["git_sha", "scenario", "seed", "build_flags", "platform"];
/// Profiling artifact key order used in reports.
pub const REQUIRED_PROFILING_ARTIFACT_KEYS: [&str; 4] =
    ["flamegraph", "hyperfine", "heaptrack", "strace"];
/// Required profiling tool names.
pub const REQUIRED_PROFILING_TOOLS: [&str; 4] =
    ["cargo-flamegraph", "hyperfine", "heaptrack", "strace"];
/// Conformance artifacts that MUST be covered by behavior-lock checksums.
pub const REQUIRED_CONFORMANCE_ARTIFACT_NAMES: [&str; 3] =
    ["CommitMarker", "CommitProof", "AbortWitness"];

/// High-level optimization lever classification used by the one-lever gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OptimizationLever {
    ComputeKernel,
    QueryPipeline,
    Concurrency,
    IoPath,
    BenchmarkHarness,
    CompilerConfig,
}

impl fmt::Display for OptimizationLever {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ComputeKernel => "compute-kernel",
            Self::QueryPipeline => "query-pipeline",
            Self::Concurrency => "concurrency",
            Self::IoPath => "io-path",
            Self::BenchmarkHarness => "benchmark-harness",
            Self::CompilerConfig => "compiler-config",
        };
        f.write_str(name)
    }
}

/// Validation error emitted by optimization loop gates.
#[derive(Debug, Clone, PartialEq)]
pub enum PerfLoopError {
    NoOptimizationLeverDetected,
    MultipleOptimizationLevers {
        levers: BTreeSet<OptimizationLever>,
    },
    MissingBaselineArtifact {
        path: PathBuf,
    },
    EmptyBaselineArtifact {
        path: PathBuf,
    },
    MissingGoldenChecksumFile {
        path: PathBuf,
    },
    InvalidChecksumLine {
        line: String,
    },
    MissingGoldenOutput {
        path: PathBuf,
    },
    GoldenChecksumMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },
    DigestCommandFailed {
        path: PathBuf,
        stderr: String,
    },
    DigestCommandOutputMalformed {
        path: PathBuf,
    },
    Io {
        path: PathBuf,
        message: String,
    },
    MissingBaselineDirectory {
        path: PathBuf,
    },
    InvalidSmokeReportField {
        field: &'static str,
    },
    MissingMeasurementMetadata {
        key: &'static str,
    },
    InvalidMeasurementMetadata {
        key: &'static str,
    },
    EmptySchedule,
    InvalidTraceFingerprint {
        value: String,
    },
    ScheduleFingerprintMismatch {
        expected: String,
        actual: String,
    },
    InvalidMeasurementField {
        field: &'static str,
    },
    InvalidOpportunityField {
        field: &'static str,
    },
    MissingOpportunityMatrix,
    MissingOpportunityEntries,
    OpportunityScoreBelowThreshold {
        hotspot: String,
        score: f64,
        threshold: f64,
    },
    InvalidProfilingField {
        field: &'static str,
    },
    MissingProfilingMetadata {
        key: &'static str,
    },
    MissingProfilingArtifactPath {
        key: &'static str,
    },
    ToolUnavailable {
        tool: String,
        remediation: String,
    },
    MissingConformanceArtifact {
        name: &'static str,
    },
}

impl fmt::Display for PerfLoopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoOptimizationLeverDetected => {
                write!(f, "bead_id={BEAD_ID} no optimization lever detected")
            }
            Self::MultipleOptimizationLevers { levers } => {
                let joined = levers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "bead_id={BEAD_ID} multiple optimization levers detected: {joined}"
                )
            }
            Self::MissingBaselineArtifact { path } => write!(
                f,
                "bead_id={BEAD_ID} missing baseline artifact: {}",
                path.display()
            ),
            Self::EmptyBaselineArtifact { path } => write!(
                f,
                "bead_id={BEAD_ID} empty baseline artifact: {}",
                path.display()
            ),
            Self::MissingGoldenChecksumFile { path } => write!(
                f,
                "bead_id={BEAD_ID} missing golden checksum file: {}",
                path.display()
            ),
            Self::InvalidChecksumLine { line } => {
                write!(f, "bead_id={BEAD_ID} invalid golden checksum line: {line}")
            }
            Self::MissingGoldenOutput { path } => write!(
                f,
                "bead_id={BEAD_ID} missing golden output file: {}",
                path.display()
            ),
            Self::GoldenChecksumMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "bead_id={BEAD_ID} checksum mismatch for {} expected={} actual={}",
                path.display(),
                expected,
                actual
            ),
            Self::DigestCommandFailed { path, stderr } => write!(
                f,
                "bead_id={BEAD_ID} sha256sum failed for {}: {stderr}",
                path.display()
            ),
            Self::DigestCommandOutputMalformed { path } => write!(
                f,
                "bead_id={BEAD_ID} sha256sum output malformed for {}",
                path.display()
            ),
            Self::Io { path, message } => write!(
                f,
                "bead_id={BEAD_ID} I/O error for {}: {message}",
                path.display()
            ),
            Self::MissingBaselineDirectory { path } => write!(
                f,
                "bead_id={BASELINE_LAYOUT_BEAD_ID} missing baseline directory: {}",
                path.display()
            ),
            Self::InvalidSmokeReportField { field } => write!(
                f,
                "bead_id={BASELINE_LAYOUT_BEAD_ID} invalid perf smoke report field: {field}"
            ),
            Self::MissingMeasurementMetadata { key } => write!(
                f,
                "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} missing measurement metadata key: {key}"
            ),
            Self::InvalidMeasurementMetadata { key } => write!(
                f,
                "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} invalid measurement metadata value: {key}"
            ),
            Self::EmptySchedule => write!(
                f,
                "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} schedule must not be empty"
            ),
            Self::InvalidTraceFingerprint { value } => write!(
                f,
                "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} invalid trace fingerprint: {value}"
            ),
            Self::ScheduleFingerprintMismatch { expected, actual } => write!(
                f,
                "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} schedule fingerprint mismatch expected={expected} actual={actual}"
            ),
            Self::InvalidMeasurementField { field } => write!(
                f,
                "bead_id={DETERMINISTIC_MEASUREMENT_BEAD_ID} invalid measurement field: {field}"
            ),
            Self::InvalidOpportunityField { field } => write!(
                f,
                "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} invalid opportunity matrix field: {field}"
            ),
            Self::MissingOpportunityMatrix => write!(
                f,
                "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} missing opportunity matrix artifact"
            ),
            Self::MissingOpportunityEntries => write!(
                f,
                "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} opportunity matrix must include at least one entry"
            ),
            Self::OpportunityScoreBelowThreshold {
                hotspot,
                score,
                threshold,
            } => write!(
                f,
                "bead_id={OPPORTUNITY_MATRIX_BEAD_ID} hotspot={hotspot} score={score:.4} below threshold={threshold:.4}"
            ),
            Self::InvalidProfilingField { field } => write!(
                f,
                "bead_id={PROFILING_COOKBOOK_BEAD_ID} invalid profiling field: {field}"
            ),
            Self::MissingProfilingMetadata { key } => write!(
                f,
                "bead_id={PROFILING_COOKBOOK_BEAD_ID} missing profiling metadata key: {key}"
            ),
            Self::MissingProfilingArtifactPath { key } => write!(
                f,
                "bead_id={PROFILING_COOKBOOK_BEAD_ID} missing profiling artifact path: {key}"
            ),
            Self::ToolUnavailable { tool, remediation } => write!(
                f,
                "bead_id={PROFILING_COOKBOOK_BEAD_ID} tool unavailable: {tool}; remediation: {remediation}"
            ),
            Self::MissingConformanceArtifact { name } => write!(
                f,
                "bead_id={GOLDEN_BEHAVIOR_LOCK_BEAD_ID} missing conformance artifact in golden output set: {name}"
            ),
        }
    }
}

impl std::error::Error for PerfLoopError {}

/// Parse changed paths from a git unified diff payload.
///
/// Expected input lines include git headers:
/// `diff --git a/<path> b/<path>`.
#[must_use]
pub fn parse_git_diff_changed_paths(diff_text: &str) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut paths = Vec::new();

    for line in diff_text.lines() {
        let Some(rest) = line.strip_prefix("diff --git ") else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let _lhs = fields.next();
        let rhs = fields.next();
        let Some(rhs) = rhs else {
            continue;
        };
        let Some(stripped) = rhs.strip_prefix("b/") else {
            continue;
        };
        let path = PathBuf::from(stripped);
        if seen.insert(path.clone()) {
            paths.push(path);
        }
    }

    paths
}

/// Classify a changed file path into one optimization lever bucket.
///
/// Returns `None` for non-lever files (docs, tests, issue metadata, etc.).
#[must_use]
pub fn classify_optimization_lever(path: &Path) -> Option<OptimizationLever> {
    let normalized = path.to_string_lossy().replace('\\', "/");

    if normalized.starts_with(".beads/")
        || normalized.starts_with("docs/")
        || normalized.ends_with(".md")
        || normalized.starts_with("tests/")
        || normalized.contains("/tests/")
    {
        return None;
    }

    if normalized == "Cargo.toml"
        || normalized == "rust-toolchain.toml"
        || normalized.ends_with("/Cargo.toml")
    {
        return Some(OptimizationLever::CompilerConfig);
    }

    if normalized.starts_with("benches/")
        || normalized.contains("/benches/")
        || normalized.starts_with("crates/fsqlite-harness/src/")
    {
        return Some(OptimizationLever::BenchmarkHarness);
    }

    if normalized.starts_with("crates/fsqlite-parser/")
        || normalized.starts_with("crates/fsqlite-planner/")
        || normalized.starts_with("crates/fsqlite-vdbe/")
    {
        return Some(OptimizationLever::QueryPipeline);
    }

    if normalized.starts_with("crates/fsqlite-mvcc/") {
        return Some(OptimizationLever::Concurrency);
    }

    if normalized.starts_with("crates/fsqlite-vfs/")
        || normalized.starts_with("crates/fsqlite-wal/")
        || normalized.starts_with("crates/fsqlite-pager/")
    {
        return Some(OptimizationLever::IoPath);
    }

    if normalized.starts_with("crates/") || normalized.starts_with("src/") {
        return Some(OptimizationLever::ComputeKernel);
    }

    None
}

/// Infer optimization lever set from changed paths.
#[must_use]
pub fn infer_optimization_levers(paths: &[PathBuf]) -> BTreeSet<OptimizationLever> {
    paths
        .iter()
        .filter_map(|path| classify_optimization_lever(path))
        .collect()
}

/// Enforce the one-lever-per-change rule.
pub fn enforce_one_lever_rule(paths: &[PathBuf]) -> Result<OptimizationLever, PerfLoopError> {
    let levers = infer_optimization_levers(paths);
    match levers.len() {
        0 => Err(PerfLoopError::NoOptimizationLeverDetected),
        1 => {
            let lever = levers
                .iter()
                .next()
                .copied()
                .ok_or(PerfLoopError::NoOptimizationLeverDetected)?;
            Ok(lever)
        }
        _ => Err(PerfLoopError::MultipleOptimizationLevers { levers }),
    }
}

/// Enforce that baseline evidence is present and non-empty.
pub fn enforce_baseline_capture(baseline_artifact: &Path) -> Result<(), PerfLoopError> {
    let metadata = match fs::metadata(baseline_artifact) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(PerfLoopError::MissingBaselineArtifact {
                path: baseline_artifact.to_path_buf(),
            });
        }
        Err(error) => {
            return Err(PerfLoopError::Io {
                path: baseline_artifact.to_path_buf(),
                message: error.to_string(),
            });
        }
    };

    if !metadata.is_file() {
        return Err(PerfLoopError::MissingBaselineArtifact {
            path: baseline_artifact.to_path_buf(),
        });
    }

    if metadata.len() == 0 {
        return Err(PerfLoopError::EmptyBaselineArtifact {
            path: baseline_artifact.to_path_buf(),
        });
    }

    Ok(())
}

/// Capture SHA-256 checksums for all top-level files in a golden output directory.
pub fn capture_golden_checksums(
    golden_output_dir: &Path,
    checksum_file: &Path,
) -> Result<(), PerfLoopError> {
    let mut files = read_top_level_files_sorted(golden_output_dir)?;
    if files.is_empty() {
        return Err(PerfLoopError::MissingGoldenOutput {
            path: golden_output_dir.to_path_buf(),
        });
    }

    let mut output = String::new();
    for file in files.drain(..) {
        let digest = compute_sha256(&file)?;
        let file_name = file
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| PerfLoopError::Io {
                path: file.clone(),
                message: "invalid UTF-8 file name".to_string(),
            })?;
        output.push_str(&format!("{digest} *{file_name}\n"));
    }

    fs::write(checksum_file, output).map_err(|error| PerfLoopError::Io {
        path: checksum_file.to_path_buf(),
        message: error.to_string(),
    })
}

/// Verify SHA-256 checksums for golden outputs against a checksum file.
pub fn verify_golden_checksums(
    golden_output_dir: &Path,
    checksum_file: &Path,
) -> Result<(), PerfLoopError> {
    if !checksum_file.exists() {
        return Err(PerfLoopError::MissingGoldenChecksumFile {
            path: checksum_file.to_path_buf(),
        });
    }

    let raw = fs::read_to_string(checksum_file).map_err(|error| PerfLoopError::Io {
        path: checksum_file.to_path_buf(),
        message: error.to_string(),
    })?;

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (expected, file_name) =
            parse_checksum_line(trimmed).ok_or_else(|| PerfLoopError::InvalidChecksumLine {
                line: trimmed.to_string(),
            })?;
        let target = golden_output_dir.join(&file_name);

        if !target.exists() {
            return Err(PerfLoopError::MissingGoldenOutput { path: target });
        }

        let actual = compute_sha256(&target)?;
        if actual != expected {
            return Err(PerfLoopError::GoldenChecksumMismatch {
                path: target,
                expected,
                actual,
            });
        }
    }

    Ok(())
}

/// Validate that golden outputs include required conformance artifacts.
pub fn validate_conformance_artifacts_included(
    golden_output_dir: &Path,
) -> Result<(), PerfLoopError> {
    let files = read_top_level_files_sorted(golden_output_dir)?;
    let names: Vec<String> = files
        .iter()
        .filter_map(|path| path.file_name())
        .filter_map(std::ffi::OsStr::to_str)
        .map(str::to_ascii_lowercase)
        .collect();

    for required in REQUIRED_CONFORMANCE_ARTIFACT_NAMES {
        let required_lower = required.to_ascii_lowercase();
        let found = names.iter().any(|name| name.contains(&required_lower));
        if !found {
            return Err(PerfLoopError::MissingConformanceArtifact { name: required });
        }
    }
    Ok(())
}

/// Capture behavior-lock checksums and enforce conformance artifact coverage.
pub fn capture_behavior_lock_checksums(
    golden_output_dir: &Path,
    checksum_file: &Path,
) -> Result<(), PerfLoopError> {
    validate_conformance_artifacts_included(golden_output_dir)?;
    capture_golden_checksums(golden_output_dir, checksum_file)
}

/// CI gate for perf-only changes: conformance artifacts must be included and checksums must match.
pub fn enforce_behavior_lock_ci(
    perf_only_change: bool,
    golden_output_dir: &Path,
    checksum_file: &Path,
) -> Result<(), PerfLoopError> {
    if !perf_only_change {
        return Ok(());
    }
    validate_conformance_artifacts_included(golden_output_dir)?;
    verify_golden_checksums(golden_output_dir, checksum_file)
}

/// Combined §17.8.1 gate:
/// one-lever rule + baseline requirement + golden checksum verification.
pub fn enforce_extreme_optimization_loop(
    changed_paths: &[PathBuf],
    baseline_artifact: &Path,
    golden_output_dir: &Path,
    checksum_file: &Path,
) -> Result<OptimizationLever, PerfLoopError> {
    let lever = enforce_one_lever_rule(changed_paths)?;
    enforce_baseline_capture(baseline_artifact)?;
    verify_golden_checksums(golden_output_dir, checksum_file)?;
    Ok(lever)
}

/// Trace event used to capture deterministic schedule fingerprints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleEvent {
    pub actor: String,
    pub action: String,
}

/// Deterministic measurement report generated from a fixed seed and schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicMeasurement {
    pub scenario_id: String,
    pub seed: u64,
    pub trace_fingerprint: String,
    pub git_sha: String,
    pub env: BTreeMap<String, String>,
    pub metrics: BTreeMap<String, u64>,
    pub schedule: Vec<ScheduleEvent>,
}

/// Artifact envelope for deterministic measurement runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementArtifactBundle {
    pub trace_id: String,
    pub scenario_id: String,
    pub seed: u64,
    pub schedule_fingerprint: String,
    pub env_fingerprint: String,
    pub git_sha: String,
    pub measurement: DeterministicMeasurement,
}

/// Build required measurement environment metadata map.
#[must_use]
pub fn record_measurement_env(
    rustflags: &str,
    feature_flags: &str,
    mode: &str,
    git_sha: &str,
    platform: &str,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert("RUSTFLAGS".to_string(), rustflags.to_string());
    env.insert("FEATURE_FLAGS".to_string(), feature_flags.to_string());
    env.insert("MODE".to_string(), mode.to_string());
    env.insert("GIT_SHA".to_string(), git_sha.to_string());
    env.insert("PLATFORM".to_string(), platform.to_string());
    env
}

/// Validate required measurement environment metadata keys and values.
pub fn validate_measurement_env(env: &BTreeMap<String, String>) -> Result<(), PerfLoopError> {
    for key in REQUIRED_MEASUREMENT_ENV_KEYS {
        let value = env
            .get(key)
            .ok_or(PerfLoopError::MissingMeasurementMetadata { key })?;
        if value.trim().is_empty() {
            return Err(PerfLoopError::InvalidMeasurementMetadata { key });
        }
    }
    Ok(())
}

/// Compute deterministic schedule fingerprint for concurrent benchmarks.
pub fn compute_trace_fingerprint(schedule: &[ScheduleEvent]) -> Result<String, PerfLoopError> {
    if schedule.is_empty() {
        return Err(PerfLoopError::EmptySchedule);
    }

    let encoded = serde_json::to_vec(schedule).map_err(|error| PerfLoopError::Io {
        path: PathBuf::from("<schedule>"),
        message: error.to_string(),
    })?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

/// Validate `sha256:<hex>` fingerprint shape.
pub fn validate_trace_fingerprint(value: &str) -> Result<(), PerfLoopError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(PerfLoopError::InvalidTraceFingerprint {
            value: value.to_string(),
        });
    };
    if hex.len() != 64 || !hex.chars().all(|char| char.is_ascii_hexdigit()) {
        return Err(PerfLoopError::InvalidTraceFingerprint {
            value: value.to_string(),
        });
    }
    Ok(())
}

/// Replay a schedule only if it matches the expected fingerprint.
pub fn replay_schedule_from_fingerprint(
    schedule: &[ScheduleEvent],
    expected_fingerprint: &str,
) -> Result<Vec<ScheduleEvent>, PerfLoopError> {
    validate_trace_fingerprint(expected_fingerprint)?;
    let actual = compute_trace_fingerprint(schedule)?;
    if actual != expected_fingerprint.to_ascii_lowercase() {
        return Err(PerfLoopError::ScheduleFingerprintMismatch {
            expected: expected_fingerprint.to_ascii_lowercase(),
            actual,
        });
    }
    Ok(schedule.to_vec())
}

/// Produce deterministic benchmark metrics from a scenario id and seed.
#[must_use]
pub fn generate_seeded_metrics(scenario_id: &str, seed: u64) -> BTreeMap<String, u64> {
    let p50 = seeded_metric_value(scenario_id, seed, "p50", 1_000, 10_000);
    let p95 = p50 + seeded_metric_value(scenario_id, seed, "p95_delta", 50, 1_000);
    let p99 = p95 + seeded_metric_value(scenario_id, seed, "p99_delta", 20, 1_000);
    let throughput = seeded_metric_value(scenario_id, seed, "throughput", 500, 1_000_000);

    let mut metrics = BTreeMap::new();
    metrics.insert("p50_micros".to_string(), p50);
    metrics.insert("p95_micros".to_string(), p95);
    metrics.insert("p99_micros".to_string(), p99);
    metrics.insert("throughput_ops_per_sec".to_string(), throughput);
    metrics
}

/// Run deterministic measurement and emit reproducible metadata.
pub fn run_deterministic_measurement(
    scenario_id: &str,
    seed: u64,
    schedule: &[ScheduleEvent],
    env: &BTreeMap<String, String>,
) -> Result<DeterministicMeasurement, PerfLoopError> {
    if scenario_id.trim().is_empty() {
        return Err(PerfLoopError::InvalidMeasurementField {
            field: "scenario_id",
        });
    }
    validate_measurement_env(env)?;

    let trace_fingerprint = compute_trace_fingerprint(schedule)?;
    let git_sha = env
        .get("GIT_SHA")
        .cloned()
        .ok_or(PerfLoopError::MissingMeasurementMetadata { key: "GIT_SHA" })?;

    Ok(DeterministicMeasurement {
        scenario_id: scenario_id.to_string(),
        seed,
        trace_fingerprint,
        git_sha,
        env: env.clone(),
        metrics: generate_seeded_metrics(scenario_id, seed),
        schedule: schedule.to_vec(),
    })
}

/// Compute environment fingerprint for artifact bundle integrity.
pub fn compute_env_fingerprint(env: &BTreeMap<String, String>) -> Result<String, PerfLoopError> {
    validate_measurement_env(env)?;
    let encoded = serde_json::to_vec(env).map_err(|error| PerfLoopError::Io {
        path: PathBuf::from("<measurement-env>"),
        message: error.to_string(),
    })?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

/// Build deterministic artifact bundle metadata for reproducible benchmark runs.
pub fn build_measurement_artifact_bundle(
    measurement: &DeterministicMeasurement,
) -> Result<MeasurementArtifactBundle, PerfLoopError> {
    if measurement.scenario_id.trim().is_empty() {
        return Err(PerfLoopError::InvalidMeasurementField {
            field: "scenario_id",
        });
    }
    if measurement.metrics.is_empty() {
        return Err(PerfLoopError::InvalidMeasurementField { field: "metrics" });
    }

    validate_measurement_env(&measurement.env)?;
    validate_trace_fingerprint(&measurement.trace_fingerprint)?;

    let schedule_fingerprint = compute_trace_fingerprint(&measurement.schedule)?;
    if schedule_fingerprint != measurement.trace_fingerprint {
        return Err(PerfLoopError::ScheduleFingerprintMismatch {
            expected: measurement.trace_fingerprint.clone(),
            actual: schedule_fingerprint,
        });
    }

    let env_fingerprint = compute_env_fingerprint(&measurement.env)?;
    let trace_id = build_trace_id(
        &measurement.scenario_id,
        measurement.seed,
        &measurement.trace_fingerprint,
    );

    Ok(MeasurementArtifactBundle {
        trace_id,
        scenario_id: measurement.scenario_id.clone(),
        seed: measurement.seed,
        schedule_fingerprint: measurement.trace_fingerprint.clone(),
        env_fingerprint,
        git_sha: measurement.git_sha.clone(),
        measurement: measurement.clone(),
    })
}

/// Persist deterministic measurement artifact bundle to JSON.
pub fn write_measurement_artifact_bundle(
    output_path: &Path,
    bundle: &MeasurementArtifactBundle,
) -> Result<(), PerfLoopError> {
    let bytes = serde_json::to_vec_pretty(bundle).map_err(|error| PerfLoopError::Io {
        path: output_path.to_path_buf(),
        message: error.to_string(),
    })?;
    fs::write(output_path, bytes).map_err(|error| PerfLoopError::Io {
        path: output_path.to_path_buf(),
        message: error.to_string(),
    })
}

/// Opportunity matrix row used by perf optimization gate (§17.8.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpportunityMatrixEntry {
    pub hotspot: String,
    pub impact: u8,
    pub confidence: u8,
    pub effort: u8,
}

/// Serializable matrix artifact for optimization candidate scoring.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityMatrix {
    pub scenario_id: String,
    pub threshold: f64,
    pub entries: Vec<OpportunityMatrixEntry>,
}

/// Score decision generated for each opportunity matrix row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityDecision {
    pub hotspot: String,
    pub score: f64,
    pub threshold: f64,
    pub selected: bool,
}

/// Compute score for one row using the normative formula:
/// `score = (impact * confidence) / effort`.
pub fn compute_opportunity_score(entry: &OpportunityMatrixEntry) -> Result<f64, PerfLoopError> {
    validate_opportunity_entry(entry)?;
    if entry.hotspot.trim().is_empty() {
        return Ok(0.0);
    }

    Ok((f64::from(entry.impact) * f64::from(entry.confidence)) / f64::from(entry.effort))
}

/// Enforce `score >= threshold` for one optimization candidate.
pub fn enforce_opportunity_score_gate(
    entry: &OpportunityMatrixEntry,
    threshold: f64,
) -> Result<f64, PerfLoopError> {
    if !threshold.is_finite() || threshold <= 0.0 {
        return Err(PerfLoopError::InvalidOpportunityField { field: "threshold" });
    }

    let score = compute_opportunity_score(entry)?;
    if score < threshold {
        return Err(PerfLoopError::OpportunityScoreBelowThreshold {
            hotspot: entry.hotspot.clone(),
            score,
            threshold,
        });
    }
    Ok(score)
}

/// Enforce matrix presence for perf optimization gates.
pub fn enforce_opportunity_matrix_required(
    matrix: Option<&OpportunityMatrix>,
) -> Result<(), PerfLoopError> {
    let matrix = matrix.ok_or(PerfLoopError::MissingOpportunityMatrix)?;
    validate_opportunity_matrix(matrix)
}

/// Validate and score each matrix row.
pub fn evaluate_opportunity_matrix(
    matrix: &OpportunityMatrix,
) -> Result<Vec<OpportunityDecision>, PerfLoopError> {
    validate_opportunity_matrix(matrix)?;

    matrix
        .entries
        .iter()
        .map(|entry| {
            let score = compute_opportunity_score(entry)?;
            Ok(OpportunityDecision {
                hotspot: entry.hotspot.clone(),
                score,
                threshold: matrix.threshold,
                selected: score >= matrix.threshold,
            })
        })
        .collect()
}

/// Enforce the gate across all entries; any below-threshold entry fails.
pub fn enforce_opportunity_matrix_gate(
    matrix: &OpportunityMatrix,
) -> Result<Vec<OpportunityDecision>, PerfLoopError> {
    let decisions = evaluate_opportunity_matrix(matrix)?;
    for decision in &decisions {
        if decision.score < decision.threshold {
            return Err(PerfLoopError::OpportunityScoreBelowThreshold {
                hotspot: decision.hotspot.clone(),
                score: decision.score,
                threshold: decision.threshold,
            });
        }
    }
    Ok(decisions)
}

/// Validate opportunity matrix shape and field ranges.
pub fn validate_opportunity_matrix(matrix: &OpportunityMatrix) -> Result<(), PerfLoopError> {
    if matrix.scenario_id.trim().is_empty() {
        return Err(PerfLoopError::InvalidOpportunityField {
            field: "scenario_id",
        });
    }
    if !matrix.threshold.is_finite() || matrix.threshold <= 0.0 {
        return Err(PerfLoopError::InvalidOpportunityField { field: "threshold" });
    }
    if matrix.entries.is_empty() {
        return Err(PerfLoopError::MissingOpportunityEntries);
    }
    for entry in &matrix.entries {
        validate_opportunity_entry(entry)?;
    }
    Ok(())
}

/// Canonical command set for profiling cookbook runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfilingCookbookCommands {
    pub cpu_flamegraph: String,
    pub hyperfine_baseline: String,
    pub allocation_profile: String,
    pub syscall_census: String,
}

/// Toolchain presence result for profiling cookbook gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfilingToolchainReport {
    pub available: BTreeMap<String, String>,
    pub missing: Vec<String>,
}

/// Structured report for E2E profiling artifact validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfilingArtifactReport {
    pub trace_id: String,
    pub scenario_id: String,
    pub git_sha: String,
    pub artifact_paths: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
}

/// Build canonical profiling commands from scenario metadata.
#[must_use]
pub fn canonical_profiling_cookbook_commands(
    bench_name: &str,
    scenario_id: &str,
    command: &str,
) -> ProfilingCookbookCommands {
    ProfilingCookbookCommands {
        cpu_flamegraph: format!(
            "RUSTFLAGS='-C force-frame-pointers=yes' cargo flamegraph --bench {bench_name} -- --bench"
        ),
        hyperfine_baseline: format!(
            "hyperfine --warmup 3 --runs 10 --export-json baselines/hyperfine/{scenario_id}.json '{command}'"
        ),
        allocation_profile: format!("heaptrack {command}"),
        syscall_census: format!("strace -f -c -o baselines/syscalls/{scenario_id}.txt {command}"),
    }
}

/// Validate profiling command strings are non-empty and include required tooling.
pub fn validate_cookbook_commands_exist(
    commands: &ProfilingCookbookCommands,
) -> Result<(), PerfLoopError> {
    validate_non_empty(&commands.cpu_flamegraph, "cpu_flamegraph").map_err(|_| {
        PerfLoopError::InvalidProfilingField {
            field: "cpu_flamegraph",
        }
    })?;
    validate_non_empty(&commands.hyperfine_baseline, "hyperfine_baseline").map_err(|_| {
        PerfLoopError::InvalidProfilingField {
            field: "hyperfine_baseline",
        }
    })?;
    validate_non_empty(&commands.allocation_profile, "allocation_profile").map_err(|_| {
        PerfLoopError::InvalidProfilingField {
            field: "allocation_profile",
        }
    })?;
    validate_non_empty(&commands.syscall_census, "syscall_census").map_err(|_| {
        PerfLoopError::InvalidProfilingField {
            field: "syscall_census",
        }
    })?;

    let contains = |haystack: &str, needle: &str| haystack.contains(needle);
    if !contains(&commands.cpu_flamegraph, "cargo flamegraph")
        || !contains(&commands.hyperfine_baseline, "hyperfine")
        || !contains(&commands.allocation_profile, "heaptrack")
        || !contains(&commands.syscall_census, "strace")
    {
        return Err(PerfLoopError::InvalidProfilingField { field: "commands" });
    }

    Ok(())
}

/// Build canonical profiling metadata map for artifact bundles.
#[must_use]
pub fn record_profiling_metadata(
    git_sha: &str,
    scenario_with_params: &str,
    seed: &str,
    build_flags: &str,
    platform: &str,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("git_sha".to_string(), git_sha.to_string());
    metadata.insert("scenario".to_string(), scenario_with_params.to_string());
    metadata.insert("seed".to_string(), seed.to_string());
    metadata.insert("build_flags".to_string(), build_flags.to_string());
    metadata.insert("platform".to_string(), platform.to_string());
    metadata
}

/// Validate mandatory profiling metadata fields.
pub fn validate_profiling_metadata(
    metadata: &BTreeMap<String, String>,
) -> Result<(), PerfLoopError> {
    for key in REQUIRED_PROFILING_METADATA_KEYS {
        let value = metadata
            .get(key)
            .ok_or(PerfLoopError::MissingProfilingMetadata { key })?;
        if value.trim().is_empty() {
            return Err(PerfLoopError::InvalidProfilingField { field: key });
        }
    }
    Ok(())
}

/// Validate flamegraph output is a non-empty SVG file.
pub fn validate_flamegraph_output(path: &Path) -> Result<(), PerfLoopError> {
    let content = fs::read_to_string(path).map_err(|error| PerfLoopError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    if content.trim().is_empty() {
        return Err(PerfLoopError::InvalidProfilingField {
            field: "flamegraph_output",
        });
    }
    if !content.contains("<svg") {
        return Err(PerfLoopError::InvalidProfilingField {
            field: "flamegraph_svg",
        });
    }
    Ok(())
}

/// Validate hyperfine JSON output has the expected shape.
pub fn validate_hyperfine_json_output(path: &Path) -> Result<(), PerfLoopError> {
    let content = fs::read_to_string(path).map_err(|error| PerfLoopError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let value: serde_json::Value =
        serde_json::from_str(&content).map_err(|error| PerfLoopError::Io {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    if value.get("results").is_none() {
        return Err(PerfLoopError::InvalidProfilingField {
            field: "hyperfine.results",
        });
    }
    if value.get("command").is_none() {
        return Err(PerfLoopError::InvalidProfilingField {
            field: "hyperfine.command",
        });
    }
    Ok(())
}

/// Resolve a profiling tool version string, if installed.
#[must_use]
pub fn resolve_profiling_tool_version(tool: &str) -> Option<String> {
    let (program, args): (&str, &[&str]) = match tool {
        "cargo-flamegraph" => ("cargo-flamegraph", &["--version"]),
        "hyperfine" => ("hyperfine", &["--version"]),
        "heaptrack" => ("heaptrack", &["--version"]),
        "strace" => ("strace", &["-V"]),
        _ => return None,
    };

    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return Some(stdout);
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return Some(stderr);
    }
    Some("available".to_string())
}

/// Evaluate profiling tool availability using a resolver callback.
pub fn check_profiling_toolchain_with<F>(resolver: F) -> ProfilingToolchainReport
where
    F: Fn(&str) -> Option<String>,
{
    let mut available = BTreeMap::new();
    let mut missing = Vec::new();

    for tool in REQUIRED_PROFILING_TOOLS {
        if let Some(version) = resolver(tool) {
            available.insert(tool.to_string(), version);
        } else {
            missing.push(tool.to_string());
        }
    }

    ProfilingToolchainReport { available, missing }
}

/// Enforce profiling tool presence gate using the default resolver.
pub fn enforce_profiling_toolchain_presence() -> Result<ProfilingToolchainReport, PerfLoopError> {
    enforce_profiling_toolchain_presence_with(resolve_profiling_tool_version)
}

/// Enforce profiling tool presence gate using a custom resolver (test-friendly).
pub fn enforce_profiling_toolchain_presence_with<F>(
    resolver: F,
) -> Result<ProfilingToolchainReport, PerfLoopError>
where
    F: Fn(&str) -> Option<String>,
{
    let report = check_profiling_toolchain_with(resolver);
    if let Some(tool) = report.missing.first() {
        return Err(PerfLoopError::ToolUnavailable {
            tool: tool.clone(),
            remediation: format!("install `{tool}` and ensure it is on PATH"),
        });
    }
    Ok(report)
}

/// Validate a profiling artifact report schema and required metadata.
pub fn validate_profiling_artifact_report(
    report: &ProfilingArtifactReport,
) -> Result<(), PerfLoopError> {
    if report.trace_id.trim().is_empty() {
        return Err(PerfLoopError::InvalidProfilingField { field: "trace_id" });
    }
    if report.scenario_id.trim().is_empty() {
        return Err(PerfLoopError::InvalidProfilingField {
            field: "scenario_id",
        });
    }
    if report.git_sha.trim().is_empty() {
        return Err(PerfLoopError::InvalidProfilingField { field: "git_sha" });
    }
    validate_profiling_metadata(&report.metadata)?;
    for key in REQUIRED_PROFILING_ARTIFACT_KEYS {
        let value = report
            .artifact_paths
            .get(key)
            .ok_or(PerfLoopError::MissingProfilingArtifactPath { key })?;
        if value.trim().is_empty() {
            return Err(PerfLoopError::InvalidProfilingField { field: key });
        }
    }
    Ok(())
}

/// Validate artifact paths exist under a report root directory.
pub fn validate_profiling_artifact_paths(
    root: &Path,
    report: &ProfilingArtifactReport,
) -> Result<(), PerfLoopError> {
    validate_profiling_artifact_report(report)?;
    for key in REQUIRED_PROFILING_ARTIFACT_KEYS {
        let relative = report
            .artifact_paths
            .get(key)
            .ok_or(PerfLoopError::MissingProfilingArtifactPath { key })?;
        let absolute = root.join(relative);
        if !absolute.exists() {
            return Err(PerfLoopError::MissingProfilingArtifactPath { key });
        }
    }
    Ok(())
}

/// Canonical perf smoke report schema (§17.8.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerfSmokeReport {
    pub generated_at: String,
    pub scenario_id: String,
    pub command: String,
    pub seed: String,
    pub trace_fingerprint: String,
    pub git_sha: String,
    pub config_hash: String,
    pub alpha_total: f64,
    pub alpha_policy: String,
    pub metric_count: u64,
    pub artifacts: PerfSmokeArtifacts,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    pub system: PerfSmokeSystem,
}

/// Artifact pointers attached to a smoke report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfSmokeArtifacts {
    pub criterion_dir: String,
    pub baseline_path: String,
    pub latest_path: String,
}

/// Host metadata attached to a smoke report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfSmokeSystem {
    pub os: String,
    pub arch: String,
    pub kernel: String,
}

/// Ensure baseline layout exists by creating required subdirectories.
pub fn ensure_baseline_layout(root: &Path) -> Result<(), PerfLoopError> {
    for name in REQUIRED_BASELINE_DIRS {
        let directory = root.join(name);
        fs::create_dir_all(&directory).map_err(|error| PerfLoopError::Io {
            path: directory,
            message: error.to_string(),
        })?;
    }
    Ok(())
}

/// Validate baseline layout contains all required subdirectories.
pub fn validate_baseline_layout(root: &Path) -> Result<(), PerfLoopError> {
    for name in REQUIRED_BASELINE_DIRS {
        let directory = root.join(name);
        let metadata =
            fs::metadata(&directory).map_err(|_| PerfLoopError::MissingBaselineDirectory {
                path: directory.clone(),
            })?;
        if !metadata.is_dir() {
            return Err(PerfLoopError::MissingBaselineDirectory { path: directory });
        }
    }
    Ok(())
}

/// Validate normative perf smoke report fields (§17.8.4).
pub fn validate_perf_smoke_report(report: &PerfSmokeReport) -> Result<(), PerfLoopError> {
    validate_non_empty(&report.generated_at, "generated_at")?;
    validate_non_empty(&report.scenario_id, "scenario_id")?;
    validate_non_empty(&report.command, "command")?;
    validate_non_empty(&report.seed, "seed")?;
    validate_non_empty(&report.trace_fingerprint, "trace_fingerprint")?;
    validate_non_empty(&report.git_sha, "git_sha")?;
    validate_non_empty(&report.config_hash, "config_hash")?;
    validate_non_empty(&report.alpha_policy, "alpha_policy")?;
    validate_non_empty(&report.artifacts.criterion_dir, "artifacts.criterion_dir")?;
    validate_non_empty(&report.artifacts.baseline_path, "artifacts.baseline_path")?;
    validate_non_empty(&report.artifacts.latest_path, "artifacts.latest_path")?;
    validate_non_empty(&report.system.os, "system.os")?;
    validate_non_empty(&report.system.arch, "system.arch")?;
    validate_non_empty(&report.system.kernel, "system.kernel")?;

    if !report.generated_at.contains('T') {
        return Err(PerfLoopError::InvalidSmokeReportField {
            field: "generated_at",
        });
    }

    if report.alpha_total <= 0.0 || report.alpha_total > 1.0 || !report.alpha_total.is_finite() {
        return Err(PerfLoopError::InvalidSmokeReportField {
            field: "alpha_total",
        });
    }

    if report.metric_count == 0 {
        return Err(PerfLoopError::InvalidSmokeReportField {
            field: "metric_count",
        });
    }

    Ok(())
}

/// Write `.gitkeep` placeholders into required baseline directories.
pub fn write_baseline_gitkeep_files(root: &Path) -> Result<(), PerfLoopError> {
    ensure_baseline_layout(root)?;
    for name in REQUIRED_BASELINE_DIRS {
        let keep_path = root.join(name).join(".gitkeep");
        if !keep_path.exists() {
            fs::write(&keep_path, "").map_err(|error| PerfLoopError::Io {
                path: keep_path.clone(),
                message: error.to_string(),
            })?;
        }
    }
    Ok(())
}

/// Read a smoke report JSON file and validate required fields.
pub fn load_and_validate_smoke_report(path: &Path) -> Result<PerfSmokeReport, PerfLoopError> {
    let raw = fs::read_to_string(path).map_err(|error| PerfLoopError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let report: PerfSmokeReport =
        serde_json::from_str(&raw).map_err(|error| PerfLoopError::Io {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    validate_perf_smoke_report(&report)?;
    Ok(report)
}

fn validate_non_empty(value: &str, field: &'static str) -> Result<(), PerfLoopError> {
    if value.trim().is_empty() {
        return Err(PerfLoopError::InvalidSmokeReportField { field });
    }
    Ok(())
}

fn parse_checksum_line(line: &str) -> Option<(String, String)> {
    let (digest, remainder) = line.split_once(char::is_whitespace)?;
    if digest.len() != 64 || !digest.chars().all(|char| char.is_ascii_hexdigit()) {
        return None;
    }
    let file = remainder
        .trim_start()
        .strip_prefix('*')
        .unwrap_or(remainder);
    if file.is_empty() {
        return None;
    }
    Some((digest.to_ascii_lowercase(), file.to_string()))
}

fn read_top_level_files_sorted(dir: &Path) -> Result<Vec<PathBuf>, PerfLoopError> {
    let mut paths = Vec::new();
    let entries = fs::read_dir(dir).map_err(|error| PerfLoopError::Io {
        path: dir.to_path_buf(),
        message: error.to_string(),
    })?;

    for entry_result in entries {
        let entry = entry_result.map_err(|error| PerfLoopError::Io {
            path: dir.to_path_buf(),
            message: error.to_string(),
        })?;
        let path = entry.path();
        if path.is_file() {
            paths.push(path);
        }
    }

    paths.sort();
    Ok(paths)
}

fn compute_sha256(path: &Path) -> Result<String, PerfLoopError> {
    let output = Command::new("sha256sum")
        .arg("--binary")
        .arg(path)
        .output()
        .map_err(|error| PerfLoopError::Io {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    if !output.status.success() {
        return Err(PerfLoopError::DigestCommandFailed {
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let digest = stdout.split_whitespace().next().ok_or_else(|| {
        PerfLoopError::DigestCommandOutputMalformed {
            path: path.to_path_buf(),
        }
    })?;

    if digest.len() != 64 || !digest.chars().all(|char| char.is_ascii_hexdigit()) {
        return Err(PerfLoopError::DigestCommandOutputMalformed {
            path: path.to_path_buf(),
        });
    }

    Ok(digest.to_ascii_lowercase())
}

fn validate_opportunity_entry(entry: &OpportunityMatrixEntry) -> Result<(), PerfLoopError> {
    if entry.impact == 0 || entry.impact > 5 {
        return Err(PerfLoopError::InvalidOpportunityField { field: "impact" });
    }
    if entry.effort == 0 || entry.effort > 5 {
        return Err(PerfLoopError::InvalidOpportunityField { field: "effort" });
    }
    if entry.confidence > 5 {
        return Err(PerfLoopError::InvalidOpportunityField {
            field: "confidence",
        });
    }
    if entry.hotspot.trim().is_empty() && entry.confidence != 0 {
        return Err(PerfLoopError::InvalidOpportunityField {
            field: "confidence",
        });
    }
    Ok(())
}

fn seeded_metric_value(scenario_id: &str, seed: u64, metric: &str, min: u64, span: u64) -> u64 {
    debug_assert!(span > 0);

    let mut hasher = Sha256::new();
    hasher.update(scenario_id.as_bytes());
    hasher.update(seed.to_le_bytes());
    hasher.update(metric.as_bytes());

    let digest = hasher.finalize();
    let mut bytes = [0_u8; std::mem::size_of::<u64>()];
    bytes.copy_from_slice(&digest[..std::mem::size_of::<u64>()]);
    min + (u64::from_le_bytes(bytes) % span)
}

fn build_trace_id(scenario_id: &str, seed: u64, schedule_fingerprint: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scenario_id.as_bytes());
    hasher.update(seed.to_le_bytes());
    hasher.update(schedule_fingerprint.as_bytes());
    let digest = hasher.finalize();
    let short = bytes_to_hex(&digest[..8]);
    format!("trace-{short}")
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to String cannot fail.
        let _ignored = write!(&mut output, "{byte:02x}");
    }
    output
}
