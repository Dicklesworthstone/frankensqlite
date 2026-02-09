//! Deterministic harness logging and repro-bundle utilities (`bd-1fpm`).
//!
//! This module defines a single logging standard for test runners:
//! - `meta.json` for run metadata
//! - `events.jsonl` for structured lifecycle events
//! - `stdout.log` / `stderr.log` for text streams
//! - optional engine artifacts (DB/WAL/SHM, oracle diffs, etc.)

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use fsqlite_error::{FrankenError, Result};
use fsqlite_vfs::host_fs;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

/// Version of the harness logging schema.
pub const LOG_SCHEMA_VERSION: u32 = 1;

/// Files that must be present in every repro bundle.
pub const REQUIRED_BUNDLE_FILES: [&str; 4] =
    ["meta.json", "events.jsonl", "stdout.log", "stderr.log"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventKind {
    RunStart,
    Setup,
    Step,
    Assertion,
    Teardown,
    RunEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleMeta {
    pub schema_version: u32,
    pub suite: String,
    pub case_id: String,
    pub seed: u64,
    pub harness_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvent {
    pub kind: LifecycleEventKind,
    pub status: Option<RunStatus>,
    pub step: u64,
    pub message: String,
    pub payload: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceDiff {
    pub case_id: String,
    pub sql: String,
    pub params: String,
    pub oracle_result: String,
    pub franken_result: String,
    pub diff: String,
}

/// Baseline measurement artifact required before optimization commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfBaselineArtifact {
    pub trace_id: String,
    pub scenario_id: String,
    pub git_sha: String,
    pub artifact_paths: Vec<String>,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub throughput_ops_per_sec: u64,
    pub alloc_count: u64,
}

/// Result of validating the mandatory optimization loop gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerfOptimizationGateReport {
    pub lever_keys: Vec<String>,
    pub baseline: Option<PerfBaselineArtifact>,
    pub golden_before_sha256: Option<String>,
    pub golden_after_sha256: Option<String>,
}

#[derive(Debug)]
pub struct ReproBundle {
    root: PathBuf,
    events_path: PathBuf,
    next_step: u64,
}

impl ReproBundle {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn emit_event(
        &mut self,
        kind: LifecycleEventKind,
        message: impl Into<String>,
        payload: BTreeMap<String, Value>,
    ) -> Result<()> {
        let event = HarnessEvent {
            kind,
            status: None,
            step: self.next_step,
            message: message.into(),
            payload,
        };
        self.next_step = self.next_step.saturating_add(1);
        self.write_event_line(&event)
    }

    pub fn append_stdout(&self, text: &str) -> Result<()> {
        append_line(&self.root.join("stdout.log"), text)
    }

    pub fn append_stderr(&self, text: &str) -> Result<()> {
        append_line(&self.root.join("stderr.log"), text)
    }

    pub fn write_artifact_json<T: Serialize>(
        &self,
        relative_path: &str,
        value: &T,
    ) -> Result<PathBuf> {
        let artifact_path = self.root.join(relative_path);
        if let Some(parent) = artifact_path.parent() {
            host_fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|err| internal_error(format!("failed to serialize artifact JSON: {err}")))?;
        host_fs::write(&artifact_path, bytes)?;
        Ok(artifact_path)
    }

    pub fn record_conformance_diff(&mut self, diff: &ConformanceDiff) -> Result<PathBuf> {
        let artifact_path = self.write_artifact_json("oracle_diff.json", diff)?;

        let mut payload = BTreeMap::new();
        payload.insert("case_id".to_string(), Value::String(diff.case_id.clone()));
        payload.insert("sql".to_string(), Value::String(diff.sql.clone()));
        payload.insert("params".to_string(), Value::String(diff.params.clone()));
        payload.insert(
            "oracle_result".to_string(),
            Value::String(diff.oracle_result.clone()),
        );
        payload.insert(
            "franken_result".to_string(),
            Value::String(diff.franken_result.clone()),
        );
        payload.insert("diff".to_string(), Value::String(diff.diff.clone()));

        self.emit_event(LifecycleEventKind::Assertion, "oracle_diff", payload)?;
        Ok(artifact_path)
    }

    pub fn finish(self, status: RunStatus) -> Result<PathBuf> {
        let event = HarnessEvent {
            kind: LifecycleEventKind::RunEnd,
            status: Some(status),
            step: self.next_step,
            message: "run_end".to_string(),
            payload: BTreeMap::new(),
        };
        self.write_event_line(&event)?;
        info!(
            suite = %self.root.display(),
            status = ?status,
            "harness repro bundle finalized"
        );
        Ok(self.root)
    }

    fn write_event_line(&self, event: &HarnessEvent) -> Result<()> {
        let encoded = serde_json::to_string(event)
            .map_err(|err| internal_error(format!("failed to serialize harness event: {err}")))?;
        host_fs::append_line(&self.events_path, &encoded)?;
        Ok(())
    }
}

pub fn init_repro_bundle(
    base_dir: &Path,
    suite: &str,
    case_id: &str,
    seed: u64,
) -> Result<ReproBundle> {
    if suite.is_empty() {
        return Err(internal_error("suite must be non-empty"));
    }
    if case_id.is_empty() {
        return Err(internal_error("case_id must be non-empty"));
    }

    let bundle_name = bundle_dir_name(suite, case_id, seed);
    let root = base_dir.join(bundle_name);
    host_fs::create_dir_all(&root)?;

    let meta = BundleMeta {
        schema_version: LOG_SCHEMA_VERSION,
        suite: suite.to_string(),
        case_id: case_id.to_string(),
        seed,
        harness_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    write_json_file(&root.join("meta.json"), &meta)?;

    host_fs::create_empty_file(&root.join("stdout.log"))?;
    host_fs::create_empty_file(&root.join("stderr.log"))?;

    let events_path = root.join("events.jsonl");
    host_fs::create_empty_file(&events_path)?;

    let mut bundle = ReproBundle {
        root,
        events_path,
        next_step: 0,
    };
    bundle.emit_event(LifecycleEventKind::RunStart, "run_start", BTreeMap::new())?;

    info!(
        suite = suite,
        case_id = case_id,
        seed = seed,
        root = %bundle.root.display(),
        "harness repro bundle initialized"
    );

    Ok(bundle)
}

pub fn validate_required_files(bundle_root: &Path) -> Result<()> {
    let missing: Vec<&str> = REQUIRED_BUNDLE_FILES
        .iter()
        .copied()
        .filter(|name| !bundle_root.join(name).is_file())
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    error!(
        bundle = %bundle_root.display(),
        missing_count = missing.len(),
        "missing required repro bundle files"
    );
    Err(internal_error(format!(
        "missing required bundle files: {}",
        missing.join(", ")
    )))
}

pub fn validate_bundle_meta(bundle_root: &Path) -> Result<BundleMeta> {
    let meta_path = bundle_root.join("meta.json");
    let bytes = host_fs::read(&meta_path)?;
    let meta: BundleMeta = serde_json::from_slice(&bytes)
        .map_err(|err| internal_error(format!("meta.json parse failure: {err}")))?;

    if meta.schema_version != LOG_SCHEMA_VERSION {
        warn!(
            expected = LOG_SCHEMA_VERSION,
            found = meta.schema_version,
            "bundle schema version mismatch"
        );
        return Err(internal_error(format!(
            "unsupported schema version: expected {LOG_SCHEMA_VERSION}, got {}",
            meta.schema_version
        )));
    }

    if meta.suite.is_empty() || meta.case_id.is_empty() {
        return Err(internal_error(
            "meta.json must include non-empty suite and case_id",
        ));
    }

    Ok(meta)
}

pub fn validate_events_jsonl(bundle_root: &Path) -> Result<Vec<HarnessEvent>> {
    let events_path = bundle_root.join("events.jsonl");
    let contents = host_fs::read_to_string(&events_path)?;
    let mut events = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            return Err(internal_error(format!(
                "events.jsonl has empty line at {}",
                line_no + 1
            )));
        }
        let event: HarnessEvent = serde_json::from_str(line).map_err(|err| {
            internal_error(format!(
                "events.jsonl parse failure at line {}: {err}",
                line_no + 1
            ))
        })?;
        if event.message.is_empty() {
            return Err(internal_error(format!(
                "events.jsonl has empty message at line {}",
                line_no + 1
            )));
        }
        events.push(event);
    }

    if events.is_empty() {
        return Err(internal_error(
            "events.jsonl must contain at least one event",
        ));
    }

    Ok(events)
}

pub fn validate_bundle(bundle_root: &Path) -> Result<()> {
    validate_required_files(bundle_root)?;
    let _meta = validate_bundle_meta(bundle_root)?;
    let events = validate_events_jsonl(bundle_root)?;

    if events.first().map(|event| event.kind) != Some(LifecycleEventKind::RunStart) {
        return Err(internal_error(
            "events.jsonl must start with a run_start event",
        ));
    }
    if events.last().map(|event| event.kind) != Some(LifecycleEventKind::RunEnd) {
        return Err(internal_error("events.jsonl must end with a run_end event"));
    }

    Ok(())
}

/// Detect optimization "lever keys" from changed paths using CI-friendly
/// git-diff heuristics.
///
/// Current heuristic:
/// - only `.rs` source files under `crates/<crate>/src/**` are considered
/// - each crate touched in this region counts as one optimization lever
#[must_use]
pub fn detect_optimization_levers(changed_paths: &[PathBuf]) -> Vec<String> {
    let mut levers = BTreeSet::new();
    for path in changed_paths {
        if let Some(lever) = optimization_lever_key(path) {
            levers.insert(lever);
        }
    }
    levers.into_iter().collect()
}

/// Parse and validate a baseline artifact captured before an optimization.
pub fn validate_perf_baseline_artifact(artifact_path: &Path) -> Result<PerfBaselineArtifact> {
    let bytes = host_fs::read(artifact_path)?;
    let baseline: PerfBaselineArtifact = serde_json::from_slice(&bytes)
        .map_err(|err| internal_error(format!("perf baseline parse failure: {err}")))?;
    validate_perf_baseline_fields(&baseline)?;
    Ok(baseline)
}

/// Enforce the strict optimization loop gates for performance-sensitive
/// changes:
/// 1) one optimization lever per commit,
/// 2) mandatory baseline artifact,
/// 3) golden-output checksum lock unchanged.
pub fn validate_perf_optimization_loop(
    changed_paths: &[PathBuf],
    baseline_artifact_path: Option<&Path>,
    golden_before_path: Option<&Path>,
    golden_after_path: Option<&Path>,
) -> Result<PerfOptimizationGateReport> {
    let lever_keys = detect_optimization_levers(changed_paths);

    if lever_keys.len() > 1 {
        return Err(internal_error(format!(
            "multiple optimization levers detected in one change set: {}",
            lever_keys.join(", ")
        )));
    }

    if lever_keys.is_empty() {
        return Ok(PerfOptimizationGateReport {
            lever_keys,
            baseline: None,
            golden_before_sha256: None,
            golden_after_sha256: None,
        });
    }

    let baseline_path = baseline_artifact_path
        .ok_or_else(|| internal_error("missing baseline artifact for optimization change set"))?;
    let baseline = validate_perf_baseline_artifact(baseline_path)?;

    let golden_before = golden_before_path
        .ok_or_else(|| internal_error("missing golden-before artifact for optimization change"))?;
    let golden_after = golden_after_path
        .ok_or_else(|| internal_error("missing golden-after artifact for optimization change"))?;

    let golden_before_sha256 = sha256_file_hex(golden_before)?;
    let golden_after_sha256 = sha256_file_hex(golden_after)?;

    if golden_before_sha256 != golden_after_sha256 {
        return Err(internal_error(format!(
            "golden checksum mismatch after optimization: before={golden_before_sha256} after={golden_after_sha256}",
        )));
    }

    Ok(PerfOptimizationGateReport {
        lever_keys,
        baseline: Some(baseline),
        golden_before_sha256: Some(golden_before_sha256),
        golden_after_sha256: Some(golden_after_sha256),
    })
}

fn validate_perf_baseline_fields(baseline: &PerfBaselineArtifact) -> Result<()> {
    if baseline.trace_id.is_empty() {
        return Err(internal_error(
            "perf baseline artifact requires non-empty trace_id",
        ));
    }
    if baseline.scenario_id.is_empty() {
        return Err(internal_error(
            "perf baseline artifact requires non-empty scenario_id",
        ));
    }
    if baseline.git_sha.is_empty() {
        return Err(internal_error(
            "perf baseline artifact requires non-empty git_sha",
        ));
    }
    if baseline.artifact_paths.is_empty() {
        return Err(internal_error(
            "perf baseline artifact requires at least one artifact path",
        ));
    }
    if baseline.artifact_paths.iter().any(String::is_empty) {
        return Err(internal_error(
            "perf baseline artifact contains empty artifact path",
        ));
    }
    if baseline.p50_micros == 0 || baseline.p95_micros == 0 || baseline.p99_micros == 0 {
        return Err(internal_error(
            "perf baseline artifact requires non-zero p50/p95/p99",
        ));
    }
    if baseline.p50_micros > baseline.p95_micros || baseline.p95_micros > baseline.p99_micros {
        return Err(internal_error(
            "perf baseline artifact must satisfy p50 <= p95 <= p99",
        ));
    }
    if baseline.throughput_ops_per_sec == 0 {
        return Err(internal_error(
            "perf baseline artifact requires non-zero throughput_ops_per_sec",
        ));
    }

    Ok(())
}

fn optimization_lever_key(path: &Path) -> Option<String> {
    if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
        return None;
    }

    let components: Vec<&str> = path
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(segment) => segment.to_str(),
            _ => None,
        })
        .collect();

    for idx in 0..components.len().saturating_sub(2) {
        if components[idx] == "crates" && components[idx + 2] == "src" {
            return Some(components[idx + 1].to_string());
        }
    }

    None
}

fn sha256_file_hex(path: &Path) -> Result<String> {
    let bytes = host_fs::read(path)?;
    Ok(sha256_hex(&bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let high = usize::from(byte >> 4);
        let low = usize::from(byte & 0x0F);
        out.push(char::from(HEX[high]));
        out.push(char::from(HEX[low]));
    }
    out
}

fn append_line(path: &Path, text: &str) -> Result<()> {
    host_fs::append_line(path, text)?;
    Ok(())
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|err| internal_error(format!("failed to serialize JSON: {err}")))?;
    host_fs::write(path, bytes)?;
    Ok(())
}

fn bundle_dir_name(suite: &str, case_id: &str, seed: u64) -> String {
    format!(
        "{}-{}-seed-{seed}",
        sanitize_segment(suite),
        sanitize_segment(case_id)
    )
}

fn sanitize_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn internal_error(message: impl Into<String>) -> FrankenError {
    FrankenError::Internal(message.into())
}
