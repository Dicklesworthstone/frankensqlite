//! Failure replay/minimization harness and operator triage UX (bd-1dp9.7.4).
//!
//! Orchestrates the pipeline: load artifact manifest → decode JSONL logs →
//! extract first-divergence events → replay with deterministic seed →
//! minimize mismatches → render operator triage report.
//!
//! Acceptance: <5 minute operator path from failure to minimal reproducer.
//!
//! # Pipeline
//!
//! 1. **Ingest**: Load `ArtifactManifest` and JSONL log artifacts from a CI gate run.
//! 2. **Decode**: Parse JSONL into `LogEventSchema` events, validate against schema.
//! 3. **Extract**: Find `FirstDivergence` events and reconstruct failure context.
//! 4. **Replay**: Build deterministic replay configuration from `BisectRequest` or log events.
//! 5. **Triage**: Render operator-facing diagnostics with first-divergence highlighting.

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;

use serde::{Deserialize, Serialize};

use crate::ci_gate_matrix::{ArtifactManifest, BisectRequest};
use crate::e2e_log_schema::{LogEventSchema, LogEventType};
use crate::log_schema_validator::{
    DecodedStream, ValidationReport, decode_jsonl_stream, validate_event_stream,
};

#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.7.4";

// ---- Replay Configuration ----

/// Configuration for replaying a failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayConfig {
    /// Deterministic seed for exact reproduction.
    pub seed: u64,
    /// Scenario identifier to replay.
    pub scenario_id: String,
    /// Command to reproduce the failure.
    pub replay_command: String,
    /// CI lane where the failure was detected.
    pub lane: String,
    /// Git SHA of the failing commit.
    pub git_sha: String,
    /// Optional known-good commit for bisection range.
    pub good_commit: Option<String>,
    /// Original run_id for log correlation.
    pub run_id: String,
}

impl ReplayConfig {
    /// Build a replay config from a `BisectRequest`.
    #[must_use]
    pub fn from_bisect_request(request: &BisectRequest, run_id: &str) -> Self {
        Self {
            seed: request.replay_seed,
            scenario_id: request.failing_gate.clone(),
            replay_command: request.replay_command.clone(),
            lane: request.lane.clone(),
            git_sha: request.bad_commit.clone(),
            good_commit: Some(request.good_commit.clone()),
            run_id: run_id.to_owned(),
        }
    }

    /// Build a replay config from log event metadata.
    #[must_use]
    pub fn from_log_event(event: &LogEventSchema, lane: &str) -> Self {
        Self {
            seed: event.seed.unwrap_or(0),
            scenario_id: event.scenario_id.clone().unwrap_or_default(),
            replay_command: format!(
                "cargo test -p fsqlite-harness -- {}",
                event.scenario_id.as_deref().unwrap_or("unknown"),
            ),
            lane: lane.to_owned(),
            git_sha: String::new(),
            good_commit: None,
            run_id: event.run_id.clone(),
        }
    }
}

// ---- First Divergence Extraction ----

/// A first-divergence event extracted from a log stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractedDivergence {
    /// Index in the event stream where divergence was detected.
    pub event_index: usize,
    /// Scenario that diverged.
    pub scenario_id: String,
    /// Run identifier for correlation.
    pub run_id: String,
    /// Seed used.
    pub seed: u64,
    /// Backend (fsqlite, sqlite, both).
    pub backend: String,
    /// Free-form divergence point description.
    pub divergence_point: String,
    /// Artifact paths associated with divergence evidence.
    pub artifact_paths: Vec<String>,
    /// Original timestamp.
    pub timestamp: String,
}

/// Extract all first-divergence events from a decoded log stream.
#[must_use]
pub fn extract_divergences(events: &[LogEventSchema]) -> Vec<ExtractedDivergence> {
    events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.event_type == LogEventType::FirstDivergence)
        .map(|(i, e)| {
            let divergence_point = e
                .context
                .get("divergence_point")
                .cloned()
                .unwrap_or_default();
            let artifact_paths: Vec<String> = e
                .context
                .get("artifact_paths")
                .map(|p| p.split(',').map(|s| s.trim().to_owned()).collect())
                .unwrap_or_default();

            ExtractedDivergence {
                event_index: i,
                scenario_id: e.scenario_id.clone().unwrap_or_default(),
                run_id: e.run_id.clone(),
                seed: e.seed.unwrap_or(0),
                backend: e.backend.clone().unwrap_or_else(|| "unknown".to_owned()),
                divergence_point,
                artifact_paths,
                timestamp: e.timestamp.clone(),
            }
        })
        .collect()
}

/// Extract failure events (Fail, Error) from a log stream.
#[must_use]
pub fn extract_failures(events: &[LogEventSchema]) -> Vec<(usize, &LogEventSchema)> {
    events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.event_type == LogEventType::Fail || e.event_type == LogEventType::Error)
        .collect()
}

// ---- Triage Session ----

/// Result of processing a CI gate run for triage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriageSession {
    /// Bead identifier for this triage.
    pub bead_id: String,
    /// Original manifest from CI gate run.
    pub manifest_summary: ManifestSummary,
    /// Schema validation result.
    pub validation_passed: bool,
    /// Total events decoded from logs.
    pub total_events: usize,
    /// Decode errors encountered.
    pub decode_errors: usize,
    /// Extracted divergences.
    pub divergences: Vec<ExtractedDivergence>,
    /// Extracted failures (event indices).
    pub failure_indices: Vec<usize>,
    /// Replay configuration (if constructible).
    pub replay_config: Option<ReplayConfig>,
    /// Phase distribution in log events.
    pub phase_distribution: BTreeMap<String, usize>,
    /// Event type distribution.
    pub event_type_distribution: BTreeMap<String, usize>,
    /// Schema validation diagnostics count.
    pub validation_errors: usize,
    pub validation_warnings: usize,
}

/// Compact summary of the artifact manifest for triage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSummary {
    pub run_id: String,
    pub lane: String,
    pub git_sha: String,
    pub seed: u64,
    pub gate_passed: bool,
    pub artifact_count: usize,
    pub has_bisect_request: bool,
}

impl ManifestSummary {
    #[must_use]
    pub fn from_manifest(manifest: &ArtifactManifest) -> Self {
        Self {
            run_id: manifest.run_id.clone(),
            lane: manifest.lane.clone(),
            git_sha: manifest.git_sha.clone(),
            seed: manifest.seed,
            gate_passed: manifest.gate_passed,
            artifact_count: manifest.artifacts.len(),
            has_bisect_request: manifest.bisect_request.is_some(),
        }
    }
}

/// Build a triage session from a manifest and JSONL log content.
#[must_use]
pub fn build_triage_session(manifest: &ArtifactManifest, jsonl_content: &str) -> TriageSession {
    let decoded: DecodedStream = decode_jsonl_stream(jsonl_content);
    let report: ValidationReport = validate_event_stream(&decoded.events);

    let divergences = extract_divergences(&decoded.events);
    let failures = extract_failures(&decoded.events);
    let failure_indices: Vec<usize> = failures.iter().map(|(i, _)| *i).collect();

    // Build phase distribution
    let mut phase_distribution = BTreeMap::new();
    for event in &decoded.events {
        *phase_distribution
            .entry(format!("{:?}", event.phase))
            .or_insert(0) += 1;
    }

    // Build event type distribution
    let mut event_type_distribution = BTreeMap::new();
    for event in &decoded.events {
        *event_type_distribution
            .entry(format!("{:?}", event.event_type))
            .or_insert(0) += 1;
    }

    // Build replay config from bisect request or first divergence
    let replay_config = manifest
        .bisect_request
        .as_ref()
        .map(|bisect| ReplayConfig::from_bisect_request(bisect, &manifest.run_id))
        .or_else(|| {
            divergences.first().map(|div| ReplayConfig {
                seed: div.seed,
                scenario_id: div.scenario_id.clone(),
                replay_command: format!("cargo test -p fsqlite-harness -- {}", div.scenario_id,),
                lane: manifest.lane.clone(),
                git_sha: manifest.git_sha.clone(),
                good_commit: None,
                run_id: manifest.run_id.clone(),
            })
        });

    TriageSession {
        bead_id: manifest.bead_id.clone(),
        manifest_summary: ManifestSummary::from_manifest(manifest),
        validation_passed: report.passed,
        total_events: decoded.events.len(),
        decode_errors: decoded.errors.len(),
        divergences,
        failure_indices,
        replay_config,
        phase_distribution,
        event_type_distribution,
        validation_errors: report.stats.error_count,
        validation_warnings: report.stats.warning_count,
    }
}

// ---- Triage Report Rendering ----

impl TriageSession {
    /// Render a compact one-line summary for log output.
    #[must_use]
    pub fn summary_line(&self) -> String {
        let status = if self.manifest_summary.gate_passed {
            "PASS"
        } else {
            "FAIL"
        };
        format!(
            "bead_id={} lane={} run_id={} gate={} events={} divergences={} failures={} errors={} warnings={}",
            self.bead_id,
            self.manifest_summary.lane,
            self.manifest_summary.run_id,
            status,
            self.total_events,
            self.divergences.len(),
            self.failure_indices.len(),
            self.validation_errors,
            self.validation_warnings,
        )
    }

    /// Render a full operator triage report (CLI-friendly).
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn render_triage_report(&self) -> String {
        let mut out = String::new();

        // Header
        let _ = writeln!(out, "=== Failure Triage Report ({}) ===\n", self.bead_id,);

        // Manifest summary
        let _ = writeln!(out, "--- Manifest ---");
        let _ = writeln!(
            out,
            "  Run:     {}\n  Lane:    {}\n  Git:     {}\n  Seed:    {}\n  Gate:    {}\n  Artifacts: {}",
            self.manifest_summary.run_id,
            self.manifest_summary.lane,
            self.manifest_summary.git_sha,
            self.manifest_summary.seed,
            if self.manifest_summary.gate_passed {
                "PASS"
            } else {
                "FAIL"
            },
            self.manifest_summary.artifact_count,
        );
        if self.manifest_summary.has_bisect_request {
            let _ = writeln!(out, "  Bisect:  REQUESTED");
        }

        // Validation summary
        let _ = writeln!(out, "\n--- Log Validation ---");
        let _ = writeln!(
            out,
            "  Events:   {} decoded, {} errors\n  Schema:   {} (errors: {}, warnings: {})",
            self.total_events,
            self.decode_errors,
            if self.validation_passed {
                "PASS"
            } else {
                "FAIL"
            },
            self.validation_errors,
            self.validation_warnings,
        );

        // Phase distribution
        if !self.phase_distribution.is_empty() {
            let _ = writeln!(out, "\n--- Phase Distribution ---");
            for (phase, count) in &self.phase_distribution {
                let _ = writeln!(out, "  {phase}: {count}");
            }
        }

        // Divergences
        if !self.divergences.is_empty() {
            let _ = writeln!(
                out,
                "\n--- First Divergences ({}) ---",
                self.divergences.len(),
            );
            for (i, div) in self.divergences.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "\n  [{i}] Scenario: {} | Seed: {} | Backend: {}",
                    div.scenario_id, div.seed, div.backend,
                );
                let _ = writeln!(
                    out,
                    "      Event index: {} | Time: {}",
                    div.event_index, div.timestamp,
                );
                if !div.divergence_point.is_empty() {
                    let _ = writeln!(out, "      Divergence: {}", div.divergence_point,);
                }
                if !div.artifact_paths.is_empty() {
                    let _ = writeln!(out, "      Artifacts: {}", div.artifact_paths.join(", "),);
                }
            }
        }

        // Failures
        if !self.failure_indices.is_empty() {
            let _ = writeln!(out, "\n--- Failures ({}) ---", self.failure_indices.len(),);
            let _ = writeln!(out, "  Event indices: {:?}", self.failure_indices,);
        }

        // Replay instructions
        if let Some(ref config) = self.replay_config {
            let _ = writeln!(out, "\n--- Replay Instructions ---");
            let _ = writeln!(out, "  Scenario: {}", config.scenario_id);
            let _ = writeln!(out, "  Seed:     {}", config.seed);
            let _ = writeln!(out, "  Lane:     {}", config.lane);
            if !config.git_sha.is_empty() {
                let _ = writeln!(out, "  Git:      {}", config.git_sha);
            }
            if let Some(ref good) = config.good_commit {
                let _ = writeln!(out, "  Good:     {}", good);
                let _ = writeln!(out, "  Range:    {}..{}", good, config.git_sha);
            }
            let _ = writeln!(out, "\n  $ {}", config.replay_command);
        }

        // Verdict
        let _ = writeln!(out, "\n--- Verdict ---");
        if self.divergences.is_empty() && self.failure_indices.is_empty() {
            let _ = writeln!(out, "  No divergences or failures detected. Gate passed.");
        } else {
            let _ = writeln!(
                out,
                "  {} divergence(s), {} failure(s) detected. Investigation required.",
                self.divergences.len(),
                self.failure_indices.len(),
            );
        }

        out
    }

    /// Whether the session indicates actionable failures.
    #[must_use]
    pub fn needs_investigation(&self) -> bool {
        !self.divergences.is_empty()
            || !self.failure_indices.is_empty()
            || !self.manifest_summary.gate_passed
    }

    /// JSON-serialize the triage session for CI artifact publishing.
    ///
    /// # Errors
    ///
    /// Returns error if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// ---- Divergence Context Rendering ----

/// Render a divergence in context of surrounding log events.
#[must_use]
pub fn render_divergence_context(
    events: &[LogEventSchema],
    divergence: &ExtractedDivergence,
    context_window: usize,
) -> String {
    let mut out = String::new();

    let _ = writeln!(
        out,
        "Divergence Context: {} (event {})\n",
        divergence.scenario_id, divergence.event_index,
    );

    let start = divergence.event_index.saturating_sub(context_window);
    let end = (divergence.event_index + context_window + 1).min(events.len());

    for (i, event) in events[start..end].iter().enumerate() {
        let abs_i = start + i;
        let marker = if abs_i == divergence.event_index {
            ">>>"
        } else {
            "   "
        };
        let _ = writeln!(
            out,
            "{marker} [{abs_i:3}] {ts} {phase:?}/{etype:?} scenario={scenario} seed={seed}",
            ts = event.timestamp,
            phase = event.phase,
            etype = event.event_type,
            scenario = event.scenario_id.as_deref().unwrap_or("-"),
            seed = event.seed.unwrap_or(0),
        );
        if abs_i == divergence.event_index && !divergence.divergence_point.is_empty() {
            let _ = writeln!(
                out,
                "        ^^^  DIVERGENCE: {}",
                divergence.divergence_point,
            );
        }
    }

    out
}

/// Render a compact reproducibility checklist for an operator.
#[must_use]
pub fn render_reproducibility_checklist(config: &ReplayConfig) -> String {
    let mut out = String::new();

    let _ = writeln!(out, "Reproducibility Checklist:");
    let check = |present: bool| if present { "[x]" } else { "[ ]" };

    let _ = writeln!(
        out,
        "  {} Deterministic seed: {}",
        check(config.seed != 0),
        config.seed,
    );
    let _ = writeln!(
        out,
        "  {} Scenario ID: {}",
        check(!config.scenario_id.is_empty()),
        if config.scenario_id.is_empty() {
            "(missing)"
        } else {
            &config.scenario_id
        },
    );
    let _ = writeln!(
        out,
        "  {} Replay command: {}",
        check(!config.replay_command.is_empty()),
        if config.replay_command.is_empty() {
            "(missing)"
        } else {
            &config.replay_command
        },
    );
    let _ = writeln!(
        out,
        "  {} Git SHA: {}",
        check(!config.git_sha.is_empty()),
        if config.git_sha.is_empty() {
            "(missing)"
        } else {
            &config.git_sha
        },
    );
    let _ = writeln!(
        out,
        "  {} Bisect range: {}",
        check(config.good_commit.is_some()),
        match config.good_commit.as_ref() {
            Some(g) => format!("{}..{}", g, config.git_sha),
            None => "(not available)".to_owned(),
        },
    );

    let completeness = [
        config.seed != 0,
        !config.scenario_id.is_empty(),
        !config.replay_command.is_empty(),
        !config.git_sha.is_empty(),
        config.good_commit.is_some(),
    ]
    .iter()
    .filter(|&&v| v)
    .count();

    let _ = writeln!(out, "\n  Completeness: {completeness}/5");
    if completeness >= 4 {
        let _ = writeln!(out, "  Verdict: REPRODUCIBLE — full context available");
    } else if completeness >= 2 {
        let _ = writeln!(
            out,
            "  Verdict: PARTIAL — replay possible with reduced context",
        );
    } else {
        let _ = writeln!(
            out,
            "  Verdict: INSUFFICIENT — manual investigation required",
        );
    }

    out
}

// ---- Orchestrator: Full Replay-Triage Workflow (bd-1dp9.7.4) ----

/// Public bead identifier for the replay-triage workflow.
pub const REPLAY_TRIAGE_BEAD_ID: &str = "bd-1dp9.7.4";

/// Verdict of the full replay-triage workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayTriageVerdict {
    /// No actionable failures detected.
    Pass,
    /// Failures detected with partial reproducibility context.
    Warning,
    /// Failures detected with full reproducibility context, investigation required.
    Fail,
}

impl std::fmt::Display for ReplayTriageVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => write!(f, "PASS"),
            Self::Warning => write!(f, "WARNING"),
            Self::Fail => write!(f, "FAIL"),
        }
    }
}

/// Configuration for the replay-triage orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayTriageConfig {
    /// Context window size for divergence rendering.
    pub context_window: usize,
    /// Minimum reproducibility completeness (0-5) to avoid WARNING.
    pub min_reproducibility: usize,
}

impl Default for ReplayTriageConfig {
    fn default() -> Self {
        Self {
            context_window: 3,
            min_reproducibility: 3,
        }
    }
}

/// Result of running the full replay-triage workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayTriageReport {
    /// Schema version.
    pub schema_version: u32,
    /// Bead identifier.
    pub bead_id: String,
    /// Overall verdict.
    pub verdict: ReplayTriageVerdict,
    /// The triage session produced from manifest + logs.
    pub session: TriageSession,
    /// Rendered triage report text.
    pub triage_report_text: String,
    /// Reproducibility checklist text (if replay config available).
    pub reproducibility_text: Option<String>,
    /// Divergence context texts (one per divergence).
    pub divergence_contexts: Vec<String>,
    /// Reproducibility completeness score (0-5).
    pub reproducibility_score: usize,
    /// Human-readable summary.
    pub summary: String,
}

impl ReplayTriageReport {
    /// Serialize to JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Compact one-line triage summary.
    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "{}: divergences={} failures={} repro={}/5 events={}",
            self.verdict,
            self.session.divergences.len(),
            self.session.failure_indices.len(),
            self.reproducibility_score,
            self.session.total_events,
        )
    }
}

/// Run the full replay-triage workflow: ingest manifest, decode logs, extract
/// divergences, build replay config, render triage report.
#[must_use]
pub fn run_replay_triage_workflow(
    manifest: &ArtifactManifest,
    jsonl_content: &str,
    config: &ReplayTriageConfig,
) -> ReplayTriageReport {
    // Step 1: Build triage session.
    let session = build_triage_session(manifest, jsonl_content);

    // Step 2: Render full triage report.
    let triage_report_text = session.render_triage_report();

    // Step 3: Render reproducibility checklist (if replay config available).
    let (reproducibility_text, reproducibility_score) =
        if let Some(ref replay) = session.replay_config {
            let text = render_reproducibility_checklist(replay);
            let score = [
                replay.seed != 0,
                !replay.scenario_id.is_empty(),
                !replay.replay_command.is_empty(),
                !replay.git_sha.is_empty(),
                replay.good_commit.is_some(),
            ]
            .iter()
            .filter(|&&v| v)
            .count();
            (Some(text), score)
        } else {
            (None, 0)
        };

    // Step 4: Render divergence contexts.
    let decoded = crate::log_schema_validator::decode_jsonl_stream(jsonl_content);
    let divergence_contexts: Vec<String> = session
        .divergences
        .iter()
        .map(|div| render_divergence_context(&decoded.events, div, config.context_window))
        .collect();

    // Step 5: Determine verdict.
    let verdict = if !session.needs_investigation() {
        ReplayTriageVerdict::Pass
    } else if reproducibility_score < config.min_reproducibility {
        ReplayTriageVerdict::Warning
    } else {
        ReplayTriageVerdict::Fail
    };

    // Step 6: Build summary.
    let summary = format!(
        "Replay triage for run {}: {} divergence(s), {} failure(s), repro {}/5, verdict={}",
        session.manifest_summary.run_id,
        session.divergences.len(),
        session.failure_indices.len(),
        reproducibility_score,
        verdict,
    );

    ReplayTriageReport {
        schema_version: 1,
        bead_id: REPLAY_TRIAGE_BEAD_ID.to_owned(),
        verdict,
        session,
        triage_report_text,
        reproducibility_text,
        divergence_contexts,
        reproducibility_score,
        summary,
    }
}

/// Write a replay-triage report to a JSON file.
///
/// # Errors
///
/// Returns `Err` if serialization or file writing fails.
pub fn write_replay_triage_report(
    path: &std::path::Path,
    report: &ReplayTriageReport,
) -> Result<(), String> {
    let json = report.to_json().map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("write: {e}"))
}

/// Load a replay-triage report from a JSON file.
///
/// # Errors
///
/// Returns `Err` if reading or deserialization fails.
pub fn load_replay_triage_report(path: &std::path::Path) -> Result<ReplayTriageReport, String> {
    let json = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
    ReplayTriageReport::from_json(&json).map_err(|e| format!("parse: {e}"))
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci_gate_matrix::{
        ArtifactEntry, ArtifactKind, BisectTrigger, build_artifact_manifest, build_bisect_request,
    };
    use crate::e2e_log_schema::LogPhase;

    const SEED: u64 = 20260213;

    fn build_test_manifest(with_bisect: bool) -> ArtifactManifest {
        let bisect = if with_bisect {
            Some(build_bisect_request(
                BisectTrigger::GateRegression,
                crate::ci_gate_matrix::CiLane::E2eDifferential,
                "test_mvcc_isolation",
                "abc1234500000000",
                "def6789000000000",
                SEED,
                "cargo test -p fsqlite-harness -- test_mvcc_isolation",
                "MVCC isolation regression",
            ))
        } else {
            None
        };

        build_artifact_manifest(
            crate::ci_gate_matrix::CiLane::E2eDifferential,
            &format!("{BEAD_ID}-{SEED}"),
            "def6789000000000",
            SEED,
            !with_bisect,
            vec![ArtifactEntry {
                kind: ArtifactKind::Log,
                path: "logs/events.jsonl".to_owned(),
                content_hash: "a".repeat(64),
                size_bytes: 4096,
                description: "Event log".to_owned(),
            }],
            bisect,
        )
    }

    fn build_test_jsonl() -> String {
        let events = vec![
            LogEventSchema {
                run_id: format!("{BEAD_ID}-{SEED}"),
                timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
                phase: LogPhase::Setup,
                event_type: LogEventType::Start,
                scenario_id: Some("MVCC-3".to_owned()),
                seed: Some(SEED),
                backend: Some("both".to_owned()),
                artifact_hash: None,
                context: BTreeMap::new(),
            },
            LogEventSchema {
                run_id: format!("{BEAD_ID}-{SEED}"),
                timestamp: "2026-02-13T09:00:01.000Z".to_owned(),
                phase: LogPhase::Execute,
                event_type: LogEventType::Info,
                scenario_id: Some("MVCC-3".to_owned()),
                seed: Some(SEED),
                backend: Some("fsqlite".to_owned()),
                artifact_hash: None,
                context: BTreeMap::new(),
            },
            LogEventSchema {
                run_id: format!("{BEAD_ID}-{SEED}"),
                timestamp: "2026-02-13T09:00:02.000Z".to_owned(),
                phase: LogPhase::Validate,
                event_type: LogEventType::FirstDivergence,
                scenario_id: Some("MVCC-3".to_owned()),
                seed: Some(SEED),
                backend: Some("both".to_owned()),
                artifact_hash: None,
                context: {
                    let mut ctx = BTreeMap::new();
                    ctx.insert("divergence_point".to_owned(), "row 42 column 3".to_owned());
                    ctx.insert("artifact_paths".to_owned(), "divergence.json".to_owned());
                    ctx
                },
            },
            LogEventSchema {
                run_id: format!("{BEAD_ID}-{SEED}"),
                timestamp: "2026-02-13T09:00:03.000Z".to_owned(),
                phase: LogPhase::Validate,
                event_type: LogEventType::Fail,
                scenario_id: Some("MVCC-3".to_owned()),
                seed: Some(SEED),
                backend: Some("both".to_owned()),
                artifact_hash: None,
                context: BTreeMap::new(),
            },
            LogEventSchema {
                run_id: format!("{BEAD_ID}-{SEED}"),
                timestamp: "2026-02-13T09:00:04.000Z".to_owned(),
                phase: LogPhase::Teardown,
                event_type: LogEventType::Info,
                scenario_id: Some("MVCC-3".to_owned()),
                seed: Some(SEED),
                backend: None,
                artifact_hash: None,
                context: BTreeMap::new(),
            },
        ];

        crate::log_schema_validator::encode_jsonl_stream(&events).unwrap()
    }

    fn build_clean_jsonl() -> String {
        let events = vec![
            LogEventSchema {
                run_id: format!("{BEAD_ID}-clean-{SEED}"),
                timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
                phase: LogPhase::Setup,
                event_type: LogEventType::Start,
                scenario_id: Some("INFRA-1".to_owned()),
                seed: Some(SEED),
                backend: Some("both".to_owned()),
                artifact_hash: None,
                context: BTreeMap::new(),
            },
            LogEventSchema {
                run_id: format!("{BEAD_ID}-clean-{SEED}"),
                timestamp: "2026-02-13T09:00:01.000Z".to_owned(),
                phase: LogPhase::Validate,
                event_type: LogEventType::Pass,
                scenario_id: Some("INFRA-1".to_owned()),
                seed: Some(SEED),
                backend: Some("fsqlite".to_owned()),
                artifact_hash: Some("b".repeat(64)),
                context: BTreeMap::new(),
            },
        ];

        crate::log_schema_validator::encode_jsonl_stream(&events).unwrap()
    }

    // ---- Divergence Extraction Tests ----

    #[test]
    fn extract_divergences_finds_first_divergence() {
        let jsonl = build_test_jsonl();
        let decoded = decode_jsonl_stream(&jsonl);
        let divergences = extract_divergences(&decoded.events);

        assert_eq!(
            divergences.len(),
            1,
            "bead_id={BEAD_ID} case=extract_divergences expected 1",
        );
        assert_eq!(divergences[0].scenario_id, "MVCC-3");
        assert_eq!(divergences[0].divergence_point, "row 42 column 3");
        assert_eq!(divergences[0].seed, SEED);
        assert_eq!(divergences[0].event_index, 2);
    }

    #[test]
    fn extract_divergences_empty_on_clean_stream() {
        let jsonl = build_clean_jsonl();
        let decoded = decode_jsonl_stream(&jsonl);
        let divergences = extract_divergences(&decoded.events);
        assert!(
            divergences.is_empty(),
            "bead_id={BEAD_ID} case=no_divergences",
        );
    }

    #[test]
    fn extract_failures_finds_fail_events() {
        let jsonl = build_test_jsonl();
        let decoded = decode_jsonl_stream(&jsonl);
        let failures = extract_failures(&decoded.events);

        assert_eq!(failures.len(), 1, "bead_id={BEAD_ID} case=extract_failures",);
        assert_eq!(failures[0].1.event_type, LogEventType::Fail);
    }

    #[test]
    fn extract_failures_empty_on_clean_stream() {
        let jsonl = build_clean_jsonl();
        let decoded = decode_jsonl_stream(&jsonl);
        let failures = extract_failures(&decoded.events);
        assert!(failures.is_empty(), "bead_id={BEAD_ID} case=no_failures",);
    }

    // ---- Replay Config Tests ----

    #[test]
    fn replay_config_from_bisect_request() {
        let bisect = build_bisect_request(
            BisectTrigger::GateRegression,
            crate::ci_gate_matrix::CiLane::Unit,
            "test_split",
            "good_sha",
            "bad_sha",
            42,
            "cargo test -- test_split",
            "regression",
        );
        let config = ReplayConfig::from_bisect_request(&bisect, "run-1");

        assert_eq!(config.seed, 42);
        assert_eq!(config.scenario_id, "test_split");
        assert_eq!(config.git_sha, "bad_sha");
        assert_eq!(config.good_commit, Some("good_sha".to_owned()));
        assert_eq!(config.run_id, "run-1");
    }

    #[test]
    fn replay_config_from_log_event() {
        let event = LogEventSchema {
            run_id: "run-42".to_owned(),
            timestamp: "2026-02-13T09:00:00Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::Fail,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(42),
            backend: Some("both".to_owned()),
            artifact_hash: None,
            context: BTreeMap::new(),
        };
        let config = ReplayConfig::from_log_event(&event, "e2e-differential");

        assert_eq!(config.seed, 42);
        assert_eq!(config.scenario_id, "MVCC-3");
        assert_eq!(config.lane, "e2e-differential");
        assert!(config.good_commit.is_none());
    }

    #[test]
    fn replay_config_json_roundtrip() {
        let config = ReplayConfig {
            seed: 42,
            scenario_id: "MVCC-3".to_owned(),
            replay_command: "cargo test".to_owned(),
            lane: "unit".to_owned(),
            git_sha: "abc".to_owned(),
            good_commit: Some("def".to_owned()),
            run_id: "run-1".to_owned(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ReplayConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, config);
    }

    // ---- Triage Session Tests ----

    #[test]
    fn triage_session_from_failing_manifest() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        assert!(
            session.needs_investigation(),
            "bead_id={BEAD_ID} case=needs_investigation",
        );
        assert_eq!(session.total_events, 5);
        assert_eq!(session.divergences.len(), 1);
        assert_eq!(session.failure_indices.len(), 1);
        assert!(session.replay_config.is_some());
    }

    #[test]
    fn triage_session_from_clean_manifest() {
        let manifest = build_test_manifest(false);
        let jsonl = build_clean_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        assert!(
            !session.needs_investigation(),
            "bead_id={BEAD_ID} case=no_investigation_needed",
        );
        assert_eq!(session.total_events, 2);
        assert!(session.divergences.is_empty());
        assert!(session.failure_indices.is_empty());
    }

    #[test]
    fn triage_session_summary_line() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        let summary = session.summary_line();
        assert!(summary.contains("FAIL"), "should contain FAIL");
        assert!(summary.contains("divergences=1"));
        assert!(summary.contains("failures=1"));
    }

    #[test]
    fn triage_session_json_roundtrip() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        let json = session.to_json().unwrap();
        let deserialized: TriageSession = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_events, session.total_events);
        assert_eq!(deserialized.divergences.len(), session.divergences.len());
    }

    // ---- Triage Report Rendering Tests ----

    #[test]
    fn triage_report_contains_sections() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        let report = session.render_triage_report();
        assert!(
            report.contains("Failure Triage Report"),
            "bead_id={BEAD_ID} case=report_header",
        );
        assert!(
            report.contains("--- Manifest ---"),
            "bead_id={BEAD_ID} case=report_manifest",
        );
        assert!(
            report.contains("--- Log Validation ---"),
            "bead_id={BEAD_ID} case=report_validation",
        );
        assert!(
            report.contains("--- First Divergences"),
            "bead_id={BEAD_ID} case=report_divergences",
        );
        assert!(
            report.contains("--- Replay Instructions ---"),
            "bead_id={BEAD_ID} case=report_replay",
        );
        assert!(
            report.contains("--- Verdict ---"),
            "bead_id={BEAD_ID} case=report_verdict",
        );
        assert!(
            report.contains("MVCC-3"),
            "bead_id={BEAD_ID} case=report_scenario",
        );
        assert!(
            report.contains("row 42 column 3"),
            "bead_id={BEAD_ID} case=report_divergence_point",
        );
    }

    #[test]
    fn triage_report_clean_run() {
        let manifest = build_test_manifest(false);
        let jsonl = build_clean_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        let report = session.render_triage_report();
        assert!(
            report.contains("No divergences or failures detected"),
            "bead_id={BEAD_ID} case=clean_verdict",
        );
    }

    // ---- Divergence Context Tests ----

    #[test]
    fn divergence_context_shows_marker() {
        let jsonl = build_test_jsonl();
        let decoded = decode_jsonl_stream(&jsonl);
        let divergences = extract_divergences(&decoded.events);
        let div = &divergences[0];

        let context = render_divergence_context(&decoded.events, div, 2);
        assert!(
            context.contains(">>>"),
            "bead_id={BEAD_ID} case=context_marker",
        );
        assert!(
            context.contains("DIVERGENCE: row 42 column 3"),
            "bead_id={BEAD_ID} case=context_divergence_text",
        );
        assert!(
            context.contains("FirstDivergence"),
            "bead_id={BEAD_ID} case=context_event_type",
        );
    }

    #[test]
    fn divergence_context_respects_window() {
        let jsonl = build_test_jsonl();
        let decoded = decode_jsonl_stream(&jsonl);
        let divergences = extract_divergences(&decoded.events);
        let div = &divergences[0];

        // Window of 1: events 1, 2, 3
        let context = render_divergence_context(&decoded.events, div, 1);
        let lines: Vec<&str> = context.lines().collect();
        // Header + 3 event lines + 1 divergence annotation = 5
        let event_lines: Vec<&&str> = lines.iter().filter(|l| l.contains("[")).collect();
        assert_eq!(
            event_lines.len(),
            3,
            "bead_id={BEAD_ID} case=context_window_size lines={event_lines:?}",
        );
    }

    // ---- Reproducibility Checklist Tests ----

    #[test]
    fn reproducibility_checklist_full() {
        let config = ReplayConfig {
            seed: 42,
            scenario_id: "MVCC-3".to_owned(),
            replay_command: "cargo test".to_owned(),
            lane: "unit".to_owned(),
            git_sha: "abc".to_owned(),
            good_commit: Some("def".to_owned()),
            run_id: "run-1".to_owned(),
        };
        let checklist = render_reproducibility_checklist(&config);
        assert!(
            checklist.contains("5/5"),
            "bead_id={BEAD_ID} case=full_checklist",
        );
        assert!(
            checklist.contains("REPRODUCIBLE"),
            "bead_id={BEAD_ID} case=full_verdict",
        );
    }

    #[test]
    fn reproducibility_checklist_partial() {
        let config = ReplayConfig {
            seed: 42,
            scenario_id: "MVCC-3".to_owned(),
            replay_command: "cargo test".to_owned(),
            lane: "unit".to_owned(),
            git_sha: String::new(),
            good_commit: None,
            run_id: "run-1".to_owned(),
        };
        let checklist = render_reproducibility_checklist(&config);
        assert!(
            checklist.contains("3/5"),
            "bead_id={BEAD_ID} case=partial_checklist",
        );
        assert!(
            checklist.contains("PARTIAL"),
            "bead_id={BEAD_ID} case=partial_verdict",
        );
    }

    #[test]
    fn reproducibility_checklist_insufficient() {
        let config = ReplayConfig {
            seed: 0,
            scenario_id: String::new(),
            replay_command: String::new(),
            lane: String::new(),
            git_sha: String::new(),
            good_commit: None,
            run_id: String::new(),
        };
        let checklist = render_reproducibility_checklist(&config);
        assert!(
            checklist.contains("0/5"),
            "bead_id={BEAD_ID} case=insufficient_checklist",
        );
        assert!(
            checklist.contains("INSUFFICIENT"),
            "bead_id={BEAD_ID} case=insufficient_verdict",
        );
    }

    // ---- Phase/Event Distribution Tests ----

    #[test]
    fn triage_tracks_phase_distribution() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        assert!(
            session.phase_distribution.contains_key("Setup"),
            "bead_id={BEAD_ID} case=phase_setup",
        );
        assert!(
            session.phase_distribution.contains_key("Execute"),
            "bead_id={BEAD_ID} case=phase_execute",
        );
        assert!(
            session.phase_distribution.contains_key("Validate"),
            "bead_id={BEAD_ID} case=phase_validate",
        );
    }

    #[test]
    fn triage_tracks_event_type_distribution() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();
        let session = build_triage_session(&manifest, &jsonl);

        assert!(
            session.event_type_distribution.contains_key("Start"),
            "bead_id={BEAD_ID} case=etype_start",
        );
        assert!(
            session
                .event_type_distribution
                .contains_key("FirstDivergence"),
            "bead_id={BEAD_ID} case=etype_divergence",
        );
        assert!(
            session.event_type_distribution.contains_key("Fail"),
            "bead_id={BEAD_ID} case=etype_fail",
        );
    }

    // ---- Determinism Tests ----

    #[test]
    fn triage_session_deterministic() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();

        let s1 = build_triage_session(&manifest, &jsonl);
        let s2 = build_triage_session(&manifest, &jsonl);

        let j1 = s1.to_json().unwrap();
        let j2 = s2.to_json().unwrap();
        assert_eq!(j1, j2, "bead_id={BEAD_ID} case=session_determinism",);
    }

    #[test]
    fn triage_report_deterministic() {
        let manifest = build_test_manifest(true);
        let jsonl = build_test_jsonl();

        let s1 = build_triage_session(&manifest, &jsonl);
        let s2 = build_triage_session(&manifest, &jsonl);

        let r1 = s1.render_triage_report();
        let r2 = s2.render_triage_report();
        assert_eq!(r1, r2, "bead_id={BEAD_ID} case=report_determinism",);
    }
}
