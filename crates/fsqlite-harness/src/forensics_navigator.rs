//! Failure forensics timeline and correlation queries (bd-mblr.7.5.2).
//!
//! This module builds operator-facing timeline and correlation views on top of
//! [`crate::evidence_index::EvidenceIndex`]. It is intentionally deterministic:
//! results are stable-sorted so CLI and CI outputs are reproducible.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::evidence_index::{
    ArtifactKind, EvidenceIndex, InvariantVerdict, RunRecord, ScenarioVerdict, run_from_jsonl,
};

/// Bead identifier for log correlation.
const BEAD_ID: &str = "bd-mblr.7.5.2";

/// Severity classes used by forensics triage views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Critical => write!(f, "critical"),
            Self::High => write!(f, "high"),
            Self::Medium => write!(f, "medium"),
            Self::Low => write!(f, "low"),
        }
    }
}

impl Severity {
    /// Parse a severity value from CLI/user input.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "critical" => Some(Self::Critical),
            "high" => Some(Self::High),
            "medium" => Some(Self::Medium),
            "low" => Some(Self::Low),
            _ => None,
        }
    }
}

/// Filters applied to forensics queries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryFilters {
    /// Bead/issue ID filter (`RunRecord::bead_ids`).
    pub issue_id: Option<String>,
    /// Git SHA filter (`RunRecord::git_sha`).
    pub commit: Option<String>,
    /// Deterministic seed filter.
    pub seed: Option<u64>,
    /// Code area/component filter (`ScenarioOutcome::code_areas`).
    pub component: Option<String>,
    /// Severity filter derived from run outcomes.
    pub severity: Option<Severity>,
    /// Optional result limit after filtering and sorting.
    pub limit: Option<usize>,
}

/// Timeline row for one matched run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub run_id: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub git_sha: String,
    pub seed: u64,
    pub severity: Severity,
    pub success: bool,
    pub bead_ids: Vec<String>,
    pub components: Vec<String>,
    pub failing_scenarios: Vec<String>,
    pub violated_invariants: Vec<String>,
    pub artifact_paths: Vec<String>,
    pub replay_command: Option<String>,
}

/// Correlation aggregate row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelationRow {
    /// Correlation key (e.g. `component:planner`, `invariant:INV-3`).
    pub key: String,
    /// Number of runs where this key appeared.
    pub run_count: usize,
    /// Stable sorted run IDs.
    pub run_ids: Vec<String>,
    /// Stable sorted artifact links.
    pub artifact_paths: Vec<String>,
}

/// Full forensics query response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForensicsQueryResult {
    pub bead_id: String,
    pub filters: QueryFilters,
    pub scanned_run_count: usize,
    pub matched_run_count: usize,
    pub timeline: Vec<TimelineEvent>,
    pub correlations: Vec<CorrelationRow>,
}

/// Load an evidence index from JSONL (`RunRecord` per line).
pub fn load_index_from_jsonl(path: &Path) -> Result<EvidenceIndex, String> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "failed to read evidence JSONL at {}: {error}",
            path.display()
        )
    })?;

    let mut index = EvidenceIndex::new();
    for (line_no, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record = run_from_jsonl(trimmed).map_err(|error| {
            format!(
                "failed to parse evidence JSONL {} line {}: {error}",
                path.display(),
                line_no + 1
            )
        })?;
        index.insert(record);
    }
    Ok(index)
}

/// Query timeline + correlation views from an evidence index.
#[must_use]
pub fn query_index(index: &EvidenceIndex, filters: &QueryFilters) -> ForensicsQueryResult {
    let scanned_run_count = index.runs.len();

    let mut matched_runs: Vec<&RunRecord> = index
        .runs
        .values()
        .filter(|run| run_matches_filters(run, filters))
        .collect();
    matched_runs.sort_by(|left, right| {
        left.started_at
            .cmp(&right.started_at)
            .then_with(|| left.run_id.0.cmp(&right.run_id.0))
    });
    if let Some(limit) = filters.limit {
        matched_runs.truncate(limit);
    }

    let timeline: Vec<TimelineEvent> = matched_runs
        .iter()
        .map(|run| build_timeline_event(run))
        .collect();
    let correlations = build_correlations(&timeline);

    ForensicsQueryResult {
        bead_id: BEAD_ID.to_owned(),
        filters: filters.clone(),
        scanned_run_count,
        matched_run_count: timeline.len(),
        timeline,
        correlations,
    }
}

/// Render a deterministic human-readable report.
#[must_use]
pub fn render_text_report(result: &ForensicsQueryResult) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "forensics report bead_id={} scanned={} matched={}",
        result.bead_id, result.scanned_run_count, result.matched_run_count
    ));
    lines.push("timeline:".to_owned());
    for event in &result.timeline {
        lines.push(format!(
            "- run_id={} started_at={} severity={} success={} commit={} seed={}",
            event.run_id,
            event.started_at,
            event.severity,
            event.success,
            event.git_sha,
            event.seed
        ));
        if !event.failing_scenarios.is_empty() {
            lines.push(format!(
                "  failing_scenarios={}",
                event.failing_scenarios.join(",")
            ));
        }
        if !event.violated_invariants.is_empty() {
            lines.push(format!(
                "  violated_invariants={}",
                event.violated_invariants.join(",")
            ));
        }
        if !event.components.is_empty() {
            lines.push(format!("  components={}", event.components.join(",")));
        }
        if let Some(replay_command) = &event.replay_command {
            lines.push(format!("  replay={replay_command}"));
        }
    }

    lines.push("correlations:".to_owned());
    for row in &result.correlations {
        lines.push(format!(
            "- key={} run_count={} runs={} artifacts={}",
            row.key,
            row.run_count,
            row.run_ids.join(","),
            row.artifact_paths.join(",")
        ));
    }
    lines.join("\n")
}

fn run_matches_filters(run: &RunRecord, filters: &QueryFilters) -> bool {
    if let Some(issue_id) = &filters.issue_id {
        if !run.bead_ids.iter().any(|bead| bead == issue_id) {
            return false;
        }
    }
    if let Some(commit) = &filters.commit {
        if run.git_sha != *commit {
            return false;
        }
    }
    if let Some(seed) = filters.seed {
        if run.seed != seed {
            return false;
        }
    }
    if let Some(component) = &filters.component {
        let has_component = run.scenarios.iter().any(|scenario| {
            scenario
                .code_areas
                .iter()
                .any(|area| area.as_str() == component.as_str())
        });
        if !has_component {
            return false;
        }
    }
    if let Some(severity_filter) = filters.severity {
        if classify_severity(run) != severity_filter {
            return false;
        }
    }
    true
}

fn classify_severity(run: &RunRecord) -> Severity {
    let has_invariant_violation = run
        .invariants
        .iter()
        .any(|invariant| invariant.verdict == InvariantVerdict::Violated);
    let has_failing_scenario = run.scenarios.iter().any(|scenario| {
        matches!(
            scenario.verdict,
            ScenarioVerdict::Fail | ScenarioVerdict::Timeout | ScenarioVerdict::Divergence
        )
    });
    let has_skip = run
        .scenarios
        .iter()
        .any(|scenario| scenario.verdict == ScenarioVerdict::Skip);

    if has_invariant_violation {
        Severity::Critical
    } else if !run.success || has_failing_scenario {
        Severity::High
    } else if has_skip {
        Severity::Medium
    } else {
        Severity::Low
    }
}

fn build_timeline_event(run: &RunRecord) -> TimelineEvent {
    let mut components: BTreeSet<String> = BTreeSet::new();
    let mut failing_scenarios = Vec::new();
    for scenario in &run.scenarios {
        for area in &scenario.code_areas {
            components.insert(area.clone());
        }
        if matches!(
            scenario.verdict,
            ScenarioVerdict::Fail | ScenarioVerdict::Timeout | ScenarioVerdict::Divergence
        ) {
            failing_scenarios.push(scenario.scenario_id.clone());
        }
    }
    failing_scenarios.sort();

    let mut violated_invariants: Vec<String> = run
        .invariants
        .iter()
        .filter(|invariant| invariant.verdict == InvariantVerdict::Violated)
        .map(|invariant| invariant.invariant_id.clone())
        .collect();
    violated_invariants.sort();

    let mut artifact_paths: Vec<String> = run
        .artifacts
        .iter()
        .map(|artifact| artifact.path.clone())
        .collect();
    artifact_paths.sort();
    let replay_command = build_replay_command(run);

    let mut bead_ids = run.bead_ids.clone();
    bead_ids.sort();

    TimelineEvent {
        run_id: run.run_id.0.clone(),
        started_at: run.started_at.clone(),
        completed_at: run.completed_at.clone(),
        git_sha: run.git_sha.clone(),
        seed: run.seed,
        severity: classify_severity(run),
        success: run.success,
        bead_ids,
        components: components.into_iter().collect(),
        failing_scenarios,
        violated_invariants,
        artifact_paths,
        replay_command,
    }
}

fn build_replay_command(run: &RunRecord) -> Option<String> {
    let manifest_path = run
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == ArtifactKind::ReplayManifest)
        .map(|artifact| artifact.path.as_str())
        .min();
    manifest_path.map(|path| {
        format!("cargo run -p fsqlite-harness --bin replay_harness -- --manifest {path}")
    })
}

fn build_correlations(events: &[TimelineEvent]) -> Vec<CorrelationRow> {
    #[derive(Default)]
    struct Bucket {
        run_ids: BTreeSet<String>,
        artifact_paths: BTreeSet<String>,
    }

    let mut by_key: BTreeMap<String, Bucket> = BTreeMap::new();
    for event in events {
        for component in &event.components {
            let entry = by_key.entry(format!("component:{component}")).or_default();
            entry.run_ids.insert(event.run_id.clone());
            for path in &event.artifact_paths {
                entry.artifact_paths.insert(path.clone());
            }
        }
        for invariant in &event.violated_invariants {
            let entry = by_key.entry(format!("invariant:{invariant}")).or_default();
            entry.run_ids.insert(event.run_id.clone());
            for path in &event.artifact_paths {
                entry.artifact_paths.insert(path.clone());
            }
        }
    }

    let mut rows: Vec<CorrelationRow> = by_key
        .into_iter()
        .map(|(key, bucket)| CorrelationRow {
            key,
            run_count: bucket.run_ids.len(),
            run_ids: bucket.run_ids.into_iter().collect(),
            artifact_paths: bucket.artifact_paths.into_iter().collect(),
        })
        .collect();

    rows.sort_by(|left, right| {
        right
            .run_count
            .cmp(&left.run_count)
            .then_with(|| left.key.cmp(&right.key))
    });
    rows
}
