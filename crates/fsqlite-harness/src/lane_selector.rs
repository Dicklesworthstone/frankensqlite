//! Deterministic CI lane selection with mandatory safety floor (bd-mblr.7.9.2).
//!
//! This module consumes the code-area impact graph and emits a minimal CI lane
//! set for the changed paths. It always enforces a **safety floor**:
//! correctness, concurrency, and recovery validation lanes must run regardless
//! of impact score or path classification confidence.
//!
//! When path-to-code-area resolution is uncertain for code-like paths, the
//! selector escalates to full-suite fallback (`CiLane::ALL`).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ci_coverage_gate::{CoverageGateReport, CoverageVerdict};
use crate::ci_gate_matrix::CiLane;
use crate::impact_graph::{ImpactGraph, ValidationLane};

/// Bead identifier for traceability in logs/artifacts.
pub const BEAD_ID: &str = "bd-mblr.7.9.2";
/// Schema version for serialized selection reports.
pub const LANE_SELECTOR_SCHEMA_VERSION: &str = "1.0.0";

/// Safety-floor validation domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafetyDomain {
    Correctness,
    Concurrency,
    Recovery,
}

impl SafetyDomain {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Correctness => "correctness",
            Self::Concurrency => "concurrency",
            Self::Recovery => "recovery",
        }
    }
}

/// One selected lane with explanatory rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneDecision {
    /// CI lane to execute.
    pub lane: CiLane,
    /// Whether lane is mandatory (`true`) or advisory (`false`).
    pub mandatory: bool,
    /// Human-readable reasons that caused this lane selection.
    pub reasons: Vec<String>,
}

/// Deterministic lane-selection output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneSelectionReport {
    /// Schema version for backward-compatible parsing.
    pub schema_version: String,
    /// Bead id that owns this report schema.
    pub bead_id: String,
    /// Normalized/sorted changed paths used as input.
    pub changed_paths: Vec<String>,
    /// Resolved code-area identifiers from the impact graph.
    pub resolved_code_areas: Vec<String>,
    /// Paths that could not be resolved to known code areas.
    pub unresolved_paths: Vec<String>,
    /// Whether full-suite fallback was applied.
    pub fallback_full_suite: bool,
    /// Selected lane decisions in deterministic lane order.
    pub decisions: Vec<LaneDecision>,
}

impl LaneSelectionReport {
    /// Return the selected lanes in deterministic order.
    #[must_use]
    pub fn selected_lanes(&self) -> Vec<CiLane> {
        self.decisions.iter().map(|d| d.lane).collect()
    }

    /// Serialize to pretty JSON.
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
    /// Returns `Err` if JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[derive(Debug, Default)]
struct DecisionAccumulator {
    mandatory: bool,
    reasons: BTreeSet<String>,
}

fn add_lane_reason(
    acc: &mut BTreeMap<CiLane, DecisionAccumulator>,
    lane: CiLane,
    mandatory: bool,
    reason: impl Into<String>,
) {
    let entry = acc.entry(lane).or_default();
    if mandatory {
        entry.mandatory = true;
    }
    let _inserted = entry.reasons.insert(reason.into());
}

fn add_safety_floor(acc: &mut BTreeMap<CiLane, DecisionAccumulator>) {
    for (domain, lane) in safety_floor_lanes() {
        add_lane_reason(
            acc,
            lane,
            true,
            format!("safety floor: {}", domain.as_str()),
        );
    }
}

#[must_use]
const fn safety_floor_lanes() -> [(SafetyDomain, CiLane); 4] {
    [
        (SafetyDomain::Correctness, CiLane::Unit),
        (SafetyDomain::Correctness, CiLane::E2eCorrectness),
        (SafetyDomain::Concurrency, CiLane::E2eDifferential),
        (SafetyDomain::Recovery, CiLane::E2eRecovery),
    ]
}

#[must_use]
const fn ci_lanes_for_validation_lane(lane: ValidationLane) -> &'static [CiLane] {
    match lane {
        ValidationLane::UnitTests => &[CiLane::Unit],
        ValidationLane::StorageIntegration => &[CiLane::E2eCorrectness],
        ValidationLane::SqlPipeline => &[CiLane::E2eDifferential, CiLane::E2eCorrectness],
        ValidationLane::ConcurrencyStress => &[CiLane::E2eDifferential],
        ValidationLane::RecoveryDurability => &[CiLane::E2eRecovery],
        ValidationLane::SoakEndurance => &[CiLane::CoverageDrift],
        ValidationLane::MetamorphicDifferential => &[CiLane::E2eDifferential],
        ValidationLane::PerformanceRegression => &[CiLane::Performance],
        ValidationLane::FullE2e => &[
            CiLane::E2eDifferential,
            CiLane::E2eCorrectness,
            CiLane::E2eRecovery,
        ],
    }
}

fn map_path_to_code_area(path: &str) -> Option<&'static str> {
    if path.starts_with("crates/fsqlite-types/") {
        Some("fsqlite-types")
    } else if path.starts_with("crates/fsqlite-vfs/") {
        Some("fsqlite-vfs")
    } else if path.starts_with("crates/fsqlite-pager/") {
        Some("fsqlite-pager")
    } else if path.starts_with("crates/fsqlite-wal/") {
        Some("fsqlite-wal")
    } else if path.starts_with("crates/fsqlite-mvcc/") {
        Some("fsqlite-mvcc")
    } else if path.starts_with("crates/fsqlite-btree/") {
        Some("fsqlite-btree")
    } else if path.starts_with("crates/fsqlite-parser/") || path.starts_with("crates/fsqlite-ast/")
    {
        Some("fsqlite-parser")
    } else if path.starts_with("crates/fsqlite-planner/") {
        Some("fsqlite-planner")
    } else if path.starts_with("crates/fsqlite-vdbe/") {
        Some("fsqlite-vdbe")
    } else if path.starts_with("crates/fsqlite-func/") {
        Some("fsqlite-functions")
    } else if path.starts_with("crates/fsqlite-ext-") {
        Some("fsqlite-extensions")
    } else if path.starts_with("crates/fsqlite-core/")
        || path.starts_with("crates/fsqlite/")
        || path.starts_with("crates/fsqlite-error/")
    {
        Some("fsqlite-core")
    } else if path.starts_with("crates/fsqlite-cli/") {
        Some("fsqlite-cli")
    } else if path.starts_with("crates/fsqlite-harness/")
        || path.starts_with("crates/fsqlite-e2e/")
        || path.starts_with("crates/fsqlite-observability/")
    {
        Some("fsqlite-harness")
    } else {
        None
    }
}

fn is_non_code_path(path: &str) -> bool {
    path.starts_with("docs/")
        || path.starts_with(".beads/")
        || path.starts_with("artifacts/")
        || path.starts_with("reports/")
        || path.ends_with(".md")
        || path.ends_with(".png")
        || path.ends_with(".webp")
        || path == "README.md"
        || path == "AGENTS.md"
}

fn looks_like_code_path(path: &str) -> bool {
    path.starts_with("crates/")
        || path.starts_with("src/")
        || path.starts_with("tests/")
        || path.starts_with(".github/workflows/")
        || path.ends_with(".rs")
        || path.ends_with(".toml")
        || path == "Cargo.toml"
        || path == "Cargo.lock"
        || path == "rust-toolchain.toml"
}

fn apply_coverage_gate_policy(
    acc: &mut BTreeMap<CiLane, DecisionAccumulator>,
    coverage_gate: Option<&CoverageGateReport>,
) {
    if let Some(report) = coverage_gate {
        match report.verdict {
            CoverageVerdict::Pass => {}
            CoverageVerdict::PassWithWarnings | CoverageVerdict::Fail => {
                add_lane_reason(
                    acc,
                    CiLane::SchemaValidation,
                    true,
                    format!("coverage gate escalation: verdict={}", report.verdict),
                );
                add_lane_reason(
                    acc,
                    CiLane::CoverageDrift,
                    true,
                    format!("coverage gate escalation: verdict={}", report.verdict),
                );
            }
        }
    }
}

fn validate_area_ids(graph: &ImpactGraph, area_ids: &BTreeSet<String>) -> BTreeSet<String> {
    let known: BTreeSet<String> = graph.code_areas.iter().map(|area| area.id()).collect();
    area_ids
        .iter()
        .filter(|id| known.contains((*id).as_str()))
        .cloned()
        .collect()
}

/// Select CI lanes for changed paths using impact mapping and safety-floor rules.
///
/// The result is deterministic: paths are normalized and sorted; selected lanes
/// are emitted in canonical `CiLane::ALL` order.
#[must_use]
pub fn select_ci_lanes_for_paths(
    changed_paths: &[impl AsRef<str>],
    graph: &ImpactGraph,
    coverage_gate: Option<&CoverageGateReport>,
) -> LaneSelectionReport {
    let normalized_paths: Vec<String> = changed_paths
        .iter()
        .map(|path| path.as_ref().trim().to_owned())
        .filter(|path| !path.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut resolved = BTreeSet::new();
    let mut unresolved = BTreeSet::new();
    for path in &normalized_paths {
        if let Some(area_id) = map_path_to_code_area(path) {
            let _inserted = resolved.insert(area_id.to_owned());
        } else if !is_non_code_path(path) {
            let _inserted = unresolved.insert(path.clone());
        }
    }

    let resolved = validate_area_ids(graph, &resolved);
    let resolved_ids: Vec<String> = resolved.iter().cloned().collect();
    let unresolved_paths: Vec<String> = unresolved.iter().cloned().collect();

    let mut acc: BTreeMap<CiLane, DecisionAccumulator> = BTreeMap::new();
    add_safety_floor(&mut acc);
    apply_coverage_gate_policy(&mut acc, coverage_gate);

    let mut fallback_full_suite = false;
    if unresolved_paths
        .iter()
        .any(|path| looks_like_code_path(path))
    {
        fallback_full_suite = true;
        for lane in CiLane::ALL {
            add_lane_reason(
                &mut acc,
                lane,
                true,
                format!(
                    "full-suite fallback: unresolved code paths ({})",
                    unresolved_paths.join(", ")
                ),
            );
        }
    } else {
        for area_id in &resolved_ids {
            for edge in &graph.lane_edges {
                if edge.code_area_id != *area_id {
                    continue;
                }
                for lane in ci_lanes_for_validation_lane(edge.lane) {
                    add_lane_reason(
                        &mut acc,
                        *lane,
                        edge.mandatory,
                        format!("impact mapping: code area `{area_id}` -> `{}`", edge.lane),
                    );
                }
            }

            if area_id == "fsqlite-harness" {
                add_lane_reason(
                    &mut acc,
                    CiLane::SchemaValidation,
                    false,
                    "harness/tooling change: schema validation advisory".to_owned(),
                );
                add_lane_reason(
                    &mut acc,
                    CiLane::CoverageDrift,
                    false,
                    "harness/tooling change: coverage drift advisory".to_owned(),
                );
            }
        }
    }

    let mut decisions = Vec::new();
    for lane in CiLane::ALL {
        if let Some(entry) = acc.get(&lane) {
            decisions.push(LaneDecision {
                lane,
                mandatory: entry.mandatory,
                reasons: entry.reasons.iter().cloned().collect(),
            });
        }
    }

    LaneSelectionReport {
        schema_version: LANE_SELECTOR_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        changed_paths: normalized_paths,
        resolved_code_areas: resolved_ids,
        unresolved_paths,
        fallback_full_suite,
        decisions,
    }
}

// ---------------------------------------------------------------------------
// Lane selection orchestrator (bd-mblr.7.9)
// ---------------------------------------------------------------------------

/// Bead identifier for the parent lane selection orchestrator.
pub const LANE_SELECTION_BEAD_ID: &str = "bd-mblr.7.9";

/// Overall verdict for a lane selection audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LaneSelectionVerdict {
    /// All scenarios resolved, safety floor enforced.
    Pass,
    /// Some unresolved paths but safety floor held.
    Warning,
    /// Safety floor compromised or critical failures.
    Fail,
}

impl std::fmt::Display for LaneSelectionVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => write!(f, "PASS"),
            Self::Warning => write!(f, "WARNING"),
            Self::Fail => write!(f, "FAIL"),
        }
    }
}

/// Configuration for the lane selection audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneSelectionAuditConfig {
    /// Test scenarios: each is a list of changed paths to evaluate.
    pub test_scenarios: Vec<Vec<String>>,
    /// Maximum allowed unresolved paths across all scenarios.
    pub max_unresolved_paths: usize,
    /// Whether fallback to full suite is acceptable.
    pub allow_fallback: bool,
}

impl Default for LaneSelectionAuditConfig {
    fn default() -> Self {
        Self {
            test_scenarios: vec![
                vec!["crates/fsqlite-pager/src/lib.rs".to_owned()],
                vec!["crates/fsqlite-btree/src/lib.rs".to_owned()],
                vec!["crates/fsqlite-parser/src/lib.rs".to_owned()],
                vec!["crates/fsqlite-mvcc/src/lib.rs".to_owned()],
                vec![
                    "crates/fsqlite-vdbe/src/lib.rs".to_owned(),
                    "crates/fsqlite-planner/src/lib.rs".to_owned(),
                ],
            ],
            max_unresolved_paths: 5,
            allow_fallback: true,
        }
    }
}

/// Per-scenario result within the audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneSelectionScenarioResult {
    /// Changed paths for this scenario.
    pub changed_paths: Vec<String>,
    /// Resolved code areas.
    pub resolved_areas: usize,
    /// Unresolved paths.
    pub unresolved_count: usize,
    /// Whether fallback was triggered.
    pub fallback_triggered: bool,
    /// Number of lanes selected.
    pub lanes_selected: usize,
    /// The lane selection report.
    pub report: LaneSelectionReport,
}

/// Aggregated lane selection audit report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneSelectionAuditReport {
    /// Schema version.
    pub schema_version: u32,
    /// Bead ID.
    pub bead_id: String,
    /// Overall verdict.
    pub verdict: LaneSelectionVerdict,
    /// Per-scenario results.
    pub scenario_results: Vec<LaneSelectionScenarioResult>,
    /// Total scenarios evaluated.
    pub total_scenarios: usize,
    /// Total unresolved paths across all scenarios.
    pub total_unresolved: usize,
    /// Total fallback triggers.
    pub total_fallbacks: usize,
    /// Impact graph validation errors (should be empty).
    pub graph_validation_errors: Vec<String>,
    /// Summary for triage.
    pub summary: String,
}

impl LaneSelectionAuditReport {
    /// Render a one-line triage summary.
    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "{}: {} scenarios, {} unresolved, {} fallbacks, {} graph errors",
            self.verdict,
            self.total_scenarios,
            self.total_unresolved,
            self.total_fallbacks,
            self.graph_validation_errors.len(),
        )
    }

    /// Whether the audit passed.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.verdict == LaneSelectionVerdict::Pass
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Write a lane selection audit report to a file.
pub fn write_lane_audit_report(
    path: &std::path::Path,
    report: &LaneSelectionAuditReport,
) -> Result<(), String> {
    let json = report.to_json().map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("write {}: {e}", path.display()))
}

/// Load a lane selection audit report from a file.
pub fn load_lane_audit_report(path: &std::path::Path) -> Result<LaneSelectionAuditReport, String> {
    let data =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    LaneSelectionAuditReport::from_json(&data).map_err(|e| format!("parse {}: {e}", path.display()))
}

/// Run the lane selection audit: evaluate multiple scenarios against the impact graph.
#[must_use]
pub fn run_lane_selection_audit(config: &LaneSelectionAuditConfig) -> LaneSelectionAuditReport {
    let graph = ImpactGraph::canonical();
    let graph_errors = graph.validate();

    let mut scenario_results = Vec::with_capacity(config.test_scenarios.len());
    let mut total_unresolved: usize = 0;
    let mut total_fallbacks: usize = 0;

    for paths in &config.test_scenarios {
        let report = select_ci_lanes_for_paths(paths, &graph, None);
        let unresolved_count = report.unresolved_paths.len();
        let fallback_triggered = report.fallback_full_suite;
        let lanes_selected = report.decisions.len();
        let resolved_areas = report.resolved_code_areas.len();

        total_unresolved += unresolved_count;
        if fallback_triggered {
            total_fallbacks += 1;
        }

        scenario_results.push(LaneSelectionScenarioResult {
            changed_paths: paths.clone(),
            resolved_areas,
            unresolved_count,
            fallback_triggered,
            lanes_selected,
            report,
        });
    }

    let verdict = if !graph_errors.is_empty() {
        LaneSelectionVerdict::Fail
    } else if total_unresolved > config.max_unresolved_paths {
        LaneSelectionVerdict::Fail
    } else if !config.allow_fallback && total_fallbacks > 0 {
        LaneSelectionVerdict::Fail
    } else if total_unresolved > 0 || total_fallbacks > 0 {
        LaneSelectionVerdict::Warning
    } else {
        LaneSelectionVerdict::Pass
    };

    let summary = format!(
        "Lane audit: {} scenarios, {} unresolved, {} fallbacks, {} graph errors, verdict={}",
        config.test_scenarios.len(),
        total_unresolved,
        total_fallbacks,
        graph_errors.len(),
        verdict,
    );

    LaneSelectionAuditReport {
        schema_version: 1,
        bead_id: LANE_SELECTION_BEAD_ID.to_owned(),
        verdict,
        scenario_results,
        total_scenarios: config.test_scenarios.len(),
        total_unresolved,
        total_fallbacks,
        graph_validation_errors: graph_errors,
        summary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact_graph::ImpactGraph;

    #[test]
    fn safety_floor_present_for_docs_only_changes() {
        let graph = ImpactGraph::canonical();
        let report = select_ci_lanes_for_paths(&["docs/test-realism/README.md"], &graph, None);
        let lanes = report.selected_lanes();

        assert!(lanes.contains(&CiLane::Unit));
        assert!(lanes.contains(&CiLane::E2eCorrectness));
        assert!(lanes.contains(&CiLane::E2eDifferential));
        assert!(lanes.contains(&CiLane::E2eRecovery));
        assert!(!report.fallback_full_suite);
    }

    #[test]
    fn unresolved_code_path_forces_full_suite_fallback() {
        let graph = ImpactGraph::canonical();
        let report =
            select_ci_lanes_for_paths(&["crates/unknown-subsystem/src/lib.rs"], &graph, None);

        assert!(report.fallback_full_suite);
        assert_eq!(report.selected_lanes().len(), CiLane::ALL.len());
        assert!(
            report
                .unresolved_paths
                .contains(&"crates/unknown-subsystem/src/lib.rs".to_owned())
        );
    }

    #[test]
    fn output_is_deterministic_for_permuted_input() {
        let graph = ImpactGraph::canonical();
        let report_a = select_ci_lanes_for_paths(
            &[
                "crates/fsqlite-mvcc/src/witness_plane.rs",
                "crates/fsqlite-parser/src/lib.rs",
            ],
            &graph,
            None,
        );
        let report_b = select_ci_lanes_for_paths(
            &[
                "crates/fsqlite-parser/src/lib.rs",
                "crates/fsqlite-mvcc/src/witness_plane.rs",
            ],
            &graph,
            None,
        );

        assert_eq!(report_a, report_b);
    }

    #[test]
    fn json_roundtrip_preserves_report() {
        let graph = ImpactGraph::canonical();
        let report =
            select_ci_lanes_for_paths(&["crates/fsqlite-core/src/connection.rs"], &graph, None);
        let json = report.to_json().expect("serialize");
        let restored = LaneSelectionReport::from_json(&json).expect("deserialize");
        assert_eq!(report, restored);
    }
}
