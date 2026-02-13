//! Unified evidence index schema (bd-mblr.7.5.1).
//!
//! Defines a searchable index over run/scenario/invariant/log/artifact
//! relationships consumed by the failure forensics navigator (bd-mblr.7.5).
//!
//! # Purpose
//!
//! After a test run completes, artifacts scatter across multiple locations:
//! log files, failure bundles, coverage reports, differential outputs.  The
//! evidence index unifies these into a single queryable structure keyed by
//! `run_id`, allowing the forensics CLI (bd-mblr.7.5.2) to answer questions
//! like:
//!
//! - "Which invariants were violated in run X?"
//! - "What scenarios touched code area Y in the last 10 runs?"
//! - "Show me every artifact from the run that first introduced regression Z."
//!
//! # Architecture
//!
//! ```text
//! EvidenceIndex
//!   ├── RunRecord[]           — one per test run
//!   │     ├── run_id, timestamp, seed, profile
//!   │     ├── ScenarioOutcome[]    — per-scenario pass/fail/skip
//!   │     ├── InvariantCheck[]     — per-invariant pass/violate
//!   │     ├── ArtifactRecord[]     — hashed output artifacts
//!   │     └── LogReference[]       — pointers to structured log files
//!   └── query methods (by_run, by_scenario, by_invariant, by_time_range)
//! ```
//!
//! # Persistence
//!
//! The index is designed for JSON-lines (JSONL) append-only storage.
//! Each [`RunRecord`] serializes to a single JSON line for efficient
//! incremental writes and `grep`-friendly searching.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.5.1";

/// Schema version for forward-compatible migrations.
pub const EVIDENCE_INDEX_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Run identification
// ---------------------------------------------------------------------------

/// Unique identifier for a test run.
///
/// Format: `{bead_or_suite}-{YYYYMMDDTHHmmSS}-{short_seed}`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RunId(pub String);

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Outcome types
// ---------------------------------------------------------------------------

/// Outcome of a scenario execution within a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ScenarioVerdict {
    /// Scenario passed all assertions.
    Pass,
    /// Scenario failed one or more assertions.
    Fail,
    /// Scenario was skipped (precondition unmet or filtered).
    Skip,
    /// Scenario timed out before completing.
    Timeout,
    /// Scenario produced a divergence from reference.
    Divergence,
}

impl fmt::Display for ScenarioVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => f.write_str("pass"),
            Self::Fail => f.write_str("fail"),
            Self::Skip => f.write_str("skip"),
            Self::Timeout => f.write_str("timeout"),
            Self::Divergence => f.write_str("divergence"),
        }
    }
}

/// Outcome of an invariant check within a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum InvariantVerdict {
    /// Invariant held throughout the run.
    Held,
    /// Invariant was violated at least once.
    Violated,
    /// Invariant was not checked (not applicable to this run).
    NotChecked,
}

impl fmt::Display for InvariantVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Held => f.write_str("held"),
            Self::Violated => f.write_str("violated"),
            Self::NotChecked => f.write_str("not_checked"),
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario outcome
// ---------------------------------------------------------------------------

/// Per-scenario outcome record within a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioOutcome {
    /// Scenario ID from the traceability matrix (e.g., `"SC-001"`).
    pub scenario_id: String,
    /// Human-readable scenario name.
    pub scenario_name: String,
    /// Verdict for this scenario.
    pub verdict: ScenarioVerdict,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// First-divergence marker, if any.
    pub first_divergence: Option<String>,
    /// Error message, if failed.
    pub error_message: Option<String>,
    /// Code areas exercised by this scenario.
    pub code_areas: Vec<String>,
}

// ---------------------------------------------------------------------------
// Invariant check
// ---------------------------------------------------------------------------

/// Per-invariant check record within a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvariantCheck {
    /// Invariant ID (e.g., `"INV-1"`, `"SOAK-INV-003"`).
    pub invariant_id: String,
    /// Human-readable invariant name.
    pub invariant_name: String,
    /// Verdict.
    pub verdict: InvariantVerdict,
    /// Violation details, if applicable.
    pub violation_detail: Option<String>,
    /// Timestamp of first violation (ISO 8601), if applicable.
    pub violation_timestamp: Option<String>,
}

// ---------------------------------------------------------------------------
// Artifact record
// ---------------------------------------------------------------------------

/// Classification of artifact type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ArtifactKind {
    /// Log file (structured JSON or plain text).
    Log,
    /// Database snapshot or page dump.
    DatabaseSnapshot,
    /// WAL file snapshot.
    WalSnapshot,
    /// Failure bundle (JSON).
    FailureBundle,
    /// Differential output (expected vs actual).
    DiffOutput,
    /// Coverage report.
    CoverageReport,
    /// Performance benchmark results.
    BenchmarkResult,
    /// Replay manifest for bisection.
    ReplayManifest,
    /// Generic artifact.
    Other,
}

impl fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Log => f.write_str("log"),
            Self::DatabaseSnapshot => f.write_str("database_snapshot"),
            Self::WalSnapshot => f.write_str("wal_snapshot"),
            Self::FailureBundle => f.write_str("failure_bundle"),
            Self::DiffOutput => f.write_str("diff_output"),
            Self::CoverageReport => f.write_str("coverage_report"),
            Self::BenchmarkResult => f.write_str("benchmark_result"),
            Self::ReplayManifest => f.write_str("replay_manifest"),
            Self::Other => f.write_str("other"),
        }
    }
}

/// A recorded artifact from a test run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    /// Artifact kind.
    pub kind: ArtifactKind,
    /// Workspace-relative path to the artifact file.
    pub path: String,
    /// BLAKE3 hash of the artifact contents.
    pub content_hash: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Timestamp when artifact was generated (ISO 8601).
    pub generated_at: String,
    /// Optional description.
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// Log reference
// ---------------------------------------------------------------------------

/// Pointer to a structured log file associated with a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogReference {
    /// Workspace-relative path to the log file.
    pub path: String,
    /// Log schema version (e.g., `"1.0.0"`).
    pub schema_version: String,
    /// Number of events in the log.
    pub event_count: u64,
    /// Log phases present.
    pub phases: Vec<String>,
    /// Whether this log contains first-divergence markers.
    pub has_divergence_markers: bool,
}

// ---------------------------------------------------------------------------
// Run record
// ---------------------------------------------------------------------------

/// Complete evidence record for a single test run.
///
/// This is the unit of storage in the evidence index.  Each record
/// serializes to a single JSONL line for append-only persistence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    /// Schema version for this record format.
    pub schema_version: u32,
    /// Unique run identifier.
    pub run_id: RunId,
    /// ISO 8601 timestamp when the run started.
    pub started_at: String,
    /// ISO 8601 timestamp when the run completed (or was aborted).
    pub completed_at: Option<String>,
    /// Deterministic seed used for this run.
    pub seed: u64,
    /// Profile name (e.g., `"soak_heavy"`, `"parity_differential"`).
    pub profile: String,
    /// Git SHA at the time of the run.
    pub git_sha: String,
    /// Rust toolchain version.
    pub toolchain: String,
    /// Platform triple.
    pub platform: String,
    /// Whether the run completed successfully overall.
    pub success: bool,
    /// Per-scenario outcomes.
    pub scenarios: Vec<ScenarioOutcome>,
    /// Per-invariant check results.
    pub invariants: Vec<InvariantCheck>,
    /// Artifacts generated during the run.
    pub artifacts: Vec<ArtifactRecord>,
    /// Log file references.
    pub logs: Vec<LogReference>,
    /// Bead IDs associated with this run.
    pub bead_ids: Vec<String>,
    /// Feature flags active during this run.
    pub feature_flags: Vec<String>,
    /// Fault profile applied, if any.
    pub fault_profile: Option<String>,
    /// Free-form metadata.
    pub metadata: BTreeMap<String, String>,
}

// ---------------------------------------------------------------------------
// Evidence index
// ---------------------------------------------------------------------------

/// Searchable evidence index over test runs.
///
/// In-memory representation; for persistence, records are written
/// one-per-line to a JSONL file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EvidenceIndex {
    /// Schema version.
    pub schema_version: u32,
    /// All run records, keyed by run ID.
    pub runs: BTreeMap<RunId, RunRecord>,
}

impl EvidenceIndex {
    /// Create a new empty index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema_version: EVIDENCE_INDEX_SCHEMA_VERSION,
            runs: BTreeMap::new(),
        }
    }

    /// Insert a run record.  Overwrites if the run ID already exists.
    pub fn insert(&mut self, record: RunRecord) {
        self.runs.insert(record.run_id.clone(), record);
    }

    /// Look up a run by ID.
    #[must_use]
    pub fn get_run(&self, run_id: &RunId) -> Option<&RunRecord> {
        self.runs.get(run_id)
    }

    /// Find all runs that exercised a given scenario.
    #[must_use]
    pub fn runs_by_scenario(&self, scenario_id: &str) -> Vec<&RunRecord> {
        self.runs
            .values()
            .filter(|r| r.scenarios.iter().any(|s| s.scenario_id == scenario_id))
            .collect()
    }

    /// Find all runs where a given invariant was violated.
    #[must_use]
    pub fn runs_with_violation(&self, invariant_id: &str) -> Vec<&RunRecord> {
        self.runs
            .values()
            .filter(|r| {
                r.invariants.iter().any(|i| {
                    i.invariant_id == invariant_id && i.verdict == InvariantVerdict::Violated
                })
            })
            .collect()
    }

    /// Find all runs within a time range (ISO 8601 string comparison).
    #[must_use]
    pub fn runs_in_time_range(&self, start: &str, end: &str) -> Vec<&RunRecord> {
        self.runs
            .values()
            .filter(|r| r.started_at.as_str() >= start && r.started_at.as_str() <= end)
            .collect()
    }

    /// Find all runs that failed.
    #[must_use]
    pub fn failed_runs(&self) -> Vec<&RunRecord> {
        self.runs.values().filter(|r| !r.success).collect()
    }

    /// Find all runs for a specific git SHA.
    #[must_use]
    pub fn runs_by_git_sha(&self, sha: &str) -> Vec<&RunRecord> {
        self.runs.values().filter(|r| r.git_sha == sha).collect()
    }

    /// Find all runs that used a specific fault profile.
    #[must_use]
    pub fn runs_by_fault_profile(&self, profile: &str) -> Vec<&RunRecord> {
        self.runs
            .values()
            .filter(|r| r.fault_profile.as_deref() == Some(profile))
            .collect()
    }

    /// Find all runs that touched a specific code area.
    #[must_use]
    pub fn runs_by_code_area(&self, code_area: &str) -> Vec<&RunRecord> {
        self.runs
            .values()
            .filter(|r| {
                r.scenarios
                    .iter()
                    .any(|s| s.code_areas.iter().any(|a| a == code_area))
            })
            .collect()
    }

    /// Get all unique scenario IDs across all runs.
    #[must_use]
    pub fn all_scenario_ids(&self) -> BTreeSet<String> {
        self.runs
            .values()
            .flat_map(|r| r.scenarios.iter().map(|s| s.scenario_id.clone()))
            .collect()
    }

    /// Get all unique invariant IDs across all runs.
    #[must_use]
    pub fn all_invariant_ids(&self) -> BTreeSet<String> {
        self.runs
            .values()
            .flat_map(|r| r.invariants.iter().map(|i| i.invariant_id.clone()))
            .collect()
    }

    /// Total number of runs in the index.
    #[must_use]
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }
}

// ---------------------------------------------------------------------------
// Index statistics
// ---------------------------------------------------------------------------

/// Summary statistics for the evidence index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Total number of runs.
    pub total_runs: usize,
    /// Number of successful runs.
    pub successful_runs: usize,
    /// Number of failed runs.
    pub failed_runs: usize,
    /// Total number of scenario outcomes across all runs.
    pub total_scenario_outcomes: usize,
    /// Total number of invariant checks across all runs.
    pub total_invariant_checks: usize,
    /// Total number of artifacts across all runs.
    pub total_artifacts: usize,
    /// Number of unique scenarios exercised.
    pub unique_scenarios: usize,
    /// Number of unique invariants checked.
    pub unique_invariants: usize,
    /// Number of invariant violations across all runs.
    pub total_violations: usize,
    /// Scenario verdict distribution.
    pub verdict_distribution: BTreeMap<String, usize>,
}

/// Compute summary statistics for the evidence index.
#[must_use]
pub fn compute_statistics(index: &EvidenceIndex) -> IndexStatistics {
    let total_runs = index.run_count();
    let successful_runs = index.runs.values().filter(|r| r.success).count();
    let failed_runs = total_runs - successful_runs;

    let total_scenario_outcomes: usize = index.runs.values().map(|r| r.scenarios.len()).sum();
    let total_invariant_checks: usize = index.runs.values().map(|r| r.invariants.len()).sum();
    let total_artifacts: usize = index.runs.values().map(|r| r.artifacts.len()).sum();

    let unique_scenarios = index.all_scenario_ids().len();
    let unique_invariants = index.all_invariant_ids().len();

    let total_violations = index
        .runs
        .values()
        .flat_map(|r| r.invariants.iter())
        .filter(|i| i.verdict == InvariantVerdict::Violated)
        .count();

    let mut verdict_distribution = BTreeMap::new();
    for run in index.runs.values() {
        for scenario in &run.scenarios {
            *verdict_distribution
                .entry(format!("{}", scenario.verdict))
                .or_insert(0) += 1;
        }
    }

    IndexStatistics {
        total_runs,
        successful_runs,
        failed_runs,
        total_scenario_outcomes,
        total_invariant_checks,
        total_artifacts,
        unique_scenarios,
        unique_invariants,
        total_violations,
        verdict_distribution,
    }
}

// ---------------------------------------------------------------------------
// JSONL serialization helpers
// ---------------------------------------------------------------------------

/// Serialize a run record to a single JSONL line.
///
/// # Errors
///
/// Returns `serde_json::Error` if serialization fails.
pub fn run_to_jsonl(record: &RunRecord) -> Result<String, serde_json::Error> {
    serde_json::to_string(record)
}

/// Deserialize a run record from a single JSONL line.
///
/// # Errors
///
/// Returns `serde_json::Error` if deserialization fails.
pub fn run_from_jsonl(line: &str) -> Result<RunRecord, serde_json::Error> {
    serde_json::from_str(line)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_run(id: &str, success: bool, seed: u64) -> RunRecord {
        RunRecord {
            schema_version: EVIDENCE_INDEX_SCHEMA_VERSION,
            run_id: RunId(id.to_owned()),
            started_at: "2026-02-13T00:00:00Z".to_owned(),
            completed_at: Some("2026-02-13T00:05:00Z".to_owned()),
            seed,
            profile: "parity_differential".to_owned(),
            git_sha: "abc123".to_owned(),
            toolchain: "nightly-2026-02-10".to_owned(),
            platform: "x86_64-unknown-linux-gnu".to_owned(),
            success,
            scenarios: vec![],
            invariants: vec![],
            artifacts: vec![],
            logs: vec![],
            bead_ids: vec![],
            feature_flags: vec![],
            fault_profile: None,
            metadata: BTreeMap::new(),
        }
    }

    fn sample_scenario(id: &str, verdict: ScenarioVerdict) -> ScenarioOutcome {
        ScenarioOutcome {
            scenario_id: id.to_owned(),
            scenario_name: format!("Scenario {id}"),
            verdict,
            duration_ms: 100,
            first_divergence: None,
            error_message: None,
            code_areas: vec![],
        }
    }

    fn sample_invariant(id: &str, verdict: InvariantVerdict) -> InvariantCheck {
        InvariantCheck {
            invariant_id: id.to_owned(),
            invariant_name: format!("Invariant {id}"),
            verdict,
            violation_detail: None,
            violation_timestamp: None,
        }
    }

    fn sample_artifact(kind: ArtifactKind, path: &str) -> ArtifactRecord {
        ArtifactRecord {
            kind,
            path: path.to_owned(),
            content_hash: "blake3_placeholder_hash".to_owned(),
            size_bytes: 1024,
            generated_at: "2026-02-13T00:03:00Z".to_owned(),
            description: None,
        }
    }

    #[test]
    fn empty_index_has_zero_runs() {
        let index = EvidenceIndex::new();
        assert_eq!(index.run_count(), 0);
        assert_eq!(index.schema_version, EVIDENCE_INDEX_SCHEMA_VERSION);
    }

    #[test]
    fn insert_and_retrieve_run() {
        let mut index = EvidenceIndex::new();
        let run = sample_run("run-001", true, 42);
        index.insert(run.clone());
        assert_eq!(index.run_count(), 1);
        let retrieved = index.get_run(&RunId("run-001".to_owned()));
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().seed, 42);
    }

    #[test]
    fn insert_overwrites_existing_run() {
        let mut index = EvidenceIndex::new();
        let run1 = sample_run("run-001", true, 42);
        let mut run2 = sample_run("run-001", false, 99);
        run2.profile = "updated".to_owned();
        index.insert(run1);
        index.insert(run2);
        assert_eq!(index.run_count(), 1);
        let retrieved = index.get_run(&RunId("run-001".to_owned())).unwrap();
        assert_eq!(retrieved.seed, 99);
        assert!(!retrieved.success);
    }

    #[test]
    fn query_runs_by_scenario() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        run1.scenarios
            .push(sample_scenario("SC-001", ScenarioVerdict::Pass));
        run1.scenarios
            .push(sample_scenario("SC-002", ScenarioVerdict::Pass));
        let mut run2 = sample_run("run-002", true, 2);
        run2.scenarios
            .push(sample_scenario("SC-002", ScenarioVerdict::Fail));
        let run3 = sample_run("run-003", true, 3);
        index.insert(run1);
        index.insert(run2);
        index.insert(run3);

        let sc002_runs = index.runs_by_scenario("SC-002");
        assert_eq!(sc002_runs.len(), 2);
        let sc001_runs = index.runs_by_scenario("SC-001");
        assert_eq!(sc001_runs.len(), 1);
        let sc999_runs = index.runs_by_scenario("SC-999");
        assert!(sc999_runs.is_empty());
    }

    #[test]
    fn query_runs_with_violation() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", false, 1);
        run1.invariants
            .push(sample_invariant("INV-1", InvariantVerdict::Violated));
        run1.invariants
            .push(sample_invariant("INV-2", InvariantVerdict::Held));
        let mut run2 = sample_run("run-002", true, 2);
        run2.invariants
            .push(sample_invariant("INV-1", InvariantVerdict::Held));
        index.insert(run1);
        index.insert(run2);

        let violated = index.runs_with_violation("INV-1");
        assert_eq!(violated.len(), 1);
        assert_eq!(violated[0].run_id, RunId("run-001".to_owned()));
    }

    #[test]
    fn query_runs_in_time_range() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        run1.started_at = "2026-02-12T10:00:00Z".to_owned();
        let mut run2 = sample_run("run-002", true, 2);
        run2.started_at = "2026-02-13T10:00:00Z".to_owned();
        let mut run3 = sample_run("run-003", true, 3);
        run3.started_at = "2026-02-14T10:00:00Z".to_owned();
        index.insert(run1);
        index.insert(run2);
        index.insert(run3);

        let in_range = index.runs_in_time_range("2026-02-13T00:00:00Z", "2026-02-13T23:59:59Z");
        assert_eq!(in_range.len(), 1);
        assert_eq!(in_range[0].run_id, RunId("run-002".to_owned()));
    }

    #[test]
    fn query_failed_runs() {
        let mut index = EvidenceIndex::new();
        index.insert(sample_run("run-001", true, 1));
        index.insert(sample_run("run-002", false, 2));
        index.insert(sample_run("run-003", false, 3));
        let failed = index.failed_runs();
        assert_eq!(failed.len(), 2);
    }

    #[test]
    fn query_runs_by_git_sha() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        run1.git_sha = "sha_a".to_owned();
        let mut run2 = sample_run("run-002", true, 2);
        run2.git_sha = "sha_b".to_owned();
        let mut run3 = sample_run("run-003", true, 3);
        run3.git_sha = "sha_a".to_owned();
        index.insert(run1);
        index.insert(run2);
        index.insert(run3);

        let sha_a = index.runs_by_git_sha("sha_a");
        assert_eq!(sha_a.len(), 2);
    }

    #[test]
    fn query_runs_by_fault_profile() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        run1.fault_profile = Some("FP-001".to_owned());
        let run2 = sample_run("run-002", true, 2);
        index.insert(run1);
        index.insert(run2);

        let with_fault = index.runs_by_fault_profile("FP-001");
        assert_eq!(with_fault.len(), 1);
        let without = index.runs_by_fault_profile("FP-999");
        assert!(without.is_empty());
    }

    #[test]
    fn query_runs_by_code_area() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        let mut scenario = sample_scenario("SC-001", ScenarioVerdict::Pass);
        scenario.code_areas = vec!["fsqlite-mvcc".to_owned(), "fsqlite-wal".to_owned()];
        run1.scenarios.push(scenario);
        let run2 = sample_run("run-002", true, 2);
        index.insert(run1);
        index.insert(run2);

        let mvcc_runs = index.runs_by_code_area("fsqlite-mvcc");
        assert_eq!(mvcc_runs.len(), 1);
        let parser_runs = index.runs_by_code_area("fsqlite-parser");
        assert!(parser_runs.is_empty());
    }

    #[test]
    fn all_scenario_ids_across_runs() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        run1.scenarios
            .push(sample_scenario("SC-001", ScenarioVerdict::Pass));
        run1.scenarios
            .push(sample_scenario("SC-002", ScenarioVerdict::Pass));
        let mut run2 = sample_run("run-002", true, 2);
        run2.scenarios
            .push(sample_scenario("SC-002", ScenarioVerdict::Fail));
        run2.scenarios
            .push(sample_scenario("SC-003", ScenarioVerdict::Pass));
        index.insert(run1);
        index.insert(run2);

        let ids = index.all_scenario_ids();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains("SC-001"));
        assert!(ids.contains("SC-002"));
        assert!(ids.contains("SC-003"));
    }

    #[test]
    fn all_invariant_ids_across_runs() {
        let mut index = EvidenceIndex::new();
        let mut run1 = sample_run("run-001", true, 1);
        run1.invariants
            .push(sample_invariant("INV-1", InvariantVerdict::Held));
        let mut run2 = sample_run("run-002", true, 2);
        run2.invariants
            .push(sample_invariant("INV-2", InvariantVerdict::Held));
        index.insert(run1);
        index.insert(run2);

        let ids = index.all_invariant_ids();
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn statistics_empty_index() {
        let index = EvidenceIndex::new();
        let stats = compute_statistics(&index);
        assert_eq!(stats.total_runs, 0);
        assert_eq!(stats.successful_runs, 0);
        assert_eq!(stats.failed_runs, 0);
        assert_eq!(stats.total_violations, 0);
    }

    #[test]
    fn statistics_with_data() {
        let mut index = EvidenceIndex::new();

        let mut run1 = sample_run("run-001", true, 1);
        run1.scenarios
            .push(sample_scenario("SC-001", ScenarioVerdict::Pass));
        run1.scenarios
            .push(sample_scenario("SC-002", ScenarioVerdict::Pass));
        run1.invariants
            .push(sample_invariant("INV-1", InvariantVerdict::Held));
        run1.artifacts
            .push(sample_artifact(ArtifactKind::Log, "logs/run-001.jsonl"));

        let mut run2 = sample_run("run-002", false, 2);
        run2.scenarios
            .push(sample_scenario("SC-001", ScenarioVerdict::Fail));
        run2.invariants
            .push(sample_invariant("INV-1", InvariantVerdict::Violated));
        run2.invariants
            .push(sample_invariant("INV-2", InvariantVerdict::Held));
        run2.artifacts.push(sample_artifact(
            ArtifactKind::FailureBundle,
            "bundles/run-002.json",
        ));
        run2.artifacts.push(sample_artifact(
            ArtifactKind::DiffOutput,
            "diffs/run-002.diff",
        ));

        index.insert(run1);
        index.insert(run2);

        let stats = compute_statistics(&index);
        assert_eq!(stats.total_runs, 2);
        assert_eq!(stats.successful_runs, 1);
        assert_eq!(stats.failed_runs, 1);
        assert_eq!(stats.total_scenario_outcomes, 3);
        assert_eq!(stats.total_invariant_checks, 3);
        assert_eq!(stats.total_artifacts, 3);
        assert_eq!(stats.unique_scenarios, 2);
        assert_eq!(stats.unique_invariants, 2);
        assert_eq!(stats.total_violations, 1);
        assert_eq!(*stats.verdict_distribution.get("pass").unwrap_or(&0), 2);
        assert_eq!(*stats.verdict_distribution.get("fail").unwrap_or(&0), 1);
    }

    #[test]
    fn jsonl_round_trip() {
        let mut run = sample_run("run-jsonl", true, 42);
        run.scenarios
            .push(sample_scenario("SC-001", ScenarioVerdict::Pass));
        run.invariants
            .push(sample_invariant("INV-1", InvariantVerdict::Held));
        run.artifacts
            .push(sample_artifact(ArtifactKind::Log, "logs/test.jsonl"));

        let line = run_to_jsonl(&run).expect("serialize to JSONL");
        assert!(
            !line.contains('\n'),
            "JSONL line should not contain newlines"
        );

        let restored = run_from_jsonl(&line).expect("deserialize from JSONL");
        assert_eq!(run.run_id, restored.run_id);
        assert_eq!(run.seed, restored.seed);
        assert_eq!(run.scenarios.len(), restored.scenarios.len());
        assert_eq!(run.invariants.len(), restored.invariants.len());
        assert_eq!(run.artifacts.len(), restored.artifacts.len());
    }

    #[test]
    fn full_index_json_round_trip() {
        let mut index = EvidenceIndex::new();
        index.insert(sample_run("run-001", true, 1));
        index.insert(sample_run("run-002", false, 2));

        let json = serde_json::to_string_pretty(&index).expect("serialize index");
        let restored: EvidenceIndex = serde_json::from_str(&json).expect("deserialize index");
        assert_eq!(index.run_count(), restored.run_count());
    }

    #[test]
    fn scenario_verdict_display() {
        assert_eq!(format!("{}", ScenarioVerdict::Pass), "pass");
        assert_eq!(format!("{}", ScenarioVerdict::Fail), "fail");
        assert_eq!(format!("{}", ScenarioVerdict::Skip), "skip");
        assert_eq!(format!("{}", ScenarioVerdict::Timeout), "timeout");
        assert_eq!(format!("{}", ScenarioVerdict::Divergence), "divergence");
    }

    #[test]
    fn invariant_verdict_display() {
        assert_eq!(format!("{}", InvariantVerdict::Held), "held");
        assert_eq!(format!("{}", InvariantVerdict::Violated), "violated");
        assert_eq!(format!("{}", InvariantVerdict::NotChecked), "not_checked");
    }

    #[test]
    fn artifact_kind_display() {
        let kinds = [
            ArtifactKind::Log,
            ArtifactKind::DatabaseSnapshot,
            ArtifactKind::WalSnapshot,
            ArtifactKind::FailureBundle,
            ArtifactKind::DiffOutput,
            ArtifactKind::CoverageReport,
            ArtifactKind::BenchmarkResult,
            ArtifactKind::ReplayManifest,
            ArtifactKind::Other,
        ];
        for kind in kinds {
            let s = format!("{kind}");
            assert!(!s.is_empty(), "Empty display for {kind:?}");
        }
    }

    #[test]
    fn run_id_display() {
        let id = RunId("test-run-001".to_owned());
        assert_eq!(format!("{id}"), "test-run-001");
    }

    #[test]
    fn log_reference_fields() {
        let log_ref = LogReference {
            path: "logs/run-001.jsonl".to_owned(),
            schema_version: "1.0.0".to_owned(),
            event_count: 150,
            phases: vec![
                "setup".to_owned(),
                "execute".to_owned(),
                "validate".to_owned(),
            ],
            has_divergence_markers: true,
        };
        assert_eq!(log_ref.event_count, 150);
        assert!(log_ref.has_divergence_markers);
        assert_eq!(log_ref.phases.len(), 3);
    }

    #[test]
    fn statistics_json_round_trip() {
        let mut index = EvidenceIndex::new();
        index.insert(sample_run("run-001", true, 1));
        let stats = compute_statistics(&index);
        let json = serde_json::to_string(&stats).expect("serialize stats");
        let restored: IndexStatistics = serde_json::from_str(&json).expect("deserialize stats");
        assert_eq!(stats.total_runs, restored.total_runs);
    }

    #[test]
    fn multiple_artifacts_per_run() {
        let mut run = sample_run("run-multi", true, 1);
        run.artifacts
            .push(sample_artifact(ArtifactKind::Log, "a.jsonl"));
        run.artifacts
            .push(sample_artifact(ArtifactKind::FailureBundle, "b.json"));
        run.artifacts
            .push(sample_artifact(ArtifactKind::DiffOutput, "c.diff"));
        run.artifacts
            .push(sample_artifact(ArtifactKind::DatabaseSnapshot, "d.db"));

        let mut index = EvidenceIndex::new();
        index.insert(run);

        let stats = compute_statistics(&index);
        assert_eq!(stats.total_artifacts, 4);
    }

    #[test]
    fn run_with_all_fields_populated() {
        let run = RunRecord {
            schema_version: EVIDENCE_INDEX_SCHEMA_VERSION,
            run_id: RunId("full-run".to_owned()),
            started_at: "2026-02-13T00:00:00Z".to_owned(),
            completed_at: Some("2026-02-13T01:00:00Z".to_owned()),
            seed: 12_345,
            profile: "soak_heavy".to_owned(),
            git_sha: "deadbeef".to_owned(),
            toolchain: "nightly-2026-02-10".to_owned(),
            platform: "x86_64-unknown-linux-gnu".to_owned(),
            success: false,
            scenarios: vec![
                sample_scenario("SC-001", ScenarioVerdict::Pass),
                sample_scenario("SC-002", ScenarioVerdict::Fail),
                sample_scenario("SC-003", ScenarioVerdict::Skip),
            ],
            invariants: vec![
                sample_invariant("INV-1", InvariantVerdict::Held),
                sample_invariant("INV-2", InvariantVerdict::Violated),
                sample_invariant("INV-3", InvariantVerdict::NotChecked),
            ],
            artifacts: vec![
                sample_artifact(ArtifactKind::Log, "logs/full.jsonl"),
                sample_artifact(ArtifactKind::FailureBundle, "bundles/full.json"),
            ],
            logs: vec![LogReference {
                path: "logs/full.jsonl".to_owned(),
                schema_version: "1.0.0".to_owned(),
                event_count: 500,
                phases: vec![
                    "setup".to_owned(),
                    "execute".to_owned(),
                    "validate".to_owned(),
                    "teardown".to_owned(),
                ],
                has_divergence_markers: true,
            }],
            bead_ids: vec!["bd-mblr.7.5.1".to_owned()],
            feature_flags: vec!["ext-json1".to_owned(), "ext-fts5".to_owned()],
            fault_profile: Some("FP-003-power-loss".to_owned()),
            metadata: {
                let mut m = BTreeMap::new();
                m.insert("ci_pipeline".to_owned(), "nightly".to_owned());
                m
            },
        };

        let line = run_to_jsonl(&run).expect("serialize full run");
        let restored = run_from_jsonl(&line).expect("deserialize full run");
        assert_eq!(run.run_id, restored.run_id);
        assert_eq!(run.scenarios.len(), restored.scenarios.len());
        assert_eq!(run.invariants.len(), restored.invariants.len());
        assert_eq!(run.artifacts.len(), restored.artifacts.len());
        assert_eq!(run.logs.len(), restored.logs.len());
        assert_eq!(run.bead_ids, restored.bead_ids);
        assert_eq!(run.feature_flags, restored.feature_flags);
        assert_eq!(run.fault_profile, restored.fault_profile);
        assert_eq!(run.metadata, restored.metadata);
    }
}
