//! Dependency-aware execution wave planning and staffing lanes (bd-1dp9.9.1).
//!
//! This module turns a backlog dependency graph into deterministic execution
//! waves with explicit lane, milestone, and ownership assignments.
//!
//! The planner is intentionally deterministic:
//! - input tasks are keyed by stable ids,
//! - topological traversal always picks the lexicographically smallest ready id,
//! - all output collections are emitted in canonical sorted order.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bead identifier for traceability.
pub const BEAD_ID: &str = "bd-1dp9.9.1";
/// JSON schema version for serialized wave plans.
pub const EXECUTION_WAVE_SCHEMA_VERSION: &str = "1.0.0";

/// Execution lane used for staffing/parallelization decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLane {
    /// Program-level orchestration and integration work.
    ProgramOrchestration,
    /// Parser/planner/VDBE and SQL semantics closure work.
    SqlPipeline,
    /// Pager/WAL/MVCC/B-tree and durability/storage work.
    StorageMvcc,
    /// Extension parity and extension-specific closure work.
    Extensions,
    /// CI, evidence, logging, and gate-hardening work.
    QualityGates,
}

impl ExecutionLane {
    /// Stable display name for logs and artifacts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProgramOrchestration => "program_orchestration",
            Self::SqlPipeline => "sql_pipeline",
            Self::StorageMvcc => "storage_mvcc",
            Self::Extensions => "extensions",
            Self::QualityGates => "quality_gates",
        }
    }
}

impl std::fmt::Display for ExecutionLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Input task used by the execution-wave planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveTaskInput {
    /// Stable task identifier (for example, a bead id).
    pub id: String,
    /// Human-readable task title.
    pub title: String,
    /// Priority, where `0` is highest urgency.
    pub priority: u8,
    /// Risk score in `[0, 100]`.
    pub risk_score: u8,
    /// Owner identity. Empty means unassigned.
    pub owner: String,
    /// Label set used for lane classification.
    pub labels: Vec<String>,
    /// Upstream dependencies that must close before this task starts.
    pub blocked_by: Vec<String>,
}

/// Per-owner staffing summary for a milestone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerAllocation {
    /// Owner identifier.
    pub owner: String,
    /// Number of tasks assigned in this milestone.
    pub task_count: usize,
    /// Distinct lanes touched by this owner in this milestone.
    pub lanes: Vec<ExecutionLane>,
}

/// Assignment of one task into a wave, lane, milestone, and owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveTaskAssignment {
    /// Stable task identifier.
    pub task_id: String,
    /// Human-readable title.
    pub title: String,
    /// Assigned execution lane.
    pub lane: ExecutionLane,
    /// Owner identifier (`UNASSIGNED` when missing).
    pub owner: String,
    /// Priority (`0` highest).
    pub priority: u8,
    /// Risk score in `[0, 100]`.
    pub risk_score: u8,
    /// Wave index (`0`-based).
    pub wave_index: usize,
    /// Longest-path length from this task to a sink node.
    pub critical_path_len: usize,
    /// Number of downstream tasks this node directly unblocks.
    pub unblocks_count: usize,
    /// Immediate dependencies for this task.
    pub blocked_by: Vec<String>,
}

/// Lane-level allocation summary for a single wave.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneAllocation {
    /// Lane identifier.
    pub lane: ExecutionLane,
    /// Tasks assigned to this lane in deterministic order.
    pub task_ids: Vec<String>,
    /// Owners scheduled in this lane.
    pub owners: Vec<String>,
    /// Count of tasks with `risk_score >= 75`.
    pub high_risk_task_count: usize,
}

/// One wave in the execution plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionWave {
    /// Wave index (`0`-based).
    pub wave_index: usize,
    /// Deterministic assignments for this wave.
    pub tasks: Vec<WaveTaskAssignment>,
    /// Per-lane staffing summary for this wave.
    pub lane_allocations: Vec<LaneAllocation>,
}

/// Blocking milestone generated per wave.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveMilestone {
    /// Stable milestone identifier.
    pub milestone_id: String,
    /// Wave index associated with this milestone.
    pub wave_index: usize,
    /// Human-readable milestone title.
    pub title: String,
    /// Highest-impact blocking tasks in this wave.
    pub blocking_tasks: Vec<String>,
    /// Explicit ownership map for staffing.
    pub owner_allocations: Vec<OwnerAllocation>,
}

/// Deterministic execution-wave plan artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionWavePlan {
    /// Artifact schema version.
    pub schema_version: String,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Number of tasks represented in this plan.
    pub tasks_total: usize,
    /// Length of the global critical path (in tasks).
    pub critical_path_length: usize,
    /// Wave decomposition.
    pub waves: Vec<ExecutionWave>,
    /// Milestones aligned to waves.
    pub milestones: Vec<WaveMilestone>,
    /// Tasks with no explicit owner.
    pub unassigned_task_ids: Vec<String>,
}

impl ExecutionWavePlan {
    /// Serialize plan to deterministic pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if JSON serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize a plan from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when input JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Parse open/backlog tasks from a `.beads/issues.jsonl` payload.
///
/// Only `open` and `in_progress` issues are included.
///
/// # Errors
///
/// Returns an error if any line cannot be parsed.
pub fn load_wave_inputs_from_issues_jsonl(jsonl: &str) -> Result<Vec<WaveTaskInput>, String> {
    #[derive(Debug, Deserialize)]
    struct JsonlDependency {
        #[serde(default)]
        depends_on_id: String,
        #[serde(rename = "type", default)]
        dep_type: String,
    }

    #[derive(Debug, Deserialize)]
    struct JsonlIssue {
        id: String,
        title: String,
        #[serde(default)]
        priority: u8,
        status: String,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        dependencies: Vec<JsonlDependency>,
    }

    let mut tasks = Vec::new();
    for (idx, line) in jsonl.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let issue: JsonlIssue = serde_json::from_str(trimmed).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=jsonl_parse_failed line={} error={error}",
                idx + 1
            )
        })?;

        if issue.status != "open" && issue.status != "in_progress" {
            continue;
        }

        let blocked_by: Vec<String> = issue
            .dependencies
            .iter()
            .filter(|dependency| dependency.dep_type.is_empty() || dependency.dep_type == "blocks")
            .map(|dependency| dependency.depends_on_id.clone())
            .filter(|dependency_id| !dependency_id.trim().is_empty())
            .collect();

        let risk_score = derive_risk_score(issue.priority, &issue.labels);
        tasks.push(WaveTaskInput {
            id: issue.id,
            title: issue.title,
            priority: issue.priority,
            risk_score,
            owner: String::new(),
            labels: issue.labels,
            blocked_by,
        });
    }
    tasks.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(tasks)
}

/// Build a deterministic execution-wave plan from dependency-linked tasks.
///
/// # Errors
///
/// Returns an error when:
/// - task ids are duplicated,
/// - a dependency references an unknown task id,
/// - the dependency graph contains a cycle.
pub fn build_execution_wave_plan(tasks: &[WaveTaskInput]) -> Result<ExecutionWavePlan, String> {
    let mut task_map: BTreeMap<String, &WaveTaskInput> = BTreeMap::new();
    for task in tasks {
        if task.id.trim().is_empty() {
            return Err(format!("bead_id={BEAD_ID} case=empty_task_id"));
        }
        if task_map.insert(task.id.clone(), task).is_some() {
            return Err(format!(
                "bead_id={BEAD_ID} case=duplicate_task_id task_id={}",
                task.id
            ));
        }
    }

    if task_map.is_empty() {
        return Err(format!("bead_id={BEAD_ID} case=no_tasks"));
    }

    for task in tasks {
        for dependency_id in &task.blocked_by {
            if !task_map.contains_key(dependency_id) {
                return Err(format!(
                    "bead_id={BEAD_ID} case=unknown_dependency task_id={} depends_on={dependency_id}",
                    task.id
                ));
            }
        }
    }

    let mut indegree: BTreeMap<String, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for task in tasks {
        indegree.insert(task.id.clone(), task.blocked_by.len());
        dependents.entry(task.id.clone()).or_default();
    }
    for task in tasks {
        for dependency_id in &task.blocked_by {
            dependents
                .entry(dependency_id.clone())
                .or_default()
                .push(task.id.clone());
        }
    }
    for children in dependents.values_mut() {
        children.sort();
        children.dedup();
    }

    let mut ready: BTreeSet<String> = indegree
        .iter()
        .filter(|(_task_id, degree)| **degree == 0)
        .map(|(task_id, _degree)| task_id.clone())
        .collect();

    let mut topo = Vec::with_capacity(tasks.len());
    let mut wave_index_by_id: BTreeMap<String, usize> = BTreeMap::new();

    while let Some(task_id) = ready.pop_first() {
        let task = task_map.get(&task_id).ok_or_else(|| {
            format!("bead_id={BEAD_ID} case=internal_missing_task task_id={task_id}")
        })?;

        let wave_index = task
            .blocked_by
            .iter()
            .filter_map(|dependency_id| wave_index_by_id.get(dependency_id).copied())
            .max()
            .map_or(0, |max_parent_wave| max_parent_wave + 1);
        wave_index_by_id.insert(task_id.clone(), wave_index);
        topo.push(task_id.clone());

        if let Some(children) = dependents.get(&task_id) {
            for child_id in children {
                let degree = indegree.get_mut(child_id).ok_or_else(|| {
                    format!("bead_id={BEAD_ID} case=internal_missing_child child_id={child_id}")
                })?;
                *degree = degree.saturating_sub(1);
                if *degree == 0 {
                    ready.insert(child_id.clone());
                }
            }
        }
    }

    if topo.len() != tasks.len() {
        return Err(format!(
            "bead_id={BEAD_ID} case=dependency_cycle_detected remaining={}",
            tasks.len() - topo.len()
        ));
    }

    let mut critical_path_len_by_id: BTreeMap<String, usize> = BTreeMap::new();
    for task_id in topo.iter().rev() {
        let child_depth = dependents
            .get(task_id)
            .into_iter()
            .flat_map(|children| children.iter())
            .filter_map(|child_id| critical_path_len_by_id.get(child_id).copied())
            .max()
            .unwrap_or(0);
        critical_path_len_by_id.insert(task_id.clone(), child_depth + 1);
    }
    let global_critical_path = critical_path_len_by_id.values().copied().max().unwrap_or(0);

    let mut assignments_by_wave: BTreeMap<usize, Vec<WaveTaskAssignment>> = BTreeMap::new();
    let mut unassigned = BTreeSet::new();

    for task_id in &topo {
        let task = task_map.get(task_id).ok_or_else(|| {
            format!(
                "bead_id={BEAD_ID} case=internal_missing_task_during_assignment task_id={task_id}"
            )
        })?;
        let owner = normalize_owner(&task.owner);
        if owner == "UNASSIGNED" {
            unassigned.insert(task_id.clone());
        }

        let wave_index = *wave_index_by_id.get(task_id).ok_or_else(|| {
            format!("bead_id={BEAD_ID} case=internal_missing_wave_index task_id={task_id}")
        })?;
        let critical_path_len = *critical_path_len_by_id.get(task_id).ok_or_else(|| {
            format!("bead_id={BEAD_ID} case=internal_missing_critical_path task_id={task_id}")
        })?;
        let unblocks_count = dependents.get(task_id).map_or(0, std::vec::Vec::len);

        assignments_by_wave
            .entry(wave_index)
            .or_default()
            .push(WaveTaskAssignment {
                task_id: task.id.clone(),
                title: task.title.clone(),
                lane: classify_lane(task),
                owner,
                priority: task.priority,
                risk_score: task.risk_score,
                wave_index,
                critical_path_len,
                unblocks_count,
                blocked_by: task.blocked_by.clone(),
            });
    }

    let mut waves = Vec::new();
    let mut milestones = Vec::new();
    for (wave_index, assignments) in assignments_by_wave {
        let mut sorted_assignments = assignments;
        sorted_assignments.sort_by(|left, right| {
            left.priority
                .cmp(&right.priority)
                .then_with(|| right.risk_score.cmp(&left.risk_score))
                .then_with(|| right.critical_path_len.cmp(&left.critical_path_len))
                .then_with(|| left.task_id.cmp(&right.task_id))
        });

        let lane_allocations = build_lane_allocations(&sorted_assignments);
        let milestone = build_milestone(wave_index, &sorted_assignments);

        waves.push(ExecutionWave {
            wave_index,
            tasks: sorted_assignments,
            lane_allocations,
        });
        milestones.push(milestone);
    }

    Ok(ExecutionWavePlan {
        schema_version: EXECUTION_WAVE_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        tasks_total: tasks.len(),
        critical_path_length: global_critical_path,
        waves,
        milestones,
        unassigned_task_ids: unassigned.into_iter().collect(),
    })
}

fn build_lane_allocations(assignments: &[WaveTaskAssignment]) -> Vec<LaneAllocation> {
    let mut grouped: BTreeMap<ExecutionLane, Vec<&WaveTaskAssignment>> = BTreeMap::new();
    for assignment in assignments {
        grouped.entry(assignment.lane).or_default().push(assignment);
    }

    grouped
        .into_iter()
        .map(|(lane, lane_assignments)| {
            let mut owners = BTreeSet::new();
            let mut task_ids = Vec::with_capacity(lane_assignments.len());
            let mut high_risk_task_count = 0_usize;

            for assignment in lane_assignments {
                owners.insert(assignment.owner.clone());
                task_ids.push(assignment.task_id.clone());
                if assignment.risk_score >= 75 {
                    high_risk_task_count += 1;
                }
            }

            LaneAllocation {
                lane,
                task_ids,
                owners: owners.into_iter().collect(),
                high_risk_task_count,
            }
        })
        .collect()
}

fn build_milestone(wave_index: usize, assignments: &[WaveTaskAssignment]) -> WaveMilestone {
    let mut blocking = assignments
        .iter()
        .filter(|assignment| assignment.unblocks_count > 0)
        .map(|assignment| {
            (
                assignment.task_id.clone(),
                assignment.unblocks_count,
                assignment.priority,
            )
        })
        .collect::<Vec<_>>();
    blocking.sort_by_key(|(task_id, unblocks_count, priority)| {
        (Reverse(*unblocks_count), *priority, task_id.clone())
    });

    let mut owner_map: BTreeMap<String, (usize, BTreeSet<ExecutionLane>)> = BTreeMap::new();
    for assignment in assignments {
        let entry = owner_map
            .entry(assignment.owner.clone())
            .or_insert((0_usize, BTreeSet::new()));
        entry.0 += 1;
        entry.1.insert(assignment.lane);
    }

    let owner_allocations = owner_map
        .into_iter()
        .map(|(owner, (task_count, lanes))| OwnerAllocation {
            owner,
            task_count,
            lanes: lanes.into_iter().collect(),
        })
        .collect();

    WaveMilestone {
        milestone_id: format!("M{}", wave_index + 1),
        wave_index,
        title: format!("Wave {} closure milestone", wave_index + 1),
        blocking_tasks: blocking
            .into_iter()
            .map(|(task_id, _, _)| task_id)
            .collect(),
        owner_allocations,
    }
}

fn normalize_owner(owner: &str) -> String {
    let trimmed = owner.trim();
    if trimmed.is_empty() {
        "UNASSIGNED".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn classify_lane(task: &WaveTaskInput) -> ExecutionLane {
    let title = task.title.to_ascii_lowercase();
    let labels = task
        .labels
        .iter()
        .map(|label| label.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if labels.iter().any(|label| label.contains("extension"))
        || contains_any(&title, &["fts", "json", "rtree", "session", "icu"])
    {
        return ExecutionLane::Extensions;
    }
    if labels.iter().any(|label| {
        label.contains("storage")
            || label.contains("mvcc")
            || label.contains("durability")
            || label.contains("wal")
    }) || contains_any(
        &title,
        &["mvcc", "wal", "pager", "btree", "durability", "recovery"],
    ) {
        return ExecutionLane::StorageMvcc;
    }
    if labels
        .iter()
        .any(|label| label.contains("sql") || label.contains("parser"))
        || contains_any(&title, &["sql", "parser", "planner", "vdbe", "query"])
    {
        return ExecutionLane::SqlPipeline;
    }
    if labels.iter().any(|label| {
        label.contains("ev-gated")
            || label.contains("ci")
            || label.contains("logging")
            || label.contains("coverage")
    }) || contains_any(
        &title,
        &[
            "gate",
            "coverage",
            "manifest",
            "logging",
            "traceability",
            "evidence",
            "ci ",
        ],
    ) {
        return ExecutionLane::QualityGates;
    }
    ExecutionLane::ProgramOrchestration
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn derive_risk_score(priority: u8, labels: &[String]) -> u8 {
    let mut risk = match priority {
        0 => 92_i16,
        1 => 80_i16,
        2 => 62_i16,
        3 => 44_i16,
        _ => 30_i16,
    };

    for label in labels {
        let lower = label.to_ascii_lowercase();
        if lower.contains("ev-gated") {
            risk += 8;
        }
        if lower.contains("concurrency")
            || lower.contains("durability")
            || lower.contains("mvcc")
            || lower.contains("risk")
        {
            risk += 6;
        }
        if lower.contains("docs") {
            risk -= 10;
        }
    }

    risk = risk.clamp(0, 100);
    u8::try_from(risk).unwrap_or(100)
}

/// Write a plan artifact to disk and return its SHA-256 digest.
///
/// # Errors
///
/// Returns an error if serialization or filesystem writes fail.
pub fn write_execution_wave_plan_artifact(
    plan: &ExecutionWavePlan,
    output_path: &Path,
) -> Result<String, String> {
    let json = plan
        .to_json()
        .map_err(|error| format!("bead_id={BEAD_ID} case=serialize_failed error={error}"))?;
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=create_parent_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    fs::write(output_path, &json).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            output_path.display()
        )
    })?;

    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Build a deterministic runtime output directory for execution-wave artifacts.
///
/// # Errors
///
/// Returns an error if directory creation fails.
pub fn runtime_output_dir(label: &str) -> Result<std::path::PathBuf, String> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root_failed error={error}"))?;
    let root = workspace_root.join("target").join("bd_1dp9_9_1_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=runtime_root_create_failed path={} error={error}",
            root.display()
        )
    })?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), nanos));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=runtime_subdir_create_failed path={} error={error}",
            path.display()
        )
    })?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tasks() -> Vec<WaveTaskInput> {
        vec![
            WaveTaskInput {
                id: "bd-root".to_owned(),
                title: "Program integration kickoff".to_owned(),
                priority: 0,
                risk_score: 95,
                owner: "orchestrator".to_owned(),
                labels: vec!["parity100".to_owned(), "orchestration".to_owned()],
                blocked_by: Vec::new(),
            },
            WaveTaskInput {
                id: "bd-storage".to_owned(),
                title: "MVCC durability closure".to_owned(),
                priority: 1,
                risk_score: 88,
                owner: "alice".to_owned(),
                labels: vec!["storage".to_owned(), "mvcc".to_owned()],
                blocked_by: vec!["bd-root".to_owned()],
            },
            WaveTaskInput {
                id: "bd-sql".to_owned(),
                title: "Planner + VDBE parity".to_owned(),
                priority: 1,
                risk_score: 83,
                owner: "bob".to_owned(),
                labels: vec!["sql".to_owned()],
                blocked_by: vec!["bd-root".to_owned()],
            },
            WaveTaskInput {
                id: "bd-gate".to_owned(),
                title: "Validation manifest gate hardening".to_owned(),
                priority: 1,
                risk_score: 90,
                owner: String::new(),
                labels: vec!["ev-gated".to_owned(), "ci".to_owned()],
                blocked_by: vec!["bd-storage".to_owned(), "bd-sql".to_owned()],
            },
        ]
    }

    #[test]
    fn plan_builds_deterministic_waves_and_milestones() -> Result<(), String> {
        let plan_a = build_execution_wave_plan(&sample_tasks())?;
        let plan_b = build_execution_wave_plan(&sample_tasks())?;
        assert_eq!(
            plan_a, plan_b,
            "bead_id={BEAD_ID} case=non_deterministic_plan"
        );
        assert_eq!(plan_a.tasks_total, 4);
        assert_eq!(plan_a.waves.len(), 3);
        assert_eq!(plan_a.critical_path_length, 3);

        let wave0_ids: Vec<&str> = plan_a.waves[0]
            .tasks
            .iter()
            .map(|assignment| assignment.task_id.as_str())
            .collect();
        assert_eq!(wave0_ids, vec!["bd-root"]);

        let wave1_ids: Vec<&str> = plan_a.waves[1]
            .tasks
            .iter()
            .map(|assignment| assignment.task_id.as_str())
            .collect();
        assert_eq!(wave1_ids, vec!["bd-storage", "bd-sql"]);

        let wave2_ids: Vec<&str> = plan_a.waves[2]
            .tasks
            .iter()
            .map(|assignment| assignment.task_id.as_str())
            .collect();
        assert_eq!(wave2_ids, vec!["bd-gate"]);

        assert_eq!(
            plan_a.unassigned_task_ids,
            vec!["bd-gate".to_owned()],
            "bead_id={BEAD_ID} case=unassigned_detection"
        );
        assert_eq!(plan_a.milestones.len(), 3);
        Ok(())
    }

    #[test]
    fn plan_rejects_cycles() {
        let cyclic_tasks = vec![
            WaveTaskInput {
                id: "a".to_owned(),
                title: "A".to_owned(),
                priority: 1,
                risk_score: 50,
                owner: "alice".to_owned(),
                labels: Vec::new(),
                blocked_by: vec!["b".to_owned()],
            },
            WaveTaskInput {
                id: "b".to_owned(),
                title: "B".to_owned(),
                priority: 1,
                risk_score: 50,
                owner: "bob".to_owned(),
                labels: Vec::new(),
                blocked_by: vec!["a".to_owned()],
            },
        ];
        let error = build_execution_wave_plan(&cyclic_tasks)
            .expect_err("bead_id=bd-1dp9.9.1 case=expected_cycle_error");
        assert!(
            error.contains("dependency_cycle_detected"),
            "bead_id={BEAD_ID} case=cycle_error_shape error={error}"
        );
    }

    #[test]
    fn jsonl_loader_includes_open_and_in_progress() -> Result<(), String> {
        let jsonl = r#"
{"id":"bd-a","title":"A","status":"open","priority":1,"labels":["storage"],"dependencies":[]}
{"id":"bd-b","title":"B","status":"in_progress","priority":2,"labels":["docs"],"dependencies":[{"depends_on_id":"bd-a","type":"blocks"}]}
{"id":"bd-c","title":"C","status":"closed","priority":0,"labels":[],"dependencies":[]}
"#;
        let loaded = load_wave_inputs_from_issues_jsonl(jsonl)?;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "bd-a");
        assert_eq!(loaded[1].id, "bd-b");
        assert_eq!(loaded[1].blocked_by, vec!["bd-a".to_owned()]);
        Ok(())
    }

    #[test]
    fn test_execution_wave_report_emits_structured_artifact() -> Result<(), String> {
        let run_id = format!("bd-1dp9.9.1-wave-seed-{}", 1_091_901_u64);
        let plan = build_execution_wave_plan(&sample_tasks())?;
        let runtime = runtime_output_dir("execution_wave_report")?;
        let artifact_path = runtime.join("bd_1dp9_9_1_execution_waves.json");
        let digest = write_execution_wave_plan_artifact(&plan, &artifact_path)?;

        eprintln!(
            "DEBUG bead_id={BEAD_ID} phase=artifact_written run_id={run_id} path={}",
            artifact_path.display()
        );
        eprintln!(
            "INFO bead_id={BEAD_ID} phase=plan_summary run_id={run_id} tasks_total={} waves={} critical_path_length={} artifact_sha256={digest}",
            plan.tasks_total,
            plan.waves.len(),
            plan.critical_path_length
        );
        eprintln!(
            "WARN bead_id={BEAD_ID} phase=ownership run_id={run_id} unassigned_task_ids={:?}",
            plan.unassigned_task_ids
        );
        eprintln!(
            "ERROR bead_id={BEAD_ID} phase=blocking run_id={run_id} top_milestone_blockers={:?}",
            plan.milestones
                .first()
                .map(|milestone| milestone.blocking_tasks.clone())
                .unwrap_or_default()
        );

        assert!(
            artifact_path.is_file(),
            "bead_id={BEAD_ID} case=artifact_missing path={}",
            artifact_path.display()
        );
        let payload = fs::read_to_string(&artifact_path).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=artifact_read_failed path={} error={error}",
                artifact_path.display()
            )
        })?;
        let parsed = ExecutionWavePlan::from_json(&payload).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=artifact_parse_failed path={} error={error}",
                artifact_path.display()
            )
        })?;
        assert_eq!(parsed.schema_version, EXECUTION_WAVE_SCHEMA_VERSION);
        assert_eq!(parsed.tasks_total, 4);
        Ok(())
    }
}
