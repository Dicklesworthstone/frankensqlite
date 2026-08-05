//! Canonical test-realism and Turso adaptation inventory.
//!
//! The inventory deliberately separates three concerns:
//!
//! - repository test discovery from the tracked `HEAD` object set;
//! - a reviewed, metadata-only Turso portfolio and provenance contract; and
//! - rendering one report model as JSON, Markdown, and CSV.
//!
//! No Turso source or fixture content is loaded into the report. The optional
//! upstream input is only the pinned GitHub Git-tree metadata response.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Canonical contract path relative to the workspace root.
pub const DEFAULT_TURSO_INVENTORY_CONTRACT_PATH: &str =
    "docs/contracts/turso_test_adaptation_inventory.toml";
/// Report schema emitted by this module.
pub const TEST_INVENTORY_REPORT_SCHEMA_VERSION: &str = "1.0.0";
/// Bead that owns this inventory.
pub const TURSO_INVENTORY_BEAD_ID: &str = "bd-turso-test-adaptation-zu081.1";
/// Stable scenario identifier used by the executable audit.
pub const TURSO_INVENTORY_SCENARIO_ID: &str = "TURSO-TEST-INVENTORY-V1";
/// Explicit Git revision metadata for RCH workers that do not receive `.git`.
pub const TURSO_INVENTORY_GIT_REVISION_ENV: &str = "FSQLITE_TURSO_INVENTORY_GIT_REVISION";
/// Newline- or semicolon-separated tracked `HEAD` paths for RCH workers without `.git`.
pub const TURSO_INVENTORY_TRACKED_PATHS_ENV: &str = "FSQLITE_TURSO_INVENTORY_TRACKED_PATHS";
/// Newline- or semicolon-separated dirty paths for RCH workers without `.git`.
pub const TURSO_INVENTORY_DIRTY_PATHS_ENV: &str = "FSQLITE_TURSO_INVENTORY_DIRTY_PATHS";
/// Newline- or semicolon-separated Beads issue IDs for RCH workers without `.beads/`.
pub const TURSO_INVENTORY_BEAD_IDS_ENV: &str = "FSQLITE_TURSO_INVENTORY_BEAD_IDS";

/// Parsed machine-readable intake and overlap contract.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TestInventoryContract {
    pub meta: ContractMeta,
    pub source: UpstreamSource,
    #[serde(default)]
    pub baseline: Vec<BaselineReference>,
    #[serde(default)]
    pub baseline_delta_explanation: Vec<BaselineDeltaExplanation>,
    #[serde(default)]
    pub portfolio: Vec<PortfolioEntry>,
    #[serde(default)]
    pub contract_authority: Vec<ContractAuthority>,
}

/// Contract identity and clean-room policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractMeta {
    pub schema_version: String,
    pub bead_id: String,
    pub contract_owner: String,
    pub reviewed_at: String,
    pub reviewer: String,
    pub source_policy: String,
    pub update_policy: String,
}

/// Pinned upstream repository identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamSource {
    pub repository: String,
    pub commit: String,
    pub committed_at: String,
    pub testing_path: String,
    pub testing_tree_sha: String,
    pub testing_entry_count: usize,
    pub top_level_entry_count: usize,
    pub license_path: String,
    pub license_blob_sha: String,
    pub license_spdx: String,
    pub license_class: String,
}

/// Historical comparison point and its reproduction command.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineReference {
    pub metric: String,
    pub reference_value: usize,
    pub reference_command: String,
    pub delta_policy: String,
}

/// Reviewed explanation for a baseline value that intentionally changed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineDeltaExplanation {
    pub metric: String,
    pub observed_value: usize,
    pub rationale: String,
}

/// Adopt/defer/reject outcome for an upstream testing area.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdoptionDecision {
    Adopt,
    Defer,
    Reject,
}

/// How upstream value may enter this clean-room project.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranslationClass {
    ConceptualAdaptation,
    ExternalTool,
    GapOnly,
    Defer,
    Reject,
    ProvenanceOnly,
}

/// Kind of a top-level entry in Turso's `testing/` tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Directory,
    File,
}

/// Required execution-path evidence for a candidate test family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLane {
    SqlResultOnly,
    PagerBackedRequired,
    PlannerRequired,
    VdbeRequired,
    MvccRequired,
    RecoveryRequired,
}

impl ExecutionLane {
    /// Stable serialized/test-facing identifier for this execution requirement.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::SqlResultOnly => "sql_result_only",
            Self::PagerBackedRequired => "pager_backed_required",
            Self::PlannerRequired => "planner_required",
            Self::VdbeRequired => "vdbe_required",
            Self::MvccRequired => "mvcc_required",
            Self::RecoveryRequired => "recovery_required",
        }
    }
}

/// Complete ownership and decision record for one Turso top-level entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioEntry {
    pub source_path: String,
    pub source_kind: SourceKind,
    pub entry_count: usize,
    pub blob_count: usize,
    pub tree_count: usize,
    pub decision: AdoptionDecision,
    pub translation_class: TranslationClass,
    pub owner_beads: Vec<String>,
    pub owner_paths: Vec<String>,
    pub surface_ids: Vec<String>,
    pub execution_lanes: Vec<ExecutionLane>,
    pub duplicate_owners: Vec<String>,
    pub adaptations: String,
    pub dedup_rationale: String,
    pub reviewer: String,
    pub update_policy: String,
}

/// Canonical path authority and handoff for one divergent root duplicate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractAuthority {
    pub logical_name: String,
    pub canonical_path: String,
    pub canonical_sha256: String,
    pub root_duplicate_path: String,
    pub root_duplicate_sha256: String,
    pub authority_owner: String,
    pub handoff_bead: String,
    #[serde(default)]
    pub live_root_consumers: Vec<String>,
    pub disposition: String,
    #[serde(default)]
    pub root_reference: Vec<RootReference>,
}

/// A root-shaped path reference classified for the canonicalization handoff.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RootReference {
    pub path: String,
    pub anchor: String,
    pub classification: String,
    pub resolves_repository_root: bool,
    pub rationale: String,
}

/// Path-level validation failure with a stable machine code.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct InventoryDiagnostic {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl InventoryDiagnostic {
    fn new(code: &str, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// A tracked-`HEAD` repository view that is unaffected by peer worktree dirt.
#[derive(Debug, Clone)]
pub struct GitSnapshot {
    workspace_root: PathBuf,
    revision: String,
    source_mode: &'static str,
    can_read_git_objects: bool,
    env_bead_ids: Option<BTreeSet<String>>,
    tracked_paths: Vec<String>,
    tracked_path_set: BTreeSet<String>,
    dirty_paths: BTreeSet<String>,
}

impl GitSnapshot {
    /// Capture the current `HEAD` object set and worktree dirt metadata.
    pub fn capture(workspace_root: &Path) -> Result<Self, String> {
        match Self::capture_from_git(workspace_root) {
            Ok(snapshot) => Ok(snapshot),
            Err(git_error) => match Self::capture_from_env(workspace_root)? {
                Some(snapshot) => Ok(snapshot),
                None => Err(git_error),
            },
        }
    }

    fn capture_from_git(workspace_root: &Path) -> Result<Self, String> {
        let revision = git_text(workspace_root, &["rev-parse", "HEAD"])?;
        let tracked = git_bytes(
            workspace_root,
            &["ls-tree", "-r", "--name-only", "-z", "HEAD"],
        )?;
        let tracked_paths = tracked
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
            .map(|entry| String::from_utf8(entry.to_vec()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("git_ls_tree_non_utf8: {error}"))?;
        let tracked_path_set = tracked_paths.iter().cloned().collect();
        let dirty_paths = parse_dirty_paths(&git_bytes(
            workspace_root,
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )?)?;

        Ok(Self {
            workspace_root: workspace_root.to_path_buf(),
            revision: revision.trim().to_owned(),
            source_mode: "tracked_git_head",
            can_read_git_objects: true,
            env_bead_ids: None,
            tracked_paths,
            tracked_path_set,
            dirty_paths,
        })
    }

    fn capture_from_env(workspace_root: &Path) -> Result<Option<Self>, String> {
        let revision = env_snapshot_value(TURSO_INVENTORY_GIT_REVISION_ENV)?;
        let tracked_paths = env_snapshot_value(TURSO_INVENTORY_TRACKED_PATHS_ENV)?;
        let dirty_paths = env_snapshot_value(TURSO_INVENTORY_DIRTY_PATHS_ENV)?;
        let bead_ids = env_snapshot_value(TURSO_INVENTORY_BEAD_IDS_ENV)?;

        let present = [
            revision.is_some(),
            tracked_paths.is_some(),
            dirty_paths.is_some(),
            bead_ids.is_some(),
        ];
        if present.iter().all(|value| !*value) {
            return Ok(None);
        }
        if !present.iter().all(|value| *value) {
            return Err(format!(
                "git_env_snapshot_incomplete required={TURSO_INVENTORY_GIT_REVISION_ENV},{TURSO_INVENTORY_TRACKED_PATHS_ENV},{TURSO_INVENTORY_DIRTY_PATHS_ENV},{TURSO_INVENTORY_BEAD_IDS_ENV}"
            ));
        }

        let revision = revision.expect("presence checked");
        if !is_full_git_hash(&revision) {
            return Err(format!(
                "git_env_revision_invalid var={TURSO_INVENTORY_GIT_REVISION_ENV}"
            ));
        }
        let tracked_paths = parse_env_path_list(
            TURSO_INVENTORY_TRACKED_PATHS_ENV,
            &tracked_paths.expect("presence checked"),
        )?;
        let dirty_paths = parse_env_path_list(
            TURSO_INVENTORY_DIRTY_PATHS_ENV,
            &dirty_paths.expect("presence checked"),
        )?
        .into_iter()
        .collect();
        let env_bead_ids = parse_env_bead_id_list(
            TURSO_INVENTORY_BEAD_IDS_ENV,
            &bead_ids.expect("presence checked"),
        )?
        .into_iter()
        .collect();
        let tracked_path_set = tracked_paths.iter().cloned().collect();

        Ok(Some(Self {
            workspace_root: workspace_root.to_path_buf(),
            revision,
            source_mode: "env_tracked_paths_workspace_content",
            can_read_git_objects: false,
            env_bead_ids: Some(env_bead_ids),
            tracked_paths,
            tracked_path_set,
            dirty_paths,
        }))
    }

    /// Commit used for the snapshot.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// How the snapshot metadata was captured.
    #[must_use]
    pub const fn source_mode(&self) -> &'static str {
        self.source_mode
    }

    /// Sorted tracked paths in the snapshot.
    #[must_use]
    pub fn tracked_paths(&self) -> &[String] {
        &self.tracked_paths
    }

    /// Worktree paths that differ from the captured commit.
    #[must_use]
    pub fn dirty_paths(&self) -> &BTreeSet<String> {
        &self.dirty_paths
    }

    fn env_bead_ids(&self) -> Option<&BTreeSet<String>> {
        self.env_bead_ids.as_ref()
    }

    fn contains_path_or_tree(&self, path: &str) -> bool {
        self.tracked_path_set.contains(path)
            || self
                .tracked_paths
                .iter()
                .any(|candidate| candidate.starts_with(&format!("{path}/")))
    }

    fn read_bytes(&self, path: &str) -> Result<Vec<u8>, String> {
        if !self.tracked_path_set.contains(path) {
            return Err(format!("snapshot_path_missing path={path}"));
        }
        if !self.dirty_paths.contains(path) || !self.can_read_git_objects {
            return fs::read(self.workspace_root.join(path))
                .map_err(|error| format!("snapshot_path_read_failed path={path} error={error}"));
        }
        git_bytes(
            &self.workspace_root,
            &["show", &format!("{}:{path}", self.revision)],
        )
    }

    fn read_text(&self, path: &str) -> Result<String, String> {
        String::from_utf8(self.read_bytes(path)?)
            .map_err(|error| format!("snapshot_path_non_utf8 path={path} error={error}"))
    }
}

fn git_bytes(workspace_root: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    let output = Command::new("git")
        // RCH and containerized CI may materialize a clean checkout under a
        // different uid. Trust only the exact workspace supplied to this run;
        // do not mutate the user's global safe.directory configuration.
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "safe.directory")
        .env("GIT_CONFIG_VALUE_0", workspace_root)
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .map_err(|error| format!("git_spawn_failed args={args:?} error={error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git_command_failed args={args:?} status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

fn git_text(workspace_root: &Path, args: &[&str]) -> Result<String, String> {
    String::from_utf8(git_bytes(workspace_root, args)?)
        .map_err(|error| format!("git_output_non_utf8 args={args:?} error={error}"))
}

fn env_snapshot_value(name: &str) -> Result<Option<String>, String> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(format!("git_env_non_utf8 var={name}")),
    }
}

fn parse_env_path_list(name: &str, raw: &str) -> Result<Vec<String>, String> {
    let mut paths = Vec::new();
    for (index, path) in raw
        .split(['\n', ';'])
        .filter(|path| !path.is_empty())
        .enumerate()
    {
        if path.contains('\r') {
            return Err(format!("git_env_path_contains_cr var={name} index={index}"));
        }
        let repository_path = Path::new(path);
        if path.contains('\\')
            || repository_path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(format!(
                "git_env_path_not_repository_relative var={name} index={index} path={path}"
            ));
        }
        paths.push(path.to_owned());
    }
    Ok(paths)
}

fn parse_env_bead_id_list(name: &str, raw: &str) -> Result<Vec<String>, String> {
    let mut ids = Vec::new();
    for (index, id) in raw
        .split(['\n', ';'])
        .filter(|id| !id.is_empty())
        .enumerate()
    {
        if id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_'))
        {
            ids.push(id.to_owned());
        } else {
            return Err(format!(
                "git_env_bead_id_invalid var={name} index={index} id={id}"
            ));
        }
    }
    Ok(ids)
}

fn parse_dirty_paths(status: &[u8]) -> Result<BTreeSet<String>, String> {
    let mut fields = status
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    let mut paths = BTreeSet::new();
    let mut index = 0_usize;
    while let Some(entry) = fields.next() {
        let (Some(status_code), Some(path_bytes)) = (entry.get(..2), entry.get(3..)) else {
            return Err(format!(
                "git_status_record_malformed index={index} record={}",
                String::from_utf8_lossy(entry)
            ));
        };
        if entry.get(2) != Some(&b' ') || path_bytes.is_empty() {
            return Err(format!(
                "git_status_record_malformed index={index} record={}",
                String::from_utf8_lossy(entry)
            ));
        }
        let path = std::str::from_utf8(path_bytes)
            .map_err(|error| format!("git_status_path_non_utf8 index={index} error={error}"))?;
        paths.insert(path.to_owned());

        if status_code
            .iter()
            .any(|status| matches!(*status, b'R' | b'C'))
        {
            index += 1;
            let source = fields.next().ok_or_else(|| {
                format!("git_status_rename_source_missing index={index} destination={path}")
            })?;
            let source = std::str::from_utf8(source).map_err(|error| {
                format!("git_status_rename_source_non_utf8 index={index} error={error}")
            })?;
            paths.insert(source.to_owned());
        }
        index += 1;
    }
    Ok(paths)
}

/// Load the canonical TOML contract.
pub fn load_test_inventory_contract(path: &Path) -> Result<TestInventoryContract, String> {
    let raw = fs::read_to_string(path).map_err(|error| {
        format!(
            "inventory_contract_read_failed path={} error={error}",
            path.display()
        )
    })?;
    toml::from_str(&raw).map_err(|error| {
        format!(
            "inventory_contract_parse_failed path={} error={error}",
            path.display()
        )
    })
}

/// Validate schema, ownership, feature references, hashes, and handoff data.
#[must_use]
pub fn validate_test_inventory_contract(
    contract: &TestInventoryContract,
    snapshot: &GitSnapshot,
) -> Vec<InventoryDiagnostic> {
    let mut diagnostics = Vec::new();
    validate_contract_meta(contract, &mut diagnostics);

    let bead_ids = load_bead_ids(snapshot, &mut diagnostics);
    let surface_ids = load_surface_ids(snapshot, &mut diagnostics);
    validate_baselines(contract, &mut diagnostics);
    validate_portfolio(
        contract,
        snapshot,
        &bead_ids,
        &surface_ids,
        &mut diagnostics,
    );
    validate_contract_authority(contract, snapshot, &bead_ids, &mut diagnostics);
    diagnostics
}

fn validate_contract_meta(
    contract: &TestInventoryContract,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) {
    require_equal(
        diagnostics,
        "schema_version_mismatch",
        "meta.schema_version",
        &contract.meta.schema_version,
        TEST_INVENTORY_REPORT_SCHEMA_VERSION,
    );
    require_equal(
        diagnostics,
        "bead_id_mismatch",
        "meta.bead_id",
        &contract.meta.bead_id,
        TURSO_INVENTORY_BEAD_ID,
    );
    for (path, value) in [
        ("meta.contract_owner", contract.meta.contract_owner.as_str()),
        ("meta.reviewed_at", contract.meta.reviewed_at.as_str()),
        ("meta.reviewer", contract.meta.reviewer.as_str()),
        ("meta.source_policy", contract.meta.source_policy.as_str()),
        ("meta.update_policy", contract.meta.update_policy.as_str()),
        ("source.repository", contract.source.repository.as_str()),
        ("source.committed_at", contract.source.committed_at.as_str()),
        ("source.testing_path", contract.source.testing_path.as_str()),
        ("source.license_path", contract.source.license_path.as_str()),
        ("source.license_spdx", contract.source.license_spdx.as_str()),
        (
            "source.license_class",
            contract.source.license_class.as_str(),
        ),
    ] {
        require_non_empty(diagnostics, path, value);
    }
    require_sha1(
        diagnostics,
        "source_commit_invalid",
        "source.commit",
        &contract.source.commit,
    );
    require_sha1(
        diagnostics,
        "testing_tree_sha_invalid",
        "source.testing_tree_sha",
        &contract.source.testing_tree_sha,
    );
    require_sha1(
        diagnostics,
        "license_blob_sha_invalid",
        "source.license_blob_sha",
        &contract.source.license_blob_sha,
    );
    if contract.source.license_spdx != "MIT" || contract.source.license_class != "permissive" {
        diagnostics.push(InventoryDiagnostic::new(
            "license_classification_invalid",
            "source.license_spdx",
            "the pinned Turso source must retain its verified MIT/permissive classification",
        ));
    }
    if contract.source.testing_entry_count == 0 || contract.source.top_level_entry_count == 0 {
        diagnostics.push(InventoryDiagnostic::new(
            "source_counts_missing",
            "source",
            "testing entry counts must be positive",
        ));
    }
}

fn load_bead_ids(
    snapshot: &GitSnapshot,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) -> BTreeSet<String> {
    if let Some(bead_ids) = snapshot.env_bead_ids() {
        return bead_ids.clone();
    }

    let Ok(raw) = snapshot.read_text(".beads/issues.jsonl") else {
        diagnostics.push(InventoryDiagnostic::new(
            "beads_inventory_missing",
            ".beads/issues.jsonl",
            "cannot validate portfolio owner beads",
        ));
        return BTreeSet::new();
    };

    raw.lines()
        .filter_map(|line| {
            let value = serde_json::from_str::<serde_json::Value>(line).ok()?;
            value.get("id")?.as_str().map(ToOwned::to_owned)
        })
        .collect()
}

fn load_surface_ids(
    snapshot: &GitSnapshot,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) -> BTreeSet<String> {
    #[derive(Deserialize)]
    struct SurfaceDocument {
        #[serde(default)]
        surface: Vec<SurfaceId>,
    }
    #[derive(Deserialize)]
    struct SurfaceId {
        feature_id: String,
    }

    let path = "docs/contracts/supported_surface_matrix.toml";
    let Ok(raw) = snapshot.read_text(path) else {
        diagnostics.push(InventoryDiagnostic::new(
            "surface_contract_missing",
            path,
            "cannot validate portfolio surface IDs",
        ));
        return BTreeSet::new();
    };
    match toml::from_str::<SurfaceDocument>(&raw) {
        Ok(document) => document
            .surface
            .into_iter()
            .map(|surface| surface.feature_id)
            .collect(),
        Err(error) => {
            diagnostics.push(InventoryDiagnostic::new(
                "surface_contract_parse_failed",
                path,
                error.to_string(),
            ));
            BTreeSet::new()
        }
    }
}

fn validate_baselines(
    contract: &TestInventoryContract,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) {
    const REQUIRED: [&str; 8] = [
        "e2e_rusqlite_files",
        "e2e_top_level_integration_files",
        "fuzz_corpus_files",
        "fuzz_targets",
        "harness_literal_beads_path_files",
        "harness_top_level_integration_files",
        "harness_tracker_shaped_files",
        "slt_files",
    ];
    let mut metrics = BTreeSet::new();
    for baseline in &contract.baseline {
        if !metrics.insert(baseline.metric.as_str()) {
            diagnostics.push(InventoryDiagnostic::new(
                "duplicate_baseline_metric",
                format!("baseline.{}", baseline.metric),
                "baseline metric appears more than once",
            ));
        }
        require_non_empty(
            diagnostics,
            &format!("baseline.{}.reference_command", baseline.metric),
            &baseline.reference_command,
        );
        require_non_empty(
            diagnostics,
            &format!("baseline.{}.delta_policy", baseline.metric),
            &baseline.delta_policy,
        );
    }
    if metrics != REQUIRED.into_iter().collect() {
        diagnostics.push(InventoryDiagnostic::new(
            "baseline_metric_set_incomplete",
            "baseline",
            format!("expected={REQUIRED:?} observed={metrics:?}"),
        ));
    }

    let mut explanations = BTreeSet::new();
    for explanation in &contract.baseline_delta_explanation {
        if !metrics.contains(explanation.metric.as_str()) {
            diagnostics.push(InventoryDiagnostic::new(
                "unknown_baseline_explanation",
                format!("baseline_delta_explanation.{}", explanation.metric),
                "delta explanation does not name a declared baseline metric",
            ));
        }
        if !explanations.insert(explanation.metric.as_str()) {
            diagnostics.push(InventoryDiagnostic::new(
                "duplicate_baseline_explanation",
                format!("baseline_delta_explanation.{}", explanation.metric),
                "baseline metric has multiple delta explanations",
            ));
        }
        require_non_empty(
            diagnostics,
            &format!(
                "baseline_delta_explanation.{}.rationale",
                explanation.metric
            ),
            &explanation.rationale,
        );
    }
}

fn validate_portfolio(
    contract: &TestInventoryContract,
    snapshot: &GitSnapshot,
    bead_ids: &BTreeSet<String>,
    surface_ids: &BTreeSet<String>,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) {
    if contract.portfolio.len() != contract.source.top_level_entry_count {
        diagnostics.push(InventoryDiagnostic::new(
            "portfolio_top_level_count_mismatch",
            "portfolio",
            format!(
                "expected={} observed={}",
                contract.source.top_level_entry_count,
                contract.portfolio.len()
            ),
        ));
    }
    let observed_entry_count = contract
        .portfolio
        .iter()
        .map(|entry| entry.entry_count)
        .sum::<usize>();
    if observed_entry_count != contract.source.testing_entry_count {
        diagnostics.push(InventoryDiagnostic::new(
            "portfolio_recursive_count_mismatch",
            "portfolio",
            format!(
                "expected={} observed={observed_entry_count}",
                contract.source.testing_entry_count
            ),
        ));
    }

    let mut source_paths = BTreeSet::new();
    for entry in &contract.portfolio {
        let prefix = format!("portfolio.{}", entry.source_path);
        if !source_paths.insert(entry.source_path.as_str()) {
            diagnostics.push(InventoryDiagnostic::new(
                "duplicate_portfolio_path",
                &entry.source_path,
                "source path appears more than once",
            ));
        }
        if !entry.source_path.starts_with("testing/") {
            diagnostics.push(InventoryDiagnostic::new(
                "portfolio_path_outside_testing",
                &entry.source_path,
                "source path must be rooted under testing/",
            ));
        }
        if entry.entry_count == 0 || entry.blob_count + entry.tree_count != entry.entry_count {
            diagnostics.push(InventoryDiagnostic::new(
                "portfolio_entry_count_invalid",
                &entry.source_path,
                format!(
                    "entry_count={} blob_count={} tree_count={}",
                    entry.entry_count, entry.blob_count, entry.tree_count
                ),
            ));
        }
        let expected_kind = if entry.tree_count == 0 {
            SourceKind::File
        } else {
            SourceKind::Directory
        };
        if entry.source_kind != expected_kind {
            diagnostics.push(InventoryDiagnostic::new(
                "portfolio_source_kind_mismatch",
                &entry.source_path,
                format!(
                    "expected={expected_kind:?} observed={:?}",
                    entry.source_kind
                ),
            ));
        }
        if entry.owner_beads.is_empty() || entry.owner_paths.is_empty() {
            diagnostics.push(InventoryDiagnostic::new(
                "portfolio_owner_missing",
                &entry.source_path,
                "every portfolio entry needs bead and existing-path owners",
            ));
        }
        for bead in &entry.owner_beads {
            if !bead_ids.contains(bead) {
                diagnostics.push(InventoryDiagnostic::new(
                    "stale_owner_bead",
                    &entry.source_path,
                    format!("unknown owner bead={bead}"),
                ));
            }
        }
        for path in &entry.owner_paths {
            if !snapshot.contains_path_or_tree(path) {
                diagnostics.push(InventoryDiagnostic::new(
                    "stale_owner_path",
                    &entry.source_path,
                    format!("missing owner path={path}"),
                ));
            }
        }
        for surface_id in &entry.surface_ids {
            if !surface_ids.contains(surface_id) {
                diagnostics.push(InventoryDiagnostic::new(
                    "unknown_surface_id",
                    &entry.source_path,
                    format!("unknown surface_id={surface_id}"),
                ));
            }
        }
        if entry.decision == AdoptionDecision::Adopt && entry.execution_lanes.is_empty() {
            diagnostics.push(InventoryDiagnostic::new(
                "adopted_entry_missing_lane",
                &entry.source_path,
                "adopted entries must declare at least one execution lane",
            ));
        }
        if entry.duplicate_owners.is_empty() {
            diagnostics.push(InventoryDiagnostic::new(
                "duplicate_owner_missing",
                &entry.source_path,
                "every decision must name existing duplicate ownership",
            ));
        }
        for (field, value) in [
            ("adaptations", entry.adaptations.as_str()),
            ("dedup_rationale", entry.dedup_rationale.as_str()),
            ("reviewer", entry.reviewer.as_str()),
            ("update_policy", entry.update_policy.as_str()),
        ] {
            require_non_empty(diagnostics, &format!("{prefix}.{field}"), value);
        }
    }
}

fn validate_contract_authority(
    contract: &TestInventoryContract,
    snapshot: &GitSnapshot,
    bead_ids: &BTreeSet<String>,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) {
    const REQUIRED: [&str; 5] = [
        "corpus_manifest",
        "feature_universe_ledger",
        "parity_taxonomy",
        "sqlite_version_contract",
        "supported_surface_matrix",
    ];
    let names = contract
        .contract_authority
        .iter()
        .map(|authority| authority.logical_name.as_str())
        .collect::<BTreeSet<_>>();
    if names != REQUIRED.into_iter().collect() {
        diagnostics.push(InventoryDiagnostic::new(
            "contract_authority_set_incomplete",
            "contract_authority",
            format!("expected={REQUIRED:?} observed={names:?}"),
        ));
    }

    for authority in &contract.contract_authority {
        let prefix = format!("contract_authority.{}", authority.logical_name);
        for (path, expected_hash, kind) in [
            (
                authority.canonical_path.as_str(),
                authority.canonical_sha256.as_str(),
                "canonical",
            ),
            (
                authority.root_duplicate_path.as_str(),
                authority.root_duplicate_sha256.as_str(),
                "root_duplicate",
            ),
        ] {
            require_sha256(
                diagnostics,
                "contract_hash_invalid",
                &format!("{prefix}.{kind}_sha256"),
                expected_hash,
            );
            match snapshot.read_bytes(path) {
                Ok(bytes) => {
                    let observed_hash = sha256_hex(&bytes);
                    if observed_hash != expected_hash {
                        diagnostics.push(InventoryDiagnostic::new(
                            "contract_hash_drift",
                            path,
                            format!("expected={expected_hash} observed={observed_hash}"),
                        ));
                    }
                }
                Err(error) => diagnostics.push(InventoryDiagnostic::new(
                    "contract_path_missing",
                    path,
                    error,
                )),
            }
        }
        if authority.canonical_sha256 == authority.root_duplicate_sha256 {
            diagnostics.push(InventoryDiagnostic::new(
                "contract_duplicate_not_divergent",
                &authority.logical_name,
                "handoff must record the currently observed divergent pair",
            ));
        }
        if authority.handoff_bead != "bd-turso-test-adaptation-zu081.18"
            || !bead_ids.contains(&authority.handoff_bead)
        {
            diagnostics.push(InventoryDiagnostic::new(
                "contract_handoff_incomplete",
                &authority.logical_name,
                format!("invalid handoff_bead={}", authority.handoff_bead),
            ));
        }
        require_non_empty(
            diagnostics,
            &format!("{prefix}.authority_owner"),
            &authority.authority_owner,
        );
        require_non_empty(
            diagnostics,
            &format!("{prefix}.disposition"),
            &authority.disposition,
        );
        for reference in &authority.root_reference {
            if reference.resolves_repository_root
                && !authority.live_root_consumers.contains(&reference.path)
            {
                diagnostics.push(InventoryDiagnostic::new(
                    "live_root_consumer_omitted",
                    &reference.path,
                    format!("logical_name={}", authority.logical_name),
                ));
            }
            match snapshot.read_text(&reference.path) {
                Ok(content) if !content.contains(&reference.anchor) => {
                    diagnostics.push(InventoryDiagnostic::new(
                        "root_reference_anchor_missing",
                        &reference.path,
                        format!("anchor={}", reference.anchor),
                    ));
                }
                Err(error) => diagnostics.push(InventoryDiagnostic::new(
                    "root_reference_path_missing",
                    &reference.path,
                    error,
                )),
                Ok(_) => {}
            }
            require_non_empty(
                diagnostics,
                &format!("{prefix}.root_reference.classification"),
                &reference.classification,
            );
            require_non_empty(
                diagnostics,
                &format!("{prefix}.root_reference.rationale"),
                &reference.rationale,
            );
        }
        for live_path in &authority.live_root_consumers {
            if !snapshot.contains_path_or_tree(live_path) {
                diagnostics.push(InventoryDiagnostic::new(
                    "live_root_consumer_missing",
                    live_path,
                    format!("logical_name={}", authority.logical_name),
                ));
            }
        }
    }
}

fn require_non_empty(diagnostics: &mut Vec<InventoryDiagnostic>, path: &str, value: &str) {
    if value.trim().is_empty() {
        diagnostics.push(InventoryDiagnostic::new(
            "required_field_missing",
            path,
            "value must be non-empty",
        ));
    }
}

fn require_equal(
    diagnostics: &mut Vec<InventoryDiagnostic>,
    code: &str,
    path: &str,
    observed: &str,
    expected: &str,
) {
    if observed != expected {
        diagnostics.push(InventoryDiagnostic::new(
            code,
            path,
            format!("expected={expected} observed={observed}"),
        ));
    }
}

fn require_sha1(diagnostics: &mut Vec<InventoryDiagnostic>, code: &str, path: &str, value: &str) {
    if !is_full_git_hash(value) {
        diagnostics.push(InventoryDiagnostic::new(
            code,
            path,
            "expected a full 40-character hexadecimal Git object ID",
        ));
    }
}

fn is_full_git_hash(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn require_sha256(diagnostics: &mut Vec<InventoryDiagnostic>, code: &str, path: &str, value: &str) {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        diagnostics.push(InventoryDiagnostic::new(
            code,
            path,
            "expected a 64-character hexadecimal SHA-256",
        ));
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    crate::bytes_to_lower_hex(Sha256::digest(bytes))
}

/// Minimal GitHub Git-tree API response used for pinned-source validation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UpstreamTree {
    pub sha: String,
    pub truncated: bool,
    #[serde(default)]
    pub tree: Vec<UpstreamTreeEntry>,
}

/// One metadata-only entry from a GitHub Git-tree response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UpstreamTreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub sha: String,
    #[serde(default)]
    pub size: Option<usize>,
}

/// Load upstream Git-tree metadata from a JSON file.
pub fn load_upstream_tree(path: &Path) -> Result<UpstreamTree, String> {
    let raw = fs::read(path).map_err(|error| {
        format!(
            "upstream_tree_read_failed path={} error={error}",
            path.display()
        )
    })?;
    serde_json::from_slice(&raw).map_err(|error| {
        format!(
            "upstream_tree_parse_failed path={} error={error}",
            path.display()
        )
    })
}

/// Validate the non-truncated pinned upstream tree against the reviewed map.
#[must_use]
pub fn validate_upstream_tree(
    contract: &TestInventoryContract,
    upstream: &UpstreamTree,
) -> Vec<InventoryDiagnostic> {
    let mut diagnostics = Vec::new();
    if upstream.sha != contract.source.commit {
        diagnostics.push(InventoryDiagnostic::new(
            "upstream_commit_mismatch",
            contract.source.repository.clone(),
            format!(
                "expected={} observed={}",
                contract.source.commit, upstream.sha
            ),
        ));
    }
    if upstream.truncated {
        diagnostics.push(InventoryDiagnostic::new(
            "upstream_tree_truncated",
            contract.source.repository.clone(),
            "GitHub tree response is truncated and cannot prove exhaustiveness",
        ));
    }

    let testing_tree_path = contract.source.testing_path.as_str();
    match upstream
        .tree
        .iter()
        .find(|entry| entry.path == testing_tree_path && entry.entry_type == "tree")
    {
        Some(entry) if entry.sha != contract.source.testing_tree_sha => {
            diagnostics.push(InventoryDiagnostic::new(
                "testing_tree_sha_mismatch",
                testing_tree_path,
                format!(
                    "expected={} observed={}",
                    contract.source.testing_tree_sha, entry.sha
                ),
            ));
        }
        None => diagnostics.push(InventoryDiagnostic::new(
            "testing_tree_missing",
            testing_tree_path,
            "pinned upstream response does not contain the testing tree",
        )),
        Some(_) => {}
    }

    match upstream
        .tree
        .iter()
        .find(|entry| entry.path == contract.source.license_path && entry.entry_type == "blob")
    {
        Some(entry) if entry.sha != contract.source.license_blob_sha => {
            diagnostics.push(InventoryDiagnostic::new(
                "license_blob_sha_mismatch",
                &contract.source.license_path,
                format!(
                    "expected={} observed={}",
                    contract.source.license_blob_sha, entry.sha
                ),
            ));
        }
        None => diagnostics.push(InventoryDiagnostic::new(
            "license_blob_missing",
            &contract.source.license_path,
            "pinned upstream response does not contain the reviewed license blob",
        )),
        Some(_) => {}
    }

    let prefix = format!("{}/", contract.source.testing_path);
    let testing_entries = upstream
        .tree
        .iter()
        .filter(|entry| entry.path.starts_with(&prefix))
        .collect::<Vec<_>>();
    if testing_entries.len() != contract.source.testing_entry_count {
        diagnostics.push(InventoryDiagnostic::new(
            "upstream_testing_count_mismatch",
            &contract.source.testing_path,
            format!(
                "expected={} observed={}",
                contract.source.testing_entry_count,
                testing_entries.len()
            ),
        ));
    }

    let mut observed = BTreeMap::<String, (usize, usize, usize)>::new();
    for entry in testing_entries {
        let suffix = &entry.path[prefix.len()..];
        let top = suffix.split('/').next().unwrap_or_default();
        if top.is_empty() {
            continue;
        }
        let key = format!("{}/{top}", contract.source.testing_path);
        let counts = observed.entry(key).or_default();
        counts.0 += 1;
        match entry.entry_type.as_str() {
            "blob" => counts.1 += 1,
            "tree" => counts.2 += 1,
            other => diagnostics.push(InventoryDiagnostic::new(
                "upstream_entry_type_unknown",
                &entry.path,
                format!("entry_type={other}"),
            )),
        }
    }

    let expected_paths = contract
        .portfolio
        .iter()
        .map(|entry| entry.source_path.as_str())
        .collect::<BTreeSet<_>>();
    for (path, (entry_count, blob_count, tree_count)) in &observed {
        let Some(expected) = contract
            .portfolio
            .iter()
            .find(|entry| entry.source_path == *path)
        else {
            diagnostics.push(InventoryDiagnostic::new(
                "unknown_upstream_subtree",
                path,
                "pinned tree contains an unclassified top-level testing entry",
            ));
            continue;
        };
        if (*entry_count, *blob_count, *tree_count)
            != (
                expected.entry_count,
                expected.blob_count,
                expected.tree_count,
            )
        {
            diagnostics.push(InventoryDiagnostic::new(
                "upstream_subtree_count_mismatch",
                path,
                format!(
                    "expected={}/{}/{} observed={entry_count}/{blob_count}/{tree_count}",
                    expected.entry_count, expected.blob_count, expected.tree_count
                ),
            ));
        }
    }
    for missing in
        expected_paths.difference(&observed.keys().map(String::as_str).collect::<BTreeSet<_>>())
    {
        diagnostics.push(InventoryDiagnostic::new(
            "upstream_portfolio_entry_missing",
            *missing,
            "reviewed portfolio entry is absent from the pinned tree",
        ));
    }

    diagnostics
}

/// Primary classification for a tracked test or corpus file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TestClass {
    Unit,
    Integration,
    Corpus,
    Fuzz,
    E2e,
    TrackerMetadata,
}

impl TestClass {
    const fn label(self) -> &'static str {
        match self {
            Self::Unit => "unit",
            Self::Integration => "integration",
            Self::Corpus => "corpus",
            Self::Fuzz => "fuzz",
            Self::E2e => "e2e",
            Self::TrackerMetadata => "tracker-metadata",
        }
    }
}

/// Per-file inventory row derived from the tracked `HEAD` snapshot.
///
/// The boolean fields are independent, stable JSON/CSV columns. Grouping them
/// behind another type would make the machine-readable report schemas diverge.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TestFileRecord {
    pub path: String,
    pub crate_name: String,
    pub class: TestClass,
    pub test_count: usize,
    pub uses_mock: bool,
    pub uses_memory: bool,
    pub uses_file_backend: bool,
    pub uses_proptest: bool,
    pub uses_rusqlite: bool,
    pub uses_tracker_metadata: bool,
    pub uses_literal_beads_path: bool,
    pub content_sha256: String,
}

/// Exact-content duplicate test sources discovered in the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DuplicateGroup {
    pub content_sha256: String,
    pub paths: Vec<String>,
}

/// Aggregated count for one primary test class.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ClassSummary {
    pub class: TestClass,
    pub file_count: usize,
    pub test_count: usize,
}

/// Current value compared with a historical reference.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BaselineObservation {
    pub metric: String,
    pub reference_value: usize,
    pub observed_value: usize,
    pub delta: i64,
    pub reference_command: String,
    pub explanation: Option<String>,
}

/// Compact test-realism totals, including overlapping storage flags.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TestInventorySummary {
    pub tracked_test_and_corpus_files: usize,
    pub direct_test_attributes: usize,
    pub classes: Vec<ClassSummary>,
    pub file_backed_files: usize,
    pub in_memory_files: usize,
    pub mocked_files: usize,
    pub property_files: usize,
    pub duplicate_groups: usize,
}

/// Provenance fields for one inventory run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct InventoryRunMetadata {
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub generated_unix_ms: u128,
    pub command: String,
}

impl InventoryRunMetadata {
    /// Construct metadata with current wall-clock time and stable scenario ID.
    pub fn now(run_id: String, trace_id: String, command: String) -> Result<Self, String> {
        let generated_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system_time_before_unix_epoch: {error}"))?
            .as_millis();
        Ok(Self {
            run_id,
            trace_id,
            scenario_id: TURSO_INVENTORY_SCENARIO_ID.to_owned(),
            generated_unix_ms,
            command,
        })
    }
}

/// Provenance for the local and upstream snapshots used by the report.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct InventoryProvenance {
    pub tool_schema_version: String,
    pub tool_crate_version: String,
    pub source_revision: String,
    pub source_mode: String,
    pub source_dirty: bool,
    pub dirty_paths: Vec<String>,
    pub upstream_repository: String,
    pub upstream_commit: String,
    pub upstream_tree_verified: bool,
    pub upstream_tree_input: Option<String>,
}

/// One complete report rendered into every supported output format.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TestInventoryReport {
    pub schema_version: String,
    pub bead_id: String,
    pub run: InventoryRunMetadata,
    pub provenance: InventoryProvenance,
    pub summary: TestInventorySummary,
    pub baseline: Vec<BaselineObservation>,
    pub files: Vec<TestFileRecord>,
    pub duplicate_groups: Vec<DuplicateGroup>,
    pub portfolio: Vec<PortfolioEntry>,
    pub contract_authority: Vec<ContractAuthority>,
    pub decision_totals: BTreeMap<AdoptionDecision, usize>,
    pub diagnostics: Vec<InventoryDiagnostic>,
}

/// Build a fail-closed report from the canonical contract and tracked `HEAD`.
pub fn build_test_inventory_report(
    workspace_root: &Path,
    contract_path: &Path,
    upstream_tree_path: Option<&Path>,
    run: InventoryRunMetadata,
) -> Result<TestInventoryReport, String> {
    let snapshot = GitSnapshot::capture(workspace_root)?;
    let contract = load_test_inventory_contract(contract_path)?;
    let mut diagnostics = validate_test_inventory_contract(&contract, &snapshot);

    let upstream_tree_input = upstream_tree_path.map(|path| path.display().to_string());
    let upstream_tree_verified = if let Some(path) = upstream_tree_path {
        let upstream = load_upstream_tree(path)?;
        let upstream_diagnostics = validate_upstream_tree(&contract, &upstream);
        let verified = upstream_diagnostics.is_empty();
        diagnostics.extend(upstream_diagnostics);
        verified
    } else {
        false
    };

    let files = scan_test_files(&snapshot)?;
    let duplicate_groups = find_duplicate_groups(&files);
    let summary = summarize_test_files(&files, duplicate_groups.len());
    let baseline = observe_baselines(&contract, &snapshot, &mut diagnostics)?;
    let decision_totals = contract.portfolio.iter().fold(
        BTreeMap::<AdoptionDecision, usize>::new(),
        |mut totals, entry| {
            *totals.entry(entry.decision).or_default() += 1;
            totals
        },
    );

    if !diagnostics.is_empty() {
        return Err(render_diagnostics(&diagnostics));
    }

    Ok(TestInventoryReport {
        schema_version: TEST_INVENTORY_REPORT_SCHEMA_VERSION.to_owned(),
        bead_id: TURSO_INVENTORY_BEAD_ID.to_owned(),
        run,
        provenance: InventoryProvenance {
            tool_schema_version: TEST_INVENTORY_REPORT_SCHEMA_VERSION.to_owned(),
            tool_crate_version: env!("CARGO_PKG_VERSION").to_owned(),
            source_revision: snapshot.revision().to_owned(),
            source_mode: snapshot.source_mode().to_owned(),
            source_dirty: !snapshot.dirty_paths().is_empty(),
            dirty_paths: snapshot.dirty_paths().iter().cloned().collect(),
            upstream_repository: contract.source.repository.clone(),
            upstream_commit: contract.source.commit.clone(),
            upstream_tree_verified,
            upstream_tree_input,
        },
        summary,
        baseline,
        files,
        duplicate_groups,
        portfolio: contract.portfolio,
        contract_authority: contract.contract_authority,
        decision_totals,
        diagnostics,
    })
}

fn scan_test_files(snapshot: &GitSnapshot) -> Result<Vec<TestFileRecord>, String> {
    let mut entries = Vec::new();
    for path in snapshot.tracked_paths() {
        let is_rust = path.ends_with(".rs");
        let is_corpus = path.starts_with("conformance/")
            || path.starts_with("crates/fsqlite-harness/conformance/");
        let is_fuzz = path.starts_with("fuzz/fuzz_targets/")
            || path.contains("/corpus/") && path.starts_with("fuzz/");
        if !is_rust && !is_corpus && !is_fuzz {
            continue;
        }
        entries.push((path.clone(), snapshot.read_bytes(path)?));
    }
    classify_test_entries(entries).map_err(|diagnostics| render_diagnostics(&diagnostics))
}

/// Classify controlled path/content pairs using the production inventory rules.
///
/// This is public so integration tests can prove discovery without creating a
/// nested Git repository. Any test-bearing Rust path outside the known unit,
/// integration, E2E, or fuzz layouts fails closed.
pub fn classify_test_entries(
    entries: Vec<(String, Vec<u8>)>,
) -> Result<Vec<TestFileRecord>, Vec<InventoryDiagnostic>> {
    let mut records = Vec::new();
    let mut diagnostics = Vec::new();
    for (path, bytes) in entries {
        let is_corpus = path.starts_with("conformance/")
            || path.starts_with("crates/fsqlite-harness/conformance/");
        let is_fuzz = path.starts_with("fuzz/fuzz_targets/")
            || path.contains("/corpus/") && path.starts_with("fuzz/");
        let content = String::from_utf8_lossy(&bytes);
        let test_count = content.matches("#[test]").count();
        let has_test_module = content.contains("#[cfg(test)]");
        let is_integration_path = path.starts_with("tests/") || path.contains("/tests/");
        if !is_corpus && !is_fuzz && !is_integration_path && test_count == 0 && !has_test_module {
            continue;
        }

        let uses_tracker_metadata =
            path.starts_with("crates/fsqlite-harness/tests/") && content.contains("issues.jsonl");
        let class = if uses_tracker_metadata {
            TestClass::TrackerMetadata
        } else if is_fuzz {
            TestClass::Fuzz
        } else if is_corpus {
            TestClass::Corpus
        } else if path.starts_with("crates/fsqlite-e2e/") || path.starts_with("e2e/") {
            TestClass::E2e
        } else if is_integration_path {
            TestClass::Integration
        } else if path.starts_with("src/") || path.contains("/src/") {
            TestClass::Unit
        } else {
            diagnostics.push(InventoryDiagnostic::new(
                "unknown_test_class",
                &path,
                "test-bearing Rust source is outside known unit, integration, E2E, and fuzz layouts",
            ));
            continue;
        };

        records.push(TestFileRecord {
            crate_name: crate_name(&path),
            path,
            class,
            test_count,
            uses_mock: contains_any(
                &content,
                &["Mock", "Fake", "Stub", "mock_", "fake_", "stub_"],
            ),
            uses_memory: contains_any(
                &content,
                &["MemDatabase", "MemoryVfs", ":memory:", "InMemory"],
            ),
            uses_file_backend: contains_any(
                &content,
                &["tempfile", "TempDir", "temp_dir", "NamedTempFile"],
            ),
            uses_proptest: content.contains("proptest") || content.contains("prop_"),
            uses_rusqlite: content.contains("rusqlite"),
            uses_tracker_metadata,
            uses_literal_beads_path: content.contains(".beads/issues.jsonl"),
            content_sha256: sha256_hex(&bytes),
        });
    }
    records.sort_by(|left, right| left.path.cmp(&right.path));
    if diagnostics.is_empty() {
        Ok(records)
    } else {
        Err(diagnostics)
    }
}

fn crate_name(path: &str) -> String {
    let mut segments = path.split('/');
    if segments.next() == Some("crates") {
        return segments.next().unwrap_or("workspace").to_owned();
    }
    if path.starts_with("fuzz/") {
        return "fuzz".to_owned();
    }
    if path.starts_with("conformance/") {
        return "conformance".to_owned();
    }
    "workspace".to_owned()
}

fn contains_any(content: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| content.contains(needle))
}

/// Find exact-content duplicates among classified sources.
#[must_use]
pub fn find_duplicate_groups(files: &[TestFileRecord]) -> Vec<DuplicateGroup> {
    let mut by_hash = BTreeMap::<&str, Vec<&str>>::new();
    for file in files {
        by_hash
            .entry(&file.content_sha256)
            .or_default()
            .push(&file.path);
    }
    by_hash
        .into_iter()
        .filter_map(|(hash, paths)| {
            (paths.len() > 1).then(|| DuplicateGroup {
                content_sha256: hash.to_owned(),
                paths: paths.into_iter().map(ToOwned::to_owned).collect(),
            })
        })
        .collect()
}

fn summarize_test_files(files: &[TestFileRecord], duplicate_groups: usize) -> TestInventorySummary {
    let mut classes = BTreeMap::<TestClass, (usize, usize)>::new();
    for file in files {
        let counts = classes.entry(file.class).or_default();
        counts.0 += 1;
        counts.1 += file.test_count;
    }
    TestInventorySummary {
        tracked_test_and_corpus_files: files.len(),
        direct_test_attributes: files.iter().map(|file| file.test_count).sum(),
        classes: classes
            .into_iter()
            .map(|(class, (file_count, test_count))| ClassSummary {
                class,
                file_count,
                test_count,
            })
            .collect(),
        file_backed_files: files.iter().filter(|file| file.uses_file_backend).count(),
        in_memory_files: files.iter().filter(|file| file.uses_memory).count(),
        mocked_files: files.iter().filter(|file| file.uses_mock).count(),
        property_files: files.iter().filter(|file| file.uses_proptest).count(),
        duplicate_groups,
    }
}

fn observe_baselines(
    contract: &TestInventoryContract,
    snapshot: &GitSnapshot,
    diagnostics: &mut Vec<InventoryDiagnostic>,
) -> Result<Vec<BaselineObservation>, String> {
    let observed = current_baseline_values(snapshot)?;
    let explanations = contract
        .baseline_delta_explanation
        .iter()
        .map(|entry| (entry.metric.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut result = Vec::with_capacity(contract.baseline.len());

    for baseline in &contract.baseline {
        let Some(observed_value) = observed.get(baseline.metric.as_str()).copied() else {
            diagnostics.push(InventoryDiagnostic::new(
                "baseline_metric_not_implemented",
                &baseline.metric,
                "canonical scanner has no implementation for this metric",
            ));
            continue;
        };
        let explanation = explanations.get(baseline.metric.as_str());
        if observed_value != baseline.reference_value {
            match explanation {
                Some(explanation) if explanation.observed_value == observed_value => {}
                Some(explanation) => diagnostics.push(InventoryDiagnostic::new(
                    "baseline_explanation_stale",
                    &baseline.metric,
                    format!(
                        "explanation_observed={} actual_observed={observed_value}",
                        explanation.observed_value
                    ),
                )),
                None => diagnostics.push(InventoryDiagnostic::new(
                    "baseline_delta_unexplained",
                    &baseline.metric,
                    format!(
                        "reference={} observed={observed_value}",
                        baseline.reference_value
                    ),
                )),
            }
        } else if explanation.is_some() {
            diagnostics.push(InventoryDiagnostic::new(
                "baseline_explanation_obsolete",
                &baseline.metric,
                "observed value matches the reference but an explanation remains",
            ));
        }
        let observed_i64 = i64::try_from(observed_value).map_err(|error| {
            format!(
                "baseline_observed_value_out_of_range metric={} value={observed_value} error={error}",
                baseline.metric
            )
        })?;
        let reference_i64 = i64::try_from(baseline.reference_value).map_err(|error| {
            format!(
                "baseline_reference_value_out_of_range metric={} value={} error={error}",
                baseline.metric, baseline.reference_value
            )
        })?;
        let delta = observed_i64.checked_sub(reference_i64).ok_or_else(|| {
            format!(
                "baseline_delta_out_of_range metric={} observed={observed_value} reference={}",
                baseline.metric, baseline.reference_value
            )
        })?;
        result.push(BaselineObservation {
            metric: baseline.metric.clone(),
            reference_value: baseline.reference_value,
            observed_value,
            delta,
            reference_command: baseline.reference_command.clone(),
            explanation: explanation.map(|entry| entry.rationale.clone()),
        });
    }
    Ok(result)
}

fn current_baseline_values(
    snapshot: &GitSnapshot,
) -> Result<BTreeMap<&'static str, usize>, String> {
    let harness_paths = snapshot
        .tracked_paths()
        .iter()
        .filter(|path| is_top_level_rust_test(path, "crates/fsqlite-harness/tests/"))
        .collect::<Vec<_>>();
    let e2e_paths = snapshot
        .tracked_paths()
        .iter()
        .filter(|path| is_top_level_rust_test(path, "crates/fsqlite-e2e/tests/"))
        .collect::<Vec<_>>();

    let mut tracker_shaped = 0_usize;
    let mut literal_beads_path = 0_usize;
    for path in &harness_paths {
        let content = snapshot.read_text(path)?;
        tracker_shaped += usize::from(content.contains("issues.jsonl"));
        literal_beads_path += usize::from(content.contains(".beads/issues.jsonl"));
    }
    let mut e2e_rusqlite = 0_usize;
    for path in &e2e_paths {
        e2e_rusqlite += usize::from(snapshot.read_text(path)?.contains("rusqlite"));
    }

    let fuzz_targets = snapshot
        .tracked_paths()
        .iter()
        .filter(|path| is_top_level_rust_test(path, "fuzz/fuzz_targets/"))
        .count();
    let fuzz_corpus_files = snapshot
        .tracked_paths()
        .iter()
        .filter(|path| path.starts_with("fuzz/") && path.contains("/corpus/"))
        .count();
    let slt_files = snapshot
        .tracked_paths()
        .iter()
        .filter(|path| path.starts_with("conformance/") && path.ends_with(".slt"))
        .count();

    Ok(BTreeMap::from([
        ("harness_top_level_integration_files", harness_paths.len()),
        ("harness_tracker_shaped_files", tracker_shaped),
        ("harness_literal_beads_path_files", literal_beads_path),
        ("e2e_top_level_integration_files", e2e_paths.len()),
        ("e2e_rusqlite_files", e2e_rusqlite),
        ("fuzz_targets", fuzz_targets),
        ("fuzz_corpus_files", fuzz_corpus_files),
        ("slt_files", slt_files),
    ]))
}

fn is_top_level_rust_test(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix)
        .is_some_and(|suffix| suffix.ends_with(".rs") && !suffix.contains('/'))
}

fn render_diagnostics(diagnostics: &[InventoryDiagnostic]) -> String {
    diagnostics
        .iter()
        .map(|diagnostic| {
            format!(
                "{} path={} message={}",
                diagnostic.code, diagnostic.path, diagnostic.message
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Serialize the canonical machine-readable report.
pub fn render_test_inventory_json(report: &TestInventoryReport) -> Result<String, String> {
    serde_json::to_string_pretty(report)
        .map(|mut json| {
            json.push('\n');
            json
        })
        .map_err(|error| format!("test_inventory_json_serialize_failed: {error}"))
}

/// Render the human-readable view from the same report model as JSON.
#[must_use]
pub fn render_test_inventory_markdown(report: &TestInventoryReport) -> String {
    let mut output = String::new();
    writeln!(output, "# Test Realism and Turso Adaptation Inventory").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "- Schema: `{}`", report.schema_version).unwrap();
    writeln!(output, "- Bead: `{}`", report.bead_id).unwrap();
    writeln!(output, "- Run: `{}`", report.run.run_id).unwrap();
    writeln!(output, "- Trace: `{}`", report.run.trace_id).unwrap();
    writeln!(output, "- Scenario: `{}`", report.run.scenario_id).unwrap();
    writeln!(
        output,
        "- FrankenSQLite source: `{}` (`{}`; dirty metadata recorded: `{}`)",
        report.provenance.source_revision,
        report.provenance.source_mode,
        report.provenance.source_dirty
    )
    .unwrap();
    writeln!(
        output,
        "- Turso source: `{}` (tree metadata verified: `{}`)",
        report.provenance.upstream_commit, report.provenance.upstream_tree_verified
    )
    .unwrap();
    writeln!(output).unwrap();

    writeln!(output, "## Test Summary").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "| Class | Files | Direct `#[test]` attributes |").unwrap();
    writeln!(output, "|---|---:|---:|").unwrap();
    for class in &report.summary.classes {
        writeln!(
            output,
            "| {} | {} | {} |",
            class.class.label(),
            class.file_count,
            class.test_count
        )
        .unwrap();
    }
    writeln!(
        output,
        "| file-backed overlay | {} | n/a |",
        report.summary.file_backed_files
    )
    .unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "Tracked test/corpus files: **{}**. Direct test attributes: **{}**. Exact-content duplicate groups: **{}**.",
        report.summary.tracked_test_and_corpus_files,
        report.summary.direct_test_attributes,
        report.summary.duplicate_groups
    )
    .unwrap();
    writeln!(output).unwrap();

    writeln!(output, "## Baseline Reconciliation").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "| Metric | Reference | Observed | Delta | Explanation |"
    )
    .unwrap();
    writeln!(output, "|---|---:|---:|---:|---|").unwrap();
    for baseline in &report.baseline {
        writeln!(
            output,
            "| `{}` | {} | {} | {:+} | {} |",
            markdown_cell(&baseline.metric),
            baseline.reference_value,
            baseline.observed_value,
            baseline.delta,
            markdown_cell(
                baseline
                    .explanation
                    .as_deref()
                    .unwrap_or("exactly reproduced")
            )
        )
        .unwrap();
    }
    writeln!(output).unwrap();

    writeln!(output, "## Turso Portfolio Decisions").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "| Source | Entries | Decision | Translation | Owner beads | Lanes | Existing owners |"
    )
    .unwrap();
    writeln!(output, "|---|---:|---|---|---|---|---|").unwrap();
    for entry in &report.portfolio {
        let lanes = entry
            .execution_lanes
            .iter()
            .map(|lane| lane.label())
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "| `{}` | {} | `{:?}` | `{:?}` | {} | {} | {} |",
            markdown_cell(&entry.source_path),
            entry.entry_count,
            entry.decision,
            entry.translation_class,
            markdown_cell(&entry.owner_beads.join(", ")),
            markdown_cell(&lanes),
            markdown_cell(&entry.duplicate_owners.join("; "))
        )
        .unwrap();
    }
    writeln!(output).unwrap();
    writeln!(
        output,
        "Decision totals: adopt={}, defer={}, reject={}.",
        report
            .decision_totals
            .get(&AdoptionDecision::Adopt)
            .copied()
            .unwrap_or_default(),
        report
            .decision_totals
            .get(&AdoptionDecision::Defer)
            .copied()
            .unwrap_or_default(),
        report
            .decision_totals
            .get(&AdoptionDecision::Reject)
            .copied()
            .unwrap_or_default()
    )
    .unwrap();
    writeln!(output).unwrap();

    writeln!(output, "## Contract Authority Handoff").unwrap();
    writeln!(output).unwrap();
    writeln!(
        output,
        "| Contract | Canonical path | Root duplicate | Live root consumers | Classified non-live references | Handoff |"
    )
    .unwrap();
    writeln!(output, "|---|---|---|---:|---:|---|").unwrap();
    for authority in &report.contract_authority {
        writeln!(
            output,
            "| `{}` | `{}` | `{}` | {} | {} | `{}` |",
            markdown_cell(&authority.logical_name),
            markdown_cell(&authority.canonical_path),
            markdown_cell(&authority.root_duplicate_path),
            authority.live_root_consumers.len(),
            authority.root_reference.len(),
            markdown_cell(&authority.handoff_bead)
        )
        .unwrap();
    }
    writeln!(output).unwrap();
    writeln!(
        output,
        "All five pairs are currently divergent. This inventory documents authority and exact references; bead `.18` owns making root duplicates inert and adding the drift guard."
    )
    .unwrap();
    writeln!(output).unwrap();

    writeln!(output, "## Reproduction").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "```text").unwrap();
    writeln!(output, "{}", report.run.command).unwrap();
    writeln!(output, "```").unwrap();
    output
}

/// Render a stable CSV compatible with the original script's file inventory.
#[must_use]
pub fn render_test_inventory_csv(report: &TestInventoryReport) -> String {
    let mut output = String::from(
        "crate,file,test_count,realism_tier,uses_mock,uses_memory,uses_file,is_proptest,uses_rusqlite,uses_tracker_metadata,uses_literal_beads_path,content_sha256\n",
    );
    for file in &report.files {
        writeln!(
            output,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_cell(&file.crate_name),
            csv_cell(&file.path),
            file.test_count,
            file.class.label(),
            file.uses_mock,
            file.uses_memory,
            file.uses_file_backend,
            file.uses_proptest,
            file.uses_rusqlite,
            file.uses_tracker_metadata,
            file.uses_literal_beads_path,
            file.content_sha256
        )
        .unwrap();
    }
    output
}

/// Write JSON, Markdown, and CSV atomically enough for one-process generation.
pub fn write_test_inventory_outputs(
    report: &TestInventoryReport,
    json_path: &Path,
    markdown_path: &Path,
    csv_path: &Path,
) -> Result<(), String> {
    for path in [json_path, markdown_path, csv_path] {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "test_inventory_output_dir_create_failed path={} error={error}",
                    parent.display()
                )
            })?;
        }
    }
    fs::write(json_path, render_test_inventory_json(report)?).map_err(|error| {
        format!(
            "test_inventory_json_write_failed path={} error={error}",
            json_path.display()
        )
    })?;
    fs::write(markdown_path, render_test_inventory_markdown(report)).map_err(|error| {
        format!(
            "test_inventory_markdown_write_failed path={} error={error}",
            markdown_path.display()
        )
    })?;
    fs::write(csv_path, render_test_inventory_csv(report)).map_err(|error| {
        format!(
            "test_inventory_csv_write_failed path={} error={error}",
            csv_path.display()
        )
    })?;
    Ok(())
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn csv_cell(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        csv_cell, is_full_git_hash, parse_dirty_paths, parse_env_bead_id_list, parse_env_path_list,
    };
    use std::collections::BTreeSet;

    #[test]
    fn dirty_paths_include_both_rename_sides_and_untracked_files() {
        let paths = parse_dirty_paths(
            b" M crates/demo/src/lib.rs\0R  new/name.rs\0old/name.rs\0?? scratch/input.sql\0",
        )
        .expect("parse NUL-delimited porcelain status");
        assert_eq!(
            paths,
            BTreeSet::from([
                "crates/demo/src/lib.rs".to_owned(),
                "new/name.rs".to_owned(),
                "old/name.rs".to_owned(),
                "scratch/input.sql".to_owned(),
            ])
        );
    }

    #[test]
    fn dirty_paths_reject_missing_rename_source() {
        let error = parse_dirty_paths(b"R  new/name.rs\0")
            .expect_err("rename without its source path must fail closed");
        assert!(error.starts_with("git_status_rename_source_missing"));
    }

    #[test]
    fn env_path_list_accepts_newline_separated_repository_paths() {
        let paths = parse_env_path_list(
            "TEST_PATHS",
            "crates/fsqlite-harness/src/test_inventory.rs\ndocs/contracts/supported_surface_matrix.toml\n",
        )
        .expect("repository-relative env paths are valid");
        assert_eq!(
            paths,
            vec![
                "crates/fsqlite-harness/src/test_inventory.rs".to_owned(),
                "docs/contracts/supported_surface_matrix.toml".to_owned(),
            ]
        );
    }

    #[test]
    fn env_path_list_accepts_semicolon_separated_repository_paths() {
        let paths = parse_env_path_list(
            "TEST_PATHS",
            "crates/fsqlite-harness/src/test_inventory.rs;docs/contracts/supported_surface_matrix.toml",
        )
        .expect("semicolon-separated repository-relative env paths are valid");
        assert_eq!(
            paths,
            vec![
                "crates/fsqlite-harness/src/test_inventory.rs".to_owned(),
                "docs/contracts/supported_surface_matrix.toml".to_owned(),
            ]
        );
    }

    #[test]
    fn env_path_list_accepts_mixed_newline_and_semicolon_separators() {
        let paths = parse_env_path_list(
            "TEST_PATHS",
            "crates/a.rs;crates/b.rs\ndocs/c.md;docs/d.md\n",
        )
        .expect("mixed supported separators are valid");
        assert_eq!(
            paths,
            vec![
                "crates/a.rs".to_owned(),
                "crates/b.rs".to_owned(),
                "docs/c.md".to_owned(),
                "docs/d.md".to_owned(),
            ]
        );
    }

    #[test]
    fn env_path_list_rejects_absolute_relative_traversal_and_cr_paths() {
        for raw in [
            "/tmp/file.rs",
            "./file.rs",
            "../file.rs",
            "a/../file.rs",
            "..\\file.rs",
            "a\\..\\file.rs",
            "ok\r.rs",
        ] {
            let error = parse_env_path_list("TEST_PATHS", raw)
                .expect_err("invalid env path must fail closed");
            assert!(error.starts_with("git_env_path_"));
        }
    }

    #[test]
    fn env_bead_id_list_accepts_repository_issue_ids() {
        let ids = parse_env_bead_id_list(
            "TEST_BEADS",
            "bd-turso-test-adaptation-zu081.10;bd-2lt76.1\nbd-uh1fv",
        )
        .expect("mixed supported separators are valid");
        assert_eq!(
            ids,
            vec![
                "bd-turso-test-adaptation-zu081.10".to_owned(),
                "bd-2lt76.1".to_owned(),
                "bd-uh1fv".to_owned(),
            ]
        );
    }

    #[test]
    fn env_bead_id_list_rejects_path_like_values() {
        let error = parse_env_bead_id_list("TEST_BEADS", "bd-ok;/tmp/file")
            .expect_err("path-like Beads ID must fail closed");
        assert!(error.starts_with("git_env_bead_id_invalid"));
    }

    #[test]
    fn full_git_hash_requires_exact_forty_hex_characters() {
        assert!(is_full_git_hash("0123456789abcdef0123456789ABCDEF01234567"));
        assert!(!is_full_git_hash("0123456789abcdef0123456789abcdef0123456"));
        assert!(!is_full_git_hash(
            "0123456789abcdef0123456789abcdef0123456z"
        ));
    }

    #[test]
    fn csv_cells_quote_commas_quotes_and_newlines() {
        assert_eq!(csv_cell("plain/path.rs"), "plain/path.rs");
        assert_eq!(csv_cell("a,b"), "\"a,b\"");
        assert_eq!(csv_cell("a\"b"), "\"a\"\"b\"");
        assert_eq!(csv_cell("a\nb"), "\"a\nb\"");
    }
}
