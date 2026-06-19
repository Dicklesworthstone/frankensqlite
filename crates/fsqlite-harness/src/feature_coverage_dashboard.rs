//! Feature-to-test coverage dashboard for Track C conformance evidence.
//!
//! The dashboard reads the canonical corpus manifest and accounts for every
//! required feature ID by family. Missing required coverage is a blocking
//! release-gate failure; seed-only coverage remains visible as `partial`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::fixture_root_contract::DEFAULT_FIXTURE_ROOT_MANIFEST_PATH;

/// Bead identifier for the feature coverage dashboard.
pub const FEATURE_COVERAGE_DASHBOARD_BEAD_ID: &str = "bd-2yqp6.3.3";
/// Machine-readable dashboard schema version.
pub const FEATURE_COVERAGE_DASHBOARD_SCHEMA_VERSION: &str = "1.0.0";
/// Coverage-accounting manifest section schema version.
pub const COVERAGE_ACCOUNTING_SCHEMA_VERSION: &str = "1.0.0";
/// Release-gate policy encoded in the canonical manifest.
pub const COVERAGE_RELEASE_GATE_POLICY: &str = "missing_required_features_block";
/// Default canonical corpus manifest path relative to the workspace root.
pub const DEFAULT_FEATURE_COVERAGE_MANIFEST_PATH: &str = DEFAULT_FIXTURE_ROOT_MANIFEST_PATH;

/// Run metadata embedded in generated dashboard artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureCoverageRunMetadata {
    /// Trace identifier shared by JSONL logs and the dashboard artifact.
    pub trace_id: String,
    /// Concrete run identifier for artifact directories.
    pub run_id: String,
    /// Scenario identifier for this gate.
    pub scenario_id: String,
    /// Deterministic seed for the gate run.
    pub seed: u64,
    /// Caller-supplied generation timestamp. Use `0` for reproducible reports.
    pub generated_unix_ms: u128,
}

impl FeatureCoverageRunMetadata {
    /// Build deterministic default run metadata for tests and local checks.
    #[must_use]
    pub fn deterministic() -> Self {
        Self {
            trace_id: format!("trace-{FEATURE_COVERAGE_DASHBOARD_BEAD_ID}-deterministic"),
            run_id: format!("{FEATURE_COVERAGE_DASHBOARD_BEAD_ID}-deterministic"),
            scenario_id: "PARITY-COVERAGE-C3".to_owned(),
            seed: 3520,
            generated_unix_ms: 0,
        }
    }
}

/// Per-feature coverage state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureCoverageStatus {
    /// The required feature has no in-scope manifest entry.
    None,
    /// The feature has manifest entries, but none are executable fixtures.
    Partial,
    /// The feature has at least one executable fixture entry.
    Full,
}

/// Release-gate outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseGateOutcome {
    /// No required feature is missing all coverage.
    Pass,
    /// At least one required feature is missing all coverage.
    Fail,
}

/// A corpus entry that contributes test evidence for a feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureTestEntry {
    /// Corpus manifest entry identifier.
    pub entry_id: String,
    /// Human-readable test title.
    pub title: String,
    /// Corpus category family, such as `ddl` or `functions`.
    pub category: String,
    /// Fixture path or seed URI.
    pub source: String,
    /// Manifest shard identifier.
    pub shard_id: String,
    /// Whether this entry is an executable fixture rather than seed coverage.
    pub execution_required: bool,
}

/// Concrete shard/run metadata that can replay feature evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureRunReference {
    /// Manifest shard identifier.
    pub shard_id: String,
    /// Scenario identifier used by structured logs.
    pub scenario_id: String,
    /// Deterministic shard seed.
    pub seed: u64,
    /// Run-ID template from the canonical manifest.
    pub run_id_template: String,
    /// Trace-ID template from the canonical manifest.
    pub trace_id_template: String,
    /// Operator replay command from the canonical manifest.
    pub replay_command: String,
}

/// Coverage accounting for a single required feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureCoverageRow {
    /// Canonical parity taxonomy feature ID, such as `F-SQL.16`.
    pub feature_id: String,
    /// Feature family parsed from the ID, such as `SQL`.
    pub family: String,
    /// None/partial/full coverage state.
    pub status: FeatureCoverageStatus,
    /// Number of in-scope manifest entries that reference this feature.
    pub entry_count: usize,
    /// Number of executable entries that reference this feature.
    pub executable_entry_count: usize,
    /// Sorted categories reached by this feature.
    pub categories: Vec<String>,
    /// Sorted evidence sources reached by this feature.
    pub sources: Vec<String>,
    /// Test entries that account for this feature.
    pub test_entries: Vec<FeatureTestEntry>,
    /// Shard/run references that can reproduce this feature's evidence.
    pub run_references: Vec<FeatureRunReference>,
}

/// Coverage summary for one feature family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureFamilyCoverage {
    /// Feature family, such as `SQL`, `FUN`, or `PGM`.
    pub family: String,
    /// Required feature count in this family.
    pub total_features: usize,
    /// Required features with no manifest coverage.
    pub none_count: usize,
    /// Required features with seed-only/non-executable coverage.
    pub partial_count: usize,
    /// Required features with at least one executable fixture.
    pub full_count: usize,
    /// Weighted coverage points: full=2, partial=1, none=0.
    pub coverage_points: usize,
    /// Maximum possible weighted coverage points.
    pub coverage_possible_points: usize,
    /// True when this family has at least one `none` feature.
    pub release_blocked: bool,
}

/// Release-gate summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureCoverageReleaseGate {
    /// Overall gate outcome.
    pub outcome: ReleaseGateOutcome,
    /// Release policy name from the canonical manifest.
    pub policy: String,
    /// Number of required features with no manifest entry.
    pub missing_feature_count: usize,
    /// Number of required features with only partial coverage.
    pub partial_feature_count: usize,
    /// Blocking feature IDs.
    pub blocking_features: Vec<String>,
}

/// Machine-readable feature coverage dashboard artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureCoverageDashboard {
    /// Dashboard schema version.
    pub schema_version: String,
    /// Bead this dashboard implements.
    pub bead_id: String,
    /// Run trace identifier.
    pub trace_id: String,
    /// Run identifier.
    pub run_id: String,
    /// Scenario identifier.
    pub scenario_id: String,
    /// Deterministic seed.
    pub seed: u64,
    /// Caller-supplied generation timestamp.
    pub generated_unix_ms: u128,
    /// Workspace-relative canonical manifest path.
    pub source_manifest_path: String,
    /// SHA-256 of the raw canonical manifest bytes.
    pub source_manifest_sha256: String,
    /// Stable hash over feature/family coverage rows.
    pub coverage_signature_sha256: String,
    /// Corpus manifest schema version.
    pub manifest_schema_version: String,
    /// Corpus manifest bead ID.
    pub manifest_bead_id: String,
    /// Corpus manifest track ID.
    pub manifest_track_id: String,
    /// Corpus manifest SQLite target version.
    pub sqlite_target: String,
    /// Corpus manifest root seed.
    pub root_seed: u64,
    /// Count of all manifest entries.
    pub manifest_entry_count: usize,
    /// Count of in-scope manifest entries.
    pub in_scope_entry_count: usize,
    /// Number of required features accounted for.
    pub required_feature_count: usize,
    /// Family-level coverage summaries.
    pub families: Vec<FeatureFamilyCoverage>,
    /// Per-feature coverage rows.
    pub features: Vec<FeatureCoverageRow>,
    /// Release-gate verdict.
    pub release_gate: FeatureCoverageReleaseGate,
}

#[derive(Debug, Deserialize)]
struct CorpusManifestDocument {
    meta: CorpusManifestMeta,
    coverage_accounting: Option<CoverageAccountingSection>,
    #[serde(default)]
    entries: Vec<CorpusEntry>,
    #[serde(default)]
    shards: Vec<CorpusShard>,
}

#[derive(Debug, Deserialize)]
struct CorpusManifestMeta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    sqlite_target: String,
    root_seed: u64,
}

#[derive(Debug, Deserialize)]
struct CoverageAccountingSection {
    schema_version: String,
    bead_id: String,
    release_gate: String,
    status_semantics: String,
    required_feature_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CorpusEntry {
    entry_id: String,
    title: String,
    category: String,
    source: String,
    #[serde(default)]
    in_scope: bool,
    #[serde(default)]
    feature_ids: Vec<String>,
    shard_id: String,
    #[serde(default)]
    execution_required: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct CorpusShard {
    shard_id: String,
    scenario_id: String,
    seed: u64,
    run_id_template: String,
    trace_id_template: String,
    replay_command: String,
}

#[derive(Debug, Clone)]
struct NormalizedEntry {
    feature_ids: Vec<String>,
    test_entry: FeatureTestEntry,
    run_reference: FeatureRunReference,
}

#[derive(Debug, Default)]
struct FamilyAccumulator {
    total_features: usize,
    none_count: usize,
    partial_count: usize,
    full_count: usize,
}

#[derive(Serialize)]
struct CoverageSignature<'a> {
    schema_version: &'a str,
    manifest_sha256: &'a str,
    families: &'a [FeatureFamilyCoverage],
    features: &'a [FeatureCoverageRow],
    release_gate: &'a FeatureCoverageReleaseGate,
}

/// Build a feature coverage dashboard from the canonical corpus manifest.
///
/// # Errors
///
/// Returns `Err` when the manifest cannot be read, cannot be parsed, or has
/// inconsistent feature/shard coverage accounting.
pub fn build_feature_coverage_dashboard(
    workspace_root: &Path,
    manifest_path: &Path,
    run: FeatureCoverageRunMetadata,
) -> Result<FeatureCoverageDashboard, String> {
    validate_run_metadata(&run)?;

    let manifest_path = resolve_workspace_path(workspace_root, manifest_path);
    if !manifest_path.is_file() {
        return Err(format!(
            "feature_coverage_manifest_missing path={}",
            manifest_path.display()
        ));
    }

    let raw = fs::read_to_string(&manifest_path).map_err(|error| {
        format!(
            "feature_coverage_manifest_read_failed path={} error={error}",
            manifest_path.display()
        )
    })?;
    if raw.trim().is_empty() {
        return Err(format!(
            "feature_coverage_manifest_empty path={}",
            manifest_path.display()
        ));
    }
    let manifest_sha256 = sha256_hex(raw.as_bytes());

    let doc = toml::from_str::<CorpusManifestDocument>(&raw).map_err(|error| {
        format!(
            "feature_coverage_manifest_parse_failed path={} error={error}",
            manifest_path.display()
        )
    })?;

    validate_meta(&doc.meta)?;

    let shards_by_id = index_shards(doc.shards)?;
    let normalized_entries = normalize_entries(doc.entries.clone(), &shards_by_id)?;
    let in_scope_entry_count = normalized_entries.len();
    let mut entry_feature_ids = BTreeSet::new();
    let mut entries_by_feature: BTreeMap<String, Vec<NormalizedEntry>> = BTreeMap::new();
    for entry in &normalized_entries {
        for feature_id in &entry.feature_ids {
            entry_feature_ids.insert(feature_id.clone());
            entries_by_feature
                .entry(feature_id.clone())
                .or_default()
                .push(entry.clone());
        }
    }

    let required_feature_ids =
        required_features_from_manifest(doc.coverage_accounting, &entry_feature_ids)?;
    validate_entry_features_are_required(&required_feature_ids, &entry_feature_ids)?;

    let mut family_accumulators: BTreeMap<String, FamilyAccumulator> = BTreeMap::new();
    let mut features = Vec::with_capacity(required_feature_ids.len());
    let mut missing_features = Vec::new();
    let mut partial_feature_count = 0_usize;

    for feature_id in &required_feature_ids {
        let family = feature_family(feature_id)?;
        let entries = entries_by_feature
            .get(feature_id)
            .cloned()
            .unwrap_or_default();
        let executable_entry_count = entries
            .iter()
            .filter(|entry| entry.test_entry.execution_required)
            .count();
        let status = if entries.is_empty() {
            FeatureCoverageStatus::None
        } else if executable_entry_count == 0 {
            FeatureCoverageStatus::Partial
        } else {
            FeatureCoverageStatus::Full
        };

        match status {
            FeatureCoverageStatus::None => missing_features.push(feature_id.clone()),
            FeatureCoverageStatus::Partial => partial_feature_count += 1,
            FeatureCoverageStatus::Full => {}
        }

        let family_accumulator = family_accumulators.entry(family.clone()).or_default();
        family_accumulator.total_features += 1;
        match status {
            FeatureCoverageStatus::None => family_accumulator.none_count += 1,
            FeatureCoverageStatus::Partial => family_accumulator.partial_count += 1,
            FeatureCoverageStatus::Full => family_accumulator.full_count += 1,
        }

        features.push(build_feature_row(
            feature_id,
            family,
            status,
            executable_entry_count,
            entries,
        ));
    }

    let families = build_family_summaries(family_accumulators);
    let release_gate = FeatureCoverageReleaseGate {
        outcome: if missing_features.is_empty() {
            ReleaseGateOutcome::Pass
        } else {
            ReleaseGateOutcome::Fail
        },
        policy: COVERAGE_RELEASE_GATE_POLICY.to_owned(),
        missing_feature_count: missing_features.len(),
        partial_feature_count,
        blocking_features: missing_features,
    };

    let signature = CoverageSignature {
        schema_version: FEATURE_COVERAGE_DASHBOARD_SCHEMA_VERSION,
        manifest_sha256: &manifest_sha256,
        families: &families,
        features: &features,
        release_gate: &release_gate,
    };
    let signature_payload = serde_json::to_string(&signature)
        .map_err(|error| format!("signature_json_failed: {error}"))?;

    Ok(FeatureCoverageDashboard {
        schema_version: FEATURE_COVERAGE_DASHBOARD_SCHEMA_VERSION.to_owned(),
        bead_id: FEATURE_COVERAGE_DASHBOARD_BEAD_ID.to_owned(),
        trace_id: run.trace_id,
        run_id: run.run_id,
        scenario_id: run.scenario_id,
        seed: run.seed,
        generated_unix_ms: run.generated_unix_ms,
        source_manifest_path: workspace_relative_path(workspace_root, &manifest_path),
        source_manifest_sha256: manifest_sha256.clone(),
        coverage_signature_sha256: sha256_hex(signature_payload.as_bytes()),
        manifest_schema_version: doc.meta.schema_version,
        manifest_bead_id: doc.meta.bead_id,
        manifest_track_id: doc.meta.track_id,
        sqlite_target: doc.meta.sqlite_target,
        root_seed: doc.meta.root_seed,
        manifest_entry_count: doc.entries.len(),
        in_scope_entry_count,
        required_feature_count: required_feature_ids.len(),
        families,
        features,
        release_gate,
    })
}

/// Write a dashboard artifact as pretty JSON.
///
/// # Errors
///
/// Returns `Err` when serialization or writing fails.
pub fn write_feature_coverage_dashboard(
    dashboard: &FeatureCoverageDashboard,
    output_path: &Path,
) -> Result<(), String> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "feature_coverage_output_dir_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    let payload = serde_json::to_string_pretty(dashboard)
        .map_err(|error| format!("feature_coverage_json_failed: {error}"))?;
    fs::write(output_path, format!("{payload}\n")).map_err(|error| {
        format!(
            "feature_coverage_output_write_failed path={} error={error}",
            output_path.display()
        )
    })
}

fn validate_run_metadata(run: &FeatureCoverageRunMetadata) -> Result<(), String> {
    non_empty_string("run.trace_id", &run.trace_id)?;
    non_empty_string("run.run_id", &run.run_id)?;
    non_empty_string("run.scenario_id", &run.scenario_id)?;
    if run.seed == 0 {
        return Err("run.seed must be > 0".to_owned());
    }
    Ok(())
}

fn validate_meta(meta: &CorpusManifestMeta) -> Result<(), String> {
    if meta.schema_version != "1.0.0" {
        return Err(format!(
            "feature_coverage_manifest_schema_version_mismatch expected=1.0.0 observed={}",
            meta.schema_version
        ));
    }
    non_empty_string("meta.bead_id", &meta.bead_id)?;
    non_empty_string("meta.track_id", &meta.track_id)?;
    non_empty_string("meta.sqlite_target", &meta.sqlite_target)?;
    if meta.root_seed == 0 {
        return Err("meta.root_seed must be > 0".to_owned());
    }
    Ok(())
}

fn index_shards(shards: Vec<CorpusShard>) -> Result<BTreeMap<String, CorpusShard>, String> {
    let mut by_id = BTreeMap::new();
    for shard in shards {
        let shard_id = non_empty_string("shards.shard_id", &shard.shard_id)?;
        non_empty_string("shards.scenario_id", &shard.scenario_id)?;
        non_empty_string("shards.run_id_template", &shard.run_id_template)?;
        non_empty_string("shards.trace_id_template", &shard.trace_id_template)?;
        non_empty_string("shards.replay_command", &shard.replay_command)?;
        if shard.seed == 0 {
            return Err(format!(
                "feature_coverage_shard_seed_zero shard_id={shard_id}"
            ));
        }
        if by_id.insert(shard_id.clone(), shard).is_some() {
            return Err(format!(
                "feature_coverage_duplicate_shard shard_id={shard_id}"
            ));
        }
    }
    Ok(by_id)
}

fn normalize_entries(
    entries: Vec<CorpusEntry>,
    shards_by_id: &BTreeMap<String, CorpusShard>,
) -> Result<Vec<NormalizedEntry>, String> {
    let mut normalized = Vec::new();
    let mut seen_entry_ids = BTreeSet::new();

    for entry in entries {
        if !entry.in_scope {
            continue;
        }
        let entry_id = non_empty_string("entries.entry_id", &entry.entry_id)?;
        if !seen_entry_ids.insert(entry_id.clone()) {
            return Err(format!(
                "feature_coverage_duplicate_entry entry_id={entry_id}"
            ));
        }
        let title = non_empty_string("entries.title", &entry.title)?;
        let category = non_empty_string("entries.category", &entry.category)?;
        let source = non_empty_string("entries.source", &entry.source)?;
        let shard_id = non_empty_string("entries.shard_id", &entry.shard_id)?;
        let shard = shards_by_id.get(&shard_id).ok_or_else(|| {
            format!("feature_coverage_entry_unknown_shard entry_id={entry_id} shard_id={shard_id}")
        })?;
        let feature_ids = normalize_feature_ids("entries.feature_ids", entry.feature_ids)?;
        if feature_ids.is_empty() {
            return Err(format!(
                "feature_coverage_entry_missing_features entry_id={entry_id}"
            ));
        }

        normalized.push(NormalizedEntry {
            feature_ids,
            test_entry: FeatureTestEntry {
                entry_id,
                title,
                category,
                source,
                shard_id: shard_id.clone(),
                execution_required: entry.execution_required,
            },
            run_reference: FeatureRunReference {
                shard_id,
                scenario_id: shard.scenario_id.clone(),
                seed: shard.seed,
                run_id_template: shard.run_id_template.clone(),
                trace_id_template: shard.trace_id_template.clone(),
                replay_command: shard.replay_command.clone(),
            },
        });
    }

    Ok(normalized)
}

fn required_features_from_manifest(
    coverage_accounting: Option<CoverageAccountingSection>,
    entry_feature_ids: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    if let Some(section) = coverage_accounting {
        if section.schema_version != COVERAGE_ACCOUNTING_SCHEMA_VERSION {
            return Err(format!(
                "coverage_accounting_schema_version_mismatch expected={} observed={}",
                COVERAGE_ACCOUNTING_SCHEMA_VERSION, section.schema_version
            ));
        }
        if section.bead_id != FEATURE_COVERAGE_DASHBOARD_BEAD_ID {
            return Err(format!(
                "coverage_accounting_bead_id_mismatch expected={} observed={}",
                FEATURE_COVERAGE_DASHBOARD_BEAD_ID, section.bead_id
            ));
        }
        if section.release_gate != COVERAGE_RELEASE_GATE_POLICY {
            return Err(format!(
                "coverage_accounting_release_gate_mismatch expected={} observed={}",
                COVERAGE_RELEASE_GATE_POLICY, section.release_gate
            ));
        }
        non_empty_string(
            "coverage_accounting.status_semantics",
            &section.status_semantics,
        )?;
        let required = normalize_feature_ids(
            "coverage_accounting.required_feature_ids",
            section.required_feature_ids,
        )?;
        if required.is_empty() {
            return Err("coverage_accounting.required_feature_ids must be non-empty".to_owned());
        }
        return Ok(required);
    }

    if entry_feature_ids.is_empty() {
        return Err("feature_coverage_no_required_features".to_owned());
    }
    Ok(entry_feature_ids.iter().cloned().collect())
}

fn validate_entry_features_are_required(
    required_feature_ids: &[String],
    entry_feature_ids: &BTreeSet<String>,
) -> Result<(), String> {
    let required: BTreeSet<&str> = required_feature_ids.iter().map(String::as_str).collect();
    let mut unlisted = entry_feature_ids
        .iter()
        .filter(|feature_id| !required.contains(feature_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if unlisted.is_empty() {
        return Ok(());
    }
    unlisted.sort();
    Err(format!(
        "feature_coverage_entry_feature_not_required features={}",
        unlisted.join(",")
    ))
}

fn build_feature_row(
    feature_id: &str,
    family: String,
    status: FeatureCoverageStatus,
    executable_entry_count: usize,
    entries: Vec<NormalizedEntry>,
) -> FeatureCoverageRow {
    let mut categories = BTreeSet::new();
    let mut sources = BTreeSet::new();
    let mut run_references_by_shard = BTreeMap::new();
    let mut test_entries = Vec::with_capacity(entries.len());

    for entry in entries {
        categories.insert(entry.test_entry.category.clone());
        sources.insert(entry.test_entry.source.clone());
        run_references_by_shard
            .entry(entry.run_reference.shard_id.clone())
            .or_insert(entry.run_reference);
        test_entries.push(entry.test_entry);
    }

    test_entries.sort_by(|left, right| left.entry_id.cmp(&right.entry_id));

    FeatureCoverageRow {
        feature_id: feature_id.to_owned(),
        family,
        status,
        entry_count: test_entries.len(),
        executable_entry_count,
        categories: categories.into_iter().collect(),
        sources: sources.into_iter().collect(),
        test_entries,
        run_references: run_references_by_shard.into_values().collect(),
    }
}

fn build_family_summaries(
    accumulators: BTreeMap<String, FamilyAccumulator>,
) -> Vec<FeatureFamilyCoverage> {
    accumulators
        .into_iter()
        .map(|(family, accumulator)| {
            let coverage_points =
                accumulator.full_count.saturating_mul(2) + accumulator.partial_count;
            let coverage_possible_points = accumulator.total_features.saturating_mul(2);
            FeatureFamilyCoverage {
                family,
                total_features: accumulator.total_features,
                none_count: accumulator.none_count,
                partial_count: accumulator.partial_count,
                full_count: accumulator.full_count,
                coverage_points,
                coverage_possible_points,
                release_blocked: accumulator.none_count > 0,
            }
        })
        .collect()
}

fn normalize_feature_ids(
    field_name: &str,
    feature_ids: Vec<String>,
) -> Result<Vec<String>, String> {
    let mut normalized = Vec::with_capacity(feature_ids.len());
    let mut seen = BTreeSet::new();
    for feature_id in feature_ids {
        let feature_id = non_empty_string(field_name, &feature_id)?;
        feature_family(&feature_id)?;
        if !seen.insert(feature_id.clone()) {
            return Err(format!(
                "{field_name} contains duplicate feature_id={feature_id}"
            ));
        }
        normalized.push(feature_id);
    }
    normalized.sort();
    Ok(normalized)
}

fn feature_family(feature_id: &str) -> Result<String, String> {
    let Some((family, ordinal)) = feature_id
        .strip_prefix("F-")
        .and_then(|tail| tail.split_once('.'))
    else {
        return Err(format!("invalid_feature_id value={feature_id}"));
    };
    if family.is_empty()
        || !family.chars().all(|ch| ch.is_ascii_uppercase())
        || ordinal.len() < 2
        || !ordinal.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!("invalid_feature_id value={feature_id}"));
    }
    Ok(family.to_owned())
}

fn non_empty_string(field_name: &str, value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(format!("{field_name} must be non-empty"))
    } else {
        Ok(trimmed.to_owned())
    }
}

fn resolve_workspace_path(workspace_root: &Path, path: &Path) -> PathBuf {
    if path.is_relative() {
        workspace_root.join(path)
    } else {
        path.to_path_buf()
    }
}

fn workspace_relative_path(workspace_root: &Path, path: &Path) -> String {
    path.strip_prefix(workspace_root).map_or_else(
        |_| path.to_string_lossy().replace('\\', "/"),
        |relative| relative.to_string_lossy().replace('\\', "/"),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    crate::bytes_to_lower_hex(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_manifest(workspace: &Path, body: &str) -> PathBuf {
        let path = workspace.join("corpus_manifest.toml");
        fs::write(&path, body).expect("write synthetic manifest");
        path
    }

    fn synthetic_manifest() -> String {
        r#"[meta]
schema_version = "1.0.0"
bead_id = "bd-test"
track_id = "bd-test-track"
sqlite_target = "3.52.0"
root_seed = 3520

[coverage_accounting]
schema_version = "1.0.0"
bead_id = "bd-2yqp6.3.3"
release_gate = "missing_required_features_block"
status_semantics = "none=no in-scope entries; partial=no executable entries; full=has executable entry"
required_feature_ids = ["F-FUN.01", "F-SQL.01", "F-SQL.02"]

[[entries]]
entry_id = "C3-001"
title = "Executable SELECT"
category = "ddl"
source = "fixtures/select.json"
in_scope = true
feature_ids = ["F-SQL.01"]
shard_id = "core"
execution_required = true

[[entries]]
entry_id = "C3-002"
title = "Seed-only function"
category = "functions"
source = "seed://functions/core"
in_scope = true
feature_ids = ["F-SQL.02"]
shard_id = "core"
execution_required = false

[[shards]]
shard_id = "core"
scenario_id = "PARITY-COVERAGE-C3-UNIT"
seed = 3520
run_id_template = "run-{seed}"
trace_id_template = "trace-{seed}"
replay_command = "cargo test -p fsqlite-harness synthetic"
"#
        .to_owned()
    }

    #[test]
    fn dashboard_reports_none_partial_and_full() {
        let workspace = TempDir::new().expect("tempdir");
        let manifest_path = write_manifest(workspace.path(), &synthetic_manifest());

        let dashboard = build_feature_coverage_dashboard(
            workspace.path(),
            &manifest_path,
            FeatureCoverageRunMetadata::deterministic(),
        )
        .expect("build dashboard");

        let statuses = dashboard
            .features
            .iter()
            .map(|row| (row.feature_id.as_str(), row.status))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(statuses["F-SQL.01"], FeatureCoverageStatus::Full);
        assert_eq!(statuses["F-SQL.02"], FeatureCoverageStatus::Partial);
        assert_eq!(statuses["F-FUN.01"], FeatureCoverageStatus::None);
        assert_eq!(dashboard.release_gate.outcome, ReleaseGateOutcome::Fail);
        assert_eq!(
            dashboard.release_gate.blocking_features,
            vec!["F-FUN.01".to_owned()]
        );
    }

    #[test]
    fn entry_features_must_be_listed_in_required_universe() {
        let workspace = TempDir::new().expect("tempdir");
        let manifest = synthetic_manifest().replace(
            r#"required_feature_ids = ["F-FUN.01", "F-SQL.01", "F-SQL.02"]"#,
            r#"required_feature_ids = ["F-FUN.01", "F-SQL.01"]"#,
        );
        let manifest_path = write_manifest(workspace.path(), &manifest);

        let error = build_feature_coverage_dashboard(
            workspace.path(),
            &manifest_path,
            FeatureCoverageRunMetadata::deterministic(),
        )
        .expect_err("unlisted entry feature should fail");

        assert!(error.contains("feature_coverage_entry_feature_not_required"));
        assert!(error.contains("F-SQL.02"));
    }
}
