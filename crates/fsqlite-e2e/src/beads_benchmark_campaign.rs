//! Canonical Beads benchmark campaign contract.
//!
//! Bead: bd-db300.1.2
//!
//! This module freezes the real-fixture benchmark campaign for the many-core
//! write-path program. Later optimization beads should point at this tracked
//! manifest instead of reconstructing fixture/workload/mode/placement choices
//! from ad hoc shell history.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable schema identifier for the canonical Beads benchmark campaign.
pub const CANONICAL_BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1: &str =
    "fsqlite-e2e.canonical_beads_benchmark_campaign.v1";

/// Workspace-relative path to the canonical Beads benchmark campaign manifest.
pub const CANONICAL_BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE: &str =
    "sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json";

/// Top-level tracked contract for the canonical Beads benchmark campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalBeadsBenchmarkCampaign {
    pub schema_version: String,
    pub bead_id: String,
    pub campaign_id: String,
    pub description: String,
    pub fixture_root: String,
    pub build_profile: String,
    pub beads_data_source: BeadsDataSource,
    pub seed_policy: SeedPolicy,
    pub retry_policy: RetryPolicy,
    pub hardware_classes: Vec<HardwareClass>,
    pub placement_profiles: Vec<PlacementProfile>,
    pub fixtures: Vec<CampaignFixture>,
    pub workloads: Vec<CampaignWorkload>,
    pub modes: Vec<CampaignMode>,
    pub scenarios: Vec<CampaignScenario>,
    pub artifact_naming: ArtifactNamingContract,
}

/// How the campaign fingerprints the Beads backlog snapshot used for evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeadsDataSource {
    pub path: String,
    pub hash_algorithm: String,
}

/// Deterministic seed contract for canonical runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedPolicy {
    pub id: String,
    pub root_seed: u64,
    pub worker_seed_derivation: String,
    pub notes: String,
}

/// Retry contract pinned for both engines in the canonical matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub id: String,
    pub sqlite: RetryMode,
    pub fsqlite: RetryMode,
    pub notes: String,
}

/// Retry settings for one executor family.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryMode {
    pub max_busy_retries: u32,
    pub backoff_base_ms: u64,
    pub backoff_cap_ms: u64,
    #[serde(default)]
    pub busy_timeout_ms: Option<u32>,
}

/// Campaign hardware-class identifier for many-core comparisons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareClass {
    pub id: String,
    pub cpu_model: String,
    pub logical_cpus: u16,
    pub physical_cores: u16,
    pub sockets: u16,
    pub numa_nodes: u16,
    pub llc_domains: u16,
    pub smt_enabled: bool,
    pub notes: String,
}

/// CPU placement contract keyed by campaign hardware class and concurrency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementProfile {
    pub id: String,
    pub kind: String,
    pub hardware_class_ids: Vec<String>,
    #[serde(default)]
    pub taskset_cpu_list_by_concurrency: BTreeMap<String, String>,
    pub notes: String,
}

/// Canonical fixture snapshot used in the campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignFixture {
    pub id: String,
    pub source_path: String,
    pub snapshot_path: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub digest_source: String,
    pub notes: String,
}

/// Canonical workload definition used in the campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignWorkload {
    pub id: String,
    pub description: String,
    pub scale: u32,
    pub notes: String,
}

/// Canonical benchmark mode (SQLite baseline, MVCC, forced single-writer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignMode {
    pub id: String,
    pub benchmark_engine_label: String,
    pub engine: String,
    #[serde(default)]
    pub concurrent_mode: Option<bool>,
    pub notes: String,
}

/// One explicit fixture/workload/concurrency slice in the canonical matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignScenario {
    pub fixture_id: String,
    pub workload_id: String,
    pub concurrency: u16,
    pub mode_ids: Vec<String>,
    pub placement_profile_ids: Vec<String>,
}

/// Stable artifact naming contract for canonical campaign output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactNamingContract {
    pub output_root: String,
    pub directory_template: String,
    pub file_stem_template: String,
    pub required_context: Vec<String>,
}

/// Fully expanded run cell after multiplying a scenario by mode and placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandedCampaignRunCell {
    pub fixture_id: String,
    pub workload_id: String,
    pub concurrency: u16,
    pub mode_id: String,
    pub placement_profile_id: String,
}

/// Dynamic context required to render canonical artifact names.
#[derive(Debug, Clone, Copy)]
pub struct CampaignArtifactContext<'a> {
    pub date_yyyymmdd: &'a str,
    pub commit_sha: &'a str,
    pub beads_data_hash: &'a str,
    pub hardware_class_id: &'a str,
}

impl CanonicalBeadsBenchmarkCampaign {
    /// Load the canonical campaign from the default workspace-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, parsed, or validated.
    pub fn load(workspace_root: &Path) -> Result<Self, String> {
        load_canonical_beads_benchmark_campaign(workspace_root)
    }

    /// Validate logical constraints that are stricter than the JSON schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is malformed or internally inconsistent.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CANONICAL_BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1 {
            return Err(format!(
                "unexpected campaign schema_version `{}`",
                self.schema_version
            ));
        }
        if self.build_profile.trim().is_empty() {
            return Err("build_profile must not be empty".to_owned());
        }
        if self.seed_policy.root_seed == 0 {
            return Err("seed_policy.root_seed must be non-zero".to_owned());
        }
        if self.beads_data_source.hash_algorithm != "sha256" {
            return Err(format!(
                "unsupported Beads data hash algorithm `{}`",
                self.beads_data_source.hash_algorithm
            ));
        }

        validate_retry_mode("sqlite", &self.retry_policy.sqlite)?;
        validate_retry_mode("fsqlite", &self.retry_policy.fsqlite)?;

        let fixture_ids = collect_unique_ids(
            "fixture",
            self.fixtures.iter().map(|fixture| fixture.id.as_str()),
        )?;
        let workload_ids = collect_unique_ids(
            "workload",
            self.workloads.iter().map(|workload| workload.id.as_str()),
        )?;
        let mode_ids = collect_unique_ids("mode", self.modes.iter().map(|mode| mode.id.as_str()))?;
        let hardware_class_ids = collect_unique_ids(
            "hardware class",
            self.hardware_classes.iter().map(|class| class.id.as_str()),
        )?;

        let mut placement_profiles_by_id = BTreeMap::new();
        for profile in &self.placement_profiles {
            if profile.id.trim().is_empty() {
                return Err("placement profile id must not be empty".to_owned());
            }
            if placement_profiles_by_id
                .insert(profile.id.clone(), profile)
                .is_some()
            {
                return Err(format!("duplicate placement profile `{}`", profile.id));
            }
            if !matches!(
                profile.kind.as_str(),
                "baseline" | "recommended" | "adversarial"
            ) {
                return Err(format!(
                    "placement profile `{}` has unsupported kind `{}`",
                    profile.id, profile.kind
                ));
            }
            for hardware_class_id in &profile.hardware_class_ids {
                if !hardware_class_ids.contains(hardware_class_id.as_str()) {
                    return Err(format!(
                        "placement profile `{}` references unknown hardware class `{hardware_class_id}`",
                        profile.id
                    ));
                }
            }
        }

        let mut scenario_keys = BTreeSet::new();
        for scenario in &self.scenarios {
            if !fixture_ids.contains(scenario.fixture_id.as_str()) {
                return Err(format!(
                    "scenario references unknown fixture `{}`",
                    scenario.fixture_id
                ));
            }
            if !workload_ids.contains(scenario.workload_id.as_str()) {
                return Err(format!(
                    "scenario references unknown workload `{}`",
                    scenario.workload_id
                ));
            }
            if scenario.concurrency == 0 {
                return Err(format!(
                    "scenario {}:{} must use concurrency >= 1",
                    scenario.fixture_id, scenario.workload_id
                ));
            }
            let scenario_key = format!(
                "{}:{}:c{}",
                scenario.fixture_id, scenario.workload_id, scenario.concurrency
            );
            if !scenario_keys.insert(scenario_key.clone()) {
                return Err(format!("duplicate scenario `{scenario_key}`"));
            }

            let mut has_baseline = false;
            let mut has_recommended = false;
            let mut has_adversarial = false;
            let scenario_mode_ids: BTreeSet<_> =
                scenario.mode_ids.iter().map(String::as_str).collect();

            for required_mode in ["sqlite3_wal", "fsqlite_mvcc", "fsqlite_single_writer"] {
                if !scenario_mode_ids.contains(required_mode) {
                    return Err(format!(
                        "scenario `{scenario_key}` is missing required mode `{required_mode}`"
                    ));
                }
            }

            for mode_id in &scenario.mode_ids {
                if !mode_ids.contains(mode_id.as_str()) {
                    return Err(format!(
                        "scenario `{scenario_key}` references unknown mode `{mode_id}`"
                    ));
                }
            }

            for placement_profile_id in &scenario.placement_profile_ids {
                let profile = placement_profiles_by_id.get(placement_profile_id).ok_or_else(|| {
                    format!(
                        "scenario `{scenario_key}` references unknown placement profile `{placement_profile_id}`"
                    )
                })?;
                match profile.kind.as_str() {
                    "baseline" => has_baseline = true,
                    "recommended" => has_recommended = true,
                    "adversarial" => has_adversarial = true,
                    _ => {}
                }
                if matches!(profile.kind.as_str(), "recommended" | "adversarial") {
                    let concurrency = scenario.concurrency.to_string();
                    if !profile
                        .taskset_cpu_list_by_concurrency
                        .contains_key(concurrency.as_str())
                    {
                        return Err(format!(
                            "placement profile `{}` is missing a taskset cpu list for concurrency {}",
                            profile.id, scenario.concurrency
                        ));
                    }
                }
            }

            if !has_baseline {
                return Err(format!(
                    "scenario `{scenario_key}` must include a baseline placement profile"
                ));
            }
            if !has_recommended {
                return Err(format!(
                    "scenario `{scenario_key}` must include a recommended placement profile"
                ));
            }
            if scenario.concurrency > 1 && !has_adversarial {
                return Err(format!(
                    "scenario `{scenario_key}` must include an adversarial placement profile"
                ));
            }
        }

        Ok(())
    }

    /// Compute the canonical Beads data hash from the configured source path.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be hashed.
    pub fn beads_data_hash(&self, workspace_root: &Path) -> Result<String, String> {
        let path = workspace_root.join(&self.beads_data_source.path);
        sha256_file(&path)
    }

    /// Expand every scenario into individual `(scenario × mode × placement)` run cells.
    ///
    /// # Errors
    ///
    /// Returns an error if the campaign fails validation.
    pub fn expand_run_cells(&self) -> Result<Vec<ExpandedCampaignRunCell>, String> {
        self.validate()?;
        let mut cells = Vec::new();
        for scenario in &self.scenarios {
            for mode_id in &scenario.mode_ids {
                for placement_profile_id in &scenario.placement_profile_ids {
                    cells.push(ExpandedCampaignRunCell {
                        fixture_id: scenario.fixture_id.clone(),
                        workload_id: scenario.workload_id.clone(),
                        concurrency: scenario.concurrency,
                        mode_id: mode_id.clone(),
                        placement_profile_id: placement_profile_id.clone(),
                    });
                }
            }
        }
        Ok(cells)
    }

    /// Render the stable artifact directory name for one placement profile.
    #[must_use]
    pub fn artifact_directory_name(
        &self,
        context: CampaignArtifactContext<'_>,
        placement_profile_id: &str,
    ) -> String {
        render_template(
            &self.artifact_naming.directory_template,
            &[
                ("{date}", context.date_yyyymmdd.to_owned()),
                (
                    "{commit_sha8}",
                    short_hash(context.commit_sha, 8).to_owned(),
                ),
                (
                    "{beads_hash12}",
                    short_hash(context.beads_data_hash, 12).to_owned(),
                ),
                ("{hardware_class_id}", context.hardware_class_id.to_owned()),
                ("{placement_profile_id}", placement_profile_id.to_owned()),
            ],
        )
    }

    /// Render the stable per-cell artifact stem (no extension).
    #[must_use]
    pub fn artifact_file_stem(&self, cell: &ExpandedCampaignRunCell) -> String {
        render_template(
            &self.artifact_naming.file_stem_template,
            &[
                ("{mode_id}", cell.mode_id.clone()),
                ("{fixture_id}", cell.fixture_id.clone()),
                ("{workload_id}", cell.workload_id.clone()),
                ("{concurrency}", cell.concurrency.to_string()),
                ("{build_profile}", self.build_profile.clone()),
            ],
        )
    }

    /// Build the stable artifact base path (directory + stem, no extension).
    #[must_use]
    pub fn artifact_base_path(
        &self,
        context: CampaignArtifactContext<'_>,
        cell: &ExpandedCampaignRunCell,
    ) -> PathBuf {
        PathBuf::from(&self.artifact_naming.output_root)
            .join(self.artifact_directory_name(context, &cell.placement_profile_id))
            .join(self.artifact_file_stem(cell))
    }

    /// Render the canonical benchmark ID for an expanded run cell.
    ///
    /// # Errors
    ///
    /// Returns an error if the referenced mode is unknown.
    pub fn benchmark_id(&self, cell: &ExpandedCampaignRunCell) -> Result<String, String> {
        let mode = self
            .modes
            .iter()
            .find(|mode| mode.id == cell.mode_id)
            .ok_or_else(|| format!("unknown mode `{}`", cell.mode_id))?;
        Ok(format!(
            "{}:{}:{}:c{}",
            mode.benchmark_engine_label, cell.workload_id, cell.fixture_id, cell.concurrency
        ))
    }
}

/// Load the canonical campaign from its default workspace-relative path.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or validated.
pub fn load_canonical_beads_benchmark_campaign(
    workspace_root: &Path,
) -> Result<CanonicalBeadsBenchmarkCampaign, String> {
    let path = workspace_root.join(CANONICAL_BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE);
    load_canonical_beads_benchmark_campaign_from(&path)
}

/// Load the canonical campaign from an explicit manifest path.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or validated.
pub fn load_canonical_beads_benchmark_campaign_from(
    path: &Path,
) -> Result<CanonicalBeadsBenchmarkCampaign, String> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "cannot read campaign manifest at {}: {error}",
            path.display()
        )
    })?;
    let campaign: CanonicalBeadsBenchmarkCampaign =
        serde_json::from_str(&content).map_err(|error| {
            format!(
                "cannot parse campaign manifest at {}: {error}",
                path.display()
            )
        })?;
    campaign.validate()?;
    Ok(campaign)
}

fn collect_unique_ids<'a>(
    kind: &str,
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<String>, String> {
    let mut unique = BTreeSet::new();
    for id in ids {
        if id.trim().is_empty() {
            return Err(format!("{kind} id must not be empty"));
        }
        if !unique.insert(id.to_owned()) {
            return Err(format!("duplicate {kind} `{id}`"));
        }
    }
    Ok(unique)
}

fn validate_retry_mode(kind: &str, mode: &RetryMode) -> Result<(), String> {
    if mode.max_busy_retries == 0 {
        return Err(format!("{kind} retry mode must allow at least one retry"));
    }
    if mode.backoff_base_ms == 0 {
        return Err(format!(
            "{kind} retry mode must use a non-zero base backoff"
        ));
    }
    if mode.backoff_cap_ms < mode.backoff_base_ms {
        return Err(format!(
            "{kind} retry mode backoff cap must be >= base backoff"
        ));
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("cannot read {} for sha256: {error}", path.display()))?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn render_template(template: &str, replacements: &[(&str, String)]) -> String {
    let mut rendered = template.to_owned();
    for (needle, value) in replacements {
        rendered = rendered.replace(needle, value);
    }
    rendered
}

fn short_hash(hash: &str, limit: usize) -> &str {
    &hash[..hash.len().min(limit)]
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use jsonschema::validator_for;
    use serde_json::Value;

    use super::{
        CANONICAL_BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE,
        CANONICAL_BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1, CampaignArtifactContext,
        load_canonical_beads_benchmark_campaign,
    };

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root should exist")
    }

    #[test]
    fn loads_campaign_and_expands_cells() {
        let root = workspace_root();
        let campaign =
            load_canonical_beads_benchmark_campaign(&root).expect("campaign should load");
        assert_eq!(
            campaign.schema_version,
            CANONICAL_BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1
        );
        let cells = campaign
            .expand_run_cells()
            .expect("campaign cells should expand");
        assert_eq!(cells.len(), 180);
        assert_eq!(
            campaign
                .benchmark_id(&cells[0])
                .expect("expanded cell should map to benchmark id"),
            "sqlite3:commutative_inserts_disjoint_keys:frankensqlite_beads:c1"
        );
    }

    #[test]
    fn campaign_manifest_matches_json_schema() {
        let root = workspace_root();
        let schema_path =
            root.join("sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.schema.json");
        let manifest_path = root.join(CANONICAL_BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE);

        let schema: Value = serde_json::from_str(
            &std::fs::read_to_string(&schema_path).expect("schema json should be readable"),
        )
        .expect("schema should parse");
        let manifest: Value = serde_json::from_str(
            &std::fs::read_to_string(&manifest_path).expect("manifest json should be readable"),
        )
        .expect("manifest should parse");

        let validator = validator_for(&schema).expect("schema should compile");
        let errors: Vec<String> = validator
            .iter_errors(&manifest)
            .map(|error| error.to_string())
            .collect();
        assert!(errors.is_empty(), "schema errors: {errors:#?}");
    }

    #[test]
    fn artifact_naming_carries_commit_and_beads_hash() {
        let root = workspace_root();
        let campaign =
            load_canonical_beads_benchmark_campaign(&root).expect("campaign should load");
        let cell = campaign
            .expand_run_cells()
            .expect("campaign cells should expand")
            .into_iter()
            .find(|cell| {
                cell.mode_id == "fsqlite_mvcc"
                    && cell.fixture_id == "frankensqlite_beads"
                    && cell.workload_id == "mixed_read_write"
                    && cell.concurrency == 8
                    && cell.placement_profile_id == "pinned_llc_spread_1t_per_core"
            })
            .expect("expected canonical run cell");

        let artifact_path = campaign.artifact_base_path(
            CampaignArtifactContext {
                date_yyyymmdd: "20260310",
                commit_sha: "0123456789abcdef",
                beads_data_hash: "abcdef0123456789fedcba9876543210abcdef0123456789fedcba9876543210",
                hardware_class_id: "amd_tr_pro_5995wx_64c128t_smt_on_l3x8_numa1",
            },
            &cell,
        );
        let rendered = artifact_path.to_string_lossy();

        assert!(rendered.contains("20260310_01234567_abcdef012345"));
        assert!(rendered.contains("amd_tr_pro_5995wx_64c128t_smt_on_l3x8_numa1"));
        assert!(rendered.contains("pinned_llc_spread_1t_per_core"));
        assert!(
            rendered
                .contains("fsqlite_mvcc__frankensqlite_beads__mixed_read_write__c8__release-perf")
        );
    }
}
