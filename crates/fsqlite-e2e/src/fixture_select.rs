//! Fixture selection UX: ergonomic, unambiguous fixture resolution.
//!
//! Bead: bd-jwuo
//!
//! Every fixture has a stable `db_id` (manifest-driven) used across CLI flags,
//! JSON reports, and TUI panels.  This module provides:
//!
//! - **Exact resolution**: `--db beads_rust_beads` selects exactly one fixture.
//! - **Prefix/substring matching**: `--db beads` matches all IDs containing "beads".
//! - **Ambiguity detection**: multiple matches → clear error listing all candidates.
//! - **Tag filtering**: `--tag wal`, `--tag large`.
//! - **Size range filtering**: `--min-size 1MB`, `--max-size 100MB`.
//! - **Feature filtering**: `--requires-wal`, `--header-ok`.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::oplog::preset_catalog;

// ── Manifest types ───────────────────────────────────────────────────

/// A single entry in the corpus manifest (`manifest.v1.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub db_id: String,
    pub golden_filename: String,
    pub sha256_golden: String,
    pub size_bytes: u64,
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub provenance: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub sqlite_meta: Option<ManifestSqliteMeta>,
}

/// SQLite PRAGMA metadata embedded in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSqliteMeta {
    #[serde(default)]
    pub page_size: Option<u32>,
    #[serde(default)]
    pub journal_mode: Option<String>,
    #[serde(default)]
    pub user_version: Option<u32>,
    #[serde(default)]
    pub application_id: Option<u32>,
}

/// Top-level manifest file structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_version: u32,
    pub entries: Vec<ManifestEntry>,
}

/// Stable schema identifier for the canonical Beads benchmark campaign.
pub const BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1: &str = "fsqlite-e2e.beads_benchmark_campaign.v1";

/// Canonical Track A benchmark campaign manifest path.
pub const BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE: &str =
    "sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json";

/// Stable baseline placement profile id for portable scheduler-default runs.
pub const PLACEMENT_PROFILE_BASELINE_UNPINNED: &str = "baseline_unpinned";
/// Stable recommended placement profile id for topology-aware pinned runs.
pub const PLACEMENT_PROFILE_RECOMMENDED_PINNED: &str = "recommended_pinned";
/// Stable adversarial placement profile id for cross-node stress runs.
pub const PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE: &str = "adversarial_cross_node";
/// Canonical placement-profile ids required across Track A/G reports.
pub const REQUIRED_PLACEMENT_PROFILE_IDS: [&str; 3] = [
    PLACEMENT_PROFILE_BASELINE_UNPINNED,
    PLACEMENT_PROFILE_RECOMMENDED_PINNED,
    PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE,
];

const BUNDLE_DIR_ROW_ID_PLACEHOLDER: &str = "{row_id}";
const BUNDLE_DIR_FIXTURE_ID_PLACEHOLDER: &str = "{fixture_id}";
const BUNDLE_DIR_MODE_PLACEHOLDER: &str = "{mode}";
const BUNDLE_DIR_PLACEMENT_PROFILE_ID_PLACEHOLDER: &str = "{placement_profile_id}";
const BUNDLE_DIR_SOURCE_REVISION_PLACEHOLDER: &str = "{source_revision}";
const BUNDLE_DIR_BEADS_HASH_PLACEHOLDER: &str = "{beads_hash}";

/// Stable hardware-class id for portable x86_64 Linux baseline hosts.
pub const HARDWARE_CLASS_LINUX_X86_64_ANY: &str = "linux_x86_64_any";
/// Stable hardware-class id for many-core NUMA-aware x86_64 Linux hosts.
pub const HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA: &str = "linux_x86_64_many_core_numa";
/// Canonical hardware-class ids required across Track A/G reports.
pub const REQUIRED_HARDWARE_CLASS_IDS: [&str; 2] = [
    HARDWARE_CLASS_LINUX_X86_64_ANY,
    HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA,
];

/// Which execution mode a canonical benchmark cell uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkMode {
    SqliteReference,
    FsqliteMvcc,
    FsqliteSingleWriter,
}

impl BenchmarkMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SqliteReference => "sqlite_reference",
            Self::FsqliteMvcc => "fsqlite_mvcc",
            Self::FsqliteSingleWriter => "fsqlite_single_writer",
        }
    }
}

/// One pinned Beads fixture used by the many-core benchmark campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeadsBenchmarkFixture {
    pub fixture_id: String,
    pub source_path: String,
    pub source_sha256: String,
    pub source_size_bytes: u64,
    pub working_copy_relpath: String,
    pub working_copy_sha256: String,
    pub working_copy_size_bytes: u64,
    pub page_size: u32,
    pub journal_mode: String,
    pub capture_rule: String,
}

/// Placement taxonomy family used across the canonical many-core reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementProfileKind {
    Baseline,
    RecommendedPinned,
    AdversarialTopology,
}

/// Whether the placement profile is portable or requires topology awareness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementAvailability {
    Universal,
    TopologyAware,
}

/// Placement vocabulary for the canonical matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementProfile {
    pub id: String,
    pub kind: PlacementProfileKind,
    pub description: String,
    pub command_hint: String,
    pub availability: PlacementAvailability,
}

/// Operating-system family encoded in a hardware-class identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareOsFamily {
    Linux,
}

impl HardwareOsFamily {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
        }
    }
}

/// CPU architecture encoded in a hardware-class identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareCpuArchitecture {
    X86_64,
}

impl HardwareCpuArchitecture {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
        }
    }
}

/// Host-topology tier encoded in a hardware-class identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareTopologyClass {
    Any,
    ManyCoreNuma,
}

impl HardwareTopologyClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::ManyCoreNuma => "many_core_numa",
        }
    }
}

/// Explicit identifier components so reports do not need to parse free-form ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareClassIdFields {
    pub os_family: HardwareOsFamily,
    pub cpu_arch: HardwareCpuArchitecture,
    pub topology_class: HardwareTopologyClass,
}

impl HardwareClassIdFields {
    #[must_use]
    pub fn canonical_id(&self) -> String {
        format!(
            "{}_{}_{}",
            self.os_family.as_str(),
            self.cpu_arch.as_str(),
            self.topology_class.as_str()
        )
    }
}

/// Hardware taxonomy attached to canonical matrix rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareClass {
    pub id: String,
    pub id_fields: HardwareClassIdFields,
    pub min_logical_cores: u32,
    pub min_numa_nodes: Option<u32>,
    pub description: String,
}

/// Busy-retry policy pinned for benchmark comparisons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub id: String,
    pub max_busy_retries: u32,
    pub busy_backoff_ms: u64,
    pub busy_backoff_max_ms: u64,
    pub notes: String,
}

/// Cargo build profile pinned for benchmark runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildProfile {
    pub id: String,
    pub cargo_profile: String,
    pub cargo_args: Vec<String>,
    pub notes: String,
}

/// Deterministic seed policy pinned for benchmark runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedPolicy {
    pub id: String,
    pub kind: String,
    pub base_seed: u64,
    pub notes: String,
}

/// One placement/hardware variant required for a benchmark row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementVariant {
    pub placement_profile_id: String,
    pub hardware_class_id: String,
    pub required: bool,
}

/// One workload/concurrency row in the canonical benchmark matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeadsBenchmarkMatrixRow {
    pub row_id: String,
    pub fixtures: Vec<String>,
    pub workload: String,
    pub concurrency: u16,
    pub modes: Vec<BenchmarkMode>,
    pub placement_variants: Vec<PlacementVariant>,
    pub retry_policy_id: String,
    pub build_profile_id: String,
    pub seed_policy_id: String,
}

/// Stable artifact naming contract for one expanded benchmark cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkArtifactContract {
    pub artifact_root_relpath: String,
    pub bundle_dir_template: String,
    pub result_jsonl_name: String,
    pub summary_md_name: String,
    pub manifest_name: String,
    pub logs_dir_name: String,
    pub profiles_dir_name: String,
}

/// Checked-in Track A campaign manifest describing the canonical benchmark matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeadsBenchmarkCampaign {
    pub schema_version: String,
    pub campaign_id: String,
    pub title: String,
    pub working_benchmark_root_relpath: String,
    pub beads_data_relpath: String,
    pub fixtures: Vec<BeadsBenchmarkFixture>,
    pub placement_profiles: Vec<PlacementProfile>,
    pub hardware_classes: Vec<HardwareClass>,
    pub retry_policies: Vec<RetryPolicy>,
    pub build_profiles: Vec<BuildProfile>,
    pub seed_policies: Vec<SeedPolicy>,
    pub matrix_rows: Vec<BeadsBenchmarkMatrixRow>,
    pub artifact_contract: BenchmarkArtifactContract,
}

/// One fully expanded benchmark cell (fixture × mode × placement).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpandedBenchmarkCell {
    pub row_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub concurrency: u16,
    pub mode: BenchmarkMode,
    pub placement_profile_id: String,
    pub hardware_class_id: String,
    pub retry_policy_id: String,
    pub build_profile_id: String,
    pub seed_policy_id: String,
}

/// Load the canonical Beads benchmark campaign manifest from the workspace root.
///
/// # Errors
///
/// Returns an error if the manifest cannot be read or parsed.
pub fn load_beads_benchmark_campaign(
    workspace_root: &Path,
) -> Result<BeadsBenchmarkCampaign, String> {
    let path = workspace_root.join(BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE);
    load_beads_benchmark_campaign_from(&path)
}

/// Load the canonical Beads benchmark campaign manifest from an explicit path.
///
/// # Errors
///
/// Returns an error if the manifest cannot be read or parsed.
pub fn load_beads_benchmark_campaign_from(path: &Path) -> Result<BeadsBenchmarkCampaign, String> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        format!(
            "cannot read Beads benchmark campaign manifest at {}: {e}",
            path.display()
        )
    })?;
    serde_json::from_str::<BeadsBenchmarkCampaign>(&content).map_err(|e| {
        format!(
            "cannot parse Beads benchmark campaign manifest at {}: {e}",
            path.display()
        )
    })
}

/// Expand the campaign into fully concrete benchmark cells.
#[must_use]
pub fn expand_beads_benchmark_campaign(
    campaign: &BeadsBenchmarkCampaign,
) -> Vec<ExpandedBenchmarkCell> {
    let mut cells = Vec::new();
    for row in &campaign.matrix_rows {
        for fixture_id in &row.fixtures {
            for &mode in &row.modes {
                for placement in &row.placement_variants {
                    cells.push(ExpandedBenchmarkCell {
                        row_id: row.row_id.clone(),
                        fixture_id: fixture_id.clone(),
                        workload: row.workload.clone(),
                        concurrency: row.concurrency,
                        mode,
                        placement_profile_id: placement.placement_profile_id.clone(),
                        hardware_class_id: placement.hardware_class_id.clone(),
                        retry_policy_id: row.retry_policy_id.clone(),
                        build_profile_id: row.build_profile_id.clone(),
                        seed_policy_id: row.seed_policy_id.clone(),
                    });
                }
            }
        }
    }
    cells
}

/// Render the stable artifact bundle directory name for one expanded cell.
#[must_use]
pub fn render_benchmark_bundle_dir(
    contract: &BenchmarkArtifactContract,
    cell: &ExpandedBenchmarkCell,
    source_revision: &str,
    beads_hash: &str,
) -> String {
    contract
        .bundle_dir_template
        .replace(BUNDLE_DIR_ROW_ID_PLACEHOLDER, &cell.row_id)
        .replace(BUNDLE_DIR_FIXTURE_ID_PLACEHOLDER, &cell.fixture_id)
        .replace(BUNDLE_DIR_MODE_PLACEHOLDER, cell.mode.as_str())
        .replace(
            BUNDLE_DIR_PLACEMENT_PROFILE_ID_PLACEHOLDER,
            &cell.placement_profile_id,
        )
        .replace(
            BUNDLE_DIR_SOURCE_REVISION_PLACEHOLDER,
            &short_hash(source_revision),
        )
        .replace(BUNDLE_DIR_BEADS_HASH_PLACEHOLDER, &short_hash(beads_hash))
}

/// Materialize the artifact bundle path for one expanded cell.
#[must_use]
pub fn benchmark_bundle_path(
    workspace_root: &Path,
    campaign: &BeadsBenchmarkCampaign,
    cell: &ExpandedBenchmarkCell,
    source_revision: &str,
    beads_hash: &str,
) -> PathBuf {
    workspace_root
        .join(&campaign.artifact_contract.artifact_root_relpath)
        .join(render_benchmark_bundle_dir(
            &campaign.artifact_contract,
            cell,
            source_revision,
            beads_hash,
        ))
}

/// Validate the canonical benchmark campaign manifest for internal consistency.
///
/// This validates checked-in benchmark working copies, symbolic references, and
/// the row/placement/mode contract without requiring the external source paths
/// to exist on every machine.
///
/// # Errors
///
/// Returns a newline-delimited list of validation failures.
pub fn validate_beads_benchmark_campaign(
    campaign: &BeadsBenchmarkCampaign,
    workspace_root: &Path,
) -> Result<(), String> {
    let mut errors = Vec::new();
    if campaign.schema_version != BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1 {
        errors.push(format!(
            "unexpected schema_version {:?} (expected {:?})",
            campaign.schema_version, BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1
        ));
    }

    let workload_names: BTreeSet<String> =
        preset_catalog().into_iter().map(|meta| meta.name).collect();
    let fixture_ids = unique_ids(
        campaign
            .fixtures
            .iter()
            .map(|fixture| fixture.fixture_id.as_str()),
        "fixture_id",
        &mut errors,
    );
    let placement_ids = unique_ids(
        campaign
            .placement_profiles
            .iter()
            .map(|profile| profile.id.as_str()),
        "placement_profile_id",
        &mut errors,
    );
    let hardware_ids = unique_ids(
        campaign
            .hardware_classes
            .iter()
            .map(|hardware| hardware.id.as_str()),
        "hardware_class_id",
        &mut errors,
    );
    let retry_ids = unique_ids(
        campaign
            .retry_policies
            .iter()
            .map(|policy| policy.id.as_str()),
        "retry_policy_id",
        &mut errors,
    );
    let build_ids = unique_ids(
        campaign
            .build_profiles
            .iter()
            .map(|profile| profile.id.as_str()),
        "build_profile_id",
        &mut errors,
    );
    let seed_ids = unique_ids(
        campaign
            .seed_policies
            .iter()
            .map(|policy| policy.id.as_str()),
        "seed_policy_id",
        &mut errors,
    );
    let mut source_paths = BTreeSet::new();
    let mut working_copy_paths = BTreeSet::new();

    for profile in &campaign.placement_profiles {
        match profile.id.as_str() {
            PLACEMENT_PROFILE_BASELINE_UNPINNED => {
                if profile.kind != PlacementProfileKind::Baseline {
                    errors.push(format!(
                        "placement profile {} must use kind baseline",
                        profile.id
                    ));
                }
                if profile.availability != PlacementAvailability::Universal {
                    errors.push(format!(
                        "placement profile {} must use universal availability",
                        profile.id
                    ));
                }
            }
            PLACEMENT_PROFILE_RECOMMENDED_PINNED => {
                if profile.kind != PlacementProfileKind::RecommendedPinned {
                    errors.push(format!(
                        "placement profile {} must use kind recommended_pinned",
                        profile.id
                    ));
                }
                if profile.availability != PlacementAvailability::TopologyAware {
                    errors.push(format!(
                        "placement profile {} must use topology_aware availability",
                        profile.id
                    ));
                }
            }
            PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE => {
                if profile.kind != PlacementProfileKind::AdversarialTopology {
                    errors.push(format!(
                        "placement profile {} must use kind adversarial_topology",
                        profile.id
                    ));
                }
                if profile.availability != PlacementAvailability::TopologyAware {
                    errors.push(format!(
                        "placement profile {} must use topology_aware availability",
                        profile.id
                    ));
                }
            }
            _ => {}
        }
    }
    for required_profile_id in REQUIRED_PLACEMENT_PROFILE_IDS {
        if !placement_ids.contains(required_profile_id) {
            errors.push(format!(
                "campaign must define required placement profile {:?}",
                required_profile_id
            ));
        }
    }

    for hardware in &campaign.hardware_classes {
        let canonical_id = hardware.id_fields.canonical_id();
        if hardware.id != canonical_id {
            errors.push(format!(
                "hardware class {} must match identifier fields {}",
                hardware.id, canonical_id
            ));
        }

        let expected_fields = match hardware.id.as_str() {
            HARDWARE_CLASS_LINUX_X86_64_ANY => Some(HardwareClassIdFields {
                os_family: HardwareOsFamily::Linux,
                cpu_arch: HardwareCpuArchitecture::X86_64,
                topology_class: HardwareTopologyClass::Any,
            }),
            HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA => Some(HardwareClassIdFields {
                os_family: HardwareOsFamily::Linux,
                cpu_arch: HardwareCpuArchitecture::X86_64,
                topology_class: HardwareTopologyClass::ManyCoreNuma,
            }),
            _ => None,
        };
        if let Some(expected_fields) = expected_fields
            && hardware.id_fields != expected_fields
        {
            errors.push(format!(
                "hardware class {} must use canonical identifier fields {:?}",
                hardware.id, expected_fields
            ));
        }
    }
    for required_hardware_id in REQUIRED_HARDWARE_CLASS_IDS {
        if !hardware_ids.contains(required_hardware_id) {
            errors.push(format!(
                "campaign must define required hardware class {:?}",
                required_hardware_id
            ));
        }
    }

    for fixture in &campaign.fixtures {
        if fixture.capture_rule.trim().is_empty() {
            errors.push(format!(
                "fixture {} capture_rule must not be empty",
                fixture.fixture_id
            ));
        }
        if !Path::new(&fixture.source_path).is_absolute() {
            errors.push(format!(
                "fixture {} source_path must be absolute: {}",
                fixture.fixture_id, fixture.source_path
            ));
        }
        if !is_sha256_hex_64(&fixture.source_sha256) {
            errors.push(format!(
                "fixture {} source_sha256 must be 64 lowercase hex chars",
                fixture.fixture_id
            ));
        }
        if !is_sha256_hex_64(&fixture.working_copy_sha256) {
            errors.push(format!(
                "fixture {} working_copy_sha256 must be 64 lowercase hex chars",
                fixture.fixture_id
            ));
        }
        if !source_paths.insert(fixture.source_path.as_str()) {
            errors.push(format!(
                "fixture source_path must be unique: {}",
                fixture.source_path
            ));
        }
        if !working_copy_paths.insert(fixture.working_copy_relpath.as_str()) {
            errors.push(format!(
                "fixture working_copy_relpath must be unique: {}",
                fixture.working_copy_relpath
            ));
        }
        if !Path::new(&fixture.working_copy_relpath)
            .starts_with(Path::new(&campaign.working_benchmark_root_relpath))
        {
            errors.push(format!(
                "fixture {} working copy must stay under campaign root: {}",
                fixture.fixture_id, fixture.working_copy_relpath
            ));
        }
        let working_copy = workspace_root.join(&fixture.working_copy_relpath);
        if !working_copy.is_file() {
            errors.push(format!(
                "fixture {} working copy missing: {}",
                fixture.fixture_id,
                working_copy.display()
            ));
            continue;
        }
        match std::fs::metadata(&working_copy) {
            Ok(metadata) if metadata.len() != fixture.working_copy_size_bytes => {
                errors.push(format!(
                    "fixture {} working copy size mismatch: manifest={} actual={} ({})",
                    fixture.fixture_id,
                    fixture.working_copy_size_bytes,
                    metadata.len(),
                    working_copy.display()
                ));
            }
            Ok(_) => {}
            Err(e) => errors.push(format!(
                "fixture {} cannot stat working copy {}: {e}",
                fixture.fixture_id,
                working_copy.display()
            )),
        }
        match sha256_hex_file(&working_copy) {
            Ok(actual) if actual != fixture.working_copy_sha256 => errors.push(format!(
                "fixture {} working copy sha256 mismatch: manifest={} actual={}",
                fixture.fixture_id, fixture.working_copy_sha256, actual
            )),
            Ok(_) => {}
            Err(e) => errors.push(format!(
                "fixture {} cannot hash working copy {}: {e}",
                fixture.fixture_id,
                working_copy.display()
            )),
        }
    }

    let row_ids = unique_ids(
        campaign.matrix_rows.iter().map(|row| row.row_id.as_str()),
        "row_id",
        &mut errors,
    );
    if row_ids.is_empty() {
        errors.push("campaign must define at least one matrix row".to_owned());
    }

    for row in &campaign.matrix_rows {
        if row.concurrency == 0 {
            errors.push(format!("row {} must use concurrency >= 1", row.row_id));
        }
        if !workload_names.contains(&row.workload) {
            errors.push(format!(
                "row {} references unknown workload {:?}",
                row.row_id, row.workload
            ));
        }
        if row.fixtures.is_empty() {
            errors.push(format!(
                "row {} must reference at least one fixture",
                row.row_id
            ));
        }
        for fixture_id in &row.fixtures {
            if !fixture_ids.contains(fixture_id.as_str()) {
                errors.push(format!(
                    "row {} references unknown fixture {:?}",
                    row.row_id, fixture_id
                ));
            }
        }

        let contains_mode = |needle: BenchmarkMode| row.modes.contains(&needle);
        if !contains_mode(BenchmarkMode::SqliteReference)
            || !contains_mode(BenchmarkMode::FsqliteMvcc)
            || !contains_mode(BenchmarkMode::FsqliteSingleWriter)
        {
            errors.push(format!(
                "row {} must include sqlite_reference, fsqlite_mvcc, and fsqlite_single_writer modes",
                row.row_id
            ));
        }

        let has_baseline = row
            .placement_variants
            .iter()
            .any(|variant| variant.placement_profile_id == PLACEMENT_PROFILE_BASELINE_UNPINNED);
        let has_recommended = row
            .placement_variants
            .iter()
            .any(|variant| variant.placement_profile_id == PLACEMENT_PROFILE_RECOMMENDED_PINNED);
        let has_adversarial = row.placement_variants.iter().any(|variant| {
            variant.placement_profile_id == PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE
        });

        if !has_baseline {
            errors.push(format!(
                "row {} must include the {} placement profile",
                row.row_id, PLACEMENT_PROFILE_BASELINE_UNPINNED
            ));
        }
        if !has_recommended {
            errors.push(format!(
                "row {} must include the {} placement profile",
                row.row_id, PLACEMENT_PROFILE_RECOMMENDED_PINNED
            ));
        }
        if row.concurrency > 1 && !has_adversarial {
            errors.push(format!(
                "row {} must include {} for concurrency > 1",
                row.row_id, PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE
            ));
        }

        for variant in &row.placement_variants {
            if !placement_ids.contains(variant.placement_profile_id.as_str()) {
                errors.push(format!(
                    "row {} references unknown placement profile {:?}",
                    row.row_id, variant.placement_profile_id
                ));
            }
            if !hardware_ids.contains(variant.hardware_class_id.as_str()) {
                errors.push(format!(
                    "row {} references unknown hardware class {:?}",
                    row.row_id, variant.hardware_class_id
                ));
            }
        }
        if !retry_ids.contains(row.retry_policy_id.as_str()) {
            errors.push(format!(
                "row {} references unknown retry policy {:?}",
                row.row_id, row.retry_policy_id
            ));
        }
        if !build_ids.contains(row.build_profile_id.as_str()) {
            errors.push(format!(
                "row {} references unknown build profile {:?}",
                row.row_id, row.build_profile_id
            ));
        }
        if !seed_ids.contains(row.seed_policy_id.as_str()) {
            errors.push(format!(
                "row {} references unknown seed policy {:?}",
                row.row_id, row.seed_policy_id
            ));
        }
    }

    for placeholder in [
        BUNDLE_DIR_ROW_ID_PLACEHOLDER,
        BUNDLE_DIR_FIXTURE_ID_PLACEHOLDER,
        BUNDLE_DIR_MODE_PLACEHOLDER,
        BUNDLE_DIR_PLACEMENT_PROFILE_ID_PLACEHOLDER,
        BUNDLE_DIR_SOURCE_REVISION_PLACEHOLDER,
        BUNDLE_DIR_BEADS_HASH_PLACEHOLDER,
    ] {
        if !campaign
            .artifact_contract
            .bundle_dir_template
            .contains(placeholder)
        {
            errors.push(format!(
                "artifact bundle_dir_template must contain placeholder {placeholder}"
            ));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn unique_ids<'a>(
    ids: impl Iterator<Item = &'a str>,
    label: &str,
    errors: &mut Vec<String>,
) -> BTreeSet<&'a str> {
    let mut ids_set = BTreeSet::new();
    for id in ids {
        if id.trim().is_empty() {
            errors.push(format!("{label} must not be empty"));
            continue;
        }
        if !ids_set.insert(id) {
            errors.push(format!("duplicate {label}: {id}"));
        }
    }
    ids_set
}

fn sha256_hex_file(path: &Path) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn is_sha256_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn short_hash(value: &str) -> String {
    value.chars().take(12).collect()
}

// ── Filter criteria ──────────────────────────────────────────────────

/// Criteria for filtering fixtures.
#[derive(Debug, Clone, Default)]
pub struct FixtureFilter {
    /// Only include fixtures whose `db_id` matches this selector.
    ///
    /// Matching rules (in priority order):
    /// 1. Exact match on `db_id`.
    /// 2. If no exact match, substring match on `db_id`.
    pub db_selector: Option<String>,

    /// Only include fixtures tagged with ALL of these tags.
    pub require_tags: Vec<String>,

    /// Exclude fixtures tagged with ANY of these tags.
    pub exclude_tags: Vec<String>,

    /// Minimum file size in bytes (inclusive).
    pub min_size_bytes: Option<u64>,

    /// Maximum file size in bytes (inclusive).
    pub max_size_bytes: Option<u64>,

    /// Only include fixtures with WAL journal mode.
    pub requires_wal: bool,

    /// Only include fixtures marked safe for CI.
    pub ci_safe_only: bool,
}

// ── Selection result ─────────────────────────────────────────────────

/// Outcome of fixture selection.
#[derive(Debug, Clone)]
pub enum SelectionResult {
    /// Exactly one fixture matched.
    Single(ManifestEntry),
    /// Multiple fixtures matched — caller must disambiguate.
    Ambiguous {
        selector: String,
        candidates: Vec<ManifestEntry>,
    },
    /// No fixtures matched the filter.
    NoMatch { reason: String },
}

impl SelectionResult {
    /// Returns `Ok` if exactly one fixture was selected, otherwise an error
    /// message suitable for display.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error string for ambiguous or no-match results.
    pub fn require_single(self) -> Result<ManifestEntry, String> {
        match self {
            Self::Single(entry) => Ok(entry),
            Self::Ambiguous {
                selector,
                candidates,
            } => {
                let mut msg = format!(
                    "ambiguous fixture selector `{selector}` matches {} fixtures:\n",
                    candidates.len()
                );
                for c in &candidates {
                    let _ = writeln!(
                        msg,
                        "  - {} ({}, {})",
                        c.db_id,
                        c.golden_filename,
                        format_size(c.size_bytes)
                    );
                }
                let _ = writeln!(msg, "\nPlease use a more specific --db value.");
                Err(msg)
            }
            Self::NoMatch { reason } => Err(format!("no fixture matched: {reason}")),
        }
    }

    /// Whether exactly one fixture was selected.
    #[must_use]
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Single(_))
    }
}

// ── Manifest loading ─────────────────────────────────────────────────

/// Default path to the manifest file relative to the workspace root.
pub const MANIFEST_PATH_RELATIVE: &str = "sample_sqlite_db_files/manifests/manifest.v1.json";

/// Load the manifest from the default path.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_manifest(workspace_root: &Path) -> Result<Manifest, String> {
    let path = workspace_root.join(MANIFEST_PATH_RELATIVE);
    load_manifest_from(&path)
}

/// Load a manifest from an explicit path.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_manifest_from(path: &Path) -> Result<Manifest, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read manifest at {}: {e}", path.display()))?;
    serde_json::from_str::<Manifest>(&content)
        .map_err(|e| format!("cannot parse manifest at {}: {e}", path.display()))
}

// ── Core selection logic ─────────────────────────────────────────────

/// Select fixtures matching the given filter.
///
/// If `filter.db_selector` is set:
/// 1. Try exact match first.
/// 2. If no exact match, try substring match.
/// 3. Apply remaining filters to the candidates.
///
/// If `filter.db_selector` is `None`, all entries pass the ID filter
/// and only tag/size/feature filters apply.
#[must_use]
pub fn select(manifest: &Manifest, filter: &FixtureFilter) -> SelectionResult {
    let id_candidates = match &filter.db_selector {
        Some(selector) => {
            // Step 1: exact match.
            let exact: Vec<&ManifestEntry> = manifest
                .entries
                .iter()
                .filter(|e| e.db_id == *selector)
                .collect();
            if exact.is_empty() {
                // Step 2: substring match.
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.db_id.contains(selector.as_str()))
                    .collect()
            } else {
                exact
            }
        }
        None => manifest.entries.iter().collect(),
    };

    // Apply secondary filters.
    let filtered: Vec<ManifestEntry> = id_candidates
        .into_iter()
        .filter(|e| passes_secondary_filters(e, filter))
        .cloned()
        .collect();

    match filtered.len() {
        0 => SelectionResult::NoMatch {
            reason: describe_filter(filter),
        },
        1 => SelectionResult::Single(filtered.into_iter().next().expect("len == 1")),
        _ => {
            if let Some(sel) = &filter.db_selector {
                SelectionResult::Ambiguous {
                    selector: sel.clone(),
                    candidates: filtered,
                }
            } else {
                // No db_selector → return all matches as "ambiguous" so caller
                // can iterate.
                SelectionResult::Ambiguous {
                    selector: "(all)".to_owned(),
                    candidates: filtered,
                }
            }
        }
    }
}

/// Select all fixtures matching the given filter (returns a vec, never fails).
#[must_use]
pub fn select_all(manifest: &Manifest, filter: &FixtureFilter) -> Vec<ManifestEntry> {
    let id_candidates: Vec<&ManifestEntry> = match &filter.db_selector {
        Some(selector) => {
            let exact: Vec<&ManifestEntry> = manifest
                .entries
                .iter()
                .filter(|e| e.db_id == *selector)
                .collect();
            if exact.is_empty() {
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.db_id.contains(selector.as_str()))
                    .collect()
            } else {
                exact
            }
        }
        None => manifest.entries.iter().collect(),
    };

    id_candidates
        .into_iter()
        .filter(|e| passes_secondary_filters(e, filter))
        .cloned()
        .collect()
}

fn passes_secondary_filters(entry: &ManifestEntry, filter: &FixtureFilter) -> bool {
    // Tag inclusion.
    if !filter.require_tags.is_empty()
        && !filter
            .require_tags
            .iter()
            .all(|t| entry.tags.iter().any(|et| et == t))
    {
        return false;
    }

    // Tag exclusion.
    if filter
        .exclude_tags
        .iter()
        .any(|t| entry.tags.iter().any(|et| et == t))
    {
        return false;
    }

    // Size range.
    if let Some(min) = filter.min_size_bytes {
        if entry.size_bytes < min {
            return false;
        }
    }
    if let Some(max) = filter.max_size_bytes {
        if entry.size_bytes > max {
            return false;
        }
    }

    // WAL requirement.
    if filter.requires_wal {
        let is_wal = entry
            .sqlite_meta
            .as_ref()
            .and_then(|m| m.journal_mode.as_deref())
            .is_some_and(|jm| jm.eq_ignore_ascii_case("wal"));
        if !is_wal {
            return false;
        }
    }

    true
}

fn describe_filter(filter: &FixtureFilter) -> String {
    let mut parts = Vec::new();
    if let Some(sel) = &filter.db_selector {
        parts.push(format!("db_id contains \"{sel}\""));
    }
    for tag in &filter.require_tags {
        parts.push(format!("tag={tag}"));
    }
    for tag in &filter.exclude_tags {
        parts.push(format!("exclude tag={tag}"));
    }
    if let Some(min) = filter.min_size_bytes {
        parts.push(format!("size >= {}", format_size(min)));
    }
    if let Some(max) = filter.max_size_bytes {
        parts.push(format!("size <= {}", format_size(max)));
    }
    if filter.requires_wal {
        parts.push("journal_mode=wal".to_owned());
    }
    if parts.is_empty() {
        "no entries in manifest".to_owned()
    } else {
        parts.join(", ")
    }
}

// ── CLI argument parsing ─────────────────────────────────────────────

/// Parse fixture-selection flags from a CLI argument list.
///
/// Recognized flags:
/// - `--db <ID>` — fixture selector (exact or substring).
/// - `--tag <TAG>` — require this tag (repeatable).
/// - `--exclude-tag <TAG>` — exclude this tag (repeatable).
/// - `--min-size <BYTES>` — minimum size (supports K/M/G suffixes).
/// - `--max-size <BYTES>` — maximum size (supports K/M/G suffixes).
/// - `--requires-wal` — only WAL-mode fixtures.
/// - `--ci-safe` — only CI-safe fixtures.
#[must_use]
pub fn parse_filter_args(args: &[String]) -> FixtureFilter {
    let mut filter = FixtureFilter::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--db" => {
                if i + 1 < args.len() {
                    filter.db_selector = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--tag" => {
                if i + 1 < args.len() {
                    filter.require_tags.push(args[i + 1].clone());
                    i += 1;
                }
            }
            "--exclude-tag" => {
                if i + 1 < args.len() {
                    filter.exclude_tags.push(args[i + 1].clone());
                    i += 1;
                }
            }
            "--min-size" => {
                if i + 1 < args.len() {
                    if let Some(bytes) = parse_size(&args[i + 1]) {
                        filter.min_size_bytes = Some(bytes);
                    }
                    i += 1;
                }
            }
            "--max-size" => {
                if i + 1 < args.len() {
                    if let Some(bytes) = parse_size(&args[i + 1]) {
                        filter.max_size_bytes = Some(bytes);
                    }
                    i += 1;
                }
            }
            "--requires-wal" => {
                filter.requires_wal = true;
            }
            "--ci-safe" => {
                filter.ci_safe_only = true;
            }
            _ => {}
        }
        i += 1;
    }
    filter
}

/// Help text for fixture selection flags.
#[must_use]
pub fn fixture_selection_help() -> &'static str {
    "\
FIXTURE SELECTION:
    --db <ID>              Select fixture by db_id (exact or substring match)
    --tag <TAG>            Require this tag (repeatable: --tag wal --tag large)
    --exclude-tag <TAG>    Exclude fixtures with this tag
    --min-size <SIZE>      Minimum file size (e.g., 1M, 500K, 1G)
    --max-size <SIZE>      Maximum file size
    --requires-wal         Only select WAL-mode fixtures
    --ci-safe              Only select CI-safe fixtures"
}

// ── Size parsing / formatting ────────────────────────────────────────

/// Parse a human-readable size string with optional suffix.
///
/// Supports: plain bytes, `K`/`KB`, `M`/`MB`, `G`/`GB` (case-insensitive).
#[must_use]
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Split numeric prefix from suffix.
    let (num_str, suffix) = split_numeric_suffix(s);
    let num: f64 = num_str.parse().ok()?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1u64,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Some((num * multiplier as f64) as u64)
}

fn split_numeric_suffix(s: &str) -> (&str, &str) {
    let boundary = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    (&s[..boundary], &s[boundary..])
}

/// Format bytes as a human-readable size string.
#[must_use]
pub fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Resolve a `db_id` to the golden file path.
#[must_use]
pub fn resolve_golden_path(workspace_root: &Path, entry: &ManifestEntry) -> PathBuf {
    workspace_root
        .join("sample_sqlite_db_files")
        .join("golden")
        .join(&entry.golden_filename)
}

/// Resolve a `db_id` to the metadata JSON path.
#[must_use]
pub fn resolve_metadata_path(workspace_root: &Path, entry: &ManifestEntry) -> PathBuf {
    workspace_root
        .join("sample_sqlite_db_files")
        .join("metadata")
        .join(format!("{}.json", entry.db_id))
}

// ── List/display helpers ─────────────────────────────────────────────

/// Format a list of entries as a human-readable table.
#[must_use]
pub fn format_fixture_table(entries: &[ManifestEntry]) -> String {
    let mut out = String::with_capacity(entries.len() * 80);
    let _ = writeln!(out, "{:<35} {:>10} {:>6} tags", "db_id", "size", "pages");
    let _ = writeln!(out, "{}", "-".repeat(80));
    for e in entries {
        let pages = e
            .sqlite_meta
            .as_ref()
            .and_then(|m| m.page_size)
            .map_or_else(
                || "?".to_owned(),
                |ps| {
                    if ps > 0 {
                        format!("{}", e.size_bytes / u64::from(ps))
                    } else {
                        "?".to_owned()
                    }
                },
            );
        let tags = if e.tags.is_empty() {
            "-".to_owned()
        } else {
            e.tags.join(", ")
        };
        let _ = writeln!(
            out,
            "{:<35} {:>10} {:>6} {}",
            e.db_id,
            format_size(e.size_bytes),
            pages,
            tags
        );
    }
    out
}

// ── Tag synchronization ──────────────────────────────────────────────

/// Metadata path relative to the workspace root.
const METADATA_DIR_RELATIVE: &str = "sample_sqlite_db_files/metadata";

/// Sync tags from per-fixture metadata JSON files into the manifest.
///
/// For each manifest entry, reads `<metadata_dir>/<db_id>.json` and
/// copies its `tags` array into the entry.  Entries without a matching
/// metadata file keep their existing tags (or an empty vec).
///
/// Returns the number of entries that had their tags updated.
///
/// # Errors
///
/// Returns an error only if the metadata directory cannot be found.
pub fn sync_tags_from_metadata(
    manifest: &mut Manifest,
    workspace_root: &Path,
) -> Result<usize, String> {
    let meta_dir = workspace_root.join(METADATA_DIR_RELATIVE);
    if !meta_dir.is_dir() {
        return Err(format!(
            "metadata directory not found: {}",
            meta_dir.display()
        ));
    }

    let mut updated = 0;
    for entry in &mut manifest.entries {
        let meta_path = meta_dir.join(format!("{}.json", entry.db_id));
        if let Ok(content) = std::fs::read_to_string(&meta_path) {
            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(tags) = meta.get("tags").and_then(|v| v.as_array()) {
                    let new_tags: Vec<String> = tags
                        .iter()
                        .filter_map(|t| t.as_str().map(str::to_owned))
                        .collect();
                    if entry.tags != new_tags {
                        entry.tags = new_tags;
                        updated += 1;
                    }
                }
            }
        }
    }
    Ok(updated)
}

/// Write a manifest to disk (pretty-printed JSON).
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub fn save_manifest(manifest: &Manifest, path: &Path) -> Result<(), String> {
    let json = serde_json::to_string_pretty(manifest)
        .map_err(|e| format!("cannot serialize manifest: {e}"))?;
    std::fs::write(path, format!("{json}\n"))
        .map_err(|e| format!("cannot write manifest to {}: {e}", path.display()))
}

/// Validate that all `db_id` values in the manifest are unique.
///
/// Returns a list of duplicate IDs (empty if all are unique).
#[must_use]
pub fn find_duplicate_db_ids(manifest: &Manifest) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();
    for entry in &manifest.entries {
        if !seen.insert(&entry.db_id) {
            duplicates.push(entry.db_id.clone());
        }
    }
    duplicates
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use jsonschema::validator_for;
    use serde_json::Value;
    use std::fs;

    fn sample_manifest() -> Manifest {
        Manifest {
            manifest_version: 1,
            entries: vec![
                ManifestEntry {
                    db_id: "beads_rust_beads".to_owned(),
                    golden_filename: "beads_rust_beads.db".to_owned(),
                    sha256_golden: "aaa".to_owned(),
                    size_bytes: 4_120_576,
                    source_path: None,
                    provenance: None,
                    tags: vec!["beads".to_owned(), "medium".to_owned(), "wal".to_owned()],
                    sqlite_meta: Some(ManifestSqliteMeta {
                        page_size: Some(4096),
                        journal_mode: Some("wal".to_owned()),
                        user_version: None,
                        application_id: None,
                    }),
                },
                ManifestEntry {
                    db_id: "beads_viewer".to_owned(),
                    golden_filename: "beads_viewer.db".to_owned(),
                    sha256_golden: "bbb".to_owned(),
                    size_bytes: 6_565_888,
                    source_path: None,
                    provenance: None,
                    tags: vec!["beads".to_owned(), "large".to_owned(), "wal".to_owned()],
                    sqlite_meta: Some(ManifestSqliteMeta {
                        page_size: Some(4096),
                        journal_mode: Some("wal".to_owned()),
                        user_version: None,
                        application_id: None,
                    }),
                },
                ManifestEntry {
                    db_id: "frankensqlite".to_owned(),
                    golden_filename: "frankensqlite.db".to_owned(),
                    sha256_golden: "ccc".to_owned(),
                    size_bytes: 500_000,
                    source_path: None,
                    provenance: None,
                    tags: vec!["medium".to_owned()],
                    sqlite_meta: Some(ManifestSqliteMeta {
                        page_size: Some(4096),
                        journal_mode: Some("delete".to_owned()),
                        user_version: None,
                        application_id: None,
                    }),
                },
                ManifestEntry {
                    db_id: "tiny_test".to_owned(),
                    golden_filename: "tiny_test.db".to_owned(),
                    sha256_golden: "ddd".to_owned(),
                    size_bytes: 10_000,
                    source_path: None,
                    provenance: None,
                    tags: vec!["small".to_owned(), "test".to_owned()],
                    sqlite_meta: None,
                },
            ],
        }
    }

    fn sample_campaign(root: &Path) -> BeadsBenchmarkCampaign {
        let working_copy_relpath =
            "sample_sqlite_db_files/working/beads_bench_20260310/golden/frankensqlite_beads.db";
        let working_copy = root.join(working_copy_relpath);
        let bytes = b"fixture-bytes";
        fs::create_dir_all(working_copy.parent().unwrap()).unwrap();
        fs::write(&working_copy, bytes).unwrap();
        let digest = sha256_hex_file(&working_copy).unwrap();

        BeadsBenchmarkCampaign {
            schema_version: BEADS_BENCHMARK_CAMPAIGN_SCHEMA_V1.to_owned(),
            campaign_id: "bd-db300.1.2".to_owned(),
            title: "sample".to_owned(),
            working_benchmark_root_relpath: "sample_sqlite_db_files/working/beads_bench_20260310"
                .to_owned(),
            beads_data_relpath: ".beads/issues.jsonl".to_owned(),
            fixtures: vec![BeadsBenchmarkFixture {
                fixture_id: "frankensqlite".to_owned(),
                source_path: "/data/projects/frankensqlite/.beads/beads.db".to_owned(),
                source_sha256: "a".repeat(64),
                source_size_bytes: bytes.len() as u64,
                working_copy_relpath: working_copy_relpath.to_owned(),
                working_copy_sha256: digest,
                working_copy_size_bytes: bytes.len() as u64,
                page_size: 4096,
                journal_mode: "wal".to_owned(),
                capture_rule: "copy pinned working copy".to_owned(),
            }],
            placement_profiles: vec![
                PlacementProfile {
                    id: PLACEMENT_PROFILE_BASELINE_UNPINNED.to_owned(),
                    kind: PlacementProfileKind::Baseline,
                    description: "scheduler default".to_owned(),
                    command_hint: "run directly".to_owned(),
                    availability: PlacementAvailability::Universal,
                },
                PlacementProfile {
                    id: PLACEMENT_PROFILE_RECOMMENDED_PINNED.to_owned(),
                    kind: PlacementProfileKind::RecommendedPinned,
                    description: "pin to sibling-free cores".to_owned(),
                    command_hint: "taskset pin".to_owned(),
                    availability: PlacementAvailability::TopologyAware,
                },
                PlacementProfile {
                    id: PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE.to_owned(),
                    kind: PlacementProfileKind::AdversarialTopology,
                    description: "spread across nodes".to_owned(),
                    command_hint: "numactl --cpunodebind".to_owned(),
                    availability: PlacementAvailability::TopologyAware,
                },
            ],
            hardware_classes: vec![
                HardwareClass {
                    id: HARDWARE_CLASS_LINUX_X86_64_ANY.to_owned(),
                    id_fields: HardwareClassIdFields {
                        os_family: HardwareOsFamily::Linux,
                        cpu_arch: HardwareCpuArchitecture::X86_64,
                        topology_class: HardwareTopologyClass::Any,
                    },
                    min_logical_cores: 4,
                    min_numa_nodes: None,
                    description: "generic".to_owned(),
                },
                HardwareClass {
                    id: HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA.to_owned(),
                    id_fields: HardwareClassIdFields {
                        os_family: HardwareOsFamily::Linux,
                        cpu_arch: HardwareCpuArchitecture::X86_64,
                        topology_class: HardwareTopologyClass::ManyCoreNuma,
                    },
                    min_logical_cores: 16,
                    min_numa_nodes: Some(2),
                    description: "many-core".to_owned(),
                },
            ],
            retry_policies: vec![RetryPolicy {
                id: "instrumented_busy_retry_v1".to_owned(),
                max_busy_retries: 10_000,
                busy_backoff_ms: 1,
                busy_backoff_max_ms: 250,
                notes: "default".to_owned(),
            }],
            build_profiles: vec![BuildProfile {
                id: "release_perf".to_owned(),
                cargo_profile: "release-perf".to_owned(),
                cargo_args: vec!["--profile".to_owned(), "release-perf".to_owned()],
                notes: "perf".to_owned(),
            }],
            seed_policies: vec![SeedPolicy {
                id: "fixed_seed_42".to_owned(),
                kind: "fixed".to_owned(),
                base_seed: 42,
                notes: "stable".to_owned(),
            }],
            matrix_rows: vec![BeadsBenchmarkMatrixRow {
                row_id: "mixed_read_write_c4".to_owned(),
                fixtures: vec!["frankensqlite".to_owned()],
                workload: "mixed_read_write".to_owned(),
                concurrency: 4,
                modes: vec![
                    BenchmarkMode::SqliteReference,
                    BenchmarkMode::FsqliteMvcc,
                    BenchmarkMode::FsqliteSingleWriter,
                ],
                placement_variants: vec![
                    PlacementVariant {
                        placement_profile_id: PLACEMENT_PROFILE_BASELINE_UNPINNED.to_owned(),
                        hardware_class_id: HARDWARE_CLASS_LINUX_X86_64_ANY.to_owned(),
                        required: true,
                    },
                    PlacementVariant {
                        placement_profile_id: PLACEMENT_PROFILE_RECOMMENDED_PINNED.to_owned(),
                        hardware_class_id: HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA.to_owned(),
                        required: true,
                    },
                    PlacementVariant {
                        placement_profile_id: PLACEMENT_PROFILE_ADVERSARIAL_CROSS_NODE.to_owned(),
                        hardware_class_id: HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA.to_owned(),
                        required: true,
                    },
                ],
                retry_policy_id: "instrumented_busy_retry_v1".to_owned(),
                build_profile_id: "release_perf".to_owned(),
                seed_policy_id: "fixed_seed_42".to_owned(),
            }],
            artifact_contract: BenchmarkArtifactContract {
                artifact_root_relpath: "artifacts/perf/bd-db300.1.2".to_owned(),
                bundle_dir_template:
                    "{row_id}__{fixture_id}__{mode}__{placement_profile_id}__rev_{source_revision}__beads_{beads_hash}"
                        .to_owned(),
                result_jsonl_name: "results.jsonl".to_owned(),
                summary_md_name: "summary.md".to_owned(),
                manifest_name: "manifest.json".to_owned(),
                logs_dir_name: "logs".to_owned(),
                profiles_dir_name: "profiles".to_owned(),
            },
        }
    }

    #[test]
    fn test_exact_match() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads_rust_beads".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(result.is_single());
        if let SelectionResult::Single(e) = result {
            assert_eq!(e.db_id, "beads_rust_beads");
        }
    }

    #[test]
    fn test_substring_match_unique() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("frankensqlite".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(result.is_single());
    }

    #[test]
    fn test_substring_match_ambiguous() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(matches!(result, SelectionResult::Ambiguous { .. }));
        if let SelectionResult::Ambiguous { candidates, .. } = result {
            assert_eq!(candidates.len(), 2);
        }
    }

    #[test]
    fn test_no_match() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("nonexistent".to_owned()),
            ..Default::default()
        };
        let result = select(&m, &filter);
        assert!(matches!(result, SelectionResult::NoMatch { .. }));
    }

    #[test]
    fn test_require_single_ok() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads_rust_beads".to_owned()),
            ..Default::default()
        };
        let entry = select(&m, &filter).require_single().unwrap();
        assert_eq!(entry.db_id, "beads_rust_beads");
    }

    #[test]
    fn test_require_single_ambiguous_error() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("beads".to_owned()),
            ..Default::default()
        };
        let err = select(&m, &filter).require_single().unwrap_err();
        assert!(err.contains("ambiguous"));
        assert!(err.contains("beads_rust_beads"));
        assert!(err.contains("beads_viewer"));
    }

    #[test]
    fn test_tag_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            require_tags: vec!["beads".to_owned()],
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.tags.contains(&"beads".to_owned())));
    }

    #[test]
    fn test_exclude_tag_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            exclude_tags: vec!["beads".to_owned()],
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|e| !e.tags.contains(&"beads".to_owned()))
        );
    }

    #[test]
    fn test_size_range_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            min_size_bytes: Some(1_000_000),
            max_size_bytes: Some(5_000_000),
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].db_id, "beads_rust_beads");
    }

    #[test]
    fn test_wal_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            requires_wal: true,
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|e| e.db_id.contains("beads")));
    }

    #[test]
    fn test_combined_filters() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            require_tags: vec!["beads".to_owned()],
            min_size_bytes: Some(5_000_000),
            ..Default::default()
        };
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].db_id, "beads_viewer");
    }

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1K"), Some(1024));
        assert_eq!(parse_size("1KB"), Some(1024));
        assert_eq!(parse_size("1M"), Some(1024 * 1024));
        assert_eq!(parse_size("1MB"), Some(1024 * 1024));
        assert_eq!(parse_size("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("1.5M"), Some(1_572_864));
        assert_eq!(parse_size("500k"), Some(512_000));
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("abc"), None);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500B");
        assert_eq!(format_size(1024), "1.0KB");
        assert_eq!(format_size(1_048_576), "1.0MB");
        assert_eq!(format_size(1_073_741_824), "1.00GB");
    }

    #[test]
    fn test_parse_filter_args() {
        let args: Vec<String> = vec![
            "run".into(),
            "--db".into(),
            "beads".into(),
            "--tag".into(),
            "wal".into(),
            "--tag".into(),
            "large".into(),
            "--min-size".into(),
            "1M".into(),
            "--requires-wal".into(),
        ];
        let filter = parse_filter_args(&args);
        assert_eq!(filter.db_selector, Some("beads".to_owned()));
        assert_eq!(filter.require_tags, vec!["wal", "large"]);
        assert_eq!(filter.min_size_bytes, Some(1024 * 1024));
        assert!(filter.requires_wal);
    }

    #[test]
    fn test_parse_filter_args_empty() {
        let args: Vec<String> = vec!["run".into()];
        let filter = parse_filter_args(&args);
        assert!(filter.db_selector.is_none());
        assert!(filter.require_tags.is_empty());
    }

    #[test]
    fn test_fixture_table_format() {
        let m = sample_manifest();
        let table = format_fixture_table(&m.entries);
        assert!(table.contains("beads_rust_beads"));
        assert!(table.contains("frankensqlite"));
        assert!(table.contains("db_id"));
    }

    #[test]
    fn test_resolve_paths() {
        let entry = ManifestEntry {
            db_id: "test_db".to_owned(),
            golden_filename: "test_db.db".to_owned(),
            sha256_golden: "abc".to_owned(),
            size_bytes: 100,
            source_path: None,
            provenance: None,
            tags: vec![],
            sqlite_meta: None,
        };
        let root = Path::new("/workspace");
        let golden = resolve_golden_path(root, &entry);
        assert_eq!(
            golden,
            PathBuf::from("/workspace/sample_sqlite_db_files/golden/test_db.db")
        );
        let meta = resolve_metadata_path(root, &entry);
        assert_eq!(
            meta,
            PathBuf::from("/workspace/sample_sqlite_db_files/metadata/test_db.json")
        );
    }

    #[test]
    fn test_selection_help_text() {
        let help = fixture_selection_help();
        assert!(help.contains("--db"));
        assert!(help.contains("--tag"));
        assert!(help.contains("--min-size"));
        assert!(help.contains("--requires-wal"));
    }

    #[test]
    fn test_load_manifest_real() {
        // Try loading the real manifest from the workspace.
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        if let Ok(manifest) = load_manifest(workspace_root) {
            assert_eq!(manifest.manifest_version, 1);
            assert!(!manifest.entries.is_empty());
            // Every entry should have a non-empty db_id.
            for entry in &manifest.entries {
                assert!(!entry.db_id.is_empty());
                assert!(!entry.golden_filename.is_empty());
                assert_eq!(entry.sha256_golden.len(), 64);
            }
        }
        // Don't fail if manifest doesn't exist (CI without corpus).
    }

    #[test]
    fn test_select_all_no_filter() {
        let m = sample_manifest();
        let filter = FixtureFilter::default();
        let results = select_all(&m, &filter);
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_no_match_description() {
        let m = sample_manifest();
        let filter = FixtureFilter {
            db_selector: Some("nonexistent".to_owned()),
            require_tags: vec!["rare".to_owned()],
            ..Default::default()
        };
        let result = select(&m, &filter);
        let SelectionResult::NoMatch { reason } = result else {
            unreachable!("expected NoMatch, got {result:?}");
        };
        assert!(reason.contains("nonexistent"));
        assert!(reason.contains("tag=rare"));
    }

    #[test]
    fn test_find_duplicate_db_ids_none() {
        let m = sample_manifest();
        let dups = find_duplicate_db_ids(&m);
        assert!(dups.is_empty(), "sample manifest should have unique IDs");
    }

    #[test]
    fn test_find_duplicate_db_ids_detects() {
        let mut m = sample_manifest();
        m.entries.push(ManifestEntry {
            db_id: "beads_rust_beads".to_owned(),
            golden_filename: "duplicate.db".to_owned(),
            sha256_golden: "eee".to_owned(),
            size_bytes: 100,
            source_path: None,
            provenance: None,
            tags: vec![],
            sqlite_meta: None,
        });
        let dups = find_duplicate_db_ids(&m);
        assert_eq!(dups, vec!["beads_rust_beads"]);
    }

    #[test]
    fn test_real_manifest_db_ids_unique() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        if let Ok(manifest) = load_manifest(workspace_root) {
            let dups = find_duplicate_db_ids(&manifest);
            assert!(dups.is_empty(), "manifest has duplicate db_ids: {dups:?}");
        }
    }

    #[test]
    fn test_sync_tags_from_metadata() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        if let Ok(mut manifest) = load_manifest(workspace_root) {
            let meta_dir = workspace_root.join(METADATA_DIR_RELATIVE);
            if meta_dir.is_dir() {
                let count = sync_tags_from_metadata(&mut manifest, workspace_root).unwrap();
                // After sync, entries with metadata files should have tags.
                let entries_with_tags = manifest
                    .entries
                    .iter()
                    .filter(|e| !e.tags.is_empty())
                    .count();
                assert!(
                    entries_with_tags > 0 || count == 0,
                    "sync should populate tags from metadata"
                );
            }
        }
    }

    #[test]
    fn test_expand_beads_benchmark_campaign() {
        let tempdir = tempfile::tempdir().unwrap();
        let campaign = sample_campaign(tempdir.path());
        let cells = expand_beads_benchmark_campaign(&campaign);
        assert_eq!(cells.len(), 9);
        assert_eq!(cells[0].row_id, "mixed_read_write_c4");
        assert_eq!(cells[0].fixture_id, "frankensqlite");
    }

    #[test]
    fn test_render_benchmark_bundle_dir_shortens_hashes() {
        let tempdir = tempfile::tempdir().unwrap();
        let campaign = sample_campaign(tempdir.path());
        let cell = expand_beads_benchmark_campaign(&campaign)
            .into_iter()
            .next()
            .unwrap();
        let rendered = render_benchmark_bundle_dir(
            &campaign.artifact_contract,
            &cell,
            "0123456789abcdef",
            "fedcba9876543210",
        );
        assert!(rendered.contains("__rev_0123456789ab"));
        assert!(rendered.contains("__beads_fedcba987654"));
    }

    #[test]
    fn test_validate_beads_benchmark_campaign_sample() {
        let tempdir = tempfile::tempdir().unwrap();
        let campaign = sample_campaign(tempdir.path());
        validate_beads_benchmark_campaign(&campaign, tempdir.path()).unwrap();
    }

    #[test]
    fn test_validate_beads_benchmark_campaign_rejects_bad_fixture_provenance() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut campaign = sample_campaign(tempdir.path());
        campaign.fixtures[0].source_sha256 = "not-a-digest".to_owned();
        campaign.fixtures[0].working_copy_relpath = "outside/beads.db".to_owned();
        campaign.fixtures[0].capture_rule.clear();

        let error =
            validate_beads_benchmark_campaign(&campaign, tempdir.path()).expect_err("must fail");
        assert!(error.contains("source_sha256"));
        assert!(error.contains("working copy must stay under campaign root"));
        assert!(error.contains("capture_rule must not be empty"));
    }

    #[test]
    fn test_beads_benchmark_campaign_taxonomy_is_explicit() {
        let tempdir = tempfile::tempdir().unwrap();
        let campaign = sample_campaign(tempdir.path());

        let baseline = campaign
            .placement_profiles
            .iter()
            .find(|profile| profile.id == PLACEMENT_PROFILE_BASELINE_UNPINNED)
            .unwrap();
        assert_eq!(baseline.kind, PlacementProfileKind::Baseline);
        assert_eq!(baseline.availability, PlacementAvailability::Universal);

        let recommended = campaign
            .placement_profiles
            .iter()
            .find(|profile| profile.id == PLACEMENT_PROFILE_RECOMMENDED_PINNED)
            .unwrap();
        assert_eq!(recommended.kind, PlacementProfileKind::RecommendedPinned);
        assert_eq!(
            recommended.availability,
            PlacementAvailability::TopologyAware
        );

        let many_core = campaign
            .hardware_classes
            .iter()
            .find(|hardware| hardware.id == HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA)
            .unwrap();
        assert_eq!(
            many_core.id_fields.canonical_id(),
            HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA
        );
        assert_eq!(
            many_core.id_fields.topology_class,
            HardwareTopologyClass::ManyCoreNuma
        );
    }

    #[test]
    fn test_load_beads_benchmark_campaign_real() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        if let Ok(campaign) = load_beads_benchmark_campaign(workspace_root) {
            validate_beads_benchmark_campaign(&campaign, workspace_root).unwrap();
            assert_eq!(campaign.campaign_id, "bd-db300.1.2");
            assert_eq!(campaign.matrix_rows.len(), 9);

            let mut fixture_ids: Vec<_> = campaign
                .fixtures
                .iter()
                .map(|fixture| fixture.fixture_id.as_str())
                .collect();
            fixture_ids.sort_unstable();
            assert_eq!(
                fixture_ids,
                vec!["frankensearch", "frankensqlite", "frankentui"]
            );

            let cells = expand_beads_benchmark_campaign(&campaign);
            assert_eq!(cells.len(), 216);
        }
    }

    #[test]
    fn test_beads_benchmark_campaign_manifest_matches_json_schema() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let schema_path = workspace_root
            .join("sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.schema.json");
        let manifest_path = workspace_root.join(BEADS_BENCHMARK_CAMPAIGN_PATH_RELATIVE);
        if !schema_path.is_file() || !manifest_path.is_file() {
            return;
        }

        let schema: Value = serde_json::from_str(
            &fs::read_to_string(&schema_path).expect("schema json should be readable"),
        )
        .expect("schema should parse");
        let manifest: Value = serde_json::from_str(
            &fs::read_to_string(&manifest_path).expect("manifest json should be readable"),
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
    fn test_real_beads_benchmark_campaign_bundle_path_contract() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let Ok(campaign) = load_beads_benchmark_campaign(workspace_root) else {
            return;
        };
        let cell = expand_beads_benchmark_campaign(&campaign)
            .into_iter()
            .find(|cell| {
                cell.row_id == "mixed_read_write_c8"
                    && cell.fixture_id == "frankensqlite"
                    && cell.mode == BenchmarkMode::FsqliteMvcc
                    && cell.placement_profile_id == PLACEMENT_PROFILE_RECOMMENDED_PINNED
                    && cell.hardware_class_id == HARDWARE_CLASS_LINUX_X86_64_MANY_CORE_NUMA
            })
            .expect("expected canonical bundle cell");

        let bundle_path = benchmark_bundle_path(
            workspace_root,
            &campaign,
            &cell,
            "0123456789abcdef",
            "abcdef0123456789fedcba9876543210",
        );
        let relative = bundle_path
            .strip_prefix(workspace_root)
            .expect("bundle path should stay in workspace");
        assert_eq!(
            relative,
            Path::new(
                "artifacts/perf/bd-db300.1.2/mixed_read_write_c8__frankensqlite__fsqlite_mvcc__recommended_pinned__rev_0123456789ab__beads_abcdef012345"
            )
        );
    }
}
