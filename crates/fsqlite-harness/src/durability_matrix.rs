//! Cross-platform durability matrix specification and environment contracts.
//!
//! Bead: bd-mblr.7.4
//!
//! This module defines a deterministic durability matrix connecting:
//! - environment contracts (`os`, `filesystem`, toolchain),
//! - crash/recovery scenarios,
//! - probe definitions used by CI/workflows for parity and drift detection.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Bead identifier for evidence/log correlation.
pub const BEAD_ID: &str = "bd-mblr.7.4";
/// Serialization schema version for `DurabilityMatrix`.
pub const MATRIX_SCHEMA_VERSION: u32 = 1;
/// Default deterministic root seed for scenario seed derivation.
pub const DEFAULT_ROOT_SEED: u64 = 0xB740_0000_0000_0001;
/// Canonical logging/reference standard.
pub const LOG_STANDARD_REF: &str = "bd-1fpm";

/// Operating system family for an environment contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OperatingSystem {
    Linux,
    MacOs,
    Windows,
    FreeBsd,
}

/// Filesystem class relevant to durability semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FilesystemClass {
    Ext4Ordered,
    XfsBarrier,
    Apfs,
    Ntfs,
    Zfs,
}

/// Toolchain variant used when executing durability probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ToolchainVariant {
    Nightly,
    Stable,
}

/// Crash pattern represented by a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CrashMode {
    MidCommit,
    PostCommitPreCheckpoint,
    DuringCheckpoint,
    CorruptionInjection,
}

/// Validation lane expected to run for a probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DurabilityLane {
    RecoveryReplay,
    CorruptionRecovery,
    CheckpointParity,
    FullSuiteFallback,
}

/// Deterministic contract for a single execution environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentContract {
    pub id: String,
    pub os: OperatingSystem,
    pub filesystem: FilesystemClass,
    pub toolchain: ToolchainVariant,
    pub requires_atomic_rename: bool,
    pub requires_fsync_durability: bool,
    pub notes: String,
}

/// One durability scenario executed across one or more environments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityScenario {
    pub id: String,
    pub crash_mode: CrashMode,
    pub command: String,
    pub scenario_ids: Vec<String>,
    pub invariants: Vec<String>,
    pub seed_offset: u64,
}

impl DurabilityScenario {
    /// Deterministically derive this scenario seed from matrix root seed.
    #[must_use]
    pub fn derived_seed(&self, root_seed: u64) -> u64 {
        root_seed.wrapping_add(self.seed_offset)
    }
}

/// Concrete probe linking an environment to a scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityProbe {
    pub id: String,
    pub environment_id: String,
    pub scenario_id: String,
    pub required_lanes: Vec<DurabilityLane>,
}

/// Canonical durability matrix used by durability-gate workflows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityMatrix {
    pub bead_id: String,
    pub schema_version: u32,
    pub root_seed: u64,
    pub log_standard_ref: String,
    pub environments: Vec<EnvironmentContract>,
    pub scenarios: Vec<DurabilityScenario>,
    pub probes: Vec<DurabilityProbe>,
}

impl DurabilityMatrix {
    /// Build canonical matrix for a given deterministic root seed.
    #[must_use]
    pub fn canonical(root_seed: u64) -> Self {
        build_durability_matrix(root_seed)
    }

    /// Validate this matrix, returning all diagnostics.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        validate_durability_matrix(self)
    }

    /// Return probes targeting a specific environment id.
    #[must_use]
    pub fn probes_for_environment(&self, environment_id: &str) -> Vec<&DurabilityProbe> {
        self.probes
            .iter()
            .filter(|probe| probe.environment_id == environment_id)
            .collect()
    }

    /// Return probes targeting a specific scenario id.
    #[must_use]
    pub fn probes_for_scenario(&self, scenario_id: &str) -> Vec<&DurabilityProbe> {
        self.probes
            .iter()
            .filter(|probe| probe.scenario_id == scenario_id)
            .collect()
    }

    /// Serialize the matrix in a deterministic pretty JSON format.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Build and validate the canonical matrix.
pub fn build_validated_durability_matrix(root_seed: u64) -> Result<DurabilityMatrix, String> {
    let matrix = build_durability_matrix(root_seed);
    let diagnostics = validate_durability_matrix(&matrix);
    if diagnostics.is_empty() {
        Ok(matrix)
    } else {
        Err(diagnostics.join("; "))
    }
}

/// Build the canonical durability matrix.
#[must_use]
pub fn build_durability_matrix(root_seed: u64) -> DurabilityMatrix {
    let mut environments = canonical_environments();
    environments.sort_by(|left, right| left.id.cmp(&right.id));

    let mut scenarios = canonical_scenarios();
    scenarios.sort_by(|left, right| left.id.cmp(&right.id));

    let mut probes = Vec::with_capacity(environments.len() * scenarios.len());
    for environment in &environments {
        for scenario in &scenarios {
            probes.push(DurabilityProbe {
                id: format!("probe-{}-{}", environment.id, scenario.id.to_lowercase()),
                environment_id: environment.id.clone(),
                scenario_id: scenario.id.clone(),
                required_lanes: lanes_for_scenario(&scenario.id),
            });
        }
    }
    probes.sort_by(|left, right| left.id.cmp(&right.id));

    DurabilityMatrix {
        bead_id: BEAD_ID.to_owned(),
        schema_version: MATRIX_SCHEMA_VERSION,
        root_seed,
        log_standard_ref: LOG_STANDARD_REF.to_owned(),
        environments,
        scenarios,
        probes,
    }
}

/// Validate durability matrix consistency and minimum coverage constraints.
#[must_use]
pub fn validate_durability_matrix(matrix: &DurabilityMatrix) -> Vec<String> {
    let mut diagnostics = Vec::new();

    if matrix.bead_id != BEAD_ID {
        diagnostics.push(format!(
            "unexpected bead_id: {} (expected {BEAD_ID})",
            matrix.bead_id
        ));
    }
    if matrix.schema_version != MATRIX_SCHEMA_VERSION {
        diagnostics.push(format!(
            "unexpected schema_version: {} (expected {MATRIX_SCHEMA_VERSION})",
            matrix.schema_version
        ));
    }
    if matrix.log_standard_ref != LOG_STANDARD_REF {
        diagnostics.push(format!(
            "unexpected log_standard_ref: {} (expected {LOG_STANDARD_REF})",
            matrix.log_standard_ref
        ));
    }

    if matrix.environments.is_empty() {
        diagnostics.push("matrix has no environments".to_owned());
    }
    if matrix.scenarios.is_empty() {
        diagnostics.push("matrix has no scenarios".to_owned());
    }
    if matrix.probes.is_empty() {
        diagnostics.push("matrix has no probes".to_owned());
    }

    let environment_ids: BTreeSet<_> = matrix.environments.iter().map(|e| e.id.as_str()).collect();
    if environment_ids.len() != matrix.environments.len() {
        diagnostics.push("duplicate environment ids detected".to_owned());
    }

    let scenario_ids: BTreeSet<_> = matrix.scenarios.iter().map(|s| s.id.as_str()).collect();
    if scenario_ids.len() != matrix.scenarios.len() {
        diagnostics.push("duplicate scenario ids detected".to_owned());
    }

    let probe_ids: BTreeSet<_> = matrix.probes.iter().map(|p| p.id.as_str()).collect();
    if probe_ids.len() != matrix.probes.len() {
        diagnostics.push("duplicate probe ids detected".to_owned());
    }

    for probe in &matrix.probes {
        if !environment_ids.contains(probe.environment_id.as_str()) {
            diagnostics.push(format!(
                "probe {} references unknown environment_id {}",
                probe.id, probe.environment_id
            ));
        }
        if !scenario_ids.contains(probe.scenario_id.as_str()) {
            diagnostics.push(format!(
                "probe {} references unknown scenario_id {}",
                probe.id, probe.scenario_id
            ));
        }
        if probe.required_lanes.is_empty() {
            diagnostics.push(format!("probe {} has no required lanes", probe.id));
        }
    }

    let os_coverage: BTreeSet<_> = matrix.environments.iter().map(|e| e.os).collect();
    for required_os in [
        OperatingSystem::Linux,
        OperatingSystem::MacOs,
        OperatingSystem::Windows,
    ] {
        if !os_coverage.contains(&required_os) {
            diagnostics.push(format!("missing required OS coverage: {required_os:?}"));
        }
    }

    for scenario in &matrix.scenarios {
        if scenario.command.trim().is_empty() {
            diagnostics.push(format!("scenario {} has empty command", scenario.id));
        }
        if scenario.scenario_ids.is_empty() {
            diagnostics.push(format!("scenario {} has empty scenario_ids", scenario.id));
        }
        if scenario.invariants.is_empty() {
            diagnostics.push(format!("scenario {} has empty invariants", scenario.id));
        }
    }

    diagnostics
}

fn canonical_environments() -> Vec<EnvironmentContract> {
    vec![
        EnvironmentContract {
            id: "env-linux-ext4-nightly".to_owned(),
            os: OperatingSystem::Linux,
            filesystem: FilesystemClass::Ext4Ordered,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "Primary CI lane baseline".to_owned(),
        },
        EnvironmentContract {
            id: "env-linux-xfs-nightly".to_owned(),
            os: OperatingSystem::Linux,
            filesystem: FilesystemClass::XfsBarrier,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "Barrier-heavy metadata ordering behavior".to_owned(),
        },
        EnvironmentContract {
            id: "env-linux-ext4-stable".to_owned(),
            os: OperatingSystem::Linux,
            filesystem: FilesystemClass::Ext4Ordered,
            toolchain: ToolchainVariant::Stable,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "Regression guard against nightly drift".to_owned(),
        },
        EnvironmentContract {
            id: "env-macos-apfs-nightly".to_owned(),
            os: OperatingSystem::MacOs,
            filesystem: FilesystemClass::Apfs,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "APFS crash-recovery ordering semantics".to_owned(),
        },
        EnvironmentContract {
            id: "env-windows-ntfs-nightly".to_owned(),
            os: OperatingSystem::Windows,
            filesystem: FilesystemClass::Ntfs,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "NTFS rename + flush behavior".to_owned(),
        },
        EnvironmentContract {
            id: "env-freebsd-zfs-nightly".to_owned(),
            os: OperatingSystem::FreeBsd,
            filesystem: FilesystemClass::Zfs,
            toolchain: ToolchainVariant::Nightly,
            requires_atomic_rename: true,
            requires_fsync_durability: true,
            notes: "ZFS durability semantics and metadata checksums".to_owned(),
        },
    ]
}

fn canonical_scenarios() -> Vec<DurabilityScenario> {
    vec![
        DurabilityScenario {
            id: "REC-1".to_owned(),
            crash_mode: CrashMode::MidCommit,
            command: "cargo test -p fsqlite-e2e --test recovery_crash_wal_replay -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["REC-1".to_owned(), "WAL-1".to_owned()],
            invariants: vec!["INV-5".to_owned(), "WAL-1".to_owned(), "PAGER-1".to_owned()],
            seed_offset: 0x101,
        },
        DurabilityScenario {
            id: "REC-2".to_owned(),
            crash_mode: CrashMode::CorruptionInjection,
            command: "cargo test -p fsqlite-e2e --test recovery_single_page -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["REC-2".to_owned()],
            invariants: vec!["PAGER-1".to_owned(), "BTREE-1".to_owned()],
            seed_offset: 0x102,
        },
        DurabilityScenario {
            id: "REC-3".to_owned(),
            crash_mode: CrashMode::CorruptionInjection,
            command: "cargo test -p fsqlite-e2e --test recovery_wal_corruption -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["REC-3".to_owned(), "CORRUPT-1".to_owned()],
            invariants: vec!["WAL-1".to_owned(), "INV-6".to_owned(), "INV-7".to_owned()],
            seed_offset: 0x103,
        },
        DurabilityScenario {
            id: "WAL-2".to_owned(),
            crash_mode: CrashMode::DuringCheckpoint,
            command: "cargo test -p fsqlite-e2e --test correctness_transactions -- --nocapture"
                .to_owned(),
            scenario_ids: vec!["WAL-2".to_owned(), "TXN-2".to_owned()],
            invariants: vec!["WAL-1".to_owned(), "INV-4".to_owned()],
            seed_offset: 0x104,
        },
    ]
}

fn lanes_for_scenario(scenario_id: &str) -> Vec<DurabilityLane> {
    match scenario_id {
        "REC-1" => vec![
            DurabilityLane::RecoveryReplay,
            DurabilityLane::CheckpointParity,
            DurabilityLane::FullSuiteFallback,
        ],
        "REC-2" | "REC-3" => vec![
            DurabilityLane::CorruptionRecovery,
            DurabilityLane::FullSuiteFallback,
        ],
        _ => vec![
            DurabilityLane::CheckpointParity,
            DurabilityLane::FullSuiteFallback,
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_matrix_validates() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let diagnostics = matrix.validate();
        assert!(
            diagnostics.is_empty(),
            "bead_id={BEAD_ID} expected valid matrix, got diagnostics: {diagnostics:?}"
        );
    }

    #[test]
    fn canonical_matrix_has_cross_platform_coverage() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let os_coverage: BTreeSet<_> = matrix.environments.iter().map(|e| e.os).collect();
        assert!(os_coverage.contains(&OperatingSystem::Linux));
        assert!(os_coverage.contains(&OperatingSystem::MacOs));
        assert!(os_coverage.contains(&OperatingSystem::Windows));
    }

    #[test]
    fn probes_exist_for_every_environment_and_scenario() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        for environment in &matrix.environments {
            let probes = matrix.probes_for_environment(&environment.id);
            assert!(
                !probes.is_empty(),
                "environment {} should have at least one probe",
                environment.id
            );
        }
        for scenario in &matrix.scenarios {
            let probes = matrix.probes_for_scenario(&scenario.id);
            assert!(
                !probes.is_empty(),
                "scenario {} should have at least one probe",
                scenario.id
            );
        }
    }

    #[test]
    fn scenario_seed_derivation_is_deterministic() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let first = matrix
            .scenarios
            .first()
            .expect("canonical matrix has at least one scenario");
        let seed_1 = first.derived_seed(matrix.root_seed);
        let seed_2 = first.derived_seed(matrix.root_seed);
        assert_eq!(seed_1, seed_2, "derived seeds must be deterministic");
    }

    #[test]
    fn json_roundtrip_preserves_shape() {
        let matrix = DurabilityMatrix::canonical(DEFAULT_ROOT_SEED);
        let json = matrix.to_json().expect("serialize matrix");
        let restored: DurabilityMatrix = serde_json::from_str(&json).expect("deserialize matrix");
        assert_eq!(restored.bead_id, matrix.bead_id);
        assert_eq!(restored.environments.len(), matrix.environments.len());
        assert_eq!(restored.scenarios.len(), matrix.scenarios.len());
        assert_eq!(restored.probes.len(), matrix.probes.len());
    }
}
