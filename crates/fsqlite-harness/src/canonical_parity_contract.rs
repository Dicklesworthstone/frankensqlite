//! Executable Track A canonical parity contract bundle and drift checks.
//!
//! This lifts the Track A TOML contract out of test-only parsing so harness
//! code, docs, and future CI/reporting flows can consume one reusable loader
//! and validator.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub const SQLITE_VERSION_CONTRACT_PATH: &str = "docs/contracts/sqlite_version_contract.toml";
pub const SUPPORTED_SURFACE_MATRIX_PATH: &str = "docs/contracts/supported_surface_matrix.toml";
pub const FEATURE_UNIVERSE_LEDGER_PATH: &str = "docs/contracts/feature_universe_ledger.toml";
pub const PARITY_SCORE_CONTRACT_PATH: &str = "docs/contracts/parity_score_contract.toml";

#[derive(Debug)]
pub enum CanonicalParityContractError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
}

impl fmt::Display for CanonicalParityContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(f, "failed to read {}: {source}", path.display())
            }
            Self::Parse { path, source } => {
                write!(f, "failed to parse {}: {source}", path.display())
            }
        }
    }
}

impl Error for CanonicalParityContractError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractDiagnostic {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalParityContractValidation {
    pub diagnostics: Vec<ContractDiagnostic>,
}

impl CanonicalParityContractValidation {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SharedContractMeta {
    pub schema_version: String,
    pub bead_id: String,
    pub track_id: String,
    pub generated_at: String,
    pub contract_owner: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqliteVersionContractBody {
    pub sqlite_target: String,
    pub runtime_pragma_sqlite_version: String,
    pub contract_reference_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqliteVersionContractReferences {
    pub runtime_source: String,
    pub surface_matrix: String,
    pub feature_ledger: String,
    pub parity_report_module: String,
    pub readme: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqliteVersionContractDocument {
    pub meta: SharedContractMeta,
    pub contract: SqliteVersionContractBody,
    pub references: SqliteVersionContractReferences,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SupportState {
    Supported,
    Partial,
    Excluded,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupportedSurfaceMatrixMeta {
    pub schema_version: String,
    pub bead_id: String,
    pub track_id: String,
    pub sqlite_target: String,
    pub sqlite_version_contract: String,
    pub generated_at: String,
    pub contract_owner: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceEntry {
    pub feature_id: String,
    pub area: String,
    pub title: String,
    pub support_state: SupportState,
    pub rationale: String,
    pub owner: String,
    pub target_evidence: Vec<String>,
    pub verification_status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupportedSurfaceMatrix {
    pub meta: SupportedSurfaceMatrixMeta,
    pub surface: Vec<SurfaceEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Declared,
    Implemented,
    Tested,
    DifferentiallyVerified,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureUniverseLedgerMeta {
    pub schema_version: String,
    pub bead_id: String,
    pub track_id: String,
    pub sqlite_target: String,
    pub sqlite_version_contract: String,
    pub generated_at: String,
    pub contract_owner: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LedgerFeature {
    pub feature_id: String,
    pub surface_id: String,
    pub component: String,
    pub feature_name: String,
    pub lifecycle_state: LifecycleState,
    pub owner: String,
    pub test_links: Vec<String>,
    pub evidence_links: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureUniverseLedger {
    pub meta: FeatureUniverseLedgerMeta,
    pub features: Vec<LedgerFeature>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParityScoreFormula {
    pub source_taxonomy: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParityScoreContractReferences {
    pub taxonomy: String,
    pub surface_matrix: String,
    pub feature_ledger: String,
    pub verification_contract_module: String,
    pub ratchet_policy_module: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParityScoreContractDocument {
    pub meta: SharedContractMeta,
    pub formula: ParityScoreFormula,
    pub references: ParityScoreContractReferences,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CanonicalParityContractBundle {
    pub version_contract: SqliteVersionContractDocument,
    pub surface_matrix: SupportedSurfaceMatrix,
    pub feature_ledger: FeatureUniverseLedger,
    pub parity_score_contract: ParityScoreContractDocument,
}

impl CanonicalParityContractBundle {
    pub fn load(workspace_root: &Path) -> Result<Self, CanonicalParityContractError> {
        Ok(Self {
            version_contract: load_toml(workspace_root, SQLITE_VERSION_CONTRACT_PATH)?,
            surface_matrix: load_toml(workspace_root, SUPPORTED_SURFACE_MATRIX_PATH)?,
            feature_ledger: load_toml(workspace_root, FEATURE_UNIVERSE_LEDGER_PATH)?,
            parity_score_contract: load_toml(workspace_root, PARITY_SCORE_CONTRACT_PATH)?,
        })
    }

    #[must_use]
    pub fn validate(&self, workspace_root: &Path) -> CanonicalParityContractValidation {
        let mut diagnostics = Vec::new();
        self.validate_track_alignment(&mut diagnostics);
        self.validate_version_alignment(&mut diagnostics);
        self.validate_surface_matrix(&mut diagnostics);
        self.validate_feature_ledger(&mut diagnostics);
        self.validate_reference_paths(workspace_root, &mut diagnostics);
        CanonicalParityContractValidation { diagnostics }
    }

    fn validate_track_alignment(&self, diagnostics: &mut Vec<ContractDiagnostic>) {
        let track_id = self.version_contract.meta.track_id.as_str();
        for (label, candidate) in [
            (
                "version_contract",
                self.version_contract.meta.track_id.as_str(),
            ),
            ("surface_matrix", self.surface_matrix.meta.track_id.as_str()),
            ("feature_ledger", self.feature_ledger.meta.track_id.as_str()),
            (
                "parity_score_contract",
                self.parity_score_contract.meta.track_id.as_str(),
            ),
        ] {
            if candidate != track_id {
                diagnostics.push(ContractDiagnostic {
                    code: "track_id_mismatch",
                    message: format!(
                        "{label} track_id '{}' does not match version contract track_id '{}'",
                        candidate, track_id
                    ),
                });
            }
        }
    }

    fn validate_version_alignment(&self, diagnostics: &mut Vec<ContractDiagnostic>) {
        let version = &self.version_contract.contract;
        if version.sqlite_target != version.runtime_pragma_sqlite_version {
            diagnostics.push(ContractDiagnostic {
                code: "runtime_version_mismatch",
                message: format!(
                    "sqlite_target '{}' does not match runtime_pragma_sqlite_version '{}'",
                    version.sqlite_target, version.runtime_pragma_sqlite_version
                ),
            });
        }

        if self.surface_matrix.meta.sqlite_target != version.sqlite_target {
            diagnostics.push(ContractDiagnostic {
                code: "surface_matrix_sqlite_target_mismatch",
                message: format!(
                    "surface_matrix sqlite_target '{}' does not match version contract '{}'",
                    self.surface_matrix.meta.sqlite_target, version.sqlite_target
                ),
            });
        }
        if self.feature_ledger.meta.sqlite_target != version.sqlite_target {
            diagnostics.push(ContractDiagnostic {
                code: "feature_ledger_sqlite_target_mismatch",
                message: format!(
                    "feature_ledger sqlite_target '{}' does not match version contract '{}'",
                    self.feature_ledger.meta.sqlite_target, version.sqlite_target
                ),
            });
        }
        if self.surface_matrix.meta.sqlite_version_contract != version.contract_reference_path {
            diagnostics.push(ContractDiagnostic {
                code: "surface_matrix_reference_mismatch",
                message: format!(
                    "surface_matrix sqlite_version_contract '{}' does not match '{}'",
                    self.surface_matrix.meta.sqlite_version_contract,
                    version.contract_reference_path
                ),
            });
        }
        if self.feature_ledger.meta.sqlite_version_contract != version.contract_reference_path {
            diagnostics.push(ContractDiagnostic {
                code: "feature_ledger_reference_mismatch",
                message: format!(
                    "feature_ledger sqlite_version_contract '{}' does not match '{}'",
                    self.feature_ledger.meta.sqlite_version_contract,
                    version.contract_reference_path
                ),
            });
        }
        let version_surface_matrix_path =
            normalize_contract_reference(&self.version_contract.references.surface_matrix);
        if version_surface_matrix_path != SUPPORTED_SURFACE_MATRIX_PATH {
            diagnostics.push(ContractDiagnostic {
                code: "surface_matrix_path_mismatch",
                message: format!(
                    "version contract surface_matrix '{}' does not match canonical '{}'",
                    self.version_contract.references.surface_matrix, SUPPORTED_SURFACE_MATRIX_PATH
                ),
            });
        }
        let version_feature_ledger_path =
            normalize_contract_reference(&self.version_contract.references.feature_ledger);
        if version_feature_ledger_path != FEATURE_UNIVERSE_LEDGER_PATH {
            diagnostics.push(ContractDiagnostic {
                code: "feature_ledger_path_mismatch",
                message: format!(
                    "version contract feature_ledger '{}' does not match canonical '{}'",
                    self.version_contract.references.feature_ledger, FEATURE_UNIVERSE_LEDGER_PATH
                ),
            });
        }
        if self.parity_score_contract.references.surface_matrix
            != self.version_contract.references.surface_matrix
        {
            diagnostics.push(ContractDiagnostic {
                code: "parity_score_surface_matrix_mismatch",
                message: format!(
                    "parity score contract surface_matrix '{}' does not match version contract '{}'",
                    self.parity_score_contract.references.surface_matrix,
                    self.version_contract.references.surface_matrix
                ),
            });
        }
        if self.parity_score_contract.references.feature_ledger
            != self.version_contract.references.feature_ledger
        {
            diagnostics.push(ContractDiagnostic {
                code: "parity_score_feature_ledger_mismatch",
                message: format!(
                    "parity score contract feature_ledger '{}' does not match version contract '{}'",
                    self.parity_score_contract.references.feature_ledger,
                    self.version_contract.references.feature_ledger
                ),
            });
        }
        if self.parity_score_contract.formula.source_taxonomy
            != self.parity_score_contract.references.taxonomy
        {
            diagnostics.push(ContractDiagnostic {
                code: "taxonomy_reference_mismatch",
                message: format!(
                    "parity score formula taxonomy '{}' does not match references.taxonomy '{}'",
                    self.parity_score_contract.formula.source_taxonomy,
                    self.parity_score_contract.references.taxonomy
                ),
            });
        }
    }

    fn validate_surface_matrix(&self, diagnostics: &mut Vec<ContractDiagnostic>) {
        let mut feature_ids = BTreeSet::new();
        for entry in &self.surface_matrix.surface {
            if !feature_ids.insert(entry.feature_id.as_str()) {
                diagnostics.push(ContractDiagnostic {
                    code: "duplicate_surface_feature_id",
                    message: format!("duplicate surface feature_id '{}'", entry.feature_id),
                });
            }
        }
    }

    fn validate_feature_ledger(&self, diagnostics: &mut Vec<ContractDiagnostic>) {
        let surface_ids = self
            .surface_matrix
            .surface
            .iter()
            .map(|entry| entry.feature_id.as_str())
            .collect::<BTreeSet<_>>();
        let mut feature_ids = BTreeSet::new();

        for feature in &self.feature_ledger.features {
            if !feature_ids.insert(feature.feature_id.as_str()) {
                diagnostics.push(ContractDiagnostic {
                    code: "duplicate_ledger_feature_id",
                    message: format!("duplicate ledger feature_id '{}'", feature.feature_id),
                });
            }
            if !surface_ids.contains(feature.surface_id.as_str()) {
                diagnostics.push(ContractDiagnostic {
                    code: "unknown_surface_id",
                    message: format!(
                        "ledger feature '{}' references unknown surface_id '{}'",
                        feature.feature_id, feature.surface_id
                    ),
                });
            }
        }
    }

    fn validate_reference_paths(
        &self,
        workspace_root: &Path,
        diagnostics: &mut Vec<ContractDiagnostic>,
    ) {
        for reference in [
            self.version_contract
                .contract
                .contract_reference_path
                .as_str(),
            self.version_contract.references.runtime_source.as_str(),
            self.version_contract.references.surface_matrix.as_str(),
            self.version_contract.references.feature_ledger.as_str(),
            self.version_contract
                .references
                .parity_report_module
                .as_str(),
            self.version_contract.references.readme.as_str(),
            self.parity_score_contract.references.taxonomy.as_str(),
            self.parity_score_contract
                .references
                .surface_matrix
                .as_str(),
            self.parity_score_contract
                .references
                .feature_ledger
                .as_str(),
            self.parity_score_contract
                .references
                .verification_contract_module
                .as_str(),
            self.parity_score_contract
                .references
                .ratchet_policy_module
                .as_str(),
        ] {
            validate_reference_exists(reference, workspace_root, diagnostics);
        }

        for entry in &self.surface_matrix.surface {
            for evidence in &entry.target_evidence {
                validate_reference_exists(evidence, workspace_root, diagnostics);
            }
        }
        for feature in &self.feature_ledger.features {
            for link in &feature.test_links {
                validate_reference_exists(link, workspace_root, diagnostics);
            }
            for link in &feature.evidence_links {
                validate_reference_exists(link, workspace_root, diagnostics);
            }
        }
    }
}

pub fn load_workspace_canonical_parity_contract(
    workspace_root: &Path,
) -> Result<CanonicalParityContractBundle, CanonicalParityContractError> {
    CanonicalParityContractBundle::load(workspace_root)
}

pub fn validate_workspace_canonical_parity_contract(
    workspace_root: &Path,
) -> Result<CanonicalParityContractValidation, CanonicalParityContractError> {
    let bundle = CanonicalParityContractBundle::load(workspace_root)?;
    Ok(bundle.validate(workspace_root))
}

fn load_toml<T>(
    workspace_root: &Path,
    relative_path: &str,
) -> Result<T, CanonicalParityContractError>
where
    T: for<'de> Deserialize<'de>,
{
    let path = workspace_root.join(relative_path);
    let content =
        fs::read_to_string(&path).map_err(|source| CanonicalParityContractError::Read {
            path: path.clone(),
            source,
        })?;
    toml::from_str(&content).map_err(|source| CanonicalParityContractError::Parse { path, source })
}

fn validate_reference_exists(
    reference: &str,
    workspace_root: &Path,
    diagnostics: &mut Vec<ContractDiagnostic>,
) {
    let Some(path_text) = reference_target_path(reference) else {
        return;
    };
    let root_candidate = workspace_root.join(path_text);
    let contract_candidate = workspace_root.join("docs/contracts").join(path_text);
    let candidate = if root_candidate.exists() || Path::new(path_text).components().count() != 1 {
        root_candidate
    } else {
        contract_candidate
    };
    if !candidate.exists() {
        diagnostics.push(ContractDiagnostic {
            code: "missing_reference_path",
            message: format!(
                "reference '{}' points to missing path '{}'",
                reference,
                candidate.display()
            ),
        });
    }
}

fn reference_target_path(reference: &str) -> Option<&str> {
    let path = reference
        .split_once('#')
        .map_or(reference, |(path, _)| path)
        .trim();
    if path.is_empty() || path.contains("://") {
        return None;
    }
    Some(path)
}

fn normalize_contract_reference(reference: &str) -> String {
    let Some(path) = reference_target_path(reference) else {
        return reference.trim().to_owned();
    };
    if Path::new(path).components().count() == 1 {
        format!("docs/contracts/{path}")
    } else {
        path.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    #[test]
    fn workspace_bundle_loads_and_validates() {
        let root = workspace_root();
        let bundle = CanonicalParityContractBundle::load(&root).expect("load bundle");
        let validation = bundle.validate(&root);
        assert!(
            validation.is_valid(),
            "expected workspace contract bundle to validate: {:?}",
            validation.diagnostics
        );
    }

    #[test]
    fn validation_reports_ledger_surface_drift() {
        let root = workspace_root();
        let mut bundle = CanonicalParityContractBundle::load(&root).expect("load bundle");
        bundle.feature_ledger.features[0].surface_id = "SURF-UNKNOWN-999".to_owned();
        let validation = bundle.validate(&root);
        assert!(
            validation
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unknown_surface_id"),
            "expected unknown_surface_id diagnostic, got {:?}",
            validation.diagnostics
        );
    }

    #[test]
    fn validation_reports_missing_referenced_paths() {
        let root = workspace_root();
        let mut bundle = CanonicalParityContractBundle::load(&root).expect("load bundle");
        bundle.surface_matrix.surface[0].target_evidence[0] =
            "crates/fsqlite-harness/src/missing_contract_target.rs".to_owned();
        let validation = bundle.validate(&root);
        assert!(
            validation
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "missing_reference_path"),
            "expected missing_reference_path diagnostic, got {:?}",
            validation.diagnostics
        );
    }

    #[test]
    fn reference_target_path_ignores_fragments_and_external_urls() {
        assert_eq!(
            reference_target_path("supported_surface_matrix.toml#SURF-SQL-CORE-001"),
            Some("supported_surface_matrix.toml")
        );
        assert_eq!(reference_target_path("https://example.com/spec"), None);
        assert_eq!(reference_target_path("#only-fragment"), None);
    }
}
