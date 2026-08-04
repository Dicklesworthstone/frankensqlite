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

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub const SQLITE_VERSION_CONTRACT_PATH: &str = "docs/contracts/sqlite_version_contract.toml";
pub const SUPPORTED_SURFACE_MATRIX_PATH: &str = "docs/contracts/supported_surface_matrix.toml";
pub const FEATURE_UNIVERSE_LEDGER_PATH: &str = "docs/contracts/feature_universe_ledger.toml";
pub const PARITY_TAXONOMY_PATH: &str = "docs/contracts/parity_taxonomy.toml";
pub const CORPUS_MANIFEST_PATH: &str = "docs/contracts/corpus_manifest.toml";
pub const PARITY_SCORE_CONTRACT_PATH: &str = "docs/contracts/parity_score_contract.toml";
pub const CONTRACT_AUTHORITY_REGISTRY_SCHEMA_VERSION: &str =
    "fsqlite.canonical_contract_authority.v1";
const INERT_CONTRACT_POINTER_SCHEMA_VERSION: &str = "fsqlite.inert_contract_pointer.v1";
const INERT_ROOT_DISPOSITION: &str =
    "historical_payload_inert; docs/contracts path is the sole authority";

/// One authoritative contract path and its inert repository-root pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CanonicalContractAuthority {
    pub logical_name: &'static str,
    pub canonical_path: &'static str,
    pub inert_root_path: &'static str,
}

pub const CANONICAL_CONTRACT_AUTHORITIES: &[CanonicalContractAuthority] = &[
    CanonicalContractAuthority {
        logical_name: "supported_surface_matrix",
        canonical_path: SUPPORTED_SURFACE_MATRIX_PATH,
        inert_root_path: "supported_surface_matrix.toml",
    },
    CanonicalContractAuthority {
        logical_name: "feature_universe_ledger",
        canonical_path: FEATURE_UNIVERSE_LEDGER_PATH,
        inert_root_path: "feature_universe_ledger.toml",
    },
    CanonicalContractAuthority {
        logical_name: "parity_taxonomy",
        canonical_path: PARITY_TAXONOMY_PATH,
        inert_root_path: "parity_taxonomy.toml",
    },
    CanonicalContractAuthority {
        logical_name: "sqlite_version_contract",
        canonical_path: SQLITE_VERSION_CONTRACT_PATH,
        inert_root_path: "sqlite_version_contract.toml",
    },
    CanonicalContractAuthority {
        logical_name: "corpus_manifest",
        canonical_path: CORPUS_MANIFEST_PATH,
        inert_root_path: "corpus_manifest.toml",
    },
];

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContractDiagnostic {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContractAuthorityEvidence {
    pub logical_name: String,
    pub canonical_path: String,
    pub canonical_sha256: String,
    pub inert_root_path: String,
    pub inert_root_sha256: String,
    pub inert_pointer_schema_version: String,
    pub inert_pointer_canonical_path: String,
    pub inert_pointer_canonical_sha256: String,
    pub root_disposition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpecializedContractConstantEvidence {
    pub owner: String,
    pub logical_name: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalContractAuthorityReport {
    pub schema_version: String,
    pub authorities: Vec<ContractAuthorityEvidence>,
    pub specialized_constants: Vec<SpecializedContractConstantEvidence>,
    pub diagnostics: Vec<ContractDiagnostic>,
}

impl CanonicalContractAuthorityReport {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InertContractPointerDocument {
    inert_contract_pointer: InertContractPointer,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InertContractPointer {
    schema_version: String,
    canonical_path: String,
    canonical_sha256: String,
    disposition: String,
    legacy_payload: String,
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
        diagnostics.extend(canonical_contract_authority_report(workspace_root).diagnostics);
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

/// Resolve a logical contract name through the sole canonical registry.
#[must_use]
pub fn canonical_contract_path(logical_name: &str) -> Option<&'static str> {
    CANONICAL_CONTRACT_AUTHORITIES
        .iter()
        .find(|authority| authority.logical_name == logical_name)
        .map(|authority| authority.canonical_path)
}

/// Build a stable, machine-readable proof for canonical and inert root paths.
#[must_use]
pub fn canonical_contract_authority_report(
    workspace_root: &Path,
) -> CanonicalContractAuthorityReport {
    let mut authorities = Vec::with_capacity(CANONICAL_CONTRACT_AUTHORITIES.len());
    let mut diagnostics = Vec::new();

    for authority in CANONICAL_CONTRACT_AUTHORITIES {
        let canonical_path = workspace_root.join(authority.canonical_path);
        let canonical_content = match fs::read_to_string(&canonical_path) {
            Ok(content) => Some(content),
            Err(error) => {
                diagnostics.push(ContractDiagnostic {
                    code: "missing_canonical_contract",
                    message: format!(
                        "{} canonical path '{}' is unreadable: {error}",
                        authority.logical_name,
                        canonical_path.display()
                    ),
                });
                None
            }
        };
        if let Some(content) = canonical_content.as_deref() {
            if content.trim().is_empty() {
                diagnostics.push(ContractDiagnostic {
                    code: "empty_canonical_contract",
                    message: format!(
                        "{} canonical path '{}' is empty",
                        authority.logical_name,
                        canonical_path.display()
                    ),
                });
            } else if let Err(error) = toml::from_str::<toml::Value>(content) {
                diagnostics.push(ContractDiagnostic {
                    code: "malformed_canonical_contract",
                    message: format!(
                        "{} canonical path '{}' is malformed TOML: {error}",
                        authority.logical_name,
                        canonical_path.display()
                    ),
                });
            }
        }
        let canonical_sha256 = canonical_content
            .as_deref()
            .map_or_else(String::new, |content| sha256_hex(content.as_bytes()));

        let root_path = workspace_root.join(authority.inert_root_path);
        let root_content = match fs::read_to_string(&root_path) {
            Ok(content) => Some(content),
            Err(error) => {
                diagnostics.push(ContractDiagnostic {
                    code: "missing_inert_root_pointer",
                    message: format!(
                        "{} root path '{}' is unreadable: {error}",
                        authority.logical_name,
                        root_path.display()
                    ),
                });
                None
            }
        };
        let inert_root_sha256 = root_content
            .as_deref()
            .map_or_else(String::new, |content| sha256_hex(content.as_bytes()));
        let pointer = match root_content.as_deref() {
            Some(content) if content.trim().is_empty() => {
                diagnostics.push(ContractDiagnostic {
                    code: "invalid_inert_root_pointer",
                    message: format!(
                        "{} root path '{}' is empty",
                        authority.logical_name,
                        root_path.display()
                    ),
                });
                None
            }
            Some(content) => match toml::from_str::<InertContractPointerDocument>(content) {
                Ok(document) => Some(document.inert_contract_pointer),
                Err(error) => {
                    diagnostics.push(ContractDiagnostic {
                        code: "invalid_inert_root_pointer",
                        message: format!(
                            "{} root path '{}' is not an inert pointer: {error}",
                            authority.logical_name,
                            root_path.display()
                        ),
                    });
                    None
                }
            },
            None => None,
        };
        let pointer_schema = pointer
            .as_ref()
            .map_or_else(String::new, |value| value.schema_version.clone());
        let pointer_path = pointer
            .as_ref()
            .map_or_else(String::new, |value| value.canonical_path.clone());
        let pointer_hash = pointer
            .as_ref()
            .map_or_else(String::new, |value| value.canonical_sha256.clone());
        let root_disposition = pointer
            .as_ref()
            .map_or_else(String::new, |value| value.disposition.clone());
        if let Some(pointer) = pointer {
            for (code, field, actual, expected) in [
                (
                    "inert_pointer_schema_mismatch",
                    "schema_version",
                    pointer.schema_version.as_str(),
                    INERT_CONTRACT_POINTER_SCHEMA_VERSION,
                ),
                (
                    "inert_pointer_path_mismatch",
                    "canonical_path",
                    pointer.canonical_path.as_str(),
                    authority.canonical_path,
                ),
                (
                    "inert_pointer_hash_mismatch",
                    "canonical_sha256",
                    pointer.canonical_sha256.as_str(),
                    canonical_sha256.as_str(),
                ),
                (
                    "inert_pointer_disposition_mismatch",
                    "disposition",
                    pointer.disposition.as_str(),
                    INERT_ROOT_DISPOSITION,
                ),
            ] {
                if actual != expected {
                    diagnostics.push(ContractDiagnostic {
                        code,
                        message: format!(
                            "{} inert pointer {field} '{}' does not match '{}'",
                            authority.logical_name, actual, expected
                        ),
                    });
                }
            }
            if pointer.legacy_payload.trim().is_empty() {
                diagnostics.push(ContractDiagnostic {
                    code: "inert_pointer_missing_legacy_payload",
                    message: format!(
                        "{} inert pointer does not preserve its historical payload",
                        authority.logical_name
                    ),
                });
            }
        }

        authorities.push(ContractAuthorityEvidence {
            logical_name: authority.logical_name.to_owned(),
            canonical_path: authority.canonical_path.to_owned(),
            canonical_sha256,
            inert_root_path: authority.inert_root_path.to_owned(),
            inert_root_sha256,
            inert_pointer_schema_version: pointer_schema,
            inert_pointer_canonical_path: pointer_path,
            inert_pointer_canonical_sha256: pointer_hash,
            root_disposition,
        });
    }

    let specialized_constants = [
        (
            "canonical_parity_contract::SQLITE_VERSION_CONTRACT_PATH",
            "sqlite_version_contract",
            SQLITE_VERSION_CONTRACT_PATH,
        ),
        (
            "canonical_parity_contract::SUPPORTED_SURFACE_MATRIX_PATH",
            "supported_surface_matrix",
            SUPPORTED_SURFACE_MATRIX_PATH,
        ),
        (
            "canonical_parity_contract::FEATURE_UNIVERSE_LEDGER_PATH",
            "feature_universe_ledger",
            FEATURE_UNIVERSE_LEDGER_PATH,
        ),
        (
            "parity_taxonomy::PARITY_TAXONOMY_CONTRACT_PATH",
            "parity_taxonomy",
            crate::parity_taxonomy::PARITY_TAXONOMY_CONTRACT_PATH,
        ),
        (
            "fixture_root_contract::DEFAULT_FIXTURE_ROOT_MANIFEST_PATH",
            "corpus_manifest",
            crate::fixture_root_contract::DEFAULT_FIXTURE_ROOT_MANIFEST_PATH,
        ),
    ]
    .into_iter()
    .map(|(owner, logical_name, path)| {
        if canonical_contract_path(logical_name) != Some(path) {
            diagnostics.push(ContractDiagnostic {
                code: "specialized_constant_path_mismatch",
                message: format!(
                    "specialized constant {owner} path '{path}' does not match registry logical name '{logical_name}'"
                ),
            });
        }
        SpecializedContractConstantEvidence {
            owner: owner.to_owned(),
            logical_name: logical_name.to_owned(),
            path: path.to_owned(),
        }
    })
    .collect();

    CanonicalContractAuthorityReport {
        schema_version: CONTRACT_AUTHORITY_REGISTRY_SCHEMA_VERSION.to_owned(),
        authorities,
        specialized_constants,
        diagnostics,
    }
}

impl CanonicalContractAuthorityReport {
    /// Emit one bounded summary; per-contract evidence stays in the report.
    pub fn emit_diagnostic(&self) {
        if self.is_valid() {
            tracing::info!(
                target: "fsqlite.contract_authority",
                registry_schema_version = %self.schema_version,
                authority_count = self.authorities.len(),
                diagnostic_count = 0,
                "canonical contract authorities validated"
            );
        } else {
            let first_failure = self
                .diagnostics
                .first()
                .map_or("unknown contract-authority failure", |value| {
                    value.message.as_str()
                });
            tracing::error!(
                target: "fsqlite.contract_authority",
                registry_schema_version = %self.schema_version,
                authority_count = self.authorities.len(),
                diagnostic_count = self.diagnostics.len(),
                first_failure,
                "canonical contract authority validation failed"
            );
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    crate::bytes_to_lower_hex(Sha256::digest(bytes))
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
    let candidate = canonical_path_for_bare_reference(path_text).map_or_else(
        || {
            let contract_candidate = workspace_root.join("docs/contracts").join(path_text);
            if root_candidate.exists() || Path::new(path_text).components().count() != 1 {
                root_candidate
            } else {
                contract_candidate
            }
        },
        |canonical| workspace_root.join(canonical),
    );
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

fn canonical_path_for_bare_reference(reference: &str) -> Option<&'static str> {
    if Path::new(reference).components().count() != 1 {
        return None;
    }
    CANONICAL_CONTRACT_AUTHORITIES
        .iter()
        .find(|authority| {
            Path::new(authority.canonical_path)
                .file_name()
                .and_then(|name| name.to_str())
                == Some(reference)
        })
        .map(|authority| authority.canonical_path)
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

    fn write_authority_layout(
        root: &Path,
        missing_canonical_index: Option<usize>,
        malformed_canonical_index: Option<usize>,
        wrong_pointer_index: Option<usize>,
    ) {
        fs::create_dir_all(root.join("docs/contracts")).expect("create contract directory");
        for (index, authority) in CANONICAL_CONTRACT_AUTHORITIES.iter().enumerate() {
            let canonical_content = if malformed_canonical_index == Some(index) {
                "not = [valid"
            } else {
                "[meta]\nschema_version = \"test\"\n"
            };
            if missing_canonical_index != Some(index) {
                fs::write(root.join(authority.canonical_path), canonical_content)
                    .expect("write canonical contract");
            }
            let canonical_hash = sha256_hex(canonical_content.as_bytes());
            let pointer_path = if wrong_pointer_index == Some(index) {
                "docs/contracts/wrong.toml"
            } else {
                authority.canonical_path
            };
            let pointer = format!(
                "[inert_contract_pointer]\n\
                 schema_version = \"{INERT_CONTRACT_POINTER_SCHEMA_VERSION}\"\n\
                 canonical_path = \"{pointer_path}\"\n\
                 canonical_sha256 = \"{canonical_hash}\"\n\
                 disposition = \"{INERT_ROOT_DISPOSITION}\"\n\
                 legacy_payload = '''historical contract payload'''\n"
            );
            fs::write(root.join(authority.inert_root_path), pointer)
                .expect("write inert root pointer");
        }
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
    fn authority_registry_is_stable_complete_and_delegated() {
        assert_eq!(CANONICAL_CONTRACT_AUTHORITIES.len(), 5);
        let logical_names = CANONICAL_CONTRACT_AUTHORITIES
            .iter()
            .map(|authority| authority.logical_name)
            .collect::<BTreeSet<_>>();
        let canonical_paths = CANONICAL_CONTRACT_AUTHORITIES
            .iter()
            .map(|authority| authority.canonical_path)
            .collect::<BTreeSet<_>>();
        assert_eq!(logical_names.len(), 5);
        assert_eq!(canonical_paths.len(), 5);
        assert!(
            CANONICAL_CONTRACT_AUTHORITIES
                .iter()
                .all(|authority| authority.canonical_path.starts_with("docs/contracts/"))
        );
        assert_eq!(
            canonical_contract_path("corpus_manifest"),
            Some(CORPUS_MANIFEST_PATH)
        );
        assert_eq!(
            crate::fixture_root_contract::DEFAULT_FIXTURE_ROOT_MANIFEST_PATH,
            CORPUS_MANIFEST_PATH
        );
        assert_eq!(
            crate::parity_taxonomy::PARITY_TAXONOMY_CONTRACT_PATH,
            PARITY_TAXONOMY_PATH
        );
    }

    #[test]
    fn workspace_authority_report_is_valid_stable_and_machine_readable() {
        let report = canonical_contract_authority_report(&workspace_root());
        assert!(report.is_valid(), "{:?}", report.diagnostics);
        assert_eq!(report.authorities.len(), 5);
        assert!(report.authorities.iter().all(|authority| {
            authority.canonical_sha256 == authority.inert_pointer_canonical_sha256
                && authority.canonical_path == authority.inert_pointer_canonical_path
                && authority.root_disposition == INERT_ROOT_DISPOSITION
                && authority.inert_root_sha256.len() == 64
        }));
        let first = serde_json::to_string(&report).expect("serialize authority report");
        let second = serde_json::to_string(&report).expect("reserialize authority report");
        assert_eq!(first, second);
        println!("FSQLITE_CONTRACT_AUTHORITY_REPORT={first}");
        report.emit_diagnostic();
    }

    #[test]
    fn bare_contract_references_never_resolve_to_root_pointers() {
        for authority in CANONICAL_CONTRACT_AUTHORITIES {
            let file_name = Path::new(authority.canonical_path)
                .file_name()
                .and_then(|name| name.to_str())
                .expect("canonical contract file name");
            assert_eq!(
                canonical_path_for_bare_reference(file_name),
                Some(authority.canonical_path)
            );
        }
        assert_eq!(canonical_path_for_bare_reference("README.md"), None);
    }

    #[test]
    fn authority_report_rejects_missing_malformed_and_wrong_path_contracts() {
        let missing = tempfile::tempdir().expect("missing layout");
        write_authority_layout(missing.path(), Some(0), None, None);
        let report = canonical_contract_authority_report(missing.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "missing_canonical_contract"
                && diagnostic.message.contains("supported_surface_matrix")
        }));
        let mut reference_diagnostics = Vec::new();
        validate_reference_exists(
            "supported_surface_matrix.toml#SURF-SQL-CORE-001",
            missing.path(),
            &mut reference_diagnostics,
        );
        assert!(reference_diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "missing_reference_path"
                && diagnostic
                    .message
                    .contains("docs/contracts/supported_surface_matrix.toml")
        }));

        let malformed = tempfile::tempdir().expect("malformed layout");
        write_authority_layout(malformed.path(), None, Some(1), None);
        let report = canonical_contract_authority_report(malformed.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "malformed_canonical_contract"
                && diagnostic.message.contains("feature_universe_ledger")
        }));

        let empty = tempfile::tempdir().expect("empty layout");
        write_authority_layout(empty.path(), None, None, None);
        let surface_authority = CANONICAL_CONTRACT_AUTHORITIES
            .first()
            .copied()
            .expect("supported surface authority");
        fs::write(empty.path().join(surface_authority.canonical_path), "")
            .expect("empty canonical contract");
        let report = canonical_contract_authority_report(empty.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "empty_canonical_contract"
                && diagnostic.message.contains("supported_surface_matrix")
        }));
        fs::write(empty.path().join(surface_authority.inert_root_path), "")
            .expect("empty inert root pointer");
        let report = canonical_contract_authority_report(empty.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_inert_root_pointer"
                && diagnostic.message.contains("supported_surface_matrix")
        }));

        let wrong_path = tempfile::tempdir().expect("wrong-path layout");
        write_authority_layout(wrong_path.path(), None, None, Some(2));
        let report = canonical_contract_authority_report(wrong_path.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "inert_pointer_path_mismatch"
                && diagnostic.message.contains("parity_taxonomy")
        }));

        let drifted = tempfile::tempdir().expect("drifted layout");
        write_authority_layout(drifted.path(), None, None, None);
        let authority = CANONICAL_CONTRACT_AUTHORITIES
            .get(3)
            .copied()
            .expect("SQLite version authority");
        let pointer_path = drifted.path().join(authority.inert_root_path);
        let pointer = fs::read_to_string(&pointer_path)
            .expect("read pointer")
            .replace(
                &sha256_hex(b"[meta]\nschema_version = \"test\"\n"),
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            );
        fs::write(&pointer_path, pointer).expect("write drifted pointer");
        let report = canonical_contract_authority_report(drifted.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "inert_pointer_hash_mismatch"
                && diagnostic.message.contains("sqlite_version_contract")
        }));

        let malformed_pointer = tempfile::tempdir().expect("malformed-pointer layout");
        write_authority_layout(malformed_pointer.path(), None, None, None);
        let authority = CANONICAL_CONTRACT_AUTHORITIES
            .get(4)
            .copied()
            .expect("corpus manifest authority");
        fs::write(
            malformed_pointer.path().join(authority.inert_root_path),
            "[inert_contract_pointer]\nunknown = true\n",
        )
        .expect("write malformed root pointer");
        let report = canonical_contract_authority_report(malformed_pointer.path());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_inert_root_pointer"
                && diagnostic.message.contains("corpus_manifest")
        }));
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
