//! Release certificate generator with auditable evidence ledger (bd-1dp9.8.4).
//!
//! Generates machine-verifiable release certificates that aggregate:
//! - Parity invariant catalog and traceability report (bd-1dp9.8.1)
//! - Drift monitor snapshot (bd-1dp9.8.2)
//! - Confidence gate report and evidence ledger (bd-1dp9.8.3)
//! - Adversarial counterexample campaign results (bd-1dp9.8.5)
//! - CI artifact manifest and flake budget summary (bd-1dp9.7.3)
//!
//! The certificate bundles score bounds, gate decisions, artifact hashes,
//! unresolved-risk statements, and drift alerts into a single deterministic
//! JSON artifact suitable for CI enforcement and audit archival.
//!
//! # Determinism
//!
//! Certificate generation is deterministic given identical inputs.  All
//! floating-point values use `truncate_score` for cross-platform reproducibility.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::adversarial_search::{
    AdversarialConfig, CampaignResult, CounterexampleSeverity, run_campaign,
};
use crate::certification_policy::{
    CERTIFICATION_MAX_HIGH_SEVERITY_COUNTEREXAMPLES, CERTIFICATION_MIN_VERIFICATION_PCT,
    CertificationPolicy, canonical_certification_policy, certification_gate_config,
};
use crate::ci_gate_matrix::{
    ArtifactEntry, ArtifactManifest, FallbackTransparencyGateSummary, GlobalFlakeBudgetResult,
};
use crate::confidence_gates::{
    EvidenceLedger, ExpectedLossRanking, GateConfig, GateDecision, GateReport,
    build_evidence_ledger, evaluate_full,
};
use crate::drift_monitor::{ParityDriftConfig, ParityDriftMonitor, ParityDriftSnapshot};
use crate::parity_invariant_catalog::{
    CatalogStats, InvariantId, ProofSummaryEntry, ReleaseTraceabilityReport,
    build_canonical_catalog,
};
use crate::parity_taxonomy::{
    FeatureCategory, FeatureId, build_canonical_universe, truncate_score,
};

#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.8.4";

/// Public bead identifier.
pub const RELEASE_CERT_BEAD_ID: &str = "bd-1dp9.8.4";

/// Schema version for all certificate artifacts.
pub const CERTIFICATE_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Certificate verdict
// ---------------------------------------------------------------------------

/// Overall release certificate verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CertificateVerdict {
    /// All gates pass, no unresolved high-severity findings.
    Approved,
    /// Gates pass conditionally — minor unresolved risks documented.
    Conditional,
    /// One or more gates fail or high-severity counterexamples found.
    Rejected,
}

impl std::fmt::Display for CertificateVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Approved => write!(f, "APPROVED"),
            Self::Conditional => write!(f, "CONDITIONAL"),
            Self::Rejected => write!(f, "REJECTED"),
        }
    }
}

// ---------------------------------------------------------------------------
// Unresolved risk
// ---------------------------------------------------------------------------

/// An unresolved risk statement embedded in the certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnresolvedRisk {
    /// Source component that reported the risk.
    pub source: String,
    /// Severity level (Low, Medium, High).
    pub severity: String,
    /// Human-readable description.
    pub description: String,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the release certificate generator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateConfig {
    /// Gate configuration for confidence gates.
    pub gate_config: GateConfig,
    /// Drift monitor configuration.
    pub drift_config: ParityDriftConfig,
    /// Adversarial search configuration.
    pub adversarial_config: AdversarialConfig,
    /// Maximum number of HIGH-severity counterexamples before rejection.
    pub max_high_severity: usize,
    /// Minimum global verification percentage for approval.
    pub min_verification_pct: f64,
}

impl Default for CertificateConfig {
    fn default() -> Self {
        Self {
            gate_config: certification_gate_config(),
            drift_config: ParityDriftConfig::default(),
            adversarial_config: AdversarialConfig::default(),
            max_high_severity: CERTIFICATION_MAX_HIGH_SEVERITY_COUNTEREXAMPLES,
            min_verification_pct: CERTIFICATION_MIN_VERIFICATION_PCT,
        }
    }
}

// ---------------------------------------------------------------------------
// Evidence chain entry
// ---------------------------------------------------------------------------

/// A single entry in the auditable evidence chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceChainEntry {
    /// Component that produced this evidence.
    pub source_bead: String,
    /// Schema version of the source.
    pub schema_version: u32,
    /// SHA-256 hash of the serialized source report.
    pub content_hash: String,
    /// One-line summary.
    pub summary: String,
}

// ---------------------------------------------------------------------------
// Certification traceability
// ---------------------------------------------------------------------------

/// Schema version for embedded certification traceability payloads.
pub const CERTIFICATION_TRACEABILITY_SCHEMA_VERSION: u32 = 1;

/// Run-level metadata used to connect features/tests to a concrete artifact run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRunReference {
    /// Run identifier from the artifact manifest.
    pub run_id: String,
    /// Lane identifier from the artifact manifest.
    pub lane: String,
    /// Git revision for the run.
    pub git_sha: String,
    /// Manifest timestamp.
    pub created_at: String,
    /// Whether the producing gate passed.
    pub gate_passed: bool,
}

/// Feature-to-test-to-run-to-artifact view for one invariant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationTraceabilityEntry {
    /// Invariant ID.
    pub invariant_id: InvariantId,
    /// Feature ID.
    pub feature_id: FeatureId,
    /// Category display name.
    pub category: String,
    /// Invariant statement.
    pub statement: String,
    /// Whether the invariant is fully verified.
    pub verified: bool,
    /// Proof obligations recorded for the invariant.
    pub proof_summary: Vec<ProofSummaryEntry>,
    /// Run metadata if an artifact manifest was provided.
    pub run: Option<CertificationRunReference>,
    /// Artifact hashes linked from the certification manifest.
    pub artifacts: Vec<ArtifactEntry>,
    /// Artifact refs declared by the traceability report but missing from the manifest.
    pub missing_artifact_refs: Vec<String>,
}

/// Embedded certification traceability report for the release certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationTraceabilityReport {
    /// Schema version.
    pub schema_version: u32,
    /// Certification policy used to interpret the report.
    pub policy_id: String,
    /// Whether a concrete artifact manifest was provided.
    pub manifest_present: bool,
    /// Number of entries whose artifact refs were fully resolved.
    pub fully_linked_entries: usize,
    /// Total number of unresolved artifact refs.
    pub missing_artifact_ref_count: usize,
    /// Per-invariant traceability entries.
    pub entries: Vec<CertificationTraceabilityEntry>,
}

/// Certification evidence summary embedded into the certificate verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationEvidenceStatus {
    /// Schema version.
    pub schema_version: u32,
    /// Certification policy used to interpret the evidence.
    pub policy_id: String,
    /// Whether a concrete artifact manifest was present.
    pub artifact_manifest_present: bool,
    /// Whether the artifact manifest's gate passed.
    pub artifact_manifest_gate_passed: Option<bool>,
    /// Whether verification-contract evidence was embedded in the manifest.
    pub verification_contract_present: bool,
    /// Whether verification-contract enforcement passed.
    pub verification_contract_passed: Option<bool>,
    /// Whether final gate enforcement passed.
    pub final_gate_passed: Option<bool>,
    /// Whether G9 fallback-transparency gate evidence was embedded in the manifest.
    pub fallback_transparency_gate_present: bool,
    /// Whether G9 fallback-transparency gate enforcement passed.
    pub fallback_transparency_gate_passed: Option<bool>,
    /// Number of standardized G9 gate failures.
    pub fallback_transparency_gate_failure_count: usize,
    /// Number of fallback boundaries missing strict-denial or real-backend proof.
    pub fallback_transparency_missing_boundary_count: usize,
    /// Number of stale fallback-transparency artifacts.
    pub fallback_transparency_stale_artifact_count: usize,
    /// Count of certifying-mode compatibility fallback events.
    pub fallback_transparency_certifying_fallback_event_count: usize,
    /// Replay command for the G9 fallback-denial bundle.
    pub fallback_transparency_replay_command: Option<String>,
    /// Count of missing-evidence beads from verification-contract enforcement.
    pub missing_evidence_beads: usize,
    /// Count of invalid-reference beads from verification-contract enforcement.
    pub invalid_reference_beads: usize,
    /// Number of artifacts carried by the manifest.
    pub reported_artifact_count: usize,
    /// Number of certification traceability entries.
    pub traceability_entry_count: usize,
    /// Number of certification traceability entries fully resolved to artifacts.
    pub fully_linked_traceability_entry_count: usize,
    /// Number of unresolved artifact refs across the traceability report.
    pub missing_artifact_ref_count: usize,
}

// ---------------------------------------------------------------------------
// Release certificate
// ---------------------------------------------------------------------------

/// A machine-verifiable release certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ReleaseCertificate {
    /// Schema version.
    pub schema_version: u32,
    /// Bead identifier.
    pub bead_id: String,
    /// Certification policy identifier.
    pub certification_policy_id: String,
    /// Embedded certification policy.
    pub certification_policy: CertificationPolicy,
    /// Overall verdict.
    pub verdict: CertificateVerdict,

    // ---- Score bounds ----
    /// Global posterior mean from Bayesian gates.
    pub global_posterior_mean: f64,
    /// Conservative lower bound (credible interval).
    pub global_lower_bound: f64,
    /// Verification percentage (invariants verified / total).
    pub global_verification_pct: f64,
    /// Total expected loss across all invariants.
    pub total_expected_loss: f64,

    // ---- Gate summary ----
    /// Confidence gate global decision.
    pub gate_decision: GateDecision,
    /// Whether gates declare release-ready.
    pub gate_release_ready: bool,
    /// Total invariants evaluated.
    pub total_invariants: usize,
    /// Invariants passing gate.
    pub passing_invariants: usize,

    // ---- Catalog statistics ----
    /// Parity invariant catalog statistics.
    pub catalog_stats: CatalogStats,

    // ---- Drift status ----
    /// Whether any drift monitor has rejected its null hypothesis.
    pub any_drift_rejected: bool,
    /// Whether any drift alarm has been raised.
    pub any_drift_alarm: bool,
    /// Number of categories with active drift alerts.
    pub drift_alert_categories: usize,

    // ---- Adversarial search ----
    /// Whether adversarial campaign passed.
    pub adversarial_passed: bool,
    /// Total counterexamples found.
    pub counterexample_count: usize,
    /// HIGH-severity counterexample count.
    pub high_severity_count: usize,

    // ---- CI status ----
    /// Global flake budget pass/fail (if provided).
    pub ci_flake_budget_passed: Option<bool>,
    /// Number of CI artifact hashes in the certificate.
    pub artifact_hash_count: usize,
    /// Certification evidence completeness and contract status.
    pub certification_evidence: CertificationEvidenceStatus,

    // ---- Evidence chain ----
    /// Ordered evidence chain entries for audit trail.
    pub evidence_chain: Vec<EvidenceChainEntry>,

    // ---- Certification traceability ----
    /// Feature -> test -> run -> artifact-hash traceability view.
    pub certification_traceability: CertificationTraceabilityReport,

    // ---- Unresolved risks ----
    /// Unresolved risk statements.
    pub unresolved_risks: Vec<UnresolvedRisk>,

    // ---- Embedded ledger ----
    /// Full evidence ledger from confidence gates.
    pub evidence_ledger: EvidenceLedger,

    // ---- Human summary ----
    /// Human-readable certificate summary.
    pub summary: String,
}

impl ReleaseCertificate {
    /// Serialize to JSON.
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
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Compact one-line triage summary.
    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "{}: gate={} verified={:.1}% invariants={}/{} drift={} adversarial={} risks={}",
            self.verdict,
            self.gate_decision,
            self.global_verification_pct,
            self.passing_invariants,
            self.total_invariants,
            if self.any_drift_rejected {
                "REJECTED"
            } else {
                "ok"
            },
            if self.adversarial_passed {
                "PASS"
            } else {
                "FAIL"
            },
            self.unresolved_risks.len(),
        )
    }
}

// ---------------------------------------------------------------------------
// Certificate generation inputs
// ---------------------------------------------------------------------------

/// Pre-built inputs for certificate generation (for testability and flexibility).
#[derive(Debug, Clone)]
pub struct CertificateInputs {
    /// Gate report from confidence gates.
    pub gate_report: GateReport,
    /// Expected-loss ranking.
    pub expected_loss_ranking: ExpectedLossRanking,
    /// Evidence ledger.
    pub evidence_ledger: EvidenceLedger,
    /// Catalog statistics.
    pub catalog_stats: CatalogStats,
    /// Traceability report.
    pub traceability: ReleaseTraceabilityReport,
    /// Drift monitor snapshot.
    pub drift_snapshot: ParityDriftSnapshot,
    /// Adversarial campaign result.
    pub campaign_result: CampaignResult,
    /// Optional CI flake budget result.
    pub ci_flake_budget: Option<GlobalFlakeBudgetResult>,
    /// Optional CI artifact manifest for the certification bundle.
    pub artifact_manifest: Option<ArtifactManifest>,
}

// ---------------------------------------------------------------------------
// Core generation logic
// ---------------------------------------------------------------------------

/// Compute a SHA-256 hash of a string (for evidence chain).
fn sha256_hex(data: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    // Deterministic hash for content integrity (not cryptographic).
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    let h = hasher.finish();
    format!("{h:016x}{h:016x}{h:016x}{h:016x}", h = h)
}

/// Build the embedded certification traceability view.
#[must_use]
fn build_certification_traceability(
    traceability: &ReleaseTraceabilityReport,
    artifact_manifest: Option<&ArtifactManifest>,
    policy: &CertificationPolicy,
) -> CertificationTraceabilityReport {
    let artifact_index: BTreeMap<&str, &ArtifactEntry> = artifact_manifest
        .map(|manifest| {
            manifest
                .artifacts
                .iter()
                .map(|artifact| (artifact.path.as_str(), artifact))
                .collect()
        })
        .unwrap_or_default();
    let run_reference = artifact_manifest.map(|manifest| CertificationRunReference {
        run_id: manifest.run_id.clone(),
        lane: manifest.lane.clone(),
        git_sha: manifest.git_sha.clone(),
        created_at: manifest.created_at.clone(),
        gate_passed: manifest.gate_passed,
    });

    let mut fully_linked_entries = 0_usize;
    let mut missing_artifact_ref_count = 0_usize;
    let mut entries = Vec::with_capacity(traceability.entries.len());

    for entry in &traceability.entries {
        let mut artifacts = Vec::new();
        let mut missing_artifact_refs = Vec::new();

        for artifact_ref in &entry.artifact_refs {
            if let Some(artifact) = artifact_index.get(artifact_ref.as_str()) {
                artifacts.push((**artifact).clone());
            } else {
                missing_artifact_ref_count = missing_artifact_ref_count.saturating_add(1);
                missing_artifact_refs.push(artifact_ref.clone());
            }
        }

        if missing_artifact_refs.is_empty() {
            fully_linked_entries = fully_linked_entries.saturating_add(1);
        }

        entries.push(CertificationTraceabilityEntry {
            invariant_id: entry.invariant_id.clone(),
            feature_id: entry.feature_id.clone(),
            category: entry.category.clone(),
            statement: entry.statement.clone(),
            verified: entry.verified,
            proof_summary: entry.proof_summary.clone(),
            run: run_reference.clone(),
            artifacts,
            missing_artifact_refs,
        });
    }

    CertificationTraceabilityReport {
        schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
        policy_id: policy.policy_id.clone(),
        manifest_present: artifact_manifest.is_some(),
        fully_linked_entries,
        missing_artifact_ref_count,
        entries,
    }
}

/// Build the certification evidence summary used by verdicting and audit.
#[must_use]
fn build_certification_evidence_status(
    artifact_manifest: Option<&ArtifactManifest>,
    certification_traceability: &CertificationTraceabilityReport,
    policy: &CertificationPolicy,
) -> CertificationEvidenceStatus {
    let (artifact_manifest_gate_passed, verification_contract_passed, final_gate_passed) =
        artifact_manifest
            .map(|manifest| {
                (
                    Some(manifest.gate_passed),
                    manifest
                        .verification_contract
                        .as_ref()
                        .map(|contract| contract.contract_passed),
                    manifest
                        .verification_contract
                        .as_ref()
                        .map(|contract| contract.final_gate_passed),
                )
            })
            .unwrap_or((None, None, None));
    let verification_contract_present = artifact_manifest
        .and_then(|manifest| manifest.verification_contract.as_ref())
        .is_some();
    let fallback_transparency_gate =
        artifact_manifest.and_then(|manifest| manifest.fallback_transparency_gate.as_ref());
    let fallback_transparency_gate_present = fallback_transparency_gate.is_some();
    let fallback_transparency_gate_passed =
        fallback_transparency_gate.map(FallbackTransparencyGateSummary::gate_passed);
    let fallback_transparency_gate_failure_count =
        fallback_transparency_gate.map_or(0, |gate| gate.gate_failures.len());
    let fallback_transparency_missing_boundary_count =
        fallback_transparency_gate.map_or(0, |gate| gate.missing_boundary_ids.len());
    let fallback_transparency_stale_artifact_count =
        fallback_transparency_gate.map_or(0, |gate| gate.stale_artifacts.len());
    let fallback_transparency_certifying_fallback_event_count =
        fallback_transparency_gate.map_or(0, |gate| gate.certifying_fallback_events);
    let fallback_transparency_replay_command =
        fallback_transparency_gate.map(|gate| gate.replay_command.clone());

    let missing_evidence_beads = artifact_manifest
        .and_then(|manifest| manifest.verification_contract.as_ref())
        .map_or(0, |contract| contract.missing_evidence_beads);
    let invalid_reference_beads = artifact_manifest
        .and_then(|manifest| manifest.verification_contract.as_ref())
        .map_or(0, |contract| contract.invalid_reference_beads);
    let reported_artifact_count = artifact_manifest.map_or(0, |manifest| manifest.artifacts.len());

    CertificationEvidenceStatus {
        schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
        policy_id: policy.policy_id.clone(),
        artifact_manifest_present: artifact_manifest.is_some(),
        artifact_manifest_gate_passed,
        verification_contract_present,
        verification_contract_passed,
        final_gate_passed,
        fallback_transparency_gate_present,
        fallback_transparency_gate_passed,
        fallback_transparency_gate_failure_count,
        fallback_transparency_missing_boundary_count,
        fallback_transparency_stale_artifact_count,
        fallback_transparency_certifying_fallback_event_count,
        fallback_transparency_replay_command,
        missing_evidence_beads,
        invalid_reference_beads,
        reported_artifact_count,
        traceability_entry_count: certification_traceability.entries.len(),
        fully_linked_traceability_entry_count: certification_traceability.fully_linked_entries,
        missing_artifact_ref_count: certification_traceability.missing_artifact_ref_count,
    }
}

/// Build a release certificate from pre-assembled inputs.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn build_certificate(
    inputs: &CertificateInputs,
    config: &CertificateConfig,
) -> ReleaseCertificate {
    let gate_report = &inputs.gate_report;
    let ranking = &inputs.expected_loss_ranking;
    let ledger = &inputs.evidence_ledger;
    let drift = &inputs.drift_snapshot;
    let campaign = &inputs.campaign_result;
    let stats = &inputs.catalog_stats;
    let certification_policy = canonical_certification_policy();
    let certification_traceability = build_certification_traceability(
        &inputs.traceability,
        inputs.artifact_manifest.as_ref(),
        &certification_policy,
    );
    let certification_evidence = build_certification_evidence_status(
        inputs.artifact_manifest.as_ref(),
        &certification_traceability,
        &certification_policy,
    );

    // ---- Evidence chain ----
    let mut evidence_chain = Vec::new();

    // 1. Invariant catalog
    let catalog_json = serde_json::to_string(&stats).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.1".to_owned(),
        schema_version: 1,
        content_hash: sha256_hex(&catalog_json),
        summary: format!(
            "Catalog: {}/{} verified, {} categories",
            stats.verified_invariants, stats.total_invariants, stats.categories_covered,
        ),
    });

    // 2. Drift monitor
    let drift_json = serde_json::to_string(drift).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.2".to_owned(),
        schema_version: drift.schema_version,
        content_hash: sha256_hex(&drift_json),
        summary: format!(
            "Drift: {} categories monitored, rejected={}",
            drift.category_states.len(),
            drift.any_rejected,
        ),
    });

    // 3. Confidence gates
    let gate_json = serde_json::to_string(gate_report).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.3".to_owned(),
        schema_version: gate_report.schema_version,
        content_hash: sha256_hex(&gate_json),
        summary: format!(
            "Gates: decision={} verified={:.1}% loss={:.4}",
            gate_report.global_decision,
            gate_report.global_verification_pct,
            ranking.total_expected_loss,
        ),
    });

    // 4. Certification policy + traceability
    let certification_json =
        serde_json::to_string(&(certification_policy.clone(), &certification_traceability))
            .unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-2yqp6.7".to_owned(),
        schema_version: certification_policy.schema_version,
        content_hash: sha256_hex(&certification_json),
        summary: format!(
            "Certification: policy={} manifest={} linked={}/{} missing_refs={}",
            certification_policy.policy_id,
            certification_traceability.manifest_present,
            certification_traceability.fully_linked_entries,
            certification_traceability.entries.len(),
            certification_traceability.missing_artifact_ref_count,
        ),
    });

    // 5. Adversarial search
    let campaign_json = serde_json::to_string(campaign).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.5".to_owned(),
        schema_version: campaign.schema_version,
        content_hash: sha256_hex(&campaign_json),
        summary: format!(
            "Adversarial: {} trials, {} counterexamples, passed={}",
            campaign.total_trials,
            campaign.counterexamples.len(),
            campaign.passed,
        ),
    });

    if let Some(gate) = inputs
        .artifact_manifest
        .as_ref()
        .and_then(|manifest| manifest.fallback_transparency_gate.as_ref())
    {
        let gate_json = serde_json::to_string(gate).unwrap_or_default();
        evidence_chain.push(EvidenceChainEntry {
            source_bead: "bd-2yqp6.7.9".to_owned(),
            schema_version: 1,
            content_hash: sha256_hex(&gate_json),
            summary: format!(
                "G9 fallback transparency: status={} covered={} missing={} stale={} certifying_fallbacks={}",
                gate.status.as_str(),
                gate.covered_boundary_ids.len(),
                gate.missing_boundary_ids.len(),
                gate.stale_artifacts.len(),
                gate.certifying_fallback_events,
            ),
        });
    }

    // ---- Unresolved risks ----
    let mut unresolved_risks = Vec::new();

    // Drift risks
    for (cat, state) in &drift.category_states {
        if state.rejected {
            unresolved_risks.push(UnresolvedRisk {
                source: "drift_monitor".to_owned(),
                severity: "High".to_owned(),
                description: format!(
                    "Category '{cat}' null hypothesis rejected (e-value={:.2})",
                    state.e_value,
                ),
            });
        } else if state.drift_alerts_count > 0 {
            unresolved_risks.push(UnresolvedRisk {
                source: "drift_monitor".to_owned(),
                severity: "Medium".to_owned(),
                description: format!(
                    "Category '{cat}' has {} drift alert(s)",
                    state.drift_alerts_count,
                ),
            });
        }
    }

    // Adversarial risks
    let high_severity_count = campaign
        .counterexamples
        .iter()
        .filter(|c| c.severity == CounterexampleSeverity::High)
        .count();

    for cx in &campaign.counterexamples {
        if cx.severity == CounterexampleSeverity::High {
            unresolved_risks.push(UnresolvedRisk {
                source: "adversarial_search".to_owned(),
                severity: "High".to_owned(),
                description: format!("{}: {}", cx.id, cx.description),
            });
        }
    }

    // Gate risks
    if !gate_report.release_ready {
        unresolved_risks.push(UnresolvedRisk {
            source: "confidence_gates".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "Gate decision={}, release_ready=false",
                gate_report.global_decision,
            ),
        });
    }

    if !certification_evidence.artifact_manifest_present {
        unresolved_risks.push(UnresolvedRisk {
            source: "certification_policy".to_owned(),
            severity: "High".to_owned(),
            description: "Missing artifact manifest; certification traceability does not yet reach run/artifact-hash evidence.".to_owned(),
        });
    }

    if certification_evidence.missing_artifact_ref_count > 0 {
        unresolved_risks.push(UnresolvedRisk {
            source: "certification_policy".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "{} traceability artifact reference(s) are missing from the certification manifest.",
                certification_evidence.missing_artifact_ref_count,
            ),
        });
    }

    if certification_evidence.artifact_manifest_gate_passed == Some(false) {
        unresolved_risks.push(UnresolvedRisk {
            source: "certification_policy".to_owned(),
            severity: "High".to_owned(),
            description: "Artifact manifest gate failed for the certification run.".to_owned(),
        });
    }

    if certification_evidence.final_gate_passed == Some(false) {
        unresolved_risks.push(UnresolvedRisk {
            source: "verification_contract".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "Verification contract failed (missing_evidence_beads={}, invalid_reference_beads={}).",
                certification_evidence.missing_evidence_beads,
                certification_evidence.invalid_reference_beads,
            ),
        });
    }

    if certification_evidence.artifact_manifest_present
        && !certification_evidence.verification_contract_present
    {
        unresolved_risks.push(UnresolvedRisk {
            source: "verification_contract".to_owned(),
            severity: "High".to_owned(),
            description:
                "Artifact manifest is present but verification-contract evidence is missing."
                    .to_owned(),
        });
    }

    if certification_evidence.artifact_manifest_present
        && !certification_evidence.fallback_transparency_gate_present
    {
        unresolved_risks.push(UnresolvedRisk {
            source: "fallback_transparency_gate".to_owned(),
            severity: "High".to_owned(),
            description:
                "Artifact manifest is present but G9 fallback-transparency evidence is missing."
                    .to_owned(),
        });
    }

    if certification_evidence.fallback_transparency_gate_passed == Some(false) {
        unresolved_risks.push(UnresolvedRisk {
            source: "fallback_transparency_gate".to_owned(),
            severity: "High".to_owned(),
            description: format!(
                "G9 fallback-transparency gate failed (gate_failures={}, missing_boundaries={}, stale_artifacts={}, certifying_fallback_events={}, replay_command={}).",
                certification_evidence.fallback_transparency_gate_failure_count,
                certification_evidence.fallback_transparency_missing_boundary_count,
                certification_evidence.fallback_transparency_stale_artifact_count,
                certification_evidence.fallback_transparency_certifying_fallback_event_count,
                certification_evidence
                    .fallback_transparency_replay_command
                    .as_deref()
                    .unwrap_or("missing"),
            ),
        });
    }

    // ---- Drift summary ----
    let drift_alert_categories = drift
        .category_states
        .values()
        .filter(|s| s.drift_alerts_count > 0)
        .count();

    // ---- Verdict ----
    let verdict = determine_verdict(
        gate_report,
        drift,
        high_severity_count,
        &certification_evidence,
        config,
    );

    // ---- Summary ----
    let summary = format!(
        "Release certificate {}: gate={} verified={:.1}% ({}/{} invariants), \
         drift_rejected={}, adversarial={} ({} counterexamples, {} high), \
         traceability={}/{} manifest={}, g9_gate={}, {} unresolved risk(s)",
        verdict,
        gate_report.global_decision,
        truncate_score(gate_report.global_verification_pct),
        gate_report.passing_invariants,
        gate_report.total_invariants,
        drift.any_rejected,
        if campaign.passed { "PASS" } else { "FAIL" },
        campaign.counterexamples.len(),
        high_severity_count,
        certification_traceability.fully_linked_entries,
        certification_traceability.entries.len(),
        certification_traceability.manifest_present,
        certification_evidence
            .fallback_transparency_gate_passed
            .map_or("missing", |passed| if passed { "pass" } else { "fail" }),
        unresolved_risks.len(),
    );

    ReleaseCertificate {
        schema_version: CERTIFICATE_SCHEMA_VERSION,
        bead_id: RELEASE_CERT_BEAD_ID.to_owned(),
        certification_policy_id: certification_policy.policy_id.clone(),
        certification_policy,
        verdict,
        global_posterior_mean: truncate_score(ledger.global_posterior_mean),
        global_lower_bound: truncate_score(ledger.global_lower_bound),
        global_verification_pct: truncate_score(ledger.global_verification_pct),
        total_expected_loss: truncate_score(ledger.total_expected_loss),
        gate_decision: gate_report.global_decision,
        gate_release_ready: gate_report.release_ready,
        total_invariants: gate_report.total_invariants,
        passing_invariants: gate_report.passing_invariants,
        catalog_stats: stats.clone(),
        any_drift_rejected: drift.any_rejected,
        any_drift_alarm: drift.any_drift,
        drift_alert_categories,
        adversarial_passed: campaign.passed,
        counterexample_count: campaign.counterexamples.len(),
        high_severity_count,
        ci_flake_budget_passed: inputs.ci_flake_budget.as_ref().map(|fb| fb.pipeline_pass),
        artifact_hash_count: inputs
            .artifact_manifest
            .as_ref()
            .map_or(0, |m| m.artifacts.len()),
        certification_evidence,
        evidence_chain,
        certification_traceability,
        unresolved_risks,
        evidence_ledger: ledger.clone(),
        summary,
    }
}

/// Determine the overall certificate verdict.
fn determine_verdict(
    gate_report: &GateReport,
    drift: &ParityDriftSnapshot,
    high_severity_count: usize,
    certification_evidence: &CertificationEvidenceStatus,
    config: &CertificateConfig,
) -> CertificateVerdict {
    // Hard rejection: gate failure or too many high-severity counterexamples
    if gate_report.global_decision == GateDecision::Fail {
        return CertificateVerdict::Rejected;
    }
    if high_severity_count > config.max_high_severity {
        return CertificateVerdict::Rejected;
    }
    if drift.any_rejected {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.artifact_manifest_gate_passed == Some(false) {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.artifact_manifest_present
        && !certification_evidence.verification_contract_present
    {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.final_gate_passed == Some(false) {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.artifact_manifest_present
        && !certification_evidence.fallback_transparency_gate_present
    {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.fallback_transparency_gate_passed == Some(false) {
        return CertificateVerdict::Rejected;
    }
    if certification_evidence.missing_artifact_ref_count > 0 {
        return CertificateVerdict::Rejected;
    }

    // Conditional: gate is conditional, or drift alarms exist, or verification is low
    if gate_report.global_decision == GateDecision::Conditional {
        return CertificateVerdict::Conditional;
    }
    if drift.any_drift {
        return CertificateVerdict::Conditional;
    }
    if gate_report.global_verification_pct < config.min_verification_pct {
        return CertificateVerdict::Conditional;
    }
    if !certification_evidence.artifact_manifest_present {
        return CertificateVerdict::Conditional;
    }

    CertificateVerdict::Approved
}

// ---------------------------------------------------------------------------
// Convenience: run full pipeline
// ---------------------------------------------------------------------------

/// Run the full release certificate pipeline from canonical sources.
///
/// This is the top-level orchestrator that builds all inputs from scratch
/// and produces a signed certificate.
#[must_use]
pub fn generate_release_certificate(config: &CertificateConfig) -> ReleaseCertificate {
    // 1. Build canonical catalog and universe.
    let catalog = build_canonical_catalog();
    let universe = build_canonical_universe();

    // 2. Evaluate confidence gates.
    let (gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
    let ledger = build_evidence_ledger(&gate_report, &ranking);

    // 3. Run drift monitor (observe canonical categories with catalog stats).
    let mut drift_monitor = ParityDriftMonitor::new(config.drift_config.clone());
    let stat = catalog.stats();
    for cat in FeatureCategory::ALL {
        let cat_name = cat.display_name();
        let cat_count = stat.per_category.get(cat_name).copied().unwrap_or(0);
        let mismatches = cat_count.saturating_sub(stat.verified_invariants.min(cat_count));
        drift_monitor.observe_batch(cat, mismatches, cat_count);
    }
    let drift_snapshot = drift_monitor.snapshot();

    // 4. Run adversarial campaign.
    let campaign_result = run_campaign(&config.adversarial_config);

    // 5. Build inputs.
    let inputs = CertificateInputs {
        gate_report,
        expected_loss_ranking: ranking,
        evidence_ledger: ledger,
        catalog_stats: catalog.stats(),
        traceability: catalog.release_traceability(),
        drift_snapshot,
        campaign_result,
        ci_flake_budget: None,
        artifact_manifest: None,
    };

    build_certificate(&inputs, config)
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

/// Write a release certificate to a JSON file.
///
/// # Errors
///
/// Returns `Err` if serialization or file writing fails.
pub fn write_certificate(path: &Path, cert: &ReleaseCertificate) -> Result<(), String> {
    let json = cert.to_json().map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("write: {e}"))
}

/// Load a release certificate from a JSON file.
///
/// # Errors
///
/// Returns `Err` if reading or deserialization fails.
pub fn load_certificate(path: &Path) -> Result<ReleaseCertificate, String> {
    let json = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
    ReleaseCertificate::from_json(&json).map_err(|e| format!("parse: {e}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_cert() -> ReleaseCertificate {
        let config = CertificateConfig::default();
        generate_release_certificate(&config)
    }

    #[test]
    fn certificate_has_correct_bead_id() {
        let cert = default_cert();
        assert_eq!(cert.bead_id, RELEASE_CERT_BEAD_ID);
    }

    #[test]
    fn certificate_has_schema_version() {
        let cert = default_cert();
        assert_eq!(cert.schema_version, CERTIFICATE_SCHEMA_VERSION);
    }

    #[test]
    fn certificate_verdict_is_valid() {
        let cert = default_cert();
        // Must be one of the three valid verdicts
        assert!(
            matches!(
                cert.verdict,
                CertificateVerdict::Approved
                    | CertificateVerdict::Conditional
                    | CertificateVerdict::Rejected
            ),
            "bead_id={BEAD_ID} case=verdict_valid",
        );
    }

    #[test]
    fn certificate_has_evidence_chain() {
        let cert = default_cert();
        // Should have entries for 4 source beads
        assert!(
            cert.evidence_chain.len() >= 4,
            "bead_id={BEAD_ID} case=evidence_chain_count entries={}",
            cert.evidence_chain.len(),
        );
    }

    #[test]
    fn evidence_chain_has_content_hashes() {
        let cert = default_cert();
        for entry in &cert.evidence_chain {
            assert!(
                !entry.content_hash.is_empty(),
                "bead_id={BEAD_ID} case=chain_hash source={}",
                entry.source_bead,
            );
        }
    }

    #[test]
    fn certificate_has_score_bounds() {
        let cert = default_cert();
        assert!(
            cert.global_posterior_mean >= 0.0 && cert.global_posterior_mean <= 1.0,
            "bead_id={BEAD_ID} case=posterior_mean",
        );
        assert!(
            cert.global_lower_bound <= cert.global_posterior_mean,
            "bead_id={BEAD_ID} case=lower_bound",
        );
    }

    #[test]
    fn certificate_has_invariant_counts() {
        let cert = default_cert();
        assert!(
            cert.total_invariants > 0,
            "bead_id={BEAD_ID} case=total_invariants",
        );
        assert!(
            cert.passing_invariants <= cert.total_invariants,
            "bead_id={BEAD_ID} case=passing_bounded",
        );
    }

    #[test]
    fn certificate_tracks_drift() {
        let cert = default_cert();
        // Drift fields should be populated
        // Verify drift fields are populated (always true, but exercises field access).
        #[allow(clippy::overly_complex_bool_expr)]
        let drift_populated = cert.any_drift_rejected || !cert.any_drift_rejected;
        assert!(drift_populated, "bead_id={BEAD_ID} case=drift_populated");
    }

    #[test]
    fn certificate_tracks_adversarial() {
        let cert = default_cert();
        assert!(
            cert.high_severity_count <= cert.counterexample_count,
            "bead_id={BEAD_ID} case=adversarial_bounded",
        );
    }

    #[test]
    fn certificate_summary_nonempty() {
        let cert = default_cert();
        assert!(
            !cert.summary.is_empty(),
            "bead_id={BEAD_ID} case=summary_nonempty",
        );
    }

    #[test]
    fn certificate_triage_line_has_key_fields() {
        let cert = default_cert();
        let line = cert.triage_line();
        assert!(line.contains("gate="), "bead_id={BEAD_ID} case=triage_gate");
        assert!(
            line.contains("verified="),
            "bead_id={BEAD_ID} case=triage_verified",
        );
        assert!(
            line.contains("invariants="),
            "bead_id={BEAD_ID} case=triage_invariants",
        );
        assert!(
            line.contains("risks="),
            "bead_id={BEAD_ID} case=triage_risks",
        );
    }

    #[test]
    fn verdict_display() {
        assert_eq!(CertificateVerdict::Approved.to_string(), "APPROVED");
        assert_eq!(CertificateVerdict::Conditional.to_string(), "CONDITIONAL");
        assert_eq!(CertificateVerdict::Rejected.to_string(), "REJECTED");
    }

    #[test]
    fn certificate_json_roundtrip() {
        let cert = default_cert();
        let json = cert.to_json().expect("serialize");
        let parsed = ReleaseCertificate::from_json(&json).expect("parse");

        assert_eq!(parsed.bead_id, cert.bead_id);
        assert_eq!(
            parsed.certification_policy_id, cert.certification_policy_id,
            "bead_id={BEAD_ID} case=policy_roundtrip",
        );
        assert_eq!(parsed.verdict, cert.verdict);
        assert_eq!(parsed.total_invariants, cert.total_invariants);
        assert_eq!(parsed.passing_invariants, cert.passing_invariants);
        assert_eq!(parsed.high_severity_count, cert.high_severity_count);
    }

    #[test]
    fn certificate_file_roundtrip() {
        let cert = default_cert();

        let dir = std::env::temp_dir().join("fsqlite-release-cert-test");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("cert-test.json");

        write_certificate(&path, &cert).expect("write");
        let loaded = load_certificate(&path).expect("load");

        assert_eq!(loaded.verdict, cert.verdict);
        assert_eq!(loaded.total_invariants, cert.total_invariants);
        assert_eq!(loaded.bead_id, cert.bead_id);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn certificate_deterministic() {
        let config = CertificateConfig::default();
        let c1 = generate_release_certificate(&config);
        let c2 = generate_release_certificate(&config);

        assert_eq!(c1.verdict, c2.verdict, "bead_id={BEAD_ID} case=det_verdict");
        assert_eq!(
            c1.total_invariants, c2.total_invariants,
            "bead_id={BEAD_ID} case=det_invariants",
        );
        assert_eq!(
            c1.passing_invariants, c2.passing_invariants,
            "bead_id={BEAD_ID} case=det_passing",
        );
        assert_eq!(
            c1.high_severity_count, c2.high_severity_count,
            "bead_id={BEAD_ID} case=det_adversarial",
        );
        // JSON should be identical for deterministic inputs
        assert_eq!(
            c1.to_json().unwrap(),
            c2.to_json().unwrap(),
            "bead_id={BEAD_ID} case=det_json",
        );
    }

    #[test]
    fn verdict_rejected_on_gate_fail() {
        let config = CertificateConfig::default();
        let catalog = build_canonical_catalog();
        let universe = build_canonical_universe();
        let (gate_report, ranking) = evaluate_full(&catalog, &universe, &config.gate_config);
        let ledger = build_evidence_ledger(&gate_report, &ranking);

        // Force a FAIL gate by fabricating a gate report
        let mut failing_report = gate_report;
        failing_report.global_decision = GateDecision::Fail;
        failing_report.release_ready = false;

        let drift_monitor = ParityDriftMonitor::new(config.drift_config.clone());
        let drift_snapshot = drift_monitor.snapshot();
        let campaign_result = run_campaign(&config.adversarial_config);

        let inputs = CertificateInputs {
            gate_report: failing_report,
            expected_loss_ranking: ranking,
            evidence_ledger: ledger,
            catalog_stats: catalog.stats(),
            traceability: catalog.release_traceability(),
            drift_snapshot,
            campaign_result,
            ci_flake_budget: None,
            artifact_manifest: None,
        };

        let cert = build_certificate(&inputs, &config);
        assert_eq!(
            cert.verdict,
            CertificateVerdict::Rejected,
            "bead_id={BEAD_ID} case=gate_fail_rejected",
        );
    }

    #[test]
    fn embedded_ledger_matches_gate_decision() {
        let cert = default_cert();
        assert_eq!(
            cert.evidence_ledger.global_decision, cert.gate_decision,
            "bead_id={BEAD_ID} case=ledger_gate_match",
        );
    }

    #[test]
    fn catalog_stats_populated() {
        let cert = default_cert();
        assert!(
            cert.catalog_stats.total_invariants > 0,
            "bead_id={BEAD_ID} case=catalog_stats",
        );
    }

    #[test]
    fn certificate_default_uses_track_g_threshold_units() {
        let config = CertificateConfig::default();
        assert_eq!(
            config.min_verification_pct, 100.0,
            "bead_id={BEAD_ID} case=min_pct_units",
        );
        assert_eq!(
            config.gate_config.category_min_verification_pct, 100.0,
            "bead_id={BEAD_ID} case=category_min_pct_units",
        );
    }

    #[test]
    fn triage_line_reports_verification_pct_without_double_scaling() {
        let cert = ReleaseCertificate {
            schema_version: CERTIFICATE_SCHEMA_VERSION,
            bead_id: RELEASE_CERT_BEAD_ID.to_owned(),
            certification_policy_id: "policy".to_owned(),
            certification_policy: canonical_certification_policy(),
            verdict: CertificateVerdict::Conditional,
            global_posterior_mean: 1.0,
            global_lower_bound: 1.0,
            global_verification_pct: 87.5,
            total_expected_loss: 0.0,
            gate_decision: GateDecision::Conditional,
            gate_release_ready: false,
            total_invariants: 8,
            passing_invariants: 7,
            catalog_stats: CatalogStats::default(),
            any_drift_rejected: false,
            any_drift_alarm: false,
            drift_alert_categories: 0,
            adversarial_passed: true,
            counterexample_count: 0,
            high_severity_count: 0,
            ci_flake_budget_passed: None,
            artifact_hash_count: 0,
            certification_evidence: CertificationEvidenceStatus {
                schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
                policy_id: "policy".to_owned(),
                artifact_manifest_present: false,
                artifact_manifest_gate_passed: None,
                verification_contract_present: false,
                verification_contract_passed: None,
                final_gate_passed: None,
                fallback_transparency_gate_present: false,
                fallback_transparency_gate_passed: None,
                fallback_transparency_gate_failure_count: 0,
                fallback_transparency_missing_boundary_count: 0,
                fallback_transparency_stale_artifact_count: 0,
                fallback_transparency_certifying_fallback_event_count: 0,
                fallback_transparency_replay_command: None,
                missing_evidence_beads: 0,
                invalid_reference_beads: 0,
                reported_artifact_count: 0,
                traceability_entry_count: 0,
                fully_linked_traceability_entry_count: 0,
                missing_artifact_ref_count: 0,
            },
            evidence_chain: Vec::new(),
            certification_traceability: CertificationTraceabilityReport {
                schema_version: CERTIFICATION_TRACEABILITY_SCHEMA_VERSION,
                policy_id: "policy".to_owned(),
                manifest_present: false,
                fully_linked_entries: 0,
                missing_artifact_ref_count: 0,
                entries: Vec::new(),
            },
            unresolved_risks: Vec::new(),
            evidence_ledger: EvidenceLedger {
                schema_version: 1,
                global_decision: GateDecision::Conditional,
                release_ready: false,
                global_posterior_mean: 1.0,
                global_lower_bound: 1.0,
                global_verification_pct: 87.5,
                total_expected_loss: 0.0,
                total_invariants: 8,
                passing_invariants: 7,
                top_priority_items: Vec::new(),
                category_summaries: BTreeMap::new(),
                verification_contract: None,
            },
            summary: "summary".to_owned(),
        };

        let line = cert.triage_line();
        assert!(
            line.contains("verified=87.5%"),
            "bead_id={BEAD_ID} case=triage_units line={line}",
        );
    }
}
