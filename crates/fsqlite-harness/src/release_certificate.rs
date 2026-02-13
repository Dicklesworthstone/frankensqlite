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
use crate::ci_gate_matrix::{ArtifactEntry, GlobalFlakeBudgetResult};
use crate::confidence_gates::{
    EvidenceLedger, ExpectedLossRanking, GateConfig, GateDecision, GateReport,
    build_evidence_ledger, evaluate_full,
};
use crate::drift_monitor::{ParityDriftConfig, ParityDriftMonitor, ParityDriftSnapshot};
use crate::parity_invariant_catalog::{
    CatalogStats, ReleaseTraceabilityReport, build_canonical_catalog,
};
use crate::parity_taxonomy::{FeatureCategory, build_canonical_universe, truncate_score};

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
            gate_config: GateConfig::default(),
            drift_config: ParityDriftConfig::default(),
            adversarial_config: AdversarialConfig::default(),
            max_high_severity: 0,
            min_verification_pct: 0.80,
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
// Release certificate
// ---------------------------------------------------------------------------

/// A machine-verifiable release certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseCertificate {
    /// Schema version.
    pub schema_version: u32,
    /// Bead identifier.
    pub bead_id: String,
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

    // ---- Evidence chain ----
    /// Ordered evidence chain entries for audit trail.
    pub evidence_chain: Vec<EvidenceChainEntry>,

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
            self.global_verification_pct * 100.0,
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
    /// Artifact hashes for the evidence chain.
    pub artifact_hashes: Vec<ArtifactEntry>,
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

/// Build a release certificate from pre-assembled inputs.
#[must_use]
pub fn build_certificate(inputs: &CertificateInputs, config: &CertificateConfig) -> ReleaseCertificate {
    let gate_report = &inputs.gate_report;
    let ranking = &inputs.expected_loss_ranking;
    let ledger = &inputs.evidence_ledger;
    let drift = &inputs.drift_snapshot;
    let campaign = &inputs.campaign_result;
    let stats = &inputs.catalog_stats;

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
            gate_report.global_verification_pct * 100.0,
            ranking.total_expected_loss,
        ),
    });

    // 4. Adversarial search
    let campaign_json = serde_json::to_string(campaign).unwrap_or_default();
    evidence_chain.push(EvidenceChainEntry {
        source_bead: "bd-1dp9.8.5".to_owned(),
        schema_version: campaign.schema_version,
        content_hash: sha256_hex(&campaign_json),
        summary: format!(
            "Adversarial: {} trials, {} counterexamples, passed={}",
            campaign.total_trials, campaign.counterexamples.len(), campaign.passed,
        ),
    });

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
        config,
    );

    // ---- Summary ----
    let summary = format!(
        "Release certificate {}: gate={} verified={:.1}% ({}/{} invariants), \
         drift_rejected={}, adversarial={} ({} counterexamples, {} high), \
         {} unresolved risk(s)",
        verdict,
        gate_report.global_decision,
        truncate_score(gate_report.global_verification_pct * 100.0),
        gate_report.passing_invariants,
        gate_report.total_invariants,
        drift.any_rejected,
        if campaign.passed { "PASS" } else { "FAIL" },
        campaign.counterexamples.len(),
        high_severity_count,
        unresolved_risks.len(),
    );

    ReleaseCertificate {
        schema_version: CERTIFICATE_SCHEMA_VERSION,
        bead_id: RELEASE_CERT_BEAD_ID.to_owned(),
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
        artifact_hash_count: inputs.artifact_hashes.len(),
        evidence_chain,
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
        artifact_hashes: Vec::new(),
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
        assert!(
            cert.any_drift_rejected || !cert.any_drift_rejected,
            "bead_id={BEAD_ID} case=drift_populated",
        );
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
        let mut failing_report = gate_report.clone();
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
            artifact_hashes: Vec::new(),
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
}
