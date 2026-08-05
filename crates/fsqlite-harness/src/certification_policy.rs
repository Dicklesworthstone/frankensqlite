//! Track G certification gates, ratchets, and release-evidence contract (bd-2yqp6.7).
//!
//! This module defines the canonical policy for when FrankenSQLite may call a
//! release "conformant" on the declared SQLite-compatible surface. The policy
//! is intentionally strict: release certification requires complete declared
//! surface verification, zero missing required evidence, and monotone ratchets.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ci_gate_matrix::CiLane;
use crate::confidence_gates::GateConfig;
use crate::ratchet_policy::RatchetPolicy;
use crate::score_engine::STATISTICAL_RELEASE_FLOOR;

/// Owning Track G bead.
pub const CERTIFICATION_POLICY_BEAD_ID: &str = "bd-2yqp6.7";
/// Stable identifier for the strict certification profile.
pub const CERTIFICATION_POLICY_ID: &str = "strict-conformant-release.v2";
/// Schema version for machine-readable certification policy artifacts.
pub const CERTIFICATION_POLICY_SCHEMA_VERSION: u32 = 1;
/// Certification requires full declared-surface verification.
pub const CERTIFICATION_MIN_VERIFICATION_PCT: f64 = 100.0;
/// Certification requires all mandatory suite lanes to pass completely.
pub const CERTIFICATION_REQUIRED_SUITE_PASS_RATE_PCT: f64 = 100.0;
/// Certification rejects any HIGH-severity unresolved counterexample.
pub const CERTIFICATION_MAX_HIGH_SEVERITY_COUNTEREXAMPLES: usize = 0;
/// Certification evidence must be fresh enough to describe the current build.
pub const CERTIFICATION_MAX_EVIDENCE_AGE_HOURS: u64 = 24;

/// Named evidence classes required by the certification bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationEvidenceClass {
    ConfidenceGateReport,
    VerificationContract,
    ArtifactManifest,
    ReleaseCertificate,
    SummaryMarkdown,
    ResultsJsonl,
    ScorecardsJson,
    CriticalPathEvidence,
    RatchetState,
}

impl CertificationEvidenceClass {
    /// Stable string identifier for documentation and JSON artifacts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfidenceGateReport => "confidence_gate_report",
            Self::VerificationContract => "verification_contract",
            Self::ArtifactManifest => "artifact_manifest",
            Self::ReleaseCertificate => "release_certificate",
            Self::SummaryMarkdown => "summary_markdown",
            Self::ResultsJsonl => "results_jsonl",
            Self::ScorecardsJson => "scorecards_json",
            Self::CriticalPathEvidence => "critical_path_evidence",
            Self::RatchetState => "ratchet_state",
        }
    }
}

/// One blocking certification gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationGateSpec {
    /// Stable gate identifier.
    pub gate_id: String,
    /// Whether the gate blocks the certification verdict.
    pub blocking: bool,
    /// Optional CI lane associated with the gate.
    pub required_lane: Option<String>,
    /// Human-readable pass condition.
    pub pass_condition: String,
}

/// One monotone ratchet requirement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRatchetSpec {
    /// Stable ratchet identifier.
    pub ratchet_id: String,
    /// Whether the ratchet blocks the certification verdict.
    pub blocking: bool,
    /// Human-readable monotonicity contract.
    pub pass_condition: String,
}

/// Persisted Track G baseline used to block certification backslides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRatchetBaseline {
    /// Schema version for the baseline artifact.
    pub schema_version: u32,
    /// Owning certification policy identifier.
    pub policy_id: String,
    /// Approved global parity lower bound.
    pub global_lower_bound: f64,
    /// Approved per-category lower bounds.
    pub category_lower_bounds: BTreeMap<String, f64>,
    /// Approved required-suite pass rate percentage.
    pub required_suite_pass_rate_pct: f64,
    /// Approved feature->test->run->artifact traceability coverage percentage.
    pub traceability_link_coverage_pct: f64,
}

/// Current certification metrics evaluated against an approved baseline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRatchetCandidate {
    /// Current global parity lower bound.
    pub global_lower_bound: f64,
    /// Current per-category lower bounds.
    pub category_lower_bounds: BTreeMap<String, f64>,
    /// Current required-suite pass rate percentage.
    pub required_suite_pass_rate_pct: f64,
    /// Current feature->test->run->artifact traceability coverage percentage.
    pub traceability_link_coverage_pct: f64,
}

/// Outcome of Track G certification ratchet evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRatchetEvaluation {
    /// Whether all blocking ratchets passed.
    pub passed: bool,
    /// Ratchet IDs that regressed below the approved baseline.
    pub regressed_ratchets: Vec<String>,
    /// Human-readable summary for CI and artifact logs.
    pub summary: String,
}

/// Canonical Track G certification policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationPolicy {
    /// Schema version.
    pub schema_version: u32,
    /// Owning bead identifier.
    pub bead_id: String,
    /// Stable certification policy identifier.
    pub policy_id: String,
    /// Gate config used for strict declared-surface certification.
    pub gate_config: GateConfig,
    /// Ratchet config used to prevent silent certification backslide.
    pub ratchet_policy: RatchetPolicy,
    /// Minimum verification percentage required to claim conformance.
    pub min_verification_pct: f64,
    /// Minimum required suite pass rate across mandatory lanes.
    pub required_suite_pass_rate_pct: f64,
    /// Maximum allowed HIGH-severity unresolved counterexamples.
    pub max_high_severity_counterexamples: usize,
    /// Maximum allowed age for required evidence artifacts.
    pub max_evidence_age_hours: u64,
    /// Mandatory CI lanes for certification.
    pub required_ci_lanes: Vec<String>,
    /// Required evidence classes for the release bundle.
    pub required_evidence: Vec<CertificationEvidenceClass>,
    /// Blocking gate definitions.
    pub gates: Vec<CertificationGateSpec>,
    /// Monotone ratchet definitions.
    pub ratchets: Vec<CertificationRatchetSpec>,
}

/// Mandatory CI lanes for declared-surface certification.
pub const REQUIRED_CERTIFICATION_LANES: [CiLane; 6] = [
    CiLane::Unit,
    CiLane::E2eDifferential,
    CiLane::E2eCorrectness,
    CiLane::E2eRecovery,
    CiLane::SchemaValidation,
    CiLane::CoverageDrift,
];

/// Build the strict gate configuration used by Track G certification.
#[must_use]
pub fn certification_gate_config() -> GateConfig {
    GateConfig {
        release_threshold: STATISTICAL_RELEASE_FLOOR,
        category_min_verification_pct: CERTIFICATION_MIN_VERIFICATION_PCT,
        waived_obligations_satisfy_gate: false,
        ..GateConfig::default()
    }
}

/// Build the strict ratchet configuration used by Track G certification.
#[must_use]
pub fn certification_ratchet_policy() -> RatchetPolicy {
    RatchetPolicy::strict()
}

/// Evaluate current certification metrics against an approved Track G baseline.
#[must_use]
pub fn evaluate_certification_ratchets(
    baseline: &CertificationRatchetBaseline,
    candidate: &CertificationRatchetCandidate,
) -> CertificationRatchetEvaluation {
    let mut regressed_ratchets = Vec::new();

    if baseline.schema_version != CERTIFICATION_POLICY_SCHEMA_VERSION
        || baseline.policy_id != CERTIFICATION_POLICY_ID
    {
        regressed_ratchets.push("baseline_identity".to_owned());
    }

    if !(0.0..=1.0).contains(&baseline.global_lower_bound)
        || !(0.0..=1.0).contains(&candidate.global_lower_bound)
        || candidate.global_lower_bound < baseline.global_lower_bound
    {
        regressed_ratchets.push("global_lower_bound".to_owned());
    }

    if baseline.category_lower_bounds.is_empty()
        || baseline
            .category_lower_bounds
            .keys()
            .ne(candidate.category_lower_bounds.keys())
        || baseline
            .category_lower_bounds
            .iter()
            .any(|(category, approved)| {
                !(0.0..=1.0).contains(approved)
                    || candidate
                        .category_lower_bounds
                        .get(category)
                        .is_none_or(|current| !(0.0..=1.0).contains(current) || current < approved)
            })
    {
        regressed_ratchets.push("category_lower_bounds".to_owned());
    }

    if !(0.0..=100.0).contains(&baseline.required_suite_pass_rate_pct)
        || !(0.0..=100.0).contains(&candidate.required_suite_pass_rate_pct)
        || candidate.required_suite_pass_rate_pct < baseline.required_suite_pass_rate_pct
    {
        regressed_ratchets.push("required_suite_pass_rate".to_owned());
    }

    if !(0.0..=100.0).contains(&baseline.traceability_link_coverage_pct)
        || !(0.0..=100.0).contains(&candidate.traceability_link_coverage_pct)
        || candidate.traceability_link_coverage_pct < baseline.traceability_link_coverage_pct
    {
        regressed_ratchets.push("traceability_link_coverage".to_owned());
    }

    let passed = regressed_ratchets.is_empty();
    let summary = if passed {
        "certification baseline preserved".to_owned()
    } else {
        format!(
            "certification baseline regressed: {}",
            regressed_ratchets.join(", ")
        )
    };

    CertificationRatchetEvaluation {
        passed,
        regressed_ratchets,
        summary,
    }
}

/// Build the canonical Track G certification policy.
#[must_use]
pub fn canonical_certification_policy() -> CertificationPolicy {
    let gate_config = certification_gate_config();
    let ratchet_policy = certification_ratchet_policy();

    let required_ci_lanes = REQUIRED_CERTIFICATION_LANES
        .iter()
        .map(|lane| lane.as_str().to_owned())
        .collect();

    let required_evidence = vec![
        CertificationEvidenceClass::ConfidenceGateReport,
        CertificationEvidenceClass::VerificationContract,
        CertificationEvidenceClass::ArtifactManifest,
        CertificationEvidenceClass::ReleaseCertificate,
        CertificationEvidenceClass::SummaryMarkdown,
        CertificationEvidenceClass::ResultsJsonl,
        CertificationEvidenceClass::ScorecardsJson,
        CertificationEvidenceClass::CriticalPathEvidence,
        CertificationEvidenceClass::RatchetState,
    ];

    let mut gates = Vec::new();
    gates.push(CertificationGateSpec {
        gate_id: "declared_surface_parity".to_owned(),
        blocking: true,
        required_lane: None,
        pass_condition: "Confidence gates and the release certificate must both report 100.0% declared-surface verification with release-ready=true.".to_owned(),
    });
    for lane in REQUIRED_CERTIFICATION_LANES {
        gates.push(CertificationGateSpec {
            gate_id: format!("required_suite_pass::{}", lane.as_str()),
            blocking: true,
            required_lane: Some(lane.as_str().to_owned()),
            pass_condition:
                "Lane must pass at 100.0% with zero terminal failures and zero blocking flakes."
                    .to_owned(),
        });
    }
    gates.push(CertificationGateSpec {
        gate_id: "verification_contract".to_owned(),
        blocking: true,
        required_lane: None,
        pass_condition: "Verification-contract enforcement must report final_gate_passed=true with zero missing-evidence beads and zero invalid-reference beads.".to_owned(),
    });
    gates.push(CertificationGateSpec {
        gate_id: "release_evidence_completeness".to_owned(),
        blocking: true,
        required_lane: None,
        pass_condition: "The certification bundle must include feature->test->run->artifact-hash traceability, scorecards, summary markdown, results.jsonl, and a machine-readable artifact manifest no older than 24 hours.".to_owned(),
    });
    gates.push(CertificationGateSpec {
        gate_id: "critical_path_evidence".to_owned(),
        blocking: true,
        required_lane: None,
        pass_condition: "No critical-path invariant may be ignored, mocked away, or left without real evidence in the validation/critical-path reports.".to_owned(),
    });

    let ratchets = vec![
        CertificationRatchetSpec {
            ratchet_id: "global_lower_bound".to_owned(),
            blocking: true,
            pass_condition: "The global parity lower bound is monotone non-decreasing across certified runs; zero regression tolerance, no waivers, no quarantine.".to_owned(),
        },
        CertificationRatchetSpec {
            ratchet_id: "category_lower_bounds".to_owned(),
            blocking: true,
            pass_condition: "Every declared-surface category lower bound is monotone non-decreasing across certified runs.".to_owned(),
        },
        CertificationRatchetSpec {
            ratchet_id: "required_suite_pass_rate".to_owned(),
            blocking: true,
            pass_condition: "Mandatory CI lane pass rates do not fall below the previous certified release baseline.".to_owned(),
        },
        CertificationRatchetSpec {
            ratchet_id: "traceability_link_coverage".to_owned(),
            blocking: true,
            pass_condition: "Feature->test->run->artifact-hash linkage coverage is monotone non-decreasing across evidence packs.".to_owned(),
        },
        CertificationRatchetSpec {
            ratchet_id: "artifact_hash_integrity".to_owned(),
            blocking: true,
            pass_condition: "Artifact bundle hashes remain stable unless a reviewed baseline update is recorded by the artifact-hash ratchet.".to_owned(),
        },
    ];

    CertificationPolicy {
        schema_version: CERTIFICATION_POLICY_SCHEMA_VERSION,
        bead_id: CERTIFICATION_POLICY_BEAD_ID.to_owned(),
        policy_id: CERTIFICATION_POLICY_ID.to_owned(),
        gate_config,
        ratchet_policy,
        min_verification_pct: CERTIFICATION_MIN_VERIFICATION_PCT,
        required_suite_pass_rate_pct: CERTIFICATION_REQUIRED_SUITE_PASS_RATE_PCT,
        max_high_severity_counterexamples: CERTIFICATION_MAX_HIGH_SEVERITY_COUNTEREXAMPLES,
        max_evidence_age_hours: CERTIFICATION_MAX_EVIDENCE_AGE_HOURS,
        required_ci_lanes,
        required_evidence,
        gates,
        ratchets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_policy_is_strict() {
        let policy = canonical_certification_policy();

        assert_eq!(policy.policy_id, CERTIFICATION_POLICY_ID);
        assert_eq!(policy.min_verification_pct, 100.0);
        assert_eq!(policy.required_suite_pass_rate_pct, 100.0);
        assert_eq!(policy.max_high_severity_counterexamples, 0);
        assert_eq!(policy.max_evidence_age_hours, 24);
        assert_eq!(
            policy.gate_config.release_threshold,
            STATISTICAL_RELEASE_FLOOR
        );
        assert_eq!(policy.gate_config.category_min_verification_pct, 100.0);
        assert!(!policy.gate_config.waived_obligations_satisfy_gate);
        assert_eq!(
            policy.ratchet_policy.minimum_release_threshold,
            STATISTICAL_RELEASE_FLOOR
        );
        assert_eq!(policy.ratchet_policy.regression_tolerance, 0.0);
        assert_eq!(policy.ratchet_policy.category_regression_tolerance, 0.0);
        assert!(!policy.ratchet_policy.quarantine_enabled);
        assert!(!policy.ratchet_policy.waivers_enabled);
    }

    #[test]
    fn canonical_policy_lists_required_lanes_and_evidence() {
        let policy = canonical_certification_policy();

        for lane in REQUIRED_CERTIFICATION_LANES {
            assert!(
                policy
                    .required_ci_lanes
                    .iter()
                    .any(|entry| entry == lane.as_str()),
                "missing required lane {}",
                lane.as_str(),
            );
        }

        for evidence in [
            CertificationEvidenceClass::ConfidenceGateReport,
            CertificationEvidenceClass::VerificationContract,
            CertificationEvidenceClass::ArtifactManifest,
            CertificationEvidenceClass::ReleaseCertificate,
            CertificationEvidenceClass::ScorecardsJson,
            CertificationEvidenceClass::RatchetState,
        ] {
            assert!(
                policy.required_evidence.contains(&evidence),
                "missing required evidence {}",
                evidence.as_str(),
            );
        }
    }
}
