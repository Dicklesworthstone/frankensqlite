//! Explicit exit criteria for `bd-db300.7.3` leapfrog claims.
//!
//! This module turns the Track G3 decision record into a typed, machine-readable
//! contract that downstream verification and reporting code can consume without
//! reinterpreting prose.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Bead identifier for the leapfrog exit-criteria gate.
pub const BEAD_ID: &str = "bd-db300.7.3";
/// Stable schema identifier for the exit-criteria contract.
pub const LEAPFROG_EXIT_CRITERIA_SCHEMA_V1: &str =
    "fsqlite-harness.leapfrog_exit_criteria.v1";
/// Workspace-relative path to the canonical exit-criteria contract.
pub const LEAPFROG_EXIT_CRITERIA_PATH: &str = "leapfrog_exit_criteria.toml";
const CANONICAL_CAMPAIGN_MANIFEST_PATH: &str =
    "sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json";

const REQUIRED_LOG_FIELDS: [&str; 8] = [
    "throughput_ratio_vs_sqlite",
    "retry_rate",
    "cpu_utilization_pct",
    "p50_latency_ratio_vs_sqlite",
    "p95_latency_ratio_vs_sqlite",
    "p99_latency_ratio_vs_sqlite",
    "responsiveness_regression_ratio_vs_sqlite",
    "topology_reassignments",
];

/// Typed decision record for Track G3 leapfrog claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeapfrogExitCriteria {
    pub meta: ExitCriteriaMeta,
    pub campaign: CampaignRequirements,
    pub scorecard: ScorecardPolicy,
    pub cell_gates: Vec<CellGate>,
    pub verification_plan: VerificationPlan,
    pub references: ExitCriteriaReferences,
}

/// Metadata for the canonical exit-criteria contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitCriteriaMeta {
    pub schema_version: String,
    pub policy_id: String,
    pub bead_id: String,
    pub track_id: String,
    pub generated_at: String,
    pub owner: String,
    pub decision_summary: String,
    pub rationale: String,
}

/// Benchmark campaign surface that the exit criteria bind to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignRequirements {
    pub manifest_path: String,
    pub required_modes: Vec<String>,
    pub required_placement_profiles: Vec<String>,
    pub required_cell_suffixes: Vec<String>,
}

/// Claim-language and placement-profile policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScorecardPolicy {
    pub recommended_profile_id: String,
    pub baseline_profile_id: String,
    pub adversarial_profile_id: String,
    pub claim_language: String,
    pub claim_forbidden_when_any_fail: bool,
}

/// Explicit gate for one canonical concurrency cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellGate {
    pub cell: String,
    pub goal: String,
    pub recommended_min_throughput_ratio_vs_sqlite: f64,
    pub baseline_catastrophic_floor_ratio_vs_sqlite: f64,
    pub adversarial_catastrophic_floor_ratio_vs_sqlite: f64,
    pub max_retry_rate: f64,
    pub min_cpu_utilization_pct: f64,
    pub max_p50_latency_ratio_vs_sqlite: f64,
    pub max_p95_latency_ratio_vs_sqlite: f64,
    pub max_p99_latency_ratio_vs_sqlite: f64,
    pub max_responsiveness_regression_ratio_vs_sqlite: f64,
    pub max_topology_reassignments_per_run: u32,
}

/// Follow-on verification obligations that must remain attached to the gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationPlan {
    pub unit_tests: Vec<String>,
    pub e2e_scenarios: Vec<String>,
    pub logging_artifacts: Vec<String>,
    pub required_log_fields: Vec<String>,
}

/// Canonical files that downstream tooling should consume or invoke.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitCriteriaReferences {
    pub contract_module: String,
    pub verification_test: String,
    pub campaign_matrix_module: String,
    pub score_engine_module: String,
    pub confidence_gates_module: String,
    pub release_certificate_module: String,
    pub verifier_script: String,
}

impl LeapfrogExitCriteria {
    /// Load the canonical contract from the workspace root.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the contract cannot be read or parsed.
    pub fn load_from_workspace_root(workspace_root: &Path) -> Result<Self, String> {
        let path = workspace_root.join(LEAPFROG_EXIT_CRITERIA_PATH);
        Self::load_from_path(&path)
    }

    /// Load the contract from an explicit path.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the file cannot be read or parsed.
    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path).map_err(|error| {
            format!(
                "leapfrog_exit_criteria_read_failed path={} error={error}",
                path.display()
            )
        })?;
        toml::from_str(&raw).map_err(|error| {
            format!(
                "leapfrog_exit_criteria_parse_failed path={} error={error}",
                path.display()
            )
        })
    }

    /// Return the gate definition for a canonical cell suffix like `c1`.
    #[must_use]
    pub fn cell_gate(&self, cell: &str) -> Option<&CellGate> {
        self.cell_gates.iter().find(|gate| gate.cell == cell)
    }

    /// Validate the contract against internal invariants and the campaign
    /// manifest it references.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the contract is malformed or references missing
    /// campaign/reporting inputs.
    pub fn validate(&self, workspace_root: &Path) -> Result<(), String> {
        require_eq(
            "meta.schema_version",
            &self.meta.schema_version,
            LEAPFROG_EXIT_CRITERIA_SCHEMA_V1,
        )?;
        require_eq("meta.bead_id", &self.meta.bead_id, BEAD_ID)?;
        require_non_empty("meta.policy_id", &self.meta.policy_id)?;
        require_non_empty("meta.track_id", &self.meta.track_id)?;
        require_non_empty("meta.generated_at", &self.meta.generated_at)?;
        require_non_empty("meta.owner", &self.meta.owner)?;
        require_non_empty("meta.decision_summary", &self.meta.decision_summary)?;
        require_non_empty("meta.rationale", &self.meta.rationale)?;

        require_non_empty(
            "scorecard.recommended_profile_id",
            &self.scorecard.recommended_profile_id,
        )?;
        require_non_empty(
            "scorecard.baseline_profile_id",
            &self.scorecard.baseline_profile_id,
        )?;
        require_non_empty(
            "scorecard.adversarial_profile_id",
            &self.scorecard.adversarial_profile_id,
        )?;
        require_non_empty("scorecard.claim_language", &self.scorecard.claim_language)?;
        if !self.scorecard.claim_forbidden_when_any_fail {
            return Err("scorecard.claim_forbidden_when_any_fail must be true".to_owned());
        }

        let required_profile_ids = unique_set(
            "campaign.required_placement_profiles",
            &self.campaign.required_placement_profiles,
        )?;
        let required_mode_ids = unique_set("campaign.required_modes", &self.campaign.required_modes)?;
        let required_cell_suffixes =
            unique_set("campaign.required_cell_suffixes", &self.campaign.required_cell_suffixes)?;
        require_eq(
            "campaign.manifest_path",
            &self.campaign.manifest_path,
            CANONICAL_CAMPAIGN_MANIFEST_PATH,
        )?;
        if !required_cell_suffixes.contains("c1")
            || !required_cell_suffixes.contains("c4")
            || !required_cell_suffixes.contains("c8")
        {
            return Err(
                "campaign.required_cell_suffixes must include c1, c4, and c8".to_owned(),
            );
        }

        for profile_id in [
            &self.scorecard.recommended_profile_id,
            &self.scorecard.baseline_profile_id,
            &self.scorecard.adversarial_profile_id,
        ] {
            if !required_profile_ids.contains(profile_id.as_str()) {
                return Err(format!(
                    "scorecard profile `{profile_id}` is not listed in campaign.required_placement_profiles"
                ));
            }
        }

        let mut seen_cells = BTreeSet::new();
        for gate in &self.cell_gates {
            if !seen_cells.insert(gate.cell.as_str()) {
                return Err(format!("duplicate cell gate `{}`", gate.cell));
            }
            if !required_cell_suffixes.contains(gate.cell.as_str()) {
                return Err(format!(
                    "cell gate `{}` is not listed in campaign.required_cell_suffixes",
                    gate.cell
                ));
            }
            require_non_empty("cell_gates.goal", &gate.goal)?;
            require_ratio_at_least_one(
                "cell_gates.recommended_min_throughput_ratio_vs_sqlite",
                gate.recommended_min_throughput_ratio_vs_sqlite,
            )?;
            require_ratio_positive(
                "cell_gates.baseline_catastrophic_floor_ratio_vs_sqlite",
                gate.baseline_catastrophic_floor_ratio_vs_sqlite,
            )?;
            require_ratio_positive(
                "cell_gates.adversarial_catastrophic_floor_ratio_vs_sqlite",
                gate.adversarial_catastrophic_floor_ratio_vs_sqlite,
            )?;
            if gate.baseline_catastrophic_floor_ratio_vs_sqlite
                > gate.recommended_min_throughput_ratio_vs_sqlite
            {
                return Err(format!(
                    "cell gate `{}` has baseline catastrophic floor above recommended target",
                    gate.cell
                ));
            }
            if gate.adversarial_catastrophic_floor_ratio_vs_sqlite
                > gate.baseline_catastrophic_floor_ratio_vs_sqlite
            {
                return Err(format!(
                    "cell gate `{}` has adversarial catastrophic floor above baseline catastrophic floor",
                    gate.cell
                ));
            }
            require_pct(
                "cell_gates.max_retry_rate",
                gate.max_retry_rate,
                0.0,
                1.0,
            )?;
            require_pct(
                "cell_gates.min_cpu_utilization_pct",
                gate.min_cpu_utilization_pct,
                0.0,
                100.0,
            )?;
            require_ratio_at_least_one(
                "cell_gates.max_p50_latency_ratio_vs_sqlite",
                gate.max_p50_latency_ratio_vs_sqlite,
            )?;
            require_ratio_at_least_one(
                "cell_gates.max_p95_latency_ratio_vs_sqlite",
                gate.max_p95_latency_ratio_vs_sqlite,
            )?;
            require_ratio_at_least_one(
                "cell_gates.max_p99_latency_ratio_vs_sqlite",
                gate.max_p99_latency_ratio_vs_sqlite,
            )?;
            require_ratio_at_least_one(
                "cell_gates.max_responsiveness_regression_ratio_vs_sqlite",
                gate.max_responsiveness_regression_ratio_vs_sqlite,
            )?;
            if gate.max_topology_reassignments_per_run == 0 {
                return Err(format!(
                    "cell gate `{}` must allow at least one topology-reassignment slot",
                    gate.cell
                ));
            }
        }

        if seen_cells.len() != required_cell_suffixes.len() {
            return Err(format!(
                "cell gate count mismatch actual={} expected={}",
                seen_cells.len(),
                required_cell_suffixes.len()
            ));
        }

        let unit_tests = unique_set("verification_plan.unit_tests", &self.verification_plan.unit_tests)?;
        let e2e_scenarios =
            unique_set("verification_plan.e2e_scenarios", &self.verification_plan.e2e_scenarios)?;
        let logging_artifacts = unique_set(
            "verification_plan.logging_artifacts",
            &self.verification_plan.logging_artifacts,
        )?;
        let log_fields = unique_set(
            "verification_plan.required_log_fields",
            &self.verification_plan.required_log_fields,
        )?;
        if unit_tests.is_empty() || e2e_scenarios.is_empty() || logging_artifacts.is_empty() {
            return Err("verification_plan must list unit tests, scenarios, and artifacts".to_owned());
        }
        for required_field in REQUIRED_LOG_FIELDS {
            if !log_fields.contains(required_field) {
                return Err(format!(
                    "verification_plan.required_log_fields missing `{required_field}`"
                ));
            }
        }

        for path in [
            &self.references.contract_module,
            &self.references.verification_test,
            &self.references.campaign_matrix_module,
            &self.references.score_engine_module,
            &self.references.confidence_gates_module,
            &self.references.release_certificate_module,
            &self.references.verifier_script,
        ] {
            require_non_empty("references.path", path)?;
            let resolved = workspace_root.join(path);
            if !resolved.exists() {
                return Err(format!(
                    "referenced path does not exist path={}",
                    resolved.display()
                ));
            }
        }

        for mode_id in &required_mode_ids {
            if !matches!(
                mode_id.as_str(),
                "sqlite_reference" | "fsqlite_mvcc" | "fsqlite_single_writer"
            ) {
                return Err(format!("unexpected required mode `{mode_id}`"));
            }
        }

        let mut covered_suffixes = BTreeSet::new();
        for scenario_id in &e2e_scenarios {
            let matched_suffix = required_cell_suffixes
                .iter()
                .find(|suffix| scenario_id.ends_with(suffix.as_str()))
                .ok_or_else(|| {
                    format!(
                        "campaign row `{scenario_id}` does not end with a required cell suffix"
                    )
                })?;
            covered_suffixes.insert(matched_suffix.clone());
        }

        if covered_suffixes != required_cell_suffixes {
            return Err(format!(
                "verification_plan.e2e_scenarios does not cover all required suffixes actual={covered_suffixes:?} expected={required_cell_suffixes:?}"
            ));
        }

        Ok(())
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

fn require_eq(field: &str, value: &str, expected: &str) -> Result<(), String> {
    if value != expected {
        return Err(format!("{field} expected `{expected}` but found `{value}`"));
    }
    Ok(())
}

fn require_ratio_positive(field: &str, value: f64) -> Result<(), String> {
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{field} must be finite and > 0, found {value}"));
    }
    Ok(())
}

fn require_ratio_at_least_one(field: &str, value: f64) -> Result<(), String> {
    if !value.is_finite() || value < 1.0 {
        return Err(format!("{field} must be finite and >= 1.0, found {value}"));
    }
    Ok(())
}

fn require_pct(field: &str, value: f64, min: f64, max: f64) -> Result<(), String> {
    if !value.is_finite() || !(min..=max).contains(&value) {
        return Err(format!(
            "{field} must be finite and in [{min}, {max}], found {value}"
        ));
    }
    Ok(())
}

fn unique_set(field: &str, values: &[String]) -> Result<BTreeSet<String>, String> {
    let mut seen = BTreeSet::new();
    for value in values {
        require_non_empty(field, value)?;
        if !seen.insert(value.clone()) {
            return Err(format!("{field} contains duplicate value `{value}`"));
        }
    }
    Ok(seen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_gate_lookup_returns_expected_gate() {
        let criteria = LeapfrogExitCriteria {
            meta: ExitCriteriaMeta {
                schema_version: LEAPFROG_EXIT_CRITERIA_SCHEMA_V1.to_owned(),
                policy_id: "test".to_owned(),
                bead_id: BEAD_ID.to_owned(),
                track_id: "bd-db300.7".to_owned(),
                generated_at: "2026-03-10".to_owned(),
                owner: "tester".to_owned(),
                decision_summary: "summary".to_owned(),
                rationale: "rationale".to_owned(),
            },
            campaign: CampaignRequirements {
                manifest_path: "manifest.json".to_owned(),
                required_modes: vec!["sqlite_reference".to_owned()],
                required_placement_profiles: vec!["recommended_pinned".to_owned()],
                required_cell_suffixes: vec!["c1".to_owned()],
            },
            scorecard: ScorecardPolicy {
                recommended_profile_id: "recommended_pinned".to_owned(),
                baseline_profile_id: "recommended_pinned".to_owned(),
                adversarial_profile_id: "recommended_pinned".to_owned(),
                claim_language: "claim".to_owned(),
                claim_forbidden_when_any_fail: true,
            },
            cell_gates: vec![CellGate {
                cell: "c1".to_owned(),
                goal: "goal".to_owned(),
                recommended_min_throughput_ratio_vs_sqlite: 1.05,
                baseline_catastrophic_floor_ratio_vs_sqlite: 0.95,
                adversarial_catastrophic_floor_ratio_vs_sqlite: 0.90,
                max_retry_rate: 0.01,
                min_cpu_utilization_pct: 45.0,
                max_p50_latency_ratio_vs_sqlite: 1.10,
                max_p95_latency_ratio_vs_sqlite: 1.20,
                max_p99_latency_ratio_vs_sqlite: 1.30,
                max_responsiveness_regression_ratio_vs_sqlite: 1.15,
                max_topology_reassignments_per_run: 2,
            }],
            verification_plan: VerificationPlan {
                unit_tests: vec!["test".to_owned()],
                e2e_scenarios: vec!["scenario".to_owned()],
                logging_artifacts: vec!["artifact".to_owned()],
                required_log_fields: REQUIRED_LOG_FIELDS
                    .iter()
                    .map(|field| (*field).to_owned())
                    .collect(),
            },
            references: ExitCriteriaReferences {
                contract_module: "contract".to_owned(),
                verification_test: "test".to_owned(),
                campaign_matrix_module: "fixture_select".to_owned(),
                score_engine_module: "score".to_owned(),
                confidence_gates_module: "gate".to_owned(),
                release_certificate_module: "cert".to_owned(),
                verifier_script: "script".to_owned(),
            },
        };

        let Some(gate) = criteria.cell_gate("c1") else {
            panic!("expected c1 gate");
        };
        assert_eq!(gate.cell, "c1");
        assert!(criteria.cell_gate("c8").is_none());
    }
}
