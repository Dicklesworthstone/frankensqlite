//! Contract tests for parity_score_contract.toml (bd-2yqp6.1.4).
//!
//! Enforces deterministic parity score semantics and strict, unambiguous
//! definition of a "100%" parity claim.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const BEAD_ID: &str = "bd-2yqp6.1.4";

#[derive(Debug, Deserialize)]
struct ParityScoreContractDocument {
    meta: ContractMeta,
    formula: FormulaContract,
    status_weights: StatusWeights,
    exclusions: ExclusionPolicy,
    divergence_policy: DivergencePolicy,
    flaky_policy: FlakyPolicy,
    coverage_debt: CoverageDebtPolicy,
    hundred_percent: HundredPercentPolicy,
    claim_validation: ClaimValidationPolicy,
    references: ContractReferences,
}

#[derive(Debug, Deserialize)]
struct ContractMeta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    generated_at: String,
    contract_owner: String,
}

#[derive(Debug, Deserialize)]
struct FormulaContract {
    score_name: String,
    numerator: String,
    denominator: String,
    result: String,
    rounding_mode: String,
    included_statuses: Vec<String>,
    excluded_statuses: Vec<String>,
    source_taxonomy: String,
}

#[derive(Debug, Deserialize)]
struct StatusWeights {
    pass: f64,
    partial: f64,
    fail: f64,
}

#[derive(Debug, Deserialize)]
struct ExclusionPolicy {
    score_denominator_treatment: String,
    hundred_percent_treatment: String,
    require_documented_rationale: bool,
}

#[derive(Debug, Deserialize)]
struct DivergencePolicy {
    open_divergences_block_hundred_percent: bool,
    requires_documented_intentional_divergence: bool,
}

#[derive(Debug, Deserialize)]
struct FlakyPolicy {
    allow_flaky_in_hundred_percent: bool,
    required_stable_replays: u32,
    required_pass_rate: f64,
}

#[derive(Debug, Deserialize)]
struct CoverageDebtPolicy {
    definition: String,
    strict_hundred_percent_requires_zero: bool,
}

#[derive(Debug, Deserialize)]
struct HundredPercentPolicy {
    required_score: f64,
    max_fail_features: u32,
    max_partial_features: u32,
    max_excluded_features: u32,
    max_open_divergences: u32,
    max_flaky_failures: u32,
    max_coverage_debt_items: u32,
}

#[derive(Debug, Deserialize)]
struct ClaimValidationPolicy {
    disallow_inequality_operators: bool,
    disallow_approximation_terms: bool,
    forbidden_terms: Vec<String>,
    required_fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ContractReferences {
    taxonomy: String,
    surface_matrix: String,
    feature_ledger: String,
    verification_contract_module: String,
    ratchet_policy_module: String,
}

#[derive(Debug, Deserialize)]
struct TaxonomyDocument {
    features: Vec<TaxonomyFeature>,
}

#[derive(Debug, Clone, Deserialize)]
struct TaxonomyFeature {
    status: String,
    weight: u32,
}

#[derive(Debug)]
struct ParityClaim<'a> {
    claim_text: &'a str,
    score: f64,
    fail_features: u32,
    partial_features: u32,
    excluded_features: u32,
    open_divergences: u32,
    flaky_failures: u32,
    coverage_debt_items: u32,
}

#[derive(Debug)]
struct ClaimVerdict {
    accepted: bool,
    reasons: Vec<String>,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_text(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    })
}

fn read_contract() -> ParityScoreContractDocument {
    let contract_path = workspace_root().join("parity_score_contract.toml");
    toml::from_str(&read_text(&contract_path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", contract_path.display());
    })
}

fn read_taxonomy(path: &str) -> TaxonomyDocument {
    let taxonomy_path = workspace_root().join(path);
    toml::from_str(&read_text(&taxonomy_path)).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", taxonomy_path.display());
    })
}

fn truncate_6dp(value: f64) -> f64 {
    (value * 1_000_000.0).trunc() / 1_000_000.0
}

fn status_in(status: &str, values: &[String]) -> bool {
    values.iter().any(|candidate| candidate == status)
}

fn status_weight(contract: &ParityScoreContractDocument, status: &str) -> Option<f64> {
    match status {
        "pass" => Some(contract.status_weights.pass),
        "partial" => Some(contract.status_weights.partial),
        "fail" => Some(contract.status_weights.fail),
        _ => None,
    }
}

fn contains_standalone_term(text: &str, term: &str) -> bool {
    if term.is_empty() {
        return false;
    }
    let mut start = 0_usize;
    while let Some(offset) = text[start..].find(term) {
        let term_start = start + offset;
        let term_end = term_start + term.len();

        let before = text[..term_start].chars().next_back();
        let after = text[term_end..].chars().next();

        let before_is_boundary = match before {
            Some(ch) => !ch.is_ascii_alphanumeric() && ch != '_',
            None => true,
        };
        let after_is_boundary = match after {
            Some(ch) => !ch.is_ascii_alphanumeric() && ch != '_',
            None => true,
        };

        if before_is_boundary && after_is_boundary {
            return true;
        }
        start = term_end;
    }
    false
}

fn compute_weighted_parity_score(
    contract: &ParityScoreContractDocument,
    features: &[TaxonomyFeature],
) -> f64 {
    let mut numerator = 0.0_f64;
    let mut denominator = 0.0_f64;

    for feature in features {
        let status = feature.status.as_str();
        let weight = f64::from(feature.weight);
        if status_in(status, &contract.formula.included_statuses) {
            let Some(weight_multiplier) = status_weight(contract, status) else {
                panic!("status '{status}' has no configured score weight");
            };
            numerator += weight * weight_multiplier;
            denominator += weight;
            continue;
        }
        if status_in(status, &contract.formula.excluded_statuses) {
            continue;
        }
        panic!("status '{status}' is neither included nor excluded");
    }

    assert!(
        denominator > 0.0,
        "score denominator must be positive after exclusions"
    );
    truncate_6dp(numerator / denominator)
}

fn evaluate_claim(contract: &ParityScoreContractDocument, claim: &ParityClaim<'_>) -> ClaimVerdict {
    let mut reasons = Vec::new();
    let lower = claim.claim_text.to_lowercase();

    if contract.claim_validation.disallow_inequality_operators {
        for operator in [">=", "<=", ">", "<", "≈", "~"] {
            if lower.contains(operator) {
                reasons.push(format!("ambiguous_operator:{operator}"));
            }
        }
    }

    if contract.claim_validation.disallow_approximation_terms {
        for term in &contract.claim_validation.forbidden_terms {
            let term_lower = term.to_lowercase();
            if contains_standalone_term(&lower, &term_lower) {
                reasons.push(format!("ambiguous_term:{term}"));
            }
        }
    }

    for required_field in &contract.claim_validation.required_fields {
        if !lower.contains(required_field) {
            reasons.push(format!("missing_field_token:{required_field}"));
        }
    }

    if truncate_6dp(claim.score) != truncate_6dp(contract.hundred_percent.required_score) {
        reasons.push("score_not_exact_hundred_percent".to_owned());
    }
    if claim.fail_features > contract.hundred_percent.max_fail_features {
        reasons.push("fail_features_nonzero".to_owned());
    }
    if claim.partial_features > contract.hundred_percent.max_partial_features {
        reasons.push("partial_features_nonzero".to_owned());
    }
    if claim.excluded_features > contract.hundred_percent.max_excluded_features {
        reasons.push("excluded_features_nonzero".to_owned());
    }
    if claim.open_divergences > contract.hundred_percent.max_open_divergences {
        reasons.push("open_divergences_nonzero".to_owned());
    }
    if claim.flaky_failures > contract.hundred_percent.max_flaky_failures {
        reasons.push("flaky_failures_nonzero".to_owned());
    }
    if claim.coverage_debt_items > contract.hundred_percent.max_coverage_debt_items {
        reasons.push("coverage_debt_nonzero".to_owned());
    }

    ClaimVerdict {
        accepted: reasons.is_empty(),
        reasons,
    }
}

fn strict_claim_text(score: f64) -> String {
    format!(
        "score={score:.6}; fail_features=0; partial_features=0; excluded_features=0; open_divergences=0; flaky_failures=0; coverage_debt_items=0"
    )
}

#[test]
fn contract_schema_and_thresholds_are_strict() {
    let contract = read_contract();

    assert_eq!(contract.meta.schema_version, "1.0.0");
    assert_eq!(contract.meta.bead_id, BEAD_ID);
    assert_eq!(contract.meta.track_id, "bd-2yqp6.1");
    assert!(!contract.meta.generated_at.trim().is_empty());
    assert!(!contract.meta.contract_owner.trim().is_empty());

    assert_eq!(contract.formula.score_name, "weighted_parity_score");
    assert_eq!(contract.formula.rounding_mode, "truncate_6dp");
    assert!(contract.formula.numerator.contains("status_weight"));
    assert!(contract.formula.denominator.contains("included_statuses"));
    assert!(contract.formula.result.contains("truncate_6dp"));
    assert!(status_in("pass", &contract.formula.included_statuses));
    assert!(status_in("partial", &contract.formula.included_statuses));
    assert!(status_in("fail", &contract.formula.included_statuses));
    assert!(status_in("excluded", &contract.formula.excluded_statuses));

    assert!((contract.status_weights.pass - 1.0).abs() < f64::EPSILON);
    assert!((contract.status_weights.partial - 0.5).abs() < f64::EPSILON);
    assert!((contract.status_weights.fail - 0.0).abs() < f64::EPSILON);

    assert!(contract.exclusions.require_documented_rationale);
    assert_eq!(
        contract.exclusions.score_denominator_treatment,
        "excluded_features_removed_from_denominator"
    );
    assert_eq!(
        contract.exclusions.hundred_percent_treatment,
        "excluded_features_count_as_coverage_debt"
    );

    assert!(
        contract
            .divergence_policy
            .open_divergences_block_hundred_percent
    );
    assert!(
        contract
            .divergence_policy
            .requires_documented_intentional_divergence
    );
    assert!(!contract.flaky_policy.allow_flaky_in_hundred_percent);
    assert_eq!(contract.flaky_policy.required_stable_replays, 3);
    assert!((contract.flaky_policy.required_pass_rate - 1.0).abs() < f64::EPSILON);
    assert!(contract.coverage_debt.strict_hundred_percent_requires_zero);
    assert!(
        contract
            .coverage_debt
            .definition
            .contains("coverage_debt_items =")
    );

    assert!((contract.hundred_percent.required_score - 1.0).abs() < f64::EPSILON);
    assert_eq!(contract.hundred_percent.max_fail_features, 0);
    assert_eq!(contract.hundred_percent.max_partial_features, 0);
    assert_eq!(contract.hundred_percent.max_excluded_features, 0);
    assert_eq!(contract.hundred_percent.max_open_divergences, 0);
    assert_eq!(contract.hundred_percent.max_flaky_failures, 0);
    assert_eq!(contract.hundred_percent.max_coverage_debt_items, 0);

    assert!(contract.claim_validation.disallow_inequality_operators);
    assert!(contract.claim_validation.disallow_approximation_terms);
    assert!(!contract.claim_validation.forbidden_terms.is_empty());
    assert!(!contract.claim_validation.required_fields.is_empty());

    assert_eq!(contract.references.taxonomy, "parity_taxonomy.toml");
    assert_eq!(
        contract.references.surface_matrix,
        "supported_surface_matrix.toml"
    );
    assert_eq!(
        contract.references.feature_ledger,
        "feature_universe_ledger.toml"
    );
    assert!(
        contract
            .references
            .verification_contract_module
            .contains("verification_contract_enforcement.rs")
    );
    assert!(
        contract
            .references
            .ratchet_policy_module
            .contains("ratchet_policy.rs")
    );
}

#[test]
fn formula_recomputes_taxonomy_deterministically() {
    let contract = read_contract();
    let taxonomy = read_taxonomy(&contract.formula.source_taxonomy);
    let score_a = compute_weighted_parity_score(&contract, &taxonomy.features);
    let score_b = compute_weighted_parity_score(&contract, &taxonomy.features);

    assert!(
        (score_a - score_b).abs() < f64::EPSILON,
        "score recompute mismatch: {score_a} vs {score_b}"
    );
    assert!(
        (0.0..=1.0).contains(&score_a),
        "score must be in [0,1], got {score_a}"
    );
}

#[test]
fn exclusions_are_removed_from_denominator() {
    let contract = read_contract();
    let synthetic = vec![
        TaxonomyFeature {
            status: "pass".to_owned(),
            weight: 10,
        },
        TaxonomyFeature {
            status: "fail".to_owned(),
            weight: 10,
        },
        TaxonomyFeature {
            status: "partial".to_owned(),
            weight: 10,
        },
        TaxonomyFeature {
            status: "excluded".to_owned(),
            weight: 200,
        },
    ];
    let score = compute_weighted_parity_score(&contract, &synthetic);
    assert!(
        (score - 0.5).abs() < f64::EPSILON,
        "expected excluded rows to be removed from denominator, got {score}"
    );
}

#[test]
fn claim_validation_rejects_ambiguous_or_partial_hundred_percent_claims() {
    let contract = read_contract();
    let strict_text = strict_claim_text(1.0);
    let clean_claim = ParityClaim {
        claim_text: &strict_text,
        score: 1.0,
        fail_features: 0,
        partial_features: 0,
        excluded_features: 0,
        open_divergences: 0,
        flaky_failures: 0,
        coverage_debt_items: 0,
    };
    let pass_verdict = evaluate_claim(&contract, &clean_claim);
    assert!(
        pass_verdict.accepted,
        "expected strict claim to pass, got {:?}",
        pass_verdict.reasons
    );

    let ambiguous_claim = ParityClaim {
        claim_text: "score>=1.0; fail_features=0; partial_features=0; excluded_features=0; open_divergences=0; flaky_failures=0; coverage_debt_items=0; approx",
        ..clean_claim
    };
    let ambiguous_verdict = evaluate_claim(&contract, &ambiguous_claim);
    assert!(
        !ambiguous_verdict.accepted,
        "ambiguous claim must be rejected"
    );
    assert!(
        ambiguous_verdict
            .reasons
            .iter()
            .any(|reason| reason.starts_with("ambiguous_")),
        "expected ambiguous_* rejection reasons, got {:?}",
        ambiguous_verdict.reasons
    );

    let partial_claim = ParityClaim {
        partial_features: 1,
        ..clean_claim
    };
    let partial_verdict = evaluate_claim(&contract, &partial_claim);
    assert!(!partial_verdict.accepted, "partial claim must be rejected");
    assert!(
        partial_verdict
            .reasons
            .iter()
            .any(|reason| reason == "partial_features_nonzero")
    );

    let flaky_claim = ParityClaim {
        flaky_failures: 1,
        ..clean_claim
    };
    let flaky_verdict = evaluate_claim(&contract, &flaky_claim);
    assert!(!flaky_verdict.accepted, "flaky claim must be rejected");
    assert!(
        flaky_verdict
            .reasons
            .iter()
            .any(|reason| reason == "flaky_failures_nonzero")
    );

    let divergence_claim = ParityClaim {
        open_divergences: 1,
        ..clean_claim
    };
    let divergence_verdict = evaluate_claim(&contract, &divergence_claim);
    assert!(
        !divergence_verdict.accepted,
        "open divergence claim must be rejected"
    );
    assert!(
        divergence_verdict
            .reasons
            .iter()
            .any(|reason| reason == "open_divergences_nonzero")
    );

    let debt_claim = ParityClaim {
        excluded_features: 1,
        coverage_debt_items: 1,
        ..clean_claim
    };
    let debt_verdict = evaluate_claim(&contract, &debt_claim);
    assert!(
        !debt_verdict.accepted,
        "coverage debt claim must be rejected"
    );
    assert!(
        debt_verdict
            .reasons
            .iter()
            .any(|reason| reason == "excluded_features_nonzero")
    );
    assert!(
        debt_verdict
            .reasons
            .iter()
            .any(|reason| reason == "coverage_debt_nonzero")
    );
}
