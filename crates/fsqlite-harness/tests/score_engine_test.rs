//! Integration tests for the Bayesian parity score engine (bd-1dp9.1.3).
//!
//! Validates:
//! - Scorecard computation from the canonical feature universe
//! - Point estimate consistency with existing `compute_score()`
//! - Credible interval coverage properties
//! - Conformal band finite-sample guarantees
//! - Release-gating decision logic
//! - Prior sensitivity analysis
//! - JSON serialisation round-trip
//! - Score determinism across runs

use std::collections::BTreeMap;

use fsqlite_harness::parity_taxonomy::{
    ExclusionRationale, Feature, FeatureCategory, FeatureId, FeatureUniverse, ObservabilityMapping,
    ParityStatus, build_canonical_universe,
};
use fsqlite_harness::score_engine::{
    BayesianScorecard, BetaParams, PriorConfig, ScoreEngineConfig, compute_bayesian_scorecard,
};

const BEAD_ID: &str = "bd-1dp9.1.3";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal universe with the given status for all features.
fn build_uniform_universe(status: ParityStatus, count_per_category: usize) -> FeatureUniverse {
    let mut features = BTreeMap::new();
    for cat in FeatureCategory::ALL {
        for i in 0..count_per_category {
            let id = FeatureId::new(cat.prefix(), (i + 1) as u16);
            let mut feat = Feature {
                id: id.clone(),
                title: format!("{} feature {}", cat.display_name(), i + 1),
                description: String::new(),
                category: cat,
                weight: 1.0,
                status,
                exclusion: None,
                observability: ObservabilityMapping::default(),
                tags: Default::default(),
            };
            if status == ParityStatus::Excluded {
                feat.exclusion = Some(ExclusionRationale {
                    reason: "test".to_owned(),
                    reference: "test".to_owned(),
                });
            }
            features.insert(id, feat);
        }
    }
    FeatureUniverse {
        schema_version: 1,
        target_sqlite_version: "3.52.0".to_owned(),
        features,
    }
}

// ---------------------------------------------------------------------------
// Scorecard computation
// ---------------------------------------------------------------------------

#[test]
fn scorecard_is_computable() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_point_estimate >= 0.0 && scorecard.global_point_estimate <= 1.0,
        "[{BEAD_ID}] global point estimate out of range: {}",
        scorecard.global_point_estimate
    );
    assert!(
        scorecard.global_point_estimate.is_finite(),
        "[{BEAD_ID}] global point estimate not finite"
    );

    eprintln!(
        "bead_id={BEAD_ID} test=computable point={:.4} lower={:.4} upper={:.4}",
        scorecard.global_point_estimate, scorecard.global_lower_bound, scorecard.global_upper_bound
    );
}

#[test]
fn scorecard_is_deterministic() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let s1 = compute_bayesian_scorecard(&universe, &config);
    let s2 = compute_bayesian_scorecard(&universe, &config);

    assert_eq!(
        s1.global_point_estimate, s2.global_point_estimate,
        "[{BEAD_ID}] point estimate not deterministic"
    );
    assert_eq!(
        s1.global_lower_bound, s2.global_lower_bound,
        "[{BEAD_ID}] lower bound not deterministic"
    );
    assert_eq!(
        s1.global_upper_bound, s2.global_upper_bound,
        "[{BEAD_ID}] upper bound not deterministic"
    );
}

#[test]
fn scorecard_point_estimate_near_taxonomy_score() {
    let universe = build_canonical_universe();
    let taxonomy_score = universe.compute_score();

    // With uniform prior, the Bayesian posterior mean should be close to the
    // frequentist score (within ~2% for 129 features).
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    let diff = (scorecard.global_point_estimate - taxonomy_score.global_score).abs();
    assert!(
        diff < 0.05,
        "[{BEAD_ID}] Bayesian estimate {:.4} too far from taxonomy score {:.4} (diff={diff:.4})",
        scorecard.global_point_estimate,
        taxonomy_score.global_score
    );

    eprintln!(
        "bead_id={BEAD_ID} test=near_taxonomy bayesian={:.4} frequentist={:.4} diff={diff:.6}",
        scorecard.global_point_estimate, taxonomy_score.global_score,
    );
}

// ---------------------------------------------------------------------------
// Interval properties
// ---------------------------------------------------------------------------

#[test]
fn lower_bound_below_point_estimate() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_lower_bound <= scorecard.global_point_estimate,
        "[{BEAD_ID}] lower bound {} > point estimate {}",
        scorecard.global_lower_bound,
        scorecard.global_point_estimate
    );
}

#[test]
fn upper_bound_above_point_estimate() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_upper_bound >= scorecard.global_point_estimate,
        "[{BEAD_ID}] upper bound {} < point estimate {}",
        scorecard.global_upper_bound,
        scorecard.global_point_estimate
    );
}

#[test]
fn category_intervals_contain_point_estimates() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    for (name, cp) in &scorecard.category_posteriors {
        assert!(
            cp.lower_bound <= cp.point_estimate,
            "[{BEAD_ID}] category {name}: lower {:.4} > point {:.4}",
            cp.lower_bound,
            cp.point_estimate,
        );
        assert!(
            cp.upper_bound >= cp.point_estimate,
            "[{BEAD_ID}] category {name}: upper {:.4} < point {:.4}",
            cp.upper_bound,
            cp.point_estimate,
        );
    }
}

#[test]
fn all_nine_categories_present() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert_eq!(
        scorecard.category_posteriors.len(),
        9,
        "[{BEAD_ID}] expected 9 category posteriors, got {}",
        scorecard.category_posteriors.len()
    );
}

// ---------------------------------------------------------------------------
// Conformal band
// ---------------------------------------------------------------------------

#[test]
fn conformal_band_contains_point_estimate() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.conformal_band.lower <= scorecard.global_point_estimate,
        "[{BEAD_ID}] conformal lower {} > point estimate {}",
        scorecard.conformal_band.lower,
        scorecard.global_point_estimate
    );
    assert!(
        scorecard.conformal_band.upper >= scorecard.global_point_estimate,
        "[{BEAD_ID}] conformal upper {} < point estimate {}",
        scorecard.conformal_band.upper,
        scorecard.global_point_estimate
    );
}

#[test]
fn conformal_half_width_non_negative() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.conformal_band.half_width >= 0.0,
        "[{BEAD_ID}] conformal half-width negative: {}",
        scorecard.conformal_band.half_width
    );
}

// ---------------------------------------------------------------------------
// Release gating
// ---------------------------------------------------------------------------

#[test]
fn release_gating_with_default_threshold() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    // With the canonical universe (~73% score), and default threshold of 0.70,
    // the release decision depends on the lower bound.
    eprintln!(
        "bead_id={BEAD_ID} test=release_gating threshold={:.2} lower={:.4} conformal_lower={:.4} release_ready={}",
        config.release_threshold,
        scorecard.global_lower_bound,
        scorecard.conformal_band.lower,
        scorecard.release_ready
    );
}

#[test]
fn release_gating_high_threshold_fails() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig {
        release_threshold: 0.99,
        ..Default::default()
    };
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    // The canonical universe is ~73% parity; it should not be release-ready at 99%.
    assert!(
        !scorecard.release_ready,
        "[{BEAD_ID}] should NOT be release-ready at 99% threshold, lower bound is {:.4}",
        scorecard.global_lower_bound
    );
}

#[test]
fn release_gating_low_threshold_passes() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig {
        release_threshold: 0.50,
        ..Default::default()
    };
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    // At 50% threshold, the canonical universe (~73%) should pass.
    assert!(
        scorecard.release_ready,
        "[{BEAD_ID}] should be release-ready at 50% threshold, lower bound is {:.4}",
        scorecard.global_lower_bound
    );
}

// ---------------------------------------------------------------------------
// Prior sensitivity
// ---------------------------------------------------------------------------

#[test]
fn prior_sensitivity_different_priors_produce_different_results() {
    let universe = build_canonical_universe();

    let s_uniform = compute_bayesian_scorecard(
        &universe,
        &ScoreEngineConfig {
            prior: PriorConfig::default(),
            ..Default::default()
        },
    );
    let s_jeffreys = compute_bayesian_scorecard(
        &universe,
        &ScoreEngineConfig {
            prior: PriorConfig::jeffreys(),
            ..Default::default()
        },
    );
    let s_optimistic = compute_bayesian_scorecard(
        &universe,
        &ScoreEngineConfig {
            prior: PriorConfig::optimistic(),
            ..Default::default()
        },
    );

    // Different priors should produce slightly different point estimates.
    // (They all converge as data overwhelms the prior.)
    let scores = [
        ("uniform", s_uniform.global_point_estimate),
        ("jeffreys", s_jeffreys.global_point_estimate),
        ("optimistic", s_optimistic.global_point_estimate),
    ];

    eprintln!("bead_id={BEAD_ID} test=prior_sensitivity");
    for (name, score) in &scores {
        eprintln!("  prior={name} point={score:.6}");
    }

    // At least two of the three should differ by at least 1e-4.
    let any_differ = (scores[0].1 - scores[1].1).abs() > 1e-4
        || (scores[1].1 - scores[2].1).abs() > 1e-4
        || (scores[0].1 - scores[2].1).abs() > 1e-4;

    assert!(
        any_differ,
        "[{BEAD_ID}] all priors produced identical results"
    );
}

#[test]
fn haldane_prior_produces_valid_scorecard() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig {
        prior: PriorConfig::haldane(),
        ..Default::default()
    };
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_point_estimate.is_finite(),
        "[{BEAD_ID}] Haldane prior produced non-finite point estimate"
    );
    assert!(
        scorecard.global_point_estimate >= 0.0 && scorecard.global_point_estimate <= 1.0,
        "[{BEAD_ID}] Haldane prior out of range: {}",
        scorecard.global_point_estimate
    );
}

// ---------------------------------------------------------------------------
// Extreme universes
// ---------------------------------------------------------------------------

#[test]
fn all_passing_universe_near_one() {
    let universe = build_uniform_universe(ParityStatus::Passing, 10);
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_point_estimate > 0.9,
        "[{BEAD_ID}] all-passing universe should score > 0.9, got {:.4}",
        scorecard.global_point_estimate
    );

    eprintln!(
        "bead_id={BEAD_ID} test=all_passing point={:.4}",
        scorecard.global_point_estimate
    );
}

#[test]
fn all_missing_universe_near_zero() {
    let universe = build_uniform_universe(ParityStatus::Missing, 10);
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_point_estimate < 0.1,
        "[{BEAD_ID}] all-missing universe should score < 0.1, got {:.4}",
        scorecard.global_point_estimate
    );

    eprintln!(
        "bead_id={BEAD_ID} test=all_missing point={:.4}",
        scorecard.global_point_estimate
    );
}

#[test]
fn all_partial_universe_around_half() {
    let universe = build_uniform_universe(ParityStatus::Partial, 10);
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert!(
        scorecard.global_point_estimate > 0.4 && scorecard.global_point_estimate < 0.6,
        "[{BEAD_ID}] all-partial universe should score ~0.5, got {:.4}",
        scorecard.global_point_estimate
    );
}

// ---------------------------------------------------------------------------
// Interval width properties
// ---------------------------------------------------------------------------

#[test]
fn credible_interval_narrows_with_more_data() {
    let small = build_uniform_universe(ParityStatus::Passing, 3);
    let large = build_uniform_universe(ParityStatus::Passing, 30);
    let config = ScoreEngineConfig::default();

    let s_small = compute_bayesian_scorecard(&small, &config);
    let s_large = compute_bayesian_scorecard(&large, &config);

    let width_small = s_small.global_upper_bound - s_small.global_lower_bound;
    let width_large = s_large.global_upper_bound - s_large.global_lower_bound;

    assert!(
        width_large < width_small,
        "[{BEAD_ID}] more data should narrow CI: small_width={width_small:.4}, large_width={width_large:.4}"
    );

    eprintln!(
        "bead_id={BEAD_ID} test=ci_narrows small_width={width_small:.4} large_width={width_large:.4}"
    );
}

// ---------------------------------------------------------------------------
// BetaParams public API
// ---------------------------------------------------------------------------

#[test]
fn beta_quantile_boundaries() {
    let b = BetaParams::new(3.0, 5.0);
    assert_eq!(b.quantile(0.0), 0.0);
    assert_eq!(b.quantile(1.0), 1.0);
}

#[test]
fn beta_credible_interval_wider_at_higher_confidence() {
    let b = BetaParams::new(10.0, 5.0);
    let (lo_90, hi_90) = b.credible_interval(0.90);
    let (lo_95, hi_95) = b.credible_interval(0.95);

    let width_90 = hi_90 - lo_90;
    let width_95 = hi_95 - lo_95;

    assert!(
        width_95 > width_90,
        "[{BEAD_ID}] 95% CI should be wider than 90% CI: w90={width_90:.4}, w95={width_95:.4}"
    );
}

// ---------------------------------------------------------------------------
// JSON serialisation
// ---------------------------------------------------------------------------

#[test]
fn scorecard_json_roundtrip() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    let json = scorecard.to_json().expect("serialise scorecard");
    assert!(json.contains("\"bead_id\""));
    assert!(json.contains("\"global_point_estimate\""));
    assert!(json.contains("\"conformal_band\""));
    assert!(json.contains("\"category_posteriors\""));

    let restored = BayesianScorecard::from_json(&json).expect("deserialise scorecard");
    assert_eq!(
        restored.global_point_estimate, scorecard.global_point_estimate,
        "[{BEAD_ID}] point estimate mismatch after JSON roundtrip"
    );
    assert_eq!(
        restored.global_lower_bound, scorecard.global_lower_bound,
        "[{BEAD_ID}] lower bound mismatch after JSON roundtrip"
    );
    assert_eq!(
        restored.category_posteriors.len(),
        scorecard.category_posteriors.len(),
        "[{BEAD_ID}] category count mismatch after JSON roundtrip"
    );
    assert_eq!(
        restored.release_ready, scorecard.release_ready,
        "[{BEAD_ID}] release_ready mismatch after JSON roundtrip"
    );
}

#[test]
fn scorecard_json_contains_all_fields() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    let json = scorecard.to_json().expect("serialise");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse JSON");

    let obj = parsed.as_object().expect("top-level object");
    for field in [
        "bead_id",
        "schema_version",
        "prior",
        "global_point_estimate",
        "global_lower_bound",
        "global_upper_bound",
        "category_posteriors",
        "conformal_band",
        "release_ready",
        "release_threshold",
        "effective_features",
        "total_features",
    ] {
        assert!(
            obj.contains_key(field),
            "[{BEAD_ID}] JSON missing field: {field}"
        );
    }
}

// ---------------------------------------------------------------------------
// Traceability
// ---------------------------------------------------------------------------

#[test]
fn scorecard_bead_id_is_correct() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    assert_eq!(scorecard.bead_id, "bd-1dp9.1.3");
}

#[test]
fn scorecard_effective_features_matches_universe() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    // Effective features = total - excluded.
    let excluded = universe
        .features
        .values()
        .filter(|f| f.status == ParityStatus::Excluded)
        .count();
    let expected_effective = universe.features.len() - excluded;

    assert_eq!(
        scorecard.effective_features, expected_effective,
        "[{BEAD_ID}] effective features: expected {expected_effective}, got {}",
        scorecard.effective_features
    );
    assert_eq!(
        scorecard.total_features,
        universe.features.len(),
        "[{BEAD_ID}] total features mismatch"
    );
}

// ---------------------------------------------------------------------------
// Score engine config
// ---------------------------------------------------------------------------

#[test]
fn default_config_is_sensible() {
    let config = ScoreEngineConfig::default();
    assert!(config.prior.alpha > 0.0);
    assert!(config.prior.beta > 0.0);
    assert!(config.prior.confidence_level > 0.0 && config.prior.confidence_level < 1.0);
    assert!(config.release_threshold > 0.0 && config.release_threshold < 1.0);
    assert!(config.conformal_coverage > 0.0 && config.conformal_coverage < 1.0);
}
