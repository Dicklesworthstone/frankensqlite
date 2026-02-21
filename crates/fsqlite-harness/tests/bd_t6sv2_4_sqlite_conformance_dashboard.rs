//! Harness integration tests for bd-t6sv2.4: SQLite Conformance & Compatibility Dashboard.
//!
//! Validates: feature taxonomy completeness, Bayesian scorecard generation,
//! MVCC divergence cataloging, per-category parity breakdowns, feature matrix
//! serialization, dashboard report determinism, and conformance summary.

use std::collections::BTreeSet;

use fsqlite_harness::parity_taxonomy::{
    build_canonical_universe, truncate_score, ExclusionRationale, Feature, FeatureCategory,
    FeatureId, FeatureUniverse, ObservabilityMapping, ParityStatus,
};
use fsqlite_harness::score_engine::{
    compute_bayesian_scorecard, PriorConfig, ScoreEngineConfig,
};

const BEAD_ID: &str = "bd-t6sv2.4";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal test universe with known features per category.
fn build_test_universe() -> FeatureUniverse {
    let mut features = std::collections::BTreeMap::new();

    // Add exactly one feature per category with known statuses for deterministic scoring.
    let specs: &[(FeatureCategory, &str, f64, ParityStatus)] = &[
        (FeatureCategory::SqlGrammar, "SELECT basic", 1.0, ParityStatus::Passing),
        (FeatureCategory::VdbeOpcodes, "OP_Init", 1.0, ParityStatus::Passing),
        (FeatureCategory::StorageTransaction, "WAL mode", 1.0, ParityStatus::Partial),
        (FeatureCategory::Pragma, "journal_mode", 1.0, ParityStatus::Passing),
        (FeatureCategory::BuiltinFunctions, "abs()", 1.0, ParityStatus::Passing),
        (FeatureCategory::Extensions, "JSON1", 1.0, ParityStatus::Missing),
        (FeatureCategory::TypeSystem, "INTEGER affinity", 1.0, ParityStatus::Passing),
        (FeatureCategory::FileFormat, "Page header", 1.0, ParityStatus::Passing),
        (FeatureCategory::ApiCli, "sqlite3_open", 1.0, ParityStatus::Passing),
    ];

    for (i, &(cat, title, weight, status)) in specs.iter().enumerate() {
        let id = FeatureId::new(cat.prefix(), (i + 1) as u16);
        features.insert(
            id.clone(),
            Feature {
                id,
                title: title.to_owned(),
                description: format!("Test feature: {title}"),
                category: cat,
                weight,
                status,
                exclusion: None,
                observability: ObservabilityMapping::default(),
                tags: BTreeSet::new(),
            },
        );
    }

    FeatureUniverse {
        schema_version: 1,
        target_sqlite_version: "3.52.0".to_owned(),
        features,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: Canonical universe has features in all 9 categories.
#[test]
fn test_canonical_universe_category_coverage() {
    let universe = build_canonical_universe();
    println!("[{BEAD_ID}] canonical universe: {} features", universe.features.len());

    for cat in FeatureCategory::ALL {
        let count = universe.features_by_category(cat).len();
        assert!(
            count > 0,
            "bead_id={BEAD_ID} category={cat} has zero features"
        );
        println!("  {}: {} features", cat.display_name(), count);
    }

    // Must have at least 50 features total (realistic universe).
    assert!(
        universe.features.len() >= 50,
        "bead_id={BEAD_ID} too few features: {}",
        universe.features.len()
    );
}

/// Test 2: Taxonomy validation passes (no structural violations).
#[test]
fn test_taxonomy_validation_passes() {
    let universe = build_canonical_universe();
    let violations = universe.validate();
    println!("[{BEAD_ID}] taxonomy violations: {}", violations.len());
    for v in &violations {
        println!("  VIOLATION: {v}");
    }
    assert!(
        violations.is_empty(),
        "bead_id={BEAD_ID} taxonomy has {} violations",
        violations.len()
    );
}

/// Test 3: Parity score computation is deterministic and bounded.
#[test]
fn test_parity_score_determinism() {
    let universe = build_canonical_universe();
    let score1 = universe.compute_score();
    let score2 = universe.compute_score();

    println!("[{BEAD_ID}] global score: {}", score1.global_score);
    println!("[{BEAD_ID}] status: passing={} partial={} missing={} excluded={}",
        score1.status_counts.passing, score1.status_counts.partial,
        score1.status_counts.missing, score1.status_counts.excluded);

    // Determinism: two runs produce identical score.
    assert_eq!(
        score1.global_score, score2.global_score,
        "bead_id={BEAD_ID} score not deterministic"
    );

    // Bounded: score in [0, 1].
    assert!(
        (0.0..=1.0).contains(&score1.global_score),
        "bead_id={BEAD_ID} score out of bounds: {}",
        score1.global_score
    );

    // Every category should have a score.
    assert_eq!(
        score1.category_scores.len(),
        9,
        "bead_id={BEAD_ID} expected 9 category scores"
    );
}

/// Test 4: Bayesian scorecard generation with default config.
#[test]
fn test_bayesian_scorecard_generation() {
    let universe = build_canonical_universe();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);

    println!("[{BEAD_ID}] bayesian point estimate: {}", scorecard.global_point_estimate);
    println!("[{BEAD_ID}] lower bound: {}", scorecard.global_lower_bound);
    println!("[{BEAD_ID}] upper bound: {}", scorecard.global_upper_bound);
    println!("[{BEAD_ID}] release_ready: {}", scorecard.release_ready);
    println!("[{BEAD_ID}] effective features: {}/{}", scorecard.effective_features, scorecard.total_features);

    // Point estimate in [0, 1].
    assert!(
        (0.0..=1.0).contains(&scorecard.global_point_estimate),
        "bead_id={BEAD_ID} point estimate out of bounds"
    );

    // Lower bound <= point estimate <= upper bound.
    assert!(
        scorecard.global_lower_bound <= scorecard.global_point_estimate,
        "bead_id={BEAD_ID} lower_bound > point_estimate"
    );
    assert!(
        scorecard.global_point_estimate <= scorecard.global_upper_bound,
        "bead_id={BEAD_ID} point_estimate > upper_bound"
    );

    // Conformal band half-width is non-negative.
    assert!(
        scorecard.conformal_band.half_width >= 0.0,
        "bead_id={BEAD_ID} negative conformal half_width"
    );

    // Scorecard must have posteriors for all 9 categories.
    assert_eq!(
        scorecard.category_posteriors.len(),
        9,
        "bead_id={BEAD_ID} expected 9 category posteriors"
    );
}

/// Test 5: MVCC divergence features are properly cataloged.
#[test]
fn test_mvcc_divergence_catalog() {
    let universe = build_canonical_universe();

    // Collect features tagged with concurrency-related tags.
    let concurrency_features = universe.features_by_tag("concurrency");
    let mvcc_features = universe.features_by_tag("mvcc");
    let wal_features = universe.features_by_tag("wal");

    println!("[{BEAD_ID}] concurrency-tagged: {}", concurrency_features.len());
    println!("[{BEAD_ID}] mvcc-tagged: {}", mvcc_features.len());
    println!("[{BEAD_ID}] wal-tagged: {}", wal_features.len());

    // Collect features with Excluded status (intentional divergences).
    let excluded = universe.features_by_status(ParityStatus::Excluded);
    println!("[{BEAD_ID}] excluded (intentional divergences): {}", excluded.len());

    // Every excluded feature must have a rationale.
    for feat in &excluded {
        assert!(
            feat.exclusion.is_some(),
            "bead_id={BEAD_ID} excluded feature {} missing rationale",
            feat.id
        );
    }

    // Collect partial features (potential MVCC behavioral differences).
    let partial = universe.features_by_status(ParityStatus::Partial);
    println!("[{BEAD_ID}] partial (behavioral differences): {}", partial.len());
    for feat in &partial {
        println!("  PARTIAL: {} — {}", feat.id, feat.title);
    }
}

/// Test 6: Per-category scorecard with known test universe.
#[test]
fn test_per_category_scorecard() {
    let universe = build_test_universe();
    let score = universe.compute_score();

    println!("[{BEAD_ID}] test universe scores:");
    for (name, cs) in &score.category_scores {
        println!("  {name}: score={} pass={} partial={} missing={}",
            cs.score, cs.passing_count, cs.partial_count, cs.missing_count);
    }

    // Storage & Transactions category has one Partial feature → score = 0.5.
    let stor = &score.category_scores["Storage & Transactions"];
    assert_eq!(
        stor.score, 0.5,
        "bead_id={BEAD_ID} Storage score expected 0.5 got {}",
        stor.score
    );

    // Extensions category has one Missing feature → score = 0.0.
    let ext = &score.category_scores["Extensions"];
    assert_eq!(
        ext.score, 0.0,
        "bead_id={BEAD_ID} Extensions score expected 0.0 got {}",
        ext.score
    );

    // SQL Grammar category has one Passing feature → score = 1.0.
    let sql = &score.category_scores["SQL Grammar"];
    assert_eq!(
        sql.score, 1.0,
        "bead_id={BEAD_ID} SQL Grammar score expected 1.0 got {}",
        sql.score
    );
}

/// Test 7: Feature matrix JSON serialization round-trip.
#[test]
fn test_feature_matrix_serialization() {
    let universe = build_test_universe();

    // Serialize to JSON.
    let json = universe.to_json().expect("serialize failed");
    assert!(!json.is_empty(), "bead_id={BEAD_ID} empty JSON");

    // Round-trip deserialize.
    let restored = FeatureUniverse::from_json(&json).expect("deserialize failed");
    assert_eq!(
        universe.features.len(),
        restored.features.len(),
        "bead_id={BEAD_ID} feature count mismatch after round-trip"
    );
    assert_eq!(
        universe.schema_version, restored.schema_version,
        "bead_id={BEAD_ID} schema_version mismatch"
    );
    assert_eq!(
        universe.target_sqlite_version, restored.target_sqlite_version,
        "bead_id={BEAD_ID} target_sqlite_version mismatch"
    );

    // Bayesian scorecard also round-trips.
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);
    let sc_json = scorecard.to_json().expect("scorecard serialize failed");
    let sc_restored = fsqlite_harness::score_engine::BayesianScorecard::from_json(&sc_json)
        .expect("scorecard deserialize failed");
    assert_eq!(
        scorecard.global_point_estimate, sc_restored.global_point_estimate,
        "bead_id={BEAD_ID} scorecard point estimate mismatch after round-trip"
    );

    println!("[{BEAD_ID}] serialization round-trip: universe=OK scorecard=OK");
}

/// Test 8: Category weight invariants hold.
#[test]
fn test_category_weight_invariants() {
    // Category weights must sum to 1.0.
    let weight_sum: f64 = FeatureCategory::ALL.iter().map(|c| c.global_weight()).sum();
    let diff = (weight_sum - 1.0).abs();
    assert!(
        diff < 1e-9,
        "bead_id={BEAD_ID} category weights sum to {weight_sum}, expected 1.0"
    );
    println!("[{BEAD_ID}] category weight sum: {weight_sum} (diff={diff:.1e})");

    // Each category has a positive weight.
    for cat in FeatureCategory::ALL {
        assert!(
            cat.global_weight() > 0.0,
            "bead_id={BEAD_ID} category {cat} has non-positive weight"
        );
    }

    // SqlGrammar and StorageTransaction are the highest-weighted.
    let sql_weight = FeatureCategory::SqlGrammar.global_weight();
    let stor_weight = FeatureCategory::StorageTransaction.global_weight();
    for cat in FeatureCategory::ALL {
        if cat != FeatureCategory::SqlGrammar {
            assert!(
                sql_weight >= cat.global_weight(),
                "bead_id={BEAD_ID} SQL Grammar weight should be highest"
            );
        }
        if cat != FeatureCategory::SqlGrammar && cat != FeatureCategory::StorageTransaction {
            assert!(
                stor_weight >= cat.global_weight(),
                "bead_id={BEAD_ID} Storage weight should be second-highest, but {} >= {}",
                cat.display_name(), stor_weight
            );
        }
    }

    println!("[{BEAD_ID}] weight invariants: PASS");
}

/// Test 9: Prior sensitivity analysis — different priors yield bounded scores.
#[test]
fn test_prior_sensitivity_analysis() {
    let universe = build_canonical_universe();

    let priors = [
        ("uniform", PriorConfig::default()),
        ("jeffreys", PriorConfig::jeffreys()),
        ("haldane", PriorConfig::haldane()),
        ("optimistic", PriorConfig::optimistic()),
    ];

    let mut estimates = Vec::new();
    for (name, prior) in &priors {
        let config = ScoreEngineConfig {
            prior: *prior,
            ..Default::default()
        };
        let scorecard = compute_bayesian_scorecard(&universe, &config);
        println!("[{BEAD_ID}] prior={name}: estimate={:.4} lower={:.4} upper={:.4}",
            scorecard.global_point_estimate,
            scorecard.global_lower_bound,
            scorecard.global_upper_bound);
        estimates.push(scorecard.global_point_estimate);

        // All estimates must be in [0, 1].
        assert!(
            (0.0..=1.0).contains(&scorecard.global_point_estimate),
            "bead_id={BEAD_ID} prior={name} estimate out of bounds"
        );
    }

    // Prior sensitivity: max spread between estimates should be < 0.3
    // (priors shouldn't drastically change the score with enough data).
    let max_spread = estimates.iter().cloned().fold(0.0_f64, f64::max)
        - estimates.iter().cloned().fold(1.0_f64, f64::min);
    println!("[{BEAD_ID}] prior sensitivity spread: {max_spread:.4}");
    assert!(
        max_spread < 0.3,
        "bead_id={BEAD_ID} prior sensitivity too high: {max_spread}"
    );
}

/// Test 10: Excluded features with rationale are excluded from scoring.
#[test]
fn test_excluded_features_handling() {
    let mut universe = build_test_universe();

    // Add an excluded feature with rationale.
    let excl_id = FeatureId::new("SQL", 100);
    universe.features.insert(
        excl_id.clone(),
        Feature {
            id: excl_id,
            title: "Virtual tables (excluded)".to_owned(),
            description: "Intentionally excluded: MVCC replaces virtual table locking model".to_owned(),
            category: FeatureCategory::SqlGrammar,
            weight: 5.0,
            status: ParityStatus::Excluded,
            exclusion: Some(ExclusionRationale {
                reason: "MVCC page-level versioning replaces SQLite file-level locking model".to_owned(),
                reference: "§15.4 MVCC design decision".to_owned(),
            }),
            observability: ObservabilityMapping::default(),
            tags: BTreeSet::from(["mvcc".to_owned(), "divergence".to_owned()]),
        },
    );

    let score_with = universe.compute_score();

    // Excluded feature should not affect scoring denominator.
    let sql_score = &score_with.category_scores["SQL Grammar"];
    assert_eq!(
        sql_score.excluded_count, 1,
        "bead_id={BEAD_ID} expected 1 excluded feature in SQL Grammar"
    );
    // SQL Grammar score should still be 1.0 (only the Passing feature counts).
    assert_eq!(
        sql_score.score, 1.0,
        "bead_id={BEAD_ID} excluded feature should not affect score"
    );

    // Validate: excluded feature without rationale fails validation.
    let bad_id = FeatureId::new("SQL", 101);
    universe.features.insert(
        bad_id.clone(),
        Feature {
            id: bad_id,
            title: "Bad excluded".to_owned(),
            description: "Missing rationale".to_owned(),
            category: FeatureCategory::SqlGrammar,
            weight: 1.0,
            status: ParityStatus::Excluded,
            exclusion: None, // missing!
            observability: ObservabilityMapping::default(),
            tags: BTreeSet::new(),
        },
    );
    let violations = universe.validate();
    assert!(
        violations.iter().any(|v| v.contains("exclusion rationale")),
        "bead_id={BEAD_ID} expected exclusion rationale violation"
    );

    println!("[{BEAD_ID}] excluded feature handling: PASS");
}

/// Test 11: truncate_score produces deterministic 6-decimal-place results.
#[test]
fn test_truncate_score_precision() {
    // Known values.
    assert_eq!(truncate_score(0.123_456_789), 0.123_456);
    assert_eq!(truncate_score(1.0), 1.0);
    assert_eq!(truncate_score(0.0), 0.0);
    assert_eq!(truncate_score(0.999_999_999), 0.999_999);
    assert_eq!(truncate_score(0.5), 0.5);

    // Truncation, not rounding.
    assert_eq!(truncate_score(0.123_456_9), 0.123_456);

    println!("[{BEAD_ID}] truncate_score precision: PASS");
}

/// Test 12: Conformance summary.
#[test]
fn test_conformance_summary() {
    let universe = build_canonical_universe();
    let score = universe.compute_score();
    let config = ScoreEngineConfig::default();
    let scorecard = compute_bayesian_scorecard(&universe, &config);
    let violations = universe.validate();

    let pass_taxonomy = violations.is_empty();
    let pass_score_bounded = (0.0..=1.0).contains(&score.global_score);
    let pass_bayesian_bounded = (0.0..=1.0).contains(&scorecard.global_point_estimate);
    let pass_category_coverage = score.category_scores.len() == 9;
    let pass_serialization = universe.to_json().is_ok() && scorecard.to_json().is_ok();
    let pass_determinism = {
        let s2 = universe.compute_score();
        score.global_score == s2.global_score
    };

    println!("\n=== {BEAD_ID} SQLite Conformance Dashboard Conformance ===");
    println!("  taxonomy_valid..............{}", if pass_taxonomy { "PASS" } else { "FAIL" });
    println!("  score_bounded...............{}", if pass_score_bounded { "PASS" } else { "FAIL" });
    println!("  bayesian_bounded............{}", if pass_bayesian_bounded { "PASS" } else { "FAIL" });
    println!("  category_coverage...........{}", if pass_category_coverage { "PASS" } else { "FAIL" });
    println!("  serialization...............{}", if pass_serialization { "PASS" } else { "FAIL" });
    println!("  determinism.................{}", if pass_determinism { "PASS" } else { "FAIL" });

    let all = [
        pass_taxonomy, pass_score_bounded, pass_bayesian_bounded,
        pass_category_coverage, pass_serialization, pass_determinism,
    ];
    let passed = all.iter().filter(|&&p| p).count();
    println!("  [{}/{}] conformance checks passed", passed, all.len());

    assert!(all.iter().all(|&p| p), "bead_id={BEAD_ID} conformance failed");
}
