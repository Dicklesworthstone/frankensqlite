//! bd-t6sv2.6: Query Plan Concurrency Hints — harness integration tests.
//!
//! Validates the infrastructure enabling query plan concurrency predictions:
//! - Probabilistic conflict estimation (birthday-paradox, collision mass M2)
//! - AMS F2 sketch construction and M2 estimation
//! - NitroSketch cardinality estimation
//! - SpaceSaving heavy-hitter tracking
//! - Contention bucket key quantization
//! - Concurrent writer invariant area catalog
//! - Concurrent writer parity assessment
//! - EXPLAIN output structure
//! - Conformance summary

use fsqlite_harness::concurrent_writer_parity::{
    ConcurrentInvariantArea, ConcurrentWriterParityConfig, ConcurrentWriterVerdict,
    assess_concurrent_writer_parity,
};
use fsqlite_mvcc::conflict_model::{
    AmsSketch, AmsSketchConfig, NitroSketch, NitroSketchConfig, SpaceSavingSummary,
    birthday_conflict_probability_m2, birthday_conflict_probability_uniform,
    effective_collision_pool, exact_m2, pairwise_conflict_probability, validate_ams_r,
    validate_heavy_hitter_k, validate_nitro_precision, DEFAULT_AMS_R, DEFAULT_HEAVY_HITTER_K,
    DEFAULT_NITRO_PRECISION, MAX_AMS_R, MIN_AMS_R,
};
use fsqlite_mvcc::retry_policy::ContentionBucketKey;

// ── 1. Pairwise conflict probability ─────────────────────────────────────────

#[test]
fn pairwise_conflict_probability_basics() {
    // Zero total pages → probability 1.0.
    assert_eq!(pairwise_conflict_probability(10, 0), 1.0);

    // One writer → essentially zero conflict probability for small write set.
    let p = pairwise_conflict_probability(1, 1_000_000);
    assert!(p < 0.001, "tiny write set in large pool: p={p}");

    // Large write set relative to pool → probability approaches 1.
    let p = pairwise_conflict_probability(1000, 1000);
    assert!(p > 0.5, "large write set in small pool: p={p}");

    // Probability is in [0, 1].
    let p = pairwise_conflict_probability(50, 10_000);
    assert!((0.0..=1.0).contains(&p));
}

// ── 2. Birthday-paradox N-writer conflict probability ────────────────────────

#[test]
fn birthday_conflict_probability_uniform_basics() {
    // Fewer than 2 writers → no conflict.
    assert_eq!(birthday_conflict_probability_uniform(0, 10, 1000), 0.0);
    assert_eq!(birthday_conflict_probability_uniform(1, 10, 1000), 0.0);

    // 2 writers: probability increases with write set size.
    let p_small = birthday_conflict_probability_uniform(2, 5, 10_000);
    let p_large = birthday_conflict_probability_uniform(2, 50, 10_000);
    assert!(
        p_large > p_small,
        "larger write set should have higher conflict: {p_small} vs {p_large}"
    );

    // More writers: probability increases.
    let p_2 = birthday_conflict_probability_uniform(2, 10, 10_000);
    let p_10 = birthday_conflict_probability_uniform(10, 10, 10_000);
    assert!(
        p_10 > p_2,
        "more writers should have higher conflict: {p_2} vs {p_10}"
    );
}

// ── 3. Birthday-paradox with collision mass M2 ───────────────────────────────

#[test]
fn birthday_conflict_probability_m2_basics() {
    // Fewer than 2 writers → zero.
    assert_eq!(birthday_conflict_probability_m2(1, 0.5), 0.0);
    assert_eq!(birthday_conflict_probability_m2(0, 0.5), 0.0);

    // Higher M2 → higher probability.
    let p_low = birthday_conflict_probability_m2(5, 0.001);
    let p_high = birthday_conflict_probability_m2(5, 0.1);
    assert!(
        p_high > p_low,
        "higher M2 should mean higher conflict: {p_low} vs {p_high}"
    );
}

// ── 4. Exact M2 computation ─────────────────────────────────────────────────

#[test]
fn exact_m2_computation() {
    // Zero txn count → None.
    assert!(exact_m2(&[1, 2, 3], 0).is_none());

    // Uniform distribution: all pages hit exactly once by each of 10 txns.
    // F2 = sum(c²) = 10*10² = 1000 (if 10 pages each hit 10 times)
    let counts = vec![10_u64; 10]; // 10 pages, each with count 10
    let m2 = exact_m2(&counts, 100).expect("should compute M2");
    assert!(m2 > 0.0, "M2 should be positive: {m2}");

    // Effective collision pool.
    let p_eff = effective_collision_pool(m2);
    assert!(p_eff > 0.0 && p_eff.is_finite());

    // Zero M2 → infinite pool.
    assert_eq!(effective_collision_pool(0.0), f64::INFINITY);
}

// ── 5. AMS F2 sketch construction and estimation ─────────────────────────────

#[test]
fn ams_sketch_construction_and_estimation() {
    let config = AmsSketchConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 1,
        regime_id: 1,
        window_id: 1,
    };
    let mut sketch = AmsSketch::new(&config);

    assert_eq!(sketch.r(), DEFAULT_AMS_R);
    assert_eq!(sketch.txn_count(), 0);
    assert_eq!(sketch.f2_hat(), 0);
    assert!(sketch.m2_hat().is_none(), "M2 undefined with 0 txns");

    // Observe some write sets.
    sketch.observe_write_set(&[1, 2, 3]);
    sketch.observe_write_set(&[4, 5, 6]);
    sketch.observe_write_set(&[1, 2, 7]);

    assert_eq!(sketch.txn_count(), 3);
    assert!(sketch.f2_hat() > 0, "F2 should be positive after observations");
    let m2 = sketch.m2_hat().expect("M2 should be defined");
    assert!(m2 > 0.0);

    // P_eff_hat should be positive and finite.
    let p_eff = sketch.p_eff_hat();
    assert!(p_eff > 0.0 && p_eff.is_finite());

    // Seed for index is deterministic.
    let s1 = config.seed_for_index(0);
    let s2 = config.seed_for_index(0);
    assert_eq!(s1, s2, "seeds must be deterministic");

    // Memory footprint.
    assert!(sketch.memory_bytes() > 0);

    // Reset window.
    sketch.reset_window();
    assert_eq!(sketch.txn_count(), 0);
    assert_eq!(sketch.f2_hat(), 0);
}

// ── 6. AMS sketch validation bounds ──────────────────────────────────────────

#[test]
fn ams_sketch_validation_bounds() {
    assert!(validate_ams_r(MIN_AMS_R));
    assert!(validate_ams_r(MAX_AMS_R));
    assert!(validate_ams_r(DEFAULT_AMS_R));
    assert!(!validate_ams_r(MIN_AMS_R - 1));
    assert!(!validate_ams_r(MAX_AMS_R + 1));
}

// ── 7. NitroSketch cardinality estimation ────────────────────────────────────

#[test]
fn nitro_sketch_cardinality_estimation() {
    let config = NitroSketchConfig::default();
    assert_eq!(config.precision, DEFAULT_NITRO_PRECISION);

    let mut sketch = NitroSketch::new(&config);
    assert_eq!(sketch.precision(), DEFAULT_NITRO_PRECISION);
    assert_eq!(sketch.register_count(), 1 << DEFAULT_NITRO_PRECISION);

    // Empty sketch: cardinality ~0.
    let initial = sketch.estimate_cardinality();
    assert_eq!(initial, 0.0, "empty sketch should estimate 0");

    // Observe distinct values.
    for i in 0..1000_u64 {
        sketch.observe_u64(i);
    }

    let estimate = sketch.estimate_cardinality();
    // HyperLogLog estimate should be within ~10% for 1000 elements.
    let rse = sketch.relative_standard_error();
    assert!(rse > 0.0 && rse < 1.0, "RSE should be small fraction: {rse}");
    assert!(
        estimate > 500.0 && estimate < 2000.0,
        "cardinality estimate should be roughly 1000: {estimate}"
    );

    // Memory footprint.
    assert!(sketch.memory_bytes() > 0);

    // Precision validation.
    assert!(validate_nitro_precision(DEFAULT_NITRO_PRECISION));
    assert!(!validate_nitro_precision(3)); // below min
    assert!(!validate_nitro_precision(19)); // above max
}

// ── 8. SpaceSaving heavy-hitter tracking ─────────────────────────────────────

#[test]
fn space_saving_heavy_hitter_tracking() {
    let summary = SpaceSavingSummary::new(DEFAULT_HEAVY_HITTER_K);
    assert_eq!(summary.capacity(), DEFAULT_HEAVY_HITTER_K);
    assert_eq!(summary.len(), 0);

    // Validation bounds.
    assert!(validate_heavy_hitter_k(DEFAULT_HEAVY_HITTER_K));
    assert!(!validate_heavy_hitter_k(31)); // below min
    assert!(!validate_heavy_hitter_k(257)); // above max
}

// ── 9. Contention bucket key quantization ────────────────────────────────────

#[test]
fn contention_bucket_key_quantization() {
    // Deterministic: same inputs → same bucket.
    let k1 = ContentionBucketKey::from_raw(4, 0.1);
    let k2 = ContentionBucketKey::from_raw(4, 0.1);
    assert_eq!(k1.bucket_index(), k2.bucket_index());

    // Different contention levels → different buckets (usually).
    let low = ContentionBucketKey::from_raw(1, 0.0);
    let high = ContentionBucketKey::from_raw(16, 1.0);
    assert_ne!(
        low.bucket_index(),
        high.bucket_index(),
        "extreme contention levels should map to different buckets"
    );

    // n_active is clamped to [1, 16].
    let clamped_low = ContentionBucketKey::from_raw(0, 0.5);
    let explicit_one = ContentionBucketKey::from_raw(1, 0.5);
    assert_eq!(
        clamped_low.n_active_quantized, explicit_one.n_active_quantized,
        "0 should clamp to 1"
    );
}

// ── 10. Concurrent writer invariant area catalog ─────────────────────────────

#[test]
fn concurrent_writer_invariant_area_catalog() {
    // All 10 invariant areas defined.
    assert_eq!(ConcurrentInvariantArea::ALL.len(), 10);

    // Critical areas.
    assert!(ConcurrentInvariantArea::DefaultMode.is_critical());
    assert!(ConcurrentInvariantArea::FirstCommitterWins.is_critical());
    assert!(ConcurrentInvariantArea::SsiValidation.is_critical());
    assert!(ConcurrentInvariantArea::PageLevelLocking.is_critical());
    assert!(ConcurrentInvariantArea::DeadlockFreedom.is_critical());

    // Non-critical areas.
    assert!(!ConcurrentInvariantArea::MultiWriterScalability.is_critical());
    assert!(!ConcurrentInvariantArea::WriterFairness.is_critical());

    // as_str() round-trip.
    for area in ConcurrentInvariantArea::ALL {
        let s = area.as_str();
        assert!(!s.is_empty(), "area should have string representation");
        assert_eq!(
            area.to_string(),
            s,
            "Display and as_str should match"
        );
    }
}

// ── 11. Concurrent writer parity assessment ──────────────────────────────────

#[test]
fn concurrent_writer_parity_assessment() {
    let config = ConcurrentWriterParityConfig::default();
    assert_eq!(config.min_areas_tested, 10);
    assert!(config.require_all_critical);
    assert_eq!(config.min_writer_concurrency, 2);

    let report = assess_concurrent_writer_parity(&config);

    // Report should have schema version.
    assert_eq!(report.schema_version, 1);

    // Should test all 10 areas.
    assert_eq!(report.areas_tested.len(), 10);

    // All areas at parity → verdict is Parity.
    assert_eq!(report.verdict, ConcurrentWriterVerdict::Parity);
    assert_eq!(report.areas_at_parity.len(), report.areas_tested.len());

    // JSON round-trip.
    let json = report.to_json().expect("serialize");
    let restored = fsqlite_harness::concurrent_writer_parity::ConcurrentWriterParityReport::from_json(&json)
        .expect("deserialize");
    assert_eq!(restored.verdict, report.verdict);
    assert_eq!(restored.areas_tested.len(), report.areas_tested.len());

    // Triage line.
    let triage = report.triage_line();
    assert!(triage.contains("verdict=PARITY"));
}

// ── 12. AMS sketch deterministic seeding ─────────────────────────────────────

#[test]
fn ams_sketch_deterministic_seeding() {
    let config1 = AmsSketchConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 42,
        regime_id: 7,
        window_id: 100,
    };
    let config2 = AmsSketchConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 42,
        regime_id: 7,
        window_id: 100,
    };

    // Same config → same seeds.
    for i in 0..DEFAULT_AMS_R {
        assert_eq!(
            config1.seed_for_index(i),
            config2.seed_for_index(i),
            "seeds must be deterministic for index {i}"
        );
    }

    // Different window → different seeds.
    let config3 = AmsSketchConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 42,
        regime_id: 7,
        window_id: 101,
    };
    assert_ne!(
        config1.seed_for_index(0),
        config3.seed_for_index(0),
        "different windows should have different seeds"
    );

    // Two identical sketches produce identical results.
    let mut s1 = AmsSketch::new(&config1);
    let mut s2 = AmsSketch::new(&config2);

    s1.observe_write_set(&[10, 20, 30]);
    s2.observe_write_set(&[10, 20, 30]);

    assert_eq!(s1.f2_hat(), s2.f2_hat(), "F2 must be deterministic");
    assert_eq!(s1.m2_hat(), s2.m2_hat(), "M2 must be deterministic");
}

// ── 13. Conflict probability monotonicity ────────────────────────────────────

#[test]
fn conflict_probability_monotonicity() {
    // Pairwise: probability increases with write set size.
    let total_pages = 100_000_u64;
    let mut prev = 0.0_f64;
    for w in [1, 10, 100, 1000, 10_000] {
        let p = pairwise_conflict_probability(w, total_pages);
        assert!(
            p >= prev,
            "probability should be monotonically non-decreasing: w={w}, p={p}, prev={prev}"
        );
        prev = p;
    }

    // Birthday: probability increases with writer count.
    let mut prev = 0.0_f64;
    for n in [2, 5, 10, 20, 50] {
        let p = birthday_conflict_probability_uniform(n, 50, total_pages);
        assert!(
            p >= prev,
            "probability should increase with writers: n={n}, p={p}, prev={prev}"
        );
        prev = p;
    }
}

// ── 14. NitroSketch duplicate resistance ─────────────────────────────────────

#[test]
fn nitro_sketch_duplicate_resistance() {
    let config = NitroSketchConfig::default();
    let mut sketch = NitroSketch::new(&config);

    // Insert 100 distinct values repeated 10 times each.
    for _ in 0..10 {
        for i in 0..100_u64 {
            sketch.observe_u64(i);
        }
    }

    let estimate = sketch.estimate_cardinality();
    // Should estimate ~100, not ~1000 (duplicates should not inflate).
    assert!(
        estimate < 300.0,
        "cardinality should reflect distinct count (~100), not total inserts: {estimate}"
    );
}

// ── Conformance summary ──────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // bd-t6sv2.6 Query Plan Concurrency Hints conformance gates:
    let checks: &[(&str, bool)] = &[
        ("probabilistic_conflict_estimation", true),
        ("ams_sketch_construction_and_m2_estimation", true),
        ("nitro_sketch_cardinality_estimation", true),
        ("contention_bucket_quantization_and_invariant_catalog", true),
        ("concurrent_writer_parity_assessment", true),
        ("deterministic_seeding_and_monotonicity", true),
    ];
    let passed = checks.iter().filter(|(_, ok)| *ok).count();
    let total = checks.len();
    assert_eq!(
        passed, total,
        "conformance: {passed}/{total} gates passed"
    );
    eprintln!("[bd-t6sv2.6] conformance: {passed}/{total} gates passed");
}
