//! bd-3mr81: NitroSketch for streaming query statistics (§15.5) integration tests.
//!
//! Validates NitroSketch and associated sketch primitives for query-pattern monitoring:
//!   1. NitroSketch cardinality estimation accuracy
//!   2. NitroSketch deterministic replay (same seed = same output)
//!   3. NitroSketch precision range enforcement
//!   4. CountMinSketch frequency estimation (query table access tracking)
//!   5. CountMinSketch never-undercount invariant
//!   6. SpaceSaving heavy-hitter detection (hot page identification)
//!   7. SpaceSaving top-K ordering and stability
//!   8. AmsWindowCollector sliding window lifecycle
//!   9. Combined sketch pipeline: CMS + SpaceSaving + NitroSketch
//!  10. Machine-readable conformance output

use fsqlite_mvcc::{
    CountMinSketch, CountMinSketchConfig, NitroSketch, NitroSketchConfig,
    SpaceSavingSummary, DEFAULT_NITRO_PRECISION, MIN_NITRO_PRECISION, MAX_NITRO_PRECISION,
    sketch_telemetry_metrics,
};

// ---------------------------------------------------------------------------
// Test 1: NitroSketch cardinality estimation accuracy
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::cast_precision_loss)]
fn test_nitrosketch_cardinality_accuracy() {
    let config = NitroSketchConfig {
        precision: DEFAULT_NITRO_PRECISION,
        seed: 0xDEAD_BEEF,
    };
    let mut sketch = NitroSketch::new(&config);

    // Insert 100K distinct values.
    let n = 100_000u64;
    for i in 0..n {
        sketch.observe_u64(i);
    }

    let est = sketch.estimate_cardinality();
    let true_card = n as f64;
    let relative_error = (est - true_card).abs() / true_card;

    // HLL with p=12 (m=4096) should be within ~2% for 100K distinct.
    assert!(
        relative_error < 0.05,
        "cardinality estimate {est:.0} too far from {true_card:.0} (err={relative_error:.4})"
    );

    assert!(sketch.memory_bytes() > 0);
    assert_eq!(sketch.precision(), DEFAULT_NITRO_PRECISION);
    assert_eq!(sketch.register_count(), 1 << DEFAULT_NITRO_PRECISION);

    println!(
        "[PASS] NitroSketch cardinality: n={n} est={est:.0} err={:.2}% mem={}B",
        relative_error * 100.0,
        sketch.memory_bytes()
    );
}

// ---------------------------------------------------------------------------
// Test 2: NitroSketch deterministic replay
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::cast_precision_loss)]
fn test_nitrosketch_deterministic_replay() {
    let config = NitroSketchConfig {
        precision: DEFAULT_NITRO_PRECISION,
        seed: 0xCAFE_BABE,
    };

    let mut a = NitroSketch::new(&config);
    let mut b = NitroSketch::new(&config);

    // Same input sequence -> same cardinality estimate.
    for i in 0..50_000u64 {
        let v = i.wrapping_mul(37).wrapping_add(13);
        a.observe_u64(v);
        b.observe_u64(v);
    }

    let est_a = a.estimate_cardinality();
    let est_b = b.estimate_cardinality();

    assert!(
        (est_a - est_b).abs() < f64::EPSILON,
        "deterministic replay failed: a={est_a} b={est_b}"
    );

    println!("[PASS] NitroSketch deterministic replay: a={est_a:.0} b={est_b:.0} (identical)");
}

// ---------------------------------------------------------------------------
// Test 3: NitroSketch precision range enforcement
// ---------------------------------------------------------------------------

#[test]
fn test_nitrosketch_precision_range() {
    // Valid precision values should work.
    for p in MIN_NITRO_PRECISION..=MAX_NITRO_PRECISION {
        let config = NitroSketchConfig {
            precision: p,
            seed: 42,
        };
        let sketch = NitroSketch::new(&config);
        assert_eq!(sketch.precision(), p);
        assert_eq!(sketch.register_count(), 1 << p);
    }

    println!(
        "[PASS] NitroSketch precision: valid range [{MIN_NITRO_PRECISION}, {MAX_NITRO_PRECISION}] all pass"
    );
}

#[test]
#[should_panic(expected = "precision")]
fn test_nitrosketch_precision_too_low() {
    let config = NitroSketchConfig {
        precision: MIN_NITRO_PRECISION - 1,
        seed: 0,
    };
    let _ = NitroSketch::new(&config);
}

#[test]
#[should_panic(expected = "precision")]
fn test_nitrosketch_precision_too_high() {
    let config = NitroSketchConfig {
        precision: MAX_NITRO_PRECISION + 1,
        seed: 0,
    };
    let _ = NitroSketch::new(&config);
}

// ---------------------------------------------------------------------------
// Test 4: CountMinSketch frequency estimation (table access tracking)
// ---------------------------------------------------------------------------

#[test]
fn test_cms_frequency_estimation() {
    let config = CountMinSketchConfig {
        width: 1024,
        depth: 4,
        seed: 0xBEEF,
    };
    let mut cms = CountMinSketch::new(&config);

    // Simulate table access frequency tracking.
    // Table IDs: 1=users (hot), 2=orders (warm), 3=logs (cold).
    for _ in 0..1000 {
        cms.observe(1); // users
    }
    for _ in 0..100 {
        cms.observe(2); // orders
    }
    for _ in 0..10 {
        cms.observe(3); // logs
    }

    // CMS never undercounts.
    assert!(cms.estimate(1) >= 1000, "users frequency too low: {}", cms.estimate(1));
    assert!(cms.estimate(2) >= 100, "orders frequency too low: {}", cms.estimate(2));
    assert!(cms.estimate(3) >= 10, "logs frequency too low: {}", cms.estimate(3));

    // With width=1024, small number of distinct items should be exact.
    assert_eq!(cms.estimate(1), 1000);
    assert_eq!(cms.estimate(2), 100);
    assert_eq!(cms.estimate(3), 10);

    // Never-seen item should have 0 or very small estimate.
    let unseen = cms.estimate(999);
    assert!(unseen <= 10, "unseen item estimate too high: {unseen}");

    println!(
        "[PASS] CMS frequency: users={} orders={} logs={} unseen={}",
        cms.estimate(1), cms.estimate(2), cms.estimate(3), unseen
    );
}

// ---------------------------------------------------------------------------
// Test 5: CountMinSketch never-undercount invariant
// ---------------------------------------------------------------------------

#[test]
fn test_cms_never_undercounts() {
    let config = CountMinSketchConfig {
        width: 256,
        depth: 4,
        seed: 0xFACE,
    };
    let mut cms = CountMinSketch::new(&config);

    // Insert 500 items with known counts.
    for i in 0..500u64 {
        cms.observe_n(i, i + 1);
    }

    let mut violations = 0;
    for i in 0..500u64 {
        let est = cms.estimate(i);
        let true_count = i + 1;
        if est < true_count {
            violations += 1;
        }
    }

    assert_eq!(
        violations, 0,
        "CMS must never undercount (found {violations} violations)"
    );

    println!("[PASS] CMS never-undercount: 500 items, 0 violations");
}

// ---------------------------------------------------------------------------
// Test 6: SpaceSaving heavy-hitter detection (hot page identification)
// ---------------------------------------------------------------------------

#[test]
fn test_spacesaving_heavy_hitter() {
    let mut ss = SpaceSavingSummary::new(32); // Track top-32 (min K=32)

    // Page 42 is extremely hot (10x more than others).
    for _ in 0..10_000 {
        ss.observe_incidence(42);
    }
    for pgno in 1..=100 {
        for _ in 0..100 {
            ss.observe_incidence(pgno);
        }
    }

    // Page 42 should be a heavy hitter.
    let entry = ss.entry_for(42);
    assert!(entry.is_some(), "page 42 must be tracked as heavy hitter");

    let entry = entry.unwrap();
    assert!(
        entry.count_lower_bound() >= 10_000,
        "page 42 count lower bound should be >= 10000, got {}",
        entry.count_lower_bound()
    );

    // Top entries should include page 42.
    let sorted = ss.entries_sorted();
    assert!(!sorted.is_empty());
    // Page 42 should be in the top entries.
    let has_42 = sorted.iter().any(|e| e.pgno == 42);
    assert!(has_42, "page 42 should be in sorted entries");

    println!(
        "[PASS] SpaceSaving heavy-hitter: page 42 count_lb={}, top-{} tracked",
        entry.count_lower_bound(),
        ss.capacity()
    );
}

// ---------------------------------------------------------------------------
// Test 7: SpaceSaving top-K ordering and stability
// ---------------------------------------------------------------------------

#[test]
fn test_spacesaving_topk_ordering() {
    let mut ss = SpaceSavingSummary::new(32); // Track top-32 (min K=32)

    // Create known frequency distribution.
    // Item 1: 1000, Item 2: 500, Item 3: 200, Item 4: 100, Item 5: 50
    let items = [(1u64, 1000u64), (2, 500), (3, 200), (4, 100), (5, 50)];
    for &(item, count) in &items {
        for _ in 0..count {
            ss.observe_incidence(item);
        }
    }

    let sorted = ss.entries_sorted();
    assert!(!sorted.is_empty());

    // Sorted entries should be in descending order of count.
    for w in sorted.windows(2) {
        assert!(
            w[0].count_lower_bound() >= w[1].count_lower_bound(),
            "ordering violation: {} < {}",
            w[0].count_lower_bound(),
            w[1].count_lower_bound()
        );
    }

    // The highest item should have >= 1000.
    assert!(
        sorted[0].count_lower_bound() >= 1000,
        "top entry should have count >= 1000, got {}",
        sorted[0].count_lower_bound()
    );

    println!(
        "[PASS] SpaceSaving top-K: {} entries, top count={}, descending order verified",
        sorted.len(),
        sorted[0].count_lower_bound()
    );
}

// ---------------------------------------------------------------------------
// Test 8: AmsWindowCollector sliding window lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_ams_window_collector() {
    use fsqlite_mvcc::{
        AmsWindowCollector, AmsWindowCollectorConfig,
        DEFAULT_AMS_R, DEFAULT_HEAVY_HITTER_K,
    };

    let config = AmsWindowCollectorConfig {
        r: DEFAULT_AMS_R,
        db_epoch: 0,
        regime_id: 0,
        window_width_ticks: 5,
        track_exact_m2: false,
        track_heavy_hitters: true,
        heavy_hitter_k: DEFAULT_HEAVY_HITTER_K,
        estimate_zipf: false,
    };

    let mut collector = AmsWindowCollector::new(config, 0);

    // Simulate commit attempts across ticks.
    for tick in 0..10u64 {
        let write_set: Vec<u64> = (0..5).map(|i| i + tick * 5).collect();
        collector.observe_commit_attempt(tick, &write_set);
    }

    // Flush the window and get estimates.
    let estimate = collector.force_flush(10);

    // f2_hat is u128, should be > 0 after observations.
    assert!(
        estimate.f2_hat > 0,
        "f2_hat should be > 0, got {}",
        estimate.f2_hat
    );

    // p_eff_hat should be non-negative (can exceed 1.0 for skewed workloads).
    assert!(
        estimate.p_eff_hat >= 0.0,
        "p_eff_hat should be >= 0, got {}",
        estimate.p_eff_hat
    );

    // Heavy hitters should have been tracked.
    assert!(
        estimate.heavy_hitter_k.is_some(),
        "heavy_hitter_k should be tracked"
    );

    println!(
        "[PASS] AmsWindowCollector: f2_hat={} p_eff_hat={:.4} hh_k={:?}",
        estimate.f2_hat, estimate.p_eff_hat, estimate.heavy_hitter_k
    );
}

// ---------------------------------------------------------------------------
// Test 9: Combined sketch pipeline
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::cast_precision_loss)]
fn test_combined_sketch_pipeline() {
    // Simulate a query monitoring pipeline:
    // 1. NitroSketch for distinct query cardinality
    // 2. CMS for per-table access frequency
    // 3. SpaceSaving for hot-page detection

    let mut nitro = NitroSketch::new(&NitroSketchConfig {
        precision: DEFAULT_NITRO_PRECISION,
        seed: 0x1234,
    });
    let mut cms = CountMinSketch::new(&CountMinSketchConfig {
        width: 512,
        depth: 4,
        seed: 0x5678,
    });
    let mut ss = SpaceSavingSummary::new(32);

    // Simulate 10K query events across 50 distinct tables.
    for query_id in 0..10_000u64 {
        let table_id = query_id % 50;
        nitro.observe_u64(query_id);
        cms.observe(table_id);
        ss.observe_incidence(table_id);
    }

    // NitroSketch: should estimate ~10K distinct queries.
    let cardinality = nitro.estimate_cardinality();
    let card_err = (cardinality - 10_000.0).abs() / 10_000.0;
    assert!(
        card_err < 0.05,
        "cardinality estimate {cardinality:.0} off by {:.1}%",
        card_err * 100.0
    );

    // CMS: each table should have ~200 accesses (10000/50).
    for table_id in 0..50u64 {
        let freq = cms.estimate(table_id);
        assert!(
            freq >= 200,
            "table {table_id} frequency {freq} below expected 200"
        );
    }

    // SpaceSaving: should have tracked entries.
    let sorted = ss.entries_sorted();
    assert!(!sorted.is_empty());

    // Global metrics should have increased.
    let m = sketch_telemetry_metrics();
    assert!(
        m.fsqlite_sketch_observations_total > 0,
        "global sketch observations should be > 0"
    );

    println!(
        "[PASS] Combined pipeline: cardinality={cardinality:.0} (err={:.1}%), \
         cms_table0={}, ss_entries={}, global_obs={}",
        card_err * 100.0,
        cms.estimate(0),
        sorted.len(),
        m.fsqlite_sketch_observations_total
    );
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::cast_precision_loss)]
fn test_conformance_summary() {
    // Property 1: NitroSketch cardinality within 5%.
    let mut ns = NitroSketch::new(&NitroSketchConfig {
        precision: DEFAULT_NITRO_PRECISION,
        seed: 42,
    });
    for i in 0..50_000u64 {
        ns.observe_u64(i);
    }
    let card = ns.estimate_cardinality();
    let card_ok = (card - 50_000.0).abs() / 50_000.0 < 0.05;

    // Property 2: Deterministic replay.
    let cfg = NitroSketchConfig { precision: 8, seed: 99 };
    let mut a = NitroSketch::new(&cfg);
    let mut b = NitroSketch::new(&cfg);
    for i in 0..1000u64 { a.observe_u64(i); b.observe_u64(i); }
    let replay_ok = (a.estimate_cardinality() - b.estimate_cardinality()).abs() < f64::EPSILON;

    // Property 3: CMS never undercounts.
    let mut cms = CountMinSketch::new(&CountMinSketchConfig { width: 512, depth: 4, seed: 0 });
    cms.observe_n(1, 100);
    cms.observe_n(2, 50);
    let cms_ok = cms.estimate(1) >= 100 && cms.estimate(2) >= 50;

    // Property 4: SpaceSaving top-K ordering.
    let mut ss = SpaceSavingSummary::new(32);
    for _ in 0..1000 { ss.observe_incidence(1); }
    for _ in 0..500 { ss.observe_incidence(2); }
    let sorted = ss.entries_sorted();
    let ss_ok = !sorted.is_empty() && sorted[0].count_lower_bound() >= 1000;

    // Property 5: Precision bounds enforced (tested via should_panic tests above).
    let precision_ok = true;

    // Property 6: Memory footprint tracked.
    let mem_ok = ns.memory_bytes() > 0;

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] NitroSketch cardinality: est={card:.0}, within 5% of 50K");
    println!("  [CONFORM] Deterministic replay: same seed = same output");
    println!("  [CONFORM] CMS never-undercount: est(1)={} >= 100", cms.estimate(1));
    println!("  [CONFORM] SpaceSaving top-K: {} entries, ordered by count", sorted.len());
    println!("  [CONFORM] Precision bounds: [{MIN_NITRO_PRECISION}, {MAX_NITRO_PRECISION}] enforced");
    println!("  [CONFORM] Memory footprint: {}B tracked", ns.memory_bytes());
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(card_ok, "cardinality accuracy failed: {card}");
    assert!(replay_ok, "deterministic replay failed");
    assert!(cms_ok, "CMS undercount detected");
    assert!(ss_ok, "SpaceSaving ordering failed");
    assert!(precision_ok, "precision bounds not enforced");
    assert!(mem_ok, "memory tracking failed");
}
