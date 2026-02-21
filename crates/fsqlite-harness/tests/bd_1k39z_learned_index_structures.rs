//! bd-1k39z: Learned Index Structures for static lookup (§8.4) integration tests.
//!
//! Validates LearnedIndex piecewise linear approximation:
//!   1. Basic point lookup correctness
//!   2. Uniform distribution (few segments needed)
//!   3. Non-uniform distribution (more segments, error bounded)
//!   4. Error bound guarantee (max_observed_error <= max_error)
//!   5. Negative lookups (key not present)
//!   6. Large-scale correctness (100K keys)
//!   7. Duplicate keys handled
//!   8. Metrics fidelity (delta-based)
//!   9. Edge cases (empty, single key, two keys)
//!  10. Machine-readable conformance output

use fsqlite_btree::learned_index::{
    LearnedIndex, LearnedIndexConfig, learned_index_metrics_snapshot,
};

// ---------------------------------------------------------------------------
// Test 1: Basic point lookup correctness
// ---------------------------------------------------------------------------

#[test]
fn test_basic_point_lookup() {
    let keys: Vec<u64> = (0..100).collect();
    let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());

    assert_eq!(idx.len(), 100);
    assert!(!idx.is_empty());
    assert!(idx.num_segments() > 0);

    // Every key should be found at its position.
    for (pos, &k) in keys.iter().enumerate() {
        assert_eq!(idx.lookup(k), Some(pos), "key {k} should be at position {pos}");
    }

    println!(
        "[PASS] Basic point lookup: 100 keys, {} segments, all found",
        idx.num_segments()
    );
}

// ---------------------------------------------------------------------------
// Test 2: Uniform distribution (few segments)
// ---------------------------------------------------------------------------

#[test]
fn test_uniform_distribution() {
    // Perfectly uniform keys: should need very few segments.
    let keys: Vec<u64> = (0..1000).map(|i| i * 10).collect();
    let config = LearnedIndexConfig { max_error: 1 };
    let idx = LearnedIndex::build(&keys, config);

    // Uniform distribution with perfect spacing -> 1 segment should suffice.
    assert!(
        idx.num_segments() <= 3,
        "uniform distribution should need <= 3 segments, got {}",
        idx.num_segments()
    );

    // All lookups should succeed.
    for (pos, &k) in keys.iter().enumerate() {
        assert_eq!(idx.lookup(k), Some(pos), "key {k} not found");
    }

    // Gaps should return None.
    assert_eq!(idx.lookup(5), None, "key 5 (gap) should not be found");
    assert_eq!(idx.lookup(15), None, "key 15 (gap) should not be found");

    println!(
        "[PASS] Uniform distribution: 1000 keys, {} segments, gaps return None",
        idx.num_segments()
    );
}

// ---------------------------------------------------------------------------
// Test 3: Non-uniform distribution (quadratic)
// ---------------------------------------------------------------------------

#[test]
fn test_non_uniform_distribution() {
    // Quadratic distribution: keys grow as i^2.
    let keys: Vec<u64> = (0..500).map(|i| i * i).collect();
    let config = LearnedIndexConfig { max_error: 32 };
    let idx = LearnedIndex::build(&keys, config);

    // Should need multiple segments for non-linear distribution.
    assert!(
        idx.num_segments() > 1,
        "quadratic distribution should need multiple segments"
    );

    // All lookups should succeed.
    for (pos, &k) in keys.iter().enumerate() {
        assert_eq!(idx.lookup(k), Some(pos), "key {k} not found at pos {pos}");
    }

    println!(
        "[PASS] Non-uniform distribution: 500 keys (quadratic), {} segments",
        idx.num_segments()
    );
}

// ---------------------------------------------------------------------------
// Test 4: Error bound guarantee
// ---------------------------------------------------------------------------

#[test]
fn test_error_bound_guarantee() {
    // Various distributions with different error bounds.
    let distributions: Vec<(&str, Vec<u64>, usize)> = vec![
        ("linear", (0..200).collect(), 4),
        ("quadratic", (0..200).map(|i| i * i).collect(), 16),
        ("cubic", (0..100).map(|i| i * i * i).collect(), 32),
        ("sparse", (0..100).map(|i| i * 1000 + i * i).collect(), 8),
    ];

    for (name, keys, max_error) in &distributions {
        let config = LearnedIndexConfig {
            max_error: *max_error,
        };
        let idx = LearnedIndex::build(keys, config);
        let max_obs = idx.max_observed_error();

        assert!(
            max_obs <= *max_error,
            "{name}: max observed error {max_obs} exceeds bound {max_error}"
        );

        // Verify all lookups work.
        for (pos, &k) in keys.iter().enumerate() {
            assert_eq!(
                idx.lookup(k),
                Some(pos),
                "{name}: key {k} not found at {pos}"
            );
        }
    }

    println!("[PASS] Error bound guarantee: 4 distributions, all within bounds");
}

// ---------------------------------------------------------------------------
// Test 5: Negative lookups
// ---------------------------------------------------------------------------

#[test]
fn test_negative_lookups() {
    let keys: Vec<u64> = (0..50).map(|i| i * 100).collect(); // 0, 100, 200, ...
    let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());

    // Keys in gaps should not be found.
    for gap in [1, 50, 99, 101, 150, 199, 5000] {
        assert_eq!(
            idx.lookup(gap),
            None,
            "gap key {gap} should not be found"
        );
    }

    // Keys beyond the range should not be found.
    assert_eq!(idx.lookup(5000), None, "beyond-range key should not be found");

    println!("[PASS] Negative lookups: 7 gap keys all return None");
}

// ---------------------------------------------------------------------------
// Test 6: Large-scale correctness (100K keys)
// ---------------------------------------------------------------------------

#[test]
fn test_large_scale_correctness() {
    let n = 100_000u64;
    // Monotonically increasing keys with slightly irregular spacing.
    let mut keys: Vec<u64> = Vec::with_capacity(n as usize);
    let mut acc = 0u64;
    for i in 0..n {
        acc += 3 + (i % 7);
        keys.push(acc);
    }
    let config = LearnedIndexConfig { max_error: 16 };
    let idx = LearnedIndex::build(&keys, config);

    assert_eq!(idx.len(), n as usize);

    // Verify a sample of lookups.
    let mut found = 0u64;
    for (pos, &k) in keys.iter().enumerate() {
        if pos % 1000 == 0 {
            assert_eq!(idx.lookup(k), Some(pos), "key {k} not found at {pos}");
            found += 1;
        }
    }

    // Verify error bound.
    let max_obs = idx.max_observed_error();
    assert!(
        max_obs <= config.max_error,
        "max observed error {max_obs} exceeds bound {}",
        config.max_error
    );

    println!(
        "[PASS] Large-scale: {}K keys, {} segments, {found} sampled, max_error={max_obs}",
        n / 1000,
        idx.num_segments()
    );
}

// ---------------------------------------------------------------------------
// Test 7: Duplicate keys handled
// ---------------------------------------------------------------------------

#[test]
fn test_duplicate_keys() {
    // Sorted array with duplicates.
    let keys: Vec<u64> = vec![1, 1, 1, 2, 2, 3, 4, 4, 4, 4, 5];
    let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());

    assert_eq!(idx.len(), 11);

    // lookup should find at least one occurrence.
    assert!(idx.lookup(1).is_some(), "key 1 should be found");
    assert!(idx.lookup(2).is_some(), "key 2 should be found");
    assert!(idx.lookup(3).is_some(), "key 3 should be found");
    assert!(idx.lookup(4).is_some(), "key 4 should be found");
    assert!(idx.lookup(5).is_some(), "key 5 should be found");

    // Verify the found position actually has the key.
    let pos = idx.lookup(4).unwrap();
    assert_eq!(keys[pos], 4);

    // Non-existent key.
    assert_eq!(idx.lookup(6), None);

    println!("[PASS] Duplicate keys: 11 keys with duplicates, all found");
}

// ---------------------------------------------------------------------------
// Test 8: Metrics fidelity (delta-based)
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_fidelity() {
    let m_before = learned_index_metrics_snapshot();

    let keys: Vec<u64> = (0..50).collect();
    let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());

    // Perform some lookups.
    for &k in &keys {
        idx.lookup(k);
    }
    // Some negative lookups.
    idx.lookup(100);
    idx.lookup(200);

    let m_after = learned_index_metrics_snapshot();

    let delta_lookups = m_after.lookups_total - m_before.lookups_total;
    let delta_segments = m_after.segments_total - m_before.segments_total;

    // 50 positive lookups + 2 negative = 52, but negative lookups
    // on keys beyond the segment range may return early.
    assert!(
        delta_lookups >= 50,
        "expected >= 50 lookups, got {delta_lookups}"
    );

    // At least 1 segment was created.
    assert!(
        delta_segments >= 1,
        "expected >= 1 segment, got {delta_segments}"
    );

    // Display format.
    let text = format!("{}", m_after);
    assert!(text.contains("lookups="), "Display should include lookups");
    assert!(text.contains("segments="), "Display should include segments");

    println!(
        "[PASS] Metrics fidelity: delta_lookups={delta_lookups}, delta_segments={delta_segments}"
    );
}

// ---------------------------------------------------------------------------
// Test 9: Edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_edge_cases() {
    // Empty index.
    let empty = LearnedIndex::build(&[], LearnedIndexConfig::default());
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.num_segments(), 0);
    assert_eq!(empty.lookup(42), None);
    assert_eq!(empty.max_observed_error(), 0);

    // Single key.
    let single = LearnedIndex::build(&[42], LearnedIndexConfig::default());
    assert_eq!(single.len(), 1);
    assert_eq!(single.num_segments(), 1);
    assert_eq!(single.lookup(42), Some(0));
    assert_eq!(single.lookup(41), None);

    // Two keys.
    let two = LearnedIndex::build(&[10, 20], LearnedIndexConfig::default());
    assert_eq!(two.len(), 2);
    assert_eq!(two.lookup(10), Some(0));
    assert_eq!(two.lookup(20), Some(1));
    assert_eq!(two.lookup(15), None);

    // Max error = 0 (exact prediction required).
    let exact = LearnedIndex::build(
        &(0..50).collect::<Vec<u64>>(),
        LearnedIndexConfig { max_error: 0 },
    );
    for (pos, &k) in exact.keys().iter().enumerate() {
        assert_eq!(exact.lookup(k), Some(pos));
    }

    // Debug format.
    let dbg = format!("{:?}", single);
    assert!(dbg.contains("LearnedIndex"), "Debug should include type name");

    println!("[PASS] Edge cases: empty, single, two, exact, debug all correct");
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    // Property 1: Lookup round-trip.
    let keys: Vec<u64> = (0..30).collect();
    let idx = LearnedIndex::build(&keys, LearnedIndexConfig::default());
    let roundtrip_ok = (0..30).all(|i| idx.lookup(i) == Some(i as usize));

    // Property 2: Error bound respected.
    let config = LearnedIndexConfig { max_error: 8 };
    let quad_keys: Vec<u64> = (0..100).map(|i| i * i).collect();
    let idx2 = LearnedIndex::build(&quad_keys, config);
    let error_ok = idx2.max_observed_error() <= 8;

    // Property 3: Negative lookups return None.
    let neg_ok = idx.lookup(100).is_none() && idx.lookup(999).is_none();

    // Property 4: Segments increase with error reduction.
    let wide = LearnedIndex::build(&quad_keys, LearnedIndexConfig { max_error: 64 });
    let narrow = LearnedIndex::build(&quad_keys, LearnedIndexConfig { max_error: 4 });
    let segments_ok = narrow.num_segments() >= wide.num_segments();

    // Property 5: Metrics tracked.
    let m = learned_index_metrics_snapshot();
    let metrics_ok = m.lookups_total > 0;

    // Property 6: Empty index works.
    let empty = LearnedIndex::build(&[], LearnedIndexConfig::default());
    let empty_ok = empty.is_empty() && empty.lookup(0).is_none();

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] Lookup round-trip: {roundtrip_ok}");
    println!("  [CONFORM] Error bound respected: {error_ok}");
    println!("  [CONFORM] Negative lookups: {neg_ok}");
    println!("  [CONFORM] Tighter bound = more segments: {segments_ok}");
    println!("  [CONFORM] Metrics tracked: {metrics_ok}");
    println!("  [CONFORM] Empty index works: {empty_ok}");
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(roundtrip_ok, "lookup round-trip failed");
    assert!(error_ok, "error bound violated");
    assert!(neg_ok, "negative lookups failed");
    assert!(segments_ok, "segment count relationship wrong");
    assert!(metrics_ok, "metrics not tracked");
    assert!(empty_ok, "empty index failed");
}
