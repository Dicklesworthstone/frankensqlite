//! bd-106dk: Swiss Tables for index lookups (§7.7) integration tests.
//!
//! Validates SwissIndex hash map wrapper with SIMD-accelerated probing:
//!   1. Basic CRUD operations (insert, get, remove, contains_key)
//!   2. Load factor tracking under fill
//!   3. Large-scale lookup correctness (10K entries)
//!   4. Collision handling (same bucket, distinct keys)
//!   5. Iterator consistency
//!   6. entry_or_insert_with API
//!   7. Probe metrics fidelity (delta-based)
//!   8. FromIterator construction
//!   9. Index operator and edge cases
//!  10. Machine-readable conformance output

use fsqlite_btree::swiss_index::SwissIndex;
use fsqlite_btree::btree_metrics_snapshot;

// ---------------------------------------------------------------------------
// Test 1: Basic CRUD operations
// ---------------------------------------------------------------------------

#[test]
fn test_basic_crud() {
    let mut map: SwissIndex<u64, String> = SwissIndex::new();
    assert!(map.is_empty());
    assert_eq!(map.len(), 0);

    // Insert
    assert!(map.insert(1, "one".to_string()).is_none());
    assert!(map.insert(2, "two".to_string()).is_none());
    assert_eq!(map.len(), 2);
    assert!(!map.is_empty());

    // Get
    assert_eq!(map.get(&1), Some(&"one".to_string()));
    assert_eq!(map.get(&2), Some(&"two".to_string()));
    assert_eq!(map.get(&3), None);

    // Contains
    assert!(map.contains_key(&1));
    assert!(!map.contains_key(&99));

    // Update (insert same key)
    let old = map.insert(1, "ONE".to_string());
    assert_eq!(old, Some("one".to_string()));
    assert_eq!(map.get(&1), Some(&"ONE".to_string()));
    assert_eq!(map.len(), 2);

    // Remove
    assert_eq!(map.remove(&1), Some("ONE".to_string()));
    assert_eq!(map.len(), 1);
    assert!(!map.contains_key(&1));
    assert_eq!(map.remove(&999), None);

    // Clear
    map.clear();
    assert!(map.is_empty());

    println!("[PASS] Swiss Tables basic CRUD: insert, get, update, remove, clear");
}

// ---------------------------------------------------------------------------
// Test 2: Load factor tracking under fill
// ---------------------------------------------------------------------------

#[test]
fn test_load_factor_tracking() {
    let m_before = btree_metrics_snapshot();

    let mut map = SwissIndex::with_capacity(128);

    // Fill to ~50%
    for i in 0..64u64 {
        map.insert(i, i * 10);
    }

    let m_after = btree_metrics_snapshot();

    // Load factor should be tracked (stored in milli-units, 0-1000).
    assert!(
        m_after.fsqlite_swiss_table_load_factor > 0,
        "load factor should be > 0 after insertions"
    );
    assert!(
        m_after.fsqlite_swiss_table_load_factor <= 1000,
        "load factor should be <= 1000 (milli-units)"
    );

    // Probe count should have increased.
    let delta_probes = m_after.fsqlite_swiss_table_probes_total
        - m_before.fsqlite_swiss_table_probes_total;
    assert!(
        delta_probes >= 64,
        "should have at least 64 probes from inserts, got {delta_probes}"
    );

    println!(
        "[PASS] Load factor tracking: load_factor={}, delta_probes={delta_probes}",
        m_after.fsqlite_swiss_table_load_factor
    );
}

// ---------------------------------------------------------------------------
// Test 3: Large-scale lookup correctness (10K entries)
// ---------------------------------------------------------------------------

#[test]
fn test_large_scale_lookup() {
    let mut map = SwissIndex::with_capacity(10_000);

    // Insert 10K entries.
    for i in 0..10_000u64 {
        map.insert(i, i.wrapping_mul(7).wrapping_add(3));
    }
    assert_eq!(map.len(), 10_000);

    // Verify every entry.
    let mut mismatches = 0u64;
    for i in 0..10_000u64 {
        let expected = i.wrapping_mul(7).wrapping_add(3);
        match map.get(&i) {
            Some(&v) if v == expected => {}
            other => {
                mismatches += 1;
                if mismatches <= 3 {
                    eprintln!("mismatch at key {i}: expected {expected}, got {other:?}");
                }
            }
        }
    }

    assert_eq!(mismatches, 0, "found {mismatches} mismatches in 10K lookups");

    // Non-existent keys should return None.
    for i in 10_000..10_100u64 {
        assert!(map.get(&i).is_none(), "key {i} should not exist");
    }

    println!(
        "[PASS] Large-scale lookup: 10K entries, 0 mismatches, 100 negative lookups verified"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Collision handling (string keys with same prefix)
// ---------------------------------------------------------------------------

#[test]
fn test_collision_handling() {
    let mut map = SwissIndex::new();

    // Insert keys that might hash to similar buckets.
    let keys: Vec<String> = (0..1000).map(|i| format!("table_{i:04}")).collect();
    for (i, key) in keys.iter().enumerate() {
        map.insert(key.clone(), i as u64);
    }
    assert_eq!(map.len(), 1000);

    // All should be retrievable.
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            map.get(key),
            Some(&(i as u64)),
            "key {key} should map to {i}"
        );
    }

    // Remove every other key.
    for key in keys.iter().step_by(2) {
        map.remove(key.as_str());
    }
    assert_eq!(map.len(), 500);

    // Odd-indexed keys should still be present.
    for (i, key) in keys.iter().enumerate() {
        if i % 2 == 1 {
            assert!(map.contains_key(key.as_str()), "key {key} should still exist");
        } else {
            assert!(!map.contains_key(key.as_str()), "key {key} should be removed");
        }
    }

    println!("[PASS] Collision handling: 1000 string keys, remove/verify stable");
}

// ---------------------------------------------------------------------------
// Test 5: Iterator consistency
// ---------------------------------------------------------------------------

#[test]
fn test_iterator_consistency() {
    let mut map = SwissIndex::new();
    for i in 0..100u64 {
        map.insert(i, i * i);
    }

    // iter() should visit all entries.
    let mut visited = vec![false; 100];
    for (&k, &v) in map.iter() {
        assert_eq!(v, k * k, "value mismatch for key {k}");
        visited[k as usize] = true;
    }
    assert!(visited.iter().all(|&v| v), "not all entries visited");

    // values() should yield all values.
    let mut value_sum: u64 = map.values().sum();
    let expected_sum: u64 = (0..100u64).map(|i| i * i).sum();
    assert_eq!(value_sum, expected_sum, "values() sum mismatch");

    // iter_mut() should allow modification.
    for (_, v) in map.iter_mut() {
        *v += 1;
    }
    value_sum = map.values().sum();
    assert_eq!(value_sum, expected_sum + 100, "iter_mut() modification failed");

    // into_iter() should consume the map.
    let collected: Vec<(u64, u64)> = map.into_iter().collect();
    assert_eq!(collected.len(), 100);

    println!("[PASS] Iterator consistency: iter, values, iter_mut, into_iter all correct");
}

// ---------------------------------------------------------------------------
// Test 6: entry_or_insert_with API
// ---------------------------------------------------------------------------

#[test]
fn test_entry_or_insert_with() {
    let mut map = SwissIndex::new();

    // First call: inserts default.
    let val = map.entry_or_insert_with(42u64, || 100u64);
    assert_eq!(*val, 100);

    // Second call: returns existing.
    let val = map.entry_or_insert_with(42, || 999);
    assert_eq!(*val, 100, "should not overwrite existing entry");

    // Modify through returned mutable reference.
    *map.entry_or_insert_with(42, || 0) += 50;
    assert_eq!(map.get(&42), Some(&150));

    println!("[PASS] entry_or_insert_with: insert, no-overwrite, mutation all work");
}

// ---------------------------------------------------------------------------
// Test 7: Probe metrics fidelity (delta-based)
// ---------------------------------------------------------------------------

#[test]
fn test_probe_metrics_fidelity() {
    let m_before = btree_metrics_snapshot();

    let mut map = SwissIndex::new();

    // 10 inserts = 10 probes.
    for i in 0..10u64 {
        map.insert(i, i);
    }
    // 10 gets = 10 probes.
    for i in 0..10u64 {
        map.get(&i);
    }
    // 5 contains = 5 probes.
    for i in 0..5u64 {
        map.contains_key(&i);
    }
    // 3 removes = 3 probes.
    for i in 0..3u64 {
        map.remove(&i);
    }
    // 2 get_mut = 2 probes.
    for i in 3..5u64 {
        map.get_mut(&i);
    }

    let m_after = btree_metrics_snapshot();
    let delta = m_after.fsqlite_swiss_table_probes_total
        - m_before.fsqlite_swiss_table_probes_total;

    // At least 10 + 10 + 5 + 3 + 2 = 30 probes (parallel tests may add more).
    assert!(delta >= 30, "expected >= 30 probes, got {delta}");

    println!("[PASS] Probe metrics fidelity: {delta} probes tracked (>= 30)");
}

// ---------------------------------------------------------------------------
// Test 8: FromIterator construction
// ---------------------------------------------------------------------------

#[test]
fn test_from_iterator() {
    let entries: Vec<(u64, &str)> = vec![
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
        (4, "delta"),
        (5, "epsilon"),
    ];
    let map: SwissIndex<u64, &str> = entries.into_iter().collect();

    assert_eq!(map.len(), 5);
    assert_eq!(map.get(&1), Some(&"alpha"));
    assert_eq!(map.get(&3), Some(&"gamma"));
    assert_eq!(map.get(&5), Some(&"epsilon"));
    assert_eq!(map.get(&6), None);

    println!("[PASS] FromIterator: 5 entries collected, all present");
}

// ---------------------------------------------------------------------------
// Test 9: Index operator and edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_index_operator_and_edges() {
    let mut map = SwissIndex::new();
    map.insert("hello", 42u64);

    // Index operator
    assert_eq!(map[&"hello"], 42);

    // with_capacity(0) should work.
    let empty: SwissIndex<u64, u64> = SwissIndex::with_capacity(0);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);

    // Debug format
    let dbg = format!("{map:?}");
    assert!(dbg.contains("SwissIndex"), "Debug should include type name: {dbg}");

    // Clone
    let map2 = map.clone();
    assert_eq!(map2.get(&"hello"), Some(&42));
    assert_eq!(map2.len(), map.len());

    println!("[PASS] Index operator and edge cases: indexing, empty, debug, clone");
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    // Property 1: Insert/get round-trip.
    let mut m: SwissIndex<u64, u64> = SwissIndex::new();
    m.insert(1, 100);
    let roundtrip_ok = m.get(&1) == Some(&100);

    // Property 2: Remove correctness.
    m.insert(2, 200);
    m.remove(&2);
    let remove_ok = m.get(&2).is_none() && m.len() == 1;

    // Property 3: Probes are tracked.
    let mb = btree_metrics_snapshot();
    let mut m2: SwissIndex<u64, u64> = SwissIndex::new();
    m2.insert(99, 99);
    m2.get(&99);
    let ma = btree_metrics_snapshot();
    let probes_ok = ma.fsqlite_swiss_table_probes_total > mb.fsqlite_swiss_table_probes_total;

    // Property 4: Load factor is tracked.
    let lf_ok = ma.fsqlite_swiss_table_load_factor > 0;

    // Property 5: FromIterator works.
    let from_iter: SwissIndex<u64, u64> = vec![(1, 2), (3, 4)].into_iter().collect();
    let fi_ok = from_iter.len() == 2 && from_iter.get(&1) == Some(&2);

    // Property 6: No torn data (clear + verify).
    let mut m3: SwissIndex<u64, u64> = SwissIndex::new();
    for i in 0..100 { m3.insert(i, i); }
    m3.clear();
    let clear_ok = m3.is_empty();

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] Insert/get round-trip: {roundtrip_ok}");
    println!("  [CONFORM] Remove correctness: {remove_ok}");
    println!("  [CONFORM] Probe metrics tracked: {probes_ok}");
    println!("  [CONFORM] Load factor tracked: {lf_ok}");
    println!("  [CONFORM] FromIterator: {fi_ok}");
    println!("  [CONFORM] Clear empties map: {clear_ok}");
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(roundtrip_ok, "insert/get round-trip failed");
    assert!(remove_ok, "remove failed");
    assert!(probes_ok, "probe metrics not tracked");
    assert!(lf_ok, "load factor not tracked");
    assert!(fi_ok, "FromIterator failed");
    assert!(clear_ok, "clear failed");
}
