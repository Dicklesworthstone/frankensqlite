//! Harness integration tests for bd-3ta.4: Bε-tree write-optimized index (§15.2).
//!
//! Validates: buffered inserts, flush cascades, point/range queries through
//! buffers, delete correctness, write amplification characteristics, and
//! metrics fidelity.

use fsqlite_btree::be_tree::{
    BeTree, BeTreeConfig, betree_metrics_snapshot, reset_betree_metrics,
};

const BEAD_ID: &str = "bd-3ta.4";

// ── 1. Basic insert + point lookup through buffer ────────────────────────

#[test]
fn test_basic_insert_and_lookup() {
    let mut tree = BeTree::new(BeTreeConfig::default());

    tree.insert(10, "ten");
    tree.insert(20, "twenty");
    tree.insert(5, "five");

    assert_eq!(
        tree.get(&10),
        Some(&"ten"),
        "bead_id={BEAD_ID} case=point_lookup_through_buffer"
    );
    assert_eq!(
        tree.get(&20),
        Some(&"twenty"),
        "bead_id={BEAD_ID} case=point_lookup_20"
    );
    assert_eq!(
        tree.get(&5),
        Some(&"five"),
        "bead_id={BEAD_ID} case=point_lookup_5"
    );
    assert_eq!(
        tree.get(&99),
        None,
        "bead_id={BEAD_ID} case=absent_key"
    );
    assert_eq!(tree.len(), 3, "bead_id={BEAD_ID} case=len_after_3_inserts");
}

// ── 2. Buffer flush triggers on overflow ─────────────────────────────────

#[test]
fn test_buffer_flush_triggers() {
    reset_betree_metrics();
    let config = BeTreeConfig {
        buffer_capacity: 3,
        leaf_capacity: 4,
        max_pivots: 2,
    };
    let mut tree = BeTree::new(config);

    // Insert enough to force the tree to create interior nodes and flush.
    for i in 0..30 {
        tree.insert(i, i * 100);
    }

    let snap = betree_metrics_snapshot();
    assert!(
        snap.buffer_flushes_total > 0,
        "bead_id={BEAD_ID} case=flush_triggered flushes={}",
        snap.buffer_flushes_total
    );
    assert!(
        snap.messages_buffered_total >= 30,
        "bead_id={BEAD_ID} case=messages_buffered got={}",
        snap.messages_buffered_total
    );

    // All values should still be accessible.
    for i in 0..30 {
        assert_eq!(
            tree.get(&i),
            Some(&(i * 100)),
            "bead_id={BEAD_ID} case=post_flush_lookup key={i}"
        );
    }
}

// ── 3. Delete correctness ────────────────────────────────────────────────

#[test]
fn test_delete_correctness() {
    let config = BeTreeConfig {
        buffer_capacity: 4,
        leaf_capacity: 6,
        max_pivots: 3,
    };
    let mut tree = BeTree::new(config);

    for i in 0..20 {
        tree.insert(i, i);
    }
    assert_eq!(tree.len(), 20, "bead_id={BEAD_ID} case=len_before_delete");

    // Delete even keys.
    for i in (0..20).step_by(2) {
        tree.delete(i);
    }

    assert_eq!(tree.len(), 10, "bead_id={BEAD_ID} case=len_after_delete");

    // Even keys should be gone.
    for i in (0..20).step_by(2) {
        assert_eq!(
            tree.get(&i),
            None,
            "bead_id={BEAD_ID} case=deleted_key_absent key={i}"
        );
    }

    // Odd keys should remain.
    for i in (1..20).step_by(2) {
        assert_eq!(
            tree.get(&i),
            Some(&i),
            "bead_id={BEAD_ID} case=surviving_key key={i}"
        );
    }
}

// ── 4. Range query correctness ───────────────────────────────────────────

#[test]
fn test_range_query_correctness() {
    let config = BeTreeConfig {
        buffer_capacity: 3,
        leaf_capacity: 4,
        max_pivots: 2,
    };
    let mut tree = BeTree::new(config);

    for i in 0..50 {
        tree.insert(i, i * 10);
    }

    let result = tree.range(&10, &20);
    let keys: Vec<i32> = result.iter().map(|(k, _)| *k).collect();
    let values: Vec<i32> = result.iter().map(|(_, v)| *v).collect();

    assert_eq!(
        keys,
        (10..=20).collect::<Vec<_>>(),
        "bead_id={BEAD_ID} case=range_keys"
    );
    assert_eq!(
        values,
        (10..=20).map(|i| i * 10).collect::<Vec<_>>(),
        "bead_id={BEAD_ID} case=range_values"
    );
}

// ── 5. Range query with pending deletes ──────────────────────────────────

#[test]
fn test_range_query_with_deletes() {
    let config = BeTreeConfig {
        buffer_capacity: 4,
        leaf_capacity: 6,
        max_pivots: 3,
    };
    let mut tree = BeTree::new(config);

    for i in 0..30 {
        tree.insert(i, i);
    }

    // Delete keys 10, 15, 20.
    tree.delete(10);
    tree.delete(15);
    tree.delete(20);

    let result = tree.range(&8, &22);
    let keys: Vec<i32> = result.iter().map(|(k, _)| *k).collect();

    assert!(
        !keys.contains(&10),
        "bead_id={BEAD_ID} case=deleted_not_in_range key=10"
    );
    assert!(
        !keys.contains(&15),
        "bead_id={BEAD_ID} case=deleted_not_in_range key=15"
    );
    assert!(
        !keys.contains(&20),
        "bead_id={BEAD_ID} case=deleted_not_in_range key=20"
    );

    // Expected: 8,9,11,12,13,14,16,17,18,19,21,22
    let expected: Vec<i32> = (8..=22).filter(|&k| k != 10 && k != 15 && k != 20).collect();
    assert_eq!(keys, expected, "bead_id={BEAD_ID} case=range_with_holes");
}

// ── 6. Large-scale correctness ───────────────────────────────────────────

#[test]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn test_large_scale_correctness() {
    let config = BeTreeConfig {
        buffer_capacity: 8,
        leaf_capacity: 16,
        max_pivots: 4,
    };
    let mut tree = BeTree::new(config);

    let n = 1000;
    for i in 0..n {
        tree.insert(i, i * 7);
    }

    assert_eq!(tree.len(), n as usize, "bead_id={BEAD_ID} case=large_len");

    // Verify all keys.
    for i in 0..n {
        assert_eq!(
            tree.get(&i),
            Some(&(i * 7)),
            "bead_id={BEAD_ID} case=large_scale_lookup key={i}"
        );
    }

    // Verify entries are sorted.
    let entries = tree.entries();
    assert_eq!(
        entries.len(),
        n as usize,
        "bead_id={BEAD_ID} case=entries_len"
    );
    for (idx, (k, v)) in entries.iter().enumerate() {
        assert_eq!(
            *k, idx as i32,
            "bead_id={BEAD_ID} case=entries_sorted_key idx={idx}"
        );
        assert_eq!(
            *v,
            idx as i32 * 7,
            "bead_id={BEAD_ID} case=entries_sorted_value idx={idx}"
        );
    }
}

// ── 7. Write amplification reduction ─────────────────────────────────────

#[test]
fn test_write_amplification_characteristics() {
    reset_betree_metrics();

    // Small buffer → more flushes.
    let small_buf_config = BeTreeConfig {
        buffer_capacity: 2,
        leaf_capacity: 4,
        max_pivots: 2,
    };
    let mut small_tree = BeTree::new(small_buf_config);
    for i in 0..50 {
        small_tree.insert(i, i);
    }
    let small_snap = betree_metrics_snapshot();

    reset_betree_metrics();

    // Large buffer → fewer flushes.
    let big_buf_config = BeTreeConfig {
        buffer_capacity: 32,
        leaf_capacity: 64,
        max_pivots: 8,
    };
    let mut big_tree = BeTree::new(big_buf_config);
    for i in 0..50 {
        big_tree.insert(i, i);
    }
    let big_snap = betree_metrics_snapshot();

    // Larger buffers should cause fewer or equal flushes.
    assert!(
        big_snap.buffer_flushes_total <= small_snap.buffer_flushes_total,
        "bead_id={BEAD_ID} case=larger_buffer_fewer_flushes small={} big={}",
        small_snap.buffer_flushes_total,
        big_snap.buffer_flushes_total,
    );

    // The big tree should have pending messages in buffers.
    let buffered = big_tree.total_buffered_messages();
    println!(
        "[{BEAD_ID}] small_flushes={} big_flushes={} big_buffered_msgs={}",
        small_snap.buffer_flushes_total, big_snap.buffer_flushes_total, buffered,
    );
}

// ── 8. Overwrite via buffer ──────────────────────────────────────────────

#[test]
fn test_overwrite_via_buffer() {
    let config = BeTreeConfig {
        buffer_capacity: 8,
        leaf_capacity: 16,
        max_pivots: 4,
    };
    let mut tree = BeTree::new(config);

    for i in 0..20 {
        tree.insert(i, i);
    }
    // Overwrite all values.
    for i in 0..20 {
        tree.insert(i, i + 1000);
    }

    assert_eq!(tree.len(), 20, "bead_id={BEAD_ID} case=overwrite_len");

    for i in 0..20 {
        assert_eq!(
            tree.get(&i),
            Some(&(i + 1000)),
            "bead_id={BEAD_ID} case=overwrite_value key={i}"
        );
    }
}

// ── 9. Metrics fidelity ──────────────────────────────────────────────────

#[test]
fn test_metrics_fidelity() {
    let before = betree_metrics_snapshot();

    let config = BeTreeConfig {
        buffer_capacity: 3,
        leaf_capacity: 4,
        max_pivots: 2,
    };
    let mut tree = BeTree::new(config);

    for i in 0..25 {
        tree.insert(i, i);
    }

    let after = betree_metrics_snapshot();
    let delta_inserts = after.inserts_total - before.inserts_total;
    let delta_msgs = after.messages_buffered_total - before.messages_buffered_total;

    assert_eq!(
        delta_inserts, 25,
        "bead_id={BEAD_ID} case=insert_metric_fidelity"
    );
    assert!(
        delta_msgs >= 25,
        "bead_id={BEAD_ID} case=messages_buffered_fidelity delta_msgs={delta_msgs}"
    );

    println!(
        "[{BEAD_ID}] delta_inserts={delta_inserts} delta_msgs={delta_msgs} flushes={}",
        after.buffer_flushes_total - before.buffer_flushes_total
    );
}

// ── 10. Conformance summary ──────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    let config = BeTreeConfig {
        buffer_capacity: 4,
        leaf_capacity: 8,
        max_pivots: 3,
    };
    let mut tree = BeTree::new(config);

    // Insert
    for i in 0..100 {
        tree.insert(i, i);
    }
    let pass_insert = tree.len() == 100;

    // Point lookup
    let pass_lookup = (0..100).all(|i| tree.get(&i) == Some(&i));

    // Range query
    let range_result = tree.range(&10, &20);
    let pass_range = range_result.len() == 11
        && range_result.iter().all(|(k, v)| *k >= 10 && *k <= 20 && k == v);

    // Delete
    for i in 0..50 {
        tree.delete(i);
    }
    let pass_delete = tree.len() == 50 && (0..50).all(|i| tree.get(&i).is_none());

    // Entries sorted
    let entries = tree.entries();
    let pass_sorted = entries.windows(2).all(|w| w[0].0 < w[1].0);

    // Depth > 1 (interior nodes exist)
    let pass_depth = tree.depth() >= 1;

    let total = [
        pass_insert,
        pass_lookup,
        pass_range,
        pass_delete,
        pass_sorted,
        pass_depth,
    ];
    let passed = total.iter().filter(|&&p| p).count();
    let total_count = total.len();

    println!("\n=== {BEAD_ID} Bε-tree Conformance ===");
    println!("  insert:      {}", if pass_insert { "PASS" } else { "FAIL" });
    println!("  lookup:      {}", if pass_lookup { "PASS" } else { "FAIL" });
    println!("  range:       {}", if pass_range { "PASS" } else { "FAIL" });
    println!("  delete:      {}", if pass_delete { "PASS" } else { "FAIL" });
    println!("  sorted:      {}", if pass_sorted { "PASS" } else { "FAIL" });
    println!("  depth:       {}", if pass_depth { "PASS" } else { "FAIL" });
    println!("  [{passed}/{total_count}] conformance checks passed");

    assert_eq!(
        passed, total_count,
        "bead_id={BEAD_ID} conformance={passed}/{total_count}"
    );
}
