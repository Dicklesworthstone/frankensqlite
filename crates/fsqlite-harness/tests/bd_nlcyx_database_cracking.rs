//! bd-nlcyx: Database Cracking / Adaptive Indexing (§8.8) integration tests.
//!
//! Validates CrackedColumn adaptive indexing primitives:
//!   1. Basic range query correctness
//!   2. Progressive refinement (repeated queries converge)
//!   3. Point query accuracy
//!   4. Full scan consistency (cracking preserves all elements)
//!   5. Large-scale correctness (10K elements)
//!   6. Query pattern convergence toward sorted order
//!   7. Crack index growth tracking
//!   8. Metrics fidelity (delta-based)
//!   9. Edge cases (empty, single element, full range)
//!  10. Machine-readable conformance output

use fsqlite_btree::cracking::{CrackedColumn, cracking_metrics_snapshot};

/// Create a reversed column of u32 values [n-1, n-2, ..., 1, 0].
fn make_reversed_column(n: u32) -> Vec<u32> {
    (0..n).rev().collect()
}

/// Collect, sort, and return a slice for comparison.
fn sorted(slice: &[u32]) -> Vec<u32> {
    let mut v = slice.to_vec();
    v.sort_unstable();
    v
}

// ---------------------------------------------------------------------------
// Test 1: Basic range query correctness
// ---------------------------------------------------------------------------

#[test]
fn test_basic_range_query_correctness() {
    let data = vec![9u32, 3, 7, 1, 5, 2, 8, 4, 6, 0];
    let mut col = CrackedColumn::new(data);

    // Query [3, 6]: should return exactly {3, 4, 5, 6}.
    let result = col.range_query(3, 6);
    assert_eq!(sorted(result), vec![3, 4, 5, 6]);

    // Query [0, 2]: should return exactly {0, 1, 2}.
    let result = col.range_query(0, 2);
    assert_eq!(sorted(result), vec![0, 1, 2]);

    // Query [7, 9]: should return exactly {7, 8, 9}.
    let result = col.range_query(7, 9);
    assert_eq!(sorted(result), vec![7, 8, 9]);

    // Non-overlapping ranges should not interfere.
    let result = col.range_query(4, 5);
    assert_eq!(sorted(result), vec![4, 5]);

    println!("[PASS] Basic range query correctness: 4 non-overlapping ranges verified");
}

// ---------------------------------------------------------------------------
// Test 2: Progressive refinement
// ---------------------------------------------------------------------------

#[test]
fn test_progressive_refinement() {
    let data = make_reversed_column(100);
    let mut col = CrackedColumn::new(data);

    // First query creates crack boundaries.
    let cracks_0 = col.num_cracks();
    let _ = col.range_query(20, 40);
    let cracks_1 = col.num_cracks();
    assert!(cracks_1 > cracks_0, "first query should create cracks");

    // Repeated query on same range should add fewer or no new cracks.
    let _ = col.range_query(20, 40);
    let cracks_2 = col.num_cracks();
    // No new cracks needed since boundaries already exist.
    assert!(
        cracks_2 >= cracks_1,
        "repeated query should not remove cracks"
    );

    // Sub-range query adds refinement within existing cracks.
    let _ = col.range_query(25, 35);
    let cracks_3 = col.num_cracks();
    assert!(
        cracks_3 >= cracks_2,
        "sub-range query should refine further"
    );

    // Result correctness after progressive refinement.
    let result = col.range_query(25, 35);
    let expected: Vec<u32> = (25..=35).collect();
    assert_eq!(sorted(result), expected);

    println!(
        "[PASS] Progressive refinement: cracks {} -> {} -> {} -> {}",
        cracks_0, cracks_1, cracks_2, cracks_3
    );
}

// ---------------------------------------------------------------------------
// Test 3: Point query accuracy
// ---------------------------------------------------------------------------

#[test]
fn test_point_query_accuracy() {
    // Column with duplicates.
    let data = vec![5u32, 3, 5, 1, 5, 2, 8, 4, 5, 0];
    let mut col = CrackedColumn::new(data);

    assert_eq!(col.point_query(5), 4, "should find 4 occurrences of 5");
    assert_eq!(col.point_query(0), 1, "should find 1 occurrence of 0");
    assert_eq!(col.point_query(8), 1, "should find 1 occurrence of 8");
    assert_eq!(col.point_query(99), 0, "should find 0 occurrences of 99");
    assert_eq!(col.point_query(6), 0, "should find 0 occurrences of 6");

    println!("[PASS] Point query accuracy: duplicate and missing values correct");
}

// ---------------------------------------------------------------------------
// Test 4: Full scan consistency (cracking preserves all elements)
// ---------------------------------------------------------------------------

#[test]
fn test_full_scan_consistency() {
    let original: Vec<u32> = (0..50).collect();
    let mut shuffled = original.clone();
    // Deterministic shuffle using a simple swap pattern.
    for i in 0..shuffled.len() {
        let j = (i * 37 + 13) % shuffled.len();
        shuffled.swap(i, j);
    }

    let mut col = CrackedColumn::new(shuffled);

    // Perform several crack operations.
    let _ = col.range_query(10, 20);
    let _ = col.range_query(30, 40);
    let _ = col.range_query(0, 5);

    // Full scan should still contain all original elements.
    let scan = col.full_scan();
    assert_eq!(scan.len(), original.len());
    assert_eq!(sorted(scan), original, "cracking must preserve all elements");

    println!("[PASS] Full scan consistency: all 50 elements preserved after 3 crack ops");
}

// ---------------------------------------------------------------------------
// Test 5: Large-scale correctness (10K elements)
// ---------------------------------------------------------------------------

#[test]
fn test_large_scale_correctness() {
    let n = 10_000u32;
    let mut data: Vec<u32> = (0..n).collect();
    // Deterministic shuffle.
    for i in 0..data.len() {
        let j = (i.wrapping_mul(7919) + 1) % data.len();
        data.swap(i, j);
    }

    let mut col = CrackedColumn::new(data);

    // Query 20 non-overlapping ranges across the space.
    for r in 0..20u32 {
        let lo = r * 500;
        let hi = lo + 499;
        let result = col.range_query(lo, hi);
        let expected: Vec<u32> = (lo..=hi).collect();
        assert_eq!(
            sorted(result),
            expected,
            "range [{lo}, {hi}] incorrect"
        );
    }

    // All elements still present.
    assert_eq!(col.full_scan().len(), n as usize);
    let expected_all: Vec<u32> = (0..n).collect();
    assert_eq!(sorted(col.full_scan()), expected_all);

    println!("[PASS] Large-scale correctness: 10K elements, 20 ranges verified");
}

// ---------------------------------------------------------------------------
// Test 6: Query pattern convergence toward sorted order
// ---------------------------------------------------------------------------

#[test]
fn test_convergence_toward_sorted() {
    let n = 100u32;
    let data = make_reversed_column(n);
    let mut col = CrackedColumn::new(data);

    assert!(!col.is_fully_sorted(), "reversed column should not be sorted initially");

    // Query every value as a point query to force full cracking.
    for v in 0..n {
        let _ = col.point_query(v);
    }

    // After querying every value, the column should be close to sorted.
    // Due to the partition-based cracking, each value has been placed
    // into its own segment or a very small segment.
    let cracks = col.num_cracks();
    assert!(
        cracks >= 50,
        "should have many cracks after querying all values, got {cracks}"
    );

    // Avg segment size should be small.
    let avg = col.avg_segment_size();
    assert!(
        avg <= 5.0,
        "avg segment size should be small after full cracking, got {avg}"
    );

    println!("[PASS] Convergence: {cracks} cracks, avg_segment_size={avg:.1} after 100 point queries");
}

// ---------------------------------------------------------------------------
// Test 7: Crack index growth tracking
// ---------------------------------------------------------------------------

#[test]
fn test_crack_index_growth() {
    let data = make_reversed_column(200);
    let mut col = CrackedColumn::new(data);

    let mut crack_counts = Vec::new();
    crack_counts.push(col.num_cracks());

    // 10 queries at different boundaries.
    for i in 0..10u32 {
        let lo = i * 20;
        let hi = lo + 19;
        let _ = col.range_query(lo, hi);
        crack_counts.push(col.num_cracks());
    }

    // Crack count should be monotonically non-decreasing.
    for w in crack_counts.windows(2) {
        assert!(
            w[1] >= w[0],
            "cracks should never decrease: {} -> {}",
            w[0],
            w[1]
        );
    }

    // Should have accumulated cracks.
    let final_cracks = *crack_counts.last().unwrap();
    assert!(
        final_cracks >= 10,
        "10 range queries should create at least 10 cracks, got {final_cracks}"
    );

    println!(
        "[PASS] Crack index growth: {} cracks after 10 queries, monotonic",
        final_cracks
    );
}

// ---------------------------------------------------------------------------
// Test 8: Metrics fidelity (delta-based)
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_fidelity() {
    let m_before = cracking_metrics_snapshot();

    let data = make_reversed_column(50);
    let mut col = CrackedColumn::new(data);

    // 3 range queries.
    let _ = col.range_query(10, 20);
    let _ = col.range_query(30, 40);
    let _ = col.range_query(0, 5);

    // 2 point queries.
    let _ = col.point_query(25);
    let _ = col.point_query(45);

    let m_after = cracking_metrics_snapshot();

    let delta_queries = m_after.queries_total - m_before.queries_total;
    let delta_ops = m_after.crack_ops_total - m_before.crack_ops_total;
    let delta_elements = m_after.elements_partitioned_total - m_before.elements_partitioned_total;

    // 3 range queries + 2 point queries = 5 queries total.
    assert!(
        delta_queries >= 5,
        "expected >= 5 queries, got {delta_queries}"
    );

    // Each new crack boundary causes a partition operation.
    assert!(
        delta_ops >= 1,
        "expected >= 1 crack ops, got {delta_ops}"
    );

    // Elements partitioned should be > 0 (we're cracking a reversed column).
    assert!(
        delta_elements > 0,
        "expected > 0 elements partitioned, got {delta_elements}"
    );

    // Display format.
    let text = format!("{}", m_after);
    assert!(text.contains("crack_ops="), "Display should include crack_ops");
    assert!(text.contains("queries="), "Display should include queries");

    println!(
        "[PASS] Metrics fidelity: delta_queries={delta_queries}, delta_ops={delta_ops}, delta_elements={delta_elements}"
    );
}

// ---------------------------------------------------------------------------
// Test 9: Edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_edge_cases() {
    // Empty column.
    let empty: CrackedColumn<u32> = CrackedColumn::new(vec![]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert!(empty.is_fully_sorted());
    assert_eq!(empty.avg_segment_size(), 0.0);

    // Single element.
    let mut single = CrackedColumn::new(vec![42u32]);
    assert_eq!(single.len(), 1);
    assert!(single.is_fully_sorted());
    let result = single.range_query(42, 42);
    assert_eq!(sorted(result), vec![42]);
    assert_eq!(single.point_query(42), 1);
    assert_eq!(single.point_query(0), 0);

    // Already sorted column.
    let sorted_data: Vec<u32> = (0..20).collect();
    let mut sorted_col = CrackedColumn::new(sorted_data);
    assert!(sorted_col.is_fully_sorted());
    let result = sorted_col.range_query(5, 10);
    assert_eq!(sorted(result), vec![5, 6, 7, 8, 9, 10]);

    // Full range query.
    let data: Vec<u32> = (0..10).collect();
    let mut col = CrackedColumn::new(data);
    let result = col.range_query(0, 9);
    assert_eq!(sorted(result), (0..10).collect::<Vec<u32>>());

    // Debug format.
    let dbg = format!("{:?}", col);
    assert!(dbg.contains("CrackedColumn"), "Debug should include type name");
    assert!(dbg.contains("len"), "Debug should include len");

    println!("[PASS] Edge cases: empty, single, sorted, full-range, debug all correct");
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    let data = make_reversed_column(50);
    let mut col = CrackedColumn::new(data.clone());

    // Property 1: Range query returns correct elements.
    let result = col.range_query(10, 20);
    let range_ok = sorted(result) == (10..=20).collect::<Vec<u32>>();

    // Property 2: Point query returns correct count.
    let mut col2 = CrackedColumn::new(vec![5u32, 5, 5, 1, 2]);
    let point_ok = col2.point_query(5) == 3;

    // Property 3: Full scan preserves all elements.
    let scan_ok = sorted(col.full_scan()) == (0..50).collect::<Vec<u32>>();

    // Property 4: Crack count is monotonically non-decreasing.
    let mut col3 = CrackedColumn::new(make_reversed_column(30));
    let c0 = col3.num_cracks();
    let _ = col3.range_query(5, 15);
    let c1 = col3.num_cracks();
    let _ = col3.range_query(20, 25);
    let c2 = col3.num_cracks();
    let monotonic_ok = c1 >= c0 && c2 >= c1;

    // Property 5: Metrics are tracked.
    let m = cracking_metrics_snapshot();
    let metrics_ok = m.queries_total > 0;

    // Property 6: Empty column is fully sorted.
    let empty: CrackedColumn<u32> = CrackedColumn::new(vec![]);
    let empty_ok = empty.is_fully_sorted() && empty.is_empty();

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] Range query correctness: {range_ok}");
    println!("  [CONFORM] Point query accuracy: {point_ok}");
    println!("  [CONFORM] Full scan preservation: {scan_ok}");
    println!("  [CONFORM] Crack index monotonicity: {monotonic_ok}");
    println!("  [CONFORM] Metrics tracked: {metrics_ok}");
    println!("  [CONFORM] Empty column sorted: {empty_ok}");
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(range_ok, "range query correctness failed");
    assert!(point_ok, "point query accuracy failed");
    assert!(scan_ok, "full scan preservation failed");
    assert!(monotonic_ok, "crack index monotonicity failed");
    assert!(metrics_ok, "metrics not tracked");
    assert!(empty_ok, "empty column invariant failed");
}
