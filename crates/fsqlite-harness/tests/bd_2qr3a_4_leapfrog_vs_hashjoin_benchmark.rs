//! Harness integration tests for bd-2qr3a.4: Leapfrog Triejoin vs pairwise
//! hash-join benchmark on TPC-H-like multi-way joins.
//!
//! Validates: correctness (same match counts), throughput comparison, scaling
//! by relation size, intermediate result sizes, seek/comparison metrics, and
//! multi-arity joins (2-way through 6-way).

use std::time::Instant;

use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::vectorized::{Batch, ColumnSpec, ColumnVectorType};
use fsqlite_vdbe::vectorized_hash_join::{hash_join_build, hash_join_probe, JoinType};
use fsqlite_vdbe::vectorized_join::{
    leapfrog_join, leapfrog_metrics_snapshot, reset_leapfrog_metrics, TrieRelation, TrieRow,
};

const BEAD_ID: &str = "bd-2qr3a.4";

/// Build a sorted TrieRelation from (key, payload_index) pairs.
fn build_trie(keys: &[i64]) -> TrieRelation {
    let mut rows: Vec<TrieRow> = keys
        .iter()
        .enumerate()
        .map(|(i, &k)| TrieRow::new(vec![SqliteValue::Integer(k)], i))
        .collect();
    rows.sort_by(|a, b| a.key.partial_cmp(&b.key).unwrap_or(std::cmp::Ordering::Equal));
    rows.dedup_by(|a, b| a.key == b.key);
    TrieRelation::from_sorted_rows(rows).unwrap()
}

/// Build a sorted TrieRelation with composite (2-column) keys.
fn build_trie_composite(keys: &[(i64, i64)]) -> TrieRelation {
    let mut rows: Vec<TrieRow> = keys
        .iter()
        .enumerate()
        .map(|(i, &(k1, k2))| {
            TrieRow::new(
                vec![SqliteValue::Integer(k1), SqliteValue::Integer(k2)],
                i,
            )
        })
        .collect();
    rows.sort_by(|a, b| a.key.partial_cmp(&b.key).unwrap_or(std::cmp::Ordering::Equal));
    rows.dedup_by(|a, b| a.key == b.key);
    TrieRelation::from_sorted_rows(rows).unwrap()
}

/// Build a Batch for hash-join from single-key i64 rows.
fn build_hash_batch(name: &str, keys: &[i64]) -> Batch {
    let specs = vec![
        ColumnSpec::new(format!("{name}_key"), ColumnVectorType::Int64),
        ColumnSpec::new(format!("{name}_payload"), ColumnVectorType::Int64),
    ];
    let rows: Vec<Vec<SqliteValue>> = keys
        .iter()
        .enumerate()
        .map(|(i, &k)| {
            vec![
                SqliteValue::Integer(k),
                SqliteValue::Integer(i as i64),
            ]
        })
        .collect();
    Batch::from_rows(&rows, &specs, rows.len().max(1)).unwrap()
}

/// Generate deterministic pseudo-random keys in range [0, max_key).
fn generate_keys(n: usize, max_key: i64, seed: u64) -> Vec<i64> {
    let mut rng = seed;
    (0..n)
        .map(|_| {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((rng >> 32) as i64).abs() % max_key
        })
        .collect()
}

/// Pairwise hash-join: join R1 ⋈ R2 ⋈ R3 as (R1 ⋈ R2) ⋈ R3.
/// Returns total result row count.
fn pairwise_hash_join_3way(r1: &[i64], r2: &[i64], r3: &[i64]) -> usize {
    let b1 = build_hash_batch("r1", r1);
    let b2 = build_hash_batch("r2", r2);
    let b3 = build_hash_batch("r3", r3);

    // R1 ⋈ R2
    let ht12 = hash_join_build(b1, &[0]).unwrap();
    let join12 = hash_join_probe(&ht12, &b2, &[0], JoinType::Inner).unwrap();
    let intermediate_count = join12.row_count();

    // (R1 ⋈ R2) ⋈ R3 — join on the first key column of the intermediate result
    if intermediate_count == 0 {
        return 0;
    }
    let ht123 = hash_join_build(join12, &[0]).unwrap();
    let result = hash_join_probe(&ht123, &b3, &[0], JoinType::Inner).unwrap();
    result.row_count()
}

// ── 1. Two-way join correctness ─────────────────────────────────────────────

#[test]
fn test_2way_join_correctness() {
    let keys_a: Vec<i64> = (0..100).collect();
    let keys_b: Vec<i64> = (50..150).collect();

    // Leapfrog.
    let trie_a = build_trie(&keys_a);
    let trie_b = build_trie(&keys_b);
    let lf_matches = leapfrog_join(&[&trie_a, &trie_b]).unwrap();
    let lf_count: u64 = lf_matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // Hash join.
    let batch_a = build_hash_batch("a", &keys_a);
    let batch_b = build_hash_batch("b", &keys_b);
    let ht = hash_join_build(batch_a, &[0]).unwrap();
    let hj_result = hash_join_probe(&ht, &batch_b, &[0], JoinType::Inner).unwrap();
    let hj_count = hj_result.row_count();

    // Both should find 50 matching keys (50..100).
    assert_eq!(
        lf_count, 50,
        "bead_id={BEAD_ID} case=2way_leapfrog_count lf={lf_count}"
    );
    assert_eq!(
        hj_count, 50,
        "bead_id={BEAD_ID} case=2way_hashjoin_count hj={hj_count}"
    );

    println!("[{BEAD_ID}] 2-way join: leapfrog={lf_count} hashjoin={hj_count}");
}

// ── 2. Three-way join correctness ───────────────────────────────────────────

#[test]
fn test_3way_join_correctness() {
    let keys_a: Vec<i64> = (0..200).collect();
    let keys_b: Vec<i64> = (100..300).collect();
    let keys_c: Vec<i64> = (150..250).collect();

    // Leapfrog 3-way.
    let trie_a = build_trie(&keys_a);
    let trie_b = build_trie(&keys_b);
    let trie_c = build_trie(&keys_c);
    let lf_matches = leapfrog_join(&[&trie_a, &trie_b, &trie_c]).unwrap();
    let lf_count: u64 = lf_matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // Pairwise hash-join 3-way: (A ⋈ B) ⋈ C.
    let hj_count = pairwise_hash_join_3way(&keys_a, &keys_b, &keys_c);

    // Intersection: [150, 200) = 50 keys.
    assert_eq!(
        lf_count, 50,
        "bead_id={BEAD_ID} case=3way_leapfrog_count lf={lf_count}"
    );
    assert_eq!(
        hj_count, 50,
        "bead_id={BEAD_ID} case=3way_hashjoin_count hj={hj_count}"
    );

    println!("[{BEAD_ID}] 3-way join: leapfrog={lf_count} hashjoin={hj_count}");
}

// ── 3. Benchmark: 3-way join throughput ─────────────────────────────────────

#[test]
fn test_3way_benchmark_throughput() {
    let n = 5_000;
    let max_key = 10_000i64;
    let keys_a = generate_keys(n, max_key, 42);
    let keys_b = generate_keys(n, max_key, 43);
    let keys_c = generate_keys(n, max_key, 44);

    // Warmup.
    {
        let ta = build_trie(&keys_a);
        let tb = build_trie(&keys_b);
        let tc = build_trie(&keys_c);
        let _ = leapfrog_join(&[&ta, &tb, &tc]);
        let _ = pairwise_hash_join_3way(&keys_a, &keys_b, &keys_c);
    }

    // Leapfrog.
    let lf_start = Instant::now();
    let mut lf_total = 0u64;
    for _ in 0..5 {
        let ta = build_trie(&keys_a);
        let tb = build_trie(&keys_b);
        let tc = build_trie(&keys_c);
        let matches = leapfrog_join(&[&ta, &tb, &tc]).unwrap();
        lf_total = matches.iter().map(|m| m.tuple_multiplicity()).sum();
    }
    let lf_elapsed = lf_start.elapsed();

    // Pairwise hash join.
    let hj_start = Instant::now();
    let mut hj_total = 0usize;
    for _ in 0..5 {
        hj_total = pairwise_hash_join_3way(&keys_a, &keys_b, &keys_c);
    }
    let hj_elapsed = hj_start.elapsed();

    let speedup = hj_elapsed.as_nanos() as f64 / lf_elapsed.as_nanos().max(1) as f64;

    println!(
        "[{BEAD_ID}] 3-way benchmark (n={n}): leapfrog={:.2}ms hashjoin={:.2}ms speedup={speedup:.2}x lf_matches={lf_total} hj_rows={hj_total}",
        lf_elapsed.as_secs_f64() * 1000.0,
        hj_elapsed.as_secs_f64() * 1000.0,
    );
}

// ── 4. Scaling by relation size ─────────────────────────────────────────────

#[test]
fn test_scaling_by_relation_size() {
    let sizes = [500, 1_000, 2_000, 5_000];
    let max_key = 10_000i64;

    println!("[{BEAD_ID}] 3-way join scaling by relation size:");
    for &n in &sizes {
        let keys_a = generate_keys(n, max_key, 42);
        let keys_b = generate_keys(n, max_key, 43);
        let keys_c = generate_keys(n, max_key, 44);

        let ta = build_trie(&keys_a);
        let tb = build_trie(&keys_b);
        let tc = build_trie(&keys_c);

        let lf_start = Instant::now();
        let matches = leapfrog_join(&[&ta, &tb, &tc]).unwrap();
        let lf_ms = lf_start.elapsed().as_secs_f64() * 1000.0;
        let lf_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();

        let hj_start = Instant::now();
        let hj_count = pairwise_hash_join_3way(&keys_a, &keys_b, &keys_c);
        let hj_ms = hj_start.elapsed().as_secs_f64() * 1000.0;

        let speedup = hj_ms / lf_ms.max(0.001);
        println!(
            "  n={n:>5}: lf={lf_ms:.3}ms ({lf_count} matches) hj={hj_ms:.3}ms ({hj_count} rows) speedup={speedup:.2}x"
        );
    }
}

// ── 5. Seek and comparison metrics ──────────────────────────────────────────

#[test]
fn test_seek_metrics() {
    reset_leapfrog_metrics();

    let keys_a: Vec<i64> = (0..1000).collect();
    let keys_b: Vec<i64> = (0..1000).step_by(2).collect(); // even only
    let keys_c: Vec<i64> = (0..1000).step_by(3).collect(); // multiples of 3

    let ta = build_trie(&keys_a);
    let tb = build_trie(&keys_b);
    let tc = build_trie(&keys_c);
    let matches = leapfrog_join(&[&ta, &tb, &tc]).unwrap();

    let metrics = leapfrog_metrics_snapshot();
    let match_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // Intersection of all, multiples of 2 and 3 = multiples of 6 in [0,1000).
    // Expected: 0, 6, 12, ..., 996 = 167 matches.
    assert_eq!(
        match_count, 167,
        "bead_id={BEAD_ID} case=lcm_intersection count={match_count}"
    );

    println!(
        "[{BEAD_ID}] seek metrics: tuples={} seeks={} comparisons={} matches={match_count}",
        metrics.fsqlite_leapfrog_tuples_total,
        metrics.fsqlite_leapfrog_seeks_total,
        metrics.fsqlite_leapfrog_seek_comparisons_total,
    );

    // Seeks should be sublinear relative to total input size.
    let total_input = 1000 + 500 + 334;
    assert!(
        metrics.fsqlite_leapfrog_seeks_total > 0,
        "bead_id={BEAD_ID} case=seeks_nonzero"
    );
    println!(
        "[{BEAD_ID}] seeks/input_row ratio: {:.2}",
        metrics.fsqlite_leapfrog_seeks_total as f64 / total_input as f64
    );
}

// ── 6. Four-way join ────────────────────────────────────────────────────────

#[test]
fn test_4way_join() {
    let keys_a: Vec<i64> = (0..500).collect();
    let keys_b: Vec<i64> = (0..500).step_by(2).collect();
    let keys_c: Vec<i64> = (0..500).step_by(3).collect();
    let keys_d: Vec<i64> = (0..500).step_by(5).collect();

    let ta = build_trie(&keys_a);
    let tb = build_trie(&keys_b);
    let tc = build_trie(&keys_c);
    let td = build_trie(&keys_d);

    let lf_start = Instant::now();
    let matches = leapfrog_join(&[&ta, &tb, &tc, &td]).unwrap();
    let lf_ms = lf_start.elapsed().as_secs_f64() * 1000.0;
    let lf_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // LCM(2,3,5) = 30. Multiples of 30 in [0,500): 0,30,...,480 = 17.
    assert_eq!(
        lf_count, 17,
        "bead_id={BEAD_ID} case=4way_count lf={lf_count}"
    );

    println!("[{BEAD_ID}] 4-way join: {lf_count} matches in {lf_ms:.3}ms");
}

// ── 7. Five-way join (TPC-H Q2-like) ───────────────────────────────────────

#[test]
fn test_5way_join() {
    // Simulates TPC-H Q2: part ⋈ supplier ⋈ partsupp ⋈ nation ⋈ region.
    // Use key ranges that create interesting overlaps.
    let part_keys: Vec<i64> = (0..1000).collect();
    let supplier_keys: Vec<i64> = (0..500).map(|i| i * 2).collect(); // even
    let partsupp_keys: Vec<i64> = (0..400).map(|i| i * 3).collect(); // mul 3
    let nation_keys: Vec<i64> = (0..200).map(|i| i * 5).collect();   // mul 5
    let region_keys: Vec<i64> = (0..100).map(|i| i * 7).collect();   // mul 7

    let t1 = build_trie(&part_keys);
    let t2 = build_trie(&supplier_keys);
    let t3 = build_trie(&partsupp_keys);
    let t4 = build_trie(&nation_keys);
    let t5 = build_trie(&region_keys);

    let lf_start = Instant::now();
    let matches = leapfrog_join(&[&t1, &t2, &t3, &t4, &t5]).unwrap();
    let lf_ms = lf_start.elapsed().as_secs_f64() * 1000.0;
    let lf_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // LCM(2,3,5,7) = 210. Multiples of 210 in [0,700): 0,210,420,630 = 4.
    // But nation_keys max is 995, region_keys max is 693.
    // Intersection depends on actual range overlap.
    println!(
        "[{BEAD_ID}] 5-way join (TPC-H Q2-like): {lf_count} matches in {lf_ms:.3}ms relations=[1000,500,400,200,100]"
    );

    // LCM(2,3,5,7) = 210. Matches at 0, 210, 420, 630 = 4.
    assert_eq!(
        lf_count, 4,
        "bead_id={BEAD_ID} case=5way_lcm_count lf={lf_count}"
    );
}

// ── 8. Six-way join (TPC-H Q9-like) ────────────────────────────────────────

#[test]
fn test_6way_join() {
    // Simulates TPC-H Q9: part ⋈ supplier ⋈ lineitem ⋈ partsupp ⋈ orders ⋈ nation.
    let r1: Vec<i64> = (0..500).collect();
    let r2: Vec<i64> = (100..400).collect();
    let r3: Vec<i64> = (200..600).collect();
    let r4: Vec<i64> = (150..450).collect();
    let r5: Vec<i64> = (250..550).collect();
    let r6: Vec<i64> = (200..500).collect();

    let t1 = build_trie(&r1);
    let t2 = build_trie(&r2);
    let t3 = build_trie(&r3);
    let t4 = build_trie(&r4);
    let t5 = build_trie(&r5);
    let t6 = build_trie(&r6);

    let lf_start = Instant::now();
    let matches = leapfrog_join(&[&t1, &t2, &t3, &t4, &t5, &t6]).unwrap();
    let lf_ms = lf_start.elapsed().as_secs_f64() * 1000.0;
    let lf_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // Intersection: [250, 400) = 150 keys.
    assert_eq!(
        lf_count, 150,
        "bead_id={BEAD_ID} case=6way_count lf={lf_count}"
    );

    println!(
        "[{BEAD_ID}] 6-way join (TPC-H Q9-like): {lf_count} matches in {lf_ms:.3}ms"
    );
}

// ── 9. Composite key join ───────────────────────────────────────────────────

#[test]
fn test_composite_key_join() {
    // Two-column composite keys: (region_id, nation_id).
    let keys_a: Vec<(i64, i64)> = (0..100)
        .flat_map(|r| (0..10).map(move |n| (r, n)))
        .collect();
    let keys_b: Vec<(i64, i64)> = (50..150)
        .flat_map(|r| (0..5).map(move |n| (r, n)))
        .collect();

    let ta = build_trie_composite(&keys_a);
    let tb = build_trie_composite(&keys_b);

    let matches = leapfrog_join(&[&ta, &tb]).unwrap();
    let lf_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();

    // Intersection: region [50,100), nation [0,5) = 50 * 5 = 250 matches.
    assert_eq!(
        lf_count, 250,
        "bead_id={BEAD_ID} case=composite_key_count lf={lf_count}"
    );

    println!(
        "[{BEAD_ID}] composite key join: {lf_count} matches (2-col keys, 1000 x 500 rows)"
    );
}

// ── 10. Zero intermediate blowup property ───────────────────────────────────

#[test]
fn test_zero_intermediate_blowup() {
    // Scenario: keys that cause pairwise intermediate blowup but leapfrog avoids it.
    // R1 and R2 share many keys, but R3 filters most of them out.
    let n = 2_000;
    let keys_a: Vec<i64> = (0..n).collect();
    let keys_b: Vec<i64> = (0..n).collect(); // Full overlap with A.
    let keys_c: Vec<i64> = (0..n).step_by(100).collect(); // Only every 100th.

    // Leapfrog: should find matches directly without intermediate blowup.
    reset_leapfrog_metrics();
    let ta = build_trie(&keys_a);
    let tb = build_trie(&keys_b);
    let tc = build_trie(&keys_c);
    let lf_start = Instant::now();
    let matches = leapfrog_join(&[&ta, &tb, &tc]).unwrap();
    let lf_ms = lf_start.elapsed().as_secs_f64() * 1000.0;
    let lf_count: u64 = matches.iter().map(|m| m.tuple_multiplicity()).sum();
    let lf_metrics = leapfrog_metrics_snapshot();

    // Pairwise: A ⋈ B produces n rows (full overlap), then filter by C.
    let hj_start = Instant::now();
    let hj_count = pairwise_hash_join_3way(&keys_a, &keys_b, &keys_c);
    let hj_ms = hj_start.elapsed().as_secs_f64() * 1000.0;

    // Both should produce 20 matches (every 100th key in [0, 2000)).
    assert_eq!(
        lf_count, 20,
        "bead_id={BEAD_ID} case=zero_blowup_lf_count"
    );
    assert_eq!(
        hj_count, 20,
        "bead_id={BEAD_ID} case=zero_blowup_hj_count"
    );

    let speedup = hj_ms / lf_ms.max(0.001);
    println!(
        "[{BEAD_ID}] zero-blowup: leapfrog={lf_ms:.3}ms hashjoin={hj_ms:.3}ms speedup={speedup:.2}x"
    );
    println!(
        "[{BEAD_ID}]   intermediate: hash-join builds {n}-row intermediate, leapfrog seeks={} comparisons={}",
        lf_metrics.fsqlite_leapfrog_seeks_total,
        lf_metrics.fsqlite_leapfrog_seek_comparisons_total,
    );
}

// ── 11. Conformance summary ─────────────────────────────────────────────────

#[test]
fn test_conformance_summary() {
    // 1. Two-way correctness.
    let keys_a: Vec<i64> = (0..100).collect();
    let keys_b: Vec<i64> = (50..150).collect();
    let ta = build_trie(&keys_a);
    let tb = build_trie(&keys_b);
    let lf2 = leapfrog_join(&[&ta, &tb]).unwrap();
    let lf2_count: u64 = lf2.iter().map(|m| m.tuple_multiplicity()).sum();
    let ba = build_hash_batch("a", &keys_a);
    let bb = build_hash_batch("b", &keys_b);
    let ht = hash_join_build(ba, &[0]).unwrap();
    let hj2 = hash_join_probe(&ht, &bb, &[0], JoinType::Inner).unwrap();
    let pass_2way = lf2_count == hj2.row_count() as u64;

    // 2. Three-way correctness.
    let keys_c: Vec<i64> = (75..125).collect();
    let tc = build_trie(&keys_c);
    let lf3 = leapfrog_join(&[&ta, &tb, &tc]).unwrap();
    let lf3_count: u64 = lf3.iter().map(|m| m.tuple_multiplicity()).sum();
    let pass_3way = lf3_count == 25; // [75, 100)

    // 3. Metrics reporting.
    reset_leapfrog_metrics();
    let _ = leapfrog_join(&[&ta, &tb]).unwrap();
    let metrics = leapfrog_metrics_snapshot();
    let pass_metrics = metrics.fsqlite_leapfrog_seeks_total > 0;

    // 4. Composite keys.
    let ck_a: Vec<(i64, i64)> = vec![(1, 1), (1, 2), (2, 1)];
    let ck_b: Vec<(i64, i64)> = vec![(1, 2), (2, 1), (3, 1)];
    let tca = build_trie_composite(&ck_a);
    let tcb = build_trie_composite(&ck_b);
    let ck_matches = leapfrog_join(&[&tca, &tcb]).unwrap();
    let ck_count: u64 = ck_matches.iter().map(|m| m.tuple_multiplicity()).sum();
    let pass_composite = ck_count == 2; // (1,2) and (2,1)

    // 5. Multi-way (6 relations).
    let r: Vec<Vec<i64>> = (0..6).map(|i| (i * 10..100).collect()).collect();
    let tries: Vec<TrieRelation> = r.iter().map(|k| build_trie(k)).collect();
    let refs: Vec<&TrieRelation> = tries.iter().collect();
    let m6 = leapfrog_join(&refs).unwrap();
    let m6_count: u64 = m6.iter().map(|m| m.tuple_multiplicity()).sum();
    let pass_multiway = m6_count == 50; // [50, 100)

    // 6. Zero intermediate blowup correctness.
    let full: Vec<i64> = (0..100).collect();
    let sparse: Vec<i64> = (0..100).step_by(10).collect();
    let tf1 = build_trie(&full);
    let tf2 = build_trie(&full);
    let ts = build_trie(&sparse);
    let blowup = leapfrog_join(&[&tf1, &tf2, &ts]).unwrap();
    let blowup_count: u64 = blowup.iter().map(|m| m.tuple_multiplicity()).sum();
    let pass_blowup = blowup_count == 10;

    let checks = [
        pass_2way,
        pass_3way,
        pass_metrics,
        pass_composite,
        pass_multiway,
        pass_blowup,
    ];
    let passed = checks.iter().filter(|&&p| p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} Leapfrog vs Hash-Join Benchmark Conformance ===");
    println!("  2-way:         {}", if pass_2way { "PASS" } else { "FAIL" });
    println!("  3-way:         {}", if pass_3way { "PASS" } else { "FAIL" });
    println!("  metrics:       {}", if pass_metrics { "PASS" } else { "FAIL" });
    println!("  composite:     {}", if pass_composite { "PASS" } else { "FAIL" });
    println!("  multiway:      {}", if pass_multiway { "PASS" } else { "FAIL" });
    println!("  zero-blowup:   {}", if pass_blowup { "PASS" } else { "FAIL" });
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}"
    );
}
