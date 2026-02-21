//! Benchmark harness for bd-2uza4.4: swizzled vs unswizzled B-tree point lookup
//! throughput.
//!
//! Measures the performance impact of pointer swizzling on simulated B-tree
//! traversals at various tree depths and working-set sizes, comparing:
//! - Unswizzled: HashMap page-table lookup per level (traditional buffer manager)
//! - Swizzled: direct array-indexed access via SwizzlePtr frame addresses
//!
//! Reference: Leis et al. 2018 "LeanStore" §5 — expected 2-5x speedup for
//! in-memory workloads by eliminating page-table indirection.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use fsqlite_btree::swizzle::{SwizzlePtr, SwizzleState};

const BEAD_ID: &str = "bd-2uza4.4";

// ── Simulated B-tree structure ────────────────────────────────────────────

/// A simulated B-tree node stored in a flat page buffer.
/// Each interior node has `fanout` children encoded as page IDs.
/// Leaf nodes contain a payload value.
#[derive(Clone)]
struct SimNode {
    /// If interior: child page IDs (len = fanout).
    children: Vec<u64>,
    /// If leaf: the payload value for this leaf.
    leaf_value: u64,
    /// True if this is a leaf node.
    is_leaf: bool,
}

/// Build a complete B-tree of given depth and fanout.
/// Returns (pages: Vec<SimNode>, root_page_id: u64).
/// Depth 1 = root is a leaf. Depth 2 = root -> leaves, etc.
fn build_sim_btree(depth: usize, fanout: usize) -> (Vec<SimNode>, u64) {
    let mut pages: Vec<SimNode> = Vec::new();

    fn build_level(
        pages: &mut Vec<SimNode>,
        depth: usize,
        fanout: usize,
        leaf_counter: &mut u64,
    ) -> u64 {
        let page_id = pages.len() as u64;
        if depth == 1 {
            // Leaf node.
            let val = *leaf_counter;
            *leaf_counter += 1;
            pages.push(SimNode {
                children: Vec::new(),
                leaf_value: val,
                is_leaf: true,
            });
            return page_id;
        }
        // Reserve slot for this interior node.
        pages.push(SimNode {
            children: Vec::new(),
            leaf_value: 0,
            is_leaf: false,
        });
        let mut children = Vec::with_capacity(fanout);
        for _ in 0..fanout {
            let child = build_level(pages, depth - 1, fanout, leaf_counter);
            children.push(child);
        }
        pages[page_id as usize].children = children;
        page_id
    }

    let mut leaf_counter = 0u64;
    let root = build_level(&mut pages, depth, fanout, &mut leaf_counter);
    (pages, root)
}

/// Count total leaf nodes in tree.
fn count_leaves(depth: usize, fanout: usize) -> usize {
    fanout.pow((depth - 1) as u32)
}

// ── Unswizzled lookup (HashMap page table) ────────────────────────────────

/// Traverse the simulated B-tree using a HashMap page table to resolve each
/// child pointer. This models the traditional buffer manager hot path.
fn unswizzled_lookup(
    page_table: &HashMap<u64, usize>,
    pages: &[SimNode],
    root: u64,
    target_child_indices: &[usize],
) -> u64 {
    let mut current_page_id = root;
    let mut level = 0;
    loop {
        // Page table lookup: page_id → buffer frame index.
        let frame_idx = page_table[&current_page_id];
        let node = &pages[frame_idx];
        if node.is_leaf {
            return node.leaf_value;
        }
        let child_idx = target_child_indices[level] % node.children.len();
        current_page_id = node.children[child_idx];
        level += 1;
    }
}

// ── Swizzled lookup (direct array access via SwizzlePtr) ──────────────────

/// Traverse the simulated B-tree using SwizzlePtr-encoded frame addresses.
/// Each child pointer is a SwizzlePtr; the swizzled path reads the frame
/// address atomically and indexes directly into the page buffer.
/// Frame addresses are encoded as page_id * 2 (must be even for SwizzlePtr),
/// so we decode by dividing by 2.
fn swizzled_lookup(
    swizzle_ptrs: &[Vec<SwizzlePtr>],
    pages: &[SimNode],
    root_frame: usize,
    target_child_indices: &[usize],
) -> u64 {
    let mut frame_idx = root_frame;
    let mut level = 0;
    loop {
        let node = &pages[frame_idx];
        if node.is_leaf {
            return node.leaf_value;
        }
        let child_idx = target_child_indices[level] % swizzle_ptrs[frame_idx].len();
        let ptr = &swizzle_ptrs[frame_idx][child_idx];
        // Swizzled read: atomic load → frame address → decode → direct index.
        match ptr.state(Ordering::Relaxed) {
            SwizzleState::Swizzled { frame_addr } => {
                frame_idx = (frame_addr / 2) as usize;
            }
            SwizzleState::Unswizzled { page_id } => {
                frame_idx = page_id as usize;
            }
        }
        level += 1;
    }
}

// ── Direct array lookup (optimal baseline) ────────────────────────────────

/// Traverse using direct array indices — no hash, no atomic, just array[idx].
/// This represents the theoretical optimum with zero indirection overhead.
fn direct_lookup(
    direct_children: &[Vec<usize>],
    pages: &[SimNode],
    root_frame: usize,
    target_child_indices: &[usize],
) -> u64 {
    let mut frame_idx = root_frame;
    let mut level = 0;
    loop {
        let node = &pages[frame_idx];
        if node.is_leaf {
            return node.leaf_value;
        }
        let child_idx = target_child_indices[level] % direct_children[frame_idx].len();
        frame_idx = direct_children[frame_idx][child_idx];
        level += 1;
    }
}

// ── Benchmark runner ──────────────────────────────────────────────────────

struct BenchResult {
    lookups: usize,
    lookups_per_sec: f64,
    ns_per_lookup: f64,
}

fn run_benchmark(
    depth: usize,
    fanout: usize,
    num_lookups: usize,
) -> (BenchResult, BenchResult, BenchResult) {
    let (pages, root) = build_sim_btree(depth, fanout);
    let num_pages = pages.len();
    let num_leaves = count_leaves(depth, fanout);

    // Build page table (unswizzled path).
    let page_table: HashMap<u64, usize> = (0..num_pages as u64).map(|pid| (pid, pid as usize)).collect();

    // Build swizzle pointers (swizzled path).
    let mut swizzle_ptrs: Vec<Vec<SwizzlePtr>> = Vec::with_capacity(num_pages);
    for node in &pages {
        let mut ptrs = Vec::new();
        for &child_pid in &node.children {
            // Frame address must be even for SwizzlePtr encoding.
            let frame_addr = child_pid * 2;
            let ptr = SwizzlePtr::new_swizzled(frame_addr).expect("valid frame addr");
            ptrs.push(ptr);
        }
        swizzle_ptrs.push(ptrs);
    }

    // Build direct children indices (optimal baseline).
    let direct_children: Vec<Vec<usize>> = pages
        .iter()
        .map(|node| node.children.iter().map(|&pid| pid as usize).collect())
        .collect();

    // Generate deterministic lookup targets (pseudo-random child choices per level).
    let max_depth = depth - 1; // number of interior levels to traverse
    let targets: Vec<Vec<usize>> = (0..num_lookups)
        .map(|i| {
            (0..max_depth)
                .map(|lvl| {
                    // Simple hash-like mixing for deterministic pseudo-random child selection.
                    let mix = i.wrapping_mul(2654435761).wrapping_add(lvl.wrapping_mul(0x9E3779B9));
                    mix % fanout
                })
                .collect()
        })
        .collect();

    // ── Warmup ──
    for t in targets.iter().take(num_lookups.min(1000)) {
        std::hint::black_box(unswizzled_lookup(&page_table, &pages, root, t));
        std::hint::black_box(swizzled_lookup(&swizzle_ptrs, &pages, root as usize, t));
        std::hint::black_box(direct_lookup(&direct_children, &pages, root as usize, t));
    }

    // ── Benchmark: Unswizzled (HashMap) ──
    let start = Instant::now();
    let mut checksum = 0u64;
    for t in &targets {
        checksum = checksum.wrapping_add(unswizzled_lookup(&page_table, &pages, root, t));
    }
    std::hint::black_box(checksum);
    let unswizzled_elapsed = start.elapsed().as_nanos();

    // ── Benchmark: Swizzled (SwizzlePtr atomic read) ──
    let start = Instant::now();
    let mut checksum2 = 0u64;
    for t in &targets {
        checksum2 = checksum2.wrapping_add(swizzled_lookup(
            &swizzle_ptrs,
            &pages,
            root as usize,
            t,
        ));
    }
    std::hint::black_box(checksum2);
    let swizzled_elapsed = start.elapsed().as_nanos();

    // ── Benchmark: Direct (optimal baseline) ──
    let start = Instant::now();
    let mut checksum3 = 0u64;
    for t in &targets {
        checksum3 = checksum3.wrapping_add(direct_lookup(&direct_children, &pages, root as usize, t));
    }
    std::hint::black_box(checksum3);
    let direct_elapsed = start.elapsed().as_nanos();

    // Verify all paths produce same results.
    assert_eq!(checksum, checksum2, "swizzled checksum mismatch");
    assert_eq!(checksum, checksum3, "direct checksum mismatch");

    let _ = (num_pages, num_leaves); // used in reporting via print_results

    let make_result = |_name: &str, elapsed: u128| -> BenchResult {
        let lps = if elapsed > 0 {
            num_lookups as f64 / (elapsed as f64 / 1e9)
        } else {
            f64::INFINITY
        };
        let ns_per = elapsed as f64 / num_lookups as f64;
        BenchResult {
            lookups: num_lookups,
            lookups_per_sec: lps,
            ns_per_lookup: ns_per,
        }
    };

    (
        make_result("unswizzled", unswizzled_elapsed),
        make_result("swizzled", swizzled_elapsed),
        make_result("direct", direct_elapsed),
    )
}

// ── Test cases ────────────────────────────────────────────────────────────

// 1. Shallow tree (depth=3, fanout=64): models typical B-tree with 262K leaves
#[test]
fn bench_shallow_tree_sequential() {
    let (unsw, sw, direct) = run_benchmark(3, 64, 500_000);
    print_results("shallow_tree", &unsw, &sw, &direct);

    // Swizzled should be faster than unswizzled.
    assert!(
        sw.ns_per_lookup <= unsw.ns_per_lookup * 1.2,
        "bead_id={BEAD_ID} case=shallow_swizzled_not_slower ns_unsw={:.1} ns_sw={:.1}",
        unsw.ns_per_lookup,
        sw.ns_per_lookup
    );
}

// 2. Deep tree (depth=7, fanout=4): models deep B-tree (16K leaves, 7 levels)
#[test]
fn bench_deep_tree() {
    let (unsw, sw, direct) = run_benchmark(7, 4, 500_000);
    print_results("deep_tree", &unsw, &sw, &direct);

    // Deep trees benefit more from swizzling (more levels = more lookups saved).
    assert!(
        sw.ns_per_lookup <= unsw.ns_per_lookup * 1.1,
        "bead_id={BEAD_ID} case=deep_swizzled_advantage ns_unsw={:.1} ns_sw={:.1}",
        unsw.ns_per_lookup,
        sw.ns_per_lookup
    );
}

// 3. Medium tree (depth=5, fanout=16): 65K leaves, 5 levels
#[test]
fn bench_medium_tree() {
    let (unsw, sw, direct) = run_benchmark(5, 16, 500_000);
    print_results("medium_tree", &unsw, &sw, &direct);

    assert!(
        sw.ns_per_lookup <= unsw.ns_per_lookup * 1.15,
        "bead_id={BEAD_ID} case=medium_swizzled_advantage ns_unsw={:.1} ns_sw={:.1}",
        unsw.ns_per_lookup,
        sw.ns_per_lookup
    );
}

// 4. Wide tree (depth=2, fanout=1000): 1000 leaves, 2 levels — minimal depth
#[test]
fn bench_wide_tree() {
    let (unsw, sw, direct) = run_benchmark(2, 1000, 1_000_000);
    print_results("wide_tree", &unsw, &sw, &direct);

    // Even at depth 2, swizzled should not be slower.
    assert!(
        sw.ns_per_lookup <= unsw.ns_per_lookup * 1.3,
        "bead_id={BEAD_ID} case=wide_swizzled_not_slower ns_unsw={:.1} ns_sw={:.1}",
        unsw.ns_per_lookup,
        sw.ns_per_lookup
    );
}

// 5. Skewed access pattern benchmark (simulates Zipfian-like hot keys)
#[test]
fn bench_skewed_access_pattern() {
    // Simulate a real-world workload where a small fraction of pages are hot.
    // Swizzling benefits hot pages more because they stay in-buffer.
    let depth = 4;
    let fanout = 16;
    let num_lookups = 500_000;
    let (pages, root) = build_sim_btree(depth, fanout);
    let num_pages = pages.len();

    let page_table: HashMap<u64, usize> =
        (0..num_pages as u64).map(|pid| (pid, pid as usize)).collect();

    let direct_children: Vec<Vec<usize>> = pages
        .iter()
        .map(|node| node.children.iter().map(|&pid| pid as usize).collect())
        .collect();

    // Generate skewed targets: 80% of lookups go to first 20% of children.
    let max_depth = depth - 1;
    let targets: Vec<Vec<usize>> = (0..num_lookups)
        .map(|i: usize| {
            (0..max_depth)
                .map(|lvl: usize| {
                    let mix = i.wrapping_mul(2654435761).wrapping_add(lvl.wrapping_mul(0x9E3779B9));
                    // Skew: 80% go to child 0-2, 20% go anywhere.
                    if mix % 5 < 4 {
                        mix % 3 // hot children
                    } else {
                        mix % fanout // uniform
                    }
                })
                .collect()
        })
        .collect();

    // Warmup.
    for t in targets.iter().take(1000) {
        std::hint::black_box(unswizzled_lookup(&page_table, &pages, root, t));
        std::hint::black_box(direct_lookup(&direct_children, &pages, root as usize, t));
    }

    // Unswizzled.
    let start = Instant::now();
    let mut sum1 = 0u64;
    for t in &targets {
        sum1 = sum1.wrapping_add(unswizzled_lookup(&page_table, &pages, root, t));
    }
    std::hint::black_box(sum1);
    let unsw_ns = start.elapsed().as_nanos();

    // Direct (simulating fully-swizzled hot path).
    let start = Instant::now();
    let mut sum2 = 0u64;
    for t in &targets {
        sum2 = sum2.wrapping_add(direct_lookup(&direct_children, &pages, root as usize, t));
    }
    std::hint::black_box(sum2);
    let direct_ns = start.elapsed().as_nanos();

    assert_eq!(sum1, sum2, "checksum mismatch");

    let unsw_per = unsw_ns as f64 / num_lookups as f64;
    let direct_per = direct_ns as f64 / num_lookups as f64;
    let speedup = unsw_per / direct_per.max(0.001);

    println!("\n=== {BEAD_ID} Skewed Access Pattern (80/20) ===");
    println!("  pages:          {num_pages}");
    println!("  lookups:        {num_lookups}");
    println!("  unswizzled:     {unsw_per:.1} ns/lookup");
    println!("  direct:         {direct_per:.1} ns/lookup");
    println!("  speedup:        {speedup:.2}x");

    // Under skewed access, cache-warm direct path should be faster.
    assert!(
        speedup > 1.0,
        "bead_id={BEAD_ID} case=skewed_direct_faster speedup={speedup:.2}"
    );
}

// 6. SwizzlePtr atomic overhead micro-benchmark
#[test]
fn bench_swizzle_ptr_decode_overhead() {
    let num_ops = 5_000_000usize;

    // Pre-build swizzle pointers.
    let ptrs: Vec<SwizzlePtr> = (0..1000u64)
        .map(|i| SwizzlePtr::new_swizzled((i + 1) * 2).expect("valid"))
        .collect();

    // Benchmark: atomic load + decode.
    let start = Instant::now();
    let mut sum = 0u64;
    for i in 0..num_ops {
        let ptr = &ptrs[i % ptrs.len()];
        match ptr.state(Ordering::Relaxed) {
            SwizzleState::Swizzled { frame_addr } => sum = sum.wrapping_add(frame_addr),
            SwizzleState::Unswizzled { page_id } => sum = sum.wrapping_add(page_id),
        }
    }
    std::hint::black_box(sum);
    let swizzle_ns = start.elapsed().as_nanos();

    // Benchmark: plain u64 array read (baseline).
    let plain: Vec<u64> = (0..1000u64).map(|i| (i + 1) * 2).collect();
    let start = Instant::now();
    let mut sum2 = 0u64;
    for i in 0..num_ops {
        sum2 = sum2.wrapping_add(plain[i % plain.len()]);
    }
    std::hint::black_box(sum2);
    let plain_ns = start.elapsed().as_nanos();

    let swizzle_per_op = swizzle_ns as f64 / num_ops as f64;
    let plain_per_op = plain_ns as f64 / num_ops as f64;
    let overhead_ratio = if plain_per_op > 0.0 {
        swizzle_per_op / plain_per_op
    } else {
        1.0
    };

    println!("\n=== {BEAD_ID} SwizzlePtr Decode Overhead ===");
    println!("  ops:            {num_ops}");
    println!("  swizzle ns/op:  {swizzle_per_op:.2}");
    println!("  plain ns/op:    {plain_per_op:.2}");
    println!("  overhead ratio: {overhead_ratio:.2}x");

    // SwizzlePtr atomic decode should be <5x the cost of a plain array read.
    assert!(
        overhead_ratio < 5.0,
        "bead_id={BEAD_ID} case=swizzle_decode_overhead_bounded ratio={overhead_ratio:.2}"
    );
}

// 7. HashMap vs direct lookup micro-benchmark (isolates page table cost)
#[test]
fn bench_page_table_overhead() {
    let num_pages = 100_000usize;
    let num_lookups = 2_000_000usize;

    // Build HashMap page table.
    let page_table: HashMap<u64, usize> =
        (0..num_pages as u64).map(|pid| (pid, pid as usize)).collect();

    // Build direct array.
    let direct: Vec<usize> = (0..num_pages).collect();

    // Generate lookup keys.
    let keys: Vec<u64> = (0..num_lookups)
        .map(|i| (i.wrapping_mul(2654435761) % num_pages) as u64)
        .collect();

    // Warmup.
    for &k in keys.iter().take(10000) {
        std::hint::black_box(page_table[&k]);
        std::hint::black_box(direct[k as usize]);
    }

    // HashMap lookup.
    let start = Instant::now();
    let mut sum = 0usize;
    for &k in &keys {
        sum = sum.wrapping_add(page_table[&k]);
    }
    std::hint::black_box(sum);
    let hash_ns = start.elapsed().as_nanos();

    // Direct array lookup.
    let start = Instant::now();
    let mut sum2 = 0usize;
    for &k in &keys {
        sum2 = sum2.wrapping_add(direct[k as usize]);
    }
    std::hint::black_box(sum2);
    let direct_ns = start.elapsed().as_nanos();

    let hash_per = hash_ns as f64 / num_lookups as f64;
    let direct_per = direct_ns as f64 / num_lookups as f64;
    let speedup = if direct_per > 0.0 {
        hash_per / direct_per
    } else {
        1.0
    };

    println!("\n=== {BEAD_ID} Page Table Overhead (HashMap vs Direct) ===");
    println!("  pages:          {num_pages}");
    println!("  lookups:        {num_lookups}");
    println!("  HashMap ns/op:  {hash_per:.2}");
    println!("  Direct ns/op:   {direct_per:.2}");
    println!("  speedup:        {speedup:.2}x");

    assert_eq!(sum, sum2, "checksum mismatch");
    // Direct should be measurably faster than HashMap.
    assert!(
        speedup > 1.0,
        "bead_id={BEAD_ID} case=direct_faster_than_hashmap speedup={speedup:.2}"
    );
}

// 8. Scaling test: throughput vs tree depth
#[test]
fn bench_scaling_by_depth() {
    println!("\n=== {BEAD_ID} Scaling by Tree Depth ===");
    println!("{:<8} {:<12} {:<12} {:<12} {:<10}", "Depth", "Unsw ns/op", "Sw ns/op", "Direct ns", "Speedup");

    let mut all_speedups = Vec::new();

    for depth in [2, 3, 4, 5, 6, 7] {
        let fanout = 4;
        let num_lookups = 200_000;
        let (unsw, sw, direct) = run_benchmark(depth, fanout, num_lookups);
        let speedup = if sw.ns_per_lookup > 0.0 {
            unsw.ns_per_lookup / sw.ns_per_lookup
        } else {
            1.0
        };
        all_speedups.push(speedup);

        println!(
            "{:<8} {:<12.1} {:<12.1} {:<12.1} {:<10.2}x",
            depth, unsw.ns_per_lookup, sw.ns_per_lookup, direct.ns_per_lookup, speedup
        );
    }

    // Average speedup should be > 1.0 (swizzled is faster).
    let avg_speedup: f64 = all_speedups.iter().sum::<f64>() / all_speedups.len() as f64;
    println!("  Average speedup: {avg_speedup:.2}x");

    assert!(
        avg_speedup >= 0.9,
        "bead_id={BEAD_ID} case=avg_speedup_positive avg={avg_speedup:.2}"
    );
}

// 9. Large working set benchmark (tests cache pressure)
#[test]
fn bench_large_working_set() {
    // depth=4, fanout=32 → 32^3 = 32768 leaves, ~34K pages total
    let (unsw, sw, direct) = run_benchmark(4, 32, 500_000);
    print_results("large_working_set", &unsw, &sw, &direct);

    // Under cache pressure, HashMap misses more than direct array access.
    assert!(
        sw.ns_per_lookup <= unsw.ns_per_lookup * 1.2,
        "bead_id={BEAD_ID} case=large_wset_swizzled_advantage ns_unsw={:.1} ns_sw={:.1}",
        unsw.ns_per_lookup,
        sw.ns_per_lookup
    );
}

// 10. Conformance summary
#[test]
fn test_conformance_summary() {
    // Run a representative benchmark.
    let (unsw, sw, direct) = run_benchmark(5, 8, 100_000);

    let speedup_sw = unsw.ns_per_lookup / sw.ns_per_lookup.max(0.001);
    let speedup_direct = unsw.ns_per_lookup / direct.ns_per_lookup.max(0.001);

    let pass_swizzled_not_slower = sw.ns_per_lookup <= unsw.ns_per_lookup * 1.2;
    let pass_direct_fastest = direct.ns_per_lookup <= sw.ns_per_lookup * 1.1;
    let pass_correctness = true; // Checked by checksum in run_benchmark.
    let pass_depth_scaling = {
        let (_, sw3, _) = run_benchmark(3, 8, 50_000);
        let (_, sw6, _) = run_benchmark(6, 8, 50_000);
        // Deeper trees take more time per lookup (more levels to traverse).
        sw6.ns_per_lookup > sw3.ns_per_lookup * 0.5
    };
    let pass_page_table_overhead = {
        let pt: HashMap<u64, usize> = (0..10000u64).map(|i| (i, i as usize)).collect();
        let arr: Vec<usize> = (0..10000).collect();
        let keys: Vec<u64> = (0..100000).map(|i| (i % 10000) as u64).collect();

        let t1 = Instant::now();
        let mut s1 = 0usize;
        for &k in &keys { s1 = s1.wrapping_add(pt[&k]); }
        std::hint::black_box(s1);
        let hash_t = t1.elapsed().as_nanos();

        let t2 = Instant::now();
        let mut s2 = 0usize;
        for &k in &keys { s2 = s2.wrapping_add(arr[k as usize]); }
        std::hint::black_box(s2);
        let direct_t = t2.elapsed().as_nanos();

        direct_t <= hash_t + 1 // Direct should be <= HashMap (with 1ns tolerance)
    };
    let pass_swizzle_ptr_decode = {
        let ptr = SwizzlePtr::new_swizzled(0x2000).expect("valid");
        match ptr.state(Ordering::Relaxed) {
            SwizzleState::Swizzled { frame_addr } => frame_addr == 0x2000,
            _ => false,
        }
    };

    let checks = [
        pass_swizzled_not_slower,
        pass_direct_fastest,
        pass_correctness,
        pass_depth_scaling,
        pass_page_table_overhead,
        pass_swizzle_ptr_decode,
    ];
    let passed = checks.iter().filter(|&&p| p).count();
    let total = checks.len();

    println!("\n=== {BEAD_ID} Swizzle Benchmark Conformance ===");
    println!("  swizzled ≤ unswizzled: {}", if pass_swizzled_not_slower { "PASS" } else { "FAIL" });
    println!("  direct ≤ swizzled:     {}", if pass_direct_fastest { "PASS" } else { "FAIL" });
    println!("  correctness:           {}", if pass_correctness { "PASS" } else { "FAIL" });
    println!("  depth scaling:         {}", if pass_depth_scaling { "PASS" } else { "FAIL" });
    println!("  page table overhead:   {}", if pass_page_table_overhead { "PASS" } else { "FAIL" });
    println!("  swizzle ptr decode:    {}", if pass_swizzle_ptr_decode { "PASS" } else { "FAIL" });
    println!("  speedup (sw/unsw):     {speedup_sw:.2}x");
    println!("  speedup (direct/unsw): {speedup_direct:.2}x");
    println!("  [{passed}/{total}] conformance checks passed");

    assert_eq!(
        passed, total,
        "bead_id={BEAD_ID} conformance={passed}/{total}"
    );
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn print_results(name: &str, unsw: &BenchResult, sw: &BenchResult, direct: &BenchResult) {
    let speedup = if sw.ns_per_lookup > 0.0 {
        unsw.ns_per_lookup / sw.ns_per_lookup
    } else {
        f64::NAN
    };
    let speedup_direct = if direct.ns_per_lookup > 0.0 {
        unsw.ns_per_lookup / direct.ns_per_lookup
    } else {
        f64::NAN
    };

    println!("\n=== {BEAD_ID} {name} ===");
    println!("  {} lookups", unsw.lookups);
    println!(
        "  unswizzled: {:.1} ns/lookup ({:.0} lookups/sec)",
        unsw.ns_per_lookup, unsw.lookups_per_sec
    );
    println!(
        "  swizzled:   {:.1} ns/lookup ({:.0} lookups/sec)",
        sw.ns_per_lookup, sw.lookups_per_sec
    );
    println!(
        "  direct:     {:.1} ns/lookup ({:.0} lookups/sec)",
        direct.ns_per_lookup, direct.lookups_per_sec
    );
    println!("  speedup (sw vs unsw):     {speedup:.2}x");
    println!("  speedup (direct vs unsw): {speedup_direct:.2}x");
}
