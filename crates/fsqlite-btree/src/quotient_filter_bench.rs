//! Micro-benchmarks for the `QuotientFilter`.
//!
//! These are `#[ignore]` by default so they don't run in the default test
//! cycle. Invoke with:
//!
//! ```text
//! cargo test -p fsqlite-btree --profile release-perf --lib quotient_filter_bench -- --ignored --nocapture
//! ```
//!
//! The benchmark models the "10K-row table, 10K DELETE-by-absent-rowid"
//! scenario from the acceptance-criteria prompt: we construct a filter
//! pre-populated with 10 000 present rowids and then measure how fast the
//! filter rejects 10 000 probe rowids that were never inserted, versus a
//! naive linear `Vec<i64>` scan and a sorted `binary_search` — which is
//! the shape of the existing MemTable fast path.

#![cfg(test)]

use std::time::Instant;

use crate::quotient_filter::{QuotientFilter, hash_rowid};

const ROWS: i64 = 10_000;

/// Reference: sorted `Vec<i64>` binary search (mirrors how `MemTable`
/// currently rejects absent rowids via `binary_search_by_key`).
fn bench_sorted_vec_absent_reject() -> (std::time::Duration, usize) {
    let sorted: Vec<i64> = (1..=ROWS).collect();
    let absent_probes: Vec<i64> = (100_001..=(100_000 + ROWS)).collect();
    let start = Instant::now();
    let mut rejected = 0usize;
    for probe in &absent_probes {
        if sorted.binary_search(probe).is_err() {
            rejected += 1;
        }
    }
    (start.elapsed(), rejected)
}

/// QF contains — the rejection path this PR introduces.
fn bench_qf_absent_reject() -> (std::time::Duration, usize) {
    let mut qf = QuotientFilter::with_capacity(ROWS as usize, 0.5, 14).unwrap();
    for r in 1..=ROWS {
        qf.insert(hash_rowid(r)).unwrap();
    }
    let start = Instant::now();
    let mut rejected = 0usize;
    for absent in 100_001..=(100_000 + ROWS) {
        if !qf.contains(hash_rowid(absent)) {
            rejected += 1;
        }
    }
    (start.elapsed(), rejected)
}

/// Naïve linear scan of a `Vec<i64>` — the worst-case baseline for how
/// much cost an unfiltered absent-key probe can incur.
fn bench_linear_scan_absent_reject() -> (std::time::Duration, usize) {
    let rows: Vec<i64> = (1..=ROWS).collect();
    let absent_probes: Vec<i64> = (100_001..=(100_000 + ROWS)).collect();
    let start = Instant::now();
    let mut rejected = 0usize;
    for probe in &absent_probes {
        if !rows.iter().any(|r| r == probe) {
            rejected += 1;
        }
    }
    (start.elapsed(), rejected)
}

#[test]
#[ignore = "benchmark — invoke with --ignored"]
fn bench_absent_key_reject_10k() {
    // Warm caches.
    let _ = bench_qf_absent_reject();
    let _ = bench_sorted_vec_absent_reject();

    // Measure 5 runs and take the minimum.
    let mut best_qf = std::time::Duration::MAX;
    let mut best_sorted = std::time::Duration::MAX;
    let mut best_linear = std::time::Duration::MAX;
    for _ in 0..5 {
        let (d, r) = bench_qf_absent_reject();
        assert_eq!(r, ROWS as usize, "QF should reject all absent rowids");
        if d < best_qf {
            best_qf = d;
        }
        let (d, r) = bench_sorted_vec_absent_reject();
        assert_eq!(r, ROWS as usize);
        if d < best_sorted {
            best_sorted = d;
        }
    }
    // Linear only once — it's slow.
    let (d, r) = bench_linear_scan_absent_reject();
    assert_eq!(r, ROWS as usize);
    best_linear = best_linear.min(d);

    eprintln!("\n=== Quotient Filter Absent-Key Reject Benchmark ===");
    eprintln!("rows               = {ROWS}");
    eprintln!("absent probes      = {ROWS}");
    eprintln!();
    #[allow(clippy::cast_precision_loss)]
    let qf_ns_per = best_qf.as_nanos() as f64 / ROWS as f64;
    #[allow(clippy::cast_precision_loss)]
    let sorted_ns_per = best_sorted.as_nanos() as f64 / ROWS as f64;
    #[allow(clippy::cast_precision_loss)]
    let linear_ns_per = best_linear.as_nanos() as f64 / ROWS as f64;
    eprintln!(
        "QF contains        : {:>10} total,  {:>7.1} ns/op",
        format!("{:?}", best_qf),
        qf_ns_per
    );
    eprintln!(
        "Sorted Vec bsearch : {:>10} total,  {:>7.1} ns/op",
        format!("{:?}", best_sorted),
        sorted_ns_per
    );
    eprintln!(
        "Linear Vec scan   : {:>10} total,  {:>7.1} ns/op",
        format!("{:?}", best_linear),
        linear_ns_per
    );
    eprintln!();
    if qf_ns_per < sorted_ns_per {
        let speedup = sorted_ns_per / qf_ns_per;
        eprintln!("QF is {:.2}x faster than sorted Vec binary_search", speedup);
    } else {
        let slowdown = qf_ns_per / sorted_ns_per;
        eprintln!(
            "QF is {:.2}x SLOWER than sorted Vec binary_search (this is fine — \
             the real win is avoiding the B-tree descent for pager-backed tables, \
             not the MemTable's already-cheap binary search)",
            slowdown
        );
    }
    let speedup_vs_linear = linear_ns_per / qf_ns_per;
    eprintln!(
        "QF is {:.2}x faster than linear Vec scan",
        speedup_vs_linear
    );
}
