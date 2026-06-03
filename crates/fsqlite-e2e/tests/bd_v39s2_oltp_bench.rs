//! bd-v39s2: Mixed-read-write OLTP bench unit tests.
//!
//! Validates the statistical helpers, result aggregation, and report
//! structure without requiring full benchmark runs.

use serde::{Deserialize, Serialize};

const BEAD_ID: &str = "bd-v39s2";

// ─── Mirrored types for testing (no pub API from bin) ───────────────────

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    #[allow(clippy::cast_precision_loss)]
    let idx = pct * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    let frac = idx - lo as f64;
    if lo == hi {
        sorted[lo]
    } else {
        sorted[hi].mul_add(frac, sorted[lo] * (1.0 - frac))
    }
}

fn jain_fairness(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 1.0;
    }
    let n = values.len() as f64;
    let sum: f64 = values.iter().sum();
    let sum_sq: f64 = values.iter().map(|v| v * v).sum();
    if sum_sq <= 0.0 {
        return 1.0;
    }
    let numerator = sum * sum;
    let denominator = n * sum_sq;
    numerator / denominator
}

fn compute_latency_stats(mut latencies_ns: Vec<u64>) -> (u64, f64, f64, f64, f64) {
    let count = latencies_ns.len() as u64;
    if latencies_ns.is_empty() {
        return (0, 0.0, 0.0, 0.0, 0.0);
    }
    latencies_ns.sort_unstable();
    #[allow(clippy::cast_precision_loss)]
    let as_us: Vec<f64> = latencies_ns.iter().map(|ns| *ns as f64 / 1_000.0).collect();
    let mean = as_us.iter().sum::<f64>() / as_us.len() as f64;
    (
        count,
        percentile(&as_us, 0.50),
        percentile(&as_us, 0.95),
        percentile(&as_us, 0.99),
        mean,
    )
}

// ─── Jain's fairness index ──────────────────────────────────────────────

#[test]
fn t1_jain_equal_rates_is_1() {
    let j = jain_fairness(&[1000.0, 1000.0, 1000.0, 1000.0]);
    assert!(
        (j - 1.0).abs() < 1e-10,
        "equal rates should give J=1.0, got {j}"
    );
}

#[test]
fn t2_jain_single_thread_is_1() {
    let j = jain_fairness(&[42.0]);
    assert!((j - 1.0).abs() < 1e-10);
}

#[test]
fn t3_jain_empty_is_1() {
    let j = jain_fairness(&[]);
    assert!((j - 1.0).abs() < 1e-10);
}

#[test]
fn t4_jain_unfair_less_than_1() {
    let j = jain_fairness(&[1000.0, 0.0]);
    assert!(j < 1.0, "unequal rates should give J<1.0, got {j}");
    assert!(j > 0.0);
}

#[test]
fn t5_jain_slightly_unequal() {
    let j = jain_fairness(&[100.0, 95.0, 105.0, 98.0]);
    assert!(j > 0.99, "slightly unequal should be >0.99, got {j}");
    assert!(j <= 1.0);
}

#[test]
fn t6_jain_known_value() {
    // For [1, 2, 3, 4]: J = 36^2 / (4 * (1+4+9+16)) = 100/120 = 0.8333...
    // Wait: sum=10, sum_sq=30, J = 100 / (4*30) = 100/120 = 5/6
    let j = jain_fairness(&[1.0, 2.0, 3.0, 4.0]);
    let expected = 100.0 / 120.0;
    assert!(
        (j - expected).abs() < 1e-10,
        "J([1,2,3,4]) should be {expected}, got {j}"
    );
}

// ─── Percentile computation ─────────────────────────────────────────────

#[test]
fn t7_percentile_sorted_basic() {
    let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    assert!((percentile(&data, 0.0) - 1.0).abs() < 1e-10);
    assert!((percentile(&data, 0.5) - 3.0).abs() < 1e-10);
    assert!((percentile(&data, 1.0) - 5.0).abs() < 1e-10);
}

#[test]
fn t8_percentile_single_value() {
    assert!((percentile(&[42.0], 0.95) - 42.0).abs() < 1e-10);
}

#[test]
fn t9_percentile_empty() {
    assert_eq!(percentile(&[], 0.5), 0.0);
}

#[test]
fn t10_percentile_interpolation() {
    let data = vec![10.0, 20.0];
    let p50 = percentile(&data, 0.5);
    assert!(
        (p50 - 15.0).abs() < 1e-10,
        "p50 of [10,20] should be 15, got {p50}"
    );
}

// ─── Latency stats ─────────────────────────────────────────────────────

#[test]
fn t11_latency_stats_empty() {
    let (count, p50, p95, p99, mean) = compute_latency_stats(vec![]);
    assert_eq!(count, 0);
    assert_eq!(p50, 0.0);
    assert_eq!(p95, 0.0);
    assert_eq!(p99, 0.0);
    assert_eq!(mean, 0.0);
}

#[test]
fn t12_latency_stats_single() {
    let (count, p50, _p95, _p99, mean) = compute_latency_stats(vec![10_000]);
    assert_eq!(count, 1);
    assert!((p50 - 10.0).abs() < 0.01, "10000ns = 10µs, got {p50}");
    assert!((mean - 10.0).abs() < 0.01);
}

#[test]
fn t13_latency_stats_ordered() {
    let data: Vec<u64> = (1..=100).map(|i| i * 1_000).collect();
    let (count, p50, p95, p99, _mean) = compute_latency_stats(data);
    assert_eq!(count, 100);
    assert!(p50 > 0.0);
    assert!(p95 >= p50, "p95 ({p95}) should be >= p50 ({p50})");
    assert!(p99 >= p95, "p99 ({p99}) should be >= p95 ({p95})");
}

// ─── Report JSON structure ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MiniReport {
    schema_version: String,
    bead_id: String,
    seed_rows: i64,
    ops_per_thread: usize,
    num_readers: usize,
    num_writers: usize,
    read_throughput_ratio: f64,
    write_throughput_ratio: f64,
}

#[test]
fn t14_report_json_round_trip() {
    let report = MiniReport {
        schema_version: "fsqlite-e2e.mt_oltp_bench_report.v1".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        seed_rows: 5000,
        ops_per_thread: 5000,
        num_readers: 4,
        num_writers: 2,
        read_throughput_ratio: 1.23,
        write_throughput_ratio: 0.87,
    };
    let json = serde_json::to_string_pretty(&report).unwrap();
    let parsed: MiniReport = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.bead_id, BEAD_ID);
    assert_eq!(parsed.num_readers, 4);
    assert_eq!(parsed.num_writers, 2);
    assert!((parsed.read_throughput_ratio - 1.23).abs() < 1e-10);
}

#[test]
fn t15_report_file_write_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("report.json");

    let report = MiniReport {
        schema_version: "fsqlite-e2e.mt_oltp_bench_report.v1".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        seed_rows: 1000,
        ops_per_thread: 1000,
        num_readers: 2,
        num_writers: 1,
        read_throughput_ratio: 1.5,
        write_throughput_ratio: 0.9,
    };

    let json = serde_json::to_string_pretty(&report).unwrap();
    std::fs::write(&path, &json).unwrap();
    let read_back = std::fs::read_to_string(&path).unwrap();
    let parsed: MiniReport = serde_json::from_str(&read_back).unwrap();
    assert_eq!(parsed.bead_id, BEAD_ID);
    assert_eq!(parsed.seed_rows, 1000);
}

// ─── Edge cases ─────────────────────────────────────────────────────────

#[test]
fn t16_jain_all_zero_rates() {
    let j = jain_fairness(&[0.0, 0.0, 0.0]);
    assert!(
        (j - 1.0).abs() < 1e-10,
        "all-zero rates: J should be 1.0 (vacuous), got {j}"
    );
}

#[test]
fn t17_jain_two_equal() {
    let j = jain_fairness(&[500.0, 500.0]);
    assert!((j - 1.0).abs() < 1e-10);
}

#[test]
fn t18_latency_stats_large_values() {
    let data = vec![1_000_000_000u64; 10]; // 1 second in ns
    let (count, p50, _p95, _p99, mean) = compute_latency_stats(data);
    assert_eq!(count, 10);
    assert!((p50 - 1_000_000.0).abs() < 0.1, "1s = 1000000µs, got {p50}");
    assert!((mean - 1_000_000.0).abs() < 0.1);
}
