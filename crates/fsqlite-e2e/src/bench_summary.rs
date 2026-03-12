//! Benchmark summary types and statistics aggregation.
//!
//! This module provides the data structures for producing a stable JSON
//! summary from repeated benchmark runs.  The [`BenchSummary`] is the
//! top-level report emitted by the `bench` subcommand.

use serde::{Deserialize, Serialize};

use crate::methodology::{EnvironmentMeta, MethodologyMeta, WARMUP_ITERATIONS};
use crate::report::{EngineRunReport, LatencySummary};

/// Top-level benchmark summary produced by `realdb-e2e bench`.
///
/// Contains methodology metadata, environment snapshot, and per-case
/// aggregated statistics across all measurement iterations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchSummary {
    pub schema_version: String,
    pub methodology: MethodologyMeta,
    pub environment: EnvironmentMeta,
    pub fixture_id: String,
    pub workload: String,
    pub cases: Vec<CaseSummary>,
}

/// Schema version for the bench summary JSON format.
pub const BENCH_SUMMARY_SCHEMA_V1: &str = "fsqlite-e2e.bench_summary.v1";

impl BenchSummary {
    /// Create a new summary with the given fixture and workload, capturing
    /// the current methodology and environment.
    #[must_use]
    pub fn new(fixture_id: &str, workload: &str, cargo_profile: &str) -> Self {
        Self {
            schema_version: BENCH_SUMMARY_SCHEMA_V1.to_owned(),
            methodology: MethodologyMeta::current(),
            environment: EnvironmentMeta::capture(cargo_profile),
            fixture_id: fixture_id.to_owned(),
            workload: workload.to_owned(),
            cases: Vec::new(),
        }
    }

    /// Serialize to pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Serialize to compact JSON (single line).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Aggregated statistics for one (engine, concurrency) combination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseSummary {
    pub engine: String,
    pub concurrency: u16,
    pub total_iterations: usize,
    pub warmup_iterations: u32,
    pub measurement_iterations: usize,
    pub throughput: ThroughputSummary,
    pub latency: LatencySummary,
    pub retries: RetryAbortSummary,
}

/// Throughput statistics aggregated from measurement iterations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputSummary {
    /// Median ops/sec across measurement iterations.
    pub median_ops_per_sec: f64,
    /// 95th percentile ops/sec.
    pub p95_ops_per_sec: f64,
    /// Median wall-clock time in milliseconds.
    pub median_wall_time_ms: u64,
    /// 95th percentile wall-clock time in milliseconds.
    pub p95_wall_time_ms: u64,
}

/// Retry/abort statistics aggregated from measurement iterations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryAbortSummary {
    pub median_retries: u64,
    pub median_aborts: u64,
    pub total_retries: u64,
    pub total_aborts: u64,
}

/// Accumulates raw data from repeated benchmark iterations and computes
/// summary statistics.
///
/// Feed it [`EngineRunReport`] values via [`RunAccumulator::record`], then
/// call [`RunAccumulator::summarize`] to produce a [`CaseSummary`].
#[derive(Debug, Clone)]
pub struct RunAccumulator {
    engine: String,
    concurrency: u16,
    wall_time_ms: Vec<u64>,
    ops_per_sec: Vec<f64>,
    latency_ms: Vec<Option<LatencySummary>>,
    retries: Vec<u64>,
    aborts: Vec<u64>,
}

impl RunAccumulator {
    /// Create a new accumulator for the given engine and concurrency level.
    #[must_use]
    pub fn new(engine: &str, concurrency: u16) -> Self {
        Self {
            engine: engine.to_owned(),
            concurrency,
            wall_time_ms: Vec::new(),
            ops_per_sec: Vec::new(),
            latency_ms: Vec::new(),
            retries: Vec::new(),
            aborts: Vec::new(),
        }
    }

    /// Record one iteration's results.
    pub fn record(&mut self, report: &EngineRunReport) {
        self.wall_time_ms.push(report.wall_time_ms);
        self.ops_per_sec.push(report.ops_per_sec);
        self.latency_ms.push(report.latency_ms.clone());
        self.retries.push(report.retries);
        self.aborts.push(report.aborts);
    }

    /// Returns the number of iterations recorded so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.wall_time_ms.len()
    }

    /// Returns true if no iterations have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.wall_time_ms.is_empty()
    }

    /// Compute summary statistics, discarding the first `warmup` iterations.
    ///
    /// If fewer than `warmup` iterations were recorded, all iterations are
    /// treated as warmup and the measurement set is empty (statistics default
    /// to zero).
    #[must_use]
    pub fn summarize(&self) -> CaseSummary {
        let warmup = WARMUP_ITERATIONS as usize;
        let total = self.wall_time_ms.len();
        let measurement_start = warmup.min(total);

        let m_wall = &self.wall_time_ms[measurement_start..];
        let m_ops = &self.ops_per_sec[measurement_start..];
        let m_latency = &self.latency_ms[measurement_start..];
        let m_retries = &self.retries[measurement_start..];
        let m_aborts = &self.aborts[measurement_start..];

        CaseSummary {
            engine: self.engine.clone(),
            concurrency: self.concurrency,
            total_iterations: total,
            warmup_iterations: WARMUP_ITERATIONS,
            measurement_iterations: m_wall.len(),
            throughput: ThroughputSummary {
                median_ops_per_sec: percentile_f64(m_ops, 50),
                p95_ops_per_sec: percentile_f64(m_ops, 95),
                median_wall_time_ms: percentile_u64(m_wall, 50),
                p95_wall_time_ms: percentile_u64(m_wall, 95),
            },
            latency: summarize_latency(m_latency),
            retries: RetryAbortSummary {
                median_retries: percentile_u64(m_retries, 50),
                median_aborts: percentile_u64(m_aborts, 50),
                total_retries: m_retries.iter().sum(),
                total_aborts: m_aborts.iter().sum(),
            },
        }
    }
}

/// Compute the `pct`-th percentile of a `u64` slice using nearest-rank.
#[must_use]
pub fn percentile_u64(data: &[u64], pct: u32) -> u64 {
    if data.is_empty() {
        return 0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_unstable();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Compute the `pct`-th percentile of an `f64` slice using nearest-rank.
#[must_use]
pub fn percentile_f64(data: &[f64], pct: u32) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_by(f64::total_cmp);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn summarize_latency(latencies: &[Option<LatencySummary>]) -> LatencySummary {
    let collected = latencies
        .iter()
        .filter_map(Option::as_ref)
        .collect::<Vec<_>>();
    if collected.is_empty() {
        return zero_latency_summary();
    }

    let p50 = collected
        .iter()
        .map(|latency| latency.p50)
        .collect::<Vec<_>>();
    let p95 = collected
        .iter()
        .map(|latency| latency.p95)
        .collect::<Vec<_>>();
    let p99 = collected
        .iter()
        .map(|latency| latency.p99)
        .collect::<Vec<_>>();

    LatencySummary {
        p50: percentile_f64(&p50, 50),
        p95: percentile_f64(&p95, 95),
        p99: percentile_f64(&p99, 99),
    }
}

fn zero_latency_summary() -> LatencySummary {
    LatencySummary {
        p50: 0.0,
        p95: 0.0,
        p99: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::CorrectnessReport;

    fn dummy_report(
        wall_ms: u64,
        ops_sec: f64,
        latency_ms: Option<LatencySummary>,
        retries: u64,
        aborts: u64,
    ) -> EngineRunReport {
        EngineRunReport {
            wall_time_ms: wall_ms,
            ops_total: 100,
            ops_per_sec: ops_sec,
            retries,
            aborts,
            correctness: CorrectnessReport {
                raw_sha256_match: None,
                dump_match: None,
                canonical_sha256_match: None,
                integrity_check_ok: Some(true),
                raw_sha256: None,
                canonical_sha256: None,
                logical_sha256: None,
                notes: None,
            },
            latency_ms,
            error: None,
            first_failure_diagnostic: None,
            storage_wiring: None,
            hot_path_profile: None,
        }
    }

    #[test]
    fn percentile_u64_basic() {
        assert_eq!(percentile_u64(&[], 50), 0);
        assert_eq!(percentile_u64(&[42], 50), 42);
        assert_eq!(percentile_u64(&[10, 20, 30, 40, 50], 50), 30);
        assert_eq!(percentile_u64(&[10, 20, 30, 40, 50], 95), 50);
    }

    #[test]
    fn percentile_f64_basic() {
        let eps = f64::EPSILON;
        assert!((percentile_f64(&[], 50) - 0.0).abs() < eps);
        assert!((percentile_f64(&[42.0], 50) - 42.0).abs() < eps);
        assert!((percentile_f64(&[1.0, 2.0, 3.0, 4.0, 5.0], 50) - 3.0).abs() < eps);
    }

    #[test]
    fn accumulator_discards_warmup() {
        let mut acc = RunAccumulator::new("test-engine", 4);

        // Record 3 warmup + 5 measurement iterations
        // Warmup: slow (1000ms)
        for _ in 0..3 {
            acc.record(&dummy_report(
                1000,
                100.0,
                Some(LatencySummary {
                    p50: 50.0,
                    p95: 75.0,
                    p99: 90.0,
                }),
                10,
                5,
            ));
        }
        // Measurement: fast (100ms)
        for _ in 0..5 {
            acc.record(&dummy_report(
                100,
                1000.0,
                Some(LatencySummary {
                    p50: 1.0,
                    p95: 2.0,
                    p99: 3.0,
                }),
                1,
                0,
            ));
        }

        let summary = acc.summarize();
        assert_eq!(summary.engine, "test-engine");
        assert_eq!(summary.concurrency, 4);
        assert_eq!(summary.total_iterations, 8);
        assert_eq!(summary.warmup_iterations, 3);
        assert_eq!(summary.measurement_iterations, 5);
        // Median should reflect only measurement data, not warmup
        assert_eq!(summary.throughput.median_wall_time_ms, 100);
        assert!((summary.throughput.median_ops_per_sec - 1000.0).abs() < f64::EPSILON);
        assert!((summary.latency.p50 - 1.0).abs() < f64::EPSILON);
        assert!((summary.latency.p95 - 2.0).abs() < f64::EPSILON);
        assert!((summary.latency.p99 - 3.0).abs() < f64::EPSILON);
        assert_eq!(summary.retries.median_retries, 1);
        assert_eq!(summary.retries.median_aborts, 0);
    }

    #[test]
    fn accumulator_handles_fewer_than_warmup_iterations() {
        let mut acc = RunAccumulator::new("test", 1);
        acc.record(&dummy_report(50, 200.0, None, 0, 0));
        // Only 1 iteration, warmup is 3 — all iterations are warmup
        let summary = acc.summarize();
        assert_eq!(summary.measurement_iterations, 0);
        assert_eq!(summary.throughput.median_wall_time_ms, 0);
        assert_eq!(summary.latency.p50, 0.0);
        assert_eq!(summary.latency.p95, 0.0);
        assert_eq!(summary.latency.p99, 0.0);
    }

    #[test]
    fn accumulator_uses_reported_latency_not_throughput() {
        let mut acc = RunAccumulator::new("test", 1);
        for _ in 0..3 {
            acc.record(&dummy_report(500, 25.0, None, 0, 0));
        }
        for _ in 0..5 {
            acc.record(&dummy_report(
                10,
                10_000.0,
                Some(LatencySummary {
                    p50: 4.0,
                    p95: 8.0,
                    p99: 12.0,
                }),
                0,
                0,
            ));
        }

        let summary = acc.summarize();
        assert!((summary.throughput.median_ops_per_sec - 10_000.0).abs() < f64::EPSILON);
        assert!((summary.latency.p50 - 4.0).abs() < f64::EPSILON);
        assert!((summary.latency.p95 - 8.0).abs() < f64::EPSILON);
        assert!((summary.latency.p99 - 12.0).abs() < f64::EPSILON);
    }

    #[test]
    fn accumulator_preserves_available_latency_when_some_iterations_lack_it() {
        let mut acc = RunAccumulator::new("test", 1);
        for _ in 0..3 {
            acc.record(&dummy_report(500, 25.0, None, 0, 0));
        }

        acc.record(&dummy_report(
            10,
            1000.0,
            Some(LatencySummary {
                p50: 2.0,
                p95: 4.0,
                p99: 6.0,
            }),
            0,
            0,
        ));
        acc.record(&dummy_report(10, 900.0, None, 0, 0));
        acc.record(&dummy_report(
            10,
            1100.0,
            Some(LatencySummary {
                p50: 8.0,
                p95: 10.0,
                p99: 12.0,
            }),
            0,
            0,
        ));
        acc.record(&dummy_report(10, 950.0, None, 0, 0));
        acc.record(&dummy_report(
            10,
            1050.0,
            Some(LatencySummary {
                p50: 14.0,
                p95: 16.0,
                p99: 18.0,
            }),
            0,
            0,
        ));

        let summary = acc.summarize();
        assert!((summary.latency.p50 - 8.0).abs() < f64::EPSILON);
        assert!((summary.latency.p95 - 10.0).abs() < f64::EPSILON);
        assert!((summary.latency.p99 - 12.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bench_summary_serialization_roundtrip() {
        let summary = BenchSummary::new("fixture-a", "commutative_inserts", "release");
        let json = summary.to_json().unwrap();
        let parsed: BenchSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.schema_version, BENCH_SUMMARY_SCHEMA_V1);
        assert_eq!(parsed.fixture_id, "fixture-a");
        assert_eq!(parsed.workload, "commutative_inserts");
        assert_eq!(parsed.methodology.version, "fsqlite-e2e.methodology.v1");
        assert!(!parsed.environment.arch.is_empty());
    }

    #[test]
    fn bench_summary_pretty_json_is_valid() {
        let mut summary = BenchSummary::new("db", "workload", "dev");
        let mut acc = RunAccumulator::new("sqlite3", 1);
        for i in 0u64..10 {
            #[allow(clippy::cast_precision_loss)]
            let ops = (i as f64).mul_add(-10.0, 1000.0);
            acc.record(&dummy_report(
                100 + i,
                ops,
                Some(LatencySummary {
                    p50: 1.0 + i as f64,
                    p95: 2.0 + i as f64,
                    p99: 3.0 + i as f64,
                }),
                0,
                0,
            ));
        }
        summary.cases.push(acc.summarize());

        let json = summary.to_pretty_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["cases"].is_array());
        assert_eq!(parsed["cases"][0]["engine"], "sqlite3");
        assert_eq!(parsed["cases"][0]["concurrency"], 1);
        assert!(parsed["cases"][0]["throughput"]["median_ops_per_sec"].is_f64());
    }
}
