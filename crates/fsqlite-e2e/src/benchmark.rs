//! Benchmark runner: repeated workload execution with statistical analysis.
//!
//! Bead: bd-1w6k.6.2
//!
//! Runs a workload function multiple times following the canonical methodology
//! ([`crate::methodology`]):
//!
//! 1. **Warmup** — discard the first N iterations to eliminate cold-start effects.
//! 2. **Measurement** — collect at least `min_iterations` samples over at least
//!    `measurement_time_secs` of wall-clock time.
//! 3. **Statistics** — compute latency (median, p95, p99, mean, stddev) and
//!    throughput (ops/sec) summaries.
//!
//! The runner is engine-agnostic: callers supply a closure that executes one
//! iteration and returns an [`crate::report::EngineRunReport`].  The caller is
//! responsible for ensuring a fresh database state per iteration.

use serde::{Deserialize, Serialize};

use crate::methodology::{
    EnvironmentMeta, MEASUREMENT_TIME_SECS, MIN_MEASUREMENT_ITERATIONS, MethodologyMeta,
    WARMUP_ITERATIONS,
};
use crate::report::EngineRunReport;

// ── Configuration ──────────────────────────────────────────────────────

/// Configuration knobs for a benchmark run.
///
/// Defaults match the canonical methodology constants.
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of warmup iterations discarded before measurement.
    pub warmup_iterations: u32,
    /// Minimum number of timed measurement iterations.
    pub min_iterations: u32,
    /// Measurement time floor in seconds — keep sampling until this much
    /// wall-clock time has elapsed *and* `min_iterations` are collected.
    pub measurement_time_secs: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: WARMUP_ITERATIONS,
            min_iterations: MIN_MEASUREMENT_ITERATIONS,
            measurement_time_secs: MEASUREMENT_TIME_SECS,
        }
    }
}

// ── Metadata ───────────────────────────────────────────────────────────

/// Identifiers for a benchmark run (engine, workload, fixture, concurrency).
#[derive(Debug, Clone)]
pub struct BenchmarkMeta {
    /// Engine name (e.g. `"sqlite3"`, `"fsqlite"`).
    pub engine: String,
    /// Workload preset name.
    pub workload: String,
    /// Fixture (database) identifier.
    pub fixture_id: String,
    /// Concurrency level.
    pub concurrency: u16,
    /// Cargo profile used for the build (e.g. `"release"`).
    pub cargo_profile: String,
}

// ── Summary output ─────────────────────────────────────────────────────

/// Complete benchmark summary — the primary output artifact.
///
/// Serializes to a self-contained JSON object suitable for JSONL logs or
/// standalone report files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    /// Stable identifier: `"{engine}:{workload}:{fixture_id}:c{concurrency}"`.
    pub benchmark_id: String,
    /// Engine under test.
    pub engine: String,
    /// Workload preset name.
    pub workload: String,
    /// Fixture (database) identifier.
    pub fixture_id: String,
    /// Concurrency level.
    pub concurrency: u16,
    /// Methodology metadata for reproducibility.
    pub methodology: MethodologyMeta,
    /// Environment metadata for reproducibility.
    pub environment: EnvironmentMeta,
    /// Number of warmup iterations executed (discarded).
    pub warmup_count: u32,
    /// Number of measurement iterations executed.
    pub measurement_count: u32,
    /// Total wall-clock time for all measurement iterations (ms).
    pub total_measurement_ms: u64,
    /// Latency statistics across measurement iterations.
    pub latency: LatencyStats,
    /// Throughput statistics across measurement iterations.
    pub throughput: ThroughputStats,
    /// Per-iteration raw data for downstream analysis.
    pub iterations: Vec<IterationRecord>,
}

impl BenchmarkSummary {
    /// Serialize to a compact JSON line (for JSONL logs).
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the summary cannot be serialized.
    pub fn to_jsonl(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Serialize to pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the summary cannot be serialized.
    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Latency statistics (all values in milliseconds).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub median_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub stddev_ms: f64,
}

/// Throughput statistics (operations per second).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputStats {
    /// Mean ops/sec across iterations.
    pub mean_ops_per_sec: f64,
    /// Median ops/sec across iterations.
    pub median_ops_per_sec: f64,
    /// Peak (max) ops/sec observed in any single iteration.
    pub peak_ops_per_sec: f64,
}

/// Raw record for a single measurement iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterationRecord {
    /// 0-based index within the measurement phase (excludes warmup).
    pub iteration: u32,
    /// Wall time in milliseconds.
    pub wall_time_ms: u64,
    /// Operations per second.
    pub ops_per_sec: f64,
    /// Total operations executed.
    pub ops_total: u64,
    /// Retries due to busy/lock contention.
    pub retries: u64,
    /// Aborted transactions.
    pub aborts: u64,
    /// Error message, if the iteration failed.
    pub error: Option<String>,
}

// ── Runner ─────────────────────────────────────────────────────────────

/// Run a benchmark: warmup + measurement iterations with statistical analysis.
///
/// `iteration_fn` is called for each iteration (warmup and measurement).
/// It receives the overall iteration index (0-based, including warmup) and
/// must return an [`EngineRunReport`] for that run.  The caller is
/// responsible for providing a fresh database state per call.
///
/// If `iteration_fn` returns `Err`, the benchmark records the error in the
/// iteration record and continues (best-effort — the iteration's wall time
/// is still measured and included in statistics).
#[allow(clippy::cast_precision_loss)]
pub fn run_benchmark<F, E>(
    config: &BenchmarkConfig,
    meta: &BenchmarkMeta,
    mut iteration_fn: F,
) -> BenchmarkSummary
where
    F: FnMut(u32) -> Result<EngineRunReport, E>,
    E: std::fmt::Display,
{
    let mut global_idx: u32 = 0;

    // ── Warmup phase ───────────────────────────────────────────────
    for _ in 0..config.warmup_iterations {
        let _ = iteration_fn(global_idx);
        global_idx = global_idx.saturating_add(1);
    }

    // ── Measurement phase ──────────────────────────────────────────
    let mut iterations: Vec<IterationRecord> = Vec::with_capacity(config.min_iterations as usize);
    let measurement_start = std::time::Instant::now();
    let time_floor = std::time::Duration::from_secs(config.measurement_time_secs);

    let mut measurement_idx: u32 = 0;
    loop {
        let iter_start = std::time::Instant::now();
        let result = iteration_fn(global_idx);
        let iter_elapsed = iter_start.elapsed();

        let record = match result {
            Ok(report) => IterationRecord {
                iteration: measurement_idx,
                wall_time_ms: duration_to_u64_ms(iter_elapsed),
                ops_per_sec: report.ops_per_sec,
                ops_total: report.ops_total,
                retries: report.retries,
                aborts: report.aborts,
                error: report.error.clone(),
            },
            Err(e) => IterationRecord {
                iteration: measurement_idx,
                wall_time_ms: duration_to_u64_ms(iter_elapsed),
                ops_per_sec: 0.0,
                ops_total: 0,
                retries: 0,
                aborts: 0,
                error: Some(e.to_string()),
            },
        };

        iterations.push(record);
        measurement_idx = measurement_idx.saturating_add(1);
        global_idx = global_idx.saturating_add(1);

        // Continue until both min iterations and time floor are met.
        if measurement_idx >= config.min_iterations && measurement_start.elapsed() >= time_floor {
            break;
        }
    }

    let total_measurement_ms = duration_to_u64_ms(measurement_start.elapsed());

    // ── Compute statistics ─────────────────────────────────────────
    let wall_times: Vec<f64> = iterations.iter().map(|r| r.wall_time_ms as f64).collect();
    let throughputs: Vec<f64> = iterations.iter().map(|r| r.ops_per_sec).collect();

    let latency = compute_latency_stats(&wall_times);
    let throughput = compute_throughput_stats(&throughputs);

    let benchmark_id = format!(
        "{}:{}:{}:c{}",
        meta.engine, meta.workload, meta.fixture_id, meta.concurrency
    );

    BenchmarkSummary {
        benchmark_id,
        engine: meta.engine.clone(),
        workload: meta.workload.clone(),
        fixture_id: meta.fixture_id.clone(),
        concurrency: meta.concurrency,
        methodology: MethodologyMeta::current(),
        environment: EnvironmentMeta::capture(&meta.cargo_profile),
        warmup_count: config.warmup_iterations,
        measurement_count: measurement_idx,
        total_measurement_ms,
        latency,
        throughput,
        iterations,
    }
}

// ── Statistics helpers ─────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss)]
fn compute_latency_stats(wall_times: &[f64]) -> LatencyStats {
    if wall_times.is_empty() {
        return LatencyStats {
            min_ms: 0.0,
            max_ms: 0.0,
            mean_ms: 0.0,
            median_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            stddev_ms: 0.0,
        };
    }

    let mut sorted = wall_times.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let m = mean(wall_times);
    let med = percentile(&sorted, 0.50);
    let p95 = percentile(&sorted, 0.95);
    let p99 = percentile(&sorted, 0.99);
    let sd = stddev(wall_times, m);

    LatencyStats {
        min_ms: min,
        max_ms: max,
        mean_ms: m,
        median_ms: med,
        p95_ms: p95,
        p99_ms: p99,
        stddev_ms: sd,
    }
}

fn compute_throughput_stats(throughputs: &[f64]) -> ThroughputStats {
    if throughputs.is_empty() {
        return ThroughputStats {
            mean_ops_per_sec: 0.0,
            median_ops_per_sec: 0.0,
            peak_ops_per_sec: 0.0,
        };
    }

    let mut sorted = throughputs.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    ThroughputStats {
        mean_ops_per_sec: mean(throughputs),
        median_ops_per_sec: percentile(&sorted, 0.50),
        peak_ops_per_sec: sorted[sorted.len() - 1],
    }
}

/// Linear-interpolation percentile on a **sorted** slice.
#[allow(clippy::cast_precision_loss)]
fn percentile(sorted: &[f64], p: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = p * (sorted.len() - 1) as f64;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let lo = idx.floor() as usize;
    let hi = lo.saturating_add(1).min(sorted.len() - 1);
    let frac = idx - lo as f64;
    sorted[lo].mul_add(1.0 - frac, sorted[hi] * frac)
}

#[allow(clippy::cast_precision_loss)]
fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// Sample standard deviation (Bessel's correction: divide by `n - 1`).
#[allow(clippy::cast_precision_loss)]
fn stddev(values: &[f64], m: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let variance = values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

fn duration_to_u64_ms(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::CorrectnessReport;

    fn dummy_report(wall_ms: u64, ops: u64) -> EngineRunReport {
        #[allow(clippy::cast_precision_loss)]
        let ops_per_sec = if wall_ms > 0 {
            ops as f64 / (wall_ms as f64 / 1000.0)
        } else {
            0.0
        };
        EngineRunReport {
            wall_time_ms: wall_ms,
            ops_total: ops,
            ops_per_sec,
            retries: 0,
            aborts: 0,
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
            latency_ms: None,
            error: None,
            first_failure_diagnostic: None,
            storage_wiring: None,
            hot_path_profile: None,
        }
    }

    fn test_meta() -> BenchmarkMeta {
        BenchmarkMeta {
            engine: "test-engine".to_owned(),
            workload: "test-workload".to_owned(),
            fixture_id: "test-fixture".to_owned(),
            concurrency: 1,
            cargo_profile: "test".to_owned(),
        }
    }

    fn fast_config() -> BenchmarkConfig {
        BenchmarkConfig {
            warmup_iterations: 1,
            min_iterations: 5,
            measurement_time_secs: 0,
        }
    }

    #[test]
    fn basic_benchmark_run() {
        let config = fast_config();
        let meta = test_meta();
        let mut call_count: u32 = 0;

        let summary = run_benchmark(&config, &meta, |_idx| {
            call_count += 1;
            Ok::<_, String>(dummy_report(100, 1000))
        });

        // 1 warmup + 5 measurement = 6 total calls.
        assert_eq!(call_count, 6);
        assert_eq!(summary.warmup_count, 1);
        assert_eq!(summary.measurement_count, 5);
        assert_eq!(summary.iterations.len(), 5);
        assert_eq!(summary.engine, "test-engine");
        assert_eq!(summary.workload, "test-workload");
        assert_eq!(
            summary.benchmark_id,
            "test-engine:test-workload:test-fixture:c1"
        );
    }

    #[test]
    fn warmup_iterations_discarded() {
        let config = BenchmarkConfig {
            warmup_iterations: 3,
            min_iterations: 2,
            measurement_time_secs: 0,
        };
        let meta = test_meta();
        let mut all_indices = Vec::new();

        let summary = run_benchmark(&config, &meta, |idx| {
            all_indices.push(idx);
            Ok::<_, String>(dummy_report(50, 500))
        });

        // 3 warmup + 2 measurement = 5 total calls.
        assert_eq!(all_indices.len(), 5);
        // Only measurement iterations appear in the summary.
        assert_eq!(summary.iterations.len(), 2);
        assert_eq!(summary.warmup_count, 3);
        assert_eq!(summary.measurement_count, 2);
    }

    #[test]
    fn error_iterations_recorded() {
        let config = fast_config();
        let meta = test_meta();
        let mut call: u32 = 0;

        let summary = run_benchmark(&config, &meta, |_idx| {
            call += 1;
            if call == 3 {
                Err("simulated failure")
            } else {
                Ok(dummy_report(100, 1000))
            }
        });

        // Error iteration should still be recorded.
        assert_eq!(summary.iterations.len(), 5);
        let err_iter = &summary.iterations[1]; // call 3 = warmup(1) + measurement(2), idx 1
        assert!(err_iter.error.is_some());
        assert_eq!(err_iter.ops_total, 0);
    }

    #[test]
    fn latency_stats_computed_correctly() {
        // Use known values for deterministic verification.
        let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let stats = compute_latency_stats(&values);

        assert!((stats.min_ms - 10.0).abs() < f64::EPSILON);
        assert!((stats.max_ms - 50.0).abs() < f64::EPSILON);
        assert!((stats.mean_ms - 30.0).abs() < f64::EPSILON);
        assert!((stats.median_ms - 30.0).abs() < f64::EPSILON);
        // p95 of [10,20,30,40,50]: index = 0.95 * 4 = 3.8 → lerp(40,50,0.8) = 48.0
        assert!((stats.p95_ms - 48.0).abs() < 0.01);
        // p99 of [10,20,30,40,50]: index = 0.99 * 4 = 3.96 → lerp(40,50,0.96) = 49.6
        assert!((stats.p99_ms - 49.6).abs() < 0.01);
        // stddev: sqrt(sum((x-30)^2)/4) = sqrt((400+100+0+100+400)/4) = sqrt(250) ≈ 15.81
        assert!((stats.stddev_ms - 15.811).abs() < 0.01);
    }

    #[test]
    fn throughput_stats_computed_correctly() {
        let values = vec![100.0, 200.0, 300.0, 400.0, 500.0];
        let stats = compute_throughput_stats(&values);

        assert!((stats.mean_ops_per_sec - 300.0).abs() < f64::EPSILON);
        assert!((stats.median_ops_per_sec - 300.0).abs() < f64::EPSILON);
        assert!((stats.peak_ops_per_sec - 500.0).abs() < f64::EPSILON);
    }

    #[test]
    fn percentile_single_element() {
        assert!((percentile(&[42.0], 0.5) - 42.0).abs() < f64::EPSILON);
        assert!((percentile(&[42.0], 0.99) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn percentile_two_elements() {
        let sorted = [10.0, 20.0];
        // p50: idx = 0.5 * 1 = 0.5 → lerp(10, 20, 0.5) = 15.0
        assert!((percentile(&sorted, 0.5) - 15.0).abs() < f64::EPSILON);
        // p0: 10.0
        assert!((percentile(&sorted, 0.0) - 10.0).abs() < f64::EPSILON);
        // p100: 20.0
        assert!((percentile(&sorted, 1.0) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn empty_stats_are_zero() {
        let lat = compute_latency_stats(&[]);
        assert!((lat.mean_ms).abs() < f64::EPSILON);
        assert!((lat.median_ms).abs() < f64::EPSILON);

        let tp = compute_throughput_stats(&[]);
        assert!((tp.mean_ops_per_sec).abs() < f64::EPSILON);
    }

    #[test]
    fn summary_serialization_roundtrip() {
        let config = fast_config();
        let meta = test_meta();

        let summary = run_benchmark(&config, &meta, |_| Ok::<_, String>(dummy_report(100, 1000)));

        let json = summary.to_pretty_json().unwrap();
        let parsed: BenchmarkSummary = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.benchmark_id, summary.benchmark_id);
        assert_eq!(parsed.measurement_count, summary.measurement_count);
        assert!((parsed.latency.median_ms - summary.latency.median_ms).abs() < 0.01);
        assert_eq!(parsed.iterations.len(), summary.iterations.len());
    }

    #[test]
    fn jsonl_output_is_single_line() {
        let config = fast_config();
        let meta = test_meta();

        let summary = run_benchmark(&config, &meta, |_| Ok::<_, String>(dummy_report(50, 500)));

        let jsonl = summary.to_jsonl().unwrap();
        assert!(!jsonl.contains('\n'), "JSONL output must be a single line");
        // Verify it's valid JSON.
        let _: serde_json::Value = serde_json::from_str(&jsonl).unwrap();
    }

    #[test]
    fn methodology_embedded_in_summary() {
        let config = fast_config();
        let meta = test_meta();

        let summary = run_benchmark(&config, &meta, |_| Ok::<_, String>(dummy_report(100, 1000)));

        assert_eq!(summary.methodology.version, "fsqlite-e2e.methodology.v1");
        assert_eq!(summary.methodology.primary_statistic, "median");
        assert_eq!(summary.methodology.tail_statistic, "p95");
    }

    #[test]
    fn environment_captured_in_summary() {
        let config = fast_config();
        let meta = test_meta();

        let summary = run_benchmark(&config, &meta, |_| Ok::<_, String>(dummy_report(100, 1000)));

        assert!(!summary.environment.arch.is_empty());
        assert!(summary.environment.cpu_count >= 1);
        assert_eq!(summary.environment.cargo_profile, "test");
    }

    #[test]
    fn default_config_matches_methodology() {
        let config = BenchmarkConfig::default();
        assert_eq!(config.warmup_iterations, WARMUP_ITERATIONS);
        assert_eq!(config.min_iterations, MIN_MEASUREMENT_ITERATIONS);
        assert_eq!(config.measurement_time_secs, MEASUREMENT_TIME_SECS);
    }
}
