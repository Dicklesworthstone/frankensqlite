//! Baseline management for operation-level performance tracking.
//!
//! Bead: bd-1lsfu.1
//!
//! Provides types and utilities for capturing, storing, and comparing
//! performance baselines across the 9 primary database operations.
//! Baselines are stored as version-controlled JSON artifacts under
//! `baselines/operations/`.
//!
//! ## Regression detection
//!
//! [`BaselineReport::check_regression`] compares two reports and flags any
//! operation whose p50 latency increased beyond a configurable threshold
//! (default: 10%).

use serde::{Deserialize, Serialize};

use crate::methodology::{EnvironmentMeta, MethodologyMeta};

/// Default regression threshold: 10% degradation = failure.
pub const DEFAULT_REGRESSION_THRESHOLD: f64 = 0.10;

/// Schema version for the operation baseline JSON format.
pub const BASELINE_SCHEMA_V1: &str = "fsqlite-e2e.operation_baseline.v1";

/// Baseline directory relative to the workspace root.
pub const BASELINE_DIR: &str = "baselines/operations";

/// Identifies one of the 9 primary database operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Full table scan of N rows.
    SequentialScan,
    /// B-tree point query by primary key.
    PointLookup,
    /// B-tree range query returning K rows.
    RangeScan,
    /// Insert one row with auto-increment PK.
    SingleRowInsert,
    /// Insert N rows in a single transaction.
    BatchInsert,
    /// Update one row by PK.
    SingleRowUpdate,
    /// Delete one row by PK.
    SingleRowDelete,
    /// Hash join of two tables.
    TwoWayEquiJoin,
    /// COUNT/SUM/AVG over full table.
    Aggregation,
}

impl Operation {
    /// Returns all 9 operations in canonical order.
    #[must_use]
    pub const fn all() -> [Operation; 9] {
        [
            Self::SequentialScan,
            Self::PointLookup,
            Self::RangeScan,
            Self::SingleRowInsert,
            Self::BatchInsert,
            Self::SingleRowUpdate,
            Self::SingleRowDelete,
            Self::TwoWayEquiJoin,
            Self::Aggregation,
        ]
    }

    /// Human-readable name for display.
    #[must_use]
    pub const fn display_name(&self) -> &'static str {
        match self {
            Self::SequentialScan => "sequential_scan",
            Self::PointLookup => "point_lookup",
            Self::RangeScan => "range_scan",
            Self::SingleRowInsert => "single_row_insert",
            Self::BatchInsert => "batch_insert",
            Self::SingleRowUpdate => "single_row_update",
            Self::SingleRowDelete => "single_row_delete",
            Self::TwoWayEquiJoin => "two_way_equi_join",
            Self::Aggregation => "aggregation",
        }
    }
}

/// Latency statistics for one operation, in microseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    /// 50th percentile (median) latency in microseconds.
    pub p50_micros: u64,
    /// 95th percentile latency in microseconds.
    pub p95_micros: u64,
    /// 99th percentile latency in microseconds.
    pub p99_micros: u64,
    /// Maximum latency observed, in microseconds.
    pub max_micros: u64,
}

/// Performance baseline for a single operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationBaseline {
    /// Which operation this baseline covers.
    pub operation: Operation,
    /// Which engine produced the numbers.
    pub engine: String,
    /// Number of rows in the table when measured.
    pub row_count: u64,
    /// Number of measurement iterations (after warmup).
    pub iterations: u32,
    /// Number of warmup iterations discarded.
    pub warmup_iterations: u32,
    /// Latency statistics.
    pub latency: LatencyStats,
    /// Throughput in operations per second at steady state.
    pub throughput_ops_per_sec: f64,
}

/// A complete baseline report containing all 9 operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineReport {
    /// Schema version for forward compatibility.
    pub schema_version: String,
    /// When this baseline was captured (ISO 8601).
    pub captured_at: String,
    /// Benchmark methodology metadata.
    pub methodology: MethodologyMeta,
    /// Environment snapshot for reproducibility.
    pub environment: EnvironmentMeta,
    /// Per-operation baselines.
    pub baselines: Vec<OperationBaseline>,
}

impl BaselineReport {
    /// Create a new empty report, capturing the current methodology and environment.
    #[must_use]
    pub fn new(cargo_profile: &str) -> Self {
        Self {
            schema_version: BASELINE_SCHEMA_V1.to_owned(),
            captured_at: now_iso8601(),
            methodology: MethodologyMeta::current(),
            environment: EnvironmentMeta::capture(cargo_profile),
            baselines: Vec::new(),
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

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the JSON is malformed or schema mismatches.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Check for regressions between `self` (old baseline) and `current`
    /// (new measurements).
    ///
    /// Returns a list of regressions where p50 latency increased by more
    /// than `threshold` (e.g., 0.10 for 10%).
    #[must_use]
    pub fn check_regression(
        &self,
        current: &BaselineReport,
        threshold: f64,
    ) -> Vec<RegressionResult> {
        let mut results = Vec::new();

        for old in &self.baselines {
            if let Some(new) = current
                .baselines
                .iter()
                .find(|b| b.operation == old.operation && b.engine == old.engine)
            {
                let old_p50 = old.latency.p50_micros as f64;
                let new_p50 = new.latency.p50_micros as f64;

                let change = if old_p50 > 0.0 {
                    (new_p50 - old_p50) / old_p50
                } else {
                    0.0
                };

                results.push(RegressionResult {
                    operation: old.operation,
                    engine: old.engine.clone(),
                    baseline_p50_micros: old.latency.p50_micros,
                    current_p50_micros: new.latency.p50_micros,
                    change_pct: change * 100.0,
                    regressed: change > threshold,
                });
            }
        }

        results
    }
}

/// Result of comparing one operation's baseline against current measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionResult {
    /// Which operation was compared.
    pub operation: Operation,
    /// Which engine.
    pub engine: String,
    /// Baseline p50 latency in microseconds.
    pub baseline_p50_micros: u64,
    /// Current p50 latency in microseconds.
    pub current_p50_micros: u64,
    /// Percentage change (positive = slower).
    pub change_pct: f64,
    /// Whether this exceeds the regression threshold.
    pub regressed: bool,
}

impl RegressionResult {
    /// Human-readable summary line.
    #[must_use]
    pub fn summary(&self) -> String {
        let dir = if self.change_pct >= 0.0 { "+" } else { "" };
        let status = if self.regressed { "REGRESSION" } else { "ok" };
        format!(
            "[{}] {} ({}): {}us -> {}us ({}{:.1}%)",
            status,
            self.operation.display_name(),
            self.engine,
            self.baseline_p50_micros,
            self.current_p50_micros,
            dir,
            self.change_pct,
        )
    }
}

/// Load a baseline report from a file path.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_baseline(path: &std::path::Path) -> Result<BaselineReport, Box<dyn std::error::Error>> {
    let json = std::fs::read_to_string(path)?;
    let report = BaselineReport::from_json(&json)?;
    Ok(report)
}

/// Save a baseline report to a file path.
///
/// # Errors
///
/// Returns an error if serialization or file I/O fails.
pub fn save_baseline(
    report: &BaselineReport,
    path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = report.to_pretty_json()?;
    std::fs::write(path, json)?;
    Ok(())
}

fn now_iso8601() -> String {
    // Simple UTC timestamp without chrono dependency.
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Approximate: good enough for a timestamp label.
    let days = secs / 86400;
    let year = 1970 + days / 365;
    format!("{year}-xx-xxT00:00:00Z (epoch_secs: {secs})")
}

/// Measure a single operation by running it `iterations` times (after
/// `warmup` discarded runs) and collecting latency samples.
///
/// Returns a `LatencyStats` and throughput value.
pub fn measure_operation<F>(warmup: u32, iterations: u32, mut f: F) -> (LatencyStats, f64)
where
    F: FnMut(),
{
    // Warmup phase.
    for _ in 0..warmup {
        f();
    }

    // Measurement phase.
    let mut samples_micros: Vec<u64> = Vec::with_capacity(iterations as usize);
    for _ in 0..iterations {
        let start = std::time::Instant::now();
        f();
        let elapsed = start.elapsed();
        samples_micros.push(elapsed.as_micros() as u64);
    }

    samples_micros.sort_unstable();

    let len = samples_micros.len();
    let p50 = percentile(&samples_micros, 50);
    let p95 = percentile(&samples_micros, 95);
    let p99 = percentile(&samples_micros, 99);
    let max = samples_micros.last().copied().unwrap_or(0);

    // Throughput: median ops/sec based on p50.
    let throughput = if p50 > 0 {
        1_000_000.0 / p50 as f64
    } else if len > 0 {
        // Sub-microsecond: estimate from total time.
        let total_micros: u64 = samples_micros.iter().sum();
        if total_micros > 0 {
            (len as f64) * 1_000_000.0 / total_micros as f64
        } else {
            f64::INFINITY
        }
    } else {
        0.0
    };

    (
        LatencyStats {
            p50_micros: p50,
            p95_micros: p95,
            p99_micros: p99,
            max_micros: max,
        },
        throughput,
    )
}

/// Nearest-rank percentile on a sorted slice.
fn percentile(sorted: &[u64], pct: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((f64::from(pct) / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_all_returns_nine() {
        assert_eq!(Operation::all().len(), 9);
    }

    #[test]
    fn operation_display_names_are_unique() {
        let names: Vec<&str> = Operation::all()
            .iter()
            .map(|op| op.display_name())
            .collect();
        let mut deduped = names.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(names.len(), deduped.len());
    }

    #[test]
    fn baseline_report_roundtrip() {
        let mut report = BaselineReport::new("test");
        report.baselines.push(OperationBaseline {
            operation: Operation::PointLookup,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 50,
                p95_micros: 100,
                p99_micros: 200,
                max_micros: 500,
            },
            throughput_ops_per_sec: 20000.0,
        });

        let json = report.to_pretty_json().unwrap();
        let parsed = BaselineReport::from_json(&json).unwrap();
        assert_eq!(parsed.schema_version, BASELINE_SCHEMA_V1);
        assert_eq!(parsed.baselines.len(), 1);
        assert_eq!(parsed.baselines[0].operation, Operation::PointLookup);
        assert_eq!(parsed.baselines[0].latency.p50_micros, 50);
    }

    #[test]
    fn regression_detection_flags_increase() {
        let mut old = BaselineReport::new("test");
        old.baselines.push(OperationBaseline {
            operation: Operation::SequentialScan,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 100,
                p95_micros: 200,
                p99_micros: 300,
                max_micros: 500,
            },
            throughput_ops_per_sec: 10000.0,
        });

        let mut current = BaselineReport::new("test");
        // 20% regression (100 -> 120).
        current.baselines.push(OperationBaseline {
            operation: Operation::SequentialScan,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 120,
                p95_micros: 250,
                p99_micros: 350,
                max_micros: 600,
            },
            throughput_ops_per_sec: 8333.0,
        });

        let results = old.check_regression(&current, 0.10);
        assert_eq!(results.len(), 1);
        assert!(results[0].regressed);
        assert!((results[0].change_pct - 20.0).abs() < 0.1);
    }

    #[test]
    fn regression_detection_ok_within_threshold() {
        let mut old = BaselineReport::new("test");
        old.baselines.push(OperationBaseline {
            operation: Operation::PointLookup,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 100,
                p95_micros: 200,
                p99_micros: 300,
                max_micros: 500,
            },
            throughput_ops_per_sec: 10000.0,
        });

        let mut current = BaselineReport::new("test");
        // 5% increase (100 -> 105): within threshold.
        current.baselines.push(OperationBaseline {
            operation: Operation::PointLookup,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 105,
                p95_micros: 210,
                p99_micros: 310,
                max_micros: 510,
            },
            throughput_ops_per_sec: 9524.0,
        });

        let results = old.check_regression(&current, 0.10);
        assert_eq!(results.len(), 1);
        assert!(!results[0].regressed);
    }

    #[test]
    fn regression_detection_improvement_not_flagged() {
        let mut old = BaselineReport::new("test");
        old.baselines.push(OperationBaseline {
            operation: Operation::BatchInsert,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 100,
                p95_micros: 200,
                p99_micros: 300,
                max_micros: 500,
            },
            throughput_ops_per_sec: 10000.0,
        });

        let mut current = BaselineReport::new("test");
        // 20% improvement (100 -> 80): should not be flagged.
        current.baselines.push(OperationBaseline {
            operation: Operation::BatchInsert,
            engine: "frankensqlite".to_owned(),
            row_count: 1000,
            iterations: 100,
            warmup_iterations: 10,
            latency: LatencyStats {
                p50_micros: 80,
                p95_micros: 160,
                p99_micros: 240,
                max_micros: 400,
            },
            throughput_ops_per_sec: 12500.0,
        });

        let results = old.check_regression(&current, 0.10);
        assert_eq!(results.len(), 1);
        assert!(!results[0].regressed);
    }

    #[test]
    fn measure_operation_produces_sane_stats() {
        let mut counter = 0u64;
        let (stats, throughput) = measure_operation(2, 10, || {
            counter += 1;
            // Busy-wait for at least 1 microsecond.
            let start = std::time::Instant::now();
            while start.elapsed().as_nanos() < 1000 {}
        });
        // Warmup (2) + measurement (10) = 12 total calls.
        assert_eq!(counter, 12);
        // p50 should be >= 1 microsecond.
        assert!(stats.p50_micros >= 1);
        // p95 >= p50.
        assert!(stats.p95_micros >= stats.p50_micros);
        // p99 >= p95.
        assert!(stats.p99_micros >= stats.p95_micros);
        // max >= p99.
        assert!(stats.max_micros >= stats.p99_micros);
        // Throughput should be positive.
        assert!(throughput > 0.0);
    }

    #[test]
    fn percentile_edge_cases() {
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[42], 50), 42);
        assert_eq!(percentile(&[10, 20, 30, 40, 50], 0), 10);
        assert_eq!(percentile(&[10, 20, 30, 40, 50], 100), 50);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_baseline.json");

        let mut report = BaselineReport::new("test");
        report.baselines.push(OperationBaseline {
            operation: Operation::Aggregation,
            engine: "frankensqlite".to_owned(),
            row_count: 5000,
            iterations: 50,
            warmup_iterations: 5,
            latency: LatencyStats {
                p50_micros: 200,
                p95_micros: 400,
                p99_micros: 600,
                max_micros: 1000,
            },
            throughput_ops_per_sec: 5000.0,
        });

        save_baseline(&report, &path).unwrap();
        let loaded = load_baseline(&path).unwrap();
        assert_eq!(loaded.baselines.len(), 1);
        assert_eq!(loaded.baselines[0].operation, Operation::Aggregation);
        assert_eq!(loaded.baselines[0].latency.p50_micros, 200);
    }

    #[test]
    fn regression_result_summary_format() {
        let result = RegressionResult {
            operation: Operation::SequentialScan,
            engine: "frankensqlite".to_owned(),
            baseline_p50_micros: 100,
            current_p50_micros: 115,
            change_pct: 15.0,
            regressed: true,
        };
        let summary = result.summary();
        assert!(summary.contains("REGRESSION"));
        assert!(summary.contains("sequential_scan"));
        assert!(summary.contains("+15.0%"));
    }
}
