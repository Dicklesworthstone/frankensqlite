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
/// Schema version for structured benchmark-recovery slice reports.
pub const BENCHMARK_RECOVERY_REPORT_SCHEMA_V1: &str = "fsqlite-e2e.benchmark_recovery_report.v1";

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
    pub const fn all() -> [Self; 9] {
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
    pub fn check_regression(&self, current: &Self, threshold: f64) -> Vec<RegressionResult> {
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

/// Probe IDs for benchmark-catastrophe recovery slices that need explicit
/// operator-facing pass/fail evaluation rather than raw log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkRecoveryProbeId {
    /// `manual_perf_probe.read_guard_shapes.in_subquery`
    InSubquery10kLatency,
    /// `manual_hot_path_profile.in_subquery_100k`
    InSubquery100kWallTime,
}

impl BenchmarkRecoveryProbeId {
    #[must_use]
    pub const fn display_name(&self) -> &'static str {
        match self {
            Self::InSubquery10kLatency => "in_subquery_10k_latency",
            Self::InSubquery100kWallTime => "in_subquery_100k_wall_time",
        }
    }

    #[must_use]
    pub const fn evidence_label(&self) -> &'static str {
        match self {
            Self::InSubquery10kLatency => "manual_perf_probe.read_guard_shapes.in_subquery",
            Self::InSubquery100kWallTime => "manual_hot_path_profile.in_subquery_100k",
        }
    }
}

/// Contract threshold for one benchmark recovery probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkRecoveryThreshold {
    pub probe_id: BenchmarkRecoveryProbeId,
    pub target_summary: String,
    pub legacy_anchor: String,
    pub max_p50_micros: Option<u64>,
    pub max_p95_micros: Option<u64>,
    pub max_wall_time_micros: Option<u64>,
    pub hard_fail_wall_time_micros: Option<u64>,
}

/// Captured benchmark measurement for one recovery probe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkRecoveryMeasurement {
    pub probe_id: BenchmarkRecoveryProbeId,
    pub evidence_label: String,
    pub row_count: u64,
    pub p50_micros: Option<u64>,
    pub p95_micros: Option<u64>,
    pub throughput_ops_per_sec: Option<f64>,
    pub wall_time_micros: Option<u64>,
}

impl BenchmarkRecoveryMeasurement {
    #[must_use]
    pub fn latency_probe(
        probe_id: BenchmarkRecoveryProbeId,
        row_count: u64,
        p50_micros: u64,
        p95_micros: u64,
        throughput_ops_per_sec: f64,
    ) -> Self {
        Self {
            probe_id,
            evidence_label: probe_id.evidence_label().to_owned(),
            row_count,
            p50_micros: Some(p50_micros),
            p95_micros: Some(p95_micros),
            throughput_ops_per_sec: Some(throughput_ops_per_sec),
            wall_time_micros: None,
        }
    }

    #[must_use]
    pub fn wall_time_probe(
        probe_id: BenchmarkRecoveryProbeId,
        row_count: u64,
        wall_time_micros: u64,
    ) -> Self {
        Self {
            probe_id,
            evidence_label: probe_id.evidence_label().to_owned(),
            row_count,
            p50_micros: None,
            p95_micros: None,
            throughput_ops_per_sec: None,
            wall_time_micros: Some(wall_time_micros),
        }
    }
}

/// Outcome for a benchmark recovery probe against its declared threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkRecoveryStatus {
    Passed,
    Failed,
    HardFail,
}

impl BenchmarkRecoveryStatus {
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::HardFail => "hard_fail",
        }
    }
}

/// Evaluation record for one captured recovery probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkRecoveryAssessment {
    pub probe_id: BenchmarkRecoveryProbeId,
    pub status: BenchmarkRecoveryStatus,
    pub findings: Vec<String>,
}

/// Structured artifact for one benchmark-catastrophe recovery slice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRecoveryReport {
    pub schema_version: String,
    pub bead_id: String,
    pub slice_id: String,
    pub captured_at: String,
    pub methodology: MethodologyMeta,
    pub environment: EnvironmentMeta,
    pub thresholds: Vec<BenchmarkRecoveryThreshold>,
    pub measurements: Vec<BenchmarkRecoveryMeasurement>,
    pub assessments: Vec<BenchmarkRecoveryAssessment>,
}

impl BenchmarkRecoveryReport {
    /// Serialize the report as pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Threshold pack for the `bd-wwqen` IN-subquery catastrophe-recovery slice.
///
/// This codifies the operator note from the benchmark catastrophe rerun:
/// the 10k residual must get back below the sub-millisecond band, and the
/// 100k catastrophe shape must stop living in the seconds range.
#[must_use]
pub fn bd_wwqen_in_subquery_recovery_thresholds() -> Vec<BenchmarkRecoveryThreshold> {
    vec![
        BenchmarkRecoveryThreshold {
            probe_id: BenchmarkRecoveryProbeId::InSubquery10kLatency,
            target_summary: "PASS if p50 < 500us and p95 < 800us".to_owned(),
            legacy_anchor:
                "2026-03-26 residual anchor: p50=3760us p95=4429us throughput=266 ops/sec"
                    .to_owned(),
            max_p50_micros: Some(500),
            max_p95_micros: Some(800),
            max_wall_time_micros: None,
            hard_fail_wall_time_micros: None,
        },
        BenchmarkRecoveryThreshold {
            probe_id: BenchmarkRecoveryProbeId::InSubquery100kWallTime,
            target_summary: "PASS if wall < 200ms; HARD FAIL if wall > 5s".to_owned(),
            legacy_anchor: "2026-03-26 catastrophe anchor: ~20s wall time at 100k rows".to_owned(),
            max_p50_micros: None,
            max_p95_micros: None,
            max_wall_time_micros: Some(200_000),
            hard_fail_wall_time_micros: Some(5_000_000),
        },
    ]
}

fn evaluate_benchmark_recovery_probe(
    threshold: &BenchmarkRecoveryThreshold,
    measurement: &BenchmarkRecoveryMeasurement,
) -> BenchmarkRecoveryAssessment {
    let mut status = BenchmarkRecoveryStatus::Passed;
    let mut findings = Vec::new();

    if let Some(max_p50_micros) = threshold.max_p50_micros {
        match measurement.p50_micros {
            Some(value) if value <= max_p50_micros => findings.push(format!(
                "p50 {}us is within the {}us target",
                value, max_p50_micros
            )),
            Some(value) => {
                status = BenchmarkRecoveryStatus::Failed;
                findings.push(format!(
                    "p50 {}us exceeds the {}us target",
                    value, max_p50_micros
                ));
            }
            None => {
                status = BenchmarkRecoveryStatus::Failed;
                findings.push("missing p50 measurement for this recovery probe".to_owned());
            }
        }
    }

    if let Some(max_p95_micros) = threshold.max_p95_micros {
        match measurement.p95_micros {
            Some(value) if value <= max_p95_micros => findings.push(format!(
                "p95 {}us is within the {}us target",
                value, max_p95_micros
            )),
            Some(value) => {
                status = BenchmarkRecoveryStatus::Failed;
                findings.push(format!(
                    "p95 {}us exceeds the {}us target",
                    value, max_p95_micros
                ));
            }
            None => {
                status = BenchmarkRecoveryStatus::Failed;
                findings.push("missing p95 measurement for this recovery probe".to_owned());
            }
        }
    }

    if let Some(max_wall_time_micros) = threshold.max_wall_time_micros {
        match measurement.wall_time_micros {
            Some(value) if value <= max_wall_time_micros => findings.push(format!(
                "wall {}us is within the {}us pass target",
                value, max_wall_time_micros
            )),
            Some(value) => {
                if status == BenchmarkRecoveryStatus::Passed {
                    status = BenchmarkRecoveryStatus::Failed;
                }
                findings.push(format!(
                    "wall {}us exceeds the {}us pass target",
                    value, max_wall_time_micros
                ));
            }
            None => {
                if status == BenchmarkRecoveryStatus::Passed {
                    status = BenchmarkRecoveryStatus::Failed;
                }
                findings.push("missing wall-time measurement for this recovery probe".to_owned());
            }
        }
    }

    if let Some(hard_fail_wall_time_micros) = threshold.hard_fail_wall_time_micros
        && let Some(value) = measurement.wall_time_micros
        && value > hard_fail_wall_time_micros
    {
        status = BenchmarkRecoveryStatus::HardFail;
        findings.push(format!(
            "wall {}us breaches the {}us hard-fail ceiling",
            value, hard_fail_wall_time_micros
        ));
    }

    BenchmarkRecoveryAssessment {
        probe_id: measurement.probe_id,
        status,
        findings,
    }
}

/// Evaluate a partial or complete `bd-wwqen` IN-subquery recovery slice.
///
/// The caller can pass just the probes collected in one manual rerun; the
/// report will assess only those measurements while still carrying the full
/// declared threshold contract for the slice.
#[must_use]
pub fn evaluate_bd_wwqen_in_subquery_recovery(
    cargo_profile: &str,
    measurements: Vec<BenchmarkRecoveryMeasurement>,
) -> BenchmarkRecoveryReport {
    let thresholds = bd_wwqen_in_subquery_recovery_thresholds();
    let assessments = measurements
        .iter()
        .filter_map(|measurement| {
            thresholds
                .iter()
                .find(|threshold| threshold.probe_id == measurement.probe_id)
                .map(|threshold| evaluate_benchmark_recovery_probe(threshold, measurement))
        })
        .collect();

    BenchmarkRecoveryReport {
        schema_version: BENCHMARK_RECOVERY_REPORT_SCHEMA_V1.to_owned(),
        bead_id: "bd-wwqen".to_owned(),
        slice_id: "in_subquery_catastrophe_recovery".to_owned(),
        captured_at: now_iso8601(),
        methodology: MethodologyMeta::current(),
        environment: EnvironmentMeta::capture(cargo_profile),
        thresholds,
        measurements,
        assessments,
    }
}

/// Render a benchmark-recovery slice report as operator-facing markdown.
#[must_use]
pub fn render_benchmark_recovery_markdown(report: &BenchmarkRecoveryReport) -> String {
    let mut out = String::with_capacity(2048);
    let _ = std::fmt::Write::write_str(&mut out, "# Benchmark Recovery Slice\n\n");
    let _ = std::fmt::Write::write_fmt(
        &mut out,
        format_args!(
            "- Bead: `{}`\n- Slice: `{}`\n- Schema: `{}`\n- Captured at: `{}`\n- Cargo profile: `{}`\n\n",
            report.bead_id,
            report.slice_id,
            report.schema_version,
            report.captured_at,
            report.environment.cargo_profile
        ),
    );
    let _ = std::fmt::Write::write_str(&mut out, "## Thresholds\n\n");
    for threshold in &report.thresholds {
        let _ = std::fmt::Write::write_fmt(
            &mut out,
            format_args!(
                "- `{}`: {}. Legacy anchor: {}.\n",
                threshold.probe_id.display_name(),
                threshold.target_summary,
                threshold.legacy_anchor
            ),
        );
    }
    let _ = std::fmt::Write::write_str(&mut out, "\n## Measurements\n\n");
    if report.measurements.is_empty() {
        let _ = std::fmt::Write::write_str(
            &mut out,
            "- No measurements were recorded for this recovery slice.\n",
        );
    } else {
        for measurement in &report.measurements {
            let p50 = measurement
                .p50_micros
                .map_or_else(|| "-".to_owned(), |value| format!("{value}us"));
            let p95 = measurement
                .p95_micros
                .map_or_else(|| "-".to_owned(), |value| format!("{value}us"));
            let throughput = measurement
                .throughput_ops_per_sec
                .map_or_else(|| "-".to_owned(), |value| format!("{value:.1} ops/sec"));
            let wall = measurement
                .wall_time_micros
                .map_or_else(|| "-".to_owned(), |value| format!("{value}us"));
            let _ = std::fmt::Write::write_fmt(
                &mut out,
                format_args!(
                    "- `{}` from `{}`: rows={} p50={} p95={} throughput={} wall={}\n",
                    measurement.probe_id.display_name(),
                    measurement.evidence_label,
                    measurement.row_count,
                    p50,
                    p95,
                    throughput,
                    wall
                ),
            );
        }
    }
    let _ = std::fmt::Write::write_str(&mut out, "\n## Assessment\n\n");
    if report.assessments.is_empty() {
        let _ = std::fmt::Write::write_str(
            &mut out,
            "- No assessments were produced because no matching recovery probes were provided.\n",
        );
    } else {
        for assessment in &report.assessments {
            let finding_summary = if assessment.findings.is_empty() {
                "no findings".to_owned()
            } else {
                assessment.findings.join("; ")
            };
            let _ = std::fmt::Write::write_fmt(
                &mut out,
                format_args!(
                    "- `{}`: {}. {}\n",
                    assessment.probe_id.display_name(),
                    assessment.status.label(),
                    finding_summary
                ),
            );
        }
    }
    out
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
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        samples_micros.push(micros);
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
    let last_index = sorted.len() - 1;
    let pct_usize = usize::try_from(pct).map_or(100, |value| value.min(100));
    let idx = pct_usize.saturating_mul(last_index).saturating_add(50) / 100;
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
            .map(Operation::display_name)
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
    fn bd_wwqen_in_subquery_recovery_threshold_pack_matches_operator_contract() {
        let thresholds = bd_wwqen_in_subquery_recovery_thresholds();
        assert_eq!(thresholds.len(), 2);
        assert_eq!(
            thresholds[0].probe_id,
            BenchmarkRecoveryProbeId::InSubquery10kLatency
        );
        assert_eq!(thresholds[0].max_p50_micros, Some(500));
        assert_eq!(thresholds[0].max_p95_micros, Some(800));
        assert!(
            thresholds[0]
                .legacy_anchor
                .contains("p50=3760us p95=4429us throughput=266 ops/sec")
        );
        assert_eq!(
            thresholds[1].probe_id,
            BenchmarkRecoveryProbeId::InSubquery100kWallTime
        );
        assert_eq!(thresholds[1].max_wall_time_micros, Some(200_000));
        assert_eq!(thresholds[1].hard_fail_wall_time_micros, Some(5_000_000));
        assert!(thresholds[1].legacy_anchor.contains("~20s wall time"));
    }

    #[test]
    fn benchmark_recovery_report_evaluates_pass_fail_and_hard_fail() {
        let pass_report = evaluate_bd_wwqen_in_subquery_recovery(
            "test",
            vec![BenchmarkRecoveryMeasurement::latency_probe(
                BenchmarkRecoveryProbeId::InSubquery10kLatency,
                10_000,
                420,
                700,
                2_500.0,
            )],
        );
        assert_eq!(pass_report.assessments.len(), 1);
        assert_eq!(
            pass_report.assessments[0].status,
            BenchmarkRecoveryStatus::Passed
        );

        let fail_report = evaluate_bd_wwqen_in_subquery_recovery(
            "test",
            vec![BenchmarkRecoveryMeasurement::latency_probe(
                BenchmarkRecoveryProbeId::InSubquery10kLatency,
                10_000,
                900,
                1_100,
                1_000.0,
            )],
        );
        assert_eq!(fail_report.assessments.len(), 1);
        assert_eq!(
            fail_report.assessments[0].status,
            BenchmarkRecoveryStatus::Failed
        );
        assert!(
            fail_report.assessments[0]
                .findings
                .iter()
                .any(|finding| finding.contains("exceeds the 500us target"))
        );

        let hard_fail_report = evaluate_bd_wwqen_in_subquery_recovery(
            "test",
            vec![BenchmarkRecoveryMeasurement::wall_time_probe(
                BenchmarkRecoveryProbeId::InSubquery100kWallTime,
                100_000,
                6_000_000,
            )],
        );
        assert_eq!(hard_fail_report.assessments.len(), 1);
        assert_eq!(
            hard_fail_report.assessments[0].status,
            BenchmarkRecoveryStatus::HardFail
        );
        assert!(
            hard_fail_report.assessments[0]
                .findings
                .iter()
                .any(|finding| finding.contains("hard-fail ceiling"))
        );
    }

    #[test]
    fn benchmark_recovery_markdown_renders_contract_and_results() {
        let report = evaluate_bd_wwqen_in_subquery_recovery(
            "test",
            vec![
                BenchmarkRecoveryMeasurement::latency_probe(
                    BenchmarkRecoveryProbeId::InSubquery10kLatency,
                    10_000,
                    420,
                    700,
                    2_500.0,
                ),
                BenchmarkRecoveryMeasurement::wall_time_probe(
                    BenchmarkRecoveryProbeId::InSubquery100kWallTime,
                    100_000,
                    250_000,
                ),
            ],
        );
        let markdown = render_benchmark_recovery_markdown(&report);
        assert!(markdown.contains("# Benchmark Recovery Slice"));
        assert!(markdown.contains("in_subquery_10k_latency"));
        assert!(markdown.contains("in_subquery_100k_wall_time"));
        assert!(markdown.contains("PASS if p50 < 500us and p95 < 800us"));
        assert!(markdown.contains("PASS if wall < 200ms; HARD FAIL if wall > 5s"));
        assert!(markdown.contains("passed"));
        assert!(markdown.contains("failed"));
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
