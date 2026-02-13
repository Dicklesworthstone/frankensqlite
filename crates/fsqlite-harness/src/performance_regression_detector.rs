//! Noise-aware performance baseline and regression detector (bd-mblr.7.3.2).
//!
//! Provides:
//! - baseline sample storage helpers,
//! - robust baseline summarization (median + MAD),
//! - regression assessment with severity and confidence metadata,
//! - deterministic JSON report generation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Bead identifier for log/assert correlation.
pub const BEAD_ID: &str = "bd-mblr.7.3.2";
/// Schema version for baseline and report payloads.
pub const SCHEMA_VERSION: u32 = 1;

const MAD_SCALE: f64 = 1.4826;
const MIN_MAD_EPSILON: f64 = 1e-6;

/// Host/runtime context attached to samples and reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostContext {
    pub os: String,
    pub arch: String,
    pub cpu_model: String,
    pub rustc_version: String,
}

/// One benchmark run sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkSample {
    pub scenario_id: String,
    pub run_id: String,
    pub git_sha: String,
    pub seed: u64,
    pub p50_micros: f64,
    pub p95_micros: f64,
    pub p99_micros: f64,
    pub throughput_ops_per_sec: f64,
    pub host: HostContext,
    pub benchmark_params: BTreeMap<String, String>,
}

/// Robust baseline summary for one scenario.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineSummary {
    pub schema_version: u32,
    pub scenario_id: String,
    pub sample_count: usize,
    pub p50_median_micros: f64,
    pub p95_median_micros: f64,
    pub p99_median_micros: f64,
    pub throughput_median_ops_per_sec: f64,
    pub p95_mad: f64,
    pub throughput_mad: f64,
    pub git_shas: Vec<String>,
}

/// Severity classification for regression assessments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RegressionSeverity {
    None,
    Info,
    Warning,
    Critical,
}

impl fmt::Display for RegressionSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Info => f.write_str("info"),
            Self::Warning => f.write_str("warning"),
            Self::Critical => f.write_str("critical"),
        }
    }
}

/// Detection policy tolerances.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegressionTolerance {
    /// Warning threshold: candidate p95 / baseline p95 median.
    pub warning_latency_ratio: f64,
    /// Critical threshold: candidate p95 / baseline p95 median.
    pub critical_latency_ratio: f64,
    /// Warning threshold: 1 - candidate throughput / baseline throughput median.
    pub warning_throughput_drop_ratio: f64,
    /// Critical threshold: 1 - candidate throughput / baseline throughput median.
    pub critical_throughput_drop_ratio: f64,
}

impl Default for RegressionTolerance {
    fn default() -> Self {
        Self {
            warning_latency_ratio: 1.10,
            critical_latency_ratio: 1.25,
            warning_throughput_drop_ratio: 0.10,
            critical_throughput_drop_ratio: 0.20,
        }
    }
}

/// Assessment output for one candidate sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegressionAssessment {
    pub scenario_id: String,
    pub severity: RegressionSeverity,
    pub latency_ratio: f64,
    pub throughput_drop_ratio: f64,
    pub p95_z_score: f64,
    pub throughput_z_score: f64,
    pub confidence: f64,
    pub reasons: Vec<String>,
}

/// Full detector report payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegressionDetectionReport {
    pub schema_version: u32,
    pub bead_id: String,
    pub baseline: BaselineSummary,
    pub candidate: BenchmarkSample,
    pub assessment: RegressionAssessment,
}

/// Persist baseline samples as deterministic pretty JSON.
pub fn write_baseline_samples(path: &Path, samples: &[BenchmarkSample]) -> Result<(), String> {
    let payload = serde_json::to_vec_pretty(samples)
        .map_err(|error| format!("baseline_serialize_failed: {error}"))?;
    std::fs::write(path, payload).map_err(|error| {
        format!(
            "baseline_write_failed path={} error={error}",
            path.display()
        )
    })
}

/// Read baseline samples from JSON file.
pub fn load_baseline_samples(path: &Path) -> Result<Vec<BenchmarkSample>, String> {
    let payload = std::fs::read(path)
        .map_err(|error| format!("baseline_read_failed path={} error={error}", path.display()))?;
    serde_json::from_slice::<Vec<BenchmarkSample>>(&payload).map_err(|error| {
        format!(
            "baseline_parse_failed path={} error={error}",
            path.display()
        )
    })
}

/// Write one detector report as deterministic pretty JSON.
pub fn write_detection_report(
    path: &Path,
    report: &RegressionDetectionReport,
) -> Result<(), String> {
    let payload = serde_json::to_vec_pretty(report)
        .map_err(|error| format!("report_serialize_failed: {error}"))?;
    std::fs::write(path, payload)
        .map_err(|error| format!("report_write_failed path={} error={error}", path.display()))
}

/// Build a baseline summary for one scenario from samples.
pub fn build_baseline_summary(
    scenario_id: &str,
    samples: &[BenchmarkSample],
) -> Result<BaselineSummary, String> {
    if scenario_id.trim().is_empty() {
        return Err("scenario_id must not be empty".to_owned());
    }
    if samples.is_empty() {
        return Err("baseline samples must not be empty".to_owned());
    }

    let mut selected = Vec::new();
    for sample in samples {
        validate_sample(sample)?;
        if sample.scenario_id == scenario_id {
            selected.push(sample);
        }
    }
    if selected.is_empty() {
        return Err(format!("no baseline samples for scenario_id={scenario_id}"));
    }

    let mut p50_values: Vec<f64> = selected.iter().map(|sample| sample.p50_micros).collect();
    let mut p95_values: Vec<f64> = selected.iter().map(|sample| sample.p95_micros).collect();
    let mut p99_values: Vec<f64> = selected.iter().map(|sample| sample.p99_micros).collect();
    let mut throughput_values: Vec<f64> = selected
        .iter()
        .map(|sample| sample.throughput_ops_per_sec)
        .collect();

    let p50_median = median(&mut p50_values)?;
    let p95_median = median(&mut p95_values)?;
    let p99_median = median(&mut p99_values)?;
    let throughput_median = median(&mut throughput_values)?;

    let p95_mad = mad(&p95_values, p95_median)?;
    let throughput_mad = mad(&throughput_values, throughput_median)?;

    let mut git_shas: BTreeSet<String> = BTreeSet::new();
    for sample in selected {
        git_shas.insert(sample.git_sha.clone());
    }

    Ok(BaselineSummary {
        schema_version: SCHEMA_VERSION,
        scenario_id: scenario_id.to_owned(),
        sample_count: p95_values.len(),
        p50_median_micros: p50_median,
        p95_median_micros: p95_median,
        p99_median_micros: p99_median,
        throughput_median_ops_per_sec: throughput_median,
        p95_mad,
        throughput_mad,
        git_shas: git_shas.into_iter().collect(),
    })
}

/// Detect regression by comparing candidate metrics against robust baseline.
pub fn detect_regression(
    baseline: &BaselineSummary,
    candidate: &BenchmarkSample,
    tolerance: &RegressionTolerance,
) -> Result<RegressionAssessment, String> {
    validate_sample(candidate)?;
    validate_tolerance(tolerance)?;

    if baseline.scenario_id != candidate.scenario_id {
        return Err(format!(
            "scenario mismatch baseline={} candidate={}",
            baseline.scenario_id, candidate.scenario_id
        ));
    }

    let latency_ratio = candidate.p95_micros / baseline.p95_median_micros;
    let throughput_ratio =
        candidate.throughput_ops_per_sec / baseline.throughput_median_ops_per_sec;
    let throughput_drop_ratio = (1.0 - throughput_ratio).max(0.0);

    let p95_z_score = robust_z_score(
        candidate.p95_micros,
        baseline.p95_median_micros,
        baseline.p95_mad,
    );
    let throughput_z_score = robust_z_score(
        baseline.throughput_median_ops_per_sec - candidate.throughput_ops_per_sec,
        0.0,
        baseline.throughput_mad,
    );
    let confidence = baseline_confidence(baseline);

    let mut reasons = Vec::new();
    let mut severity = RegressionSeverity::None;

    if latency_ratio >= tolerance.critical_latency_ratio {
        severity = RegressionSeverity::Critical;
        reasons.push(format!(
            "latency ratio {:.4} >= critical threshold {:.4}",
            latency_ratio, tolerance.critical_latency_ratio
        ));
    } else if latency_ratio >= tolerance.warning_latency_ratio {
        severity = RegressionSeverity::Warning;
        reasons.push(format!(
            "latency ratio {:.4} >= warning threshold {:.4}",
            latency_ratio, tolerance.warning_latency_ratio
        ));
    }

    if throughput_drop_ratio >= tolerance.critical_throughput_drop_ratio {
        severity = RegressionSeverity::Critical;
        reasons.push(format!(
            "throughput drop {:.4} >= critical threshold {:.4}",
            throughput_drop_ratio, tolerance.critical_throughput_drop_ratio
        ));
    } else if throughput_drop_ratio >= tolerance.warning_throughput_drop_ratio
        && severity < RegressionSeverity::Warning
    {
        severity = RegressionSeverity::Warning;
        reasons.push(format!(
            "throughput drop {:.4} >= warning threshold {:.4}",
            throughput_drop_ratio, tolerance.warning_throughput_drop_ratio
        ));
    }

    if severity == RegressionSeverity::None
        && confidence >= 0.50
        && (p95_z_score >= 3.0 || throughput_z_score >= 3.0)
    {
        severity = RegressionSeverity::Info;
        reasons.push(format!(
            "high-z-score anomaly p95_z={:.4} throughput_z={:.4} (confidence {:.3})",
            p95_z_score, throughput_z_score, confidence
        ));
    }

    Ok(RegressionAssessment {
        scenario_id: baseline.scenario_id.clone(),
        severity,
        latency_ratio,
        throughput_drop_ratio,
        p95_z_score,
        throughput_z_score,
        confidence,
        reasons,
    })
}

/// Build full report directly from baseline samples + candidate sample.
pub fn evaluate_candidate_against_baseline(
    baseline_samples: &[BenchmarkSample],
    candidate: &BenchmarkSample,
    tolerance: &RegressionTolerance,
) -> Result<RegressionDetectionReport, String> {
    let baseline = build_baseline_summary(&candidate.scenario_id, baseline_samples)?;
    let assessment = detect_regression(&baseline, candidate, tolerance)?;
    Ok(RegressionDetectionReport {
        schema_version: SCHEMA_VERSION,
        bead_id: BEAD_ID.to_owned(),
        baseline,
        candidate: candidate.clone(),
        assessment,
    })
}

fn validate_tolerance(tolerance: &RegressionTolerance) -> Result<(), String> {
    if !tolerance.warning_latency_ratio.is_finite()
        || !tolerance.critical_latency_ratio.is_finite()
        || tolerance.warning_latency_ratio <= 0.0
        || tolerance.critical_latency_ratio <= 0.0
        || tolerance.warning_latency_ratio > tolerance.critical_latency_ratio
    {
        return Err("invalid latency tolerance thresholds".to_owned());
    }
    if !tolerance.warning_throughput_drop_ratio.is_finite()
        || !tolerance.critical_throughput_drop_ratio.is_finite()
        || tolerance.warning_throughput_drop_ratio < 0.0
        || tolerance.critical_throughput_drop_ratio < 0.0
        || tolerance.warning_throughput_drop_ratio > tolerance.critical_throughput_drop_ratio
    {
        return Err("invalid throughput-drop tolerance thresholds".to_owned());
    }
    Ok(())
}

fn validate_sample(sample: &BenchmarkSample) -> Result<(), String> {
    if sample.scenario_id.trim().is_empty() {
        return Err("sample.scenario_id must not be empty".to_owned());
    }
    if sample.run_id.trim().is_empty() {
        return Err("sample.run_id must not be empty".to_owned());
    }
    if sample.git_sha.trim().is_empty() {
        return Err("sample.git_sha must not be empty".to_owned());
    }
    for metric in [
        sample.p50_micros,
        sample.p95_micros,
        sample.p99_micros,
        sample.throughput_ops_per_sec,
    ] {
        if !metric.is_finite() || metric <= 0.0 {
            return Err("sample metrics must be finite and > 0".to_owned());
        }
    }
    if sample.p50_micros > sample.p95_micros || sample.p95_micros > sample.p99_micros {
        return Err("sample latency ordering must satisfy p50 <= p95 <= p99".to_owned());
    }
    Ok(())
}

fn median(values: &mut [f64]) -> Result<f64, String> {
    if values.is_empty() {
        return Err("median requires non-empty input".to_owned());
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Ok(f64::midpoint(values[mid - 1], values[mid]))
    } else {
        Ok(values[mid])
    }
}

fn mad(values: &[f64], center: f64) -> Result<f64, String> {
    if values.is_empty() {
        return Err("mad requires non-empty input".to_owned());
    }
    let mut residuals: Vec<f64> = values.iter().map(|value| (*value - center).abs()).collect();
    median(&mut residuals).map(|value| value.max(MIN_MAD_EPSILON))
}

fn robust_z_score(value: f64, median_value: f64, mad_value: f64) -> f64 {
    (value - median_value).abs() / (MAD_SCALE * mad_value.max(MIN_MAD_EPSILON))
}

fn baseline_confidence(baseline: &BaselineSummary) -> f64 {
    let sample_factor = (baseline.sample_count.min(20) as f64) / 20.0;
    let latency_noise = baseline.p95_mad / baseline.p95_median_micros.max(MIN_MAD_EPSILON);
    let throughput_noise =
        baseline.throughput_mad / baseline.throughput_median_ops_per_sec.max(MIN_MAD_EPSILON);
    let noise_factor = 1.0 / (1.0 + latency_noise + throughput_noise);
    (sample_factor * noise_factor).clamp(0.0, 1.0)
}
