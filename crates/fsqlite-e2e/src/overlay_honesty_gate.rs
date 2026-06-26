#![allow(
    clippy::derive_partial_eq_without_eq,
    clippy::derivable_impls,
    clippy::zero_sized_map_values
)]
// Pedantic-only style preferences inside this module's gate-policy types.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::benchmark::{
    BenchmarkCausalScorecard, BenchmarkCausalScorecardReport, BenchmarkSummary,
    build_benchmark_causal_scorecard_report,
};

pub const MATRIX_REGRESSION_GATE_SCHEMA_V2: &str =
    "fsqlite-e2e.complete_benchmark_matrix_regression_gate.v2";
pub const OVERLAY_HONESTY_GATE_SCHEMA_V1: &str = "fsqlite-e2e.overlay_honesty_gate.v1";
pub const BENCHMARK_HONEST_GATE_SCHEMA_V1: &str = "fsqlite-e2e.benchmark_honest_gate.v1";

pub const DEFAULT_MAX_P95_RATIO: f64 = 1.25;
pub const DEFAULT_MIN_THROUGHPUT_RATIO: f64 = 0.80;
pub const DEFAULT_HEALTHY_MARGIN_MIN: f64 = 1.10;

pub const MATRIX_MAX_P95_RATIO_ENV: &str = "FSQLITE_MATRIX_MAX_P95_RATIO";
pub const MATRIX_MIN_THROUGHPUT_RATIO_ENV: &str = "FSQLITE_MATRIX_MIN_THROUGHPUT_RATIO";
pub const OVERLAY_C1_SCORECARD_JSON_ENV: &str = "FSQLITE_OVERLAY_C1_SCORECARD_JSON";
pub const OVERLAY_PERSISTENT_SCORECARD_JSON_ENV: &str = "FSQLITE_OVERLAY_PERSISTENT_SCORECARD_JSON";
pub const OVERLAY_ENFORCE_HONESTY_GATE_ENV: &str = "FSQLITE_OVERLAY_ENFORCE_HONESTY_GATE";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlayGateVerdict {
    Pass,
    Warning,
    Fail,
    Incomplete,
    NoData,
}

impl OverlayGateVerdict {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Pass => 0,
            Self::Warning => 1,
            Self::Fail => 2,
            Self::Incomplete => 3,
            Self::NoData => 4,
        }
    }

    #[must_use]
    pub const fn worse(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }

    #[must_use]
    pub const fn is_green(self) -> bool {
        matches!(self, Self::Pass)
    }
}

impl fmt::Display for OverlayGateVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => f.write_str("pass"),
            Self::Warning => f.write_str("warning"),
            Self::Fail => f.write_str("fail"),
            Self::Incomplete => f.write_str("incomplete"),
            Self::NoData => f.write_str("no_data"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkHonestGateClassification {
    MissingBaseline,
    BelowParity,
    TailSlowerThanSqlite,
    ParityToMargin,
    HealthyMargin,
}

impl BenchmarkHonestGateClassification {
    #[must_use]
    pub const fn is_red(self) -> bool {
        matches!(self, Self::BelowParity | Self::TailSlowerThanSqlite)
    }
}

impl fmt::Display for BenchmarkHonestGateClassification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBaseline => f.write_str("missing_baseline"),
            Self::BelowParity => f.write_str("below_parity"),
            Self::TailSlowerThanSqlite => f.write_str("tail_slower_than_sqlite"),
            Self::ParityToMargin => f.write_str("parity_to_margin"),
            Self::HealthyMargin => f.write_str("healthy_margin"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MatrixRegressionThresholds {
    pub max_p95_ratio: f64,
    pub min_throughput_ratio: f64,
}

impl Default for MatrixRegressionThresholds {
    fn default() -> Self {
        Self {
            max_p95_ratio: DEFAULT_MAX_P95_RATIO,
            min_throughput_ratio: DEFAULT_MIN_THROUGHPUT_RATIO,
        }
    }
}

impl MatrixRegressionThresholds {
    /// # Errors
    ///
    /// Returns an error if the environment overrides are invalid.
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            max_p95_ratio: parse_ratio_env(MATRIX_MAX_P95_RATIO_ENV, DEFAULT_MAX_P95_RATIO)?,
            min_throughput_ratio: parse_ratio_env(
                MATRIX_MIN_THROUGHPUT_RATIO_ENV,
                DEFAULT_MIN_THROUGHPUT_RATIO,
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BenchmarkCellKey {
    pub benchmark_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<String>,
    pub fixture_id: String,
    pub workload: String,
    pub concurrency: u16,
    pub mode_id: String,
}

impl fmt::Display for BenchmarkCellKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref row_id) = self.row_id {
            write!(
                f,
                "{row_id} [{}:{}:c{}:{}]",
                self.fixture_id, self.workload, self.concurrency, self.mode_id
            )
        } else {
            write!(
                f,
                "{} [{}:{}:c{}:{}]",
                self.benchmark_id, self.fixture_id, self.workload, self.concurrency, self.mode_id
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixRegressionCheck {
    pub cell_key: BenchmarkCellKey,
    pub baseline_p95_ms: f64,
    pub current_p95_ms: f64,
    pub p95_ratio: f64,
    pub baseline_throughput_ops_per_sec: f64,
    pub current_throughput_ops_per_sec: f64,
    pub throughput_ratio: f64,
    pub passed: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixRegressionGateReport {
    pub schema_version: String,
    pub baseline_label: String,
    pub current_label: String,
    pub thresholds: MatrixRegressionThresholds,
    pub compared_cells: usize,
    pub missing_baseline_cells: Vec<BenchmarkCellKey>,
    pub failing_cells: Vec<BenchmarkCellKey>,
    pub checks: Vec<MatrixRegressionCheck>,
}

impl MatrixRegressionGateReport {
    #[must_use]
    pub fn failure_summary(&self) -> Option<String> {
        if self.missing_baseline_cells.is_empty() && self.failing_cells.is_empty() {
            return None;
        }

        let mut segments = Vec::new();
        if !self.missing_baseline_cells.is_empty() {
            segments.push(format!(
                "missing baseline cells: {}",
                self.missing_baseline_cells
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !self.failing_cells.is_empty() {
            segments.push(format!(
                "regressed cells: {}",
                self.failing_cells
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        Some(segments.join(" | "))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayCausalScorecardDigest {
    pub benchmark_id: String,
    pub row_identity: BenchmarkCellKey,
    pub baseline_comparator: String,
    pub claim_summary: String,
    pub interpretation_note: String,
    pub optimization_families: Vec<String>,
    pub negative_findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OverlayMatrixFinding {
    pub cell_key: BenchmarkCellKey,
    pub reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_scorecard: Option<OverlayCausalScorecardDigest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OverlayHonestyGateConfig {
    pub matrix_thresholds: MatrixRegressionThresholds,
    pub require_c1_pack: bool,
    pub require_persistent_pack: bool,
    pub fail_on_warning: bool,
}

impl Default for OverlayHonestyGateConfig {
    fn default() -> Self {
        Self {
            matrix_thresholds: MatrixRegressionThresholds::default(),
            require_c1_pack: false,
            require_persistent_pack: false,
            fail_on_warning: false,
        }
    }
}

impl OverlayHonestyGateConfig {
    #[must_use]
    pub fn strict_overlay() -> Self {
        Self {
            matrix_thresholds: MatrixRegressionThresholds::default(),
            require_c1_pack: true,
            require_persistent_pack: true,
            fail_on_warning: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1ComparatorContract {
    #[serde(default)]
    pub aggregate_rows_are_secondary: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1HonestGateSummary {
    pub verdict: OverlayGateVerdict,
    pub expected_critical_cell_count: usize,
    pub critical_cell_count: usize,
    pub comparable_cell_count: usize,
    pub missing_baseline_count: usize,
    pub below_parity_count: usize,
    pub parity_to_margin_count: usize,
    pub healthy_margin_count: usize,
    #[serde(default)]
    pub hard_fail_when_below_parity_present: bool,
    #[serde(default)]
    pub critical_red_cell_ids: Vec<String>,
    #[serde(default)]
    pub margin_band_cell_ids: Vec<String>,
    #[serde(default)]
    pub missing_baseline_row_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1EvidencePackRow {
    pub row_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub mode_id: String,
    #[serde(default)]
    pub mode_label: String,
    #[serde(default)]
    pub speedup_vs_sqlite: Option<f64>,
    #[serde(default)]
    pub classification: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1ModeRollup {
    pub mode_id: String,
    #[serde(default)]
    pub geometric_mean_speedup: Option<f64>,
    pub comparable_cell_count: usize,
    pub below_parity: usize,
    pub parity_to_margin: usize,
    pub healthy_margin: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1WorkloadRollup {
    pub mode_id: String,
    pub workload: String,
    #[serde(default)]
    pub geometric_mean_speedup: Option<f64>,
    pub comparable_cell_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1EvidencePackScorecard {
    pub schema_version: String,
    pub run_id: String,
    pub healthy_margin_min: f64,
    pub concurrency: u16,
    #[serde(default)]
    pub pack_role: Option<String>,
    #[serde(default)]
    pub fixtures: Vec<String>,
    #[serde(default)]
    pub workloads: Vec<String>,
    #[serde(default)]
    pub rows: Vec<C1EvidencePackRow>,
    #[serde(default)]
    pub mode_rollup: Vec<C1ModeRollup>,
    #[serde(default)]
    pub workload_rollup: Vec<C1WorkloadRollup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparator_contract: Option<C1ComparatorContract>,
    pub honest_gate_summary: C1HonestGateSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentComparatorContract {
    #[serde(default)]
    pub aggregate_rows_are_secondary: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentHonestGateSummary {
    pub verdict: OverlayGateVerdict,
    pub critical_regime_count: usize,
    pub complete_regime_count: usize,
    pub incomplete_regime_count: usize,
    pub no_data_regime_count: usize,
    #[serde(default)]
    pub red_regimes: Vec<String>,
    #[serde(default)]
    pub incomplete_regimes: Vec<String>,
    #[serde(default)]
    pub no_data_regimes: Vec<String>,
    #[serde(default)]
    pub rule: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentCriticalRegime {
    pub regime_id: String,
    pub verdict: String,
    pub coverage_state: String,
    #[serde(default)]
    pub critical_surface_primary: bool,
    #[serde(default)]
    pub throughput_ratio_vs_sqlite: Option<f64>,
    #[serde(default)]
    pub throughput_band: Option<String>,
    #[serde(default)]
    pub collapse_override_applies: bool,
    #[serde(default)]
    pub measured_reasons: Vec<String>,
    #[serde(default)]
    pub missing_artifacts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentPhasePackScorecard {
    pub schema_version: String,
    pub run_id: String,
    pub entrypoint: String,
    pub healthy_margin_min: f64,
    #[serde(default)]
    pub aggregate_views_secondary_only: bool,
    #[serde(default)]
    pub critical_surface_primary: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparator_contract: Option<PersistentComparatorContract>,
    pub honest_gate_summary: PersistentHonestGateSummary,
    #[serde(default)]
    pub critical_regimes: Vec<PersistentCriticalRegime>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct C1PackAssessment {
    pub present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<OverlayGateVerdict>,
    #[serde(default)]
    pub red_cell_ids: Vec<String>,
    #[serde(default)]
    pub margin_cell_ids: Vec<String>,
    #[serde(default)]
    pub missing_baseline_row_ids: Vec<String>,
    #[serde(default)]
    pub no_fake_win_findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentRegimeAssessment {
    pub regime_id: String,
    pub verdict: String,
    pub coverage_state: String,
    #[serde(default)]
    pub throughput_ratio_vs_sqlite: Option<f64>,
    #[serde(default)]
    pub collapse_override_applies: bool,
    #[serde(default)]
    pub measured_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentPackAssessment {
    pub present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<OverlayGateVerdict>,
    #[serde(default)]
    pub red_regimes: Vec<String>,
    #[serde(default)]
    pub incomplete_regimes: Vec<String>,
    #[serde(default)]
    pub no_data_regimes: Vec<String>,
    #[serde(default)]
    pub missing_required_regimes: Vec<String>,
    #[serde(default)]
    pub critical_regimes: Vec<PersistentRegimeAssessment>,
    #[serde(default)]
    pub no_fake_win_findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OverlayHonestyGateReport {
    pub schema_version: String,
    pub config: OverlayHonestyGateConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix_regression: Option<MatrixRegressionGateReport>,
    pub causal_scorecards: BenchmarkCausalScorecardReport,
    #[serde(default)]
    pub matrix_findings: Vec<OverlayMatrixFinding>,
    pub c1_pack: C1PackAssessment,
    pub persistent_pack: PersistentPackAssessment,
    pub overall_verdict: OverlayGateVerdict,
    pub ci_blocking: bool,
    pub blocking_findings: Vec<String>,
    pub warning_findings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkHonestGateRow {
    pub surface_id: String,
    pub cell_key: BenchmarkCellKey,
    pub comparator_state: String,
    pub classification: BenchmarkHonestGateClassification,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_ratio_vs_sqlite: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_latency_ratio_vs_sqlite: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_latency_ratio_vs_sqlite: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_latency_ratio_vs_sqlite: Option<f64>,
    pub median_ops_per_sec: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite_median_ops_per_sec: Option<f64>,
    pub median_latency_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite_median_latency_ms: Option<f64>,
    pub retries_total: u64,
    pub aborts_total: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkHonestGateSurface {
    pub surface_id: String,
    pub description: String,
    pub verdict: OverlayGateVerdict,
    pub expected_critical_row_count: usize,
    pub critical_row_count: usize,
    pub comparable_row_count: usize,
    pub missing_baseline_count: usize,
    pub below_parity_count: usize,
    pub tail_slower_than_sqlite_count: usize,
    pub parity_to_margin_count: usize,
    pub healthy_margin_count: usize,
    #[serde(default)]
    pub critical_red_row_ids: Vec<String>,
    #[serde(default)]
    pub margin_band_row_ids: Vec<String>,
    #[serde(default)]
    pub missing_baseline_row_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkHonestGateSummary {
    pub verdict: OverlayGateVerdict,
    pub expected_critical_row_count: usize,
    pub critical_row_count: usize,
    pub comparable_row_count: usize,
    pub missing_baseline_count: usize,
    pub below_parity_count: usize,
    pub tail_slower_than_sqlite_count: usize,
    pub parity_to_margin_count: usize,
    pub healthy_margin_count: usize,
    #[serde(default)]
    pub hard_fail_when_red_row_present: bool,
    #[serde(default)]
    pub critical_red_row_ids: Vec<String>,
    #[serde(default)]
    pub margin_band_row_ids: Vec<String>,
    #[serde(default)]
    pub missing_baseline_row_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkHonestGateReport {
    pub schema_version: String,
    pub healthy_margin_min: f64,
    #[serde(default)]
    pub aggregate_rows_are_secondary: bool,
    #[serde(default)]
    pub critical_surface_primary: bool,
    pub honest_gate_summary: BenchmarkHonestGateSummary,
    #[serde(default)]
    pub surfaces: Vec<BenchmarkHonestGateSurface>,
    #[serde(default)]
    pub rows: Vec<BenchmarkHonestGateRow>,
}

impl OverlayHonestyGateReport {
    #[must_use]
    pub fn failure_summary(&self) -> Option<String> {
        if !self.ci_blocking {
            return None;
        }
        Some(self.blocking_findings.join(" | "))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BenchmarkHonestGateSurfaceKind {
    C1FixedTax,
    PersistentConcurrentWrite8,
    PersistentConcurrentWrite16,
}

impl BenchmarkHonestGateSurfaceKind {
    #[must_use]
    const fn id(self) -> &'static str {
        match self {
            Self::C1FixedTax => "c1_fixed_tax",
            Self::PersistentConcurrentWrite8 => "persistent_concurrent_write_8t",
            Self::PersistentConcurrentWrite16 => "persistent_concurrent_write_16t",
        }
    }

    #[must_use]
    const fn description(self) -> &'static str {
        match self {
            Self::C1FixedTax => {
                "Every c1 MVCC and forced single-writer row is critical; aggregate rollups are secondary."
            }
            Self::PersistentConcurrentWrite8 => {
                "Persistent concurrent-write c8 rows stay individually visible; aggregate rollups are secondary."
            }
            Self::PersistentConcurrentWrite16 => {
                "Persistent concurrent-write c16 rows stay individually visible; aggregate rollups are secondary."
            }
        }
    }
}

/// Build the row-level honest-gate surface for benchmark summaries so c1 and
/// persistent high-thread failures remain visible even when aggregate tables
/// look friendly.
#[must_use]
pub fn build_benchmark_honest_gate_report(
    summaries: &[BenchmarkSummary],
) -> Option<BenchmarkHonestGateReport> {
    let grouped = summaries.iter().fold(BTreeMap::new(), |mut acc, summary| {
        acc.entry((
            summary.fixture_id.clone(),
            summary.workload.clone(),
            summary.concurrency,
        ))
        .or_insert_with(Vec::new)
        .push(summary);
        acc
    });

    let expected_by_surface = expected_critical_rows_by_surface(summaries);
    if expected_by_surface.values().all(|count| *count == 0) {
        return None;
    }

    let mut rows = summaries
        .iter()
        .filter_map(|summary| {
            let surface = critical_surface_for_summary(summary)?;
            let comparator = grouped
                .get(&(
                    summary.fixture_id.clone(),
                    summary.workload.clone(),
                    summary.concurrency,
                ))
                .and_then(|group| {
                    group.iter().copied().find(|candidate| {
                        matches!(
                            candidate.comparison_mode_id(),
                            "sqlite_reference" | "sqlite3"
                        )
                    })
                });
            Some(build_honest_gate_row(summary, comparator, surface))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        left.surface_id
            .cmp(&right.surface_id)
            .then_with(|| left.cell_key.fixture_id.cmp(&right.cell_key.fixture_id))
            .then_with(|| left.cell_key.workload.cmp(&right.cell_key.workload))
            .then_with(|| left.cell_key.concurrency.cmp(&right.cell_key.concurrency))
            .then_with(|| left.cell_key.mode_id.cmp(&right.cell_key.mode_id))
    });

    let surfaces = expected_by_surface
        .into_iter()
        .filter_map(|(surface, expected_critical_row_count)| {
            if expected_critical_row_count == 0 {
                return None;
            }
            let surface_id = surface.id().to_owned();
            let surface_rows = rows
                .iter()
                .filter(|row| row.surface_id == surface_id)
                .collect::<Vec<_>>();
            Some(build_honest_gate_surface(
                surface,
                expected_critical_row_count,
                &surface_rows,
            ))
        })
        .collect::<Vec<_>>();

    let honest_gate_summary = build_honest_gate_summary(&surfaces);

    Some(BenchmarkHonestGateReport {
        schema_version: BENCHMARK_HONEST_GATE_SCHEMA_V1.to_owned(),
        healthy_margin_min: DEFAULT_HEALTHY_MARGIN_MIN,
        aggregate_rows_are_secondary: true,
        critical_surface_primary: true,
        honest_gate_summary,
        surfaces,
        rows,
    })
}

fn expected_critical_rows_by_surface(
    summaries: &[BenchmarkSummary],
) -> BTreeMap<BenchmarkHonestGateSurfaceKind, usize> {
    let mut c1_groups = BTreeMap::new();
    let mut persistent8_groups = BTreeMap::new();
    let mut persistent16_groups = BTreeMap::new();

    for summary in summaries {
        if summary.concurrency == 1 {
            c1_groups.insert((summary.fixture_id.clone(), summary.workload.clone()), ());
        }
        if is_persistent_high_thread(summary) {
            let key = (
                summary.fixture_id.clone(),
                summary.workload.clone(),
                summary.concurrency,
            );
            if summary.concurrency == 8 {
                persistent8_groups.insert(key, ());
            } else if summary.concurrency == 16 {
                persistent16_groups.insert(key, ());
            }
        }
    }

    let mut expected = BTreeMap::new();
    expected.insert(
        BenchmarkHonestGateSurfaceKind::C1FixedTax,
        c1_groups.len() * 2,
    );
    expected.insert(
        BenchmarkHonestGateSurfaceKind::PersistentConcurrentWrite8,
        persistent8_groups.len(),
    );
    expected.insert(
        BenchmarkHonestGateSurfaceKind::PersistentConcurrentWrite16,
        persistent16_groups.len(),
    );
    expected
}

fn critical_surface_for_summary(
    summary: &BenchmarkSummary,
) -> Option<BenchmarkHonestGateSurfaceKind> {
    match (summary.concurrency, summary.comparison_mode_id()) {
        (1, "fsqlite_mvcc" | "fsqlite_single_writer") => {
            Some(BenchmarkHonestGateSurfaceKind::C1FixedTax)
        }
        (8, "fsqlite_mvcc") if is_persistent_high_thread(summary) => {
            Some(BenchmarkHonestGateSurfaceKind::PersistentConcurrentWrite8)
        }
        (16, "fsqlite_mvcc") if is_persistent_high_thread(summary) => {
            Some(BenchmarkHonestGateSurfaceKind::PersistentConcurrentWrite16)
        }
        _ => None,
    }
}

fn is_persistent_high_thread(summary: &BenchmarkSummary) -> bool {
    matches!(summary.concurrency, 8 | 16)
        && summary.workload.contains("persistent_concurrent_write")
}

fn build_honest_gate_row(
    summary: &BenchmarkSummary,
    comparator: Option<&BenchmarkSummary>,
    surface: BenchmarkHonestGateSurfaceKind,
) -> BenchmarkHonestGateRow {
    let throughput_ratio = comparator.and_then(|baseline| {
        ratio(
            summary.throughput.median_ops_per_sec,
            baseline.throughput.median_ops_per_sec,
        )
    });
    let median_latency_ratio = comparator
        .and_then(|baseline| ratio(summary.latency.median_ms, baseline.latency.median_ms));
    let p95_latency_ratio =
        comparator.and_then(|baseline| ratio(summary.latency.p95_ms, baseline.latency.p95_ms));
    let p99_latency_ratio =
        comparator.and_then(|baseline| ratio(summary.latency.p99_ms, baseline.latency.p99_ms));

    let classification = if comparator.is_none() {
        BenchmarkHonestGateClassification::MissingBaseline
    } else if throughput_ratio.is_some_and(|value| value < 1.0) {
        BenchmarkHonestGateClassification::BelowParity
    } else if matches!(
        surface,
        BenchmarkHonestGateSurfaceKind::PersistentConcurrentWrite8
            | BenchmarkHonestGateSurfaceKind::PersistentConcurrentWrite16
    ) && (p95_latency_ratio.is_some_and(|value| value > 1.0)
        || p99_latency_ratio.is_some_and(|value| value > 1.0))
    {
        BenchmarkHonestGateClassification::TailSlowerThanSqlite
    } else if throughput_ratio.is_some_and(|value| value < DEFAULT_HEALTHY_MARGIN_MIN) {
        BenchmarkHonestGateClassification::ParityToMargin
    } else {
        BenchmarkHonestGateClassification::HealthyMargin
    };

    BenchmarkHonestGateRow {
        surface_id: surface.id().to_owned(),
        cell_key: benchmark_cell_key(summary),
        comparator_state: if comparator.is_some() {
            "same_group_sqlite_reference_available".to_owned()
        } else {
            "missing_sqlite_reference".to_owned()
        },
        classification,
        throughput_ratio_vs_sqlite: throughput_ratio,
        median_latency_ratio_vs_sqlite: median_latency_ratio,
        p95_latency_ratio_vs_sqlite: p95_latency_ratio,
        p99_latency_ratio_vs_sqlite: p99_latency_ratio,
        median_ops_per_sec: summary.throughput.median_ops_per_sec,
        sqlite_median_ops_per_sec: comparator
            .map(|baseline| baseline.throughput.median_ops_per_sec),
        median_latency_ms: summary.latency.median_ms,
        sqlite_median_latency_ms: comparator.map(|baseline| baseline.latency.median_ms),
        retries_total: summary.total_iteration_retries(),
        aborts_total: summary.total_iteration_aborts(),
    }
}

fn build_honest_gate_surface(
    surface: BenchmarkHonestGateSurfaceKind,
    expected_critical_row_count: usize,
    rows: &[&BenchmarkHonestGateRow],
) -> BenchmarkHonestGateSurface {
    let comparable_row_count = rows
        .iter()
        .filter(|row| row.throughput_ratio_vs_sqlite.is_some())
        .count();
    let missing_baseline_row_ids = rows
        .iter()
        .filter(|row| {
            matches!(
                row.classification,
                BenchmarkHonestGateClassification::MissingBaseline
            )
        })
        .map(|row| row.cell_key.to_string())
        .collect::<Vec<_>>();
    let critical_red_row_ids = rows
        .iter()
        .filter(|row| row.classification.is_red())
        .map(|row| row.cell_key.to_string())
        .collect::<Vec<_>>();
    let margin_band_row_ids = rows
        .iter()
        .filter(|row| {
            matches!(
                row.classification,
                BenchmarkHonestGateClassification::ParityToMargin
            )
        })
        .map(|row| row.cell_key.to_string())
        .collect::<Vec<_>>();
    let below_parity_count = rows
        .iter()
        .filter(|row| {
            matches!(
                row.classification,
                BenchmarkHonestGateClassification::BelowParity
            )
        })
        .count();
    let tail_slower_than_sqlite_count = rows
        .iter()
        .filter(|row| {
            matches!(
                row.classification,
                BenchmarkHonestGateClassification::TailSlowerThanSqlite
            )
        })
        .count();
    let parity_to_margin_count = margin_band_row_ids.len();
    let healthy_margin_count = rows
        .iter()
        .filter(|row| {
            matches!(
                row.classification,
                BenchmarkHonestGateClassification::HealthyMargin
            )
        })
        .count();
    let missing_baseline_count = missing_baseline_row_ids.len();
    let critical_row_count = rows.len();

    let verdict = if comparable_row_count == 0 {
        OverlayGateVerdict::NoData
    } else if critical_row_count < expected_critical_row_count || missing_baseline_count > 0 {
        OverlayGateVerdict::Incomplete
    } else if !critical_red_row_ids.is_empty() {
        OverlayGateVerdict::Fail
    } else if parity_to_margin_count > 0 {
        OverlayGateVerdict::Warning
    } else {
        OverlayGateVerdict::Pass
    };

    BenchmarkHonestGateSurface {
        surface_id: surface.id().to_owned(),
        description: surface.description().to_owned(),
        verdict,
        expected_critical_row_count,
        critical_row_count,
        comparable_row_count,
        missing_baseline_count,
        below_parity_count,
        tail_slower_than_sqlite_count,
        parity_to_margin_count,
        healthy_margin_count,
        critical_red_row_ids,
        margin_band_row_ids,
        missing_baseline_row_ids,
    }
}

fn build_honest_gate_summary(
    surfaces: &[BenchmarkHonestGateSurface],
) -> BenchmarkHonestGateSummary {
    let expected_critical_row_count = surfaces
        .iter()
        .map(|surface| surface.expected_critical_row_count)
        .sum();
    let critical_row_count = surfaces
        .iter()
        .map(|surface| surface.critical_row_count)
        .sum();
    let comparable_row_count = surfaces
        .iter()
        .map(|surface| surface.comparable_row_count)
        .sum();
    let missing_baseline_count = surfaces
        .iter()
        .map(|surface| surface.missing_baseline_count)
        .sum();
    let below_parity_count = surfaces
        .iter()
        .map(|surface| surface.below_parity_count)
        .sum();
    let tail_slower_than_sqlite_count = surfaces
        .iter()
        .map(|surface| surface.tail_slower_than_sqlite_count)
        .sum();
    let parity_to_margin_count = surfaces
        .iter()
        .map(|surface| surface.parity_to_margin_count)
        .sum();
    let healthy_margin_count = surfaces
        .iter()
        .map(|surface| surface.healthy_margin_count)
        .sum();
    let critical_red_row_ids = surfaces
        .iter()
        .flat_map(|surface| surface.critical_red_row_ids.iter().cloned())
        .collect::<Vec<_>>();
    let margin_band_row_ids = surfaces
        .iter()
        .flat_map(|surface| surface.margin_band_row_ids.iter().cloned())
        .collect::<Vec<_>>();
    let missing_baseline_row_ids = surfaces
        .iter()
        .flat_map(|surface| surface.missing_baseline_row_ids.iter().cloned())
        .collect::<Vec<_>>();

    let verdict = if comparable_row_count == 0 {
        OverlayGateVerdict::NoData
    } else if critical_row_count < expected_critical_row_count || missing_baseline_count > 0 {
        OverlayGateVerdict::Incomplete
    } else if !critical_red_row_ids.is_empty() {
        OverlayGateVerdict::Fail
    } else if parity_to_margin_count > 0 {
        OverlayGateVerdict::Warning
    } else {
        OverlayGateVerdict::Pass
    };

    BenchmarkHonestGateSummary {
        verdict,
        expected_critical_row_count,
        critical_row_count,
        comparable_row_count,
        missing_baseline_count,
        below_parity_count,
        tail_slower_than_sqlite_count,
        parity_to_margin_count,
        healthy_margin_count,
        hard_fail_when_red_row_present: true,
        critical_red_row_ids,
        margin_band_row_ids,
        missing_baseline_row_ids,
    }
}

fn ratio(numerator: f64, denominator: f64) -> Option<f64> {
    if numerator.is_finite() && denominator.is_finite() && denominator > 0.0 {
        Some(numerator / denominator)
    } else {
        None
    }
}

/// # Errors
///
/// Returns an error if the JSONL file cannot be read or parsed.
pub fn load_benchmark_summaries(path: &Path) -> Result<Vec<BenchmarkSummary>, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|error| format!("read benchmark summaries {}: {error}", path.display()))?;
    let mut summaries = Vec::new();
    for (line_idx, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let summary: BenchmarkSummary = serde_json::from_str(line).map_err(|error| {
            format!(
                "parse benchmark summary line {} from {}: {error}",
                line_idx + 1,
                path.display()
            )
        })?;
        summaries.push(summary);
    }
    Ok(summaries)
}

/// # Errors
///
/// Returns an error if the JSON file cannot be read or parsed.
pub fn load_c1_evidence_pack_scorecard(path: &Path) -> Result<C1EvidencePackScorecard, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|error| format!("read c1 scorecard {}: {error}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|error| format!("parse c1 scorecard {}: {error}", path.display()))
}

/// # Errors
///
/// Returns an error if the JSON file cannot be read or parsed.
pub fn load_persistent_phase_pack_scorecard(
    path: &Path,
) -> Result<PersistentPhasePackScorecard, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|error| format!("read persistent scorecard {}: {error}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|error| format!("parse persistent scorecard {}: {error}", path.display()))
}

#[must_use]
pub fn benchmark_cell_key(summary: &BenchmarkSummary) -> BenchmarkCellKey {
    let row_identity = summary
        .comparison
        .as_ref()
        .map(|comparison| &comparison.row_identity);
    BenchmarkCellKey {
        benchmark_id: summary.benchmark_id.clone(),
        row_id: row_identity.and_then(|identity| identity.row_id.clone()),
        fixture_id: row_identity.map_or_else(
            || summary.fixture_id.clone(),
            |identity| identity.fixture_id.clone(),
        ),
        workload: row_identity.map_or_else(
            || summary.workload.clone(),
            |identity| identity.workload.clone(),
        ),
        concurrency: row_identity.map_or(summary.concurrency, |identity| identity.concurrency),
        mode_id: row_identity.map_or_else(
            || summary.comparison_mode_id().to_owned(),
            |identity| identity.mode_id.clone(),
        ),
    }
}

/// # Errors
///
/// Returns an error if the summaries contain duplicate row keys.
pub fn evaluate_matrix_regression_gate(
    baseline_summaries: &[BenchmarkSummary],
    current_summaries: &[BenchmarkSummary],
    baseline_label: impl Into<String>,
    current_label: impl Into<String>,
    thresholds: MatrixRegressionThresholds,
) -> Result<MatrixRegressionGateReport, String> {
    let baseline_label = baseline_label.into();
    let current_label = current_label.into();
    let baseline = build_summary_index(&baseline_label, baseline_summaries)?;
    let current = build_summary_index(&current_label, current_summaries)?;

    let mut checks = Vec::new();
    let mut missing_baseline_cells = Vec::new();
    let mut failing_cells = Vec::new();

    for (cell_key, current_summary) in &current {
        let Some(baseline_summary) = baseline.get(cell_key) else {
            missing_baseline_cells.push(cell_key.clone());
            continue;
        };

        let mut reasons = Vec::new();

        let p95_ratio = if baseline_summary.latency.p95_ms > 0.0 {
            current_summary.latency.p95_ms / baseline_summary.latency.p95_ms
        } else {
            reasons.push(format!(
                "baseline p95 must be > 0, got {:.4}",
                baseline_summary.latency.p95_ms
            ));
            f64::INFINITY
        };
        if p95_ratio > thresholds.max_p95_ratio {
            reasons.push(format!(
                "p95 ratio {:.4} > allowed {:.4}",
                p95_ratio, thresholds.max_p95_ratio
            ));
        }

        let throughput_ratio = if baseline_summary.throughput.median_ops_per_sec > 0.0 {
            current_summary.throughput.median_ops_per_sec
                / baseline_summary.throughput.median_ops_per_sec
        } else {
            reasons.push(format!(
                "baseline throughput must be > 0, got {:.4}",
                baseline_summary.throughput.median_ops_per_sec
            ));
            0.0
        };
        if throughput_ratio < thresholds.min_throughput_ratio {
            reasons.push(format!(
                "throughput ratio {:.4} < allowed {:.4}",
                throughput_ratio, thresholds.min_throughput_ratio
            ));
        }

        let passed = reasons.is_empty();
        if !passed {
            failing_cells.push(cell_key.clone());
        }

        checks.push(MatrixRegressionCheck {
            cell_key: cell_key.clone(),
            baseline_p95_ms: baseline_summary.latency.p95_ms,
            current_p95_ms: current_summary.latency.p95_ms,
            p95_ratio,
            baseline_throughput_ops_per_sec: baseline_summary.throughput.median_ops_per_sec,
            current_throughput_ops_per_sec: current_summary.throughput.median_ops_per_sec,
            throughput_ratio,
            passed,
            reasons,
        });
    }

    Ok(MatrixRegressionGateReport {
        schema_version: MATRIX_REGRESSION_GATE_SCHEMA_V2.to_owned(),
        baseline_label,
        current_label,
        thresholds,
        compared_cells: checks.len(),
        missing_baseline_cells,
        failing_cells,
        checks,
    })
}

/// # Errors
///
/// Returns an error if the input files cannot be read or parsed.
pub fn evaluate_matrix_regression_gate_from_paths(
    baseline_jsonl: &Path,
    current_jsonl: &Path,
    thresholds: MatrixRegressionThresholds,
) -> Result<MatrixRegressionGateReport, String> {
    let baseline = load_benchmark_summaries(baseline_jsonl)?;
    let current = load_benchmark_summaries(current_jsonl)?;
    evaluate_matrix_regression_gate(
        &baseline,
        &current,
        baseline_jsonl.display().to_string(),
        current_jsonl.display().to_string(),
        thresholds,
    )
}

/// # Errors
///
/// Returns an error if any input artifact cannot be read or parsed.
pub fn evaluate_overlay_honesty_gate_from_paths(
    current_matrix_jsonl: &Path,
    baseline_matrix_jsonl: Option<&Path>,
    c1_scorecard_json: Option<&Path>,
    persistent_scorecard_json: Option<&Path>,
    config: OverlayHonestyGateConfig,
) -> Result<OverlayHonestyGateReport, String> {
    let current_summaries = load_benchmark_summaries(current_matrix_jsonl)?;
    let baseline_summaries = baseline_matrix_jsonl
        .map(load_benchmark_summaries)
        .transpose()?;
    let c1_scorecard = c1_scorecard_json
        .map(load_c1_evidence_pack_scorecard)
        .transpose()?;
    let persistent_scorecard = persistent_scorecard_json
        .map(load_persistent_phase_pack_scorecard)
        .transpose()?;

    evaluate_overlay_honesty_gate(
        &current_summaries,
        baseline_summaries.as_deref(),
        current_matrix_jsonl.display().to_string(),
        baseline_matrix_jsonl.map(|path| path.display().to_string()),
        c1_scorecard.as_ref(),
        persistent_scorecard.as_ref(),
        config,
    )
}

/// # Errors
///
/// Returns an error if the benchmark summaries contain duplicate row keys.
pub fn evaluate_overlay_honesty_gate(
    current_summaries: &[BenchmarkSummary],
    baseline_summaries: Option<&[BenchmarkSummary]>,
    current_label: String,
    baseline_label: Option<String>,
    c1_scorecard: Option<&C1EvidencePackScorecard>,
    persistent_scorecard: Option<&PersistentPhasePackScorecard>,
    config: OverlayHonestyGateConfig,
) -> Result<OverlayHonestyGateReport, String> {
    let current_index = build_summary_index(&current_label, current_summaries)?;
    let causal_scorecards = build_benchmark_causal_scorecard_report(current_summaries);

    let matrix_regression = match (baseline_summaries, baseline_label) {
        (Some(baseline), Some(label)) => Some(evaluate_matrix_regression_gate(
            baseline,
            current_summaries,
            label,
            current_label.clone(),
            config.matrix_thresholds,
        )?),
        _ => None,
    };

    let mut overall_verdict = OverlayGateVerdict::Pass;
    let mut ci_blocking = false;
    let mut blocking_findings = Vec::new();
    let mut warning_findings = Vec::new();

    let matrix_findings = if let Some(ref report) = matrix_regression {
        let mut findings = Vec::new();
        for cell_key in &report.missing_baseline_cells {
            let causal_scorecard = current_index
                .get(cell_key)
                .and_then(|summary| find_scorecard_digest(&causal_scorecards, summary));
            findings.push(OverlayMatrixFinding {
                cell_key: cell_key.clone(),
                reasons: vec!["baseline row is missing, so the current cell cannot hide behind aggregate-only reporting".to_owned()],
                causal_scorecard,
            });
        }
        for check in report.checks.iter().filter(|check| !check.passed) {
            let causal_scorecard = current_index
                .get(&check.cell_key)
                .and_then(|summary| find_scorecard_digest(&causal_scorecards, summary));
            findings.push(OverlayMatrixFinding {
                cell_key: check.cell_key.clone(),
                reasons: check.reasons.clone(),
                causal_scorecard,
            });
        }
        if let Some(summary) = report.failure_summary() {
            ci_blocking = true;
            overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
            blocking_findings.push(format!("matrix regression gate: {summary}"));
        }
        findings
    } else {
        Vec::new()
    };

    let c1_pack = assess_c1_pack(
        c1_scorecard,
        &config,
        &mut overall_verdict,
        &mut ci_blocking,
        &mut blocking_findings,
        &mut warning_findings,
    );
    let persistent_pack = assess_persistent_pack(
        persistent_scorecard,
        &config,
        &mut overall_verdict,
        &mut ci_blocking,
        &mut blocking_findings,
        &mut warning_findings,
    );

    Ok(OverlayHonestyGateReport {
        schema_version: OVERLAY_HONESTY_GATE_SCHEMA_V1.to_owned(),
        config,
        matrix_regression,
        causal_scorecards,
        matrix_findings,
        c1_pack,
        persistent_pack,
        overall_verdict,
        ci_blocking,
        blocking_findings,
        warning_findings,
    })
}

fn assess_c1_pack(
    scorecard: Option<&C1EvidencePackScorecard>,
    config: &OverlayHonestyGateConfig,
    overall_verdict: &mut OverlayGateVerdict,
    ci_blocking: &mut bool,
    blocking_findings: &mut Vec<String>,
    warning_findings: &mut Vec<String>,
) -> C1PackAssessment {
    let Some(scorecard) = scorecard else {
        if config.require_c1_pack {
            *ci_blocking = true;
            *overall_verdict = overall_verdict.worse(OverlayGateVerdict::NoData);
            blocking_findings.push("c1 honest-gain scorecard is required but missing".to_owned());
        }
        return C1PackAssessment {
            present: false,
            run_id: None,
            verdict: None,
            red_cell_ids: Vec::new(),
            margin_cell_ids: Vec::new(),
            missing_baseline_row_ids: Vec::new(),
            no_fake_win_findings: Vec::new(),
        };
    };

    let verdict = scorecard.honest_gate_summary.verdict;
    let mut no_fake_win_findings = Vec::new();

    if !scorecard
        .comparator_contract
        .as_ref()
        .is_some_and(|contract| contract.aggregate_rows_are_secondary)
    {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
        blocking_findings.push(
            "c1 scorecard must mark aggregate rows secondary so row-level failures stay visible"
                .to_owned(),
        );
    }

    if scorecard.honest_gate_summary.critical_cell_count
        < scorecard.honest_gate_summary.expected_critical_cell_count
    {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Incomplete);
        blocking_findings.push(format!(
            "c1 scorecard is incomplete: captured {} of {} critical cells",
            scorecard.honest_gate_summary.critical_cell_count,
            scorecard.honest_gate_summary.expected_critical_cell_count
        ));
    }

    if !scorecard
        .honest_gate_summary
        .critical_red_cell_ids
        .is_empty()
    {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
        blocking_findings.push(format!(
            "c1 critical red cells remain visible: {}",
            scorecard
                .honest_gate_summary
                .critical_red_cell_ids
                .join(", ")
        ));
    }

    if !scorecard
        .honest_gate_summary
        .missing_baseline_row_ids
        .is_empty()
    {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Incomplete);
        blocking_findings.push(format!(
            "c1 same-pack sqlite baselines are missing for: {}",
            scorecard
                .honest_gate_summary
                .missing_baseline_row_ids
                .join(", ")
        ));
    }

    for rollup in &scorecard.mode_rollup {
        if rollup
            .geometric_mean_speedup
            .is_some_and(|value| value >= scorecard.healthy_margin_min)
        {
            let has_non_green = scorecard
                .rows
                .iter()
                .any(|row| row.mode_id == rollup.mode_id && row.classification != "healthy_margin");
            if has_non_green {
                let finding = format!(
                    "c1 mode rollup `{}` looks healthy in aggregate but still contains non-green row cells; aggregates are secondary",
                    rollup.mode_id
                );
                no_fake_win_findings.push(finding.clone());
                *ci_blocking = true;
                *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
                blocking_findings.push(finding);
            }
        }
    }

    for rollup in &scorecard.workload_rollup {
        if rollup
            .geometric_mean_speedup
            .is_some_and(|value| value >= scorecard.healthy_margin_min)
        {
            let has_non_green = scorecard.rows.iter().any(|row| {
                row.mode_id == rollup.mode_id
                    && row.workload == rollup.workload
                    && row.classification != "healthy_margin"
            });
            if has_non_green {
                let finding = format!(
                    "c1 workload rollup `{}` / `{}` looks healthy in aggregate but still contains non-green fixture rows; fixture skew must not hide the red cells",
                    rollup.mode_id, rollup.workload
                );
                no_fake_win_findings.push(finding.clone());
                *ci_blocking = true;
                *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
                blocking_findings.push(finding);
            }
        }
    }

    match verdict {
        OverlayGateVerdict::Pass => {}
        OverlayGateVerdict::Warning => {
            *overall_verdict = overall_verdict.worse(verdict);
            let finding = format!(
                "c1 scorecard run `{}` is warning: cells reached parity but not the healthy margin",
                scorecard.run_id
            );
            if config.fail_on_warning {
                *ci_blocking = true;
                blocking_findings.push(finding);
            } else {
                warning_findings.push(finding);
            }
        }
        OverlayGateVerdict::Fail | OverlayGateVerdict::Incomplete | OverlayGateVerdict::NoData => {
            *ci_blocking = true;
            *overall_verdict = overall_verdict.worse(verdict);
            blocking_findings.push(format!(
                "c1 scorecard run `{}` reported `{}`",
                scorecard.run_id, verdict
            ));
        }
    }

    C1PackAssessment {
        present: true,
        run_id: Some(scorecard.run_id.clone()),
        verdict: Some(verdict),
        red_cell_ids: scorecard.honest_gate_summary.critical_red_cell_ids.clone(),
        margin_cell_ids: scorecard.honest_gate_summary.margin_band_cell_ids.clone(),
        missing_baseline_row_ids: scorecard
            .honest_gate_summary
            .missing_baseline_row_ids
            .clone(),
        no_fake_win_findings,
    }
}

fn assess_persistent_pack(
    scorecard: Option<&PersistentPhasePackScorecard>,
    config: &OverlayHonestyGateConfig,
    overall_verdict: &mut OverlayGateVerdict,
    ci_blocking: &mut bool,
    blocking_findings: &mut Vec<String>,
    warning_findings: &mut Vec<String>,
) -> PersistentPackAssessment {
    const REQUIRED_REGIMES: [&str; 2] = [
        "persistent_concurrent_write_8t",
        "persistent_concurrent_write_16t",
    ];

    let Some(scorecard) = scorecard else {
        if config.require_persistent_pack {
            *ci_blocking = true;
            *overall_verdict = overall_verdict.worse(OverlayGateVerdict::NoData);
            blocking_findings
                .push("persistent honest-gain phase pack is required but missing".to_owned());
        }
        return PersistentPackAssessment {
            present: false,
            run_id: None,
            verdict: None,
            red_regimes: Vec::new(),
            incomplete_regimes: Vec::new(),
            no_data_regimes: Vec::new(),
            missing_required_regimes: Vec::new(),
            critical_regimes: Vec::new(),
            no_fake_win_findings: Vec::new(),
        };
    };

    let verdict = scorecard.honest_gate_summary.verdict;
    let mut no_fake_win_findings = Vec::new();

    if !scorecard.aggregate_views_secondary_only {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
        blocking_findings.push(
            "persistent scorecard must keep aggregate views secondary to 8t/16t regime verdicts"
                .to_owned(),
        );
    }

    if !scorecard.critical_surface_primary {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
        blocking_findings
            .push("persistent scorecard must mark the 8t/16t regime surface as primary".to_owned());
    }

    if !scorecard
        .comparator_contract
        .as_ref()
        .is_some_and(|contract| contract.aggregate_rows_are_secondary)
    {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
        blocking_findings.push(
            "persistent scorecard comparator contract must say aggregate rows are secondary"
                .to_owned(),
        );
    }

    let missing_required_regimes = REQUIRED_REGIMES
        .iter()
        .filter(|required| {
            !scorecard
                .critical_regimes
                .iter()
                .any(|regime| regime.regime_id == **required)
        })
        .map(|required| (*required).to_owned())
        .collect::<Vec<_>>();
    if !missing_required_regimes.is_empty() {
        *ci_blocking = true;
        *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Incomplete);
        blocking_findings.push(format!(
            "persistent scorecard is missing required critical regimes: {}",
            missing_required_regimes.join(", ")
        ));
    }

    let critical_regimes = scorecard
        .critical_regimes
        .iter()
        .map(|regime| PersistentRegimeAssessment {
            regime_id: regime.regime_id.clone(),
            verdict: regime.verdict.clone(),
            coverage_state: regime.coverage_state.clone(),
            throughput_ratio_vs_sqlite: regime.throughput_ratio_vs_sqlite,
            collapse_override_applies: regime.collapse_override_applies,
            measured_reasons: regime.measured_reasons.clone(),
        })
        .collect::<Vec<_>>();

    for regime in &scorecard.critical_regimes {
        match regime.verdict.as_str() {
            "pass" => {}
            "warning" => {
                let finding = format!(
                    "persistent regime `{}` is warning and still below the healthy-gain bar",
                    regime.regime_id
                );
                *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Warning);
                if config.fail_on_warning {
                    *ci_blocking = true;
                    blocking_findings.push(finding);
                } else {
                    warning_findings.push(finding);
                }
            }
            "below_parity" | "collapse_red" | "incomplete" | "no_data" => {
                *ci_blocking = true;
                *overall_verdict = overall_verdict.worse(match regime.verdict.as_str() {
                    "incomplete" => OverlayGateVerdict::Incomplete,
                    "no_data" => OverlayGateVerdict::NoData,
                    _ => OverlayGateVerdict::Fail,
                });
                blocking_findings.push(format!(
                    "persistent regime `{}` is `{}`",
                    regime.regime_id, regime.verdict
                ));
            }
            other => {
                *ci_blocking = true;
                *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Fail);
                blocking_findings.push(format!(
                    "persistent regime `{}` has unknown verdict `{other}`",
                    regime.regime_id
                ));
            }
        }
        if regime.coverage_state != "complete" {
            let finding = format!(
                "persistent regime `{}` coverage is `{}` instead of `complete`",
                regime.regime_id, regime.coverage_state
            );
            *ci_blocking = true;
            *overall_verdict = overall_verdict.worse(OverlayGateVerdict::Incomplete);
            blocking_findings.push(finding);
        }
        if regime.critical_surface_primary && regime.verdict != "pass" {
            let finding = format!(
                "persistent regime `{}` stays individually visible with verdict `{}`; aggregate views must not override it",
                regime.regime_id, regime.verdict
            );
            no_fake_win_findings.push(finding);
        }
    }

    match verdict {
        OverlayGateVerdict::Pass => {}
        OverlayGateVerdict::Warning => {
            *overall_verdict = overall_verdict.worse(verdict);
            let finding = format!("persistent scorecard run `{}` is warning", scorecard.run_id);
            if config.fail_on_warning {
                *ci_blocking = true;
                blocking_findings.push(finding);
            } else {
                warning_findings.push(finding);
            }
        }
        OverlayGateVerdict::Fail | OverlayGateVerdict::Incomplete | OverlayGateVerdict::NoData => {
            *ci_blocking = true;
            *overall_verdict = overall_verdict.worse(verdict);
            blocking_findings.push(format!(
                "persistent scorecard run `{}` reported `{}`",
                scorecard.run_id, verdict
            ));
        }
    }

    PersistentPackAssessment {
        present: true,
        run_id: Some(scorecard.run_id.clone()),
        verdict: Some(verdict),
        red_regimes: scorecard.honest_gate_summary.red_regimes.clone(),
        incomplete_regimes: scorecard.honest_gate_summary.incomplete_regimes.clone(),
        no_data_regimes: scorecard.honest_gate_summary.no_data_regimes.clone(),
        missing_required_regimes,
        critical_regimes,
        no_fake_win_findings,
    }
}

fn build_summary_index<'a>(
    label: &str,
    summaries: &'a [BenchmarkSummary],
) -> Result<BTreeMap<BenchmarkCellKey, &'a BenchmarkSummary>, String> {
    let mut index = BTreeMap::new();
    for summary in summaries {
        let cell_key = benchmark_cell_key(summary);
        if index.insert(cell_key.clone(), summary).is_some() {
            return Err(format!("duplicate benchmark cell in {label}: {cell_key}"));
        }
    }
    Ok(index)
}

fn find_scorecard_digest(
    report: &BenchmarkCausalScorecardReport,
    summary: &BenchmarkSummary,
) -> Option<OverlayCausalScorecardDigest> {
    let target = benchmark_cell_key(summary);
    report
        .groups
        .iter()
        .flat_map(|group| group.scorecards.iter())
        .find(|scorecard| scorecard_matches_cell(scorecard, &target))
        .map(digest_from_scorecard)
}

fn scorecard_matches_cell(
    scorecard: &BenchmarkCausalScorecard,
    cell_key: &BenchmarkCellKey,
) -> bool {
    let identity = &scorecard.row_identity;
    identity.fixture_id == cell_key.fixture_id
        && identity.workload == cell_key.workload
        && identity.concurrency == cell_key.concurrency
        && identity.mode_id == cell_key.mode_id
        && (cell_key.row_id.is_none() || identity.row_id == cell_key.row_id)
}

fn digest_from_scorecard(scorecard: &BenchmarkCausalScorecard) -> OverlayCausalScorecardDigest {
    OverlayCausalScorecardDigest {
        benchmark_id: scorecard.benchmark_id.clone(),
        row_identity: BenchmarkCellKey {
            benchmark_id: scorecard.benchmark_id.clone(),
            row_id: scorecard.row_identity.row_id.clone(),
            fixture_id: scorecard.row_identity.fixture_id.clone(),
            workload: scorecard.row_identity.workload.clone(),
            concurrency: scorecard.row_identity.concurrency,
            mode_id: scorecard.row_identity.mode_id.clone(),
        },
        baseline_comparator: scorecard.baseline_comparator.clone(),
        claim_summary: scorecard.claim_summary.clone(),
        interpretation_note: scorecard.interpretation_note.clone(),
        optimization_families: scorecard
            .causal_chain
            .iter()
            .map(|link| link.optimization_family.clone())
            .collect(),
        negative_findings: scorecard.negative_findings.clone(),
    }
}

fn parse_ratio_env(key: &str, default: f64) -> Result<f64, String> {
    let Some(raw) = std::env::var_os(key) else {
        return Ok(default);
    };
    let text = raw.to_string_lossy();
    let value: f64 = text
        .parse()
        .map_err(|error| format!("invalid {key} `{text}`: {error}"))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{key} must be finite and > 0, got `{text}`"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark::{
        BenchmarkComparisonMetadata, BenchmarkConfig, BenchmarkMeta, IterationRecord, LatencyStats,
        ThroughputStats, run_benchmark,
    };
    use crate::methodology::{EnvironmentMeta, MethodologyMeta};

    fn scorecard_summary(
        mode_id: &str,
        fixture_id: &str,
        workload: &str,
        concurrency: u16,
        wall_ms: u64,
        ops_total: u64,
    ) -> BenchmarkSummary {
        let engine = match mode_id {
            "sqlite_reference" => "sqlite3",
            _ => "fsqlite",
        };
        let mut summary = run_benchmark(
            &BenchmarkConfig {
                warmup_iterations: 0,
                min_iterations: 1,
                measurement_time_secs: 0,
            },
            &BenchmarkMeta {
                engine: engine.to_owned(),
                workload: workload.to_owned(),
                fixture_id: fixture_id.to_owned(),
                concurrency,
                cargo_profile: "test".to_owned(),
            },
            |_| {
                Ok::<_, String>(crate::report::EngineRunReport {
                    wall_time_ms: wall_ms,
                    ops_total,
                    ops_per_sec: ops_total as f64 / (wall_ms as f64 / 1000.0),
                    retries: 0,
                    aborts: 0,
                    correctness: crate::report::CorrectnessReport {
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
                    runtime_phase_timing: None,
                    hot_path_profile: None,
                })
            },
        );
        summary.comparison = Some(BenchmarkComparisonMetadata::anonymous(&summary, mode_id));
        summary
    }

    fn sample_benchmark_summary(
        benchmark_id: &str,
        mode_id: &str,
        workload: &str,
        fixture_id: &str,
        concurrency: u16,
        p95_ms: f64,
        throughput_ops_per_sec: f64,
    ) -> BenchmarkSummary {
        let mut summary = BenchmarkSummary {
            benchmark_id: benchmark_id.to_owned(),
            engine: mode_id.to_owned(),
            workload: workload.to_owned(),
            fixture_id: fixture_id.to_owned(),
            concurrency,
            methodology: MethodologyMeta::current(),
            environment: EnvironmentMeta::capture("test"),
            warmup_count: 0,
            measurement_count: 3,
            total_measurement_ms: 30,
            latency: LatencyStats {
                min_ms: p95_ms * 0.5,
                max_ms: p95_ms * 1.1,
                mean_ms: p95_ms * 0.8,
                median_ms: p95_ms * 0.75,
                p95_ms,
                p99_ms: p95_ms * 1.05,
                stddev_ms: p95_ms * 0.1,
            },
            throughput: ThroughputStats {
                mean_ops_per_sec: throughput_ops_per_sec * 0.98,
                median_ops_per_sec: throughput_ops_per_sec,
                peak_ops_per_sec: throughput_ops_per_sec * 1.02,
            },
            iterations: vec![IterationRecord {
                iteration: 0,
                wall_time_ms: 10,
                ops_per_sec: throughput_ops_per_sec,
                ops_total: 100,
                retries: 0,
                aborts: 0,
                error: None,
            }],
            comparison: None,
            aggregated_hot_path: None,
        };
        summary.comparison = Some(BenchmarkComparisonMetadata::anonymous(&summary, mode_id));
        summary
    }

    fn c1_scorecard_with_red_rows() -> C1EvidencePackScorecard {
        C1EvidencePackScorecard {
            schema_version: "bd-db300.c1_evidence_pack_scorecard.v1".to_owned(),
            run_id: "c1-run".to_owned(),
            healthy_margin_min: 1.1,
            concurrency: 1,
            pack_role: Some("honest_gate_scorecard".to_owned()),
            fixtures: vec!["fixture-a".to_owned(), "fixture-b".to_owned()],
            workloads: vec!["mixed_read_write".to_owned()],
            rows: vec![
                C1EvidencePackRow {
                    row_id: "fixture-a:mixed_read_write:fsqlite_mvcc".to_owned(),
                    fixture_id: "fixture-a".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    mode_id: "fsqlite_mvcc".to_owned(),
                    mode_label: "FrankenSQLite MVCC".to_owned(),
                    speedup_vs_sqlite: Some(1.40),
                    classification: "healthy_margin".to_owned(),
                },
                C1EvidencePackRow {
                    row_id: "fixture-b:mixed_read_write:fsqlite_mvcc".to_owned(),
                    fixture_id: "fixture-b".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    mode_id: "fsqlite_mvcc".to_owned(),
                    mode_label: "FrankenSQLite MVCC".to_owned(),
                    speedup_vs_sqlite: Some(0.80),
                    classification: "below_parity".to_owned(),
                },
                C1EvidencePackRow {
                    row_id: "fixture-a:mixed_read_write:fsqlite_single".to_owned(),
                    fixture_id: "fixture-a".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    mode_id: "fsqlite_single".to_owned(),
                    mode_label: "FrankenSQLite Single Writer".to_owned(),
                    speedup_vs_sqlite: Some(1.25),
                    classification: "healthy_margin".to_owned(),
                },
                C1EvidencePackRow {
                    row_id: "fixture-b:mixed_read_write:fsqlite_single".to_owned(),
                    fixture_id: "fixture-b".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    mode_id: "fsqlite_single".to_owned(),
                    mode_label: "FrankenSQLite Single Writer".to_owned(),
                    speedup_vs_sqlite: Some(1.05),
                    classification: "parity_to_margin".to_owned(),
                },
            ],
            mode_rollup: vec![
                C1ModeRollup {
                    mode_id: "fsqlite_mvcc".to_owned(),
                    geometric_mean_speedup: Some(1.15),
                    comparable_cell_count: 2,
                    below_parity: 1,
                    parity_to_margin: 0,
                    healthy_margin: 1,
                },
                C1ModeRollup {
                    mode_id: "fsqlite_single".to_owned(),
                    geometric_mean_speedup: Some(1.14),
                    comparable_cell_count: 2,
                    below_parity: 0,
                    parity_to_margin: 1,
                    healthy_margin: 1,
                },
            ],
            workload_rollup: vec![C1WorkloadRollup {
                mode_id: "fsqlite_mvcc".to_owned(),
                workload: "mixed_read_write".to_owned(),
                geometric_mean_speedup: Some(1.15),
                comparable_cell_count: 2,
            }],
            comparator_contract: Some(C1ComparatorContract {
                aggregate_rows_are_secondary: true,
            }),
            honest_gate_summary: C1HonestGateSummary {
                verdict: OverlayGateVerdict::Fail,
                expected_critical_cell_count: 4,
                critical_cell_count: 4,
                comparable_cell_count: 4,
                missing_baseline_count: 0,
                below_parity_count: 1,
                parity_to_margin_count: 1,
                healthy_margin_count: 2,
                hard_fail_when_below_parity_present: true,
                critical_red_cell_ids: vec!["fixture-b:mixed_read_write:fsqlite_mvcc".to_owned()],
                margin_band_cell_ids: vec!["fixture-b:mixed_read_write:fsqlite_single".to_owned()],
                missing_baseline_row_ids: Vec::new(),
            },
        }
    }

    fn persistent_scorecard_with_missing_16t() -> PersistentPhasePackScorecard {
        PersistentPhasePackScorecard {
            schema_version: "bd-db300.persistent_phase_pack_scorecard.v3".to_owned(),
            run_id: "persistent-run".to_owned(),
            entrypoint: "scripts/capture_persistent_phase_pack.sh".to_owned(),
            healthy_margin_min: 1.1,
            aggregate_views_secondary_only: true,
            critical_surface_primary: true,
            comparator_contract: Some(PersistentComparatorContract {
                aggregate_rows_are_secondary: true,
            }),
            honest_gate_summary: PersistentHonestGateSummary {
                verdict: OverlayGateVerdict::Fail,
                critical_regime_count: 1,
                complete_regime_count: 1,
                incomplete_regime_count: 0,
                no_data_regime_count: 0,
                red_regimes: vec!["persistent_concurrent_write_8t".to_owned()],
                incomplete_regimes: Vec::new(),
                no_data_regimes: Vec::new(),
                rule: "8t and 16t stay individually visible".to_owned(),
            },
            critical_regimes: vec![PersistentCriticalRegime {
                regime_id: "persistent_concurrent_write_8t".to_owned(),
                verdict: "below_parity".to_owned(),
                coverage_state: "complete".to_owned(),
                critical_surface_primary: true,
                throughput_ratio_vs_sqlite: Some(0.71),
                throughput_band: Some("below_parity".to_owned()),
                collapse_override_applies: false,
                measured_reasons: vec![
                    "throughput midpoint ratio vs same-pack sqlite3 is 0.707x".to_owned(),
                ],
                missing_artifacts: Vec::new(),
            }],
        }
    }

    #[test]
    fn matrix_regression_gate_is_mode_aware() {
        let baseline = vec![
            sample_benchmark_summary(
                "fsqlite:mixed_read_write:fixture:c1",
                "fsqlite_mvcc",
                "mixed_read_write",
                "fixture",
                1,
                10.0,
                1_000.0,
            ),
            sample_benchmark_summary(
                "fsqlite:mixed_read_write:fixture:c1",
                "fsqlite_single_writer",
                "mixed_read_write",
                "fixture",
                1,
                9.0,
                1_050.0,
            ),
        ];
        let current = vec![
            sample_benchmark_summary(
                "fsqlite:mixed_read_write:fixture:c1",
                "fsqlite_mvcc",
                "mixed_read_write",
                "fixture",
                1,
                13.0,
                700.0,
            ),
            sample_benchmark_summary(
                "fsqlite:mixed_read_write:fixture:c1",
                "fsqlite_single_writer",
                "mixed_read_write",
                "fixture",
                1,
                8.5,
                1_100.0,
            ),
        ];

        let report = evaluate_matrix_regression_gate(
            &baseline,
            &current,
            "baseline",
            "current",
            MatrixRegressionThresholds {
                max_p95_ratio: 1.20,
                min_throughput_ratio: 0.80,
            },
        )
        .expect("matrix regression gate should evaluate");

        assert_eq!(report.compared_cells, 2);
        assert_eq!(report.failing_cells.len(), 1);
        assert_eq!(report.failing_cells[0].mode_id, "fsqlite_mvcc");
    }

    #[test]
    fn overlay_honesty_gate_fails_closed_and_attaches_causal_digests() {
        let baseline = vec![
            scorecard_summary(
                "sqlite_reference",
                "fixture",
                "mixed_read_write",
                4,
                100,
                1_000,
            ),
            scorecard_summary(
                "fsqlite_single_writer",
                "fixture",
                "mixed_read_write",
                4,
                70,
                1_000,
            ),
            scorecard_summary("fsqlite_mvcc", "fixture", "mixed_read_write", 4, 50, 1_000),
        ];
        let current = vec![
            scorecard_summary(
                "sqlite_reference",
                "fixture",
                "mixed_read_write",
                4,
                100,
                1_000,
            ),
            scorecard_summary(
                "fsqlite_single_writer",
                "fixture",
                "mixed_read_write",
                4,
                75,
                1_000,
            ),
            scorecard_summary("fsqlite_mvcc", "fixture", "mixed_read_write", 4, 80, 700),
        ];

        let report = evaluate_overlay_honesty_gate(
            &current,
            Some(&baseline),
            "current".to_owned(),
            Some("baseline".to_owned()),
            Some(&c1_scorecard_with_red_rows()),
            Some(&persistent_scorecard_with_missing_16t()),
            OverlayHonestyGateConfig::strict_overlay(),
        )
        .expect("overlay honesty gate should evaluate");

        assert!(report.ci_blocking);
        assert_eq!(report.overall_verdict, OverlayGateVerdict::Incomplete);
        assert!(
            report
                .blocking_findings
                .iter()
                .any(|finding| finding.contains("c1 critical red cells"))
        );
        assert!(
            report
                .blocking_findings
                .iter()
                .any(|finding| finding.contains("missing required critical regimes"))
        );
        assert!(
            report
                .matrix_findings
                .iter()
                .any(|finding| finding.causal_scorecard.is_some())
        );
        let digest = report
            .matrix_findings
            .iter()
            .find_map(|finding| finding.causal_scorecard.as_ref())
            .expect("matrix finding should include causal scorecard");
        assert!(
            digest
                .optimization_families
                .iter()
                .any(|family| family == "mvcc_concurrency_routing")
        );
        assert!(
            report
                .c1_pack
                .no_fake_win_findings
                .iter()
                .any(|finding| finding.contains("aggregate"))
        );
    }

    #[test]
    fn overlay_honesty_gate_passes_when_surfaces_are_green() {
        let baseline = vec![
            scorecard_summary(
                "sqlite_reference",
                "fixture",
                "mixed_read_write",
                4,
                100,
                1_000,
            ),
            scorecard_summary(
                "fsqlite_single_writer",
                "fixture",
                "mixed_read_write",
                4,
                70,
                1_100,
            ),
            scorecard_summary("fsqlite_mvcc", "fixture", "mixed_read_write", 4, 50, 1_300),
        ];
        let current = vec![
            scorecard_summary(
                "sqlite_reference",
                "fixture",
                "mixed_read_write",
                4,
                100,
                1_000,
            ),
            scorecard_summary(
                "fsqlite_single_writer",
                "fixture",
                "mixed_read_write",
                4,
                68,
                1_120,
            ),
            scorecard_summary("fsqlite_mvcc", "fixture", "mixed_read_write", 4, 48, 1_320),
        ];
        let c1 = C1EvidencePackScorecard {
            schema_version: "bd-db300.c1_evidence_pack_scorecard.v1".to_owned(),
            run_id: "c1-green".to_owned(),
            healthy_margin_min: 1.1,
            concurrency: 1,
            pack_role: Some("honest_gate_scorecard".to_owned()),
            fixtures: vec!["fixture".to_owned()],
            workloads: vec!["mixed_read_write".to_owned()],
            rows: vec![
                C1EvidencePackRow {
                    row_id: "fixture:mixed_read_write:fsqlite_mvcc".to_owned(),
                    fixture_id: "fixture".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    mode_id: "fsqlite_mvcc".to_owned(),
                    mode_label: "FrankenSQLite MVCC".to_owned(),
                    speedup_vs_sqlite: Some(1.20),
                    classification: "healthy_margin".to_owned(),
                },
                C1EvidencePackRow {
                    row_id: "fixture:mixed_read_write:fsqlite_single".to_owned(),
                    fixture_id: "fixture".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    mode_id: "fsqlite_single".to_owned(),
                    mode_label: "FrankenSQLite Single Writer".to_owned(),
                    speedup_vs_sqlite: Some(1.18),
                    classification: "healthy_margin".to_owned(),
                },
            ],
            mode_rollup: vec![
                C1ModeRollup {
                    mode_id: "fsqlite_mvcc".to_owned(),
                    geometric_mean_speedup: Some(1.20),
                    comparable_cell_count: 1,
                    below_parity: 0,
                    parity_to_margin: 0,
                    healthy_margin: 1,
                },
                C1ModeRollup {
                    mode_id: "fsqlite_single".to_owned(),
                    geometric_mean_speedup: Some(1.18),
                    comparable_cell_count: 1,
                    below_parity: 0,
                    parity_to_margin: 0,
                    healthy_margin: 1,
                },
            ],
            workload_rollup: vec![
                C1WorkloadRollup {
                    mode_id: "fsqlite_mvcc".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    geometric_mean_speedup: Some(1.20),
                    comparable_cell_count: 1,
                },
                C1WorkloadRollup {
                    mode_id: "fsqlite_single".to_owned(),
                    workload: "mixed_read_write".to_owned(),
                    geometric_mean_speedup: Some(1.18),
                    comparable_cell_count: 1,
                },
            ],
            comparator_contract: Some(C1ComparatorContract {
                aggregate_rows_are_secondary: true,
            }),
            honest_gate_summary: C1HonestGateSummary {
                verdict: OverlayGateVerdict::Pass,
                expected_critical_cell_count: 2,
                critical_cell_count: 2,
                comparable_cell_count: 2,
                missing_baseline_count: 0,
                below_parity_count: 0,
                parity_to_margin_count: 0,
                healthy_margin_count: 2,
                hard_fail_when_below_parity_present: true,
                critical_red_cell_ids: Vec::new(),
                margin_band_cell_ids: Vec::new(),
                missing_baseline_row_ids: Vec::new(),
            },
        };
        let persistent = PersistentPhasePackScorecard {
            schema_version: "bd-db300.persistent_phase_pack_scorecard.v3".to_owned(),
            run_id: "persistent-green".to_owned(),
            entrypoint: "scripts/capture_persistent_phase_pack.sh".to_owned(),
            healthy_margin_min: 1.1,
            aggregate_views_secondary_only: true,
            critical_surface_primary: true,
            comparator_contract: Some(PersistentComparatorContract {
                aggregate_rows_are_secondary: true,
            }),
            honest_gate_summary: PersistentHonestGateSummary {
                verdict: OverlayGateVerdict::Pass,
                critical_regime_count: 2,
                complete_regime_count: 2,
                incomplete_regime_count: 0,
                no_data_regime_count: 0,
                red_regimes: Vec::new(),
                incomplete_regimes: Vec::new(),
                no_data_regimes: Vec::new(),
                rule: "8t and 16t stay individually visible".to_owned(),
            },
            critical_regimes: vec![
                PersistentCriticalRegime {
                    regime_id: "persistent_concurrent_write_8t".to_owned(),
                    verdict: "pass".to_owned(),
                    coverage_state: "complete".to_owned(),
                    critical_surface_primary: true,
                    throughput_ratio_vs_sqlite: Some(1.20),
                    throughput_band: Some("pass".to_owned()),
                    collapse_override_applies: false,
                    measured_reasons: Vec::new(),
                    missing_artifacts: Vec::new(),
                },
                PersistentCriticalRegime {
                    regime_id: "persistent_concurrent_write_16t".to_owned(),
                    verdict: "pass".to_owned(),
                    coverage_state: "complete".to_owned(),
                    critical_surface_primary: true,
                    throughput_ratio_vs_sqlite: Some(1.11),
                    throughput_band: Some("pass".to_owned()),
                    collapse_override_applies: false,
                    measured_reasons: Vec::new(),
                    missing_artifacts: Vec::new(),
                },
            ],
        };

        let report = evaluate_overlay_honesty_gate(
            &current,
            Some(&baseline),
            "current".to_owned(),
            Some("baseline".to_owned()),
            Some(&c1),
            Some(&persistent),
            OverlayHonestyGateConfig::strict_overlay(),
        )
        .expect("overlay honesty gate should evaluate");

        assert_eq!(report.overall_verdict, OverlayGateVerdict::Pass);
        assert!(!report.ci_blocking);
        assert!(report.failure_summary().is_none());
    }

    #[test]
    fn benchmark_honest_gate_report_marks_c1_and_persistent_rows_individually() {
        let summaries = vec![
            scorecard_summary(
                "sqlite_reference",
                "fixture",
                "commutative_inserts_disjoint_keys",
                1,
                100,
                1_000,
            ),
            scorecard_summary(
                "fsqlite_single_writer",
                "fixture",
                "commutative_inserts_disjoint_keys",
                1,
                100,
                1_050,
            ),
            scorecard_summary(
                "fsqlite_mvcc",
                "fixture",
                "commutative_inserts_disjoint_keys",
                1,
                120,
                800,
            ),
            sample_benchmark_summary(
                "sqlite:fixture:persistent_concurrent_write_16t:c16",
                "sqlite_reference",
                "persistent_concurrent_write_16t",
                "fixture",
                16,
                1.0,
                100.0,
            ),
            sample_benchmark_summary(
                "mvcc:fixture:persistent_concurrent_write_16t:c16",
                "fsqlite_mvcc",
                "persistent_concurrent_write_16t",
                "fixture",
                16,
                3.0,
                120.0,
            ),
        ];
        let report = build_benchmark_honest_gate_report(&summaries)
            .expect("critical summaries should produce a report");

        assert_eq!(report.honest_gate_summary.verdict, OverlayGateVerdict::Fail);
        assert_eq!(report.honest_gate_summary.expected_critical_row_count, 3);
        assert_eq!(report.honest_gate_summary.critical_row_count, 3);
        assert!(report.surfaces.iter().any(|surface| {
            surface.surface_id == "c1_fixed_tax"
                && surface.verdict == OverlayGateVerdict::Fail
                && surface.parity_to_margin_count == 1
        }));
        assert!(report.rows.iter().any(|row| {
            row.surface_id == "c1_fixed_tax"
                && row.classification == BenchmarkHonestGateClassification::BelowParity
        }));
        assert!(report.rows.iter().any(|row| {
            row.surface_id == "persistent_concurrent_write_16t"
                && row.classification == BenchmarkHonestGateClassification::TailSlowerThanSqlite
        }));
    }
}
