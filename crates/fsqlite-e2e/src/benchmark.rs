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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::fixture_select::BenchmarkArtifactManifest;
use crate::methodology::{
    EnvironmentMeta, MEASUREMENT_TIME_SECS, MIN_MEASUREMENT_ITERATIONS, MethodologyMeta,
    WARMUP_ITERATIONS,
};
use crate::report::{EngineRunReport, FsqliteHotPathProfile};

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

impl BenchmarkConfig {
    /// Build the exact methodology record for this benchmark configuration.
    #[must_use]
    pub fn methodology_meta(&self) -> MethodologyMeta {
        MethodologyMeta {
            version: "fsqlite-e2e.methodology.v1".to_owned(),
            warmup_iterations: self.warmup_iterations,
            min_measurement_iterations: self.min_iterations,
            measurement_time_secs: self.measurement_time_secs,
            primary_statistic: "median".to_owned(),
            tail_statistic: "p95".to_owned(),
            fresh_db_per_iteration: true,
            identical_pragmas_enforced: true,
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
    /// Optional canonical comparison envelope used to align SQLite, MVCC, and
    /// forced single-writer rows under one mechanically comparable schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison: Option<BenchmarkComparisonMetadata>,
    /// Aggregated FrankenSQLite hot-path profile from the last measurement
    /// iteration.  Present only for FrankenSQLite runs that capture profiling
    /// data; always `None` for the SQLite reference engine.  Used by
    /// [`BenchmarkCounterSchema::from_summary`] to populate `mode_specific`
    /// counters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregated_hot_path: Option<FsqliteHotPathProfile>,
    /// Per-iteration raw data for downstream analysis.
    pub iterations: Vec<IterationRecord>,
}

/// Canonical comparison metadata shared by SQLite, MVCC, and single-writer
/// benchmark rows when a run can be mapped onto the tracked benchmark matrix.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkComparisonMetadata {
    /// Row-level identity used for side-by-side matching across modes.
    pub row_identity: BenchmarkComparisonRowIdentity,
    /// Auxiliary provenance that is useful for scorecards or packaging but is
    /// not itself part of the primary side-by-side row key.
    pub provenance: BenchmarkComparisonProvenance,
    /// Stable comparable-vs-mode-specific counter separation for downstream
    /// tooling.
    pub counter_schema: BenchmarkCounterSchema,
    /// Canonical artifact layout when the comparison harness resolved this row
    /// against the tracked benchmark matrix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_layout: Option<BenchmarkArtifactLayout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_artifact_manifest: Option<BenchmarkArtifactManifest>,
}

/// Canonical row identity that downstream scorecards can compare without
/// reverse-engineering engine-specific field names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkComparisonRowIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<String>,
    pub fixture_id: String,
    pub workload: String,
    pub concurrency: u16,
    pub mode_id: String,
    pub build_profile_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

/// Provenance surfaced next to the row identity but intentionally separated
/// from the cross-mode key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BenchmarkComparisonProvenance {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_policy_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_class_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardware_signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beads_data_hash: Option<String>,
}

/// Workspace-relative artifact layout emitted by the canonical comparison
/// harness.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkArtifactLayout {
    pub artifact_bundle_key: String,
    pub artifact_bundle_relpath: String,
    pub artifact_manifest_path: String,
    pub result_jsonl_path: String,
    pub summary_md_path: String,
    pub logs_dir_relpath: String,
    pub profiles_dir_relpath: String,
}

/// Stable comparable counter ids exported by every mode.
pub const BENCHMARK_COUNTER_MEASUREMENT_COUNT: &str = "measurement_iteration_count";
pub const BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS: &str = "measurement_wall_time_total_ms";
pub const BENCHMARK_COUNTER_MEASUREMENT_OPS_TOTAL: &str = "measurement_ops_total";
pub const BENCHMARK_COUNTER_LATENCY_MEDIAN_MS: &str = "latency_median_ms";
pub const BENCHMARK_COUNTER_LATENCY_P95_MS: &str = "latency_p95_ms";
pub const BENCHMARK_COUNTER_LATENCY_P99_MS: &str = "latency_p99_ms";
pub const BENCHMARK_COUNTER_THROUGHPUT_MEDIAN_OPS_PER_SEC: &str = "throughput_median_ops_per_sec";
pub const BENCHMARK_COUNTER_THROUGHPUT_PEAK_OPS_PER_SEC: &str = "throughput_peak_ops_per_sec";
pub const BENCHMARK_COUNTER_RETRY_TOTAL: &str = "retry_total";
pub const BENCHMARK_COUNTER_ABORT_TOTAL: &str = "abort_total";

/// Ordered comparable counter surface emitted for every benchmark row.
pub const BENCHMARK_COMPARABLE_COUNTER_IDS: [&str; 10] = [
    BENCHMARK_COUNTER_MEASUREMENT_COUNT,
    BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS,
    BENCHMARK_COUNTER_MEASUREMENT_OPS_TOTAL,
    BENCHMARK_COUNTER_LATENCY_MEDIAN_MS,
    BENCHMARK_COUNTER_LATENCY_P95_MS,
    BENCHMARK_COUNTER_LATENCY_P99_MS,
    BENCHMARK_COUNTER_THROUGHPUT_MEDIAN_OPS_PER_SEC,
    BENCHMARK_COUNTER_THROUGHPUT_PEAK_OPS_PER_SEC,
    BENCHMARK_COUNTER_RETRY_TOTAL,
    BENCHMARK_COUNTER_ABORT_TOTAL,
];

/// Counter value encoding that preserves integer-vs-float semantics in the
/// machine-readable schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "value_kind", content = "value", rename_all = "snake_case")]
pub enum BenchmarkCounterValue {
    Integer(u64),
    Float(f64),
}

/// One named metric plus its unit, aggregation rule, and meaning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCounterMetric {
    pub counter_id: String,
    pub unit: String,
    pub aggregation: String,
    pub semantics: String,
    pub value: BenchmarkCounterValue,
}

/// Explicit separation between counters safe for cross-mode comparison and
/// counters that are mode-specific advisory detail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct BenchmarkCounterSchema {
    #[serde(default)]
    pub comparable: Vec<BenchmarkCounterMetric>,
    #[serde(default)]
    pub mode_specific: Vec<BenchmarkCounterMetric>,
}

/// Schema version for grouped causal scorecards derived from aligned benchmark
/// rows.
pub const BENCHMARK_CAUSAL_SCORECARD_REPORT_SCHEMA_V1: &str =
    "fsqlite-e2e.benchmark_causal_scorecard_report.v1";
/// Schema version for one benchmark row's causal scorecard.
pub const BENCHMARK_CAUSAL_SCORECARD_SCHEMA_V1: &str = "fsqlite-e2e.benchmark_causal_scorecard.v1";

/// Run-level scorecard report grouped by `(fixture, workload, concurrency)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCausalScorecardReport {
    pub schema_version: String,
    pub groups: Vec<BenchmarkCausalScorecardGroup>,
}

/// One canonical comparison group and the per-mode scorecards derived from it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCausalScorecardGroup {
    pub fixture_id: String,
    pub workload: String,
    pub concurrency: u16,
    pub scorecards: Vec<BenchmarkCausalScorecard>,
}

/// Causal scorecard for one benchmark row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCausalScorecard {
    pub schema_version: String,
    pub benchmark_id: String,
    pub row_identity: BenchmarkComparisonRowIdentity,
    pub baseline_comparator: String,
    pub claim_summary: String,
    pub causal_chain: Vec<BenchmarkCausalChainLink>,
    #[serde(default)]
    pub negative_findings: Vec<String>,
    pub interpretation_note: String,
}

/// One attributable transition in the comparison chain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCausalChainLink {
    pub rank: u32,
    pub from_mode_id: String,
    pub to_mode_id: String,
    pub optimization_family: String,
    pub claim_summary: String,
    pub rationale: String,
    pub attributed_wall_time_delta_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_of_total_wall_time_gain_basis_points: Option<u32>,
    pub counter_deltas: Vec<BenchmarkCausalMetricDelta>,
    pub evidence: Vec<String>,
}

/// Counter delta attached to one causal chain link.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkCausalMetricDelta {
    pub counter_id: String,
    pub unit: String,
    pub baseline_value: BenchmarkCounterValue,
    pub candidate_value: BenchmarkCounterValue,
    pub delta: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub improvement_pct: Option<f64>,
    pub direction: String,
    pub summary: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CounterPreference {
    HigherIsBetter,
    LowerIsBetter,
    Invariant,
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

    /// Return the canonical comparison mode when present, falling back to the
    /// legacy `engine` label for older summaries.
    #[must_use]
    pub fn comparison_mode_id(&self) -> &str {
        self.comparison
            .as_ref()
            .map_or(self.engine.as_str(), |comparison| {
                comparison.row_identity.mode_id.as_str()
            })
    }

    /// Total retries summed across all measurement iterations.
    #[must_use]
    pub fn total_iteration_retries(&self) -> u64 {
        self.iterations
            .iter()
            .map(|iteration| iteration.retries)
            .sum()
    }

    /// Total aborts summed across all measurement iterations.
    #[must_use]
    pub fn total_iteration_aborts(&self) -> u64 {
        self.iterations
            .iter()
            .map(|iteration| iteration.aborts)
            .sum()
    }

    /// Total successful operations summed across all measurement iterations.
    #[must_use]
    pub fn total_iteration_ops(&self) -> u64 {
        self.iterations
            .iter()
            .map(|iteration| iteration.ops_total)
            .sum()
    }

    /// Total reported measurement wall time summed across iterations.
    #[must_use]
    pub fn total_iteration_wall_time_ms(&self) -> u64 {
        self.iterations
            .iter()
            .map(|iteration| iteration.wall_time_ms)
            .sum()
    }
}

impl BenchmarkComparisonMetadata {
    /// Build the canonical comparison envelope for a benchmark row that is not
    /// attached to a fully resolved artifact manifest.
    #[must_use]
    pub fn anonymous(summary: &BenchmarkSummary, mode_id: impl Into<String>) -> Self {
        let mode_id = mode_id.into();
        Self {
            row_identity: BenchmarkComparisonRowIdentity {
                row_id: None,
                fixture_id: summary.fixture_id.clone(),
                workload: summary.workload.clone(),
                concurrency: summary.concurrency,
                mode_id,
                build_profile_id: summary.environment.cargo_profile.clone(),
                placement_profile_id: None,
                run_id: None,
                source_revision: None,
            },
            provenance: BenchmarkComparisonProvenance::default(),
            counter_schema: BenchmarkCounterSchema::from_summary(summary),
            artifact_layout: None,
            canonical_artifact_manifest: None,
        }
    }

    /// Build the canonical comparison envelope for a row resolved against the
    /// tracked benchmark matrix and artifact bundle contract.
    #[must_use]
    pub fn canonical(
        summary: &BenchmarkSummary,
        manifest: BenchmarkArtifactManifest,
        hardware_signature: Option<String>,
    ) -> Self {
        let row_identity = BenchmarkComparisonRowIdentity {
            row_id: Some(manifest.row_id.clone()),
            fixture_id: manifest.fixture_id.clone(),
            workload: manifest.workload.clone(),
            concurrency: manifest.concurrency,
            mode_id: manifest.mode.as_str().to_owned(),
            build_profile_id: manifest.build_profile_id.clone(),
            placement_profile_id: Some(manifest.placement_profile_id.clone()),
            run_id: Some(manifest.run_id.clone()),
            source_revision: Some(manifest.provenance.source_revision.clone()),
        };
        let provenance = BenchmarkComparisonProvenance {
            retry_policy_id: Some(manifest.retry_policy_id.clone()),
            seed_policy_id: Some(manifest.seed_policy_id.clone()),
            hardware_class_id: Some(manifest.hardware_class_id.clone()),
            hardware_signature,
            beads_data_hash: Some(manifest.provenance.beads_data_hash.clone()),
        };
        let artifact_layout = Some(BenchmarkArtifactLayout::from_manifest(&manifest));
        Self {
            row_identity,
            provenance,
            counter_schema: BenchmarkCounterSchema::from_summary(summary),
            artifact_layout,
            canonical_artifact_manifest: Some(manifest),
        }
    }
}

impl BenchmarkArtifactLayout {
    #[must_use]
    pub fn from_manifest(manifest: &BenchmarkArtifactManifest) -> Self {
        Self {
            artifact_bundle_key: manifest.artifact_bundle_key.clone(),
            artifact_bundle_relpath: manifest.artifact_bundle_relpath.clone(),
            artifact_manifest_path: artifact_path(
                &manifest.artifact_bundle_relpath,
                &manifest.artifact_names.manifest_json,
            ),
            result_jsonl_path: artifact_path(
                &manifest.artifact_bundle_relpath,
                &manifest.artifact_names.result_jsonl,
            ),
            summary_md_path: artifact_path(
                &manifest.artifact_bundle_relpath,
                &manifest.artifact_names.summary_md,
            ),
            logs_dir_relpath: artifact_path(
                &manifest.artifact_bundle_relpath,
                &manifest.artifact_names.logs_dir,
            ),
            profiles_dir_relpath: artifact_path(
                &manifest.artifact_bundle_relpath,
                &manifest.artifact_names.profiles_dir,
            ),
        }
    }
}

impl BenchmarkCounterSchema {
    #[must_use]
    pub fn from_summary(summary: &BenchmarkSummary) -> Self {
        Self {
            comparable: vec![
                integer_counter(
                    BENCHMARK_COUNTER_MEASUREMENT_COUNT,
                    "count",
                    "exact measurement iterations in this benchmark row",
                    "per benchmark summary",
                    u64::from(summary.measurement_count),
                ),
                integer_counter(
                    BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS,
                    "ms",
                    "reported wall time spent in successful or failed measurement iterations",
                    "sum across measurement iterations",
                    summary.total_iteration_wall_time_ms(),
                ),
                integer_counter(
                    BENCHMARK_COUNTER_MEASUREMENT_OPS_TOTAL,
                    "ops",
                    "successful logical operations executed during measurement",
                    "sum across measurement iterations",
                    summary.total_iteration_ops(),
                ),
                float_counter(
                    BENCHMARK_COUNTER_LATENCY_MEDIAN_MS,
                    "ms",
                    "median measurement iteration latency",
                    "median across measurement iterations",
                    summary.latency.median_ms,
                ),
                float_counter(
                    BENCHMARK_COUNTER_LATENCY_P95_MS,
                    "ms",
                    "95th percentile measurement iteration latency",
                    "p95 across measurement iterations",
                    summary.latency.p95_ms,
                ),
                float_counter(
                    BENCHMARK_COUNTER_LATENCY_P99_MS,
                    "ms",
                    "99th percentile measurement iteration latency",
                    "p99 across measurement iterations",
                    summary.latency.p99_ms,
                ),
                float_counter(
                    BENCHMARK_COUNTER_THROUGHPUT_MEDIAN_OPS_PER_SEC,
                    "ops_per_sec",
                    "median measurement iteration throughput",
                    "median across measurement iterations",
                    summary.throughput.median_ops_per_sec,
                ),
                float_counter(
                    BENCHMARK_COUNTER_THROUGHPUT_PEAK_OPS_PER_SEC,
                    "ops_per_sec",
                    "peak measurement iteration throughput",
                    "max across measurement iterations",
                    summary.throughput.peak_ops_per_sec,
                ),
                integer_counter(
                    BENCHMARK_COUNTER_RETRY_TOTAL,
                    "count",
                    "contention retries reported by the engine during measurement",
                    "sum across measurement iterations",
                    summary.total_iteration_retries(),
                ),
                integer_counter(
                    BENCHMARK_COUNTER_ABORT_TOTAL,
                    "count",
                    "transaction aborts reported by the engine during measurement",
                    "sum across measurement iterations",
                    summary.total_iteration_aborts(),
                ),
            ],
            mode_specific: summary
                .aggregated_hot_path
                .as_ref()
                .map_or_else(Vec::new, mode_specific_counters_from_hot_path),
        }
    }
}

/// Counter ids emitted as mode-specific advisory detail for FrankenSQLite runs.
pub const MODE_SPECIFIC_COUNTER_VDBE_OPCODES: &str = "fsqlite.vdbe_opcodes_executed_total";
pub const MODE_SPECIFIC_COUNTER_VDBE_STATEMENTS: &str = "fsqlite.vdbe_statements_total";
pub const MODE_SPECIFIC_COUNTER_PARSER_CALLS: &str = "fsqlite.parser_tokenize_calls_total";
pub const MODE_SPECIFIC_COUNTER_WAL_FRAMES: &str = "fsqlite.wal_frames_written_total";
pub const MODE_SPECIFIC_COUNTER_WAL_GROUP_COMMITS: &str = "fsqlite.wal_group_commits_total";
pub const MODE_SPECIFIC_COUNTER_BTREE_SEEKS: &str = "fsqlite.btree_seek_total";
pub const MODE_SPECIFIC_COUNTER_BTREE_INSERTS: &str = "fsqlite.btree_insert_total";
pub const MODE_SPECIFIC_COUNTER_BTREE_SPLITS: &str = "fsqlite.btree_page_splits_total";
pub const MODE_SPECIFIC_COUNTER_VFS_READ_OPS: &str = "fsqlite.vfs_read_ops";
pub const MODE_SPECIFIC_COUNTER_VFS_WRITE_OPS: &str = "fsqlite.vfs_write_ops";
pub const MODE_SPECIFIC_COUNTER_RETRY_BUSY: &str = "fsqlite.retry_kind_busy";
pub const MODE_SPECIFIC_COUNTER_RETRY_BUSY_SNAPSHOT: &str = "fsqlite.retry_kind_busy_snapshot";

fn mode_specific_counters_from_hot_path(
    profile: &FsqliteHotPathProfile,
) -> Vec<BenchmarkCounterMetric> {
    let mut counters = vec![
        integer_counter(
            MODE_SPECIFIC_COUNTER_VDBE_OPCODES,
            "count",
            "VDBE opcodes executed during profiled run",
            "sum",
            profile.vdbe.actual_opcodes_executed_total,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_VDBE_STATEMENTS,
            "count",
            "VDBE statements executed during profiled run",
            "sum",
            profile.vdbe.actual_statements_total,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_PARSER_CALLS,
            "count",
            "parser tokenize calls during profiled run",
            "sum",
            profile.parser.tokenize_calls_total,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_WAL_FRAMES,
            "count",
            "WAL frames written during profiled run",
            "sum",
            profile.wal.frames_written_total,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_WAL_GROUP_COMMITS,
            "count",
            "WAL group commits during profiled run",
            "sum",
            profile.wal.group_commits_total,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_VFS_READ_OPS,
            "count",
            "VFS read operations during profiled run",
            "sum",
            profile.vfs.read_ops,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_VFS_WRITE_OPS,
            "count",
            "VFS write operations during profiled run",
            "sum",
            profile.vfs.write_ops,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_RETRY_BUSY,
            "count",
            "BUSY retries by kind during profiled run",
            "sum",
            profile.runtime_retry.kind.busy,
        ),
        integer_counter(
            MODE_SPECIFIC_COUNTER_RETRY_BUSY_SNAPSHOT,
            "count",
            "BUSY_SNAPSHOT retries by kind during profiled run",
            "sum",
            profile.runtime_retry.kind.busy_snapshot,
        ),
    ];
    if let Some(ref btree) = profile.btree {
        counters.push(integer_counter(
            MODE_SPECIFIC_COUNTER_BTREE_SEEKS,
            "count",
            "B-tree seek operations during profiled run",
            "sum",
            btree.seek_total,
        ));
        counters.push(integer_counter(
            MODE_SPECIFIC_COUNTER_BTREE_INSERTS,
            "count",
            "B-tree insert operations during profiled run",
            "sum",
            btree.insert_total,
        ));
        counters.push(integer_counter(
            MODE_SPECIFIC_COUNTER_BTREE_SPLITS,
            "count",
            "B-tree page splits during profiled run",
            "sum",
            btree.page_splits_total,
        ));
    }
    counters
}

/// Build grouped causal scorecards from aligned benchmark rows.
#[must_use]
pub fn build_benchmark_causal_scorecard_report(
    summaries: &[BenchmarkSummary],
) -> BenchmarkCausalScorecardReport {
    let mut grouped: BTreeMap<(String, String, u16), Vec<&BenchmarkSummary>> = BTreeMap::new();
    for summary in summaries {
        let key = (
            summary.fixture_id.clone(),
            summary.workload.clone(),
            summary.concurrency,
        );
        grouped.entry(key).or_default().push(summary);
    }

    let groups = grouped
        .into_iter()
        .map(|((fixture_id, workload, concurrency), group)| {
            let mut ordered = group;
            ordered.sort_by(|left, right| {
                benchmark_mode_sort_key(left.comparison_mode_id())
                    .cmp(&benchmark_mode_sort_key(right.comparison_mode_id()))
            });
            let by_mode = ordered
                .iter()
                .map(|summary| (summary.comparison_mode_id(), *summary))
                .collect::<BTreeMap<_, _>>();
            let scorecards = ordered
                .into_iter()
                .map(|summary| build_scorecard_for_subject(summary, &by_mode))
                .collect();
            BenchmarkCausalScorecardGroup {
                fixture_id,
                workload,
                concurrency,
                scorecards,
            }
        })
        .collect();

    BenchmarkCausalScorecardReport {
        schema_version: BENCHMARK_CAUSAL_SCORECARD_REPORT_SCHEMA_V1.to_owned(),
        groups,
    }
}

fn build_scorecard_for_subject(
    summary: &BenchmarkSummary,
    by_mode: &BTreeMap<&str, &BenchmarkSummary>,
) -> BenchmarkCausalScorecard {
    let subject_mode = summary.comparison_mode_id();
    let row_identity = scorecard_row_identity(summary);
    let sqlite = by_mode.get("sqlite_reference").copied();
    let single_writer = by_mode.get("fsqlite_single_writer").copied();
    let subject_measurement_count =
        comparable_counter_scalar(summary, BENCHMARK_COUNTER_MEASUREMENT_COUNT);
    let subject_ops_total =
        comparable_counter_scalar(summary, BENCHMARK_COUNTER_MEASUREMENT_OPS_TOTAL);
    let subject_wall_time_ms =
        comparable_counter_scalar(summary, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS);

    let mut causal_chain = Vec::new();
    let mut negative_findings = Vec::new();
    let (baseline_comparator, claim_summary, interpretation_note) = match subject_mode {
        "sqlite_reference" => (
            "self".to_owned(),
            "Reference anchor row for cross-mode scorecards.".to_owned(),
            "This row is the baseline anchor. Savings claims are attached to downstream mode transitions rather than to the reference itself.".to_owned(),
        ),
        "fsqlite_single_writer" => {
            if let Some(baseline) = sqlite {
                let total_wall_gain =
                    comparable_counter_scalar(baseline, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS)
                        - subject_wall_time_ms;
                causal_chain.push(build_causal_chain_link(
                    1,
                    baseline,
                    summary,
                    "shared_fixed_tax_reduction",
                    "Single-writer isolates shared FrankenSQLite engine savings relative to the SQLite reference.".to_owned(),
                    "This step holds fixture, workload, concurrency, and counter schema constant while switching from the SQLite reference to FrankenSQLite without MVCC concurrency. The measured delta therefore belongs to shared parser/VDBE/storage fast lanes rather than to concurrent-writer routing.".to_owned(),
                    Some(total_wall_gain),
                ));
                (
                    "sqlite_reference".to_owned(),
                    "Single-writer claims only the shared FrankenSQLite engine step relative to the SQLite reference.".to_owned(),
                    dominant_link_note(&causal_chain, total_wall_gain, "Shared fixed-tax reductions dominate the observed single-writer gain."),
                )
            } else {
                negative_findings.push(
                    "sqlite_reference row is missing, so the single-writer scorecard cannot ground its savings against the canonical baseline."
                        .to_owned(),
                );
                (
                    "missing_sqlite_reference".to_owned(),
                    "Single-writer row is present but the SQLite reference comparator is missing.".to_owned(),
                    "This scorecard is advisory only because the baseline comparator row is absent.".to_owned(),
                )
            }
        }
        "fsqlite_mvcc" => match (sqlite, single_writer) {
            (Some(baseline), Some(serial)) => {
                let total_wall_gain =
                    comparable_counter_scalar(baseline, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS)
                        - subject_wall_time_ms;
                causal_chain.push(build_causal_chain_link(
                    1,
                    baseline,
                    serial,
                    "shared_fixed_tax_reduction",
                    "The first chain step captures shared FrankenSQLite engine savings before MVCC-specific routing is introduced.".to_owned(),
                    "Comparing the SQLite reference to the forced single-writer row isolates shared parser/VDBE/storage reductions under the same workload geometry.".to_owned(),
                    Some(total_wall_gain),
                ));
                causal_chain.push(build_causal_chain_link(
                    2,
                    serial,
                    summary,
                    "mvcc_concurrency_routing",
                    "The second chain step captures the incremental gain from enabling MVCC concurrent-writer routing on top of the same FrankenSQLite engine.".to_owned(),
                    "Comparing forced single-writer to MVCC keeps the engine, fixture, workload, and aligned counter surface fixed while toggling concurrent-writer behavior, so the incremental delta belongs to the MVCC routing/publication path rather than to shared fixed-tax cuts.".to_owned(),
                    Some(total_wall_gain),
                ));
                if summary.concurrency == 1 {
                    negative_findings.push(
                        "Concurrency is 1, so the MVCC-specific step is structurally weaker evidence than at c>1 even if the row still differs."
                            .to_owned(),
                    );
                }
                (
                    "sqlite_reference".to_owned(),
                    "MVCC claims are partitioned into shared FrankenSQLite savings plus the incremental MVCC concurrency step.".to_owned(),
                    dominant_link_note(&causal_chain, total_wall_gain, "MVCC gains are split across a shared fixed-tax step and an incremental concurrency step."),
                )
            }
            (Some(baseline), None) => {
                let total_wall_gain =
                    comparable_counter_scalar(baseline, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS)
                        - subject_wall_time_ms;
                causal_chain.push(build_causal_chain_link(
                    1,
                    baseline,
                    summary,
                    "combined_shared_and_mvcc_gain",
                    "The SQLite reference is available but the forced single-writer bridge row is missing, so shared and MVCC-specific gains remain combined in one step.".to_owned(),
                    "Without the single-writer bridge, the benchmark can only attribute the total SQLite-to-MVCC delta to a combined FrankenSQLite stack change, not to a clean split between shared fast lanes and MVCC routing.".to_owned(),
                    Some(total_wall_gain),
                ));
                negative_findings.push(
                    "fsqlite_single_writer row is missing, so the MVCC scorecard cannot partition shared fixed-tax savings from concurrency-specific savings."
                        .to_owned(),
                );
                (
                    "sqlite_reference".to_owned(),
                    "MVCC row is compared directly to SQLite because the single-writer bridge row is missing.".to_owned(),
                    dominant_link_note(&causal_chain, total_wall_gain, "The measured gain is real but remains a combined stack delta until a single-writer bridge row is present."),
                )
            }
            (None, Some(serial)) => {
                let total_wall_gain =
                    comparable_counter_scalar(serial, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS)
                        - subject_wall_time_ms;
                causal_chain.push(build_causal_chain_link(
                    1,
                    serial,
                    summary,
                    "mvcc_concurrency_routing",
                    "The MVCC row can only be compared against the single-writer bridge row because the SQLite reference anchor is absent.".to_owned(),
                    "This still isolates the incremental concurrency step, but it cannot express the total win or loss versus the canonical SQLite baseline.".to_owned(),
                    Some(total_wall_gain),
                ));
                negative_findings.push(
                    "sqlite_reference row is missing, so the MVCC scorecard cannot anchor total savings against the canonical baseline."
                        .to_owned(),
                );
                (
                    "fsqlite_single_writer".to_owned(),
                    "MVCC row is currently anchored to the single-writer bridge because the SQLite reference row is missing.".to_owned(),
                    dominant_link_note(&causal_chain, total_wall_gain, "Only the incremental MVCC step is attributable in this partial comparison set."),
                )
            }
            (None, None) => {
                negative_findings.push(
                    "Neither sqlite_reference nor fsqlite_single_writer is present, so this MVCC row has no causal bridge for scorecard attribution."
                        .to_owned(),
                );
                (
                    "missing_comparators".to_owned(),
                    "MVCC row is present without any comparator rows.".to_owned(),
                    "This scorecard is unavailable until at least one comparator mode is emitted for the same fixture/workload/concurrency group.".to_owned(),
                )
            }
        },
        _ => {
            negative_findings.push(format!(
                "unrecognized comparison mode `{subject_mode}`; no causal template is defined"
            ));
            (
                "unknown".to_owned(),
                format!("No causal scorecard template exists for mode `{subject_mode}`."),
                "This row uses an unrecognized mode label and therefore cannot be mapped onto the canonical benchmark scorecard chain.".to_owned(),
            )
        }
    };

    for comparator in causal_chain
        .iter()
        .flat_map(|link| [&link.from_mode_id, &link.to_mode_id])
        .filter_map(|mode| by_mode.get(mode.as_str()).copied())
    {
        if comparator.benchmark_id == summary.benchmark_id {
            continue;
        }
        push_accounting_negative_findings(
            &mut negative_findings,
            summary,
            comparator,
            subject_measurement_count,
            subject_ops_total,
        );
    }

    BenchmarkCausalScorecard {
        schema_version: BENCHMARK_CAUSAL_SCORECARD_SCHEMA_V1.to_owned(),
        benchmark_id: summary.benchmark_id.clone(),
        row_identity,
        baseline_comparator,
        claim_summary,
        causal_chain,
        negative_findings,
        interpretation_note,
    }
}

fn build_causal_chain_link(
    rank: u32,
    baseline: &BenchmarkSummary,
    candidate: &BenchmarkSummary,
    optimization_family: &str,
    claim_summary: String,
    rationale: String,
    total_wall_gain: Option<f64>,
) -> BenchmarkCausalChainLink {
    let baseline_mode = baseline.comparison_mode_id().to_owned();
    let candidate_mode = candidate.comparison_mode_id().to_owned();
    let baseline_wall =
        comparable_counter_scalar(baseline, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS);
    let candidate_wall =
        comparable_counter_scalar(candidate, BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS);
    let attributed_wall_time_delta_ms = baseline_wall - candidate_wall;
    let share_of_total_wall_time_gain_basis_points = total_wall_gain.and_then(|total| {
        if total > 0.0 && attributed_wall_time_delta_ms > 0.0 {
            Some(
                (((attributed_wall_time_delta_ms / total) * 10_000.0).round() as i64)
                    .clamp(0, 10_000) as u32,
            )
        } else {
            None
        }
    });

    let counter_deltas = causal_metric_ids()
        .iter()
        .filter_map(|counter_id| {
            let baseline_metric = comparable_counter_metric(baseline, counter_id)?;
            let candidate_metric = comparable_counter_metric(candidate, counter_id)?;
            Some(build_metric_delta(baseline_metric, candidate_metric))
        })
        .collect::<Vec<_>>();

    let mut evidence = vec![
        format!(
            "transition `{}` -> `{}` uses the aligned counter schema for the same fixture/workload/concurrency row",
            baseline_mode, candidate_mode
        ),
        format!(
            "row bridge: `{}` -> `{}`",
            baseline.benchmark_id, candidate.benchmark_id
        ),
    ];
    if let Some(ref row_id) = scorecard_row_identity(baseline).row_id {
        evidence.push(format!("baseline row id: `{row_id}`"));
    }
    if let Some(ref row_id) = scorecard_row_identity(candidate).row_id {
        evidence.push(format!("candidate row id: `{row_id}`"));
    }

    BenchmarkCausalChainLink {
        rank,
        from_mode_id: baseline_mode,
        to_mode_id: candidate_mode,
        optimization_family: optimization_family.to_owned(),
        claim_summary,
        rationale,
        attributed_wall_time_delta_ms,
        share_of_total_wall_time_gain_basis_points,
        counter_deltas,
        evidence,
    }
}

fn build_metric_delta(
    baseline_metric: &BenchmarkCounterMetric,
    candidate_metric: &BenchmarkCounterMetric,
) -> BenchmarkCausalMetricDelta {
    let baseline_scalar = counter_value_as_f64(&baseline_metric.value);
    let candidate_scalar = counter_value_as_f64(&candidate_metric.value);
    let delta = candidate_scalar - baseline_scalar;
    let preference = counter_preference(&baseline_metric.counter_id);
    let direction = metric_direction(preference, baseline_scalar, candidate_scalar);
    let improvement_pct = metric_improvement_pct(preference, baseline_scalar, candidate_scalar);
    let summary = match improvement_pct {
        Some(pct) if pct.abs() > f64::EPSILON => format!(
            "{:.2} -> {:.2} {} ({:+.2} {}, {:.1}% {})",
            baseline_scalar,
            candidate_scalar,
            baseline_metric.unit,
            delta,
            baseline_metric.unit,
            pct.abs(),
            direction
        ),
        _ => format!(
            "{:.2} -> {:.2} {} ({:+.2} {}, {})",
            baseline_scalar,
            candidate_scalar,
            baseline_metric.unit,
            delta,
            baseline_metric.unit,
            direction
        ),
    };

    BenchmarkCausalMetricDelta {
        counter_id: baseline_metric.counter_id.clone(),
        unit: baseline_metric.unit.clone(),
        baseline_value: baseline_metric.value.clone(),
        candidate_value: candidate_metric.value.clone(),
        delta,
        improvement_pct,
        direction,
        summary,
    }
}

fn comparable_counter_metric<'a>(
    summary: &'a BenchmarkSummary,
    counter_id: &str,
) -> Option<&'a BenchmarkCounterMetric> {
    summary.comparison.as_ref().and_then(|comparison| {
        comparison
            .counter_schema
            .comparable
            .iter()
            .find(|metric| metric.counter_id == counter_id)
    })
}

fn comparable_counter_scalar(summary: &BenchmarkSummary, counter_id: &str) -> f64 {
    comparable_counter_metric(summary, counter_id)
        .map(|metric| counter_value_as_f64(&metric.value))
        .unwrap_or(0.0)
}

fn scorecard_row_identity(summary: &BenchmarkSummary) -> BenchmarkComparisonRowIdentity {
    summary
        .comparison
        .as_ref()
        .map(|comparison| comparison.row_identity.clone())
        .unwrap_or_else(|| BenchmarkComparisonRowIdentity {
            row_id: None,
            fixture_id: summary.fixture_id.clone(),
            workload: summary.workload.clone(),
            concurrency: summary.concurrency,
            mode_id: summary.comparison_mode_id().to_owned(),
            build_profile_id: summary.environment.cargo_profile.clone(),
            placement_profile_id: None,
            run_id: None,
            source_revision: None,
        })
}

fn push_accounting_negative_findings(
    negative_findings: &mut Vec<String>,
    subject: &BenchmarkSummary,
    comparator: &BenchmarkSummary,
    subject_measurement_count: f64,
    subject_ops_total: f64,
) {
    let comparator_measurement_count =
        comparable_counter_scalar(comparator, BENCHMARK_COUNTER_MEASUREMENT_COUNT);
    if (subject_measurement_count - comparator_measurement_count).abs() > f64::EPSILON {
        push_unique_finding(
            negative_findings,
            format!(
                "measurement_iteration_count differs between `{}` ({:.0}) and `{}` ({:.0}); compare with caution",
                subject.comparison_mode_id(),
                subject_measurement_count,
                comparator.comparison_mode_id(),
                comparator_measurement_count
            ),
        );
    }

    let comparator_ops_total =
        comparable_counter_scalar(comparator, BENCHMARK_COUNTER_MEASUREMENT_OPS_TOTAL);
    if (subject_ops_total - comparator_ops_total).abs() > f64::EPSILON {
        push_unique_finding(
            negative_findings,
            format!(
                "measurement_ops_total differs between `{}` ({:.0}) and `{}` ({:.0}); some performance deltas may reflect accounting asymmetry rather than pure engine cost",
                subject.comparison_mode_id(),
                subject_ops_total,
                comparator.comparison_mode_id(),
                comparator_ops_total
            ),
        );
    }
}

fn push_unique_finding(negative_findings: &mut Vec<String>, finding: String) {
    if !negative_findings
        .iter()
        .any(|existing| existing == &finding)
    {
        negative_findings.push(finding);
    }
}

fn dominant_link_note(
    links: &[BenchmarkCausalChainLink],
    total_wall_gain: f64,
    fallback: &str,
) -> String {
    if total_wall_gain <= 0.0 {
        return format!(
            "{fallback} Total wall-time gain is non-positive in this comparison, so the scorecard should be read as a regression/explanation surface rather than as a savings proof."
        );
    }
    let Some(dominant) = links.iter().max_by(|left, right| {
        left.attributed_wall_time_delta_ms
            .total_cmp(&right.attributed_wall_time_delta_ms)
    }) else {
        return fallback.to_owned();
    };

    let share = dominant
        .share_of_total_wall_time_gain_basis_points
        .map(|bps| format!("{:.1}%", f64::from(bps) / 100.0))
        .unwrap_or_else(|| "an unquantified share".to_owned());
    format!(
        "{fallback} Dominant measured wall-time contribution: `{}` ({share} of the positive wall-time gain).",
        dominant.optimization_family
    )
}

fn causal_metric_ids() -> [&'static str; 6] {
    [
        BENCHMARK_COUNTER_MEASUREMENT_WALL_TIME_MS,
        BENCHMARK_COUNTER_LATENCY_MEDIAN_MS,
        BENCHMARK_COUNTER_LATENCY_P95_MS,
        BENCHMARK_COUNTER_THROUGHPUT_MEDIAN_OPS_PER_SEC,
        BENCHMARK_COUNTER_RETRY_TOTAL,
        BENCHMARK_COUNTER_ABORT_TOTAL,
    ]
}

fn benchmark_mode_sort_key(mode: &str) -> (u8, &str) {
    match mode {
        "sqlite_reference" | "sqlite3" => (0, mode),
        "fsqlite_single_writer" => (1, mode),
        "fsqlite_mvcc" => (2, mode),
        _ => (3, mode),
    }
}

fn counter_value_as_f64(value: &BenchmarkCounterValue) -> f64 {
    match value {
        BenchmarkCounterValue::Integer(value) => *value as f64,
        BenchmarkCounterValue::Float(value) => *value,
    }
}

fn counter_preference(counter_id: &str) -> CounterPreference {
    match counter_id {
        BENCHMARK_COUNTER_THROUGHPUT_MEDIAN_OPS_PER_SEC
        | BENCHMARK_COUNTER_THROUGHPUT_PEAK_OPS_PER_SEC
        | BENCHMARK_COUNTER_MEASUREMENT_OPS_TOTAL => CounterPreference::HigherIsBetter,
        BENCHMARK_COUNTER_MEASUREMENT_COUNT => CounterPreference::Invariant,
        _ => CounterPreference::LowerIsBetter,
    }
}

#[allow(clippy::match_same_arms)]
fn metric_direction(preference: CounterPreference, baseline: f64, candidate: f64) -> String {
    let changed = (candidate - baseline).abs() > f64::EPSILON;
    match preference {
        CounterPreference::HigherIsBetter if changed && candidate > baseline => "improvement",
        CounterPreference::HigherIsBetter if changed => "regression",
        CounterPreference::LowerIsBetter if changed && candidate < baseline => "improvement",
        CounterPreference::LowerIsBetter if changed => "regression",
        CounterPreference::Invariant if changed => "mismatch",
        _ => "neutral",
    }
    .to_owned()
}

fn metric_improvement_pct(
    preference: CounterPreference,
    baseline: f64,
    candidate: f64,
) -> Option<f64> {
    if baseline.abs() <= f64::EPSILON {
        return None;
    }
    Some(match preference {
        CounterPreference::HigherIsBetter | CounterPreference::Invariant => {
            ((candidate - baseline) / baseline) * 100.0
        }
        CounterPreference::LowerIsBetter => ((baseline - candidate) / baseline) * 100.0,
    })
}

fn integer_counter(
    counter_id: &str,
    unit: &str,
    semantics: &str,
    aggregation: &str,
    value: u64,
) -> BenchmarkCounterMetric {
    BenchmarkCounterMetric {
        counter_id: counter_id.to_owned(),
        unit: unit.to_owned(),
        aggregation: aggregation.to_owned(),
        semantics: semantics.to_owned(),
        value: BenchmarkCounterValue::Integer(value),
    }
}

fn float_counter(
    counter_id: &str,
    unit: &str,
    semantics: &str,
    aggregation: &str,
    value: f64,
) -> BenchmarkCounterMetric {
    BenchmarkCounterMetric {
        counter_id: counter_id.to_owned(),
        unit: unit.to_owned(),
        aggregation: aggregation.to_owned(),
        semantics: semantics.to_owned(),
        value: BenchmarkCounterValue::Float(value),
    }
}

fn artifact_path(root: &str, name: &str) -> String {
    format!("{root}/{name}")
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
    let mut last_hot_path: Option<FsqliteHotPathProfile> = None;

    let mut measurement_idx: u32 = 0;
    loop {
        let iter_start = std::time::Instant::now();
        let result = iteration_fn(global_idx);
        let iter_elapsed = iter_start.elapsed();

        let record = match result {
            Ok(report) => {
                if let Some(profile) = report.hot_path_profile {
                    last_hot_path = Some(profile);
                }
                IterationRecord {
                    iteration: measurement_idx,
                    wall_time_ms: report.wall_time_ms,
                    ops_per_sec: report.ops_per_sec,
                    ops_total: report.ops_total,
                    retries: report.retries,
                    aborts: report.aborts,
                    error: report.error.clone(),
                }
            }
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
        methodology: config.methodology_meta(),
        environment: EnvironmentMeta::capture(&meta.cargo_profile),
        warmup_count: config.warmup_iterations,
        measurement_count: measurement_idx,
        total_measurement_ms,
        latency,
        throughput,
        comparison: None,
        aggregated_hot_path: last_hot_path,
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
    use std::path::PathBuf;

    use super::*;
    use crate::fixture_select::{
        BenchmarkArtifactCommand, BenchmarkArtifactProvenanceCapture,
        BenchmarkArtifactRetentionClass, BenchmarkArtifactToolVersion, BenchmarkMode,
        PLACEMENT_PROFILE_BASELINE_UNPINNED, build_benchmark_artifact_manifest,
        expand_beads_benchmark_campaign, load_beads_benchmark_campaign,
    };
    use crate::report::{
        BtreeRuntimeHotPathProfile, CorrectnessReport, HotPathRetryBreakdown,
        HotPathRetryKindBreakdown, HotPathRetryPhaseBreakdown, HotPathValueHistogram,
        ParserHotPathProfile, ResultRowHotPathProfile, VdbeHotPathProfile, VfsHotPathProfile,
        WalHotPathProfile,
    };

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
            runtime_phase_timing: None,
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

    fn scorecard_summary(
        mode_id: &str,
        wall_ms: u64,
        ops_total: u64,
        retries: u64,
        aborts: u64,
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
                workload: "mixed_read_write".to_owned(),
                fixture_id: "frankensqlite".to_owned(),
                concurrency: 4,
                cargo_profile: "test".to_owned(),
            },
            |_| {
                let mut report = dummy_report(wall_ms, ops_total);
                report.retries = retries;
                report.aborts = aborts;
                Ok::<_, String>(report)
            },
        );
        summary.comparison = Some(BenchmarkComparisonMetadata::anonymous(&summary, mode_id));
        summary
    }

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root should resolve")
    }

    fn canonical_manifest_for(mode: BenchmarkMode) -> BenchmarkArtifactManifest {
        let workspace_root = workspace_root();
        let campaign =
            load_beads_benchmark_campaign(&workspace_root).expect("campaign manifest should load");
        let cell = expand_beads_benchmark_campaign(&campaign)
            .into_iter()
            .find(|cell| {
                cell.row_id == "mixed_read_write_c4"
                    && cell.fixture_id == "frankensqlite"
                    && cell.mode == mode
                    && cell.placement_profile_id == PLACEMENT_PROFILE_BASELINE_UNPINNED
            })
            .expect("canonical benchmark cell should exist");
        build_benchmark_artifact_manifest(
            &workspace_root,
            &campaign,
            &cell,
            BenchmarkArtifactProvenanceCapture {
                run_id: "bench-20260409T120000Z".to_owned(),
                retention_class: BenchmarkArtifactRetentionClass::FullProof,
                command_entrypoint: "realdb-e2e bench".to_owned(),
                source_revision: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                beads_data_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                kernel_release: "Linux-test".to_owned(),
                commands: vec![BenchmarkArtifactCommand {
                    tool: "realdb-e2e".to_owned(),
                    command_line: "realdb-e2e bench --fixture frankensqlite".to_owned(),
                }],
                tool_versions: vec![BenchmarkArtifactToolVersion {
                    tool: "cargo".to_owned(),
                    version: "cargo test".to_owned(),
                }],
                fallback_notes: Vec::new(),
            },
        )
        .expect("canonical benchmark manifest should build")
    }

    fn comparable_counter_ids(metadata: &BenchmarkComparisonMetadata) -> Vec<&str> {
        metadata
            .counter_schema
            .comparable
            .iter()
            .map(|metric| metric.counter_id.as_str())
            .collect()
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
        assert_eq!(
            summary.methodology.warmup_iterations,
            config.warmup_iterations
        );
        assert_eq!(
            summary.methodology.min_measurement_iterations,
            config.min_iterations
        );
        assert_eq!(
            summary.methodology.measurement_time_secs,
            config.measurement_time_secs
        );
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

    #[test]
    fn anonymous_comparison_metadata_keeps_cross_mode_counter_schema_aligned() {
        let config = fast_config();
        let sqlite_summary = run_benchmark(&config, &test_meta(), |_| {
            Ok::<_, String>(dummy_report(100, 1_000))
        });
        let mvcc_meta = BenchmarkMeta {
            engine: "fsqlite".to_owned(),
            ..test_meta()
        };
        let mvcc_summary = run_benchmark(&config, &mvcc_meta, |_| {
            Ok::<_, String>(dummy_report(100, 1_000))
        });
        let single_writer_meta = BenchmarkMeta {
            engine: "fsqlite".to_owned(),
            ..test_meta()
        };
        let single_writer_summary = run_benchmark(&config, &single_writer_meta, |_| {
            Ok::<_, String>(dummy_report(100, 1_000))
        });

        let sqlite = BenchmarkComparisonMetadata::anonymous(&sqlite_summary, "sqlite_reference");
        let mvcc = BenchmarkComparisonMetadata::anonymous(&mvcc_summary, "fsqlite_mvcc");
        let single_writer =
            BenchmarkComparisonMetadata::anonymous(&single_writer_summary, "fsqlite_single_writer");

        assert_eq!(
            comparable_counter_ids(&sqlite).as_slice(),
            BENCHMARK_COMPARABLE_COUNTER_IDS
        );
        assert_eq!(
            comparable_counter_ids(&mvcc).as_slice(),
            BENCHMARK_COMPARABLE_COUNTER_IDS
        );
        assert_eq!(
            comparable_counter_ids(&single_writer).as_slice(),
            BENCHMARK_COMPARABLE_COUNTER_IDS
        );
        assert!(sqlite.counter_schema.mode_specific.is_empty());
        assert!(mvcc.counter_schema.mode_specific.is_empty());
        assert!(single_writer.counter_schema.mode_specific.is_empty());
        assert_eq!(sqlite.row_identity.build_profile_id, "test");
        assert_eq!(mvcc.row_identity.build_profile_id, "test");
        assert_eq!(single_writer.row_identity.build_profile_id, "test");
    }

    #[test]
    fn canonical_comparison_metadata_uses_manifest_row_identity_and_artifact_layout() {
        let summary = run_benchmark(&fast_config(), &test_meta(), |_| {
            Ok::<_, String>(dummy_report(100, 1_000))
        });
        let manifest = canonical_manifest_for(BenchmarkMode::SqliteReference);
        let metadata = BenchmarkComparisonMetadata::canonical(
            &summary,
            manifest.clone(),
            Some("linux:x86_64:any".to_owned()),
        );

        assert_eq!(
            metadata.row_identity.row_id.as_deref(),
            Some("mixed_read_write_c4")
        );
        assert_eq!(metadata.row_identity.fixture_id, "frankensqlite");
        assert_eq!(metadata.row_identity.workload, "mixed_read_write");
        assert_eq!(metadata.row_identity.concurrency, 4);
        assert_eq!(metadata.row_identity.mode_id.as_str(), "sqlite_reference");
        assert_eq!(
            metadata.row_identity.build_profile_id.as_str(),
            manifest.build_profile_id.as_str()
        );
        assert_eq!(
            metadata.row_identity.placement_profile_id.as_deref(),
            Some(PLACEMENT_PROFILE_BASELINE_UNPINNED)
        );
        assert_eq!(
            metadata.row_identity.run_id.as_deref(),
            Some("bench-20260409T120000Z")
        );
        assert_eq!(
            metadata.row_identity.source_revision.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );

        let layout = metadata
            .artifact_layout
            .as_ref()
            .expect("canonical layout should exist");
        assert_eq!(
            layout.artifact_bundle_key.as_str(),
            manifest.artifact_bundle_key.as_str()
        );
        assert_eq!(
            layout.artifact_bundle_relpath.as_str(),
            manifest.artifact_bundle_relpath.as_str()
        );
        assert_eq!(
            layout.artifact_manifest_path.as_str(),
            format!(
                "{}/{}",
                manifest.artifact_bundle_relpath, manifest.artifact_names.manifest_json
            )
        );
        assert_eq!(
            layout.result_jsonl_path.as_str(),
            format!(
                "{}/{}",
                manifest.artifact_bundle_relpath, manifest.artifact_names.result_jsonl
            )
        );
        assert_eq!(
            layout.summary_md_path.as_str(),
            format!(
                "{}/{}",
                manifest.artifact_bundle_relpath, manifest.artifact_names.summary_md
            )
        );
        assert_eq!(
            metadata.provenance.retry_policy_id.as_deref(),
            Some(manifest.retry_policy_id.as_str())
        );
        assert_eq!(
            metadata.provenance.seed_policy_id.as_deref(),
            Some(manifest.seed_policy_id.as_str())
        );
        assert_eq!(
            metadata.provenance.hardware_class_id.as_deref(),
            Some(manifest.hardware_class_id.as_str())
        );
        assert_eq!(
            metadata.provenance.hardware_signature.as_deref(),
            Some("linux:x86_64:any")
        );
        assert_eq!(
            metadata.provenance.beads_data_hash.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn causal_scorecard_report_partitions_three_mode_wall_time_gain() {
        let sqlite = scorecard_summary("sqlite_reference", 100, 1_000, 6, 2);
        let single_writer = scorecard_summary("fsqlite_single_writer", 70, 1_000, 2, 1);
        let mvcc = scorecard_summary("fsqlite_mvcc", 50, 1_000, 0, 0);

        let report = build_benchmark_causal_scorecard_report(&[
            sqlite.clone(),
            single_writer.clone(),
            mvcc.clone(),
        ]);

        assert_eq!(
            report.schema_version,
            BENCHMARK_CAUSAL_SCORECARD_REPORT_SCHEMA_V1
        );
        assert_eq!(report.groups.len(), 1);
        let group = &report.groups[0];
        assert_eq!(group.scorecards.len(), 3);

        let mvcc_scorecard = group
            .scorecards
            .iter()
            .find(|scorecard| scorecard.row_identity.mode_id == "fsqlite_mvcc")
            .expect("mvcc scorecard should exist");
        assert_eq!(mvcc_scorecard.baseline_comparator, "sqlite_reference");
        assert_eq!(mvcc_scorecard.causal_chain.len(), 2);
        assert_eq!(
            mvcc_scorecard.causal_chain[0].optimization_family,
            "shared_fixed_tax_reduction"
        );
        assert_eq!(
            mvcc_scorecard.causal_chain[1].optimization_family,
            "mvcc_concurrency_routing"
        );
        assert!(
            (mvcc_scorecard.causal_chain[0].attributed_wall_time_delta_ms - 30.0).abs()
                < f64::EPSILON
        );
        assert!(
            (mvcc_scorecard.causal_chain[1].attributed_wall_time_delta_ms - 20.0).abs()
                < f64::EPSILON
        );
        assert_eq!(
            mvcc_scorecard.causal_chain[0].share_of_total_wall_time_gain_basis_points,
            Some(6000)
        );
        assert_eq!(
            mvcc_scorecard.causal_chain[1].share_of_total_wall_time_gain_basis_points,
            Some(4000)
        );
        assert!(
            mvcc_scorecard
                .interpretation_note
                .contains("shared_fixed_tax_reduction")
                || mvcc_scorecard
                    .interpretation_note
                    .contains("mvcc_concurrency_routing")
        );
    }

    #[test]
    fn causal_scorecard_flags_missing_bridge_row_for_mvcc() {
        let sqlite = scorecard_summary("sqlite_reference", 100, 1_000, 4, 1);
        let mvcc = scorecard_summary("fsqlite_mvcc", 65, 1_000, 1, 0);

        let report = build_benchmark_causal_scorecard_report(&[sqlite, mvcc]);
        let mvcc_scorecard = report.groups[0]
            .scorecards
            .iter()
            .find(|scorecard| scorecard.row_identity.mode_id == "fsqlite_mvcc")
            .expect("mvcc scorecard should exist");

        assert_eq!(mvcc_scorecard.causal_chain.len(), 1);
        assert_eq!(
            mvcc_scorecard.causal_chain[0].optimization_family,
            "combined_shared_and_mvcc_gain"
        );
        assert!(
            mvcc_scorecard
                .negative_findings
                .iter()
                .any(|finding| finding.contains("fsqlite_single_writer row is missing"))
        );
    }

    fn sample_hot_path_profile() -> FsqliteHotPathProfile {
        FsqliteHotPathProfile {
            collection_mode: "test".to_owned(),
            parser: ParserHotPathProfile {
                tokenize_tokens_total: 500,
                tokenize_calls_total: 42,
                tokenize_duration_sum_micros: 1200,
                parsed_statements_total: 20,
                semantic_errors_total: 0,
            },
            vdbe: VdbeHotPathProfile {
                actual_opcodes_executed_total: 8_000,
                actual_statements_total: 20,
                actual_statement_duration_us_total: 5_000,
                ..VdbeHotPathProfile::default()
            },
            vfs: VfsHotPathProfile {
                read_ops: 150,
                write_ops: 75,
                ..VfsHotPathProfile::default()
            },
            wal: WalHotPathProfile {
                frames_written_total: 30,
                group_commits_total: 5,
                ..WalHotPathProfile::default()
            },
            decoded_values: HotPathValueHistogram::default(),
            workload_input_types: HotPathValueHistogram::default(),
            result_rows: ResultRowHotPathProfile::default(),
            allocator_pressure: None,
            btree: Some(BtreeRuntimeHotPathProfile {
                seek_total: 200,
                insert_total: 100,
                delete_total: 10,
                page_splits_total: 3,
                swiss_probes_total: 50,
                swizzle_faults_total: 0,
                swizzle_in_total: 0,
                swizzle_out_total: 0,
            }),
            runtime_retry: HotPathRetryBreakdown {
                total_retries: 2,
                total_aborts: 0,
                kind: HotPathRetryKindBreakdown {
                    busy: 1,
                    busy_snapshot: 1,
                    busy_recovery: 0,
                    busy_other: 0,
                },
                phase: HotPathRetryPhaseBreakdown::default(),
                max_batch_attempts: 2,
                top_snapshot_conflict_pages: Vec::new(),
                last_busy_message: None,
            },
            statement_hotspots: Vec::new(),
        }
    }

    fn dummy_report_with_hot_path(wall_ms: u64, ops: u64) -> EngineRunReport {
        let mut report = dummy_report(wall_ms, ops);
        report.hot_path_profile = Some(sample_hot_path_profile());
        report
    }

    #[test]
    fn mode_specific_counters_populated_from_hot_path_profile() {
        let config = fast_config();
        let fsqlite_meta = BenchmarkMeta {
            engine: "fsqlite".to_owned(),
            ..test_meta()
        };
        let summary = run_benchmark(&config, &fsqlite_meta, |_| {
            Ok::<_, String>(dummy_report_with_hot_path(100, 1_000))
        });
        assert!(
            summary.aggregated_hot_path.is_some(),
            "run_benchmark should capture the last iteration's hot_path_profile"
        );

        let metadata = BenchmarkComparisonMetadata::anonymous(&summary, "fsqlite_mvcc");

        assert_eq!(
            comparable_counter_ids(&metadata).as_slice(),
            BENCHMARK_COMPARABLE_COUNTER_IDS,
            "comparable counters must be unchanged"
        );

        let mode_ids: Vec<&str> = metadata
            .counter_schema
            .mode_specific
            .iter()
            .map(|m| m.counter_id.as_str())
            .collect();
        assert!(
            mode_ids.contains(&MODE_SPECIFIC_COUNTER_VDBE_OPCODES),
            "should contain VDBE opcodes counter"
        );
        assert!(
            mode_ids.contains(&MODE_SPECIFIC_COUNTER_BTREE_SEEKS),
            "should contain B-tree seeks counter"
        );
        assert!(
            mode_ids.contains(&MODE_SPECIFIC_COUNTER_WAL_FRAMES),
            "should contain WAL frames counter"
        );
        assert!(
            mode_ids.len() >= 9,
            "should have at least 9 mode-specific counters (9 base + 3 btree), got {}",
            mode_ids.len()
        );
    }

    #[test]
    fn cross_mode_rows_mechanically_comparable_without_custom_translation() {
        let config = fast_config();

        let sqlite_summary = run_benchmark(&config, &test_meta(), |_| {
            Ok::<_, String>(dummy_report(100, 1_000))
        });
        let mvcc_summary = run_benchmark(
            &config,
            &BenchmarkMeta {
                engine: "fsqlite".to_owned(),
                ..test_meta()
            },
            |_| Ok::<_, String>(dummy_report_with_hot_path(80, 1_000)),
        );
        let sw_summary = run_benchmark(
            &config,
            &BenchmarkMeta {
                engine: "fsqlite".to_owned(),
                ..test_meta()
            },
            |_| Ok::<_, String>(dummy_report_with_hot_path(90, 1_000)),
        );

        let sqlite_meta =
            BenchmarkComparisonMetadata::anonymous(&sqlite_summary, "sqlite_reference");
        let mvcc_meta = BenchmarkComparisonMetadata::anonymous(&mvcc_summary, "fsqlite_mvcc");
        let sw_meta = BenchmarkComparisonMetadata::anonymous(&sw_summary, "fsqlite_single_writer");

        let sqlite_comparable_ids = comparable_counter_ids(&sqlite_meta);
        let mvcc_comparable_ids = comparable_counter_ids(&mvcc_meta);
        let sw_comparable_ids = comparable_counter_ids(&sw_meta);
        assert_eq!(
            sqlite_comparable_ids, mvcc_comparable_ids,
            "comparable counter ids must be identical across SQLite and MVCC"
        );
        assert_eq!(
            mvcc_comparable_ids, sw_comparable_ids,
            "comparable counter ids must be identical across MVCC and single-writer"
        );

        assert!(
            sqlite_meta.counter_schema.mode_specific.is_empty(),
            "SQLite reference should have no mode-specific counters"
        );
        assert!(
            !mvcc_meta.counter_schema.mode_specific.is_empty(),
            "MVCC with hot-path should have mode-specific counters"
        );
        assert!(
            !sw_meta.counter_schema.mode_specific.is_empty(),
            "single-writer with hot-path should have mode-specific counters"
        );

        let mvcc_mode_ids: Vec<&str> = mvcc_meta
            .counter_schema
            .mode_specific
            .iter()
            .map(|m| m.counter_id.as_str())
            .collect();
        let sw_mode_ids: Vec<&str> = sw_meta
            .counter_schema
            .mode_specific
            .iter()
            .map(|m| m.counter_id.as_str())
            .collect();
        assert_eq!(
            mvcc_mode_ids, sw_mode_ids,
            "FrankenSQLite mode-specific counter ids must be identical across MVCC and single-writer"
        );

        for mode in [&sqlite_meta, &mvcc_meta, &sw_meta] {
            assert!(
                !mode.row_identity.fixture_id.is_empty(),
                "row_identity.fixture_id must be present"
            );
            assert!(
                !mode.row_identity.workload.is_empty(),
                "row_identity.workload must be present"
            );
            assert!(
                !mode.row_identity.mode_id.is_empty(),
                "row_identity.mode_id must be present"
            );
            assert!(
                !mode.row_identity.build_profile_id.is_empty(),
                "row_identity.build_profile_id must be present"
            );
        }

        let json_sqlite = serde_json::to_value(&sqlite_meta).unwrap();
        let json_mvcc = serde_json::to_value(&mvcc_meta).unwrap();
        let json_sw = serde_json::to_value(&sw_meta).unwrap();
        for json_row in [&json_sqlite, &json_mvcc, &json_sw] {
            let obj = json_row.as_object().unwrap();
            assert!(obj.contains_key("row_identity"));
            assert!(obj.contains_key("counter_schema"));
            let cs = obj["counter_schema"].as_object().unwrap();
            assert!(cs.contains_key("comparable"));
            assert!(cs.contains_key("mode_specific"));
        }
    }
}
