//! Performance benchmark matrix runner.
//!
//! Bead: bd-2als.2.2
//!
//! Orchestrates repeated benchmark execution across a matrix of:
//! - **Engine**: sqlite3 (rusqlite) vs fsqlite
//! - **Fixture**: golden database files
//! - **Workload**: OpLog preset names
//! - **Concurrency**: worker counts (1, 2, 4, 8, …)
//!
//! For each matrix cell, the runner uses [`crate::benchmark::run_benchmark`]
//! to perform warmup + measurement iterations, producing a
//! [`crate::benchmark::BenchmarkSummary`] per cell.  Results are collected
//! into a [`PerfResult`] and can be serialized to JSONL for downstream
//! analysis and reporting.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use fsqlite_core::connection::{
    HotPathProfileSnapshot, ParserHotPathProfileSnapshot, hot_path_profile_enabled,
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};

use crate::HarnessSettings;
use crate::benchmark::{BenchmarkConfig, BenchmarkMeta, BenchmarkSummary, run_benchmark};
use crate::fsqlite_executor::run_oplog_fsqlite;
use crate::oplog::{self, OpLog};
use crate::report::EngineRunReport;
use crate::run_workspace::{WorkspaceConfig, create_workspace_with_label};
use crate::sqlite_executor::run_oplog_sqlite;

// ── Configuration ──────────────────────────────────────────────────────

/// Which database engine to benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Engine {
    /// C SQLite via rusqlite.
    Sqlite3,
    /// FrankenSQLite.
    Fsqlite,
}

impl Engine {
    /// Returns the canonical string name used in benchmark IDs and reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite3 => "sqlite3",
            Self::Fsqlite => "fsqlite",
        }
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Configuration for the performance benchmark matrix.
#[derive(Debug, Clone)]
pub struct PerfMatrixConfig {
    /// Engines to benchmark.
    pub engines: Vec<Engine>,
    /// Fixture IDs from the golden corpus to test against.
    pub fixture_ids: Vec<String>,
    /// Workload preset names (must match names in `oplog::preset_catalog()`).
    pub workload_presets: Vec<String>,
    /// Concurrency levels to sweep.
    pub concurrency_levels: Vec<u16>,
    /// Seed for deterministic workload generation.
    pub seed: u64,
    /// Scale factor for workload size (row counts, etc.).
    pub scale: u32,
    /// Path to the project root (for locating golden fixtures).
    pub project_root: PathBuf,
    /// Benchmark configuration (warmup, min iterations, time floor).
    pub benchmark_config: BenchmarkConfig,
    /// Harness settings (PRAGMAs, journal mode, etc.).
    pub settings: HarnessSettings,
    /// Cargo profile string for metadata (e.g. `"release"`).
    pub cargo_profile: String,
    /// If true, stop on first error.
    pub fail_fast: bool,
}

/// A single cell in the benchmark matrix.
#[derive(Debug, Clone)]
pub struct MatrixCell {
    pub engine: Engine,
    pub fixture_id: String,
    pub workload: String,
    pub concurrency: u16,
}

impl std::fmt::Display for MatrixCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}:c{}",
            self.engine, self.workload, self.fixture_id, self.concurrency
        )
    }
}

// ── Result types ───────────────────────────────────────────────────────

/// Outcome of a single matrix cell benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellOutcome {
    /// The benchmark summary (present on success).
    pub summary: Option<BenchmarkSummary>,
    /// Error message if the cell failed entirely.
    pub error: Option<String>,
    /// Engine name.
    pub engine: String,
    /// Fixture ID.
    pub fixture_id: String,
    /// Workload preset name.
    pub workload: String,
    /// Concurrency level.
    pub concurrency: u16,
}

/// Complete result of running the performance matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfResult {
    /// Schema version for forward compatibility.
    pub schema_version: String,
    /// Total cells in the matrix.
    pub total_cells: usize,
    /// Number of cells that completed successfully.
    pub success_count: usize,
    /// Number of cells that errored.
    pub error_count: usize,
    /// Per-cell outcomes.
    pub cells: Vec<CellOutcome>,
}

/// Schema version for the perf result JSONL format.
pub const PERF_RESULT_SCHEMA_V1: &str = "fsqlite-e2e.perf_result.v1";
/// Schema version for mixed-read-write hot-path profile reports.
pub const HOT_PATH_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_profile.v1";
/// Schema version for hot-path artifact manifests.
pub const HOT_PATH_PROFILE_MANIFEST_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_profile_manifest.v1";
/// Schema version for raw opcode profile packs.
pub const HOT_PATH_OPCODE_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_opcode_profile.v1";
/// Schema version for raw subsystem profile packs.
pub const HOT_PATH_SUBSYSTEM_PROFILE_SCHEMA_V1: &str = "fsqlite-e2e.hot_path_subsystem_profile.v1";
/// Schema version for structured D1 actionable hotspot ranking artifacts.
pub const HOT_PATH_PROFILE_ACTIONABLE_RANKING_SCHEMA_V2: &str =
    "fsqlite-e2e.hot_path_actionable_ranking.v2";
/// Bead identifier for the hot-path profiling work.
pub const HOT_PATH_PROFILE_BEAD_ID: &str = "bd-db300.4.1";
/// Canonical scenario identifier for the mixed read/write hot path.
pub const HOT_PATH_PROFILE_SCENARIO_ID: &str = "bd-db300.4.1.mixed_read_write";

/// Configuration for a focused FrankenSQLite mixed-read-write hot-path profile.
#[derive(Debug, Clone)]
pub struct FsqliteHotPathProfileConfig {
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub exec_config: crate::fsqlite_executor::FsqliteExecConfig,
    pub replay_command: String,
    pub golden_dir: Option<String>,
    pub working_base: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathValueTypeProfile {
    pub total_values: u64,
    pub nulls: u64,
    pub integers: u64,
    pub reals: u64,
    pub texts: u64,
    pub blobs: u64,
    pub text_bytes_total: u64,
    pub blob_bytes_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathParserProfile {
    pub parse_single_calls: u64,
    pub parse_multi_calls: u64,
    pub parse_cache_hits: u64,
    pub parse_cache_misses: u64,
    pub parsed_sql_bytes: u64,
    pub parse_time_ns: u64,
    pub rewrite_calls: u64,
    pub rewrite_time_ns: u64,
    pub compiled_cache_hits: u64,
    pub compiled_cache_misses: u64,
    pub compile_time_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathRecordDecodeProfile {
    pub parse_record_calls: u64,
    pub parse_record_into_calls: u64,
    pub parse_record_column_calls: u64,
    pub record_bytes_scanned: u64,
    pub record_vec_capacity_slots: u64,
    pub decode_time_ns: u64,
    pub decoded_values: HotPathValueTypeProfile,
    pub vdbe_record_decode_calls_total: u64,
    pub vdbe_column_reads_total: u64,
    pub vdbe_decoded_value_heap_bytes_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathRowMaterializationProfile {
    pub result_rows_total: u64,
    pub result_values_total: u64,
    pub result_value_heap_bytes_total: u64,
    pub result_row_materialization_time_ns_total: u64,
    pub make_record_calls_total: u64,
    pub make_record_blob_bytes_total: u64,
    pub value_types: HotPathValueTypeProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathOpcodeProfileEntry {
    pub opcode: String,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathOpcodeProfilePack {
    pub schema_version: String,
    pub bead_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub replay_command: String,
    pub opcodes: Vec<HotPathOpcodeProfileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathTypeProfile {
    pub decoded: HotPathValueTypeProfile,
    pub materialized: HotPathValueTypeProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathSubsystemProfilePack {
    pub schema_version: String,
    pub bead_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub replay_command: String,
    pub subsystem_ranking: Vec<HotPathRankingEntry>,
    pub allocator_ranking: Vec<HotPathRankingEntry>,
    pub parser: HotPathParserProfile,
    pub record_decode: HotPathRecordDecodeProfile,
    pub row_materialization: HotPathRowMaterializationProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathRankingEntry {
    pub subsystem: String,
    pub metric_kind: String,
    pub metric_value: u64,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathActionableRankingEntry {
    pub rank: u32,
    pub subsystem: String,
    pub metric_kind: String,
    pub metric_value: u64,
    pub rationale: String,
    pub implication: String,
    pub mapped_beads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathCostComponentEntry {
    pub rank: u32,
    pub component: String,
    pub time_ns: u64,
    pub time_share_basis_points: u32,
    pub allocator_pressure_bytes: u64,
    pub allocator_share_basis_points: u32,
    pub activity_count: u64,
    pub rationale: String,
    pub implication: String,
    pub mapped_beads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathAllocatorPressure {
    pub parser_sql_bytes: u64,
    pub decoded_value_heap_bytes_total: u64,
    pub result_value_heap_bytes_total: u64,
    pub record_vec_capacity_slots: u64,
    pub ranked_sources: Vec<HotPathRankingEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotPathProfileReport {
    pub schema_version: String,
    pub bead_id: String,
    pub scenario_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub concurrent_mode: bool,
    pub run_integrity_check: bool,
    pub golden_dir: Option<String>,
    pub working_base: Option<String>,
    pub replay_command: String,
    pub engine_report: EngineRunReport,
    pub parser: HotPathParserProfile,
    pub record_decode: HotPathRecordDecodeProfile,
    pub row_materialization: HotPathRowMaterializationProfile,
    pub opcode_profile: Vec<HotPathOpcodeProfileEntry>,
    pub type_profile: HotPathTypeProfile,
    pub subsystem_ranking: Vec<HotPathRankingEntry>,
    pub allocator_pressure: HotPathAllocatorPressure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathActionableRanking {
    pub schema_version: String,
    pub bead_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub replay_command: String,
    pub named_hotspots: Vec<HotPathActionableRankingEntry>,
    pub cost_components: Vec<HotPathCostComponentEntry>,
    pub allocator_pressure: Vec<HotPathActionableRankingEntry>,
    pub top_opcodes: Vec<HotPathOpcodeProfileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathArtifactFile {
    pub path: String,
    pub bytes: u64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HotPathArtifactManifest {
    pub schema_version: String,
    pub bead_id: String,
    pub run_id: String,
    pub trace_id: String,
    pub scenario_id: String,
    pub fixture_id: String,
    pub workload: String,
    pub seed: u64,
    pub scale: u32,
    pub concurrency: u16,
    pub concurrent_mode: bool,
    pub run_integrity_check: bool,
    pub golden_dir: Option<String>,
    pub working_base: Option<String>,
    pub replay_command: String,
    pub files: Vec<HotPathArtifactFile>,
}

struct HotPathProfileScope {
    was_enabled: bool,
}

impl HotPathProfileScope {
    fn enable() -> Self {
        let was_enabled = hot_path_profile_enabled();
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        Self { was_enabled }
    }
}

impl Drop for HotPathProfileScope {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(self.was_enabled);
    }
}

impl PerfResult {
    /// Returns true if all cells completed successfully.
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.error_count == 0
    }

    /// Serialize all cell outcomes to JSONL (one JSON object per line).
    ///
    /// # Errors
    ///
    /// Returns a serialization error if any cell cannot be serialized.
    pub fn to_jsonl(&self) -> Result<String, serde_json::Error> {
        let mut out = String::new();
        for cell in &self.cells {
            out.push_str(&serde_json::to_string(cell)?);
            out.push('\n');
        }
        Ok(out)
    }
}

struct HotPathValueMetrics {
    total_values: u64,
    nulls: u64,
    integers: u64,
    reals: u64,
    texts: u64,
    blobs: u64,
    text_bytes_total: u64,
    blob_bytes_total: u64,
}

#[must_use]
fn hot_path_value_profile(metrics: HotPathValueMetrics) -> HotPathValueTypeProfile {
    HotPathValueTypeProfile {
        total_values: metrics.total_values,
        nulls: metrics.nulls,
        integers: metrics.integers,
        reals: metrics.reals,
        texts: metrics.texts,
        blobs: metrics.blobs,
        text_bytes_total: metrics.text_bytes_total,
        blob_bytes_total: metrics.blob_bytes_total,
    }
}

#[must_use]
fn parser_profile(snapshot: ParserHotPathProfileSnapshot) -> HotPathParserProfile {
    HotPathParserProfile {
        parse_single_calls: snapshot.parse_single_calls,
        parse_multi_calls: snapshot.parse_multi_calls,
        parse_cache_hits: snapshot.parse_cache_hits,
        parse_cache_misses: snapshot.parse_cache_misses,
        parsed_sql_bytes: snapshot.parsed_sql_bytes,
        parse_time_ns: snapshot.parse_time_ns,
        rewrite_calls: snapshot.rewrite_calls,
        rewrite_time_ns: snapshot.rewrite_time_ns,
        compiled_cache_hits: snapshot.compiled_cache_hits,
        compiled_cache_misses: snapshot.compiled_cache_misses,
        compile_time_ns: snapshot.compile_time_ns,
    }
}

#[must_use]
fn build_hot_path_profile_report(
    fixture_id: &str,
    config: &FsqliteHotPathProfileConfig,
    run_id: String,
    trace_id: String,
    engine_report: EngineRunReport,
    snapshot: HotPathProfileSnapshot,
) -> HotPathProfileReport {
    let decoded_types = hot_path_value_profile(HotPathValueMetrics {
        total_values: snapshot.record_decode.decoded_values.total_values(),
        nulls: snapshot.record_decode.decoded_values.null_count,
        integers: snapshot.record_decode.decoded_values.integer_count,
        reals: snapshot.record_decode.decoded_values.float_count,
        texts: snapshot.record_decode.decoded_values.text_count,
        blobs: snapshot.record_decode.decoded_values.blob_count,
        text_bytes_total: snapshot.record_decode.decoded_values.text_bytes,
        blob_bytes_total: snapshot.record_decode.decoded_values.blob_bytes,
    });
    let materialized_types = hot_path_value_profile(HotPathValueMetrics {
        total_values: snapshot.vdbe.result_value_types.total_values,
        nulls: snapshot.vdbe.result_value_types.nulls,
        integers: snapshot.vdbe.result_value_types.integers,
        reals: snapshot.vdbe.result_value_types.reals,
        texts: snapshot.vdbe.result_value_types.texts,
        blobs: snapshot.vdbe.result_value_types.blobs,
        text_bytes_total: snapshot.vdbe.result_value_types.text_bytes_total,
        blob_bytes_total: snapshot.vdbe.result_value_types.blob_bytes_total,
    });

    let opcode_profile = snapshot
        .vdbe
        .opcode_execution_totals
        .iter()
        .map(|entry| HotPathOpcodeProfileEntry {
            opcode: entry.opcode.clone(),
            total: entry.total,
        })
        .collect();

    let parser = parser_profile(snapshot.parser);
    let parser_sql_bytes = parser.parsed_sql_bytes;
    let record_decode = HotPathRecordDecodeProfile {
        parse_record_calls: snapshot.record_decode.parse_record_calls,
        parse_record_into_calls: snapshot.record_decode.parse_record_into_calls,
        parse_record_column_calls: snapshot.record_decode.parse_record_column_calls,
        record_bytes_scanned: snapshot.record_decode.record_bytes_scanned,
        record_vec_capacity_slots: snapshot.record_decode.record_vec_capacity_slots,
        decode_time_ns: snapshot.record_decode.decode_time_ns,
        decoded_values: decoded_types.clone(),
        vdbe_record_decode_calls_total: snapshot.vdbe.record_decode_calls_total,
        vdbe_column_reads_total: snapshot.vdbe.column_reads_total,
        vdbe_decoded_value_heap_bytes_total: snapshot.vdbe.decoded_value_heap_bytes_total,
    };
    let row_materialization = HotPathRowMaterializationProfile {
        result_rows_total: snapshot.vdbe.result_rows_total,
        result_values_total: snapshot.vdbe.result_values_total,
        result_value_heap_bytes_total: snapshot.vdbe.result_value_heap_bytes_total,
        result_row_materialization_time_ns_total: snapshot
            .vdbe
            .result_row_materialization_time_ns_total,
        make_record_calls_total: snapshot.vdbe.make_record_calls_total,
        make_record_blob_bytes_total: snapshot.vdbe.make_record_blob_bytes_total,
        value_types: materialized_types.clone(),
    };

    let parser_time_ns = parser
        .parse_time_ns
        .saturating_add(parser.rewrite_time_ns)
        .saturating_add(parser.compile_time_ns);
    let mut subsystem_ranking = vec![
        HotPathRankingEntry {
            subsystem: "parser_ast_churn".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: parser_time_ns,
            rationale: "parse + rewrite + compile time on the connection path".to_owned(),
        },
        HotPathRankingEntry {
            subsystem: "record_decode".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: record_decode.decode_time_ns,
            rationale: "time spent decoding SQLite record payloads".to_owned(),
        },
        HotPathRankingEntry {
            subsystem: "row_materialization".to_owned(),
            metric_kind: "time_ns".to_owned(),
            metric_value: row_materialization.result_row_materialization_time_ns_total,
            rationale: "time spent cloning registers into emitted result rows".to_owned(),
        },
    ];
    subsystem_ranking.sort_by(|lhs, rhs| {
        rhs.metric_value
            .cmp(&lhs.metric_value)
            .then_with(|| lhs.subsystem.cmp(&rhs.subsystem))
    });

    let mut allocator_ranking = vec![
        HotPathRankingEntry {
            subsystem: "result_row_values".to_owned(),
            metric_kind: "heap_bytes".to_owned(),
            metric_value: row_materialization.result_value_heap_bytes_total,
            rationale: "estimated heap carried by emitted result values".to_owned(),
        },
        HotPathRankingEntry {
            subsystem: "record_decode_values".to_owned(),
            metric_kind: "heap_bytes".to_owned(),
            metric_value: record_decode.vdbe_decoded_value_heap_bytes_total,
            rationale: "estimated heap carried by decoded record/column values".to_owned(),
        },
        HotPathRankingEntry {
            subsystem: "parser_sql_bytes".to_owned(),
            metric_kind: "bytes".to_owned(),
            metric_value: parser.parsed_sql_bytes,
            rationale: "SQL text volume parsed on cache-miss paths".to_owned(),
        },
    ];
    allocator_ranking.sort_by(|lhs, rhs| {
        rhs.metric_value
            .cmp(&lhs.metric_value)
            .then_with(|| lhs.subsystem.cmp(&rhs.subsystem))
    });

    HotPathProfileReport {
        schema_version: HOT_PATH_PROFILE_SCHEMA_V1.to_owned(),
        bead_id: HOT_PATH_PROFILE_BEAD_ID.to_owned(),
        scenario_id: HOT_PATH_PROFILE_SCENARIO_ID.to_owned(),
        run_id,
        trace_id,
        fixture_id: fixture_id.to_owned(),
        workload: "mixed_read_write".to_owned(),
        seed: config.seed,
        scale: config.scale,
        concurrency: config.concurrency,
        concurrent_mode: config.exec_config.concurrent_mode,
        run_integrity_check: config.exec_config.run_integrity_check,
        golden_dir: config.golden_dir.clone(),
        working_base: config.working_base.clone(),
        replay_command: config.replay_command.clone(),
        engine_report,
        parser,
        record_decode,
        row_materialization,
        opcode_profile,
        type_profile: HotPathTypeProfile {
            decoded: decoded_types,
            materialized: materialized_types,
        },
        subsystem_ranking,
        allocator_pressure: HotPathAllocatorPressure {
            parser_sql_bytes,
            decoded_value_heap_bytes_total: snapshot.vdbe.decoded_value_heap_bytes_total,
            result_value_heap_bytes_total: snapshot.vdbe.result_value_heap_bytes_total,
            record_vec_capacity_slots: snapshot.record_decode.record_vec_capacity_slots,
            ranked_sources: allocator_ranking,
        },
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub fn profile_fsqlite_mixed_read_write_hot_path(
    db_path: &Path,
    fixture_id: &str,
    config: &FsqliteHotPathProfileConfig,
) -> crate::E2eResult<HotPathProfileReport> {
    if config.concurrency == 0 {
        return Err(crate::E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hot-path profile concurrency must be >= 1",
        )));
    }

    let now_ms = unix_timestamp_millis();
    let run_id = format!(
        "{HOT_PATH_PROFILE_BEAD_ID}-{fixture_id}-c{}-s{}-{now_ms}",
        config.concurrency, config.seed
    );
    let trace_id = format!(
        "{HOT_PATH_PROFILE_SCENARIO_ID}:{fixture_id}:c{}",
        config.concurrency
    );
    let oplog = generate_oplog(
        "mixed_read_write",
        fixture_id,
        config.seed,
        config.concurrency,
        config.scale,
    )
    .ok_or_else(|| {
        crate::E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "mixed_read_write preset unavailable",
        ))
    })?;

    let _scope = HotPathProfileScope::enable();
    let engine_report = run_oplog_fsqlite(db_path, &oplog, &config.exec_config)?;
    let snapshot = hot_path_profile_snapshot();
    Ok(build_hot_path_profile_report(
        fixture_id,
        config,
        run_id,
        trace_id,
        engine_report,
        snapshot,
    ))
}

#[must_use]
pub fn render_hot_path_profile_markdown(report: &HotPathProfileReport) -> String {
    let actionable_ranking = build_hot_path_actionable_ranking(report);
    let mut out = String::with_capacity(4096);
    let _ = writeln!(out, "# Mixed Read/Write Hot-Path Profile\n");
    let _ = writeln!(out, "- Bead: `{}`", report.bead_id);
    let _ = writeln!(out, "- Run ID: `{}`", report.run_id);
    let _ = writeln!(out, "- Trace ID: `{}`", report.trace_id);
    let _ = writeln!(out, "- Scenario: `{}`", report.scenario_id);
    let _ = writeln!(out, "- Fixture: `{}`", report.fixture_id);
    let _ = writeln!(out, "- Workload: `{}`", report.workload);
    let _ = writeln!(out, "- Seed: `{}`", report.seed);
    let _ = writeln!(out, "- Concurrency: `{}`", report.concurrency);
    let _ = writeln!(out, "- Scale: `{}`", report.scale);
    let _ = writeln!(
        out,
        "- Concurrent mode: `{}`",
        if report.concurrent_mode { "ON" } else { "OFF" }
    );
    let _ = writeln!(
        out,
        "- Integrity check: `{}`",
        if report.run_integrity_check {
            "enabled"
        } else {
            "disabled"
        }
    );
    if let Some(golden_dir) = &report.golden_dir {
        let _ = writeln!(out, "- Golden dir: `{golden_dir}`");
    }
    if let Some(working_base) = &report.working_base {
        let _ = writeln!(out, "- Working base: `{working_base}`");
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Engine Summary\n");
    let _ = writeln!(
        out,
        "- Wall time (ms): {}",
        report.engine_report.wall_time_ms
    );
    let _ = writeln!(out, "- Ops total: {}", report.engine_report.ops_total);
    let _ = writeln!(out, "- Ops/sec: {:.2}", report.engine_report.ops_per_sec);
    let _ = writeln!(out, "- Retries: {}", report.engine_report.retries);
    let _ = writeln!(out, "- Aborts: {}", report.engine_report.aborts);
    let _ = writeln!(
        out,
        "- Integrity check: {}",
        report
            .engine_report
            .correctness
            .integrity_check_ok
            .map_or_else(|| "skipped".to_owned(), |ok| ok.to_string())
    );
    if let Some(error) = &report.engine_report.error {
        let _ = writeln!(out, "- Error: `{error}`");
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Ranked Hotspots\n");
    for entry in &actionable_ranking.named_hotspots {
        let _ = writeln!(
            out,
            "- rank {} {}: {}={} -> {} [{}]",
            entry.rank,
            entry.subsystem,
            entry.metric_kind,
            entry.metric_value,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Quantified Cost Components\n");
    for entry in &actionable_ranking.cost_components {
        let _ = writeln!(
            out,
            "- rank {} {}: time_ns={}, time_share_bps={}, allocator_pressure_bytes={}, allocator_share_bps={}, activity_count={} -> {} [{}]",
            entry.rank,
            entry.component,
            entry.time_ns,
            entry.time_share_basis_points,
            entry.allocator_pressure_bytes,
            entry.allocator_share_basis_points,
            entry.activity_count,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Allocator Pressure\n");
    for entry in &actionable_ranking.allocator_pressure {
        let _ = writeln!(
            out,
            "- rank {} {}: {}={} -> {} [{}]",
            entry.rank,
            entry.subsystem,
            entry.metric_kind,
            entry.metric_value,
            entry.implication,
            entry.mapped_beads.join(", ")
        );
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Top Opcodes\n");
    for entry in report.opcode_profile.iter().take(12) {
        let _ = writeln!(out, "- {}: {}", entry.opcode, entry.total);
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Replay\n");
    let _ = writeln!(out, "```sh\n{}\n```", report.replay_command);
    let _ = writeln!(out);
    let _ = writeln!(out, "## Structured Artifacts\n");
    let _ = writeln!(out, "- `profile.json` — raw scenario profile");
    let _ = writeln!(
        out,
        "- `opcode_profile.json` — raw opcode totals for this canonical hot-cell run"
    );
    let _ = writeln!(
        out,
        "- `subsystem_profile.json` — raw execution-subsystem timing and heap profile for this run"
    );
    let _ = writeln!(
        out,
        "- `actionable_ranking.json` — hotspot-to-bead ledger for D2-D4 handoff"
    );
    let _ = writeln!(
        out,
        "- `manifest.json` — replay metadata + artifact inventory"
    );
    out
}

fn hotspot_implication(subsystem: &str) -> (&'static str, &'static [&'static str]) {
    match subsystem {
        "parser_ast_churn" => (
            "D2 target: parser, AST, and compile churn should be reduced through prepared-artifact reuse.",
            &["bd-db300.4.2", "bd-db300.4.2.1"],
        ),
        "record_decode" => (
            "D3/D4 target: row decode work is expensive enough to justify scratch-space reuse and copy reduction.",
            &[
                "bd-db300.4.3",
                "bd-db300.4.3.1",
                "bd-db300.4.4",
                "bd-db300.4.4.1",
            ],
        ),
        "row_materialization" => (
            "D2/D3 target: result-row materialization is paying avoidable clone/allocation cost in the mixed hot path.",
            &["bd-db300.4.2", "bd-db300.4.3"],
        ),
        _ => (
            "Secondary follow-up bucket after the named Track D hotspots.",
            &[],
        ),
    }
}

fn allocator_implication(subsystem: &str) -> (&'static str, &'static [&'static str]) {
    match subsystem {
        "result_row_values" => (
            "D3 target: emitted result rows are carrying most of the transient heap pressure.",
            &["bd-db300.4.3", "bd-db300.4.3.2"],
        ),
        "record_decode_values" => (
            "D3/D4 target: decoded record values create enough heap churn to justify scratch buffers and copy reduction.",
            &["bd-db300.4.3", "bd-db300.4.4"],
        ),
        "parser_sql_bytes" => (
            "D2 target: parse-volume churn is visible and should be reduced with reuse rather than repeated prepare work.",
            &["bd-db300.4.2", "bd-db300.4.2.1"],
        ),
        _ => (
            "Secondary allocator-pressure source after the named Track D hotspots.",
            &[],
        ),
    }
}

fn cost_component_implication(component: &str) -> (&'static str, &'static [&'static str]) {
    match component {
        "parser_ast_churn" => (
            "D2 target: parser and compile reuse still dominate this component enough to justify prepared-artifact work next.",
            &["bd-db300.4.2", "bd-db300.4.2.1"],
        ),
        "record_decode" => (
            "D3/D4 target: decode cost is large enough to justify scratch buffers and copy-reduction work.",
            &["bd-db300.4.3", "bd-db300.4.4"],
        ),
        "row_materialization" => (
            "D3 target: emitted-row cloning and transient value ownership remain a first-class hot-path cost.",
            &["bd-db300.4.3", "bd-db300.4.3.1"],
        ),
        _ => (
            "Secondary cost component after the named Track D follow-on buckets.",
            &[],
        ),
    }
}

fn ratio_basis_points(value: u64, total: u64) -> u32 {
    if total == 0 {
        return 0;
    }
    let numerator = u128::from(value).saturating_mul(10_000);
    let denominator = u128::from(total);
    let rounded = numerator.saturating_add(denominator / 2) / denominator;
    u32::try_from(rounded).unwrap_or(u32::MAX)
}

fn actionable_entry(
    rank: usize,
    entry: &HotPathRankingEntry,
    implication: &'static str,
    mapped_beads: &'static [&'static str],
) -> HotPathActionableRankingEntry {
    HotPathActionableRankingEntry {
        rank: u32::try_from(rank + 1).unwrap_or(u32::MAX),
        subsystem: entry.subsystem.clone(),
        metric_kind: entry.metric_kind.clone(),
        metric_value: entry.metric_value,
        rationale: entry.rationale.clone(),
        implication: implication.to_owned(),
        mapped_beads: mapped_beads.iter().map(|bead| (*bead).to_owned()).collect(),
    }
}

fn build_hot_path_cost_components(report: &HotPathProfileReport) -> Vec<HotPathCostComponentEntry> {
    let parser_time_ns = report
        .parser
        .parse_time_ns
        .saturating_add(report.parser.rewrite_time_ns)
        .saturating_add(report.parser.compile_time_ns);
    let parser_activity_count = report
        .parser
        .parse_single_calls
        .saturating_add(report.parser.parse_multi_calls);
    let record_decode_activity_count = report
        .record_decode
        .parse_record_into_calls
        .saturating_add(report.record_decode.parse_record_column_calls);
    let total_time_ns = parser_time_ns
        .saturating_add(report.record_decode.decode_time_ns)
        .saturating_add(
            report
                .row_materialization
                .result_row_materialization_time_ns_total,
        );
    let total_allocator_pressure_bytes = report
        .allocator_pressure
        .decoded_value_heap_bytes_total
        .saturating_add(report.allocator_pressure.result_value_heap_bytes_total);

    let mut entries = vec![
        HotPathCostComponentEntry {
            rank: 0,
            component: "parser_ast_churn".to_owned(),
            time_ns: parser_time_ns,
            time_share_basis_points: ratio_basis_points(parser_time_ns, total_time_ns),
            allocator_pressure_bytes: 0,
            allocator_share_basis_points: 0,
            activity_count: parser_activity_count,
            rationale:
                "parse, rewrite, and compile time on the connection path; parser heap pressure is not directly instrumented"
                    .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathCostComponentEntry {
            rank: 0,
            component: "record_decode".to_owned(),
            time_ns: report.record_decode.decode_time_ns,
            time_share_basis_points: ratio_basis_points(
                report.record_decode.decode_time_ns,
                total_time_ns,
            ),
            allocator_pressure_bytes: report.record_decode.vdbe_decoded_value_heap_bytes_total,
            allocator_share_basis_points: ratio_basis_points(
                report.record_decode.vdbe_decoded_value_heap_bytes_total,
                total_allocator_pressure_bytes,
            ),
            activity_count: record_decode_activity_count,
            rationale: "record/column decode time paired with decoded-value heap pressure"
                .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
        HotPathCostComponentEntry {
            rank: 0,
            component: "row_materialization".to_owned(),
            time_ns: report
                .row_materialization
                .result_row_materialization_time_ns_total,
            time_share_basis_points: ratio_basis_points(
                report
                    .row_materialization
                    .result_row_materialization_time_ns_total,
                total_time_ns,
            ),
            allocator_pressure_bytes: report.row_materialization.result_value_heap_bytes_total,
            allocator_share_basis_points: ratio_basis_points(
                report.row_materialization.result_value_heap_bytes_total,
                total_allocator_pressure_bytes,
            ),
            activity_count: report.row_materialization.result_rows_total,
            rationale: "result-row cloning time paired with emitted-value allocator pressure"
                .to_owned(),
            implication: String::new(),
            mapped_beads: Vec::new(),
        },
    ];
    entries.sort_by(|lhs, rhs| {
        rhs.time_ns
            .cmp(&lhs.time_ns)
            .then_with(|| {
                rhs.allocator_pressure_bytes
                    .cmp(&lhs.allocator_pressure_bytes)
            })
            .then_with(|| lhs.component.cmp(&rhs.component))
    });
    for (rank, entry) in entries.iter_mut().enumerate() {
        entry.rank = u32::try_from(rank + 1).unwrap_or(u32::MAX);
        let (implication, mapped_beads) = cost_component_implication(&entry.component);
        entry.implication = implication.to_owned();
        entry.mapped_beads = mapped_beads.iter().map(|bead| (*bead).to_owned()).collect();
    }
    entries
}

#[must_use]
pub fn build_hot_path_opcode_profile(report: &HotPathProfileReport) -> HotPathOpcodeProfilePack {
    let mut opcodes = report.opcode_profile.clone();
    opcodes.sort_by(|lhs, rhs| {
        rhs.total
            .cmp(&lhs.total)
            .then_with(|| lhs.opcode.cmp(&rhs.opcode))
    });

    HotPathOpcodeProfilePack {
        schema_version: HOT_PATH_OPCODE_PROFILE_SCHEMA_V1.to_owned(),
        bead_id: report.bead_id.clone(),
        run_id: report.run_id.clone(),
        trace_id: report.trace_id.clone(),
        scenario_id: report.scenario_id.clone(),
        fixture_id: report.fixture_id.clone(),
        workload: report.workload.clone(),
        seed: report.seed,
        scale: report.scale,
        concurrency: report.concurrency,
        replay_command: report.replay_command.clone(),
        opcodes,
    }
}

#[must_use]
pub fn build_hot_path_subsystem_profile(
    report: &HotPathProfileReport,
) -> HotPathSubsystemProfilePack {
    HotPathSubsystemProfilePack {
        schema_version: HOT_PATH_SUBSYSTEM_PROFILE_SCHEMA_V1.to_owned(),
        bead_id: report.bead_id.clone(),
        run_id: report.run_id.clone(),
        trace_id: report.trace_id.clone(),
        scenario_id: report.scenario_id.clone(),
        fixture_id: report.fixture_id.clone(),
        workload: report.workload.clone(),
        seed: report.seed,
        scale: report.scale,
        concurrency: report.concurrency,
        replay_command: report.replay_command.clone(),
        subsystem_ranking: report.subsystem_ranking.clone(),
        allocator_ranking: report.allocator_pressure.ranked_sources.clone(),
        parser: report.parser.clone(),
        record_decode: report.record_decode.clone(),
        row_materialization: report.row_materialization.clone(),
    }
}

#[must_use]
pub fn build_hot_path_actionable_ranking(
    report: &HotPathProfileReport,
) -> HotPathActionableRanking {
    let named_hotspots = report
        .subsystem_ranking
        .iter()
        .enumerate()
        .map(|(rank, entry)| {
            let (implication, mapped_beads) = hotspot_implication(&entry.subsystem);
            actionable_entry(rank, entry, implication, mapped_beads)
        })
        .collect();
    let allocator_pressure = report
        .allocator_pressure
        .ranked_sources
        .iter()
        .enumerate()
        .map(|(rank, entry)| {
            let (implication, mapped_beads) = allocator_implication(&entry.subsystem);
            actionable_entry(rank, entry, implication, mapped_beads)
        })
        .collect();
    let cost_components = build_hot_path_cost_components(report);
    let top_opcodes = build_hot_path_opcode_profile(report)
        .opcodes
        .into_iter()
        .take(12)
        .collect();

    HotPathActionableRanking {
        schema_version: HOT_PATH_PROFILE_ACTIONABLE_RANKING_SCHEMA_V2.to_owned(),
        bead_id: report.bead_id.clone(),
        run_id: report.run_id.clone(),
        trace_id: report.trace_id.clone(),
        scenario_id: report.scenario_id.clone(),
        fixture_id: report.fixture_id.clone(),
        workload: report.workload.clone(),
        seed: report.seed,
        scale: report.scale,
        concurrency: report.concurrency,
        replay_command: report.replay_command.clone(),
        named_hotspots,
        cost_components,
        allocator_pressure,
        top_opcodes,
    }
}

pub fn write_hot_path_profile_artifacts(
    report: &HotPathProfileReport,
    output_dir: &Path,
) -> std::io::Result<HotPathArtifactManifest> {
    std::fs::create_dir_all(output_dir)?;

    let report_json = serde_json::to_string_pretty(report)
        .map_err(|error| std::io::Error::other(format!("profile JSON: {error}")))?;
    let opcode_profile = build_hot_path_opcode_profile(report);
    let opcode_profile_json = serde_json::to_string_pretty(&opcode_profile)
        .map_err(|error| std::io::Error::other(format!("opcode profile JSON: {error}")))?;
    let subsystem_profile = build_hot_path_subsystem_profile(report);
    let subsystem_profile_json = serde_json::to_string_pretty(&subsystem_profile)
        .map_err(|error| std::io::Error::other(format!("subsystem profile JSON: {error}")))?;
    let actionable_ranking = build_hot_path_actionable_ranking(report);
    let actionable_ranking_json = serde_json::to_string_pretty(&actionable_ranking)
        .map_err(|error| std::io::Error::other(format!("actionable ranking JSON: {error}")))?;
    let summary_md = render_hot_path_profile_markdown(report);

    let report_path = output_dir.join("profile.json");
    let opcode_profile_path = output_dir.join("opcode_profile.json");
    let subsystem_profile_path = output_dir.join("subsystem_profile.json");
    let actionable_ranking_path = output_dir.join("actionable_ranking.json");
    let summary_path = output_dir.join("summary.md");
    std::fs::write(&report_path, report_json.as_bytes())?;
    std::fs::write(&opcode_profile_path, opcode_profile_json.as_bytes())?;
    std::fs::write(&subsystem_profile_path, subsystem_profile_json.as_bytes())?;
    std::fs::write(&actionable_ranking_path, actionable_ranking_json.as_bytes())?;
    std::fs::write(&summary_path, summary_md.as_bytes())?;

    let manifest = HotPathArtifactManifest {
        schema_version: HOT_PATH_PROFILE_MANIFEST_SCHEMA_V1.to_owned(),
        bead_id: report.bead_id.clone(),
        run_id: report.run_id.clone(),
        trace_id: report.trace_id.clone(),
        scenario_id: report.scenario_id.clone(),
        fixture_id: report.fixture_id.clone(),
        workload: report.workload.clone(),
        seed: report.seed,
        scale: report.scale,
        concurrency: report.concurrency,
        concurrent_mode: report.concurrent_mode,
        run_integrity_check: report.run_integrity_check,
        golden_dir: report.golden_dir.clone(),
        working_base: report.working_base.clone(),
        replay_command: report.replay_command.clone(),
        files: vec![
            HotPathArtifactFile {
                path: "profile.json".to_owned(),
                bytes: u64::try_from(report_json.len()).unwrap_or(u64::MAX),
                description: "structured hot-path profile report".to_owned(),
            },
            HotPathArtifactFile {
                path: "opcode_profile.json".to_owned(),
                bytes: u64::try_from(opcode_profile_json.len()).unwrap_or(u64::MAX),
                description: "raw opcode totals for the canonical hot-cell run".to_owned(),
            },
            HotPathArtifactFile {
                path: "subsystem_profile.json".to_owned(),
                bytes: u64::try_from(subsystem_profile_json.len()).unwrap_or(u64::MAX),
                description: "raw execution-subsystem timing and heap profile for the run"
                    .to_owned(),
            },
            HotPathArtifactFile {
                path: "summary.md".to_owned(),
                bytes: u64::try_from(summary_md.len()).unwrap_or(u64::MAX),
                description: "human-readable hotspot ranking summary".to_owned(),
            },
            HotPathArtifactFile {
                path: "actionable_ranking.json".to_owned(),
                bytes: u64::try_from(actionable_ranking_json.len()).unwrap_or(u64::MAX),
                description: "structured hotspot-to-bead ledger for D2-D4 handoff".to_owned(),
            },
        ],
    };
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(|error| std::io::Error::other(format!("artifact manifest: {error}")))?;
    std::fs::write(output_dir.join("manifest.json"), manifest_json.as_bytes())?;

    let mut files = manifest.files.clone();
    files.push(HotPathArtifactFile {
        path: "manifest.json".to_owned(),
        bytes: u64::try_from(manifest_json.len()).unwrap_or(u64::MAX),
        description: "artifact manifest with replay metadata".to_owned(),
    });
    Ok(HotPathArtifactManifest { files, ..manifest })
}

// ── Matrix expansion ───────────────────────────────────────────────────

/// Expand the configuration into a flat list of matrix cells.
#[must_use]
pub fn expand_matrix(config: &PerfMatrixConfig) -> Vec<MatrixCell> {
    let mut cells = Vec::new();
    for engine in &config.engines {
        for fixture_id in &config.fixture_ids {
            for workload in &config.workload_presets {
                for &concurrency in &config.concurrency_levels {
                    cells.push(MatrixCell {
                        engine: *engine,
                        fixture_id: fixture_id.clone(),
                        workload: workload.clone(),
                        concurrency,
                    });
                }
            }
        }
    }
    cells
}

// ── OpLog generation ───────────────────────────────────────────────────

/// Generate an `OpLog` for the given preset name and parameters.
///
/// Returns `None` if the preset name is unrecognized.
#[must_use]
pub fn generate_oplog(
    preset_name: &str,
    fixture_id: &str,
    seed: u64,
    concurrency: u16,
    scale: u32,
) -> Option<OpLog> {
    match preset_name {
        "commutative_inserts_disjoint_keys" => Some(
            oplog::preset_commutative_inserts_disjoint_keys(fixture_id, seed, concurrency, scale),
        ),
        "hot_page_contention" => Some(oplog::preset_hot_page_contention(
            fixture_id,
            seed,
            concurrency,
            scale,
        )),
        "mixed_read_write" => Some(oplog::preset_mixed_read_write(
            fixture_id,
            seed,
            concurrency,
            scale,
        )),
        "deterministic_transform" => Some(oplog::preset_deterministic_transform(
            fixture_id, seed, scale,
        )),
        "large_txn" => Some(oplog::preset_large_txn(
            fixture_id,
            seed,
            concurrency,
            scale,
        )),
        "schema_migration" => Some(oplog::preset_schema_migration(fixture_id, seed, scale)),
        "btree_stress_sequential" => Some(oplog::preset_btree_stress_sequential(
            fixture_id, seed, scale,
        )),
        "wide_row_overflow" => Some(oplog::preset_wide_row_overflow(
            fixture_id, seed, scale, 2000,
        )),
        "bulk_delete_reinsert" => Some(oplog::preset_bulk_delete_reinsert(fixture_id, seed, scale)),
        "scatter_write" => Some(oplog::preset_scatter_write(
            fixture_id,
            seed,
            concurrency,
            scale,
        )),
        "multi_table_foreign_keys" => Some(oplog::preset_multi_table_foreign_keys(
            fixture_id, seed, scale,
        )),
        _ => None,
    }
}

// ── Cell execution ─────────────────────────────────────────────────────

/// Run a single matrix cell through the benchmark runner.
fn run_cell(cell: &MatrixCell, config: &PerfMatrixConfig) -> CellOutcome {
    let Some(oplog) = generate_oplog(
        &cell.workload,
        &cell.fixture_id,
        config.seed,
        cell.concurrency,
        config.scale,
    ) else {
        return CellOutcome {
            summary: None,
            error: Some(format!("unknown workload preset: {}", cell.workload)),
            engine: cell.engine.as_str().to_owned(),
            fixture_id: cell.fixture_id.clone(),
            workload: cell.workload.clone(),
            concurrency: cell.concurrency,
        };
    };

    let meta = BenchmarkMeta {
        engine: cell.engine.as_str().to_owned(),
        workload: cell.workload.clone(),
        fixture_id: cell.fixture_id.clone(),
        concurrency: cell.concurrency,
        cargo_profile: config.cargo_profile.clone(),
    };

    let workspace_config = WorkspaceConfig {
        golden_dir: config.project_root.join("sample_sqlite_db_files/golden"),
        working_base: config.project_root.join("sample_sqlite_db_files/working"),
    };

    let settings = config.settings.clone();
    let engine = cell.engine;
    let fixture_id = cell.fixture_id.clone();

    let summary = run_benchmark(&config.benchmark_config, &meta, |iteration_idx| {
        run_single_iteration(
            engine,
            &fixture_id,
            &oplog,
            &workspace_config,
            &settings,
            iteration_idx,
        )
    });

    CellOutcome {
        summary: Some(summary),
        error: None,
        engine: cell.engine.as_str().to_owned(),
        fixture_id: cell.fixture_id.clone(),
        workload: cell.workload.clone(),
        concurrency: cell.concurrency,
    }
}

/// Execute a single benchmark iteration for one engine.
///
/// Creates a fresh working copy, applies fairness PRAGMAs, runs the OpLog,
/// and returns the engine report.
fn run_single_iteration(
    engine: Engine,
    fixture_id: &str,
    oplog: &OpLog,
    workspace_config: &WorkspaceConfig,
    settings: &HarnessSettings,
    iteration_idx: u32,
) -> Result<EngineRunReport, crate::E2eError> {
    let label = format!("perf_{engine}_{fixture_id}_iter{iteration_idx}");
    let workspace = create_workspace_with_label(workspace_config, &[fixture_id], &label)?;

    let db = workspace.databases.first().ok_or_else(|| {
        crate::E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("fixture not found in workspace: {fixture_id}"),
        ))
    })?;

    match engine {
        Engine::Sqlite3 => {
            let exec_config = settings.to_sqlite_exec_config();
            run_oplog_sqlite(&db.db_path, oplog, &exec_config)
        }
        Engine::Fsqlite => {
            let exec_config = settings.to_fsqlite_exec_config();
            run_oplog_fsqlite(&db.db_path, oplog, &exec_config)
        }
    }
}

// ── Matrix runner ──────────────────────────────────────────────────────

/// Run the full performance benchmark matrix.
///
/// Iterates over every (engine × fixture × workload × concurrency) cell,
/// runs the benchmark for each, and collects results.
#[must_use]
pub fn run_perf_matrix(config: &PerfMatrixConfig) -> PerfResult {
    let cells = expand_matrix(config);
    let total = cells.len();
    let mut outcomes = Vec::with_capacity(total);
    let mut success_count = 0usize;
    let mut error_count = 0usize;

    for (i, cell) in cells.iter().enumerate() {
        eprintln!("[perf] ({}/{}) running {}", i + 1, total, cell,);

        let outcome = run_cell(cell, config);

        if outcome.error.is_some() {
            error_count += 1;
            if config.fail_fast {
                outcomes.push(outcome);
                break;
            }
        } else {
            success_count += 1;
        }

        outcomes.push(outcome);
    }

    PerfResult {
        schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
        total_cells: total,
        success_count,
        error_count,
        cells: outcomes,
    }
}

/// Write all per-cell benchmark outcomes to a JSONL file.
///
/// Each line is a serialized [`CellOutcome`], including failed cells.
///
/// # Errors
///
/// Returns an I/O error if the file cannot be written.
pub fn write_results_jsonl(result: &PerfResult, path: &Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    for cell in &result.cells {
        let line = serde_json::to_string(cell).map_err(std::io::Error::other)?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as SqliteConnection;

    #[test]
    fn test_engine_display() {
        assert_eq!(Engine::Sqlite3.as_str(), "sqlite3");
        assert_eq!(Engine::Fsqlite.as_str(), "fsqlite");
        assert_eq!(format!("{}", Engine::Sqlite3), "sqlite3");
    }

    #[test]
    fn test_expand_matrix() {
        let config = PerfMatrixConfig {
            engines: vec![Engine::Sqlite3, Engine::Fsqlite],
            fixture_ids: vec!["fix1".to_owned(), "fix2".to_owned()],
            workload_presets: vec!["deterministic_transform".to_owned()],
            concurrency_levels: vec![1, 4],
            seed: 42,
            scale: 10,
            project_root: PathBuf::from("/tmp"),
            benchmark_config: BenchmarkConfig::default(),
            settings: HarnessSettings::default(),
            cargo_profile: "release".to_owned(),
            fail_fast: false,
        };

        let cells = expand_matrix(&config);
        // 2 engines × 2 fixtures × 1 workload × 2 concurrencies = 8
        assert_eq!(cells.len(), 8);

        // Verify all combinations are present.
        assert!(
            cells.iter().any(|c| c.engine == Engine::Sqlite3
                && c.fixture_id == "fix1"
                && c.concurrency == 1)
        );
        assert!(
            cells.iter().any(|c| c.engine == Engine::Fsqlite
                && c.fixture_id == "fix2"
                && c.concurrency == 4)
        );
    }

    #[test]
    fn test_generate_oplog_known_presets() {
        let known = [
            "commutative_inserts_disjoint_keys",
            "hot_page_contention",
            "mixed_read_write",
            "deterministic_transform",
            "large_txn",
            "schema_migration",
            "btree_stress_sequential",
            "wide_row_overflow",
            "bulk_delete_reinsert",
            "scatter_write",
            "multi_table_foreign_keys",
        ];

        for name in &known {
            let log = generate_oplog(name, "test-fix", 42, 2, 20);
            assert!(
                log.is_some(),
                "generate_oplog should recognize preset '{name}'"
            );
            let log = log.unwrap();
            assert_eq!(log.header.preset.as_deref(), Some(*name));
        }
    }

    #[test]
    fn test_generate_oplog_unknown_preset() {
        assert!(generate_oplog("nonexistent_preset", "fix", 42, 1, 10).is_none());
    }

    #[test]
    fn test_matrix_cell_display() {
        let cell = MatrixCell {
            engine: Engine::Sqlite3,
            fixture_id: "mydb".to_owned(),
            workload: "hot_page_contention".to_owned(),
            concurrency: 8,
        };
        assert_eq!(format!("{cell}"), "sqlite3:hot_page_contention:mydb:c8");
    }

    #[test]
    fn test_perf_result_schema() {
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 0,
            success_count: 0,
            error_count: 0,
            cells: Vec::new(),
        };
        assert!(result.all_succeeded());
        assert_eq!(result.to_jsonl().unwrap(), "");
    }

    #[test]
    fn test_cell_outcome_with_error() {
        let outcome = CellOutcome {
            summary: None,
            error: Some("test error".to_owned()),
            engine: "sqlite3".to_owned(),
            fixture_id: "fix1".to_owned(),
            workload: "test".to_owned(),
            concurrency: 1,
        };
        let json = serde_json::to_string(&outcome).unwrap();
        let parsed: CellOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.error.as_deref(), Some("test error"));
        assert!(parsed.summary.is_none());
    }

    #[test]
    fn test_perf_jsonl_includes_error_cells() {
        let result = PerfResult {
            schema_version: PERF_RESULT_SCHEMA_V1.to_owned(),
            total_cells: 1,
            success_count: 0,
            error_count: 1,
            cells: vec![CellOutcome {
                summary: None,
                error: Some("boom".to_owned()),
                engine: "fsqlite".to_owned(),
                fixture_id: "fix1".to_owned(),
                workload: "hot_page_contention".to_owned(),
                concurrency: 4,
            }],
        };

        let jsonl = result.to_jsonl().unwrap();
        assert!(jsonl.contains("\"error\":\"boom\""));
        assert!(jsonl.contains("\"engine\":\"fsqlite\""));
    }

    #[test]
    fn hot_path_profile_smoke_writes_artifacts() {
        let tempdir = tempfile::tempdir().unwrap();
        let db_path = tempdir.path().join("profile.db");
        SqliteConnection::open(&db_path).unwrap();

        let config = FsqliteHotPathProfileConfig {
            seed: 7,
            scale: 6,
            concurrency: 1,
            exec_config: crate::fsqlite_executor::FsqliteExecConfig {
                concurrent_mode: true,
                run_integrity_check: false,
                ..crate::fsqlite_executor::FsqliteExecConfig::default()
            },
            replay_command: "cargo run -p fsqlite-e2e --bin realdb-e2e -- hot-profile --db smoke"
                .to_owned(),
            golden_dir: Some("sample_sqlite_db_files/golden".to_owned()),
            working_base: Some("sample_sqlite_db_files/working".to_owned()),
        };

        let report = profile_fsqlite_mixed_read_write_hot_path(&db_path, "smoke", &config).unwrap();

        assert_eq!(report.schema_version, HOT_PATH_PROFILE_SCHEMA_V1);
        assert_eq!(report.fixture_id, "smoke");
        assert_eq!(report.workload, "mixed_read_write");
        assert!(report.concurrent_mode);
        assert!(!report.run_integrity_check);
        assert_eq!(
            report.golden_dir.as_deref(),
            Some("sample_sqlite_db_files/golden")
        );
        assert_eq!(
            report.working_base.as_deref(),
            Some("sample_sqlite_db_files/working")
        );
        assert!(!report.opcode_profile.is_empty());
        assert!(
            report.record_decode.parse_record_column_calls > 0
                || report.record_decode.parse_record_into_calls > 0
        );

        let artifact_dir = tempdir.path().join("artifacts");
        let manifest = write_hot_path_profile_artifacts(&report, &artifact_dir).unwrap();
        let opcode_profile: HotPathOpcodeProfilePack = serde_json::from_slice(
            &std::fs::read(artifact_dir.join("opcode_profile.json")).unwrap(),
        )
        .unwrap();
        let subsystem_profile: HotPathSubsystemProfilePack = serde_json::from_slice(
            &std::fs::read(artifact_dir.join("subsystem_profile.json")).unwrap(),
        )
        .unwrap();
        let actionable_ranking: HotPathActionableRanking = serde_json::from_slice(
            &std::fs::read(artifact_dir.join("actionable_ranking.json")).unwrap(),
        )
        .unwrap();

        assert_eq!(manifest.schema_version, HOT_PATH_PROFILE_MANIFEST_SCHEMA_V1);
        assert!(manifest.concurrent_mode);
        assert!(!manifest.run_integrity_check);
        assert_eq!(
            manifest.golden_dir.as_deref(),
            Some("sample_sqlite_db_files/golden")
        );
        assert_eq!(
            manifest.working_base.as_deref(),
            Some("sample_sqlite_db_files/working")
        );
        assert!(artifact_dir.join("profile.json").exists());
        assert!(artifact_dir.join("opcode_profile.json").exists());
        assert!(artifact_dir.join("subsystem_profile.json").exists());
        assert!(artifact_dir.join("summary.md").exists());
        assert!(artifact_dir.join("actionable_ranking.json").exists());
        assert!(artifact_dir.join("manifest.json").exists());
        assert_eq!(manifest.files.len(), 6);
        assert_eq!(
            opcode_profile.schema_version,
            HOT_PATH_OPCODE_PROFILE_SCHEMA_V1
        );
        assert_eq!(
            subsystem_profile.schema_version,
            HOT_PATH_SUBSYSTEM_PROFILE_SCHEMA_V1
        );
        assert_eq!(
            actionable_ranking.schema_version,
            HOT_PATH_PROFILE_ACTIONABLE_RANKING_SCHEMA_V2
        );
        assert!(!opcode_profile.opcodes.is_empty());
        assert!(!subsystem_profile.subsystem_ranking.is_empty());
        assert!(!actionable_ranking.named_hotspots.is_empty());
        assert!(!actionable_ranking.cost_components.is_empty());
        assert_eq!(
            actionable_ranking.top_opcodes,
            opcode_profile
                .opcodes
                .iter()
                .take(actionable_ranking.top_opcodes.len())
                .cloned()
                .collect::<Vec<_>>()
        );
        assert!(
            std::fs::read_to_string(artifact_dir.join("summary.md"))
                .unwrap()
                .contains("## Quantified Cost Components")
        );
        assert!(
            actionable_ranking
                .named_hotspots
                .iter()
                .flat_map(|entry| entry.mapped_beads.iter())
                .any(|bead| bead == "bd-db300.4.2"
                    || bead == "bd-db300.4.3"
                    || bead == "bd-db300.4.4")
        );
        assert_eq!(actionable_ranking.cost_components.len(), 3);
        let component_names = actionable_ranking
            .cost_components
            .iter()
            .map(|entry| entry.component.as_str())
            .collect::<Vec<_>>();
        assert!(component_names.contains(&"parser_ast_churn"));
        assert!(component_names.contains(&"record_decode"));
        assert!(component_names.contains(&"row_materialization"));
    }
}
